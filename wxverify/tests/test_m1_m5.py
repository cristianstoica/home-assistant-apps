from __future__ import annotations

import asyncio
import contextlib
import errno
import json
import logging
import re
import sqlite3
import threading
import time
from html.parser import HTMLParser
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from wxverify import __version__, config
from wxverify.api.app import _default_stop_process, _stop_on_worker_done, create_app
from wxverify.collection.budget import current_billing_day
from wxverify.collection.forecast_fetcher import persist_fetch_result
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.migrations import TARGET_USER_VERSION
from wxverify.db.queue import (
    Job,
    claim_next_job,
    complete,
    enqueue_if_absent,
    reclaim_all_stale,
)
from wxverify.feeds.meteoblue import MeteoblueAdapter
from wxverify.feeds.open_meteo import (
    _samples_from_hourly as open_meteo_samples_from_hourly,
)
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastRequest,
)
from wxverify.obs.pws_adapter import (
    PwsStation,
    observations_from_payload,
)
from wxverify.scoring.consensus import (
    StationReading,
    compute_consensus,
    insert_station_observation,
)
from wxverify.settings.keys import set_setting
from wxverify.worker.control import JobDeferred
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    clear_domain_backoff,
    record_http_backoff,
    source_domain,
)
from wxverify.worker.processor import (
    _is_process_fatal_permission_error,
    dispatch,
    run_worker,
)
from wxverify.worker.station_pacing import (
    PWS_STATION_MAX_DELAY_SECONDS,
    PWS_STATION_MIN_DELAY_SECONDS,
    pace_station_call,
    station_call_delay_seconds,
)


class _AttrParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tags: list[tuple[str, dict[str, str]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append((tag, {key: value or "" for key, value in attrs}))


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


TIMESTAMP_MILLIS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


def test_migrations_seed_fk_and_not_null_census(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert conn.execute("PRAGMA user_version").fetchone()[0] == TARGET_USER_VERSION
    assert (
        conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type='table' AND name='runtime_state'
            """
        ).fetchone()
        is not None
    )

    sources = {
        row["source"]: row
        for row in conn.execute("SELECT * FROM sources ORDER BY source").fetchall()
    }
    assert sources["open-meteo"]["daily_call_limit"] == 10000
    assert sources["meteoblue"]["daily_call_limit"] == 5
    assert sources["meteoblue"]["daily_credit_limit"] == 65000
    assert sources["weathercom"]["daily_credit_limit"] is None

    meteoblue_package = conn.execute(
        """
        SELECT fetch_interval_minutes
        FROM feeds
        WHERE source='meteoblue' AND model='multimodel'
        """
    ).fetchone()
    assert meteoblue_package is not None
    assert meteoblue_package["fetch_interval_minutes"] == 360

    open_meteo = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM feeds
        WHERE source='open-meteo' AND default_subscribed=1
        """
    ).fetchone()
    assert open_meteo["n"] == 8
    assert (
        conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM feeds
            WHERE source='meteoblue' AND model!='multimodel'
            """
        ).fetchone()["n"]
        == 0
    )

    required = {
        "sites": {
            "forecast_lat",
            "forecast_lon",
            "elevation_m",
            "timezone",
            "enabled",
            "rain_threshold_mm",
            "backfill_status",
        },
        "stations": {
            "site_id",
            "pws_station_id",
            "lat",
            "lon",
            "dem_elevation_m",
            "enabled",
            "error_count",
        },
        "feeds": {
            "source",
            "model",
            "enabled",
            "is_virtual",
            "default_subscribed",
            "fetch_interval_minutes",
            "max_lead_hours",
        },
        "forecast_samples": {
            "site_id",
            "feed_id",
            "variable",
            "issued_at",
            "valid_at",
            "lead_hours",
            "value",
            "model_run_id",
            "source_raw",
        },
        "forecast_history": {
            "site_id",
            "feed_id",
            "variable",
            "day_ahead",
            "valid_at",
            "local_date",
            "value",
            "lead_hours",
        },
        "station_observations": {
            "station_id",
            "variable",
            "valid_at",
            "value",
            "qc_flag",
        },
        "observations": {
            "site_id",
            "variable",
            "valid_at",
            "value",
            "n_stations",
            "rejected_stations",
        },
        "sources": {"source", "daily_call_limit", "billing_tz"},
        "api_budget": {"source", "billing_day"},
    }
    for table, columns in required.items():
        info = {row["name"]: row for row in conn.execute(f"PRAGMA table_info({table})")}
        for column in columns:
            assert info[column]["notnull"] == 1, f"{table}.{column}"

    nullable = {
        ("sources", "daily_credit_limit"),
        ("site_feed_state", "grid_lat"),
        ("site_feed_state", "grid_lon"),
        ("site_feed_state", "grid_elevation_m"),
        ("site_feed_state", "enabled"),
        ("sites", "last_obs_at"),
    }
    for table, column in nullable:
        info = {row["name"]: row for row in conn.execute(f"PRAGMA table_info({table})")}
        assert info[column]["notnull"] == 0, f"{table}.{column}"

    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('Test', 40.0, -105.0, 900.0, 'Europe/Athens')
        """
    )
    site_id = int(cur.lastrowid)
    row = conn.execute(
        """
        SELECT enabled, rain_threshold_mm, backfill_status
        FROM sites WHERE id=?
        """,
        (site_id,),
    ).fetchone()
    assert row["enabled"] == 1
    assert row["rain_threshold_mm"] == 0.2
    assert row["backfill_status"] == "pending"

    try:
        conn.execute("UPDATE sites SET backfill_status='bogus' WHERE id=?", (site_id,))
    except sqlite3.IntegrityError:
        pass
    else:  # pragma: no cover
        raise AssertionError("invalid backfill_status was accepted")

    try:
        conn.execute(
            """
            INSERT INTO jobs (type, site_id, job_key)
            VALUES ('fetch_feed', NULL, 'bad')
            """
        )
    except sqlite3.IntegrityError:
        pass
    else:  # pragma: no cover
        raise AssertionError("site-scoped NULL job was accepted")

    feed_id = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' LIMIT 1"
    ).fetchone()["id"]
    try:
        conn.execute(
            """
            INSERT INTO forecast_history
                (site_id, feed_id, variable, day_ahead, valid_at, local_date,
                 value, lead_hours, issued_at, contributors)
            VALUES (?, ?, 'temperature', 8, '2026-01-01T01:00:00Z',
                    '2026-01-01', 7.0, 1, '2026-01-01T00:00:00Z', NULL)
            """,
            (site_id, feed_id),
        )
    except sqlite3.IntegrityError:
        pass
    else:  # pragma: no cover
        raise AssertionError("day_ahead=8 was accepted")


def test_worker_status_exposes_runtime_and_completed_job_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "worker-status.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed_completed_jobs(conn: sqlite3.Connection) -> None:
            site_id = int(
                conn.execute(
                    """
                    INSERT INTO sites
                        (name, forecast_lat, forecast_lon, elevation_m, timezone)
                    VALUES ('Runtime', 40, -105, 900, 'UTC')
                    """
                ).lastrowid
            )
            conn.execute(
                """
                INSERT INTO jobs(type, site_id, job_key, payload, status, updated_at)
                VALUES
                    ('fetch_feed', ?, 'feed', '{}', 'completed',
                     '2035-01-01T01:00:00.000Z'),
                    ('fetch_obs', ?, 'obs', '{}', 'completed',
                     '2035-01-01T02:00:00.000Z'),
                    ('pair_and_score', ?, 'score', '{}', 'completed',
                     '2035-01-01T03:00:00.000Z')
                """,
                (site_id, site_id, site_id),
            )

        db.write_sync(_seed_completed_jobs)
        payload = client.get("/api/worker/status").json()

    assert payload["worker_started_at"] is not None
    assert TIMESTAMP_MILLIS_RE.match(payload["worker_started_at"])
    assert payload["worker_last_loop_at"] is None
    assert payload["scheduler_last_tick_at"] is None
    assert payload["last_completed_fetch_feed_at"] == "2035-01-01T01:00:00.000Z"
    assert payload["last_completed_fetch_obs_at"] == "2035-01-01T02:00:00.000Z"
    assert payload["last_completed_pair_and_score_at"] is None, (
        "no active publication is seeded here, so the newest published_at"
        " read has nothing to report"
    )


def test_idle_worker_stamps_runtime_heartbeats(tmp_path: Path) -> None:
    close_db()
    config.db_path = str(tmp_path / "worker-heartbeat.db")
    config.options_path = str(tmp_path / "missing-options.json")
    stopped: list[None] = []
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))
    with TestClient(app) as client:
        payload: dict[str, object] = {}
        for _ in range(20):
            payload = client.get("/api/worker/status").json()
            if payload["worker_last_loop_at"] and payload["scheduler_last_tick_at"]:
                break
            time.sleep(0.1)

    assert stopped == []
    for key in (
        "worker_started_at",
        "worker_last_loop_at",
        "scheduler_last_tick_at",
    ):
        assert isinstance(payload[key], str)
        assert TIMESTAMP_MILLIS_RE.match(payload[key])


def test_worker_heartbeat_write_failure_logs_and_loop_continues(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    idle_current_obs_poller: None,
) -> None:
    class ReachedClaim(Exception):
        pass

    class FakeDb:
        async def read(self, fn):  # type: ignore[no-untyped-def]
            return fn(None)

        async def write(self, fn):  # type: ignore[no-untyped-def]
            return fn(None)

    injected: dict[str, sqlite3.OperationalError] = {}

    def fail_heartbeat(conn: object, key: str) -> None:
        error = sqlite3.OperationalError(f"heartbeat failed {key}")
        injected[key] = error
        raise error

    def stop_at_claim(conn: object) -> None:
        raise ReachedClaim()

    monkeypatch.setattr(
        "wxverify.worker.processor.set_runtime_state_now", fail_heartbeat
    )
    monkeypatch.setattr("wxverify.worker.processor.scheduler_tick", lambda conn: None)
    monkeypatch.setattr(
        "wxverify.worker.processor.purge_failed_jobs_older_than",
        lambda conn, hours: None,
    )
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", stop_at_claim)

    with pytest.raises(ReachedClaim):
        asyncio.run(run_worker(FakeDb()))  # type: ignore[arg-type]

    assert "runtime heartbeat write failed key=worker_last_loop_at" in caplog.text
    assert "runtime heartbeat write failed key=scheduler_last_tick_at" in caplog.text
    for key in ("worker_last_loop_at", "scheduler_last_tick_at"):
        records = [
            r
            for r in caplog.records
            if r.getMessage() == f"runtime heartbeat write failed key={key}"
        ]
        assert len(records) == 1
        assert records[0].exc_info is not None
        assert records[0].exc_info[1] is injected[key]


def test_worker_permission_error_is_process_fatal(
    monkeypatch: pytest.MonkeyPatch, idle_current_obs_poller: None
) -> None:
    job = Job(
        id=99,
        type="fetch_feed",
        site_id=1,
        job_key="fetch:1",
        payload={"feed_id": 1},
        status="running",
        retry_count=0,
        max_retries=5,
    )
    failed_jobs: list[int] = []

    class FakeDb:
        async def read(self, fn):  # type: ignore[no-untyped-def]
            return fn(None)

        async def write(self, fn):  # type: ignore[no-untyped-def]
            return fn(None)

    def claim_once(conn: object) -> Job:
        return job

    async def raise_permission_error(db: object, writer: object, claimed: Job) -> None:
        assert claimed == job
        raise PermissionError(errno.EPERM, "Operation not permitted")

    def record_fail(conn: object, job_id: int, error: str) -> None:
        failed_jobs.append(job_id)

    monkeypatch.setattr(
        "wxverify.worker.processor.set_runtime_state_now", lambda c, k: None
    )
    monkeypatch.setattr("wxverify.worker.processor.scheduler_tick", lambda conn: None)
    monkeypatch.setattr(
        "wxverify.worker.processor.purge_failed_jobs_older_than",
        lambda conn, hours: None,
    )
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", claim_once)
    monkeypatch.setattr("wxverify.worker.processor.dispatch", raise_permission_error)
    monkeypatch.setattr("wxverify.worker.processor.fail", record_fail)

    with pytest.raises(PermissionError):
        asyncio.run(run_worker(FakeDb()))  # type: ignore[arg-type]

    assert failed_jobs == []


def test_process_fatal_permission_error_matches_wrapped_httpx() -> None:
    try:
        try:
            raise PermissionError(errno.EPERM, "Operation not permitted")
        except PermissionError as exc:
            raise httpx.ConnectError("[Errno 1] Operation not permitted") from exc
    except httpx.ConnectError as exc:
        wrapped = exc

    assert _is_process_fatal_permission_error(wrapped)


def test_worker_done_callback_terminates_only_non_cancelled_tasks() -> None:
    async def crash() -> None:
        raise RuntimeError("boom")

    async def clean_return() -> None:
        return None

    async def wait_forever() -> None:
        await asyncio.Event().wait()

    async def exercise() -> tuple[list[str], list[str], list[str]]:
        crashed_calls: list[str] = []
        crashed = asyncio.create_task(crash())
        with pytest.raises(RuntimeError):
            await crashed
        _stop_on_worker_done(crashed, lambda: crashed_calls.append("stop"))

        returned_calls: list[str] = []
        returned = asyncio.create_task(clean_return())
        await returned
        _stop_on_worker_done(returned, lambda: returned_calls.append("stop"))

        cancelled_calls: list[str] = []
        cancelled = asyncio.create_task(wait_forever())
        await asyncio.sleep(0)
        cancelled.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cancelled
        _stop_on_worker_done(cancelled, lambda: cancelled_calls.append("stop"))

        return crashed_calls, returned_calls, cancelled_calls

    crashed_calls, returned_calls, cancelled_calls = asyncio.run(exercise())

    assert crashed_calls == ["stop"]
    assert returned_calls == ["stop"]
    assert cancelled_calls == []
    assert "_exit" in _default_stop_process.__code__.co_names
    assert 1 in _default_stop_process.__code__.co_consts


def test_worker_done_callback_registered_fires_on_worker_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Integration: crashing worker triggers stop_process via the real lifespan."""
    close_db()
    config.db_path = str(tmp_path / "cb-crash.db")
    config.options_path = str(tmp_path / "missing-options.json")
    callback_fired = threading.Event()

    async def _crashing_worker(db: object) -> None:
        raise RuntimeError("worker crash")

    monkeypatch.setattr("wxverify.api.app.run_worker", _crashing_worker)
    app = create_app(root_path="", _stop_process=lambda: callback_fired.set())
    # The worker crashes during the lifespan yield. `_cancel_and_reap`
    # captures every non-cancelled task outcome and logs it by name
    # instead of letting it re-raise out of the `finally`; the reasoning
    # is in that helper's docstring, under "Failure policy". The old
    # propagation this test used to assert is obsolete scaffolding.
    # `TestClient(app)` is therefore entered BARE: a regression back to
    # propagation makes this test error at `__exit__` on its own, so the
    # bare `with` block is itself an oracle, not just a precondition for
    # the callback check.
    with (
        caplog.at_level(logging.ERROR, logger="wxverify.api.app"),
        TestClient(app),
    ):
        callback_fired.wait(timeout=2.0)
    assert callback_fired.is_set(), (
        "add_done_callback registration is missing: stop_process was never called"
    )

    # Positive companion to the bare `with` above: prove the crashed worker
    # was actually reaped at shutdown, not merely that nothing escaped.
    # Without this, deleting `tasks.append(("worker", worker))` in app.py
    # would still leave `callback_fired` set (`add_done_callback` is wired
    # independently of `tasks`) and this test would go green over a worker
    # that is never cancelled or awaited at shutdown at all.
    worker_records = [
        r
        for r in caplog.records
        if r.name == "wxverify.api.app" and r.getMessage() == "shutdown: worker failed"
    ]
    assert len(worker_records) == 1, [r.getMessage() for r in caplog.records]
    exc = worker_records[0].exc_info
    assert exc is not None and isinstance(exc[1], RuntimeError)
    assert str(exc[1]) == "worker crash"

    # The siblings (export sweeper, read-cache warm) must be reaped cleanly
    # alongside the crashed worker -- no other "shutdown: ... failed" record
    # should appear. Confirmed empirically against a real run: the warm's
    # own cancellation surfaces only as a WARNING from
    # `wxverify.db.connection` ("db read failed or cancelled"), never as an
    # app-logger shutdown record, so this list is exactly one entry long.
    shutdown_messages = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.api.app" and r.getMessage().startswith("shutdown: ")
    ]
    assert shutdown_messages == ["shutdown: worker failed"]


def test_worker_done_callback_registered_fires_on_clean_return(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Integration: worker that returns cleanly also triggers stop_process."""
    close_db()
    config.db_path = str(tmp_path / "cb-return.db")
    config.options_path = str(tmp_path / "missing-options.json")
    callback_fired = threading.Event()

    async def _returning_worker(db: object) -> None:
        return None

    monkeypatch.setattr("wxverify.api.app.run_worker", _returning_worker)
    app = create_app(root_path="", _stop_process=lambda: callback_fired.set())
    with TestClient(app):
        callback_fired.wait(timeout=2.0)
    assert callback_fired.is_set(), (
        "add_done_callback registration is missing: stop_process was never called"
    )


def test_s6_finish_halts_on_worker_crash_exit_but_not_signal_shutdown() -> None:
    finish = Path("rootfs/etc/services.d/wxverify/finish").read_text(encoding="utf-8")

    assert '"${exit_code}" -ne 0' in finish
    assert '"${exit_code}" -ne 256' in finish
    assert "exec /run/s6/basedir/bin/halt" in finish


# There is no migrate_v1 (or migrate_v2 ... migrate_v16) function to test:
# user_version 1 is refused outright rather than upgraded, and a fresh
# build's sites table carries both backfill columns from create_tables
# directly -- test_migrations_seed_fk_and_not_null_census above already
# pins backfill_status there.


def test_database_rejects_sqlite_without_returning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "old-sqlite.db")
    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 34, 0))
    with pytest.raises(RuntimeError, match="sqlite 3.35.0"):
        init_db(config.db_path)


def test_queue_dedupe_and_stale_reclaim(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('Q', 40, -105, 900, 'UTC')
        """
    ).lastrowid
    first = enqueue_if_absent(conn, "fetch_feed", site_id, "fetch:1", {"feed_id": 1})
    second = enqueue_if_absent(conn, "fetch_feed", site_id, "fetch:1", {"feed_id": 1})
    third = enqueue_if_absent(conn, "fetch_feed", site_id, "fetch:2", {"feed_id": 2})
    assert first.created is True
    assert second.created is False
    assert third.created is True

    job = claim_next_job(conn)
    assert job is not None
    assert job.site_id == site_id
    assert reclaim_all_stale(conn) == 1
    reclaimed = claim_next_job(conn)
    assert reclaimed is not None
    complete(conn, reclaimed.id)


def test_claim_next_job_dispositions_unreadable_row_instead_of_returning_it(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('Q', 40, -105, 900, 'UTC')
        """
    ).lastrowid
    bad_id = int(
        conn.execute(
            """
            INSERT INTO jobs (type, site_id, job_key, payload, status)
            VALUES ('fetch_feed', ?, 'fetch:bad', 'not json', 'pending')
            """,
            (site_id,),
        ).lastrowid
    )
    good = enqueue_if_absent(conn, "fetch_feed", site_id, "fetch:good", {"feed_id": 1})
    assert good.created is True

    # The bad row sorts first (ORDER BY created_at, id); claim_next_job claims
    # and dispositions exactly one row per call, so the first call consumes
    # the bad row and returns None rather than falling through to the good one.
    assert claim_next_job(conn) is None, (
        "unreadable row must not be handed to the worker"
    )

    row = conn.execute(
        "SELECT status, last_error FROM jobs WHERE id = ?", (bad_id,)
    ).fetchone()
    assert row["status"] == "failed"
    assert row["last_error"] == "unreadable job row"

    claimed = claim_next_job(conn)
    assert claimed is not None
    assert claimed.id == good.job_id, "the next claim call must reach the good job"
    complete(conn, claimed.id)

    assert claim_next_job(conn) is None, (
        "the disposed row is terminal ('failed'), not 'pending' -- it must"
        " never be re-claimed"
    )


def test_claim_next_job_dispositions_blob_retry_count_instead_of_returning_it(
    tmp_path: Path,
) -> None:
    """A BLOB in retry_count is a distinct failure class from a malformed
    payload: int(row["retry_count"]) raises ValueError, not JSONDecodeError
    or TypeError. INTEGER affinity does not coerce a BLOB, so this is a
    legitimate on-disk state. This pins that the except tuple in
    claim_next_job actually covers ValueError -- narrowing it back to
    (JSONDecodeError, TypeError) would let this row escape disposition and
    be handed to the worker instead.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('Q', 40, -105, 900, 'UTC')
        """
    ).lastrowid
    bad_id = int(
        conn.execute(
            """
            INSERT INTO jobs (type, site_id, job_key, payload, status, retry_count)
            VALUES ('fetch_feed', ?, 'fetch:blob-retry', '{}', 'pending', ?)
            """,
            (site_id, b"\x00\x01"),
        ).lastrowid
    )

    assert claim_next_job(conn) is None, (
        "a BLOB in retry_count must not be handed to the worker"
    )

    row = conn.execute(
        "SELECT status, last_error FROM jobs WHERE id = ?", (bad_id,)
    ).fetchone()
    assert row["status"] == "failed"
    assert row["last_error"] == "unreadable job row"


def test_pws_parser_parses_temperature_and_precip_from_observations_payload() -> None:
    parsed = observations_from_payload(
        {
            "observations": [
                {
                    "obsTimeUtc": "2026-01-01 01:53:00",
                    "metric": {
                        "tempAvg": 6.5,
                        "windspeedAvg": 36.0,
                        "precipTotal": 0.3,
                    },
                }
            ]
        }
    )
    assert [(row.variable, row.valid_at) for row in parsed] == [
        ("temperature", "2026-01-01T01:00:00Z"),
        ("precip", "2026-01-01T01:00:00Z"),
    ]
    assert parsed[0].value == 6.5
    assert parsed[1].value == 0.3
    precip_series = [
        row
        for row in observations_from_payload(
            {
                "observations": [
                    {
                        "obsTimeUtc": "2026-01-01 00:53:00",
                        "obsTimeLocal": "2026-01-01 02:53:00",
                        "metric": {"precipTotal": 0.2},
                    },
                    {
                        "obsTimeUtc": "2026-01-01 01:53:00",
                        "obsTimeLocal": "2026-01-01 03:53:00",
                        "metric": {"precipTotal": 0.7},
                    },
                    {
                        "obsTimeUtc": "2026-01-01 22:53:00",
                        "obsTimeLocal": "2026-01-02 00:53:00",
                        "metric": {"precipTotal": 0.1},
                    },
                    {
                        "obsTimeUtc": "2026-01-01 23:53:00",
                        "obsTimeLocal": "2026-01-02 01:53:00",
                        "metric": {"precipTotal": 0.4},
                    },
                ]
            }
        )
        if row.variable == "precip"
    ]
    assert [row.value for row in precip_series] == pytest.approx([0.2, 0.5, 0.1, 0.3])
    assert precip_series[1].source_raw == "0.7 mm precipTotal"


def test_station_call_pacing_delay_is_seeded_bounded_and_rate_limits_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delay = station_call_delay_seconds(7, 11, 1, seed=123)
    assert station_call_delay_seconds(7, 11, 1, seed=123) == delay
    assert PWS_STATION_MIN_DELAY_SECONDS <= delay <= PWS_STATION_MAX_DELAY_SECONDS
    assert station_call_delay_seconds(7, 11, 0, seed=123) == 0.0

    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("wxverify.worker.station_pacing.asyncio.sleep", fake_sleep)
    asyncio.run(pace_station_call(7, 11, 0))
    asyncio.run(pace_station_call(7, 11, 1))
    assert sleeps == [station_call_delay_seconds(7, 11, 1)]


def test_consensus_outlier_rejection_oracle() -> None:
    """Numeric oracle: temperature outlier MUST be rejected and excluded from median.

    Arithmetic (all stations at dem=500 m == site 500 m; lapse term = 0 for all):
      raw values:       [20.0, 20.5, 20.2, 35.0]
      initial median:   sorted=[20.0,20.2,20.5,35.0] → (20.2+20.5)/2 = 20.35
      abs deviations:   [0.35, 0.15, 0.15, 14.65]
      MAD:              sorted=[0.15,0.15,0.35,14.65] → (0.15+0.35)/2 = 0.25
      temperature floor (0.5): effective MAD = max(0.25, 0.5) = 0.5
      band:             3.0 * 1.4826 * 0.5 = 2.2239
      |35.0 − 20.35| = 14.65 > 2.2239  → REJECTED
      inliers:          [20.0, 20.5, 20.2] → median([20.0,20.2,20.5]) = 20.2
    Mutation sensitivity:
      k→100 : band=74.13, 35.0 not rejected → rejected_stations=0 (fail) and
              value=median([20.0,20.5,20.2,35.0])=20.35≠20.2 (fail)
      MAD floor→0: band=1.11; 35.0 still rejected, inliers unchanged
      (LAPSE mutations caught by test_consensus_lapse_normalization_oracle)
    """
    readings = [
        StationReading(
            station_id=1,
            dem_elevation_m=500.0,
            variable="temperature",
            valid_at="2026-01-01T00:00:00Z",
            value=20.0,
        ),
        StationReading(
            station_id=2,
            dem_elevation_m=500.0,
            variable="temperature",
            valid_at="2026-01-01T00:00:00Z",
            value=20.5,
        ),
        StationReading(
            station_id=3,
            dem_elevation_m=500.0,
            variable="temperature",
            valid_at="2026-01-01T00:00:00Z",
            value=20.2,
        ),
        StationReading(
            station_id=4,
            dem_elevation_m=500.0,
            variable="temperature",
            valid_at="2026-01-01T00:00:00Z",
            value=35.0,
        ),
    ]
    result = compute_consensus(
        readings,
        variable="temperature",
        site_elevation_m=500.0,
        mad_floor=0.5,  # MAD_FLOORS["temperature"]
    )
    assert result is not None
    assert result.rejected_stations == 1
    assert result.n_stations == 3
    assert result.value == pytest.approx(20.2)


def test_consensus_lapse_normalization_oracle() -> None:
    """Numeric oracle: LAPSE=0.0065 K/m must shift normalized values and change median.

    Arithmetic:
      site elevation: 500 m
      Station A: dem=800 m, raw=17.0 °C
                 normalized = 17.0 + 0.0065*(800−500)
                            = 17.0 + 0.0065*300 = 17.0+1.95 = 18.95
      Station B: dem=200 m, raw=22.0 °C
                 normalized = 22.0 + 0.0065*(200−500)
                            = 22.0 + 0.0065*(−300) = 22.0−1.95 = 20.05
      Station C: dem=500 m, raw=20.5 °C
                 normalized = 20.5 + 0.0065*(500−500) = 20.5
      normalized values: [18.95, 20.05, 20.5]
      initial median:    sorted=[18.95, 20.05, 20.5] → 20.05
      abs deviations:    [1.1, 0.0, 0.45]
      MAD:               sorted=[0.0, 0.45, 1.1] → 0.45; floor→ effective MAD=0.5
      band:              3.0 * 1.4826 * 0.5 = 2.2239; all inliers
      consensus:         median([18.95, 20.05, 20.5]) = 20.05
    Without lapse (LAPSE=0):
      raw values [17.0, 22.0, 20.5] → median=20.5 ≠ 20.05 → RED
    """
    readings = [
        StationReading(
            station_id=1,
            dem_elevation_m=800.0,
            variable="temperature",
            valid_at="2026-01-01T00:00:00Z",
            value=17.0,
        ),
        StationReading(
            station_id=2,
            dem_elevation_m=200.0,
            variable="temperature",
            valid_at="2026-01-01T00:00:00Z",
            value=22.0,
        ),
        StationReading(
            station_id=3,
            dem_elevation_m=500.0,
            variable="temperature",
            valid_at="2026-01-01T00:00:00Z",
            value=20.5,
        ),
    ]
    result = compute_consensus(
        readings,
        variable="temperature",
        site_elevation_m=500.0,
        mad_floor=0.5,  # MAD_FLOORS["temperature"]
    )
    assert result is not None
    assert result.rejected_stations == 0
    assert result.n_stations == 3
    assert result.value == pytest.approx(20.05)


# materialize_consensus never deletes or rebuilds pairs/score_cache rows
# when an observation is written or corrected, so there is no
# delete-then-rebuild behavior to pin here.


def test_station_toggle_leaves_forecast_history_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "mutation-recompute.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> tuple[int, int, int]:
            set_setting(conn, "min_n", "1")
            site_id = int(
                conn.execute(
                    """
                    INSERT INTO sites
                        (name, forecast_lat, forecast_lon, elevation_m, timezone)
                    VALUES ('Recompute', 40, -105, 900, 'UTC')
                    """
                ).lastrowid
            )
            station_a = int(
                conn.execute(
                    """
                    INSERT INTO stations
                        (site_id, pws_station_id, lat, lon, dem_elevation_m)
                    VALUES (?, 'A', 40, -105, 900)
                    """,
                    (site_id,),
                ).lastrowid
            )
            station_b = int(
                conn.execute(
                    """
                    INSERT INTO stations
                        (site_id, pws_station_id, lat, lon, dem_elevation_m)
                    VALUES (?, 'B', 40, -105, 900)
                    """,
                    (site_id,),
                ).lastrowid
            )
            for station_id, value in ((station_a, 10.0), (station_b, 12.0)):
                insert_station_observation(
                    conn,
                    station_id=station_id,
                    variable="temperature",
                    valid_at="2035-01-02T00:00:00Z",
                    value=value,
                    source_raw=f"{value}C",
                )
            feed_id = int(
                conn.execute(
                    "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 1"
                ).fetchone()["id"]
            )
            conn.execute(
                """
                INSERT INTO observations
                    (site_id, variable, valid_at, value, n_stations)
                VALUES (?, 'precip', '2035-01-02T00:00:00Z', 0.4, 1)
                """,
                (site_id,),
            )
            for variable, forecast in (("temperature", 11.0), ("precip", 0.5)):
                conn.execute(
                    """
                    INSERT INTO forecast_samples
                        (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                         value, source_raw, model_run_id, fetched_at)
                    VALUES (?, ?, ?, '2035-01-01T00:00:00Z',
                            '2035-01-02T00:00:00Z', 24, ?, 'raw', ?,
                            '2035-01-01T00:00:00Z')
                    """,
                    (site_id, feed_id, variable, forecast, f"{variable}-run"),
                )
            # Fixture only: there is no inline pairing/scoring pass to build
            # this row, so plant the one forecast_history row the assertion
            # below reads directly, with the forecast value from the sample
            # above (11.0), rather than pin anything about how the row is
            # built.
            conn.execute(
                """
                INSERT INTO forecast_history
                    (site_id, feed_id, variable, day_ahead, valid_at, local_date,
                     value, lead_hours, issued_at, contributors)
                VALUES (?, ?, 'temperature', 1, '2035-01-02T00:00:00Z',
                        '2035-01-02', 11.0, 24, '2035-01-01T00:00:00Z', NULL)
                """,
                (site_id, feed_id),
            )
            return site_id, station_b, feed_id

        site_id, station_b, feed_id = db.write_sync(_seed)
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        station_response = client.put(
            f"/api/sites/{site_id}/stations/{station_b}",
            json={"enabled": False},
            headers=headers,
        )
        assert station_response.status_code == 200
        temperature_history = db.read_sync(
            lambda conn: conn.execute(
                """
                SELECT value
                FROM forecast_history
                WHERE site_id=? AND feed_id=? AND variable='temperature'
                """,
                (site_id, feed_id),
            ).fetchone()
        )
        # The toggle does not re-materialize or inline-score, so the history
        # row keeps its pre-toggle forecast value.
        assert temperature_history["value"] == pytest.approx(11.0)
        station_row = db.read_sync(
            lambda conn: conn.execute(
                "SELECT enabled FROM stations WHERE id=?", (station_b,)
            ).fetchone()
        )
        assert station_row["enabled"] == 0


# update_site only stores the rain threshold; it does not delete, re-pair
# or re-score anything, so there is no delete-and-rebuild behavior for
# PUT /api/sites/{id} to pin here.


def test_domain_backoff_records_defers_and_clears(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    request = httpx.Request("GET", "https://api.weather.com/v2/test")
    response = httpx.Response(429, headers={"Retry-After": "120"}, request=request)
    next_attempt_at = record_http_backoff(conn, response)
    assert next_attempt_at is not None
    with pytest.raises(JobDeferred):
        check_domain_backoff(conn, source_domain("weathercom"))
    clear_domain_backoff(conn, source_domain("weathercom"))
    check_domain_backoff(conn, source_domain("weathercom"))


def test_provider_http_errors_are_redacted_before_persisting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Redact', 40, -105, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )

    class FailingAdapter:
        def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
            request = httpx.Request(
                "GET",
                "https://provider.example/forecast?apikey=secret-value&model=x",
            )
            response = httpx.Response(400, request=request)
            raise httpx.HTTPStatusError(
                "bad provider response", request=request, response=response
            )

    def fake_build_adapter(source: str, client: httpx.AsyncClient) -> FailingAdapter:
        assert source == "open-meteo"
        assert client is not None
        return FailingAdapter()

    monkeypatch.setattr("wxverify.worker.processor.build_adapter", fake_build_adapter)
    dispatch_db = get_db()
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(
            dispatch(
                dispatch_db,
                FencedWriter(dispatch_db),
                Job(
                    id=41,
                    type="fetch_feed",
                    site_id=site_id,
                    job_key=f"fetch:{feed_id}",
                    payload={"feed_id": feed_id},
                    status="running",
                    retry_count=0,
                    max_retries=5,
                ),
            )
        )
    row = conn.execute(
        """
        SELECT last_error
        FROM site_feed_state
        WHERE site_id=? AND feed_id=?
        """,
        (site_id, feed_id),
    ).fetchone()
    assert row is not None
    assert "secret-value" not in row["last_error"]
    assert "apikey=%2A%2A%2A" in row["last_error"]


def test_api_guard_and_routes(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    close_db()
    config.db_path = str(tmp_path / "api.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")

    async def fake_validate_station(station_id: str, api_key: str) -> PwsStation:
        assert api_key == "secret-weather"
        return PwsStation(station_id=station_id, lat=40.1, lon=-104.9)

    async def fake_lookup_elevation_m(lat: float, lon: float) -> float:
        assert (lat, lon) == (40.1, -104.9)
        return 910.0

    monkeypatch.setattr(
        "wxverify.api.routes.stations.validate_station", fake_validate_station
    )
    monkeypatch.setattr(
        "wxverify.api.routes.stations.lookup_elevation_m", fake_lookup_elevation_m
    )
    # network guard: idle the worker so the lifespan never reaches a provider
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        site = client.post(
            "/api/sites",
            json={
                "name": "API",
                "forecast_lat": 40.0,
                "forecast_lon": -105.0,
                "elevation_m": 900.0,
                "timezone": "UTC",
            },
            headers=headers,
        )
        assert site.status_code == 200
        site_id = site.json()["id"]

        bad_timezone = client.post(
            "/api/sites",
            json={
                "name": "Bad TZ",
                "forecast_lat": 40.0,
                "forecast_lon": -105.0,
                "elevation_m": 900.0,
                "timezone": "Not/AZone",
            },
            headers=headers,
        )
        assert bad_timezone.status_code == 422

        rejected_identity = client.put(
            f"/api/sites/{site_id}",
            json={"forecast_lat": 41.0},
            headers=headers,
        )
        assert rejected_identity.status_code == 422

        station = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": "TEST1"},
            headers=headers,
        )
        assert station.status_code == 200
        assert station.json()["dem_elevation_m"] == 910.0
        rejected_station_identity = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": "TEST2", "lat": 40.1},
            headers=headers,
        )
        assert rejected_station_identity.status_code == 422

        cross = client.post(
            "/api/catchup",
            headers={"Origin": "https://evil.example", "X-CSRF-Token": csrf},
        )
        assert cross.status_code == 403
        simple = client.put(
            f"/api/sites/{site_id}",
            content="enabled=true",
            headers={
                "Origin": "http://testserver",
                "X-CSRF-Token": csrf,
                "Content-Type": "text/plain",
            },
        )
        assert simple.status_code == 415
        # Contract (0.8.1): a body with NO declared Content-Type passes the
        # content-type gate and reaches the handler -- it is protected by the
        # same-origin check + CSRF double-submit only. (HA ingress forwards
        # bodyless POSTs as chunked with Content-Length stripped and no
        # Content-Type; 0.8.0 wrongly 415'd those.) The allowlist still rejects
        # a DISALLOWED content-type that IS declared -- see the `text/plain`
        # PUT above, which stays 415: that is this gate's paired negative.
        missing_type_body = client.put(
            f"/api/sites/{site_id}",
            content=b'{"enabled": true}',
            headers={
                "Origin": "http://testserver",
                "X-CSRF-Token": csrf,
            },
        )
        assert missing_type_body.status_code == 200
        missing_csrf = client.post(
            "/api/catchup", headers={"Origin": "http://testserver"}
        )
        assert missing_csrf.status_code == 403

        keys = client.get("/api/health/keys").json()
        assert keys["weathercom"] is True
        assert "secret-weather" not in client.get("/api/health/keys").text


def test_health_feeds_reports_disabled_site_no_data_and_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "health-feeds.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> tuple[int, int, int, int, int, int]:
            active_site_id = int(
                conn.execute(
                    """
                    INSERT INTO sites
                        (name, forecast_lat, forecast_lon, elevation_m, timezone)
                    VALUES ('Active Health', 40, -105, 900, 'UTC')
                    """
                ).lastrowid
            )
            disabled_site_id = int(
                conn.execute(
                    """
                    INSERT INTO sites
                        (name, forecast_lat, forecast_lon, elevation_m, timezone,
                         enabled)
                    VALUES ('Disabled Health', 40, -105, 900, 'UTC', 0)
                    """
                ).lastrowid
            )
            open_meteo_feed_id = int(
                conn.execute(
                    "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 1"
                ).fetchone()["id"]
            )
            error_feed_id = int(
                conn.execute(
                    """
                    SELECT id FROM feeds
                    WHERE source='open-meteo'
                    ORDER BY id
                    LIMIT 1 OFFSET 1
                    """
                ).fetchone()["id"]
            )
            meteoblue_package_id = int(
                conn.execute(
                    """
                    SELECT id FROM feeds
                    WHERE source='meteoblue' AND model='multimodel'
                    """
                ).fetchone()["id"]
            )
            meteoblue_member_id = int(
                conn.execute(
                    """
                    INSERT INTO feeds
                        (source, model, enabled, default_subscribed,
                         fetch_interval_minutes, max_lead_hours, is_virtual)
                    VALUES ('meteoblue', 'NEMS4', 1, 0, 1440, 168, 0)
                    """
                ).lastrowid
            )
            conn.execute(
                """
                INSERT INTO site_feed_state
                    (site_id, feed_id, last_run_at, error_count)
                VALUES (?, ?, '2035-01-01T00:00:00Z', 0)
                """,
                (active_site_id, open_meteo_feed_id),
            )
            conn.execute(
                """
                INSERT INTO site_feed_state
                    (site_id, feed_id, last_run_at, last_error, error_count)
                VALUES (?, ?, '2035-01-01T00:00:00Z', 'provider boom', 1)
                """,
                (active_site_id, error_feed_id),
            )
            conn.execute(
                """
                INSERT INTO site_feed_state
                    (site_id, feed_id, enabled, last_run_at, error_count)
                VALUES (?, ?, 1, '2035-01-01T00:00:00Z', 0)
                """,
                (active_site_id, meteoblue_package_id),
            )
            conn.execute(
                """
                INSERT INTO forecast_samples
                    (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                     value, source_raw, model_run_id, fetched_at)
                VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z',
                        '2035-01-01T01:00:00Z', 1, 5.0, '5 C', 'NEMS4:run',
                        '2035-01-01T00:05:00Z')
                """,
                (active_site_id, meteoblue_member_id),
            )
            return (
                active_site_id,
                disabled_site_id,
                open_meteo_feed_id,
                error_feed_id,
                meteoblue_package_id,
            )

        (
            active_site_id,
            disabled_site_id,
            open_meteo_feed_id,
            error_feed_id,
            meteoblue_package_id,
        ) = db.write_sync(_seed)
        response = client.get("/api/health/feeds")
        assert response.status_code == 200
        by_site_feed = {
            (int(row["site_id"]), int(row["feed_id"])): row for row in response.json()
        }
        active_open_meteo = by_site_feed[(active_site_id, open_meteo_feed_id)]
        disabled_open_meteo = by_site_feed[(disabled_site_id, open_meteo_feed_id)]
        errored_feed = by_site_feed[(active_site_id, error_feed_id)]
        active_meteoblue = by_site_feed[(active_site_id, meteoblue_package_id)]
        assert active_open_meteo["status"] == "ran / no usable data"
        assert active_open_meteo["sample_count"] == 0
        assert disabled_open_meteo["status"] == "site disabled"
        assert disabled_open_meteo["site_enabled"] is False
        assert errored_feed["status"] == "error"
        assert errored_feed["last_error"] == "provider boom"
        assert errored_feed["sample_count"] == 0
        assert active_meteoblue["status"] == "ok"
        assert active_meteoblue["sample_count"] == 1

        ops = client.get("/ops")
        assert ops.status_code == 200
        assert "site disabled" in ops.text
        assert "ran / no usable data" in ops.text
        assert "provider boom" in ops.text
        assert "Meteoblue multimodel package" in ops.text
        assert "no enabled stations" in ops.text


def test_health_budget_reports_current_billing_day_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "health-budget.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            today = current_billing_day("UTC")
            conn.execute(
                """
                INSERT INTO api_budget (source, billing_day, calls, credits)
                VALUES ('open-meteo', '1999-01-01', 99, 0)
                """
            )
            conn.execute(
                """
                INSERT INTO api_budget (source, billing_day, calls, credits)
                VALUES ('open-meteo', ?, 3, 0)
                """,
                (today,),
            )

        db.write_sync(_seed)
        response = client.get("/api/health/budget")
        assert response.status_code == 200
        rows = response.json()
        open_meteo_rows = [row for row in rows if row["source"] == "open-meteo"]
        assert len(open_meteo_rows) == 1
        assert open_meteo_rows[0]["calls"] == 3

        ops = client.get("/ops")
        assert ops.status_code == 200
        assert "99 / 10000 calls" not in ops.text
        assert "3 / 10000 calls" in ops.text


def test_site_create_does_not_require_weather_key_but_station_does(
    tmp_path: Path, monkeypatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "nokey.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.delenv("WXV_WEATHERCOM_KEY", raising=False)
    # network guard: idle the worker so the lifespan never reaches a provider
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        site = client.post(
            "/api/sites",
            json={
                "name": "No key",
                "forecast_lat": 40.0,
                "forecast_lon": -105.0,
                "elevation_m": 900.0,
                "timezone": "UTC",
            },
            headers=headers,
        )
        assert site.status_code == 200
        station = client.post(
            f"/api/sites/{site.json()['id']}/stations",
            json={"pws_station_id": "NO_KEY"},
            headers=headers,
        )
        assert station.status_code == 503


def test_ui_root_path_htmx_and_create_site_fragment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "ui.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    # network guard: idle the worker so the lifespan never reaches a provider
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="/ingress/path/")
    with TestClient(app) as client:
        root = client.get("/", follow_redirects=False)
        assert root.status_code == 200
        assert "<h1>Forecast</h1>" in root.text
        page = client.get("/sites")
        assert page.status_code == 200
        html = page.text
        assert f'src="/ingress/path/static/{__version__}/htmx.min.js"' in html
        assert f'src="/ingress/path/static/{__version__}/htmx-ext-json-enc.js"' in html
        assert 'class="brand" href="/ingress/path/"' in html
        assert 'src="https://' not in html
        assert (
            "Session expired or request blocked — reload the page to continue" in html
        )
        assert "Create site" in html
        assert 'hx-post="/ingress/path/api/sites"' in html
        assert "method=" not in re.search(
            r"<form[^>]+id=\"site-create-form\"[^>]*>", html
        ).group(0)
        assert "Path=/ingress/path" in page.headers["set-cookie"]

        parser = _AttrParser()
        parser.feed(html)
        url_attrs = {"src", "href", "hx-post", "hx-put", "hx-delete", "data-src"}
        for _tag, attrs in parser.tags:
            for attr in url_attrs:
                value = attrs.get(attr)
                if value and value.startswith("/"):
                    assert value.startswith("/ingress/path/"), value
                    assert "//" not in value.removeprefix("/ingress/path"), value
            mutating = any(name in attrs for name in ("hx-post", "hx-put", "hx-delete"))
            if mutating:
                assert attrs.get("hx-ext") == "json-enc"
                assert "X-CSRF-Token" in attrs.get("hx-headers", "")

        token = re.search(r'<meta name="csrf-token" content="([^"]+)">', html).group(1)
        created = client.post(
            "/api/sites",
            json={
                "name": "Ingress Site",
                "forecast_lat": 40.0,
                "forecast_lon": -105.0,
                "elevation_m": 900.0,
                "timezone": "Europe/Athens",
            },
            headers={
                "Origin": "http://testserver",
                "X-CSRF-Token": token,
                "HX-Request": "true",
                "Cookie": f"csrf={page.cookies['csrf']}",
            },
        )
        assert created.status_code == 200
        assert created.headers["content-type"].startswith("text/html")
        assert "set-cookie" not in created.headers
        assert "Ingress Site" in created.text
        assert "NOAA GFS global model." in created.text
        assert "DWD ICON global model." in created.text
        assert "Multimodel package via one API call." in created.text
        second = client.post(
            "/api/sites",
            json={
                "name": "Second Ingress Site",
                "forecast_lat": 40.2,
                "forecast_lon": -104.8,
                "elevation_m": 910.0,
                "timezone": "Europe/Athens",
            },
            headers={
                "Origin": "http://testserver",
                "X-CSRF-Token": token,
                "HX-Request": "true",
                "Cookie": f"csrf={page.cookies['csrf']}",
            },
        )
        assert second.status_code == 200


def test_ui_dashboard_ops_overlay_smoke_and_key_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    config.standalone_origin = None
    monkeypatch.delenv("WXV_METEOBLUE_KEY", raising=False)
    monkeypatch.delenv("WXV_ROLLING_WINDOW_DAYS", raising=False)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Smoke', 40, -105, 900, 'UTC')
            """
        ).lastrowid
    )
    disabled_site_id = int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES ('Paused Smoke', 41, -104, 910, 'UTC', 0)
            """
        ).lastrowid
    )
    conn.execute("UPDATE settings SET value='14' WHERE key='rolling_window_days'")
    conn.commit()
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' LIMIT 1"
        ).fetchone()["id"]
    )
    # network guard: idle the worker so the lifespan never reaches a provider
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        dashboard = client.get(f"/dashboard?site={site_id}")
        assert dashboard.status_code == 200
        assert "Last 14 days" in dashboard.text
        assert "lead=D%2B1" in dashboard.text
        assert 'aria-label="Lead time"' in dashboard.text
        assert ">Today<" in dashboard.text
        assert ">+7 days<" in dashboard.text
        assert "/api/curve?site=" in dashboard.text
        decoded_plus_dashboard = client.get(
            f"/dashboard?site={site_id}&variable=precip&window=rolling&lead=D+1"
        )
        assert decoded_plus_dashboard.status_code == 200
        assert "Paused Smoke" not in dashboard.text
        disabled_dashboard = client.get(f"/dashboard?site={disabled_site_id}")
        assert disabled_dashboard.status_code == 200
        assert "Paused Smoke - paused - Temperature" in disabled_dashboard.text

        ops = client.get("/ops")
        assert ops.status_code == 200
        assert "weathercom" in ops.text
        assert "present" in ops.text
        assert "not subscribed / available" in ops.text

        overlay = client.get(f"/overlay?site={site_id}&feed_id={feed_id}")
        assert overlay.status_code == 200
        assert f"/api/timeseries?site={site_id}" in overlay.text
        assert "Paused Smoke" not in overlay.text
        disabled_overlay = client.get(
            f"/overlay?site={disabled_site_id}&feed_id={feed_id}"
        )
        assert disabled_overlay.status_code == 200
        assert "Paused Smoke - paused - forecast vs observed" in disabled_overlay.text


def test_open_meteo_trace_negative_precip_clamp_preserves_source_raw() -> None:
    # Precip is labelled by the start of its hour: the raw
    # 01:00 reading becomes lead 0 against the 2026-01-01T00:00:00Z issue
    # time and is dropped, so label 01:00Z now holds the raw 02:00 value and
    # there is no 03:00Z precip label at all.
    samples = open_meteo_samples_from_hourly(
        "jma_gsm",
        "2026-01-01T00:00:00Z",
        {
            "hourly": {
                "time": [
                    "2026-01-01T01:00",
                    "2026-01-01T02:00",
                    "2026-01-01T03:00",
                ],
                "precipitation": [-0.1, -0.01, -0.2],
            }
        },
    )

    precip = {sample.valid_at: sample for sample in samples}
    assert "2026-01-01T00:00:00Z" not in precip
    assert precip["2026-01-01T01:00:00Z"].value == 0.0
    assert precip["2026-01-01T01:00:00Z"].source_raw == "-0.01"
    assert precip["2026-01-01T02:00:00Z"].value == -0.2
    assert precip["2026-01-01T02:00:00Z"].source_raw == "-0.2"
    assert "2026-01-01T03:00:00Z" not in precip


def test_meteoblue_parser_and_member_registration(
    tmp_path: Path,
) -> None:
    async def _run() -> None:
        open_meteo = open_meteo_samples_from_hourly(
            "ecmwf_ifs",
            "2026-01-01T00:00:00Z",
            {
                "hourly": {
                    "time": ["2026-01-01T01:00"],
                    "temperature_2m": [2.0],
                    "wind_speed_10m": [36.0],
                    "precipitation": [0.2],
                }
            },
        )
        open_values = {sample.variable: sample.value for sample in open_meteo}
        assert open_values["temperature"] == 2.0
        assert open_values["wind"] == pytest.approx(10.0)
        # Precip is labelled by the start of its hour: the raw
        # 01:00 reading becomes lead 0 against the 2026-01-01T00:00:00Z
        # issue time and is dropped, so there is no forward precip sample.
        assert "precip" not in open_values
        assert {sample.valid_at for sample in open_meteo} == {"2026-01-01T01:00:00Z"}

        meteoblue_payload = {
            "metadata": {
                "models": ["gfs", "icon"],
                "modelrun_utc": ["2026-01-01 00:00", "2026-01-01 00:00"],
                "latitude": 40.05,
                "longitude": -104.95,
                "height": 910.0,
            },
            "multimodel": {
                "data_1h": {
                    "time": ["2026-01-01 01:00", "2026-01-01 02:00"],
                    "temperature": [[1.0, 2.0], [3.0, None]],
                    "windspeed": [[36.0, 18.0], [9.0, 9.0]],
                    "precipitation": [[0.0, 0.2], [0.1, 0.0]],
                }
            },
        }

        def meteoblue_handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/packages/multimodel-1h"
            assert request.url.params["tz"] == "utc"
            return httpx.Response(200, json=meteoblue_payload)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(meteoblue_handler)
        ) as client:
            result = await MeteoblueAdapter("key", client).fetch_forecast(
                ForecastRequest(
                    lat=40,
                    lon=-105,
                    model="multimodel",
                    variables=("temperature", "wind", "precip"),
                    max_lead_hours=168,
                )
            )

        assert result.grid is not None
        assert result.grid.grid_elevation_m == 910.0
        assert {sample.model for sample in result.samples} == {"gfs", "icon"}
        assert [
            sample.value
            for sample in result.samples
            if sample.model == "gfs" and sample.variable == "temperature"
        ] == [1.0, 2.0]
        assert [
            sample.value
            for sample in result.samples
            if sample.model == "gfs" and sample.variable == "wind"
        ] == pytest.approx([10.0, 5.0])
        assert all(sample.model_run_id for sample in result.samples)

        conn = _init_tmp_db(tmp_path)
        site_id = int(
            conn.execute(
                """
                INSERT INTO sites
                    (name, forecast_lat, forecast_lon, elevation_m, timezone)
                VALUES ('M7', 40, -105, 900, 'UTC')
                """
            ).lastrowid
        )
        package_feed_id = int(
            conn.execute(
                """
                SELECT id FROM feeds
                WHERE source='meteoblue' AND model='multimodel'
                """
            ).fetchone()["id"]
        )
        persist_fetch_result(
            conn,
            site_id=site_id,
            source="meteoblue",
            fetch_feed_id=package_feed_id,
            result=result,
        )
        members = {
            row["model"]: row
            for row in conn.execute(
                """
                SELECT model, enabled, is_virtual
                FROM feeds
                WHERE source='meteoblue' AND model!='multimodel'
                """
            )
        }
        assert set(members) == {"gfs", "icon"}
        assert all(
            row["enabled"] == 1 and row["is_virtual"] == 0 for row in members.values()
        )
        package_state = conn.execute(
            """
            SELECT grid_lat, grid_lon, grid_elevation_m
            FROM site_feed_state
            WHERE site_id=? AND feed_id=?
            """,
            (site_id, package_feed_id),
        ).fetchone()
        assert package_state["grid_elevation_m"] == 910.0

    asyncio.run(_run())


def test_options_boot_apply_bad_options_and_packaging_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    db_path = tmp_path / "boot.db"
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps(
            {
                "rolling_window_days": 14,
                "min_n": 50,
                "obs_interval_minutes": 240,
                "obs_jitter_minutes": 35,
            }
        ),
        encoding="utf-8",
    )
    config.db_path = str(db_path)
    config.options_path = str(options_path)
    conn = init_db(str(db_path))._conn  # noqa: SLF001 - tests seed boot state
    conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('Boot', 40, -105, 900, 'UTC')
        """
    )
    # network guard: idle the worker so the lifespan never reaches a provider
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    with TestClient(create_app(root_path="")) as client:
        assert client.get("/api/csrf").status_code == 200
        rows = {
            row["key"]: row["value"]
            for row in get_db().read_sync(
                lambda db_conn: db_conn.execute(
                    "SELECT key, value FROM settings"
                ).fetchall()
            )
        }
        assert rows["rolling_window_days"] == "14"
        assert rows["min_n"] == "50"
        assert rows["obs_interval_minutes"] == "240"
        assert rows["obs_jitter_minutes"] == "35"

    malformed = tmp_path / "bad-options.json"
    malformed.write_text('{"min_n": 50,', encoding="utf-8")
    config.options_path = str(malformed)
    with pytest.raises(json.JSONDecodeError):
        from wxverify.core.options import load_runtime_config

        load_runtime_config()

    invalid = tmp_path / "invalid-options.json"
    invalid.write_text(json.dumps({"min_n": -5}), encoding="utf-8")
    config.options_path = str(invalid)
    with pytest.raises(ValueError):
        from wxverify.core.options import load_runtime_config

        load_runtime_config()

    repo = Path(__file__).resolve().parents[1]
    config_yaml = (repo / "config.yaml").read_text(encoding="utf-8")
    assert "meteoblue_key: password?" in config_yaml
    assert "weathercom_key: password?" in config_yaml
    assert "rolling_window_days: int(1,3650)" in config_yaml

    dockerfile = (repo / "Dockerfile").read_text(encoding="utf-8")
    assert "uv==0.9.17" in dockerfile
    assert "uv export --frozen" in dockerfile
    assert "--only-binary=:all:" in dockerfile
    assert "uvicorn[standard]" not in dockerfile

    # wxverify is wired into monorepo CI in the bundled de-nest PR.
    # builder.yaml MONITORED_FILES includes wxverify, pyproject.toml, and uv.lock;
    # checks.yaml carries the wxverify-gates job (lint.yaml only calls
    # checks.yaml on the nightly schedule).
    workflow_dir = repo.parent / ".github" / "workflows"
    builder = (workflow_dir / "builder.yaml").read_text(encoding="utf-8")
    checks = (workflow_dir / "checks.yaml").read_text(encoding="utf-8")
    assert "wxverify" in builder
    assert "pyproject.toml" in builder
    assert "uv.lock" in builder
    assert "wxverify-gates" in checks


# ---------------------------------------------------------------------------
# Add-on contract / oracle net
# ---------------------------------------------------------------------------


def test_health_backoffs_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """GET /api/health/backoffs: empty→[], single row returned exactly, multiple rows
    ordered by next_attempt_at ASC."""
    close_db()
    config.db_path = str(tmp_path / "backoffs.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        resp = client.get("/api/health/backoffs")
        assert resp.status_code == 200
        assert resp.json() == []

        def _seed(conn: sqlite3.Connection) -> None:
            conn.executemany(
                """
                INSERT INTO domain_backoffs (domain, next_attempt_at, retry_count)
                VALUES (?, ?, ?)
                """,
                [
                    ("api.example.com", "2035-01-01T02:00:00Z", 3),
                    ("weather.example.com", "2035-01-01T01:00:00Z", 1),
                ],
            )

        db.write_sync(_seed)

        resp = client.get("/api/health/backoffs")
        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 2
        assert rows[0] == {
            "domain": "weather.example.com",
            "next_attempt_at": "2035-01-01T01:00:00Z",
            "retry_count": 1,
        }
        assert rows[1] == {
            "domain": "api.example.com",
            "next_attempt_at": "2035-01-01T02:00:00Z",
            "retry_count": 3,
        }


def test_health_backoffs_survives_unreadable_stored_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """GET /api/health/backoffs with one well-formed row and one whose stored
    retry_count is TEXT (bindable into an INTEGER column; only an imported or
    hand-edited database carries it): 200, BOTH rows in the body, the readable
    count exact and the unreadable one null.  The two-row assertion rejects the
    tempting wrong fix of filtering the malformed row out of the response --
    this route exists to show the operator the broken record, and null (not a
    fabricated 0) is the honest rendering of a value the database does not
    contain."""
    close_db()
    config.db_path = str(tmp_path / "backoffs-unreadable.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> None:
            conn.executemany(
                """
                INSERT INTO domain_backoffs (domain, next_attempt_at, retry_count)
                VALUES (?, ?, ?)
                """,
                [
                    ("api.example.com", "2035-01-01T01:00:00Z", 3),
                    ("weather.example.com", "2035-01-01T02:00:00Z", "abc"),
                ],
            )

        db.write_sync(_seed)

        resp = client.get("/api/health/backoffs")
        assert resp.status_code == 200
        rows = resp.json()
        assert len(rows) == 2
        by_domain = {row["domain"]: row for row in rows}
        assert by_domain["api.example.com"]["retry_count"] == 3
        assert by_domain["weather.example.com"]["retry_count"] is None


def test_worker_status_pins_api_prefix_empty_queue_and_null_pair_score(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Worker-status regression pins:
    - /worker/status (no /api prefix) → 404
    - empty jobs table → {"jobs": {}} (keys absent, not 0)
    - no active publication → last_completed_pair_and_score_at is null
    """
    close_db()
    config.db_path = str(tmp_path / "ws-pins.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        assert client.get("/worker/status").status_code == 404

        payload = client.get("/api/worker/status").json()
        assert payload["jobs"] == {}
        assert payload["last_completed_pair_and_score_at"] is None


def test_csrf_garbage_token_rejects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CSRF negative triple item (2): wrong/garbage token → 403.

    Items (1) no header and (3) correct token are already pinned in
    test_api_guard_and_routes. This test closes the gap for item (2).
    """
    close_db()
    config.db_path = str(tmp_path / "csrf-garbage.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        client.get("/api/csrf")
        resp = client.post(
            "/api/catchup",
            headers={
                "Origin": "http://testserver",
                "X-CSRF-Token": "garbage.notavalidtoken",
            },
        )
        assert resp.status_code == 403


def test_translations_key_parity() -> None:
    """translations/en.yaml configuration keys == config.yaml options keys.

    Uses a minimal line parser; PyYAML is not a project dependency.
    """
    repo_root = Path(__file__).resolve().parents[1]

    config_text = (repo_root / "config.yaml").read_text(encoding="utf-8")
    config_keys: set[str] = set()
    in_options = False
    for line in config_text.splitlines():
        stripped = line.rstrip()
        if stripped == "options:":
            in_options = True
            continue
        if in_options:
            m = re.match(r"^  (\w+):", stripped)
            if m:
                config_keys.add(m.group(1))
            elif stripped and not stripped.startswith(" "):
                break

    trans_text = (repo_root / "translations" / "en.yaml").read_text(encoding="utf-8")
    trans_keys: set[str] = set()
    in_configuration = False
    for line in trans_text.splitlines():
        stripped = line.rstrip()
        if stripped == "configuration:":
            in_configuration = True
            continue
        if in_configuration:
            m = re.match(r"^  (\w+):", stripped)
            if m:
                trans_keys.add(m.group(1))
            elif stripped and not stripped.startswith(" "):
                break

    assert config_keys, "no options keys found in config.yaml"
    assert trans_keys, "no configuration keys found in translations/en.yaml"
    assert config_keys == trans_keys, (
        f"key mismatch — in config.yaml only: {config_keys - trans_keys!r}; "
        f"in translations only: {trans_keys - config_keys!r}"
    )
