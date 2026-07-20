from __future__ import annotations

import asyncio
import contextlib
import errno
import json
import math
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
from wxverify.api.routes.feeds import _rebuild_mean_for_site
from wxverify.collection.budget import current_billing_day
from wxverify.collection.forecast_fetcher import persist_fetch_result
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.queue import (
    Job,
    claim_next_job,
    complete,
    enqueue_if_absent,
    reclaim_all_stale,
)
from wxverify.feeds.meteoblue import MeteoblueAdapter
from wxverify.feeds.open_meteo import (
    OpenMeteoAdapter,
)
from wxverify.feeds.open_meteo import (
    _samples_from_hourly as open_meteo_samples_from_hourly,
)
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastRequest,
    GridProvenance,
    NormalizedSample,
)
from wxverify.obs.config import RECENT_REFRESH_HOURS
from wxverify.obs.pws_adapter import (
    PwsObservation,
    PwsStation,
    fetch_hourly_history_range,
    observations_from_payload,
)
from wxverify.scoring.cache import ScoreCacheRow, is_cache_fresh
from wxverify.scoring.composite import composite
from wxverify.scoring.consensus import (
    StationReading,
    compute_consensus,
    insert_station_observation,
)
from wxverify.scoring.engine import pair_and_score
from wxverify.scoring.leaderboard import (
    below_baseline,
    leaderboard,
    leaderboard_with_status,
    score_badge,
)
from wxverify.scoring.winrate import winrate
from wxverify.settings.keys import set_setting
from wxverify.settings.service import set_rolling_window_days_sync
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
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
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
    assert open_meteo["n"] == 7
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
        "score_cache": {
            "site_id",
            "feed_id",
            "variable",
            "day_ahead",
            "window_key",
            "n",
            "computed_at",
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
        "forecast_pairs": {
            "site_id",
            "feed_id",
            "variable",
            "issued_at",
            "valid_at",
            "day_ahead",
            "lead_hours",
            "forecast",
            "observed",
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
        VALUES ('Test', 47.0, 25.0, 900.0, 'Europe/Bucharest')
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
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed)
            VALUES (?, ?, 'temperature', '2026-01-01T00:00:00Z',
                    '2026-01-01T01:00:00Z', 1, 8, 7.0, 6.0)
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
                    VALUES ('Runtime', 47, 25, 900, 'UTC')
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
    assert payload["last_completed_pair_and_score_at"] == "2035-01-01T03:00:00.000Z"


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
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    class ReachedClaim(Exception):
        pass

    class FakeDb:
        async def write(self, fn):  # type: ignore[no-untyped-def]
            return fn(None)

    def fail_heartbeat(conn: object, key: str) -> None:
        raise sqlite3.OperationalError(f"heartbeat failed {key}")

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


def test_worker_permission_error_is_process_fatal(
    monkeypatch: pytest.MonkeyPatch,
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
        async def write(self, fn):  # type: ignore[no-untyped-def]
            return fn(None)

    def claim_once(conn: object) -> Job:
        return job

    async def raise_permission_error(db: object, claimed: Job) -> None:
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
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
    # The worker crashes during the lifespan yield.  When the TestClient context
    # exits, the lifespan finally-block awaits the already-failed task and
    # re-raises its exception — wrap that expected shutdown error here so the
    # real oracle (callback_fired) can be checked cleanly afterward.
    with pytest.raises(RuntimeError, match="worker crash"), TestClient(app):
        callback_fired.wait(timeout=2.0)
    assert callback_fired.is_set(), (
        "add_done_callback registration is missing: stop_process was never called"
    )


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


def test_v1_migration_adds_backfill_columns(tmp_path: Path) -> None:
    close_db()
    db_path = tmp_path / "wxverify-v1.db"
    raw = sqlite3.connect(db_path)
    raw.execute(
        """
        CREATE TABLE sites (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            forecast_lat REAL NOT NULL,
            forecast_lon REAL NOT NULL,
            elevation_m REAL NOT NULL,
            timezone TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
            rain_threshold_mm REAL NOT NULL DEFAULT 0.2 CHECK(rain_threshold_mm >= 0),
            last_obs_at TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        )
        """
    )
    raw.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('Old', 47, 25, 900, 'UTC')
        """
    )
    raw.execute("PRAGMA user_version = 1")
    raw.commit()
    raw.close()

    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001 - tests inspect the real writer connection
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(sites)")}
    assert {"backfill_status", "backfill_through"} <= columns
    row = conn.execute("SELECT backfill_status, backfill_through FROM sites").fetchone()
    assert row["backfill_status"] == "pending"
    assert row["backfill_through"] is None
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 3


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
        VALUES ('Q', 47, 25, 900, 'UTC')
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


def test_pws_parser_and_fetch_obs_refresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")
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
        ("wind", "2026-01-01T01:00:00Z"),
        ("precip", "2026-01-01T01:00:00Z"),
    ]
    assert parsed[0].value == 6.5
    assert parsed[1].value == pytest.approx(10.0)
    assert parsed[2].value == 0.3
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

    def history_range_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v2/pws/history/hourly"
        assert request.url.params["stationId"] == "OBS1"
        assert request.url.params["format"] == "json"
        assert request.url.params["units"] == "m"
        assert request.url.params["startDate"] == "20260622"
        assert request.url.params["endDate"] == "20260624"
        assert request.url.params["numericPrecision"] == "decimal"
        return httpx.Response(
            200,
            json={
                "observations": [
                    {
                        "obsTimeUtc": "2026-06-21T23:59:00Z",
                        "metric": {"tempAvg": 1.0},
                    },
                    {
                        "obsTimeUtc": "2026-06-22T00:15:00Z",
                        "metric": {
                            "tempAvg": 2.0,
                            "windspeedAvg": 36.0,
                            "precipTotal": 0.2,
                        },
                    },
                    {
                        "obsTimeUtc": "2026-06-24T00:00:00Z",
                        "metric": {"tempAvg": 3.0},
                    },
                ]
            },
        )

    async def read_history_range() -> list[PwsObservation]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(history_range_handler)
        ) as client:
            return await fetch_hourly_history_range(
                "OBS1",
                "secret-weather",
                window_start="2026-06-22T00:00:00Z",
                window_end="2026-06-24T00:00:00Z",
                timezone="Europe/Bucharest",
                client=client,
            )

    ranged = asyncio.run(read_history_range())
    assert [(row.variable, row.valid_at, row.value) for row in ranged] == [
        ("temperature", "2026-06-22T00:00:00Z", 2.0),
        ("wind", "2026-06-22T00:00:00Z", pytest.approx(10.0)),
        ("precip", "2026-06-22T00:00:00Z", 0.2),
    ]

    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Obs', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    conn.execute(
        """
        INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)
        VALUES (?, 'OBS1', 47, 25, 900)
        """,
        (site_id,),
    )

    async def fake_history(
        station_id: str,
        api_key: str,
        *,
        hours: int,
        timezone: str | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> list[PwsObservation]:
        assert station_id == "OBS1"
        assert api_key == "secret-weather"
        assert hours == RECENT_REFRESH_HOURS == 6
        assert timezone == "UTC"
        assert client is not None
        return [
            PwsObservation(
                variable="temperature",
                valid_at="2026-06-24T01:00:00Z",
                value=5.0,
                source_raw="5.0 C",
            )
        ]

    monkeypatch.setattr("wxverify.worker.processor.fetch_hourly_history", fake_history)
    asyncio.run(
        dispatch(
            get_db(),
            Job(
                id=1,
                type="fetch_obs",
                site_id=site_id,
                job_key="obs",
                payload={},
                status="running",
                retry_count=0,
                max_retries=5,
            ),
        )
    )
    raw = conn.execute(
        """
        SELECT value FROM station_observations
        WHERE variable='temperature' AND valid_at='2026-06-24T01:00:00Z'
        """
    ).fetchone()
    assert raw["value"] == 5.0
    consensus = conn.execute(
        "SELECT value, n_stations FROM observations WHERE site_id=?", (site_id,)
    ).fetchone()
    assert consensus["value"] == 5.0
    assert consensus["n_stations"] == 1
    budget = conn.execute(
        "SELECT calls FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    assert budget["calls"] == 1
    site = conn.execute(
        "SELECT last_obs_at FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    assert site["last_obs_at"] is not None
    queued = conn.execute(
        """
        SELECT type, site_id FROM jobs
        WHERE type='pair_and_score' AND status='pending'
        """
    ).fetchone()
    assert queued["site_id"] == site_id

    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' LIMIT 1"
        ).fetchone()["id"]
    )
    seen: dict[str, object] = {}

    class FakeForecastAdapter:
        supports_historical = True

        def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
            seen["request"] = req
            assert req.lat == 47
            assert req.lon == 25
            assert req.variables == ("temperature", "wind", "precip")
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
            return FetchResult(
                samples=[
                    NormalizedSample(
                        model=req.model,
                        variable="temperature",
                        issued_at="2026-06-24T00:00:00Z",
                        valid_at="2026-06-24T02:00:00Z",
                        lead_hours=2,
                        value=8.0,
                        source_raw="8.0 C",
                        model_run_id=f"{req.model}:2026-06-24T00:00:00Z",
                    )
                ],
                grid=GridProvenance(
                    grid_lat=47.01,
                    grid_lon=25.02,
                    grid_elevation_m=901.0,
                ),
            )

        async def fetch_historical(
            self, req: ForecastRequest, *, window_start: str, window_end: str
        ) -> FetchResult | None:
            return None

    def fake_build_adapter(
        source: str, client: httpx.AsyncClient
    ) -> FakeForecastAdapter:
        assert source == "open-meteo"
        assert client is not None
        seen["source"] = source
        return FakeForecastAdapter()

    monkeypatch.setattr("wxverify.worker.processor.build_adapter", fake_build_adapter)
    asyncio.run(
        dispatch(
            get_db(),
            Job(
                id=2,
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
    state = conn.execute(
        """
        SELECT last_run_at, last_error, error_count
             , grid_lat, grid_lon, grid_elevation_m
        FROM site_feed_state
        WHERE site_id=? AND feed_id=?
        """,
        (site_id, feed_id),
    ).fetchone()
    assert state["last_run_at"] is not None
    assert state["last_error"] is None
    assert state["error_count"] == 0
    assert state["grid_lat"] == pytest.approx(47.01)
    assert state["grid_lon"] == pytest.approx(25.02)
    assert state["grid_elevation_m"] == pytest.approx(901.0)
    assert seen["source"] == "open-meteo"
    assert (
        conn.execute("SELECT COUNT(*) AS n FROM forecast_samples").fetchone()["n"] == 1
    )
    open_meteo_budget = conn.execute(
        "SELECT calls FROM api_budget WHERE source='open-meteo'"
    ).fetchone()
    assert open_meteo_budget["calls"] == 1


def test_station_call_pacing_is_seeded_bounded_and_used_by_fetch_obs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")

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

    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Cluster', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    station_rows: list[tuple[int, str]] = []
    for pws_id in ("C1", "C2", "C3"):
        station_id = int(
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m)
                VALUES (?, ?, 47, 25, 900)
                """,
                (site_id, pws_id),
            ).lastrowid
        )
        station_rows.append((station_id, pws_id))

    pacing_calls: list[tuple[int, int, int]] = []
    history_calls: list[str] = []

    async def fake_pace(site_id_arg: int, station_id_arg: int, ordinal: int) -> None:
        pacing_calls.append((site_id_arg, station_id_arg, ordinal))

    async def fake_history(
        station_id_arg: str,
        api_key: str,
        *,
        hours: int,
        timezone: str | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> list[PwsObservation]:
        assert api_key == "secret-weather"
        assert hours == RECENT_REFRESH_HOURS
        assert timezone == "UTC"
        assert client is not None
        history_calls.append(station_id_arg)
        return []

    monkeypatch.setattr("wxverify.worker.processor.pace_station_call", fake_pace)
    monkeypatch.setattr("wxverify.worker.processor.fetch_hourly_history", fake_history)

    asyncio.run(
        dispatch(
            get_db(),
            Job(
                id=40,
                type="fetch_obs",
                site_id=site_id,
                job_key="obs",
                payload={},
                status="running",
                retry_count=0,
                max_retries=5,
            ),
        )
    )

    assert pacing_calls == [
        (site_id, station_rows[0][0], 0),
        (site_id, station_rows[1][0], 1),
        (site_id, station_rows[2][0], 2),
    ]
    assert history_calls == ["C1", "C2", "C3"]
    budget = conn.execute(
        "SELECT calls FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    assert budget["calls"] == 3


def test_backfill_and_catchup_write_domain_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Backfill', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    station_id = int(
        conn.execute(
            """
            INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)
            VALUES (?, 'BF1', 47, 25, 900)
            """,
            (site_id,),
        ).lastrowid
    )
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )
    conn.execute(
        "UPDATE feeds SET default_subscribed=0 WHERE source='open-meteo' AND id<>?",
        (feed_id,),
    )
    recent_history_calls: list[int] = []
    station_history_windows: list[tuple[str, str]] = []
    historical_windows: list[tuple[str, str]] = []

    async def fake_recent_history(
        station_id_arg: str,
        api_key: str,
        *,
        hours: int,
        timezone: str | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> list[PwsObservation]:
        assert station_id_arg == "BF1"
        assert api_key == "secret-weather"
        assert timezone == "UTC"
        assert client is not None
        recent_history_calls.append(hours)
        return [
            PwsObservation(
                variable="temperature",
                valid_at="2026-06-23T00:00:00Z",
                value=10.0,
                source_raw="10.0 C",
            )
        ]

    async def fake_history_range(
        station_id_arg: str,
        api_key: str,
        *,
        window_start: str,
        window_end: str,
        timezone: str,
        client: httpx.AsyncClient | None = None,
    ) -> list[PwsObservation]:
        assert station_id_arg == "BF1"
        assert api_key == "secret-weather"
        assert timezone == "UTC"
        assert client is not None
        station_history_windows.append((window_start, window_end))
        return [
            PwsObservation(
                variable="temperature",
                valid_at="2026-06-23T00:00:00Z",
                value=10.0,
                source_raw="10.0 C",
            )
        ]

    class FakeHistoricalAdapter:
        supports_historical = True

        def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
            assert req.model == "ecmwf_ifs"
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
            raise AssertionError("backfill should use historical replay")

        async def fetch_historical(
            self, req: ForecastRequest, *, window_start: str, window_end: str
        ) -> FetchResult | None:
            historical_windows.append((window_start, window_end))
            return FetchResult(
                samples=[
                    NormalizedSample(
                        model=req.model,
                        variable="temperature",
                        issued_at="2026-06-22T00:00:00Z",
                        valid_at="2026-06-23T00:00:00Z",
                        lead_hours=24,
                        value=11.0,
                        source_raw="11.0 previous_day1",
                        model_run_id=f"{req.model}:2026-06-22T00:00:00Z",
                    )
                ],
                grid=GridProvenance(
                    grid_lat=47.1,
                    grid_lon=25.1,
                    grid_elevation_m=905.0,
                ),
            )

    def fake_build_adapter(
        source: str, client: httpx.AsyncClient
    ) -> FakeHistoricalAdapter:
        assert source == "open-meteo"
        assert client is not None
        return FakeHistoricalAdapter()

    monkeypatch.setattr(
        "wxverify.worker.backfill.fetch_hourly_history", fake_recent_history
    )
    monkeypatch.setattr(
        "wxverify.worker.backfill.fetch_hourly_history_range", fake_history_range
    )
    monkeypatch.setattr(
        "wxverify.worker.catchup.fetch_hourly_history_range", fake_history_range
    )
    monkeypatch.setattr("wxverify.worker.backfill.build_adapter", fake_build_adapter)
    continuation = asyncio.run(
        dispatch(
            get_db(),
            Job(
                id=3,
                type="backfill_site",
                site_id=site_id,
                job_key=f"backfill:{site_id}",
                payload={
                    "site_id": site_id,
                    "window_start": "2026-06-22T00:00:00Z",
                    "window_end": "2026-06-24T00:00:00Z",
                    "cursor_start": "2026-06-22T00:00:00Z",
                },
                status="running",
                retry_count=0,
                max_retries=5,
            ),
        )
    )
    assert continuation is None
    assert station_history_windows == [("2026-06-22T00:00:00Z", "2026-06-24T00:00:00Z")]
    assert historical_windows == [("2026-06-22T00:00:00Z", "2026-06-24T00:00:00Z")]
    site = conn.execute(
        "SELECT backfill_status, backfill_through FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    assert site["backfill_status"] == "complete"
    assert site["backfill_through"] == "2026-06-24T00:00:00Z"
    assert (
        conn.execute("SELECT COUNT(*) AS n FROM station_observations").fetchone()["n"]
        == 1
    )
    assert (
        conn.execute("SELECT COUNT(*) AS n FROM forecast_samples").fetchone()["n"] == 1
    )
    assert conn.execute("SELECT COUNT(*) AS n FROM forecast_pairs").fetchone()["n"] >= 1
    assert conn.execute("SELECT COUNT(*) AS n FROM score_cache").fetchone()["n"] >= 1
    budget = {
        row["source"]: row["calls"]
        for row in conn.execute("SELECT source, calls FROM api_budget")
    }
    assert budget["weathercom"] == 1
    assert budget["open-meteo"] == 1
    feed_state = conn.execute(
        """
        SELECT last_run_at, grid_lat, grid_lon, grid_elevation_m
        FROM site_feed_state
        WHERE site_id=? AND feed_id=?
        """,
        (site_id, feed_id),
    ).fetchone()
    assert feed_state is not None
    assert feed_state["last_run_at"] is None
    assert feed_state["grid_lat"] == pytest.approx(47.1)
    assert feed_state["grid_lon"] == pytest.approx(25.1)
    assert feed_state["grid_elevation_m"] == pytest.approx(905.0)

    conn.execute(
        """
        DELETE FROM station_observations
        WHERE station_id=? AND valid_at='2026-06-23T00:00:00Z'
        """,
        (station_id,),
    )
    conn.execute(
        "DELETE FROM observations WHERE site_id=? AND valid_at='2026-06-23T00:00:00Z'",
        (site_id,),
    )
    conn.execute(
        """
        DELETE FROM forecast_pairs
        WHERE site_id=? AND valid_at='2026-06-23T00:00:00Z'
        """,
        (site_id,),
    )
    station_history_windows.clear()
    asyncio.run(
        dispatch(
            get_db(),
            Job(
                id=4,
                type="catchup",
                site_id=None,
                job_key="catchup",
                payload={
                    "window_start": "2026-06-22T00:00:00Z",
                    "window_end": "2026-06-24T00:00:00Z",
                },
                status="running",
                retry_count=0,
                max_retries=5,
            ),
        )
    )
    assert station_history_windows == [("2026-06-22T00:00:00Z", "2026-06-24T00:00:00Z")]
    restored = conn.execute(
        """
        SELECT value FROM station_observations
        WHERE station_id=? AND valid_at='2026-06-23T00:00:00Z'
        """,
        (station_id,),
    ).fetchone()
    assert restored["value"] == 10.0
    assert conn.execute("SELECT COUNT(*) AS n FROM forecast_pairs").fetchone()["n"] >= 1
    catchup_at = conn.execute(
        "SELECT value FROM settings WHERE key='last_catchup_at'"
    ).fetchone()
    assert catchup_at["value"] != "queued"
    assert (
        conn.execute(
            """
            SELECT last_run_at FROM site_feed_state
            WHERE site_id=? AND feed_id=?
            """,
            (site_id, feed_id),
        ).fetchone()["last_run_at"]
        is None
    )


def test_backfill_fetches_pws_history_once_across_forecast_chunks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Chunked', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    conn.execute(
        """
        INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)
        VALUES (?, 'BF2', 47, 25, 900)
        """,
        (site_id,),
    )
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )
    conn.execute(
        "UPDATE feeds SET default_subscribed=0 WHERE source='open-meteo' AND id<>?",
        (feed_id,),
    )
    station_history_windows: list[tuple[str, str, str]] = []
    forecast_windows: list[tuple[str, str]] = []

    async def fake_history_range(
        station_id_arg: str,
        api_key: str,
        *,
        window_start: str,
        window_end: str,
        timezone: str,
        client: httpx.AsyncClient | None = None,
    ) -> list[PwsObservation]:
        assert station_id_arg == "BF2"
        assert api_key == "secret-weather"
        assert client is not None
        station_history_windows.append((window_start, window_end, timezone))
        return []

    class FakeHistoricalAdapter:
        supports_historical = True

        def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
            assert req.model == "ecmwf_ifs"
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
            raise AssertionError("backfill should use historical replay")

        async def fetch_historical(
            self, req: ForecastRequest, *, window_start: str, window_end: str
        ) -> FetchResult | None:
            forecast_windows.append((window_start, window_end))
            return FetchResult(samples=[], grid=None)

    def fake_build_adapter(
        source: str, client: httpx.AsyncClient
    ) -> FakeHistoricalAdapter:
        assert source == "open-meteo"
        assert client is not None
        return FakeHistoricalAdapter()

    monkeypatch.setattr(
        "wxverify.worker.backfill.fetch_hourly_history_range", fake_history_range
    )
    monkeypatch.setattr("wxverify.worker.backfill.build_adapter", fake_build_adapter)

    payload: dict[str, object] = {
        "site_id": site_id,
        "window_start": "2026-06-01T00:00:00Z",
        "window_end": "2026-06-16T00:00:00Z",
        "cursor_start": "2026-06-01T00:00:00Z",
    }
    continuations = 0
    for job_id in range(20, 23):
        continuation = asyncio.run(
            dispatch(
                get_db(),
                Job(
                    id=job_id,
                    type="backfill_site",
                    site_id=site_id,
                    job_key=f"backfill:{site_id}",
                    payload=payload,
                    status="running",
                    retry_count=0,
                    max_retries=5,
                ),
            )
        )
        if continuation is None:
            break
        continuations += 1
        assert continuation.payload["station_history_complete"] is True
        payload = continuation.payload

    assert continuations == 2
    assert station_history_windows == [
        ("2026-06-01T00:00:00Z", "2026-06-16T00:00:00Z", "UTC")
    ]
    assert forecast_windows == [
        ("2026-06-01T00:00:00Z", "2026-06-08T00:00:00Z"),
        ("2026-06-08T00:00:00Z", "2026-06-15T00:00:00Z"),
        ("2026-06-15T00:00:00Z", "2026-06-16T00:00:00Z"),
    ]
    budget = conn.execute(
        "SELECT calls FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    assert budget["calls"] == 1


def test_catchup_replays_open_meteo_and_continues_by_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )
    conn.execute(
        "UPDATE feeds SET default_subscribed=0 WHERE source='open-meteo' AND id<>?",
        (feed_id,),
    )
    site_ids: list[int] = []
    for index in range(3):
        site_id = int(
            conn.execute(
                """
                INSERT INTO sites
                    (name, forecast_lat, forecast_lon, elevation_m, timezone)
                VALUES (?, ?, 25, 900, 'UTC')
                """,
                (f"Catchup {index}", 47.0 + index),
            ).lastrowid
        )
        station_id = int(
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m)
                VALUES (?, ?, 47, 25, 900)
                """,
                (site_id, f"CU{index}"),
            ).lastrowid
        )
        insert_station_observation(
            conn,
            station_id=station_id,
            variable="temperature",
            valid_at="2026-06-23T00:00:00Z",
            value=10.0 + index,
            source_raw="10.0 C",
        )
        insert_station_observation(
            conn,
            station_id=station_id,
            variable="wind",
            valid_at="2026-06-23T00:00:00Z",
            value=3.0,
            source_raw="10.8 km/h",
        )
        insert_station_observation(
            conn,
            station_id=station_id,
            variable="precip",
            valid_at="2026-06-23T00:00:00Z",
            value=0.0,
            source_raw="0.0 mm precipTotal",
        )
        site_ids.append(site_id)

    pws_calls: list[str] = []
    forecast_calls: list[tuple[float, str, str]] = []

    async def fake_history_range(
        station_id_arg: str,
        api_key: str,
        *,
        window_start: str,
        window_end: str,
        timezone: str,
        client: httpx.AsyncClient | None = None,
    ) -> list[PwsObservation]:
        pws_calls.append(station_id_arg)
        return []

    class FakeCatchupAdapter:
        supports_historical = True

        def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
            raise AssertionError("catchup should use historical replay")

        async def fetch_historical(
            self, req: ForecastRequest, *, window_start: str, window_end: str
        ) -> FetchResult | None:
            forecast_calls.append((req.lat, window_start, window_end))
            return FetchResult(
                samples=[
                    NormalizedSample(
                        model=req.model,
                        variable="temperature",
                        issued_at="2026-06-22T00:00:00Z",
                        valid_at="2026-06-23T00:00:00Z",
                        lead_hours=24,
                        value=req.lat - 37.0,
                        source_raw="previous_day1",
                        model_run_id=f"{req.model}:2026-06-22T00:00:00Z",
                    )
                ],
                grid=None,
            )

    def fake_build_adapter(
        source: str, client: httpx.AsyncClient
    ) -> FakeCatchupAdapter:
        assert source == "open-meteo"
        assert client is not None
        return FakeCatchupAdapter()

    monkeypatch.setattr(
        "wxverify.worker.catchup.fetch_hourly_history_range", fake_history_range
    )
    monkeypatch.setattr("wxverify.worker.catchup.build_adapter", fake_build_adapter)

    first = asyncio.run(
        dispatch(
            get_db(),
            Job(
                id=30,
                type="catchup",
                site_id=None,
                job_key="catchup",
                payload={
                    "window_start": "2026-06-23T00:00:00Z",
                    "window_end": "2026-06-23T01:00:00Z",
                },
                status="running",
                retry_count=0,
                max_retries=5,
            ),
        )
    )
    assert first is not None
    assert first.job_type == "catchup"
    assert first.site_id is None
    assert first.job_key == "catchup"
    assert first.payload["cursor_site_id"] == site_ids[1]

    second = asyncio.run(
        dispatch(
            get_db(),
            Job(
                id=31,
                type="catchup",
                site_id=None,
                job_key="catchup",
                payload=first.payload,
                status="running",
                retry_count=0,
                max_retries=5,
            ),
        )
    )
    assert second is None
    assert pws_calls == []
    assert forecast_calls == [
        (47.0, "2026-06-23T00:00:00Z", "2026-06-23T01:00:00Z"),
        (48.0, "2026-06-23T00:00:00Z", "2026-06-23T01:00:00Z"),
        (49.0, "2026-06-23T00:00:00Z", "2026-06-23T01:00:00Z"),
    ]
    assert (
        conn.execute("SELECT COUNT(*) AS n FROM forecast_samples").fetchone()["n"] == 3
    )
    assert conn.execute("SELECT COUNT(*) AS n FROM forecast_pairs").fetchone()["n"] >= 3
    budget = {
        row["source"]: row["calls"]
        for row in conn.execute("SELECT source, calls FROM api_budget")
    }
    assert budget["open-meteo"] == 3
    catchup_at = conn.execute(
        "SELECT value FROM settings WHERE key='last_catchup_at'"
    ).fetchone()
    assert catchup_at["value"] == "2026-06-23T01:00:00Z"


def test_consensus_pairing_settings_and_cache_freshness(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Score', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    station_id = int(
        conn.execute(
            """
            INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)
            VALUES (?, 'S1', 47, 25, 900)
            """,
            (site_id,),
        ).lastrowid
    )
    insert_station_observation(
        conn,
        station_id=station_id,
        variable="temperature",
        valid_at="2026-01-01T01:00:00Z",
        value=6.85,
        source_raw="6.85C",
    )
    obs = conn.execute(
        "SELECT * FROM observations WHERE site_id=?", (site_id,)
    ).fetchone()
    assert obs["n_stations"] == 1
    assert obs["value"] == pytest.approx(6.85)
    for name, value in (("S2", 7.05), ("S3", 8.65)):
        sibling_id = int(
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m)
                VALUES (?, ?, 47, 25, 900)
                """,
                (site_id, name),
            ).lastrowid
        )
        insert_station_observation(
            conn,
            station_id=sibling_id,
            variable="temperature",
            valid_at="2026-01-01T01:00:00Z",
            value=value,
            source_raw=f"{value}C",
        )
    obs = conn.execute(
        "SELECT * FROM observations WHERE site_id=?", (site_id,)
    ).fetchone()
    assert obs["n_stations"] == 3
    assert obs["rejected_stations"] == 0
    try:
        insert_station_observation(
            conn,
            station_id=station_id,
            variable="wind",
            valid_at="2026-01-01T01:00:00Z",
            value=3.0,
            source_raw=None,
        )
    except ValueError:
        pass
    else:  # pragma: no cover
        raise AssertionError("target obs write without source_raw was accepted")

    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' LIMIT 1"
        ).fetchone()["id"]
    )
    persist_fetch_result(
        conn,
        site_id=site_id,
        source="open-meteo",
        fetch_feed_id=feed_id,
        result=FetchResult(
            samples=[
                NormalizedSample(
                    model="ecmwf_ifs",
                    variable="temperature",
                    issued_at="2026-01-01T00:00:00Z",
                    valid_at="2026-01-01T01:00:00Z",
                    lead_hours=1,
                    value=7.85,
                    source_raw="7.85C",
                    model_run_id="run-1",
                )
            ]
        ),
    )
    pair_and_score(conn, site_id)
    assert conn.execute("SELECT COUNT(*) AS n FROM forecast_pairs").fetchone()["n"] >= 1
    conn.execute(
        """
        INSERT OR REPLACE INTO score_cache
            (site_id, feed_id, variable, day_ahead, window_key, n, computed_at)
        VALUES (?, ?, 'temperature', 0, 'w:30', 1, '2026-01-01T00:00:00Z')
        """,
        (site_id, feed_id),
    )
    set_rolling_window_days_sync(conn, 14)
    assert (
        conn.execute(
            "SELECT COUNT(*) AS n FROM score_cache WHERE window_key='w:30'"
        ).fetchone()["n"]
        == 0
    )
    assert is_cache_fresh(ScoreCacheRow(computed_at="not-a-date"), "w:all") is False
    assert (
        is_cache_fresh(ScoreCacheRow(computed_at="2026-01-01T00:00:00Z"), "w:all")
        is True
    )
    assert score_badge(0.734) == 73
    assert score_badge(-0.2) == 0
    assert below_baseline(-0.2) is True
    assert score_badge(None) is None


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


def test_corrected_observation_rebuilds_future_persistence(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Persistence', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    station_id = int(
        conn.execute(
            """
            INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)
            VALUES (?, 'P1', 47, 25, 900)
            """,
            (site_id,),
        ).lastrowid
    )
    for valid_at, value in (
        ("2035-01-01T00:00:00Z", 10.0),
        ("2035-01-01T02:00:00Z", 13.0),
    ):
        insert_station_observation(
            conn,
            station_id=station_id,
            variable="temperature",
            valid_at=valid_at,
            value=value,
            source_raw=f"{value} C",
        )
    pair_and_score(conn, site_id)
    persistence_feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
        ).fetchone()["id"]
    )
    stale = conn.execute(
        """
        SELECT forecast, observed
        FROM forecast_pairs
        WHERE site_id=?
          AND feed_id=?
          AND variable='temperature'
          AND issued_at='2035-01-01T00:00:00Z'
          AND valid_at='2035-01-01T02:00:00Z'
          AND lead_hours=2
        """,
        (site_id, persistence_feed_id),
    ).fetchone()
    assert stale["forecast"] == pytest.approx(10.0)
    assert stale["observed"] == pytest.approx(13.0)

    insert_station_observation(
        conn,
        station_id=station_id,
        variable="temperature",
        valid_at="2035-01-01T00:00:00Z",
        value=11.0,
        source_raw="11.0 C",
    )
    assert (
        conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM forecast_pairs
            WHERE site_id=?
              AND feed_id=?
              AND variable='temperature'
              AND issued_at='2035-01-01T00:00:00Z'
              AND valid_at='2035-01-01T02:00:00Z'
              AND lead_hours=2
            """,
            (site_id, persistence_feed_id),
        ).fetchone()["n"]
        == 0
    )
    pair_and_score(conn, site_id)
    rebuilt = conn.execute(
        """
        SELECT forecast, observed
        FROM forecast_pairs
        WHERE site_id=?
          AND feed_id=?
          AND variable='temperature'
          AND issued_at='2035-01-01T00:00:00Z'
          AND valid_at='2035-01-01T02:00:00Z'
          AND lead_hours=2
        """,
        (site_id, persistence_feed_id),
    ).fetchone()
    assert rebuilt["forecast"] == pytest.approx(11.0)
    assert rebuilt["observed"] == pytest.approx(13.0)


def test_skill_uses_shared_persistence_cells_and_virtual_precip_flags(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone,
                 rain_threshold_mm)
            VALUES ('Shared Skill', 47, 25, 900, 'UTC', 0.2)
            """
        ).lastrowid
    )
    real_feed_ids = [
        int(row["id"])
        for row in conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 2"
        )
    ]
    persistence_feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
        ).fetchone()["id"]
    )
    mean_feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
        ).fetchone()["id"]
    )

    def add_pair(
        feed_id: int,
        *,
        variable: str,
        issued_at: str,
        valid_at: str,
        forecast: float,
        observed: float,
        day_ahead: int = 1,
        lead_hours: int = 24,
        cat_hit: int | None = None,
        cat_false: int | None = None,
        cat_miss: int | None = None,
        cat_correct_neg: int | None = None,
        rain_threshold_mm: float | None = None,
    ) -> None:
        error = forecast - observed
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 cat_hit, cat_false, cat_miss, cat_correct_neg, rain_threshold_mm)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                feed_id,
                variable,
                issued_at,
                valid_at,
                lead_hours,
                day_ahead,
                forecast,
                observed,
                error,
                abs(error),
                error * error,
                cat_hit,
                cat_false,
                cat_miss,
                cat_correct_neg,
                rain_threshold_mm,
            ),
        )

    add_pair(
        real_feed_ids[0],
        variable="temperature",
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-02T00:00:00Z",
        forecast=11.0,
        observed=10.0,
    )
    add_pair(
        real_feed_ids[0],
        variable="temperature",
        issued_at="2035-01-01T01:00:00Z",
        valid_at="2035-01-02T01:00:00Z",
        forecast=13.0,
        observed=10.0,
    )
    add_pair(
        persistence_feed_id,
        variable="temperature",
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-02T00:00:00Z",
        forecast=12.0,
        observed=10.0,
    )

    skill_rows = leaderboard(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        window="all",
    )
    skill_by_feed = {row.feed_id: row for row in skill_rows}
    skill_row = skill_by_feed[real_feed_ids[0]]
    assert skill_row.feed_id == real_feed_ids[0]
    assert skill_row.skill_score == pytest.approx(0.75)

    conn.execute(
        """
        INSERT INTO observations
            (site_id, variable, valid_at, value, n_stations)
        VALUES
            (?, 'precip', '2035-01-01T00:00:00Z', 0.0, 1),
            (?, 'precip', '2035-01-02T00:00:00Z', 0.3, 1)
        """,
        (site_id, site_id),
    )
    add_pair(
        real_feed_ids[0],
        variable="precip",
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-02T00:00:00Z",
        forecast=0.4,
        observed=0.3,
        cat_hit=1,
        cat_false=0,
        cat_miss=0,
        cat_correct_neg=0,
        rain_threshold_mm=0.2,
    )
    add_pair(
        real_feed_ids[1],
        variable="precip",
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-02T00:00:00Z",
        forecast=0.0,
        observed=0.3,
        cat_hit=0,
        cat_false=0,
        cat_miss=1,
        cat_correct_neg=0,
        rain_threshold_mm=0.2,
    )
    pair_and_score(conn, site_id)
    persistence_precip = conn.execute(
        """
        SELECT cat_hit, cat_false, cat_miss, cat_correct_neg, rain_threshold_mm
        FROM forecast_pairs
        WHERE site_id=? AND feed_id=? AND variable='precip'
          AND issued_at='2035-01-01T00:00:00Z'
          AND valid_at='2035-01-02T00:00:00Z'
        """,
        (site_id, persistence_feed_id),
    ).fetchone()
    assert persistence_precip["cat_hit"] == 0
    assert persistence_precip["cat_false"] == 0
    assert persistence_precip["cat_miss"] == 1
    assert persistence_precip["cat_correct_neg"] == 0
    assert persistence_precip["rain_threshold_mm"] == pytest.approx(0.2)
    mean_precip = conn.execute(
        """
        SELECT forecast, cat_hit, cat_false, cat_miss, cat_correct_neg,
               rain_threshold_mm
        FROM forecast_pairs
        WHERE site_id=? AND feed_id=? AND variable='precip'
        """,
        (site_id, mean_feed_id),
    ).fetchone()
    assert mean_precip["forecast"] == pytest.approx(0.2)
    assert mean_precip["cat_hit"] == 1
    assert mean_precip["cat_false"] == 0
    assert mean_precip["cat_miss"] == 0
    assert mean_precip["cat_correct_neg"] == 0
    assert mean_precip["rain_threshold_mm"] == pytest.approx(0.2)


def test_winrate_uses_latest_issued_ties_and_sparse_denominator(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Winrate', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_ids = [
        int(row["id"])
        for row in conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 4"
        )
    ]

    def add_pair(
        feed_id: int, issued_at: str, valid_at: str, forecast: float, observed: float
    ) -> None:
        error = forecast - observed
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', ?, ?, 24, 1, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                feed_id,
                issued_at,
                valid_at,
                forecast,
                observed,
                error,
                abs(error),
                error * error,
            ),
        )

    add_pair(
        feed_ids[0],
        "2026-06-21T00:00:00Z",
        "2026-06-23T00:00:00Z",
        10.0,
        10.0,
    )
    add_pair(
        feed_ids[0],
        "2026-06-22T00:00:00Z",
        "2026-06-23T00:00:00Z",
        13.0,
        10.0,
    )
    add_pair(
        feed_ids[1],
        "2026-06-22T00:00:00Z",
        "2026-06-23T00:00:00Z",
        11.0,
        10.0,
    )
    add_pair(
        feed_ids[0],
        "2026-06-23T00:00:00Z",
        "2026-06-24T00:00:00Z",
        9.0,
        10.0,
    )
    add_pair(
        feed_ids[1],
        "2026-06-23T00:00:00Z",
        "2026-06-24T00:00:00Z",
        11.0,
        10.0,
    )
    add_pair(
        feed_ids[2],
        "2026-06-23T00:00:00Z",
        "2026-06-24T00:00:00Z",
        14.0,
        10.0,
    )
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled)
        VALUES (?, ?, 0)
        """,
        (site_id, feed_ids[3]),
    )
    add_pair(
        feed_ids[3],
        "2026-06-23T00:00:00Z",
        "2026-06-24T00:00:00Z",
        10.0,
        10.0,
    )

    rows = {
        int(row["feed_id"]): row
        for row in winrate(conn, site_id=site_id, variable="temperature", day_ahead=1)
    }
    assert set(rows) == set(feed_ids[:3])
    assert rows[feed_ids[0]]["covered"] == 2
    assert rows[feed_ids[0]]["comparable"] == 2
    assert rows[feed_ids[0]]["wins"] == pytest.approx(0.5)
    assert rows[feed_ids[0]]["win_rate"] == pytest.approx(0.25)
    assert rows[feed_ids[1]]["wins"] == pytest.approx(1.5)
    assert rows[feed_ids[1]]["win_rate"] == pytest.approx(0.75)
    assert rows[feed_ids[2]]["covered"] == 1
    assert rows[feed_ids[2]]["comparable"] == 1
    assert rows[feed_ids[2]]["win_rate"] == 0.0


def test_winrate_applies_window_to_canonical_cells(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Winrate Window', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_ids = [
        int(row["id"])
        for row in conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 3"
        )
    ]

    def add_pair(
        feed_id: int, issued_at: str, valid_at: str, forecast: float, observed: float
    ) -> None:
        error = forecast - observed
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', ?, ?, 24, 1, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                feed_id,
                issued_at,
                valid_at,
                forecast,
                observed,
                error,
                abs(error),
                error * error,
            ),
        )

    add_pair(
        feed_ids[0],
        "2035-01-01T00:00:00Z",
        "2035-01-02T00:00:00Z",
        10.0,
        10.0,
    )
    add_pair(
        feed_ids[1],
        "2035-01-01T00:00:00Z",
        "2035-01-02T00:00:00Z",
        12.0,
        10.0,
    )
    add_pair(
        feed_ids[1],
        "2020-01-01T00:00:00Z",
        "2020-01-02T00:00:00Z",
        20.0,
        10.0,
    )
    add_pair(
        feed_ids[2],
        "2020-01-01T00:00:00Z",
        "2020-01-02T00:00:00Z",
        10.0,
        10.0,
    )

    recent = {
        int(row["feed_id"]): row
        for row in winrate(
            conn,
            site_id=site_id,
            variable="temperature",
            day_ahead=1,
            window="1d",
        )
    }
    assert set(recent) == {feed_ids[0], feed_ids[1]}
    assert recent[feed_ids[0]]["win_rate"] == 1.0
    assert recent[feed_ids[1]]["win_rate"] == 0.0

    all_rows = {
        int(row["feed_id"]): row
        for row in winrate(
            conn,
            site_id=site_id,
            variable="temperature",
            day_ahead=1,
            window="all",
        )
    }
    assert set(all_rows) == set(feed_ids)


def test_composite_averages_live_components_and_filters_feeds(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Composite', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_ids = [
        int(row["id"])
        for row in conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 4"
        )
    ]
    persistence_feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
        ).fetchone()["id"]
    )

    def add_pair(
        feed_id: int,
        variable: str,
        skill_score: float,
        *,
        day_ahead: int = 1,
        valid_hour: int,
    ) -> None:
        observed = 10.0
        persistence_sq_error = 4.0
        feed_sq_error = (1.0 - skill_score) * persistence_sq_error
        feed_error = math.sqrt(feed_sq_error)
        valid_at = f"2035-01-02T{valid_hour:02d}:00:00Z"
        issued_at = f"2035-01-01T{valid_hour:02d}:00:00Z"
        persistence_error = math.sqrt(persistence_sq_error)
        conn.execute(
            """
            INSERT OR IGNORE INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, ?, ?, ?, 24, ?, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                persistence_feed_id,
                variable,
                issued_at,
                valid_at,
                day_ahead,
                observed + persistence_error,
                observed,
                persistence_error,
                abs(persistence_error),
                persistence_sq_error,
            ),
        )
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, ?, ?, ?, 24, ?, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                feed_id,
                variable,
                issued_at,
                valid_at,
                day_ahead,
                observed + feed_error,
                observed,
                feed_error,
                abs(feed_error),
                feed_sq_error,
            ),
        )

    add_pair(feed_ids[0], "temperature", 0.5, valid_hour=0)
    add_pair(feed_ids[0], "wind", -0.2, valid_hour=1)
    add_pair(feed_ids[1], "temperature", 0.8, valid_hour=2)
    add_pair(feed_ids[1], "wind", 0.4, valid_hour=3)
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled)
        VALUES (?, ?, 0)
        """,
        (site_id, feed_ids[3]),
    )
    add_pair(feed_ids[3], "temperature", 1.0, valid_hour=4)

    # Custom `Nd` window: the live compute path. The `rolling` window is
    # cache-backed since 0.4.2 and would return `rebuilding` (empty) with no
    # seeded score_cache; the live aggregation/filter math under test here is
    # unchanged and identical across window cutoffs.
    rows = {
        int(row["feed_id"]): row
        for row in composite(conn, site_id=site_id, window="30d")
    }
    assert feed_ids[0] in rows
    assert feed_ids[1] in rows
    assert feed_ids[3] not in rows
    assert rows[feed_ids[0]]["component_count"] == 2
    assert rows[feed_ids[0]]["components"] == {"temperature": 0.5, "wind": 0.0}
    assert rows[feed_ids[0]]["raw_components"] == pytest.approx(
        {
            "temperature": 0.5,
            "wind": -0.2,
        }
    )
    assert rows[feed_ids[0]]["score"] == pytest.approx(0.25)
    assert rows[feed_ids[0]]["raw_score"] == pytest.approx(0.15)
    assert rows[feed_ids[0]]["badge"] == 25
    assert rows[feed_ids[1]]["score"] == pytest.approx(0.6)
    assert rows[feed_ids[1]]["badge"] == 60


def test_composite_custom_window_computes_live_without_cache(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Composite Live', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    real_feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )
    persistence_feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
        ).fetchone()["id"]
    )

    def add_pair(feed_id: int, forecast: float) -> None:
        observed = 10.0
        error = forecast - observed
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z',
                    '2035-01-02T00:00:00Z', 24, 1, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                feed_id,
                forecast,
                observed,
                error,
                abs(error),
                error * error,
            ),
        )

    add_pair(real_feed_id, 11.0)
    add_pair(persistence_feed_id, 12.0)

    rows = {
        int(row["feed_id"]): row
        for row in composite(conn, site_id=site_id, window="1d")
    }
    assert rows[real_feed_id]["window_key"] == "live:1d"
    assert rows[real_feed_id]["components"] == {"temperature": 0.75}
    assert rows[real_feed_id]["raw_score"] == pytest.approx(0.75)
    assert rows[real_feed_id]["badge"] == 75
    assert conn.execute("SELECT COUNT(*) AS n FROM score_cache").fetchone()["n"] == 0


def test_subscription_rebuilds_multimodel_mean(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Mean Rebuild', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_ids = [
        int(row["id"])
        for row in conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 2"
        )
    ]
    mean_feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
        ).fetchone()["id"]
    )

    def add_pair(feed_id: int, forecast: float) -> None:
        observed = 10.0
        error = forecast - observed
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z',
                    '2035-01-02T00:00:00Z', 24, 1, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                feed_id,
                forecast,
                observed,
                error,
                abs(error),
                error * error,
            ),
        )

    add_pair(feed_ids[0], 8.0)
    add_pair(feed_ids[1], 12.0)
    _rebuild_mean_for_site(conn, site_id)
    row = conn.execute(
        """
        SELECT forecast, contributors FROM forecast_pairs
        WHERE site_id=? AND feed_id=?
        """,
        (site_id, mean_feed_id),
    ).fetchone()
    assert row["forecast"] == pytest.approx(10.0)
    assert row["contributors"] == 2

    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled)
        VALUES (?, ?, 0)
        """,
        (site_id, feed_ids[1]),
    )
    _rebuild_mean_for_site(conn, site_id)
    assert (
        conn.execute(
            """
            SELECT COUNT(*) AS n FROM forecast_pairs
            WHERE site_id=? AND feed_id=?
            """,
            (site_id, mean_feed_id),
        ).fetchone()["n"]
        == 0
    )

    conn.execute(
        "UPDATE site_feed_state SET enabled=1 WHERE site_id=? AND feed_id=?",
        (site_id, feed_ids[1]),
    )
    _rebuild_mean_for_site(conn, site_id)
    assert (
        conn.execute(
            """
            SELECT COUNT(*) AS n FROM forecast_pairs
            WHERE site_id=? AND feed_id=?
            """,
            (site_id, mean_feed_id),
        ).fetchone()["n"]
        == 1
    )


def test_meteoblue_package_gates_members_for_leaderboard_and_mean(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Meteoblue Gate', 47, 25, 900, 'UTC')
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
    member_ids: list[int] = []
    for model in ("gfs", "icon"):
        member_ids.append(
            int(
                conn.execute(
                    """
                    INSERT INTO feeds
                        (source, model, enabled, default_subscribed,
                         fetch_interval_minutes, max_lead_hours, is_virtual)
                    VALUES ('meteoblue', ?, 1, 0, 1440, 168, 0)
                    """,
                    (model,),
                ).lastrowid
            )
        )
    mean_feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
        ).fetchone()["id"]
    )

    for feed_id, forecast in zip(member_ids, (8.0, 12.0), strict=True):
        observed = 10.0
        error = forecast - observed
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z',
                    '2035-01-02T00:00:00Z', 24, 1, ?, ?, ?, ?, ?)
            """,
            (site_id, feed_id, forecast, observed, error, abs(error), error * error),
        )

    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled)
        VALUES (?, ?, 1)
        """,
        (site_id, package_feed_id),
    )
    _rebuild_mean_for_site(conn, site_id)
    mean = conn.execute(
        """
        SELECT forecast, contributors
        FROM forecast_pairs
        WHERE site_id=? AND feed_id=?
        """,
        (site_id, mean_feed_id),
    ).fetchone()
    assert mean["forecast"] == pytest.approx(10.0)
    assert mean["contributors"] == 2
    listed = {
        row.feed_id
        for row in leaderboard(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window="1d"
        )
    }
    assert set(member_ids).issubset(listed)

    conn.execute(
        "UPDATE site_feed_state SET enabled=0 WHERE site_id=? AND feed_id=?",
        (site_id, package_feed_id),
    )
    _rebuild_mean_for_site(conn, site_id)
    assert (
        conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM forecast_pairs
            WHERE site_id=? AND feed_id=?
            """,
            (site_id, mean_feed_id),
        ).fetchone()["n"]
        == 0
    )
    listed_after_unsubscribe = {
        row.feed_id
        for row in leaderboard(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window="1d"
        )
    }
    assert set(member_ids).isdisjoint(listed_after_unsubscribe)


def test_dashboard_rolling_readthrough_enqueues_only_on_cache_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "dashboard-cache.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = int(
                conn.execute(
                    """
                    INSERT INTO sites
                        (name, forecast_lat, forecast_lon, elevation_m, timezone)
                    VALUES ('Readthrough', 47, 25, 900, 'UTC')
                    """
                ).lastrowid
            )
            feed_id = int(
                conn.execute(
                    "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 1"
                ).fetchone()["id"]
            )
            observed = 10.0
            forecast = 11.0
            error = forecast - observed
            conn.execute(
                """
                INSERT INTO forecast_pairs
                    (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                     day_ahead, forecast, observed, error, abs_error, sq_error)
                VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z',
                        '2035-01-02T00:00:00Z', 24, 1, ?, ?, ?, ?, ?)
                """,
                (
                    site_id,
                    feed_id,
                    forecast,
                    observed,
                    error,
                    abs(error),
                    error * error,
                ),
            )
            return site_id

        site_id = db.write_sync(_seed)
        response = client.get(
            "/api/leaderboard",
            params={"site": site_id, "variable": "temperature", "lead": "D+1"},
        )
        assert response.status_code == 200
        rows = response.json()
        assert rows
        assert rows[0]["window_key"] == "w:14"
        assert rows[0]["window_days"] == 14
        assert (
            db.read_sync(
                lambda conn: conn.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM jobs
                    WHERE type='pair_and_score' AND site_id=?
                    """,
                    (site_id,),
                ).fetchone()["n"]
            )
            >= 1
        )


def test_cached_leaderboard_misses_when_active_feed_cache_is_incomplete(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Partial Cache', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_ids = [
        int(row["id"])
        for row in conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 2"
        )
    ]
    for feed_id, forecast in zip(feed_ids, (11.0, 12.0), strict=True):
        error = forecast - 10.0
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z',
                    '2035-01-02T00:00:00Z', 24, 1, ?, 10.0, ?, ?, ?)
            """,
            (site_id, feed_id, forecast, error, abs(error), error * error),
        )
    conn.execute(
        """
        INSERT INTO score_cache
            (site_id, feed_id, variable, day_ahead, window_key, n, skill_score,
             computed_at)
        VALUES (?, ?, 'temperature', 1, 'w:all', 1, 0.5, '2035-01-02T00:00:00Z')
        """,
        (site_id, feed_ids[0]),
    )
    result = leaderboard_with_status(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        window="all",
    )
    assert result.cache_miss is True
    assert {row.feed_id for row in result.rows} == set(feed_ids)


def test_station_toggle_and_rain_threshold_restore_pairs(
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
                    VALUES ('Recompute', 47, 25, 900, 'UTC')
                    """
                ).lastrowid
            )
            station_a = int(
                conn.execute(
                    """
                    INSERT INTO stations
                        (site_id, pws_station_id, lat, lon, dem_elevation_m)
                    VALUES (?, 'A', 47, 25, 900)
                    """,
                    (site_id,),
                ).lastrowid
            )
            station_b = int(
                conn.execute(
                    """
                    INSERT INTO stations
                        (site_id, pws_station_id, lat, lon, dem_elevation_m)
                    VALUES (?, 'B', 47, 25, 900)
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
                         value, source_raw, model_run_id)
                    VALUES (?, ?, ?, '2035-01-01T00:00:00Z',
                            '2035-01-02T00:00:00Z', 24, ?, 'raw', ?)
                    """,
                    (site_id, feed_id, variable, forecast, f"{variable}-run"),
                )
            pair_and_score(conn, site_id)
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
        temperature_pair = db.read_sync(
            lambda conn: conn.execute(
                """
                SELECT observed
                FROM forecast_pairs
                WHERE site_id=? AND feed_id=? AND variable='temperature'
                """,
                (site_id, feed_id),
            ).fetchone()
        )
        assert temperature_pair["observed"] == pytest.approx(10.0)

        rain_response = client.put(
            f"/api/sites/{site_id}",
            json={"rain_threshold_mm": 1.0},
            headers=headers,
        )
        assert rain_response.status_code == 200
        precip_pair = db.read_sync(
            lambda conn: conn.execute(
                """
                SELECT rain_threshold_mm
                FROM forecast_pairs
                WHERE site_id=? AND feed_id=? AND variable='precip'
                """,
                (site_id, feed_id),
            ).fetchone()
        )
        assert precip_pair["rain_threshold_mm"] == pytest.approx(1.0)


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
            VALUES ('Redact', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )

    class FailingAdapter:
        supports_historical = True

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

        async def fetch_historical(
            self, req: ForecastRequest, *, window_start: str, window_end: str
        ) -> FetchResult | None:
            return None

    def fake_build_adapter(source: str, client: httpx.AsyncClient) -> FailingAdapter:
        assert source == "open-meteo"
        assert client is not None
        return FailingAdapter()

    monkeypatch.setattr("wxverify.worker.processor.build_adapter", fake_build_adapter)
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(
            dispatch(
                get_db(),
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
        return PwsStation(station_id=station_id, lat=47.1, lon=25.1)

    async def fake_lookup_elevation_m(lat: float, lon: float) -> float:
        assert (lat, lon) == (47.1, 25.1)
        return 910.0

    monkeypatch.setattr(
        "wxverify.api.routes.stations.validate_station", fake_validate_station
    )
    monkeypatch.setattr(
        "wxverify.api.routes.stations.lookup_elevation_m", fake_lookup_elevation_m
    )
    app = create_app(root_path="")
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        site = client.post(
            "/api/sites",
            json={
                "name": "API",
                "forecast_lat": 47.0,
                "forecast_lon": 25.0,
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
                "forecast_lat": 47.0,
                "forecast_lon": 25.0,
                "elevation_m": 900.0,
                "timezone": "Not/AZone",
            },
            headers=headers,
        )
        assert bad_timezone.status_code == 422

        rejected_identity = client.put(
            f"/api/sites/{site_id}",
            json={"forecast_lat": 48.0},
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
            json={"pws_station_id": "TEST2", "lat": 47.1},
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
            data="enabled=true",
            headers={
                "Origin": "http://testserver",
                "X-CSRF-Token": csrf,
                "Content-Type": "text/plain",
            },
        )
        assert simple.status_code == 415
        missing_type_body = client.put(
            f"/api/sites/{site_id}",
            content=b'{"enabled": true}',
            headers={
                "Origin": "http://testserver",
                "X-CSRF-Token": csrf,
            },
        )
        assert missing_type_body.status_code == 415
        missing_csrf = client.post(
            "/api/catchup", headers={"Origin": "http://testserver"}
        )
        assert missing_csrf.status_code == 403
        catchup = client.post("/api/catchup", headers=headers)
        assert catchup.status_code == 200

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
                    VALUES ('Active Health', 47, 25, 900, 'UTC')
                    """
                ).lastrowid
            )
            disabled_site_id = int(
                conn.execute(
                    """
                    INSERT INTO sites
                        (name, forecast_lat, forecast_lon, elevation_m, timezone,
                         enabled)
                    VALUES ('Disabled Health', 47, 25, 900, 'UTC', 0)
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
    app = create_app(root_path="")
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        site = client.post(
            "/api/sites",
            json={
                "name": "No key",
                "forecast_lat": 47.0,
                "forecast_lon": 25.0,
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


def test_ui_root_path_htmx_and_create_site_fragment(tmp_path: Path) -> None:
    close_db()
    config.db_path = str(tmp_path / "ui.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    app = create_app(root_path="/ingress/path/")
    with TestClient(app) as client:
        root = client.get("/", follow_redirects=False)
        assert root.status_code == 307
        assert root.headers["location"] == "/ingress/path/dashboard"
        page = client.get("/sites")
        assert page.status_code == 200
        html = page.text
        assert f'src="/ingress/path/static/{__version__}/htmx.min.js"' in html
        assert f'src="/ingress/path/static/{__version__}/htmx-ext-json-enc.js"' in html
        assert 'class="brand" href="/ingress/path/dashboard"' in html
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
                "forecast_lat": 47.0,
                "forecast_lon": 25.0,
                "elevation_m": 900.0,
                "timezone": "Europe/Bucharest",
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
                "forecast_lat": 47.2,
                "forecast_lon": 25.2,
                "elevation_m": 910.0,
                "timezone": "Europe/Bucharest",
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
            VALUES ('Smoke', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    disabled_site_id = int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES ('Paused Smoke', 48, 26, 910, 'UTC', 0)
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
    assert precip["2026-01-01T01:00:00Z"].value == 0.0
    assert precip["2026-01-01T01:00:00Z"].source_raw == "-0.1"
    assert precip["2026-01-01T02:00:00Z"].value == 0.0
    assert precip["2026-01-01T02:00:00Z"].source_raw == "-0.01"
    assert precip["2026-01-01T03:00:00Z"].value == -0.2
    assert precip["2026-01-01T03:00:00Z"].source_raw == "-0.2"


def test_open_meteo_historical_trace_negative_precip_clamp_preserves_raw() -> None:
    async def _run() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.host == "previous-runs-api.open-meteo.com"
            assert request.url.params["hourly"] == "precipitation_previous_day1"
            return httpx.Response(
                200,
                json={
                    "latitude": 47.1,
                    "longitude": 25.1,
                    "elevation": 901.0,
                    "hourly": {
                        "time": ["2026-01-02T01:00", "2026-01-02T02:00"],
                        "precipitation_previous_day1": [-0.1, -0.2],
                    },
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            historical = await OpenMeteoAdapter(client).fetch_historical(
                ForecastRequest(
                    lat=47,
                    lon=25,
                    model="jma_gsm",
                    variables=("precip",),
                    max_lead_hours=24,
                ),
                window_start="2026-01-02T00:00:00Z",
                window_end="2026-01-03T00:00:00Z",
            )

        assert historical is not None
        precip = {sample.valid_at: sample for sample in historical.samples}
        assert precip["2026-01-02T01:00:00Z"].value == 0.0
        assert precip["2026-01-02T01:00:00Z"].source_raw == "-0.1 previous_day1"
        assert precip["2026-01-02T02:00:00Z"].value == -0.2
        assert precip["2026-01-02T02:00:00Z"].source_raw == "-0.2 previous_day1"

    asyncio.run(_run())


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
        assert open_values["precip"] == 0.2
        assert {sample.valid_at for sample in open_meteo} == {"2026-01-01T01:00:00Z"}

        def open_meteo_history_handler(request: httpx.Request) -> httpx.Response:
            assert request.url.host == "previous-runs-api.open-meteo.com"
            assert request.url.params["start_date"] == "2026-01-01"
            assert request.url.params["end_date"] == "2026-01-04"
            assert "temperature_2m_previous_day2" in request.url.params["hourly"].split(
                ","
            )
            return httpx.Response(
                200,
                json={
                    "latitude": 47.1,
                    "longitude": 25.1,
                    "elevation": 901.0,
                    "hourly": {
                        "time": ["2026-01-03T00:00"],
                        "temperature_2m_previous_day2": [4.0],
                        "wind_speed_10m_previous_day2": [36.0],
                        "precipitation_previous_day2": [0.3],
                    },
                },
            )

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(open_meteo_history_handler)
        ) as client:
            historical = await OpenMeteoAdapter(client).fetch_historical(
                ForecastRequest(
                    lat=47,
                    lon=25,
                    model="ecmwf_ifs",
                    variables=("temperature", "wind", "precip"),
                    max_lead_hours=48,
                ),
                window_start="2026-01-01T00:00:00Z",
                window_end="2026-01-04T00:00:00Z",
            )
        assert historical is not None
        assert historical.grid is not None
        assert historical.grid.grid_elevation_m == 901.0
        historical_values = {sample.variable: sample for sample in historical.samples}
        assert historical_values["temperature"].issued_at == "2026-01-01T00:00:00Z"
        assert historical_values["temperature"].valid_at == "2026-01-03T00:00:00Z"
        assert historical_values["temperature"].lead_hours == 48
        assert historical_values["wind"].value == pytest.approx(10.0)
        assert historical_values["precip"].value == 0.3

        meteoblue_payload = {
            "metadata": {
                "models": ["gfs", "icon"],
                "modelrun_utc": ["2026-01-01 00:00", "2026-01-01 00:00"],
                "latitude": 47.05,
                "longitude": 25.05,
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
                    lat=47,
                    lon=25,
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
                VALUES ('M7', 47, 25, 900, 'UTC')
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


def test_options_boot_apply_bad_options_and_packaging_files(tmp_path: Path) -> None:
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
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Boot', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' LIMIT 1"
        ).fetchone()["id"]
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO score_cache
            (site_id, feed_id, variable, day_ahead, window_key, n, computed_at)
        VALUES (?, ?, 'temperature', 0, 'w:30', 1, '2026-01-01T00:00:00Z')
        """,
        (site_id, feed_id),
    )

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
        assert (
            get_db().read_sync(
                lambda db_conn: db_conn.execute(
                    "SELECT COUNT(*) AS n FROM score_cache WHERE window_key='w:30'"
                ).fetchone()["n"]
            )
            == 0
        )

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

    # wxverify is wired into monorepo CI in the bundled de-nest PR (§5.2/§5.3).
    # builder.yaml MONITORED_FILES includes wxverify, pyproject.toml, and uv.lock;
    # lint.yaml carries a wxverify-gates job.
    workflow_dir = repo.parent / ".github" / "workflows"
    builder = (workflow_dir / "builder.yaml").read_text(encoding="utf-8")
    lint = (workflow_dir / "lint.yaml").read_text(encoding="utf-8")
    assert "wxverify" in builder
    assert "pyproject.toml" in builder
    assert "uv.lock" in builder
    assert "wxverify-gates" in lint


# ---------------------------------------------------------------------------
# Bucket-1 add-on contract / oracle net (§11a)
# ---------------------------------------------------------------------------


def test_health_backoffs_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """GET /api/health/backoffs: empty→[], single row returned exactly, multiple rows
    ordered by next_attempt_at ASC (§7.1 / §11a-A)."""
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


def test_worker_status_pins_api_prefix_empty_queue_and_null_pair_score(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Worker-status regression pins (§11a-A / S03):
    - /worker/status (no /api prefix) → 404
    - empty jobs table → {"jobs": {}} (keys absent, not 0)
    - no completed pair_and_score → last_completed_pair_and_score_at is null
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


def test_button_typed_job_oracle_and_leaderboard_negative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Button typed-job oracle + paired negative (§11a-B / T01 / S02):
    - POST /api/catchup → 'catchup' typed row in jobs (not just pending count)
    - POST /api/sites/<id>/backfill → 'backfill_site' row for that site_id
    - GET /api/leaderboard (cache-miss) → NO catchup/backfill_site row created
    """
    close_db()
    config.db_path = str(tmp_path / "oracle.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = int(
                conn.execute(
                    """
                    INSERT INTO sites
                        (name, forecast_lat, forecast_lon, elevation_m, timezone)
                    VALUES ('Oracle', 47.0, 25.0, 900.0, 'UTC')
                    """
                ).lastrowid
            )
            feed_id = int(
                conn.execute(
                    "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT 1"
                ).fetchone()["id"]
            )
            error = 1.0
            conn.execute(
                """
                INSERT INTO forecast_pairs
                    (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                     day_ahead, forecast, observed, error, abs_error, sq_error)
                VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z',
                        '2035-01-02T00:00:00Z', 24, 1, 11.0, 10.0, ?, ?, ?)
                """,
                (site_id, feed_id, error, abs(error), error * error),
            )
            return site_id

        site_id = db.write_sync(_seed)
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}

        assert client.post("/api/catchup", headers=headers).status_code == 200
        catchup_row = db.read_sync(
            lambda conn: conn.execute(
                "SELECT type, site_id FROM jobs WHERE type='catchup'"
            ).fetchone()
        )
        assert catchup_row is not None
        assert catchup_row["type"] == "catchup"

        assert (
            client.post(f"/api/sites/{site_id}/backfill", headers=headers).status_code
            == 200
        )
        backfill_row = db.read_sync(
            lambda conn: conn.execute(
                "SELECT type, site_id FROM jobs WHERE type='backfill_site'"
            ).fetchone()
        )
        assert backfill_row is not None
        assert backfill_row["type"] == "backfill_site"
        assert backfill_row["site_id"] == site_id

        db.write_sync(lambda conn: conn.execute("DELETE FROM jobs"))

        assert (
            client.get(
                "/api/leaderboard",
                params={"site": site_id, "variable": "temperature", "lead": "D+1"},
            ).status_code
            == 200
        )
        job_types = db.read_sync(
            lambda conn: {
                str(row["type"])
                for row in conn.execute("SELECT DISTINCT type FROM jobs").fetchall()
            }
        )
        assert "catchup" not in job_types
        assert "backfill_site" not in job_types
        assert "pair_and_score" in job_types


def test_csrf_garbage_token_rejects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CSRF negative triple item (2): wrong/garbage token → 403 (§11a-C / T06).

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
    """translations/en.yaml configuration keys == config.yaml options keys (§11a-F).

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
