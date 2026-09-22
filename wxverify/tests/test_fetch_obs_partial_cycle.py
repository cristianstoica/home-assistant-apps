"""Tests for Item B -- one station's HTTP 204 must not abort the site's
observation cycle (plan section 6, B.10 roster).

This file covers **B-T1 through B-T34**. B-T18 has three parts; the
first two (``progress=3/3`` and ``progress=2/3``) already live in
``tests/test_upstream_payload_error.py`` and are not repeated here -- only
the third case (``progress=2/6``) is authored below.

Every station id, coordinate, API key and host literal below is synthetic:
station ids ``ISTATION01``-``ISTATION06``, site timezone ``UTC``,
coordinates ``0.0``/``0.0``, key ``0123456789abcdef0123456789abcdef``.

Per the plan's closing paragraph on B.10, every expected value below is
derived by reading the actual production function bodies for the seeded
fixture, not copied from the plan document -- each test's docstring or
inline comment names the function read and what it produces here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from wxverify import config
from wxverify.collection.budget import is_refundable_transport_error
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.migrations import create_schema, run_migrations
from wxverify.db.queue import Job
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.monitor import build_verdict
from wxverify.obs.pws_adapter import (
    HOURLY_HISTORY_PATH,
    HOURLY_HISTORY_URL,
    PayloadDiagnostics,
    UpstreamPayloadError,
    is_hourly_history_no_content,
)
from wxverify.scoring.consensus import insert_station_observation
from wxverify.verification.truth import (
    materialize_daily_truth,
    regenerate_marked_truth_chunk,
)
from wxverify.worker.processor import (
    _park_station_history,  # noqa: PLC2701 -- private, exercised directly (see B-T10)
    _retention_hours,  # noqa: PLC2701 -- private, exercised directly (see B-T31)
    dispatch,
)
from wxverify.worker.scheduler import (
    _enqueue_due_obs,  # noqa: PLC2701 -- see other suites
)
from wxverify.worker.verification_run import (
    _clear_state,  # noqa: PLC2701 -- private, exercised directly (see B-T34)
    _load_state,  # noqa: PLC2701 -- private, exercised directly (see B-T33/B-T34)
    advance_verification,
)

_API_KEY = "0123456789abcdef0123456789abcdef"
_TZ = "UTC"


# ---------------------------------------------------------------------------
# Shared harness
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _weathercom_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)


@pytest.fixture(autouse=True)
def _fast_pace(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_pace(site_id: int, station_id: int, ordinal: int) -> None:
        return None

    monkeypatch.setattr("wxverify.worker.processor.pace_station_call", _fake_pace)


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 -- tests inspect the real writer connection


def _freeze(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    """Freeze ``utc_now()`` in both modules that call it on the fetch_obs path.

    ``processor.py`` and ``pws_adapter.py`` each import ``utc_now`` by name
    (``from wxverify.core.timeutil import ... utc_now``), so each module's
    own reference must be patched separately -- patching
    ``wxverify.core.timeutil.utc_now`` alone would not reach either call
    site. ``isoformat_utc()`` called with NO argument inside
    ``processor.py`` (``_complete_obs_cycle``'s ``last_obs_cycle_at``,
    ``_persist_station_observations``'s ``last_run_at``) is NOT covered by
    this freeze: ``isoformat_utc`` is `wxverify.core.timeutil`'s own
    function and calls timeutil's own (unpatched) ``utc_now`` internally --
    those two stamps are real wall-clock, by construction, on every test
    below. ``isoformat_utc(utc_now() + delay)`` (the park path) and
    ``isoformat_utc(utc_now().replace(microsecond=0))`` (the freshness
    watermark) DO pass an explicit value, so they ARE frozen.
    """
    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: when)


def _seed_site_and_stations(
    conn: sqlite3.Connection, station_ids: list[str]
) -> tuple[int, dict[str, int]]:
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Cluster', 0.0, 0.0, 0.0, ?)
            """,
            (_TZ,),
        ).lastrowid
    )
    ids: dict[str, int] = {}
    for pws_id in station_ids:
        ids[pws_id] = int(
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m)
                VALUES (?, ?, 0.0, 0.0, 0.0)
                """,
                (site_id, pws_id),
            ).lastrowid
        )
    return site_id, ids


def _obs_response(readings: list[tuple[str, float]]) -> httpx.Response:
    """A 200 whose body decodes to one temperature observation per reading.

    Only ``metric.temp`` is populated, so ``observations_from_payload``
    (``wxverify/obs/pws_adapter.py:393-464``) emits exactly one
    ``PwsObservation`` (variable ``"temperature"``) per row -- wind/precip
    are left out so a station's row count stays 1:1 with its reading count.
    """
    body = {
        "observations": [
            {"obsTimeUtc": stamp, "metric": {"temp": value}}
            for stamp, value in readings
        ]
    }
    return httpx.Response(
        200,
        content=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
    )


def _empty_response() -> httpx.Response:
    return httpx.Response(200, content=b'{"observations": []}')


def _no_content_response() -> httpx.Response:
    return httpx.Response(204)


def _run_fetch_obs(
    db: object, writer: FencedWriter, site_id: int, handler: object, *, job_id: int = 1
) -> None:
    real_client = httpx.AsyncClient
    with patch(
        "wxverify.worker.processor.httpx.AsyncClient",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),  # type: ignore[arg-type]
    ):
        asyncio.run(
            dispatch(
                db,  # type: ignore[arg-type]
                writer,
                Job(
                    id=job_id,
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


def _station_state(conn: sqlite3.Connection, station_id: int) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT history_next_attempt_at, history_last_error, history_error_count,
               last_run_at, last_error, error_count
        FROM stations WHERE id=?
        """,
        (station_id,),
    ).fetchone()
    assert row is not None
    return row


def _site_state(conn: sqlite3.Connection, site_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT last_obs_at, last_obs_cycle_at FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    assert row is not None
    return row


def _budget_calls(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COALESCE(SUM(calls), 0) FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    return int(row[0])


def _condition(verdict: dict[str, object], cond_id: str) -> dict[str, object]:
    conditions = verdict["conditions"]
    assert isinstance(conditions, list)
    matches = [c for c in conditions if isinstance(c, dict) and c.get("id") == cond_id]
    assert len(matches) == 1, f"{cond_id} must be present exactly once"
    return matches[0]


# ---------------------------------------------------------------------------
# B-T1
# ---------------------------------------------------------------------------


def test_204_station_does_not_abort_the_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    station_ids = [f"ISTATION0{i}" for i in range(1, 7)]
    site_id, ids = _seed_site_and_stations(conn, station_ids)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        call_log.append(station)
        if station == "ISTATION02":
            return _no_content_response()
        return _obs_response([("2026-09-22T11:00:00Z", 10.0)])

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, handler)  # must not raise

    assert call_log == station_ids  # _enabled_stations orders by pws_station_id
    # ISTATION01's row, written before the 204, is still present.
    row1 = conn.execute(
        "SELECT 1 FROM station_observations WHERE station_id=? AND valid_at=?",
        (ids["ISTATION01"], "2026-09-22T11:00:00Z"),
    ).fetchone()
    assert row1 is not None
    # Stations 3-6 were all fetched (attempted after the 204).
    for pws_id in ("ISTATION03", "ISTATION04", "ISTATION05", "ISTATION06"):
        assert _station_state(conn, ids[pws_id])["last_run_at"] is not None
    # ISTATION02 was parked, not fetched.
    row2 = _station_state(conn, ids["ISTATION02"])
    assert row2["last_run_at"] is None
    assert row2["history_error_count"] == 1


# ---------------------------------------------------------------------------
# B-T2
# ---------------------------------------------------------------------------


def test_first_cycle_reserves_once_per_attempted_station(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_reserve_obs_call`` (processor.py:875-893) reserves 1 call via
    ``reserve_budget(conn, "weathercom", 1)`` for every station reached,
    BEFORE the HTTP outcome is known -- so the 204 station reserves too."""
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    station_ids = [f"ISTATION0{i}" for i in range(1, 7)]
    site_id, _ = _seed_site_and_stations(conn, station_ids)

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == "ISTATION02":
            return _no_content_response()
        return _obs_response([("2026-09-22T11:00:00Z", 10.0)])

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, handler)

    assert _budget_calls(conn) == 6


# ---------------------------------------------------------------------------
# B-T3
# ---------------------------------------------------------------------------


def test_second_cycle_inside_park_window_skips_the_parked_reservation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    station_ids = [f"ISTATION0{i}" for i in range(1, 7)]
    site_id, _ = _seed_site_and_stations(conn, station_ids)

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == "ISTATION02":
            return _no_content_response()
        return _obs_response([("2026-09-22T11:00:00Z", 10.0)])

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, handler, job_id=1)
    after_cycle1 = _budget_calls(conn)
    assert after_cycle1 == 6

    # ISTATION02 parked at NOW + 1h (rung 1); stay inside that window.
    _freeze(monkeypatch, now + timedelta(minutes=10))
    _run_fetch_obs(db, writer, site_id, handler, job_id=2)
    after_cycle2 = _budget_calls(conn)

    assert after_cycle2 - after_cycle1 == 5


# ---------------------------------------------------------------------------
# B-T4
# ---------------------------------------------------------------------------


def test_partial_cycle_advances_both_clocks_to_the_survivors_own_valid_at(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``fetch_hourly_history`` (pws_adapter.py:287-331) hour-floors via
    ``_valid_at`` before ``_persist_station_observations`` ever runs, so the
    survivor's reading at 14:15Z floors to 14:00Z -- one hour behind the
    15:00Z test clock, deliberately not equal to the fetch clock itself."""
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 15, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, ids = _seed_site_and_stations(conn, ["ISTATION01", "ISTATION02"])

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == "ISTATION02":
            return _no_content_response()
        return _obs_response([("2026-09-22T14:15:00Z", 12.0)])

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, handler)

    site = _site_state(conn, site_id)
    assert site["last_obs_at"] == "2026-09-22T14:00:00Z"
    assert site["last_obs_cycle_at"] is not None

    _enqueue_due_obs(conn)
    pending = conn.execute(
        "SELECT 1 FROM jobs WHERE type='fetch_obs' AND site_id=?", (site_id,)
    ).fetchone()
    assert pending is None


# ---------------------------------------------------------------------------
# B-T5
# ---------------------------------------------------------------------------


def test_all_204_cycle_leaves_freshness_byte_identical(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 10, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, ids = _seed_site_and_stations(conn, ["ISTATION01", "ISTATION02"])

    def handler_success(request: httpx.Request) -> httpx.Response:
        return _obs_response([("2026-09-22T09:45:00Z", 12.0)])

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, handler_success, job_id=1)
    site_after_1 = _site_state(conn, site_id)
    assert site_after_1["last_obs_at"] == "2026-09-22T09:00:00Z"
    cycle_at_1 = site_after_1["last_obs_cycle_at"]
    assert cycle_at_1 is not None

    def handler_204(request: httpx.Request) -> httpx.Response:
        return _no_content_response()

    _run_fetch_obs(db, writer, site_id, handler_204, job_id=2)
    site_after_2 = _site_state(conn, site_id)

    assert site_after_2["last_obs_at"] == site_after_1["last_obs_at"]
    assert site_after_2["last_obs_cycle_at"] != cycle_at_1
    assert site_after_2["last_obs_cycle_at"] > cycle_at_1

    _enqueue_due_obs(conn)
    assert (
        conn.execute(
            "SELECT 1 FROM jobs WHERE type='fetch_obs' AND site_id=?", (site_id,)
        ).fetchone()
        is None
    )

    verdict = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=False,
        db_enabled=False,
        now=now + timedelta(hours=13),  # past OBS_STALE_HOURS=12 (monitor.py:30)
        export_sweeper_dead=None,
    )
    obs_stale = _condition(verdict, "obs_stale")
    assert obs_stale["ok"] is False
    assert obs_stale["count"] == 1


# ---------------------------------------------------------------------------
# B-T6
# ---------------------------------------------------------------------------


def test_all_parked_cycle_completes_without_raising(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, lambda r: _no_content_response(), job_id=1)
    calls_after_park = _budget_calls(conn)
    cycle_at_1 = _site_state(conn, site_id)["last_obs_cycle_at"]

    # Still inside the park window: the only station is skipped entirely.
    _freeze(monkeypatch, now + timedelta(minutes=10))
    _run_fetch_obs(db, writer, site_id, lambda r: _no_content_response(), job_id=2)

    assert _budget_calls(conn) == calls_after_park  # nothing reserved
    cycle_at_2 = _site_state(conn, site_id)["last_obs_cycle_at"]
    assert cycle_at_2 is not None
    assert cycle_at_2 >= cycle_at_1  # still advances (real wall-clock stamp)


# ---------------------------------------------------------------------------
# B-T7
# ---------------------------------------------------------------------------


def test_monitor_condition_visible_through_four_phases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mirrors the ``record_gap_scan_degraded`` four-phase pattern at
    ``tests/test_record_lifecycle_oracles.py:638-680``, keyed on
    ``obs_station_history_failing`` instead."""
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, ids = _seed_site_and_stations(conn, ["ISTATION01", "ISTATION02"])

    def handler_partial(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == "ISTATION02":
            return _no_content_response()
        return _obs_response([("2026-09-22T11:00:00Z", 10.0)])

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, handler_partial, job_id=1)

    # Phase 1: present, count 1.
    verdict1 = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=False,
        db_enabled=False,
        now=now,
        export_sweeper_dead=None,
    )
    cond1 = _condition(verdict1, "obs_station_history_failing")
    assert cond1["ok"] is False
    assert cond1["count"] == 1

    # Phase 2: clock advanced past history_next_attempt_at, no further cycle.
    # _station_history_failures (monitor.py:143-171) counts
    # history_error_count > 0 -- NOT a deadline comparison -- so this must
    # still read count 1.
    next_attempt = _station_state(conn, ids["ISTATION02"])["history_next_attempt_at"]
    past_deadline = parse_utc(str(next_attempt)) + timedelta(minutes=1)
    verdict2 = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=False,
        db_enabled=False,
        now=past_deadline,
        export_sweeper_dead=None,
    )
    cond2 = _condition(verdict2, "obs_station_history_failing")
    assert cond2["ok"] is False
    assert cond2["count"] == 1

    # Phase 3: the station recovers.
    _freeze(monkeypatch, past_deadline)

    def handler_recovered(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == "ISTATION02":
            return _obs_response([("2026-09-22T13:00:00Z", 11.0)])
        return _obs_response([("2026-09-22T13:00:00Z", 10.0)])

    _run_fetch_obs(db, writer, site_id, handler_recovered, job_id=2)
    verdict3 = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=False,
        db_enabled=False,
        now=past_deadline,
        export_sweeper_dead=None,
    )
    cond3 = _condition(verdict3, "obs_station_history_failing")
    assert cond3["ok"] is True
    assert cond3["count"] == 0

    # Phase 4: monitor_pipeline disabled -- present, skipped=True (build_verdict,
    # monitor.py:636-650), regardless of overall severity.
    verdict4 = build_verdict(
        conn,
        pipeline_enabled=False,
        budget_enabled=False,
        db_enabled=False,
        now=now,
        export_sweeper_dead=None,
    )
    cond4 = _condition(verdict4, "obs_station_history_failing")
    assert cond4["skipped"] is True


# ---------------------------------------------------------------------------
# B-T8
# ---------------------------------------------------------------------------


def test_partial_cycle_scoring_counts_only_stations_that_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``insert_station_observation`` (consensus.py:162-223) calls
    ``materialize_consensus`` inline on every write, so the ``observations``
    row for this hour already reflects the cycle by the time dispatch
    returns -- no separate score job needs to run for this assertion."""
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, ids = _seed_site_and_stations(conn, ["ISTATION01", "ISTATION02"])

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == "ISTATION02":
            return _no_content_response()
        return _obs_response([("2026-09-22T11:00:00Z", 10.0)])

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, handler)

    pair_job = conn.execute(
        "SELECT 1 FROM jobs WHERE type='pair_and_score' AND site_id=?", (site_id,)
    ).fetchone()
    assert pair_job is not None

    row = conn.execute(
        "SELECT n_stations FROM observations "
        "WHERE site_id=? AND variable='temperature' AND valid_at=?",
        (site_id, "2026-09-22T11:00:00Z"),
    ).fetchone()
    assert row is not None
    assert row["n_stations"] == 1  # only ISTATION01 reported; no carry-forward


# ---------------------------------------------------------------------------
# B-T9
# ---------------------------------------------------------------------------


def test_parking_survives_a_process_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, ids = _seed_site_and_stations(conn, ["ISTATION01", "ISTATION02"])

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == "ISTATION02":
            return _no_content_response()
        return _obs_response([("2026-09-22T11:00:00Z", 10.0)])

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, handler, job_id=1)
    calls_after_1 = _budget_calls(conn)

    # Reopen the same file: this is genuinely a new Database/FencedWriter,
    # not the same in-memory object.
    db_path = str(config.db_path)
    close_db()
    db2 = init_db(db_path)
    writer2 = FencedWriter(db2, db2.generation)
    conn2 = db2._conn  # noqa: SLF001

    _freeze(monkeypatch, now + timedelta(minutes=10))  # still inside park window
    _run_fetch_obs(db2, writer2, site_id, handler, job_id=2)

    assert _budget_calls(conn2) - calls_after_1 == 1  # only ISTATION01 reserved
    assert _station_state(conn2, ids["ISTATION02"])["last_run_at"] is None


# ---------------------------------------------------------------------------
# B-T10
# ---------------------------------------------------------------------------


def _v6_db() -> sqlite3.Connection:
    """A genuinely-v6 database: a fresh schema (which already has the four
    new columns) with each one dropped by a real ``ALTER TABLE ... DROP
    COLUMN``, then ``user_version`` rolled back to 6 -- mirrors
    ``tests/test_daily_truth_admission_migration.py``'s ``_v5_db()``, for
    the same reason its module docstring gives: calling ``create_schema``
    already contains the new columns, so a column-probe against it would
    no-op unless they are genuinely removed first.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    conn.execute("ALTER TABLE sites DROP COLUMN last_obs_cycle_at")
    conn.execute("ALTER TABLE stations DROP COLUMN history_next_attempt_at")
    conn.execute("ALTER TABLE stations DROP COLUMN history_last_error")
    conn.execute("ALTER TABLE stations DROP COLUMN history_error_count")
    conn.execute("PRAGMA user_version = 6")
    return conn


def test_backoff_ladder_on_a_migrated_database(monkeypatch: pytest.MonkeyPatch) -> None:
    """Seeds the station BEFORE ``run_migrations`` brings the file to v7, so
    its ``history_error_count`` comes from the migration's ``DEFAULT 0``
    rather than from ``create_tables`` -- and asserts the FIRST park first,
    per B-T10: against a nullable migrated column that call raises
    ``TypeError`` immediately, rather than failing deep in the sequence."""
    conn = _v6_db()
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Cluster', 0.0, 0.0, 0.0, ?)
            """,
            (_TZ,),
        ).lastrowid
    )
    station_id = int(
        conn.execute(
            """
            INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)
            VALUES (?, 'ISTATION01', 0.0, 0.0, 0.0)
            """,
            (site_id,),
        ).lastrowid
    )
    conn.commit()

    run_migrations(conn)
    conn.commit()

    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: now)

    expected_hours = [1, 2, 4, 8, 16, 24, 24]
    for expected_count, delay_hours in enumerate(expected_hours, start=1):
        park = _park_station_history(conn, station_id, "synthetic reason")
        assert park.error_count == expected_count
        assert park.next_attempt_at == isoformat_utc(now + timedelta(hours=delay_hours))
    conn.commit()

    row = conn.execute(
        "SELECT history_error_count FROM stations WHERE id=?", (station_id,)
    ).fetchone()
    assert row["history_error_count"] == 7


# ---------------------------------------------------------------------------
# B-T11
# ---------------------------------------------------------------------------


def test_recovery_clears_the_trio_both_halves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    db = get_db()
    writer = FencedWriter(db, db.generation)

    # --- half (a): a duplicate-only 200 still clears the trio. -------------
    reading = ("2026-09-22T11:00:00Z", 10.0)
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response([reading]), job_id=1)

    _freeze(monkeypatch, now + timedelta(minutes=1))
    _run_fetch_obs(db, writer, site_id, lambda r: _no_content_response(), job_id=2)
    parked = _station_state(conn, station_id)
    assert parked["history_error_count"] == 1

    conn.execute("DELETE FROM jobs")  # isolate cycle 3's own enqueue decision
    conn.commit()
    _freeze(monkeypatch, now + timedelta(hours=1, minutes=2))  # past the 1h park
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response([reading]), job_id=3)

    recovered = _station_state(conn, station_id)
    assert recovered["history_next_attempt_at"] is None
    assert recovered["history_last_error"] is None
    assert recovered["history_error_count"] == 0
    assert recovered["last_run_at"] is not None
    # The rows are byte-identical to cycle 1's, so insert_station_observation
    # (consensus.py:195-201) returned False for all of them: nothing changed,
    # so no pair_and_score was (re-)enqueued this cycle.
    assert (
        conn.execute("SELECT 1 FROM jobs WHERE type='pair_and_score'").fetchone()
        is None
    )

    # --- half (b): a 200 fully outside the recency window re-parks. --------
    site_id2, (station_id2,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION02"])
    )
    _freeze(monkeypatch, now)
    _run_fetch_obs(db, writer, site_id2, lambda r: _no_content_response(), job_id=4)
    assert _station_state(conn, station_id2)["history_error_count"] == 1

    _freeze(monkeypatch, now + timedelta(hours=1, minutes=1))  # past the 1h park
    # No watermark was ever set (the station has never had usable data), so
    # _retention_hours (processor.py:917-931) returns
    # _HISTORY_RETENTION_MAX_HOURS=168; 200h back is outside that cutoff.
    cutoff_base = now + timedelta(hours=1, minutes=1)
    stale_stamp = isoformat_utc(cutoff_base - timedelta(hours=200))
    _run_fetch_obs(
        db, writer, site_id2, lambda r: _obs_response([(stale_stamp, 9.0)]), job_id=5
    )
    reparked = _station_state(conn, station_id2)
    assert reparked["history_error_count"] == 2  # incremented, not reset
    assert reparked["last_run_at"] is not None  # persist still ran


# ---------------------------------------------------------------------------
# B-T12 / B-T13 / B-T14 / B-T15 -- allowlist unit tests
# ---------------------------------------------------------------------------


def _diagnostics(*, kind: str, endpoint: str) -> PayloadDiagnostics:
    return PayloadDiagnostics(
        kind=kind,  # type: ignore[arg-type]
        station_id="ISTATION01",
        endpoint=endpoint,
        status=204 if kind == "no_content" else 200,
        content_type="absent",
        body="empty",
        body_bytes=0,
        elapsed_ms=0,
        request_id=None,
        reason=None,
        pos=None,
    )


def test_range_endpoint_204_is_not_the_hourly_history_allowlist_match() -> None:
    exc = UpstreamPayloadError(
        _diagnostics(kind="no_content", endpoint="/v2/pws/history/hourly")
    )
    assert is_hourly_history_no_content(exc) is False


@pytest.mark.parametrize("kind", ["json_decode", "invalid_structure", "provider_error"])
def test_non_no_content_kinds_from_the_history_endpoint_are_not_parked_and_abort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    # The allowlist predicate itself:
    exc = UpstreamPayloadError(_diagnostics(kind=kind, endpoint=HOURLY_HISTORY_PATH))
    assert is_hourly_history_no_content(exc) is False

    # And end to end: the cycle aborts, and the station is never parked.
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )

    bodies = {
        "json_decode": b"",
        "invalid_structure": b"{}",
        "provider_error": b'{"errors": "synthetic upstream failure"}',
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=bodies[kind])

    db = get_db()
    writer = FencedWriter(db, db.generation)
    with pytest.raises(UpstreamPayloadError):
        _run_fetch_obs(db, writer, site_id, handler)

    assert _station_state(conn, station_id)["history_error_count"] == 0


def test_hourly_history_url_and_path_agree() -> None:
    assert httpx.URL(HOURLY_HISTORY_URL).path == HOURLY_HISTORY_PATH


def test_refund_policy_unchanged_for_upstream_payload_error() -> None:
    exc = UpstreamPayloadError(
        _diagnostics(kind="invalid_structure", endpoint=HOURLY_HISTORY_PATH)
    )
    assert is_refundable_transport_error(exc) is False


# ---------------------------------------------------------------------------
# B-T16
# ---------------------------------------------------------------------------


def test_unparseable_next_attempt_at_fails_open_and_is_attempted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    conn.execute(
        "UPDATE stations SET history_next_attempt_at='not-a-timestamp' WHERE id=?",
        (station_id,),
    )
    conn.commit()

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(
        db,
        writer,
        site_id,
        lambda r: _obs_response([("2026-09-22T11:00:00Z", 10.0)]),
    )

    assert _station_state(conn, station_id)["last_run_at"] is not None


# ---------------------------------------------------------------------------
# B-T17
# ---------------------------------------------------------------------------


def test_migration_and_fresh_columns_are_defined_identically() -> None:
    fresh = sqlite3.connect(":memory:")
    fresh.row_factory = sqlite3.Row
    fresh.execute("PRAGMA foreign_keys=ON")
    run_migrations(fresh)

    migrated = _v6_db()
    site_id = int(
        migrated.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                                timezone, last_obs_at)
            VALUES ('Cluster', 0.0, 0.0, 0.0, 'UTC', '2026-09-20T00:00:00Z')
            """
        ).lastrowid
    )
    migrated.execute(
        """
        INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)
        VALUES (?, 'ISTATION01', 0.0, 0.0, 0.0)
        """,
        (site_id,),
    )
    migrated.commit()
    run_migrations(migrated)

    def _columns(conn: sqlite3.Connection, table: str) -> dict[str, tuple[int, object]]:
        return {
            str(row["name"]): (int(row["notnull"]), row["dflt_value"])
            for row in conn.execute(f"PRAGMA table_info({table})")
        }

    fresh_sites_cols = _columns(fresh, "sites")
    fresh_stations_cols = _columns(fresh, "stations")
    migrated_sites_cols = _columns(migrated, "sites")
    migrated_stations_cols = _columns(migrated, "stations")

    for name in ["last_obs_cycle_at"]:
        assert migrated_sites_cols[name] == fresh_sites_cols[name], name
    for name in [
        "history_next_attempt_at",
        "history_last_error",
        "history_error_count",
    ]:
        assert migrated_stations_cols[name] == fresh_stations_cols[name], name

    assert migrated_stations_cols["history_error_count"] == (1, "0")
    assert migrated_stations_cols["history_next_attempt_at"] == (0, None)
    assert migrated_stations_cols["history_last_error"] == (0, None)
    assert migrated_sites_cols["last_obs_cycle_at"] == (0, None)

    null_count = migrated.execute(
        "SELECT COUNT(*) FROM stations WHERE history_error_count IS NULL"
    ).fetchone()[0]
    assert null_count == 0

    site_row = migrated.execute(
        "SELECT last_obs_at, last_obs_cycle_at FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    seeded_stamp = "2026-09-20T00:00:00Z"
    assert site_row["last_obs_cycle_at"] == seeded_stamp
    assert site_row["last_obs_at"] == seeded_stamp

    before = dict(site_row)
    run_migrations(migrated)  # re-running changes nothing
    site_row_again = migrated.execute(
        "SELECT last_obs_at, last_obs_cycle_at FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    assert dict(site_row_again) == before


# ---------------------------------------------------------------------------
# B-T18 (third case only -- see module docstring)
# ---------------------------------------------------------------------------


def test_progress_note_reports_second_of_six(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.core.error_sanitize import sanitized_exception

    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    station_ids = [f"ISTATION0{i}" for i in range(1, 7)]
    site_id, ids = _seed_site_and_stations(conn, station_ids)

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == "ISTATION02":
            return httpx.Response(200, content=b"")  # json_decode -> raises, not parked
        return _obs_response([("2026-09-22T11:00:00Z", 10.0)])

    db = get_db()
    writer = FencedWriter(db, db.generation)
    with pytest.raises(UpstreamPayloadError) as info:
        _run_fetch_obs(db, writer, site_id, handler)

    assert sanitized_exception(info.value).endswith("station=ISTATION02 progress=2/6")


# ---------------------------------------------------------------------------
# B-T19
# ---------------------------------------------------------------------------


def test_decoded_but_empty_after_cutoff_is_not_a_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )

    # First-ever fetch: no watermark, so _retention_hours returns
    # _HISTORY_RETENTION_MAX_HOURS=168 (processor.py:917-931); a reading 200h
    # back is outside that cutoff and fetch_hourly_history's own filter
    # (pws_adapter.py:317-325) drops it before persistence ever sees it.
    stale_stamp = isoformat_utc(now - timedelta(hours=200))

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response([(stale_stamp, 9.0)]))

    site = _site_state(conn, site_id)
    assert site["last_obs_cycle_at"] is not None
    assert site["last_obs_at"] is None

    assert (
        conn.execute("SELECT 1 FROM jobs WHERE type='pair_and_score'").fetchone()
        is None
    )
    station = _station_state(conn, station_id)
    assert station["history_error_count"] == 1
    assert station["history_next_attempt_at"] == isoformat_utc(now + timedelta(hours=1))


# ---------------------------------------------------------------------------
# B-T20
# ---------------------------------------------------------------------------


def test_duplicate_payload_does_not_refresh_freshness_but_cycle_clock_advances(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now1 = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now1)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    reading = ("2026-09-22T11:30:00Z", 10.0)

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response([reading]), job_id=1)
    site1 = _site_state(conn, site_id)
    assert site1["last_obs_at"] == "2026-09-22T11:00:00Z"
    cycle_at_1 = site1["last_obs_cycle_at"]

    now2 = now1 + timedelta(hours=2)
    _freeze(monkeypatch, now2)
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response([reading]), job_id=2)
    site2 = _site_state(conn, site_id)

    assert site2["last_obs_at"] == site1["last_obs_at"]  # same string, unchanged
    assert site2["last_obs_cycle_at"] > cycle_at_1  # cycle clock still advanced


# ---------------------------------------------------------------------------
# B-T21
# ---------------------------------------------------------------------------


def test_the_three_outcomes_are_not_collapsed_into_one_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, ids = _seed_site_and_stations(conn, ["ISTATION01", "ISTATION02"])
    reading = ("2026-09-22T11:00:00Z", 10.0)

    db = get_db()
    writer = FencedWriter(db, db.generation)

    # Seed run: writes station 1's reading through the real code path, so the
    # "duplicate" cycle below matches it exactly (value, qc_flag, source_raw)
    # -- insert_station_observation's equality check (consensus.py:195-201).
    def handler_seed(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == "ISTATION01":
            return _obs_response([reading])
        return _obs_response([reading])  # station 2 also gets a clean history

    _run_fetch_obs(db, writer, site_id, handler_seed, job_id=1)
    conn.execute("DELETE FROM jobs")
    conn.commit()

    stale_stamp = isoformat_utc(now - timedelta(hours=200))

    def handler_test(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == "ISTATION01":
            return _obs_response([reading])  # duplicate-only
        return _obs_response([(stale_stamp, 8.0)])  # decodes, nothing in-window

    _run_fetch_obs(db, writer, site_id, handler_test, job_id=2)

    s1 = _station_state(conn, ids["ISTATION01"])
    s2 = _station_state(conn, ids["ISTATION02"])
    assert s1["history_error_count"] == 0  # cleared: usable, even though unchanged
    assert s2["history_error_count"] == 1  # not usable: parked
    assert (
        conn.execute("SELECT 1 FROM jobs WHERE type='pair_and_score'").fetchone()
        is None
    )  # nothing changed this cycle
    site = _site_state(conn, site_id)
    assert site["last_obs_at"] == "2026-09-22T11:00:00Z"  # station 1's newest time


# ---------------------------------------------------------------------------
# B-T22
# ---------------------------------------------------------------------------


def test_alternating_failure_escalates_the_ladder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    db = get_db()
    writer = FencedWriter(db, db.generation)

    handlers = [
        lambda r: _no_content_response(),  # 1: 204
        lambda r: _empty_response(),  # 2: empty-200
        lambda r: _no_content_response(),  # 3: 204
        lambda r: _empty_response(),  # 4: empty-200
    ]
    expected_counts = [1, 2, 3, 4]
    expected_delay_hours = [1, 2, 4, 8]

    clock = now
    for job_id, (handler, expected_count, delay_hours) in enumerate(
        zip(handlers, expected_counts, expected_delay_hours, strict=True), start=1
    ):
        _freeze(monkeypatch, clock)
        _run_fetch_obs(db, writer, site_id, handler, job_id=job_id)
        state = _station_state(conn, station_id)
        assert state["history_error_count"] == expected_count
        assert state["history_next_attempt_at"] == isoformat_utc(
            clock + timedelta(hours=delay_hours)
        )
        clock = parse_utc(str(state["history_next_attempt_at"])) + timedelta(minutes=1)


# ---------------------------------------------------------------------------
# B-T23
# ---------------------------------------------------------------------------


def test_park_transitions_log_exactly_one_warning_each(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    db = get_db()
    writer = FencedWriter(db, db.generation)

    caplog.set_level(logging.WARNING)

    # Cause 1: a 204 park.
    caplog.clear()
    _run_fetch_obs(db, writer, site_id, lambda r: _no_content_response(), job_id=1)
    records = [r for r in caplog.records if r.name == "wxverify.worker.processor"]
    assert len(records) == 1
    msg = records[0].getMessage()
    assert f"station={station_id}" in msg
    assert "errors=1" in msg
    next_attempt = _station_state(conn, station_id)["history_next_attempt_at"]
    assert str(next_attempt) in msg

    # A later cycle inside the park window: skipped, no WARNING at all.
    caplog.clear()
    _freeze(monkeypatch, now + timedelta(minutes=5))
    _run_fetch_obs(db, writer, site_id, lambda r: _no_content_response(), job_id=2)
    records = [r for r in caplog.records if r.name == "wxverify.worker.processor"]
    assert records == []

    # Cause 2: an empty-payload park, once the station is attemptable again.
    caplog.clear()
    _freeze(monkeypatch, now + timedelta(hours=1, minutes=1))
    stale_stamp = isoformat_utc(
        now + timedelta(hours=1, minutes=1) - timedelta(hours=200)
    )
    _run_fetch_obs(
        db, writer, site_id, lambda r: _obs_response([(stale_stamp, 9.0)]), job_id=3
    )
    records = [r for r in caplog.records if r.name == "wxverify.worker.processor"]
    assert len(records) == 1
    msg2 = records[0].getMessage()
    assert "errors=2" in msg2
    next_attempt2 = _station_state(conn, station_id)["history_next_attempt_at"]
    assert str(next_attempt2) in msg2


# ---------------------------------------------------------------------------
# B-T24
# ---------------------------------------------------------------------------


def test_a_future_dated_reading_cannot_set_freshness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 14, 30, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )

    future = ("2026-09-22T15:15:00Z", 20.0)  # floors to 15:00Z, > 14:30Z clock
    past = ("2026-09-22T13:45:00Z", 15.0)  # floors to 13:00Z, in window

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response([future, past]))

    for stamp in ("2026-09-22T15:00:00Z", "2026-09-22T13:00:00Z"):
        row = conn.execute(
            "SELECT 1 FROM station_observations WHERE station_id=? AND valid_at=?",
            (station_id, stamp),
        ).fetchone()
        assert row is not None, stamp  # both rows persisted, exactly as today

    assert _station_state(conn, station_id)["history_error_count"] == 0  # trio cleared
    site = _site_state(conn, site_id)
    assert site["last_obs_at"] == "2026-09-22T13:00:00Z"  # never the future one


# ---------------------------------------------------------------------------
# B-T25
# ---------------------------------------------------------------------------


def test_a_present_hour_reading_is_not_mistaken_for_future(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Freezes the clock at ``14:00:00.312000Z``, one second (well, one
    microsecond field) into the hour. ``_persist_station_observations``
    (processor.py:971-1032) computes ``now_stamp =
    isoformat_utc(utc_now().replace(microsecond=0))`` == ``"...14:00:00Z"``;
    the seeded reading floors to the SAME stamp, so the ``<=`` comparison
    must treat them as equal. Against ``isoformat_utc(utc_now())`` WITHOUT
    the microsecond strip, ``"...14:00:00Z" <= "...14:00:00.312000Z"`` is
    lexically FALSE (``'.' < 'Z'`` under string comparison), so an
    unpatched regression drops the reading and this assertion catches it."""
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 14, 0, 0, 312000, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )

    reading = ("2026-09-22T14:20:00Z", 18.0)  # floors to exactly 14:00:00Z

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response([reading]))

    site = _site_state(conn, site_id)
    assert site["last_obs_at"] == "2026-09-22T14:00:00Z"


# ---------------------------------------------------------------------------
# B-T26 -- B-T31 shared helper
# ---------------------------------------------------------------------------


def _hourly_readings(
    start: datetime, end: datetime, *, value: float = 10.0
) -> list[tuple[str, float]]:
    """One reading per hour in ``[start, end]`` inclusive, all at ``value``
    (a flat series avoids ``qc.py``'s 15-degree spike gate, which is not
    under test here)."""
    readings: list[tuple[str, float]] = []
    hour = start
    while hour <= end:
        readings.append((isoformat_utc(hour), value))
        hour += timedelta(hours=1)
    return readings


# ---------------------------------------------------------------------------
# B-T26
# ---------------------------------------------------------------------------


def test_widened_retention_ingests_the_hours_a_six_hour_window_loses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_retention_hours`` (processor.py:917-931): watermark=t0, now=t0+63h
    gives ``gap_hours=63``, ``retention=min(63+1,168)=64h`` -- a cutoff
    (``fetch_hourly_history``, pws_adapter.py:317) of ``now-64h == t0-1h``,
    which keeps every hour from ``t0+40h`` onward. A fixed 6h window's
    cutoff would instead sit at ``t0+57h`` and could never see ``t0+40h``.
    """
    conn = _init_tmp_db(tmp_path)
    t0 = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, t0)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    db = get_db()
    writer = FencedWriter(db, db.generation)

    _run_fetch_obs(
        db,
        writer,
        site_id,
        lambda r: _obs_response([(isoformat_utc(t0), 5.0)]),
        job_id=1,
    )
    assert _site_state(conn, site_id)["last_obs_at"] == isoformat_utc(t0)

    now = t0 + timedelta(hours=63)
    _freeze(monkeypatch, now)
    readings = _hourly_readings(t0 + timedelta(hours=40), t0 + timedelta(hours=63))
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response(readings), job_id=2)

    stored = {
        str(r["valid_at"])
        for r in conn.execute(
            "SELECT valid_at FROM station_observations WHERE station_id=? "
            "AND variable='temperature' AND valid_at >= ?",
            (station_id, isoformat_utc(t0 + timedelta(hours=40))),
        )
    }
    expected_hours = {isoformat_utc(t0 + timedelta(hours=h)) for h in range(40, 64)}
    assert stored == expected_hours
    assert isoformat_utc(t0 + timedelta(hours=40)) in stored


# ---------------------------------------------------------------------------
# B-T27
# ---------------------------------------------------------------------------


def test_gap_with_no_failure_is_closed_by_the_watermark_not_the_ladder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A window derived from ``history_error_count``/``history_next_attempt_at``
    would stay at the buggy fixed 6h window here -- the counter never leaves
    0, never touching the backoff ladder -- and drop the middle hours; the
    watermark-derived ``_retention_hours`` closes the gap regardless."""
    conn = _init_tmp_db(tmp_path)
    t0 = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, t0)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    db = get_db()
    writer = FencedWriter(db, db.generation)

    _run_fetch_obs(
        db,
        writer,
        site_id,
        lambda r: _obs_response([(isoformat_utc(t0), 5.0)]),
        job_id=1,
    )
    assert _station_state(conn, station_id)["history_error_count"] == 0

    now = t0 + timedelta(hours=30)
    _freeze(monkeypatch, now)
    readings = _hourly_readings(t0 + timedelta(hours=1), t0 + timedelta(hours=30))
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response(readings), job_id=2)

    assert _station_state(conn, station_id)["history_error_count"] == 0
    stored = {
        str(r["valid_at"])
        for r in conn.execute(
            "SELECT valid_at FROM station_observations WHERE station_id=? "
            "AND variable='temperature'",
            (station_id,),
        )
    }
    expected_hours = {isoformat_utc(t0)} | {
        isoformat_utc(t0 + timedelta(hours=h)) for h in range(1, 31)
    }
    assert stored == expected_hours


# ---------------------------------------------------------------------------
# B-T28
# ---------------------------------------------------------------------------


def test_watermark_survives_a_process_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The watermark must come from ``station_observations`` itself, read
    fresh from the database every cycle via ``station.obs_watermark_at``
    (processor.py:554) -- not a module-level dict, the adapter, or the
    previous cycle's in-memory ``StationFetchTarget``, any of which would
    be reset by B-T9's shape of restart and collapse back to the buggy
    fixed window."""
    conn = _init_tmp_db(tmp_path)
    t0 = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, t0)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(
        db,
        writer,
        site_id,
        lambda r: _obs_response([(isoformat_utc(t0), 5.0)]),
        job_id=1,
    )

    db_path = str(config.db_path)
    close_db()
    db2 = init_db(db_path)
    writer2 = FencedWriter(db2, db2.generation)
    conn2 = db2._conn  # noqa: SLF001 -- tests inspect the real writer connection

    now = t0 + timedelta(hours=30)
    _freeze(monkeypatch, now)
    readings = _hourly_readings(t0 + timedelta(hours=1), t0 + timedelta(hours=30))
    _run_fetch_obs(db2, writer2, site_id, lambda r: _obs_response(readings), job_id=2)

    assert _station_state(conn2, station_id)["history_error_count"] == 0
    stored = {
        str(r["valid_at"])
        for r in conn2.execute(
            "SELECT valid_at FROM station_observations WHERE station_id=? "
            "AND variable='temperature'",
            (station_id,),
        )
    }
    expected_hours = {isoformat_utc(t0)} | {
        isoformat_utc(t0 + timedelta(hours=h)) for h in range(1, 31)
    }
    assert stored == expected_hours


# ---------------------------------------------------------------------------
# B-T29
# ---------------------------------------------------------------------------


def test_widened_window_empty_payload_still_parks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Half (a): widening retention must not turn an empty payload into a
    success -- ``usable = bool(observations)`` (processor.py:987) is still
    False regardless of how wide ``hours`` was computed, so the station
    still parks."""
    conn = _init_tmp_db(tmp_path)
    t0 = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, t0)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(
        db,
        writer,
        site_id,
        lambda r: _obs_response([(isoformat_utc(t0), 5.0)]),
        job_id=1,
    )

    conn.execute("DELETE FROM jobs")
    conn.commit()
    now = t0 + timedelta(hours=30)
    _freeze(monkeypatch, now)
    _run_fetch_obs(db, writer, site_id, lambda r: _empty_response(), job_id=2)

    state = _station_state(conn, station_id)
    assert state["history_error_count"] == 1
    assert state["history_next_attempt_at"] == isoformat_utc(now + timedelta(hours=1))
    assert _site_state(conn, site_id)["last_obs_at"] == isoformat_utc(t0)
    assert (
        conn.execute("SELECT 1 FROM jobs WHERE type='pair_and_score'").fetchone()
        is None
    )


def test_widened_window_partial_provider_coverage_stores_only_what_arrived(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Half (b): the provider covers only ``t0+50h..t0+63h``; the older
    ``t0+40h..t0+50h`` part of the same gap is simply absent from this
    provider response -- nothing is invented to fill it, and the station
    still clears (a partial recovery is still a recovery)."""
    conn = _init_tmp_db(tmp_path)
    t0 = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, t0)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(
        db,
        writer,
        site_id,
        lambda r: _obs_response([(isoformat_utc(t0), 5.0)]),
        job_id=1,
    )

    now = t0 + timedelta(hours=63)
    _freeze(monkeypatch, now)
    readings = _hourly_readings(t0 + timedelta(hours=50), t0 + timedelta(hours=63))
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response(readings), job_id=2)

    state = _station_state(conn, station_id)
    assert state["history_error_count"] == 0
    assert state["history_next_attempt_at"] is None

    absent_count = conn.execute(
        "SELECT COUNT(*) FROM station_observations WHERE station_id=? "
        "AND variable='temperature' AND valid_at >= ? AND valid_at < ?",
        (
            station_id,
            isoformat_utc(t0 + timedelta(hours=40)),
            isoformat_utc(t0 + timedelta(hours=50)),
        ),
    ).fetchone()[0]
    assert absent_count == 0

    stored = {
        str(r["valid_at"])
        for r in conn.execute(
            "SELECT valid_at FROM station_observations WHERE station_id=? "
            "AND variable='temperature' AND valid_at >= ?",
            (station_id, isoformat_utc(t0 + timedelta(hours=50))),
        )
    }
    expected = {isoformat_utc(t0 + timedelta(hours=h)) for h in range(50, 64)}
    assert stored == expected
    assert _site_state(conn, site_id)["last_obs_at"] == isoformat_utc(
        t0 + timedelta(hours=63)
    )


# ---------------------------------------------------------------------------
# B-T30
# ---------------------------------------------------------------------------


def test_re_offered_hours_neither_duplicate_nor_churn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(i)/(ii): ``station_observations`` is ``UNIQUE(station_id, variable,
    valid_at)`` (migrations.py:78) and ``insert_station_observation``
    resolves a collision with ``ON CONFLICT ... DO UPDATE``
    (consensus.py:195-213) -- a re-offer is an update, never a second row,
    and the byte-identical guard (:195-201) makes an UNCHANGED re-offer a
    no-op (no ``pair_and_score`` job). (iii): the policy is last-write-wins
    -- a changed hour overwrites and DOES enqueue rescoring."""
    conn = _init_tmp_db(tmp_path)
    t0 = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, t0)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(
        db,
        writer,
        site_id,
        lambda r: _obs_response([(isoformat_utc(t0), 5.0)]),
        job_id=1,
    )

    now = t0 + timedelta(hours=10)
    _freeze(monkeypatch, now)
    readings = _hourly_readings(t0 + timedelta(hours=1), t0 + timedelta(hours=10))
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response(readings), job_id=2)

    def _rows() -> dict[str, tuple[float, str, object]]:
        return {
            str(r["valid_at"]): (r["value"], r["qc_flag"], r["source_raw"])
            for r in conn.execute(
                "SELECT valid_at, value, qc_flag, source_raw "
                "FROM station_observations "
                "WHERE station_id=? AND variable='temperature'",
                (station_id,),
            )
        }

    count_after_2 = conn.execute(
        "SELECT COUNT(*) FROM station_observations WHERE station_id=?", (station_id,)
    ).fetchone()[0]
    rows_after_2 = _rows()

    conn.execute("DELETE FROM jobs")
    conn.commit()
    _freeze(monkeypatch, now + timedelta(minutes=30))
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response(readings), job_id=3)

    count_after_3 = conn.execute(
        "SELECT COUNT(*) FROM station_observations WHERE station_id=?", (station_id,)
    ).fetchone()[0]
    assert count_after_3 == count_after_2
    assert _rows() == rows_after_2
    assert (
        conn.execute("SELECT 1 FROM jobs WHERE type='pair_and_score'").fetchone()
        is None
    )

    conn.execute("DELETE FROM jobs")
    conn.commit()
    changed_hour = isoformat_utc(t0 + timedelta(hours=5))
    changed_readings = [
        (stamp, 20.0) if stamp == changed_hour else (stamp, value)
        for stamp, value in readings
    ]
    _freeze(monkeypatch, now + timedelta(minutes=45))
    _run_fetch_obs(
        db, writer, site_id, lambda r: _obs_response(changed_readings), job_id=4
    )

    final_rows = _rows()
    assert final_rows[changed_hour][0] == 20.0
    for stamp, (value, _qc, _src) in rows_after_2.items():
        if stamp != changed_hour:
            assert final_rows[stamp][0] == value
    assert (
        conn.execute("SELECT 1 FROM jobs WHERE type='pair_and_score'").fetchone()
        is not None
    )


# ---------------------------------------------------------------------------
# B-T31
# ---------------------------------------------------------------------------


def test_outage_past_provider_retention_leaves_a_bounded_hole(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 10-day outage exceeds even the widened window's ceiling: the
    retention derived from the watermark clamps at
    ``_HISTORY_RETENTION_MAX_HOURS=168`` (processor.py:899), leaving a
    genuine, bounded hole between the old watermark and the provider's
    own 7-day horizon -- and the station's degraded state is visible on
    both monitor conditions along the way."""
    conn = _init_tmp_db(tmp_path)
    t0 = datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, t0)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(
        db,
        writer,
        site_id,
        lambda r: _obs_response([(isoformat_utc(t0), 5.0)]),
        job_id=1,
    )

    park_time = t0 + timedelta(hours=1)
    _freeze(monkeypatch, park_time)
    _run_fetch_obs(db, writer, site_id, lambda r: _no_content_response(), job_id=2)
    assert _station_state(conn, station_id)["history_error_count"] == 1

    verdict_park = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=False,
        db_enabled=False,
        now=park_time,
        export_sweeper_dead=None,
    )
    cond_failing = _condition(verdict_park, "obs_station_history_failing")
    assert cond_failing["ok"] is False
    assert cond_failing["count"] == 1

    verdict_stale = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=False,
        db_enabled=False,
        now=t0 + timedelta(hours=13),  # OBS_STALE_HOURS=12 (monitor.py:30)
        export_sweeper_dead=None,
    )
    obs_stale = _condition(verdict_stale, "obs_stale")
    assert obs_stale["ok"] is False
    assert obs_stale["count"] == 1

    now2 = t0 + timedelta(days=10)
    watermark_stamp = isoformat_utc(t0)
    assert (
        _retention_hours(watermark_stamp, now2) == 168
    )  # _HISTORY_RETENTION_MAX_HOURS

    _freeze(monkeypatch, now2)
    payload_start = now2 - timedelta(hours=167)
    readings = _hourly_readings(payload_start, now2)
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response(readings), job_id=3)

    state = _station_state(conn, station_id)
    assert state["history_error_count"] == 0
    assert state["history_next_attempt_at"] is None

    stored = {
        str(r["valid_at"])
        for r in conn.execute(
            "SELECT valid_at FROM station_observations WHERE station_id=? "
            "AND variable='temperature' AND valid_at >= ?",
            (station_id, isoformat_utc(payload_start)),
        )
    }
    hour = payload_start
    expected = set()
    while hour <= now2:
        expected.add(isoformat_utc(hour))
        hour += timedelta(hours=1)
    assert stored == expected

    hole_count = conn.execute(
        "SELECT COUNT(*) FROM station_observations WHERE station_id=? "
        "AND variable='temperature' AND valid_at > ? AND valid_at < ?",
        (station_id, watermark_stamp, isoformat_utc(payload_start)),
    ).fetchone()[0]
    assert hole_count == 0
    assert _site_state(conn, site_id)["last_obs_at"] == isoformat_utc(now2)

    # Immediately afterwards, the watermark has advanced past the hole and
    # the window narrows back to RECENT_REFRESH_HOURS=6: an old reading well
    # outside that window is dropped while a fresh one is kept.
    now3 = now2 + timedelta(hours=1)
    _freeze(monkeypatch, now3)
    assert _retention_hours(isoformat_utc(now2), now3) == 6  # RECENT_REFRESH_HOURS
    stale_but_in_old_window = isoformat_utc(
        now2 - timedelta(hours=200)
    )  # before payload_start
    fresh_reading = isoformat_utc(now3)
    _run_fetch_obs(
        db,
        writer,
        site_id,
        lambda r: _obs_response([(stale_but_in_old_window, 1.0), (fresh_reading, 2.0)]),
        job_id=4,
    )
    dropped = conn.execute(
        "SELECT 1 FROM station_observations WHERE station_id=? AND valid_at=?",
        (station_id, stale_but_in_old_window),
    ).fetchone()
    assert dropped is None
    kept = conn.execute(
        "SELECT 1 FROM station_observations WHERE station_id=? AND valid_at=?",
        (station_id, fresh_reading),
    ).fetchone()
    assert kept is not None


# ---------------------------------------------------------------------------
# B-T32
# ---------------------------------------------------------------------------


def test_recovered_observations_invalidate_and_recompute_daily_truth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reuses B-T26's recovery mechanics; the 24 recovered hours cover the
    UTC day ``2026-09-21`` in full, with the day's genuine extremum (a
    single 20.0 peak at hour 45, a single 0.0 trough at hour 42, against a
    flat 10.0 elsewhere) placed inside ``t0+40h..t0+57h`` -- the span a
    six-hour window would lose. A buggy fixed window would instead see only
    the flat 18:00-23:00Z tail and compute max=min=10.0, not the 20.0/0.0
    asserted below. ``mark_daily_truth_stale`` (consensus.py:281, ALWAYS
    called by ``materialize_consensus``) marks the day's two temperature
    quantities stale and leaves an unrelated day untouched;
    ``regenerate_marked_truth_chunk`` (truth.py:380-408, no lower-date
    bound on its ``stale=1`` selection) then rebuilds it."""
    conn = _init_tmp_db(tmp_path)
    t0 = datetime(2026, 9, 19, 8, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, t0)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    generation_id = ensure_published_generation(conn, site_id)
    conn.commit()
    day = "2026-09-21"
    day_unaffected = "2026-09-22"
    materialize_daily_truth(
        conn, site_id=site_id, local_date=day, tz_generation_id=generation_id
    )
    materialize_daily_truth(
        conn, site_id=site_id, local_date=day_unaffected, tz_generation_id=generation_id
    )
    conn.commit()
    pre = {
        str(r["quantity"]): int(r["stale"])
        for r in conn.execute(
            "SELECT quantity, stale FROM daily_truth WHERE site_id=? AND local_date=?",
            (site_id, day),
        )
    }
    assert pre["temperature_high"] == 0
    assert pre["temperature_low"] == 0

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(
        db,
        writer,
        site_id,
        lambda r: _obs_response([(isoformat_utc(t0), 5.0)]),
        job_id=1,
    )

    now = t0 + timedelta(hours=63)
    _freeze(monkeypatch, now)
    readings = []
    for h in range(24):
        stamp = isoformat_utc(t0 + timedelta(hours=40 + h))
        if h == 5:
            value = 20.0
        elif h == 2:
            value = 0.0
        else:
            value = 10.0
        readings.append((stamp, value))
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response(readings), job_id=2)

    peak_stamp = isoformat_utc(t0 + timedelta(hours=45))
    obs_row = conn.execute(
        "SELECT n_stations FROM observations "
        "WHERE site_id=? AND variable='temperature' AND valid_at=?",
        (site_id, peak_stamp),
    ).fetchone()
    assert obs_row is not None
    assert obs_row["n_stations"] == 1

    post = {
        str(r["quantity"]): int(r["stale"])
        for r in conn.execute(
            "SELECT quantity, stale FROM daily_truth WHERE site_id=? AND local_date=?",
            (site_id, day),
        )
    }
    assert post["temperature_high"] == 1
    assert post["temperature_low"] == 1
    post_unaffected = {
        str(r["quantity"]): int(r["stale"])
        for r in conn.execute(
            "SELECT quantity, stale FROM daily_truth WHERE site_id=? AND local_date=?",
            (site_id, day_unaffected),
        )
    }
    assert post_unaffected["temperature_high"] == 0
    assert post_unaffected["temperature_low"] == 0

    pair_jobs = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type='pair_and_score' AND site_id=?",
        (site_id,),
    ).fetchone()[0]
    assert pair_jobs == 1

    regenerated = regenerate_marked_truth_chunk(conn, site_id=site_id, limit=20)
    conn.commit()
    assert regenerated >= 1

    rebuilt = {
        str(r["quantity"]): (int(r["stale"]), r["value"])
        for r in conn.execute(
            "SELECT quantity, stale, value FROM daily_truth "
            "WHERE site_id=? AND local_date=?",
            (site_id, day),
        )
    }
    assert rebuilt["temperature_high"] == (0, 20.0)
    assert rebuilt["temperature_low"] == (0, 0.0)


# ---------------------------------------------------------------------------
# B-T33
# ---------------------------------------------------------------------------


def test_regen_chunk_has_no_lower_date_bound_and_discovery_drops_its_cursor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two mechanics named by the roster, driven through the real chain with
    ``truth_discovery_days=1`` (one day per chunk) over a two-day observed
    extent: ``verification_run.py`` sets ``truth_cursor`` to the day just
    discovered whenever a chunk is FULL (:258-262: ``len(attempted) <
    limit`` is False), advances it on the next full chunk, and only drops it
    once a chunk comes back empty -- the exact moment it also transitions
    ``discover -> regen``. ``regenerate_marked_truth_chunk`` then selects
    ANY stale (day, generation) group for the site with no lower-date bound
    (truth.py:395-402) -- so ``day1``, discovered and admitted on the very
    FIRST chunk (long before the cursor advanced past it, and still further
    behind by the time it is dropped), still gets regenerated once marked
    stale, regardless of where the (now-gone) cursor last pointed.

    Staleness is produced the way B-T32 produces it: a real ``fetch_obs``
    recovery cycle re-offers ``day1``'s only hour with a CHANGED value
    (B-T30's last-write-wins mechanic, consensus.py:195-213) so
    ``insert_station_observation`` -> ``materialize_consensus`` ->
    ``mark_daily_truth_stale`` (consensus.py:281) fires on its own -- nothing
    in ``truth.py`` is called directly. The post-regeneration assertion
    below checks the rebuilt VALUE, derived by reading
    ``materialize_daily_truth`` (truth.py:272) for a single-station,
    single-hour day: both ``temperature_high`` and ``temperature_low``
    equal that one corrected reading.

    Deviation from B-T32's literal database: this does not continue B-T32's
    own on-disk state. In B-T32's fixture the recovered/invalidated day
    (``2026-09-21``) is the site's LAST observed day -- the one nearest
    wherever the cursor ends up, never one it is left behind by. Reusing
    that exact database would contradict the property this test exists to
    pin (``day1`` discovered on the very FIRST chunk, invalidated only after
    the cursor has moved past it and been dropped), so a fresh database is
    built here with the same real-recovery-fetch TECHNIQUE B-T32 uses,
    rather than B-T32's own rows.
    """
    conn = _init_tmp_db(tmp_path)
    t0 = datetime(2026, 9, 19, 8, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, t0)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    day1 = "2026-09-19"
    day2 = "2026-09-20"
    day1_stamp = isoformat_utc(t0)
    day2_stamp = isoformat_utc(t0 + timedelta(hours=24))

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(
        db, writer, site_id, lambda r: _obs_response([(day1_stamp, 10.0)]), job_id=1
    )

    # 30 days on: both days are long past their own 24h admission deadline
    # (completeness.py's ``TRUTH_COMPLETENESS_DEADLINE_HOURS=24``), so each
    # gets a genuine daily_truth row on discovery despite having only one
    # observed hour -- deadline admission does not gate on coverage.
    chain_now = t0 + timedelta(days=30)
    monkeypatch.setattr("wxverify.worker.verification_run.utc_now", lambda: chain_now)

    ok = advance_verification(conn, site_id, {"truth_discovery_days": 1})
    conn.commit()
    assert ok is True
    state = _load_state(conn, site_id)
    assert state is not None
    assert state["phase"] == "discover"
    assert state["truth_cursor"] == day1  # chunk was full (1 day found == limit 1)
    day1_row = conn.execute(
        "SELECT admission_basis FROM daily_truth WHERE site_id=? AND local_date=? "
        "AND quantity='temperature_high'",
        (site_id, day1),
    ).fetchone()
    assert day1_row is not None
    assert day1_row["admission_basis"] == "deadline"

    # The real recovery fetch: day1's only hour, re-offered with a changed
    # value. `insert_station_observation`'s byte-identical guard (:195-201)
    # is defeated by the change, so this reaches `materialize_consensus`,
    # which ALWAYS calls `mark_daily_truth_stale` (consensus.py:281) --
    # exactly the path B.10's closing paragraph names as the sole
    # `observations` writer. The station's watermark is still `day1_stamp`
    # itself (nothing newer has been ingested yet), so no retention-window
    # widening is needed to reach it.
    _freeze(monkeypatch, t0 + timedelta(hours=1))
    _run_fetch_obs(
        db, writer, site_id, lambda r: _obs_response([(day1_stamp, 15.0)]), job_id=2
    )

    # day2's hour is seeded AFTER day1's correction, and only now: once it
    # exists it becomes the station's watermark, and `_retention_hours`
    # (processor.py:917-931) can then never widen a later fetch's window
    # back past it to reach day1 again -- so day1's correction had to happen
    # first, while day1 was still the newest (only) thing the station had
    # reported.
    _freeze(monkeypatch, t0 + timedelta(hours=25))
    _run_fetch_obs(
        db, writer, site_id, lambda r: _obs_response([(day2_stamp, 10.0)]), job_id=3
    )

    ok = advance_verification(conn, site_id, {"truth_discovery_days": 1})
    conn.commit()
    assert ok is True
    state = _load_state(conn, site_id)
    assert state is not None
    assert state["phase"] == "discover"
    assert state["truth_cursor"] == day2  # chunk was full again (day2 == limit 1)

    ok = advance_verification(conn, site_id, {"truth_discovery_days": 1})
    conn.commit()
    assert ok is True
    state = _load_state(conn, site_id)
    assert state is not None
    assert state["phase"] == "regen"
    assert "truth_cursor" not in state  # empty chunk: cursor dropped on discover->regen

    before_regen = {
        str(r["quantity"]): int(r["stale"])
        for r in conn.execute(
            "SELECT quantity, stale FROM daily_truth WHERE site_id=? AND local_date=?",
            (site_id, day1),
        )
    }
    assert before_regen["temperature_high"] == 1  # invalidated by the real fetch above

    ok = advance_verification(conn, site_id, {"regen_chunk_groups": 20})
    conn.commit()
    assert ok is True
    after_regen = {
        str(r["quantity"]): (int(r["stale"]), r["value"])
        for r in conn.execute(
            "SELECT quantity, stale, value FROM daily_truth WHERE site_id=? "
            "AND local_date=?",
            (site_id, day1),
        )
    }
    # day1 holds exactly one recovered hour (15.0), so its high and low both
    # equal that value -- regenerated with no cursor anywhere near it.
    assert after_regen["temperature_high"] == (0, 15.0)
    assert after_regen["temperature_low"] == (0, 15.0)


# ---------------------------------------------------------------------------
# B-T34
# ---------------------------------------------------------------------------


def test_deferred_day_is_admitted_once_coverage_completes_and_settles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Drives day D through the real admission chain, never seeded: (1) a
    discover pass with only temperature hours 0-15 present DEFERS D --
    ``decide_admission`` (completeness.py:134-160) returns
    ``reason="incomplete_coverage"`` (``_evaluate_day`` always yields all
    FIVE quantity outcomes, so an entirely-absent wind/precip still leaves
    ``covered_hours`` non-empty -- the branch taken is "not complete", not
    the vacuous "no_quantities_evaluated" one), asserted against both the
    INFO log ``materialize_missing_truth_days`` emits and the absence of any
    ``daily_truth`` row; (2) a genuine recovery fetch cycle (B-T26's
    mechanics) backfills hours 16-23 into ``observations`` (the station's
    watermark is temperature-only at this point -- ``_enabled_stations``
    derives it from ``MAX(station_observations.valid_at)`` with no variable
    filter, so seeding wind/precip only AFTER this cycle keeps the recovery
    window exactly what B-T26 predicts); (3) wind and precip are then filled
    for all 24 hours, directly, the same way; (4) ``_clear_state`` resets
    the chain -- move 1 already advanced ``truth_cursor`` to D itself (a
    deferred day still counts toward a full chunk), so without clearing it
    discovery would skip past D forever -- and a fresh discover pass now
    finds D complete and ADMITS it with ``admission_basis == "complete"``,
    proven distinct from a deadline admission because ``chain_now`` sits
    hours before ``day_end_utc + TRUTH_COMPLETENESS_DEADLINE_HOURS=24``.

    ``observations.computed_at`` is written by ``isoformat_utc()`` with NO
    argument (consensus.py:307), which resolves through
    ``wxverify.core.timeutil``'s OWN ``utc_now`` -- the one reference
    ``_freeze`` never patches (see its docstring), so every ``computed_at``
    stamp below is REAL wall-clock time, years behind the chain's pinned
    synthetic-future ``now``. This makes ``decide_admission``'s C2
    (quiescence) leg trivially satisfied the moment C1 (completeness) is --
    which is exactly why this test does not attempt to isolate C2 as a
    separate move: with an unpatchable real-wall-clock ``computed_at``, no
    fixture can hold C1 true while C2 is pending, so the admission this test
    proves is driven by completeness, with quiescence along for the ride by
    construction rather than independently demonstrated. The chain's own
    ``now`` is pinned in a synthetic future (2030) so that year-scale margin
    holds regardless of the real date the suite happens to run on."""
    conn = _init_tmp_db(tmp_path)
    day = "2030-01-08"
    day_start = datetime(2030, 1, 8, 0, 0, 0, tzinfo=UTC)
    site_id, (station_id,) = (lambda pair: (pair[0], tuple(pair[1].values())))(
        _seed_site_and_stations(conn, ["ISTATION01"])
    )
    source_raw = '{"synthetic": true}'

    # Only the pre-outage half of temperature, written directly through the
    # real per-hour consensus path -- this is construction, not seeding:
    # every row goes through `insert_station_observation` ->
    # `materialize_consensus`, the same function the fetch cycle itself
    # calls. Wind and precip stay unwritten so the station's watermark
    # (MAX(station_observations.valid_at), all variables) is temperature-only
    # until after the move-2 recovery cycle below.
    for h in range(16):
        stamp = isoformat_utc(day_start + timedelta(hours=h))
        insert_station_observation(
            conn,
            station_id=station_id,
            variable="temperature",
            valid_at=stamp,
            value=10.0,
            source_raw=source_raw,
        )
    conn.commit()

    chain_now = datetime(2030, 1, 9, 12, 0, 0, tzinfo=UTC)  # well before day_end+24h
    monkeypatch.setattr("wxverify.worker.verification_run.utc_now", lambda: chain_now)

    caplog_records: list[str] = []
    logger = logging.getLogger("wxverify.verification.truth")
    handler = logging.Handler()
    handler.setLevel(logging.INFO)
    handler.emit = lambda record: caplog_records.append(record.getMessage())  # type: ignore[method-assign]
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        ok = advance_verification(conn, site_id, {"truth_discovery_days": 1})
    finally:
        logger.removeHandler(handler)
    conn.commit()
    assert ok is True

    # Move 1: D is deferred for incomplete coverage.
    assert any(
        f"local_date={day}" in msg and "reason=incomplete_coverage" in msg
        for msg in caplog_records
    ), caplog_records
    assert (
        conn.execute(
            "SELECT 1 FROM daily_truth WHERE site_id=? AND local_date=?", (site_id, day)
        ).fetchone()
        is None
    )
    state = _load_state(conn, site_id)
    assert state is not None
    assert state["truth_cursor"] == day

    # Move 2: a genuine recovery fetch cycle backfills hours 16-23, reading
    # the temperature-only watermark (hour 15) established above.
    _freeze(monkeypatch, chain_now)
    db = get_db()
    writer = FencedWriter(db, db.generation)
    readings = [
        (isoformat_utc(day_start + timedelta(hours=h)), 10.0) for h in range(16, 24)
    ]
    _run_fetch_obs(db, writer, site_id, lambda r: _obs_response(readings), job_id=1)

    recovered_stamp = isoformat_utc(day_start + timedelta(hours=20))
    obs_row = conn.execute(
        "SELECT 1 FROM observations WHERE site_id=? AND variable='temperature' "
        "AND valid_at=?",
        (site_id, recovered_stamp),
    ).fetchone()
    assert obs_row is not None

    # Move 3: wind and precip, filled for the full day, directly -- this
    # runs AFTER the recovery cycle so it never perturbs move 2's watermark.
    for h in range(24):
        stamp = isoformat_utc(day_start + timedelta(hours=h))
        insert_station_observation(
            conn,
            station_id=station_id,
            variable="wind",
            valid_at=stamp,
            value=5.0,
            source_raw=source_raw,
        )
        insert_station_observation(
            conn,
            station_id=station_id,
            variable="precip",
            valid_at=stamp,
            value=0.0,
            source_raw=source_raw,
        )
    conn.commit()

    # Move 4: `truth_cursor` was set to D itself after move 1 (a deferred day
    # still counted toward a full chunk), so discovery would otherwise skip
    # straight past it forever; `_clear_state` resets the chain the same way
    # an operator-triggered rerun would, and a fresh discover pass now finds
    # D complete and admits it.
    _clear_state(conn, site_id)
    conn.commit()
    ok = advance_verification(conn, site_id, {"truth_discovery_days": 1})
    conn.commit()
    assert ok is True

    rows = {
        str(r["quantity"]): (
            r["eligible"],
            r["admission_basis"],
            r["value"],
            r["stale"],
        )
        for r in conn.execute(
            "SELECT quantity, eligible, admission_basis, value, stale "
            "FROM daily_truth WHERE site_id=? AND local_date=?",
            (site_id, day),
        )
    }
    assert set(rows) == {
        "temperature_high",
        "temperature_low",
        "wind_max",
        "precip_total",
        "precip_occurrence",
    }
    for quantity, (eligible, basis, _value, stale) in rows.items():
        assert bool(eligible) is True, quantity
        assert basis == "complete", quantity
        assert int(stale) == 0, quantity
    assert rows["temperature_high"][2] == 10.0
    assert rows["temperature_low"][2] == 10.0
    assert rows["wind_max"][2] == 5.0
    assert rows["precip_total"][2] == 0.0
    assert rows["precip_occurrence"][2] == 0.0
