"""Tests: group 6 of plan section 15.1 (2026-09-30) -- T70-T79.

Live fetch_obs' wind-write gate (T70-T72), the wind-days lane's isolation
from the live-stream liveness checks and the `_wind_history_trips` condition
table (T73-T78), and the db-transfer upload validator's wind-column stamp
guard (T79).

File placement is a deliberate deviation from the plan's suggested homes
(`tests/test_monitor.py`, `tests/test_db_transfer.py`): both of those files
show as locally modified (`git status`) by the concurrent python-dev pass
implementing plan section 15.2, so editing them here risks colliding with
in-flight changes neither visible nor owned by this dispatch. This file is
new and self-contained instead, replicating only the small harness pieces
it needs (never importing fixtures across files, since none of these three
files share a conftest-level fixture for this).

Every station id, site name and coordinate below is synthetic (public
repo): pws station ids ``WSTATION01``/``WSTATION02``, site name "Testsite",
timezone "UTC", coordinates 0.0/0.0, key "0123456789abcdef0123456789abcdef".

Per the plan's closing paragraph on section 15.1, every expected value below
is derived by reading the actual production function bodies, not copied
from the plan document -- each test's docstring or inline comment names the
function read and what it produces.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from wxverify import config
from wxverify.api.errors import ApiError
from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import Job
from wxverify.db.runtime_state import delete_runtime_state, set_runtime_state
from wxverify.db.wind_basis import (
    set_wind_basis_state,
    wind_blocked_key,
    wind_progress_key,
    write_auth_hold,
)
from wxverify.monitor import (
    _wind_history_trips,  # noqa: PLC2701 -- private, exercised directly
    build_verdict,
)
from wxverify.worker.catchup import GAP_VARIABLES, _station_has_gap  # noqa: PLC2701
from wxverify.worker.processor import dispatch

_API_KEY = "0123456789abcdef0123456789abcdef"
_TZ = "UTC"


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
    """Freeze ``utc_now()`` in the two modules that call it on the fetch path.

    See ``tests/test_fetch_obs_partial_cycle.py``'s ``_freeze`` for why each
    module needs its own patch (each imports ``utc_now`` by name).
    """
    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: when)


def _seed_site_and_station(
    conn: sqlite3.Connection, pws_id: str = "WSTATION01"
) -> tuple[int, int]:
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, ?)",
            (_TZ,),
        ).lastrowid
    )
    station_id = int(
        conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m) VALUES (?, ?, 0.0, 0.0, 0.0)",
            (site_id, pws_id),
        ).lastrowid
    )
    return site_id, station_id


def _wind_capable_response(stamp: str) -> httpx.Response:
    """A 200 whose body decodes to one temperature AND one wind reading.

    ``observations_from_payload`` (``wxverify/obs/pws_adapter.py``) reads
    ``metric.tempAvg`` -> ``"temperature"`` and ``metric.windSpeedAvg`` ->
    ``"wind"`` (via ``kmh_to_ms``); both fields on one ``obsTimeUtc`` row
    produce two ``PwsObservation`` rows for the same station/hour.
    """
    body = {
        "observations": [
            {
                "obsTimeUtc": stamp,
                "metric": {"tempAvg": 10.0, "windSpeedAvg": 7.2},
            }
        ]
    }
    return httpx.Response(
        200,
        content=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
    )


def _run_fetch_obs(
    db: object, writer: FencedWriter, site_id: int, handler: object
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


def _obs_rows(
    conn: sqlite3.Connection, station_id: int, variable: str
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM station_observations WHERE station_id=? AND variable=?",
        (station_id, variable),
    ).fetchall()


# ---------------------------------------------------------------------------
# T70/T72 -- live fetch_obs wind gate
# ---------------------------------------------------------------------------


def test_t70_live_fetch_writes_wind_only_while_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``insert_station_observation`` (consensus.py:166-211) gates wind rows on
    the site's wind-basis state ('staging' only); temperature is unguarded.

    Two site/station pairs, same response shape, differing only in the
    wind-basis state set before the fetch runs.
    """
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)

    staging_site, staging_station = _seed_site_and_station(conn, "WSTATION01")
    pm_site, pm_station = _seed_site_and_station(conn, "WSTATION02")
    set_wind_basis_state(conn, staging_site, "staging")
    set_wind_basis_state(conn, pm_site, "pair_max")

    db = get_db()
    writer = FencedWriter(db, db.generation)

    def handler(request: httpx.Request) -> httpx.Response:
        return _wind_capable_response("2026-09-22T11:00:00Z")

    _run_fetch_obs(db, writer, staging_site, handler)
    _run_fetch_obs(db, writer, pm_site, handler)

    # staging: both variables land.
    assert len(_obs_rows(conn, staging_station, "temperature")) == 1
    assert len(_obs_rows(conn, staging_station, "wind")) == 1
    # pair_max: temperature still lands, wind is gated out.
    assert len(_obs_rows(conn, pm_station, "temperature")) == 1
    assert len(_obs_rows(conn, pm_station, "wind")) == 0


def test_t72_live_fetch_never_writes_wind_history_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The live fetch_obs path only ever calls ``insert_station_observation``
    (``station_observations``); ``station_wind_records``/``station_wind_days``
    are written exclusively by the wind-days lane (``wind_days.py``), never
    reachable from ``dispatch(... job_key="obs" ...)``.
    """
    conn = _init_tmp_db(tmp_path)
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    site_id, station_id = _seed_site_and_station(conn)
    set_wind_basis_state(conn, site_id, "staging")

    db = get_db()
    writer = FencedWriter(db, db.generation)

    def handler(request: httpx.Request) -> httpx.Response:
        return _wind_capable_response("2026-09-22T11:00:00Z")

    _run_fetch_obs(db, writer, site_id, handler)

    assert len(_obs_rows(conn, station_id, "wind")) == 1  # the gate did fire
    assert conn.execute("SELECT COUNT(*) FROM station_wind_records").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM station_wind_days").fetchone()[0] == 0


def test_t71_station_has_gap_ignores_wind(tmp_path: Path) -> None:
    """``_station_has_gap`` (``catchup.py:446-471``) scores only
    ``GAP_VARIABLES = ("precip", "temperature")`` -- wind rows present or
    absent must not move its verdict either way.
    """
    assert "wind" not in GAP_VARIABLES
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id, station_id = _seed_site_and_station(conn)
    window_start, window_end = "2026-09-22T10:00:00Z", "2026-09-22T12:00:00Z"  # 2 hrs

    def _insert(variable: str, hour: str) -> None:
        conn.execute(
            "INSERT INTO station_observations"
            " (station_id, variable, valid_at, value, qc_flag, source_raw)"
            " VALUES (?, ?, ?, 1.0, 'ok', '{}')",
            (station_id, variable, hour),
        )

    # Full temp+precip coverage for both hours, wind present for neither:
    # no gap, because wind isn't in GAP_VARIABLES.
    for hour in ("2026-09-22T10:00:00Z", "2026-09-22T11:00:00Z"):
        _insert("temperature", hour)
        _insert("precip", hour)
    conn.commit()
    assert (
        _station_has_gap(
            conn, station_id, window_start=window_start, window_end=window_end
        )
        is False
    )

    # Add a wind-only hour on top: still no gap -- the check must not count it
    # as satisfying anything, but also must not count it against anything.
    _insert("wind", "2026-09-22T10:00:00Z")
    conn.commit()
    assert (
        _station_has_gap(
            conn, station_id, window_start=window_start, window_end=window_end
        )
        is False
    )

    # Now drop one hour's temperature row: a real gap in a counted variable.
    conn.execute(
        "DELETE FROM station_observations WHERE station_id=? AND variable='temperature'"
        " AND valid_at=?",
        (station_id, "2026-09-22T11:00:00Z"),
    )
    conn.commit()
    assert (
        _station_has_gap(
            conn, station_id, window_start=window_start, window_end=window_end
        )
        is True
    )


# ---------------------------------------------------------------------------
# T73 -- fetch_obs_live liveness reads job_key="obs" only
# ---------------------------------------------------------------------------


def _seed_site(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone, enabled) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC', 1)"
        ).lastrowid
    )


def _seed_enabled_station(conn: sqlite3.Connection, site_id: int, pws_id: str) -> int:
    return int(
        conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m, enabled) VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
            (site_id, pws_id),
        ).lastrowid
    )


def _seed_completed_job(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    job_type: str,
    job_key: str,
    updated_at: str,
) -> None:
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status, updated_at)"
        " VALUES (?, ?, ?, 'completed', ?)",
        (job_type, site_id, job_key, updated_at),
    )


def _cond(body: dict[str, object], cond_id: str) -> dict[str, object]:
    conditions = body["conditions"]
    assert isinstance(conditions, list)
    return next(c for c in conditions if c["id"] == cond_id)


def test_t73_wind_days_completion_does_not_satisfy_live_obs_liveness(
    tmp_path: Path,
) -> None:
    """``_pipeline_conditions`` (``monitor.py:651-655``) reads
    ``_has_completed_within(..., "fetch_obs", ..., job_key="obs")`` for
    ``fetch_obs_live`` -- a completed ``job_key="wind-days"`` row in the same
    window must NOT satisfy it (the two lanes share one ``type``).

    Kills: dropping the ``job_key="obs"`` argument (reading ``job_key=None``)
    would let the wind-days-only fixture below satisfy liveness too --
    at this oracle's exact assertion, correct = True (tripped),
    mutant = False (not tripped).
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id = _seed_site(conn)
    _seed_enabled_station(conn, site_id, "WSTATION01")
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    recent = isoformat_utc(now - timedelta(hours=1))
    _seed_completed_job(
        conn,
        site_id=site_id,
        job_type="fetch_obs",
        job_key="wind-days",
        updated_at=recent,
    )
    conn.commit()

    body = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=False,
        db_enabled=False,
        now=now,
        export_sweeper_dead=None,
    )
    assert _cond(body, "fetch_obs_live")["ok"] is False

    # Paired positive: the identical fixture, but the completed row carries
    # job_key="obs" instead -- now it does satisfy liveness.
    conn.execute("DELETE FROM jobs")
    _seed_completed_job(
        conn, site_id=site_id, job_type="fetch_obs", job_key="obs", updated_at=recent
    )
    conn.commit()
    body2 = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=False,
        db_enabled=False,
        now=now,
        export_sweeper_dead=None,
    )
    assert _cond(body2, "fetch_obs_live")["ok"] is True


# ---------------------------------------------------------------------------
# T74-T77 -- _wind_history_trips conditions (a)-(h)
# ---------------------------------------------------------------------------


_NOW = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)  # past local midnight + 6h grace


def _healthy_site(conn: sqlite3.Connection) -> tuple[int, int]:
    """A site/station pair that trips none of (a)-(h) at ``_NOW``.

    ``pair_max`` skips (b); a fresh, well-attended ``station_wind_days`` row
    for today (non-zero ``record_count``, ``last_ok_at`` inside the
    2x-interval window) clears (e) and (h); no failed/unavailable rows
    clears (c)/(d); no auth hold clears (g); a plain 'UTC' timezone clears
    (f). Each test below mutates exactly one of these away from healthy.
    """
    site_id = _seed_site(conn)
    station_id = _seed_enabled_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "pair_max")
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, record_count, last_ok_at, attempts,"
        "  legacy_hours, updated_at)"
        " VALUES (?, '2026-09-22', 'fetched', 24, ?, 1, 0, ?)",
        (
            station_id,
            isoformat_utc(_NOW - timedelta(minutes=30)),
            isoformat_utc(_NOW),
        ),
    )
    conn.commit()
    return site_id, station_id


def _trips(conn: sqlite3.Connection) -> tuple[int, str | None]:
    return _wind_history_trips(conn, _NOW)


def test_t74_healthy_site_trips_nothing(tmp_path: Path) -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    _healthy_site(conn)
    assert _trips(conn) == (0, None)


def test_t74a_blocked_switch_trips(tmp_path: Path) -> None:
    """(a): a runtime-state blocked marker trips, naming the stored reason."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id, _ = _healthy_site(conn)
    set_runtime_state(
        conn, wind_blocked_key(site_id), json.dumps({"reason": "no eligible stations"})
    )
    conn.commit()
    tripped, first = _trips(conn)
    assert tripped == 1
    assert first == "wind rebuild blocked: no eligible stations"


def test_t74b_stalled_rebuild_trips_with_headroom_detail(tmp_path: Path) -> None:
    """(b): a non-``pair_max`` site with no (or stale) progress stamp trips;
    the reason names the state and, when call headroom is exhausted, says so.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id, _ = _healthy_site(conn)
    set_wind_basis_state(conn, site_id, "staging")
    # set_wind_basis_state itself stamps progress via the real wall clock
    # (stamp_wind_progress), which is not frozen in this sync harness and
    # lands AFTER _NOW -- erase it so "no progress" is genuinely true at
    # _NOW, the state this check actually reads.
    delete_runtime_state(conn, wind_progress_key(site_id))
    conn.commit()
    tripped, first = _trips(conn)
    assert tripped == 1
    assert first is not None
    assert "wind rebuild has made no progress since never (state staging)" in first


def test_t74c_failed_station_days_trip(tmp_path: Path) -> None:
    """(c): a station-day that failed after >=3 attempts inside the last 7
    local days trips, independent of today's own row.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id, station_id = _healthy_site(conn)
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, record_count, last_ok_at, attempts,"
        "  legacy_hours, updated_at)"
        " VALUES (?, '2026-09-20', 'failed', 0, NULL, 3, 0, ?)",
        (station_id, isoformat_utc(_NOW)),
    )
    conn.commit()
    tripped, first = _trips(conn)
    assert tripped == 1
    assert first == (
        "wind history: 1 station-days failed after 3 attempts in the last 7 days"
    )


def test_t74d_unavailable_share_over_threshold_trips(tmp_path: Path) -> None:
    """(d): unavailable legacy-wind station-days over 10% of the lookback
    window trips -- 2 of 9 (> 10%) but 1 of 10 (= 10%, not > 10%) does not.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id, station_id = _healthy_site(conn)
    for day, status in [
        ("2026-09-01", "unavailable"),
        ("2026-09-02", "unavailable"),
        *[(f"2026-09-{d:02d}", "fetched") for d in range(3, 10)],  # 7 fetched
    ]:
        conn.execute(
            "INSERT INTO station_wind_days"
            " (station_id, local_date, status, record_count, last_ok_at, attempts,"
            "  legacy_hours, updated_at)"
            " VALUES (?, ?, ?, 1, NULL, 1, 1, ?)",
            (station_id, day, status, isoformat_utc(_NOW)),
        )
    conn.commit()
    tripped, first = _trips(conn)
    assert tripped == 1
    assert first == (
        "wind history: 2 of 9 station-days with old wind are unavailable from"
        " weather.com (last 30 days)"
    )


def test_t74f_unresolvable_timezone_cannot_evaluate(tmp_path: Path) -> None:
    """(f): a timezone the stdlib cannot resolve surfaces ``"cannot evaluate"``
    instead of silently passing every later check.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id, _ = _healthy_site(conn)
    conn.execute("UPDATE sites SET timezone='Not/AZone' WHERE id=?", (site_id,))
    conn.commit()
    tripped, first = _trips(conn)
    assert tripped == 1
    assert first == "cannot evaluate"


def test_t74e_no_wind_readings_today_trips(tmp_path: Path) -> None:
    """(e): an enabled station with no today row (or a zero count) trips,
    once past the post-midnight grace window.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id = _seed_site(conn)
    _seed_enabled_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "pair_max")
    conn.commit()  # no station_wind_days row for today at all
    tripped, first = _trips(conn)
    assert tripped == 1
    assert first == "no wind readings today from any station"


def test_t74g_auth_hold_trips(tmp_path: Path) -> None:
    """(g): a held weather.com auth hold trips, named by endpoint."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    _healthy_site(conn)
    write_auth_hold(
        conn, "history_all", status="held", since=isoformat_utc(_NOW), error="401"
    )
    conn.commit()
    tripped, first = _trips(conn)
    assert tripped == 1
    assert first == "wind history paused: weather.com refused the key (history_all)"


def test_t74h_stale_or_missing_today_row_trips(tmp_path: Path) -> None:
    """(h): once past grace, a station with no today row, or one whose
    ``last_ok_at`` is NULL or older than 2x the obs interval, trips -- one
    reason per such station.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id = _seed_site(conn)
    set_wind_basis_state(conn, site_id, "pair_max")
    missing_row_station = _seed_enabled_station(conn, site_id, "WSTATION01")
    null_last_ok_station = _seed_enabled_station(conn, site_id, "WSTATION02")
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, record_count, last_ok_at, attempts,"
        "  legacy_hours, updated_at)"
        " VALUES (?, '2026-09-22', 'fetched', 24, NULL, 1, 0, ?)",
        (null_last_ok_station, isoformat_utc(_NOW)),
    )
    conn.commit()
    tripped, first = _trips(conn)
    assert tripped == 1
    # _wind_history_trips only surfaces the SITE's first reason, not every
    # per-station reason -- missing_row_station sorts first by id, so its
    # "since midnight" text (raw is None -> since="midnight", same text the
    # NULL-last_ok_at station would also produce) is what's checked here.
    assert first == "wind for WSTATION01 has not updated since midnight"
    assert missing_row_station != null_last_ok_station


def test_t74h_switching_state_skips_today_checks(tmp_path: Path) -> None:
    """(h)/(e) grace carve-out: ``switching``/``rescoring`` never run the
    today checks at all, paired against the healthy ``pair_max`` baseline
    above where the same "no today row" fixture DOES trip.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id = _seed_site(conn)
    _seed_enabled_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "switching")
    delete_runtime_state(conn, wind_progress_key(site_id))
    conn.commit()
    tripped, first = _trips(conn)
    # (b) still fires (state != pair_max, no progress stamp) but (e)/(h) do not
    # add their own reasons on top -- confirmed by there being exactly the one
    # (b) reason, not more.
    assert tripped == 1
    assert first is not None
    assert first.startswith("wind rebuild has made no progress")


# ---------------------------------------------------------------------------
# T78 -- wind_history in the pipeline-disabled skip list
# ---------------------------------------------------------------------------


def test_t78_wind_history_skipped_when_pipeline_disabled(tmp_path: Path) -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    body = build_verdict(
        conn,
        pipeline_enabled=False,
        budget_enabled=False,
        db_enabled=False,
        now=_NOW,
        export_sweeper_dead=None,
    )
    cond = _cond(body, "wind_history")
    assert cond["ok"] is True
    assert cond["skipped"] is True


# ---------------------------------------------------------------------------
# T79 -- db-transfer upload validator's wind-column stamp guard
# ---------------------------------------------------------------------------


def _make_valid_upload_db(path: Path, *, obs_at: str) -> None:
    """A minimal but import-admissible DB: migrated schema, one
    ``station_wind_records`` row carrying ``obs_at``.
    """
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id = _seed_site(conn)
    station_id = _seed_enabled_station(conn, site_id, "WSTATION01")
    conn.execute(
        "INSERT INTO station_wind_records (station_id, obs_at, speed_kmh)"
        " VALUES (?, ?, 7.2)",
        (station_id, obs_at),
    )
    conn.commit()
    conn.close()


def test_t79_canonical_wind_obs_at_passes_validation(tmp_path: Path) -> None:
    db_path = tmp_path / "upload.db"
    _make_valid_upload_db(db_path, obs_at="2026-09-22T11:00:00Z")
    _validate_upload(db_path)  # must not raise


def test_t79_noncanonical_wind_obs_at_rejected(tmp_path: Path) -> None:
    """``_CANONICAL_STAMP_COLUMNS`` already includes
    (``station_wind_records``, ``obs_at``) (``db_transfer.py``) -- a stamp
    missing the trailing ``Z`` must be refused at import, paired against the
    identical fixture above that passes on a canonical stamp.

    Kills: dropping this pair from ``_CANONICAL_STAMP_COLUMNS`` -- at this
    oracle's assertion, correct = raises ``ApiError``, mutant = returns None
    (no exception). The fixture forces the divergence because ``obs_at``
    here is exactly non-canonical (no trailing "Z") and nothing else in the
    row trips any of the validator's other guards.
    """
    db_path = tmp_path / "upload.db"
    _make_valid_upload_db(db_path, obs_at="2026-09-22 11:00:00")
    with pytest.raises(ApiError) as excinfo:
        _validate_upload(db_path)
    assert "station_wind_records.obs_at" in str(excinfo.value)
