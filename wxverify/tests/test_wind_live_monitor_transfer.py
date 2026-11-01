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

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from wxverify.api.errors import ApiError
from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.monitor import (
    _wind_history_trips,  # noqa: PLC2701 -- private, exercised directly
    build_verdict,
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
    """A site/station pair that trips none of (c), (e) to (h) at ``_NOW``.

    A fresh, well-attended ``station_wind_days`` row for today (non-zero
    ``record_count``, ``last_ok_at`` inside the 2x-interval window) clears
    (e) and (h); no failed rows clears (c); no auth hold clears (g); a
    plain 'UTC' timezone clears (f). (i) is gated on a ``history_all``
    auth hold being held at all -- this baseline has none, so (i) is never
    reached here, not demonstrated clear. Each test below mutates exactly
    one of these away from healthy.
    """
    site_id = _seed_site(conn)
    station_id = _seed_enabled_station(conn, site_id, "WSTATION01")
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, record_count, last_ok_at, attempts,"
        "  updated_at)"
        " VALUES (?, '2026-09-22', 'fetched', 24, ?, 1, ?)",
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
        "  updated_at)"
        " VALUES (?, '2026-09-20', 'failed', 0, NULL, 3, ?)",
        (station_id, isoformat_utc(_NOW)),
    )
    conn.commit()
    tripped, first = _trips(conn)
    assert tripped == 1
    assert first == (
        "wind history: 1 station-days failed after 3 attempts in the last 7 days"
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
    conn.commit()  # no station_wind_days row for today at all
    tripped, first = _trips(conn)
    assert tripped == 1
    assert first == "no wind readings today from any station"


def test_t74h_stale_or_missing_today_row_trips(tmp_path: Path) -> None:
    """(h): once past grace, a station with no today row, or one whose
    ``last_ok_at`` is NULL or older than 2x the obs interval, trips -- one
    reason per such station.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id = _seed_site(conn)
    missing_row_station = _seed_enabled_station(conn, site_id, "WSTATION01")
    null_last_ok_station = _seed_enabled_station(conn, site_id, "WSTATION02")
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, record_count, last_ok_at, attempts,"
        "  updated_at)"
        " VALUES (?, '2026-09-22', 'fetched', 24, NULL, 1, ?)",
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
        "INSERT INTO station_wind_records (station_id, obs_at, source,"
        " speed_kmh) VALUES (?, ?, 'five_minute', 7.2)",
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
