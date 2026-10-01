"""Tests: the wind-history lane, group 1 of plan section 15.1 (2026-09-30).

Covers T8, T30-T34, T36-T42, T44, T45, T47, T48, T104-T107 against
``wxverify/worker/wind_days.py`` (selector, persist, staging purge, switch
check, switching/rescoring, scheduler integration) using a real sqlite3
``:memory:`` connection + ``run_migrations`` for the sync surface, and a
tmp-path ``Database``/``FencedWriter`` for the handful of cases that go
through an async write path (T45, T47 via dispatch/scheduler).

Also covers T35 (the ``_fetch_failed`` >=500/payload-fault/refundable-
transport-error dispatch -- in ``tests/test_wind_auth_hold.py``, alongside
its existing 401/403/429 coverage), T43 (``_run_rescoring``'s
``ScoringInputsBusy`` defer, via injected failing fakes for
``run_split_pair_phases``/``run_batched_scoring``), T46's chunk-level
headroom-exhaustion integration (``_run_fetch_chunk`` raising
``WindHeadroomExhausted`` through a live chunk loop -- in
``tests/test_wind_quota.py``, reusing its headroom-0 fixture), and T97 (a
station-day stuck on repeated 500s going final after 3 attempts across 4
real jobs, driven through ``run_wind_days`` with a tmp-path DB +
``httpx.MockTransport``, mirroring ``tests/test_wind_auth_hold.py``'s async
harness), T98 (``switching``/``rescoring`` dispatch never reaching the fetch
chunk even with a due row, an active domain backoff and today's spend at
cap), T99 (the same two states never probing a ``probing`` auth hold, paired
against a positive showing the first ``pair_max`` job does probe it), and
T100 (a prior job's bad exit -- a headroom-0 ``JobDeferred``, or the switch
check itself raising -- never gates the next job's fresh switch check). T98-
T100 reuse T97's async dispatch + ``httpx.MockTransport`` harness
(``_t97_init_tmp_db``/``_t97_freeze``/``_t97_run_wind_days``).

Every station id, site name and coordinate below is synthetic (public repo):
pws station ids ``WSTATION01``/``WSTATION02``, site name "Testsite",
timezone "UTC" or "Etc/UTC", coordinates 0.0/0.0.
"""

from __future__ import annotations

import ast
import asyncio
import json
import logging
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.migrations import run_migrations, seed_default_sources
from wxverify.db.queue import Job, claim_next_job
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.wind_basis import (
    read_auth_hold,
    read_wind_cursor,
    set_wind_basis_state,
    wind_basis_key,
    wind_basis_state,
    wind_blocked_key,
    wind_cursor_key,
    wind_done_at_key,
    wind_progress_key,
    write_auth_hold,
    write_wind_cursor,
)
from wxverify.forecast.wind_blend import MIN_OBS_HOURS
from wxverify.obs.wind_pairs import WindRecord
from wxverify.worker.processor import dispatch
from wxverify.worker.scheduler import (
    _enqueue_due_wind_days,  # noqa: PLC2701 -- private, exercised directly (T47)
)
from wxverify.worker.score_batches import ScoringInputsBusy
from wxverify.worker.wind_days import (  # noqa: PLC2701 -- private, exercised directly
    WIND_DAYS_JOB_KEY,
    _derive_days,
    _end_purge,
    _headroom_wake,
    _install_step,
    _persist_day,
    _purge_step,
    _reconcile,
    _run_rescoring,
    _staging_purge_day,
    _switch_check,
    _write_blocked,
    endpoint_for,
    ensure_wind_days,
    open_endpoints,
    run_wind_days,
    wind_days_due,
    wind_lane_due,
)

_TZ = "UTC"


# ---------------------------------------------------------------------------
# Shared harness
# ---------------------------------------------------------------------------


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, name: str = "Testsite", tz: str = _TZ) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 0.0, 0.0, 0.0, ?)",
        (name, tz),
    )
    assert cur.lastrowid is not None
    conn.commit()
    return int(cur.lastrowid)


def _make_station(
    conn: sqlite3.Connection, site_id: int, pws_id: str, *, enabled: bool = True
) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m,"
        " enabled) VALUES (?, ?, 0.0, 0.0, 0.0, ?)",
        (site_id, pws_id, 1 if enabled else 0),
    )
    assert cur.lastrowid is not None
    conn.commit()
    return int(cur.lastrowid)


def _insert_day(
    conn: sqlite3.Connection,
    station_id: int,
    local_date: date | str,
    *,
    status: str = "pending",
    attempts: int = 0,
    record_count: int = 0,
    pair_hours: int = 0,
    legacy_hours: int | None = None,
    next_attempt_at: str | None = None,
    refetch_at: str | None = None,
    refetched: int = 0,
    last_error: str | None = None,
    last_ok_at: str | None = None,
) -> None:
    ld = local_date.isoformat() if isinstance(local_date, date) else local_date
    conn.execute(
        """
        INSERT INTO station_wind_days
            (station_id, local_date, status, attempts, record_count, pair_hours,
             legacy_hours, next_attempt_at, refetch_at, refetched, last_error,
             last_ok_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '2026-01-01T00:00:00Z')
        """,
        (
            station_id,
            ld,
            status,
            attempts,
            record_count,
            pair_hours,
            legacy_hours,
            next_attempt_at,
            refetch_at,
            refetched,
            last_error,
            last_ok_at,
        ),
    )
    conn.commit()


def _freeze_wind_days_today(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    """``_switch_check``/``_run_switching`` compute ``today`` internally via
    ``_local_today(tz_name)`` -> ``utc_now()`` -- freeze the clock the lane
    module itself imported, not the wall clock."""
    import wxverify.worker.wind_days as wind_days_module

    monkeypatch.setattr(wind_days_module, "utc_now", lambda: when)


def _freeze_scheduler_now(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    """``_enqueue_due_wind_days`` computes ``now`` via the scheduler module's
    own imported ``utc_now``, and the ``enqueue_if_absent_with_cooldown``
    wrapper it calls computes the success-cooldown comparison via its OWN
    separately-imported ``utc_now`` in ``wxverify.db.queue`` -- freeze both
    bindings, not the wall clock."""
    import wxverify.db.queue as queue_module
    import wxverify.worker.scheduler as scheduler_module

    monkeypatch.setattr(scheduler_module, "utc_now", lambda: when)
    monkeypatch.setattr(queue_module, "utc_now", lambda: when)


def _insert_job(
    conn: sqlite3.Connection,
    job_type: str,
    site_id: int,
    job_key: str,
    *,
    status: str = "pending",
    updated_at: str = "2026-01-01T00:00:00Z",
) -> None:
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status, updated_at)"
        " VALUES (?, ?, ?, ?, ?)",
        (job_type, site_id, job_key, status, updated_at),
    )
    conn.commit()


def _day_row(
    conn: sqlite3.Connection, station_id: int, local_date: date
) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM station_wind_days WHERE station_id = ? AND local_date = ?",
        (station_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    return row


def _insert_pair_max_obs(
    conn: sqlite3.Connection, station_id: int, valid_at: str, speed_kmh: float = 10.0
) -> None:
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw)"
        " VALUES (?, 'wind', ?, ?, 'ok', ?)",
        (station_id, valid_at, speed_kmh / 3.6, f"pair-max {speed_kmh} km/h"),
    )
    conn.commit()


# =============================================================================
# T8 -- a record outside the stored window is not stored; record_count counts
# in-day records only.
# =============================================================================


def test_persist_day_window_and_record_count_scope() -> None:
    """``_persist_day`` (wind_days.py:1043-1161): stored window is
    [start-24h, min(end+24h, now+5m)); ``record_count`` is the exact
    [start, end) day window. A record inside the stored margin but outside
    the day is kept in ``station_wind_records`` but not counted; a record
    outside the stored margin entirely is dropped from both."""
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    local_date = date(2020, 1, 15)
    set_wind_basis_state(conn, site_id, "staging")
    _insert_day(conn, station_id, local_date, status="pending")

    start = datetime(2020, 1, 15, 0, 0, 0, tzinfo=UTC)
    in_margin_not_in_day = WindRecord(
        obs_at=start - timedelta(minutes=30), speed_kmh=5.0
    )
    outside_margin = WindRecord(obs_at=start - timedelta(hours=25), speed_kmh=5.0)
    in_day = WindRecord(obs_at=start + timedelta(hours=1), speed_kmh=7.0)

    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=_TZ,
        today=date(2020, 1, 20),
        records=[in_margin_not_in_day, outside_margin, in_day],
        probe=None,
    )

    stored = {
        row["obs_at"]
        for row in conn.execute(
            "SELECT obs_at FROM station_wind_records WHERE station_id = ?",
            (station_id,),
        )
    }
    assert in_margin_not_in_day.obs_at.isoformat().replace("+00:00", "Z") in stored
    assert in_day.obs_at.isoformat().replace("+00:00", "Z") in stored
    assert outside_margin.obs_at.isoformat().replace("+00:00", "Z") not in stored

    row = _day_row(conn, station_id, local_date)
    assert row["record_count"] == 1, (
        "mutant -> at this assertion: correct = 1 (only `in_day` falls"
        " inside [start, end)), mutant (COUNT(*) dropped the obs_at >= start"
        " AND obs_at < end window clause, counting every stored record for"
        " the station instead) = 2"
    )


# =============================================================================
# T30 -- ensure_wind_days
# =============================================================================


def test_ensure_wind_days_null_first_observation_gives_today_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.wind_days"):
        ensure_wind_days(conn, site_id, _TZ, today)  # type: ignore[arg-type]

    rows = conn.execute(
        "SELECT local_date FROM station_wind_days ORDER BY local_date"
    ).fetchall()
    assert [r["local_date"] for r in rows] == [today.isoformat()]
    assert any("no first observation" in rec.message for rec in caplog.records)


def test_ensure_wind_days_range_from_first_observation_to_today() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
        " VALUES (?, 'temperature', '2026-03-07T00:00:00Z', 1.0, 1)",
        (site_id,),
    )
    conn.commit()
    today = date(2026, 3, 10)

    from zoneinfo import ZoneInfo

    ensure_wind_days(conn, site_id, ZoneInfo(_TZ), today)

    dates = {
        r["local_date"]
        for r in conn.execute(
            "SELECT local_date FROM station_wind_days WHERE station_id = ?",
            (station_id,),
        )
    }
    assert dates == {"2026-03-07", "2026-03-08", "2026-03-09", "2026-03-10"}, (
        "mutant -> at this assertion: correct = 4 contiguous dates from the"
        " first observation's local date through today, mutant (range off"
        " by a day, e.g. first+1 or today-1 as the upper bound) = a"
        " different date set"
    )


def test_ensure_wind_days_deletes_unparseable_local_date(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    _insert_day(conn, station_id, "2026-13-40", status="pending")  # not canonical
    today = date(2026, 3, 10)

    from zoneinfo import ZoneInfo

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.wind_days"):
        ensure_wind_days(conn, site_id, ZoneInfo(_TZ), today)

    remaining = conn.execute(
        "SELECT local_date FROM station_wind_days WHERE station_id = ?"
        " AND local_date = '2026-13-40'",
        (station_id,),
    ).fetchall()
    assert remaining == [], (
        "mutant -> at this assertion: correct = [] (the malformed row is"
        " deleted), mutant (the canonical-date guard dropped) = the row"
        " still present"
    )
    assert any("unreadable date deleted" in rec.message for rec in caplog.records)


# =============================================================================
# T31 -- persist in `staging` is counts-only: no station row/observation/job
# =============================================================================


def test_persist_day_in_staging_updates_counts_only_no_observation_written() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    local_date = date(2026, 3, 5)
    set_wind_basis_state(conn, site_id, "staging")
    _insert_day(conn, station_id, local_date, status="pending")

    start = datetime(2026, 3, 5, 0, 0, 0, tzinfo=UTC)
    records = [
        WindRecord(obs_at=start + timedelta(hours=1), speed_kmh=5.0),
        WindRecord(obs_at=start + timedelta(hours=1, minutes=3), speed_kmh=6.0),
    ]
    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=_TZ,
        today=date(2026, 3, 10),
        records=records,
        probe=None,
    )

    obs = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    assert obs["n"] == 0, (
        "mutant -> at this assertion: correct = 0 (staging writes no station"
        " observation row), mutant (pair_max flag inverted so staging also"
        " materializes) = 1"
    )
    jobs = conn.execute("SELECT COUNT(*) AS n FROM jobs").fetchone()
    assert jobs["n"] == 0, (
        "mutant -> at this assertion: correct = 0 (no pair_and_score"
        " enqueue in staging), mutant (enqueue runs unconditionally) = 1"
    )
    row = _day_row(conn, station_id, local_date)
    assert row["record_count"] == 2
    assert row["pair_hours"] == 1  # the two records pair into one hour
    assert row["status"] == "fetched"


# =============================================================================
# T32 -- staging purge: day by day, re-materialize, NULL-julianday handling
# =============================================================================


def test_staging_purge_day_deletes_oldest_day_first_then_stray_then_stops(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "staging")
    _insert_pair_max_obs(conn, station_id, "2026-03-02T01:00:00Z")
    _insert_pair_max_obs(conn, station_id, "2026-03-04T01:00:00Z")
    _insert_pair_max_obs(conn, station_id, "garbage-timestamp")

    # Round 1: the earliest parseable day (03-02) is purged; the later day
    # and the unparseable row are untouched.
    more1 = _staging_purge_day(conn, site_id, _TZ)
    assert more1 is True
    remaining = {
        str(r["valid_at"])
        for r in conn.execute(
            "SELECT valid_at FROM station_observations WHERE station_id = ?",
            (station_id,),
        )
    }
    assert remaining == {"2026-03-04T01:00:00Z", "garbage-timestamp"}, (
        "mutant -> at this assertion: correct = the 03-02 row alone removed"
        " (oldest parseable day first), mutant (whole-table purge in one"
        " call) = an empty remaining set"
    )

    # Round 2: the remaining parseable day (03-04) is purged.
    more2 = _staging_purge_day(conn, site_id, _TZ)
    assert more2 is True
    remaining2 = {
        str(r["valid_at"])
        for r in conn.execute(
            "SELECT valid_at FROM station_observations WHERE station_id = ?",
            (station_id,),
        )
    }
    assert remaining2 == {"garbage-timestamp"}

    # Round 3: only the unparseable row is left -> MIN(julianday) is NULL ->
    # the stray branch purges it directly and logs a WARNING, then stops.
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.wind_days"):
        more3 = _staging_purge_day(conn, site_id, _TZ)
    assert more3 is False, (
        "mutant -> at this assertion: correct = False (nothing left after"
        " the stray sweep), mutant (stray branch returns True, looping"
        " forever) = True"
    )
    final = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    assert final["n"] == 0
    assert any("unreadable time purged" in rec.message for rec in caplog.records)


def test_staging_purge_day_false_when_not_in_staging() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "pair_max")
    _insert_pair_max_obs(conn, station_id, "2026-03-02T01:00:00Z")

    more = _staging_purge_day(conn, site_id, _TZ)

    assert more is False, (
        "mutant -> at this assertion: correct = False (the state guard"
        " refuses to run outside staging), mutant (guard dropped) = True"
    )
    still_there = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    assert still_there["n"] == 1


# =============================================================================
# T33 -- due selector: ordering, failed@3, unreadable next_attempt_at,
# held/probing endpoints
# =============================================================================


def test_open_endpoints_held_hidden_probing_visible_only_when_asked() -> None:
    from wxverify.db.wind_basis import AuthHold

    held = AuthHold(endpoint="history_all", status="held", since="x", error=None)
    probing = AuthHold(endpoint="all_1day", status="probing", since="x", error=None)

    opened_default = open_endpoints(
        {"history_all": held, "all_1day": probing}, probing_open=False
    )
    assert opened_default == frozenset(), (
        "mutant -> at this assertion: correct = frozenset() (a probing hold"
        " is NOT open when probing_open=False), mutant (probing_open check"
        " dropped) = frozenset({'all_1day'})"
    )

    opened_probing = open_endpoints(
        {"history_all": held, "all_1day": probing}, probing_open=True
    )
    assert opened_probing == frozenset({"all_1day"}), (
        "mutant -> at this assertion: correct = {'all_1day'} (held stays"
        " closed even when probing is asked for), mutant (status=='held'"
        " also treated as probing-eligible) = {'history_all', 'all_1day'}"
    )


def test_wind_days_due_orders_exempt_then_date_then_station() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    s1 = _make_station(conn, site_id, "WSTATION01")
    s2 = _make_station(conn, site_id, "WSTATION02")
    today = date(2026, 3, 10)
    yesterday = today - timedelta(days=1)
    # Non-exempt, older date, both stations (date asc, then station asc).
    _insert_day(conn, s2, today - timedelta(days=3), status="pending")
    _insert_day(conn, s1, today - timedelta(days=3), status="pending")
    _insert_day(conn, s2, today - timedelta(days=2), status="pending")
    # Exempt (>= yesterday) rows, which must sort first regardless of date.
    _insert_day(conn, s2, yesterday, status="pending")
    _insert_day(conn, s1, today, status="pending")

    due = wind_days_due(
        conn,
        site_id,
        today=today,
        now=datetime(2026, 3, 10, 12, tzinfo=UTC),
        open_endpoints={"history_all", "all_1day"},
        limit=10,
    )

    key = [(row.exempt, row.local_date, row.station_id) for row in due]
    assert key == [
        (True, yesterday, s2),
        (True, today, s1),
        (False, today - timedelta(days=3), s1),
        (False, today - timedelta(days=3), s2),
        (False, today - timedelta(days=2), s2),
    ], (
        "mutant -> at this assertion: correct = exempt rows first (ordered"
        " by date then station), then non-exempt by date then station;"
        " mutant (`ORDER BY exempt DESC` flipped to `exempt ASC`) = the"
        " non-exempt rows sort first instead"
    )


def test_wind_days_due_excludes_failed_at_max_attempts() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    _insert_day(
        conn, station_id, today - timedelta(days=5), status="failed", attempts=3
    )

    due = wind_days_due(
        conn,
        site_id,
        today=today,
        now=datetime(2026, 3, 10, 12, tzinfo=UTC),
        open_endpoints={"history_all", "all_1day"},
        limit=10,
    )

    assert due == [], (
        "mutant -> at this assertion: correct = [] (failed at attempts>=3 is"
        " never due), mutant (attempts < 3 relaxed to <=3) = one row present"
    )


def test_wind_days_due_unreadable_next_attempt_at_fails_open() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    _insert_day(
        conn,
        station_id,
        today - timedelta(days=5),
        status="pending",
        next_attempt_at="not-a-timestamp",
    )

    due = wind_days_due(
        conn,
        site_id,
        today=today,
        now=datetime(2026, 3, 10, 12, tzinfo=UTC),
        open_endpoints={"history_all", "all_1day"},
        limit=10,
    )

    assert len(due) == 1, (
        "mutant -> at this assertion: correct = 1 (an unparseable"
        " next_attempt_at fails OPEN -- the row is due), mutant (the"
        " `julianday(...) IS NULL` fail-open clause dropped) = 0"
    )


# =============================================================================
# T34 -- status transitions on persist
# =============================================================================


def test_persist_day_today_204_stays_partial_with_refresh_schedule() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    _insert_day(conn, station_id, today, status="partial", attempts=0)

    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=today,
        tz_name=_TZ,
        today=today,
        records=[],
        probe=None,
    )

    row = _day_row(conn, station_id, today)
    assert row["status"] == "partial", (
        "mutant -> at this assertion: correct = 'partial' (today's row never"
        " leaves partial), mutant (falls through to the"
        " attempts-based branch) = 'pending' or 'unavailable'"
    )
    assert row["next_attempt_at"] is not None


@pytest.mark.parametrize(
    ("call", "expected_status", "expected_attempts"),
    [(1, "pending", 1), (2, "pending", 2), (3, "pending", 3), (4, "unavailable", 3)],
)
def test_persist_day_yesterday_204_retries_then_unavailable(
    call: int, expected_status: str, expected_attempts: int
) -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    yesterday = today - timedelta(days=1)
    _insert_day(conn, station_id, yesterday, status="pending", attempts=0)

    for _ in range(call):
        _persist_day(
            conn,
            site_id=site_id,
            station_id=station_id,
            local_date=yesterday,
            tz_name=_TZ,
            today=today,
            records=[],
            probe=None,
        )

    row = _day_row(conn, station_id, yesterday)
    assert (row["status"], row["attempts"]) == (expected_status, expected_attempts), (
        "mutant -> at this assertion: correct = "
        f"({expected_status!r}, {expected_attempts}) after {call} empty"
        " persists (3 retries, then unavailable), mutant (_MAX_ATTEMPTS off"
        " by one) = a different pair"
    )


def test_persist_day_older_than_yesterday_204_is_unavailable_at_once() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    older = today - timedelta(days=5)
    _insert_day(conn, station_id, older, status="pending", attempts=0)

    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=older,
        tz_name=_TZ,
        today=today,
        records=[],
        probe=None,
    )

    row = _day_row(conn, station_id, older)
    assert row["status"] == "unavailable", (
        "mutant -> at this assertion: correct = 'unavailable' (an older-than"
        "-yesterday 204 never gets the yesterday-only retry path), mutant"
        " (`local_date == yesterday` loosened to `<=`) = 'pending'"
    )
    assert row["attempts"] == 0


def test_today_next_attempt_near_midnight_rolls_to_tomorrow_plus_an_hour() -> None:
    from wxverify.worker.wind_days import _today_next_attempt  # noqa: PLC2701

    conn = _make_db()
    end = datetime(2026, 3, 11, 0, 0, 0, tzinfo=UTC)

    normal = _today_next_attempt(conn, end - timedelta(hours=5), end)
    assert normal < end.isoformat().replace("+00:00", "Z"), (
        "mutant -> at this assertion: correct = now+interval (well before"
        " midnight), mutant (always rolls to end+1h) = end+1h regardless"
    )

    near_midnight = _today_next_attempt(conn, end - timedelta(minutes=5), end)
    assert near_midnight == "2026-03-11T01:00:00Z", (
        "mutant -> at this assertion: correct = start(d+1)+1h"
        " ('2026-03-11T01:00:00Z'), mutant (the >= end rollover check"
        " dropped) = a time before midnight"
    )


def test_endpoint_for_today_is_all_1day_else_history_all() -> None:
    today = date(2026, 3, 10)
    assert endpoint_for(today, today) == "all_1day"
    assert endpoint_for(today - timedelta(days=1), today) == "history_all", (
        "mutant -> at this assertion: correct = 'history_all' for a past"
        " date, mutant (the equality check inverted) = 'all_1day'"
    )


# =============================================================================
# T36 -- persist in `pair_max` materializes only changed hours and enqueues
# =============================================================================


def test_persist_day_pair_max_materializes_changed_hours_and_enqueues() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    local_date = date(2026, 3, 5)
    set_wind_basis_state(conn, site_id, "pair_max")
    _insert_day(conn, station_id, local_date, status="pending")

    start = datetime(2026, 3, 5, 0, 0, 0, tzinfo=UTC)
    records = [
        WindRecord(obs_at=start + timedelta(hours=1), speed_kmh=5.0),
        WindRecord(obs_at=start + timedelta(hours=1, minutes=3), speed_kmh=9.0),
    ]
    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=_TZ,
        today=date(2026, 3, 10),
        records=records,
        probe=None,
    )

    obs = conn.execute(
        "SELECT value, source_raw FROM station_observations WHERE station_id = ?",
        (station_id,),
    ).fetchall()
    assert len(obs) == 1, (
        "mutant -> at this assertion: correct = 1 row (one paired hour),"
        " mutant (write_rows gated wrong so nothing is written) = 0"
    )
    assert str(obs[0]["source_raw"]).startswith("pair-max ")
    jobs = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type = 'pair_and_score'"
    ).fetchone()
    assert jobs["n"] == 1, (
        "mutant -> at this assertion: correct = 1 (changed hours enqueue"
        " pair_and_score), mutant (enqueue gated on `if write_rows` dropped"
        " the `if changed` check) = 0 when nothing changed, but here"
        " something did change, so a broken enqueue shows as 0"
    )

    # A second persist with identical data writes no new observation and
    # does not enqueue again (idempotent -- `changed` stays empty).
    jobs_before = jobs["n"]
    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=_TZ,
        today=date(2026, 3, 10),
        records=records,
        probe=None,
    )
    jobs_after = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type = 'pair_and_score'"
    ).fetchone()["n"]
    assert jobs_after == jobs_before, (
        "mutant -> at this assertion: correct = unchanged job count"
        " (identical data -> no re-materialize, no re-enqueue), mutant"
        " (enqueue runs unconditionally on every persist) = jobs_before + 1"
    )


# =============================================================================
# T37/T38/T39/T40 -- switch check (plan section 8.9)
# =============================================================================


def _seed_switchable_site(conn: sqlite3.Connection) -> tuple[int, int]:
    """A site with one station whose yesterday row is fetched and nothing
    else outstanding -- a pass under every predicate."""
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "staging")
    today = date(2026, 3, 10)
    _insert_day(
        conn,
        station_id,
        today - timedelta(days=1),
        status="fetched",
        record_count=5,
        pair_hours=MIN_OBS_HOURS,
    )
    return site_id, station_id


_BIG_FREE = 1024 * 1024 * 1024


def test_switch_check_passes_when_all_seven_predicates_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _freeze_wind_days_today(monkeypatch, datetime(2026, 3, 10, 12, tzinfo=UTC))
    conn = _make_db()
    site_id, _ = _seed_switchable_site(conn)

    switched = _switch_check(conn, site_id, _TZ, _BIG_FREE)

    assert switched is True
    assert wind_basis_state(conn, site_id) == "switching", (
        "mutant -> at this assertion: correct = 'switching' (the CAS moved"
        " the site forward), mutant (any one predicate inverted) = the"
        " state stays 'staging'"
    )


@pytest.mark.parametrize(
    "break_predicate",
    [
        "p1_retry_before_yesterday",
        "p2_yesterday_usable_no_fetched_row",
        "p2_yesterday_usable_pair_hours_zero",
        "p4_pair_max_rows",
        "p6_auth_hold",
    ],
)
def test_switch_check_fails_on_each_predicate(
    break_predicate: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _freeze_wind_days_today(monkeypatch, datetime(2026, 3, 10, 12, tzinfo=UTC))
    conn = _make_db()
    site_id, station_id = _seed_switchable_site(conn)
    today = date(2026, 3, 10)

    if break_predicate == "p1_retry_before_yesterday":
        _insert_day(
            conn, station_id, today - timedelta(days=3), status="pending", attempts=0
        )
    elif break_predicate == "p2_yesterday_usable_no_fetched_row":
        # 'partial' (not 'unavailable') keeps retry_before_today > 0 via
        # yesterday's own row, so `exhausted` stays False and p2 alone is
        # what blocks the switch -- 'unavailable' would make this
        # indistinguishable from the exhausted-dominated path. No row at
        # all meets predicate 2 (none is `fetched`).
        conn.execute(
            "UPDATE station_wind_days SET status = 'partial'"
            " WHERE station_id = ? AND local_date = ?",
            (station_id, (today - timedelta(days=1)).isoformat()),
        )
        conn.commit()
    elif break_predicate == "p2_yesterday_usable_pair_hours_zero":
        # The other way predicate 2 can fail: yesterday IS `fetched`, but
        # its `pair_hours` is 0 (records that never paired). This drives
        # `exhausted` True (yesterday is final, not retryable), so the
        # check takes the blocked branch -- still `switched is False`.
        conn.execute(
            "UPDATE station_wind_days SET pair_hours = 0"
            " WHERE station_id = ? AND local_date = ?",
            (station_id, (today - timedelta(days=1)).isoformat()),
        )
        conn.commit()
    elif break_predicate == "p4_pair_max_rows":
        _insert_pair_max_obs(conn, station_id, "2026-03-01T01:00:00Z")
    elif break_predicate == "p6_auth_hold":
        from wxverify.db.wind_basis import write_auth_hold

        write_auth_hold(
            conn, "history_all", status="held", since="2026-03-01T00:00:00Z", error=None
        )
        conn.commit()

    switched = _switch_check(conn, site_id, _TZ, _BIG_FREE)

    assert switched is False, (
        f"mutant -> at this assertion: correct = False (breaking"
        f" {break_predicate} alone must block the switch), mutant (that"
        " predicate dropped from the `and` chain) = True"
    )
    assert wind_basis_state(conn, site_id) == "staging"


def test_switch_check_p7_free_disk_none_blocks_with_warning(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    _freeze_wind_days_today(monkeypatch, datetime(2026, 3, 10, 12, tzinfo=UTC))
    conn = _make_db()
    site_id, _ = _seed_switchable_site(conn)

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.wind_days"):
        switched = _switch_check(conn, site_id, _TZ, None)

    assert switched is False, (
        "mutant -> at this assertion: correct = False (an unreadable free-"
        "disk value blocks the switch), mutant (`free is None` treated as"
        " passing p7) = True"
    )
    assert any("free disk space unreadable" in rec.message for rec in caplog.records)
    blocked = get_runtime_state(conn, wind_blocked_key(site_id))
    assert blocked is not None
    reason = json.loads(blocked)["reason"]
    assert reason == "free disk space could not be read", (
        "mutant -> at this assertion: correct = 'free disk space could not"
        " be read' (reason 1, §8.9), mutant (a different reason string"
        " written, or reason 1 not checked first) ="
        f" {reason!r}"
    )


def test_switch_check_blocked_vs_waiting(monkeypatch: pytest.MonkeyPatch) -> None:
    """``exhausted`` (all retryable work for yesterday done, none fetched, or
    the failed-final share too high) writes the blocked key; a retry still
    pending (p1 false but not exhausted) leaves no blocked key."""
    _freeze_wind_days_today(monkeypatch, datetime(2026, 3, 10, 12, tzinfo=UTC))
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "staging")
    today = date(2026, 3, 10)
    # Yesterday's only row ends unavailable (nothing fetched) and no retry
    # is pending anywhere -> exhausted -> blocked.
    _insert_day(conn, station_id, today - timedelta(days=1), status="unavailable")

    switched = _switch_check(conn, site_id, _TZ, _BIG_FREE)

    assert switched is False
    blocked = get_runtime_state(conn, wind_blocked_key(site_id))
    assert blocked is not None, (
        "mutant -> at this assertion: correct = a blocked key IS written"
        " (yesterday fully exhausted with nothing fetched), mutant (the"
        " `exhausted` condition dropped the `yesterday_usable == 0` arm)"
        " = no blocked key"
    )
    reason = json.loads(blocked)["reason"]
    assert reason == "no station returned yesterday's data", (
        "mutant -> at this assertion: correct = 'no station returned"
        " yesterday's data' (reason 5, §8.9 -- yesterday_fetched == 0),"
        " mutant (reason 4's text written instead, or the two reasons"
        f" swapped) = {reason!r}"
    )

    # The other exhausted-blocked shape: yesterday IS fetched, but its
    # pair_hours is 0 (records that never paired) -> reason 4, not 5.
    conn1b = _make_db()
    site_id1b = _make_site(conn1b)
    station_id1b = _make_station(conn1b, site_id1b, "WSTATION01")
    set_wind_basis_state(conn1b, site_id1b, "staging")
    _insert_day(
        conn1b,
        station_id1b,
        today - timedelta(days=1),
        status="fetched",
        record_count=96,
        pair_hours=0,
    )
    switched1b = _switch_check(conn1b, site_id1b, _TZ, _BIG_FREE)
    assert switched1b is False
    blocked1b = get_runtime_state(conn1b, wind_blocked_key(site_id1b))
    assert blocked1b is not None
    reason1b = json.loads(blocked1b)["reason"]
    assert reason1b == (
        "no station had 22 hours of wind readings at most 10 minutes apart yesterday"
    ), (
        "mutant -> at this assertion: correct = reason 4's text, naming"
        f" MIN_OBS_HOURS (22), mutant (reason 5's text, or a different"
        f" threshold) = {reason1b!r}"
    )
    assert f"{MIN_OBS_HOURS} hours" in reason1b

    # Now add a still-pending retry for an older day: p1 is false (not
    # waiting-exhausted, since retry_before_today > 0) -> no blocked key.
    conn2 = _make_db()
    site_id2 = _make_site(conn2)
    station_id2 = _make_station(conn2, site_id2, "WSTATION01")
    set_wind_basis_state(conn2, site_id2, "staging")
    _insert_day(conn2, station_id2, today - timedelta(days=1), status="unavailable")
    _insert_day(
        conn2, station_id2, today - timedelta(days=3), status="pending", attempts=0
    )
    switched2 = _switch_check(conn2, site_id2, _TZ, _BIG_FREE)
    assert switched2 is False
    blocked2 = get_runtime_state(conn2, wind_blocked_key(site_id2))
    assert blocked2 is None, (
        "mutant -> at this assertion: correct = None (a pending retry"
        " elsewhere means the site is still WAITING, not blocked), mutant"
        " (every non-switch outcome writes blocked) = not None"
    )


def test_switch_check_too_little_disk_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    _freeze_wind_days_today(monkeypatch, datetime(2026, 3, 10, 12, tzinfo=UTC))
    conn = _make_db()
    site_id, _ = _seed_switchable_site(conn)

    switched = _switch_check(conn, site_id, _TZ, 1024)  # far below the 256 MiB floor

    assert switched is False, (
        "mutant -> at this assertion: correct = False (free bytes below"
        " WIND_SWITCH_MIN_FREE_BYTES blocks), mutant (p7 threshold check"
        " dropped) = True"
    )
    blocked = get_runtime_state(conn, wind_blocked_key(site_id))
    assert blocked is not None
    reason = json.loads(blocked)["reason"]
    assert reason == "not enough free disk space for the switch", (
        "mutant -> at this assertion: correct = 'not enough free disk space"
        " for the switch' (reason 2, §8.9), mutant (reason 1's text written"
        f" instead, or a different message) = {reason!r}"
    )


def test_switch_check_too_many_failed_final_blocks_with_reason_3(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed-final rows above 10% of before-today rows block with reason 3
    -- even when yesterday is also unusable (predicate 2 failing too), the
    first-applicable-reason ordering (§8.9) still picks reason 3. The
    second (``conn2``) fixture below is the identical shape also pinned
    directly inside ``test_t138_records_that_never_pair_block_the_switch``'s
    third case, driven through T138's own assertions rather than only
    here."""
    _freeze_wind_days_today(monkeypatch, datetime(2026, 3, 10, 12, tzinfo=UTC))
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "staging")
    today = date(2026, 3, 10)
    # Yesterday usable (pair_hours >= MIN_OBS_HOURS) so predicate 2 alone
    # does not also block -- isolates predicate 3.
    _insert_day(
        conn,
        station_id,
        today - timedelta(days=1),
        status="fetched",
        record_count=96,
        pair_hours=MIN_OBS_HOURS,
    )
    # 9 more final rows before today: 2 failed-final (attempts=3), 7 fetched
    # -> failed_final=2, before_today=10, share=20% > 10%.
    for n in range(2, 9):
        _insert_day(
            conn,
            station_id,
            today - timedelta(days=n),
            status="fetched",
            record_count=24,
        )
    _insert_day(
        conn, station_id, today - timedelta(days=9), status="failed", attempts=3
    )
    _insert_day(
        conn, station_id, today - timedelta(days=10), status="failed", attempts=3
    )

    switched = _switch_check(conn, site_id, _TZ, _BIG_FREE)

    assert switched is False
    blocked = get_runtime_state(conn, wind_blocked_key(site_id))
    assert blocked is not None
    reason = json.loads(blocked)["reason"]
    assert reason == "too many station-days failed", (
        "mutant -> at this assertion: correct = 'too many station-days"
        " failed' (reason 3, §8.9, failed_final > 10% of before_today),"
        f" mutant (predicate 3's check dropped or its reason text wrong) ="
        f" {reason!r}"
    )

    # Same failed-final overflow, but now yesterday ALSO fails predicate 2
    # (pair_hours 0) -- reason 3 still wins, being first in the §8.9 order.
    conn2 = _make_db()
    site_id2 = _make_site(conn2)
    station_id2 = _make_station(conn2, site_id2, "WSTATION01")
    set_wind_basis_state(conn2, site_id2, "staging")
    _insert_day(
        conn2,
        station_id2,
        today - timedelta(days=1),
        status="fetched",
        record_count=96,
        pair_hours=0,
    )
    for n in range(2, 9):
        _insert_day(
            conn2,
            station_id2,
            today - timedelta(days=n),
            status="fetched",
            record_count=24,
        )
    _insert_day(
        conn2, station_id2, today - timedelta(days=9), status="failed", attempts=3
    )
    _insert_day(
        conn2, station_id2, today - timedelta(days=10), status="failed", attempts=3
    )

    switched2 = _switch_check(conn2, site_id2, _TZ, _BIG_FREE)

    assert switched2 is False
    blocked2 = get_runtime_state(conn2, wind_blocked_key(site_id2))
    assert blocked2 is not None
    reason2 = json.loads(blocked2)["reason"]
    assert reason2 == "too many station-days failed", (
        "mutant -> at this assertion: correct = reason 3 (checked before"
        " reason 4 in the §8.9 order, even though predicate 2 also fails"
        " here), mutant (the order flipped so reason 4's text wins) ="
        f" {reason2!r}"
    )


# =============================================================================
# T138-T143 -- switch coverage, record reads and site deletion (plan 15.1)
# =============================================================================


def test_t138_records_that_never_pair_block_the_switch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every predicate holds except predicate 2: the enabled station's
    yesterday row is `fetched` with a full day of records (96) but zero
    pairing hours. A second case drives the reason-3-before-reason-4
    ordering (the same mutant shape ``test_switch_check_too_many_failed_
    final_blocks_with_reason_3`` above pins against its own ``conn2``)
    directly through this test's own fixture and assertions too: yesterday
    is unusable (predicate 2 fails, as in the primary case) AND
    failed-final rows exceed the 10% share (predicate 3 also fails), so
    reason 3 must still win over reason 4."""
    _freeze_wind_days_today(monkeypatch, datetime(2026, 3, 10, 12, tzinfo=UTC))
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "staging")
    today = date(2026, 3, 10)
    _insert_day(
        conn,
        station_id,
        today - timedelta(days=1),
        status="fetched",
        record_count=96,
        pair_hours=0,
    )

    switched = _switch_check(conn, site_id, _TZ, _BIG_FREE)

    assert switched is False, (
        "mutant -> at this assertion: correct = False (predicate 2 fails --"
        " yesterday is `fetched` but has zero pairing hours), mutant (the"
        " SQL form: the `pair_hours >= :min_pair_hours` clause dropped from"
        " `yesterday_usable`'s CASE expression in the SQL query, so it"
        " counts `fetched` alone, ignoring pair_hours) = True. (The Python"
        " form, `p2 = counts.yesterday_fetched > 0` in place of"
        " `counts.yesterday_usable > 0`, is killed by T140, not here.)"
    )
    assert wind_basis_state(conn, site_id) == "staging"
    blocked = get_runtime_state(conn, wind_blocked_key(site_id))
    assert blocked is not None
    reason = json.loads(blocked)["reason"]
    assert reason == (
        "no station had 22 hours of wind readings at most 10 minutes apart yesterday"
    ), (
        "mutant -> at this assertion: correct = reason 4's text, naming"
        f" MIN_OBS_HOURS, mutant (a different reason) = {reason!r}"
    )
    assert f"{MIN_OBS_HOURS} hours" in reason

    # Sub-case: yesterday fetched with pair_hours=0 (predicate 2 fails, as
    # above) AND failed-final rows exceed the 10% share (predicate 3 also
    # fails) -- reason 3 must still win, being checked first in the §8.9
    # order. Mirrors test_switch_check_too_many_failed_final_blocks_with_
    # reason_3's conn2, but pinned directly here in T138.
    conn3 = _make_db()
    site_id3 = _make_site(conn3)
    station_id3 = _make_station(conn3, site_id3, "WSTATION01")
    set_wind_basis_state(conn3, site_id3, "staging")
    _insert_day(
        conn3,
        station_id3,
        today - timedelta(days=1),
        status="fetched",
        record_count=96,
        pair_hours=0,
    )
    for n in range(2, 9):
        _insert_day(
            conn3,
            station_id3,
            today - timedelta(days=n),
            status="fetched",
            record_count=24,
        )
    _insert_day(
        conn3, station_id3, today - timedelta(days=9), status="failed", attempts=3
    )
    _insert_day(
        conn3, station_id3, today - timedelta(days=10), status="failed", attempts=3
    )

    switched3 = _switch_check(conn3, site_id3, _TZ, _BIG_FREE)

    assert switched3 is False
    blocked3 = get_runtime_state(conn3, wind_blocked_key(site_id3))
    assert blocked3 is not None
    reason3 = json.loads(blocked3)["reason"]
    assert reason3 == "too many station-days failed", (
        "mutant -> at this assertion: correct = 'too many station-days"
        " failed' (reason 3, checked before reason 4 in the §8.9 order,"
        " even though predicate 2 also fails here), mutant (the reason-4"
        " branch moved above the `elif not p3` check, e.g. written as"
        " `elif counts.yesterday_usable == 0 and counts.yesterday_fetched >"
        " 0:`) = reason 4's text instead"
    )


def test_t139_switch_pair_hours_boundary_and_at_least_one_station_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The predicate-2 threshold boundary at MIN_OBS_HOURS, and the "at
    least one station" semantics -- the switch counts stations whose own
    `pair_hours >= MIN_OBS_HOURS`, not a per-station AND, and not a total
    `pair_hours` summed across stations."""
    _freeze_wind_days_today(monkeypatch, datetime(2026, 3, 10, 12, tzinfo=UTC))
    today = date(2026, 3, 10)

    # 21 (MIN_OBS_HOURS - 1): blocked, reason 4.
    conn1 = _make_db()
    site_id1 = _make_site(conn1)
    station_id1 = _make_station(conn1, site_id1, "WSTATION01")
    set_wind_basis_state(conn1, site_id1, "staging")
    _insert_day(
        conn1,
        station_id1,
        today - timedelta(days=1),
        status="fetched",
        record_count=96,
        pair_hours=MIN_OBS_HOURS - 1,
    )
    switched1 = _switch_check(conn1, site_id1, _TZ, _BIG_FREE)
    assert switched1 is False, (
        "mutant -> at this assertion: correct = False (pair_hours ="
        f" {MIN_OBS_HOURS - 1}, below MIN_OBS_HOURS), mutant (a threshold"
        f" lowered to {MIN_OBS_HOURS - 1} or below) = True. (The `>`-for-"
        "`>=` mutant is NOT caught here -- 21 > 22 and 21 >= 22 are both"
        " False; the 22-hour case below is the one that catches it.)"
    )
    blocked1 = get_runtime_state(conn1, wind_blocked_key(site_id1))
    assert blocked1 is not None
    assert json.loads(blocked1)["reason"] == (
        "no station had 22 hours of wind readings at most 10 minutes apart yesterday"
    )

    # 22 (== MIN_OBS_HOURS): passes.
    conn2 = _make_db()
    site_id2 = _make_site(conn2)
    station_id2 = _make_station(conn2, site_id2, "WSTATION01")
    set_wind_basis_state(conn2, site_id2, "staging")
    _insert_day(
        conn2,
        station_id2,
        today - timedelta(days=1),
        status="fetched",
        record_count=96,
        pair_hours=MIN_OBS_HOURS,
    )
    switched2 = _switch_check(conn2, site_id2, _TZ, _BIG_FREE)
    assert switched2 is True, (
        "mutant -> at this assertion: correct = True (pair_hours =="
        f" {MIN_OBS_HOURS} meets the threshold), mutant (`>` used for `>=`,"
        " requiring one more hour) = False"
    )
    assert wind_basis_state(conn2, site_id2) == "switching"

    # Two stations: one at 0, one at MIN_OBS_HOURS -- "at least one usable
    # station", not "every enabled station usable".
    conn3 = _make_db()
    site_id3 = _make_site(conn3)
    station_a = _make_station(conn3, site_id3, "WSTATION01")
    station_b = _make_station(conn3, site_id3, "WSTATION02")
    set_wind_basis_state(conn3, site_id3, "staging")
    _insert_day(
        conn3,
        station_a,
        today - timedelta(days=1),
        status="fetched",
        record_count=96,
        pair_hours=0,
    )
    _insert_day(
        conn3,
        station_b,
        today - timedelta(days=1),
        status="fetched",
        record_count=96,
        pair_hours=MIN_OBS_HOURS,
    )
    switched3 = _switch_check(conn3, site_id3, _TZ, _BIG_FREE)
    assert switched3 is True, (
        "mutant -> at this assertion: correct = True (station B alone"
        " meets the threshold -- the count counts stations whose own"
        " pair_hours >= MIN_OBS_HOURS), mutant (predicate 2 required over"
        " EVERY station) = False"
    )
    assert wind_basis_state(conn3, site_id3) == "switching"

    # Two stations, each with pair_hours == MIN_OBS_HOURS // 2: neither
    # station alone reaches the threshold, but their total across stations
    # (MIN_OBS_HOURS) would. A "total hours across stations" rewrite of
    # `yesterday_usable` would pass here; the correct per-station count
    # must not.
    conn4 = _make_db()
    site_id4 = _make_site(conn4)
    station_c = _make_station(conn4, site_id4, "WSTATION01")
    station_d = _make_station(conn4, site_id4, "WSTATION02")
    set_wind_basis_state(conn4, site_id4, "staging")
    _insert_day(
        conn4,
        station_c,
        today - timedelta(days=1),
        status="fetched",
        record_count=96,
        pair_hours=MIN_OBS_HOURS // 2,
    )
    _insert_day(
        conn4,
        station_d,
        today - timedelta(days=1),
        status="fetched",
        record_count=96,
        pair_hours=MIN_OBS_HOURS // 2,
    )
    switched4 = _switch_check(conn4, site_id4, _TZ, _BIG_FREE)
    assert switched4 is False, (
        "mutant -> at this assertion: correct = False (neither station's"
        f" own pair_hours ({MIN_OBS_HOURS // 2}) reaches MIN_OBS_HOURS, even"
        f" though their total ({2 * (MIN_OBS_HOURS // 2)}) would), mutant"
        " (`yesterday_usable` rewritten to sum `pair_hours` across every"
        " enabled station and compare the total to MIN_OBS_HOURS) = True"
    )
    assert wind_basis_state(conn4, site_id4) == "staging"
    blocked4 = get_runtime_state(conn4, wind_blocked_key(site_id4))
    assert blocked4 is not None
    reason4 = json.loads(blocked4)["reason"]
    assert reason4 == (
        "no station had 22 hours of wind readings at most 10 minutes apart yesterday"
    ), (
        "mutant -> at this assertion: correct = reason 4's text, naming"
        f" MIN_OBS_HOURS, mutant (a different reason) = {reason4!r}"
    )


def test_t140_waiting_not_blocked_when_another_station_is_still_due(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Station A's yesterday is unusable (pair_hours 0), but station B's
    yesterday row is still `pending`: predicate 1 (nothing retryable
    strictly before yesterday) still holds, and `exhausted` is False
    because B's own row is retryable through today -- so the check returns
    False WITHOUT writing a `wind_basis_blocked` key: waiting, not
    blocked."""
    _freeze_wind_days_today(monkeypatch, datetime(2026, 3, 10, 12, tzinfo=UTC))
    today = date(2026, 3, 10)
    yesterday = today - timedelta(days=1)

    conn = _make_db()
    site_id = _make_site(conn)
    station_a = _make_station(conn, site_id, "WSTATION01")
    station_b = _make_station(conn, site_id, "WSTATION02")
    set_wind_basis_state(conn, site_id, "staging")
    _insert_day(
        conn, station_a, yesterday, status="fetched", record_count=96, pair_hours=0
    )
    _insert_day(conn, station_b, yesterday, status="pending")

    switched = _switch_check(conn, site_id, _TZ, _BIG_FREE)

    assert switched is False
    assert wind_basis_state(conn, site_id) == "staging"
    blocked = get_runtime_state(conn, wind_blocked_key(site_id))
    assert blocked is None, (
        "mutant -> at this assertion: correct = None (B's yesterday-dated"
        " `pending` row keeps `retry_before_today` > 0, so `exhausted`"
        " stays False and nothing is written -- the site is merely"
        " waiting), mutant (`retry_before_today` computed only over rows"
        " strictly before YESTERDAY, like predicate 1, missing B's row) ="
        f" a blocked key written = {blocked!r}"
    )

    # Control: B's row `unavailable` (final, not retryable) instead --
    # now nothing is retryable through today, `exhausted` is True, and the
    # same predicate-2 failure blocks with reason 4.
    conn2 = _make_db()
    site_id2 = _make_site(conn2)
    station_a2 = _make_station(conn2, site_id2, "WSTATION01")
    station_b2 = _make_station(conn2, site_id2, "WSTATION02")
    set_wind_basis_state(conn2, site_id2, "staging")
    _insert_day(
        conn2, station_a2, yesterday, status="fetched", record_count=96, pair_hours=0
    )
    _insert_day(conn2, station_b2, yesterday, status="unavailable")

    switched2 = _switch_check(conn2, site_id2, _TZ, _BIG_FREE)

    assert switched2 is False
    blocked2 = get_runtime_state(conn2, wind_blocked_key(site_id2))
    assert blocked2 is not None, (
        "mutant -> at this assertion: correct = a blocked key IS written"
        " once B's row is no longer retryable (exhausted becomes True),"
        " mutant (the waiting/blocked distinction collapsed so this stays"
        " unblocked too) = None"
    )
    assert json.loads(blocked2)["reason"] == (
        "no station had 22 hours of wind readings at most 10 minutes apart yesterday"
    )


def test_t141_persist_day_writes_pair_hours_through_the_counts_only_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End to end through the counts-only persist (`staging`, not
    `pair_max`): `_persist_day` must itself write `pair_hours` from the
    real records, not merely `record_count` -- 900 s spacing never pairs
    (gap > MAX_PAIR_GAP_SECONDS), 300 s spacing pairs every hour."""
    _freeze_wind_days_today(monkeypatch, datetime(2026, 3, 10, 12, tzinfo=UTC))
    today = date(2026, 3, 10)
    yesterday = today - timedelta(days=1)
    start = datetime(2026, 3, 9, 0, 0, 0, tzinfo=UTC)

    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_runtime_state(conn, wind_basis_key(site_id), "staging")
    _insert_day(conn, station_id, yesterday, status="pending")

    spaced_900 = [
        WindRecord(obs_at=start + timedelta(seconds=900 * i), speed_kmh=5.0)
        for i in range(96)
    ]
    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=yesterday,
        tz_name=_TZ,
        today=today,
        records=spaced_900,
        probe=None,
    )
    row = _day_row(conn, station_id, yesterday)
    assert row["status"] == "fetched"
    assert row["record_count"] == 96
    assert row["pair_hours"] == 0, (
        "mutant -> at this assertion: correct = 0 (900 s spacing exceeds"
        " the 600 s pair gap, no pair in any hour), mutant (`pair_hours`"
        " not written by the counts-only persist, left at its default, or"
        " read from `record_count` instead) = " + repr(row["pair_hours"])
    )

    switched = _switch_check(conn, site_id, _TZ, _BIG_FREE)
    assert switched is False, (
        "mutant -> at this assertion: correct = False (predicate 2 reads"
        " the just-persisted pair_hours, which is 0), mutant (predicate 2"
        " read from record_count instead, seeing 96 and passing) = True"
    )
    blocked = get_runtime_state(conn, wind_blocked_key(site_id))
    assert blocked is not None
    assert json.loads(blocked)["reason"] == (
        "no station had 22 hours of wind readings at most 10 minutes apart yesterday"
    )

    # Same day, 300 s spacing: every adjacent pair is inside the gap, so
    # every one of the 24 hours gets a pair.
    conn2 = _make_db()
    site_id2 = _make_site(conn2)
    station_id2 = _make_station(conn2, site_id2, "WSTATION01")
    set_runtime_state(conn2, wind_basis_key(site_id2), "staging")
    _insert_day(conn2, station_id2, yesterday, status="pending")

    spaced_300 = [
        WindRecord(obs_at=start + timedelta(seconds=300 * i), speed_kmh=5.0)
        for i in range(288)
    ]
    _persist_day(
        conn2,
        site_id=site_id2,
        station_id=station_id2,
        local_date=yesterday,
        tz_name=_TZ,
        today=today,
        records=spaced_300,
        probe=None,
    )
    row2 = _day_row(conn2, station_id2, yesterday)
    assert row2["status"] == "fetched"
    assert row2["pair_hours"] == 24, (
        "mutant -> at this assertion: correct = 24 (5-minute spacing pairs"
        " every hour of the day), mutant (`pair_hours` not written by the"
        " counts-only persist) = " + repr(row2["pair_hours"])
    )

    switched2 = _switch_check(conn2, site_id2, _TZ, _BIG_FREE)
    assert switched2 is True, (
        "mutant -> at this assertion: correct = True (pair_hours == 24 >="
        " MIN_OBS_HOURS), mutant (`pair_hours` never persisted outside"
        " `pair_max`, staying 0) = False"
    )
    assert wind_basis_state(conn2, site_id2) == "switching"


def test_t142_switch_check_reasons_match_the_remedies_map() -> None:
    """AST walk of `_switch_check` (plan section 15.1 T142): the string
    constants it can assign to `reason` are exactly the five section-8.9
    reasons, and exactly the keys of `WIND_BLOCKED_REMEDIES`; the reason-4
    and reason-5 remedies never fall back to "disable a station", which
    cannot help when the problem IS that a station has no data."""
    import wxverify.worker.wind_days as wind_days_module
    from wxverify.web.context import WIND_BLOCKED_REMEDIES

    source = Path(wind_days_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    switch_check = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_switch_check"
    )
    reasons: set[str] = set()
    for node in ast.walk(switch_check):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "reason"
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            reasons.add(node.value.value)

    reason_4 = (
        "no station had 22 hours of wind readings at most 10 minutes apart yesterday"
    )
    reason_5 = "no station returned yesterday's data"
    expected_reasons = {
        "free disk space could not be read",
        "not enough free disk space for the switch",
        "too many station-days failed",
        reason_4,
        reason_5,
    }
    assert reasons == expected_reasons, (
        "mutant -> at this assertion: correct = the five section-8.9"
        " reason strings, mutant (a reason string reworded or dropped"
        f" from `_switch_check`) = {reasons!r}"
    )
    assert reasons == set(WIND_BLOCKED_REMEDIES), (
        "mutant -> at this assertion: correct = WIND_BLOCKED_REMEDIES has"
        " exactly these five keys, mutant (a reason missing from the map,"
        " falling back to the disable-a-station default for it) ="
        f" {set(WIND_BLOCKED_REMEDIES)!r}"
    )

    assert WIND_BLOCKED_REMEDIES[reason_4] == (
        "make sure at least one station uploads its readings at least"
        " every 10 minutes all day; the switch checks again once the next"
        " day is fetched"
    ), (
        "mutant -> at this assertion: correct = the section-8.9 reason-4"
        " remedy verbatim, mutant (a different or disable-a-station"
        f" remedy) = {WIND_BLOCKED_REMEDIES[reason_4]!r}"
    )
    assert WIND_BLOCKED_REMEDIES[reason_5] == (
        "check that at least one station is online and uploading to"
        " weather.com; the switch checks again once the next day is"
        " fetched"
    ), (
        "mutant -> at this assertion: correct = the section-8.9 reason-5"
        " remedy verbatim, mutant (a different or disable-a-station"
        f" remedy) = {WIND_BLOCKED_REMEDIES[reason_5]!r}"
    )
    assert "disable" not in WIND_BLOCKED_REMEDIES[reason_4].lower(), (
        "mutant -> at this assertion: correct = no 'disable a station'"
        " language for reason 4 (it is a data-coverage problem, not a"
        " faulty-station one), mutant (the generic disable-a-station"
        f" fallback used here) = {WIND_BLOCKED_REMEDIES[reason_4]!r}"
    )
    assert "disable" not in WIND_BLOCKED_REMEDIES[reason_5].lower(), (
        "mutant -> at this assertion: correct = no 'disable a station'"
        " language for reason 5, mutant (the generic disable-a-station"
        f" fallback used here) = {WIND_BLOCKED_REMEDIES[reason_5]!r}"
    )


def test_t142_load_wind_history_surfaces_the_reason_4_remedy() -> None:
    """`load_wind_history` must surface the reason-4 remedy for a site
    whose `wind_basis_blocked` key holds reason 4 -- the panel-level wiring
    `WIND_BLOCKED_REMEDIES` alone cannot prove."""
    from wxverify.web.context import load_wind_history

    conn = _make_db()
    site_id = _make_site(conn)
    _make_station(conn, site_id, "WSTATION01")
    reason_4 = (
        "no station had 22 hours of wind readings at most 10 minutes apart yesterday"
    )
    set_runtime_state(
        conn,
        wind_blocked_key(site_id),
        json.dumps(
            {"reason": reason_4, "since": "2026-03-01T00:00:00Z", "reset_on": None}
        ),
    )

    panel = load_wind_history(conn)

    site_row = next(s for s in panel.sites if s.site_id == site_id)
    assert site_row.blocked_reason == reason_4
    assert site_row.remedy == (
        "make sure at least one station uploads its readings at least"
        " every 10 minutes all day; the switch checks again once the next"
        " day is fetched"
    ), (
        "mutant -> at this assertion: correct = the reason-4 remedy,"
        " mutant (WIND_BLOCKED_REMEDIES missing reason 4, falling back to"
        f" the disable-a-station default) = {site_row.remedy!r}"
    )


def _t143_insert_records(conn: sqlite3.Connection, station_id: int) -> None:
    """2026-03-09T18:00Z through 2026-03-10T20:00Z every 5 minutes at
    5.0 km/h, except 40.0 km/h at 18:40/18:45/18:50Z on 2026-03-10 --
    those three make the 18:00Z hour's max pair 40.0 (plan T143)."""
    start = datetime(2026, 3, 9, 18, 0, 0, tzinfo=UTC)
    end = datetime(2026, 3, 10, 20, 0, 0, tzinfo=UTC)
    hot = {
        datetime(2026, 3, 10, 18, 40, 0, tzinfo=UTC),
        datetime(2026, 3, 10, 18, 45, 0, tzinfo=UTC),
        datetime(2026, 3, 10, 18, 50, 0, tzinfo=UTC),
    }
    rows: list[tuple[int, str, float]] = []
    t = start
    while t <= end:
        rows.append((station_id, isoformat_utc(t), 40.0 if t in hot else 5.0))
        t += timedelta(minutes=5)
    conn.executemany(
        "INSERT INTO station_wind_records (station_id, obs_at, speed_kmh)"
        " VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()


def test_t143_record_read_in_a_non_whole_hour_offset_zone() -> None:
    """Plan section 8.5 step 3: the stored window is bounded by SELECTED
    UTC hours, not by the local day's own midnight-to-midnight bounds.
    Asia/Kolkata (UTC+5:30) is where the two bounds diverge: D =
    2026-03-10's last selected hour is 18:00Z, but D's own local midnight
    (the end of D) is 18:30Z UTC -- 30 minutes LATER. The records that
    make 18:00Z's pair (18:40/45/50Z) sit strictly after that local
    midnight and are only read because the window used is the UTC-hour
    one (``hours[-1] + 1h``), not ``local_day_slots``'s own end."""
    d = date(2026, 3, 10)
    tz = "Asia/Kolkata"
    from wxverify.db.wind_basis import WindCursor

    # -- install leg: `switching`, D `fetched`. ------------------------
    conn1 = _make_db()
    site_id1 = _make_site(conn1, tz=tz)
    station_id1 = _make_station(conn1, site_id1, "WSTATION01")
    set_wind_basis_state(conn1, site_id1, "switching")
    _insert_day(conn1, station_id1, d, status="fetched", record_count=288)
    _t143_insert_records(conn1, station_id1)

    more = _install_step(
        conn1,
        site_id1,
        tz,
        d + timedelta(days=2),
        WindCursor(phase="install", cursor_date=d, date_present=True),
    )
    assert more is True
    row1 = conn1.execute(
        "SELECT source_raw FROM station_observations"
        " WHERE station_id = ? AND valid_at = '2026-03-10T18:00:00Z'",
        (station_id1,),
    ).fetchone()
    assert row1 is not None and row1["source_raw"] == "pair-max 40.0 km/h", (
        "mutant -> at this assertion: correct = 'pair-max 40.0 km/h' (the"
        " 18:40/45/50Z records are read because the bound is the"
        " UTC-hour one), mutant (the read bounded by D's own local"
        " midnight, 18:30Z, instead -- those records fall outside it and"
        " the hour's only pair is 5.0) = "
        + repr(None if row1 is None else row1["source_raw"])
    )

    # -- last-derived-day leg: `pair_max`, D `fetched`, D+1 neither
    # `fetched` nor `partial`. ------------------------------------------
    conn2 = _make_db()
    site_id2 = _make_site(conn2, tz=tz)
    station_id2 = _make_station(conn2, site_id2, "WSTATION01")
    set_wind_basis_state(conn2, site_id2, "pair_max")
    _insert_day(conn2, station_id2, d, status="fetched", record_count=288)
    _insert_day(conn2, station_id2, d + timedelta(days=1), status="pending")
    _t143_insert_records(conn2, station_id2)

    _derive_days(
        conn2,
        site_id=site_id2,
        station_id=station_id2,
        local_date=d,
        tz_name=tz,
        today=d + timedelta(days=2),
        write_rows=True,
    )
    row2 = conn2.execute(
        "SELECT source_raw FROM station_observations"
        " WHERE station_id = ? AND valid_at = '2026-03-10T18:00:00Z'",
        (station_id2,),
    ).fetchone()
    assert row2 is not None and row2["source_raw"] == "pair-max 40.0 km/h", (
        "mutant -> at this assertion: correct = 'pair-max 40.0 km/h',"
        " mutant (the read bounded by D's own local midnight instead of"
        " the UTC-hour window) = " + repr(None if row2 is None else row2["source_raw"])
    )
    day2_row = _day_row(conn2, station_id2, d)
    assert day2_row["pair_hours"] == 24, (
        "mutant -> at this assertion: correct = 24 (every selected UTC"
        " hour pairs, given 5-minute spacing throughout), mutant (the"
        " window narrowed so some hours lose their pair) = "
        + repr(day2_row["pair_hours"])
    )

    # -- control: same records, site in UTC -- 18:00Z lies INSIDE D here,
    # so a local-midnight bound and the UTC-hour bound coincide; this leg
    # alone cannot distinguish the mutant above, only confirms the
    # non-tz-shifted case still produces the expected value. ------------
    conn3 = _make_db()
    site_id3 = _make_site(conn3, tz="UTC")
    station_id3 = _make_station(conn3, site_id3, "WSTATION01")
    set_wind_basis_state(conn3, site_id3, "switching")
    _insert_day(conn3, station_id3, d, status="fetched", record_count=288)
    _t143_insert_records(conn3, station_id3)

    more3 = _install_step(
        conn3,
        site_id3,
        "UTC",
        d + timedelta(days=2),
        WindCursor(phase="install", cursor_date=d, date_present=True),
    )
    assert more3 is True
    row3 = conn3.execute(
        "SELECT source_raw FROM station_observations"
        " WHERE station_id = ? AND valid_at = '2026-03-10T18:00:00Z'",
        (station_id3,),
    ).fetchone()
    assert row3 is not None and row3["source_raw"] == "pair-max 40.0 km/h"

    conn4 = _make_db()
    site_id4 = _make_site(conn4, tz="UTC")
    station_id4 = _make_station(conn4, site_id4, "WSTATION01")
    set_wind_basis_state(conn4, site_id4, "pair_max")
    _insert_day(conn4, station_id4, d, status="fetched", record_count=288)
    _insert_day(conn4, station_id4, d + timedelta(days=1), status="pending")
    _t143_insert_records(conn4, station_id4)
    _derive_days(
        conn4,
        site_id=site_id4,
        station_id=station_id4,
        local_date=d,
        tz_name="UTC",
        today=d + timedelta(days=2),
        write_rows=True,
    )
    row4 = conn4.execute(
        "SELECT source_raw FROM station_observations"
        " WHERE station_id = ? AND valid_at = '2026-03-10T18:00:00Z'",
        (station_id4,),
    ).fetchone()
    assert row4 is not None and row4["source_raw"] == "pair-max 40.0 km/h"


# =============================================================================
# T43 -- rescoring defers on ScoringInputsBusy
# =============================================================================


def test_t43_scoring_inputs_busy_on_every_cas_attempt_defers_the_rescore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_run_rescoring`` catches ``ScoringInputsBusy`` (every CAS attempt in
    ``run_batched_scoring``/``run_split_pair_phases`` lost to concurrent
    writes) and converts it to a ``JobDeferred`` 60s out, rather than letting
    the scoring internals' exception propagate raw. The two scoring
    functions are injected failing fakes here -- their own CAS-retry
    internals are a different unit, already exercised elsewhere; this test
    targets only ``_run_rescoring``'s own except/defer wiring.
    """
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    site_id = _make_site(db._conn)  # noqa: SLF001
    db._conn.commit()  # noqa: SLF001
    writer = FencedWriter(db, db.generation)

    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze_wind_days_today(monkeypatch, now)

    async def fake_split_pair_phases(
        writer: FencedWriter, site_id: int, *, require_enabled: bool
    ) -> None:
        return None

    async def fake_batched_scoring(writer: FencedWriter, site_id: int) -> None:
        raise ScoringInputsBusy(site_id, "window w1")

    monkeypatch.setattr(
        "wxverify.worker.wind_days.run_split_pair_phases", fake_split_pair_phases
    )
    monkeypatch.setattr(
        "wxverify.worker.wind_days.run_batched_scoring", fake_batched_scoring
    )

    from wxverify.worker.control import JobDeferred

    with pytest.raises(JobDeferred) as exc_info:
        asyncio.run(_run_rescoring(writer, site_id))

    assert exc_info.value.next_attempt_at == isoformat_utc(
        now + timedelta(seconds=60)
    ), (
        "mutant -> at this assertion: correct = the wake time is exactly"
        " now + 60s (_RESCORE_BUSY_DEFER), mutant (a different/zero defer"
        " duration spliced into the JobDeferred) = a different"
        " next_attempt_at"
    )


# =============================================================================
# T97 -- a station-day stuck on repeated 500s becomes final after 3 attempts,
# and no longer blocks the switch's own "retry before yesterday" predicate
# =============================================================================


_T97_API_KEY = "0123456789abcdef0123456789abcdef"


def _t97_init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001 -- tests inspect the real writer connection
    seed_default_sources(conn)
    conn.commit()
    return conn


def _t97_freeze(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    """Freeze ``utc_now()`` in every module the wind-days job calls it from."""
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.domain_backoff.utc_now", lambda: when)


def _t97_run_wind_days(
    site_id: int, handler: object, *, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _T97_API_KEY)
    real_client = httpx.AsyncClient
    db = get_db()
    writer = FencedWriter(db, db.generation)
    with patch(
        "wxverify.worker.wind_days.httpx.AsyncClient",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),  # type: ignore[arg-type]
    ):
        asyncio.run(run_wind_days(db, writer, site_id, client=None))


def test_t97_a_row_stuck_on_500s_goes_final_after_three_attempts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Drives four real jobs through ``run_wind_days`` against a single
    enabled station with one station-day two days before today (strictly
    before "yesterday", so it feeds the switch check's own
    ``retry_before_yesterday`` predicate), where ``history/all`` always
    answers 500. Today's auto-fabricated row is pre-closed so it never
    competes for a call.

    Jobs 1-3 each make exactly one request, record a row failure (attempts
    1, 2, 3; next_attempt_at +1h then +4h) and a domain backoff, and each
    raises ``JobDeferred``. Attempts reaching 3 makes the row no longer
    ``_RETRYABLE_SQL`` -- job 4, run well past both the row's own
    ``next_attempt_at`` and the domain backoff, makes zero requests and
    returns normally (no due row is left for it to call).
    """
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    stuck_date = today - timedelta(days=2)
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, updated_at)"
        " VALUES (?, ?, 'fetched', 0, 0, 0, 0, '2026-01-01T00:00:00Z')",
        (station_id, today.isoformat()),
    )
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, updated_at)"
        " VALUES (?, ?, 'pending', 0, 0, 0, 0, '2026-01-01T00:00:00Z')",
        (station_id, stuck_date.isoformat()),
    )
    # A past (non-exempt) row's call is gated on backfill headroom, which is
    # None -- treated as exhausted -- without a prior billing day's api_budget
    # row to compute a baseline from (plan's backfill_headroom contract).
    conn.execute(
        "INSERT INTO api_budget (source, billing_day, calls) VALUES"
        " ('weathercom', ?, 0)",
        ((today - timedelta(days=1)).isoformat(),),
    )
    conn.commit()

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(500, request=request)

    from wxverify.worker.control import JobDeferred

    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)
    _t97_freeze(monkeypatch, now)
    with pytest.raises(JobDeferred):
        _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert len(call_log) == 1
    row = _day_row(conn, station_id, stuck_date)
    assert row["status"] == "failed"
    assert row["attempts"] == 1

    now = now + timedelta(hours=1, minutes=1)
    _t97_freeze(monkeypatch, now)
    with pytest.raises(JobDeferred):
        _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert len(call_log) == 2
    row = _day_row(conn, station_id, stuck_date)
    assert row["status"] == "failed"
    assert row["attempts"] == 2

    now = now + timedelta(hours=4, minutes=1)
    _t97_freeze(monkeypatch, now)
    with pytest.raises(JobDeferred):
        _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert len(call_log) == 3
    row = _day_row(conn, station_id, stuck_date)
    assert row["status"] == "failed"
    assert row["attempts"] == 3, (
        "mutant -> at this assertion: correct = 3 (the row-failure rule runs"
        " on every >=500, incrementing attempts each time), mutant"
        " (``failure(conn)`` dropped from ``server_error`` in ``_fetch_failed``)"
        " = 0, attempts never advances"
    )

    now = now + timedelta(hours=4, minutes=1)
    _t97_freeze(monkeypatch, now)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)  # must not raise
    assert len(call_log) == 3, (
        "mutant -> at this assertion: correct = 3 (the now-final row, attempts"
        " >= 3, is excluded from WIND_DUE_SQL's retryable clause regardless of"
        " the clock, so job 4 has nothing left to call), mutant (``failure"
        "(conn)`` dropped from ``server_error``, so attempts stays 0 and"
        " next_attempt_at is never advanced past the first backoff) = 4, job 4"
        " calls the still-due row again"
    )
    row = _day_row(conn, station_id, stuck_date)
    assert row["attempts"] == 3


# =============================================================================
# T98 -- switching and rescoring never reach the fetch chunk, even with a due
# row, an active domain backoff and today's spend at the cap
# =============================================================================


def _t98_seed_traps(conn: sqlite3.Connection, station_id: int, today: date) -> None:
    """A due (exempt) row, an active weathercom domain backoff and today's
    spend at the stored cap -- three independent reasons a wrongly-dispatched
    fetch chunk would be blocked or would fail loudly, none of which the
    correct switching/rescoring dispatch ever reaches."""
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, updated_at)"
        " VALUES (?, ?, 'pending', 0, 0, 0, 0, '2026-01-01T00:00:00Z')",
        (station_id, (today - timedelta(days=1)).isoformat()),
    )
    conn.execute(
        "INSERT INTO domain_backoffs (domain, next_attempt_at, retry_count)"
        " VALUES ('api.weather.com', '2026-03-10T23:00:00Z', 1)"
    )
    conn.execute("UPDATE sources SET daily_call_limit = 1 WHERE source = 'weathercom'")
    conn.execute(
        "INSERT INTO api_budget (source, billing_day, calls) VALUES"
        " ('weathercom', ?, 1)",
        (today.isoformat(),),
    )
    conn.commit()


def test_t98_switching_makes_no_fetch_even_with_a_due_row_backoff_and_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    set_wind_basis_state(conn, site_id, "switching")
    write_wind_cursor(
        conn, site_id, phase="install", cursor_date=today - timedelta(days=20)
    )
    _t98_seed_traps(conn, station_id, today)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        raise httpx.ConnectError("refused", request=request)

    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)
    _t97_freeze(monkeypatch, now)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)  # must not raise

    assert len(call_log) == 0
    cursor = read_wind_cursor(conn, site_id)
    assert cursor.cursor_date == today - timedelta(days=6), (
        "mutant -> at this assertion: correct = the switching lane advanced"
        " the install cursor 14 days (``_DAY_TRANSACTIONS``) to today - 6 and"
        " returned, mutant (the early return after ``_run_switching`` dropped,"
        " so the job falls through) = the cursor never advances because the"
        " fallthrough raises JobDeferred before any install step runs"
    )
    assert wind_basis_state(conn, site_id) == "switching"


def test_t98_rescoring_makes_no_fetch_even_with_a_due_row_backoff_and_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    set_wind_basis_state(conn, site_id, "rescoring")
    _t98_seed_traps(conn, station_id, today)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        raise httpx.ConnectError("refused", request=request)

    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)
    _t97_freeze(monkeypatch, now)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)  # must not raise

    assert len(call_log) == 0
    assert wind_basis_state(conn, site_id) == "pair_max", (
        "mutant -> at this assertion: correct = 'pair_max' (rescoring reaches"
        " the scoring step and transitions normally), mutant (the early"
        " return after ``_run_rescoring`` dropped) = the job raises"
        " JobDeferred from the fallthrough fetch attempt instead of returning"
    )
    assert get_runtime_state(conn, wind_done_at_key(site_id)) is not None


# ---------------------------------------------------------------------------
# T99 -- switching and rescoring never probe a `probing` hold; the first
# `pair_max` job does.
# ---------------------------------------------------------------------------


def _t99_seed_probing_due_row(
    conn: sqlite3.Connection, station_id: int, today: date
) -> None:
    """Yesterday's row is due via ``history_all``, where the probing hold
    lives; today's row is pre-closed so it never competes for the chunk."""
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, updated_at)"
        " VALUES (?, ?, 'fetched', 0, 24, 24, 0, '2026-01-01T00:00:00Z')",
        (station_id, today.isoformat()),
    )
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count, pair_hours,"
        "  refetched, updated_at)"
        " VALUES (?, ?, 'pending', 0, 0, 0, 0, '2026-01-01T00:00:00Z')",
        (station_id, (today - timedelta(days=1)).isoformat()),
    )
    conn.commit()


def test_t99_switching_does_not_probe_a_probing_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    set_wind_basis_state(conn, site_id, "switching")
    write_auth_hold(
        conn, "history_all", status="probing", since="2026-03-09T00:00:00Z", error="x"
    )
    _t99_seed_probing_due_row(conn, station_id, today)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        raise httpx.ConnectError("refused", request=request)

    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)
    _t97_freeze(monkeypatch, now)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)  # must not raise

    assert len(call_log) == 0, (
        "mutant -> at this assertion: correct = the switching lane never"
        " reaches the probe step (len(call_log) == 0), mutant (the probe run"
        " in every state) = the probing hold's due row is fetched even while"
        " switching"
    )
    hold = read_auth_hold(conn, "history_all")
    assert hold is not None
    assert hold.status == "probing"


def test_t99_rescoring_does_not_probe_a_probing_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    set_wind_basis_state(conn, site_id, "rescoring")
    write_auth_hold(
        conn, "history_all", status="probing", since="2026-03-09T00:00:00Z", error="x"
    )
    _t99_seed_probing_due_row(conn, station_id, today)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        raise httpx.ConnectError("refused", request=request)

    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)
    _t97_freeze(monkeypatch, now)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)  # must not raise

    assert len(call_log) == 0, (
        "mutant -> at this assertion: correct = the rescoring lane never"
        " reaches the probe step (len(call_log) == 0), mutant (the probe run"
        " in every state) = the probing hold's due row is fetched even while"
        " rescoring"
    )
    hold = read_auth_hold(conn, "history_all")
    assert hold is not None
    assert hold.status == "probing"


def test_t99_pair_max_probes_the_held_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive for the two tests above: once the site is in
    ``pair_max`` (the default, untouched state), the same probing hold and
    due row DO get a single probe call -- proving the switching/rescoring
    suppression is a real state check, not a harness artifact."""
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    write_auth_hold(
        conn, "history_all", status="probing", since="2026-03-09T00:00:00Z", error="x"
    )
    _t99_seed_probing_due_row(conn, station_id, today)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)
    _t97_freeze(monkeypatch, now)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    assert len(call_log) == 1, "exactly one probe call per chunk"
    assert read_auth_hold(conn, "history_all") is None, (
        "a successful probe clears the hold"
    )


# ---------------------------------------------------------------------------
# T100 -- a prior job's bad exit (headroom-0 ``JobDeferred``, or the switch
# check itself raising) never gates the next job's fresh switch check.
# ---------------------------------------------------------------------------


def test_t100_a_prior_headroom_deferral_does_not_block_the_next_switch_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.worker.control import JobDeferred

    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    set_wind_basis_state(conn, site_id, "staging")
    # Job 1: yesterday not yet fetched (switch check predicate 2 fails), and
    # a true backfill row (today - 2, non-exempt) is due with headroom 0 --
    # the fallthrough fetch chunk ends in JobDeferred.
    _insert_day(conn, station_id, today - timedelta(days=2), status="pending")
    _insert_day(conn, station_id, today - timedelta(days=1), status="pending")
    conn.execute("UPDATE sources SET daily_call_limit = 0 WHERE source = 'weathercom'")
    conn.commit()

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)
    _t97_freeze(monkeypatch, now)
    with pytest.raises(JobDeferred):
        _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert len(call_log) == 0
    assert wind_basis_state(conn, site_id) == "staging"

    # Job 2: yesterday is now fetched, the backfill row is closed and
    # headroom is restored -- the switch check passes fresh, and the job
    # returns before the fetch chunk runs at all.
    conn.execute(
        "UPDATE station_wind_days SET status = 'fetched', record_count = 24,"
        " pair_hours = 24 WHERE station_id = ? AND local_date IN (?, ?)",
        (
            station_id,
            (today - timedelta(days=2)).isoformat(),
            (today - timedelta(days=1)).isoformat(),
        ),
    )
    conn.execute(
        "UPDATE sources SET daily_call_limit = 1000 WHERE source = 'weathercom'"
    )
    conn.commit()

    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert len(call_log) == 0, (
        "mutant -> at this assertion: correct = the switch check passes and"
        " the job returns before the fetch chunk (0 requests), mutant (the"
        " fetch chunk run before the check, or the check gated on job 1's"
        " JobDeferred exit) = the chunk runs anyway and makes a request"
    )
    assert wind_basis_state(conn, site_id) == "switching"


def test_t100_a_prior_raising_switch_check_does_not_block_the_next_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    set_wind_basis_state(conn, site_id, "staging")
    # Both past rows start final/fetched so job 2's real switch check will
    # pass; job 1's own call is forced to raise instead of evaluating them.
    _insert_day(
        conn,
        station_id,
        today - timedelta(days=2),
        status="fetched",
        record_count=24,
        pair_hours=24,
    )
    _insert_day(
        conn,
        station_id,
        today - timedelta(days=1),
        status="fetched",
        record_count=288,
        pair_hours=24,
    )
    # Yesterday's `pair_hours` literal above does not survive job 1: its
    # fetch chunk persists TODAY's auto-fabricated row, and `_derive_days`
    # pulls in yesterday as a `status='fetched'` neighbour and recomputes
    # its `pair_hours` from the actual `station_wind_records` rows -- with
    # none stored, that recompute silently zeroes the literal back out.
    # Back it with real 5-minute-spaced records so the recompute reproduces
    # 24 paired hours instead of wiping the column.
    yesterday_start = datetime(2026, 3, 9, 0, 0, 0, tzinfo=UTC)
    conn.executemany(
        "INSERT OR IGNORE INTO station_wind_records (station_id, obs_at, speed_kmh)"
        " VALUES (?, ?, ?)",
        [
            (
                station_id,
                isoformat_utc(yesterday_start + timedelta(minutes=5 * i)),
                5.0,
            )
            for i in range(288)
        ],
    )
    conn.commit()

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    now = datetime(2026, 3, 10, 0, 1, 0, tzinfo=UTC)
    _t97_freeze(monkeypatch, now)

    def _raising_switch_check(
        conn: sqlite3.Connection, site_id: int, tz_name: str, free: int | None
    ) -> bool:
        raise ValueError("injected: switch check unreadable")

    monkeypatch.setattr(
        "wxverify.worker.wind_days._switch_check", _raising_switch_check
    )
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)  # caught; falls
    # through to the fetch chunk, which closes today's due live row.
    # mutant -> at this call: correct = the raised ValueError is caught
    # internally and the call above returns normally; mutant (narrowing
    # `except (sqlite3.Error, ValueError)` to `except sqlite3.Error` in
    # wind_days.py) = job 1's own call above raises `ValueError: injected:
    # switch check unreadable`, uncaught -- this is the real divergence
    # point, confirmed by a real mutant run, not the final assertion below.
    assert wind_basis_state(conn, site_id) == "staging"
    monkeypatch.undo()
    _t97_freeze(monkeypatch, now)

    call_log.clear()
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert len(call_log) == 0, (
        "job 2's fresh switch check passes and the job returns before the"
        " fetch chunk (0 requests); under the mutant above, execution never"
        " reaches this line at all, so this assertion is a same-run sanity"
        " check on the non-mutant path, not the mutant's divergence point"
    )
    assert wind_basis_state(conn, site_id) == "switching"


def test_write_blocked_resets_failed_final_rows_once_per_utc_day() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    _insert_day(
        conn, station_id, today - timedelta(days=5), status="failed", attempts=3
    )

    _write_blocked(conn, site_id, today, "test reason")
    row_after_1 = _day_row(conn, station_id, today - timedelta(days=5))
    assert row_after_1["status"] == "pending", (
        "mutant -> at this assertion: correct = 'pending' (blocked resets"
        " failed-final rows), mutant (the reset UPDATE scope/condition"
        " broken) = still 'failed'"
    )
    assert row_after_1["attempts"] == 0

    # Mark it failed-final again and call _write_blocked a second time the
    # same UTC day: the reset must not fire twice.
    conn.execute(
        "UPDATE station_wind_days SET status = 'failed', attempts = 3"
        " WHERE station_id = ? AND local_date = ?",
        (station_id, (today - timedelta(days=5)).isoformat()),
    )
    conn.commit()
    _write_blocked(conn, site_id, today, "test reason")
    row_after_2 = _day_row(conn, station_id, today - timedelta(days=5))
    assert row_after_2["status"] == "failed", (
        "mutant -> at this assertion: correct = 'failed' (the reset already"
        " ran today -- `reset_on` gates a second reset the same UTC day),"
        " mutant (the `reset_on != utc_day` gate dropped) = 'pending' again"
    )


# =============================================================================
# T41 -- switch pass (i): per-day purge + end-of-purge confirm/sweep
# =============================================================================


def test_purge_step_deletes_legacy_rows_day_by_day_with_sweep() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "switching")
    write_wind_cursor(conn, site_id, phase="purge", cursor_date=None)
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw)"
        " VALUES (?, 'wind', '2026-03-01T01:00:00Z', 1.0, 'ok', 'legacy raw')",
        (station_id,),
    )
    conn.commit()
    today = date(2026, 3, 10)

    more = _purge_step(conn, site_id, _TZ, today)

    assert more is True
    left = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    assert left["n"] == 0, (
        "mutant -> at this assertion: correct = 0 (the legacy wind row for"
        " that day is deleted), mutant (the day-window range check broken)"
        " = 1"
    )


def test_end_purge_confirm_raises_on_leftover_row() -> None:
    """A planted leftover row (simulating the confirm step finding an
    unaccounted-for row) raises RuntimeError and the whole transaction rolls
    back -- that is the caller's responsibility in sqlite3's isolation
    model, so here we assert the raise itself and that nothing downstream
    (the cursor write) happened before it."""
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "switching")
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw)"
        " VALUES (?, 'wind', '2026-03-01T01:00:00Z', 1.0, 'ok', 'leftover raw')",
        (station_id,),
    )
    conn.commit()
    today = date(2026, 3, 10)

    with pytest.raises(RuntimeError, match="purge incomplete"):
        _end_purge(conn, site_id, today)

    cursor = read_wind_cursor(conn, site_id)
    assert cursor.phase is None, (
        "mutant -> at this assertion: correct = None (the confirm raise"
        " happens before the cursor is advanced to 'install'), mutant (the"
        " confirm check dropped so _end_purge proceeds past the leftover"
        " row) = 'install'"
    )


def test_end_purge_sweeps_unparseable_rows_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "switching")
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw)"
        " VALUES (?, 'wind', 'not-a-time', 1.0, 'ok', 'stray raw')",
        (station_id,),
    )
    conn.commit()
    today = date(2026, 3, 10)

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.wind_days"):
        _end_purge(conn, site_id, today)

    left = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    assert left["n"] == 0, (
        "mutant -> at this assertion: correct = 0 (unparseable rows are"
        " swept outside the dated range_sql), mutant (the stray sweep"
        " limited to parseable rows only) = 1"
    )
    assert any("unreadable time purged" in rec.message for rec in caplog.records)
    cursor = read_wind_cursor(conn, site_id)
    assert cursor.phase == "install", (
        "mutant -> at this assertion: correct = 'install' (confirm passed,"
        " pass (ii) begins), mutant (phase left at 'purge') = 'purge'"
    )


# =============================================================================
# T42 -- switch pass (ii): install, ascending cursor, midnight carry
# =============================================================================


def test_install_step_writes_pairs_and_advances_cursor() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "switching")
    day = date(2026, 3, 1)
    _insert_day(conn, station_id, day, status="fetched", record_count=2)
    start = datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)
    conn.executemany(
        "INSERT INTO station_wind_records (station_id, obs_at, speed_kmh)"
        " VALUES (?, ?, ?)",
        [
            (
                station_id,
                (start + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
                5.0,
            ),
            (
                station_id,
                (start + timedelta(hours=1, minutes=3))
                .isoformat()
                .replace("+00:00", "Z"),
                9.0,
            ),
        ],
    )
    conn.commit()
    from wxverify.db.wind_basis import WindCursor

    more = _install_step(
        conn,
        site_id,
        _TZ,
        date(2026, 3, 10),
        WindCursor(phase="install", cursor_date=day, date_present=True),
    )

    assert more is True
    obs = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    assert obs["n"] == 1, (
        "mutant -> at this assertion: correct = 1 paired hour written,"
        " mutant (the day's fetched/partial status filter dropped so no"
        " stations qualify) = 0"
    )
    cursor = read_wind_cursor(conn, site_id)
    assert cursor.cursor_date == day + timedelta(days=1), (
        "mutant -> at this assertion: correct = day+1 (cursor advances one"
        " day), mutant (cursor_date left unchanged, an infinite loop) ="
        " unchanged day"
    )


def test_install_step_past_today_transitions_to_rescoring() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "switching")
    write_wind_cursor(conn, site_id, phase="install", cursor_date=date(2026, 3, 11))
    from wxverify.db.wind_basis import WindCursor

    more = _install_step(
        conn,
        site_id,
        _TZ,
        date(2026, 3, 10),
        WindCursor(phase="install", cursor_date=date(2026, 3, 11), date_present=True),
    )

    assert more is False, (
        "mutant -> at this assertion: correct = False (install is done, the"
        " job stops), mutant (`day > today` comparison inverted) = True"
    )
    assert wind_basis_state(conn, site_id) == "rescoring", (
        "mutant -> at this assertion: correct = 'rescoring' (the CAS moved"
        " the site forward past install), mutant (transition call dropped)"
        " = 'switching'"
    )
    assert get_runtime_state(conn, wind_cursor_key(site_id)) is None


# =============================================================================
# T44 -- reconcile a short yesterday
# =============================================================================


def _seed_reconcile_pool(
    conn: sqlite3.Connection, station_id: int, upto: date, counts: list[int]
) -> None:
    for offset, count in enumerate(counts, start=1):
        _insert_day(
            conn,
            station_id,
            upto - timedelta(days=offset),
            status="fetched",
            record_count=count,
        )


def test_reconcile_sets_refetch_when_short_of_the_median() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    yesterday = date(2026, 3, 9)
    _insert_day(conn, station_id, yesterday, status="fetched", record_count=200)
    _seed_reconcile_pool(conn, station_id, yesterday, [280, 280, 280])

    _reconcile(conn, station_id, yesterday, _TZ, record_count=200)

    row = _day_row(conn, station_id, yesterday)
    assert row["refetch_at"] is not None, (
        "mutant -> at this assertion: correct = a refetch_at IS set (200 <"
        " 0.9 * median(280)=252), mutant (the ratio comparison inverted) ="
        " None"
    )
    assert row["refetch_at"] == "2026-03-11T01:00:00Z", (
        "mutant -> at this assertion: correct = start(d+2)+1h"
        " ('2026-03-11T01:00:00Z'), mutant (d+1 instead of d+2) ="
        " '2026-03-10T01:00:00Z'"
    )


def test_reconcile_no_refetch_when_pool_too_small() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    yesterday = date(2026, 3, 9)
    _insert_day(conn, station_id, yesterday, status="fetched", record_count=1)
    _seed_reconcile_pool(conn, station_id, yesterday, [280, 280])  # only 2 < min 3

    _reconcile(conn, station_id, yesterday, _TZ, record_count=1)

    row = _day_row(conn, station_id, yesterday)
    assert row["refetch_at"] is None, (
        "mutant -> at this assertion: correct = None (fewer than 3 prior"
        " fetched days disables reconcile), mutant (_RECONCILE_MIN check"
        " dropped) = a refetch_at timestamp"
    )


def test_reconcile_no_refetch_when_count_meets_the_ratio() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    yesterday = date(2026, 3, 9)
    _insert_day(conn, station_id, yesterday, status="fetched", record_count=260)
    _seed_reconcile_pool(conn, station_id, yesterday, [280, 280, 280])

    _reconcile(conn, station_id, yesterday, _TZ, record_count=260)  # >= 252

    row = _day_row(conn, station_id, yesterday)
    assert row["refetch_at"] is None


# =============================================================================
# T104/T105/T106/T107 -- reconcile refetch correction
# =============================================================================


def test_reconcile_median_pool_excludes_unavailable_rows() -> None:
    """T105: the pool is `fetched` rows only -- an `unavailable` row's
    record_count (always 0) must not pull the median down."""
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    yesterday = date(2026, 3, 9)
    _insert_day(conn, station_id, yesterday, status="fetched", record_count=200)
    for offset, (status, count) in enumerate(
        [
            ("fetched", 280),
            ("unavailable", 0),
            ("fetched", 280),
            ("unavailable", 0),
            ("fetched", 280),
            ("unavailable", 0),
            ("unavailable", 0),
        ],
        start=1,
    ):
        _insert_day(
            conn,
            station_id,
            yesterday - timedelta(days=offset),
            status=status,
            record_count=count,
        )

    _reconcile(conn, station_id, yesterday, _TZ, record_count=200)

    row = _day_row(conn, station_id, yesterday)
    assert row["refetch_at"] is not None, (
        "mutant -> at this assertion: correct = a refetch_at IS set"
        " (median of the 3 `fetched` rows is 280; 200 < 0.9*280=252),"
        " mutant (the `status = 'fetched'` filter dropped so the 4"
        " `unavailable` zeros enter the pool, median collapses to 0,"
        " and 200 >= 0) = None"
    )


def test_persist_day_refetch_correction_in_pair_max_enqueues() -> None:
    """T106: a refetch of an `unavailable` row that now returns records
    becomes `fetched` and enqueues in `pair_max`; the refetched flag is set
    so `_reconcile` never re-fires on this row."""
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    local_date = date(2026, 3, 5)
    set_wind_basis_state(conn, site_id, "pair_max")
    _insert_day(
        conn,
        station_id,
        local_date,
        status="unavailable",
        record_count=0,
        refetch_at="2026-03-06T01:00:00Z",
        refetched=0,
    )

    start = datetime(2026, 3, 5, 0, 0, 0, tzinfo=UTC)
    records = [
        WindRecord(obs_at=start + timedelta(hours=1), speed_kmh=5.0),
        WindRecord(obs_at=start + timedelta(hours=1, minutes=3), speed_kmh=9.0),
    ]
    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=_TZ,
        today=date(2026, 3, 10),
        records=records,
        probe=None,
    )

    row = _day_row(conn, station_id, local_date)
    assert row["status"] == "fetched", (
        "mutant -> at this assertion: correct = 'fetched' (a successful"
        " refetch with records lands fetched, same as any dated row with"
        " record_count > 0), mutant (`_is_refetch` short-circuits status to"
        " something else) = a status other than 'fetched'"
    )
    assert row["refetched"] == 1, (
        "mutant -> at this assertion: correct = 1 (the refetch flag is set"
        " so reconcile never re-schedules this row), mutant (`refetched ="
        " CASE WHEN ? THEN 1 ELSE refetched END` dropped) = 0"
    )
    jobs = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type = 'pair_and_score'"
    ).fetchone()
    assert jobs["n"] == 1


def test_persist_day_refetch_correction_in_staging_no_station_row_no_enqueue() -> None:
    """T106 (staging branch): same refetch correction but in `staging` --
    counts update, no station row, no enqueue."""
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    local_date = date(2026, 3, 5)
    set_wind_basis_state(conn, site_id, "staging")
    _insert_day(
        conn,
        station_id,
        local_date,
        status="unavailable",
        record_count=0,
        refetch_at="2026-03-06T01:00:00Z",
        refetched=0,
    )

    start = datetime(2026, 3, 5, 0, 0, 0, tzinfo=UTC)
    records = [WindRecord(obs_at=start + timedelta(hours=1), speed_kmh=5.0)]
    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=_TZ,
        today=date(2026, 3, 10),
        records=records,
        probe=None,
    )

    row = _day_row(conn, station_id, local_date)
    assert row["status"] == "fetched"
    assert row["record_count"] == 1
    obs = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    assert obs["n"] == 0, (
        "mutant -> at this assertion: correct = 0 (staging never writes a"
        " station row even on a refetch correction), mutant (the refetch"
        " path bypasses the pair_max gate) = 1"
    )
    jobs = conn.execute("SELECT COUNT(*) AS n FROM jobs").fetchone()
    assert jobs["n"] == 0


def test_persist_day_refetch_partial_response_keeps_stored_record_count() -> None:
    """T107: a refetch response holding fewer records than already stored
    leaves ``record_count`` at the larger, already-stored figure -- it is
    derived from the table, never from the response length."""
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    local_date = date(2026, 3, 5)
    set_wind_basis_state(conn, site_id, "staging")
    _insert_day(
        conn,
        station_id,
        local_date,
        status="unavailable",
        record_count=280,
        refetch_at="2026-03-06T01:00:00Z",
        refetched=0,
    )
    start = datetime(2026, 3, 5, 0, 0, 0, tzinfo=UTC)
    # Pre-seed 280 records already stored from the ORIGINAL fetch.
    conn.executemany(
        "INSERT OR IGNORE INTO station_wind_records (station_id, obs_at, speed_kmh)"
        " VALUES (?, ?, ?)",
        [
            (
                station_id,
                (start + timedelta(minutes=5 * i)).isoformat().replace("+00:00", "Z"),
                5.0,
            )
            for i in range(280)
        ],
    )
    conn.commit()
    # The refetch response holds only 250 of those same records.
    refetch_records = [
        WindRecord(obs_at=start + timedelta(minutes=5 * i), speed_kmh=5.0)
        for i in range(250)
    ]

    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=_TZ,
        today=date(2026, 3, 10),
        records=refetch_records,
        probe=None,
    )

    row = _day_row(conn, station_id, local_date)
    assert row["record_count"] == 280, (
        "mutant -> at this assertion: correct = 280 (record_count re-derives"
        " from the stored table via COUNT(*), unaffected by how many the"
        " response happened to carry), mutant (record_count set to"
        " len(records) from the response) = 250"
    )


# =============================================================================
# T45 -- dispatch routes ("fetch_obs","wind-days") to the lane,
# ("fetch_obs","obs") to _fetch_obs
# =============================================================================


def test_dispatch_routes_wind_days_job_key_to_the_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import wxverify.worker.processor as processor_module

    called: list[str] = []

    async def fake_run_wind_days(db: object, writer: object, site_id: int) -> None:
        called.append("wind_days")

    async def fake_fetch_obs(db: object, writer: object, site_id: int) -> None:
        called.append("obs")

    monkeypatch.setattr(processor_module, "run_wind_days", fake_run_wind_days)
    monkeypatch.setattr(processor_module, "_fetch_obs", fake_fetch_obs)

    job = Job(
        id=1,
        type="fetch_obs",
        site_id=1,
        job_key=WIND_DAYS_JOB_KEY,
        payload={},
        status="running",
        retry_count=0,
        max_retries=5,
    )
    asyncio.run(dispatch(None, None, job))  # type: ignore[arg-type]

    assert called == ["wind_days"], (
        "mutant -> at this assertion: correct = ['wind_days'] (job_key =="
        " WIND_DAYS_JOB_KEY routes to run_wind_days), mutant (the job_key"
        " branch check inverted or dropped) = ['obs']"
    )


def test_dispatch_routes_obs_job_key_to_fetch_obs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import wxverify.worker.processor as processor_module

    called: list[str] = []

    async def fake_run_wind_days(db: object, writer: object, site_id: int) -> None:
        called.append("wind_days")

    async def fake_fetch_obs(db: object, writer: object, site_id: int) -> None:
        called.append("obs")

    monkeypatch.setattr(processor_module, "run_wind_days", fake_run_wind_days)
    monkeypatch.setattr(processor_module, "_fetch_obs", fake_fetch_obs)

    job = Job(
        id=2,
        type="fetch_obs",
        site_id=1,
        job_key="obs",
        payload={},
        status="running",
        retry_count=0,
        max_retries=5,
    )
    asyncio.run(dispatch(None, None, job))  # type: ignore[arg-type]

    assert called == ["obs"], (
        "mutant -> at this assertion: correct = ['obs'] (job_key 'obs'"
        " never reaches run_wind_days), mutant (the WIND_DAYS_JOB_KEY check"
        " always true) = ['wind_days']"
    )


# =============================================================================
# T46 -- headroom wake (pure function only; chunk-level exhaustion deferred)
# =============================================================================


def test_headroom_wake_is_min_of_hour_billing_window_and_earliest_exempt() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    today = date(2026, 3, 10)
    # An exempt (yesterday) row whose next_attempt_at is sooner than now+1h.
    soon = now + timedelta(minutes=10)
    _insert_day(
        conn,
        station_id,
        today - timedelta(days=1),
        status="pending",
        next_attempt_at=soon.isoformat().replace("+00:00", "Z"),
    )

    wake = _headroom_wake(
        conn,
        site_id,
        today=today,
        now=now,
        opened=frozenset({"history_all", "all_1day"}),
    )

    assert wake == soon, (
        "mutant -> at this assertion: correct = the earliest exempt row's"
        " next_attempt_at (10 minutes out, sooner than now+1h), mutant (the"
        " exempt-row candidate loop dropped) = now+1h"
    )


def test_headroom_wake_ignores_rows_behind_a_closed_endpoint() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    today = date(2026, 3, 10)
    soon = now + timedelta(minutes=10)
    # Today's row uses all_1day, which is NOT in `opened` here.
    _insert_day(
        conn,
        station_id,
        today,
        status="partial",
        next_attempt_at=soon.isoformat().replace("+00:00", "Z"),
    )

    wake = _headroom_wake(
        conn, site_id, today=today, now=now, opened=frozenset({"history_all"})
    )

    assert wake == now + timedelta(hours=1), (
        "mutant -> at this assertion: correct = now+1h (the closed"
        " endpoint's row is excluded, so the only candidate left is the"
        " hour fallback), mutant (the `endpoint_for(...) not in opened`"
        " filter dropped) = the 10-minute-out candidate"
    )


# =============================================================================
# T47 -- scheduler enqueues the wind lane
# =============================================================================


def test_enqueue_due_wind_days_enqueues_for_a_site_with_an_enabled_station() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "staging")
    # staging + no pair-max rows + nothing due + today's row already present
    # would make wind_lane_due False; to make it due, leave today's row
    # missing (ensure_wind_days has not run yet in this bare fixture).

    _enqueue_due_wind_days(conn)

    jobs = conn.execute("SELECT job_key FROM jobs WHERE type = 'fetch_obs'").fetchall()
    assert [r["job_key"] for r in jobs] == [WIND_DAYS_JOB_KEY], (
        "mutant -> at this assertion: correct = one fetch_obs/wind-days job"
        " enqueued (a site with an enabled station and a missing today's"
        " row is due), mutant (the due check inverted) = no job"
    )


def test_enqueue_due_wind_days_skips_a_site_with_no_enabled_station() -> None:
    """Pins the no-enabled-station outcome. NOT a kill claim against the
    site-selection EXISTS clause alone: dropping `st.enabled=1` from that
    EXISTS is equivalent under this fixture, because `_missing_today_row`
    and `WIND_DUE_SQL` both independently re-filter on `st.enabled = 1`
    downstream -- with zero enabled stations, `wind_lane_due` still returns
    False either way (verified empirically: that one-line mutant leaves
    this assertion green). A real kill needs a second, OTHER site with an
    enabled station in the same run and an assertion on which site_id the
    job was enqueued for -- deferred; this test still pins the correct
    all-disabled behavior as a regression guard."""
    conn = _make_db()
    site_id = _make_site(conn)
    _make_station(conn, site_id, "WSTATION01", enabled=False)

    _enqueue_due_wind_days(conn)

    jobs = conn.execute("SELECT COUNT(*) AS n FROM jobs").fetchone()
    assert jobs["n"] == 0


def test_enqueue_due_wind_days_excludes_a_disabled_only_site_even_when_due() -> None:
    """The real (non-equivalent) kill of the site-selection EXISTS clause the
    test above's docstring deferred: a second site whose only station is
    disabled, but whose wind-basis state is forced outside ``STATES``.
    ``wind_lane_due``'s own first branch (``state not in STATES -> True``)
    would call that site due unconditionally, with no reference to station
    enablement at all -- so the ONLY thing standing between it and an
    enqueued job is the outer site-selection ``EXISTS (... st.enabled=1)``
    clause itself. A disabled-only site with a normal state (the test above)
    never reaches that shortcut and is equivalent; corrupting the state
    closes that gap and makes the kill real.
    """
    conn = _make_db()
    due_site_id = _make_site(conn, "Due Site")
    _make_station(conn, due_site_id, "WSTATION01")
    set_wind_basis_state(conn, due_site_id, "staging")

    disabled_site_id = _make_site(conn, "Disabled Station Site")
    _make_station(conn, disabled_site_id, "WSTATION02", enabled=False)
    # set_wind_basis_state rejects an unknown state; write it directly to
    # force wind_lane_due's unconditional-True branch regardless of any
    # station's enabled flag.
    set_runtime_state(conn, wind_basis_key(disabled_site_id), "corrupted-state")

    _enqueue_due_wind_days(conn)

    site_ids = {
        int(r["site_id"])
        for r in conn.execute(
            "SELECT site_id FROM jobs WHERE type = 'fetch_obs' AND job_key = ?",
            (WIND_DAYS_JOB_KEY,),
        )
    }
    assert site_ids == {due_site_id}, (
        "mutant -> at this assertion: correct = only the enabled-station"
        " site's job (the disabled-station-only site is excluded before"
        " wind_lane_due ever runs), mutant (st.enabled=1 dropped from the"
        " site-selection EXISTS clause) = both sites' jobs, because the"
        " disabled-station site's corrupted state makes wind_lane_due"
        " return True unconditionally once it is no longer excluded"
        " upstream"
    )


def test_enqueue_due_wind_days_respects_the_success_cooldown_spacing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_enqueue_due_wind_days`` (scheduler.py:397): a just-completed lane
    job spaces the next enqueue by ``WIND_DAYS_SPACING`` (5 min), so a lane
    that is due on every tick (today's row missing here) does not get a
    fresh job every tick. 1 minute after completion is paired against 6
    minutes after -- both inside the SAME frozen ``now``, isolating the
    cooldown boundary itself rather than any other due condition."""
    conn = _make_db()
    site_id = _make_site(conn)
    _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "staging")
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze_scheduler_now(monkeypatch, now)

    one_min_ago = (now - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
    _insert_job(
        conn,
        "fetch_obs",
        site_id,
        WIND_DAYS_JOB_KEY,
        status="completed",
        updated_at=one_min_ago,
    )

    _enqueue_due_wind_days(conn)

    pending = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs"
        " WHERE type = 'fetch_obs' AND job_key = ? AND status = 'pending'",
        (WIND_DAYS_JOB_KEY,),
    ).fetchone()
    assert pending["n"] == 0, (
        "mutant -> at this assertion: correct = 0 (1 minute inside the"
        " 5-minute success cooldown suppresses the enqueue), mutant"
        " (`success_cooldown` dropped/passed as None unconditionally) = 1"
    )

    six_min_ago = (now - timedelta(minutes=6)).isoformat().replace("+00:00", "Z")
    conn.execute(
        "UPDATE jobs SET updated_at = ? WHERE type = 'fetch_obs' AND job_key = ?",
        (six_min_ago, WIND_DAYS_JOB_KEY),
    )
    conn.commit()

    _enqueue_due_wind_days(conn)

    pending_after = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs"
        " WHERE type = 'fetch_obs' AND job_key = ? AND status = 'pending'",
        (WIND_DAYS_JOB_KEY,),
    ).fetchone()
    assert pending_after["n"] == 1, (
        "mutant -> at this assertion: correct = 1 (6 minutes is outside the"
        " 5-minute cooldown, so the due lane enqueues), mutant"
        " (`WIND_DAYS_SPACING` widened past 6 minutes, or the cooldown"
        " check inverted to always suppress) = 0"
    )


def test_enqueue_due_wind_days_fails_open_on_an_unreadable_success_stamp(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """scheduler.py:401-410: the generic ``enqueue_if_absent_with_cooldown``
    wrapper fails CLOSED (suppresses) on an unreadable ``updated_at`` for a
    success cooldown -- the module docstring for ``_enqueue_due_wind_days``
    says that would "stop the lane for good," so this call site pre-checks
    the latest completed job itself and passes ``success_cooldown=None``
    (fail OPEN) when the stamp doesn't parse, logging its own WARNING
    distinct from the wrapper's."""
    conn = _make_db()
    site_id = _make_site(conn)
    _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "staging")
    _insert_job(
        conn,
        "fetch_obs",
        site_id,
        WIND_DAYS_JOB_KEY,
        status="completed",
        updated_at="garbage",
    )

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.scheduler"):
        _enqueue_due_wind_days(conn)

    pending = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs"
        " WHERE type = 'fetch_obs' AND job_key = ? AND status = 'pending'",
        (WIND_DAYS_JOB_KEY,),
    ).fetchone()
    assert pending["n"] == 1, (
        "mutant -> at this assertion: correct = 1 (the wind-lane-specific"
        " pre-check overrides the cooldown to None on an unreadable stamp,"
        " so the due lane still enqueues), mutant (the pre-check/override"
        " dropped, leaving the wrapper's own fail-closed default) = 0"
    )
    assert any(
        "unreadable updated_at on the latest wind lane job" in rec.message
        for rec in caplog.records
    ), (
        "mutant -> at this assertion: correct = the wind-lane-specific"
        " WARNING text is present, mutant (the pre-check removed, falling"
        " through to the wrapper's own differently-worded warning or none"
        " at all) = text absent"
    )


def test_enqueue_due_wind_days_skips_a_site_with_an_unusable_time_zone(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """scheduler.py:386-394: a site whose stored ``timezone`` fails
    ``ZoneInfo(...)`` is skipped before ``wind_lane_due`` is ever called,
    with a WARNING -- paired against two ordinary-timezone sites (good1,
    good2) so this is a real divergence on the zone itself, not on any
    other due condition, and so a mutant that stops the skip loop early
    (``continue`` -> ``break``) is distinguishable from one that only
    narrows which bad zones get caught: good2 sorts after BOTH bad sites,
    so it is enqueued only if the loop actually continues past each of
    them rather than aborting on the first.

    Two distinct bad-zone shapes are seeded on purpose:
    ``zoneinfo.ZoneInfoNotFoundError`` ("Not/AZone", a well-formed key that
    is simply not in the tz database) and ``ValueError`` ("../etc/zone", a
    path-traversal-shaped key that ``ZoneInfo`` rejects outright --
    empirically confirmed to raise ``ValueError`` on this interpreter, not
    ``ZoneInfoNotFoundError``). A narrowed ``except ZoneInfoNotFoundError``
    (dropping the ``ValueError`` arm) lets the second bad site's exception
    escape uncaught, which this test also catches."""
    conn = _make_db()
    good1_id = _make_site(conn, name="Testsite-good1", tz="UTC")
    _make_station(conn, good1_id, "KTEST001")
    set_wind_basis_state(conn, good1_id, "staging")

    bad_zinfe_id = _make_site(conn, name="Testsite-bad-zinfe", tz="Not/AZone")
    _make_station(conn, bad_zinfe_id, "KTEST002")
    set_wind_basis_state(conn, bad_zinfe_id, "staging")

    bad_value_id = _make_site(conn, name="Testsite-bad-value", tz="../etc/zone")
    _make_station(conn, bad_value_id, "KTEST003")
    set_wind_basis_state(conn, bad_value_id, "staging")

    good2_id = _make_site(conn, name="Testsite-good2", tz="UTC")
    _make_station(conn, good2_id, "KTEST004")
    set_wind_basis_state(conn, good2_id, "staging")

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.scheduler"):
        # mutant -> at this call: correct = returns normally (both bad
        # zones are caught and skipped), mutant (the except clause
        # narrowed to `ZoneInfoNotFoundError` only) = raises `ValueError`
        # uncaught on the bad_value_id site
        _enqueue_due_wind_days(conn)

    rows = conn.execute("SELECT site_id FROM jobs WHERE type = 'fetch_obs'").fetchall()
    enqueued_site_ids = {int(row["site_id"]) for row in rows}
    assert enqueued_site_ids == {good1_id, good2_id}, (
        "mutant -> at this assertion: correct = {good1_id, good2_id} (both"
        " bad zones are skipped and both good sites are enqueued), mutant"
        " (the `continue` after the warning changed to `break`) ="
        " {good1_id} only -- the loop exits after the first bad zone and"
        " never reaches good2_id, which a single-bad-site fixture cannot"
        " distinguish from the correct behavior"
    )
    warning_messages = [rec.message for rec in caplog.records]
    assert sum("unusable time zone" in msg for msg in warning_messages) == 2, (
        "mutant -> at this assertion: correct = 2 (one WARNING per bad"
        " zone), mutant (the warning call dropped from the except branch)"
        " = fewer than 2"
    )


def test_wind_lane_due_true_for_switching_rescoring_and_an_unknown_state() -> None:
    """wind_lane_due's unconditional-True branch (wind_days.py:375): any
    state outside ``STATES``, or either of the two transitional states
    themselves, is due regardless of any other condition. Paired against a
    quiescent ``pair_max`` site (today's row already fetched, no pair-max
    observations) that is NOT due on its own, so each state below is a real
    divergence rather than "everything in this fixture is due.\""""
    from zoneinfo import ZoneInfo

    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    tz = ZoneInfo(_TZ)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    today = now.date()
    _insert_day(conn, station_id, today, status="fetched", record_count=1)
    set_wind_basis_state(conn, site_id, "pair_max")

    assert wind_lane_due(conn, site_id, tz, now=now) is False, (
        "setup check: a quiescent pair_max site with today's row already"
        " fetched and no pair-max observations must not be due, or the"
        " states below prove nothing"
    )

    for state in ("switching", "rescoring"):
        set_runtime_state(conn, wind_basis_key(site_id), state)
        assert wind_lane_due(conn, site_id, tz, now=now) is True, (
            f"mutant -> at this assertion (state={state}): correct = True"
            " (switching/rescoring are unconditionally due), mutant (the"
            ' `state in ("switching", "rescoring")` clause dropped) = False,'
            " falling through to the same quiescent checks that returned"
            " False above"
        )

    set_runtime_state(conn, wind_basis_key(site_id), "corrupted-state")
    assert wind_lane_due(conn, site_id, tz, now=now) is True, (
        "mutant -> at this assertion: correct = True (an unknown state"
        " outside STATES is unconditionally due), mutant (`state not in"
        " STATES` dropped) = False"
    )


def test_wind_lane_due_true_for_staging_with_pair_max_rows_present() -> None:
    """wind_lane_due's staging+pair-max-rows branch (wind_days.py:377):
    staging normally defers to the ordinary due checks, but a pair-max
    observation already present (installed ahead of the staging rows
    themselves) makes the lane due immediately. Paired against the same
    staging site with no pair-max rows and today's row already fetched, so
    this is a real divergence on the pair-max-rows clause itself."""
    from zoneinfo import ZoneInfo

    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    tz = ZoneInfo(_TZ)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    today = now.date()
    _insert_day(conn, station_id, today, status="fetched", record_count=1)
    set_wind_basis_state(conn, site_id, "staging")

    assert wind_lane_due(conn, site_id, tz, now=now) is False, (
        "setup check: staging with today's row fetched and no pair-max"
        " observations must not be due on its own, or the pair-max rows"
        " below prove nothing"
    )

    _insert_pair_max_obs(conn, station_id, "2026-03-09T12:00:00Z")

    assert wind_lane_due(conn, site_id, tz, now=now) is True, (
        "mutant -> at this assertion: correct = True (staging with a"
        " pair-max observation present is due), mutant (the"
        ' `state == "staging" and _has_pair_max_rows(...)` clause dropped)'
        " = False"
    )


def test_wind_days_lane_quiescence_leaves_a_claim_window_for_verification_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plan rationale ('Why not JobContinuation'): a wind-days lane with a
    job always pending would starve the tier-2 ``verification_run`` lane,
    because ``claim_next_job``'s CASE orders ``verification_run``/
    ``timezone_correction`` strictly after every other pending type
    (wind_days.py's ``WIND_DAYS_SPACING`` is what keeps a wind-days job
    from always being pending -- see the spacing test above). This pins
    both halves of that rationale in one place, DRIVING the real scheduler
    (``_enqueue_due_wind_days``) rather than hand-inserting the wind job,
    so the lane's own ``WIND_DAYS_SPACING`` cooldown is what keeps it
    quiescent, not just an absent row by construction: with the lane
    quiescent (a completed job 1 minute inside its 5-minute cooldown, so no
    pending wind-days job), a pending ``verification_run`` job IS the next
    claim; the paired positive re-stamps the completed job 6 minutes ago
    (outside the cooldown), drives the scheduler again, and shows the
    resulting pending wind-days job claims FIRST over a re-pended
    ``verification_run``, which is exactly the tier ordering that would
    starve it if a wind-days job were always pending.

    mutant -> at the step-3 pending-count assertion: correct = 0 (1 minute
    is inside WIND_DAYS_SPACING, so the scheduler does not enqueue), mutant
    (the lane's spacing/``success_cooldown`` argument dropped) = 1 -- the
    scheduler enqueues every tick regardless of the just-completed job,
    which is the real-clock case this test's own docstring must describe
    correctly (the earlier hand-inserted-job version of this test used an
    unfrozen wall clock and a bare ``pytest.raises``-free fixture, so its
    'inside its cooldown' claim never actually ran the scheduler)."""
    conn = _make_db()
    site_id = _make_site(conn)
    _make_station(conn, site_id, "WSTATION01")
    set_wind_basis_state(conn, site_id, "staging")
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    _freeze_scheduler_now(monkeypatch, now)

    one_min_ago = (now - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
    _insert_job(
        conn,
        "fetch_obs",
        site_id,
        WIND_DAYS_JOB_KEY,
        status="completed",
        updated_at=one_min_ago,
    )
    _insert_job(conn, "verification_run", site_id, "verification", status="pending")

    _enqueue_due_wind_days(conn)

    pending = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs"
        " WHERE type = 'fetch_obs' AND job_key = ? AND status = 'pending'",
        (WIND_DAYS_JOB_KEY,),
    ).fetchone()
    assert pending["n"] == 0, (
        "mutant -> at this assertion: correct = 0 (1 minute inside"
        " WIND_DAYS_SPACING suppresses the enqueue, leaving the lane"
        " quiescent), mutant (the spacing argument dropped from the"
        " scheduler's enqueue call) = 1"
    )

    claimed_quiescent = claim_next_job(conn)
    assert (
        claimed_quiescent is not None and claimed_quiescent.type == "verification_run"
    ), (
        "mutant -> at this assertion: correct = the verification_run job"
        " (it is the only pending row), mutant (MAIN_LANE_TYPE_SQL excludes"
        " verification_run from the main-lane claim) = None"
    )

    six_min_ago = (now - timedelta(minutes=6)).isoformat().replace("+00:00", "Z")
    conn.execute(
        "UPDATE jobs SET updated_at = ? WHERE type = 'fetch_obs' AND job_key = ?",
        (six_min_ago, WIND_DAYS_JOB_KEY),
    )
    conn.execute("UPDATE jobs SET status = 'pending' WHERE type = 'verification_run'")
    conn.commit()

    _enqueue_due_wind_days(conn)

    pending_after = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs"
        " WHERE type = 'fetch_obs' AND job_key = ? AND status = 'pending'",
        (WIND_DAYS_JOB_KEY,),
    ).fetchone()
    assert pending_after["n"] == 1, (
        "mutant -> at this assertion: correct = 1 (6 minutes is outside"
        " WIND_DAYS_SPACING, so the now-due lane enqueues), mutant"
        " (WIND_DAYS_SPACING widened past 6 minutes) = 0"
    )

    claimed_busy = claim_next_job(conn)
    assert claimed_busy is not None and claimed_busy.type == "fetch_obs", (
        "mutant -> at this assertion: correct = the fetch_obs (wind-days)"
        " job claims first (tier 1 beats tier 2 in the claim CASE), mutant"
        " (verification_run's CASE tier changed to rank ahead of or equal"
        " to the default tier) = the verification_run job claimed instead"
        " -- exactly the ordering that would starve it if a wind-days job"
        " were always pending"
    )


# =============================================================================
# T48 -- progress stamp
# =============================================================================


def test_progress_stamp_moves_on_pre_yesterday_status_change_only() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    older = today - timedelta(days=5)
    _insert_day(conn, station_id, older, status="pending", attempts=0)
    before = get_runtime_state(conn, wind_progress_key(site_id))

    # A status change on a date before yesterday (unavailable, since the
    # record_count is 0) must stamp progress.
    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=older,
        tz_name=_TZ,
        today=today,
        records=[],
        probe=None,
    )
    after = get_runtime_state(conn, wind_progress_key(site_id))
    assert after is not None and after != before, (
        "mutant -> at this assertion: correct = the progress stamp moves"
        " (status changed pending -> unavailable, date before yesterday),"
        " mutant (the `local_date < yesterday` guard dropped so a today-"
        "window change is required too) = the stamp stays unset/unchanged"
    )


def test_progress_stamp_does_not_move_when_today_row_only_refreshes() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    _insert_day(conn, station_id, today, status="partial", attempts=0)
    before = get_runtime_state(conn, wind_progress_key(site_id))

    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=today,
        tz_name=_TZ,
        today=today,
        records=[],
        probe=None,
    )

    after = get_runtime_state(conn, wind_progress_key(site_id))
    assert after == before, (
        "mutant -> at this assertion: correct = unchanged (today's row"
        " staying 'partial' is not a status change before yesterday),"
        " mutant (stamp_wind_progress called unconditionally on every"
        " persist) = a new stamp value"
    )


def test_progress_stamp_moves_on_the_switch_pass_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_switch_check``'s own unconditional stamp (wind_days.py, right after
    the compare-and-set to 'switching') -- distinct from every other stamp
    site in this file, none of which drive a pass through here.

    Seeds the state directly via ``set_runtime_state`` rather than
    ``_seed_switchable_site``'s usual ``set_wind_basis_state``: the latter
    stamps progress itself, and that setup stamp and the switch's own stamp
    both come from SQLite's own ``strftime('...','now')`` on one connection
    -- a real sub-millisecond clock with no Python-side seam to freeze --
    so a before/after value diff can collide. Keeping ``before`` at its
    true unset baseline (``None``) avoids relying on clock resolution.
    """
    _freeze_wind_days_today(monkeypatch, datetime(2026, 3, 10, 12, tzinfo=UTC))
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    set_runtime_state(conn, wind_basis_key(site_id), "staging")
    today = date(2026, 3, 10)
    _insert_day(
        conn,
        station_id,
        today - timedelta(days=1),
        status="fetched",
        record_count=5,
        pair_hours=MIN_OBS_HOURS,
    )
    before = get_runtime_state(conn, wind_progress_key(site_id))
    assert before is None, "fixture bug: expected no progress stamp before the switch"

    switched = _switch_check(conn, site_id, _TZ, _BIG_FREE)

    assert switched is True
    after = get_runtime_state(conn, wind_progress_key(site_id))
    assert after is not None and after != before, (
        "mutant -> at this assertion: correct = the progress stamp moves on"
        " a successful switch pass, mutant (BOTH the CAS's own"
        " `transition_wind_basis` success-path stamp AND `_switch_check`'s"
        " own `stamp_wind_progress` call after the CAS dropped) = the stamp"
        " stays unset/unchanged. Dropping only `_switch_check`'s own call"
        " does NOT move this assertion: `transition_wind_basis` already"
        " stamps progress on a successful compare-and-set, so that single"
        " mutant survives this test and is not claimed here."
    )


def test_progress_stamp_moves_on_a_blocked_reset_state_change() -> None:
    """``_write_blocked``'s own conditional stamp: when the once-per-UTC-day
    failed-final reset actually flips a pre-yesterday row back to pending,
    progress must move -- a state change distinct from ``_persist_day``'s."""
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    _insert_day(
        conn, station_id, today - timedelta(days=5), status="failed", attempts=3
    )
    before = get_runtime_state(conn, wind_progress_key(site_id))

    _write_blocked(conn, site_id, today, "test reason")

    row = _day_row(conn, station_id, today - timedelta(days=5))
    assert row["status"] == "pending"
    after = get_runtime_state(conn, wind_progress_key(site_id))
    assert after is not None and after != before, (
        "mutant -> at this assertion: correct = the progress stamp moves"
        " when the failed-final reset actually resets a pre-yesterday row,"
        " mutant (the `stamp_wind_progress` call inside the reset-fired"
        " branch dropped) = the stamp stays unset/unchanged"
    )


def test_progress_stamp_does_not_move_on_a_chunk_that_only_fetched_yesterday() -> None:
    """``_persist_day``'s guard is ``local_date < yesterday`` (strictly
    pre-yesterday) -- yesterday itself is excluded even though its status
    changes pending -> fetched. Distinct from the existing today-only and
    five-days-back cases: this is the boundary the guard must still reject."""
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    today = date(2026, 3, 10)
    yesterday = today - timedelta(days=1)
    _insert_day(conn, station_id, yesterday, status="pending", attempts=0)
    before = get_runtime_state(conn, wind_progress_key(site_id))

    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=yesterday,
        tz_name=_TZ,
        today=today,
        records=[
            WindRecord(obs_at=datetime(2026, 3, 9, 12, 0, 0, tzinfo=UTC), speed_kmh=5.0)
        ],
        probe=None,
    )

    row = _day_row(conn, station_id, yesterday)
    assert row["status"] == "fetched", "fixture bug: expected a status change"
    after = get_runtime_state(conn, wind_progress_key(site_id))
    assert after == before, (
        "mutant -> at this assertion: correct = unchanged (yesterday fetching"
        " is excluded by `local_date < yesterday`, even though its status"
        " changed pending -> fetched), mutant (the guard loosened to"
        " `<=`, stamping on yesterday-only chunks too) = a new stamp value"
    )
