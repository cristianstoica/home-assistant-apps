"""Tests: the wind-history lane, group 1 of plan section 15.1 (2026-09-30).

Covers T8, T30, T33, T34, T36, T44, T45, T47, T104-T107 against
``wxverify/worker/wind_days.py`` (selector, persist, scheduler integration)
using a real sqlite3 ``:memory:`` connection + ``run_migrations`` for the
sync surface, and a tmp-path ``Database``/``FencedWriter`` for the handful
of cases that go through an async write path (T45, T47 via dispatch/scheduler).

Also covers T35 (the ``_fetch_failed`` >=500/payload-fault/refundable-
transport-error dispatch -- in ``tests/test_wind_auth_hold.py``, alongside
its existing 401/403/429 coverage), T46's chunk-level
headroom-exhaustion integration (``_run_fetch_chunk`` raising
``WindHeadroomExhausted`` through a live chunk loop -- in
``tests/test_wind_quota.py``, reusing its headroom-0 fixture), T97 (a
station-day stuck on repeated 500s going final after 3 attempts across 4
real jobs, driven through ``run_wind_days`` with a tmp-path DB +
``httpx.MockTransport``, mirroring ``tests/test_wind_auth_hold.py``'s async
harness), and T99 (a ``pair_max`` job probing a ``probing`` auth hold once
and clearing it on success). T99 reuses T97's async dispatch +
``httpx.MockTransport`` harness
(``_t97_init_tmp_db``/``_t97_freeze``/``_t97_run_wind_days``).

Every station id, site name and coordinate below is synthetic (public repo):
pws station ids ``WSTATION01``/``WSTATION02``, site name "Testsite",
timezone "UTC" or "Etc/UTC", coordinates 0.0/0.0.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from wxverify import config
from wxverify.core.timeutil import isoformat_utc, local_day_slots
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.migrations import run_migrations, seed_default_sources
from wxverify.db.queue import Job, claim_next_job
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.wind_basis import WIND_FINAL_LEAD, read_auth_hold, write_auth_hold
from wxverify.forecast.wind_blend import TRAINING_DAYS
from wxverify.monitor import (  # noqa: PLC2701 -- private, exercised directly
    _wind_history_trips,
)
from wxverify.obs.wind_pairs import WindRecord
from wxverify.verification.truth import materialize_daily_truth
from wxverify.worker.control import JobDeferred
from wxverify.worker.processor import dispatch
from wxverify.worker.scheduler import (
    _enqueue_due_wind_days,  # noqa: PLC2701 -- private, exercised directly (T47)
)
from wxverify.worker.wind_days import (  # noqa: PLC2701 -- private, exercised directly
    _FINAL_RETRY_LEAD,
    _FINISHABLE_SQL,
    WIND_DAYS_JOB_KEY,
    _derive_days,
    _finish_day,
    _finish_floor,
    _headroom_wake,
    _persist_day,
    _reconcile,
    _today_next_attempt,
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
    conn = _make_db()
    end = datetime(2026, 3, 11, 0, 0, 0, tzinfo=UTC)

    normal = _today_next_attempt(conn, end - timedelta(hours=5), end, failed=False)
    assert normal < end.isoformat().replace("+00:00", "Z"), (
        "mutant -> at this assertion: correct = now+interval (well before"
        " midnight), mutant (always rolls to end+1h) = end+1h regardless"
    )

    near_midnight = _today_next_attempt(
        conn, end - timedelta(minutes=5), end, failed=False
    )
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
        " mutant (the row-write loop skipped so nothing is written) = 0"
    )
    assert str(obs[0]["source_raw"]).startswith("pair-max ")
    jobs = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type = 'pair_and_score'"
    ).fetchone()
    assert jobs["n"] == 1, (
        "mutant -> at this assertion: correct = 1 (changed hours enqueue"
        " pair_and_score), mutant (the enqueue under `if changed` dropped)"
        " = 0"
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
# T143 -- record reads in a non-whole-hour offset zone (plan 15.1)
# =============================================================================


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

    # -- last-derived-day leg: `pair_max`, D `fetched`, D+1 neither
    # `fetched` nor `partial`. ------------------------------------------
    conn2 = _make_db()
    site_id2 = _make_site(conn2, tz=tz)
    station_id2 = _make_station(conn2, site_id2, "WSTATION01")
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
    conn4 = _make_db()
    site_id4 = _make_site(conn4, tz="UTC")
    station_id4 = _make_station(conn4, site_id4, "WSTATION01")
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
    )
    row4 = conn4.execute(
        "SELECT source_raw FROM station_observations"
        " WHERE station_id = ? AND valid_at = '2026-03-10T18:00:00Z'",
        (station_id4,),
    ).fetchone()
    assert row4 is not None and row4["source_raw"] == "pair-max 40.0 km/h"


# =============================================================================
# T97 -- a station-day stuck on repeated 500s becomes final after 3 attempts
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
    before "yesterday"), where ``history/all`` always answers 500. Today's
    auto-fabricated row is pre-closed so it never competes for a call.

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


# ---------------------------------------------------------------------------
# T99 -- a wind job probes a `probing` hold.
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


def test_t99_pair_max_probes_the_held_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A probing ``history_all`` hold with a due row gets a single probe
    call, and a successful probe clears the hold."""
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


def test_persist_day_refetch_partial_response_keeps_stored_record_count() -> None:
    """T107: a refetch response holding fewer records than already stored
    leaves ``record_count`` at the larger, already-stored figure -- it is
    derived from the table, never from the response length."""
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    local_date = date(2026, 3, 5)
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
    # nothing due + today's row already present would make wind_lane_due
    # False; to make it due, leave today's row missing (ensure_wind_days has
    # not run yet in this bare fixture).

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


def test_enqueue_due_wind_days_excludes_a_disabled_only_site_even_when_due(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real (non-equivalent) kill of the site-selection EXISTS clause the
    test above's docstring deferred: a second site whose only station is
    disabled, with ``wind_lane_due`` patched to return True for every site.
    The patched ``wind_lane_due`` calls that site due unconditionally, with
    no reference to station enablement at all -- so the ONLY thing standing
    between it and an enqueued job is the outer site-selection
    ``EXISTS (... st.enabled=1)`` clause itself. A disabled-only site with
    the real ``wind_lane_due`` (the test above) is equivalent; patching it
    to return True closes that gap and makes the kill real.
    """
    conn = _make_db()
    due_site_id = _make_site(conn, "Due Site")
    _make_station(conn, due_site_id, "WSTATION01")

    disabled_site_id = _make_site(conn, "Disabled Station Site")
    _make_station(conn, disabled_site_id, "WSTATION02", enabled=False)
    # Every site is due, whatever any station's enabled flag says.
    monkeypatch.setattr("wxverify.worker.scheduler.wind_lane_due", lambda *a, **k: True)

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
        " patched wind_lane_due returns True for every site, the"
        " disabled-station site included once it is no longer excluded"
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

    bad_zinfe_id = _make_site(conn, name="Testsite-bad-zinfe", tz="Not/AZone")
    _make_station(conn, bad_zinfe_id, "KTEST002")

    bad_value_id = _make_site(conn, name="Testsite-bad-value", tz="../etc/zone")
    _make_station(conn, bad_value_id, "KTEST003")

    good2_id = _make_site(conn, name="Testsite-good2", tz="UTC")
    _make_station(conn, good2_id, "KTEST004")

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


@pytest.mark.parametrize("value", ["pair_max", "staging", "switching", "bogus"])
def test_wind_lane_due_ignores_any_stored_wind_basis_value(value: str) -> None:
    """``wind_lane_due`` reads no stored wind-basis key: a quiescent site
    (today's row already fetched, nothing else due) stays not due whatever
    value an old ``wind_basis:<site_id>`` key holds, the old transitional
    ``switching`` value and an unknown ``bogus`` value included. Paired
    against the same site before any key is written, so each value is a
    real divergence rather than a fixture in which nothing is ever due."""
    from zoneinfo import ZoneInfo

    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    tz = ZoneInfo(_TZ)
    now = datetime(2026, 3, 10, 12, 0, 0, tzinfo=UTC)
    today = now.date()
    _insert_day(conn, station_id, today, status="fetched", record_count=1)

    assert wind_lane_due(conn, site_id, tz, now=now) is False, (
        "setup check: a quiescent site with today's row already fetched and"
        " no stored key must not be due, or the value below proves nothing"
    )

    set_runtime_state(conn, f"wind_basis:{site_id}", value)
    assert wind_lane_due(conn, site_id, tz, now=now) is False, (
        f"mutant -> at this assertion (value={value}): correct = False"
        " (wind_lane_due reads no stored key, so the quiescent site stays"
        " not due), mutant (a branch on the stored value restored, such as"
        ' `state in ("switching", "rescoring")` or an unknown-value branch)'
        " = True"
    )


def test_wind_days_lane_quiescence_leaves_a_claim_window_for_verification_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plan rationale ('Why not JobContinuation'): a wind-days lane with a job always
    pending would hold every ``verification_run`` chunk back, because
    ``claim_next_job``'s CASE orders ``verification_run``/``timezone_correction`` after
    every other pending type for their first 5 minutes (wind_days.py's
    ``WIND_DAYS_SPACING`` is what keeps a wind-days job from always being pending -- see
    the spacing test above). This pins both halves of that rationale in one place,
    DRIVING the real scheduler (``_enqueue_due_wind_days``) rather than hand-inserting
    the wind job, so the lane's own ``WIND_DAYS_SPACING`` cooldown is what keeps it
    quiescent, not just an absent row by construction: with the lane quiescent (a
    completed job 1 minute inside its 5-minute cooldown, so no pending wind-days job), a
    pending ``verification_run`` job IS the next claim; the paired positive re-stamps
    the completed job 6 minutes ago (outside the cooldown), drives the scheduler again,
    and shows the resulting pending wind-days job claims FIRST over a re-pended
    ``verification_run``, which is exactly the tier ordering that would hold it in tier
    2 for its first 5 minutes if a wind-days job were always pending.

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
        " job claims first (tier 1 beats the chain row's tier 2 -- it was"
        " claimed seconds ago, still inside its first 5 minutes, so it is"
        " not yet promoted), mutant"
        " (verification_run's CASE tier changed to rank ahead of or equal"
        " to the default tier) = the verification_run job claimed instead"
        " -- exactly the ordering that would hold it in tier 2 for its"
        " first 5 minutes if a wind-days job were always pending"
    )


# =============================================================================
# 2026-10-23 wind pre-midnight fetch (plan §9) -- S1-S6, items 1-8, 10, 11.
# D = the local day under test; start, end = local_day_slots(D, tz)[:2];
# F = end - WIND_FINAL_LEAD; R = end - _FINAL_RETRY_LEAD. Every expected
# value below is derived from these, never written as a bare literal.
# =============================================================================

_TZ_DENVER = "America/Denver"  # fall-back zone, see test_daily_truth_completeness.py:78


def _wind_payload(*timestamps: datetime) -> dict[str, object]:
    """A weather.com ``all_1day``-shaped response: one observation per
    timestamp (field names from ``wind_records_from_payload`` in
    ``obs/pws_adapter.py``, precedent: ``tests/test_wind_quota.py:662-671``)."""
    return {
        "observations": [
            {"epoch": int(ts.timestamp()), "metric": {"windspeedAvg": 5.0}}
            for ts in timestamps
        ]
    }


def _budget_calls(conn: sqlite3.Connection) -> int:
    """Total weathercom calls spent across every billing day (precedent:
    ``tests/test_wind_auth_hold.py:215-217``)."""
    row = conn.execute(
        "SELECT COALESCE(SUM(calls), 0) AS n FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    return int(row["n"])


def _five_minute_marks(start: datetime, through: datetime) -> list[datetime]:
    """5-minute observations from ``start`` through ``through`` inclusive
    (plan §9's build-records instruction)."""
    marks: list[datetime] = []
    mark = start
    while mark <= through:
        marks.append(mark)
        mark += timedelta(minutes=5)
    return marks


def test_all_1day_only_day_finishes_with_eligible_wind_truth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Plan item 1 (S1-S3). Today's final call lands at F and caps there;
    the finish step closes D after midnight without any history/all call,
    leaving record_count/attempts/last_ok_at/the raw-record count from the
    F call unchanged; the resulting day publishes eligible wind_max truth
    covering 24/24 hours."""
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    D = date(2026, 3, 10)
    start, end, _ = local_day_slots(D, _TZ)
    final = end - WIND_FINAL_LEAD
    write_auth_hold(
        conn, "history_all", status="held", since=isoformat_utc(start), error="401"
    )
    _insert_day(
        conn, station_id, D, status="partial", next_attempt_at=isoformat_utc(final)
    )

    payload = _wind_payload(*_five_minute_marks(start, final - timedelta(minutes=5)))
    call_log: list[str] = []
    responses = [200]
    # D's one seeded response only answers D's own call. Past midnight the
    # lane also opens tomorrow's row and fetches it as the new "today"; a
    # 200 there would let its own neighbour-derive write D's hour 23 too,
    # masking whether `_finish_day`'s own derive did -- so this unrelated
    # call is answered 404 instead (precedent:
    # test_missed_final_call_is_reported_and_the_day_still_finishes).
    new_day_responses = [404]

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        call_log.append(url)
        if "history/all" in url:
            return httpx.Response(500, request=request)
        status = responses.pop(0) if responses else new_day_responses.pop(0)
        if status == 200:
            return httpx.Response(200, json=payload)
        return httpx.Response(404, request=request)

    _t97_freeze(monkeypatch, final)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert len(call_log) == 1, f"expected exactly one call at F, got {call_log}"
    assert responses == [], "fixture bug: D's seeded response must be consumed"
    row = _day_row(conn, station_id, D)
    assert row["next_attempt_at"] == isoformat_utc(end + timedelta(hours=1)), (
        "mutant -> at this assertion: correct = end + 1h (now == F is not"
        " < final, and failed=False skips the retry branch), mutant (the"
        " now < final cap left open, or the retry branch taken despite"
        " failed=False) = F or R"
    )
    assert row["last_ok_at"] == isoformat_utc(final), (
        "mutant -> at this assertion: correct = F (the successful call's own"
        " clock reading), mutant (last_ok_at left stale from before this"
        " call) = a different/earlier timestamp"
    )
    before_record_count = row["record_count"]
    before_attempts = row["attempts"]
    before_raw_count = conn.execute(
        "SELECT COUNT(*) AS n FROM station_wind_records WHERE station_id = ?"
        " AND obs_at >= ? AND obs_at < ?",
        (station_id, isoformat_utc(start), isoformat_utc(end)),
    ).fetchone()["n"]

    call_log.clear()
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.wind_days"):
        _t97_freeze(monkeypatch, end + timedelta(minutes=5))
        _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert not any("history/all" in url for url in call_log), (
        "mutant -> at this assertion: correct = no history/all call (the"
        " finish step reads only stored records), mutant (the finish gate"
        " dropped, falling back to the old history/all retry path) = a"
        f" history/all URL appears, got {call_log}"
    )
    assert not any(
        "wind final collection missed" in rec.message for rec in caplog.records
    ), (
        "mutant -> at this assertion: correct = no missed-final WARNING"
        " (last_ok_at == F satisfies count_missed_final's [F, end) bound,"
        " read as >=), mutant (the `>=` bound at count_missed_final.py"
        " loosened to `>`, excluding a last_ok_at exactly at F) = a missed"
        f" WARNING, logged = {[rec.message for rec in caplog.records]}"
    )
    row = _day_row(conn, station_id, D)
    assert row["status"] == "fetched", (
        "mutant -> at this assertion: correct = fetched (records exist in"
        " [start, end)), mutant (the finish UPDATE's CASE WHEN n > 0"
        " inverted) = unavailable"
    )
    assert row["next_attempt_at"] is None, (
        "mutant -> at this assertion: correct = NULL (a finished row is"
        " never due again), mutant (next_attempt_at left set) = a timestamp"
    )
    assert row["record_count"] == before_record_count, (
        "finish recomputes record_count over the same [start, end) window"
        " as the F call, so it must land on the same value"
    )
    assert row["attempts"] == before_attempts
    after_raw_count = conn.execute(
        "SELECT COUNT(*) AS n FROM station_wind_records WHERE station_id = ?"
        " AND obs_at >= ? AND obs_at < ?",
        (station_id, isoformat_utc(start), isoformat_utc(end)),
    ).fetchone()["n"]
    assert after_raw_count == before_raw_count, (
        "mutant -> at this assertion: correct = unchanged (the finish step"
        " makes no API call and deletes no record), mutant (the finish"
        " step re-fetches or purges D's own window) = a different count"
    )

    hour_23 = end - timedelta(hours=1)
    hour_23_rows = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations WHERE station_id = ?"
        " AND variable = 'wind' AND valid_at = ?",
        (station_id, isoformat_utc(hour_23)),
    ).fetchone()["n"]
    assert hour_23_rows == 1, (
        "mutant -> at this assertion: correct = 1 (D's own neighbour is"
        " never a 'fetched'/'partial' row at this point, so only"
        " `_finish_day`'s own `_derive_days` call can write D's hour 23),"
        " mutant (that `_derive_days` call dropped from `_finish_day`) = 0"
    )

    outcomes = materialize_daily_truth(conn, site_id=site_id, local_date=D.isoformat())
    wind_max = outcomes["wind_max"]
    assert wind_max.eligible is True, (
        "mutant -> at this assertion: correct = eligible (24 >= expected - 1"
        " = 23), mutant (hour 23 never derived as a past day, leaving 23"
        " covered hours short of a 24-hour expectation... still eligible"
        " under the -1 rule, so a stricter break would read ineligible"
        " here) = False"
    )
    assert wind_max.covered_hours == wind_max.expected_slots == 24, (
        "mutant -> at this assertion: correct = 24 == 24 (hour 23 is"
        " written by the past-day derive inside the finish step), mutant"
        " (the finish step's own _derive_days call dropped, or _derive_hours'"
        " `local_date != today` branch never taken) = 23 covered"
    )


def test_fall_back_day_final_call_covers_25_hours(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan item 2. Same as item 1, on the 25-hour fall-back day."""
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn, tz=_TZ_DENVER)
    station_id = _make_station(conn, site_id, "WSTATION01")
    D = date(2026, 11, 1)
    start, end, _ = local_day_slots(D, _TZ_DENVER)
    assert end - start == timedelta(hours=25), (
        "fixture bug: 2026-11-01 must be the America/Denver fall-back day"
    )
    final = end - WIND_FINAL_LEAD
    write_auth_hold(
        conn, "history_all", status="held", since=isoformat_utc(start), error="401"
    )
    _insert_day(
        conn, station_id, D, status="partial", next_attempt_at=isoformat_utc(final)
    )

    payload = _wind_payload(*_five_minute_marks(start, final - timedelta(minutes=5)))
    responses = [200]
    # D's one seeded response only answers D's own call. Past midnight the
    # lane also opens tomorrow's row and fetches it as the new "today"; a
    # 200 there would let its own neighbour-derive write D's hour 24 too,
    # masking whether `_finish_day`'s own derive did -- so this unrelated
    # call is answered 404 instead (precedent:
    # test_missed_final_call_is_reported_and_the_day_still_finishes).
    new_day_responses = [404]

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "history/all" in url:
            return httpx.Response(500, request=request)
        status = responses.pop(0) if responses else new_day_responses.pop(0)
        if status == 200:
            return httpx.Response(200, json=payload)
        return httpx.Response(404, request=request)

    _t97_freeze(monkeypatch, final)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    _t97_freeze(monkeypatch, end + timedelta(minutes=5))
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert responses == [], "fixture bug: D's seeded response must be consumed"

    outcomes = materialize_daily_truth(conn, site_id=site_id, local_date=D.isoformat())
    wind_max = outcomes["wind_max"]
    assert wind_max.expected_slots == wind_max.covered_hours == 25, (
        "mutant -> at this assertion: correct = 25 == 25 (the fall-back"
        " day's extra hour is derived the same way as any other past-day"
        " hour), mutant (a hardcoded 24 anywhere in the expected-slots or"
        " hour-derive path) = 24"
    )
    assert wind_max.eligible is True


def test_today_next_attempt_caps_at_final_and_retries_once() -> None:
    """Plan item 3. A unit table at the default 180-minute cadence, over
    both ``failed`` values, at six ``now`` points spanning well-before-F,
    just-before-F, exactly F, between F and R, exactly R, and
    well-after-R."""
    conn = _make_db()
    end = datetime(2026, 3, 11, 0, 0, 0, tzinfo=UTC)
    final = end - WIND_FINAL_LEAD
    retry = end - _FINAL_RETRY_LEAD

    now_points = [
        end - timedelta(hours=5),
        end - timedelta(minutes=36),
        final,
        end - timedelta(minutes=25),
        retry,
        end - timedelta(minutes=5),
    ]
    expected_not_failed = [
        isoformat_utc(end - timedelta(hours=2)),
        isoformat_utc(final),
        isoformat_utc(end + timedelta(hours=1)),
        isoformat_utc(end + timedelta(hours=1)),
        isoformat_utc(end + timedelta(hours=1)),
        isoformat_utc(end + timedelta(hours=1)),
    ]
    expected_failed = [
        isoformat_utc(end - timedelta(hours=2)),
        isoformat_utc(final),
        isoformat_utc(retry),
        isoformat_utc(retry),
        isoformat_utc(end + timedelta(hours=1)),
        isoformat_utc(end + timedelta(hours=1)),
    ]

    for now, want in zip(now_points, expected_not_failed, strict=True):
        got = _today_next_attempt(conn, now, end, failed=False)
        assert got == want, (
            f"failed=False at now={now.isoformat()}:"
            " mutant -> at this assertion: correct ="
            f" {want}, mutant = {got}"
        )
    for now, want in zip(now_points, expected_failed, strict=True):
        got = _today_next_attempt(conn, now, end, failed=True)
        assert got == want, (
            f"failed=True at now={now.isoformat()}:"
            " mutant -> at this assertion: correct ="
            f" {want}, mutant = {got}"
        )


def test_missed_final_call_is_reported_and_the_day_still_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Plan item 4 (S4). A 200 well before the final window, then a 404 at
    F and a 404 at R, leave D's own last_ok_at stale from that single
    successful call -- well outside [F, end) -- so the finish step still
    closes D (from that call's stored records) but reports exactly one
    missed-final-collection WARNING, and the monitor reports it too."""
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    D = date(2026, 3, 10)
    start, end, _ = local_day_slots(D, _TZ)
    final = end - WIND_FINAL_LEAD
    retry = end - _FINAL_RETRY_LEAD
    write_auth_hold(
        conn, "history_all", status="held", since=isoformat_utc(start), error="401"
    )
    seed_at = end - timedelta(hours=3)
    _insert_day(
        conn, station_id, D, status="partial", next_attempt_at=isoformat_utc(seed_at)
    )
    payload = _wind_payload(*_five_minute_marks(start, seed_at - timedelta(minutes=5)))
    responses = [200, 404, 404]
    # D's three seeded responses only answer D's own calls. Past midnight the
    # lane also opens tomorrow's row and fetches it as the new "today", an
    # unchanged, unrelated call this fixture must still answer.
    new_day_responses = [404]

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "history/all" in url:
            return httpx.Response(500, request=request)
        status = responses.pop(0) if responses else new_day_responses.pop(0)
        if status == 200:
            return httpx.Response(200, json=payload)
        return httpx.Response(404, request=request)

    _t97_freeze(monkeypatch, seed_at)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    row = _day_row(conn, station_id, D)
    assert row["next_attempt_at"] == isoformat_utc(final), (
        "mutant -> at this assertion: correct = F (a 200 well before F"
        " caps the obs-cadence call at F), mutant (the now < final cap"
        " dropped) = a different value"
    )

    _t97_freeze(monkeypatch, final)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    row = _day_row(conn, station_id, D)
    assert row["next_attempt_at"] == isoformat_utc(retry), (
        "mutant -> at this assertion: correct = R (a 404 at F falls"
        " through to the ordinary row-failure path, retrying once at R),"
        " mutant (a 404 mistakenly dispatched like a 4xx-auth or 5xx"
        " branch) = a different value"
    )

    _t97_freeze(monkeypatch, retry)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    row = _day_row(conn, station_id, D)
    assert row["next_attempt_at"] == isoformat_utc(end + timedelta(hours=1)), (
        "mutant -> at this assertion: correct = end + 1h (the one retry is"
        " exhausted), mutant (the retry-exhaustion boundary dropped,"
        " retrying forever) = R again"
    )
    assert responses == [], "fixture bug: all three seeded responses must be consumed"

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.wind_days"):
        _t97_freeze(monkeypatch, end + timedelta(minutes=5))
        _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    missed_text = f"wind final collection missed site={site_id} date={D} stations=1/1"
    missed_records = [rec for rec in caplog.records if rec.message == missed_text]
    assert len(missed_records) == 1, (
        "mutant -> at this assertion: correct = exactly one missed-final"
        " WARNING (last_ok_at is stale from the first 200, well outside"
        " [F, end)), mutant (count_missed_final's window comparison"
        " inverted, treating the stale last_ok_at as covering, or the"
        " missed > 0 guard dropped) = zero matching records, logged ="
        f" {[rec.message for rec in caplog.records]}"
    )
    row = _day_row(conn, station_id, D)
    assert row["status"] == "fetched"
    assert row["next_attempt_at"] is None

    tripped, first = _wind_history_trips(conn, end + timedelta(hours=2))
    assert first == (
        f"wind: last collection before midnight missed for 1 of 1 stations ({D})"
    ), (
        "mutant -> at this assertion: correct = the missed-final text"
        f" naming {D}, mutant (the monitor's own count_missed_final call,"
        " or its missed > 0 guard, or the string split after 'for ')"
        f" = {first!r}"
    )
    assert tripped == 1


@pytest.mark.parametrize("status", [401, 403])
def test_final_call_refused_keeps_hold_behaviour(
    status: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Plan item 5 (S5). A 401/403 on the final call is unchanged
    behaviour: it sets the all_1day hold and leaves the row's own status
    and next_attempt_at exactly as seeded; afterwards the finish step
    still closes D with no "missed" WARNING (the hold already explains
    it), and the lane is no longer due."""
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    D = date(2026, 3, 10)
    start, end, _ = local_day_slots(D, _TZ)
    final = end - WIND_FINAL_LEAD
    write_auth_hold(
        conn, "history_all", status="held", since=isoformat_utc(start), error="401"
    )
    seeded_status = "partial"
    seeded_next_attempt = isoformat_utc(final)
    _insert_day(
        conn,
        station_id,
        D,
        status=seeded_status,
        next_attempt_at=seeded_next_attempt,
    )

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        call_log.append(url)
        if "history/all" in url:
            return httpx.Response(500, request=request)
        return httpx.Response(status, request=request)

    before_budget = _budget_calls(conn)
    _t97_freeze(monkeypatch, final)
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert len(call_log) == 1
    assert _budget_calls(conn) == before_budget + 1, (
        "mutant -> at this assertion: correct = spent (a 401/403 is a"
        " dispatched, non-refunded call), mutant (the reservation refunded"
        " on auth refusal) = unchanged"
    )
    hold = read_auth_hold(conn, "all_1day")
    assert hold is not None and hold.status == "held", (
        "mutant -> at this assertion: correct = held (unchanged auth-hold"
        " behaviour), mutant (the hold write dropped for a final call) ="
        " None or a different status"
    )
    row = _day_row(conn, station_id, D)
    assert row["status"] == seeded_status, (
        "mutant -> at this assertion: correct = unchanged ("
        f"{seeded_status!r}), mutant (_auth_refused mistakenly touches"
        f" status) = {row['status']!r}"
    )
    assert row["next_attempt_at"] == seeded_next_attempt, (
        "mutant -> at this assertion: correct = unchanged"
        f" ({seeded_next_attempt!r}), mutant (_auth_refused mistakenly"
        f" touches next_attempt_at) = {row['next_attempt_at']!r}"
    )

    call_log.clear()
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.wind_days"):
        _t97_freeze(monkeypatch, end + timedelta(minutes=5))
        _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert call_log == [], (
        f"a held all_1day endpoint must not be called again, got {call_log}"
    )
    row = _day_row(conn, station_id, D)
    assert row["status"] == "unavailable", (
        "mutant -> at this assertion: correct = unavailable (D's refused"
        " final call stored no records, so the finish step still closes"
        " the day as unavailable), mutant (the all_1day hold mistakenly"
        " blocks the finish step too) = partial (unchanged)"
    )
    assert row["next_attempt_at"] is None, (
        "mutant -> at this assertion: correct = None (the finish step"
        " clears next_attempt_at), mutant (the all_1day hold mistakenly"
        " blocks the finish step too) ="
        f" {seeded_next_attempt!r} (unchanged)"
    )
    assert not any("missed" in rec.message for rec in caplog.records), (
        "mutant -> at this assertion: correct = no missed-final WARNING"
        " (the all_1day hold already explains the gap, plan's"
        ' "all_1day" not in holds guard), mutant (the guard dropped) ='
        f" a missed WARNING, logged = {[rec.message for rec in caplog.records]}"
    )

    from zoneinfo import ZoneInfo

    assert wind_lane_due(
        conn, site_id, ZoneInfo(_TZ), now=end + timedelta(hours=2)
    ) is (False), (
        "mutant -> at this assertion: correct = False (D is finished and"
        " nothing else is due), mutant (wind_lane_due's finishable check"
        " still matches the closed row) = True"
    )


def test_budget_exhausted_final_call_defers_and_the_day_still_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Plan item 6 (S6). A daily_call_limit of 0 defers the final call
    itself with JobDeferred before any request and before the row
    changes; after midnight the finish step still closes D (it reserves
    no budget) and reports the missed-final WARNING, even though the
    same run's own fetch attempt for the new today row also defers."""
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    D = date(2026, 3, 10)
    start, end, _ = local_day_slots(D, _TZ)
    final = end - WIND_FINAL_LEAD
    write_auth_hold(
        conn, "history_all", status="held", since=isoformat_utc(start), error="401"
    )
    seeded_next_attempt = isoformat_utc(final)
    _insert_day(
        conn, station_id, D, status="partial", next_attempt_at=seeded_next_attempt
    )
    conn.execute("UPDATE sources SET daily_call_limit = 0 WHERE source = 'weathercom'")
    conn.commit()

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    _t97_freeze(monkeypatch, final)
    with pytest.raises(JobDeferred):
        _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert call_log == [], (
        "mutant -> at this assertion: correct = no call (the budget"
        " reservation is checked before any request), mutant (the"
        " reservation check moved after the request) = a request"
        f" happened, got {call_log}"
    )
    row = _day_row(conn, station_id, D)
    assert row["status"] == "partial"
    assert row["next_attempt_at"] == seeded_next_attempt

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.wind_days"):
        _t97_freeze(monkeypatch, end + timedelta(minutes=5))
        with pytest.raises(JobDeferred):
            _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    row = _day_row(conn, station_id, D)
    assert row["status"] == "unavailable", (
        "mutant -> at this assertion: correct = unavailable (D never"
        " stored a record, since every call was deferred by the zero"
        " budget), mutant (the finish step never ran, so status stays"
        " partial) = partial"
    )
    assert row["next_attempt_at"] is None, (
        "mutant -> at this assertion: correct = NULL (the finish step"
        " runs -- and completes -- before any budget reservation),"
        " mutant (the finish step moved after the fetch loop's own"
        " reservation, so a JobDeferred there skips it) = unchanged"
    )
    assert any(
        rec.message
        == f"wind final collection missed site={site_id} date={D} stations=1/1"
        for rec in caplog.records
    ), (
        "mutant -> at this assertion: correct = the missed-final WARNING"
        " is still logged despite the same run later raising JobDeferred,"
        " mutant (the finish step's own write ordered after the"
        " exception-raising fetch attempt) = no such record, logged ="
        f" {[rec.message for rec in caplog.records]}"
    )


def test_finish_gate_is_an_allowlist() -> None:
    """Plan item 7. ``_finish_day`` is called directly; the gate is an
    allowlist, so every disqualifying condition returns False and writes
    nothing, and every qualifying one returns True and writes exactly
    the documented change."""
    today = date(2026, 3, 10)
    yesterday = today - timedelta(days=1)
    start, end, _ = local_day_slots(yesterday, _TZ)

    def _setup(
        *, hold_status: str | None = "held"
    ) -> tuple[sqlite3.Connection, int, int]:
        conn = _make_db()
        site_id = _make_site(conn)
        station_id = _make_station(conn, site_id, "WSTATION01")
        if hold_status is not None:
            write_auth_hold(
                conn,
                "history_all",
                status=hold_status,  # type: ignore[arg-type]
                since=isoformat_utc(start),
                error=None,
            )
        return conn, site_id, station_id

    def _unchanged(
        got: bool, conn: sqlite3.Connection, station_id: int, before: dict
    ) -> None:
        assert got is False, (
            "mutant -> at this assertion: correct = False (a disqualifying"
            " gate condition), mutant (the allowlist check for this"
            " condition dropped) = True"
        )
        after = dict(
            _day_row(conn, station_id, date.fromisoformat(before["local_date"]))
        )
        assert after == before, (
            "mutant -> at this assertion: correct = unchanged (a gated-out"
            " row is written to), mutant (a write happens before the gate"
            " re-check) = a changed row"
        )

    # -- False: no hold ---------------------------------------------------
    conn, site_id, station_id = _setup(hold_status=None)
    _insert_day(conn, station_id, yesterday, status="pending")
    before = dict(_day_row(conn, station_id, yesterday))
    got = _finish_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=yesterday,
        tz_name=_TZ,
        today=today,
    )
    _unchanged(got, conn, station_id, before)

    # -- False: probing ----------------------------------------------------
    conn, site_id, station_id = _setup(hold_status="probing")
    _insert_day(conn, station_id, yesterday, status="pending")
    before = dict(_day_row(conn, station_id, yesterday))
    got = _finish_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=yesterday,
        tz_name=_TZ,
        today=today,
    )
    _unchanged(got, conn, station_id, before)

    # -- False: today's row --------------------------------------------
    conn, site_id, station_id = _setup()
    _insert_day(conn, station_id, today, status="pending")
    before = dict(_day_row(conn, station_id, today))
    got = _finish_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=today,
        tz_name=_TZ,
        today=today,
    )
    _unchanged(got, conn, station_id, before)

    # -- False: failed with attempts == 3 ----------------------------------
    conn, site_id, station_id = _setup()
    _insert_day(conn, station_id, yesterday, status="failed", attempts=3)
    before = dict(_day_row(conn, station_id, yesterday))
    got = _finish_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=yesterday,
        tz_name=_TZ,
        today=today,
    )
    _unchanged(got, conn, station_id, before)

    # -- True: partial with records -> fetched -----------------------------
    conn, site_id, station_id = _setup()
    _insert_day(conn, station_id, yesterday, status="partial")
    conn.execute(
        "INSERT INTO station_wind_records (station_id, obs_at, speed_kmh)"
        " VALUES (?, ?, 5.0)",
        (station_id, isoformat_utc(start + timedelta(hours=1))),
    )
    conn.commit()
    got = _finish_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=yesterday,
        tz_name=_TZ,
        today=today,
    )
    assert got is True
    row = _day_row(conn, station_id, yesterday)
    assert row["status"] == "fetched", (
        "mutant -> at this assertion: correct = fetched (n > 0 records"
        " found), mutant (the CASE WHEN n > 0 inverted) = unavailable"
    )
    assert row["record_count"] == 1
    assert row["next_attempt_at"] is None

    # -- True: pending without records -> unavailable -----------------
    conn, site_id, station_id = _setup()
    _insert_day(conn, station_id, yesterday, status="pending")
    got = _finish_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=yesterday,
        tz_name=_TZ,
        today=today,
    )
    assert got is True
    row = _day_row(conn, station_id, yesterday)
    assert row["status"] == "unavailable", (
        "mutant -> at this assertion: correct = unavailable (n == 0),"
        " mutant (the CASE WHEN n > 0 inverted) = fetched"
    )

    # -- True: fetched with a pending refetch -> refetched=1, status kept --
    conn, site_id, station_id = _setup()
    _insert_day(
        conn,
        station_id,
        yesterday,
        status="fetched",
        record_count=5,
        refetch_at=isoformat_utc(start),
        refetched=0,
    )
    got = _finish_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=yesterday,
        tz_name=_TZ,
        today=today,
    )
    assert got is True
    row = _day_row(conn, station_id, yesterday)
    assert row["refetched"] == 1, (
        "mutant -> at this assertion: correct = 1 (a pending refetch is"
        " closed), mutant (the refetch-row branch dropped) = 0"
    )
    assert row["status"] == "fetched", (
        "mutant -> at this assertion: correct = fetched (status kept,"
        " not recomputed), mutant (the refetch row falls through to the"
        " ordinary recompute path) = a possibly-different status"
    )

    # -- Floor boundary: K is in, K-1 is out -------------------------------
    conn, site_id, station_id = _setup()
    floor_date = today - timedelta(days=TRAINING_DAYS)
    _insert_day(conn, station_id, floor_date, status="pending")
    got = _finish_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=floor_date,
        tz_name=_TZ,
        today=today,
    )
    assert got is True, (
        "mutant -> at this assertion: correct = True (today - TRAINING_DAYS"
        " is the first day kept, >= floor), mutant (the floor boundary off"
        " by one, excluding this exact date) = False"
    )
    row = _day_row(conn, station_id, floor_date)
    assert row["status"] == "unavailable"

    conn, site_id, station_id = _setup()
    before_floor_date = floor_date - timedelta(days=1)
    _insert_day(conn, station_id, before_floor_date, status="pending")
    before = dict(_day_row(conn, station_id, before_floor_date))
    got = _finish_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=before_floor_date,
        tz_name=_TZ,
        today=today,
    )
    assert got is False, (
        "mutant -> at this assertion: correct = False (one day older than"
        " the floor is excluded), mutant (the floor boundary off by one,"
        " including this date too) = True"
    )
    after = dict(_day_row(conn, station_id, before_floor_date))
    assert after == before


def test_wind_lane_due_while_a_finishable_row_waits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan item 8. ``wind_lane_due`` must see a waiting finishable row
    only while ``history_all`` is held, and must stop seeing it once
    ``_finish_day`` closes it; a row whose date cannot be parsed never
    counts as due either way, and the lane's own per-start housekeeping
    (``ensure_wind_days``) deletes it rather than leaving it "unchanged"
    (mandatory correction over the plan's literal wording)."""
    from zoneinfo import ZoneInfo

    today = date(2026, 3, 15)
    yesterday = today - timedelta(days=1)
    start, _end, _ = local_day_slots(yesterday, _TZ)
    future_next_attempt = isoformat_utc(
        datetime.combine(today, datetime.min.time(), tzinfo=UTC) + timedelta(hours=1)
    )

    # -- no hold: a waiting row is not due --------------------------------
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    _insert_day(
        conn, station_id, today, status="fetched", next_attempt_at=future_next_attempt
    )
    _insert_day(
        conn,
        station_id,
        yesterday,
        status="partial",
        next_attempt_at=future_next_attempt,
    )
    assert (
        wind_lane_due(
            conn,
            site_id,
            ZoneInfo(_TZ),
            now=datetime.combine(today, datetime.min.time(), tzinfo=UTC),
        )
        is False
    ), (
        "mutant -> at this assertion: correct = False (no hold, so the"
        " finishable-row due check must not fire), mutant (the due check"
        " dropped the held-hold guard) = True"
    )

    # -- held: the same waiting row is now due ----------------------------
    write_auth_hold(
        conn, "history_all", status="held", since=isoformat_utc(start), error="401"
    )
    now = datetime.combine(today, datetime.min.time(), tzinfo=UTC)
    assert wind_lane_due(conn, site_id, ZoneInfo(_TZ), now=now) is True, (
        "mutant -> at this assertion: correct = True (the same row, now"
        " finishable), mutant (the finishable-row due check never added)"
        " = False"
    )

    # -- after _finish_day closes it: not due again ------------------------
    _finish_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=yesterday,
        tz_name=_TZ,
        today=today,
    )
    assert wind_lane_due(conn, site_id, ZoneInfo(_TZ), now=now) is False, (
        "mutant -> at this assertion: correct = False (the row is"
        " finished, so no finishable row remains), mutant (the due check"
        " re-reads a stale row set, or ignores status) = True"
    )

    # -- a row with an unparseable date never counts as due, either way --
    bad_date = f"{today:%Y-%m}-0x"
    conn2 = _t97_init_tmp_db(tmp_path)
    site_id2 = _make_site(conn2)
    station_id2 = _make_station(conn2, site_id2, "WSTATION01")
    write_auth_hold(
        conn2, "history_all", status="held", since=isoformat_utc(start), error="401"
    )
    _insert_day(
        conn2, station_id2, today, status="fetched", next_attempt_at=future_next_attempt
    )
    _insert_day(conn2, station_id2, bad_date, status="partial")
    assert wind_lane_due(conn2, site_id2, ZoneInfo(_TZ), now=now) is False, (
        "mutant -> at this assertion: correct = False (the canonical-date"
        " guard excludes an unparseable row from the finishable-row due"
        " check), mutant (the guard dropped from wind_lane_due's own"
        " query) = True"
    )

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    _t97_freeze(monkeypatch, now)
    _t97_run_wind_days(site_id2, handler, monkeypatch=monkeypatch)  # must not raise
    assert call_log == [], (
        f"neither row is due or finishable by a real call, got {call_log}"
    )
    remaining = conn2.execute(
        "SELECT local_date FROM station_wind_days WHERE station_id = ?"
        " AND local_date = ?",
        (station_id2, bad_date),
    ).fetchall()
    assert remaining == [], (
        "mutant -> at this assertion: correct = gone (ensure_wind_days'"
        " per-start housekeeping deletes any row whose local_date is not"
        " a real date, so this row is deleted rather than left"
        ' "unchanged" as a bare reading of the plan text would claim),'
        " mutant (the housekeeping delete skipped) = still present"
    )


def test_final_call_429_leaves_row_due_until_the_backoff_ends(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan item 10. A 429 on the final call raises JobDeferred carrying
    the domain backoff's own F + 60s (the default initial delay, no
    Retry-After header), while the row's own next_attempt_at stays at F
    -- the two times must be asserted separately. Once the backoff
    clears, the next run makes the call and finishes the obs-cadence
    schedule normally."""
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    D = date(2026, 3, 10)
    start, end, _ = local_day_slots(D, _TZ)
    final = end - WIND_FINAL_LEAD
    write_auth_hold(
        conn, "history_all", status="held", since=isoformat_utc(start), error="401"
    )
    seeded_next_attempt = isoformat_utc(final)
    _insert_day(
        conn, station_id, D, status="partial", next_attempt_at=seeded_next_attempt
    )

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        call_log.append(url)
        if "history/all" in url:
            return httpx.Response(500, request=request)
        if len(call_log) == 1:
            return httpx.Response(429, request=request)
        return httpx.Response(200, json={"observations": []})

    before_budget = _budget_calls(conn)
    _t97_freeze(monkeypatch, final)
    with pytest.raises(JobDeferred) as exc_info:
        _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert len(call_log) == 1
    assert exc_info.value.next_attempt_at == isoformat_utc(
        final + timedelta(seconds=60)
    ), (
        "mutant -> at this assertion: correct = F + 60s (record_http_backoff's"
        " default initial delay with no Retry-After header), mutant (the"
        " domain backoff's default delay changed, or the 429 branch stopped"
        " raising JobDeferred with that time) = a different value"
    )
    row = _day_row(conn, station_id, D)
    assert row["status"] == "partial", (
        "mutant -> at this assertion: correct = unchanged (a 429 only"
        " records a domain backoff and a last_error; it never touches the"
        " row itself), mutant (the row mistakenly marked failed/fetched"
        " on a 429) = a different status"
    )
    assert row["next_attempt_at"] == seeded_next_attempt, (
        "mutant -> at this assertion: correct = unchanged (F, the row's"
        " own column), mutant (the row's next_attempt_at mistakenly"
        " overwritten with the domain-backoff time) = F + 60s"
    )
    assert row["last_error"] is not None
    assert _budget_calls(conn) == before_budget + 1, (
        "mutant -> at this assertion: correct = spent (a 429 is a"
        " dispatched, non-refunded call), mutant (the reservation"
        " refunded on a 429) = unchanged"
    )

    _t97_freeze(monkeypatch, final + timedelta(seconds=60))
    _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)
    assert len(call_log) == 2, (
        f"the backoff must have cleared by F + 60s, got calls {call_log}"
    )
    row = _day_row(conn, station_id, D)
    assert row["next_attempt_at"] == isoformat_utc(end + timedelta(hours=1)), (
        "mutant -> at this assertion: correct = end + 1h (a successful"
        " obs-cadence call past F advances to the normal post-finish"
        " schedule), mutant (the prior 429's backoff state leaked into"
        " this call's own scheduling) = a different value"
    )
    assert _budget_calls(conn) == before_budget + 2


def test_finish_exception_marks_the_row_and_the_step_moves_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Plan item 11. ``_finish_waiting_days`` wraps each row's
    ``_finish_day`` call in its own try/except: an exception for one
    station-day is caught, logs the documented WARNING naming that
    station and date, and records a row failure -- it must not stop the
    loop (a sibling station's row still finishes) or the day's own
    all_1day call. After three failing attempts the row is no longer
    retryable and the lane is no longer due for it."""
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    pws_a = "WSTATION01"
    pws_b = "WSTATION02"
    station_a = _make_station(conn, site_id, pws_a)
    station_b = _make_station(conn, site_id, pws_b)
    today = date(2026, 3, 10)
    yesterday = today - timedelta(days=1)
    start, _end, _ = local_day_slots(yesterday, _TZ)
    write_auth_hold(
        conn, "history_all", status="held", since=isoformat_utc(start), error="401"
    )
    now = datetime.combine(today, datetime.min.time(), tzinfo=UTC)
    due_next_attempt = isoformat_utc(now - timedelta(minutes=1))
    for station_id in (station_a, station_b):
        _insert_day(
            conn,
            station_id,
            yesterday,
            status="partial",
            record_count=1,
        )
        conn.execute(
            "INSERT INTO station_wind_records (station_id, obs_at, speed_kmh)"
            " VALUES (?, ?, 1.0)",
            (station_id, isoformat_utc(start + timedelta(hours=1))),
        )
        _insert_day(
            conn,
            station_id,
            today,
            status="partial",
            next_attempt_at=due_next_attempt,
        )
    conn.commit()

    import wxverify.worker.wind_days as wind_days_module

    real_derive_days = wind_days_module._derive_days  # noqa: SLF001 -- test wrapper

    def failing_derive_days(conn: sqlite3.Connection, **kwargs: object) -> object:
        if (
            kwargs.get("station_id") == station_a
            and kwargs.get("local_date") == yesterday
        ):
            raise RuntimeError("synthetic finish failure")
        return real_derive_days(conn, **kwargs)

    monkeypatch.setattr(wind_days_module, "_derive_days", failing_derive_days)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(200, json={"observations": []})

    _t97_freeze(monkeypatch, now)
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.wind_days"):
        _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    row_b = _day_row(conn, station_b, yesterday)
    assert row_b["status"] == "fetched", (
        "mutant -> at this assertion: correct = fetched (station B's"
        " finish succeeds independently of A's exception), mutant (the"
        " per-row try/except dropped, so one exception aborts the whole"
        " loop) = unchanged (partial)"
    )
    assert any("observations/all/1day" in url for url in call_log), (
        "mutant -> at this assertion: correct = today's due all_1day call"
        " still happens after the finish step (a finish exception must"
        " not abort the rest of the job), mutant (the exception"
        " propagates out of _finish_waiting_days uncaught, aborting the"
        f" chunk before the fetch loop runs) = no such call, got {call_log}"
    )
    warning_text = (
        f"wind day finish failed site={site_id} station={pws_a}"
        f" date={yesterday}: synthetic finish failure"
    )
    matching = [rec for rec in caplog.records if rec.message == warning_text]
    assert len(matching) == 1, (
        "mutant -> at this assertion: correct = exactly one WARNING"
        " naming station A, yesterday's date and the exception text,"
        " mutant (the log message format, or the except block itself,"
        f" dropped) = {[rec.message for rec in caplog.records]}"
    )
    row_a = _day_row(conn, station_a, yesterday)
    assert row_a["status"] == "failed"
    assert row_a["attempts"] == 1, (
        "mutant -> at this assertion: correct = 1 (the except block"
        " records an ordinary row failure), mutant (the except block"
        " swallows the exception without a row failure) = 0"
    )

    for _ in range(2):
        caplog.clear()
        now = now + timedelta(hours=5)
        _t97_freeze(monkeypatch, now)
        _t97_run_wind_days(site_id, handler, monkeypatch=monkeypatch)

    row_a = _day_row(conn, station_a, yesterday)
    assert row_a["attempts"] == 3, (
        "mutant -> at this assertion: correct = 3 (every finish exception"
        " is an ordinary row failure, incrementing attempts each time),"
        " mutant (attempts capped early, or not incremented on repeat"
        " failures) = a different value"
    )
    no_longer_finishable = conn.execute(
        _FINISHABLE_SQL,
        {
            "site_id": site_id,
            "today": today.isoformat(),
            "floor": _finish_floor(today),
            "station_id": station_a,
            "local_date": yesterday.isoformat(),
            "limit": 1,
        },
    ).fetchone()
    assert no_longer_finishable is None, (
        "mutant -> at this assertion: correct = excluded (status='failed'"
        " with attempts >= 3 matches none of _FINISHABLE_SQL's status"
        " clauses, the same retry-exhaustion rule any other failed row"
        " follows), mutant (repeated finish exceptions never reach"
        " attempts >= 3, or the allowlist's failed/attempts<3 clause"
        " dropped) = still matched"
    )
