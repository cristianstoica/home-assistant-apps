"""Tests: the wind-history lane, group 1 of plan section 15.1 (2026-09-30).

Covers T8, T30 (today-only row creation), T34 (today's own persist/retry),
T45, T47 against ``wxverify/worker/wind_days.py`` (selector, persist,
scheduler integration) using a real sqlite3 ``:memory:`` connection +
``run_migrations`` for the sync surface, and a tmp-path
``Database``/``FencedWriter`` for the handful of cases that go through an
async write path (T45, T47 via dispatch/scheduler).

Also covers T35 (the ``_fetch_failed`` >=500/payload-fault/refundable-
transport-error dispatch -- in ``tests/test_wind_auth_hold.py``, alongside
its existing 401/403/429 coverage) and T46's chunk-level
headroom-exhaustion integration (``_run_fetch_chunk`` raising
``WindHeadroomExhausted`` through a live chunk loop -- in
``tests/test_wind_quota.py``, reusing its headroom-0 fixture).

T33 (held/probing endpoint selection), T36 (``pair_max`` materialize and
``pair_and_score`` enqueue), T44/T104-T107 (reconcile/refetch), T97 (a
past-day row retried to final) and T99 (a probing-hold job) are retired by
the daily-close switch-over plan (section 7.2, R4/R5/R7): the lane no
longer fetches or finishes a past day, derives hourly station rows, or
holds ``history_all``/``all_1day`` behind an auth hold. The remaining
429/budget-deferral coverage for today's own evening-window row stays
(``test_final_call_429_leaves_row_due_until_the_backoff_ends``, reusing
``_t97_init_tmp_db``/``_t97_freeze``/``_t97_run_wind_days``).

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
from wxverify.db.queue import Job
from wxverify.db.runtime_state import set_runtime_state
from wxverify.obs.hourly_values import StationReading
from wxverify.worker.control import JobDeferred
from wxverify.worker.processor import dispatch
from wxverify.worker.scheduler import (
    _enqueue_due_wind_days,  # noqa: PLC2701 -- private, exercised directly (T47)
)
from wxverify.worker.wind_days import (  # noqa: PLC2701 -- private, exercised directly
    WIND_DAYS_JOB_KEY,
    _persist_day,
    _today_next_attempt,
    ensure_wind_days,
    run_wind_days,
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
             next_attempt_at, refetch_at, refetched, last_error, last_ok_at,
             updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '2026-01-01T00:00:00Z')
        """,
        (
            station_id,
            ld,
            status,
            attempts,
            record_count,
            pair_hours,
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
    """``_persist_day`` (``worker/wind_days.py:1345``) for today's row,
    close off: the stored window is yesterday's start to today's end,
    capped at now plus ``_FUTURE_SLACK`` (``_today_window``, ``:1312``; P2
    4.1, the past-row margin gone with R4); ``record_count`` is today's
    exact [start, end). A reading in yesterday's bounds is stored but not
    counted; one before yesterday's start is dropped from both."""
    conn = _make_db()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    local_date = date(2020, 1, 15)
    _insert_day(conn, station_id, local_date, status="pending")

    start = datetime(2020, 1, 15, 0, 0, 0, tzinfo=UTC)

    def _reading(obs_at: datetime, speed_kmh: float) -> StationReading:
        return StationReading(
            obs_at=obs_at,
            source="five_minute",
            local_date=None,
            temp_c=None,
            precip_total_mm=None,
            speed_kmh=speed_kmh,
        )

    in_yesterday_not_in_day = _reading(start - timedelta(minutes=30), 5.0)
    before_yesterday = _reading(start - timedelta(hours=25), 5.0)
    in_day = _reading(start + timedelta(hours=1), 7.0)

    _persist_day(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=_TZ,
        today=local_date,
        records=[in_yesterday_not_in_day, before_yesterday, in_day],
    )

    stored = {
        row["obs_at"]
        for row in conn.execute(
            "SELECT obs_at FROM station_wind_records WHERE station_id = ?",
            (station_id,),
        )
    }
    assert in_yesterday_not_in_day.obs_at.isoformat().replace("+00:00", "Z") in stored
    assert in_day.obs_at.isoformat().replace("+00:00", "Z") in stored
    assert before_yesterday.obs_at.isoformat().replace("+00:00", "Z") not in stored

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
        no_content=True,
    )

    row = _day_row(conn, station_id, today)
    assert row["status"] == "partial", (
        "mutant -> at this assertion: correct = 'partial' (R4/R7: the lane"
        " selects only today's row, and _persist_day always sets it"
        " partial), mutant (status set to something else) = a different"
        " status"
    )
    assert row["next_attempt_at"] is not None


def test_today_next_attempt_near_midnight_rolls_to_tomorrow_plus_an_hour() -> None:
    """4.2: the daytime cadence is capped at end - 1h (23:00 local), not the
    old end - 35 min; a try at or after the cap clamps there too (the
    evening window, not this function, owns the midnight rollover)."""
    conn = _make_db()
    end = datetime(2026, 3, 11, 0, 0, 0, tzinfo=UTC)
    cap = end - timedelta(hours=1)

    normal = _today_next_attempt(conn, end - timedelta(hours=5), cap=cap)
    assert normal < isoformat_utc(cap), (
        "mutant -> at this assertion: correct = now+interval (well before"
        " the 23:00 cap), mutant (the old 23:25 cap, or always rolls to"
        " the cap) = a time at or after 23:00"
    )

    at_cap = _today_next_attempt(conn, cap, cap=cap)
    assert at_cap == isoformat_utc(cap), (
        "mutant -> at this assertion: correct = the cap itself"
        " ('2026-03-10T23:00:00Z'; min(now+interval, cap) clamps once now"
        " has reached the cap), mutant (the old end - 35 min cap, or a"
        " jump to tomorrow + 1h kept here instead of in the evening"
        " window) = a different value"
    )


# =============================================================================
# Shared async dispatch harness (tmp-path DB + httpx.MockTransport), used by
# the plan section 9 item-10 test below. T97 itself (a past-day row retried
# to final) is retired by R4.
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

    monkeypatch.setattr(processor_module, "run_wind_days", fake_run_wind_days)

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
        " WIND_DAYS_JOB_KEY routes to run_wind_days), mutant = RuntimeError"
        " unknown job type"
    )


def test_dispatch_sends_a_fetch_obs_job_with_another_key_to_the_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import wxverify.worker.processor as processor_module

    called: list[str] = []

    async def fake_run_wind_days(db: object, writer: object, site_id: int) -> None:
        called.append("wind_days")

    monkeypatch.setattr(processor_module, "run_wind_days", fake_run_wind_days)

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
    with pytest.raises(RuntimeError, match="unknown job type fetch_obs"):
        asyncio.run(dispatch(None, None, job))  # type: ignore[arg-type]

    assert called == []


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


# =============================================================================
# 2026-10-23 wind pre-midnight fetch (plan §9). Items 1, 2, 4-8 and 11
# (S1-S6, the day-finish step) are retired by the daily-close switch-over
# plan (R4/R7: nothing fetches or finishes a past/waiting day, and the
# ``history_all`` auth hold is gone). Item 3 (``_today_next_attempt``'s
# cap) and item 10 (a 429 during the evening window) are kept below, for
# today's own row. D = the local day under test; start, end =
# local_day_slots(D, tz)[:2]. Every expected value is derived from `end`,
# never written as a bare literal.
# =============================================================================


def _budget_calls(conn: sqlite3.Connection) -> int:
    """Total weathercom calls spent across every billing day (precedent:
    ``tests/test_wind_auth_hold.py:215-217``)."""
    row = conn.execute(
        "SELECT COALESCE(SUM(calls), 0) AS n FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    return int(row["n"])


def test_today_next_attempt_caps_at_final_and_retries_once() -> None:
    """Plan item 3, rewritten for 4.2's evening window (14.12) and again for
    0.16.14's signature change (``cap`` required, ``failed`` dropped -- it
    never changed the result, so the old outer ``for failed in (False,
    True)`` loop ran the identical call twice and is dropped here along
    with the kwarg itself): the daytime cadence is ``min(now + interval,
    cap)`` -- 4.2 drops the old one-retry-near-midnight window (the 15
    evening slots, tested in ``test_evening_window.py``, own every retry at
    or after the cap)."""
    conn = _make_db()
    end = datetime(2026, 3, 11, 0, 0, 0, tzinfo=UTC)
    cap = end - timedelta(hours=1)

    now_points = [
        end - timedelta(hours=5),
        end - timedelta(minutes=61),
        cap,
    ]
    expected = [
        isoformat_utc(end - timedelta(hours=2)),
        isoformat_utc(cap),
        isoformat_utc(cap),
    ]

    for now, want in zip(now_points, expected, strict=True):
        got = _today_next_attempt(conn, now, cap=cap)
        assert got == want, (
            f"at now={now.isoformat()}:"
            " mutant -> at this assertion: correct ="
            f" {want} (the cap is 23:00, 4.2), mutant ="
            f" {got}"
        )


def test_final_call_429_leaves_row_due_until_the_backoff_ends(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan item 10, adapted for 4.2's window-deferral rule (14.12): a 429
    on the final call (now the always-run 23:56 slot) raises JobDeferred
    carrying the domain backoff's own F + 60s (the default initial delay,
    no Retry-After header), while the row's own next_attempt_at moves --
    unlike 0.16.13's unconditional 15-minute defer -- to the first evening
    slot at or after that backoff end (4.2's "Deferrals inside the
    window"), here 23:57 -- the two times must be asserted separately.
    Once the backoff clears, the next run makes the call and finishes the
    evening window normally."""
    conn = _t97_init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "WSTATION01")
    D = date(2026, 3, 10)
    _, end, _ = local_day_slots(D, _TZ)
    final = end - timedelta(minutes=4)
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
    assert row["next_attempt_at"] == isoformat_utc(final + timedelta(minutes=1)), (
        "mutant -> at this assertion: correct = 23:57 (4.2: in the window,"
        " a 429 back-off moves the row to the first evening slot at or"
        " after the deferral's end, F + 60s = 23:57:00), mutant (the row"
        " left unchanged at F, 0.16.13's behaviour, or jumped 15 minutes"
        f" ahead) = {seeded_next_attempt!r} or a different value"
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
