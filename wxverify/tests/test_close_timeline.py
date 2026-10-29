"""The night and the first attempt (plan 2026-10-21-daily-close-close-job.md
section 15.4): TL1 to TL10.

Written before ``close_chain.enqueue_due_close_work`` and ``close_p4``
exist, and fails first for that reason: every test here reaches a call
that needs one of them, either directly (an inline import inside a test
body, below) or through ``tests.helpers.install_round_state_standin``,
which patches an attribute of ``wxverify.worker.close_p4`` -- a module
that does not exist yet, so the patch itself raises
``ModuleNotFoundError``. Where the close branch is never reached at all
(``scheduler_tick`` has no ``daily_close_on`` branch to call it from
until piece 4), the test instead fails on a plain assertion that needs
P3's behaviour -- the moved-day or new-job-row check simply finds nothing
moved and nothing enqueued.

Section 16.2 of the plan names the mutant each test ID kills; that mapping
is given inline on the exact assertion it kills at, in the file's own
style. TL7 has no listed mutant (16.2 lists none for it), so its assertion
carries a plain message.

Real SQLite only (``tests.helpers.standard_night``, built on
``tests.helpers.day_close_site``, real ``run_migrations`` via ``init_db``
on a ``tmp_path`` file). Station ids are synthetic (``ISTATION01`` to
``ISTATION04``), zones are ``UTC`` and, for TL5, ``Etc/GMT-14`` alongside
``UTC``, and every date is the fixed synthetic ``tests.helpers.D``/the
derived round date ``R``. No live id, coordinate, site name or real zone
name appears.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import (
    NOW,
    STANDARD_NIGHT_R,
    STANDARD_NIGHT_ROUND_BOUND,
    D,
    MovingClock,
    clear_close_chain_log_map,
    day_close_site,
    disable_other_enqueuers,
    install_p4_standins,
    install_round_state_standin,
    note_day_clock,
    parse_utc,
    patch_close_clock,
    run_due_jobs,
    standard_night,
    stub_fetch_feed,
    stub_wind_days,
    tick,
)
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro
from wxverify.db.queue import claim_next_job, enqueue_if_absent, reclaim_all_stale
from wxverify.db.site_days import begin_close, day_record, fail_close, finish_close
from wxverify.verification.coverage import local_day_bounds
from wxverify.worker.close_p4 import ScoreSummary


@pytest.fixture(autouse=True)
def _clear_close_chain_log_map() -> None:
    """This file's own autouse wiring of ``clear_close_chain_log_map``
    (15.1's "The log map.") -- autouse HERE, inside this one P3 test
    file, never across the whole suite (constraint: the module-level
    fixture lives in this file, not in ``tests/conftest.py``)."""
    clear_close_chain_log_map()


R = STANDARD_NIGHT_R
BOUND = STANDARD_NIGHT_ROUND_BOUND


def _r_at(hour: int, minute: int) -> datetime:
    return datetime(R.year, R.month, R.day, hour, minute, tzinfo=UTC)


async def _state(db, site_id, local_date=D):  # type: ignore[no-untyped-def]
    return await db.read(lambda c: day_record(c, site_id, local_date))


async def _stage1_row(db, site_id, local_date=D):  # type: ignore[no-untyped-def]
    return await db.read(
        lambda c: c.execute(
            "SELECT started_at, ended_at, job_id FROM close_stage_log"
            " WHERE site_id = ? AND local_date = ? AND stage = 1",
            (site_id, local_date.isoformat()),
        ).fetchone()
    )


async def _day_close_job_keys(db, site_id):  # type: ignore[no-untyped-def]
    rows = await db.read(
        lambda c: c.execute(
            "SELECT job_key FROM jobs WHERE type = 'day_close' AND site_id = ?",
            (site_id,),
        ).fetchall()
    )
    return [row[0] for row in rows]


def _close_earlier_day(conn, site_id: int, local_date) -> None:  # type: ignore[no-untyped-def]
    """``begin_close``+``finish_close`` a day 30 minutes after its own
    end (the pattern ``day_close_site``'s own ``with_records`` path and
    ``tests/test_gap_fill.py``'s ``_close_earlier_days`` both use)."""
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    t = parse_utc(row["day_end_utc"]) + timedelta(minutes=30)
    begin_close(conn, site_id, local_date, now_utc=note_day_clock(t))
    finish_close(conn, site_id, local_date, now_utc=t)


# ---------------------------------------------------------------------------
# TL1
# ---------------------------------------------------------------------------


def test_tl1_attempt_starts_only_once_the_round_settles(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TL1: the round of R settles at R 00:20Z; ticks at R 00:00Z, 00:10Z,
    00:20Z. No attempt before 00:20Z; at 00:20Z ``begin_close`` (attempt
    1), stage 1's log row with ``started_at`` D's end and ``ended_at``
    00:20Z, one ``day_close`` job keyed ``close:{S}:{D}:2`` (5.1, 8.1).

    M13 -> at the 00:20Z assertion below: correct = ('closing', 1) (the
    attempt starts once the round settles, 20 minutes before its bound),
    mutant M13 (the first attempt starts on the round's bound only,
    ignoring 'settled') = ('open', 0), since the mutant would wait until
    00:40Z regardless of settlement.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)

        round_state.set_answer(site_id, R, settled=False, bound_utc=BOUND)
        await tick(db, clock, _r_at(0, 0))
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("open", 0), (
            "no attempt may exist before the round settles or its bound"
            " passes (tick at R 00:00Z, unsettled, bound 40 minutes off)"
        )

        await tick(db, clock, _r_at(0, 10))
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("open", 0), (
            "still no attempt (tick at R 00:10Z, still unsettled, bound 30 minutes off)"
        )

        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        await tick(db, clock, _r_at(0, 20))
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("closing", 1), (
            "M13 -> at this assertion (tick at R 00:20Z, now settled, bound"
            " still 20 minutes off): correct = ('closing', 1), mutant M13 ="
            " ('open', 0)"
        )

        stage1 = await _stage1_row(db, site_id)
        assert stage1 is not None, "stage 1's own stage-log row (8.1) was never written"
        assert stage1["job_id"] is None, (
            "stage 1 has no job_id (8.1: stage 1 = job_id NULL)"
        )
        d_end_row = await db.read(
            lambda c: c.execute(
                "SELECT day_end_utc FROM site_days"
                " WHERE site_id = ? AND local_date = ?",
                (site_id, D.isoformat()),
            ).fetchone()
        )
        assert stage1["started_at"] == d_end_row["day_end_utc"], (
            "stage 1's started_at must be D's own day_end_utc (4.1: the round"
            " starts just after D's end)"
        )
        assert stage1["ended_at"] == isoformat_utc_micro(_r_at(0, 20)), (
            "stage 1's ended_at must be the instant it actually settled, not"
            " the bound or some other time (plan 12.3: every stamp P3 writes"
            " uses isoformat_utc_micro)"
        )

        keys = await _day_close_job_keys(db, site_id)
        assert keys == [f"close:{site_id}:{D.isoformat()}:2"], (
            "exactly one day_close job, keyed close:{site}:{date}:2 (8.1)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TL2
# ---------------------------------------------------------------------------


def test_tl2_bound_starts_the_attempt_when_the_round_never_settles(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TL2: the round never settles; ticks at R 00:39Z and 00:40Z. The
    attempt starts at 00:40Z, not before (5.1, :154).

    M14 -> at the 00:40Z assertion below: correct = ('closing', 1) (the
    bound alone starts the attempt once reached, even though 'settled'
    stays False), mutant M14 (the first attempt starts on 'settled'
    only, ignoring the bound) = ('open', 0), since the mutant would wait
    forever for a round that never settles.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=False, bound_utc=BOUND)

        await tick(db, clock, _r_at(0, 39))
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("open", 0), (
            "no attempt may fire a minute before the bound (R 00:39Z, unsettled)"
        )

        await tick(db, clock, _r_at(0, 40))
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("closing", 1), (
            "M14 -> at this assertion (R 00:40Z, the bound exactly, still"
            " unsettled): correct = ('closing', 1), mutant M14 = ('open', 0)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TL3
# ---------------------------------------------------------------------------


def test_tl3_close_waits_behind_a_due_wind_lane_job(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TL3: a ``fetch_obs`` ``wind-days`` job of S is ``pending`` and due
    at R 00:40Z. No attempt at that tick; the wind job is claimed; the
    attempt starts at the next tick (5.1).

    M15 -> at the first assertion below: correct = ('open', 0) (the
    attempt does not start while the due wind-lane job is still
    pending), mutant M15 (the wind-lane job check removed) = ('closing',
    1).
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)
        wind_calls = stub_wind_days(monkeypatch)

        clock.set(BOUND)
        await db.write(
            lambda c: enqueue_if_absent(c, "fetch_obs", site_id, "wind-days", {})
        )

        await tick(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("open", 0), (
            "M15 -> at this assertion: correct = ('open', 0) (a pending, due"
            " wind-lane job holds off the first attempt), mutant M15 (the"
            " wind-lane job check removed) = ('closing', 1)"
        )

        # The row says only that the attempt STARTS at the next tick (5.1),
        # not that it runs to completion -- stop once stage 2's job is
        # pending, so this test never reaches stage 5's close_p4.score_day
        # (install_p4_standins above is the safety net if it ever did).
        claims = await run_due_jobs(db, clock, stop_before=("day_close", 2))
        types_keys = [(t, k) for t, k, _outcome in claims]
        assert ("fetch_obs", "wind-days") in types_keys, (
            "the due wind-lane job must be claimed before the attempt starts"
        )
        assert wind_calls == [site_id], "the wind-lane stub must have run exactly once"

        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("closing", 1), (
            "once the wind-lane job is out of the way, the next tick starts"
            " the attempt (5.1)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TL4
# ---------------------------------------------------------------------------


def test_tl4_days_are_attempted_oldest_first(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TL4: D - 1 still ``retry_due`` at R 00:40Z (``BOUND``), its retry
    now due within ``CLOSE_LATE_LIMIT`` (1h) of the tick, so the "running
    when due" rule of 5.2 does not itself fire here. No attempt for D
    until D - 1 is done (P1 §4.5).

    M16 -> at the assertion below: correct = D - 1 moves to ``closing``
    (attempt 2) at this tick, its retry now due and D - 1 being the
    site's oldest not-done day, while D stays ``open`` at 0 attempts
    (D has ended by ``BOUND`` and is blocked only because D - 1 is not
    done, P1 §4.5), mutant M16 (the tick takes the site's newest
    not-done day instead of its oldest, so ``begin_close`` is tried on D
    instead and refused) = D - 1 stays ``retry_due`` at attempt 1, its
    retry never attempted, while D is unchanged too.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        d_minus_1 = D - timedelta(days=1)
        # Undo standard_night's D-1 closure: put it back to closing/attempt 1,
        # then fail it 1h30m before BOUND, so its retry_at (failure +
        # RETRY_DELAY, 1h) lands 30 minutes before BOUND -- inside the hour
        # the "running when due" rule (5.2) allows. standard_night's
        # worker_started_at is NOW (D 12:00Z), before retry_at, so the
        # add-on counts as running.
        failed_at = note_day_clock(BOUND - timedelta(minutes=90))
        await db.write(
            lambda c: c.execute(
                "UPDATE site_days SET state = 'closing', attempts = 1, done_at = NULL"
                " WHERE site_id = ? AND local_date = ?",
                (site_id, d_minus_1.isoformat()),
            )
        )
        await db.write(
            lambda c: fail_close(
                c, site_id, d_minus_1, now_utc=failed_at, reason="network"
            )
        )
        before = await _state(db, site_id, d_minus_1)
        assert (before.state, before.attempts) == ("retry_due", 1), (
            "D-1 must start this test retry_due at attempt 1"
        )

        # Tick at R's bound (plan 5.1): D-1's retry_at is BOUND - 30m, within
        # CLOSE_LATE_LIMIT (1h) of this tick, so the "running when due" rule
        # (5.2) lets the retry be attempted -- it is this case's oldest
        # not-done day.
        await tick(db, clock, BOUND)

        d1_after = await _state(db, site_id, d_minus_1)
        d_after = await _state(db, site_id)
        assert (d1_after.state, d1_after.attempts) == ("closing", 2), (
            "M16 -> at this assertion: correct = ('closing', 2) (D-1's retry,"
            " now due, is attempted this tick, oldest day first), mutant M16"
            " = ('retry_due', 1), unchanged, since the mutant tries D instead"
            " and never reaches D-1"
        )
        assert (d_after.state, d_after.attempts) == ("open", 0), (
            "D itself must stay blocked behind D-1 regardless (P1 §4.5)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TL5
# ---------------------------------------------------------------------------


def test_tl5_sites_are_enqueued_in_due_instant_order(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TL5: two sites, S1 in ``Etc/GMT-14`` and S2 in ``UTC``, both due.
    S1's attempt at its own bound, S2's at its own; at a tick where both
    are due, S1's job is created first when its due instant is earlier
    (4.5, 8.5).

    S1's own round bound is set a few minutes earlier than S2's (the
    round date's bound is whatever ``round_state``'s stand-in answers,
    independent of either site's real timezone math), the smallest gap
    that still orders S1 before S2 -- any larger gap risks ticking more
    than ``CLOSE_LATE_LIMIT`` (1h) after S1's own due instant, which
    would itself trip the "running when due" rule of 5.2 and strand S1
    unattempted instead of testing the ordering this case is about.

    Built so the GMT-14 site gets the HIGHER site id, deliberately
    opposite of its due-instant order -- the only arrangement where
    "ordered by due instant" and "ordered by site id" disagree, so M17
    can only pass against the correct rule and must fail against its
    own.

    M17 -> at the ordering assertion below: correct = the GMT-14 site's
    ``day_close`` job created before the UTC site's (its due instant is
    earlier), mutant M17 (sites ordered by site id only) = the UTC
    site's job created first instead, since the UTC site has the LOWER
    id here.
    """

    async def _run() -> None:
        db, _writer, site_utc, _st = standard_night(tmp_path)
        conn = db._conn  # noqa: SLF001

        site_gmt14 = day_close_site(
            conn,
            timezone="Etc/GMT-14",
            close_on=True,
            with_records=True,
            station_ids=("ISTATION03", "ISTATION04"),
        )
        assert site_gmt14 > site_utc, (
            "the GMT-14 site must get the HIGHER id for this test to discriminate"
        )
        _close_earlier_day(conn, site_gmt14, D - timedelta(days=1))
        conn.commit()

        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)

        bound_gmt14 = BOUND - timedelta(minutes=10)
        round_state.set_answer(site_gmt14, R, settled=True, bound_utc=bound_gmt14)
        round_state.set_answer(site_utc, R, settled=True, bound_utc=BOUND)

        # One tick at the UTC site's (later) bound: by then both bounds have
        # passed, so both sites are due at this single tick.
        await tick(db, clock, BOUND)

        record_gmt14 = await _state(db, site_gmt14)
        record_utc = await _state(db, site_utc)
        assert (record_gmt14.state, record_gmt14.attempts) == ("closing", 1), (
            "the GMT-14 site's attempt must have started"
        )
        assert (record_utc.state, record_utc.attempts) == ("closing", 1), (
            "the UTC site's attempt must have started too"
        )

        jobs = await db.read(
            lambda c: c.execute(
                "SELECT site_id, id FROM jobs WHERE type = 'day_close' ORDER BY id"
            ).fetchall()
        )
        ids_by_site = {row["site_id"]: row["id"] for row in jobs}
        assert ids_by_site[site_gmt14] < ids_by_site[site_utc], (
            "M17 -> at this assertion: correct = the GMT-14 site's day_close"
            " job created before the UTC site's, mutant M17 = the UTC site's"
            " job created first instead"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TL6
# ---------------------------------------------------------------------------


def test_tl6_a_changeover_day_is_missed_not_scored(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TL6: D is a changeover day (P1 §6.4); ticks to R 01:00Z. At its
    due instant D is ``missed``, ``zone_changeover``; no ``day_close``
    job; D + 1 is closed as usual the next night (4.4).

    M18 -> at the assertion below: correct = ('missed',
    'zone_changeover') with no day_close job, mutant M18 (a changeover
    day given an attempt) = ('closing', 1) with a day_close job created.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        # The tick's due-instant read (_site_due_instant, plan 4.5/8.5) asks
        # close_p4.round_state for the round's bound even for a changeover
        # day -- without a stand-in the real, unimplemented round_state
        # raises, the per-site savepoint rolls the whole tick back, and D
        # never moves (plan 173, 219).
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        await db.write(
            lambda c: c.execute(
                "UPDATE site_days SET zone_changeover = 1"
                " WHERE site_id = ? AND local_date = ?",
                (site_id, D.isoformat()),
            )
        )

        await tick(db, clock, _r_at(1, 0))
        record = await _state(db, site_id)
        assert (record.state, record.missed_reason) == (
            "missed",
            "zone_changeover",
        ), (
            "M18 -> at this assertion: correct = ('missed', 'zone_changeover')"
            " (a changeover day is moved to missed at its first due instant,"
            " never attempted), mutant M18 (a changeover day given an"
            " attempt) = ('closing', None)"
        )
        keys = await _day_close_job_keys(db, site_id)
        assert keys == [], (
            "no day_close job of any stage may exist for a changeover day"
            " (4.4: 'with no attempt')"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TL7
# ---------------------------------------------------------------------------


def _seed_r_record_sample(conn, site_id, local_date) -> None:  # type: ignore[no-untyped-def]
    """One feed, one full day of hourly temperature samples for ``local_date``
    (copied and trimmed to a single variable from
    ``tests/test_forecast_record.py``'s ``_make_feed``/``_insert_temp_day``):
    the 07:00 record needs an actual candidate on hand to write a saved row,
    not just a published generation -- an empty day writes zero rows
    (``tests/test_record_oracles.py::test_zero_sample_day_writes_no_rows``).
    """
    cur = conn.execute(
        "INSERT INTO feeds (source, model, default_subscribed,"
        " fetch_interval_minutes, max_lead_hours)"
        " VALUES ('example-src', 'tl7-model', 1, 360, 192)"
    )
    assert cur.lastrowid is not None
    feed_id = int(cur.lastrowid)
    issued = datetime(local_date.year, local_date.month, local_date.day, 6, tzinfo=UTC)
    issued_at = isoformat_utc(issued)
    fetched_at = isoformat_utc(issued + timedelta(minutes=5))
    for hour in range(24):
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        lead = max(1, int((valid - issued).total_seconds() // 3600))
        conn.execute(
            "INSERT INTO forecast_samples"
            " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
            " value, source_raw, model_run_id, fetched_at)"
            " VALUES (?, ?, 'temperature', ?, ?, ?, 10.0, '{}', 'run-tl7', ?)",
            (site_id, feed_id, issued_at, isoformat_utc(valid), lead, fetched_at),
        )


async def _forecast_record_status(db, site_id, local_date):  # type: ignore[no-untyped-def]
    # Keyed to R's own job (production key "record:{local_date.isoformat()}",
    # written by _enqueue_due_forecast_records in wxverify/worker/scheduler.py)
    # -- a bare first-row read would pick up any forecast_record job for the
    # site, not R's. At most one row is expected for this key (the due-check
    # is suppressed by its own success/failure cooldown once a row exists);
    # asserting that keeps the read honest rather than silently picking one.
    job_key = f"record:{local_date.isoformat()}"
    rows = await db.read(
        lambda c: c.execute(
            "SELECT status FROM jobs"
            " WHERE type = 'forecast_record' AND site_id = ? AND job_key = ?",
            (site_id, job_key),
        ).fetchall()
    )
    assert len(rows) <= 1, f"expected at most one forecast_record job keyed {job_key!r}"
    return None if not rows else str(rows[0]["status"])


def test_tl7_the_0700_record_runs_independently_of_a_blocked_chain(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TL7: the chain blocked at stage 2 (``run_due_jobs`` stops before
    claiming it); tick at R 07:00Z. Right after the tick, D is still
    ``closing`` and R's ``forecast_record`` job is ``pending`` (M101: it
    must not be skipped for a site with a ``closing`` day). After
    ``run_due_jobs`` runs the queue to empty, that job is ``completed`` and
    R's record row is saved. The stage-2 job, older in tier 0, is claimed
    first and fails the attempt as ``stalled`` (8.3, ``CLOSE_LATE_LIMIT`` of
    1 hour against the 7 h gap to 07:00) before ever reaching ``fill_gaps``
    -- expected, and not asserted beyond D leaving ``closing`` (8.7, Q5).
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch, keep=("_enqueue_due_forecast_records",))
        await db.write(lambda c: _seed_r_record_sample(c, site_id, R))
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        clock.set(BOUND)
        await run_due_jobs(db, clock, stop_before=("day_close", 2))
        record = await _state(db, site_id)
        assert record.state == "closing", "the attempt must have reached stage 2"

        await tick(db, clock, _r_at(7, 0))
        record = await _state(db, site_id)
        assert record.state == "closing", (
            "D must still be mid-attempt right after the tick -- the stage-2"
            " job is only held open by the clock gap to stage 2's claim, not"
            " claimed and resolved by this tick"
        )
        status = await _forecast_record_status(db, site_id, R)
        assert status == "pending", (
            "M101 -> at this assertion: correct = 'pending' (the 07:00"
            " record is enqueued for R even though D's day_close is"
            " 'closing'), mutant M101 (skips a site while one of its days is"
            " 'closing') = None (no forecast_record job enqueued at all)"
        )

        await run_due_jobs(db, clock)
        status = await _forecast_record_status(db, site_id, R)
        assert status == "completed", (
            "the 07:00 record job must run to completion once it is claimed,"
            " independently of the still-unresolved close chain"
        )
        record_rows = await db.read(
            lambda c: c.execute(
                "SELECT COUNT(*) AS n FROM forecast_of_record"
                " WHERE site_id = ? AND snapshot_local_date = ?",
                (site_id, R.isoformat()),
            ).fetchone()
        )
        assert record_rows is not None and int(record_rows["n"]) > 0, (
            "R's forecast-of-record row must be saved by persist_forecast_record"
            " once the job completes"
        )
        record = await _state(db, site_id)
        assert record.state != "closing", (
            "D must leave 'closing' once its stage-2 job is claimed and found"
            " stalled (8.3); not asserted beyond that it leaves 'closing'"
            " (8.7, Q5)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TL8
# ---------------------------------------------------------------------------


def test_tl8_close_kinds_claim_ahead_of_an_older_tier1_job(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TL8: a ``fetch_feed`` job queued before the attempt starts, so it
    is older than every ``day_close`` and ``day_publish`` job;
    ``run_due_jobs``. Claims in order: ``day_close`` stages 2 to 6,
    ``day_publish`` stages 7 and 8, then the ``fetch_feed`` job, which
    runs through the stub of 15.1 (tier 0, 4.3); after the run, no
    ``day_close`` or ``day_publish`` job is in tier 2.

    M19/M19b -> at the ordering assertion below: correct = every claim
    of type ``day_close``/``day_publish`` comes before the ``fetch_feed``
    claim (tier 0 beats tier 1 regardless of age), mutant M19
    (``day_close`` left out of tier 0) or M19b (``day_publish`` left
    out) = that close kind falls to tier 1 and loses the FIFO race to
    the OLDER ``fetch_feed`` job, putting its claim AFTER fetch_feed's.

    Explicitly NOT killed here: M20/M20b (``day_close``/``day_publish``
    added to ``_CHAIN_TYPES_SQL``) change only reclaim-stale tier-2
    promotion, which this oracle's final "no tier-2 row" read does cover
    -- see that assertion.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)
        fetch_feed_calls = stub_fetch_feed(monkeypatch)

        feed_row = await db.read(
            lambda c: c.execute("SELECT id FROM feeds ORDER BY id LIMIT 1").fetchone()
        )
        assert feed_row is not None, "run_migrations must have seeded at least one feed"
        feed_id = int(feed_row[0])

        await db.write(clock.prime)
        await db.write(
            lambda c: enqueue_if_absent(
                c, "fetch_feed", site_id, f"feed:{feed_id}", {"feed_id": feed_id}
            )
        )
        await tick(db, clock)  # stamps the seeded job to NOW, before the round bound

        clock.set(BOUND)
        claims = await run_due_jobs(db, clock)

        claim_types = [t for t, _k, _o in claims]
        fetch_feed_index = claim_types.index("fetch_feed")
        close_indices = [
            i for i, t in enumerate(claim_types) if t in ("day_close", "day_publish")
        ]
        assert close_indices, "the close chain must have claimed at least one job"
        assert max(close_indices) < fetch_feed_index, (
            "M19/M19b -> at this assertion: correct = every day_close/"
            "day_publish claim index is below fetch_feed's, mutant M19/M19b ="
            " the close kind's index is ABOVE fetch_feed's instead"
        )
        assert fetch_feed_calls == [(site_id, feed_id)], (
            "the fetch_feed stub must have run exactly once, never the real"
            " handler (which would trip the network guard)"
        )

        record = await _state(db, site_id)
        assert record.state == "closed", (
            "the standard night's attempt must have closed D"
        )

        stale_tier2 = await db.read(
            lambda c: c.execute(
                "SELECT 1 FROM jobs WHERE type IN ('day_close', 'day_publish')"
                " AND next_attempt_at IS NOT NULL AND next_attempt_at != updated_at"
            ).fetchone()
        )
        assert stale_tier2 is None, (
            "M20/M20b -> at this assertion: correct = no day_close/day_publish"
            " row has a next_attempt_at moved off its own updated_at by"
            " reclaim_all_stale's tier-2 handling (they are never chain"
            " types, 4.3), mutant M20/M20b (either kind added to"
            " _CHAIN_TYPES_SQL) = such a row exists after a stale reclaim --"
            " not exercised in THIS run (nothing here goes stale), so this"
            " assertion does not kill M20/M20b; it only confirms the"
            " non-stale path leaves no trace, consistent with either verdict"
        )

    asyncio.run(_run())


def test_tl8_day_close_and_day_publish_claim_ahead_of_an_older_job_directly(
    tmp_path,
) -> None:
    """TL8 extension: a direct, minimal claim-order check against
    ``claim_next_job`` alone, isolated from the chain's own machinery.
    TL8 above drives the full chain through ``run_due_jobs``, with many
    moving parts (round-state stand-ins, gap-fill, repeated ticks); the
    main thread's actual run of that test against M19/M19b showed it
    does not reliably kill them. Here, two jobs only: an OLDER
    ``fetch_feed`` row and a NEWER ``day_close`` row (then, separately,
    ``day_publish``) -- real tier-0 membership must win over FIFO age
    regardless of anything else in the chain.

    M19 -> at the first claim assertion below: correct = "day_close"
    (tier 0 beats a FIFO-older tier-1 job), mutant M19 (day_close left
    out of the tier-0 CASE, ``db/queue.py:277``) = "fetch_feed" (day_close
    falls to tier 1 and loses the FIFO race to the older row).

    M19b -> at the second claim assertion below: correct = "day_publish",
    mutant M19b (day_publish left out of the tier-0 CASE) = "fetch_feed".
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)

        feed_row = await db.read(
            lambda c: c.execute("SELECT id FROM feeds ORDER BY id LIMIT 1").fetchone()
        )
        assert feed_row is not None, "run_migrations must have seeded at least one feed"
        feed_id = int(feed_row[0])

        older = isoformat_utc_micro(NOW)
        newer = isoformat_utc_micro(NOW + timedelta(minutes=10))

        def _seed_close(c):  # type: ignore[no-untyped-def]
            c.execute(
                "INSERT INTO jobs (type, site_id, job_key, status, created_at,"
                " updated_at) VALUES ('fetch_feed', ?, ?, 'pending', ?, ?)",
                (site_id, f"feed:{feed_id}", older, older),
            )
            c.execute(
                "INSERT INTO jobs (type, site_id, job_key, payload, status,"
                " max_retries, created_at, updated_at) VALUES ('day_close', ?,"
                " ?, '{}', 'pending', 0, ?, ?)",
                (site_id, f"close:{site_id}:{D.isoformat()}:2", newer, newer),
            )

        await db.write(_seed_close)
        job = await db.write(claim_next_job)
        assert job is not None and job.type == "day_close", (
            "M19 -> at this assertion: correct = 'day_close' (tier 0 beats"
            " the FIFO-older fetch_feed row), mutant M19 (day_close left"
            " out of the tier-0 CASE) = 'fetch_feed'"
        )

        await db.write(lambda c: c.execute("DELETE FROM jobs"))

        def _seed_publish(c):  # type: ignore[no-untyped-def]
            c.execute(
                "INSERT INTO jobs (type, site_id, job_key, status, created_at,"
                " updated_at) VALUES ('fetch_feed', ?, ?, 'pending', ?, ?)",
                (site_id, f"feed:{feed_id}", older, older),
            )
            c.execute(
                "INSERT INTO jobs (type, site_id, job_key, payload, status,"
                " max_retries, created_at, updated_at) VALUES ('day_publish',"
                " ?, ?, '{}', 'pending', 0, ?, ?)",
                (site_id, f"publish:{site_id}:7", newer, newer),
            )

        await db.write(_seed_publish)
        job2 = await db.write(claim_next_job)
        assert job2 is not None and job2.type == "day_publish", (
            "M19b -> at this assertion: correct = 'day_publish', mutant"
            " M19b (day_publish left out of the tier-0 CASE) = 'fetch_feed'"
        )

    asyncio.run(_run())


def test_tl8_reclaim_all_stale_leaves_close_kinds_next_attempt_at_alone(
    tmp_path,
) -> None:
    """TL8 extension: ``reclaim_all_stale`` must never touch a stuck
    ``day_close``/``day_publish`` job's ``next_attempt_at`` -- they are
    not chain types (4.3), so a kill left behind by a restart simply
    re-pends with its stamp untouched, never promoted into tier 2's
    stale-chain handling. TL8's own end-of-run "no stale tier-2 row"
    read is explicit that it cannot tell this apart from the real code,
    since nothing in that run actually goes stale; this test reclaims a
    job left ``running`` directly.

    M20 -> at the day_close assertion below: correct = ``next_attempt_at
    IS NULL`` (day_close is not in ``_CHAIN_TYPES_SQL``,
    ``db/queue.py:264``, so ``reclaim_all_stale``'s CASE leaves it
    alone), mutant M20 (day_close added to ``_CHAIN_TYPES_SQL``) =
    ``next_attempt_at`` is set to the reclaim's "now" stamp.

    M20b -> at the day_publish assertion below: correct = ``next_attempt_at
    IS NULL``, mutant M20b (day_publish added to ``_CHAIN_TYPES_SQL``) =
    set.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        stamp = isoformat_utc_micro(NOW)

        def _seed(c):  # type: ignore[no-untyped-def]
            c.execute(
                "INSERT INTO jobs (type, site_id, job_key, payload, status,"
                " max_retries, created_at, updated_at) VALUES ('day_close', ?,"
                " ?, '{}', 'running', 0, ?, ?)",
                (site_id, f"close:{site_id}:{D.isoformat()}:4", stamp, stamp),
            )
            c.execute(
                "INSERT INTO jobs (type, site_id, job_key, payload, status,"
                " max_retries, created_at, updated_at) VALUES ('day_publish',"
                " ?, ?, '{}', 'running', 0, ?, ?)",
                (site_id, f"publish:{site_id}:7", stamp, stamp),
            )

        await db.write(_seed)
        await db.write(reclaim_all_stale)

        rows = await db.read(
            lambda c: c.execute(
                "SELECT type, status, next_attempt_at FROM jobs WHERE type IN"
                " ('day_close', 'day_publish') ORDER BY type"
            ).fetchall()
        )
        by_type = {row["type"]: (row["status"], row["next_attempt_at"]) for row in rows}
        # Paired positive: both rows must actually have been reclaimed
        # (status 'running' -> 'pending') -- otherwise the next_attempt_at
        # IS NULL checks below would also pass on a reclaim that never
        # touched these rows at all, an ambient coincidence rather than a
        # real check of _CHAIN_TYPES_SQL's own exclusion.
        assert by_type["day_close"][0] == "pending", (
            "reclaim_all_stale must still re-pend a stuck day_close job"
            " (it is a stuck 'running' job like any other, just not a"
            " chain type) -- otherwise the next_attempt_at IS NULL check"
            " below would hold vacuously"
        )
        assert by_type["day_publish"][0] == "pending", (
            "reclaim_all_stale must still re-pend a stuck day_publish job,"
            " for the same reason"
        )
        assert by_type["day_close"][1] is None, (
            "M20 -> at this assertion: correct = None (day_close is not a"
            " chain type, db/queue.py:264), mutant M20 (day_close added to"
            " _CHAIN_TYPES_SQL) = the reclaim's 'now' stamp"
        )
        assert by_type["day_publish"][1] is None, (
            "M20b -> at this assertion: correct = None, mutant M20b"
            " (day_publish added to _CHAIN_TYPES_SQL) = the reclaim's"
            " 'now' stamp"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TL9
# ---------------------------------------------------------------------------


def test_tl9_a_retention_failure_does_not_undo_the_ticks_moves(
    tmp_path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """TL9: AT4's start (``worker_started_at`` after the bound, so D is
    never attempted), with P2's ``prune_raw_readings`` raising a
    ``RuntimeError`` where ``close_chain`` calls it. The first tick's
    write raises nothing; D ``missed``, ``never_attempted``, read back
    on a second connection; one ERROR line naming ``RuntimeError`` (8.4,
    8.5).

    M75 -> at the assertion below: correct = D read back as 'missed'/
    'never_attempted' on a FRESH connection (the tick's write committed
    despite the retention failure inside it), mutant M75
    (``run_close_retention`` without its catch, so a prune exception
    propagates) = the whole tick's write rolls back, so D is still
    'open' with 0 attempts on the fresh read.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        # D's own due instant still needs P4's bound even on this AT4-style
        # start (plan 173, 219): without a round_state stand-in the real,
        # unimplemented round_state raises, the per-site savepoint rolls
        # the tick back, and D never moves.
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        # AT4's start: worker_started_at AFTER the bound (standard_night's
        # own worker_started_at, NOW, is before the bound, so it must be
        # overridden here) -- the first attempt's due instant (the bound)
        # falls while the add-on is not yet started (5.2): D is missed,
        # never_attempted, with no gap-fill call at all.
        late_start = BOUND + timedelta(minutes=61)
        await db.write(
            lambda c: c.execute(
                "INSERT INTO runtime_state (key, value) VALUES"
                " ('worker_started_at', ?)"
                " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (isoformat_utc(late_start),),
            )
        )

        def _raising_prune(conn: object, *, now_utc: object) -> None:
            raise RuntimeError("synthetic prune failure for TL9")

        # close_chain.py imports prune_raw_readings BY NAME (14.4: "P3 calls
        # ... prune_raw_readings"), so the patch targets close_chain's own
        # name.
        monkeypatch.setattr(
            "wxverify.worker.close_chain.prune_raw_readings",
            _raising_prune,
        )

        caplog.set_level("ERROR", logger="wxverify.worker.close_chain")
        await tick(db, clock, late_start)

        conn2 = await db.read(lambda c: c)
        row = conn2.execute(
            "SELECT state, missed_reason FROM site_days"
            " WHERE site_id = ? AND local_date = ?",
            (site_id, D.isoformat()),
        ).fetchone()
        assert (row["state"], row["missed_reason"]) == (
            "missed",
            "never_attempted",
        ), (
            "M75 -> at this assertion: correct = ('missed', 'never_attempted'),"
            " committed despite the retention failure inside the same write,"
            " mutant M75 (run_close_retention without its catch) = the write"
            " rolls back entirely, leaving ('open', None)"
        )
        assert any(
            "RuntimeError" in rec.message and rec.levelname == "ERROR"
            for rec in caplog.records
        ), "one ERROR line naming RuntimeError must be written (8.4)"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TL10
# ---------------------------------------------------------------------------


def test_tl10_one_sites_fault_does_not_stop_the_others(
    tmp_path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """TL10: TL5's two sites at a tick where both are due, S1's due
    instant the earlier; the ``round_state`` stand-in raises a
    ``RuntimeError`` on every call for S1; a second tick 10 minutes
    later. S2's attempt begins at the first tick (``begin_close``, one
    ``day_close`` job); after both ticks S1's day records unchanged and
    no ``day_close`` job of S1; one ERROR line naming S1 and step 3
    after the first tick, none added by the second (8.5).

    M74 -> at the per-site-isolation assertion below: correct = S2's
    attempt still begins even though S1's round_state call raised,
    mutant M74 (``enqueue_due_close_work`` without its per-site
    savepoint, so one site's exception fails the tick) = S2 gets NO
    attempt either (the whole tick's write fails).
    """

    async def _run() -> None:
        db, _writer, site1, _st1 = standard_night(tmp_path)
        conn = db._conn  # noqa: SLF001
        site2 = day_close_site(
            conn,
            timezone="UTC",
            close_on=True,
            with_records=True,
            station_ids=("ISTATION03", "ISTATION04"),
        )
        _close_earlier_day(conn, site2, D - timedelta(days=1))
        conn.commit()

        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site2, R, settled=True, bound_utc=BOUND)
        round_state.raise_for_site(
            site1, RuntimeError("synthetic round-state fault for S1")
        )

        before_s1 = await _state(db, site1)

        caplog.set_level("ERROR", logger="wxverify.worker.close_chain")
        await tick(db, clock, BOUND)

        record2 = await _state(db, site2)
        assert (record2.state, record2.attempts) == ("closing", 1), (
            "M74 -> at this assertion: correct = S2's attempt begins at this"
            " tick despite S1's round_state call raising, mutant M74"
            " (enqueue_due_close_work without its per-site savepoint) = S2 is"
            " also left ('open', 0), since the whole tick's write would fail"
            " and roll back together with S1's"
        )

        after_s1_first = await _state(db, site1)
        assert (after_s1_first.state, after_s1_first.attempts) == (
            before_s1.state,
            before_s1.attempts,
        ), "S1's own day record must be unchanged by its own fault"
        s1_jobs = await db.read(
            lambda c: c.execute(
                "SELECT 1 FROM jobs WHERE type = 'day_close' AND site_id = ?",
                (site1,),
            ).fetchone()
        )
        assert s1_jobs is None, "no day_close job of S1 may exist after its fault"

        error_lines_after_first = [
            rec for rec in caplog.records if rec.levelname == "ERROR"
        ]
        assert len(error_lines_after_first) == 1, (
            "exactly one ERROR line after the first tick naming S1's fault"
        )
        assert str(site1) in error_lines_after_first[0].message, (
            "the ERROR line must name the site"
        )
        assert "3" in error_lines_after_first[0].message, (
            "the ERROR line must name the step (round_state is read in step"
            " 3 of enqueue_due_close_work, 8.5)"
        )

        caplog.clear()
        await tick(db, clock, BOUND + timedelta(minutes=10))
        error_lines_after_second = [
            rec for rec in caplog.records if rec.levelname == "ERROR"
        ]
        assert error_lines_after_second == [], (
            "the flood guard (8.5's log map) must suppress a second ERROR"
            " line for the same site and step inside the same hour"
        )

    asyncio.run(_run())


def test_tl10_a_second_loop_fault_does_not_fail_the_whole_tick(
    tmp_path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """TL10 extension: TL10 above puts S1's fault in ``round_state``,
    which ``_site_due_instant`` reaches in ``enqueue_due_close_work``'s
    FIRST per-site loop (8.5) -- a loop M74 never touches (M74 drops the
    savepoint only from the SECOND loop, ``close_chain.py:2074``, where
    ``_site_steps`` runs). S1 is excluded from the ``order`` list already
    in that first, still-protected loop, so its fault never reaches the
    loop M74 breaks, and the mutant survives TL10 as written.

    This test puts the fault in the second loop instead: S1 gets a
    ``closing`` ``site_days`` row with NO matching ``close_attempts``
    row, so step 4's repair (``_repair``, plan 8.2) raises
    ``RuntimeError("the closing attempt has no attempt record")`` from
    inside ``_site_steps``. ``_site_due_instant`` never touches
    ``close_attempts`` for a ``closing`` day (its only branches are
    ``open``/``retry_due``), so S1 still clears the first loop cleanly
    and reaches the second, where M74's missing savepoint matters.

    M74 -> at the per-site-isolation assertion below: correct = S2's
    attempt still begins despite S1's repair exception, mutant M74
    (``enqueue_due_close_work`` without its per-site savepoint in the
    SECOND loop) = the exception propagates out of the tick (``_conn``'s
    ``_run_immediate`` rolls back the whole transaction on any
    uncaught exception) and S2 is left ('open', 0) too.
    """

    async def _run() -> None:
        db, _writer, site2, _st2 = standard_night(tmp_path)
        conn = db._conn  # noqa: SLF001
        site1 = day_close_site(
            conn,
            timezone="UTC",
            close_on=True,
            with_records=False,
            station_ids=("ISTATION03", "ISTATION04"),
        )
        bounds = local_day_bounds(D, "UTC")
        now = datetime(D.year, D.month, D.day, 23, 0, tzinfo=UTC)
        conn.execute(
            "INSERT INTO site_days (site_id, local_date, state, day_start_utc,"
            " day_end_utc, timezone, attempts, created_at, state_changed_at)"
            " VALUES (?, ?, 'closing', ?, ?, 'UTC', 1, ?, ?)",
            (
                site1,
                D.isoformat(),
                isoformat_utc(bounds.start_utc),
                isoformat_utc(bounds.end_utc),
                isoformat_utc(now),
                isoformat_utc(now),
            ),
        )
        conn.commit()

        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site2, R, settled=True, bound_utc=BOUND)

        before_s2 = await _state(db, site2)
        assert (before_s2.state, before_s2.attempts) == ("open", 0)

        caplog.set_level("ERROR", logger="wxverify.worker.close_chain")
        raised = False
        try:
            await tick(db, clock, BOUND)
        except RuntimeError:
            raised = True

        record2 = await _state(db, site2)
        assert (record2.state, record2.attempts) == ("closing", 1), (
            "M74 -> at this assertion: correct = S2's attempt begins at"
            " this tick despite S1's repair exception, mutant M74"
            " (enqueue_due_close_work without its per-site savepoint in"
            " the second loop) = S2 is also left ('open', 0), since the"
            f" whole tick's write rolls back together with S1's (tick"
            f" raised={raised})"
        )


# ---------------------------------------------------------------------------
# F1
# ---------------------------------------------------------------------------


def test_f1_stage5_duration_is_read_after_score_day_not_before(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F1: stage 5's ``close_stage_log.duration_s`` must be stamped from a
    fresh clock read taken AFTER ``close_p4.score_day`` runs, not from the
    stamp read before the stage's write began -- today's code reads
    ``now`` once, before calling ``score_day``, so ``duration_s`` always
    comes out 0 no matter how long the work actually took.

    The ``score_day`` stand-in itself advances this test's own
    ``MovingClock`` by 30 seconds before returning, standing in for stage
    5's own work taking 30 seconds.

    F1 -> at the duration_s assertion below: correct = 30.0 (the end
    stamp is a fresh read taken after score_day's 30-second advance),
    today's code (end stamp read before the work) = 0.0.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        standins = install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        def _score_and_advance_clock(*args, **kwargs):  # type: ignore[no-untyped-def]
            clock.advance(timedelta(seconds=30))
            return ScoreSummary(scored={}, absent={})

        standins["score_day"].result = _score_and_advance_clock
        standins["publish"].result = "pub-D"
        standins["clean_worksheets"].result = 0

        clock.set(BOUND)
        await run_due_jobs(db, clock, stop_before=("day_close", 6))

        row = await db.read(
            lambda c: c.execute(
                "SELECT duration_s FROM close_stage_log WHERE site_id = ?"
                " AND local_date = ? AND stage = 5",
                (site_id, D.isoformat()),
            ).fetchone()
        )
        assert row is not None, "stage 5's own stage-log row was never written"
        assert row["duration_s"] == 30.0, (
            "F1 -> at this assertion: correct = 30.0 (the end stamp is a"
            " fresh read taken after score_day's 30-second advance),"
            " today's code (end stamp read before the work) = 0.0"
        )

    asyncio.run(_run())
