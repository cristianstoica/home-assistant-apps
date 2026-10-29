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
    Stop,
    clear_close_chain_log_map,
    day_close_site,
    disable_other_enqueuers,
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
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.queue import enqueue_if_absent
from wxverify.db.site_days import begin_close, day_record, fail_close, finish_close


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
        assert stage1["ended_at"] == isoformat_utc(_r_at(0, 20)), (
            "stage 1's ended_at must be the instant it actually settled, not"
            " the bound or some other time"
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

        claims = await run_due_jobs(db, clock)
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
    """TL4: D - 1 still ``retry_due`` at R 00:40Z, its retry now due. No
    attempt for D until D - 1 is done (P1 §4.5).

    M16 -> at the assertion below: correct = D - 1 moves to ``closing``
    (attempt 2) at this tick, its retry now due and D - 1 being the
    site's oldest not-done day, while D stays ``open`` at 0 attempts
    (blocked behind D - 1), mutant M16 (the tick takes the site's newest
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
        # then fail it so it lands retry_due with its retry due well before
        # R 00:40Z (NOW + 1h, vs. a tick roughly 12.7h after NOW).
        await db.write(
            lambda c: c.execute(
                "UPDATE site_days SET state = 'closing', attempts = 1, done_at = NULL"
                " WHERE site_id = ? AND local_date = ?",
                (site_id, d_minus_1.isoformat()),
            )
        )
        await db.write(
            lambda c: fail_close(c, site_id, d_minus_1, now_utc=NOW, reason="network")
        )
        before = await _state(db, site_id, d_minus_1)
        assert (before.state, before.attempts) == ("retry_due", 1), (
            "D-1 must start this test retry_due at attempt 1"
        )

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

    Built so the GMT-14 site (whose due instant is 14 hours EARLIER in
    UTC) gets the HIGHER site id, deliberately opposite of its due-
    instant order -- the only arrangement where "ordered by due instant"
    and "ordered by site id" disagree, so M17 can only pass against the
    correct rule and must fail against its own.

    M17 -> at the ordering assertion below: correct = the GMT-14 site's
    ``day_close`` job created before the UTC site's (its due instant is
    14 hours earlier), mutant M17 (sites ordered by site id only) = the
    UTC site's job created first instead, since the UTC site has the
    LOWER id here.
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

        bound_gmt14 = BOUND - timedelta(hours=14)
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


def test_tl7_the_0700_record_runs_independently_of_a_blocked_chain(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TL7: the chain blocked at stage 2 (the stand-in for ``fill_gaps``
    waits); tick at R 07:00Z. The 07:00 record is enqueued and runs
    (8.7, Q5). Plan section 16.2 names no mutant for TL7.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch, keep=("_enqueue_due_forecast_records",))
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        async def _blocked_fill_gaps(*args: object, **kwargs: object) -> object:
            raise Stop("stage 2 held open for TL7")

        monkeypatch.setattr("wxverify.worker.day_values.fill_gaps", _blocked_fill_gaps)

        await run_due_jobs(db, clock, stop_before=("day_close", 2))
        record = await _state(db, site_id)
        assert record.state == "closing", "the attempt must have reached stage 2"

        await tick(db, clock, _r_at(7, 0))
        record_row = await db.read(
            lambda c: c.execute(
                "SELECT 1 FROM jobs WHERE type = 'forecast_record' AND site_id = ?"
                " AND status IN ('pending', 'running', 'completed')",
                (site_id,),
            ).fetchone()
        )
        assert record_row is not None, (
            "the 07:00 record must be enqueued at R 07:00Z even though the"
            " close chain is still stuck at stage 2 (8.7, Q5): the record"
            " never waits for the chain"
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

        # worker_started_at D 12:00Z (standard_night's NOW), bound R 00:40Z.
        # Starting the first tick well after the bound means the first
        # attempt fell due while the add-on was not running (AT4/5.2): D is
        # missed, never_attempted, with no gap-fill call at all.
        late_start = BOUND + timedelta(minutes=61)
        await db.write(
            lambda c: c.execute(
                "INSERT INTO runtime_state (key, value) VALUES"
                " ('worker_last_loop_at', ?)"
                " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (isoformat_utc(late_start),),
            )
        )

        def _raising_prune(conn: object, *, now_utc: object) -> None:
            raise RuntimeError("synthetic prune failure for TL9")

        # Patched at its defining module (db/station_readings.py). Once
        # close_chain.py exists and imports prune_raw_readings BY NAME (14.4:
        # "P3 calls ... prune_raw_readings"), the patch target may need to
        # move to wxverify.worker.close_chain.prune_raw_readings instead --
        # see the hand-back message.
        monkeypatch.setattr(
            "wxverify.db.station_readings.prune_raw_readings",
            _raising_prune,
            raising=False,
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
