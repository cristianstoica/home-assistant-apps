"""One attempt, one retry, missed days (plan 2026-10-21-daily-close-close-
job.md section 15.5): AT1 to AT17.

Written before ``close_chain.enqueue_due_close_work``, ``close_chain.
fail_attempt``, ``close_chain.settle_interrupted_closes`` and ``close_p4``
exist, and before ``verification.truth.write_close_truth`` exists. Every
test here reaches a call that needs one of them -- either directly (an
inline/top-level reference to a P3 name that is not there yet, raising
``ModuleNotFoundError`` or ``AttributeError``) or through
``tests.helpers.install_round_state_standin``/``install_p4_standins``, which
patch an attribute of ``wxverify.worker.close_p4`` -- a module that does not
exist yet. Where neither applies, the test instead fails on a plain
assertion that needs P3's behaviour: ``scheduler_tick`` has no
``daily_close_on`` branch to call ``enqueue_due_close_work`` from until
piece 4, so the moved-day, attempt or job-row check simply finds nothing
moved, started or enqueued.

Section 16.2 of the plan names the mutant each test ID kills; that mapping
is given inline on the exact assertion it kills at, in
``tests/test_close_timeline.py``'s own style.

Real SQLite only (``tests.helpers.standard_night``, built on
``tests.helpers.day_close_site``, real ``run_migrations`` via ``init_db``
on a ``tmp_path`` file). Station ids are synthetic (``ISTATION01`` to
``ISTATION04``), the only zone used is ``UTC``, and every date is the fixed
synthetic ``tests.helpers.D``/the derived round date ``R``. No live id,
coordinate, site name or real zone name appears.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from tests.helpers import (
    NOW,
    STANDARD_NIGHT_R,
    STANDARD_NIGHT_ROUND_BOUND,
    D,
    MovingClock,
    clear_close_chain_log_map,
    disable_other_enqueuers,
    disable_station_pacing,
    failed_scope_count,
    install_p4_standins,
    install_round_state_standin,
    note_day_clock,
    patch_close_clock,
    patch_weathercom_transport,
    run_due_jobs,
    standard_night,
    tick,
    write_weathercom_key,
)
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.queue import claim_next_job
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.site_days import day_record, ensure_site_days, start_site_days
from wxverify.worker import day_values
from wxverify.worker.day_values import GapFillResult, StationFill
from wxverify.worker.processor import run_claimed_job

R = STANDARD_NIGHT_R
BOUND = STANDARD_NIGHT_ROUND_BOUND


@pytest.fixture(autouse=True)
def _clear_close_chain_log_map() -> None:
    """This file's own autouse wiring of ``clear_close_chain_log_map``
    (15.1's "The log map.") -- autouse HERE, inside this one P3 test file,
    never across the whole suite."""
    clear_close_chain_log_map()


def _r_at(hour: int, minute: int) -> datetime:
    return datetime(R.year, R.month, R.day, hour, minute, tzinfo=UTC)


def _r_at_date(day, hour: int, minute: int) -> datetime:  # type: ignore[no-untyped-def]
    """``_r_at``, generalized to an arbitrary date (AT7's several nights)."""
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)


async def _state(db, site_id, local_date=D):  # type: ignore[no-untyped-def]
    return await db.read(lambda c: day_record(c, site_id, local_date))


async def _attempt_row(db, site_id, attempt, local_date=D):  # type: ignore[no-untyped-def]
    return await db.read(
        lambda c: c.execute(
            "SELECT * FROM close_attempts"
            " WHERE site_id = ? AND local_date = ? AND attempt = ?",
            (site_id, local_date.isoformat(), attempt),
        ).fetchone()
    )


async def _stage_log_rows(db, site_id, stage, local_date=D):  # type: ignore[no-untyped-def]
    rows = await db.read(
        lambda c: c.execute(
            "SELECT attempt, outcome, reason FROM close_stage_log"
            " WHERE site_id = ? AND local_date = ? AND stage = ? ORDER BY id",
            (site_id, local_date.isoformat(), stage),
        ).fetchall()
    )
    return [(row[0], row[1], row[2]) for row in rows]


async def _try_log_rows(db, site_id, station_id, local_date=D):  # type: ignore[no-untyped-def]
    rows = await db.read(
        lambda c: c.execute(
            "SELECT outcome, reason FROM close_try_log"
            " WHERE site_id = ? AND local_date = ? AND station_id = ?"
            " AND slot = 'gap_fill' ORDER BY id",
            (site_id, local_date.isoformat(), station_id),
        ).fetchall()
    )
    return [(row[0], row[1]) for row in rows]


async def _day_close_job_keys(db, site_id):  # type: ignore[no-untyped-def]
    rows = await db.read(
        lambda c: c.execute(
            "SELECT job_key FROM jobs WHERE type = 'day_close' AND site_id = ?",
            (site_id,),
        ).fetchall()
    )
    return [row[0] for row in rows]


def _trim_station_hours(conn, station_id: int, local_date, drop_hours) -> None:  # type: ignore[no-untyped-def]
    """Delete ``station_wind_records`` rows for ``station_id`` on
    ``local_date`` at every hour in ``drop_hours``, turning an otherwise
    full day (``tests.helpers._seed_five_minute_day``) into one with real
    gaps at exactly those hours (AT16(i))."""
    for hour in drop_hours:
        conn.execute(
            "DELETE FROM station_wind_records"
            " WHERE station_id = ? AND local_date = ?"
            " AND strftime('%H', obs_at) = ?",
            (station_id, local_date.isoformat(), f"{hour:02d}"),
        )


def _fixed_fill_gaps(result: GapFillResult):  # type: ignore[no-untyped-def]
    """An async stand-in for ``fill_gaps``, with its real signature (plan
    §15.5 AT15's wording), that always returns ``result`` and never calls
    or reserves anything -- patched at ``wxverify.worker.day_values.
    fill_gaps``, the target ``tests/test_close_timeline.py``'s TL7 already
    uses for ``close_chain``'s call to it."""

    async def _standin(
        db,  # noqa: ANN001
        writer,  # noqa: ANN001
        *,
        site_id: int,
        local_date,  # noqa: ANN001
        now_utc: datetime,
        skip_station_ids=frozenset(),  # noqa: ANN001, B008
    ) -> GapFillResult:
        return result

    return _standin


# ---------------------------------------------------------------------------
# AT1
# ---------------------------------------------------------------------------


def _patch_stage_fault(monkeypatch: pytest.MonkeyPatch, stage: int, p4: dict | None):  # type: ignore[no-untyped-def]
    """Make stage ``stage`` raise unconditionally, by the plan §15.5 AT1
    reading of "the stage raises": stage 2 through ``day_values.fill_gaps``
    (module-qualified, as TL7 patches it); stage 3 through
    ``day_values.write_hourly_values``; stage 4 through the not-yet-built
    ``verification.truth.write_close_truth`` (``monkeypatch.setattr`` on a
    missing attribute raises ``AttributeError`` at once -- an acceptable
    present-day failure reason in its own right, since that name is P3's);
    stage 5 through the ``score_day`` P4 stand-in (``p4`` must be the dict
    :func:`tests.helpers.install_p4_standins` returned); stage 6 through
    P1's own ``finish_close``.
    """
    message = f"synthetic stage {stage} failure"
    if stage == 2:

        async def _raise_fill(*a: object, **kw: object) -> GapFillResult:
            raise RuntimeError(message)

        monkeypatch.setattr("wxverify.worker.close_chain.fill_gaps", _raise_fill)
    elif stage == 3:

        def _raise_write(*a: object, **kw: object) -> object:
            raise RuntimeError(message)

        monkeypatch.setattr(
            "wxverify.worker.close_chain.write_hourly_values", _raise_write
        )
    elif stage == 4:
        monkeypatch.setattr(
            "wxverify.worker.close_chain.write_close_truth",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError(message)),
        )
    elif stage == 5:
        assert p4 is not None
        p4["score_day"].raises = RuntimeError(message)
    else:
        assert stage == 6

        def _raise_finish(*a: object, **kw: object) -> object:
            raise RuntimeError(message)

        monkeypatch.setattr("wxverify.worker.close_chain.finish_close", _raise_finish)


@pytest.mark.parametrize("stage", [2, 3, 4, 5, 6])
def test_at1_each_stage_fails_the_attempt_once(
    stage: int, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT1: for each stage k from 2 to 6, the stage raises in attempt 1 at
    R 00:45Z. D ``retry_due``, ``retry_at`` R 01:45Z, ``last_failure`` the
    sanitized message; the attempt record ``failed``, ``failed_stage`` k;
    no ``day_close`` job left ``pending``; the stage-log row of k
    ``failed`` (9.1).

    M21 -> at the "no pending day_close job" assertion below: correct =
    no row (the queue never retries a stage job itself, ``max_retries``
    0), mutant M21 (``day_close`` enqueued with the queue's default
    ``max_retries`` 5) = a ``pending`` retry row of the failed job left
    behind by the queue's own retry, instead of the close's.

    M22 -> at the attempt-record assertion below: correct =
    ``('failed', stage)`` (``_fail_job``'s ``day_close`` branch records
    the failure), mutant M22 (``_fail_job``'s ``day_close`` branch
    removed) = the attempt record stays ``('running', None)``, since
    nothing closes it out.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        p4 = install_p4_standins(monkeypatch)
        _patch_stage_fault(monkeypatch, stage, p4)

        clock.set(BOUND)
        await run_due_jobs(db, clock, stop_before=None)

        record = await _state(db, site_id)
        assert record.state == "retry_due" and record.attempts == 1, (
            "the one failure of attempt 1 must leave D retry_due at attempt 1"
        )
        assert record.last_failure is not None and (
            f"synthetic stage {stage} failure" in record.last_failure
        ), "last_failure must carry the sanitized failure message (9.1)"
        attempt = await _attempt_row(db, site_id, 1)
        assert attempt is not None
        outcome, failed_stage = attempt["outcome"], attempt["failed_stage"]
        assert (outcome, failed_stage) == ("failed", stage), (
            "M22 -> at this assertion: correct = "
            f"('failed', {stage}), mutant M22 (_fail_job's day_close branch"
            " removed) = ('running', None)"
        )

        pending = await db.read(
            lambda c: c.execute(
                "SELECT 1 FROM jobs WHERE type = 'day_close' AND site_id = ?"
                " AND status = 'pending'",
                (site_id,),
            ).fetchone()
        )
        assert pending is None, (
            "M21 -> at this assertion: correct = no pending day_close job"
            " (max_retries 0, the close's own retry is the only one),"
            " mutant M21 (day_close enqueued with the queue's default"
            " max_retries 5) = a pending retry row of the failed job exists"
        )

        rows = await _stage_log_rows(db, site_id, stage)
        assert rows and rows[-1][:2] == (1, "failed"), (
            f"stage {stage}'s own stage-log row must be outcome='failed'"
            " for attempt 1 (9.1)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT2
# ---------------------------------------------------------------------------


def _fail_hourly_values_once(monkeypatch: pytest.MonkeyPatch, message: str) -> None:
    """``day_values.write_hourly_values`` raises on its first call only
    (attempt 1's stage 3), then runs the real function it replaced for
    every later call (attempt 2's retry must actually rebuild the day's
    hourly rows, plan §5.4)."""
    real = day_values.write_hourly_values
    calls = {"n": 0}

    def _standin(*a: object, **kw: object) -> object:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError(message)
        return real(*a, **kw)

    monkeypatch.setattr("wxverify.worker.close_chain.write_hourly_values", _standin)


def test_at2_the_retry_runs_at_its_own_due_instant_and_closes(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT2: AT1 for k = 3, then ticks to R 01:45Z. The retry starts at R
    01:45Z, not at 01:44Z; stages 2 to 6 run; D ``closed``, ``attempts``
    2; the stand-in ``score_day`` saw attempt 2 with the same truth
    version stage 4 recorded for it (5.4).

    (Plan wording: "the stand-in score_day saw attempts 1 and 2 with the
    same truth version." Attempt 1 fails at stage 3, before stage 5, so
    it never calls score_day; read here as "the one call score_day sees
    carries attempt 2, with the truth version stage 4 of THAT attempt
    recorded for it" -- internal consistency between stage 4's write and
    stage 5's call, not a comparison across two different calls. Flagged
    as a plan-wording interpretation in the hand-back.)

    M23 -> at the 01:44Z assertion below: correct = ('retry_due', 1) (the
    retry has not fired a minute early), mutant M23 (``retry_at`` taken
    as ``now_utc`` + 59 minutes) = ('closing', 2) instead, a minute early.

    M24 -> at the 01:45Z closed assertion below: correct = ('closed', 2)
    (the retry has no stage 1 of its own and does not wait for another
    round), mutant M24 (the retry re-runs stage 1's round check) = the
    retry stays stuck in some pre-stage-2 state instead of closing.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        p4 = install_p4_standins(monkeypatch)
        _fail_hourly_values_once(monkeypatch, "synthetic stage 3 failure")

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1), (
            "attempt 1 must have failed at stage 3 and left D retry_due"
        )
        retry_at = record.retry_at
        assert retry_at is not None

        await tick(db, clock, retry_at - timedelta(minutes=1))
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1), (
            "M23 -> at this assertion: correct = ('retry_due', 1) (one"
            " minute before retry_at, no retry yet), mutant M23 (retry_at"
            " taken as now_utc + 59 minutes) = ('closing', 2) instead"
        )

        clock.set(retry_at)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("closed", 2), (
            "M24 -> at this assertion: correct = ('closed', 2) (the retry"
            " has no stage 1, so it runs straight through stages 2 to 6),"
            " mutant M24 (the retry re-runs stage 1's round check) = D"
            " never reaches closed because no further round settles"
        )

        attempt2 = await _attempt_row(db, site_id, 2)
        assert attempt2 is not None
        calls = p4["score_day"].calls
        assert len(calls) == 1, "score_day must be called exactly once (attempt 2 only)"
        assert calls[0].get("attempt") == 2, "score_day's one call must carry attempt 2"
        assert calls[0].get("truth_version") == attempt2["truth_version"], (
            "score_day must be handed the SAME truth version stage 4 of"
            " attempt 2 recorded (5.4)"
        )

    asyncio.run(_run())


def test_at2_the_retry_does_not_re_check_the_round(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT2 extension: M24 adds a round-settled re-check to the
    ``retry_due`` branch of ``_close_due_days`` (``close_chain.py``,
    around :1921-1926). AT2 above cannot tell this apart from the real
    code because its ``round_state`` stand-in is left ``settled=True``
    for the whole test (set once, before attempt 1, and never changed):
    M24's extra check is then always satisfied too, so it never actually
    blocks anything there and the mutant survives AT2 as written.

    This test drives attempt 1 past an UNSETTLED round instead (as TL2
    does for M14), so the round stays unsettled right through the retry:
    the real retry ignores ``round_state`` entirely and still reaches
    ``closed``, while M24's retry re-reads it, finds it unsettled, and
    gets stuck.

    M24 -> at the retry-closed assertion below: correct = ('closed', 2)
    (the retry_due branch has no round check of its own), mutant M24
    (the retry re-runs stage 1's round check and waits for the next
    round) = ('retry_due', 1), stuck, since round_state never becomes
    settled in this test.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=False, bound_utc=BOUND)
        install_p4_standins(monkeypatch)
        _fail_hourly_values_once(monkeypatch, "synthetic stage 3 failure")

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1), (
            "attempt 1 must start on the bound alone (the round never"
            " settles, as TL2/M14 establish) and fail at stage 3, leaving"
            " D retry_due"
        )
        retry_at = record.retry_at
        assert retry_at is not None

        clock.set(retry_at)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("closed", 2), (
            "M24 -> at this assertion: correct = ('closed', 2) (the retry"
            " ignores round_state entirely), mutant M24 (the retry"
            " re-checks the round in close_chain.py's retry_due branch) ="
            " ('retry_due', 1), stuck, since round_state never becomes"
            " settled in this test"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT3
# ---------------------------------------------------------------------------


def test_at3_both_attempts_failing_misses_the_day_and_runs_retention(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT3: both attempts fail. D ``missed``, ``retry_failed``; P2's
    ``prune_raw_readings`` called after it (8.4); no third ``begin_close``
    (5.3).

    M25 -> at the retention-call assertion below: correct = the prune
    stub was called at least once after D moved to missed, mutant M25
    (raw retention not called after a move to missed) = the stub is
    never called.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)

        prune_calls: list[object] = []

        def _raise_write(*a: object, **kw: object) -> object:
            raise RuntimeError("synthetic stage 3 failure, attempt N")

        monkeypatch.setattr(
            "wxverify.worker.close_chain.write_hourly_values", _raise_write
        )

        def _counting_prune(conn: object, *, now_utc: object) -> int:
            prune_calls.append(now_utc)
            return 0

        monkeypatch.setattr(
            "wxverify.worker.close_chain.prune_raw_readings",
            _counting_prune,
        )

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1), (
            "attempt 1 must fail and leave D retry_due"
        )
        retry_at = record.retry_at
        assert retry_at is not None

        await run_due_jobs(db, clock, stop_before=None)
        await tick(db, clock, retry_at)
        await run_due_jobs(db, clock)

        record = await _state(db, site_id)
        assert (record.state, record.missed_reason, record.attempts) == (
            "missed",
            "retry_failed",
            2,
        ), (
            "both attempts failing at stage 3 must leave D missed,"
            " retry_failed, with no third attempt ever made"
        )
        assert prune_calls, (
            "M25 -> at this assertion: correct = run_close_retention's"
            " prune stub was called at least once after D moved to"
            " missed, mutant M25 (raw retention not called after a move"
            " to missed) = prune_calls stays empty"
        )

        begin_count = await db.read(
            lambda c: c.execute(
                "SELECT COUNT(*) FROM close_attempts"
                " WHERE site_id = ? AND local_date = ?",
                (site_id, D.isoformat()),
            ).fetchone()
        )
        assert begin_count[0] == 2, "no third begin_close may ever run (5.3)"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT4
# ---------------------------------------------------------------------------


def test_at4_a_day_that_fell_due_before_startup_is_never_attempted(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT4: ``worker_started_at`` R 00:41Z (the add-on started after the
    bound). At the first tick D is ``missed``, ``never_attempted``; the
    gap-fill transport saw no request (5.2, :123).

    M26 -> at the state assertion below: correct = ('missed',
    'never_attempted') (the add-on was not running when the first
    attempt fell due at R 00:40Z, since it only started at R 00:41Z),
    mutant M26 (the "running when due" rule without the
    ``worker_started_at`` part) = ('closing', 1) instead, since the
    mutant would start the attempt anyway.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        transport_calls: list[str] = []

        def _handler(request):  # type: ignore[no-untyped-def]
            transport_calls.append(str(request.url))
            raise AssertionError("the transport must never be called (AT4)")

        patch_weathercom_transport(monkeypatch, _handler)
        disable_station_pacing(monkeypatch)

        started_at = _r_at(0, 41)
        await db.write(
            lambda c: set_runtime_state(
                c, "worker_started_at", isoformat_utc(started_at)
            )
        )

        await tick(db, clock, _r_at(1, 0))
        record = await _state(db, site_id)
        assert (record.state, record.missed_reason) == (
            "missed",
            "never_attempted",
        ), (
            "M26 -> at this assertion: correct = ('missed',"
            " 'never_attempted'), mutant M26 (the running-when-due rule"
            " without the worker_started_at part) = ('closing', 1)"
        )
        assert transport_calls == [], (
            "no gap-fill request may be made for a never-attempted day"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT5
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("minutes_late", "expected"),
    [
        (59, ("closing", 1, None)),
        (60, ("closing", 1, None)),
        (61, ("missed", 0, "never_attempted")),
    ],
)
def test_at5_a_frozen_process_misses_a_day_closed_hours_late(
    minutes_late: int,
    expected: tuple[str, int, str | None],
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AT5: the process ran (``worker_started_at`` D 12:00Z) but the first
    tick after R 00:40Z is at (i) R 01:39Z (59 minutes late), (ii) R
    01:41Z (61 minutes late). (i) the attempt starts; (ii) D ``missed``,
    ``never_attempted`` (5.2). Plan section 16.2's M27 names only these
    two; this file adds a third case, exactly 60 minutes late (the
    boundary the rule's own text puts inside: "no later than T plus
    CLOSE_LATE_LIMIT"), to discriminate M28 on its own.

    M27 -> at the 61-minute assertion below: correct = ('missed', 0,
    'never_attempted'), mutant M27 (the rule without the
    CLOSE_LATE_LIMIT part) = ('closing', 1, None) regardless of lateness.

    M28 -> at the 60-minute (exact-boundary) assertion below: correct =
    ('closing', 1, None) (60 minutes late is still "no later than" the
    limit, so the attempt starts), mutant M28 (CLOSE_LATE_LIMIT compared
    with >= instead of > at the boundary) = ('missed', 0,
    'never_attempted') instead, since the mutant treats exactly-60 as
    already past the limit.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        await tick(db, clock, BOUND + timedelta(minutes=minutes_late))
        record = await _state(db, site_id)
        state, attempts, missed_reason = expected
        assert (record.state, record.attempts, record.missed_reason) == (
            state,
            attempts,
            missed_reason,
        ), (
            f"M27/M28 -> at this assertion ({minutes_late} minutes late):"
            f" correct = {expected!r}, mutant M27 (the rule without the"
            " CLOSE_LATE_LIMIT part) would start the attempt in both cases"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT6
# ---------------------------------------------------------------------------


def test_at6_a_retry_not_run_while_the_add_on_was_down_is_missed(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT6: AT1 for k = 2, then ``worker_started_at`` R 02:00Z (down from
    R 01:00Z to 02:00Z). At the first tick D ``missed``, ``retry_not_run``
    (5.2).

    M26 -> at the state assertion below: correct = ('missed',
    'retry_not_run') (the add-on started at R 02:00Z, after the retry's
    due instant R 01:40Z, so the retry was never covered), mutant M26
    (the "running when due" rule without the worker_started_at part) =
    ('retry_due', 1) instead, or an attempt started, since the mutant
    would ignore that the add-on restarted too late for the retry.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        async def _raise_fill(*a: object, **kw: object) -> GapFillResult:
            raise RuntimeError("synthetic stage 2 failure")

        monkeypatch.setattr("wxverify.worker.close_chain.fill_gaps", _raise_fill)

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1)
        retry_at = record.retry_at
        assert retry_at is not None and retry_at == BOUND + timedelta(hours=1)

        await db.write(
            lambda c: set_runtime_state(
                c, "worker_started_at", isoformat_utc(retry_at + timedelta(minutes=20))
            )
        )
        await tick(db, clock, retry_at + timedelta(minutes=20))

        record = await _state(db, site_id)
        assert (record.state, record.missed_reason) == (
            "missed",
            "retry_not_run",
        ), (
            "M26 -> at this assertion: correct = ('missed',"
            " 'retry_not_run'), mutant M26 (the running-when-due rule"
            " without the worker_started_at part) = ('retry_due', 1), the"
            " retry left pending as if the add-on had been running all along"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT7
# ---------------------------------------------------------------------------


def _bound_for(round_date) -> datetime:  # type: ignore[no-untyped-def]
    """The standard night's own round-bound shape (00:40Z), generalized to
    any round date, for AT7's several site-days."""
    return datetime(
        round_date.year, round_date.month, round_date.day, 0, 40, tzinfo=UTC
    )


def test_at7_an_outage_spanning_several_nights_misses_each_in_date_order(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT7: the add-on down from D - 1 22:00Z to D + 2 03:00Z, then
    started. D - 1, D and D + 1 moved to ``missed``, ``never_attempted``,
    in date order at the first tick; no gap-fill request for any of
    them; D + 2 closes the next night as usual (5.2).

    M29 -> at the per-day assertion below: correct = D - 1, D and D + 1
    are each read back ``missed``/``never_attempted`` after the one tick
    (processed oldest first, so each earlier day is already done by the
    time ``mark_missed`` checks the next one, P1 §4.7's
    ``_has_earlier_undone`` guard), mutant M29 (missed days moved in
    reverse date order) = NONE of the three is moved: processing D + 1
    first calls ``mark_missed`` while D - 1 and D are still ``open``,
    which P1's own guard refuses with ``DayStateError``, and 8.5's
    per-site savepoint then rolls the whole site's step back for this
    tick, leaving all three ``open`` with one ERROR line logged instead.
    """

    async def _run() -> None:
        from tests.helpers import _seed_five_minute_day, day_close_site

        close_db_path = tmp_path / "wxverify.db"
        from wxverify import config
        from wxverify.db.connection import close_db, init_db

        close_db()
        tmp_path.mkdir(parents=True, exist_ok=True)
        config.db_path = str(close_db_path)
        db = init_db(str(close_db_path))
        conn = db._conn  # noqa: SLF001

        d_minus_1 = D - timedelta(days=1)
        d_plus_1 = D + timedelta(days=1)
        d_plus_2 = D + timedelta(days=2)

        site_id = day_close_site(
            conn, timezone="UTC", close_on=True, with_records=False
        )
        stations = {
            str(row["pws_station_id"]): int(row["id"])
            for row in conn.execute(
                "SELECT id, pws_station_id FROM stations WHERE site_id = ? ORDER BY id",
                (site_id,),
            )
        }
        start_site_days(
            conn,
            site_id,
            d_minus_1,
            now_utc=note_day_clock(
                datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, tzinfo=UTC)
            ),
        )
        ensure_site_days(
            conn,
            site_id,
            now_utc=note_day_clock(
                datetime(d_plus_2.year, d_plus_2.month, d_plus_2.day, 12, tzinfo=UTC)
            ),
        )
        for station_id in stations.values():
            _seed_five_minute_day(conn, station_id, d_plus_2)
        set_runtime_state(
            conn, "worker_started_at", isoformat_utc(_r_at_date(d_plus_2, 3, 0))
        )
        conn.commit()

        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        for round_date in (D, d_plus_1, d_plus_2, d_plus_2 + timedelta(days=1)):
            round_state.set_answer(
                site_id, round_date, settled=True, bound_utc=_bound_for(round_date)
            )
        install_p4_standins(monkeypatch)
        transport_calls: list[str] = []

        def _handler(request):  # type: ignore[no-untyped-def]
            transport_calls.append(str(request.url))
            raise AssertionError("no gap-fill request may be made for a missed day")

        patch_weathercom_transport(monkeypatch, _handler)
        disable_station_pacing(monkeypatch)

        await tick(db, clock, _r_at_date(d_plus_2, 3, 0))

        for local_date in (d_minus_1, D, d_plus_1):
            record = await _state(db, site_id, local_date)
            assert (record.state, record.missed_reason) == (
                "missed",
                "never_attempted",
            ), (
                f"M29 -> at this assertion ({local_date}): correct ="
                " ('missed', 'never_attempted') for every day whose first"
                " due instant passed while the add-on was down, mutant M29"
                " (missed days moved in reverse date order) processes the"
                " same set of days but starting from the newest instead"
            )
        assert transport_calls == [], "no gap-fill call for any missed day"

        clock.set(_bound_for(d_plus_2 + timedelta(days=1)))
        await run_due_jobs(db, clock)
        record = await _state(db, site_id, d_plus_2)
        assert record.state == "closed", "D + 2 must close the next night as usual"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT8
# ---------------------------------------------------------------------------


def _station_fill(outcome, hours_left=()):  # type: ignore[no-untyped-def]
    reason = "refused_access" if outcome in ("held", "refused") else None
    hours_filled = {} if outcome != "filled" else {"temperature": (NOW,)}
    left = hours_left if outcome != "filled" else ()
    return StationFill(
        outcome=outcome,
        reason=reason,
        detail=None,
        hours_filled=hours_filled,
        hours_left=left,
        next_allowed_at=None,
        readings_kept=0,
        readings_refused=0,
    )


@pytest.mark.parametrize(
    ("a_outcome", "b_outcome"),
    [
        ("held", "filled"),
        ("refused", "held"),
        ("no_readings", "filled"),
    ],
)
def test_at8_a_hold_or_refused_access_or_no_readings_does_not_fail_the_attempt(
    a_outcome: str, b_outcome: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT8: the gap fill: (i) A ``held``, B ``filled``; (ii) A ``refused``
    (401), B ``held`` after it; (iii) A ``no_readings``. Each: no
    failure; D ``closed`` in attempt 1 with A's gap hours left empty,
    values judged on coverage (5.3, 10.3).

    M30 -> at the closed-state assertion below (case "held"): correct =
    ('closed', 1), mutant M30 (a ``held`` gap-fill station counted as a
    failure) = ('retry_due', 1) instead.

    M31 -> at the closed-state assertion below (case "refused"): correct
    = ('closed', 1), mutant M31 (a ``refused``/``refused_access`` station
    counted as a failure) = ('retry_due', 1) instead.

    M98 -> at the closed-state assertion below (case "no_readings"):
    correct = ('closed', 1), mutant M98 (a ``no_readings`` station
    counted as a failure) = ('retry_due', 1) instead.
    """

    async def _run() -> None:
        db, _writer, site_id, stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)

        station_a = stations["ISTATION01"]
        station_b = stations["ISTATION02"]
        gap_hour = datetime(D.year, D.month, D.day, 21, tzinfo=UTC)
        result = GapFillResult(
            ended=True,
            stations={
                station_a: _station_fill(a_outcome, hours_left=(gap_hour,)),
                station_b: _station_fill(b_outcome),
            },
        )
        monkeypatch.setattr(
            "wxverify.worker.close_chain.fill_gaps", _fixed_fill_gaps(result)
        )

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("closed", 1), (
            f"M30/M31/M98 -> at this assertion (A={a_outcome!r}): correct"
            " = ('closed', 1) (a hold, a refused-access refusal or a bare"
            " no_readings outcome is never a failure), mutant (any of"
            " M30/M31/M98 counting that outcome as a failure) ="
            " ('retry_due', 1) instead"
        )

        a_log = await _try_log_rows(db, site_id, station_a)
        assert a_log and a_log[-1][0] == a_outcome, (
            "the gap-fill try-log row for A must record its real outcome"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT9
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "reason",
    ["network", "provider", "backoff", "call_limit"],
)
def test_at9_a_technical_gap_fill_failure_fails_the_attempt(
    reason: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT9: the gap fill: A ``failed`` with (i) ``network``, (ii)
    ``provider``, (iii) ``backoff``, (iv) ``call_limit``. Each: attempt 1
    fails at stage 2, reason naming the station's outcome without its
    id; retry at +1 h (5.3).

    M32 -> at the state assertion below (reason "network"): correct =
    ('retry_due', 1), mutant M32 (a ``failed``/``network`` station not
    counted as a failure) = ('closed', 1) instead.
    """

    async def _run() -> None:
        db, _writer, site_id, stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)

        station_a = stations["ISTATION01"]
        station_b = stations["ISTATION02"]
        failing = StationFill(
            outcome="failed",
            reason=reason,
            detail=None,
            hours_filled={},
            hours_left=(datetime(D.year, D.month, D.day, 21, tzinfo=UTC),),
            next_allowed_at=None,
            readings_kept=0,
            readings_refused=0,
        )
        result = GapFillResult(
            ended=True,
            stations={
                station_a: failing,
                station_b: _station_fill("nothing_to_fill"),
            },
        )
        monkeypatch.setattr(
            "wxverify.worker.close_chain.fill_gaps", _fixed_fill_gaps(result)
        )

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1), (
            f"M32 -> at this assertion (reason={reason!r}): correct ="
            " ('retry_due', 1) (a technical gap-fill failure fails the"
            " attempt), mutant M32 (a failed/network station not counted"
            " as a failure) = ('closed', 1) instead"
        )
        assert record.retry_at == BOUND + timedelta(hours=1)
        assert record.last_failure is not None
        assert "ISTATION01" not in record.last_failure, (
            "the failure reason must name the outcome/reason, never the"
            " station's own id"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT10
# ---------------------------------------------------------------------------


def test_at10_a_thin_day_closes_with_ineligible_values_not_a_failure(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT10: A and B with readings in hours 0 to 10 only. D ``closed`` in
    attempt 1; high and low ``eligible`` 0 (6.2); no retry.

    Hours 11 to 23 are real gaps here (unlike AT15's stand-in): the
    provider is given a real key and transport, answering every hourly
    request with a bare 204 (``no_readings``), so the day stays genuinely
    thin without needing a network-free stand-in.

    M33 -> at the state assertion below: correct = ('closed', 1) (a thin
    value is final, never a failure), mutant M33 (the attempt fails when
    a daily value is thin) = ('retry_due', 1) instead.
    """

    async def _run() -> None:
        import httpx

        db, _writer, site_id, stations = standard_night(tmp_path)
        conn = db._conn  # noqa: SLF001
        for station_id in stations.values():
            _trim_station_hours(conn, station_id, D, range(11, 24))
        conn.commit()

        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)
        write_weathercom_key(monkeypatch, tmp_path, key="ci-placeholder")

        def _handler(request: object) -> object:
            return httpx.Response(204)

        patch_weathercom_transport(monkeypatch, _handler)
        disable_station_pacing(monkeypatch)

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("closed", 1), (
            "M33 -> at this assertion: correct = ('closed', 1) (a thin day"
            " closes with its thin values marked ineligible, never a"
            " failure), mutant M33 (the attempt fails when a daily value"
            " is thin) = ('retry_due', 1) instead"
        )

        rows = await db.read(
            lambda c: c.execute(
                "SELECT quantity, eligible FROM daily_truth"
                " WHERE site_id = ? AND local_date = ?"
                " AND quantity IN ('temperature_high', 'temperature_low')",
                (site_id, D.isoformat()),
            ).fetchall()
        )
        assert rows, "stage 4 must have written the day's truth rows"
        assert all(row[1] == 0 for row in rows), (
            "both high and low must be eligible=0 on a day with only 11"
            " hours of readings (6.2's 18-hour minimum)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT11
# ---------------------------------------------------------------------------


def test_at11_a_stalled_claim_fails_the_attempt_before_doing_any_work(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT11: the stage-3 job's claim comes 61 minutes after it was
    enqueued: the clock moves 61 minutes after the helper stamps the
    job's insert and before the claim. Attempt fails at stage 3 with
    reason ``stalled``, before ``write_hourly_values`` is called (8.3).

    M34 -> at the state assertion below: correct = ('retry_due', 1) (the
    queue-wait check fails the attempt before the stage does any work),
    mutant M34 (the queue-wait check removed) = ('closed', 1) instead,
    since the stage would run normally despite the stale claim.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)

        write_calls: list[object] = []
        real_write = day_values.write_hourly_values

        def _spy(*a: object, **kw: object) -> object:
            write_calls.append((a, kw))
            return real_write(*a, **kw)

        monkeypatch.setattr("wxverify.worker.close_chain.write_hourly_values", _spy)

        clock.set(BOUND)
        await run_due_jobs(db, clock, stop_before=("day_close", 3))
        clock.advance(timedelta(minutes=61))
        job = await db.write(claim_next_job)
        assert job is not None and job.type == "day_close"
        await run_claimed_job(db, job, lane="main")

        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1), (
            "M34 -> at this assertion: correct = ('retry_due', 1) (stage 3"
            " was claimed 61 minutes after it was enqueued, past"
            " CLOSE_LATE_LIMIT), mutant M34 (the queue-wait check removed)"
            " = ('closed', 1) instead"
        )
        assert write_calls == [], (
            "write_hourly_values must never be called once the stage is"
            " found stalled (8.3: the check runs before any work)"
        )
        rows = await _stage_log_rows(db, site_id, 3)
        assert rows and rows[-1] == (1, "failed", "stalled"), (
            "the stage-3 stage-log row must record outcome='failed', reason='stalled'"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT12
# ---------------------------------------------------------------------------


def test_at12_a_stale_jobs_exception_does_not_touch_the_current_attempt(
    tmp_path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """AT12: D ``closing`` in attempt 2 after AT1 for k = 3; a stage-3
    job with payload ``{"attempt": 1}`` inserted and run, whose first
    read (P1's ``day_record`` as ``close_chain`` calls it) raises once.
    The job row ``failed``; D still ``closing`` with ``attempts`` 2,
    ``retry_at`` and ``last_failure`` unchanged; attempt 2's record
    ``running``, attempt 1's as it was; no stage-log row added; one
    WARNING line naming the site, the date, stage 3 and ``closing`` (9.1).

    M76 -> at the unchanged-record assertions below: correct = D's
    record, attempt 2's record and the stage-3 log-row count are all
    BYTE-FOR-BYTE what they were right before this stale job ran (the
    stale job's own exception never reaches ``fail_attempt``, since
    ``_fail_job`` first checks that D is ``closing`` in the JOB's own
    attempt, 1, which it is not), mutant M76 (``_fail_job``'s ``day_close``
    branch calls ``fail_attempt`` without that check) = D is moved to
    ``retry_due`` (or further) by the stale job's own exception instead,
    stamped with attempt 1's stage and reason even though the real
    attempt in progress is 2.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)
        _fail_hourly_values_once(monkeypatch, "synthetic stage 3 failure")

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1)
        retry_at = record.retry_at
        assert retry_at is not None

        # Run the retry (attempt 2) up to, but not including, stage 4: its
        # own stage 3 completes normally (the real write_hourly_values is
        # used on this call, per _fail_hourly_values_once), freeing stage
        # 3's job key for this test's own stale insert below.
        await tick(db, clock, retry_at)
        await run_due_jobs(db, clock, stop_before=("day_close", 4))
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("closing", 2), (
            "attempt 2 must be mid-flight, past its own stage 3"
        )

        attempt1_before = await _attempt_row(db, site_id, 1)
        attempt2_before = await _attempt_row(db, site_id, 2)
        stage3_count_before = len(await _stage_log_rows(db, site_id, 3))

        real_day_record = day_record
        calls = {"n": 0}

        def _flaky_day_record(*a: object, **kw: object) -> object:
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("synthetic transient read failure")
            return real_day_record(*a, **kw)

        # close_chain imports day_record BY NAME, so the patch targets
        # wxverify.worker.close_chain.day_record: the first call (the gate
        # check in _current, inside _open_stage) raises; the stale job's
        # exception then reaches fail_day_close_job's own call, the second,
        # which runs the real day_record and finds D is still closing in
        # attempt 2, not the stale job's attempt 1, so nothing else changes.
        monkeypatch.setattr("wxverify.worker.close_chain.day_record", _flaky_day_record)

        # Backdated by one second so claim_next_job's tier-0 FIFO
        # (created_at, id) picks this stale job ahead of attempt 2's own
        # pending stage-4 job, which the earlier run_due_jobs call just
        # enqueued at this same clock instant (db/queue.py's
        # _CLAIM_ORDER_BY) -- one second is far under CLOSE_LATE_LIMIT (1
        # hour, 8.3), so it does not make the job read as stalled.
        stamp = isoformat_utc(clock.now() - timedelta(seconds=1))
        await db.write(
            lambda c: c.execute(
                "INSERT INTO jobs (type, site_id, job_key, status, payload,"
                " max_retries, created_at, updated_at)"
                " VALUES ('day_close', ?, ?, 'pending', '{\"attempt\": 1}',"
                " 0, ?, ?)",
                (site_id, f"close:{site_id}:{D.isoformat()}:3", stamp, stamp),
            )
        )

        caplog.set_level("WARNING", logger="wxverify.worker.close_chain")
        stale_job = await db.write(claim_next_job)
        assert stale_job is not None and stale_job.job_key.endswith(":3")
        await run_claimed_job(db, stale_job, lane="main")

        job_row = await db.read(
            lambda c: c.execute(
                "SELECT status FROM jobs WHERE id = ?", (stale_job.id,)
            ).fetchone()
        )
        assert job_row is not None and job_row[0] == "failed", (
            "the stale job row itself must be failed"
        )

        record_after = await _state(db, site_id)
        assert (record_after.state, record_after.attempts) == ("closing", 2), (
            "M76 -> at this assertion: correct = D is still ('closing', 2),"
            " untouched by the stale attempt-1 job's exception, mutant M76"
            " (_fail_job's day_close branch without its closing-in-this-"
            "attempt check) = D moved further (e.g. 'retry_due') instead"
        )
        assert (record_after.retry_at, record_after.last_failure) == (
            record.retry_at,
            record.last_failure,
        ), "retry_at and last_failure must be unchanged by the stale job"

        attempt1_after = await _attempt_row(db, site_id, 1)
        attempt2_after = await _attempt_row(db, site_id, 2)
        assert tuple(attempt1_after) == tuple(attempt1_before), (
            "attempt 1's record must be byte-for-byte unchanged"
        )
        assert attempt2_after["outcome"] == "running" and tuple(
            attempt2_after
        ) == tuple(attempt2_before), (
            "attempt 2's record must stay 'running', unchanged by the stale job"
        )

        stage3_count_after = len(await _stage_log_rows(db, site_id, 3))
        assert stage3_count_after == stage3_count_before, (
            "no new stage-3 stage-log row may be written for the stale job"
        )

        warnings = [rec for rec in caplog.records if rec.levelname == "WARNING"]
        assert len(warnings) == 1, "exactly one WARNING line must be written"
        message = warnings[0].message
        assert str(site_id) in message
        assert D.isoformat() in message
        assert "3" in message
        assert "closing" in message

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT13
# ---------------------------------------------------------------------------


def test_at13_a_recovered_close_reports_no_failed_scope(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT13: AT2, then the failed-scope count (15.1) read at R + 3 d. 0
    failed scopes, though attempt 1's stage-3 job row is ``failed`` and
    older than 48 hours (9.6).

    M80 -> at the count assertion below: correct = 0 (``day_close`` is
    excluded from the failed arm), mutant M80 (``FAILED_SCOPES_SQL``
    without ``j.type != 'day_close'``) = 1 (attempt 1's failed stage-3
    job counts as a failed scope once it is older than 48 hours).
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)
        _fail_hourly_values_once(monkeypatch, "synthetic stage 3 failure")

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1)
        retry_at = record.retry_at
        assert retry_at is not None
        await tick(db, clock, retry_at)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("closed", 2)

        later = R + timedelta(days=3)
        count = await db.read(
            lambda c: failed_scope_count(
                c, now_utc=datetime.combine(later, NOW.time()).replace(tzinfo=UTC)
            )
        )
        assert count == 0, (
            "M80 -> at this assertion: correct = 0 (day_close jobs never"
            " count in the failed-scopes arm), mutant M80"
            " (FAILED_SCOPES_SQL without j.type != 'day_close') = 1"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT14
# ---------------------------------------------------------------------------


def test_at14_a_missed_day_from_a_failed_stage_reports_no_failed_scope(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT14: AT3 with both attempts failing by an exception in stage 4,
    then the failed-scope count (15.1) read at R + 3 d. 0 failed scopes;
    D ``missed``, ``retry_failed`` (9.6).

    M81 -> at the count assertion below: correct = 0 (``day_close`` is
    left OUT of the failed arm entirely, never put on the single-row
    allowlist), mutant M81 (``day_close`` put on the single-row allowlist
    instead) = a nonzero count, since the allowlist path still counts a
    job once it is the only one of its scope and old enough, exactly
    what leaving it out of the clause altogether is meant to prevent.

    Patches ``verification.truth.write_close_truth`` (plan §6.1), which
    does not exist before piece 2: the bare ``monkeypatch.setattr`` (no
    ``raising=False``) raises ``AttributeError`` at once -- the right
    failure reason for this P3 name today.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)
        monkeypatch.setattr(
            "wxverify.worker.close_chain.write_close_truth",
            lambda *a, **kw: (_ for _ in ()).throw(
                RuntimeError("synthetic stage 4 failure")
            ),
        )

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1)
        retry_at = record.retry_at
        assert retry_at is not None
        await tick(db, clock, retry_at)
        await run_due_jobs(db, clock)

        record = await _state(db, site_id)
        assert (record.state, record.missed_reason, record.attempts) == (
            "missed",
            "retry_failed",
            2,
        )

        later = R + timedelta(days=3)
        count = await db.read(
            lambda c: failed_scope_count(
                c, now_utc=datetime.combine(later, NOW.time()).replace(tzinfo=UTC)
            )
        )
        assert count == 0, (
            "M81 -> at this assertion: correct = 0, mutant M81 (day_close"
            " put on the single-row allowlist instead of left out of the"
            " failed arm) = a nonzero count"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT15
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("a_outcome", ["nothing_to_fill", "not_on_day"])
def test_at15_an_outcome_that_does_not_fail_the_attempt_closes_the_day(
    a_outcome: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT15: ``fill_gaps``, as ``close_chain`` calls it, replaced by a
    stand-in with its signature that returns ``GapFillResult(ended=True,
    ...)`` with B ``filled`` and A (i) ``nothing_to_fill``, (ii)
    ``not_on_day``. Each: no failure; D ``closed`` in attempt 1 (5.3).

    M92/M95/M96 -> at the state assertion below: correct = ('closed', 1)
    for every one of these outcomes, mutant M92 (a ``filled`` station
    counted as a failure, exercised here through B), M95 (a
    ``nothing_to_fill`` station counted as a failure, case (i)) or M96 (a
    ``not_on_day`` station counted as a failure, case (ii)) = ('retry_due',
    1) instead.
    """

    async def _run() -> None:
        db, _writer, site_id, stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)

        station_a = stations["ISTATION01"]
        station_b = stations["ISTATION02"]
        result = GapFillResult(
            ended=True,
            stations={
                station_a: _station_fill(a_outcome),
                station_b: _station_fill("filled"),
            },
        )
        monkeypatch.setattr(
            "wxverify.worker.close_chain.fill_gaps", _fixed_fill_gaps(result)
        )

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("closed", 1), (
            f"M92/M95/M96 -> at this assertion (A={a_outcome!r}): correct"
            " = ('closed', 1), mutant (counting that outcome, or B's"
            " filled, as a failure) = ('retry_due', 1) instead"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT16
# ---------------------------------------------------------------------------


def test_at16i_a_missing_key_fails_both_attempts_when_there_are_hours_to_fill(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT16(i): an ``options.json`` without ``weathercom_key`` (15.1); A
    with readings in hours 0 to 20 only, so it has hours to fill. Attempt
    1 fails at stage 2, ``failed_stage`` 2, ``failure_reason`` and
    ``last_failure`` the sanitized message ``weathercom key is not
    configured``; D ``retry_due``; the retry fails the same; D
    ``missed``, ``retry_failed``; the transport saw no request; no
    gap-fill try-log row.

    M93 -> at the first-failure assertion below: correct = ('retry_due',
    1) with ``last_failure`` the exact sanitized key message (the
    ``RuntimeError`` from ``_Fill.key()`` is never caught by stage 2),
    mutant M93 (stage 2 catches that exception and goes on as if no
    station had gaps) = ('closed', 1) instead.
    """

    async def _run() -> None:
        db, _writer, site_id, stations = standard_night(tmp_path)
        conn = db._conn  # noqa: SLF001
        station_a = stations["ISTATION01"]
        _trim_station_hours(conn, station_a, D, range(21, 24))
        conn.commit()

        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)
        write_weathercom_key(monkeypatch, tmp_path, key=None)
        transport_calls: list[str] = []

        def _handler(request):  # type: ignore[no-untyped-def]
            transport_calls.append(str(request.url))
            raise AssertionError("no request may be made without a key")

        patch_weathercom_transport(monkeypatch, _handler)
        disable_station_pacing(monkeypatch)

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1), (
            "M93 -> at this assertion: correct = ('retry_due', 1), mutant"
            " M93 (stage 2 catches the missing-key RuntimeError and goes"
            " on) = ('closed', 1) instead"
        )
        assert record.last_failure == "weathercom key is not configured", (
            "the sanitized key-missing message must be stored verbatim"
        )
        retry_at = record.retry_at
        assert retry_at is not None

        await tick(db, clock, retry_at)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.missed_reason, record.attempts) == (
            "missed",
            "retry_failed",
            2,
        ), "the retry fails the same way, since the key is still missing"

        assert transport_calls == [], "the transport must never see a request"
        a_log = await _try_log_rows(db, site_id, station_a)
        assert a_log == [], "no gap-fill try-log row may exist for A"

    asyncio.run(_run())


def test_at16ii_a_missing_key_does_not_matter_with_nothing_to_fill(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT16(ii): an ``options.json`` without ``weathercom_key``; A and B
    with full readings, nothing to fill. D ``closed`` in attempt 1 with
    no request: the key is read only for a station with hours to fill
    (5.3, 10.1).

    M95 -> at the closed-state assertion below: correct = ('closed', 1)
    (neither station has anything to fill, so the key is never even
    read), mutant M95 (a ``nothing_to_fill`` station counted as a
    failure) = ('retry_due', 1) instead.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)
        write_weathercom_key(monkeypatch, tmp_path, key=None)
        transport_calls: list[str] = []

        def _handler(request):  # type: ignore[no-untyped-def]
            transport_calls.append(str(request.url))
            raise AssertionError("no request may be made with nothing to fill")

        patch_weathercom_transport(monkeypatch, _handler)
        disable_station_pacing(monkeypatch)

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("closed", 1), (
            "M95 -> at this assertion: correct = ('closed', 1) (nothing"
            " to fill for either station, standard_night's own full-day"
            " seeding), mutant M95 (a nothing_to_fill station counted as"
            " a failure) = ('retry_due', 1) instead"
        )
        assert transport_calls == [], (
            "the key is read only for a station with hours to fill"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# AT17
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "outcome", "reason"),
    [
        ("day_guard", "refused", "day_guard"),
        ("none_reason", "refused", None),
        ("other_outcome", cast("object", "other"), None),
    ],
)
def test_at17_an_unlisted_outcome_or_reason_fails_the_attempt(
    label: str,
    outcome: object,
    reason: str | None,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AT17: AT15's stand-in, B ``filled`` and A (i) ``refused`` with
    reason ``day_guard``, (ii) ``refused`` with reason ``None``, (iii) an
    outcome outside P2's ``FillOutcome`` (``"other"``, built with
    ``typing.cast``). Each: attempt 1 fails at stage 2, the reason naming
    A's outcome and reason without its id; D ``retry_due``, ``retry_at``
    1 hour on (5.3, 10.1).

    M91 -> at the state assertion below (case "day_guard"): correct =
    ('retry_due', 1) (a ``refused``/``day_guard`` station IS a failure --
    P1's guard took no write), mutant M91 (``refused`` sorted by outcome
    alone, as a hold) = ('closed', 1) instead.

    M94 -> at the state assertion below (cases "none_reason" and
    "other_outcome"): correct = ('retry_due', 1) for both (the sort is an
    allowlist: anything not explicitly named fails), mutant M94 (the sort
    written as a denylist) = ('closed', 1) instead, since neither a
    ``refused`` with no reason nor an unlisted outcome is on the denylist.
    """

    async def _run() -> None:
        db, _writer, site_id, stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        install_p4_standins(monkeypatch)

        station_a = stations["ISTATION01"]
        station_b = stations["ISTATION02"]
        a_fill = StationFill(
            outcome=outcome,  # type: ignore[arg-type]
            reason=reason,
            detail=None,
            hours_filled={},
            hours_left=(datetime(D.year, D.month, D.day, 21, tzinfo=UTC),),
            next_allowed_at=None,
            readings_kept=0,
            readings_refused=0,
        )
        result = GapFillResult(
            ended=True,
            stations={station_a: a_fill, station_b: _station_fill("filled")},
        )
        monkeypatch.setattr(
            "wxverify.worker.close_chain.fill_gaps", _fixed_fill_gaps(result)
        )

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert (record.state, record.attempts) == ("retry_due", 1), (
            f"M91/M94 -> at this assertion (label={label!r}, outcome="
            f"{outcome!r}, reason={reason!r}): correct = ('retry_due', 1),"
            " mutant (M91's hold-style sort for day_guard, or M94's"
            " denylist for an unnamed reason/outcome) = ('closed', 1) instead"
        )
        assert record.retry_at == BOUND + timedelta(hours=1)
        assert record.last_failure is not None
        assert "ISTATION01" not in record.last_failure, (
            "the failure reason must name A's outcome/reason, never its id"
        )

    asyncio.run(_run())
