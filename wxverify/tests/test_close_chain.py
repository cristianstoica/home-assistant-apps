"""Hand-offs (plan 2026-10-21-daily-close-close-job.md section 15.7): HO1
to HO6.

Written before ``close_chain.enqueue_due_close_work``, ``close_p4`` and
``close_chain.fail_attempt`` exist, and fails first for that reason: every
test in this file drives the real night chain end to end
(``tests.helpers.install_round_state_standin``/``install_p4_standins``,
the pattern of ``tests/test_close_timeline.py``), so each reaches
``install_round_state_standin`` before doing anything else -- a
``ModuleNotFoundError`` today, since ``wxverify.worker.close_p4`` does not
exist. HO4 then changes one of D's real ``observations`` values through
P1's real, already-built ``materialize_consensus``/``finish_close`` (6.1)
between stage 4 and stage 6; HO6 patches
``wxverify.worker.close_chain.enqueue_if_absent`` (a name that does not
exist in ``close_chain.py`` at this commit, which holds only
``record_collection_try``, so the patch uses ``raising=False``) to fail
only the ``day_publish`` enqueue inside stage 6's own completing write.

Section 16.2 names the mutant each test ID kills; the mapping is given
inline on the exact assertion it kills at.

Real SQLite only (``tests.helpers.standard_night``, real ``run_migrations``
via ``init_db`` on a ``tmp_path`` file). Station ids are synthetic
(``ISTATION01``/``ISTATION02``), the zone is ``UTC``, and every date is the
fixed synthetic ``tests.helpers.D``/the derived round date ``R``. No live
id, coordinate, site name or real zone name appears.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import timedelta

import pytest

from tests.helpers import (
    NOW,
    STANDARD_NIGHT_R,
    STANDARD_NIGHT_ROUND_BOUND,
    D,
    MovingClock,
    clear_close_chain_log_map,
    disable_other_enqueuers,
    install_p4_standins,
    install_round_state_standin,
    patch_close_clock,
    run_due_jobs,
    standard_night,
    tick,
)
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.queue import enqueue_if_absent
from wxverify.db.site_days import DayStateError, day_record, finish_close
from wxverify.scoring.consensus import materialize_consensus, write_station_observation


@pytest.fixture(autouse=True)
def _clear_close_chain_log_map() -> None:
    """This file's own autouse wiring of ``clear_close_chain_log_map``
    (15.1's "The log map.") -- autouse HERE, inside this one P3 test
    file, never across the whole suite."""
    clear_close_chain_log_map()


R = STANDARD_NIGHT_R
BOUND = STANDARD_NIGHT_ROUND_BOUND


async def _state(db, site_id, local_date=D):  # type: ignore[no-untyped-def]
    return await db.read(lambda c: day_record(c, site_id, local_date))


async def _publication_row(db, site_id, cutoff):  # type: ignore[no-untyped-def]
    return await db.read(
        lambda c: c.execute(
            "SELECT state, publication_id FROM close_publications"
            " WHERE site_id = ? AND cutoff = ?",
            (site_id, cutoff.isoformat()),
        ).fetchone()
    )


async def _real_publication_id(db, site_id, cutoff):  # type: ignore[no-untyped-def]
    """The real ``publications`` row's id for ``cutoff``, as text -- the
    id ``close_p4.publish``'s call-through actually returns, now that
    P3's stand-in no longer invents one (plan 18.13)."""
    row = await db.read(
        lambda c: c.execute(
            "SELECT id FROM publications WHERE site_id = ? AND cutoff = ?",
            (site_id, cutoff.isoformat()),
        ).fetchone()
    )
    assert row is not None, (
        f"expected a real publications row for site {site_id}, cutoff {cutoff}"
    )
    return str(row[0])


async def _attempt_row(db, site_id, local_date, attempt):  # type: ignore[no-untyped-def]
    return await db.read(
        lambda c: c.execute(
            "SELECT outcome, last_stage, last_progress_at, failed_stage,"
            " failure_reason FROM close_attempts"
            " WHERE site_id = ? AND local_date = ? AND attempt = ?",
            (site_id, local_date.isoformat(), attempt),
        ).fetchone()
    )


async def _day_close_job_keys(db, site_id):  # type: ignore[no-untyped-def]
    rows = await db.read(
        lambda c: c.execute(
            "SELECT job_key FROM jobs WHERE type IN ('day_close', 'day_publish')"
            " AND site_id = ? ORDER BY id",
            (site_id,),
        ).fetchall()
    )
    return [row[0] for row in rows]


# ---------------------------------------------------------------------------
# HO1
# ---------------------------------------------------------------------------


def test_ho1_the_standard_night_hands_off_every_stage_in_order(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HO1: the standard night, ``run_due_jobs`` from R 00:40Z. Claims in
    order: ``day_close`` stages 2 to 6, ``day_publish`` stage 7, each
    keyed as 8.1; D ``closed``; the publication ``published``, ``pub-D``
    (8.1).

    M46 -> at the keys assertion below: correct = the five ``day_close``
    keys end ``:2`` through ``:6`` and the ``day_publish`` key ends ``:7``,
    in that order, mutant M46 (the stage number left out of the job key)
    = every ``day_close`` key collapses to the SAME key
    (``close:{site}:{date}`` with no stage suffix), so ``enqueue_if_absent``
    dedupes every later stage against the first and only one job of each
    kind is ever created.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        clock.set(BOUND)
        claims = await run_due_jobs(db, clock)

        keys = await _day_close_job_keys(db, site_id)
        expected = [
            f"close:{site_id}:{D.isoformat()}:2",
            f"close:{site_id}:{D.isoformat()}:3",
            f"close:{site_id}:{D.isoformat()}:4",
            f"close:{site_id}:{D.isoformat()}:5",
            f"close:{site_id}:{D.isoformat()}:6",
            f"publish:{site_id}:7",
        ]
        assert keys == expected, (
            f"M46 -> at this assertion: correct = {expected!r} (one job per"
            " stage, each keyed with its own stage number), mutant M46"
            " (the stage number left out of the job key) = every day_close"
            " key collapses to one value, so stages 3-6 are deduped away"
            " and never run"
        )

        claim_types = [t for t, _k, _o in claims]
        assert claim_types == [
            "day_close",
            "day_close",
            "day_close",
            "day_close",
            "day_close",
            "day_publish",
        ], "the claim order must be stages 2-6 then 7, nothing interleaved"

        record = await _state(db, site_id)
        assert record.state == "closed", "the standard night must close D"
        expected_pub_id = await _real_publication_id(db, site_id, D)
        publication = await _publication_row(db, site_id, D)
        assert (publication["state"], publication["publication_id"]) == (
            "published",
            expected_pub_id,
        ), "the publication must be published with P4's identifier"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# HO2
# ---------------------------------------------------------------------------


async def _delete_job(db, job_type, stage, *, cutoff=None):  # type: ignore[no-untyped-def]
    suffix = f":{stage}"
    deleted = await db.write(
        lambda c: (
            c.execute(
                "DELETE FROM jobs WHERE type = ? AND status = 'pending'"
                " AND job_key LIKE '%' || ?",
                (job_type, suffix),
            ).rowcount
        )
    )
    assert deleted == 1, f"expected exactly one pending {job_type} job ending {suffix}"


async def _job_exists(db, job_type, stage):  # type: ignore[no-untyped-def]
    rows = await db.read(
        lambda c: c.execute(
            "SELECT id FROM jobs WHERE type = ? AND status = 'pending'"
            " AND job_key LIKE '%' || ?",
            (job_type, f":{stage}"),
        ).fetchall()
    )
    return [row[0] for row in rows]


def test_ho2_a_lost_handoff_is_repaired_once_a_published_record_never(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HO2: after each completion of stages 1 to 6, the next job deleted;
    then one tick enqueues exactly that stage, a second tick adds nothing.
    Before stage 7 (the chain's last stage) runs, its job is deleted too:
    a tick repairs it the same way. Once that repaired job runs and the
    publication is published, a further tick adds nothing (a published
    record is never repaired).

    M40 (its stage-6 case) -> at the stage-7-after-stage-6-deletion
    assertion below: correct = the repair re-enqueues ``publish:{site}:7``
    once the open publication record (opened by stage 6) has no job,
    mutant M40 (the repair skips open publication records) = no
    ``day_publish`` job reappears, the publication stalls forever.

    M43 -> at each day_close repair assertion below: correct = the
    repaired job's key names the SAME stage whose job was deleted (one
    stage ahead of ``last_stage``), mutant M43 (the repair enqueues the
    stage after ``last_stage`` + 1, one too far) = the repaired key names
    the NEXT stage instead, skipping the deleted one entirely.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        clock.set(BOUND)
        day_close_stops = [2, 3, 4, 5, 6]
        for stage in day_close_stops:
            await run_due_jobs(db, clock, stop_before=("day_close", stage))
            await _delete_job(db, "day_close", stage)
            await tick(db, clock)
            repaired = await _job_exists(db, "day_close", stage)
            assert len(repaired) == 1, (
                f"M43 -> at this assertion (stage {stage}): correct = one"
                f" day_close job keyed to end ':{stage}' reappears after"
                " one tick, mutant M43 (the repair enqueues last_stage + 2)"
                f" = a job keyed to end ':{stage + 1}' appears instead,"
                f" skipping stage {stage} entirely"
            )
            await tick(db, clock)
            still = await _job_exists(db, "day_close", stage)
            assert still == repaired, "a second tick must add no duplicate"

        await run_due_jobs(db, clock, stop_before=("day_publish", 7))
        await _delete_job(db, "day_publish", 7)
        await tick(db, clock)
        repaired7 = await _job_exists(db, "day_publish", 7)
        assert len(repaired7) == 1, (
            "M40 -> at this assertion: correct = one day_publish job keyed"
            " ':7' reappears (the repair covers an open publication record"
            " with no job), mutant M40 (the repair skips open publication"
            " records) = no job reappears, stalling the publication forever"
        )
        await tick(db, clock)
        still7 = await _job_exists(db, "day_publish", 7)
        assert still7 == repaired7, "a second tick must add no duplicate"

        await run_due_jobs(db, clock)
        before_count = await db.read(
            lambda c: c.execute("SELECT COUNT(*) FROM jobs").fetchone()
        )
        await tick(db, clock)
        after_count = await db.read(
            lambda c: c.execute("SELECT COUNT(*) FROM jobs").fetchone()
        )
        assert after_count[0] == before_count[0], (
            "once the publication is published, a tick must add no further"
            " day_close/day_publish job -- a published record is never"
            " repaired"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# HO3
# ---------------------------------------------------------------------------


def test_ho3_a_repeated_stage_job_is_skipped_not_rerun(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HO3: the standard night up to stage 3's completion, the stage-4
    job left ``pending``; the clock moved 1 minute; a second stage-3 job
    with the same payload inserted, its ``created_at`` set before the
    stage-4 job's so ``claim_next_job`` claims it first; run it alone,
    with no tick. It returns at once; its row ``completed``; one
    stage-log row ``skipped`` with its job id, beside stage 3's
    ``completed`` row; the attempt record's ``last_stage`` and
    ``last_progress_at`` unchanged; the stage-4 job still the only
    ``pending`` job; no second write of the stage (8.2).

    M44 -> at the stage-log-count assertion below: correct = exactly ONE
    'skipped' row for stage 3 (the repeated job's own body did no work,
    only recorded that it was skipped), mutant M44 (a stage body without
    its "already complete" check) = a SECOND 'completed' row for stage 3
    instead (the repeated job re-ran the whole stage, duplicating P2's
    writes).
    """

    async def _run() -> None:
        from wxverify.db.queue import claim_next_job
        from wxverify.worker.processor import run_claimed_job

        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        clock.set(BOUND)
        await run_due_jobs(db, clock, stop_before=("day_close", 4))

        before_attempt = await _attempt_row(db, site_id, D, 1)
        before_log_count = await db.read(
            lambda c: c.execute(
                "SELECT COUNT(*) FROM close_stage_log WHERE site_id = ? AND stage = 3",
                (site_id,),
            ).fetchone()
        )
        assert before_log_count[0] == 1, (
            "stage 3 must have completed exactly once first"
        )

        clock.advance(timedelta(minutes=1))

        def _insert_duplicate_stage3(conn: sqlite3.Connection) -> int:
            result = enqueue_if_absent(
                conn,
                "day_close",
                site_id,
                f"close:{site_id}:{D.isoformat()}:3",
                {"attempt": 1},
                max_retries=0,
            )
            assert result.created, "stage 3's completed job must not dedupe the new one"
            assert result.job_id is not None
            earlier = isoformat_utc(BOUND - timedelta(minutes=1))
            conn.execute(
                "UPDATE jobs SET created_at = ?, updated_at = ? WHERE id = ?",
                (earlier, earlier, result.job_id),
            )
            return result.job_id

        dup_job_id = await db.write(_insert_duplicate_stage3)

        job = await db.write(claim_next_job)
        assert job is not None and job.id == dup_job_id, (
            "the duplicate stage-3 job, created earlier than stage 4's,"
            " must be the one FIFO claims next"
        )
        await run_claimed_job(db, job, lane="main")

        job_row = await db.read(
            lambda c: c.execute(
                "SELECT status FROM jobs WHERE id = ?", (dup_job_id,)
            ).fetchone()
        )
        assert job_row["status"] == "completed", (
            "the repeated job must complete at once"
        )

        log_count = await db.read(
            lambda c: c.execute(
                "SELECT COUNT(*) FROM close_stage_log WHERE site_id = ? AND stage = 3",
                (site_id,),
            ).fetchone()
        )
        assert log_count[0] == 2, (
            "M44 -> at this assertion: correct = 2 stage-3 log rows total"
            " (the original 'completed' plus one 'skipped' for the"
            " repeat), mutant M44 (no already-complete check) = a second"
            " 'completed' row instead, re-running the stage's writes"
        )
        skipped = await db.read(
            lambda c: c.execute(
                "SELECT job_id FROM close_stage_log WHERE site_id = ? AND stage = 3"
                " AND outcome = 'skipped'",
                (site_id,),
            ).fetchone()
        )
        assert skipped is not None and skipped["job_id"] == dup_job_id, (
            "the skipped row must carry the repeated job's own id"
        )

        after_attempt = await _attempt_row(db, site_id, D, 1)
        assert (
            after_attempt["last_stage"],
            after_attempt["last_progress_at"],
            after_attempt["outcome"],
        ) == (
            before_attempt["last_stage"],
            before_attempt["last_progress_at"],
            before_attempt["outcome"],
        ), (
            "M90 -> at this assertion: correct = last_progress_at UNCHANGED"
            " (a repeated/skipped stage job writes only its stage-log row,"
            " 8.2 -- _open_stage's completion branch must not touch"
            " close_attempts at all), mutant M90 (the skipped path also"
            " runs the completing write's close_attempts UPDATE) = a NEW"
            " last_progress_at, stamped at the repeated job's own run time"
        )

        pending_day_close = await db.read(
            lambda c: c.execute(
                "SELECT job_key FROM jobs WHERE type = 'day_close'"
                " AND site_id = ? AND status = 'pending'",
                (site_id,),
            ).fetchall()
        )
        assert [row[0] for row in pending_day_close] == [
            f"close:{site_id}:{D.isoformat()}:4"
        ], "the stage-4 job must remain the only pending day_close job"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# HO4
# ---------------------------------------------------------------------------


def test_ho4_stage_6_refuses_over_stale_truth_fails_the_attempt(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HO4: between stage 4 and stage 6, one observations value of D
    changed through ``materialize_consensus``. Stage 6's ``finish_close``
    refuses; attempt 1 fails at stage 6; the retry rebuilds truth and
    closes (6.1, 8.3).

    Runs the standard night through ``run_due_jobs`` with ``stop_before``
    set to pause right before stage 6 claims, with stages 2-5 real (so D's
    ``daily_truth`` is genuinely current at that point, not hand-planted
    stale from the start -- the old version of this test seeded a stale
    row directly and never let stage 6's body run at all). Station A's
    hourly ``temperature`` reading for one hour of D is then changed for
    real, through ``write_station_observation``, and
    ``materialize_consensus`` recomputes the consensus and marks D's truth
    stale (``scoring/consensus.py:370-377``) -- P1's own, already-built
    mechanism (confirmed by the ``DayStateError`` check below, real
    ``db/site_days.py`` behaviour, unchanged from the old version of this
    test). ``run_due_jobs`` is then let continue: stage 6 claims, calls
    the real ``finish_close``, gets the ``DayStateError`` and -- once
    ``close_chain.fail_attempt`` exists -- fails the attempt instead of
    propagating or swallowing it. The clock is then moved past the
    retry's due instant and ``run_due_jobs`` runs again, so the retry
    rebuilds hours, truth and scores for real and closes D.

    M45 -> at the state assertion below: correct = 'retry_due' (the
    DayStateError fails the attempt at stage 6, 9.1), mutant M45 (stage 6
    catches DayStateError and closes anyway) = 'closed', over a truth row
    that no longer matches the day's hours.
    """

    async def _run() -> None:
        db, _writer, site_id, stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        clock.set(BOUND)
        await run_due_jobs(db, clock, stop_before=("day_close", 6))

        valid_at = f"{D.isoformat()}T12:00:00Z"
        change_at = BOUND + timedelta(minutes=1)
        station_a_id = stations["ISTATION01"]
        await db.write(
            lambda c: write_station_observation(
                c,
                station_id=station_a_id,
                variable="temperature",
                valid_at=valid_at,
                value=25.0,
                source_raw="synthetic-ho4",
                now_utc=change_at,
            )
        )
        await db.write(
            lambda c: materialize_consensus(
                c,
                site_id=site_id,
                variable="temperature",
                valid_at=valid_at,
                now_utc=change_at,
            )
        )

        # P1's own refusal, confirmed real and unchanged (db/site_days.py):
        # a direct finish_close over the now-stale truth raises on its own,
        # before stage 6's body is ever asked to handle it.
        with pytest.raises(DayStateError):
            await db.write(
                lambda c: finish_close(
                    c, site_id, D, now_utc=change_at + timedelta(minutes=1)
                )
            )

        await run_due_jobs(db, clock)

        record = await _state(db, site_id)
        assert record.state == "retry_due", (
            "M45 -> at this assertion: correct = 'retry_due', mutant M45"
            " (stage 6 catches DayStateError and closes anyway) = 'closed'"
        )
        attempt_row = await _attempt_row(db, site_id, D, 1)
        assert (attempt_row["outcome"], attempt_row["failed_stage"]) == (
            "failed",
            6,
        ), "the attempt record must show failed_stage 6 (9.1)"
        assert attempt_row["failure_reason"] == (
            f"site {site_id} day {D.isoformat()} (closing): the day's truth is stale"
        ), (
            "attempt 1's failure_reason must be the stale-truth DayStateError"
            " text itself (db/site_days.py's own _refuse message), not a"
            " generic or swallowed reason"
        )

        clock.advance(timedelta(hours=1, minutes=5))
        await run_due_jobs(db, clock)

        retried = await _state(db, site_id)
        assert (retried.state, retried.attempts) == ("closed", 2), (
            "the retry must rebuild truth from the changed hour and close D,"
            " at attempt 2 (6.1, 8.3)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# HO5
# ---------------------------------------------------------------------------


def test_ho5_a_pending_day_close_job_blocks_a_second_one(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HO5: while a ``day_close`` job of S is pending, a tick. No second
    ``day_close`` job for S (8.1).

    M46 -> at the assertion below: correct = exactly one pending
    ``day_close`` job keyed ``:2`` after the extra tick (``enqueue_if_absent``
    dedupes the attempt's own stage-2 job against itself), mutant M46
    (the stage number left out of the job key) does not change THIS
    assertion's outcome on its own -- see HO1 for M46's own discriminating
    oracle; this test's own purpose is the predicate in 8.5 step 3 (no
    second ``begin_close`` while a day_close job is pending), named with a
    plain message.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        # The clock starts at NOW, well before D's own day_end_utc -- move
        # it to the round's bound first (the pattern of TL3/TL8 in
        # tests/test_close_timeline.py), or the first attempt is never due
        # and no day_close job is ever created.
        clock.set(BOUND)
        await run_due_jobs(db, clock, stop_before=("day_close", 2))
        before = await db.read(
            lambda c: c.execute(
                "SELECT id FROM jobs WHERE type = 'day_close' AND site_id = ?",
                (site_id,),
            ).fetchall()
        )
        assert len(before) == 1, "exactly one day_close job must be pending"

        await tick(db, clock)

        after = await db.read(
            lambda c: c.execute(
                "SELECT id FROM jobs WHERE type = 'day_close' AND site_id = ?",
                (site_id,),
            ).fetchall()
        )
        assert [row[0] for row in after] == [row[0] for row in before], (
            "while a day_close job of S is pending, a further tick must"
            " create no second one (8.1)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# HO6
# ---------------------------------------------------------------------------


def test_ho6_a_failed_handoff_after_stage_6_fails_the_attempt_atomically(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HO6: ``enqueue_if_absent`` raises for ``day_publish`` inside stage
    6's completing write. D ``retry_due``; the attempt record ``failed``
    with ``failed_stage`` 6 and ``last_stage`` 5; no publication record;
    no ``day_publish`` job (8.2, 9.1).

    Runs the standard night through ``run_due_jobs`` with ``stop_before``
    set to pause right before stage 6 claims, stages 2-5 real. The real
    ``wxverify.db.queue.enqueue_if_absent`` is then patched onto
    ``wxverify.worker.close_chain.enqueue_if_absent`` -- the name under
    which ``close_chain``'s own stage-6 body will look it up, once it
    exists -- with a wrapper that raises only for ``day_publish`` and
    calls the real function for every other job type (``close_chain.py``
    does not define this name yet today, holding only
    ``record_collection_try``; ``raising=False`` tolerates patching a name
    piece 4 has not added). ``run_due_jobs`` then runs the real stage-6
    body: it calls ``finish_close`` and the raising enqueue inside its own
    ONE completing-write transaction (8.2), so the raise rolls back
    ``finish_close``'s effect together with the failed enqueue, and the
    exception reaches the real ``_fail_job`` terminal branch, which --
    once ``close_chain.fail_attempt`` exists -- fails the attempt.

    M42 -> at the state/publication assertions below: correct = D
    'retry_due', no ``close_publications`` row, no ``day_publish`` job
    (``finish_close``'s effect and the failed enqueue share one
    transaction, so neither survives the raise, and ``_fail_job`` finds D
    still 'closing' and fails the attempt, 9.1), mutant M42 (stage 6 saves
    its completion and enqueues stage 7 in TWO transactions) = D 'closed'
    with an opened ``close_publications`` row already committed before the
    enqueue failed, no attempt failure at all (``_fail_job``'s day_close
    branch finds D already 'closed', not 'closing', and only fails the job
    row, 9.1) -- a closure stranded without its successor's job.
    """

    def _raising_enqueue(
        conn: sqlite3.Connection,
        job_type: str,
        site_id: int,
        job_key: str,
        payload: dict[str, object] | None = None,
        *,
        max_retries: int | None = None,
    ) -> object:
        if job_type == "day_publish":
            raise RuntimeError("synthetic enqueue_if_absent failure for HO6")
        return enqueue_if_absent(
            conn, job_type, site_id, job_key, payload, max_retries=max_retries
        )

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)

        clock.set(BOUND)
        await run_due_jobs(db, clock, stop_before=("day_close", 6))

        monkeypatch.setattr(
            "wxverify.worker.close_chain.enqueue_if_absent",
            _raising_enqueue,
            raising=False,
        )

        await run_due_jobs(db, clock)

        record = await _state(db, site_id)
        assert record.state == "retry_due", (
            "M42 -> at this assertion: correct = 'retry_due' (finish_close's"
            " write is rolled back together with the failed enqueue, one"
            " transaction, so _fail_job finds D still 'closing' and fails"
            " the attempt, 9.1), mutant M42 (two transactions) = 'closed'"
            " already, with the attempt never failed at all"
        )
        attempt_row = await _attempt_row(db, site_id, D, 1)
        assert (
            attempt_row["outcome"],
            attempt_row["failed_stage"],
            attempt_row["last_stage"],
        ) == ("failed", 6, 5), (
            "the attempt record must show failed_stage 6, last_stage 5 (9.1)"
        )
        publication_count = await db.read(
            lambda c: c.execute(
                "SELECT COUNT(*) FROM close_publications"
                " WHERE site_id = ? AND cutoff = ?",
                (site_id, D.isoformat()),
            ).fetchone()
        )
        assert publication_count[0] == 0, (
            "M42 -> at this assertion: correct = 0 (no close_publications"
            " row: finish_close's publication-opening write shares the"
            " raised transaction), mutant M42 (two transactions) = 1, an"
            " opened record stranded with no day_publish job to run it"
        )
        publish_jobs = await db.read(
            lambda c: c.execute(
                "SELECT COUNT(*) FROM jobs WHERE type = 'day_publish' AND site_id = ?",
                (site_id,),
            ).fetchone()
        )
        assert publish_jobs[0] == 0, "no day_publish job may exist"

    asyncio.run(_run())
