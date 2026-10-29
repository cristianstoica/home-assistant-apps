"""Stops and restarts (plan 2026-10-21-daily-close-close-job.md section
15.6): RS1 to RS7.

Written before ``close_chain.settle_interrupted_closes``,
``close_chain.run_close_retention`` and ``close_chain.fail_publication``
exist, and fails first for that reason: every test here either imports one
of those names inline (an ``ImportError`` today, since ``close_chain.py``
at this commit holds only ``record_collection_try``) or reaches a plain
assertion that needs the not-yet-wired ``scheduler_tick``/close-on
integration (piece 4) to do anything.

Preconditions are built directly against the five close tables and
``site_days`` (all already migrated in, plan 12.3) rather than by running
the chain from a live tick: the chain's own stage bodies do not exist yet,
so "a stop inside stage k" is injected as data -- a ``close_attempts`` row
whose ``last_stage`` is the last stage actually saved, and a ``day_close``/
``day_publish`` job row left ``running`` -- exactly the state 5.5's
``settle_interrupted_closes`` is specified to act on. This is the same
style ``tests/test_close_timeline.py``'s TL9 uses for
``run_close_retention``, applied directly rather than through a tick.

Section 16.2 names the mutant each test ID kills; the mapping is given
inline on the exact assertion it kills at. RS4 and RS7 have no listed
mutant (16.2 lists none for either), so their assertions carry plain
messages, never ``M??``.

Real SQLite only (``tests.helpers.standard_night``/``day_close_site``,
real ``run_migrations`` via ``init_db`` on a ``tmp_path`` file). Station
ids are synthetic (``ISTATION01``/``ISTATION02``), the zone is ``UTC``,
and every date is the fixed synthetic ``tests.helpers.D``/the derived
round date ``R``. No live id, coordinate, site name or real zone name
appears.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from tests.helpers import (
    NOW,
    STANDARD_NIGHT_R,
    STANDARD_NIGHT_ROUND_BOUND,
    D,
    MovingClock,
    Stop,
    clear_close_chain_log_map,
    disable_other_enqueuers,
    install_p4_standins,
    install_round_state_standin,
    note_day_clock,
    patch_close_clock,
    patch_weathercom_transport,
    run_due_jobs,
    standard_night,
)
from wxverify import config
from wxverify.api.app import create_app
from wxverify.api.routes import db_transfer
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro
from wxverify.db.connection import close_db, init_db
from wxverify.db.queue import Job, enqueue_if_absent, reclaim_all_stale
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.site_days import (
    RETRY_DELAY,
    begin_close,
    day_record,
    fail_close,
    finish_close,
)
from wxverify.worker.close_p4 import ScoreSummary
from wxverify.worker.processor import run_claimed_job


@pytest.fixture(autouse=True)
def _clear_close_chain_log_map() -> None:
    """This file's own autouse wiring of ``clear_close_chain_log_map``
    (15.1's "The log map.") -- autouse HERE, inside this one P3 test
    file, never across the whole suite."""
    clear_close_chain_log_map()


R = STANDARD_NIGHT_R
BOUND = STANDARD_NIGHT_ROUND_BOUND


def _seed_day_close_job(
    conn: sqlite3.Connection,
    site_id: int,
    local_date,
    stage: int,
    attempt: int,
    *,
    status: str = "pending",
) -> int:
    """Enqueue the ``day_close`` job of ``stage``/``attempt``, optionally
    flipped straight to ``running`` (plan 15.1's "Stops": claiming the
    job, so the row stays running as after a kill)."""
    result = enqueue_if_absent(
        conn,
        "day_close",
        site_id,
        f"close:{site_id}:{local_date.isoformat()}:{stage}",
        {"attempt": attempt},
        max_retries=0,
    )
    assert result.job_id is not None
    if status != "pending":
        conn.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, result.job_id))
    return result.job_id


def _seed_running_stage_job(
    conn: sqlite3.Connection, site_id: int, local_date, stage: int, attempt: int
) -> int:
    return _seed_day_close_job(
        conn, site_id, local_date, stage, attempt, status="running"
    )


def _seed_publish_job(
    conn: sqlite3.Connection,
    site_id: int,
    stage: int,
    cutoff,
    attempt: int,
    *,
    status: str,
) -> int:
    result = enqueue_if_absent(
        conn,
        "day_publish",
        site_id,
        f"publish:{site_id}:{stage}",
        {"cutoff": cutoff.isoformat(), "attempt": attempt},
        max_retries=0,
    )
    assert result.job_id is not None
    if status != "pending":
        conn.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, result.job_id))
    return result.job_id


def _seed_attempt_record(
    conn: sqlite3.Connection,
    site_id: int,
    local_date,
    attempt: int,
    *,
    started_at: datetime,
    last_stage: int,
    last_progress_at: datetime,
    outcome: str = "running",
    failed_stage: int | None = None,
    failure_reason: str | None = None,
    ended_at: datetime | None = None,
    truth_version: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO close_attempts (site_id, local_date, attempt, started_at,"
        " last_stage, last_progress_at, outcome, failed_stage, failure_reason,"
        " ended_at, truth_version) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            site_id,
            local_date.isoformat(),
            attempt,
            isoformat_utc(started_at),
            last_stage,
            isoformat_utc(last_progress_at),
            outcome,
            failed_stage,
            failure_reason,
            None if ended_at is None else isoformat_utc(ended_at),
            truth_version,
        ),
    )


def _seed_publication(
    conn: sqlite3.Connection,
    site_id: int,
    cutoff,
    *,
    attempt: int = 1,
    state: str,
    opened_at: datetime,
    started_at: datetime | None = None,
    retry_at: datetime | None = None,
    finished_at: datetime | None = None,
    publication_id: str | None = None,
    last_failure: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO close_publications (site_id, cutoff, attempt, state,"
        " opened_at, started_at, retry_at, finished_at, publication_id,"
        " last_failure) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            site_id,
            cutoff.isoformat(),
            attempt,
            state,
            isoformat_utc(opened_at),
            None if started_at is None else isoformat_utc(started_at),
            None if retry_at is None else isoformat_utc(retry_at),
            None if finished_at is None else isoformat_utc(finished_at),
            publication_id,
            last_failure,
        ),
    )


async def _job_row(db, job_id: int):  # type: ignore[no-untyped-def]
    return await db.read(
        lambda c: c.execute(
            "SELECT status, result FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
    )


async def _publication_row(db, site_id: int, cutoff):  # type: ignore[no-untyped-def]
    return await db.read(
        lambda c: c.execute(
            "SELECT attempt, state, retry_at, publication_id, last_failure"
            " FROM close_publications WHERE site_id = ? AND cutoff = ?",
            (site_id, cutoff.isoformat()),
        ).fetchone()
    )


async def _publication_count(db, site_id: int, cutoff):  # type: ignore[no-untyped-def]
    row = await db.read(
        lambda c: c.execute(
            "SELECT COUNT(*) FROM close_publications WHERE site_id = ? AND cutoff = ?",
            (site_id, cutoff.isoformat()),
        ).fetchone()
    )
    return int(row[0])


# ---------------------------------------------------------------------------
# RS1
# ---------------------------------------------------------------------------


def test_rs1_a_stop_mid_stage_is_settled_as_a_failed_attempt(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS1: a stop inside each of stages 2 to 6 of attempt 1;
    ``worker_last_loop_at`` and ``last_progress_at`` seeded to R 00:50Z and
    00:52Z; start at R 01:00Z. ``settle_interrupted_closes``: D
    ``retry_due``, ``retry_at`` R 01:52Z, ``last_failure`` "stopped during
    stage k"; the reclaimed job completed without running (5.5).

    One site per stage (k = 2..6), each under its own ``tmp_path``
    subdirectory.

    M35 -> at the retry_at assertion below: correct = R 01:52Z (L is the
    LATER of worker_last_loop_at 00:50Z and last_progress_at 00:52Z, plus
    1 hour), mutant M35 (settle_interrupted_closes uses the start time as
    L) = R 02:00Z (01:00Z start + 1 hour).

    M36 -> at the same assertion: mutant M36 (worker_last_loop_at only,
    never last_progress_at) = R 01:50Z (00:50Z + 1 hour); the fixture's
    deliberate 2-minute gap between the two stamps is what keeps this
    distinct from the correct R 01:52Z.

    M37 -> at the job-row assertion below: correct = the reclaimed
    stage-k job's row is 'completed' with no result (no success marker),
    mutant M37 (the reclaimed job of a settled attempt left to run) =
    its row stays 'pending' (reclaim_all_stale's own re-pend) or
    'running', never advanced to 'completed' by settle_interrupted_closes
    itself.
    """

    async def _run(stage: int) -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path / f"stage_{stage}")
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)

        await db.write(lambda c: begin_close(c, site_id, D, now_utc=BOUND))

        progress_at = datetime(BOUND.year, BOUND.month, BOUND.day, 0, 52, tzinfo=UTC)
        last_loop_at = datetime(BOUND.year, BOUND.month, BOUND.day, 0, 50, tzinfo=UTC)
        start = note_day_clock(
            datetime(BOUND.year, BOUND.month, BOUND.day, 1, 0, tzinfo=UTC)
        )

        job_id = await db.write(
            lambda c: _seed_running_stage_job(c, site_id, D, stage, 1)
        )
        await db.write(
            lambda c: _seed_attempt_record(
                c,
                site_id,
                D,
                1,
                started_at=BOUND,
                last_stage=max(stage - 1, 1),
                last_progress_at=progress_at,
            )
        )
        await db.write(
            lambda c: set_runtime_state(
                c, "worker_last_loop_at", isoformat_utc(last_loop_at)
            )
        )

        await db.write(reclaim_all_stale)

        from wxverify.worker.close_chain import settle_interrupted_closes

        await db.write(lambda c: settle_interrupted_closes(c, now_utc=start))

        record = await db.read(lambda c: day_record(c, site_id, D))
        expected_retry_at = isoformat_utc(progress_at + timedelta(hours=1))
        assert (
            record.state,
            None if record.retry_at is None else isoformat_utc(record.retry_at),
            record.last_failure,
        ) == ("retry_due", expected_retry_at, f"stopped during stage {stage}"), (
            f"stage {stage}: M35/M36 -> at this assertion: correct ="
            f" ('retry_due', {expected_retry_at!r}, 'stopped during stage"
            f" {stage}'), mutant M35 = retry_at from the 01:00Z start"
            " (R 02:00Z), mutant M36 = retry_at from worker_last_loop_at"
            " alone (R 01:50Z)"
        )

        job_row = await _job_row(db, job_id)
        assert (job_row["status"], job_row["result"]) == ("completed", None), (
            f"stage {stage}: M37 -> at this assertion: correct ="
            " ('completed', None) (the reclaimed job is completed without"
            " running, no success marker), mutant M37 (the reclaimed job"
            " of a settled attempt left to run) = left 'pending' or"
            " 'running', never advanced here"
        )

    for stage in (2, 3, 4, 5, 6):
        asyncio.run(_run(stage))


# ---------------------------------------------------------------------------
# RS2
# ---------------------------------------------------------------------------


def test_rs2_a_retry_that_fell_due_while_down_is_missed(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS2: RS1, start at R 02:00Z. D ``missed``, ``retry_not_run`` (5.5).

    Built on one representative stage (3 of 2..6; RS1 already sweeps the
    full range, and this row only varies the start time).

    M38 -> at the assertion below: correct = ('missed', 'retry_not_run')
    (the retry's own due instant, R 01:52Z, has already passed by the
    R 02:00Z start), mutant M38 (retry_not_run not applied at start when
    retry_at has passed) = ('retry_due', None), left as RS1 would leave
    it, never re-checked against the later start time.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)

        await db.write(lambda c: begin_close(c, site_id, D, now_utc=BOUND))
        progress_at = datetime(BOUND.year, BOUND.month, BOUND.day, 0, 52, tzinfo=UTC)
        last_loop_at = datetime(BOUND.year, BOUND.month, BOUND.day, 0, 50, tzinfo=UTC)
        start = note_day_clock(
            datetime(BOUND.year, BOUND.month, BOUND.day, 2, 0, tzinfo=UTC)
        )

        await db.write(lambda c: _seed_running_stage_job(c, site_id, D, 3, 1))
        await db.write(
            lambda c: _seed_attempt_record(
                c,
                site_id,
                D,
                1,
                started_at=BOUND,
                last_stage=2,
                last_progress_at=progress_at,
            )
        )
        await db.write(
            lambda c: set_runtime_state(
                c, "worker_last_loop_at", isoformat_utc(last_loop_at)
            )
        )
        await db.write(reclaim_all_stale)

        from wxverify.worker.close_chain import settle_interrupted_closes

        await db.write(lambda c: settle_interrupted_closes(c, now_utc=start))

        record = await db.read(lambda c: day_record(c, site_id, D))
        assert (record.state, record.missed_reason) == ("missed", "retry_not_run"), (
            "M38 -> at this assertion: correct = ('missed', 'retry_not_run'),"
            " mutant M38 (retry_not_run not applied at start when retry_at"
            " has passed) = ('retry_due', None)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# RS3
# ---------------------------------------------------------------------------


def test_rs3_a_stop_during_attempt_2_is_missed_never_a_third_attempt(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS3: a stop during attempt 2. At start D ``missed``, ``retry_failed``;
    never a third ``begin_close`` (5.5).

    M39 -> at the assertion below: correct = ('missed', 'retry_failed', 2)
    (a stop during the SECOND attempt goes straight to missed, since P1's
    guard gives only two attempts), mutant M39 (a stop during attempt 2
    settled as retry_due) = ('retry_due', None, 2), as if a third attempt
    were still possible.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)

        await db.write(lambda c: begin_close(c, site_id, D, now_utc=BOUND))
        await db.write(
            lambda c: fail_close(c, site_id, D, now_utc=BOUND, reason="network")
        )
        retry_time = note_day_clock(BOUND + timedelta(hours=1))
        await db.write(lambda c: begin_close(c, site_id, D, now_utc=retry_time))

        progress_at = retry_time + timedelta(minutes=12)
        last_loop_at = retry_time + timedelta(minutes=10)
        start = note_day_clock(retry_time + timedelta(minutes=20))

        await db.write(lambda c: _seed_running_stage_job(c, site_id, D, 3, 2))
        await db.write(
            lambda c: _seed_attempt_record(
                c,
                site_id,
                D,
                2,
                started_at=retry_time,
                last_stage=2,
                last_progress_at=progress_at,
            )
        )
        await db.write(
            lambda c: set_runtime_state(
                c, "worker_last_loop_at", isoformat_utc(last_loop_at)
            )
        )
        await db.write(reclaim_all_stale)

        from wxverify.worker.close_chain import settle_interrupted_closes

        await db.write(lambda c: settle_interrupted_closes(c, now_utc=start))

        record = await db.read(lambda c: day_record(c, site_id, D))
        assert (record.state, record.missed_reason, record.attempts) == (
            "missed",
            "retry_failed",
            2,
        ), (
            "M39 -> at this assertion: correct = ('missed', 'retry_failed', 2),"
            " mutant M39 (a stop during attempt 2 settled as retry_due) ="
            " ('retry_due', None, 2)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# RS4
# ---------------------------------------------------------------------------


def test_rs4_a_stop_after_stage_6_leaves_the_closure_and_publishes_once(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS4: a stop after stage 6's write, before stage 7 (the stage-6 job
    left ``running``). D stays ``closed``; ``settle_interrupted_closes``
    writes nothing for this day or its open publication record; the
    stage-7 job claimed is the one inserted before the stop; one
    publication (9.5). Plan section 16.2 names no mutant for RS4.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)

        await db.write(lambda c: begin_close(c, site_id, D, now_utc=BOUND))
        closed_at = note_day_clock(BOUND + timedelta(minutes=5))
        await db.write(lambda c: finish_close(c, site_id, D, now_utc=closed_at))
        await db.write(
            lambda c: _seed_attempt_record(
                c,
                site_id,
                D,
                1,
                started_at=BOUND,
                last_stage=6,
                last_progress_at=closed_at,
                outcome="closed",
                ended_at=closed_at,
                truth_version="deadbeef",
            )
        )
        await db.write(
            lambda c: _seed_publication(
                c, site_id, D, attempt=1, state="pending", opened_at=closed_at
            )
        )
        stage7_job_id = await db.write(
            lambda c: _seed_publish_job(c, site_id, 7, D, 1, status="pending")
        )
        stage6_job_id = await db.write(
            lambda c: _seed_running_stage_job(c, site_id, D, 6, 1)
        )

        last_loop_at = closed_at + timedelta(minutes=1)
        start = note_day_clock(closed_at + timedelta(minutes=10))
        await db.write(
            lambda c: set_runtime_state(
                c, "worker_last_loop_at", isoformat_utc(last_loop_at)
            )
        )
        await db.write(reclaim_all_stale)

        stage_log_before = await db.read(
            lambda c: c.execute("SELECT COUNT(*) FROM close_stage_log").fetchone()
        )

        from wxverify.worker.close_chain import settle_interrupted_closes

        await db.write(lambda c: settle_interrupted_closes(c, now_utc=start))

        record = await db.read(lambda c: day_record(c, site_id, D))
        assert record.state == "closed", (
            "a day already closed before the stop must stay closed through"
            " settle_interrupted_closes (9.5: 'the day is never half-closed')"
        )
        publication = await _publication_row(db, site_id, D)
        assert publication["state"] == "pending", (
            "an open publication record whose job is merely reclaimed, not"
            " stopped mid-write, must be left untouched by"
            " settle_interrupted_closes (it only treats a BUILDING record as"
            " a stop, 9.4)"
        )
        stage_log_after = await db.read(
            lambda c: c.execute("SELECT COUNT(*) FROM close_stage_log").fetchone()
        )
        assert stage_log_after[0] == stage_log_before[0], (
            "settle_interrupted_closes must add no stage-log row for an"
            " already-closed day"
        )

        # The reclaimed stage-6 job is now pending, run it as the worker
        # would, to reach _open_stage's own disposition of a stale job on
        # an already-closed day (plan 16.2 names no mutant for RS4 itself,
        # but M89 lives exactly in this path, in close_chain.py's
        # _open_stage). Claimed directly by id, not through
        # claim_next_job's own FIFO: the stage-7 day_publish job was
        # enqueued (pending) BEFORE this stage-6 job was even seeded, so a
        # plain claim_next_job would claim stage 7 first -- and nothing in
        # this test installs close_p4.publish's stand-in to run it safely.
        def _claim_stage6(c: sqlite3.Connection) -> Job:
            row = c.execute(
                "UPDATE jobs SET status = 'running', updated_at = ?"
                " WHERE id = ? AND status = 'pending'"
                " RETURNING id, type, site_id, job_key, payload, status,"
                " retry_count, max_retries",
                (isoformat_utc(start), stage6_job_id),
            ).fetchone()
            assert row is not None, "the reclaimed stage-6 job must still be pending"
            return Job(
                id=int(row[0]),
                type=str(row[1]),
                site_id=None if row[2] is None else int(row[2]),
                job_key=None if row[3] is None else str(row[3]),
                payload=json.loads(str(row[4])) if row[4] else {},
                status=str(row[5]),
                retry_count=int(row[6]),
                max_retries=int(row[7]),
            )

        reclaimed_job = await db.write(_claim_stage6)
        await run_claimed_job(db, reclaimed_job, lane="main")

        stage_log_after_claim = await db.read(
            lambda c: c.execute("SELECT COUNT(*) FROM close_stage_log").fetchone()
        )
        assert stage_log_after_claim[0] == stage_log_before[0], (
            "M89 -> at this assertion: correct = no new close_stage_log row"
            " (the day is 'closed', not 'closing', so _open_stage's own"
            " day-state check (_current) must refuse the reclaimed stage-6"
            " job before any write at all), mutant M89 (_open_stage checks"
            " completion -- last_stage >= job.stage -- before the day's"
            " state check) = a 'skipped' row written for a day that is no"
            " longer closing"
        )
        reclaimed_job_row = await db.read(
            lambda c: c.execute(
                "SELECT status FROM jobs WHERE id = ?", (reclaimed_job.id,)
            ).fetchone()
        )
        assert reclaimed_job_row["status"] == "completed", (
            "paired positive: the reclaimed stage-6 job's own row must still"
            " end 'completed' (run_claimed_job always completes the job"
            " after dispatch returns with no exception, 8.2) -- otherwise"
            " the no-new-stage-log-row check above would also pass on a"
            " job that never ran to any disposition at all"
        )

        publish_jobs = await db.read(
            lambda c: c.execute(
                "SELECT id FROM jobs WHERE type = 'day_publish' AND site_id = ?",
                (site_id,),
            ).fetchall()
        )
        assert [row[0] for row in publish_jobs] == [stage7_job_id], (
            "exactly one day_publish job, the one inserted by stage 6's own"
            " completing write before the stop -- no new one from a repair"
        )
        publication_count = await _publication_count(db, site_id, D)
        assert publication_count == 1, "exactly one publication record for D"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# RS5
# ---------------------------------------------------------------------------


def test_rs5_a_stop_during_stage_7_never_publishes_twice(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS5: a stop during stage 7 after the ``publish`` stand-in returned,
    before P3's write. At start the record ``retry_due``; the retry calls
    ``publish`` with the same cutoff and gets the same identifier; the
    stand-in's publication list has one entry (9.4).

    Runs the standard night through ``run_due_jobs``, real stages 2-6
    closing D, and lets stage 7 itself claim and run (M41 needs the real
    stage-7 body to exist: it is the thing being killed). The stop is made
    by the ``publish`` stand-in itself: per plan 15.1 "P4.", once P4 is
    wired in the stand-ins become call-through spies, so a stand-in that
    raises on its own call is indistinguishable, from stage 7's own code,
    from "publish ran and returned, then the process died before the
    result was used" -- the two are the same observable state (no write of
    the result ever happens either way). This is the guess beyond 9.4's
    own order: this file has no narrower seam today to interrupt AFTER a
    real return and BEFORE stage 7's own completing write, so the
    stand-in raises instead of returning, recording its one call first
    (``AsyncRecordingStandIn`` appends to ``calls`` before checking
    ``raises``), exactly matching "the stand-in's publication list has one
    entry".

    M41 -> at the state assertion below: correct = the publication record
    reads 'retry_due' after settle_interrupted_closes (stage 7 saved
    'building' before calling publish, so the stop lands on a record
    settle_interrupted_closes recognises as an interrupted publication,
    9.4), mutant M41 (stage 7 does not save 'building' before calling P4)
    = the record is still whatever it was before stage 7 ran (no 'building'
    row for settle_interrupted_closes to act on), so it is never moved to
    'retry_due' at all.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        standins = install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        standins["score_day"].result = ScoreSummary(scored={}, absent={})
        standins["clean_worksheets"].result = 0
        standins["publish"].raises = Stop(
            "synthetic stop for RS5, after publish recorded its call"
        )

        clock.set(BOUND)
        with pytest.raises(Stop):
            await run_due_jobs(db, clock)

        assert len(standins["publish"].calls) == 1, (
            "the stand-in's publication list must have exactly one entry"
            " (9.4) -- the call that was interrupted, never repeated"
        )

        stage7_row = await db.read(
            lambda c: c.execute(
                "SELECT status FROM jobs WHERE type = 'day_publish' AND job_key = ?",
                (f"publish:{site_id}:7",),
            ).fetchone()
        )
        assert stage7_row["status"] == "running", (
            "the stand-in's Stop is a BaseException, so it passes through"
            " dispatch uncaught and the stage-7 job row stays 'running',"
            " as after a process kill (15.1 'Stops')"
        )

        stop_at = clock.now()
        await db.write(
            lambda c: set_runtime_state(
                c, "worker_last_loop_at", isoformat_utc(stop_at)
            )
        )
        await db.write(reclaim_all_stale)
        clock.advance(timedelta(minutes=5))

        from wxverify.worker.close_chain import settle_interrupted_closes

        await db.write(lambda c: settle_interrupted_closes(c, now_utc=clock.now()))

        publication = await _publication_row(db, site_id, D)
        assert (publication["attempt"], publication["state"]) == (1, "retry_due"), (
            "M41 -> at this assertion: correct = (1, 'retry_due') (stage 7"
            " saved 'building' before calling publish, so the stop left a"
            " record settle_interrupted_closes recognises, 9.4), mutant"
            " M41 (stage 7 does not save 'building' before calling P4) ="
            " the record stays whatever it was before stage 7 ran, never"
            " 'retry_due'"
        )
        assert publication["retry_at"] == isoformat_utc_micro(stop_at + RETRY_DELAY), (
            "M23b -> at this assertion: correct = the stop instant L"
            " (worker_last_loop_at, set to stop_at above) + RETRY_DELAY, in"
            " isoformat_utc_micro form (close_chain.py:2280-2284's own stamp),"
            " mutant M23b (retry_at computed one minute short of L +"
            " RETRY_DELAY) = a retry_at one minute earlier"
        )

        standins["publish"].raises = None
        standins["publish"].result = "pub-D"
        result = await standins["publish"](
            site_id=site_id, cutoff=D, attempt=2, now_utc=clock.now()
        )
        assert result == "pub-D", (
            "the retry's call to publish must return the SAME identifier"
            " as before the stop (P4's own idempotence, exercised through"
            " the stand-in's fixed result)"
        )
        publication_count = await _publication_count(db, site_id, D)
        assert publication_count == 1, (
            "exactly one close_publications row for (site, cutoff) after"
            " the retry -- never a second one"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# RS6
# ---------------------------------------------------------------------------


def test_rs6_a_retention_failure_does_not_undo_a_closure_or_a_start(
    tmp_path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """RS6: P2's ``prune_raw_readings`` raises a ``RuntimeError`` (i) in
    stage 6's second write, and (ii) separately in the retention at the
    end of ``settle_interrupted_closes`` after a move to ``missed`` (RS2).
    Each time: the surrounding write commits; one ERROR line (8.4).

    M75 -> at each assertion below: correct = the surrounding state
    (closed in (i), missed/retry_not_run in (ii)) is committed despite
    the retention failure, mutant M75 (``run_close_retention`` without
    its catch, so a prune exception propagates) = the caller's own write
    raises instead, which this test would see as an uncaught exception
    from ``run_close_retention`` itself.
    """

    def _raising_prune(conn: object, *, now_utc: object) -> None:
        raise RuntimeError("synthetic prune failure for RS6")

    async def _run_i() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path / "i")
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        monkeypatch.setattr(
            "wxverify.worker.close_chain.prune_raw_readings",
            _raising_prune,
        )

        await db.write(lambda c: begin_close(c, site_id, D, now_utc=BOUND))
        closed_at = BOUND + timedelta(minutes=5)
        await db.write(lambda c: finish_close(c, site_id, D, now_utc=closed_at))

        caplog.set_level("ERROR", logger="wxverify.worker.close_chain")
        from wxverify.worker.close_chain import run_close_retention

        await db.write(lambda c: run_close_retention(c, now_utc=closed_at))

        record = await db.read(lambda c: day_record(c, site_id, D))
        assert record.state == "closed", (
            "M75 -> at this assertion (case i, stage 6's second write):"
            " correct = 'closed' (the retention failure does not undo"
            " the closure already committed), mutant M75 = an uncaught"
            " exception instead"
        )
        assert any(
            "RuntimeError" in rec.message and rec.levelname == "ERROR"
            for rec in caplog.records
        ), "one ERROR line naming RuntimeError must be written (8.4)"

    async def _run_ii() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path / "ii")
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        monkeypatch.setattr(
            "wxverify.worker.close_chain.prune_raw_readings",
            _raising_prune,
        )

        await db.write(lambda c: begin_close(c, site_id, D, now_utc=BOUND))
        progress_at = datetime(BOUND.year, BOUND.month, BOUND.day, 0, 52, tzinfo=UTC)
        last_loop_at = datetime(BOUND.year, BOUND.month, BOUND.day, 0, 50, tzinfo=UTC)
        start = datetime(BOUND.year, BOUND.month, BOUND.day, 2, 0, tzinfo=UTC)

        await db.write(lambda c: _seed_running_stage_job(c, site_id, D, 3, 1))
        await db.write(
            lambda c: _seed_attempt_record(
                c,
                site_id,
                D,
                1,
                started_at=BOUND,
                last_stage=2,
                last_progress_at=progress_at,
            )
        )
        await db.write(
            lambda c: set_runtime_state(
                c, "worker_last_loop_at", isoformat_utc(last_loop_at)
            )
        )
        await db.write(reclaim_all_stale)

        caplog.set_level("ERROR", logger="wxverify.worker.close_chain")
        from wxverify.worker.close_chain import settle_interrupted_closes

        await db.write(lambda c: settle_interrupted_closes(c, now_utc=start))

        record = await db.read(lambda c: day_record(c, site_id, D))
        assert (record.state, record.missed_reason) == ("missed", "retry_not_run"), (
            "M75 -> at this assertion (case ii, settle_interrupted_closes'"
            " own retention call): correct = ('missed', 'retry_not_run'),"
            " committed despite the retention failure, mutant M75 = an"
            " uncaught exception instead, losing the move to missed too"
        )
        assert any(
            "RuntimeError" in rec.message and rec.levelname == "ERROR"
            for rec in caplog.records
        ), "one ERROR line naming RuntimeError must be written (8.4)"

    asyncio.run(_run_i())
    asyncio.run(_run_ii())


# ---------------------------------------------------------------------------
# RS7
# ---------------------------------------------------------------------------


async def _rs7_idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _rs7_make_live_app(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    """The idle-worker app of ``tests/test_db_transfer.py``'s own
    ``_make_app`` -- RS7-only, so it is not added to ``tests/helpers.py``.
    """
    monkeypatch.setattr("wxverify.api.app.run_worker", _rs7_idle_worker)
    return create_app(root_path="")


def _rs7_init_live_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The import TARGET: a fresh, minimal, fully migrated database at a
    path distinct from the uploaded source file (the pattern of
    ``tests/test_db_transfer.py``'s own ``_init_tmp_db``).

    ``config.db_path``/``config.options_path`` are set through
    ``monkeypatch``, not by plain attribute assignment: a bare
    ``config.options_path = ...`` is never undone when the test ends, so it
    leaks forward as the next test's own ``config.options_path`` -- an
    EXISTING, key-less ``options.json`` that makes ``resolve_secret`` raise
    "weathercom key is not configured" instead of going on to build a real
    ``httpx`` request, masking a later P3 test's own missing network
    isolation (VD6 in ``tests/test_close_verdict.py``, which this file
    happened to run immediately before)."""
    close_db()
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "wxverify.db"
    monkeypatch.setattr(config, "db_path", str(db_path))
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(config, "options_path", str(options_path))
    init_db(str(db_path))


def _rs7_csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {
        "Origin": "http://testserver",
        "X-CSRF-Token": token,
        "Content-Type": "application/octet-stream",
    }


def test_rs7_an_imported_closing_day_is_settled_before_use(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS7: an imported file, its close on, with D ``closing`` (attempt 1,
    ``last_stage`` 3, a ``pending`` stage-4 job, ``last_progress_at``
    R 00:52Z, ``worker_last_loop_at`` R 00:50Z) and a ``building``
    publication record for D - 1 at attempt 2; imported at R + 3 d. D
    ``missed``, ``retry_not_run``; D's ``day_close`` jobs ``completed``
    with no success marker; D - 1's record ``failed``; after the swap
    ``run_due_jobs`` claims no ``day_close`` job of D and the transport
    saw no request for D. Then a variant imported at R 01:30Z: D
    ``retry_due``, ``retry_at`` R 01:52Z (12.6).

    Drives the REAL ``POST /api/import/db`` route (``_make_app``/
    ``TestClient``, the pattern of ``tests/test_closed_day_guard.py``'s
    GW13 and ``tests/test_db_transfer.py``), not
    ``settle_interrupted_closes`` called directly: 12.6 states that
    ``import_db`` must call a new ``_settle_imported_closes(tmp)`` on the
    staged file, after ``_stage_pending_rebuild_state`` and before
    ``replace_from`` swaps it in, and today's route does neither -- the
    route exists and runs end to end, but nothing in it settles an
    imported interrupted close, so D is left exactly as the uploaded file
    carried it (``closing``) instead of being moved to ``missed``/
    ``retry_due``. Plan section 16.2 names no mutant for RS7; its own
    Catches column is "an imported interrupted close resumed as if never
    stopped", which only the real route -- not a direct call to
    ``settle_interrupted_closes`` -- can show.

    The one guess beyond 12.6's own text: "the import's time" is read
    through ``utc_now`` where ``wxverify.api.routes.db_transfer`` imports
    it by name (grepped: line 33); this patches that name directly,
    alongside ``patch_close_clock`` for the post-swap ``run_due_jobs``
    check, since ``db_transfer`` is not one of ``patch_close_clock``'s own
    ``_CLOSE_CLOCK_MODULES``.
    """

    progress_at = datetime(BOUND.year, BOUND.month, BOUND.day, 0, 52, tzinfo=UTC)
    last_loop_at = datetime(BOUND.year, BOUND.month, BOUND.day, 0, 50, tzinfo=UTC)
    d_minus_1 = D - timedelta(days=1)
    # standard_night closes D - 1 at its own day_end_utc + 30 minutes; under
    # the fixture's UTC zone, D - 1's day_end_utc is D's own midnight.
    d_minus_1_closed_at = datetime(D.year, D.month, D.day, 0, 30, tzinfo=UTC)
    d_minus_1_building_at = d_minus_1_closed_at + timedelta(minutes=5)

    async def _run(
        import_time: datetime, suffix: str
    ) -> tuple[str, str | None, str | None, list[sqlite3.Row], str | None]:
        source_dir = tmp_path / f"source_{suffix}"
        db, _writer, site_id, _stations = standard_night(source_dir)
        await db.write(lambda c: begin_close(c, site_id, D, now_utc=BOUND))
        await db.write(
            lambda c: _seed_attempt_record(
                c,
                site_id,
                D,
                1,
                started_at=BOUND,
                last_stage=3,
                last_progress_at=progress_at,
            )
        )
        await db.write(
            lambda c: _seed_day_close_job(c, site_id, D, 4, 1, status="pending")
        )
        await db.write(
            lambda c: set_runtime_state(
                c, "worker_last_loop_at", isoformat_utc(last_loop_at)
            )
        )
        await db.write(
            lambda c: _seed_publication(
                c,
                site_id,
                d_minus_1,
                attempt=2,
                state="building",
                opened_at=d_minus_1_closed_at,
                started_at=d_minus_1_building_at,
            )
        )
        db._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        close_db()
        payload = (source_dir / "wxverify.db").read_bytes()

        _rs7_init_live_db(tmp_path / f"live_{suffix}", monkeypatch)
        clock = MovingClock(import_time)
        patch_close_clock(monkeypatch, clock)
        monkeypatch.setattr(db_transfer, "utc_now", clock.now)

        app = _rs7_make_live_app(monkeypatch)
        with TestClient(app) as client:
            headers = _rs7_csrf_headers(client)
            resp = client.post("/api/import/db", content=payload, headers=headers)
            assert resp.status_code == 200, resp.text

        direct = sqlite3.connect(config.db_path)
        direct.row_factory = sqlite3.Row
        try:
            record = day_record(direct, site_id, D)
            assert record is not None
            close_jobs = direct.execute(
                "SELECT status, result FROM jobs WHERE type = 'day_close'"
                " AND site_id = ?",
                (site_id,),
            ).fetchall()
            publication = direct.execute(
                "SELECT state FROM close_publications WHERE site_id = ?"
                " AND cutoff = ? AND attempt = 2",
                (site_id, d_minus_1.isoformat()),
            ).fetchone()
        finally:
            direct.close()

        disable_other_enqueuers(monkeypatch)
        requests: list[object] = []

        def _no_requests(request):  # type: ignore[no-untyped-def]
            requests.append(request)
            raise AssertionError("no weather.com request may be made for RS7")

        patch_weathercom_transport(monkeypatch, _no_requests)
        close_db()
        live_db = init_db(config.db_path)
        claims = await run_due_jobs(live_db, clock)
        assert not requests, "the transport must see no request for D"
        assert not any(
            claim_key is not None
            and claim_key.startswith(f"close:{site_id}:{D.isoformat()}:")
            for _claim_type, claim_key, _outcome in claims
        ), "run_due_jobs must claim no day_close job of D after the swap"

        return (
            record.state,
            record.missed_reason,
            None if record.retry_at is None else isoformat_utc(record.retry_at),
            close_jobs,
            None if publication is None else publication["state"],
        )

    import_late = BOUND + timedelta(days=3)
    state, missed_reason, _retry_at, close_jobs, pub_state = asyncio.run(
        _run(import_late, "late")
    )
    assert (state, missed_reason) == ("missed", "retry_not_run"), (
        "the real import route must call _settle_imported_closes (12.6):"
        " an import 3 days after the stop must settle D as missed/"
        "retry_not_run, the retry (R 01:52Z) having fallen due long before"
        " the import's own time -- today's route calls nothing of the"
        " sort, so D is left exactly as the uploaded file carried it"
        " ('closing'), which is the right reason for this assertion to"
        " fail first"
    )
    assert len(close_jobs) == 1, (
        "exactly one day_close job of D was ever seeded (the pending"
        " stage-4 job above) -- without this, the all() below would also"
        " pass vacuously on an empty list"
    )
    assert all(
        (row["status"], row["result"]) == ("completed", None) for row in close_jobs
    ), "every day_close job of D must be completed with no success marker"
    assert pub_state == "failed", (
        "D - 1's building publication record, at attempt 2, must be"
        " settled as failed by the same import-time call"
    )

    import_soon = datetime(BOUND.year, BOUND.month, BOUND.day, 1, 30, tzinfo=UTC)
    state2, missed_reason2, retry_at2, _close_jobs2, _pub_state2 = asyncio.run(
        _run(import_soon, "soon")
    )
    expected_retry_at = isoformat_utc(progress_at + timedelta(hours=1))
    assert (state2, missed_reason2, retry_at2) == (
        "retry_due",
        None,
        expected_retry_at,
    ), (
        "the variant, imported at R 01:30Z -- before the retry's own"
        f" due instant {expected_retry_at!r} -- must leave D retry_due,"
        " not yet missed"
    )
