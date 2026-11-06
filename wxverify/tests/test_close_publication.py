"""The publication (plan 2026-10-21-daily-close-close-job.md section
15.11): PB1 to PB9.

Written before ``close_chain.enqueue_due_close_work``, ``close_p4`` and
``close_chain.fail_publication``/``publication_record`` exist, and fails
first for that reason: every test here either drives the real night chain
through ``tests.helpers.install_round_state_standin``/``install_p4_standins``
(a ``ModuleNotFoundError`` today, since ``wxverify.worker.close_p4`` does
not exist) or imports ``publication_record`` from ``close_chain`` inline (an
``ImportError`` today, since ``close_chain.py`` at this commit holds only
``record_collection_try``).

PB1-PB3 and PB7, PB9 drive the real night chain end to end (the pattern of
``tests/test_close_chain.py``/``tests/test_close_attempts.py``): the
publication's own state only exists once stage 6 has genuinely opened it.
PB4-PB6 instead build their precondition directly against
``close_publications``/``site_days``/``jobs`` and then insert, claim and run
one stray stage-7 job (plan 15.1's own pattern, and PB6's own plan-text
description, "a stage-7 job for D, attempt 1, inserted and run"),
since the precondition each needs (two open records, an already-published
one, an already-failed one) is state stage 6/7's own earlier run would
produce, not something this test's own code under test builds.

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
import json
import sqlite3
from datetime import UTC, datetime, timedelta

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
from wxverify.db.queue import claim_next_job, enqueue_if_absent
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.site_days import day_record
from wxverify.worker.processor import run_claimed_job


@pytest.fixture(autouse=True)
def _clear_close_chain_log_map() -> None:
    """This file's own autouse wiring of ``clear_close_chain_log_map``
    (15.1's "The log map.") -- autouse HERE, inside this one P3 test
    file, never across the whole suite."""
    clear_close_chain_log_map()


R = STANDARD_NIGHT_R
BOUND = STANDARD_NIGHT_ROUND_BOUND
D_PLUS_1 = D + timedelta(days=1)
D_PLUS_2 = D + timedelta(days=2)


def _bound_for(round_date) -> datetime:  # type: ignore[no-untyped-def]
    """The standard night's own round-bound shape (00:40Z), generalized to
    any round date (the pattern of ``tests/test_close_attempts.py``'s
    ``_bound_for``, copied locally per this file's own-helpers rule)."""
    return datetime(
        round_date.year, round_date.month, round_date.day, 0, 40, tzinfo=UTC
    )


async def _state(db, site_id, local_date=D):  # type: ignore[no-untyped-def]
    return await db.read(lambda c: day_record(c, site_id, local_date))


async def _publication_row(db, site_id, cutoff):  # type: ignore[no-untyped-def]
    return await db.read(
        lambda c: c.execute(
            "SELECT attempt, state, retry_at, publication_id, last_failure"
            " FROM close_publications WHERE site_id = ? AND cutoff = ?",
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


async def _publication_count(db, site_id, cutoff):  # type: ignore[no-untyped-def]
    row = await db.read(
        lambda c: c.execute(
            "SELECT COUNT(*) FROM close_publications WHERE site_id = ? AND cutoff = ?",
            (site_id, cutoff.isoformat()),
        ).fetchone()
    )
    return int(row[0])


async def _job_row(db, job_id):  # type: ignore[no-untyped-def]
    return await db.read(
        lambda c: c.execute(
            "SELECT status, result, payload FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
    )


def _seed_closed_site_day(
    conn: sqlite3.Connection, site_id: int, local_date, *, timezone: str = "UTC"
) -> None:
    """Make ``site_days``'s row of ``local_date`` (PB4's second cutoff)
    ``closed`` directly, satisfying its own CHECKs (``attempts=1``,
    ``done_at`` set, ``retry_at`` NULL). A precondition from P1's own table,
    not from the close chain under test: PB4 exercises stage 7's own "a
    newer open record exists" check, which never reads ``site_days`` at
    all, so this only needs to satisfy ``close_publications``'s own foreign
    key to ``site_days(site_id, local_date)``.

    ``standard_night``'s own ``ensure_site_days`` call (P1 4.6, "through
    its next local date") already creates D + 1's row, ``open`` -- an
    INSERT here collides with it (UNIQUE site_id, local_date), so this
    UPDATEs the existing row to ``closed`` instead of inserting a new one.
    """
    stamp = isoformat_utc(
        datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)
    )
    updated = conn.execute(
        "UPDATE site_days SET state = 'closed', attempts = 1, done_at = ?,"
        " state_changed_at = ?, timezone = ? WHERE site_id = ? AND local_date = ?",
        (stamp, stamp, timezone, site_id, local_date.isoformat()),
    ).rowcount
    assert updated == 1, (
        f"expected an existing site_days row for {local_date.isoformat()}"
        " (standard_night's own ensure_site_days fills through D + 1)"
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
    finished_at: datetime | None = None,
    publication_id: str | None = None,
) -> None:
    """Insert a ``close_publications`` row directly (the shape of
    ``tests/test_close_restart.py``'s own ``_seed_publication``, copied
    locally per this file's own-helpers rule), for a precondition PB4/PB6
    need that only an earlier, already-run stage 6/7 would otherwise build.
    """
    conn.execute(
        "INSERT INTO close_publications (site_id, cutoff, attempt, state,"
        " opened_at, started_at, finished_at, publication_id)"
        " VALUES (?,?,?,?,?,?,?,?)",
        (
            site_id,
            cutoff.isoformat(),
            attempt,
            state,
            isoformat_utc(opened_at),
            None if started_at is None else isoformat_utc(started_at),
            None if finished_at is None else isoformat_utc(finished_at),
            publication_id,
        ),
    )


def _seed_publish_job(
    conn: sqlite3.Connection,
    site_id: int,
    stage: int,
    cutoff,
    attempt: int,
    *,
    created_at: datetime,
) -> int:
    """Enqueue a ``day_publish`` job keyed to ``stage``, stamped
    ``created_at`` (the pattern of ``tests/test_close_restart.py``'s
    ``_seed_publish_job``, copied locally)."""
    result = enqueue_if_absent(
        conn,
        "day_publish",
        site_id,
        f"publish:{site_id}:{stage}",
        {"cutoff": cutoff.isoformat(), "attempt": attempt},
        max_retries=0,
    )
    assert result.job_id is not None
    stamp = isoformat_utc(created_at)
    conn.execute(
        "UPDATE jobs SET created_at = ?, updated_at = ? WHERE id = ?",
        (stamp, stamp, result.job_id),
    )
    return result.job_id


# ---------------------------------------------------------------------------
# PB1
# ---------------------------------------------------------------------------


def test_pb1_a_missed_day_opens_no_publication(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PB1: D closed; D + 1 missed. One publication for cutoff D; none for
    D + 1 (8.6).

    Closes D for real through ``run_due_jobs`` (so D's own publication
    record opens and publishes normally), then pushes
    ``worker_started_at`` past D + 1's own first-attempt due instant (D +
    2's round bound) -- the real "never_attempted" path of 5.2/8.5, the
    same mechanism ``tests/test_close_attempts.py``'s AT4/AT7 use -- so D +
    1 is moved to ``missed`` by ``enqueue_due_close_work``'s own
    missed-day step, never by this test calling P1's ``mark_missed``
    directly. Only that real path can be checked against M64: a
    hand-seeded missed day would never reach the code 8.6 names.

    M64 -> at the publication-count assertion below: correct = (1, 0) (one
    ``close_publications`` row, for cutoff D only), mutant M64 (a missed
    day opens a publication record) = (1, 1), a stray row for D + 1 too.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        round_state.set_answer(
            site_id, D_PLUS_2, settled=False, bound_utc=_bound_for(D_PLUS_2)
        )

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        record = await _state(db, site_id)
        assert record.state == "closed", "D must close first (8.1)"

        # Push the add-on's own start time past D+1's due instant (D+2's
        # round bound) without ever letting its attempt run -- 5.2's
        # "never_attempted" rule, exercised for real.
        late_start = _bound_for(D_PLUS_2) + timedelta(minutes=5)
        await db.write(
            lambda c: set_runtime_state(
                c, "worker_started_at", isoformat_utc(late_start)
            )
        )
        await tick(db, clock, late_start)

        d_plus_1_record = await _state(db, site_id, D_PLUS_1)
        assert (d_plus_1_record.state, d_plus_1_record.missed_reason) == (
            "missed",
            "never_attempted",
        ), "D + 1's first attempt must fall due while the add-on was not running"

        pub_count_d = await _publication_count(db, site_id, D)
        pub_count_d1 = await _publication_count(db, site_id, D_PLUS_1)
        assert (pub_count_d, pub_count_d1) == (1, 0), (
            "M64 -> at this assertion: correct = (1, 0) (a missed day starts"
            " no publication), mutant M64 (a missed day opens a publication"
            " record) = (1, 1)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB2
# ---------------------------------------------------------------------------


def test_pb2_a_failed_publish_retries_once_an_hour_later(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PB2: ``publish`` raises once. Record ``retry_due``, ``retry_at`` +1
    h; the previous publication untouched; at the retry ``published``
    (9.4).

    Closes D for real, stage 7's own body claims and runs through
    ``run_due_jobs``, so the real ``_fail_job`` terminal branch for
    ``day_publish`` calls the real ``fail_publication`` on a genuine
    ``RuntimeError`` from P4's stand-in.

    M65 -> at the retry_at assertion below: correct = the failure instant
    plus exactly one hour, mutant M65 (the publication's retry 30 minutes
    after a failure) = the failure instant plus 30 minutes.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        standins = install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        standins["publish"].raises = RuntimeError("synthetic publish failure for PB2")

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        failure_at = clock.now()

        from wxverify.core.timeutil import parse_utc

        publication = await _publication_row(db, site_id, D)
        expected_retry_at = failure_at + timedelta(hours=1)
        retry_at_str = publication["retry_at"]
        assert retry_at_str is not None
        retry_at = parse_utc(retry_at_str)
        assert (publication["state"], retry_at, publication["publication_id"]) == (
            "retry_due",
            expected_retry_at,
            None,
        ), (
            f"M65 -> at this assertion: correct = ('retry_due',"
            f" {isoformat_utc(expected_retry_at)!r}, None), mutant M65 (the"
            " publication's retry 30 minutes after a failure) gives a"
            " retry_at 30 minutes earlier than this"
        )

        count = await _publication_count(db, site_id, D)
        assert count == 1, (
            "exactly one close_publications row for D, untouched elsewhere"
        )

        standins["publish"].raises = None
        await tick(db, clock, retry_at)
        await run_due_jobs(db, clock)

        expected_pub_id = await _real_publication_id(db, site_id, D)
        retried = await _publication_row(db, site_id, D)
        assert (retried["attempt"], retried["state"], retried["publication_id"]) == (
            2,
            "published",
            expected_pub_id,
        ), "the retry must close out as published, attempt 2, with P4's identifier"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB3
# ---------------------------------------------------------------------------


def test_pb3_a_failed_publication_does_not_block_the_next_cutoff(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PB3: ``publish`` raises twice. Record ``failed``; the next closed
    day's publication has cutoff D + 1 and is built (9.4).

    Closes D for real (``publish`` raising on both the first attempt and
    its one retry), then closes D + 1 for real too, with ``publish``
    finally succeeding -- the only way to show D + 1's own publication is
    not blocked is to let the real chain build and publish it.

    M66 -> at the second-publication assertion below: correct = D + 1's
    record ``published`` with its own identifier (the new cutoff is built
    and publishes normally), mutant M66 (a failed publication record
    blocks the next cutoff) = D + 1's record never opens, or never leaves
    ``pending``/``building``.
    """

    async def _run() -> None:
        from tests.helpers import _seed_five_minute_day

        db, _writer, site_id, stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        standins = install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        round_state.set_answer(
            site_id, D_PLUS_2, settled=True, bound_utc=_bound_for(D_PLUS_2)
        )
        standins["publish"].raises = RuntimeError("synthetic publish failure for PB3")

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        first = await _publication_row(db, site_id, D)
        assert (first["attempt"], first["state"]) == (1, "retry_due"), (
            "attempt 1's failure must leave D's publication retry_due"
        )
        retry_at_str = first["retry_at"]
        assert retry_at_str is not None
        from wxverify.core.timeutil import parse_utc

        await tick(db, clock, parse_utc(retry_at_str))
        await run_due_jobs(db, clock)

        failed = await _publication_row(db, site_id, D)
        assert (failed["attempt"], failed["state"]) == (2, "failed"), (
            "the second publish failure must leave D's publication failed"
            " for good -- no further retry"
        )

        # D + 1 must close and publish normally, unblocked by D's failure.
        await db.write(
            lambda c: _seed_five_minute_day(c, stations["ISTATION01"], D_PLUS_1)
        )
        await db.write(
            lambda c: _seed_five_minute_day(c, stations["ISTATION02"], D_PLUS_1)
        )
        standins["publish"].raises = None
        await tick(db, clock, _bound_for(D_PLUS_2))
        await run_due_jobs(db, clock)

        expected_pub_id = await _real_publication_id(db, site_id, D_PLUS_1)
        second = await _publication_row(db, site_id, D_PLUS_1)
        assert (second["attempt"], second["state"], second["publication_id"]) == (
            1,
            "published",
            expected_pub_id,
        ), (
            f"M66 -> at this assertion: correct = (1, 'published', {expected_pub_id!r})"
            " (D + 1's own publication is built and published, unblocked by"
            " D's failed one), mutant M66 (a failed publication record"
            " blocks the next cutoff) = D + 1 never opens or never"
            " publishes"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB4
# ---------------------------------------------------------------------------


def test_pb4_an_older_open_record_is_superseded_by_a_newer_one(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PB4: two closed days' records open before stage 7 runs. The older
    ends ``superseded`` with no call to P4; one call, for the newer (8.6).

    Builds the precondition directly: two ``pending`` ``close_publications``
    rows (D, then D + 1), and D's stage-7 job -- not D + 1's own yet. Both
    jobs share the one ``day_publish`` job key ``publish:{site_id}:7``
    (``_enqueue_publish_stage``, ``close_chain.py:607-618`` -- it is not
    keyed by cutoff), and the ``jobs`` table's partial unique index only
    forbids a second PENDING/RUNNING row of that key (`migrations.py:771-
    772`), so D + 1's own job cannot be seeded until D's has run to
    completion -- exactly the real sequence (plan 8.6: the repair step,
    8.5 item 4, enqueues the newer cutoff's stage 7 only once the older
    job is no longer pending). D's job is claimed and run first, below;
    D + 1's job is seeded only afterward, once D's row is no longer
    pending/running. Only the state of ``close_publications`` (not
    ``site_days``) decides stage 7's own body, so D + 1's ``site_days`` row
    is seeded directly as already ``closed`` (P1's own table, not the code
    under test), rather than driving two full nights through a FIFO race
    this test has no seam to control deterministically.

    M67 -> at the two state assertions below: correct = D 'superseded' and
    D + 1 'published', with ``publish`` called exactly once (for D + 1),
    mutant M67 (stage 7 does not check for a newer open record) = D also
    'published', with ``publish`` called twice.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        standins = install_p4_standins(monkeypatch)

        opened_d = NOW
        opened_d1 = NOW + timedelta(minutes=1)

        await db.write(lambda c: _seed_closed_site_day(c, site_id, D_PLUS_1))
        await db.write(
            lambda c: _seed_publication(
                c, site_id, D, state="pending", opened_at=opened_d
            )
        )
        await db.write(
            lambda c: _seed_publication(
                c, site_id, D_PLUS_1, state="pending", opened_at=opened_d1
            )
        )
        job_d = await db.write(
            lambda c: _seed_publish_job(c, site_id, 7, D, 1, created_at=opened_d)
        )

        first = await db.write(claim_next_job)
        assert first is not None and first.id == job_d, (
            "D's stage-7 job, the only one seeded so far, must be the one claimed"
        )
        await run_claimed_job(db, first, lane="main")

        older = await _publication_row(db, site_id, D)
        assert older["state"] == "superseded", (
            "M67 -> at this assertion: correct = 'superseded' (D's stage 7"
            " sees D + 1's record already open and supersedes itself"
            " without calling publish), mutant M67 (stage 7 does not check"
            " for a newer open record) = 'published' instead"
        )
        assert standins["publish"].calls == [], (
            "publish must not be called for the superseded cutoff D"
        )

        # D's job is no longer pending/running, so the shared job key
        # (publish:{site_id}:7) is free again (migrations.py:771-772) and
        # D + 1's own stage-7 job can now be seeded, as the repair step
        # (plan 8.5 item 4) would enqueue it on the next tick.
        job_d1 = await db.write(
            lambda c: _seed_publish_job(
                c, site_id, 7, D_PLUS_1, 1, created_at=opened_d1
            )
        )

        second = await db.write(claim_next_job)
        assert second is not None and second.id == job_d1
        await run_claimed_job(db, second, lane="main")

        expected_pub_id = await _real_publication_id(db, site_id, D_PLUS_1)
        newer = await _publication_row(db, site_id, D_PLUS_1)
        assert (newer["state"], newer["publication_id"]) == (
            "published",
            expected_pub_id,
        ), (
            "the newer cutoff's own stage 7 must publish normally, with"
            " its own identifier"
        )
        assert len(standins["publish"].calls) == 1, (
            "M67 -> at this assertion: correct = 1 (publish called exactly"
            " once, for the newer cutoff only), mutant M67 (stage 7 does"
            " not check for a newer open record) = 2, since D's own stage 7"
            " would have called publish too instead of superseding itself"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB4b
# ---------------------------------------------------------------------------


def test_pb4b_a_retrying_older_record_is_superseded_by_a_published_newer_one(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PB4b (F2): ``_newer_publication`` (close_chain.py:1212) must
    also count a later cutoff's record that is already ``published``, not
    only ``pending``/``building`` -- the old code (``e70b81f``) checks only
    ``state IN ('pending', 'building')``, so an older cutoff's own retry
    would publish a second time over an already-published later cutoff.

    Cutoff C1 (D) starts ``retry_due``; C2 (D + 1) is already
    ``published``. PB7 already proves the ``retry_due`` -> ``pending``
    attempt-2 transition (plan 8.5 step 5), so this test builds that
    transition directly (PB4's own build-the-precondition-directly rule,
    since the code under test here is stage 7's own
    ``_newer_publication`` check, not the retry repair step), then
    seeds and runs C1's own retry job (attempt 2).

    F2 -> at the two assertions below: correct = 'superseded' and publish
    not called (C1's retry sees C2's already-published record as newer),
    the old code (``e70b81f``) (only pending/building count as newer) = C1
    published after C2.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        standins = install_p4_standins(monkeypatch)

        opened_c1 = NOW
        opened_c2 = NOW + timedelta(minutes=1)
        failed_at = NOW
        retry_at = failed_at + timedelta(hours=1)

        await db.write(lambda c: _seed_closed_site_day(c, site_id, D_PLUS_1))

        def _seed_c1_retry_due(c: sqlite3.Connection) -> None:
            c.execute(
                "INSERT INTO close_publications (site_id, cutoff, attempt,"
                " state, opened_at, retry_at)"
                " VALUES (?, ?, 1, 'retry_due', ?, ?)",
                (
                    site_id,
                    D.isoformat(),
                    isoformat_utc(opened_c1),
                    isoformat_utc(retry_at),
                ),
            )

        await db.write(_seed_c1_retry_due)
        await db.write(
            lambda c: _seed_publication(
                c,
                site_id,
                D_PLUS_1,
                state="published",
                opened_at=opened_c2,
                started_at=opened_c2,
                finished_at=opened_c2,
                publication_id="pub-D+1",
            )
        )

        # PB7 already proves the retry_due -> pending, attempt-2 transition
        # (8.5 step 5); build it directly here (the file's own
        # build-the-precondition-directly rule).
        await db.write(
            lambda c: c.execute(
                "UPDATE close_publications SET state = 'pending', attempt = 2,"
                " retry_at = NULL WHERE site_id = ? AND cutoff = ?",
                (site_id, D.isoformat()),
            )
        )
        job_c1 = await db.write(
            lambda c: _seed_publish_job(c, site_id, 7, D, 2, created_at=retry_at)
        )

        job = await db.write(claim_next_job)
        assert job is not None and job.id == job_c1
        await run_claimed_job(db, job, lane="main")

        c1_row = await _publication_row(db, site_id, D)
        message = (
            "F2 -> at this assertion: correct = 'superseded' and publish"
            " not called (C1's retry sees C2's already-published record as"
            " newer), the old code (e70b81f) (only pending/building count"
            " as newer) = C1 published after C2"
        )
        assert c1_row is not None and c1_row["state"] == "superseded", message
        assert standins["publish"].calls == [], message

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB6
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("seeded_state", ["published", "pending", "none"])
def test_pb6_a_stale_job_exception_never_touches_a_non_building_record(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    seeded_state: str,
) -> None:
    """PB6: a stage-7 job for D, attempt 1, inserted and run, whose first
    read of the record (``publication_record`` as ``close_chain`` calls
    it, 8.2) raises once. The job row ``failed``; the record is
    ``fail_publication``'s own "any other record" case, so it is never
    touched: one WARNING line and no stage-log row (9.4).

    ``seeded_state`` ``published`` is the original PB6 case: every write
    ``fail_publication``'s own ``retry_due`` branch could make to a
    ``published`` record breaks ``close_publications``'s own
    ``(state = 'published') = (publication_id IS NOT NULL)`` CHECK (the
    mutated write would leave ``publication_id`` set while moving the
    state away from ``published``), so SQLite's own CHECK kills mutant
    M77 there, not this test's own assertion.

    ``seeded_state`` ``pending`` is M77's real kill: a ``pending`` record
    has no ``publication_id`` set, so the same mutated write satisfies
    every CHECK (``retry_due`` with ``retry_at`` set, ``attempt`` 1,
    ``finished_at`` NULL) and commits silently, moving a record
    ``fail_publication`` must never have touched.

    ``seeded_state`` ``none`` is the no-record case, paired with the other
    two (a positive precondition each): no ``close_publications`` row at
    all for D, so M77's own UPDATE (unguarded) matches zero rows and the
    record stays absent either way -- the record-state check cannot tell
    this case apart from correct code. The no-stage-log-row check below
    does: M77 removes the whole guard, so it falls through to an
    unconditional ``_write_stage_row`` call regardless of whether any
    record existed, where correct code's early return (``record is
    None``) writes no row at all.

    Imports ``publication_record`` from ``close_chain`` inline (an
    ``ImportError`` today, the name does not exist until piece 3/4), wraps
    it to raise only on its first call -- stage 7's own predicate read
    (8.2 step 1) -- and call through to the real function afterward, so
    the SECOND read, inside the real ``fail_publication``, finds the
    record genuinely as seeded (never ``building``) and declines to touch
    it, logging the WARNING 9.4 names for "any other record".

    M77 -> at the ``pending`` state assertion below: correct = still
    'pending' (``fail_publication`` acts only on a 'building' record of
    the job's own attempt; a 'pending' record is "any other record",
    changed only by the WARNING path), mutant M77 (``fail_publication``
    acts on a record that is not 'building' in the job's attempt) =
    'retry_due', a transition the CHECK does not forbid for a record with
    no ``publication_id``.

    M77 -> at the no-stage-log-row assertion below, for ``seeded_state``
    ``none``: correct = the stage-7 row count is unchanged (the guard's
    early return writes nothing), mutant M77 (the guard removed) = one
    new stage-7 row is written for a cutoff with no publication record at
    all.
    """

    async def _run() -> None:
        from wxverify.worker.close_chain import (
            publication_record as _real_publication_record,
        )

        db, _writer, site_id, _stations = standard_night(tmp_path)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)

        if seeded_state == "published":
            await db.write(
                lambda c: _seed_publication(
                    c,
                    site_id,
                    D,
                    state="published",
                    opened_at=NOW,
                    started_at=NOW,
                    finished_at=NOW,
                    publication_id="pub-D",
                )
            )
        elif seeded_state == "pending":
            await db.write(
                lambda c: _seed_publication(
                    c, site_id, D, state="pending", opened_at=NOW
                )
            )
        # "none": no close_publications row is seeded for D at all.

        job_id = await db.write(
            lambda c: _seed_publish_job(c, site_id, 7, D, 1, created_at=NOW)
        )

        calls = {"n": 0}

        def _flaky(conn: sqlite3.Connection, *, site_id: int, cutoff):  # type: ignore[no-untyped-def]
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("synthetic publication_record failure for PB6")
            return _real_publication_record(conn, site_id=site_id, cutoff=cutoff)

        monkeypatch.setattr(
            "wxverify.worker.close_chain.publication_record", _flaky, raising=False
        )

        caplog.set_level("WARNING", logger="wxverify.worker.close_chain")
        stage_log_before = (
            await db.read(
                lambda c: c.execute(
                    "SELECT COUNT(*) FROM close_stage_log WHERE site_id = ?"
                    " AND local_date = ? AND stage = 7",
                    (site_id, D.isoformat()),
                ).fetchone()
            )
        )[0]

        job = await db.write(claim_next_job)
        assert job is not None and job.id == job_id
        await run_claimed_job(db, job, lane="main")

        job_row = await _job_row(db, job_id)
        assert job_row["status"] == "failed", "the stale job's own row must fail"

        record = await _publication_row(db, site_id, D)
        if seeded_state == "published":
            assert record is not None and (
                record["state"],
                record["publication_id"],
            ) == (
                "published",
                "pub-D",
            ), (
                "M77 -> at this assertion: correct = ('published', 'pub-D'),"
                " mutant M77 (fail_publication acts on a record that is not"
                " 'building' in the job's attempt) = ('retry_due', 'pub-D'),"
                " a write the close_publications CHECK refuses here"
            )
        elif seeded_state == "pending":
            assert record is not None and record["state"] == "pending", (
                "M77 -> at this assertion: correct = 'pending' (fail_publication"
                " acts only on a 'building' record of the job's own attempt;"
                " 'pending' is 'any other record', changed only by the"
                " WARNING path), mutant M77 (fail_publication acts on a"
                " record that is not 'building' in the job's attempt) ="
                " 'retry_due', which the CHECK does not forbid here"
            )
        else:
            assert record is None, (
                "a stale job with no publication record must not create one"
            )

        warnings = [rec for rec in caplog.records if rec.levelname == "WARNING"]
        assert len(warnings) == 1, "exactly one WARNING line must be written (9.4)"

        stage_log_after = (
            await db.read(
                lambda c: c.execute(
                    "SELECT COUNT(*) FROM close_stage_log WHERE site_id = ?"
                    " AND local_date = ? AND stage = 7",
                    (site_id, D.isoformat()),
                ).fetchone()
            )
        )[0]
        assert stage_log_after == stage_log_before, (
            "M77 -> at this assertion (seeded_state="
            f"{seeded_state!r}): correct = the stage-7 row count unchanged"
            " (the WARNING path's early return writes nothing), mutant M77"
            " (the guard removed) = one new stage-7 row written regardless"
            " of the record's own state"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB7
# ---------------------------------------------------------------------------


def test_pb7_a_retry_due_record_is_not_repaired_early(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PB7: PB2's record ``retry_due``, its stage-7 job deleted; ticks
    before ``retry_at``, then at ``retry_at``. Before: nothing enqueued,
    the record ``retry_due``; at ``retry_at``: the record ``pending`` with
    ``attempt`` 2 and no ``retry_at``, and exactly one stage-7 job with
    payload ``{"cutoff": D, "attempt": 2}``; it publishes ``pub-D`` (8.5,
    9.4).

    Closes D for real (``publish`` raising once), deletes the now-``failed``
    stage-7 job row so the "before" tick's own absence of any job is
    unambiguous, then ticks once before ``retry_at`` and once at it.

    M79 -> at the before-retry_at assertion below: correct = no
    ``day_publish`` job exists yet (the repair of 8.2 never touches a
    ``retry_due`` record -- only the publication retry of 8.5 step 5 does,
    and only once ``retry_at`` has passed), mutant M79 (the repair treats a
    ``retry_due`` record as open) = a stage-7 job reappears a tick early.
    """

    async def _run() -> None:
        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        standins = install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        standins["publish"].raises = RuntimeError("synthetic publish failure for PB7")

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        before = await _publication_row(db, site_id, D)
        assert before["state"] == "retry_due"
        retry_at_str = before["retry_at"]
        assert retry_at_str is not None
        from wxverify.core.timeutil import parse_utc

        retry_at = parse_utc(retry_at_str)

        await db.write(
            lambda c: c.execute(
                "DELETE FROM jobs WHERE type = 'day_publish' AND site_id = ?"
                " AND job_key = ?",
                (site_id, f"publish:{site_id}:7"),
            )
        )

        await tick(db, clock, retry_at - timedelta(minutes=5))
        jobs_before = await db.read(
            lambda c: c.execute(
                "SELECT id FROM jobs WHERE type = 'day_publish' AND site_id = ?",
                (site_id,),
            ).fetchall()
        )
        assert jobs_before == [], (
            "M79 -> at this assertion: correct = [] (no day_publish job"
            " before retry_at; a retry_due record is never repaired),"
            " mutant M79 (the repair treats a retry_due record as open) ="
            " a stage-7 job reappears early"
        )
        still_retry_due = await _publication_row(db, site_id, D)
        assert still_retry_due["state"] == "retry_due", "the record must not move early"

        standins["publish"].raises = None
        await tick(db, clock, retry_at)

        retried = await _publication_row(db, site_id, D)
        assert (retried["attempt"], retried["state"], retried["retry_at"]) == (
            2,
            "pending",
            None,
        ), (
            "at retry_at, in one write: attempt 2, pending, retry_at"
            " cleared (8.5 step 5)"
        )

        jobs_at_retry = await db.read(
            lambda c: c.execute(
                "SELECT payload FROM jobs WHERE type = 'day_publish' AND site_id = ?",
                (site_id,),
            ).fetchall()
        )
        assert len(jobs_at_retry) == 1
        assert json.loads(jobs_at_retry[0][0]) == {
            "cutoff": D.isoformat(),
            "attempt": 2,
        }, "the retry's own stage-7 job must carry the new attempt"

        await run_due_jobs(db, clock)
        expected_pub_id = await _real_publication_id(db, site_id, D)
        published = await _publication_row(db, site_id, D)
        assert (
            published["attempt"],
            published["state"],
            published["publication_id"],
        ) == (
            2,
            "published",
            expected_pub_id,
        ), "the retry must publish, with P4's identifier"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PB9
# ---------------------------------------------------------------------------


def test_pb9_both_failed_stage7_jobs_report_no_failed_scope(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PB9: PB3 up to the record ``failed``, then the failed-scope count
    (15.1) read at the second stage-7 failure + 49 h, with no tick and no
    job run in between. 0 failed scopes, though both stage-7 job rows are
    ``failed`` and older than 48 hours; the record still ``failed`` (9.6).

    ``FAILED_SCOPES_SQL`` excludes the whole ``day_publish`` job type (the
    publication record itself reports a failed stage 7, `monitor.py`'s
    ``j.type != 'day_publish'`` clause), so this stays 0 whatever the
    attempt or stage: at the count assertion below, correct = 0; a mutant
    that drops that type exclusion gives a nonzero count, since a failed
    stage-7 job would then count as a scope once old enough.
    """

    async def _run() -> None:
        from wxverify.monitor import FAILED_JOB_AGE_HOURS

        db, _writer, site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)
        round_state = install_round_state_standin(monkeypatch)
        standins = install_p4_standins(monkeypatch)
        round_state.set_answer(site_id, R, settled=True, bound_utc=BOUND)
        standins["publish"].raises = RuntimeError("synthetic publish failure for PB9")

        clock.set(BOUND)
        await run_due_jobs(db, clock)
        first = await _publication_row(db, site_id, D)
        assert first["state"] == "retry_due"
        retry_at_str = first["retry_at"]
        assert retry_at_str is not None
        from wxverify.core.timeutil import parse_utc

        await tick(db, clock, parse_utc(retry_at_str))
        await run_due_jobs(db, clock)
        second_failure_at = clock.now()

        failed = await _publication_row(db, site_id, D)
        assert failed["state"] == "failed", "both attempts must fail for good"

        failed_jobs = await db.read(
            lambda c: c.execute(
                "SELECT COUNT(*) FROM jobs WHERE type = 'day_publish'"
                " AND job_key = ? AND status = 'failed'",
                (f"publish:{site_id}:7",),
            ).fetchone()
        )
        assert failed_jobs[0] == 2, (
            "both stage-7 job rows (attempts 1 and 2) must be failed"
        )

        from tests.helpers import failed_scope_count

        later = second_failure_at + timedelta(hours=FAILED_JOB_AGE_HOURS + 1)
        count = await db.read(lambda c: failed_scope_count(c, now_utc=later))
        assert count == 0, (
            "correct = 0 (FAILED_SCOPES_SQL excludes the whole day_publish"
            " job type); a mutant that drops that type exclusion gives a"
            " nonzero count here"
        )

    asyncio.run(_run())
