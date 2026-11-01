"""Landing dormant: DO3 to DO5 (plan 2026-10-21-daily-close-close-job.md
section 15.2). DO4 needs ``enqueue_due_close_work`` and
``settle_interrupted_closes``, which land with later pieces of this plan,
and is written before they exist.

DO3 claims and runs a stray ``day_close``/
``day_publish`` job through the real queue and ``dispatch``
(``worker/processor.py:601``); ``dispatch`` has no route for either kind
yet, so each job dies on ``RuntimeError(f"unknown job type {job.type}")``
(``:775``) instead of ending ``completed`` with one WARNING -- the right
present-day failure, not a fixture bug. DO4 imports
``enqueue_due_close_work``/``settle_interrupted_closes`` from
``wxverify.worker.close_chain`` inline, which raises ``ImportError`` at
once (that module holds only ``record_collection_try`` today). DO5 is
unchanged from the prior batch.

Section 16.2 of the plan names the mutant each test ID kills; that mapping
is given inline on the assertion, in the file's own style.

Real SQLite only (``tests.helpers.asof_conn``/``day_close_site``, real
``run_migrations``; DO3 additionally uses a real ``Database``/
``FencedWriter`` on a ``tmp_path`` file, since it must claim and run a job
through the real queue). Station ids are synthetic (``ISTATION01``,
``ISTATION02``), zone ``UTC``, and every date is the fixed synthetic
``tests.helpers.D``. No live id, coordinate, site name or real zone name
appears.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from tests.helpers import D, asof_conn, day_close_site
from wxverify import config
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro
from wxverify.db.connection import close_db, init_db
from wxverify.db.queue import claim_next_job
from wxverify.verification.coverage import local_day_bounds
from wxverify.worker.processor import run_claimed_job

_CLOSE_TABLES: tuple[str, ...] = (
    "close_attempts",
    "close_station_days",
    "close_publications",
    "close_stage_log",
    "close_try_log",
)


def _close_table_counts(conn) -> dict[str, int]:  # type: ignore[no-untyped-def]
    """Row counts of the five new tables (plan 12.3), for a before/after
    "nothing written" oracle shared by DO3 and DO4."""
    return {
        table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
        for table in _CLOSE_TABLES
    }


# ---------------------------------------------------------------------------
# DO3
# ---------------------------------------------------------------------------


def test_do3_a_stray_close_job_is_cancelled_not_run(
    tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    """DO3: close off; a stray ``day_close`` and ``day_publish`` job
    inserted and run. Each ends ``JobCancelled`` with one WARNING; every
    table unchanged apart from the job rows (13.1).

    M4 -> at the per-job outcome assertion below: correct = each job row
    ``completed`` with ``result`` NULL (the ``JobCancelled`` contract) and
    one WARNING logged, mutant M4 (a stage body without its predicate
    check) = the body does real work instead of refusing. At ``a4a919c``
    neither kind is routed by ``dispatch`` at all (``worker/processor.py``
    has no ``day_close``/``day_publish`` branch), so each job actually
    dies on ``RuntimeError('unknown job type ...')`` and ends ``failed``
    with no WARNING -- the right present-day failure, not a fixture bug.
    """

    async def _run() -> None:
        close_db()
        tmp_path.mkdir(parents=True, exist_ok=True)
        db_path = tmp_path / "wxverify.db"
        config.db_path = str(db_path)
        db = init_db(str(db_path))
        conn = db._conn  # noqa: SLF001

        site_id = day_close_site(conn, close_on=False, with_records=False)
        conn.execute(
            "INSERT INTO jobs (type, site_id, job_key, payload, status,"
            " max_retries) VALUES ('day_close', ?, ?, ?, 'pending', 0)",
            (site_id, f"close:{site_id}:{D.isoformat()}:2", '{"attempt": 1}'),
        )
        conn.execute(
            "INSERT INTO jobs (type, site_id, job_key, payload, status,"
            " max_retries) VALUES ('day_publish', ?, ?, ?, 'pending', 0)",
            (
                site_id,
                f"publish:{site_id}:7",
                '{"cutoff": "' + D.isoformat() + '", "attempt": 1}',
            ),
        )
        conn.commit()

        before = _close_table_counts(conn)
        site_days_before = conn.execute(
            "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()[0]

        caplog.set_level("WARNING", logger="wxverify.worker.close_chain")
        outcomes: list[tuple[str, str | None]] = []
        for _ in range(2):
            job = await db.write(claim_next_job)
            assert job is not None, "both stray jobs must be claimable"
            await run_claimed_job(db, job, lane="main")

            def _row(c, job_id: int = job.id):  # type: ignore[no-untyped-def]
                return c.execute(
                    "SELECT status, result FROM jobs WHERE id = ?", (job_id,)
                ).fetchone()

            outcomes.append(tuple(await db.read(_row)))

        for outcome in outcomes:
            assert outcome == ("completed", None), (
                "M4 -> at this assertion: correct = ('completed', None) per"
                " job (the JobCancelled contract: the predicate is read and"
                " refuses, writing nothing), mutant M4 (a stage body without"
                " its predicate check) = the job does real work instead;"
                f" present-day actual = {outcome!r} (an uncaught"
                " RuntimeError, since dispatch has no route for either kind"
                " yet)"
            )
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 2, (
            "one WARNING line per stray job (13.1): a stale/off-cutover job"
            " must log exactly one, not zero and not an unhandled exception"
        )

        after = _close_table_counts(conn)
        site_days_after = conn.execute(
            "SELECT COUNT(*) FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchone()[0]
        assert after == before and site_days_after == site_days_before, (
            "every table besides jobs must be unchanged by the stray"
            " close-kind jobs (13.1)"
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# DO4
# ---------------------------------------------------------------------------


def test_do4_direct_calls_write_nothing_with_the_close_off() -> None:
    """DO4: close off; ``enqueue_due_close_work`` and
    ``settle_interrupted_closes`` called directly, with a P1 record in
    ``closing`` seeded. Nothing written (13.1).

    M3 -> at the final snapshot assertion below: correct = no change at
    all (both functions read the close-off predicate first and return),
    mutant M3 (``enqueue_due_close_work`` called with the close off --
    i.e. its own off-check removed) = the seeded ``closing`` day is
    repaired/acted on.

    M5 -> at the same assertion: correct = no change, mutant M5
    (``settle_interrupted_closes`` without its predicate check) = the
    seeded ``closing`` day is settled (moved to ``retry_due``/``missed``)
    even though the close is off.

    Imports ``enqueue_due_close_work``/``settle_interrupted_closes`` from
    ``wxverify.worker.close_chain`` inline: at ``a4a919c`` that module
    holds only ``record_collection_try``, so this raises ``ImportError``
    at once -- the right present-day failure.
    """
    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False, with_records=False)

    bounds = local_day_bounds(D, "UTC")
    now = datetime(D.year, D.month, D.day, 23, 0, tzinfo=UTC)
    conn.execute(
        "INSERT INTO site_days (site_id, local_date, state, day_start_utc,"
        " day_end_utc, timezone, attempts, created_at, state_changed_at)"
        " VALUES (?, ?, 'closing', ?, ?, 'UTC', 1, ?, ?)",
        (
            site_id,
            D.isoformat(),
            isoformat_utc(bounds.start_utc),
            isoformat_utc(bounds.end_utc),
            isoformat_utc(now),
            isoformat_utc(now),
        ),
    )
    conn.commit()

    before_site_day = dict(
        conn.execute(
            "SELECT * FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, D.isoformat()),
        ).fetchone()
    )
    before_tables = _close_table_counts(conn)

    from wxverify.worker.close_chain import (  # noqa: PLC0415
        enqueue_due_close_work,
        settle_interrupted_closes,
    )

    enqueue_due_close_work(conn, now_utc=now)
    settle_interrupted_closes(conn, now_utc=now)

    after_site_day = dict(
        conn.execute(
            "SELECT * FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, D.isoformat()),
        ).fetchone()
    )
    after_tables = _close_table_counts(conn)
    assert after_site_day == before_site_day and after_tables == before_tables, (
        "M3/M5 -> at this assertion: correct = no change at all (both"
        " functions' own close-off predicate check returns at once),"
        " mutant M3/M5 (either without its predicate check) = the seeded"
        " closing day gets acted on (repaired or settled)"
    )


def test_do4_enqueue_due_close_work_checks_its_own_predicate_directly() -> None:
    """DO4 extension: a second, more exacting drive of
    ``enqueue_due_close_work`` alone, with the close off, where the
    ``closing`` day's own ``close_attempts`` row is present so step 4's
    repair (8.2) can run to completion instead of raising before any
    write.

    The DO4 test above seeds a ``closing`` ``site_days`` row with NO
    matching ``close_attempts`` row; with M3p in place (the function's own
    ``if not daily_close_on(conn): return`` removed), the per-site loop
    still runs, but step 4's ``_repair`` then raises
    ``RuntimeError("the closing attempt has no attempt record")`` before
    any write, in EVERY build mutant included -- caught by the per-site
    savepoint and logged, leaving nothing written either way. That
    fixture cannot tell M3p's own-predicate removal from the real guard:
    both end with nothing written, for different reasons. This fixture
    gives ``_repair`` a real attempt record to act on, so the mutant's
    write becomes observable.

    M3p -> at the day_close job assertion below: correct = no day_close
    job exists (``enqueue_due_close_work``'s own close-off predicate,
    ``close_chain.py:2055-2056``, returns before the per-site loop ever
    runs), mutant M3p (that predicate removed) = a pending day_close job
    keyed ``close:{site}:{D}:4`` exists, because ``_repair`` finds the
    ``closing`` day's ``last_stage`` (3) and enqueues the next stage.

    (Separately, the plan's own M3 -- an extra call of
    ``enqueue_due_close_work`` added to ``scheduler_tick``'s close-off
    branch, ``worker/scheduler.py`` about :90 -- is equivalent as built:
    ``enqueue_due_close_work`` itself still carries its own close-off
    guard there, so the extra call is a no-op in every observable state.
    Not retested here; M3p above targets the guard itself.)
    """
    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False, with_records=False)

    bounds = local_day_bounds(D, "UTC")
    now = datetime(D.year, D.month, D.day, 23, 0, tzinfo=UTC)
    stamp = isoformat_utc_micro(now)
    conn.execute(
        "INSERT INTO site_days (site_id, local_date, state, day_start_utc,"
        " day_end_utc, timezone, attempts, created_at, state_changed_at)"
        " VALUES (?, ?, 'closing', ?, ?, 'UTC', 1, ?, ?)",
        (
            site_id,
            D.isoformat(),
            isoformat_utc(bounds.start_utc),
            isoformat_utc(bounds.end_utc),
            isoformat_utc(now),
            isoformat_utc(now),
        ),
    )
    conn.execute(
        "INSERT INTO close_attempts (site_id, local_date, attempt,"
        " started_at, last_stage, last_progress_at, outcome)"
        " VALUES (?, ?, 1, ?, 3, ?, 'running')",
        (site_id, D.isoformat(), stamp, stamp),
    )
    conn.commit()

    from wxverify.worker.close_chain import enqueue_due_close_work  # noqa: PLC0415

    enqueue_due_close_work(conn, now_utc=now)

    job = conn.execute(
        "SELECT 1 FROM jobs WHERE type = 'day_close' AND site_id = ? AND job_key = ?",
        (site_id, f"close:{site_id}:{D.isoformat()}:4"),
    ).fetchone()
    assert job is None, (
        "M3p -> at this assertion: correct = None (enqueue_due_close_work's"
        " own close-off predicate, close_chain.py:2055-2056, returns before"
        " the per-site loop runs, so step 4's repair never sees this"
        " closing day), mutant M3p (that predicate removed) = a row"
        " exists (the repair enqueues close:{site}:{D}:4 from the attempt"
        " record's last_stage of 3)"
    )


def test_do5_record_collection_try_writes_its_row_with_the_close_off() -> None:
    """DO5: with the close off, ``record_collection_try`` still inserts one
    ``close_try_log`` row carrying exactly the given values -- the writer
    does not gate on ``daily_close_on`` (11.2, 15.2): P2's evening tries
    call it unconditionally, before this release's switch is ever turned
    on.

    M6 -> at the row-count assertion below: correct = 1 (the writer inserts
    regardless of ``daily_close_on``), mutant (``record_collection_try``
    returns without writing while the close is off) = 0.
    """
    from wxverify.worker.close_chain import record_collection_try

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False)
    station_id = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? ORDER BY id LIMIT 1",
        (site_id,),
    ).fetchone()[0]
    tried_at = isoformat_utc_micro(datetime(D.year, D.month, D.day, 19, 40, tzinfo=UTC))

    record_collection_try(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=D.isoformat(),
        kind="five_minute",
        slot="19:40",
        close_attempt=None,
        tried_at=tried_at,
        outcome="stored",
        reason=None,
        detail=None,
        readings_kept=3,
        readings_refused=0,
    )

    rows = conn.execute(
        "SELECT site_id, local_date, station_id, kind, slot, close_attempt,"
        " tried_at, outcome, reason, detail, readings_kept, readings_refused"
        " FROM close_try_log"
    ).fetchall()
    assert len(rows) == 1, (
        "mutant -> at this assertion: correct = 1 (the writer inserts"
        " regardless of daily_close_on), mutant (record_collection_try"
        " returns without writing while the close is off) = 0"
    )
    row = rows[0]
    assert row["site_id"] == site_id
    assert row["local_date"] == D.isoformat()
    assert row["station_id"] == station_id
    assert row["kind"] == "five_minute"
    assert row["slot"] == "19:40"
    assert row["close_attempt"] is None
    assert row["tried_at"] == tried_at
    assert row["outcome"] == "stored"
    assert row["reason"] is None
    assert row["detail"] is None
    assert row["readings_kept"] == 3
    assert row["readings_refused"] == 0
