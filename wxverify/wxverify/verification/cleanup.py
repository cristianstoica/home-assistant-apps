"""The worksheet cleanup (daily-close plan, section 14).

A verification run's worksheet is its ``verification_evidence`` rows. Every
run of a site older than the site's published run loses its worksheet, in
small saves; the published run, any run in state ``running`` and any run
newer than the published one are never touched. Every run keeps its
``verification_runs`` row (settings snapshot included), its results,
verdicts, day context and run inputs, and its ``worksheet_kept`` flag says
whether the worksheet is still there (1) or was deleted (0).

The database is consistent after every save: a run either still has
evidence rows and ``worksheet_kept`` 1, or has none and 0, so a stop part-way
loses at most the save in flight and the next pass goes on.

One entry point, only after a publish (14.4): the daily close's stage 8
calls :func:`clean_worksheets`. The replay chain's ``cleanup`` phase went
with the nightly replay (switch-over plan R12). Every save re-reads the
published pointer.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime

from wxverify.db.connection import Database, FencedWriter
from wxverify.verification.runs import published_run_id

logger = logging.getLogger(__name__)

#: Evidence rows one save deletes at most (14.2). Set so the longest single
#: save stays under the 7.8 s slow-write watch line; measured on a Mac copy
#: before release (K33) and lowered if needed. Read at call time, so a test
#: can patch it.
CLEANUP_SLICE_ROWS = 20_000

#: Saves one :func:`clean_worksheets` call makes at most (14.2), about one
#: million rows. Read at call time, so a test can patch it.
CLEANUP_SLICES_PER_CALL = 50

# The oldest run 14.1 allows that still has its worksheet: older than the
# published run (by id, which is creation order), not `running`, and
# `worksheet_kept` 1. The published run itself and every newer run fail the
# `id < ?` bound.
_NEXT_RUN_SQL = """
    SELECT id FROM verification_runs
    WHERE site_id = ? AND id < ? AND state != 'running' AND worksheet_kept = 1
    ORDER BY id
    LIMIT 1
"""

_DELETE_SLICE_SQL = """
    DELETE FROM verification_evidence
    WHERE id IN (
        SELECT id FROM verification_evidence
        WHERE run_id = ?
        ORDER BY id
        LIMIT ?
    )
"""


@dataclass(frozen=True)
class CleanupStep:
    """What one :func:`clean_worksheet_slice` save did.

    ``run_id`` is the run it touched (None when no run was left),
    ``rows_deleted`` the evidence rows it deleted, ``run_finished`` whether
    that run has no evidence left and its ``worksheet_kept`` became 0 in this
    save, and ``runs_left`` whether any run is still left to clean after it.
    """

    run_id: int | None
    rows_deleted: int
    run_finished: bool
    runs_left: bool


_NOTHING_LEFT = CleanupStep(
    run_id=None, rows_deleted=0, run_finished=False, runs_left=False
)


def _published_bound(conn: sqlite3.Connection, site_id: int) -> int | None:
    """The site's published run id, read now, or None when there is none.

    The pointer counts only while it names a run of this site in state
    ``published``. A pointer left dangling or aimed at another state (an
    import can leave it aimed at a run that is mid-write) deletes nothing.
    """
    run_id = published_run_id(conn, site_id)
    if run_id is None:
        return None
    row = conn.execute(
        """
        SELECT 1 FROM verification_runs
        WHERE id = ? AND site_id = ? AND state = 'published'
        """,
        (run_id, site_id),
    ).fetchone()
    return None if row is None else run_id


def _next_run(conn: sqlite3.Connection, site_id: int, published: int) -> int | None:
    row = conn.execute(_NEXT_RUN_SQL, (site_id, published)).fetchone()
    return None if row is None else int(row[0])


def clean_worksheet_slice(conn: sqlite3.Connection, *, site_id: int) -> CleanupStep:
    """Run one cleanup save in the caller's write transaction (14.2).

    Picks the oldest run 14.1 allows with ``worksheet_kept`` 1, deletes up to
    ``CLEANUP_SLICE_ROWS`` of its evidence rows by ``id``, and, when that
    leaves the run with none, sets its ``worksheet_kept`` to 0 in the same
    save. Re-reads the published pointer every time. Never commits.
    """
    published = _published_bound(conn, site_id)
    if published is None:
        return _NOTHING_LEFT
    run_id = _next_run(conn, site_id, published)
    if run_id is None:
        return _NOTHING_LEFT
    deleted = conn.execute(_DELETE_SLICE_SQL, (run_id, CLEANUP_SLICE_ROWS)).rowcount
    remaining = conn.execute(
        "SELECT 1 FROM verification_evidence WHERE run_id = ? LIMIT 1", (run_id,)
    ).fetchone()
    if remaining is not None:
        return CleanupStep(
            run_id=run_id, rows_deleted=deleted, run_finished=False, runs_left=True
        )
    conn.execute(
        "UPDATE verification_runs SET worksheet_kept = 0 WHERE id = ?", (run_id,)
    )
    logger.info(
        "worksheet cleanup: run=%s of site=%s deleted (published run=%s)",
        run_id,
        site_id,
        published,
    )
    return CleanupStep(
        run_id=run_id,
        rows_deleted=deleted,
        run_finished=True,
        runs_left=_next_run(conn, site_id, published) is not None,
    )


async def clean_worksheets(
    db: Database, writer: FencedWriter, *, site_id: int, now_utc: datetime
) -> int:
    """Delete older runs' worksheets, one save per ``writer.write`` (14.2).

    Calls :func:`clean_worksheet_slice` in its own write up to
    ``CLEANUP_SLICES_PER_CALL`` times, stopping early when no run is left,
    and returns the runs finished in this call. Each slice commits on its
    own, so other writes run between them, and the call is safe to repeat.
    ``db`` and ``now_utc`` are part of the daily close's stage-8 call point
    (plan 16.1) and are not read: the cleanup reads no clock.
    """
    finished = 0
    for _ in range(CLEANUP_SLICES_PER_CALL):
        step = await writer.write(
            lambda conn: clean_worksheet_slice(conn, site_id=site_id)
        )
        if step.run_finished:
            finished += 1
        if not step.runs_left:
            break
    return finished
