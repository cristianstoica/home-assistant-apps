"""Shared batched-scoring orchestrator for the worker's rescore lanes.

``run_batched_scoring`` replaces the worker's single monolithic
``_score_all_windows`` write transaction with bounded cell batches so the
process-wide write lock is never held for a whole rebuild. It lives in its
own module (not ``processor.py``) for two forced reasons: (i) catchup's
rescore lane needs it too, and ``processor.py`` imports ``run_catchup``
from ``catchup.py`` — catchup cannot import a processor-hosted orchestrator
back at module level without a circular import; (ii) a public name spares
the concurrent-rebuild bench (``scripts/bench_route_during_rebuild.py``)
importing a processor-private symbol.

Correctness rests on the convergence-invariant comment at the
``pair_and_score`` dispatch site (worker/processor.py) — read it before
changing anything here.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Awaitable, Callable
from typing import Final

from wxverify.db.connection import Database
from wxverify.scoring.engine import (
    ScoreCell,
    ScoreWindow,
    ScoreWork,
    discover_score_work,
    score_cell_batch,
    sweep_score_orphans,
)
from wxverify.worker.control import JobCancelled

# Fixed-size cell batches, not per window×variable: per-variable batches
# vary unboundedly with feed count and are lopsided (precip aggregates are
# the heaviest); a fixed cell count gives a deterministic per-transaction
# bound that survives catalog growth. Sizing from live evidence: ~30-90 s
# for ~900-1400 cells on the RPi ⇒ ~33-100 ms/cell ⇒ worst-case
# single-transaction hold ~0.8-2.4 s, against 30-90 s for the monolithic
# phase. Between batches the write lock is released and re-acquired
# (asyncio.Lock wakes waiters FIFO), so queued writers interleave.
# scripts/bench_route_during_rebuild.py gates the realized hold time.
SCORE_BATCH_CELLS: Final = 24

logger = logging.getLogger(__name__)


async def run_batched_scoring(
    db: Database,
    site_id: int,
    on_batch_committed: Callable[[], Awaitable[None]] | None = None,
) -> None:
    """Run the scoring rebuild for ``site_id`` in bounded write transactions.

    1. Discovery (cell queries AND the ``run_stamp`` capture) runs inside
       the batched run's FIRST write transaction, never on the read
       connection: acquiring the write lock guarantees every in-flight
       writer — in particular an inline route rescore whose
       single-transaction rebuild is uncommitted at that instant — has
       committed before the stamp is taken, so its cells are visible to the
       DISTINCT queries and get re-upserted rather than swept.
    2. Per window, each ``SCORE_BATCH_CELLS``-sized slice of cells runs in
       its own write transaction behind a site-enabled guard (raising
       ``JobCancelled`` if the site vanished or was disabled mid-run).
    3. A final transaction runs the guard plus ``sweep_score_orphans``.

    ``on_batch_committed`` is a test-only seam awaited after each batch
    transaction commits; production callers leave it ``None``.
    """
    started = time.monotonic()
    work = await db.write(lambda conn: discover_score_work(conn, site_id))
    total_cells = sum(len(window.cells) for window in work.windows)
    logger.info(
        "score discovery site=%s cells=%s elapsed=%.1fs",
        site_id,
        total_cells,
        time.monotonic() - started,
    )
    for window in work.windows:
        window_started = time.monotonic()
        batches = 0
        for start in range(0, len(window.cells), SCORE_BATCH_CELLS):
            batch = window.cells[start : start + SCORE_BATCH_CELLS]
            await db.write(
                lambda conn, w=window, b=batch: _score_batch_if_enabled(
                    conn, site_id, work, w, b
                )
            )
            batches += 1
            if on_batch_committed is not None:
                await on_batch_committed()
        logger.info(
            "score window=%s site=%s cells=%s batches=%s elapsed=%.1fs",
            window.window_key,
            site_id,
            len(window.cells),
            batches,
            time.monotonic() - window_started,
        )
    sweep_started = time.monotonic()
    removed = await db.write(
        lambda conn: _sweep_if_enabled(conn, site_id, work.run_stamp)
    )
    logger.info(
        "score sweep site=%s removed=%s elapsed=%.1fs",
        site_id,
        removed,
        time.monotonic() - sweep_started,
    )


def _ensure_site_enabled(conn: sqlite3.Connection, site_id: int) -> None:
    """Same check as processor's ``_run_score_phase_if_enabled`` (per batch)."""
    row = conn.execute("SELECT enabled FROM sites WHERE id=?", (site_id,)).fetchone()
    if row is None or not bool(row["enabled"]):
        raise JobCancelled()


def _score_batch_if_enabled(
    conn: sqlite3.Connection,
    site_id: int,
    work: ScoreWork,
    window: ScoreWindow,
    batch: tuple[ScoreCell, ...],
) -> int:
    _ensure_site_enabled(conn, site_id)
    return score_cell_batch(
        conn,
        site_id=site_id,
        window_key=window.window_key,
        cutoff=window.cutoff,
        cells=batch,
        min_n=work.min_n,
        computed_at=work.run_stamp,
    )


def _sweep_if_enabled(conn: sqlite3.Connection, site_id: int, run_stamp: str) -> int:
    _ensure_site_enabled(conn, site_id)
    return sweep_score_orphans(conn, site_id, run_stamp)
