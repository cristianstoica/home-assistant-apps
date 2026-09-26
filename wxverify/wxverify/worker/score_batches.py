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
from dataclasses import dataclass
from typing import Final

from wxverify.db.connection import EpochMoved, FencedWriter
from wxverify.scoring.engine import (
    SPLIT_PAIR_PHASES,
    ScoreCell,
    ScoreWindow,
    ScoreWork,
    SplitPhase,
    discover_score_work,
    score_cell_batch,
    sweep_score_orphans,
)
from wxverify.scoring.split import (
    SCORING_APPLY_CHUNK_ROWS,
    GenerationResolver,
    PairDelta,
    PairOp,
    apply_pair_ops,
    chunk_ops,
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

# CAS attempts per scoring step. A miss needs a commit that could change a
# scoring input to land during that step's compute; the last miss raises
# ScoringInputsBusy.
SCORING_CAS_ATTEMPTS: Final = 3

logger = logging.getLogger(__name__)


class ScoringInputsBusy(Exception):
    """A scoring step's inputs changed on every one of its CAS attempts.

    Deliberately not a ``JobControl``: each caller decides what it means (the
    processor defers the job, catchup hands the site to the main lane). It
    carries only the site id and the step name, never an attempt's payload.
    """

    def __init__(self, site_id: int, step: str) -> None:
        super().__init__(f"site {site_id}: {step}")
        self.site_id = site_id
        self.step = step


@dataclass(frozen=True, slots=True)
class _AttemptStats:
    """What one CAS attempt reports: flags and numbers, never its payload."""

    moved: bool
    compute_s: float
    apply_s: float
    apply_max: float
    applies: int
    ops: int


async def run_split_pair_phases(
    writer: FencedWriter, site_id: int, *, require_enabled: bool
) -> None:
    """Run every pair phase for ``site_id``: compute off-lock, apply under CAS.

    Each phase in ``SPLIT_PAIR_PHASES`` computes its delta on a pooled read
    connection at an input epoch, then applies it in chunks of at most
    ``SCORING_APPLY_CHUNK_ROWS`` statements through ``write_if_current``,
    each chunk at the epoch the previous one returned. A miss discards the
    rest of the delta and recomputes from committed state; after
    ``SCORING_CAS_ATTEMPTS`` misses in a row for one phase,
    ``ScoringInputsBusy`` propagates. ``require_enabled`` runs the
    site-enabled check at the top of every chunk, inside its transaction.
    ``SPLIT_PAIR_PHASES`` and ``SCORING_APPLY_CHUNK_ROWS`` are looked up on
    this module at call time.
    """
    for phase in SPLIT_PAIR_PHASES:
        started = time.monotonic()
        compute_s = apply_s = apply_max = 0.0
        chunks = ops_applied = 0
        attempt = 0
        for attempt in range(1, SCORING_CAS_ATTEMPTS + 1):
            stats = await _split_pair_attempt(
                writer, site_id, phase, require_enabled=require_enabled
            )
            compute_s += stats.compute_s
            apply_s += stats.apply_s
            apply_max = max(apply_max, stats.apply_max)
            chunks, ops_applied = stats.applies, stats.ops
            if not stats.moved:
                break
            logger.info(
                "score phase=%s site=%s cas_miss attempt=%d",
                phase.name,
                site_id,
                attempt,
            )
        else:
            raise ScoringInputsBusy(site_id, phase.name)
        logger.info(
            "score phase=%s site=%s elapsed=%.1fs compute=%.1fs apply=%.1fs "
            "apply_max=%.1fs chunks=%d ops=%d attempts=%d",
            phase.name,
            site_id,
            time.monotonic() - started,
            compute_s,
            apply_s,
            apply_max,
            chunks,
            ops_applied,
            attempt,
        )


async def _split_pair_attempt(
    writer: FencedWriter, site_id: int, phase: SplitPhase, *, require_enabled: bool
) -> _AttemptStats:
    """One compute and its chunked apply. Every reference to the delta is a
    local of this frame, so it is released when this returns."""
    compute_started = time.monotonic()
    epoch, delta = await writer.read_at_epoch(
        lambda conn, run=phase.compute: run(conn, site_id),
        label=f"score compute {phase.name}",
    )
    compute_s = time.monotonic() - compute_started
    resolver = GenerationResolver(delta.seed_sites)
    apply_s = apply_max = 0.0
    chunks = ops_applied = 0
    moved = False
    for index, chunk in enumerate(chunk_ops(delta.ops, cap=SCORING_APPLY_CHUNK_ROWS)):
        apply_started = time.monotonic()
        result = await writer.write_if_current(
            lambda conn, ops=chunk, first=(index == 0), d=delta, res=resolver: (
                _apply_split_chunk(
                    conn,
                    site_id,
                    d,
                    ops,
                    res,
                    first=first,
                    require_enabled=require_enabled,
                )
            ),
            epoch=epoch,
        )
        held = time.monotonic() - apply_started
        apply_s += held
        apply_max = max(apply_max, held)
        if isinstance(result, EpochMoved):
            moved = True
            break
        applied, epoch = result
        chunks += 1
        ops_applied += applied
    return _AttemptStats(
        moved=moved,
        compute_s=compute_s,
        apply_s=apply_s,
        apply_max=apply_max,
        applies=chunks,
        ops=ops_applied,
    )


def _apply_split_chunk(
    conn: sqlite3.Connection,
    site_id: int,
    delta: PairDelta,
    ops: tuple[PairOp, ...],
    resolver: GenerationResolver,
    *,
    first: bool,
    require_enabled: bool,
) -> int:
    """Apply one chunk of ``delta`` in the caller's CAS transaction.

    The enabled check (when required) comes first; the first chunk then
    seeds every seed site's published generation before any op, and the
    later chunks reuse the resolver's cached ids. Returns the number of ops
    applied, not the INSERT rowcount.
    """
    if require_enabled:
        _ensure_site_enabled(conn, site_id)
    if first:
        resolver.seed(conn)
    apply_pair_ops(conn, delta, ops, resolver)
    return len(ops)


async def run_batched_scoring(
    writer: FencedWriter,
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
    work = await writer.write(lambda conn: discover_score_work(conn, site_id))
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
            await writer.write(
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
    removed = await writer.write(
        lambda conn: _sweep_if_enabled(conn, site_id, work.run_stamp)
    )
    logger.info(
        "score sweep site=%s removed=%s elapsed=%.1fs",
        site_id,
        removed,
        time.monotonic() - sweep_started,
    )


def _ensure_site_enabled(conn: sqlite3.Connection, site_id: int) -> None:
    """Raise ``JobCancelled`` unless the site exists and is enabled.

    Scoring writes that must skip a disabled or deleted site call this first,
    inside their write transaction, so the check sees the committed ``sites``
    row and a site disabled mid-run stops at its next such write.
    """
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
