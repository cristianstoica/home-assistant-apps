"""Scoring engine orchestration."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Final

from wxverify.core.timeutil import isoformat_utc_micro, window_cutoff
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.metrics import strategy_for
from wxverify.scoring.multimodel import materialize_multimodel_mean
from wxverify.scoring.pairing import pair_real_models
from wxverify.scoring.persistence import materialize_persistence
from wxverify.settings.keys import get_number_setting

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScoreCell:
    """One score-cache cell identity discovered from ``forecast_pairs``."""

    site_id: int
    feed_id: int
    variable: str
    day_ahead: int


@dataclass(frozen=True)
class ScoreWindow:
    """One scoring window's key, cutoff, and discovered cell universe."""

    window_key: str
    cutoff: str | None
    cells: tuple[ScoreCell, ...]


@dataclass(frozen=True)
class ScoreWork:
    """Frozen discovery snapshot for one scoring run.

    ``run_stamp`` is the single ``computed_at`` value every row upserted by
    the run carries; the run's final sweep deletes strictly-older rows.
    """

    run_stamp: str
    min_n: int
    windows: tuple[ScoreWindow, ...]


def pair_and_score(conn: sqlite3.Connection, site_id: int | None = None) -> None:
    """Run the full scoring pipeline monolithically on one connection.

    HTTP routes and the CLI call this inside a single write transaction.
    The worker's ``pair_and_score`` dispatch and catchup's rescore lane
    instead run the three ``PAIR_PHASES`` with one write transaction per
    phase and then the batched scoring orchestrator
    (``worker/score_batches.run_batched_scoring``) so the write lock is
    never held for a whole rebuild — see the convergence-invariant comment
    at the worker dispatch site (worker/processor.py).
    ``PAIR_AND_SCORE_PHASES`` remains for this monolithic path only.
    """
    logger.debug("pair_and_score start site=%s", site_id)
    cells = 0
    for phase in PAIR_AND_SCORE_PHASES:
        logger.debug("pair_and_score phase=%s site=%s", phase.__name__, site_id)
        result = phase(conn, site_id)
        if isinstance(result, int):
            cells += result
    logger.info(
        "scoring run complete site=%s cells=%d",
        "all" if site_id is None else site_id,
        cells,
    )


def discover_score_work(conn: sqlite3.Connection, site_id: int | None) -> ScoreWork:
    """Snapshot settings, window keys/cutoffs, cell universes, and run stamp.

    Captures ONE ``run_stamp`` for the whole run via the fixed-width
    ``isoformat_utc_micro`` formatter: the sweep's strict ``computed_at <
    run_stamp`` predicate relies on SQL string comparison as a time
    ordering, which only holds across fixed-width stamps (the plain
    ``isoformat_utc`` collapses whole seconds to ``…:00Z`` and ``'.' <
    'Z'`` would order a later same-second stamp before it).

    Callers that split the run across transactions must run discovery
    inside the run's FIRST write transaction so any in-flight inline
    rescore has committed before the stamp is taken — its cells are then
    visible here and get re-upserted rather than swept
    (worker/score_batches.py).
    """
    rolling_days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    run_stamp = isoformat_utc_micro()
    windows = tuple(
        ScoreWindow(window_key=key, cutoff=cutoff, cells=_distinct_cells(conn, site_id))
        for key, cutoff in (
            (f"w:{rolling_days}", window_cutoff(rolling_days)),
            ("w:all", None),
        )
    )
    logger.debug(
        "score discovery site=%s rolling_days=%s min_n=%s cells=%s",
        site_id,
        rolling_days,
        min_n,
        [len(window.cells) for window in windows],
    )
    return ScoreWork(run_stamp=run_stamp, min_n=min_n, windows=windows)


def score_cell_batch(
    conn: sqlite3.Connection,
    *,
    site_id: int | None,
    window_key: str,
    cutoff: str | None,
    cells: Sequence[ScoreCell],
    min_n: int,
    computed_at: str,
) -> int:
    """Aggregate and upsert the given cells for one window; returns upserts.

    ``computed_at`` is the caller's run stamp. Cells whose aggregate has
    ``n == 0`` are skipped (not upserted), exactly as before — the run's
    final ``sweep_score_orphans`` removes their stale rows. ``site_id``
    mirrors the run scope for callers/logging; each cell carries its own
    full identity.
    """
    upserts = 0
    for cell in cells:
        result = strategy_for(cell.variable).aggregate(
            conn,
            site_id=cell.site_id,
            feed_id=cell.feed_id,
            variable=cell.variable,
            day_ahead=cell.day_ahead,
            window_cutoff=cutoff,
            min_n=min_n,
        )
        if result.n == 0:
            continue
        upsert_score_cache(
            conn,
            site_id=cell.site_id,
            feed_id=cell.feed_id,
            variable=cell.variable,
            day_ahead=cell.day_ahead,
            window_key=window_key,
            result=result,
            computed_at=computed_at,
        )
        upserts += 1
    logger.debug(
        "score batch site=%s window=%s cells=%s upserts=%s",
        site_id,
        window_key,
        len(cells),
        upserts,
    )
    return upserts


def sweep_score_orphans(
    conn: sqlite3.Connection, site_id: int | None, run_stamp: str
) -> int:
    """Delete score-cache rows strictly older than ``run_stamp``.

    Strict ``<``: rows written by this run carry exactly ``run_stamp``;
    rows written by a concurrently-interleaved inline route rescore carry a
    later stamp and survive (they are fresher). This also sweeps rows under
    an abandoned ``window_key`` after an operator changes
    ``rolling_window_days``. Returns the number of rows deleted.
    """
    if site_id is None:
        cur = conn.execute(
            "DELETE FROM score_cache WHERE computed_at < ?", (run_stamp,)
        )
    else:
        cur = conn.execute(
            "DELETE FROM score_cache WHERE computed_at < ? AND site_id = ?",
            (run_stamp, site_id),
        )
    removed = int(cur.rowcount)
    logger.debug("score sweep site=%s removed=%s", site_id, removed)
    return removed


def _score_all_windows(conn: sqlite3.Connection, site_id: int | None = None) -> int:
    """Recompute the score cache for both windows: upsert, then sweep.

    Single code path shared with the batched orchestrator; run in one
    transaction this is observationally equivalent to the old
    delete-all-then-rebuild (atomic either way). Returns the number of
    score-cache cells written across both windows.
    """
    work = discover_score_work(conn, site_id)
    cells = 0
    for window in work.windows:
        cells += score_cell_batch(
            conn,
            site_id=site_id,
            window_key=window.window_key,
            cutoff=window.cutoff,
            cells=window.cells,
            min_n=work.min_n,
            computed_at=work.run_stamp,
        )
    sweep_score_orphans(conn, site_id, work.run_stamp)
    return cells


def _distinct_cells(
    conn: sqlite3.Connection, site_id: int | None
) -> tuple[ScoreCell, ...]:
    params: tuple[object, ...]
    where = ""
    if site_id is None:
        params = ()
    else:
        where = "WHERE site_id = ?"
        params = (site_id,)
    rows = conn.execute(
        f"""
        SELECT DISTINCT site_id, feed_id, variable, day_ahead
        FROM forecast_pairs
        {where}
        """,
        params,
    ).fetchall()
    return tuple(
        ScoreCell(
            site_id=int(row["site_id"]),
            feed_id=int(row["feed_id"]),
            variable=str(row["variable"]),
            day_ahead=int(row["day_ahead"]),
        )
        for row in rows
    )


# Ordered pipeline phases. Each phase only derives state from tables written
# by earlier phases (samples/observations -> pairs -> score cache), so
# running them in separate write transactions is end-state equivalent to the
# monolithic run as long as no observation write interleaves between phases.
# The worker lanes run PAIR_PHASES one-transaction-per-phase and then the
# batched scoring orchestrator (worker/score_batches.py); the monolithic
# callers (inline route rescores, CLI `_score`) run PAIR_AND_SCORE_PHASES
# in a single transaction via pair_and_score.
PAIR_PHASES: Final[tuple[Callable[[sqlite3.Connection, int | None], object], ...]] = (
    pair_real_models,
    materialize_persistence,
    materialize_multimodel_mean,
)

PAIR_AND_SCORE_PHASES: Final[
    tuple[Callable[[sqlite3.Connection, int | None], object], ...]
] = (*PAIR_PHASES, _score_all_windows)
