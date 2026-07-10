"""Scoring engine orchestration."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable
from typing import Final

from wxverify.core.timeutil import isoformat_utc, window_cutoff
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.metrics import strategy_for
from wxverify.scoring.multimodel import materialize_multimodel_mean
from wxverify.scoring.pairing import pair_real_models
from wxverify.scoring.persistence import materialize_persistence
from wxverify.settings.keys import get_number_setting

logger = logging.getLogger(__name__)


def pair_and_score(conn: sqlite3.Connection, site_id: int | None = None) -> None:
    """Run the full scoring pipeline monolithically on one connection.

    HTTP routes and the CLI call this inside a single write transaction.
    The worker dispatch instead iterates ``PAIR_AND_SCORE_PHASES`` with one
    write transaction per phase so the event loop (and the healthcheck) can
    breathe between phases — see the convergence-invariant comment at the
    worker dispatch site (worker/processor.py).
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


def _score_all_windows(conn: sqlite3.Connection, site_id: int | None = None) -> int:
    """Clear and recompute the score cache for both scoring windows.

    Returns the number of score-cache cells written across both windows.
    """
    _clear_score_cache(conn, site_id)
    rolling_days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    logger.debug(
        "score windows site=%s rolling_days=%s min_n=%s", site_id, rolling_days, min_n
    )
    cells = _score_window(
        conn, site_id, f"w:{rolling_days}", window_cutoff(rolling_days), min_n
    )
    cells += _score_window(conn, site_id, "w:all", None, min_n)
    return cells


# Ordered pipeline phases. Each phase only derives state from tables written
# by earlier phases (samples/observations -> pairs -> score cache), so
# running them in separate write transactions is end-state equivalent to the
# monolithic run as long as no observation write interleaves between phases.
PAIR_AND_SCORE_PHASES: Final[
    tuple[Callable[[sqlite3.Connection, int | None], object], ...]
] = (
    pair_real_models,
    materialize_persistence,
    materialize_multimodel_mean,
    _score_all_windows,
)


def _clear_score_cache(conn: sqlite3.Connection, site_id: int | None) -> None:
    if site_id is None:
        conn.execute("DELETE FROM score_cache")
        return
    conn.execute("DELETE FROM score_cache WHERE site_id=?", (site_id,))


def _score_window(
    conn: sqlite3.Connection,
    site_id: int | None,
    window_key: str,
    cutoff: str | None,
    min_n: int,
) -> int:
    """Recompute one scoring window; returns the number of cells upserted."""
    params: tuple[object, ...]
    where = ""
    if site_id is None:
        params = ()
    else:
        where = "WHERE site_id = ?"
        params = (site_id,)
    cells = conn.execute(
        f"""
        SELECT DISTINCT site_id, feed_id, variable, day_ahead
        FROM forecast_pairs
        {where}
        """,
        params,
    ).fetchall()
    logger.debug(
        "score window key=%s cells=%s cutoff=%s", window_key, len(cells), cutoff
    )
    now = isoformat_utc()
    upserts = 0
    for cell in cells:
        result = strategy_for(str(cell["variable"])).aggregate(
            conn,
            site_id=int(cell["site_id"]),
            feed_id=int(cell["feed_id"]),
            variable=str(cell["variable"]),
            day_ahead=int(cell["day_ahead"]),
            window_cutoff=cutoff,
            min_n=min_n,
        )
        if result.n == 0:
            continue
        upsert_score_cache(
            conn,
            site_id=int(cell["site_id"]),
            feed_id=int(cell["feed_id"]),
            variable=str(cell["variable"]),
            day_ahead=int(cell["day_ahead"]),
            window_key=window_key,
            result=result,
            computed_at=now,
        )
        upserts += 1
    logger.debug("score window key=%s upserts=%s", window_key, upserts)
    return upserts
