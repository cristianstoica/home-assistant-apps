"""Scoring engine orchestration."""

from __future__ import annotations

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


def pair_and_score(conn: sqlite3.Connection, site_id: int | None = None) -> None:
    """Run the full scoring pipeline monolithically on one connection.

    HTTP routes and the CLI call this inside a single write transaction.
    The worker dispatch instead iterates ``PAIR_AND_SCORE_PHASES`` with one
    write transaction per phase so the event loop (and the healthcheck) can
    breathe between phases — see the convergence-invariant comment at the
    worker dispatch site (worker/processor.py).
    """
    for phase in PAIR_AND_SCORE_PHASES:
        phase(conn, site_id)


def _score_all_windows(conn: sqlite3.Connection, site_id: int | None = None) -> None:
    """Clear and recompute the score cache for both scoring windows."""
    _clear_score_cache(conn, site_id)
    rolling_days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    _score_window(
        conn, site_id, f"w:{rolling_days}", window_cutoff(rolling_days), min_n
    )
    _score_window(conn, site_id, "w:all", None, min_n)


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
) -> None:
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
    now = isoformat_utc()
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
