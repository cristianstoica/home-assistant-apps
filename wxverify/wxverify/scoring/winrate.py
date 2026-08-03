"""Live read-side win-rate query."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from wxverify.scoring.effective import active_feed_cte
from wxverify.scoring.leaderboard import cutoff_for_window


@dataclass(frozen=True)
class CanonicalCell:
    feed_id: int
    source: str
    model: str
    valid_at: str
    abs_error: float


@dataclass
class FeedStats:
    source: str
    model: str
    covered: int = 0
    comparable: int = 0
    wins: float = 0.0


def winrate_sql(window_clause: str) -> str:
    """Canonical-cell query: latest issued_at per (feed, valid_at), in SQL.

    Canonicalizes via ROW_NUMBER() rather than fetching every candidate row
    and reducing in Python: UNIQUE(site_id, feed_id, variable, issued_at,
    valid_at) makes issued_at unique within a (feed, valid_at) slot, so
    rn = 1 always picks exactly the row the old max-issued_at reduction
    picked, with no ties possible. active_feed_cte() replaces a per-row
    correlated EXISTS with a JOIN against a feed set computed once.
    """
    return f"""
        WITH {active_feed_cte()},
        canonical AS (
            SELECT fp.feed_id, fp.valid_at, fp.abs_error,
                   ROW_NUMBER() OVER (
                       PARTITION BY fp.feed_id, fp.valid_at
                       ORDER BY fp.issued_at DESC
                   ) AS rn
            FROM forecast_pairs fp
            JOIN active_feeds a ON a.feed_id = fp.feed_id
            WHERE fp.site_id = ?
              AND fp.variable = ?
              AND fp.day_ahead = ?
              AND fp.abs_error IS NOT NULL
              {window_clause}
        )
        SELECT c.feed_id, f.source, f.model, c.valid_at, c.abs_error
        FROM canonical c
        JOIN feeds f ON f.id = c.feed_id
        WHERE c.rn = 1
        -- Load-bearing, not cosmetic: cells_by_valid_at below groups rows by
        -- insertion order (dict order), and the win/tie loop sums float
        -- credits per group in that order -- float addition is
        -- order-dependent, so this ORDER BY is what makes `wins`
        -- bit-identical across runs rather than merely approximately equal.
        ORDER BY c.valid_at, c.feed_id
        """


def winrate(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str = "rolling",
) -> list[dict[str, object]]:
    cutoff = cutoff_for_window(conn, window)
    window_clause = "" if cutoff is None else "AND fp.valid_at >= ?"
    # active_feed_cte() consumes TWO binds (its LEFT JOIN, and the meteoblue
    # package EXISTS inside active_competitor_clause) and they come first
    # because the CTE is textually first -- same bind shape as
    # leaderboard._expected_active_feed_ids.
    params: list[object] = [site_id, site_id, site_id, variable, day_ahead]
    if cutoff is not None:
        params.append(cutoff)
    rows = conn.execute(winrate_sql(window_clause), tuple(params)).fetchall()
    stats: dict[int, FeedStats] = {}
    cells_by_valid_at: dict[str, list[CanonicalCell]] = {}
    for row in rows:
        feed_id = int(row["feed_id"])
        cell = CanonicalCell(
            feed_id=feed_id,
            source=str(row["source"]),
            model=str(row["model"]),
            valid_at=str(row["valid_at"]),
            abs_error=float(row["abs_error"]),
        )
        feed_stats = stats.setdefault(
            feed_id, FeedStats(source=cell.source, model=cell.model)
        )
        feed_stats.covered += 1
        cells_by_valid_at.setdefault(cell.valid_at, []).append(cell)

    for cells in cells_by_valid_at.values():
        if len(cells) < 2:
            continue
        best = min(cell.abs_error for cell in cells)
        winners = [cell for cell in cells if abs(cell.abs_error - best) <= 1e-9]
        credit = 1.0 / len(winners)
        winner_ids = {cell.feed_id for cell in winners}
        for cell in cells:
            feed_stats = stats[cell.feed_id]
            feed_stats.comparable += 1
            if cell.feed_id in winner_ids:
                feed_stats.wins += credit

    return [
        {
            "feed_id": feed_id,
            "source": feed_stats.source,
            "model": feed_stats.model,
            "covered": feed_stats.covered,
            "comparable": feed_stats.comparable,
            "wins": feed_stats.wins,
            "win_rate": None
            if feed_stats.comparable == 0
            else feed_stats.wins / feed_stats.comparable,
        }
        for feed_id, feed_stats in sorted(
            stats.items(),
            key=lambda item: (
                1.0
                if item[1].comparable == 0
                else -(item[1].wins / item[1].comparable),
                item[1].source,
                item[1].model,
                item[0],
            ),
        )
    ]
