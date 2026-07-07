"""Live read-side win-rate query."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.leaderboard import cutoff_for_window


@dataclass(frozen=True)
class CanonicalCell:
    feed_id: int
    source: str
    model: str
    valid_at: str
    issued_at: str
    abs_error: float


@dataclass
class FeedStats:
    source: str
    model: str
    covered: int = 0
    comparable: int = 0
    wins: float = 0.0


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
    params: list[object] = [site_id, variable, day_ahead]
    if cutoff is not None:
        params.append(cutoff)
    rows = conn.execute(
        f"""
        SELECT fp.feed_id, f.source, f.model, fp.valid_at, fp.issued_at,
               fp.abs_error
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE fp.site_id = ?
          AND fp.variable = ?
          AND fp.day_ahead = ?
          AND fp.abs_error IS NOT NULL
          {window_clause}
          AND {active_competitor_clause(site_expr="fp.site_id")}
        ORDER BY fp.valid_at, fp.feed_id, fp.issued_at
        """,
        tuple(params),
    ).fetchall()
    canonical: dict[tuple[int, str], CanonicalCell] = {}
    stats: dict[int, FeedStats] = {}
    for row in rows:
        feed_id = int(row["feed_id"])
        cell = CanonicalCell(
            feed_id=feed_id,
            source=str(row["source"]),
            model=str(row["model"]),
            valid_at=str(row["valid_at"]),
            issued_at=str(row["issued_at"]),
            abs_error=float(row["abs_error"]),
        )
        stats.setdefault(feed_id, FeedStats(source=cell.source, model=cell.model))
        key = (feed_id, cell.valid_at)
        previous = canonical.get(key)
        if previous is None or cell.issued_at > previous.issued_at:
            canonical[key] = cell

    cells_by_valid_at: dict[str, list[CanonicalCell]] = {}
    for cell in canonical.values():
        stats[cell.feed_id].covered += 1
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
