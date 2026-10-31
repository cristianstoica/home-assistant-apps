"""Live read-side win-rate query."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from zoneinfo import ZoneInfoNotFoundError

from wxverify.core.timeutil import isoformat_utc, local_day_slots
from wxverify.db.site_days import daily_close_on
from wxverify.db.snapshot import read_snapshot
from wxverify.db.tz_generations import published_generation_clause
from wxverify.scoring.effective import active_feed_cte
from wxverify.scoring.leaderboard import cutoff_for_window
from wxverify.scoring.publication import Publication, active_publication


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


@dataclass(frozen=True)
class WinrateResult:
    """The win-rate rows, and with the close on the cutoff they end at (9.6)."""

    rows: list[dict[str, object]]
    evidence_through: date | None = None


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
              AND {published_generation_clause("fp")}
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


# The closed part of a close-on win rate (9.2): ``close_pairs`` of the dates
# whose record is ``closed`` (a missed date's rows are never read, 7.2). A
# closed date holds one row per feed, hour and day ahead; the shortest lead
# is kept should a second ever appear, so an hour is never counted twice.
# Same two binds for ``active_feed_cte()`` first.
_CLOSED_WINRATE_SQL = f"""
    WITH {active_feed_cte()},
    closed AS (
        SELECT cp.feed_id, cp.valid_at, cp.abs_error,
               ROW_NUMBER() OVER (
                   PARTITION BY cp.feed_id, cp.valid_at
                   ORDER BY cp.lead_hours, cp.issued_at DESC
               ) AS rn
        FROM close_pairs cp
        JOIN site_days sd
          ON sd.site_id = cp.site_id AND sd.local_date = cp.local_date
        JOIN active_feeds a ON a.feed_id = cp.feed_id
        WHERE cp.site_id = ?
          AND cp.variable = ?
          AND cp.day_ahead = ?
          AND cp.abs_error IS NOT NULL
          AND sd.state = 'closed'
          AND cp.local_date >= ?
          AND cp.local_date <= ?
    )
    SELECT c.feed_id, f.source, f.model, c.valid_at, c.abs_error
    FROM closed c
    JOIN feeds f ON f.id = c.feed_id
    WHERE c.rn = 1
"""


def winrate(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str = "rolling",
) -> list[dict[str, object]]:
    return winrate_with_evidence(
        conn, site_id=site_id, variable=variable, day_ahead=day_ahead, window=window
    ).rows


def winrate_with_evidence(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str = "rolling",
) -> WinrateResult:
    """The win rate of each feed, plus the cutoff it ends at with the close on.

    Close off: today's query over ``forecast_pairs``, ``evidence_through``
    None. Close on (9.2), decided once from ``daily_close_on``: the same
    winner rule over the window's dates ending at the active publication's
    cutoff, ``close_pairs`` for closed record dates and today's
    ``forecast_pairs`` query for the starting-history dates before the first
    record; no publication gives no rows.

    Inside the caller's transaction when one is open; otherwise the switch
    is read on its own and, only when it is on, again with the rows in one
    read snapshot, since the close-on read is several statements. The
    close-off path stays today's single statement and opens no transaction.
    """
    if conn.in_transaction:
        return _winrate_now(
            conn, site_id=site_id, variable=variable, day_ahead=day_ahead, window=window
        )
    if not daily_close_on(conn):
        return _live_winrate(
            conn, site_id=site_id, variable=variable, day_ahead=day_ahead, window=window
        )
    with read_snapshot(conn, label="winrate"):
        return _winrate_now(
            conn, site_id=site_id, variable=variable, day_ahead=day_ahead, window=window
        )


def _winrate_now(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str,
) -> WinrateResult:
    """The win rate by the switch as read now, inside the caller's read."""
    if daily_close_on(conn):
        return _published_winrate(
            conn, site_id=site_id, variable=variable, day_ahead=day_ahead, window=window
        )
    return _live_winrate(
        conn, site_id=site_id, variable=variable, day_ahead=day_ahead, window=window
    )


def _live_winrate(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str,
) -> WinrateResult:
    """The close-off win rate: today's query over ``forecast_pairs``."""
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
    return WinrateResult(rows=_tally([_canonical_cell(row) for row in rows]))


def _published_winrate(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str,
) -> WinrateResult:
    """The close-on win rate (9.2), inside the caller's read."""
    pub = active_publication(conn, site_id=site_id, at_utc=None)
    if pub is None:
        return WinrateResult(rows=[])
    start = _window_start(pub, window)
    closed = conn.execute(
        _CLOSED_WINRATE_SQL,
        (
            site_id,
            site_id,
            site_id,
            variable,
            day_ahead,
            (start or date.min).isoformat(),
            pub.cutoff.isoformat(),
        ),
    ).fetchall()
    cells = [_canonical_cell(row) for row in closed]
    if start is None or start < pub.first_record_date:
        cells.extend(
            _starting_cells(
                conn,
                pub,
                site_id=site_id,
                variable=variable,
                day_ahead=day_ahead,
                start=start,
            )
        )
    # The ORDER BY of ``winrate_sql``, over both parts: the tally's float
    # sums follow it.
    cells.sort(key=lambda cell: (cell.valid_at, cell.feed_id))
    return WinrateResult(rows=_tally(cells), evidence_through=pub.cutoff)


def _window_start(pub: Publication, window: str) -> date | None:
    """The window's first date, ending at the cutoff; None for ``all``.

    ``rolling`` (and any other token, as ``resolve_window`` reads it) spans
    the publication's ``window_days``, an ``Nd`` window N dates.
    """
    if window == "all":
        return None
    days = (
        int(window[:-1])
        if window.endswith("d") and window[:-1].isdigit()
        else pub.window_days
    )
    ordinal = pub.cutoff.toordinal() - (days - 1)
    return date.fromordinal(min(max(ordinal, 1), date.max.toordinal()))


def _starting_cells(
    conn: sqlite3.Connection,
    pub: Publication,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    start: date | None,
) -> list[CanonicalCell]:
    """The starting-history part: today's ``forecast_pairs`` query, its hours
    bounded to the window's dates (from ``start``, None for no bound) that
    fall before the first record and by the cutoff (7.3).

    A site whose zone does not load contributes no starting part.
    """
    row = conn.execute("SELECT timezone FROM sites WHERE id = ?", (site_id,)).fetchone()
    if row is None:
        return []
    timezone = str(row["timezone"])
    end = min(pub.first_record_date, pub.cutoff + timedelta(days=1))
    try:
        upper = isoformat_utc(local_day_slots(end, timezone)[0])
        lower = (
            None
            if start is None
            else isoformat_utc(local_day_slots(start, timezone)[0])
        )
    except (ZoneInfoNotFoundError, ValueError):
        return []
    window_clause = "AND fp.valid_at < ?"
    params: list[object] = [site_id, site_id, site_id, variable, day_ahead, upper]
    if lower is not None:
        window_clause = "AND fp.valid_at >= ? AND fp.valid_at < ?"
        params = [site_id, site_id, site_id, variable, day_ahead, lower, upper]
    rows = conn.execute(winrate_sql(window_clause), tuple(params)).fetchall()
    return [_canonical_cell(row) for row in rows]


def _canonical_cell(row: sqlite3.Row) -> CanonicalCell:
    return CanonicalCell(
        feed_id=int(row["feed_id"]),
        source=str(row["source"]),
        model=str(row["model"]),
        valid_at=str(row["valid_at"]),
        abs_error=float(row["abs_error"]),
    )


def _tally(cells: list[CanonicalCell]) -> list[dict[str, object]]:
    """The winner rule (3.3) over canonical cells in (valid_at, feed) order."""
    stats: dict[int, FeedStats] = {}
    cells_by_valid_at: dict[str, list[CanonicalCell]] = {}
    for cell in cells:
        feed_stats = stats.setdefault(
            cell.feed_id, FeedStats(source=cell.source, model=cell.model)
        )
        feed_stats.covered += 1
        cells_by_valid_at.setdefault(cell.valid_at, []).append(cell)

    for group in cells_by_valid_at.values():
        if len(group) < 2:
            continue
        best = min(cell.abs_error for cell in group)
        winners = [cell for cell in group if abs(cell.abs_error - best) <= 1e-9]
        credit = 1.0 / len(winners)
        winner_ids = {cell.feed_id for cell in winners}
        for cell in group:
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
