"""Read-side win-rate query."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date

from wxverify.db.snapshot import read_snapshot
from wxverify.scoring.history import winrate_errors
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
    """The win-rate rows, and the cutoff they end at (9.6)."""

    rows: list[dict[str, object]]
    evidence_through: date | None = None


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
    """The win rate of each feed, plus the cutoff it ends at (9.2).

    The winner rule over the window's dates ending at the active
    publication's cutoff, read from ``forecast_history`` + ``observations``
    for the closed record dates and the pre-close dates before the first
    record together, persistence rebuilt at the shortest lead
    (storage-release plan 3.6, ``scoring.history.winrate_errors``); no
    publication gives no rows.

    Inside the caller's transaction when one is open; otherwise in one read
    snapshot of its own, since the read is several statements.
    """
    if conn.in_transaction:
        return _published_winrate(
            conn, site_id=site_id, variable=variable, day_ahead=day_ahead, window=window
        )
    with read_snapshot(conn, label="winrate"):
        return _published_winrate(
            conn, site_id=site_id, variable=variable, day_ahead=day_ahead, window=window
        )


def _published_winrate(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str,
) -> WinrateResult:
    """The win rate (9.2), inside the caller's read: the errors of the
    window's dates ending at the cutoff (storage-release plan 3.6), closed
    and pre-close together, persistence's among them."""
    pub = active_publication(conn, site_id=site_id, at_utc=None)
    if pub is None:
        return WinrateResult(rows=[])
    cells = [
        CanonicalCell(
            feed_id=error.feed_id,
            source=error.source,
            model=error.model,
            valid_at=error.valid_at,
            abs_error=error.abs_error,
        )
        for error in winrate_errors(
            conn,
            site_id=site_id,
            variable=variable,
            day_ahead=day_ahead,
            start=_window_start(pub, window),
            end=pub.cutoff,
        )
    ]
    # Load-bearing: the tally groups cells in this (valid_at, feed) order
    # and sums float credits in it, so ``wins`` is bit-identical across runs.
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
