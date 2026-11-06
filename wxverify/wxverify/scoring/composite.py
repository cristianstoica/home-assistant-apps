"""Read-side composite score query, from the active publication."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import date
from typing import Literal

from wxverify.collection.forecast_validation import FORECAST_VARIABLES
from wxverify.db.snapshot import read_snapshot
from wxverify.scoring.effective import MAX_DAY_AHEAD
from wxverify.scoring.leaderboard import score_badge
from wxverify.scoring.publication import (
    ScoredCell,
    active_publication,
    live_cells,
    published_cells,
)

CompositeStatus = Literal["hit", "stale", "rebuilding", "empty", "live"]


@dataclass
class CompositeParts:
    source: str
    model: str
    raw_components: dict[str, float] = field(default_factory=lambda: {})
    components: dict[str, float] = field(default_factory=lambda: {})
    #: Per-variable count of day_ahead cells that entered that variable's
    #: mean. Disclosure only -- feeds with different horizons average over
    #: different lead sets, and this is what makes that legible.
    lead_counts: dict[str, int] = field(default_factory=lambda: {})


@dataclass(frozen=True)
class CompositeResult:
    rows: list[dict[str, object]]
    status: CompositeStatus
    #: The cutoff of the publication read (9.6); None with no publication.
    evidence_through: date | None = None


def composite(
    conn: sqlite3.Connection, *, site_id: int, window: str = "rolling"
) -> list[dict[str, object]]:
    """Return composite rows for a window."""
    return composite_with_status(conn, site_id=site_id, window=window).rows


def composite_with_status(
    conn: sqlite3.Connection, *, site_id: int, window: str = "rolling"
) -> CompositeResult:
    """Return composite rows plus a status for the resolved window.

    The verdict comes from the active publication (9.2), read inside one
    WAL read snapshot (``read_snapshot``): no publication, or the site
    disabled, is ``empty``; ``rolling`` and ``all`` group the publication's
    ``w:{N}`` and ``w:all`` rows on the canonical (variable, day ahead)
    grid, ``hit``; an ``Nd`` window groups the live day sums ending at the
    publication's cutoff, ``live``. The block is read-only.
    """
    with read_snapshot(conn, label="composite"):
        return _composite_in_transaction(conn, site_id=site_id, window=window)


def composite_in_snapshot(
    conn: sqlite3.Connection, *, site_id: int, window: str = "rolling"
) -> CompositeResult:
    """``composite_with_status`` for a response that holds its own snapshot
    (9.3: the dashboard), where the facade's snapshot would refuse to nest.

    Raises ``RuntimeError`` when no transaction is open on ``conn``.
    """
    if not conn.in_transaction:
        raise RuntimeError("composite_in_snapshot needs the caller's snapshot")
    return _composite_in_transaction(conn, site_id=site_id, window=window)


def _composite_in_transaction(
    conn: sqlite3.Connection, *, site_id: int, window: str
) -> CompositeResult:
    """The verdict of ``composite_with_status``, inside the caller's snapshot."""
    return _published_composite(conn, site_id=site_id, window=window)


def _published_composite(
    conn: sqlite3.Connection, *, site_id: int, window: str
) -> CompositeResult:
    """The verdict of ``composite_with_status`` (9.2)."""
    pub = (
        active_publication(conn, site_id=site_id, at_utc=None)
        if _site_enabled(conn, site_id)
        else None
    )
    if pub is None:
        return CompositeResult(rows=[], status="empty")
    if window.endswith("d") and window[:-1].isdigit():
        days = int(window[:-1])
        cells = live_cells(conn, pub=pub, window_days=days)
        return CompositeResult(
            rows=_grouped_composite(cells, min_n=pub.min_n, window_key=f"live:{days}d"),
            status="live",
            evidence_through=pub.cutoff,
        )
    window_key = "w:all" if window == "all" else f"w:{pub.window_days}"
    cells = published_cells(conn, pub=pub, window_key=window_key)
    return CompositeResult(
        rows=_grouped_composite(cells, min_n=pub.min_n, window_key=window_key),
        status="hit",
        evidence_through=pub.cutoff,
    )


def _grouped_composite(
    cells: list[ScoredCell], *, min_n: int, window_key: str
) -> list[dict[str, object]]:
    """The canonical grid and grouping over a window's cells, in their
    (source, model, variable, day ahead) order: a cell off the canonical
    grid, with no skill score or below ``min_n`` is left out."""
    canonical_variables = set(FORECAST_VARIABLES)
    grouped: dict[int, CompositeParts] = {}
    by_variable: dict[int, dict[str, list[float]]] = {}
    for cell in cells:
        on_grid = (
            cell.variable in canonical_variables
            and 0 <= cell.day_ahead <= MAX_DAY_AHEAD
        )
        skill = cell.skill_score
        if not on_grid or skill is None or cell.n < min_n:
            continue
        grouped.setdefault(
            cell.feed_id, CompositeParts(source=cell.source, model=cell.model)
        )
        by_variable.setdefault(cell.feed_id, {}).setdefault(cell.variable, []).append(
            skill
        )
    for feed_id, variables in by_variable.items():
        parts = grouped[feed_id]
        for variable, scores in variables.items():
            raw = sum(scores) / len(scores)
            parts.raw_components[variable] = raw
            parts.components[variable] = max(0.0, raw)
            parts.lead_counts[variable] = len(scores)
    return _format_composite(grouped, window_key)


def _site_enabled(conn: sqlite3.Connection, site_id: int) -> bool:
    row = conn.execute("SELECT enabled FROM sites WHERE id=?", (site_id,)).fetchone()
    return row is not None and bool(row["enabled"])


def _format_composite(
    grouped: dict[int, CompositeParts], window_key: str
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for feed_id, parts in grouped.items():
        if not parts.components:
            continue
        score = sum(parts.components.values()) / len(parts.components)
        raw_score = sum(parts.raw_components.values()) / len(parts.raw_components)
        out.append(
            {
                "feed_id": feed_id,
                "source": parts.source,
                "model": parts.model,
                "window_key": window_key,
                "component_count": len(parts.components),
                "components": dict(sorted(parts.components.items())),
                "raw_components": dict(sorted(parts.raw_components.items())),
                "lead_counts": dict(sorted(parts.lead_counts.items())),
                "lead_cells_max": max(parts.lead_counts.values()),
                "lead_cells_total": MAX_DAY_AHEAD + 1,
                "score": score,
                "raw_score": raw_score,
                "badge": score_badge(score),
                "below_baseline": raw_score < 0,
            }
        )
    out.sort(key=_sort_key)
    return out


def _sort_key(row: dict[str, object]) -> tuple[float, str, str, int]:
    score = row["score"]
    feed_id = row["feed_id"]
    if not isinstance(score, float) or not isinstance(feed_id, int):
        raise TypeError("invalid composite row")
    return (-score, str(row["source"]), str(row["model"]), feed_id)
