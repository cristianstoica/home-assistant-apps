"""Winner selection for one (variable, display-day) cell.

Pure logic — no SQLite. The service layer builds :class:`CellCandidate`
values from the sample query plus the skill ranking; this module applies the
fallback ladder from the spec / ADR 0001:

1. one or more *confident* rows -> top ``min(N, count)`` by skill (normal);
   this covers both the ">= N confident" and the "exactly one confident"
   rungs — a single confident feed simply yields a blend of one.
2. else any *scored* rows (pairs exist but none confident) -> rank by pair
   count, then lowest MAE, take top N (low-confidence).
3. else feeds with future samples but no scored pairs at all (fresh install)
   -> rank by future-sample count for the day, take top N (low-confidence).
4. no candidates at all -> the cell is not available.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

_MAE_NONE_LAST = float("inf")


@dataclass(frozen=True)
class CellCandidate:
    """A feed eligible for one cell: skill info (if any) + sample presence."""

    feed_id: int
    source: str
    model: str
    confident: bool
    skill_score: float | None
    pair_n: int
    mae: float | None
    future_sample_count: int


@dataclass(frozen=True)
class CellSelection:
    """Chosen feeds in rank order plus how they were chosen."""

    feeds: list[CellCandidate]
    low_confidence: bool

    @property
    def available(self) -> bool:
        return bool(self.feeds)


def representative_day_ahead(day_aheads: Sequence[int]) -> int:
    """Modal issue-relative day_ahead for a feed's samples within one tile day.

    A display day is usually served by a single run (one day_ahead); when a
    day is stitched from two runs the value covering the most hours wins, and
    ties resolve to the smaller (better-scored, shorter-lead) cell.
    """
    if not day_aheads:
        raise ValueError("representative_day_ahead requires at least one value")
    counts = Counter(day_aheads)
    best = max(counts.items(), key=lambda item: (item[1], -item[0]))
    return best[0]


def select_cell_feeds(
    candidates: Sequence[CellCandidate], *, blend_depth: int
) -> CellSelection:
    """Apply the fallback ladder to one cell's candidates.

    ``candidates`` must already be restricted to feeds that have future
    samples for the tile day (the ranking itself is exclusion-filtered
    upstream in :func:`wxverify.forecast.data.forecast_ranking`).
    """
    if not candidates:
        return CellSelection(feeds=[], low_confidence=False)
    depth = max(1, blend_depth)

    confident = [c for c in candidates if c.confident]
    if confident:
        ranked = sorted(
            confident,
            key=lambda c: (-_skill_or_zero(c.skill_score), c.source, c.model),
        )
        return CellSelection(feeds=ranked[:depth], low_confidence=False)

    scored = [c for c in candidates if c.pair_n > 0]
    if scored:
        ranked = sorted(
            scored,
            key=lambda c: (
                -c.pair_n,
                c.mae if c.mae is not None else _MAE_NONE_LAST,
                c.source,
                c.model,
            ),
        )
        return CellSelection(feeds=ranked[:depth], low_confidence=True)

    ranked = sorted(
        candidates,
        key=lambda c: (-c.future_sample_count, c.source, c.model),
    )
    return CellSelection(feeds=ranked[:depth], low_confidence=True)


def _skill_or_zero(value: float | None) -> float:
    return value if value is not None else 0.0
