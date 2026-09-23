"""Winner selection for one (variable, display-day) cell.

Pure logic — no SQLite. The service layer builds :class:`CellCandidate`
values from the sample query plus the skill ranking; this module applies the
fallback ladder below.

A selection returns two products with different coverage requirements:

* ``feeds`` — the hourly blend set. An hourly series needs no complete day:
  every plotted point is a real forecast for its hour. It drives the hourly
  drill-down's per-feed series for every variable, its aggregate line for
  temperature and wind, the cell state and the ``partial`` badge.
* ``extrema_feeds`` — the feed set a displayed daily value is computed from
  (temperature's high and low, precipitation's total and wet-hour count),
  built only when the caller passes ``extrema_coverage_required=True``. It
  also supplies precipitation's drill-down aggregate line. Eligibility
  (``CellCandidate.extrema_eligible``) is specific to the variable: a
  temperature candidate qualifies when its own samples cover the whole
  target local day
  (:func:`wxverify.forecast.aggregate.covers_local_day`), a precipitation
  candidate when they supply each hour of it exactly once
  (:func:`wxverify.forecast.aggregate.covers_local_day_exactly`), because a
  sum and a count need each hour once where an extremum needs it at least
  once.

Blend set. Before the ladder ranks, candidates are first restricted to a
coverage pool: those clearing ``MIN_SPREAD_HOURS`` when any do, else
multi-point feeds (>= ``MULTIPOINT_MIN_HOURS``) when any do, else all
candidates (the all-single-point fallback). The ladder then ranks within
that pool. This keeps a lone far-horizon single-point feed (whose daily
high == low) from winning on skill alone while a multi-point feed is
available. A max == min value from a single point is truthful only when it
is labelled as a limited-period value, never under a daily high/low label;
whether a displayed daily value is shown at all is decided by the
variable's coverage rule, ``aggregate.covers_local_day`` or
``aggregate.covers_local_day_exactly``, not by this pool.

Extrema set. Eligibility is decided over the whole candidate list BEFORE any
ranking; the eligible candidates are then ranked by the same ladder and cut
to depth. There is no pool fallback and the depth budget is never
backfilled with ineligible feeds: when none qualifies the set is empty and
``extrema_coverage`` is ``EXTREMA_COVERAGE_INSUFFICIENT``. When the caller
does not ask, the set is empty and ``EXTREMA_COVERAGE_NOT_EVALUATED``. Each
set records the ladder's own confidence verdict: ``low_confidence`` for the
blend set, ``extrema_low_confidence`` for the extrema set (False unless the
extrema coverage is ``EXTREMA_COVERAGE_COMPLETE``).

The ladder:

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

from wxverify.forecast.aggregate import (
    EXTREMA_COVERAGE_COMPLETE,
    EXTREMA_COVERAGE_INSUFFICIENT,
    EXTREMA_COVERAGE_NOT_EVALUATED,
    MIN_SPREAD_HOURS,
    MULTIPOINT_MIN_HOURS,
)

_MAE_NONE_LAST = float("inf")


@dataclass(frozen=True)
class CellCandidate:
    """A feed eligible for one cell: skill info (if any) + sample presence.

    ``covered_hours`` is the count of distinct UTC hour instants the feed supplies
    for this tile day — the selection-side coverage signal used to build the
    pre-ladder coverage pool.

    ``extrema_eligible`` is whether the feed's own samples satisfy its
    variable's coverage predicate for the target local day:
    :func:`wxverify.forecast.aggregate.covers_local_day_exactly` for
    precipitation, which also rejects a repeated or off-hour instant, and
    :func:`wxverify.forecast.aggregate.covers_local_day` for every other
    variable. It is computed where the samples are, so selection stays free
    of timezones and dates, and it has no default so every construction site
    must answer it.
    """

    feed_id: int
    source: str
    model: str
    confident: bool
    skill_score: float | None
    pair_n: int
    mae: float | None
    future_sample_count: int
    covered_hours: int
    extrema_eligible: bool


@dataclass(frozen=True)
class CellSelection:
    """Chosen feeds in rank order plus how they were chosen."""

    feeds: list[CellCandidate]
    low_confidence: bool
    extrema_feeds: list[CellCandidate]
    extrema_coverage: str
    extrema_low_confidence: bool

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
    candidates: Sequence[CellCandidate],
    *,
    blend_depth: int,
    extrema_coverage_required: bool,
) -> CellSelection:
    """Apply the fallback ladder to one cell's candidates.

    ``candidates`` must already be restricted to feeds that have future
    samples for the tile day (the ranking itself is exclusion-filtered
    upstream in :func:`wxverify.forecast.data.forecast_ranking`).

    ``feeds`` and ``low_confidence`` are the blend set, chosen by the ladder
    over the coverage pool. ``extrema_coverage_required`` has no default so
    each caller states its policy: when True, ``extrema_feeds`` is the same
    ladder over the ``extrema_eligible`` candidates alone, never backfilled,
    and ``extrema_low_confidence`` is that ranking's own verdict (False when
    no candidate is eligible). ``feeds`` and ``low_confidence`` never depend
    on this argument; when False, ``extrema_feeds`` is empty with
    ``EXTREMA_COVERAGE_NOT_EVALUATED`` and ``extrema_low_confidence`` is
    False.
    """
    depth = max(1, blend_depth)
    feeds: list[CellCandidate] = []
    low_confidence = False
    if candidates:
        adequate = [c for c in candidates if c.covered_hours >= MIN_SPREAD_HOURS]
        multipoint = [c for c in candidates if c.covered_hours >= MULTIPOINT_MIN_HOURS]
        pool = adequate or multipoint or candidates
        ranked, low_confidence = _rank(pool)
        feeds = ranked[:depth]

    if not extrema_coverage_required:
        return CellSelection(
            feeds=feeds,
            low_confidence=low_confidence,
            extrema_feeds=[],
            extrema_coverage=EXTREMA_COVERAGE_NOT_EVALUATED,
            extrema_low_confidence=False,
        )
    eligible = [c for c in candidates if c.extrema_eligible]
    extrema_feeds: list[CellCandidate] = []
    extrema_coverage = EXTREMA_COVERAGE_INSUFFICIENT
    extrema_low_confidence = False
    if eligible:
        extrema_ranked, extrema_low_confidence = _rank(eligible)
        extrema_feeds = extrema_ranked[:depth]
        extrema_coverage = EXTREMA_COVERAGE_COMPLETE
    return CellSelection(
        feeds=feeds,
        low_confidence=low_confidence,
        extrema_feeds=extrema_feeds,
        extrema_coverage=extrema_coverage,
        extrema_low_confidence=extrema_low_confidence,
    )


def _rank(pool: Sequence[CellCandidate]) -> tuple[list[CellCandidate], bool]:
    """Rank candidates by the ladder; return ``(ranked, low_confidence)``.

    Shared by the blend set and the extrema set, so eligibility changes which
    candidates are ranked, never how. Each rung keeps only its own rows, so
    the caller's depth cut never reaches into a lower rung.
    """
    confident = [c for c in pool if c.confident]
    if confident:
        ranked = sorted(
            confident,
            key=lambda c: (-_skill_or_zero(c.skill_score), c.source, c.model),
        )
        return ranked, False

    scored = [c for c in pool if c.pair_n > 0]
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
        return ranked, True

    ranked = sorted(
        pool,
        key=lambda c: (-c.future_sample_count, c.source, c.model),
    )
    return ranked, True


def _skill_or_zero(value: float | None) -> float:
    return value if value is not None else 0.0
