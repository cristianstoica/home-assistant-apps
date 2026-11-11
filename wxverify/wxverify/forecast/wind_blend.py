"""Weighted wind forecast: every eligible feed, weighted by past accuracy.

The served wind is not the blend-depth selection. Each feed's weight at a lead
``k`` is ``1 / max(MAE, FLOOR_MS)``, where the MAE compares the feed's
forecast daily high (the latest run with at least ``MIN_RUN_HOURS`` hours for
the target day) with the observed daily high (a local day with at least
``MIN_OBS_HOURS`` hours) over the last ``TRAINING_DAYS`` complete local days.
A feed needs ``MIN_TRAINING_DAYS`` matched days to get a weight, and a cell
needs ``MIN_FEEDS`` weighted feeds with ``MIN_RUN_HOURS`` hours to be served
at all; otherwise it is shown as unavailable with a note, never silently
narrowed.

Units are m/s throughout, like storage; the callers convert for display.

The served weights are the active publication's (``load_wind_serving``);
the publication builds them with ``compute_wind_weights``.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

from wxverify.forecast.aggregate import EXTREMA_COVERAGE_NOT_EVALUATED
from wxverify.forecast.selection import CellCandidate, CellSelection

FLOOR_MS: Final = 0.1
TRAINING_DAYS: Final = 30
MIN_OBS_HOURS: Final = 22
MIN_RUN_HOURS: Final = 20
MIN_TRAINING_DAYS: Final = 20
MIN_FEEDS: Final = 2
DAY_AHEADS: Final = range(8)
WIND_METHOD: Final = "inverse_mae_daily_high_v1"

NOT_ENOUGH_FEEDS_NOTE: Final = "Not enough feeds with a wind track record yet"


@dataclass(frozen=True)
class WindTraining:
    """The daily highs the weights are trained on, in m/s."""

    #: (feed_id, k) -> target local date -> the forecast daily high of the
    #: latest run at lead k with at least ``MIN_RUN_HOURS`` hours.
    feed_highs: dict[tuple[int, int], dict[date, float]]
    #: Target local date -> the observed daily high, for days with at least
    #: ``MIN_OBS_HOURS`` hours whose station inventory is settled.
    observed_highs: dict[date, float]


@dataclass(frozen=True)
class FeedWeight:
    """One (feed, lead) weight and the evidence behind it."""

    mae_ms: float
    days: int
    weight: float


#: (feed_id, k) -> weight; pairs with fewer than ``MIN_TRAINING_DAYS`` are absent.
WindWeights = dict[tuple[int, int], FeedWeight]


@dataclass(frozen=True)
class WindChoice:
    """The served wind feeds for one cell, their normalized weights and a note."""

    selection: CellSelection
    #: feed_id -> weight, normalized to sum 1; empty when not served.
    weights: dict[int, float]
    note: str | None


@dataclass(frozen=True)
class WindServing:
    """What one request serves for wind."""

    weights: WindWeights


def compute_wind_weights(training: WindTraining) -> WindWeights:
    """Inverse-MAE weight per (feed, k) over the days both sides have. Pure."""
    weights: WindWeights = {}
    for key in sorted(training.feed_highs):
        highs = training.feed_highs[key]
        days = sorted(day for day in highs if day in training.observed_highs)
        if len(days) < MIN_TRAINING_DAYS:
            continue
        mae = sum(abs(highs[day] - training.observed_highs[day]) for day in days) / len(
            days
        )
        weights[key] = FeedWeight(
            mae_ms=mae, days=len(days), weight=1.0 / max(mae, FLOOR_MS)
        )
    return weights


def empty_wind_selection() -> CellSelection:
    """The selection of a wind cell that is not served (``available`` False)."""
    return CellSelection(
        feeds=[],
        low_confidence=False,
        extrema_feeds=[],
        extrema_coverage=EXTREMA_COVERAGE_NOT_EVALUATED,
        extrema_low_confidence=False,
    )


def choose_wind_feeds(
    candidates: Sequence[CellCandidate],
    rep_k: Mapping[int, int],
    weights: WindWeights,
) -> WindChoice:
    """Every eligible feed, ordered by weight; unavailable below ``MIN_FEEDS``.

    A candidate is eligible when it has a weight at its representative lead
    ``rep_k[feed_id]`` and at least ``MIN_RUN_HOURS`` covered hours.
    """
    eligible: list[tuple[CellCandidate, float]] = []
    for candidate in candidates:
        k = rep_k.get(candidate.feed_id)
        if k is None or candidate.covered_hours < MIN_RUN_HOURS:
            continue
        feed_weight = weights.get((candidate.feed_id, k))
        if feed_weight is None:
            continue
        eligible.append((candidate, feed_weight.weight))
    if len(eligible) < MIN_FEEDS:
        return WindChoice(
            selection=empty_wind_selection(), weights={}, note=NOT_ENOUGH_FEEDS_NOTE
        )
    eligible.sort(key=lambda item: (-item[1], item[0].feed_id))
    total = sum(weight for _, weight in eligible)
    return WindChoice(
        selection=CellSelection(
            feeds=[candidate for candidate, _ in eligible],
            low_confidence=False,
            extrema_feeds=[],
            extrema_coverage=EXTREMA_COVERAGE_NOT_EVALUATED,
            extrema_low_confidence=False,
        ),
        weights={candidate.feed_id: weight / total for candidate, weight in eligible},
        note=None,
    )


def weighted_daily_high(
    values: Mapping[int, Sequence[float]], weights: Mapping[int, float]
) -> float | None:
    """Σ w·max(v) / Σ w over the weighted feeds with values; None when none.

    Summed in feed-id order, so the result does not depend on mapping order.
    """
    numerator = 0.0
    denominator = 0.0
    for feed_id in sorted(values):
        feed_values = values[feed_id]
        weight = weights.get(feed_id)
        if not feed_values or weight is None:
            continue
        numerator += weight * max(feed_values)
        denominator += weight
    if denominator <= 0:
        return None
    return numerator / denominator


def weighted_hourly(
    series: Mapping[int, Mapping[str, float]], weights: Mapping[int, float]
) -> dict[str, float]:
    """Per-hour weighted mean over the feeds present at that hour.

    ``series`` maps each feed to its values by ``valid_at``. The weights are
    renormalized at every hour over the feeds that have a value there; an
    hour with no weighted feed is absent. Keys come back in hour order.
    """
    numerator: dict[str, float] = {}
    denominator: dict[str, float] = {}
    for feed_id in sorted(series):
        weight = weights.get(feed_id)
        if weight is None:
            continue
        for hour, value in series[feed_id].items():
            numerator[hour] = numerator.get(hour, 0.0) + weight * value
            denominator[hour] = denominator.get(hour, 0.0) + weight
    return {
        hour: numerator[hour] / denominator[hour]
        for hour in sorted(numerator)
        if denominator[hour] > 0
    }


def load_wind_serving(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    timezone: str,
    today: date,
) -> WindServing:
    """The wind weights of the active publication.

    The latest publication's, for the feeds switched on now; ``{}`` with no
    publication, which wind serving reads as "not enough feeds". ``timezone``
    and ``today`` are not read.
    """
    # Deferred: the publication module imports this one.
    from wxverify.scoring.publication import published_wind_serving

    return WindServing(
        weights=published_wind_serving(conn, site_id=site_id, as_of=None)
    )
