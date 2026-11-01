"""The rain model ranking per day ahead (rain release, Part C, C2).

``plans/2026-11-01-rain-release.md`` C2, built from §9.1 of
``plans/2026-09-29-forecast-method.md``. Pure: no SQLite, no clock. The
publication (``scoring/publication.py``) reads the window's rain truth and
the models' daily rain totals, and calls :func:`rank_rain` once per day
ahead.

- **Counted day:** a day whose truth is rain or dry, for which the model has
  a daily total at the day ahead. Unknown days are never counted, and never
  as dry.
- **Minimum:** on when at least ``MIN_MODELS`` models have
  ``MIN_COUNTED_DAYS`` counted days; only those are eligible, and
  N = ``MIN_COUNTED_DAYS``. Off otherwise: every model with a counted day is
  eligible, N = the smallest of ``MIN_COUNTED_DAYS`` and the largest count,
  and the ranking is provisional.
- **Common days:** the models are compared on the days they all counted.
  While fewer than N are common, the model with the fewest counted days is
  dropped (on a tie, the last in (source, model) order).
- **Score:** ``MISS_WEIGHT`` · misses + false alarms over the common days,
  lowest first; ties go to fewer misses, then (source, model).
- **Selected model:** the incumbent stays unless the best-score model's total
  is lower by ``SWITCH_MARGIN`` or more; an incumbent that is not ranked is
  replaced at once, and with none the best-score model is selected.
- **Group:** the first (3·J + 3) div 4 ranked models.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

#: A daily total forecasts rain at 0.2 mm or more, compared in hundredths.
RAIN_TRIGGER_HUNDREDTHS: Final = 20
#: The window: the 60 target days ending at the cutoff.
WINDOW_DAYS: Final = 60
#: Each day ahead is ranked on its own.
RAIN_DAYS_AHEAD: Final = tuple(range(8))
#: The minimum's counted days, and how many models must reach it.
MIN_COUNTED_DAYS: Final = 20
MIN_MODELS: Final = 2
#: A miss weighs twice a false alarm (the owner's w_r, D3).
MISS_WEIGHT: Final = 2
#: The incumbent is replaced when it trails by this many points or more.
SWITCH_MARGIN: Final = 3


@dataclass(frozen=True)
class ModelSeries:
    """One model's counted days at one day ahead.

    ``counted`` maps each counted day to whether the model forecast rain
    that day (:func:`forecasts_rain` of its daily total). A day with no
    forecast or an unknown truth is not a key.
    """

    feed_id: int
    source: str
    model: str
    counted: Mapping[date, bool]


@dataclass(frozen=True)
class RankedModel:
    """One eligible model's place in the ranking.

    ``rank`` is 1-based, best first. ``misses``, ``false_alarms`` and
    ``common_days`` are over the ranking's common days; ``counted_days`` is
    the model's own count.
    """

    feed_id: int
    rank: int
    misses: int
    false_alarms: int
    common_days: int
    counted_days: int
    in_group: bool
    selected: bool


@dataclass(frozen=True)
class RainRanking:
    """The ranking of one day ahead. ``ranked`` holds the eligible models
    left after the drop rule, in rank order; ``selected_feed_id`` is ``None``
    when it is empty."""

    day_ahead: int
    provisional: bool
    ranked: tuple[RankedModel, ...]
    selected_feed_id: int | None


def forecasts_rain(total_mm: float) -> bool:
    """Whether a daily total forecasts rain: ``round(total * 100) >= 20``."""
    return round(total_mm * 100) >= RAIN_TRIGGER_HUNDREDTHS


def group_size(ranked: int) -> int:
    """The group's size q for ``ranked`` models: (3·J + 3) div 4."""
    return (3 * ranked + 3) // 4


def _order_key(model: ModelSeries) -> tuple[str, str, int]:
    return (model.source, model.model, model.feed_id)


def rank_rain(
    *,
    day_ahead: int,
    truth: Mapping[date, bool],
    models: Sequence[ModelSeries],
    incumbent_feed_id: int | None,
) -> RainRanking:
    """Rank the models at ``day_ahead`` and select one (C2).

    ``truth`` maps each known day of the window to whether it was rain
    nearby; ``models`` give each model's forecasts on its counted days. A
    model's day outside ``truth`` is not counted. ``incumbent_feed_id`` is
    the model selected most recently at this day ahead, or ``None``.
    """
    counted: dict[int, dict[date, bool]] = {}
    by_id: dict[int, ModelSeries] = {}
    for model in models:
        if model.feed_id in by_id:
            continue
        by_id[model.feed_id] = model
        counted[model.feed_id] = {
            day: rain for day, rain in model.counted.items() if day in truth
        }
    counts = {feed_id: len(days) for feed_id, days in counted.items()}

    def drop_key(model: ModelSeries) -> tuple[int, tuple[str, str, int]]:
        return (-counts[model.feed_id], _order_key(model))

    pool: list[ModelSeries]
    at_minimum = [m for m in by_id.values() if counts[m.feed_id] >= MIN_COUNTED_DAYS]
    if len(at_minimum) >= MIN_MODELS:
        provisional = False
        pool = at_minimum
        needed = MIN_COUNTED_DAYS
    else:
        provisional = True
        pool = [m for m in by_id.values() if counts[m.feed_id] >= 1]
        needed = min(
            MIN_COUNTED_DAYS, max((counts[m.feed_id] for m in pool), default=0)
        )
    if not pool:
        return RainRanking(
            day_ahead=day_ahead,
            provisional=provisional,
            ranked=(),
            selected_feed_id=None,
        )
    pool = sorted(pool, key=_order_key)
    common = _common_days(pool, counted)
    while len(common) < needed and len(pool) > 1:
        dropped = max(pool, key=drop_key)
        pool = [m for m in pool if m.feed_id != dropped.feed_id]
        common = _common_days(pool, counted)
    scored: list[tuple[int, int, int, ModelSeries]] = []
    for model in pool:
        misses = sum(
            1 for day in common if truth[day] and not counted[model.feed_id][day]
        )
        false_alarms = sum(
            1 for day in common if not truth[day] and counted[model.feed_id][day]
        )
        scored.append(
            (MISS_WEIGHT * misses + false_alarms, misses, false_alarms, model)
        )
    scored.sort(key=lambda item: (item[0], item[1], _order_key(item[3])))
    best_score, best = scored[0][0], scored[0][3]
    selected = best.feed_id
    for score, _misses, _false, model in scored:
        if model.feed_id == incumbent_feed_id:
            if score - best_score < SWITCH_MARGIN:
                selected = model.feed_id
            break
    in_group = group_size(len(scored))
    ranked = tuple(
        RankedModel(
            feed_id=model.feed_id,
            rank=index + 1,
            misses=misses,
            false_alarms=false_alarms,
            common_days=len(common),
            counted_days=counts[model.feed_id],
            in_group=index < in_group,
            selected=model.feed_id == selected,
        )
        for index, (_score, misses, false_alarms, model) in enumerate(scored)
    )
    return RainRanking(
        day_ahead=day_ahead,
        provisional=provisional,
        ranked=ranked,
        selected_feed_id=selected,
    )


def _common_days(
    pool: Sequence[ModelSeries], counted: Mapping[int, Mapping[date, bool]]
) -> frozenset[date]:
    """The days every model of ``pool`` counted."""
    days: set[date] | None = None
    for model in pool:
        own = set(counted[model.feed_id])
        days = own if days is None else days & own
    return frozenset(days or ())
