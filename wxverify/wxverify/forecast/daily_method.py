"""Daily forecast-method trial: the pure rules (Phase I, reused by Phase II).

Every function here is pure: no database, no clock, no file or network I/O.
Every tunable is a required keyword argument, so a caller cannot fall back to
a silent default. Millimetre lines are compared in hundredths, as integers
(``round(total * 100)``), and every ratio is a :class:`fractions.Fraction`.

A model is named by one key, the string ``"<source>/<model>"``. The key is
only compared for equality, never split; every tie order sorts on the
``(source, model)`` pair carried with each entry, never on the key string.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from fractions import Fraction
from typing import Literal, cast

AMOUNT_METHOD: tuple[str, int] = ("wet_end_top2", 1)
"""Name and version of the observed-amount rule (mean of the two largest)."""

RAIN_TRIGGER_HUNDREDTHS = 20
"""The one rain trigger: 0.2 mm, for the ranking, the vote and the chance."""

INCUMBENT_LOOKBACK_DATES = 60
"""How many earlier snapshot dates the remembered incumbent may reach back."""

TRIGGER_GRID_HUNDREDTHS: tuple[int, ...] = (10, 20, 30, 50, 100)
"""A15's trigger grid (a benchmark only, never served)."""

METEOBLUE_SOURCE = "meteoblue"
METEOBLUE_BLEND_MODEL = "multimodel"

EventOutcome = Literal["rain_nearby", "dry", "unknown"]
HourlyLine = tuple[float | None, ...] | Literal["unavailable"]


def hundredths(value_mm: float) -> int:
    """A millimetre value in hundredths, as the integer every rain line uses."""
    return round(value_mm * 100)


# --------------------------------------------------------------------------
# Model identity
# --------------------------------------------------------------------------


@dataclass(frozen=True, order=True)
class ModelRef:
    """One feed's model, ordered by ``(source, model)``."""

    source: str
    model: str

    @property
    def key(self) -> str:
        """The model key ``"<source>/<model>"``."""
        return f"{self.source}/{self.model}"

    @property
    def is_meteoblue_member(self) -> bool:
        """A meteoblue member model (not meteoblue's own blend)."""
        return self.source == METEOBLUE_SOURCE and self.model != METEOBLUE_BLEND_MODEL


# --------------------------------------------------------------------------
# Temperature helpers (§8.1, §8.2)
# --------------------------------------------------------------------------


def asymmetric_loss(
    forecast: float, observed: float, *, under_weight: float, over_weight: float
) -> float:
    """under_weight * max(y - F, 0) + over_weight * max(F - y, 0)."""
    return under_weight * max(observed - forecast, 0.0) + over_weight * max(
        forecast - observed, 0.0
    )


def trimean(values: Sequence[float]) -> float:
    """(Q1 + 2 * median + Q3) / 4 with inclusive quartiles."""
    q1, q2, q3 = statistics.quantiles(values, n=4, method="inclusive")
    return (q1 + 2 * q2 + q3) / 4


def shrunk_bias(errors: Sequence[float], *, shrink: float, limit: float) -> float:
    """n / (n + shrink) * trimean(errors), limited to +-limit.

    With no errors the shrink factor is 0, so the bias is 0.0.
    """
    n = len(errors)
    if n == 0:
        return 0.0
    raw = n / (n + shrink) * trimean(errors)
    return max(-limit, min(limit, raw))


def pooled_bias(
    per_lead: Mapping[int, Sequence[float]], *, shrink: float, limit: float
) -> float:
    """The pooled regime's bias (§8.2): per-lead shrunk trimeans, averaged.

    ``per_lead`` maps each lead to that lead's errors. Each lead with at least
    one error gives n / (n + shrink) * trimean(errors), shrunk by its own count
    and not limited on its own. The bias is the mean of those values, each
    lead counting once however many days it has, limited to +-limit. A lead
    with no errors is left out, not counted as 0. With no errors at any lead
    the bias is 0.0, as in ``shrunk_bias``.
    """
    lead_biases = [
        len(errors) / (len(errors) + shrink) * trimean(errors)
        for errors in per_lead.values()
        if errors
    ]
    if not lead_biases:
        return 0.0
    return max(-limit, min(limit, statistics.fmean(lead_biases)))


def _lead_mean(per_lead: Mapping[int, Sequence[float]]) -> float | None:
    """The mean of the per-lead means, each lead with at least one day."""
    means = [statistics.fmean(values) for values in per_lead.values() if values]
    if not means:
        return None
    return statistics.fmean(means)


def pooled_statistic(
    per_lead: Mapping[int, Sequence[float]], *, min_days: int
) -> float | None:
    """The mean with equal weight per lead, or None below ``min_days`` in total.

    Each lead with at least one day contributes its own mean once, so a lead
    with many days does not outweigh a lead with few.
    """
    total = sum(len(values) for values in per_lead.values())
    if total < min_days:
        return None
    return _lead_mean(per_lead)


def inverse_mae_weights[K](
    histories: Mapping[K, Mapping[int, Sequence[float]]], *, floor: float
) -> dict[K, float]:
    """u = 1 / max(MAE, floor) per entry, from signed errors per lead.

    The MAE has equal weight per lead (a single lead gives the plain mean).
    An entry with no errors gets no weight.
    """
    weights: dict[K, float] = {}
    for key, per_lead in histories.items():
        mae = _lead_mean(
            {lead: [abs(e) for e in errs] for lead, errs in per_lead.items()}
        )
        if mae is None:
            continue
        weights[key] = 1.0 / max(mae, floor)
    return weights


TieBreak = Literal["under_forecast", "source_model"]


@dataclass(frozen=True)
class RankedFeed:
    """One feed's mean training loss and the tie-break figures."""

    feed: ModelRef
    mean_loss: float
    mean_under: float
    mean_over: float
    days: int


def rank_by_loss(
    histories: Mapping[ModelRef, Mapping[int, Sequence[tuple[float, float]]]],
    *,
    under_weight: float,
    over_weight: float,
    tie_break: TieBreak,
) -> tuple[RankedFeed, ...]:
    """Rank feeds by mean training loss, lowest first.

    ``histories`` maps each feed to its (forecast, observed) pairs per lead;
    every mean has equal weight per lead. Ties: ``under_forecast`` breaks on
    the smaller mean under-forecast first; both then break on (source, model).
    Feeds with no pairs are left out.
    """
    ranked: list[RankedFeed] = []
    for feed, per_lead in histories.items():
        losses = {
            lead: [
                asymmetric_loss(
                    f, y, under_weight=under_weight, over_weight=over_weight
                )
                for f, y in pairs
            ]
            for lead, pairs in per_lead.items()
        }
        mean_loss = _lead_mean(losses)
        if mean_loss is None:
            continue
        under = _lead_mean(
            {
                lead: [max(y - f, 0.0) for f, y in pairs]
                for lead, pairs in per_lead.items()
            }
        )
        over = _lead_mean(
            {
                lead: [max(f - y, 0.0) for f, y in pairs]
                for lead, pairs in per_lead.items()
            }
        )
        ranked.append(
            RankedFeed(
                feed=feed,
                mean_loss=mean_loss,
                mean_under=under if under is not None else 0.0,
                mean_over=over if over is not None else 0.0,
                days=sum(len(p) for p in per_lead.values()),
            )
        )

    def order(entry: RankedFeed) -> tuple[float, float, str, str]:
        second = entry.mean_under if tie_break == "under_forecast" else 0.0
        return (entry.mean_loss, second, entry.feed.source, entry.feed.model)

    return tuple(sorted(ranked, key=order))


def top_k[T](ranked: Sequence[T], *, k: int) -> tuple[T, ...]:
    """The first ``k`` of a ranking, or all of it when it is shorter."""
    if k < 1:
        raise ValueError("k must be at least 1")
    return tuple(ranked[:k])


def weighted_value[K](
    values: Mapping[K, float], weights: Mapping[K, float]
) -> float | None:
    """sum(w * v) / sum(w) over the weighted keys, or None with no weight."""
    keys = [key for key in weights if key in values]
    total_weight = sum(weights[key] for key in keys)
    if not keys or total_weight <= 0:
        return None
    return sum(weights[key] * values[key] for key in keys) / total_weight


def training_days(
    recorded: Mapping[tuple[date, date], datetime],
    *,
    first: date,
    last: date,
    lead: int,
) -> list[tuple[date, datetime]]:
    """(d', T(d' - L)) for each d' in [first, last] with a recorded row.

    ``recorded`` maps (snapshot date, target date) to that row's
    ``snapshot_utc``. A day without its (d' - L, d') row is not returned:
    there is no fallback.
    """
    out: list[tuple[date, datetime]] = []
    day = first
    while day <= last:
        at = recorded.get((day - timedelta(days=lead), day))
        if at is not None:
            out.append((day, at))
        day += timedelta(days=1)
    return out


# --------------------------------------------------------------------------
# Temperature candidates (§7.4, §8.2) and benchmarks (§7.5)
# --------------------------------------------------------------------------

Regime = Literal["per_lead", "pooled", "no_eligible"]


@dataclass(frozen=True)
class FeedCase:
    """One feed at one case: its forecast for the target and its training.

    ``value`` is None when the feed does not cover the target. ``training``
    maps each lead to the feed's (forecast, observed) pairs on its counted
    training days at that lead.
    """

    feed: ModelRef
    value: float | None
    training: Mapping[int, Sequence[tuple[float, float]]]


@dataclass(frozen=True)
class CandidateValue:
    """One candidate's value at one case, with what produced it."""

    name: str
    value: float | None
    regime: Regime
    eligible: tuple[str, ...]
    weights: Mapping[str, float]
    biases: Mapping[str, float]
    ranks: tuple[str, ...]
    flags: tuple[str, ...]
    fallback_reason: str | None


def training_regime(
    feeds: Sequence[FeedCase], *, lead: int, min_days: int, pooled_leads: Sequence[int]
) -> tuple[Regime, tuple[FeedCase, ...]]:
    """The case's regime and its eligible feeds (§7.4).

    Per lead: some covering feed has ``min_days`` counted days at ``lead``;
    the eligible feeds are those. Pooled: no feed reaches it at ``lead``, and
    the eligible feeds are the covering ones with ``min_days`` in total over
    ``pooled_leads``. Otherwise ``no_eligible``.
    """
    covering = [f for f in feeds if f.value is not None]
    per_lead = tuple(f for f in covering if len(f.training.get(lead, ())) >= min_days)
    if per_lead:
        return "per_lead", per_lead
    pooled = tuple(
        f
        for f in covering
        if sum(len(f.training.get(ld, ())) for ld in pooled_leads) >= min_days
    )
    if pooled:
        return "pooled", pooled
    return "no_eligible", ()


def _regime_training(
    feed: FeedCase, *, regime: Regime, lead: int, pooled_leads: Sequence[int]
) -> dict[int, Sequence[tuple[float, float]]]:
    if regime == "per_lead":
        return {lead: feed.training.get(lead, ())}
    return {ld: feed.training.get(ld, ()) for ld in pooled_leads}


def _no_eligible(name: str, control: float) -> CandidateValue:
    return CandidateValue(
        name=name,
        value=control,
        regime="no_eligible",
        eligible=(),
        weights={},
        biases={},
        ranks=(),
        flags=("no_eligible",),
        fallback_reason="no_eligible",
    )


def compose_iw(
    feeds: Sequence[FeedCase],
    *,
    control: float,
    lead: int,
    min_days: int,
    pooled_leads: Sequence[int],
    floor: float,
) -> CandidateValue:
    """IW: weights 1 / max(MAE, floor) over the eligible feeds (§8.2)."""
    regime, eligible = training_regime(
        feeds, lead=lead, min_days=min_days, pooled_leads=pooled_leads
    )
    if regime == "no_eligible":
        return _no_eligible("IW", control)
    histories = {
        f.feed: {
            ld: [fc - y for fc, y in pairs]
            for ld, pairs in _regime_training(
                f, regime=regime, lead=lead, pooled_leads=pooled_leads
            ).items()
        }
        for f in eligible
    }
    weights = inverse_mae_weights(histories, floor=floor)
    values = {f.feed: f.value for f in eligible if f.value is not None}
    value = weighted_value(values, weights)
    return CandidateValue(
        name="IW",
        value=value,
        regime=regime,
        eligible=tuple(f.feed.key for f in eligible),
        weights={feed.key: w for feed, w in weights.items()},
        biases={},
        ranks=(),
        flags=(regime,),
        fallback_reason=None,
    )


def compose_td_k(
    feeds: Sequence[FeedCase],
    *,
    control: float,
    lead: int,
    min_days: int,
    pooled_leads: Sequence[int],
    k: int,
    under_weight: float,
    over_weight: float,
    tie_break: TieBreak,
) -> CandidateValue:
    """TD-K: the equal mean of the K best by mean training loss (§8.2)."""
    regime, eligible = training_regime(
        feeds, lead=lead, min_days=min_days, pooled_leads=pooled_leads
    )
    if regime == "no_eligible":
        return _no_eligible("TD-K", control)
    ranked = rank_by_loss(
        {
            f.feed: _regime_training(
                f, regime=regime, lead=lead, pooled_leads=pooled_leads
            )
            for f in eligible
        },
        under_weight=under_weight,
        over_weight=over_weight,
        tie_break=tie_break,
    )
    best = top_k(ranked, k=k)
    values = {f.feed: f.value for f in eligible if f.value is not None}
    chosen = [values[r.feed] for r in best if r.feed in values]
    return CandidateValue(
        name="TD-K",
        value=statistics.fmean(chosen) if chosen else None,
        regime=regime,
        eligible=tuple(f.feed.key for f in eligible),
        weights={r.feed.key: 1.0 / len(best) for r in best},
        biases={},
        ranks=tuple(r.feed.key for r in ranked),
        flags=(regime,),
        fallback_reason=None,
    )


def _feed_biases(
    eligible: Sequence[FeedCase],
    *,
    regime: Regime,
    lead: int,
    pooled_leads: Sequence[int],
    shrink: float,
    limit: float,
) -> dict[ModelRef, float]:
    """Each eligible feed's one b (§8.2).

    Per lead: ``shrunk_bias`` on its errors at the case's lead. Pooled:
    ``pooled_bias`` on its errors at each of ``pooled_leads``.
    """
    biases: dict[ModelRef, float] = {}
    for f in eligible:
        errors = {
            ld: [fc - y for fc, y in pairs]
            for ld, pairs in _regime_training(
                f, regime=regime, lead=lead, pooled_leads=pooled_leads
            ).items()
        }
        if regime == "pooled":
            biases[f.feed] = pooled_bias(errors, shrink=shrink, limit=limit)
        else:
            biases[f.feed] = shrunk_bias(errors[lead], shrink=shrink, limit=limit)
    return biases


def compose_iw_bc(
    feeds: Sequence[FeedCase],
    *,
    control: float,
    lead: int,
    min_days: int,
    pooled_leads: Sequence[int],
    floor: float,
    shrink: float,
    limit: float,
) -> CandidateValue:
    """IW-BC: IW on bias-corrected values, weights from mean |e - b| (§8.2).

    Each feed has one b: ``shrunk_bias`` at the case's lead in the per-lead
    regime, ``pooled_bias`` over ``pooled_leads`` in the pooled one. P' = P - b
    at every lead, and the mean |e - b| has equal weight per lead, as IW's
    MAE does.
    """
    regime, eligible = training_regime(
        feeds, lead=lead, min_days=min_days, pooled_leads=pooled_leads
    )
    if regime == "no_eligible":
        return _no_eligible("IW-BC", control)
    biases = _feed_biases(
        eligible,
        regime=regime,
        lead=lead,
        pooled_leads=pooled_leads,
        shrink=shrink,
        limit=limit,
    )
    histories = {
        f.feed: {
            ld: [fc - y - biases[f.feed] for fc, y in pairs]
            for ld, pairs in _regime_training(
                f, regime=regime, lead=lead, pooled_leads=pooled_leads
            ).items()
        }
        for f in eligible
    }
    weights = inverse_mae_weights(histories, floor=floor)
    values = {f.feed: f.value - biases[f.feed] for f in eligible if f.value is not None}
    return CandidateValue(
        name="IW-BC",
        value=weighted_value(values, weights),
        regime=regime,
        eligible=tuple(f.feed.key for f in eligible),
        weights={feed.key: w for feed, w in weights.items()},
        biases={feed.key: b for feed, b in biases.items()},
        ranks=(),
        flags=(regime,),
        fallback_reason=None,
    )


def compose_td_k_bc(
    feeds: Sequence[FeedCase],
    *,
    control: float,
    lead: int,
    min_days: int,
    pooled_leads: Sequence[int],
    k: int,
    under_weight: float,
    over_weight: float,
    tie_break: TieBreak,
    shrink: float,
    limit: float,
) -> CandidateValue:
    """TD-K-BC: TD-K ranked by the loss of P' = P - b, averaging P' (§8.2).

    Its b is IW-BC's in both regimes; in the pooled one the loss of P' is
    taken at every lead of ``pooled_leads``, with equal weight per lead.
    """
    regime, eligible = training_regime(
        feeds, lead=lead, min_days=min_days, pooled_leads=pooled_leads
    )
    if regime == "no_eligible":
        return _no_eligible("TD-K-BC", control)
    biases = _feed_biases(
        eligible,
        regime=regime,
        lead=lead,
        pooled_leads=pooled_leads,
        shrink=shrink,
        limit=limit,
    )
    ranked = rank_by_loss(
        {
            f.feed: {
                ld: [(fc - biases[f.feed], y) for fc, y in pairs]
                for ld, pairs in _regime_training(
                    f, regime=regime, lead=lead, pooled_leads=pooled_leads
                ).items()
            }
            for f in eligible
        },
        under_weight=under_weight,
        over_weight=over_weight,
        tie_break=tie_break,
    )
    best = top_k(ranked, k=k)
    values = {f.feed: f.value - biases[f.feed] for f in eligible if f.value is not None}
    chosen = [values[r.feed] for r in best if r.feed in values]
    return CandidateValue(
        name="TD-K-BC",
        value=statistics.fmean(chosen) if chosen else None,
        regime=regime,
        eligible=tuple(f.feed.key for f in eligible),
        weights={r.feed.key: 1.0 / len(best) for r in best},
        biases={feed.key: b for feed, b in biases.items()},
        ranks=tuple(r.feed.key for r in ranked),
        flags=(regime,),
        fallback_reason=None,
    )


@dataclass(frozen=True)
class BenchmarkValue:
    """One benchmark's value at one case, with its flags."""

    name: str
    value: float | None
    flags: tuple[str, ...]
    members: tuple[str, ...]


def compose_eq(values: Mapping[ModelRef, float], *, name: str = "EQ") -> BenchmarkValue:
    """EQ: the equal-weight mean of every covering feed's value (§7.5, §9.8)."""
    if not values:
        return BenchmarkValue(name=name, value=None, flags=("absent",), members=())
    ordered = sorted(values)
    return BenchmarkValue(
        name=name,
        value=statistics.fmean(values[m] for m in ordered),
        flags=(),
        members=tuple(m.key for m in ordered),
    )


def compose_mb_mean(values: Mapping[ModelRef, float]) -> BenchmarkValue:
    """MB-mean: the mean of the meteoblue member feeds that cover the day."""
    members = {m: v for m, v in values.items() if m.is_meteoblue_member}
    return compose_eq(members, name="MB-mean")


def compose_persistence(
    window_truth: Mapping[date, float], *, control: float | None
) -> BenchmarkValue:
    """Persistence: the last eligible truth in the window, else the control.

    ``window_truth`` holds only the window's eligible days. With none, the
    control's value is taken, flagged ``no_eligible``.
    """
    if not window_truth:
        return BenchmarkValue(
            name="persistence", value=control, flags=("no_eligible",), members=()
        )
    last = max(window_truth)
    return BenchmarkValue(
        name="persistence",
        value=window_truth[last],
        flags=(),
        members=(last.isoformat(),),
    )


# --------------------------------------------------------------------------
# The rain truth (§6.6, §6.7)
# --------------------------------------------------------------------------

_PRECIP_TOTAL_SUFFIX = " mm precipTotal"


@dataclass(frozen=True)
class RainReading:
    """One ``ok`` rain reading of one station: its hour, increment and raw."""

    hour: datetime
    value: float
    source_raw: str | None
    fetched_at: datetime | None


@dataclass(frozen=True)
class StationDayTotal:
    """One station's daily total and its measurement findings (§6.7)."""

    total: float
    hundredths: int
    hours: int
    expected_hours: int
    problems: tuple[str, ...]
    correction: str | None
    invalid: bool
    after_gap: int

    @property
    def reliable(self) -> bool:
        """Every expected hour read, no problem and no correction."""
        return (
            self.hours >= self.expected_hours
            and not self.problems
            and self.correction is None
        )


def _parse_running_total(source_raw: str | None) -> float | None:
    if source_raw is None or not source_raw.endswith(_PRECIP_TOTAL_SUFFIX):
        return None
    head = source_raw[: -len(_PRECIP_TOTAL_SUFFIX)].strip()
    try:
        value = float(head)
    except ValueError:
        return None
    if not math.isfinite(value):
        return None
    return value


def station_day_total(
    readings: Sequence[RainReading], *, expected_hours: int, gap_hours: int
) -> StationDayTotal:
    """One station's total for one local day, with its problems (§6.7).

    Readings are taken in hour order. Problems: ``counter_reset`` (the
    running total drops by more than 0.01 mm), ``unparsed_total``,
    ``unreconciled`` (the increments miss the last running total by more
    than 0.01 mm) and ``future_timestamp`` (an hour after its fetch). An
    ``unreconciled`` day whose running totals all parse and never go down
    takes the last running total (``from_running_total``) and stays valid;
    every other problem day is invalid. ``after_gap`` counts the readings
    that follow the previous reading by ``gap_hours`` or more. No reading is
    changed.
    """
    ordered = sorted(readings, key=lambda r: r.hour)
    hours = len({r.hour.replace(minute=0, second=0, microsecond=0) for r in ordered})
    total = sum(r.value for r in ordered)
    problems: list[str] = []
    running = [_parse_running_total(r.source_raw) for r in ordered]
    parsed = [v for v in running if v is not None]
    if len(parsed) != len(running):
        problems.append("unparsed_total")
    running_h = [hundredths(v) for v in parsed]
    never_down = all(b >= a for a, b in zip(running_h, running_h[1:], strict=False))
    if any(a - b > 1 for a, b in zip(running_h, running_h[1:], strict=False)):
        problems.append("counter_reset")
    if (
        parsed
        and len(parsed) == len(running)
        and abs(hundredths(total) - running_h[-1]) > 1
    ):
        problems.append("unreconciled")
    if any(r.fetched_at is not None and r.hour > r.fetched_at for r in ordered):
        problems.append("future_timestamp")
    correction: str | None = None
    if problems == ["unreconciled"] and never_down:
        correction = "from_running_total"
        total = parsed[-1]
    after_gap = sum(
        1
        for prev, cur in zip(ordered, ordered[1:], strict=False)
        if cur.hour - prev.hour >= timedelta(hours=gap_hours)
    )
    return StationDayTotal(
        total=total,
        hundredths=hundredths(total),
        hours=hours,
        expected_hours=expected_hours,
        problems=tuple(problems),
        correction=correction,
        invalid=bool(problems) and correction is None,
        after_gap=after_gap,
    )


@dataclass(frozen=True)
class StationDay:
    """One station on one day, as the day's event rules see it."""

    station_id: str
    hundredths: int
    hours: int
    invalid: bool
    quarantined: bool

    @property
    def counted(self) -> bool:
        """Validated, not invalid, with at least one ``ok`` reading."""
        return not self.quarantined and not self.invalid and self.hours >= 1


@dataclass(frozen=True)
class HighReadingFlags:
    """The day's ``large_total`` and ``lone_reading`` stations, if any."""

    large_total: str | None
    large_total_hundredths: tuple[int, int] | None
    lone_reading: str | None


def _contributing(
    stations: Sequence[StationDay], *, min_hours: int, wet: int
) -> list[StationDay]:
    out = [
        s
        for s in stations
        if s.counted and s.hours >= min_hours and s.hundredths >= wet
    ]
    return sorted(out, key=lambda s: (-s.hundredths, s.station_id))


def high_reading_flags(
    stations: Sequence[StationDay],
    *,
    wet_hundredths: int,
    large_factor: int,
    min_hours: int,
) -> HighReadingFlags:
    """Flags only, never a removal (§6.7).

    ``large_total``: with at least two contributing stations (validated,
    ``min_hours`` or more, at or above ``wet_hundredths``), the largest is
    more than ``large_factor`` times the second. ``lone_reading``: exactly
    one counted station at or above ``wet_hundredths``.
    """
    contributing = _contributing(stations, min_hours=min_hours, wet=wet_hundredths)
    large: str | None = None
    pair: tuple[int, int] | None = None
    if len(contributing) >= 2:
        h1, h2 = contributing[0].hundredths, contributing[1].hundredths
        if h1 > large_factor * h2:
            large = contributing[0].station_id
            pair = (h1, h2)
    wet = [s for s in stations if s.counted and s.hundredths >= wet_hundredths]
    lone = wet[0].station_id if len(wet) == 1 else None
    return HighReadingFlags(
        large_total=large, large_total_hundredths=pair, lone_reading=lone
    )


@dataclass(frozen=True)
class RainEvent:
    """One local day's rain-nearby outcome (§6.6)."""

    outcome: EventOutcome
    trigger: str | None
    adequate: tuple[str, ...]
    partial: tuple[str, ...]


def rain_nearby_event(
    stations: Sequence[StationDay],
    *,
    min_hours: int,
    pair_hundredths: int,
    single_hundredths: int,
    min_dry_stations: int,
) -> RainEvent:
    """Rain nearby, dry or unknown for one day (§6.6).

    Every counted station counts toward rain nearby, whatever its hours.
    Dry needs the rule unmet and ``min_dry_stations`` counted stations with
    ``min_hours`` or more; otherwise the day is unknown.
    """
    counted = sorted((s for s in stations if s.counted), key=lambda s: s.station_id)
    adequate = tuple(s.station_id for s in counted if s.hours >= min_hours)
    partial = tuple(s.station_id for s in counted if s.hours < min_hours)
    two = sum(1 for s in counted if s.hundredths >= pair_hundredths) >= 2
    one = any(s.hundredths >= single_hundredths for s in counted)
    if two or one:
        trigger = "both" if two and one else ("two_at_0.2" if two else "one_at_0.5")
        return RainEvent("rain_nearby", trigger, adequate, partial)
    if len(adequate) >= min_dry_stations:
        return RainEvent("dry", None, adequate, partial)
    return RainEvent("unknown", None, adequate, partial)


@dataclass(frozen=True)
class ObservedAmount:
    """The owner's observed amount for one day (§6.6)."""

    amount: float | None
    reason: str | None
    contributing: tuple[tuple[str, int, int], ...]
    partial: tuple[tuple[str, int, int], ...]
    method: tuple[str, int]


def observed_amount(
    stations: Sequence[StationDay],
    *,
    outcome: EventOutcome,
    min_hours: int,
    wet_hundredths: int,
) -> ObservedAmount:
    """(h1 + h2) / 200 mm, or h1 / 100 with one contributing station.

    Dry gives 0.0, unknown gives None, and a rain-nearby day with no
    contributing station gives None with ``amount_not_available``. The
    contributing and partial stations are (id, hundredths, hours).
    """
    contributing = _contributing(stations, min_hours=min_hours, wet=wet_hundredths)
    partial = tuple(
        (s.station_id, s.hundredths, s.hours)
        for s in sorted(stations, key=lambda s: s.station_id)
        if s.counted and s.hours < min_hours
    )
    contrib = tuple((s.station_id, s.hundredths, s.hours) for s in contributing)
    if outcome == "dry":
        return ObservedAmount(0.0, None, contrib, partial, AMOUNT_METHOD)
    if outcome == "unknown":
        return ObservedAmount(None, None, contrib, partial, AMOUNT_METHOD)
    if not contributing:
        return ObservedAmount(
            None, "amount_not_available", contrib, partial, AMOUNT_METHOD
        )
    if len(contributing) == 1:
        amount = contributing[0].hundredths / 100
    else:
        amount = (contributing[0].hundredths + contributing[1].hundredths) / 200
    return ObservedAmount(amount, None, contrib, partial, AMOUNT_METHOD)


def comparison_amount(
    contributing_hundredths: Sequence[int], *, rule: Literal["median", "p90"]
) -> float:
    """A16's other amount targets, never a truth: the median or p90 / 100."""
    if not contributing_hundredths:
        raise ValueError("no contributing totals")
    values = list(contributing_hundredths)
    if rule == "median":
        return statistics.median(values) / 100
    if len(values) == 1:
        return values[0] / 100
    return statistics.quantiles(values, n=10, method="inclusive")[8] / 100


@dataclass(frozen=True)
class PeerDay:
    """X's valid peers on one day: how many, and their median in hundredths."""

    peers: int
    median: Fraction


def rain_episodes(
    days: Mapping[date, PeerDay], *, min_peers: int, min_median_hundredths: int
) -> dict[date, int]:
    """Number the maximal runs of consecutive episode days (§6.7).

    An episode day has at least ``min_peers`` valid peers and a peer median
    at or above the line. A missing day, or a day short of either, ends the
    run. Episodes are numbered from 1 in date order.
    """
    out: dict[date, int] = {}
    number = 0
    previous: date | None = None
    for day in sorted(days):
        info = days[day]
        if info.peers < min_peers or info.median < min_median_hundredths:
            continue
        if previous is None or day - previous != timedelta(days=1):
            number += 1
        out[day] = number
        previous = day
    return out


@dataclass(frozen=True)
class QualifyingDay:
    """A qualifying day for one station: its ratio and the counts behind it."""

    ratio: Fraction
    peer_median: Fraction
    station_hundredths: int
    dry_while_wet: bool


@dataclass(frozen=True)
class FaultChange:
    """One change of fault state: date, rule and the counts behind it."""

    station_id: str
    day: date
    rule: str
    state: str
    qualifying_days: int
    episodes: int
    median_ratio: Fraction | None
    dry_days: int


@dataclass(frozen=True)
class FaultStates:
    """Every (station, day) state, the qualifying days and the change log."""

    states: Mapping[tuple[str, date], str]
    qualifying: Mapping[tuple[str, date], QualifyingDay]
    log: tuple[FaultChange, ...]


def _median_fraction(values: Sequence[Fraction]) -> Fraction:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def station_fault_states(
    history: Mapping[str, Mapping[date, StationDayTotal]],
    *,
    adequate_hours: int,
    wet_line_hundredths: int,
    dry_line_hundredths: int,
    min_peers: int,
    suspect_lookback_days: int,
    suspect_min_days: int,
    suspect_ratio_below: Fraction,
    quarantine_lookback_days: int,
    quarantine_min_days: int,
    quarantine_min_episodes: int,
    quarantine_ratio_at_most: Fraction,
    quarantine_dry_share: Fraction,
    recovery_lookback_days: int,
    recovery_min_days: int,
    recovery_min_episodes: int,
    recovery_ratio_at_least: Fraction,
    recovery_max_dry_days: int,
) -> FaultStates:
    """The §6.7 states of every station on every day from its first reading.

    Each day is decided only from earlier days, in date order: a quarantined
    station recovers by the recovery rule (counting qualifying days from the
    first quarantined day on); any other station is quarantined by the
    quarantine rule, else suspect by the suspect rule, else ok. A dry spell
    never clears a quarantine. Valid peers of X on d are the other stations
    with adequate coverage on d that are not quarantined on d.
    """
    if not history:
        return FaultStates(states={}, qualifying={}, log=())
    first_day = {sid: min(days) for sid, days in history.items() if days}
    all_days = [d for days in history.values() for d in days]
    start, end = min(all_days), max(all_days)
    states: dict[tuple[str, date], str] = {}
    qualifying: dict[tuple[str, date], QualifyingDay] = {}
    peer_days: dict[str, dict[date, PeerDay]] = {sid: {} for sid in history}
    quarantine_start: dict[str, date] = {}
    current: dict[str, str] = {}
    log: list[FaultChange] = []

    def window(
        sid: str, day: date, lookback: int, since: date | None
    ) -> list[tuple[date, QualifyingDay]]:
        lo = day - timedelta(days=lookback)
        if since is not None and since > lo:
            lo = since
        return [
            (d, qualifying[(sid, d)])
            for d in sorted(peer_days[sid])
            if lo <= d < day and (sid, d) in qualifying
        ]

    def counts(
        sid: str, day: date, rows: list[tuple[date, QualifyingDay]]
    ) -> tuple[int, int, Fraction | None, int]:
        episodes = rain_episodes(
            {d: p for d, p in peer_days[sid].items() if d < day},
            min_peers=min_peers,
            min_median_hundredths=wet_line_hundredths,
        )
        ids = {episodes[d] for d, _ in rows if d in episodes}
        ratio = _median_fraction([q.ratio for _, q in rows]) if rows else None
        dry = sum(1 for _, q in rows if q.dry_while_wet)
        return len(rows), len(ids), ratio, dry

    day = start
    while day <= end:
        active = sorted(sid for sid, first in first_day.items() if first <= day)
        for sid in active:
            previous = current.get(sid, "ok")
            state = previous
            rule = ""
            if previous == "quarantined":
                rows = window(sid, day, recovery_lookback_days, quarantine_start[sid])
                n, eps, ratio, dry = counts(sid, day, rows)
                if (
                    n >= recovery_min_days
                    and eps >= recovery_min_episodes
                    and ratio is not None
                    and ratio >= recovery_ratio_at_least
                    and dry <= recovery_max_dry_days
                ):
                    state, rule = "ok", "recovery"
            else:
                rows = window(sid, day, quarantine_lookback_days, None)
                n, eps, ratio, dry = counts(sid, day, rows)
                if (
                    n >= quarantine_min_days
                    and eps >= quarantine_min_episodes
                    and ratio is not None
                    and ratio <= quarantine_ratio_at_most
                    and Fraction(dry, n) >= quarantine_dry_share
                ):
                    state, rule = "quarantined", "quarantine"
                else:
                    rows = window(sid, day, suspect_lookback_days, None)
                    n, eps, ratio, dry = counts(sid, day, rows)
                    if (
                        n >= suspect_min_days
                        and ratio is not None
                        and ratio < suspect_ratio_below
                    ):
                        state, rule = "suspect", "suspect"
                    else:
                        state, rule = "ok", "suspect_cleared"
            if state != previous:
                if state == "quarantined":
                    quarantine_start[sid] = day
                log.append(
                    FaultChange(
                        station_id=sid,
                        day=day,
                        rule=rule,
                        state=state,
                        qualifying_days=n,
                        episodes=eps,
                        median_ratio=ratio,
                        dry_days=dry,
                    )
                )
            current[sid] = state
            states[(sid, day)] = state
        # The day's peers and qualifying days, from the states just decided.
        adequate = {
            sid: history[sid][day]
            for sid in active
            if day in history[sid]
            and history[sid][day].hours >= adequate_hours
            and not history[sid][day].invalid
            and states[(sid, day)] != "quarantined"
        }
        for sid in active:
            peers = [Fraction(t.hundredths) for p, t in adequate.items() if p != sid]
            median = _median_fraction(peers) if peers else Fraction(0)
            peer_days[sid][day] = PeerDay(peers=len(peers), median=median)
            own = history[sid].get(day)
            if own is None or not own.reliable:
                continue
            wet_peers = sum(1 for h in peers if h >= wet_line_hundredths)
            if wet_peers < min_peers or median < wet_line_hundredths:
                continue
            qualifying[(sid, day)] = QualifyingDay(
                ratio=Fraction(own.hundredths) / median,
                peer_median=median,
                station_hundredths=own.hundredths,
                dry_while_wet=own.hundredths < dry_line_hundredths,
            )
        day += timedelta(days=1)
    return FaultStates(states=states, qualifying=qualifying, log=tuple(log))


# --------------------------------------------------------------------------
# The selector (§9.1)
# --------------------------------------------------------------------------


def forecast_rain(total_mm: float, *, trigger_hundredths: int) -> bool:
    """A daily total forecasts rain when it reaches the trigger, in hundredths."""
    return hundredths(total_mm) >= trigger_hundredths


@dataclass(frozen=True)
class ModelWindow:
    """One model's lead-matched window at one (s, L).

    ``totals`` holds the model's daily total for every window day it covered
    exactly at the lead, as of that day's decision time; ``covers_target``
    says whether it covers the case's target exactly.
    """

    source: str
    model: str
    totals: Mapping[date, float]
    covers_target: bool

    @property
    def ref(self) -> ModelRef:
        """The model's ``(source, model)`` pair."""
        return ModelRef(self.source, self.model)


@dataclass(frozen=True)
class RainWindow:
    """The window of one (s, L): known events and each model's totals.

    ``events`` holds only the window's known days (True for rain nearby,
    False for dry); an unknown day is absent and never counted. ``models`` is
    keyed by model key and holds every model the loader returned.
    """

    events: Mapping[date, bool]
    models: Mapping[str, ModelWindow]


IncumbentOutcome = Literal[
    "kept", "replaced_margin", "replaced_no_forecast", "replaced_not_eligible"
]


@dataclass(frozen=True)
class RankedModel:
    """One ranked model's record on the common days."""

    key: str
    source: str
    model: str
    trigger_hundredths: int
    misses: int
    false_alarms: int
    penalty: Fraction
    common_days: int
    rain_nearby_days: int
    dry_days: int
    counted_days: int

    @property
    def score(self) -> Fraction:
        """The penalty per common day."""
        return self.penalty / self.common_days


@dataclass(frozen=True)
class Selection:
    """What :func:`select_model` decided at one (s, L)."""

    no_eligible: bool
    ranked: tuple[RankedModel, ...]
    best: str | None
    selected: str | None
    incumbent: str | None
    incumbent_outcome: IncumbentOutcome | None
    provisional: bool
    n_required: int
    known_days: int
    common_days: tuple[date, ...]
    models_returned: int
    not_covering: tuple[tuple[str, int], ...]
    not_eligible: tuple[tuple[str, int], ...]
    eligible: tuple[tuple[str, int], ...]
    dropped: tuple[tuple[str, int], ...]

    @property
    def known_not_common(self) -> int:
        """The window's known days that are not common days."""
        return self.known_days - len(self.common_days)

    def entry(self, key: str) -> RankedModel:
        """The ranked entry of ``key``."""
        for item in self.ranked:
            if item.key == key:
                return item
        raise KeyError(key)


def _counted_days(window: RainWindow, model: ModelWindow) -> list[date]:
    return sorted(d for d in model.totals if d in window.events)


def _score_at(
    window: RainWindow,
    model: ModelWindow,
    days: Sequence[date],
    *,
    trigger: int,
    miss_weight: Fraction,
) -> tuple[int, int, Fraction]:
    misses = 0
    false_alarms = 0
    for day in days:
        wet = forecast_rain(model.totals[day], trigger_hundredths=trigger)
        if window.events[day] and not wet:
            misses += 1
        elif not window.events[day] and wet:
            false_alarms += 1
    return misses, false_alarms, miss_weight * misses + false_alarms


def select_model(
    window: RainWindow,
    *,
    min_days: int,
    miss_weight: float,
    triggers_hundredths: Sequence[int],
    incumbent: str | None,
    switch_margin: int,
) -> Selection:
    """The selector's ranking and the selected model at one (s, L) (§9.1).

    Only models that cover the target exactly are counted. The minimum is on
    when two of them have ``min_days`` counted days (eligible: ``min_days``
    or more, N = ``min_days``); otherwise every model with a counted day is
    eligible, N is the largest count capped at ``min_days`` and the ranking
    is ``provisional``. The model with the fewest counted days (on a tie the
    last by ``(source, model)``) is dropped until N days are common or one
    model is left. Each model takes its lowest-penalty trigger (a tie goes
    to the lower one); the order is (score, misses, source, model). The
    incumbent stays unless the best-score total is ``switch_margin`` or more
    lower, and is replaced at once when it has no exact-cover forecast or is
    no longer ranked.
    """
    if not triggers_hundredths:
        raise ValueError("at least one trigger is required")
    weight = Fraction(miss_weight)
    returned = len(window.models)
    counted = {key: _counted_days(window, m) for key, m in window.models.items()}
    covering = {k: m for k, m in window.models.items() if m.covers_target}
    not_covering = tuple(
        (k, len(counted[k]))
        for k in sorted(window.models, key=lambda k: window.models[k].ref)
        if k not in covering
    )
    at_min = [k for k in covering if len(counted[k]) >= min_days]
    if len(at_min) >= 2:
        provisional = False
        n_required = min_days
        eligible = sorted(at_min, key=lambda k: covering[k].ref)
    else:
        provisional = True
        eligible = sorted(
            (k for k in covering if counted[k]), key=lambda k: covering[k].ref
        )
        n_required = min(min_days, max((len(counted[k]) for k in eligible), default=0))
    not_eligible = tuple(
        (k, len(counted[k]))
        for k in sorted(covering, key=lambda k: covering[k].ref)
        if k not in eligible
    )
    known = len(window.events)
    if not eligible:
        return Selection(
            no_eligible=True,
            ranked=(),
            best=None,
            selected=None,
            incumbent=incumbent,
            incumbent_outcome=None,
            provisional=provisional,
            n_required=n_required,
            known_days=known,
            common_days=(),
            models_returned=returned,
            not_covering=not_covering,
            not_eligible=not_eligible,
            eligible=(),
            dropped=(),
        )
    remaining = list(eligible)
    dropped: list[tuple[str, int]] = []

    def common_of(keys: Sequence[str]) -> list[date]:
        shared = set(counted[keys[0]])
        for key in keys[1:]:
            shared &= set(counted[key])
        return sorted(shared)

    common = common_of(remaining)
    while len(common) < n_required and len(remaining) > 1:
        fewest = min(len(counted[k]) for k in remaining)
        tied = [k for k in remaining if len(counted[k]) == fewest]
        victim = max(tied, key=lambda k: covering[k].ref)
        remaining.remove(victim)
        dropped.append((victim, fewest))
        common = common_of(remaining)
    ranked: list[RankedModel] = []
    for key in remaining:
        model = covering[key]
        best_at: tuple[Fraction, int, int, int] | None = None
        for trigger in sorted(triggers_hundredths):
            misses, false_alarms, penalty = _score_at(
                window, model, common, trigger=trigger, miss_weight=weight
            )
            if best_at is None or penalty < best_at[0]:
                best_at = (penalty, trigger, misses, false_alarms)
        assert best_at is not None
        penalty, trigger, misses, false_alarms = best_at
        wet_days = sum(1 for d in common if window.events[d])
        ranked.append(
            RankedModel(
                key=key,
                source=model.source,
                model=model.model,
                trigger_hundredths=trigger,
                misses=misses,
                false_alarms=false_alarms,
                penalty=penalty,
                common_days=len(common),
                rain_nearby_days=wet_days,
                dry_days=len(common) - wet_days,
                counted_days=len(counted[key]),
            )
        )
    ranked.sort(key=lambda r: (r.penalty, r.misses, r.source, r.model))
    best = ranked[0]
    selected = best.key
    outcome: IncumbentOutcome | None = None
    if incumbent is not None:
        current = window.models.get(incumbent)
        if current is None or not current.covers_target:
            outcome = "replaced_no_forecast"
        elif incumbent not in remaining:
            outcome = "replaced_not_eligible"
        else:
            held = next(r for r in ranked if r.key == incumbent)
            if held.penalty - best.penalty >= switch_margin:
                outcome = "replaced_margin"
            else:
                outcome = "kept"
                selected = incumbent
    return Selection(
        no_eligible=False,
        ranked=tuple(ranked),
        best=best.key,
        selected=selected,
        incumbent=incumbent,
        incumbent_outcome=outcome,
        provisional=provisional,
        n_required=n_required,
        known_days=known,
        common_days=tuple(common),
        models_returned=returned,
        not_covering=not_covering,
        not_eligible=not_eligible,
        eligible=tuple((k, len(counted[k])) for k in eligible),
        dropped=tuple(dropped),
    )


# --------------------------------------------------------------------------
# Memory through a gap (§9.1)
# --------------------------------------------------------------------------

DecisionKind = Literal["selected", "clear", "gap", "unrecognised"]
_GAP_REASONS = ("not_ready", "error")


@dataclass(frozen=True)
class RecordDecision:
    """What one record row decided at a lead: a model, a clear, a gap or not known."""

    kind: DecisionKind
    model: str | None = None


def record_decision(displayed: object) -> RecordDecision:
    """Sort one record row's parsed ``displayed`` (``None`` if it did not parse).

    A selected model: ``method`` ``selector``, no ``fallback_reason`` and a
    non-empty ``selected_feed``. A clear: ``selector`` with ``no_eligible``
    and no selected model, or ``method`` ``current``. A gap: ``selector``
    with ``not_ready`` or ``error`` and no selected model, or an object with
    no ``method`` key. Anything else is unrecognised.
    """
    if not isinstance(displayed, dict):
        return RecordDecision("unrecognised")
    row = cast("dict[str, object]", displayed)
    if "method" not in row:
        return RecordDecision("gap")
    method = row["method"]
    if method == "current":
        return RecordDecision("clear")
    if method != "selector":
        return RecordDecision("unrecognised")
    reason = row.get("fallback_reason")
    feed = row.get("selected_feed")
    if reason is None:
        if isinstance(feed, str) and feed:
            return RecordDecision("selected", feed)
        return RecordDecision("unrecognised")
    if feed is not None:
        return RecordDecision("unrecognised")
    if reason == "no_eligible":
        return RecordDecision("clear")
    if reason in _GAP_REASONS:
        return RecordDecision("gap")
    return RecordDecision("unrecognised")


@dataclass(frozen=True)
class RememberedIncumbent:
    """The incumbent found by the look-back, or why there is none."""

    model: str | None
    selected_on: date | None
    unrecognised_on: date | None


def remembered_incumbent(
    decisions: Mapping[date, RecordDecision], *, snapshot_date: date
) -> RememberedIncumbent:
    """The incumbent at one lead, read back through gaps (§9.1).

    Reads ``snapshot_date - INCUMBENT_LOOKBACK_DATES`` to ``snapshot_date -
    1``, latest first; a missing date is a gap. The first selected model is
    the incumbent; a clear ends the memory; an unrecognised row ends it and
    is returned with its date.
    """
    for offset in range(1, INCUMBENT_LOOKBACK_DATES + 1):
        day = snapshot_date - timedelta(days=offset)
        decision = decisions.get(day)
        if decision is None or decision.kind == "gap":
            continue
        if decision.kind == "selected":
            return RememberedIncumbent(decision.model, day, None)
        if decision.kind == "clear":
            return RememberedIncumbent(None, None, None)
        return RememberedIncumbent(None, None, day)
    return RememberedIncumbent(None, None, None)


# --------------------------------------------------------------------------
# A15's own-days ranking and the group (§9.2, §9.3, §9.4)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class OwnDaysEntry:
    """One model scored on its own counted days (A15 only)."""

    key: str
    source: str
    model: str
    misses: int
    false_alarms: int
    counted_days: int
    score: Fraction


def rank_own_days(
    window: RainWindow, *, min_days: int, miss_weight: float, trigger_hundredths: int
) -> tuple[OwnDaysEntry, ...]:
    """The backtest's own-days ranking (A15 only, never served).

    Every model that covers the target with ``min_days`` or more counted
    days, scored on its own counted days, with no common days and no drop
    rule; ordered by (score, misses, source, model), compared exactly.
    """
    weight = Fraction(miss_weight)
    out: list[OwnDaysEntry] = []
    for key, model in window.models.items():
        days = _counted_days(window, model)
        if not model.covers_target or len(days) < min_days or not days:
            continue
        misses, false_alarms, penalty = _score_at(
            window, model, days, trigger=trigger_hundredths, miss_weight=weight
        )
        out.append(
            OwnDaysEntry(
                key=key,
                source=model.source,
                model=model.model,
                misses=misses,
                false_alarms=false_alarms,
                counted_days=len(days),
                score=penalty / len(days),
            )
        )
    out.sort(key=lambda e: (e.score, e.misses, e.source, e.model))
    return tuple(out)


def top_two_rain(totals: Sequence[float], *, trigger_hundredths: int) -> bool:
    """S-TOP2's exact test: ``round(t1*100) + round(t2*100) >= 2 * trigger``."""
    if len(totals) != 2:
        raise ValueError("top_two_rain needs exactly two totals")
    return hundredths(totals[0]) + hundredths(totals[1]) >= 2 * trigger_hundredths


def group_size(j: int, *, group_fraction: Fraction) -> int:
    """q = ceil(group_fraction * J), computed exactly."""
    if j < 1:
        raise ValueError("the group needs at least one ranked model")
    return math.ceil(group_fraction * j)


def rain_group[T](ranked: Sequence[T], *, group_fraction: Fraction) -> tuple[T, ...]:
    """The first q of the score order, never re-sorted (§9.2)."""
    return tuple(ranked[: group_size(len(ranked), group_fraction=group_fraction)])


@dataclass(frozen=True)
class GroupVote:
    """CH-Q: the group's members, their votes and k / q."""

    j: int
    q: int
    members: tuple[str, ...]
    votes: tuple[tuple[str, float, bool], ...]
    k: int
    chance: Fraction


def group_vote_chance(
    ranked_totals: Sequence[tuple[str, float]],
    *,
    group_fraction: Fraction,
    trigger_hundredths: int,
) -> GroupVote:
    """CH-Q for J >= 1: the group cut by :func:`rain_group` and its vote share."""
    group = rain_group(ranked_totals, group_fraction=group_fraction)
    votes = tuple(
        (key, total, forecast_rain(total, trigger_hundredths=trigger_hundredths))
        for key, total in group
    )
    k = sum(1 for _, _, vote in votes if vote)
    return GroupVote(
        j=len(ranked_totals),
        q=len(group),
        members=tuple(key for key, _ in group),
        votes=votes,
        k=k,
        chance=Fraction(k, len(group)),
    )


@dataclass(frozen=True)
class GroupAmounts:
    """The group median and group mean (benchmarks only, §9.4)."""

    q: int
    members: tuple[str, ...]
    median: float
    mean: float


def group_amount_benchmarks(
    ranked_totals: Sequence[tuple[str, float]], *, group_fraction: Fraction
) -> GroupAmounts:
    """The median and the mean of the group's totals, 0 mm included."""
    group = rain_group(ranked_totals, group_fraction=group_fraction)
    totals = [total for _, total in group]
    return GroupAmounts(
        q=len(group),
        members=tuple(key for key, _ in group),
        median=statistics.median(totals),
        mean=statistics.fmean(totals),
    )


# --------------------------------------------------------------------------
# The served rain (§9.2, §9.4, §9.6)
# --------------------------------------------------------------------------


def selected_hourly(
    samples: Sequence[tuple[datetime, float]], *, hours: Sequence[datetime]
) -> HourlyLine:
    """One entry per local-day hour: the stored value, or None; never altered.

    With no samples at all the line is ``unavailable``. Samples off the
    day's hours are not read; two samples at one hour raise ValueError.
    """
    if not samples:
        return "unavailable"
    by_hour: dict[datetime, float] = {}
    for instant, value in samples:
        if instant in by_hour:
            raise ValueError(f"two samples at {instant.isoformat()}")
        by_hour[instant] = value
    return tuple(by_hour.get(hour) for hour in hours)


def counted_hours(values: HourlyLine, *, threshold_mm: float) -> int | None:
    """N h: hours with v > 0 and v >= threshold; None if any hour is missing."""
    if isinstance(values, str):
        return None
    present = [v for v in values if v is not None]
    if len(present) != len(values):
        return None
    return sum(1 for v in present if v > 0 and v >= threshold_mm)


@dataclass(frozen=True)
class ServedRain:
    """The served rain of one case, or ``no_eligible`` and nothing else."""

    no_eligible: bool
    selected: str | None = None
    best: str | None = None
    provisional: bool | None = None
    n_required: int | None = None
    group: GroupVote | None = None
    symbol: bool | None = None
    chance: Fraction | None = None
    amount: float | None = None
    hourly: HourlyLine | None = None
    hours_counted: int | None = None


def served_rain(
    selection: Selection,
    ranked_totals: Mapping[str, float],
    samples_by_model: Mapping[str, Sequence[tuple[datetime, float]]],
    *,
    hours: Sequence[datetime],
    group_fraction: Fraction,
    trigger_hundredths: int,
    miss_weight: float,
    threshold_mm: float,
) -> ServedRain:
    """S-Q, CH-Q and the selected model's amount, hourly line and N h.

    The group is cut from the score order, never around the selected model.
    The amount is the selected model's total, unchanged; with a missing hour
    or an ``unavailable`` line the amount and N h are None. The symbol and
    the amount are never made to agree.
    """
    if selection.no_eligible or selection.selected is None:
        return ServedRain(no_eligible=True)
    order = [(item.key, ranked_totals[item.key]) for item in selection.ranked]
    vote = group_vote_chance(
        order, group_fraction=group_fraction, trigger_hundredths=trigger_hundredths
    )
    line = selected_hourly(samples_by_model.get(selection.selected, ()), hours=hours)
    n_hours = counted_hours(line, threshold_mm=threshold_mm)
    amount = ranked_totals[selection.selected] if n_hours is not None else None
    return ServedRain(
        no_eligible=False,
        selected=selection.selected,
        best=selection.best,
        provisional=selection.provisional,
        n_required=selection.n_required,
        group=vote,
        symbol=glyph_from_votes(vote.k, vote.q, miss_weight=miss_weight),
        chance=vote.chance,
        amount=amount,
        hourly=line,
        hours_counted=n_hours,
    )


def amount_tradeoff(amount_mm: float, *, outcome: EventOutcome) -> str | None:
    """``dry_shown`` (dry, F >= 0.1 mm) or ``wet_short`` (rain nearby, F < 0.2 mm)."""
    value = hundredths(amount_mm)
    if outcome == "dry" and value >= 10:
        return "dry_shown"
    if outcome == "rain_nearby" and value < 20:
        return "wet_short"
    return None


@dataclass(frozen=True)
class ClimChance:
    """R-clim, or ``clim_short`` with fewer than the minimum known days."""

    chance: Fraction | None
    known_days: int
    rain_nearby_days: int
    fallback_reason: str | None


def climatology_chance(events: Mapping[date, bool], *, min_days: int) -> ClimChance:
    """The share of rain-nearby days among the window's known days (§9.2)."""
    known = len(events)
    wet = sum(1 for v in events.values() if v)
    if known < min_days or known == 0:
        return ClimChance(None, known, wet, "clim_short")
    return ClimChance(Fraction(wet, known), known, wet, None)


# --------------------------------------------------------------------------
# The vote benchmarks (§9.8)
# --------------------------------------------------------------------------


def wet_vote(values: Sequence[float], *, threshold_mm: float) -> bool:
    """A feed votes wet if any hour has v > 0 and v >= threshold (> 0)."""
    if threshold_mm <= 0:
        raise ValueError("the wet vote needs a threshold above 0")
    return any(v > 0 and v >= threshold_mm for v in values)


def vote_share(k: int, n: int) -> Fraction:
    """R0, the vote share k / n."""
    if n < 1:
        raise ValueError("no voters")
    return Fraction(k, n)


def vote_logit(k: int, n: int) -> float:
    """x = logit((k + 0.5) / (n + 1)), finite at k = 0 and k = n."""
    if n < 1 or not 0 <= k <= n:
        raise ValueError("need 0 <= k <= n and n >= 1")
    p = (k + 0.5) / (n + 1)
    return math.log(p / (1 - p))


def _sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def fit_calibration(
    xs: Sequence[float],
    ys: Sequence[int],
    *,
    lam: float,
    max_iterations: int,
    tolerance: float,
) -> tuple[float, float, bool]:
    """Penalised logistic fit of y on x by Newton's method from (0, 1).

    Minimises the log loss plus ``lam * (a**2 + (b - 1)**2)``; converged when
    both steps of one iteration are below ``tolerance``.
    """
    if len(xs) != len(ys):
        raise ValueError("xs and ys differ in length")
    a, b = 0.0, 1.0
    for _ in range(max_iterations):
        g_a = 2 * lam * a
        g_b = 2 * lam * (b - 1)
        h_aa = 2 * lam
        h_ab = 0.0
        h_bb = 2 * lam
        for x, y in zip(xs, ys, strict=True):
            p = _sigmoid(a + b * x)
            w = p * (1 - p)
            g_a += p - y
            g_b += (p - y) * x
            h_aa += w
            h_ab += w * x
            h_bb += w * x * x
        det = h_aa * h_bb - h_ab * h_ab
        if det == 0:
            return a, b, False
        step_a = -(h_bb * g_a - h_ab * g_b) / det
        step_b = -(h_aa * g_b - h_ab * g_a) / det
        a += step_a
        b += step_b
        if abs(step_a) < tolerance and abs(step_b) < tolerance:
            return a, b, True
    return a, b, False


def calibrated_chance(x: float, *, a: float, b: float) -> float:
    """R1 = sigmoid(a + b * x)."""
    return _sigmoid(a + b * x)


def glyph_from_votes(k: int, n: int, *, miss_weight: float) -> bool:
    """The exact vote line k * (1 + w_r) >= n, that is k / n >= 1 / (1 + w_r)."""
    if n < 1:
        raise ValueError("no voters")
    return k * (1 + Fraction(miss_weight)) >= n


def glyph_from_chance(p: float, *, miss_weight: float) -> bool:
    """G-R1's float test p >= 1 / (1 + w_r)."""
    return p >= 1.0 / (1.0 + miss_weight)


def qualifying_sum(values: Sequence[float], *, threshold_mm: float) -> float:
    """The sum of the hours with v > 0 and v >= threshold."""
    return sum(v for v in values if v > 0 and v >= threshold_mm)


# --------------------------------------------------------------------------
# Rain composers: benchmarks (§9.1, §9.2, §9.3, §9.8)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class FeedDay:
    """One covering feed's hourly values for the target day (hour order)."""

    feed_id: int
    ref: ModelRef
    values: tuple[float, ...]

    @property
    def total(self) -> float:
        """The all-hours daily total."""
        return sum(self.values)


@dataclass(frozen=True)
class SymbolValue:
    """One symbol or chance benchmark at one case."""

    name: str
    symbol: bool | None
    chance: float | None = None
    k: int | None = None
    n: int | None = None
    members: tuple[str, ...] = ()
    fit: tuple[float, float] | None = None
    fallback_reason: str | None = None
    flags: tuple[str, ...] = ()


def compose_fixed_symbol(*, show: bool) -> SymbolValue:
    """The always-show and never-show benchmarks."""
    return SymbolValue(name="always" if show else "never", symbol=show)


def compose_symbol_persistence(
    window_events: Mapping[date, bool], *, control: bool | None
) -> SymbolValue:
    """Persistence: the last known event in the window, else the control."""
    if not window_events:
        return SymbolValue(name="persistence", symbol=control, flags=("no_eligible",))
    last = max(window_events)
    return SymbolValue(
        name="persistence", symbol=window_events[last], members=(last.isoformat(),)
    )


def compose_s_pick(selection: Selection, totals: Mapping[str, float]) -> SymbolValue:
    """S-PICK: the selected model's own symbol at the trigger it was ranked at."""
    if selection.no_eligible or selection.selected is None:
        return SymbolValue(name="S-PICK", symbol=None, flags=("no_eligible",))
    entry = selection.entry(selection.selected)
    return SymbolValue(
        name="S-PICK",
        symbol=forecast_rain(
            totals[entry.key], trigger_hundredths=entry.trigger_hundredths
        ),
        members=(entry.key,),
    )


def compose_s_top2(
    selection: Selection, totals: Mapping[str, float], *, trigger_hundredths: int
) -> SymbolValue:
    """S-TOP2: the two best in score order; with one model, S-PICK (``one_model``)."""
    if selection.no_eligible:
        return SymbolValue(name="S-TOP2", symbol=None, flags=("no_eligible",))
    if len(selection.ranked) == 1:
        pick = compose_s_pick(selection, totals)
        return SymbolValue(
            name="S-TOP2",
            symbol=pick.symbol,
            members=pick.members,
            flags=("one_model",),
        )
    first, second = selection.ranked[0].key, selection.ranked[1].key
    return SymbolValue(
        name="S-TOP2",
        symbol=top_two_rain(
            (totals[first], totals[second]), trigger_hundredths=trigger_hundredths
        ),
        members=(first, second),
    )


@dataclass(frozen=True)
class GridRanking:
    """One A15 trigger-grid ranking with its best-score model and S-PICK."""

    label: str
    selection: Selection
    s_pick: SymbolValue


def compose_trigger_grid(
    window: RainWindow,
    totals: Mapping[str, float],
    *,
    min_days: int,
    miss_weight: float,
    grid_hundredths: Sequence[int],
    incumbent: str | None,
    switch_margin: int,
) -> tuple[GridRanking, ...]:
    """A15's grid: each model at its own lowest-scoring value, then each fixed value."""
    runs: list[tuple[str, tuple[int, ...]]] = [("own_lowest", tuple(grid_hundredths))]
    runs.extend((f"fixed_{t}", (t,)) for t in grid_hundredths)
    out: list[GridRanking] = []
    for label, triggers in runs:
        selection = select_model(
            window,
            min_days=min_days,
            miss_weight=miss_weight,
            triggers_hundredths=triggers,
            incumbent=incumbent,
            switch_margin=switch_margin,
        )
        out.append(GridRanking(label, selection, compose_s_pick(selection, totals)))
    return tuple(out)


@dataclass(frozen=True)
class OwnDaysComparison:
    """A15's own-days ranking set beside the served ranking."""

    ranking: tuple[OwnDaysEntry, ...]
    best: str | None
    same_best: bool | None
    group: tuple[str, ...]
    served_group: tuple[str, ...]
    shared_members: int


def compose_own_days(
    window: RainWindow,
    served: Selection,
    *,
    min_days: int,
    miss_weight: float,
    trigger_hundredths: int,
    group_fraction: Fraction,
) -> OwnDaysComparison:
    """The own-days ranking, its best model and group, against the served ones."""
    ranking = rank_own_days(
        window,
        min_days=min_days,
        miss_weight=miss_weight,
        trigger_hundredths=trigger_hundredths,
    )
    own_group: tuple[str, ...] = ()
    if ranking:
        own_group = tuple(
            e.key for e in rain_group(ranking, group_fraction=group_fraction)
        )
    served_group: tuple[str, ...] = ()
    if served.ranked:
        served_group = tuple(
            r.key for r in rain_group(served.ranked, group_fraction=group_fraction)
        )
    best = ranking[0].key if ranking else None
    same = None if best is None or served.best is None else best == served.best
    return OwnDaysComparison(
        ranking=ranking,
        best=best,
        same_best=same,
        group=own_group,
        served_group=served_group,
        shared_members=len(set(own_group) & set(served_group)),
    )


def cast_votes(
    feeds: Sequence[FeedDay], *, threshold_mm: float
) -> tuple[tuple[FeedDay, bool], ...] | None:
    """Each covering feed's wet vote; None at a threshold of 0 (``threshold_zero``)."""
    if threshold_mm <= 0:
        return None
    return tuple((f, wet_vote(f.values, threshold_mm=threshold_mm)) for f in feeds)


def _vote_counts(
    votes: Sequence[tuple[FeedDay, bool]],
) -> tuple[int, int, tuple[str, ...]]:
    members = tuple(sorted(f.ref.key for f, _ in votes))
    return sum(1 for _, v in votes if v), len(votes), members


def compose_r0(
    feeds: Sequence[FeedDay], *, threshold_mm: float, name: str = "R0"
) -> SymbolValue:
    """R0, the vote share of every covering feed (``threshold_zero``, ``no_voters``)."""
    votes = cast_votes(feeds, threshold_mm=threshold_mm)
    if votes is None:
        return SymbolValue(name=name, symbol=None, fallback_reason="threshold_zero")
    k, n, members = _vote_counts(votes)
    if n == 0:
        return SymbolValue(name=name, symbol=None, n=0, fallback_reason="no_voters")
    return SymbolValue(
        name=name,
        symbol=None,
        chance=float(vote_share(k, n)),
        k=k,
        n=n,
        members=members,
    )


def compose_mb_r0(feeds: Sequence[FeedDay], *, threshold_mm: float) -> SymbolValue:
    """MB-R0, the vote share of the meteoblue members only."""
    members = [f for f in feeds if f.ref.is_meteoblue_member]
    return compose_r0(members, threshold_mm=threshold_mm, name="MB-R0")


def compose_g_r0(
    feeds: Sequence[FeedDay], *, threshold_mm: float, miss_weight: float
) -> SymbolValue:
    """G-R0: every covering feed votes; the symbol when k * (1 + w_r) >= n."""
    r0 = compose_r0(feeds, threshold_mm=threshold_mm, name="G-R0")
    if r0.k is None or r0.n is None or r0.n == 0:
        return r0
    return SymbolValue(
        name="G-R0",
        symbol=glyph_from_votes(r0.k, r0.n, miss_weight=miss_weight),
        chance=r0.chance,
        k=r0.k,
        n=r0.n,
        members=r0.members,
    )


def compose_g_es(
    feeds: Sequence[FeedDay],
    extrema_feed_ids: Sequence[int],
    *,
    threshold_mm: float,
    miss_weight: float,
) -> SymbolValue:
    """G-ES: the same vote line over the control row's extrema feeds."""
    wanted = set(extrema_feed_ids)
    members = [f for f in feeds if f.feed_id in wanted]
    r0 = compose_r0(members, threshold_mm=threshold_mm, name="G-ES")
    if r0.k is None or r0.n is None or r0.n == 0:
        return r0
    return SymbolValue(
        name="G-ES",
        symbol=glyph_from_votes(r0.k, r0.n, miss_weight=miss_weight),
        chance=r0.chance,
        k=r0.k,
        n=r0.n,
        members=r0.members,
    )


@dataclass(frozen=True)
class VoteDay:
    """One window day's vote at the lead and its known event, for R1's fit."""

    k: int
    n: int
    rain_nearby: bool


def compose_r1(
    feeds: Sequence[FeedDay],
    training: Sequence[VoteDay],
    *,
    threshold_mm: float,
    lam: float,
    max_iterations: int,
    tolerance: float,
    min_days: int,
    min_each: int,
) -> SymbolValue:
    """R1, the calibrated vote share, falling back to R0 with a reason.

    Falls back when the window has fewer than ``min_days`` known days, fewer
    than ``min_each`` rain-nearby or dry days, or the fit does not converge.
    """
    r0 = compose_r0(feeds, threshold_mm=threshold_mm, name="R1")
    if r0.k is None or r0.n is None or r0.n == 0:
        return r0
    days = [d for d in training if d.n >= 1]
    wet = sum(1 for d in days if d.rain_nearby)
    reason: str | None = None
    if len(days) < min_days:
        reason = "short_window"
    elif wet < min_each:
        reason = "few_rain_nearby"
    elif len(days) - wet < min_each:
        reason = "few_dry"
    fit: tuple[float, float] | None = None
    if reason is None:
        a, b, converged = fit_calibration(
            [vote_logit(d.k, d.n) for d in days],
            [1 if d.rain_nearby else 0 for d in days],
            lam=lam,
            max_iterations=max_iterations,
            tolerance=tolerance,
        )
        fit = (a, b)
        if converged:
            return SymbolValue(
                name="R1",
                symbol=None,
                chance=calibrated_chance(vote_logit(r0.k, r0.n), a=a, b=b),
                k=r0.k,
                n=r0.n,
                members=r0.members,
                fit=fit,
            )
        reason = "not_converged"
    return SymbolValue(
        name="R1",
        symbol=None,
        chance=r0.chance,
        k=r0.k,
        n=r0.n,
        members=r0.members,
        fit=fit,
        fallback_reason=reason,
    )


def compose_g_r1(r1: SymbolValue, *, miss_weight: float) -> SymbolValue:
    """G-R1: the symbol when R1 (or its R0 fallback) >= 1 / (1 + w_r)."""
    if r1.chance is None:
        return SymbolValue(name="G-R1", symbol=None, fallback_reason=r1.fallback_reason)
    return SymbolValue(
        name="G-R1",
        symbol=glyph_from_chance(r1.chance, miss_weight=miss_weight),
        chance=r1.chance,
        k=r1.k,
        n=r1.n,
        members=r1.members,
        fit=r1.fit,
        fallback_reason=r1.fallback_reason,
    )


def compose_m_c(feeds: Sequence[FeedDay], *, threshold_mm: float) -> BenchmarkValue:
    """M-C: the mean counted-hour sum of the wet-voting feeds; 0 if none votes wet."""
    votes = cast_votes(feeds, threshold_mm=threshold_mm)
    if votes is None:
        return BenchmarkValue("M-C", None, ("threshold_zero",), ())
    wet = sorted((f for f, v in votes if v), key=lambda f: f.ref)
    if not wet:
        return BenchmarkValue("M-C", 0.0, (), ())
    return BenchmarkValue(
        "M-C",
        statistics.fmean(
            qualifying_sum(f.values, threshold_mm=threshold_mm) for f in wet
        ),
        (),
        tuple(f.ref.key for f in wet),
    )


def compose_m_ciw(
    feeds: Sequence[FeedDay],
    history: Mapping[int, Sequence[tuple[float, float]]],
    *,
    threshold_mm: float,
    min_days: int,
    floor: float,
) -> BenchmarkValue:
    """M-CIW: the wet voters weighted by 1 / max(MAE_f, floor).

    ``history`` maps a feed id to (counted-hour sum, observed amount) on its
    rain-nearby training days at the lead. A feed needs ``min_days`` of
    them; with none left, M-C is used, flagged ``short_wet_history``.
    """
    votes = cast_votes(feeds, threshold_mm=threshold_mm)
    if votes is None:
        return BenchmarkValue("M-CIW", None, ("threshold_zero",), ())
    wet = sorted((f for f, v in votes if v), key=lambda f: f.ref)
    weights: dict[ModelRef, float] = {}
    values: dict[ModelRef, float] = {}
    for feed in wet:
        pairs = history.get(feed.feed_id, ())
        if len(pairs) < min_days:
            continue
        mae = statistics.fmean(abs(f - y) for f, y in pairs)
        weights[feed.ref] = 1.0 / max(mae, floor)
        values[feed.ref] = qualifying_sum(feed.values, threshold_mm=threshold_mm)
    if not weights:
        fallback = compose_m_c(feeds, threshold_mm=threshold_mm)
        return BenchmarkValue(
            "M-CIW", fallback.value, ("short_wet_history",), fallback.members
        )
    return BenchmarkValue(
        "M-CIW",
        weighted_value(values, weights),
        (),
        tuple(m.key for m in sorted(weights)),
    )
