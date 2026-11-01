"""The rain check: whether a station's gauge reads zero in rain.

Plan ``2026-11-01-rain-release.md``, Part A (A3). ``judge_day`` judges one
station's day against its peers, and ``fold`` decides from the station's
earlier check rows whether the day sets the station aside, takes it back,
or changes nothing. It is pure: no database, no clock, no network. It
imports only the standard library.

Every constant is a starting value and is not validated. Rain is compared
in whole hundredths of a millimetre, so a sum such as 0.19999999999999996
counts as 0.2, except the ``zero`` outcome, which needs a day total of
exactly 0.0.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final, Literal

Outcome = Literal["zero", "agree", "disagree", "dry", "not_widespread", "cant_judge"]
Reason = Literal[
    "too_few_stations",
    "wet_hours_unknown",
    "last_hour",
    "gap_filled",
    "cold",
    "target_missing",
]
Decision = Literal["set_aside", "taken_back", "ambiguous", "site_limit"]

OUTCOMES: Final[tuple[Outcome, ...]] = (
    "zero",
    "agree",
    "disagree",
    "dry",
    "not_widespread",
    "cant_judge",
)
DECISIONS: Final[tuple[Decision, ...]] = (
    "set_aside",
    "taken_back",
    "ambiguous",
    "site_limit",
)

#: Rule 1: the fewest valid peers a day is judged with.
MIN_VALID_PEERS: Final = 3
#: Rule 2: a peer median below this is a ``dry`` day.
DRY_MEDIAN_MM: Final = 0.2
#: Rule 3: a peer median below this is ``not_widespread``.
WIDESPREAD_MEDIAN_MM: Final = 2.0
#: Rule 3: a peer below this is a low peer; more than ``MAX_LOW_PEERS`` of
#: them is ``not_widespread``.
LOW_PEER_MM: Final = 1.0
MAX_LOW_PEERS: Final = 1
#: Rule 4: an hour is wet when at least ``WET_HOUR_PEERS`` valid peers rose
#: by at least ``WET_HOUR_RISE_MM`` in it.
WET_HOUR_RISE_MM: Final = 0.01
WET_HOUR_PEERS: Final = 2
#: Rule 7: a wet hour is cold below this temperature, or with fewer than
#: ``MIN_PEER_TEMPS`` peer temperatures.
WARM_C: Final = 3.0
MIN_PEER_TEMPS: Final = 2
#: Rule 7: the site temperatures read before the first wet hour, the frost
#: line among them, and the most of them that may be missing.
BEFORE_HOURS: Final = 48
FROST_C: Final = 1.0
MAX_MISSING_BEFORE: Final = 4
#: Rule 9: ``agree`` when the station's total is within this factor of M.
AGREE_FACTOR: Final = 2
#: Set aside: this many ``zero`` days in this many wet spells, within the
#: last ``SET_ASIDE_WINDOW_DAYS`` days.
SET_ASIDE_DAYS: Final = 3
SET_ASIDE_SPELLS: Final = 2
SET_ASIDE_WINDOW_DAYS: Final = 30
#: Take back: this many ``agree`` days in this many wet spells; no window.
TAKE_BACK_DAYS: Final = 3
TAKE_BACK_SPELLS: Final = 2

_ONE_HOUR: Final = timedelta(hours=1)


@dataclass(frozen=True)
class StationDayReadings:
    """One station's hourly values for the judged day.

    ``precip_mm`` is each hour's own rain; ``temperature_c`` each hour's
    temperature. A missing key, or a None temperature, means the hour has
    no value. ``gap_hours`` are the hours filled from the hourly download.
    """

    precip_mm: Mapping[datetime, float]
    temperature_c: Mapping[datetime, float | None]
    gap_hours: frozenset[datetime]


@dataclass(frozen=True)
class JudgeResult:
    """``judge_day``'s result.

    ``reason`` is set only for ``cant_judge``. ``station_mm`` is the
    station's day total when it has rain in every hour, else None;
    ``peer_median_mm`` the median of the valid peers' totals, None with no
    valid peer; ``peers`` the number of valid peers.
    """

    outcome: Outcome
    reason: Reason | None
    station_mm: float | None
    peer_median_mm: float | None
    peers: int


@dataclass(frozen=True)
class RainCheckRow:
    """One earlier check row of the station: its date, outcome and decision."""

    local_date: date
    outcome: Outcome
    decision: Decision | None = None


@dataclass(frozen=True)
class FoldResult:
    """``fold``'s result: the day's decision and the state after it (0 or 1)."""

    decision: Decision | None
    counted_after: int


def _hundredths(value: float) -> int:
    return round(value * 100)


def _complete(readings: StationDayReadings, hours: Sequence[datetime]) -> bool:
    return all(hour in readings.precip_mm for hour in hours)


def _total(readings: StationDayReadings, hours: Sequence[datetime]) -> float:
    return math.fsum(readings.precip_mm[hour] for hour in hours)


def _cold(
    wet: Sequence[datetime],
    target: StationDayReadings,
    peers: Sequence[StationDayReadings],
    site_temps_before: Mapping[datetime, float | None],
) -> bool:
    """Rule 7: a cold or unknown temperature in a wet hour or before them."""
    for hour in wet:
        temps = [
            temp for peer in peers if (temp := peer.temperature_c.get(hour)) is not None
        ]
        if len(temps) < MIN_PEER_TEMPS or statistics.median(temps) < WARM_C:
            return True
        own = target.temperature_c.get(hour)
        if own is None or own < WARM_C:
            return True
    first = wet[0]
    missing = 0
    for step in range(1, BEFORE_HOURS + 1):
        temp = site_temps_before.get(first - step * _ONE_HOUR)
        if temp is None:
            missing += 1
        elif temp < FROST_C:
            return True
    return missing > MAX_MISSING_BEFORE


def judge_day(
    hours: Sequence[datetime],
    target: StationDayReadings,
    peers: Sequence[StationDayReadings],
    *,
    site_temps_before: Mapping[datetime, float | None],
) -> JudgeResult:
    """Judge one station's day against its peers (plan A3, rules 1-9).

    ``hours`` are the day's hours in order; ``peers`` the day's other listed
    stations that are counted; ``site_temps_before`` the site's hourly
    temperature for at least the 48 hours before the day's first wet hour,
    a missing key or None being a missing hour. The first rule that applies
    decides. A peer is valid when it has rain in every hour.
    """
    valid = [peer for peer in peers if _complete(peer, hours)]
    totals = [_total(peer, hours) for peer in valid]
    median = statistics.median(totals) if totals else None
    station_mm = _total(target, hours) if _complete(target, hours) else None

    def result(outcome: Outcome, reason: Reason | None = None) -> JudgeResult:
        return JudgeResult(
            outcome=outcome,
            reason=reason,
            station_mm=station_mm,
            peer_median_mm=median,
            peers=len(valid),
        )

    if median is None or len(valid) < MIN_VALID_PEERS:
        return result("cant_judge", "too_few_stations")
    median_h = _hundredths(median)
    if median_h < _hundredths(DRY_MEDIAN_MM):
        return result("dry")
    low_peers = sum(
        1 for total in totals if _hundredths(total) < _hundredths(LOW_PEER_MM)
    )
    if median_h < _hundredths(WIDESPREAD_MEDIAN_MM) or low_peers > MAX_LOW_PEERS:
        return result("not_widespread")
    rise_h = _hundredths(WET_HOUR_RISE_MM)
    wet = [
        hour
        for hour in hours
        if sum(1 for peer in valid if _hundredths(peer.precip_mm[hour]) >= rise_h)
        >= WET_HOUR_PEERS
    ]
    if not wet:
        return result("cant_judge", "wet_hours_unknown")
    if hours[-1] in wet:
        return result("cant_judge", "last_hour")
    if any(
        hour in target.gap_hours or any(hour in peer.gap_hours for peer in valid)
        for hour in wet
    ):
        return result("cant_judge", "gap_filled")
    if _cold(wet, target, peers, site_temps_before):
        return result("cant_judge", "cold")
    if station_mm is None:
        return result("cant_judge", "target_missing")
    if station_mm == 0.0:
        return result("zero")
    station_h = _hundredths(station_mm)
    if median_h <= AGREE_FACTOR * station_h and station_h <= AGREE_FACTOR * median_h:
        return result("agree")
    return result("disagree")


def _days_and_spells(rows: Iterable[RainCheckRow], outcome: Outcome) -> tuple[int, int]:
    """How many rows have ``outcome``, and in how many wet spells.

    A ``dry`` row between two rows puts them in different spells; every
    other outcome neither counts nor separates.
    """
    spell = 0
    days = 0
    spells: set[int] = set()
    for row in rows:
        if row.outcome == "dry":
            spell += 1
        elif row.outcome == outcome:
            days += 1
            spells.add(spell)
    return days, len(spells)


def _set_aside_due(history: Sequence[RainCheckRow], local_date: date) -> bool:
    """Whether a ``zero`` on ``local_date`` completes the set-aside count."""
    after = local_date - timedelta(days=SET_ASIDE_WINDOW_DAYS)
    for row in history:
        if row.outcome in ("agree", "disagree") or row.decision == "taken_back":
            after = max(after, row.local_date)
    rows = [row for row in history if after < row.local_date < local_date]
    rows.append(RainCheckRow(local_date=local_date, outcome="zero"))
    days, spells = _days_and_spells(rows, "zero")
    return days >= SET_ASIDE_DAYS and spells >= SET_ASIDE_SPELLS


def _take_back_due(history: Sequence[RainCheckRow], local_date: date) -> bool:
    """Whether an ``agree`` on ``local_date`` completes the take-back count."""
    after = date.min
    for row in history:
        if row.decision == "set_aside" or row.outcome in ("zero", "disagree"):
            after = max(after, row.local_date)
    rows = [row for row in history if after < row.local_date < local_date]
    rows.append(RainCheckRow(local_date=local_date, outcome="agree"))
    days, spells = _days_and_spells(rows, "agree")
    return days >= TAKE_BACK_DAYS and spells >= TAKE_BACK_SPELLS


def fold(
    history: Sequence[RainCheckRow],
    local_date: date,
    outcome: Outcome,
    *,
    counted_before: int,
    other_new_candidates_today: int = 0,
    other_station_set_aside_today: bool = False,
) -> FoldResult:
    """Whether ``local_date``'s outcome changes the station's state (plan A3).

    ``history`` is the station's earlier rows, in date order; rows dated on
    or after ``local_date`` are ignored. ``counted_before`` is 1 when the
    station is counted going into the day, else 0. A counted station whose
    ``zero`` completes the count is set aside, unless another listed
    station also qualifies that day (``ambiguous``) or another listed
    station is already set aside on the day (``site_limit``). A set-aside
    station whose ``agree`` completes the count is taken back; nothing
    blocks a take-back.
    """
    if counted_before:
        if outcome == "zero" and _set_aside_due(history, local_date):
            if other_new_candidates_today > 0:
                return FoldResult(decision="ambiguous", counted_after=1)
            if other_station_set_aside_today:
                return FoldResult(decision="site_limit", counted_after=1)
            return FoldResult(decision="set_aside", counted_after=0)
        return FoldResult(decision=None, counted_after=1)
    if outcome == "agree" and _take_back_due(history, local_date):
        return FoldResult(decision="taken_back", counted_after=1)
    return FoldResult(decision=None, counted_after=0)
