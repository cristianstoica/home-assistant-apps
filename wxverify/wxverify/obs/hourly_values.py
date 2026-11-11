"""One station's hourly values from its raw readings.

The one hourly-value method: a station's hourly temperature,
rain and wind for one site-day, built from its raw readings of every source,
so the close and the provisional line run the same code. It is pure: no
database, no clock, no network. It imports only the standard library,
``wxverify.obs.wind_pairs`` and ``wxverify.core``.

The hours of a site-day are the whole UTC hours inside its bounds. For each
hour and quantity the readings used are the hour's ``five_minute`` readings
that carry the quantity; if there are none, its ``current_obs`` ones; if
there are none and the hour is a gap, its ``hourly`` ones. A gap is an hour
with no ``five_minute`` and no ``current_obs`` reading. Values here are in
°C, mm and km/h.
"""

from __future__ import annotations

import itertools
import logging
import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final, Literal

from wxverify.core.timeutil import floor_hour, isoformat_utc
from wxverify.obs.wind_pairs import MAX_PAIR_GAP_SECONDS, WindRecord, pair_max_by_hour

logger = logging.getLogger(__name__)

Source = Literal["five_minute", "current_obs", "hourly"]
Quantity = Literal["temperature", "precip", "wind"]
#: The source of an hour's value: its readings' source, or for a day's last
#: hour whose rain also used later current-conditions readings, both.
HourSource = Literal["five_minute", "current_obs", "hourly", "five_minute+current_obs"]

#: Every quantity, in the order values are written.
QUANTITIES: Final[tuple[Quantity, ...]] = ("temperature", "precip", "wind")

_ONE_HOUR: Final = timedelta(hours=1)
_PAIR_LOOKBACK: Final = timedelta(seconds=MAX_PAIR_GAP_SECONDS)
#: The two collection sources, in the source order; ``hourly`` comes last and
#: only for a gap hour.
_COLLECTION_SOURCES: Final[tuple[Source, Source]] = ("five_minute", "current_obs")
_SOURCE_RANK: Final[dict[Source, int]] = {
    "five_minute": 0,
    "current_obs": 1,
    "hourly": 2,
}


@dataclass(frozen=True)
class StationReading:
    """One raw reading of one station from one source.

    ``obs_at`` is a tz-aware UTC instant in whole seconds. ``local_date`` is
    the reading's own local date as the payload states it, ``None`` when it
    states none. Each value is ``None`` when missing or invalid: temperature
    in °C, rain as the running total since the station's local midnight in
    mm, wind in km/h.
    """

    obs_at: datetime
    source: Source
    local_date: date | None
    temp_c: float | None
    precip_total_mm: float | None
    speed_kmh: float | None


@dataclass(frozen=True)
class HourValue:
    """One station's value of one quantity in one hour.

    From ``station_hours`` the value is in °C, mm or km/h. ``source`` is the
    source of the hour's own readings used for it; ``readings`` counts the
    readings it was computed from: the hour's chosen readings (plus, for a
    last hour's rain, the later current-conditions readings), or 1 for an
    hour that took the hourly wind.
    """

    value: float
    source: HourSource
    readings: int


@dataclass(frozen=True)
class StationHours:
    """One station's hourly values for one site-day, from ``station_hours``.

    ``hours`` are the day's hours. ``values`` has a key for every quantity,
    each holding only the hours that have a value. ``gap_hours`` are the
    hours with no ``five_minute`` and no ``current_obs`` reading;
    ``no_pair_hours`` the other hours that have no wind pair,
    whether or not an hourly wind fills them.
    """

    hours: tuple[datetime, ...]
    values: dict[Quantity, dict[datetime, HourValue]]
    gap_hours: tuple[datetime, ...]
    no_pair_hours: tuple[datetime, ...]


def day_hours(day_start_utc: datetime, day_end_utc: datetime) -> tuple[datetime, ...]:
    """The whole UTC hours inside ``[day_start_utc, day_end_utc)``, in order.

    An hour counts when it lies wholly inside the bounds: a 23- or 25-hour
    day has 23 or 25 hours, and a zone whose offset is not a whole hour
    leaves out the part-hours at either end.
    """
    hour = floor_hour(day_start_utc)
    if hour < day_start_utc:
        hour += _ONE_HOUR
    hours: list[datetime] = []
    while hour + _ONE_HOUR <= day_end_utc:
        hours.append(hour)
        hour += _ONE_HOUR
    return tuple(hours)


def station_hours(
    readings: Iterable[StationReading],
    *,
    day_start_utc: datetime,
    day_end_utc: datetime,
    local_date: date,
    station_id: int | None = None,
) -> StationHours:
    """One station's hourly temperature, rain and wind for one site-day.

    ``readings`` are the station's readings of every source; those outside
    the day's hours are used only as the wind lookback, the readings in the
    1200 s before the day's start. Temperature is the mean of the
    hour's readings; rain the rise in the running total from 0 at the
    day's start; wind the largest mean of two neighbouring readings at
    most 1200 s apart, else the hour's latest ``hourly`` wind. The
    day's last hour adds, for rain only, the current-conditions readings
    later than its last 5-minute rain reading. An hour with no value
    is left out, never 0. A rain reading whose own local date is not
    ``local_date`` is not used; the first one is logged once at WARNING.
    ``station_id`` only names the station in that log line.
    """
    hours = day_hours(day_start_utc, day_end_utc)
    ordered = sorted(
        readings, key=lambda reading: (reading.obs_at, _SOURCE_RANK[reading.source])
    )
    by_hour: dict[datetime, list[StationReading]] = {hour: [] for hour in hours}
    for reading in ordered:
        bucket = by_hour.get(floor_hour(reading.obs_at))
        if bucket is not None:
            bucket.append(reading)
    gaps = frozenset(
        hour
        for hour in hours
        if not any(reading.source in _COLLECTION_SOURCES for reading in by_hour[hour])
    )
    _warn_other_dates(hours, by_hour, local_date=local_date, station_id=station_id)
    wind, no_pair_hours = _wind_values(
        hours, by_hour, gaps, ordered, day_start_utc=day_start_utc
    )
    values: dict[Quantity, dict[datetime, HourValue]] = {
        "temperature": _temperature_values(hours, by_hour, gaps),
        "precip": _rain_values(
            hours,
            by_hour,
            gaps,
            ordered,
            local_date=local_date,
            day_end_utc=day_end_utc,
        ),
        "wind": wind,
    }
    return StationHours(
        hours=hours,
        values=values,
        gap_hours=tuple(hour for hour in hours if hour in gaps),
        no_pair_hours=no_pair_hours,
    )


def _pick(
    readings: Sequence[StationReading],
    carries: Callable[[StationReading], bool],
    *,
    gap: bool,
    hourly: bool = True,
) -> tuple[Source, list[StationReading]] | None:
    """The hour's readings of one quantity by the source order.

    ``hourly`` readings are taken only in a gap hour, and never when
    ``hourly`` is False (wind pairs). None when no source has one.
    """
    for source in _COLLECTION_SOURCES:
        chosen = [r for r in readings if r.source == source and carries(r)]
        if chosen:
            return source, chosen
    if hourly and gap:
        chosen = [r for r in readings if r.source == "hourly" and carries(r)]
        if chosen:
            return "hourly", chosen
    return None


def _temperature_values(
    hours: tuple[datetime, ...],
    by_hour: dict[datetime, list[StationReading]],
    gaps: frozenset[datetime],
) -> dict[datetime, HourValue]:
    """The plain mean of each hour's temperature readings."""
    values: dict[datetime, HourValue] = {}
    for hour in hours:
        pick = _pick(by_hour[hour], _has_temperature, gap=hour in gaps)
        if pick is None:
            continue
        source, chosen = pick
        temps = [r.temp_c for r in chosen if r.temp_c is not None]
        values[hour] = HourValue(
            value=math.fsum(temps) / len(temps), source=source, readings=len(temps)
        )
    return values


def _has_temperature(reading: StationReading) -> bool:
    return reading.temp_c is not None


def _has_wind(reading: StationReading) -> bool:
    return reading.speed_kmh is not None


def _rain_total(reading: StationReading, local_date: date) -> float | None:
    """The reading's rain total when rain may use it.

    None when it carries no total or its own local date is another date; a
    reading with no local date counts as the day's.
    """
    if reading.local_date is not None and reading.local_date != local_date:
        return None
    return reading.precip_total_mm


def _totals(readings: Sequence[StationReading], local_date: date) -> list[float]:
    return [
        total
        for reading in readings
        if (total := _rain_total(reading, local_date)) is not None
    ]


def _rise(totals: Sequence[float]) -> float:
    """The sum of the steps through ``totals``; a fall is a step of 0."""
    return math.fsum(
        max(0.0, later - earlier) for earlier, later in itertools.pairwise(totals)
    )


def _rain_values(
    hours: tuple[datetime, ...],
    by_hour: dict[datetime, list[StationReading]],
    gaps: frozenset[datetime],
    ordered: Sequence[StationReading],
    *,
    local_date: date,
    day_end_utc: datetime,
) -> dict[datetime, HourValue]:
    """Each hour's rise in the running total.

    An hour starts from 0 at the day's start when it is the day's first
    hour, else from the last rain reading of the hour before it; with no
    rain reading in the hour or in the hour before it, it has no value. In
    the day's last hour, when it has ``five_minute`` rain readings, the rain
    the latest later ``current_obs`` reading (before the day's end) adds
    over the last of them is added, never below 0.
    """

    def usable(reading: StationReading) -> bool:
        return _rain_total(reading, local_date) is not None

    picked: dict[datetime, tuple[Source, list[StationReading]]] = {}
    for hour in hours:
        pick = _pick(by_hour[hour], usable, gap=hour in gaps)
        if pick is not None:
            picked[hour] = pick
    values: dict[datetime, HourValue] = {}
    last_index = len(hours) - 1
    for index, hour in enumerate(hours):
        own = picked.get(hour)
        if own is None:
            continue
        if index == 0:
            start = 0.0
        else:
            before = picked.get(hours[index - 1])
            if before is None:
                continue
            start = _totals(before[1], local_date)[-1]
        source, chosen = own
        totals = _totals(chosen, local_date)
        value = _rise([start, *totals])
        hour_source: HourSource = source
        count = len(totals)
        if index == last_index and source == "five_minute":
            last_at = chosen[-1].obs_at
            later = [
                total
                for reading in ordered
                if reading.source == "current_obs"
                and last_at < reading.obs_at < day_end_utc
                and (total := _rain_total(reading, local_date)) is not None
            ]
            if later:
                value += max(0.0, later[-1] - totals[-1])
                hour_source = "five_minute+current_obs"
                count += len(later)
        values[hour] = HourValue(value=value, source=hour_source, readings=count)
    return values


def _warn_other_dates(
    hours: tuple[datetime, ...],
    by_hour: dict[datetime, list[StationReading]],
    *,
    local_date: date,
    station_id: int | None,
) -> None:
    """Log the day's first rain reading of another local date, once."""
    others = [
        reading
        for hour in hours
        for reading in by_hour[hour]
        if reading.precip_total_mm is not None
        and reading.local_date is not None
        and reading.local_date != local_date
    ]
    if not others:
        return
    first = others[0]
    logger.warning(
        "rain readings of another local date not used station=%s date=%s"
        " first_at=%s first_date=%s count=%d",
        "-" if station_id is None else station_id,
        local_date.isoformat(),
        isoformat_utc(first.obs_at),
        "-" if first.local_date is None else first.local_date.isoformat(),
        len(others),
    )


def _wind_values(
    hours: tuple[datetime, ...],
    by_hour: dict[datetime, list[StationReading]],
    gaps: frozenset[datetime],
    ordered: Sequence[StationReading],
    *,
    day_start_utc: datetime,
) -> tuple[dict[datetime, HourValue], tuple[datetime, ...]]:
    """Each hour's pair-max wind, else its latest hourly wind.

    The readings that pair are each hour's wind readings by the source order
    without ``hourly``, plus the readings in the 1200 s before the day's
    start, by the source order of the hour before the day. Returns the
    values and the hours that are not gaps and have no pair.
    """
    previous = [
        reading
        for reading in ordered
        if day_start_utc - _ONE_HOUR <= reading.obs_at < day_start_utc
    ]
    series: list[WindRecord] = []
    lookback = _pick(previous, _has_wind, gap=False, hourly=False)
    if lookback is not None:
        series.extend(
            WindRecord(obs_at=r.obs_at, speed_kmh=r.speed_kmh)
            for r in lookback[1]
            if r.speed_kmh is not None and r.obs_at >= day_start_utc - _PAIR_LOOKBACK
        )
    own: dict[datetime, tuple[Source, int]] = {}
    for hour in hours:
        pick = _pick(by_hour[hour], _has_wind, gap=hour in gaps, hourly=False)
        if pick is None:
            continue
        source, chosen = pick
        own[hour] = (source, len(chosen))
        series.extend(
            WindRecord(obs_at=r.obs_at, speed_kmh=r.speed_kmh)
            for r in chosen
            if r.speed_kmh is not None
        )
    pairs = pair_max_by_hour(series, hours)
    values: dict[datetime, HourValue] = {}
    no_pair: list[datetime] = []
    for hour in hours:
        pair = pairs.get(hour)
        if pair is not None:
            source, count = own[hour]
            values[hour] = HourValue(value=pair, source=source, readings=count)
            continue
        if hour not in gaps:
            no_pair.append(hour)
        hourly = [
            r.speed_kmh
            for r in by_hour[hour]
            if r.source == "hourly" and r.speed_kmh is not None
        ]
        if hourly:
            values[hour] = HourValue(value=hourly[-1], source="hourly", readings=1)
    return values, tuple(no_pair)
