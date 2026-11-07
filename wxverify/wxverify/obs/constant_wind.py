"""A wind station's constant reading while other stations report wind.

The rule of the warning "constant reading while other stations report wind",
labelled "suspected": a station whose stored wind value stays the same
through ``Rule.hours`` consecutive whole UTC hours that each have a reading,
while in each of those hours at least ``Rule.other_stations`` other stations
of its site have a reading and the median of their hourly means is at least
``Rule.median_kmh``. A missing hour breaks a run; a changed reading ends an
open episode; an hour the others do not cover is "can't assess", never
"recovered". It is a warning only: nothing here changes what the blend uses.

It is pure: no database, no clock, no network. It imports only the standard
library and ``wxverify.core``. Values are km/h, as the stored raw readings
hold them, and are compared with plain float equality, never rounded.
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final, Literal, Protocol

from wxverify.core.timeutil import floor_hour

EpisodeState = Literal["suspected", "cant_assess"]

#: One station's wind values per whole UTC hour, as ``hour_winds`` builds them.
HourWinds = Mapping[datetime, tuple[float, ...]]

#: The source order of an hour's wind values: the hour's values all come from
#: the first of these sources that has a wind value in that hour.
SOURCE_ORDER: Final[tuple[str, ...]] = ("five_minute", "current_obs", "hourly")


class WindReading(Protocol):
    """One stored raw reading, as far as the rule reads it.

    ``obs_at`` is a tz-aware instant, ``source`` one of ``SOURCE_ORDER`` and
    ``speed_kmh`` the wind in km/h, None when the reading has none.
    """

    @property
    def obs_at(self) -> datetime: ...

    @property
    def source(self) -> str: ...

    @property
    def speed_kmh(self) -> float | None: ...


@dataclass(frozen=True)
class Rule:
    """The rule's three settings: the run's length in hours, the fewest other
    stations an hour needs, and the lowest median of their means in km/h."""

    hours: int
    other_stations: int
    median_kmh: float


@dataclass(frozen=True)
class Episode:
    """One open episode: the unchanged value in km/h, the first hour of its
    run, and whether its latest hour could be assessed."""

    speed_kmh: float
    since: datetime
    state: EpisodeState


def hour_winds(readings: Sequence[WindReading]) -> dict[datetime, tuple[float, ...]]:
    """Each whole UTC hour's wind values, in hour order.

    An hour's values are those of the first source in ``SOURCE_ORDER`` that
    has a wind value in the hour, in the readings' own order; the other
    sources' readings of that hour are not used. An hour with no wind value
    has no key.
    """
    by_hour: dict[datetime, dict[str, list[float]]] = {}
    for reading in readings:
        speed = reading.speed_kmh
        if speed is None:
            continue
        sources = by_hour.setdefault(floor_hour(reading.obs_at), {})
        sources.setdefault(reading.source, []).append(speed)
    winds: dict[datetime, tuple[float, ...]] = {}
    for hour in sorted(by_hour):
        sources = by_hour[hour]
        for source in SOURCE_ORDER:
            values = sources.get(source)
            if values:
                winds[hour] = tuple(values)
                break
    return winds


def reporting_others(others: Sequence[HourWinds], hour: datetime) -> int:
    """How many of ``others`` have wind values in ``hour``."""
    return sum(1 for other in others if other.get(hour))


def assessable(others: Sequence[HourWinds], hour: datetime, *, rule: Rule) -> bool:
    """Whether the other stations cover ``hour``.

    True when at least ``rule.other_stations`` of ``others`` have values in
    the hour and the median of each one's mean is at least
    ``rule.median_kmh``. An hour that is not assessable is "can't tell".
    """
    means = [statistics.mean(values) for other in others if (values := other.get(hour))]
    if not means or len(means) < rule.other_stations:
        return False
    return statistics.median(means) >= rule.median_kmh


def _holds(own: HourWinds, hour: datetime, value: float) -> bool:
    """Whether ``own`` has values in ``hour`` and every one equals ``value``."""
    values = own.get(hour)
    if not values:
        return False
    return all(v == value for v in values)


def assess(
    own: HourWinds,
    others: Sequence[HourWinds],
    *,
    window: Sequence[datetime],
    new_hours: Sequence[datetime],
    episode: Episode | None,
    rule: Rule,
) -> Episode | None:
    """The station's episode after the hours since the last assessment.

    With an open ``episode``: any value of ``own`` in an hour of
    ``new_hours`` (oldest first) other than ``episode.speed_kmh`` clears it
    (None); an hour with no value changes nothing. Otherwise the episode is
    returned with state ``suspected`` when the last hour of ``new_hours``
    holds its value and is assessable, else ``cant_assess``.

    With no episode: the run is the last ``rule.hours`` hours of ``window``.
    When every one holds the same value and is assessable, a ``suspected``
    episode from the run's first hour is returned, else None. An hour that
    is not assessable is "can't tell", not healthy, and never counts toward
    the run. A window shorter than ``rule.hours`` opens nothing.
    """
    if episode is not None:
        value = episode.speed_kmh
        for hour in new_hours:
            if any(v != value for v in own.get(hour, ())):
                return None
        if not new_hours:
            return episode
        last = new_hours[-1]
        state: EpisodeState = (
            "suspected"
            if _holds(own, last, value) and assessable(others, last, rule=rule)
            else "cant_assess"
        )
        return Episode(speed_kmh=value, since=episode.since, state=state)
    if rule.hours < 1 or len(window) < rule.hours:
        return None
    run = window[len(window) - rule.hours :]
    first = own.get(run[0])
    if not first:
        return None
    value = first[0]
    if all(
        _holds(own, hour, value) and assessable(others, hour, rule=rule) for hour in run
    ):
        return Episode(speed_kmh=value, since=run[0], state="suspected")
    return None
