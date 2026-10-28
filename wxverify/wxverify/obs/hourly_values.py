"""One station's hourly values from its raw readings.

This module will hold the one hourly-value method (plan §7.1): a station's
hourly temperature, rain and wind, built from its raw readings of every
source for one site-day. It is pure: no database, no clock, no network. For
now it holds only the reading types the payload parsers produce.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

Source = Literal["five_minute", "current_obs", "hourly"]
Quantity = Literal["temperature", "precip", "wind"]


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
