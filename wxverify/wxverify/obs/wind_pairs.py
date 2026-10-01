"""The station wind figure ``max_adjacent_pair_mean_wind``.

Pure functions over one station's raw 5-minute records. The figure is the
largest mean of two back-to-back readings in an hour. It is this add-on's own
statistic and makes no WMO claim.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from wxverify.core.timeutil import floor_hour

MAX_PAIR_GAP_SECONDS: Final = 600


@dataclass(frozen=True)
class WindRecord:
    """One raw station reading: a whole-second UTC instant and ``windspeedAvg``."""

    obs_at: datetime
    speed_kmh: float


def pair_max_by_hour(
    records: Sequence[WindRecord], hours: Collection[datetime]
) -> dict[datetime, float]:
    """max_adjacent_pair_mean_wind: for consecutive records (a, b) of ONE
    station, sorted by obs_at, with 0 < (b - a) <= 600 s, the pair value
    (a.speed + b.speed) / 2 counts in floor_hour(b.obs_at). Returns
    {hour: max pair value} for hours in `hours` only; hours without a
    pair are absent."""
    wanted = set(hours)
    ordered = sorted(records, key=lambda record: record.obs_at)
    result: dict[datetime, float] = {}
    for earlier, later in zip(ordered, ordered[1:], strict=False):
        gap = (later.obs_at - earlier.obs_at).total_seconds()
        if not 0 < gap <= MAX_PAIR_GAP_SECONDS:
            continue
        hour = floor_hour(later.obs_at)
        if hour not in wanted:
            continue
        value = (earlier.speed_kmh + later.speed_kmh) / 2
        current = result.get(hour)
        if current is None or value > current:
            result[hour] = value
    return result
