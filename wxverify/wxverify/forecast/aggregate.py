"""Pure daily aggregation and blending for the Forecast page.

Everything here is arithmetic over already-selected samples — no SQLite, no
clock reads — so each rule from the spec (daily quantities, the coverage
guard, chance-of-rain) is an independently testable unit.

Methodology (ADR 0001, "aggregate per feed, then blend"):

* each feed's hourly samples are reduced to daily quantities first
  (high/low/max/total/wet-share), THEN the per-feed daily values are blended
  with equal weights — never a pooled blend of raw hours, which would let a
  feed with more hours dominate.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import parse_utc

# A feed's day counts as fully covered at >= 18 of 24 local hours. DST days
# have 23 or 25 local hours; the threshold stays a fixed 18 distinct covered
# hours rather than a fraction, per the spec.
MIN_COVERAGE_HOURS = 18


def display_day_index(valid_at: str, *, timezone: str, now: datetime) -> int:
    """Now-relative local-day index of a sample: 0 = Today tile, 1 = Tomorrow.

    This is the DISPLAY mapping — distinct from the issue-relative
    ``day_ahead`` used for skill lookup. The two diverge near local midnight:
    a sample valid today but issued yesterday has ``day_ahead = 1`` yet
    belongs on the Today tile.
    """
    tz = ZoneInfo(timezone)
    valid_day = parse_utc(valid_at).astimezone(tz).date()
    today = now.astimezone(tz).date()
    return (valid_day - today).days


def covered_hours(valid_ats: Iterable[str], *, timezone: str) -> int:
    """Distinct local wall-clock hours covered by a feed's samples in a day."""
    tz = ZoneInfo(timezone)
    hours = {
        parse_utc(valid_at).astimezone(tz).replace(minute=0, second=0, microsecond=0)
        for valid_at in valid_ats
    }
    return len(hours)


def clears_coverage(hours: int) -> bool:
    """Whether a feed's day clears the >= 18-of-24 local-hour coverage guard."""
    return hours >= MIN_COVERAGE_HOURS


def blend_mean(values: Sequence[float]) -> float | None:
    """Equal-weight blend of per-feed daily values; None when empty."""
    if not values:
        return None
    return sum(values) / len(values)


def wet_share(values: Sequence[float], *, threshold_mm: float) -> float | None:
    """Share of a feed's covered hourly slots at/above the site rain threshold.

    The boundary is inclusive (``value >= threshold``): a slot exactly at the
    site's ``rain_threshold_mm`` counts as wet, matching the threshold's
    meaning of "the smallest amount that counts as rain here".
    """
    if not values:
        return None
    wet = sum(1 for value in values if value >= threshold_mm)
    return wet / len(values)


def chance_of_rain(per_feed_shares: Sequence[float]) -> float | None:
    """Blend per-feed wet shares (equal weights) into the displayed chance.

    Per the spec this is a coverage-of-the-day estimate, not a calibrated
    probability of precipitation: each feed contributes ITS share of wet
    slots, and the shares are averaged across feeds.
    """
    return blend_mean(per_feed_shares)
