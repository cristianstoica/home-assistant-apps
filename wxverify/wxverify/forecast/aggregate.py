"""Pure daily aggregation and blending for the Forecast page.

Everything here is arithmetic over already-selected samples — no SQLite, no
clock reads — so each rule (daily quantities, the coverage
guard, the complete-day coverage predicate, chance-of-rain) is an
independently testable unit.

Methodology (aggregate per feed, then blend):

* each feed's hourly samples are reduced to daily quantities first
  (high/low/max/total/wet-share), THEN the per-feed daily values are blended
  with equal weights — never a pooled blend of raw hours, which would let a
  feed with more hours dominate.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import local_day_slots, parse_utc

# A feed clears the partial-badge guard at >= 18 distinct UTC hour instants for
# the day. DST days have 23 or 25 local hours; the threshold stays a fixed 18
# distinct covered hours rather than a fraction.
MIN_COVERAGE_HOURS = 18

# A feed needs enough distinct UTC hour instants (counted by
# :func:`covered_hours`) to be preferred for the hourly blend set: with
# contiguous-from-midnight partial coverage, the feed must reach far enough
# into the day to catch the afternoon peak, not just the overnight minimum.
# This selection-preference floor is deliberately BELOW MIN_COVERAGE_HOURS
# (the >= 18h partial-badge gate) so a feed can be preferred for selection yet
# still flagged partial. It is not an extrema-validity test: whether a
# temperature cell shows a daily high/low at all is decided per feed by
# :func:`covers_local_day`.
MIN_SPREAD_HOURS = 12

# Absolute degeneracy floor: a feed covering fewer than this many distinct
# UTC hour instants cannot express ANY daily spread (a single point has max == min),
# so it must never be the sole selected feed while a multi-point feed exists.
MULTIPOINT_MIN_HOURS = 2

# The three-valued extrema-coverage verdict a selection carries
# (``CellSelection.extrema_coverage``), so an empty extrema set is never
# ambiguous: "asked and a feed covered the whole local day", "asked and none
# did", or "not asked" (the caller passed ``extrema_coverage_required=False``).
EXTREMA_COVERAGE_COMPLETE = "complete"
EXTREMA_COVERAGE_INSUFFICIENT = "insufficient"
EXTREMA_COVERAGE_NOT_EVALUATED = "not_evaluated"


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


def covered_hours(valid_ats: Iterable[str]) -> int:
    """Distinct UTC hour instants covered by a feed's samples in a day.

    Counted in UTC deliberately: local wall-clock hours collapse the autumn
    DST fold (aware datetimes differing only in ``fold`` compare and hash
    equal), undercounting the fall-back day by one real hour. Truncating the
    UTC instant to the hour BEFORE any local conversion counts every real
    hour exactly once on 23-, 24- and 25-local-hour days alike.
    """
    hours = {
        parse_utc(valid_at).replace(minute=0, second=0, microsecond=0)
        for valid_at in valid_ats
    }
    return len(hours)


def covers_local_day(
    valid_ats: Iterable[str], *, local_date: date, timezone: str
) -> bool:
    """Whether a feed supplies every UTC hourly instant of one local day.

    Instants are truncated to the hour in UTC before any local conversion
    (the same fold-correct rule as :func:`covered_hours`) and are counted
    only inside the day's own ``[start, end)`` window, so a sample either
    side of a boundary cannot stand in for a missing hour inside it.

    The required hours are enumerated explicitly: every on-the-hour UTC
    instant from the first one at or after ``start`` up to, but not
    including, ``end``. They are not counted from the window's floored
    duration, which undercounts a window that starts off the hour: a
    half-hour DST shift can put 25 such instants in 24.5 hours.
    """
    start, end, _ = local_day_slots(local_date, timezone)
    slot = start.replace(minute=0, second=0, microsecond=0)
    if slot < start:
        slot += timedelta(hours=1)
    required: set[datetime] = set()
    while slot < end:
        required.add(slot)
        slot += timedelta(hours=1)
    hours = {
        instant
        for instant in (
            parse_utc(valid_at).replace(minute=0, second=0, microsecond=0)
            for valid_at in valid_ats
        )
        if start <= instant < end
    }
    return hours == required


def clears_coverage(hours: int) -> bool:
    """Whether a feed's day clears the >= 18-distinct-UTC-hour coverage guard.

    ``hours`` is a :func:`covered_hours` count of distinct UTC hour instants.
    This is the ``partial`` badge gate and, for wind and precipitation, the
    aggregation-subset gate; it is not an extrema-validity test (see
    :func:`covers_local_day`).
    """
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


def clearing_subset(
    selected_ids: Sequence[int], covered_by_feed: Mapping[int, int]
) -> tuple[list[int], bool]:
    """The feed subset whose values aggregate for display, plus ``partial``.

    Feeds clearing the >= 18-hour coverage guard aggregate alone; when NO
    selected feed clears it, ALL selected feeds still aggregate (the tile
    stays populated) and the cell carries the orthogonal ``partial`` badge.
    Shared by the Forecast page and the forecast-of-record builder so the
    two cannot drift. On the Forecast page and in the record's ``displayed``
    block, a temperature cell's high/low no longer aggregate over this
    subset — the selection's extrema set, decided per feed by
    :func:`covers_local_day`, does — and there it still sets the ``partial``
    badge. The scored product and the simulator still aggregate over it.
    """
    clearing = [fid for fid in selected_ids if clears_coverage(covered_by_feed[fid])]
    if clearing:
        return clearing, False
    return list(selected_ids), True


def displayed_daily(
    variable: str,
    per_feed_values: Sequence[Sequence[float]],
    *,
    rain_threshold_mm: float,
) -> dict[str, float | None]:
    """The DISPLAYED daily quantities for one cell, aggregate-per-feed-then-blend.

    Each inner sequence is one feed's hourly values for the day (already
    restricted by the caller: to the :func:`clearing_subset`, or for a
    displayed temperature cell to the selection's extrema set; an empty
    sequence yields ``None`` values). Native units throughout
    (wind in m/s; chance as a 0..1 fraction) — unit conversion and percent
    rounding are presentational. Shared by the Forecast page and the
    forecast-of-record builder so the two cannot drift.
    """
    if variable == "temperature":
        return {
            "high_c": blend_mean([max(v) for v in per_feed_values if v]),
            "low_c": blend_mean([min(v) for v in per_feed_values if v]),
        }
    if variable == "wind":
        return {"max_ms": blend_mean([max(v) for v in per_feed_values if v])}
    if variable == "precip":
        shares = [
            share
            for share in (
                wet_share(v, threshold_mm=rain_threshold_mm) for v in per_feed_values
            )
            if share is not None
        ]
        return {
            "total_mm": blend_mean([sum(v) for v in per_feed_values if v]),
            "chance": predicted_wet_hour_share(shares),
        }
    raise ValueError(f"unknown variable {variable!r}")


def predicted_wet_hour_share(per_feed_shares: Sequence[float]) -> float | None:
    """Blend per-feed wet shares (equal weights) into the displayed value.

    §16's shipped vocabulary: this is the PREDICTED WET-HOUR SHARE — a
    coverage-of-the-day estimate, not a calibrated probability of
    precipitation. Each feed contributes ITS share of wet slots, and the
    shares are averaged across feeds. (The payload key stays ``chance``:
    it is a wire and template contract, not internal vocabulary.)
    """
    return blend_mean(per_feed_shares)
