"""Pure daily aggregation and blending for the Forecast page.

Everything here is arithmetic over already-selected samples — no SQLite, no
clock reads — so each rule (daily quantities, the coverage
guard, the complete-day coverage predicates, the wet-hour count) is an
independently testable unit.

Methodology (aggregate per feed, then blend):

* each feed's hourly samples are reduced to daily quantities first
  (high/low/max/total/wet-hour count), THEN the per-feed daily values are
  blended with equal weights — never a pooled blend of raw hours, which would
  let a feed with more hours dominate.
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

# The three-valued ``CellSelection.extrema_coverage`` verdict, so an empty
# extrema set is never ambiguous: "asked and a feed covered the local day"
# (:func:`covers_local_day`; for precip, :func:`covers_local_day_exactly`),
# "asked and none did", or "not asked" (``extrema_coverage_required=False``).
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
    duration, because the two need not agree: a 24.5-hour window that starts
    on the hour, as on a half-hour DST shift, holds 25 on-the-hour instants
    but floors to 24.
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


def covers_local_day_exactly(
    valid_ats: Iterable[str], *, local_date: date, timezone: str
) -> bool:
    """Whether a feed supplies each hour of one local day exactly once.

    Stricter than :func:`covers_local_day`, which a sum and a count need and
    an extremum does not. The required instants are enumerated exactly as
    :func:`covers_local_day` enumerates them: every on-the-hour UTC instant
    from the first one at or after the window's ``start`` up to, but not
    including, its ``end``. The supplied instants must be that set and
    nothing else, each appearing once, so a missing hour, a repeated one, an
    off-hour sample and an instant outside the window all fail. Instants are
    compared UNTRUNCATED, so an off-hour sample fails the rule rather than
    being silently folded onto the hour it is nearest.
    """
    start, end, _ = local_day_slots(local_date, timezone)
    slot = start.replace(minute=0, second=0, microsecond=0)
    if slot < start:
        slot += timedelta(hours=1)
    required: set[datetime] = set()
    while slot < end:
        required.add(slot)
        slot += timedelta(hours=1)
    instants = [parse_utc(valid_at) for valid_at in valid_ats]
    return len(instants) == len(set(instants)) and set(instants) == required


def clears_coverage(hours: int) -> bool:
    """Whether a feed's day clears the >= 18-distinct-UTC-hour coverage guard.

    ``hours`` is a :func:`covered_hours` count of distinct UTC hour instants.
    This is the ``partial`` badge gate for every variable and, for wind alone,
    the gate on the feeds a displayed daily value aggregates; the scored
    product and the simulator still aggregate over the subset it defines for
    every variable. It is not a validity test for a displayed daily value
    (see :func:`covers_local_day` for temperature and
    :func:`covers_local_day_exactly` for precipitation).
    """
    return hours >= MIN_COVERAGE_HOURS


def blend_mean(values: Sequence[float]) -> float | None:
    """Equal-weight blend of per-feed daily values; None when empty."""
    if not values:
        return None
    return sum(values) / len(values)


def wet_hours(values: Sequence[float], *, threshold_mm: float) -> int | None:
    """Count of a feed's hourly slots at/above the site rain threshold.

    A COUNT, not a share: the caller guarantees one value per hour of the
    local day (:func:`covers_local_day_exactly`), so the count is already in
    hours and needs no denominator — which is what the share it replaces got
    wrong, dividing by however many samples the feed happened to supply.
    The boundary is inclusive (``value >= threshold``): a slot exactly at the
    site's ``rain_threshold_mm`` counts as wet, matching the threshold's
    meaning of "the smallest amount that counts as rain here". ``None`` for an
    empty sequence, never ``0`` — a feed with no values has not forecast a dry
    day.
    """
    if not values:
        return None
    return sum(1 for value in values if value >= threshold_mm)


def fixed_membership_series(
    hours: Sequence[str],
    members: Sequence[Mapping[str, float]],
) -> list[float | None]:
    """The aggregate value at each instant of ``hours`` over a FIXED member set.

    Each member maps a ``valid_at`` string to that feed's value. Null at every
    instant when ``members`` is empty, and null at any instant a member does not
    supply; never renormalised over the members present. Values are blended in
    ``members`` order with :func:`blend_mean`.
    """
    if not members:
        return [None] * len(hours)
    out: list[float | None] = []
    for valid_at in hours:
        values = [member.get(valid_at) for member in members]
        if any(value is None for value in values):
            out.append(None)
            continue
        out.append(blend_mean([v for v in values if v is not None]))
    return out


def clearing_subset(
    selected_ids: Sequence[int], covered_by_feed: Mapping[int, int]
) -> tuple[list[int], bool]:
    """The feed subset whose values aggregate for display, plus ``partial``.

    Feeds clearing the >= 18-hour coverage guard aggregate alone; when NO
    selected feed clears it, ALL selected feeds still aggregate (the tile
    stays populated) and the cell carries the orthogonal ``partial`` badge.
    Shared by the Forecast page and the forecast-of-record builder so the
    two cannot drift. On the Forecast page and in the record's ``displayed``
    block, neither a temperature cell's high/low nor a precipitation cell's
    total and wet-hour count aggregate over this subset — the selection's
    extrema set does, decided per feed by :func:`covers_local_day` for
    temperature and by :func:`covers_local_day_exactly` for precipitation —
    and there the subset still sets the ``partial`` badge. The scored product
    and the simulator still aggregate over it.
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
    displayed temperature or precipitation cell to the selection's extrema
    set; an empty sequence yields ``None`` values). Native units throughout
    (wind in m/s; wet hours as a count of hours, unrounded) — unit conversion
    and the rounding of that count are presentational, done once in
    ``_build_tile``. Shared by the Forecast page and the forecast-of-record
    builder so the two cannot drift.
    """
    if variable == "temperature":
        return {
            "high_c": blend_mean([max(v) for v in per_feed_values if v]),
            "low_c": blend_mean([min(v) for v in per_feed_values if v]),
        }
    if variable == "wind":
        return {"max_ms": blend_mean([max(v) for v in per_feed_values if v])}
    if variable == "precip":
        counts = [
            count
            for count in (
                wet_hours(v, threshold_mm=rain_threshold_mm) for v in per_feed_values
            )
            if count is not None
        ]
        return {
            "total_mm": blend_mean([sum(v) for v in per_feed_values if v]),
            "wet_hours": blend_mean(counts),
        }
    raise ValueError(f"unknown variable {variable!r}")
