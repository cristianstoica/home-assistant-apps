"""Pure per-quantity coverage gates and daily-quantity evaluation (plan §4/§5).

One shared implementation serves BOTH sides of the comparison:

* truth materialization (``wxverify.verification.truth``) evaluates hourly
  CONSENSUS observations for a local day;
* forecast-side eligibility (§5) evaluates an entity's as-of-T hourly
  forecast samples for the target day with the very same gates — "renderable
  by the production page" is not sufficient forecast eligibility.

Coverage counting follows §4's distinct-UTC-instant rule: expected slots come
from the site timezone's actual local-day boundaries (DST days contain 23, 24
or 25 UTC hourly instants; both instants of the repeated autumn wall-clock
hour count), and covered hours are distinct UTC hour instants (truncate the
UTC instant to the hour BEFORE any local conversion — aware datetimes
differing only in ``fold`` compare and hash equal, which would collapse the
autumn fold). Local wall-clock time is used only for peak-window membership.

Everything here is arithmetic over already-loaded samples — no SQLite, no
clock reads — so every gate boundary is an independently testable unit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import parse_utc
from wxverify.verification.methodology import (
    NEAR_COMPLETE_SLOT_ALLOWANCE,
    TEMP_PEAK_WINDOW_HIGH,
    TEMP_PEAK_WINDOW_LOW,
    TEMP_TRUTH_MIN_HOURS,
)

# The five daily quantities (§4). Scorability is recorded per quantity,
# never per variable: one outcome of a day may score while another is
# excluded.
QUANTITY_TEMPERATURE_HIGH = "temperature_high"
QUANTITY_TEMPERATURE_LOW = "temperature_low"
QUANTITY_WIND_MAX = "wind_max"
QUANTITY_PRECIP_TOTAL = "precip_total"
QUANTITY_PRECIP_OCCURRENCE = "precip_occurrence"

QUANTITIES: tuple[str, ...] = (
    QUANTITY_TEMPERATURE_HIGH,
    QUANTITY_TEMPERATURE_LOW,
    QUANTITY_WIND_MAX,
    QUANTITY_PRECIP_TOTAL,
    QUANTITY_PRECIP_OCCURRENCE,
)

# Observation/forecast variable -> the daily quantities derived from it.
VARIABLE_QUANTITIES: dict[str, tuple[str, ...]] = {
    "temperature": (QUANTITY_TEMPERATURE_HIGH, QUANTITY_TEMPERATURE_LOW),
    "wind": (QUANTITY_WIND_MAX,),
    "precip": (QUANTITY_PRECIP_TOTAL, QUANTITY_PRECIP_OCCURRENCE),
}

# Recorded per-quantity exclusion reasons. Classification is by FIRST
# failing gate, in the order §4's table states them (same policy as the
# knowability exclusions in wxverify.verification.asof).
EXCLUDE_INSUFFICIENT_COVERAGE = "insufficient_coverage"
EXCLUDE_MISSING_PEAK_WINDOW = "missing_peak_window"
EXCLUDE_BELOW_NEAR_COMPLETE = "below_near_complete"
EXCLUDE_DRY_WITHOUT_NEAR_COMPLETE = "dry_without_near_complete"


@dataclass(frozen=True)
class DayBounds:
    """UTC boundaries and expected slot count of one local calendar day."""

    local_date: date
    timezone: str
    start_utc: datetime
    end_utc: datetime
    expected_slots: int


def local_day_bounds(local_date: date, timezone: str) -> DayBounds:
    """UTC window ``[local midnight, next local midnight)`` and its slot count.

    ``expected_slots`` is the number of UTC hourly instants inside the
    window — 23, 24 or 25 on DST-transition days.
    """
    tz = ZoneInfo(timezone)
    start = datetime(
        local_date.year, local_date.month, local_date.day, tzinfo=tz
    ).astimezone(UTC)
    next_day = local_date + timedelta(days=1)
    end = datetime(next_day.year, next_day.month, next_day.day, tzinfo=tz).astimezone(
        UTC
    )
    expected = int((end - start).total_seconds() // 3600)
    return DayBounds(
        local_date=local_date,
        timezone=timezone,
        start_utc=start,
        end_utc=end,
        expected_slots=expected,
    )


@dataclass(frozen=True)
class QuantityOutcome:
    """One daily quantity's value, eligibility, and coverage metadata.

    ``value`` is retained even when ineligible (an explicitly labeled
    diagnostic — §4 keeps every excluded outcome visible with its coverage
    count and reason; §5 retains partial-product results as diagnostics,
    never headline inputs). The single exception: an ineligible "dry"
    occurrence verdict has ``value = None`` — partial coverage cannot prove
    a dry day, so there is no diagnostic value to label.
    """

    quantity: str
    value: float | None
    eligible: bool
    exclusion_reason: str | None
    covered_hours: int
    expected_slots: int
    peak_window_ok: bool | None
    wet_hours: int | None
    dry_hours: int | None


def _hourly_slots(
    samples: list[tuple[str, float]], bounds: DayBounds
) -> dict[datetime, float]:
    """Deduplicate samples to one value per distinct UTC hour instant.

    Truncation to the hour happens in UTC, before any local conversion
    (§4's fold-correct counting rule). Membership is judged on the
    truncated instant against the day's UTC window. On duplicate instants
    the last sample wins — hourly consensus rows are unique per instant by
    schema, so this only matters for caller-assembled forecast samples.
    """
    slots: dict[datetime, float] = {}
    for valid_at, value in samples:
        instant = parse_utc(valid_at).replace(minute=0, second=0, microsecond=0)
        if bounds.start_utc <= instant < bounds.end_utc:
            slots[instant] = value
    return slots


def _has_peak_observation(
    slots: dict[datetime, float], bounds: DayBounds, window: tuple[int, int]
) -> bool:
    """Whether any covered slot's LOCAL wall-clock hour falls in the window.

    The window is half-open ``[start, end)`` in local hours; both instants
    of the repeated autumn hour map to the same wall-clock hour and each
    satisfies membership independently.
    """
    tz = ZoneInfo(bounds.timezone)
    start, end = window
    return any(start <= instant.astimezone(tz).hour < end for instant in slots)


def _temperature_outcome(
    quantity: str,
    slots: dict[datetime, float],
    bounds: DayBounds,
    window: tuple[int, int],
    reduce_max: bool,
) -> QuantityOutcome:
    covered = len(slots)
    peak_ok = _has_peak_observation(slots, bounds, window)
    value = None
    if slots:
        value = max(slots.values()) if reduce_max else min(slots.values())
    reason = None
    if covered < TEMP_TRUTH_MIN_HOURS:
        reason = EXCLUDE_INSUFFICIENT_COVERAGE
    elif not peak_ok:
        reason = EXCLUDE_MISSING_PEAK_WINDOW
    return QuantityOutcome(
        quantity=quantity,
        value=value,
        eligible=reason is None,
        exclusion_reason=reason,
        covered_hours=covered,
        expected_slots=bounds.expected_slots,
        peak_window_ok=peak_ok,
        wet_hours=None,
        dry_hours=None,
    )


def near_complete_threshold(expected_slots: int) -> int:
    """Minimum covered slots for the near-complete gate (expected − 1)."""
    return expected_slots - NEAR_COMPLETE_SLOT_ALLOWANCE


def evaluate_temperature(
    samples: list[tuple[str, float]], *, timezone: str, local_date: date
) -> tuple[QuantityOutcome, QuantityOutcome]:
    """Daily high and low with §4's temperature gates.

    Eligible iff ≥ 18 distinct covered hours AND ≥ 1 slot inside the
    applicable peak window (12–18 local for the high, 03–09 local for the
    low). First failing gate is the recorded reason.
    """
    bounds = local_day_bounds(local_date, timezone)
    slots = _hourly_slots(samples, bounds)
    high = _temperature_outcome(
        QUANTITY_TEMPERATURE_HIGH, slots, bounds, TEMP_PEAK_WINDOW_HIGH, True
    )
    low = _temperature_outcome(
        QUANTITY_TEMPERATURE_LOW, slots, bounds, TEMP_PEAK_WINDOW_LOW, False
    )
    return high, low


def evaluate_wind(
    samples: list[tuple[str, float]], *, timezone: str, local_date: date
) -> QuantityOutcome:
    """Daily wind maximum with the near-complete gate (≥ expected − 1)."""
    bounds = local_day_bounds(local_date, timezone)
    slots = _hourly_slots(samples, bounds)
    covered = len(slots)
    eligible = covered >= near_complete_threshold(bounds.expected_slots)
    return QuantityOutcome(
        quantity=QUANTITY_WIND_MAX,
        value=max(slots.values()) if slots else None,
        eligible=eligible,
        exclusion_reason=None if eligible else EXCLUDE_BELOW_NEAR_COMPLETE,
        covered_hours=covered,
        expected_slots=bounds.expected_slots,
        peak_window_ok=None,
        wet_hours=None,
        dry_hours=None,
    )


def evaluate_precip(
    samples: list[tuple[str, float]],
    *,
    timezone: str,
    local_date: date,
    rain_threshold_mm: float,
) -> tuple[QuantityOutcome, QuantityOutcome]:
    """Daily precip total and occurrence with §4's precip gates.

    Total: near-complete gate — an incomplete sum is never exact truth (the
    partial sum is still stored as a labeled diagnostic). Occurrence is
    asymmetric: one qualifying wet slot (``value >= rain_threshold_mm``,
    inclusive, matching the production wet-share boundary) proves "wet" at
    ANY coverage; a "dry" verdict requires near-complete coverage, and a
    partial dry day carries no value at all.
    """
    bounds = local_day_bounds(local_date, timezone)
    slots = _hourly_slots(samples, bounds)
    covered = len(slots)
    near_complete = covered >= near_complete_threshold(bounds.expected_slots)
    wet = sum(1 for value in slots.values() if value >= rain_threshold_mm)
    dry = covered - wet
    total = QuantityOutcome(
        quantity=QUANTITY_PRECIP_TOTAL,
        value=sum(slots.values()) if slots else None,
        eligible=near_complete,
        exclusion_reason=None if near_complete else EXCLUDE_BELOW_NEAR_COMPLETE,
        covered_hours=covered,
        expected_slots=bounds.expected_slots,
        peak_window_ok=None,
        wet_hours=wet,
        dry_hours=dry,
    )
    if wet >= 1:
        occurrence_value: float | None = 1.0
        occurrence_eligible = True
        occurrence_reason = None
    elif near_complete:
        occurrence_value = 0.0
        occurrence_eligible = True
        occurrence_reason = None
    else:
        occurrence_value = None
        occurrence_eligible = False
        occurrence_reason = EXCLUDE_DRY_WITHOUT_NEAR_COMPLETE
    occurrence = QuantityOutcome(
        quantity=QUANTITY_PRECIP_OCCURRENCE,
        value=occurrence_value,
        eligible=occurrence_eligible,
        exclusion_reason=occurrence_reason,
        covered_hours=covered,
        expected_slots=bounds.expected_slots,
        peak_window_ok=None,
        wet_hours=wet,
        dry_hours=dry,
    )
    return total, occurrence


def evaluate_variable(
    variable: str,
    samples: list[tuple[str, float]],
    *,
    timezone: str,
    local_date: date,
    rain_threshold_mm: float,
) -> tuple[QuantityOutcome, ...]:
    """Evaluate every quantity derived from ``variable`` for one local day.

    Shared entry point for truth materialization (consensus samples) and
    §5 forecast-side eligibility (an entity's as-of hourly samples) —
    identical gates by construction. Unknown variables yield no outcomes.
    """
    if variable == "temperature":
        return evaluate_temperature(samples, timezone=timezone, local_date=local_date)
    if variable == "wind":
        return (evaluate_wind(samples, timezone=timezone, local_date=local_date),)
    if variable == "precip":
        return evaluate_precip(
            samples,
            timezone=timezone,
            local_date=local_date,
            rain_threshold_mm=rain_threshold_mm,
        )
    return ()
