"""Unit tests for ``wxverify.forecast.aggregate`` and ``core.units.ms_to_kmh``.

This file covers ms_to_kmh conversion,
precip total (with a stray negative filtered out), the wet-hour count at the
rain-threshold boundary, and a partially-covered day flagged 'partial'.

The stray-negative-filtered and partial-day-flagged parts of that are
DB-facing (the negative is dropped by `invalid_forecast_sample_sql` in the
data layer, and the "partial" badge is assembled in the service layer from
`clears_coverage`) — those live in test_forecast_data.py / test_forecast_service.py.
This file owns everything pure: `ms_to_kmh`, `display_day_index` (the display
half of the day-boundary gate), `covered_hours`, `clears_coverage` (pinned
against `MIN_COVERAGE_HOURS`, not a hardcoded 18), `blend_mean`,
`wet_hours` (the rain-threshold inclusive boundary and per-feed-then-blend
counting, proven distinct from a naive thresholded mean), and
`fixed_membership_series` (null at any instant a member does not supply,
never renormalised over the members present).

No SQLite anywhere in this module — nothing to isolate.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from wxverify.core.units import kmh_to_ms, ms_to_kmh
from wxverify.forecast.aggregate import (
    MIN_COVERAGE_HOURS,
    blend_mean,
    clears_coverage,
    covered_hours,
    display_day_index,
    displayed_daily,
    fixed_membership_series,
    wet_hours,
)

# ---------------------------------------------------------------------------
# ms_to_kmh
# ---------------------------------------------------------------------------


def test_ms_to_kmh_5_ms_is_18_kmh() -> None:
    assert ms_to_kmh(5.0) == 18.0


def test_ms_to_kmh_zero_is_zero() -> None:
    assert ms_to_kmh(0.0) == 0.0


def test_ms_to_kmh_is_not_accidentally_kmh_to_ms() -> None:
    # Regression guard against the two helpers being aliased/swapped: they
    # must diverge on a non-zero input (5*3.6=18.0 vs 5/3.6=1.388...).
    assert ms_to_kmh(5.0) != kmh_to_ms(5.0)


def test_ms_to_kmh_round_trips_through_kmh_to_ms() -> None:
    assert kmh_to_ms(ms_to_kmh(5.0)) == 5.0


# ---------------------------------------------------------------------------
# display_day_index — now-relative LOCAL date, distinct from UTC date.
# ---------------------------------------------------------------------------


def test_display_day_index_same_local_day_is_zero() -> None:
    now = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
    idx = display_day_index("2026-07-20T14:00:00Z", timezone="UTC", now=now)
    assert idx == 0


def test_display_day_index_uses_local_date_not_utc_date() -> None:
    # now and valid_at share the SAME UTC calendar date (2026-07-20) but a
    # UTC-4 local clock puts `now` on 2026-07-19 and `valid_at` (04:00Z ->
    # 00:00 local) on 2026-07-20 -- a full local day apart. If this ever
    # regressed to comparing UTC dates instead of local dates, this would
    # wrongly read 0.
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)  # 2026-07-19T22:00 local
    valid_at = "2026-07-20T04:00:00Z"  # 2026-07-20T00:00 local
    idx = display_day_index(valid_at, timezone="America/New_York", now=now)
    assert idx == 1


def test_display_day_index_past_local_day_is_negative() -> None:
    now = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
    idx = display_day_index("2026-07-19T10:00:00Z", timezone="UTC", now=now)
    assert idx == -1


# ---------------------------------------------------------------------------
# covered_hours — distinct UTC hour instants.
# ---------------------------------------------------------------------------


def test_covered_hours_dedupes_same_clock_hour() -> None:
    hours = covered_hours(["2026-07-20T05:00:00Z", "2026-07-20T05:45:00Z"])
    assert hours == 1


def test_covered_hours_counts_24_distinct_hours() -> None:
    valid_ats = [f"2026-07-20T{h:02d}:00:00Z" for h in range(24)]
    assert covered_hours(valid_ats) == 24


def test_covered_hours_empty_is_zero() -> None:
    assert covered_hours([]) == 0


def test_covered_hours_counts_both_instants_of_the_autumn_fold() -> None:
    # Europe/Athens 2026 fall-back: 00:00Z and 01:00Z both map to local
    # 03:xx on 2026-10-25. Counting local wall-clock hours collapses them
    # (aware datetimes differing only in `fold` compare equal); counting UTC
    # hour instants keeps both real hours.
    assert covered_hours(["2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z"]) == 2


def _local_day_hourly_utc_instants(day: datetime, tz: ZoneInfo) -> list[str]:
    """Every whole-hour UTC instant inside `day`'s local calendar day.

    Walked in UTC (never local wall-clock) so the fold cannot collapse two
    instants into one while building the fixture itself.
    """
    start = day.replace(tzinfo=tz).astimezone(UTC)
    end = (day + timedelta(days=1)).replace(tzinfo=tz).astimezone(UTC)
    instants: list[str] = []
    cursor = start
    while cursor < end:
        instants.append(cursor.strftime("%Y-%m-%dT%H:%M:%SZ"))
        cursor += timedelta(hours=1)
    return instants


def test_covered_hours_full_fall_back_local_day_is_25() -> None:
    # Europe/Athens 2026-10-25 is 25 real hours long. A localize-then-
    # dedupe implementation collapses the repeated 03:xx wall-clock hour
    # and reports 24.
    instants = _local_day_hourly_utc_instants(
        datetime(2026, 10, 25), ZoneInfo("Europe/Athens")
    )
    assert len(instants) == 25  # fixture guard: the local day really has 25
    assert covered_hours(instants) == 25


def test_covered_hours_spring_forward_local_day_is_23() -> None:
    # Europe/Athens 2026-03-29 is 23 real hours long (03:00 EET jumps to
    # 04:00 EEST). Anything assuming 24 hours per local day overcounts.
    instants = _local_day_hourly_utc_instants(
        datetime(2026, 3, 29), ZoneInfo("Europe/Athens")
    )
    assert len(instants) == 23  # fixture guard: the skipped hour is absent
    assert covered_hours(instants) == 23


# ---------------------------------------------------------------------------
# clears_coverage — boundary pinned against MIN_COVERAGE_HOURS, not "18".
# ---------------------------------------------------------------------------


def test_clears_coverage_at_threshold_clears() -> None:
    assert clears_coverage(MIN_COVERAGE_HOURS) is True


def test_clears_coverage_one_below_threshold_does_not_clear() -> None:
    assert clears_coverage(MIN_COVERAGE_HOURS - 1) is False


def test_min_coverage_hours_constant_is_18() -> None:
    # Pin the constant's actual value once, explicitly, so the two boundary
    # tests above stay meaningful even if someone reads them in isolation.
    assert MIN_COVERAGE_HOURS == 18


# ---------------------------------------------------------------------------
# blend_mean
# ---------------------------------------------------------------------------


def test_blend_mean_empty_is_none() -> None:
    assert blend_mean([]) is None


def test_blend_mean_averages() -> None:
    assert blend_mean([1.0, 2.0, 3.0]) == 2.0


# ---------------------------------------------------------------------------
# wet_hours — inclusive `>= threshold` boundary (G-T15, successor to the
# `wet_share` boundary tests).
# ---------------------------------------------------------------------------


def test_wet_hours_value_exactly_at_threshold_counts_as_wet() -> None:
    count = wet_hours([0.2, 0.1], threshold_mm=0.2)
    assert count == 1


def test_wet_hours_value_just_below_threshold_does_not_count() -> None:
    count = wet_hours([0.19, 0.1], threshold_mm=0.2)
    assert count == 0


def test_wet_hours_empty_is_none() -> None:
    assert wet_hours([], threshold_mm=0.2) is None


# ---------------------------------------------------------------------------
# wet_hours (blended across feeds by the caller, via blend_mean) — per-feed
# counting, proven distinct from a naive thresholded mean (G-T14, successor
# to `test_predicted_wet_hour_share_is_per_feed_averaged_not_pooled`). This
# keeps one feed's longer horizon from out-voting a shorter one, and proves
# the count is not a threshold applied to the hourly mean.
# ---------------------------------------------------------------------------


def test_wet_hours_count_is_not_a_thresholded_mean() -> None:
    # Feed A: 12 hours at 0.4mm (wet), 12 at 0.0mm (dry) -> count 12.
    # Feed B: 24 hours at 0.15mm (dry, below 0.2 threshold) -> count 0.
    threshold = 0.2
    feed_a = [0.4] * 12 + [0.0] * 12
    feed_b = [0.15] * 24

    count_a = wet_hours(feed_a, threshold_mm=threshold)
    count_b = wet_hours(feed_b, threshold_mm=threshold)
    assert count_a is not None
    assert count_b is not None
    assert count_a == 12  # fixture sanity check
    assert count_b == 0  # fixture sanity check

    # The shipped value, from the production entry point (`displayed_daily`),
    # not re-derived inside the test.
    shipped = displayed_daily("precip", [feed_a, feed_b], rain_threshold_mm=threshold)
    assert shipped["wet_hours"] == 6.0

    # A thresholded-mean implementation would instead blend the raw hourly
    # values first, hour by hour, and threshold THAT mean: hours 0-11 blend
    # to (0.4 + 0.15) / 2 == 0.275mm (>= threshold, wet), hours 12-23 blend
    # to (0.0 + 0.15) / 2 == 0.075mm (dry) -- computed independently here,
    # inside the test, from the same two feeds.
    hourly_blend = [(a + b) / 2 for a, b in zip(feed_a, feed_b, strict=True)]
    thresholded_mean_count = sum(1 for v in hourly_blend if v >= threshold)
    assert shipped["wet_hours"] != thresholded_mean_count
    assert thresholded_mean_count == 12  # hours 0-11 alone clear the threshold


def test_wet_hours_blend_empty_is_none() -> None:
    assert blend_mean([]) is None


# ---------------------------------------------------------------------------
# fixed_membership_series -- null at any instant a member does not supply,
# never renormalised over the members present (G-T12's underlying rule,
# proven directly here rather than through the drill-down's rendered
# series -- see test_aggregate_null_axis_instant_and_total_agrees_with_tile).
# ---------------------------------------------------------------------------


def test_fixed_membership_series_gap_in_one_member_is_null_not_the_other_value() -> (
    None
):
    # Member "a" has a nonzero value at the instant; member "b" lacks it
    # entirely. A renormalising bug would fall back to "a"'s lone value
    # (1.0); the fixed-membership rule requires null instead.
    result = fixed_membership_series(["h0"], [{"h0": 1.0}, {}])
    assert result == [None]
    assert result != [1.0]


def test_fixed_membership_series_both_members_present_is_their_blend() -> None:
    # Paired positive for the gap case above: when every member supplies
    # the instant, the series is not null -- it is their blend_mean.
    result = fixed_membership_series(
        ["h0", "h1"], [{"h0": 1.0, "h1": 3.0}, {"h0": 2.0}]
    )
    assert result == [1.5, None]


def test_fixed_membership_series_empty_members_is_all_null() -> None:
    assert fixed_membership_series(["h0", "h1"], []) == [None, None]
