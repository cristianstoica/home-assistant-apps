"""Unit tests for ``wxverify.forecast.aggregate`` and ``core.units.ms_to_kmh``.

This file covers ms_to_kmh conversion,
precip total (with a stray negative filtered out), chance-of-rain at the
threshold boundary, and a partially-covered day flagged 'partial'.

The stray-negative-filtered and partial-day-flagged parts of that are
DB-facing (the negative is dropped by `invalid_forecast_sample_sql` in the
data layer, and the "partial" badge is assembled in the service layer from
`clears_coverage`) — those live in test_forecast_data.py / test_forecast_service.py.
This file owns everything pure: `ms_to_kmh`, `display_day_index` (the display
half of the day-boundary gate), `covered_hours`, `clears_coverage` (pinned
against `MIN_COVERAGE_HOURS`, not a hardcoded 18), `blend_mean`, `wet_share`
(the rain-threshold inclusive boundary), and `predicted_wet_hour_share` (per-feed
average, proven distinct from a naive pooled share).

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
    predicted_wet_hour_share,
    wet_share,
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
# wet_share — inclusive `>= threshold` boundary.
# ---------------------------------------------------------------------------


def test_wet_share_value_exactly_at_threshold_counts_as_wet() -> None:
    share = wet_share([0.2, 0.1], threshold_mm=0.2)
    assert share == 0.5


def test_wet_share_value_just_below_threshold_does_not_count() -> None:
    share = wet_share([0.19, 0.1], threshold_mm=0.2)
    assert share == 0.0


def test_wet_share_empty_is_none() -> None:
    assert wet_share([], threshold_mm=0.2) is None


# ---------------------------------------------------------------------------
# predicted_wet_hour_share — equal-weight average of PER-FEED shares, proven distinct
# from a naive pooled share (this keeps one feed's longer horizon from
# out-voting a shorter one).
# ---------------------------------------------------------------------------


def test_predicted_wet_hour_share_is_per_feed_averaged_not_pooled() -> None:
    # Feed A: 2 of 2 covered hours wet -> share 1.0.
    # Feed B: 1 of 4 covered hours wet -> share 0.25.
    feed_a = [0.5, 0.5]
    feed_b = [0.5, 0.0, 0.0, 0.0]
    threshold = 0.2

    share_a = wet_share(feed_a, threshold_mm=threshold)
    share_b = wet_share(feed_b, threshold_mm=threshold)
    assert share_a is not None
    assert share_b is not None

    per_feed_averaged = predicted_wet_hour_share([share_a, share_b])
    # Naive pooled share across all 6 raw hourly slots (3 wet of 6): what a
    # POOLED implementation would (wrongly) produce.
    pooled = 3 / 6
    assert per_feed_averaged == 0.625
    assert per_feed_averaged != pooled


def test_predicted_wet_hour_share_empty_is_none() -> None:
    assert predicted_wet_hour_share([]) is None
