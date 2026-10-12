"""§15.2 rows 13-21: the rain-truth event/amount/correction rules (§6.6, §6.7).

Expected values are derived directly from the plan's §15.2 table text and
from `rain_nearby_event`/`observed_amount`/`high_reading_flags`/
`station_day_total`'s own documented rules -- never read off a run of the
implementation. All station ids are synthetic (`SYN-`); all dates use the
module's own `date` objects, no real calendar meaning is relied on.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from wxverify.forecast.daily_method import (
    RainReading,
    StationDay,
    high_reading_flags,
    observed_amount,
    rain_nearby_event,
    station_day_total,
)


def _station(sid: str, mm: float, hours: int) -> StationDay:
    return StationDay(
        station_id=sid,
        hundredths=round(mm * 100),
        hours=hours,
        invalid=False,
        quarantined=False,
    )


def test_event_exact_sum_float_trap() -> None:
    """ "event, exact sum": the plan's own readings, 0.05, 0.05 and
    0.09999999999999996, sum in IEEE-754 float to two ULPs below 0.2 mm,
    whose *100 is 19.999999999999996 -- not an integer, so `round()` and
    `int()` give different hundredths (20 vs 19). The sum must pass through
    `station_day_total` -- the function that actually calls `hundredths()`
    on a raw float total (`StationDayTotal.hundredths=hundredths(total)`)
    -- not a hand-rounded fixture value, or a mutant in `hundredths()`
    itself (M42: truncating instead of rounding) goes unseen.

    `round(19.999999999999996) == 20`, so SYN-1 clears the 0.2 mm pair line
    exactly; with SYN-2 at 0.25 mm two stations are >= 0.2 mm -> rain
    nearby, trigger "two_at_0.2". `int(19.999999999999996) == 19` instead
    stays below the line, which is the "M42 gives dry" the plan names.
    """
    a, b, c = 0.05, 0.05, 0.09999999999999996
    two_ulps_below_02 = a + b + c
    assert two_ulps_below_02 * 100 == 19.999999999999996  # noqa: PLR2004
    readings = [
        RainReading(
            hour=datetime(2035, 1, 1, h), value=mm, source_raw=None, fetched_at=None
        )
        for h, mm in enumerate([a, b, c])
    ]
    syn1_total = station_day_total(readings, expected_hours=24, gap_hours=6)
    assert syn1_total.total == two_ulps_below_02
    assert syn1_total.hundredths == 20
    syn1 = StationDay(
        station_id="SYN-1",
        hundredths=syn1_total.hundredths,
        hours=24,
        invalid=False,
        quarantined=False,
    )
    syn2 = _station("SYN-2", 0.25, 24)
    syn3 = _station("SYN-3", 0.0, 24)
    event = rain_nearby_event(
        [syn1, syn2, syn3],
        min_hours=1,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert event.outcome == "rain_nearby"
    assert event.trigger == "two_at_0.2"


def test_event_one_alone_below_half_mm_is_dry() -> None:
    """Row 14: one station at 0.25 mm, two at 0.0 -- below the 0.5 mm line."""
    stations = [
        _station("SYN-1", 0.25, 24),
        _station("SYN-2", 0.0, 24),
        _station("SYN-3", 0.0, 24),
    ]
    event = rain_nearby_event(
        stations,
        min_hours=1,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert event.outcome == "dry"


def test_event_one_at_half_mm_then_a_second_pushes_both() -> None:
    """Row 15: SYN-1 at 0.5 mm alone -> one_at_0.5; adding SYN-2 at 0.25 -> both."""
    base = [
        _station("SYN-1", 0.5, 24),
        _station("SYN-2", 0.0, 24),
        _station("SYN-3", 0.0, 24),
    ]
    first = rain_nearby_event(
        base,
        min_hours=1,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert first.outcome == "rain_nearby"
    assert first.trigger == "one_at_0.5"

    with_second = [
        _station("SYN-1", 0.5, 24),
        _station("SYN-2", 0.25, 24),
        _station("SYN-3", 0.0, 24),
    ]
    second = rain_nearby_event(
        with_second,
        min_hours=1,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert second.outcome == "rain_nearby"
    assert second.trigger == "both"


def test_event_coverage_partial_hours_still_count_for_rain_nearby() -> None:
    """Row 16: min_hours=22. SYN-1 0.5mm/21h, SYN-2..4 0.0mm/24h.

    SYN-1 is short of min_hours (21 < 22) but StationDay.counted only needs
    hours>=1, so it still votes toward rain nearby (one_at_0.5) and lands in
    ``partial``, not ``adequate``. observed_amount then has no station that
    is both adequate (hours>=22) and >=wet_hundredths -> amount_not_available.
    Raising SYN-1 to the row's 22 hours clears min_hours, so it becomes the
    sole contributing station and the amount is its own 0.5 mm.
    """
    stations_21h = [
        _station("SYN-1", 0.5, 21),
        _station("SYN-2", 0.0, 24),
        _station("SYN-3", 0.0, 24),
        _station("SYN-4", 0.0, 24),
    ]
    event = rain_nearby_event(
        stations_21h,
        min_hours=22,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert event.outcome == "rain_nearby"
    assert event.trigger == "one_at_0.5"
    assert event.partial == ("SYN-1",)
    assert event.adequate == ("SYN-2", "SYN-3", "SYN-4")
    amount = observed_amount(
        stations_21h, outcome="rain_nearby", min_hours=22, wet_hundredths=50
    )
    assert amount.amount is None
    assert amount.reason == "amount_not_available"

    stations_22h = [
        _station("SYN-1", 0.5, 22),
        _station("SYN-2", 0.0, 24),
        _station("SYN-3", 0.0, 24),
        _station("SYN-4", 0.0, 24),
    ]
    event_22 = rain_nearby_event(
        stations_22h,
        min_hours=22,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert event_22.outcome == "rain_nearby"
    amount_22 = observed_amount(
        stations_22h, outcome="rain_nearby", min_hours=22, wet_hundredths=50
    )
    assert amount_22.amount == pytest.approx(0.5)  # pyright: ignore[reportUnknownMemberType]


def test_dry_needs_one_covered_station() -> None:
    """Row 17: min_dry_stations=1, min_hours=22.

    SYN-1 at 0.0mm/22h meets min_hours; SYN-2/SYN-3 at 21h do not. One
    adequate station is enough to certify dry (min_dry_stations=1) -> dry.
    With all three dropped to 21 hours, none meet min_hours=22, so there is
    no adequate station at all -> unknown, not dry.
    """
    one_adequate = [
        _station("SYN-1", 0.0, 22),
        _station("SYN-2", 0.0, 21),
        _station("SYN-3", 0.0, 21),
    ]
    event_dry = rain_nearby_event(
        one_adequate,
        min_hours=22,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert event_dry.outcome == "dry"

    none_adequate = [
        _station("SYN-1", 0.0, 21),
        _station("SYN-2", 0.0, 21),
        _station("SYN-3", 0.0, 21),
    ]
    event_unknown = rain_nearby_event(
        none_adequate,
        min_hours=22,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert event_unknown.outcome == "unknown"


def test_one_covered_station_a_partial_light_reading() -> None:
    """Row 18: SYN-1 at 0.25 mm/24h; SYN-2 at 0.25 mm/21h; SYN-3 0.0 mm/21h.

    Two stations >= 0.2 mm -> two_at_0.2; only SYN-1 meets min_hours=24, so
    the amount comes from SYN-1 alone: 0.25 mm. With SYN-2 dropped to 0.0,
    SYN-1 is the lone counted station >= 0.5 mm... but 0.25 mm is below the
    0.5 mm single line, so lone_reading needs wet_hundredths=25 to flag it
    at all; using that line, SYN-1 is flagged lone_reading and the event is
    dry (0.25 mm never clears the 0.5 mm single trigger, and a solitary
    0.2 mm-line station does not meet the two-station rule alone).
    """
    stations = [
        _station("SYN-1", 0.25, 24),
        _station("SYN-2", 0.25, 21),
        _station("SYN-3", 0.0, 21),
    ]
    event = rain_nearby_event(
        stations,
        min_hours=24,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert event.outcome == "rain_nearby"
    assert event.trigger == "two_at_0.2"
    amount = observed_amount(
        stations, outcome="rain_nearby", min_hours=24, wet_hundredths=20
    )
    assert amount.amount == pytest.approx(0.25)  # pyright: ignore[reportUnknownMemberType]
    assert amount.contributing == (("SYN-1", 25, 24),)

    dropped = [
        _station("SYN-1", 0.25, 24),
        _station("SYN-2", 0.0, 21),
        _station("SYN-3", 0.0, 21),
    ]
    flags = high_reading_flags(dropped, wet_hundredths=25, large_factor=2, min_hours=24)
    assert flags.lone_reading == "SYN-1"
    event_dry = rain_nearby_event(
        dropped,
        min_hours=24,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert event_dry.outcome == "dry"
    amount_dry = observed_amount(
        dropped, outcome="dry", min_hours=24, wet_hundredths=25
    )
    assert amount_dry.amount == pytest.approx(0.0)  # pyright: ignore[reportUnknownMemberType]
    # SYN-2/SYN-3 are counted but short of min_hours -> partial, not absent.
    assert amount_dry.partial == (("SYN-2", 0, 21), ("SYN-3", 0, 21))
    # SYN-1's 0.25 mm reading is still visible in the station-day output
    # even though the day is dry -- "kept", never erased by the flag.
    assert amount_dry.contributing == (("SYN-1", 25, 24),)


def test_event_partial_and_adequate_wet_stations() -> None:
    """Row 19: SYN-1 0.5mm/21h, SYN-2 0.3mm/24h, SYN-3 0.0mm/24h, min_hours=24.

    Both SYN-1 (>=0.5) and SYN-2+SYN-1 (>=0.2, two stations) trigger
    "both"; only SYN-2 meets min_hours -> amount from SYN-2 alone, 0.3 mm
    (not the two-largest average (50+30)/200 = 0.4, since SYN-1 is partial).
    """
    stations = [
        _station("SYN-1", 0.5, 21),
        _station("SYN-2", 0.3, 24),
        _station("SYN-3", 0.0, 24),
    ]
    event = rain_nearby_event(
        stations,
        min_hours=24,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert event.outcome == "rain_nearby"
    assert event.trigger == "both"
    amount = observed_amount(
        stations, outcome="rain_nearby", min_hours=24, wet_hundredths=20
    )
    assert amount.amount == pytest.approx(0.3)  # pyright: ignore[reportUnknownMemberType]
    assert amount.contributing == (("SYN-2", 30, 24),)


def test_lone_reading_flagged_and_kept() -> None:
    """Row 20: SYN-1 0.5mm, SYN-2/SYN-3 0.0mm, all 24h, large_factor=2.

    Exactly one counted station >= wet_hundredths=50 -> lone_reading; with
    only one contributing station there is no second to compare for
    large_total. The event is rain_nearby, one_at_0.5, amount 0.5 mm (from
    SYN-1 alone, never dropped for being lone).
    """
    stations = [
        _station("SYN-1", 0.5, 24),
        _station("SYN-2", 0.0, 24),
        _station("SYN-3", 0.0, 24),
    ]
    flags = high_reading_flags(
        stations, wet_hundredths=50, large_factor=2, min_hours=24
    )
    assert flags.lone_reading == "SYN-1"
    assert flags.large_total is None
    event = rain_nearby_event(
        stations,
        min_hours=24,
        pair_hundredths=20,
        single_hundredths=50,
        min_dry_stations=1,
    )
    assert event.outcome == "rain_nearby"
    assert event.trigger == "one_at_0.5"
    amount = observed_amount(
        stations, outcome="rain_nearby", min_hours=24, wet_hundredths=50
    )
    assert amount.amount == pytest.approx(0.5)  # pyright: ignore[reportUnknownMemberType]


def test_station_day_correction_from_running_total() -> None:
    """Row 21: increasing running totals reconcile; a drop is counter_reset.

    First day: increments 0.25/0.25/0.25 (sum 0.75) against running totals
    0.25, 0.50, 0.50. The last running total (0.50) differs from the
    0.75 sum by more than 0.01 mm -> "unreconciled"; running totals never
    go down -> correction "from_running_total", total becomes 0.50, valid.
    Second day: running totals 0.50 then 0.25 -- a drop -> "counter_reset",
    which is not a lone "unreconciled" problem set, so the day stays
    invalid with no correction.
    """
    good_day = [
        RainReading(
            hour=datetime(2035, 1, 1, h),
            value=0.25,
            source_raw=f"{total} mm precipTotal",
            fetched_at=None,
        )
        for h, total in [(0, 0.25), (1, 0.50), (2, 0.50)]
    ]
    result = station_day_total(good_day, expected_hours=3, gap_hours=2)
    assert result.total == pytest.approx(0.50)  # pyright: ignore[reportUnknownMemberType]
    assert result.correction == "from_running_total"
    assert result.invalid is False
    assert result.problems == ("unreconciled",)

    reset_day = [
        RainReading(
            hour=datetime(2035, 1, 1, h),
            value=0.0,
            source_raw=f"{total} mm precipTotal",
            fetched_at=None,
        )
        for h, total in [(0, 0.50), (1, 0.25)]
    ]
    reset_result = station_day_total(reset_day, expected_hours=2, gap_hours=2)
    assert "counter_reset" in reset_result.problems
    assert reset_result.correction is None
    assert reset_result.invalid is True


@pytest.mark.parametrize(
    ("source_raw", "value", "expected_hundredths"),
    [
        pytest.param("0.0 mm precipTotal", 0.0, 0, id="accepted-0.0"),
        pytest.param("-0.0 mm precipTotal", -0.0, 0, id="accepted-neg-0.0"),
        pytest.param("1e-05 mm precipTotal", 1e-05, 0, id="accepted-1e-05"),
        pytest.param(
            "0.30000000000000004 mm precipTotal",
            0.30000000000000004,
            30,
            id="accepted-0.3-float-trap",
        ),
        pytest.param("16.41 mm precipTotal", 16.41, 1641, id="accepted-16.41"),
        pytest.param("500.0 mm precipTotal", 500.0, 50000, id="accepted-500-cap"),
        pytest.param("1e+16 mm precipTotal", 1e16, None, id="refused-1e16-cap"),
        pytest.param("-1.0 mm precipTotal", 0.0, None, id="refused-negative-total"),
        pytest.param("nan mm precipTotal", 0.0, None, id="refused-nan"),
        pytest.param("inf mm precipTotal", 0.0, None, id="refused-inf"),
        pytest.param(
            "1.0   mm precipTotal",
            0.0,
            None,
            id="refused-two-trailing-spaces",
        ),
        pytest.param(" 1.0 mm precipTotal", 0.0, None, id="refused-leading-space"),
        pytest.param("1.0  mm precipTotal", 0.0, None, id="refused-one-trailing-space"),
        pytest.param("1_000 mm precipTotal", 0.0, None, id="refused-underscore"),
        pytest.param("+1.0 mm precipTotal", 0.0, None, id="refused-plus-sign"),
        pytest.param("1e-5 mm precipTotal", 0.0, None, id="refused-short-exponent"),
        pytest.param("01.0 mm precipTotal", 0.0, None, id="refused-leading-zero"),
        pytest.param("", 0.0, None, id="refused-empty-string-as-whole"),
        pytest.param("1.0 mm", 0.0, None, id="refused-1.0-mm-as-whole"),
        pytest.param(" mm precipTotal", 0.0, None, id="refused-empty-formatted"),
        pytest.param("1.0 mm mm precipTotal", 0.0, None, id="refused-1.0mm-formatted"),
    ],
)
def test_station_day_total_running_total_text_rule(
    source_raw: str, value: float, expected_hundredths: int | None
) -> None:
    """Row 21 / #115 §19.4: the running-total text rule, one reading alone.

    The contract (forecast-method plan line 385, time-alignment plan
    §10.4.3 step 1): a reading's ``source_raw`` must parse as exactly
    ``"<number> mm precipTotal"``, with the number's own ``str()`` text
    matching the number text exactly, finite and >= 0, or the day carries
    ``unparsed_total``; the same problem covers a parsed total above the
    500 mm cap. The accepted hundredths are #115 §19.4's own T1 values for
    the same strings: 0, 0, 0, 30, 1641, 50000.

    With only one reading, no second running total exists to drop
    (``counter_reset``) or disagree with the increment sum
    (``unreconciled``), and ``fetched_at`` is ``None`` so
    ``future_timestamp`` cannot fire -- ``unparsed_total`` is the only
    problem either rule can add.
    """
    reading = RainReading(
        hour=datetime(2035, 1, 1, 0),
        value=value,
        source_raw=source_raw,
        fetched_at=None,
    )
    result = station_day_total([reading], expected_hours=1, gap_hours=2)
    if expected_hundredths is None:
        assert result.problems == ("unparsed_total",)
        assert result.invalid is True
    else:
        assert result.hundredths == expected_hundredths
        assert result.problems == ()
        assert result.correction is None
        assert result.invalid is False


@pytest.mark.parametrize(
    ("source_raw", "expected_hundredths"),
    [
        pytest.param("1.015 mm precipTotal", 102, id="corrected-half-even-1.015"),
        pytest.param("0.575 mm precipTotal", 58, id="corrected-half-even-0.575"),
        pytest.param("0.015 mm precipTotal", 2, id="corrected-half-even-0.015"),
        pytest.param("0.025 mm precipTotal", 2, id="corrected-half-even-0.025"),
        pytest.param("16.41 mm precipTotal", 1641, id="corrected-half-even-16.41"),
        pytest.param("500.0 mm precipTotal", 50000, id="corrected-half-even-500.0"),
    ],
)
def test_station_day_total_correction_reads_decimal_half_even(
    source_raw: str, expected_hundredths: int
) -> None:
    """A corrected day takes the running total's own half-even hundredths.

    One reading, value 0.0, whose running-total text disagrees with the
    (zero) increment sum by more than the running total itself -- the only
    problem is ``unreconciled``, the running total never goes down (one
    value, trivially), so the day is corrected to ``from_running_total``
    and its total is exactly the running-total text read as a float. The
    hundredths pinned here are the text's own half-even decimal reading,
    not a float ``round(total * 100)`` of that same number -- the two
    disagree for "1.015" (102 vs 101) and "0.575" (58 vs 57).
    """
    number_text = source_raw.split(" ", 1)[0]
    reading = RainReading(
        hour=datetime(2035, 1, 1, 0),
        value=0.0,
        source_raw=source_raw,
        fetched_at=None,
    )
    result = station_day_total([reading], expected_hours=1, gap_hours=2)
    assert result.problems == ("unreconciled",)
    assert result.correction == "from_running_total"
    assert result.invalid is False
    assert result.total == float(number_text)
    assert result.hundredths == expected_hundredths


def test_station_day_total_counter_reset_uses_decimal_rounding() -> None:
    """A counter reset is caught by the half-even reading, not float rounding.

    The running totals "1.015" then "1.0" read as half-even hundredths 102
    then 100, a drop of 2 -- above the 1-hundredth tolerance, so
    ``counter_reset`` fires. A float ``round(total * 100)`` of the same two
    numbers gives 101 then 100, a drop of only 1, which would not fire.
    """
    readings = [
        RainReading(
            hour=datetime(2035, 1, 1, 0),
            value=1.0,
            source_raw="1.015 mm precipTotal",
            fetched_at=None,
        ),
        RainReading(
            hour=datetime(2035, 1, 1, 1),
            value=0.0,
            source_raw="1.0 mm precipTotal",
            fetched_at=None,
        ),
    ]
    result = station_day_total(readings, expected_hours=2, gap_hours=2)
    assert result.problems == ("counter_reset",)
    assert result.correction is None
    assert result.invalid is True


def test_station_day_total_small_increment_mismatch_is_unreconciled() -> None:
    """A 0.015 mm gap between the increment sum and the running total corrects.

    The single reading's increment (1.0 mm) misses its own running total
    ("1.015 mm precipTotal") by 0.015 mm -- more than the 0.01 mm
    tolerance, so the day is ``unreconciled`` and corrected to the running
    total, whose half-even hundredths is 102.
    """
    reading = RainReading(
        hour=datetime(2035, 1, 1, 0),
        value=1.0,
        source_raw="1.015 mm precipTotal",
        fetched_at=None,
    )
    result = station_day_total([reading], expected_hours=1, gap_hours=2)
    assert result.problems == ("unreconciled",)
    assert result.correction == "from_running_total"
    assert result.total == 1.015
    assert result.hundredths == 102
    assert result.invalid is False


@pytest.mark.parametrize(
    ("source_raw", "value"),
    [
        pytest.param("500.01 mm precipTotal", 500.01, id="refused-above-cap-500.01"),
        pytest.param(
            "500.004 mm precipTotal",
            500.004,
            id="refused-above-cap-before-rounding",
        ),
        pytest.param("500 mm precipTotal", 500.0, id="refused-500-str-mismatch"),
        pytest.param(None, 0.0, id="refused-no-source-raw"),
    ],
)
def test_station_day_total_cap_and_odd_texts_are_unparsed(
    source_raw: str | None, value: float
) -> None:
    """The 500 mm cap and malformed running-total texts are ``unparsed_total``.

    "500.01" is refused by the plain 500 mm cap; "500.004" is above 500 mm
    before any rounding (its half-even hundredths, 50000, would otherwise
    look acceptable, so the cap must be checked on the float, not the
    rounded hundredths); "500" is refused because ``str(500.0)`` is
    "500.0", not "500"; and a ``None`` running total is refused outright.
    """
    reading = RainReading(
        hour=datetime(2035, 1, 1, 0),
        value=value,
        source_raw=source_raw,
        fetched_at=None,
    )
    result = station_day_total([reading], expected_hours=1, gap_hours=2)
    assert result.problems == ("unparsed_total",)
    assert result.invalid is True
