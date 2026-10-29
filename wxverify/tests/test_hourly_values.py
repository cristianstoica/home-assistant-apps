"""The pure hourly-value method (plan 2026-10-21-daily-close, 7, 14.4
HV1-HV13).

``obs/hourly_values.py`` is pure: no database, no clock, no network. It
builds one station's hourly temperature, rain and wind values from its raw
readings of every source, for one site-day.
"""

from __future__ import annotations

import ast
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import wxverify.obs.hourly_values as wxverify_hourly_values
from wxverify.obs.hourly_values import (
    Source,
    StationHours,
    StationReading,
    day_hours,
    station_hours,
)


def _hour(hour: int, *, day: int = 27) -> datetime:
    return datetime(2026, 10, day, hour, 0, tzinfo=UTC)


def _reading(
    hour: int,
    minute: int,
    *,
    second: int = 0,
    day: int = 27,
    source: Source = "five_minute",
    local_date: date | None = None,
    temp_c: float | None = None,
    precip_total_mm: float | None = None,
    speed_kmh: float | None = None,
) -> StationReading:
    return StationReading(
        obs_at=datetime(2026, 10, day, hour, minute, second, tzinfo=UTC),
        source=source,
        local_date=local_date or date(2026, 10, day),
        temp_c=temp_c,
        precip_total_mm=precip_total_mm,
        speed_kmh=speed_kmh,
    )


# --- HV1 ---------------------------------------------------------------


def test_hv1_temperature_is_plain_mean_absent_hours_have_no_key() -> None:
    """HV1 (plan 14.4): 10/11/12 avg in H, one 20.0 reading in H+1, none in
    H+2.

    No mutant is named for HV1 in 15.2; it pins the plain-mean/no-minimum-
    count rule of 7.2 directly: one reading is enough, an empty hour is
    absent, not 0.
    """
    h0 = _hour(10)
    readings = [
        _reading(10, 5, temp_c=10.0),
        _reading(10, 25, temp_c=11.0),
        _reading(10, 45, temp_c=12.0),
        _reading(11, 10, temp_c=20.0),
    ]
    result = station_hours(
        readings,
        day_start_utc=h0,
        day_end_utc=h0 + timedelta(hours=3),
        local_date=date(2026, 10, 27),
    )
    temps = result.values["temperature"]
    assert temps[h0].value == pytest.approx(11.0)
    assert temps[h0].source == "five_minute"
    assert temps[h0].readings == 3
    assert temps[h0 + timedelta(hours=1)].value == pytest.approx(20.0)
    assert (h0 + timedelta(hours=2)) not in temps


# --- HV2 ---------------------------------------------------------------


def test_hv2_source_order_is_per_quantity_not_per_hour() -> None:
    """HV2 (plan 14.4): per-quantity source order; mixed five_minute and
    current_obs readings in one hour.

    mutant_co_before_five_minute (M12) -> at the H0 temperature assertion:
    correct = 11.0 (the five_minute mean), mutant (current_obs tried
    first) = 99.0 (the current_obs reading's temperature). The fixture's
    current_obs reading in H0 carries a temperature that differs from the
    five_minute mean, so the two literals diverge.
    mutant_one_source_per_hour (M13) -> at the H0 rain assertion: correct =
    0.4 (H0's five_minute readings carry no rain, so rain falls back to
    the current_obs reading's total), mutant (one source chosen for the
    WHOLE hour, so an hour with any five_minute reading never consults
    current_obs for a quantity it lacks) = H0 absent from the rain dict.
    H0 has a five_minute reading (not a plain "no five_minute" gap) yet
    still needs current_obs for rain alone, so only a per-quantity order
    gives it a value.
    """
    h0 = _hour(12)
    h1 = h0 + timedelta(hours=1)
    h2 = h0 + timedelta(hours=2)
    readings = [
        _reading(12, 5, temp_c=10.0),
        _reading(12, 10, temp_c=12.0),
        _reading(12, 20, source="current_obs", temp_c=99.0, precip_total_mm=0.4),
        _reading(13, 15, source="current_obs", temp_c=15.0, precip_total_mm=0.7),
        _reading(14, 5, temp_c=20.0),
        _reading(14, 10, temp_c=22.0),
        _reading(14, 20, source="current_obs", precip_total_mm=0.9),
    ]
    result = station_hours(
        readings,
        day_start_utc=h0,
        day_end_utc=h0 + timedelta(hours=3),
        local_date=date(2026, 10, 27),
    )
    temps = result.values["temperature"]
    rain = result.values["precip"]
    assert temps[h0].value == pytest.approx(11.0)
    assert temps[h0].source == "five_minute"
    assert rain[h0].value == pytest.approx(0.4)
    assert rain[h0].source == "current_obs"
    assert temps[h1].value == pytest.approx(15.0)
    assert temps[h1].source == "current_obs"
    assert rain[h1].value == pytest.approx(0.3)
    assert temps[h2].value == pytest.approx(21.0)
    assert temps[h2].source == "five_minute"
    assert rain[h2].value == pytest.approx(0.2)
    assert rain[h2].source == "current_obs"


# --- HV3 ---------------------------------------------------------------


def test_hv3_hourly_used_only_for_a_gap_hour() -> None:
    """HV3 (plan 14.4): an hourly reading in a gap hour is used; in a
    non-gap hour it is not, quantity by quantity.

    mutant_hourly_in_non_gap_hour (M14) -> at the N-hour rain assertion:
    correct = N absent from the rain dict (N is not a gap -- it has a
    current_obs reading -- so step 3 of 7.1 never runs for rain even
    though current_obs carries no rain total there), mutant (the gap check
    dropped) = N present, using the hourly reading's total as the rise
    from G's hourly-filled total. The fixture's N-hour current_obs reading
    deliberately carries no precip_total_mm, so only the gap check decides
    whether the hourly reading's rain is used.
    """
    g = _hour(15)
    n = g + timedelta(hours=1)
    readings = [
        _reading(15, 10, source="hourly", temp_c=50.0, precip_total_mm=0.6),
        _reading(16, 5, source="current_obs", temp_c=10.0),
        _reading(16, 10, source="hourly", temp_c=999.0, precip_total_mm=99.0),
    ]
    result = station_hours(
        readings,
        day_start_utc=g,
        day_end_utc=g + timedelta(hours=2),
        local_date=date(2026, 10, 27),
    )
    temps = result.values["temperature"]
    rain = result.values["precip"]
    assert temps[g].value == pytest.approx(50.0)
    assert temps[g].source == "hourly"
    assert rain[g].value == pytest.approx(0.6)
    assert rain[g].source == "hourly"
    assert temps[n].value == pytest.approx(10.0)
    assert temps[n].source == "current_obs"
    assert n not in rain


# --- HV4 ---------------------------------------------------------------


def test_hv4_days_first_hour_starts_rain_at_zero() -> None:
    """HV4 (plan 14.4): D0's rain starts at 0, never at a reading from
    before the day.

    mutant_prev_day_total (M15) -> at the D0 assertion: correct = 0.5 (D0's
    start is 0, as 5.4/7.3 require for the day's first hour: steps 0->0.2
    rise 0.2, 0.2->0.5 rise 0.3, sum 0.5), mutant (the first hour's start
    taken as the previous day's last total, 5.0) = 0.3 (the step 5.0->0.2
    is now a same-value fall, counted as 0, then the step 0.2->0.5 still
    adds 0.3). The fixture's pre-day reading sits in the `readings` passed
    to `station_hours` (the lookback window 8.1 allows for wind)
    specifically so a buggy implementation that reaches past the day's
    bounds for rain finds it; a correct one never consults any reading
    before `day_start_utc` for rain.
    """
    d0 = _hour(0, day=28)
    d0_plus_1 = d0 + timedelta(hours=1)
    readings = [
        _reading(23, 50, day=27, local_date=date(2026, 10, 27), precip_total_mm=5.0),
        _reading(0, 5, day=28, precip_total_mm=0.2),
        _reading(0, 40, day=28, precip_total_mm=0.5),
        _reading(1, 5, day=28, precip_total_mm=0.5),
        _reading(1, 40, day=28, precip_total_mm=0.9),
    ]
    result = station_hours(
        readings,
        day_start_utc=d0,
        day_end_utc=d0 + timedelta(hours=2),
        local_date=date(2026, 10, 28),
    )
    rain = result.values["precip"]
    assert rain[d0].value == pytest.approx(0.5)
    assert rain[d0_plus_1].value == pytest.approx(0.4)


# --- HV5 ---------------------------------------------------------------


def test_hv5_same_date_fall_gives_a_zero_step_not_a_reset() -> None:
    """HV5 (plan 14.4): a same-date fall is a step of 0, not a running-
    total reset.

    mutant_fall_is_reset (M16) -> at this assertion: correct = 0.3 (the
    step 2.0->1.8 is a fall, counted as 0; the step 1.8->2.1 is a rise of
    0.3; sum 0.3), mutant (a fall treated as a reset, so the fallen
    reading's step becomes its own total instead of 0) = 2.1 (1.8 + 0.3).
    The fixture's fall (2.0 -> 1.8) is on the SAME local date, so 7.3's
    reset rule (a restart is a change of local date) must not fire, and
    only the two literals' different treatment of the fall distinguishes
    them.
    """
    h_minus_1 = _hour(5)
    h = h_minus_1 + timedelta(hours=1)
    readings = [
        _reading(5, 40, precip_total_mm=2.0),
        _reading(6, 5, precip_total_mm=1.8),
        _reading(6, 40, precip_total_mm=2.1),
    ]
    result = station_hours(
        readings,
        day_start_utc=h_minus_1,
        day_end_utc=h_minus_1 + timedelta(hours=2),
        local_date=date(2026, 10, 27),
    )
    rain = result.values["precip"]
    assert rain[h].value == pytest.approx(0.3)


# --- HV6 ---------------------------------------------------------------


def test_hv6_unfilled_gap_rain_is_absent_never_folded_forward() -> None:
    """HV6 (plan 14.4): an unfilled rain gap stays absent in both hours;
    once filled from the hourly download, the gap's hour becomes its
    neighbour's start.

    mutant_reach_past_immediate_predecessor (M17) -> at the H+1 assertion
    in scenario A: correct = H+1 absent (its immediate predecessor H has
    no rain reading, 7.3), mutant (the start of H+1 taken as the last rain
    reading of ANY age, not just H) = 0.6 (1.6 minus H-1's 1.0, reaching
    past the gap). The fixture's H has no rain reading at all, so only a
    buggy "any age" lookup can reach H-1's 1.0.
    mutant_missing_hour_gets_zero (M18) -> at the H assertion in scenario
    A: correct = H absent (no rain reading in H at all, unfilled), mutant
    (a missing rain hour written as 0.0) = 0.0 present. `in` distinguishes
    "absent" from "present with value 0.0", which `==` alone would not.
    """
    h_minus_1 = _hour(8)
    h = h_minus_1 + timedelta(hours=1)
    h_plus_1 = h + timedelta(hours=1)
    base_readings = [
        _reading(8, 40, precip_total_mm=1.0),
        _reading(10, 10, precip_total_mm=1.6),
    ]
    scenario_a = station_hours(
        base_readings,
        day_start_utc=h_minus_1,
        day_end_utc=h_minus_1 + timedelta(hours=3),
        local_date=date(2026, 10, 27),
    )
    rain_a = scenario_a.values["precip"]
    assert h not in rain_a
    assert h_plus_1 not in rain_a

    filled_readings = [
        *base_readings,
        _reading(9, 20, source="hourly", precip_total_mm=1.3),
    ]
    scenario_b = station_hours(
        filled_readings,
        day_start_utc=h_minus_1,
        day_end_utc=h_minus_1 + timedelta(hours=3),
        local_date=date(2026, 10, 27),
    )
    rain_b = scenario_b.values["precip"]
    assert rain_b[h].value == pytest.approx(0.3)
    assert rain_b[h].source == "hourly"
    assert rain_b[h_plus_1].value == pytest.approx(0.3)


# --- HV7 ---------------------------------------------------------------


def test_hv7_reading_with_mismatched_local_date_is_not_used_for_rain(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """HV7 (plan 14.4): a rain reading whose own local date is the day
    before is dropped for rain and logged once at WARNING; a reading with
    the site-day's own date is used normally (the paired positive).

    mutant_wrong_date_used (M19) -> at the H1 assertion: correct = H1
    absent (its only rain reading carries the PREVIOUS day's local date
    and is dropped, 7.3), mutant (readings of another local date used for
    rain) = 8.5 (9.0 minus H0's 0.5). H0's own reading carries the
    site-day's date and IS used (the paired positive, value 0.5), so only
    H1's date mismatch decides the two literals.
    """
    h0 = _hour(13)
    h1 = h0 + timedelta(hours=1)
    readings = [
        _reading(13, 5, local_date=date(2026, 10, 27), precip_total_mm=0.5),
        _reading(14, 5, local_date=date(2026, 10, 26), precip_total_mm=9.0),
    ]
    caplog.set_level("WARNING", logger="wxverify.obs.hourly_values")
    result = station_hours(
        readings,
        day_start_utc=h0,
        day_end_utc=h0 + timedelta(hours=2),
        local_date=date(2026, 10, 27),
    )
    rain = result.values["precip"]
    assert rain[h0].value == pytest.approx(0.5)
    assert h1 not in rain
    warnings = [
        r
        for r in caplog.records
        if r.name == "wxverify.obs.hourly_values" and r.levelname == "WARNING"
    ]
    assert len(warnings) == 1


# --- HV8 ---------------------------------------------------------------


def test_hv8_wind_pairs_across_midnight_hourly_never_pairs() -> None:
    """HV8 (plan 14.4): the lookback before the day's start still pairs; an
    hourly reading never joins the pair series.

    mutant_lookback_dropped (M21) -> at the H0 assertion: correct = 12.0
    (the previous-day reading 10 minutes before the day's start pairs with
    H0's first reading, mean (10+14)/2), mutant (readings before
    `day_start_utc` excluded from the pair series) = H0 absent (with the
    lookback reading gone, H0 has only one wind reading, which cannot pair
    with itself). H0 has no other reading to form a competing pair, so the
    lookback reading is the only thing that can give H0 a value at all.
    mutant_hourly_pairs (M20) -> at the H1 assertion: correct = 35.0 (the
    largest same-source pair in H1, (30+40)/2 from the two five_minute
    readings 15 minutes apart), mutant (hourly readings included in the
    pair series) = 519.5 ((40+999)/2, pairing the five_minute reading at
    H1's start with the hourly reading 5 minutes later). The fixture's
    hourly reading carries a wildly different speed (999.0) specifically
    so an illegitimate pair is unmistakably larger than the legitimate
    one.
    """
    h0 = _hour(20)
    h1 = h0 + timedelta(hours=1)
    readings = [
        _reading(19, 50, speed_kmh=10.0),
        _reading(20, 2, speed_kmh=14.0),
        _reading(20, 50, speed_kmh=30.0),
        _reading(21, 5, speed_kmh=40.0),
        _reading(21, 10, source="hourly", speed_kmh=999.0),
    ]
    result = station_hours(
        readings,
        day_start_utc=h0,
        day_end_utc=h0 + timedelta(hours=2),
        local_date=date(2026, 10, 27),
    )
    wind = result.values["wind"]
    assert wind[h0].value == pytest.approx(12.0)
    assert wind[h1].value == pytest.approx(35.0)


# --- HV9 ---------------------------------------------------------------


def test_hv9_hourly_wind_fills_a_no_pair_hour_never_a_paired_one() -> None:
    """HV9 (plan 14.4): the hourly wind fills an hour with no pair; it
    never overrides an hour that has one; with neither a pair nor an
    hourly reading the hour has no wind value (the paired negative).

    mutant_hourly_overrides_pair (M23) -> at the P-hour assertion: correct
    = 15.0 (P's five_minute pair, the two readings 3 minutes apart),
    mutant (the hourly wind replaces a pair value) = 999.0 (P's hourly
    reading, used even though P has a pair). P's hourly reading carries a
    wildly different speed specifically so an illegitimate override is
    unmistakable.
    mutant_no_pair_gets_no_hourly (M22) -> at the Q-hour assertion: correct
    = 50.0 (Q has no pair -- its single reading cannot pair with itself --
    so it takes its hourly reading's wind), mutant (a no-pair hour gets no
    hourly wind) = Q absent. R, which has neither a pair nor an hourly
    reading, stays absent in both the correct and the mutant run -- it is
    the paired negative showing Q's value comes from its hourly reading,
    not from some default.
    """
    p = _hour(0)
    q = _hour(3)
    r = _hour(6)
    readings = [
        _reading(0, 2, speed_kmh=14.0),
        _reading(0, 5, speed_kmh=16.0),
        _reading(0, 20, source="hourly", speed_kmh=999.0),
        _reading(3, 20, speed_kmh=10.0),
        _reading(3, 40, source="hourly", speed_kmh=50.0),
        _reading(6, 20, speed_kmh=20.0),
    ]
    result = station_hours(
        readings,
        day_start_utc=p,
        day_end_utc=p + timedelta(hours=7),
        local_date=date(2026, 10, 27),
    )
    wind = result.values["wind"]
    assert wind[p].value == pytest.approx(15.0)
    assert wind[p].source == "five_minute"
    assert wind[q].value == pytest.approx(50.0)
    assert wind[q].source == "hourly"
    assert r not in wind


# --- HV10 ----------------------------------------------------------------


def test_hv10_day_hours_is_the_days_own_span_not_a_fixed_24() -> None:
    """HV10 (plan 14.4): `day_hours` returns 23 UTC hours on a 23-hour day
    and 25 on a 25-hour day, as plain bounds (no `station_hours`).

    mutant_fixed_24 (M24) -> at both assertions: correct = 23 then 25 (the
    bounds given), mutant (the hours always 24 from the day's start) = 24
    in both cases. The fixture's two day lengths differ from each other
    and from 24, so the same mutant literal (24) diverges from both
    correct literals.
    """
    start_23 = datetime(2026, 3, 29, 0, 0, tzinfo=UTC)
    hours_23 = day_hours(start_23, start_23 + timedelta(hours=23))
    assert len(hours_23) == 23
    assert hours_23[0] == start_23
    assert hours_23[-1] == start_23 + timedelta(hours=22)

    start_25 = datetime(2026, 10, 25, 0, 0, tzinfo=UTC)
    hours_25 = day_hours(start_25, start_25 + timedelta(hours=25))
    assert len(hours_25) == 25
    assert hours_25[0] == start_25
    assert hours_25[-1] == start_25 + timedelta(hours=24)


# --- HV11 ----------------------------------------------------------------


def test_hv11_module_imports_only_stdlib_wind_pairs_and_core() -> None:
    """HV11 (plan 14.4): `obs/hourly_values.py` imports no database or
    worker module.

    mutant_db_import (M25) -> at this assertion: correct = set() (no
    disallowed module named), mutant (the module additionally imports
    `wxverify.db.connection`) = {"wxverify.db.connection"}. This assertion
    only distinguishes the two literals once the module exists.
    """
    hourly_values_file = wxverify_hourly_values.__file__
    assert hourly_values_file is not None
    module_path = Path(hourly_values_file).parent / "hourly_values.py"
    tree = ast.parse(module_path.read_text(), filename=str(module_path))
    disallowed: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                if name.startswith("wxverify") and not (
                    name == "wxverify.obs.wind_pairs"
                    or name.startswith("wxverify.core")
                ):
                    disallowed.add(name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module.startswith("wxverify") and not (
                module == "wxverify.obs.wind_pairs"
                or module.startswith("wxverify.core")
            ):
                disallowed.add(module)
    assert disallowed == set()


# --- HV12 ----------------------------------------------------------------


def test_hv12_last_hours_rain_uses_later_current_obs_readings_too() -> None:
    """HV12 (plan 14.4): in the day's last hour, rain adds only the
    current_obs readings later than the last five_minute one, clamped at
    0, and never a reading at or after the day's end; temperature and wind
    stay five_minute-only.

    mutant_later_readings_dropped (M26) -> at the (i) assertion: correct =
    0.9 (the rise to the last five_minute reading, 0.5, plus the added
    rain, 0.4), mutant (the later current_obs readings dropped in the
    day's last hour) = 0.5 (the rise alone). At the (iii) assertion:
    correct = 0.7 (0.5 plus 0.2, the 23:54 reading's added rain once the
    23:59 one is excluded for lying at the day's end), mutant = 0.5 again.
    Case (ii)'s 0.5 only pins the "never negative" clamp (7.3) -- it does
    not discriminate M26, since the clamped added rain there is 0 either
    way under both the correct code and this mutant.
    """
    prev = _hour(22)
    last = prev + timedelta(hours=1)
    day_end = last + timedelta(hours=1)
    base = [
        _reading(22, 40, precip_total_mm=2.5),
        _reading(23, 10, temp_c=9.0, speed_kmh=12.0, precip_total_mm=3.0),
        _reading(23, 15, temp_c=10.0, speed_kmh=16.0),
    ]

    def _run(later_readings: list[StationReading]) -> StationHours:
        return station_hours(
            [*base, *later_readings],
            day_start_utc=prev,
            day_end_utc=day_end,
            local_date=date(2026, 10, 27),
        )

    case_i = _run(
        [
            _reading(
                23,
                54,
                source="current_obs",
                temp_c=50.0,
                speed_kmh=999.0,
                precip_total_mm=3.2,
            ),
            _reading(
                23,
                59,
                source="current_obs",
                temp_c=60.0,
                speed_kmh=998.0,
                precip_total_mm=3.4,
            ),
        ]
    )
    rain_i = case_i.values["precip"]
    temps_i = case_i.values["temperature"]
    wind_i = case_i.values["wind"]
    assert rain_i[last].value == pytest.approx(0.9)
    assert rain_i[last].source == "five_minute+current_obs"
    assert temps_i[last].value == pytest.approx(9.5)
    assert temps_i[last].source == "five_minute"
    assert wind_i[last].value == pytest.approx(14.0)

    case_ii = _run(
        [
            _reading(23, 54, source="current_obs", precip_total_mm=3.2),
            _reading(23, 59, source="current_obs", precip_total_mm=2.9),
        ]
    )
    assert case_ii.values["precip"][last].value == pytest.approx(0.5)

    case_iii = _run(
        [
            _reading(23, 54, source="current_obs", precip_total_mm=3.2),
            _reading(0, 0, day=28, source="current_obs", precip_total_mm=3.9),
        ]
    )
    assert case_iii.values["precip"][last].value == pytest.approx(0.7)


# --- HV13 ----------------------------------------------------------------


def test_hv13_next_days_first_hour_never_inherits_the_old_days_total() -> None:
    """HV13 (plan 14.4): D's last hour has no later reading, so nothing is
    added past the rise to 23:45; the next day's first hour starts fresh
    at 0, never carrying the previous day's lookback reading into its rain
    chain.

    mutant_lookback_reading_used_for_rain -> at the next-day assertion:
    correct = 0.0 (the day's first hour starts at 0 regardless of any
    reading before `day_start_utc`), mutant (a reading from before the
    day's start folded into the first hour's rain chain because its own
    local date happens to match the new day) = 5.0 (the pre-day reading's
    5.0 rise, then a same-date fall to 0.0 counted as 0, giving 5.0). The
    lookback reading is deliberately tagged with the NEW day's local date,
    isolating the time boundary from the date check: the two literals
    diverge only if the implementation uses the date instead of
    `day_start_utc` to decide what belongs to the day. No mutant is named
    for HV13 in 15.2; it pins 7.6's last point directly.
    """
    day_a_start = _hour(23)
    day_a_end = day_a_start + timedelta(hours=1)
    day_a_readings = [_reading(23, 45, precip_total_mm=5.0)]
    day_a = station_hours(
        day_a_readings,
        day_start_utc=day_a_start,
        day_end_utc=day_a_end,
        local_date=date(2026, 10, 27),
    )
    rain_a = day_a.values["precip"]
    assert rain_a[day_a_start].value == pytest.approx(5.0)
    assert rain_a[day_a_start].source == "five_minute"

    day_b_start = day_a_end
    day_b_end = day_b_start + timedelta(hours=1)
    lookback_reading = _reading(
        23, 45, local_date=date(2026, 10, 28), precip_total_mm=5.0
    )
    day_b_own_reading = _reading(0, 0, day=28, precip_total_mm=0.0)
    day_b = station_hours(
        [lookback_reading, day_b_own_reading],
        day_start_utc=day_b_start,
        day_end_utc=day_b_end,
        local_date=date(2026, 10, 28),
    )
    rain_b = day_b.values["precip"]
    assert rain_b[day_b_start].value == pytest.approx(0.0)
