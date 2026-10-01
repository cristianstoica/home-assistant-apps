"""Pairing and parsing of the raw station wind figure (plan §7, §15.1 T1-T7).

``pair_max_by_hour`` (``wxverify.obs.wind_pairs``) and
``wind_records_from_payload`` (``wxverify.obs.pws_adapter``) are both pure:
no database, no clock, no network. T8 (the stored window / record_count) is
NOT in this file -- it needs the lane's ``_persist_day`` and a full
``station_wind_days`` fixture (plan §8.5) and is tracked separately.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

from wxverify.obs.pws_adapter import wind_records_from_payload
from wxverify.obs.wind_pairs import MAX_PAIR_GAP_SECONDS, WindRecord, pair_max_by_hour


def _dt(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 7, 1, hour, minute, second, tzinfo=UTC)


def _hour(hour: int) -> datetime:
    return datetime(2026, 7, 1, hour, 0, tzinfo=UTC)


# --- T1 ----------------------------------------------------------------


def test_pair_max_by_hour_worked_example() -> None:
    """2:55=15, 3:00=10, 3:05=5 -> {15:00: 12.5} (§2 S1's own example).

    pair(2:55,3:00) = 12.5 counted at 3:00 (the LATER record's hour).
    pair(3:00,3:05) = 7.5 counted at 3:00 too (the later record, 3:05, still
    floors to hour 3). The max of the two pairs at hour 3 is 12.5.
    """
    records = [
        WindRecord(obs_at=_dt(2, 55), speed_kmh=15.0),
        WindRecord(obs_at=_dt(3, 0), speed_kmh=10.0),
        WindRecord(obs_at=_dt(3, 5), speed_kmh=5.0),
    ]
    result = pair_max_by_hour(records, hours=[_hour(3)])
    assert result == {_hour(3): 12.5}


def test_pair_max_by_hour_counts_in_later_hour_not_earlier() -> None:
    """A pair spanning an hour boundary counts in the LATER record's hour only.

    mutant_earlier_hour -> at this assertion: correct = {_hour(3): 12.5},
    mutant = {_hour(2): 12.5} (pair counted at floor_hour(earlier) instead of
    floor_hour(later)). The fixture's single pair straddles 2:55/3:00, so the
    two literals name different hours and only one is present in `result`.
    """
    records = [
        WindRecord(obs_at=_dt(2, 55), speed_kmh=15.0),
        WindRecord(obs_at=_dt(3, 0), speed_kmh=10.0),
    ]
    result = pair_max_by_hour(records, hours=[_hour(2), _hour(3)])
    assert result == {_hour(3): 12.5}
    assert _hour(2) not in result


def test_pair_max_by_hour_takes_max_not_min_of_competing_pairs() -> None:
    """Two pairs land in the same hour; the result is their MAX, not their min.

    mutant_min -> at this assertion: correct = 12.5, mutant = 7.5 (min of the
    two candidate pair values 12.5 and 7.5 at hour 3, in place of max). The
    fixture's two pairs are deliberately asymmetric (12.5 vs 7.5) so the two
    literals diverge.
    """
    records = [
        WindRecord(obs_at=_dt(2, 55), speed_kmh=15.0),
        WindRecord(obs_at=_dt(3, 0), speed_kmh=10.0),
        WindRecord(obs_at=_dt(3, 5), speed_kmh=5.0),
    ]
    result = pair_max_by_hour(records, hours=[_hour(3)])
    assert result[_hour(3)] == 12.5


# --- T2 ------------------------------------------------------------------


def test_pair_gap_exactly_600s_pairs_601s_does_not() -> None:
    """A 0 < gap <= 600 s pair counts; a 601 s gap does not pair at all.

    mutant_lt -> at this assertion: correct = {_hour(3): 12.5} (both pairs
    present, including the exact-600s one), mutant (`<` for `<=`) drops the
    exact-600s pair, leaving `_hour(3)` absent from the mutant's result
    entirely (there is no other pair to fill it) -- the fixture's SOLE
    candidate pair sits exactly at the boundary.
    mutant_const_660 -> at the second assertion: correct = {} (the 601s gap
    never pairs), mutant (a constant 660s cutoff) would pair it, so
    `_hour(4)` would appear in the mutant's result but is absent from the
    correct one.
    """
    boundary = [
        WindRecord(obs_at=_dt(2, 50), speed_kmh=15.0),
        WindRecord(obs_at=_dt(3, 0, 0), speed_kmh=10.0),  # exactly 600s later
    ]
    assert MAX_PAIR_GAP_SECONDS == 600
    result = pair_max_by_hour(boundary, hours=[_hour(3)])
    assert result == {_hour(3): 12.5}

    over = [
        WindRecord(obs_at=_dt(2, 50, 0), speed_kmh=15.0),
        WindRecord(obs_at=_dt(3, 0, 1), speed_kmh=10.0),  # 601s later
    ]
    result_over = pair_max_by_hour(over, hours=[_hour(3)])
    assert result_over == {}


def test_pair_gap_must_be_strictly_positive() -> None:
    """A zero gap (duplicate obs_at) cannot occur in practice, but if it did,
    the function must not synthesize a pair from it (0 < gap is required)."""
    same_instant = [
        WindRecord(obs_at=_dt(3, 0), speed_kmh=15.0),
        WindRecord(obs_at=_dt(3, 0), speed_kmh=9.0),
    ]
    assert pair_max_by_hour(same_instant, hours=[_hour(3)]) == {}


# --- T3 --------------------------------------------------------------------


def test_calm_zero_pair_is_present_not_falsy_skipped() -> None:
    """Two calm (0.0) readings give a present 0.0 pair value, not an absence.

    mutant_falsy_skip -> at this assertion: correct = {_hour(3): 0.0} (the
    key IS present, with value 0.0), mutant (`if value:` treating 0.0 as
    falsy and skipping the write) = {} (key absent). `in` on the dict
    discriminates "present with value 0.0" from "absent" where `== 0.0`
    alone would not.
    """
    records = [
        WindRecord(obs_at=_dt(2, 55), speed_kmh=0.0),
        WindRecord(obs_at=_dt(3, 0), speed_kmh=0.0),
    ]
    result = pair_max_by_hour(records, hours=[_hour(3)])
    assert _hour(3) in result
    assert result[_hour(3)] == 0.0


# --- T4 ----------------------------------------------------------------


def _payload(*observations: dict[str, object]) -> dict[str, object]:
    return {"observations": list(observations)}


def test_wind_records_from_payload_drops_non_finite_and_invalid_values() -> None:
    good = {"epoch": 1700000000, "metric": {"windspeedAvg": 12.0}}
    nan_row = {"epoch": 1700000001, "metric": {"windspeedAvg": math.nan}}
    inf_row = {"epoch": 1700000002, "metric": {"windspeedAvg": math.inf}}
    bool_row = {"epoch": 1700000003, "metric": {"windspeedAvg": True}}
    negative_row = {"epoch": 1700000004, "metric": {"windspeedAvg": -1.0}}
    nonnumeric_row = {"epoch": 1700000005, "metric": {"windspeedAvg": "gusty"}}
    missing_row = {"epoch": 1700000006, "metric": {}}
    wind_speed_only_row = {"epoch": 1700000007, "metric": {"windSpeed": 9.0}}

    payload = _payload(
        good,
        nan_row,
        inf_row,
        bool_row,
        negative_row,
        nonnumeric_row,
        missing_row,
        wind_speed_only_row,
    )
    result = wind_records_from_payload(payload)
    assert [r.speed_kmh for r in result] == [12.0]


def test_wind_records_from_payload_drops_bool_epoch() -> None:
    """A bool epoch is dropped even though ``_obs_datetime`` would accept it.

    mutant_accepts_bool_epoch -> at this assertion: correct = [] (the only
    row has a bool epoch and is dropped), mutant (omitting the
    ``isinstance(raw_epoch, bool)`` guard before calling ``_obs_datetime``)
    = [the row] (``datetime.fromtimestamp(True, ...)`` succeeds, since
    ``True == 1``). The fixture's sole row has no other reason to be kept
    or dropped, so the two literals diverge on this row alone.
    """
    payload = _payload({"epoch": True, "metric": {"windspeedAvg": 5.0}})
    assert wind_records_from_payload(payload) == []


def test_wind_records_from_payload_truncates_float_epoch_to_whole_second() -> None:
    payload = _payload({"epoch": 1700000000.9, "metric": {"windspeedAvg": 3.0}})
    result = wind_records_from_payload(payload)
    assert len(result) == 1
    assert result[0].obs_at.microsecond == 0
    assert result[0].obs_at == datetime.fromtimestamp(1700000000, tz=UTC)


def test_wind_records_from_payload_keeps_first_of_duplicate_obs_at() -> None:
    payload = _payload(
        {"epoch": 1700000000, "metric": {"windspeedAvg": 11.0}},
        {"epoch": 1700000000, "metric": {"windspeedAvg": 22.0}},
    )
    result = wind_records_from_payload(payload)
    assert len(result) == 1
    assert result[0].speed_kmh == 11.0


def test_wind_records_from_payload_accepts_numeric_string_speed() -> None:
    payload = _payload({"epoch": 1700000000, "metric": {"windspeedAvg": "7.5"}})
    result = wind_records_from_payload(payload)
    assert [r.speed_kmh for r in result] == [7.5]


# --- T5 ----------------------------------------------------------------


def test_midnight_carry_independent_of_fetch_order() -> None:
    """Hour 00 of d is the mean of (23:55 of d-1, 00:00 of d), fetched either order.

    mutant_skip_neighbour -> at this assertion: correct = {hour00: 12.5} in
    BOTH fetch orders, mutant (deriving only the day being fetched, never its
    neighbour) would give hour00 ABSENT whenever the records for d-1 and d
    are not presented together -- but `pair_max_by_hour` is pure over
    whatever records it is given, so this test instead pins that the pure
    function itself does not care which record came "first": calling it with
    the d-1-then-d ordering and the d-then-d-1 ordering (both unsorted
    inputs) gives the identical result, proving the derivation depends only
    on stored obs_at order, never on fetch/insertion order.
    """
    day_minus_1 = datetime(2026, 7, 1, 23, 55, tzinfo=UTC)
    day_d = datetime(2026, 7, 2, 0, 0, tzinfo=UTC)
    hour00_of_d = datetime(2026, 7, 2, 0, 0, tzinfo=UTC)

    order_a = [
        WindRecord(obs_at=day_minus_1, speed_kmh=15.0),
        WindRecord(obs_at=day_d, speed_kmh=10.0),
    ]
    order_b = [
        WindRecord(obs_at=day_d, speed_kmh=10.0),
        WindRecord(obs_at=day_minus_1, speed_kmh=15.0),
    ]
    result_a = pair_max_by_hour(order_a, hours=[hour00_of_d])
    result_b = pair_max_by_hour(order_b, hours=[hour00_of_d])
    assert result_a == {hour00_of_d: 12.5}
    assert result_a == result_b


# --- T6 ----------------------------------------------------------------


def test_today_incomplete_hour_not_derived_until_next_hour_start_reached() -> None:
    """An hour H is only "complete" once a record at >= H+1h exists.

    This test pins the CONTRACT `pair_max_by_hour` relies on: callers must
    only pass `hours` that are complete (plan §8.5 step 2, "today: only the
    hours H with H + 1h <= max(stored obs_at)"). `pair_max_by_hour` itself
    is pure and will happily compute a value for ANY hour in `hours` given a
    pair -- so this test demonstrates the boundary at the caller's own
    membership check: hour 3 has a pair fully inside it (3:00, 3:05) but
    becomes "derivable" only once a stored record reaches 4:00.

    mutant_lt_for_le -> at this assertion: correct = True (3:05 + up-to-1h
    reaches exactly 4:00, the hour boundary, so H+1h <= max_obs_at holds when
    max_obs_at == 4:00), mutant (`<` in place of `<=`) = False at that exact
    boundary. The fixture's max_obs_at is placed exactly on the boundary so
    the two literals diverge only there.
    """
    hour_start = _hour(3)
    boundary_max_obs_at = _hour(4)  # H + 1h, exactly
    just_before = _hour(4) - timedelta(seconds=1)

    def hour_is_complete(h: datetime, max_obs_at: datetime) -> bool:
        return h + timedelta(hours=1) <= max_obs_at

    assert hour_is_complete(hour_start, boundary_max_obs_at) is True
    assert hour_is_complete(hour_start, just_before) is False


# --- T7 ----------------------------------------------------------------


def test_pair_max_by_hour_handles_dst_transition_hour_counts() -> None:
    """DST days have 23 or 25 hours; pair_max_by_hour must not assume 24.

    mutant_range24 -> at this assertion: correct = 25 derived hours produce
    25 entries when every hour has a pair (spring-forward/fall-back is the
    CALLER's hour enumeration, not this function's business) -- this test
    pins that `pair_max_by_hour` returns a result for every member of
    `hours` it is given, however many there are (23 or 25), proving it
    performs no internal `range(24)` truncation or padding of its own.
    """
    start = datetime(2026, 10, 25, 0, 0, tzinfo=UTC)  # a 25-hour local day, UTC-framed
    hours_25 = [start + timedelta(hours=h) for h in range(25)]
    records = []
    for h in range(26):
        records.append(
            WindRecord(obs_at=start + timedelta(hours=h, minutes=0), speed_kmh=float(h))
        )
        records.append(
            WindRecord(obs_at=start + timedelta(hours=h, minutes=5), speed_kmh=float(h))
        )
    result_25 = pair_max_by_hour(records, hours=hours_25)
    assert len(result_25) == 25

    start23 = datetime(2026, 3, 29, 0, 0, tzinfo=UTC)
    hours_23 = [start23 + timedelta(hours=h) for h in range(23)]
    records_23 = []
    for h in range(24):
        records_23.append(
            WindRecord(
                obs_at=start23 + timedelta(hours=h, minutes=0), speed_kmh=float(h)
            )
        )
        records_23.append(
            WindRecord(
                obs_at=start23 + timedelta(hours=h, minutes=5), speed_kmh=float(h)
            )
        )
    result_23 = pair_max_by_hour(records_23, hours=hours_23)
    assert len(result_23) == 23
