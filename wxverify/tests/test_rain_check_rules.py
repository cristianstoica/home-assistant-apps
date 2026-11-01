"""Pure rules of the rain check (plan 2026-11-01-rain-release.md, Part A;
test IDs A2-A6; A1 and A7 are covered at the database level in
``tests/test_rain_check_close.py``, since they need a real close to show a
list row and an ``observations`` row).

Written before ``wxverify.obs.rain_check`` exists: every test here fails at
``ImportError`` until the module is built. No I/O, no clock, no SQLite --
every fixture is a plain in-memory value. Nothing real: station ids are
synthetic ints, dates are a fixed synthetic ``2024-06-10`` and after.

Assumed API (``wxverify/obs/rain_check.py``), for the builder to match:

    @dataclass(frozen=True)
    class StationDayReadings:
        precip_mm: Mapping[datetime, float]       # hour -> the hour's OWN
                                                    # rain amount; a missing
                                                    # key means no value
        temperature_c: Mapping[datetime, float | None]
        gap_hours: frozenset[datetime]             # hours filled from the
                                                    # hourly download

    @dataclass(frozen=True)
    class JudgeResult:
        outcome: str   # "zero" | "agree" | "disagree" | "dry"
                       # | "not_widespread" | "cant_judge"
        reason: str | None   # set only when outcome == "cant_judge":
                             # "too_few_stations" | "wet_hours_unknown"
                             # | "last_hour" | "gap_filled" | "cold"
                             # | "target_missing"
        station_mm: float | None
        peer_median_mm: float | None
        peers: int

    def judge_day(
        hours: Sequence[datetime],       # the day's hours, in order (24)
        target: StationDayReadings,
        peers: Sequence[StationDayReadings],
        *,
        site_temps_before: Mapping[datetime, float | None],
            # the site's `observations` temperature, keyed by hour, for
            # (at least) the 48 hours before the day; a missing key means
            # that hour is missing
    ) -> JudgeResult: ...

    @dataclass(frozen=True)
    class RainCheckRow:
        local_date: date
        outcome: str
        decision: str | None = None   # "set_aside" | "taken_back"
                                       # | "ambiguous" | "site_limit" | None

    @dataclass(frozen=True)
    class FoldResult:
        decision: str | None
        counted_after: int   # 0 or 1

    def fold(
        history: Sequence[RainCheckRow],   # X's earlier rows, ascending
        local_date: date,
        outcome: str,
        *,
        counted_before: int,   # X's rain_counted going into `local_date`
        other_new_candidates_today: int = 0,
            # other listed stations that ALSO newly qualify for set-aside
            # on `local_date` (site-limit bullet, "two or more candidates")
        other_station_set_aside_today: bool = False,
            # another listed station is already set aside as of
            # `local_date` (site-limit bullet, "already ... set aside")
    ) -> FoldResult: ...
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from wxverify.obs.rain_check import (
    JudgeResult,
    RainCheckRow,
    StationDayReadings,
    fold,
    judge_day,
)

# ---------------------------------------------------------------------------
# judge_day fixtures (A4, A5a)
# ---------------------------------------------------------------------------

_DAY = date(2024, 6, 10)
HOURS: tuple[datetime, ...] = tuple(
    datetime(_DAY.year, _DAY.month, _DAY.day, h, tzinfo=UTC) for h in range(24)
)
WET_HOUR_IDX = 10
LAST_HOUR_IDX = 23


def _station(
    *,
    precip_overrides: dict[int, float] | None = None,
    missing_precip_hours: frozenset[int] = frozenset(),
    temp_overrides: dict[int, float | None] | None = None,
    gap_hour_indices: frozenset[int] = frozenset(),
    default_temp: float = 20.0,
) -> StationDayReadings:
    """24 hours of 0.0 mm and ``default_temp``, perturbed by the overrides."""
    precip_overrides = precip_overrides or {}
    temp_overrides = temp_overrides or {}
    precip_mm = {
        HOURS[i]: precip_overrides.get(i, 0.0)
        for i in range(24)
        if i not in missing_precip_hours
    }
    temperature_c: dict[datetime, float | None] = {
        HOURS[i]: temp_overrides.get(i, default_temp) for i in range(24)
    }
    gap_hours = frozenset(HOURS[i] for i in gap_hour_indices)
    return StationDayReadings(
        precip_mm=precip_mm, temperature_c=temperature_c, gap_hours=gap_hours
    )


def _peer(
    rise_mm: float = 3.0,
    *,
    wet_hour_idx: int = WET_HOUR_IDX,
    temp_overrides: dict[int, float | None] | None = None,
    gap_hour_indices: frozenset[int] = frozenset(),
) -> StationDayReadings:
    """A valid, warm peer-day that rises ``rise_mm`` at one hour only."""
    return _station(
        precip_overrides={wet_hour_idx: rise_mm},
        temp_overrides=temp_overrides,
        gap_hour_indices=gap_hour_indices,
    )


def _three_peers(
    rise_mm: float = 3.0, *, wet_hour_idx: int = WET_HOUR_IDX
) -> list[StationDayReadings]:
    """3 valid peers, each rising ``rise_mm`` at the same hour (M == rise_mm,
    0 peers below 1.0 mm): satisfies steps 1-3 without triggering ``dry`` or
    ``not_widespread``.
    """
    return [_peer(rise_mm, wet_hour_idx=wet_hour_idx) for _ in range(3)]


def _warm_pre_window(wet_hour: datetime) -> dict[datetime, float | None]:
    """48 hours before ``wet_hour``, each a warm, present site temperature."""
    return {wet_hour - timedelta(hours=i): 10.0 for i in range(1, 49)}


def _judge(
    target: StationDayReadings,
    peers: list[StationDayReadings],
    *,
    wet_hour_idx: int = WET_HOUR_IDX,
    site_temps_before: dict[datetime, float | None] | None = None,
) -> JudgeResult:
    window = site_temps_before
    if window is None:
        window = _warm_pre_window(HOURS[wet_hour_idx])
    return judge_day(HOURS, target, peers, site_temps_before=window)


# ---------------------------------------------------------------------------
# A4: one test per "can't judge" case, each changing nothing else
# ---------------------------------------------------------------------------


def test_a4_gap_filled_hour_for_target_cant_judge() -> None:
    """Rain in a gap-filled hour -- the TARGET's own gap (rule 6)."""
    target = _peer(3.0, gap_hour_indices=frozenset({WET_HOUR_IDX}))
    result = _judge(target, _three_peers())
    assert result.outcome == "cant_judge"
    assert result.reason == "gap_filled"


def test_a4_gap_filled_hour_for_a_peer_cant_judge() -> None:
    """Rain in a gap-filled hour -- a PEER's gap (rule 6)."""
    peers = _three_peers()
    peers[0] = _peer(3.0, gap_hour_indices=frozenset({WET_HOUR_IDX}))
    target = _peer(3.0)
    result = _judge(target, peers)
    assert result.outcome == "cant_judge"
    assert result.reason == "gap_filled"


def test_a4_wet_hour_is_the_last_hour_cant_judge() -> None:
    """The day's last hour is in W (rule 5), checked before gap/cold."""
    target = _peer(3.0, wet_hour_idx=LAST_HOUR_IDX)
    result = _judge(
        target, _three_peers(wet_hour_idx=LAST_HOUR_IDX), wet_hour_idx=LAST_HOUR_IDX
    )
    assert result.outcome == "cant_judge"
    assert result.reason == "last_hour"


def test_a4_cold_wet_hour_by_peer_median_cant_judge() -> None:
    """Peer median temperature below 3.0C at the wet hour (rule 7, peer
    branch): 3 peer temps of 1.0/1.5/2.0 at the wet hour, median 1.5."""
    peers = [_peer(3.0, temp_overrides={WET_HOUR_IDX: t}) for t in (1.0, 1.5, 2.0)]
    target = _peer(3.0)
    result = _judge(target, peers)
    assert result.outcome == "cant_judge"
    assert result.reason == "cold"


def test_a4_cold_wet_hour_by_target_own_temp_cant_judge() -> None:
    """The target's own temperature below 3.0C at the wet hour (rule 7)."""
    target = _peer(3.0, temp_overrides={WET_HOUR_IDX: 1.0})
    result = _judge(target, _three_peers())
    assert result.outcome == "cant_judge"
    assert result.reason == "cold"


def test_a4_missing_target_temperature_at_wet_hour_cant_judge() -> None:
    """The target's temperature missing (None), not just low, at the wet
    hour (rule 7: "missing or below 3.0C")."""
    target = _peer(3.0, temp_overrides={WET_HOUR_IDX: None})
    result = _judge(target, _three_peers())
    assert result.outcome == "cant_judge"
    assert result.reason == "cold"


def test_a4_frost_in_the_48_hours_before_cant_judge() -> None:
    """The site's own temperature below 1.0C in the 48 hours before W's
    first hour (rule 7, the pre-window branch)."""
    target = _peer(3.0)
    window = _warm_pre_window(HOURS[WET_HOUR_IDX])
    window[HOURS[WET_HOUR_IDX] - timedelta(hours=5)] = 0.5
    result = _judge(target, _three_peers(), site_temps_before=window)
    assert result.outcome == "cant_judge"
    assert result.reason == "cold"


def test_a4_only_two_valid_peers_cant_judge() -> None:
    """Fewer than 3 valid peers (rule 1), checked before everything else."""
    peers = [_peer(3.0), _peer(3.0)]
    target = _peer(3.0)
    result = _judge(target, peers)
    assert result.outcome == "cant_judge"
    assert result.reason == "too_few_stations"


def test_a4_missing_target_rain_hour_is_never_a_zero() -> None:
    """A missing target hour is ``target_missing``, never folded in as a
    0.0 contribution to S (rule 8, "A missing hour is never a zero")."""
    target = _station(
        precip_overrides={WET_HOUR_IDX: 0.0},
        missing_precip_hours=frozenset({5}),
    )
    result = _judge(target, _three_peers())
    assert result.outcome == "cant_judge"
    assert result.reason == "target_missing"


# ---------------------------------------------------------------------------
# A5a: a gauge at 0.01 mm is not "zero"
# ---------------------------------------------------------------------------


def test_a5_gauge_at_0_01mm_is_disagree_not_zero() -> None:
    """S == 0.01 is neither exactly 0.0 (rule 9's ``zero``) nor inside
    [M/2, 2M] == [1.5, 6.0] for M == 3.0 -- it must be ``disagree``."""
    target = _peer(0.01)
    result = _judge(target, _three_peers(3.0))
    assert result.outcome == "disagree"
    assert result.reason is None


# ---------------------------------------------------------------------------
# A5b / A2 / A3 / A6: fold()
# ---------------------------------------------------------------------------


def _row(d: date, outcome: str, decision: str | None = None) -> RainCheckRow:
    return RainCheckRow(local_date=d, outcome=outcome, decision=decision)


def test_a5_disagree_resets_the_zero_count() -> None:
    """2 zero days, then a disagree, then a dry (new spell), then 1 more
    zero day: WITHOUT the disagree's reset this would be 3 zero days
    (day1, day2, day5) spanning 2 spells and would set X aside; the
    disagree must drop day1/day2 from the window, leaving only 1 zero day
    in range at day5, so X stays counted."""
    day1, day2, day3, day4, day5 = (
        date(2024, 1, 1),
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
    )
    history = [
        _row(day1, "zero"),
        _row(day2, "zero"),
        _row(day3, "disagree"),
        _row(day4, "dry"),
    ]
    result = fold(history, day5, "zero", counted_before=1)
    assert result.decision is None
    assert result.counted_after == 1


def test_a2_three_zero_days_in_one_spell_is_no_set_aside() -> None:
    """3 consecutive zero days, no dry row between any of them: 1 spell
    only, so the spell count (>= 2) is never met."""
    day1, day2, day3 = date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)
    history = [_row(day1, "zero"), _row(day2, "zero")]
    result = fold(history, day3, "zero", counted_before=1)
    assert result.decision is None
    assert result.counted_after == 1


def test_a2_zero_days_spread_over_more_than_30_days_is_no_set_aside() -> None:
    """The earliest zero day sits exactly ON the D-30 cutoff (excluded,
    since the window is rows AFTER the cutoff): only 2 zero days remain
    in range, short of the 3 the rule needs."""
    d = date(2024, 2, 1)
    day_a = d - timedelta(days=30)  # == cutoff, excluded
    day_b = d - timedelta(days=29)  # one day inside the window
    day_c = d - timedelta(days=5)
    history = [_row(day_a, "zero"), _row(day_b, "dry"), _row(day_c, "zero")]
    result = fold(history, d, "zero", counted_before=1)
    assert result.decision is None
    assert result.counted_after == 1


def test_a3_three_agree_days_in_two_spells_takes_x_back() -> None:
    """X is set aside on day0; agree(day1) opens one spell, dry(day2)
    opens a new one, agree(day3), agree(day4, the fold target): 3 agree
    rows (day1, day3, day4) across 2 spells (day1's, and day3/day4's)
    takes X back."""
    day0, day1, day2, day3, day4 = (
        date(2024, 6, 1),
        date(2024, 6, 2),
        date(2024, 6, 3),
        date(2024, 6, 4),
        date(2024, 6, 5),
    )
    history = [
        _row(day0, "zero", decision="set_aside"),
        _row(day1, "agree"),
        _row(day2, "dry"),
        _row(day3, "agree"),
    ]
    result = fold(history, day4, "agree", counted_before=0)
    assert result.decision == "taken_back"
    assert result.counted_after == 1


def test_a3_three_agree_days_in_one_spell_stays_set_aside() -> None:
    """X is set aside on day0; agree(day1), agree(day2), agree(day3, the
    fold target), with no dry row anywhere between them: 1 spell only, so
    X is not taken back."""
    day0, day1, day2, day3 = (
        date(2024, 6, 1),
        date(2024, 6, 2),
        date(2024, 6, 3),
        date(2024, 6, 4),
    )
    history = [
        _row(day0, "zero", decision="set_aside"),
        _row(day1, "agree"),
        _row(day2, "agree"),
    ]
    result = fold(history, day3, "agree", counted_before=0)
    assert result.decision is None
    assert result.counted_after == 0


def _legitimate_set_aside_history() -> tuple[list[RainCheckRow], date]:
    """2 zero days (spell 1), a dry day, then the fold target is a 3rd
    zero day (spell 2): on its own this satisfies the set-aside rule, so
    each A6 test below only has to vary the site-limit flags."""
    day1, day2, day3, day4 = (
        date(2024, 3, 1),
        date(2024, 3, 2),
        date(2024, 3, 3),
        date(2024, 3, 4),
    )
    return [_row(day1, "zero"), _row(day2, "zero"), _row(day3, "dry")], day4


def test_a6_sanity_positive_lone_candidate_is_set_aside() -> None:
    """Paired positive for the two A6 negatives below: with neither
    site-limit flag set, the legitimate candidate IS set aside."""
    history, day4 = _legitimate_set_aside_history()
    result = fold(history, day4, "zero", counted_before=1)
    assert result.decision == "set_aside"
    assert result.counted_after == 0


def test_a6_two_candidates_same_day_give_ambiguous() -> None:
    """A second listed station also newly qualifies on the same day: both
    stay counted, decision is ``ambiguous`` -- not ``set_aside``."""
    history, day4 = _legitimate_set_aside_history()
    result = fold(history, day4, "zero", counted_before=1, other_new_candidates_today=1)
    assert result.decision == "ambiguous"
    assert result.counted_after == 1


def test_a6_candidate_while_another_already_set_aside_gives_site_limit() -> None:
    """Another listed station is already set aside as of this day: the new
    candidate is blocked with ``site_limit``, not set aside."""
    history, day4 = _legitimate_set_aside_history()
    result = fold(
        history,
        day4,
        "zero",
        counted_before=1,
        other_station_set_aside_today=True,
    )
    assert result.decision == "site_limit"
    assert result.counted_after == 1


def test_a6_take_back_is_never_blocked_by_a_site_limit() -> None:
    """Site limits are never applied to a take-back (rule: "Take-backs are
    never blocked"): with X already set aside, 3 agree days (day1, day3,
    day4) in 2 spells still take it back even while another station is
    already set aside."""
    day0, day1, day2, day3, day4 = (
        date(2024, 6, 1),
        date(2024, 6, 2),
        date(2024, 6, 3),
        date(2024, 6, 4),
        date(2024, 6, 5),
    )
    history = [
        _row(day0, "zero", decision="set_aside"),
        _row(day1, "agree"),
        _row(day2, "dry"),
        _row(day3, "agree"),
    ]
    result = fold(
        history,
        day4,
        "agree",
        counted_before=0,
        other_station_set_aside_today=True,
    )
    assert result.decision == "taken_back"
    assert result.counted_after == 1
