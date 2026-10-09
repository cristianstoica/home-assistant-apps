"""§15.2 rows 22-23: the station fault check decides each day from earlier
days only, and a dry spell never clears a quarantine (§6.7).

The thresholds below are not invented for the test: they are the plan's own
published values (section 10.2, "Its header holds the rain settings"): 30,
60 and 30 day look-backs; the 1 mm and 0.1 mm lines; 3 peers; 5 and 8
qualifying days; 3 and 2 episodes; and the ratios 0.5 and 0.25 (suspect and
quarantine) -- recovery shares the quarantine ratio pairing at 0.5, per
§6.7's "a median ratio of 0.5 or more" recovery rule, and allows at most 1
dry-while-wet day, per "at most one dry-while-wet day".
"""

from __future__ import annotations

from datetime import date, timedelta
from fractions import Fraction
from typing import TypedDict

from wxverify.forecast.daily_method import StationDayTotal, station_fault_states

_BASE = date(2035, 1, 1)


class _FaultKwargs(TypedDict):
    adequate_hours: int
    wet_line_hundredths: int
    dry_line_hundredths: int
    min_peers: int
    suspect_lookback_days: int
    suspect_min_days: int
    suspect_ratio_below: Fraction
    quarantine_lookback_days: int
    quarantine_min_days: int
    quarantine_min_episodes: int
    quarantine_ratio_at_most: Fraction
    quarantine_dry_share: Fraction
    recovery_lookback_days: int
    recovery_min_days: int
    recovery_min_episodes: int
    recovery_ratio_at_least: Fraction
    recovery_max_dry_days: int


_FAULT_KWARGS: _FaultKwargs = {
    "adequate_hours": 22,
    "wet_line_hundredths": 100,
    "dry_line_hundredths": 10,
    "min_peers": 3,
    "suspect_lookback_days": 30,
    "suspect_min_days": 5,
    "suspect_ratio_below": Fraction(1, 2),
    "quarantine_lookback_days": 60,
    "quarantine_min_days": 8,
    "quarantine_min_episodes": 3,
    "quarantine_ratio_at_most": Fraction(1, 4),
    "quarantine_dry_share": Fraction(1, 2),
    "recovery_lookback_days": 30,
    "recovery_min_days": 5,
    "recovery_min_episodes": 2,
    "recovery_ratio_at_least": Fraction(1, 2),
    "recovery_max_dry_days": 1,
}


def _d(n: int) -> date:
    return _BASE + timedelta(days=n - 1)


def _total(mm: float) -> StationDayTotal:
    return StationDayTotal(
        total=mm,
        hundredths=round(mm * 100),
        hours=24,
        expected_hours=24,
        problems=(),
        correction=None,
        invalid=False,
        after_gap=0,
    )


_WET_DAYS_1_13 = {1, 2, 3, 6, 7, 8, 11, 12}  # three episodes, 8 of 13 days


def test_fault_states_use_earlier_days_only() -> None:
    """Row 22: SYN-1..3 wet (2.0mm) on days 1-3, 6-8, 11-12 (three episodes),
    dry elsewhere; SYN-4 dry (0.0mm) throughout.

    SYN-4 qualifies (its peers' median reaches 1mm) on exactly the 8 wet
    days, always at ratio 0 (dry-while-wet). Each day's state is decided
    from STRICTLY earlier days: the 5th qualifying day is day 7 (days
    1,2,3,6,7), so suspect first appears on day 8 -- not day 7, which would
    be the bug of including the day itself. The 8th qualifying day is day
    12 (days 1,2,3,6,7,8,11,12), spanning all 3 episodes, so quarantine
    first appears on day 13 -- the day after, not day 12 itself (M44's bug).
    """
    peers = {d: (2.0 if d in _WET_DAYS_1_13 else 0.0) for d in range(1, 14)}
    history = {
        "SYN-1": {_d(n): _total(mm) for n, mm in peers.items()},
        "SYN-2": {_d(n): _total(mm) for n, mm in peers.items()},
        "SYN-3": {_d(n): _total(mm) for n, mm in peers.items()},
        "SYN-4": {_d(n): _total(0.0) for n in range(1, 14)},
    }
    result = station_fault_states(history, **_FAULT_KWARGS)

    for n in range(1, 8):
        assert result.states[("SYN-4", _d(n))] == "ok", n
    for n in range(8, 13):
        assert result.states[("SYN-4", _d(n))] == "suspect", n
    assert result.states[("SYN-4", _d(13))] == "quarantined"

    changes = [c for c in result.log if c.station_id == "SYN-4"]
    assert [(c.day, c.state) for c in changes] == [
        (_d(8), "suspect"),
        (_d(13), "quarantined"),
    ]
    quarantine_change = changes[-1]
    assert quarantine_change.qualifying_days == 8
    assert quarantine_change.episodes == 3
    assert quarantine_change.dry_days == 8

    # SYN-4's own qualifying days, independent of the states above.
    qualifying_days = {n for n in range(1, 14) if ("SYN-4", _d(n)) in result.qualifying}
    assert qualifying_days == _WET_DAYS_1_13

    # The peers themselves never trip a fault state (ratio stays 1.0 -- see
    # the module docstring above for why they are not expected to).
    for source in ("SYN-1", "SYN-2", "SYN-3"):
        assert all(result.states[(source, _d(n))] == "ok" for n in range(1, 14))


def test_a_dry_spell_never_clears() -> None:
    """Row 23: the same 13-day scenario (SYN-4 quarantined on day 13), then
    40 dry days (14-53) with every station at 0.0mm -- no qualifying days
    at all, so the quarantine is untouched (M45's bug clears it here).

    Then 5 more qualifying days in two episodes (54-56, 58-59, with day 57
    dry) on which SYN-4 matches its peers exactly (ratio 1.0, never
    dry-while-wet): the 5th such day is day 59, so recovery lands on day
    60, the day after.
    """
    peers = {d: (2.0 if d in _WET_DAYS_1_13 else 0.0) for d in range(1, 14)}
    for n in range(14, 54):
        peers[n] = 0.0  # the 40-day dry spell
    recovery_wet_days = {54, 55, 56, 58, 59}
    for n in range(54, 61):
        peers[n] = 1.0 if n in recovery_wet_days else 0.0

    syn4 = {n: 0.0 for n in range(1, 54)}
    for n in range(54, 61):
        syn4[n] = 1.0 if n in recovery_wet_days else 0.0  # matches its peers

    history = {
        "SYN-1": {_d(n): _total(mm) for n, mm in peers.items()},
        "SYN-2": {_d(n): _total(mm) for n, mm in peers.items()},
        "SYN-3": {_d(n): _total(mm) for n, mm in peers.items()},
        "SYN-4": {_d(n): _total(mm) for n, mm in syn4.items()},
    }
    result = station_fault_states(history, **_FAULT_KWARGS)

    assert result.states[("SYN-4", _d(13))] == "quarantined"
    for n in range(14, 54):
        assert result.states[("SYN-4", _d(n))] == "quarantined", n
    for n in range(54, 60):
        assert result.states[("SYN-4", _d(n))] == "quarantined", n
    assert result.states[("SYN-4", _d(60))] == "ok"

    changes = [c for c in result.log if c.station_id == "SYN-4"]
    recovery_change = changes[-1]
    assert recovery_change.day == _d(60)
    assert recovery_change.state == "ok"
    assert recovery_change.rule == "recovery"
    assert recovery_change.qualifying_days == 5
    assert recovery_change.episodes == 2
    assert recovery_change.dry_days == 0
