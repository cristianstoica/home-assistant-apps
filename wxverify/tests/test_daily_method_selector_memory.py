"""§15.2 rows 33-36: the incumbent memory through a gap (§9.1).

Pure dict-driven rows over `record_decision` and `remembered_incumbent` --
no RainWindow needed. Dates are the module's own `date` objects offset
from a synthetic 2035 base; no calendar meaning beyond ordering is used.
"""

from __future__ import annotations

from datetime import date, timedelta

from wxverify.forecast.daily_method import (
    RecordDecision,
    record_decision,
    remembered_incumbent,
)

_BASE = date(2035, 1, 1)


def _d(n: int) -> date:
    return _BASE + timedelta(days=n)


def test_incumbent_remembered_the_60_date_bound() -> None:
    """Row 33: SYN-a selected on dates 1-40, nothing after; snapshot 100, 101.

    At snapshot 100 the 60-day lookback (offsets 1..60) reaches back to
    day 40, which is the only decision in range -> SYN-a, last selected on
    40. At snapshot 101 the lookback reaches back only to day 41, missing
    every recorded decision -> None.
    """
    decisions = {_d(n): RecordDecision("selected", "SYN-a") for n in range(1, 41)}
    at_100 = remembered_incumbent(decisions, snapshot_date=_d(100))
    assert at_100.model == "SYN-a"
    assert at_100.selected_on == _d(40)
    assert at_100.unrecognised_on is None

    at_101 = remembered_incumbent(decisions, snapshot_date=_d(101))
    assert at_101.model is None
    assert at_101.selected_on is None
    assert at_101.unrecognised_on is None


def test_gaps_carry_the_incumbent() -> None:
    """Row 34: SYN-a selected on 40; not_ready on 41-44, error on 45, no row
    on 46, a row with no "method" key on 47; snapshot_date 48.

    All of 41-47 decide as gaps, so the lookback reads straight through
    them to the day-40 selection: SYN-a, last selected on 40.
    """
    decisions: dict[date, RecordDecision] = {
        _d(40): record_decision({"method": "selector", "selected_feed": "SYN-a"}),
    }
    for n in range(41, 45):
        decisions[_d(n)] = record_decision(
            {"method": "selector", "fallback_reason": "not_ready"}
        )
    decisions[_d(45)] = record_decision(
        {"method": "selector", "fallback_reason": "error"}
    )
    # day 46 deliberately absent: "no row on 46".
    decisions[_d(47)] = record_decision({})

    for n in range(41, 46):
        assert decisions[_d(n)].kind == "gap", n
    assert decisions[_d(47)].kind == "gap"

    result = remembered_incumbent(decisions, snapshot_date=_d(48))
    assert result.model == "SYN-a"
    assert result.selected_on == _d(40)
    assert result.unrecognised_on is None


def test_clears_end_the_memory() -> None:
    """Row 35: a no_eligible row on 41, not_ready on 42-50, snapshot 51.

    The clear at day 41 is reached (scanning latest-first) before the
    day-40 selection ever is -> None. A "method": "current" row at 41
    clears the same way. With SYN-b selected on 45 instead of not_ready,
    the scan (latest day first) reaches day 45's selection before it ever
    reaches day 41's clear -> SYN-b, last selected on 45.
    """
    base: dict[date, RecordDecision] = {
        _d(40): RecordDecision("selected", "SYN-a"),
        _d(41): record_decision(
            {"method": "selector", "fallback_reason": "no_eligible"}
        ),
    }
    for n in range(42, 51):
        base[_d(n)] = record_decision(
            {"method": "selector", "fallback_reason": "not_ready"}
        )
    assert base[_d(41)].kind == "clear"
    result = remembered_incumbent(base, snapshot_date=_d(51))
    assert result.model is None
    assert result.selected_on is None
    assert result.unrecognised_on is None

    current_clear = dict(base)
    current_clear[_d(41)] = record_decision({"method": "current"})
    assert current_clear[_d(41)].kind == "clear"
    result_current = remembered_incumbent(current_clear, snapshot_date=_d(51))
    assert result_current.model is None
    assert result_current.selected_on is None
    assert result_current.unrecognised_on is None

    with_syn_b = dict(base)
    with_syn_b[_d(45)] = RecordDecision("selected", "SYN-b")
    result_b = remembered_incumbent(with_syn_b, snapshot_date=_d(51))
    assert result_b.model == "SYN-b"
    assert result_b.selected_on == _d(45)


def test_an_unrecognised_row_ends_the_memory() -> None:
    """Row 36: six record_decision inputs -- the first five unrecognised,
    the sixth (``fallback_reason: null`` with a non-empty ``selected_feed``)
    is the selected model SYN-a. Then remembered_incumbent stops dead at
    the first unrecognised row and reports its date, never reaching the
    day-40 selection behind it.
    """
    unrecognised_inputs: list[object] = [
        {"method": "selector"},
        {"method": "selector", "fallback_reason": "stale"},
        {"method": "auto"},
        [],
        None,
    ]
    for payload in unrecognised_inputs:
        assert record_decision(payload).kind == "unrecognised"
    sixth = record_decision(
        {"method": "selector", "selected_feed": "SYN-a", "fallback_reason": None}
    )
    assert sixth.kind == "selected"
    assert sixth.model == "SYN-a"

    decisions: dict[date, RecordDecision] = {
        _d(40): RecordDecision("selected", "SYN-a"),
    }
    for n in range(41, 45):
        decisions[_d(n)] = record_decision(
            {"method": "selector", "fallback_reason": "not_ready"}
        )
    decisions[_d(45)] = record_decision({"method": "selector"})  # unrecognised
    for n in range(46, 51):
        decisions[_d(n)] = record_decision(
            {"method": "selector", "fallback_reason": "not_ready"}
        )
    result = remembered_incumbent(decisions, snapshot_date=_d(51))
    assert result.model is None
    assert result.selected_on is None
    assert result.unrecognised_on == _d(45)
