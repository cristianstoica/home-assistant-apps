"""§15.2 rows 24, 26, 27, 29-32, 37: the selector's ranking, minimum,
switching margin, incumbent rules and trigger grid (§9.1, §9.2).

Every ``Selection`` here comes from a real `select_model` call over a
hand-built `RainWindow` -- never a stand-in dataclass -- so the miss/false
-alarm counting, the drop rule and the incumbent-outcome branches are all
exercised through the real code path.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from fractions import Fraction

import pytest

from wxverify.forecast.daily_method import (
    GroupVote,
    ModelWindow,
    RainWindow,
    Selection,
    ServedRain,
    group_vote_chance,
    select_model,
    served_rain,
)
from wxverify.verification import daily_shadow

_BASE = date(2035, 1, 1)


def _d(n: int) -> date:
    return _BASE + timedelta(days=n)


def _model(
    source: str, totals: dict[date, float], *, covers: bool = True
) -> ModelWindow:
    return ModelWindow(
        source=source, model="SYN-m", totals=totals, covers_target=covers
    )


def test_selector_tie_a_stays_then_b_wins_with_no_incumbent() -> None:
    """Row 24: three models over 20 common days, trigger 20 (0.2mm),
    miss_weight=2, switch_margin=1.

    a: misses on days 1-2, false alarms on day 11 only -> penalty 2*2+1=5.
    b: misses on day 1, false alarms on days 11-13 -> penalty 2*1+3=5.
    c: no misses, false alarms on days 11-16 -> penalty 2*0+6=6.
    a and b tie at penalty 5; b has fewer misses (1 vs 2) and ranks first.
    With incumbent a: held(5) - best(5) = 0, not >= switch_margin(1) ->
    kept, a stays selected. With no incumbent: selected = best = b.
    """
    events = {_d(n): (n <= 10) for n in range(1, 21)}
    a_totals = {_d(n): 0.0 for n in range(1, 21)}
    for n in (1, 2):
        a_totals[_d(n)] = 0.0  # miss: dry forecast on a wet day
    a_totals[_d(11)] = 0.3  # false alarm on a dry day
    for n in (3, 4, 5, 6, 7, 8, 9, 10):
        a_totals[_d(n)] = 0.3  # correct wet forecast

    b_totals = {_d(n): 0.0 for n in range(1, 21)}
    b_totals[_d(1)] = 0.0  # miss
    for n in (2, 3, 4, 5, 6, 7, 8, 9, 10):
        b_totals[_d(n)] = 0.3  # correct wet forecast
    for n in (11, 12, 13):
        b_totals[_d(n)] = 0.3  # false alarms

    c_totals = {_d(n): 0.3 for n in range(1, 11)}  # correct on every wet day
    c_totals.update({_d(n): 0.0 for n in range(11, 21)})
    for n in (11, 12, 13, 14, 15, 16):
        c_totals[_d(n)] = 0.3  # false alarms

    window = RainWindow(
        events=events,
        models={
            "SYN-a/SYN-m": _model("SYN-a", a_totals),
            "SYN-b/SYN-m": _model("SYN-b", b_totals),
            "SYN-c/SYN-m": _model("SYN-c", c_totals),
        },
    )
    with_incumbent = select_model(
        window,
        min_days=20,
        miss_weight=2.0,
        triggers_hundredths=(20,),
        incumbent="SYN-a/SYN-m",
        switch_margin=1,
    )
    a_entry = with_incumbent.entry("SYN-a/SYN-m")
    b_entry = with_incumbent.entry("SYN-b/SYN-m")
    c_entry = with_incumbent.entry("SYN-c/SYN-m")
    assert (a_entry.misses, a_entry.false_alarms, a_entry.penalty) == (2, 1, 5)
    assert (b_entry.misses, b_entry.false_alarms, b_entry.penalty) == (1, 3, 5)
    assert (c_entry.misses, c_entry.false_alarms, c_entry.penalty) == (0, 6, 6)
    assert with_incumbent.best == "SYN-b/SYN-m"
    assert with_incumbent.incumbent_outcome == "kept"
    assert with_incumbent.selected == "SYN-a/SYN-m"

    no_incumbent = select_model(
        window,
        min_days=20,
        miss_weight=2.0,
        triggers_hundredths=(20,),
        incumbent=None,
        switch_margin=1,
    )
    assert no_incumbent.selected == "SYN-b/SYN-m"


def test_eligibility_and_drop_rule() -> None:
    """Row 26: a,b on days 1-20, c on days 1-19 -> c not eligible (minimum
    on with a,b at 20). Then a,b on days 1-40, c on days 31-60 -> c has the
    fewest counted days (30) and is dropped; common becomes a and b's 40
    days; 20 known days (41-60) are not common.
    """
    events = {_d(n): False for n in range(1, 61)}
    window_a = RainWindow(
        events=events,
        models={
            "SYN-a/SYN-m": _model("SYN-a", {_d(n): 0.0 for n in range(1, 21)}),
            "SYN-b/SYN-m": _model("SYN-b", {_d(n): 0.0 for n in range(1, 21)}),
            "SYN-c/SYN-m": _model("SYN-c", {_d(n): 0.0 for n in range(1, 20)}),
        },
    )
    sel_a = select_model(
        window_a,
        min_days=20,
        miss_weight=1.0,
        triggers_hundredths=(20,),
        incumbent=None,
        switch_margin=0,
    )
    assert dict(sel_a.not_eligible) == {"SYN-c/SYN-m": 19}
    assert {k for k, _ in sel_a.eligible} == {"SYN-a/SYN-m", "SYN-b/SYN-m"}

    window_b = RainWindow(
        events=events,
        models={
            "SYN-a/SYN-m": _model("SYN-a", {_d(n): 0.0 for n in range(1, 41)}),
            "SYN-b/SYN-m": _model("SYN-b", {_d(n): 0.0 for n in range(1, 41)}),
            "SYN-c/SYN-m": _model("SYN-c", {_d(n): 0.0 for n in range(31, 61)}),
        },
    )
    sel_b = select_model(
        window_b,
        min_days=20,
        miss_weight=1.0,
        triggers_hundredths=(20,),
        incumbent=None,
        switch_margin=0,
    )
    assert sel_b.dropped == (("SYN-c/SYN-m", 30),)
    assert len(sel_b.common_days) == 40
    assert sel_b.known_days == 60
    assert sel_b.known_not_common == 20
    assert {e.key for e in sel_b.ranked} == {"SYN-a/SYN-m", "SYN-b/SYN-m"}


def test_the_minimum_switches_on() -> None:
    """Row 27: a on 1-30, b on 1-19, c on 1-5 -> minimum off, provisional,
    N=20 (capped), c then b dropped, a ranked alone. Then b moves to
    1-20 -> minimum on, c not eligible, a and b ranked on 20 common days.
    """
    events = {_d(n): False for n in range(1, 31)}
    window_off = RainWindow(
        events=events,
        models={
            "SYN-a/SYN-m": _model("SYN-a", {_d(n): 0.0 for n in range(1, 31)}),
            "SYN-b/SYN-m": _model("SYN-b", {_d(n): 0.0 for n in range(1, 20)}),
            "SYN-c/SYN-m": _model("SYN-c", {_d(n): 0.0 for n in range(1, 6)}),
        },
    )
    sel_off = select_model(
        window_off,
        min_days=20,
        miss_weight=1.0,
        triggers_hundredths=(20,),
        incumbent=None,
        switch_margin=0,
    )
    assert sel_off.provisional is True
    assert sel_off.n_required == 20
    assert sel_off.dropped == (("SYN-c/SYN-m", 5), ("SYN-b/SYN-m", 19))
    assert [e.key for e in sel_off.ranked] == ["SYN-a/SYN-m"]
    assert len(sel_off.common_days) == 30

    window_on = RainWindow(
        events=events,
        models={
            "SYN-a/SYN-m": _model("SYN-a", {_d(n): 0.0 for n in range(1, 31)}),
            "SYN-b/SYN-m": _model("SYN-b", {_d(n): 0.0 for n in range(1, 21)}),
            "SYN-c/SYN-m": _model("SYN-c", {_d(n): 0.0 for n in range(1, 6)}),
        },
    )
    sel_on = select_model(
        window_on,
        min_days=20,
        miss_weight=1.0,
        triggers_hundredths=(20,),
        incumbent=None,
        switch_margin=0,
    )
    assert sel_on.provisional is False
    assert sel_on.n_required == 20
    assert dict(sel_on.not_eligible) == {"SYN-c/SYN-m": 5}
    assert [e.key for e in sel_on.ranked] == ["SYN-a/SYN-m", "SYN-b/SYN-m"]
    assert len(sel_on.common_days) == 20


def test_switching_margin() -> None:
    """Row 29: the run's own margin, daily_shadow.RAIN_SWITCH_MARGIN (3),
    20 common days, incumbent a (penalty 5), b (penalty 3): best is b in
    both cases; 5-3=2 < 3 keeps a. With b's false alarms reduced to 2
    (penalty 2): 5-2=3 >= 3 replaces a with b.
    """
    events = {_d(1): True, **{_d(n): False for n in range(2, 21)}}
    a_totals = {_d(n): 0.0 for n in range(1, 21)}
    for n in (2, 3, 4):
        a_totals[_d(n)] = 0.3
    b_totals = {_d(n): 0.0 for n in range(1, 21)}
    b_totals[_d(1)] = 0.3
    for n in (2, 3, 4):
        b_totals[_d(n)] = 0.3

    window_first = RainWindow(
        events=events,
        models={
            "SYN-a/SYN-m": _model("SYN-a", a_totals),
            "SYN-b/SYN-m": _model("SYN-b", b_totals),
        },
    )
    sel_first = select_model(
        window_first,
        min_days=20,
        miss_weight=2.0,
        triggers_hundredths=(20,),
        incumbent="SYN-a/SYN-m",
        switch_margin=daily_shadow.RAIN_SWITCH_MARGIN,
    )
    assert sel_first.entry("SYN-a/SYN-m").penalty == 5
    assert sel_first.entry("SYN-b/SYN-m").penalty == 3
    assert sel_first.best == "SYN-b/SYN-m"
    assert sel_first.incumbent_outcome == "kept"
    assert sel_first.selected == "SYN-a/SYN-m"

    b_totals_margin = dict(b_totals)
    b_totals_margin[_d(4)] = 0.0  # one fewer false alarm: FA 3 -> 2
    window_second = RainWindow(
        events=events,
        models={
            "SYN-a/SYN-m": _model("SYN-a", a_totals),
            "SYN-b/SYN-m": _model("SYN-b", b_totals_margin),
        },
    )
    sel_second = select_model(
        window_second,
        min_days=20,
        miss_weight=2.0,
        triggers_hundredths=(20,),
        incumbent="SYN-a/SYN-m",
        switch_margin=daily_shadow.RAIN_SWITCH_MARGIN,
    )
    assert sel_second.entry("SYN-b/SYN-m").penalty == 2
    assert sel_second.incumbent_outcome == "replaced_margin"
    assert sel_second.selected == "SYN-b/SYN-m"


def test_incumbent_with_no_forecast() -> None:
    """Row 30: incumbent a has no exact-cover forecast for the target
    (covers_target=False) -> replaced at once regardless of score,
    selected = the only remaining eligible model, b.
    """
    window = RainWindow(
        events={_d(1): False},
        models={
            "SYN-a/SYN-m": _model("SYN-a", {}, covers=False),
            "SYN-b/SYN-m": _model("SYN-b", {_d(1): 0.0}),
        },
    )
    selection = select_model(
        window,
        min_days=1,
        miss_weight=1.0,
        triggers_hundredths=(20,),
        incumbent="SYN-a/SYN-m",
        switch_margin=0,
    )
    assert selection.incumbent_outcome == "replaced_no_forecast"
    assert selection.selected == "SYN-b/SYN-m"


def test_incumbent_no_longer_eligible() -> None:
    """Row 31: incumbent a counted on 19 days (below min_days=20, so not
    eligible once b and c reach 20) -> replaced at once; selected = the
    best-scoring eligible model, b (penalty 3 vs c's 4).
    """
    events = {_d(n): False for n in range(1, 21)}
    a_totals = {_d(n): 0.0 for n in range(1, 20)}  # 19 days, no misses/FA
    b_totals = {_d(n): 0.0 for n in range(1, 21)}
    for n in (1, 2, 3):
        b_totals[_d(n)] = 0.3  # 3 false alarms
    c_totals = {_d(n): 0.0 for n in range(1, 21)}
    for n in (1, 2, 3, 4):
        c_totals[_d(n)] = 0.3  # 4 false alarms

    window = RainWindow(
        events=events,
        models={
            "SYN-a/SYN-m": _model("SYN-a", a_totals),
            "SYN-b/SYN-m": _model("SYN-b", b_totals),
            "SYN-c/SYN-m": _model("SYN-c", c_totals),
        },
    )
    selection = select_model(
        window,
        min_days=20,
        miss_weight=1.0,
        triggers_hundredths=(20,),
        incumbent="SYN-a/SYN-m",
        switch_margin=0,
    )
    assert dict(selection.not_eligible) == {"SYN-a/SYN-m": 19}
    assert selection.entry("SYN-b/SYN-m").penalty == 3
    assert selection.entry("SYN-c/SYN-m").penalty == 4
    assert selection.incumbent_outcome == "replaced_not_eligible"
    assert selection.selected == "SYN-b/SYN-m"


def test_the_margin_never_changes_the_group() -> None:
    """Row 32: switch_margin=3, J=4, 20 common days. b (penalty 3), c and
    d tie at penalty 4 (c before d by (source, model)), a (penalty 5,
    incumbent). a stays selected (5-3=2 < 3). The group (q=3) is cut from
    the SCORE order [b, c, d, a] -> [b, c, d], never an order with the
    selected model moved first; its target totals are all 0.0 -> k=0, no
    symbol, chance 0. The served amount is still a's own 1.0mm.
    """
    events = {_d(1): True, **{_d(n): False for n in range(2, 21)}}

    def _totals(miss_day1: bool, fa_days: tuple[int, ...]) -> dict[date, float]:
        totals = {_d(n): 0.0 for n in range(1, 21)}
        if not miss_day1:
            totals[_d(1)] = 0.3
        for n in fa_days:
            totals[_d(n)] = 0.3
        return totals

    a_totals = _totals(miss_day1=True, fa_days=(2, 3, 4))  # misses 1, FA 3 -> 5
    b_totals = _totals(miss_day1=False, fa_days=(2, 3, 4))  # misses 0, FA 3 -> 3
    c_totals = _totals(miss_day1=True, fa_days=(2, 3))  # misses 1, FA 2 -> 4
    d_totals = _totals(miss_day1=True, fa_days=(2, 3))  # misses 1, FA 2 -> 4

    window = RainWindow(
        events=events,
        models={
            "SYN-a/SYN-m": _model("SYN-a", a_totals),
            "SYN-b/SYN-m": _model("SYN-b", b_totals),
            "SYN-c/SYN-m": _model("SYN-c", c_totals),
            "SYN-d/SYN-m": _model("SYN-d", d_totals),
        },
    )
    selection = select_model(
        window,
        min_days=20,
        miss_weight=2.0,
        triggers_hundredths=(20,),
        incumbent="SYN-a/SYN-m",
        switch_margin=3,
    )
    assert [e.key for e in selection.ranked] == [
        "SYN-b/SYN-m",
        "SYN-c/SYN-m",
        "SYN-d/SYN-m",
        "SYN-a/SYN-m",
    ]
    assert selection.incumbent_outcome == "kept"
    assert selection.selected == "SYN-a/SYN-m"

    ranked_totals = {
        "SYN-a/SYN-m": 1.0,
        "SYN-b/SYN-m": 0.0,
        "SYN-c/SYN-m": 0.0,
        "SYN-d/SYN-m": 0.0,
    }
    order = [(item.key, ranked_totals[item.key]) for item in selection.ranked]
    vote = group_vote_chance(
        order, group_fraction=Fraction(3, 4), trigger_hundredths=20
    )
    assert vote.q == 3
    assert vote.members == ("SYN-b/SYN-m", "SYN-c/SYN-m", "SYN-d/SYN-m")
    assert vote.k == 0
    assert vote.chance == Fraction(0)

    hours = tuple(datetime(2035, 1, 2, h) for h in range(24))
    samples = {"SYN-a/SYN-m": tuple((h, 0.0) for h in hours)}
    served = served_rain(
        selection,
        ranked_totals,
        samples,
        hours=hours,
        group_fraction=Fraction(3, 4),
        trigger_hundredths=20,
        miss_weight=2,
        threshold_mm=0.1,
    )
    assert served.amount == pytest.approx(1.0)  # pyright: ignore[reportUnknownMemberType]
    assert served.chance == Fraction(0)
    assert served.symbol is False


def test_group_vote_tie_at_the_cut() -> None:
    """Row 44: four models, trigger 20, 20 common days, no incumbent.

    a: 0 misses/0 FA (penalty 0); b: 0/1 (penalty 1); c: 1/1 (penalty 3);
    d: 0/3 (penalty 3). c and d tie at penalty 3, but d's fewer misses (0
    vs 1) puts it ahead of c -> ranked order [a, b, d, c]. q=ceil(3)=3 ->
    the group [a, b, d] excludes c. Target totals are 0.0 except d's
    0.5mm -> k=1, chance 1/3.

    With d's own misses/FA changed to 1/1 (same penalty 3, now tied with
    c on misses too), the (source, model) tie-break puts c ahead of d ->
    group [a, b, c] excludes d -> k=0, chance 0.

    Reverting d and setting the incumbent to c changes nothing about the
    group (built from the ranked SCORE order, not from who is selected):
    group is [a, b, d] again, chance 1/3.

    Each case is also read through `served_rain`, whose group must be the
    same vote: cut from the score order, misses included.
    """
    events = {_d(1): True, **{_d(n): False for n in range(2, 21)}}

    def _totals(wet_day1: bool, fa_days: tuple[int, ...]) -> dict[date, float]:
        totals = {_d(n): 0.0 for n in range(1, 21)}
        if wet_day1:
            totals[_d(1)] = 0.3
        for n in fa_days:
            totals[_d(n)] = 0.3
        return totals

    a_totals = _totals(wet_day1=True, fa_days=())  # 0 misses, 0 FA
    b_totals = _totals(wet_day1=True, fa_days=(2,))  # 0 misses, 1 FA
    c_totals = _totals(wet_day1=False, fa_days=(2,))  # 1 miss, 1 FA
    d_totals_scenario1 = _totals(wet_day1=True, fa_days=(2, 3, 4))  # 0 misses, 3 FA

    def _window(d_totals: dict[date, float]) -> RainWindow:
        return RainWindow(
            events=events,
            models={
                "SYN-a/SYN-m": _model("SYN-a", a_totals),
                "SYN-b/SYN-m": _model("SYN-b", b_totals),
                "SYN-c/SYN-m": _model("SYN-c", c_totals),
                "SYN-d/SYN-m": _model("SYN-d", d_totals),
            },
        )

    ranked_totals = {
        "SYN-a/SYN-m": 0.0,
        "SYN-b/SYN-m": 0.0,
        "SYN-c/SYN-m": 0.0,
        "SYN-d/SYN-m": 0.5,
    }

    def _group_vote(selection: Selection) -> GroupVote:
        order = [(item.key, ranked_totals[item.key]) for item in selection.ranked]
        return group_vote_chance(
            order, group_fraction=Fraction(3, 4), trigger_hundredths=20
        )

    def _served(selection: Selection) -> ServedRain:
        return served_rain(
            selection,
            ranked_totals,
            {},
            hours=(),
            group_fraction=Fraction(3, 4),
            trigger_hundredths=20,
            miss_weight=2.0,
            threshold_mm=0.1,
        )

    selection1 = select_model(
        _window(d_totals_scenario1),
        min_days=20,
        miss_weight=2.0,
        triggers_hundredths=(20,),
        incumbent=None,
        switch_margin=0,
    )
    assert [e.key for e in selection1.ranked] == [
        "SYN-a/SYN-m",
        "SYN-b/SYN-m",
        "SYN-d/SYN-m",
        "SYN-c/SYN-m",
    ]
    vote1 = _group_vote(selection1)
    assert vote1.q == 3
    assert vote1.members == ("SYN-a/SYN-m", "SYN-b/SYN-m", "SYN-d/SYN-m")
    assert vote1.k == 1
    assert vote1.chance == Fraction(1, 3)
    served1 = _served(selection1)
    assert served1.group == vote1
    assert served1.chance == Fraction(1, 3)

    d_totals_scenario2 = _totals(wet_day1=False, fa_days=(2,))  # 1 miss, 1 FA
    selection2 = select_model(
        _window(d_totals_scenario2),
        min_days=20,
        miss_weight=2.0,
        triggers_hundredths=(20,),
        incumbent=None,
        switch_margin=0,
    )
    assert [e.key for e in selection2.ranked] == [
        "SYN-a/SYN-m",
        "SYN-b/SYN-m",
        "SYN-c/SYN-m",
        "SYN-d/SYN-m",
    ]
    vote2 = _group_vote(selection2)
    assert vote2.q == 3
    assert vote2.members == ("SYN-a/SYN-m", "SYN-b/SYN-m", "SYN-c/SYN-m")
    assert vote2.k == 0
    assert vote2.chance == Fraction(0)
    served2 = _served(selection2)
    assert served2.group == vote2
    assert served2.chance == Fraction(0)

    selection3 = select_model(
        _window(d_totals_scenario1),
        min_days=20,
        miss_weight=2.0,
        triggers_hundredths=(20,),
        incumbent="SYN-c/SYN-m",
        switch_margin=0,
    )
    assert [e.key for e in selection3.ranked] == [
        "SYN-a/SYN-m",
        "SYN-b/SYN-m",
        "SYN-d/SYN-m",
        "SYN-c/SYN-m",
    ]
    vote3 = _group_vote(selection3)
    assert vote3.q == 3
    assert vote3.members == ("SYN-a/SYN-m", "SYN-b/SYN-m", "SYN-d/SYN-m")
    assert vote3.k == 1
    assert vote3.chance == Fraction(1, 3)
    served3 = _served(selection3)
    assert served3.group == vote3
    assert served3.chance == Fraction(1, 3)


def test_trigger_grid_tie_picks_the_lower_of_the_two() -> None:
    """Row 37: one model's window has no total in [0.2mm, 0.3mm) -- its
    scores at 0.2mm and 0.3mm are equal, and both beat 0.1mm, 0.5mm and
    1.0mm. A 0.35mm wet day is correctly caught at all three low
    triggers, but missed at 0.5 and 1.0mm; a 0.15mm dry day is a false
    alarm only at 0.1mm. strict `<` on the sorted triggers keeps 0.2mm
    (the first-seen of the tie) over 0.3mm.
    """
    events = {_d(1): True, _d(2): False}
    totals = {_d(1): 0.35, _d(2): 0.15}
    window = RainWindow(events=events, models={"SYN-a/SYN-m": _model("SYN-a", totals)})
    selection = select_model(
        window,
        min_days=1,
        miss_weight=1.0,
        triggers_hundredths=(10, 20, 30, 50, 100),
        incumbent=None,
        switch_margin=0,
    )
    entry = selection.entry("SYN-a/SYN-m")
    assert entry.trigger_hundredths == 20
    assert entry.penalty == 0
