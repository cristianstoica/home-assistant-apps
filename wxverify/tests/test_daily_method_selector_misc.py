"""§15.2 rows 25, 28: common-day rate (A15's own-days benchmark) and the
provisional few-days case (§9.1, §9.2).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from fractions import Fraction

from wxverify.forecast.daily_method import (
    ModelWindow,
    RainWindow,
    climatology_chance,
    rank_own_days,
    select_model,
    served_rain,
)

_BASE = date(2035, 1, 1)


def _d(n: int) -> date:
    return _BASE + timedelta(days=n)


def _model(source: str, totals: dict[date, float]) -> ModelWindow:
    return ModelWindow(source=source, model="SYN-m", totals=totals, covers_target=True)


def test_common_day_rate() -> None:
    """Row 25: a counted on days 1-40, b on days 1-25; on the 25 common
    days a has 1 false alarm, b has 2 -> select_model picks a (1/25 vs
    2/25). But rank_own_days scores each on its OWN counted days: a also
    has 3 more false alarms on days 26-40 (4/40 total), while b's score
    stays 2/25 -- 2/25 < 4/40, so A15's benchmark picks b instead.
    """
    events = {_d(n): False for n in range(1, 41)}  # every day dry
    a_totals = {_d(n): 0.0 for n in range(1, 41)}
    a_totals[_d(1)] = 0.3  # false alarm within days 1-25
    for n in (26, 27, 28):
        a_totals[_d(n)] = 0.3  # 3 more false alarms on days 26-40
    b_totals = {_d(n): 0.0 for n in range(1, 26)}
    b_totals[_d(1)] = 0.3
    b_totals[_d(2)] = 0.3  # 2 false alarms, both within days 1-25

    window = RainWindow(
        events=events,
        models={
            "SYN-a/SYN-m": _model("SYN-a", a_totals),
            "SYN-b/SYN-m": _model("SYN-b", b_totals),
        },
    )
    selection = select_model(
        window,
        min_days=25,
        miss_weight=1.0,
        triggers_hundredths=(20,),
        incumbent=None,
        switch_margin=0,
    )
    assert len(selection.common_days) == 25
    assert selection.entry("SYN-a/SYN-m").penalty == 1
    assert selection.entry("SYN-b/SYN-m").penalty == 2
    assert selection.selected == "SYN-a/SYN-m"

    own_days = rank_own_days(window, min_days=1, miss_weight=1.0, trigger_hundredths=20)
    by_key = {e.key: e for e in own_days}
    a_own = by_key["SYN-a/SYN-m"]
    b_own = by_key["SYN-b/SYN-m"]
    assert (a_own.false_alarms, a_own.counted_days) == (4, 40)
    assert (b_own.false_alarms, b_own.counted_days) == (2, 25)
    assert a_own.score == Fraction(4, 40)
    assert b_own.score == Fraction(2, 25)
    assert own_days[0].key == "SYN-b/SYN-m"  # b's lower score (0.08 < 0.1) wins


def test_provisional_few_days() -> None:
    """Row 28: three models each counted on the window's only 3 known
    days, all tied at penalty 0 -> provisional, N=3. The group (J=3,
    group_fraction 3/4 -> q=3) holds every model; target totals a=0.0,
    b=0.0, c=0.3mm give k=1, symbol (1*3>=3), chance 1/3.
    climatology_chance over the same 3 known days is clim_short.
    """
    events = {_d(1): False, _d(2): False, _d(3): False}
    scoring_totals = {_d(1): 0.0, _d(2): 0.0, _d(3): 0.0}
    window = RainWindow(
        events=events,
        models={
            "SYN-a/SYN-m": _model("SYN-a", scoring_totals),
            "SYN-b/SYN-m": _model("SYN-b", scoring_totals),
            "SYN-c/SYN-m": _model("SYN-c", scoring_totals),
        },
    )
    selection = select_model(
        window,
        min_days=20,
        miss_weight=2.0,
        triggers_hundredths=(20,),
        incumbent=None,
        switch_margin=0,
    )
    assert selection.provisional is True
    assert selection.n_required == 3
    assert [e.key for e in selection.ranked] == [
        "SYN-a/SYN-m",
        "SYN-b/SYN-m",
        "SYN-c/SYN-m",
    ]

    ranked_totals = {
        "SYN-a/SYN-m": 0.0,
        "SYN-b/SYN-m": 0.0,
        "SYN-c/SYN-m": 0.3,
    }
    hour = (datetime(2035, 1, 10, 0),)
    samples = {"SYN-a/SYN-m": ((hour[0], 0.0),)}
    served = served_rain(
        selection,
        ranked_totals,
        samples,
        hours=hour,
        group_fraction=Fraction(3, 4),
        trigger_hundredths=20,
        miss_weight=2,
        threshold_mm=0.1,
    )
    assert served.group is not None
    assert served.group.q == 3
    assert served.group.k == 1
    assert served.symbol is True
    assert served.chance == Fraction(1, 3)
    assert served.provisional is True
    assert served.n_required == 3

    clim = climatology_chance(events, min_days=20)
    assert clim.chance is None
    assert clim.fallback_reason == "clim_short"
    assert clim.known_days == 3
