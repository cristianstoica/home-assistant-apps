"""§15.2 rows 46, 51-55: no ranked model, the served rain and the hourly line.

Each ``Selection`` used below comes from a real `select_model` call (never a
hand-built dataclass standing in for it): one common day on which every
model forecasts dry with no miss and no false alarm, so every candidate
ties at penalty 0 and the (source, model) tie-break alone decides the
order -- exactly the "ranked in that order" alphabetical ranking the
plan's rows assume for SYN-a.. SYN-f / SYN-a..SYN-d.
"""

from __future__ import annotations

from datetime import date, datetime
from fractions import Fraction
from zoneinfo import ZoneInfo

import pytest

from wxverify.forecast.daily_method import (
    ModelWindow,
    RainWindow,
    Selection,
    counted_hours,
    select_model,
    selected_hourly,
    served_rain,
)

_DAY = date(2035, 1, 1)
_HOURS = tuple(datetime(2035, 1, 1, h) for h in range(24))


def _ranked_selection(keys: list[str], *, min_days: int = 1) -> Selection:
    """A Selection where every model ties at penalty 0 -- order = (source, model)."""
    models = {
        key: ModelWindow(
            source=key.split("/")[0],
            model=key.split("/")[1],
            totals={_DAY: 0.0},
            covers_target=True,
        )
        for key in keys
    }
    window = RainWindow(events={_DAY: False}, models=models)
    return select_model(
        window,
        min_days=min_days,
        miss_weight=1.0,
        triggers_hundredths=(20,),
        incumbent=None,
        switch_margin=0,
    )


def test_no_ranked_model_serves_nothing() -> None:
    """Row 46: select_model's no_eligible (no model covers the target) ->
    served_rain reports no_eligible and nothing else."""
    window = RainWindow(events={_DAY: False}, models={})
    selection = select_model(
        window,
        min_days=1,
        miss_weight=1.0,
        triggers_hundredths=(20,),
        incumbent=None,
        switch_margin=0,
    )
    assert selection.no_eligible is True
    served = served_rain(
        selection,
        {},
        {},
        hours=_HOURS,
        group_fraction=Fraction(3, 4),
        trigger_hundredths=20,
        miss_weight=2,
        threshold_mm=0.1,
    )
    assert served.no_eligible is True
    assert served.selected is None
    assert served.symbol is None
    assert served.chance is None
    assert served.amount is None
    assert served.hours_counted is None
    assert served.hourly is None


def _six_model_selection() -> Selection:
    keys = [f"SYN-{c}/SYN-m" for c in "abcdef"]
    return _ranked_selection(keys)


def test_served_rain_symbol_off_while_selected_is_wet() -> None:
    """Row 51: SYN-a selected (alphabetical tie); SYN-a 0.3mm at 14:00 only.

    group_fraction 3/4, J=6 -> q=ceil(4.5)=5, the top 5 (a..e) hold SYN-a
    alone at or above the 0.2mm trigger -> k=1; 1*(1+2)=3 < 5 -> no symbol,
    chance 1/5. amount = SYN-a's own total 0.3mm (shown without the
    symbol); N h = 1 (only 14:00 clears T=0.1mm); the hourly line is SYN-a's
    24 values, unchanged.
    """
    selection = _six_model_selection()
    assert selection.selected == "SYN-a/SYN-m"
    ranked_totals = {f"SYN-{c}/SYN-m": 0.0 for c in "abcdef"}
    ranked_totals["SYN-a/SYN-m"] = 0.3
    line = tuple(0.3 if h.hour == 14 else 0.0 for h in _HOURS)
    samples = {"SYN-a/SYN-m": tuple(zip(_HOURS, line, strict=True))}
    served = served_rain(
        selection,
        ranked_totals,
        samples,
        hours=_HOURS,
        group_fraction=Fraction(3, 4),
        trigger_hundredths=20,
        miss_weight=2,
        threshold_mm=0.1,
    )
    assert served.group is not None
    assert served.group.q == 5
    assert served.group.k == 1
    assert served.chance == Fraction(1, 5)
    assert served.symbol is False
    assert served.amount == pytest.approx(0.3)  # pyright: ignore[reportUnknownMemberType]
    assert served.hours_counted == 1
    assert served.hourly == line


def test_served_rain_symbol_on_while_selected_is_dry() -> None:
    """Row 52: the same six-model selection; SYN-a stays dry (0.0 total);
    SYN-b/c/d are at 0.5mm at 10:00 in the group's top 5 -> k=3, symbol on,
    but the served amount is still SYN-a's own 0.0mm (never the group's)
    and N h is 0.
    """
    selection = _six_model_selection()
    ranked_totals = {f"SYN-{c}/SYN-m": 0.0 for c in "abcdef"}
    ranked_totals["SYN-b/SYN-m"] = 0.5
    ranked_totals["SYN-c/SYN-m"] = 0.5
    ranked_totals["SYN-d/SYN-m"] = 0.5
    zero_line = tuple(0.0 for _ in _HOURS)
    samples = {"SYN-a/SYN-m": tuple(zip(_HOURS, zero_line, strict=True))}
    served = served_rain(
        selection,
        ranked_totals,
        samples,
        hours=_HOURS,
        group_fraction=Fraction(3, 4),
        trigger_hundredths=20,
        miss_weight=2,
        threshold_mm=0.1,
    )
    assert served.group is not None
    assert served.group.q == 5
    assert served.group.k == 3
    assert served.chance == Fraction(3, 5)
    assert served.symbol is True
    assert served.amount == pytest.approx(0.0)  # pyright: ignore[reportUnknownMemberType]
    assert served.hours_counted == 0
    assert served.hourly == zero_line


def test_hourly_line_missing_hours() -> None:
    """Row 53: SYN-a's samples omit 02:00 and 03:00 of a 24-hour day.

    selected_hourly reports None at those two hours and the stored values
    elsewhere; counted_hours on that line is None (a missing hour spoils
    the count); served_rain then has no amount and no N h, because its own
    counted_hours call on the selected model's line sees the same gap.
    Calling selected_hourly with no samples at all gives "unavailable".
    """
    samples = tuple((h, 0.1) for h in _HOURS if h.hour not in (2, 3))
    line = selected_hourly(samples, hours=_HOURS)
    assert line[2] is None
    assert line[3] is None
    assert all(v == pytest.approx(0.1) for i, v in enumerate(line) if i not in (2, 3))  # pyright: ignore[reportUnknownMemberType]
    assert counted_hours(line, threshold_mm=0.05) is None

    selection = _six_model_selection()
    ranked_totals = {f"SYN-{c}/SYN-m": 0.0 for c in "abcdef"}
    served = served_rain(
        selection,
        ranked_totals,
        {"SYN-a/SYN-m": samples},
        hours=_HOURS,
        group_fraction=Fraction(3, 4),
        trigger_hundredths=20,
        miss_weight=2,
        threshold_mm=0.05,
    )
    assert served.amount is None
    assert served.hours_counted is None

    assert selected_hourly((), hours=_HOURS) == "unavailable"


def test_hourly_line_unscaled() -> None:
    """Row 54: four models a-d tie alphabetically (a selected); a..c form
    the q=3 group (median 0.4mm); the served amount is SYN-a's own 1.0mm;
    the line holds SYN-a's raw 0.6 and 0.4 mm values, never divided by 100
    (the trap a mutant that scales by 1/100 would fail, M61 in the table).
    Then the spring-forward day of 2035 in America/New_York has only 23
    distinct local hours, so a 23-entry hour list still returns the stored
    values unchanged.
    """
    keys = [f"SYN-{c}/SYN-m" for c in "abcd"]
    selection = _ranked_selection(keys)
    assert selection.selected == "SYN-a/SYN-m"
    ranked_totals = {
        "SYN-a/SYN-m": 1.0,
        "SYN-b/SYN-m": 0.4,
        "SYN-c/SYN-m": 0.0,
        "SYN-d/SYN-m": 2.0,
    }
    line = tuple(0.6 if h.hour == 9 else (0.4 if h.hour == 15 else 0.0) for h in _HOURS)
    samples = {"SYN-a/SYN-m": tuple(zip(_HOURS, line, strict=True))}
    served = served_rain(
        selection,
        ranked_totals,
        samples,
        hours=_HOURS,
        group_fraction=Fraction(3, 4),
        trigger_hundredths=20,
        miss_weight=2,
        threshold_mm=0.1,
    )
    assert served.group is not None
    assert served.group.q == 3
    assert served.amount == pytest.approx(1.0)  # pyright: ignore[reportUnknownMemberType]
    assert served.hourly == line
    assert isinstance(served.hourly, tuple)
    assert 0.6 in served.hourly  # not 0.006 (a /100 scaling bug)

    tz = ZoneInfo("America/New_York")
    spring_forward = date(2035, 3, 11)
    local_hours: list[datetime] = []
    for h in range(24):
        if h == 2:
            continue  # 02:00 local does not exist on the spring-forward day
        local_hours.append(datetime(2035, 3, 11, h, tzinfo=tz))
    assert len(local_hours) == 23
    dst_samples = tuple((h, 0.2) for h in local_hours)
    dst_line = selected_hourly(dst_samples, hours=tuple(local_hours))
    assert dst_line == tuple(0.2 for _ in range(23))
    assert spring_forward.isoformat() == "2035-03-11"


def test_small_amounts_kept_as_calculated() -> None:
    """Row 55: SYN-a's 14:00 value 0.04mm, then 0.15mm -- unrounded amounts.

    Reusing row 51's six-model dry-selected setup: the served amount is the
    selected model's own ranked total, carried through with no rounding to
    a 0.01mm line.
    """
    selection = _six_model_selection()
    for small_amount in (0.04, 0.15):
        ranked_totals = {f"SYN-{c}/SYN-m": 0.0 for c in "abcdef"}
        ranked_totals["SYN-a/SYN-m"] = small_amount
        line = tuple(small_amount if h.hour == 14 else 0.0 for h in _HOURS)
        samples = {"SYN-a/SYN-m": tuple(zip(_HOURS, line, strict=True))}
        served = served_rain(
            selection,
            ranked_totals,
            samples,
            hours=_HOURS,
            group_fraction=Fraction(3, 4),
            trigger_hundredths=20,
            miss_weight=2,
            threshold_mm=0.5,
        )
        assert served.amount == pytest.approx(small_amount)  # pyright: ignore[reportUnknownMemberType]
        assert served.hourly == line


def test_served_rain_group_cut_ignores_the_incumbent() -> None:
    """Supplementary to row 32 ("the margin never changes the group"):
    that row's own test reimplements the group cut from `selection.ranked`
    directly, never calling `served_rain` -- it cannot see a mutation
    inside `served_rain`'s own order-building step (M42's trap exactly:
    the test's literal fixture never reaches the mutated line). This test
    drives the same invariant through `served_rain` itself.

    Six models tie at penalty 0 (alphabetical ranked order a..f); the
    incumbent is SYN-f, the LAST-ranked model, not a top-5 member. group
    is built from the ranked SCORE order, not from the incumbent's
    identity, so the q=5 group stays [a, b, c, d, e] and excludes f. c, d
    and e are wet (0.25mm, >= the 0.2mm trigger) -> k=3, chance 3/5.

    A mutant that puts the incumbent first before the cut gives group
    [f, a, b, c, d] instead: f is dry (0 of the 3 wet members), e drops
    out of the group, leaving only c and d wet -> k=2, chance 2/5 --
    distinguishable from the correct 3/5.
    """
    keys = [f"SYN-{c}/SYN-m" for c in "abcdef"]
    models = {
        key: ModelWindow(
            source=key.split("/")[0],
            model=key.split("/")[1],
            totals={_DAY: 0.0},
            covers_target=True,
        )
        for key in keys
    }
    window = RainWindow(events={_DAY: False}, models=models)
    selection = select_model(
        window,
        min_days=1,
        miss_weight=1.0,
        triggers_hundredths=(20,),
        incumbent="SYN-f/SYN-m",
        switch_margin=0,
    )
    assert [e.key for e in selection.ranked] == keys
    assert selection.incumbent == "SYN-f/SYN-m"

    ranked_totals = {f"SYN-{c}/SYN-m": 0.0 for c in "abcdef"}
    ranked_totals["SYN-c/SYN-m"] = 0.25
    ranked_totals["SYN-d/SYN-m"] = 0.25
    ranked_totals["SYN-e/SYN-m"] = 0.25
    served = served_rain(
        selection,
        ranked_totals,
        {},
        hours=_HOURS,
        group_fraction=Fraction(3, 4),
        trigger_hundredths=20,
        miss_weight=2,
        threshold_mm=0.1,
    )
    assert served.group is not None
    assert served.group.q == 5
    assert served.group.members == (
        "SYN-a/SYN-m",
        "SYN-b/SYN-m",
        "SYN-c/SYN-m",
        "SYN-d/SYN-m",
        "SYN-e/SYN-m",
    )
    assert served.group.k == 3
    assert served.chance == Fraction(3, 5)


def test_served_rain_hourly_line_stays_the_selected_models_own() -> None:
    """Supplementary to rows 51/52 ("the hourly line taken from the SUT's
    selected model"): those rows only supply samples for the selected model
    (SYN-a), so a mutant that falls back to the group's own average when
    a group member's line is unavailable passes them vacuously -- every
    other group member IS unavailable there, so the fallback branch never
    distinguishes `selected_hourly(selected)` from an average.

    Three models a-c tie alphabetically (a selected); group_fraction=1
    puts all three in the q=3 group, and all three get a full 24-hour
    line with no missing hour, so an averaging mutant has everything it
    needs to diverge. a's own line is flat 1.0mm; b and c are flat 3.0mm
    and 5.0mm. The group mean would be flat 3.0mm -- the served line must
    stay a's own flat 1.0mm, per the docstring's "never... the group".
    """
    keys = [f"SYN-{c}/SYN-m" for c in "abc"]
    selection = _ranked_selection(keys)
    assert selection.selected == "SYN-a/SYN-m"
    ranked_totals = {"SYN-a/SYN-m": 1.0, "SYN-b/SYN-m": 3.0, "SYN-c/SYN-m": 5.0}
    a_line = tuple(1.0 for _ in _HOURS)
    b_line = tuple(3.0 for _ in _HOURS)
    c_line = tuple(5.0 for _ in _HOURS)
    samples = {
        "SYN-a/SYN-m": tuple(zip(_HOURS, a_line, strict=True)),
        "SYN-b/SYN-m": tuple(zip(_HOURS, b_line, strict=True)),
        "SYN-c/SYN-m": tuple(zip(_HOURS, c_line, strict=True)),
    }
    served = served_rain(
        selection,
        ranked_totals,
        samples,
        hours=_HOURS,
        group_fraction=Fraction(1, 1),
        trigger_hundredths=20,
        miss_weight=2,
        threshold_mm=0.1,
    )
    assert served.group is not None
    assert served.group.members == tuple(keys)
    assert served.hourly == a_line
    assert served.hourly != b_line
    assert served.hourly is not None
    assert all(v == pytest.approx(1.0) for v in served.hourly)  # pyright: ignore[reportUnknownMemberType]
