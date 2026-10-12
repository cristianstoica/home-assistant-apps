"""§15.3 "Split rule" (plan :1396): opposite signs in two large segments.

Two segments with 20 dates at every lead and point Deltas of opposite
signs make a sign flip, so a PASS becomes INCONCLUSIVE and nothing better;
one segment with 19 dates at one lead is not large, so the rule does not
apply. Lines are built directly; ``split_segments``, ``segment_report`` and
``candidate_outcome`` are the production functions.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import cast

from wxverify.forecast import daily_method
from wxverify.verification import daily_shadow

_BASE = date(2031, 3, 1)
_PASSING_TEMP_GUARD: dict[str, dict[str, bool]] = {
    "temperature_high": {"pass": True},
    "temperature_low": {"pass": True},
}
_GUARDS: dict[str, object] = {
    "G2": _PASSING_TEMP_GUARD,
    "G4": _PASSING_TEMP_GUARD,
    "G3": {"pass": True},
    "frost": {"pass": True},
}
_CHECKS: dict[str, bool] = {"f1_pass": True, "minimums_met": True}


def _candidate(value: float) -> daily_method.CandidateValue:
    return daily_method.CandidateValue(
        name="X",
        value=value,
        regime="per_lead",
        eligible=(),
        weights={},
        biases={},
        ranks=(),
        flags=(),
        fallback_reason=None,
    )


def _line(
    *,
    index: int,
    lead: int,
    quantity: str,
    segment: str,
    control: float,
    truth: float,
    value: float,
) -> daily_shadow.TempLine:
    snapshot = _BASE + timedelta(days=index)
    return daily_shadow.TempLine(
        key=daily_shadow.CaseKey(
            snapshot_date=snapshot,
            lead=lead,
            target=snapshot + timedelta(days=lead),
        ),
        quantity=quantity,
        row=None,
        unmatched=None,
        control=control,
        truth=truth,
        f1=True,
        segment=(segment,),
        candidates={"X": _candidate(value)},
    )


def _two_segments(
    *, drop: tuple[int, int] | None = None
) -> list[daily_shadow.TempLine]:
    """40 dates split into a first segment ("A", indices 0-19) where ``X``
    beats control (point Delta < 0) and a second segment ("B", indices
    20-39) where ``X`` loses to control (point Delta > 0) -- opposite
    signs across the split. ``drop`` removes one (index, lead) case at
    both quantities, shrinking that lead's date count by one.
    """
    lines: list[daily_shadow.TempLine] = []
    for index in range(40):
        for lead in daily_shadow.SCORED_LEADS:
            for quantity in daily_shadow.TEMPERATURE_QUANTITIES:
                if drop is not None and (index, lead) == drop:
                    continue
                if index < 20:
                    lines.append(
                        _line(
                            index=index,
                            lead=lead,
                            quantity=quantity,
                            segment="A",
                            control=12.0,
                            truth=10.0,
                            value=10.0,
                        )
                    )
                else:
                    lines.append(
                        _line(
                            index=index,
                            lead=lead,
                            quantity=quantity,
                            segment="B",
                            control=10.0,
                            truth=10.0,
                            value=12.0,
                        )
                    )
    return lines


def _point(entry: dict[str, object]) -> float:
    points = cast("dict[str, float | None]", entry["point_delta"])
    point = points["X"]
    assert point is not None
    return point


def _tile(*, point: float, upper: float) -> daily_shadow.Bounds:
    return daily_shadow.Bounds(
        point=point,
        lower=point - 1.0,
        upper=upper,
        kept=1000,
        discarded=0,
        level=0.95,
    )


def _outcome(tile: daily_shadow.Bounds, *, flip: bool) -> daily_shadow.CandidateOutcome:
    return daily_shadow.candidate_outcome(
        "X",
        tile=tile,
        missing=0,
        guards=_GUARDS,
        twin=None,
        twin_missing=0,
        checks=_CHECKS,
        flip=flip,
        station_set_changed=None,
        db_import_changed=False,
    )


def test_two_large_segments_with_opposite_signs_flip() -> None:
    """Two full 20-date segments with opposite-sign point Deltas at every
    lead make ``segment_report`` flip ``X``."""
    assert daily_shadow.SPLIT_MIN_DATES == 20
    segments = daily_shadow.split_segments(_two_segments())
    assert len(segments) == 2
    report, flips = daily_shadow.segment_report(segments, ["X"])
    assert [entry["key"] for entry in report] == [("A",), ("B",)]
    for entry in report:
        assert entry["large"] is True
        assert entry["dates_per_lead"] == {
            str(lead): 20 for lead in daily_shadow.SCORED_LEADS
        }
    assert _point(report[0]) < 0 < _point(report[1])
    assert flips == {"X": True}


def test_a_segment_with_19_dates_at_one_lead_does_not_apply() -> None:
    """Dropping one (index, lead) case from segment B leaves it with only
    19 dates at that lead -- not large -- so the split rule does not fire
    even though the signs still differ."""
    report, flips = daily_shadow.segment_report(
        daily_shadow.split_segments(_two_segments(drop=(25, 3))), ["X"]
    )
    assert report[0]["large"] is True
    assert report[1]["large"] is False
    assert report[1]["dates_per_lead"] == {
        **{str(lead): 20 for lead in daily_shadow.SCORED_LEADS},
        "3": 19,
    }
    assert _point(report[0]) < 0 < _point(report[1])
    assert flips == {"X": False}


def test_a_segment_with_19_dates_at_one_quantity_does_not_apply() -> None:
    """Segment B one date short at lead 3 for ``temperature_low`` alone is
    not large: a segment needs 20 dates at every lead for both quantities."""
    lines = [
        line
        for line in _two_segments()
        if not (
            line.key.snapshot_date == _BASE + timedelta(days=25)
            and line.key.lead == 3
            and line.quantity == "temperature_low"
        )
    ]
    report, flips = daily_shadow.segment_report(
        daily_shadow.split_segments(lines), ["X"]
    )
    assert report[1]["dates_per_lead"] == {
        **{str(lead): 20 for lead in daily_shadow.SCORED_LEADS},
        "3": 19,
    }
    assert report[1]["large"] is False
    assert flips == {"X": False}


def test_a_zero_point_delta_has_no_sign() -> None:
    """A large segment whose point Delta is exactly 0 has no sign, so it
    makes no flip against a negative or a positive one."""
    for control, value, sign in ((12.0, 10.0, -1.0), (10.0, 12.0, 1.0)):
        lines = [
            _line(
                index=index,
                lead=lead,
                quantity=quantity,
                segment="A" if index < 20 else "B",
                control=control if index < 20 else 10.0,
                truth=10.0,
                value=value if index < 20 else 10.0,
            )
            for index in range(40)
            for lead in daily_shadow.SCORED_LEADS
            for quantity in daily_shadow.TEMPERATURE_QUANTITIES
        ]
        report, flips = daily_shadow.segment_report(
            daily_shadow.split_segments(lines), ["X"]
        )
        assert [entry["large"] for entry in report] == [True, True]
        assert _point(report[0]) * sign > 0
        assert _point(report[1]) == 0.0
        assert flips == {"X": False}


def test_the_flip_turns_a_pass_into_inconclusive() -> None:
    """A tile that would otherwise PASS becomes INCONCLUSIVE under the
    flip, and stays PASS when the rule does not apply."""
    _, flips = daily_shadow.segment_report(
        daily_shadow.split_segments(_two_segments()), ["X"]
    )
    _, no_flips = daily_shadow.segment_report(
        daily_shadow.split_segments(_two_segments(drop=(25, 3))), ["X"]
    )
    passing = _tile(point=-1.0, upper=-0.5)

    flipped = _outcome(passing, flip=flips["X"])
    assert flipped.outcome == "INCONCLUSIVE"
    assert flipped.reasons == ("segment_sign_flip",)

    unflipped = _outcome(passing, flip=no_flips["X"])
    assert unflipped.outcome == "PASS"
    assert unflipped.reasons == ()


def test_the_flip_never_changes_a_result_that_is_not_a_pass() -> None:
    """The flip only ever demotes a PASS -- a FAIL or INCONCLUSIVE outcome
    keeps its own reasons untouched."""
    failing = _outcome(_tile(point=0.5, upper=0.9), flip=True)
    assert failing.outcome == "FAIL"
    assert failing.reasons == ("point_delta_not_negative",)

    inconclusive = _outcome(_tile(point=-1.0, upper=0.5), flip=True)
    assert inconclusive.outcome == "INCONCLUSIVE"
    assert inconclusive.reasons == ("upper_bound_not_negative",)
