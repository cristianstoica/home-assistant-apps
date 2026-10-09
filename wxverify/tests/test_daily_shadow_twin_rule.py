"""§15.3 Twin rule (§8.5; §13.3, "A candidate's outcome is one function").

Direct calls of ``candidate_outcome``, each made once with the name
"IW-BC" and once with "TD-K-BC"; the expected outcome and reasons are the
same for both names. Also covers H1 (``twin_missing`` is required
keyword-only) and the architect's precedence/sign-flip check.
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence

import pytest

from wxverify.verification.daily_shadow import (
    Bounds,
    CandidateOutcome,
    candidate_outcome,
)

CANDIDATE_NAMES: tuple[str, str] = ("IW-BC", "TD-K-BC")

_PASSING_GUARD: dict[str, object] = {"pass": True}
_PASSING_TEMP_GUARD: dict[str, object] = {
    "temperature_high": {"pass": True},
    "temperature_low": {"pass": True},
}
BASE_GUARDS: dict[str, object] = {
    "G2": _PASSING_TEMP_GUARD,
    "G4": _PASSING_TEMP_GUARD,
    "G3": _PASSING_GUARD,
    "frost": _PASSING_GUARD,
}
BASE_CHECKS: dict[str, object] = {"f1_pass": True, "minimums_met": True}
BASE_TILE = Bounds(
    point=-0.5, lower=-0.9, upper=-0.1, kept=1000, discarded=0, level=0.975
)
TWIN_OF_I = Bounds(
    point=-0.2, lower=-0.4, upper=-0.05, kept=1000, discarded=0, level=0.975
)


def _call(
    name: str,
    *,
    tile: Bounds = BASE_TILE,
    missing: int = 0,
    guards: Mapping[str, object] = BASE_GUARDS,
    twin: Bounds | None = TWIN_OF_I,
    twin_missing: int = 0,
    checks: Mapping[str, object] = BASE_CHECKS,
    flip: bool = False,
    station_set_changed: Mapping[str, Sequence[str]] | None = None,
    db_import_changed: bool | None = False,
) -> CandidateOutcome:
    return candidate_outcome(
        name,
        tile=tile,
        missing=missing,
        guards=guards,
        twin=twin,
        twin_missing=twin_missing,
        checks=checks,
        flip=flip,
        station_set_changed=station_set_changed,
        db_import_changed=db_import_changed,
    )


@pytest.mark.parametrize("name", CANDIDATE_NAMES)
def test_twin_rule_i_complete_comparison_keeps_its_outcome(name: str) -> None:
    """(i) A complete comparison keeps its outcome."""
    result = _call(name, twin=TWIN_OF_I)
    assert result.outcome == "PASS"
    assert result.reasons == ()


@pytest.mark.parametrize("name", CANDIDATE_NAMES)
def test_twin_rule_i_twin_upper_not_negative_does_not_beat_twin(name: str) -> None:
    """(i) the same with the twin's upper 0.2: the candidate does not beat its twin."""
    twin = Bounds(
        point=-0.2, lower=-0.4, upper=0.2, kept=1000, discarded=0, level=0.975
    )
    result = _call(name, twin=twin)
    assert result.outcome == "INCONCLUSIVE"
    assert result.reasons == ("does_not_beat_its_twin",)


@pytest.mark.parametrize("name", CANDIDATE_NAMES)
def test_twin_rule_ii_missing_value_at_scored_case(name: str) -> None:
    """(ii) IW-BC/TD-K-BC has no value at some scored case (missing=3)."""
    result = _call(name, missing=3, twin=TWIN_OF_I)
    assert result.outcome == "NOT_ASSESSED"
    assert result.reasons == ("cases_without_value:3",)


@pytest.mark.parametrize("name", CANDIDATE_NAMES)
def test_twin_rule_iii_twin_values_missing(name: str) -> None:
    """(iii) the twin's values are missing at some scored case (twin_missing=2)."""
    result = _call(name, missing=0, twin_missing=2, twin=TWIN_OF_I)
    assert result.outcome == "NOT_ASSESSED"
    assert result.reasons == ("twin_values_missing:2",)


@pytest.mark.parametrize("name", CANDIDATE_NAMES)
def test_twin_rule_iii_twin_values_missing_beats_point_delta_not_negative(
    name: str,
) -> None:
    """(iii) the same with a tile whose point/upper are not negative.

    The missing-values reason is reported before the comparison with the
    control is judged: still NOT_ASSESSED with ``twin_values_missing:2``,
    never FAIL with ``point_delta_not_negative``.
    """
    tile = Bounds(point=0.3, lower=0.1, upper=0.5, kept=1000, discarded=0, level=0.975)
    result = _call(name, tile=tile, missing=0, twin_missing=2, twin=TWIN_OF_I)
    assert result.outcome == "NOT_ASSESSED"
    assert result.reasons == ("twin_values_missing:2",)


@pytest.mark.parametrize("name", CANDIDATE_NAMES)
def test_twin_rule_iv_both_sides_missing(name: str) -> None:
    """(iv) both sides missing: only the candidate's own count is reported."""
    result = _call(name, missing=3, twin_missing=5, twin=TWIN_OF_I)
    assert result.outcome == "NOT_ASSESSED"
    assert result.reasons == ("cases_without_value:3",)


@pytest.mark.parametrize("name", CANDIDATE_NAMES)
def test_twin_rule_v_head_to_head_unavailable_no_bounds(name: str) -> None:
    """(v) the head-to-head is unavailable: twin's point and U are both None."""
    twin = Bounds(
        point=-0.2, lower=None, upper=None, kept=0, discarded=1000, level=0.975
    )
    result = _call(name, twin=twin)
    assert result.outcome == "NOT_ASSESSED"
    assert result.reasons == ("twin_comparison_unavailable",)


@pytest.mark.parametrize("name", CANDIDATE_NAMES)
def test_twin_rule_v_head_to_head_unavailable_missing_point_only(name: str) -> None:
    """(v) a missing point is caught even with U (upper) present."""
    twin = Bounds(
        point=None, lower=-0.4, upper=-0.05, kept=1000, discarded=0, level=0.975
    )
    result = _call(name, twin=twin)
    assert result.outcome == "NOT_ASSESSED"
    assert result.reasons == ("twin_comparison_unavailable",)


@pytest.mark.parametrize("name", CANDIDATE_NAMES)
def test_twin_rule_v_twin_looked_at_only_once_control_comparison_would_pass(
    name: str,
) -> None:
    """(v) with the control comparison itself inconclusive, the twin is unread."""
    tile = Bounds(
        point=-0.5, lower=-0.9, upper=0.1, kept=1000, discarded=0, level=0.975
    )
    twin = Bounds(
        point=-0.2, lower=None, upper=None, kept=0, discarded=1000, level=0.975
    )
    result = _call(name, tile=tile, twin=twin)
    assert result.outcome == "INCONCLUSIVE"
    assert result.reasons == ("upper_bound_not_negative",)


@pytest.mark.parametrize("name", CANDIDATE_NAMES)
def test_twin_rule_vi_failed_data_check_comes_first(name: str) -> None:
    """(vi) a failed data check comes first, even with the twin also missing."""
    checks = {"f1_pass": False, "minimums_met": True}
    result = _call(name, checks=checks, twin_missing=2, twin=TWIN_OF_I)
    assert result.outcome == "INSUFFICIENT"
    assert result.reasons == ("f1_below_95",)


@pytest.mark.parametrize("name", CANDIDATE_NAMES)
def test_twin_rule_precedence_changed_header_overrides_twin_missing(name: str) -> None:
    """The architect's extra check: the changed-header rule applies last.

    ``twin_missing=2`` alone gives NOT_ASSESSED, ``twin_values_missing:2``;
    with ``db_import_changed=True`` the changed-header rule overrides that
    outcome to INCONCLUSIVE and the override itself is also recorded as a
    reason, appended after the twin-missing reason.
    """
    result = _call(name, twin_missing=2, twin=TWIN_OF_I, db_import_changed=True)
    assert result.outcome == "INCONCLUSIVE"
    assert result.reasons == ("twin_values_missing:2", "station_set_or_import_changed")


def test_h1_twin_missing_is_required_keyword_only() -> None:
    """H1: ``twin_missing`` is a required keyword-only parameter.

    A positional-friendly signature would let a caller pass it by position
    (or omit it with a default), silently decoupling it from the named
    ``missing=``/``twin=`` arguments it must always travel with.
    """
    signature = inspect.signature(candidate_outcome)
    parameter = signature.parameters["twin_missing"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is inspect.Parameter.empty
