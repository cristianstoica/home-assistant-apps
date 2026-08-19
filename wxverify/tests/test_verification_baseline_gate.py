"""§8 (W5) oracles: the baseline gate validates a REQUIRED SET, and a lead
lacking a required baseline is dropped from the adequacy set in lockstep.

Two defects are pinned here. (1) The gate used to run only for quantities
that were already improved, so an unimproved quantity could fail its
baseline and still ride along in a recommendation. (2) The gate validated
the baselines it happened to be handed rather than the ones the
specification requires, so one present, passing baseline let a missing one
through. The resolution for a per-lead shortfall is to drop that lead, not
to fail the whole variable — the four-survive / three-survive pair below is
what pins that, and both halves must fail against the pre-W5 code.

All fixture data is synthetic (invented dates, fake entity keys, UTC).
"""

from __future__ import annotations

from typing import cast

from wxverify.verification.decision import (
    CandidateSeries,
    ContinuousLead,
    OccurrenceLead,
    TempLead,
    VariableInputs,
    Verdict,
    decide_variable,
)

_DATES = [f"2026-07-{d:02d}" for d in range(1, 25)]  # 24 days > the 20 floor
_FIVE_LEADS = (1, 2, 3, 4, 5)


def _flat(cand: float, opp: float, days: int = 24) -> ContinuousLead:
    return {d: (cand, opp) for d in _DATES[:days]}


def _record(verdict: Verdict, key: str) -> dict[str, object]:
    candidates = cast(dict[str, object], verdict.detail["candidates"])
    return cast(dict[str, object], candidates[key])


def _dropped(verdict: Verdict, key: str) -> list[dict[str, object]]:
    headline = cast(dict[str, object], _record(verdict, key)["headline"])
    return cast(list[dict[str, object]], headline["dropped_leads"])


def _adequate(verdict: Verdict, key: str) -> list[int]:
    headline = cast(dict[str, object], _record(verdict, key)["headline"])
    return cast(list[int], headline["adequate_leads"])


# ---------------------------------------------------------------------------
# The lead-drop boundary pair — ONE fixture, only the baseline removals
# differ, so the pair brackets MIN_ADEQUATE_LEADS_PER_VARIABLE (4).
# ---------------------------------------------------------------------------


def _five_lead_wind(*, all_feed_mean_leads: tuple[int, ...]) -> CandidateSeries:
    """Adequate at five leads by data; all_feed_mean present only where named."""
    return CandidateSeries(
        key="3",
        continuous={"wind_max": {ld: _flat(1.0, 2.0) for ld in _FIVE_LEADS}},
        baseline_continuous={
            "baseline_persistence": {
                "wind_max": {ld: _flat(1.0, 3.0) for ld in _FIVE_LEADS}
            },
            "baseline_all_feed_mean": {
                "wind_max": {ld: _flat(1.0, 3.0) for ld in all_feed_mean_leads}
            },
        },
    )


def _wind_verdict(candidate: CandidateSeries) -> Verdict:
    inputs = VariableInputs(variable="wind", incumbent_key="2", candidates=(candidate,))
    return decide_variable(inputs, seed=20260814, resamples=120)


def test_lead_drop_boundary_four_surviving_leads_still_recommend() -> None:
    verdict = _wind_verdict(_five_lead_wind(all_feed_mean_leads=(1, 2, 3, 4)))
    assert verdict.outcome == "recommend"
    assert verdict.recommended_key == "3"
    # Exactly the four baseline-checked leads — not the five the data
    # supports. A drop applied AFTER the sufficiency test would leave 5 here.
    assert _adequate(verdict, "3") == [1, 2, 3, 4]
    assert _dropped(verdict, "3") == [
        {
            "lead": 5,
            "reason": "baseline_absent",
            "missing_baselines": ["baseline_all_feed_mean"],
        },
        {"lead": 6, "reason": "thin_data", "days": 0},
        {"lead": 7, "reason": "thin_data", "days": 0},
    ]


def test_lead_drop_boundary_three_surviving_leads_are_insufficient() -> None:
    verdict = _wind_verdict(_five_lead_wind(all_feed_mean_leads=(1, 2, 3)))
    # The existing insufficiency token — W5 adds no verdict vocabulary.
    assert verdict.outcome == "insufficient_evidence"
    assert verdict.recommended_key is None
    assert _adequate(verdict, "3") == [1, 2, 3]
    assert [d["lead"] for d in _dropped(verdict, "3")] == [4, 5, 6, 7]


def test_thin_data_and_absent_baseline_drops_are_distinguishable() -> None:
    # Lead 5 is thin (10 days); lead 4 is fully sampled but lacks
    # all_feed_mean. The two causes must not collapse into one report.
    candidate = CandidateSeries(
        key="3",
        continuous={
            "wind_max": {
                **{ld: _flat(1.0, 2.0) for ld in (1, 2, 3, 4)},
                5: _flat(1.0, 2.0, days=10),
            }
        },
        baseline_continuous={
            "baseline_persistence": {
                "wind_max": {ld: _flat(1.0, 3.0) for ld in _FIVE_LEADS}
            },
            "baseline_all_feed_mean": {
                "wind_max": {ld: _flat(1.0, 3.0) for ld in (1, 2, 3)}
            },
        },
    )
    dropped = _dropped(_wind_verdict(candidate), "3")
    reasons = {int(cast(int, d["lead"])): d["reason"] for d in dropped}
    assert reasons == {
        4: "baseline_absent",
        5: "thin_data",
        6: "thin_data",
        7: "thin_data",
    }
    by_lead = {int(cast(int, d["lead"])): d for d in dropped}
    assert by_lead[4]["missing_baselines"] == ["baseline_all_feed_mean"]
    assert by_lead[5]["days"] == 10


# ---------------------------------------------------------------------------
# Missing-one-baseline oracles, one per gate branch
# ---------------------------------------------------------------------------


def test_continuous_branch_requires_all_feed_mean() -> None:
    candidate = CandidateSeries(
        key="3",
        continuous={"wind_max": {ld: _flat(1.0, 2.0) for ld in range(1, 8)}},
        baseline_continuous={
            "baseline_persistence": {
                "wind_max": {ld: _flat(1.0, 3.0) for ld in range(1, 8)}
            }
        },
    )
    verdict = _wind_verdict(candidate)
    assert verdict.recommended_key is None
    assert verdict.outcome == "insufficient_evidence"
    assert all(
        d["missing_baselines"] == ["baseline_all_feed_mean"]
        for d in _dropped(verdict, "3")
    )


def _temp_lead(days: int = 24) -> TempLead:
    return {d: ((1.0, 2.0), (1.0, 2.0)) for d in _DATES[:days]}


def _temp_baseline_lead() -> TempLead:
    return {d: ((1.0, 3.0), (1.0, 3.0)) for d in _DATES}


def test_temperature_branch_requires_persistence() -> None:
    beaten = {ld: _flat(1.0, 3.0) for ld in range(1, 8)}
    candidate = CandidateSeries(
        key="3",
        temp={ld: _temp_lead() for ld in range(1, 8)},
        continuous={
            "temperature_high": {ld: _flat(1.0, 2.0) for ld in range(1, 8)},
            "temperature_low": {ld: _flat(1.0, 2.0) for ld in range(1, 8)},
        },
        baseline_continuous={
            "baseline_all_feed_mean": {
                "temperature_high": beaten,
                "temperature_low": beaten,
            }
        },
    )
    inputs = VariableInputs(
        variable="temperature", incumbent_key="2", candidates=(candidate,)
    )
    verdict = decide_variable(inputs, seed=20260814, resamples=120)
    assert verdict.recommended_key is None
    assert verdict.outcome == "insufficient_evidence"
    assert all(
        d["missing_baselines"] == ["baseline_persistence"]
        for d in _dropped(verdict, "3")
    )


def _occ_series() -> tuple[OccurrenceLead, OccurrenceLead]:
    """24 days, 10 wet (8 hit + 2 miss) / 14 dry (1 false alarm).

    The incumbent misses every wet day and false-alarms on six dry days;
    the baseline opponent is worse still (misses every wet day and
    false-alarms every dry day), so a present baseline is decisively beaten.
    """
    vs_incumbent: OccurrenceLead = {}
    vs_baseline: OccurrenceLead = {}
    for i, d in enumerate(_DATES):
        if i < 10:
            cand = "hit" if i < 8 else "miss"
            opp, base = "miss", "miss"
        else:
            cand = "false_alarm" if i == 10 else "correct_negative"
            opp = "false_alarm" if i < 17 else "correct_negative"
            base = "false_alarm"
        vs_incumbent[d] = (cand, opp)
        vs_baseline[d] = (cand, base)
    return vs_incumbent, vs_baseline


def _precip_candidate(
    *,
    occurrence_baselines: tuple[str, ...],
    total_baseline: ContinuousLead,
) -> CandidateSeries:
    vs_incumbent, vs_baseline = _occ_series()
    return CandidateSeries(
        key="3",
        continuous={"precip_total": {ld: _flat(2.0, 2.0) for ld in range(1, 8)}},
        occurrence={ld: dict(vs_incumbent) for ld in range(1, 8)},
        baseline_continuous={
            name: {"precip_total": {ld: dict(total_baseline) for ld in range(1, 8)}}
            for name in ("baseline_persistence", "baseline_all_feed_mean")
        },
        baseline_occurrence={
            name: {ld: dict(vs_baseline) for ld in range(1, 8)}
            for name in occurrence_baselines
        },
    )


def _precip_verdict(candidate: CandidateSeries) -> Verdict:
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(candidate,)
    )
    return decide_variable(inputs, seed=20260814, resamples=200)


_ALL_OCCURRENCE_BASELINES = (
    "baseline_persistence",
    "baseline_all_feed_mean",
    "baseline_always_dry",
)
#: Beaten by the flat (2.0, 2.0) candidate total: relative improvement 0.5.
_BEATEN_TOTAL = _flat(2.0, 4.0)
#: Beats the candidate total, so the total gate fails without dropping leads.
_BEATING_TOTAL = _flat(2.0, 1.0)


def test_occurrence_branch_requires_always_dry() -> None:
    candidate = _precip_candidate(
        occurrence_baselines=("baseline_persistence", "baseline_all_feed_mean"),
        total_baseline=_BEATEN_TOTAL,
    )
    verdict = _precip_verdict(candidate)
    assert verdict.recommended_key is None
    record = _record(verdict, "3")
    occurrence = cast(dict[str, object], record["occurrence"])
    dropped = cast(list[dict[str, object]], occurrence["dropped_leads"])
    assert dropped
    assert all(d["missing_baselines"] == ["baseline_always_dry"] for d in dropped)


# ---------------------------------------------------------------------------
# The gate runs for a quantity that is NOT in `improved` — and is a gate,
# not a blanket rejection: the mirror case is unchanged.
# ---------------------------------------------------------------------------


def test_unimproved_quantity_failing_its_baseline_blocks_the_recommendation() -> None:
    verdict = _precip_verdict(
        _precip_candidate(
            occurrence_baselines=_ALL_OCCURRENCE_BASELINES,
            total_baseline=_BEATING_TOTAL,
        )
    )
    conditions = cast(dict[str, object], _record(verdict, "3")["conditions"])
    # The total endpoint is flat (effect 0): permissive, never improved.
    assert conditions["improved_endpoints"] == ["occurrence"]
    assert conditions["beats_baselines"] is False
    assert verdict.recommended_key is None
    assert verdict.outcome != "recommend"
    # The gate ran and is reported even though the quantity was unimproved —
    # an absent detail block reads as "passed" on the surface.
    baselines = cast(dict[str, object], _record(verdict, "3")["baselines"])
    total = cast(dict[str, object], baselines["total"])
    for name in ("baseline_persistence", "baseline_all_feed_mean"):
        entry = cast(dict[str, object], total[name])
        assert entry["passed"] is False
        assert entry["insufficient"] is False


def test_unimproved_quantity_passing_its_baseline_leaves_the_outcome_unchanged() -> (
    None
):
    verdict = _precip_verdict(
        _precip_candidate(
            occurrence_baselines=_ALL_OCCURRENCE_BASELINES,
            total_baseline=_BEATEN_TOTAL,
        )
    )
    conditions = cast(dict[str, object], _record(verdict, "3")["conditions"])
    assert conditions["improved_endpoints"] == ["occurrence"]
    assert conditions["beats_baselines"] is True
    assert verdict.outcome == "recommend"
    assert verdict.recommended_key == "3"
