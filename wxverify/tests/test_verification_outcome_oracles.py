"""QA oracles for §18.2 (known statistical outcomes) and §18.13 (D0
robustness), plus the §8/§11 aggregate common-core resolution.

Construction discipline: candidate series use a CONSTANT candidate/opponent
ratio per lead, so the per-lead relative improvement is invariant under any
bootstrap re-weighting — the pooled effect and its CI degenerate to a
hand-computable point, making every gate boundary exact. Where a genuinely
noisy CI is needed (zero-straddle, tie-break overlap) the ratio alternates
by date parity with hand-sized amplitude so the CI's relation to the
threshold is robust at the chosen resample count.

All expected values are hand-derived on paper from the spec; fixture data
is synthetic (invented dates, fake feed ids, UTC).
"""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import date, timedelta
from typing import cast

import pytest

from tests.helpers import asof_conn, occurrence_baseline_set
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.decision import (
    CandidateSeries,
    ContinuousLead,
    OccurrenceLead,
    TempLead,
    VariableInputs,
    Verdict,
    _baseline_endpoint,  # noqa: SLF001
    _evaluate_endpoint,  # noqa: SLF001
    decide_variable,
)
from wxverify.verification.engine import aggregate_run
from wxverify.verification.methodology import BASELINE_GATE_CI_LEVEL
from wxverify.verification.runs import RosterFeed, RunConfig

# 24 target dates — enough for the 20-day adequacy floor with margin.
_DATES = [f"2026-07-{d:02d}" for d in range(1, 25)]


def _dates_n(n: int) -> list[str]:
    start = date(2026, 7, 1)
    return [(start + timedelta(days=i)).isoformat() for i in range(n)]


def _ratio_series(
    dates: list[str], ratio: float, *, base: float = 2.0, step: float = 0.1
) -> ContinuousLead:
    """Divergent opponent values with a constant candidate/opponent ratio."""
    out: ContinuousLead = {}
    for i, d in enumerate(dates):
        opp = base + step * i
        out[d] = (ratio * opp, opp)
    return out


def _strong_baseline(series: ContinuousLead) -> ContinuousLead:
    """Baseline pairs (cand, 2*cand): exactly 0.5 improvement, any weights."""
    return {d: (c, 2.0 * c) for d, (c, _o) in series.items()}


def _wind_candidate(key: str, leads: dict[int, ContinuousLead]) -> CandidateSeries:
    base = {ld: _strong_baseline(s) for ld, s in leads.items()}
    return CandidateSeries(
        key=key,
        continuous={"wind_max": leads},
        baseline_continuous={
            "baseline_persistence": {"wind_max": base},
            "baseline_all_feed_mean": {"wind_max": base},
        },
    )


def _record(verdict: Verdict, key: str) -> dict[str, object]:
    candidates = cast(dict[str, object], verdict.detail["candidates"])
    return cast(dict[str, object], candidates[key])


def _headline(verdict: Verdict, key: str) -> dict[str, object]:
    return cast(dict[str, object], _record(verdict, key)["headline"])


def _conditions(verdict: Verdict, key: str) -> dict[str, object]:
    return cast(dict[str, object], _record(verdict, key)["conditions"])


# ---------------------------------------------------------------------------
# O1 — two clear passers with non-overlapping CIs: greatest pooled wins
# ---------------------------------------------------------------------------


def test_two_passers_non_overlapping_cis_pick_greatest_pooled() -> None:
    cand4 = _wind_candidate("4", {ld: _ratio_series(_DATES, 0.5) for ld in range(1, 8)})
    cand3 = _wind_candidate("3", {ld: _ratio_series(_DATES, 0.9) for ld in range(1, 8)})
    inputs = VariableInputs(
        variable="wind", incumbent_key="2", candidates=(cand3, cand4)
    )
    verdict = decide_variable(inputs, seed=20260701, resamples=60)
    assert verdict.outcome == "recommend"
    assert verdict.recommended_key == "4"
    assert verdict.detail["tie_break"] == {"best_by_pooled": "4", "chosen": "4"}
    assert "statistically_unresolved" not in verdict.detail
    # Constant ratios: pooled effects are exactly 1 - ratio (up to fp noise).
    assert _headline(verdict, "4")["pooled_point"] == pytest.approx(0.5)
    assert _headline(verdict, "3")["pooled_point"] == pytest.approx(0.1)
    for key in ("3", "4"):
        assert _conditions(verdict, key) == {
            "ci_excludes_zero": True,
            "lead_stability": True,
            "practical_floor": True,
            "beats_baselines": True,
            "components_non_inferior": True,
        }


# ---------------------------------------------------------------------------
# O2 — equal lead weighting AFTER per-lead effects; 20-day boundary is
# inclusive (§12: a lead with exactly 20 strict-common days is adequate)
# ---------------------------------------------------------------------------


def test_pooled_effect_weights_leads_equally_not_by_day_count() -> None:
    forty = _dates_n(40)
    leads: dict[int, ContinuousLead] = {1: _ratio_series(forty, 0.9)}
    for ld in range(2, 8):
        leads[ld] = _ratio_series(forty[:20], 0.5)  # exactly 20 days each
    inputs = VariableInputs(
        variable="wind",
        incumbent_key="2",
        candidates=(_wind_candidate("3", leads),),
    )
    verdict = decide_variable(inputs, seed=20260702, resamples=200)
    assert verdict.outcome == "recommend"
    headline = _headline(verdict, "3")
    assert headline["adequate_leads"] == list(range(1, 8))
    per_lead = cast(dict[str, object], headline["per_lead"])
    assert per_lead["1"] == pytest.approx(0.1)
    assert per_lead["2"] == pytest.approx(0.5)
    # Equal lead weighting: (0.1 + 6*0.5)/7 = 31/70. Day-weighted pooling
    # over (40 + 6*20) = 160 day-pairs would give (40*0.1 + 120*0.5)/160
    # = 0.4 — assert we are far from it.
    pooled = cast(float, headline["pooled_point"])
    assert pooled == pytest.approx(31 / 70)
    assert abs(pooled - 0.4) > 0.02


# ---------------------------------------------------------------------------
# O3 — adequacy floors: >= 20 days per lead, >= 4 adequate leads
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("lead_ids", "days", "expected"),
    [
        # Exactly 4 leads at exactly 20 days: both floors inclusive -> pass.
        ((1, 2, 3, 4), 20, "recommend"),
        # 3 adequate leads < 4 -> insufficient.
        ((1, 2, 3), 24, "insufficient_evidence"),
        # 19 days per lead < 20 -> no adequate lead -> insufficient.
        ((1, 2, 3, 4, 5, 6, 7), 19, "insufficient_evidence"),
    ],
)
def test_adequacy_floor_boundaries(
    lead_ids: tuple[int, ...], days: int, expected: str
) -> None:
    dates = _dates_n(days)
    leads = {ld: _ratio_series(dates, 0.5) for ld in lead_ids}
    inputs = VariableInputs(
        variable="wind",
        incumbent_key="2",
        candidates=(_wind_candidate("3", leads),),
    )
    verdict = decide_variable(inputs, seed=20260703, resamples=60)
    assert verdict.outcome == expected


# ---------------------------------------------------------------------------
# O4 — lead stability: 4 of 7 beneficial leads misses ceil(2/3 * 7) = 5
# ---------------------------------------------------------------------------


def test_mixed_by_lead_when_agreement_below_two_thirds_ceiling() -> None:
    leads: dict[int, ContinuousLead] = {}
    for ld in range(1, 5):
        leads[ld] = _ratio_series(_DATES, 0.5)  # improvement +0.5
    for ld in range(5, 8):
        leads[ld] = _ratio_series(_DATES, 1.2)  # improvement -0.2
    inputs = VariableInputs(
        variable="wind",
        incumbent_key="2",
        candidates=(_wind_candidate("3", leads),),
    )
    verdict = decide_variable(inputs, seed=20260704, resamples=60)
    assert verdict.outcome == "mixed_by_lead"
    # pooled = (4*0.5 + 3*(-0.2))/7 = 1.4/7 = 0.2 exactly on every resample.
    assert _headline(verdict, "3")["pooled_point"] == pytest.approx(0.2)
    assert _conditions(verdict, "3") == {
        "ci_excludes_zero": True,
        "lead_stability": False,
        "practical_floor": True,
        "beats_baselines": True,
        "components_non_inferior": True,
    }


# ---------------------------------------------------------------------------
# O5 — practical floor is inclusive at exactly 0.05 relative MAE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("cand_mae", "expected", "floor_met"),
    [
        # (20-19)/20 = 1/20 -> the float 0.05 EXACTLY (same rounding as the
        # literal); the >= floor comparison must pass at equality.
        (19.0, "recommend", True),
        # (20-19.2)/20 = 0.04 < 0.05 -> only the floor gate fails.
        (19.2, "retain_incumbent", False),
    ],
)
def test_practical_floor_boundary_exact(
    cand_mae: float, expected: str, floor_met: bool
) -> None:
    # Constant values on purpose: weighted MAEs stay exactly 19/20 under
    # any resample weights, so improvement is bit-exactly 1/20.
    series: ContinuousLead = {d: (cand_mae, 20.0) for d in _DATES}
    inputs = VariableInputs(
        variable="wind",
        incumbent_key="2",
        candidates=(_wind_candidate("3", {ld: dict(series) for ld in range(1, 8)}),),
    )
    verdict = decide_variable(inputs, seed=20260705, resamples=60)
    assert verdict.outcome == expected
    assert _conditions(verdict, "3")["practical_floor"] is floor_met


# ---------------------------------------------------------------------------
# O6 — baseline gate: EVERY baseline must be beaten (tie with persistence
# fails the conjunction even though all_feed_mean is beaten)
# ---------------------------------------------------------------------------


def test_baseline_gate_requires_beating_every_baseline() -> None:
    series = _ratio_series(_DATES, 0.5)
    tie = {d: (c, c) for d, (c, _o) in series.items()}  # CI (0, 0): no beat
    candidate = CandidateSeries(
        key="3",
        continuous={"wind_max": {ld: dict(series) for ld in range(1, 8)}},
        baseline_continuous={
            "baseline_persistence": {"wind_max": {ld: dict(tie) for ld in range(1, 8)}},
            "baseline_all_feed_mean": {
                "wind_max": {ld: _strong_baseline(series) for ld in range(1, 8)}
            },
        },
    )
    inputs = VariableInputs(variable="wind", incumbent_key="2", candidates=(candidate,))
    verdict = decide_variable(inputs, seed=20260706, resamples=60)
    assert verdict.outcome == "retain_incumbent"
    conditions = _conditions(verdict, "3")
    assert conditions["beats_baselines"] is False
    assert conditions["ci_excludes_zero"] is True
    assert conditions["practical_floor"] is True
    baselines = cast(dict[str, object], _record(verdict, "3")["baselines"])
    persistence = cast(dict[str, object], baselines["baseline_persistence"])
    all_feed_mean = cast(dict[str, object], baselines["baseline_all_feed_mean"])
    assert persistence["passed"] is False
    assert all_feed_mean["passed"] is True


# ---------------------------------------------------------------------------
# O7 — CI straddling zero: positive point, noisy series, c1 alone fails
# ---------------------------------------------------------------------------


def test_ci_straddling_zero_blocks_recommendation() -> None:
    # Constant opponent 2.0; candidate ratio alternates 0.4 / 1.4 by date
    # parity: point = 1 - mean(0.4, 1.4) = 0.1, but per-date effects swing
    # +-0.5 so the 98.33% CI comfortably straddles zero.
    noisy: ContinuousLead = {}
    for i, d in enumerate(_DATES):
        ratio = 0.4 if i % 2 == 0 else 1.4
        noisy[d] = (ratio * 2.0, 2.0)
    inputs = VariableInputs(
        variable="wind",
        incumbent_key="2",
        candidates=(_wind_candidate("3", {ld: dict(noisy) for ld in range(1, 8)}),),
    )
    verdict = decide_variable(inputs, seed=20260707, resamples=400)
    assert verdict.outcome == "retain_incumbent"
    headline = _headline(verdict, "3")
    assert headline["pooled_point"] == pytest.approx(0.1)
    ci = cast(list[float], headline["ci"])
    assert ci[0] < 0.0 < ci[1]
    conditions = _conditions(verdict, "3")
    assert conditions == {
        "ci_excludes_zero": False,
        "lead_stability": True,
        "practical_floor": True,
        "beats_baselines": True,
        "components_non_inferior": True,
    }


# ---------------------------------------------------------------------------
# O8 — temperature condition 5: a component degrading beyond the 2% margin
# vetoes an otherwise-passing headline; within the margin it does not
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("low_ratio", "expected", "degraded"),
    [
        # low MAE ratio 33/32: component effect exactly -0.03125 < -0.02.
        (1.03125, "retain_incumbent", True),
        # low MAE ratio 65/64: component effect exactly -0.015625 >= -0.02.
        (1.015625, "recommend", False),
    ],
)
def test_temperature_component_non_inferiority_margin(
    low_ratio: float, expected: str, degraded: bool
) -> None:
    high: ContinuousLead = {}
    low: ContinuousLead = {}
    joint: TempLead = {}
    for i, d in enumerate(_DATES):
        oh = 4.0 + 0.2 * i
        ol = 2.0 + 0.1 * i
        high[d] = (0.5 * oh, oh)
        low[d] = (low_ratio * ol, ol)
        joint[d] = (high[d], low[d])
    # Condition 4 is evaluated on the COMPOSITE (mean of high and low MAE)
    # over the high∩low intersection, so BOTH components must be supplied:
    # (c, 2c) on each side makes the composite improvement exactly 0.5 under
    # any resample weights, leaving condition 5 as the sole discriminator.
    base_high = _strong_baseline(high)
    base_low = _strong_baseline(low)
    candidate = CandidateSeries(
        key="3",
        continuous={
            "temperature_high": {ld: dict(high) for ld in range(1, 8)},
            "temperature_low": {ld: dict(low) for ld in range(1, 8)},
        },
        temp={ld: dict(joint) for ld in range(1, 8)},
        baseline_continuous={
            "baseline_persistence": {
                "temperature_high": {ld: dict(base_high) for ld in range(1, 8)},
                "temperature_low": {ld: dict(base_low) for ld in range(1, 8)},
            },
            "baseline_all_feed_mean": {
                "temperature_high": {ld: dict(base_high) for ld in range(1, 8)},
                "temperature_low": {ld: dict(base_low) for ld in range(1, 8)},
            },
        },
    )
    inputs = VariableInputs(
        variable="temperature", incumbent_key="2", candidates=(candidate,)
    )
    verdict = decide_variable(inputs, seed=20260708, resamples=60)
    assert verdict.outcome == expected
    components = cast(dict[str, object], _record(verdict, "3")["components"])
    low_comp = cast(dict[str, object], components["temperature_low"])
    assert low_comp["degraded"] is degraded
    assert low_comp["pooled_point"] == pytest.approx(1.0 - low_ratio)
    assert _conditions(verdict, "3")["components_non_inferior"] is (not degraded)
    # Condition 5 must be the ONLY discriminator between the two rows: if the
    # composite baseline gate ever fails here, the retain_incumbent row would
    # pass for the wrong reason and stop testing the margin at all.
    assert _conditions(verdict, "3")["beats_baselines"] is True


# ---------------------------------------------------------------------------
# O8b/O8c/O8d — temperature condition 4 is the COMPOSITE gate (§11/§12.5):
# each baseline is compared on mean(high MAE, low MAE) over the high∩low
# intersection, never on temperature_high alone. O8b is the negative (a
# baseline that loses on highs but WINS on lows must not be beaten), O8c its
# paired positive, O8d the §16.5 shape pin on the published family.
# ---------------------------------------------------------------------------


def _flat_lead(cand: float, opp: float, dates: list[str]) -> ContinuousLead:
    """A constant paired series: weighted MAEs are invariant under any
    resample weighting, so every effect below is an exact hand-computed
    point and each CI degenerates to that point."""
    return {d: (cand, opp) for d in dates}


def _temp_joint(high: ContinuousLead, low: ContinuousLead) -> TempLead:
    """The high∩low joint series the composite endpoint is built from."""
    return {d: (high[d], low[d]) for d in high.keys() & low.keys()}


def _temp_candidate(
    high: ContinuousLead,
    low: ContinuousLead,
    baselines: dict[str, dict[str, dict[int, ContinuousLead]]],
) -> CandidateSeries:
    return CandidateSeries(
        key="3",
        continuous={
            "temperature_high": {ld: dict(high) for ld in range(1, 8)},
            "temperature_low": {ld: dict(low) for ld in range(1, 8)},
        },
        temp={ld: _temp_joint(high, low) for ld in range(1, 8)},
        baseline_continuous=baselines,
    )


def _temp_verdict(candidate: CandidateSeries, *, seed: int) -> Verdict:
    inputs = VariableInputs(
        variable="temperature", incumbent_key="2", candidates=(candidate,)
    )
    return decide_variable(inputs, seed=seed, resamples=60)


# Candidate vs incumbent, shared by O8b/O8c: high and low MAE 2.0 against an
# incumbent at 4.0 -> composite improvement exactly 0.5 on every lead, so
# conditions 1, 2, 3 and 5 all pass and condition 4 is the sole variable.
_CAND_HIGH = _flat_lead(2.0, 4.0, _DATES)
_CAND_LOW = _flat_lead(2.0, 4.0, _DATES)
# A baseline the candidate crushes on both components (composite 1 - 2/8).
_WEAK_BASELINE = {
    "temperature_high": {ld: _flat_lead(2.0, 8.0, _DATES) for ld in range(1, 8)},
    "temperature_low": {ld: _flat_lead(2.0, 8.0, _DATES) for ld in range(1, 8)},
}


def test_temperature_baseline_gate_rejects_baseline_that_wins_on_lows() -> None:
    # Persistence loses on highs (2.0 vs 3.0) but WINS on lows (2.0 vs 1.0).
    # Composite means are 2.0 on both sides -> effect exactly 0.0 on every
    # resample, a degenerate [0, 0] CI that does not exclude zero. A
    # high-only gate would instead see 1 - 2/3 = 1/3 and wave it through.
    candidate = _temp_candidate(
        _CAND_HIGH,
        _CAND_LOW,
        {
            "baseline_persistence": {
                "temperature_high": {
                    ld: _flat_lead(2.0, 3.0, _DATES) for ld in range(1, 8)
                },
                "temperature_low": {
                    ld: _flat_lead(2.0, 1.0, _DATES) for ld in range(1, 8)
                },
            },
            "baseline_all_feed_mean": _WEAK_BASELINE,
        },
    )
    verdict = _temp_verdict(candidate, seed=20260713)
    assert verdict.outcome != "recommend"
    conditions = _conditions(verdict, "3")
    assert conditions["beats_baselines"] is False
    # Isolate the gate: nothing else about this candidate fails.
    assert conditions["ci_excludes_zero"] is True
    assert conditions["lead_stability"] is True
    assert conditions["practical_floor"] is True
    assert conditions["components_non_inferior"] is True
    baselines = cast(dict[str, object], _record(verdict, "3")["baselines"])
    persistence = cast(dict[str, object], baselines["baseline_persistence"])
    assert persistence["passed"] is False
    assert persistence["pooled_point"] == pytest.approx(0.0)
    all_feed_mean = cast(dict[str, object], baselines["baseline_all_feed_mean"])
    assert all_feed_mean["passed"] is True


def test_temperature_baseline_gate_passes_when_baseline_weak_on_both() -> None:
    # Same fixture shape as the negative, with persistence made weak on the
    # LOW component too (2.0 vs 3.0): composite opponent mean 3.0 against a
    # candidate mean of 2.0 -> effect exactly 1/3, the gate passes, and the
    # candidate reaches 'recommend'. Without this pair the negative above
    # could be green for any unrelated reason.
    candidate = _temp_candidate(
        _CAND_HIGH,
        _CAND_LOW,
        {
            "baseline_persistence": {
                "temperature_high": {
                    ld: _flat_lead(2.0, 3.0, _DATES) for ld in range(1, 8)
                },
                "temperature_low": {
                    ld: _flat_lead(2.0, 3.0, _DATES) for ld in range(1, 8)
                },
            },
            "baseline_all_feed_mean": _WEAK_BASELINE,
        },
    )
    verdict = _temp_verdict(candidate, seed=20260714)
    assert verdict.outcome == "recommend"
    assert verdict.recommended_key == "3"
    assert _conditions(verdict, "3")["beats_baselines"] is True
    baselines = cast(dict[str, object], _record(verdict, "3")["baselines"])
    persistence = cast(dict[str, object], baselines["baseline_persistence"])
    assert persistence["passed"] is True
    assert persistence["pooled_point"] == pytest.approx(1 / 3)


def test_temperature_baseline_family_describes_the_composite_sample() -> None:
    # §16.5: `baselines` is published as the verdict's COMPLETE tested
    # family, so every entry must describe the composite (high∩low) sample
    # the headline was decided on — not the wider high-only sample.
    #
    # Each baseline's HIGH series covers all 24 dates on all 7 leads; its LOW
    # series covers 20 dates on leads 1-5 and only 19 on leads 6-7. The
    # intersection is therefore 20 days on leads 1-5 (adequate: the §12 floor
    # is inclusive) and 19 on leads 6-7 (below the floor). A high-only family
    # would report 24 days on all 7 leads and adequate_leads [1..7]; the
    # composite family must report [1, 2, 3, 4, 5].
    low_dates = {ld: _DATES[:20] if ld <= 5 else _DATES[:19] for ld in range(1, 8)}
    baseline = {
        "temperature_high": {ld: _flat_lead(2.0, 4.0, _DATES) for ld in range(1, 8)},
        "temperature_low": {
            ld: _flat_lead(2.0, 4.0, low_dates[ld]) for ld in range(1, 8)
        },
    }
    candidate = _temp_candidate(
        _CAND_HIGH,
        _CAND_LOW,
        {
            "baseline_persistence": baseline,
            "baseline_all_feed_mean": {
                quantity: {ld: dict(series) for ld, series in by_lead.items()}
                for quantity, by_lead in baseline.items()
            },
        },
    )
    verdict = _temp_verdict(candidate, seed=20260715)
    record = _record(verdict, "3")
    baselines = cast(dict[str, object], record["baselines"])
    assert set(baselines) == {"baseline_persistence", "baseline_all_feed_mean"}
    for name in ("baseline_persistence", "baseline_all_feed_mean"):
        entry = cast(dict[str, object], baselines[name])
        assert entry["adequate_leads"] == [1, 2, 3, 4, 5], name
        assert set(cast(dict[str, object], entry["per_lead"])) == {
            "1",
            "2",
            "3",
            "4",
            "5",
        }, name
        assert entry["pooled_point"] == pytest.approx(0.5), name
    # §8/W5 lockstep: the headline adequacy set follows the baseline
    # shortfall, so leads 6-7 are dropped from the headline too — the
    # candidate is never scored on a lead it was not baseline-checked at.
    #
    # Kills the 0.11.0 implementation, in which headline adequacy was
    # computed from the candidate's own sample alone: leads 6 and 7 stayed
    # in `adequate_leads` ([1..7]) and carried no `dropped_leads` entry, so
    # the verdict pooled two leads that no baseline had ever been compared
    # on. This assertion is what the earlier version of this test agreed
    # with; `[1, 2, 3, 4, 5]` here is the whole behavioural change.
    headline = _headline(verdict, "3")
    assert headline["adequate_leads"] == [1, 2, 3, 4, 5]
    # Paired positive: the shortfall is the injected low-series geometry, not
    # an ambient adequacy failure — the candidate's own composite spans all
    # 24 dates on every lead, so 6 and 7 are dropped for the baseline reason
    # only, naming BOTH required baselines.
    dropped = cast(list[dict[str, object]], headline["dropped_leads"])
    assert [(d["lead"], d["reason"]) for d in dropped] == [
        (6, "baseline_absent"),
        (7, "baseline_absent"),
    ]
    for entry in dropped:
        assert entry["missing_baselines"] == [
            "baseline_all_feed_mean",
            "baseline_persistence",
        ]


# ---------------------------------------------------------------------------
# O8e/O8f — §12 condition 4 is evaluated on the HEADLINE CORE, and each
# endpoint's gate gets its OWN core. Both fixtures make the core differ from
# the baseline's own adequate set; without that difference the restriction is
# a no-op and every assertion below is vacuous.
# ---------------------------------------------------------------------------


def test_the_baseline_gate_is_evaluated_on_the_headline_core() -> None:
    """A candidate that wins only on a lead its own headline dropped.

    Persistence loses to the candidate by 0.125 on leads 1-4 and wins big on
    lead 5; all_feed_mean has no lead-5 series at all, so §8 drops lead 5
    from the headline core with reason ``baseline_absent``. Evaluated on
    that core the persistence gate must FAIL (pooled -0.125); evaluated on
    persistence's own wider set it passes (pooled +0.05), which is what the
    unrestricted gate shipped in 0.11.1 does.

    Kills:
    - M1, the shipped gate that passes no core: assertions 1, 3, 4 and 5 go
      red against it (`recommend`, `beats_baselines: True`,
      `adequate_leads == [1,2,3,4,5]`, `pooled_point == +0.05`).
    - M2, `core: tuple[int, ...] | None = None` meaning "unrestricted":
      behaviourally identical to M1, killed identically.
    - M3, restricting by REBUILDING the endpoint from the core leads instead
      of restricting the adequate set. Every value assertion here is green
      under M3 — the fixture's per-lead ratio is constant, so the effect is
      weight-invariant and shrinking the bootstrap universe moves no number.
      Assertion 6 is the sole discriminator: a rebuilt endpoint holds no
      lead-5 pairs, so `_adequate_leads`' `if pairs:` guard suppresses every
      record for it and no ``outside_core`` entry can exist.
    """
    headline = {ld: _ratio_series(_DATES, 0.5) for ld in range(1, 6)}
    persistence = {ld: _ratio_series(_DATES, 1.125) for ld in range(1, 5)}
    persistence[5] = _ratio_series(_DATES, 0.25)
    all_feed_mean = {ld: _ratio_series(_DATES, 0.5) for ld in range(1, 5)}
    candidate = CandidateSeries(
        key="3",
        continuous={"wind_max": headline},
        baseline_continuous={
            "baseline_persistence": {"wind_max": persistence},
            "baseline_all_feed_mean": {"wind_max": all_feed_mean},
        },
    )
    inputs = VariableInputs(variable="wind", incumbent_key="2", candidates=(candidate,))
    verdict = decide_variable(inputs, seed=20260815, resamples=60)

    # 1 — the flip itself.
    assert verdict.outcome == "retain_incumbent"
    assert verdict.recommended_key is None
    # 2 — the premise: the headline core excludes lead 5.
    head = _headline(verdict, "3")
    assert head["adequate_leads"] == [1, 2, 3, 4]
    assert head["pooled_point"] == pytest.approx(0.5)
    dropped = cast(list[dict[str, object]], head["dropped_leads"])
    assert dropped == [
        {
            "lead": 5,
            "reason": "baseline_absent",
            "missing_baselines": ["baseline_all_feed_mean"],
        }
    ]
    # 3 — the gate is isolated as the sole failing condition.
    conditions = _conditions(verdict, "3")
    assert conditions["beats_baselines"] is False
    assert conditions["ci_excludes_zero"] is True
    assert conditions["lead_stability"] is True
    assert conditions["practical_floor"] is True

    baselines = cast(dict[str, object], _record(verdict, "3")["baselines"])
    persistence_detail = cast(dict[str, object], baselines["baseline_persistence"])
    # 4 — the direct same-core pin.
    assert persistence_detail["adequate_leads"] == [1, 2, 3, 4]
    assert persistence_detail["passed"] is False
    assert persistence_detail["insufficient"] is False
    # 5 — the pooled effect on the core, and its sign.
    assert persistence_detail["pooled_point"] == pytest.approx(-0.125)
    persistence_ci = cast(list[float], persistence_detail["ci"])
    assert persistence_ci[0] < 0.0
    # 6 — MANDATORY: the drop record M3 structurally cannot emit.
    assert {"lead": 5, "reason": "outside_core"} in cast(
        list[dict[str, object]], persistence_detail["dropped_leads"]
    )
    # 7 — the other required baseline is beaten, so the conjunction fails on
    # persistence alone.
    all_feed_detail = cast(dict[str, object], baselines["baseline_all_feed_mean"])
    assert all_feed_detail["passed"] is True
    assert all_feed_detail["adequate_leads"] == [1, 2, 3, 4]

    # 8 — discriminating control. The SAME persistence endpoint, evaluated
    # with NO restriction at the gate's own level and seed (decide_variable
    # derives seed + 1000 * (offset + 1) per candidate, the wind gate then
    # adds 1), passes: adequate [1..5], pooled +0.05, CI above zero. Without
    # this the test would be green for any candidate that simply loses
    # everywhere; with it, the flip is proven to be caused by the
    # restriction alone.
    endpoint = _baseline_endpoint(
        candidate,
        "baseline_persistence",
        quantity="wind_max",
        occurrence=False,
        temp=False,
    )
    unrestricted = _evaluate_endpoint(
        endpoint,
        occurrence=False,
        level=BASELINE_GATE_CI_LEVEL,
        seed=20260815 + 1000 + 1,
        resamples=60,
    )
    assert unrestricted.adequate == (1, 2, 3, 4, 5)
    assert unrestricted.point == pytest.approx(0.05)
    assert unrestricted.ci is not None
    assert unrestricted.ci[0] > 0.0


def test_each_precip_gate_uses_its_own_endpoint_core() -> None:
    """The precip path has two gates and two DIFFERENT cores.

    ``baseline_all_feed_mean`` has no precip-total series at lead 5, so §8
    drops lead 5 from the total core; ``baseline_persistence`` DOES carry
    lead 5 there, and every occurrence baseline covers leads 1-5, so the
    occurrence core keeps it. Assertions 1 and 2 are the premise — they
    establish that the two cores genuinely differ, without which 3 and 4 are
    vacuous.

    The asymmetry between the two total baselines is deliberate: it is what
    makes the total gate's own restriction observable. With both total
    baselines stopping at lead 4 their unrestricted sets already equal the
    core and M1 is an equivalent mutant on this fixture.

    Kills:
    - passing `total.adequate` to the occurrence gate: the occurrence
      baselines' adequate sets collapse to [1, 2, 3, 4] (assertion 4);
    - passing `occ.adequate` to the total gate: persistence then reports
      [1, 2, 3, 4, 5] and all_feed_mean, which holds no lead-5 total series,
      fires `missing_leads == [5]` and turns `insufficient: True` (both
      halves of assertion 3);
    - M1/M2 on the total gate: unrestricted, persistence reports
      [1, 2, 3, 4, 5] and carries no ``outside_core`` record (assertion 3).
    """
    total_series = {ld: _ratio_series(_DATES, 0.5, base=1.0) for ld in range(1, 6)}
    total_persistence = {
        ld: _strong_baseline(_ratio_series(_DATES, 0.5, base=1.0)) for ld in range(1, 6)
    }
    total_all_feed_mean = {
        ld: _strong_baseline(_ratio_series(_DATES, 0.5, base=1.0)) for ld in range(1, 5)
    }
    # 12 candidate-wet and 12 candidate-dry days per lead, both clear of the
    # OCCURRENCE_MIN_WET_DAYS / OCCURRENCE_MIN_DRY_DAYS floor of 8.
    occ: OccurrenceLead = {}
    for i, day in enumerate(_DATES):
        occ[day] = ("hit", "miss") if i < 12 else ("correct_negative", "false_alarm")
    candidate = CandidateSeries(
        key="3",
        continuous={"precip_total": total_series},
        occurrence={ld: dict(occ) for ld in range(1, 6)},
        baseline_continuous={
            "baseline_persistence": {"precip_total": total_persistence},
            "baseline_all_feed_mean": {"precip_total": total_all_feed_mean},
        },
        baseline_occurrence=occurrence_baseline_set(
            {ld: dict(occ) for ld in range(1, 6)}
        ),
    )
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(candidate,)
    )
    verdict = decide_variable(inputs, seed=20260816, resamples=60)
    record = _record(verdict, "3")

    # 1 and 2 — the premise: the two cores differ.
    total_core = cast(dict[str, object], record["total"])["adequate_leads"]
    occ_core = cast(dict[str, object], record["occurrence"])["adequate_leads"]
    assert total_core == [1, 2, 3, 4]
    assert occ_core == [1, 2, 3, 4, 5]

    baselines = cast(dict[str, object], record["baselines"])
    total_detail = cast(dict[str, object], baselines["total"])
    occ_detail = cast(dict[str, object], baselines["occurrence"])
    # 3 — the total gate got the TOTAL core, and cleared it.
    assert set(total_detail) == {"baseline_persistence", "baseline_all_feed_mean"}
    for name in total_detail:
        entry = cast(dict[str, object], total_detail[name])
        assert entry["adequate_leads"] == [1, 2, 3, 4], name
        assert entry["insufficient"] is False, name
        assert entry["passed"] is True, name
        assert "missing_leads" not in entry, name
    # ... and persistence, which DOES hold a lead-5 total series, names the
    # lead the restriction removed. The `if pairs:` guard means only a
    # genuinely supported lead can produce this record.
    assert cast(dict[str, object], total_detail["baseline_persistence"])[
        "dropped_leads"
    ] == [{"lead": 5, "reason": "outside_core"}]
    assert (
        cast(dict[str, object], total_detail["baseline_all_feed_mean"])["dropped_leads"]
        == []
    )
    # 4 — the occurrence gate got the OCCURRENCE core, all five leads.
    assert set(occ_detail) == {
        "baseline_persistence",
        "baseline_all_feed_mean",
        "baseline_always_dry",
    }
    for name in occ_detail:
        entry = cast(dict[str, object], occ_detail[name])
        assert entry["adequate_leads"] == [1, 2, 3, 4, 5], name
        assert entry["insufficient"] is False, name
        assert entry["dropped_leads"] == [], name


# ---------------------------------------------------------------------------
# O9 — precip mixed_by_quantity: material total improvement, occurrence
# degraded beyond the non-inferiority margin
# ---------------------------------------------------------------------------


def test_precip_mixed_by_quantity_when_occurrence_degrades() -> None:
    dates = _dates_n(28)
    total = {ld: _ratio_series(dates, 0.5, base=1.0) for ld in range(1, 8)}
    # Occurrence: candidate h=8 m=4 f=4 cn=12 vs a PERFECT opponent
    # (h=12, cn=16). cand ETS = (8 - 144/28)/(16 - 144/28) = (20/7)/(76/7)
    # = 5/19; opp ETS = 1.0 -> pooled diff = 5/19 - 1 = -14/19 << -0.02.
    occ: OccurrenceLead = {}
    # §8: the occurrence gate is evaluated unconditionally, so the endpoint
    # needs its required baselines. A baseline that misses every wet day AND
    # false-alarms every dry day (ETS < 0) is beaten decisively, keeping the
    # case about the incumbent comparison rather than the gate.
    occ_baseline: OccurrenceLead = {}
    for i, d in enumerate(dates):
        if i < 8:
            occ[d] = ("hit", "hit")
        elif i < 12:
            occ[d] = ("miss", "hit")
        elif i < 16:
            occ[d] = ("false_alarm", "correct_negative")
        else:
            occ[d] = ("correct_negative", "correct_negative")
        occ_baseline[d] = (occ[d][0], "miss" if i < 12 else "false_alarm")
    strong = {ld: _strong_baseline(total[ld]) for ld in range(1, 8)}
    candidate = CandidateSeries(
        key="3",
        continuous={"precip_total": total},
        occurrence={ld: dict(occ) for ld in range(1, 8)},
        baseline_continuous={
            "baseline_persistence": {"precip_total": strong},
            "baseline_all_feed_mean": {"precip_total": strong},
        },
        baseline_occurrence=occurrence_baseline_set(
            {ld: occ_baseline for ld in range(1, 8)}
        ),
    )
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(candidate,)
    )
    verdict = decide_variable(inputs, seed=20260709, resamples=200)
    assert verdict.outcome == "mixed_by_quantity"
    record = _record(verdict, "3")
    occ_eval = cast(dict[str, object], record["occurrence"])
    assert occ_eval["pooled_point"] == pytest.approx(5 / 19 - 1.0)
    assert record["conditions"] == {
        "total_material": True,
        "occurrence_material": False,
        "total_non_inferior": True,
        "occurrence_non_inferior": False,
        "improved_endpoints": [],
        "beats_baselines": True,
    }


# ---------------------------------------------------------------------------
# O10 — tie-break on material CI overlap: prefer the depth closest to the
# incumbent; the other passer is reported statistically unresolved
# ---------------------------------------------------------------------------


def test_tie_break_overlap_prefers_depth_closest_to_incumbent() -> None:
    # "3": constant ratio 0.5 -> degenerate CI at 0.5.
    cand3 = _wind_candidate("3", {ld: _ratio_series(_DATES, 0.5) for ld in range(1, 8)})
    # "4": alternating ratio 0.39/0.59 on constant opponent 2.0 -> point
    # 1 - 0.49 = 0.51 with CI ~ +-0.03: overlaps [0.5, 0.5], best by pooled.
    noisy: ContinuousLead = {}
    for i, d in enumerate(_DATES):
        ratio = 0.39 if i % 2 == 0 else 0.59
        noisy[d] = (ratio * 2.0, 2.0)
    cand4 = _wind_candidate("4", {ld: dict(noisy) for ld in range(1, 8)})
    inputs = VariableInputs(
        variable="wind", incumbent_key="2", candidates=(cand3, cand4)
    )
    verdict = decide_variable(inputs, seed=20260710, resamples=400)
    assert verdict.outcome == "recommend"
    assert verdict.recommended_key == "3"  # |3-2| = 1 < |4-2| = 2
    assert verdict.detail["tie_break"] == {"best_by_pooled": "4", "chosen": "3"}
    assert verdict.detail["statistically_unresolved"] == ["4"]
    # The overlap premise itself: candidate 4's CI must contain 0.5.
    ci4 = cast(list[float], _headline(verdict, "4")["ci"])
    assert ci4[0] < 0.5 < ci4[1]
    assert _headline(verdict, "4")["pooled_point"] == pytest.approx(0.51)


# ---------------------------------------------------------------------------
# O11/O12 — §18.13 D0 robustness: lead 0 is structurally excluded from the
# decision and cannot flip or change any verdict
# ---------------------------------------------------------------------------


def _identity_series(dates: list[str]) -> ContinuousLead:
    return {d: (o, o) for d, (_c, o) in _ratio_series(dates, 1.0).items()}


def test_extreme_day0_improvement_cannot_flip_a_retain() -> None:
    # Leads 1-7: no signal at all (candidate == incumbent). Lead 0: a
    # massive 0.9 improvement. §12 decides on D1..D7 only -> retain.
    leads: dict[int, ContinuousLead] = {
        ld: _identity_series(_DATES) for ld in range(1, 8)
    }
    leads[0] = _ratio_series(_DATES, 0.1)
    inputs = VariableInputs(
        variable="wind",
        incumbent_key="2",
        candidates=(_wind_candidate("3", leads),),
    )
    verdict = decide_variable(inputs, seed=20260711, resamples=60)
    assert verdict.outcome == "retain_incumbent"
    headline = _headline(verdict, "3")
    assert headline["adequate_leads"] == list(range(1, 8))
    per_lead = cast(dict[str, object], headline["per_lead"])
    assert set(per_lead) == {str(ld) for ld in range(1, 8)}
    assert headline["pooled_point"] == pytest.approx(0.0)


def test_day0_series_never_changes_the_verdict() -> None:
    def build(with_d0: bool) -> Verdict:
        leads3: dict[int, ContinuousLead] = {
            ld: _ratio_series(_DATES, 0.5) for ld in range(1, 8)
        }
        leads4: dict[int, ContinuousLead] = {
            ld: _ratio_series(_DATES, 1.3) for ld in range(1, 8)
        }
        if with_d0:
            leads3[0] = _ratio_series(_DATES, 1.0)
            leads4[0] = _ratio_series(_DATES, 0.02)  # huge D0-only win
        inputs = VariableInputs(
            variable="wind",
            incumbent_key="2",
            candidates=(
                _wind_candidate("3", leads3),
                _wind_candidate("4", leads4),
            ),
        )
        return decide_variable(inputs, seed=20260712, resamples=60)

    with_d0 = build(True)
    without_d0 = build(False)
    assert with_d0 == without_d0
    assert with_d0.outcome == "recommend"
    assert with_d0.recommended_key == "3"


# ---------------------------------------------------------------------------
# O13 — aggregate: strict common core + availability floor over real
# evidence rows in a migrated in-memory database (§8/§11)
# ---------------------------------------------------------------------------

_O13_DAYS = ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04"]


def _insert_run_row(conn: sqlite3.Connection, site_id: int, generation: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, period_start, period_end,
             settled_through, bootstrap_seed, bootstrap_resamples,
             input_fingerprint, created_at)
        VALUES (?, ?, 1, 'test', 'running', 1, '{}', '2026-07-01',
                '2026-07-04', '2026-07-04', 1, 10, 'f' || ?,
                '2026-07-05T12:00:00Z')
        """,
        (site_id, generation, site_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_evidence(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    day: str,
    entity_type: str,
    entity_key: str,
    predicted: float | None,
    eligible: bool = True,
    truth: float = 10.0,
) -> None:
    abs_error = None if predicted is None else abs(predicted - truth)
    conn.execute(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, predicted, forecast_eligible,
             truth_value, truth_eligible, abs_error)
        VALUES (?, ?, ?, 1, 'wind', 'wind_max', ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            run_id,
            day,
            day,
            entity_type,
            entity_key,
            predicted,
            1 if eligible else 0,
            truth,
            abs_error,
        ),
    )


def test_aggregate_strict_common_core_and_availability_floor() -> None:
    conn = asof_conn()
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                           timezone)
        VALUES ('oracle-town', 47.0, 25.0, 900.0, 'UTC')
        """
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    generation = ensure_published_generation(conn, site_id)
    run_id = _insert_run_row(conn, site_id, generation)

    # Depths (all 4 days eligible). Incumbent depth 2 errors: 2, -4, 6, 8.
    depth_predictions = {
        "1": [13.0, 13.0, 13.0, 13.0],
        "2": [12.0, 6.0, 16.0, 18.0],
        "3": [11.0, 8.0, 13.0, 14.0],  # errors +1, -2, +3, +4
        "4": [15.0, 15.0, 15.0, 15.0],
    }
    for key, values in depth_predictions.items():
        for day, predicted in zip(_O13_DAYS, values, strict=True):
            _insert_evidence(
                conn,
                run_id,
                day=day,
                entity_type="depth",
                entity_key=key,
                predicted=predicted,
            )
    for baseline, key, predicted in (
        ("baseline_persistence", "persistence", 14.0),
        ("baseline_all_feed_mean", "all_feed_mean", 16.0),
    ):
        for day in _O13_DAYS:
            _insert_evidence(
                conn,
                run_id,
                day=day,
                entity_type=baseline,
                entity_key=key,
                predicted=predicted,
            )
    # Feed 101: eligible on 3 of 4 truth days -> availability 0.75 >= 0.70.
    for day in _O13_DAYS[:3]:
        _insert_evidence(
            conn,
            run_id,
            day=day,
            entity_type="feed",
            entity_key="101",
            predicted=10.5,
        )
    _insert_evidence(
        conn,
        run_id,
        day=_O13_DAYS[3],
        entity_type="feed",
        entity_key="101",
        predicted=None,
        eligible=False,
    )
    # Feed 102: eligible on only 2 of 4 -> 0.5 < 0.70: below the floor.
    for day in _O13_DAYS[:2]:
        _insert_evidence(
            conn,
            run_id,
            day=day,
            entity_type="feed",
            entity_key="102",
            predicted=10.5,
        )
    _insert_evidence(
        conn,
        run_id,
        day=_O13_DAYS[2],
        entity_type="feed",
        entity_key="102",
        predicted=None,
        eligible=False,
    )
    conn.commit()

    cfg = RunConfig(
        site_id=site_id,
        run_id=run_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        wall_clock="wc",
        blend_depth=2,
        # Divergent on purpose: only `wind` evidence exists, so every anchor
        # below is unchanged, but temperature/precip now differ from both
        # each other and the global `blend_depth=2`. A mutant that resolved
        # the incumbent from the global (or from the first/any variable's
        # entry) instead of the cell's OWN variable would pick 1 or 4 here
        # and break the hand-derived deltas (0.5 and 5/6).
        blend_depths={"temperature": 1, "wind": 2, "precip": 4},
        min_n=30,
        window_days=30,
        tz_generation_id=generation,
        roster=(
            RosterFeed(feed_id=101, source="example-src", model="model-gamma"),
            RosterFeed(feed_id=102, source="example-src", model="model-delta"),
        ),
        period_start="2026-07-01",
        period_end="2026-07-04",
        bootstrap_seed=1,
        bootstrap_resamples=10,
    )
    aggregate_run(conn, cfg)
    conn.commit()

    raw_state = conn.execute(
        "SELECT aggregate_state FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()["aggregate_state"]
    state = cast(dict[str, object], json.loads(str(raw_state)))
    cell = cast(dict[str, object], state["wind|1|wind_max"])
    # Strict common core: truth {d1..d4} intersected with EVERY member's
    # eligible dates; feed 101 (a member) is missing d4 -> core = d1..d3.
    assert cell["common_dates"] == _O13_DAYS[:3]
    members = [tuple(cast(list[str], m)) for m in cast(list[object], cell["members"])]
    assert ("feed", "101") in members
    assert ("feed", "102") not in members
    assert ("depth", "3") in members
    assert ("baseline_persistence", "persistence") in members

    def result(entity_type: str, entity_key: str) -> sqlite3.Row:
        row = conn.execute(
            """
            SELECT * FROM verification_results
            WHERE run_id = ? AND entity_type = ? AND entity_key = ?
            """,
            (run_id, entity_type, entity_key),
        ).fetchone()
        assert row is not None
        return cast(sqlite3.Row, row)

    # depth 3 on the common core d1..d3: errors +1, -2, +3.
    depth3 = result("depth", "3")
    assert int(depth3["headline"]) == 1
    assert int(depth3["common_days"]) == 3
    assert float(depth3["mae"]) == pytest.approx(2.0)
    assert float(depth3["bias"]) == pytest.approx(2 / 3)
    assert float(depth3["rmse"]) == pytest.approx(math.sqrt(14 / 3))
    # Incumbent depth 2 on the same core: errors 2, -4, 6 -> MAE 4.0;
    # delta = (4 - 2)/4 = 0.5.
    assert float(depth3["delta_vs_incumbent"]) == pytest.approx(0.5)

    feed101 = result("feed", "101")
    assert int(feed101["headline"]) == 1
    assert float(feed101["availability_rate"]) == pytest.approx(0.75)
    assert int(feed101["common_days"]) == 3
    assert float(feed101["mae"]) == pytest.approx(0.5)

    # Below-floor feed: headline 0, scored on the PAIRWISE core with the
    # incumbent (d1, d2 — the feed's own eligible days).
    feed102 = result("feed", "102")
    assert int(feed102["headline"]) == 0
    assert float(feed102["availability_rate"]) == pytest.approx(0.5)
    assert int(feed102["common_days"]) == 2
    assert float(feed102["mae"]) == pytest.approx(0.5)
    # Incumbent on d1, d2: errors 2, -4 -> MAE 3.0; delta = (3 - 0.5)/3.
    assert float(feed102["delta_vs_incumbent"]) == pytest.approx(5 / 6)
