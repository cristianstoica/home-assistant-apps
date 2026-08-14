"""Pure §12 gatekeeping: bootstrap CIs, gates, and the per-variable verdict.

No SQLite, no clock — the engine hands this module already-joined
common-core paired series (candidate vs incumbent and candidate vs each
baseline, per lead and quantity) and gets back one verdict per variable
plus a JSON-able evidence record for ``verification_verdicts.tested_family``.

Clustering unit: the TARGET DATE. One moving-block bootstrap resample draws
a date multiset from the comparison's date universe; every lead and
quantity re-weights its paired days by those date multiplicities, so
same-date dependence across leads survives resampling (§12).
"""

from __future__ import annotations

import math
import random
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field

from wxverify.verification.methodology import (
    ADEQUATE_LEAD_MIN_DAYS,
    BASELINE_GATE_CI_LEVEL,
    BOOTSTRAP_BLOCK_LENGTH_DAYS,
    CANDIDATE_CI_LEVEL,
    CONTINUOUS_BASELINES,
    LEAD_STABILITY_DENOMINATOR,
    LEAD_STABILITY_NUMERATOR,
    MIN_ADEQUATE_LEADS_PER_VARIABLE,
    NON_INFERIORITY_ETS_MARGIN,
    NON_INFERIORITY_MAE_MARGIN,
    OCCURRENCE_BASELINES,
    OCCURRENCE_MIN_DRY_DAYS,
    OCCURRENCE_MIN_WET_DAYS,
    PRACTICAL_FLOOR_ETS,
    PRACTICAL_FLOOR_RELATIVE_MAE,
    PRECIP_IMPROVEMENT_CI_LEVEL,
)
from wxverify.verification.stats import (
    Contingency,
    ets,
    moving_block_indices,
    percentile_ci,
)

#: Decision leads: D1..D7 (§12 — day 0 is display-only diagnostics).
DECISION_LEADS: tuple[int, ...] = (1, 2, 3, 4, 5, 6, 7)

#: date -> (candidate abs error, opponent abs error) on one lead's common core.
ContinuousLead = dict[str, tuple[float, float]]
#: date -> ((cand_high, opp_high), (cand_low, opp_low)) on the high∩low core.
TempLead = dict[str, tuple[tuple[float, float], tuple[float, float]]]
#: date -> (candidate class, opponent class); classes are contingency labels.
OccurrenceLead = dict[str, tuple[str, str]]

_WET_CLASSES = frozenset({"hit", "miss"})


@dataclass(frozen=True)
class CandidateSeries:
    """One candidate entity's paired common-core series for one variable.

    ``vs_incumbent*`` drive conditions 1-3 and 5-6; ``vs_baseline*`` drive
    condition 4 (keys: baseline entity_type). Temperature uses ``temp``;
    wind and precip-total use ``continuous`` keyed by quantity.
    """

    key: str
    continuous: dict[str, dict[int, ContinuousLead]] = field(
        default_factory=dict[str, dict[int, ContinuousLead]]
    )
    temp: dict[int, TempLead] = field(default_factory=dict[int, TempLead])
    occurrence: dict[int, OccurrenceLead] = field(
        default_factory=dict[int, OccurrenceLead]
    )
    baseline_continuous: dict[str, dict[str, dict[int, ContinuousLead]]] = field(
        default_factory=dict[str, dict[str, dict[int, ContinuousLead]]]
    )
    baseline_occurrence: dict[str, dict[int, OccurrenceLead]] = field(
        default_factory=dict[str, dict[int, OccurrenceLead]]
    )


@dataclass(frozen=True)
class VariableInputs:
    """Everything §12 needs to decide one variable."""

    variable: str
    incumbent_key: str
    candidates: tuple[CandidateSeries, ...]


@dataclass(frozen=True)
class Verdict:
    """One variable's decision plus its JSON-able evidence record."""

    variable: str
    outcome: str
    recommended_key: str | None
    detail: dict[str, object]


def required_lead_agreement(adequate: int) -> int:
    """Leads that must agree for lead-stability, shared with §10 (W7)."""
    return math.ceil(adequate * LEAD_STABILITY_NUMERATOR / LEAD_STABILITY_DENOMINATOR)


def _weighted_mae(
    pairs: list[tuple[int, float, float]], counts: Counter[int] | None, side: int
) -> float | None:
    """Weighted MAE of one side of a paired series; None on zero weight."""
    total = 0.0
    weight = 0
    for idx, cand, opp in pairs:
        w = 1 if counts is None else counts.get(idx, 0)
        if w == 0:
            continue
        total += w * (cand if side == 0 else opp)
        weight += w
    if weight == 0:
        return None
    return total / weight


def _rel_improvement(
    pairs: list[tuple[int, float, float]], counts: Counter[int] | None
) -> float | None:
    """(MAE_opp - MAE_cand) / MAE_opp on the weighted common core."""
    cand = _weighted_mae(pairs, counts, 0)
    opp = _weighted_mae(pairs, counts, 1)
    if cand is None or opp is None or opp == 0:
        return None
    return (opp - cand) / opp


def _temp_headline_improvement(
    pairs: list[tuple[int, tuple[float, float], tuple[float, float]]],
    counts: Counter[int] | None,
) -> float | None:
    """Relative improvement of mean(high MAE, low MAE) on the h∩l core (§12.5)."""
    hi = [(idx, h[0], h[1]) for idx, h, _ in pairs]
    lo = [(idx, low[0], low[1]) for idx, _, low in pairs]
    cand_h = _weighted_mae(hi, counts, 0)
    opp_h = _weighted_mae(hi, counts, 1)
    cand_l = _weighted_mae(lo, counts, 0)
    opp_l = _weighted_mae(lo, counts, 1)
    if None in (cand_h, opp_h, cand_l, opp_l):
        return None
    assert cand_h is not None and opp_h is not None
    assert cand_l is not None and opp_l is not None
    cand = (cand_h + cand_l) / 2.0
    opp = (opp_h + opp_l) / 2.0
    if opp == 0:
        return None
    return (opp - cand) / opp


def _weighted_contingency(
    pairs: list[tuple[int, str, str]], counts: Counter[int] | None, side: int
) -> Contingency:
    hits = misses = fas = cns = 0
    for idx, cand, opp in pairs:
        w = 1 if counts is None else counts.get(idx, 0)
        if w == 0:
            continue
        label = cand if side == 0 else opp
        if label == "hit":
            hits += w
        elif label == "miss":
            misses += w
        elif label == "false_alarm":
            fas += w
        else:
            cns += w
    return Contingency(
        hits=hits, misses=misses, false_alarms=fas, correct_negatives=cns
    )


def _ets_diff(
    pairs: list[tuple[int, str, str]], counts: Counter[int] | None
) -> float | None:
    """ETS(candidate) - ETS(opponent), recomputed from full tables (§12)."""
    cand = ets(_weighted_contingency(pairs, counts, 0))
    opp = ets(_weighted_contingency(pairs, counts, 1))
    if cand is None or opp is None:
        return None
    return cand - opp


@dataclass(frozen=True)
class _Endpoint:
    """One comparison endpoint's indexed per-lead series + effect function."""

    kind: str  # 'continuous' | 'temp' | 'occurrence'
    leads: dict[int, list[tuple[int, object, object]]]
    universe: list[str]

    def lead_effect(self, lead: int, counts: Counter[int] | None) -> float | None:
        pairs = self.leads.get(lead)
        if not pairs:
            return None
        if self.kind == "continuous":
            typed_c = [(i, float(a), float(b)) for i, a, b in pairs]  # pyright: ignore[reportArgumentType]
            return _rel_improvement(typed_c, counts)
        if self.kind == "temp":
            typed_t: list[tuple[int, tuple[float, float], tuple[float, float]]] = pairs  # pyright: ignore[reportAssignmentType]
            return _temp_headline_improvement(typed_t, counts)
        typed_o = [(i, str(a), str(b)) for i, a, b in pairs]
        return _ets_diff(typed_o, counts)

    def pooled_effect(
        self, adequate: tuple[int, ...], counts: Counter[int] | None
    ) -> float | None:
        effects = [
            e
            for e in (self.lead_effect(lead, counts) for lead in adequate)
            if e is not None
        ]
        if not effects:
            return None
        return sum(effects) / len(effects)


def _index_endpoint(
    kind: str, series_by_lead: Mapping[int, Mapping[str, tuple[object, object]]]
) -> _Endpoint:
    universe = sorted({d for lead in series_by_lead.values() for d in lead})
    date_idx = {d: i for i, d in enumerate(universe)}
    leads: dict[int, list[tuple[int, object, object]]] = {}
    for lead, days in series_by_lead.items():
        leads[lead] = [
            (date_idx[d], pair[0], pair[1]) for d, pair in sorted(days.items())
        ]
    return _Endpoint(kind=kind, leads=leads, universe=universe)


def _adequate_leads(
    endpoint: _Endpoint,
    *,
    occurrence: bool,
    baseline_support: Mapping[str, frozenset[int]] | None = None,
    restrict_to: tuple[int, ...] | None = None,
) -> tuple[tuple[int, ...], list[dict[str, object]]]:
    """§12 adequacy: >= 20 strict-common days; occurrence also needs the
    minimum wet/dry event counts on the OBSERVED side of the common core.

    §8/W5 adds the required-baseline condition INSIDE this same loop rather
    than as a filter over its output: a lead where a required baseline has
    no adequately supported series is dropped, so the candidate is never
    scored on a lead it was not baseline-checked at. The returned drop
    records keep the two causes distinguishable — a thin lead and a
    baseline-less lead are different claims and must not collapse into one
    report.

    ``restrict_to`` is the headline core the caller pools over; it is the
    LAST condition applied, so ``outside_core`` names only leads this
    endpoint genuinely supported. ``None`` means no restriction — never an
    empty core.
    """
    out: list[int] = []
    dropped: list[dict[str, object]] = []
    for lead in DECISION_LEADS:
        pairs = endpoint.leads.get(lead, [])
        if len(pairs) < ADEQUATE_LEAD_MIN_DAYS:
            if pairs:
                dropped.append(
                    {"lead": lead, "reason": "thin_data", "days": len(pairs)}
                )
            continue
        if occurrence:
            wet = sum(1 for _, cand, _ in pairs if str(cand) in _WET_CLASSES)
            dry = len(pairs) - wet
            if wet < OCCURRENCE_MIN_WET_DAYS or dry < OCCURRENCE_MIN_DRY_DAYS:
                dropped.append(
                    {"lead": lead, "reason": "thin_events", "wet": wet, "dry": dry}
                )
                continue
        missing = sorted(
            baseline
            for baseline, leads in (baseline_support or {}).items()
            if lead not in leads
        )
        if missing:
            dropped.append(
                {
                    "lead": lead,
                    "reason": "baseline_absent",
                    "missing_baselines": missing,
                }
            )
            continue
        if restrict_to is not None and lead not in restrict_to:
            dropped.append({"lead": lead, "reason": "outside_core"})
            continue
        out.append(lead)
    return tuple(out), dropped


def _bootstrap_ci(
    endpoint: _Endpoint,
    adequate: tuple[int, ...],
    *,
    level: float,
    seed: int,
    resamples: int,
    block: int = BOOTSTRAP_BLOCK_LENGTH_DAYS,
) -> tuple[float, float] | None:
    """Percentile CI of the pooled effect under the date-clustered bootstrap.

    Resamples with an undefined pooled effect (e.g. degenerate ETS tables)
    are dropped; the CI is undefined when fewer than half the resamples
    survive — a deliberately conservative fail-closed rule.
    """
    n = len(endpoint.universe)
    if n == 0 or not adequate:
        return None
    rng = random.Random(seed)
    draws: list[float] = []
    for _ in range(resamples):
        counts = Counter(moving_block_indices(rng, n, block))
        effect = endpoint.pooled_effect(adequate, counts)
        if effect is not None:
            draws.append(effect)
    if len(draws) < resamples / 2:
        return None
    return percentile_ci(draws, level)


@dataclass(frozen=True)
class _EndpointEvaluation:
    adequate: tuple[int, ...]
    point: float | None
    ci: tuple[float, float] | None
    per_lead: dict[int, float | None]
    dropped: list[dict[str, object]] = field(default_factory=list[dict[str, object]])

    def as_json(self) -> dict[str, object]:
        return {
            "adequate_leads": list(self.adequate),
            "pooled_point": self.point,
            "ci": None if self.ci is None else list(self.ci),
            "per_lead": {str(k): v for k, v in self.per_lead.items()},
            "dropped_leads": self.dropped,
        }


def _evaluate_endpoint(
    endpoint: _Endpoint,
    *,
    occurrence: bool,
    level: float,
    seed: int,
    resamples: int,
    baseline_support: Mapping[str, frozenset[int]] | None = None,
    restrict_to: tuple[int, ...] | None = None,
) -> _EndpointEvaluation:
    adequate, dropped = _adequate_leads(
        endpoint,
        occurrence=occurrence,
        baseline_support=baseline_support,
        restrict_to=restrict_to,
    )
    point = endpoint.pooled_effect(adequate, None) if adequate else None
    ci = _bootstrap_ci(endpoint, adequate, level=level, seed=seed, resamples=resamples)
    per_lead = {lead: endpoint.lead_effect(lead, None) for lead in adequate}
    return _EndpointEvaluation(
        adequate=adequate, point=point, ci=ci, per_lead=per_lead, dropped=dropped
    )


def _lead_stability(evaluation: _EndpointEvaluation) -> bool:
    beneficial = sum(1 for e in evaluation.per_lead.values() if e is not None and e > 0)
    return beneficial >= required_lead_agreement(len(evaluation.adequate))


def _beats(evaluation: _EndpointEvaluation) -> bool:
    return evaluation.ci is not None and evaluation.ci[0] > 0


def _baseline_endpoint(
    candidate: CandidateSeries,
    baseline: str,
    *,
    quantity: str | None,
    occurrence: bool,
    temp: bool,
) -> _Endpoint:
    """The candidate-vs-one-baseline endpoint, built the SAME way everywhere.

    One construction serves both the gate and the per-lead support map that
    reduces the candidate's adequacy set, so the two can never disagree
    about what a baseline supports at a lead.
    """
    if occurrence:
        series = candidate.baseline_occurrence.get(baseline, {})
        return _index_endpoint(
            "occurrence",
            {
                ld: {d: (c, o) for d, (c, o) in days.items()}
                for ld, days in series.items()
            },
        )
    per_quantity = candidate.baseline_continuous.get(baseline, {})
    if temp:
        high_by_lead = per_quantity.get("temperature_high", {})
        low_by_lead = per_quantity.get("temperature_low", {})
        joint_by_lead: dict[int, TempLead] = {}
        for lead in high_by_lead.keys() & low_by_lead.keys():
            high = high_by_lead[lead]
            low = low_by_lead[lead]
            joint: TempLead = {d: (high[d], low[d]) for d in high.keys() & low.keys()}
            if joint:
                joint_by_lead[lead] = joint
        return _index_endpoint(
            "temp",
            {
                ld: {d: (h, low_pair) for d, (h, low_pair) in days.items()}
                for ld, days in joint_by_lead.items()
            },
        )
    assert quantity is not None
    continuous = per_quantity.get(quantity, {})
    return _index_endpoint(
        "continuous",
        {
            ld: {d: (c, o) for d, (c, o) in days.items()}
            for ld, days in continuous.items()
        },
    )


def _required_baselines(*, occurrence: bool) -> tuple[str, ...]:
    return OCCURRENCE_BASELINES if occurrence else CONTINUOUS_BASELINES


def _baseline_support(
    candidate: CandidateSeries,
    *,
    quantity: str | None,
    occurrence: bool,
    temp: bool = False,
) -> dict[str, frozenset[int]]:
    """Per required baseline, the leads where it has an adequate series.

    §8/W5: membership is tested PER LEAD, never over the lead-aggregated
    map — an aggregated test passes whenever any single lead supplied the
    baseline, which is exactly the empty-roster case §7 produces.
    """
    return {
        baseline: frozenset(
            _adequate_leads(
                _baseline_endpoint(
                    candidate,
                    baseline,
                    quantity=quantity,
                    occurrence=occurrence,
                    temp=temp,
                ),
                occurrence=occurrence,
            )[0]
        )
        for baseline in _required_baselines(occurrence=occurrence)
    }


def _baseline_gate(
    candidate: CandidateSeries,
    *,
    quantity: str | None,
    occurrence: bool,
    temp: bool = False,
    seed: int,
    resamples: int,
    core: tuple[int, ...],
) -> tuple[bool, dict[str, object]]:
    """Condition 4: beat EVERY REQUIRED baseline at 95% on ``core``.

    ``core`` is the headline evaluation's adequate-lead set, passed as a
    REQUIRED keyword so no call site can fall back to a per-baseline set
    of its own — the omission this signature exists to make impossible.

    Allowlist, not presence test: the gate iterates the required set, so a
    missing or unsupported baseline fails it and writes a named
    ``insufficient`` entry. A baseline that cannot support every lead of
    ``core`` fails the same way and names the leads: §8 already removed
    such leads from ``core``, so a shortfall here is a broken contract and
    is reported as NOT SHOWN — never as a silent pass, never as a verdict
    of worse. An absent detail block reads as "passed" on the surface, so
    no branch may omit one.
    """
    detail: dict[str, object] = {}
    passed = True
    for baseline in _required_baselines(occurrence=occurrence):
        endpoint = _baseline_endpoint(
            candidate, baseline, quantity=quantity, occurrence=occurrence, temp=temp
        )
        evaluation = _evaluate_endpoint(
            endpoint,
            occurrence=occurrence,
            level=BASELINE_GATE_CI_LEVEL,
            seed=seed,
            resamples=resamples,
            restrict_to=core,
        )
        if not evaluation.adequate:
            passed = False
            detail[baseline] = {
                "passed": False,
                "insufficient": True,
                "reason": "required baseline missing or under-supported",
                **evaluation.as_json(),
            }
            continue
        missing_leads = [lead for lead in core if lead not in evaluation.adequate]
        if missing_leads:
            passed = False
            detail[baseline] = {
                "passed": False,
                "insufficient": True,
                "reason": "required baseline not supported on every core lead",
                "missing_leads": missing_leads,
                **evaluation.as_json(),
            }
            continue
        ok = _beats(evaluation)
        passed = passed and ok
        detail[baseline] = {"passed": ok, "insufficient": False, **evaluation.as_json()}
    return passed, detail


@dataclass(frozen=True)
class _CandidateResult:
    key: str
    passed: bool
    insufficient: bool
    mixed_by_lead: bool
    mixed_by_quantity: bool
    pooled: float | None
    ci: tuple[float, float] | None
    record: dict[str, object]


def _decide_wind_or_temp(
    inputs: VariableInputs, candidate: CandidateSeries, *, seed: int, resamples: int
) -> _CandidateResult:
    temp = inputs.variable == "temperature"
    headline_quantity = None if temp else "wind_max"
    # §8/W5: the adequacy set is reduced BEFORE the sufficiency test below,
    # so a candidate can never clear the four-lead floor on leads it was
    # never baseline-checked at.
    support = _baseline_support(
        candidate, quantity=headline_quantity, occurrence=False, temp=temp
    )
    if inputs.variable == "temperature":
        endpoint = _index_endpoint(
            "temp",
            {
                ld: {d: (h, low) for d, (h, low) in days.items()}
                for ld, days in candidate.temp.items()
            },
        )
    else:
        series = candidate.continuous.get("wind_max", {})
        endpoint = _index_endpoint(
            "continuous",
            {
                ld: {d: (c, o) for d, (c, o) in days.items()}
                for ld, days in series.items()
            },
        )
    evaluation = _evaluate_endpoint(
        endpoint,
        occurrence=False,
        level=CANDIDATE_CI_LEVEL,
        seed=seed,
        resamples=resamples,
        baseline_support=support,
    )
    record: dict[str, object] = {"headline": evaluation.as_json()}
    if len(evaluation.adequate) < MIN_ADEQUATE_LEADS_PER_VARIABLE:
        return _CandidateResult(
            key=candidate.key,
            passed=False,
            insufficient=True,
            mixed_by_lead=False,
            mixed_by_quantity=False,
            pooled=evaluation.point,
            ci=evaluation.ci,
            record=record,
        )
    c1 = _beats(evaluation)
    c2 = _lead_stability(evaluation)
    c3 = (
        evaluation.point is not None
        and evaluation.point >= PRACTICAL_FLOOR_RELATIVE_MAE
    )
    if inputs.variable == "temperature":
        c4, baseline_detail = _baseline_gate(
            candidate,
            quantity=None,
            occurrence=False,
            temp=True,
            seed=seed + 1,
            resamples=resamples,
            core=evaluation.adequate,
        )
    else:
        c4, baseline_detail = _baseline_gate(
            candidate,
            quantity="wind_max",
            occurrence=False,
            seed=seed + 1,
            resamples=resamples,
            core=evaluation.adequate,
        )
    c5 = True
    if inputs.variable == "temperature":
        components: dict[str, object] = {}
        for component in ("temperature_high", "temperature_low"):
            comp_series = candidate.continuous.get(component, {})
            comp_endpoint = _index_endpoint(
                "continuous",
                {
                    ld: {d: (c, o) for d, (c, o) in days.items()}
                    for ld, days in comp_series.items()
                },
            )
            comp_point = comp_endpoint.pooled_effect(evaluation.adequate, None)
            degraded = (
                comp_point is not None and comp_point < -NON_INFERIORITY_MAE_MARGIN
            )
            components[component] = {"pooled_point": comp_point, "degraded": degraded}
            if degraded:
                c5 = False
        record["components"] = components
    record["conditions"] = {
        "ci_excludes_zero": c1,
        "lead_stability": c2,
        "practical_floor": c3,
        "beats_baselines": c4,
        "components_non_inferior": c5,
    }
    record["baselines"] = baseline_detail
    return _CandidateResult(
        key=candidate.key,
        passed=c1 and c2 and c3 and c4 and c5,
        insufficient=False,
        mixed_by_lead=c1 and c3 and c4 and c5 and not c2,
        mixed_by_quantity=False,
        pooled=evaluation.point,
        ci=evaluation.ci,
        record=record,
    )


def _decide_precip(
    candidate: CandidateSeries, *, seed: int, resamples: int
) -> _CandidateResult:
    total_support = _baseline_support(
        candidate, quantity="precip_total", occurrence=False
    )
    occ_support = _baseline_support(candidate, quantity=None, occurrence=True)
    total_series = candidate.continuous.get("precip_total", {})
    total = _evaluate_endpoint(
        _index_endpoint(
            "continuous",
            {
                ld: {d: (c, o) for d, (c, o) in days.items()}
                for ld, days in total_series.items()
            },
        ),
        occurrence=False,
        level=PRECIP_IMPROVEMENT_CI_LEVEL,
        seed=seed,
        resamples=resamples,
        baseline_support=total_support,
    )
    occ = _evaluate_endpoint(
        _index_endpoint(
            "occurrence",
            {
                ld: {d: (c, o) for d, (c, o) in days.items()}
                for ld, days in candidate.occurrence.items()
            },
        ),
        occurrence=True,
        level=PRECIP_IMPROVEMENT_CI_LEVEL,
        seed=seed,
        resamples=resamples,
        baseline_support=occ_support,
    )
    record: dict[str, object] = {
        "total": total.as_json(),
        "occurrence": occ.as_json(),
    }
    if (
        len(total.adequate) < MIN_ADEQUATE_LEADS_PER_VARIABLE
        and len(occ.adequate) < MIN_ADEQUATE_LEADS_PER_VARIABLE
    ):
        return _CandidateResult(
            key=candidate.key,
            passed=False,
            insufficient=True,
            mixed_by_lead=False,
            mixed_by_quantity=False,
            pooled=None,
            ci=None,
            record=record,
        )
    total_material = (
        len(total.adequate) >= MIN_ADEQUATE_LEADS_PER_VARIABLE
        and _beats(total)
        and total.point is not None
        and total.point >= PRACTICAL_FLOOR_RELATIVE_MAE
        and _lead_stability(total)
    )
    occ_material = (
        len(occ.adequate) >= MIN_ADEQUATE_LEADS_PER_VARIABLE
        and _beats(occ)
        and occ.point is not None
        and occ.point >= PRACTICAL_FLOOR_ETS
        and _lead_stability(occ)
    )
    # F-1: non-inferiority must be MEASURED, never vacuous. An endpoint with
    # zero adequate leads has point=None; treating that as non-inferior
    # would let a candidate reach 'recommend' with the other endpoint
    # unmeasurable (the wet-day-starved case). Fail closed: an unmeasurable
    # endpoint blocks the other endpoint's improvement, and the outcome
    # falls through to mixed_by_quantity below.
    total_non_inferior = (
        total.point is not None and total.point >= -NON_INFERIORITY_MAE_MARGIN
    )
    occ_non_inferior = (
        occ.point is not None and occ.point >= -NON_INFERIORITY_ETS_MARGIN
    )
    improved: list[str] = []
    if total_material and occ_non_inferior:
        improved.append("total")
    if occ_material and total_non_inferior:
        improved.append("occurrence")
    # §8/W5: both gates run UNCONDITIONALLY and both details are always
    # populated. `improved` reports which endpoint drove the outcome; it
    # never decides whether the gate runs, because a conjunction evaluated
    # over a subset of its own conditions is not the conjunction. The seed
    # offsets are unchanged so the bootstrap stream matches runs where both
    # gates already ran.
    total_ok, total_detail = _baseline_gate(
        candidate,
        quantity="precip_total",
        occurrence=False,
        seed=seed + 1,
        resamples=resamples,
        core=total.adequate,
    )
    occ_ok, occ_detail = _baseline_gate(
        candidate,
        quantity=None,
        occurrence=True,
        seed=seed + 2,
        resamples=resamples,
        core=occ.adequate,
    )
    c4 = total_ok and occ_ok
    baseline_detail: dict[str, object] = {
        "total": total_detail,
        "occurrence": occ_detail,
    }
    record["conditions"] = {
        "total_material": total_material,
        "occurrence_material": occ_material,
        "total_non_inferior": total_non_inferior,
        "occurrence_non_inferior": occ_non_inferior,
        "improved_endpoints": improved,
        "beats_baselines": c4,
    }
    record["baselines"] = baseline_detail
    mixed_by_quantity = (total_material or occ_material) and not improved
    mixed_by_lead = (
        not improved
        and not mixed_by_quantity
        and (
            (_beats(total) and not _lead_stability(total))
            or (_beats(occ) and not _lead_stability(occ))
        )
    )
    # F-4 (documented unit mix): total.point is a RELATIVE-MAE improvement
    # while occ.point is an ETS DIFFERENCE — different units, deliberately
    # NOT normalized. `pooled` only orders passing candidates for the §12
    # tie-break (and CI overlap then prefers the depth nearest the
    # incumbent), so a cross-unit max is an ordering heuristic, not a
    # reported effect size; the per-endpoint effects in `record` stay in
    # their own units.
    pooled_candidates = [
        e
        for e in (
            total.point if "total" in improved or not improved else None,
            occ.point if "occurrence" in improved else None,
        )
        if e is not None
    ]
    pooled = max(pooled_candidates) if pooled_candidates else total.point
    ci = total.ci if "occurrence" not in improved else occ.ci
    return _CandidateResult(
        key=candidate.key,
        passed=bool(improved) and c4,
        insufficient=False,
        mixed_by_lead=mixed_by_lead,
        mixed_by_quantity=mixed_by_quantity,
        pooled=pooled,
        ci=ci,
        record=record,
    )


def _ci_overlaps(a: tuple[float, float] | None, b: tuple[float, float] | None) -> bool:
    if a is None or b is None:
        return False
    return a[0] <= b[1] and b[0] <= a[1]


def decide_variable(inputs: VariableInputs, *, seed: int, resamples: int) -> Verdict:
    """Run every §12 gate for one variable and produce its verdict.

    Deterministic: each candidate's bootstrap streams derive from ``seed``
    plus a stable per-candidate offset, so identical inputs reproduce the
    verdict bit-for-bit (§18.6).
    """
    results: list[_CandidateResult] = []
    for offset, candidate in enumerate(inputs.candidates):
        candidate_seed = seed + 1000 * (offset + 1)
        if inputs.variable == "precip":
            results.append(
                _decide_precip(candidate, seed=candidate_seed, resamples=resamples)
            )
        else:
            results.append(
                _decide_wind_or_temp(
                    inputs, candidate, seed=candidate_seed, resamples=resamples
                )
            )
    detail: dict[str, object] = {
        "incumbent": inputs.incumbent_key,
        "candidates": {r.key: r.record for r in results},
    }
    passers = [r for r in results if r.passed and r.pooled is not None]
    if passers:
        best = max(passers, key=lambda r: (r.pooled or 0.0, r.key))
        overlapping = [
            r for r in passers if r is not best and _ci_overlaps(best.ci, r.ci)
        ]
        chosen = best
        if overlapping:
            # Material CI overlap: prefer the depth closest to the incumbent;
            # the others are reported statistically unresolved (§12).
            def distance(r: _CandidateResult) -> tuple[float, str]:
                try:
                    return (
                        abs(int(r.key) - int(inputs.incumbent_key)),
                        r.key,
                    )
                except ValueError:
                    return (math.inf, r.key)

            pool = [best, *overlapping]
            chosen = min(pool, key=distance)
            detail["statistically_unresolved"] = [
                r.key for r in pool if r is not chosen
            ]
        detail["tie_break"] = {
            "best_by_pooled": best.key,
            "chosen": chosen.key,
        }
        return Verdict(
            variable=inputs.variable,
            outcome="recommend",
            recommended_key=chosen.key,
            detail=detail,
        )
    if results and all(r.insufficient for r in results):
        outcome = "insufficient_evidence"
    elif any(r.mixed_by_quantity for r in results):
        outcome = "mixed_by_quantity"
    elif any(r.mixed_by_lead for r in results):
        outcome = "mixed_by_lead"
    elif not results:
        outcome = "insufficient_evidence"
    else:
        outcome = "retain_incumbent"
    return Verdict(
        variable=inputs.variable,
        outcome=outcome,
        recommended_key=None,
        detail=detail,
    )
