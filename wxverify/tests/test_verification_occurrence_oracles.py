"""QA oracles for §18.3 (occurrence edge cases) + §17 methodology constants.

Every expected value is hand-derived on paper from the spec formulas —
never by running the code under test. ETS anchors are exact rationals;
the moving-block construction is pinned with a scripted RNG so the block
layout, truncation, and start-bound are asserted literally; the
fail-closed bootstrap CI rule is pinned with seeds whose mixed-resample
counts were derived by an independent replay of the RNG draw protocol
(two uniform draws per resample at n=2, block=1).

All fixture data is synthetic (invented dates, no real stations/models).
"""

from __future__ import annotations

import math
import random
from datetime import date, timedelta

import pytest

from wxverify.verification import methodology
from wxverify.verification.decision import (
    CandidateSeries,
    OccurrenceLead,
    VariableInputs,
    _bootstrap_ci,
    _index_endpoint,
    decide_variable,
)
from wxverify.verification.stats import (
    Contingency,
    classify_occurrence,
    ets,
    moving_block_indices,
    percentile_ci,
)

# ---------------------------------------------------------------------------
# T1/T2/T3 — ETS known values, undefined cases, always-dry baseline (§11/§18.3)
# ---------------------------------------------------------------------------


def test_ets_hand_derived_asymmetric_table() -> None:
    # h=13 m=4 f=6 cn=9, n=32.
    # hits_random = (13+4)(13+6)/32 = 17*19/32 = 323/32.
    # denominator = 13+4+6 - 323/32 = 23 - 323/32 = 413/32.
    # ETS = (13 - 323/32)/(413/32) = (93/32)/(413/32) = 93/413.
    value = ets(Contingency(hits=13, misses=4, false_alarms=6, correct_negatives=9))
    assert value == pytest.approx(93 / 413)


def test_ets_undefined_and_defined_degenerate_tables() -> None:
    # Empty table: n == 0 -> undefined.
    assert ets(Contingency()) is None
    # All-hit: hits_random = 3*3/3 = 3, denominator = 3-3 = 0 -> undefined.
    assert ets(Contingency(hits=3)) is None
    # All-correct-negative: hits_random = 0, denominator = 0 -> undefined.
    assert ets(Contingency(correct_negatives=7)) is None
    # All-miss: hits_random = 5*0/5 = 0, denominator = 5 -> ETS = 0.0 DEFINED.
    assert ets(Contingency(misses=5)) == 0.0
    # All-false-alarm: hits_random = 0*4/4 = 0, denominator = 4 -> 0.0 DEFINED.
    assert ets(Contingency(false_alarms=4)) == 0.0
    # Minimal perfect mixed table: h=1 cn=1 -> hits_random = 1*1/2 = 0.5,
    # denominator = 1 - 0.5 = 0.5, ETS = (1-0.5)/0.5 = 1.0.
    assert ets(Contingency(hits=1, correct_negatives=1)) == pytest.approx(1.0)


def test_always_dry_baseline_ets_is_exactly_zero_on_mixed_days() -> None:
    # §18.3: the always-dry baseline never predicts wet, so its table is
    # misses + correct negatives only. hits_random = (0+m)*(0+0)/n = 0,
    # denominator = m != 0 -> ETS = (0-0)/m = 0.0 exactly (defined).
    table = Contingency()
    for _ in range(8):
        table = table.add(classify_occurrence(False, True))  # observed wet
    for _ in range(17):
        table = table.add(classify_occurrence(False, False))  # observed dry
    assert table == Contingency(misses=8, correct_negatives=17)
    assert ets(table) == 0.0
    # But on an all-dry-observed period even always-dry is undefined
    # (all-one-class): 25 correct negatives -> denominator 0.
    dry = Contingency(correct_negatives=25)
    assert ets(dry) is None


# ---------------------------------------------------------------------------
# T4 — moving-block construction: overlap, truncation, start bound (§12)
# ---------------------------------------------------------------------------


class _ScriptedRng(random.Random):
    """random.Random whose randint returns scripted starts and records bounds."""

    def __init__(self, starts: list[int]) -> None:
        super().__init__(0)
        self._starts = list(starts)
        self.bounds: list[tuple[int, int]] = []

    def randint(self, a: int, b: int) -> int:
        self.bounds.append((a, b))
        return self._starts.pop(0)


def test_moving_block_exact_layout_truncation_and_start_bound() -> None:
    # n=10, block=3: starts_hi must be 10-3 = 7 (inclusive). Scripted starts
    # [0, 7, 7, 2] produce blocks [0,1,2] [7,8,9] [7,8,9] [2,3,4] = 12
    # indices, truncated to exactly n=10.
    rng = _ScriptedRng([0, 7, 7, 2])
    out = moving_block_indices(rng, 10, 3)
    assert out == [0, 1, 2, 7, 8, 9, 7, 8, 9, 2]
    assert rng.bounds == [(0, 7)] * 4


def test_moving_block_short_series_truncates_single_block() -> None:
    # n=2 < block=3: block length falls back to n=2, starts_hi = 0.
    rng = _ScriptedRng([0])
    assert moving_block_indices(rng, 2, 3) == [0, 1]
    assert rng.bounds == [(0, 0)]


def test_moving_block_length_one_draws_each_index_independently() -> None:
    # block=1: every draw is one index, bounds (0, n-1).
    rng = _ScriptedRng([4, 4, 0, 2, 1])
    assert moving_block_indices(rng, 5, 1) == [4, 4, 0, 2, 1]
    assert rng.bounds == [(0, 4)] * 5


# ---------------------------------------------------------------------------
# T5 — percentile CI linear interpolation (§12)
# ---------------------------------------------------------------------------


def test_percentile_ci_linear_interpolation_hand_values() -> None:
    # 4 samples, level 0.5: q=0.25 -> position 0.75 -> 10*0.25 + 20*0.75 = 17.5
    #                       q=0.75 -> position 2.25 -> 30*0.75 + 40*0.25 = 32.5
    assert percentile_ci([10.0, 20.0, 30.0, 40.0], 0.5) == pytest.approx((17.5, 32.5))
    # 5 samples, level 0.9: q=0.05 -> position 0.2 -> 1*0.8 + 2*0.2 = 1.2
    #                       q=0.95 -> position 3.8 -> 4*0.2 + 5*0.8 = 4.8
    assert percentile_ci([1.0, 2.0, 3.0, 4.0, 5.0], 0.9) == pytest.approx((1.2, 4.8))
    # Single sample: both bounds are the sample itself.
    assert percentile_ci([7.5], 0.99) == (7.5, 7.5)


# ---------------------------------------------------------------------------
# T6 — fail-closed bootstrap CI: undefined resamples are dropped; the CI is
# undefined when fewer than half the resamples survive (§12/§18.3)
# ---------------------------------------------------------------------------

# Two dates; one carries (hit, miss), the other (cn, cn). A resample that
# draws BOTH copies of one date builds an all-one-class table on at least
# one side -> ETS None -> the draw is dropped. A mixed resample gives
# cand {h=1, cn=1} -> ETS 1.0 and opp {m=1, cn=1} -> ETS 0.0 -> diff 1.0.
_FAIL_CLOSED_SERIES: dict[int, dict[str, tuple[str, str]]] = {
    1: {
        "2026-07-01": ("hit", "miss"),
        "2026-07-02": ("correct_negative", "correct_negative"),
    }
}


def _mixed_count(seed: int, resamples: int) -> int:
    """Independent replay of the resample draw protocol at n=2, block=1.

    Each resample consumes exactly two uniform draws on [0, 1] (one block
    of length 1 per index); the resample is 'mixed' iff they differ.
    Derived from the §12 moving-block construction, not from the code
    under test.
    """
    rng = random.Random(seed)
    mixed = 0
    for _ in range(resamples):
        a = rng.randint(0, 1)
        b = rng.randint(0, 1)
        mixed += int(a != b)
    return mixed


def test_bootstrap_ci_fail_closed_when_under_half_the_draws_survive() -> None:
    endpoint = _index_endpoint("occurrence", _FAIL_CLOSED_SERIES)
    # seed=2 at 101 resamples: 44 mixed draws survive. 44 < 101/2 = 50.5,
    # so the CI must be undefined — but 44 > 101/4 = 25.25, so a mutant
    # that relaxes the rule to a quarter would wrongly define it.
    assert _mixed_count(2, 101) == 44
    assert 101 / 4 < 44 < 101 / 2
    ci = _bootstrap_ci(endpoint, (1,), level=0.95, seed=2, resamples=101, block=1)
    assert ci is None


def test_bootstrap_ci_defined_when_half_the_draws_survive() -> None:
    endpoint = _index_endpoint("occurrence", _FAIL_CLOSED_SERIES)
    # seed=1 at 101 resamples: 58 mixed draws survive (>= 50.5). Every
    # surviving draw's effect is exactly 1.0 - 0.0 = 1.0, so the
    # percentile CI is exactly (1.0, 1.0).
    assert _mixed_count(1, 101) == 58
    assert 58 >= 101 / 2
    ci = _bootstrap_ci(endpoint, (1,), level=0.95, seed=1, resamples=101, block=1)
    assert ci == (1.0, 1.0)


# ---------------------------------------------------------------------------
# T7 — occurrence minimum-event adequacy boundaries (§12/§18.3): >= 8 wet
# AND >= 8 dry observed days on the common core, counted on the candidate
# labels' observed side (hit/miss = observed wet).
# ---------------------------------------------------------------------------

_OCC_DATES = [f"2026-07-{d:02d}" for d in range(1, 26)]  # 25 days


def _occ_series(
    hits: int, misses: int, false_alarms: int
) -> tuple[OccurrenceLead, OccurrenceLead]:
    """(candidate-vs-incumbent, candidate-vs-always-dry) series on 25 days.

    Wet days (hits then misses) are interleaved with dry days so block
    resamples mix classes. Opponent (incumbent) misses every wet day and
    false-alarms on min(6, dry) dry days — clearly worse than the
    candidate. Always-dry: miss on wet days, correct_negative on dry.
    """
    wet_total = hits + misses
    # Spread wet days: every 3rd index until wet_total placed, then fill.
    wet_idx: list[int] = []
    step_positions = list(range(0, 25, 3)) + [i for i in range(25) if i % 3 != 0]
    for i in step_positions:
        if len(wet_idx) == wet_total:
            break
        wet_idx.append(i)
    wet_set = set(wet_idx)
    dry_idx = [i for i in range(25) if i not in wet_set]
    cand_label: dict[int, str] = {}
    for k, i in enumerate(sorted(wet_idx)):
        cand_label[i] = "hit" if k < hits else "miss"
    for k, i in enumerate(dry_idx):
        cand_label[i] = "false_alarm" if k < false_alarms else "correct_negative"
    opp_fa = set(dry_idx[: min(6, len(dry_idx))])
    vs_incumbent: OccurrenceLead = {}
    vs_always_dry: OccurrenceLead = {}
    for i, d in enumerate(_OCC_DATES):
        cand = cand_label[i]
        if i in wet_set:
            opp = "miss"
            base = "miss"
        else:
            opp = "false_alarm" if i in opp_fa else "correct_negative"
            base = "correct_negative"
        vs_incumbent[d] = (cand, opp)
        vs_always_dry[d] = (cand, base)
    return vs_incumbent, vs_always_dry


@pytest.mark.parametrize(
    ("hits", "misses", "false_alarms", "expected"),
    [
        # 8 wet (6h+2m), 17 dry: both minimums met -> recommend.
        (6, 2, 1, "recommend"),
        # 7 wet (6h+1m), 18 dry: wet below the 8 floor -> insufficient.
        # A mutant counting hit+false_alarm as 'wet' would see 6+2=8 here
        # (false_alarms=2) and wrongly recommend.
        (6, 1, 2, "insufficient_evidence"),
        # 17 wet (15h+2m), 8 dry: dry exactly at the floor -> recommend.
        (15, 2, 1, "recommend"),
        # 18 wet (16h+2m), 7 dry: dry below the floor -> insufficient.
        (16, 2, 1, "insufficient_evidence"),
    ],
)
def test_occurrence_min_event_boundaries(
    hits: int, misses: int, false_alarms: int, expected: str
) -> None:
    vs_incumbent, vs_always_dry = _occ_series(hits, misses, false_alarms)
    candidate = CandidateSeries(
        key="3",
        occurrence={lead: dict(vs_incumbent) for lead in range(1, 8)},
        baseline_occurrence={
            "baseline_always_dry": {lead: dict(vs_always_dry) for lead in range(1, 8)}
        },
    )
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(candidate,)
    )
    verdict = decide_variable(inputs, seed=20260713, resamples=400)
    assert verdict.outcome == expected
    if expected == "recommend":
        assert verdict.recommended_key == "3"


def test_occurrence_recommend_case_ets_point_is_hand_derived() -> None:
    # 25 days, wet=8 (6h+2m), dry=17 (1fa+16cn):
    #   cand: h=6 m=2 f=1 cn=16 -> hits_random = 8*7/25 = 2.24,
    #         denominator = 9 - 2.24 = 6.76, ETS = 3.76/6.76 = 94/169.
    #   opp:  h=0 m=8 f=6 cn=11 -> hits_random = 8*6/25 = 1.92,
    #         denominator = 14 - 1.92 = 12.08, ETS = -1.92/12.08 = -24/151.
    #   pooled ETS diff (identical on every lead) = 94/169 + 24/151.
    vs_incumbent, vs_always_dry = _occ_series(6, 2, 1)
    candidate = CandidateSeries(
        key="3",
        occurrence={lead: dict(vs_incumbent) for lead in range(1, 8)},
        baseline_occurrence={
            "baseline_always_dry": {lead: dict(vs_always_dry) for lead in range(1, 8)}
        },
    )
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(candidate,)
    )
    verdict = decide_variable(inputs, seed=20260713, resamples=400)
    record = verdict.detail["candidates"]["3"]  # type: ignore[index]
    occ = record["occurrence"]  # type: ignore[index]
    assert occ["adequate_leads"] == list(range(1, 8))  # type: ignore[index]
    assert occ["pooled_point"] == pytest.approx(94 / 169 + 24 / 151)  # type: ignore[index]


# ---------------------------------------------------------------------------
# T9 — the Bonferroni-adjusted level (1 - 0.05/6) governs precip
# materiality: an effect that a plain 95% CI would call material must NOT
# clear the wider per-quantity CI (§12/§17)
# ---------------------------------------------------------------------------


def _bonferroni_series() -> tuple[OccurrenceLead, OccurrenceLead]:
    """40 synthetic days, 16 wet / 24 dry, hand-derived tables.

    cand: h=13 m=3 f=3 cn=21 -> hits_random = 16*16/40 = 6.4,
          ETS = (13 - 6.4)/(19 - 6.4) = 6.6/12.6 = 11/21.
    opp:  h=10 m=6 f=5 cn=19 -> hits_random = 16*15/40 = 6,
          ETS = (10 - 6)/(21 - 6) = 4/15.
    pooled diff = 11/21 - 4/15 = 27/105 = 9/35 (> the 0.05 floor).
    The error days are spread so date-resampling makes the diff noisy:
    at decide seed 35 the Bonferroni CI straddles zero while a plain 95%
    CI excludes it (premises re-asserted in the test).
    """
    start = date(2026, 7, 1)
    dates = [(start + timedelta(days=i)).isoformat() for i in range(40)]
    cand_miss = {3, 7, 11}
    opp_miss = {1, 3, 5, 9, 13, 15}
    cand_fa = {16, 24, 32}
    opp_fa = {17, 21, 25, 29, 33}
    vs_incumbent: OccurrenceLead = {}
    vs_always_dry: OccurrenceLead = {}
    for i, d in enumerate(dates):
        if i < 16:  # wet day
            cand = "miss" if i in cand_miss else "hit"
            opp = "miss" if i in opp_miss else "hit"
            base = "miss"
        else:  # dry day
            cand = "false_alarm" if i in cand_fa else "correct_negative"
            opp = "false_alarm" if i in opp_fa else "correct_negative"
            base = "correct_negative"
        vs_incumbent[d] = (cand, opp)
        vs_always_dry[d] = (cand, base)
    return vs_incumbent, vs_always_dry


def test_bonferroni_level_governs_occurrence_materiality() -> None:
    vs_incumbent, vs_always_dry = _bonferroni_series()
    leads = (1, 2, 3, 4)
    candidate = CandidateSeries(
        key="3",
        occurrence={lead: dict(vs_incumbent) for lead in leads},
        baseline_occurrence={
            "baseline_always_dry": {lead: dict(vs_always_dry) for lead in leads}
        },
    )
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(candidate,)
    )
    verdict = decide_variable(inputs, seed=35, resamples=400)
    record = verdict.detail["candidates"]["3"]  # type: ignore[index]
    occ = record["occurrence"]  # type: ignore[index]
    assert occ["pooled_point"] == pytest.approx(9 / 35)  # type: ignore[index]
    ci = occ["ci"]  # type: ignore[index]
    assert isinstance(ci, list)
    # Premise: the Bonferroni-wide CI straddles zero -> NOT material.
    assert ci[0] <= 0 < ci[1]
    conditions = record["conditions"]  # type: ignore[index]
    assert conditions["occurrence_material"] is False  # type: ignore[index]
    assert conditions["improved_endpoints"] == []  # type: ignore[index]
    assert verdict.outcome == "retain_incumbent"
    # Discriminating control: the SAME endpoint/seed at a plain 95% level
    # WOULD exclude zero — so a mutant that narrows the per-quantity level
    # to 0.95 flips both the ci assertion above and the outcome. The seed
    # mirrors decide_variable's per-candidate derivation (seed + 1000).
    endpoint = _index_endpoint(
        "occurrence",
        {ld: {d: (c, o) for d, (c, o) in vs_incumbent.items()} for ld in leads},
    )
    narrow = _bootstrap_ci(endpoint, leads, level=0.95, seed=35 + 1000, resamples=400)
    assert narrow is not None
    assert narrow[0] > 0


# ---------------------------------------------------------------------------
# T8 — §17 methodology constants pinned verbatim
# ---------------------------------------------------------------------------


def test_methodology_constants_match_spec_table() -> None:
    assert methodology.METHODOLOGY_VERSION == 1
    assert methodology.ADEQUATE_LEAD_MIN_DAYS == 20
    assert methodology.MIN_ADEQUATE_LEADS_PER_VARIABLE == 4
    assert methodology.BOOTSTRAP_BLOCK_LENGTH_DAYS == 3
    assert methodology.BOOTSTRAP_RESAMPLES == 10_000
    assert methodology.CANDIDATE_CI_LEVEL == 1 - 0.05 / 3
    assert pytest.approx(0.9833333, abs=1e-6) == methodology.CANDIDATE_CI_LEVEL
    assert methodology.PRECIP_IMPROVEMENT_CI_LEVEL == 1 - 0.05 / 6
    assert pytest.approx(0.9916667, abs=1e-6) == methodology.PRECIP_IMPROVEMENT_CI_LEVEL
    assert methodology.BASELINE_GATE_CI_LEVEL == 0.95
    assert methodology.LEAD_STABILITY_NUMERATOR == 2
    assert methodology.LEAD_STABILITY_DENOMINATOR == 3
    assert math.ceil(7 * 2 / 3) == 5  # sanity: the O4 agreement threshold
    assert methodology.PRACTICAL_FLOOR_RELATIVE_MAE == 0.05
    assert methodology.PRACTICAL_FLOOR_ETS == 0.05
    assert methodology.NON_INFERIORITY_MAE_MARGIN == 0.02
    assert methodology.NON_INFERIORITY_ETS_MARGIN == 0.02
    assert methodology.OCCURRENCE_MIN_WET_DAYS == 8
    assert methodology.OCCURRENCE_MIN_DRY_DAYS == 8
    assert methodology.ROSTER_AVAILABILITY_FLOOR == 0.70
