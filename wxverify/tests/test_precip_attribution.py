"""Oracles for the precip-attribution fix (plan §9): the condition-4 gate
must name the CANDIDATE endpoint, not a REQUIRED BASELINE, when an empty
core came from candidate-side thinness AND the baseline being evaluated was
itself fully supported wherever it had data. Two baselines inside the same
gate call may legitimately receive different reasons (case I).

Fixture spec transcribed verbatim from plan §9.1 — every outcome asserted
below is a property of THESE constructors, executed against the live
decision engine and re-verified at this HEAD (§4.6). All fixture data is
synthetic (invented dates, fake entity key, UTC).

The literal "required candidate endpoint missing or under-supported" is
typed out (not imported) in O1, O4 and O10, so a rename of the module
constant cannot silently keep this suite green.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import cast

import pytest

from tests.helpers import asof_conn, asof_make_site
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.decision import (
    CandidateSeries,
    ContinuousLead,
    OccurrenceLead,
    VariableInputs,
    Verdict,
    decide_variable,
)
from wxverify.verification.engine import finalize_verdicts
from wxverify.verification.methodology import METHODOLOGY_VERSION
from wxverify.verification.runs import (
    capture_config_snapshot,
    input_fingerprint,
    run_config_from_row,
)

# ---------------------------------------------------------------------------
# §9.1 fixture spec — transcribed verbatim
# ---------------------------------------------------------------------------

_DATES = [f"2026-07-{d:02d}" for d in range(1, 25)]  # 24 days > the 20 floor
_LEADS = range(1, 8)
_CONT_BASELINES = ("baseline_persistence", "baseline_all_feed_mean")
_ALL_OCC_BASELINES = (
    "baseline_persistence",
    "baseline_all_feed_mean",
    "baseline_always_dry",
)


def _flat(cand: float, opp: float, days: int = 24) -> ContinuousLead:
    return {d: (cand, opp) for d in _DATES[:days]}


def _occ_series(days: int = 24) -> tuple[OccurrenceLead, OccurrenceLead]:
    """24 days, 10 wet (8 hit + 2 miss) / 14 dry (1 false alarm).

    Copy of tests/test_verification_baseline_gate.py:200-219, plus a `days`
    parameter that truncates ONLY the candidate-vs-incumbent map.
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
    if days < 24:
        keep = set(_DATES[:days])
        vs_incumbent = {d: v for d, v in vs_incumbent.items() if d in keep}
    return vs_incumbent, vs_baseline


def _base_continuous() -> dict[int, ContinuousLead]:
    return {ld: _flat(2.0, 2.0) for ld in _LEADS}


def _base_occurrence() -> dict[int, OccurrenceLead]:
    return {ld: dict(_occ_series()[0]) for ld in _LEADS}


def _base_baseline_continuous() -> dict[str, dict[str, dict[int, ContinuousLead]]]:
    #: `_flat(2.0, 4.0)` is the beaten baseline total (control), never
    #: `_flat(2.0, 1.0)` (the beating total) — that builder is not used here.
    return {
        n: {"precip_total": {ld: _flat(2.0, 4.0) for ld in _LEADS}}
        for n in _CONT_BASELINES
    }


def _base_baseline_occurrence() -> dict[str, dict[int, OccurrenceLead]]:
    return {
        n: {ld: dict(_occ_series()[1]) for ld in _LEADS} for n in _ALL_OCC_BASELINES
    }


def _case_d() -> CandidateSeries:
    """Case D — the healthy control. No delta."""
    return CandidateSeries(
        key="3",
        continuous={"precip_total": _base_continuous()},
        occurrence=_base_occurrence(),
        baseline_continuous=_base_baseline_continuous(),
        baseline_occurrence=_base_baseline_occurrence(),
    )


def _case_a() -> CandidateSeries:
    """Case A — candidate total 10 days/lead. The defect, unfixed."""
    return CandidateSeries(
        key="3",
        continuous={"precip_total": {ld: _flat(2.0, 2.0, 10) for ld in _LEADS}},
        occurrence=_base_occurrence(),
        baseline_continuous=_base_baseline_continuous(),
        baseline_occurrence=_base_baseline_occurrence(),
    )


def _case_b_prime() -> CandidateSeries:
    """Case B' — candidate occurrence 10 days/lead; baselines untouched."""
    return CandidateSeries(
        key="3",
        continuous={"precip_total": _base_continuous()},
        occurrence={ld: dict(_occ_series(10)[0]) for ld in _LEADS},
        baseline_continuous=_base_baseline_continuous(),
        baseline_occurrence=_base_baseline_occurrence(),
    )


def _case_c() -> CandidateSeries:
    """Case C — both A's and B's deltas: both endpoints thin."""
    return CandidateSeries(
        key="3",
        continuous={"precip_total": {ld: _flat(2.0, 2.0, 10) for ld in _LEADS}},
        occurrence={ld: dict(_occ_series(10)[0]) for ld in _LEADS},
        baseline_continuous=_base_baseline_continuous(),
        baseline_occurrence=_base_baseline_occurrence(),
    )


def _case_e() -> CandidateSeries:
    """Case E — `baseline_all_feed_mean` precip_total absent at every lead."""
    baseline_continuous = _base_baseline_continuous()
    baseline_continuous["baseline_all_feed_mean"] = {"precip_total": {}}
    return CandidateSeries(
        key="3",
        continuous={"precip_total": _base_continuous()},
        occurrence=_base_occurrence(),
        baseline_continuous=baseline_continuous,
        baseline_occurrence=_base_baseline_occurrence(),
    )


def _case_f() -> CandidateSeries:
    """Case F — candidate total 10 days/lead at leads 3-7; 1-2 stay 24 days."""
    continuous_leads = {ld: _flat(2.0, 2.0) for ld in (1, 2)}
    continuous_leads.update({ld: _flat(2.0, 2.0, 10) for ld in (3, 4, 5, 6, 7)})
    return CandidateSeries(
        key="3",
        continuous={"precip_total": continuous_leads},
        occurrence=_base_occurrence(),
        baseline_continuous=_base_baseline_continuous(),
        baseline_occurrence=_base_baseline_occurrence(),
    )


def _case_g() -> CandidateSeries:
    """Case G — candidate total thin at 1-3; `all_feed_mean` present only there."""
    continuous_leads = {ld: _flat(2.0, 2.0, 10) for ld in (1, 2, 3)}
    continuous_leads.update({ld: _flat(2.0, 2.0) for ld in (4, 5, 6, 7)})
    baseline_continuous = _base_baseline_continuous()
    baseline_continuous["baseline_all_feed_mean"] = {
        "precip_total": {ld: _flat(2.0, 4.0) for ld in (1, 2, 3)}
    }
    return CandidateSeries(
        key="3",
        continuous={"precip_total": continuous_leads},
        occurrence=_base_occurrence(),
        baseline_continuous=baseline_continuous,
        baseline_occurrence=_base_baseline_occurrence(),
    )


def _case_h() -> CandidateSeries:
    """Case H — A's delta plus both total baselines 10 days/lead too."""
    baseline_continuous = {
        n: {"precip_total": {ld: _flat(2.0, 4.0, 10) for ld in _LEADS}}
        for n in _CONT_BASELINES
    }
    return CandidateSeries(
        key="3",
        continuous={"precip_total": {ld: _flat(2.0, 2.0, 10) for ld in _LEADS}},
        occurrence=_base_occurrence(),
        baseline_continuous=baseline_continuous,
        baseline_occurrence=_base_baseline_occurrence(),
    )


def _case_i() -> CandidateSeries:
    """Case I — A's delta plus `baseline_persistence` thin at leads 1-3 only."""
    baseline_continuous = _base_baseline_continuous()
    persistence_leads = {ld: _flat(2.0, 4.0) for ld in _LEADS}
    persistence_leads.update({ld: _flat(2.0, 4.0, 10) for ld in (1, 2, 3)})
    baseline_continuous["baseline_persistence"] = {"precip_total": persistence_leads}
    return CandidateSeries(
        key="3",
        continuous={"precip_total": {ld: _flat(2.0, 2.0, 10) for ld in _LEADS}},
        occurrence=_base_occurrence(),
        baseline_continuous=baseline_continuous,
        baseline_occurrence=_base_baseline_occurrence(),
    )


def _verdict(candidate: CandidateSeries) -> Verdict:
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(candidate,)
    )
    return decide_variable(inputs, seed=20260814, resamples=200)


def _record(verdict: Verdict, key: str) -> dict[str, object]:
    candidates = cast(dict[str, object], verdict.detail["candidates"])
    return cast(dict[str, object], candidates[key])


def _endpoint(verdict: Verdict, key: str, endpoint: str) -> dict[str, object]:
    return cast(dict[str, object], _record(verdict, key)[endpoint])


def _baseline_entries(verdict: Verdict, key: str, endpoint: str) -> dict[str, object]:
    baselines = cast(dict[str, object], _record(verdict, key)["baselines"])
    return cast(dict[str, object], baselines[endpoint])


def _drops(entry: dict[str, object]) -> list[dict[str, object]]:
    return cast(list[dict[str, object]], entry["dropped_leads"])


def _drop_reasons(entry: dict[str, object]) -> list[str]:
    return [str(d["reason"]) for d in _drops(entry)]


# ---------------------------------------------------------------------------
# O1 — the acceptance oracle: the production write path, same-connection read
# ---------------------------------------------------------------------------


def test_o1_case_a_persisted_row_names_the_candidate_endpoint() -> None:
    verdict = _verdict(_case_a())

    conn = asof_conn()
    site_id = asof_make_site(conn, "oracle-precip-attribution-town")
    ensure_published_generation(conn, site_id)
    snapshot = capture_config_snapshot(conn, site_id)
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    run_id = int(
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint)
            VALUES (?, ?, ?, 'test', 'running', 1, ?, '2026-07-01',
                    '2026-07-24', '2026-07-24', 20260814, 200, ?)
            """,
            (
                site_id,
                int(str(snapshot["tz_generation_id"])),
                METHODOLOGY_VERSION,
                json.dumps(snapshot),
                fingerprint,
            ),
        ).lastrowid
    )
    cfg = run_config_from_row(conn, run_id)

    # `finalize_verdicts` does NOT commit — the verifying SELECT below must
    # run on this SAME connection, or it reads an empty table.
    finalize_verdicts(conn, cfg, [verdict])

    row = conn.execute(
        "SELECT outcome, recommended_depth, tested_family FROM verification_verdicts"
        " WHERE run_id = ? AND variable = 'precip'",
        (run_id,),
    ).fetchone()
    assert row is not None
    assert row["outcome"] == "mixed_by_quantity"
    assert row["recommended_depth"] is None

    tested_family = json.loads(str(row["tested_family"]))
    candidates = cast(dict[str, object], tested_family["candidates"])
    record = cast(dict[str, object], candidates["3"])
    baselines = cast(dict[str, object], record["baselines"])
    total = cast(dict[str, object], baselines["total"])
    for name in _CONT_BASELINES:
        entry = cast(dict[str, object], total[name])
        assert entry["passed"] is False
        assert entry["insufficient"] is True
        assert (
            entry["reason"] == "required candidate endpoint missing or under-supported"
        )
        dropped = cast(list[dict[str, object]], entry["dropped_leads"])
        assert dropped
        assert all(d["reason"] == "outside_core" for d in dropped)


# ---------------------------------------------------------------------------
# O2 — case E: an absent required baseline keeps the baseline reason
# ---------------------------------------------------------------------------


def test_o2_case_e_absent_baseline_keeps_the_baseline_reason() -> None:
    verdict = _verdict(_case_e())
    total = _baseline_entries(verdict, "3", "total")

    for name in _CONT_BASELINES:
        entry = cast(dict[str, object], total[name])
        assert entry["reason"] == "required baseline missing or under-supported"

    total_endpoint = _endpoint(verdict, "3", "total")
    assert _drop_reasons(total_endpoint) and all(
        r == "baseline_absent" for r in _drop_reasons(total_endpoint)
    )

    # This case also pins that condition 2 (the baseline's own drops are all
    # `outside_core`) alone is insufficient: `baseline_persistence` clears
    # it, yet still keeps the baseline reason because condition 1 fails.
    persistence = cast(dict[str, object], total["baseline_persistence"])
    assert _drop_reasons(persistence) and all(
        r == "outside_core" for r in _drop_reasons(persistence)
    )


# ---------------------------------------------------------------------------
# O3 — case C: both endpoints thin, the early return, unchanged
# ---------------------------------------------------------------------------


def test_o3_case_c_both_endpoints_thin_is_insufficient_evidence() -> None:
    verdict = _verdict(_case_c())
    assert verdict.outcome == "insufficient_evidence"
    assert verdict.recommended_key is None
    record = _record(verdict, "3")
    assert "baselines" not in record


# ---------------------------------------------------------------------------
# O4 — case B': the mirrored defect on the occurrence endpoint
# ---------------------------------------------------------------------------


def test_o4_case_b_prime_occurrence_endpoint_carries_the_new_reason() -> None:
    verdict = _verdict(_case_b_prime())
    assert verdict.outcome == "retain_incumbent"

    occurrence = _baseline_entries(verdict, "3", "occurrence")
    for name in _ALL_OCC_BASELINES:
        entry = cast(dict[str, object], occurrence[name])
        assert (
            entry["reason"] == "required candidate endpoint missing or under-supported"
        )
        # Proves the fixture didn't accidentally thin the baselines: a
        # thinned baseline would drop `thin_data`, not `outside_core`.
        assert _drop_reasons(entry) and all(
            r == "outside_core" for r in _drop_reasons(entry)
        )


# ---------------------------------------------------------------------------
# O5 — case F: a thin but non-empty core is not this defect
# ---------------------------------------------------------------------------


def test_o5_case_f_non_empty_core_is_unaffected() -> None:
    verdict = _verdict(_case_f())
    total = _baseline_entries(verdict, "3", "total")
    for name in _CONT_BASELINES:
        entry = cast(dict[str, object], total[name])
        assert entry["insufficient"] is False
        assert "reason" not in entry

    total_endpoint = _endpoint(verdict, "3", "total")
    assert total_endpoint["adequate_leads"] == [1, 2]
    assert verdict.outcome == "recommend"
    assert verdict.recommended_key == "3"


# ---------------------------------------------------------------------------
# O6 — case G: a mixed cause keeps the baseline reason (D3's allowlist)
# ---------------------------------------------------------------------------


def test_o6_case_g_mixed_cause_keeps_the_baseline_reason() -> None:
    verdict = _verdict(_case_g())
    total_endpoint = _endpoint(verdict, "3", "total")
    assert _drop_reasons(total_endpoint) == [
        "thin_data",
        "thin_data",
        "thin_data",
        "baseline_absent",
        "baseline_absent",
        "baseline_absent",
        "baseline_absent",
    ]

    total = _baseline_entries(verdict, "3", "total")
    for name in _CONT_BASELINES:
        entry = cast(dict[str, object], total[name])
        assert entry["reason"] == "required baseline missing or under-supported"


# ---------------------------------------------------------------------------
# O7 — regression: one test parametrised over all nine fixtures, asserting
# the measured invariants of §4.6 (outcome, recommended_key, each endpoint's
# adequate_leads, the conditions block where present) plus, for every gate
# entry across all nine, that the key set other than `reason` is unchanged
# and `passed`/`insufficient` match §4.6.
# ---------------------------------------------------------------------------

_GATE_ENTRY_KEYS = {
    "passed",
    "insufficient",
    "reason",
    "adequate_leads",
    "pooled_point",
    "ci",
    "per_lead",
    "window",
    "dropped_leads",
}

_MATERIAL_CONDITIONS = {
    # occurrence is always material and beats its baselines in these nine
    # fixtures when it has any adequate lead; the candidate total is
    # always the zero-effect series, so it is never material.
    "total_material": False,
    "occurrence_material": True,
    "total_non_inferior": False,
    "occurrence_non_inferior": True,
    "improved_endpoints": [],
    "beats_baselines": False,
}
_RECOMMEND_CONDITIONS = {
    "total_material": False,
    "occurrence_material": True,
    "total_non_inferior": True,
    "occurrence_non_inferior": True,
    "improved_endpoints": ["occurrence"],
    "beats_baselines": True,
}
_RETAIN_CONDITIONS = {
    "total_material": False,
    "occurrence_material": False,
    "total_non_inferior": True,
    "occurrence_non_inferior": False,
    "improved_endpoints": [],
    "beats_baselines": False,
}

_O7_CASES = [
    ("A", _case_a, "mixed_by_quantity", None, [], list(_LEADS), _MATERIAL_CONDITIONS),
    (
        "B_prime",
        _case_b_prime,
        "retain_incumbent",
        None,
        list(_LEADS),
        [],
        _RETAIN_CONDITIONS,
    ),
    ("C", _case_c, "insufficient_evidence", None, [], [], None),
    (
        "D",
        _case_d,
        "recommend",
        "3",
        list(_LEADS),
        list(_LEADS),
        _RECOMMEND_CONDITIONS,
    ),
    ("E", _case_e, "mixed_by_quantity", None, [], list(_LEADS), _MATERIAL_CONDITIONS),
    ("F", _case_f, "recommend", "3", [1, 2], list(_LEADS), _RECOMMEND_CONDITIONS),
    ("G", _case_g, "mixed_by_quantity", None, [], list(_LEADS), _MATERIAL_CONDITIONS),
    ("H", _case_h, "mixed_by_quantity", None, [], list(_LEADS), _MATERIAL_CONDITIONS),
    ("I", _case_i, "mixed_by_quantity", None, [], list(_LEADS), _MATERIAL_CONDITIONS),
]


@pytest.mark.parametrize(
    "builder,outcome,recommended_key,total_adequate,occ_adequate,conditions",
    [c[1:] for c in _O7_CASES],
    ids=[c[0] for c in _O7_CASES],
)
def test_o7_regression_measured_invariants(
    builder: Callable[[], CandidateSeries],
    outcome: str,
    recommended_key: str | None,
    total_adequate: list[int],
    occ_adequate: list[int],
    conditions: dict[str, object] | None,
) -> None:
    verdict = _verdict(builder())
    assert verdict.outcome == outcome
    assert verdict.recommended_key == recommended_key
    assert _endpoint(verdict, "3", "total")["adequate_leads"] == total_adequate
    assert _endpoint(verdict, "3", "occurrence")["adequate_leads"] == occ_adequate

    record = _record(verdict, "3")
    if conditions is None:
        assert "conditions" not in record
        assert "baselines" not in record
        return
    assert record["conditions"] == conditions

    baselines = cast(dict[str, object], record["baselines"])
    for endpoint_name, endpoint_adequate in (
        ("total", total_adequate),
        ("occurrence", occ_adequate),
    ):
        entries = cast(dict[str, object], baselines[endpoint_name])
        expect_insufficient = endpoint_adequate == []
        for raw_entry in entries.values():
            entry = cast(dict[str, object], raw_entry)
            assert entry["insufficient"] is expect_insufficient
            assert entry["passed"] is (not expect_insufficient)
            assert set(entry.keys()) - {"reason"} == _GATE_ENTRY_KEYS - {"reason"}
            if not expect_insufficient:
                assert "reason" not in entry


# ---------------------------------------------------------------------------
# O10 — the over-application guard: the two-condition pair
# ---------------------------------------------------------------------------


def test_o10_case_h_candidate_and_baseline_both_thin_keeps_baseline_reason() -> None:
    """Condition 1 alone would mislabel this: the candidate really is thin,
    but so is every baseline, and the old string is earned by both."""
    verdict = _verdict(_case_h())
    total = _baseline_entries(verdict, "3", "total")
    for name in _CONT_BASELINES:
        entry = cast(dict[str, object], total[name])
        assert entry["reason"] == "required baseline missing or under-supported"
        assert _drop_reasons(entry) and all(
            r == "thin_data" for r in _drop_reasons(entry)
        )


def test_o10_case_i_same_gate_call_yields_two_different_reasons() -> None:
    """The decisive shape: any implementation that resolves the reason ONCE
    per gate call, outside the per-baseline loop, is wrong by construction."""
    verdict = _verdict(_case_i())
    total = _baseline_entries(verdict, "3", "total")

    persistence = cast(dict[str, object], total["baseline_persistence"])
    assert persistence["reason"] == "required baseline missing or under-supported"
    assert set(_drop_reasons(persistence)) == {"outside_core", "thin_data"}

    all_feed_mean = cast(dict[str, object], total["baseline_all_feed_mean"])
    assert (
        all_feed_mean["reason"]
        == "required candidate endpoint missing or under-supported"
    )
    assert _drop_reasons(all_feed_mean) and all(
        r == "outside_core" for r in _drop_reasons(all_feed_mean)
    )
