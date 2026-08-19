"""QA oracles for the strict-common-core / pairwise-core split (plan §8,
oracles O1-O14; O15 lives here too as the request-level companion).

Construction discipline mirrors ``test_verification_outcome_oracles.py``:
synthetic dates, fake feed/depth ids, constant-ratio series wherever a hand-
exact pooled effect is needed, and a two-segment split (a candidate-only
range plus a shared range) wherever a test must show a value that differs
between a candidate's own window and a shared, intersected basis. All
fixture data is synthetic (invented dates, RFC-5737-style ids, UTC).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import cast

import pytest

from tests.helpers import asof_conn, continuous_baseline_set, occurrence_baseline_set
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.contract import CONTRACT, VERIFICATION_SCHEMA
from wxverify.verification.decision import (
    DECISION_LEADS,
    CandidateSeries,
    ContinuousLead,
    OccurrenceLead,
    VariableInputs,
    Verdict,
    _adequate_leads,  # noqa: SLF001
    _decide_precip,  # noqa: SLF001
    _endpoint_window,  # noqa: SLF001
    _index_endpoint,  # noqa: SLF001
    decide_variable,
)
from wxverify.verification.engine import (
    _load_cell,  # noqa: SLF001
    _resolve_cell,  # noqa: SLF001
    aggregate_run,
    prepare_bootstrap_inputs,
)
from wxverify.verification.methodology import (
    ADEQUATE_LEAD_MIN_DAYS,
    METHODOLOGY_VERSION,
)
from wxverify.verification.runs import (
    RosterFeed,
    RunConfig,
    capture_config_snapshot,
    input_fingerprint,
    publish_run,
)

# ---------------------------------------------------------------------------
# Shared construction helpers
# ---------------------------------------------------------------------------


def _dates_n(n: int, *, start: date = date(2026, 7, 1)) -> list[str]:
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
        baseline_continuous=continuous_baseline_set("wind_max", base),
    )


def _record(verdict: Verdict, key: str) -> dict[str, object]:
    candidates = cast(dict[str, object], verdict.detail["candidates"])
    return cast(dict[str, object], candidates[key])


def _headline(verdict: Verdict, key: str) -> dict[str, object]:
    return cast(dict[str, object], _record(verdict, key)["headline"])


def _insert_evidence(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    day: str,
    lead: int,
    variable: str,
    quantity: str,
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        """,
        (
            run_id,
            day,
            day,
            lead,
            variable,
            quantity,
            entity_type,
            entity_key,
            predicted,
            1 if eligible else 0,
            truth,
            abs_error,
        ),
    )


def _insert_run_row(conn: sqlite3.Connection, site_id: int, generation: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, period_start, period_end,
             settled_through, bootstrap_seed, bootstrap_resamples,
             input_fingerprint, created_at)
        VALUES (?, ?, 2, 'test', 'running', 1, '{}', '2026-07-01',
                '2026-09-30', '2026-09-30', 1, 10, 'f' || ?,
                '2026-07-05T12:00:00Z')
        """,
        (site_id, generation, site_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _make_run() -> tuple[sqlite3.Connection, int, int]:
    conn = asof_conn()
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                           timezone)
        VALUES ('oracle-town', 40.0, -105.0, 900.0, 'UTC')
        """
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    generation = ensure_published_generation(conn, site_id)
    run_id = _insert_run_row(conn, site_id, generation)
    return conn, site_id, run_id


def _base_cfg(*, run_id: int, site_id: int, generation: int) -> RunConfig:
    return RunConfig(
        site_id=site_id,
        run_id=run_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        wall_clock="wc",
        blend_depth=2,
        blend_depths={"temperature": 2, "wind": 2, "precip": 2},
        min_n=30,
        window_days=90,
        tz_generation_id=generation,
        roster=(),
        period_start="2026-07-01",
        period_end="2026-09-30",
        bootstrap_seed=1,
        bootstrap_resamples=20,
    )


# ---------------------------------------------------------------------------
# O1 -- total accounting, globally: every endpoint record accounts for every
# DECISION_LEAD exactly once, split between adequate_leads and dropped_leads.
# ---------------------------------------------------------------------------


def _assert_total_accounting(endpoint_record: dict[str, object]) -> None:
    adequate = cast(list[int], endpoint_record["adequate_leads"])
    dropped = cast(list[dict[str, object]], endpoint_record["dropped_leads"])
    dropped_leads = [cast(int, d["lead"]) for d in dropped]
    combined = sorted(adequate + dropped_leads)
    assert combined == list(DECISION_LEADS)
    assert len(combined) == len(set(combined))


def test_o1_total_accounting_sweep_over_fixture_corpus() -> None:
    # Fixture 1: a wind candidate defined on leads 1-6 only (lead 7 is a
    # pure thin_data gap) -- exercises the boundary the D4 fix targets.
    dates = _dates_n(24)
    cand = _wind_candidate("3", {ld: _ratio_series(dates, 0.5) for ld in range(1, 7)})
    inputs = VariableInputs(variable="wind", incumbent_key="2", candidates=(cand,))
    verdict = decide_variable(inputs, seed=1, resamples=20)
    record = _record(verdict, "3")
    _assert_total_accounting(cast(dict[str, object], record["headline"]))
    for baseline_detail in (
        cast(dict[str, object], record["baselines"]).values()
        if "baselines" in record
        else ()
    ):
        _assert_total_accounting(cast(dict[str, object], baseline_detail))

    # Fixture 2: two passers -- baselines are always present here, so their
    # accounting is exercised too.
    cand4 = _wind_candidate("4", {ld: _ratio_series(dates, 0.5) for ld in range(1, 8)})
    cand3 = _wind_candidate("3", {ld: _ratio_series(dates, 0.9) for ld in range(1, 8)})
    verdict2 = decide_variable(
        VariableInputs(variable="wind", incumbent_key="2", candidates=(cand3, cand4)),
        seed=20260701,
        resamples=40,
    )
    for key in ("3", "4"):
        record2 = _record(verdict2, key)
        _assert_total_accounting(cast(dict[str, object], record2["headline"]))
        baselines2 = cast(dict[str, object], record2["baselines"])
        assert baselines2, "two clear passers must reach the baseline gate"
        for baseline_detail in baselines2.values():
            _assert_total_accounting(cast(dict[str, object], baseline_detail))


# ---------------------------------------------------------------------------
# O2 -- the zero-day record itself: an entirely absent lead still emits a
# named {"lead": N, "reason": "thin_data", "days": 0} record, verbatim.
# ---------------------------------------------------------------------------


def test_o2_zero_day_leads_emit_explicit_days_zero_record() -> None:
    dates = _dates_n(24)
    cand = _wind_candidate("3", {ld: _ratio_series(dates, 0.5) for ld in range(1, 5)})
    inputs = VariableInputs(variable="wind", incumbent_key="2", candidates=(cand,))
    verdict = decide_variable(inputs, seed=1, resamples=20)
    dropped = cast(list[dict[str, object]], _headline(verdict, "3")["dropped_leads"])
    for lead in (5, 6, 7):
        assert {"lead": lead, "reason": "thin_data", "days": 0} in dropped


# ---------------------------------------------------------------------------
# O10 -- window content, exactly.
# ---------------------------------------------------------------------------


def test_o10_window_content_exact_distinct_dates_and_adequate_only() -> None:
    dates_a = _dates_n(ADEQUATE_LEAD_MIN_DAYS, start=date(2026, 7, 1))
    dates_b = _dates_n(ADEQUATE_LEAD_MIN_DAYS, start=date(2026, 7, 1))  # same dates
    thin_dates = _dates_n(5, start=date(2026, 9, 1))  # outside the range, too few
    endpoint = _index_endpoint(
        "continuous",
        {
            1: {d: (1.0, 2.0) for d in dates_a},
            2: {d: (1.0, 2.0) for d in dates_b},
            3: {d: (1.0, 2.0) for d in thin_dates},
        },
    )
    adequate, _dropped = _adequate_leads(endpoint, occurrence=False)
    assert adequate == (1, 2)  # lead 3 is thin_data (5 < 20), so dropped
    window = _endpoint_window(endpoint, adequate)
    assert window["first"] == dates_a[0]
    assert window["last"] == dates_a[-1]
    # Leads 1 and 2 share every date -> the union is NOT double the count.
    assert window["days"] == ADEQUATE_LEAD_MIN_DAYS
    per_lead = cast(dict[str, object], window["per_lead"])
    assert set(per_lead) == {"1", "2"}
    for lead_key in ("1", "2"):
        block = cast(dict[str, object], per_lead[lead_key])
        assert block == {
            "first": dates_a[0],
            "last": dates_a[-1],
            "days": ADEQUATE_LEAD_MIN_DAYS,
        }
    # The dropped lead's (Sept) dates never move the range: adding a thin
    # lead with dates far outside 1-20 must not widen "last".
    assert window["last"] != thin_dates[-1]


# ---------------------------------------------------------------------------
# O11 -- empty window is a present block, not an absent key.
# ---------------------------------------------------------------------------


def test_o11_empty_window_is_a_present_block() -> None:
    endpoint = _index_endpoint("continuous", {1: {d: (1.0, 2.0) for d in _dates_n(3)}})
    adequate, _dropped = _adequate_leads(endpoint, occurrence=False)
    assert adequate == ()  # only 3 days -- below the floor
    window = _endpoint_window(endpoint, adequate)
    assert window == {"first": None, "last": None, "days": 0, "per_lead": {}}


# ---------------------------------------------------------------------------
# O12 -- contract text and core map.
# ---------------------------------------------------------------------------


def test_o12_contract_text_and_core_map() -> None:
    sample_definition = str(CONTRACT["sample_definition"])
    assert "pairwise" in sample_definition
    assert "strict common core" in sample_definition
    assert CONTRACT["comparison_core"] == {
        "decision": "pairwise",
        "headline_table": "strict_common",
        "non_headline_rows": "pairwise",
    }


# ---------------------------------------------------------------------------
# O13 -- both counters moved together.
# ---------------------------------------------------------------------------


def test_o13_methodology_and_schema_versions_moved_together() -> None:
    # This change (pairwise decision cores) bumped both counters together;
    # a mutant reverting either one in isolation fails this assertion.
    assert (METHODOLOGY_VERSION, VERIFICATION_SCHEMA) == (2, 2)


# ---------------------------------------------------------------------------
# O14 -- fingerprint sensitivity to the methodology version.
# ---------------------------------------------------------------------------


def test_o14_fingerprint_sensitive_to_methodology_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from wxverify.verification import runs as runs_module

    conn, site_id, _run_id = _make_run()
    generation = ensure_published_generation(conn, site_id)
    snapshot: dict[str, object] = {
        "timezone": "UTC",
        "rain_threshold_mm": 0.2,
        "wall_clock": "wc",
        "blend_depth": 2,
        "blend_depths": {"temperature": 2, "wind": 2, "precip": 2},
        "min_n": 30,
        "window_days": 90,
        "tz_generation_id": generation,
        "roster": [],
    }
    monkeypatch.setattr(runs_module, "METHODOLOGY_VERSION", 1)
    fp_v1 = runs_module.input_fingerprint(conn, site_id, snapshot)
    monkeypatch.setattr(runs_module, "METHODOLOGY_VERSION", 2)
    fp_v2 = runs_module.input_fingerprint(conn, site_id, snapshot)
    assert fp_v1 != fp_v2


# ---------------------------------------------------------------------------
# O3/O4/O5 -- the complementary-window construction (run 1's wind lead 3,
# reduced to a fixture): one depth eligible only on the first half of the
# period, another only on the second half, so the STRICT common core is
# empty while the PAIRWISE core (candidate vs incumbent) is the full period.
# The three oracles share one fixture: O3 proves the core split itself, O4
# proves the baseline endpoint reads its OWN pairwise pair (not the
# incumbent's dates), and O5 proves a baseline gap at one lead survives as
# a named, lockstepped drop rather than a silent re-admission.
# ---------------------------------------------------------------------------

_CW_DATES = _dates_n(48)
_CW_FIRST_HALF = _CW_DATES[:24]
_CW_SECOND_HALF = _CW_DATES[24:]
_CW_LEADS = (1, 2, 3, 4, 5)
_CW_BASELINE_MISSING_LEAD = 2


def _build_complementary_window_run() -> tuple[sqlite3.Connection, RunConfig]:
    """depth 1 on days 1-24 only, depth 3 on days 25-48 only, depth 2
    (incumbent) and depth 4 (candidate) on all 48 -- at every lead in
    ``_CW_LEADS``. ``baseline_persistence`` is present on days 1-24 at every
    lead EXCEPT ``_CW_BASELINE_MISSING_LEAD``, where it is entirely absent.
    ``baseline_all_feed_mean`` is present on all 48 days at every lead, so
    the required-baseline SET stays satisfiable and the candidate can reach
    a full verdict.
    """
    conn, site_id, run_id = _make_run()
    for lead in _CW_LEADS:
        for day in _CW_FIRST_HALF:
            _insert_evidence(
                conn,
                run_id,
                day=day,
                lead=lead,
                variable="wind",
                quantity="wind_max",
                entity_type="depth",
                entity_key="1",
                predicted=15.0,
            )
        for day in _CW_SECOND_HALF:
            _insert_evidence(
                conn,
                run_id,
                day=day,
                lead=lead,
                variable="wind",
                quantity="wind_max",
                entity_type="depth",
                entity_key="3",
                predicted=9.0,
            )
        for day in _CW_DATES:
            _insert_evidence(
                conn,
                run_id,
                day=day,
                lead=lead,
                variable="wind",
                quantity="wind_max",
                entity_type="depth",
                entity_key="2",
                predicted=13.0,
            )
            _insert_evidence(
                conn,
                run_id,
                day=day,
                lead=lead,
                variable="wind",
                quantity="wind_max",
                entity_type="depth",
                entity_key="4",
                predicted=11.0,
            )
            _insert_evidence(
                conn,
                run_id,
                day=day,
                lead=lead,
                variable="wind",
                quantity="wind_max",
                entity_type="baseline_all_feed_mean",
                entity_key="all_feed_mean",
                predicted=8.0,
            )
        if lead != _CW_BASELINE_MISSING_LEAD:
            for day in _CW_FIRST_HALF:
                _insert_evidence(
                    conn,
                    run_id,
                    day=day,
                    lead=lead,
                    variable="wind",
                    quantity="wind_max",
                    entity_type="baseline_persistence",
                    entity_key="persistence",
                    predicted=20.0,
                )
    conn.commit()
    generation = ensure_published_generation(conn, site_id)
    cfg = _base_cfg(run_id=run_id, site_id=site_id, generation=generation)
    return conn, cfg


def test_o3_complementary_window_construction_reproduces_the_defect() -> None:
    conn, cfg = _build_complementary_window_run()
    cell = _load_cell(conn, cfg.run_id, "wind", 1, "wind_max")
    members, common_core, _availability = _resolve_cell(cell, cfg, "wind_max")
    # (a) the strict common core is empty: depth 1 (days 1-24) and depth 3
    # (days 25-48) are both unconditional members with disjoint coverage.
    assert ("depth", "1") in members
    assert ("depth", "3") in members
    assert common_core == []

    inputs = prepare_bootstrap_inputs(conn, cfg)
    wind_inputs = next(i for i in inputs if i.variable == "wind")
    candidate4 = next(c for c in wind_inputs.candidates if c.key == "4")
    series_at_lead1 = candidate4.continuous["wind_max"][1]
    # (b) the candidate-vs-incumbent pairwise series spans the full 48 days,
    # unbroken by depth 1 or depth 3's coverage gaps.
    assert len(series_at_lead1) == 48
    assert set(series_at_lead1) == set(_CW_DATES)

    # (c) the lead reaches adequate_leads on that series.
    endpoint = _index_endpoint("continuous", {1: series_at_lead1})
    adequate, _dropped = _adequate_leads(endpoint, occurrence=False)
    assert adequate == (1,)


def test_o4_baseline_gate_reads_its_own_pairwise_pair() -> None:
    conn, cfg = _build_complementary_window_run()
    inputs = prepare_bootstrap_inputs(conn, cfg)
    wind_inputs = next(i for i in inputs if i.variable == "wind")
    candidate4 = next(c for c in wind_inputs.candidates if c.key == "4")
    verdict = decide_variable(
        VariableInputs(variable="wind", incumbent_key="2", candidates=(candidate4,)),
        seed=1,
        resamples=20,
    )
    record = _record(verdict, "4")
    headline = cast(dict[str, object], record["headline"])
    baselines = cast(dict[str, object], record["baselines"])
    headline_window = cast(dict[str, object], headline["window"])
    persistence_window = cast(
        dict[str, object],
        cast(dict[str, object], baselines["baseline_persistence"])["window"],
    )
    # Both windows appear in the SAME verdict record.
    assert headline_window["days"] == 48
    # A copy-paste that handed the baseline series the incumbent's (48-day)
    # dates instead of its OWN pairwise pair with the candidate would make
    # this 48 too; the fixture's baseline is eligible on only the first
    # half, so a correct implementation reports 24.
    assert persistence_window["days"] == 24


def test_o5_lockstep_survives_a_baseline_gap_at_one_lead() -> None:
    conn, cfg = _build_complementary_window_run()
    inputs = prepare_bootstrap_inputs(conn, cfg)
    wind_inputs = next(i for i in inputs if i.variable == "wind")
    candidate4 = next(c for c in wind_inputs.candidates if c.key == "4")
    verdict = decide_variable(
        VariableInputs(variable="wind", incumbent_key="2", candidates=(candidate4,)),
        seed=1,
        resamples=20,
    )
    record = _record(verdict, "4")
    headline = cast(dict[str, object], record["headline"])
    dropped = cast(list[dict[str, object]], headline["dropped_leads"])
    drop_for_missing_lead = next(
        d for d in dropped if d["lead"] == _CW_BASELINE_MISSING_LEAD
    )
    assert drop_for_missing_lead["reason"] == "baseline_absent"
    assert "baseline_persistence" in cast(
        list[object], drop_for_missing_lead["missing_baselines"]
    )
    assert _CW_BASELINE_MISSING_LEAD not in cast(list[int], headline["adequate_leads"])
    # The lockstep drop is NOT re-reported as a per-baseline shortfall: the
    # baseline gate only ever sees the already-reduced core, so no detail
    # block should claim the baseline was missing on a core lead.
    baselines = cast(dict[str, object], record["baselines"])
    for baseline_detail in baselines.values():
        assert "missing_leads" not in cast(dict[str, object], baseline_detail)


def test_o6_headline_table_is_the_unchanged_strict_common_core() -> None:
    """Golden snapshot: aggregate_run's headline table and aggregate_state
    common_dates are still computed on the strict common core, hand-derived,
    exactly as they were before pairwise decision cores existed -- proving
    D1 did not "helpfully" move the descriptive table too.
    """
    conn, site_id, run_id = _make_run()
    days = _dates_n(4)
    depth_predictions = {
        "1": [13.0, 13.0, 13.0, 13.0],
        "2": [12.0, 6.0, 16.0, 18.0],
        "3": [11.0, 8.0, 13.0, 14.0],  # errors +1, -2, +3, +4
        "4": [15.0, 15.0, 15.0, 15.0],
    }
    for key, values in depth_predictions.items():
        for day, predicted in zip(days, values, strict=True):
            _insert_evidence(
                conn,
                run_id,
                day=day,
                lead=1,
                variable="wind",
                quantity="wind_max",
                entity_type="depth",
                entity_key=key,
                predicted=predicted,
            )
    for baseline, key, predicted in (
        ("baseline_persistence", "persistence", 14.0),
        ("baseline_all_feed_mean", "all_feed_mean", 16.0),
    ):
        for day in days:
            _insert_evidence(
                conn,
                run_id,
                day=day,
                lead=1,
                variable="wind",
                quantity="wind_max",
                entity_type=baseline,
                entity_key=key,
                predicted=predicted,
            )
    # Feed 101: eligible on 3 of 4 truth days -> availability 0.75 >= 0.70.
    for day in days[:3]:
        _insert_evidence(
            conn,
            run_id,
            day=day,
            lead=1,
            variable="wind",
            quantity="wind_max",
            entity_type="feed",
            entity_key="101",
            predicted=10.5,
        )
    _insert_evidence(
        conn,
        run_id,
        day=days[3],
        lead=1,
        variable="wind",
        quantity="wind_max",
        entity_type="feed",
        entity_key="101",
        predicted=None,
        eligible=False,
    )
    conn.commit()
    generation = ensure_published_generation(conn, site_id)
    cfg = RunConfig(
        site_id=site_id,
        run_id=run_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        wall_clock="wc",
        blend_depth=2,
        blend_depths={"temperature": 2, "wind": 2, "precip": 2},
        min_n=30,
        window_days=30,
        tz_generation_id=generation,
        roster=(
            RosterFeed(
                feed_id=101,
                source="example-src",
                model="model-gamma",
                max_lead_hours=168,
            ),
        ),
        period_start=days[0],
        period_end=days[-1],
        bootstrap_seed=1,
        bootstrap_resamples=10,
    )
    aggregate_run(conn, cfg)
    conn.commit()

    row = conn.execute(
        """
        SELECT * FROM verification_results
        WHERE run_id = ? AND variable = 'wind' AND lead = 1
              AND entity_type = 'depth' AND entity_key = '3'
        """,
        (run_id,),
    ).fetchone()
    assert row is not None
    # depth 3 on the common core (all 4 depths + feed 101 eligible every
    # day: d1-d3): errors +1, -2, +3.
    assert int(row["headline"]) == 1
    assert int(row["common_days"]) == 3
    assert float(row["mae"]) == pytest.approx(2.0)
    # incumbent depth 2 errors on d1-d3: 2, -4, 6 -> MAE 4.0.
    assert float(row["delta_vs_incumbent"]) == pytest.approx(0.5)

    raw_state = conn.execute(
        "SELECT aggregate_state FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()["aggregate_state"]
    state = cast(dict[str, object], json.loads(str(raw_state)))
    cell = cast(dict[str, object], state["wind|1|wind_max"])
    assert cell["common_dates"] == days[:3]


# ---------------------------------------------------------------------------
# O7 / O7b / O7c -- shared-basis ordering: two multi-day passers whose OWN
# (per-window) pooled point reverses on the shared, intersected basis, and
# whose OWN-window CIs overlap while their shared-basis CIs are disjoint.
#
# Construction: candidate X owns dates 1-24 (X-only) plus 25-48 (shared with
# Y); candidate Y owns 25-48 (the same shared block) plus 49-72 (Y-only).
# X's shared-block values are poor (effect ~0.15) while its X-only block is
# excellent (effect ~0.99); Y's shared-block values are excellent (~0.85)
# while its Y-only block is poor (~0.1). Both blocks alternate two values so
# the moving-block bootstrap actually resamples real within-block variance
# (a fully constant block would yield a degenerate, zero-width CI on ANY
# subsample and could never show CI overlap changing).
#
# Own-window pooled point: X (0.57) > Y (0.475) -- the naive, per-candidate-
# window winner is X.
# Shared-basis (dates 25-48 only) pooled point: X (0.15) < Y (0.85) -- the
# correct shared-basis winner is Y. This is the exact reversal O7 targets.
# ---------------------------------------------------------------------------

_O7_ALL_DATES = _dates_n(72)
_O7_X_ONLY = _O7_ALL_DATES[:24]
_O7_SHARED = _O7_ALL_DATES[24:48]
_O7_Y_ONLY = _O7_ALL_DATES[48:72]
_O7_LEADS = (1, 2, 3, 4)
_O7_OPP = 10.0


def _o7_alternate(dates: list[str], lo: float, hi: float) -> list[float]:
    return [lo if i % 2 == 0 else hi for i in range(len(dates))]


def _o7_series(
    shared_values: list[float], x_only_value: float | None, y_only_value: float | None
) -> ContinuousLead:
    series: ContinuousLead = {}
    if x_only_value is not None:
        for d in _O7_X_ONLY:
            series[d] = (x_only_value, _O7_OPP)
    for d, c in zip(_O7_SHARED, shared_values, strict=True):
        series[d] = (c, _O7_OPP)
    if y_only_value is not None:
        for d in _O7_Y_ONLY:
            series[d] = (y_only_value, _O7_OPP)
    return series


def _o7_candidates() -> tuple[CandidateSeries, CandidateSeries]:
    x_series = _o7_series(_o7_alternate(_O7_SHARED, 9.5, 7.5), 0.1, None)
    y_series = _o7_series(_o7_alternate(_O7_SHARED, 2.0, 1.0), None, 9.0)
    x_leads = {ld: x_series for ld in _O7_LEADS}
    y_leads = {ld: y_series for ld in _O7_LEADS}
    x_base = {ld: _strong_baseline(s) for ld, s in x_leads.items()}
    y_base = {ld: _strong_baseline(s) for ld, s in y_leads.items()}
    x_cand = CandidateSeries(
        key="5",
        continuous={"wind_max": x_leads},
        baseline_continuous=continuous_baseline_set("wind_max", x_base),
    )
    y_cand = CandidateSeries(
        key="6",
        continuous={"wind_max": y_leads},
        baseline_continuous=continuous_baseline_set("wind_max", y_base),
    )
    return x_cand, y_cand


def _o7_verdict() -> Verdict:
    x_cand, y_cand = _o7_candidates()
    inputs = VariableInputs(
        variable="wind", incumbent_key="2", candidates=(x_cand, y_cand)
    )
    return decide_variable(inputs, seed=1, resamples=200)


def test_o7_shared_basis_ordering_reverses_the_naive_per_window_winner() -> None:
    verdict = _o7_verdict()
    x_headline = _headline(verdict, "5")
    y_headline = _headline(verdict, "6")
    # Own-window points: naive winner is "5" (X).
    assert x_headline["pooled_point"] == pytest.approx(0.57)
    assert y_headline["pooled_point"] == pytest.approx(0.475)
    assert cast(float, x_headline["pooled_point"]) > cast(
        float, y_headline["pooled_point"]
    )
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    # mutant -> at tie_break["chosen"]: correct = "6", mutant (naive
    # per-window max, no shared-basis restriction) = "5".
    assert tie_break["best_by_pooled"] == "6"
    assert tie_break["chosen"] == "6"
    assert verdict.outcome == "recommend"
    assert verdict.recommended_key == "6"
    basis = cast(dict[str, object], tie_break["basis"])
    points = cast(dict[str, float], basis["points"])
    assert points["5"] == pytest.approx(0.15)
    assert points["6"] == pytest.approx(0.85)
    # The basis window's own arithmetic: days_total is the size of the
    # DISTINCT-date union across all basis leads, never the sum of the
    # per-lead day counts. Every one of O7's four leads (1-4) reuses the
    # SAME 24 shared dates (_O7_SHARED), so the hand-computed union is
    # exactly those 24 dates while the per-lead sum quadruples it.
    basis_days = cast(dict[str, int], basis["days"])
    hand_computed_union = set(_O7_SHARED)
    assert basis["days_total"] == len(hand_computed_union)
    # mutant -> at basis["days_total"]: correct = 24 (the distinct-date
    # union), mutant (days_total computed as the SUM of the per-lead "days"
    # counts) = 96 -- strictly greater, since the fixture's leads overlap
    # completely rather than partially.
    assert basis["days_total"] < sum(basis_days.values())


def test_o7b_overlap_verdict_flips_between_stored_and_restricted_cis() -> None:
    verdict = _o7_verdict()
    x_headline = _headline(verdict, "5")
    y_headline = _headline(verdict, "6")
    x_ci = cast(tuple[float, float], x_headline["ci"])
    y_ci = cast(tuple[float, float], y_headline["ci"])
    # The STORED, own-window CIs overlap: a mutant that ran the overlap
    # check against these (restricting only the point, per the plan's D5
    # mutant) would find "5" and "6" statistically tied.
    assert x_ci[0] < y_ci[1] and y_ci[0] < x_ci[1]
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    basis = cast(dict[str, object], tie_break["basis"])
    cis = cast(dict[str, tuple[float, float]], basis["cis"])
    x_restricted_ci = cis["5"]
    y_restricted_ci = cis["6"]
    # The RESTRICTED, shared-basis CIs are disjoint.
    assert x_restricted_ci[1] < y_restricted_ci[0]
    # mutant -> at "statistically_unresolved" in verdict.detail: correct =
    # False (restricted CIs don't overlap, so "6" is a clean, unresolved
    # winner), mutant (overlap computed on the stored per-window CIs above,
    # which DO overlap) = True.
    assert "statistically_unresolved" not in verdict.detail
    assert tie_break["chosen"] == "6"


# ---------------------------------------------------------------------------
# O7c fixture extension -- both O7 candidates gain a fifth lead carrying a
# full 24-day shared-block pair complement, with `baseline_all_feed_mean`
# withheld at that lead for BOTH candidates. This reproduces the exact shape
# the carried-adequate-set discipline exists for: a lead with a complete
# candidate-vs-incumbent pair count that the §8/W5 baseline lockstep still
# drops -- the pair count alone can never distinguish it from an adequate
# lead, only the carried ``adequate_by_endpoint`` set can. Adding the lead to
# BOTH candidates, at the shared block's own dates, leaves O7/O7b's own-window
# and shared-basis numbers (computed over leads 1-4 only) untouched.
# ---------------------------------------------------------------------------

_O7C_LEAD = 5


def _o7c_candidates() -> tuple[CandidateSeries, CandidateSeries]:
    x_cand, y_cand = _o7_candidates()
    lead5_series: ContinuousLead = {d: (1.0, 2.0) for d in _O7_SHARED}
    x_leads = {**x_cand.continuous["wind_max"], _O7C_LEAD: lead5_series}
    y_leads = {**y_cand.continuous["wind_max"], _O7C_LEAD: lead5_series}
    x_persistence = {
        **x_cand.baseline_continuous["baseline_persistence"]["wind_max"],
        _O7C_LEAD: _strong_baseline(lead5_series),
    }
    y_persistence = {
        **y_cand.baseline_continuous["baseline_persistence"]["wind_max"],
        _O7C_LEAD: _strong_baseline(lead5_series),
    }
    x_cand5 = CandidateSeries(
        key=x_cand.key,
        continuous={"wind_max": x_leads},
        baseline_continuous={
            "baseline_persistence": {"wind_max": x_persistence},
            # baseline_all_feed_mean deliberately omits lead 5: the
            # required-baseline lockstep drops it with reason
            # "baseline_absent" even though its candidate-vs-incumbent pair
            # count at that lead is full.
            "baseline_all_feed_mean": x_cand.baseline_continuous[
                "baseline_all_feed_mean"
            ],
        },
    )
    y_cand5 = CandidateSeries(
        key=y_cand.key,
        continuous={"wind_max": y_leads},
        baseline_continuous={
            "baseline_persistence": {"wind_max": y_persistence},
            "baseline_all_feed_mean": y_cand.baseline_continuous[
                "baseline_all_feed_mean"
            ],
        },
    )
    return x_cand5, y_cand5


def _o7c_verdict() -> tuple[Verdict, CandidateSeries, CandidateSeries]:
    x_cand, y_cand = _o7c_candidates()
    inputs = VariableInputs(
        variable="wind", incumbent_key="2", candidates=(x_cand, y_cand)
    )
    verdict = decide_variable(inputs, seed=1, resamples=200)
    return verdict, x_cand, y_cand


def test_o7c_disclosed_basis_is_the_carried_adequate_set_not_a_recount() -> None:
    verdict, x_cand, y_cand = _o7c_verdict()
    x_headline = _headline(verdict, "5")
    y_headline = _headline(verdict, "6")
    x_adequate = cast(list[int], x_headline["adequate_leads"])
    y_adequate = cast(list[int], y_headline["adequate_leads"])
    # Lead 5 carries a full 24-day candidate-vs-incumbent pair complement for
    # BOTH candidates -- the drop below is caused ONLY by the withheld
    # baseline, never by a thin pair count, and the recorded reason is
    # pinned exactly (not by substring) so a future drift onto the wrong
    # reason can't hide behind this fixture.
    for key, cand in (("5", x_cand), ("6", y_cand)):
        endpoint = _index_endpoint("continuous", cand.continuous["wind_max"])
        assert len(endpoint.leads[_O7C_LEAD]) == 24
        headline = _headline(verdict, key)
        dropped = cast(list[dict[str, object]], headline["dropped_leads"])
        lead5_drop = next(d for d in dropped if d["lead"] == _O7C_LEAD)
        assert lead5_drop["reason"] == "baseline_absent"
    assert _O7C_LEAD not in x_adequate
    assert _O7C_LEAD not in y_adequate

    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    basis = cast(dict[str, object], tie_break["basis"])
    # The disclosed basis is exactly the shared 24-day block (2026-07-25 to
    # 2026-08-17) at all 4 adequate leads -- the carried intersection of
    # each endpoint's own adequate-lead date sets, not a re-derivation from
    # raw per-lead pair counts (which would see lead 5's full 24-day pair
    # complement at both candidates and re-admit it, even though §8/W5
    # already dropped it for a missing required baseline).
    assert basis["leads"] == [1, 2, 3, 4]
    assert _O7C_LEAD not in cast(list[int], basis["leads"])
    assert basis["leads"] == sorted(set(x_adequate) & set(y_adequate))
    assert basis["first"] == _O7_SHARED[0]
    assert basis["last"] == _O7_SHARED[-1]
    days = cast(dict[str, int], basis["days"])
    # mutant -> at basis["leads"]: correct = [1, 2, 3, 4] (the carried
    # adequate-lead intersection, from which §8/W5 already dropped lead 5),
    # mutant (re-deriving leads from `ordering_endpoint.leads` pair counts
    # instead of the carried `adequate_by_endpoint` set) = [1, 2, 3, 4, 5]
    # (lead 5's full 24-day pair complement at both candidates passes a bare
    # pair-count check that knows nothing about the baseline lockstep).
    for lead in ("1", "2", "3", "4"):
        assert days[lead] == 24


def _o7c_with_all_feed_mean_at_five(cand: CandidateSeries) -> CandidateSeries:
    all_feed_mean = cand.baseline_continuous["baseline_all_feed_mean"]["wind_max"]
    lead5_series = cand.continuous["wind_max"][_O7C_LEAD]
    updated = {**all_feed_mean, _O7C_LEAD: _strong_baseline(lead5_series)}
    return CandidateSeries(
        key=cand.key,
        continuous=cand.continuous,
        baseline_continuous={
            "baseline_persistence": cand.baseline_continuous["baseline_persistence"],
            "baseline_all_feed_mean": {"wind_max": updated},
        },
    )


def test_o7c_positive_control_lead_joins_basis_once_its_baseline_is_supplied() -> None:
    """Liveness half of O7c: identical lead-5 shape, but the previously
    withheld baseline is supplied too, so §8/W5 no longer drops lead 5 and
    it joins the shared basis. Without this, O7c's exclusion of lead 5
    could be a permanent fixture quirk (e.g. some code path that always
    skips a fifth lead) rather than the baseline-driven drop it claims to
    pin -- this proves the toggle actually moves.
    """
    x_cand, y_cand = _o7c_candidates()
    x_full = _o7c_with_all_feed_mean_at_five(x_cand)
    y_full = _o7c_with_all_feed_mean_at_five(y_cand)
    inputs = VariableInputs(
        variable="wind", incumbent_key="2", candidates=(x_full, y_full)
    )
    verdict = decide_variable(inputs, seed=1, resamples=200)
    x_headline = _headline(verdict, "5")
    y_headline = _headline(verdict, "6")
    assert _O7C_LEAD in cast(list[int], x_headline["adequate_leads"])
    assert _O7C_LEAD in cast(list[int], y_headline["adequate_leads"])
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    basis = cast(dict[str, object], tie_break["basis"])
    assert basis["leads"] == [1, 2, 3, 4, 5]
    days = cast(dict[str, int], basis["days"])
    assert days["5"] == 24


# ---------------------------------------------------------------------------
# O21c -- page-level companion to D9's basis precedence: the four §16.2
# decision-summary fields report the RESOLVED shared basis (D5/D9), scoped
# to exactly those four field nodes -- never candidate "6"'s OWN-window
# point/interval/window, which the §16.5 tested-family table still reports,
# completely unchanged, for every candidate row. Reuses O7's real
# decide_variable() result and O7's own pinned numbers (0.85 basis point /
# 24-day basis window for "6", vs. 0.475 own point / 48-day own window),
# persisting the WHOLE ``verdict.detail`` as ``tested_family`` -- O15's
# partial {"incumbent", "candidates"} shape drops "tie_break" and could
# never resolve a basis at all.
#
# Deliberately placed here, beside O7, rather than in
# test_phase8_verification_v16.py: with no ``tests/__init__.py``, this
# module and ``tests.test_verification_comparison_core`` are two distinct
# module objects, so a fixture built here cannot safely be imported into
# the v16 file (or vice versa) without risking drift between two "copies"
# of the same constants.
# ---------------------------------------------------------------------------


def _o21c_field(card: str, name: str) -> str:
    """One §16.2 field's own node, scoped to just that field.

    §16.2 fields are a mix of ``<div>`` (the ``<dl class="facts">`` entries)
    and ``<p>`` (``basis_inconsistent``, ``ordering_endpoint_unresolved``,
    ``primary_missing``, ...) -- this closes on whichever wrapping tag
    actually follows the marker, never assuming ``</div>`` unconditionally
    (which could over-capture into a later element's markup). Mirrors
    ``_field`` in ``tests/test_phase8_verification_v16.py:812``.
    """
    marker = f'data-v16="16.2.{name}"'
    start = card.index(marker)
    div_end = card.find("</div>", start)
    p_end = card.find("</p>", start)
    ends = [e for e in (div_end, p_end) if e != -1]
    end = min(ends)
    tag_len = len("</div>") if end == div_end else len("</p>")
    return card[start : end + tag_len]


def test_o21c_decision_summary_reports_the_basis_not_the_own_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fastapi.testclient import TestClient

    from wxverify import config
    from wxverify.api.app import create_app
    from wxverify.db.connection import close_db, init_db

    verdict = _o7_verdict()
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    basis = cast(dict[str, object], tie_break["basis"])
    # Non-vacuousness: the payload about to be persisted really does carry
    # a RESOLVED shared basis with a "points" map (not a refusal, and not
    # O15's partial shape, which has no "tie_break" key at all).
    assert "reason" not in basis
    points = cast(dict[str, float], basis["points"])
    cis = cast(dict[str, list[float]], basis["cis"])
    assert points["6"] == pytest.approx(0.85)
    assert verdict.outcome == "recommend"
    assert verdict.recommended_key == "6"
    basis_first = str(basis["first"])
    basis_last = str(basis["last"])
    basis_days_total = basis["days_total"]
    assert basis_days_total == 24
    basis_ci = cis["6"]

    y_headline = _headline(verdict, "6")
    assert y_headline["pooled_point"] == pytest.approx(0.475)
    y_window = cast(dict[str, object], y_headline["window"])
    assert y_window["days"] == 48
    # The premise that makes (b) below non-vacuous: the basis and own-window
    # numbers really do differ, on both the point and the CI.
    assert basis_ci != list(cast(tuple[float, float], y_headline["ci"]))

    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001

    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                               timezone, enabled)
            VALUES ('oracle-o21c-town', 40.0, -105.0, 900.0, 'UTC', 1)
            """
        ).lastrowid
    )
    generation_id = ensure_published_generation(conn, site_id)
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
            VALUES (?, ?, 2, 'test', 'running', 1, ?, '2026-07-01',
                    '2026-09-30', '2026-09-30', 1, 200, ?)
            """,
            (site_id, generation_id, json.dumps(snapshot), fingerprint),
        ).lastrowid
    )
    # The WHOLE verdict.detail -- incumbent, candidates, tie_break AND
    # statistically_unresolved -- never O15's partial
    # {"incumbent", "candidates"} shape, which drops "tie_break" and so
    # could never resolve a basis at all.
    conn.execute(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, recommended_depth, incumbent_depth,
             tested_family)
        VALUES (?, 'wind', 'recommend', 6, 2, ?)
        """,
        (run_id, json.dumps(verdict.detail)),
    )
    publish_run(conn, site_id, run_id)
    conn.commit()

    async def _idle_worker(_db: object) -> None:
        import asyncio

        await asyncio.Event().wait()

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200
    page = response.text

    card_start = page.index('data-v16="16.2.card" data-variable="wind"')
    card_end = page.index('id="verification-headline"')
    card = page[card_start:card_end]

    # A RESOLVED basis (tie_break.basis present, with no "reason") must not
    # render the ordering-refusal notice: that notice is reserved for a
    # basis that carries a "reason" (a refused comparison). O7's fixture
    # resolves "6" over "5" outright with no refusal, so this is the
    # positive-basis control the o22c "no basis at all" fixture cannot be:
    # a basis.reason default of "thin_shared_basis" would make this fire.
    assert 'data-v16="16.2.ordering_refusal"' not in card
    # Not asserting the paired "statistically unresolved against the chosen
    # depth" wording here: O7's verdict carries no
    # detail["statistically_unresolved"] (points 0.15 vs 0.85, cis
    # [0.125,0.175] vs [0.833,0.8625] -- no CI overlap with the chosen key),
    # so v.unresolved is empty and the 16.2.unresolved block never renders
    # at all in this fixture; asserting the wording would be vacuous.
    effect = _o21c_field(card, "primary_effect")
    ci_field = _o21c_field(card, "primary_ci")
    adequate = _o21c_field(card, "adequate_leads")
    window = _o21c_field(card, "decision_window")

    # (a) all four fields carry the RESOLVED BASIS values for "6".
    assert "0.850" in effect
    assert f"[{basis_ci[0]:.3f}, {basis_ci[1]:.3f}]" in ci_field
    assert "4 of 4 required" in adequate
    assert basis_first in window
    assert f"{basis_last} (24 days)" in window

    # (b) ...and NOWHERE do those same four field nodes carry "6"'s OWN,
    # per-candidate-window numbers -- scoped to exactly these four nodes,
    # never the whole page (see (c): a page-wide "nowhere" formulation
    # would be self-contradictory against this very page).
    for field in (effect, ci_field, adequate, window):
        assert "0.475" not in field
        assert "48 days" not in field

    # (c) the §16.5 tested-family table still reports "6"'s OWN window and
    # point, completely unchanged by the basis resolution above -- proving
    # (b)'s scoping is load-bearing: "0.475"/"48 days" genuinely DO appear
    # on this page, just never inside the four decision-summary fields.
    row_marker = 'data-candidate="6" data-endpoint="headline"'
    row_start = page.index(row_marker)
    row_end = page.index("</tr>", row_start)
    row = page[row_start:row_end]
    assert "0.475" in row
    assert "48 days" in row


def test_o21d_resolved_basis_does_not_license_an_unlabelled_number(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same real-verdict fixture O21c uses, with "6"'s
    ``ordering_endpoint_name`` rewritten in the PERSISTED payload to
    "occurrence" -- a name this headline-only wind record does not hold. The
    verdict therefore carries a RESOLVED shared basis (D9) AND an
    unresolvable selector (D14) at once.
    """
    from fastapi.testclient import TestClient

    from wxverify import config
    from wxverify.api.app import create_app
    from wxverify.db.connection import close_db, init_db

    verdict = _o7_verdict()
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    basis = cast(dict[str, object], tie_break["basis"])
    assert "reason" not in basis
    points = cast(dict[str, float], basis["points"])
    cis = cast(dict[str, list[float]], basis["cis"])
    assert points["6"] == pytest.approx(0.85)
    basis_first = str(basis["first"])
    basis_last = str(basis["last"])
    basis_days_total = basis["days_total"]
    assert basis_days_total == 24
    basis_ci = cis["6"]

    detail = cast(dict[str, object], json.loads(json.dumps(verdict.detail)))
    candidates = cast(dict[str, object], detail["candidates"])
    chosen = cast(dict[str, object], candidates["6"])
    # This wind record holds only the "headline" endpoint -- "occurrence"
    # is a name it does not contain (§7.12/D14's third row).
    assert set(chosen) & {"headline", "total", "occurrence"} == {"headline"}
    chosen["ordering_endpoint_name"] = "occurrence"

    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001

    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                               timezone, enabled)
            VALUES ('oracle-o21d-town', 40.0, -105.0, 900.0, 'UTC', 1)
            """
        ).lastrowid
    )
    generation_id = ensure_published_generation(conn, site_id)
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
            VALUES (?, ?, 2, 'test', 'running', 1, ?, '2026-07-01',
                    '2026-09-30', '2026-09-30', 1, 200, ?)
            """,
            (site_id, generation_id, json.dumps(snapshot), fingerprint),
        ).lastrowid
    )
    conn.execute(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, recommended_depth, incumbent_depth,
             tested_family)
        VALUES (?, 'wind', 'recommend', 6, 2, ?)
        """,
        (run_id, json.dumps(detail)),
    )
    publish_run(conn, site_id, run_id)
    conn.commit()

    async def _idle_worker(_db: object) -> None:
        import asyncio

        await asyncio.Event().wait()

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200
    page = response.text

    card_start = page.index('data-v16="16.2.card" data-variable="wind"')
    card_end = page.index('id="verification-headline"')
    card = page[card_start:card_end]

    # (b) the unresolved-selector line names the literal recorded name.
    unresolved_field = _o21c_field(card, "ordering_endpoint_unresolved")
    assert "occurrence" in unresolved_field

    # (c) primary_effect/primary_ci take D13's not-available phrase, and the
    # formatted basis point and interval strings appear NOWHERE inside those
    # two nodes -- a number the verdict really did use, but whose endpoint
    # cannot be named, is not evidence (D9) and must not print unlabelled.
    effect = _o21c_field(card, "primary_effect")
    ci_field = _o21c_field(card, "primary_ci")
    for field in (effect, ci_field):
        assert "Not available" in field
        assert "0.850" not in field
        assert f"[{basis_ci[0]:.3f}, {basis_ci[1]:.3f}]" not in field

    # (d) adequate_leads/decision_window still carry the BASIS lead count and
    # the basis first-last over days_total days -- neither depends on naming
    # an endpoint.
    adequate = _o21c_field(card, "adequate_leads")
    window = _o21c_field(card, "decision_window")
    assert "4 of 4 required" in adequate
    assert basis_first in window
    assert f"{basis_last} ({basis_days_total} days)" in window

    # (e) the warning states the RULE, not a field count: "four summary
    # fields" is absent, since two of the four (adequate_leads,
    # decision_window) ARE filled on this very card.
    assert "four summary fields" not in card


def _o21e_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    detail: dict[str, object],
    *,
    site_name: str,
) -> str:
    """Persist the given ``tested_family`` for a wind verdict and render it.

    Shared by O21e's modified-basis case and its (f) unmodified-fixture
    control -- both need the identical insert/publish/render machinery
    O21c and O21d already duplicate once each.
    """
    from fastapi.testclient import TestClient

    from wxverify import config
    from wxverify.api.app import create_app
    from wxverify.db.connection import close_db, init_db

    close_db()
    db_path = tmp_path / f"{site_name}.db"
    config.db_path = str(db_path)
    options_path = tmp_path / f"{site_name}-options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001

    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                               timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)
            """,
            (site_name,),
        ).lastrowid
    )
    generation_id = ensure_published_generation(conn, site_id)
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
            VALUES (?, ?, 2, 'test', 'running', 1, ?, '2026-07-01',
                    '2026-09-30', '2026-09-30', 1, 200, ?)
            """,
            (site_id, generation_id, json.dumps(snapshot), fingerprint),
        ).lastrowid
    )
    conn.execute(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, recommended_depth, incumbent_depth,
             tested_family)
        VALUES (?, 'wind', 'recommend', 6, 2, ?)
        """,
        (run_id, json.dumps(detail)),
    )
    publish_run(conn, site_id, run_id)
    conn.commit()

    async def _idle_worker(_db: object) -> None:
        import asyncio

        await asyncio.Event().wait()

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200
    return response.text


def test_o21e_basis_missing_the_chosen_key_is_disclosed_not_backfilled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O21c's real-verdict fixture, with "6" deleted from ``basis["points"]``
    AND ``basis["cis"]`` in the PERSISTED payload -- the selector stays
    resolvable and ``recommended_depth`` stays 6, so this is a THIRD, distinct
    stored-verdict inconsistency from O21d's unresolvable-selector one (D9).
    """
    verdict = _o7_verdict()
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    basis = cast(dict[str, object], tie_break["basis"])
    # Non-vacuousness premises, asserted first so the fixture cannot rot
    # into a no-op: a genuinely resolved basis...
    assert "reason" not in basis
    points = cast(dict[str, float], basis["points"])
    cis = cast(dict[str, list[float]], basis["cis"])
    assert points["6"] == pytest.approx(0.85)
    assert "6" in cis
    basis_first = str(basis["first"])
    basis_last = str(basis["last"])
    basis_days_total = basis["days_total"]
    assert basis_days_total == 24

    y_headline = _headline(verdict, "6")
    assert y_headline["pooled_point"] == pytest.approx(0.475)
    y_window = cast(dict[str, object], y_headline["window"])
    assert y_window["days"] == 48

    # Deep-copy before mutating (O21d's discipline, :1150) -- the payload
    # is a shared-shape fixture, never mutated in place.
    detail = cast(dict[str, object], json.loads(json.dumps(verdict.detail)))
    mutated_tie_break = cast(dict[str, object], detail["tie_break"])
    mutated_basis = cast(dict[str, object], mutated_tie_break["basis"])
    mutated_points = cast(dict[str, object], mutated_basis["points"])
    mutated_cis = cast(dict[str, object], mutated_basis["cis"])
    del mutated_points["6"]
    del mutated_cis["6"]
    # ...that now genuinely holds no entry for the chosen key.
    assert "6" not in mutated_points
    assert "6" not in mutated_cis

    page = _o21e_run(tmp_path, monkeypatch, detail, site_name="oracle-o21e-town")
    card_start = page.index('data-v16="16.2.card" data-variable="wind"')
    card_end = page.index('id="verification-headline"')
    card = page[card_start:card_end]

    # (a) already implied by the 200 status asserted inside _o21e_run.

    # (b) primary_effect/primary_ci carry D13's not-available phrase, and
    # NEITHER "6"'s own-window point/window nor its basis point/window --
    # this basis is inconsistent, so nothing on that scale may print --
    # appear anywhere within the `16.2.card` node. This is the separating
    # assertion: it is what distinguishes disclosure from a silent
    # own-window fall-back.
    effect = _o21c_field(card, "primary_effect")
    ci_field = _o21c_field(card, "primary_ci")
    for field in (effect, ci_field):
        assert "Not available" in field
    assert "0.475" not in card
    assert "48 days" not in card

    # (c) the defect is named on the card.
    assert 'data-v16="16.2.basis_inconsistent"' in card

    # (d) adequate_leads/decision_window are exempt (D9): neither depends on
    # the missing per-key entry, so both still carry the basis's own
    # lead count and window.
    adequate = _o21c_field(card, "adequate_leads")
    window = _o21c_field(card, "decision_window")
    assert "4 of 4 required" in adequate
    assert basis_first in window
    assert f"{basis_last} ({basis_days_total} days)" in window

    # (e) primary_missing and ordering_endpoint_unresolved are absent: this
    # is a third, distinct stored-verdict inconsistency from either of
    # theirs, and must not collapse into one of them.
    assert 'data-v16="16.2.primary_missing"' not in card
    assert 'data-v16="16.2.ordering_endpoint_unresolved"' not in card

    # (f) control: the SAME O7-derived fixture, unmodified, renders the
    # basis point 0.85 and carries no basis_inconsistent node -- proving
    # the defect above is caused by the deletion, not by something else in
    # this fixture family.
    control_page = _o21e_run(
        tmp_path, monkeypatch, verdict.detail, site_name="oracle-o21e-control-town"
    )
    control_card_start = control_page.index('data-v16="16.2.card" data-variable="wind"')
    control_card_end = control_page.index('id="verification-headline"')
    control_card = control_page[control_card_start:control_card_end]
    control_effect = _o21c_field(control_card, "primary_effect")
    assert "0.850" in control_effect
    assert 'data-v16="16.2.basis_inconsistent"' not in control_card


# ---------------------------------------------------------------------------
# O8 -- fail-closed thin basis: two passers whose OWN adequate-lead sets
# each individually clear MIN_ADEQUATE_LEADS_PER_VARIABLE (4) but whose
# INTERSECTION does not (leads {1,2,3,4} vs {2,3,4,5} share only 3).
# ---------------------------------------------------------------------------


def _o8_ratio_series(dates: list[str], ratio: float) -> ContinuousLead:
    return {d: (ratio * (10.0 + 0.1 * i), 10.0 + 0.1 * i) for i, d in enumerate(dates)}


def test_o8_fail_closed_thin_shared_lead_intersection() -> None:
    dates = _dates_n(24)
    x_leads = {ld: _o8_ratio_series(dates, 0.5) for ld in (1, 2, 3, 4)}
    y_leads = {ld: _o8_ratio_series(dates, 0.5) for ld in (2, 3, 4, 5)}
    x_base = {ld: _strong_baseline(s) for ld, s in x_leads.items()}
    y_base = {ld: _strong_baseline(s) for ld, s in y_leads.items()}
    x_cand = CandidateSeries(
        key="5",
        continuous={"wind_max": x_leads},
        baseline_continuous=continuous_baseline_set("wind_max", x_base),
    )
    y_cand = CandidateSeries(
        key="6",
        continuous={"wind_max": y_leads},
        baseline_continuous=continuous_baseline_set("wind_max", y_base),
    )
    inputs = VariableInputs(
        variable="wind", incumbent_key="2", candidates=(x_cand, y_cand)
    )
    verdict = decide_variable(inputs, seed=1, resamples=40)
    x_headline = _headline(verdict, "5")
    y_headline = _headline(verdict, "6")
    # The premise: each candidate is independently adequate on 4 leads.
    assert x_headline["adequate_leads"] == [1, 2, 3, 4]
    assert y_headline["adequate_leads"] == [2, 3, 4, 5]
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    # mutant -> at tie_break["basis"]: correct = {"reason":
    # "thin_shared_basis"} (the shared lead set {2,3,4} has only 3 members,
    # below MIN_ADEQUATE_LEADS_PER_VARIABLE), mutant (a shared-basis step
    # that intersects DATES only, never LEADS) = a basis dict with "leads"
    # and "points" keys instead of a refusal.
    assert tie_break["basis"] == {"reason": "thin_shared_basis"}
    assert tie_break["best_by_pooled"] is None
    assert "statistically_unresolved" in verdict.detail
    assert verdict.detail["statistically_unresolved"] == ["6"]


# ---------------------------------------------------------------------------
# O8b -- fail-closed undefined restricted CI: two passers with a clean,
# well-defined OWN-window CI each, whose shared-basis restriction collapses
# one of them to an undefined ratio (opponent value == 0 on every shared
# date, deterministically undefined for ANY non-empty resample of it) --
# paired with a liveness-half positive control proving the identical
# construction resolves normally once that restricted subset is no longer
# degenerate.
# ---------------------------------------------------------------------------

_O8B_ALL_DATES = _dates_n(72)
_O8B_X_ONLY = _O8B_ALL_DATES[:24]
_O8B_SHARED = _O8B_ALL_DATES[24:48]
_O8B_Y_ONLY = _O8B_ALL_DATES[48:72]
_O8B_LEADS = (1, 2, 3, 4)


def _o8b_candidates(shared_opp: float) -> tuple[CandidateSeries, CandidateSeries]:
    x_series: ContinuousLead = {}
    for d in _O8B_X_ONLY:
        x_series[d] = (1.0, 10.0)
    for d in _O8B_SHARED:
        x_series[d] = (1.0, shared_opp)
    y_series: ContinuousLead = {}
    for d in _O8B_SHARED:
        y_series[d] = (1.0, 8.0)
    for d in _O8B_Y_ONLY:
        y_series[d] = (1.0, 8.0)
    x_leads = {ld: x_series for ld in _O8B_LEADS}
    y_leads = {ld: y_series for ld in _O8B_LEADS}
    x_base = {ld: _strong_baseline(s) for ld, s in x_leads.items()}
    y_base = {ld: _strong_baseline(s) for ld, s in y_leads.items()}
    x_cand = CandidateSeries(
        key="5",
        continuous={"wind_max": x_leads},
        baseline_continuous=continuous_baseline_set("wind_max", x_base),
    )
    y_cand = CandidateSeries(
        key="6",
        continuous={"wind_max": y_leads},
        baseline_continuous=continuous_baseline_set("wind_max", y_base),
    )
    return x_cand, y_cand


def test_o8b_fail_closed_undefined_restricted_ci() -> None:
    x_cand, y_cand = _o8b_candidates(shared_opp=0.0)
    inputs = VariableInputs(
        variable="wind", incumbent_key="2", candidates=(x_cand, y_cand)
    )
    verdict = decide_variable(inputs, seed=1, resamples=200)
    x_headline = _headline(verdict, "5")
    # The premise: X's OWN, full-window CI is clean and well-defined -- the
    # degeneracy is a property of the shared basis, not of X on its own.
    assert x_headline["ci"] is not None
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    # mutant -> at tie_break["basis"]: correct = {"reason":
    # "undefined_restricted_ci"} (X's shared-basis dates all carry
    # opponent == 0, so every resample of them is an undefined ratio),
    # mutant (a step that recomputes only the POINT on the shared basis and
    # reuses each passer's STORED per-window CI) = a basis dict with a
    # defined "cis" entry for "5" instead of a refusal.
    assert tie_break["basis"] == {"reason": "undefined_restricted_ci"}
    assert tie_break["best_by_pooled"] is None
    assert "statistically_unresolved" in verdict.detail


def test_o8b_positive_control_same_construction_resolves_when_defined() -> None:
    """Liveness half of O8b: identical shape, non-degenerate shared block."""
    x_cand, y_cand = _o8b_candidates(shared_opp=5.0)
    inputs = VariableInputs(
        variable="wind", incumbent_key="2", candidates=(x_cand, y_cand)
    )
    verdict = decide_variable(inputs, seed=1, resamples=200)
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    basis = cast(dict[str, object], tie_break["basis"])
    assert "reason" not in basis
    assert set(basis) == {
        "leads",
        "days",
        "days_total",
        "first",
        "last",
        "points",
        "cis",
    }
    cis = cast(dict[str, object], basis["cis"])
    assert cis["5"] is not None
    assert cis["6"] is not None


# ---------------------------------------------------------------------------
# O8c -- fail-closed thick-in-days-thin-in-events: two occurrence passers
# whose shared basis clears the 20-day floor (24 shared days) but whose
# shared-only wet count (2) falls below OCCURRENCE_MIN_WET_DAYS (8), while
# each candidate's OWN full occurrence window clears both floors on its
# own. A day/event-token-separation assertion and a liveness-half positive
# control (comfortable shared wet/dry counts) are both included.
# ---------------------------------------------------------------------------

_O8C_ALL_DATES = _dates_n(72)
_O8C_X_ONLY = _O8C_ALL_DATES[:24]
_O8C_SHARED = _O8C_ALL_DATES[24:48]
_O8C_Y_ONLY = _O8C_ALL_DATES[48:72]
_O8C_LEADS = (1, 2, 3, 4)


def _o8c_flip_baseline(occ: OccurrenceLead) -> OccurrenceLead:
    flip = {"hit": "miss", "correct_negative": "false_alarm"}
    return {d: (cand, flip[cand]) for d, (cand, _opp) in occ.items()}


def _o8c_build_occ(
    only_dates: list[str],
    only_pair: tuple[str, str],
    shared_pairs: list[tuple[str, str]],
) -> OccurrenceLead:
    out: OccurrenceLead = {}
    for d in only_dates:
        out[d] = only_pair
    for d, pair in zip(_O8C_SHARED, shared_pairs, strict=True):
        out[d] = pair
    return out


def _o8c_candidates(x_wet_in_shared: int) -> tuple[CandidateSeries, CandidateSeries]:
    x_shared: list[tuple[str, str]] = [("hit", "miss")] * x_wet_in_shared + [
        ("correct_negative", "false_alarm")
    ] * (24 - x_wet_in_shared)
    x_occ = _o8c_build_occ(_O8C_X_ONLY, ("hit", "miss"), x_shared)
    y_shared: list[tuple[str, str]] = [("hit", "miss")] * 10 + [
        ("correct_negative", "false_alarm")
    ] * 14
    y_occ = _o8c_build_occ(_O8C_Y_ONLY, ("correct_negative", "false_alarm"), y_shared)

    # A trivial, exactly-tied precip_total (effect 0) on all dates: clears
    # F-1's non-inferiority check for both candidates without contributing
    # any material improvement of its own.
    x_total: ContinuousLead = {d: (1.0, 1.0) for d in (*_O8C_X_ONLY, *_O8C_SHARED)}
    y_total: ContinuousLead = {d: (1.0, 1.0) for d in (*_O8C_SHARED, *_O8C_Y_ONLY)}

    x_cand = CandidateSeries(
        key="5",
        continuous={"precip_total": {ld: x_total for ld in _O8C_LEADS}},
        occurrence={ld: dict(x_occ) for ld in _O8C_LEADS},
        baseline_continuous=continuous_baseline_set(
            "precip_total", {ld: _strong_baseline(x_total) for ld in _O8C_LEADS}
        ),
        baseline_occurrence=occurrence_baseline_set(
            {ld: _o8c_flip_baseline(x_occ) for ld in _O8C_LEADS}
        ),
    )
    y_cand = CandidateSeries(
        key="6",
        continuous={"precip_total": {ld: y_total for ld in _O8C_LEADS}},
        occurrence={ld: dict(y_occ) for ld in _O8C_LEADS},
        baseline_continuous=continuous_baseline_set(
            "precip_total", {ld: _strong_baseline(y_total) for ld in _O8C_LEADS}
        ),
        baseline_occurrence=occurrence_baseline_set(
            {ld: _o8c_flip_baseline(y_occ) for ld in _O8C_LEADS}
        ),
    )
    return x_cand, y_cand


def test_o8c_fail_closed_thick_days_thin_events() -> None:
    x_cand, y_cand = _o8c_candidates(x_wet_in_shared=2)
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(x_cand, y_cand)
    )
    verdict = decide_variable(inputs, seed=1, resamples=40)
    x_occ_record = cast(dict[str, object], _record(verdict, "5")["occurrence"])
    # The premise: X's OWN, full occurrence window is adequate on all 4
    # leads -- the event-thinness is a property of the shared 24-day block
    # alone (2 wet days there), not of X's own window (26 wet, 22 dry).
    assert x_occ_record["adequate_leads"] == [1, 2, 3, 4]
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    # mutant -> at tie_break["basis"]["reason"]: correct =
    # "thin_shared_events" (the shared block clears the 20-day floor but
    # not the 8-wet-day floor), mutant (a shared-basis step that checks
    # only the day floor and never re-applies the event floor on the
    # restricted endpoint) = a basis dict with "leads"/"points" keys, no
    # refusal at all.
    assert tie_break["basis"] == {"reason": "thin_shared_events"}
    # Day/event-token separation: this refusal is NOT "thin_shared_basis"
    # (the day floor genuinely passed here; O8 pins that token for the
    # day-caused case).
    assert cast(dict[str, object], tie_break["basis"])["reason"] != "thin_shared_basis"


def test_o8c_positive_control_comfortable_shared_events_resolves() -> None:
    """Liveness half of O8c: same day-shape, comfortable shared wet count."""
    x_cand, y_cand = _o8c_candidates(x_wet_in_shared=10)
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(x_cand, y_cand)
    )
    verdict = decide_variable(inputs, seed=1, resamples=40)
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    basis = cast(dict[str, object], tie_break["basis"])
    assert "reason" not in basis
    assert basis["days"] == {"1": 24, "2": 24, "3": 24, "4": 24}


# ---------------------------------------------------------------------------
# O9 -- fail-closed mixed endpoint kind (F-4): two precip passers, one
# ordering on "total" (continuous) and the other on "occurrence" -- §12's
# shared-basis step cannot pool a relative-MAE endpoint with an ETS-diff
# endpoint, so it must refuse rather than silently comparing units.
# ---------------------------------------------------------------------------


def test_o9_fail_closed_mixed_endpoint_kind() -> None:
    dates = _dates_n(24)
    leads = (1, 2, 3, 4)

    # Candidate A: material precip_total improvement; occurrence exactly
    # tied with the opponent (never material) -> orders on "total".
    a_total: ContinuousLead = {
        d: (0.5 * (10.0 + 0.1 * i), 10.0 + 0.1 * i) for i, d in enumerate(dates)
    }
    a_occ: OccurrenceLead = {}
    for i, d in enumerate(dates):
        a_occ[d] = (
            ("hit", "hit") if i < 12 else ("correct_negative", "correct_negative")
        )
    a_cand = CandidateSeries(
        key="5",
        continuous={"precip_total": {ld: a_total for ld in leads}},
        occurrence={ld: dict(a_occ) for ld in leads},
        baseline_continuous=continuous_baseline_set(
            "precip_total", {ld: _strong_baseline(a_total) for ld in leads}
        ),
        baseline_occurrence=occurrence_baseline_set(
            {ld: _o8c_flip_baseline(a_occ) for ld in leads}
        ),
    )

    # Candidate B: material occurrence improvement; precip_total exactly
    # tied with the opponent (never material) -> orders on "occurrence".
    b_total: ContinuousLead = {d: (1.0, 1.0) for d in dates}
    b_occ: OccurrenceLead = {}
    for i, d in enumerate(dates):
        b_occ[d] = ("hit", "miss") if i < 12 else ("correct_negative", "false_alarm")
    b_cand = CandidateSeries(
        key="6",
        continuous={"precip_total": {ld: b_total for ld in leads}},
        occurrence={ld: dict(b_occ) for ld in leads},
        baseline_continuous=continuous_baseline_set(
            "precip_total", {ld: _strong_baseline(b_total) for ld in leads}
        ),
        baseline_occurrence=occurrence_baseline_set(
            {ld: _o8c_flip_baseline(b_occ) for ld in leads}
        ),
    )

    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(a_cand, b_cand)
    )
    verdict = decide_variable(inputs, seed=1, resamples=40)
    a_record = _record(verdict, "5")
    b_record = _record(verdict, "6")
    # The premise: the two passers really do order on different endpoint
    # kinds.
    assert a_record["ordering_endpoint_name"] == "total"
    assert b_record["ordering_endpoint_name"] == "occurrence"
    tie_break = cast(dict[str, object], verdict.detail["tie_break"])
    # mutant -> at tie_break["basis"]: correct = {"reason":
    # "mixed_endpoint_kind"} (a relative-MAE endpoint and an ETS-diff
    # endpoint can never share a basis), mutant (a shared-basis step that
    # never checks `.kind` before intersecting) = a basis dict that pools
    # the two candidates' unrelated dates and units together.
    assert tie_break["basis"] == {"reason": "mixed_endpoint_kind"}
    assert tie_break["best_by_pooled"] is None


# ---------------------------------------------------------------------------
# O9b -- atomic ordering-tuple selection (Fix 1, D5): when a precip
# candidate improves on BOTH endpoints, the recorded point/CI/endpoint-name
# tuple must all come from the SAME endpoint -- occurrence, per F-4's
# occurrence-priority rule -- never `pooled = max(pooled_candidates)` paired
# with a different endpoint's CI. Two cases flip which endpoint's own point
# is numerically larger, so only the case where total's point exceeds
# occurrence's can separate a `max()` mutant from the correct selection.
# ---------------------------------------------------------------------------

_O9B_DATES = _dates_n(24)
_O9B_LEADS = (1, 2, 3, 4)


def _o9b_occurrence() -> OccurrenceLead:
    """Candidate materially beats its opponent: ETS-diff point exactly 0.5,
    independent of the `total` endpoint's ratio (a separate endpoint)."""
    out: OccurrenceLead = {}
    for i, d in enumerate(_O9B_DATES[:12]):
        cand = "hit" if i < 10 else "miss"
        opp = "hit" if i < 6 else "miss"
        out[d] = (cand, opp)
    for i, d in enumerate(_O9B_DATES[12:]):
        cand = "correct_negative" if i < 10 else "false_alarm"
        opp = "correct_negative" if i < 6 else "false_alarm"
        out[d] = (cand, opp)
    return out


def _o9b_weak_baseline(occ: OccurrenceLead) -> OccurrenceLead:
    """A baseline that underperforms regardless of the candidate's own
    class mix (`_o8c_flip_baseline` assumes a perfect-skill candidate and
    KeyErrors here, since this candidate's own class includes "miss")."""
    out: OccurrenceLead = {}
    for i, d in enumerate(occ):
        out[d] = ("miss", "miss") if i < 12 else ("false_alarm", "false_alarm")
    return out


def _o9b_candidate(key: str, total_ratio: float) -> CandidateSeries:
    total = {ld: _o8_ratio_series(_O9B_DATES, total_ratio) for ld in _O9B_LEADS}
    occ = _o9b_occurrence()
    return CandidateSeries(
        key=key,
        continuous={"precip_total": total},
        occurrence={ld: dict(occ) for ld in _O9B_LEADS},
        baseline_continuous=continuous_baseline_set(
            "precip_total",
            {ld: _strong_baseline(s) for ld, s in total.items()},
        ),
        baseline_occurrence=occurrence_baseline_set(
            {ld: _o9b_weak_baseline(occ) for ld in _O9B_LEADS}
        ),
    )


def test_o9b_precip_ordering_tuple_is_atomic_when_both_endpoints_improve() -> None:
    # Case (a): total's own point (0.7) exceeds occurrence's own point
    # (0.5) -- the case that separates a `max()` mutant from F-4 selection.
    cand_a = _o9b_candidate("5", total_ratio=0.3)
    result_a = _decide_precip(cand_a, seed=1, resamples=40)
    total_a = cast(dict[str, object], result_a.record["total"])
    occ_a = cast(dict[str, object], result_a.record["occurrence"])
    conditions_a = cast(dict[str, object], result_a.record["conditions"])
    # The premise: both endpoints really did improve.
    assert conditions_a["improved_endpoints"] == ["total", "occurrence"]
    assert total_a["pooled_point"] == pytest.approx(0.7)
    assert occ_a["pooled_point"] == pytest.approx(0.5)
    assert total_a["pooled_point"] > occ_a["pooled_point"]
    # mutant -> at result_a.pooled: correct = 0.5 (occurrence's own point --
    # F-4 orders on occurrence whenever it is in `improved`), mutant
    # (`pooled = max(pooled_candidates)`) = 0.7 (total's own point, the
    # larger of the two candidates in this case).
    assert result_a.pooled == pytest.approx(occ_a["pooled_point"])
    assert result_a.ci is not None
    assert list(result_a.ci) == occ_a["ci"]
    assert result_a.ordering_endpoint_name == "occurrence"
    assert result_a.record["ordering_endpoint_name"] == "occurrence"

    # Case (b): total's own point (0.3) is now BELOW occurrence's own point
    # (0.5) -- the control, where `max()` and F-4 selection coincide.
    cand_b = _o9b_candidate("5", total_ratio=0.7)
    result_b = _decide_precip(cand_b, seed=1, resamples=40)
    total_b = cast(dict[str, object], result_b.record["total"])
    occ_b = cast(dict[str, object], result_b.record["occurrence"])
    conditions_b = cast(dict[str, object], result_b.record["conditions"])
    assert conditions_b["improved_endpoints"] == ["total", "occurrence"]
    assert total_b["pooled_point"] == pytest.approx(0.3)
    assert occ_b["pooled_point"] == pytest.approx(0.5)
    assert total_b["pooled_point"] < occ_b["pooled_point"]
    assert result_b.pooled == pytest.approx(occ_b["pooled_point"])
    assert result_b.ci is not None
    assert list(result_b.ci) == occ_b["ci"]
    assert result_b.ordering_endpoint_name == "occurrence"
    assert result_b.record["ordering_endpoint_name"] == "occurrence"


# ---------------------------------------------------------------------------
# O15 -- request-level companion: the window a decision-path candidate
# carries (`headline.window.days`) must reach the API response verbatim,
# not just the in-process ``Verdict.detail`` dict.
# ---------------------------------------------------------------------------


def _o15_verdict() -> Verdict:
    dates = _dates_n(24)
    leads = (1, 2, 3, 4)
    series = _ratio_series(dates, 0.5)
    cand = _wind_candidate("5", {ld: series for ld in leads})
    inputs = VariableInputs(variable="wind", incumbent_key="2", candidates=(cand,))
    return decide_variable(inputs, seed=1, resamples=40)


def test_o15_tested_family_window_days_reaches_the_verdicts_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fastapi.testclient import TestClient

    from wxverify import config
    from wxverify.api.app import create_app
    from wxverify.db.connection import close_db, init_db

    verdict = _o15_verdict()
    headline = _headline(verdict, "5")
    window = cast(dict[str, object], headline["window"])
    # Sanity: the decision path really does produce a non-trivial window
    # (otherwise the assertion below would be vacuously satisfied by two
    # absent/None values).
    assert window["days"] == 24

    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001

    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                               timezone, enabled)
            VALUES ('oracle-o15-town', 40.0, -105.0, 900.0, 'UTC', 1)
            """
        ).lastrowid
    )
    generation = ensure_published_generation(conn, site_id)
    run_id = _insert_run_row(conn, site_id, generation)
    tested_family = {"incumbent": "2", "candidates": verdict.detail["candidates"]}
    conn.execute(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, recommended_depth, incumbent_depth,
             tested_family)
        VALUES (?, 'wind', 'retain_incumbent', NULL, 2, ?)
        """,
        (run_id, json.dumps(tested_family)),
    )
    conn.commit()

    async def _idle_worker(_db: object) -> None:
        import asyncio

        await asyncio.Event().wait()

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        response = client.get(f"/api/verification/runs/{run_id}/verdicts")
    assert response.status_code == 200
    body = response.json()
    verdicts = cast(list[dict[str, object]], body["verdicts"])
    wind_verdict = next(v for v in verdicts if v["variable"] == "wind")
    tested_family_out = cast(dict[str, object], wind_verdict["tested_family"])
    candidates_out = cast(dict[str, object], tested_family_out["candidates"])
    candidate_out = cast(dict[str, object], candidates_out["5"])
    headline_out = cast(dict[str, object], candidate_out["headline"])
    window_out = cast(dict[str, object], headline_out["window"])
    # mutant -> at window_out["days"]: correct = 24 (the decision-path
    # window carried verbatim through the JSON column and the API's
    # dict-comprehension response), mutant (a response builder that
    # constructs its own field list from `row` columns instead of
    # `_parse_json(row["tested_family"])`, dropping "window") = a KeyError
    # or an absent "window" key -- either way the assertion below fails.
    assert window_out["days"] == 24
