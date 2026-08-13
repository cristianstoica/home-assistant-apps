"""§15/§18.11 oracle suite: per-variable depth resolution, clearing,
lockstep pinning, F-3 skipped boundary, F-1 measured-endpoint rule, and
the engine's fail-closed missing-incumbent guard (phase 7 QA).

Complements ``tests/test_phase7_surface.py`` (the implementer suite) with
the adversarial boundary oracles: valid overrides at DEPTH 1 and 6 (the
inclusive range bounds), an asymmetric three-variable override fixture
whose expected dict is hand-stated, the clearing rule's delete-vs-update
asymmetry, record/run-snapshot pinning across a mid-day override flip
(only post-flip artifacts move), the F-3 4-vs-5 range boundary on both
``preskipped_verdicts`` and ``prepare_bootstrap_inputs``, the paired F-1
positive/negative (measured flat total => recommend; absent total =>
mixed_by_quantity), and aggregate metrics that stay NULL — never zero —
when the pinned incumbent depth has no simulated rows.

Every expected value is hand-derived from the spec (§15 resolution order,
§12 decision rule, §17 constants); fixtures are synthetic (fake site
names, UTC, invented feed ids).
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

import pytest

from tests.helpers import asof_conn, asof_make_site
from wxverify.core.options import RuntimeOptions
from wxverify.db.connection import close_db, init_db
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.settings.depth import (
    EffectiveDepth,
    depth_override_key,
    effective_blend_depth,
    effective_blend_depths,
)
from wxverify.settings.keys import get_setting, set_setting
from wxverify.settings.service import apply_plain_settings
from wxverify.verification.decision import (
    CandidateSeries,
    ContinuousLead,
    OccurrenceLead,
    VariableInputs,
    decide_variable,
)
from wxverify.verification.engine import (
    aggregate_run,
    prepare_bootstrap_inputs,
    preskipped_verdicts,
)
from wxverify.verification.record import (
    build_forecast_record,
    parse_wall_clock,
    snapshot_wall_clock,
)
from wxverify.verification.runs import (
    RunConfig,
    capture_config_snapshot,
    run_config_from_row,
)

# ---------------------------------------------------------------------------
# Shared fixture builders
# ---------------------------------------------------------------------------


def _make_run_config(
    *,
    site_id: int = 1,
    run_id: int = 1,
    generation: int = 1,
    blend_depth: int = 2,
    blend_depths: dict[str, int],
) -> RunConfig:
    return RunConfig(
        site_id=site_id,
        run_id=run_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        wall_clock="07:00",
        blend_depth=blend_depth,
        blend_depths=blend_depths,
        min_n=30,
        window_days=30,
        tz_generation_id=generation,
        roster=(),
        period_start="2026-05-01",
        period_end="2026-05-30",
        bootstrap_seed=1,
        bootstrap_resamples=40,
    )


def _insert_run(
    conn: sqlite3.Connection,
    site_id: int,
    generation: int,
    *,
    config_snapshot: str = "{}",
    aggregate_state: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, period_start, period_end,
             settled_through, bootstrap_seed, bootstrap_resamples,
             input_fingerprint, aggregate_state)
        VALUES (?, ?, 1, 'test', 'running', 1, ?, '2026-05-01',
                '2026-05-30', '2026-05-30', 1, 40, 'fp-' || ?, ?)
        """,
        (site_id, generation, config_snapshot, site_id, aggregate_state),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


# ---------------------------------------------------------------------------
# §15 — resolution-order boundaries the implementer suite leaves open:
# overrides at exactly DEPTH_MIN=1 and DEPTH_MAX=6 are VALID (the range is
# inclusive on both ends; test_phase7_surface.py pins only that 0 and 7
# fall through).
# ---------------------------------------------------------------------------


def test_override_boundary_values_one_and_six_are_valid() -> None:
    conn = asof_conn()
    set_setting(conn, depth_override_key("temperature"), "1")
    set_setting(conn, depth_override_key("wind"), "6")
    assert effective_blend_depth(conn, "temperature") == EffectiveDepth(
        depth=1, source="override"
    )
    assert effective_blend_depth(conn, "wind") == EffectiveDepth(
        depth=6, source="override"
    )


def test_asymmetric_overrides_resolve_per_variable_not_globally() -> None:
    # Divergent fixture: global 3; temperature overridden to 1, precip to
    # 6, wind override INVALID ("7", out of range) -> global fall-through.
    # Expected dict hand-stated, never derived from the helper.
    conn = asof_conn()
    set_setting(conn, "forecast_blend_depth", "3")
    set_setting(conn, depth_override_key("temperature"), "1")
    set_setting(conn, depth_override_key("wind"), "7")
    set_setting(conn, depth_override_key("precip"), "6")
    assert effective_blend_depths(conn) == {
        "temperature": EffectiveDepth(depth=1, source="override"),
        "wind": EffectiveDepth(depth=3, source="global"),
        "precip": EffectiveDepth(depth=6, source="override"),
    }


def test_snapshot_pins_asymmetric_depths_with_hand_stated_dicts() -> None:
    conn = asof_conn()
    site_id = asof_make_site(conn, "Depth Pin Town")
    set_setting(conn, "forecast_blend_depth", "3")
    set_setting(conn, depth_override_key("temperature"), "1")
    set_setting(conn, depth_override_key("precip"), "6")
    snapshot = capture_config_snapshot(conn, site_id)
    assert snapshot["blend_depths"] == {"temperature": 1, "wind": 3, "precip": 6}
    assert snapshot["blend_depth_sources"] == {
        "temperature": "override",
        "wind": "global",
        "precip": "override",
    }


def test_snapshot_wall_clock_is_canonical_hh_mm_in_the_pinned_run_snapshot() -> None:
    """§15: the resolved snapshot time is canonicalised to bare ``HH:MM``.

    ``parse_wall_clock`` accepts a padded value (``int(" 05")`` succeeds), so
    a settings row written with stray whitespace resolves successfully — and
    without canonicalisation the padded text would be pinned verbatim into
    the run's config snapshot and stamped on the record identity, where it
    compares unequal to the same time written cleanly. Paired positive: a
    clean value is returned unchanged, so this is not a "strip everything"
    assertion but a canonical-form one.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Canonical Clock Town")
    set_setting(conn, "record_snapshot_local_time", "06:30")
    assert snapshot_wall_clock(conn, site_id) == "06:30"  # clean: unchanged

    set_setting(conn, f"record_snapshot_local_time:{site_id}", " 05:45 ")
    assert parse_wall_clock(" 05:45 ") == (5, 45)  # the padded row IS valid
    assert snapshot_wall_clock(conn, site_id) == "05:45"
    snapshot = capture_config_snapshot(conn, site_id)
    assert snapshot["wall_clock"] == "05:45"
    assert json.dumps(snapshot).count(" 05:45 ") == 0


# ---------------------------------------------------------------------------
# §15 clearing rule — the delete-vs-update asymmetry inside ONE apply:
# a key absent from the apply is DELETED, a key present is UPDATED, and the
# plain global key keeps apply-when-present semantics (never cleared).
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_path_db(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    """A real tmp-file Database wired into the module-global accessor so
    the async settings writers hit it; torn down on every exit path."""
    close_db()
    db = init_db(str(tmp_path / "wxverify.db"))
    yield db._conn  # noqa: SLF001
    close_db()


def test_clearing_deletes_absent_keys_updates_present_keeps_plain(
    tmp_path_db: sqlite3.Connection,
) -> None:
    conn = tmp_path_db
    asyncio.run(
        apply_plain_settings(
            RuntimeOptions(
                forecast_blend_depth=4,
                forecast_blend_depth_wind=4,
                forecast_blend_depth_precip=6,
            )
        )
    )
    assert get_setting(conn, "forecast_blend_depth") == "4"
    assert get_setting(conn, depth_override_key("wind")) == "4"
    assert get_setting(conn, depth_override_key("precip")) == "6"

    # Second apply: wind absent (-> DELETE), precip present (-> UPDATE to
    # 5), plain global absent (-> RETAINED, apply-when-present).
    asyncio.run(apply_plain_settings(RuntimeOptions(forecast_blend_depth_precip=5)))
    assert get_setting(conn, depth_override_key("wind")) is None
    assert get_setting(conn, depth_override_key("precip")) == "5"
    assert get_setting(conn, "forecast_blend_depth") == "4"
    assert effective_blend_depth(conn, "wind") == EffectiveDepth(
        depth=4, source="global"
    )
    assert effective_blend_depth(conn, "precip") == EffectiveDepth(
        depth=5, source="override"
    )


# ---------------------------------------------------------------------------
# §15 wall-clock parse — INCLUSIVE upper bounds. The implementer suite pins
# the invalid side (24:00, 07:60 rejected); this is the paired positive
# that goes red if the bounds tighten (<= mutated to <).
# ---------------------------------------------------------------------------


def test_parse_wall_clock_inclusive_boundaries() -> None:
    assert parse_wall_clock("23:59") == (23, 59)
    assert parse_wall_clock("00:00") == (0, 0)
    assert parse_wall_clock("23:00") == (23, 0)
    assert parse_wall_clock("00:59") == (0, 59)


# ---------------------------------------------------------------------------
# §15 lockstep — the record builder and the run snapshot pin the SAME
# effective depths, and a mid-day override flip moves only post-flip
# artifacts: the day-1 record rows and the day-1 run config stay pinned.
# ---------------------------------------------------------------------------

_DAY1 = date(2035, 6, 15)
_DAY2 = date(2035, 6, 16)


def _record_policy(
    conn: sqlite3.Connection, site_id: int, snapshot_local_date: str
) -> dict[str, object]:
    row = conn.execute(
        """
        SELECT policy FROM forecast_of_record
        WHERE site_id = ? AND snapshot_local_date = ?
        ORDER BY id LIMIT 1
        """,
        (site_id, snapshot_local_date),
    ).fetchone()
    assert row is not None
    return cast(dict[str, object], json.loads(str(row["policy"])))


def test_record_and_run_snapshot_pin_depths_flip_moves_only_post_flip() -> None:
    conn = asof_conn()
    site_id = asof_make_site(conn, "Lockstep Town")
    generation = ensure_published_generation(conn, site_id)
    set_setting(conn, depth_override_key("precip"), "1")

    # Day-1 record at T+1h under the precip=1 override.
    t1 = datetime(2035, 6, 15, 8, 0, tzinfo=UTC)
    build_forecast_record(conn, site_id, _DAY1.isoformat(), now=t1)
    policy1 = _record_policy(conn, site_id, _DAY1.isoformat())
    assert policy1["blend_depths"] == {"temperature": 2, "wind": 2, "precip": 1}
    assert policy1["blend_depth_sources"] == {
        "temperature": "global",
        "wind": "global",
        "precip": "override",
    }

    # Day-1 run pinned under the same settings.
    snapshot = capture_config_snapshot(conn, site_id)
    assert snapshot["blend_depths"] == policy1["blend_depths"]
    run_id = _insert_run(
        conn, site_id, generation, config_snapshot=json.dumps(snapshot)
    )
    conn.commit()

    # Flip the override mid-day. The pinned run config must NOT re-read
    # live settings; the immutable day-1 record must not move.
    set_setting(conn, depth_override_key("precip"), "3")
    cfg = run_config_from_row(conn, run_id)
    assert cfg.blend_depths == {"temperature": 2, "wind": 2, "precip": 1}
    assert cfg.incumbent_depth("precip") == 1
    assert cfg.incumbent_depth("wind") == 2

    t2 = datetime(2035, 6, 16, 8, 0, tzinfo=UTC)
    build_forecast_record(conn, site_id, _DAY2.isoformat(), now=t2)
    policy2 = _record_policy(conn, site_id, _DAY2.isoformat())
    assert policy2["blend_depths"] == {"temperature": 2, "wind": 2, "precip": 3}
    # Day-1 rows are append-only and still carry the pre-flip depth.
    assert _record_policy(conn, site_id, _DAY1.isoformat())["blend_depths"] == {
        "temperature": 2,
        "wind": 2,
        "precip": 1,
    }


def test_parse_blend_depths_pre_015_snapshot_inherits_pinned_global() -> None:
    # A pre-§15 run whose snapshot has no blend_depths: every variable
    # inherits the PINNED global (4), not the current live setting (2).
    conn = asof_conn()
    site_id = asof_make_site(conn, "Legacy Run Town")
    generation = ensure_published_generation(conn, site_id)
    legacy = {
        "timezone": "UTC",
        "rain_threshold_mm": 0.2,
        "wall_clock": "07:00",
        "blend_depth": 4,
        "min_n": 30,
        "window_days": 30,
        "tz_generation_id": generation,
        "roster": [],
    }
    run_id = _insert_run(conn, site_id, generation, config_snapshot=json.dumps(legacy))
    cfg = run_config_from_row(conn, run_id)
    assert cfg.blend_depths == {"temperature": 4, "wind": 4, "precip": 4}
    assert cfg.incumbent_depth("temperature") == 4


# ---------------------------------------------------------------------------
# F-3 — the SIM_DEPTHS range boundary (4 in, 5/6 out) on the skip path.
# ---------------------------------------------------------------------------


def test_preskipped_verdicts_boundary_four_in_five_six_out() -> None:
    cfg = _make_run_config(blend_depths={"temperature": 4, "wind": 5, "precip": 6})
    skipped = preskipped_verdicts(cfg)
    # Depth 4 is the LAST simulated depth: temperature must not skip.
    assert [(v.variable, v.outcome) for v in skipped] == [
        ("wind", "skipped"),
        ("precip", "skipped"),
    ]
    assert skipped[0].detail == {
        "incumbent": "5",
        "reason": "incumbent_depth_out_of_simulated_range",
        "simulated_depths": [1, 2, 3, 4],
    }
    assert skipped[1].detail["incumbent"] == "6"
    assert all(v.recommended_key is None for v in skipped)
    # All-in-range config: no skip rows at all.
    assert (
        preskipped_verdicts(
            _make_run_config(blend_depths={"temperature": 1, "wind": 2, "precip": 4})
        )
        == []
    )


def test_prepare_bootstrap_inputs_skips_out_of_range_variable_only() -> None:
    conn = asof_conn()
    site_id = asof_make_site(conn, "Skip Town")
    generation = ensure_published_generation(conn, site_id)
    run_id = _insert_run(conn, site_id, generation, aggregate_state="{}")
    cfg = _make_run_config(
        site_id=site_id,
        run_id=run_id,
        generation=generation,
        blend_depths={"temperature": 4, "wind": 2, "precip": 5},
    )
    inputs = prepare_bootstrap_inputs(conn, cfg)
    assert [i.variable for i in inputs] == ["temperature", "wind"]
    by_var = {i.variable: i for i in inputs}
    # The incumbent is the variable's own pinned depth, and it is never
    # its own candidate.
    assert by_var["temperature"].incumbent_key == "4"
    assert [c.key for c in by_var["temperature"].candidates] == ["1", "2", "3"]
    assert by_var["wind"].incumbent_key == "2"
    assert [c.key for c in by_var["wind"].candidates] == ["1", "3", "4"]


def test_verdicts_check_constraint_accepts_skipped_rejects_unknown() -> None:
    conn = asof_conn()
    site_id = asof_make_site(conn, "Check Town")
    generation = ensure_published_generation(conn, site_id)
    run_id = _insert_run(conn, site_id, generation)

    def insert(outcome: str, variable: str) -> None:
        conn.execute(
            """
            INSERT INTO verification_verdicts
                (run_id, variable, outcome, recommended_depth,
                 incumbent_depth, tested_family)
            VALUES (?, ?, ?, NULL, 5, '[]')
            """,
            (run_id, variable, outcome),
        )

    insert("skipped", "precip")  # must be accepted post-migration
    with pytest.raises(sqlite3.IntegrityError):
        insert("not_an_outcome", "wind")


# ---------------------------------------------------------------------------
# F-1 — paired positive/negative: recommending on the occurrence endpoint
# requires a MEASURED, non-inferior total endpoint. A flat equal-error
# total (effect exactly 0 >= -0.02) admits the recommendation; an absent
# total blocks it into mixed_by_quantity with improved_endpoints == [].
# This pair also guards the premise of the F-1 fixture change inside
# tests/test_verification_occurrence_oracles.py::T7.
# ---------------------------------------------------------------------------

_F1_DATES = [f"2026-07-{d:02d}" for d in range(1, 26)]  # 25 days


def _f1_occurrence_series() -> tuple[OccurrenceLead, OccurrenceLead]:
    """25 days, 8 wet (6 hit + 2 miss) / 17 dry (1 fa + 16 cn), wet days
    spread every 3rd index; the opponent misses every wet day and false-
    alarms on 6 dry days — hand-derived material candidate improvement
    (cand ETS 94/169 vs opp ETS -24/151, same construction as the phase-6
    T7 recommend case)."""
    wet_idx: list[int] = []
    order = list(range(0, 25, 3)) + [i for i in range(25) if i % 3 != 0]
    for i in order:
        if len(wet_idx) == 8:
            break
        wet_idx.append(i)
    wet_set = set(wet_idx)
    dry_idx = [i for i in range(25) if i not in wet_set]
    cand_label: dict[int, str] = {}
    for k, i in enumerate(sorted(wet_idx)):
        cand_label[i] = "hit" if k < 6 else "miss"
    for k, i in enumerate(dry_idx):
        cand_label[i] = "false_alarm" if k < 1 else "correct_negative"
    opp_fa = set(dry_idx[:6])
    vs_incumbent: OccurrenceLead = {}
    vs_always_dry: OccurrenceLead = {}
    for i, d in enumerate(_F1_DATES):
        cand = cand_label[i]
        if i in wet_set:
            opp, base = "miss", "miss"
        else:
            opp = "false_alarm" if i in opp_fa else "correct_negative"
            base = "correct_negative"
        vs_incumbent[d] = (cand, opp)
        vs_always_dry[d] = (cand, base)
    return vs_incumbent, vs_always_dry


def _f1_candidate(*, with_flat_total: bool) -> CandidateSeries:
    vs_incumbent, vs_always_dry = _f1_occurrence_series()
    flat_total: ContinuousLead = {d: (2.0, 2.0) for d in _F1_DATES}
    continuous = (
        {"precip_total": {ld: dict(flat_total) for ld in range(1, 8)}}
        if with_flat_total
        else {}
    )
    return CandidateSeries(
        key="3",
        continuous=continuous,
        occurrence={ld: dict(vs_incumbent) for ld in range(1, 8)},
        baseline_occurrence={
            "baseline_always_dry": {ld: dict(vs_always_dry) for ld in range(1, 8)}
        },
    )


def test_f1_measured_flat_total_admits_occurrence_recommendation() -> None:
    inputs = VariableInputs(
        variable="precip",
        incumbent_key="2",
        candidates=(_f1_candidate(with_flat_total=True),),
    )
    verdict = decide_variable(inputs, seed=20260713, resamples=400)
    assert verdict.outcome == "recommend"
    assert verdict.recommended_key == "3"
    record = cast(
        dict[str, object],
        cast(dict[str, object], verdict.detail["candidates"])["3"],
    )
    conditions = cast(dict[str, object], record["conditions"])
    assert conditions["improved_endpoints"] == ["occurrence"]
    assert conditions["total_non_inferior"] is True


def test_f1_unmeasured_total_blocks_recommendation_to_mixed() -> None:
    inputs = VariableInputs(
        variable="precip",
        incumbent_key="2",
        candidates=(_f1_candidate(with_flat_total=False),),
    )
    verdict = decide_variable(inputs, seed=20260713, resamples=400)
    assert verdict.outcome == "mixed_by_quantity"
    assert verdict.recommended_key is None
    record = cast(
        dict[str, object],
        cast(dict[str, object], verdict.detail["candidates"])["3"],
    )
    conditions = cast(dict[str, object], record["conditions"])
    assert conditions["occurrence_material"] is True
    assert conditions["total_non_inferior"] is False
    assert conditions["improved_endpoints"] == []


def test_f1_unmeasured_occurrence_blocks_total_recommendation() -> None:
    # The symmetric F-1 arm: a clearly material total improvement (constant
    # candidate 1.0 vs incumbent 2.0 abs error -> relative improvement
    # exactly 0.5 on every resample) with NO occurrence series. The
    # unmeasurable occurrence endpoint must block the recommendation.
    material_total: ContinuousLead = {d: (1.0, 2.0) for d in _F1_DATES}
    candidate = CandidateSeries(
        key="3",
        continuous={"precip_total": {ld: dict(material_total) for ld in range(1, 8)}},
        occurrence={},
    )
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(candidate,)
    )
    verdict = decide_variable(inputs, seed=20260713, resamples=400)
    assert verdict.outcome == "mixed_by_quantity"
    assert verdict.recommended_key is None
    record = cast(
        dict[str, object],
        cast(dict[str, object], verdict.detail["candidates"])["3"],
    )
    conditions = cast(dict[str, object], record["conditions"])
    assert conditions["total_material"] is True
    assert conditions["occurrence_non_inferior"] is False
    assert conditions["improved_endpoints"] == []


# ---------------------------------------------------------------------------
# Fail-closed engine guard — a depth-5 incumbent has no simulated rows, so
# every vs-incumbent metric must stay NULL (never fabricated zero) while
# each entity's OWN metrics are still computed; the paired positive shows
# the same fixture with an in-range incumbent defines the delta.
# ---------------------------------------------------------------------------

_FC_DAYS = ["2026-07-01", "2026-07-02", "2026-07-03"]


def _seed_wind_evidence(conn: sqlite3.Connection, run_id: int) -> None:
    # truth 10.0 on every day. Hand-derived per-depth errors:
    #   depth 1: 13,13,13 -> +3,+3,+3          MAE 3.0
    #   depth 2: 12, 6,16 -> +2,-4,+6          MAE 4.0
    #   depth 3: 11, 8,13 -> +1,-2,+3          MAE 2.0
    #   depth 4: 15,15,15 -> +5,+5,+5          MAE 5.0
    values = {
        "1": [13.0, 13.0, 13.0],
        "2": [12.0, 6.0, 16.0],
        "3": [11.0, 8.0, 13.0],
        "4": [15.0, 15.0, 15.0],
    }
    for key, preds in values.items():
        for day, predicted in zip(_FC_DAYS, preds, strict=True):
            conn.execute(
                """
                INSERT INTO verification_evidence
                    (run_id, snapshot_local_date, target_local_date, lead,
                     variable, quantity, entity_type, entity_key, predicted,
                     forecast_eligible, truth_value, truth_eligible,
                     abs_error)
                VALUES (?, ?, ?, 1, 'wind', 'wind_max', 'depth', ?, ?, 1,
                        10.0, 1, ?)
                """,
                (run_id, day, day, key, predicted, abs(predicted - 10.0)),
            )


def _wind_result(conn: sqlite3.Connection, run_id: int, entity_key: str) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT * FROM verification_results
        WHERE run_id = ? AND quantity = 'wind_max' AND lead = 1
          AND entity_type = 'depth' AND entity_key = ?
        """,
        (run_id, entity_key),
    ).fetchone()
    assert row is not None
    return cast(sqlite3.Row, row)


def test_aggregate_out_of_range_incumbent_keeps_delta_null_never_zero() -> None:
    conn = asof_conn()
    site_id = asof_make_site(conn, "Fail Closed Town")
    generation = ensure_published_generation(conn, site_id)
    run_id = _insert_run(conn, site_id, generation)
    _seed_wind_evidence(conn, run_id)
    conn.commit()
    cfg = _make_run_config(
        site_id=site_id,
        run_id=run_id,
        generation=generation,
        blend_depths={"temperature": 2, "wind": 5, "precip": 2},
    )
    aggregate_run(conn, cfg)
    conn.commit()
    depth3 = _wind_result(conn, run_id, "3")
    # Own metrics computed on the 3-day core...
    assert int(depth3["common_days"]) == 3
    assert float(depth3["mae"]) == pytest.approx(2.0)
    # ...but the vs-incumbent delta is NULL: depth 5 has no rows, and the
    # comparison must fail closed rather than fabricate 0.0.
    assert depth3["delta_vs_incumbent"] is None
    assert _wind_result(conn, run_id, "1")["delta_vs_incumbent"] is None


def test_aggregate_in_range_incumbent_defines_delta_paired_positive() -> None:
    conn = asof_conn()
    site_id = asof_make_site(conn, "Fail Closed Control Town")
    generation = ensure_published_generation(conn, site_id)
    run_id = _insert_run(conn, site_id, generation)
    _seed_wind_evidence(conn, run_id)
    conn.commit()
    cfg = _make_run_config(
        site_id=site_id,
        run_id=run_id,
        generation=generation,
        blend_depths={"temperature": 2, "wind": 2, "precip": 2},
    )
    aggregate_run(conn, cfg)
    conn.commit()
    depth3 = _wind_result(conn, run_id, "3")
    # Incumbent depth 2 MAE 4.0, candidate MAE 2.0: delta = (4-2)/4 = 0.5.
    assert float(depth3["delta_vs_incumbent"]) == pytest.approx(0.5)


def test_aggregate_occurrence_absent_incumbent_ets_defined_delta_null() -> None:
    conn = asof_conn()
    site_id = asof_make_site(conn, "Occ Fail Closed Town")
    generation = ensure_published_generation(conn, site_id)
    run_id = _insert_run(conn, site_id, generation)
    # Hand-derived candidate table over 3 days: hit, correct_negative, hit
    # -> h=2 cn=1, hits_random = 2*2/3, denominator = 2 - 4/3 = 2/3,
    # ETS = (2 - 4/3)/(2/3) = 1.0 exactly.
    outcomes = {
        "1": ["hit", "correct_negative", "hit"],
        "2": ["miss", "correct_negative", "hit"],
        "3": ["hit", "correct_negative", "hit"],
        "4": ["miss", "false_alarm", "miss"],
    }
    for key, labels in outcomes.items():
        for day, outcome in zip(_FC_DAYS, labels, strict=True):
            wet = outcome in ("hit", "miss")
            conn.execute(
                """
                INSERT INTO verification_evidence
                    (run_id, snapshot_local_date, target_local_date, lead,
                     variable, quantity, entity_type, entity_key, predicted,
                     forecast_eligible, truth_value, truth_eligible,
                     abs_error, occurrence_outcome)
                VALUES (?, ?, ?, 1, 'precip', 'precip_occurrence', 'depth',
                        ?, ?, 1, ?, 1, NULL, ?)
                """,
                (
                    run_id,
                    day,
                    day,
                    key,
                    1.0,
                    1.0 if wet else 0.0,
                    outcome,
                ),
            )
    conn.commit()
    cfg = _make_run_config(
        site_id=site_id,
        run_id=run_id,
        generation=generation,
        blend_depths={"temperature": 2, "wind": 2, "precip": 6},
    )
    aggregate_run(conn, cfg)  # must not crash on the absent incumbent
    conn.commit()
    row = conn.execute(
        """
        SELECT * FROM verification_results
        WHERE run_id = ? AND quantity = 'precip_occurrence' AND lead = 1
          AND entity_type = 'depth' AND entity_key = '3'
        """,
        (run_id,),
    ).fetchone()
    assert row is not None
    assert float(row["ets"]) == pytest.approx(1.0)
    assert int(row["hits"]) == 2
    assert int(row["correct_negatives"]) == 1
    assert row["delta_vs_incumbent"] is None
