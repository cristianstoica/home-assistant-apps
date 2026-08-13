"""Implementation tests for the phase-6 verification-run engine (§8-§14).

Covers the pure statistics helpers, the §12 decision rule on deterministic
synthetic series, run pinning/fingerprint semantics, the end-to-end sync
chain (regen → decide → start → simulate → aggregate → bootstrap →
publish) against a real migrated in-memory database, the nightly scheduler
trigger, and the F4/F5 cleanup widening. All fixture values are synthetic
(fake sources/models, UTC site, invented coordinates).
"""

from __future__ import annotations

import json
import random
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import asof_conn, asof_insert_pair, asof_make_real_feed
from wxverify.db.queue import Job
from wxverify.db.runtime_state import get_runtime_state
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.settings.keys import set_setting
from wxverify.verification.decision import (
    CandidateSeries,
    ContinuousLead,
    OccurrenceLead,
    VariableInputs,
    decide_variable,
)
from wxverify.verification.engine import prepare_bootstrap_inputs, preskipped_verdicts
from wxverify.verification.runs import (
    capture_config_snapshot,
    input_fingerprint,
    published_run_id,
    roster_feeds,
    run_config_from_row,
    seed_from_fingerprint,
)
from wxverify.verification.stats import (
    Contingency,
    classify_occurrence,
    ets,
    mae,
    moving_block_indices,
    percentile_ci,
)
from wxverify.worker import scheduler as scheduler_module
from wxverify.worker.scheduler import _enqueue_due_verification_runs  # noqa: SLF001
from wxverify.worker.tz_correction import _cleanup_chunk  # noqa: SLF001
from wxverify.worker.verification_run import (
    _compute_verdicts,  # noqa: SLF001
    _load_state,  # noqa: SLF001
    _persist_verdicts,  # noqa: SLF001
    advance_verification,
    mark_verification_failed,
    verification_job_key,
    verification_state_key,
)

# ---------------------------------------------------------------------------
# stats.py
# ---------------------------------------------------------------------------


def test_error_metrics_and_classification() -> None:
    assert mae([1.0, -3.0]) == 2.0
    assert classify_occurrence(True, True) == "hit"
    assert classify_occurrence(True, False) == "false_alarm"
    assert classify_occurrence(False, True) == "miss"
    assert classify_occurrence(False, False) == "correct_negative"


def test_ets_known_value_and_undefined_cases() -> None:
    # Classic 2x2: hits=20 misses=10 false_alarms=5 cn=65, n=100.
    table = Contingency(hits=20, misses=10, false_alarms=5, correct_negatives=65)
    hits_random = 30 * 25 / 100  # 7.5
    expected = (20 - hits_random) / (20 + 10 + 5 - hits_random)
    value = ets(table)
    assert value is not None and value == pytest.approx(expected)
    assert ets(Contingency()) is None
    # All-one-class table: denominator collapses to zero.
    assert ets(Contingency(correct_negatives=30)) is None


def test_moving_block_indices_are_deterministic_blocks() -> None:
    a = moving_block_indices(random.Random(7), 10, 3)
    b = moving_block_indices(random.Random(7), 10, 3)
    assert a == b
    assert len(a) == 10
    assert all(0 <= i < 10 for i in a)
    # Every full block is 3 CONSECUTIVE indices (the clustering property).
    for start in range(0, 9, 3):
        block = a[start : start + 3]
        assert block == list(range(block[0], block[0] + 3))
    # n shorter than one block still yields n indices.
    assert len(moving_block_indices(random.Random(1), 2, 3)) == 2
    assert moving_block_indices(random.Random(1), 0, 3) == []


def test_percentile_ci_bounds() -> None:
    samples = [float(v) for v in range(1, 101)]
    lo, hi = percentile_ci(samples, 0.95)
    assert 1.0 <= lo < hi <= 100.0
    assert lo == pytest.approx(3.475)
    assert hi == pytest.approx(97.525)


# ---------------------------------------------------------------------------
# decision.py — deterministic synthetic series
# ---------------------------------------------------------------------------

_DATES = [f"2026-05-{d:02d}" for d in range(1, 31)]


def _flat_continuous(cand: float, opp: float, days: int = 30) -> ContinuousLead:
    return {d: (cand, opp) for d in _DATES[:days]}


def _wind_candidate(
    key: str, *, cand: float, opp: float, base_opp: float, days: int = 30
) -> CandidateSeries:
    leads = {lead: _flat_continuous(cand, opp, days) for lead in range(1, 8)}
    base_leads = {lead: _flat_continuous(cand, base_opp, days) for lead in range(1, 8)}
    return CandidateSeries(
        key=key,
        continuous={"wind_max": leads},
        baseline_continuous={
            "baseline_persistence": {"wind_max": base_leads},
            "baseline_all_feed_mean": {"wind_max": base_leads},
        },
    )


def test_decision_recommends_a_clear_winner() -> None:
    inputs = VariableInputs(
        variable="wind",
        incumbent_key="2",
        candidates=(_wind_candidate("3", cand=1.0, opp=2.0, base_opp=3.0),),
    )
    verdict = decide_variable(inputs, seed=42, resamples=60)
    assert verdict.outcome == "recommend"
    assert verdict.recommended_key == "3"
    conditions = verdict.detail["candidates"]["3"]["conditions"]  # type: ignore[index]
    assert conditions == {
        "ci_excludes_zero": True,
        "lead_stability": True,
        "practical_floor": True,
        "beats_baselines": True,
        "components_non_inferior": True,
    }


def test_decision_retains_incumbent_on_no_signal() -> None:
    inputs = VariableInputs(
        variable="wind",
        incumbent_key="2",
        candidates=(_wind_candidate("3", cand=2.0, opp=2.0, base_opp=2.0),),
    )
    verdict = decide_variable(inputs, seed=42, resamples=60)
    assert verdict.outcome == "retain_incumbent"
    assert verdict.recommended_key is None


def test_decision_insufficient_evidence_below_day_floor() -> None:
    inputs = VariableInputs(
        variable="wind",
        incumbent_key="2",
        candidates=(_wind_candidate("3", cand=1.0, opp=2.0, base_opp=3.0, days=10),),
    )
    verdict = decide_variable(inputs, seed=42, resamples=60)
    assert verdict.outcome == "insufficient_evidence"


def test_decision_baseline_gate_blocks_without_baseline_series() -> None:
    candidate = CandidateSeries(
        key="3",
        continuous={
            "wind_max": {lead: _flat_continuous(1.0, 2.0) for lead in range(1, 8)}
        },
    )
    inputs = VariableInputs(variable="wind", incumbent_key="2", candidates=(candidate,))
    verdict = decide_variable(inputs, seed=42, resamples=60)
    assert verdict.outcome == "retain_incumbent"


def test_decision_precip_occurrence_path() -> None:
    wet = set(_DATES[:10])
    occ: OccurrenceLead = {}
    base: OccurrenceLead = {}
    for d in _DATES:
        if d in wet:
            occ[d] = ("hit", "miss")
            base[d] = ("hit", "miss")
        else:
            occ[d] = ("correct_negative", "false_alarm")
            base[d] = ("correct_negative", "correct_negative")
    # F-1: the total endpoint must be MEASURED non-inferior (a candidate
    # with no precip_total series can no longer recommend vacuously), so
    # the fixture carries a flat, equal-error total series (effect = 0).
    candidate = CandidateSeries(
        key="1",
        continuous={
            "precip_total": {lead: _flat_continuous(2.0, 2.0) for lead in range(1, 8)}
        },
        occurrence={lead: dict(occ) for lead in range(1, 8)},
        baseline_occurrence={
            "baseline_always_dry": {lead: dict(base) for lead in range(1, 8)}
        },
    )
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(candidate,)
    )
    verdict = decide_variable(inputs, seed=9, resamples=60)
    assert verdict.outcome == "recommend"
    assert verdict.recommended_key == "1"


def test_decision_wet_day_starved_total_alone_cannot_recommend() -> None:
    """F-1 dedicated oracle: a materially improved precip_total with an
    UNMEASURED occurrence endpoint must not recommend — the vacuous
    non-inferiority read (absent point treated as non-inferior) is the
    exact bug F-1 fixed, so pre-fix this fixture recommends and post-fix
    it lands in mixed_by_quantity."""
    leads = {lead: _flat_continuous(1.0, 2.0) for lead in range(1, 8)}
    base_leads = {lead: _flat_continuous(1.0, 3.0) for lead in range(1, 8)}
    candidate = CandidateSeries(
        key="1",
        continuous={"precip_total": leads},
        baseline_continuous={
            "baseline_persistence": {"precip_total": base_leads},
            "baseline_all_feed_mean": {"precip_total": base_leads},
        },
        # Wet-day-starved: no occurrence series at all -> the occurrence
        # endpoint has no measurable effect at any lead.
    )
    inputs = VariableInputs(
        variable="precip", incumbent_key="2", candidates=(candidate,)
    )
    verdict = decide_variable(inputs, seed=7, resamples=60)
    assert verdict.outcome != "recommend"
    assert verdict.outcome == "mixed_by_quantity"


def test_decision_is_deterministic() -> None:
    inputs = VariableInputs(
        variable="wind",
        incumbent_key="2",
        candidates=(_wind_candidate("3", cand=1.5, opp=2.0, base_opp=2.5),),
    )
    first = decide_variable(inputs, seed=1234, resamples=80)
    second = decide_variable(inputs, seed=1234, resamples=80)
    assert first == second


# ---------------------------------------------------------------------------
# Fixture: a small but real site with two feeds, truth, and hourly samples
# ---------------------------------------------------------------------------

_PERIOD_DAYS = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05"]
_QUANTITY_VALUES = {
    "temperature_high": 21.0,
    "temperature_low": 9.0,
    "wind_max": 6.0,
    "precip_total": 0.0,
    "precip_occurrence": 0.0,
}


def _make_verification_site(conn: sqlite3.Connection) -> tuple[int, list[int]]:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('verify-town', 47.0, 25.0, 900.0, 'UTC')
        """
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    feeds = [
        asof_make_real_feed(conn, "model-alpha"),
        asof_make_real_feed(conn, "model-beta"),
    ]
    generation_id = ensure_published_generation(conn, site_id)
    for day in _PERIOD_DAYS:
        for quantity, value in _QUANTITY_VALUES.items():
            conn.execute(
                """
                INSERT INTO daily_truth
                    (site_id, local_date, quantity, value, eligible,
                     covered_hours, expected_slots, wet_hours, dry_hours,
                     rain_threshold_mm, day_start_utc, day_end_utc, timezone,
                     tz_generation_id)
                VALUES (?, ?, ?, ?, 1, 24, 24, ?, ?, 0.2, ?, ?, 'UTC', ?)
                """,
                (
                    site_id,
                    day,
                    quantity,
                    value,
                    0 if quantity.startswith("precip") else None,
                    24 if quantity.startswith("precip") else None,
                    f"{day}T00:00:00Z",
                    f"{day}T23:59:59Z",
                    generation_id,
                ),
            )
    values = {"temperature": 15.0, "wind": 5.0, "precip": 0.0}
    issued = "2026-05-31T05:00:00Z"
    for feed_index, feed_id in enumerate(feeds):
        for variable, base in values.items():
            for day_offset in range(len(_PERIOD_DAYS) + 8):
                for hour in range(24):
                    total_hours = day_offset * 24 + hour
                    valid = datetime(2026, 6, 1, tzinfo=UTC) + timedelta(
                        hours=total_hours
                    )
                    conn.execute(
                        """
                        INSERT INTO forecast_samples
                            (site_id, feed_id, variable, issued_at, valid_at,
                             lead_hours, value, source_raw, model_run_id,
                             fetched_at)
                        VALUES (?, ?, ?, ?, ?, 6, ?, '{}', 'run-a', ?)
                        """,
                        (
                            site_id,
                            feed_id,
                            variable,
                            issued,
                            valid.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            base + 0.5 * feed_index,
                            issued,
                        ),
                    )
    conn.commit()
    return site_id, feeds


def _drive_chain(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    *,
    resamples: int = 40,
    max_steps: int = 300,
) -> None:
    """Run the sync chain to completion, emulating the async bootstrap phase."""
    for _ in range(max_steps):
        blob = _load_state(conn, site_id)
        if blob is not None and blob.get("phase") == "bootstrap":
            run_id = blob["run_id"]
            assert isinstance(run_id, int)
            cfg = run_config_from_row(conn, run_id)
            inputs = prepare_bootstrap_inputs(conn, cfg)
            verdicts = _compute_verdicts(inputs, cfg.bootstrap_seed, resamples)
            # Mirror the async worker exactly (§15/F-3): out-of-range
            # incumbents get explicit 'skipped' verdicts appended.
            verdicts.extend(preskipped_verdicts(cfg))
            _persist_verdicts(conn, site_id, cfg, verdicts)
            continue
        if not advance_verification(conn, site_id, payload):
            return
    raise AssertionError("verification chain did not terminate")


def _decisions(conn: sqlite3.Connection, site_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM verification_trigger_decisions
        WHERE site_id = ? ORDER BY id
        """,
        (site_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# runs.py — pinning + fingerprint semantics
# ---------------------------------------------------------------------------


def test_fingerprint_deterministic_and_input_sensitive() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_verification_site(conn)
    snapshot = capture_config_snapshot(conn, site_id)
    first = input_fingerprint(conn, site_id, snapshot)
    assert first == input_fingerprint(conn, site_id, snapshot)
    assert seed_from_fingerprint(first) == seed_from_fingerprint(first)
    conn.execute(
        """
        INSERT INTO observations
            (site_id, variable, valid_at, value, n_stations, computed_at)
        VALUES (?, 'temperature', '2026-06-06T12:00:00Z', 14.0, 3,
                '2026-06-06T12:05:00Z')
        """,
        (site_id,),
    )
    changed = input_fingerprint(conn, site_id, capture_config_snapshot(conn, site_id))
    assert changed != first


# ---------------------------------------------------------------------------
# The full sync chain
# ---------------------------------------------------------------------------


def test_chain_publishes_a_complete_run() -> None:
    conn = asof_conn()
    site_id, feeds = _make_verification_site(conn)
    payload: dict[str, object] = {
        "trigger_date": "2026-06-06",
        "snapshot_days_per_chunk": 2,
    }
    _drive_chain(conn, site_id, payload)

    run_id = published_run_id(conn, site_id)
    assert run_id is not None
    run = conn.execute(
        "SELECT * FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert run["state"] == "published"
    assert run["period_start"] == _PERIOD_DAYS[0]
    assert run["period_end"] == _PERIOD_DAYS[-1]
    assert run["aggregate_state"] is not None

    decisions = _decisions(conn, site_id)
    assert [str(d["decision"]) for d in decisions] == ["run_started"]
    assert int(decisions[0]["run_id"]) == run_id

    # Evidence: every snapshot day has depth + feed + baseline entities.
    entity_types = {
        str(r["entity_type"])
        for r in conn.execute(
            "SELECT DISTINCT entity_type FROM verification_evidence WHERE run_id = ?",
            (run_id,),
        )
    }
    assert {
        "depth",
        "feed",
        "baseline_persistence",
        "baseline_all_feed_mean",
        "baseline_always_dry",
        "daily_rank_depth",
    } <= entity_types
    feed_keys = {
        str(r["entity_key"])
        for r in conn.execute(
            """
            SELECT DISTINCT entity_key FROM verification_evidence
            WHERE run_id = ? AND entity_type = 'feed'
            """,
            (run_id,),
        )
    }
    # The pinned roster covers the two synthetic feeds AND the
    # migration-seeded default-subscribed feeds — assert containment.
    assert {str(f) for f in feeds} <= feed_keys

    # Day context: one row per snapshot day, with exclusion accounting.
    contexts = conn.execute(
        "SELECT * FROM verification_day_context WHERE run_id = ? ORDER BY id",
        (run_id,),
    ).fetchall()
    assert [str(c["snapshot_local_date"]) for c in contexts] == _PERIOD_DAYS
    parsed: object = json.loads(str(contexts[0]["knowability_exclusions"]))
    assert isinstance(parsed, dict)
    assert int(contexts[0]["null_availability_samples"]) == 0

    # Verdicts: exactly one per variable; results non-empty.
    verdicts = conn.execute(
        "SELECT variable, outcome FROM verification_verdicts WHERE run_id = ?",
        (run_id,),
    ).fetchall()
    assert {str(v["variable"]) for v in verdicts} == {
        "temperature",
        "wind",
        "precip",
    }
    # Five settled days can never clear the 20-day adequacy floor.
    assert {str(v["outcome"]) for v in verdicts} == {"insufficient_evidence"}
    results_n = conn.execute(
        "SELECT COUNT(*) AS n FROM verification_results WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    assert int(results_n["n"]) > 0

    # Chain state fully cleared after publish.
    assert get_runtime_state(conn, verification_state_key(site_id)) is None


def test_preskipped_verdicts_only_for_out_of_range_incumbents() -> None:
    """F-3 unit: depth-5/6 incumbents yield explicit 'skipped' verdicts."""
    conn = asof_conn()
    site_id, _feeds = _make_verification_site(conn)
    set_setting(conn, "forecast_blend_depth_precip", "5")
    snapshot = capture_config_snapshot(conn, site_id)
    assert snapshot["blend_depths"] == {"temperature": 2, "wind": 2, "precip": 5}
    # Build a config through the real snapshot path.
    from wxverify.verification.runs import RunConfig, _parse_blend_depths

    cfg = RunConfig(
        site_id=site_id,
        run_id=1,
        timezone="UTC",
        rain_threshold_mm=0.2,
        wall_clock="07:00",
        blend_depth=2,
        blend_depths=_parse_blend_depths(snapshot["blend_depths"], 2),
        min_n=30,
        window_days=30,
        tz_generation_id=1,
        roster=(),
        period_start="2026-06-01",
        period_end="2026-06-05",
        bootstrap_seed=1,
        bootstrap_resamples=40,
    )
    skipped = preskipped_verdicts(cfg)
    assert [v.variable for v in skipped] == ["precip"]
    assert skipped[0].outcome == "skipped"
    assert skipped[0].recommended_key is None
    assert skipped[0].detail == {
        "incumbent": "5",
        "reason": "incumbent_depth_out_of_simulated_range",
        "simulated_depths": [1, 2, 3, 4],
    }


def test_chain_depth5_incumbent_publishes_explicit_skip() -> None:
    """F-3 end-to-end: a depth-5 precip incumbent never yields a silent
    all-insufficient publish — the run publishes with precip = 'skipped'
    (incumbent_depth 5) while the in-range variables decide normally."""
    conn = asof_conn()
    site_id, _feeds = _make_verification_site(conn)
    set_setting(conn, "forecast_blend_depth_precip", "5")
    conn.commit()
    _drive_chain(conn, site_id, {"trigger_date": "2026-06-06"})

    run_id = published_run_id(conn, site_id)
    assert run_id is not None
    run = conn.execute(
        "SELECT state FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert run["state"] == "published"
    verdicts = {
        str(r["variable"]): r
        for r in conn.execute(
            """
            SELECT variable, outcome, incumbent_depth, tested_family
            FROM verification_verdicts WHERE run_id = ?
            """,
            (run_id,),
        )
    }
    assert set(verdicts) == {"temperature", "wind", "precip"}
    assert str(verdicts["precip"]["outcome"]) == "skipped"
    assert int(verdicts["precip"]["incumbent_depth"]) == 5
    # In-range variables still decide through the normal path (five settled
    # days -> insufficient), never inherit the skip.
    assert str(verdicts["temperature"]["outcome"]) == "insufficient_evidence"
    assert str(verdicts["wind"]["outcome"]) == "insufficient_evidence"
    # prepare_bootstrap_inputs excludes the out-of-range variable entirely.
    cfg = run_config_from_row(conn, run_id)
    inputs = prepare_bootstrap_inputs(conn, cfg)
    assert {i.variable for i in inputs} == {"temperature", "wind"}


def test_chain_skips_when_fingerprint_unchanged() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_verification_site(conn)
    _drive_chain(conn, site_id, {"trigger_date": "2026-06-06"})
    first_run = published_run_id(conn, site_id)
    _drive_chain(conn, site_id, {"trigger_date": "2026-06-07"})
    assert published_run_id(conn, site_id) == first_run
    decisions = _decisions(conn, site_id)
    assert [str(d["decision"]) for d in decisions] == [
        "run_started",
        "no_change_skip",
    ]
    runs = conn.execute(
        "SELECT COUNT(*) AS n FROM verification_runs WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    assert int(runs["n"]) == 1


def test_chain_attempt_cap_skips_after_two_failures() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_verification_site(conn)
    snapshot = capture_config_snapshot(conn, site_id)
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    for _ in range(2):
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint, created_at)
            VALUES (?, ?, 1, 'test', 'failed', 1, '{}', '2026-06-01',
                    '2026-06-05', '2026-06-05', 1, 10, ?,
                    '2026-06-05T12:00:00Z')
            """,
            (site_id, int(str(snapshot["tz_generation_id"])), fingerprint),
        )
    _drive_chain(conn, site_id, {"trigger_date": "2026-06-06"})
    assert published_run_id(conn, site_id) is None
    decisions = _decisions(conn, site_id)
    assert [str(d["decision"]) for d in decisions] == ["skipped"]
    assert "attempt cap" in str(decisions[0]["reason"])


def test_mid_run_config_change_fails_the_simulate_chunk() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_verification_site(conn)
    payload: dict[str, object] = {"trigger_date": "2026-06-06"}
    for _ in range(50):
        blob = _load_state(conn, site_id)
        if blob is not None and blob.get("phase") == "simulate":
            break
        assert advance_verification(conn, site_id, payload)
    else:
        raise AssertionError("chain never reached the simulate phase")
    conn.execute("UPDATE sites SET rain_threshold_mm = 5.0 WHERE id = ?", (site_id,))
    with pytest.raises(RuntimeError, match="inputs changed mid-run"):
        advance_verification(conn, site_id, payload)


def test_mark_verification_failed_fails_run_and_clears_state() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_verification_site(conn)
    payload: dict[str, object] = {"trigger_date": "2026-06-06"}
    for _ in range(50):
        blob = _load_state(conn, site_id)
        if blob is not None and blob.get("phase") == "simulate":
            break
        assert advance_verification(conn, site_id, payload)
    job = Job(
        id=1,
        type="verification_run",
        site_id=site_id,
        job_key=verification_job_key(site_id),
        payload=payload,
        status="running",
        retry_count=3,
        max_retries=3,
    )
    mark_verification_failed(conn, job)
    run = conn.execute(
        "SELECT state FROM verification_runs WHERE site_id = ?", (site_id,)
    ).fetchone()
    assert str(run["state"]) == "failed"
    assert get_runtime_state(conn, verification_state_key(site_id)) is None


def test_run_config_round_trips_the_pinned_roster() -> None:
    conn = asof_conn()
    site_id, feeds = _make_verification_site(conn)
    _drive_chain(conn, site_id, {"trigger_date": "2026-06-06"})
    run_id = published_run_id(conn, site_id)
    assert run_id is not None
    cfg = run_config_from_row(conn, run_id)
    pinned_ids = {f.feed_id for f in cfg.roster}
    assert set(feeds) <= pinned_ids
    # Round-trip: the rehydrated roster equals the live derivation.
    assert cfg.roster == roster_feeds(conn, site_id)
    assert cfg.timezone == "UTC"
    assert cfg.blend_depth == 2
    assert cfg.bootstrap_seed == seed_from_fingerprint(
        str(
            conn.execute(
                "SELECT input_fingerprint FROM verification_runs WHERE id = ?",
                (run_id,),
            ).fetchone()["input_fingerprint"]
        )
    )


# ---------------------------------------------------------------------------
# Scheduler trigger
# ---------------------------------------------------------------------------


def test_scheduler_enqueues_and_then_suppresses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = asof_conn()
    site_id, _feeds = _make_verification_site(conn)
    fixed_now = datetime(2026, 6, 6, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(scheduler_module, "utc_now", lambda: fixed_now)

    _enqueue_due_verification_runs(conn)
    jobs = conn.execute(
        "SELECT * FROM jobs WHERE type = 'verification_run' AND site_id = ?",
        (site_id,),
    ).fetchall()
    assert len(jobs) == 1
    assert str(jobs[0]["job_key"]) == verification_job_key(site_id)
    assert json.loads(str(jobs[0]["payload"])) == {"trigger_date": "2026-06-06"}

    # Second tick with the chain still active: durable suppression, no dup.
    _enqueue_due_verification_runs(conn)
    jobs = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type = 'verification_run'"
    ).fetchone()
    assert int(jobs["n"]) == 1
    decisions = _decisions(conn, site_id)
    assert [str(d["decision"]) for d in decisions] == ["suppressed_because_active"]

    # Third tick: the suppression row now gates the whole local day.
    _enqueue_due_verification_runs(conn)
    assert len(_decisions(conn, site_id)) == 1


def test_scheduler_respects_trigger_time_and_bad_timezone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = asof_conn()
    site_id, _feeds = _make_verification_site(conn)
    before_trigger = datetime(2026, 6, 6, 1, 30, tzinfo=UTC)
    monkeypatch.setattr(scheduler_module, "utc_now", lambda: before_trigger)
    _enqueue_due_verification_runs(conn)
    jobs = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type = 'verification_run'"
    ).fetchone()
    assert int(jobs["n"]) == 0

    conn.execute("UPDATE sites SET timezone = 'Not/AZone' WHERE id = ?", (site_id,))
    after_trigger = datetime(2026, 6, 6, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(scheduler_module, "utc_now", lambda: after_trigger)
    _enqueue_due_verification_runs(conn)
    jobs = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type = 'verification_run'"
    ).fetchone()
    assert int(jobs["n"]) == 0


# ---------------------------------------------------------------------------
# F4/F5: tz-generation cleanup widened to failed generations
# ---------------------------------------------------------------------------


def test_cleanup_sweeps_failed_generation_rows() -> None:
    conn = asof_conn()
    site_id, feeds = _make_verification_site(conn)
    published = ensure_published_generation(conn, site_id)
    cur = conn.execute(
        """
        INSERT INTO timezone_generations (site_id, timezone, mode, state)
        VALUES (?, 'UTC', 'retrospective_correction', 'failed')
        """,
        (site_id,),
    )
    assert cur.lastrowid is not None
    failed_gen = int(cur.lastrowid)
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feeds[0],
        valid_at="2026-06-01T12:00:00Z",
        issued_at="2026-05-31T12:00:00Z",
        forecast=15.0,
        observed=14.0,
        first_known_at="2026-06-01T15:00:00Z",
        generation_id=failed_gen,
    )
    blob: dict[str, object] = {"phase": "cleanup"}
    while _cleanup_chunk(conn, site_id, published, blob, {}):
        pass
    remaining = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_pairs WHERE tz_generation_id = ?",
        (failed_gen,),
    ).fetchone()
    assert int(remaining["n"]) == 0
