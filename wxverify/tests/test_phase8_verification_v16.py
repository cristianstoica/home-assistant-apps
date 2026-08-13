"""§16 content-contract smoke tests for the /verification page (NB-3).

One test per §16 subsection: the run-status banner (§16.1), the per-variable
verdict cards (§16.2), the headline evidence table (§16.3), the non-enactable
diagnostics (§16.4), and methodology & provenance (§16.5) — plus the two hard
rendering constraints: an insufficient / not-applicable / failed value never
renders as numeric zero, and the page performs no simulation or bootstrap
work on the request path.

The oracle in §18.11 asserts on the stable ``data-v16="<subsection>.<element>"``
attributes these tests pin.

Synthetic fixtures only — invented site name/coords, UTC timezone, fake feed
ids and models; no real station identifiers.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, init_db
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.runs import (
    capture_config_snapshot,
    input_fingerprint,
    publish_run,
)

# ---------------------------------------------------------------------------
# Harness.
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _make_site(conn: sqlite3.Connection) -> int:
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES ('V16 Town', 47.0, 25.0, 900.0, 'UTC', 1)
            """,
        ).lastrowid
    )
    ensure_published_generation(conn, site_id)
    return site_id


_TEMPERATURE_FAMILY: dict[str, object] = {
    "incumbent": "2",
    "candidates": {
        "3": {
            "headline": {
                "adequate_leads": [0, 1, 2],
                "pooled_point": 0.12,
                "ci": [0.06, 0.19],
                "per_lead": {"1": 0.1, "2": 0.14},
            },
            "conditions": {
                "ci_excludes_zero": True,
                "lead_stability": True,
                "practical_floor": True,
                "beats_baselines": True,
                # Never evaluated -> must read 'insufficient', not vanish.
                "components_non_inferior": None,
            },
            "baselines": {
                "baseline_persistence": {"passed": True, "ci": [0.2, 0.4]},
                "baseline_all_feed_mean": {"passed": False, "ci": [-0.1, 0.2]},
            },
            "components": {
                "temperature_high": {"pooled_point": 0.10},
                "temperature_low": {"pooled_point": 0.02, "degraded": True},
            },
        }
    },
    "statistically_unresolved": ["4"],
    "tie_break": {"chosen": "3", "reason": "nearest the incumbent"},
}

_WIND_FAMILY: dict[str, object] = {
    "incumbent": "2",
    "candidates": {
        "3": {
            "headline": {
                "adequate_leads": [1],
                "pooled_point": 0.01,
                "ci": [-0.04, 0.06],
                "per_lead": {"1": 0.01},
            },
            "conditions": {
                "ci_excludes_zero": False,
                "lead_stability": False,
                "practical_floor": False,
                "beats_baselines": False,
            },
            "baselines": {},
            "components": {},
        }
    },
    "statistically_unresolved": [],
}

_PRECIP_FAMILY: dict[str, object] = {
    "incumbent": 6,
    "reason": "incumbent depth 6 lies outside the simulated range 1-5",
    "simulated_depths": [1, 2, 3, 4, 5],
}


def _seed_rich_run(conn: sqlite3.Connection, site_id: int) -> int:
    """A published run exercising every §16 element the page can render."""
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
            VALUES (?, ?, 1, '0.11.0-test', 'running', 2, ?, '2026-05-01',
                    '2026-05-30', '2026-05-30', 4242, 10000, ?)
            """,
            (site_id, generation_id, json.dumps(snapshot), fingerprint),
        ).lastrowid
    )
    conn.executemany(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, recommended_depth, incumbent_depth,
             tested_family)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (run_id, "temperature", "recommend", 3, 2, json.dumps(_TEMPERATURE_FAMILY)),
            (run_id, "wind", "retain_incumbent", None, 2, json.dumps(_WIND_FAMILY)),
            (run_id, "precip", "skipped", None, 6, json.dumps(_PRECIP_FAMILY)),
        ],
    )
    columns = (
        "run_id, variable, lead, quantity, entity_type, entity_key, headline, "
        "common_days, mae, bias, rmse, hits, misses, false_alarms, "
        "correct_negatives, ets, availability_rate, delta_vs_incumbent"
    )
    rows: list[tuple[object, ...]] = [
        # §16.3 headline: candidate depth 3 and the incumbent depth 2.
        (
            run_id,
            "temperature",
            1,
            "temperature_high",
            "depth",
            "3",
            1,
            25,
            0.80,
            0.10,
            1.00,
            None,
            None,
            None,
            None,
            None,
            0.95,
            0.12,
        ),
        (
            run_id,
            "temperature",
            1,
            "temperature_high",
            "depth",
            "2",
            1,
            25,
            0.92,
            0.15,
            1.10,
            None,
            None,
            None,
            None,
            None,
            0.95,
            None,
        ),
        # Insufficient cell: metrics stay NULL, must never render as 0.
        (
            run_id,
            "wind",
            3,
            "wind_max",
            "depth",
            "3",
            1,
            4,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        # §16.4 D0 — diagnostic only, never in the headline table.
        (
            run_id,
            "temperature",
            0,
            "temperature_high",
            "depth",
            "3",
            0,
            12,
            0.40,
            0.05,
            0.60,
            None,
            None,
            None,
            None,
            None,
            0.90,
            None,
        ),
        # §16.3 baseline comparisons for the temperature D1 cell.
        (
            run_id,
            "temperature",
            1,
            "temperature_high",
            "baseline_persistence",
            "-",
            0,
            25,
            1.40,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            run_id,
            "temperature",
            1,
            "temperature_high",
            "baseline_all_feed_mean",
            "-",
            0,
            25,
            0.98,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        # §16.3/§16.4 precip occurrence: contingency counts + ETS.
        (
            run_id,
            "precip",
            1,
            "precip_occurrence",
            "depth",
            "3",
            1,
            22,
            None,
            None,
            None,
            9,
            3,
            4,
            6,
            0.31,
            0.91,
            0.04,
        ),
        # §16.4 daily-rank diagnostic policy.
        (
            run_id,
            "temperature",
            1,
            "temperature_high",
            "daily_rank_depth",
            "top2",
            0,
            20,
            0.88,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            -0.02,
        ),
        # §16.4 feeds: one below the availability floor, one pairwise-only.
        (
            run_id,
            "temperature",
            1,
            "temperature_high",
            "feed",
            "101",
            1,
            25,
            1.05,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            0.55,
            None,
        ),
        (
            run_id,
            "temperature",
            1,
            "temperature_high",
            "feed",
            "102",
            0,
            25,
            0.99,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            0.88,
            None,
        ),
    ]
    conn.executemany(
        f"INSERT INTO verification_results ({columns}) VALUES ({', '.join('?' * 18)})",
        rows,
    )
    # Evidence backs §16.3 realized-contributor depths; the ineligible row is
    # excluded, so its cell renders an em-dash rather than a count of zero.
    conn.executemany(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, predicted, forecast_eligible,
             realized_contributors, truth_value, truth_eligible, abs_error)
        VALUES (?, ?, ?, ?, ?, ?, 'depth', ?, ?, ?, ?, ?, 1, ?)
        """,
        [
            (
                run_id,
                "2026-05-01",
                "2026-05-02",
                1,
                "temperature",
                "temperature_high",
                "3",
                15.0,
                1,
                2,
                14.5,
                0.5,
            ),
            (
                run_id,
                "2026-05-02",
                "2026-05-03",
                1,
                "temperature",
                "temperature_high",
                "3",
                16.0,
                1,
                3,
                14.0,
                2.0,
            ),
            (
                run_id,
                "2026-05-01",
                "2026-05-04",
                3,
                "wind",
                "wind_max",
                "3",
                5.0,
                0,
                1,
                6.0,
                1.0,
            ),
        ],
    )
    conn.execute(
        """
        INSERT INTO verification_day_context
            (run_id, snapshot_local_date, snapshot_utc,
             knowability_exclusions, null_availability_samples)
        VALUES (?, '2026-05-01', '2026-05-01T07:00:00Z', ?, 2)
        """,
        (run_id, json.dumps({"temperature_high": "truth_pending"})),
    )
    publish_run(conn, site_id, run_id)
    conn.commit()
    return run_id


@pytest.fixture
def page(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    _seed_rich_run(conn, site_id)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200
    return response.text


# ---------------------------------------------------------------------------
# §16.1 Run-status banner.
# ---------------------------------------------------------------------------


def test_run_status_banner_renders_every_element(page: str) -> None:
    for element in (
        "16.1.declared_configuration",
        "16.1.run_id",
        "16.1.period",
        "16.1.data_cutoff",
        "16.1.published_at",
        "16.1.tz_generation",
        "16.1.methodology_version",
        "16.1.app_version",
        "16.1.config_snapshot",
        "16.1.pinned_depths",
        "16.1.roster_snapshot",
        "16.1.run_link",
    ):
        assert f'data-v16="{element}"' in page
    assert 'id="verification-run-banner"' in page
    assert "2026-05-01 to 2026-05-30" in page
    assert "attempt 2" in page
    assert "v0.11.0-test" not in page  # app version is printed unprefixed
    assert "0.11.0-test" in page


# ---------------------------------------------------------------------------
# §16.2 Per-variable verdict cards.
# ---------------------------------------------------------------------------


def test_verdict_cards_carry_outcome_effect_and_caveats(page: str) -> None:
    assert page.count('data-v16="16.2.card"') == 3
    for variable in ("temperature", "wind", "precip"):
        assert f'data-v16="16.2.card" data-variable="{variable}"' in page
    assert "Recommend depth change" in page
    assert "Retain incumbent" in page
    assert "This is not proof that the incumbent depth is optimal." in page
    # A skipped verdict is labelled a placeholder, not an improvement.
    assert 'data-v16="16.2.placeholder"' in page
    assert "Not evidence of improvement." in page
    assert "incumbent depth 6 lies outside the simulated range 1-5" in page
    # Pooled effect, CI, adequate leads, common-day range, practical floor.
    assert "0.120" in page
    assert "[0.060, 0.190]" in page
    assert "3 of 4 required" in page
    assert 'data-v16="16.2.common_day_range"' in page
    assert 'data-v16="16.2.practical_significance"' in page
    # Every declared §12 gate is shown; the unevaluated one is insufficient.
    assert 'data-gate="ci_excludes_zero" data-gate-state="pass"' in page
    assert 'data-gate="components_non_inferior" data-gate-state="insufficient"' in page
    assert 'data-v16="16.2.baseline_gates"' in page
    assert 'data-v16="16.2.unresolved"' in page


def test_live_depth_is_distinct_from_the_runs_pinned_incumbent(page: str) -> None:
    assert "Incumbent depth (pinned by run #" in page
    assert 'data-v16="16.2.live_depth"' in page
    assert "Live effective blend depth" in page
    # Live global depth is 2; precip's pinned incumbent is 6 -> mismatch notice.
    assert 'data-v16="16.2.depth_mismatch"' in page


# ---------------------------------------------------------------------------
# §16.3 Headline evidence table.
# ---------------------------------------------------------------------------


def test_headline_table_is_keyed_by_variable_lead_quantity_and_depth(
    page: str,
) -> None:
    assert 'data-v16="16.3.table"' in page
    assert (
        '<tr data-v16="16.3.row" data-variable="temperature" data-lead="1" '
        'data-quantity="temperature_high" data-depth="3">'
    ) in page
    assert (
        '<tr data-v16="16.3.row" data-variable="precip" data-lead="1" '
        'data-quantity="precip_occurrence" data-depth="3">'
    ) in page
    # D0 and non-headline rows stay out of the table.
    assert 'data-lead="0"' not in page.split('id="verification-diagnostics"')[0]
    for element in (
        "16.3.candidate_depth",
        "16.3.primary_metric",
        "16.3.incumbent_delta",
        "16.3.ci",
        "16.3.common_days",
        "16.3.observed_events",
        "16.3.realized_contributors",
        "16.3.baseline_comparisons",
        "16.3.gate_states",
    ):
        assert f'data-v16="{element}"' in page
    # Occurrence rows show ETS and the contingency counts; the incumbent row
    # has no delta against itself.
    assert "ETS 0.310" in page
    assert "9/3/4/6" in page
    assert "n/a" in page
    # Realized contributor span comes from the run's own evidence rows.
    assert "2&ndash;3" in page or "2–3" in page
    assert "persistence 1.400" in page


def test_insufficient_cells_never_render_as_numeric_zero(page: str) -> None:
    assert "0.000" not in page
    assert (
        '<td data-label="Primary metric" data-v16="16.3.primary_metric">MAE —</td>'
        in page
    )
    # The all-ineligible wind cell has no contributor count at all.
    assert (
        '<td data-label="Realized contributors" '
        'data-v16="16.3.realized_contributors">—</td>' in page
    )
    assert 'data-gate="lead_adequacy" data-gate-state="insufficient"' in page


# ---------------------------------------------------------------------------
# §16.4 Diagnostics (non-enactable).
# ---------------------------------------------------------------------------


def test_diagnostics_are_separated_and_labelled_non_enactable(page: str) -> None:
    assert 'id="verification-diagnostics"' in page
    assert "nothing in this section feeds a verdict" in page
    for element in (
        "16.4.non_enactable",
        "16.4.day_context",
        "16.4.d0",
        "16.4.daily_rank",
        "16.4.feeds",
        "16.4.bias_rmse",
        "16.4.contingency",
    ):
        assert f'data-v16="{element}"' in page
    assert "D0 (partly elapsed target day" in page
    assert 'data-below-floor="yes"' in page
    assert 'data-pairwise-only="yes"' in page
    assert "1 snapshot days" in page
    assert "2 null-availability samples" in page


def test_diagnostics_declare_the_sets_methodology_v1_cannot_produce(
    page: str,
) -> None:
    assert 'data-v16="16.4.wet_hour_share"' in page
    assert "Not available — Deferred to methodology version 2:" in page
    assert "neither the bin edges nor the predicted-vs-observed denominator" in page
    assert 'data-v16="16.4.split_half"' not in page
    assert "split-half" not in page.lower()


# ---------------------------------------------------------------------------
# §16.5 Methodology and provenance.
# ---------------------------------------------------------------------------


def test_methodology_block_states_constants_and_provenance(page: str) -> None:
    assert 'id="verification-methodology"' in page
    for element in (
        "16.5.snapshot_time",
        "16.5.timezone",
        "16.5.truth_rules",
        "16.5.eligibility_rules",
        "16.5.roster_floor",
        "16.5.candidates",
        "16.5.baselines",
        "16.5.metrics",
        "16.5.units",
        "16.5.bootstrap",
        "16.5.thresholds",
        "16.5.ranking_basis",
        "16.5.code_version",
        "16.5.input_fingerprint",
        "16.5.exclusion_counts",
        "16.5.schema",
        "16.5.config_snapshot",
        "16.5.run_link",
    ):
        assert f'data-v16="{element}"' in page
    assert "block length 3 days" in page
    assert "10000 resamples" in page
    assert "seed 4242" in page
    assert "70% of truth-eligible days" in page
    assert "verification_schema 1" in page


def test_tested_family_lists_every_candidate_with_adjusted_confidence(
    page: str,
) -> None:
    assert 'data-v16="16.5.tested_family"' in page
    assert 'data-v16="16.5.tested_family_variable" data-variable="temperature"' in page
    assert 'data-v16="16.5.adjusted_confidence"' in page
    assert "0.9833" in page  # candidate CI level
    assert "0.9917" in page  # precip improvement CI level
    assert 'data-v16="16.5.gate_outcomes"' in page
    assert 'data-v16="16.5.baseline_comparisons"' in page


# ---------------------------------------------------------------------------
# §19 — the page reads persisted rows only.
# ---------------------------------------------------------------------------


def test_page_performs_no_simulation_or_bootstrap_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    _seed_rich_run(conn, site_id)

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("the /verification page must not simulate per request")

    monkeypatch.setattr("wxverify.verification.simulate.simulate_snapshot_day", _boom)
    monkeypatch.setattr("wxverify.verification.engine.prepare_bootstrap_inputs", _boom)

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        assert client.get(f"/verification?site={site_id}").status_code == 200
