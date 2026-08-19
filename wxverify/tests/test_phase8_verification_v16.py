"""§16 content-contract smoke tests for the /verification page (NB-3).

Many tests per §16 subsection: the run-status banner (§16.1), the
per-variable verdict cards (§16.2), the headline evidence table (§16.3), the
non-enactable diagnostics (§16.4), and methodology & provenance (§16.5) —
plus the two hard rendering constraints (an insufficient / not-applicable /
failed value never renders as numeric zero, and the page performs no
simulation or bootstrap work on the request path), the decision-core era
gate (O-V1/O-V2/O-V4/O-V6/B3), and per-outcome/per-family regression oracles
(O16-O20) pinning specific fixture shapes against specific mutants.

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
            VALUES ('V16 Town', 40.0, -105.0, 900.0, 'UTC', 1)
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


def _seed_rich_run(
    conn: sqlite3.Connection, site_id: int, *, methodology_version: int
) -> int:
    """A published run exercising every §16 element the page can render.

    ``methodology_version`` is required and keyword-only, not defaulted: a
    fixture's version is a deliberate choice a reader can see at the call
    site, not an accident of the INSERT literal it used to be.
    """
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
            VALUES (?, ?, ?, '0.11.0-test', 'running', 2, ?, '2026-05-01',
                    '2026-05-30', '2026-05-30', 4242, 10000, ?)
            """,
            (
                site_id,
                generation_id,
                methodology_version,
                json.dumps(snapshot),
                fingerprint,
            ),
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
    # Version 1 -- predates the pairwise decision core (contract.py:18's
    # `PAIRWISE_DECISION_CORE_SINCE = 2`), so this fixture renders the
    # `strict`-era §16.3 prose.
    _seed_rich_run(conn, site_id, methodology_version=1)
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


def test_diagnostics_declare_the_sets_the_shipped_methodology_cannot_produce(
    page: str,
) -> None:
    assert 'data-v16="16.4.wet_hour_share"' in page
    assert (
        "Not available — Not specified by the methodology version this run "
        "was scored under:" in page
    )
    assert "neither the bin edges nor the predicted-vs-observed denominator" in page
    assert 'data-v16="16.4.split_half"' not in page
    assert "split-half" not in page.lower()


def test_o_v5_the_unavailable_diagnostic_reason_names_no_version_number(
    page: str,
) -> None:
    """O-V5: the wet-hour-share reason is version-free (Fix 2).

    Scoped to the notice node, not the whole page -- the page legitimately
    prints "Methodology version v1" at §16.1 and §16.5, so a page-wide
    negative assertion would be both wrong and pass for the wrong reason.
    """
    marker = 'data-v16="16.4.wet_hour_share"'
    section_start = page.index(marker)
    # Slice down to the reason paragraph itself, not the whole subsection --
    # the enclosing div's own `data-v16="16.4..."` attribute (and the §16.1/
    # §16.5 "Methodology version" text elsewhere on the page) legitimately
    # contain digits, so scanning anything wider would make the negative
    # assertion pass for the wrong reason.
    reason_start = page.index('<p class="empty">', section_start)
    notice = page[reason_start : page.index("</p>", reason_start)]
    # mutant -> at 16.4.wet_hour_share: correct = no digit character appears
    # in the reason paragraph (the version-free wording), mutant (the old
    # `f"Not specified in methodology version {N}:"` interpolation this fix
    # removed) = a digit ("1" or "2") appears in the reason paragraph.
    assert not any(ch.isdigit() for ch in notice)
    assert "Not specified by the methodology version this run was scored" in notice


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
    assert "verification_schema 2" in page


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
    _seed_rich_run(conn, site_id, methodology_version=1)

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("the /verification page must not simulate per request")

    monkeypatch.setattr("wxverify.verification.simulate.simulate_snapshot_day", _boom)
    monkeypatch.setattr("wxverify.verification.engine.prepare_bootstrap_inputs", _boom)

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        assert client.get(f"/verification?site={site_id}").status_code == 200


# ---------------------------------------------------------------------------
# Plan §8 oracles O16-O22b (D8/D9/D13/D14 rendering). Hand-built
# ``tested_family`` records only -- these oracles pin the rendering of a
# decision-path payload, not the decision path itself.
# ---------------------------------------------------------------------------

_UNSET: object = object()

# The domain the CHECK constraint at db/migrations.py:439-441 allows; kept
# here (not imported) so this stays a fixture-side check, independent of
# whatever the production enum happens to export.
_KNOWN_OUTCOMES = frozenset(
    {
        "recommend",
        "retain_incumbent",
        "mixed_by_lead",
        "mixed_by_quantity",
        "insufficient_evidence",
        "skipped",
    }
)


def _run_with_verdicts(
    conn: sqlite3.Connection,
    site_id: int,
    verdicts: list[tuple[str, str, int | None, int, dict[str, object]]],
    *,
    methodology_version: int = 2,
) -> int:
    """A published run carrying exactly the given hand-built verdict rows.

    ``methodology_version`` defaults to 2 -- every existing O16-O22b caller
    relies on that default unchanged. Pass 1 or 3 for the §16.3 decision-
    core-era oracles (O-V1/O-V2/B4), which need runs on either side of
    ``PAIRWISE_DECISION_CORE_SINCE`` and above this build's own version.
    """
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
            VALUES (?, ?, ?, '0.13.2-test', 'running', 1, ?, '2026-05-01',
                    '2026-05-30', '2026-05-30', 4242, 200, ?)
            """,
            (
                site_id,
                generation_id,
                methodology_version,
                json.dumps(snapshot),
                fingerprint,
            ),
        ).lastrowid
    )

    def _insert() -> None:
        conn.executemany(
            """
            INSERT INTO verification_verdicts
                (run_id, variable, outcome, recommended_depth, incumbent_depth,
                 tested_family)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    variable,
                    outcome,
                    recommended_depth,
                    incumbent_depth,
                    json.dumps(family),
                )
                for variable, outcome, recommended_depth, incumbent_depth, family in (
                    verdicts
                )
            ],
        )

    if any(outcome not in _KNOWN_OUTCOMES for _, outcome, _, _, _ in verdicts):
        # O19b's unknown-outcome oracle deliberately inserts an `outcome`
        # value the CHECK constraint at db/migrations.py:439-441 makes
        # unreachable in production. That is the point of the test, not a
        # workaround for one: it proves the render layer degrades
        # gracefully on a genuinely-reachable TEMPLATE branch (a future
        # outcome value added to the Python enum before the DB constraint
        # catches up), which no *valid* row could ever exercise. Scoped to
        # only this insert -- callers whose verdicts stay in-domain never
        # relax the constraint, so they still get the real schema check --
        # and always turned back off in a `finally`.
        conn.execute("PRAGMA ignore_check_constraints = ON")
        try:
            _insert()
        finally:
            conn.execute("PRAGMA ignore_check_constraints = OFF")
    else:
        _insert()
    publish_run(conn, site_id, run_id)
    conn.commit()
    return run_id


def _render(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    verdicts: list[tuple[str, str, int | None, int, dict[str, object]]],
) -> str:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    _run_with_verdicts(conn, site_id, verdicts)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200
    return response.text


def _card(page: str, variable: str) -> str:
    """One variable's §16.2 card node, for scoped assertions."""
    marker = f'data-v16="16.2.card" data-variable="{variable}"'
    start = page.index(marker)
    next_start = page.find('data-v16="16.2.card"', start + 1)
    end = next_start if next_start != -1 else page.index('id="verification-headline"')
    return page[start:end]


def _field(card: str, name: str) -> str:
    """One §16.2 field's own node, scoped to just that field.

    §16.2 fields are a mix of ``<div>`` (the ``<dl class="facts">`` entries)
    and ``<p>`` (``decision_statement``, ``baseline_gates``,
    ``completeness_guards``, ``ordering_endpoint_unresolved``,
    ``primary_missing``, ...) -- this closes on whichever wrapping tag
    actually follows the marker, never assuming one shape.
    """
    marker = f'data-v16="16.2.{name}"'
    start = card.index(marker)
    div_end = card.find("</div>", start)
    p_end = card.find("</p>", start)
    ends = [e for e in (div_end, p_end) if e != -1]
    end = min(ends)
    tag_len = len("</div>") if end == div_end else len("</p>")
    return card[start : end + tag_len]


# ---------------------------------------------------------------------------
# O16 -- a pre-window (run-1-shaped) verdict, on both card shapes.
# ---------------------------------------------------------------------------

_O16_FAMILY: dict[str, object] = {
    "incumbent": "2",
    "candidates": {
        "3": {
            "headline": {
                "adequate_leads": [1, 2],
                "pooled_point": 0.1,
                "ci": [0.05, 0.15],
                "per_lead": {"1": 0.1, "2": 0.1},
                "dropped_leads": [{"lead": 3, "reason": "thin_data", "days": 5}],
            },
            "conditions": {
                "ci_excludes_zero": True,
                "lead_stability": True,
                "practical_floor": True,
                "beats_baselines": True,
            },
            "baselines": {"baseline_persistence": {"passed": True, "ci": [0.1, 0.2]}},
            "components": {},
        }
    },
    "statistically_unresolved": [],
}


def test_o16a_pre_window_verdict_with_no_recommendation_renders_a_decision_statement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = _render(
        tmp_path, monkeypatch, [("wind", "retain_incumbent", None, 2, _O16_FAMILY)]
    )
    assert "Traceback" not in page
    card = _card(page, "wind")
    assert 'data-v16="16.2.decision_statement"' in card


def test_o16b_pre_window_verdict_with_a_recommendation_shows_a_dash_decision_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = _render(tmp_path, monkeypatch, [("wind", "recommend", 3, 2, _O16_FAMILY)])
    assert "Traceback" not in page
    card = _card(page, "wind")
    window_field = card[card.index('data-v16="16.2.decision_window"') :]
    window_field = window_field[: window_field.index("</div>")]
    # mutant -> at 16.2.decision_window: correct = '—' (the record predates
    # the window disclosure, so `_decision_window` finds no "window" key even
    # though a depth is recommended), mutant (a template that assumes
    # `window` is always present once a depth is recommended) = a Jinja
    # UndefinedError -- the page never renders at all.
    assert "—" in window_field


# ---------------------------------------------------------------------------
# O17/O17b -- unmeasured-endpoint warning, on a card with no primary.
# ---------------------------------------------------------------------------


def _occ_endpoint(reason: str, count: int, start_lead: int = 1) -> dict[str, object]:
    return {
        "adequate_leads": [],
        "pooled_point": None,
        "ci": None,
        "per_lead": {},
        "dropped_leads": [
            {"lead": start_lead + i, "reason": reason, "days": 5} for i in range(count)
        ],
    }


def _precip_candidate(
    total_adequate: list[int], occ_reason: str, occ_count: int
) -> dict[str, object]:
    return {
        "total": {
            "adequate_leads": total_adequate,
            "pooled_point": 0.0,
            "ci": [-0.01, 0.01],
            "per_lead": {str(ld): 0.0 for ld in total_adequate},
            "dropped_leads": [],
        },
        "occurrence": _occ_endpoint(occ_reason, occ_count),
        "conditions": {
            "total_material": False,
            "occurrence_material": False,
            "total_non_inferior": True,
            "occurrence_non_inferior": True,
            "beats_baselines": False,
        },
        "baselines": {},
        "components": {},
    }


_O17_FAMILY: dict[str, object] = {
    "incumbent": "2",
    "candidates": {
        "3": _precip_candidate([1, 2, 3, 4], "thin_events", 3),
        "4": _precip_candidate([1, 2, 3, 4], "thin_events", 2),
    },
    "statistically_unresolved": ["3", "4"],
}


def test_o17_unmeasured_endpoint_warning_names_occurrence_and_dominant_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = _render(
        tmp_path, monkeypatch, [("precip", "retain_incumbent", None, 2, _O17_FAMILY)]
    )
    card = _card(page, "precip")
    # mutant -> at 16.2.evidence_scope: correct = present, naming
    # "occurrence" and "thin_events" (a disclosure sourced from BOTH tested
    # candidates' empty occurrence adequate sets), mutant (a disclosure
    # sourced from `v.primary`, which is `None` on every NULL-depth
    # verdict) = the notice never rendering at all.
    assert 'data-v16="16.2.evidence_scope"' in card
    assert "occurrence" in card
    assert "thin_events" in card


def test_o17b_partial_shortfall_is_not_a_verdict_wide_unmeasured_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    partial_family: dict[str, object] = {
        "incumbent": "2",
        "candidates": {
            "3": _precip_candidate([1, 2, 3, 4], "thin_events", 3),
            "4": {
                **_precip_candidate([1, 2, 3, 4], "thin_events", 0),
                "occurrence": {
                    "adequate_leads": [1, 2, 3, 4],
                    "pooled_point": 0.05,
                    "ci": [0.01, 0.09],
                    "per_lead": {"1": 0.05, "2": 0.05, "3": 0.05, "4": 0.05},
                    "dropped_leads": [],
                },
            },
        },
        "statistically_unresolved": ["3"],
    }
    page = _render(
        tmp_path, monkeypatch, [("precip", "retain_incumbent", None, 2, partial_family)]
    )
    card = _card(page, "precip")
    # mutant -> at 16.2.evidence_scope: correct = absent (candidate "4"
    # holds a non-empty occurrence adequate set, so the endpoint IS measured
    # on this verdict), mutant (a rule written as "any candidate's
    # occurrence is unmeasured" instead of "every candidate's") = present.
    assert 'data-v16="16.2.evidence_scope"' not in card
    row = page[page.index('data-candidate="4" data-endpoint="occurrence"') :]
    row = row[: row.index("</tr>")]
    assert "1, 2, 3, 4" in row


def test_o17b_tie_break_names_the_earlier_precedence_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    family: dict[str, object] = {
        "incumbent": "2",
        "candidates": {
            "3": _precip_candidate([1, 2, 3, 4], "baseline_absent", 3),
            "4": _precip_candidate([1, 2, 3, 4], "thin_events", 3),
        },
        "statistically_unresolved": ["3", "4"],
    }
    page = _render(
        tmp_path, monkeypatch, [("precip", "retain_incumbent", None, 2, family)]
    )
    card = _card(page, "precip")
    # mutant -> at 16.2.evidence_scope: correct = names "thin_events" (a 3-3
    # count tie is broken by `_adequate_leads`' own precedence, where
    # thin_events (decision.py:275) precedes baseline_absent (:285)),
    # mutant (a tie resolved by dict iteration order, "3" inserted before
    # "4") = names "baseline_absent" instead.
    assert "thin_events" in card
    assert "baseline_absent" not in card


# ---------------------------------------------------------------------------
# O17c -- the unmeasured-endpoint warning's closing clause is unconditional:
# no outcome token, known or unknown, may license the reassuring wording.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "outcome",
    [
        # Every real token the CHECK constraint admits (db/migrations.py:
        # 439-441), including the two that used to sit on opposite sides of
        # the deleted allowlist (`retain_incumbent` was the allowlist's one
        # live entry; the rest were already excluded).
        "recommend",
        "retain_incumbent",
        "mixed_by_lead",
        "mixed_by_quantity",
        "insufficient_evidence",
        "skipped",
        # An unknown future token -- proves the clause is unconditional, not
        # a denylist that only special-cases the tokens known today.
        "no_such_outcome_token",
    ],
)
def test_o17c_closing_clause_is_unconditional_on_every_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
) -> None:
    page = _render(tmp_path, monkeypatch, [("precip", outcome, None, 2, _O17_FAMILY)])
    card = _card(page, "precip")
    # Presence is asserted FIRST and unconditionally, before the wording is
    # checked: a suppression fix that guts the whole node on some outcome
    # must die here, not be masked by a wording assertion that never runs
    # because the node is gone.
    #
    # mutant -> at 16.2.evidence_scope (presence): correct = present on
    # every arm, mutant (gating the whole node on e.g.
    # `v.outcome != 'insufficient_evidence'`, i.e. show.html:152's
    # `{% if v.unmeasured_endpoints %}` narrowed to also require that) =
    # absent on the `insufficient_evidence` arm.
    assert 'data-v16="16.2.evidence_scope"' in card
    assert "occurrence" in card
    assert "thin_events" in card
    rests_only = "The outcome above rests only on the parts that were measured."
    no_conclusion = (
        "The outcome above is not a conclusion about the parts that were "
        "never measured."
    )
    # mutant -> at 16.2.evidence_scope (wording): correct = no-conclusion
    # present, "rests only" absent on every arm, mutant (a reintroduced
    # allowlist naming `retain_incumbent`/`recommend`, i.e. the deleted
    # `['retain_incumbent', 'recommend']` branch) = "rests only" present
    # instead of no-conclusion on the `retain_incumbent` and `recommend`
    # arms -- the exact two tokens the deleted allowlist licensed.
    assert no_conclusion in card
    assert rests_only not in card


# ---------------------------------------------------------------------------
# O18 -- decision-sample column, and the hint that describes it.
# ---------------------------------------------------------------------------

_O18_FAMILY: dict[str, object] = {
    "incumbent": "2",
    "candidates": {
        "3": {
            "headline": {
                "adequate_leads": [1],
                "pooled_point": 0.2,
                "ci": [0.1, 0.3],
                "per_lead": {"1": 0.2},
                "dropped_leads": [],
                "window": {
                    "first": "2026-05-01",
                    "last": "2026-06-16",
                    "days": 47,
                    "per_lead": {
                        "1": {"first": "2026-05-01", "last": "2026-06-16", "days": 47}
                    },
                },
            },
            "conditions": {
                "ci_excludes_zero": True,
                "lead_stability": True,
                "practical_floor": True,
                "beats_baselines": True,
            },
            "baselines": {},
            "components": {},
        }
    },
    "statistically_unresolved": [],
}


def test_o18_decision_sample_column_and_hint_agree_with_pairwise_core(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    run_id = _run_with_verdicts(
        conn, site_id, [("wind", "recommend", 3, 2, _O18_FAMILY)]
    )
    columns = (
        "run_id, variable, lead, quantity, entity_type, entity_key, headline, "
        "common_days, mae, bias, rmse, hits, misses, false_alarms, "
        "correct_negatives, ets, availability_rate, delta_vs_incumbent"
    )
    conn.execute(
        f"INSERT INTO verification_results ({columns}) VALUES ({', '.join('?' * 18)})",
        (
            run_id,
            "wind",
            1,
            "wind_max",
            "depth",
            "3",
            1,
            0,
            0.5,
            0.05,
            0.6,
            None,
            None,
            None,
            None,
            None,
            0.95,
            0.1,
        ),
    )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200
    page = response.text
    row = page[page.index('data-v16="16.3.row" data-variable="wind" data-lead="1"') :]
    row = row[: row.index("</tr>")]
    assert '<td data-label="Common days" data-v16="16.3.common_days">0</td>' in row
    # mutant -> at 16.3.decision_days: correct = 47 (the pairwise window the
    # verdict actually used at lead 1), mutant (a column sourced from the
    # same strict-core count as "Common days") = 0.
    assert (
        '<td data-label="Decision sample" data-v16="16.3.decision_days">47</td>' in row
    )
    reworded = (
        "the Decision sample column is the pairwise count the verdict used "
        "for that depth. D0 is diagnostic only and appears below."
    )
    assert reworded in page
    assert "on the strict common core. D0 is diagnostic only" not in page


# ---------------------------------------------------------------------------
# O-V1/O-V2/B3/B4/B5 -- §16.3 decision-core era (Fix 2): the panel-hint
# clause and the closing note pick exactly one of the three eras
# (`_decision_core_era`, web/verification.py:962-975), and the wire
# envelope's own version never follows the run's.
# ---------------------------------------------------------------------------


def _render_headline_page(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    verdicts: list[tuple[str, str, int | None, int, dict[str, object]]],
    *,
    methodology_version: int,
    headline_row: tuple[object, ...] | None = None,
) -> tuple[str, int]:
    """A published run at the given methodology version, with an optional
    headline row, rendered. Returns the page and the run id.

    A caller that wants the Decision sample column populated must pass a
    ``headline_row`` alongside a family carrying a real ``window.per_lead``
    entry (``_O18_FAMILY``'s shape) -- otherwise ``_decision_days`` (web/
    verification.py:738-753) returns None for the row regardless of era,
    the column renders an em-dash either way, and an oracle built on it
    would pass against the strict branch without noticing.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    run_id = _run_with_verdicts(
        conn, site_id, verdicts, methodology_version=methodology_version
    )
    if headline_row is not None:
        columns = (
            "run_id, variable, lead, quantity, entity_type, entity_key, headline, "
            "common_days, mae, bias, rmse, hits, misses, false_alarms, "
            "correct_negatives, ets, availability_rate, delta_vs_incumbent"
        )
        conn.execute(
            f"INSERT INTO verification_results ({columns}) "
            f"VALUES ({', '.join('?' * 18)})",
            (run_id, *headline_row),
        )
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/verification?site={site_id}")
    assert response.status_code == 200
    return response.text, run_id


#: The O18 headline row shape (wind D1, quantity wind_max, candidate depth
#: 3) -- reused so a v2-era page has a real Decision-sample value to check
#: (see ``_render_headline_page``'s docstring).
_O_V_HEADLINE_ROW: tuple[object, ...] = (
    "wind",
    1,
    "wind_max",
    "depth",
    "3",
    1,
    0,
    0.5,
    0.05,
    0.6,
    None,
    None,
    None,
    None,
    None,
    0.95,
    0.1,
)

_PAIRWISE_CLAUSE = "the Decision sample column is the pairwise count the verdict used"
_STRICT_CLAUSE = "scored before pairwise decision samples were recorded"
_UNKNOWN_CLAUSE = "this build does not carry"


def test_o_v1_strict_era_page_shows_only_the_strict_prose(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O-V1: methodology_version 1 (< PAIRWISE_DECISION_CORE_SINCE) renders
    the `strict` era -- exactly one of the three panel-hint clauses and one
    of the three closing-note markers, paired against O-V2 below.
    """
    page, _ = _render_headline_page(
        tmp_path,
        monkeypatch,
        [("wind", "recommend", 3, 2, _O18_FAMILY)],
        methodology_version=1,
        headline_row=_O_V_HEADLINE_ROW,
    )
    # mutant -> at 16.3 panel-hint (era clause): correct = the strict clause
    # present, mutant (a `decision_core_era` that always resolves
    # 'pairwise', e.g. dropping contract.py:18's `>=` threshold check) = the
    # pairwise clause present instead.
    assert _STRICT_CLAUSE in page
    assert _PAIRWISE_CLAUSE not in page
    assert _UNKNOWN_CLAUSE not in page
    # mutant -> at 16.3.one_sample (note presence): correct = present,
    # mutant (the note's era check inverted or dropped) = absent.
    assert 'data-v16="16.3.one_sample"' in page
    assert 'data-v16="16.3.two_samples"' not in page
    assert 'data-v16="16.3.core_unavailable"' not in page


def test_o_v2_pairwise_era_page_shows_only_the_pairwise_prose(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O-V2: methodology_version 2 (this build's own) renders `pairwise` --
    the mirror image of O-V1, so a mutant that drops either bound of the
    `>=` threshold is caught by whichever arm it flips.
    """
    page, _ = _render_headline_page(
        tmp_path,
        monkeypatch,
        [("wind", "recommend", 3, 2, _O18_FAMILY)],
        methodology_version=2,
        headline_row=_O_V_HEADLINE_ROW,
    )
    # mutant -> at 16.3 panel-hint (era clause): correct = the pairwise
    # clause present, mutant (a `decision_core_era` that always resolves
    # 'strict', e.g. the `>=` replaced with `>`) = the strict clause present
    # instead -- dies here where O-V1 cannot (O-V1's arm is < the
    # threshold either way).
    assert _PAIRWISE_CLAUSE in page
    assert _STRICT_CLAUSE not in page
    assert _UNKNOWN_CLAUSE not in page
    assert 'data-v16="16.3.two_samples"' in page
    assert 'data-v16="16.3.one_sample"' not in page
    assert 'data-v16="16.3.core_unavailable"' not in page


def test_o_v4_a_run_newer_than_this_build_is_neither_old_nor_new(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B4 -- the version ceiling the audit added: methodology_version 3 (>
    this build's own 2) must NOT be described as pairwise. An earlier draft
    of this test asserted the page still rendered v2 pairwise prose at
    version 3; that was wrong under the design that shipped -- a run from a
    newer build is `unknown`, not assumed compatible. Combined with O-V1/
    O-V2 above, this pins both edges of the `strict < pairwise <= unknown`
    range, so a mutant that drops either bound is killed.
    """
    page, _ = _render_headline_page(
        tmp_path,
        monkeypatch,
        [("wind", "recommend", 3, 2, _O18_FAMILY)],
        methodology_version=3,
        headline_row=_O_V_HEADLINE_ROW,
    )
    # mutant -> at 16.3 panel-hint / _decision_core_era: correct = the
    # 'unknown' clause present, mutant (dropping the
    # `methodology_version > methodology.METHODOLOGY_VERSION` ceiling
    # check, web/verification.py:971-972, so any version >= the pairwise
    # threshold reads as 'pairwise') = the pairwise clause present instead.
    assert _UNKNOWN_CLAUSE in page
    assert _PAIRWISE_CLAUSE not in page
    assert _STRICT_CLAUSE not in page
    assert 'data-v16="16.3.core_unavailable"' in page
    assert 'data-v16="16.3.two_samples"' not in page
    assert 'data-v16="16.3.one_sample"' not in page


@pytest.mark.parametrize(
    ("methodology_version", "era_id"),
    [(1, "strict"), (3, "unknown")],
)
def test_o_v6_decision_sample_cell_is_dashed_by_the_era_gate_alone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    methodology_version: int,
    era_id: str,
) -> None:
    """O-V6: the §16.3 Decision sample CELL -- not just the panel-hint
    clause and closing-note markers O-V1/O-V2/O-V4 pin -- is suppressed by
    the era gate at `strict` and `unknown`, using `_O18_FAMILY`, which
    carries a POPULATED `window.per_lead` entry (unlike `_O16_FAMILY`
    below). Because the record has a number (47) to print, the era gate is
    the only thing suppressing it here -- a discriminating construction
    B3/O-V3b below cannot offer, since a window-less family already dashes
    the cell for its own reason regardless of era.

    Paired with `test_o_v2_pairwise_era_page_shows_only_the_pairwise_prose`
    at the same methodology_version=2 boundary reading `_O18_FAMILY`'s cell
    as "47" (see `test_o18_decision_sample_column_and_hint_agree_with_
    pairwise_core` above) -- together the three eras pin both directions:
    a mutant that dashes the cell unconditionally is caught by the pairwise
    arm; a mutant that never dashes (the pre-fix behaviour, `{{ r.
    decision_days }}` rendered with no era guard) is caught here.

    COHERENCE: on this SAME render, also proves the cell and the closing
    note agree on one era -- `_decision_core_era` is a single point of
    failure feeding the cell (show.html:284), the panel-hint clause
    (:249), and the closing note (:300-306), so a classifier bug could
    make all three read a wrong-but-consistent era, which the isolated
    per-surface pins (O-V1/O-V2/O-V4, and the cell-only assertions above)
    cannot distinguish from a DECOUPLED page where the cell reads one era
    and the note reads another. Asserting the era-correct note marker is
    present and the other two are absent, on the very page whose cell was
    just read, closes that gap.
    """
    page, _ = _render_headline_page(
        tmp_path,
        monkeypatch,
        [("wind", "recommend", 3, 2, _O18_FAMILY)],
        methodology_version=methodology_version,
        headline_row=_O_V_HEADLINE_ROW,
    )
    row = page[page.index('data-v16="16.3.row" data-variable="wind" data-lead="1"') :]
    row = row[: row.index("</tr>")]
    marker = 'data-v16="16.3.decision_days"'
    cell_start = row.index(">", row.index(marker)) + 1
    cell = row[cell_start : row.index("</td>", cell_start)]
    # mutant -> at 16.3.decision_days: correct = an em-dash with no digit
    # (the era gate suppresses `_O18_FAMILY`'s populated window at
    # `strict`/`unknown`), mutant (the pre-fix cell, `{{ r.decision_days }}`
    # rendered unconditionally) = "47" -- proven by a runtime rebind of the
    # two template expressions against this same context (era=strict:
    # fixed='—', pre-fix='47'; era=unknown: fixed='—',
    # pre-fix='47'; `_decision_days`, web/verification.py:738-753, is
    # era-independent, so both expressions see the same r.decision_days=47).
    assert era_id in ("strict", "unknown")  # sanity: parametrize id matches
    assert "—" in cell
    assert not any(ch.isdigit() for ch in cell)
    # Coherence: the SAME page's closing note must name the SAME era the
    # cell was just read under, not a different one.
    era_note_marker = {
        "strict": "16.3.one_sample",
        "unknown": "16.3.core_unavailable",
    }[era_id]
    other_note_markers = {
        "16.3.two_samples",
        "16.3.one_sample",
        "16.3.core_unavailable",
    } - {era_note_marker}
    # mutant -> at closing-note/cell coherence: correct = the era-correct
    # note marker present and the other two absent on THIS page, mutant (a
    # decoupled page where the cell is dashed by one era while the note
    # renders another, e.g. `_decision_core_era` re-evaluated a second time
    # against a stale/different `run["methodology_version"]` for the note
    # branch) = the era-correct note marker absent or a wrong note marker
    # present instead.
    assert f'data-v16="{era_note_marker}"' in page
    for other in other_note_markers:
        assert f'data-v16="{other}"' not in page


def test_o_v3b_v1_page_dashes_every_decision_sample_cell(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B3 -- the Decision sample column is gated by TWO independent
    conditions (show.html:284: `decision_core_era == 'pairwise' and
    r.decision_days is not none`), and this oracle isolates the second
    half: on a `pairwise`-era (v2) run, a window-less tested-family record
    (`_O16_FAMILY`, whose candidate headline carries no `window` key) must
    still dash the cell, because `_decision_days` (web/verification.py:
    738-753) returns None for it regardless of era.

    Using a `strict`-era run here would NOT discriminate -- the era half of
    the guard alone already forces the dash at `strict`, so a mutant that
    deletes the `r.decision_days is not none` clause would go undetected
    (this is exactly the coverage this test lost, silently, when the
    original v1-era construction predated the era gate). Pairwise-era is
    the only era where the `is not none` clause is load-bearing, matched
    by the `16.3.two_samples` note (this run has SOME pairwise-eligible
    rows, per O-V2, even though this particular family's row does not).
    """
    # _O16_FAMILY's candidate headline carries no "window" key -- a
    # realistic pairwise-era shape for a record whose endpoint never
    # recorded pairwise decision-sample data (e.g. an incumbent-only cell).
    page, _ = _render_headline_page(
        tmp_path,
        monkeypatch,
        [("wind", "recommend", 3, 2, _O16_FAMILY)],
        methodology_version=2,
        headline_row=_O_V_HEADLINE_ROW,
    )
    assert 'data-v16="16.3.two_samples"' in page
    assert _PAIRWISE_CLAUSE in page
    row = page[page.index('data-v16="16.3.row" data-variable="wind" data-lead="1"') :]
    row = row[: row.index("</tr>")]
    # mutant -> at 16.3.decision_days: correct = an em-dash (_O16_FAMILY's
    # headline carries no `window` key, so `_decision_days` returns None
    # for this cell even though the era is `pairwise`), mutant (the
    # `r.decision_days is not none` clause dropped, so a pairwise-era row
    # with no window data renders `r.decision_days` -- here `None` --
    # unguarded, or a mutant that renders a strict-core-style count
    # whenever the verdict happens to hold ANY numeric field) = a digit
    # renders instead, or the raw string "None" appears.
    marker = 'data-v16="16.3.decision_days"'
    cell_start = row.index(">", row.index(marker)) + 1
    cell = row[cell_start : row.index("</td>", cell_start)]
    assert "—" in cell
    assert not any(ch.isdigit() for ch in cell)


def test_o_v7_the_response_envelope_version_does_not_follow_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O-V7 (page+API pairing) -- `verification_schema` is a property of
    this build, never of the run it describes (contract.py:15), so it must
    still read 2 on a version-1 run, on both surfaces a mutant could gate
    it from.
    """
    page, run_id = _render_headline_page(
        tmp_path,
        monkeypatch,
        [("wind", "recommend", 3, 2, _O18_FAMILY)],
        methodology_version=1,
    )
    # mutant -> at 16.5.schema: correct = "verification_schema 2" (the
    # build constant), mutant (a template that reads
    # `run.methodology_version` instead of the injected `verification_
    # schema` context value) = "verification_schema 1".
    assert "verification_schema 2" in page
    assert "verification_schema 1" not in page

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        api = client.get(f"/api/verification/runs/{run_id}").json()
    # mutant -> at the API route's verification_schema field: correct = 2
    # (VERIFICATION_SCHEMA, contract.py:15), mutant (a route that echoes
    # the row's own `methodology_version` under this key instead of the
    # module constant) = 1.
    assert api["verification_schema"] == 2


# ---------------------------------------------------------------------------
# O19/O19b -- the recommending card, and every NULL-depth outcome's own
# decision statement.
# ---------------------------------------------------------------------------

_O19_FAMILY: dict[str, object] = {
    "incumbent": "2",
    "candidates": {
        "3": {
            "headline": {
                "adequate_leads": [1, 2, 3, 4],
                "pooled_point": 0.25,
                "ci": [0.15, 0.35],
                "per_lead": {"1": 0.25, "2": 0.25, "3": 0.25, "4": 0.25},
                "dropped_leads": [],
                "window": {
                    "first": "2026-05-01",
                    "last": "2026-06-16",
                    "days": 47,
                    "per_lead": {
                        str(ld): {
                            "first": "2026-05-01",
                            "last": "2026-06-16",
                            "days": 47,
                        }
                        for ld in (1, 2, 3, 4)
                    },
                },
            },
            "conditions": {
                "ci_excludes_zero": True,
                "lead_stability": True,
                "practical_floor": True,
                "beats_baselines": True,
            },
            "baselines": {"baseline_persistence": {"passed": True, "ci": [0.3, 0.5]}},
            "components": {},
        }
    },
    "statistically_unresolved": [],
}


def test_o19_recommending_card_renders_primary_values_not_the_not_applicable_phrase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = _render(tmp_path, monkeypatch, [("wind", "recommend", 3, 2, _O19_FAMILY)])
    card = _card(page, "wind")
    assert 'data-v16="16.2.decision_statement"' not in card
    assert "Not applicable" not in page
    assert "0.250 (Headline)" in card
    assert "[0.150, 0.350]" in card
    assert "4 of 4 required" in card
    assert "Practical floor: pass" in card
    assert "persistence: pass" in card
    assert "CI excludes zero: pass" in card


@pytest.mark.parametrize(
    "outcome",
    ["retain_incumbent", "insufficient_evidence", "mixed_by_lead", "mixed_by_quantity"],
)
def test_o19b_every_null_depth_outcome_gets_its_own_decision_statement(
    outcome: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = _render(tmp_path, monkeypatch, [("wind", outcome, None, 2, _O16_FAMILY)])
    card = _card(page, "wind")
    assert 'data-v16="16.2.decision_statement"' in card
    stmt = card[card.index('data-v16="16.2.decision_statement"') :]
    stmt = stmt[: stmt.index("</p>")]
    assert stmt.strip() != ""


def test_o19b_the_four_decision_statements_are_pairwise_distinct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    outcomes = [
        "retain_incumbent",
        "insufficient_evidence",
        "mixed_by_lead",
        "mixed_by_quantity",
    ]
    statements: set[str] = set()
    for i, outcome in enumerate(outcomes):
        sub = tmp_path / str(i)
        sub.mkdir()
        page = _render(sub, monkeypatch, [("wind", outcome, None, 2, _O16_FAMILY)])
        card = _card(page, "wind")
        stmt = card[card.index('data-v16="16.2.decision_statement"') :]
        stmt = stmt[: stmt.index("</p>")]
        statements.add(stmt)
    # mutant -> at len(statements): correct = 4 (each of the four NULL-depth
    # outcomes gets its own sentence), mutant (one generic line reused for
    # all four, the fall-through in an `{% if %}` chain that covers only
    # `retain_incumbent`) = 1.
    assert len(statements) == 4


def test_o19b_skipped_outcome_shows_skip_reason_not_a_decision_statement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    family = {
        **_O16_FAMILY,
        "reason": "incumbent depth 6 lies outside the simulated range 1-5",
    }
    page = _render(tmp_path, monkeypatch, [("wind", "skipped", None, 6, family)])
    card = _card(page, "wind")
    # mutant -> at 16.2.decision_statement: correct = absent, with
    # 16.2.skip_reason present instead (D13's deliberate exception), mutant
    # (a mapping that emits a statement for "skipped" too) = present,
    # duplicating 16.2.skip_reason.
    assert 'data-v16="16.2.decision_statement"' not in card
    assert 'data-v16="16.2.skip_reason"' in card
    assert "incumbent depth 6 lies outside the simulated range 1-5" in card


def test_o19b_unknown_outcome_still_renders_a_default_statement_html_escaped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The outcome value carries an HTML-special payload that reaches
    # rendered text (§16.2's default-statement branch echoes `v.outcome`
    # verbatim) -- the payload itself is what makes the escaping half of
    # this test able to fail; an outcome with no special character can
    # never distinguish an escaping template from one that doesn't escape.
    outcome = "some_future_outcome<script>evil()</script>"
    page = _render(tmp_path, monkeypatch, [("wind", outcome, None, 2, _O16_FAMILY)])
    card = _card(page, "wind")
    assert 'data-v16="16.2.decision_statement"' in card
    escaped = "some_future_outcome&lt;script&gt;evil()&lt;/script&gt;"
    assert f"No candidate depth was recommended. ({escaped})" in card
    assert "<script>evil()</script>" not in card


# ---------------------------------------------------------------------------
# O20 -- baseline comparisons publish their interval and their own window,
# on both variable shapes.
# ---------------------------------------------------------------------------

_O20_WIND_FAMILY: dict[str, object] = {
    "incumbent": "2",
    "candidates": {
        "3": {
            "headline": {
                "adequate_leads": [1],
                "pooled_point": 0.1,
                "ci": [0.05, 0.15],
                "per_lead": {"1": 0.1},
                "dropped_leads": [],
            },
            "conditions": {
                "ci_excludes_zero": True,
                "lead_stability": True,
                "practical_floor": True,
                "beats_baselines": True,
            },
            "baselines": {
                "baseline_persistence": {
                    "passed": True,
                    "ci": [0.2, 0.4],
                    "window": {
                        "first": "2026-05-01",
                        "last": "2026-05-10",
                        "days": 10,
                        "per_lead": {},
                    },
                },
                "baseline_all_feed_mean": {"passed": False, "ci": None},
            },
            "components": {},
        }
    },
    "statistically_unresolved": [],
}


def test_o20_baseline_comparisons_publish_interval_and_window_flat_shape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = _render(
        tmp_path, monkeypatch, [("wind", "retain_incumbent", None, 2, _O20_WIND_FAMILY)]
    )
    row = page[page.index('data-candidate="3" data-endpoint="headline"') :]
    row = row[: row.index("</tr>")]
    # Baselines render in name-sorted order: all_feed_mean, then persistence.
    allfeed_frag = row[row.index("all_feed_mean:") : row.index("persistence:")]
    persistence_frag = row[row.index("persistence:") :]
    assert "[0.200, 0.400]" in persistence_frag
    assert "2026-05-01&ndash;2026-05-10 (10 days)" in persistence_frag
    # mutant -> at the all_feed_mean baseline's CI cell: correct = '—' (this
    # baseline's stored `ci` is `null`), mutant (a formatter that raises on
    # `None` or prints an empty string) = a 500 or a blank cell -- the '—'
    # is what distinguishes "measured and failed" from "never measured".
    assert "—" in allfeed_frag
    # mutant -> at the all_feed_mean baseline_window span: correct = '—'
    # (this baseline detail carries no "window" key at all), mutant (a
    # window accessor that renders "0 days" on a missing key instead of
    # falling through to the no-block case) = "0 days" where nothing was
    # ever measured.
    assert "0 days" not in allfeed_frag


_O20_PRECIP_FAMILY: dict[str, object] = {
    "incumbent": "2",
    "candidates": {
        "3": {
            "total": {
                "adequate_leads": [1],
                "pooled_point": 0.1,
                "ci": [0.05, 0.15],
                "per_lead": {"1": 0.1},
                "dropped_leads": [],
            },
            "occurrence": {
                "adequate_leads": [1],
                "pooled_point": 0.05,
                "ci": [0.01, 0.09],
                "per_lead": {"1": 0.05},
                "dropped_leads": [],
            },
            "conditions": {
                "total_material": False,
                "occurrence_material": False,
                "total_non_inferior": True,
                "occurrence_non_inferior": True,
                "beats_baselines": False,
            },
            "baselines": {
                "total": {
                    "baseline_persistence": {
                        "passed": True,
                        "ci": [0.2, 0.3],
                        "window": {
                            "first": "2026-05-01",
                            "last": "2026-05-12",
                            "days": 12,
                            "per_lead": {},
                        },
                    },
                },
                "occurrence": {
                    "baseline_persistence": {
                        "passed": False,
                        "ci": [0.0, 0.1],
                        "window": {
                            "first": "2026-04-01",
                            "last": "2026-04-30",
                            "days": 30,
                            "per_lead": {},
                        },
                    },
                },
            },
            "components": {},
        }
    },
    "statistically_unresolved": [],
}


def test_o20_baseline_comparisons_publish_interval_and_window_nested_shape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = _render(
        tmp_path,
        monkeypatch,
        [("precip", "retain_incumbent", None, 2, _O20_PRECIP_FAMILY)],
    )
    row = page[page.index('data-candidate="3" data-endpoint="total"') :]
    row = row[: row.index("</tr>")]
    # Baseline entries render name-sorted by their full "name (scope)" label:
    # "persistence (occurrence)" precedes "persistence (total)".
    occ_frag = row[
        row.index("persistence (occurrence)") : row.index("persistence (total)")
    ]
    total_frag = row[row.index("persistence (total)") :]
    # mutant -> at the two 16.5.baseline_window spans: correct = two
    # DIFFERENT spans, "2026-05-01&ndash;2026-05-12 (12 days)" for the
    # total-scope baseline and "2026-04-01&ndash;2026-04-30 (30 days)" for
    # the occurrence-scope one, mutant (a cell that prints one endpoint's
    # window for every baseline it lists) = the same span in both fragments.
    assert "2026-05-01&ndash;2026-05-12 (12 days)" in total_frag
    assert "2026-04-01&ndash;2026-04-30 (30 days)" in occ_frag
    assert "2026-04-01&ndash;2026-04-30 (30 days)" not in total_frag


# ---------------------------------------------------------------------------
# O21/O21b -- the card summarises the endpoint the decision was ordered on.
# ---------------------------------------------------------------------------


def _o21_family(
    ordering_name: object, *, drop_occurrence: bool = False
) -> dict[str, object]:
    candidate: dict[str, object] = {
        "total": {
            "adequate_leads": [1, 2],
            "pooled_point": 0.111,
            "ci": [0.100, 0.122],
            "per_lead": {"1": 0.111, "2": 0.111},
            "dropped_leads": [],
            "window": {
                "first": "2026-05-01",
                "last": "2026-05-10",
                "days": 10,
                "per_lead": {},
            },
        },
        "occurrence": {
            "adequate_leads": [1, 2, 3, 4],
            "pooled_point": 0.777,
            "ci": [0.700, 0.850],
            "per_lead": {"1": 0.777, "2": 0.777, "3": 0.777, "4": 0.777},
            "dropped_leads": [],
            "window": {
                "first": "2026-04-01",
                "last": "2026-05-10",
                "days": 40,
                "per_lead": {},
            },
        },
        "conditions": {
            "total_material": ordering_name == "total",
            "occurrence_material": ordering_name == "occurrence",
            "total_non_inferior": True,
            "occurrence_non_inferior": True,
            "beats_baselines": True,
        },
        "baselines": {},
        "components": {},
    }
    if ordering_name is not _UNSET:
        candidate["ordering_endpoint_name"] = ordering_name
    if drop_occurrence:
        del candidate["occurrence"]
    return {
        "incumbent": "2",
        "candidates": {"3": candidate},
        "statistically_unresolved": [],
    }


def test_o21_precip_card_summarises_occurrence_when_ordered_on_occurrence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    family = _o21_family("occurrence")
    # D9's no-basis row: this fixture carries no resolved `tie_break.basis`
    # at all, so the four fields below must come from the ordering
    # endpoint's own row, never a shared-basis recomputation.
    assert "tie_break" not in family
    page = _render(
        tmp_path,
        monkeypatch,
        [("precip", "recommend", 3, 2, family)],
    )
    card = _card(page, "precip")
    # mutant -> at 16.2.primary_effect: correct = "0.777 (Precip occurrence)"
    # (decision.py:803-804 ordered on the occurrence endpoint), mutant (four
    # card fields taking `endpoints[0]`, which `_candidate_view` fixes to
    # `total` for every precip candidate) = "0.111 (Precip total)".
    assert "0.777 (Precip occurrence)" in card
    assert "[0.700, 0.850]" in card
    assert "4 of 4 required" in card
    assert "2026-04-01" in card
    assert "2026-05-10 (40 days)" in card
    assert "0.111" not in card
    assert "[0.100, 0.122]" not in card
    assert "0.111" in page  # still present in the §16.5 tested-family row


def test_o21_control_precip_card_summarises_total_when_ordered_on_total(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    family = _o21_family("total")
    # D9's no-basis row: no resolved `tie_break.basis` here either.
    assert "tie_break" not in family
    page = _render(tmp_path, monkeypatch, [("precip", "recommend", 3, 2, family)])
    card = _card(page, "precip")
    assert "0.111 (Precip total)" in card
    assert "[0.100, 0.122]" in card
    assert "2 of 4 required" in card
    assert "0.777" not in card
    assert "[0.700, 0.850]" not in card


def test_o21_control_wind_card_is_byte_identical_since_headline_is_endpoints_0(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # D9's no-basis row: `_O19_FAMILY` carries no resolved `tie_break.basis`
    # either -- the card fields below come from the wind headline row.
    assert "tie_break" not in _O19_FAMILY
    page = _render(tmp_path, monkeypatch, [("wind", "recommend", 3, 2, _O19_FAMILY)])
    card = _card(page, "wind")
    assert "0.250 (Headline)" in card


def test_o21b_a_absent_selector_falls_back_to_first_listed_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    family = _o21_family(_UNSET)
    # D9's no-basis row: no resolved `tie_break.basis` here.
    assert "tie_break" not in family
    page = _render(tmp_path, monkeypatch, [("precip", "recommend", 3, 2, family)])
    card = _card(page, "precip")
    assert "0.111 (Precip total)" in card
    assert 'data-v16="16.2.ordering_endpoint_unresolved"' not in card


def test_o21b_b_resolvable_name_summarises_the_named_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    family = _o21_family("occurrence")
    # D9's no-basis row: no resolved `tie_break.basis` here.
    assert "tie_break" not in family
    page = _render(
        tmp_path,
        monkeypatch,
        [("precip", "recommend", 3, 2, family)],
    )
    card = _card(page, "precip")
    assert "0.777 (Precip occurrence)" in card
    assert 'data-v16="16.2.ordering_endpoint_unresolved"' not in card


def test_o21b_c_unresolvable_name_is_disclosed_not_silently_substituted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    family = _o21_family("occurrence", drop_occurrence=True)
    # D9's no-basis row: no resolved `tie_break.basis` here.
    assert "tie_break" not in family
    page = _render(
        tmp_path,
        monkeypatch,
        [
            ("wind", "retain_incumbent", None, 2, _O16_FAMILY),
            ("precip", "recommend", 3, 2, family),
        ],
    )
    card = _card(page, "precip")
    assert 'data-v16="16.2.ordering_endpoint_unresolved"' in card
    assert "occurrence" in card
    # D13's not-AVAILABLE phrase, never not-applicable: a depth WAS
    # recommended (recommended_depth=3, and "3" is a key of
    # tested_family.candidates), so "no recommended depth" is false on this
    # card. All four selector-read fields carry it -- these fixtures carry
    # no resolved tie_break.basis, so nothing endpoint-independent is
    # available to fill the lead count or the window from either.
    for field_name in (
        "primary_effect",
        "primary_ci",
        "adequate_leads",
        "decision_window",
    ):
        assert "Not available" in _field(card, field_name)
    assert "Not applicable" not in card
    # mutant -> at 16.2.primary_effect (and the other three selector-read
    # fields): correct = the not-available phrase (the record names
    # "occurrence" but does not hold it, so D14 refuses to substitute
    # another endpoint's numbers), mutant (the collapse mutant: reusing the
    # absent-case tuple-order fallback) = "0.111 (Precip total)".
    assert "0.111" not in card
    assert 'data-candidate="3" data-endpoint="total"' in page
    assert 'data-candidate="3" data-endpoint="occurrence"' not in page
    # The run's other verdict still renders.
    assert 'data-v16="16.2.card" data-variable="wind"' in page


def _o21f_family(ordering_name: str) -> dict[str, object]:
    """A candidate naming a selector while holding NO endpoint at all.

    D14's selector is read from ``record.get("ordering_endpoint_name")``
    *before* the emptiness guard (``web/verification.py:429-434``), so a
    record that names an endpoint while holding none still discloses the
    name -- rather than losing it to the guard, the way the pre-fix
    ``if not endpoints: return None, None`` placed ahead of the read would.
    """
    return {
        "incumbent": "2",
        "candidates": {
            "3": {
                "ordering_endpoint_name": ordering_name,
                "conditions": {},
                "baselines": {},
                "components": {},
            }
        },
        "statistically_unresolved": [],
    }


def test_o21f_a_named_selector_on_an_endpointless_record_still_discloses_the_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    family = _o21f_family("occurrence")
    # D9's no-basis row: this fixture carries no resolved `tie_break.basis`.
    assert "tie_break" not in family
    page = _render(
        tmp_path,
        monkeypatch,
        [("precip", "recommend", 3, 2, family)],
    )
    card = _card(page, "precip")
    # mutant -> at 16.2.ordering_endpoint_unresolved (presence): correct =
    # present, naming "occurrence" (the selector is read BEFORE the
    # emptiness guard, `web/verification.py:429-434`), mutant (the pre-fix
    # early return `if not endpoints: return None, None` placed ahead of the
    # selector read) = the field never renders at all, because
    # `ordering_endpoint_unresolved` stays `None` on an empty-endpoint
    # record whatever selector it named.
    assert 'data-v16="16.2.ordering_endpoint_unresolved"' in card
    unresolved_field = _field(card, "ordering_endpoint_unresolved")
    assert "occurrence" in unresolved_field
    # The empty-set arm (show.html:201): the tail states the record holds no
    # endpoint at all, never the table-below-lists-them claim that arm makes
    # when `v.primary.endpoints` is non-empty.
    assert "This record holds no endpoint at all" in unresolved_field
    assert "table below lists the endpoints the record does hold" not in (
        unresolved_field
    )
    # The four selector-dependent summary fields carry D13's not-available
    # phrase: there is no endpoint to name, and no basis resolved either.
    for field_name in (
        "primary_effect",
        "primary_ci",
        "adequate_leads",
        "decision_window",
    ):
        assert "Not available" in _field(card, field_name)


def test_o21b_d_an_unresolvable_name_is_html_escaped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    family = _o21_family("<b>x</b>", drop_occurrence=True)
    # D9's no-basis row: no resolved `tie_break.basis` here.
    assert "tie_break" not in family
    page = _render(
        tmp_path,
        monkeypatch,
        [("precip", "recommend", 3, 2, family)],
    )
    card = _card(page, "precip")
    assert "&lt;b&gt;x&lt;/b&gt;" in card
    assert "<b>x</b>" not in card


# ---------------------------------------------------------------------------
# O22/O22b -- a refused ordering says so on the page, in words.
# ---------------------------------------------------------------------------

_O22_TOKENS = (
    "thin_shared_basis",
    "thin_shared_events",
    "mixed_endpoint_kind",
    "undefined_restricted_ci",
)


def _o22_family(reason: str) -> dict[str, object]:
    def _cand(point: float, ci: list[float]) -> dict[str, object]:
        return {
            "headline": {
                "adequate_leads": [1],
                "pooled_point": point,
                "ci": ci,
                "per_lead": {"1": point},
                "dropped_leads": [],
                "window": {
                    "first": "2026-05-01",
                    "last": "2026-05-05",
                    "days": 5,
                    "per_lead": {},
                },
            },
            "conditions": {
                "ci_excludes_zero": True,
                "lead_stability": True,
                "practical_floor": True,
                "beats_baselines": True,
            },
            "baselines": {},
            "components": {},
        }

    return {
        "incumbent": "2",
        "candidates": {"5": _cand(0.1, [0.05, 0.15]), "6": _cand(0.2, [0.1, 0.3])},
        "statistically_unresolved": ["6"],
        "tie_break": {"chosen": "5", "basis": {"reason": reason}},
    }


@pytest.mark.parametrize("reason", _O22_TOKENS)
def test_o22a_a_refused_ordering_names_itself_and_does_not_claim_an_overlap_test(
    reason: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = _render(
        tmp_path, monkeypatch, [("wind", "recommend", 5, 2, _o22_family(reason))]
    )
    card = _card(page, "wind")
    assert 'data-v16="16.2.ordering_refusal"' in card
    # mutant -> at 16.2.ordering_refusal: correct = the D5 wet/dry or
    # mixed-unit sentence, never the raw enum token (today's tree renders
    # `tie_break` nowhere at all), mutant (a template that echoes the raw
    # token with no mapped sentence) = the literal reason token as the only
    # visible text.
    assert reason not in card
    assert "statistically unresolved against the chosen depth" not in card.lower()
    # D9's refusal row: a refused ordering must NOT blank the four
    # decision-summary fields -- they still carry the ordering endpoint's
    # OWN point, interval, adequate count and window, unchanged.
    assert "0.100 (Headline)" in card
    assert "[0.050, 0.150]" in card
    assert "1 of 4 required" in card
    assert "2026-05-01" in card
    assert "2026-05-05 (5 days)" in card
    assert "Not applicable" not in card


def test_o22b_the_four_refusal_sentences_are_pairwise_distinct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sentences: set[str] = set()
    for i, reason in enumerate(_O22_TOKENS):
        sub = tmp_path / str(i)
        sub.mkdir()
        page = _render(
            sub, monkeypatch, [("wind", "recommend", 5, 2, _o22_family(reason))]
        )
        card = _card(page, "wind")
        frag = card[card.index('data-v16="16.2.ordering_refusal"') :]
        frag = frag[: frag.index("</p>")]
        sentences.add(frag)
    # mutant -> at len(sentences): correct = 4 (each refusal token maps to
    # its own sentence per the plan's D5 table), mutant (a template that
    # renders one generic line for every token) = 1.
    assert len(sentences) == 4


def test_o22c_a_resolved_basis_control_renders_no_ordering_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    family = _o22_family("thin_shared_basis")
    family["tie_break"] = {"chosen": "6", "best_by_pooled": "6"}
    family["statistically_unresolved"] = []
    page = _render(tmp_path, monkeypatch, [("wind", "recommend", 6, 2, family)])
    card = _card(page, "wind")
    assert 'data-v16="16.2.ordering_refusal"' not in card


def test_o22d_an_unknown_refusal_token_degrades_to_a_legible_sentence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = _render(
        tmp_path,
        monkeypatch,
        [("wind", "recommend", 5, 2, _o22_family("some_future_token"))],
    )
    card = _card(page, "wind")
    assert 'data-v16="16.2.ordering_refusal"' in card
    assert (
        "Depths could not be compared; the depth shown is the one closest "
        "to the incumbent. (some_future_token)"
    ) in card


# ---------------------------------------------------------------------------
# O23 -- a recommended depth whose candidate record is missing is disclosed
# as a defect, not as an absence of recommendation. `tested_family.candidates`
# holds only keys "1" and "2"; `recommended_depth` names "3".
# ---------------------------------------------------------------------------

_O23_FAMILY: dict[str, object] = {
    "incumbent": "2",
    "candidates": {
        "1": {
            "headline": {
                "adequate_leads": [1, 2],
                "pooled_point": 0.111,
                "ci": [0.100, 0.122],
                "per_lead": {"1": 0.111, "2": 0.111},
                "dropped_leads": [],
                "window": {
                    "first": "2026-05-01",
                    "last": "2026-05-10",
                    "days": 10,
                    "per_lead": {},
                },
            },
            "conditions": {
                "ci_excludes_zero": True,
                "lead_stability": True,
                "practical_floor": True,
                "beats_baselines": True,
            },
            "baselines": {"baseline_persistence": {"passed": True, "ci": [0.1, 0.2]}},
            "components": {},
        },
        "2": {
            "headline": {
                "adequate_leads": [1, 2, 3],
                "pooled_point": 0.222,
                "ci": [0.200, 0.244],
                "per_lead": {"1": 0.222, "2": 0.222, "3": 0.222},
                "dropped_leads": [],
                "window": {
                    "first": "2026-04-01",
                    "last": "2026-05-10",
                    "days": 40,
                    "per_lead": {},
                },
            },
            "conditions": {
                "ci_excludes_zero": True,
                "lead_stability": True,
                "practical_floor": True,
                "beats_baselines": True,
            },
            "baselines": {"baseline_persistence": {"passed": True, "ci": [0.2, 0.3]}},
            "components": {},
        },
    },
    "statistically_unresolved": [],
}

# The six primary-gated fields, plus 16.2.decision_window -- seven fields
# total, none of which may depend on the missing depth's own candidate data.
_O23_SEVEN_FIELDS = (
    "primary_effect",
    "primary_ci",
    "adequate_leads",
    "practical_significance",
    "baseline_gates",
    "completeness_guards",
    "decision_window",
)

# Every value that identifies candidate "1" or "2"'s own record -- none of
# these may leak into a not-available field.
_O23_CANDIDATE_VALUES = (
    "0.111",
    "0.222",
    "0.100",
    "0.122",
    "0.200",
    "0.244",
    "10 days",
    "40 days",
)


def test_o23_recommended_depth_with_no_matching_candidate_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = _render(
        tmp_path,
        monkeypatch,
        [
            ("wind", "retain_incumbent", None, 2, _O16_FAMILY),
            ("precip", "recommend", 3, 2, _O23_FAMILY),
        ],
    )
    card = _card(page, "precip")

    # (b) the stored depth still prints -- it is the evidence of the
    # inconsistency, not a thing to hide.
    assert "3" in _field(card, "recommended_depth")

    # (c) the six primary-gated fields and 16.2.decision_window carry D13's
    # not-available phrase, and "no recommended depth" -- the not-applicable
    # phrase's own claim -- never appears on this card. The separating
    # assertion, and the whole subject of the oracle.
    for name in _O23_SEVEN_FIELDS:
        assert "Not available" in _field(card, name)
    assert "no recommended depth" not in card
    assert "Not applicable" not in card

    # (d) 16.2.decision_window does not carry the no-recommendation sentence
    # either -- a depth WAS recommended, so that claim would also be false.
    assert "No single decision window applies" not in _field(card, "decision_window")

    # (e) 16.2.primary_missing is present and names the literal depth.
    missing_field = _field(card, "primary_missing")
    assert 'data-v16="16.2.primary_missing"' in missing_field
    assert "3" in missing_field

    # (f) no decision statement: a depth was recommended, so `no_recommendation`
    # is false.
    assert 'data-v16="16.2.decision_statement"' not in card

    # (g) neither tested candidate's own values leak into any of the seven
    # not-available fields.
    for name in _O23_SEVEN_FIELDS:
        field = _field(card, name)
        for value in _O23_CANDIDATE_VALUES:
            assert value not in field

    # (a) the run's other verdict still renders untouched.
    assert 'data-v16="16.2.card" data-variable="wind"' in page


def test_o23_control_null_depth_retain_incumbent_pins_the_two_states_apart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Same fixture, but recommended_depth NULL and outcome retain_incumbent
    # -- the "no candidate depth was recommended" state O23 must not be
    # confused with. `primary_missing` fires only when `recommended_depth`
    # is non-NULL and unmatched; a `primary is None` computation would fire
    # here too, since NULL depth also leaves `primary` at None.
    page = _render(
        tmp_path, monkeypatch, [("precip", "retain_incumbent", None, 2, _O23_FAMILY)]
    )
    card = _card(page, "precip")
    assert 'data-v16="16.2.primary_missing"' not in card
    assert 'data-v16="16.2.decision_statement"' in card
    # The six primary-gated fields take the not-applicable phrase.
    for name in (
        "primary_effect",
        "primary_ci",
        "adequate_leads",
        "practical_significance",
        "baseline_gates",
        "completeness_guards",
    ):
        assert "Not applicable" in _field(card, name)
    # 16.2.decision_window has its own no-recommendation sentence (D13),
    # never the shared not-applicable phrase.
    assert "No single decision window applies" in _field(card, "decision_window")


def test_o22d_a_script_tag_refusal_token_is_escaped_not_executed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    page = _render(
        tmp_path,
        monkeypatch,
        [("wind", "recommend", 5, 2, _o22_family("<script>x</script>"))],
    )
    card = _card(page, "wind")
    assert "&lt;script&gt;x&lt;/script&gt;" in card
    assert "<script>x</script>" not in card
