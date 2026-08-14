"""§16/§18.11 oracle suite: the read-only /api/verification/* contract.

Adversarial complements to tests/test_phase7_surface.py: the schema
constant pinned by value in every endpoint payload, the confidence
levels pinned to their hand-computed Bonferroni values, limit/offset
clamp BOUNDARIES on both pagers (501 vs 500, 0 vs 1, negative offset),
deterministic id-ordered paging whose pages are disjoint and reassemble
the full set, filter conjunction kills (each eligibility branch selects
exactly one hand-placed row; dropping either conjunct of ``eligible``
admits a decoy), run-scoping (one run's evidence never leaks into
another's page), the 404 sweep over every run-scoped endpoint, the
/latest pointer resolved against a strictly NEWER decoy run, and the
§18.11 three-way consistency oracle (rendered page == JSON payload ==
persisted rows for verdicts, counts, units, exclusions, run identity,
and the never-numeric-zero null encoding).

Synthetic fixtures only: fake town names, UTC, invented feed ids.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.test_phase7_surface import (
    _init_tmp_db,
    _make_app,
    _make_site,
    _seed_published_run,
)
from wxverify.api.routes.verification import VERIFICATION_SCHEMA
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.runs import capture_config_snapshot

# ---------------------------------------------------------------------------
# Fixture: two runs; run A carries six hand-placed evidence rows chosen so
# every filter selects a distinct, known subset; run B carries one decoy.
# ---------------------------------------------------------------------------

_ROW_TUPLE = tuple[str, int, str, str, str]


def _insert_run(conn: sqlite3.Connection, site_id: int) -> int:
    generation = ensure_published_generation(conn, site_id)
    # A full pinned snapshot, as the production write path stores: readers
    # rehydrate a RunConfig from this column, so '{}' is unproducible.
    snapshot = json.dumps(capture_config_snapshot(conn, site_id), separators=(",", ":"))
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, period_start, period_end,
             settled_through, bootstrap_seed, bootstrap_resamples,
             input_fingerprint)
        VALUES (?, ?, 1, 'test', 'running', 1, ?, '2026-05-01',
                '2026-05-30', '2026-05-30', 1, 40, 'fp-contract')
        """,
        (site_id, generation, snapshot),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


# (variable, lead, quantity, entity_type, entity_key, fe, te)
_RUN_A_ROWS: list[tuple[str, int, str, str, str, int, int]] = [
    ("temperature", 1, "temperature_high", "depth", "3", 1, 1),  # id order 1
    ("temperature", 1, "temperature_high", "depth", "4", 0, 1),  # 2 fc-inelig
    ("temperature", 1, "temperature_high", "feed", "101", 1, 0),  # 3 tr-inelig
    ("temperature", 2, "temperature_high", "depth", "3", 1, 1),  # 4
    ("wind", 1, "wind_max", "depth", "3", 1, 1),  # 5
    ("precip", 1, "precip_occurrence", "depth", "3", 1, 1),  # 6
]


def _seed_evidence(
    conn: sqlite3.Connection,
    run_id: int,
    rows: list[tuple[str, int, str, str, str, int, int]],
) -> None:
    for variable, lead, quantity, entity_type, entity_key, fe, te in rows:
        conn.execute(
            """
            INSERT INTO verification_evidence
                (run_id, snapshot_local_date, target_local_date, lead,
                 variable, quantity, entity_type, entity_key, predicted,
                 forecast_eligible, truth_value, truth_eligible, abs_error)
            VALUES (?, '2026-05-01', '2026-05-02', ?, ?, ?, ?, ?, 1.0, ?,
                    2.0, ?, 1.0)
            """,
            (run_id, lead, variable, quantity, entity_type, entity_key, fe, te),
        )


def _tuples(payload: dict[str, object]) -> set[_ROW_TUPLE]:
    evidence = payload["evidence"]
    assert isinstance(evidence, list)
    return {
        (
            str(row["variable"]),
            int(row["lead"]),
            str(row["quantity"]),
            str(row["entity_type"]),
            str(row["entity_key"]),
        )
        for row in evidence
    }


@pytest.fixture
def contract_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[TestClient, int, int, int]]:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Contract Town")
    run_a = _insert_run(conn, site_id)
    run_b = _insert_run(conn, site_id)
    _seed_evidence(conn, run_a, _RUN_A_ROWS)
    # Run-B decoy: same variable/lead shape, distinctive entity_key.
    _seed_evidence(conn, run_b, [("wind", 1, "wind_max", "depth", "9", 1, 1)])
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        yield client, site_id, run_a, run_b


# ---------------------------------------------------------------------------
# Schema version + measurement contract.
# ---------------------------------------------------------------------------


def test_schema_constant_is_one_and_stamped_on_every_endpoint(
    contract_client: tuple[TestClient, int, int, int],
) -> None:
    client, site_id, run_a, _ = contract_client
    assert VERIFICATION_SCHEMA == 1  # the wire contract version itself
    endpoints = [
        f"/api/verification/status?site={site_id}",
        f"/api/verification/runs?site={site_id}",
        f"/api/verification/runs/{run_a}",
        f"/api/verification/runs/{run_a}/verdicts",
        f"/api/verification/runs/{run_a}/evidence",
        f"/api/verification/runs/{run_a}/diagnostics",
        f"/api/verification/runs/{run_a}/methodology",
    ]
    for url in endpoints:
        payload = client.get(url).json()
        assert payload["verification_schema"] == 1, url


def test_contract_confidence_levels_hand_pinned(
    contract_client: tuple[TestClient, int, int, int],
) -> None:
    client, site_id, _, _ = contract_client
    contract = client.get(f"/api/verification/status?site={site_id}").json()["contract"]
    levels = contract["confidence_levels"]
    # Hand-derived Bonferroni splits of alpha=0.05 (§17): 3-way candidate
    # family 1 - 0.05/3 = 59/60; 6-way precip family 1 - 0.05/6 = 119/120;
    # baseline gate plain 95%.
    assert levels["candidate_ci"] == pytest.approx(59 / 60)
    assert levels["precip_improvement_ci"] == pytest.approx(119 / 120)
    assert levels["baseline_gate_ci"] == pytest.approx(0.95)
    assert contract["metric_direction"]["mae"] == "lower_is_better"
    assert contract["metric_direction"]["ets"] == "higher_is_better"


# ---------------------------------------------------------------------------
# Clamp boundaries — evidence pager (max 500, min 1, offset floor 0) and
# runs pager (max 200, min 1).
# ---------------------------------------------------------------------------


def test_evidence_limit_clamp_boundaries(
    contract_client: tuple[TestClient, int, int, int],
) -> None:
    client, _, run_a, _ = contract_client
    base = f"/api/verification/runs/{run_a}/evidence"
    assert client.get(f"{base}?limit=501").json()["limit"] == 500
    assert client.get(f"{base}?limit=500").json()["limit"] == 500
    low = client.get(f"{base}?limit=0").json()
    assert low["limit"] == 1
    assert len(low["evidence"]) == 1  # the clamp actually bounds the page
    assert client.get(f"{base}?limit=-3").json()["limit"] == 1
    neg_offset = client.get(f"{base}?limit=1&offset=-4").json()
    assert neg_offset["offset"] == 0
    # A negative offset floors to 0: the page starts at the FIRST row.
    all_ids = [row["id"] for row in client.get(base).json()["evidence"]]
    assert neg_offset["evidence"][0]["id"] == all_ids[0]


def test_runs_limit_clamp_boundaries(
    contract_client: tuple[TestClient, int, int, int],
) -> None:
    client, site_id, _, _ = contract_client
    base = f"/api/verification/runs?site={site_id}"
    assert client.get(f"{base}&limit=9999").json()["limit"] == 200
    low = client.get(f"{base}&limit=0").json()
    assert low["limit"] == 1
    assert len(low["runs"]) == 1  # two runs exist; the clamp bounds the page


# ---------------------------------------------------------------------------
# Deterministic paging: ORDER BY id, disjoint pages, full reassembly.
# ---------------------------------------------------------------------------


def test_evidence_paging_is_disjoint_ordered_and_complete(
    contract_client: tuple[TestClient, int, int, int],
) -> None:
    client, _, run_a, _ = contract_client
    base = f"/api/verification/runs/{run_a}/evidence"
    full = [row["id"] for row in client.get(base).json()["evidence"]]
    assert len(full) == 6
    assert full == sorted(full)
    pages: list[list[int]] = []
    for offset in (0, 2, 4):
        page = client.get(f"{base}?limit=2&offset={offset}").json()["evidence"]
        assert len(page) == 2
        pages.append([row["id"] for row in page])
    flat = [i for page in pages for i in page]
    assert flat == full  # disjoint, ordered, reassembles exactly


# ---------------------------------------------------------------------------
# Filters — each one selects the hand-placed subset; the eligibility
# branches each pick exactly one decoy-guarded row.
# ---------------------------------------------------------------------------


def test_evidence_filters_select_exact_hand_placed_subsets(
    contract_client: tuple[TestClient, int, int, int],
) -> None:
    client, _, run_a, _ = contract_client
    base = f"/api/verification/runs/{run_a}/evidence"

    def rows(query: str) -> set[_ROW_TUPLE]:
        return _tuples(client.get(f"{base}?{query}").json())

    assert rows("variable=temperature") == {
        ("temperature", 1, "temperature_high", "depth", "3"),
        ("temperature", 1, "temperature_high", "depth", "4"),
        ("temperature", 1, "temperature_high", "feed", "101"),
        ("temperature", 2, "temperature_high", "depth", "3"),
    }
    assert rows("variable=temperature&lead=1") == {
        ("temperature", 1, "temperature_high", "depth", "3"),
        ("temperature", 1, "temperature_high", "depth", "4"),
        ("temperature", 1, "temperature_high", "feed", "101"),
    }
    assert rows("entity_type=feed") == {
        ("temperature", 1, "temperature_high", "feed", "101"),
    }
    assert rows("quantity=wind_max") == {("wind", 1, "wind_max", "depth", "3")}
    # eligible = fe=1 AND te=1. Dropping the forecast_eligible conjunct
    # admits the depth-4 decoy (fe=0, te=1); dropping the truth_eligible
    # conjunct admits the feed-101 decoy (fe=1, te=0).
    assert rows("eligibility=eligible") == {
        ("temperature", 1, "temperature_high", "depth", "3"),
        ("temperature", 2, "temperature_high", "depth", "3"),
        ("wind", 1, "wind_max", "depth", "3"),
        ("precip", 1, "precip_occurrence", "depth", "3"),
    }
    assert rows("eligibility=forecast_ineligible") == {
        ("temperature", 1, "temperature_high", "depth", "4"),
    }
    assert rows("eligibility=truth_ineligible") == {
        ("temperature", 1, "temperature_high", "feed", "101"),
    }


def test_evidence_pages_never_cross_runs(
    contract_client: tuple[TestClient, int, int, int],
) -> None:
    client, _, run_a, run_b = contract_client
    a_rows = _tuples(client.get(f"/api/verification/runs/{run_a}/evidence").json())
    b_rows = _tuples(client.get(f"/api/verification/runs/{run_b}/evidence").json())
    assert ("wind", 1, "wind_max", "depth", "9") not in a_rows
    assert b_rows == {("wind", 1, "wind_max", "depth", "9")}


# ---------------------------------------------------------------------------
# 404 sweep — every run-scoped endpoint refuses an unknown run id.
# ---------------------------------------------------------------------------


def test_unknown_run_404_on_every_run_scoped_endpoint(
    contract_client: tuple[TestClient, int, int, int],
) -> None:
    client, _, _, _ = contract_client
    for suffix in ("", "/verdicts", "/evidence", "/diagnostics", "/methodology"):
        resp = client.get(f"/api/verification/runs/424242{suffix}")
        assert resp.status_code == 404, suffix


# ---------------------------------------------------------------------------
# /latest — the pointer, never max(id). A NEWER run must not hijack it.
# ---------------------------------------------------------------------------


def test_latest_follows_published_pointer_not_the_newest_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Pointer Town")
    published = _seed_published_run(conn, site_id)
    # Decoy: a strictly NEWER run for the SAME site, left unpublished. A
    # mutant resolving /latest (or the status banner) by MAX(id) / ORDER BY
    # id DESC LIMIT 1 instead of the runtime_state pointer lands here.
    newer = _insert_run(conn, site_id)
    assert newer > published
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.get(
            f"/api/verification/latest?site={site_id}", follow_redirects=False
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == f"/api/verification/runs/{published}"
        status = client.get(f"/api/verification/status?site={site_id}").json()
        assert status["sites"][0]["published_run"]["run_id"] == published
        # ...and the page banner agrees with the pointer, not the decoy.
        page = client.get(f"/verification?site={site_id}")
        assert f"Run #{published}" in page.text
        assert f"Run #{newer}" not in page.text
        # Non-vacuity: the decoy IS visible on the runs collection (it is
        # not merely absent from the database), and it sorts FIRST there.
        runs = client.get(f"/api/verification/runs?site={site_id}").json()["runs"]
        assert [r["run_id"] for r in runs] == [newer, published]


# ---------------------------------------------------------------------------
# §18.11 — page, API, and persisted rows report the SAME verdicts, counts,
# units, exclusions, and run identity. Values are read off the DB rows and
# must appear, formatted per the template's declared precision, in both the
# JSON payloads and the rendered HTML.
# ---------------------------------------------------------------------------


def test_page_api_and_rows_agree_on_verdicts_counts_and_nulls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Consistency Town")
    run_id = _seed_published_run(conn, site_id)
    conn.commit()

    # --- The persisted truth, read straight from the rows. --------------
    db_verdicts = {
        str(r["variable"]): (
            str(r["outcome"]),
            r["recommended_depth"],
            int(r["incumbent_depth"]),
        )
        for r in conn.execute(
            "SELECT * FROM verification_verdicts WHERE run_id = ?", (run_id,)
        )
    }
    assert db_verdicts == {
        "temperature": ("recommend", 3, 2),
        "wind": ("retain_incumbent", None, 2),
        "precip": ("skipped", None, 5),
    }
    headline_rows = list(
        conn.execute(
            """
            SELECT * FROM verification_results
            WHERE run_id = ? AND headline = 1 ORDER BY id
            """,
            (run_id,),
        )
    )
    assert len(headline_rows) == 2  # the third seeded result is headline=0

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        api_verdicts = {
            str(v["variable"]): (
                str(v["outcome"]),
                v["recommended_depth"],
                int(v["incumbent_depth"]),
            )
            for v in client.get(f"/api/verification/runs/{run_id}/verdicts").json()[
                "verdicts"
            ]
        }
        api_headline = client.get(
            f"/api/verification/runs/{run_id}/diagnostics?headline=1"
        ).json()["results"]
        raw_page = client.get(f"/verification?site={site_id}").text

    # Jinja's block whitespace is not part of the contract; the token
    # sequence is. Collapse runs of whitespace before matching prose.
    page = re.sub(r"\s+", " ", raw_page)

    # 1. Verdicts: rows == API == page.
    assert api_verdicts == db_verdicts
    assert "<h3>temperature</h3>" in page
    assert "Recommend depth change" in page
    assert f"<dt>Incumbent depth (pinned by run #{run_id})</dt><dd>2</dd>" in page
    assert "<dt>Recommended depth</dt><dd>3</dd>" in page
    assert "<h3>precip</h3>" in page
    assert f"<dt>Incumbent depth (pinned by run #{run_id})</dt><dd>5</dd>" in page
    # A skipped/retained variable has NO recommended depth: exactly one card
    # carries a numeric recommendation, the other two an em dash.
    assert page.count("<dt>Recommended depth</dt><dd>—</dd>") == 2
    # The placeholder ('skipped') verdict is never shown as successful
    # evidence (§16.1).
    assert 'data-v16="16.2.placeholder"' in page

    # 2. Run identity is the same integer in all three places.
    assert f"Run #{run_id}" in page
    assert {int(r["run_id"]) for r in api_headline} == {run_id}

    # 3. Headline counts and metrics: rows == API == page, with the page's
    #    declared %.3f / %.0f%% precision hand-applied here.
    by_cell = {
        (str(r["variable"]), int(r["lead"]), str(r["entity_key"])): r
        for r in api_headline
    }
    assert len(by_cell) == len(headline_rows) == 2
    for row in headline_rows:
        cell = by_cell[(str(row["variable"]), int(row["lead"]), str(row["entity_key"]))]
        assert cell["common_days"] == int(row["common_days"])
        assert cell["mae"] == row["mae"]
        assert cell["availability_rate"] == row["availability_rate"]
        assert (
            f'<td data-label="Common days" data-v16="16.3.common_days">'
            f"{int(row['common_days'])}</td>" in page
        )
        metric = "MAE" if str(row["quantity"]) != "precip_occurrence" else "ETS"
        if row["mae"] is None:
            # 4. Insufficient is an em dash on the page and null on the
            #    wire — NEVER a numeric zero on either.
            assert cell["mae"] is None
            assert "0.000" not in page
            assert (
                '<td data-label="Primary metric" data-v16="16.3.primary_metric">'
                f"{metric} —</td>" in page
            )
        else:
            assert (
                '<td data-label="Primary metric" data-v16="16.3.primary_metric">'
                f"{metric} {float(row['mae']):.3f}</td>" in page
            )
        if row["availability_rate"] is not None:
            pct = f"{float(row['availability_rate']) * 100:.0f}%"
            assert f'<td data-label="Availability">{pct}</td>' in page

    # 5. Exclusions/diagnostic counts agree between page and API.
    assert "1 snapshot days" in page
    assert "1 with knowability exclusions" in page
    assert "0 null-availability samples" in page

    # 6. Units and confidence levels are machine-readable on the wire and
    #    rendered with the template's precision on the page.
    assert "candidate CI 0.9833" in page
    assert "precip-improvement CI 0.9917" in page
    assert "baseline-gate CI 0.95" in page
