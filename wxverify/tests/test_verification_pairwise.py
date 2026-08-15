"""§9 (W6): below-floor rows compared against the RECOMMENDED blend.

The comparison cannot exist before the verdicts do, so it runs in its own
`pairwise` phase between `bootstrap` and `publish`, writes with an UPDATE
(``_write_result``'s INSERT carries ``ON CONFLICT … DO NOTHING``, so a
re-INSERT is a silent no-op), and is read back by BOTH surfaces — the page
and the API diagnostics payload.

Geometry is chosen so the three day-counts differ: the strict common core is
4 days, the below-floor feed's pairwise core against the incumbent is 4, and
its pairwise core against the recommended depth is 5. An assertion on the
inner ``dates_n`` is therefore non-vacuous.

All fixture data is synthetic: a UTC site with invented coordinates, one
fake feed, invented dates.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_conn, asof_make_real_feed
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.engine import aggregate_run, write_pairwise_comparisons
from wxverify.verification.runs import (
    RosterFeed,
    RunConfig,
    capture_config_snapshot,
    publish_run,
)
from wxverify.verification.simulate import SIM_VARIABLES
from wxverify.worker.verification_run import (
    _load_state,  # noqa: SLF001
    _save_state,  # noqa: SLF001
    advance_verification,
)

#: Ten truth-eligible target dates; the feed is forecast-eligible on six of
#: them (0.6) so it lands BELOW the 0.70 availability floor.
_DATES = [f"2026-06-{d:02d}" for d in range(1, 11)]
_INCUMBENT_DEPTH = 2
_RECOMMENDED_DEPTH = 3
#: Per entity, the dates it is forecast-eligible on, and its constant error.
_ELIGIBILITY: dict[tuple[str, str], tuple[list[str], float]] = {
    ("depth", "1"): (_DATES[:4], 1.0),
    ("depth", "2"): (_DATES[:4], 1.0),
    ("depth", "3"): (_DATES[:5], 4.0),
    ("depth", "4"): (_DATES[:4], 1.0),
}
_FEED_ERROR = 2.0
_TRUTH = 6.0


def _make_run(conn: sqlite3.Connection) -> tuple[int, int, RunConfig]:
    site_id = int(
        cast(
            int,
            conn.execute(
                """
                INSERT INTO sites
                    (name, forecast_lat, forecast_lon, elevation_m, timezone)
                VALUES ('site-alpha', 40.0, -105.0, 900.0, 'UTC')
                """
            ).lastrowid,
        )
    )
    feed_id = asof_make_real_feed(conn, "model-alpha")
    generation_id = ensure_published_generation(conn, site_id)
    snapshot = capture_config_snapshot(conn, site_id)
    snapshot["blend_depth"] = _INCUMBENT_DEPTH
    snapshot["blend_depths"] = {v: _INCUMBENT_DEPTH for v in SIM_VARIABLES}
    run_id = int(
        cast(
            int,
            conn.execute(
                """
                INSERT INTO verification_runs
                    (site_id, tz_generation_id, methodology_version, app_version,
                     state, attempt, config_snapshot, period_start, period_end,
                     settled_through, bootstrap_seed, bootstrap_resamples,
                     input_fingerprint)
                VALUES (?, ?, 1, '0.11.1-test', 'running', 1, ?,
                        ?, ?, ?, 77, 200, 'fp-pairwise-test')
                """,
                (
                    site_id,
                    generation_id,
                    json.dumps(snapshot, separators=(",", ":")),
                    _DATES[0],
                    _DATES[-1],
                    _DATES[-1],
                ),
            ).lastrowid,
        )
    )
    cfg = RunConfig(
        site_id=site_id,
        run_id=run_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        wall_clock="07:00",
        blend_depth=_INCUMBENT_DEPTH,
        blend_depths={v: _INCUMBENT_DEPTH for v in SIM_VARIABLES},
        min_n=1,
        window_days=30,
        tz_generation_id=generation_id,
        roster=(
            RosterFeed(
                feed_id=feed_id,
                source="alpha",
                model="model-alpha",
                max_lead_hours=168,
            ),
        ),
        period_start=_DATES[0],
        period_end=_DATES[-1],
        bootstrap_seed=77,
        bootstrap_resamples=200,
    )
    _seed_wind_cell(conn, run_id, feed_id)
    return site_id, run_id, cfg


def _insert_evidence(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    entity: tuple[str, str],
    target: str,
    predicted: float | None,
) -> None:
    conn.execute(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, predicted, forecast_eligible,
             realized_contributors, truth_value, truth_eligible, abs_error)
        VALUES (?, ?, ?, 1, 'wind', 'wind_max', ?, ?, ?, ?, 1, ?, 1, ?)
        """,
        (
            run_id,
            target,
            target,
            entity[0],
            entity[1],
            predicted,
            1 if predicted is not None else 0,
            _TRUTH,
            None if predicted is None else abs(predicted - _TRUTH),
        ),
    )


def _seed_wind_cell(conn: sqlite3.Connection, run_id: int, feed_id: int) -> None:
    for entity, (dates, error) in _ELIGIBILITY.items():
        for target in _DATES:
            _insert_evidence(
                conn,
                run_id,
                entity=entity,
                target=target,
                predicted=_TRUTH + error if target in dates else None,
            )
    for target in _DATES:
        _insert_evidence(
            conn,
            run_id,
            entity=("feed", str(feed_id)),
            target=target,
            predicted=_TRUTH + _FEED_ERROR if target in _DATES[:6] else None,
        )


def _seed_verdicts(conn: sqlite3.Connection, run_id: int, *, wind_outcome: str) -> None:
    for variable in SIM_VARIABLES:
        recommend = variable == "wind" and wind_outcome == "recommend"
        conn.execute(
            """
            INSERT INTO verification_verdicts
                (run_id, variable, outcome, recommended_depth, incumbent_depth,
                 tested_family)
            VALUES (?,?,?,?,?,'{}')
            """,
            (
                run_id,
                variable,
                wind_outcome if variable == "wind" else "retain_incumbent",
                _RECOMMENDED_DEPTH if recommend else None,
                _INCUMBENT_DEPTH,
            ),
        )


def _feed_row(conn: sqlite3.Connection, run_id: int) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT * FROM verification_results
        WHERE run_id = ? AND entity_type = 'feed'
        """,
        (run_id,),
    ).fetchone()
    assert row is not None
    return row


def _detail(row: sqlite3.Row) -> dict[str, object]:
    parsed: object = json.loads(str(row["detail"]))
    assert isinstance(parsed, dict)
    return cast(dict[str, object], parsed)


def _vs_recommended(row: sqlite3.Row) -> dict[str, object]:
    value = _detail(row)["vs_recommended"]
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _prepared(
    conn: sqlite3.Connection, *, wind_outcome: str
) -> tuple[int, int, RunConfig]:
    site_id, run_id, cfg = _make_run(conn)
    aggregate_run(conn, cfg)
    _seed_verdicts(conn, run_id, wind_outcome=wind_outcome)
    return site_id, run_id, cfg


# ---------------------------------------------------------------------------
# The comparison itself
# ---------------------------------------------------------------------------


def test_below_floor_row_gains_the_recommended_comparison() -> None:
    conn = asof_conn()
    _site_id, run_id, cfg = _prepared(conn, wind_outcome="recommend")

    before = _feed_row(conn, run_id)
    assert int(before["headline"]) == 0
    # Pre-state: the incumbent comparison lives in the COLUMNS, and `detail`
    # carries only the outer dates_n.
    assert _detail(before) == {"dates_n": 4}
    incumbent_delta = float(before["delta_vs_incumbent"])
    incumbent_mae = float(before["mae"])

    write_pairwise_comparisons(conn, cfg)
    row = _feed_row(conn, run_id)

    # The incumbent comparison is untouched, in its own storage location.
    assert float(row["delta_vs_incumbent"]) == pytest.approx(incumbent_delta)
    assert float(row["mae"]) == pytest.approx(incumbent_mae)
    assert int(row["common_days"]) == 4
    assert _detail(row)["dates_n"] == 4

    comparison = _vs_recommended(row)
    assert comparison["available"] is True
    assert comparison["reason"] is None
    assert comparison["recommended_entity"] == {
        "entity_type": "depth",
        "entity_key": str(_RECOMMENDED_DEPTH),
    }
    # The PAIRWISE core of this pair (5), never the strict common core (4)
    # and never the pair-vs-incumbent core the outer dates_n records (4).
    assert comparison["dates_n"] == 5
    assert comparison["mae"] == pytest.approx(_FEED_ERROR)
    # Continuous quantity: mae populated, ets null.
    assert comparison["ets"] is None
    # Same sign convention as delta_vs_incumbent: (4.0 - 2.0) / 4.0.
    assert comparison["delta_vs_recommended"] == pytest.approx(0.5)


def test_retain_incumbent_run_records_the_reason_not_a_null() -> None:
    conn = asof_conn()
    _site_id, run_id, cfg = _prepared(conn, wind_outcome="retain_incumbent")
    write_pairwise_comparisons(conn, cfg)

    comparison = _vs_recommended(_feed_row(conn, run_id))
    assert comparison["available"] is False
    assert comparison["reason"] == "no_recommendation"
    assert comparison["recommended_entity"] is None
    for field in ("dates_n", "mae", "ets", "delta_vs_recommended"):
        assert comparison[field] is None, field


def test_pairwise_pass_is_byte_identical_when_re_entered() -> None:
    conn = asof_conn()
    _site_id, run_id, cfg = _prepared(conn, wind_outcome="recommend")
    write_pairwise_comparisons(conn, cfg)
    first = str(_feed_row(conn, run_id)["detail"])
    write_pairwise_comparisons(conn, cfg)
    assert str(_feed_row(conn, run_id)["detail"]) == first


def test_the_comparison_does_not_disturb_the_verdicts() -> None:
    """Diagnostic-only: below-floor rows never feed the recommendation."""
    conn = asof_conn()
    _site_id, run_id, cfg = _prepared(conn, wind_outcome="recommend")
    query = """
        SELECT variable, outcome, recommended_depth, incumbent_depth,
               tested_family
        FROM verification_verdicts WHERE run_id = ? ORDER BY variable
    """
    before = [tuple(r) for r in conn.execute(query, (run_id,)).fetchall()]
    write_pairwise_comparisons(conn, cfg)
    assert [tuple(r) for r in conn.execute(query, (run_id,)).fetchall()] == before


# ---------------------------------------------------------------------------
# §3.2 — the phase mutates and re-saves the blob; `run_id` survives
# ---------------------------------------------------------------------------


def test_pairwise_phase_survives_a_separate_chunk_transaction() -> None:
    """pairwise → publish across two committed transactions.

    A fresh-dict write in `pairwise` drops `run_id`, `_blob_config` then
    raises `JobCancelled`, the processor COMPLETES the cancellation, and the
    chain ends with no publish and no error — so the assertion that matters
    is that the run reaches `published`.
    """
    conn = asof_conn()
    site_id, run_id, _cfg = _prepared(conn, wind_outcome="recommend")
    _save_state(conn, site_id, {"phase": "pairwise", "run_id": run_id})
    conn.commit()

    assert advance_verification(conn, site_id, {}) is True
    conn.commit()
    blob = _load_state(conn, site_id)
    assert blob is not None
    assert blob["run_id"] == run_id
    assert blob["phase"] == "publish"
    assert _vs_recommended(_feed_row(conn, run_id))["available"] is True

    assert advance_verification(conn, site_id, {}) is False
    conn.commit()
    state = conn.execute(
        "SELECT state FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert str(state["state"]) == "published"


# ---------------------------------------------------------------------------
# API-vs-page parity
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:  # pragma: no cover - never awaited
    import asyncio

    await asyncio.Event().wait()


def _published_app_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> int:
    from wxverify import config
    from wxverify.db.connection import close_db, init_db

    close_db()
    config.db_path = str(tmp_path / "wxverify.db")
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(config.db_path)
    conn = db._conn  # noqa: SLF001
    site_id, run_id, cfg = _prepared(conn, wind_outcome="recommend")
    write_pairwise_comparisons(conn, cfg)
    publish_run(conn, site_id, run_id)
    conn.commit()
    return site_id


def test_api_and_page_carry_the_same_comparison(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from wxverify.api.app import create_app

    site_id = _published_app_run(tmp_path, monkeypatch)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app: Any = create_app(root_path="")
    with TestClient(app) as client:
        page = client.get(f"/verification?site={site_id}")
        runs = client.get("/api/verification/runs")
        assert runs.status_code == 200
        run_id = int(runs.json()["runs"][0]["run_id"])
        payload = client.get(f"/api/verification/runs/{run_id}/diagnostics")
    assert page.status_code == 200
    assert payload.status_code == 200

    api_rows = [
        r
        for r in cast(list[dict[str, Any]], payload.json()["results"])
        if r["entity_type"] == "feed"
    ]
    assert len(api_rows) == 1
    comparison = cast(dict[str, Any], api_rows[0]["detail"]["vs_recommended"])
    assert comparison["delta_vs_recommended"] is not None

    # The page renders the same value, in the below-floor feeds table.
    assert 'data-vs-recommended="yes"' in page.text
    rendered = page.text.split('data-vs-recommended="yes"', 1)[1]
    assert f"{comparison['dates_n']} shared days" in rendered
    assert f"{comparison['delta_vs_recommended']:.2f}" in rendered
