"""Phase-7 surface tests: §15 per-variable blend depth + §16 verification UI/API.

Covers the depth-resolution helper (override wins, invalid falls through,
provenance), the options→settings clearing rule, the record/snapshot
lockstep, the F-2 daily-rank truth-revision gate, the read-only
``/api/verification/*`` endpoints (schema field, filters, pagination,
redirect), the ``record_snapshot_local_time`` setter endpoint, the
``/verification`` page render states, the forecast-page ride-alongs, and
the export/import round-trip over the new 0.11.0 tables + published-run
pointer.

Synthetic fixtures only — fake site names/coords, UTC timezone, invented
feed models; no real station IDs or keys.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_conn
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.options import RuntimeOptions, load_runtime_options
from wxverify.db.connection import Database, close_db, init_db
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.settings.depth import (
    DEPTH_VARIABLES,
    EffectiveDepth,
    depth_override_key,
    effective_blend_depth,
    effective_blend_depths,
)
from wxverify.settings.keys import get_setting, set_setting
from wxverify.settings.service import apply_plain_settings
from wxverify.verification.record import SNAPSHOT_TIME_KEY, parse_wall_clock
from wxverify.verification.runs import (
    RosterFeed,
    RunConfig,
    _parse_blend_depths,  # noqa: SLF001
    capture_config_snapshot,
    input_fingerprint,
    publish_run,
    published_run_id,
)
from wxverify.verification.simulate import _daily_rank_order  # noqa: SLF001

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_web_ui.py / tests/test_db_transfer.py).
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


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


def _make_site(conn: sqlite3.Connection, name: str = "Verify Town") -> int:
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)
            """,
            (name,),
        ).lastrowid
    )
    ensure_published_generation(conn, site_id)
    return site_id


def _seed_published_run(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    fresh_fingerprint: bool = True,
    methodology_version: int = 1,
) -> int:
    """A published run with verdicts (incl. 'skipped'), headline results
    (incl. NULL metrics), evidence, day context, and a trigger decision.

    ``methodology_version`` defaults to 1 -- the version every existing
    caller relies on unchanged -- so it mirrors the live published run
    (still on the pre-pairwise methodology). Pass 2 for O-V3's matching-
    version arm.
    """
    generation_id = ensure_published_generation(conn, site_id)
    snapshot = capture_config_snapshot(conn, site_id)
    fingerprint = (
        input_fingerprint(conn, site_id, snapshot) if fresh_fingerprint else "0" * 64
    )
    run_id = int(
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint)
            VALUES (?, ?, ?, '0.11.0-test', 'running', 1, ?, '2026-05-01',
                    '2026-05-30', '2026-05-30', 12345, 100, ?)
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
    verdict_rows = [
        ("temperature", "recommend", 3, 2, json.dumps(["1", "3", "4"])),
        ("wind", "retain_incumbent", None, 2, json.dumps(["1", "3"])),
        ("precip", "skipped", None, 5, json.dumps([])),
    ]
    conn.executemany(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, recommended_depth, incumbent_depth,
             tested_family)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [(run_id, *row) for row in verdict_rows],
    )
    conn.executemany(
        """
        INSERT INTO verification_results
            (run_id, variable, lead, quantity, entity_type, entity_key,
             headline, common_days, mae, bias, rmse, ets, availability_rate,
             delta_vs_incumbent, detail)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            # NULL metrics: insufficient must surface as null, never 0.
            (
                run_id,
                "temperature",
                1,
                "temperature_high",
                "depth",
                "3",
                1,
                25,
                0.8,
                0.1,
                1.0,
                None,
                0.95,
                0.12,
                json.dumps({"note": "synthetic"}),
            ),
            (
                run_id,
                "wind",
                1,
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
            ),
            (
                run_id,
                "wind",
                2,
                "wind_max",
                "depth",
                "1",
                0,
                20,
                1.4,
                -0.2,
                1.9,
                None,
                0.8,
                -0.05,
                None,
            ),
        ],
    )
    conn.executemany(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, predicted, forecast_eligible,
             truth_value, truth_eligible, abs_error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 1, ?)
        """,
        [
            (
                run_id,
                "2026-05-01",
                "2026-05-02",
                1,
                "temperature",
                "temperature_high",
                "depth",
                "3",
                15.0,
                14.5,
                0.5,
            ),
            (
                run_id,
                "2026-05-01",
                "2026-05-02",
                1,
                "wind",
                "wind_max",
                "depth",
                "3",
                5.0,
                6.0,
                1.0,
            ),
            (
                run_id,
                "2026-05-01",
                "2026-05-03",
                2,
                "temperature",
                "temperature_high",
                "depth",
                "3",
                16.0,
                14.0,
                2.0,
            ),
        ],
    )
    conn.execute(
        """
        INSERT INTO verification_day_context
            (run_id, snapshot_local_date, snapshot_utc,
             knowability_exclusions, null_availability_samples)
        VALUES (?, '2026-05-01', '2026-05-01T07:00:00Z', ?, 0)
        """,
        (run_id, json.dumps({"temperature_high": "truth_pending"})),
    )
    conn.execute(
        """
        INSERT INTO verification_trigger_decisions
            (site_id, trigger_date, decision, reason, input_fingerprint, run_id)
        VALUES (?, '2026-05-31', 'run_started', NULL, ?, ?)
        """,
        (site_id, fingerprint, run_id),
    )
    publish_run(conn, site_id, run_id)
    conn.commit()
    return run_id


# ---------------------------------------------------------------------------
# §15 — depth resolution, provenance, clearing.
# ---------------------------------------------------------------------------


def test_effective_depth_global_default_and_provenance() -> None:
    conn = asof_conn()
    depths = effective_blend_depths(conn)
    assert set(depths) == set(DEPTH_VARIABLES)
    for d in depths.values():
        assert d == EffectiveDepth(depth=2, source="global")


def test_effective_depth_override_wins_and_others_stay_global() -> None:
    conn = asof_conn()
    set_setting(conn, "forecast_blend_depth", "3")
    set_setting(conn, depth_override_key("wind"), "5")
    depths = effective_blend_depths(conn)
    assert depths["wind"] == EffectiveDepth(depth=5, source="override")
    assert depths["temperature"] == EffectiveDepth(depth=3, source="global")
    assert depths["precip"] == EffectiveDepth(depth=3, source="global")


@pytest.mark.parametrize("bad", ["0", "7", "abc", "", "2.5", "-1"])
def test_effective_depth_invalid_override_falls_through_to_global(
    bad: str,
) -> None:
    conn = asof_conn()
    set_setting(conn, depth_override_key("precip"), bad)
    resolved = effective_blend_depth(conn, "precip")
    assert resolved == EffectiveDepth(depth=2, source="global")


def test_apply_plain_settings_clears_absent_depth_overrides(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    try:
        # Present key -> row written.
        asyncio.run(apply_plain_settings(RuntimeOptions(forecast_blend_depth_wind=4)))
        assert get_setting(conn, depth_override_key("wind")) == "4"
        # Absent key on the next apply -> row DELETED (clearing rule),
        # while the untouched global key semantics stay apply-when-present.
        asyncio.run(apply_plain_settings(RuntimeOptions()))
        assert get_setting(conn, depth_override_key("wind")) is None
        assert effective_blend_depth(conn, "wind").source == "global"
    finally:
        close_db()


def test_options_env_and_json_plumbing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Env path: options file absent.
    monkeypatch.setattr(config, "options_path", str(tmp_path / "missing.json"))
    monkeypatch.setenv("WXV_FORECAST_BLEND_DEPTH_TEMPERATURE", "6")
    opts = load_runtime_options()
    assert opts.forecast_blend_depth_temperature == 6
    assert opts.forecast_blend_depth_wind is None
    # File path: env ignored, keys read from options.json.
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps({"forecast_blend_depth_precip": 1}), encoding="utf-8"
    )
    monkeypatch.setattr(config, "options_path", str(options_path))
    opts = load_runtime_options()
    assert opts.forecast_blend_depth_precip == 1
    assert opts.forecast_blend_depth_temperature is None


# ---------------------------------------------------------------------------
# §15 — lockstep: snapshot pins, backward compat, incumbent resolution.
# ---------------------------------------------------------------------------


def test_snapshot_pins_depths_and_provenance() -> None:
    conn = asof_conn()
    site_id = _make_site(conn)
    set_setting(conn, depth_override_key("precip"), "5")
    snapshot = capture_config_snapshot(conn, site_id)
    assert snapshot["blend_depths"] == {
        "temperature": 2,
        "wind": 2,
        "precip": 5,
    }
    assert snapshot["blend_depth_sources"] == {
        "temperature": "global",
        "wind": "global",
        "precip": "override",
    }
    # Lockstep: the snapshot agrees with the live helper by construction.
    live = effective_blend_depths(conn)
    assert snapshot["blend_depths"] == {v: d.depth for v, d in live.items()}


def test_parse_blend_depths_backfills_pre_015_snapshots() -> None:
    # Pre-§15 snapshot (no blend_depths) -> every variable inherits the
    # pinned global depth, preserving the old incumbent semantics.
    assert _parse_blend_depths(None, 3) == {
        "temperature": 3,
        "wind": 3,
        "precip": 3,
    }
    assert _parse_blend_depths({"wind": 4}, 2) == {
        "temperature": 2,
        "wind": 4,
        "precip": 2,
    }


def test_run_config_incumbent_depth_per_variable() -> None:
    cfg = RunConfig(
        site_id=1,
        run_id=1,
        timezone="UTC",
        rain_threshold_mm=0.2,
        wall_clock="07:00",
        blend_depth=2,
        blend_depths={"temperature": 2, "wind": 4, "precip": 5},
        min_n=30,
        window_days=30,
        tz_generation_id=1,
        roster=(),
        period_start="2026-05-01",
        period_end="2026-05-30",
        bootstrap_seed=1,
        bootstrap_resamples=100,
    )
    assert cfg.incumbent_depth("temperature") == 2
    assert cfg.incumbent_depth("wind") == 4
    assert cfg.incumbent_depth("precip") == 5


# ---------------------------------------------------------------------------
# F-2 — daily-rank order must ignore truth revised after as_of.
# ---------------------------------------------------------------------------


def test_daily_rank_order_excludes_post_asof_revised_truth() -> None:
    conn = asof_conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    run_id = int(
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, bootstrap_seed,
                 bootstrap_resamples, input_fingerprint)
            VALUES (?, ?, 1, 'test', 'running', 1, '{}', 1, 100, 'fp')
            """,
            (site_id, generation_id),
        ).lastrowid
    )
    cfg = RunConfig(
        site_id=site_id,
        run_id=run_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        wall_clock="07:00",
        blend_depth=2,
        blend_depths={v: 2 for v in DEPTH_VARIABLES},
        min_n=30,
        window_days=30,
        tz_generation_id=generation_id,
        roster=(
            RosterFeed(
                feed_id=101, source="syn-src", model="model-a", max_lead_hours=168
            ),
            RosterFeed(
                feed_id=102, source="syn-src", model="model-b", max_lead_hours=168
            ),
        ),
        period_start="2026-05-01",
        period_end="2026-05-30",
        bootstrap_seed=1,
        bootstrap_resamples=100,
    )
    as_of = "2026-05-20T07:00:00Z"
    clean_days = [f"2026-05-{d:02d}" for d in range(1, 13)]
    revised_days = [f"2026-04-{d:02d}" for d in range(1, 13)]

    def _truth(day: str, computed_at: str | None) -> None:
        conn.execute(
            """
            INSERT INTO daily_truth
                (site_id, local_date, quantity, value, eligible,
                 covered_hours, expected_slots, day_start_utc, day_end_utc,
                 timezone, tz_generation_id, source_max_computed_at)
            VALUES (?, ?, 'wind_max', 5.0, 1, 24, 24, ?, ?, 'UTC', ?, ?)
            """,
            (
                site_id,
                day,
                f"{day}T00:00:00Z",
                f"{day}T23:59:59Z",
                generation_id,
                computed_at,
            ),
        )

    def _evidence(day: str, key: str, abs_error: float) -> None:
        conn.execute(
            """
            INSERT INTO verification_evidence
                (run_id, snapshot_local_date, target_local_date, lead,
                 variable, quantity, entity_type, entity_key, predicted,
                 forecast_eligible, truth_value, truth_eligible, abs_error)
            VALUES (?, ?, ?, 1, 'wind', 'wind_max', 'feed', ?, 5.0, 1, 5.0,
                    1, ?)
            """,
            (run_id, day, day, key, abs_error),
        )

    # 12 clean days (truth settled before as_of): feed 101 clearly better.
    for day in clean_days:
        _truth(day, "2026-05-19T00:00:00Z")
        _evidence(day, "101", 0.5)
        _evidence(day, "102", 1.0)
    # 12 days whose truth was REVISED after as_of: feed 102 hugely better.
    # A ranker without the F-2 gate would rank 102 first on these.
    for day in revised_days:
        _truth(day, "2026-05-25T00:00:00Z")
        _evidence(day, "101", 9.0)
        _evidence(day, "102", 0.1)
    conn.commit()

    order = _daily_rank_order(
        conn, cfg, quantity="wind_max", knowable_through="2026-05-30", as_of=as_of
    )
    assert order == [101, 102], (
        "post-as_of truth revisions must not flip the daily rank order"
    )


# ---------------------------------------------------------------------------
# §16 — /api/verification/* endpoints.
# ---------------------------------------------------------------------------


def test_api_status_runs_and_run_detail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    run_id = _seed_published_run(conn, site_id)

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        status = client.get(f"/api/verification/status?site={site_id}").json()
        assert status["verification_schema"] == 2
        assert "units" in status["contract"]
        (entry,) = status["sites"]
        assert entry["published_run"]["run_id"] == run_id
        warnings = entry["warnings"]
        assert warnings == {
            "no_publishable_run": False,
            "stale_inputs": False,
            "failed_newer_attempt": False,
        }

        runs = client.get(f"/api/verification/runs?site={site_id}").json()
        assert [r["run_id"] for r in runs["runs"]] == [run_id]

        detail = client.get(f"/api/verification/runs/{run_id}").json()
        assert detail["verification_schema"] == 2
        snapshot = detail["run"]["config_snapshot"]
        assert snapshot["blend_depths"] == {
            "temperature": 2,
            "wind": 2,
            "precip": 2,
        }

        assert client.get("/api/verification/runs/999999").status_code == 404
        assert client.get("/api/verification/status?site=999999").status_code == 404


def test_api_status_stale_and_failed_warnings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    run_id = _seed_published_run(conn, site_id, fresh_fingerprint=False)
    conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, bootstrap_seed,
             bootstrap_resamples, input_fingerprint, error)
        VALUES (?, ?, 1, 'test', 'failed', 2, '{}', 1, 100, 'fp2', 'boom')
        """,
        (site_id, ensure_published_generation(conn, site_id)),
    )
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        status = client.get(f"/api/verification/status?site={site_id}").json()
        (entry,) = status["sites"]
        assert entry["published_run"]["run_id"] == run_id
        assert entry["warnings"]["stale_inputs"] is True
        assert entry["warnings"]["failed_newer_attempt"] is True


def test_api_verdicts_evidence_diagnostics_methodology_latest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    run_id = _seed_published_run(conn, site_id)
    other = _make_site(conn, "No Run Town")
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        verdicts = client.get(f"/api/verification/runs/{run_id}/verdicts").json()[
            "verdicts"
        ]
        by_var = {v["variable"]: v for v in verdicts}
        assert by_var["precip"]["outcome"] == "skipped"
        assert by_var["precip"]["incumbent_depth"] == 5
        assert by_var["temperature"]["recommended_depth"] == 3
        assert by_var["temperature"]["tested_family"] == ["1", "3", "4"]

        evidence = client.get(
            f"/api/verification/runs/{run_id}/evidence?variable=temperature&lead=1"
        ).json()
        assert evidence["verification_schema"] == 2
        assert len(evidence["evidence"]) == 1
        assert evidence["evidence"][0]["quantity"] == "temperature_high"

        # Limit clamps to the 500 cap; deterministic id order.
        clamped = client.get(
            f"/api/verification/runs/{run_id}/evidence?limit=9999"
        ).json()
        assert clamped["limit"] == 500
        ids = [row["id"] for row in clamped["evidence"]]
        assert ids == sorted(ids)

        bad = client.get(f"/api/verification/runs/{run_id}/evidence?eligibility=bogus")
        assert bad.status_code == 400

        diag = client.get(
            f"/api/verification/runs/{run_id}/diagnostics?headline=1"
        ).json()
        assert len(diag["results"]) == 2
        # NULL metrics stay null in JSON, never 0.
        wind_row = next(r for r in diag["results"] if r["quantity"] == "wind_max")
        assert wind_row["mae"] is None
        assert wind_row["ets"] is None
        assert diag["day_context"][0]["null_availability_samples"] == 0

        # O-V3, v1 arm: this fixture's run was scored under methodology
        # version 1 (the shape of the live published run today), which does
        # not match this build's version 2 -- so the endpoint refuses to
        # answer with the current constants/contract rather than describing
        # a run under a version it does not carry. See
        # test_api_methodology_matches_the_build_version_it_was_scored_under
        # for the matching-version (v2) arm.
        meth = client.get(f"/api/verification/runs/{run_id}/methodology").json()
        assert meth["contract"] is None
        assert meth["constants"] is None
        assert meth["contract_unavailable_reason"] is not None
        assert "methodology version 1" in meth["contract_unavailable_reason"]
        assert "methodology version 2" in meth["contract_unavailable_reason"]
        assert meth["provenance"]["run_id"] == run_id

        latest = client.get(
            f"/api/verification/latest?site={site_id}",
            follow_redirects=False,
        )
        assert latest.status_code == 307
        assert latest.headers["location"] == f"/api/verification/runs/{run_id}"

        assert (
            client.get(
                f"/api/verification/latest?site={other}",
                follow_redirects=False,
            ).status_code
            == 404
        )


def test_api_methodology_matches_the_build_version_it_was_scored_under(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O-V3, v2 arm: a run scored under this build's own methodology
    version gets the real contract/constants, paired with the v1 refusal
    arm above (`_seed_published_run`'s default) so a mutant that always
    refuses -- or always answers -- is caught by whichever arm it breaks.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    run_id = _seed_published_run(conn, site_id, methodology_version=2)
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        meth = client.get(f"/api/verification/runs/{run_id}/methodology").json()
        assert meth["contract_unavailable_reason"] is None
        assert meth["contract"] is not None
        assert meth["constants"] is not None
        assert meth["constants"]["methodology_version"] == 2
        assert meth["constants"]["bootstrap_resamples"] == 10_000
        assert meth["constants"]["simulated_depths"] == [1, 2, 3, 4]
        assert meth["contract"]["units"]["wind_max"] == "m/s"
        assert meth["provenance"]["run_id"] == run_id


# ---------------------------------------------------------------------------
# Obligation 2 — record_snapshot_local_time setter endpoint.
# ---------------------------------------------------------------------------


def _read_setting(key: str) -> str | None:
    """Fresh direct read: the seed conn is closed once the app starts."""
    direct = sqlite3.connect(config.db_path)
    try:
        row = direct.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row[0])
    finally:
        direct.close()


def test_snapshot_time_setter_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        # Global set, canonicalised.
        resp = client.put(
            "/api/settings/record-snapshot-time",
            json={"time": "6:05"},
            headers=headers,
        )
        assert resp.status_code == 200
        assert resp.json() == {"key": SNAPSHOT_TIME_KEY, "time": "06:05"}
        assert _read_setting(SNAPSHOT_TIME_KEY) == "06:05"

        # Per-site override.
        resp = client.put(
            "/api/settings/record-snapshot-time",
            json={"time": "08:30", "site_id": site_id},
            headers=headers,
        )
        assert resp.status_code == 200
        assert _read_setting(f"{SNAPSHOT_TIME_KEY}:{site_id}") == "08:30"

        # Validation (same parser the record reader uses).
        for bad in ("24:00", "07:60", "nope", "1200", ""):
            assert parse_wall_clock(bad) is None
            resp = client.put(
                "/api/settings/record-snapshot-time",
                json={"time": bad},
                headers=headers,
            )
            assert resp.status_code == 400

        # Unknown site.
        resp = client.put(
            "/api/settings/record-snapshot-time",
            json={"time": "07:00", "site_id": 999999},
            headers=headers,
        )
        assert resp.status_code == 404

        # Clearing the per-site override falls back to the global value.
        resp = client.delete(
            f"/api/settings/record-snapshot-time?site={site_id}",
            headers=headers,
        )
        assert resp.status_code == 200
        assert _read_setting(f"{SNAPSHOT_TIME_KEY}:{site_id}") is None
        assert _read_setting(SNAPSHOT_TIME_KEY) == "06:05"


# ---------------------------------------------------------------------------
# §16 — /verification page + forecast ride-alongs.
# ---------------------------------------------------------------------------


def test_verification_page_empty_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        page = client.get(f"/verification?site={site_id}")
        assert page.status_code == 200
        assert "No published verification run" in page.text
        # Nav tab present on every page.
        assert ">Verification</a>" in page.text


def test_verification_page_published_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    _seed_published_run(conn, site_id)
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        page = client.get(f"/verification?site={site_id}")
        assert page.status_code == 200
        assert "Recommend depth change" in page.text
        assert "Retain incumbent" in page.text
        assert "Skipped (incumbent outside simulated range)" in page.text
        assert "Headline results" in page.text
        assert "Methodology v1" in page.text
        # Live effective depth is labelled as LIVE, distinctly from the
        # run's pinned incumbent depth (§16.1/§16.2).
        assert "Live effective blend depth" in page.text
        assert "(global)" in page.text
        assert 'data-v16="16.2.live_depth"' in page.text
        assert 'data-v16="16.1.config_snapshot"' in page.text


def test_forecast_page_ride_alongs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn)
    conn.commit()
    # The override must arrive via options.json: startup's
    # apply_plain_settings CLEARS any depth-override row whose key is
    # absent from the applied options (the §15 clearing rule), so a
    # DB-seeded override would be wiped by the app lifespan.
    (tmp_path / "options.json").write_text(
        json.dumps({"forecast_blend_depth_wind": 4}), encoding="utf-8"
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        page = client.get("/forecast")
        assert page.status_code == 200
        assert "Blend depth" in page.text
        assert "wind 4 (override)" in page.text
        assert "temperature 2 (global)" in page.text
        # Relabel: "predicted wet-hour share" replaces "chance of rain".
        assert "Chance of rain" not in page.text
        if "tile-row" in page.text and "Rain" in page.text:
            assert "Predicted wet-hour share" in page.text or "title=" not in page.text


# ---------------------------------------------------------------------------
# §16 — export/import round-trip over the new 0.11.0 tables + pointer.
# ---------------------------------------------------------------------------


def test_import_round_trip_preserves_verification_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)

    # Build the source DB standalone (never clobbers the live instance).
    b_path = tmp_path / "source-b.db"
    b_db = Database(str(b_path))
    try:
        conn_b = b_db._conn  # noqa: SLF001
        site_b = _make_site(conn_b, "Round Trip Town")
        run_b = _seed_published_run(conn_b, site_b)
        set_setting(conn_b, depth_override_key("precip"), "5")
        conn_b.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn_b.commit()
    finally:
        b_db.close()
    payload = b_path.read_bytes()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = {**_csrf_headers(client), "Content-Type": "application/octet-stream"}
        resp = client.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 200
        assert resp.json()["status"] == "imported"

        # The verification surface serves the imported run end-to-end.
        status = client.get("/api/verification/status").json()
        (entry,) = status["sites"]
        assert entry["published_run"]["run_id"] == run_b

    direct = sqlite3.connect(config.db_path)
    direct.row_factory = sqlite3.Row
    try:
        counts = {
            table: int(
                direct.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]  # noqa: S608
            )
            for table in (
                "verification_runs",
                "verification_verdicts",
                "verification_results",
                "verification_evidence",
                "verification_day_context",
                "verification_trigger_decisions",
            )
        }
        assert counts == {
            "verification_runs": 1,
            "verification_verdicts": 3,
            "verification_results": 3,
            "verification_evidence": 3,
            "verification_day_context": 1,
            "verification_trigger_decisions": 1,
        }
        site_row = direct.execute("SELECT id FROM sites").fetchone()
        pointer = direct.execute(
            "SELECT value FROM runtime_state WHERE key = ?",
            (f"verification_published_run:{int(site_row['id'])}",),
        ).fetchone()
        assert pointer is not None and int(pointer["value"]) == run_b
        depth_row = direct.execute(
            "SELECT value FROM settings WHERE key = ?",
            (depth_override_key("precip"),),
        ).fetchone()
        assert depth_row is not None and depth_row["value"] == "5"
    finally:
        direct.close()


def test_published_pointer_reads_through_app_layer(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    try:
        site_id = _make_site(conn)
        run_id = _seed_published_run(conn, site_id)
        assert published_run_id(conn, site_id) == run_id
    finally:
        close_db()
