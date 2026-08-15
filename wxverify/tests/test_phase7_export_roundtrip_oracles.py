"""§18.10 oracle: export/restore round-trip over the 0.11.0 state.

The strong form of the round-trip contract: a DB carrying every new
artifact — a published verification run (with a 'skipped' verdict,
NULL-metric results, evidence, day context, a trigger decision, and the
published-run pointer), per-variable depth-override settings, global +
per-site record-snapshot-time settings, and a full day of
forecast_of_record rows — is served by app A, then its bytes are
imported into a fresh app B, and B must serve BYTE-IDENTICAL JSON on
every read-only verification endpoint. Both apps run the same
options.json (carrying the depth override) because startup's
apply_plain_settings clears any depth-override row absent from options —
the §15 clearing rule — so a mismatch there would make the two apps
diverge for reasons outside the transfer path.

A second oracle drives the SHIPPED transfer path instead of hand-built
bytes — POST /api/export/begin -> /export/status -> /export/download
(VACUUM INTO + gzip) -> POST /api/import/db — and adds the two §18.10
artifacts a payload comparison cannot see: truth generations
(``timezone_generations`` + ``daily_truth``) and survival of the
post-import derived rebuild, which DELETEs and re-materializes
``forecast_pairs``/``observations``/``score_cache`` and must leave every
verification artifact and the published-run pointer untouched.

Synthetic fixtures only: invented town names, UTC, fake fingerprints, an
invented PWS station id.
"""

from __future__ import annotations

import gzip
import json
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.test_phase7_surface import (
    _csrf_headers,
    _make_app,
    _make_site,
    _seed_published_run,
)
from wxverify import config
from wxverify.db.connection import Database, close_db, init_db
from wxverify.settings.depth import depth_override_key
from wxverify.settings.keys import set_setting
from wxverify.verification.record import SNAPSHOT_TIME_KEY, build_forecast_record

_RECORD_DAY = "2026-05-20"

_ENDPOINT_SUFFIXES = (
    "status",
    "runs",
    "run_detail",
    "verdicts",
    "evidence",
    "diagnostics",
    "methodology",
)


def _seed_record_grid(conn: sqlite3.Connection, site_id: int) -> None:
    """Samples for every identity of the ``_RECORD_DAY`` grid.

    The record now writes only identities that had samples at T, so the
    24-row round-trip anchor below needs a fully seeded day.
    """
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('example-src', 'grid-model', 1, 360, 192)
        """
    )
    assert cur.lastrowid is not None
    feed_id = int(cur.lastrowid)
    start = datetime.fromisoformat(f"{_RECORD_DAY}T00:00:00+00:00")
    issued = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    fetched = (start + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for offset in range(8):
        for variable, value in (("temperature", 10.0), ("wind", 5.0), ("precip", 0.0)):
            for hour in range(24):
                valid = start + timedelta(days=offset, hours=hour)
                conn.execute(
                    """
                    INSERT INTO forecast_samples
                        (site_id, feed_id, variable, issued_at, valid_at,
                         lead_hours, value, source_raw, model_run_id, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-x', ?)
                    """,
                    (
                        site_id,
                        feed_id,
                        variable,
                        issued,
                        valid.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        max(1, int((valid - start).total_seconds() // 3600)),
                        value,
                        fetched,
                    ),
                )


def _capture(client: TestClient, site_id: int, run_id: int) -> dict[str, object]:
    urls = {
        "status": f"/api/verification/status?site={site_id}",
        "runs": f"/api/verification/runs?site={site_id}",
        "run_detail": f"/api/verification/runs/{run_id}",
        "verdicts": f"/api/verification/runs/{run_id}/verdicts",
        "evidence": f"/api/verification/runs/{run_id}/evidence",
        "diagnostics": f"/api/verification/runs/{run_id}/diagnostics",
        "methodology": f"/api/verification/runs/{run_id}/methodology",
    }
    out: dict[str, object] = {}
    for name in _ENDPOINT_SUFFIXES:
        resp = client.get(urls[name])
        assert resp.status_code == 200, name
        out[name] = resp.json()
    return out


def test_round_trip_serves_identical_api_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # --- Build the source DB standalone (app-layer connection only). ----
    source_path = tmp_path / "source-a.db"
    a_db = Database(str(source_path))
    try:
        conn_a = a_db._conn  # noqa: SLF001
        site_id = _make_site(conn_a, "Export Origin Town")
        # Settings BEFORE the run seed so the snapshot/fingerprint pin the
        # override (precip depth 5) and the per-site wall clock (06:30).
        set_setting(conn_a, depth_override_key("precip"), "5")
        set_setting(conn_a, SNAPSHOT_TIME_KEY, "05:45")
        set_setting(conn_a, f"{SNAPSHOT_TIME_KEY}:{site_id}", "06:30")
        run_id = _seed_published_run(conn_a, site_id)
        _seed_record_grid(conn_a, site_id)
        # One full record day: 8 target days x 3 variables = 24 rows,
        # written at T=06:30 local (the per-site override) + 30 minutes.
        build_forecast_record(
            conn_a,
            site_id,
            _RECORD_DAY,
            now=datetime(2026, 5, 20, 7, 0, tzinfo=UTC),
        )
        conn_a.commit()
        conn_a.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        a_db.close()
    payload = source_path.read_bytes()

    # Both apps run the SAME options so startup's clearing rule keeps the
    # precip override alive on each side.
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps({"forecast_blend_depth_precip": 5}), encoding="utf-8"
    )
    config.options_path = str(options_path)

    # --- App A serves the source DB directly. ---------------------------
    close_db()
    config.db_path = str(source_path)
    init_db(str(source_path))
    app_a = _make_app(monkeypatch)
    with TestClient(app_a) as client_a:
        responses_a = _capture(client_a, site_id, run_id)
    close_db()

    # Non-vacuity anchors on side A before comparing: the payloads carry
    # the real phase-7 state, not empty shells.
    status_sites = responses_a["status"]["sites"]  # type: ignore[index]
    assert status_sites[0]["published_run"]["run_id"] == run_id
    verdicts = responses_a["verdicts"]["verdicts"]  # type: ignore[index]
    assert {v["variable"]: v["outcome"] for v in verdicts} == {
        "temperature": "recommend",
        "wind": "retain_incumbent",
        "precip": "skipped",
    }
    assert len(responses_a["evidence"]["evidence"]) == 3  # type: ignore[index]
    snapshot = responses_a["run_detail"]["run"]["config_snapshot"]  # type: ignore[index]
    assert snapshot["blend_depths"] == {"temperature": 2, "wind": 2, "precip": 5}
    assert snapshot["wall_clock"] == "06:30"

    # --- App B: fresh DB, import the bytes, serve the same endpoints. ---
    b_path = tmp_path / "serving-b.db"
    config.db_path = str(b_path)
    init_db(str(b_path))
    app_b = _make_app(monkeypatch)
    with TestClient(app_b) as client_b:
        headers = {
            **_csrf_headers(client_b),
            "Content-Type": "application/octet-stream",
        }
        resp = client_b.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 200
        assert resp.json()["status"] == "imported"
        responses_b = _capture(client_b, site_id, run_id)
    close_db()

    assert responses_b == responses_a

    # --- Direct row-level checks on the imported store. -----------------
    direct = sqlite3.connect(config.db_path)
    direct.row_factory = sqlite3.Row
    try:
        record_rows = direct.execute(
            """
            SELECT COUNT(*) AS n FROM forecast_of_record
            WHERE site_id = ? AND snapshot_local_date = ?
            """,
            (site_id, _RECORD_DAY),
        ).fetchone()
        assert int(record_rows["n"]) == 24  # 8 days x 3 variables

        settings = {
            str(r["key"]): str(r["value"])
            for r in direct.execute(
                "SELECT key, value FROM settings WHERE key IN (?, ?, ?)",
                (
                    depth_override_key("precip"),
                    SNAPSHOT_TIME_KEY,
                    f"{SNAPSHOT_TIME_KEY}:{site_id}",
                ),
            )
        }
        assert settings == {
            depth_override_key("precip"): "5",
            SNAPSHOT_TIME_KEY: "05:45",
            f"{SNAPSHOT_TIME_KEY}:{site_id}": "06:30",
        }

        pointer = direct.execute(
            "SELECT value FROM runtime_state WHERE key = ?",
            (f"verification_published_run:{site_id}",),
        ).fetchone()
        assert pointer is not None
        assert int(pointer["value"]) == run_id

        policy = direct.execute(
            """
            SELECT policy FROM forecast_of_record
            WHERE site_id = ? AND snapshot_local_date = ?
            ORDER BY id LIMIT 1
            """,
            (site_id, _RECORD_DAY),
        ).fetchone()
        parsed = json.loads(str(policy["policy"]))
        assert parsed["blend_depths"] == {
            "temperature": 2,
            "wind": 2,
            "precip": 5,
        }
        assert parsed["blend_depth_sources"]["precip"] == "override"
    finally:
        direct.close()


# ---------------------------------------------------------------------------
# The REAL transfer path. The oracle above hand-builds the source file and
# posts its bytes; that never exercises `POST /api/export/begin` ->
# `GET /export/status` -> `GET /export/download` (VACUUM INTO + gzip), so an
# export that failed to carry the 0.11.0 tables would go unnoticed there.
# This one runs the shipped loop end to end and adds the two §18.10 artifacts
# the byte-comparison above cannot see: truth generations (timezone_
# generations + daily_truth) and survival of the post-import derived
# rebuild, which DELETEs and regenerates forecast_pairs/score_cache and must
# leave every verification artifact and the published-run pointer untouched.
# ---------------------------------------------------------------------------

_EXPORT_BASE = "/api/export"


def _await_ready(client: TestClient, export_id: str, *, tries: int = 100) -> None:
    """Poll `status` until the fire-and-forget snapshot leaves `preparing`.

    Each poll re-enters the TestClient loop, which is what lets the `begin`
    task progress; no wall-clock assertion is made on the result.
    """
    for _ in range(tries):
        data = client.get(f"{_EXPORT_BASE}/status/{export_id}").json()
        if data["state"] != "preparing":
            assert data["state"] == "ready", data
            return
        time.sleep(0.02)
    raise AssertionError(f"export {export_id} never left 'preparing'")


def _seed_truth_generation(conn: sqlite3.Connection, site_id: int) -> tuple[int, str]:
    """A published tz generation plus one materialized daily_truth row.

    Returns (generation_id, local_date). Values are hand-fixed so the
    post-restore comparison is against literals, not a re-read of the
    source.
    """
    generation_id = int(
        conn.execute(
            "SELECT id FROM timezone_generations"
            " WHERE site_id = ? AND state = 'published'",
            (site_id,),
        ).fetchone()["id"]
    )
    conn.execute(
        """
        INSERT INTO daily_truth
            (site_id, tz_generation_id, local_date, quantity, value, eligible,
             covered_hours, expected_slots, peak_window_ok, day_start_utc,
             day_end_utc, timezone)
        VALUES (?, ?, '2026-05-19', 'temperature_high', 21.5, 1, 24, 24, 1,
                '2026-05-19T00:00:00Z', '2026-05-20T00:00:00Z', 'UTC')
        """,
        (site_id, generation_id),
    )
    return generation_id, "2026-05-19"


def test_shipped_export_download_restores_full_0_11_0_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps({"forecast_blend_depth_precip": 5}), encoding="utf-8"
    )
    config.options_path = str(options_path)

    # --- Source instance: real app, real export endpoints. --------------
    source_path = tmp_path / "origin.db"
    close_db()
    config.db_path = str(source_path)
    conn_a = init_db(str(source_path))._conn  # noqa: SLF001
    site_id = _make_site(conn_a, "Shipped Export Town")
    set_setting(conn_a, depth_override_key("precip"), "5")
    run_id = _seed_published_run(conn_a, site_id)
    generation_id, truth_date = _seed_truth_generation(conn_a, site_id)
    # Station data so the post-import derived rebuild has real work to do:
    # `_rebuild_all` DELETEs forecast_pairs and re-materializes consensus
    # from these rows. Without them the rebuild is a no-op and "the rebuild
    # did not destroy the verification state" would be vacuously true.
    station_id = int(
        conn_a.execute(
            """
            INSERT INTO stations (site_id, pws_station_id, lat, lon,
                                  dem_elevation_m, enabled)
            VALUES (?, 'FAKESTATION1', 40.0, -105.0, 900.0, 1)
            """,
            (site_id,),
        ).lastrowid
    )
    conn_a.executemany(
        """
        INSERT INTO station_observations
            (station_id, variable, valid_at, value, qc_flag, fetched_at)
        VALUES (?, 'temperature', ?, ?, 'ok', '2026-05-19T23:00:00Z')
        """,
        [(station_id, f"2026-05-19T{h:02d}:00:00Z", 10.0 + h) for h in range(24)],
    )
    conn_a.commit()

    app_a = _make_app(monkeypatch)
    with TestClient(app_a) as client_a:
        begin = client_a.post(f"{_EXPORT_BASE}/begin", headers=_csrf_headers(client_a))
        assert begin.status_code == 202
        export_id = str(begin.json()["export_id"])
        _await_ready(client_a, export_id)
        download = client_a.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert download.status_code == 200
        assert download.content[:2] == b"\x1f\x8b"  # gzip magic
        payload = gzip.decompress(download.content)
        source_api = _capture(client_a, site_id, run_id)
    close_db()

    # --- Destination instance: empty DB, then the shipped import. -------
    dest_path = tmp_path / "dest.db"
    config.db_path = str(dest_path)
    init_db(str(dest_path))
    app_b = _make_app(monkeypatch)
    with TestClient(app_b) as client_b:
        # Non-vacuity: before the import the destination knows nothing
        # about this run, so the post-import equality below cannot be
        # satisfied by ambient state.
        assert client_b.get(f"/api/verification/runs/{run_id}").status_code == 404
        headers = {
            **_csrf_headers(client_b),
            "Content-Type": "application/octet-stream",
        }
        resp = client_b.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 200
        # The derived rebuild is a post-response background task; the
        # capture below re-enters the loop and lets it finish first.
        dest_api = _capture(client_b, site_id, run_id)
    close_db()

    assert dest_api == source_api

    # --- Row-level survival of the artifacts the API does not expose. ---
    direct = Database(str(dest_path))
    try:
        conn_b = direct._conn  # noqa: SLF001
        # Truth generations: same generation id, same materialized row.
        gen = conn_b.execute(
            "SELECT id, state FROM timezone_generations WHERE site_id = ?",
            (site_id,),
        ).fetchall()
        assert [(int(r["id"]), str(r["state"])) for r in gen] == [
            (generation_id, "published")
        ]
        truth = conn_b.execute(
            """
            SELECT value, covered_hours, eligible FROM daily_truth
            WHERE site_id = ? AND local_date = ? AND quantity = 'temperature_high'
            """,
            (site_id, truth_date),
        ).fetchone()
        assert truth is not None
        assert float(truth["value"]) == 21.5
        assert int(truth["covered_hours"]) == 24
        assert int(truth["eligible"]) == 1
        # The run still points at that generation.
        run_gen = conn_b.execute(
            "SELECT tz_generation_id FROM verification_runs WHERE id = ?", (run_id,)
        ).fetchone()
        assert int(run_gen["tz_generation_id"]) == generation_id
        # Published-run pointer survived the rebuild.
        pointer = conn_b.execute(
            "SELECT value FROM runtime_state WHERE key = ?",
            (f"verification_published_run:{site_id}",),
        ).fetchone()
        assert pointer is not None and int(pointer["value"]) == run_id
        # The rebuild genuinely ran: consensus was materialized from the
        # imported station rows (so the survival assertions above are not
        # vacuous).
        obs = conn_b.execute(
            "SELECT COUNT(*) AS n FROM observations WHERE site_id = ?", (site_id,)
        ).fetchone()
        assert int(obs["n"]) == 24
        rebuild = conn_b.execute(
            "SELECT value FROM runtime_state WHERE key = 'import_rebuild_state'"
        ).fetchone()
        assert rebuild is not None and str(rebuild["value"]) == "done"
        # And the depth override is still an override, not a global.
        assert (
            conn_b.execute(
                "SELECT value FROM settings WHERE key = ?",
                (depth_override_key("precip"),),
            ).fetchone()["value"]
            == "5"
        )
    finally:
        direct.close()
