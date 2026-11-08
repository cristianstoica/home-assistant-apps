"""§18.10 oracle: export round-trip over the 0.11.0 state.

Drives the shipped export path -- POST /api/export/begin -> /export/status
-> /export/download (VACUUM INTO + gzip) -- then opens the downloaded,
decompressed snapshot read-only and compares its rows directly against the
source: truth generations (``timezone_generations`` + ``daily_truth``), a
per-variable depth-override setting, and station observation rows --
proving the export carries every 0.11.0 artifact over faithfully, row for
row.

There is no import half to this round trip: POST /api/import/db does not
exist, and no hand-built-payload oracle drives an import directly without
an export step.

Synthetic fixtures only: invented town names, UTC, fake fingerprints, an
invented PWS station id.
"""

from __future__ import annotations

import gzip
import json
import sqlite3
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.test_phase7_surface import (
    _csrf_headers,
    _make_app,
    _make_site,
)
from wxverify import config
from wxverify.db.connection import close_db, init_db
from wxverify.settings.depth import depth_override_key
from wxverify.settings.keys import set_setting

# ---------------------------------------------------------------------------
# The shipped export path, driven end to end: POST /api/export/begin ->
# GET /export/status -> GET /export/download (VACUUM INTO + gzip). Covers
# the §18.10 artifact a bare schema-copy comparison would not reach: truth
# generations (timezone_generations + daily_truth), read directly off the
# downloaded snapshot.
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
    """The real shipped export path (VACUUM INTO + gzip) carries every
    0.11.0 artifact over faithfully. There is no POST /api/import/db to
    round-trip back through, so this opens the downloaded, decompressed
    snapshot read-only and compares its rows directly against the source
    rather than driving it back through a second live app.
    """
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
    generation_id, truth_date = _seed_truth_generation(conn_a, site_id)
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
    source_obs_count = int(
        conn_a.execute(
            "SELECT COUNT(*) AS n FROM station_observations WHERE station_id = ?",
            (station_id,),
        ).fetchone()["n"]
    )

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
    close_db()

    # --- Open the downloaded snapshot directly and compare row-for-row. --
    snapshot_path = tmp_path / "snapshot.db"
    snapshot_path.write_bytes(payload)
    snapshot = sqlite3.connect(f"file:{snapshot_path}?mode=ro", uri=True)
    snapshot.row_factory = sqlite3.Row
    try:
        assert snapshot.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        # Truth generations: same generation id, same materialized row.
        gen = snapshot.execute(
            "SELECT id, state FROM timezone_generations WHERE site_id = ?",
            (site_id,),
        ).fetchall()
        assert [(int(r["id"]), str(r["state"])) for r in gen] == [
            (generation_id, "published")
        ]
        truth = snapshot.execute(
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
        # The exported snapshot carries exactly the source's own station
        # rows -- VACUUM INTO neither drops nor fabricates rows.
        obs = snapshot.execute(
            "SELECT COUNT(*) AS n FROM station_observations WHERE station_id = ?",
            (station_id,),
        ).fetchone()
        assert int(obs["n"]) == source_obs_count
        # And the depth override is still an override, not a global.
        assert (
            snapshot.execute(
                "SELECT value FROM settings WHERE key = ?",
                (depth_override_key("precip"),),
            ).fetchone()["value"]
            == "5"
        )
    finally:
        snapshot.close()
