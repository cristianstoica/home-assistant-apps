"""HTTP-level regression tests for the site state that
``DELETE /api/sites/{site_id}`` and ``POST /api/sites`` clear.

Site and timezone generation ids are reused (``INTEGER PRIMARY KEY``, no
``AUTOINCREMENT``), and the ``runtime_state`` and ``settings`` rows keyed by
them have no foreign key, so the ``sites`` cascade never reaches them. A
delete drops the site's per-site snapshot time, and leaves every other
site's rows and the shared auth hold intact; a create clears whatever a
pre-fix delete left on the reused id before seeding the new site. A failed
cleanup rolls the whole delete back.

Isolation: a real tmp-file SQLite DB via ``init_db``/``close_db`` + an idle
worker + ``TestClient`` (mirrors ``tests/test_forecast_routes.py`` /
``tests/test_web_ui.py``'s harness) — a tmp-file DB, not ``:memory:``, for
the same reason those modules use one: the app's WAL-mode reads come from a
separate pooled connection than whatever writes the fixture rows.

Synthetic fixtures only — fake site names and station ids.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.runtime_state import (
    get_runtime_state,
    set_runtime_state,
)
from wxverify.db.switch import ensure_site_generation, site_generation_id
from wxverify.settings.keys import get_setting, set_setting
from wxverify.verification.snapshot_time import SNAPSHOT_TIME_KEY, snapshot_wall_clock

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_forecast_routes.py).
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


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)
            """,
            (name,),
        ).lastrowid
    )


# ---------------------------------------------------------------------------
# Test A/B/F helpers (plan §6).
# ---------------------------------------------------------------------------


def _post_create(client: Any, headers: dict[str, str], name: str) -> Any:
    return client.post(
        "/api/sites",
        headers=headers,
        json={
            "name": name,
            "forecast_lat": 41.0,
            "forecast_lon": -104.0,
            "elevation_m": 950.0,
            "timezone": "UTC",
            "rain_threshold_mm": 0.5,
        },
    )


# ---------------------------------------------------------------------------
# Test A: every site key the delete must drop, and a reused id starts fresh
# (F1-F6).
# ---------------------------------------------------------------------------


def test_delete_site_drops_all_site_state_and_a_reused_id_starts_fresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_y = _make_site(conn, "Testsite Y")
    site_x = _make_site(conn, "Testsite X")
    assert site_y == 1 and site_x == 2

    gen_y = ensure_site_generation(conn, site_y)
    gen_x = ensure_site_generation(conn, site_x)
    assert gen_y == 1 and gen_x == 2

    for site, snap in ((site_y, "06:15"), (site_x, "05:45")):
        set_setting(conn, f"{SNAPSHOT_TIME_KEY}:{site}", snap)

    set_setting(conn, SNAPSHOT_TIME_KEY, "05:00")
    # A neutral literal key, untouched by the delete (the untouched-key control).
    hold_key = "wind_history_auth_hold:history_all"
    set_runtime_state(conn, hold_key, "held")

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        conn = get_db()._conn  # noqa: SLF001

        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}

        # 1. Positive controls before the delete.
        assert site_generation_id(conn, site_x) == gen_x

        # 2. Delete.
        response = client.delete(f"/api/sites/{site_x}", headers=headers)
        assert response.status_code == 200

        # 3. Before the recreate.
        assert get_setting(conn, f"{SNAPSHOT_TIME_KEY}:{site_x}") is None
        assert snapshot_wall_clock(conn, site_y) == "06:15"
        assert get_setting(conn, SNAPSHOT_TIME_KEY) == "05:00"
        assert get_runtime_state(conn, hold_key) == "held"

        # Paired negative: deleting a nonexistent site 404s and touches no
        # state at all.
        missing_id = site_x + 1000
        missing_resp = client.delete(f"/api/sites/{missing_id}", headers=headers)
        assert missing_resp.status_code == 404
        assert snapshot_wall_clock(conn, site_y) == "06:15"
        assert get_runtime_state(conn, hold_key) == "held"

        # 4. Recreate.
        create_resp = _post_create(client, headers, "Testsite Z")
        assert create_resp.status_code == 200
        new_id = create_resp.json()["id"]
        assert new_id == site_x, (
            "precondition: deleted site's id must be reused by the next "
            "insert -- rebuild, don't weaken"
        )

        # 5. After the recreate.
        new_gen = site_generation_id(conn, new_id)
        assert new_gen is not None
        gen_row = conn.execute(
            "SELECT site_id, mode, state FROM timezone_generations WHERE id = ?",
            (new_gen,),
        ).fetchone()
        assert tuple(gen_row) == (new_id, "initial", "published")
        assert snapshot_wall_clock(conn, new_id) == "05:00"


# ---------------------------------------------------------------------------
# Test B: a create-time clear removes pre-0.16.7 leftovers on a reused id
# (F7).
# ---------------------------------------------------------------------------


def test_create_site_clears_keys_a_pre_fix_delete_left_on_the_reused_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_y = _make_site(conn, "Testsite Y")
    gen_y = ensure_site_generation(conn, site_y)

    orphan = site_y + 1
    set_setting(conn, f"{SNAPSHOT_TIME_KEY}:{orphan}", "05:45")

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        conn = get_db()._conn  # noqa: SLF001

        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}

        # Create; the new site must reuse the orphan's id.
        create_resp = _post_create(client, headers, "Testsite Z")
        assert create_resp.status_code == 200
        new_id = create_resp.json()["id"]
        assert new_id == orphan, (
            "precondition: the new site's id must equal the orphan's id "
            "-- rebuild, don't weaken"
        )

        # After the create.
        pointer = site_generation_id(conn, new_id)
        assert pointer is not None
        gen_row = conn.execute(
            "SELECT site_id, mode, state FROM timezone_generations WHERE id = ?",
            (pointer,),
        ).fetchone()
        assert tuple(gen_row) == (new_id, "initial", "published")
        assert get_setting(conn, f"{SNAPSHOT_TIME_KEY}:{new_id}") is None
        assert site_generation_id(conn, site_y) == gen_y


# ---------------------------------------------------------------------------
# Test F: a failed state cleanup rolls back the whole delete.
# ---------------------------------------------------------------------------


def test_delete_site_rolls_back_entirely_when_the_state_cleanup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_x = _make_site(conn, "Testsite X")
    gen_x = ensure_site_generation(conn, site_x)
    set_setting(conn, f"{SNAPSHOT_TIME_KEY}:{site_x}", "05:45")

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("synthetic cleanup failure")

    monkeypatch.setattr("wxverify.api.routes.sites._clear_site_state", _boom)

    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        conn = get_db()._conn  # noqa: SLF001

        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}

        response = client.delete(f"/api/sites/{site_x}", headers=headers)
        assert response.status_code == 500

        conn = get_db()._conn  # noqa: SLF001
        assert (
            conn.execute("SELECT 1 FROM sites WHERE id = ?", (site_x,)).fetchone()
            is not None
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM timezone_generations WHERE site_id = ?",
                (site_x,),
            ).fetchone()[0]
            == 1
        )
        assert site_generation_id(conn, site_x) == gen_x
        assert get_setting(conn, f"{SNAPSHOT_TIME_KEY}:{site_x}") == "05:45"
