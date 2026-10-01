"""HTTP-level regression test for ``DELETE /api/sites/{site_id}`` wind-state
cleanup (release 0.16.6).

Site ids are reused (``sites.id INTEGER PRIMARY KEY``, no ``AUTOINCREMENT``),
and an absent ``wind_basis:<id>`` key means ``pair_max``
(``wxverify.db.wind_basis.wind_basis_state``). Before this fix, deleting a
site left its six per-site ``runtime_state`` wind keys behind; a later site
recreated at the same id would then inherit whatever stale wind-basis state
(e.g. ``staging``) the deleted site had stamped, instead of starting fresh at
``pair_max``.

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
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.wind_basis import (
    auth_hold_key,
    wind_basis_key,
    wind_basis_state,
    wind_blocked_key,
    wind_cursor_key,
    wind_done_at_key,
    wind_progress_key,
    wind_report_key,
)

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


def _wind_keys(site_id: int) -> tuple[str, ...]:
    return (
        wind_basis_key(site_id),
        wind_progress_key(site_id),
        wind_cursor_key(site_id),
        wind_blocked_key(site_id),
        wind_report_key(site_id),
        wind_done_at_key(site_id),
    )


def _seed_wind_keys(conn: sqlite3.Connection, site_id: int) -> None:
    for key in _wind_keys(site_id):
        set_runtime_state(conn, key, "staging")


def _assert_all_present(conn: sqlite3.Connection, site_id: int) -> None:
    for key in _wind_keys(site_id):
        assert get_runtime_state(conn, key) is not None, key


def _assert_all_absent(conn: sqlite3.Connection, site_id: int) -> None:
    for key in _wind_keys(site_id):
        assert get_runtime_state(conn, key) is None, key


# ---------------------------------------------------------------------------
# DELETE /api/sites/{site_id} — wind-state cleanup, scoped to the deleted
# site only, and a reused id starts fresh at pair_max.
# ---------------------------------------------------------------------------


def test_delete_site_drops_its_wind_state_leaves_other_site_and_auth_hold_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_y = _make_site(conn, "Testsite Y")
    site_x = _make_site(conn, "Testsite X")  # created second -> highest id
    assert site_x > site_y

    _seed_wind_keys(conn, site_x)
    _seed_wind_keys(conn, site_y)
    hold_key = auth_hold_key("history_all")
    set_runtime_state(conn, hold_key, "held")

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        # The app's lifespan calls init_db() again on startup, replacing the
        # Database (and closing the seeding connection above); re-fetch the
        # live connection for every assertion from here on.
        conn = get_db()._conn  # noqa: SLF001

        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}

        response = client.delete(f"/api/sites/{site_x}", headers=headers)
        assert response.status_code == 200

        # X's six wind keys are gone; Y's and the auth-hold key are intact.
        _assert_all_absent(conn, site_x)
        _assert_all_present(conn, site_y)
        assert get_runtime_state(conn, hold_key) == "held"

        # A new site reuses X's id (INTEGER PRIMARY KEY, no AUTOINCREMENT)
        # and must start at the absent-key default, pair_max -- not inherit
        # X's stale "staging" state.
        create_resp = client.post(
            "/api/sites",
            headers=headers,
            json={
                "name": "Testsite Z",
                "forecast_lat": 41.0,
                "forecast_lon": -104.0,
                "elevation_m": 950.0,
                "timezone": "UTC",
                "rain_threshold_mm": 0.5,
            },
        )
        assert create_resp.status_code == 200
        new_id = create_resp.json()["id"]
        assert new_id == site_x, (
            "precondition for this regression: deleted site's id must be "
            "reused by the next insert (INTEGER PRIMARY KEY, no "
            "AUTOINCREMENT) -- if this fails, the id-reuse precondition no "
            "longer holds and the test needs to be rebuilt, not weakened"
        )
        assert wind_basis_state(conn, new_id) == "pair_max"

        # Paired negative: deleting a nonexistent site 404s and touches no
        # state at all.
        missing_id = site_x + 1000
        missing_resp = client.delete(f"/api/sites/{missing_id}", headers=headers)
        assert missing_resp.status_code == 404
        _assert_all_present(conn, site_y)
        assert get_runtime_state(conn, hold_key) == "held"
