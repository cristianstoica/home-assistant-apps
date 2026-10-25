"""HTTP-level regression tests for the site and generation state that
``DELETE /api/sites/{site_id}`` and ``POST /api/sites`` clear.

Site and timezone generation ids are reused (``INTEGER PRIMARY KEY``, no
``AUTOINCREMENT``), and the ``runtime_state`` and ``settings`` rows keyed by
them have no foreign key, so the ``sites`` cascade never reaches them. A
delete drops the site's published generation pointer, verification state and
heartbeat, published run, gap-scan failures and per-site snapshot time, plus
the correction-chain state and heartbeat of each of its generations, and
leaves every other site's rows and the shared auth hold intact; a create
clears whatever a pre-0.16.7 delete left on the reused id before seeding the
new site. A verification day computed before a delete and recreate is
discarded, and a failed cleanup rolls the whole delete back.

Isolation: a real tmp-file SQLite DB via ``init_db``/``close_db`` + an idle
worker + ``TestClient`` (mirrors ``tests/test_forecast_routes.py`` /
``tests/test_web_ui.py``'s harness) — a tmp-file DB, not ``:memory:``, for
the same reason those modules use one: the app's WAL-mode reads come from a
separate pooled connection than whatever writes the fixture rows.

Synthetic fixtures only — fake site names and station ids.
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
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.runtime_state import (
    get_runtime_state,
    set_runtime_state,
)
from wxverify.db.tz_generations import (
    correction_heartbeat_key,
    correction_state_key,
    ensure_published_generation,
    published_generation_id,
    published_pointer_key,
)
from wxverify.db.wind_basis import auth_hold_key
from wxverify.settings.keys import get_setting, set_setting
from wxverify.verification.record import (
    SNAPSHOT_TIME_KEY,
    gap_scan_degraded_sites,
    gap_scan_failures_key,
    snapshot_wall_clock,
)
from wxverify.verification.runs import published_run_id, published_run_key
from wxverify.worker.control import JobCancelled
from wxverify.worker.verification_run import (
    _DayWork,
    _persist_day,
    verification_heartbeat_key,
    verification_state_key,
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


# ---------------------------------------------------------------------------
# Test A/B/D/F helpers (plan §6).
# ---------------------------------------------------------------------------


def _site_keys(site_id: int) -> tuple[str, ...]:
    return (
        published_pointer_key(site_id),  # first: the pre-fix code fails here
        verification_state_key(site_id),
        verification_heartbeat_key(site_id),
        published_run_key(site_id),
        gap_scan_failures_key(site_id),
    )


def _correction_keys(generation_id: int) -> tuple[str, str]:
    return (
        correction_state_key(generation_id),
        correction_heartbeat_key(generation_id),
    )


def _add_correction_generation(conn: sqlite3.Connection, site_id: int) -> int:
    return int(
        conn.execute(
            "INSERT INTO timezone_generations (site_id, timezone, mode, state)"
            " VALUES (?, 'UTC', 'retrospective_correction', 'building')",
            (site_id,),
        ).lastrowid
    )


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
# Test A: every site/generation key the delete must drop, and a reused id
# starts fresh (F1-F6).
# ---------------------------------------------------------------------------


def test_delete_site_drops_all_site_and_generation_state_and_a_reused_id_starts_fresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_y = _make_site(conn, "Testsite Y")
    site_x = _make_site(conn, "Testsite X")
    assert site_y == 1 and site_x == 2

    gen_y = ensure_published_generation(conn, site_y)
    gen_x = ensure_published_generation(conn, site_x)
    assert gen_y == 1 and gen_x == 2
    corr_y = _add_correction_generation(conn, site_y)
    corr_x = _add_correction_generation(conn, site_x)
    assert corr_y == 3 and corr_x == 4

    for site, stamp, snap in (
        (site_y, "2026-03-08T07:00:00Z", "06:15"),
        (site_x, "2026-03-09T07:00:00Z", "05:45"),
    ):
        set_runtime_state(
            conn, verification_state_key(site), '{"phase":"bootstrap","run_id":7}'
        )
        set_runtime_state(conn, verification_heartbeat_key(site), stamp)
        set_runtime_state(conn, published_run_key(site), "7")
        set_runtime_state(
            conn,
            gap_scan_failures_key(site),
            json.dumps(
                {
                    "as_of": stamp,
                    "dates": {
                        "2026-03-08": {
                            "error": "synthetic",
                            "last_failed_at": stamp,
                        }
                    },
                }
            ),
        )
        set_setting(conn, f"{SNAPSHOT_TIME_KEY}:{site}", snap)

    for generation in (corr_y, corr_x):
        set_runtime_state(conn, correction_state_key(generation), '{"phase":"days"}')
        set_runtime_state(
            conn, correction_heartbeat_key(generation), "2026-03-09T07:00:00Z"
        )

    set_setting(conn, SNAPSHOT_TIME_KEY, "05:00")
    hold_key = auth_hold_key("history_all")
    set_runtime_state(conn, hold_key, "held")

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        conn = get_db()._conn  # noqa: SLF001

        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}

        # 1. Positive controls before the delete.
        assert published_generation_id(conn, site_x) == gen_x
        assert gap_scan_degraded_sites(conn) == (2, "2026-03-09T07:00:00Z")

        # 2. Delete.
        response = client.delete(f"/api/sites/{site_x}", headers=headers)
        assert response.status_code == 200

        # 3. Before the recreate.
        for key in (*_site_keys(site_x), *_correction_keys(corr_x)):
            assert get_runtime_state(conn, key) is None, key
        assert get_setting(conn, f"{SNAPSHOT_TIME_KEY}:{site_x}") is None
        for key in (*_site_keys(site_y), *_correction_keys(corr_y)):
            assert get_runtime_state(conn, key) is not None, key
        assert snapshot_wall_clock(conn, site_y) == "06:15"
        assert get_setting(conn, SNAPSHOT_TIME_KEY) == "05:00"
        assert get_runtime_state(conn, hold_key) == "held"
        assert gap_scan_degraded_sites(conn) == (1, "2026-03-08T07:00:00Z")

        # Paired negative: deleting a nonexistent site 404s and touches no
        # state at all.
        missing_id = site_x + 1000
        missing_resp = client.delete(f"/api/sites/{missing_id}", headers=headers)
        assert missing_resp.status_code == 404
        for key in _site_keys(site_y):
            assert get_runtime_state(conn, key) is not None, key
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
        pointer = published_generation_id(conn, new_id)
        assert pointer == corr_x, (
            "precondition: the next generation id must be the reused "
            "value -- rebuild, don't weaken"
        )
        gen_row = conn.execute(
            "SELECT site_id, mode, state FROM timezone_generations WHERE id = ?",
            (pointer,),
        ).fetchone()
        assert tuple(gen_row) == (new_id, "initial", "published")
        for key in _correction_keys(pointer):
            assert get_runtime_state(conn, key) is None, key
        assert published_run_id(conn, new_id) is None
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
    gen_y = ensure_published_generation(conn, site_y)
    set_runtime_state(
        conn,
        gap_scan_failures_key(site_y),
        json.dumps(
            {
                "as_of": "2026-03-08T07:00:00Z",
                "dates": {
                    "2026-03-08": {
                        "error": "synthetic",
                        "last_failed_at": "2026-03-08T07:00:00Z",
                    }
                },
            }
        ),
    )

    orphan = site_y + 1
    set_runtime_state(conn, published_pointer_key(orphan), "99")
    set_runtime_state(
        conn,
        gap_scan_failures_key(orphan),
        json.dumps(
            {
                "as_of": "2026-03-09T07:00:00Z",
                "dates": {
                    "2026-03-09": {
                        "error": "synthetic",
                        "last_failed_at": "2026-03-09T07:00:00Z",
                    }
                },
            }
        ),
    )
    set_runtime_state(
        conn, verification_state_key(orphan), '{"phase":"bootstrap","run_id":7}'
    )
    set_runtime_state(conn, published_run_key(orphan), "7")
    set_setting(conn, f"{SNAPSHOT_TIME_KEY}:{orphan}", "05:45")

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        conn = get_db()._conn  # noqa: SLF001

        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}

        # 1. Before the create, the orphan (no site row yet) is ignored.
        assert gap_scan_degraded_sites(conn) == (1, "2026-03-08T07:00:00Z")

        # 2. Create; the new site must reuse the orphan's id.
        create_resp = _post_create(client, headers, "Testsite Z")
        assert create_resp.status_code == 200
        new_id = create_resp.json()["id"]
        assert new_id == orphan, (
            "precondition: the new site's id must equal the orphan's id "
            "-- rebuild, don't weaken"
        )

        # 3. After the create.
        pointer = published_generation_id(conn, new_id)
        assert pointer is not None
        assert pointer != 99
        gen_row = conn.execute(
            "SELECT site_id, mode, state FROM timezone_generations WHERE id = ?",
            (pointer,),
        ).fetchone()
        assert tuple(gen_row) == (new_id, "initial", "published")
        assert get_runtime_state(conn, verification_state_key(new_id)) is None
        assert published_run_id(conn, new_id) is None
        assert gap_scan_degraded_sites(conn) == (1, "2026-03-08T07:00:00Z")
        assert get_setting(conn, f"{SNAPSHOT_TIME_KEY}:{new_id}") is None
        assert published_generation_id(conn, site_y) == gen_y


# ---------------------------------------------------------------------------
# Test D: a verification day computed before a delete and recreate is
# discarded (F4, F12).
# ---------------------------------------------------------------------------


def test_verification_day_computed_before_delete_and_recreate_is_cancelled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_x = _make_site(conn, "Testsite X")

    raw = '{"phase":"simulate","run_id":7,"cursor":"2026-03-08"}'
    set_runtime_state(conn, verification_state_key(site_x), raw)
    work = _DayWork(
        raw_blob=raw,
        next_blob={"phase": "simulate", "run_id": 7, "cursor": "2026-03-09"},
        evidence=None,
        day="2026-03-08",
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        conn = get_db()._conn  # noqa: SLF001

        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}

        response = client.delete(f"/api/sites/{site_x}", headers=headers)
        assert response.status_code == 200

        create_resp = _post_create(client, headers, "Testsite Z")
        assert create_resp.status_code == 200
        new_id = create_resp.json()["id"]
        assert new_id == site_x, (
            "precondition: deleted site's id must be reused by the next "
            "insert -- rebuild, don't weaken"
        )

        with pytest.raises(JobCancelled):
            _persist_day(conn, new_id, work)
        assert get_runtime_state(conn, verification_state_key(new_id)) is None


# ---------------------------------------------------------------------------
# Test F: a failed state cleanup rolls back the whole delete.
# ---------------------------------------------------------------------------


def test_delete_site_rolls_back_entirely_when_the_state_cleanup_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_x = _make_site(conn, "Testsite X")
    gen_x = ensure_published_generation(conn, site_x)
    set_setting(conn, f"{SNAPSHOT_TIME_KEY}:{site_x}", "05:45")

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("synthetic cleanup failure")

    monkeypatch.setattr("wxverify.api.routes.sites.delete_runtime_state", _boom)

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
        assert published_generation_id(conn, site_x) == gen_x
        assert get_setting(conn, f"{SNAPSHOT_TIME_KEY}:{site_x}") == "05:45"
