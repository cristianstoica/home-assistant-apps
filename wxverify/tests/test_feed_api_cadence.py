"""HTTP-level tests for ``/api/feeds`` cadence validation and readback.

Covers: ``PUT /api/feeds/{id}`` rejects a ``fetch_interval_minutes`` above the
runtime ceiling, accepts one exactly at the ceiling, a non-regression pin
that updating an unrelated field (``enabled``) still succeeds against a feed
whose stored interval already exceeds the ceiling, and that ``GET
/api/feeds`` survives a hostile stored cadence instead of raising.

Isolation: real tmp-file SQLite DB via ``init_db``/``close_db`` + an idle
worker + ``TestClient`` (mirrors ``tests/test_forecast_routes.py``'s
harness).

Synthetic fixtures only (public repo): no real site/station identifiers.
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
from wxverify.db.connection import close_db, init_db
from wxverify.worker.cadence import MAX_FETCH_INTERVAL_MINUTES


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


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
    ).fetchone()
    assert row is not None, f"seed feed not found: {source}/{model}"
    return int(row["id"])


def test_put_feed_rejects_interval_above_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        response = client.put(
            f"/api/feeds/{feed_id}",
            json={"fetch_interval_minutes": MAX_FETCH_INTERVAL_MINUTES + 1},
            headers=headers,
        )
    assert response.status_code == 422


def test_put_feed_accepts_interval_at_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        response = client.put(
            f"/api/feeds/{feed_id}",
            json={"fetch_interval_minutes": MAX_FETCH_INTERVAL_MINUTES},
            headers=headers,
        )
    assert response.status_code == 200
    assert response.json()["fetch_interval_minutes"] == MAX_FETCH_INTERVAL_MINUTES


def test_put_feed_unrelated_field_survives_stored_interval_above_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Non-regression pin: the schema bound only constrains new writes --
    updating an unrelated field against a feed whose ALREADY-STORED interval
    exceeds the ceiling (e.g. hand-edited or imported) must still succeed."""
    conn = _init_tmp_db(tmp_path)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 50000 WHERE id = ?", (feed_id,)
    )
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        response = client.put(
            f"/api/feeds/{feed_id}", json={"enabled": True}, headers=headers
        )
    assert response.status_code == 200


def test_get_feeds_survives_hostile_stored_cadence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    conn.execute("UPDATE feeds SET fetch_interval_minutes = 0 WHERE id = ?", (feed_id,))
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/api/feeds")
    assert response.status_code == 200
    by_id = {row["id"]: row for row in response.json()}
    assert by_id[feed_id]["fetch_interval_minutes"] is None


def test_get_feeds_survives_non_integral_real_stored_cadence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fractional REAL (e.g. an imported/hand-edited 1.9) must surface as
    ``None``, not be silently floored to 1."""
    conn = _init_tmp_db(tmp_path)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 1.9 WHERE id = ?", (feed_id,)
    )
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/api/feeds")
    assert response.status_code == 200
    by_id = {row["id"]: row for row in response.json()}
    assert by_id[feed_id]["fetch_interval_minutes"] is None


def test_get_feeds_accepts_exact_integral_real_stored_cadence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression pin: a whole-number cadence written via a REAL literal
    (e.g. 360.0) -- stored as the integer 360 by this INTEGER-affinity
    column -- is a legitimate cadence and must surface as its integer
    value, not be rejected. The genuine REAL-carrier acceptance is pinned
    at the unit layer in
    test_cadence_parse.py::test_accepts_exact_integral_real."""
    conn = _init_tmp_db(tmp_path)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 360.0 WHERE id = ?", (feed_id,)
    )
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/api/feeds")
    assert response.status_code == 200
    by_id = {row["id"]: row for row in response.json()}
    assert by_id[feed_id]["fetch_interval_minutes"] == 360
