"""HTTP-level tests for the Forecast home page routes.

Spec build-sequence step 6 verify hook: "/" returns 200, "/forecast/tiles"
returns 204 vs a fragment depending on the fingerprint, "/api/forecast/hourly"
404s for an unknown site and clamps an out-of-range day, and the exact
empty-state copy renders when a site has no forecast data yet.

Isolation: a real tmp-file SQLite DB via ``init_db``/``close_db`` + an idle
worker + ``TestClient`` (mirrors ``tests/test_web_ui.py``'s harness) — a
tmp-file DB, not ``:memory:``, because the app's WAL-mode connection is a
SEPARATE ``_read_conn`` from whatever writes the fixture rows; a real file is
required for the read side to see the write side's committed data.

Synthetic fixtures only — fake site name/coords.
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

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_web_ui.py / tests/test_static_ingress.py).
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


def _make_site(conn: sqlite3.Connection, name: str = "Test Site") -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 47.0, 25.0, 900.0, 'UTC', 1)
            """,
            (name,),
        ).lastrowid
    )


# ---------------------------------------------------------------------------
# "/" and "/forecast" — 200 with the Forecast heading.
# ---------------------------------------------------------------------------


def test_root_returns_200_with_forecast_heading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/")
    assert response.status_code == 200
    assert "<h1>Forecast</h1>" in response.text


def test_forecast_route_returns_200_with_forecast_heading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/forecast")
    assert response.status_code == 200
    assert "<h1>Forecast</h1>" in response.text


def test_root_with_no_sites_shows_no_sites_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)  # no site inserted
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/")
    assert response.status_code == 200
    assert "No sites configured." in response.text


# ---------------------------------------------------------------------------
# Exact empty-state copy.
# ---------------------------------------------------------------------------


def test_empty_forecast_shows_exact_still_collecting_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn)  # site exists but has zero forecast_samples
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/forecast")
    assert response.status_code == 200
    assert "Still collecting forecasts — check back shortly." in response.text


# ---------------------------------------------------------------------------
# /forecast/tiles — 204 (no swap) vs a re-rendered fragment, keyed on
# whether the caller's fingerprint matches the current one.
# ---------------------------------------------------------------------------


def test_forecast_tiles_204_when_fingerprint_matches_fragment_when_it_differs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)  # zero samples -> fingerprint is always "0"
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        stale = client.get(f"/forecast/tiles?site={site_id}&fingerprint=")
        fresh = client.get(f"/forecast/tiles?site={site_id}&fingerprint=0")

    assert stale.status_code == 200
    assert 'id="forecast-tiles"' in stale.text
    assert fresh.status_code == 204
    assert fresh.text == ""


def test_forecast_tiles_unknown_site_yields_204(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/forecast/tiles?site=999999&fingerprint=")
    assert response.status_code == 204


# ---------------------------------------------------------------------------
# /api/forecast/hourly — 404 for an unknown site; day clamps into [0, 7]
# rather than erroring.
# ---------------------------------------------------------------------------


def test_api_forecast_hourly_404s_for_unknown_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/api/forecast/hourly?site=999999")
    assert response.status_code == 404


def test_api_forecast_hourly_day_clamps_above_and_below_range(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        above = client.get(f"/api/forecast/hourly?site={site_id}&day=99")
        below = client.get(f"/api/forecast/hourly?site={site_id}&day=-5")

    assert above.status_code == 200
    assert above.json()["day"] == 7
    assert below.status_code == 200
    assert below.json()["day"] == 0


def test_api_forecast_hourly_in_range_day_passes_through_unclamped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Paired positive for the clamp test above: an in-range day is NOT
    # coerced to an endpoint, proving the clamp is a min/max, not an
    # unconditional override.
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/api/forecast/hourly?site={site_id}&day=3")
    assert response.status_code == 200
    assert response.json()["day"] == 3


# ---------------------------------------------------------------------------
# /forecast/day — day clamps the SAME way, reflected in the embedded chart
# data-src URL the client-side JS reads.
# ---------------------------------------------------------------------------


def test_forecast_day_clamps_and_embeds_clamped_day_in_chart_src(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast/day?site={site_id}&day=99")
    assert response.status_code == 200
    # Jinja autoescapes the literal `&` in the query string to `&amp;`.
    assert f"/api/forecast/hourly?site={site_id}&amp;day=7" in response.text
    assert "day=99" not in response.text
