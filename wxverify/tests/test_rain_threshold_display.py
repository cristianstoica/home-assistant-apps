"""Rain-threshold display precision (0.16.7).

Covers the Sites card's ``snapshot.rain_threshold_mm if ... is not none
else '—'`` expression, which prints the stored value verbatim instead of
rounding to one decimal, and the create-site form's threshold input
accepting any step, not just tenths.

Synthetic fixtures only: a fabricated site name/coords, UTC timezone.

The route-level case carries CONTROL or KILL in its id: a CONTROL renders
identically on the pre-fix code (proving the fixture does not manufacture a
spurious difference); a KILL is asserted to diverge from the pre-fix
renderer.
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.switch import ensure_site_generation

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_web_ui.py and tests/test_phase8_verification_v16.py).
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


def _make_site(conn: sqlite3.Connection, *, rain_threshold_mm: float = 0.2) -> int:
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled,
                 rain_threshold_mm)
            VALUES ('Rain Threshold Town', 40.0, -105.0, 900.0, 'UTC', 1, ?)
            """,
            (rain_threshold_mm,),
        ).lastrowid
    )
    ensure_site_generation(conn, site_id)
    return site_id


# The Sites card's wrapping markup for the threshold row.
_DT_DD = re.compile(r"<dt>Rain threshold</dt><dd>([^<]*)</dd>")


def _card_text(html: str) -> str:
    match = _DT_DD.search(html)
    assert match is not None, "no 'Rain threshold' <dt>/<dd> row in the page"
    return match.group(1)


# ---------------------------------------------------------------------------
# 1. Sites card precision.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("stored", "expected_text"),
    [
        pytest.param(0.25, "0.25 mm", id="KILL-quarter-mm-precision"),
        pytest.param(0.2, "0.2 mm", id="CONTROL-two-tenths-unchanged"),
        pytest.param(1.0, "1.0 mm", id="CONTROL-one-point-oh-unchanged"),
    ],
)
def test_sites_card_renders_stored_threshold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stored: float,
    expected_text: str,
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, rain_threshold_mm=stored)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/sites")
    assert response.status_code == 200
    assert _card_text(response.text) == expected_text


# ---------------------------------------------------------------------------
# 3. Create-site form.
# ---------------------------------------------------------------------------


def test_create_site_form_input_is_step_any(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/sites")
    assert response.status_code == 200
    match = re.search(r'<input name="rain_threshold_mm"[^>]*>', response.text)
    assert match is not None, "rain_threshold_mm input not found"
    input_tag = match.group(0)
    assert 'step="any"' in input_tag  # KILL: pre-fix was step="0.1"
    assert 'min="0"' in input_tag  # unaffected by this diff; still true


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


def test_create_site_accepts_a_string_quarter_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CONTROL: unaffected by this diff -- `SiteCreate`/`StrictModel` (no
    `strict=True`) already coerced a JSON string field to float before this
    fix; kept as a paired positive beside the -0.1 rejection below.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        response = client.post(
            "/api/sites",
            json={
                "name": "String Threshold Site",
                "forecast_lat": 40.0,
                "forecast_lon": -105.0,
                "elevation_m": 900.0,
                "timezone": "UTC",
                "rain_threshold_mm": "0.25",
            },
            headers=headers,
        )
        assert response.status_code == 200
        body = response.json()
        assert body["rain_threshold_mm"] == 0.25
        row = get_db().read_sync(
            lambda conn: conn.execute(
                "SELECT rain_threshold_mm FROM sites WHERE id = ?", (body["id"],)
            ).fetchone()
        )
    assert row["rain_threshold_mm"] == 0.25


def test_create_site_rejects_a_negative_string_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CONTROL: unaffected by this diff -- `Field(ge=0)` already rejected a
    negative threshold before this fix; paired with the acceptance case
    above so the ge=0 gate itself is proven live, not merely assumed.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        response = client.post(
            "/api/sites",
            json={
                "name": "Negative Threshold Site",
                "forecast_lat": 40.0,
                "forecast_lon": -105.0,
                "elevation_m": 900.0,
                "timezone": "UTC",
                "rain_threshold_mm": "-0.1",
            },
            headers=headers,
        )
        assert response.status_code == 422
        count = get_db().read_sync(
            lambda conn: conn.execute(
                "SELECT COUNT(*) AS n FROM sites WHERE name = 'Negative Threshold Site'"
            ).fetchone()["n"]
        )
    assert count == 0
