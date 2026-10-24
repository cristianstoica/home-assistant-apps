"""Site schema finite-number validation (0.16.7, #81).

Before this fix, ``SiteCreate.forecast_lat``/``forecast_lon``/``elevation_m``
accepted any float pydantic could coerce, including a non-finite one sent as
a JSON string (``"inf"``/``"-inf"``/``"nan"``; ``StrictModel`` sets
``extra="forbid"`` but not ``strict=True``, so the string is coerced). An
infinity was PERSISTED (the REAL NOT NULL columns store it) and the JSON
reply then failed to render, a 500; a ``nan`` was bound by sqlite3 as NULL,
so the insert failed its NOT NULL constraint, also a 500, with nothing
stored. The two ``rain_threshold_mm`` fields already refused ``nan`` and
``-inf`` through ``ge=0`` (``greater_than_equal``) but persisted ``+inf``
(``inf >= 0``). Each field now carries ``allow_inf_nan=False``, which
pydantic checks before ``ge=0`` (confirmed empirically: a non-finite
threshold fails with ``finite_number``, never ``greater_than_equal``, even
for ``-inf``) -- so every case below is reachable without ge=0 competing
for the same input.

A standard JSON number that overflows to infinity while parsing (``1e999``)
is also rejected by this fix, but that case is covered in
``tests/test_validation_error_finite_json.py`` instead: rendering its 422
error response requires the OTHER 0.16.7 fix too (the raw ``input`` pydantic
reports is itself a non-finite float, straight from ``json.loads``), so it
belongs with the fix that makes the response renderable, not here.

Synthetic fixtures only: a fabricated site name and 40.0/-105.0 coordinates.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db, init_db

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_rain_threshold_display.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> None:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    init_db(str(db_path))


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


def _valid_site_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "name": "Finite Field Town",
        "forecast_lat": 40.0,
        "forecast_lon": -105.0,
        "elevation_m": 900.0,
        "timezone": "UTC",
        "rain_threshold_mm": 0.2,
    }
    payload.update(overrides)
    return payload


def _site_count() -> int:
    return int(
        get_db().read_sync(
            lambda conn: conn.execute("SELECT COUNT(*) AS n FROM sites").fetchone()["n"]
        )
    )


# ---------------------------------------------------------------------------
# 1. POST /api/sites rejects a non-finite value on each of the four fields,
#    and nothing is added.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("forecast_lat", "inf", id="forecast_lat-string-inf"),
        pytest.param("forecast_lat", "-inf", id="forecast_lat-string-neg-inf"),
        pytest.param("forecast_lon", "nan", id="forecast_lon-string-nan"),
        pytest.param("elevation_m", "nan", id="elevation_m-string-nan"),
        pytest.param("rain_threshold_mm", "nan", id="rain_threshold_mm-string-nan"),
        pytest.param(
            "rain_threshold_mm", "-inf", id="rain_threshold_mm-string-neg-inf"
        ),
        pytest.param("rain_threshold_mm", "inf", id="rain_threshold_mm-string-inf"),
    ],
)
def test_create_site_rejects_non_finite_string_field(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: str,
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        response = client.post(
            "/api/sites",
            json=_valid_site_payload(**{field: value}),
            headers=headers,
        )
        assert response.status_code == 422
        errors = response.json()["detail"]
        assert any(
            err["type"] == "finite_number" and err["loc"] == ["body", field]
            for err in errors
        ), errors
        count = _site_count()
    assert count == 0


# ---------------------------------------------------------------------------
# 2. PUT /api/sites/{id} rejects a non-finite rain_threshold_mm and leaves
#    the stored value unchanged.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("nan", id="rain_threshold_mm-nan-existing-behavior-control"),
        pytest.param("inf", id="rain_threshold_mm-inf-regression-case"),
    ],
)
def test_update_site_rejects_non_finite_threshold_and_leaves_it_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """``"nan"`` was already refused by ``ge=0`` (``greater_than_equal``)
    before this fix -- the existing-behaviour control. ``"inf"`` passes
    ``ge=0`` (``inf >= 0``) and is the regression case this fix closes; on
    the pre-fix code the PUT stored the infinity and both the PUT reply and
    a later GET failed to render, 500s -- ``raise_server_exceptions=False``
    lets those reach the assertions below instead of raising.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        headers = _csrf_headers(client)
        create_response = client.post(
            "/api/sites",
            json=_valid_site_payload(rain_threshold_mm=0.3),
            headers=headers,
        )
        assert create_response.status_code == 200
        site_id = create_response.json()["id"]

        response = client.put(
            f"/api/sites/{site_id}",
            json={"rain_threshold_mm": value},
            headers=headers,
        )
        assert response.status_code == 422
        errors = response.json()["detail"]
        assert any(
            err["type"] == "finite_number"
            and err["loc"] == ["body", "rain_threshold_mm"]
            for err in errors
        ), errors

        get_response = client.get(f"/api/sites/{site_id}")
    assert get_response.status_code == 200
    assert get_response.json()["rain_threshold_mm"] == pytest.approx(0.3)


# ---------------------------------------------------------------------------
# 3. GET /api/sites still works after a rejected non-finite payload. On the
#    pre-fix code an ``"inf"`` latitude was stored, so the POST returned 500
#    and every later GET /api/sites returned 500 too;
#    ``raise_server_exceptions=False`` lets those 500s reach the assertions.
# ---------------------------------------------------------------------------


def test_list_sites_still_succeeds_after_a_rejected_non_finite_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        headers = _csrf_headers(client)
        post = client.post(
            "/api/sites",
            json=_valid_site_payload(forecast_lat="inf"),
            headers=headers,
        )
        count = _site_count()
        response = client.get("/api/sites")
    assert post.status_code == 422
    assert count == 0
    assert response.status_code == 200
    assert response.json() == []


# ---------------------------------------------------------------------------
# 4. Positive controls: unaffected by this diff -- ordinary finite values,
#    including edge-adjacent ones (threshold 0, a negative elevation), must
#    still be accepted both before and after the fix.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("rain_threshold_mm", 0, id="CONTROL-threshold-zero"),
        pytest.param("rain_threshold_mm", 0.25, id="CONTROL-threshold-quarter"),
        pytest.param("elevation_m", -10.0, id="CONTROL-negative-elevation"),
        pytest.param("forecast_lat", 40.0, id="CONTROL-ordinary-lat"),
        pytest.param("forecast_lon", -105.0, id="CONTROL-ordinary-lon"),
    ],
)
def test_create_site_accepts_ordinary_finite_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: float,
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        response = client.post(
            "/api/sites",
            json=_valid_site_payload(**{field: value}),
            headers=headers,
        )
    assert response.status_code == 200
    assert response.json()[field] == pytest.approx(value)
