"""Validation-error finite-JSON rendering (0.16.7).

Before this fix, a JSON body containing a bare ``NaN`` / ``Infinity`` /
``-Infinity`` constant (or a JSON number that overflows to infinity, e.g.
``1e999``) was correctly rejected by pydantic and nothing was stored -- but
the resulting 422's ``detail`` echoes the offending value back as ``input``,
and ``JSONResponse`` renders with ``allow_nan=False``. Encoding a non-finite
float there raised, so the client got a plain-text 500 instead of the 422.
``_finite_json`` walks the
already-``jsonable_encoder``-ed error detail and turns every non-finite
float, at any nesting depth, into the string ``"NaN"``/``"Infinity"``/
``"-Infinity"`` before the response is built.

Every response body here is verified to parse as STRICT JSON (a bare
constant token is a hard error), by calling ``json.loads`` with a
``parse_constant`` callback that raises -- that is what actually
distinguishes "still non-finite floats leaking into the payload" from
"successfully stringified", where a plain ``response.json()`` call would
silently accept either.

An ordinary validation error (no non-finite input anywhere) is pinned
BYTE-FOR-BYTE, so this fix cannot be shown to have changed a normal
response's shape.

Two endpoint-level cases here rely on the SIBLING 0.16.7 fix too (#81's
``allow_inf_nan=False`` on the site schema fields, in
``tests/test_site_schema_finite_number.py``): ``create-bare-infinity`` (a bare
``Infinity`` on ``rain_threshold_mm``) and
``test_create_site_rejects_a_json_number_that_overflows_to_infinity`` (a
``1e999`` on ``forecast_lat``). Without #81 neither field rejects positive
infinity: ``forecast_lat`` has no constraint at all, and
``rain_threshold_mm``'s ``ge=0`` passes it (``inf >= 0`` is true). The site is
stored and its success reply fails to render, so these two cases only reach a
``finite_number`` error, and this fix's rendering of it, when BOTH fixes are
present.

Synthetic fixtures only: fabricated site/station identifiers and
40.0/-105.0 coordinates.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.api.errors import register_error_handlers
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
    return {
        "Origin": "http://testserver",
        "X-CSRF-Token": token,
        "Content-Type": "application/json",
    }


def _valid_site_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "name": "Constant Town",
        "forecast_lat": 40.0,
        "forecast_lon": -105.0,
        "elevation_m": 900.0,
        "timezone": "UTC",
        "rain_threshold_mm": 0.2,
    }
    payload.update(overrides)
    return payload


def _strict_json_loads(raw: bytes) -> Any:
    """Parse ``raw`` as STRICT JSON: a bare NaN/Infinity/-Infinity token
    raises instead of silently round-tripping, unlike ``json.loads``'s
    (and ``response.json()``'s) default, permissive behaviour.
    """

    def _reject_constant(token: str) -> float:
        raise AssertionError(f"non-finite constant leaked into the response: {token}")

    return json.loads(raw, parse_constant=_reject_constant)


def _site_count() -> int:
    return int(
        get_db().read_sync(
            lambda conn: conn.execute("SELECT COUNT(*) AS n FROM sites").fetchone()["n"]
        )
    )


def _seed_station(site_id: int) -> int:
    """Insert a station directly (mirrors tests/test_m1_m5.py) -- the real
    POST /stations route calls out to weather.com/Open-Meteo, which this
    fix has nothing to do with.
    """

    def _write(conn: sqlite3.Connection) -> int:
        return int(
            conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m)
                VALUES (?, 'A', 40.0, -105.0, 900.0)
                """,
                (site_id,),
            ).lastrowid
        )

    return get_db().write_sync(_write)


# ---------------------------------------------------------------------------
# 1. Bare NaN / Infinity / -Infinity on POST /api/sites and PUT
#    /api/sites/{id} -- strict-JSON 422, no row added/changed.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        pytest.param("NaN", "NaN", id="create-bare-nan"),
        pytest.param("Infinity", "Infinity", id="create-bare-infinity"),
        pytest.param("-Infinity", "-Infinity", id="create-bare-neg-infinity"),
    ],
)
def test_create_site_bare_constant_yields_strict_json_422(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    token: str,
    expected: str,
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    body_text = (
        '{"name": "Constant Town", "forecast_lat": 40.0, "forecast_lon": -105.0, '
        f'"elevation_m": 900.0, "timezone": "UTC", "rain_threshold_mm": {token}}}'
    )
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        response = client.post("/api/sites", content=body_text, headers=headers)
        assert response.status_code == 422
        assert response.headers["content-type"].startswith("application/json")
        body = _strict_json_loads(response.content)
        errors = body["detail"]
        match = next(e for e in errors if e["loc"] == ["body", "rain_threshold_mm"])
        assert match["input"] == expected
        count = _site_count()
    assert count == 0


def test_update_site_bare_nan_yields_strict_json_422_and_leaves_threshold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        create_response = client.post(
            "/api/sites",
            json=_valid_site_payload(rain_threshold_mm=0.4),
            headers=headers,
        )
        assert create_response.status_code == 200
        site_id = create_response.json()["id"]

        response = client.put(
            f"/api/sites/{site_id}",
            content='{"rain_threshold_mm": NaN}',
            headers=headers,
        )
        assert response.status_code == 422
        assert response.headers["content-type"].startswith("application/json")
        body = _strict_json_loads(response.content)
        match = next(
            e for e in body["detail"] if e["loc"] == ["body", "rain_threshold_mm"]
        )
        assert match["input"] == "NaN"

        get_response = client.get(f"/api/sites/{site_id}")
    assert get_response.status_code == 200
    assert get_response.json()["rain_threshold_mm"] == pytest.approx(0.4)


# ---------------------------------------------------------------------------
# 2. A standard JSON number that overflows to infinity while parsing
#    (``1e999`` -- a valid JSON number token, not the non-standard
#    ``Infinity`` keyword) needs BOTH 0.16.7 fixes: #81 rejects it
#    (``finite_number``), and this fix renders the resulting 422 without a
#    500 -- covering the "huge literal" input class distinctly from the
#    bare-constant forms above.
# ---------------------------------------------------------------------------


def test_create_site_rejects_a_json_number_that_overflows_to_infinity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    body_text = (
        '{"name": "Overflow Town", "forecast_lat": 1e999, '
        '"forecast_lon": -105.0, "elevation_m": 900.0, '
        '"timezone": "UTC", "rain_threshold_mm": 0.2}'
    )
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        response = client.post("/api/sites", content=body_text, headers=headers)
        assert response.status_code == 422
        assert response.headers["content-type"].startswith("application/json")
        body = _strict_json_loads(response.content)
        errors = body["detail"]
        match = next(e for e in errors if e["loc"] == ["body", "forecast_lat"])
        assert match["type"] == "finite_number"
        assert match["input"] == "Infinity"
        count = _site_count()
    assert count == 0


# ---------------------------------------------------------------------------
# 3. A `missing`-field error echoes the WHOLE body as `input` -- a nested
#    non-finite value must be stringified in place, keeping every other key
#    and value intact.
# ---------------------------------------------------------------------------


def test_missing_field_error_echoes_whole_body_with_nested_constant_stringified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    body_text = (
        '{"forecast_lat": NaN, "forecast_lon": -105.0, "elevation_m": 900.0, '
        '"timezone": "UTC", "rain_threshold_mm": 0.2}'
    )
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        response = client.post("/api/sites", content=body_text, headers=headers)
        assert response.status_code == 422
        assert response.headers["content-type"].startswith("application/json")
        body = _strict_json_loads(response.content)
        count = _site_count()
    assert count == 0
    missing = next(
        e
        for e in body["detail"]
        if e["type"] == "missing" and e["loc"] == ["body", "name"]
    )
    assert missing["input"] == {
        "forecast_lat": "NaN",
        "forecast_lon": -105.0,
        "elevation_m": 900.0,
        "timezone": "UTC",
        "rain_threshold_mm": 0.2,
    }


# ---------------------------------------------------------------------------
# 4. `extra_forbidden` echoes the extra value itself.
# ---------------------------------------------------------------------------


def test_extra_forbidden_error_echoes_constant_extra_value_as_string(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        create_response = client.post(
            "/api/sites", json=_valid_site_payload(), headers=headers
        )
        assert create_response.status_code == 200
        site_id = create_response.json()["id"]

        response = client.put(
            f"/api/sites/{site_id}",
            content='{"bogus_field": NaN}',
            headers=headers,
        )
        assert response.status_code == 422
        assert response.headers["content-type"].startswith("application/json")
        body = _strict_json_loads(response.content)
    match = next(e for e in body["detail"] if e["type"] == "extra_forbidden")
    assert match["loc"] == ["body", "bogus_field"]
    assert match["input"] == "NaN"


# ---------------------------------------------------------------------------
# 5. A non-float route: `{"enabled": NaN}` on the station PUT, whose only
#    field is a bool -- proves `_finite_json` reaches a non-finite value
#    regardless of the failing field's own declared type.
# ---------------------------------------------------------------------------


def test_non_float_route_rejects_nan_body_with_strict_json_422(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        create_response = client.post(
            "/api/sites", json=_valid_site_payload(), headers=headers
        )
        assert create_response.status_code == 200
        site_id = create_response.json()["id"]
        station_id = _seed_station(site_id)

        response = client.put(
            f"/api/sites/{site_id}/stations/{station_id}",
            content='{"enabled": NaN}',
            headers=headers,
        )
        assert response.status_code == 422
        assert response.headers["content-type"].startswith("application/json")
        body = _strict_json_loads(response.content)
    match = next(e for e in body["detail"] if e["loc"] == ["body", "enabled"])
    assert match["input"] == "NaN"


# ---------------------------------------------------------------------------
# 6. `ctx` coverage: no current route puts a non-finite value in `ctx`, so
#    this drives a minimal FastAPI app through the real
#    `register_error_handlers`, with a route that raises a
#    `RequestValidationError` carrying non-finite values in BOTH `input`
#    and `ctx`.
# ---------------------------------------------------------------------------


def test_ctx_and_input_non_finite_values_are_stringified() -> None:
    app = FastAPI()
    register_error_handlers(app)

    @app.get("/boom")
    async def boom() -> None:
        raise RequestValidationError(
            [
                {
                    "type": "greater_than_equal",
                    "loc": ("body", "threshold"),
                    "msg": "Input should be greater than or equal to 0",
                    "input": float("nan"),
                    "ctx": {"ge": float("inf")},
                }
            ]
        )

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/boom")
    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/json")
    body = _strict_json_loads(response.content)
    error = body["detail"][0]
    assert error["loc"] == ["body", "threshold"]
    assert error["input"] == "NaN"
    assert error["ctx"] == {"ge": "Infinity"}


# ---------------------------------------------------------------------------
# 7. Ordinary error, pinned byte-for-byte -- this fix must not change a
#    normal response's shape at all.
# ---------------------------------------------------------------------------


def test_ordinary_validation_error_is_byte_for_byte_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        response = client.post(
            "/api/sites",
            json=_valid_site_payload(rain_threshold_mm=-0.1),
            headers=headers,
        )
    assert response.status_code == 422
    assert response.content == (
        b'{"error":"validation failed","detail":[{"type":"greater_than_equal",'
        b'"loc":["body","rain_threshold_mm"],"msg":"Input should be greater '
        b'than or equal to 0","input":-0.1,"ctx":{"ge":0.0}}]}'
    )
