# pyright: strict
"""HA Core-API request-shaping checks: update_entity POST and GET /states.

Drives the real `HaApiClient` against a recording `FakeHttp` and asserts the
captured calls' method / exact url / headers / body / timeout — proving the
Supervisor proxy form (``/core/api/...``), the bearer + Content-Type contract,
the exact JSON body, and that the **configured** timeout reaches the seam (not a
hard-coded default).
"""

from __future__ import annotations

import json

from .. import fixtures
from ..haapi import STATES_URL, UPDATE_ENTITY_URL, HaApiClient
from .fakes import FakeHttp, ok_response, states_response
from .report import report

# A configured timeout distinct from any plausible hard-coded default, so the
# "configured timeout reaches the seam" assertion is discriminating.
_TIMEOUT = 30.0


def check_request_shaping() -> bool:
    """Assert the update_entity POST and the GET /states request shaping.

    POST: ``method="POST"``, exact url ``.../core/api/services/homeassistant/
    update_entity``, ``Authorization: Bearer <token>``, ``Content-Type:
    application/json``, decoded body ``{"entity_id": "sensor.wu_temp_istation01"}``,
    and ``timeout == request_timeout_seconds``.

    GET: ``method="GET"``, exact url ``.../core/api/states``,
    ``Authorization: Bearer <token>``, **no** request body (``data is None`` →
    captured body ``None``), no ``Content-Type``, and ``timeout ==
    request_timeout_seconds`` — proving the bearer + configured timeout reach the
    shared seam on the GET path and the proxy form (``/core/api/states``) is used.
    """
    token = fixtures.EXAMPLE_TOKEN
    checks: list[tuple[str, bool]] = []

    # --- update_entity POST ---------------------------------------------------
    http_post = FakeHttp(ok_response(""))
    api_post = HaApiClient(http_post, token, _TIMEOUT)
    api_post.update_entity("sensor.wu_temp_istation01")
    post = http_post.calls[0]
    body_obj = json.loads(post.body) if post.body is not None else None
    checks += [
        ("update_entity method is POST", post.method == "POST"),
        (
            "update_entity url is the exact Supervisor proxy form",
            post.url == UPDATE_ENTITY_URL
            and post.url
            == "http://supervisor/core/api/services/homeassistant/update_entity",
        ),
        (
            "update_entity Authorization is Bearer <token>",
            post.headers.get("Authorization") == f"Bearer {token}",
        ),
        (
            "update_entity Content-Type is application/json (body present)",
            post.headers.get("Content-Type") == "application/json",
        ),
        (
            "update_entity body is exactly {'entity_id': 'sensor.wu_temp_istation01'}",
            body_obj == {"entity_id": "sensor.wu_temp_istation01"},
        ),
        (
            "update_entity timeout equals configured request_timeout_seconds",
            post.timeout == _TIMEOUT,
        ),
    ]

    # --- GET /states ----------------------------------------------------------
    http_get = FakeHttp(states_response(fixtures.station_states("istation01")))
    api_get = HaApiClient(http_get, token, _TIMEOUT)
    api_get.get_states()
    get = http_get.calls[0]
    checks += [
        ("states method is GET", get.method == "GET"),
        (
            "states url is the exact Supervisor proxy form (/core/api/states)",
            get.url == STATES_URL and get.url == "http://supervisor/core/api/states",
        ),
        (
            "states Authorization is Bearer <token>",
            get.headers.get("Authorization") == f"Bearer {token}",
        ),
        ("states carries no request body (data is None)", get.body is None),
        (
            "states sends no Content-Type (no body)",
            "Content-Type" not in get.headers,
        ),
        (
            "states timeout equals configured request_timeout_seconds",
            get.timeout == _TIMEOUT,
        ),
    ]
    return report("REQUEST-SHAPING", "shaping", checks)
