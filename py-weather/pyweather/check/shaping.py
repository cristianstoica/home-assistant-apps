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

from .. import config, fixtures
from ..errors import TransientError
from ..haapi import STATES_URL, UPDATE_ENTITY_URL, HaApiClient
from ..httpclient import HttpError
from ..models import Station
from ..supervisor import BASE_URL as SUPERVISOR_OPTIONS_URL
from ..supervisor import SupervisorSelfClient, to_options_dict
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


def check_supervisor_request_shaping() -> bool:
    """Assert the real `SupervisorSelfClient.set_options` POST wire shaping + failure mapping.

    Drives the REAL client against a recording `FakeHttp` (mirroring
    `check_request_shaping` for `HaApiClient`), asserting on the captured
    `HttpCall`: method POST; exact url `http://supervisor/addons/self/options`;
    `Authorization: Bearer <token>`; `Content-Type: application/json`; decoded
    body equals `{"options": <full blob>}`; and `timeout` equals the configured
    `request_timeout_seconds` (a value distinct from any plausible default, so the
    assertion is discriminating). Then drives the REAL client against a `FakeHttp`
    that RAISES an `HttpError` (the seam's non-2xx / transport failure form) and
    asserts `set_options` SURFACES it as a `TransientError` (not a bare
    `HttpError`, not swallowed) carrying THIS endpoint's Supervisor `BASE_URL`
    `http://supervisor/addons/self/options` and NOT the haapi `/states` Core base
    — pinning the deliberate non-reuse of `haapi._classify`. This is additive to
    the faked-client orchestration assertion (which never exercises the real
    client's wire shape or its error mapping).
    """
    token = fixtures.EXAMPLE_TOKEN
    cfg = config.validate(fixtures.default_options(stations=[]))
    stations = [
        Station(
            key="istation01",
            update_entity="sensor.wu_temp_istation01",
            expected_sensors=4,
        )
    ]
    blob = to_options_dict(cfg, stations)

    http = FakeHttp(ok_response(""))
    client = SupervisorSelfClient(http, token, _TIMEOUT)
    client.set_options(blob)
    call = http.calls[0]
    body_obj = json.loads(call.body) if call.body is not None else None

    # --- failure mapping: a non-2xx / transport HttpError from the seam is -----
    # surfaced by set_options as a TransientError carrying THIS module's
    # Supervisor BASE_URL (not haapi's /states Core base — the deliberate
    # non-reuse of haapi._classify). FakeHttp raises a queued HttpError when its
    # turn comes (fakes.py:46-74), so a single scripted HttpError(status=503)
    # exercises the real client's `except HttpError: raise TransientError(...)`.
    http_fail = FakeHttp(HttpError("http 503", status=503))
    client_fail = SupervisorSelfClient(http_fail, token, _TIMEOUT)
    mapped: Exception | None = None
    try:
        client_fail.set_options(blob)
    except TransientError as exc:
        mapped = exc
    except HttpError as exc:  # the bug this pins: the raw HttpError leaked through
        mapped = exc

    checks: list[tuple[str, bool]] = [
        ("set_options method is POST", call.method == "POST"),
        (
            "set_options url is the exact addons/self/options form",
            call.url == SUPERVISOR_OPTIONS_URL
            and call.url == "http://supervisor/addons/self/options",
        ),
        (
            "set_options Authorization is Bearer <token>",
            call.headers.get("Authorization") == f"Bearer {token}",
        ),
        (
            "set_options Content-Type is application/json",
            call.headers.get("Content-Type") == "application/json",
        ),
        (
            "set_options body wraps the full blob under 'options'",
            body_obj == {"options": blob},
        ),
        (
            "set_options timeout equals configured request_timeout_seconds",
            call.timeout == _TIMEOUT,
        ),
        (
            "set_options maps a seam HttpError to TransientError (not swallowed, not a bare HttpError)",
            isinstance(mapped, TransientError),
        ),
        (
            "set_options error carries the Supervisor addons/self/options URL, not the /states Core base",
            mapped is not None
            and "http://supervisor/addons/self/options" in str(mapped)
            and "/core/api/states" not in str(mapped),
        ),
    ]
    return report("SUPERVISOR-SHAPING", "supervisor-shaping", checks)
