# pyright: strict
"""The Home Assistant Core-API proxy client (Supervisor add-on form).

Two calls, both through the in-cluster proxy with the ``SUPERVISOR_TOKEN``
bearer:

* ``POST http://supervisor/core/api/services/homeassistant/update_entity`` with
  the JSON body ``{"entity_id": "<station.update_entity>"}`` — drives the sibling
  Weather.com REST sensor to refresh.
* ``GET http://supervisor/core/api/states`` — reads every entity's state +
  timestamps, projected into `EntityState` for the health/freshness evaluation.

The ``/core`` segment is **required** (the Supervisor proxy form); the direct-Core
``/api/...`` form is wrong for an add-on and is never produced here.

API failure classification (the py-ddns terminal/transient taxonomy):

* **Terminal** (config/token fault, will not self-heal): ``401``/``403`` on any
  call; a ``4xx`` (≠``429``) on ``update_entity`` (especially ``404``/``422``); a
  ``4xx`` (≠``429``) on ``GET /states`` (wrong proxy path, revoked token, or a
  contract break). Raised as `TerminalError`.
* **Transient** (retryable): transport/connection, timeout, ``5xx``, and
  ``429`` — on **every** call, ``update_entity`` included. The ``429``-transient
  rule takes **precedence** over the ``4xx``-on-``update_entity`` terminal rule
  (the ``exc.status != 429`` guard runs first), so a rate-limited
  ``update_entity`` POST takes the transient path, never the terminal hold. A
  malformed/non-JSON ``/states`` body is also transient. Raised as
  `TransientError`.
"""

from __future__ import annotations

import json
from typing import cast

from .errors import TerminalError, TransientError
from .httpclient import HttpClient, HttpError
from .models import EntityState

BASE_URL = "http://supervisor/core/api"
UPDATE_ENTITY_URL = f"{BASE_URL}/services/homeassistant/update_entity"
STATES_URL = f"{BASE_URL}/states"


def _classify(exc: HttpError, *, is_update_entity: bool) -> Exception:
    """Map an `HttpError` to a `TerminalError` / `TransientError`.

    The ``429`` guard is checked **first** so a ``429`` is transient on every
    call (precedence over the ``4xx``-on-``update_entity`` terminal rule). A
    transport failure (``status is None``) is transient. ``401``/``403`` is
    terminal everywhere; any other ``4xx`` is terminal on both ``update_entity``
    and ``/states`` (a wrong proxy path / revoked token / contract break).
    ``5xx`` is transient.
    """
    status = exc.status
    if status is None:
        return TransientError(str(exc))
    if status == 429:
        return TransientError(str(exc))
    if status in (401, 403):
        return TerminalError(str(exc))
    if 400 <= status < 500:
        # A non-429 4xx is terminal on both calls: on update_entity it is a
        # misconfigured target (404/422); on /states it is a wrong proxy path or
        # revoked token. `is_update_entity` is kept for an explicit, auditable
        # message rather than to change the outcome.
        target = (
            "update_entity target" if is_update_entity else "GET /states path/token"
        )
        return TerminalError(f"{target}: {exc}")
    # 5xx and anything else server-side: retryable.
    return TransientError(str(exc))


def _parse_timestamp(value: object) -> str | None:
    """Project a raw ``last_*`` field to a non-empty string, else ``None``.

    A present-but-``null`` (JSON ``null`` → Python ``None``) or empty/whitespace
    string is normalized to ``None`` so the freshness check routes it to the
    degrade-safe fallback rather than treating it as a real signal. A non-string,
    non-null value (unexpected shape) is also ``None``.
    """
    if not isinstance(value, str):
        return None
    return value if value.strip() != "" else None


class HaApiClient:
    """The Home Assistant Core-API proxy client.

    `token` is the ``SUPERVISOR_TOKEN`` bearer; `timeout_seconds` is the per-call
    timeout (the configured ``request_timeout_seconds``, threaded through to the
    HTTP seam so the configured value reaches the wire rather than a hard-coded
    default).
    """

    def __init__(self, http: HttpClient, token: str, timeout_seconds: float) -> None:
        self._http = http
        self._token = token
        self._timeout = timeout_seconds

    def _headers(self, *, with_body: bool) -> dict[str, str]:
        """Bearer auth always; ``Content-Type`` only when a body is sent.

        Mirrors the py-ddns HTTP seam, which sets ``Content-Type`` only on a
        body-bearing request.
        """
        headers = {"Authorization": f"Bearer {self._token}"}
        if with_body:
            headers["Content-Type"] = "application/json"
        return headers

    def update_entity(self, entity_id: str) -> None:
        """POST ``update_entity`` for one representative sensor.

        Raises `TerminalError` / `TransientError` per the classification above
        (``429`` transient takes precedence over the ``4xx`` terminal rule).
        """
        body = json.dumps({"entity_id": entity_id}).encode("utf-8")
        try:
            self._http.request(
                "POST",
                UPDATE_ENTITY_URL,
                headers=self._headers(with_body=True),
                data=body,
                timeout=self._timeout,
            )
        except HttpError as exc:
            raise _classify(exc, is_update_entity=True) from None

    def get_states(self) -> list[EntityState]:
        """GET ``/states`` and project each entity into an `EntityState`.

        A non-2xx is classified (a non-429 4xx is terminal). A 2xx body that is
        non-JSON or not a JSON array (schema-invalid) is a **transient** poll
        failure — never silently healthy.
        """
        try:
            resp = self._http.request(
                "GET",
                STATES_URL,
                headers=self._headers(with_body=False),
                data=None,
                timeout=self._timeout,
            )
        except HttpError as exc:
            raise _classify(exc, is_update_entity=False) from None

        try:
            parsed = resp.json()
        except json.JSONDecodeError as exc:
            raise TransientError(f"GET /states: non-JSON body: {exc}") from None
        if not isinstance(parsed, list):
            raise TransientError(
                "GET /states: schema-invalid body (expected a JSON array)"
            )
        entries = cast(list[object], parsed)

        states: list[EntityState] = []
        for entry in entries:
            if not isinstance(entry, dict):
                # A non-object array member is a schema break, not silently healthy.
                raise TransientError(
                    "GET /states: schema-invalid entity (not an object)"
                )
            obj = cast(dict[str, object], entry)
            entity_id = obj.get("entity_id")
            state = obj.get("state")
            if not isinstance(entity_id, str) or not isinstance(state, str):
                raise TransientError(
                    "GET /states: schema-invalid entity "
                    "(entity_id/state missing or not strings)"
                )
            states.append(
                EntityState(
                    entity_id=entity_id,
                    state=state,
                    last_reported=_parse_timestamp(obj.get("last_reported")),
                    last_updated=_parse_timestamp(obj.get("last_updated")),
                    last_changed=_parse_timestamp(obj.get("last_changed")),
                )
            )
        return states
