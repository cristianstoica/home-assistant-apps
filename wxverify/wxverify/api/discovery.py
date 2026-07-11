"""Home Assistant Supervisor add-on discovery publication.

At startup the add-on POSTs a discovery message to the Supervisor so the
companion HA integration can be autodiscovered instead of hand-typing the
host/port. Outside HA (no ``SUPERVISOR_TOKEN``) this is a silent no-op, and
any failure is non-fatal: the service must start regardless.
"""

from __future__ import annotations

import logging
import os
import socket
from typing import cast

import httpx

logger = logging.getLogger(__name__)

_SUPERVISOR_DISCOVERY_URL = "http://supervisor/discovery"
_SERVICE_NAME = "wxverify"


async def publish_discovery(port: int) -> None:
    """Announce this add-on to the Supervisor discovery endpoint.

    ``port`` is the port uvicorn binds. Host is the container hostname, which
    the Supervisor sets to the add-on's internal DNS name. Skips silently when
    ``SUPERVISOR_TOKEN`` is absent (standalone/dev/test); logs a single warning
    and returns on any HTTP failure or non-200 response.
    """
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        logger.debug("SUPERVISOR_TOKEN absent; skipping add-on discovery")
        return
    host = socket.gethostname()
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                _SUPERVISOR_DISCOVERY_URL,
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "service": _SERVICE_NAME,
                    "config": {"host": host, "port": port},
                },
                timeout=httpx.Timeout(5.0, connect=5.0),
            )
    except httpx.HTTPError as exc:
        logger.warning("add-on discovery request failed: %s", exc)
        return
    if response.status_code != 200:
        logger.warning(
            "add-on discovery returned unexpected status %s", response.status_code
        )
        return
    try:
        body = cast(object, response.json())
    except ValueError as exc:
        logger.warning("add-on discovery returned unparseable body: %s", exc)
        return
    result: object = None
    if isinstance(body, dict):
        result = cast(dict[str, object], body).get("result")
    if result == "ok":
        logger.info("published add-on discovery (host=%s port=%s)", host, port)
    else:
        logger.warning("add-on discovery returned unexpected result: %r", result)
