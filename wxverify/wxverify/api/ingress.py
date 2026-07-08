"""Home Assistant Ingress root-path middleware."""

from __future__ import annotations

from starlette.types import ASGIApp, Receive, Scope, Send

from wxverify import config

_INGRESS_PATH_HEADER = b"x-ingress-path"


class IngressPathMiddleware:
    """Set ``scope["root_path"]`` from the Supervisor's ``X-Ingress-Path`` header.

    Home Assistant's ingress proxy strips the ``/api/hassio_ingress/<token>``
    prefix before forwarding and advertises it in ``X-Ingress-Path``. The
    header is honored only when the request originates from the Supervisor
    ingress proxy (``config.SUPERVISOR_INGRESS_CLIENT``); for any other client
    the static ``--root-path`` value (if any) continues to apply. When both
    are present the header wins: the live proxy value beats the boot-time
    snapshot.
    """

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            ingress_path = _trusted_ingress_path(scope)
            if ingress_path is not None:
                scope["root_path"] = ingress_path.rstrip("/")
        await self._app(scope, receive, send)


def _trusted_ingress_path(scope: Scope) -> str | None:
    client: tuple[str, int] | None = scope.get("client")
    if client is None or client[0] != config.SUPERVISOR_INGRESS_CLIENT:
        return None
    headers: list[tuple[bytes, bytes]] = scope.get("headers", [])
    for name, value in headers:
        if name == _INGRESS_PATH_HEADER:
            return value.decode("latin-1")
    return None
