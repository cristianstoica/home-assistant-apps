"""Single HTTP mutation guard."""

from __future__ import annotations

from urllib.parse import urlparse

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response

from wxverify.api.csrf import validate_csrf

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

# Raw-body upload routes allowed to POST application/octet-stream. Everything
# else about the mutation contract (same-origin check, CSRF double-submit)
# still applies to these paths; multipart stays rejected everywhere.
_OCTET_STREAM_PATHS = frozenset({"/api/import/db"})


class MutationGuard(BaseHTTPMiddleware):
    def __init__(self, app: object, *, standalone_origin: str | None = None) -> None:
        super().__init__(app)  # type: ignore[arg-type]
        self.standalone_origin = standalone_origin

    async def dispatch(self, request: Request, call_next: object) -> Response:
        if request.method in SAFE_METHODS:
            return await call_next(request)  # type: ignore[misc]
        origin = request.headers.get("origin") or _origin_of(
            request.headers.get("referer")
        )
        expected = _expected_origin(request, self.standalone_origin)
        if origin is not None and origin != expected:
            return JSONResponse(
                {"error": "cross-origin mutation rejected"}, status_code=403
            )
        content_type = request.headers.get("content-type", "").split(";")[0].strip()
        has_body = int(request.headers.get("content-length", "0") or "0") > 0
        allowed = {"application/json"}
        if _bare_path(request) in _OCTET_STREAM_PATHS:
            allowed.add("application/octet-stream")
        if has_body and content_type not in allowed:
            return JSONResponse({"error": "disallowed content-type"}, status_code=415)
        if not validate_csrf(request):
            return JSONResponse({"error": "bad csrf token"}, status_code=403)
        return await call_next(request)  # type: ignore[misc]


def _bare_path(request: Request) -> str:
    """Route path with the ingress ``root_path`` prefix removed.

    ``IngressPathMiddleware`` runs outside this guard and PREPENDS the
    ingress prefix to ``scope["path"]``, so ``request.url.path`` arrives
    prefixed under ingress. Stripping the root path (the ``active_page``
    idiom, web/render.py) yields the bare route the allowlist compares
    against — the same path standalone and under ingress alike.
    """
    path = request.url.path
    root = str(request.scope.get("root_path", "") or "").rstrip("/")
    if root and path.startswith(root):
        path = path[len(root) :] or "/"
    return path


def _origin_of(referer: str | None) -> str | None:
    if not referer:
        return None
    parsed = urlparse(referer)
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"


def _expected_origin(request: Request, standalone_origin: str | None) -> str:
    if standalone_origin:
        return standalone_origin.rstrip("/")
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host")
    if host is None:
        return str(request.base_url).rstrip("/")
    return f"{proto}://{host}"
