"""Regression oracles for wxverify 0.1.2 — static-404 under HA Ingress.

Bug: in 0.1.1 IngressPathMiddleware set scope["root_path"] but did NOT
re-prepend the prefix to scope["path"], so Starlette's StaticFiles router
saw path="/static/app.css" but root_path="/api/hassio_ingress/<token>" and
could not reconcile them → 404 on every static asset under Ingress.

Fix (0.1.2): when the request comes from the Supervisor ingress client
(172.30.32.2) and carries X-Ingress-Path, the middleware now also prepends
the prefix to scope["path"], guarded by an empty-prefix check and an
_already_applied idempotency check so proxy-side non-stripping does not
double-prepend.

ORACLE SUITE
  1. static-under-ingress     — the load-bearing regression test
  2. standalone static serve  — paired positive; direct/reverse-proxy path unchanged
  3. dashboard under ingress  — top-level HTML routes must survive the fix
  4. idempotency              — no double-prepend when prefix already present
  5. pass-through (3 cases)   — non-Supervisor clients leave scope unchanged
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
from wxverify.api.ingress import IngressPathMiddleware
from wxverify.db.connection import close_db, init_db

# ---------------------------------------------------------------------------
# Synthetic ingress token — RFC-5737 IP is used for non-Supervisor pass-through
# ---------------------------------------------------------------------------
_INGRESS_TOKEN = "abc123synthetic"
_INGRESS_PREFIX = f"/api/hassio_ingress/{_INGRESS_TOKEN}"
_SUPERVISOR_IP = "172.30.32.2"
_NON_SUPERVISOR_IP = "192.0.2.10"  # RFC-5737 documentation range


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


async def _idle_worker(db: object) -> None:
    """Drop-in run_worker shim that idles without touching the scheduler."""
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Return a fully-configured FastAPI app with idle worker and tmp DB."""
    _init_tmp_db(tmp_path)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


# ---------------------------------------------------------------------------
# Oracle 1 — static-under-ingress (the 0.1.1 regression)
#
# GET /static/app.css from the Supervisor ingress client with X-Ingress-Path
# must return 200 + text/css + non-empty body.
#
# In 0.1.1 this returned 404 because scope["path"] was never prepended.
# ---------------------------------------------------------------------------


def test_static_css_under_ingress_returns_200(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Oracle 1 (regression gate): static asset served 200 under HA Ingress."""
    app = _make_app(tmp_path, monkeypatch)
    with TestClient(
        app, client=(_SUPERVISOR_IP, 4321), follow_redirects=False
    ) as client:
        resp = client.get(
            "/static/app.css",
            headers={"X-Ingress-Path": _INGRESS_PREFIX},
        )
    assert resp.status_code == 200, (
        f"Expected 200 for /static/app.css under ingress; got {resp.status_code}. "
        "This is the 0.1.1 static-404 regression — IngressPathMiddleware must "
        "prepend the prefix to scope['path'] so StaticFiles resolves correctly."
    )
    content_type = resp.headers.get("content-type", "")
    assert "text/css" in content_type, (
        f"Expected text/css; got {content_type!r}"
    )
    assert len(resp.content) > 0, "app.css body must be non-empty"


# ---------------------------------------------------------------------------
# Oracle 2 — standalone static serve (paired positive for Oracle 1)
#
# GET /static/app.css with no ingress header from a non-Supervisor client
# must still return 200 — the fix must not break direct / reverse-proxy serving.
# ---------------------------------------------------------------------------


def test_static_css_standalone_returns_200(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Oracle 2: direct (no-ingress) static serving is unaffected by the fix."""
    app = _make_app(tmp_path, monkeypatch)
    # Non-Supervisor client, no X-Ingress-Path header — standalone mode.
    with TestClient(
        app, client=(_NON_SUPERVISOR_IP, 9000), follow_redirects=False
    ) as client:
        resp = client.get("/static/app.css")
    assert resp.status_code == 200, (
        f"Standalone static serve broken; got {resp.status_code}"
    )
    assert "text/css" in resp.headers.get("content-type", "")
    assert len(resp.content) > 0


# ---------------------------------------------------------------------------
# Oracle 3 — dashboard HTML under ingress
#
# GET /dashboard from the Supervisor ingress client must return 200.
# Top-level HTML routes must continue to work after the scope["path"] change.
# ---------------------------------------------------------------------------


def test_dashboard_under_ingress_returns_200(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Oracle 3: /dashboard HTML route is reachable under HA Ingress."""
    app = _make_app(tmp_path, monkeypatch)
    with TestClient(
        app, client=(_SUPERVISOR_IP, 4321), follow_redirects=False
    ) as client:
        resp = client.get(
            "/dashboard",
            headers={"X-Ingress-Path": _INGRESS_PREFIX},
        )
    assert resp.status_code == 200, (
        f"Expected 200 for /dashboard under ingress; got {resp.status_code}"
    )


# ---------------------------------------------------------------------------
# Oracle 4 — idempotency: no double-prepend
#
# If scope["root_path"] already equals the prefix AND scope["path"] already
# starts with the prefix, the middleware must pass the scope through unchanged.
# Implemented by driving a raw ASGI scope through IngressPathMiddleware with
# a spy downstream app that records the scope it receives.
# ---------------------------------------------------------------------------


def test_ingress_middleware_does_not_double_prepend() -> None:
    """Oracle 4: _already_applied guard prevents double-prepend."""
    prefix = _INGRESS_PREFIX
    original_path = f"{prefix}/static/app.css"

    received_scope: dict[str, Any] = {}

    async def _spy(scope: Any, receive: Any, send: Any) -> None:
        received_scope.update(scope)

    async def run() -> None:
        app = IngressPathMiddleware(_spy)
        scope: dict[str, Any] = {
            "type": "http",
            "method": "GET",
            "path": original_path,
            "query_string": b"",
            "headers": [(b"x-ingress-path", prefix.encode())],
            "client": (_SUPERVISOR_IP, 4321),
            "root_path": prefix,  # already set
        }

        async def receive() -> dict[str, Any]:  # noqa: RUF029 (async needed for protocol)
            return {"type": "http.request", "body": b""}

        async def send_fn(_event: dict[str, Any]) -> None:
            pass

        await app(scope, receive, send_fn)

    asyncio.run(run())

    downstream_path: str = received_scope.get("path", "")
    # The prefix must appear exactly once
    assert downstream_path.startswith(prefix), (
        f"path must start with prefix; got {downstream_path!r}"
    )
    remainder = downstream_path[len(prefix):]
    assert not remainder.startswith(prefix), (
        f"double-prepend detected: path={downstream_path!r} starts with prefix twice"
    )
    # root_path must equal the prefix (not doubled)
    assert received_scope.get("root_path") == prefix, (
        f"root_path must equal prefix; got {received_scope.get('root_path')!r}"
    )


# ---------------------------------------------------------------------------
# Supplementary: _already_applied unit test (white-box support for Oracle 4)
# ---------------------------------------------------------------------------


def test_already_applied_returns_true_when_prefix_present() -> None:
    """_already_applied correctly identifies an already-mutated scope."""
    from wxverify.api.ingress import _already_applied

    prefix = _INGRESS_PREFIX
    scope: dict[str, Any] = {
        "root_path": prefix,
        "path": f"{prefix}/static/app.css",
    }
    assert _already_applied(scope, prefix) is True


def test_already_applied_returns_false_when_path_not_prefixed() -> None:
    """_already_applied correctly identifies a scope that still needs mutation."""
    from wxverify.api.ingress import _already_applied

    prefix = _INGRESS_PREFIX
    scope: dict[str, Any] = {
        "root_path": "",
        "path": "/static/app.css",
    }
    assert _already_applied(scope, prefix) is False


# ---------------------------------------------------------------------------
# Oracle 5 — pass-through: non-Supervisor clients leave scope unchanged
#
# Three sub-cases, each driven through the raw middleware (no full app needed):
#   (a) RFC-5737 non-Supervisor client IP (192.0.2.10) with X-Ingress-Path header
#   (b) Supervisor client (172.30.32.2) with NO X-Ingress-Path header
#   (c) client is None
# In all three cases, downstream scope["path"] and scope["root_path"] must be
# identical to what was injected — no mutation, no exception.
# ---------------------------------------------------------------------------


async def _drive_middleware(scope: dict[str, Any]) -> dict[str, Any]:
    """Drive *scope* through IngressPathMiddleware; return the scope seen downstream."""
    received: dict[str, Any] = {}

    async def _spy(s: Any, _recv: Any, _send: Any) -> None:
        received.update(s)

    app = IngressPathMiddleware(_spy)

    async def receive() -> dict[str, Any]:  # noqa: RUF029
        return {"type": "http.request", "body": b""}

    async def send_fn(_event: dict[str, Any]) -> None:
        pass

    await app(scope, receive, send_fn)
    return received


def _base_scope(
    *,
    client: tuple[str, int] | None,
    headers: list[tuple[bytes, bytes]] | None = None,
) -> dict[str, Any]:
    return {
        "type": "http",
        "method": "GET",
        "path": "/static/app.css",
        "query_string": b"",
        "headers": headers or [],
        "client": client,
        "root_path": "",
    }


def test_passthrough_non_supervisor_ip_with_ingress_header() -> None:
    """Oracle 5a: RFC-5737 non-Supervisor IP — X-Ingress-Path header is ignored."""
    scope = _base_scope(
        client=(_NON_SUPERVISOR_IP, 9000),
        headers=[(b"x-ingress-path", _INGRESS_PREFIX.encode())],
    )
    result = asyncio.run(_drive_middleware(scope))
    assert result["path"] == "/static/app.css", (
        f"path must be unchanged for non-Supervisor client; got {result['path']!r}"
    )
    assert result["root_path"] == "", (
        f"root_path must be unchanged; got {result['root_path']!r}"
    )


def test_passthrough_supervisor_ip_no_header() -> None:
    """Oracle 5b: Supervisor IP but no X-Ingress-Path header — no mutation."""
    scope = _base_scope(
        client=(_SUPERVISOR_IP, 4321),
        headers=[],  # deliberately omit X-Ingress-Path
    )
    result = asyncio.run(_drive_middleware(scope))
    assert result["path"] == "/static/app.css", (
        f"path must be unchanged when header absent; got {result['path']!r}"
    )
    assert result["root_path"] == "", (
        f"root_path must be unchanged; got {result['root_path']!r}"
    )


def test_passthrough_none_client() -> None:
    """Oracle 5c: client is None — no crash, scope path and root_path unchanged."""
    scope = _base_scope(
        client=None,
        headers=[(b"x-ingress-path", _INGRESS_PREFIX.encode())],
    )
    result = asyncio.run(_drive_middleware(scope))
    assert result["path"] == "/static/app.css", (
        f"path must be unchanged when client is None; got {result['path']!r}"
    )
    assert result["root_path"] == "", (
        f"root_path must be unchanged when client is None; got {result['root_path']!r}"
    )
