"""Oracles for ``tests/network_guard.py`` (plan: network-denial-guard, §9).

Every target here is RFC 2606 / RFC 5737: ``HOST`` is a reserved ``.invalid``
name, ``LITERAL`` is a TEST-NET-1 address. Provider hostnames appear only in
O7, which exercises the real production swallow path under the guard -- the
guard denies the connection before any provider is ever reached, from any
mutant of this module.

Every raw socket used here is bounded and closed; every httpx call passes an
explicit ``timeout``. Under a mutant that lets a call through, the run still
terminates quickly: the ``.invalid`` lookup fails at the resolver and the
TEST-NET-1 connect times out.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import sqlite3
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import Response

from tests.network_guard import (
    LEDGER_CAP,
    NetworkAccessDenied,
    attempt_count,
    attempts,
    deny_network_scope,
    drain,
    is_allowed_destination,
    is_locally_resolvable,
    render,
)
from wxverify import config
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.worker.processor import _is_process_fatal_permission_error

HOST = "wxverify-network-guard.invalid"
LITERAL = ("192.0.2.1", 9)


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


# ---------------------------------------------------------------------------
# O1 -- sync hostname
# ---------------------------------------------------------------------------


def test_sync_hostname_denied() -> None:
    with pytest.raises(NetworkAccessDenied) as ei:
        httpx.get(f"https://{HOST}/", timeout=2.0)
    assert "getaddrinfo(host='wxverify-network-guard.invalid', port=443" in str(
        ei.value
    )
    recorded, total = drain()
    assert total == 1
    assert recorded[0].kind == "getaddrinfo"
    assert recorded[0].thread == "MainThread"


# ---------------------------------------------------------------------------
# O2 -- async hostname
# ---------------------------------------------------------------------------


def test_async_hostname_denied() -> None:
    async def _request() -> None:
        async with httpx.AsyncClient(timeout=2.0) as client:
            await client.get(f"https://{HOST}/")

    with pytest.raises(NetworkAccessDenied) as ei:
        asyncio.run(_request())
    message = str(ei.value)
    assert "b'" not in message
    assert "getaddrinfo(host='wxverify-network-guard.invalid', port=443" in message
    recorded, total = drain()
    assert total == 1
    assert recorded[0].thread != "MainThread"
    assert not _is_process_fatal_permission_error(ei.value)


# ---------------------------------------------------------------------------
# O3 -- IP literal, both paths
# ---------------------------------------------------------------------------


def test_ip_literal_denied_async() -> None:
    async def _request() -> None:
        async with httpx.AsyncClient(timeout=2.0) as client:
            await client.get("http://192.0.2.1:9/")

    with pytest.raises(ExceptionGroup) as ei:
        asyncio.run(_request())
    assert ei.group_contains(NetworkAccessDenied)
    recorded, total = drain()
    assert total == 1
    assert recorded[0].kind == "connect"
    assert recorded[0].host == "192.0.2.1"
    assert recorded[0].port == 9


def test_ip_literal_denied_sync() -> None:
    with pytest.raises(NetworkAccessDenied):
        httpx.get("http://192.0.2.1:9/", timeout=2.0)
    _, total = drain()
    assert total == 1


# ---------------------------------------------------------------------------
# O4 -- raw sockets
# ---------------------------------------------------------------------------


def test_raw_sockets_denied() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        with pytest.raises(NetworkAccessDenied) as ei:
            s.connect((HOST, 443))
    assert "connect" in str(ei.value)
    recorded_a, _ = drain()
    assert recorded_a[0].kind == "connect"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        with pytest.raises(NetworkAccessDenied) as ei:
            s.connect_ex(LITERAL)
    assert isinstance(ei.value, NetworkAccessDenied)
    recorded_b, _ = drain()
    assert recorded_b[0].kind == "connect_ex"

    with pytest.raises(NetworkAccessDenied) as ei:
        socket.getaddrinfo(HOST, 443)
    recorded_c, _ = drain()
    assert recorded_c[0].kind == "getaddrinfo"

    assert len(recorded_a) == len(recorded_b) == len(recorded_c) == 1


# ---------------------------------------------------------------------------
# O5 -- predicate tables
# ---------------------------------------------------------------------------


def test_is_allowed_destination_table() -> None:
    denied = [
        (socket.AF_INET, "192.0.2.1"),
        (socket.AF_INET, "10.0.0.1"),
        (socket.AF_INET, "172.16.0.1"),
        (socket.AF_INET, "192.168.1.1"),
        (socket.AF_INET, "example.invalid"),
        (socket.AF_INET, "0.0.0.0"),
        (socket.AF_INET6, "2001:db8::1"),
    ]
    for family, host in denied:
        assert is_allowed_destination(family, host) is False, (family, host)

    allowed = [
        (socket.AF_INET, "127.0.0.1"),
        (socket.AF_INET, "127.255.255.254"),
        (socket.AF_INET, "localhost"),
        (socket.AF_INET6, "::1"),
        (socket.AF_INET6, "::ffff:127.0.0.1"),
        (socket.AF_UNIX, "/any/path"),
    ]
    for family, host in allowed:
        assert is_allowed_destination(family, host) is True, (family, host)


def test_is_locally_resolvable_table() -> None:
    for host in (None, "", "localhost", "127.0.0.1", "192.0.2.1", b"127.0.0.1"):
        assert is_locally_resolvable(host) is True, host
    for host in ("example.invalid", b"example.invalid"):
        assert is_locally_resolvable(host) is False, host


# ---------------------------------------------------------------------------
# O6 -- swallowed trip fails at teardown
# ---------------------------------------------------------------------------


def test_swallowed_trip_fails_at_teardown() -> None:
    scope = deny_network_scope()
    next(scope)
    try:
        with contextlib.suppress(Exception):
            socket.getaddrinfo(HOST, 80)
        with pytest.raises(pytest.fail.Exception) as ei:
            next(scope)
        message = ei.value.msg
        assert message is not None
        assert "real network access attempted 1 time(s)" in message
        assert HOST in message
        assert "getaddrinfo" in message
        assert "test_network_guard.py" in message
        # A5: the reported call site is the test frame, not a guard-module frame.
        assert "in test_swallowed_trip_fails_at_teardown" in message
        for guard_frame_name in (
            "_guarded_getaddrinfo",
            "_check_connect",
            "_guarded_connect",
            "_guarded_connect_ex",
            "_record",
            "_where",
        ):
            assert f" in {guard_frame_name}" not in message
        assert attempt_count() == 0
    finally:
        scope.close()


# ---------------------------------------------------------------------------
# O7 -- the production swallow path, test 2's shape
# ---------------------------------------------------------------------------


def test_catchup_swallows_denied_open_meteo_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('guard-site', 0.0, 0.0, 0.0, 'UTC')
        """
    )
    conn.commit()
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "SYNTHETIC-KEY")

    from wxverify.worker.catchup import run_catchup  # noqa: PLC0415

    db = get_db()
    writer = FencedWriter(db, db.generation)

    scope = deny_network_scope()
    next(scope)
    try:
        asyncio.run(run_catchup(db, writer, {}))
        with pytest.raises(pytest.fail.Exception) as ei:
            next(scope)
        message = ei.value.msg
        assert message is not None
        assert (
            message.count(
                "getaddrinfo(host='previous-runs-api.open-meteo.com', port=443"
            )
            == 7
        )
        assert "host='api.open-meteo.com'" not in message
        assert "[Errno 1]" not in message
    finally:
        scope.close()


# ---------------------------------------------------------------------------
# O8 -- positive controls, zero attempts
# ---------------------------------------------------------------------------


def test_positive_controls_record_nothing() -> None:
    # (a) in-process ASGI TestClient
    app = FastAPI()

    @app.get("/ping")
    def _ping() -> dict[str, bool]:
        return {"ok": True}

    with TestClient(app) as client:
        response = client.get("/ping")
    assert response.status_code == 200
    assert attempt_count() == 0

    # (b) httpx.AsyncClient over an httpx.MockTransport
    def _handler(request: httpx.Request) -> Response:
        return Response(204)

    async def _mocked_request() -> httpx.Response:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_handler)
        ) as mock_client:
            return await mock_client.get("https://api.open-meteo.com/v1/forecast")

    response = asyncio.run(_mocked_request())
    assert response.status_code == 204
    assert attempt_count() == 0

    # (c) sqlite3 in-memory
    with sqlite3.connect(":memory:") as conn:
        conn.execute("select 1")
    assert attempt_count() == 0

    # (d) loopback stdlib
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_sock:
            client_sock.settimeout(1.0)
            client_sock.connect(("127.0.0.1", port))
            accepted, _ = server.accept()
            accepted.close()
    assert attempt_count() == 0

    # (e) asyncio loopback server + open_connection
    async def _asyncio_loopback() -> None:
        async def _handle(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(_handle, "127.0.0.1", 0)
        try:
            host, port = server.sockets[0].getsockname()[:2]
            reader, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(_asyncio_loopback())
    assert attempt_count() == 0

    # (f) httpx.AsyncClient against a real loopback HTTP/1.0 server
    async def _asyncio_http_server() -> None:
        async def _handle(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            await reader.readline()
            writer.write(b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\nok")
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(_handle, "127.0.0.1", 0)
        try:
            port = server.sockets[0].getsockname()[1]
            async with httpx.AsyncClient(timeout=2.0) as client:
                response = await client.get(f"http://localhost:{port}/")
            assert response.status_code == 200
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(_asyncio_http_server())
    assert attempt_count() == 0

    # (g) socket.gethostname
    assert isinstance(socket.gethostname(), str)
    assert attempt_count() == 0

    # (h) socket.socketpair
    a, b = socket.socketpair()
    a.close()
    b.close()
    assert attempt_count() == 0


# ---------------------------------------------------------------------------
# O9 -- nested scopes restore, not remove
# ---------------------------------------------------------------------------


def test_nested_scope_restores_outer() -> None:
    inner = deny_network_scope()
    next(inner)
    with pytest.raises(StopIteration):
        next(inner)

    assert "connect" in socket.socket.__dict__
    with pytest.raises(NetworkAccessDenied):
        socket.getaddrinfo(HOST, 80)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        with pytest.raises(NetworkAccessDenied):
            s.connect(LITERAL)

    _, total = drain()
    assert total == 2


# ---------------------------------------------------------------------------
# O10 -- cap and total
# ---------------------------------------------------------------------------


def test_ledger_cap_and_total() -> None:
    for _ in range(25):
        with pytest.raises(NetworkAccessDenied):
            socket.getaddrinfo(HOST, 80)
    assert len(attempts()) == LEDGER_CAP == 20
    assert attempt_count() == 25
    recorded, total = drain()
    message = render(recorded, total)
    assert "attempted 25 time(s)" in message
    assert "5 more not shown" in message


# ---------------------------------------------------------------------------
# O11 -- setup clears
# ---------------------------------------------------------------------------


def test_scope_setup_clears_prior_attempts() -> None:
    with contextlib.suppress(Exception):
        socket.getaddrinfo(HOST, 80)
    assert attempt_count() == 1

    scope = deny_network_scope()
    next(scope)
    try:
        with pytest.raises(StopIteration):
            next(scope)
    finally:
        scope.close()
