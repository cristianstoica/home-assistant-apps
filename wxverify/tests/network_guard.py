"""Deny real network access from every test.

Two layers, both patched at the socket level so stdlib, asyncio, anyio and
httpcore are all covered through the same seam: ``socket.getaddrinfo``
("no DNS" -- a hostname that is not ``""``/``None``/``"localhost"`` never
reaches a resolver) and ``socket.socket.connect`` / ``connect_ex``
("destination" -- an address that is not a loopback literal or
``"localhost"`` never reaches a real host). An allowlist admits only
loopback, by predicate rather than by an enumerated denylist.

Every trip is recorded in a process-global ledger (trips can arrive from
executor threads, so the ledger is lock-protected) before it is raised, so a
trip that the code under test swallows is still visible: the fixture in
``tests/conftest.py`` drains the ledger at teardown and fails the test if
anything was recorded, even when the test body itself passed.

This module is imported as ``tests.network_guard`` -- never as bare
``network_guard``, which pytest's default "prepend" import mode also makes
importable and which would create a second copy of this module's state.

The call-site report attached to each denial (``_where``) walks the stack
without reading source files: it extracts frame summaries with
``lookup_lines=False``, so it never falls through to ``linecache`` for the
literal source line. That is a guarantee about stack reporting only, not a
claim that this module performs no I/O -- the denied ``getaddrinfo`` /
``connect`` calls it intercepts are themselves I/O attempts.
"""

from __future__ import annotations

import ipaddress
import os
import socket
import threading
import traceback
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest

LEDGER_CAP = 20

_ORIG_GETADDRINFO = socket.getaddrinfo
_ORIG_CONNECT = socket.socket.connect
_ORIG_CONNECT_EX = socket.socket.connect_ex

_THIS_FILE = str(Path(__file__).resolve())
_TESTS_DIR = str(Path(__file__).resolve().parent) + os.sep
_PKG_DIR = str(Path(__file__).resolve().parent.parent / "wxverify") + os.sep


class NetworkAccessDenied(Exception):
    """Raised inside the code under test at the point it would leave the process."""


@dataclass(frozen=True)
class Attempt:
    kind: str  # "getaddrinfo" | "connect" | "connect_ex"
    host: str
    port: object
    family: str
    thread: str
    where: str


_LOCK = threading.Lock()
_RECORDED: list[Attempt] = []
_total = 0


def _host_text(host: object) -> str:
    if isinstance(host, (bytes, bytearray)):
        return bytes(host).decode("ascii", "replace")
    if host is None:
        return ""
    return str(host)


def _ip_literal(
    host: str,
) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return None
    if isinstance(addr, ipaddress.IPv6Address):
        mapped = addr.ipv4_mapped
        if mapped is not None:
            return mapped
    return addr


def is_locally_resolvable(host: object) -> bool:
    """Layer-1 predicate: what ``getaddrinfo`` is allowed to resolve."""
    text = _host_text(host)
    return text in ("", "localhost") or _ip_literal(text) is not None


def is_allowed_destination(family: int, host: object) -> bool:
    """Layer-2 predicate: what ``connect`` / ``connect_ex`` may reach."""
    if family == socket.AF_UNIX:
        return True
    text = _host_text(host)
    if text == "localhost":
        return True
    addr = _ip_literal(text)
    return addr is not None and addr.is_loopback


def _family_name(family: int) -> str:
    try:
        return socket.AddressFamily(family).name
    except ValueError:
        return str(family)


def _where() -> str:
    stack = traceback.StackSummary.extract(
        traceback.walk_stack(None), lookup_lines=False
    )
    frames = [
        frame
        for frame in stack
        if (
            frame.filename.startswith(_TESTS_DIR) or frame.filename.startswith(_PKG_DIR)
        )
        and frame.filename != _THIS_FILE
    ]
    if not frames:
        return "(no repo frames on this thread)"
    innermost = frames[:3]
    return " <- ".join(
        f"{Path(frame.filename).name}:{frame.lineno} in {frame.name}"
        for frame in innermost
    )


def _record(kind: str, host: str, port: object, family: str) -> NetworkAccessDenied:
    global _total
    thread = threading.current_thread().name
    where = _where()
    with _LOCK:
        _total += 1
        if len(_RECORDED) < LEDGER_CAP:
            _RECORDED.append(Attempt(kind, host, port, family, thread, where))
    return NetworkAccessDenied(
        f"test attempted real network access: "
        f"{kind}(host={host!r}, port={port!r}, family={family}) "
        f"from thread {thread!r}"
    )


def _guarded_getaddrinfo(
    host: bytes | str | None,
    port: bytes | str | int | None,
    family: int = 0,
    type: int = 0,  # noqa: A002 - matches socket.getaddrinfo's own parameter name
    proto: int = 0,
    flags: int = 0,
) -> Any:
    text = _host_text(host)
    literal = _ip_literal(text)
    if not is_locally_resolvable(text):
        raise _record("getaddrinfo", text, port, _family_name(family))
    if literal is not None:
        return _ORIG_GETADDRINFO(
            host, port, family, type, proto, flags | socket.AI_NUMERICHOST
        )
    return _ORIG_GETADDRINFO(host, port, family, type, proto, flags)


def _check_connect(sock: socket.socket, address: Any, kind: str) -> None:
    port: object
    if isinstance(address, tuple):
        addr_tuple = cast("tuple[object, ...]", address)
        if len(addr_tuple) > 0:
            host = _host_text(addr_tuple[0])
            port = addr_tuple[1] if len(addr_tuple) > 1 else None
        else:
            host = _host_text(addr_tuple)
            port = None
    else:
        host = _host_text(address)
        port = None
    if is_allowed_destination(sock.family, host):
        return
    raise _record(kind, host, port, _family_name(sock.family))


def _guarded_connect(self: socket.socket, address: Any) -> None:
    _check_connect(self, address, "connect")
    _ORIG_CONNECT(self, address)


def _guarded_connect_ex(self: socket.socket, address: Any) -> int:
    _check_connect(self, address, "connect_ex")
    return _ORIG_CONNECT_EX(self, address)


def attempts() -> list[Attempt]:
    with _LOCK:
        return list(_RECORDED)


def attempt_count() -> int:
    with _LOCK:
        return _total


def drain() -> tuple[list[Attempt], int]:
    global _total
    with _LOCK:
        recorded = list(_RECORDED)
        total = _total
        _RECORDED.clear()
        _total = 0
    return recorded, total


def render(recorded: list[Attempt], total: int) -> str:
    lines = [
        f"real network access attempted {total} time(s) during this test "
        f"(showing {len(recorded)}):"
    ]
    for attempt in recorded:
        lines.append(
            f"  {attempt.kind}(host={attempt.host!r}, port={attempt.port!r}, "
            f"family={attempt.family}) thread={attempt.thread!r} "
            f"at {attempt.where}"
        )
    if total > len(recorded):
        lines.append(
            f"  ... {total - len(recorded)} more not shown (LEDGER_CAP={LEDGER_CAP})"
        )
    lines.append(
        "Stub the provider (patch the module's build_adapter / "
        "httpx.AsyncClient) or idle the worker (patch "
        "wxverify.api.app.run_worker); never widen the allowlist."
    )
    return "\n".join(lines)


def deny_network_scope() -> Iterator[None]:
    drain()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(socket, "getaddrinfo", _guarded_getaddrinfo)
        mp.setattr(socket.socket, "connect", _guarded_connect)
        mp.setattr(socket.socket, "connect_ex", _guarded_connect_ex)
        try:
            yield
        finally:
            recorded, total = drain()
    if total:
        pytest.fail(render(recorded, total), pytrace=False)
