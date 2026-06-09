# pyright: strict
"""Resolve `name` for the status/verification log line and the `url` drift signal.

Returns the three-way `ResolveOutcome` — never a bare ``None`` — so the updater's
callback-confirmation gate can tell "the record holds value X" / "no such record"
/ "the query failed transiently" apart. Misclassifying a transient failure as
"stale" would force a needless refire and mark a correct record unconfirmed.

When `test_ns` is set the resolver sends a small **stdlib UDP DNS A-query**
straight at that nameserver (RD=1, so it works against both a recursive resolver
and the zone's authoritative NS); a hostname `test_ns` is resolved via
`getaddrinfo` first. When `test_ns` is blank it falls back to
`socket.getaddrinfo` (the system resolver's recursive/cached A view).

The packet build/parse is pure (`build_query` / `parse_response`) and exercised
directly by the ``--check`` oracle; the UDP transport is behind a small seam so
the oracle can drive answer-parse / no-record / transient outcomes with no real
socket.
"""

from __future__ import annotations

import logging
import secrets
import socket
import struct
from ipaddress import AddressValueError, IPv4Address
from typing import Protocol

from .models import ResolveOutcome, ResolveStatus

_log = logging.getLogger("pyddns")

# Per the plan: DNS UDP query timeout is 3s per nameserver.
_DNS_TIMEOUT_S = 3.0
_DNS_PORT = 53
_TYPE_A = 1
_CLASS_IN = 1
_FLAG_RD = 0x0100
_RCODE_MASK = 0x000F
_RCODE_NXDOMAIN = 3


class DnsParseError(Exception):
    """The response bytes could not be parsed as a DNS reply to our query."""


def build_query(name: str, query_id: int) -> bytes:
    """Build a single A-record DNS query packet (RD=1) for `name`.

    One question, recursion-desired set. `query_id` is the 16-bit transaction ID
    the response must echo. Each label is length-prefixed; the name is
    root-terminated. A label > 63 octets is rejected (malformed name).
    """
    header = struct.pack(">HHHHHH", query_id, _FLAG_RD, 1, 0, 0, 0)
    qname = bytearray()
    for label in name.rstrip(".").split("."):
        # Enforce the 63-octet label limit *before* the idna encode: the idna
        # codec raises a bare UnicodeError on an over-long label, which would
        # escape this function's DnsParseError contract (and resolve()'s catch
        # set). Guarding the raw length first keeps the documented behavior.
        if len(label) > 63:
            raise DnsParseError(f"label too long in {name!r}")
        try:
            encoded = label.encode("idna") if label else b""
        except UnicodeError as exc:
            raise DnsParseError(f"invalid label in {name!r}: {exc}") from None
        if len(encoded) > 63:
            raise DnsParseError(f"label too long in {name!r}")
        qname.append(len(encoded))
        qname.extend(encoded)
    qname.append(0)
    question = bytes(qname) + struct.pack(">HH", _TYPE_A, _CLASS_IN)
    return header + question


def _skip_name(data: bytes, offset: int) -> int:
    """Advance past a (possibly compressed) DNS name, returning the next offset."""
    while True:
        if offset >= len(data):
            raise DnsParseError("truncated name")
        length = data[offset]
        if length == 0:
            return offset + 1
        if length & 0xC0 == 0xC0:  # compression pointer: 2 octets, name ends here
            return offset + 2
        offset += 1 + length


def parse_response(data: bytes, query_id: int) -> ResolveOutcome:
    """Parse a DNS reply into a `ResolveOutcome`.

    Returns `RESOLVED` with the first A record's value, `NO_RECORD` for an
    NXDOMAIN or an empty/no-A answer (an authoritative "does not exist"), and
    raises `DnsParseError` for a malformed reply or a transaction-ID mismatch
    (the caller maps that to `FAILED` — a transient condition).
    """
    if len(data) < 12:
        raise DnsParseError("response shorter than DNS header")
    resp_id, flags, qd, an, _ns, _ar = struct.unpack(">HHHHHH", data[:12])
    if resp_id != query_id:
        raise DnsParseError("transaction id mismatch")
    rcode = flags & _RCODE_MASK
    if rcode == _RCODE_NXDOMAIN:
        return ResolveOutcome(ResolveStatus.NO_RECORD, None)
    offset = 12
    for _ in range(qd):  # skip the echoed question section
        offset = _skip_name(data, offset)
        offset += 4  # QTYPE + QCLASS
    for _ in range(an):
        offset = _skip_name(data, offset)
        if offset + 10 > len(data):
            raise DnsParseError("truncated answer record")
        rtype, _rclass, _ttl, rdlength = struct.unpack(
            ">HHIH", data[offset : offset + 10]
        )
        offset += 10
        if offset + rdlength > len(data):
            raise DnsParseError("truncated rdata")
        if rtype == _TYPE_A and rdlength == 4:
            try:
                return ResolveOutcome(
                    ResolveStatus.RESOLVED, IPv4Address(data[offset : offset + 4])
                )
            except (AddressValueError, ValueError) as exc:
                raise DnsParseError(f"bad A rdata: {exc}") from None
        offset += rdlength
    # A successful (rcode 0) reply with no A record == the record does not exist.
    return ResolveOutcome(ResolveStatus.NO_RECORD, None)


class UdpQuerier(Protocol):
    """Send `query` to `(server_ip, 53)` over UDP and return the reply bytes.

    Behind a seam so the ``--check`` oracle can return canned reply bytes /
    raise a timeout, with no real socket. Raises ``OSError`` / ``TimeoutError``
    on a transport failure (the caller maps either to `FAILED`).
    """

    def __call__(self, server_ip: str, query: bytes) -> bytes: ...


def _udp_query(server_ip: str, query: bytes) -> bytes:
    """Production `UdpQuerier`: one UDP round-trip under a 3s timeout."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(_DNS_TIMEOUT_S)
        sock.sendto(query, (server_ip, _DNS_PORT))
        data, _addr = sock.recvfrom(512)
    return data


class DnsResolver:
    """Resolve `name` via `test_ns` (UDP) when set, else `getaddrinfo`.

    The UDP transport and the `getaddrinfo` call are injectable so the oracle can
    exercise both branches and all three outcomes deterministically.
    """

    def __init__(
        self,
        test_ns: str,
        *,
        querier: UdpQuerier = _udp_query,
        getaddrinfo: "GetAddrInfo | None" = None,
    ) -> None:
        self._test_ns = test_ns
        self._querier = querier
        self._getaddrinfo = (
            getaddrinfo if getaddrinfo is not None else _system_getaddrinfo
        )

    def resolve(self, name: str) -> ResolveOutcome:
        """Resolve `name` to a `ResolveOutcome` (logged, never fatal)."""
        if self._test_ns:
            return self._resolve_via_ns(name, self._test_ns)
        return self._resolve_via_system(name)

    def _resolve_via_ns(self, name: str, test_ns: str) -> ResolveOutcome:
        server_ip = self._resolve_ns_address(test_ns)
        if server_ip is None:
            _log.warning("test_ns %r could not be resolved to an address", test_ns)
            return ResolveOutcome(ResolveStatus.FAILED, None)
        query_id = secrets.randbelow(0x10000)
        try:
            reply = self._querier(server_ip, build_query(name, query_id))
        except (OSError, TimeoutError, DnsParseError) as exc:
            # DnsParseError here means build_query rejected a malformed `name`
            # (over-long/illegal label). Config-load validation
            # (config.validate_dns_hostname) makes this unreachable in practice;
            # catching it is defense-in-depth so resolve() stays total over its
            # return type rather than letting a UnicodeError-derived exception
            # escape DnsResolver.
            _log.warning("DNS UDP query to %s failed: %s", server_ip, exc)
            return ResolveOutcome(ResolveStatus.FAILED, None)
        try:
            return parse_response(reply, query_id)
        except DnsParseError as exc:
            _log.warning("DNS reply from %s unparseable: %s", server_ip, exc)
            return ResolveOutcome(ResolveStatus.FAILED, None)

    def _resolve_ns_address(self, test_ns: str) -> str | None:
        """An IP-literal `test_ns` is used directly (no resolution); a hostname is
        resolved via `getaddrinfo` to its first IPv4 (inheriting the ~5s musl
        bound). Returns ``None`` when resolution fails."""
        try:
            IPv4Address(test_ns)
            return test_ns
        except (AddressValueError, ValueError):
            pass
        try:
            infos = self._getaddrinfo(test_ns)
        except OSError:
            return None
        for addr in infos:
            return addr
        return None

    def _resolve_via_system(self, name: str) -> ResolveOutcome:
        try:
            infos = self._getaddrinfo(name)
        except socket.gaierror as exc:
            # gaierror with NONAME/NODATA == authoritative "no such record";
            # other gaierror codes (e.g. AGAIN/temporary failure) are transient.
            if exc.errno in (socket.EAI_NONAME, getattr(socket, "EAI_NODATA", -5)):
                return ResolveOutcome(ResolveStatus.NO_RECORD, None)
            _log.warning("getaddrinfo for %r failed transiently: %s", name, exc)
            return ResolveOutcome(ResolveStatus.FAILED, None)
        except OSError as exc:
            _log.warning("getaddrinfo for %r failed: %s", name, exc)
            return ResolveOutcome(ResolveStatus.FAILED, None)
        for addr in infos:
            try:
                return ResolveOutcome(ResolveStatus.RESOLVED, IPv4Address(addr))
            except (AddressValueError, ValueError):
                continue
        return ResolveOutcome(ResolveStatus.NO_RECORD, None)


class GetAddrInfo(Protocol):
    """Resolve a host to a list of IPv4 address strings (the system-resolver seam)."""

    def __call__(self, host: str) -> list[str]: ...


def _system_getaddrinfo(host: str) -> list[str]:
    """Production `GetAddrInfo`: A-record lookup via `socket.getaddrinfo`.

    Returns the IPv4 address strings (AF_INET only). `socket.getaddrinfo` takes
    no timeout argument; on the Alpine/musl base image the resolver is internally
    bounded to ~5s (see README), so this fails transiently rather than hanging.
    """
    results = socket.getaddrinfo(
        host, None, family=socket.AF_INET, type=socket.SOCK_DGRAM
    )
    return [str(sockaddr[0]) for *_unused, sockaddr in results]
