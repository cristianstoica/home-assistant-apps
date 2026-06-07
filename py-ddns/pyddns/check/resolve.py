# pyright: strict
"""Resolver checks: query shaping, three-way answer parse, test_ns UDP + fallback."""

from __future__ import annotations

import socket
import struct
from ipaddress import IPv4Address

from ..models import ResolveStatus
from ..resolver import DnsParseError, DnsResolver, build_query, parse_response
from .report import report

_TYPE_A = 1
_CLASS_IN = 1


def _a_reply(query_id: int, name: str, ip: str) -> bytes:
    """Build a minimal wire-format reply: one question echo + one A answer."""
    header = struct.pack(">HHHHHH", query_id, 0x8180, 1, 1, 0, 0)  # QR=1, RA, RD
    qname = bytearray()
    for label in name.rstrip(".").split("."):
        encoded = label.encode("idna")
        qname.append(len(encoded))
        qname.extend(encoded)
    qname.append(0)
    question = bytes(qname) + struct.pack(">HH", _TYPE_A, _CLASS_IN)
    # Answer: name compression pointer to the question name at offset 12.
    answer = (
        struct.pack(">H", 0xC00C)
        + struct.pack(">HHIH", _TYPE_A, _CLASS_IN, 60, 4)
        + IPv4Address(ip).packed
    )
    return header + question + answer


def _nxdomain_reply(query_id: int, name: str) -> bytes:
    """Build a wire-format NXDOMAIN reply (rcode 3, no answers)."""
    header = struct.pack(">HHHHHH", query_id, 0x8183, 1, 0, 0, 0)  # rcode 3
    qname = bytearray()
    for label in name.rstrip(".").split("."):
        encoded = label.encode("idna")
        qname.append(len(encoded))
        qname.extend(encoded)
    qname.append(0)
    question = bytes(qname) + struct.pack(">HH", _TYPE_A, _CLASS_IN)
    return header + question


def _noanswer_reply(query_id: int, name: str) -> bytes:
    """Build a rcode-0 (success) reply with **zero answers** — a no-A success.

    A NOERROR reply with ancount 0 (or only non-A records) means the name exists
    but holds no A record (CNAME-only / empty A set). `parse_response` must map
    this to ``NO_RECORD``, distinct from a transient ``FAILED``.
    """
    header = struct.pack(">HHHHHH", query_id, 0x8180, 1, 0, 0, 0)  # rcode 0, an=0
    qname = bytearray()
    for label in name.rstrip(".").split("."):
        encoded = label.encode("idna")
        qname.append(len(encoded))
        qname.extend(encoded)
    qname.append(0)
    question = bytes(qname) + struct.pack(">HH", _TYPE_A, _CLASS_IN)
    return header + question


def check_resolver() -> bool:
    """Assert query shaping, the three-way parse, and the test_ns/fallback dispatch.

    * `build_query` produces a single-question RD=1 packet whose echoed id and
      qname round-trip through `parse_response` (built reply uses the same id).
    * `parse_response` maps an A answer → ``RESOLVED`` (with the value), an
      NXDOMAIN → ``NO_RECORD``, a no-A success → ``NO_RECORD``, and an
      id-mismatch → the caller's ``FAILED`` (via `DnsResolver`'s UDP path).
    * With ``test_ns`` set, `DnsResolver` sends UDP at the literal NS and parses
      the reply; a UDP timeout maps to ``FAILED`` (transient, not stale).
    * With ``test_ns`` blank, `DnsResolver` uses the injected ``getaddrinfo``:
      a result → ``RESOLVED``; ``EAI_NONAME`` → ``NO_RECORD``; ``EAI_AGAIN`` →
      ``FAILED``.
    """
    name = "home.example.com"
    checks: list[tuple[str, bool]] = []

    # --- query shaping ---
    q = build_query(name, 0x1234)
    qid, flags, qd, an = struct.unpack(">HHHH", q[:8])
    checks += [
        ("build_query echoes the transaction id", qid == 0x1234),
        ("build_query sets RD=1", flags == 0x0100),
        ("build_query has exactly one question, no answers", qd == 1 and an == 0),
    ]

    # A label > 63 octets is a malformed name -> DnsParseError (GAP 5).
    raised_long_label = False
    try:
        build_query("x" * 64 + ".example.com", 1)
    except DnsParseError:
        raised_long_label = True
    checks.append(("build_query rejects a label > 63 octets", raised_long_label))

    # Defense-in-depth (Part B/3): an over-long `name` that slips past config
    # validation must NOT escape DnsResolver.resolve() — the test_ns UDP path
    # widens its catch to DnsParseError and maps it to FAILED. Driving resolve()
    # (not build_query) pins the integration behavior, not just the builder.
    def _udp_unreachable(server_ip: str, query: bytes) -> bytes:
        raise AssertionError("querier must not be reached for a malformed name")

    resolve_long = ResolveStatus.FAILED  # default if it somehow returns nothing
    raised_from_resolve = False
    try:
        resolve_long = (
            DnsResolver("192.0.2.53", querier=_udp_unreachable)
            .resolve("x" * 64 + ".example.com")
            .status
        )
    except Exception:  # noqa: BLE001 - the check is that resolve() does NOT raise
        raised_from_resolve = True
    checks.append(
        (
            "resolve(over-long name) -> FAILED, does not raise",
            not raised_from_resolve and resolve_long is ResolveStatus.FAILED,
        )
    )

    # --- parse: RESOLVED / NO_RECORD (nxdomain) / NO_RECORD (no-A) ---
    resolved = parse_response(_a_reply(0x1234, name, "203.0.113.7"), 0x1234)
    checks.append(
        (
            "A answer -> RESOLVED with value",
            resolved.status is ResolveStatus.RESOLVED
            and resolved.value == IPv4Address("203.0.113.7"),
        )
    )
    nx = parse_response(_nxdomain_reply(0x1234, name), 0x1234)
    checks.append(("NXDOMAIN -> NO_RECORD", nx.status is ResolveStatus.NO_RECORD))
    no_a = parse_response(_noanswer_reply(0x1234, name), 0x1234)
    checks.append(
        (
            "rcode-0 no-A success -> NO_RECORD",
            no_a.status is ResolveStatus.NO_RECORD and no_a.value is None,
        )
    )

    # --- test_ns UDP path via injected querier ---
    def _udp_resolved(server_ip: str, query: bytes) -> bytes:
        qid_local = struct.unpack(">H", query[:2])[0]
        return _a_reply(qid_local, name, "203.0.113.7")

    ns_resolver = DnsResolver("192.0.2.53", querier=_udp_resolved)
    ns_outcome = ns_resolver.resolve(name)
    checks.append(
        (
            "test_ns UDP A answer -> RESOLVED",
            ns_outcome.status is ResolveStatus.RESOLVED
            and ns_outcome.value == IPv4Address("203.0.113.7"),
        )
    )

    def _udp_timeout(server_ip: str, query: bytes) -> bytes:
        raise TimeoutError("simulated UDP timeout")

    ns_fail = DnsResolver("192.0.2.53", querier=_udp_timeout).resolve(name)
    checks.append(
        (
            "test_ns UDP timeout -> FAILED (transient)",
            ns_fail.status is ResolveStatus.FAILED,
        )
    )

    def _udp_idmismatch(server_ip: str, query: bytes) -> bytes:
        return _a_reply(0xDEAD, name, "203.0.113.7")  # wrong id

    ns_mismatch = DnsResolver("192.0.2.53", querier=_udp_idmismatch).resolve(name)
    checks.append(
        (
            "test_ns id-mismatch reply -> FAILED",
            ns_mismatch.status is ResolveStatus.FAILED,
        )
    )

    # --- blank test_ns: system getaddrinfo path ---
    def _gai_ok(host: str) -> list[str]:
        return ["203.0.113.7"]

    sys_resolved = DnsResolver("", getaddrinfo=_gai_ok).resolve(name)
    checks.append(
        (
            "blank test_ns getaddrinfo result -> RESOLVED",
            sys_resolved.status is ResolveStatus.RESOLVED
            and sys_resolved.value == IPv4Address("203.0.113.7"),
        )
    )

    def _gai_noname(host: str) -> list[str]:
        raise socket.gaierror(socket.EAI_NONAME, "name not known")

    sys_norecord = DnsResolver("", getaddrinfo=_gai_noname).resolve(name)
    checks.append(
        (
            "blank test_ns EAI_NONAME -> NO_RECORD",
            sys_norecord.status is ResolveStatus.NO_RECORD,
        )
    )

    def _gai_again(host: str) -> list[str]:
        raise socket.gaierror(socket.EAI_AGAIN, "temporary failure")

    sys_failed = DnsResolver("", getaddrinfo=_gai_again).resolve(name)
    checks.append(
        (
            "blank test_ns EAI_AGAIN -> FAILED (transient)",
            sys_failed.status is ResolveStatus.FAILED,
        )
    )
    return report("RESOLVER", "resolver", checks)
