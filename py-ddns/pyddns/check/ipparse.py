# pyright: strict
"""IP-source parse/guard checks: stripping, multi-token reject, global-unicast guard."""

from __future__ import annotations

from .. import fixtures
from ..ipsource import IpSourceClient, parse_global_ipv4
from .fakes import FakeHttp, ok_response
from .report import report


def check_ip_parse() -> bool:
    """Assert the IP-source body parser strips, rejects multi-token, guards scope.

    Drives `parse_global_ipv4` against `IP_SOURCE_BODIES`: a clean / newline-
    padded / whitespace-padded global IPv4 parses; an empty body, a two-token
    body, a non-IP, and every non-global-unicast class (RFC1918, CGNAT
    ``100.64/10``, loopback, link-local, ``0.0.0.0``) is rejected.

    Then drives the full `IpSourceClient` over the HTTP seam: a primary source
    returning a non-global value falls through to the next source.
    """
    checks: list[tuple[str, bool]] = []
    for case in fixtures.IP_SOURCE_BODIES:
        result = parse_global_ipv4(case.body)
        actual = str(result) if result is not None else None
        checks.append((f"[{case.name}] -> {case.expected!r}", actual == case.expected))

    # Fallthrough: source 1 returns a private address, source 2 returns a global.
    glob = fixtures.EXAMPLE_GLOBAL_IPV4
    http = FakeHttp(ok_response("192.168.1.5"), ok_response(glob))
    client = IpSourceClient(("https://a.example.com", "https://b.example.com"), http)
    detected = client.detect()
    checks.append(
        (
            "client falls through non-global source to the next",
            str(detected) == glob,
        )
    )

    # All-fail: both sources non-global -> None (hold last-good / still fire url).
    http_all_bad = FakeHttp(ok_response("10.0.0.1"), ok_response("100.64.0.1"))
    client_bad = IpSourceClient(
        ("https://a.example.com", "https://b.example.com"), http_all_bad
    )
    checks.append(
        ("client returns None when all sources non-global", client_bad.detect() is None)
    )
    return report("IP-PARSE", "ip-parse", checks)
