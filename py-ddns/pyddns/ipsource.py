# pyright: strict
"""Egress-IPv4 discovery via a configurable primary+fallback provider list.

Each source is an HTTPS echo endpoint (ipify / icanhazip) returning the box's
egress IPv4 as text. The response is **whitespace-stripped** (icanhazip returns a
trailing newline) but **internal whitespace / multiple tokens are rejected**, the
single token is parsed with `ipaddress.IPv4Address`, and **non-global-unicast**
addresses (RFC1918, CGNAT ``100.64/10``, loopback, link-local, ``0.0.0.0``) are
rejected — a private/CGNAT/spoofed value must never be published as an A record.

On all-fail / all-rejected, `detect` returns ``None`` and logs loudly: the
updater then holds last-good (API archetype) but still fires (callback archetype,
since the server detects the real IP).
"""

from __future__ import annotations

import logging
from ipaddress import AddressValueError, IPv4Address

from .httpclient import HttpClient, HttpError

_log = logging.getLogger("pyddns")

# Per the plan: IP-source GET timeout is 5s.
_IP_SOURCE_TIMEOUT_S = 5.0


def parse_global_ipv4(text: str) -> IPv4Address | None:
    """Parse a single global-unicast IPv4 from an echo body, else ``None``.

    Strips surrounding whitespace, rejects a body that is not exactly one token,
    rejects a non-IPv4 literal, and rejects any non-global-unicast address. Each
    rejection is the caller's signal to try the next source / hold last-good.
    """
    stripped = text.strip()
    if stripped == "" or len(stripped.split()) != 1:
        return None
    try:
        addr = IPv4Address(stripped)
    except (AddressValueError, ValueError):
        return None
    if not addr.is_global or addr.is_unspecified:
        return None
    return addr


class IpSourceClient:
    """Try each configured HTTPS IP-source in order; first global IPv4 wins."""

    def __init__(self, sources: tuple[str, ...], http: HttpClient) -> None:
        self._sources = sources
        self._http = http

    def detect(self) -> IPv4Address | None:
        """Return the detected global egress IPv4, or ``None`` if all sources
        failed or returned a non-global address (logged loudly so a hold-last-good
        cycle is visible)."""
        for url in self._sources:
            try:
                resp = self._http.request("GET", url, timeout=_IP_SOURCE_TIMEOUT_S)
            except HttpError as exc:
                _log.warning("ip source failed: %s", exc)
                continue
            addr = parse_global_ipv4(resp.body)
            if addr is not None:
                return addr
            _log.warning(
                "ip source returned a non-global / malformed address; trying next"
            )
        _log.warning("all ip sources failed or returned non-global addresses")
        return None
