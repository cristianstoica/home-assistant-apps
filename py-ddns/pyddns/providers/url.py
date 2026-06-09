# pyright: strict
"""The `url` provider — the callback archetype.

The server does the work: the box just ``GET``s a secret callback URL and the
cPanel server reads the request source IP and sets the A record itself. So this
provider:

* `read_current` returns ``None`` — the server owns the value; drift is judged by
  DNS-resolving `name` (the updater does that, not the provider).
* `apply` **fires regardless** of whether a client IP is known (server-side
  detection is the whole point). When `url_send_myip` is set and a detected IP
  exists, ``myip=<ip>`` is merged into the query via `urllib.parse` so an endpoint
  that already carries a query string keeps its params and the secret path is
  never mangled by naive ``?``/``&`` concatenation.

Status handling: a 2xx is a `FIRED_SERVER_DETECTED` (the updater confirms the
real effect by a post-fire resolve). A ``4xx`` other than ``429`` is **terminal**
(a bad/disabled callback URL or wrong secret won't fix itself). ``429`` / ``5xx``
/ network / timeout is **transient**.

Secret-safe: the ``url_endpoint`` *is* the secret, so it is never logged — only
its scheme + host (the path is masked). The HTTP seam already sanitizes its
exception strings against the request URL.
"""

from __future__ import annotations

import logging
from ipaddress import IPv4Address
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ..errors import TerminalError, TransientError
from ..httpclient import HttpClient, HttpError
from ..models import ApplyAction, ApplyResult
from ..redact import redact_url

_log = logging.getLogger("pyddns")

# Per the plan: URL fire GET timeout is 10s.
_URL_TIMEOUT_S = 10.0


def compose_fire_url(
    endpoint: str, detected_ip: IPv4Address | None, send_myip: bool
) -> str:
    """Return the URL to fire, merging ``myip`` only when requested AND known.

    Uses `urllib.parse` so a pre-existing query string is preserved and the
    secret path is never mangled: ``urlsplit`` → ``parse_qsl`` (keeping blank
    values) → set/replace ``myip`` → ``urlencode`` → ``urlunsplit``. When
    `send_myip` is false or no IP is known, the endpoint is fired unchanged (the
    server detects the IP itself).
    """
    if not send_myip or detected_ip is None:
        return endpoint
    parts = urlsplit(endpoint)
    params = [
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != "myip"
    ]
    params.append(("myip", str(detected_ip)))
    new_query = urlencode(params)
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, new_query, parts.fragment)
    )


class UrlProvider:
    """The `url` (callback archetype) `DnsProvider`."""

    def __init__(self, endpoint: str, send_myip: bool, http: HttpClient) -> None:
        self._endpoint = endpoint
        self._send_myip = send_myip
        self._http = http

    def read_current(self) -> IPv4Address | None:
        """Always ``None`` — the server owns the value; drift is judged by DNS."""
        return None

    def apply(self, detected_ip: IPv4Address | None) -> ApplyResult:
        """Fire the secret callback URL (regardless of a known IP).

        A 2xx is `FIRED_SERVER_DETECTED` (the HTTP success proves only that the URL
        fired, not that DNS moved — the updater confirms by a post-fire resolve).
        A ``4xx``≠``429`` raises `TerminalError`; ``429``/``5xx``/network raises
        `TransientError`.
        """
        url = compose_fire_url(self._endpoint, detected_ip, self._send_myip)
        try:
            self._http.request("GET", url, timeout=_URL_TIMEOUT_S)
        except HttpError as exc:
            if exc.status is not None and 400 <= exc.status < 500 and exc.status != 429:
                raise TerminalError(
                    f"callback URL {redact_url(self._endpoint)} returned "
                    f"HTTP {exc.status} (terminal — bad/disabled URL or wrong secret)"
                ) from None
            raise TransientError(
                f"callback URL {redact_url(self._endpoint)} transient failure: {exc}",
                retry_after=exc.retry_after,
            ) from None
        detail = f"fired callback {redact_url(self._endpoint)}" + (
            f" with myip={detected_ip}"
            if self._send_myip and detected_ip
            else " (server-detected)"
        )
        return ApplyResult(ApplyAction.FIRED_SERVER_DETECTED, detail, None)

    def plan(self, detected_ip: IPv4Address | None) -> str:
        """A redacted, secret-free description of the planned fire (for --dry-run).

        Reports the GET method + scheme + host with the secret path masked, and
        whether ``myip`` would be appended — never the full secret ``url_endpoint``.
        """
        myip = (
            f"myip={detected_ip}"
            if self._send_myip and detected_ip is not None
            else "server-detected (no myip)"
        )
        return (
            f"url: would GET {redact_url(self._endpoint)} ({myip}); "
            "the cPanel server reads the request source IP and sets the record"
        )
