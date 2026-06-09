# pyright: strict
"""The injectable HTTP seam shared by the providers and the IP source.

A tiny request/response surface over `urllib.request`, behind a `HttpClient`
Protocol so the ``--check`` oracle can drive every status-handling branch
(2xx / 404 / 401 / 4xx / 429 / 5xx / network) with a fake — no real sockets.

`HttpError` is the domain error carrying the parsed HTTP **status code** for an
error response (an ``HTTPError`` has a status; a connection/timeout failure has
``status=None`` and is classified transient by the caller). The real client
**sanitizes** the underlying exception string against the request URL before
raising, so a secret callback URL can never surface in a logged error.

The client may be constructed with TLS certificate verification **disabled**
(``UrllibHttpClient(insecure_skip_verify=True)``) — used **only** by the URL
(callback) provider when ``url.insecure_skip_verify`` is set; the default
constructor verifies. The skip keeps the channel encrypted but drops endpoint
authentication, so it is deliberately narrow (callback path only) and loud.
"""

from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.request
from typing import Protocol

from .redact import redact_url, sanitize


class HttpResponse:
    """A read HTTP response: status code, decoded text body, and headers.

    Headers are lower-cased keys for case-insensitive lookup (e.g.
    ``retry-after``). The body is decoded UTF-8 with ``errors="replace"`` so a
    malformed body degrades rather than raising.
    """

    def __init__(self, status: int, body: str, headers: dict[str, str]) -> None:
        self.status = status
        self.body = body
        self.headers = headers

    def json(self) -> object:
        """Parse the body as JSON; raises ``json.JSONDecodeError`` on bad JSON."""
        return json.loads(self.body)


class HttpError(Exception):
    """A failed HTTP call. `status` is the HTTP code, or ``None`` for a
    connection/timeout failure (which the caller classifies as transient).

    `retry_after` carries a parsed ``Retry-After`` seconds value when present
    (``429`` / ``503``), else ``None``. The message is already sanitized.
    """

    def __init__(
        self,
        message: str,
        *,
        status: int | None,
        retry_after: float | None = None,
        body: str = "",
    ) -> None:
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after
        self.body = body


class HttpClient(Protocol):
    """The HTTP transport seam.

    `request` performs one HTTP call and returns an `HttpResponse` on a 2xx,
    raising `HttpError` on any non-2xx or transport failure. The caller passes a
    per-call `timeout` so a hung socket becomes a transient `HttpError`, never a
    stuck loop.
    """

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        data: bytes | None = None,
        timeout: float,
    ) -> HttpResponse: ...


def parse_retry_after(value: str | None) -> float | None:
    """Parse a ``Retry-After`` header's delta-seconds form, else ``None``.

    Only the integer-seconds form is honored (the HTTP-date form is rare for
    these APIs and would need a clock); a non-integer header is ignored. The
    caller caps the returned value, so a hostile large value cannot stall.
    """
    if value is None:
        return None
    try:
        seconds = int(value.strip())
    except ValueError:
        return None
    return float(seconds) if seconds >= 0 else None


class UrllibHttpClient:
    """The production `HttpClient`, backed by `urllib.request`.

    Every raised `HttpError` is sanitized: the underlying exception string (which
    can echo the requested secret URL) is scrubbed against that URL and the host
    is the only locator kept. A transport failure (no HTTP status) raises with
    ``status=None`` so the caller classifies it transient.

    When constructed with ``insecure_skip_verify=True`` the client passes an SSL
    context with certificate verification disabled to ``urlopen`` — used **only**
    by the URL provider when ``url.insecure_skip_verify`` is set. The default
    (verifying) client stores ``None`` and passes nothing, so its behaviour is
    byte-for-byte today's. `verifies_tls` exposes which mode this client is in.
    """

    def __init__(self, *, insecure_skip_verify: bool = False) -> None:
        """Build the client, optionally disabling TLS certificate verification.

        Keyword-only so a positional caller can never accidentally enable the
        skip. When enabled, the unverified context is built **once** here and
        reused per request; ``check_hostname`` must be cleared **before**
        ``verify_mode = CERT_NONE`` (``ssl`` raises otherwise).
        """
        self._ssl_context: ssl.SSLContext | None = None
        if insecure_skip_verify:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self._ssl_context = ctx

    @property
    def verifies_tls(self) -> bool:
        """``True`` iff this client verifies TLS certificates (the default).

        A small public predicate over the (private) SSL-context state so callers
        — including the ``--check`` scope oracle — can assert which mode a built
        client is in without reaching past the seam into a private attribute.
        """
        return self._ssl_context is None

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        data: bytes | None = None,
        timeout: float,
    ) -> HttpResponse:
        req = urllib.request.Request(  # noqa: S310 — scheme is https-validated in config
            url, data=data, method=method, headers=headers or {}
        )
        try:
            with urllib.request.urlopen(  # noqa: S310
                req, timeout=timeout, context=self._ssl_context
            ) as resp:
                raw: bytes = resp.read()
                status = int(resp.status)
                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
        except urllib.error.HTTPError as exc:
            body_bytes: bytes = exc.read() if hasattr(exc, "read") else b""
            err_headers = (
                {k.lower(): v for k, v in exc.headers.items()} if exc.headers else {}
            )
            raise HttpError(
                f"{method} {redact_url(url)} -> HTTP {exc.code}",
                status=int(exc.code),
                retry_after=parse_retry_after(err_headers.get("retry-after")),
                body=body_bytes.decode("utf-8", errors="replace"),
            ) from None
        except urllib.error.URLError as exc:
            # A transport failure (DNS, connect, timeout) has no HTTP status. The
            # reason string can echo the URL, so sanitize against it.
            safe = sanitize(str(exc.reason), (url,))
            raise HttpError(
                f"{method} {redact_url(url)} -> transport error: {safe}",
                status=None,
            ) from None
        except TimeoutError:
            raise HttpError(
                f"{method} {redact_url(url)} -> timed out after {timeout}s",
                status=None,
            ) from None
        return HttpResponse(status, raw.decode("utf-8", errors="replace"), resp_headers)
