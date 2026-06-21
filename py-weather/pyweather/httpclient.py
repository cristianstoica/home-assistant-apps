# pyright: strict
"""The injectable HTTP seam for the Home Assistant Core-API proxy calls.

A tiny request/response surface over `urllib.request`, behind a `HttpClient`
Protocol so the ``--check`` oracle can drive every status-handling branch
(2xx / 401 / 403 / 404 / 422 / 429 / 5xx / network / timeout) with a recording
fake — no real sockets.

`HttpError` is the domain error carrying the parsed HTTP **status code** for an
error response (an ``HTTPError`` has a status; a connection/timeout failure has
``status=None`` and is classified transient by the caller). The real client
**sanitizes** the underlying exception string against the bearer token before
raising, so the ``SUPERVISOR_TOKEN`` can never surface in a logged error.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Protocol


class HttpResponse:
    """A read HTTP response: status code, decoded text body, and headers.

    Headers are lower-cased keys for case-insensitive lookup. The body is decoded
    UTF-8 with ``errors="replace"`` so a malformed body degrades rather than
    raising at read time (the JSON parse is where a malformed body surfaces).
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

    The message is already sanitized against the bearer token.
    """

    def __init__(self, message: str, *, status: int | None) -> None:
        super().__init__(message)
        self.status = status


class HttpClient(Protocol):
    """The HTTP transport seam.

    `request` performs one HTTP call and returns an `HttpResponse` on a 2xx,
    raising `HttpError` on any non-2xx or transport failure. The caller passes a
    per-call `timeout` so a hung socket becomes a transient `HttpError`, never a
    stuck loop, and a `headers`/`data` pair so the same seam carries both the
    body-bearing POST and the bodyless GET.
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


class UrllibHttpClient:
    """The production `HttpClient`, backed by `urllib.request`.

    Every raised `HttpError` is sanitized: the underlying exception string (which
    can echo a request header carrying the bearer token) is scrubbed against the
    `secrets` provided at construction. A transport failure (no HTTP status)
    raises with ``status=None`` so the caller classifies it transient.

    The base URL is always the in-cluster ``http://supervisor`` proxy (validated
    by construction in `haapi`), so plain HTTP here is correct, not a downgrade:
    the Supervisor socket is local and unencrypted by design.
    """

    def __init__(self, secrets: tuple[str, ...] = ()) -> None:
        self._secrets = secrets

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        data: bytes | None = None,
        timeout: float,
    ) -> HttpResponse:
        from .redact import sanitize

        req = urllib.request.Request(  # noqa: S310 — fixed http://supervisor proxy host
            url, data=data, method=method, headers=headers or {}
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
                raw: bytes = resp.read()
                status = int(resp.status)
                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
        except urllib.error.HTTPError as exc:
            raise HttpError(
                sanitize(f"{method} {url} -> HTTP {exc.code}", self._secrets),
                status=int(exc.code),
            ) from None
        except urllib.error.URLError as exc:
            safe = sanitize(str(exc.reason), self._secrets)
            raise HttpError(
                f"{method} {url} -> transport error: {safe}",
                status=None,
            ) from None
        except TimeoutError:
            raise HttpError(
                f"{method} {url} -> timed out after {timeout}s",
                status=None,
            ) from None
        return HttpResponse(status, raw.decode("utf-8", errors="replace"), resp_headers)
