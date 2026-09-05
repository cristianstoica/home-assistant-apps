"""Sanitize exception text before it is persisted or displayed."""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

_URL_RE = re.compile(r"https?://[^\s'\"<>]+")
_SECRET_QUERY_KEYS = frozenset(
    {"apikey", "api_key", "appid", "key", "token", "password"}
)


def sanitized_exception(exc: BaseException) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        return _http_status_error(exc)
    return redact_urls(str(exc))


def safe_detail(exc: BaseException) -> str:
    """Render an exception for a surface that must not raise while rendering.

    Three properties, each load-bearing. The TYPE NAME is included because
    ``sanitized_exception`` is ``redact_urls(str(exc))`` and carries none, so a
    zero-argument ``raise RuntimeError`` would otherwise ship an empty detail --
    discarding the single most diagnostic token on a surface that leaves the
    process. The REDACTION stays, because this string reaches Home Assistant.
    And the render CANNOT RAISE: ``sanitized_exception`` calls ``exc.__str__``,
    and a pathological one must degrade to the class name rather than propagate
    into a done-callback or out of a never-raises coroutine.
    """
    try:
        return f"{type(exc).__name__}: {sanitized_exception(exc)}"
    except Exception:
        return type(exc).__name__


def redact_urls(message: str) -> str:
    return _URL_RE.sub(lambda match: _redact_url(match.group(0)), message)


def _http_status_error(exc: httpx.HTTPStatusError) -> str:
    response = exc.response
    request = response.request
    return (
        f"HTTP {response.status_code} {response.reason_phrase} "
        f"for {request.method} {_redact_url(str(request.url))}"
    )


def _redact_url(raw_url: str) -> str:
    parts = urlsplit(raw_url)
    redacted = [
        (key, "***" if key.lower() in _SECRET_QUERY_KEYS else value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
    ]
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(redacted, doseq=True),
            parts.fragment,
        )
    )
