"""Sanitize exception text before it is persisted or displayed."""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx

_URL_RE = re.compile(r"https?://[^\s'\"<>]+")
_SECRET_QUERY_KEYS = frozenset(
    {"apikey", "api_key", "appid", "key", "token", "password"}
)


def _sanitized_parts(exc: BaseException) -> tuple[str, list[str]]:
    """Redacted base message (may be empty) and redacted notes, unjoined."""
    if isinstance(exc, httpx.HTTPStatusError):
        text = _http_status_error(exc)
    else:
        text = redact_urls(str(exc)).strip()
    notes = exc.__notes__ if hasattr(exc, "__notes__") else []
    return text, [redact_urls(note) for note in notes]


def sanitized_exception(exc: BaseException) -> str:
    text, notes = _sanitized_parts(exc)
    return " ".join([text or type(exc).__name__, *notes])


def safe_detail(exc: BaseException) -> str:
    """Render an exception for a surface that must not raise while rendering.

    The class name leads and appears exactly once: ``RuntimeError`` for a
    zero-argument raise, ``RuntimeError: worker stopped`` when there is a
    message. Whether the ``: message`` part is present is decided by the
    sanitizer's own redacted base text being empty -- never by inspecting
    what the rendered text starts with, so a message that happens to begin
    with the class name is kept whole. Notes (PEP 678) follow in the
    sanitizer's redacted form. The REDACTION stays, because this string
    reaches Home Assistant. And the render CANNOT RAISE: sanitizing calls
    ``exc.__str__``, and a pathological one must degrade to the class name
    alone rather than propagate into a done-callback or out of a
    never-raises coroutine.
    """
    name = type(exc).__name__
    try:
        text, notes = _sanitized_parts(exc)
    except Exception:
        return name
    return " ".join([f"{name}: {text}" if text else name, *notes])


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
