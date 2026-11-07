"""Sanitize exception text before it is persisted or displayed."""

from __future__ import annotations

import json
import re
from typing import Final, cast
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit

import httpx

_URL_RE = re.compile(r"https?://[^\s'\"<>]+")
_SECRET_QUERY_KEYS = frozenset(
    {"apikey", "api_key", "appid", "key", "token", "password"}
)

#: A reply body larger than this is never parsed for a reason.
_REASON_MAX_BYTES: Final = 16384
#: A reason longer than this is cut to one less, plus an ellipsis.
_REASON_MAX_CHARS: Final = 200
#: The only places a reason is read from, in the order they are tried.
_REASON_PATHS: Final[tuple[tuple[str | int, ...], ...]] = (
    ("errors", 0, "error", "message"),
    ("errors", 0, "message"),
    ("error", "message"),
    ("message",),
)
_WHITESPACE_RE = re.compile(r"\s+")
_ADDRESS: Final = "[address]"
_MASK: Final = "***"


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
    text = (
        f"HTTP {response.status_code} {response.reason_phrase} "
        f"for {request.method} {_redact_url(str(request.url))}"
    )
    reason = provider_reason(response)
    if reason is None:
        return text
    return f'{text}; provider says "{reason}"'


def provider_reason(response: httpx.Response) -> str | None:
    """The provider's own reason for a failed reply, masked; else None.

    Reads ``response.content`` only: never ``read()``, never a new request, so
    an unread streamed reply gives None. A body over 16384 bytes, or one
    ``json.loads`` refuses, gives None. The reason is the first non-empty
    string at one of ``_REASON_PATHS``, tried in order; a value that is not a
    string is skipped, and nothing else in the body is read. That text is
    masked (``_mask_reason``), flattened to one printable line, and cut to
    200 characters, the last an ellipsis; empty after flattening gives None.
    Never raises: any failure gives None, so the caller's text stays what it
    is without a reason.
    """
    try:
        body = response.content
        if len(body) > _REASON_MAX_BYTES:
            return None
        data: object = json.loads(body)
        found = _first_reason(data)
        if found is None:
            return None
        masked = _mask_reason(found, response.request.url)
        spaced = _WHITESPACE_RE.sub(" ", masked)
        text = "".join(ch for ch in spaced if ch.isprintable()).strip()
        if not text:
            return None
        if len(text) > _REASON_MAX_CHARS:
            text = text[: _REASON_MAX_CHARS - 1] + "…"
        return text
    except Exception:
        return None


def _first_reason(data: object) -> str | None:
    """The first non-empty string at one of ``_REASON_PATHS``, else None."""
    for path in _REASON_PATHS:
        value = _value_at(data, path)
        if isinstance(value, str) and value:
            return value
    return None


def _value_at(data: object, path: tuple[str | int, ...]) -> object:
    """The value ``path`` leads to in ``data``; None where a step is missing."""
    node = data
    for step in path:
        if isinstance(step, int):
            if not isinstance(node, list):
                return None
            items = cast(list[object], node)
            if step >= len(items):
                return None
            node = items[step]
        else:
            if not isinstance(node, dict):
                return None
            fields = cast(dict[str, object], node)
            if step not in fields:
                return None
            node = fields[step]
    return node


def _mask_reason(text: str, url: httpx.URL) -> str:
    """``text`` with addresses and the request's own values masked.

    In this order: every ``_URL_RE`` match becomes ``[address]``; each value
    of a ``_SECRET_QUERY_KEYS`` query parameter of ``url``, raw and
    percent-encoded, becomes ``***``; every other query value of 3 or more
    characters, and every path segment holding a digit, becomes ``***``,
    longest first so a value inside another is masked whole; then the host
    becomes ``[address]``.
    """
    text = _URL_RE.sub(_ADDRESS, text)
    parts = urlsplit(str(url))
    secrets: set[str] = set()
    others: set[str] = set()
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if key.lower() in _SECRET_QUERY_KEYS:
            if value:
                secrets.update((value, quote(value, safe="")))
        elif len(value) >= 3:
            others.add(value)
    for segment in parts.path.split("/"):
        if any(ch.isdigit() for ch in segment):
            others.add(unquote(segment))
    for secret in _longest_first(secrets):
        text = text.replace(secret, _MASK)
    for value in _longest_first(others):
        text = text.replace(value, _MASK)
    host = parts.hostname
    if host:
        text = re.sub(re.escape(host), _ADDRESS, text, flags=re.IGNORECASE)
    return text


def _longest_first(values: set[str]) -> list[str]:
    """``values`` longest first, ties in text order, so the order is fixed."""
    return sorted(values, key=lambda value: (-len(value), value))


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
