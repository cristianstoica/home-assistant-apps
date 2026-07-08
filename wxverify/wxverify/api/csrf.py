"""Signed double-submit CSRF tokens."""

from __future__ import annotations

import hmac
import secrets
from dataclasses import dataclass

from fastapi import Request, Response

_CSRF_KEY: bytes = secrets.token_bytes(32)


@dataclass(frozen=True)
class CsrfPair:
    nonce: str
    token: str


def _sign(nonce: str) -> str:
    return hmac.new(_CSRF_KEY, nonce.encode("utf-8"), "sha256").hexdigest()


def issue_csrf_pair() -> CsrfPair:
    nonce = secrets.token_urlsafe(32)
    return CsrfPair(nonce=nonce, token=f"{nonce}.{_sign(nonce)}")


def set_csrf_cookie(response: Response, pair: CsrfPair, root_path: str) -> None:
    path = _cookie_path(root_path)
    response.set_cookie(
        "csrf",
        pair.nonce,
        path=path,
        samesite="strict",
        httponly=False,
    )
    if path != "/":
        # 0.1.0 issued this cookie at path "/". A stale copy left over from an
        # upgrade shadows the path-scoped cookie (the server keeps the last
        # occurrence while browsers send the most-specific path first), which
        # would fail CSRF validation until the browser is restarted.
        response.delete_cookie("csrf", path="/")


def validate_csrf(request: Request) -> bool:
    sent = request.headers.get("x-csrf-token")
    nonce = request.cookies.get("csrf")
    if sent is None or nonce is None:
        return False
    expected = f"{nonce}.{_sign(nonce)}"
    return hmac.compare_digest(sent, expected)


def _cookie_path(root_path: str) -> str:
    normalized = (root_path or "").rstrip("/")
    return normalized or "/"
