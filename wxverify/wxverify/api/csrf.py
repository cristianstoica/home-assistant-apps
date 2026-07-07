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
    response.set_cookie(
        "csrf",
        pair.nonce,
        path=_cookie_path(root_path),
        samesite="strict",
        httponly=False,
    )


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
