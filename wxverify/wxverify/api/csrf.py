"""Signed double-submit CSRF tokens."""

from __future__ import annotations

import hmac
import re
import secrets
from dataclasses import dataclass
from typing import Final

from fastapi import Request, Response

_CSRF_KEY: bytes = secrets.token_bytes(32)
_NONCE_BYTES: Final = 32
# secrets.token_urlsafe(32) is 32 random bytes in URL-safe base64 with the
# "=" padding stripped: always exactly 43 characters from A-Z a-z 0-9 - _.
_NONCE_RE: Final = re.compile(r"[A-Za-z0-9_-]{43}")


@dataclass(frozen=True)
class CsrfPair:
    nonce: str
    token: str


def _sign(nonce: str) -> str:
    return hmac.new(_CSRF_KEY, nonce.encode("utf-8"), "sha256").hexdigest()


def issue_csrf_pair() -> CsrfPair:
    return _pair(secrets.token_urlsafe(_NONCE_BYTES))


def csrf_pair_for(request: Request) -> CsrfPair:
    """The pair for a response: the cookie's nonce when it is well formed,
    signed with the current key; otherwise a new nonce.

    Reusing the nonce keeps every open tab's token valid when another tab
    loads a page. After a restart the key is new, so an old page's token
    fails and the next page load re-signs the same nonce. Known gap: two
    requests on a first visit, made before either has the cookie, can still
    get different nonces, and the first one's page then needs a reload.
    """
    nonce = request.cookies.get("csrf")
    if nonce is not None and _NONCE_RE.fullmatch(nonce) is not None:
        return _pair(nonce)
    return issue_csrf_pair()


def _pair(nonce: str) -> CsrfPair:
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
