"""Keep the CSRF nonce across tabs (card 21, 0.16.19).

``render`` and ``/api/csrf`` reuse the cookie's nonce (``csrf_pair_for``)
instead of minting a fresh one on every page load, so a second tab's token
stays valid after the first tab reloads. A restart still rotates the
server-side signing key, so an old page's token is refused and the next
page load re-signs the same nonce for every open tab at once.

One ``TestClient`` per test (one cookie jar, like one browser), built the
way ``tests/test_rain_threshold_display.py`` builds its app. Synthetic
fixtures only: a made-up site name, ``UTC`` timezone, an obviously fake
ingress token.
"""

from __future__ import annotations

import asyncio
import re
import secrets
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api import csrf
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db, init_db

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_rain_threshold_display.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


# The meta tag base.html writes the page's CSRF token into.
_CSRF_META = re.compile(r'<meta name="csrf-token" content="([^"]*)">')

# secrets.token_urlsafe(32): 32 random bytes, URL-safe base64, "=" stripped --
# always exactly 43 characters from A-Z a-z 0-9 - _.
_NONCE_RE = re.compile(r"[A-Za-z0-9_-]{43}")


def _page_token(html: str) -> str:
    match = _CSRF_META.search(html)
    assert match is not None, 'no <meta name="csrf-token"> tag in the page'
    return match.group(1)


def _mutation_headers(token: str) -> dict[str, str]:
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


def _site_payload(name: str) -> dict[str, object]:
    return {
        "name": name,
        "forecast_lat": 40.0,
        "forecast_lon": -105.0,
        "elevation_m": 900.0,
        "timezone": "UTC",
    }


def _site_row_exists(name: str) -> bool:
    row = get_db().read_sync(
        lambda conn: conn.execute(
            "SELECT 1 FROM sites WHERE name = ?", (name,)
        ).fetchone()
    )
    return row is not None


# ---------------------------------------------------------------------------
# C1 -- two tabs.
# ---------------------------------------------------------------------------


def test_two_tabs_share_one_nonce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response_a = client.get("/sites")
        cookie_after_a = client.cookies.get("csrf")
        token_a = _page_token(response_a.text)

        response_b = client.get("/sites")
        cookie_after_b = client.cookies.get("csrf")
        token_b = _page_token(response_b.text)

        # K1: `render` back to `issue_csrf_pair()` mints a fresh nonce on the
        # second GET, so the cookie would differ here and the tokens below
        # would not match.
        assert cookie_after_a == cookie_after_b, (
            f"cookie changed between two GETs: correct = same value, "
            f"mutant = {cookie_after_a!r} then {cookie_after_b!r}"
        )
        assert token_a == token_b, (
            f"second tab's page token diverged: correct = {token_a!r}, "
            f"mutant = {token_b!r}"
        )

        response = client.post(
            "/api/sites",
            json=_site_payload("Two Tabs Site"),
            headers=_mutation_headers(token_a),
        )
        site_exists = _site_row_exists("Two Tabs Site")
    assert response.status_code == 200, response.text
    assert site_exists


# ---------------------------------------------------------------------------
# C2 -- first visit.
# ---------------------------------------------------------------------------


def test_first_visit_issues_a_well_formed_fresh_nonce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/sites")
        cookie = client.cookies.get("csrf")
        assert cookie is not None
        assert _NONCE_RE.fullmatch(cookie) is not None, (
            f"cookie is not a 43-char urlsafe nonce: {cookie!r}"
        )
        token = _page_token(response.text)
        assert token == f"{cookie}.{token.split('.', 1)[1]}"
        signature = token.split(".", 1)[1]
        assert re.fullmatch(r"[0-9a-f]{64}", signature) is not None, signature

    with TestClient(app) as other_client:
        other_client.get("/sites")
        other_cookie = other_client.cookies.get("csrf")

    assert other_cookie is not None and _NONCE_RE.fullmatch(other_cookie) is not None, (
        other_cookie
    )
    assert other_cookie != cookie, (
        f"two fresh clients got the same nonce: correct = different values, "
        f"mutant = both {cookie!r}"
    )


# ---------------------------------------------------------------------------
# C3 -- restart.
# ---------------------------------------------------------------------------


def test_nonce_survives_a_key_restart_but_stale_token_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response_a = client.get("/sites")
        token_a = _page_token(response_a.text)
        nonce_before = client.cookies.get("csrf")

        # Simulate a process restart: the signing key is replaced, the nonce
        # (stored client-side in the cookie) is not.
        monkeypatch.setattr(csrf, "_CSRF_KEY", secrets.token_bytes(32))

        stale_response = client.post(
            "/api/sites",
            json=_site_payload("Restart Stale Site"),
            headers=_mutation_headers(token_a),
        )
        # K4: a cache on `_sign` would keep answering with the pre-restart
        # signature, so the stale token would still validate here.
        assert stale_response.status_code == 403, stale_response.text
        assert not _site_row_exists("Restart Stale Site")

        response_r = client.get("/sites")
        token_r = _page_token(response_r.text)

        # K1 (reissuing a fresh nonce on reuse) would also change the cookie
        # and the token's nonce half here, and a restart that forgot to
        # re-sign the kept nonce would leave token_r == token_a.
        assert client.cookies.get("csrf") == nonce_before, (
            f"nonce changed across the restart: correct = {nonce_before!r}, "
            f"mutant = {client.cookies.get('csrf')!r}"
        )
        assert token_r.split(".", 1)[0] == nonce_before, (
            f"post-restart token's nonce half diverged: correct = "
            f"{nonce_before!r}, mutant = {token_r.split('.', 1)[0]!r}"
        )
        assert token_r != token_a, (
            f"post-restart token matches the pre-restart token: correct = "
            f"a re-signed token != {token_a!r}, mutant = {token_r!r}"
        )

        response_s = client.get("/sites")
        token_s = _page_token(response_s.text)

        # K1: reissuing a fresh nonce on every GET would also change the
        # nonce across these two post-restart loads, so R and S would differ.
        assert token_r == token_s, (
            f"token changed across two post-restart GETs: correct = same "
            f"value, mutant = {token_r!r} then {token_s!r}"
        )

        response_r_post = client.post(
            "/api/sites",
            json=_site_payload("Restart Recovered Site R"),
            headers=_mutation_headers(token_r),
        )
        response_s_post = client.post(
            "/api/sites",
            json=_site_payload("Restart Recovered Site S"),
            headers=_mutation_headers(token_s),
        )
    assert response_r_post.status_code == 200, response_r_post.text
    assert response_s_post.status_code == 200, response_s_post.text


# ---------------------------------------------------------------------------
# C4 -- forged signature.
# ---------------------------------------------------------------------------


def test_forged_signature_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        client.get("/sites")
        nonce = client.cookies.get("csrf")
        assert nonce is not None
        forged_token = f"{nonce}." + "0" * 64

        response = client.post(
            "/api/sites",
            json=_site_payload("Forged Signature Site"),
            headers=_mutation_headers(forged_token),
        )
        site_exists = _site_row_exists("Forged Signature Site")
    # K5: comparing only the nonce half (`sent.split(".")[0] == nonce`) would
    # accept this forged signature.
    assert response.status_code == 403, response.text
    assert response.json()["error"] == "bad csrf token"
    assert not site_exists


# ---------------------------------------------------------------------------
# C5 -- malformed cookie.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("a" * 42, id="42-chars-too-short"),
        pytest.param("a" * 43 + "A", id="44-chars-valid-nonce-plus-one"),
        pytest.param("a" * 21 + "." + "a" * 21, id="43-chars-with-one-dot"),
        pytest.param("a" * 21 + "=" + "a" * 21, id="43-chars-with-one-equals"),
    ],
)
def test_malformed_cookie_is_replaced_with_a_fresh_nonce(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    request: pytest.FixtureRequest,
) -> None:
    case_id = request.node.callspec.id
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        # domain must match the one http.cookiejar assigns to the app's own
        # Set-Cookie response: TestClient's host "testserver" has no dot, so
        # eff_request_host() (cpython http/cookiejar.py) stores it under
        # "testserver.local" -- a different domain here would leave the jar
        # holding both cookies and client.cookies.get raising CookieConflict.
        client.cookies.set("csrf", value, domain="testserver.local")
        response = client.get("/sites")
        new_cookie = client.cookies.get("csrf")

        # K2 (any value reused) and K3 (`match` instead of `fullmatch`,
        # defeated only by the 44-char case) would keep `value` as the
        # cookie instead of replacing it.
        assert new_cookie != value, (
            f"malformed cookie {case_id} was reused verbatim: "
            f"correct = a fresh nonce, mutant = {value!r} kept"
        )
        assert new_cookie is not None
        assert _NONCE_RE.fullmatch(new_cookie) is not None, new_cookie

        token = _page_token(response.text)
        post_response = client.post(
            "/api/sites",
            json=_site_payload(f"Malformed Cookie Site {case_id}"),
            headers=_mutation_headers(token),
        )
        site_exists = _site_row_exists(f"Malformed Cookie Site {case_id}")
    assert post_response.status_code == 200, post_response.text
    assert site_exists


# ---------------------------------------------------------------------------
# C6 -- ingress path.
# ---------------------------------------------------------------------------

_SUPERVISOR_IP = "172.30.32.2"
_INGRESS_PREFIX = "/api/hassio_ingress/synthetic-token"


def test_ingress_path_cookie_keeps_the_prefixed_path_and_deletes_the_root_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app, client=(_SUPERVISOR_IP, 4321)) as client:
        first = client.get("/sites", headers={"X-Ingress-Path": _INGRESS_PREFIX})
        nonce = client.cookies.get("csrf")
        assert nonce is not None

        # The jar scopes the cookie to the ingress-prefixed path, so a GET to
        # the unprefixed test path would not resend it: pass it explicitly.
        second = client.get(
            "/sites",
            headers={
                "X-Ingress-Path": _INGRESS_PREFIX,
                "Cookie": f"csrf={nonce}",
            },
        )
    cookie_headers = second.headers.get_list("set-cookie")
    assert any(
        f"csrf={nonce}" in h and f"Path={_INGRESS_PREFIX}" in h for h in cookie_headers
    ), (
        # K6: skipping `set_csrf_cookie` on reuse would leave no matching
        # Set-Cookie header here at all.
        f"no prefixed Set-Cookie for the reused nonce: {cookie_headers!r}"
    )
    assert any(
        "Path=/" in h and ("Max-Age=0" in h or "expires=" in h) for h in cookie_headers
    ), f"no stale Path=/ delete: {cookie_headers!r}"
    assert first.status_code == 200


# ---------------------------------------------------------------------------
# C7 -- /api/csrf.
# ---------------------------------------------------------------------------


def test_api_csrf_endpoint_reuses_the_page_nonce(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response_a = client.get("/sites")
        cookie_after_page = client.cookies.get("csrf")
        token_a = _page_token(response_a.text)

        api_response = client.get("/api/csrf")
        cookie_after_api = client.cookies.get("csrf")

        # K7: `/api/csrf` back to `issue_csrf_pair()` would mint a new nonce
        # here, changing the cookie.
        assert cookie_after_api == cookie_after_page, (
            f"cookie changed after /api/csrf: correct = {cookie_after_page!r}, "
            f"mutant = {cookie_after_api!r}"
        )
        assert api_response.json()["csrf_token"] == token_a

        post_response = client.post(
            "/api/sites",
            json=_site_payload("Api Csrf Reuse Site"),
            headers=_mutation_headers(token_a),
        )
        site_exists = _site_row_exists("Api Csrf Reuse Site")
    assert post_response.status_code == 200, post_response.text
    assert site_exists
