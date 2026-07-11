"""Tests for wxverify.api.discovery.publish_discovery.

publish_discovery is a fire-and-forget startup side-effect:
  - SUPERVISOR_TOKEN absent in env → silent no-op, zero HTTP calls made.
  - Token present + Supervisor returns 200 {"result":"ok"} → exactly one POST
    to http://supervisor/discovery with the expected Authorization header and
    JSON body.
  - Transport failure (connect error) → returns normally, no exception raised.
  - Non-200 status → returns normally, no exception raised.
  - Non-"ok" result body → returns normally, no exception raised.

The function owns its httpx.AsyncClient (``async with httpx.AsyncClient()``),
so the tests patch ``wxverify.api.discovery.httpx.AsyncClient`` to inject a
client backed by ``httpx.MockTransport``.  The same approach is used in
test_m1_m5.py for adapters that own their client.

socket.gethostname is monkeypatched to a fixed string for determinism.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from wxverify.api.discovery import publish_discovery

# ---------------------------------------------------------------------------
# Test constants — synthetic placeholder data only
# ---------------------------------------------------------------------------

_FAKE_TOKEN = "synthetic-supervisor-token-01"
_FAKE_HOSTNAME = "wxverify-test-host"
_DISCOVERY_URL = "http://supervisor/discovery"
_SERVICE_NAME = "wxverify"
_TEST_PORT = 8099


# ---------------------------------------------------------------------------
# Shared fixture builder
# ---------------------------------------------------------------------------


def _mock_client_class(
    handler: object,
) -> Any:
    """Return a patched httpx.AsyncClient class backed by MockTransport.

    publish_discovery uses ``async with httpx.AsyncClient() as client``.  We
    patch the class so that the ``async with`` block yields an AsyncClient
    wired to our MockTransport — capturing the outgoing request without any
    real network activity.
    """
    real_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]

    class _ContextManager:
        async def __aenter__(self) -> httpx.AsyncClient:
            return real_client

        async def __aexit__(self, *args: object) -> None:
            await real_client.aclose()

    mock_cls = MagicMock()
    mock_cls.return_value = _ContextManager()
    return mock_cls


# ---------------------------------------------------------------------------
# 1. SUPERVISOR_TOKEN absent — silent no-op
# ---------------------------------------------------------------------------


class TestTokenAbsent:
    """Without SUPERVISOR_TOKEN, publish_discovery returns without HTTP calls."""

    def test_no_token_no_http_call(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """SUPERVISOR_TOKEN absent → returns immediately, no POST made.

        Injected precondition: monkeypatch.delenv removes the token from the
        process environment for this test only.  Paired positive:
        test_token_present_posts_to_supervisor, which proves the POST IS made
        when the token is present — so this negative cannot pass vacuously if
        the POST branch is dead.
        """
        monkeypatch.delenv("SUPERVISOR_TOKEN", raising=False)

        call_log: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            call_log.append(request)
            return httpx.Response(200, json={"result": "ok"})

        with patch(
            "wxverify.api.discovery.httpx.AsyncClient",
            _mock_client_class(handler),
        ):
            asyncio.run(publish_discovery(_TEST_PORT))

        assert call_log == [], (
            "publish_discovery must make zero HTTP calls "
            "when SUPERVISOR_TOKEN is absent"
        )

    def test_no_token_does_not_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """SUPERVISOR_TOKEN absent → no exception propagates."""
        monkeypatch.delenv("SUPERVISOR_TOKEN", raising=False)

        # No patch needed — we just want to confirm no exception escapes.
        asyncio.run(publish_discovery(_TEST_PORT))


# ---------------------------------------------------------------------------
# 2. Token present + 200 {"result": "ok"} — exactly one POST with correct shape
# ---------------------------------------------------------------------------


class TestTokenPresentSuccess:
    """Token present + Supervisor returns 200 ok → one POST with correct shape."""

    def test_posts_to_supervisor_discovery_url(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Exactly one POST to http://supervisor/discovery when token is set.

        Paired positive for TestTokenAbsent.test_no_token_no_http_call.
        """
        monkeypatch.setenv("SUPERVISOR_TOKEN", _FAKE_TOKEN)
        monkeypatch.setattr("socket.gethostname", lambda: _FAKE_HOSTNAME)

        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(200, json={"result": "ok"})

        with patch(
            "wxverify.api.discovery.httpx.AsyncClient",
            _mock_client_class(handler),
        ):
            asyncio.run(publish_discovery(_TEST_PORT))

        assert len(captured) == 1, (
            "publish_discovery must make exactly one POST when token is present"
        )
        assert str(captured[0].url) == _DISCOVERY_URL, (
            f"POST must target {_DISCOVERY_URL!r}, got {captured[0].url!r}"
        )
        assert captured[0].method == "POST"

    def test_authorization_header_carries_bearer_token(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Authorization: Bearer <token> header present in the POST."""
        monkeypatch.setenv("SUPERVISOR_TOKEN", _FAKE_TOKEN)
        monkeypatch.setattr("socket.gethostname", lambda: _FAKE_HOSTNAME)

        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(200, json={"result": "ok"})

        with patch(
            "wxverify.api.discovery.httpx.AsyncClient",
            _mock_client_class(handler),
        ):
            asyncio.run(publish_discovery(_TEST_PORT))

        assert len(captured) == 1
        auth = captured[0].headers.get("authorization", "")
        assert auth == f"Bearer {_FAKE_TOKEN}", (
            f"Authorization header must be 'Bearer {_FAKE_TOKEN}', got {auth!r}"
        )

    def test_json_body_contains_service_host_and_port(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """POST body is JSON with service=wxverify, host=hostname, port=<port>."""
        monkeypatch.setenv("SUPERVISOR_TOKEN", _FAKE_TOKEN)
        monkeypatch.setattr("socket.gethostname", lambda: _FAKE_HOSTNAME)

        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(200, json={"result": "ok"})

        with patch(
            "wxverify.api.discovery.httpx.AsyncClient",
            _mock_client_class(handler),
        ):
            asyncio.run(publish_discovery(_TEST_PORT))

        assert len(captured) == 1
        body = json.loads(captured[0].content)
        assert body["service"] == _SERVICE_NAME, (
            f"body.service must be {_SERVICE_NAME!r}, got {body.get('service')!r}"
        )
        assert body["config"]["host"] == _FAKE_HOSTNAME, (
            f"body.config.host must be {_FAKE_HOSTNAME!r}, "
            f"got {body.get('config', {}).get('host')!r}"
        )
        assert body["config"]["port"] == _TEST_PORT, (
            f"body.config.port must be {_TEST_PORT}, "
            f"got {body.get('config', {}).get('port')!r}"
        )

    def test_success_does_not_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Token present + 200 ok → publish_discovery returns without raising."""
        monkeypatch.setenv("SUPERVISOR_TOKEN", _FAKE_TOKEN)
        monkeypatch.setattr("socket.gethostname", lambda: _FAKE_HOSTNAME)

        def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
            return httpx.Response(200, json={"result": "ok"})

        with patch(
            "wxverify.api.discovery.httpx.AsyncClient",
            _mock_client_class(handler),
        ):
            asyncio.run(publish_discovery(_TEST_PORT))

    def test_port_reflected_in_body(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """publish_discovery(port) propagates the caller-supplied port into the body.

        Verifies the port param is used, not a hardcoded constant.
        """
        monkeypatch.setenv("SUPERVISOR_TOKEN", _FAKE_TOKEN)
        monkeypatch.setattr("socket.gethostname", lambda: _FAKE_HOSTNAME)

        for port in (8099, 8080, 3000):
            captured: list[httpx.Request] = []
            _captured = captured  # bind for closure

            def _make_handler(
                sink: list[httpx.Request],
            ):  # noqa: ANN202
                def handler(request: httpx.Request) -> httpx.Response:
                    sink.append(request)
                    return httpx.Response(200, json={"result": "ok"})

                return handler

            with patch(
                "wxverify.api.discovery.httpx.AsyncClient",
                _mock_client_class(_make_handler(_captured)),
            ):
                asyncio.run(publish_discovery(port))

            body = json.loads(_captured[0].content)
            assert body["config"]["port"] == port, (
                f"port {port} must appear in body.config.port"
            )


# ---------------------------------------------------------------------------
# 3. HTTP transport failure — fail-open, no exception propagates
# ---------------------------------------------------------------------------


class TestTransportFailure:
    """Transport-level errors return normally (fail-open: startup is never blocked)."""

    def test_connect_error_does_not_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ConnectError from the Supervisor POST → publish_discovery returns normally.

        Fail-open guarantee: the add-on must start even when the Supervisor is
        unreachable.  Injected precondition: MockTransport raises ConnectError.
        Paired with test_posts_to_supervisor_discovery_url (positive), which
        confirms the POST IS made when the transport succeeds.
        """
        monkeypatch.setenv("SUPERVISOR_TOKEN", _FAKE_TOKEN)
        monkeypatch.setattr("socket.gethostname", lambda: _FAKE_HOSTNAME)

        def error_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        with patch(
            "wxverify.api.discovery.httpx.AsyncClient",
            _mock_client_class(error_handler),
        ):
            # Must not raise — the fail-open guarantee.
            asyncio.run(publish_discovery(_TEST_PORT))

    def test_timeout_error_does_not_raise(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """TimeoutException from the Supervisor POST → returns normally."""
        monkeypatch.setenv("SUPERVISOR_TOKEN", _FAKE_TOKEN)
        monkeypatch.setattr("socket.gethostname", lambda: _FAKE_HOSTNAME)

        def error_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("timed out", request=request)

        with patch(
            "wxverify.api.discovery.httpx.AsyncClient",
            _mock_client_class(error_handler),
        ):
            asyncio.run(publish_discovery(_TEST_PORT))


# ---------------------------------------------------------------------------
# 4. Non-200 response — fail-open
# ---------------------------------------------------------------------------


class TestNon200Response:
    """Non-200 Supervisor responses return normally (fail-open)."""

    @pytest.mark.parametrize(
        "status",
        [404, 500, 503],
        ids=["404", "500", "503"],
    )
    def test_non_200_does_not_raise(
        self, monkeypatch: pytest.MonkeyPatch, status: int
    ) -> None:
        """Supervisor returns a non-200 status → publish_discovery returns normally.

        The Supervisor API is best-effort: an unexpected status must never
        prevent the add-on from completing startup.
        """
        monkeypatch.setenv("SUPERVISOR_TOKEN", _FAKE_TOKEN)
        monkeypatch.setattr("socket.gethostname", lambda: _FAKE_HOSTNAME)

        def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
            return httpx.Response(
                status_code=status,
                content=b"",
                request=httpx.Request("POST", _DISCOVERY_URL),
            )

        with patch(
            "wxverify.api.discovery.httpx.AsyncClient",
            _mock_client_class(handler),
        ):
            asyncio.run(publish_discovery(_TEST_PORT))


# ---------------------------------------------------------------------------
# 5. 200 with unparseable body — fail-open (guards the ValueError branch)
# ---------------------------------------------------------------------------


class TestMalformed200Body:
    """200 response with a non-JSON body returns normally (fail-open).

    These tests pin the try/except ValueError guard added around response.json()
    in publish_discovery.  Before the fix, any 200 with a non-JSON body raised
    json.JSONDecodeError through the app lifespan and aborted startup.

    Paired negative (this class) + positive (TestNonOkResult / TestTokenPresentSuccess):
    the positive confirms the POST and JSON-parse path are exercised when the
    body IS valid JSON — so these negatives cannot pass vacuously if that branch
    is dead.
    """

    @pytest.mark.parametrize(
        ("content", "description"),
        [
            (b"", "empty-body"),
            (b"<html>error</html>", "html-body"),
        ],
        ids=["empty-body", "html-body"],
    )
    def test_200_unparseable_body_does_not_raise(
        self,
        monkeypatch: pytest.MonkeyPatch,
        content: bytes,
        description: str,
    ) -> None:
        """200 with a non-JSON body → publish_discovery returns normally.

        Injected precondition: MockTransport returns status 200 with
        ``content`` as the raw response bytes, which httpx cannot parse as
        JSON.  The ValueError guard must catch the parse failure and return
        without propagating an exception.
        """
        monkeypatch.setenv("SUPERVISOR_TOKEN", _FAKE_TOKEN)
        monkeypatch.setattr("socket.gethostname", lambda: _FAKE_HOSTNAME)

        def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
            return httpx.Response(
                status_code=200,
                content=content,
                request=httpx.Request("POST", _DISCOVERY_URL),
            )

        with patch(
            "wxverify.api.discovery.httpx.AsyncClient",
            _mock_client_class(handler),
        ):
            asyncio.run(publish_discovery(_TEST_PORT))


# ---------------------------------------------------------------------------
# 6. Non-"ok" result body — fail-open
# ---------------------------------------------------------------------------


class TestNonOkResult:
    """200 response with a non-"ok" result body returns normally (fail-open)."""

    @pytest.mark.parametrize(
        "body",
        [
            {"result": "error", "message": "unknown service"},
            {"result": None},
            {},
            {"something_else": True},
        ],
        ids=["result-error", "result-null", "empty-body", "unknown-key"],
    )
    def test_non_ok_result_does_not_raise(
        self, monkeypatch: pytest.MonkeyPatch, body: dict[str, object]
    ) -> None:
        """200 response with result != "ok" → publish_discovery returns normally.

        The Supervisor may return {"result":"error"} for unknown services;
        the add-on must not crash.
        """
        monkeypatch.setenv("SUPERVISOR_TOKEN", _FAKE_TOKEN)
        monkeypatch.setattr("socket.gethostname", lambda: _FAKE_HOSTNAME)

        def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
            return httpx.Response(200, json=body)

        with patch(
            "wxverify.api.discovery.httpx.AsyncClient",
            _mock_client_class(handler),
        ):
            asyncio.run(publish_discovery(_TEST_PORT))
