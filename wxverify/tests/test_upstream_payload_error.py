"""Tests for the decoded-payload diagnostic boundary (§9 O1-O13 of the
terminal-error-and-json-decode plan).

O1-O13 exercise ``decode_observations_payload`` through the real adapter
functions (``fetch_hourly_history``, ``validate_station``) under an
``httpx.MockTransport``-backed client — never by calling the decoder
directly — so a call site left unwired is caught.

Every station id, API key and header value below is synthetic
(``ISTATION0x``, ``SYNTHETIC-SECRET-KEY``, ``SYNTHETIC-CODE`` …) and no
request in this file crosses ``httpx.MockTransport`` to a real socket.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from wxverify.obs.config import RECENT_REFRESH_HOURS
from wxverify.obs.pws_adapter import (
    UpstreamPayloadError,
    fetch_hourly_history,
    observations_from_payload,
    validate_station,
)

_STATION_ID = "ISTATION01"
_API_KEY = "SYNTHETIC-SECRET-KEY"
_NOW = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# O1-O13 harness helpers
# ---------------------------------------------------------------------------


async def _fetch_hourly_via(
    handler: object, *, hours: int = RECENT_REFRESH_HOURS
) -> list[object]:
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:  # type: ignore[arg-type]
        return await fetch_hourly_history(
            _STATION_ID, _API_KEY, hours=hours, timezone="UTC", client=client
        )


def _run(coro: object) -> object:
    return asyncio.run(coro)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# O1
# ---------------------------------------------------------------------------


def test_empty_200_is_json_decode_empty() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"")

    with pytest.raises(UpstreamPayloadError) as info:
        _run(_fetch_hourly_via(handler))
    d = info.value.diagnostics
    assert d.kind == "json_decode"
    assert d.body == "empty"
    assert d.body_bytes == 0
    assert d.status == 200
    assert d.endpoint == "/v2/pws/observations/hourly/7day"
    assert d.station_id == _STATION_ID
    assert d.reason == "Expecting value"
    assert d.pos == 0
    assert isinstance(info.value.__cause__, json.JSONDecodeError)


# ---------------------------------------------------------------------------
# O2
# ---------------------------------------------------------------------------


def test_whitespace_body_is_its_own_body_kind() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b" \n\t ")

    with pytest.raises(UpstreamPayloadError) as info:
        _run(_fetch_hourly_via(handler))
    d = info.value.diagnostics
    assert d.body == "whitespace"
    assert d.body_bytes == 4
    assert d.kind == "json_decode"


# ---------------------------------------------------------------------------
# O3
# ---------------------------------------------------------------------------


def test_non_json_2xx_never_leaks_its_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"<html><body>SYNTHETIC-BODY</body></html>",
            headers={"content-type": "text/html; charset=utf-8"},
        )

    with pytest.raises(UpstreamPayloadError) as info:
        _run(_fetch_hourly_via(handler))
    d = info.value.diagnostics
    rendered = str(info.value)
    assert d.kind == "json_decode"
    assert d.body == "content"
    assert d.content_type == "text/html"
    assert "SYNTHETIC-BODY" not in rendered
    assert "<" not in rendered
    assert "SYNTHETIC-SECRET" not in rendered
    assert "?" not in d.endpoint
    assert "apiKey" not in rendered


# ---------------------------------------------------------------------------
# O4
# ---------------------------------------------------------------------------


def test_204_is_no_content_not_a_decode_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        response = httpx.Response(204)
        response.json = MagicMock(  # type: ignore[method-assign]
            side_effect=AssertionError("json() must not be called")
        )
        return response

    with pytest.raises(UpstreamPayloadError) as info:
        _run(_fetch_hourly_via(handler))
    d = info.value.diagnostics
    assert d.kind == "no_content"
    assert d.body == "empty"


# ---------------------------------------------------------------------------
# O5
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        b"null",
        b"[]",
        b'"x"',
        b"{}",
        b'{"observations": null}',
        b'{"observations": {}}',
    ],
)
def test_structural_shapes_are_failures_on_fetch_hourly_history(body: bytes) -> None:
    assert observations_from_payload(json.loads(body)) == [], (
        "precondition: the tolerant helper must still accept this shape — "
        "proving the decoder, not the helper, is what rejects it"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    with pytest.raises(UpstreamPayloadError) as info:
        _run(_fetch_hourly_via(handler))
    assert info.value.diagnostics.kind == "invalid_structure"


# ---------------------------------------------------------------------------
# O6
# ---------------------------------------------------------------------------


class TestProviderErrorEnvelope:
    def test_errors_list_is_classified_not_quoted(self) -> None:
        body = (
            b'{"errors": [{"error": {"code": "SYNTHETIC-CODE", '
            b'"message": "SYNTHETIC-MSG"}}]}'
        )

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=body)

        with pytest.raises(UpstreamPayloadError) as info:
            _run(_fetch_hourly_via(handler))
        rendered = str(info.value)
        assert info.value.diagnostics.kind == "provider_error"
        assert "SYNTHETIC-CODE" not in rendered
        assert "SYNTHETIC-MSG" not in rendered

    def test_error_object_is_also_classified(self) -> None:
        body = b'{"error": {"code": "SYNTHETIC-CODE"}}'

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=body)

        with pytest.raises(UpstreamPayloadError) as info:
            _run(_fetch_hourly_via(handler))
        assert info.value.diagnostics.kind == "provider_error"

    def test_observations_key_present_takes_precedence_over_errors(self) -> None:
        body = b'{"observations": [], "errors": []}'

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=body)

        result = _run(_fetch_hourly_via(handler))
        assert result == []


# ---------------------------------------------------------------------------
# O7
# ---------------------------------------------------------------------------


def test_valid_empty_observations_stays_a_success() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b'{"observations": []}')

    assert _run(_fetch_hourly_via(handler)) == []


# ---------------------------------------------------------------------------
# O8
# ---------------------------------------------------------------------------


def test_success_path_stays_untouched() -> None:
    payload = {
        "observations": [
            {
                "obsTimeUtc": "2026-07-10T11:30:00Z",
                "metric": {"temp": 18.3, "windSpeed": 12.0, "precipTotal": 0.5},
            }
        ]
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=json.dumps(payload).encode(),
            headers={"content-type": "application/json"},
        )

    import wxverify.obs.pws_adapter as pws_adapter_module

    with (
        patch("wxverify.obs.pws_adapter.utc_now", return_value=_NOW),
        patch(
            "wxverify.obs.pws_adapter._diagnose",
            wraps=pws_adapter_module._diagnose,
        ) as diagnose_spy,
    ):
        result = _run(_fetch_hourly_via(handler))
    assert len(result) == 2  # temperature + precip (first-row increment = total)
    # _diagnose must only build diagnostics on the raising path -- the
    # success path must never call it.
    diagnose_spy.assert_not_called()


# ---------------------------------------------------------------------------
# O9
# ---------------------------------------------------------------------------


class TestElapsedIsMeasuredWhenAvailable:
    def test_streamed_response_has_a_measured_elapsed(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, stream=httpx.ByteStream(b""))

        with pytest.raises(UpstreamPayloadError) as info:
            _run(_fetch_hourly_via(handler))
        d = info.value.diagnostics
        assert isinstance(d.elapsed_ms, int)
        assert d.elapsed_ms >= 0
        assert "elapsed_ms=-" not in str(info.value)

    def test_content_response_elapsed_is_none_not_a_crash(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"")

        with pytest.raises(UpstreamPayloadError) as info:
            _run(_fetch_hourly_via(handler))
        assert info.value.diagnostics.elapsed_ms is None
        assert "elapsed_ms=-" in str(info.value)


# ---------------------------------------------------------------------------
# O10
# ---------------------------------------------------------------------------


class TestRequestIdAllowlist:
    def test_wellformed_request_id_is_captured(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, content=b"", headers={"x-request-id": "req-synthetic-0001"}
            )

        with pytest.raises(UpstreamPayloadError) as info:
            _run(_fetch_hourly_via(handler))
        assert info.value.diagnostics.request_id == "req-synthetic-0001"

    def test_value_with_spaces_is_rejected(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, content=b"", headers={"x-request-id": "bad value with spaces"}
            )

        with pytest.raises(UpstreamPayloadError) as info:
            _run(_fetch_hourly_via(handler))
        assert info.value.diagnostics.request_id is None

    def test_overlong_value_is_rejected(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"", headers={"x-request-id": "a" * 129})

        with pytest.raises(UpstreamPayloadError) as info:
            _run(_fetch_hourly_via(handler))
        assert info.value.diagnostics.request_id is None

    def test_unlisted_header_name_is_never_captured(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, content=b"", headers={"x-other-id": "req-synthetic-0001"}
            )

        with pytest.raises(UpstreamPayloadError) as info:
            _run(_fetch_hourly_via(handler))
        assert info.value.diagnostics.request_id is None
        assert "req-synthetic-0001" not in str(info.value)


# ---------------------------------------------------------------------------
# O11
# ---------------------------------------------------------------------------


class TestContentTypeAllowlist:
    def test_recognized_type_strips_the_charset_parameter(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=b"",
                headers={"content-type": "application/json; charset=utf-8"},
            )

        with pytest.raises(UpstreamPayloadError) as info:
            _run(_fetch_hourly_via(handler))
        assert info.value.diagnostics.content_type == "application/json"

    def test_absent_header_renders_as_absent(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"")

        with pytest.raises(UpstreamPayloadError) as info:
            _run(_fetch_hourly_via(handler))
        assert info.value.diagnostics.content_type == "absent"

    def test_unparseable_value_is_never_echoed(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200, content=b"", headers={"content-type": "text/html <SYNTHETIC-HDR>"}
            )

        with pytest.raises(UpstreamPayloadError) as info:
            _run(_fetch_hourly_via(handler))
        assert info.value.diagnostics.content_type == "unrecognized"
        assert "SYNTHETIC-HDR" not in str(info.value)


# ---------------------------------------------------------------------------
# O12
# ---------------------------------------------------------------------------


class TestValidateStationSharesTheDecoder:
    def test_empty_body_raises_upstream_payload_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"")

        real_client = httpx.AsyncClient
        with (
            patch(
                "wxverify.obs.pws_adapter.httpx.AsyncClient",
                lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),
            ),
            pytest.raises(UpstreamPayloadError) as info,
        ):
            _run(validate_station(_STATION_ID, _API_KEY))
        d = info.value.diagnostics
        assert d.endpoint == "/v2/pws/observations/current"
        assert d.kind == "json_decode"

    def test_empty_observations_list_keeps_the_runtime_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b'{"observations": []}')

        real_client = httpx.AsyncClient
        with (
            patch(
                "wxverify.obs.pws_adapter.httpx.AsyncClient",
                lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),
            ),
            pytest.raises(RuntimeError, match="no current observation"),
        ):
            _run(validate_station(_STATION_ID, _API_KEY))


# ---------------------------------------------------------------------------
# O13
# ---------------------------------------------------------------------------


def test_utf16_bom_decodes_to_an_empty_document_as_json_decode() -> None:
    """``\\xff\\xfe`` is the UTF-16-LE BOM: ``json.detect_encoding`` picks
    utf-16, decodes to ``""``, and ``response.json()`` raises
    ``json.JSONDecodeError("Expecting value")`` — the same branch as an
    empty body, never the non-JSONDecodeError ``ValueError`` branch below.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"\xff\xfe")

    with pytest.raises(UpstreamPayloadError) as info:
        _run(_fetch_hourly_via(handler))
    rendered = str(info.value)
    assert info.value.diagnostics.kind == "json_decode"
    assert "0xff" not in rendered
    assert "can't decode" not in rendered


def test_invalid_utf8_renders_a_class_name_not_codec_text() -> None:
    """A lone continuation byte is not a valid UTF-8 start byte, so
    ``response.json()`` raises ``UnicodeDecodeError`` (a ``ValueError`` that
    is NOT a ``JSONDecodeError``) — exercising the ``reason =
    type(cause).__name__`` branch in ``_diagnose``, distinct from the BOM
    case above.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"\x80")

    with pytest.raises(UpstreamPayloadError) as info:
        _run(_fetch_hourly_via(handler))
    d = info.value.diagnostics
    rendered = str(info.value)
    assert d.kind == "json_decode"
    assert d.reason == "UnicodeDecodeError"
    assert "0x80" not in rendered
    assert "can't decode" not in rendered
    assert "0x80" not in d.render()
    assert "can't decode" not in d.render()
