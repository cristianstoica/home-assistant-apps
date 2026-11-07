"""Tests for wxverify.core.error_sanitize: sanitized_exception and safe_detail.

O14 — sanitized_exception never returns an empty string, even for an
exception whose str() is empty (httpx maps a bare TimeoutError() this way
under httpx 0.28.1; see the plan's ground truth at
error_sanitize.py:16-19). O15 — exception notes (PEP 678, add_note) are
appended and redacted the same way the base message is. O16 — a rendered
UpstreamPayloadError line survives sanitization byte-for-byte. O22(b) pins
the same empty-text guarantee at the real ``jobs.last_error`` column via
``fail()``. ``TestSafeDetail`` pins the never-raising render both task-death
surfaces use: the class name leads and appears once, the message and notes
are the sanitizer's own redacted text, and a raising ``__str__`` degrades to
the class name alone.

Synthetic data only: ``SYNTHETIC-SECRET-KEY`` stands in for a real API key.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

import httpx
import pytest

from wxverify import config
from wxverify.core.error_sanitize import (
    _http_status_error,  # noqa: PLC2701 - pins the no-reason byte-for-byte text
    provider_reason,
    safe_detail,
    sanitized_exception,
)
from wxverify.db.connection import close_db, init_db
from wxverify.db.queue import enqueue_if_absent, fail
from wxverify.obs.pws_adapter import decode_observations_payload

_STATION_ID = "ISTATION01"
_API_KEY = "SYNTHETIC-SECRET-KEY"

# ---------------------------------------------------------------------------
# Part F (plan §6): provider_reason -- shared fixtures (its own request-
# building style, host/key/station all made up).
# ---------------------------------------------------------------------------

_PF_HOST = "api.example.test"
_PF_KEY = "SYNTHKEY0123456789abcdef"
_PF_STATION = "ISTATIONA1"
_PF_REQUEST = httpx.Request(
    "GET",
    f"https://{_PF_HOST}/v1/obs?apiKey={_PF_KEY}&stationId={_PF_STATION}&format=json",
)


def _pf_response(
    status: int, body: bytes, *, request: httpx.Request = _PF_REQUEST, **headers: str
) -> httpx.Response:
    return httpx.Response(status, content=body, request=request, headers=headers)


def _pf_json_response(status: int, payload: object, **headers: str) -> httpx.Response:
    return _pf_response(status, json.dumps(payload).encode(), **headers)


def _pf_padded_body(message: str, *, exact_bytes: int) -> bytes:
    """A JSON body of ``exact_bytes``: ``message`` plus an ASCII ``pad``
    field sized to hit the exact length (padding is plain characters, so
    each one adds exactly one byte)."""
    base = json.dumps({"message": message, "pad": ""}).encode()
    pad_len = exact_bytes - len(base)
    assert pad_len >= 0, f"exact_bytes too small: need at least {len(base)}"
    return json.dumps({"message": message, "pad": "P" * pad_len}).encode()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


# ---------------------------------------------------------------------------
# O14 — sanitized_exception never returns empty
# ---------------------------------------------------------------------------


class TestSanitizedExceptionNeverEmpty:
    def test_read_timeout_empty_text_falls_back_to_class_name(self) -> None:
        assert sanitized_exception(httpx.ReadTimeout("")) == "ReadTimeout"

    def test_bare_timeout_error_falls_back_to_class_name(self) -> None:
        assert sanitized_exception(TimeoutError()) == "TimeoutError"

    def test_whitespace_only_message_falls_back_to_class_name(self) -> None:
        assert sanitized_exception(RuntimeError("  ")) == "RuntimeError"

    def test_non_empty_message_is_unchanged(self) -> None:
        assert sanitized_exception(RuntimeError("boom")) == "boom"

    def test_cancelled_error_falls_back_to_class_name(self) -> None:
        assert sanitized_exception(asyncio.CancelledError()) == "CancelledError"


# ---------------------------------------------------------------------------
# O15 — notes are appended and redacted
# ---------------------------------------------------------------------------


class TestSanitizedExceptionAppendsNotes:
    def test_note_is_appended_to_the_base_message(self) -> None:
        exc = RuntimeError("boom")
        exc.add_note("station=ISTATION01 progress=2/3")
        assert sanitized_exception(exc) == "boom station=ISTATION01 progress=2/3"

    def test_note_carrying_a_secret_url_is_redacted(self) -> None:
        exc = RuntimeError("boom")
        exc.add_note(f"see https://api.example.com/x?apiKey={_API_KEY}")
        result = sanitized_exception(exc)
        assert _API_KEY not in result, (
            "a secret query value in a note must be redacted the same way "
            "the base message is"
        )
        assert "apiKey=%2A%2A%2A" in result, (
            "the note's secret query value must be REDACTED (urlencoded ***), "
            "not merely dropped"
        )

    def test_http_status_error_keeps_its_prefix_and_gains_the_note(self) -> None:
        request = httpx.Request(
            "GET",
            f"https://api.weather.com/v2/pws/observations/current?apiKey={_API_KEY}",
        )
        response = httpx.Response(503, request=request)
        exc = httpx.HTTPStatusError(
            "service unavailable", request=request, response=response
        )
        exc.add_note("station=ISTATION01 progress=0/1")
        result = sanitized_exception(exc)
        assert result.startswith("HTTP 503")
        assert result.endswith("station=ISTATION01 progress=0/1")
        assert _API_KEY not in result


# ---------------------------------------------------------------------------
# O16 — the decoded-payload line survives sanitization intact
# ---------------------------------------------------------------------------


def test_upstream_payload_error_survives_sanitization_intact() -> None:
    request = httpx.Request(
        "GET",
        f"https://api.weather.com/v2/pws/observations/hourly/7day"
        f"?stationId={_STATION_ID}&apiKey={_API_KEY}",
    )
    response = httpx.Response(
        200,
        content=b"<html><body>SYNTHETIC-BODY</body></html>",
        headers={"content-type": "text/html; charset=utf-8"},
        request=request,
    )
    exc = None
    try:
        decode_observations_payload(response, station_id=_STATION_ID)
    except Exception as raised:  # noqa: BLE001 - captured for str() comparison
        exc = raised
    assert exc is not None
    rendered = str(exc)
    assert rendered != ""
    assert sanitized_exception(exc) == rendered


# ---------------------------------------------------------------------------
# O22(b) — an empty-text exception persists its class name in jobs.last_error
# ---------------------------------------------------------------------------


def test_fail_persists_class_name_for_empty_text_exception(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    enqueued = enqueue_if_absent(conn, "catchup", None, "catchup", {})
    assert enqueued.job_id is not None

    fail(conn, enqueued.job_id, sanitized_exception(httpx.ReadTimeout("")))

    last_error = conn.execute(
        "SELECT last_error FROM jobs WHERE id=?", (enqueued.job_id,)
    ).fetchone()["last_error"]
    assert last_error == "ReadTimeout"


# ---------------------------------------------------------------------------
# safe_detail — class name once, the sanitizer's message and notes, no raise
# ---------------------------------------------------------------------------


class TestSafeDetail:
    def test_zero_argument_exception_is_the_class_name_alone(self) -> None:
        assert safe_detail(RuntimeError()) == "RuntimeError"

    def test_message_is_prefixed_with_the_class_name(self) -> None:
        assert (
            safe_detail(RuntimeError("worker stopped"))
            == "RuntimeError: worker stopped"
        )

    def test_zero_argument_with_notes_names_the_class_once(self) -> None:
        exc = RuntimeError()
        exc.add_note(f"see https://api.example.com/x?apiKey={_API_KEY}")
        detail = safe_detail(exc)
        assert detail == "RuntimeError see https://api.example.com/x?apiKey=%2A%2A%2A"
        assert _API_KEY not in detail
        assert detail == sanitized_exception(exc), (
            "with no message there is nothing to prefix: safe_detail and "
            "sanitized_exception must agree byte-for-byte"
        )

    def test_message_and_notes_keep_the_sanitizer_order(self) -> None:
        exc = RuntimeError("boom")
        exc.add_note("station=ISTATION01 progress=2/3")
        assert safe_detail(exc) == "RuntimeError: boom station=ISTATION01 progress=2/3"

    def test_message_beginning_with_the_class_name_is_kept_whole(self) -> None:
        assert (
            safe_detail(RuntimeError("RuntimeError happened"))
            == "RuntimeError: RuntimeError happened"
        )

    def test_unrenderable_exception_degrades_to_the_class_name(self) -> None:
        class BoomError(RuntimeError):
            def __str__(self) -> str:
                raise ValueError("str exploded")

        exc = BoomError("unrendered")
        with pytest.raises(ValueError):
            sanitized_exception(exc)  # liveness: the fallback path is reached
        assert safe_detail(exc) == "BoomError"


# ---------------------------------------------------------------------------
# Part F, FT1 (plan §6) -- provider_reason at the unit level: the two
# weather.com-shaped messages (path cases on the hold/display side live in
# tests/test_access_hold_current_obs.py, test_access_hold_five_minute.py and
# tests/test_access_days.py).
# ---------------------------------------------------------------------------


class TestProviderReasonWeatherComShape:
    def test_throttled_key_message_is_found_and_unmasked(self) -> None:
        """Catches: only `_http_status_error` changed so the owner's own
        case is never shown (TODO item 4)."""
        response = _pf_json_response(
            401,
            {
                "errors": [
                    {
                        "error": {
                            "code": "SYNTHETIC-CODE",
                            "message": "API key is throttled",
                        }
                    }
                ]
            },
        )
        assert provider_reason(response) == "API key is throttled"

    def test_unauthorized_product_message_is_found_and_unmasked(self) -> None:
        response = _pf_json_response(
            403,
            {
                "errors": [
                    {
                        "error": {
                            "code": "SYNTHETIC-CODE",
                            "message": "apikey is not authorized for this product",
                        }
                    }
                ]
            },
        )
        assert provider_reason(response) == "apikey is not authorized for this product"


# ---------------------------------------------------------------------------
# Part F, FT3 (plan §6) -- the fixed _REASON_PATHS, tried in order; a
# non-string value skipped; a deeply nested body raises nothing.
# ---------------------------------------------------------------------------


class TestProviderReasonNestedPaths:
    def test_error_message_path_is_read(self) -> None:
        response = _pf_json_response(401, {"error": {"message": "inner"}})
        assert provider_reason(response) == "inner"

    def test_errors_error_message_path_is_tried_before_the_top_level_message(
        self,
    ) -> None:
        """Catches: the path order reversed or ignored -- at this
        assertion: correct = 'deep' (errors[0].error.message, tried
        first), mutant = 'top' (the bare `message` field)."""
        response = _pf_json_response(
            401,
            {"errors": [{"error": {"message": "deep"}}], "message": "top"},
        )
        assert provider_reason(response) == "deep", (
            "mutant -> at this assertion: correct = 'deep', mutant = 'top'"
            " (a path-order mutant would read the bare message field first)"
        )

    def test_non_string_message_value_is_skipped(self) -> None:
        """Catches: a non-string value stored as the reason -- at this
        assertion: correct = None, mutant = the dict `{'text': 'x'}`."""
        response = _pf_json_response(401, {"message": {"text": "x"}})
        assert provider_reason(response) is None

    def test_message_outside_the_fixed_paths_is_not_found(self) -> None:
        """Catches: a search for any `message` key anywhere in the body --
        at this assertion: correct = None (``data.message`` is not one of
        the fixed paths), mutant = 'x'."""
        response = _pf_json_response(401, {"data": {"message": "x"}})
        assert provider_reason(response) is None

    def test_empty_errors_list_gives_none(self) -> None:
        response = _pf_json_response(401, {"errors": []})
        assert provider_reason(response) is None

    def test_top_level_list_gives_none(self) -> None:
        response = _pf_json_response(401, [1, 2])
        assert provider_reason(response) is None

    def test_5000_deep_nested_list_under_the_byte_limit_gives_none_and_raises_nothing(
        self,
    ) -> None:
        """Catches: a RecursionError escaping instead of being caught --
        this test's only assertion is that calling provider_reason does
        not raise; a bare `pytest.raises` is never written here on
        purpose, since the contract is silence, not an exception."""
        body = b"[" * 5000 + b"]" * 5000
        assert len(body) <= 16384
        response = _pf_response(401, body)
        assert provider_reason(response) is None


# ---------------------------------------------------------------------------
# Part F, FT4 (plan §6) -- an HTML page: no reason, and _http_status_error's
# text is byte-for-byte today's (no new code runs on this path).
# ---------------------------------------------------------------------------


def test_ft4_html_page_gives_no_reason_and_unchanged_status_error_text() -> None:
    """Catches: raw text stored from an unparsable body (TODO item 1) --
    at the final assertion: correct = byte-for-byte today's text (no
    `; provider says ...` suffix), mutant = the suffix present."""
    html_body = b"<html><head><title>Forbidden words here</title></head></html>"
    response = _pf_response(403, html_body, **{"content-type": "text/html"})
    assert provider_reason(response) is None

    exc = httpx.HTTPStatusError("forbidden", request=_PF_REQUEST, response=response)
    no_body_response = _pf_response(403, b"")
    exc_no_body = httpx.HTTPStatusError(
        "forbidden", request=_PF_REQUEST, response=no_body_response
    )
    assert _http_status_error(exc) == _http_status_error(exc_no_body), (
        "mutant -> at this assertion: correct = the two texts equal (no"
        " reason ever reaches an HTML body), mutant = they differ"
    )


# ---------------------------------------------------------------------------
# Part F, FT5 (plan §6) -- the byte-size bound and the 200-character cut.
# ---------------------------------------------------------------------------


class TestProviderReasonSizeBounds:
    def test_body_one_byte_over_the_limit_gives_none(self) -> None:
        """Catches: no size bound, or an off-by-one at the bound -- at
        this assertion: correct = None, mutant = the short message."""
        body = _pf_padded_body("short reason", exact_bytes=16385)
        assert len(body) == 16385
        response = _pf_response(401, body)
        assert provider_reason(response) is None

    def test_body_exactly_at_the_limit_with_a_long_message_is_cut_to_200(
        self,
    ) -> None:
        """Catches: no cut, or an off-by-one at the bound -- at the length
        assertion below: correct = 200, mutant = 300 (the raw message)."""
        body = _pf_padded_body("M" * 300, exact_bytes=16384)
        assert len(body) == 16384
        response = _pf_response(401, body)
        reason = provider_reason(response)
        assert reason is not None
        assert len(reason) == 200, (
            f"mutant -> at this assertion: correct = 200, mutant = {len(reason)}"
        )
        assert reason == "M" * 199 + "…"


# ---------------------------------------------------------------------------
# Part F, FT6 (plan §6) -- echoes of the request are masked, in place.
# ---------------------------------------------------------------------------


def test_ft6_the_bare_key_url_station_and_host_are_each_masked_in_place() -> None:
    """Catches: masking skipped entirely (the raw secrets, station id and
    host would all survive); masking only URLs (the bare key/station/host
    outside a URL would survive)."""
    full_url = str(_PF_REQUEST.url)
    message = f"key={_PF_KEY} request={full_url} station={_PF_STATION} host={_PF_HOST}"
    response = _pf_json_response(401, {"message": message})
    reason = provider_reason(response)
    assert reason is not None
    assert _PF_KEY not in reason
    assert _PF_STATION not in reason
    assert _PF_HOST not in reason
    assert full_url not in reason
    assert reason == "key=*** request=[address] station=*** host=[address]", (
        "mutant -> at this assertion: correct ="
        " 'key=*** request=[address] station=*** host=[address]', mutant ="
        f" {reason!r}"
    )


def test_ft6_a_key_with_reserved_characters_is_masked_both_raw_and_encoded() -> None:
    """Catches: masking the percent-encoded form only, or the raw form
    only -- the plan's own key has no reserved characters, so its encoded
    form is identical to it and cannot discriminate the two; this key
    does."""
    from urllib.parse import quote

    key = "SYNTH+KEY/0123=abcdef"
    encoded = quote(key, safe="")
    assert encoded != key, "fixture bug: the key must actually need encoding"
    request = httpx.Request("GET", f"https://{_PF_HOST}/v1/obs", params={"apiKey": key})
    message = f"key={key} encoded={encoded}"
    response = _pf_response(
        401, json.dumps({"message": message}).encode(), request=request
    )
    reason = provider_reason(response)
    assert reason is not None
    assert key not in reason
    assert encoded not in reason
    assert reason == "key=*** encoded=***", (
        "mutant -> at this assertion: correct = 'key=*** encoded=***',"
        f" mutant = {reason!r}"
    )


def test_ft6_a_400_char_message_with_the_key_at_190_214_masks_not_cuts() -> None:
    """Catches: masking after the 200-character cut -- the key (24 chars,
    at positions 190-214 of the raw 400-char message) would then be split
    by the cut and survive partially in the clear; masking first keeps it
    whole, as ``***``, inside the kept 199 characters."""
    filler_before = "A" * 190
    filler_after = "A" * (400 - 190 - len(_PF_KEY))
    message = filler_before + _PF_KEY + filler_after
    assert len(message) == 400
    response = _pf_json_response(401, {"message": message})
    reason = provider_reason(response)
    assert reason is not None
    assert _PF_KEY not in reason, (
        "mutant -> at this assertion: correct = the key absent (masked"
        f" whole before the cut), mutant = {reason!r}"
    )
    assert "***" in reason
    assert reason.endswith("…")
    assert len(reason) == 200


def test_ft6_a_digit_bearing_path_segment_is_masked_in_the_message() -> None:
    """Catches: a path segment holding a digit (a station id in the path)
    left unmasked."""
    request = httpx.Request(
        "GET", "https://api.example.test/v3/obs/SYNTHSTN42/history?format=json"
    )
    message = "station SYNTHSTN42 has no data"
    response = _pf_response(
        401, json.dumps({"message": message}).encode(), request=request
    )
    reason = provider_reason(response)
    assert reason == "station *** has no data", (
        "mutant -> at this assertion: correct = 'station *** has no data',"
        f" mutant = {reason!r}"
    )

    # Paired control: the same message, but a request whose path has no
    # digit-bearing segment that appears in the message -- the station id
    # must stay visible, so the mask above is proven to come from the
    # digit-bearing path segment, not from some unconditional id-shaped
    # pattern applied to the message text itself.
    control_request = httpx.Request(
        "GET", "https://api.example.test/obs/history?format=json"
    )
    control_response = _pf_response(
        401, json.dumps({"message": message}).encode(), request=control_request
    )
    control_reason = provider_reason(control_response)
    assert control_reason == message, (
        "mutant -> at this assertion: correct ="
        f" {message!r} (unmasked, no digit-bearing path segment), mutant ="
        f" {control_reason!r}"
    )


# ---------------------------------------------------------------------------
# Part F, FT7 (plan §6) -- a reply provider_reason cannot read.
# ---------------------------------------------------------------------------


class TestProviderReasonUnreadableReply:
    def test_undecodable_bytes_give_none(self) -> None:
        """Catches: an exception escaping instead of giving None."""
        response = _pf_response(401, b"\xff\xfe{")
        assert provider_reason(response) is None

    def test_unread_streamed_reply_gives_none_and_stays_unread(self) -> None:
        """Catches: a `read()` that consumes the stream (TODO item 3) --
        the second assertion proves the stream is still unread afterward,
        so the real caller's own later read is untouched."""
        stream_body = b'{"message":"x"}'
        response = httpx.Response(
            401, stream=httpx.ByteStream(stream_body), request=_PF_REQUEST
        )
        assert provider_reason(response) is None
        assert response.is_stream_consumed is False, (
            "mutant -> at this assertion: correct = False (never read()), mutant = True"
        )
        assert response.read() == stream_body

    def test_content_property_raising_gives_todays_text_from_both_renders(
        self,
    ) -> None:
        """Catches: an exception from `.content` hiding the original
        failure (TODO item 3) -- at the two equality assertions below:
        correct = equal to the no-reason render, mutant = raises or
        differs."""

        class _BoomResponse(httpx.Response):
            @property
            def content(self) -> bytes:  # type: ignore[override]
                raise RuntimeError("boom")

        boom_response = _BoomResponse(401, request=_PF_REQUEST)
        assert provider_reason(boom_response) is None

        exc = httpx.HTTPStatusError(
            "unauthorized", request=_PF_REQUEST, response=boom_response
        )
        no_reason_response = _pf_response(401, b"")
        exc_no_reason = httpx.HTTPStatusError(
            "unauthorized", request=_PF_REQUEST, response=no_reason_response
        )
        assert sanitized_exception(exc) == sanitized_exception(exc_no_reason), (
            "mutant -> at this assertion: correct = equal, mutant = differ or raises"
        )
        assert safe_detail(exc) == safe_detail(exc_no_reason)


# ---------------------------------------------------------------------------
# Part F, FT8 (plan §6) -- flattened to one printable line.
# ---------------------------------------------------------------------------


class TestProviderReasonFlattening:
    def test_mixed_whitespace_and_control_chars_collapse_to_one_line(self) -> None:
        """Catches: a stored reason that breaks a log or table row across
        lines -- at this assertion: correct = one space-joined line with
        the control char and the zero-width space both dropped."""
        message = "line one\nline\ttwo\r\n\x00end​"
        response = _pf_json_response(401, {"message": message})
        assert provider_reason(response) == "line one line two end", (
            "mutant -> at this assertion: correct = 'line one line two"
            f" end', mutant = {provider_reason(response)!r}"
        )

    def test_whitespace_only_message_gives_none_and_todays_text(self) -> None:
        response = _pf_json_response(401, {"message": "   \t\n  "})
        assert provider_reason(response) is None

        exc = httpx.HTTPStatusError(
            "unauthorized", request=_PF_REQUEST, response=response
        )
        no_body_response = _pf_response(401, b"")
        exc_no_body = httpx.HTTPStatusError(
            "unauthorized", request=_PF_REQUEST, response=no_body_response
        )
        assert _http_status_error(exc) == _http_status_error(exc_no_body)
