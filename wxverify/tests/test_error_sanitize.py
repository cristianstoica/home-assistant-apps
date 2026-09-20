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
import sqlite3
from pathlib import Path

import httpx
import pytest

from wxverify import config
from wxverify.core.error_sanitize import safe_detail, sanitized_exception
from wxverify.db.connection import close_db, init_db
from wxverify.db.queue import enqueue_if_absent, fail
from wxverify.obs.pws_adapter import decode_observations_payload

_STATION_ID = "ISTATION01"
_API_KEY = "SYNTHETIC-SECRET-KEY"


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
