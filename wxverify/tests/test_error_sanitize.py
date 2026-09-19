"""Tests for wxverify.core.error_sanitize.sanitized_exception.

O14 — sanitized_exception never returns an empty string, even for an
exception whose str() is empty (httpx maps a bare TimeoutError() this way
under httpx 0.28.1; see the plan's ground truth at
error_sanitize.py:16-19). O15 — exception notes (PEP 678, add_note) are
appended and redacted the same way the base message is. O16 — a rendered
UpstreamPayloadError line survives sanitization byte-for-byte. O22(b) pins
the same empty-text guarantee at the real ``jobs.last_error`` column via
``fail()``.

Synthetic data only: ``SYNTHETIC-SECRET-KEY`` stands in for a real API key.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import httpx

from wxverify import config
from wxverify.core.error_sanitize import sanitized_exception
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
