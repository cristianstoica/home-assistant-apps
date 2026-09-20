"""Tests for the decoded-payload diagnostic boundary (§9 O1-O13, O19, O20,
O22(c), O23 of the terminal-error-and-json-decode plan).

O1-O13 exercise ``decode_observations_payload`` through the real adapter
functions (``fetch_hourly_history``, ``fetch_hourly_history_range``,
``validate_station``) under an ``httpx.MockTransport``-backed client — never
by calling the decoder directly — so a call site left unwired is caught.

O19/O20/O22(c) drive ``dispatch`` for a ``fetch_obs`` job through the real
adapter and the real ``stations``/``sites``/``api_budget``/``domain_backoffs``
tables in a tmp-path SQLite database; O23 does the same for ``run_catchup``
with two sites.

Every station id, API key and header value below is synthetic
(``ISTATION0x``, ``SYNTHETIC-SECRET-KEY``, ``SYNTHETIC-CODE`` …) and no
request in this file crosses ``httpx.MockTransport`` to a real socket.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from wxverify import config
from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.queue import Job
from wxverify.obs.config import RECENT_REFRESH_HOURS
from wxverify.obs.pws_adapter import (
    UpstreamPayloadError,
    fetch_hourly_history,
    fetch_hourly_history_range,
    observations_from_payload,
    validate_station,
)
from wxverify.settings.keys import get_setting
from wxverify.worker.catchup import run_catchup
from wxverify.worker.processor import dispatch

_STATION_ID = "ISTATION01"
_API_KEY = "SYNTHETIC-SECRET-KEY"
_NOW = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


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


async def _fetch_hourly_range_via(handler: object) -> list[object]:
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:  # type: ignore[arg-type]
        return await fetch_hourly_history_range(
            _STATION_ID,
            _API_KEY,
            window_start="2026-07-10T11:00:00Z",
            window_end="2026-07-10T12:00:00Z",
            timezone="UTC",
            client=client,
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
def test_structural_shapes_are_failures_on_both_hourly_functions(body: bytes) -> None:
    assert observations_from_payload(json.loads(body)) == [], (
        "precondition: the tolerant helper must still accept this shape — "
        "proving the decoder, not the helper, is what rejects it"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    with pytest.raises(UpstreamPayloadError) as info:
        _run(_fetch_hourly_via(handler))
    assert info.value.diagnostics.kind == "invalid_structure"

    with pytest.raises(UpstreamPayloadError) as info_range:
        _run(_fetch_hourly_range_via(handler))
    assert info_range.value.diagnostics.kind == "invalid_structure"


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
    assert _run(_fetch_hourly_range_via(handler)) == []


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
    assert len(result) == 3  # temperature + wind + precip (first-row increment = total)
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


# ---------------------------------------------------------------------------
# O19/O20/O22(c) harness helpers
# ---------------------------------------------------------------------------


def _seed_site_and_stations(
    conn: sqlite3.Connection, station_ids: list[str]
) -> tuple[int, list[int]]:
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Cluster', 40, -105, 900, 'UTC')
            """
        ).lastrowid
    )
    station_row_ids: list[int] = []
    for pws_id in station_ids:
        station_row_ids.append(
            int(
                conn.execute(
                    """
                    INSERT INTO stations
                        (site_id, pws_station_id, lat, lon, dem_elevation_m)
                    VALUES (?, ?, 40, -105, 900)
                    """,
                    (site_id, pws_id),
                ).lastrowid
            )
        )
    return site_id, station_row_ids


async def _fake_pace(site_id_arg: int, station_id_arg: int, ordinal: int) -> None:
    return None


def _station_row(conn: sqlite3.Connection, station_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT last_run_at, last_error, error_count FROM stations WHERE id=?",
        (station_id,),
    ).fetchone()
    assert row is not None
    return row


# ---------------------------------------------------------------------------
# O19
# ---------------------------------------------------------------------------


def test_partial_progress_and_no_success_advance_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)
    monkeypatch.setattr("wxverify.worker.processor.pace_station_call", _fake_pace)
    site_id, (s1, s2, s3) = _seed_site_and_stations(
        conn, ["ISTATION01", "ISTATION02", "ISTATION03"]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        station = request.url.params["stationId"]
        if station == "ISTATION03":
            return httpx.Response(200, content=b"")
        return httpx.Response(200, content=b'{"observations": []}')

    real_client = httpx.AsyncClient
    db = get_db()
    writer = FencedWriter(db, db.generation)
    with (
        patch(
            "wxverify.worker.processor.httpx.AsyncClient",
            lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),
        ),
        pytest.raises(UpstreamPayloadError) as info,
    ):
        asyncio.run(
            dispatch(
                db,
                writer,
                Job(
                    id=1,
                    type="fetch_obs",
                    site_id=site_id,
                    job_key="obs",
                    payload={},
                    status="running",
                    retry_count=0,
                    max_retries=5,
                ),
            )
        )

    row1 = _station_row(conn, s1)
    row2 = _station_row(conn, s2)
    row3 = _station_row(conn, s3)
    assert row1["last_run_at"] is not None
    assert row1["last_error"] is None
    assert row1["error_count"] == 0
    assert row2["last_run_at"] is not None
    assert row2["last_error"] is None
    assert row2["error_count"] == 0
    assert row3["last_run_at"] is None
    assert row3["error_count"] == 1
    rendered = str(info.value)
    assert row3["last_error"] == rendered
    assert "station=ISTATION03" in row3["last_error"]
    assert "progress=" not in row3["last_error"]

    site_row = conn.execute(
        "SELECT last_obs_at FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    assert site_row["last_obs_at"] is None
    pair_job = conn.execute("SELECT 1 FROM jobs WHERE type='pair_and_score'").fetchone()
    assert pair_job is None

    budget = conn.execute(
        "SELECT calls FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    assert budget["calls"] == 3

    sanitized = sanitized_exception(info.value)
    assert sanitized.endswith("station=ISTATION03 progress=2/3")
    assert "SYNTHETIC-SECRET" not in sanitized


def test_second_station_failure_leaves_third_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)
    monkeypatch.setattr("wxverify.worker.processor.pace_station_call", _fake_pace)
    site_id, (s1, s2, s3) = _seed_site_and_stations(
        conn, ["ISTATION01", "ISTATION02", "ISTATION03"]
    )

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        call_log.append(station)
        if station == "ISTATION02":
            return httpx.Response(200, content=b"")
        return httpx.Response(200, content=b'{"observations": []}')

    real_client = httpx.AsyncClient
    db = get_db()
    writer = FencedWriter(db, db.generation)
    with (
        patch(
            "wxverify.worker.processor.httpx.AsyncClient",
            lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),
        ),
        pytest.raises(UpstreamPayloadError) as info,
    ):
        asyncio.run(
            dispatch(
                db,
                writer,
                Job(
                    id=2,
                    type="fetch_obs",
                    site_id=site_id,
                    job_key="obs",
                    payload={},
                    status="running",
                    retry_count=0,
                    max_retries=5,
                ),
            )
        )

    assert sanitized_exception(info.value).endswith(
        "station=ISTATION02 progress=1/3"
    ), "the note pins the 0-based index of the failing station, not a 1-based ordinal"

    assert call_log == ["ISTATION01", "ISTATION02"]
    row2 = _station_row(conn, s2)
    # The station-level last_error is the pre-note render: _fetch_obs computes
    # sanitized_exception(exc) BEFORE calling exc.add_note(...), so the
    # persisted text carries the payload diagnostics but never the
    # progress= note (that note only reaches the re-raised exception object,
    # asserted separately above at the outer-catch call site).
    assert "kind=json_decode" in str(row2["last_error"])
    assert "station=ISTATION02" in str(row2["last_error"])
    assert "progress=" not in str(row2["last_error"])
    row3 = _station_row(conn, s3)
    assert row3["last_run_at"] is None
    assert row3["last_error"] is None
    assert row3["error_count"] == 0


# ---------------------------------------------------------------------------
# O20
# ---------------------------------------------------------------------------


def test_invalid_structure_no_longer_advances_the_station(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)
    monkeypatch.setattr("wxverify.worker.processor.pace_station_call", _fake_pace)
    site_id, (station_id,) = _seed_site_and_stations(conn, ["ISTATION01"])

    past_stamp = isoformat_utc(utc_now() - timedelta(hours=1))
    conn.execute(
        "INSERT INTO domain_backoffs (domain, next_attempt_at, retry_count) "
        "VALUES ('api.weather.com', ?, 1)",
        (past_stamp,),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"{}")

    real_client = httpx.AsyncClient
    db = get_db()
    writer = FencedWriter(db, db.generation)
    with (
        patch(
            "wxverify.worker.processor.httpx.AsyncClient",
            lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),
        ),
        pytest.raises(UpstreamPayloadError),
    ):
        asyncio.run(
            dispatch(
                db,
                writer,
                Job(
                    id=3,
                    type="fetch_obs",
                    site_id=site_id,
                    job_key="obs",
                    payload={},
                    status="running",
                    retry_count=0,
                    max_retries=5,
                ),
            )
        )

    row = _station_row(conn, station_id)
    assert row["last_run_at"] is None
    assert "kind=invalid_structure" in str(row["last_error"])
    site_row = conn.execute(
        "SELECT last_obs_at FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    assert site_row["last_obs_at"] is None
    backoff_row = conn.execute(
        "SELECT 1 FROM domain_backoffs WHERE domain='api.weather.com'"
    ).fetchone()
    assert backoff_row is not None, (
        "the failure path never touches domain_backoffs — only the success "
        "path (_persist_station_observations) clears it"
    )


# ---------------------------------------------------------------------------
# O22(c)
# ---------------------------------------------------------------------------


def test_read_timeout_persists_class_name_and_bills_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)
    monkeypatch.setattr("wxverify.worker.processor.pace_station_call", _fake_pace)
    site_id, (station_id,) = _seed_site_and_stations(conn, ["ISTATION01"])

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("", request=request)

    real_client = httpx.AsyncClient
    db = get_db()
    writer = FencedWriter(db, db.generation)
    with (
        patch(
            "wxverify.worker.processor.httpx.AsyncClient",
            lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),
        ),
        pytest.raises(httpx.ReadTimeout),
    ):
        asyncio.run(
            dispatch(
                db,
                writer,
                Job(
                    id=4,
                    type="fetch_obs",
                    site_id=site_id,
                    job_key="obs",
                    payload={},
                    status="running",
                    retry_count=0,
                    max_retries=5,
                ),
            )
        )

    row = _station_row(conn, station_id)
    assert row["last_error"] == "ReadTimeout"
    budget = conn.execute(
        "SELECT calls FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    assert budget["calls"] == 1, (
        "ReadTimeout is not in _REFUNDABLE_TRANSPORT_ERRORS — this pins "
        "today's accounting, not a change introduced by this plan"
    )


# ---------------------------------------------------------------------------
# O23
# ---------------------------------------------------------------------------


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO sites "
        "(name, forecast_lat, forecast_lon, elevation_m, timezone, enabled) "
        "VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)",
        (name,),
    )
    site_id = cur.lastrowid
    assert site_id is not None
    return int(site_id)


def _make_station(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)
        VALUES (?, ?, 40.0, -105.0, 900.0)
        """,
        (site_id, pws_station_id),
    )
    station_id = cur.lastrowid
    assert station_id is not None
    return int(station_id)


def test_catchup_per_site_warning_retained_abort_other_site_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _init_tmp_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)
    monkeypatch.setattr("wxverify.worker.catchup.scheduler_tick", lambda c: None)
    monkeypatch.setattr("wxverify.worker.catchup.pace_station_call", _fake_pace)

    site_a = _make_site(conn, "SiteA")
    site_b = _make_site(conn, "SiteB")
    station_1 = _make_station(conn, site_a, "ISTATION01")
    station_2 = _make_station(conn, site_a, "ISTATION02")
    station_3 = _make_station(conn, site_b, "ISTATION03")

    call_log: list[tuple[str | None, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        station = request.url.params.get("stationId")
        call_log.append((host, station))
        if host == "api.weather.com":
            if station == "ISTATION01":
                return httpx.Response(200, content=b'{"observations": null}')
            return httpx.Response(200, content=b'{"observations": []}')
        raise httpx.ConnectError("synthetic", request=request)

    real_client = httpx.AsyncClient
    db = get_db()
    writer = FencedWriter(db, db.generation)
    with (
        patch(
            "wxverify.worker.catchup.httpx.AsyncClient",
            lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),
        ),
        caplog.at_level(logging.DEBUG, logger="wxverify.worker.catchup"),
    ):
        result = asyncio.run(run_catchup(db, writer, {}))

    assert result is None

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    warning_msg = warnings[0].getMessage()
    assert f"site={site_a}" in warning_msg
    assert "kind=invalid_structure" in warning_msg
    assert "station=ISTATION01" in warning_msg
    assert "remaining sites continue" in warning_msg
    assert "SYNTHETIC-SECRET" not in caplog.text
    assert "apiKey" not in caplog.text
    assert "?" not in warning_msg

    row1 = _station_row(conn, station_1)
    assert row1["last_error"] is not None
    assert "kind=invalid_structure" in row1["last_error"]
    assert "SYNTHETIC-SECRET" not in row1["last_error"]
    assert "apiKey" not in row1["last_error"]
    assert row1["error_count"] == 1
    assert row1["last_run_at"] is None

    assert ("api.weather.com", "ISTATION02") not in call_log
    row2 = _station_row(conn, station_2)
    assert row2["last_run_at"] is None
    assert row2["last_error"] is None
    assert row2["error_count"] == 0

    open_meteo_hosts = [
        (host, station) for host, station in call_log if host != "api.weather.com"
    ]
    station3_index = next(
        i for i, (host, station) in enumerate(call_log) if station == "ISTATION03"
    )
    open_meteo_before_station3 = [
        idx
        for idx, (host, _station) in enumerate(call_log)
        if host != "api.weather.com" and idx < station3_index
    ]
    assert open_meteo_before_station3 == [], (
        "site A's open-meteo lane must not run — site A aborted at its "
        "station lane before reaching _fetch_due_open_meteo"
    )
    assert len(open_meteo_hosts) >= 1, "site B's open-meteo lane must run"

    debug_msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.DEBUG]
    assert not any(
        f"catchup due open-meteo site={site_a} feed=" in m for m in debug_msgs
    )
    assert any(f"catchup due open-meteo site={site_b}" in m for m in debug_msgs)

    row3 = _station_row(conn, station_3)
    assert row3["last_run_at"] is not None
    assert row3["last_error"] is None
    assert any(f"catchup site result site={site_b}" in m for m in debug_msgs)
    assert not any(f"catchup site result site={site_a}" in m for m in debug_msgs)

    assert get_setting(conn, "last_catchup_at") is not None
