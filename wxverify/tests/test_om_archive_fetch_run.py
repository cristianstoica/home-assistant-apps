"""``fetch_run`` (``worker/om_archive_fill.py``) and the no-leak property of
plan §4 T13's ``fetch_run`` and ``om-archive check`` parts.

``fetch_run`` never raises for a request or reply error, and never logs or
returns the request address or the site's coordinates -- only a status code
or an exception class name. Covered here: the outcome each HTTP status (or
transport failure) maps to, and that four literal strings derived from one
site's coordinates (its decimal value and its archive grid-point echo)
appear in none of: the log records captured at DEBUG, the CLI's stdout or
stderr, or any exception message raised along the way. ``jobs.last_error``
is excluded (step 4's job wiring, not built yet).

Synthetic data only: coordinates 12.3456/65.4321 (never a real site), model
names already in ``ALLOWANCE_MINUTES``.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from wxverify.__main__ import main
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db
from wxverify.db.migrations import create_schema
from wxverify.db.switch import ensure_site_generation
from wxverify.worker.om_archive_fill import (
    FetchOutcome,
    NoRun,
    Refused,
    Retry,
    Run,
    fetch_run,
)

_LAT = 12.3456
_LON = 65.4321
#: The archive's own grid-point echo, Open-Meteo-style (rounded to 2dp).
_GRID_LAT = "12.35"
_GRID_LON = "65.43"
_LEAK_STRINGS = (str(_LAT), str(_LON), _GRID_LAT, _GRID_LON)


def _assert_no_leak(*texts: str) -> None:
    for text in texts:
        for leak in _LEAK_STRINGS:
            assert leak not in text, (leak, text)


@pytest.fixture
def quiet_httpx_wire() -> Iterator[None]:
    """Set the ``httpx``/``httpcore`` loggers to WARNING for the test, as
    production's ``__main__._configure_logging`` does outside DEBUG.

    Without this, ``caplog.at_level(logging.DEBUG)`` raises the *root*
    logger's level, and since ``httpx``/``httpcore`` carry no level of
    their own in a bare test process, their effective level follows the
    root -- so httpx's own per-request INFO line (``HTTP Request: GET
    https://...?latitude=...``) reaches caplog too, and it carries the
    very coordinates ``_assert_no_leak`` checks for. Restored to each
    logger's own prior level afterward (not reset to NOTSET), so no later
    test inherits a changed level.
    """
    loggers = [logging.getLogger("httpx"), logging.getLogger("httpcore")]
    previous = [wire_logger.level for wire_logger in loggers]
    for wire_logger in loggers:
        wire_logger.setLevel(logging.WARNING)
    try:
        yield
    finally:
        for wire_logger, level in zip(loggers, previous, strict=True):
            wire_logger.setLevel(level)


# ---------------------------------------------------------------------------
# Positive control: caplog actually sees a record that carries the string,
# so an all-pass "no leak" result below is not merely an oracle that never
# looks. Precedent: tests/test_icon_eu_coverage_check.py's
# test_positive_control_adapter_debug_line_is_visible_under_at_level.
# ---------------------------------------------------------------------------


def test_positive_control_caplog_sees_a_planted_leak(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG):
        logging.getLogger("wxverify.worker.om_archive_fill").warning(
            "planted leak latitude=%s", _LAT
        )
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert str(_LAT) in joined


def test_positive_control_survives_the_quiet_httpx_fixture(
    caplog: pytest.LogCaptureFixture, quiet_httpx_wire: None
) -> None:
    """With ``httpx``/``httpcore`` raised to WARNING (as the fixture used by
    the no-leak tests below does), a WARNING from the code under test --
    ``wxverify.worker.om_archive_fill`` itself -- still reaches caplog.
    Proves the fixture quiets only httpx's own wire logging, not the
    module the no-leak tests are actually checking."""
    del quiet_httpx_wire
    with caplog.at_level(logging.DEBUG):
        logging.getLogger("wxverify.worker.om_archive_fill").warning(
            "planted leak latitude=%s", _LAT
        )
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert str(_LAT) in joined


# ---------------------------------------------------------------------------
# fetch_run: status -> outcome, and no exception escapes.
# ---------------------------------------------------------------------------


async def _fetch(
    handler: httpx.AsyncBaseTransport,
    *,
    model: str = "icon_eu",
    run_at: datetime = datetime(2026, 6, 2, tzinfo=UTC),
    forecast_hours: int = 5,
) -> FetchOutcome:
    async with httpx.AsyncClient(transport=handler) as client:
        return await fetch_run(
            client,
            model=model,
            run_at=run_at,
            latitude=_LAT,
            longitude=_LON,
            forecast_hours=forecast_hours,
        )


def test_fetch_run_400_is_no_run_and_the_reason_is_masked(
    caplog: pytest.LogCaptureFixture, quiet_httpx_wire: None
) -> None:
    """A 400 whose JSON reason echoes the latitude back: NoRun, and the
    masked reason never carries the coordinate into the log."""
    del quiet_httpx_wire

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={"reason": f"invalid coordinate {_LAT},{_LON}"},
            request=request,
        )

    with caplog.at_level(logging.DEBUG):
        result = asyncio.run(_fetch(httpx.MockTransport(_handler)))
    assert isinstance(result, NoRun)
    joined = " ".join(r.getMessage() for r in caplog.records)
    _assert_no_leak(joined)


@pytest.mark.parametrize("status", [401, 402, 403, 404])
def test_fetch_run_other_statuses_are_refused(
    status: int, caplog: pytest.LogCaptureFixture, quiet_httpx_wire: None
) -> None:
    del quiet_httpx_wire

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status, content=str(request.url).encode(), request=request
        )

    with caplog.at_level(logging.DEBUG):
        result = asyncio.run(_fetch(httpx.MockTransport(_handler)))
    assert isinstance(result, Refused)
    assert result.status == status
    joined = " ".join(r.getMessage() for r in caplog.records)
    _assert_no_leak(joined)


def test_fetch_run_429_is_retry() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, request=request)

    result = asyncio.run(_fetch(httpx.MockTransport(_handler)))
    assert isinstance(result, Retry)
    assert result.status == 429


def test_fetch_run_5xx_is_retry() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    result = asyncio.run(_fetch(httpx.MockTransport(_handler)))
    assert isinstance(result, Retry)
    assert result.status == 503


def test_fetch_run_missing_hourly_is_retry_not_a_crash(
    caplog: pytest.LogCaptureFixture, quiet_httpx_wire: None
) -> None:
    """A 200 reply with no ``hourly`` field: a pydantic ``ValidationError``
    (a ``ValueError``), caught as Retry -- never raised, and the reply's
    echoed latitude (pydantic embeds the offending input) never leaks."""
    del quiet_httpx_wire

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"latitude": _LAT, "longitude": _LON}, request=request
        )

    with caplog.at_level(logging.DEBUG):
        result = asyncio.run(_fetch(httpx.MockTransport(_handler)))
    assert isinstance(result, Retry)
    joined = " ".join(r.getMessage() for r in caplog.records)
    _assert_no_leak(joined)


def test_fetch_run_all_null_reply_is_no_run() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "latitude": 0.0,
                "longitude": 0.0,
                "hourly": {
                    "time": ["2026-06-02T01:00"],
                    "temperature_2m": [None],
                    "precipitation": [None],
                    "wind_speed_10m": [None],
                },
            },
            request=request,
        )

    result = asyncio.run(_fetch(httpx.MockTransport(_handler)))
    assert isinstance(result, NoRun)


def test_fetch_run_timeout_is_retry_and_never_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def _handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        raise httpx.ConnectTimeout("synthetic timeout")

    with caplog.at_level(logging.DEBUG):
        result = asyncio.run(_fetch(httpx.MockTransport(_handler)))
    assert isinstance(result, Retry)
    joined = " ".join(r.getMessage() for r in caplog.records)
    _assert_no_leak(joined)


def test_fetch_run_a_valid_run_is_converted() -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "latitude": 0.0,
                "longitude": 0.0,
                "hourly": {
                    "time": ["2026-06-02T01:00", "2026-06-02T02:00"],
                    "temperature_2m": [10.0, 11.0],
                    "precipitation": [0.0, 0.0],
                    "wind_speed_10m": [None, None],
                },
            },
            request=request,
        )

    result = asyncio.run(
        _fetch(httpx.MockTransport(_handler), run_at=datetime(2026, 6, 1, tzinfo=UTC))
    )
    assert isinstance(result, Run)
    # 2 temperature + 2 precip (rain shifted a label earlier, still 2
    # distinct hours); wind is null in every hour and contributes nothing.
    assert len(result.samples) == 4
    assert {s.variable for s in result.samples} == {"temperature", "precip"}


# ---------------------------------------------------------------------------
# The CLI (`om-archive check`), end to end through the no-leak site.
# ---------------------------------------------------------------------------


def _build_leak_check_db(path: Path) -> tuple[int, int]:
    """A site at the leak-check coordinates, one icon_eu feed, one closed
    download whose only candidate run always comes back 403 -- enough to
    make the CLI actually send one archive request."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('leak-check-fixture', ?, ?, 0.0, 'UTC')",
        (_LAT, _LON),
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO feeds (source, model, enabled, default_subscribed,"
        " fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES ('open-meteo', 'icon_eu', 1, 1, 180, 200, 0)"
    )
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' AND model='icon_eu'"
        ).fetchone()[0]
    )
    ensure_site_generation(conn, site_id)

    local_date = "2026-06-02"
    fetched_at = isoformat_utc(datetime(2026, 6, 1, 0, 10, tzinfo=UTC))
    day_start = isoformat_utc(datetime(2026, 6, 2, tzinfo=UTC))
    day_end = isoformat_utc(datetime(2026, 6, 3, tzinfo=UTC))
    conn.execute(
        "INSERT INTO site_days (site_id, local_date, state, day_start_utc,"
        " day_end_utc, timezone, attempts, created_at, state_changed_at, done_at)"
        " VALUES (?, ?, 'closed', ?, ?, 'UTC', 1, ?, ?, ?)",
        (site_id, local_date, day_start, day_end, fetched_at, fetched_at, fetched_at),
    )
    conn.execute(
        "INSERT INTO close_record_downloads (site_id, local_date, feed_id,"
        " day_ahead, download_id, download_source, download_date, fetched_at)"
        " VALUES (?, ?, ?, 0, 1, 'recorded', ?, ?)",
        (site_id, local_date, feed_id, local_date, fetched_at),
    )
    valid_at = isoformat_utc(datetime(2026, 6, 2, 6, tzinfo=UTC))
    conn.execute(
        "INSERT INTO forecast_history (site_id, feed_id, variable, day_ahead,"
        " valid_at, local_date, value, lead_hours, issued_at)"
        " VALUES (?, ?, 'temperature', 0, ?, ?, 10.0, 6, ?)",
        (site_id, feed_id, valid_at, local_date, fetched_at),
    )
    conn.commit()
    conn.close()
    return site_id, feed_id


def test_cli_om_archive_check_leaks_no_coordinate(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    restore_logging_state: None,
) -> None:
    db_path = tmp_path / "wxv.db"
    site_id, _feed_id = _build_leak_check_db(db_path)

    calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        # The reason echoes the grid-point-style rounded coordinates, as a
        # real refusal body might when it reflects the request back.
        return httpx.Response(
            403,
            json={"reason": f"blocked for {_GRID_LAT},{_GRID_LON}"},
            request=request,
        )

    import wxverify.worker.om_archive_fill as om_archive_fill

    # ``om_archive_fill.httpx`` IS the ``httpx`` module object (a plain
    # ``import httpx``), so patching its ``AsyncClient`` attribute changes
    # ``httpx.AsyncClient`` everywhere, this module's own `httpx` import
    # included. Save the real class first: building the mock client from
    # ``httpx.AsyncClient`` inside ``_mock_async_client`` would call the
    # patched name right back into itself (RecursionError).
    real_async_client = httpx.AsyncClient

    def _mock_async_client(*args: object, **kwargs: object) -> httpx.AsyncClient:
        del args, kwargs
        return real_async_client(transport=httpx.MockTransport(_handler))

    try:
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(om_archive_fill.httpx, "AsyncClient", _mock_async_client)
            code = main(
                ["--db", str(db_path), "om-archive", "check", "--site-id", str(site_id)]
            )
    finally:
        close_db()

    assert calls >= 1
    assert code == 1  # a refused model never passes
    httpx_logger = logging.getLogger("httpx")
    assert httpx_logger.level == logging.WARNING  # the URL-bearing INFO line is cut off
    out, err = capsys.readouterr()
    _assert_no_leak(out, err)


def test_cli_om_archive_check_no_enabled_site_exits_1(tmp_path: Path) -> None:
    db_path = tmp_path / "wxv-empty.db"
    conn = sqlite3.connect(db_path)
    create_schema(conn)
    conn.commit()
    conn.close()

    try:
        code = main(["--db", str(db_path), "om-archive", "check"])
    finally:
        close_db()
    assert code == 1


def test_fetch_run_never_raises_for_a_connect_error() -> None:
    """``httpx.ConnectError`` (a transport error, distinct from a timeout)
    is also caught, as the docstring promises."""

    def _handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        raise httpx.ConnectError("synthetic connect failure")

    result = asyncio.run(_fetch(httpx.MockTransport(_handler)))
    assert isinstance(result, Retry)
    assert result.refundable is True
