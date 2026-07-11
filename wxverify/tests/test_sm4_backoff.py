"""S-M4 integration tests: _fetch_current_obs end-to-end backoff coverage.

Bucket-E2E-A: 429 / >=500 HTTP status → domain_backoffs row written + JobDeferred.
Bucket-E2E-B: transport exception (TimeoutException / ConnectError) → TRANSIENT
    persisted at MIN_INTERVAL floor, domain_backoffs row absent, exception propagates.

Both buckets exercise processor._fetch_current_obs with a real :memory: SQLite DB
and fetch_current_observation mocked at its USE site
(``wxverify.worker.processor.fetch_current_observation``), closing the layer-sep
handoff documented at the bottom of test_sm3_current_obs.py.

The paired positive/negative structure: Test class A's "domain row written" (positive)
is paired with Test class B's "domain row absent" (negative) — neither can vacuously
pass when the other side is exercised.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TypeVar
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from wxverify.db.migrations import (
    create_schema,
    seed_default_feeds,
    seed_default_settings,
    seed_default_sources,
)
from wxverify.worker.control import JobDeferred
from wxverify.worker.current_obs import MIN_INTERVAL_SECONDS
from wxverify.worker.processor import _fetch_current_obs  # noqa: PLC2701

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STATION_PWS_ID = "ISTATION01"

# Fixed "now" for wall-clock patches — matches the S-M3 fixture clock.
_NOW = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)
_NOW_ISO = "2026-07-10T12:00:00Z"

# The real adapter hits https://api.weather.com/v2/pws/observations/current
# (confirmed pws_adapter.py:404-408). record_http_backoff keys on response.url.host
# (domain_backoff.py:60), so the mocked response must carry this host.
_WEATHER_COM_HOST = "api.weather.com"
_WEATHER_COM_URL = (
    "https://api.weather.com/v2/pws/observations/current"
    "?stationId=ISTATION01&format=json&units=m&apiKey=SYNTHETIC"
)

# Patch target: fetch_current_observation imported INTO processor's namespace.
_PATCH_TARGET = "wxverify.worker.processor.fetch_current_observation"

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Fake Database: real :memory: SQLite, synchronous callbacks
# ---------------------------------------------------------------------------


class _RealDb:
    """DB shim that wraps one :memory: sqlite3 connection.

    write() and read() call the lambda synchronously — sufficient for the
    single-threaded asyncio.run() test context and avoids a real file path.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    async def write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return fn(self._conn)

    async def read(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return fn(self._conn)


# ---------------------------------------------------------------------------
# DB fixture helpers (mirrors test_sm3_current_obs.py)
# ---------------------------------------------------------------------------


def _make_conn() -> sqlite3.Connection:
    """Open an in-memory v3-schema DB with WAL + foreign_keys."""
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    seed_default_sources(conn)
    seed_default_feeds(conn)
    seed_default_settings(conn)
    conn.execute("PRAGMA user_version = 3")
    return conn


def _seed_site(conn: sqlite3.Connection, *, name: str = "SITE-A") -> int:
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 47.0, 25.0, 900.0, 'UTC')",
        (name,),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_station(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    pws_id: str = _STATION_PWS_ID,
    enabled: int = 1,
    last_run_at: str | None = None,
    last_error: str | None = None,
    error_count: int = 0,
) -> int:
    conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m,"
        "  enabled, last_run_at, last_error, error_count)"
        " VALUES (?, ?, 47.0, 25.0, 900.0, ?, ?, ?, ?)",
        (site_id, pws_id, enabled, last_run_at, last_error, error_count),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_poll_state(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    health_state: str = "online",
    next_poll_at: str = _NOW_ISO,
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state"
        " (station_id, health_state, next_poll_at, updated_at)"
        " VALUES (?, ?, ?, ?)",
        (station_id, health_state, next_poll_at, _NOW_ISO),
    )


def _poll_state(conn: sqlite3.Connection, station_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM station_poll_state WHERE station_id = ?", (station_id,)
    ).fetchone()


def _domain_backoff_row(conn: sqlite3.Connection, domain: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM domain_backoffs WHERE domain = ?", (domain,)
    ).fetchone()


def _station_row(conn: sqlite3.Connection, station_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM stations WHERE id = ?", (station_id,)).fetchone()


# ---------------------------------------------------------------------------
# Fake httpx.Response builder
# ---------------------------------------------------------------------------


def _error_response(status: int) -> httpx.Response:
    """Build an httpx.Response with a real api.weather.com URL.

    record_http_backoff (domain_backoff.py:60) keys the domain row on
    response.url.host, so the Request MUST carry an api.weather.com URL.
    """
    return httpx.Response(
        status_code=status,
        content=b"",
        request=httpx.Request("GET", _WEATHER_COM_URL),
    )


# ---------------------------------------------------------------------------
# Clock patch helper (mirrors test_sm3_current_obs.py)
# ---------------------------------------------------------------------------


def _patched_now(now: datetime = _NOW):  # type: ignore[no-untyped-def]
    """Context manager: freeze utc_now() + isoformat_utc() in current_obs module."""
    now_iso = now.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def fake_utc_now() -> datetime:
        return now

    def fake_isoformat_utc(value: datetime | None = None) -> str:
        if value is None:
            return now_iso
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    from unittest.mock import patch as _patch

    return _patch.multiple(
        "wxverify.worker.current_obs",
        utc_now=fake_utc_now,
        isoformat_utc=fake_isoformat_utc,
    )


# ---------------------------------------------------------------------------
# Bucket-E2E-A: HTTP 429 / >=500 → domain_backoffs written + JobDeferred
# ---------------------------------------------------------------------------


class TestHttpErrorBackoff:
    """_fetch_current_obs: 429 and >=500 write domain_backoffs and raise JobDeferred.

    Positive leg of the paired positive/negative structure: these tests prove
    the domain-backoff row IS written on HTTP failure. The paired negative is
    TestTransportFailNoBackoff, which proves the row is ABSENT on transport error.
    """

    @pytest.mark.parametrize(
        "status",
        [429, 503],
        ids=["status-429", "status-503"],
    )
    def test_http_error_writes_domain_backoff_and_raises_job_deferred(
        self, status: int
    ) -> None:
        """429 and >=500 both write a domain_backoffs row and raise JobDeferred.

        Covers the ``status == 429 or status >= 500`` branch at processor.py:399.
        record_http_backoff (domain_backoff.py:55+) is called inside
        _record_current_obs_backoff (processor.py:436-445), which is the only
        write path that touches domain_backoffs.

        Assertions:
        1. JobDeferred is raised.
        2. domain_backoffs row keyed api.weather.com is written with
           next_attempt_at > now and retry_count >= 1.
        3. station_poll_state.health_state == 'transient' (persist_poll_result
           called from _record_current_obs_backoff).
        4. station_poll_state.last_error is non-None and contains the status.
        5. stations.last_error / error_count / last_run_at are NOT touched
           (diagnostics-isolation contract, plan §6).
        """
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(
            conn,
            site_id,
            last_run_at="2026-01-01T00:00:00Z",
            last_error="stations-sentinel",
            error_count=42,
        )
        _seed_poll_state(conn, station_id)

        # Capture stations.* before the call to detect any mutation.
        st_before = _station_row(conn, station_id)
        assert st_before is not None

        fake_response = _error_response(status)
        mock_fetch = AsyncMock(return_value=fake_response)

        with (
            patch(_PATCH_TARGET, mock_fetch),
            patch(
                "wxverify.worker.processor.resolve_secret",
                return_value="SYNTHETIC",
            ),
            _patched_now(_NOW),
            patch(
                "wxverify.worker.domain_backoff.utc_now",
                return_value=_NOW,
            ),
            patch(
                "wxverify.worker.domain_backoff.isoformat_utc",
                side_effect=lambda v=None: (
                    _NOW.astimezone(UTC).isoformat().replace("+00:00", "Z")
                    if v is None
                    else v.astimezone(UTC).isoformat().replace("+00:00", "Z")
                ),
            ),
        ):
            db = _RealDb(conn)
            with pytest.raises(JobDeferred):
                asyncio.run(_fetch_current_obs(db, site_id, station_id))

        mock_fetch.assert_called_once()

        # 2. domain_backoffs row written for api.weather.com
        backoff = _domain_backoff_row(conn, _WEATHER_COM_HOST)
        assert backoff is not None, (
            f"domain_backoffs row must be written for status={status}"
        )
        assert int(backoff["retry_count"]) >= 1, (
            "retry_count must be >= 1 after first backoff"
        )
        next_attempt = datetime.fromisoformat(
            str(backoff["next_attempt_at"]).replace("Z", "+00:00")
        ).astimezone(UTC)
        assert next_attempt > _NOW, "next_attempt_at must be strictly after now"

        # 3. poll-state health_state == 'transient'
        ps = _poll_state(conn, station_id)
        assert ps is not None
        assert ps["health_state"] == "transient", (
            f"health_state must be 'transient' after status={status}, "
            f"got {ps['health_state']!r}"
        )

        # 4. last_error carries the HTTP status
        assert ps["last_error"] is not None, (
            "last_error must be set on the poll-state row"
        )
        assert str(status) in str(ps["last_error"]), (
            f"last_error must reference status {status}, got {ps['last_error']!r}"
        )
        assert int(ps["error_count"]) >= 1, "error_count must be >= 1"

        # 5. stations.* sentinel values UNCHANGED (plan §6 diagnostics isolation)
        st_after = _station_row(conn, station_id)
        assert st_after is not None
        assert st_after["last_error"] == st_before["last_error"], (
            "stations.last_error must not be modified by _fetch_current_obs"
        )
        assert int(st_after["error_count"]) == int(st_before["error_count"]), (
            "stations.error_count must not be modified by _fetch_current_obs"
        )
        assert st_after["last_run_at"] == st_before["last_run_at"], (
            "stations.last_run_at must not be modified by _fetch_current_obs"
        )


# ---------------------------------------------------------------------------
# Bucket-E2E-B: transport error → TRANSIENT persisted + exception propagates
# ---------------------------------------------------------------------------


class TestTransportFailNoBackoff:
    """_fetch_current_obs: transport errors persist TRANSIENT and re-raise.

    Negative leg of the paired positive/negative: proves domain_backoffs is NOT
    written on transport failure. The paired positive is TestHttpErrorBackoff,
    which proves the row IS written on an HTTP 4xx/5xx — so this negative is only
    meaningful alongside that positive.

    Production path (processor.py:381-391): the try/except catches any Exception
    from fetch_current_observation, constructs PollOutcome(Health.TRANSIENT,…),
    calls persist_poll_result (which writes next_poll_at = now + MIN_INTERVAL_SECONDS
    per current_obs.py:220), then re-raises. No record_http_backoff call is made
    (no HTTP status to key on).
    """

    @pytest.mark.parametrize(
        "exc_factory",
        [
            lambda: httpx.TimeoutException("timeout"),
            lambda: httpx.ConnectError("connection refused"),
        ],
        ids=["timeout", "connect-error"],
    )
    def test_transport_error_persists_transient_and_propagates(
        self,
        exc_factory: object,
    ) -> None:
        """Transport exceptions (timeout, connect-error) propagate and write TRANSIENT.

        Assertions:
        1. The original transport exception propagates (not wrapped in JobDeferred).
        2. station_poll_state.health_state == 'transient'.
        3. station_poll_state.next_poll_at = now + MIN_INTERVAL_SECONDS (300 s floor).
        4. NO domain_backoffs row for api.weather.com.
        5. stations.* sentinel values are NOT touched.
        """
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(
            conn,
            site_id,
            last_run_at="2026-01-01T00:00:00Z",
            last_error="stations-sentinel",
            error_count=7,
        )
        _seed_poll_state(conn, station_id)

        st_before = _station_row(conn, station_id)
        assert st_before is not None

        # Verify no pre-existing domain_backoffs row (injected precondition).
        assert _domain_backoff_row(conn, _WEATHER_COM_HOST) is None, (
            "precondition: no domain_backoffs row must exist before the call"
        )

        exc = exc_factory()  # type: ignore[operator]
        mock_fetch = AsyncMock(side_effect=exc)

        with (
            patch(_PATCH_TARGET, mock_fetch),
            patch(
                "wxverify.worker.processor.resolve_secret",
                return_value="SYNTHETIC",
            ),
            _patched_now(_NOW),
        ):
            db = _RealDb(conn)
            # 1. Transport exception propagates (not swallowed, not wrapped).
            with pytest.raises(httpx.TransportError):
                asyncio.run(_fetch_current_obs(db, site_id, station_id))

        mock_fetch.assert_called_once()

        # 2. health_state == 'transient'
        ps = _poll_state(conn, station_id)
        assert ps is not None
        assert ps["health_state"] == "transient", (
            f"health_state must be 'transient' after transport error, "
            f"got {ps['health_state']!r}"
        )

        # 3. next_poll_at = now + MIN_INTERVAL_SECONDS (300 s)
        expected_floor = _NOW + timedelta(seconds=MIN_INTERVAL_SECONDS)
        expected_iso = expected_floor.astimezone(UTC).isoformat().replace("+00:00", "Z")
        assert ps["next_poll_at"] == expected_iso, (
            f"next_poll_at must be the MIN_INTERVAL floor ({MIN_INTERVAL_SECONDS} s), "
            f"got {ps['next_poll_at']!r}"
        )

        # 4. NO domain_backoffs row written (transport error has no HTTP status)
        assert _domain_backoff_row(conn, _WEATHER_COM_HOST) is None, (
            "domain_backoffs must NOT be written on transport failure — "
            "no HTTP status to key on (processor.py:383)"
        )

        # 5. stations.* sentinel values UNCHANGED
        st_after = _station_row(conn, station_id)
        assert st_after is not None
        assert st_after["last_error"] == st_before["last_error"], (
            "stations.last_error must not be modified on transport failure"
        )
        assert int(st_after["error_count"]) == int(st_before["error_count"]), (
            "stations.error_count must not be modified on transport failure"
        )
        assert st_after["last_run_at"] == st_before["last_run_at"], (
            "stations.last_run_at must not be modified on transport failure"
        )
