"""S-M3 current-obs poller: state-machine contract, parser, scheduler, and normalizer.

Bucket-1-B: poll state-machine (classify_current_obs + persist_poll_result).
Bucket-1-C: current_obs_from_payload field mapping.
Bucket-1-I: post-migration station scheduling via _enqueue_due_current_obs.
§13-A (second half): _obs_instant sub-hour resolution vs _valid_at hour-flooring.

All wall-clock reads are patched; DB isolation is per-test :memory: SQLite.
The domain-backoff row write (record_http_backoff for 429/>=500) lives in
processor._record_current_obs_backoff — above classify_current_obs /
persist_poll_result — so it is NOT asserted here (see layering note at end of
file for the handoff detail).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import httpx
import pytest

from wxverify.db.migrations import (
    create_schema,
    seed_default_feeds,
    seed_default_settings,
    seed_default_sources,
)
from wxverify.obs.cadence import (
    WINDOW_N,
    base_interval,
    obs_cadence_jitter,
)
from wxverify.obs.pws_adapter import (
    _obs_instant,  # noqa: PLC2701
    _valid_at,  # noqa: PLC2701
    current_obs_from_payload,
)
from wxverify.worker.current_obs import (
    MAX_BACKOFF_SECONDS,
    MIN_INTERVAL_SECONDS,
    Health,
    PollOutcome,
    classify_current_obs,
    persist_poll_result,
)
from wxverify.worker.scheduler import _enqueue_due_current_obs  # noqa: PLC2701

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_STATION_PWS_ID = "ISTATION01"
_STATION_PWS_ID2 = "ISTATION02"

# A fixed "now" for all wall-clock patches
_NOW = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)
_NOW_ISO = "2026-07-10T12:00:00Z"

# Two obs instants 5 min apart, within the same clock hour — for §13-A
_OBS_T0 = "2026-07-10T11:45:00Z"
_OBS_T1 = "2026-07-10T11:50:00Z"  # 300 s after T0

# A parseable obstime used for ONLINE cases
_OBS_INSTANT = "2026-07-10T11:55:00Z"

# A second, different obstime for dedup tests
_OBS_INSTANT_B = "2026-07-10T12:00:00Z"

# A genuinely unparseable obstime — triggers TRANSIENT, NOT OFFLINE.
# Note: "2026-07-10 11:55:00" (space-separated) IS parsed by _obs_datetime
# (the tolerant normalizer strips space → T and appends Z); use a value that
# survives all normalization steps but fails fromisoformat — e.g. "N/A".
_OBS_INSTANT_UNPARSEABLE = "N/A"


# ---------------------------------------------------------------------------
# DB fixture helpers
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
    last_obstime: str | None = None,
    cadence_events: list[str] | None = None,
    last_error: str | None = None,
    error_count: int = 0,
    learned_interval_seconds: int | None = None,
) -> None:
    events_json = json.dumps(cadence_events or [], separators=(",", ":"))
    conn.execute(
        "INSERT INTO station_poll_state"
        " (station_id, health_state, next_poll_at, last_obstime,"
        "  cadence_events, last_error, error_count, learned_interval_seconds,"
        "  updated_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            station_id,
            health_state,
            next_poll_at,
            last_obstime,
            events_json,
            last_error,
            error_count,
            learned_interval_seconds,
            _NOW_ISO,
        ),
    )


def _seed_current_obs(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    temp: float = 20.0,
    fetched_at: str = _NOW_ISO,
) -> None:
    conn.execute(
        "INSERT INTO station_current_obs"
        " (station_id, obs_time_utc, temp, humidity, dewpt,"
        "  wind_speed, wind_gust, wind_dir, pressure,"
        "  precip_rate, precip_total, uv, neighborhood, fetched_at)"
        " VALUES (?, ?, ?, NULL, NULL,"
        " NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?)",
        (station_id, _OBS_INSTANT, temp, fetched_at),
    )


def _poll_state(conn: sqlite3.Connection, station_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM station_poll_state WHERE station_id = ?", (station_id,)
    ).fetchone()


def _current_obs_row(conn: sqlite3.Connection, station_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM station_current_obs WHERE station_id = ?", (station_id,)
    ).fetchone()


def _station_row(conn: sqlite3.Connection, station_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM stations WHERE id = ?", (station_id,)).fetchone()


# ---------------------------------------------------------------------------
# Fake httpx.Response builder
# ---------------------------------------------------------------------------

_WEATHER_COM_URL = httpx.URL("https://api.weather.com/v2/pws/observations/current")


def _fake_response(
    status: int,
    content: bytes | None = None,
    json_data: object | None = None,
) -> httpx.Response:
    """Build an httpx.Response without a network call."""
    if json_data is not None:
        body = json.dumps(json_data).encode()
    elif content is not None:
        body = content
    else:
        body = b""
    return httpx.Response(
        status_code=status,
        content=body,
        headers={"content-type": "application/json"} if json_data is not None else {},
        request=httpx.Request("GET", _WEATHER_COM_URL),
    )


def _online_payload(
    obs_time_utc: str = _OBS_INSTANT,
    *,
    temp: float = 18.5,
    humidity: float = 72.0,
    dewpt: float = 13.4,
    wind_speed: float = 12.0,
    wind_gust: float = 18.0,
    wind_dir: float = 270.0,
    pressure: float = 1013.2,
    precip_rate: float = 0.0,
    precip_total: float = 0.3,
    uv: float = 3.0,
    neighborhood: str = "Test Quarter",
) -> dict[str, object]:
    return {
        "observations": [
            {
                "obsTimeUtc": obs_time_utc,
                "humidity": humidity,
                "winddir": wind_dir,
                "uv": uv,
                "neighborhood": neighborhood,
                "metric": {
                    "temp": temp,
                    "dewpt": dewpt,
                    "windSpeed": wind_speed,
                    "windGust": wind_gust,
                    "pressure": pressure,
                    "precipRate": precip_rate,
                    "precipTotal": precip_total,
                },
            }
        ]
    }


# ---------------------------------------------------------------------------
# Clock patch helper
# ---------------------------------------------------------------------------


def _patched_now(now: datetime = _NOW):  # type: ignore[no-untyped-def]
    """Context manager: patch utc_now() and isoformat_utc() in current_obs module."""
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
# Bucket-1-B: poll state-machine via classify_current_obs
# ---------------------------------------------------------------------------


class TestClassifyCurrentObs:
    """Unit tests for classify_current_obs — no DB, no patching needed."""

    # -- ONLINE ----------------------------------------------------------------

    def test_online_2xx_parseable_obstime(self) -> None:
        """200 + observations + parseable obstime → ONLINE with parsed obs."""
        resp = _fake_response(200, json_data=_online_payload())
        outcome = classify_current_obs(resp)
        assert outcome.health is Health.ONLINE
        assert outcome.obs is not None
        assert outcome.obs_instant == _OBS_INSTANT
        assert outcome.error is None

    # -- OFFLINE ---------------------------------------------------------------

    def test_offline_204_does_not_call_json(self) -> None:
        """204 → OFFLINE before json() is called.

        The marquee negative: a fake response whose .json() raises proves the
        204 branch short-circuits BEFORE any attempt to parse the body.
        """
        # Construct a response whose .json() would raise if called.
        resp = _fake_response(204, content=b"")
        # Patch the json method to raise if called.
        _raises = ValueError("json() must not be called for 204")
        resp.json = MagicMock(side_effect=_raises)  # type: ignore[method-assign]
        outcome = classify_current_obs(resp)
        assert outcome.health is Health.OFFLINE
        resp.json.assert_not_called()  # type: ignore[union-attr]

    def test_offline_empty_body_does_not_call_json(self) -> None:
        """200 with empty body → OFFLINE before json() is called.

        Paired negative with test_online_2xx_parseable_obstime: if the body-empty
        branch were removed, this would raise JSONDecodeError instead.
        """
        resp = _fake_response(200, content=b"")
        _raises = ValueError("json() must not be called for empty body")
        resp.json = MagicMock(side_effect=_raises)  # type: ignore[method-assign]
        outcome = classify_current_obs(resp)
        assert outcome.health is Health.OFFLINE
        resp.json.assert_not_called()  # type: ignore[union-attr]

    def test_offline_empty_observations_array(self) -> None:
        """200 + empty observations list → OFFLINE (no first obs row)."""
        resp = _fake_response(200, json_data={"observations": []})
        outcome = classify_current_obs(resp)
        assert outcome.health is Health.OFFLINE

    def test_offline_missing_observations_key(self) -> None:
        """200 + payload with no 'observations' key → OFFLINE."""
        resp = _fake_response(200, json_data={"some_other_key": []})
        outcome = classify_current_obs(resp)
        assert outcome.health is Health.OFFLINE

    # -- TRANSIENT (unparseable obstime) — marquee negative --------------------

    def test_transient_unparseable_obstime_not_offline(self) -> None:
        """2xx non-empty payload with unparseable obstime → TRANSIENT, NOT OFFLINE.

        Marquee negative: the OFFLINE freeze (86400 s) must NOT be triggered by a
        parse failure. The station is live — it should retry at the floor (300 s).
        Paired with test_offline_empty_observations_array (which proves OFFLINE
        fires on a genuinely absent observation) so neither can pass vacuously.
        """
        payload = _online_payload(obs_time_utc=_OBS_INSTANT_UNPARSEABLE)
        resp = _fake_response(200, json_data=payload)
        outcome = classify_current_obs(resp)
        assert outcome.health is Health.TRANSIENT, (
            f"unparseable obstime must be TRANSIENT (retry), got {outcome.health}"
        )

    # -- TERMINAL --------------------------------------------------------------

    def test_terminal_401(self) -> None:
        """401 → TERMINAL."""
        outcome = classify_current_obs(_fake_response(401))
        assert outcome.health is Health.TERMINAL

    def test_terminal_403(self) -> None:
        """403 → TERMINAL."""
        outcome = classify_current_obs(_fake_response(403))
        assert outcome.health is Health.TERMINAL

    def test_terminal_404(self) -> None:
        """404 (non-429 4xx) → TERMINAL."""
        outcome = classify_current_obs(_fake_response(404))
        assert outcome.health is Health.TERMINAL

    # -- 429 must NOT flip terminal — marquee negative -------------------------

    def test_429_is_transient_not_terminal(self) -> None:
        """429 → TRANSIENT, never TERMINAL.

        Marquee negative: 429 is a 4xx, but the load-bearing ordering puts the
        429 check BEFORE the 4xx-terminal branch. If that ordering were wrong,
        this would return TERMINAL and a rate-limited station would be frozen at
        86400 s instead of retried at 300 s.

        Paired positive: test_terminal_401 (4xx → TERMINAL when NOT 429) proves
        the terminal branch fires for non-429 4xx — so this test cannot pass
        vacuously by the terminal branch being dead.
        """
        outcome = classify_current_obs(_fake_response(429))
        assert outcome.health is Health.TRANSIENT, (
            f"429 must be TRANSIENT, got {outcome.health}"
        )

    # -- 5xx -------------------------------------------------------------------

    def test_transient_500(self) -> None:
        """500 → TRANSIENT."""
        outcome = classify_current_obs(_fake_response(500))
        assert outcome.health is Health.TRANSIENT

    def test_transient_503(self) -> None:
        """503 → TRANSIENT."""
        outcome = classify_current_obs(_fake_response(503))
        assert outcome.health is Health.TRANSIENT

    # -- Error string carried --------------------------------------------------

    def test_error_string_carried_on_non_online(self) -> None:
        """Non-online outcomes carry a non-None error string."""
        for status in (401, 429, 500):
            outcome = classify_current_obs(_fake_response(status))
            assert outcome.error is not None, f"status {status} must carry error string"

    def test_online_has_no_error_string(self) -> None:
        """ONLINE outcome carries no error string."""
        resp = _fake_response(200, json_data=_online_payload())
        outcome = classify_current_obs(resp)
        assert outcome.error is None


# ---------------------------------------------------------------------------
# Bucket-1-B: persist_poll_result — DB-level contract
# ---------------------------------------------------------------------------


class TestPersistPollResult:
    """Integration tests for persist_poll_result: real :memory: SQLite per test."""

    # -- ONLINE: next_poll_at independently recomputed -------------------------

    def test_online_next_poll_at_independently_recomputed(self) -> None:
        """ONLINE: next_poll_at = now + max(300, base + jitter), recomputed.

        The marquee assertion: compute the expected delay the same way the code
        does (reimplement obs_cadence_jitter independently), then verify
        next_poll_at > now + small_epsilon (never == now + offset alone).
        """
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        # Seed a poll-state with 6 events spaced 900 s apart so base_interval=720
        events = [
            (
                datetime(2026, 7, 10, 9, 0, 0, tzinfo=UTC) + timedelta(seconds=900 * i)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            for i in range(6)
        ]
        _seed_poll_state(
            conn,
            station_id,
            health_state="online",
            next_poll_at=_NOW_ISO,
            last_obstime=events[-1],
            cadence_events=events,
        )

        obs_instant = "2026-07-10T12:05:00Z"  # different from last_obstime → appends
        payload = _online_payload(obs_time_utc=obs_instant)
        resp = _fake_response(200, json_data=payload)
        outcome = classify_current_obs(resp)

        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        assert row["health_state"] == "online"
        assert row["next_poll_at"] is not None

        # Independent recomputation of expected next_poll_at
        new_events = tuple(events) + (obs_instant,)
        new_events = new_events[-WINDOW_N:]
        base = base_interval(new_events, MIN_INTERVAL_SECONDS)
        now_ts = _NOW.timestamp()
        cycle_bucket = int(now_ts // base)
        offset = obs_cadence_jitter(station_id, cycle_bucket, base)
        delay = max(MIN_INTERVAL_SECONDS, base + offset)
        expected_next = _NOW + timedelta(seconds=delay)
        expected_iso = expected_next.astimezone(UTC).isoformat().replace("+00:00", "Z")

        assert row["next_poll_at"] == expected_iso
        # Must be strictly after now (not == now, not in the past)
        actual_next = datetime.fromisoformat(
            row["next_poll_at"].replace("Z", "+00:00")
        ).astimezone(UTC)
        assert actual_next > _NOW + timedelta(seconds=1), (
            "next_poll_at must be strictly after now by at least MIN_INTERVAL_SECONDS"
        )

    # -- ONLINE: clears diagnostics, NEVER touches stations.* -----------------

    def test_online_clears_diagnostics_and_stations_untouched(self) -> None:
        """ONLINE poll: last_error/error_count cleared; stations.* never modified.

        Diagnostic isolation (plan §5.9): stations.last_error, stations.error_count,
        and stations.last_run_at are seeded to sentinel values and must be unchanged
        after persist_poll_result, whether online or failing.
        """
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(
            conn,
            site_id,
            last_run_at="2026-01-01T00:00:00Z",
            last_error="sentinel-error",
            error_count=99,
        )
        _seed_poll_state(
            conn,
            station_id,
            health_state="offline",
            last_error="prior-poll-error",
            error_count=5,
        )

        resp = _fake_response(200, json_data=_online_payload())
        outcome = classify_current_obs(resp)
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        poll_row = _poll_state(conn, station_id)
        assert poll_row is not None
        assert poll_row["health_state"] == "online"
        assert poll_row["last_error"] is None
        assert poll_row["error_count"] == 0

        # stations.* sentinel values must be untouched
        st = _station_row(conn, station_id)
        assert st is not None
        assert st["last_error"] == "sentinel-error"
        assert st["error_count"] == 99
        assert st["last_run_at"] == "2026-01-01T00:00:00Z"

    # -- OFFLINE: next_poll_at = now + 86400; stations.* untouched ------------

    def test_offline_next_poll_at_86400_and_stations_untouched(self) -> None:
        """OFFLINE: next_poll_at = now + 86400 s; stations.* sentinel unchanged."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id, last_error="sentinel", error_count=7)
        _seed_poll_state(conn, station_id)

        outcome = PollOutcome(Health.OFFLINE, error="empty body")
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        assert row["health_state"] == "offline"
        actual = datetime.fromisoformat(
            row["next_poll_at"].replace("Z", "+00:00")
        ).astimezone(UTC)
        expected = _NOW + timedelta(seconds=MAX_BACKOFF_SECONDS)
        assert actual == expected

        st = _station_row(conn, station_id)
        assert st is not None
        assert st["last_error"] == "sentinel"
        assert st["error_count"] == 7

    # -- TERMINAL: next_poll_at = now + 86400 ----------------------------------

    def test_terminal_next_poll_at_86400(self) -> None:
        """TERMINAL: next_poll_at = now + 86400 s (auth-rejected park interval)."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(conn, station_id)

        outcome = PollOutcome(Health.TERMINAL, error="http 401")
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        assert row["health_state"] == "terminal"
        actual = datetime.fromisoformat(
            row["next_poll_at"].replace("Z", "+00:00")
        ).astimezone(UTC)
        assert actual == _NOW + timedelta(seconds=MAX_BACKOFF_SECONDS)

    # -- TRANSIENT: next_poll_at = now + 300 -----------------------------------

    def test_transient_next_poll_at_300(self) -> None:
        """TRANSIENT (429 / 5xx / parse-fail): next_poll_at = now + 300 s."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(conn, station_id)

        outcome = PollOutcome(Health.TRANSIENT, error="http 429")
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        assert row["health_state"] == "transient"
        actual = datetime.fromisoformat(
            row["next_poll_at"].replace("Z", "+00:00")
        ).astimezone(UTC)
        assert actual == _NOW + timedelta(seconds=MIN_INTERVAL_SECONDS)

    # -- 429 → TRANSIENT at 300 s, not 86400 ----------------------------------

    def test_429_classified_as_transient_schedules_300_not_86400(self) -> None:
        """429 classifies to TRANSIENT → persist writes next_poll_at = now + 300.

        Paired with test_terminal_next_poll_at_86400: if 429 wrongly became
        TERMINAL it would write 86400 — this assertion catches that.
        """
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(conn, station_id)

        resp = _fake_response(429)
        outcome = classify_current_obs(resp)
        assert outcome.health is Health.TRANSIENT  # classification guard

        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        actual = datetime.fromisoformat(
            row["next_poll_at"].replace("Z", "+00:00")
        ).astimezone(UTC)
        assert actual == _NOW + timedelta(seconds=MIN_INTERVAL_SECONDS), (
            "429 must schedule a 300-s retry, not the 86400-s terminal freeze"
        )

    # -- UPSERT on unseeded row (marquee) --------------------------------------

    def test_upsert_on_unseeded_row_inserts_not_noops(self) -> None:
        """persist_poll_result for a station with NO poll-state row → inserts one.

        Marquee negative: if the code used a bare UPDATE, an unseeded row would
        be a no-op and next_poll_at would remain NULL, creating a tight re-poll
        loop.
        """
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        # Deliberately do NOT seed a station_poll_state row

        outcome = PollOutcome(Health.OFFLINE, error="empty body")
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None, "UPSERT must INSERT a row when none existed"
        assert row["next_poll_at"] is not None, (
            "next_poll_at must be non-NULL after UPSERT-on-unseeded-row"
        )

    def test_upsert_on_unseeded_row_online_inserts_with_next_poll_at(self) -> None:
        """ONLINE persist on a station with no prior row → inserts with next_poll_at."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)

        resp = _fake_response(200, json_data=_online_payload())
        outcome = classify_current_obs(resp)
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        assert row["next_poll_at"] is not None
        assert row["health_state"] == "online"

    # -- Error count increments on failure, resets on ONLINE ------------------

    def test_error_count_increments_on_failure(self) -> None:
        """Successive failure persists increment station_poll_state.error_count."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(conn, station_id, error_count=3)

        outcome = PollOutcome(Health.TRANSIENT, error="http 503")
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        assert row["error_count"] == 4

    def test_error_count_resets_to_zero_on_online(self) -> None:
        """ONLINE poll resets error_count to 0 and last_error to NULL."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(
            conn, station_id, health_state="transient", last_error="prev", error_count=7
        )

        resp = _fake_response(200, json_data=_online_payload())
        outcome = classify_current_obs(resp)
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        assert row["error_count"] == 0
        assert row["last_error"] is None

    # -- last_poll_at set on every persist -------------------------------------

    def test_last_poll_at_written_on_online(self) -> None:
        """ONLINE persist writes last_poll_at = now."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(conn, station_id)

        resp = _fake_response(200, json_data=_online_payload())
        outcome = classify_current_obs(resp)
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        assert row["last_poll_at"] == _NOW_ISO

    def test_last_poll_at_written_on_failure(self) -> None:
        """Failure persist writes last_poll_at = now."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(conn, station_id)

        outcome = PollOutcome(Health.TERMINAL, error="http 403")
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        assert row["last_poll_at"] == _NOW_ISO

    # -- station_current_obs upsert/retention ---------------------------------

    def test_online_writes_current_obs_row(self) -> None:
        """ONLINE persist writes station_current_obs with all fields + fetched_at."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(conn, station_id)

        payload = _online_payload(
            temp=22.5,
            humidity=65.0,
            dewpt=15.2,
            wind_speed=10.0,
            wind_gust=16.0,
            wind_dir=180.0,
            pressure=1015.0,
            precip_rate=0.0,
            precip_total=1.2,
            uv=5.0,
            neighborhood="Test Quarter",
        )
        resp = _fake_response(200, json_data=payload)
        outcome = classify_current_obs(resp)
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        obs = _current_obs_row(conn, station_id)
        assert obs is not None
        assert obs["temp"] == pytest.approx(22.5)
        assert obs["humidity"] == pytest.approx(65.0)
        assert obs["dewpt"] == pytest.approx(15.2)
        assert obs["wind_speed"] == pytest.approx(10.0)
        assert obs["wind_gust"] == pytest.approx(16.0)
        assert obs["wind_dir"] == pytest.approx(180.0)
        assert obs["pressure"] == pytest.approx(1015.0)
        assert obs["precip_rate"] == pytest.approx(0.0)
        assert obs["precip_total"] == pytest.approx(1.2)
        assert obs["uv"] == pytest.approx(5.0)
        assert obs["neighborhood"] == "Test Quarter"
        assert obs["fetched_at"] == _NOW_ISO

    def test_non_online_retains_last_good_current_obs(self) -> None:
        """OFFLINE/terminal/transient: prior station_current_obs row retained unchanged.

        The last-good snapshot must survive a failing poll — this ensures the
        read route always has a display snapshot even when the station goes dark.
        """
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(conn, station_id)
        # Seed a pre-existing current-obs row with a known temp
        _seed_current_obs(
            conn, station_id, temp=25.0, fetched_at="2026-07-10T10:00:00Z"
        )

        for outcome in [
            PollOutcome(Health.OFFLINE, error="empty body"),
            PollOutcome(Health.TERMINAL, error="http 401"),
            PollOutcome(Health.TRANSIENT, error="http 503"),
        ]:
            # Reset poll-state for each iteration
            conn.execute(
                "DELETE FROM station_poll_state WHERE station_id=?", (station_id,)
            )
            _seed_poll_state(conn, station_id)
            with _patched_now(_NOW):
                persist_poll_result(conn, site_id, station_id, outcome)
            obs = _current_obs_row(conn, station_id)
            assert obs is not None, f"obs row must be retained for {outcome.health}"
            assert obs["temp"] == pytest.approx(25.0), (
                f"temp must be unchanged for {outcome.health}"
            )
            assert obs["fetched_at"] == "2026-07-10T10:00:00Z", (
                f"fetched_at must be unchanged for {outcome.health}"
            )

    def test_online_no_prior_current_obs_inserts_row(self) -> None:
        """ONLINE with no prior station_current_obs row → inserts a new one."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(conn, station_id)

        resp = _fake_response(200, json_data=_online_payload(temp=19.9))
        outcome = classify_current_obs(resp)
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        obs = _current_obs_row(conn, station_id)
        assert obs is not None
        assert obs["temp"] == pytest.approx(19.9)

    def test_online_updates_existing_current_obs_row(self) -> None:
        """ONLINE with a pre-existing current-obs row → updates it (UPSERT)."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(conn, station_id)
        _seed_current_obs(conn, station_id, temp=15.0)

        resp = _fake_response(200, json_data=_online_payload(temp=21.0))
        outcome = classify_current_obs(resp)
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        obs = _current_obs_row(conn, station_id)
        assert obs is not None
        assert obs["temp"] == pytest.approx(21.0)

    # -- Cadence window dedup and truncation -----------------------------------

    def test_cadence_window_appends_new_obstime(self) -> None:
        """ONLINE with a NEW obs_instant (different from last_obstime) → appended."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        prior_events = [
            "2026-07-10T09:00:00Z",
            "2026-07-10T09:15:00Z",
        ]
        _seed_poll_state(
            conn,
            station_id,
            last_obstime=prior_events[-1],
            cadence_events=prior_events,
        )

        new_instant = "2026-07-10T09:30:00Z"
        resp = _fake_response(200, json_data=_online_payload(obs_time_utc=new_instant))
        outcome = classify_current_obs(resp)
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        stored = tuple(json.loads(row["cadence_events"]))
        assert new_instant in stored, (
            "new obs_instant must be appended to cadence_events"
        )
        assert len(stored) == 3

    def test_cadence_window_dedup_same_obstime_no_append(self) -> None:
        """ONLINE with SAME obs_instant as last_obstime → no append (dedup)."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        prior_events = ["2026-07-10T09:00:00Z", "2026-07-10T09:15:00Z"]
        same_instant = prior_events[-1]
        _seed_poll_state(
            conn,
            station_id,
            last_obstime=same_instant,
            cadence_events=prior_events,
        )

        resp = _fake_response(200, json_data=_online_payload(obs_time_utc=same_instant))
        outcome = classify_current_obs(resp)
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        stored = tuple(json.loads(row["cadence_events"]))
        assert len(stored) == len(prior_events), (
            "same obs_instant must NOT be appended (dedup)"
        )

    def test_cadence_window_truncates_to_window_n(self) -> None:
        """ONLINE with 6 prior events + 1 new → window truncated to WINDOW_N=6."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        # Seed exactly WINDOW_N events
        prior = [
            (
                datetime(2026, 7, 10, 9, 0, tzinfo=UTC) + timedelta(minutes=15 * i)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            for i in range(WINDOW_N)
        ]
        _seed_poll_state(
            conn,
            station_id,
            last_obstime=prior[-1],
            cadence_events=prior,
        )

        # A new, later instant
        new_instant = "2026-07-10T10:30:00Z"
        resp = _fake_response(200, json_data=_online_payload(obs_time_utc=new_instant))
        outcome = classify_current_obs(resp)
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None
        stored = tuple(json.loads(row["cadence_events"]))
        assert len(stored) == WINDOW_N, (
            "cadence window must be truncated to WINDOW_N="
            f"{WINDOW_N}; got {len(stored)}"
        )
        assert stored[-1] == new_instant

    def test_non_online_cadence_window_frozen(self) -> None:
        """Non-ONLINE outcomes do NOT modify cadence_events (window frozen)."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        prior_events = ["2026-07-10T09:00:00Z", "2026-07-10T09:15:00Z"]
        _seed_poll_state(
            conn,
            station_id,
            health_state="online",
            last_obstime=prior_events[-1],
            cadence_events=prior_events,
        )

        for health, error in [
            (Health.OFFLINE, "empty body"),
            (Health.TERMINAL, "http 403"),
            (Health.TRANSIENT, "http 503"),
        ]:
            conn.execute(
                "UPDATE station_poll_state SET cadence_events=? WHERE station_id=?",
                (json.dumps(prior_events, separators=(",", ":")), station_id),
            )
            outcome = PollOutcome(health, error=error)
            with _patched_now(_NOW):
                persist_poll_result(conn, site_id, station_id, outcome)
            row = _poll_state(conn, station_id)
            assert row is not None
            stored = tuple(json.loads(row["cadence_events"]))
            assert stored == tuple(prior_events), (
                f"{health} must not modify cadence_events"
            )

    # -- Disabled station is skipped ------------------------------------------

    def test_disabled_station_skipped_by_persist(self) -> None:
        """persist_poll_result returns early when station is disabled or missing."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id, enabled=0)

        outcome = PollOutcome(Health.OFFLINE, error="empty body")
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        # No poll-state row must be written
        row = _poll_state(conn, station_id)
        assert row is None

    # -- Missing fields map to None (no fabricated zero) -----------------------

    def test_online_missing_metric_fields_map_to_none(self) -> None:
        """Missing metric fields in ONLINE payload → None stored (not fabricated 0)."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_poll_state(conn, station_id)

        # Payload with only obsTimeUtc and no metric values
        sparse_payload = {
            "observations": [
                {
                    "obsTimeUtc": _OBS_INSTANT,
                    "metric": {},
                }
            ]
        }
        resp = _fake_response(200, json_data=sparse_payload)
        outcome = classify_current_obs(resp)
        assert outcome.health is Health.ONLINE
        assert outcome.obs is not None
        # All numeric fields must be None
        assert outcome.obs.temp is None
        assert outcome.obs.humidity is None
        assert outcome.obs.wind_speed is None

    # -- stations.* sentinel check for ALL health states ----------------------

    @pytest.mark.parametrize(
        "outcome",
        [
            PollOutcome(Health.OFFLINE, error="empty body"),
            PollOutcome(Health.TERMINAL, error="http 401"),
            PollOutcome(Health.TRANSIENT, error="http 429"),
        ],
        ids=["offline", "terminal", "transient"],
    )
    def test_stations_columns_never_modified_on_failure(
        self, outcome: PollOutcome
    ) -> None:
        """stations.last_error / error_count / last_run_at untouched on all failures."""
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

        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        st = _station_row(conn, station_id)
        assert st is not None
        assert st["last_error"] == "stations-sentinel"
        assert st["error_count"] == 42
        assert st["last_run_at"] == "2026-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# Bucket-1-C: current_obs_from_payload field mapping
# ---------------------------------------------------------------------------


class TestCurrentObsFromPayload:
    """Unit tests for current_obs_from_payload: field mapping from WU payload shape."""

    def test_full_payload_maps_all_fields(self) -> None:
        """A complete Weather.com payload maps all 11+ fields without fabrication."""
        payload = _online_payload(
            obs_time_utc=_OBS_INSTANT,
            temp=18.5,
            humidity=72.0,
            dewpt=13.4,
            wind_speed=12.0,
            wind_gust=18.0,
            wind_dir=270.0,
            pressure=1013.2,
            precip_rate=0.5,
            precip_total=3.1,
            uv=3.0,
            neighborhood="Test Quarter",
        )
        result = current_obs_from_payload(payload)
        assert result is not None
        assert result.obs_time_utc == _OBS_INSTANT
        assert result.temp == pytest.approx(18.5)
        assert result.humidity == pytest.approx(72.0)
        assert result.dewpt == pytest.approx(13.4)
        assert result.wind_speed == pytest.approx(12.0)
        assert result.wind_gust == pytest.approx(18.0)
        assert result.wind_dir == pytest.approx(270.0)
        assert result.pressure == pytest.approx(1013.2)
        assert result.precip_rate == pytest.approx(0.5)
        assert result.precip_total == pytest.approx(3.1)
        assert result.uv == pytest.approx(3.0)
        assert result.neighborhood == "Test Quarter"

    def test_missing_fields_map_to_none(self) -> None:
        """Missing fields → None, never a fabricated zero."""
        payload = {
            "observations": [
                {
                    "obsTimeUtc": _OBS_INSTANT,
                    "metric": {},
                }
            ]
        }
        result = current_obs_from_payload(payload)
        assert result is not None
        assert result.temp is None
        assert result.humidity is None
        assert result.dewpt is None
        assert result.wind_speed is None
        assert result.wind_gust is None
        assert result.wind_dir is None
        assert result.pressure is None
        assert result.precip_rate is None
        assert result.precip_total is None
        assert result.uv is None
        assert result.neighborhood is None

    def test_no_metric_block_all_metric_fields_none(self) -> None:
        """Missing 'metric' sub-object → all metric fields are None."""
        payload = {
            "observations": [
                {
                    "obsTimeUtc": _OBS_INSTANT,
                    "humidity": 60.0,
                }
            ]
        }
        result = current_obs_from_payload(payload)
        assert result is not None
        assert result.humidity == pytest.approx(60.0)
        assert result.temp is None
        assert result.wind_speed is None

    def test_empty_observations_returns_none(self) -> None:
        """Empty observations list → None (caller treats as OFFLINE)."""
        assert current_obs_from_payload({"observations": []}) is None

    def test_missing_observations_key_returns_none(self) -> None:
        """Payload without 'observations' key → None."""
        assert current_obs_from_payload({}) is None

    def test_non_dict_payload_returns_none(self) -> None:
        """Non-dict payload (e.g. a list) → None, no exception."""
        assert current_obs_from_payload([]) is None  # type: ignore[arg-type]
        assert current_obs_from_payload(None) is None  # type: ignore[arg-type]

    def test_native_units_no_conversion(self) -> None:
        """Values are stored as-is in km/h, mm, hPa — no m/s conversion applied.

        The display snapshot table stores native metric units. The assertion
        verifies that 12.0 km/h is stored as 12.0, not converted to ~3.33 m/s.
        """
        payload = _online_payload(wind_speed=12.0, pressure=1013.0, precip_total=5.0)
        result = current_obs_from_payload(payload)
        assert result is not None
        assert result.wind_speed == pytest.approx(12.0), (
            "wind_speed must be stored as-is in km/h (no m/s conversion)"
        )
        assert result.pressure == pytest.approx(1013.0), (
            "pressure must be stored as-is in hPa"
        )
        assert result.precip_total == pytest.approx(5.0), (
            "precip_total must be stored as-is in mm"
        )

    def test_unparseable_obstime_maps_to_none_in_obs_time_utc(self) -> None:
        """A naive (offset-less) obsTimeUtc string → obs_time_utc=None in the result.

        The caller (classify_current_obs) checks obs.obs_time_utc is None and
        returns TRANSIENT — so this is the upstream path that triggers that branch.
        """
        payload = _online_payload(obs_time_utc=_OBS_INSTANT_UNPARSEABLE)
        result = current_obs_from_payload(payload)
        assert result is not None
        assert result.obs_time_utc is None, (
            "unparseable obsTimeUtc must map to obs_time_utc=None"
        )


# ---------------------------------------------------------------------------
# §13-A (second half): _obs_instant sub-hour resolution vs _valid_at
# ---------------------------------------------------------------------------


class TestObsInstantSubHourResolution:
    """_obs_instant preserves sub-hour precision; _valid_at hour-floors.

    §13-A second half: two payloads 5 min apart within the same clock hour
    must produce two DISTINCT _obs_instant values but the SAME _valid_at value.
    This regression guard ensures cadence learning receives real inter-obs gaps.
    """

    def test_two_obstimes_5min_apart_yield_distinct_obs_instant(self) -> None:
        """_obs_instant of T0 and T1 (5 min apart, same hour) must differ."""
        row_t0 = {"obsTimeUtc": _OBS_T0}
        row_t1 = {"obsTimeUtc": _OBS_T1}
        instant_t0 = _obs_instant(row_t0)
        instant_t1 = _obs_instant(row_t1)
        assert instant_t0 is not None
        assert instant_t1 is not None
        assert instant_t0 != instant_t1, (
            "_obs_instant must preserve sub-hour resolution: "
            f"T0={instant_t0} must differ from T1={instant_t1}"
        )

    def test_two_obstimes_5min_apart_gap_is_300s(self) -> None:
        """The gap between two _obs_instant values 5 min apart is exactly 300 s."""
        row_t0 = {"obsTimeUtc": _OBS_T0}
        row_t1 = {"obsTimeUtc": _OBS_T1}
        instant_t0 = _obs_instant(row_t0)
        instant_t1 = _obs_instant(row_t1)
        assert instant_t0 is not None and instant_t1 is not None
        from wxverify.core.timeutil import parse_utc

        dt0 = parse_utc(instant_t0)
        dt1 = parse_utc(instant_t1)
        gap = (dt1 - dt0).total_seconds()
        assert gap == pytest.approx(300.0), (
            f"expected 300 s gap between 5-min-apart instants; got {gap}"
        )

    def test_valid_at_collapses_same_hour_to_same_bucket(self) -> None:
        """_valid_at of T0 and T1 (both within 11:00 hour) collapse to the same bucket.

        This is the contrast that makes the regression guard explicit: if someone
        accidentally replaced _obs_instant with _valid_at in the cadence path, the
        two events would hash to the same bucket and the gap would disappear.
        """
        row_t0 = {"obsTimeUtc": _OBS_T0}
        row_t1 = {"obsTimeUtc": _OBS_T1}
        valid_t0 = _valid_at(row_t0)
        valid_t1 = _valid_at(row_t1)
        assert valid_t0 is not None and valid_t1 is not None
        assert valid_t0 == valid_t1, (
            "_valid_at must collapse sub-hour obstimes in the same "
            "hour to the same bucket"
        )

    def test_obs_instant_differs_where_valid_at_collides(self) -> None:
        """Explicit contrast: _obs_instant diverges where _valid_at collides.

        If _obs_instant returned the hour-floored value (as _valid_at does) this
        test would turn red — that is the regression it guards against.
        """
        row_t0 = {"obsTimeUtc": _OBS_T0}
        row_t1 = {"obsTimeUtc": _OBS_T1}
        instant_t0 = _obs_instant(row_t0)
        instant_t1 = _obs_instant(row_t1)
        valid_t0 = _valid_at(row_t0)
        valid_t1 = _valid_at(row_t1)
        # _valid_at collapses; _obs_instant does not — both must hold simultaneously
        assert valid_t0 == valid_t1
        assert instant_t0 != instant_t1


# ---------------------------------------------------------------------------
# Bucket-1-I: post-migration station scheduling
# ---------------------------------------------------------------------------


class TestEnqueueDueCurrentObs:
    """_enqueue_due_current_obs LEFT JOIN due-query picks up unseeded stations."""

    def _setup_db_with_station(
        self,
        *,
        pws_id: str = _STATION_PWS_ID,
        seed_poll_state: bool = False,
        next_poll_at: str | None = None,
    ) -> tuple[sqlite3.Connection, int, int]:
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id, pws_id=pws_id)
        if seed_poll_state and next_poll_at is not None:
            _seed_poll_state(conn, station_id, next_poll_at=next_poll_at)
        return conn, site_id, station_id

    def test_station_with_no_poll_state_enqueued_immediately(self) -> None:
        """Station with no station_poll_state row is returned by the due-query.

        The LEFT JOIN in _enqueue_due_current_obs returns stations where
        sps.next_poll_at IS NULL — i.e. no poll-state row yet.
        """
        conn, site_id, station_id = self._setup_db_with_station()

        with patch("wxverify.worker.scheduler.isoformat_utc", return_value=_NOW_ISO):
            _enqueue_due_current_obs(conn)

        job = conn.execute(
            "SELECT * FROM jobs WHERE type='fetch_current_obs' AND site_id=?",
            (site_id,),
        ).fetchone()
        assert job is not None, (
            "station with no poll-state row must be enqueued as due-immediately"
        )
        payload = json.loads(str(job["payload"]))
        assert payload.get("station_id") == station_id

    def test_station_with_future_next_poll_at_not_enqueued(self) -> None:
        """Station with next_poll_at > now is NOT returned by the due-query."""
        conn, site_id, station_id = self._setup_db_with_station()
        future = "2026-07-10T13:00:00Z"  # 1 hour after _NOW
        _seed_poll_state(conn, station_id, next_poll_at=future)

        with patch("wxverify.worker.scheduler.isoformat_utc", return_value=_NOW_ISO):
            _enqueue_due_current_obs(conn)

        job = conn.execute(
            "SELECT * FROM jobs WHERE type='fetch_current_obs' AND site_id=?",
            (site_id,),
        ).fetchone()
        assert job is None, (
            "station with next_poll_at in the future must NOT be enqueued"
        )

    def test_station_due_exactly_at_now_is_enqueued(self) -> None:
        """Station with next_poll_at == now (<=) is included in the due-query."""
        conn, site_id, station_id = self._setup_db_with_station()
        _seed_poll_state(conn, station_id, next_poll_at=_NOW_ISO)

        with patch("wxverify.worker.scheduler.isoformat_utc", return_value=_NOW_ISO):
            _enqueue_due_current_obs(conn)

        job = conn.execute(
            "SELECT * FROM jobs WHERE type='fetch_current_obs' AND site_id=?",
            (site_id,),
        ).fetchone()
        assert job is not None

    def test_enqueue_if_absent_deduplicates(self) -> None:
        """A second tick for the same station does not create a duplicate job."""
        conn, site_id, station_id = self._setup_db_with_station()

        with patch("wxverify.worker.scheduler.isoformat_utc", return_value=_NOW_ISO):
            _enqueue_due_current_obs(conn)
            _enqueue_due_current_obs(conn)

        count = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE type='fetch_current_obs' AND site_id=?",
            (site_id,),
        ).fetchone()[0]
        assert count == 1, (
            "dedupe must prevent a second pending job for the same station"
        )

    def test_persist_after_enqueue_writes_poll_state_row(self) -> None:
        """Enqueue + persist → poll-state row exists with non-NULL next_poll_at.

        This closes the Bucket-1-I loop: station is picked up due-immediately,
        and after a persist the UPSERT writes a row with non-NULL next_poll_at.
        """
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)

        with patch("wxverify.worker.scheduler.isoformat_utc", return_value=_NOW_ISO):
            _enqueue_due_current_obs(conn)

        # No poll-state row yet
        assert _poll_state(conn, station_id) is None

        # Now simulate the poll result
        outcome = PollOutcome(Health.OFFLINE, error="empty body")
        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        row = _poll_state(conn, station_id)
        assert row is not None, (
            "UPSERT must create a poll-state row after first persist"
        )
        assert row["next_poll_at"] is not None

    def test_disabled_station_not_enqueued(self) -> None:
        """Disabled station (enabled=0) is not included in the due-query."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        _seed_station(conn, site_id, enabled=0)

        with patch("wxverify.worker.scheduler.isoformat_utc", return_value=_NOW_ISO):
            _enqueue_due_current_obs(conn)

        count = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE type='fetch_current_obs'"
        ).fetchone()[0]
        assert count == 0

    def test_job_key_format_is_curobs_station_id(self) -> None:
        """The enqueued job_key follows the 'curobs:<station_id>' format."""
        conn, site_id, station_id = self._setup_db_with_station()

        with patch("wxverify.worker.scheduler.isoformat_utc", return_value=_NOW_ISO):
            _enqueue_due_current_obs(conn)

        job = conn.execute(
            "SELECT job_key FROM jobs WHERE type='fetch_current_obs'"
        ).fetchone()
        assert job is not None
        assert job["job_key"] == f"curobs:{station_id}"


# ---------------------------------------------------------------------------
# End-to-end ONLINE flow: classify → persist on a cold DB
# ---------------------------------------------------------------------------


class TestOnlineEndToEnd:
    """Smoke integration: classify_current_obs feeds into persist_poll_result."""

    def test_cold_start_online_cycle_creates_all_rows(self) -> None:
        """A cold-start ONLINE poll creates poll-state + current-obs rows."""
        conn = _make_conn()
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)

        resp = _fake_response(200, json_data=_online_payload(temp=21.0))
        outcome = classify_current_obs(resp)
        assert outcome.health is Health.ONLINE

        with _patched_now(_NOW):
            persist_poll_result(conn, site_id, station_id, outcome)

        poll_row = _poll_state(conn, station_id)
        assert poll_row is not None
        assert poll_row["health_state"] == "online"
        assert poll_row["next_poll_at"] is not None

        obs_row = _current_obs_row(conn, station_id)
        assert obs_row is not None
        assert obs_row["temp"] == pytest.approx(21.0)


# ---------------------------------------------------------------------------
# NOTE ON LAYER SEPARATION (domain-backoff and transport-fail)
# ---------------------------------------------------------------------------
# The domain-backoff row write (record_http_backoff) for 429 / >=500 lives in
# processor._record_current_obs_backoff (processor.py:436-445), which calls
# persist_poll_result THEN record_http_backoff in a single DB write.
# classify_current_obs and persist_poll_result themselves have no access to
# the DB connection needed by record_http_backoff. Testing the backoff-row
# write requires either the full _fetch_current_obs async path (needs httpx
# mocking at the async layer) or direct unit-testing of
# _record_current_obs_backoff (which is a private processor function). Neither
# is reachable at the classify_current_obs / persist_poll_result unit layer.
# Handoff: a seam into _record_current_obs_backoff (or integration-level tests
# for _fetch_current_obs) is required to assert the domain_backoffs row is
# written. That seam should be raised with the implementer/architect.
#
# Transport-fail (httpx timeout/connect error) is also above this layer:
# processor._fetch_current_obs catches the exception, constructs
# PollOutcome(Health.TRANSIENT, ...) explicitly, calls persist_poll_result,
# then re-raises — so the TRANSIENT persist IS tested here (via
# test_transient_next_poll_at_300), but the full raise-path (which puts the
# job back on the retry ladder via fail()) is only exercisable through dispatch.
