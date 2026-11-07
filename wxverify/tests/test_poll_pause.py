"""Part B: recheck a quiet station after an hour (plan `2026-11-07-health-
release.md` §2, test IDs BT1-BT4, BT7, BT8; BT5 lives in
`tests/test_raw_store.py` on its own fixture; BT6 lives below, reusing
`tests/test_station_lifecycle_routes.py`'s app harness; BT9 lives in
`tests/test_migration_v18.py`).

BT1-BT4 run `persist_poll_result` directly on `tests/test_sm3_current_obs.py`'s
`_make_conn` (an in-memory, fully-schemad SQLite connection) with the clock
fixed the same way that file's `_patched_now` does. BT6-BT8 run through the
real app (`tests/test_station_lifecycle_routes.py`'s `_setup_app`), a real,
file-backed `tmp_path` database, with the worker lane idled.

Synthetic data only (public repo): stations A-F in prose, made-up
`pws_station_id`s in code (`<A>`-style placeholders), sites in `UTC`.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.helpers import NOW
from tests.test_sm3_current_obs import (
    _current_obs_row,
    _fake_response,
    _make_conn,
    _online_payload,
    _patched_now,
    _poll_state,
    _seed_site,
    _seed_station,
)
from tests.test_station_lifecycle_routes import _csrf_headers, _setup_app
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.obs.pws_adapter import CurrentObservation
from wxverify.settings.keys import set_setting
from wxverify.web.context import poll_status_text
from wxverify.worker.current_obs import (
    MAX_BACKOFF_SECONDS,
    MIN_INTERVAL_SECONDS,
    Health,
    PollOutcome,
    classify_current_obs,
    ladder_delay,
    persist_poll_result,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _seed_paused(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    count: int,
    reason: str | None,
    health_state: str = "offline",
    next_poll_at: str | None = None,
    last_obstime: str | None = None,
) -> None:
    """Seed a `station_poll_state` row with a chosen pause count/reason.

    `tests/test_sm3_current_obs.py`'s own `_seed_poll_state` has no
    pause-column parameters (it predates Part B), so this is a small,
    local helper rather than an edit of that file.
    """
    conn.execute(
        "INSERT INTO station_poll_state"
        " (station_id, health_state, next_poll_at, last_obstime,"
        "  pause_reason, pause_evidence_count)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (station_id, health_state, next_poll_at, last_obstime, reason, count),
    )


# ---------------------------------------------------------------------------
# BT1: the ladder.
# ---------------------------------------------------------------------------


def test_bt1_ladder_delay_table() -> None:
    """Catches: an off-by-one in the hour table; no 24 h cap past rung 6;
    `ladder_delay(0)` not raising."""
    assert [ladder_delay(n).total_seconds() / 3600 for n in range(1, 9)] == [
        1,
        2,
        4,
        8,
        16,
        24,
        24,
        24,
    ]
    with pytest.raises(ValueError):
        ladder_delay(0)


def test_bt1_seven_offline_replies_climb_the_ladder() -> None:
    """Seven OFFLINE outcomes in a row, each at the clock the previous
    reply's own `next_poll_at` landed on. Catches: the 24 h park on the
    first empty reply (TODO step 2); an off-by-one (the first reply on
    2 h); the setting ignored (the second case below)."""
    conn = _make_conn()
    site_id = _seed_site(conn)
    station_id = _seed_station(conn, site_id)
    outcome = PollOutcome(Health.OFFLINE, error="empty body")
    expected_hours = [1, 2, 4, 8, 16, 24, 24]

    clock = datetime(2026, 7, 10, 0, 0, 0, tzinfo=UTC)
    for n, hours in enumerate(expected_hours, start=1):
        with _patched_now(clock):
            persist_poll_result(conn, site_id, station_id, outcome)
        row = _poll_state(conn, station_id)
        assert row is not None
        assert row["pause_evidence_count"] == n
        assert row["pause_reason"] == "offline"
        assert row["health_state"] == "offline"
        expected_next = clock + timedelta(hours=hours)
        actual_next = datetime.fromisoformat(row["next_poll_at"].replace("Z", "+00:00"))
        assert actual_next == expected_next, (
            f"reply {n}: expected next_poll_at {expected_next}, got {actual_next}"
        )
        clock = expected_next


def test_bt1_max_backoff_setting_caps_each_rung() -> None:
    """With `max_backoff_seconds` 7200 (2 h): 1, 2, 2, 2 h over four replies.
    Catches: the setting ignored (the module constant 86400 used instead)."""
    conn = _make_conn()
    site_id = _seed_site(conn)
    station_id = _seed_station(conn, site_id)
    set_setting(conn, "max_backoff_seconds", "7200")
    outcome = PollOutcome(Health.OFFLINE, error="empty body")
    expected_hours = [1, 2, 2, 2]

    clock = datetime(2026, 7, 10, 0, 0, 0, tzinfo=UTC)
    for n, hours in enumerate(expected_hours, start=1):
        with _patched_now(clock):
            persist_poll_result(conn, site_id, station_id, outcome)
        row = _poll_state(conn, station_id)
        assert row is not None
        assert row["pause_evidence_count"] == n
        expected_next = clock + timedelta(hours=hours)
        actual_next = datetime.fromisoformat(row["next_poll_at"].replace("Z", "+00:00"))
        assert actual_next == expected_next, (
            f"reply {n}: expected next_poll_at {expected_next}, got {actual_next}"
        )
        clock = expected_next


# ---------------------------------------------------------------------------
# BT2: 404 against other refusals.
# ---------------------------------------------------------------------------


def test_bt2_404_sets_evidence_other_refusals_do_not() -> None:
    """A TERMINAL http 404 counts as evidence, reason not_found, +1 h; a
    TERMINAL http 410 and one with no error leave the count at 0 and park
    for max_backoff; OFFLINE then http 404 on one station climbs to
    (2, 'not_found'). Catches: a 404 parked for 24 h; every 4xx counted;
    the reason kept from the first reply instead of switching to
    not_found."""
    conn = _make_conn()
    site_id = _seed_site(conn)
    now = datetime(2026, 7, 10, 0, 0, 0, tzinfo=UTC)

    station_404 = _seed_station(conn, site_id, pws_id="<A>")
    with _patched_now(now):
        persist_poll_result(
            conn,
            site_id,
            station_404,
            PollOutcome(Health.TERMINAL, error="http 404"),
        )
    row = _poll_state(conn, station_404)
    assert row is not None
    assert (row["pause_evidence_count"], row["pause_reason"]) == (1, "not_found")
    actual = datetime.fromisoformat(row["next_poll_at"].replace("Z", "+00:00"))
    assert actual == now + timedelta(hours=1)

    station_410 = _seed_station(conn, site_id, pws_id="<C>")
    with _patched_now(now):
        persist_poll_result(
            conn,
            site_id,
            station_410,
            PollOutcome(Health.TERMINAL, error="http 410"),
        )
    row_410 = _poll_state(conn, station_410)
    assert row_410 is not None
    assert (row_410["pause_evidence_count"], row_410["pause_reason"]) == (0, None)
    actual_410 = datetime.fromisoformat(row_410["next_poll_at"].replace("Z", "+00:00"))
    assert actual_410 == now + timedelta(seconds=MAX_BACKOFF_SECONDS)

    station_no_error = _seed_station(conn, site_id, pws_id="<D>")
    with _patched_now(now):
        persist_poll_result(
            conn,
            site_id,
            station_no_error,
            PollOutcome(Health.TERMINAL, error=None),
        )
    row_none = _poll_state(conn, station_no_error)
    assert row_none is not None
    assert (row_none["pause_evidence_count"], row_none["pause_reason"]) == (0, None)

    station_b = _seed_station(conn, site_id, pws_id="<B>")
    with _patched_now(now):
        persist_poll_result(
            conn, site_id, station_b, PollOutcome(Health.OFFLINE, error="empty body")
        )
    with _patched_now(now + timedelta(hours=1)):
        persist_poll_result(
            conn,
            site_id,
            station_b,
            PollOutcome(Health.TERMINAL, error="http 404"),
        )
    row_b = _poll_state(conn, station_b)
    assert row_b is not None
    assert (row_b["pause_evidence_count"], row_b["pause_reason"]) == (2, "not_found")


# ---------------------------------------------------------------------------
# BT3: a timeout while paused.
# ---------------------------------------------------------------------------


def test_bt3_transient_while_paused_does_not_touch_the_ladder() -> None:
    """A station seeded at (2, 'offline'): a TRANSIENT http 503 gives +2 h
    and leaves (2, 'offline'); at count 0, a TRANSIENT gives +min_interval.
    Catches: a paused station retried every 5 min; a transient reply that
    resets or advances the ladder."""
    conn = _make_conn()
    site_id = _seed_site(conn)
    now = datetime(2026, 7, 10, 0, 0, 0, tzinfo=UTC)

    station_paused = _seed_station(conn, site_id, pws_id="<A>")
    _seed_paused(
        conn,
        station_paused,
        count=2,
        reason="offline",
        health_state="offline",
        next_poll_at=isoformat_utc(now),
    )
    with _patched_now(now):
        persist_poll_result(
            conn,
            site_id,
            station_paused,
            PollOutcome(Health.TRANSIENT, error="http 503"),
        )
    row = _poll_state(conn, station_paused)
    assert row is not None
    assert (row["pause_evidence_count"], row["pause_reason"]) == (2, "offline")
    assert row["health_state"] == "offline"
    assert row["last_error"] == "http 503"
    actual = datetime.fromisoformat(row["next_poll_at"].replace("Z", "+00:00"))
    assert actual == now + timedelta(hours=2)

    station_healthy = _seed_station(conn, site_id, pws_id="<B>")
    with _patched_now(now):
        persist_poll_result(
            conn,
            site_id,
            station_healthy,
            PollOutcome(Health.TRANSIENT, error="http 503"),
        )
    row_healthy = _poll_state(conn, station_healthy)
    assert row_healthy is not None
    assert (row_healthy["pause_evidence_count"], row_healthy["pause_reason"]) == (
        0,
        None,
    )
    assert row_healthy["health_state"] == "transient"
    actual_healthy = datetime.fromisoformat(
        row_healthy["next_poll_at"].replace("Z", "+00:00")
    )
    assert actual_healthy == now + timedelta(seconds=MIN_INTERVAL_SECONDS)


def test_bt3_timeout_then_stale_reply_keeps_the_paused_health() -> None:
    """A station seeded at (2, 'offline'): a TRANSIENT http 503, then a
    stale ONLINE reply (61 min old), lands at (3, 'offline') with
    `health_state` still `offline`. Catches: a timeout while paused writing
    `health_state` `transient` -- the close monitor's silent-station check
    reads `health_state`, not `pause_reason`, so a station the ladder still
    considers paused would drop out of that check while its reason and
    count say otherwise."""
    conn = _make_conn()
    site_id = _seed_site(conn)
    now = datetime(2026, 7, 10, 0, 0, 0, tzinfo=UTC)

    station_id = _seed_station(conn, site_id, pws_id="<A>")
    _seed_paused(
        conn,
        station_id,
        count=2,
        reason="offline",
        health_state="offline",
        next_poll_at=isoformat_utc(now),
    )
    with _patched_now(now):
        persist_poll_result(
            conn,
            site_id,
            station_id,
            PollOutcome(Health.TRANSIENT, error="http 503"),
        )
    row_after_timeout = _poll_state(conn, station_id)
    assert row_after_timeout is not None
    assert row_after_timeout["health_state"] == "offline"

    obs_stale = isoformat_utc(now - timedelta(minutes=61))
    outcome_stale = classify_current_obs(
        _fake_response(200, json_data=_online_payload(obs_time_utc=obs_stale))
    )
    with _patched_now(now):
        persist_poll_result(conn, site_id, station_id, outcome_stale)

    row = _poll_state(conn, station_id)
    assert row is not None
    assert (row["pause_evidence_count"], row["pause_reason"]) == (3, "offline")
    assert row["health_state"] == "offline"


# ---------------------------------------------------------------------------
# BT4: the fresh-reading test.
# ---------------------------------------------------------------------------


def test_bt4_fresh_reading_test_60_minute_boundary() -> None:
    """A station seeded at (2, 'offline'). Catches: `<` in place of `<=` at
    the 60-minute boundary; a stale reply that ends the pause; `online`
    written for a stale reading (Home Assistant would show the station
    connected); the fresh-reading test applied at count 0."""
    conn = _make_conn()
    site_id = _seed_site(conn)
    now = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)

    # Exactly 60 min old: fresh, ends the pause and writes online.
    station_a = _seed_station(conn, site_id, pws_id="<A>")
    _seed_paused(
        conn,
        station_a,
        count=2,
        reason="offline",
        health_state="offline",
        next_poll_at=isoformat_utc(now),
    )
    obs_60 = isoformat_utc(now - timedelta(minutes=60))
    outcome = classify_current_obs(
        _fake_response(200, json_data=_online_payload(obs_time_utc=obs_60))
    )
    with _patched_now(now):
        persist_poll_result(conn, site_id, station_a, outcome)
    row_a = _poll_state(conn, station_a)
    assert row_a is not None
    assert (row_a["pause_evidence_count"], row_a["pause_reason"]) == (0, None)
    assert row_a["health_state"] == "online"
    assert _current_obs_row(conn, station_a) is not None

    # 60 min 1 s old: stale, climbs one more rung, health_state stays offline.
    station_b = _seed_station(conn, site_id, pws_id="<B>")
    _seed_paused(
        conn,
        station_b,
        count=2,
        reason="offline",
        health_state="offline",
        next_poll_at=isoformat_utc(now),
        last_obstime=None,
    )
    obs_stale = isoformat_utc(now - timedelta(minutes=60, seconds=1))
    outcome_stale = classify_current_obs(
        _fake_response(200, json_data=_online_payload(obs_time_utc=obs_stale))
    )
    with _patched_now(now):
        persist_poll_result(conn, site_id, station_b, outcome_stale)
    row_b = _poll_state(conn, station_b)
    assert row_b is not None
    assert (row_b["pause_evidence_count"], row_b["pause_reason"]) == (3, "offline")
    assert row_b["health_state"] == "offline"
    assert row_b["last_error"] == "stale observation (age 60 min)"
    assert row_b["last_obstime"] is None, "a stale reply must not move last_obstime"
    assert _current_obs_row(conn, station_b) is None, (
        "a stale reply must not write station_current_obs"
    )
    actual_b = datetime.fromisoformat(row_b["next_poll_at"].replace("Z", "+00:00"))
    assert actual_b == now + timedelta(hours=4)

    # 61 min ahead: the ahead-text branch, still climbing the ladder.
    station_c = _seed_station(conn, site_id, pws_id="<C>")
    _seed_paused(
        conn,
        station_c,
        count=2,
        reason="offline",
        health_state="offline",
        next_poll_at=isoformat_utc(now),
    )
    obs_ahead = isoformat_utc(now + timedelta(minutes=61))
    outcome_ahead = classify_current_obs(
        _fake_response(200, json_data=_online_payload(obs_time_utc=obs_ahead))
    )
    with _patched_now(now):
        persist_poll_result(conn, site_id, station_c, outcome_ahead)
    row_c = _poll_state(conn, station_c)
    assert row_c is not None
    assert row_c["pause_evidence_count"] == 3
    assert row_c["last_error"] == "observation dated 61 min ahead"

    # The same stale reply at count 0: today's online write, untouched by
    # the fresh-reading test.
    station_d = _seed_station(conn, site_id, pws_id="<D>")
    outcome_stale_count0 = classify_current_obs(
        _fake_response(200, json_data=_online_payload(obs_time_utc=obs_stale))
    )
    with _patched_now(now):
        persist_poll_result(conn, site_id, station_d, outcome_stale_count0)
    row_d = _poll_state(conn, station_d)
    assert row_d is not None
    assert row_d["health_state"] == "online"
    assert row_d["pause_evidence_count"] == 0
    assert _current_obs_row(conn, station_d) is not None


def test_bt4_unparseable_obs_instant_at_count_1() -> None:
    """Builder choice (not itemized by BT4): an ONLINE outcome whose
    `obs_instant` `parse_utc` rejects, at count 1, gives `last_error`
    `stale observation (age unknown)`. `classify_current_obs` never
    produces this -- it classifies an unparseable obstime TRANSIENT before
    persist_poll_result is ever reached -- so this exercises
    `_stale_reading_error`'s own defensive branch directly, by constructing
    the outcome rather than deriving it from a fake HTTP response.
    Catches: the unparseable branch missing its own text, or raising
    instead of degrading to the age-unknown message."""
    conn = _make_conn()
    site_id = _seed_site(conn)
    station_id = _seed_station(conn, site_id)
    now = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)
    _seed_paused(
        conn,
        station_id,
        count=1,
        reason="offline",
        health_state="offline",
        next_poll_at=isoformat_utc(now),
    )
    bogus_obs = CurrentObservation(
        obs_time_utc="not-a-stamp",
        temp=None,
        humidity=None,
        dewpt=None,
        wind_speed=None,
        wind_gust=None,
        wind_dir=None,
        pressure=None,
        precip_rate=None,
        precip_total=None,
        uv=None,
        neighborhood=None,
    )
    outcome = PollOutcome(Health.ONLINE, obs=bogus_obs, obs_instant="not-a-stamp")
    with _patched_now(now):
        persist_poll_result(conn, site_id, station_id, outcome)
    row = _poll_state(conn, station_id)
    assert row is not None
    assert row["last_error"] == "stale observation (age unknown)"
    assert row["pause_evidence_count"] == 2


# ---------------------------------------------------------------------------
# BT7: the cell.
# ---------------------------------------------------------------------------


def test_bt7_poll_status_text_table() -> None:
    """Catches: empty reply and station error not told apart (the TODO's
    "told apart"); a wrong boundary between count 1 and count 2 text; the
    UTC-fallback suffix or the unknown text missing."""
    now_iso = "2026-07-10T13:00:00Z"
    assert poll_status_text("offline", 1, now_iso, zone_name="UTC") == (
        "Rechecking after an empty reply · next check 2026-07-10 13:00"
    )
    assert poll_status_text("offline", 2, now_iso, zone_name="UTC") == (
        "Current-condition polling paused (empty reply) · next check 2026-07-10 13:00"
    )
    assert poll_status_text("not_found", 1, now_iso, zone_name="UTC") == (
        "Rechecking after station not found (http 404) · next check 2026-07-10 13:00"
    )
    assert poll_status_text("not_found", 2, now_iso, zone_name="UTC") == (
        "Current-condition polling paused (station not found, http 404) ·"
        " next check 2026-07-10 13:00"
    )
    assert poll_status_text("offline", 0, now_iso, zone_name="UTC") is None
    assert poll_status_text(None, 1, now_iso, zone_name="UTC") is None
    assert poll_status_text("offline", 1, now_iso, zone_name="Not/AZone") == (
        "Rechecking after an empty reply · next check 2026-07-10 13:00 UTC"
    )
    assert poll_status_text("offline", 1, None, zone_name="UTC") == (
        "Rechecking after an empty reply · next check unknown"
    )
    assert poll_status_text("offline", 1, "not-a-stamp", zone_name="UTC") == (
        "Rechecking after an empty reply · next check unknown"
    )


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 -- test seeds the real writer conn


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _make_app(monkeypatch: pytest.MonkeyPatch) -> object:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _make_site(conn: sqlite3.Connection, name: str, *, enabled: int = 1) -> int:
    return int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone, enabled) VALUES (?, 40.0, -105.0, 900.0, 'UTC', ?)",
            (name, enabled),
        ).lastrowid
    )


def _insert_station(
    conn: sqlite3.Connection, site_id: int, pws_station_id: str, *, enabled: int = 1
) -> int:
    return int(
        conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m, enabled) VALUES (?, ?, 40.0, -105.0, 900.0, ?)",
            (site_id, pws_station_id, enabled),
        ).lastrowid
    )


def _row_for_station(html: str, pws_station_id: str) -> str:
    start = html.index(f"<td>{pws_station_id}</td>")
    end = html.index("</tr>", start)
    return html[start:end]


def _retry_button(row: str) -> str:
    end = row.index(">Retry now</button>")
    return row[row.rindex("<button", 0, end) : end]


def test_bt7_rendered_page_shows_status_and_the_retry_button(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A at count 1 and B at count 2 show their own text with a Retry now
    button; a disabled D at count 2, and E at count 2 under a disabled
    second site, show neither; F at count 0 shows no text and no button.
    Catches: the button on a healthy row; a paused text on a disabled
    station; the status text and button gated differently; the Retry now
    button losing its own json-enc or CSRF attribute."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "On Site")
    station_a = _insert_station(conn, site_id, "STN-A")
    station_b = _insert_station(conn, site_id, "STN-B")
    station_d = _insert_station(conn, site_id, "STN-D", enabled=0)
    _insert_station(conn, site_id, "STN-F")  # count 0: no poll-state row.
    _seed_paused(
        conn,
        station_a,
        count=1,
        reason="offline",
        health_state="offline",
        next_poll_at=isoformat_utc(NOW + timedelta(hours=1)),
    )
    _seed_paused(
        conn,
        station_b,
        count=2,
        reason="not_found",
        health_state="terminal",
        next_poll_at=isoformat_utc(NOW + timedelta(hours=2)),
    )
    _seed_paused(
        conn,
        station_d,
        count=2,
        reason="offline",
        health_state="offline",
        next_poll_at=isoformat_utc(NOW + timedelta(hours=2)),
    )
    off_site_id = _make_site(conn, "Off Site", enabled=0)
    station_e = _insert_station(conn, off_site_id, "STN-E")
    _seed_paused(
        conn,
        station_e,
        count=2,
        reason="offline",
        health_state="offline",
        next_poll_at=isoformat_utc(NOW + timedelta(hours=2)),
    )
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/sites")
    assert response.status_code == 200
    html = response.text
    close_db()

    row_a = _row_for_station(html, "STN-A")
    assert "Rechecking after an empty reply" in row_a
    assert "Retry now" in row_a
    retry_a = _retry_button(row_a)
    assert 'hx-ext="json-enc"' in retry_a
    assert 'hx-headers=\'{"X-CSRF-Token":' in retry_a

    row_b = _row_for_station(html, "STN-B")
    assert "Current-condition polling paused (station not found, http 404)" in row_b
    assert "Retry now" in row_b
    retry_b = _retry_button(row_b)
    assert 'hx-ext="json-enc"' in retry_b
    assert 'hx-headers=\'{"X-CSRF-Token":' in retry_b

    row_d = _row_for_station(html, "STN-D")
    assert "Rechecking" not in row_d
    assert "Current-condition polling paused" not in row_d
    assert "Retry now" not in row_d

    row_e = _row_for_station(html, "STN-E")
    assert "Rechecking" not in row_e
    assert "Current-condition polling paused" not in row_e
    assert "Retry now" not in row_e

    row_f = _row_for_station(html, "STN-F")
    assert "Rechecking" not in row_f
    assert "Current-condition polling paused" not in row_f
    assert "Retry now" not in row_f


# ---------------------------------------------------------------------------
# BT6: Retry now (api/routes/stations.py's retry_station_now).
# ---------------------------------------------------------------------------

_RETRY_NOW_PATH = "/api/sites/{site_id}/stations/{station_id}/retry-now"


def _seed_site_for_retry(conn: sqlite3.Connection, *, enabled: int = 1) -> int:
    return int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone, enabled) VALUES ('Retry Site', 0.0, 0.0, 0.0, 'UTC', ?)",
            (enabled,),
        ).lastrowid
    )


def _seed_station_for_retry(
    conn: sqlite3.Connection,
    site_id: int,
    pws_id: str,
    *,
    enabled: int = 1,
    retired_at: str | None = None,
) -> int:
    return int(
        conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m, enabled, retired_at)"
            " VALUES (?, ?, 0.0, 0.0, 0.0, ?, ?)",
            (site_id, pws_id, enabled, retired_at),
        ).lastrowid
    )


def _seed_paused_for_retry(
    conn: sqlite3.Connection, station_id: int, *, count: int, next_poll_at: str
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state (station_id, health_state, next_poll_at,"
        " pause_reason, pause_evidence_count) VALUES (?, 'offline', ?, 'offline', ?)",
        (station_id, next_poll_at, count),
    )


def _seed_healthy_polled_for_retry(
    conn: sqlite3.Connection, station_id: int, *, next_poll_at: str
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state (station_id, health_state, next_poll_at,"
        " pause_reason, pause_evidence_count) VALUES (?, 'online', ?, NULL, 0)",
        (station_id, next_poll_at),
    )


def _seed_failed_job(
    conn: sqlite3.Connection, site_id: int, station_id: int, *, updated_at: str
) -> None:
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, status,"
        " next_attempt_at, updated_at) VALUES"
        " ('fetch_current_obs', ?, ?, '{}', 'failed', ?, ?)",
        (site_id, f"curobs:{station_id}", updated_at, updated_at),
    )


def _pending_curobs_jobs(site_id: int, station_id: int) -> int:
    row = get_db().read_sync(
        lambda conn: conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE site_id=? AND job_key=?"
            " AND status='pending'",
            (site_id, f"curobs:{station_id}"),
        ).fetchone()
    )
    return int(row["n"])


def _poll_next_poll_at(station_id: int) -> str | None:
    row = get_db().read_sync(
        lambda conn: conn.execute(
            "SELECT next_poll_at FROM station_poll_state WHERE station_id=?",
            (station_id,),
        ).fetchone()
    )
    return None if row is None else row["next_poll_at"]


def test_bt6_retry_now_route(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """C at (2, 'offline') with a failed curobs:<id> job updated 10 min
    ago: a POST with the CSRF header gives 200, one pending curobs:<id>
    job, next_poll_at now; a second POST adds no job. Without the header:
    403, nothing written. A disabled station, a station of a disabled
    site, a station at count 0 (no poll-state row), a station at count 0
    with a poll-state row (health_state 'online', a healthy polled
    station): 409, no job, next_poll_at unchanged. A retired station: 404.
    With HX-Request: true: the cluster fragment.
    Catches: the cooldown variant (no job inside the hour, since the
    failed job is only 10 min old); a press that spends a call on a
    healthy station, including one that has been polled and has its own
    poll-state row; a refusal that writes first."""
    app = _setup_app(tmp_path, monkeypatch, "bt6")
    with TestClient(app) as client:
        headers = _csrf_headers(client)

        def _seed(conn: sqlite3.Connection) -> dict[str, int]:
            site_id = _seed_site_for_retry(conn)
            station_c = _seed_station_for_retry(conn, site_id, "<C>")
            _seed_paused_for_retry(
                conn,
                station_c,
                count=2,
                next_poll_at=isoformat_utc(NOW + timedelta(hours=2)),
            )
            _seed_failed_job(
                conn,
                site_id,
                station_c,
                updated_at=isoformat_utc(NOW - timedelta(minutes=10)),
            )

            disabled_station = _seed_station_for_retry(conn, site_id, "<D>", enabled=0)
            _seed_paused_for_retry(
                conn,
                disabled_station,
                count=2,
                next_poll_at=isoformat_utc(NOW + timedelta(hours=2)),
            )

            disabled_site_id = _seed_site_for_retry(conn, enabled=0)
            station_in_disabled_site = _seed_station_for_retry(
                conn, disabled_site_id, "<E>"
            )
            _seed_paused_for_retry(
                conn,
                station_in_disabled_site,
                count=2,
                next_poll_at=isoformat_utc(NOW + timedelta(hours=2)),
            )

            healthy_station = _seed_station_for_retry(conn, site_id, "<F>")

            healthy_polled_station = _seed_station_for_retry(conn, site_id, "<G>")
            _seed_healthy_polled_for_retry(
                conn,
                healthy_polled_station,
                next_poll_at=isoformat_utc(NOW + timedelta(minutes=5)),
            )

            retired_station = _seed_station_for_retry(
                conn,
                site_id,
                "<A>",
                retired_at=isoformat_utc(NOW - timedelta(days=1)),
            )
            _seed_paused_for_retry(
                conn,
                retired_station,
                count=2,
                next_poll_at=isoformat_utc(NOW + timedelta(hours=2)),
            )
            conn.commit()
            return {
                "site_id": site_id,
                "station_c": station_c,
                "disabled_station": disabled_station,
                "disabled_site_id": disabled_site_id,
                "station_in_disabled_site": station_in_disabled_site,
                "healthy_station": healthy_station,
                "healthy_polled_station": healthy_polled_station,
                "retired_station": retired_station,
            }

        ids = get_db().write_sync(_seed)
        site_id = ids["site_id"]
        station_c = ids["station_c"]

        resp = client.post(
            _RETRY_NOW_PATH.format(site_id=site_id, station_id=station_c),
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"queued": True}
        assert _pending_curobs_jobs(site_id, station_c) == 1
        assert _poll_next_poll_at(station_c) == isoformat_utc(NOW)

        # A second press absorbs into the already-queued job.
        resp2 = client.post(
            _RETRY_NOW_PATH.format(site_id=site_id, station_id=station_c),
            headers=headers,
        )
        assert resp2.status_code == 200, resp2.text
        assert resp2.json() == {"queued": False}
        assert _pending_curobs_jobs(site_id, station_c) == 1

        # Without the CSRF header: refused before anything is written.
        resp_no_csrf = client.post(
            _RETRY_NOW_PATH.format(site_id=site_id, station_id=station_c),
        )
        assert resp_no_csrf.status_code == 403
        assert _pending_curobs_jobs(site_id, station_c) == 1

        # A disabled station.
        resp_disabled = client.post(
            _RETRY_NOW_PATH.format(site_id=site_id, station_id=ids["disabled_station"]),
            headers=headers,
        )
        assert resp_disabled.status_code == 409
        assert resp_disabled.json()["error"] == "station is disabled"
        assert _pending_curobs_jobs(site_id, ids["disabled_station"]) == 0
        assert _poll_next_poll_at(ids["disabled_station"]) == isoformat_utc(
            NOW + timedelta(hours=2)
        )

        # A station of a disabled site.
        resp_disabled_site = client.post(
            _RETRY_NOW_PATH.format(
                site_id=ids["disabled_site_id"],
                station_id=ids["station_in_disabled_site"],
            ),
            headers=headers,
        )
        assert resp_disabled_site.status_code == 409
        assert resp_disabled_site.json()["error"] == "site is disabled"
        assert (
            _pending_curobs_jobs(
                ids["disabled_site_id"], ids["station_in_disabled_site"]
            )
            == 0
        )
        assert _poll_next_poll_at(ids["station_in_disabled_site"]) == isoformat_utc(
            NOW + timedelta(hours=2)
        )

        # A station at count 0 (no poll-state row).
        resp_healthy = client.post(
            _RETRY_NOW_PATH.format(site_id=site_id, station_id=ids["healthy_station"]),
            headers=headers,
        )
        assert resp_healthy.status_code == 409
        assert resp_healthy.json()["error"] == "station is not paused"
        assert _pending_curobs_jobs(site_id, ids["healthy_station"]) == 0

        # A station at count 0 that HAS been polled (a poll-state row with
        # health_state 'online' and count 0, not a missing row).
        healthy_polled_next_poll_at = isoformat_utc(NOW + timedelta(minutes=5))
        resp_healthy_polled = client.post(
            _RETRY_NOW_PATH.format(
                site_id=site_id, station_id=ids["healthy_polled_station"]
            ),
            headers=headers,
        )
        assert resp_healthy_polled.status_code == 409
        assert resp_healthy_polled.json()["error"] == "station is not paused"
        assert _pending_curobs_jobs(site_id, ids["healthy_polled_station"]) == 0
        assert (
            _poll_next_poll_at(ids["healthy_polled_station"])
            == healthy_polled_next_poll_at
        )

        # A retired station.
        resp_retired = client.post(
            _RETRY_NOW_PATH.format(site_id=site_id, station_id=ids["retired_station"]),
            headers=headers,
        )
        assert resp_retired.status_code == 404
        assert resp_retired.json()["error"] == "station not found"

        # HX-Request: true answers the cluster fragment.
        resp_hx = client.post(
            _RETRY_NOW_PATH.format(site_id=site_id, station_id=station_c),
            headers={**headers, "HX-Request": "true"},
        )
        assert resp_hx.status_code == 200, resp_hx.text
        assert f'id="station-cluster-{site_id}"' in resp_hx.text


def test_bt6_missing_site_answers_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Builder choice (not itemized by BT6): a station whose site row is
    gone answers 404 'site not found', not a 5xx. The site row is removed
    with `foreign_keys` off, since the column's own `ON DELETE CASCADE`
    makes this otherwise unreachable while the pragma is on. Catches: an
    unguarded sites lookup turning a dangling site_id into an unhandled
    500."""
    app = _setup_app(tmp_path, monkeypatch, "bt6-missing-site")
    with TestClient(app) as client:
        headers = _csrf_headers(client)

        def _seed(conn: sqlite3.Connection) -> dict[str, int]:
            site_id = _seed_site_for_retry(conn)
            station_id = _seed_station_for_retry(conn, site_id, "<A>")
            _seed_paused_for_retry(
                conn, station_id, count=2, next_poll_at=isoformat_utc(NOW)
            )
            return {"site_id": site_id, "station_id": station_id}

        ids = get_db().write_sync(_seed)

        # The FK pragma is a no-op inside a transaction, so the toggle and
        # the delete run directly on the writer's own (autocommit) handle,
        # outside write_sync's BEGIN IMMEDIATE/commit wrapper.
        writer_conn = get_db()._conn  # noqa: SLF001 -- test needs the raw handle
        writer_conn.execute("PRAGMA foreign_keys = OFF")
        writer_conn.execute("DELETE FROM sites WHERE id=?", (ids["site_id"],))
        writer_conn.execute("PRAGMA foreign_keys = ON")

        resp = client.post(
            _RETRY_NOW_PATH.format(
                site_id=ids["site_id"], station_id=ids["station_id"]
            ),
            headers=headers,
        )
        assert resp.status_code == 404
        assert resp.json()["error"] == "site not found"


# ---------------------------------------------------------------------------
# BT8: Home Assistant's contract (/api/observations/current).
# ---------------------------------------------------------------------------


def test_bt8_observations_current_reflects_the_pause_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A at (2, 'offline') after a stale reply: health_state offline,
    provider_reported_offline true; after a fresh reply: online, false.
    Catches: a field rename, or a stale reply surfacing as online, which
    the integration's availability contract needs no change for only
    while it holds."""
    app = _setup_app(tmp_path, monkeypatch, "bt8")
    with TestClient(app) as client:

        def _seed(conn: sqlite3.Connection) -> dict[str, int]:
            site_id = _seed_site_for_retry(conn)
            station_id = _seed_station_for_retry(conn, site_id, "<A>")
            _seed_paused_for_retry(
                conn, station_id, count=2, next_poll_at=isoformat_utc(NOW)
            )
            conn.commit()
            return {"site_id": site_id, "station_id": station_id}

        ids = get_db().write_sync(_seed)
        site_id, station_id = ids["site_id"], ids["station_id"]

        stale_instant = isoformat_utc(NOW - timedelta(minutes=61))
        stale_outcome = classify_current_obs(
            _fake_response(200, json_data=_online_payload(obs_time_utc=stale_instant))
        )

        def _stale(conn: sqlite3.Connection) -> None:
            with _patched_now(NOW):
                persist_poll_result(conn, site_id, station_id, stale_outcome)
            conn.commit()

        get_db().write_sync(_stale)

        resp = client.get(f"/api/observations/current?station={station_id}")
        assert resp.status_code == 200, resp.text
        body = next(row for row in resp.json() if row["station_id"] == station_id)
        assert body["health_state"] == "offline"
        assert body["provider_reported_offline"] is True

        fresh_instant = isoformat_utc(NOW)
        fresh_outcome = classify_current_obs(
            _fake_response(200, json_data=_online_payload(obs_time_utc=fresh_instant))
        )

        def _fresh(conn: sqlite3.Connection) -> None:
            with _patched_now(NOW):
                persist_poll_result(conn, site_id, station_id, fresh_outcome)
            conn.commit()

        get_db().write_sync(_fresh)

        resp2 = client.get(f"/api/observations/current?station={station_id}")
        assert resp2.status_code == 200, resp2.text
        body2 = next(row for row in resp2.json() if row["station_id"] == station_id)
        assert body2["health_state"] == "online"
        assert body2["provider_reported_offline"] is False
