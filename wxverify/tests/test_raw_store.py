"""Tests: storage, commit 4 of P2 of the daily close (plan 2026-10-21-daily-
close-download-and-raw-store.md, section 14.5, RS1-RS7).

Covers ``wxverify.db.station_readings.store_readings`` writing the widened
``station_wind_records`` table through P1's closed-day guard (section 6),
the lane's today row (RS1, RS2, RS6) and the current-conditions poll's
storage (RS5, RS7). Every station id below is synthetic (public repo):
``ISTATION01`` to ``ISTATION06``, standing for A to F; the weather.com key
is the CI placeholder; zones are ``UTC`` only.

No production code for P2 exists yet, so every test here fails at
collection with an ``ImportError`` naming a P2 module
(``wxverify.db.station_readings`` or ``wxverify.obs.hourly_values``), not
for an unrelated reason.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import httpx
import pytest

from wxverify import config
from wxverify.core.timeutil import isoformat_utc, local_day_slots
from wxverify.db.connection import Database, FencedWriter, close_db, init_db
from wxverify.db.migrations import run_migrations
from wxverify.db.site_days import (
    WriteKind,
    check_day_write,
    ensure_site_days,
    start_site_days,
)
from wxverify.db.station_readings import StoreResult, store_readings
from wxverify.obs.hourly_values import StationReading
from wxverify.worker.access_hold import read_access_record, record_access_refused
from wxverify.worker.current_obs import (
    Health,
    classify_current_obs,
    end_poll_pause_on_fresh_reading,
)
from wxverify.worker.processor import (  # noqa: PLC2701 -- private, exercised directly
    _persist_poll_result_clearing,
)
from wxverify.worker.wind_days import (  # noqa: PLC2701 -- private, exercised directly
    run_wind_days,
)

_API_KEY = "0123456789abcdef0123456789abcdef"


def _make_plain_site(
    conn: sqlite3.Connection, *, pws_id: str, today: date
) -> tuple[int, int]:
    """One enabled site and one enabled station, with a P1 record covering
    ``today`` -- the daily close is always on, so P1's guard needs a real
    record holding an instant before it accepts a ``READING``/``HOURLY``
    write of that instant."""
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Raw Store Test Site', 0.0, 0.0, 0.0, 'UTC')"
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    cur = conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
        " VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
        (site_id, pws_id),
    )
    assert cur.lastrowid is not None
    station_id = int(cur.lastrowid)
    start_site_days(
        conn,
        site_id,
        today,
        now_utc=datetime(today.year, today.month, today.day, tzinfo=UTC),
    )
    conn.commit()
    return site_id, station_id


def _init_tmp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Database:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)
    return init_db(str(db_path))


def _freeze_wind_lane(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.domain_backoff.utc_now", lambda: when)


def _run_wind_chunk(
    db: Database,
    site_id: int,
    handler: Callable[[httpx.Request], httpx.Response],
) -> None:
    real_client = httpx.AsyncClient
    writer = FencedWriter(db)
    with patch(
        "wxverify.worker.wind_days.httpx.AsyncClient",
        lambda *a, **kw: real_client(  # type: ignore[arg-type]
            transport=httpx.MockTransport(handler)
        ),
    ):
        asyncio.run(run_wind_days(db, writer, site_id, client=None))


def _five_minute_response(
    readings: list[tuple[datetime, float, float, float]],
) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "observations": [
                {
                    "valid_time_gmt": int(instant.timestamp()),
                    "obsTimeLocal": instant.strftime("%Y-%m-%d %H:%M:%S"),
                    "metric": {
                        "tempAvg": temp,
                        "precipTotal": rain,
                        "windspeedAvg": wind,
                    },
                }
                for instant, temp, rain, wind in readings
            ]
        },
    )


# =============================================================================
# RS1 -- a full 5-minute reply for B stored by the lane's today row.
# =============================================================================


def test_rs1_full_five_minute_reply_stores_every_quantity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan 14.5 RS1: every reading of a full 5-minute reply is stored with
    source ``five_minute``, its three values and local date, ``fetched_at``
    NOW -- not wind alone, as today's lane-only store does."""
    from tests.helpers import NOW, D, day_close_site

    db = _init_tmp_db(tmp_path, monkeypatch)
    conn = db._conn  # noqa: SLF001 -- real writer connection, as other tests do
    site_id = day_close_site(conn)
    conn.execute("UPDATE stations SET enabled = 0 WHERE pws_station_id = 'ISTATION01'")
    station_b = conn.execute(
        "SELECT id FROM stations WHERE pws_station_id = 'ISTATION02'"
    ).fetchone()["id"]
    conn.commit()
    _freeze_wind_lane(monkeypatch, NOW)

    readings = [
        (datetime(D.year, D.month, D.day, 0, 4, tzinfo=UTC), 9.0, 0.0, 3.0),
        (datetime(D.year, D.month, D.day, 6, 0, tzinfo=UTC), 11.0, 0.4, 7.0),
        (datetime(D.year, D.month, D.day, 11, 55, tzinfo=UTC), 14.0, 1.2, 5.0),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return _five_minute_response(readings)

    _run_wind_chunk(db, site_id, handler)

    rows = conn.execute(
        "SELECT obs_at, source, local_date, temp_c, precip_total_mm,"
        " speed_kmh, fetched_at FROM station_wind_records"
        " WHERE station_id = ? ORDER BY obs_at",
        (station_b,),
    ).fetchall()
    assert len(rows) == 3
    for row, (_instant, temp, rain, wind) in zip(rows, readings, strict=True):
        assert row["source"] == "five_minute"
        assert row["local_date"] == D.isoformat()
        assert row["temp_c"] == temp, "wind-only storage leaves temp_c NULL"
        assert row["precip_total_mm"] == rain, (
            "wind-only storage leaves precip_total_mm NULL"
        )
        assert row["speed_kmh"] == wind
        assert row["fetched_at"] == isoformat_utc(NOW)
    close_db()


# =============================================================================
# RS2 -- the same instant again from a later download: M27, M28.
# =============================================================================


def test_rs2_later_download_replaces_only_what_it_carries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan 14.5 RS2: a later download of the same instant replaces the
    value it changed and keeps the one it is missing; ``fetched_at``
    moves; the row is never duplicated."""
    from tests.helpers import NOW, D, day_close_site

    db = _init_tmp_db(tmp_path, monkeypatch)
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn)
    conn.execute("UPDATE stations SET enabled = 0 WHERE pws_station_id = 'ISTATION01'")
    station_b = conn.execute(
        "SELECT id FROM stations WHERE pws_station_id = 'ISTATION02'"
    ).fetchone()["id"]
    conn.commit()

    instant = datetime(D.year, D.month, D.day, 6, 0, tzinfo=UTC)

    _freeze_wind_lane(monkeypatch, NOW)
    _run_wind_chunk(
        db,
        site_id,
        lambda request: _five_minute_response([(instant, 10.0, 1.0, 5.0)]),
    )

    now2 = NOW + timedelta(hours=3, minutes=1)
    _freeze_wind_lane(monkeypatch, now2)

    def second_handler(request: httpx.Request) -> httpx.Response:
        # windspeedAvg absent: this download carries no wind for the instant.
        return httpx.Response(
            200,
            json={
                "observations": [
                    {
                        "valid_time_gmt": int(instant.timestamp()),
                        "obsTimeLocal": instant.strftime("%Y-%m-%d %H:%M:%S"),
                        "metric": {"tempAvg": 12.0, "precipTotal": 1.0},
                    }
                ]
            },
        )

    _run_wind_chunk(db, site_id, second_handler)

    rows = conn.execute(
        "SELECT temp_c, precip_total_mm, speed_kmh, fetched_at"
        " FROM station_wind_records WHERE station_id = ? AND obs_at = ?",
        (station_b, isoformat_utc(instant)),
    ).fetchall()
    assert len(rows) == 1, "the same instant from a later download is one row"
    row = rows[0]
    assert row["temp_c"] == 12.0, (
        "mutant -> at this assertion: correct = 12.0 (the upsert's COALESCE"
        " takes the new download's value when it has one), mutant"
        " (COALESCE(<col>, excluded.<col>) -- the stored value wins even"
        " when the new download changed it) = 10.0"
    )
    assert row["speed_kmh"] == 5.0, (
        "mutant -> at this assertion: correct = 5.0 (the second download's"
        " missing wind value never blanks the stored one), mutant (the"
        " upsert sets speed_kmh = excluded.speed_kmh with no COALESCE) ="
        " None"
    )
    assert row["precip_total_mm"] == 1.0
    assert row["fetched_at"] == isoformat_utc(now2)
    close_db()


# =============================================================================
# RS3 -- one instant from two sources: M29.
# =============================================================================


def test_rs3_one_instant_from_two_sources_gives_two_rows() -> None:
    """Plan 14.5 RS3: a current-conditions reading of the same instant as a
    5-minute reading lands as two rows, one per source, never one
    overwriting the other."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    _site_id, station_id = _make_plain_site(
        conn, pws_id="ISTATION03", today=date(2024, 6, 5)
    )

    instant = datetime(2024, 6, 5, 6, 0, 0, tzinfo=UTC)
    fetched_at = datetime(2024, 6, 5, 6, 5, 0, tzinfo=UTC)

    store_readings(
        conn,
        station_id=station_id,
        source="five_minute",
        readings=[
            StationReading(
                obs_at=instant,
                source="five_minute",
                local_date=date(2024, 6, 5),
                temp_c=10.0,
                precip_total_mm=None,
                speed_kmh=None,
            )
        ],
        fetched_at=fetched_at,
        now_utc=fetched_at,
    )
    store_readings(
        conn,
        station_id=station_id,
        source="current_obs",
        readings=[
            StationReading(
                obs_at=instant,
                source="current_obs",
                local_date=date(2024, 6, 5),
                temp_c=11.0,
                precip_total_mm=None,
                speed_kmh=None,
            )
        ],
        fetched_at=fetched_at,
        now_utc=fetched_at,
    )

    rows = conn.execute(
        "SELECT source, temp_c FROM station_wind_records"
        " WHERE station_id = ? AND obs_at = ? ORDER BY source",
        (station_id, isoformat_utc(instant)),
    ).fetchall()
    assert len(rows) == 2, (
        "mutant -> at this assertion: correct = 2 (the primary key includes"
        " source, so one instant from two sources is two rows), mutant"
        " (the key is (station_id, obs_at) with a conflict rule by source"
        " rank, so the second source's insert conflicts with the first) = 1"
    )
    by_source = {row["source"]: row["temp_c"] for row in rows}
    assert by_source == {"five_minute": 10.0, "current_obs": 11.0}


# =============================================================================
# RS4 -- the guard refuses a closed day's readings: M30.
# =============================================================================


def test_rs4_guard_refuses_a_closed_days_readings(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Plan 14.5 RS4: a reading of a closed day is dropped and counted, one
    log line from P1's guard; a reading of an open day beside it is
    stored."""
    from tests.helpers import NOW, D, day_close_site

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    day_close_site(conn)
    station_b = conn.execute(
        "SELECT id FROM stations WHERE pws_station_id = 'ISTATION02'"
    ).fetchone()["id"]

    d_minus_3 = D - timedelta(days=3)
    closed_instant = datetime(
        d_minus_3.year, d_minus_3.month, d_minus_3.day, 10, 0, tzinfo=UTC
    )
    open_instant = datetime(D.year, D.month, D.day, 10, 0, tzinfo=UTC)

    with caplog.at_level(logging.DEBUG, logger="wxverify.db.site_days"):
        result = store_readings(
            conn,
            station_id=station_b,
            source="five_minute",
            readings=[
                StationReading(
                    obs_at=closed_instant,
                    source="five_minute",
                    local_date=d_minus_3,
                    temp_c=9.0,
                    precip_total_mm=None,
                    speed_kmh=None,
                ),
                StationReading(
                    obs_at=open_instant,
                    source="five_minute",
                    local_date=D,
                    temp_c=13.0,
                    precip_total_mm=None,
                    speed_kmh=None,
                ),
            ],
            fetched_at=NOW,
            now_utc=NOW,
        )
    assert isinstance(result, StoreResult)

    stored = {
        row["obs_at"]
        for row in conn.execute(
            "SELECT obs_at FROM station_wind_records WHERE station_id = ?",
            (station_b,),
        )
    }
    assert stored == {isoformat_utc(open_instant)}, (
        "mutant -> at this assertion: correct = {the open instant only}"
        " (P1's guard refuses the closed day's reading), mutant"
        " (store_readings skips P1's guard) = both instants stored"
    )
    assert result.refused == 1
    assert result.kept == 1

    declined = [r for r in caplog.records if "declined" in r.getMessage()]
    assert len(declined) == 1, "one log line from P1's guard, not one per row"
    assert "kind=reading" in declined[0].getMessage()
    assert "count=1" in declined[0].getMessage()


# =============================================================================
# RS5 -- an offline poll stores no raw row; a repeat is still one row: M31.
# =============================================================================


def test_rs5_poll_dedup_and_offline_poll_stores_no_raw_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plan 14.5 RS5: an online poll stores one ``current_obs`` row; an
    offline poll (204) stores none; a poll repeating the last reading is
    still one row."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id, station_id = _make_plain_site(
        conn, pws_id="ISTATION04", today=date(2024, 6, 5)
    )

    now = datetime(2024, 6, 5, 6, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.current_obs.utc_now", lambda: now)
    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: now)

    def _raw_current_obs_count() -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM station_wind_records"
            " WHERE station_id = ? AND source = 'current_obs'",
            (station_id,),
        ).fetchone()
        return int(row["n"])

    def _reading_response() -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "observations": [
                    {
                        "obsTimeUtc": "2024-06-05T05:55:00Z",
                        "metric": {
                            "temp": 10.0,
                            "precipTotal": 1.0,
                            "windSpeed": 5.0,
                        },
                    }
                ]
            },
        )

    online = classify_current_obs(_reading_response())
    assert online.health is Health.ONLINE
    _persist_poll_result_clearing(conn, site_id, station_id, online, 200)
    assert _raw_current_obs_count() == 1

    offline = classify_current_obs(httpx.Response(204, content=b""))
    assert offline.health is Health.OFFLINE
    _persist_poll_result_clearing(conn, site_id, station_id, offline, 204)
    assert _raw_current_obs_count() == 1, (
        "mutant -> at this assertion: correct = 1 (an offline poll stores"
        " no raw row), mutant (an offline poll stores a row) = 2"
    )

    repeat = classify_current_obs(_reading_response())
    assert repeat.health is Health.ONLINE
    _persist_poll_result_clearing(conn, site_id, station_id, repeat, 200)
    assert _raw_current_obs_count() == 1, (
        "a poll repeating the last obsTimeUtc is the same reading, never a second row"
    )

    # Plan 14.5 RS5's fourth case: a station disabled before the poll is
    # saved. Paired with the same save after re-enabling the station, so the
    # suppression cannot pass vacuously (the positive would fail if the
    # disabled-station check misfired and always suppressed the row).
    def _online_at(instant: str) -> httpx.Response:
        return classify_current_obs(
            httpx.Response(
                200,
                json={
                    "observations": [
                        {
                            "obsTimeUtc": instant,
                            "metric": {
                                "temp": 10.0,
                                "precipTotal": 1.0,
                                "windSpeed": 5.0,
                            },
                        }
                    ]
                },
            )
        )

    new_instant = "2024-06-05T06:10:00Z"
    conn.execute("UPDATE stations SET enabled = 0 WHERE id = ?", (station_id,))
    disabled_online = _online_at(new_instant)
    assert disabled_online.health is Health.ONLINE
    _persist_poll_result_clearing(conn, site_id, station_id, disabled_online, 200)
    assert _raw_current_obs_count() == 1, (
        "mutant -> at this assertion: correct = 1 (the poll's save skips a"
        " disabled, deleted or moved station, worker/current_obs.py:200-206,"
        " and its raw row follows), mutant (the raw row is stored even when"
        " the poll's save skipped the station) = 2"
    )

    conn.execute("UPDATE stations SET enabled = 1 WHERE id = ?", (station_id,))
    _persist_poll_result_clearing(conn, site_id, station_id, disabled_online, 200)
    assert _raw_current_obs_count() == 2, (
        "the same new-instant poll with the station enabled adds a row --"
        " the suppression above is not vacuous"
    )


# =============================================================================
# RS7 -- the poll's values pass 5.2's per-value check before the insert: M80.
# =============================================================================


def test_rs7_poll_values_pass_the_per_value_check_before_the_raw_insert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plan 14.5 RS7: every poll's own write (poll state, the
    ``station_current_obs`` snapshot, ``next_poll_at``, the hold clear)
    lands regardless of the raw store; an all-null reply stores no raw row;
    an invalid-value reply stores one row with only the valid value."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id, station_id = _make_plain_site(
        conn, pws_id="ISTATION06", today=date(2024, 6, 5)
    )

    now = datetime(2024, 6, 5, 6, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.current_obs.utc_now", lambda: now)
    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: now)

    record_access_refused(conn, "current_obs", 401, reason=None)
    conn.commit()

    all_null = classify_current_obs(
        httpx.Response(
            200,
            json={
                "observations": [{"obsTimeUtc": "2024-06-05T05:55:00Z", "metric": {}}]
            },
        )
    )
    assert all_null.health is Health.ONLINE
    _persist_poll_result_clearing(conn, site_id, station_id, all_null, 200)

    poll_row = conn.execute(
        "SELECT next_poll_at FROM station_poll_state WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    assert poll_row is not None and poll_row["next_poll_at"] is not None
    assert read_access_record(conn, "current_obs") is None, (
        "a granted current_obs poll clears the current_obs hold whether or"
        " not it has a value to store"
    )
    snapshot = conn.execute(
        "SELECT 1 FROM station_current_obs WHERE station_id = ?", (station_id,)
    ).fetchone()
    assert snapshot is not None

    first_raw = conn.execute(
        "SELECT COUNT(*) AS n FROM station_wind_records"
        " WHERE station_id = ? AND source = 'current_obs'",
        (station_id,),
    ).fetchone()
    assert first_raw["n"] == 0, "an all-null reading stores no raw row"

    second_instant = datetime(2024, 6, 5, 6, 5, 0, tzinfo=UTC)
    second = classify_current_obs(
        httpx.Response(
            200,
            json={
                "observations": [
                    {
                        "obsTimeUtc": isoformat_utc(second_instant),
                        "metric": {
                            "temp": 12.0,
                            "precipTotal": -1.0,
                            "windSpeed": "nan",
                        },
                    }
                ]
            },
        )
    )
    assert second.health is Health.ONLINE

    raised: str | None = None
    try:
        _persist_poll_result_clearing(conn, site_id, station_id, second, 200)
    except sqlite3.IntegrityError as exc:
        raised = str(exc)
    assert raised is None, (
        "mutant -> at this assertion: correct = None (5.2's per-value check"
        " drops the invalid precip_total_mm (-1.0) and speed_kmh (nan)"
        " before the insert, so the row's CHECK never sees them), mutant"
        " (the poll's storage skips the per-value check, so the invalid"
        " values reach the insert and its CHECK rolls the write back) = " + repr(raised)
    )

    row = conn.execute(
        "SELECT temp_c, precip_total_mm, speed_kmh FROM station_wind_records"
        " WHERE station_id = ? AND source = 'current_obs' AND obs_at = ?",
        (station_id, isoformat_utc(second_instant)),
    ).fetchone()
    assert row is not None
    assert row["temp_c"] == 12.0
    assert row["precip_total_mm"] is None
    assert row["speed_kmh"] is None

    third_instant = datetime(2024, 6, 5, 6, 10, 0, tzinfo=UTC)
    third = classify_current_obs(
        httpx.Response(
            200,
            json={
                "observations": [
                    {
                        "obsTimeUtc": isoformat_utc(third_instant),
                        "metric": {
                            "temp": 12.5,
                            "windSpeed": True,
                        },
                    }
                ]
            },
        )
    )
    assert third.health is Health.ONLINE

    raised_third: str | None = None
    try:
        _persist_poll_result_clearing(conn, site_id, station_id, third, 200)
    except sqlite3.IntegrityError as exc:
        raised_third = str(exc)
    assert raised_third is None

    third_poll_row = conn.execute(
        "SELECT next_poll_at FROM station_poll_state WHERE station_id = ?",
        (station_id,),
    ).fetchone()
    assert third_poll_row is not None and third_poll_row["next_poll_at"] is not None
    third_snapshot = conn.execute(
        "SELECT 1 FROM station_current_obs WHERE station_id = ?", (station_id,)
    ).fetchone()
    assert third_snapshot is not None

    third_row = conn.execute(
        "SELECT temp_c, speed_kmh FROM station_wind_records"
        " WHERE station_id = ? AND source = 'current_obs' AND obs_at = ?",
        (station_id, isoformat_utc(third_instant)),
    ).fetchone()
    assert third_row is not None
    assert third_row["temp_c"] == 12.5
    assert third_row["speed_kmh"] is None, (
        "mutant -> at this assertion: correct = None (5.2's per-value check"
        " rejects a bool as a wind value, the way it rejects one for"
        " temperature and rain, so a JSON true windSpeed is stored NULL),"
        " mutant (the raw values are taken from _number's output, which"
        " accepts a bool as an int, turning True into 1.0) = 1.0"
    )


# =============================================================================
# RS8 -- today's row at a DST-shifted day boundary, where the old 24-hour
# _STORE_MARGIN and the new yesterday-start/today-end bounds (plan 4.1 item
# 1) disagree, and the future cap (plan 4.1) still applies to today's row.
# =============================================================================


_RS8_CASES = [
    pytest.param(
        date(2024, 11, 4),
        timedelta(minutes=30),
        timedelta(hours=25),
        timedelta(hours=24, minutes=30),
        True,
        id="autumn-25h-yesterday",
    ),
    pytest.param(
        date(2024, 3, 11),
        timedelta(minutes=-30),
        timedelta(hours=23),
        timedelta(hours=23, minutes=30),
        False,
        id="spring-23h-yesterday",
    ),
]


@pytest.mark.parametrize(
    (
        "local_today",
        "earlier_offset",
        "yesterday_span",
        "earlier_before_today_start",
        "earlier_stored",
    ),
    _RS8_CASES,
)
def test_rs8_todays_row_keeps_only_yesterday_and_todays_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    local_today: date,
    earlier_offset: timedelta,
    yesterday_span: timedelta,
    earlier_before_today_start: timedelta,
    earlier_stored: bool,
) -> None:
    """Plan 14.5 RS8: today's row in an ``America/New_York`` site on the
    day after a clock change, so yesterday is not 24 hours long (plan 5.8).

    Autumn (today 2024-11-04, yesterday 25 h): a reading 30 minutes after
    yesterday's start -- 24.5 h before today's start -- lies inside
    yesterday's bounds and is stored. Spring (today 2024-03-11, yesterday
    23 h): a reading 30 minutes before yesterday's start -- 23.5 h before
    today's start -- lies in the day before yesterday and is not stored.

    In both cases, one reading inside today's bounds before now is stored,
    and one inside today's bounds 30 minutes after now is not: the future
    cap at now plus ``_FUTURE_SLACK`` stays on today's row even though the
    24-hour margin left on a past row's window no longer applies to it.
    """
    from tests.helpers import day_close_site

    timezone = "America/New_York"
    db = _init_tmp_db(tmp_path, monkeypatch)
    conn = db._conn  # noqa: SLF001 -- real writer connection, as other tests do
    site_id = day_close_site(conn, timezone=timezone, with_records=False)
    conn.execute("UPDATE stations SET enabled = 0 WHERE pws_station_id = 'ISTATION01'")
    station_b = conn.execute(
        "SELECT id FROM stations WHERE pws_station_id = 'ISTATION02'"
    ).fetchone()["id"]
    conn.commit()

    now = datetime(
        local_today.year,
        local_today.month,
        local_today.day,
        12,
        0,
        tzinfo=ZoneInfo(timezone),
    ).astimezone(UTC)
    # The lane needs P1 records (the daily close is always on): start one
    # two local days before `local_today` (so the earlier reading's own
    # bounds check has a real, open record to land in or outside of,
    # exactly as this test's subject needs) and fill forward to today's.
    day_before_yesterday_local = local_today - timedelta(days=2)
    start_site_days(
        conn,
        site_id,
        day_before_yesterday_local,
        now_utc=datetime(
            day_before_yesterday_local.year,
            day_before_yesterday_local.month,
            day_before_yesterday_local.day,
            1,
            tzinfo=ZoneInfo(timezone),
        ).astimezone(UTC),
    )
    ensure_site_days(conn, site_id, now_utc=now)
    _freeze_wind_lane(monkeypatch, now)

    today_start, today_end, _ = local_day_slots(local_today, timezone)
    yesterday_start, _yesterday_end, _ = local_day_slots(
        local_today - timedelta(days=1), timezone
    )

    assert today_start - yesterday_start == yesterday_span, (
        f"fixture bug: yesterday must span {yesterday_span}, not"
        f" {today_start - yesterday_start}"
    )
    earlier_reading = yesterday_start + earlier_offset
    assert today_start - earlier_reading == earlier_before_today_start, (
        f"fixture bug: the earlier reading must sit {earlier_before_today_start}"
        f" before today's start, not {today_start - earlier_reading}"
    )

    in_today_before_now = today_start + timedelta(hours=1)
    in_today_after_now = now + timedelta(minutes=30)
    assert in_today_before_now < now < today_end
    assert now < in_today_after_now < today_end

    readings = [
        (earlier_reading, 9.0, 0.0, 3.0),
        (in_today_before_now, 11.0, 0.4, 7.0),
        (in_today_after_now, 14.0, 1.2, 5.0),
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return _five_minute_response(readings)

    assert (
        check_day_write(
            conn,
            site_id,
            isoformat_utc(earlier_reading),
            WriteKind.READING,
            now_utc=now,
        )
        is True
    )

    _run_wind_chunk(db, site_id, handler)

    stored = {
        row["obs_at"]
        for row in conn.execute(
            "SELECT obs_at FROM station_wind_records"
            " WHERE station_id = ? AND source = 'five_minute'",
            (station_b,),
        )
    }
    expected = {isoformat_utc(in_today_before_now)}
    if earlier_stored:
        expected.add(isoformat_utc(earlier_reading))
    # A 24-hour margin instead of yesterday's real start flips whether the
    # earlier reading falls inside the window: it drops the stored autumn
    # reading and keeps the spring reading that must be dropped.
    margin_mutant = expected ^ {isoformat_utc(earlier_reading)}
    # Dropping the future cap lets the now+30-minute reading through, since
    # it is still inside today's own bounds.
    cap_mutant = expected | {isoformat_utc(in_today_after_now)}

    assert stored == expected, (
        "24-hour margin left on today's row -> at this assertion: correct = "
        + repr(sorted(expected))
        + ", mutant (today's window uses the 24-hour margin, not yesterday's"
        " real start) = "
        + repr(sorted(margin_mutant))
        + "\nfuture cap dropped from today's row -> at this assertion:"
        " correct = "
        + repr(sorted(expected))
        + ", mutant (no now + _FUTURE_SLACK cap on today's row) = "
        + repr(sorted(cap_mutant))
        + "\nactual stored = "
        + repr(sorted(stored))
    )
    close_db()


# =============================================================================
# BT5 -- the 5-minute download ends a current-conditions poll pause (plan
# 2026-11-07-health-release.md §2, Part B4).
# =============================================================================


def _seed_poll_pause(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    count: int,
    reason: str | None,
    next_poll_at: str | None,
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state"
        " (station_id, health_state, next_poll_at, pause_reason,"
        "  pause_evidence_count)"
        " VALUES (?, 'offline', ?, ?, ?)",
        (station_id, next_poll_at, reason, count),
    )
    conn.commit()


def _poll_pause_row(conn: sqlite3.Connection, station_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT pause_evidence_count, pause_reason, next_poll_at"
        " FROM station_poll_state WHERE station_id = ?",
        (station_id,),
    ).fetchone()


def _clear_poll_pause_and_raw_rows(conn: sqlite3.Connection, station_id: int) -> None:
    conn.execute("DELETE FROM station_poll_state WHERE station_id = ?", (station_id,))
    conn.execute("DELETE FROM station_wind_records WHERE station_id = ?", (station_id,))
    conn.commit()


def test_bt5_fresh_five_minute_reading_ends_the_pause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan §2 BT5 (first case): station B seeded at (3, 'not_found'),
    ``next_poll_at`` 8 h ahead. A reply with a reading 10 min old ends the
    pause: count 0, reason NULL, ``next_poll_at`` equal to the frozen now.
    Catches: the call not wired into ``_persist_day`` -- a unit-only test
    of ``end_poll_pause_on_fresh_reading`` would pass without it."""
    from tests.helpers import NOW, day_close_site

    db = _init_tmp_db(tmp_path, monkeypatch)
    conn = db._conn  # noqa: SLF001 -- real writer connection, as other tests do
    site_id = day_close_site(conn)
    conn.execute("UPDATE stations SET enabled = 0 WHERE pws_station_id = 'ISTATION01'")
    station_b = conn.execute(
        "SELECT id FROM stations WHERE pws_station_id = 'ISTATION02'"
    ).fetchone()["id"]
    far_future = isoformat_utc(NOW + timedelta(hours=8))
    _seed_poll_pause(
        conn, station_b, count=3, reason="not_found", next_poll_at=far_future
    )
    _freeze_wind_lane(monkeypatch, NOW)

    fresh_reading = [(NOW - timedelta(minutes=10), 9.0, 0.0, 3.0)]

    def handler(request: httpx.Request) -> httpx.Response:
        return _five_minute_response(fresh_reading)

    _run_wind_chunk(db, site_id, handler)

    row = _poll_pause_row(conn, station_b)
    assert row is not None
    assert (row["pause_evidence_count"], row["pause_reason"]) == (0, None)
    assert row["next_poll_at"] == isoformat_utc(NOW)
    close_db()


def test_bt5_stale_five_minute_reading_does_not_end_the_pause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan §2 BT5 (second case): a reply whose readings are all more than
    60 minutes old leaves a paused station's poll-state row unchanged."""
    from tests.helpers import NOW, day_close_site

    db = _init_tmp_db(tmp_path, monkeypatch)
    conn = db._conn  # noqa: SLF001 -- real writer connection, as other tests do
    site_id = day_close_site(conn)
    conn.execute("UPDATE stations SET enabled = 0 WHERE pws_station_id = 'ISTATION01'")
    station_b = conn.execute(
        "SELECT id FROM stations WHERE pws_station_id = 'ISTATION02'"
    ).fetchone()["id"]
    far_future = isoformat_utc(NOW + timedelta(hours=8))
    _seed_poll_pause(
        conn, station_b, count=3, reason="not_found", next_poll_at=far_future
    )
    _freeze_wind_lane(monkeypatch, NOW)

    stale_reading = [(NOW - timedelta(minutes=90), 9.0, 0.0, 3.0)]

    def handler(request: httpx.Request) -> httpx.Response:
        return _five_minute_response(stale_reading)

    _run_wind_chunk(db, site_id, handler)

    row = _poll_pause_row(conn, station_b)
    assert row is not None
    assert (row["pause_evidence_count"], row["pause_reason"]) == (3, "not_found")
    assert row["next_poll_at"] == far_future
    close_db()


def test_bt5_end_poll_pause_on_fresh_reading_direct_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Direct calls of ``end_poll_pause_on_fresh_reading``: an ``hourly``
    or ``current_obs`` row 10 min old ends nothing; at count 0, a
    ``five_minute`` row 10 min old with ``next_poll_at`` 10 min ahead
    returns ``False`` and writes nothing; at count 2, a ``five_minute``
    row exactly 60 min old ends the pause, as does one exactly 60 min
    ahead (a separate case), while one 60 min 1 s old or ahead ends
    nothing. Catches: the hourly source counted (the TODO keeps scoring
    input out); a healthy station's poll pulled forward (the count guard
    dropped, reached only with a fresh row present); the 1 s left off the
    excluded end (the row exactly 60 min ahead missed)."""
    from tests.helpers import NOW, day_close_site

    db = _init_tmp_db(tmp_path, monkeypatch)
    conn = db._conn  # noqa: SLF001 -- real writer connection, as other tests do
    day_close_site(conn)
    conn.execute("UPDATE stations SET enabled = 0 WHERE pws_station_id = 'ISTATION01'")
    station_b = conn.execute(
        "SELECT id FROM stations WHERE pws_station_id = 'ISTATION02'"
    ).fetchone()["id"]
    conn.commit()
    far_future = isoformat_utc(NOW + timedelta(hours=8))

    def _insert_raw(obs_at: datetime, source: str) -> None:
        conn.execute(
            "INSERT INTO station_wind_records"
            " (station_id, obs_at, source, local_date, speed_kmh)"
            " VALUES (?, ?, ?, ?, 5.0)",
            (station_b, isoformat_utc(obs_at), source, obs_at.date().isoformat()),
        )
        conn.commit()

    # hourly, 10 min old, count 2: ends nothing.
    _seed_poll_pause(
        conn, station_b, count=2, reason="offline", next_poll_at=far_future
    )
    _insert_raw(NOW - timedelta(minutes=10), "hourly")
    assert end_poll_pause_on_fresh_reading(conn, station_b, now_utc=NOW) is False
    row = _poll_pause_row(conn, station_b)
    assert row is not None
    assert (row["pause_evidence_count"], row["pause_reason"]) == (2, "offline")
    _clear_poll_pause_and_raw_rows(conn, station_b)

    # current_obs, 10 min old, count 2: ends nothing.
    _seed_poll_pause(
        conn, station_b, count=2, reason="offline", next_poll_at=far_future
    )
    _insert_raw(NOW - timedelta(minutes=10), "current_obs")
    assert end_poll_pause_on_fresh_reading(conn, station_b, now_utc=NOW) is False
    row = _poll_pause_row(conn, station_b)
    assert row is not None
    assert (row["pause_evidence_count"], row["pause_reason"]) == (2, "offline")
    _clear_poll_pause_and_raw_rows(conn, station_b)

    # count 0, five_minute 10 min old: returns False, writes nothing.
    near_future = isoformat_utc(NOW + timedelta(minutes=10))
    _seed_poll_pause(conn, station_b, count=0, reason=None, next_poll_at=near_future)
    _insert_raw(NOW - timedelta(minutes=10), "five_minute")
    assert end_poll_pause_on_fresh_reading(conn, station_b, now_utc=NOW) is False
    row = _poll_pause_row(conn, station_b)
    assert row is not None
    assert row["next_poll_at"] == near_future, (
        "a healthy station's own poll must not be pulled forward"
    )
    _clear_poll_pause_and_raw_rows(conn, station_b)

    # count 2, five_minute EXACTLY 60 min old: ends the pause.
    _seed_poll_pause(
        conn, station_b, count=2, reason="offline", next_poll_at=far_future
    )
    _insert_raw(NOW - timedelta(minutes=60), "five_minute")
    assert end_poll_pause_on_fresh_reading(conn, station_b, now_utc=NOW) is True
    row = _poll_pause_row(conn, station_b)
    assert row is not None
    assert (row["pause_evidence_count"], row["pause_reason"]) == (0, None)
    assert row["next_poll_at"] == isoformat_utc(NOW)
    _clear_poll_pause_and_raw_rows(conn, station_b)

    # count 2, five_minute EXACTLY 60 min ahead: ends the pause too.
    _seed_poll_pause(
        conn, station_b, count=2, reason="offline", next_poll_at=far_future
    )
    _insert_raw(NOW + timedelta(minutes=60), "five_minute")
    assert end_poll_pause_on_fresh_reading(conn, station_b, now_utc=NOW) is True
    row = _poll_pause_row(conn, station_b)
    assert row is not None
    assert (row["pause_evidence_count"], row["pause_reason"]) == (0, None)
    _clear_poll_pause_and_raw_rows(conn, station_b)

    # count 2, five_minute 60 min 1 s OLD: ends nothing.
    _seed_poll_pause(
        conn, station_b, count=2, reason="offline", next_poll_at=far_future
    )
    _insert_raw(NOW - timedelta(minutes=60, seconds=1), "five_minute")
    assert end_poll_pause_on_fresh_reading(conn, station_b, now_utc=NOW) is False
    row = _poll_pause_row(conn, station_b)
    assert row is not None
    assert (row["pause_evidence_count"], row["pause_reason"]) == (2, "offline")
    _clear_poll_pause_and_raw_rows(conn, station_b)

    # count 2, five_minute 60 min 1 s AHEAD: ends nothing.
    _seed_poll_pause(
        conn, station_b, count=2, reason="offline", next_poll_at=far_future
    )
    _insert_raw(NOW + timedelta(minutes=60, seconds=1), "five_minute")
    assert end_poll_pause_on_fresh_reading(conn, station_b, now_utc=NOW) is False
    row = _poll_pause_row(conn, station_b)
    assert row is not None
    assert (row["pause_evidence_count"], row["pause_reason"]) == (2, "offline")

    close_db()
