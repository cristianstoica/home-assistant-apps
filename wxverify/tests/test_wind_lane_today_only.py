"""The wind lane after the switch (daily-close switch-over plan §7.2 row R4,
R5; test IDs WL1-WL2 of §17.8).

Written before R4/R5 land: today's base still selects and fetches past
``station_wind_days`` rows (``WIND_DUE_SQL``'s past-row clause, M50) and
still derives hourly station rows from every fetched day (``_persist_day``'s
unconditional ``_derive_days`` call, M51). Both tests drive the real
``run_wind_days`` job end to end (plan §17.1: "a fake adapter") and fail
today on a plain assertion against that unremoved behaviour -- never at
import or on a fixture error.

Synthetic data only: station id ``ISTATION01``, zone UTC, dates built off a
fixed synthetic ``_TODAY``.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import httpx
import pytest

from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.migrations import seed_default_sources
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.site_days import DAILY_CLOSE_ON_KEY, start_site_days
from wxverify.obs.pws_adapter import ALL_1DAY_PATH
from wxverify.worker.wind_days import ensure_wind_days, run_wind_days

_TZ = "UTC"
_TODAY: date = date(2026, 3, 10)
_API_KEY = "synthetic-weathercom-key"  # noqa: S105

# R6's retired dated endpoint. The package no longer names it (RE5), so the
# test keeps its own copy of the path.
_HISTORY_ALL_PATH = "/v2/pws/history/all"


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001
    seed_default_sources(conn)
    conn.commit()
    return conn


def _make_site(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('wl-site', 0.0, 0.0, 0.0, ?)",
        (_TZ,),
    )
    assert cur.lastrowid is not None
    conn.commit()
    return int(cur.lastrowid)


def _make_station(conn: sqlite3.Connection, site_id: int, pws_id: str) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
        " dem_elevation_m, enabled) VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
        (site_id, pws_id),
    )
    assert cur.lastrowid is not None
    conn.commit()
    return int(cur.lastrowid)


def _insert_day(
    conn: sqlite3.Connection,
    station_id: int,
    local_date: date,
    *,
    status: str,
    refetch_at: str | None = None,
    refetched: int = 0,
) -> None:
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count,"
        "  pair_hours, refetch_at, refetched, updated_at)"
        " VALUES (?, ?, ?, 0, 0, 0, ?, ?, '2026-01-01T00:00:00Z')",
        (station_id, local_date.isoformat(), status, refetch_at, refetched),
    )
    conn.commit()


def _day_row(conn: sqlite3.Connection, station_id: int, local_date: date) -> tuple:
    row = conn.execute(
        "SELECT status, attempts, record_count, pair_hours FROM station_wind_days"
        " WHERE station_id = ? AND local_date = ?",
        (station_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    return tuple(row)


def _freeze(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.domain_backoff.utc_now", lambda: when)


def _run_lane(
    site_id: int, handler: object, *, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)
    real_client = httpx.AsyncClient
    db = get_db()
    writer = FencedWriter(db, db.generation)
    with patch(
        "wxverify.worker.wind_days.httpx.AsyncClient",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),  # type: ignore[arg-type]
    ):
        asyncio.run(run_wind_days(db, writer, site_id, client=None))


def _setup(tmp_path: Path) -> tuple[sqlite3.Connection, int, int]:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "ISTATION01")
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(datetime.now(tz=UTC)))
    # The closed-day guard's first record must be TODAY, not tomorrow (its
    # own default when none exists, db/site_days.py's `_ensure`) -- a
    # test-controlled precondition, not an ambient one, so today's hour is
    # writable the same way it would be once the real cutover has run.
    start_site_days(conn, site_id, _TODAY, now_utc=datetime(2026, 1, 1, tzinfo=UTC))
    conn.commit()
    # A seed observation well before the three station_wind_days rows below,
    # so ensure_wind_days (called directly, WL1) has a real range to fill if
    # it is not restricted to today only.
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
        " VALUES (?, 'temperature', ?, 1.0, 1)",
        (site_id, isoformat_utc(datetime(2026, 2, 28, tzinfo=UTC))),
    )
    conn.commit()
    return conn, site_id, station_id


def test_wl1_only_todays_row_is_fetched_past_rows_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, site_id, station_id = _setup(tmp_path)
    yesterday = _TODAY - timedelta(days=1)
    d_minus_3 = _TODAY - timedelta(days=3)
    _insert_day(
        conn,
        station_id,
        _TODAY,
        status="pending",
    )
    _insert_day(
        conn,
        station_id,
        yesterday,
        status="pending",
        refetch_at=isoformat_utc(datetime(2026, 3, 9, tzinfo=UTC)),
        refetched=0,
    )
    _insert_day(conn, station_id, d_minus_3, status="partial")

    before_yesterday = _day_row(conn, station_id, yesterday)
    before_d3 = _day_row(conn, station_id, d_minus_3)

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(str(request.url))
        return httpx.Response(204, request=request)

    now = datetime(_TODAY.year, _TODAY.month, _TODAY.day, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    _run_lane(site_id, handler, monkeypatch=monkeypatch)

    called_all_1day = any(ALL_1DAY_PATH in url for url in call_log)
    called_history_all = any(_HISTORY_ALL_PATH in url for url in call_log)
    assert called_all_1day, "today's row must be called through fetch_all_1day"
    assert not called_history_all, (
        "R6 regression guard: correct = no call to the dated history/all"
        f" endpoint, found = {call_log!r}. M50 is killed by the row"
        " assertions below, not here"
    )

    after_yesterday = _day_row(conn, station_id, yesterday)
    after_d3 = _day_row(conn, station_id, d_minus_3)
    assert after_yesterday == before_yesterday, (
        "M50 -> at this assertion: correct = yesterday's row unchanged"
        f" ({before_yesterday!r}), mutant = {after_yesterday!r} (a past row"
        " was selected and its try advanced it)"
    )
    assert after_d3 == before_d3, (
        "M50 -> at this assertion: correct = D-3's row unchanged"
        f" ({before_d3!r}), mutant = {after_d3!r}"
    )

    # ensure_wind_days must make no past row either (R4's other half; no
    # numbered mutant of its own, so a plain assertion).
    before_count = conn.execute(
        "SELECT COUNT(*) FROM station_wind_days WHERE station_id = ?",
        (station_id,),
    ).fetchone()[0]
    ensure_wind_days(conn, site_id, ZoneInfo(_TZ), _TODAY)
    after_rows = {
        date.fromisoformat(str(r[0]))
        for r in conn.execute(
            "SELECT local_date FROM station_wind_days WHERE station_id = ?",
            (station_id,),
        ).fetchall()
    }
    assert after_rows == {_TODAY, yesterday, d_minus_3}, (
        "ensure_wind_days must make no past row beyond the three already"
        f" seeded; found {sorted(after_rows)!r}"
    )
    after_count = conn.execute(
        "SELECT COUNT(*) FROM station_wind_days WHERE station_id = ?",
        (station_id,),
    ).fetchone()[0]
    assert after_count == before_count, "no row may be added by ensure_wind_days here"


def test_wl2_no_station_observation_and_no_pair_and_score_from_todays_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, site_id, station_id = _setup(tmp_path)
    _insert_day(conn, station_id, _TODAY, status="pending")

    # Five-minute readings spanning hours 8:00 and 9:00 of today, latest at
    # 10:05, so both hours are "complete" under _derive_hours's H + 1h <=
    # newest rule (worker/wind_days.py:1430) and each adjacent pair is well
    # inside MAX_PAIR_GAP_SECONDS (wind_pairs.py:17) -- the old,
    # un-R5'd _persist_day would derive a pair-max station row for each.
    start = datetime(_TODAY.year, _TODAY.month, _TODAY.day, 8, 0, 0, tzinfo=UTC)
    reading_times = [start + timedelta(minutes=5 * i) for i in range(26)]  # to 10:05
    assert reading_times[-1] == start + timedelta(hours=2, minutes=5)

    def handler(request: httpx.Request) -> httpx.Response:
        payload = {
            "observations": [
                {
                    "valid_time_gmt": int(reading_at.timestamp()),
                    "metric": {
                        "windspeedAvg": 5.0,
                        "tempAvg": 10.0,
                        "precipTotal": 0.0,
                    },
                }
                for reading_at in reading_times
            ]
        }
        return httpx.Response(200, json=payload, request=request)

    now = datetime(_TODAY.year, _TODAY.month, _TODAY.day, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    _run_lane(site_id, handler, monkeypatch=monkeypatch)

    # Control: the readings actually arrived and were stored, so "no station
    # row" below cannot pass merely because nothing was fetched.
    stored_reading_rows = conn.execute(
        "SELECT COUNT(*) FROM station_wind_records WHERE station_id = ?"
        " AND source = 'five_minute' AND speed_kmh IS NOT NULL",
        (station_id,),
    ).fetchone()[0]
    assert stored_reading_rows >= 2, (
        "control: the five-minute readings must be stored before this test"
        f" can mean anything; found {stored_reading_rows} rows"
    )

    station_observation_rows = conn.execute(
        "SELECT COUNT(*) FROM station_observations WHERE station_id = ?",
        (station_id,),
    ).fetchone()[0]
    assert station_observation_rows == 0, (
        "M51 -> at this assertion: correct = 0 (R5 removes _persist_day's"
        " derive call, so no pair-max station row is written for today),"
        f" mutant = {station_observation_rows} (_persist_day still derives"
        " station rows)"
    )

    # With the daily close on, enqueue_scoring_run already queues nothing
    # (P4's own switch, scoring/engine.py; true before and after R5, so this
    # is a regression pin, not an independent M51 divergence point).
    pair_and_score_rows = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type = 'pair_and_score' AND site_id = ?",
        (site_id,),
    ).fetchone()[0]
    assert pair_and_score_rows == 0
