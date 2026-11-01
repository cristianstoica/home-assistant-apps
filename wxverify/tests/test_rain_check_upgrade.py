"""Rain release Part A outside the close: the v16 upgrade and the Sites page.

Plan 2026-11-01-rain-release.md, Part A, "Schema (v16)" / "Upgrade
carry-over" and A5. ``tests/test_rain_check_rules.py`` and
``tests/test_rain_check_close.py`` cover the rules and the close; this file
pins what those leave out:

- U1: a made-up v15 file (no ``station_rain_checks``, no
  ``site_day_stations.rain_counted``) upgrades with every existing list row
  counted, and a second run changes nothing.
- U2: the Sites page Status cell adds `` · rain not counted since <date>``
  only while a station is set aside, the date being the first day left out.

Synthetic data only: stations ``RAINUP-A`` to ``RAINUP-C``, dates from
2024-06-10.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import wxverify.db.migrations as migrations
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, init_db
from wxverify.db.site_days import day_rain_station_ids

_DAY = date(2024, 6, 10)


def _insert_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 40.0, -105.0, 900.0, 'UTC')",
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_station(conn: sqlite3.Connection, site_id: int, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)"
        " VALUES (?, ?, 40.0, -105.0, 900.0)",
        (site_id, name),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _list_rows(conn: sqlite3.Connection) -> list[tuple[str, int, int]]:
    return [
        (str(row[0]), int(row[1]), int(row[2]))
        for row in conn.execute(
            "SELECT local_date, station_id, rain_counted FROM site_day_stations"
            " ORDER BY local_date, station_id"
        )
    ]


def test_u1_v15_file_upgrades_with_every_list_row_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    # The made-up v15 file: the first run stops at 15, then v16's objects go.
    with monkeypatch.context() as v15:
        v15.setattr(migrations, "TARGET_USER_VERSION", 15)
        migrations.run_migrations(conn)
    conn.execute("DROP TABLE station_rain_checks")
    conn.execute("ALTER TABLE site_day_stations DROP COLUMN rain_counted")
    conn.execute("DROP TABLE forecast_scored")
    conn.execute("DROP INDEX idx_samples_slot")
    conn.execute("PRAGMA user_version = 15")
    site_id = _insert_site(conn, "rain-upgrade-site")
    station_ids = [
        _insert_station(conn, site_id, f"RAINUP-{letter}") for letter in "AB"
    ]
    stamp = isoformat_utc(datetime(_DAY.year, _DAY.month, _DAY.day, tzinfo=UTC))
    for offset in range(2):
        day = _DAY + timedelta(days=offset)
        start = datetime(day.year, day.month, day.day, tzinfo=UTC)
        conn.execute(
            "INSERT INTO site_days (site_id, local_date, state, day_start_utc,"
            " day_end_utc, timezone, created_at, state_changed_at)"
            " VALUES (?, ?, 'open', ?, ?, 'UTC', ?, ?)",
            (
                site_id,
                day.isoformat(),
                isoformat_utc(start),
                isoformat_utc(start + timedelta(days=1)),
                stamp,
                stamp,
            ),
        )
        for station_id in station_ids:
            conn.execute(
                "INSERT INTO site_day_stations (site_id, local_date, station_id)"
                " VALUES (?, ?, ?)",
                (site_id, day.isoformat(), station_id),
            )
    conn.commit()
    assert "rain_counted" not in {
        str(row["name"]) for row in conn.execute("PRAGMA table_info(site_day_stations)")
    }, "fixture sanity: the v15 list has no rain flag"

    migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 16
    first = _list_rows(conn)
    assert first == [
        (day.isoformat(), station_id, 1)
        for day in (_DAY, _DAY + timedelta(days=1))
        for station_id in station_ids
    ]
    assert day_rain_station_ids(conn, site_id, _DAY) == frozenset(station_ids)
    assert conn.execute("SELECT COUNT(*) FROM station_rain_checks").fetchone()[0] == 0

    migrations.run_migrations(conn)  # a second run changes nothing

    assert _list_rows(conn) == first
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 16


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _insert_check(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    day: date,
    outcome: str,
    decision: str | None,
    counted_after: int,
) -> None:
    conn.execute(
        "INSERT INTO station_rain_checks (site_id, local_date, station_id, outcome,"
        " reason, station_mm, peer_median_mm, peers, decision, counted_after,"
        " checked_at) VALUES (?, ?, ?, ?, NULL, 0.0, 3.0, 5, ?, ?, ?)",
        (
            site_id,
            day.isoformat(),
            station_id,
            outcome,
            decision,
            counted_after,
            isoformat_utc(datetime(day.year, day.month, day.day, tzinfo=UTC)),
        ),
    )


def _row_for_station(html: str, pws_station_id: str) -> str:
    start = html.index(f"<td>{pws_station_id}</td>")
    end = html.index("</tr>", start)
    return html[start:end]


def test_u2_sites_page_shows_rain_not_counted_while_set_aside(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn, "rain-upgrade-site")
    aside = _insert_station(conn, site_id, "RAINUP-A")
    back = _insert_station(conn, site_id, "RAINUP-B")
    counted = _insert_station(conn, site_id, "RAINUP-C")
    day1, day2, day3 = _DAY, _DAY + timedelta(days=1), _DAY + timedelta(days=2)
    # A: set aside on day2, still out on day3 -> not counted since day3.
    _insert_check(conn, site_id, aside, day1, "zero", None, 1)
    _insert_check(conn, site_id, aside, day2, "zero", "set_aside", 0)
    _insert_check(conn, site_id, aside, day3, "agree", None, 0)
    # B: set aside on day1, taken back on day3 -> counted again.
    _insert_check(conn, site_id, back, day1, "zero", "set_aside", 0)
    _insert_check(conn, site_id, back, day3, "agree", "taken_back", 1)
    _insert_check(conn, site_id, counted, day3, "agree", None, 1)
    conn.commit()

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    with TestClient(create_app(root_path="")) as client:
        response = client.get("/sites")
    assert response.status_code == 200
    html = response.text

    expected = f" · rain not counted since {(day2 + timedelta(days=1)).isoformat()}"
    assert expected in _row_for_station(html, "RAINUP-A")
    assert "rain not counted" not in _row_for_station(html, "RAINUP-B")
    assert "rain not counted" not in _row_for_station(html, "RAINUP-C")
