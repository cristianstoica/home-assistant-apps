"""The rain check outside the close: the Sites page.

``tests/test_rain_check_rules.py`` and ``tests/test_rain_check_close.py``
cover the rules and the close; this file pins what those leave out:

- U2: the Sites page Status cell adds `` · rain not counted since <date>``
  only while a station is set aside, the date being the first day left out.

A made-up v15 file upgrading to v16 with every existing list row counted
has no test here: the dedicated v15->v16 migration function it
pinned is deleted along with every other ``migrate_v2`` ... ``migrate_v16``
function -- ``station_rain_checks`` is now just
part of the permanent schema (``create_tables``), with no conversion step
left to test, and a file at user_version 1-15 is refused outright rather
than upgraded.

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

from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, init_db

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
