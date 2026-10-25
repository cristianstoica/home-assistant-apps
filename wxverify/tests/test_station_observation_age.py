"""Tests for plan Step 7: the Sites page shows each station's observation age.

Covers the pure boundary function, the loader that builds ``StationView`` via
``_load_stations``'s ``LEFT JOIN`` on ``station_poll_state``, and the rendered
Status cell on the Sites page. Synthetic fixtures only.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import wxverify.web.context as web_context
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, init_db
from wxverify.db.migrations import run_migrations
from wxverify.web.context import observation_age_text

NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)


def test_observation_age_text_boundaries() -> None:
    """Pin every boundary of ``observation_age_text`` against a fixed ``now``.

    Catches: ``>=`` in place of ``>`` at 60 min (the exact-60-min case);
    rounding instead of rounding down (the 23 h 59 min case); minutes shown
    below 1 h (the 30-min case); ``abs(age)`` in place of ``age`` (the
    future-stamp case); no ``ValueError`` catch (the ``"not-a-stamp"`` case).
    """
    assert observation_age_text(None, now=NOW) is None
    assert (
        observation_age_text(isoformat_utc(NOW - timedelta(minutes=30)), now=NOW)
        is None
    )
    assert (
        observation_age_text(isoformat_utc(NOW - timedelta(minutes=60)), now=NOW)
        is None
    )
    assert (
        observation_age_text(isoformat_utc(NOW + timedelta(hours=2)), now=NOW) is None
    )
    assert (
        observation_age_text(
            isoformat_utc(NOW - timedelta(minutes=60, seconds=1)), now=NOW
        )
        == "1 h old"
    )
    assert (
        observation_age_text(isoformat_utc(NOW - timedelta(minutes=61)), now=NOW)
        == "1 h old"
    )
    assert (
        observation_age_text(isoformat_utc(NOW - timedelta(hours=3)), now=NOW)
        == "3 h old"
    )
    assert (
        observation_age_text(
            isoformat_utc(NOW - timedelta(hours=23, minutes=59)), now=NOW
        )
        == "23 h old"
    )
    assert (
        observation_age_text(isoformat_utc(NOW - timedelta(hours=24)), now=NOW)
        == "1 d old"
    )
    assert (
        observation_age_text(isoformat_utc(NOW - timedelta(days=3)), now=NOW)
        == "3 d old"
    )
    assert observation_age_text("not-a-stamp", now=NOW) == "age unknown"
    assert observation_age_text("", now=NOW) == "age unknown"
    assert observation_age_text("9999-12-31T23:59:59-05:00", now=NOW) == "age unknown"


def _insert_station(
    conn: sqlite3.Connection, site_id: int, pws_station_id: str, *, enabled: int
) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO stations (site_id, pws_station_id, lat, lon,
                                   dem_elevation_m, enabled)
            VALUES (?, ?, 40.0, -105.0, 900.0, ?)
            """,
            (site_id, pws_station_id, enabled),
        ).lastrowid
    )


def _insert_poll_state(
    conn: sqlite3.Connection, station_id: int, last_obstime: str | None
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state (station_id, last_obstime) VALUES (?, ?)",
        (station_id, last_obstime),
    )


def test_load_site_observation_age(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pin ``_load_stations``'s LEFT JOIN: order, and age per poll-state shape.

    Catches: an INNER JOIN (station B, with no poll-state row, would vanish
    from the result instead of showing no age); the enabled-first,
    station-id-ordered sort changed.
    """
    monkeypatch.setattr(web_context, "utc_now", lambda: NOW)
    db_path = tmp_path / "wxverify.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                               timezone, enabled)
            VALUES ('Example Site', 40.0, -105.0, 900.0, 'UTC', 1)
            """
        ).lastrowid
    )
    station_a = _insert_station(conn, site_id, "EXAMPLE-A", enabled=1)
    _insert_station(conn, site_id, "EXAMPLE-B", enabled=1)
    station_c = _insert_station(conn, site_id, "EXAMPLE-C", enabled=1)
    station_d = _insert_station(conn, site_id, "EXAMPLE-D", enabled=0)
    _insert_poll_state(conn, station_a, isoformat_utc(NOW - timedelta(hours=3)))
    # EXAMPLE-B gets no poll-state row at all.
    _insert_poll_state(conn, station_c, None)
    _insert_poll_state(conn, station_d, isoformat_utc(NOW - timedelta(days=2)))
    conn.commit()

    site = web_context.load_site(conn, site_id)
    assert site is not None
    ages = [
        (station.pws_station_id, station.observation_age) for station in site.stations
    ]
    assert ages == [
        ("EXAMPLE-A", "3 h old"),
        ("EXAMPLE-B", None),
        ("EXAMPLE-C", None),
        ("EXAMPLE-D", "2 d old"),
    ]
    conn.close()


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


def _make_app(monkeypatch: pytest.MonkeyPatch) -> object:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _make_site(conn: sqlite3.Connection, name: str, *, enabled: int = 1) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                               timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', ?)
            """,
            (name, enabled),
        ).lastrowid
    )


def _row_for_station(html: str, pws_station_id: str) -> str:
    start = html.index(f"<td>{pws_station_id}</td>")
    end = html.index("</tr>", start)
    return html[start:end]


def test_sites_page_shows_observation_age(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pin the rendered Status cell: the age suffix and its template gate.

    Catches: the ``{% if %}`` gate dropped (station E would render
    ``latest observation None``); the age hidden for a disabled station or a
    disabled site's station; the label moved after the age.
    """
    monkeypatch.setattr(web_context, "utc_now", lambda: NOW)
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Example On")
    station_a = _insert_station(conn, site_id, "EXAMPLE-A", enabled=1)
    station_e = _insert_station(conn, site_id, "EXAMPLE-E", enabled=1)
    station_d = _insert_station(conn, site_id, "EXAMPLE-D", enabled=0)
    _insert_poll_state(conn, station_a, isoformat_utc(NOW - timedelta(hours=3)))
    _insert_poll_state(conn, station_e, isoformat_utc(NOW - timedelta(minutes=30)))
    _insert_poll_state(conn, station_d, isoformat_utc(NOW - timedelta(days=2)))

    off_site_id = _make_site(conn, "Example Off", enabled=0)
    station_f = _insert_station(conn, off_site_id, "EXAMPLE-F", enabled=1)
    _insert_poll_state(conn, station_f, isoformat_utc(NOW - timedelta(hours=5)))
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/sites")
    assert response.status_code == 200
    html = response.text

    row_a = _row_for_station(html, "EXAMPLE-A")
    assert "Enabled · latest observation 3 h old" in row_a

    row_d = _row_for_station(html, "EXAMPLE-D")
    assert "Disabled · latest observation 2 d old" in row_d

    row_f = _row_for_station(html, "EXAMPLE-F")
    assert "Enabled · latest observation 5 h old" in row_f

    row_e = _row_for_station(html, "EXAMPLE-E")
    assert "Enabled" in row_e
    assert "latest observation" not in row_e

    assert "next check" not in html
