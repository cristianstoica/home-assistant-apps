"""The widened ``station_wind_records`` schema (plan 2026-10-21-daily-
close-download-and-raw-store.md, section 11, test IDs SC3-SC4): its live
CHECK/key constraints and the site-delete cascade. SC1 and SC2, which
pinned the now-deleted migration that used to build this shape onto an
older file, are retired (see the note below).

Real SQLite and real migrations throughout; nothing in the code under test
is mocked. Synthetic data only: station id ``ISTATION01``, site name
``Testsite``, zone ``UTC``.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.migrations import run_migrations

# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------


def _migrated_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_station(conn: sqlite3.Connection, pws_station_id: str = "ISTATION01") -> int:
    site_id = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
    ).lastrowid
    station_id = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
        " dem_elevation_m, enabled) VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
        (site_id, pws_station_id),
    ).lastrowid
    conn.commit()
    return int(station_id)


# SC1 (opening a version-11 file rebuilds station_wind_records to the
# fresh shape, rows kept) and SC2 (the rebuild's own re-run guard) are
# retired: both pinned migrate_v12_widen_raw_readings, deleted along with
# every other migrate_v2 ... migrate_v16 function (storage release 4.1).
# station_wind_records' widened shape is an unconditional part of
# create_tables now, with no separate rebuild step left to guard or
# compare against a fresh file, and user_version 1-15 is refused outright
# rather than migrated.


# --- SC3 ----------------------------------------------------------------


def test_sc3_checks_and_key_reject_every_bad_row() -> None:
    """SC3 (14.11): each CHECK and the PRIMARY KEY refuses its bad row with
    ``sqlite3.IntegrityError``.

    This used to also run against a version-11 file rebuilt through
    ``run_migrations`` (the version-12 migration's own copy of the CREATE
    statement) -- that comparison is retired with ``migrate_v12_widen_raw_
    readings`` (see the SC1/SC2 note above): there is now only the one
    ``create_tables`` definition of this table left to check.

    M68 -> at the rain-``CHECK`` insert below: correct = raises
    ``sqlite3.IntegrityError`` (``precip_total_mm >= 0``), mutant (the rain
    CHECK dropped) = the insert succeeds.
    """
    conn = _migrated_conn()
    station_id = _make_station(conn)

    def _bad_insert(sql: str, params: tuple[object, ...]) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(sql, params)
        conn.rollback()

    # Every value column NULL -> the "at least one" table CHECK.
    _bad_insert(
        "INSERT INTO station_wind_records (station_id, obs_at, source)"
        " VALUES (?, '2026-10-27T10:00:00Z', 'five_minute')",
        (station_id,),
    )
    # Negative rain -> the rain CHECK (M68).
    _bad_insert(
        "INSERT INTO station_wind_records (station_id, obs_at, source,"
        " precip_total_mm) VALUES (?, '2026-10-27T10:05:00Z', 'five_minute', -0.1)",
        (station_id,),
    )
    # Negative wind -> the wind CHECK.
    _bad_insert(
        "INSERT INTO station_wind_records (station_id, obs_at, source,"
        " speed_kmh) VALUES (?, '2026-10-27T10:10:00Z', 'five_minute', -1.0)",
        (station_id,),
    )
    # An unlisted source -> the source CHECK.
    _bad_insert(
        "INSERT INTO station_wind_records (station_id, obs_at, source,"
        " speed_kmh) VALUES (?, '2026-10-27T10:15:00Z', 'bogus', 7.0)",
        (station_id,),
    )
    # No source at all -> NOT NULL.
    _bad_insert(
        "INSERT INTO station_wind_records (station_id, obs_at, speed_kmh)"
        " VALUES (?, '2026-10-27T10:20:00Z', 7.0)",
        (station_id,),
    )
    # A real row, then its exact key repeated -> the PRIMARY KEY.
    conn.execute(
        "INSERT INTO station_wind_records (station_id, obs_at, source,"
        " speed_kmh) VALUES (?, '2026-10-27T10:25:00Z', 'five_minute', 7.0)",
        (station_id,),
    )
    conn.commit()
    _bad_insert(
        "INSERT INTO station_wind_records (station_id, obs_at, source,"
        " speed_kmh) VALUES (?, '2026-10-27T10:25:00Z', 'five_minute', 8.0)",
        (station_id,),
    )


# --- SC4 ----------------------------------------------------------------


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


def test_sc4_site_delete_route_cascades_to_station_wind_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SC4 (14.11): deleting a site through the route removes its stations'
    raw readings via the ``stations(id)`` cascade.

    M69 -> at the ``remaining`` assertion: correct = [] (``ON DELETE
    CASCADE`` removes the row with its station), mutant (``ON DELETE
    CASCADE`` dropped from ``station_wind_records``) = one surviving row.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
    ).lastrowid
    station_id = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
        " dem_elevation_m, enabled) VALUES (?, 'ISTATION01', 0.0, 0.0, 0.0, 1)",
        (site_id,),
    ).lastrowid
    conn.execute(
        "INSERT INTO station_wind_records (station_id, obs_at, source,"
        " speed_kmh) VALUES (?, '2026-10-27T10:00:00Z', 'five_minute', 7.0)",
        (station_id,),
    )
    conn.commit()

    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        response = client.delete(f"/api/sites/{site_id}", headers=headers)
        assert response.status_code == 200

        live_conn = get_db()._conn  # noqa: SLF001
        remaining = live_conn.execute(
            "SELECT 1 FROM station_wind_records WHERE station_id = ?",
            (station_id,),
        ).fetchall()
        assert remaining == []
        fk_violations = live_conn.execute(
            "PRAGMA foreign_key_check(station_wind_records)"
        ).fetchall()
        assert fk_violations == []


# SC5 (import-file validation of fetched_at/local_date in
# station_wind_records via `_validate_upload`) was removed with the database
# import (storage-release plan 5.5).
