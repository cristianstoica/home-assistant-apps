"""The widened ``station_wind_records`` schema and its import checks (plan
2026-10-21-daily-close-download-and-raw-store.md, section 11, test IDs
SC1-SC5).

Real SQLite and real migrations throughout; nothing in the code under test
is mocked. Synthetic data only: station id ``ISTATION01``, site name
``Testsite``, zone ``UTC``.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.api.errors import ApiError
from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.migrations import (
    TARGET_USER_VERSION,
    migrate_v12_widen_raw_readings,
    run_migrations,
)

# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

# Today's pre-widening shape (db/migrations.py:100-106), copied verbatim, the
# way tests/helpers.py's make_v8_wind_state freezes the v8 station_wind_days
# shape.
_V11_STATION_WIND_RECORDS_DDL = """
    CREATE TABLE station_wind_records (
        station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
        obs_at TEXT NOT NULL,
        speed_kmh REAL NOT NULL CHECK (speed_kmh >= 0),
        PRIMARY KEY (station_id, obs_at)
    ) WITHOUT ROWID;
"""


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


def _v11_fixture_conn() -> tuple[sqlite3.Connection, int]:
    """A fully migrated database with ``station_wind_records`` put back in
    today's pre-widening shape and ``user_version`` forced back to 11.

    ``run_migrations`` above also brings ``station_wind_days`` to the
    CURRENT shape, which once P2's migration lands already carries
    ``refused_at`` (11.2) -- a version-11 file never had it. Drop it here,
    guarded by its own presence check, so SC1's ``assert "refused_at" in
    wind_days_cols`` actually exercises the migration's own column-add
    (3.) instead of a column the full migration above already put there.
    """
    conn = _migrated_conn()
    conn.execute("DROP TABLE station_wind_records")
    conn.executescript(_V11_STATION_WIND_RECORDS_DDL)
    columns = {
        r["name"] for r in conn.execute("PRAGMA table_xinfo(station_wind_records)")
    }
    assert columns == {"station_id", "obs_at", "speed_kmh"}, (
        "the v11 DDL copy must carry none of the widened columns"
    )
    wind_days_columns = {
        r["name"] for r in conn.execute("PRAGMA table_info(station_wind_days)")
    }
    if "refused_at" in wind_days_columns:
        conn.execute("ALTER TABLE station_wind_days DROP COLUMN refused_at")
    station_id = _make_station(conn)
    conn.execute(
        "INSERT INTO station_wind_records (station_id, obs_at, speed_kmh)"
        " VALUES (?, '2026-10-27T10:00:00Z', 7.0)",
        (station_id,),
    )
    conn.execute("PRAGMA user_version = 11")
    conn.commit()
    return conn, station_id


# --- SC1 ----------------------------------------------------------------


def test_sc1_v11_file_rebuilds_to_the_fresh_shape_with_rows_kept() -> None:
    """SC1 (14.11): opening a version-11 file rebuilds ``station_wind_records``
    to exactly the fresh shape, with its existing rows copied forward as
    ``source='five_minute'`` and the widened columns NULL; ``refused_at`` is
    present on ``station_wind_days``.

    M66 -> at the ``len(rows)`` assertion below: correct = 1 (the existing
    row survives the rebuild), mutant (the rebuild drops the existing rows,
    no ``INSERT ... SELECT``) = 0.
    """
    old_conn, station_id = _v11_fixture_conn()
    run_migrations(old_conn)

    fresh_conn = _migrated_conn()

    old_shape = [
        dict(r) for r in old_conn.execute("PRAGMA table_xinfo(station_wind_records)")
    ]
    fresh_shape = [
        dict(r) for r in fresh_conn.execute("PRAGMA table_xinfo(station_wind_records)")
    ]
    assert old_shape == fresh_shape

    for conn in (old_conn, fresh_conn):
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == TARGET_USER_VERSION, (
            "the version is read from TARGET_USER_VERSION, never a literal, "
            "since P3-P6 raise it further"
        )

    rows = old_conn.execute(
        "SELECT station_id, source, local_date, speed_kmh, temp_c,"
        " precip_total_mm, fetched_at FROM station_wind_records"
    ).fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["station_id"] == station_id
    assert row["source"] == "five_minute"
    assert row["speed_kmh"] == 7.0
    assert row["local_date"] is None
    assert row["temp_c"] is None
    assert row["precip_total_mm"] is None
    assert row["fetched_at"] is None

    wind_days_cols = {
        r["name"] for r in old_conn.execute("PRAGMA table_info(station_wind_days)")
    }
    assert "refused_at" in wind_days_cols


# --- SC2 ----------------------------------------------------------------


def test_sc2_rerun_guard_skips_the_rebuild_when_source_already_present() -> None:
    """SC2 (14.11): the rebuild already committed but ``user_version`` was
    left at 11 (a crash between the two) -- re-entry opens straight to
    ``TARGET_USER_VERSION`` with no error and both rows unchanged, column for
    column: one ``five_minute`` row and, at another instant, a
    ``current_obs`` row with speed, temperature, rain total, local date and
    fetch time.

    M67 -> at the ``after_rows == before_rows`` assertion: correct = True
    (the re-run guard skips the ``INSERT ... SELECT`` when the column
    already exists, so neither row changes); mutant (the re-run guard
    dropped) = False, the ``current_obs`` row comes back with ``source``
    overwritten to ``'five_minute'`` and its ``local_date``, ``temp_c``,
    ``precip_total_mm`` and ``fetched_at`` all NULL, the copy running a
    second time against the already-widened table.
    """
    conn = _migrated_conn()
    station_id = _make_station(conn)
    conn.execute(
        "INSERT INTO station_wind_records (station_id, obs_at, source,"
        " speed_kmh) VALUES (?, '2026-10-27T10:00:00Z', 'five_minute', 7.0)",
        (station_id,),
    )
    conn.execute(
        "INSERT INTO station_wind_records (station_id, obs_at, source,"
        " speed_kmh, temp_c, precip_total_mm, local_date, fetched_at)"
        " VALUES (?, '2026-10-27T10:05:00Z', 'current_obs', 5.0, 12.3, 0.4,"
        " '2026-10-27', '2026-10-27T10:06:00Z')",
        (station_id,),
    )
    # The crash: the rebuild (step 2) already committed, but the version
    # PRAGMA (written only at the end of run_migrations) was not.
    conn.execute("PRAGMA user_version = 11")
    conn.commit()

    before_rows = [
        dict(r)
        for r in conn.execute(
            "SELECT station_id, obs_at, source, local_date, speed_kmh,"
            " temp_c, precip_total_mm, fetched_at FROM station_wind_records"
            " ORDER BY obs_at, source"
        )
    ]

    run_migrations(conn)

    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == TARGET_USER_VERSION

    after_rows = [
        dict(r)
        for r in conn.execute(
            "SELECT station_id, obs_at, source, local_date, speed_kmh,"
            " temp_c, precip_total_mm, fetched_at FROM station_wind_records"
            " ORDER BY obs_at, source"
        )
    ]
    assert after_rows == before_rows

    # Calling the migration directly on a connection whose table already
    # carries `source` (the guard's own condition) must change nothing: the
    # re-run guard skips the rebuild, and `refused_at` is already present.
    before_shape = [
        dict(r) for r in conn.execute("PRAGMA table_xinfo(station_wind_records)")
    ]
    before_count = conn.execute("SELECT COUNT(*) FROM station_wind_records").fetchone()[
        0
    ]

    migrate_v12_widen_raw_readings(conn)

    after_shape = [
        dict(r) for r in conn.execute("PRAGMA table_xinfo(station_wind_records)")
    ]
    after_count = conn.execute("SELECT COUNT(*) FROM station_wind_records").fetchone()[
        0
    ]
    assert after_shape == before_shape
    assert after_count == before_count


# --- SC3 ----------------------------------------------------------------


def _fresh_station_conn() -> tuple[sqlite3.Connection, int]:
    """SC3's first builder: a fresh ``create_tables``-shaped table."""
    conn = _migrated_conn()
    return conn, _make_station(conn)


def _v11_rebuilt_station_conn() -> tuple[sqlite3.Connection, int]:
    """SC3's second builder: a version-11 file rebuilt via ``run_migrations``,
    as SC1 does -- the version-12 rebuild's own copy of the CREATE
    statement (``_rebuild_station_wind_records``), not the one
    ``create_tables`` runs on a fresh file.
    """
    conn, station_id = _v11_fixture_conn()
    run_migrations(conn)
    return conn, station_id


@pytest.mark.parametrize(
    "build_conn",
    [_fresh_station_conn, _v11_rebuilt_station_conn],
    ids=["fresh_create_tables", "v11_file_rebuilt"],
)
def test_sc3_checks_and_key_reject_every_bad_row(
    build_conn: Callable[[], tuple[sqlite3.Connection, int]],
) -> None:
    """SC3 (14.11): each CHECK and the PRIMARY KEY refuses its bad row with
    ``sqlite3.IntegrityError``.

    Run on BOTH of ``station_wind_records``'s two builders: a fresh
    ``create_tables``-built table, and one rebuilt from a version-11 file
    through ``run_migrations`` (SC1's own ``_v11_fixture_conn`` plus
    ``run_migrations``, the version-12 migration's own copy of the CREATE
    statement, ``_rebuild_station_wind_records``). SC1 compares only
    ``table_xinfo``, which carries no CHECKs, so without this parametrize
    the rebuild's own CREATE-statement copy had no CHECK/key coverage of
    its own -- a CHECK present on the fresh table but dropped from the
    rebuild's copy would pass this test on the ``fresh_create_tables`` case
    alone.

    M68 -> at the rain-``CHECK`` insert below: correct = raises
    ``sqlite3.IntegrityError`` (``precip_total_mm >= 0``), mutant (the rain
    CHECK dropped) = the insert succeeds.
    """
    conn, station_id = build_conn()

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


# --- SC5 ----------------------------------------------------------------


def _make_upload_db(
    path: Path, *, fetched_at: str | None, local_date: str | None, drop_table: bool
) -> None:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    if drop_table:
        conn.execute("DROP TABLE station_wind_records")
        conn.commit()
        conn.close()
        return
    station_id = _make_station(conn)
    conn.execute(
        "INSERT INTO station_wind_records (station_id, obs_at, source,"
        " speed_kmh, local_date, fetched_at) VALUES (?, ?, 'five_minute',"
        " 7.0, ?, ?)",
        (station_id, "2026-10-27T10:00:00Z", local_date, fetched_at),
    )
    conn.commit()
    conn.close()


def _make_v11_shape_upload_db(path: Path) -> None:
    """A backup from before this release: ``station_wind_records`` still in
    its version-11 shape (no ``source``, no ``fetched_at``, no
    ``local_date``), with one canonical row.
    """
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    conn.execute("DROP TABLE station_wind_records")
    conn.executescript(_V11_STATION_WIND_RECORDS_DDL)
    station_id = _make_station(conn)
    conn.execute(
        "INSERT INTO station_wind_records (station_id, obs_at, speed_kmh)"
        " VALUES (?, '2026-10-27T10:00:00Z', 7.0)",
        (station_id,),
    )
    conn.commit()
    conn.close()


def test_sc5_fetched_at_and_local_date_checked_the_table_absent_skipped(
    tmp_path: Path,
) -> None:
    """SC5 (14.11): import files with a non-canonical ``fetched_at``, a bad
    ``local_date``, then all NULL, then no table, then the table in its
    version-11 shape (no ``fetched_at``, no ``local_date``) -- 422 for the
    first two with the existing messages; the last three accepted.

    M70 -> if ``("station_wind_records", "fetched_at")`` joined
    ``_CANONICAL_STAMP_COLUMNS`` (which refuses NULL), the ``all_null``
    file below -- whose ``fetched_at`` IS NULL -- would be refused too;
    correct = ``_validate_upload`` does not raise on it, mutant = it does.
    """
    bad_fetched_at = tmp_path / "bad_fetched_at.db"
    _make_upload_db(
        bad_fetched_at,
        fetched_at="2026-10-27 10:00:00",
        local_date=None,
        drop_table=False,
    )
    with pytest.raises(ApiError) as excinfo:
        _validate_upload(bad_fetched_at)
    assert excinfo.value.status_code == 422
    assert (
        "invalid timestamp in station_wind_records.fetched_at: expected UTC"
        " form YYYY-MM-DDTHH:MM:SSZ" in str(excinfo.value)
    )

    bad_local_date = tmp_path / "bad_local_date.db"
    _make_upload_db(
        bad_local_date,
        fetched_at=None,
        local_date="2026/10/27",
        drop_table=False,
    )
    with pytest.raises(ApiError) as excinfo:
        _validate_upload(bad_local_date)
    assert excinfo.value.status_code == 422
    assert (
        "invalid date in station_wind_records.local_date: expected"
        " YYYY-MM-DD" in str(excinfo.value)
    )

    all_null = tmp_path / "all_null.db"
    _make_upload_db(all_null, fetched_at=None, local_date=None, drop_table=False)
    _validate_upload(all_null)  # must not raise

    no_table = tmp_path / "no_table.db"
    _make_upload_db(no_table, fetched_at=None, local_date=None, drop_table=True)
    _validate_upload(no_table)  # must not raise

    v11_shape = tmp_path / "v11_shape.db"
    _make_v11_shape_upload_db(v11_shape)
    _validate_upload(v11_shape)  # must not raise
