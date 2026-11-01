"""The v11 daily-close-days migration (plan 2026-10-20-daily-close-day-
state-and-guard.md §10.2-10.7, test IDs MG1-MG6).

A "version-10 file" is built by migrating a fresh ``:memory:`` connection
with ``run_migrations``, then genuinely removing the v11 additions: the
four new tables (dropped ``site_day_stations``, ``site_days``,
``station_changes``, ``site_zone_changes``, in that order, so the table
whose foreign key points at ``site_days`` is gone before ``site_days``
itself is dropped) and the ``stations.retired_at`` column via a real
``ALTER TABLE ... DROP COLUMN``, before setting ``PRAGMA user_version =
10`` -- the same drop-then-roll-back shape
``tests/test_v8_last_usable_fetch_migration.py`` uses for its
``_drop_stamp_column_and_roll_back`` and ``_v6_db``. It holds station A
(enabled) and B (turned off) at site 1, and station C (enabled) at site 2,
per the plan's MG1 fixture note (§10.7-adjacent text above the MG table).

``migrate_v11_daily_close_days`` does not exist yet, so importing it is
part of the red proof for this whole module.
"""

from __future__ import annotations

import sqlite3

import pytest

from tests.helpers import NOW, D
from wxverify.core.timeutil import isoformat_utc_micro
from wxverify.db.migrations import (
    TARGET_USER_VERSION,
    migrate_v11_daily_close_days,
    run_migrations,
)

_FAKED_NOW = NOW.replace(hour=0, minute=30, second=0, microsecond=0)
_SEED_EFFECTIVE_DATE = "0001-01-01"


def _v10_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)

    # Genuinely remove the v11 additions rather than guard around them: a
    # conditional drop would let this fixture silently stay at the wrong
    # version. site_day_stations first, since its foreign key points at
    # site_days; station_changes and site_zone_changes have no foreign key
    # between them or to site_days, so their order does not matter.
    conn.execute("DROP TABLE site_day_stations")
    conn.execute("DROP TABLE site_days")
    conn.execute("DROP TABLE station_changes")
    conn.execute("DROP TABLE site_zone_changes")

    cols_before = {r["name"] for r in conn.execute("PRAGMA table_info(stations)")}
    assert "retired_at" in cols_before
    conn.execute("ALTER TABLE stations DROP COLUMN retired_at")
    cols_after = {r["name"] for r in conn.execute("PRAGMA table_info(stations)")}
    assert "retired_at" not in cols_after

    conn.execute("PRAGMA user_version = 10")

    conn.execute(
        """
        INSERT INTO sites (id, name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (1, 'Site One', 40.0, -105.0, 900.0, 'UTC')
        """
    )
    conn.execute(
        """
        INSERT INTO sites (id, name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (2, 'Site Two', 10.0, 20.0, 100.0, 'UTC')
        """
    )
    conn.execute(
        """
        INSERT INTO stations
            (id, site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
        VALUES (1, 1, 'ISTATION01', 0.0, 0.0, 0.0, 1)
        """
    )
    conn.execute(
        """
        INSERT INTO stations
            (id, site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
        VALUES (2, 1, 'ISTATION02', 0.0, 0.0, 0.0, 0)
        """
    )
    conn.execute(
        """
        INSERT INTO stations
            (id, site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
        VALUES (3, 2, 'ISTATION03', 0.0, 0.0, 0.0, 1)
        """
    )
    conn.commit()
    return conn


def _migrate(conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("wxverify.db.migrations.utc_now", lambda: _FAKED_NOW)
    run_migrations(conn)


def _station_changes(conn: sqlite3.Connection) -> list[tuple[int, str, str, str, str]]:
    rows = conn.execute(
        """
        SELECT station_id, change, effective_date, reason, changed_at
        FROM station_changes ORDER BY station_id
        """
    ).fetchall()
    return [
        (
            r["station_id"],
            r["change"],
            r["effective_date"],
            r["reason"],
            r["changed_at"],
        )
        for r in rows
    ]


def test_mg1_migrate_the_version_10_file(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _v10_db()

    _migrate(conn, monkeypatch)

    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == TARGET_USER_VERSION

    retired = {
        r["id"]: r["retired_at"]
        for r in conn.execute("SELECT id, retired_at FROM stations")
    }
    assert set(retired.values()) == {None}

    assert conn.execute("SELECT COUNT(*) FROM site_days").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM site_day_stations").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM site_zone_changes").fetchone()[0] == 0

    expected_changed_at = isoformat_utc_micro(_FAKED_NOW)
    changes = _station_changes(conn)
    assert changes == [
        (1, "added", _SEED_EFFECTIVE_DATE, "switch_seed", expected_changed_at),
        (2, "turned_off", _SEED_EFFECTIVE_DATE, "switch_seed", expected_changed_at),
        (3, "added", _SEED_EFFECTIVE_DATE, "switch_seed", expected_changed_at),
    ]


def test_mg2_a_station_already_seeded_is_left_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _v10_db()
    _migrate(conn, monkeypatch)

    conn.execute(
        """
        INSERT INTO stations
            (id, site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
        VALUES (4, 1, 'ISTATION04', 0.0, 0.0, 0.0, 1)
        """
    )
    seeded_changed_at = isoformat_utc_micro(_FAKED_NOW)
    conn.execute(
        """
        INSERT INTO station_changes
            (site_id, station_id, change, effective_date, reason, changed_at)
        VALUES (1, 4, 'added', ?, 'switch_seed', ?)
        """,
        (_SEED_EFFECTIVE_DATE, seeded_changed_at),
    )
    conn.commit()

    later = _FAKED_NOW.replace(minute=31)
    monkeypatch.setattr("wxverify.db.migrations.utc_now", lambda: later)
    migrate_v11_daily_close_days(conn)
    run_migrations(conn)

    rows = conn.execute(
        "SELECT change, effective_date, reason, changed_at "
        "FROM station_changes WHERE station_id = 4"
    ).fetchall()
    assert len(rows) == 1
    assert (
        rows[0]["change"],
        rows[0]["effective_date"],
        rows[0]["reason"],
        rows[0]["changed_at"],
    ) == ("added", _SEED_EFFECTIVE_DATE, "switch_seed", seeded_changed_at)


def test_mg3_a_crash_before_the_version_write_is_safe_to_repeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _v10_db()
    _migrate(conn, monkeypatch)

    first_changes = _station_changes(conn)

    conn.execute("PRAGMA user_version = 10")
    run_migrations(conn)

    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == TARGET_USER_VERSION
    assert _station_changes(conn) == first_changes


def test_mg4_fresh_and_migrated_files_agree_on_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migrated = _v10_db()
    _migrate(migrated, monkeypatch)

    fresh = sqlite3.connect(":memory:")
    fresh.row_factory = sqlite3.Row
    fresh.execute("PRAGMA foreign_keys=ON")
    run_migrations(fresh)

    for table in (
        "stations",
        "site_days",
        "station_changes",
        "site_day_stations",
        "site_zone_changes",
    ):
        fresh_cols = [tuple(r) for r in fresh.execute(f"PRAGMA table_xinfo({table})")]
        migrated_cols = [
            tuple(r) for r in migrated.execute(f"PRAGMA table_xinfo({table})")
        ]
        assert fresh_cols == migrated_cols, table

    fresh_station_cols = [
        r["name"] for r in fresh.execute("PRAGMA table_info(stations)")
    ]
    assert fresh_station_cols[-1] == "retired_at"

    for index in (
        "idx_site_days_state",
        "idx_station_changes_station",
        "idx_site_zone_changes_site",
        "idx_site_days_writable",
    ):
        fresh_idx = [tuple(r) for r in fresh.execute(f"PRAGMA index_xinfo({index})")]
        migrated_idx = [
            tuple(r) for r in migrated.execute(f"PRAGMA index_xinfo({index})")
        ]
        assert fresh_idx == migrated_idx, index


def test_mg5_site_days_check_constraints(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _v10_db()
    _migrate(conn, monkeypatch)

    base = {
        "site_id": 1,
        "local_date": D.isoformat(),
        "day_start_utc": isoformat_utc_micro(NOW),
        "day_end_utc": isoformat_utc_micro(NOW),
        "timezone": "UTC",
        "created_at": isoformat_utc_micro(NOW),
        "state_changed_at": isoformat_utc_micro(NOW),
    }

    def _insert(extra: dict[str, object]) -> None:
        row = dict(base)
        row.update(extra)
        columns = ", ".join(row.keys())
        placeholders = ", ".join("?" for _ in row)
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                f"INSERT INTO site_days ({columns}) VALUES ({placeholders})",
                tuple(row.values()),
            )

    # closed without done_at
    _insert({"state": "closed", "attempts": 1})
    # missed without missed_reason (attempts 0, the never_attempted shape, so
    # this trips only the missed/missed_reason CHECK and not the retry_at one)
    _insert({"state": "missed", "attempts": 0, "done_at": isoformat_utc_micro(NOW)})
    # open with attempts 1
    _insert({"state": "open", "attempts": 1})
    # retry_due without retry_at
    _insert({"state": "retry_due", "attempts": 1})
    # closing with attempts 1 and a retry_at (retry_at only valid at attempts=2
    # or state retry_due or missed_reason retry_not_run)
    _insert(
        {
            "state": "closing",
            "attempts": 1,
            "retry_at": isoformat_utc_micro(NOW),
        }
    )
    # closed with zone_changeover 1 (changeover day must be open or missed)
    _insert(
        {
            "state": "closed",
            "attempts": 1,
            "done_at": isoformat_utc_micro(NOW),
            "zone_changeover": 1,
        }
    )
    # open with missed_reason zone_changeover (missed_reason only on missed days)
    _insert({"state": "open", "attempts": 0, "missed_reason": "zone_changeover"})
    # a sixth, invalid state
    _insert({"state": "bogus", "attempts": 0})


def test_mg6_station_changes_cascades_with_its_station(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _v10_db()
    _migrate(conn, monkeypatch)

    problems = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert problems == []

    conn.execute("DELETE FROM stations WHERE id = 3")

    remaining = conn.execute(
        "SELECT COUNT(*) FROM station_changes WHERE station_id = 3"
    ).fetchone()[0]
    assert remaining == 0
