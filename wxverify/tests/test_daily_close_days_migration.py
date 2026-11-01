"""The v11 daily-close-days schema's live constraints (plan 2026-10-20-
daily-close-day-state-and-guard.md §10.2-10.7, test IDs MG5-MG6).

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
"""

from __future__ import annotations

import sqlite3

import pytest

from tests.helpers import NOW, D
from wxverify.core.timeutil import isoformat_utc_micro
from wxverify.db.migrations import run_migrations

_FAKED_NOW = NOW.replace(hour=0, minute=30, second=0, microsecond=0)


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
