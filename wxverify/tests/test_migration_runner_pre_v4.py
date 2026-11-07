"""Pre-v4 database upgrade: the runner orders tables -> migrations -> indexes.

Only the fresh-database path remains here: `run_migrations` on an empty
file must yield exactly what `create_schema` produces, plus the three
v2-backfill triggers.
"""

from __future__ import annotations

import sqlite3

from wxverify.db.migrations import create_schema, run_migrations

_TRIGGERS = frozenset(
    {
        "trg_sites_backfill_status_insert_check",
        "trg_sites_backfill_status_insert_default",
        "trg_sites_backfill_status_update_check",
    }
)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _run(conn: sqlite3.Connection) -> None:
    """Mirror `Database._run_immediate` on a bare connection."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        run_migrations(conn)
    except BaseException:
        conn.rollback()
        raise
    conn.commit()


def test_o10_run_migrations_on_empty_matches_create_schema_plus_triggers() -> None:
    """`run_migrations` on an empty file yields every object `create_schema`
    produces, plus the three v2-backfill triggers.

    Catches: `create_indexes` dropped from the runner while still in the
    composite (M3), `create_tables` dropped (M4).
    """
    fresh = _connect()
    _run(fresh)
    fresh_names = {
        row["name"]
        for row in fresh.execute(
            "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        )
    }

    composite = _connect()
    create_schema(composite)
    composite_names = {
        row["name"]
        for row in composite.execute(
            "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        )
    }

    assert fresh_names == composite_names | _TRIGGERS
    # 78 since the rain release (Part B): forecast_scored and idx_samples_slot;
    # 79 with Part A's station_rain_checks; 83 with Part C's rain_days,
    # rain_forecast_days, rain_picks and publication_rain_ranks; 69 with the
    # storage release: +1 forecast_history, -15 for forecast_pairs,
    # close_pairs and the seven verification_* tables and their own
    # indexes, all dropped (3.1, 4.1); 68 with card 29.
    assert len(fresh_names) == 68
