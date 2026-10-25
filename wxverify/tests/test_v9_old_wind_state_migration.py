"""The ``user_version`` 9 migration step: deletes the six per-site wind key
kinds left over from the 0.16.6 wind switch and drops
``station_wind_days.legacy_hours`` (old-wind-method-removal plan §10.5,
§6.9).

Imports kept names only (``run_migrations``, ``create_tables``,
``TARGET_USER_VERSION``, ``set_runtime_state``, ``get_runtime_state``), so
this file imports cleanly on the pre-removal tree; every pin below is D and
fails there by assertion, because base ``run_migrations`` has no v9 gate and
leaves a ``user_version`` 8 database at 8 with its keys and column.

Synthetic data only: site ``Testsite``, station ``WSTATION01``, dates in
2035.
"""

from __future__ import annotations

import logging
import sqlite3

import pytest

from tests.helpers import make_v8_wind_state
from wxverify.db.migrations import TARGET_USER_VERSION, create_tables, run_migrations
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state

_KEPT_KEYS = (
    "wind_history_auth_hold:history_all",
    "wind_backfill_calls",
    "verification_state:1",
)
_DECOY_KEYS = ("wind-basis:1", "WIND_BASIS:1", "wind_basis_extra:1")
_REMOVED_KEY_KINDS = (
    "wind_basis",
    "wind_basis_progress_at",
    "wind_basis_cursor",
    "wind_basis_blocked",
    "wind_rebuild_report",
    "wind_rebuild_done_at",
)


def _v8_fixture_conn() -> sqlite3.Connection:
    """An empty database migrated to v8, put back in the v8 wind shape, with
    two sites' worth of leftover keys plus the keepers and decoys that must
    survive (plan §10.5)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    make_v8_wind_state(conn)

    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    scur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m,"
        " enabled) VALUES (?, 'WSTATION01', 0.0, 0.0, 0.0, 1)",
        (site_id,),
    )
    assert scur.lastrowid is not None
    station_id = int(scur.lastrowid)

    conn.execute(
        "INSERT INTO station_wind_days (station_id, local_date, status,"
        " legacy_hours, updated_at) VALUES (?, '2035-06-01', 'fetched', 3,"
        " '2035-06-01T23:00:00Z')",
        (station_id,),
    )
    conn.execute(
        "INSERT INTO station_wind_days (station_id, local_date, status,"
        " legacy_hours, updated_at) VALUES (?, '2035-06-02', 'partial', NULL,"
        " '2035-06-02T23:00:00Z')",
        (station_id,),
    )

    for site in (1, 2):
        for kind in _REMOVED_KEY_KINDS:
            set_runtime_state(conn, f"{kind}:{site}", f"leftover-{site}")
    for key in _KEPT_KEYS:
        set_runtime_state(conn, key, "keep")
    for key in _DECOY_KEYS:
        set_runtime_state(conn, key, "decoy")
    conn.commit()
    return conn


def _all_runtime_state(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        row["key"]: row["value"]
        for row in conn.execute("SELECT key, value FROM runtime_state")
    }


def _wind_days_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM station_wind_days ORDER BY local_date"
    ).fetchall()


def test_d17_v9_step_deletes_only_the_twelve_leftover_keys() -> None:
    conn = _v8_fixture_conn()
    before = _all_runtime_state(conn)

    run_migrations(conn)

    after = _all_runtime_state(conn)
    removed_keys = {f"{kind}:{site}" for site in (1, 2) for kind in _REMOVED_KEY_KINDS}
    for key in removed_keys:
        assert key not in after, (
            "mutant -> at this assertion: correct = every leftover key gone,"
            f" mutant (a kind left out of the DELETE's IN list) = {key!r} still"
            " present"
        )
    for key, value in before.items():
        if key in removed_keys:
            continue
        assert after.get(key) == value, (
            "mutant -> at this assertion: correct = every kept key unchanged,"
            f" mutant (LIKE 'wind_%' or a per-kind LIKE) = {key!r} altered or"
            " gone"
        )
    assert set(after) == set(before) - removed_keys


def test_d18_v9_step_drops_legacy_hours_matching_a_fresh_database() -> None:
    conn = _v8_fixture_conn()
    before_rows = _wind_days_rows(conn)

    run_migrations(conn)

    after_cols = [
        dict(row) for row in conn.execute("PRAGMA table_xinfo(station_wind_days)")
    ]
    fresh = sqlite3.connect(":memory:")
    fresh.row_factory = sqlite3.Row
    fresh.execute("PRAGMA foreign_keys=ON")
    run_migrations(fresh)
    fresh_cols = [
        dict(row) for row in fresh.execute("PRAGMA table_xinfo(station_wind_days)")
    ]
    assert "legacy_hours" not in {c["name"] for c in after_cols}, (
        "mutant -> at this assertion: correct = legacy_hours gone, mutant"
        " (the ALTER TABLE ... DROP COLUMN removed) = legacy_hours still"
        " present"
    )
    assert after_cols == fresh_cols, (
        "mutant -> at this assertion: correct = a migrated database's"
        " columns match a fresh database's, mutant (TARGET_USER_VERSION"
        " left at 8, so run_migrations stops before the v9 gate) ="
        " legacy_hours still present and the column lists diverge"
    )

    after_rows = _wind_days_rows(conn)
    assert len(after_rows) == len(before_rows) == 2
    for before_row, after_row in zip(before_rows, after_rows, strict=True):
        assert after_row["station_id"] == before_row["station_id"]
        assert after_row["local_date"] == before_row["local_date"]
        assert after_row["status"] == before_row["status"]
        assert after_row["record_count"] == before_row["record_count"]
        assert after_row["pair_hours"] == before_row["pair_hours"]
        assert after_row["attempts"] == before_row["attempts"]
        assert after_row["next_attempt_at"] == before_row["next_attempt_at"]
        assert after_row["last_error"] == before_row["last_error"]
        assert after_row["last_ok_at"] == before_row["last_ok_at"]
        assert after_row["refetch_at"] == before_row["refetch_at"]
        assert after_row["refetched"] == before_row["refetched"]
        assert after_row["updated_at"] == before_row["updated_at"]

    row = conn.execute("PRAGMA user_version").fetchone()
    assert int(row[0]) == TARGET_USER_VERSION == 10, (
        "mutant -> at this assertion: correct = user_version 10, mutant"
        " (TARGET_USER_VERSION left at 9) = user_version 9"
    )

    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    empty.execute("PRAGMA foreign_keys=ON")
    create_tables(empty)
    created_cols = {
        r["name"] for r in empty.execute("PRAGMA table_info(station_wind_days)")
    }
    assert "legacy_hours" not in created_cols, (
        "mutant -> at this assertion: correct = create_tables no longer"
        " adds legacy_hours, mutant (create_tables keeps legacy_hours"
        " INTEGER,) = legacy_hours present on a fresh database"
    )


def test_d19_v9_step_logs_exactly_one_info_line(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _v8_fixture_conn()

    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        run_migrations(conn)

    matching = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO
        and r.getMessage().startswith("old wind state removed: 12 keys deleted,")
        and r.getMessage().endswith(" ms")
    ]
    assert len(matching) == 1, (
        "mutant -> at this assertion: correct = exactly one matching INFO"
        " record, mutant (the logger.info call removed) = none; got"
        f" {len(matching)}"
    )

    before_second = _all_runtime_state(conn)
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        run_migrations(conn)
    second_run = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO and "old wind state removed" in r.getMessage()
    ]
    assert second_run == [], (
        "mutant -> at this assertion: correct = a second run logs nothing,"
        " mutant (the `if deleted or dropped:` guard removed) = a second"
        " line logged"
    )
    assert get_runtime_state(conn, "wind_backfill_calls") == "keep"
    assert _all_runtime_state(conn) == before_second

    fresh = sqlite3.connect(":memory:")
    fresh.row_factory = sqlite3.Row
    fresh.execute("PRAGMA foreign_keys=ON")
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        run_migrations(fresh)
    fresh_run = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO and "old wind state removed" in r.getMessage()
    ]
    assert fresh_run == [], (
        "mutant -> at this assertion: correct = a fresh database logs"
        " nothing, mutant (create_tables keeps legacy_hours INTEGER, so the"
        " step still finds the column) = a line logged"
    )


def test_d20_v9_step_converges_whichever_half_already_ran(
    caplog: pytest.LogCaptureFixture,
) -> None:
    removed_keys = {f"{kind}:{site}" for site in (1, 2) for kind in _REMOVED_KEY_KINDS}

    column_already_dropped = _v8_fixture_conn()
    column_already_dropped.execute(
        "ALTER TABLE station_wind_days DROP COLUMN legacy_hours"
    )
    column_already_dropped.commit()
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        run_migrations(column_already_dropped)
    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert any(
        m.startswith("old wind state removed: 12 keys deleted,")
        and "legacy_hours column already gone" in m
        for m in messages
    ), (
        "mutant -> at this assertion: correct = the re-entry line reports"
        " the column already gone, mutant (the DELETE removed) = the"
        " twelve keys survive, so re-running the step never reaches this"
        " line"
    )
    after_column_first = _all_runtime_state(column_already_dropped)
    assert not removed_keys & set(after_column_first)
    for key in (*_KEPT_KEYS, *_DECOY_KEYS):
        assert key in after_column_first
    row = column_already_dropped.execute("PRAGMA user_version").fetchone()
    assert int(row[0]) == 10

    caplog.clear()
    keys_already_deleted = _v8_fixture_conn()
    for key in removed_keys:
        keys_already_deleted.execute("DELETE FROM runtime_state WHERE key = ?", (key,))
    keys_already_deleted.commit()
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        run_migrations(keys_already_deleted)
    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert any(
        m.startswith("old wind state removed: 0 keys deleted,")
        and "legacy_hours column dropped" in m
        for m in messages
    ), (
        "mutant -> at this assertion: correct = the re-entry line reports"
        " zero keys deleted and the column dropped, mutant (the ALTER"
        " TABLE ... DROP COLUMN removed) = legacy_hours survives, so"
        " re-running the step never reaches this line"
    )
    cols_after = {
        r["name"]
        for r in keys_already_deleted.execute("PRAGMA table_info(station_wind_days)")
    }
    assert "legacy_hours" not in cols_after

    caplog.clear()
    both_already_done = _v8_fixture_conn()
    for key in removed_keys:
        both_already_done.execute("DELETE FROM runtime_state WHERE key = ?", (key,))
    both_already_done.execute("ALTER TABLE station_wind_days DROP COLUMN legacy_hours")
    both_already_done.commit()
    before_keys = _all_runtime_state(both_already_done)
    before_rows = _wind_days_rows(both_already_done)
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        run_migrations(both_already_done)
    messages = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert not any("old wind state removed" in m for m in messages), (
        "mutant -> at this assertion: correct = nothing logged when both"
        " halves already ran, mutant (the `if deleted or dropped:` guard"
        " removed) = a line logged anyway"
    )
    row = both_already_done.execute("PRAGMA user_version").fetchone()
    assert int(row[0]) == 10
    assert _all_runtime_state(both_already_done) == before_keys
    assert _wind_days_rows(both_already_done) == before_rows
