"""The version-18 upgrade (plan `2026-11-07-health-release.md` §9, §10;
test IDs UT1-UT6, adapted to the sub-steps wired in so far). The version
18 upgrade runs under `migrate_v18`; UT1, UT4, UT5 and UT6 below do not
depend on which of its sub-steps are wired in, and stay correct as later
steps add more. Of U4's four sub-steps (``_v18_poll_pause``,
``_v18_drop_unused_write_state``, ``_v18_prune_wind_history``,
``_v18_drop_bad_display_values``), only ``_v18_drop_unused_write_state`` is
wired in as of this file; UT2 (each part's effect) and UT3 (all-or-nothing)
below cover that one sub-step only -- BT9's ``CHECK`` table in UT1 and the
other three sub-steps' own UT2/UT3 pieces land with the steps that add
them.

Fixture: ``tests/fixtures/v17_schema.sql`` and ``v17_seed.sql``, dumped
from a fresh 0.16.17 (``6f49f1a``) file the same way the version-16 pair
was (6f49f1a's ``tests/test_migration_v17.py:14-25``: schema from ``sqlite_master``,
seed with ``iterdump``, restricted to the boot-seed tables -- ``sources``,
``feeds``, ``settings``, ``runtime_state``; no site, station or job row).
Every test below loads both into a fresh ``tmp_path`` file, sets
``user_version`` 17 and ``foreign_keys`` on, and calls
``migrations.run_migrations`` directly on the autocommit connection
(6f49f1a's ``tests/test_migration_v17.py``/``_run_migrate_v17`` convention: no
surrounding ``BEGIN``, since ``create_tables``'s own ``executescript``
ends any outer transaction anyway, per plan §9's "the transaction" note).

Isolation: every connection is a real, file-backed SQLite database under
``tmp_path``; no mocked persistence. Synthetic data only: nothing here
inserts a site, station or any real identifier -- the fixture itself is
an empty, pre-seeded shell.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pytest

from wxverify.core import stop as stop_module
from wxverify.db import migrations

_SCHEMA_SQL = (Path(__file__).parent / "fixtures" / "v17_schema.sql").read_text(
    encoding="utf-8"
)
_SEED_SQL = (Path(__file__).parent / "fixtures" / "v17_seed.sql").read_text(
    encoding="utf-8"
)


def _v17_connect(db_path: Path) -> sqlite3.Connection:
    """A real, file-backed v17 database at ``db_path``: the 0.16.17 schema
    plus its boot seed, pinned at ``user_version`` 17, autocommit (the
    ``run_migrations``-direct convention; ``wxverify.db.connection.Database``
    also opens with ``isolation_level=None``)."""
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA_SQL)
    conn.executescript(_SEED_SQL)
    conn.execute("PRAGMA user_version = 17")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _schema_names(conn: sqlite3.Connection, sqlite_type: str) -> tuple[str, ...]:
    return tuple(
        sorted(
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = ?"
                " AND name NOT LIKE 'sqlite_%'",
                (sqlite_type,),
            )
        )
    )


def _table_xinfo(
    conn: sqlite3.Connection, table: str
) -> tuple[tuple[object, ...], ...]:
    return tuple(tuple(row) for row in conn.execute(f"PRAGMA table_xinfo('{table}')"))


def _all_table_rows(conn: sqlite3.Connection) -> dict[str, list[tuple[object, ...]]]:
    return {
        table: [tuple(row) for row in conn.execute(f"SELECT * FROM {table}")]  # noqa: S608
        for table in _schema_names(conn, "table")
    }


# ---------------------------------------------------------------------------
# UT1 (adapted): a fresh file and an upgraded v17 file produce the same
# table and index names, and the same table_xinfo for every table. BT9's
# CHECK-table part is not written here -- it lands with step 3 (B).
# ---------------------------------------------------------------------------


def test_ut1_fresh_and_upgraded_schema_have_the_same_names_and_table_xinfo(
    tmp_path: Path,
) -> None:
    """Catches: a table or index the upgrade leaves behind that a fresh file
    does not have (or vice versa); a column the upgrade adds in a different
    order, type or default than a fresh build's `create_tables` -- either
    would show up as a `table_xinfo` mismatch for the affected table."""
    fresh = sqlite3.connect(str(tmp_path / "fresh.db"), isolation_level=None)
    fresh.row_factory = sqlite3.Row
    fresh.execute("PRAGMA foreign_keys = ON")
    migrations.run_migrations(fresh)
    assert fresh.execute("PRAGMA user_version").fetchone()[0] == 18

    upgraded = _v17_connect(tmp_path / "upgraded.db")
    migrations.run_migrations(upgraded)
    assert upgraded.execute("PRAGMA user_version").fetchone()[0] == 18

    assert _schema_names(fresh, "table") == _schema_names(upgraded, "table"), (
        "a fresh open and an upgraded v17 file must end with the same tables"
    )
    assert _schema_names(fresh, "index") == _schema_names(upgraded, "index"), (
        "a fresh open and an upgraded v17 file must end with the same indexes"
    )
    for table in _schema_names(fresh, "table"):
        assert _table_xinfo(fresh, table) == _table_xinfo(upgraded, table), (
            f"table {table!r} has a different column shape after the v18"
            " upgrade than a fresh build produces"
        )


# ---------------------------------------------------------------------------
# UT4: a second open of the upgraded file is a no-op for the upgrade.
# ---------------------------------------------------------------------------


def test_ut4_a_second_open_at_18_writes_no_v18_upgrade_line_and_tables_match(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Catches: the step running again once the file is already at 18 (the
    `current == UPGRADE_FROM_USER_VERSION` guard missing or wrong), which
    would log a second `v18 upgrade: secure_delete is ...` line and could
    change a table's rows on an open that should do nothing."""
    conn = _v17_connect(tmp_path / "once.db")
    migrations.run_migrations(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 18
    before = _all_table_rows(conn)

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 18
    assert not any("v18 upgrade:" in r.getMessage() for r in caplog.records), (
        "a second open at user_version 18 must not run the v18 upgrade again"
    )
    after = _all_table_rows(conn)
    assert before == after, "a second open at 18 must not change any table's rows"


# ---------------------------------------------------------------------------
# UT5: the version refusals, and a fresh file's target version.
# ---------------------------------------------------------------------------


def test_ut5_a_file_at_16_is_refused_and_left_unchanged(tmp_path: Path) -> None:
    """Catches: the file being half-upgraded before the age check runs (the
    refusal must come before `create_tables`, so nothing is written), and
    the old 0.16.15-era refusal text surviving ("install 0.16.15 first"
    instead of "install 0.16.17 first")."""
    conn = sqlite3.connect(str(tmp_path / "too-old.db"), isolation_level=None)
    conn.execute("PRAGMA user_version = 16")

    with pytest.raises(RuntimeError, match="install 0.16.17 first"):
        migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 16, (
        "a refused file's version must not move"
    )
    assert conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()[0] == 0, (
        "the refusal must come before create_tables writes anything"
    )


def test_ut5_a_file_at_19_is_refused_as_newer_than_this_app(tmp_path: Path) -> None:
    """Catches: the upper-bound refusal being dropped or its threshold left
    at the old `TARGET_USER_VERSION` (17) instead of moving to 18."""
    conn = sqlite3.connect(str(tmp_path / "too-new.db"), isolation_level=None)
    conn.execute("PRAGMA user_version = 19")

    with pytest.raises(RuntimeError, match="newer than this app"):
        migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 19
    assert conn.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()[0] == 0, (
        "the refusal must come before create_tables writes anything"
    )


def test_ut5_a_fresh_file_reads_18(tmp_path: Path) -> None:
    """Catches: `TARGET_USER_VERSION` left at 17 (or moved to the wrong
    number) for a brand-new file."""
    conn = sqlite3.connect(str(tmp_path / "fresh.db"), isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")

    migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 18


# ---------------------------------------------------------------------------
# UT6: secure_delete is restored for the process, upgrade or no upgrade.
# ---------------------------------------------------------------------------


def test_ut6_secure_delete_reads_1_after_a_good_upgrade(tmp_path: Path) -> None:
    """Catches: the setting left at 0 for the connection after a successful
    upgrade (the `finally` restore missing, or restoring the wrong value)."""
    conn = _v17_connect(tmp_path / "secure-delete-good.db")
    conn.execute("PRAGMA secure_delete = 1")

    migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 18
    assert conn.execute("PRAGMA secure_delete").fetchone()[0] == 1


def test_ut6_secure_delete_reads_1_after_a_stop_before_migrate_v18_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired negative for the test above: a stop raised at the
    `stop.begin_step("migrate_v18")` call, the `tests/test_stop.py` /
    6f49f1a's `tests/test_migration_v17.py::_patch_begin_step_to_stop_at` precedent,
    stops `run_migrations` before `migrate_v18` is ever entered, so
    `secure_delete` must never have been touched. Catches: `migrate_v18`'s
    `PRAGMA secure_delete = 0` toggle running unconditionally, ahead of (or
    without regard to) the `migrate_v18` step boundary -- a wiring bug that
    would leave the setting at 0 even though the step never completed."""
    real_begin_step = stop_module.begin_step

    def _fake_begin_step(name: str) -> None:
        if name == "migrate_v18":
            raise stop_module.StopRequested
        real_begin_step(name)

    monkeypatch.setattr(stop_module, "begin_step", _fake_begin_step)

    conn = _v17_connect(tmp_path / "secure-delete-stop.db")
    conn.execute("PRAGMA secure_delete = 1")

    with pytest.raises(stop_module.StopRequested):
        migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 17, (
        "a stop before migrate_v18 runs must leave the version at 17"
    )
    assert conn.execute("PRAGMA secure_delete").fetchone()[0] == 1


# ---------------------------------------------------------------------------
# U4's first wired-in sub-step: _v18_drop_unused_write_state.
# ---------------------------------------------------------------------------


def _score_cache_exists(conn: sqlite3.Connection) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='score_cache'"
        ).fetchone()
        is not None
    )


def _download_record_since_exists(conn: sqlite3.Connection) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM runtime_state WHERE key = 'download_record_since'"
        ).fetchone()
        is not None
    )


def _seed_score_cache_rows(conn: sqlite3.Connection) -> int:
    """Insert one made-up site and two made-up ``score_cache`` rows under
    boot-seeded feed 1. Returns the row count inserted (2)."""
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Site A', 1.0, 1.0, 1.0, 'UTC')"
    )
    site_id = conn.execute("SELECT id FROM sites WHERE name = 'Site A'").fetchone()[0]
    for day_ahead in (0, 1):
        conn.execute(
            "INSERT INTO score_cache (site_id, feed_id, variable, day_ahead,"
            " window_key, n, computed_at) VALUES (?, 1, 'temperature', ?,"
            " 'w:all', 1, '2026-01-01T00:00:00Z')",
            (site_id, day_ahead),
        )
    return 2


def _assert_sub_step_done(conn: sqlite3.Connection) -> None:
    """The shared, correct end state this sub-step must reach: at 18,
    `score_cache` gone, `download_record_since` gone -- UT2's own result,
    reused by UT3's "the next open finishes" checks."""
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 18
    assert not _score_cache_exists(conn)
    assert not _download_record_since_exists(conn)


def test_fresh_file_has_no_score_cache_and_no_download_record_since(
    tmp_path: Path,
) -> None:
    """Catches: the `score_cache` DDL left in `create_tables` (a fresh file
    never reaches `migrate_v18`, which only runs at version 17, so this is
    the only place that would catch it); the `download_record_since` key
    written again at a fresh build."""
    conn = sqlite3.connect(str(tmp_path / "fresh.db"), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 18
    assert not _score_cache_exists(conn)
    assert not _download_record_since_exists(conn)
    assert (
        conn.execute(
            "SELECT value FROM runtime_state WHERE key = 'daily_close_on_at'"
        ).fetchone()
        is not None
    )
    assert (
        conn.execute(
            "SELECT value FROM runtime_state WHERE key = 'storage_compacted_at'"
        ).fetchone()
        is not None
    )


def test_ut2_the_score_cache_table_and_only_its_key_go(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """UT2 for this sub-step. Catches: `_v18_drop_unused_write_state` not
    wired into `migrate_v18` (`score_cache` and the key would survive the
    upgrade); the sub-step reaching a row or table outside its own two
    statements (another `runtime_state` key going missing, or any other
    table's rows changing, would show up below)."""
    conn = _v17_connect(tmp_path / "ut2.db")
    inserted = _seed_score_cache_rows(conn)
    conn.commit()

    before_keys = {row[0] for row in conn.execute("SELECT key FROM runtime_state")}
    assert "download_record_since" in before_keys
    assert len(before_keys - {"download_record_since"}) >= 2, (
        "the v17 seed must carry download_record_since beside at least two"
        " other runtime_state keys, or this oracle cannot tell a sub-step"
        " that deletes more than its own key from one that does not"
    )
    before = _all_table_rows(conn)
    before_sets = {table: set(rows) for table, rows in before.items()}
    removed_row = next(
        row for row in before["runtime_state"] if row[0] == "download_record_since"
    )

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        migrations.run_migrations(conn)

    _assert_sub_step_done(conn)
    after = _all_table_rows(conn)
    after_sets = {table: set(rows) for table, rows in after.items()}

    assert set(after_sets) == set(before_sets) - {"score_cache"}, (
        "exactly score_cache must disappear from the schema"
    )
    assert after_sets["runtime_state"] == before_sets["runtime_state"] - {
        removed_row
    }, "exactly the download_record_since row must disappear from runtime_state"
    for table, rows in after_sets.items():
        if table == "runtime_state":
            continue
        assert rows == before_sets[table], (
            f"table {table!r} changed, but this sub-step touches only"
            " score_cache and runtime_state"
        )

    assert any(
        f"score_cache dropped=1 ({inserted} row(s))" in r.getMessage()
        and "download_record_since removed=1" in r.getMessage()
        for r in caplog.records
    ), "the INFO line must report the real counts"


def test_ut3_a_stop_at_the_sub_step_leaves_everything_as_it_was(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """UT3 for this sub-step, the stop half. Catches: the sub-step doing
    anything -- an `executescript`, an early commit -- before the
    `stop.begin_step("v18_drop_unused_write_state")` check, which would
    leave `score_cache` or the key gone even though `run_migrations` never
    finished."""
    real_begin_step = stop_module.begin_step

    def _fake_begin_step(name: str) -> None:
        if name == "v18_drop_unused_write_state":
            raise stop_module.StopRequested
        real_begin_step(name)

    monkeypatch.setattr(stop_module, "begin_step", _fake_begin_step)

    conn = _v17_connect(tmp_path / "ut3-stop.db")
    inserted = _seed_score_cache_rows(conn)
    conn.execute("PRAGMA secure_delete = 1")
    conn.commit()
    before = _all_table_rows(conn)

    with pytest.raises(stop_module.StopRequested):
        migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 17, (
        "a stop before the sub-step runs must leave the version at 17"
    )
    assert _score_cache_exists(conn)
    assert conn.execute("SELECT COUNT(*) FROM score_cache").fetchone()[0] == inserted
    assert _download_record_since_exists(conn)
    assert conn.execute("PRAGMA secure_delete").fetchone()[0] == 1
    assert _all_table_rows(conn) == before

    monkeypatch.setattr(stop_module, "begin_step", real_begin_step)
    migrations.run_migrations(conn)
    _assert_sub_step_done(conn)


def test_ut3_a_denied_runtime_state_delete_rolls_back_the_drop_too(
    tmp_path: Path,
) -> None:
    """UT3 for this sub-step, the all-or-nothing half. Catches: the
    sub-step's `DROP TABLE` and `DELETE` not sharing one atomic unit with
    `migrate_v18`'s `SAVEPOINT` -- if the drop had already committed on its
    own before the delete failed, `score_cache` would stay gone even though
    the upgrade as a whole raised and the version stayed at 17.

    The authorizer denies the SECOND `DELETE FROM runtime_state` statement
    `run_migrations` issues, by call order: the first is
    `drop_forecast_record`'s (every start, well before `migrate_v18` is
    even entered); the second is this sub-step's own. A `delete_calls`
    count of exactly 2 at the point of the raise is the fixture's own
    precondition check that the denial landed where intended, not on
    `drop_forecast_record`'s unrelated delete. `order` additionally records
    whether `score_cache`'s `DROP TABLE` was seen before the denied DELETE,
    not merely that exactly two DELETEs ran -- so a later sub-step that adds
    a `runtime_state` DELETE earlier in the order (ahead of the DROP) fails
    this test loudly instead of silently denying the wrong statement.
    """
    conn = _v17_connect(tmp_path / "ut3-deny.db")
    inserted = _seed_score_cache_rows(conn)
    conn.execute("PRAGMA secure_delete = 1")
    conn.commit()
    before = _all_table_rows(conn)

    delete_calls = {"n": 0}
    order: list[str] = []

    def _authorizer(
        action_code: int,
        arg1: str | None,
        arg2: str | None,
        dbname: str | None,
        source: str | None,
    ) -> int:
        if action_code == sqlite3.SQLITE_DROP_TABLE and arg1 == "score_cache":
            order.append("drop_score_cache")
        if action_code == sqlite3.SQLITE_DELETE and arg1 == "runtime_state":
            delete_calls["n"] += 1
            if delete_calls["n"] == 2:
                order.append("denied_delete")
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    conn.set_authorizer(_authorizer)
    try:
        with pytest.raises(sqlite3.DatabaseError):
            migrations.run_migrations(conn)
    finally:
        conn.set_authorizer(None)

    assert delete_calls["n"] == 2, (
        "fixture bug: the denial must land on the sub-step's own DELETE,"
        " not drop_forecast_record's -- otherwise the DROP TABLE this"
        " oracle depends on never ran"
    )
    assert "drop_score_cache" in order and "denied_delete" in order, (
        "fixture bug: expected both the score_cache DROP TABLE and the"
        f" denied runtime_state DELETE to fire -- got order={order!r}"
    )
    assert order.index("drop_score_cache") < order.index("denied_delete"), (
        "fixture bug: score_cache's DROP TABLE must be authorized before"
        " the denied DELETE, so this oracle actually exercises the"
        " all-or-nothing rollback of a drop that already ran -- got"
        f" order={order!r}"
    )
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 17
    assert _score_cache_exists(conn), (
        "a sub-step committing its DROP TABLE ahead of the failed DELETE"
        " would leave score_cache gone here"
    )
    assert conn.execute("SELECT COUNT(*) FROM score_cache").fetchone()[0] == inserted
    assert _download_record_since_exists(conn)
    assert conn.execute("PRAGMA secure_delete").fetchone()[0] == 1
    assert _all_table_rows(conn) == before

    migrations.run_migrations(conn)
    _assert_sub_step_done(conn)
