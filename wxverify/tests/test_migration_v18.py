"""U1-U3, the version-18 upgrade's shape only (plan `2026-11-07-health-
release.md` §9, §10 step 1; test IDs UT1, UT4, UT5, UT6, adapted to this
step). `migrate_v18` at this step holds only the ``secure_delete`` handling
and the ``PRAGMA user_version = 18`` write inside its ``SAVEPOINT``; the
four part sub-steps (U4: ``_v18_poll_pause``, ``_v18_drop_unused_write_
state``, ``_v18_prune_wind_history``, ``_v18_drop_bad_display_values``) are
not wired in yet, so UT2 (each part's effect) and UT3 (all-or-nothing
across the four sub-steps, BT9's ``CHECK`` table in UT1) are not written
here -- they belong to the later steps that add those sub-steps.

Fixture: ``tests/fixtures/v17_schema.sql`` and ``v17_seed.sql``, dumped
from a fresh 0.16.17 (``6f49f1a``) file the same way the version-16 pair
was (6f49f1a's ``tests/test_migration_v17.py:14-25``: schema from ``sqlite_master``,
seed with ``iterdump``, restricted to the boot-seed tables -- ``sources``,
``feeds``, ``settings``, ``runtime_state``; no site, station or job row).
Every test below loads both into a fresh ``tmp_path`` file, sets
``user_version`` 17 and ``foreign_keys`` on, and calls
``migrations.run_migrations`` directly on the autocommit connection (the
6f49f1a's ``tests/test_migration_v17.py``/``_run_migrate_v17`` convention: no
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
