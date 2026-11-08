"""The version-18 upgrade (plan `2026-11-07-health-release.md` §9, §10;
test IDs UT1-UT6, BT9, H5). The version 18 upgrade runs under
`migrate_v18`. Of U4's four sub-steps (``_v18_poll_pause``,
``_v18_drop_unused_write_state``, ``_v18_prune_wind_history``,
``_v18_drop_bad_display_values``), all four are now wired in:
``_v18_poll_pause`` (Part B), ``_v18_drop_unused_write_state`` (Part D),
``_v18_prune_wind_history`` (Part G) and ``_v18_drop_bad_display_values``
(Part H, H5). UT1 also checks BT9's ``CHECK`` table (the two poll-pause
columns) on a fresh file and on an upgraded one, and BT9's own carry-over
test lives here too. UT2 and UT3 below cover each sub-step in turn.

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
``tmp_path``; no mocked persistence. Synthetic data only: the fixture
itself is an empty, pre-seeded shell, and every site/station a test adds
on top of it (BT9's carry-over test) uses an obviously made-up id
(``<A>``-style placeholders), never a real identifier.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

import pytest

from wxverify.core import stop as stop_module
from wxverify.core.timeutil import isoformat_utc
from wxverify.db import migrations
from wxverify.provider_ops import bad_sample_count_sql

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


def _with_empty_station_wind_constant(
    rows: dict[str, list[tuple[object, ...]]],
) -> dict[str, list[tuple[object, ...]]]:
    """``rows`` plus an empty ``station_wind_constant`` table.

    ``create_tables`` makes ``station_wind_constant`` (Part A, §1 A2) on
    every open, unconditionally and ahead of the version-17 savepoint, so
    even a run that stops or is denied before `migrate_v18` does anything
    still leaves this one empty table behind on a v17 file that never had
    it. A before/after row comparison taken around such a run must expect
    exactly this one addition, nothing else -- any other difference still
    fails.
    """
    return {**rows, "station_wind_constant": []}


# ---------------------------------------------------------------------------
# UT1 (adapted): a fresh file and an upgraded v17 file produce the same
# table and index names, and the same table_xinfo for every table. BT9's
# CHECK-table part is not written here -- it lands with step 3 (B).
# ---------------------------------------------------------------------------


def _insert_bare_station(conn: sqlite3.Connection, pws_id: str) -> int:
    """One made-up site and one enabled station, for a bare CHECK-constraint
    probe: no other column of either row is read by that probe."""
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES (?, 1.0, 1.0, 1.0, 'UTC')",
        (f"UT1 CHECK site {pws_id}",),
    )
    site_id = int(cur.lastrowid)
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)"
        " VALUES (?, ?, 1.0, 1.0, 1.0)",
        (site_id, pws_id),
    )
    return int(cur.lastrowid)


def _insert_pause_state(
    conn: sqlite3.Connection, station_id: int, *, count: int, reason: str | None
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state (station_id, pause_evidence_count,"
        " pause_reason) VALUES (?, ?, ?)",
        (station_id, count, reason),
    )


def _assert_bt9_check_table(conn: sqlite3.Connection, pws_prefix: str) -> None:
    """BT9's ``CHECK`` table: ``(0, NULL)`` and ``(1, 'offline')`` accepted;
    ``(1, NULL)``, ``(0, 'offline')``, ``(-1, None)``, ``(-1, 'offline')``
    and reason ``'refused'`` all refused. ``(-1, 'offline')`` is the only
    case that isolates the ``>= 0`` clause: ``(-1, None)`` is already
    refused by the equality clause alone (count and reason must agree on
    "paused" or not), so without the extra case a ``>= 0`` bound dropped
    from the ``CHECK`` would pass unnoticed."""
    accepted = ((0, None), (1, "offline"))
    refused = (
        (1, None),
        (0, "offline"),
        (-1, None),
        (-1, "offline"),
        (1, "refused"),
    )
    for i, (count, reason) in enumerate(accepted):
        station_id = _insert_bare_station(conn, f"{pws_prefix}-OK{i}")
        _insert_pause_state(conn, station_id, count=count, reason=reason)
    for i, (count, reason) in enumerate(refused):
        station_id = _insert_bare_station(conn, f"{pws_prefix}-BAD{i}")
        with pytest.raises(sqlite3.IntegrityError):
            _insert_pause_state(conn, station_id, count=count, reason=reason)


def test_ut1_fresh_and_upgraded_schema_have_the_same_names_and_table_xinfo(
    tmp_path: Path,
) -> None:
    """Catches: a table or index the upgrade leaves behind that a fresh file
    does not have (or vice versa); a column the upgrade adds in a different
    order, type or default than a fresh build's `create_tables` -- either
    would show up as a `table_xinfo` mismatch for the affected table; BT9's
    ``CHECK`` table accepted and refused differently on a fresh file than on
    an upgraded one (a column written in a weaker form by one path)."""
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

    _assert_bt9_check_table(fresh, "FRESH")
    _assert_bt9_check_table(upgraded, "UPGRADED")


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


def _has_pause_columns(conn: sqlite3.Connection) -> bool:
    names = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(station_poll_state)").fetchall()
    }
    return "pause_reason" in names and "pause_evidence_count" in names


def _assert_sub_step_done(conn: sqlite3.Connection) -> None:
    """Checks the full ``migrate_v18`` end state: ``user_version`` at 18,
    the poll-pause columns present, `score_cache` gone, and
    `download_record_since` gone. It does not check the wind-history
    prune or the display-values drop -- those are checked separately by
    each test that needs them."""
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 18
    assert _has_pause_columns(conn)
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
    table's rows changing, would show up below). `run_migrations` also runs
    `delete_pre_utc_meteosource` on every call and that step writes its own
    `meteosource_utc_fix_at` key (0.16.20); that new row is matched by key
    below, since its value and `updated_at` are stamps, not compared as an
    exact tuple."""
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

    assert set(after_sets) == (
        set(before_sets) - {"score_cache"} | {"station_wind_constant"}
    ), (
        "exactly score_cache must disappear from the schema, and the empty"
        " station_wind_constant table create_tables always makes must appear"
    )
    after_runtime_state = after_sets["runtime_state"]
    fix_rows = {
        row for row in after_runtime_state if row[0] == "meteosource_utc_fix_at"
    }
    assert len(fix_rows) == 1, (
        "delete_pre_utc_meteosource must write exactly one"
        " meteosource_utc_fix_at row on this (fresh-to-it) v17 file"
    )
    assert after_runtime_state - fix_rows == before_sets["runtime_state"] - {
        removed_row
    }, (
        "besides the new meteosource_utc_fix_at row, exactly the"
        " download_record_since row must disappear from runtime_state"
    )
    assert after_sets["station_wind_constant"] == set(), (
        "station_wind_constant must be empty -- this sub-step seeds no episode"
    )
    for table, rows in after_sets.items():
        if table in ("runtime_state", "station_wind_constant"):
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
    assert not _has_pause_columns(conn), (
        "a stop at v18_drop_unused_write_state runs after v18_poll_pause, so"
        " this would only fail if the poll-pause ALTER TABLEs committed on"
        " their own instead of rolling back with the rest of the savepoint"
    )
    assert conn.execute("PRAGMA secure_delete").fetchone()[0] == 1
    assert _all_table_rows(conn) == _with_empty_station_wind_constant(before)

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
    assert not _has_pause_columns(conn), (
        "v18_poll_pause's ALTER TABLEs run inside the same savepoint, ahead"
        " of the denied DELETE; if they had committed on their own the"
        " columns would survive this rollback"
    )
    assert conn.execute("PRAGMA secure_delete").fetchone()[0] == 1
    assert _all_table_rows(conn) == _with_empty_station_wind_constant(before)

    migrations.run_migrations(conn)
    _assert_sub_step_done(conn)


# ---------------------------------------------------------------------------
# U4's second wired-in sub-step: _v18_poll_pause (BT9, Part B).
# ---------------------------------------------------------------------------


def _seed_bare_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES (?, 1.0, 1.0, 1.0, 'UTC')",
        (name,),
    )
    return int(cur.lastrowid)


def _seed_bare_station(
    conn: sqlite3.Connection,
    site_id: int,
    pws_id: str,
    *,
    health_state: str,
    next_poll_at: str | None,
    last_error: str | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)"
        " VALUES (?, ?, 1.0, 1.0, 1.0)",
        (site_id, pws_id),
    )
    station_id = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO station_poll_state (station_id, health_state, next_poll_at,"
        " last_error) VALUES (?, ?, ?, ?)",
        (station_id, health_state, next_poll_at, last_error),
    )
    return station_id


def test_bt9_carry_over_recheck_for_offline_and_404_stations(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BT9's carry-over, on the v17 fixture file (plan §2's Upgrade needs,
    U4's ``_v18_poll_pause``). Rows: ``offline`` (``next_poll_at`` 20 h
    ahead), ``terminal`` ``http 404`` (``next_poll_at`` unset), ``terminal``
    ``http 410``, ``terminal`` ``http 401``, ``online`` -- the last three
    keep a far-future ``next_poll_at`` that must survive untouched. Catches:
    a 404 station parked for a day instead of rechecked at once (TODO
    step 2); every 4xx counted as evidence, not just ``offline``/404; the
    carry-over reaching the ``online`` row; the INFO line's counts wrong.
    """
    fixed_now = datetime(2026, 11, 7, 6, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(migrations, "utc_now", lambda: fixed_now)
    now_iso = fixed_now.isoformat().replace("+00:00", "Z")
    far_future = (fixed_now + timedelta(hours=20)).isoformat().replace("+00:00", "Z")

    conn = _v17_connect(tmp_path / "bt9-carry-over.db")
    site_id = _seed_bare_site(conn, "BT9 carry-over site")
    station_offline = _seed_bare_station(
        conn, site_id, "<A>", health_state="offline", next_poll_at=far_future
    )
    station_404 = _seed_bare_station(
        conn,
        site_id,
        "<B>",
        health_state="terminal",
        next_poll_at=None,
        last_error="http 404",
    )
    station_410 = _seed_bare_station(
        conn,
        site_id,
        "<C>",
        health_state="terminal",
        next_poll_at=far_future,
        last_error="http 410",
    )
    station_401 = _seed_bare_station(
        conn,
        site_id,
        "<D>",
        health_state="terminal",
        next_poll_at=far_future,
        last_error="http 401",
    )
    station_online = _seed_bare_station(
        conn, site_id, "<E>", health_state="online", next_poll_at=far_future
    )
    conn.commit()

    def _row(station_id: int) -> sqlite3.Row:
        row = conn.execute(
            "SELECT pause_evidence_count, pause_reason, next_poll_at,"
            " health_state, last_error FROM station_poll_state"
            " WHERE station_id = ?",
            (station_id,),
        ).fetchone()
        assert row is not None
        return row

    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 18

    offline_row = _row(station_offline)
    assert (offline_row["pause_evidence_count"], offline_row["pause_reason"]) == (
        1,
        "offline",
    ), "an offline row must become one evidence reply, reason offline"
    assert offline_row["next_poll_at"] == now_iso, (
        "an offline station parked for a day must be due at once, not at"
        " the end of its 20-h-ahead park"
    )

    row_404 = _row(station_404)
    assert (row_404["pause_evidence_count"], row_404["pause_reason"]) == (
        1,
        "not_found",
    ), "a terminal http 404 row must become one evidence reply, reason not_found"
    assert row_404["next_poll_at"] == now_iso

    for station_id, expected_error in (
        (station_410, "http 410"),
        (station_401, "http 401"),
    ):
        row = _row(station_id)
        assert (row["pause_evidence_count"], row["pause_reason"]) == (0, None), (
            f"a terminal row with error {expected_error!r} must not be"
            " swept into the carry-over"
        )
        assert row["next_poll_at"] == far_future, (
            f"a terminal row with error {expected_error!r} must keep its"
            " own next_poll_at untouched"
        )
        assert row["last_error"] == expected_error

    online_row = _row(station_online)
    assert (online_row["pause_evidence_count"], online_row["pause_reason"]) == (
        0,
        None,
    ), "an online row must not be touched by the carry-over"
    assert online_row["next_poll_at"] == far_future

    assert any(
        "2 station(s) set to recheck" in r.getMessage() and "added=2" in r.getMessage()
        for r in caplog.records
    ), "the INFO line must report 2 columns added and 2 stations rechecked"


def test_ut3_a_stop_at_poll_pause_leaves_everything_as_it_was(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """UT3 for the ``v18_poll_pause`` sub-step. Catches: the two ALTER
    TABLEs or the carry-over UPDATE running ahead of the
    ``stop.begin_step("v18_poll_pause")`` check, which would leave the two
    pause columns (and a rechecked station) on the file even though
    `run_migrations` never finished -- this sub-step runs FIRST in
    `migrate_v18`, so a stop here must leave every one of its later
    sub-steps' effects absent too, which `_all_table_rows` catches."""
    real_begin_step = stop_module.begin_step

    def _fake_begin_step(name: str) -> None:
        if name == "v18_poll_pause":
            raise stop_module.StopRequested
        real_begin_step(name)

    monkeypatch.setattr(stop_module, "begin_step", _fake_begin_step)

    conn = _v17_connect(tmp_path / "ut3-stop-poll-pause.db")
    conn.execute("PRAGMA secure_delete = 1")
    conn.commit()
    before = _all_table_rows(conn)

    with pytest.raises(stop_module.StopRequested):
        migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 17, (
        "a stop before the poll-pause sub-step runs must leave the version at 17"
    )
    assert not _has_pause_columns(conn)
    assert conn.execute("PRAGMA secure_delete").fetchone()[0] == 1
    assert _all_table_rows(conn) == _with_empty_station_wind_constant(before)

    monkeypatch.setattr(stop_module, "begin_step", real_begin_step)
    migrations.run_migrations(conn)
    _assert_sub_step_done(conn)


# ---------------------------------------------------------------------------
# U4's third wired-in sub-step: _v18_prune_wind_history (plan §7 Part G).
# ---------------------------------------------------------------------------

_UT2_WIND_MARGIN_S: Final = 1200  # MAX_PAIR_GAP_SECONDS (obs/wind_pairs.py)


def _insert_bare_wind_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES (?, 1.0, 1.0, 1.0, 'UTC')",
        (name,),
    )
    return int(cur.lastrowid)


def _insert_bare_wind_station(
    conn: sqlite3.Connection, site_id: int, pws_id: str
) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)"
        " VALUES (?, ?, 1.0, 1.0, 1.0)",
        (site_id, pws_id),
    )
    return int(cur.lastrowid)


def _insert_bare_site_days_record(
    conn: sqlite3.Connection,
    site_id: int,
    local_date: str,
    day_start_utc: str,
    day_end_utc: str,
    *,
    done: bool,
) -> None:
    """One ``site_days`` row, open or closed, with every CHECK-required
    column filled in directly (no ``start_site_days``/``begin_close``/
    ``finish_close`` call -- those live in ``wxverify.db.site_days``, which
    this migration-only fixture does not otherwise need)."""
    conn.execute(
        "INSERT INTO site_days (site_id, local_date, state, day_start_utc,"
        " day_end_utc, timezone, zone_changeover, attempts, created_at,"
        " state_changed_at, done_at) VALUES (?, ?, ?, ?, ?, 'UTC', 0, ?, ?, ?, ?)",
        (
            site_id,
            local_date,
            "closed" if done else "open",
            day_start_utc,
            day_end_utc,
            1 if done else 0,
            day_start_utc,
            day_start_utc,
            day_start_utc if done else None,
        ),
    )


def _insert_wind_history_row(
    conn: sqlite3.Connection,
    station_id: int,
    obs_at: datetime,
    *,
    fetched_at: datetime | None,
) -> None:
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, local_date, speed_kmh, fetched_at)"
        " VALUES (?, ?, 'five_minute', ?, 5.0, ?)",
        (
            station_id,
            isoformat_utc(obs_at),
            obs_at.date().isoformat(),
            None if fetched_at is None else isoformat_utc(fetched_at),
        ),
    )


def _wind_history_row_exists(
    conn: sqlite3.Connection, station_id: int, obs_at: datetime
) -> bool:
    row = conn.execute(
        "SELECT 1 FROM station_wind_records WHERE station_id = ? AND obs_at = ?",
        (station_id, isoformat_utc(obs_at)),
    ).fetchone()
    return row is not None


def test_ut2_prune_wind_history_deletes_only_the_done_sites_old_null_rows(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """UT2 for the ``v18_prune_wind_history`` sub-step (plan §7 G1; §9 UT2).

    One made-up site whose first ``site_days`` record is closed (done), one
    whose first record is open (not done); each with one station and four
    ``station_wind_records`` rows (old/boundary/near/``fetched_at``-set).
    Catches: `_v18_prune_wind_history` not wired into `migrate_v18` (the old
    rows would survive the upgrade untouched); the delete reaching the
    not-done site's rows (its rows, and the ``kept_sites`` id, would then be
    wrong); the margin bound off by one second (the boundary row, exactly
    F's start minus 1200 s, would be deleted instead of kept); the sub-step
    reaching a row outside ``station_wind_records``.
    """
    conn = _v17_connect(tmp_path / "ut2-wind-prune.db")

    f_start = datetime(2026, 11, 7, tzinfo=UTC)
    f_end = f_start + timedelta(days=1)

    done_site = _insert_bare_wind_site(conn, "UT2 wind-prune done site")
    done_station = _insert_bare_wind_station(conn, done_site, "<WPA>")
    _insert_bare_site_days_record(
        conn,
        done_site,
        f_start.date().isoformat(),
        isoformat_utc(f_start),
        isoformat_utc(f_end),
        done=True,
    )

    open_site = _insert_bare_wind_site(conn, "UT2 wind-prune open site")
    open_station = _insert_bare_wind_station(conn, open_site, "<WPB>")
    _insert_bare_site_days_record(
        conn,
        open_site,
        f_start.date().isoformat(),
        isoformat_utc(f_start),
        isoformat_utc(f_end),
        done=False,
    )

    old = f_start - timedelta(seconds=_UT2_WIND_MARGIN_S + 800)
    boundary = f_start - timedelta(seconds=_UT2_WIND_MARGIN_S)
    near = f_start - timedelta(seconds=600)
    fetched_set_at = f_start - timedelta(seconds=5000)

    for station_id in (done_station, open_station):
        _insert_wind_history_row(conn, station_id, old, fetched_at=None)
        _insert_wind_history_row(conn, station_id, boundary, fetched_at=None)
        _insert_wind_history_row(conn, station_id, near, fetched_at=None)
        _insert_wind_history_row(
            conn, station_id, fetched_set_at, fetched_at=fetched_set_at
        )
    conn.commit()

    excluded = {"station_wind_records", "score_cache", "runtime_state"}
    before = {
        table: rows
        for table, rows in _all_table_rows(conn).items()
        if table not in excluded
    }

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        migrations.run_migrations(conn)

    _assert_sub_step_done(conn)

    assert not _wind_history_row_exists(conn, done_station, old)
    assert _wind_history_row_exists(conn, done_station, boundary)
    assert _wind_history_row_exists(conn, done_station, near)
    assert _wind_history_row_exists(conn, done_station, fetched_set_at)

    assert _wind_history_row_exists(conn, open_station, old)
    assert _wind_history_row_exists(conn, open_station, boundary)
    assert _wind_history_row_exists(conn, open_station, near)
    assert _wind_history_row_exists(conn, open_station, fetched_set_at)

    after = {
        table: rows
        for table, rows in _all_table_rows(conn).items()
        if table not in excluded
    }
    assert after == _with_empty_station_wind_constant(before), (
        "the prune must not touch any table besides station_wind_records,"
        " beyond the empty station_wind_constant table create_tables always makes"
    )

    assert any(
        "wind history prune: 1 row(s) deleted" in r.getMessage()
        and f"kept at sites [{open_site}]" in r.getMessage()
        for r in caplog.records
    ), "the INFO line must report the deleted count and the one kept site id"


def test_ut3_a_stop_at_prune_wind_history_leaves_everything_as_it_was(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """UT3 for the ``v18_prune_wind_history`` sub-step, the stop half. This
    stop restores the old row either way, so it cannot catch a ``DELETE``
    running ahead of the ``stop.begin_step("v18_prune_wind_history")``
    check. Catches: the earlier sub-steps (poll-pause,
    drop-unused-write-state) committing on their own instead of rolling
    back with this one's stop; ``secure_delete`` not restored; the call
    itself replaced with ``pass`` -- the "next open, with no fault" check
    below is what would then still show the old row."""
    real_begin_step = stop_module.begin_step

    def _fake_begin_step(name: str) -> None:
        if name == "v18_prune_wind_history":
            raise stop_module.StopRequested
        real_begin_step(name)

    monkeypatch.setattr(stop_module, "begin_step", _fake_begin_step)

    conn = _v17_connect(tmp_path / "ut3-stop-wind-prune.db")
    f_start = datetime(2026, 11, 7, tzinfo=UTC)
    f_end = f_start + timedelta(days=1)
    done_site = _insert_bare_wind_site(conn, "UT3 wind-prune stop site")
    done_station = _insert_bare_wind_station(conn, done_site, "<WPC>")
    _insert_bare_site_days_record(
        conn,
        done_site,
        f_start.date().isoformat(),
        isoformat_utc(f_start),
        isoformat_utc(f_end),
        done=True,
    )
    old = f_start - timedelta(seconds=_UT2_WIND_MARGIN_S + 800)
    _insert_wind_history_row(conn, done_station, old, fetched_at=None)
    conn.execute("PRAGMA secure_delete = 1")
    conn.commit()
    before = _all_table_rows(conn)

    with pytest.raises(stop_module.StopRequested):
        migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 17, (
        "a stop before the wind-history-prune sub-step runs must leave the"
        " version at 17"
    )
    assert _wind_history_row_exists(conn, done_station, old), (
        "the old row must survive a stop before this sub-step runs"
    )
    assert conn.execute("PRAGMA secure_delete").fetchone()[0] == 1
    assert _all_table_rows(conn) == _with_empty_station_wind_constant(before), (
        "a stop at v18_prune_wind_history runs after v18_poll_pause and"
        " v18_drop_unused_write_state, so this also fails if either of"
        " those committed on its own instead of rolling back with the rest"
        " of the savepoint"
    )

    monkeypatch.setattr(stop_module, "begin_step", real_begin_step)
    migrations.run_migrations(conn)
    _assert_sub_step_done(conn)
    assert not _wind_history_row_exists(conn, done_station, old), (
        "the next open, with no fault, must finish the sub-step it stopped short of"
    )


# ---------------------------------------------------------------------------
# U4's fourth wired-in sub-step: _v18_drop_bad_display_values (plan §8 Part
# H; §9 H5). feed_id 1 and 2 are the boot-seed's own (`feeds` rows from
# `v17_seed.sql`), never created by these tests.
# ---------------------------------------------------------------------------


def _insert_forecast_sample(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    variable: str,
    value: float,
    valid_at: str,
    *,
    issued_at: str = "2026-11-07T00:00:00Z",
    lead_hours: int = 6,
) -> int:
    cur = conn.execute(
        "INSERT INTO forecast_samples (site_id, feed_id, variable, issued_at,"
        " valid_at, lead_hours, value, source_raw, model_run_id)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-1')",
        (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value),
    )
    return int(cur.lastrowid)


def test_h5_drop_bad_display_values_deletes_only_the_out_of_range_rows(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """H5 / UT2 for the ``v18_drop_bad_display_values`` sub-step (plan §8
    Part H; §9 H5). Feed X (boot-seeded feed 1) carries display rows at
    eight distinct hours: cloud cover -4, 0, 100, 106, wind direction 361,
    360, wind gust -1, 0, and one in-range cloud cover (50) with a
    malformed ``valid_at``. Feed Y (boot-seeded feed 2) carries one scored
    temperature 75 (out of ``FORECAST_VALUE_RANGES``). Catches: a delete
    reaching a scored-roster row (the temperature 75 row would disappear);
    a predicate built from cloud cover alone (the 361/360 direction rows
    would survive untouched instead of dropping the 361 one); the
    malformed-stamp row deleted though it is not out of range
    (``out_of_range_display_sql`` must gate the delete, not
    ``invalid_stored_sample_sql`` alone -- the statement running at all
    also proves ``INDEXED BY idx_samples_invalid`` is usable, since SQLite
    refuses to prepare an ``INDEXED BY`` it cannot use). Under the AND,
    the boundary rows (0/100/360) fail ``invalid_stored_sample_sql``, so a
    ``<=`` slip in ``out_of_range_display_sql`` cannot be seen here;
    ``test_out_of_range_display_sql_boundaries_and_non_display_rows`` (in
    ``test_display_rosters.py``) pins it.
    """
    conn = _v17_connect(tmp_path / "h5-drop-bad-display.db")
    site_id = _seed_bare_site(conn, "H5 site")
    feed_x, feed_y = 1, 2

    hours = [f"2026-11-07T0{h}:00:00Z" for h in range(8)]
    id_cloud_low = _insert_forecast_sample(
        conn, site_id, feed_x, "cloud_cover", -4.0, hours[0]
    )
    _insert_forecast_sample(conn, site_id, feed_x, "cloud_cover", 0.0, hours[1])
    _insert_forecast_sample(conn, site_id, feed_x, "cloud_cover", 100.0, hours[2])
    id_cloud_high = _insert_forecast_sample(
        conn, site_id, feed_x, "cloud_cover", 106.0, hours[3]
    )
    id_direction_high = _insert_forecast_sample(
        conn, site_id, feed_x, "wind_direction", 361.0, hours[4]
    )
    _insert_forecast_sample(conn, site_id, feed_x, "wind_direction", 360.0, hours[5])
    id_gust_low = _insert_forecast_sample(
        conn, site_id, feed_x, "wind_gust", -1.0, hours[6]
    )
    _insert_forecast_sample(conn, site_id, feed_x, "wind_gust", 0.0, hours[7])
    _insert_forecast_sample(
        conn, site_id, feed_x, "cloud_cover", 50.0, "2026-11-07T08:00:00"
    )
    _insert_forecast_sample(
        conn, site_id, feed_y, "temperature", 75.0, "2026-11-07T09:00:00Z"
    )
    conn.commit()

    before = {
        int(row["id"]): tuple(row)
        for row in conn.execute("SELECT * FROM forecast_samples ORDER BY id")
    }

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        migrations.run_migrations(conn)

    _assert_sub_step_done(conn)

    after = {
        int(row["id"]): tuple(row)
        for row in conn.execute("SELECT * FROM forecast_samples ORDER BY id")
    }
    deleted_ids = set(before) - set(after)
    assert deleted_ids == {
        id_cloud_low,
        id_cloud_high,
        id_direction_high,
        id_gust_low,
    }, f"expected exactly the four out-of-range rows deleted, got {deleted_ids!r}"
    for row_id, row in after.items():
        assert row == before[row_id], (
            f"row id={row_id} changed though it must survive untouched"
        )

    sql = bad_sample_count_sql("?")
    count_x = conn.execute(sql, (site_id, feed_x)).fetchone()["bad_sample_count"]
    count_y = conn.execute(sql, (site_id, feed_y)).fetchone()["bad_sample_count"]
    assert int(count_x) == 1, (
        "only the malformed-stamp row must still read as bad for feed X"
    )
    assert int(count_y) == 1, (
        "the scored out-of-range row must still read as bad for feed Y"
    )

    assert any(
        "4 out-of-range display value(s) deleted" in r.getMessage()
        for r in caplog.records
    ), "the INFO line must report the deleted count"

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.db.migrations"):
        migrations.run_migrations(conn)
    assert conn.execute("SELECT COUNT(*) FROM forecast_samples").fetchone()[0] == len(
        after
    ), "a second open must not delete anything else"
    assert not any(
        "out-of-range display value(s) deleted" in r.getMessage()
        for r in caplog.records
    ), "a second open at 18 must not run the v18 upgrade again"


def test_ut3_a_stop_at_drop_bad_display_values_leaves_everything_as_it_was(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """UT3 for the ``v18_drop_bad_display_values`` sub-step, the stop half.
    Catches: any of the three earlier sub-steps (poll-pause,
    drop-unused-write-state, wind-history prune) committing on their own
    instead of rolling back with this one's stop -- this sub-step runs
    LAST in ``migrate_v18``, so a stop here must leave every one of the
    earlier sub-steps' effects absent too, which ``_all_table_rows``
    catches."""
    real_begin_step = stop_module.begin_step

    def _fake_begin_step(name: str) -> None:
        if name == "v18_drop_bad_display_values":
            raise stop_module.StopRequested
        real_begin_step(name)

    monkeypatch.setattr(stop_module, "begin_step", _fake_begin_step)

    conn = _v17_connect(tmp_path / "ut3-stop-drop-bad-display.db")
    site_id = _seed_bare_site(conn, "UT3 drop-bad-display stop site")
    out_of_range_id = _insert_forecast_sample(
        conn, site_id, 1, "cloud_cover", 106.0, "2026-11-07T06:00:00Z"
    )
    inserted = _seed_score_cache_rows(conn)

    f_start = datetime(2026, 11, 7, tzinfo=UTC)
    f_end = f_start + timedelta(days=1)
    wind_site = _insert_bare_wind_site(conn, "UT3 drop-bad-display wind site")
    wind_station = _insert_bare_wind_station(conn, wind_site, "<WPDBD>")
    _insert_bare_site_days_record(
        conn,
        wind_site,
        f_start.date().isoformat(),
        isoformat_utc(f_start),
        isoformat_utc(f_end),
        done=True,
    )
    old_wind_at = f_start - timedelta(seconds=_UT2_WIND_MARGIN_S + 800)
    _insert_wind_history_row(conn, wind_station, old_wind_at, fetched_at=None)

    conn.execute("PRAGMA secure_delete = 1")
    conn.commit()
    before = _all_table_rows(conn)

    with pytest.raises(stop_module.StopRequested):
        migrations.run_migrations(conn)

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 17, (
        "a stop before the drop-bad-display-values sub-step runs must leave"
        " the version at 17"
    )
    assert (
        conn.execute(
            "SELECT 1 FROM forecast_samples WHERE id = ?", (out_of_range_id,)
        ).fetchone()
        is not None
    ), "the out-of-range row must survive a stop before this sub-step runs"
    assert not _has_pause_columns(conn)
    assert _score_cache_exists(conn)
    assert conn.execute("SELECT COUNT(*) FROM score_cache").fetchone()[0] == inserted
    assert _wind_history_row_exists(conn, wind_station, old_wind_at), (
        "the old wind row must survive -- v18_prune_wind_history runs"
        " before this sub-step and must roll back with it"
    )
    assert conn.execute("PRAGMA secure_delete").fetchone()[0] == 1
    assert _all_table_rows(conn) == _with_empty_station_wind_constant(before)

    monkeypatch.setattr(stop_module, "begin_step", real_begin_step)
    migrations.run_migrations(conn)
    _assert_sub_step_done(conn)
    assert (
        conn.execute(
            "SELECT 1 FROM forecast_samples WHERE id = ?", (out_of_range_id,)
        ).fetchone()
        is None
    ), "the next open, with no fault, must finish the sub-step it stopped short of"
    assert not _wind_history_row_exists(conn, wind_station, old_wind_at), (
        "the next open, with no fault, must also finish G's wind-history prune"
    )


def test_ut3_a_denied_forecast_samples_delete_rolls_back_the_wind_prune_too(
    tmp_path: Path,
) -> None:
    """UT3 for the ``v18_drop_bad_display_values`` sub-step, the
    all-or-nothing half. Catches: ``_v18_prune_wind_history`` (G) and
    ``_v18_drop_bad_display_values`` (H) not sharing one atomic unit with
    ``migrate_v18``'s SAVEPOINT -- if the wind-history DELETE had already
    committed on its own before the display-values DELETE failed, the old
    wind row would stay gone even though the upgrade as a whole raised and
    the version stayed at 17.

    The authorizer denies the ``SQLITE_DELETE`` on ``forecast_samples`` and
    records, in order, the ``station_wind_records`` DELETE (G's own) and
    the denied ``forecast_samples`` DELETE (H's own) -- the same
    order-list pattern as
    ``test_ut3_a_denied_runtime_state_delete_rolls_back_the_drop_too``. The
    wind DELETE coming first in ``order`` is this fixture's own
    precondition check that G ran (and was rolled back) ahead of H's
    denial, not merely that the denial landed on the right table.
    """
    conn = _v17_connect(tmp_path / "ut3-deny-drop-bad-display.db")
    site_id = _seed_bare_site(conn, "UT3 deny drop-bad-display site")
    out_of_range_id = _insert_forecast_sample(
        conn, site_id, 1, "cloud_cover", 106.0, "2026-11-07T06:00:00Z"
    )

    f_start = datetime(2026, 11, 7, tzinfo=UTC)
    f_end = f_start + timedelta(days=1)
    wind_site = _insert_bare_wind_site(conn, "UT3 deny drop-bad-display wind site")
    wind_station = _insert_bare_wind_station(conn, wind_site, "<WPDENY>")
    _insert_bare_site_days_record(
        conn,
        wind_site,
        f_start.date().isoformat(),
        isoformat_utc(f_start),
        isoformat_utc(f_end),
        done=True,
    )
    old_wind_at = f_start - timedelta(seconds=_UT2_WIND_MARGIN_S + 800)
    _insert_wind_history_row(conn, wind_station, old_wind_at, fetched_at=None)
    conn.execute("PRAGMA secure_delete = 1")
    conn.commit()
    before = _all_table_rows(conn)

    order: list[str] = []

    def _authorizer(
        action_code: int,
        arg1: str | None,
        arg2: str | None,
        dbname: str | None,
        source: str | None,
    ) -> int:
        if action_code == sqlite3.SQLITE_DELETE and arg1 == "station_wind_records":
            order.append("wind_delete")
        if action_code == sqlite3.SQLITE_DELETE and arg1 == "forecast_samples":
            order.append("denied_delete")
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    conn.set_authorizer(_authorizer)
    try:
        with pytest.raises(sqlite3.DatabaseError):
            migrations.run_migrations(conn)
    finally:
        conn.set_authorizer(None)

    assert "wind_delete" in order and "denied_delete" in order, (
        "fixture bug: expected both the station_wind_records DELETE and the"
        f" denied forecast_samples DELETE to fire -- got order={order!r}"
    )
    assert order.index("wind_delete") < order.index("denied_delete"), (
        "fixture bug: the station_wind_records DELETE must be authorized"
        " before the denied forecast_samples DELETE, so this oracle"
        " actually exercises the rollback of a prune that already ran --"
        f" got order={order!r}"
    )
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 17
    assert _wind_history_row_exists(conn, wind_station, old_wind_at), (
        "a sub-step committing its DELETE ahead of the later failed DELETE"
        " would leave the old wind row gone here"
    )
    assert (
        conn.execute(
            "SELECT 1 FROM forecast_samples WHERE id = ?", (out_of_range_id,)
        ).fetchone()
        is not None
    )
    assert conn.execute("PRAGMA secure_delete").fetchone()[0] == 1
    assert _all_table_rows(conn) == _with_empty_station_wind_constant(before)
    assert not _has_pause_columns(conn)

    migrations.run_migrations(conn)
    _assert_sub_step_done(conn)
    assert (
        conn.execute(
            "SELECT 1 FROM forecast_samples WHERE id = ?", (out_of_range_id,)
        ).fetchone()
        is None
    ), "the next open, with no fault, must finish dropping the row"
    assert not _wind_history_row_exists(conn, wind_station, old_wind_at), (
        "the next open, with no fault, must also finish G's wind-history prune"
    )
