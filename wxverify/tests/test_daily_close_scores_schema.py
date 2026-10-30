"""Schema and migration for daily-close P4 piece 1 (plan
2026-10-22-daily-close-pairs-and-publication.md, section 15, test IDs
SZ1-SZ4, 18.12).

Every test here is written before ``migrate_v14_daily_close_scores`` and
P4's twelve tables and four widened columns exist, and fails first for
that reason: ``TARGET_USER_VERSION`` is still 13 (15.1), the new tables are
absent from a migrated database, and the import checks of 15.6 do not yet
cover them. Section 19 of the plan names the mutant each test ID must
kill; that mapping is also given inline on each assertion.

Real SQLite and real migrations throughout; nothing in the code under test
is mocked. Synthetic data only: station ids ``ISTATION01``/``ISTATION02``
(via ``tests.helpers.day_close_site``), site names ``Testsite``/``site-a``/
``site-b``, zone ``UTC``, model names ``model-a``/``model-b``.

Column lists for the twelve new tables are taken from the plan's own
tables (15.2, 4.2, 5.2, 6.3, 6.4, 6.7, 7.1, 7.3); where the plan does not
state nullability explicitly, every documented column is given a value
here to avoid relying on an unconfirmed default.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.helpers import NOW, D, asof_conn, asof_make_real_feed, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import TARGET_USER_VERSION, run_migrations
from wxverify.db.runtime_state import get_runtime_state
from wxverify.db.tz_generations import ensure_published_generation

# ---------------------------------------------------------------------------
# Shared fixture plumbing
# ---------------------------------------------------------------------------

_NEW_TABLES = (
    "forecast_downloads",
    "forecast_value_changes",
    "close_pairs",
    "day_scores",
    "close_records",
    "close_record_downloads",
    "starting_scores",
    "starting_wind_days",
    "publications",
    "publication_rankings",
    "publication_wind_weights",
    "publication_coverage",
)

# 15.3 names two widened tables (``verification_runs`` gains
# ``worksheet_kept``; ``forecast_of_record`` gains three publication
# columns). The plan's own 18.12 SZ1 row says "the three widened ones";
# only two are named anywhere in section 15, so this pins the two the
# schema section actually documents rather than guessing at a third.
_WIDENED_TABLES = ("verification_runs", "forecast_of_record")

_STAMP = "2026-07-06T00:00:00Z"


def _table_shape(conn: sqlite3.Connection, table: str) -> list[dict[str, object]]:
    return [dict(r) for r in conn.execute(f"PRAGMA table_xinfo({table})")]


def _table_indexes(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(r["name"])
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
            (table,),
        )
    }


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(r["name"]) for r in conn.execute(f"PRAGMA table_info({table})")}


def _drop_columns_if_present(
    conn: sqlite3.Connection, table: str, columns: tuple[str, ...]
) -> None:
    present = _table_columns(conn, table)
    for column in columns:
        if column in present:
            conn.execute(f"ALTER TABLE {table} DROP COLUMN {column}")  # noqa: S608


def _strip_p4_additions(conn: sqlite3.Connection) -> None:
    """Undo P4's schema additions on an already-migrated connection, mirroring
    ``tests/test_close_schema.py``'s ``_build_v12_file`` technique: drop the
    twelve new tables, drop the four new columns if present, remove the
    ``download_record_since`` runtime-state row, and set ``user_version``
    back to 13. Safe to call before piece 1 lands too (every drop is a
    no-op then), so one helper builds a genuine v13 file in either state.
    """
    for table in _NEW_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS {table}")  # noqa: S608
    _drop_columns_if_present(conn, "verification_runs", ("worksheet_kept",))
    _drop_columns_if_present(
        conn,
        "forecast_of_record",
        ("publication_id", "publication_cutoff", "publication_age_days"),
    )
    conn.execute("DELETE FROM runtime_state WHERE key = 'download_record_since'")
    conn.execute("PRAGMA user_version = 13")


def _insert_verification_run(conn: sqlite3.Connection, site_id: int) -> int:
    gen_id = ensure_published_generation(conn, site_id)
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, bootstrap_seed,
             bootstrap_resamples, input_fingerprint)
        VALUES (?, ?, 1, '0.0.0-test', 'published', 1, '{}', 1, 10, 'fp-1')
        """,
        (site_id, gen_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_forecast_of_record(conn: sqlite3.Connection, site_id: int) -> None:
    gen_row = conn.execute(
        "SELECT id FROM timezone_generations WHERE site_id = ?", (site_id,)
    ).fetchone()
    assert gen_row is not None
    conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, missed_reason, methodology_version,
             app_version)
        VALUES (?, ?, 'UTC', ?, '07:00', ?, ?, 'temperature', 0, 'missed',
                'no_data', 1, '0.0.0-test')
        """,
        (site_id, gen_row[0], D.isoformat(), isoformat_utc(NOW), D.isoformat()),
    )


def _build_v13_file_with_runs_and_records(path: Path) -> tuple[Path, int, int]:
    """A genuine schema-version-13 file carrying a ``verification_runs`` row
    and a ``forecast_of_record`` row, built by migrating fully then undoing
    P4's additions (SC1's technique in ``tests/test_close_schema.py``). SZ1.
    """
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = asof_make_site(conn, "Testsite")
    run_id = _insert_verification_run(conn, site_id)
    _insert_forecast_of_record(conn, site_id)
    _strip_p4_additions(conn)
    conn.commit()
    conn.close()
    return path, site_id, run_id


# --- SZ1 --------------------------------------------------------------------


def test_sz1_fresh_and_migrated_v13_files_agree_and_old_rows_survive(
    tmp_path: Path,
) -> None:
    """SZ1: a fresh file and a file at schema version 13 carrying a
    ``verification_runs`` row and a ``forecast_of_record`` row, both
    migrated, land on the same ``user_version``, the same shapes and
    indexes for every new table and the two widened ones, keep the old
    run's ``worksheet_kept`` at 1 and the record's new columns NULL, and
    set ``download_record_since`` once.
    """
    fresh_conn = sqlite3.connect(":memory:")
    fresh_conn.row_factory = sqlite3.Row
    fresh_conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(fresh_conn)

    v13_path, site_id, run_id = _build_v13_file_with_runs_and_records(
        tmp_path / "v13.db"
    )
    migrated_conn = sqlite3.connect(str(v13_path))
    migrated_conn.row_factory = sqlite3.Row
    migrated_conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(migrated_conn)

    for conn in (fresh_conn, migrated_conn):
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == TARGET_USER_VERSION, (
            "M116 -> at this assertion: correct = user_version read from"
            " TARGET_USER_VERSION (14 in this release; P6 raises it"
            " further), never a literal, mutant M116 (a new column added in"
            " a different position on migrate) = the migrated file's"
            " PRAGMA table_xinfo diverges from the fresh file's below even"
            " though both reach the same user_version"
        )

    for table in (*_NEW_TABLES, *_WIDENED_TABLES):
        fresh_shape = _table_shape(fresh_conn, table)
        migrated_shape = _table_shape(migrated_conn, table)
        assert fresh_shape == migrated_shape, (
            f"M116 -> at the {table} table_xinfo comparison: correct ="
            " identical column lists (name, type, and position) on both"
            " paths, mutant M116 (a new column added in a different"
            f" position on migrate) = the two {table} shapes disagree"
        )
        fresh_idx = _table_indexes(fresh_conn, table)
        migrated_idx = _table_indexes(migrated_conn, table)
        assert fresh_idx == migrated_idx, (
            f"mutant -> at the {table} index-name comparison: correct ="
            " the same index names on both paths, mutant (an index lost in"
            " the rebuild) = the migrated file missing one"
        )

    run_row = migrated_conn.execute(
        "SELECT worksheet_kept FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert run_row is not None and run_row["worksheet_kept"] == 1, (
        "mutant -> at this assertion: correct = 1 (every pre-existing"
        " verification_runs row gets worksheet_kept 1, 15.5), mutant (the"
        " ALTER TABLE ... ADD COLUMN default left out or set to 0) ="
        " worksheet_kept is 0 or NULL on the old row"
    )
    record_row = migrated_conn.execute(
        "SELECT publication_id, publication_cutoff, publication_age_days"
        " FROM forecast_of_record WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    assert record_row is not None
    assert (
        record_row["publication_id"] is None
        and record_row["publication_cutoff"] is None
        and record_row["publication_age_days"] is None
    ), (
        "mutant -> at this assertion: correct = all three NULL (15.5: every"
        " pre-existing record gets NULL publication columns), mutant (a"
        " non-NULL default on one of the three) = at least one is not None"
    )

    since = get_runtime_state(migrated_conn, "download_record_since")
    assert since is not None, (
        "mutant -> at this assertion: correct = download_record_since set"
        " (15.4: the migration writes it once, with the migration's time,"
        " when absent), mutant (the runtime_state write left out of"
        " migrate_v14_daily_close_scores) = the key stays absent"
    )


# --- SZ2 --------------------------------------------------------------------


def test_sz2_migration_run_twice_adds_nothing_twice(tmp_path: Path) -> None:
    """SZ2 (first case): running the v14 migration twice on a fresh file
    raises no error and leaves ``download_record_since`` exactly as the
    first run set it -- the probe that lets the migration skip its own
    column/table adds a second time must still be in place.
    """
    conn = sqlite3.connect(str(tmp_path / "fresh.db"))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    conn.commit()
    first_since = get_runtime_state(conn, "download_record_since")
    assert first_since is not None, (
        "download_record_since not set after the first migration run: P4"
        " piece 1 (15.4) has migrate_v14_daily_close_scores write this"
        " runtime_state key once, with the migration's time, when absent"
    )

    run_migrations(conn)  # must not raise (no duplicate column/table error)
    conn.commit()

    second_since = get_runtime_state(conn, "download_record_since")
    assert second_since == first_since, (
        "M117 -> at this assertion: correct = unchanged (the migration"
        " writes download_record_since only when absent, 15.4), mutant"
        " M117 (the column/table probe removed, so every re-run repeats"
        " every write unconditionally) = second_since differs from"
        " first_since (a later timestamp from the second run)"
    )


def test_sz2_a_hand_added_worksheet_kept_column_is_left_alone(
    tmp_path: Path,
) -> None:
    """SZ2 (second case): a version-13 file where ``verification_runs.
    worksheet_kept`` was already added by hand, with a non-default value on
    an existing row, migrates to the target version without error and
    without resetting that value -- proving the migration checks
    ``_table_columns`` before its own ``ALTER TABLE ... ADD COLUMN``
    (15.3) rather than running it unconditionally.
    """
    v13_path, site_id, run_id = _build_v13_file_with_runs_and_records(
        tmp_path / "v13-hand-altered.db"
    )
    conn = sqlite3.connect(str(v13_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    assert "worksheet_kept" not in _table_columns(conn, "verification_runs"), (
        "fixture bug: the v13 file must not already carry worksheet_kept"
        " before the by-hand ALTER below"
    )
    conn.execute(
        "ALTER TABLE verification_runs ADD COLUMN worksheet_kept INTEGER"
        " NOT NULL DEFAULT 1"
    )
    conn.execute(
        "UPDATE verification_runs SET worksheet_kept = 0 WHERE id = ?",
        (run_id,),
    )
    conn.commit()

    run_migrations(conn)  # M117 -> must not raise "duplicate column name"
    conn.commit()

    since = get_runtime_state(conn, "download_record_since")
    assert since is not None, (
        "precondition -> at this assertion: correct = download_record_since"
        " set (P4 piece 1's migrate_v14_daily_close_scores writes it once,"
        " 15.4), missing = the migration never ran on this file, so this"
        " test proves nothing about the column-exists probe below"
    )
    forecast_downloads_present = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='forecast_downloads'"
    ).fetchone()
    assert forecast_downloads_present is not None, (
        "precondition -> at this assertion: correct = forecast_downloads"
        " present (P4 piece 1's 15.2 creates it in create_tables, which"
        " migrate_v14_daily_close_scores reaches on this file), missing ="
        " the migration never ran on this file, so this test proves"
        " nothing about the column-exists probe below"
    )

    row = conn.execute(
        "SELECT worksheet_kept FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert row is not None and row["worksheet_kept"] == 0, (
        "M117 -> at this assertion: correct = 0 (the migration's own ADD"
        " COLUMN is skipped when the column already exists, 15.3, so the"
        " hand-set value survives untouched), mutant M117 (the"
        " _table_columns probe removed) = either a sqlite3.OperationalError"
        " ('duplicate column name: worksheet_kept') was raised above, or"
        " (a variant that catches it and re-runs the UPDATE default) the"
        " value is reset to 1"
    )


# --- SZ3 --------------------------------------------------------------------


def _seed_cascading_p4_rows(
    conn: sqlite3.Connection, *, site_id: int, feed_id: int, local_date: str
) -> None:
    """One row in every P4 table that carries a ``site_id`` or cascades
    through a parent that does, for ``site_id``/``feed_id`` (SZ3).
    """
    download_id = conn.execute(
        """
        INSERT INTO forecast_downloads
            (site_id, feed_id, local_date, slot, round_date, round_try,
             started_at, ended_at, fetched_at, outcome, reason, usable,
             inserted, labels)
        VALUES (?, ?, ?, 'unscheduled', NULL, NULL, ?, ?, ?, 'stored', NULL,
                1, 1, '{}')
        """,
        (site_id, feed_id, local_date, _STAMP, _STAMP, _STAMP),
    ).lastrowid
    assert download_id is not None

    conn.execute(
        """
        INSERT INTO forecast_value_changes
            (download_id, previous_download_id, feed_id, variable,
             hours_compared, hours_changed, max_abs_change,
             first_changed_valid_at, labels_changed)
        VALUES (?, NULL, ?, 'temperature', 0, 0, NULL, NULL, 0)
        """,
        (download_id, feed_id),
    )

    conn.execute(
        """
        INSERT INTO close_pairs
            (site_id, local_date, feed_id, variable, day_ahead, valid_at,
             lead_hours, forecast, observed, error, abs_error, sq_error,
             cat_hit, cat_false, cat_miss, cat_correct_neg,
             rain_threshold_mm, ref_sq_error, download_id, download_date,
             issued_at, fetched_at, contributors)
        VALUES (?, ?, ?, 'temperature', 0, ?, 6, 10.0, 8.0, 2.0, 2.0, 4.0,
                NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?, NULL)
        """,
        (site_id, local_date, feed_id, _STAMP, download_id, local_date, _STAMP, _STAMP),
    )

    conn.execute(
        """
        INSERT INTO day_scores
            (site_id, local_date, feed_id, variable, day_ahead, status,
             download_id, download_date, fetched_at, n, err_n, err_sum,
             abs_sum, sq_sum, hits, false_alarms, misses, correct_negs,
             skill_n, skill_sq_sum, skill_ref_sq_sum, wind_fc_high,
             wind_fc_hours, wind_obs_high)
        VALUES (?, ?, ?, 'temperature', 0, 'scored', ?, ?, ?, 1, 1, 2.0,
                2.0, 4.0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL)
        """,
        (site_id, local_date, feed_id, download_id, local_date, _STAMP),
    )

    conn.execute(
        """
        INSERT INTO close_records
            (site_id, local_date, attempt, truth_version, scoring_rule,
             scored_at)
        VALUES (?, ?, 1, 'fp-1', 'first-stored-download-of-local-day/1', ?)
        """,
        (site_id, local_date, _STAMP),
    )

    conn.execute(
        """
        INSERT INTO close_record_downloads
            (site_id, local_date, feed_id, day_ahead, download_id,
             download_source, download_date, fetched_at, labels)
        VALUES (?, ?, ?, 0, ?, 'recorded', ?, ?, '{}')
        """,
        (site_id, local_date, feed_id, download_id, local_date, _STAMP),
    )

    publication_id = conn.execute(
        """
        INSERT INTO publications
            (site_id, cutoff, first_record_date, window_days,
             window_start_date, wind_start_date, settings,
             methodology_version, scoring_rule, wind_method, wind_constants,
             build_started_at, published_at)
        VALUES (?, ?, ?, 30, ?, ?, '{}', 4,
                'first-stored-download-of-local-day/1', 'persistence_blend',
                '{}', ?, ?)
        """,
        (site_id, local_date, local_date, local_date, local_date, _STAMP, _STAMP),
    ).lastrowid
    assert publication_id is not None

    conn.execute(
        """
        INSERT INTO publication_rankings
            (publication_id, feed_id, variable, day_ahead, window_key, n,
             bias, mae, rmse, pod, far, csi, ets, hss, skill_score)
        VALUES (?, ?, 'temperature', 0, 'w:30', 1, 2.0, 2.0, 2.0, NULL,
                NULL, NULL, NULL, NULL, NULL)
        """,
        (publication_id, feed_id),
    )


def test_sz3_site_delete_clears_every_p4_row_feed_delete_with_scores_refused() -> None:
    """SZ3: deleting a site removes every P4 row keyed to it (``site_id``
    cascades, 15.2) across every table that carries ``site_id`` or cascades
    from a parent that does; deleting a feed that still has a ``day_scores``
    row is refused with ``sqlite3.IntegrityError`` (``feed_id`` restricts,
    15.2), on a second, untouched site so the first site's cascade cannot
    remove the row the refusal needs.
    """
    conn = asof_conn()

    site_a = asof_make_site(conn, "site-a")
    feed_a = asof_make_real_feed(conn, "model-a")
    _seed_cascading_p4_rows(
        conn, site_id=site_a, feed_id=feed_a, local_date=D.isoformat()
    )

    site_b = asof_make_site(conn, "site-b")
    feed_b = asof_make_real_feed(conn, "model-b")
    conn.execute(
        """
        INSERT INTO day_scores
            (site_id, local_date, feed_id, variable, day_ahead, status, n,
             err_n, err_sum, abs_sum, sq_sum)
        VALUES (?, ?, ?, 'temperature', 0, 'scored', 1, 1, 2.0, 2.0, 4.0)
        """,
        (site_b, D.isoformat(), feed_b),
    )
    conn.commit()

    conn.execute("DELETE FROM sites WHERE id = ?", (site_a,))
    conn.commit()

    direct_site_tables = (
        "forecast_downloads",
        "close_pairs",
        "day_scores",
        "close_records",
        "close_record_downloads",
        "publications",
    )
    for table in direct_site_tables:
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE site_id = ?",  # noqa: S608
            (site_a,),
        ).fetchone()[0]
        assert count == 0, (
            f"M118 -> at this assertion: correct = 0 ({table}.site_id"
            " references sites(id) ON DELETE CASCADE, 15.2), mutant M118 (a"
            f" P4 table without ON DELETE CASCADE on site_id) = {table}"
            " still has a row for the deleted site"
        )

    cascaded_child_tables = ("forecast_value_changes", "publication_rankings")
    for table in cascaded_child_tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
        assert count == 0, (
            f"M118 -> at this assertion: correct = 0 ({table} cascades from"
            " its parent, which itself cascades from sites, 15.2), mutant"
            f" M118 (a missing ON DELETE CASCADE somewhere in that chain) ="
            f" {table} still has an orphaned row"
        )

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("DELETE FROM feeds WHERE id = ?", (feed_b,))
    conn.rollback()

    # Paired positive: site_b's own row is untouched by site_a's cascade,
    # and deleting site_b (rather than its feed) DOES clear it -- so the
    # refusal above is feed_id's RESTRICT specifically, not some unrelated
    # fault that would have refused every delete.
    still_there = conn.execute(
        "SELECT COUNT(*) FROM day_scores WHERE site_id = ?", (site_b,)
    ).fetchone()[0]
    assert still_there == 1
    conn.execute("DELETE FROM sites WHERE id = ?", (site_b,))
    conn.commit()
    gone = conn.execute(
        "SELECT COUNT(*) FROM day_scores WHERE site_id = ?", (site_b,)
    ).fetchone()[0]
    assert gone == 0


@pytest.mark.parametrize(
    "table",
    [
        "starting_scores",
        "starting_wind_days",
        "publication_wind_weights",
        "publication_coverage",
    ],
)
def test_sz3_the_remaining_tables_declare_the_same_delete_rules(table: str) -> None:
    """SZ3 (structural twin): the four P4 tables not exercised by row-level
    delete above still declare ``site_id``/the parent cascade and
    ``feed_id`` RESTRICT per 15.2, read from ``PRAGMA foreign_key_list``
    rather than by inserting a row -- ``starting_scores`` and
    ``starting_wind_days`` carry ``site_id`` directly;
    ``publication_wind_weights`` and ``publication_coverage`` cascade only
    through ``publication_id`` (no ``site_id`` column of their own).
    """
    conn = asof_conn()
    names = {
        str(row["name"])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert table in names, (
        f"{table} does not exist yet: P4 piece 1 (plan 15.2) adds it as one"
        " of the twelve new tables; migrate_v14_daily_close_scores has not"
        " landed"
    )
    fks = conn.execute(f"PRAGMA foreign_key_list({table})").fetchall()
    by_column = {str(row["from"]): row for row in fks}

    if "site_id" in by_column:
        assert by_column["site_id"]["on_delete"] == "CASCADE", (
            f"M118 -> at this assertion: correct = CASCADE ({table}.site_id"
            " references sites(id) ON DELETE CASCADE, 15.2), mutant M118"
            f" (missing ON DELETE CASCADE) = {by_column['site_id']['on_delete']}"
        )
    else:
        assert "publication_id" in by_column, (
            f"fixture/plan-reading error: {table} has neither site_id nor"
            " publication_id, 15.2 does not describe this table correctly"
        )
        assert by_column["publication_id"]["on_delete"] == "CASCADE", (
            f"M118 -> at this assertion: correct = CASCADE ({table}"
            " cascades from publications, 15.2), mutant M118 (missing"
            " ON DELETE CASCADE) ="
            f" {by_column['publication_id']['on_delete']}"
        )

    if "feed_id" in by_column:
        assert by_column["feed_id"]["on_delete"] == "RESTRICT", (
            f"M118 -> at this assertion: correct = RESTRICT ({table}.feed_id"
            " references feeds(id) ON DELETE RESTRICT, 15.2, as"
            " score_cache does), mutant M118 (ON DELETE CASCADE given to"
            f" feed_id instead) = {by_column['feed_id']['on_delete']}"
        )


# --- SZ4 --------------------------------------------------------------------


def _build_fresh_v14_file(path: Path) -> Path:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    conn.commit()
    conn.close()
    return path


def test_sz4_a_fresh_file_carrying_every_new_table_is_accepted(
    tmp_path: Path,
) -> None:
    """SZ4 (case 1): a freshly migrated file -- every one of P4's twelve
    tables present, empty -- passes ``_validate_upload``.
    """
    from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701

    path = _build_fresh_v14_file(tmp_path / "all-new-tables.db")
    check_conn = sqlite3.connect(str(path))
    try:
        present = {
            str(r[0])
            for r in check_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    finally:
        check_conn.close()
    missing = [table for table in _NEW_TABLES if table not in present]
    assert not missing, (
        "precondition -> at this assertion: correct = all twelve of P4"
        " piece 1's new tables (15.2) present on this fresh file, missing"
        f" = {missing} absent, so this test's _validate_upload call below"
        " would not exercise P4's per-table import checks (15.6) for them"
    )

    _validate_upload(path)  # must not raise


def test_sz4_a_blob_in_close_pairs_variable_is_refused(tmp_path: Path) -> None:
    """SZ4 (case 2): a BLOB in ``close_pairs.variable`` is refused at 422,
    the message naming that table and column (15.6: the text ``variable``
    columns join ``_BLOB_GUARDED_COLUMNS``).
    """
    from wxverify.api.errors import ApiError
    from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701

    path = tmp_path / "blob-variable.db"
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = asof_make_site(conn, "Testsite")
    feed_id = asof_make_real_feed(conn, "model-a")
    conn.execute(
        """
        INSERT INTO close_pairs
            (site_id, local_date, feed_id, variable, day_ahead, valid_at,
             lead_hours, forecast, observed, error, abs_error, sq_error,
             download_id, download_date, issued_at, fetched_at)
        VALUES (?, ?, ?, ?, 0, ?, 6, 10.0, 8.0, 2.0, 2.0, 4.0, NULL, ?, ?, ?)
        """,
        (
            site_id,
            D.isoformat(),
            feed_id,
            b"not-a-string",
            _STAMP,
            D.isoformat(),
            _STAMP,
            _STAMP,
        ),
    )
    conn.commit()
    conn.close()

    with pytest.raises(ApiError) as excinfo:
        _validate_upload(path)
    assert excinfo.value.status_code == 422
    assert excinfo.value.message == "invalid data in close_pairs.variable", (
        "M119 -> at this assertion: correct = 'invalid data in"
        " close_pairs.variable' (close_pairs.variable joins"
        " _BLOB_GUARDED_COLUMNS, 15.6), mutant M119 (close_pairs left out"
        " of _BLOB_GUARDED_COLUMNS) = _validate_upload does not raise for"
        " this file at all"
    )


def test_sz4_a_non_canonical_published_at_is_refused(tmp_path: Path) -> None:
    """SZ4 (case 3): a non-canonical ``publications.published_at`` is
    refused at 422 with P1/P3's stamp message (15.6: the stamp columns join
    ``_CANONICAL_STAMP_COLUMNS``).
    """
    from wxverify.api.errors import ApiError
    from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701

    path = tmp_path / "bad-published-at.db"
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = asof_make_site(conn, "Testsite")
    conn.execute(
        """
        INSERT INTO publications
            (site_id, cutoff, first_record_date, window_days,
             window_start_date, wind_start_date, settings,
             methodology_version, scoring_rule, wind_method, wind_constants,
             build_started_at, published_at)
        VALUES (?, ?, ?, 30, ?, ?, '{}', 4,
                'first-stored-download-of-local-day/1', 'persistence_blend',
                '{}', ?, ?)
        """,
        (
            site_id,
            D.isoformat(),
            D.isoformat(),
            D.isoformat(),
            D.isoformat(),
            _STAMP,
            "2026-07-06T00:00:00.000000Z",  # microseconds: not whole-second canonical
        ),
    )
    conn.commit()
    conn.close()

    with pytest.raises(ApiError) as excinfo:
        _validate_upload(path)
    assert excinfo.value.status_code == 422
    assert excinfo.value.message == (
        "invalid timestamp in publications.published_at: "
        "expected UTC form YYYY-MM-DDTHH:MM:SSZ"
    ), (
        "M119 -> at this assertion: correct = the stamp-message above"
        " (publications.published_at joins _CANONICAL_STAMP_COLUMNS, 15.6),"
        " mutant M119 (publications left out of _CANONICAL_STAMP_COLUMNS) ="
        " _validate_upload does not raise for this file at all"
    )


def test_sz4_a_bad_cutoff_date_is_refused(tmp_path: Path) -> None:
    """SZ4 (case 4): a non-canonical ``publications.cutoff`` is refused at
    422 with P1's date message (15.6: the date columns join
    ``_CANONICAL_DATE_COLUMNS``; ``publications.cutoff`` has no foreign key
    to ``site_days``, unlike P3's record tables, so only the per-column
    date check can catch this).
    """
    from wxverify.api.errors import ApiError
    from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701

    path = tmp_path / "bad-cutoff.db"
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = asof_make_site(conn, "Testsite")
    conn.execute(
        """
        INSERT INTO publications
            (site_id, cutoff, first_record_date, window_days,
             window_start_date, wind_start_date, settings,
             methodology_version, scoring_rule, wind_method, wind_constants,
             build_started_at, published_at)
        VALUES (?, ?, ?, 30, ?, ?, '{}', 4,
                'first-stored-download-of-local-day/1', 'persistence_blend',
                '{}', ?, ?)
        """,
        (
            site_id,
            "2026/07/06",
            D.isoformat(),
            D.isoformat(),
            D.isoformat(),
            _STAMP,
            _STAMP,
        ),
    )
    conn.commit()
    conn.close()

    with pytest.raises(ApiError) as excinfo:
        _validate_upload(path)
    assert excinfo.value.status_code == 422
    assert excinfo.value.message == (
        "invalid date in publications.cutoff: expected YYYY-MM-DD"
    ), (
        "M119 -> at this assertion: correct = the date-message above"
        " (publications.cutoff joins _CANONICAL_DATE_COLUMNS, 15.6), mutant"
        " M119 (publications.cutoff left out of _CANONICAL_DATE_COLUMNS) ="
        " _validate_upload does not raise for this file at all"
    )


def test_sz4_a_file_from_before_p4_is_accepted(tmp_path: Path) -> None:
    """SZ4 (case 5): a file genuinely at schema version 13 -- none of P4's
    twelve tables present -- is accepted by ``_validate_upload`` (15.6:
    each check is skipped when its table is absent) and migrates the rest
    of the way on open.
    """
    from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701

    path, _site_id, _run_id = _build_v13_file_with_runs_and_records(
        tmp_path / "pre-p4.db"
    )

    _validate_upload(path)  # must not raise

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == TARGET_USER_VERSION, (
        "mutant -> at this assertion: correct = TARGET_USER_VERSION (the"
        " older, pre-this-release file migrates the rest of the way on"
        " open), mutant (an upload-time version check that refuses a"
        " version-13 file outright) = this file never reaches"
        " TARGET_USER_VERSION here"
    )
    names = {
        str(row["name"])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    for table in _NEW_TABLES:
        assert table in names, (
            f"mutant -> at this assertion: correct = {table} present after"
            " migrating the older file, mutant (a new table left out of"
            f" create_tables/migrate_v14_daily_close_scores) = {table}"
            " absent"
        )
