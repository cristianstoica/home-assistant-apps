"""Schema for daily-close P4 piece 1 (plan
2026-10-22-daily-close-pairs-and-publication.md, section 15, test IDs
SZ2-SZ3, 18.12): the live, fresh-schema behaviour of P4's twelve new tables
and their migration -- idempotence on repeat, foreign-key cascade/restrict
rules. SZ4 (``_validate_upload``'s per-table import checks) was removed
with the database import (storage-release plan 5.5). Section 19 of the
plan names the mutant each test ID must kill; that mapping is also given
inline on each assertion.

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

from tests.helpers import D, asof_conn, asof_make_real_feed, asof_make_site
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import get_runtime_state

# ---------------------------------------------------------------------------
# Shared fixture plumbing
# ---------------------------------------------------------------------------

_STAMP = "2026-07-06T00:00:00Z"


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
        INSERT INTO forecast_history
            (site_id, feed_id, variable, day_ahead, valid_at, local_date,
             value, lead_hours, issued_at, contributors)
        VALUES (?, ?, 'temperature', 0, ?, ?, 10.0, 6, ?, NULL)
        """,
        (site_id, feed_id, _STAMP, local_date, _STAMP),
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
        "forecast_history",
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


# SZ4 (fresh-file acceptance, BLOB-guarded columns, canonical stamp/date
# checks on close_pairs/publications) tested only ``_validate_upload``,
# removed with the database import (storage-release plan 5.5).
