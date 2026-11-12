"""T7-T9: the one-time deletion of every Meteosource row stored before the
UTC fix.

Isolation: every connection is a real, file-backed SQLite database under
``tmp_path``, opened once through ``init_db`` so the schema and the boot
seed (sources, feeds, settings) are the real ones -- no mocked persistence,
and no reach into ``wxverify.db.migrations`` module internals beyond the
two public names (``delete_pre_utc_meteosource``,
``METEOSOURCE_UTC_FIX_KEY``) and ``METEOSOURCE_PRUNE_TABLES``, the step's
table-order constant.

Synthetic fixtures only: a site named ``Testsite`` at ``0.0, 0.0`` in zone
``UTC``, synthetic dates, and the real visualcrossing ``blend`` feed used
only as an inert control row -- never a real station id or coordinate.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from wxverify import config
from wxverify.db import migrations
from wxverify.db.connection import close_db, init_db
from wxverify.db.runtime_state import get_runtime_state

_SITE_SQL = """
INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')
"""


def _init_tmp_db(tmp_path: Path, name: str = "wxverify.db") -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / name
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _feed_ids(conn: sqlite3.Connection) -> tuple[int, int]:
    """``(meteosource_feed_id, control_feed_id)`` from the boot seed."""
    ms_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='meteosource' AND model='blend'"
        ).fetchone()["id"]
    )
    control_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='visualcrossing' AND model='blend'"
        ).fetchone()["id"]
    )
    return ms_id, control_id


def _seed_publication(conn: sqlite3.Connection, site_id: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO publications
            (site_id, cutoff, first_record_date, window_days,
             window_start_date, wind_start_date, settings, methodology_version,
             scoring_rule, wind_method, wind_constants, build_started_at,
             published_at)
        VALUES
            (?, '2026-06-15T00:00:00Z', '2026-01-01', 60, '2026-04-17',
             '2026-04-17', '{}', 1, 'synthetic', 'synthetic', '{}',
             '2026-06-15T00:00:00Z', '2026-06-15T00:00:01Z')
        """,
        (site_id,),
    )
    return int(cur.lastrowid)


def _seed_download(
    conn: sqlite3.Connection, site_id: int, feed_id: int, local_date: str
) -> int:
    cur = conn.execute(
        """
        INSERT INTO forecast_downloads
            (site_id, feed_id, local_date, slot, started_at, ended_at,
             outcome, usable, inserted, labels)
        VALUES (?, ?, ?, 'unscheduled', ?, ?, 'stored', 1, 1, '{}')
        """,
        (
            site_id,
            feed_id,
            local_date,
            f"{local_date}T00:00:00Z",
            f"{local_date}T00:00:01Z",
        ),
    )
    return int(cur.lastrowid)


def _seed_one_row_per_table(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    publication_id: int,
    local_date: str,
    cutoff: str,
    rank: int,
) -> None:
    """One row of ``feed_id`` in each of :data:`migrations.METEOSOURCE_PRUNE_TABLES`
    (``forecast_downloads`` included, as its own parent)."""
    download_id = _seed_download(conn, site_id, feed_id, local_date)
    conn.execute(
        """
        INSERT INTO forecast_value_changes
            (download_id, feed_id, variable, hours_compared, hours_changed,
             labels_changed)
        VALUES (?, ?, 'temperature', 1, 0, 0)
        """,
        (download_id, feed_id),
    )
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, 1, 10.0, '10.0 C', 'synthetic-run', ?)
        """,
        (
            site_id,
            feed_id,
            f"{local_date}T00:00:00Z",
            f"{local_date}T01:00:00Z",
            f"{local_date}T00:00:01Z",
        ),
    )
    conn.execute(
        """
        INSERT INTO forecast_scored
            (site_id, feed_id, variable, valid_at, day_ahead, value, lead_hours,
             issued_at, download_date, fetched_at)
        VALUES (?, ?, 'temperature', ?, 0, 10.0, 1, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            f"{local_date}T01:00:00Z",
            f"{local_date}T00:00:00Z",
            local_date,
            f"{local_date}T00:00:01Z",
        ),
    )
    conn.execute(
        """
        INSERT INTO forecast_history
            (site_id, feed_id, variable, day_ahead, valid_at, local_date, value,
             lead_hours, issued_at)
        VALUES (?, ?, 'temperature', 0, ?, ?, 10.0, 1, ?)
        """,
        (
            site_id,
            feed_id,
            f"{local_date}T01:00:00Z",
            local_date,
            f"{local_date}T00:00:00Z",
        ),
    )
    conn.execute(
        """
        INSERT INTO day_scores
            (site_id, local_date, feed_id, variable, day_ahead, status, n, err_n)
        VALUES (?, ?, ?, 'temperature', 0, 'scored', 1, 0)
        """,
        (site_id, local_date, feed_id),
    )
    conn.execute(
        """
        INSERT INTO close_record_downloads
            (site_id, local_date, feed_id, day_ahead)
        VALUES (?, ?, ?, 0)
        """,
        (site_id, local_date, feed_id),
    )
    conn.execute(
        """
        INSERT INTO starting_scores
            (site_id, local_date, feed_id, variable, day_ahead, n, err_n)
        VALUES (?, ?, ?, 'temperature', 0, 1, 0)
        """,
        (site_id, local_date, feed_id),
    )
    conn.execute(
        """
        INSERT INTO starting_wind_days (site_id, local_date, feed_id, day_ahead)
        VALUES (?, ?, ?, 0)
        """,
        (site_id, local_date, feed_id),
    )
    conn.execute(
        """
        INSERT INTO rain_forecast_days
            (site_id, local_date, feed_id, day_ahead, total_mm)
        VALUES (?, ?, ?, 0, 1.0)
        """,
        (site_id, local_date, feed_id),
    )
    conn.execute(
        """
        INSERT INTO publication_rankings
            (publication_id, variable, day_ahead, window_key, feed_id, n)
        VALUES (?, 'temperature', 0, ?, ?, 1)
        """,
        (publication_id, f"feed-{feed_id}", feed_id),
    )
    conn.execute(
        """
        INSERT INTO publication_coverage
            (publication_id, variable, window_key, day_ahead, feed_id, scored,
             expected, starting_days)
        VALUES (?, 'temperature', ?, 0, ?, 1, 1, 1)
        """,
        (publication_id, f"feed-{feed_id}", feed_id),
    )
    conn.execute(
        """
        INSERT INTO publication_wind_weights
            (publication_id, day_ahead, feed_id, mae_ms, days, weight)
        VALUES (?, 0, ?, 1.0, 1, 1.0)
        """,
        (publication_id, feed_id),
    )
    conn.execute(
        """
        INSERT INTO publication_rain_ranks
            (publication_id, day_ahead, rank, feed_id, misses, false_alarms,
             common_days, counted_days, provisional, selected, in_group)
        VALUES (?, 0, ?, ?, 0, 0, 1, 1, 0, 1, 1)
        """,
        (publication_id, rank, feed_id),
    )
    conn.execute(
        """
        INSERT INTO rain_picks (site_id, cutoff, day_ahead, feed_id)
        VALUES (?, ?, 0, ?)
        """,
        (site_id, cutoff, feed_id),
    )
    conn.execute(
        """
        INSERT INTO om_archive_cells
            (site_id, feed_id, variable, day_ahead, local_date, download_date,
             outcome, hours, run_at, filled_at)
        VALUES (?, ?, 'temperature', 0, ?, ?, 'written', 1, ?, ?)
        """,
        (
            site_id,
            feed_id,
            local_date,
            local_date,
            f"{local_date}T00:00:00Z",
            f"{local_date}T00:00:01Z",
        ),
    )


def _row_count(conn: sqlite3.Connection, table: str, feed_id: int) -> int:
    return int(
        conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE feed_id = ?",  # noqa: S608
            (feed_id,),
        ).fetchone()[0]
    )


def _all_feed_counts(conn: sqlite3.Connection, feed_id: int) -> dict[str, int]:
    return {
        table: _row_count(conn, table, feed_id)
        for table in migrations.METEOSOURCE_PRUNE_TABLES
        if table != "rain_picks"
    }


# ---------------------------------------------------------------------------
# T7: the deletion clears every Meteosource row and leaves every control row
# and the schema pin.
# ---------------------------------------------------------------------------


def test_t7_deletes_every_meteosource_row_and_only_meteosource_rows(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """``delete_pre_utc_meteosource`` deletes every Meteosource row from
    every table it prunes, leaves every control-feed row and the
    ``feeds``/``site_feed_state`` rows unchanged, sets the key, and its one
    INFO line reports the seeded counts."""
    conn = _init_tmp_db(tmp_path)
    try:
        conn.execute(
            "DELETE FROM runtime_state WHERE key = ?",
            (migrations.METEOSOURCE_UTC_FIX_KEY,),
        )
        site_id = int(conn.execute(_SITE_SQL).lastrowid)
        ms_feed_id, control_feed_id = _feed_ids(conn)
        conn.execute("UPDATE feeds SET enabled = 0 WHERE id = ?", (ms_feed_id,))
        conn.execute(
            "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 0)",
            (site_id, ms_feed_id),
        )
        publication_id = _seed_publication(conn, site_id)
        _seed_one_row_per_table(
            conn,
            site_id=site_id,
            feed_id=ms_feed_id,
            publication_id=publication_id,
            local_date="2026-06-15",
            cutoff="2026-06-15T00:00:00Z",
            rank=1,
        )
        _seed_one_row_per_table(
            conn,
            site_id=site_id,
            feed_id=control_feed_id,
            publication_id=publication_id,
            local_date="2026-06-16",
            cutoff="2026-06-16T00:00:00Z",
            rank=2,
        )
        feeds_before = dict(
            conn.execute(
                "SELECT enabled, disabled_reason FROM feeds WHERE id = ?",
                (ms_feed_id,),
            ).fetchone()
        )
        state_before = dict(
            conn.execute(
                "SELECT enabled FROM site_feed_state WHERE site_id = ? AND feed_id = ?",
                (site_id, ms_feed_id),
            ).fetchone()
        )
        control_before = _all_feed_counts(conn, control_feed_id)
        conn.commit()

        caplog.clear()
        with caplog.at_level("INFO", logger="wxverify.db.migrations"):
            migrations.delete_pre_utc_meteosource(conn)

        ms_after = _all_feed_counts(conn, ms_feed_id)
        for table, count in ms_after.items():
            assert count == 0, (
                f"M8 -> at this assertion ({table}): correct = 0, mutant = {count}"
            )
        rain_pick = conn.execute(
            "SELECT feed_id FROM rain_picks WHERE site_id = ? AND cutoff = ?",
            (site_id, "2026-06-15T00:00:00Z"),
        ).fetchone()
        assert rain_pick is not None
        assert rain_pick["feed_id"] is None, (
            "M8 -> at this assertion (rain_picks): correct = None (cleared),"
            f" mutant = {rain_pick['feed_id']!r}"
        )

        control_after = _all_feed_counts(conn, control_feed_id)
        assert control_after == control_before, (
            "every control-feed row must survive the deletion untouched"
        )
        control_rain_pick = conn.execute(
            "SELECT feed_id FROM rain_picks WHERE site_id = ? AND cutoff = ?",
            (site_id, "2026-06-16T00:00:00Z"),
        ).fetchone()
        assert control_rain_pick is not None
        assert control_rain_pick["feed_id"] == control_feed_id

        feeds_after = dict(
            conn.execute(
                "SELECT enabled, disabled_reason FROM feeds WHERE id = ?",
                (ms_feed_id,),
            ).fetchone()
        )
        assert feeds_after == feeds_before
        state_after = dict(
            conn.execute(
                "SELECT enabled FROM site_feed_state WHERE site_id = ? AND feed_id = ?",
                (site_id, ms_feed_id),
            ).fetchone()
        )
        assert state_after == state_before

        assert get_runtime_state(conn, migrations.METEOSOURCE_UTC_FIX_KEY) is not None

        expected_tokens = [
            f"{table}=1"
            for table in migrations.METEOSOURCE_PRUNE_TABLES
            if table != "rain_picks"
        ] + ["rain_picks_cleared=1"]
        info_lines = [
            r.getMessage()
            for r in caplog.records
            if r.getMessage().startswith("meteosource utc fix:")
        ]
        assert len(info_lines) == 1
        for token in expected_tokens:
            assert token in info_lines[0], (
                f"M8 -> at this assertion (log line, {token}): correct = present,"
                f" mutant = {info_lines[0]!r}"
            )
    finally:
        close_db()


def test_t7_schema_pin_every_feed_id_table_is_named(tmp_path: Path) -> None:
    """Every table with a ``feed_id`` column is exactly the step's own
    table constant plus ``site_feed_state`` -- a table added to the schema
    with a ``feed_id`` column and left out of the constant shows up here."""
    conn = _init_tmp_db(tmp_path)
    try:
        table_names = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
                " AND name NOT LIKE 'sqlite_%'"
            )
        }
        with_feed_id = {
            table
            for table in table_names
            if any(
                row[1] == "feed_id"
                for row in conn.execute(f"PRAGMA table_info('{table}')")  # noqa: S608
            )
        }
        expected = set(migrations.METEOSOURCE_PRUNE_TABLES) | {"site_feed_state"}
        assert with_feed_id == expected, (
            "M8 -> at this assertion: correct ="
            f" {sorted(expected)!r}, mutant = {sorted(with_feed_id)!r}"
        )
    finally:
        close_db()


# ---------------------------------------------------------------------------
# T8: once the key is present, the step never runs again.
# ---------------------------------------------------------------------------


def test_t8_a_new_meteosource_row_survives_once_the_key_is_set(
    tmp_path: Path,
) -> None:
    """With ``METEOSOURCE_UTC_FIX_KEY`` present, a new Meteosource sample
    survives a second ``delete_pre_utc_meteosource`` call and a full
    ``run_migrations``; a fresh file has the key from its first open and
    logs no ``"meteosource utc fix:"`` line."""
    conn = _init_tmp_db(tmp_path)
    try:
        site_id = int(conn.execute(_SITE_SQL).lastrowid)
        ms_feed_id, _control_feed_id = _feed_ids(conn)
        conn.commit()

        # The key is already set by this (fresh) file's own first open
        # (`_record_fresh_build_keys`); the first call here is a no-op.
        assert get_runtime_state(conn, migrations.METEOSOURCE_UTC_FIX_KEY) is not None
        migrations.delete_pre_utc_meteosource(conn)

        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES
                (?, ?, 'temperature', '2026-06-20T00:00:00Z',
                 '2026-06-20T01:00:00Z', 1, 10.0, '10.0 C',
                 'synthetic-run', '2026-06-20T00:00:01Z')
            """,
            (site_id, ms_feed_id),
        )
        conn.commit()

        migrations.run_migrations(conn)

        count = _row_count(conn, "forecast_samples", ms_feed_id)
        assert count == 1, f"M9 -> at this assertion: correct = 1, mutant = {count}"
    finally:
        close_db()


def test_t8_a_fresh_file_has_the_key_and_logs_no_fix_line(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A fresh file's first ``run_migrations`` writes
    ``METEOSOURCE_UTC_FIX_KEY`` through the fresh-build keys, so
    ``delete_pre_utc_meteosource`` returns at once and logs nothing."""
    conn = sqlite3.connect(str(tmp_path / "fresh.db"), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    with caplog.at_level("INFO", logger="wxverify.db.migrations"):
        migrations.run_migrations(conn)

    assert get_runtime_state(conn, migrations.METEOSOURCE_UTC_FIX_KEY) is not None
    assert not any(
        r.getMessage().startswith("meteosource utc fix:") for r in caplog.records
    )


# ---------------------------------------------------------------------------
# T9: all or nothing.
# ---------------------------------------------------------------------------


def test_t9_a_failure_mid_delete_leaves_every_table_as_it_was(
    tmp_path: Path,
) -> None:
    """A failure raised while deleting ``rain_picks`` (the step's last
    table) rolls the whole deletion back: every earlier delete the step
    already ran is undone too. Dropping the trigger and calling again then
    deletes everything and sets the key."""
    conn = _init_tmp_db(tmp_path)
    try:
        conn.execute(
            "DELETE FROM runtime_state WHERE key = ?",
            (migrations.METEOSOURCE_UTC_FIX_KEY,),
        )
        site_id = int(conn.execute(_SITE_SQL).lastrowid)
        ms_feed_id, _control_feed_id = _feed_ids(conn)
        publication_id = _seed_publication(conn, site_id)
        _seed_one_row_per_table(
            conn,
            site_id=site_id,
            feed_id=ms_feed_id,
            publication_id=publication_id,
            local_date="2026-06-15",
            cutoff="2026-06-15T00:00:00Z",
            rank=1,
        )
        before = _all_feed_counts(conn, ms_feed_id)
        before_rain_pick = conn.execute(
            "SELECT feed_id FROM rain_picks WHERE site_id = ? AND cutoff = ?",
            (site_id, "2026-06-15T00:00:00Z"),
        ).fetchone()["feed_id"]
        conn.commit()

        conn.execute(
            """
            CREATE TEMP TRIGGER synthetic_rain_picks_abort
            BEFORE UPDATE ON rain_picks
            BEGIN SELECT RAISE(ABORT, 'synthetic');
            END
            """
        )

        with pytest.raises(sqlite3.DatabaseError):
            migrations.delete_pre_utc_meteosource(conn)

        after_failure = _all_feed_counts(conn, ms_feed_id)
        assert after_failure == before, (
            "M10 -> at this assertion: correct ="
            f" {before!r} (every delete rolled back), mutant = {after_failure!r}"
        )
        after_failure_rain_pick = conn.execute(
            "SELECT feed_id FROM rain_picks WHERE site_id = ? AND cutoff = ?",
            (site_id, "2026-06-15T00:00:00Z"),
        ).fetchone()["feed_id"]
        assert after_failure_rain_pick == before_rain_pick, (
            "M10 -> at this assertion: correct ="
            f" {before_rain_pick!r} (rain_picks row untouched), mutant ="
            f" {after_failure_rain_pick!r}"
        )
        assert get_runtime_state(conn, migrations.METEOSOURCE_UTC_FIX_KEY) is None

        conn.execute("DROP TRIGGER synthetic_rain_picks_abort")
        migrations.delete_pre_utc_meteosource(conn)

        after_retry = _all_feed_counts(conn, ms_feed_id)
        for table, count in after_retry.items():
            assert count == 0, f"table {table} not cleared on the retried call"
        after_retry_rain_pick = conn.execute(
            "SELECT feed_id FROM rain_picks WHERE site_id = ? AND cutoff = ?",
            (site_id, "2026-06-15T00:00:00Z"),
        ).fetchone()["feed_id"]
        assert after_retry_rain_pick is None
        assert get_runtime_state(conn, migrations.METEOSOURCE_UTC_FIX_KEY) is not None
    finally:
        close_db()
