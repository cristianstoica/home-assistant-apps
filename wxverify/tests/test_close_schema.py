"""Schema for daily-close piece 1 (plan 2026-10-21-daily-close-close-job.md,
section 12, test IDs SC3-SC5): the five ``close_*`` tables and the widened
``jobs`` CHECK, both now unconditional parts of ``create_tables``.
``migrate_v13_close_jobs``, the migration that originally added them to an
existing file, is deleted along with every other ``migrate_v2`` ...
``migrate_v16`` function (storage release 4.1) -- SC1 and SC2's migration-
mechanics tests retired with it; what remains (SC3-SC5) exercises the
tables' own CHECK/foreign-key rules directly. Section 16.2 of the plan
names the mutant each surviving test ID kills; that mapping is also given
inline on each assertion, in the file's own style.

Real SQLite and real migrations throughout; nothing in the code under test
is mocked. Synthetic data only: station ids ``ISTATION01``/``ISTATION02``
(via ``tests.helpers.day_close_site``), site name ``Testsite``, zone
``UTC``.
"""

from __future__ import annotations

import sqlite3

import pytest

from tests.helpers import D, asof_conn, day_close_site
from tests.test_migration_runner_pre_v4 import _connect  # noqa: PLC2701
from wxverify.db.migrations import create_tables

# --- SC1 ------------------------------------------------------------------
#
# A hand-crafted version-12 file (today's narrow jobs CHECK, the five
# close_* tables absent) used to stand in for a pre-piece-1 database here,
# built by migrating a fresh file up and then undoing piece 1's additions.
# That technique no longer has anything to undo onto a reachable schema:
# migrate_v13_close_jobs (and every other migrate_v2 ... migrate_v16
# function) is deleted, create_tables carries the widened jobs CHECK and
# the five close_* tables unconditionally, and user_version 1-15 is
# refused outright rather than migrated (storage release 4.1) -- so no
# test below builds one any more.


# test_sc1_fresh_and_migrated_files_agree_and_the_check_admits_both_kinds
# is retired: it proved a migrated version-12 file lands on the same shape
# as a fresh one, but migrate_v13_close_jobs (like every other migrate_v2
# ... migrate_v16 function) is deleted, and user_version 1-15 is now
# refused outright rather than migrated (storage release 4.1) -- there is
# no migrated-from-v12 path left to compare against a fresh file.


# --- SC1 (extension): the CHECK copy in isolation ---------------------------
#
# 12.2 names create_tables' own jobs DDL as one of the places that must
# carry the identical widened CHECK list. The fresh-vs-migrated comparison
# above cannot tell this apart from the copy reached via
# migrate_v13_close_jobs's rebuild, so this case drives create_tables alone
# on an empty file (M7c).


def _assert_jobs_check_admits_both_close_kinds_refuses_bogus(
    conn: sqlite3.Connection, *, mutant: str
) -> None:
    """Shared SC1-extension assertion: a day_close and a day_publish insert
    both land, a bogus_type insert is refused -- the twin that proves the
    acceptance comes from the CHECK admitting the two kinds, not from some
    other path around it.
    """
    site_id = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
    ).lastrowid
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status)"
        " VALUES ('day_close', ?, 'close:1:2026-07-06:2', 'pending')",
        (site_id,),
    )
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status)"
        " VALUES ('day_publish', ?, 'publish:1:7', 'pending')",
        (site_id,),
    )
    accepted = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type IN ('day_close', 'day_publish')"
        " AND site_id = ?",
        (site_id,),
    ).fetchone()[0]
    assert accepted == 2, (
        "mutant -> at this assertion: correct = 2 (this copy of the CHECK"
        f" admits both new kinds), mutant ({mutant}, 'day_publish' left out"
        " of this copy) = the day_publish INSERT above raises"
        " sqlite3.IntegrityError before this count is ever read"
    )
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO jobs (type, site_id, status)"
            " VALUES ('bogus_type', ?, 'pending')",
            (site_id,),
        )


def test_sc1_create_tables_copy_of_the_check_admits_both_kinds_alone() -> None:
    """SC1 extension: create_tables' own jobs DDL, alone, on an empty file.

    mutant -> at the shared assertion above: correct = 2, mutant (M7c,
    'day_publish' left out of create_tables' jobs CHECK) = the day_publish
    INSERT raises sqlite3.IntegrityError.
    """
    conn = _connect()
    create_tables(conn)
    _assert_jobs_check_admits_both_close_kinds_refuses_bogus(conn, mutant="M7c")


# test_sc1_the_rebuild_guard_checks_for_day_publish_not_day_close and
# test_sc2_a_fault_on_rename_leaves_v12_intact_and_the_rerun_converges are
# both retired: both call migrate_v13_close_jobs directly, and it is
# deleted along with every other migrate_v2 ... migrate_v16 function
# (storage release 4.1) -- create_tables carries the widened jobs CHECK
# and the five close_* tables unconditionally now, with no rebuild step
# left to guard or fault-inject.


# --- SC3 --------------------------------------------------------------------


def test_sc3_every_check_of_the_five_tables_refuses_its_bad_row() -> None:
    """SC3: one insert per CHECK constraint of 12.3, each row valid except
    for the one constraint it is built to break, and each refused with
    ``sqlite3.IntegrityError``. Catches a missing CHECK (M10a-M10k and
    beyond): with the targeted CHECK dropped, exactly that insert would
    succeed while the others still protect the row.

    The real defect this guards against: ``day_close_site``'s site,
    station and ``site_days`` rows are built on this same connection but
    never committed. ``_bad()``'s own ``conn.rollback()`` therefore rolls
    back not just the one failed insert but the WHOLE uncommitted
    transaction -- the fixture included -- the first time it runs. Every
    ``_bad()`` call after the first one then references a ``site_days``
    row (for ``close_attempts``/``close_station_days``/
    ``close_publications``) or a ``sites``/``stations`` row that no
    longer exists, so it is refused by a FOREIGN KEY violation
    regardless of whether the CHECK it targets is present or missing --
    a false pass for every CHECK but the very first one tried
    (``attempt IN (1,2)``, M10 at ``db/migrations.py:621``). ``_good()``
    below pins each table's baseline row as accepted (the paired
    positive), and committing the fixture up front keeps it in place
    across every later ``_bad()`` rollback.
    """
    conn = asof_conn()
    site_id = day_close_site(conn)
    station_id = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? ORDER BY id LIMIT 1",
        (site_id,),
    ).fetchone()[0]
    local_date = D.isoformat()
    stamp = "2026-07-06T00:00:00.000000Z"
    # Locks the fixture (site, station, site_days rows) in so that a later
    # _bad() call's conn.rollback() can never discard it -- see the
    # docstring above.
    conn.commit()

    def _bad(sql: str, params: tuple[object, ...]) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(sql, params)
        conn.rollback()

    def _good(sql: str, params: tuple[object, ...], *, table: str) -> None:
        """A row with every field valid must be ACCEPTED -- the paired
        positive proving the refusals below come from the one targeted
        CHECK and not from an FK, a NOT NULL, or the stale-rollback
        defect this test now avoids. Deleted by rowid right after (never
        rolled back), so it cannot collide with a later _bad() row that
        shares its primary key.
        """
        cur = conn.execute(sql, params)
        row = conn.execute(
            f"SELECT 1 FROM {table} WHERE rowid = ?",
            (cur.lastrowid,),  # noqa: S608
        ).fetchone()
        assert row is not None, (
            f"the {table} baseline row (every field valid) must be accepted"
        )
        conn.execute(f"DELETE FROM {table} WHERE rowid = ?", (cur.lastrowid,))  # noqa: S608
        conn.commit()

    # --- close_attempts: 7 CHECKs -----------------------------------------
    attempts_sql = (
        "INSERT INTO close_attempts (site_id, local_date, attempt,"
        " started_at, last_stage, last_progress_at, outcome, failed_stage,"
        " ended_at, truth_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    _good(
        attempts_sql,
        (site_id, local_date, 1, stamp, 1, stamp, "running", None, None, None),
        table="close_attempts",
    )
    # attempt IN (1,2)
    _bad(
        attempts_sql,
        (site_id, local_date, 3, stamp, 1, stamp, "running", None, None, None),
    )
    # last_stage BETWEEN 1 AND 6
    _bad(
        attempts_sql,
        (site_id, local_date, 1, stamp, 0, stamp, "running", None, None, None),
    )
    # outcome IN ('running','closed','failed')
    _bad(
        attempts_sql,
        (site_id, local_date, 1, stamp, 6, stamp, "bogus", None, stamp, None),
    )
    # failed_stage BETWEEN 2 AND 6 (set, so outcome must be 'failed' to keep
    # the running/ended_at and failed/failed_stage equalities satisfied)
    _bad(
        attempts_sql,
        (site_id, local_date, 1, stamp, 3, stamp, "failed", 99, stamp, None),
    )
    # (outcome = 'running') = (ended_at IS NULL): running with ended_at set
    _bad(
        attempts_sql,
        (site_id, local_date, 1, stamp, 1, stamp, "running", None, stamp, None),
    )
    # (outcome = 'failed') = (failed_stage IS NOT NULL): failed, no stage
    _bad(
        attempts_sql,
        (site_id, local_date, 1, stamp, 4, stamp, "failed", None, stamp, None),
    )
    # outcome <> 'closed' OR (last_stage = 6 AND truth_version NOT NULL)
    _bad(
        attempts_sql,
        (site_id, local_date, 1, stamp, 1, stamp, "closed", None, stamp, None),
    )

    # --- close_station_days: 6 CHECKs --------------------------------------
    station_days_sql = (
        "INSERT INTO close_station_days (site_id, local_date, station_id,"
        " five_minute, current_obs, hourly, answered, refused_lost, paused)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    _good(
        station_days_sql,
        (site_id, local_date, station_id, 0, 0, 0, 0, 0, 0),
        table="close_station_days",
    )
    _bad(station_days_sql, (site_id, local_date, station_id, -1, 0, 0, 0, 0, 0))
    _bad(station_days_sql, (site_id, local_date, station_id, 0, -1, 0, 0, 0, 0))
    _bad(station_days_sql, (site_id, local_date, station_id, 0, 0, -1, 0, 0, 0))
    _bad(station_days_sql, (site_id, local_date, station_id, 0, 0, 0, 2, 0, 0))
    _bad(station_days_sql, (site_id, local_date, station_id, 0, 0, 0, 0, 2, 0))
    _bad(station_days_sql, (site_id, local_date, station_id, 0, 0, 0, 0, 0, 2))

    # --- close_publications: 4 CHECKs --------------------------------------
    publications_sql = (
        "INSERT INTO close_publications (site_id, cutoff, attempt, state,"
        " opened_at, retry_at, finished_at, publication_id)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    _good(
        publications_sql,
        (site_id, local_date, 1, "pending", stamp, None, None, None),
        table="close_publications",
    )
    # attempt IN (1,2)
    _bad(
        publications_sql,
        (site_id, local_date, 3, "pending", stamp, None, None, None),
    )
    # state IN (...)
    _bad(
        publications_sql,
        (site_id, local_date, 1, "bogus", stamp, None, None, None),
    )
    # (state = 'published') = (publication_id IS NOT NULL)
    _bad(
        publications_sql,
        (site_id, local_date, 1, "published", stamp, None, None, None),
    )
    # (state = 'retry_due') = (retry_at NOT NULL AND attempt=1 AND
    # finished_at IS NULL)
    _bad(
        publications_sql,
        (site_id, local_date, 1, "retry_due", stamp, None, None, None),
    )

    # --- close_stage_log: 4 CHECKs ------------------------------------------
    stage_log_sql = (
        "INSERT INTO close_stage_log (site_id, local_date, attempt, stage,"
        " job_id, enqueued_at, started_at, ended_at, queue_wait_s,"
        " duration_s, outcome, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
        " ?, '{}')"
    )
    _good(
        stage_log_sql,
        (site_id, local_date, 1, 2, 10, stamp, stamp, stamp, 0.0, 0.0, "completed"),
        table="close_stage_log",
    )
    # attempt IN (1,2)
    _bad(
        stage_log_sql,
        (site_id, local_date, 3, 2, 10, stamp, stamp, stamp, 0.0, 0.0, "completed"),
    )
    # stage BETWEEN 1 AND 8
    _bad(
        stage_log_sql,
        (site_id, local_date, 1, 9, 10, stamp, stamp, stamp, 0.0, 0.0, "completed"),
    )
    # outcome IN (...)
    _bad(
        stage_log_sql,
        (site_id, local_date, 1, 2, 10, stamp, stamp, stamp, 0.0, 0.0, "bogus"),
    )
    # (stage = 1) = (job_id IS NULL): stage 1 with a job_id
    _bad(
        stage_log_sql,
        (site_id, local_date, 1, 1, 10, stamp, stamp, stamp, 0.0, 0.0, "completed"),
    )

    # --- close_try_log: 6 CHECKs ---------------------------------------------
    try_log_sql = (
        "INSERT INTO close_try_log (site_id, local_date, station_id, kind,"
        " slot, close_attempt, tried_at, outcome, readings_kept,"
        " readings_refused) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    _good(
        try_log_sql,
        (
            site_id,
            local_date,
            station_id,
            "five_minute",
            "daytime",
            None,
            stamp,
            "stored",
            0,
            0,
        ),
        table="close_try_log",
    )
    # kind IN ('five_minute','hourly')
    _bad(
        try_log_sql,
        (
            site_id,
            local_date,
            station_id,
            "bogus",
            "daytime",
            None,
            stamp,
            "stored",
            0,
            0,
        ),
    )
    # close_attempt IN (1,2), set via a valid gap_fill/hourly pairing
    _bad(
        try_log_sql,
        (
            site_id,
            local_date,
            station_id,
            "hourly",
            "gap_fill",
            3,
            stamp,
            "filled",
            0,
            0,
        ),
    )
    # readings_kept >= 0
    _bad(
        try_log_sql,
        (
            site_id,
            local_date,
            station_id,
            "five_minute",
            "daytime",
            None,
            stamp,
            "stored",
            -1,
            0,
        ),
    )
    # readings_refused >= 0
    _bad(
        try_log_sql,
        (
            site_id,
            local_date,
            station_id,
            "five_minute",
            "daytime",
            None,
            stamp,
            "stored",
            0,
            -1,
        ),
    )
    # (slot = 'gap_fill') = (kind = 'hourly'): gap_fill with five_minute,
    # close_attempt kept non-NULL so only this CHECK breaks
    _bad(
        try_log_sql,
        (
            site_id,
            local_date,
            station_id,
            "five_minute",
            "gap_fill",
            1,
            stamp,
            "filled",
            0,
            0,
        ),
    )
    # (slot = 'gap_fill') = (close_attempt IS NOT NULL): gap_fill with no
    # attempt, kind kept 'hourly' so only this CHECK breaks
    _bad(
        try_log_sql,
        (
            site_id,
            local_date,
            station_id,
            "hourly",
            "gap_fill",
            None,
            stamp,
            "filled",
            0,
            0,
        ),
    )


# --- SC4 --------------------------------------------------------------------


def test_sc4_station_delete_scopes_to_its_rows_site_delete_clears_all_five() -> None:
    """SC4: rows in every table for site S and stations A and B; deleting
    station A removes only its ``close_station_days`` row (``close_try_log``
    has no foreign key on ``station_id`` and is untouched); deleting the
    site removes every row of the five tables, with no foreign-key
    violation left behind.
    """
    conn = asof_conn()
    site_id = day_close_site(conn)
    station_rows = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? ORDER BY id", (site_id,)
    ).fetchall()
    station_a, station_b = station_rows[0]["id"], station_rows[1]["id"]
    local_date = D.isoformat()
    stamp = "2026-07-06T00:00:00.000000Z"

    conn.execute(
        "INSERT INTO close_attempts (site_id, local_date, attempt,"
        " started_at, last_stage, last_progress_at, outcome, ended_at,"
        " truth_version) VALUES (?, ?, 1, ?, 6, ?, 'closed', ?, 'abc123')",
        (site_id, local_date, stamp, stamp, stamp),
    )
    for station_id in (station_a, station_b):
        conn.execute(
            "INSERT INTO close_station_days (site_id, local_date,"
            " station_id, five_minute, current_obs, hourly, answered,"
            " refused_lost, paused) VALUES (?, ?, ?, 10, 10, 0, 1, 0, 0)",
            (site_id, local_date, station_id),
        )
    conn.execute(
        "INSERT INTO close_publications (site_id, cutoff, attempt, state,"
        " opened_at, started_at, finished_at, publication_id)"
        " VALUES (?, ?, 1, 'published', ?, ?, ?, 'pub-test')",
        (site_id, local_date, stamp, stamp, stamp),
    )
    conn.execute(
        "INSERT INTO close_stage_log (site_id, local_date, attempt, stage,"
        " job_id, enqueued_at, started_at, ended_at, queue_wait_s,"
        " duration_s, outcome, detail)"
        " VALUES (?, ?, 1, 6, 10, ?, ?, ?, 0.0, 0.0, 'completed', '{}')",
        (site_id, local_date, stamp, stamp, stamp),
    )
    conn.execute(
        "INSERT INTO close_try_log (site_id, local_date, station_id, kind,"
        " slot, close_attempt, tried_at, outcome, readings_kept,"
        " readings_refused) VALUES (?, ?, ?, 'five_minute', 'daytime', NULL,"
        " ?, 'stored', 3, 0)",
        (site_id, local_date, station_a, stamp),
    )
    conn.commit()

    conn.execute("DELETE FROM stations WHERE id = ?", (station_a,))
    conn.commit()

    remaining_station_days = {
        row["station_id"]
        for row in conn.execute(
            "SELECT station_id FROM close_station_days WHERE site_id = ?"
            " AND local_date = ?",
            (site_id, local_date),
        )
    }
    assert remaining_station_days == {station_b}, (
        "mutant -> at this assertion: correct = {station_b} (deleting"
        " station A cascades to its own close_station_days row only),"
        " mutant (M11, close_station_days.station_id without ON DELETE"
        " CASCADE) = station A's row survives the station delete"
    )
    for table, date_column in (
        ("close_attempts", "local_date"),
        ("close_publications", "cutoff"),
        ("close_stage_log", "local_date"),
    ):
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE site_id = ? AND {date_column} = ?",
            (site_id, local_date),
        ).fetchone()[0]
        assert count == 1
    try_log_count = conn.execute(
        "SELECT COUNT(*) FROM close_try_log WHERE site_id = ? AND local_date = ?",
        (site_id, local_date),
    ).fetchone()[0]
    assert try_log_count == 1, (
        "close_try_log.station_id carries no foreign key (12.3), so a"
        " station-only delete must leave its try row in place"
    )
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []

    conn.execute("DELETE FROM sites WHERE id = ?", (site_id,))
    conn.commit()

    for table in (
        "close_attempts",
        "close_station_days",
        "close_publications",
        "close_stage_log",
        "close_try_log",
    ):
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE site_id = ?", (site_id,)
        ).fetchone()[0]
        assert count == 0, (
            f"mutant -> at the {table} post-site-delete count: correct = 0"
            " (every row of the five tables cascades from the site),"
            " mutant (a missing cascade) = a row survives the site delete"
        )
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


# --- SC5 --------------------------------------------------------------------
#
# SC5's db-transfer-import tests (the two log tables' local_date check, and
# close_publications.cutoff's foreign-key coverage) tested only
# ``_validate_upload``, removed with the database import (storage-release
# plan 5.5). The one SC5 test that also checked a live migration path (an
# older version-12 file migrating the rest of the way on open) is retired
# too: a version-12 file is now refused outright rather than migrated
# (storage release 4.1), and a fresh or v16 file already carries the five
# close_* tables unconditionally via create_tables -- nothing migrated-
# from-v12-specific survives to test.
