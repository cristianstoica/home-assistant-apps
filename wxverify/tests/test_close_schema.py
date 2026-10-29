"""Schema and migration for daily-close piece 1 (plan
2026-10-21-daily-close-close-job.md, section 12, test IDs SC1-SC5).

Every test here is written before ``migrate_v13_close_jobs`` and the five
``close_*`` tables exist and fails first for that reason: the inline import
of ``wxverify.db.migrations.migrate_v13_close_jobs`` below raises
``ImportError``, and the five tables are absent from a freshly migrated
database until piece 1 lands. Section 16.2 of the plan names the mutant
each test ID must kill; that mapping is also given inline on each
assertion, in the file's own style.

Real SQLite and real migrations throughout; nothing in the code under test
is mocked. Synthetic data only: station ids ``ISTATION01``/``ISTATION02``
(via ``tests.helpers.day_close_site``), site name ``Testsite``, zone
``UTC``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tests.helpers import D, asof_conn, day_close_site
from tests.test_migration_runner_pre_v4 import (  # noqa: PLC2701
    HISTORICAL,
    _build_historical,
    _connect,
)
from wxverify.db.migrations import (
    TARGET_USER_VERSION,
    create_tables,
    migrate_v3,
    migrate_v4,
    run_migrations,
)

# ---------------------------------------------------------------------------
# A version-12 file: today's narrow jobs CHECK (no 'day_close'/'day_publish'),
# the five close_* tables absent, one seeded pending fetch_feed job (15.3
# preamble). Built by migrating a fresh on-disk file all the way up (harmless
# before piece 1 lands, since TARGET_USER_VERSION is still 12 then) and then
# undoing piece 1's additions, rather than hand-copying every historical
# migration -- mirrors tests/test_raw_table_schema.py's _v11_fixture_conn,
# which forces PRAGMA user_version back down the same way.
# ---------------------------------------------------------------------------

_V12_JOBS_DDL = """
    CREATE TABLE jobs_v12 (
        id INTEGER PRIMARY KEY,
        type TEXT NOT NULL,
        site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE,
        job_key TEXT,
        payload TEXT NOT NULL DEFAULT '{}',
        status TEXT NOT NULL DEFAULT 'pending'
            CHECK(status IN ('pending','running','completed','failed')),
        next_attempt_at TEXT,
        retry_count INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 5,
        last_error TEXT,
        result TEXT,
        created_at TEXT NOT NULL
            DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
        updated_at TEXT NOT NULL
            DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
        CHECK (
            (type = 'catchup' AND site_id IS NULL)
            OR (
                type IN ('fetch_feed','fetch_obs','fetch_current_obs',
                         'pair_and_score','backfill_site',
                         'forecast_record','record_gap_scan',
                         'verification_run','timezone_correction')
                AND site_id IS NOT NULL
            )
        )
    );
"""

_CLOSE_TABLES = (
    "close_attempts",
    "close_station_days",
    "close_publications",
    "close_stage_log",
    "close_try_log",
)


def _build_v12_file(path: Path) -> Path:
    """Write a version-12 database to ``path`` (15.3 preamble)."""
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    for table in _CLOSE_TABLES:
        conn.execute(f"DROP TABLE {table}")
    conn.execute("SAVEPOINT narrow_jobs")
    try:
        conn.execute(_V12_JOBS_DDL)
        conn.execute("INSERT INTO jobs_v12 SELECT * FROM jobs")
        conn.execute("DROP TABLE jobs")
        conn.execute("ALTER TABLE jobs_v12 RENAME TO jobs")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe"
            " ON jobs(type, COALESCE(site_id, -1), job_key)"
            " WHERE status IN ('pending','running') AND job_key IS NOT NULL"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_type_key_site"
            " ON jobs(type, job_key, site_id, id)"
        )
    except BaseException:
        conn.execute("ROLLBACK TO narrow_jobs")
        conn.execute("RELEASE narrow_jobs")
        raise
    conn.execute("RELEASE narrow_jobs")
    site_id = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
    ).lastrowid
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status)"
        " VALUES ('fetch_feed', ?, 'seed-fetch-feed', 'pending')",
        (site_id,),
    )
    conn.execute("PRAGMA user_version = 12")
    conn.commit()
    conn.close()
    return path


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


# --- SC1 ------------------------------------------------------------------


def test_sc1_fresh_and_migrated_files_agree_and_the_check_admits_both_kinds(
    tmp_path: Path,
) -> None:
    """SC1: a fresh file and a migrated version-12 file land on the same
    ``user_version``, the same ``jobs``/five-table shapes and indexes, admit
    both new job kinds and refuse a bogus one, and keep the seeded job.
    """
    fresh_conn = sqlite3.connect(":memory:")
    fresh_conn.row_factory = sqlite3.Row
    fresh_conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(fresh_conn)

    v12_path = _build_v12_file(tmp_path / "v12.db")
    migrated_conn = sqlite3.connect(str(v12_path))
    migrated_conn.row_factory = sqlite3.Row
    migrated_conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(migrated_conn)

    for conn in (fresh_conn, migrated_conn):
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == TARGET_USER_VERSION, (
            "mutant -> at this assertion: correct = user_version read from"
            " TARGET_USER_VERSION, never a literal, since P4 and P6 raise"
            " it further; mutant (M9, the early-return tests for"
            " 'day_close' only, one kind added without the other) ="
            " the narrow CHECK partially widened and TARGET_USER_VERSION"
            " never reached on the migrated file"
        )

    for table in ("jobs", *_CLOSE_TABLES):
        fresh_shape = _table_shape(fresh_conn, table)
        migrated_shape = _table_shape(migrated_conn, table)
        assert fresh_shape == migrated_shape, (
            f"mutant -> at the {table} table_xinfo comparison: correct ="
            " identical column lists on both paths, mutant (M7, "
            "'day_publish' left out of migrate_v3's jobs_new) = the two"
            " jobs shapes agree but one path's CHECK text differs"
        )
        fresh_idx = _table_indexes(fresh_conn, table)
        migrated_idx = _table_indexes(migrated_conn, table)
        assert fresh_idx == migrated_idx, (
            f"mutant -> at the {table} index-name comparison: correct ="
            " the same index names on both paths, mutant (an index lost"
            " in the rebuild) = the migrated file missing one"
        )

    for conn in (fresh_conn, migrated_conn):
        site_id = conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon,"
            " elevation_m, timezone) VALUES ('Testsite', 0.0, 0.0, 0.0,"
            " 'UTC')"
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
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO jobs (type, site_id, status)"
                " VALUES ('bogus_type', ?, 'pending')",
                (site_id,),
            )
        conn.rollback()

    seeded = migrated_conn.execute(
        "SELECT type, status FROM jobs WHERE job_key = 'seed-fetch-feed'"
    ).fetchone()
    assert seeded is not None and seeded["type"] == "fetch_feed", (
        "mutant -> at this assertion: correct = the pre-existing fetch_feed"
        " job survives the jobs rebuild, mutant (INSERT INTO jobs_new"
        " SELECT * FROM jobs dropped from migrate_v13_close_jobs) = the"
        " seeded row lost"
    )
    assert seeded["status"] == "pending"


# --- SC1 (extension): each single copy of the CHECK, in isolation ----------
#
# 12.2 names three places that must carry the identical widened CHECK list:
# create_tables' own jobs DDL, migrate_v3's jobs_new rebuild and migrate_v4's
# jobs_new rebuild. The fresh-vs-migrated comparison above cannot tell these
# apart: on a fresh file migrate_v4 returns at its guard and
# migrate_v13_close_jobs rebuilds jobs wide whatever the other two say, so a
# single copy left narrow would not show up there (plan 12.2 item 2). Each
# case below drives exactly one copy and no other: create_tables alone on an
# empty file (M7c); migrate_v3 alone on the v2 file of HISTORICAL (M7);
# migrate_v4 alone on the v3-late file of HISTORICAL (M7b). create_tables is
# called first in the v2/v3-late cases only to backfill tables the bare
# historical schema lacks that the targeted migration writes into
# (station_poll_state for migrate_v3, timezone_generations for migrate_v4);
# jobs itself already exists from the historical schema, so create_tables'
# own `CREATE TABLE IF NOT EXISTS jobs` is a no-op there and only the
# targeted migration's rebuild can widen the CHECK.


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


def test_sc1_migrate_v3_copy_of_the_check_admits_both_kinds_alone() -> None:
    """SC1 extension: migrate_v3's jobs_new rebuild, alone, on the v2 file of
    HISTORICAL.

    mutant -> at the shared assertion above: correct = 2, mutant (M7,
    'day_publish' left out of migrate_v3's jobs_new) = the day_publish
    INSERT raises sqlite3.IntegrityError.
    """
    conn = _connect()
    schema_sql, user_version = HISTORICAL["v2"]
    _build_historical(conn, schema_sql, user_version)
    create_tables(conn)
    migrate_v3(conn)
    _assert_jobs_check_admits_both_close_kinds_refuses_bogus(conn, mutant="M7")


def test_sc1_migrate_v4_copy_of_the_check_admits_both_kinds_alone() -> None:
    """SC1 extension: migrate_v4's jobs_new rebuild, alone, on the v3-late
    file of HISTORICAL.

    mutant -> at the shared assertion above: correct = 2, mutant (M7b,
    'day_publish' left out of migrate_v4's jobs_new) = the day_publish
    INSERT raises sqlite3.IntegrityError.
    """
    conn = _connect()
    schema_sql, user_version = HISTORICAL["v3-late"]
    _build_historical(conn, schema_sql, user_version)
    create_tables(conn)
    migrate_v4(conn)
    _assert_jobs_check_admits_both_close_kinds_refuses_bogus(conn, mutant="M7b")


# --- SC1 (extension): the re-run guard checks 'day_publish', not 'day_close' -
#
# The three call points that ever write this CHECK (create_tables,
# migrate_v3, migrate_v4) always add 'day_close' and 'day_publish' together,
# so on every reachable real-world schema the two literals co-occur and
# migrate_v13_close_jobs's own re-run guard cannot be told, by any test that
# only drives those three functions, whether it tests for 'day_close' or for
# 'day_publish' (M9: the guard tests for 'day_close' only). This test builds
# the one state no production code path can reach but a future historical
# migration could -- a jobs CHECK naming 'day_close' alone -- so the two
# literals finally diverge.


def test_sc1_the_rebuild_guard_checks_for_day_publish_not_day_close(
    tmp_path: Path,
) -> None:
    """SC1 extension: a ``jobs`` CHECK naming ``'day_close'`` but not
    ``'day_publish'`` (a state no real migration leaves behind today, but
    the guard must still tell the two literals apart rather than relying
    on them always co-occurring).

    M9 -> at the day_publish-insert assertion below: correct = the insert
    is accepted (``'day_publish'`` is absent, so the real guard's
    ``"'day_publish'" in str(row[0])`` is False and the rebuild runs,
    widening the CHECK for both kinds), mutant M9 (the guard reads
    ``"'day_close'" in str(row[0])`` instead, ``db/migrations.py:2220``)
    = ``sqlite3.IntegrityError`` (``'day_close'`` is present, so the
    mutant's guard returns at once and the CHECK is never widened for
    ``day_publish``).
    """
    from wxverify.db.migrations import migrate_v13_close_jobs

    _ASYMMETRIC_JOBS_DDL = """
        CREATE TABLE jobs_asym (
            id INTEGER PRIMARY KEY,
            type TEXT NOT NULL,
            site_id INTEGER REFERENCES sites(id) ON DELETE CASCADE,
            job_key TEXT,
            payload TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending','running','completed','failed')),
            next_attempt_at TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 5,
            last_error TEXT,
            result TEXT,
            created_at TEXT NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            updated_at TEXT NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            CHECK (
                (type = 'catchup' AND site_id IS NULL)
                OR (
                    type IN ('fetch_feed','fetch_obs','fetch_current_obs',
                             'pair_and_score','backfill_site',
                             'forecast_record','record_gap_scan',
                             'verification_run','timezone_correction',
                             'day_close')
                    AND site_id IS NOT NULL
                )
            )
        );
    """

    v12_path = _build_v12_file(tmp_path / "v12.db")
    conn = sqlite3.connect(str(v12_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("SAVEPOINT make_asymmetric")
    conn.execute(_ASYMMETRIC_JOBS_DDL)
    conn.execute("INSERT INTO jobs_asym SELECT * FROM jobs")
    conn.execute("DROP TABLE jobs")
    conn.execute("ALTER TABLE jobs_asym RENAME TO jobs")
    conn.execute("RELEASE make_asymmetric")
    site_id = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
    ).lastrowid
    before = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'"
    ).fetchone()[0]
    assert "'day_close'" in before and "'day_publish'" not in before, (
        "fixture bug: the asymmetric jobs table must name day_close without"
        " day_publish before the guard runs"
    )
    conn.commit()

    migrate_v13_close_jobs(conn)

    # M9 -> at this insert: correct = accepted, no exception ('day_publish'
    # was absent before the call, so the real guard ran the rebuild and
    # widened the CHECK for both kinds), mutant M9 (the guard checks
    # "'day_close'" instead, which was already present) = sqlite3.
    # IntegrityError (the rebuild never ran; day_publish is still refused).
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status)"
        " VALUES ('day_publish', ?, 'publish:1:7', 'pending')",
        (site_id,),
    )
    conn.commit()


# --- SC2 --------------------------------------------------------------------


def test_sc2_a_fault_on_rename_leaves_v12_intact_and_the_rerun_converges(
    tmp_path: Path,
) -> None:
    """SC2: a fault on the rebuild's final ``RENAME TO jobs`` leaves version
    12, the old ``jobs`` and its seeded job, with no orphan ``jobs_new``; the
    rerun then reaches ``TARGET_USER_VERSION``, and an extra direct call
    changes nothing.
    """
    from wxverify.db.migrations import migrate_v13_close_jobs

    v12_path = _build_v12_file(tmp_path / "v12.db")
    conn = sqlite3.connect(str(v12_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    def _authorizer(
        action: int,
        arg1: str | None,
        arg2: str | None,
        db: str | None,
        trigger: str | None,
    ) -> int:
        if action == sqlite3.SQLITE_ALTER_TABLE and arg2 == "jobs_new":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    conn.set_authorizer(_authorizer)
    with pytest.raises(sqlite3.DatabaseError):
        run_migrations(conn)
    conn.set_authorizer(None)

    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == 12, (
        "mutant -> at this assertion: correct = 12 (the fault leaves the"
        " fixture's own version; run_migrations writes user_version only"
        " at its last line), mutant (M8, migrate_v13_close_jobs's rebuild"
        " run outside its savepoint) = the version advances to 13 even"
        " though the RENAME never completed"
    )
    seeded = conn.execute(
        "SELECT type FROM jobs WHERE job_key = 'seed-fetch-feed'"
    ).fetchone()
    assert seeded is not None and seeded["type"] == "fetch_feed"
    orphan = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs_new'"
    ).fetchone()
    assert orphan is None, (
        "mutant -> at this assertion: correct = no orphan jobs_new survives"
        " the rollback (M8), mutant (ROLLBACK TO the savepoint dropped from"
        " the except clause) = jobs_new left behind"
    )

    run_migrations(conn)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == TARGET_USER_VERSION, (
        "mutant -> at this assertion: correct = the rerun reaches"
        " TARGET_USER_VERSION (M8, a step not safe to repeat would stop"
        " short here instead)"
    )

    before_shape = _table_shape(conn, "jobs")
    before_count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    migrate_v13_close_jobs(conn)
    after_shape = _table_shape(conn, "jobs")
    after_count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    assert after_shape == before_shape
    assert after_count == before_count, (
        "mutant -> at this assertion: correct = a direct extra call to"
        " migrate_v13_close_jobs changes nothing (its own"
        " already-contains-'day_publish' re-run guard), mutant (the"
        " early-return guard dropped) = the jobs table is rebuilt again,"
        " changing row count or shape"
    )


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


_SC5_STAMP = "2026-07-06T00:00:00.000000Z"

# The two close_* columns P1's per-column date list actually covers
# (close_stage_log.local_date, close_try_log.local_date): the row values
# besides the date column itself, keyed by table (12.6 bullet 2; the three
# record tables -- close_attempts, close_station_days, close_publications --
# are covered by their foreign key to site_days instead, never by this list,
# and are not parametrised here).
_SC5_LOG_ROW: dict[str, dict[str, object]] = {
    "close_stage_log": {
        "local_date": None,  # filled in per case
        "attempt": 1,
        "stage": 1,
        "job_id": None,
        "enqueued_at": _SC5_STAMP,
        "started_at": _SC5_STAMP,
        "ended_at": _SC5_STAMP,
        "queue_wait_s": 0.0,
        "duration_s": 0.0,
        "outcome": "completed",
        "detail": "{}",
    },
    "close_try_log": {
        "local_date": None,  # filled in per case
        "station_id": 1,
        "kind": "five_minute",
        "slot": "daytime",
        "close_attempt": None,
        "tried_at": _SC5_STAMP,
        "outcome": "stored",
        "readings_kept": 0,
        "readings_refused": 0,
    },
}


def _upload_db_with_close_log_date(path: Path, *, table: str, date_value: str) -> None:
    """A migrated file with a real ``sites`` row and one row in ``table``
    (``close_stage_log`` or ``close_try_log``) whose ``local_date`` is
    ``date_value``, ``site_id`` pointing at that real row; every other
    column holds a valid, CHECK-satisfying value.

    Both log tables carry ``site_id REFERENCES sites(id)``, and
    ``_validate_upload`` runs ``PRAGMA foreign_key_check`` -- which checks
    every foreign key whatever the connection's own ``foreign_keys``
    setting -- before its date loop (12.6). A fixture with no matching
    ``sites`` row would therefore be refused by the foreign-key message
    before the date check under test here ever ran; the real row here
    breaks nothing except the one date value.
    """
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
    ).lastrowid
    row = dict(_SC5_LOG_ROW[table])
    row["site_id"] = site_id
    row["local_date"] = date_value
    columns = list(row)
    placeholders = ", ".join("?" for _ in columns)
    conn.execute(
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
        [row[c] for c in columns],
    )
    conn.commit()
    conn.close()


@pytest.mark.parametrize("table", ["close_stage_log", "close_try_log"])
def test_sc5_the_two_log_tables_date_column_is_checked_at_import(
    tmp_path: Path, table: str
) -> None:
    """SC5: a bad ``local_date`` in each of the two log tables P1's
    per-column date list actually covers is refused with P1's date message
    at 422 (12.6); a real matching ``sites`` row keeps the foreign-key
    check out of the way, so only the date value is under test.

    For ``close_try_log`` -> at the ``excinfo`` assertions below: correct =
    ApiError(422) with "invalid date in close_try_log.local_date: expected
    YYYY-MM-DD", mutant (M12, the date check skipped for
    close_try_log.local_date) = ``_validate_upload`` does not raise.
    """
    from wxverify.api.errors import ApiError
    from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701

    bad_path = tmp_path / "bad.db"
    _upload_db_with_close_log_date(bad_path, table=table, date_value="2026/07/06")

    with pytest.raises(ApiError) as excinfo:
        _validate_upload(bad_path)
    assert excinfo.value.status_code == 422
    assert (
        f"invalid date in {table}.local_date: expected YYYY-MM-DD"
        in excinfo.value.message
    )


@pytest.mark.parametrize("table", ["close_stage_log", "close_try_log"])
def test_sc5_the_two_log_tables_good_date_twin_is_accepted(
    tmp_path: Path, table: str
) -> None:
    """SC5 twin of the test above: the identical fixture with a
    canonical-form date passes ``_validate_upload`` -- proving the refusal
    above comes from the date value itself, not from anything else the
    fixture sets up (the real ``sites`` row, the FK check, or the row's
    other columns).
    """
    from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701

    good_path = tmp_path / "good.db"
    _upload_db_with_close_log_date(good_path, table=table, date_value="2026-07-06")

    _validate_upload(good_path)  # must not raise


def test_sc5_close_publications_cutoff_is_covered_by_the_site_days_fk_not_the_date_list(
    tmp_path: Path,
) -> None:
    """SC5: a bad date in ``close_publications.cutoff``, with no
    ``site_days`` row whose ``(site_id, local_date)`` matches it, is
    refused by the foreign-key check, never by a per-column date check --
    12.6 bullet 2: the three record tables (``close_attempts``,
    ``close_station_days``, ``close_publications``) are covered by their
    foreign key to ``site_days``, not by the date list, and
    ``_validate_upload`` runs ``PRAGMA foreign_key_check`` (:988-992)
    before the date loop (:1082).
    """
    from wxverify.api.errors import ApiError
    from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701

    path = tmp_path / "bad-cutoff.db"
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    site_id = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
    ).lastrowid
    # No site_days row is inserted at all, so no (site_id, local_date) pair
    # can match this cutoff, bad-looking or not.
    conn.execute(
        "INSERT INTO close_publications (site_id, cutoff, attempt, state,"
        " opened_at) VALUES (?, ?, 1, 'pending', ?)",
        (site_id, "2026/07/06", _SC5_STAMP),
    )
    conn.commit()
    conn.close()

    with pytest.raises(ApiError) as excinfo:
        _validate_upload(path)
    assert excinfo.value.status_code == 422
    assert excinfo.value.message == "database failed foreign key check", (
        "mutant -> at this assertion: correct = 'database failed foreign"
        " key check' (close_publications.cutoff has no per-column date"
        " check of its own -- only the composite foreign key to"
        " site_days(site_id, local_date) catches a bad value here), mutant"
        " (a per-column date check added for close_publications.cutoff) ="
        " excinfo.value.message reads the date message"
        " 'invalid date in close_publications.cutoff: expected YYYY-MM-DD'"
        " instead"
    )


def test_sc5_a_version_12_file_without_the_five_tables_is_accepted(
    tmp_path: Path,
) -> None:
    """SC5: a real version-12 file -- narrow ``jobs`` CHECK, the five
    close_* tables absent, the pre-existing ``fetch_feed`` job seeded (the
    15.3 preamble fixture, built by ``_build_v12_file``) -- is accepted by
    ``_validate_upload`` and then migrates the rest of the way on open
    (12.6).
    """
    from wxverify.api.routes.db_transfer import _validate_upload  # noqa: PLC2701

    path = tmp_path / "older.db"
    _build_v12_file(path)

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
        " version-12 file outright) = this file never reaches"
        " TARGET_USER_VERSION here"
    )
    names = {
        str(row["name"])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    for table in _CLOSE_TABLES:
        assert table in names, (
            f"mutant -> at this assertion: correct = {table} present after"
            " migrating the older file (migrate_v13_close_jobs's"
            " create_tables call in run_migrations creates it), mutant (a"
            " close_* table not created for a pre-this-release upload) ="
            f" {table} missing from names"
        )
