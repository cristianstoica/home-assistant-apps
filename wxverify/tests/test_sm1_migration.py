"""Tests for S-M1: schema v3 migration (jobs CHECK widening + station poll-state).

Bucket-1-E: schema-migration happy-path (v2→v3) and crash-recovery regression.
Bucket-1-H: fetch_current_obs enqueue on a migrated v3 DB.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from wxverify import config
from wxverify.db.connection import close_db, init_db
from wxverify.db.migrations import (
    POLL_SEED_STAGGER_SECONDS,
    TARGET_USER_VERSION,
    create_schema,
    migrate_v3,
    run_migrations,
    seed_default_feeds,
    seed_default_settings,
    seed_default_sources,
)

# ---------------------------------------------------------------------------
# Helpers: build a v2-era DB in raw SQLite (old jobs CHECK, no station tables)
# ---------------------------------------------------------------------------

_V2_JOBS_DDL = """\
CREATE TABLE jobs (
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
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    CHECK (
        (type = 'catchup' AND site_id IS NULL)
        OR (
            type IN ('fetch_feed','fetch_obs','pair_and_score','backfill_site')
            AND site_id IS NOT NULL
        )
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe
    ON jobs(type, COALESCE(site_id, -1), job_key)
    WHERE status IN ('pending','running') AND job_key IS NOT NULL;
"""

_SITES_DDL = """\
CREATE TABLE sites (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    forecast_lat REAL NOT NULL,
    forecast_lon REAL NOT NULL,
    elevation_m REAL NOT NULL,
    timezone TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    rain_threshold_mm REAL NOT NULL DEFAULT 0.2 CHECK(rain_threshold_mm >= 0),
    last_obs_at TEXT,
    backfill_status TEXT NOT NULL DEFAULT 'pending'
        CHECK(backfill_status IN ('pending','in_progress','complete')),
    backfill_through TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""

_STATIONS_DDL = """\
CREATE TABLE stations (
    id INTEGER PRIMARY KEY,
    site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
    pws_station_id TEXT NOT NULL UNIQUE,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    dem_elevation_m REAL NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    last_run_at TEXT,
    last_error TEXT,
    error_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""


def _build_v2_db(db_path: Path) -> sqlite3.Connection:
    """Create a minimal v2 DB with old jobs CHECK, pragmas, and user_version=2."""
    raw = sqlite3.connect(str(db_path), isolation_level=None)
    raw.row_factory = sqlite3.Row
    raw.execute("PRAGMA journal_mode=WAL")
    raw.execute("PRAGMA foreign_keys=ON")
    raw.executescript(_SITES_DDL)
    raw.executescript(_STATIONS_DDL)
    raw.executescript(_V2_JOBS_DDL)
    raw.execute("PRAGMA user_version = 2")
    return raw


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        is not None
    )


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _jobs_check_admits(conn: sqlite3.Connection, job_type: str, site_id: int) -> bool:
    """Return True if the CHECK on jobs allows an INSERT of the given type."""
    try:
        conn.execute(
            "INSERT INTO jobs (type, site_id, job_key) VALUES (?, ?, ?)",
            (job_type, site_id, f"probe-{job_type}-{site_id}"),
        )
        conn.execute(
            "DELETE FROM jobs WHERE type=? AND site_id=?",
            (job_type, site_id),
        )
        return True
    except sqlite3.IntegrityError:
        return False


# ---------------------------------------------------------------------------
# Bucket-1-E, test 1: happy-path v2→v3 upgrade
# ---------------------------------------------------------------------------


def test_v2_to_v3_migration_happy_path(tmp_path: Path) -> None:
    """v2 DB with stations+jobs upgrades cleanly to v3; all post-conditions hold."""
    db_path = tmp_path / "wxverify-v2.db"
    raw = _build_v2_db(db_path)

    # Seed one site and one station.
    raw.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('SITE-A', 47.0, 25.0, 900.0, 'UTC')"
    )
    site_id = int(raw.execute("SELECT last_insert_rowid()").fetchone()[0])
    raw.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)"
        " VALUES (?, 'ISTATION01', 47.0, 25.0, 900.0)",
        (site_id,),
    )
    # Seed a pending job to survive the rebuild.
    raw.execute(
        "INSERT INTO jobs (type, site_id, job_key, status)"
        " VALUES ('fetch_feed', ?, 'feed-probe', 'pending')",
        (site_id,),
    )
    pre_job_id = int(raw.execute("SELECT last_insert_rowid()").fetchone()[0])
    raw.close()

    # Run migrations via init_db (matches the production boot path).
    close_db()
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001

    # 1. user_version reaches TARGET.
    assert conn.execute("PRAGMA user_version").fetchone()[0] == TARGET_USER_VERSION

    # 2. New tables exist.
    assert _table_exists(conn, "station_poll_state"), (
        "station_poll_state must be created by S-M1"
    )
    assert _table_exists(conn, "station_current_obs"), (
        "station_current_obs must be created by S-M1"
    )

    # 3. Widened CHECK admits fetch_current_obs (positive).
    assert _jobs_check_admits(conn, "fetch_current_obs", site_id), (
        "jobs CHECK must admit fetch_current_obs after v3 migration"
    )

    # 4. Bogus type still rejected (paired negative — keeps the oracle honest).
    assert not _jobs_check_admits(conn, "bogus_type", site_id), (
        "jobs CHECK must still reject unknown job types"
    )

    # 5. Pre-existing queued job survived the jobs table rebuild.
    surviving = conn.execute(
        "SELECT id, type, status FROM jobs WHERE id=?", (pre_job_id,)
    ).fetchone()
    assert surviving is not None, (
        "pre-existing pending job must survive the jobs rebuild"
    )
    assert surviving["type"] == "fetch_feed"
    assert surviving["status"] == "pending"

    # 6. One station_poll_state row seeded per station.
    poll_rows = conn.execute(
        "SELECT station_id, next_poll_at FROM station_poll_state ORDER BY station_id"
    ).fetchall()
    station_ids = [
        r["id"] for r in conn.execute("SELECT id FROM stations ORDER BY id").fetchall()
    ]
    assert len(poll_rows) == len(station_ids), (
        "migrate_v3 must seed exactly one poll-state row per station"
    )
    for poll_row, station_id in zip(poll_rows, station_ids, strict=True):
        assert poll_row["station_id"] == station_id
        assert poll_row["next_poll_at"] is not None, (
            f"station {station_id} must have next_poll_at set"
        )


def test_v2_to_v3_stagger_is_monotonic_for_multiple_stations(tmp_path: Path) -> None:
    """migrate_v3 fans out next_poll_at with a positive stagger per station."""
    db_path = tmp_path / "wxverify-v2-multi.db"
    raw = _build_v2_db(db_path)

    raw.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('SITE-MULTI', 47.0, 25.0, 900.0, 'UTC')"
    )
    site_id = int(raw.execute("SELECT last_insert_rowid()").fetchone()[0])
    for i in range(3):
        raw.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)"
            " VALUES (?, ?, 47.0, 25.0, 900.0)",
            (site_id, f"ISTATION0{i + 1}"),
        )
    raw.close()

    close_db()
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001

    rows = conn.execute(
        "SELECT next_poll_at FROM station_poll_state ORDER BY station_id"
    ).fetchall()
    assert len(rows) == 3

    # next_poll_at values must be strictly increasing (POLL_SEED_STAGGER_SECONDS apart).
    times = [
        datetime.fromisoformat(r["next_poll_at"].replace("Z", "+00:00")).astimezone(UTC)
        for r in rows
    ]
    for earlier, later in zip(times, times[1:], strict=False):
        diff_seconds = (later - earlier).total_seconds()
        assert diff_seconds == pytest.approx(POLL_SEED_STAGGER_SECONDS, abs=1), (
            f"stagger must be {POLL_SEED_STAGGER_SECONDS}s between consecutive rows;"
            f" got {diff_seconds}s"
        )


# ---------------------------------------------------------------------------
# Bucket-1-E, test 2: crash-recovery regression (SAVEPOINT correctness)
# ---------------------------------------------------------------------------


class _FaultConn(sqlite3.Connection):
    """sqlite3.Connection subclass that faults on a specific SQL statement.

    ``fault_on`` is compared (case-insensitive substring) against the SQL
    string passed to ``execute``.  On the first match it raises
    ``sqlite3.OperationalError``; subsequent calls pass through normally.
    This lets us inject a hard fault at a precise point in migrate_v3's
    rebuild sequence without monkeypatching a read-only C method.
    """

    def __init__(self, *args: object, fault_on: str, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[call-arg]
        self._fault_on = fault_on.upper()
        self._faulted = False

    def execute(self, sql: str, parameters: object = (), /) -> sqlite3.Cursor:  # type: ignore[override]
        if not self._faulted and self._fault_on in sql.upper():
            self._faulted = True
            raise sqlite3.OperationalError(
                f"_FaultConn: injected fault on statement matching {self._fault_on!r}"
            )
        return super().execute(sql, parameters)  # type: ignore[call-arg]


def _build_v2_db_with_raw_conn(db_path: Path) -> None:
    """Write a v2 DB to disk using a plain connection, then close it."""
    raw = _build_v2_db(db_path)
    raw.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('CRASH-SITE', 47.0, 25.0, 900.0, 'UTC')"
    )
    site_id = int(raw.execute("SELECT last_insert_rowid()").fetchone()[0])
    raw.execute(
        "INSERT INTO jobs (type, site_id, job_key, status)"
        " VALUES ('fetch_feed', ?, 'survives-crash', 'pending')",
        (site_id,),
    )
    raw.close()


def _open_v2_fault_conn(db_path: Path, *, fault_on: str) -> _FaultConn:
    """Open a _FaultConn to an existing DB with WAL + foreign_keys."""
    conn: _FaultConn = sqlite3.connect(  # type: ignore[call-overload]
        str(db_path),
        isolation_level=None,
        factory=_FaultConn,
        fault_on=fault_on,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def test_migrate_v3_crash_recovery_regression(tmp_path: Path) -> None:
    """SAVEPOINT ensures atomicity: a fault mid-rebuild leaves a clean v2 DB.

    This is the regression oracle for the 'executescript implicit COMMIT' bug.
    Without the SAVEPOINT, a fault after DROP TABLE jobs would orphan jobs_new
    and destroy the queued job.  With the SAVEPOINT, ROLLBACK TO restores the
    original state and the DB is ready for a clean retry.

    The test then retries run_migrations without the fault and asserts full
    success — proving the crash left a retryable v2 state, not a corrupted one.
    """
    db_path = tmp_path / "wxverify-crash.db"
    _build_v2_db_with_raw_conn(db_path)

    # ---- Phase 1: fault partway through the rebuild (RENAME = worst window) ----
    fault_conn = _open_v2_fault_conn(db_path, fault_on="RENAME TO jobs")

    # create_schema runs first (via executescript; _FaultConn.execute is not hit
    # because executescript bypasses our override — that is fine, the schema
    # tables don't exist yet and get created cleanly by the real executescript).
    # Then migrate_v3 opens a SAVEPOINT and starts the rebuild via .execute().
    # The fault fires on the ALTER TABLE jobs_new RENAME TO jobs statement.
    create_schema(fault_conn)
    seed_default_sources(fault_conn)
    seed_default_feeds(fault_conn)
    seed_default_settings(fault_conn)

    with pytest.raises(sqlite3.OperationalError, match="_FaultConn"):
        migrate_v3(fault_conn)

    # After the rollback the connection is still open (SAVEPOINT was released).
    # We can interrogate it directly.

    # user_version must still be 2 (never bumped — run_migrations aborts on raise).
    version = fault_conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == 2, (
        f"user_version must remain 2 after a faulted migrate_v3; got {version}"
    )

    # jobs table must still be present and intact (SAVEPOINT rolled back the DROP).
    assert _table_exists(fault_conn, "jobs"), (
        "jobs table must survive a mid-rebuild crash (SAVEPOINT rollback)"
    )
    surviving = fault_conn.execute(
        "SELECT type, status, job_key FROM jobs WHERE job_key='survives-crash'"
    ).fetchone()
    assert surviving is not None, (
        "the pending job must survive the crash — SAVEPOINT must have rolled it back"
    )
    assert surviving["type"] == "fetch_feed"
    assert surviving["status"] == "pending"

    # No orphan jobs_new must linger (the SAVEPOINT rollback removed it).
    assert not _table_exists(fault_conn, "jobs_new"), (
        "jobs_new must not exist after a rolled-back migrate_v3 "
        "(without SAVEPOINT it would be orphaned here)"
    )

    fault_conn.close()

    # ---- Phase 2: clean retry on the same (now clean v2) DB ----
    # This is the critical half: the crash must have left a retryable state.
    # Without the SAVEPOINT fix, jobs_new would still be there and the retry
    # would fail with "table jobs_new already exists".
    retry_conn = sqlite3.connect(str(db_path), isolation_level=None)
    retry_conn.row_factory = sqlite3.Row
    retry_conn.execute("PRAGMA journal_mode=WAL")
    retry_conn.execute("PRAGMA foreign_keys=ON")

    # Simulate what Database._run_immediate does: wrap in BEGIN IMMEDIATE.
    retry_conn.execute("BEGIN IMMEDIATE")
    try:
        run_migrations(retry_conn)
    except BaseException:
        retry_conn.rollback()
        raise
    retry_conn.commit()

    # Full success: user_version at target, new tables present, job survived.
    version_after = retry_conn.execute("PRAGMA user_version").fetchone()[0]
    assert version_after == TARGET_USER_VERSION
    assert _table_exists(retry_conn, "station_poll_state")
    assert _table_exists(retry_conn, "station_current_obs")
    assert not _table_exists(retry_conn, "jobs_new"), (
        "retry must not leave an orphan jobs_new"
    )
    surviving_after_retry = retry_conn.execute(
        "SELECT type, status FROM jobs WHERE job_key='survives-crash'"
    ).fetchone()
    assert surviving_after_retry is not None, (
        "the pending job must survive through crash and retry"
    )
    assert surviving_after_retry["type"] == "fetch_feed"

    # Widened CHECK now admits fetch_current_obs on the retried DB.
    site_id_row = retry_conn.execute("SELECT id FROM sites LIMIT 1").fetchone()
    assert site_id_row is not None
    site_id = int(site_id_row["id"])
    assert _jobs_check_admits(retry_conn, "fetch_current_obs", site_id), (
        "post-retry DB must admit fetch_current_obs"
    )

    retry_conn.close()


# ---------------------------------------------------------------------------
# Bucket-1-H, test 3: fetch_current_obs enqueue on a migrated v3 DB
# ---------------------------------------------------------------------------


def test_fetch_current_obs_enqueue_on_v3_db(tmp_path: Path) -> None:
    """On a migrated v3 DB, fetch_current_obs inserts succeed and bogus types fail.

    Also checks that the catchup / site_id IS NULL branch still works —
    the rebuild must not have silently dropped that arm of the CHECK.
    """
    close_db()
    config.db_path = str(tmp_path / "wxverify-v3.db")
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(config.db_path)
    conn = db._conn  # noqa: SLF001

    assert conn.execute("PRAGMA user_version").fetchone()[0] == TARGET_USER_VERSION

    # Insert a site to satisfy FK constraints.
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('OBS-SITE', 47.0, 25.0, 900.0, 'UTC')"
    )
    site_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    # Positive: fetch_current_obs with site_id must be accepted.
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key)"
        " VALUES ('fetch_current_obs', ?, 'pco-probe')",
        (site_id,),
    )
    row = conn.execute(
        "SELECT type, site_id FROM jobs WHERE job_key='pco-probe'"
    ).fetchone()
    assert row is not None
    assert row["type"] == "fetch_current_obs"
    assert row["site_id"] == site_id

    # Negative (bogus type): must be rejected.
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO jobs (type, site_id, job_key)"
            " VALUES ('bogus_type', ?, 'bad-probe')",
            (site_id,),
        )

    # Negative (site-scoped type with NULL site_id): must be rejected.
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO jobs (type, site_id, job_key)"
            " VALUES ('fetch_current_obs', NULL, 'null-site-probe')"
        )

    # Positive: catchup with site_id IS NULL must still be accepted.
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key)"
        " VALUES ('catchup', NULL, 'catchup-probe')"
    )
    catchup_row = conn.execute(
        "SELECT type, site_id FROM jobs WHERE job_key='catchup-probe'"
    ).fetchone()
    assert catchup_row is not None
    assert catchup_row["type"] == "catchup"
    assert catchup_row["site_id"] is None
