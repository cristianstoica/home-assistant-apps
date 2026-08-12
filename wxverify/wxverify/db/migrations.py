"""SQLite schema and insert-only boot seeds."""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import timedelta

from wxverify import config
from wxverify.collection.forecast_validation import invalid_forecast_sample_sql
from wxverify.core.timeutil import isoformat_utc, utc_now

logger = logging.getLogger(__name__)

TARGET_USER_VERSION = 4

# Seed offset applied per station when migrate_v3 backfills station_poll_state,
# so cold-start polls fan out instead of bursting all at once.
POLL_SEED_STAGGER_SECONDS = 10


def _executescript(conn: sqlite3.Connection, script: str) -> None:
    conn.executescript(script)


def create_schema(conn: sqlite3.Connection) -> None:
    _executescript(
        conn,
        """
        CREATE TABLE IF NOT EXISTS sites (
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

        CREATE TABLE IF NOT EXISTS stations (
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

        CREATE TABLE IF NOT EXISTS station_observations (
            id INTEGER PRIMARY KEY,
            station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
            variable TEXT NOT NULL,
            valid_at TEXT NOT NULL,
            value REAL NOT NULL,
            qc_flag TEXT NOT NULL CHECK(qc_flag IN ('ok','range','spike')),
            source_raw TEXT,
            fetched_at TEXT,
            UNIQUE(station_id, variable, valid_at)
        );

        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            variable TEXT NOT NULL,
            valid_at TEXT NOT NULL,
            value REAL NOT NULL,
            n_stations INTEGER NOT NULL,
            rejected_stations INTEGER NOT NULL DEFAULT 0,
            computed_at TEXT,
            UNIQUE(site_id, variable, valid_at)
        );

        CREATE TABLE IF NOT EXISTS feeds (
            id INTEGER PRIMARY KEY,
            source TEXT NOT NULL,
            model TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
            disabled_reason TEXT,
            default_subscribed INTEGER NOT NULL DEFAULT 0
                CHECK(default_subscribed IN (0,1)),
            fetch_interval_minutes INTEGER NOT NULL,
            max_lead_hours INTEGER NOT NULL DEFAULT 168,
            is_virtual INTEGER NOT NULL DEFAULT 0 CHECK(is_virtual IN (0,1)),
            UNIQUE(source, model)
        );

        CREATE TABLE IF NOT EXISTS site_feed_state (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            enabled INTEGER CHECK(enabled IS NULL OR enabled IN (0,1)),
            last_run_at TEXT,
            last_error TEXT,
            error_count INTEGER NOT NULL DEFAULT 0,
            grid_lat REAL,
            grid_lon REAL,
            grid_elevation_m REAL,
            PRIMARY KEY(site_id, feed_id)
        );

        CREATE TABLE IF NOT EXISTS sources (
            source TEXT PRIMARY KEY NOT NULL,
            daily_call_limit INTEGER NOT NULL,
            daily_credit_limit INTEGER,
            billing_tz TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS forecast_samples (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            variable TEXT NOT NULL,
            issued_at TEXT NOT NULL,
            valid_at TEXT NOT NULL,
            lead_hours INTEGER NOT NULL CHECK(lead_hours >= 1),
            value REAL NOT NULL,
            source_raw TEXT NOT NULL,
            model_run_id TEXT NOT NULL,
            fetched_at TEXT,
            UNIQUE(site_id, feed_id, variable, issued_at, valid_at)
        );
        CREATE INDEX IF NOT EXISTS idx_samples_site_var_valid
            ON forecast_samples(site_id, variable, valid_at);
        CREATE INDEX IF NOT EXISTS idx_samples_runs
            ON forecast_samples(site_id, feed_id, model_run_id);

        CREATE TABLE IF NOT EXISTS api_budget (
            source TEXT NOT NULL REFERENCES sources(source) ON DELETE RESTRICT,
            billing_day TEXT NOT NULL,
            calls INTEGER NOT NULL DEFAULT 0,
            credits INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(source, billing_day)
        );

        CREATE TABLE IF NOT EXISTS timezone_generations (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            timezone TEXT NOT NULL,
            mode TEXT NOT NULL CHECK(mode IN
                ('initial','retrospective_correction','prospective_change')),
            effective_from TEXT,
            effective_to TEXT,
            state TEXT NOT NULL DEFAULT 'building'
                CHECK(state IN ('building','published','retired','failed')),
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            published_at TEXT,
            examined_count INTEGER,
            changed_count INTEGER,
            unchanged_count INTEGER,
            excluded_count INTEGER,
            provenance TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tz_generations_site
            ON timezone_generations(site_id, state);

        CREATE TABLE IF NOT EXISTS forecast_pairs (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            variable TEXT NOT NULL,
            issued_at TEXT NOT NULL,
            valid_at TEXT NOT NULL,
            lead_hours INTEGER NOT NULL CHECK(lead_hours >= 1),
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            forecast REAL NOT NULL,
            observed REAL NOT NULL,
            error REAL,
            abs_error REAL,
            sq_error REAL,
            cat_hit INTEGER,
            cat_false INTEGER,
            cat_miss INTEGER,
            cat_correct_neg INTEGER,
            rain_threshold_mm REAL,
            contributors INTEGER,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            first_known_at TEXT,
            tz_generation_id INTEGER NOT NULL
                REFERENCES timezone_generations(id),
            UNIQUE(site_id, feed_id, variable, issued_at, valid_at,
                   tz_generation_id)
        );
        CREATE INDEX IF NOT EXISTS idx_pairs_leaderboard
            ON forecast_pairs(site_id, variable, day_ahead, valid_at);
        CREATE INDEX IF NOT EXISTS idx_pairs_cell
            ON forecast_pairs(site_id, feed_id, variable, day_ahead, valid_at);
        CREATE INDEX IF NOT EXISTS idx_pairs_winrate
            ON forecast_pairs(site_id, variable, day_ahead, feed_id,
                              valid_at, issued_at DESC, abs_error,
                              tz_generation_id);

        CREATE TABLE IF NOT EXISTS daily_truth (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            quantity TEXT NOT NULL CHECK(quantity IN
                ('temperature_high','temperature_low','wind_max',
                 'precip_total','precip_occurrence')),
            value REAL,
            eligible INTEGER NOT NULL CHECK(eligible IN (0,1)),
            exclusion_reason TEXT,
            covered_hours INTEGER NOT NULL,
            expected_slots INTEGER NOT NULL,
            peak_window_ok INTEGER
                CHECK(peak_window_ok IS NULL OR peak_window_ok IN (0,1)),
            wet_hours INTEGER,
            dry_hours INTEGER,
            rain_threshold_mm REAL,
            day_start_utc TEXT NOT NULL,
            day_end_utc TEXT NOT NULL,
            timezone TEXT NOT NULL,
            source_max_computed_at TEXT,
            stale INTEGER NOT NULL DEFAULT 0 CHECK(stale IN (0,1)),
            generated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            tz_generation_id INTEGER NOT NULL
                REFERENCES timezone_generations(id),
            UNIQUE(site_id, quantity, local_date, tz_generation_id)
        );
        CREATE INDEX IF NOT EXISTS idx_daily_truth_stale
            ON daily_truth(site_id, local_date, tz_generation_id)
            WHERE stale = 1;

        CREATE TABLE IF NOT EXISTS forecast_of_record (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            tz_generation_id INTEGER NOT NULL
                REFERENCES timezone_generations(id),
            timezone TEXT NOT NULL,
            tz_rebuild_in_progress INTEGER NOT NULL DEFAULT 0
                CHECK(tz_rebuild_in_progress IN (0,1)),
            snapshot_local_date TEXT NOT NULL,
            snapshot_local_time TEXT NOT NULL,
            snapshot_utc TEXT NOT NULL,
            target_local_date TEXT NOT NULL,
            variable TEXT NOT NULL
                CHECK(variable IN ('temperature','wind','precip')),
            display_lead INTEGER NOT NULL
                CHECK(display_lead BETWEEN 0 AND 7),
            status TEXT NOT NULL CHECK(status IN ('recorded','missed')),
            missed_reason TEXT,
            write_path TEXT
                CHECK(write_path IS NULL
                      OR write_path IN ('on_time','late_reconstruction')),
            write_latency_seconds INTEGER,
            policy TEXT,
            methodology_version INTEGER NOT NULL,
            app_version TEXT NOT NULL,
            candidates TEXT,
            selected_feed_ids TEXT,
            feed_weights TEXT,
            effective_cells TEXT,
            source_runs TEXT,
            hourly_values TEXT,
            daily_quantities TEXT,
            leaderboard_status TEXT,
            created_at TEXT NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            CHECK(
                (status = 'missed'
                 AND missed_reason IS NOT NULL AND write_path IS NULL)
                OR (status = 'recorded'
                    AND missed_reason IS NULL AND write_path IS NOT NULL)
            ),
            UNIQUE(site_id, tz_generation_id, snapshot_local_date,
                   variable, target_local_date)
        );

        CREATE TABLE IF NOT EXISTS score_cache (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            variable TEXT NOT NULL,
            day_ahead INTEGER NOT NULL,
            window_key TEXT NOT NULL,
            n INTEGER NOT NULL,
            bias REAL,
            mae REAL,
            rmse REAL,
            pod REAL,
            far REAL,
            csi REAL,
            ets REAL,
            hss REAL,
            skill_score REAL,
            computed_at TEXT NOT NULL,
            PRIMARY KEY(site_id, feed_id, variable, day_ahead, window_key)
        );

        CREATE TABLE IF NOT EXISTS jobs (
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
                    type IN ('fetch_feed','fetch_obs','fetch_current_obs',
                             'pair_and_score','backfill_site',
                             'forecast_record','record_gap_scan',
                             'verification_run','timezone_correction')
                    AND site_id IS NOT NULL
                )
            )
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe
            ON jobs(type, COALESCE(site_id, -1), job_key)
            WHERE status IN ('pending','running') AND job_key IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_jobs_type_key_site
            ON jobs(type, job_key, site_id, id);

        CREATE TABLE IF NOT EXISTS station_poll_state (
            station_id INTEGER PRIMARY KEY REFERENCES stations(id) ON DELETE CASCADE,
            cadence_events TEXT NOT NULL DEFAULT '[]',
            last_obstime TEXT,
            learned_interval_seconds INTEGER,
            health_state TEXT NOT NULL DEFAULT 'cold'
                CHECK(health_state IN
                    ('cold','online','offline','terminal','transient')),
            next_poll_at TEXT,
            last_poll_at TEXT,
            last_error TEXT,
            error_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

        CREATE TABLE IF NOT EXISTS station_current_obs (
            station_id INTEGER PRIMARY KEY REFERENCES stations(id) ON DELETE CASCADE,
            obs_time_utc TEXT,
            temp REAL, humidity REAL, dewpt REAL,
            wind_speed REAL, wind_gust REAL, wind_dir REAL,
            pressure REAL, precip_rate REAL, precip_total REAL, uv REAL,
            neighborhood TEXT,
            fetched_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS domain_backoffs (
            domain TEXT PRIMARY KEY NOT NULL,
            next_attempt_at TEXT NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runtime_state (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );
        """,
    )
    _sync_forecast_sample_index(conn, "idx_samples_invalid", invalid_sample_index_ddl())
    _sync_forecast_sample_index(conn, "idx_samples_recent", SAMPLES_RECENT_INDEX_DDL)


# Serves the bounded-window provider-health statements. The leading
# (site_id, feed_id, issued_at) columns seek the recent window; the trailing
# (valid_at, variable, model_run_id) columns make both windowed statements
# index-only over it -- measured 22.6% faster than the three-column form on a
# production-sized copy, at ~135 MB of index for a ~1 GB database. Same idiom
# as idx_samples_runs (shaped to cover the model-run count) and
# idx_pairs_winrate (shaped to the win-rate query).
SAMPLES_RECENT_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_samples_recent "
    "ON forecast_samples(site_id, feed_id, issued_at, valid_at, "
    "variable, model_run_id)"
)


def invalid_sample_index_ddl() -> str:
    """DDL for the partial invalid-sample index, generated from the live
    validation predicate so index and query stay the same string by
    construction."""
    return (
        "CREATE INDEX IF NOT EXISTS idx_samples_invalid "
        "ON forecast_samples(site_id, feed_id) "
        f"WHERE {invalid_forecast_sample_sql('forecast_samples')}"
    )


def _sync_forecast_sample_index(conn: sqlite3.Connection, name: str, ddl: str) -> None:
    """Reconcile a code-generated forecast_samples index with its stored DDL.

    CREATE INDEX IF NOT EXISTS matches on name only: on a database that
    already has the index it is a no-op even when the stored definition
    differs. For a partial index (idx_samples_invalid) the consequence is
    loud -- an edit to the validation constants leaves the old predicate on
    disk, INDEXED BY tries to prove the query predicate implies the stored
    one, cannot, and the statement fails with "no query solution" at prepare
    time. For a full index (idx_samples_recent) it is silent and worse: a
    stale column list still satisfies the pin while the plan quietly loses
    its covering property. Reconcile instead of trusting the name: when the
    stored definition (compared from "ON forecast_samples" onward) no longer
    matches the generated DDL, DROP and re-CREATE.

    The SAVEPOINT is load-bearing, not ceremony. The executescript that runs
    immediately before this commits the pending transaction before running
    its own script, so by this point the connection is back in autocommit and
    each statement below would otherwise land individually: an unprotected
    failure between DROP and CREATE would leave no index on disk at all, and
    INDEXED BY would then fail with "no such index" -- the same class of
    error, reached from the other direction. SAVEPOINT nests regardless of
    autocommit state, and execute() (unlike executescript) never forces an
    implicit commit.

    Only the branch that actually issues CREATE INDEX logs, with elapsed
    milliseconds, so a slow first boot after an upgrade is legible in the
    log while a no-op boot stays silent.
    """
    conn.execute(f"SAVEPOINT {name}_sync")
    try:
        stored = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
            (name,),
        ).fetchone()
        if stored is not None and _index_predicate(stored["sql"]) != _index_predicate(
            ddl
        ):
            logger.info("rebuilding %s: stored definition changed", name)
            conn.execute(f"DROP INDEX {name}")
            stored = None
        if stored is None:
            started = time.perf_counter()
            conn.execute(ddl)
            logger.info(
                "index %s built in %d ms",
                name,
                int((time.perf_counter() - started) * 1000),
            )
    except BaseException:
        conn.execute(f"ROLLBACK TO {name}_sync")
        conn.execute(f"RELEASE {name}_sync")
        raise
    conn.execute(f"RELEASE {name}_sync")


def _index_predicate(ddl: str) -> str:
    # Comparing from "ON forecast_samples" onward, rather than the full
    # statement, sidesteps SQLite stripping "IF NOT EXISTS" from the text it
    # persists to sqlite_master -- a full-text compare against our own
    # generated DDL (which still has it) would otherwise always mismatch.
    return _squash_ws(ddl[ddl.index("ON forecast_samples") :])


def _squash_ws(text: str) -> str:
    return " ".join(text.split())


def seed_default_sources(conn: sqlite3.Connection) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO sources
            (source, daily_call_limit, daily_credit_limit, billing_tz)
        VALUES (?, ?, ?, ?)
        """,
        [
            (
                seed.source,
                seed.daily_call_limit,
                seed.daily_credit_limit,
                seed.billing_tz,
            )
            for seed in config.SOURCE_SEEDS
        ],
    )


def seed_default_feeds(conn: sqlite3.Connection) -> None:
    conn.executemany(
        """
        INSERT OR IGNORE INTO feeds
            (source, model, enabled, disabled_reason, default_subscribed,
             fetch_interval_minutes, max_lead_hours, is_virtual)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                seed.source,
                seed.model,
                1 if seed.enabled else 0,
                seed.disabled_reason,
                1 if seed.default_subscribed else 0,
                seed.fetch_interval_minutes,
                seed.max_lead_hours,
                1 if seed.is_virtual else 0,
            )
            for seed in config.FEED_SEEDS
        ],
    )


def seed_default_settings(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
        (
            ("rolling_window_days", "30"),
            ("min_n", "30"),
            ("obs_interval_minutes", "180"),
            ("obs_jitter_minutes", "20"),
        ),
    )


def run_migrations(conn: sqlite3.Connection) -> None:
    row = conn.execute("PRAGMA user_version").fetchone()
    current = int(row[0]) if row is not None else 0
    if current > TARGET_USER_VERSION:
        raise RuntimeError(f"database user_version {current} is newer than this app")
    logger.debug(
        "migrations begin user_version=%s target=%s", current, TARGET_USER_VERSION
    )
    create_schema(conn)
    logger.debug("migrations schema ensured")
    if current < 2:
        logger.debug("migrations applying v2 backfill_status")
        migrate_v2_backfill_status(conn)
    if current < 3:
        logger.debug("migrations applying v3 station poll-state")
        migrate_v3(conn)
    if current < 4:
        logger.debug("migrations applying v4 timezone generations")
        migrate_v4(conn)
    seed_default_sources(conn)
    seed_default_feeds(conn)
    seed_default_settings(conn)
    logger.debug("migrations seeded sources+feeds+settings")
    conn.execute(f"PRAGMA user_version = {TARGET_USER_VERSION}")
    logger.debug("migrations done user_version=%s", TARGET_USER_VERSION)


def migrate_v2_backfill_status(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "sites")
    if "backfill_status" not in columns:
        conn.execute(
            """
            ALTER TABLE sites
            ADD COLUMN backfill_status TEXT NOT NULL DEFAULT 'pending'
                CHECK(backfill_status IN ('pending','in_progress','complete'))
            """
        )
    if "backfill_through" not in columns:
        conn.execute("ALTER TABLE sites ADD COLUMN backfill_through TEXT")
    conn.execute(
        """
        UPDATE sites
        SET backfill_status = 'pending'
        WHERE backfill_status IS NULL
           OR backfill_status NOT IN ('pending','in_progress','complete')
        """
    )
    _executescript(
        conn,
        """
        CREATE TRIGGER IF NOT EXISTS trg_sites_backfill_status_insert_default
        AFTER INSERT ON sites
        FOR EACH ROW
        WHEN NEW.backfill_status IS NULL
        BEGIN
            UPDATE sites SET backfill_status='pending' WHERE id=NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_sites_backfill_status_insert_check
        BEFORE INSERT ON sites
        FOR EACH ROW
        WHEN NEW.backfill_status IS NOT NULL
         AND NEW.backfill_status NOT IN ('pending','in_progress','complete')
        BEGIN
            SELECT RAISE(ABORT, 'invalid backfill_status');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_sites_backfill_status_update_check
        BEFORE UPDATE OF backfill_status ON sites
        FOR EACH ROW
        WHEN NEW.backfill_status IS NULL
          OR NEW.backfill_status NOT IN ('pending','in_progress','complete')
        BEGIN
            SELECT RAISE(ABORT, 'invalid backfill_status');
        END;
        """,
    )


def migrate_v3(conn: sqlite3.Connection) -> None:
    """Widen the jobs CHECK for fetch_current_obs and seed station poll-state.

    Runs only under the ``current < 3`` gate in :func:`run_migrations`, so the
    live ``jobs`` table still carries the pre-v3 CHECK and the rebuild always
    applies cleanly. The two new tables (``station_poll_state``,
    ``station_current_obs``) are created by :func:`create_schema`; this function
    rebuilds ``jobs`` to admit ``fetch_current_obs`` and backfills a staggered
    poll-state row per existing station.
    """
    # The whole v3 step (jobs rebuild + poll-state seed) must be all-or-nothing.
    # The outer BEGIN IMMEDIATE in Database._run_immediate does NOT protect us:
    # create_schema's executescript issues an implicit COMMIT before migrate_v3
    # runs, so by here the connection is effectively back in autocommit and each
    # statement would land individually. An explicit SAVEPOINT opens (nests) a
    # transaction regardless of autocommit state, and execute/executemany (unlike
    # executescript) never force an implicit commit, so the rebuild becomes
    # atomic on its own terms. On failure we ROLL BACK TO the savepoint (removing
    # any orphan jobs_new, leaving jobs intact) and re-raise so run_migrations
    # aborts WITHOUT bumping user_version, leaving a clean v2 DB the next boot
    # retries.
    conn.execute("SAVEPOINT migrate_v3")
    try:
        # Rebuild jobs to carry the widened CHECK (SQLite cannot ALTER a CHECK,
        # and CREATE TABLE IF NOT EXISTS is a no-op on the existing table). The
        # jobs_new CHECK and column list must stay identical to create_schema's
        # fresh DDL. The unconditional CREATE TABLE jobs_new is safe: the
        # savepoint rollback guarantees no orphan survives an aborted run.
        conn.execute(
            """
            CREATE TABLE jobs_new (
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
            )
            """
        )
        conn.execute("INSERT INTO jobs_new SELECT * FROM jobs")
        conn.execute("DROP TABLE jobs")
        conn.execute("ALTER TABLE jobs_new RENAME TO jobs")
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe
                ON jobs(type, COALESCE(site_id, -1), job_key)
                WHERE status IN ('pending','running') AND job_key IS NOT NULL
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_jobs_type_key_site
                ON jobs(type, job_key, site_id, id)
            """
        )
        # Seed a poll-state row per existing station with a staggered
        # next_poll_at so cold-start polls fan out; the leftmost seeds at ~now.
        # INSERT OR IGNORE keeps this idempotent (re-run is a no-op) and never
        # disturbs live poll state.
        now = utc_now()
        rows = [
            (
                int(row["id"]),
                isoformat_utc(
                    now + timedelta(seconds=index * POLL_SEED_STAGGER_SECONDS)
                ),
            )
            for index, row in enumerate(
                conn.execute("SELECT id FROM stations ORDER BY id")
            )
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO station_poll_state (station_id, next_poll_at) "
            "VALUES (?, ?)",
            rows,
        )
    except BaseException:
        conn.execute("ROLLBACK TO migrate_v3")
        conn.execute("RELEASE migrate_v3")
        raise
    conn.execute("RELEASE migrate_v3")


def migrate_v4(conn: sqlite3.Connection) -> None:
    """Timezone generations: widen the jobs CHECK, generation-tag pairs.

    Mirrors :func:`migrate_v3`'s SAVEPOINT-wrapped rebuild pattern (same
    atomicity rationale — see that function's comment). Steps, all inside one
    savepoint:

    1. rebuild ``jobs`` with the CHECK widened for the 0.11.0 job types
       (``forecast_record``, ``record_gap_scan``, ``verification_run``,
       ``timezone_correction`` — all per-site) and recreate its two indexes;
    2. seed one ``initial`` timezone generation per existing site (state
       ``published``, NULL-bounded effective interval — covers the whole
       history) plus the per-site published-pointer ``runtime_state`` row
       (``tz_generation_published:<site_id>``);
    3. rebuild ``forecast_pairs`` with ``first_known_at`` (left NULL for
       pre-existing rows — availability is defined by the pair producers
       going forward, never invented retroactively) and
       ``tz_generation_id NOT NULL`` inside the widened unique key,
       backfilling every existing row to its site's seeded ``initial``
       generation, then recreate the three pairs indexes.

    ``timezone_generations`` itself comes from :func:`create_schema`, as in
    v3. On a fresh database (user_version 0) both rebuilds copy zero rows
    from the already-new-shape tables created by :func:`create_schema`, so
    running the whole chain is harmless.
    """
    # Re-run guard: run_migrations writes PRAGMA user_version only AFTER the
    # RELEASE below commits, so a crash between the two leaves the v4 body
    # committed with user_version still 3. The SAVEPOINT makes the body
    # all-or-nothing, so this column's presence <=> the whole body (jobs
    # rebuild + seed + backfill) already committed; without the guard a
    # re-entry would seed duplicate initial generations and the pairs rebuild
    # would crash-loop on its UNIQUE(id). Precedent:
    # :func:`migrate_v2_backfill_status`'s column-probed idempotence.
    if "tz_generation_id" in _table_columns(conn, "forecast_pairs"):
        return
    conn.execute("SAVEPOINT migrate_v4")
    try:
        conn.execute(
            """
            CREATE TABLE jobs_new (
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
            )
            """
        )
        conn.execute("INSERT INTO jobs_new SELECT * FROM jobs")
        conn.execute("DROP TABLE jobs")
        conn.execute("ALTER TABLE jobs_new RENAME TO jobs")
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe
                ON jobs(type, COALESCE(site_id, -1), job_key)
                WHERE status IN ('pending','running') AND job_key IS NOT NULL
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_jobs_type_key_site
                ON jobs(type, job_key, site_id, id)
            """
        )
        # Seed the initial published generation per existing site, then the
        # published-pointer runtime_state row. Mirrors
        # wxverify.db.tz_generations.ensure_published_generation's seed shape
        # (kept in SQL here so the whole step stays inside this savepoint).
        now = isoformat_utc(utc_now())
        conn.execute(
            """
            INSERT INTO timezone_generations
                (site_id, timezone, mode, state, published_at)
            SELECT id, timezone, 'initial', 'published', ?
            FROM sites
            ORDER BY id
            """,
            (now,),
        )
        conn.execute(
            """
            INSERT INTO runtime_state (key, value)
            SELECT 'tz_generation_published:' || tg.site_id,
                   CAST(tg.id AS TEXT)
            FROM timezone_generations tg
            WHERE tg.mode = 'initial' AND tg.state = 'published'
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
            """
        )
        conn.execute(
            """
            CREATE TABLE forecast_pairs_new (
                id INTEGER PRIMARY KEY,
                site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
                feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
                variable TEXT NOT NULL,
                issued_at TEXT NOT NULL,
                valid_at TEXT NOT NULL,
                lead_hours INTEGER NOT NULL CHECK(lead_hours >= 1),
                day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
                forecast REAL NOT NULL,
                observed REAL NOT NULL,
                error REAL,
                abs_error REAL,
                sq_error REAL,
                cat_hit INTEGER,
                cat_false INTEGER,
                cat_miss INTEGER,
                cat_correct_neg INTEGER,
                rain_threshold_mm REAL,
                contributors INTEGER,
                created_at TEXT NOT NULL
                    DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                first_known_at TEXT,
                tz_generation_id INTEGER NOT NULL
                    REFERENCES timezone_generations(id),
                UNIQUE(site_id, feed_id, variable, issued_at, valid_at,
                       tz_generation_id)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO forecast_pairs_new
                (id, site_id, feed_id, variable, issued_at, valid_at,
                 lead_hours, day_ahead, forecast, observed, error, abs_error,
                 sq_error, cat_hit, cat_false, cat_miss, cat_correct_neg,
                 rain_threshold_mm, contributors, created_at, first_known_at,
                 tz_generation_id)
            SELECT fp.id, fp.site_id, fp.feed_id, fp.variable, fp.issued_at,
                   fp.valid_at, fp.lead_hours, fp.day_ahead, fp.forecast,
                   fp.observed, fp.error, fp.abs_error, fp.sq_error,
                   fp.cat_hit, fp.cat_false, fp.cat_miss, fp.cat_correct_neg,
                   fp.rain_threshold_mm, fp.contributors, fp.created_at,
                   NULL, tg.id
            FROM forecast_pairs fp
            JOIN timezone_generations tg
              ON tg.site_id = fp.site_id
             AND tg.mode = 'initial' AND tg.state = 'published'
            """
        )
        conn.execute("DROP TABLE forecast_pairs")
        conn.execute("ALTER TABLE forecast_pairs_new RENAME TO forecast_pairs")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pairs_leaderboard
                ON forecast_pairs(site_id, variable, day_ahead, valid_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pairs_cell
                ON forecast_pairs(site_id, feed_id, variable, day_ahead,
                                  valid_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pairs_winrate
                ON forecast_pairs(site_id, variable, day_ahead, feed_id,
                                  valid_at, issued_at DESC, abs_error,
                                  tz_generation_id)
            """
        )
    except BaseException:
        conn.execute("ROLLBACK TO migrate_v4")
        conn.execute("RELEASE migrate_v4")
        raise
    conn.execute("RELEASE migrate_v4")


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")}
