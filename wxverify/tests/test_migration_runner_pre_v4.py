"""Pre-v4 database upgrade: the runner orders tables -> migrations -> indexes.

Fixtures are the real `create_schema` bodies of 0.2.1, 0.3.0 and 0.10.1,
embedded verbatim; see the plan for why the existing fixtures cannot model
a 0.1.0-0.8.11 file.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from wxverify import config
from wxverify.db import migrations
from wxverify.db.connection import close_db, init_db
from wxverify.db.migrations import (
    TARGET_USER_VERSION,
    _table_columns,
    create_indexes,
    create_schema,
    create_tables,
    migrate_v2_backfill_status,
    run_migrations,
)

_V2_SCHEMA_SQL = """
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

        CREATE TABLE IF NOT EXISTS api_budget (
            source TEXT NOT NULL REFERENCES sources(source) ON DELETE RESTRICT,
            billing_day TEXT NOT NULL,
            calls INTEGER NOT NULL DEFAULT 0,
            credits INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(source, billing_day)
        );

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
            UNIQUE(site_id, feed_id, variable, issued_at, valid_at)
        );
        CREATE INDEX IF NOT EXISTS idx_pairs_leaderboard
            ON forecast_pairs(site_id, variable, day_ahead, valid_at);
        CREATE INDEX IF NOT EXISTS idx_pairs_cell
            ON forecast_pairs(site_id, feed_id, variable, day_ahead, valid_at);

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
                    type IN ('fetch_feed','fetch_obs','pair_and_score','backfill_site')
                    AND site_id IS NOT NULL
                )
            )
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe
            ON jobs(type, COALESCE(site_id, -1), job_key)
            WHERE status IN ('pending','running') AND job_key IS NOT NULL;

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
        """  # create_schema body at 9072e48 (0.2.1), verbatim
_V3_EARLY_SCHEMA_SQL = """
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

        CREATE TABLE IF NOT EXISTS api_budget (
            source TEXT NOT NULL REFERENCES sources(source) ON DELETE RESTRICT,
            billing_day TEXT NOT NULL,
            calls INTEGER NOT NULL DEFAULT 0,
            credits INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(source, billing_day)
        );

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
            UNIQUE(site_id, feed_id, variable, issued_at, valid_at)
        );
        CREATE INDEX IF NOT EXISTS idx_pairs_leaderboard
            ON forecast_pairs(site_id, variable, day_ahead, valid_at);
        CREATE INDEX IF NOT EXISTS idx_pairs_cell
            ON forecast_pairs(site_id, feed_id, variable, day_ahead, valid_at);

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
                             'pair_and_score','backfill_site')
                    AND site_id IS NOT NULL
                )
            )
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe
            ON jobs(type, COALESCE(site_id, -1), job_key)
            WHERE status IN ('pending','running') AND job_key IS NOT NULL;

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
        """  # at abe41e8 (0.3.0), verbatim
_V3_LATE_SCHEMA_SQL = """
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
            UNIQUE(site_id, feed_id, variable, issued_at, valid_at)
        );
        CREATE INDEX IF NOT EXISTS idx_pairs_leaderboard
            ON forecast_pairs(site_id, variable, day_ahead, valid_at);
        CREATE INDEX IF NOT EXISTS idx_pairs_cell
            ON forecast_pairs(site_id, feed_id, variable, day_ahead, valid_at);
        CREATE INDEX IF NOT EXISTS idx_pairs_winrate
            ON forecast_pairs(site_id, variable, day_ahead, feed_id,
                              valid_at, issued_at DESC, abs_error);

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
                             'pair_and_score','backfill_site')
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
        """  # at d2eb2ac (0.10.1), verbatim

_TRIGGERS = frozenset(
    {
        "trg_sites_backfill_status_insert_check",
        "trg_sites_backfill_status_insert_default",
        "trg_sites_backfill_status_update_check",
    }
)
Master = set[tuple[str, str, str, str]]

HISTORICAL: dict[str, tuple[str, int]] = {
    "v2": (_V2_SCHEMA_SQL, 2),
    "v3-early": (_V3_EARLY_SCHEMA_SQL, 3),
    "v3-late": (_V3_LATE_SCHEMA_SQL, 3),
}


def _connect(path: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path) if path else ":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _seed_synthetic_rows(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('site-one', 10.0, 20.0, 100.0, 'UTC')"
    )
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES ('site-two', 11.0, 21.0, 200.0, 'Etc/GMT-3')"
    )
    conn.execute(
        "INSERT INTO feeds (source, model, fetch_interval_minutes)"
        " VALUES ('synthetic', 'model-a', 60)"
    )
    conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)"
        " VALUES (1, 'STATION-0001', 10.0, 20.0, 100.0)"
    )
    conn.execute(
        "INSERT INTO forecast_pairs (site_id, feed_id, variable, issued_at, valid_at,"
        " lead_hours, day_ahead, forecast, observed) VALUES"
        " (1, 1, 'temp', '2026-01-01T00:00:00Z', '2026-01-01T06:00:00Z',"
        " 6, 0, 1.0, 1.5)"
    )
    conn.execute(
        "INSERT INTO forecast_pairs (site_id, feed_id, variable, issued_at, valid_at,"
        " lead_hours, day_ahead, forecast, observed) VALUES"
        " (2, 1, 'temp', '2026-01-01T00:00:00Z', '2026-01-02T06:00:00Z',"
        " 30, 1, 2.0, 2.5)"
    )
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key) VALUES ('fetch_feed', 1, 'k1')"
    )
    conn.execute("INSERT INTO jobs (type, site_id) VALUES ('catchup', NULL)")
    conn.execute("INSERT INTO settings (key, value) VALUES ('synthetic_key', 'v')")


def _build_historical(
    conn: sqlite3.Connection, schema_sql: str, user_version: int
) -> None:
    conn.executescript(schema_sql)
    migrate_v2_backfill_status(conn)  # trigger text unchanged since 0.2.1
    _seed_synthetic_rows(conn)
    conn.execute(f"PRAGMA user_version = {user_version}")


def _run(
    conn: sqlite3.Connection,
    fn: Callable[[sqlite3.Connection], None] = run_migrations,
) -> None:
    """Mirror `Database._run_immediate` on a bare connection."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        fn(conn)
    except BaseException:
        conn.rollback()
        raise
    conn.commit()


def _master(conn: sqlite3.Connection) -> Master:
    rows = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master"
        " WHERE name NOT LIKE 'sqlite_%'"
    )
    return {(r["type"], r["name"], r["tbl_name"], r["sql"]) for r in rows}


def _normalised(master: Master) -> Master:
    """Erase the two divergences SQLite itself introduces: the double quotes
    `ALTER TABLE ... RENAME` puts around the name, and whitespace."""
    return {
        (kind, name, tbl, " ".join(sql.split()).replace(f'"{name}"', name))
        for kind, name, tbl, sql in master
    }


def _user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _fresh() -> sqlite3.Connection:
    conn = _connect()
    _run(conn)
    return conn


def _denying(names: frozenset[str]) -> Callable[[sqlite3.Connection, str], None]:
    """Wrap `_executescript`: the first CREATE of a named object is refused."""
    real = migrations._executescript  # noqa: SLF001

    def wrapper(conn: sqlite3.Connection, script: str) -> None:
        def authorizer(
            action: int,
            arg1: str | None,
            arg2: str | None,
            db: str | None,
            trigger: str | None,
        ) -> int:
            if (
                action in (sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_CREATE_INDEX)
                and arg1 in names
            ):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        conn.set_authorizer(authorizer)
        try:
            real(conn, script)
        finally:
            conn.set_authorizer(None)

    return wrapper


@pytest.fixture
def process_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A file path wired as the process database; `close_db()` on teardown."""
    db_path = tmp_path / "wxverify.db"
    monkeypatch.setattr(config, "db_path", str(db_path))
    monkeypatch.setattr(config, "options_path", str(tmp_path / "missing-options.json"))
    close_db()
    yield db_path
    close_db()


def test_o2_pre_v4_inputs_converge_to_one_schema() -> None:
    """The regression pin: v2, early-v3 and late-v3 reach one schema.

    Fails at the pre-split runner with `sqlite3.OperationalError: no such
    column: tz_generation_id` for `v2` and `v3-early` -- ledger L0.
    """
    masters: list[Master] = []
    versions: list[int] = []
    for schema_sql, user_version in HISTORICAL.values():
        conn = _connect()
        _build_historical(conn, schema_sql, user_version)
        _run(conn)
        versions.append(_user_version(conn))
        masters.append(_master(conn))

    assert versions == [TARGET_USER_VERSION] * 3
    assert masters[0] == masters[1] == masters[2]
    assert len(masters[0]) == 44


@pytest.mark.parametrize(
    ("key", "expected_objects", "winrate_columns"),
    [
        ("v2", 22, None),
        ("v3-early", 24, None),
        ("v3-late", 27, 7),
    ],
)
def test_o4_fixtures_are_representative(
    key: str, expected_objects: int, winrate_columns: int | None
) -> None:
    """The fixtures are the real pre-v4 shape, and the composite still fails.

    Catches a fixture that pre-creates `idx_pairs_winrate` or omits
    `forecast_pairs` -- the two traps the existing suites fall into.
    """
    schema_sql, user_version = HISTORICAL[key]
    conn = _connect()
    _build_historical(conn, schema_sql, user_version)

    names = {
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        )
    }
    assert len(names | _TRIGGERS) == expected_objects
    assert "tz_generation_id" not in _table_columns(conn, "forecast_pairs")

    winrate_rows = conn.execute("PRAGMA index_info(idx_pairs_winrate)").fetchall()
    if winrate_columns is None:
        assert winrate_rows == []
    else:
        assert len(winrate_rows) == winrate_columns

    if key == "v3-late":
        create_schema(conn)  # 7-column index already present: no-op, no raise
    else:
        with pytest.raises(
            sqlite3.OperationalError, match="no such column: tz_generation_id"
        ):
            create_schema(conn)


def _assert_v2_seed_state(conn: sqlite3.Connection) -> None:
    """The O5 assertions: seeded rows survive with the v4 backfill applied."""
    assert conn.execute("SELECT COUNT(*) FROM sites").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM stations").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM forecast_pairs").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == 2

    settings = {
        row["key"]: row["value"]
        for row in conn.execute("SELECT key, value FROM settings")
    }
    assert settings["synthetic_key"] == "v"
    assert conn.execute("SELECT COUNT(*) FROM feeds").fetchone()[0] == 16

    tz_rows = conn.execute(
        "SELECT site_id, timezone, mode, state FROM timezone_generations"
        " ORDER BY site_id"
    ).fetchall()
    assert [tuple(r) for r in tz_rows] == [
        (1, "UTC", "initial", "published"),
        (2, "Etc/GMT-3", "initial", "published"),
    ]

    pairs = conn.execute(
        "SELECT id, site_id, tz_generation_id FROM forecast_pairs ORDER BY id"
    ).fetchall()
    assert [tuple(r) for r in pairs] == [(1, 1, 1), (2, 2, 2)]

    assert conn.execute("SELECT COUNT(*) FROM station_poll_state").fetchone()[0] == 1

    jobs = conn.execute(
        "SELECT id, type, site_id, job_key FROM jobs ORDER BY id"
    ).fetchall()
    assert [tuple(r) for r in jobs] == [
        (1, "fetch_feed", 1, "k1"),
        (2, "catchup", None, None),
    ]


def test_o5_seeded_rows_survive_v4_backfill() -> None:
    """Seeded synthetic rows survive migration with the v4 backfill applied."""
    schema_sql, user_version = HISTORICAL["v2"]
    conn = _connect()
    _build_historical(conn, schema_sql, user_version)

    _run(conn)

    _assert_v2_seed_state(conn)


def test_o1_split_is_a_partition_of_the_composite() -> None:
    """`create_tables` + `create_indexes` == `create_schema`, exactly.

    Catches: a statement in the wrong half (M5), a dropped
    `_sync_forecast_sample_index` call (M6), a composite that is not
    tables + indexes (M7).
    """
    conn = _connect()
    create_tables(conn)
    table_rows = conn.execute(
        "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
    ).fetchall()
    assert len(table_rows) == 27
    assert all(row["type"] == "table" for row in table_rows)

    create_indexes(conn)
    all_rows = conn.execute(
        "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
    ).fetchall()
    index_rows = [row for row in all_rows if row["type"] == "index"]
    assert len(index_rows) == 14

    split_master = _master(conn)

    composite_conn = _connect()
    create_schema(composite_conn)
    assert split_master == _master(composite_conn)


@pytest.mark.parametrize("key", list(HISTORICAL))
def test_o3_each_migrated_input_is_structurally_fresh(key: str) -> None:
    """Parametrised over `HISTORICAL`: normalised equality with a fresh db.

    Catches: `create_indexes` never called (M3 -- fresh lacks the pairs
    indexes, migrated has them from `migrate_v4`), a `_sync` call dropped
    only on one side, text drift in any statement that exists historically
    (M8).
    """
    schema_sql, user_version = HISTORICAL[key]
    conn = _connect()
    _build_historical(conn, schema_sql, user_version)
    _run(conn)

    assert _normalised(_master(conn)) == _normalised(_master(_fresh()))


@pytest.mark.parametrize("key", [*HISTORICAL, "fresh"])
def test_o6_idempotence_after_migration(key: str) -> None:
    """Re-running the migrated runner, then the composite, changes nothing.

    Catches: a statement that lost its `IF NOT EXISTS` in the move; a runner
    that re-enters a rebuild at `user_version` 6.
    """
    if key == "fresh":
        conn = _connect()
        _run(conn)
    else:
        schema_sql, user_version = HISTORICAL[key]
        conn = _connect()
        _build_historical(conn, schema_sql, user_version)
        _run(conn)

    snapshot = (_master(conn), _user_version(conn))

    _run(conn)
    assert (_master(conn), _user_version(conn)) == snapshot

    create_schema(conn)
    assert (_master(conn), _user_version(conn)) == snapshot


def test_o7_interrupted_fresh_boot_converges(
    process_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A denied `stations` CREATE mid-`create_tables` on an empty file still
    converges to a fresh database on the next boot.

    Catches: a non-convergent table half (e.g. a `CREATE TABLE` without
    `IF NOT EXISTS`), a seam that never installs the authorizer (T2).
    """
    with monkeypatch.context() as m:
        m.setattr(migrations, "_executescript", _denying(frozenset({"stations"})))
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            init_db(str(process_db))

    raw = _connect(process_db)
    assert (
        raw.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sites'"
        ).fetchone()
        is not None
    )
    assert (
        raw.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stations'"
        ).fetchone()
        is None
    )
    assert _user_version(raw) == 0
    raw.close()

    db = init_db(str(process_db))
    conn = db._conn  # noqa: SLF001
    assert _user_version(conn) == TARGET_USER_VERSION
    assert _master(conn) == _master(_fresh())


def test_o8_interrupted_v2_boot_inside_create_tables_converges(
    process_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A denied `daily_truth` CREATE mid-`create_tables` on a v2 file leaves
    no migration applied, and a plain reopen converges as O5 does.

    Catches: a runner whose second pass double-applies `migrate_v2`'s
    triggers or ALTERs.
    """
    schema_sql, user_version = HISTORICAL["v2"]
    raw = _connect(process_db)
    _build_historical(raw, schema_sql, user_version)
    raw.close()

    with monkeypatch.context() as m:
        m.setattr(migrations, "_executescript", _denying(frozenset({"daily_truth"})))
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            init_db(str(process_db))

    raw = _connect(process_db)
    assert (
        raw.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table'"
            " AND name='timezone_generations'"
        ).fetchone()
        is not None
    )
    assert (
        raw.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='daily_truth'"
        ).fetchone()
        is None
    )
    assert "tz_generation_id" not in _table_columns(raw, "forecast_pairs")
    assert _user_version(raw) == 2
    raw.close()

    db = init_db(str(process_db))
    conn = db._conn  # noqa: SLF001
    assert _user_version(conn) == TARGET_USER_VERSION
    assert _normalised(_master(conn)) == _normalised(_master(_fresh()))
    _assert_v2_seed_state(conn)


def test_o9_interrupted_v2_boot_inside_create_indexes_converges_no_reseed(
    process_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A denied `idx_tz_generations_site` CREATE mid-`create_indexes` on a v2
    file leaves every version migration committed, and a plain reopen
    converges without re-seeding.

    Catches: `PRAGMA user_version` written before `create_indexes` (M9) or
    before the gates (M10); a `migrate_v4` guard regression (would
    double-seed); a `create_indexes` that is not `IF NOT EXISTS` throughout.
    """
    schema_sql, user_version = HISTORICAL["v2"]
    raw = _connect(process_db)
    _build_historical(raw, schema_sql, user_version)
    raw.close()

    with monkeypatch.context() as m:
        m.setattr(
            migrations,
            "_executescript",
            _denying(frozenset({"idx_tz_generations_site"})),
        )
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            init_db(str(process_db))

    raw = _connect(process_db)
    assert (
        raw.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_samples_runs'"
        ).fetchone()
        is not None
    )
    assert (
        raw.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index'"
            " AND name='idx_tz_generations_site'"
        ).fetchone()
        is None
    )
    assert "tz_generation_id" in _table_columns(raw, "forecast_pairs")
    tz_rows_before = raw.execute(
        "SELECT id FROM timezone_generations ORDER BY id"
    ).fetchall()
    assert len(tz_rows_before) == 2
    generation_ids = [int(row["id"]) for row in tz_rows_before]
    pairs_before = [
        tuple(row)
        for row in raw.execute(
            "SELECT id, site_id, tz_generation_id FROM forecast_pairs ORDER BY id"
        ).fetchall()
    ]
    assert _user_version(raw) == 2
    raw.close()

    db = init_db(str(process_db))
    conn = db._conn  # noqa: SLF001
    assert _user_version(conn) == TARGET_USER_VERSION
    assert _normalised(_master(conn)) == _normalised(_master(_fresh()))

    tz_rows_after = conn.execute(
        "SELECT id FROM timezone_generations ORDER BY id"
    ).fetchall()
    assert [int(row["id"]) for row in tz_rows_after] == generation_ids

    pairs_after = [
        tuple(row)
        for row in conn.execute(
            "SELECT id, site_id, tz_generation_id FROM forecast_pairs ORDER BY id"
        ).fetchall()
    ]
    assert pairs_after == pairs_before

    jobs = conn.execute(
        "SELECT id, type, site_id, job_key FROM jobs ORDER BY id"
    ).fetchall()
    assert [tuple(row) for row in jobs] == [
        (1, "fetch_feed", 1, "k1"),
        (2, "catchup", None, None),
    ]


def test_o10_run_migrations_on_empty_matches_create_schema_plus_triggers() -> None:
    """`run_migrations` on an empty file yields every object `create_schema`
    produces, plus the three v2-backfill triggers.

    Catches: `create_indexes` dropped from the runner while still in the
    composite (M3), `create_tables` dropped (M4).
    """
    fresh = _connect()
    _run(fresh)
    fresh_names = {
        row["name"]
        for row in fresh.execute(
            "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        )
    }

    composite = _connect()
    create_schema(composite)
    composite_names = {
        row["name"]
        for row in composite.execute(
            "SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        )
    }

    assert fresh_names == composite_names | _TRIGGERS
    assert len(fresh_names) == 44
