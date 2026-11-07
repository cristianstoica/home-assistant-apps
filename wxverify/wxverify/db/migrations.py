"""SQLite schema, insert-only boot seeds, and the one upgrade to version 17."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify import config
from wxverify.collection.forecast_validation import invalid_stored_sample_sql
from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
from wxverify.core import stop
from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.site_days import DAILY_CLOSE_ON_KEY

logger = logging.getLogger(__name__)

TARGET_USER_VERSION = 17

# The one version run_migrations upgrades from; 1 to 15 are refused.
UPGRADE_FROM_USER_VERSION: Final = 16


def _executescript(conn: sqlite3.Connection, script: str) -> None:
    conn.executescript(script)


def create_tables(conn: sqlite3.Connection) -> None:
    """Every CREATE TABLE IF NOT EXISTS of the current schema, verbatim, and
    the three ``sites`` triggers on ``backfill_status``.

    Runs first in `run_migrations`, before the version 17 upgrade, which
    writes into ``forecast_history``. No index lives here: `run_migrations`
    makes them in `create_indexes`, after the upgrade.
    """
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
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            last_obs_cycle_at TEXT
        );

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
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            history_next_attempt_at TEXT,
            history_last_error TEXT,
            history_error_count INTEGER NOT NULL DEFAULT 0,
            retired_at TEXT
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

        CREATE TABLE IF NOT EXISTS station_wind_records (
            station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
            obs_at TEXT NOT NULL,            -- canonical YYYY-MM-DDTHH:MM:SSZ
            source TEXT NOT NULL
                CHECK (source IN ('five_minute','current_obs','hourly')),
            -- the reading's own local date, YYYY-MM-DD
            local_date TEXT,
            speed_kmh REAL CHECK (speed_kmh IS NULL OR speed_kmh >= 0),
            temp_c REAL,
            precip_total_mm REAL
                CHECK (precip_total_mm IS NULL OR precip_total_mm >= 0),
            -- canonical; NULL for rows from before 0.16.14
            fetched_at TEXT,
            CHECK (speed_kmh IS NOT NULL OR temp_c IS NOT NULL
                   OR precip_total_mm IS NOT NULL),
            PRIMARY KEY (station_id, obs_at, source)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS station_wind_days (
            station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,        -- YYYY-MM-DD in the site time zone
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN
                    ('pending','partial','fetched','unavailable','failed')),
            record_count INTEGER NOT NULL DEFAULT 0,   -- records inside the day
            pair_hours INTEGER NOT NULL DEFAULT 0,     -- hours with a pair
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at TEXT,
            -- always via core/error_sanitize.sanitized_exception
            last_error TEXT,
            last_ok_at TEXT,                 -- last successful persist, a 204 included
            refetch_at TEXT,                 -- reconcile refetch due time
            refetched INTEGER NOT NULL DEFAULT 0 CHECK (refetched IN (0, 1)),
            updated_at TEXT NOT NULL,
            refused_at TEXT,
            PRIMARY KEY (station_id, local_date)
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
            last_usable_fetch_at TEXT,
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
            admission_basis TEXT
                CHECK(admission_basis IS NULL
                      OR admission_basis IN ('complete','deadline')),
            UNIQUE(site_id, quantity, local_date, tz_generation_id)
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
                             'verification_run','timezone_correction',
                             'day_close','day_publish')
                    AND site_id IS NOT NULL
                )
            )
        );

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

        CREATE TABLE IF NOT EXISTS site_days (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            state TEXT NOT NULL
                CHECK(state IN ('open','closing','retry_due','closed','missed')),
            day_start_utc TEXT NOT NULL,
            day_end_utc TEXT NOT NULL,
            timezone TEXT NOT NULL,
            zone_changeover INTEGER NOT NULL DEFAULT 0
                CHECK(zone_changeover IN (0,1)),
            attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 2),
            retry_at TEXT,
            last_failure TEXT,
            missed_reason TEXT
                CHECK(missed_reason IS NULL OR missed_reason IN
                      ('never_attempted','retry_failed','retry_not_run',
                       'zone_changeover')),
            created_at TEXT NOT NULL,
            state_changed_at TEXT NOT NULL,
            done_at TEXT,
            PRIMARY KEY (site_id, local_date),
            CHECK((state IN ('closed','missed')) = (done_at IS NOT NULL)),
            CHECK((state = 'missed') = (missed_reason IS NOT NULL)),
            CHECK(state <> 'open' OR attempts = 0),
            CHECK(state NOT IN ('closing','closed') OR attempts >= 1),
            CHECK(state <> 'retry_due' OR attempts = 1),
            CHECK((retry_at IS NOT NULL) =
                  (attempts = 2 OR state = 'retry_due'
                   OR missed_reason IS 'retry_not_run')),
            CHECK(zone_changeover = 0 OR state IN ('open','missed')),
            CHECK(missed_reason IS NOT 'zone_changeover' OR zone_changeover = 1)
        );

        CREATE TABLE IF NOT EXISTS station_changes (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
            change TEXT NOT NULL
                CHECK(change IN ('added','turned_on','turned_off','retired')),
            effective_date TEXT NOT NULL,
            reason TEXT NOT NULL,
            changed_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS site_day_stations (
            site_id INTEGER NOT NULL,
            local_date TEXT NOT NULL,
            station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
            -- rain release, Part A: 0 while the rain check sets the station
            -- aside; the day's rain reads only the stations with 1
            rain_counted INTEGER NOT NULL DEFAULT 1 CHECK(rain_counted IN (0,1)),
            PRIMARY KEY (site_id, local_date, station_id),
            FOREIGN KEY (site_id, local_date)
                REFERENCES site_days(site_id, local_date) ON DELETE CASCADE
        );

        -- The rain check (rain release, Part A): one row per listed station
        -- and closed day. No link to site_days, so the rows outlive day
        -- retention: the take-back rule needs the set-aside row.
        CREATE TABLE IF NOT EXISTS station_rain_checks (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
            outcome TEXT NOT NULL CHECK(outcome IN
                ('zero','agree','disagree','dry','not_widespread','cant_judge')),
            reason TEXT,
            station_mm REAL,
            peer_median_mm REAL,
            peers INTEGER NOT NULL,
            decision TEXT CHECK(decision IS NULL OR decision IN
                ('set_aside','taken_back','ambiguous','site_limit')),
            counted_after INTEGER NOT NULL CHECK(counted_after IN (0,1)),
            checked_at TEXT NOT NULL,
            PRIMARY KEY (site_id, local_date, station_id)
        );

        CREATE TABLE IF NOT EXISTS site_zone_changes (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            timezone TEXT NOT NULL,
            effective_date TEXT NOT NULL,
            entered_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS close_attempts (
            site_id INTEGER NOT NULL,
            local_date TEXT NOT NULL,
            attempt INTEGER NOT NULL CHECK(attempt IN (1,2)),
            started_at TEXT NOT NULL,
            last_stage INTEGER NOT NULL DEFAULT 1 CHECK(last_stage BETWEEN 1 AND 6),
            last_progress_at TEXT NOT NULL,
            outcome TEXT NOT NULL DEFAULT 'running'
                CHECK(outcome IN ('running','closed','failed')),
            failed_stage INTEGER CHECK(failed_stage BETWEEN 2 AND 6),
            failure_reason TEXT,
            ended_at TEXT,
            truth_version TEXT,
            PRIMARY KEY (site_id, local_date, attempt),
            FOREIGN KEY (site_id, local_date)
                REFERENCES site_days(site_id, local_date) ON DELETE CASCADE,
            CHECK((outcome = 'running') = (ended_at IS NULL)),
            CHECK((outcome = 'failed') = (failed_stage IS NOT NULL)),
            CHECK(outcome <> 'closed' OR (last_stage = 6 AND truth_version IS NOT NULL))
        );

        CREATE TABLE IF NOT EXISTS close_station_days (
            site_id INTEGER NOT NULL,
            local_date TEXT NOT NULL,
            station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
            five_minute INTEGER NOT NULL CHECK(five_minute >= 0),
            current_obs INTEGER NOT NULL CHECK(current_obs >= 0),
            hourly INTEGER NOT NULL CHECK(hourly >= 0),
            answered INTEGER NOT NULL CHECK(answered IN (0,1)),
            refused_lost INTEGER NOT NULL CHECK(refused_lost IN (0,1)),
            paused INTEGER NOT NULL CHECK(paused IN (0,1)),
            PRIMARY KEY (site_id, local_date, station_id),
            FOREIGN KEY (site_id, local_date)
                REFERENCES site_days(site_id, local_date) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS close_publications (
            site_id INTEGER NOT NULL,
            cutoff TEXT NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 1 CHECK(attempt IN (1,2)),
            state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN
                ('pending','building','published','retry_due','failed','superseded')),
            opened_at TEXT NOT NULL,
            started_at TEXT,
            retry_at TEXT,
            finished_at TEXT,
            publication_id TEXT,
            last_failure TEXT,
            PRIMARY KEY (site_id, cutoff),
            FOREIGN KEY (site_id, cutoff)
                REFERENCES site_days(site_id, local_date) ON DELETE CASCADE,
            CHECK((state = 'published') = (publication_id IS NOT NULL)),
            CHECK((state = 'retry_due') = (retry_at IS NOT NULL AND attempt = 1
                                           AND finished_at IS NULL))
        );

        CREATE TABLE IF NOT EXISTS close_stage_log (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            attempt INTEGER NOT NULL CHECK(attempt IN (1,2)),
            stage INTEGER NOT NULL CHECK(stage BETWEEN 1 AND 8),
            job_id INTEGER,
            enqueued_at TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT NOT NULL,
            queue_wait_s REAL NOT NULL,
            duration_s REAL NOT NULL,
            outcome TEXT NOT NULL
                CHECK(outcome IN ('completed','failed','superseded','skipped')),
            reason TEXT,
            detail TEXT NOT NULL DEFAULT '{}',
            CHECK((stage = 1) = (job_id IS NULL))
        );

        CREATE TABLE IF NOT EXISTS close_try_log (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            station_id INTEGER NOT NULL,
            kind TEXT NOT NULL CHECK(kind IN ('five_minute','hourly')),
            slot TEXT NOT NULL,
            close_attempt INTEGER CHECK(close_attempt IN (1,2)),
            tried_at TEXT NOT NULL,
            outcome TEXT NOT NULL,
            reason TEXT,
            detail TEXT,
            readings_kept INTEGER NOT NULL CHECK(readings_kept >= 0),
            readings_refused INTEGER NOT NULL CHECK(readings_refused >= 0),
            CHECK((slot = 'gap_fill') = (kind = 'hourly')),
            CHECK((slot = 'gap_fill') = (close_attempt IS NOT NULL))
        );

        -- Daily-close scores and publications (P4). Dates are YYYY-MM-DD,
        -- stamps canonical UTC ...Z text. No table here has a foreign key to
        -- site_days, and none carries a tz_generation_id.

        CREATE TABLE IF NOT EXISTS forecast_downloads (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            local_date TEXT NOT NULL,
            slot TEXT NOT NULL,
            round_date TEXT,
            round_try INTEGER,
            started_at TEXT NOT NULL,
            ended_at TEXT NOT NULL,
            fetched_at TEXT,
            outcome TEXT NOT NULL,
            reason TEXT,
            usable INTEGER NOT NULL,
            inserted INTEGER NOT NULL,
            labels TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS forecast_value_changes (
            download_id INTEGER NOT NULL
                REFERENCES forecast_downloads(id) ON DELETE CASCADE,
            previous_download_id INTEGER,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            variable TEXT NOT NULL,
            hours_compared INTEGER NOT NULL,
            hours_changed INTEGER NOT NULL,
            max_abs_change REAL,
            first_changed_valid_at TEXT,
            labels_changed INTEGER NOT NULL,
            PRIMARY KEY (download_id, feed_id, variable)
        );

        -- The scored value (rain release, Part B): the first stored download
        -- of each local day S, one row per feed, variable, hour and day
        -- ahead. forecast_samples keeps only the newest value.
        CREATE TABLE IF NOT EXISTS forecast_scored (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            variable TEXT NOT NULL,
            valid_at TEXT NOT NULL,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            value REAL NOT NULL,
            lead_hours INTEGER NOT NULL,
            issued_at TEXT NOT NULL,
            download_id INTEGER,
            download_date TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (site_id, feed_id, variable, valid_at, day_ahead)
        ) WITHOUT ROWID;

        -- The scored forecasts of closed days (storage release, 3.1). Stage 5
        -- writes the hours it scored, real feeds and the multi-model mean,
        -- never persistence. The truth stays in observations. The version 17
        -- upgrade also writes the days before each site's first record.
        CREATE TABLE IF NOT EXISTS forecast_history (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            variable TEXT NOT NULL,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            valid_at TEXT NOT NULL,      -- the observations stamp, same text
            local_date TEXT NOT NULL,    -- the day the hour belongs to
            value REAL NOT NULL,         -- the forecast; for the mean, the mean
            lead_hours INTEGER NOT NULL,
            issued_at TEXT,              -- NULL only for a closed day's mean
            contributors INTEGER,        -- the mean only
            PRIMARY KEY (site_id, feed_id, variable, day_ahead, valid_at)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS day_scores (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            variable TEXT NOT NULL,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            status TEXT NOT NULL,
            download_id INTEGER,
            download_date TEXT,
            fetched_at TEXT,
            n INTEGER NOT NULL,
            err_n INTEGER NOT NULL,
            err_sum REAL,
            abs_sum REAL,
            sq_sum REAL,
            hits INTEGER,
            false_alarms INTEGER,
            misses INTEGER,
            correct_negs INTEGER,
            skill_n INTEGER,
            skill_sq_sum REAL,
            skill_ref_sq_sum REAL,
            wind_fc_high REAL,
            wind_fc_hours INTEGER,
            wind_obs_high REAL,
            PRIMARY KEY (site_id, local_date, feed_id, variable, day_ahead)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS close_records (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            attempt INTEGER NOT NULL,
            truth_version TEXT NOT NULL,
            scoring_rule TEXT NOT NULL,
            scored_at TEXT NOT NULL,
            PRIMARY KEY (site_id, local_date)
        );

        CREATE TABLE IF NOT EXISTS close_record_downloads (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            download_id INTEGER,
            download_source TEXT,
            download_date TEXT,
            fetched_at TEXT,
            labels TEXT,
            PRIMARY KEY (site_id, local_date, feed_id, day_ahead)
        );

        CREATE TABLE IF NOT EXISTS starting_scores (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            variable TEXT NOT NULL,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            n INTEGER NOT NULL,
            err_n INTEGER NOT NULL,
            err_sum REAL,
            abs_sum REAL,
            sq_sum REAL,
            hits INTEGER,
            false_alarms INTEGER,
            misses INTEGER,
            correct_negs INTEGER,
            skill_n INTEGER,
            skill_sq_sum REAL,
            skill_ref_sq_sum REAL,
            PRIMARY KEY (site_id, local_date, feed_id, variable, day_ahead)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS starting_wind_days (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            forecast_high REAL,
            observed_high REAL,
            PRIMARY KEY (site_id, local_date, feed_id, day_ahead)
        );

        CREATE TABLE IF NOT EXISTS publications (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            cutoff TEXT NOT NULL,
            first_record_date TEXT NOT NULL,
            window_days INTEGER NOT NULL,
            window_start_date TEXT NOT NULL,
            wind_start_date TEXT NOT NULL,
            settings TEXT NOT NULL,
            methodology_version INTEGER NOT NULL,
            scoring_rule TEXT NOT NULL,
            wind_method TEXT NOT NULL,
            wind_constants TEXT NOT NULL,
            build_started_at TEXT NOT NULL,
            published_at TEXT NOT NULL,
            UNIQUE(site_id, cutoff)
        );

        CREATE TABLE IF NOT EXISTS publication_rankings (
            publication_id INTEGER NOT NULL
                REFERENCES publications(id) ON DELETE CASCADE,
            variable TEXT NOT NULL,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            window_key TEXT NOT NULL,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
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
            PRIMARY KEY (publication_id, variable, day_ahead, window_key, feed_id)
        );

        CREATE TABLE IF NOT EXISTS publication_wind_weights (
            publication_id INTEGER NOT NULL
                REFERENCES publications(id) ON DELETE CASCADE,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            mae_ms REAL NOT NULL,
            days INTEGER NOT NULL,
            weight REAL NOT NULL,
            PRIMARY KEY (publication_id, day_ahead, feed_id)
        );

        CREATE TABLE IF NOT EXISTS publication_coverage (
            publication_id INTEGER NOT NULL
                REFERENCES publications(id) ON DELETE CASCADE,
            variable TEXT NOT NULL,
            window_key TEXT NOT NULL,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            scored INTEGER NOT NULL,
            expected INTEGER NOT NULL,
            starting_days INTEGER NOT NULL,
            PRIMARY KEY (publication_id, variable, window_key, day_ahead, feed_id)
        );

        -- Rain (rain release, Part C). The day's "rain nearby" truth, the
        -- models' daily rain totals per day ahead, the selected rain model
        -- per cutoff and day ahead, and each publication's rain ranking.
        -- rain_days and rain_forecast_days are kept: the ranking reads 60
        -- days, and they have no link to site_days.
        CREATE TABLE IF NOT EXISTS rain_days (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            outcome TEXT NOT NULL CHECK(outcome IN ('rain','dry','unknown')),
            stations_full INTEGER NOT NULL,
            stations_partial INTEGER NOT NULL,
            PRIMARY KEY (site_id, local_date)
        );

        CREATE TABLE IF NOT EXISTS rain_forecast_days (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            total_mm REAL NOT NULL,
            PRIMARY KEY (site_id, local_date, feed_id, day_ahead)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS rain_picks (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            cutoff TEXT NOT NULL,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            feed_id INTEGER REFERENCES feeds(id) ON DELETE SET NULL,
            PRIMARY KEY (site_id, cutoff, day_ahead)
        );

        CREATE TABLE IF NOT EXISTS publication_rain_ranks (
            publication_id INTEGER NOT NULL
                REFERENCES publications(id) ON DELETE CASCADE,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            rank INTEGER NOT NULL,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            misses INTEGER NOT NULL,
            false_alarms INTEGER NOT NULL,
            common_days INTEGER NOT NULL,
            counted_days INTEGER NOT NULL,
            provisional INTEGER NOT NULL,
            selected INTEGER NOT NULL,
            in_group INTEGER NOT NULL,
            PRIMARY KEY (publication_id, day_ahead, rank)
        );
        """,
    )


def create_indexes(conn: sqlite3.Connection) -> None:
    """Every CREATE INDEX IF NOT EXISTS of the current schema, verbatim, plus
    the two code-generated forecast_samples indexes and idx_samples_slot.

    Runs after the version 17 upgrade in `run_migrations`; an index already
    there is skipped by name.
    """
    _executescript(
        conn,
        """
        CREATE INDEX IF NOT EXISTS idx_samples_site_var_valid
            ON forecast_samples(site_id, variable, valid_at);
        CREATE INDEX IF NOT EXISTS idx_samples_runs
            ON forecast_samples(site_id, feed_id, model_run_id);

        CREATE INDEX IF NOT EXISTS idx_tz_generations_site
            ON timezone_generations(site_id, state);

        CREATE INDEX IF NOT EXISTS idx_daily_truth_stale
            ON daily_truth(site_id, local_date, tz_generation_id)
            WHERE stale = 1;

        CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe
            ON jobs(type, COALESCE(site_id, -1), job_key)
            WHERE status IN ('pending','running') AND job_key IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_jobs_type_key_site
            ON jobs(type, job_key, site_id, id);

        CREATE INDEX IF NOT EXISTS idx_site_days_state
            ON site_days(site_id, state, local_date);
        CREATE INDEX IF NOT EXISTS idx_station_changes_station
            ON station_changes(station_id, effective_date);
        CREATE INDEX IF NOT EXISTS idx_site_zone_changes_site
            ON site_zone_changes(site_id, effective_date);
        CREATE INDEX IF NOT EXISTS idx_site_days_writable
            ON site_days(site_id, local_date)
            WHERE state IN ('open','closing');

        CREATE INDEX IF NOT EXISTS idx_close_stage_log_day
            ON close_stage_log(site_id, local_date, stage);
        CREATE INDEX IF NOT EXISTS idx_close_stage_log_ended
            ON close_stage_log(ended_at);
        CREATE INDEX IF NOT EXISTS idx_close_try_log_day
            ON close_try_log(site_id, local_date, station_id);
        CREATE INDEX IF NOT EXISTS idx_close_try_log_tried
            ON close_try_log(tried_at);

        CREATE INDEX IF NOT EXISTS idx_forecast_downloads_day
            ON forecast_downloads(site_id, feed_id, local_date, ended_at, id);
        """,
    )
    _sync_forecast_sample_index(conn, "idx_samples_invalid", invalid_sample_index_ddl())
    _sync_forecast_sample_index(conn, "idx_samples_recent", SAMPLES_RECENT_INDEX_DDL)
    conn.execute(SAMPLES_SLOT_INDEX_DDL)


def create_schema(conn: sqlite3.Connection) -> None:
    """Fresh-database convenience: tables, then indexes, in one call.

    Not used by `run_migrations`, which runs the version 17 upgrade between
    the two halves.
    """
    create_tables(conn)
    create_indexes(conn)


# Serves the bounded-window provider-health statements. The leading
# (site_id, feed_id, issued_at) columns seek the recent window; the trailing
# (valid_at, variable, model_run_id) columns make both windowed statements
# index-only over it -- measured 22.6% faster than the three-column form on a
# production-sized copy, at ~135 MB of index for a ~1 GB database. Same idiom
# as idx_samples_runs (shaped to cover the model-run count).
SAMPLES_RECENT_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_samples_recent "
    "ON forecast_samples(site_id, feed_id, issued_at, valid_at, "
    "variable, model_run_id)"
)

# One forecast_samples row per hour: the newest value (rain release, Part B).
# The forecast_samples upsert names these four columns as its conflict
# target.
SAMPLES_SLOT_INDEX_DDL: Final = (
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_samples_slot"
    " ON forecast_samples(site_id, feed_id, variable, valid_at)"
)


def invalid_sample_index_ddl() -> str:
    """DDL for the partial invalid-sample index, generated from the live
    stored-sample predicate (both rosters, card 7) so index and query
    (``provider_ops.bad_sample_count_sql``) stay the same string by
    construction. A file built with an older predicate is rebuilt once, at
    the next start, by ``_sync_forecast_sample_index``."""
    return (
        "CREATE INDEX IF NOT EXISTS idx_samples_invalid "
        "ON forecast_samples(site_id, feed_id) "
        f"WHERE {invalid_stored_sample_sql('forecast_samples')}"
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
            "SELECT sql FROM sqlite_master WHERE type = 'index'"
            " AND name = ? COLLATE NOCASE",
            (name,),
        ).fetchone()
        if stored is not None and (
            "ON forecast_samples" not in str(stored["sql"])
            or _index_predicate(str(stored["sql"])) != _index_predicate(ddl)
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


#: One-shot marker for the 0.12.0 Google horizon correction. Presence --
#: not value -- is the gate.
GOOGLE_HORIZON_CORRECTION_KEY = "google_horizon_correction_applied"


def correct_google_horizon(conn: sqlite3.Connection) -> None:
    """Raise the Google blend feed to the product's 7-day horizon, once.

    Not a `user_version` migration: no schema shape changes, and older
    code reads the corrected row correctly, so bumping the version
    would assert an incompatibility that does not exist.

    Targeted, not a general seed reconciliation: `seed_default_feeds`
    runs on every open, so an UPSERT-all pass would reset the three
    operator-writable columns (`enabled`, `fetch_interval_minutes`,
    `default_subscribed`) at every boot.

    The `runtime_state` marker -- not the `WHERE` clause -- is what
    makes this one-shot. The `max_lead_hours = 24` predicate is
    self-idempotent only while `max_lead_hours` is not
    operator-writable (`api/routes/feeds.py`), which is a fact about a
    different module; the marker does not depend on it.

    Statement order is the crash guard, so no SAVEPOINT is needed:
    `UPDATE` first, marker second. A crash between them leaves the row
    at 168 with no marker, and the next boot re-runs an `UPDATE` that
    matches nothing and writes the marker.
    """
    if get_runtime_state(conn, GOOGLE_HORIZON_CORRECTION_KEY) is not None:
        return
    conn.execute(
        """
        UPDATE feeds SET max_lead_hours = 168
        WHERE source = 'google' AND model = 'blend' AND max_lead_hours = 24
        """
    )
    set_runtime_state(conn, GOOGLE_HORIZON_CORRECTION_KEY, "applied")


#: One-shot marker for the 0.16.0 Open-Meteo horizon correction. Presence --
#: not value -- is the gate, matching GOOGLE_HORIZON_CORRECTION_KEY.
OPEN_METEO_HORIZON_CORRECTION_KEY = "open_meteo_horizon_correction_applied"


def correct_open_meteo_horizons(conn: sqlite3.Connection) -> None:
    """Move every Open-Meteo feed to its own request horizon, once.

    The seven original feeds were seeded at a uniform 168 h. The four
    longest models -- `ecmwf_ifs`, `gfs_global`, `gem_global` and
    `jma_gsm` -- rise to `config.DISPLAY_REQUEST_HOURS`, the hours the
    eight-day product can actually consume, rather than to their
    advertised maxima, which run far past it. `icon_global` rises to
    its own maximum, 180 h, which already sits below that cap.
    `meteofrance_arpege_world` and `ukmo_global_deterministic_10km`
    keep 168. `config.OPEN_METEO_MAX_LEAD_HOURS` is the same table
    `FEED_SEEDS` seeds from, so a fresh database and an upgraded one
    cannot drift apart.

    Not a `user_version` migration: no schema shape changes, and older
    code reads a corrected row correctly, so bumping the version would
    assert an incompatibility that does not exist.

    The `runtime_state` marker -- not the `WHERE` clause -- is what
    makes this one-shot. The `max_lead_hours = 168` predicate is
    self-idempotent only while `max_lead_hours` is not
    operator-writable (`api/routes/feeds.py`), which is a fact about a
    different module; the marker does not depend on it.

    Statement order is the crash guard, so no SAVEPOINT is needed:
    every `UPDATE` first, marker last. A crash in between leaves some
    rows corrected with no marker, and the next boot re-runs `UPDATE`s
    that match only the still-uncorrected rows before writing it.

    Targeted, not a general seed reconciliation: `seed_default_feeds`
    runs on every open, so an UPSERT-all pass would reset the
    operator-writable columns (`enabled`, `disabled_reason`,
    `fetch_interval_minutes`, `default_subscribed`) at every boot.

    Two of the seven original iterations write 168 over 168 -- a no-op
    by arithmetic, not by a special case. A model added later is seeded
    at its config value, so this pass either matches no row of it (a
    value other than 168) or writes the same value back: it can never
    change that model's row, because `max_lead_hours` is not
    operator-writable.
    """
    if get_runtime_state(conn, OPEN_METEO_HORIZON_CORRECTION_KEY) is not None:
        return
    for model, hours in config.OPEN_METEO_MAX_LEAD_HOURS.items():
        conn.execute(
            """
            UPDATE feeds SET max_lead_hours = ?
            WHERE source = 'open-meteo' AND model = ? AND max_lead_hours = 168
            """,
            (hours, model),
        )
    set_runtime_state(conn, OPEN_METEO_HORIZON_CORRECTION_KEY, "applied")


#: One-shot marker for the 0.16.0 Open-Meteo fetch-interval correction.
#: Presence -- not value -- is the gate, matching
#: OPEN_METEO_HORIZON_CORRECTION_KEY.
OPEN_METEO_INTERVAL_CORRECTION_KEY = "open_meteo_interval_correction_applied"


def correct_open_meteo_fetch_intervals(conn: sqlite3.Connection) -> None:
    """Move every Open-Meteo feed to one poll per published run, once.

    The seven original feeds were seeded at a uniform 360 minutes.
    `gem_global` updates every 12 hours, so a 6-hour poll collected each
    update twice; it moves to 720. The other six update every 6 hours
    and keep 360.
    `config.OPEN_METEO_FETCH_INTERVAL_MINUTES` is the same table
    `FEED_SEEDS` seeds from, so a fresh database and an upgraded one
    cannot drift apart.

    Not a `user_version` migration: no schema shape changes, and older
    code reads a corrected row correctly, so bumping the version would
    assert an incompatibility that does not exist.

    `fetch_interval_minutes` IS operator-writable (`update_feed` in
    `api/routes/feeds.py`), so the `fetch_interval_minutes = 360`
    predicate is what leaves an interval the operator has already changed
    exactly as the operator set it. The `runtime_state` marker -- not
    that predicate -- is what makes this one-shot: once it is written, a
    row the operator later sets back to 360 stays at 360.

    Statement order is the crash guard, so no SAVEPOINT is needed:
    every `UPDATE` first, marker last. A crash in between leaves some
    rows corrected with no marker, and the next boot re-runs `UPDATE`s
    that match only the still-uncorrected rows before writing it.

    Targeted, not a general seed reconciliation: `seed_default_feeds`
    runs on every open, so an UPSERT-all pass would reset the
    operator-writable columns (`enabled`, `disabled_reason`,
    `fetch_interval_minutes`, `default_subscribed`) at every boot.

    Six of the seven original iterations write 360 over 360 -- a no-op
    by arithmetic, not by a special case. A model added later is seeded
    at its config value, so on its row as seeded this pass either
    matches nothing (a value other than 360) or writes the same 360
    back.
    """
    if get_runtime_state(conn, OPEN_METEO_INTERVAL_CORRECTION_KEY) is not None:
        return
    for model, minutes in config.OPEN_METEO_FETCH_INTERVAL_MINUTES.items():
        conn.execute(
            """
            UPDATE feeds SET fetch_interval_minutes = ?
            WHERE source = 'open-meteo' AND model = ? AND fetch_interval_minutes = 360
            """,
            (minutes, model),
        )
    set_runtime_state(conn, OPEN_METEO_INTERVAL_CORRECTION_KEY, "applied")


#: One-shot marker for the meteoblue member drop. Presence -- not value -- is
#: the gate, matching GOOGLE_HORIZON_CORRECTION_KEY.
METEOBLUE_MEMBER_DROP_KEY = "meteoblue_member_drop_applied"


def pause_dropped_meteoblue_members(conn: sqlite3.Connection) -> None:
    """Switch off the dropped meteoblue package members, once.

    Each member named in `config.DROPPED_METEOBLUE_MEMBERS` gets
    `enabled = 0` and `config.DROPPED_METEOBLUE_REASON` as its
    `disabled_reason`. The match needs `source = 'meteoblue'` as well as
    the model name, so a feed of another source with the same model name
    and the package row itself keep their state.

    Not a `user_version` migration: no schema shape changes, and older
    code reads a paused row correctly, so bumping the version would
    assert an incompatibility that does not exist.

    The `runtime_state` marker -- not the `enabled = 1` predicate -- is
    what makes this one-shot: once it is written, a member the operator
    switches back on stays on.

    The marker is written when at least one member row exists, not when
    the `UPDATE` changed a row. Members are not seeded: they appear at
    the first package fetch, through `register_feed_if_needed` at
    `enabled = 1`. So on a database that holds none of them yet no
    marker is written and the next boot still pauses them; on one where
    every member is already off the marker is still written, so a later
    switch back on stays.

    Statement order is the crash guard, so no SAVEPOINT is needed:
    `UPDATE` first, marker last. The `UPDATE` is one statement, so it
    lands for every matching row or for none. A crash between them
    leaves the members off with no marker, and the next boot re-runs an
    `UPDATE` that matches nothing and writes the marker. Marker first
    would be the bug: a crash after it would leave the members on for
    good.
    """
    if get_runtime_state(conn, METEOBLUE_MEMBER_DROP_KEY) is not None:
        return
    members = config.DROPPED_METEOBLUE_MEMBERS
    placeholders = ", ".join("?" for _ in members)
    changed = conn.execute(
        f"""
        UPDATE feeds SET enabled = 0, disabled_reason = ?
        WHERE source = 'meteoblue' AND model IN ({placeholders}) AND enabled = 1
        """,
        (config.DROPPED_METEOBLUE_REASON, *members),
    ).rowcount
    logger.info("meteoblue member drop: %d feed(s) paused", changed)
    present = conn.execute(
        f"""
        SELECT 1 FROM feeds
        WHERE source = 'meteoblue' AND model IN ({placeholders})
        LIMIT 1
        """,
        members,
    ).fetchone()
    if present is not None:
        set_runtime_state(conn, METEOBLUE_MEMBER_DROP_KEY, "applied")


def record_precip_hour_start_boundary(conn: sqlite3.Connection) -> None:
    """Keep rain stored under end-of-hour labels out of scoring, once.

    Open-Meteo and meteoblue label rain by the hour's end; their adapters
    now store it under the hour's start (`precip_hour_start`). Stored rows
    are not relabelled. This step records the newest parseable `fetched_at`
    of their real-feed rain under `PRECIP_HOUR_START_BOUNDARY_KEY`, and
    `precip_label_eligible_sql` keeps every such row fetched at or before
    it out of pairing, rebuilds and the served forecast. The pick orders by
    `julianday`, not text: as text, a stamp with fractional seconds sorts
    below the same second without them. With no such row the boundary is
    the `1970-01-01T00:00:00Z` sentinel. No clock is read.

    Only the key is written: no pair, `score_cache`, job or
    `forecast_samples` row changes, so a file from before the boundary
    keeps its stored pairs and scores as they stand, like any other file.

    Gated on the key, not on `user_version`, whose number depends on which
    release ships first; an older backup imported later carries no key and
    gets a boundary from its own stamps. Deleting the key by hand is a
    reset: the next open records the newest stored stamp.

    The key is the step's one write, so no SAVEPOINT is needed. Nothing is
    swallowed: a failure fails the open before the `user_version` write and
    the next open retries.
    """
    if get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY) is not None:
        return
    newest = conn.execute(
        """
        SELECT fs.fetched_at
        FROM forecast_samples fs JOIN feeds f ON f.id = fs.feed_id
        WHERE fs.variable = 'precip'
          AND f.source IN ('open-meteo', 'meteoblue') AND f.is_virtual = 0
          AND julianday(fs.fetched_at) IS NOT NULL
        ORDER BY julianday(fs.fetched_at) DESC
        LIMIT 1
        """
    ).fetchone()
    boundary = "1970-01-01T00:00:00Z" if newest is None else str(newest[0])
    set_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY, boundary)
    logger.info("precip hour-start boundary recorded at %s", boundary)


# Value: when the download record (``forecast_downloads``) starts, a
# canonical whole-second UTC ``...Z`` stamp. A fresh file gets its build time.
DOWNLOAD_RECORD_SINCE_KEY: Final = "download_record_since"
# Value: when the file was last compacted, a canonical UTC ``...Z`` stamp. A
# fresh file gets its build time: it has nothing to compact.
STORAGE_COMPACTED_KEY: Final = "storage_compacted_at"

_CLOSE_OFF_REFUSAL: Final = (
    f"database schema {UPGRADE_FROM_USER_VERSION} has no {DAILY_CLOSE_ON_KEY}"
    " key; this release needs the daily close on"
)


def run_migrations(conn: sqlite3.Connection) -> None:
    """Bring the file to version 17, or refuse it.

    Refused with ``RuntimeError`` before anything is written: a file above
    17; a file at 1 to 15 (or below 0); a file at 16 whose daily close is
    off (no ``DAILY_CLOSE_ON_KEY``). Then, in order: `create_tables`; at 16
    only, `migrate_v17_storage`; on every start, `drop_forecast_record`;
    `create_indexes`; the four one-shot corrections, the three seeds and the
    rain-label boundary; at 0 only, the fresh file's three keys
    (`_record_fresh_build_keys`); last, ``user_version`` 17. A file already
    at 17 runs the same steps less the upgrade and the fresh keys.

    A stop is checked before each step (``core.stop.begin_step``), which
    also names the step for ``Database._open``'s log line. A step that does
    not run leaves ``user_version`` unchanged, and each step finds its own
    work done on a repeat, so the next start reruns from it.
    """
    row = conn.execute("PRAGMA user_version").fetchone()
    current = int(row[0]) if row is not None else 0
    if current > TARGET_USER_VERSION:
        raise RuntimeError(f"database user_version {current} is newer than this app")
    if current != 0 and current < UPGRADE_FROM_USER_VERSION:
        raise RuntimeError(
            f"database schema {current} is too old for this release;"
            " install 0.16.15 first"
        )
    if current == UPGRADE_FROM_USER_VERSION and not _close_on_recorded(conn):
        raise RuntimeError(_CLOSE_OFF_REFUSAL)
    logger.debug(
        "migrations begin user_version=%s target=%s", current, TARGET_USER_VERSION
    )
    stop.begin_step("create_tables")
    create_tables(conn)
    logger.debug("migrations tables ensured")
    if current == UPGRADE_FROM_USER_VERSION:
        migrate_v17_storage(conn)
    stop.begin_step("drop_forecast_record")
    drop_forecast_record(conn)
    stop.begin_step("create_indexes")
    create_indexes(conn)
    logger.debug("migrations indexes ensured")
    stop.begin_step("correct_google_horizon")
    correct_google_horizon(conn)
    stop.begin_step("correct_open_meteo_horizons")
    correct_open_meteo_horizons(conn)
    stop.begin_step("correct_open_meteo_fetch_intervals")
    correct_open_meteo_fetch_intervals(conn)
    stop.begin_step("pause_dropped_meteoblue_members")
    pause_dropped_meteoblue_members(conn)
    stop.begin_step("seed_default_sources")
    seed_default_sources(conn)
    stop.begin_step("seed_default_feeds")
    seed_default_feeds(conn)
    stop.begin_step("seed_default_settings")
    seed_default_settings(conn)
    logger.debug("migrations seeded sources+feeds+settings")
    stop.begin_step("record_precip_hour_start_boundary")
    record_precip_hour_start_boundary(conn)
    if current == 0:
        stop.begin_step("record_fresh_build_keys")
        _record_fresh_build_keys(conn)
    conn.execute(f"PRAGMA user_version = {TARGET_USER_VERSION}")
    logger.debug("migrations done user_version=%s", TARGET_USER_VERSION)


def _record_fresh_build_keys(conn: sqlite3.Connection) -> None:
    """A fresh file's three ``runtime_state`` keys, each written only when absent.

    ``DAILY_CLOSE_ON_KEY``: the daily close is on from the build.
    ``DOWNLOAD_RECORD_SINCE_KEY``: the download record starts at the build,
    in whole seconds. ``STORAGE_COMPACTED_KEY``: the file has nothing to
    compact. All three come from one read of this module's ``utc_now``.
    """
    now = utc_now()
    for key, value in (
        (DAILY_CLOSE_ON_KEY, isoformat_utc(now)),
        (DOWNLOAD_RECORD_SINCE_KEY, isoformat_utc(now.replace(microsecond=0))),
        (STORAGE_COMPACTED_KEY, isoformat_utc(now)),
    ):
        if get_runtime_state(conn, key) is None:
            set_runtime_state(conn, key, value)


# The runtime_state key prefix of the removed record's gap scan, matched
# exactly (no LIKE, which reads ``_`` as a wildcard).
_RECORD_GAP_SCAN_KEY_PREFIX: Final = "record_gap_scan_failures:"


def drop_forecast_record(conn: sqlite3.Connection) -> None:
    """Delete what is left of the removed daily forecast record.

    `run_migrations` calls it on every start, just after
    `migrate_v17_storage` (which runs at version 16 only). One SAVEPOINT
    holds three statements, in this order: ``DROP TABLE IF EXISTS
    forecast_of_record``; a DELETE of the ``forecast_record`` and
    ``record_gap_scan`` jobs, in any status; a DELETE of the
    ``runtime_state`` keys beginning ``record_gap_scan_failures:``. Nothing
    references the table, so the drop cascades nowhere. Every other job,
    key, setting and table is left as it is. One INFO line gives the three
    counts; on a file already done they are all 0.

    Any error rolls the savepoint back and is re-raised, so the table, jobs
    and keys stay as they were and the next start runs the step again; the
    in_transaction check covers an error that has already ended the
    transaction.
    """
    tables = 1 if _table_exists(conn, "forecast_of_record") else 0
    conn.execute("SAVEPOINT drop_forecast_record")
    try:
        conn.execute("DROP TABLE IF EXISTS forecast_of_record")
        jobs = conn.execute(
            "DELETE FROM jobs WHERE type IN ('forecast_record', 'record_gap_scan')"
        ).rowcount
        keys = conn.execute(
            "DELETE FROM runtime_state WHERE substr(key, 1, length(?1)) = ?1",
            (_RECORD_GAP_SCAN_KEY_PREFIX,),
        ).rowcount
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK TO drop_forecast_record")
            conn.execute("RELEASE drop_forecast_record")
        raise
    conn.execute("RELEASE drop_forecast_record")
    logger.info(
        "drop_forecast_record: %d table(s) dropped, %d job(s), %d key(s)",
        tables,
        jobs,
        keys,
    )


# ---------------------------------------------------------------------------
# The version 17 upgrade: one forecast history
# ---------------------------------------------------------------------------

#: The tables `migrate_v17_storage` drops, in its order: every child before
#: its parent. With foreign keys on, dropping a parent first runs an implicit
#: DELETE of its rows, which cascades row by row, or fails on a plain
#: REFERENCES with no ON DELETE action.
V17_DROP_ORDER: Final[tuple[str, ...]] = (
    "verification_evidence",
    "verification_day_context",
    "verification_results",
    "verification_verdicts",
    "verification_run_inputs",
    "verification_trigger_decisions",
    "verification_runs",
    "close_pairs",
    "forecast_pairs",
)

# The jobs the upgrade keeps, in any status: the kinds dispatch still runs,
# and fetch_obs only under the key wind-days. IS, not =, so a NULL job_key
# compares as a value.
_V17_KEPT_JOBS_SQL: Final = """
    type IN ('fetch_current_obs', 'fetch_feed', 'day_close', 'day_publish')
    OR (type = 'fetch_obs' AND job_key IS 'wind-days')
"""
# The persistence yardstick's feed id, left out of the forecast history.
_V17_PERSISTENCE_FEEDS_SQL: Final = (
    "SELECT id FROM feeds WHERE source = 'virtual' AND model = '_persistence'"
)
# The runtime_state keys of the removed code: a prefix, and one key.
_V17_REMOVED_KEY_PREFIX: Final = "verification_"
_V17_REMOVED_KEY: Final = "import_rebuild_state"
_V17_REMOVED_SETTING: Final = "verification_publish_hold"
# The day_publish stage whose jobs the upgrade deletes; the close chain's
# stages end at 7.
_V17_REMOVED_PUBLISH_STAGE: Final = 8


@dataclass(frozen=True, slots=True)
class _V17Site:
    """One site's source for the pre-close conversion: its ``published`` time
    zone generation, and the ``valid_at`` text its rows lie below (its first
    record's ``day_start_utc``; None while it has no record: every row)."""

    site_id: int
    generation_id: int
    bound: str | None


def migrate_v17_storage(conn: sqlite3.Connection) -> None:
    """Upgrade a version 16 file to one forecast history.

    `run_migrations` calls it at version 16 only, after `create_tables` made
    ``forecast_history``, and writes ``user_version`` 17 after it.

    Raises ``RuntimeError``, writing nothing, when ``DAILY_CLOSE_ON_KEY`` is
    absent. Logs ``PRAGMA secure_delete`` and sets it to 0 on this
    connection, so the drops do not overwrite each freed page; it is put
    back on the way out. With ``forecast_pairs`` gone, an earlier run
    committed every step: one INFO line "already converted", and it returns.

    Otherwise one SAVEPOINT holds five steps, each named for a stop check
    (``core.stop.begin_step``) and logged at INFO with its rows and
    milliseconds:

    1. ``v17_date_map``: the TEMP table ``v17_dates``, the local date of
       each hour the conversion reads (`_v17_date_map`);
    2. ``v17_convert_pre_close``: each site's ``forecast_pairs`` rows from
       before its first record (`_v17_convert_pre_close`);
    3. ``v17_copy_close_pairs``: the ``close_pairs`` rows
       (`_v17_copy_close_pairs`);
    4. ``v17_delete_removed_jobs_keys``: the jobs, keys and setting of
       removed code (`_v17_delete_removed_jobs_keys`);
    5. ``v17_drop_tables``: ``V17_DROP_ORDER``, then ``v17_dates``
       (`_v17_drop_tables`).

    Any error rolls the savepoint back and is re-raised, so the file stays
    at version 16 as it was and the next open runs the upgrade again; the
    in_transaction check covers an error that has already ended the
    transaction. The RELEASE commits the five steps; a stop after it and
    before the version write leaves a converted version 16 file, on which
    the next open returns at the guard.
    """
    stop.begin_step("migrate_v17_storage")
    if not _close_on_recorded(conn):
        raise RuntimeError(_CLOSE_OFF_REFUSAL)
    secure_delete = int(conn.execute("PRAGMA secure_delete").fetchone()[0])
    logger.info(
        "v17 upgrade: secure_delete is %d; set to 0 for the drops", secure_delete
    )
    conn.execute("PRAGMA secure_delete = 0")
    try:
        if not _table_exists(conn, "forecast_pairs"):
            logger.info("v17 upgrade: already converted")
            return
        conn.execute("SAVEPOINT migrate_v17")
        try:
            stop.begin_step("v17_date_map")
            sites = _v17_date_map(conn)
            stop.begin_step("v17_convert_pre_close")
            _v17_convert_pre_close(conn, sites)
            stop.begin_step("v17_copy_close_pairs")
            _v17_copy_close_pairs(conn)
            stop.begin_step("v17_delete_removed_jobs_keys")
            _v17_delete_removed_jobs_keys(conn)
            stop.begin_step("v17_drop_tables")
            _v17_drop_tables(conn)
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK TO migrate_v17")
                conn.execute("RELEASE migrate_v17")
            raise
        conn.execute("RELEASE migrate_v17")
    finally:
        restored = "FAST" if secure_delete == 2 else str(secure_delete)
        conn.execute(f"PRAGMA secure_delete = {restored}")


def _v17_date_map(conn: sqlite3.Connection) -> list[_V17Site]:
    """Step 1: fill the TEMP table ``v17_dates``; return each site's source.

    For each site, in id order, with ``core.stop.check`` once per site: its
    ``published`` time zone generation (the highest id, should it have
    several) and its bound, its first record's ``day_start_utc`` as
    ``isoformat_utc`` text. Each distinct ``valid_at`` of the site's
    ``forecast_pairs`` rows of that generation below the bound, compared as
    text, gets its local date in the generation's zone. A site with no
    ``published`` generation has no source.

    Raises ``RuntimeError`` at the first site, in id order, whose zone does
    not load (the message names the site and the zone), or any of whose
    ``valid_at`` stamps do not parse (the message names the site, how many
    do not parse, and the lowest of them as text). Every hour is mapped or
    the upgrade is refused: `migrate_v17_storage` rolls the savepoint back,
    so the file stays at version 16.
    """
    started = time.perf_counter()
    conn.execute(
        "CREATE TEMP TABLE v17_dates ("
        " site_id INTEGER NOT NULL, valid_at TEXT NOT NULL,"
        " local_date TEXT NOT NULL, PRIMARY KEY (site_id, valid_at))"
    )
    site_ids = [
        int(row[0])
        for row in conn.execute("SELECT id FROM sites ORDER BY id").fetchall()
    ]
    sites: list[_V17Site] = []
    hours = 0
    for site_id in site_ids:
        stop.check()
        generation = conn.execute(
            "SELECT id, timezone FROM timezone_generations"
            " WHERE site_id = ? AND state = 'published'"
            " ORDER BY id DESC LIMIT 1",
            (site_id,),
        ).fetchone()
        if generation is None:
            continue
        first = conn.execute(
            "SELECT day_start_utc FROM site_days WHERE site_id = ?"
            " ORDER BY local_date LIMIT 1",
            (site_id,),
        ).fetchone()
        site = _V17Site(
            site_id=site_id,
            generation_id=int(generation[0]),
            bound=None if first is None else isoformat_utc(parse_utc(str(first[0]))),
        )
        sites.append(site)
        zone_name = str(generation[1])
        try:
            zone = ZoneInfo(zone_name)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise RuntimeError(
                f"v17 upgrade: site {site_id}: zone {zone_name!r} does not load;"
                " the upgrade is refused and the file stays at version 16"
            ) from exc
        dates: list[tuple[int, str, str]] = []
        unparsed: list[str] = []
        for row in conn.execute(
            "SELECT DISTINCT valid_at FROM forecast_pairs"
            " WHERE site_id = :site_id AND tz_generation_id = :generation_id"
            " AND (:bound IS NULL OR valid_at < :bound)",
            _v17_site_binds(site),
        ).fetchall():
            valid_at = str(row[0])
            try:
                local = parse_utc(valid_at).astimezone(zone).date()
            except ValueError:
                unparsed.append(valid_at)
                continue
            dates.append((site_id, valid_at, local.isoformat()))
        if unparsed:
            raise RuntimeError(
                f"v17 upgrade: site {site_id}: {len(unparsed)} valid_at"
                f" stamp(s) do not parse, for example {min(unparsed)!r}; the"
                " upgrade is refused and the file stays at version 16"
            )
        conn.executemany(
            "INSERT INTO v17_dates (site_id, valid_at, local_date) VALUES (?, ?, ?)",
            dates,
        )
        hours += len(dates)
    logger.info(
        "v17 upgrade: v17_date_map: %d hour(s) of %d site(s), %d ms",
        hours,
        len(sites),
        _elapsed_ms(started),
    )
    return sites


def _v17_convert_pre_close(conn: sqlite3.Connection, sites: list[_V17Site]) -> None:
    """Step 2: each site's pre-close forecasts into ``forecast_history``.

    The source of a site (`_v17_date_map`): its ``forecast_pairs`` rows of
    its ``published`` generation with ``valid_at`` below its bound as text,
    persistence left out, joined to ``v17_dates`` for ``local_date``. Per key
    (site, feed, variable, day ahead, ``valid_at``) one row: the earliest
    run, ordered by ``julianday(issued_at)``, then ``issued_at``, then
    ``id``, so two text forms of one instant tie on the instant. ``value`` is
    the stored forecast; ``day_ahead``, ``lead_hours``, ``issued_at`` and
    ``contributors`` (the mean's) are copied as stored.
    """
    started = time.perf_counter()
    written = 0
    for site in sites:
        written += conn.execute(
            f"""
            INSERT INTO forecast_history
                (site_id, feed_id, variable, day_ahead, valid_at, local_date,
                 value, lead_hours, issued_at, contributors)
            SELECT site_id, feed_id, variable, day_ahead, valid_at, local_date,
                   forecast, lead_hours, issued_at, contributors
            FROM (
                SELECT fp.site_id, fp.feed_id, fp.variable, fp.day_ahead,
                       fp.valid_at, d.local_date, fp.forecast, fp.lead_hours,
                       fp.issued_at, fp.contributors,
                       ROW_NUMBER() OVER (
                           PARTITION BY fp.site_id, fp.feed_id, fp.variable,
                                        fp.day_ahead, fp.valid_at
                           ORDER BY julianday(fp.issued_at), fp.issued_at, fp.id
                       ) AS pick
                FROM forecast_pairs fp
                JOIN v17_dates d
                  ON d.site_id = fp.site_id AND d.valid_at = fp.valid_at
                WHERE fp.site_id = :site_id
                  AND fp.tz_generation_id = :generation_id
                  AND (:bound IS NULL OR fp.valid_at < :bound)
                  AND fp.feed_id NOT IN ({_V17_PERSISTENCE_FEEDS_SQL})
            )
            WHERE pick = 1
            """,
            _v17_site_binds(site),
        ).rowcount
    logger.info(
        "v17 upgrade: v17_convert_pre_close: %d row(s) written, %d ms",
        written,
        _elapsed_ms(started),
    )


def _v17_site_binds(site: _V17Site) -> dict[str, object]:
    """The named binds of one site's source reads."""
    return {
        "site_id": site.site_id,
        "generation_id": site.generation_id,
        "bound": site.bound,
    }


def _v17_copy_close_pairs(conn: sqlite3.Connection) -> None:
    """Step 3: copy the ``close_pairs`` rows into ``forecast_history``.

    Every row as it is, persistence left out: ``value`` is the stored
    forecast, the other columns of the history are copied. A plain INSERT,
    so a key clash fails the upgrade rather than being skipped.
    """
    started = time.perf_counter()
    copied = conn.execute(
        f"""
        INSERT INTO forecast_history
            (site_id, feed_id, variable, day_ahead, valid_at, local_date,
             value, lead_hours, issued_at, contributors)
        SELECT site_id, feed_id, variable, day_ahead, valid_at, local_date,
               forecast, lead_hours, issued_at, contributors
        FROM close_pairs
        WHERE feed_id NOT IN ({_V17_PERSISTENCE_FEEDS_SQL})
        """
    ).rowcount
    logger.info(
        "v17 upgrade: v17_copy_close_pairs: %d row(s), %d ms",
        copied,
        _elapsed_ms(started),
    )


def _v17_delete_removed_jobs_keys(conn: sqlite3.Connection) -> None:
    """Step 4: delete the jobs, keys and setting of removed code.

    ``jobs`` keeps only ``_V17_KEPT_JOBS_SQL``'s rows, in any status, less
    the ``day_publish`` rows of stage 8 (`_publish_stage`). ``runtime_state``
    loses every key beginning ``verification_`` (compared case-sensitively,
    no LIKE) and ``import_rebuild_state``; ``settings`` loses
    ``verification_publish_hold``.
    """
    started = time.perf_counter()
    jobs = conn.execute(f"DELETE FROM jobs WHERE NOT ({_V17_KEPT_JOBS_SQL})").rowcount
    stage_jobs = [
        (int(row[0]),)
        for row in conn.execute(
            "SELECT id, job_key FROM jobs WHERE type = 'day_publish'"
        ).fetchall()
        if _publish_stage(row[1]) == _V17_REMOVED_PUBLISH_STAGE
    ]
    conn.executemany("DELETE FROM jobs WHERE id = ?", stage_jobs)
    keys = conn.execute(
        "DELETE FROM runtime_state WHERE substr(key, 1, ?) = ? OR key = ?",
        (len(_V17_REMOVED_KEY_PREFIX), _V17_REMOVED_KEY_PREFIX, _V17_REMOVED_KEY),
    ).rowcount
    settings = conn.execute(
        "DELETE FROM settings WHERE key = ?", (_V17_REMOVED_SETTING,)
    ).rowcount
    logger.info(
        "v17 upgrade: v17_delete_removed_jobs_keys: %d job(s), %d stage-%d"
        " publish job(s), %d key(s), %d setting(s), %d ms",
        jobs,
        len(stage_jobs),
        _V17_REMOVED_PUBLISH_STAGE,
        keys,
        settings,
        _elapsed_ms(started),
    )


def _publish_stage(job_key: object) -> int | None:
    """The stage of a ``day_publish`` key ``publish:<site>:<stage>``, read as
    the close chain reads it (both numbers through ``int``); None when the
    key does not read."""
    if not isinstance(job_key, str):
        return None
    parts = job_key.split(":")
    if len(parts) != 3 or parts[0] != "publish":
        return None
    try:
        _site, stage = int(parts[1]), int(parts[2])
    except ValueError:
        return None
    return stage


def _v17_drop_tables(conn: sqlite3.Connection) -> None:
    """Step 5: ``DROP TABLE IF EXISTS`` each table of ``V17_DROP_ORDER``, in
    that order, then the TEMP table ``v17_dates``. The INFO line gives each
    drop's milliseconds."""
    started = time.perf_counter()
    timings: list[str] = []
    dropped = 0
    for table in V17_DROP_ORDER:
        if _table_exists(conn, table):
            dropped += 1
        table_started = time.perf_counter()
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        timings.append(f"{table} {_elapsed_ms(table_started)} ms")
    conn.execute("DROP TABLE IF EXISTS temp.v17_dates")
    logger.info(
        "v17 upgrade: v17_drop_tables: %d table(s) dropped (%s), %d ms",
        dropped,
        ", ".join(timings),
        _elapsed_ms(started),
    )


def _close_on_recorded(conn: sqlite3.Connection) -> bool:
    """Whether ``runtime_state`` holds ``DAILY_CLOSE_ON_KEY``. Read-only; a
    file with no ``runtime_state`` table holds no key."""
    if not _table_exists(conn, "runtime_state"):
        return False
    row = conn.execute(
        "SELECT 1 FROM runtime_state WHERE key = ?", (DAILY_CLOSE_ON_KEY,)
    ).fetchone()
    return row is not None


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """Whether the main schema has a table named ``table``. Read-only."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _elapsed_ms(started: float) -> int:
    """Whole milliseconds since ``started``, a ``time.perf_counter`` value."""
    return int((time.perf_counter() - started) * 1000)
