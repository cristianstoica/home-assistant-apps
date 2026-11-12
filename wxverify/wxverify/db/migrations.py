"""SQLite schema, insert-only boot seeds, the one upgrade to version 18, and
the one-time deletion of the Meteosource rows stored before its UTC fix."""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Final

from wxverify import config
from wxverify.collection.forecast_validation import (
    invalid_stored_sample_sql,
    out_of_range_display_sql,
)
from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
from wxverify.core import stop
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.site_days import DAILY_CLOSE_ON_KEY
from wxverify.db.switch import prune_wind_history

logger = logging.getLogger(__name__)

TARGET_USER_VERSION = 18

# The one version run_migrations upgrades from; 1 to 16 are refused.
UPGRADE_FROM_USER_VERSION: Final = 17


def _executescript(conn: sqlite3.Connection, script: str) -> None:
    conn.executescript(script)


def create_tables(conn: sqlite3.Connection) -> None:
    """Every CREATE TABLE IF NOT EXISTS of the current schema, verbatim, and
    the three ``sites`` triggers on ``backfill_status``.

    Runs first in `run_migrations`. No index lives here: `run_migrations`
    makes them in `create_indexes`, before the version 18 upgrade.
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
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            pause_reason TEXT
                CHECK(pause_reason IS NULL OR pause_reason IN ('offline','not_found')),
            pause_evidence_count INTEGER NOT NULL DEFAULT 0
                CHECK(pause_evidence_count >= 0
                      AND (pause_evidence_count = 0) = (pause_reason IS NULL))
        );

        CREATE TABLE IF NOT EXISTS station_wind_constant (
            station_id INTEGER PRIMARY KEY REFERENCES stations(id) ON DELETE CASCADE,
            speed_kmh REAL NOT NULL,
            since TEXT NOT NULL,   -- canonical UTC stamp of the run's first hour
            state TEXT NOT NULL CHECK(state IN ('suspected','cant_assess'))
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

        -- The ledger of the one-off Open-Meteo archive fill: one row per
        -- cell (site, feed, variable, day ahead, local date) of a day before
        -- the site's first record that the fill handled. The 'written' rows
        -- are exactly the forecast_history rows it added.
        CREATE TABLE IF NOT EXISTS om_archive_cells (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            variable TEXT NOT NULL,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            local_date TEXT NOT NULL,          -- D
            download_date TEXT NOT NULL,       -- S = D - day_ahead
            outcome TEXT NOT NULL CHECK(outcome IN
                ('written','present','no_hours','no_run','refused')),
            hours INTEGER NOT NULL,            -- rows written; 0 unless written
            run_at TEXT,                       -- the newest run used; NULL for
                                               -- no_run, present and refused
            filled_at TEXT NOT NULL,
            PRIMARY KEY (site_id, feed_id, variable, day_ahead, local_date)
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

    Runs before the version 18 upgrade in `run_migrations`; an index already
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

    Not used by `run_migrations`, which runs `drop_forecast_record` between
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

# One forecast_samples row per hour: the newest value.
# The forecast_samples upsert names these four columns as its conflict
# target.
SAMPLES_SLOT_INDEX_DDL: Final = (
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_samples_slot"
    " ON forecast_samples(site_id, feed_id, variable, valid_at)"
)


def invalid_sample_index_ddl() -> str:
    """DDL for the partial invalid-sample index, generated from the live
    stored-sample predicate (both rosters) so index and query
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

    Only the key is written: no pair, job or `forecast_samples` row
    changes, so a file from before the boundary
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


# Value: when the file was last compacted, a canonical UTC ``...Z`` stamp. A
# fresh file gets its build time: it has nothing to compact.
STORAGE_COMPACTED_KEY: Final = "storage_compacted_at"

# Value: when `delete_pre_utc_meteosource` ran, a canonical UTC ``...Z``
# stamp. Presence, not value, is the gate. A fresh file gets its build time:
# it has no Meteosource row to delete.
METEOSOURCE_UTC_FIX_KEY: Final = "meteosource_utc_fix_at"

#: The Meteosource feeds: every row of one is cleared by
#: `delete_pre_utc_meteosource`.
METEOSOURCE_FEED_IDS_SQL: Final = "SELECT id FROM feeds WHERE source = 'meteosource'"

#: Every table with a ``feed_id`` column but ``site_feed_state`` (which holds
#: the off switch and is kept), in the order `delete_pre_utc_meteosource`
#: clears them: the value changes before their downloads, and ``rain_picks``,
#: whose ``feed_id`` is set to NULL rather than its row deleted, last.
METEOSOURCE_PRUNE_TABLES: Final[tuple[str, ...]] = (
    "forecast_value_changes",
    "forecast_downloads",
    "forecast_samples",
    "forecast_scored",
    "om_archive_cells",
    "forecast_history",
    "day_scores",
    "close_record_downloads",
    "starting_scores",
    "starting_wind_days",
    "rain_forecast_days",
    "publication_rankings",
    "publication_coverage",
    "publication_wind_weights",
    "publication_rain_ranks",
    "rain_picks",
)

# The tables of METEOSOURCE_PRUNE_TABLES with no site_id column, cleared by
# feed id alone; every other one is cleared per site and feed.
_FEED_KEYED_PRUNE_TABLES: Final = frozenset(
    {
        "forecast_value_changes",
        "publication_rankings",
        "publication_coverage",
        "publication_wind_weights",
        "publication_rain_ranks",
    }
)


def delete_pre_utc_meteosource(conn: sqlite3.Connection) -> None:
    """Clear every stored Meteosource row, once.

    Every row of a Meteosource feed (`METEOSOURCE_FEED_IDS_SQL`) was written
    before its adapter asked for UTC, so each sits at the wrong hour. Returns
    at once when `METEOSOURCE_UTC_FIX_KEY` is present; the check asks only
    whether the row exists, so it works whatever the connection's
    ``row_factory`` is. Otherwise one SAVEPOINT clears the tables of
    `METEOSOURCE_PRUNE_TABLES`, in its order:
    ``DELETE ... WHERE site_id = ? AND feed_id = ?`` for each site and
    Meteosource feed, or ``WHERE feed_id = ?`` for each Meteosource feed in
    the tables with no ``site_id``; ``rain_picks`` gets ``SET feed_id =
    NULL``, its own ``ON DELETE SET NULL`` meaning. Last, the key. The
    ``feeds`` row, ``site_feed_state`` (the off switch), ``jobs``,
    ``api_budget`` and every row of every other feed stay as they are. Plain
    ``conn.execute`` only: nothing here ends the transaction. After the
    RELEASE, one INFO line gives the count of each table, in the same order
    (``rain_picks_cleared`` for ``rain_picks``), and the time taken.

    Any error rolls the savepoint back and is re-raised, so every table
    stays as it was, the key stays absent, and the next start runs the step
    again; the in_transaction check covers an error that has already ended
    the transaction.
    """
    present = conn.execute(
        "SELECT 1 FROM runtime_state WHERE key = ?", (METEOSOURCE_UTC_FIX_KEY,)
    ).fetchone()
    if present is not None:
        return
    started = time.perf_counter()
    counts: dict[str, int] = {}
    conn.execute("SAVEPOINT delete_pre_utc_meteosource")
    try:
        site_ids = [int(row[0]) for row in conn.execute("SELECT id FROM sites")]
        feed_ids = [int(row[0]) for row in conn.execute(METEOSOURCE_FEED_IDS_SQL)]
        for table in METEOSOURCE_PRUNE_TABLES:
            counts[table] = _prune_meteosource_table(conn, table, site_ids, feed_ids)
        set_runtime_state(conn, METEOSOURCE_UTC_FIX_KEY, isoformat_utc(utc_now()))
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK TO delete_pre_utc_meteosource")
            conn.execute("RELEASE delete_pre_utc_meteosource")
        raise
    conn.execute("RELEASE delete_pre_utc_meteosource")
    tokens = " ".join(
        f"{'rain_picks_cleared' if table == 'rain_picks' else table}={count}"
        for table, count in counts.items()
    )
    logger.info("meteosource utc fix: deleted %s, %d ms", tokens, _elapsed_ms(started))


def _prune_meteosource_table(
    conn: sqlite3.Connection,
    table: str,
    site_ids: list[int],
    feed_ids: list[int],
) -> int:
    """Clear the Meteosource rows of one table of `METEOSOURCE_PRUNE_TABLES`;
    the number of rows deleted (cleared, for ``rain_picks``)."""
    count = 0
    if table in _FEED_KEYED_PRUNE_TABLES:
        for feed_id in feed_ids:
            count += conn.execute(
                f"DELETE FROM {table} WHERE feed_id = ?", (feed_id,)
            ).rowcount
        return count
    statement = (
        f"UPDATE {table} SET feed_id = NULL WHERE site_id = ? AND feed_id = ?"
        if table == "rain_picks"
        else f"DELETE FROM {table} WHERE site_id = ? AND feed_id = ?"
    )
    for site_id in site_ids:
        for feed_id in feed_ids:
            count += conn.execute(statement, (site_id, feed_id)).rowcount
    return count


def run_migrations(conn: sqlite3.Connection) -> None:
    """Bring the file to version 18, or refuse it.

    Refused with ``RuntimeError`` before anything is written: a file above
    18 (``newer than this app``); a file at 1 to 16 (or below 0), with
    ``install 0.16.17 first``. Then, in order: `create_tables`; on every
    start, `drop_forecast_record`; `create_indexes`; at 17 only,
    `migrate_v18` (the station poll-pause columns and their carry-over,
    the drop of the unused write state, the one-time prune of the old wind
    history, then the delete of the stored out-of-range display values, in
    one savepoint); the four one-shot corrections, the three seeds and the
    rain-label boundary; at 0 only, the fresh file's three keys
    (`_record_fresh_build_keys`); `delete_pre_utc_meteosource`, which
    returns at its key on a fresh file and on any file it has already
    cleared; last, ``user_version`` 18. A file already at 18 runs the same
    steps less the upgrade and the fresh keys.

    A stop is checked before each step (``core.stop.begin_step``), which
    also names the step for ``Database._open``'s log line; `migrate_v18`
    checks again before each of its sub-steps. A step that does
    not run leaves ``user_version`` unchanged, and each step finds its own
    work done on a repeat, so the next start reruns from it. `migrate_v18`
    writes ``user_version`` 18 inside its own savepoint, so a stop after it
    leaves an upgraded file, which the next start opens at 18; the steps
    after it, the fresh keys aside, run on every start.
    """
    row = conn.execute("PRAGMA user_version").fetchone()
    current = int(row[0]) if row is not None else 0
    if current > TARGET_USER_VERSION:
        raise RuntimeError(f"database user_version {current} is newer than this app")
    if current != 0 and current < UPGRADE_FROM_USER_VERSION:
        raise RuntimeError(
            f"database schema {current} is too old for this release;"
            " install 0.16.17 first"
        )
    logger.debug(
        "migrations begin user_version=%s target=%s", current, TARGET_USER_VERSION
    )
    stop.begin_step("create_tables")
    create_tables(conn)
    logger.debug("migrations tables ensured")
    stop.begin_step("drop_forecast_record")
    drop_forecast_record(conn)
    stop.begin_step("create_indexes")
    create_indexes(conn)
    logger.debug("migrations indexes ensured")
    if current == UPGRADE_FROM_USER_VERSION:
        stop.begin_step("migrate_v18")
        migrate_v18(conn)
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
    stop.begin_step("delete_pre_utc_meteosource")
    delete_pre_utc_meteosource(conn)
    conn.execute(f"PRAGMA user_version = {TARGET_USER_VERSION}")
    logger.debug("migrations done user_version=%s", TARGET_USER_VERSION)


def _record_fresh_build_keys(conn: sqlite3.Connection) -> None:
    """A fresh file's three ``runtime_state`` keys, each written only when
    absent.

    ``DAILY_CLOSE_ON_KEY``: the daily close's switch time is the build's.
    ``STORAGE_COMPACTED_KEY``: the file has nothing to compact.
    ``METEOSOURCE_UTC_FIX_KEY``: the file has no Meteosource row to delete.
    All three come from one read of this module's ``utc_now``.
    """
    now = utc_now()
    for key, value in (
        (DAILY_CLOSE_ON_KEY, isoformat_utc(now)),
        (STORAGE_COMPACTED_KEY, isoformat_utc(now)),
        (METEOSOURCE_UTC_FIX_KEY, isoformat_utc(now)),
    ):
        if get_runtime_state(conn, key) is None:
            set_runtime_state(conn, key, value)


# The runtime_state key prefix of the removed record's gap scan, matched
# exactly (no LIKE, which reads ``_`` as a wildcard).
_RECORD_GAP_SCAN_KEY_PREFIX: Final = "record_gap_scan_failures:"


def drop_forecast_record(conn: sqlite3.Connection) -> None:
    """Delete what is left of the removed daily forecast record.

    `run_migrations` calls it on every start, just after `create_tables`.
    One SAVEPOINT holds three statements, in this order: ``DROP TABLE IF EXISTS
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
# The version 18 upgrade
# ---------------------------------------------------------------------------


def migrate_v18(conn: sqlite3.Connection) -> None:
    """Upgrade a version 17 file to version 18.

    `run_migrations` calls it at version 17 only, after `create_indexes`,
    under the stop check ``migrate_v18``. Logs ``PRAGMA secure_delete`` and
    sets it to 0 on this connection, so the deletes do not overwrite each
    freed page; the ``finally`` puts it back, whatever happens.

    One SAVEPOINT holds the upgrade, each sub-step under a stop check of
    its own name: `_v18_poll_pause` (``v18_poll_pause``), then
    `_v18_drop_unused_write_state` (``v18_drop_unused_write_state``), then
    `_v18_prune_wind_history` (``v18_prune_wind_history``), then
    `_v18_drop_bad_display_values` (``v18_drop_bad_display_values``), then
    ``user_version`` 18. The version
    sits in the file header, which the savepoint rolls back with everything
    else. `create_indexes` has run first, so ``idx_samples_invalid`` holds
    the live predicate the last sub-step's ``INDEXED BY`` needs.
    `create_tables`'s ``executescript`` has already committed any outer
    transaction, so the RELEASE is the commit, and nothing inside the
    savepoint may end the transaction (no ``executescript``, commit,
    rollback, ``BEGIN`` or ``VACUUM``).

    Any error, a stop at a sub-step's check included, rolls the savepoint
    back and is re-raised, so the file stays at version 17 as it was and
    the next open runs the upgrade again; the in_transaction check covers
    an error that has already ended the transaction.
    """
    secure_delete = int(conn.execute("PRAGMA secure_delete").fetchone()[0])
    logger.info(
        "v18 upgrade: secure_delete is %d; set to 0 for the deletes", secure_delete
    )
    conn.execute("PRAGMA secure_delete = 0")
    try:
        conn.execute("SAVEPOINT migrate_v18")
        try:
            stop.begin_step("v18_poll_pause")
            _v18_poll_pause(conn)
            stop.begin_step("v18_drop_unused_write_state")
            _v18_drop_unused_write_state(conn)
            stop.begin_step("v18_prune_wind_history")
            _v18_prune_wind_history(conn)
            stop.begin_step("v18_drop_bad_display_values")
            _v18_drop_bad_display_values(conn)
            conn.execute("PRAGMA user_version = 18")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK TO migrate_v18")
                conn.execute("RELEASE migrate_v18")
            raise
        conn.execute("RELEASE migrate_v18")
    finally:
        restored = "FAST" if secure_delete == 2 else str(secure_delete)
        conn.execute(f"PRAGMA secure_delete = {restored}")


# The two poll-pause station_poll_state columns, as `create_tables` writes them,
# pause_reason first: the count's CHECK names it.
_V18_POLL_PAUSE_COLUMNS: Final = (
    (
        "pause_reason",
        "pause_reason TEXT CHECK(pause_reason IS NULL OR pause_reason IN"
        " ('offline','not_found'))",
    ),
    (
        "pause_evidence_count",
        "pause_evidence_count INTEGER NOT NULL DEFAULT 0"
        " CHECK(pause_evidence_count >= 0"
        " AND (pause_evidence_count = 0) = (pause_reason IS NULL))",
    ),
)


def _v18_poll_pause(conn: sqlite3.Connection) -> None:
    """Add the two poll-pause columns and recheck the stations parked for a day.

    A sub-step of `migrate_v18`, run inside its savepoint. ``PRAGMA
    table_info(station_poll_state)`` lists the columns present, and
    ``pause_reason``, then ``pause_evidence_count``, is each added with
    ``ALTER TABLE ... ADD COLUMN`` only when absent, so a file this release
    built and a repeat add none. The column text is `create_tables`' own,
    so a fresh and an upgraded table compare equal. Then one UPDATE takes
    each ``offline`` row, and each ``terminal`` row whose last error is
    ``http 404``, as one evidence reply: count 1, reason ``offline`` or
    ``not_found``, and ``next_poll_at`` now when it is unset or later, so
    the station is due at once rather than at the end of its day-long park.
    Rows with any other error keep count 0. Plain ``conn.execute`` only:
    nothing here ends the transaction. One INFO line gives the counts and
    the time taken.
    """
    started = time.perf_counter()
    present = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(station_poll_state)").fetchall()
    }
    added = 0
    for name, ddl in _V18_POLL_PAUSE_COLUMNS:
        if name not in present:
            conn.execute(f"ALTER TABLE station_poll_state ADD COLUMN {ddl}")
            added += 1
    rechecked = conn.execute(
        """
        UPDATE station_poll_state
        SET pause_reason = CASE health_state
                               WHEN 'offline' THEN 'offline' ELSE 'not_found' END,
            pause_evidence_count = 1,
            next_poll_at = CASE WHEN next_poll_at IS NULL OR next_poll_at > :now
                                THEN :now ELSE next_poll_at END
        WHERE health_state = 'offline'
           OR (health_state = 'terminal' AND last_error = 'http 404')
        """,
        {"now": isoformat_utc(utc_now())},
    ).rowcount
    logger.info(
        "v18 upgrade: station poll pause columns added=%d, "
        "%d station(s) set to recheck, %d ms",
        added,
        rechecked,
        _elapsed_ms(started),
    )


def _v18_drop_unused_write_state(conn: sqlite3.Connection) -> None:
    """Drop the score table and the download-record key nothing uses.

    A sub-step of `migrate_v18`, run inside its savepoint. Two statements:
    ``DROP TABLE IF EXISTS score_cache`` (nothing writes, reads or
    references it, so the drop cascades nowhere), then a DELETE of the
    ``runtime_state`` row whose key is exactly ``download_record_since``
    (read by nothing). The table's row count is read before the drop, when
    the table exists. Plain ``conn.execute`` only: nothing here ends the
    transaction. One INFO line gives the counts and the time taken.
    """
    started = time.perf_counter()
    dropped = 0
    rows = 0
    if _table_exists(conn, "score_cache"):
        dropped = 1
        rows = int(conn.execute("SELECT COUNT(*) FROM score_cache").fetchone()[0])
    conn.execute("DROP TABLE IF EXISTS score_cache")
    removed = conn.execute(
        "DELETE FROM runtime_state WHERE key = 'download_record_since'"
    ).rowcount
    logger.info(
        "v18 upgrade: score_cache dropped=%d (%d row(s)), "
        "download_record_since removed=%d, %d ms",
        dropped,
        rows,
        removed,
        _elapsed_ms(started),
    )


def _v18_prune_wind_history(conn: sqlite3.Connection) -> None:
    """Delete the old wind history from before each site's first closed day.

    A sub-step of `migrate_v18`, run inside its savepoint: one call of
    `wxverify.db.switch.prune_wind_history`, whose statements are plain
    ``conn.execute`` only, so nothing here ends the transaction. One INFO
    line gives the rows deleted, the ids of the sites kept because their
    first day is missing or not done (as plain integers inside brackets,
    ``[]`` when none), and the time taken.
    """
    started = time.perf_counter()
    result = prune_wind_history(conn)
    kept = "[" + ", ".join(str(site_id) for site_id in result.kept_sites) + "]"
    logger.info(
        "v18 upgrade: wind history prune: %d row(s) deleted; "
        "kept at sites %s (first day not done), %d ms",
        result.deleted,
        kept,
        _elapsed_ms(started),
    )


def _v18_drop_bad_display_values(conn: sqlite3.Connection) -> None:
    """Delete the stored display values outside their range.

    A sub-step of `migrate_v18`, run inside its savepoint, after
    `create_indexes`. One DELETE on ``forecast_samples`` ``INDEXED BY
    idx_samples_invalid``: its first condition is the index's own predicate
    (`invalid_stored_sample_sql`), word for word, so the delete reads only
    the invalid rows; its second (`out_of_range_display_sql`, generated
    from ``DISPLAY_VALUE_RANGES``) keeps it to display rows out of range.
    Scored invalid rows and display rows invalid for another reason (a
    malformed stamp) stay. The writer
    (`wxverify.collection.forecast_fetcher.persist_fetch_result`) no longer
    stores such values, so only rows written before version 18 match.
    Plain ``conn.execute`` only: nothing here ends the transaction. One
    INFO line gives the rows deleted and the time taken.
    """
    started = time.perf_counter()
    deleted = conn.execute(
        "DELETE FROM forecast_samples INDEXED BY idx_samples_invalid"
        f" WHERE {invalid_stored_sample_sql('forecast_samples')}"
        f" AND {out_of_range_display_sql('forecast_samples')}"
    ).rowcount
    logger.info(
        "v18 upgrade: %d out-of-range display value(s) deleted, %d ms",
        deleted,
        _elapsed_ms(started),
    )


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """Whether the main schema has a table named ``table``. Read-only."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _elapsed_ms(started: float) -> int:
    """Whole milliseconds since ``started``, a ``time.perf_counter`` value."""
    return int((time.perf_counter() - started) * 1000)
