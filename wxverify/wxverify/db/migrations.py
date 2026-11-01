"""SQLite schema and insert-only boot seeds."""

from __future__ import annotations

import functools
import logging
import sqlite3
import time
from datetime import date, timedelta
from typing import Final

from wxverify import config
from wxverify.collection.forecast_validation import invalid_forecast_sample_sql
from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro, utc_now
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.switch import migrate_v15_switch

logger = logging.getLogger(__name__)

TARGET_USER_VERSION = 16

# Seed offset applied per station when migrate_v3 backfills station_poll_state,
# so cold-start polls fan out instead of bursting all at once.
POLL_SEED_STAGGER_SECONDS = 10


def _executescript(conn: sqlite3.Connection, script: str) -> None:
    conn.executescript(script)


def create_tables(conn: sqlite3.Connection) -> None:
    """Every CREATE TABLE IF NOT EXISTS of the current schema, verbatim.

    Runs before the version migrations in `run_migrations`: `migrate_v3` and
    `migrate_v4` seed tables a pre-v4 file does not have. No index lives here
    -- an index may name a column that only a version migration adds (see
    `create_indexes`).
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
            -- the publication the record used: plain values with no foreign key
            publication_id INTEGER,
            publication_cutoff TEXT,
            publication_age_days INTEGER,
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
                             'verification_run','timezone_correction',
                             'day_close','day_publish')
                    AND site_id IS NOT NULL
                )
            )
        );

        CREATE TABLE IF NOT EXISTS verification_runs (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            tz_generation_id INTEGER NOT NULL
                REFERENCES timezone_generations(id),
            methodology_version INTEGER NOT NULL,
            app_version TEXT NOT NULL,
            state TEXT NOT NULL
                CHECK(state IN ('running','published','failed')),
            attempt INTEGER NOT NULL,
            config_snapshot TEXT NOT NULL,
            period_start TEXT,
            period_end TEXT,
            settled_through TEXT,
            bootstrap_seed INTEGER NOT NULL,
            bootstrap_resamples INTEGER NOT NULL,
            input_fingerprint TEXT NOT NULL,
            aggregate_state TEXT,
            error TEXT,
            created_at TEXT NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            published_at TEXT,
            result_basis_fingerprint TEXT,
            worksheet_kept INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS verification_evidence (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL
                REFERENCES verification_runs(id) ON DELETE CASCADE,
            snapshot_local_date TEXT NOT NULL,
            target_local_date TEXT NOT NULL,
            lead INTEGER NOT NULL CHECK(lead BETWEEN 0 AND 7),
            variable TEXT NOT NULL
                CHECK(variable IN ('temperature','wind','precip')),
            quantity TEXT NOT NULL CHECK(quantity IN
                ('temperature_high','temperature_low','wind_max',
                 'precip_total','precip_occurrence')),
            entity_type TEXT NOT NULL CHECK(entity_type IN
                ('depth','feed','baseline_persistence',
                 'baseline_all_feed_mean','baseline_always_dry',
                 'daily_rank_depth')),
            entity_key TEXT NOT NULL,
            predicted REAL,
            forecast_eligible INTEGER NOT NULL
                CHECK(forecast_eligible IN (0,1)),
            forecast_exclusion_reason TEXT,
            covered_hours INTEGER,
            realized_contributors INTEGER,
            truth_value REAL,
            truth_eligible INTEGER NOT NULL CHECK(truth_eligible IN (0,1)),
            truth_exclusion_reason TEXT,
            truth_covered_hours INTEGER,
            truth_wet_hours INTEGER,
            truth_dry_hours INTEGER,
            abs_error REAL,
            occurrence_outcome TEXT
                CHECK(occurrence_outcome IS NULL OR occurrence_outcome IN
                    ('hit','miss','false_alarm','correct_negative')),
            UNIQUE(run_id, snapshot_local_date, lead, variable, quantity,
                   entity_type, entity_key)
        );

        CREATE TABLE IF NOT EXISTS verification_day_context (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL
                REFERENCES verification_runs(id) ON DELETE CASCADE,
            snapshot_local_date TEXT NOT NULL,
            snapshot_utc TEXT NOT NULL,
            knowability_exclusions TEXT NOT NULL,
            null_availability_samples INTEGER NOT NULL,
            UNIQUE(run_id, snapshot_local_date)
        );

        CREATE TABLE IF NOT EXISTS verification_results (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL
                REFERENCES verification_runs(id) ON DELETE CASCADE,
            variable TEXT NOT NULL,
            lead INTEGER NOT NULL,
            quantity TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            headline INTEGER NOT NULL CHECK(headline IN (0,1)),
            common_days INTEGER NOT NULL,
            mae REAL,
            bias REAL,
            rmse REAL,
            hits INTEGER,
            misses INTEGER,
            false_alarms INTEGER,
            correct_negatives INTEGER,
            ets REAL,
            availability_rate REAL,
            delta_vs_incumbent REAL,
            detail TEXT,
            UNIQUE(run_id, variable, lead, quantity, entity_type, entity_key)
        );

        CREATE TABLE IF NOT EXISTS verification_verdicts (
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL
                REFERENCES verification_runs(id) ON DELETE CASCADE,
            variable TEXT NOT NULL,
            outcome TEXT NOT NULL CHECK(outcome IN
                ('recommend','retain_incumbent','mixed_by_lead',
                 'mixed_by_quantity','insufficient_evidence','skipped')),
            recommended_depth INTEGER,
            incumbent_depth INTEGER NOT NULL,
            tested_family TEXT NOT NULL,
            UNIQUE(run_id, variable)
        );

        CREATE TABLE IF NOT EXISTS verification_run_inputs (
            run_id INTEGER NOT NULL
                REFERENCES verification_runs(id) ON DELETE CASCADE,
            component TEXT NOT NULL,
            value TEXT NOT NULL,
            scope TEXT NOT NULL,
            PRIMARY KEY (run_id, component)
        );

        CREATE TABLE IF NOT EXISTS verification_trigger_decisions (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            trigger_date TEXT NOT NULL,
            decided_at TEXT NOT NULL
                DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            decision TEXT NOT NULL CHECK(decision IN
                ('run_started','no_change_skip','suppressed_because_active',
                 'skipped')),
            reason TEXT,
            input_fingerprint TEXT,
            run_id INTEGER REFERENCES verification_runs(id)
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

        CREATE TABLE IF NOT EXISTS close_pairs (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            variable TEXT NOT NULL,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            valid_at TEXT NOT NULL,
            lead_hours INTEGER NOT NULL,
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
            ref_sq_error REAL,
            download_id INTEGER,
            download_date TEXT,
            issued_at TEXT,
            fetched_at TEXT,
            contributors INTEGER,
            PRIMARY KEY (site_id, local_date, feed_id, variable, day_ahead,
                         valid_at, lead_hours)
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


@functools.cache
def schema_table_names() -> tuple[str, ...]:
    """Names of every table `create_tables` makes, sorted, read back from SQLite.

    Built on first use against a private in-memory database, so the list
    cannot drift from the DDL, and importing this module runs no SQL.
    """
    conn = sqlite3.connect(":memory:")
    try:
        create_tables(conn)
        return tuple(
            sorted(
                str(name_row[0])
                for name_row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            )
        )
    finally:
        conn.close()


def create_indexes(conn: sqlite3.Connection) -> None:
    """Every CREATE INDEX IF NOT EXISTS of the current schema, verbatim, plus
    the two code-generated forecast_samples indexes and idx_samples_slot.

    Runs after the version migrations in `run_migrations`: by then every
    column an index names exists on any supported input (idx_pairs_winrate
    names tz_generation_id, which `migrate_v4` adds by rebuilding
    forecast_pairs), and an index a migration already recreated is skipped by
    name. On a fresh file the order is immaterial.
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

        CREATE INDEX IF NOT EXISTS idx_pairs_leaderboard
            ON forecast_pairs(site_id, variable, day_ahead, valid_at);
        CREATE INDEX IF NOT EXISTS idx_pairs_cell
            ON forecast_pairs(site_id, feed_id, variable, day_ahead, valid_at);
        CREATE INDEX IF NOT EXISTS idx_pairs_winrate
            ON forecast_pairs(site_id, variable, day_ahead, feed_id,
                              valid_at, issued_at DESC, abs_error,
                              tz_generation_id);

        CREATE INDEX IF NOT EXISTS idx_daily_truth_stale
            ON daily_truth(site_id, local_date, tz_generation_id)
            WHERE stale = 1;

        CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_dedupe
            ON jobs(type, COALESCE(site_id, -1), job_key)
            WHERE status IN ('pending','running') AND job_key IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_jobs_type_key_site
            ON jobs(type, job_key, site_id, id);

        CREATE INDEX IF NOT EXISTS idx_verification_runs_site
            ON verification_runs(site_id, state, id);

        CREATE INDEX IF NOT EXISTS idx_verification_evidence_cell
            ON verification_evidence(run_id, entity_type, quantity, lead,
                                     target_local_date);

        CREATE INDEX IF NOT EXISTS idx_verification_trigger_site_date
            ON verification_trigger_decisions(site_id, trigger_date, id);

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

    Not used by `run_migrations`, which runs the version migrations between
    the two halves. On a file whose tables predate a column an index names,
    this raises `sqlite3.OperationalError` exactly as the pre-split runner did.
    """
    create_tables(conn)
    create_indexes(conn)


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

# One forecast_samples row per hour: the newest value (rain release, Part B).
# The forecast_samples upsert names these four columns as its conflict
# target. A file below version 16 can hold several rows per hour, so
# migrate_v16_rain_release makes the index after it deletes the extra rows.
SAMPLES_SLOT_INDEX_DDL: Final = (
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_samples_slot"
    " ON forecast_samples(site_id, feed_id, variable, valid_at)"
)

# migrate_v16_rain_release's own index, made before its carry-over and
# dropped right after the rain history fill, inside the migration's
# SAVEPOINT, so no file ever keeps it. It serves the inferred-download reads
# of scoring/downloads.py (_inferred_download), which no other index does:
# each fixes site_id and feed_id, then a fetched_at range or value, then
# reads issued_at and tests source_raw, so with all five columns each reads
# only this index.
V16_FETCHED_INDEX: Final = "tmp_v16_samples_fetched"
V16_FETCHED_INDEX_DDL: Final = (
    f"CREATE INDEX {V16_FETCHED_INDEX}"
    " ON forecast_samples(site_id, feed_id, fetched_at, issued_at, source_raw)"
)
# The forecast_samples indexes migrate_v16_rain_release drops before
# keep_newest_samples' delete, which would otherwise update each of them for
# every row it deletes. create_indexes, which run_migrations calls after the
# version migrations, makes all four again.
V16_DROPPED_SAMPLE_INDEXES: Final = (
    "idx_samples_site_var_valid",
    "idx_samples_runs",
    "idx_samples_recent",
    "idx_samples_invalid",
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


#: One-time bootstrap decision marker (D2). Written exactly once per
#: database, in either branch, on the first boot that reaches
#: `run_migrations`. Its presence -- not its value -- is the gate: once
#: written it is never rewritten or deleted by this release.
PUBLISH_HOLD_BOOTSTRAP_KEY = "verification_publish_hold_bootstrap"


def bootstrap_publish_hold(
    conn: sqlite3.Connection, *, pre_migration_user_version: int
) -> None:
    """Arm the operator kill switch on upgrade; never on a fresh install (D1-D3).

    ``pre_migration_user_version`` is required and keyword-only so a future
    caller cannot silently pass the post-migration value and turn every
    boot into "fresh". Runs as the last step of `run_migrations`, before the
    `PRAGMA user_version` write, so a failure here aborts the migration
    without bumping user_version and the next boot retries the decision.

    The SAVEPOINT buys bootstrap-write atomicity -- the hold setting row,
    its two last-transition metadata rows and the one-time marker land
    together or not at all -- NOT atomicity of the whole migration:
    `create_tables` and `create_indexes` have already COMMITTED by the time
    we get here, because executescript implicitly commits the pending
    transaction before running its script, so the outer BEGIN IMMEDIATE from
    Database._run_immediate is long gone and each statement below would
    otherwise land individually.
    That commit boundary is pre-existing -- these bootstrap writes expose
    it, they do not introduce it. Unprotected, a failure after the hold
    write but before the marker write would leave the database HELD with no
    marker, and since marker PRESENCE (not value) is the anti-rearm gate, a
    later operator release would then be silently re-armed on the next boot
    -- the exact hazard this bootstrap exists to prevent. The savepoint
    opens before the marker read so the already-bootstrapped path closes it
    too, and execute() (unlike executescript) never forces an implicit
    commit.
    """
    from wxverify.verification.publish_hold import set_publish_hold

    conn.execute("SAVEPOINT bootstrap_publish_hold")
    try:
        # Phrased as "not yet bootstrapped" rather than an early return so
        # there is exactly one exit and one RELEASE: an early return inside
        # the savepoint would have to release on its own path too.
        if get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) is None:
            site_count_row = conn.execute(
                "SELECT COUNT(*) AS n FROM sites WHERE enabled = 1"
            ).fetchone()
            enabled_site_count = int(site_count_row["n"])
            existing_installation = (
                pre_migration_user_version > 0 or enabled_site_count > 0
            )
            if existing_installation:
                set_publish_hold(conn, held=True, source="bootstrap")
                set_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY, "armed")
            else:
                set_runtime_state(
                    conn, PUBLISH_HOLD_BOOTSTRAP_KEY, "skipped_fresh_install"
                )
    except BaseException:
        conn.execute("ROLLBACK TO bootstrap_publish_hold")
        conn.execute("RELEASE bootstrap_publish_hold")
        raise
    conn.execute("RELEASE bootstrap_publish_hold")


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
#: not value -- is the gate, matching PUBLISH_HOLD_BOOTSTRAP_KEY.
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
    assert an incompatibility that does not exist. The v7 obs-cycle
    migration bumps the version for its own reasons; this correction
    rides alongside it and leaves the version alone.

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
#: the gate, matching GOOGLE_HORIZON_CORRECTION_KEY. The key holds no colon, so
#: the v9 step's first-segment key match can never delete it.
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


def run_migrations(conn: sqlite3.Connection) -> None:
    row = conn.execute("PRAGMA user_version").fetchone()
    current = int(row[0]) if row is not None else 0
    if current > TARGET_USER_VERSION:
        raise RuntimeError(f"database user_version {current} is newer than this app")
    logger.debug(
        "migrations begin user_version=%s target=%s", current, TARGET_USER_VERSION
    )
    # Tables first, indexes last: a version migration may add a column an
    # index names (migrate_v4 -> idx_pairs_winrate), and the migrations seed
    # tables a pre-v4 file lacks. Both halves are IF NOT EXISTS throughout,
    # so re-entry after a partial run converges.
    create_tables(conn)
    logger.debug("migrations tables ensured")
    if current < 2:
        logger.debug("migrations applying v2 backfill_status")
        migrate_v2_backfill_status(conn)
    if current < 3:
        logger.debug("migrations applying v3 station poll-state")
        migrate_v3(conn)
    if current < 4:
        logger.debug("migrations applying v4 timezone generations")
        migrate_v4(conn)
    if current < 5:
        logger.debug("migrations applying v5 result-basis fingerprint")
        migrate_v5_result_basis_fingerprint(conn)
    if current < 6:
        logger.debug("migrations applying v6 daily-truth admission basis")
        migrate_v6_daily_truth_admission_basis(conn)
    if current < 7:
        logger.debug("migrations applying v7 obs cycle clock + station history")
        migrate_v7_obs_cycle_and_station_history(conn)
    if current < 8:
        logger.debug("migrations applying v8 last usable fetch stamp")
        migrate_v8_last_usable_fetch_at(conn)
    if current < 9:
        logger.debug("migrations applying v9 old wind state removal")
        migrate_v9_drop_old_wind_state(conn)
    if current < 11:
        logger.debug("migrations applying v11 daily-close days")
        migrate_v11_daily_close_days(conn)
    if current < 12:
        logger.debug("migrations applying v12 widened raw readings")
        migrate_v12_widen_raw_readings(conn)
    if current < 13:
        logger.debug("migrations applying v13 daily-close job kinds")
        migrate_v13_close_jobs(conn)
    if current < 14:
        logger.debug("migrations applying v14 daily-close scores")
        migrate_v14_daily_close_scores(conn)
    if current < 15:
        logger.debug("migrations applying v15 switch to the daily close")
        migrate_v15_switch(conn)
    if current < 16:
        logger.debug("migrations applying v16 rain release")
        migrate_v16_rain_release(conn)
    create_indexes(conn)
    logger.debug("migrations indexes ensured")
    correct_google_horizon(conn)
    correct_open_meteo_horizons(conn)
    correct_open_meteo_fetch_intervals(conn)
    pause_dropped_meteoblue_members(conn)
    seed_default_sources(conn)
    seed_default_feeds(conn)
    seed_default_settings(conn)
    logger.debug("migrations seeded sources+feeds+settings")
    record_precip_hour_start_boundary(conn)
    bootstrap_publish_hold(conn, pre_migration_user_version=current)
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
    ``station_current_obs``) are created by :func:`create_tables`; this function
    rebuilds ``jobs`` to admit ``fetch_current_obs`` and backfills a staggered
    poll-state row per existing station.
    """
    # The whole v3 step (jobs rebuild + poll-state seed) must be all-or-nothing.
    # The outer BEGIN IMMEDIATE in Database._run_immediate does NOT protect us:
    # create_tables' executescript issues an implicit COMMIT before migrate_v3
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
        # jobs_new CHECK and column list must stay identical to create_tables'
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
                                 'verification_run','timezone_correction',
                                 'day_close','day_publish')
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

    ``timezone_generations`` itself comes from :func:`create_tables`, as in
    v3. On a fresh database (user_version 0) both rebuilds copy zero rows
    from the already-new-shape tables created by :func:`create_tables`, so
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
                                 'verification_run','timezone_correction',
                                 'day_close','day_publish')
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
        # Seed the initial published generation per existing site (the row
        # `wxverify.db.switch.ensure_site_generation` makes), then the
        # published-pointer runtime_state row, which A1 of `migrate_v15_switch` reads
        # and A4 deletes. Kept in SQL here so the whole step stays inside this
        # savepoint.
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


def migrate_v5_result_basis_fingerprint(conn: sqlite3.Connection) -> None:
    """Add ``verification_runs.result_basis_fingerprint`` (nullable).

    One additive ``ALTER TABLE``; nothing is backfilled. A run published
    before this column existed reads NULL, which the freshness derivation
    reports as *unknown* rather than fabricating a baseline.

    Column-probed idempotence, following
    :func:`migrate_v2_backfill_status`: ``run_migrations`` writes
    ``PRAGMA user_version`` only after this returns, so a crash in between
    leaves the column present at user_version 4 and the next boot re-enters
    the ``current < 5`` gate. :func:`create_tables` already creates the
    column on a fresh database, where this is likewise a no-op -- which is
    why the DDL appends it after ``published_at``, so a fresh and a migrated
    database agree on column order, not just membership.
    """
    if "result_basis_fingerprint" not in _table_columns(conn, "verification_runs"):
        conn.execute(
            "ALTER TABLE verification_runs ADD COLUMN result_basis_fingerprint TEXT"
        )


def migrate_v6_daily_truth_admission_basis(conn: sqlite3.Connection) -> None:
    """Add ``daily_truth.admission_basis`` (nullable, CHECK-constrained).

    One additive ``ALTER TABLE``; nothing is backfilled. A row created
    before this column existed reads NULL, and NULL is the exact,
    by-construction marker of a pre-gate row -- a row whose creation was
    never subject to an admission decision. Backfilling any value would
    fabricate a decision that was never made, so nothing is backfilled.

    Column-probed idempotence, following
    :func:`migrate_v5_result_basis_fingerprint`: ``run_migrations`` writes
    ``PRAGMA user_version`` only after this returns, so a crash in between
    leaves the column present at user_version 5 and the next boot re-enters
    the ``current < 6`` gate. :func:`create_tables` already creates the
    column on a fresh database, where this is likewise a no-op -- which is
    why the DDL appends it after ``tz_generation_id``, so a fresh and a
    migrated database agree on column order, not just membership.

    The ``CHECK`` predicate is repeated here, matching the one in the
    ``create_tables`` DDL: ``ALTER TABLE ... ADD COLUMN`` accepts a bare
    ``TEXT`` happily, so dropping it would leave migrated installs
    accepting values a fresh install rejects.
    """
    if "admission_basis" not in _table_columns(conn, "daily_truth"):
        conn.execute(
            "ALTER TABLE daily_truth ADD COLUMN admission_basis TEXT "
            "CHECK(admission_basis IS NULL "
            "OR admission_basis IN ('complete','deadline'))"
        )


def migrate_v7_obs_cycle_and_station_history(conn: sqlite3.Connection) -> None:
    """Split the obs cycle clock from obs freshness; add the station history trio.

    ``sites.last_obs_cycle_at`` is the cycle-attempt clock the scheduler
    reads, leaving ``sites.last_obs_at`` to mean data freshness alone.
    ``stations.history_next_attempt_at`` / ``history_last_error`` /
    ``history_error_count`` carry the per-station history-recovery state so
    one station's empty hourly history can no longer abort its site's whole
    observation cycle.

    Column-probed idempotence, following
    :func:`migrate_v6_daily_truth_admission_basis`: ``run_migrations`` writes
    ``PRAGMA user_version`` only after this returns, so a crash in between
    leaves the columns present at user_version 6 and the next boot re-enters
    the ``current < 7`` gate. :func:`create_tables` already creates them on a
    fresh database, where the probes are a no-op -- which is why the DDL
    appends each one last in its table, so a fresh and a migrated database
    agree on column order, not just membership.

    ``history_error_count`` repeats its full ``NOT NULL DEFAULT 0``
    definition, matching the ``create_tables`` DDL: ``ALTER TABLE ... ADD
    COLUMN history_error_count INTEGER`` is accepted happily and would leave
    every pre-existing station row holding NULL, which reads as neither
    ``> 0`` nor ``<= 0`` in the monitor's condition. The other three are
    nullable ``TEXT`` on both paths, so a bare type is their full definition.

    The backfill runs outside the probe guards and is self-idempotent via its
    own ``WHERE``, so a crash between the ``ALTER`` and the ``UPDATE``
    converges on the next boot. It exists so an upgraded instance does not
    become due for every site at once, and so no reader needs a permanent
    ``COALESCE`` fallback.
    """
    if "last_obs_cycle_at" not in _table_columns(conn, "sites"):
        conn.execute("ALTER TABLE sites ADD COLUMN last_obs_cycle_at TEXT")
    station_columns = _table_columns(conn, "stations")
    if "history_next_attempt_at" not in station_columns:
        conn.execute("ALTER TABLE stations ADD COLUMN history_next_attempt_at TEXT")
    if "history_last_error" not in station_columns:
        conn.execute("ALTER TABLE stations ADD COLUMN history_last_error TEXT")
    if "history_error_count" not in station_columns:
        conn.execute(
            "ALTER TABLE stations ADD COLUMN "
            "history_error_count INTEGER NOT NULL DEFAULT 0"
        )
    conn.execute(
        "UPDATE sites SET last_obs_cycle_at = last_obs_at "
        "WHERE last_obs_cycle_at IS NULL"
    )


# Seeds ``site_feed_state.last_usable_fetch_at`` from ``last_run_at`` when a
# stored sample proves that fetch returned something usable: a valid sample
# whose ``fetched_at`` is exactly that stamp. Both columns are written from
# the same ``fetched_at`` value by one ``persist_fetch_result`` call, so exact
# string equality is sound. A meteoblue package row also accepts samples of
# its member feeds, which is where that fetch stores them. The member branch
# is an ``IN (... UNION ALL ...)``, not an ``OR``: the ``OR`` form degrades
# the probe to a ``site_id``-only seek of idx_samples_recent, while this form
# seeks ``(site_id, feed_id, issued_at)``. The ``issued_at`` floor is that
# seek's range. Idempotent through ``IS NULL``; every case it cannot prove
# (a no-op fetch, a duplicates-only fetch, runs older than 2 days) is left
# NULL, which the Forecast page reads as "fetch time unknown".
SEED_LAST_USABLE_FETCH_SQL: Final[str] = f"""
    UPDATE site_feed_state AS sfs
    SET last_usable_fetch_at = sfs.last_run_at
    WHERE sfs.last_usable_fetch_at IS NULL
      AND sfs.last_run_at IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM forecast_samples fs
          WHERE fs.site_id = sfs.site_id
            AND fs.feed_id IN (
                SELECT sfs.feed_id
                UNION ALL
                SELECT m.id FROM feeds m, feeds p
                WHERE p.id = sfs.feed_id
                  AND p.source = 'meteoblue' AND p.model = 'multimodel'
                  AND m.source = 'meteoblue' AND m.model != 'multimodel')
            AND fs.issued_at
                >= strftime('%Y-%m-%dT%H:%M:%SZ', sfs.last_run_at, '-2 days')
            AND fs.fetched_at = sfs.last_run_at
            AND NOT {invalid_forecast_sample_sql("fs")}
      )
    """


def migrate_v8_last_usable_fetch_at(conn: sqlite3.Connection) -> None:
    """Add ``site_feed_state.last_usable_fetch_at`` and seed it.

    The column records when the app last completed a forward fetch for the
    site and feed that returned at least one usable sample; the Forecast
    page's ``stale`` badge reads it. ``persist_fetch_result`` writes it from
    now on; :data:`SEED_LAST_USABLE_FETCH_SQL` recovers what the stored
    samples can prove for fetches made before the column existed.

    Column-probed idempotence, following
    :func:`migrate_v7_obs_cycle_and_station_history`: ``run_migrations``
    writes ``PRAGMA user_version`` only after this returns, so a crash in
    between leaves the column present at user_version 7 and the next boot
    re-enters the ``current < 8`` gate. :func:`create_tables` already creates
    the column on a fresh database, where the probe is a no-op -- which is
    why the DDL appends it last in its table, so a fresh and a migrated
    database agree on column order, not just membership. The seed runs
    outside the probe guard and is self-idempotent through its ``IS NULL``,
    so a crash between the ``ALTER`` and the ``UPDATE`` converges on the next
    boot.
    """
    if "last_usable_fetch_at" not in _table_columns(conn, "site_feed_state"):
        conn.execute("ALTER TABLE site_feed_state ADD COLUMN last_usable_fetch_at TEXT")
    conn.execute(SEED_LAST_USABLE_FETCH_SQL)


def migrate_v9_drop_old_wind_state(conn: sqlite3.Connection) -> None:
    """Delete the six per-site wind key kinds and drop ``legacy_hours``.

    Both are left over from the 0.16.6 wind switch and nothing reads them any
    more: the ``runtime_state`` keys ``wind_basis:``,
    ``wind_basis_progress_at:``, ``wind_basis_cursor:``,
    ``wind_basis_blocked:``, ``wind_rebuild_report:`` and
    ``wind_rebuild_done_at:`` of every site, and the column
    ``station_wind_days.legacy_hours``. Forward-only: nothing puts them back,
    and once ``run_migrations`` has written ``user_version`` 9 an older
    release refuses the database.

    The six prefixes are written out here, so the step keeps meaning the same
    keys whatever later code does. A key matches only when its whole first
    segment, up to and including its first colon, is one of the six; ``IN``
    compares case-sensitively. ``LIKE`` is not used: its ``_`` matches any one
    character and it ignores ASCII case. The weather.com auth holds
    (``wind_history_auth_hold:<endpoint>``) and the call counters stay.

    Re-entry, following :func:`migrate_v8_last_usable_fetch_at`:
    ``run_migrations`` writes ``PRAGMA user_version`` only after this
    returns, so a stop after the ``DELETE`` or after the ``ALTER`` leaves
    user_version 8 and the next boot re-enters the ``current < 9`` gate. Each
    part is safe to run again: the ``DELETE`` then matches nothing and the
    column probe skips a column already gone. :func:`create_tables` no longer
    creates the column, so on a fresh database the step deletes nothing,
    drops nothing and logs nothing. ``DROP COLUMN`` keeps the order of the
    columns it leaves, so a fresh and a migrated database agree on column
    order, not just membership. One INFO line is logged only when the step
    removed something.
    """
    started = time.perf_counter()
    deleted = conn.execute(
        "DELETE FROM runtime_state"
        " WHERE substr(key, 1, instr(key, ':')) IN ("
        "'wind_basis:', 'wind_basis_progress_at:', 'wind_basis_cursor:',"
        " 'wind_basis_blocked:', 'wind_rebuild_report:', 'wind_rebuild_done_at:')"
    ).rowcount
    dropped = "legacy_hours" in _table_columns(conn, "station_wind_days")
    if dropped:
        conn.execute("ALTER TABLE station_wind_days DROP COLUMN legacy_hours")
    if deleted or dropped:
        logger.info(
            "old wind state removed: %d keys deleted, legacy_hours column %s, %d ms",
            deleted,
            "dropped" if dropped else "already gone",
            int((time.perf_counter() - started) * 1000),
        )


# Starts ``station_changes`` in step with today's ``stations.enabled`` flags:
# one row per station that has no change yet, ``added`` when enabled and
# ``turned_off`` otherwise. ``:effective_date`` is ``date.min``, earlier than
# any record's date, so the seed row decides a station's list membership
# until a route records a change. ``:changed_at`` is one fixed-width stamp
# for the whole seed. Idempotent through ``NOT EXISTS``; on a fresh database
# there are no stations and it writes nothing.
SEED_STATION_CHANGES_SQL: Final[str] = """
    INSERT INTO station_changes
        (site_id, station_id, change, effective_date, reason, changed_at)
    SELECT s.site_id, s.id,
           CASE WHEN s.enabled = 1 THEN 'added' ELSE 'turned_off' END,
           :effective_date, 'switch_seed', :changed_at
    FROM stations AS s
    WHERE NOT EXISTS (
        SELECT 1 FROM station_changes AS c WHERE c.station_id = s.id
    )
    ORDER BY s.id
"""


def migrate_v11_daily_close_days(conn: sqlite3.Connection) -> None:
    """Add ``stations.retired_at`` and seed ``station_changes``.

    :func:`create_tables` makes the four daily-close tables (``site_days``,
    ``station_changes``, ``site_day_stations``, ``site_zone_changes``) before
    the gates and :func:`create_indexes` their indexes after them, so this
    step only adds the column and runs :data:`SEED_STATION_CHANGES_SQL`. It
    creates no ``site_days`` row and seeds no zone change, and it changes no
    existing row beyond the new NULL column.

    Re-entry, following :func:`migrate_v8_last_usable_fetch_at`:
    ``run_migrations`` writes ``PRAGMA user_version`` only after this
    returns, so a crash in between leaves user_version below 11 and the next
    boot re-enters the ``current < 11`` gate. The column probe skips a column
    already there -- on a fresh database :func:`create_tables` made it, last
    in its table, so a fresh and a migrated database agree on column order --
    and the seed's ``NOT EXISTS`` skips every station already seeded.
    ``changed_at`` reads the clock through this module's ``utc_now``.
    """
    if "retired_at" not in _table_columns(conn, "stations"):
        conn.execute("ALTER TABLE stations ADD COLUMN retired_at TEXT")
    conn.execute(
        SEED_STATION_CHANGES_SQL,
        {
            "effective_date": date.min.isoformat(),
            "changed_at": isoformat_utc_micro(utc_now()),
        },
    )


def migrate_v12_widen_raw_readings(conn: sqlite3.Connection) -> None:
    """Widen ``station_wind_records`` and add ``station_wind_days.refused_at``.

    ``station_wind_records`` becomes the one table of raw station readings:
    wind, temperature and the rain running total, from the 5-minute
    download, the current-conditions poll and the hourly download. SQLite
    cannot drop ``speed_kmh``'s ``NOT NULL`` with ``ALTER TABLE``, and
    ``source`` joins the ``WITHOUT ROWID`` primary key, so the table is
    rebuilt in :func:`create_tables`' shape. Every existing row is copied
    with ``source`` ``'five_minute'`` and its speed, the new columns NULL;
    nothing is backfilled. ``refused_at`` (NULL) is the latest time a
    5-minute try for the station-day was held or refused.

    Steps, all inside one savepoint, following :func:`migrate_v4`:

    1. the re-run guard: the rebuild is skipped when ``station_wind_records``
       already has ``source`` -- a fresh file :func:`create_tables` made, or
       a rebuild already committed. ``run_migrations`` writes ``PRAGMA
       user_version`` only after this returns, so a crash in between leaves
       user_version below 12 and the next boot re-enters the ``current < 12``
       gate;
    2. the rebuild (:func:`_rebuild_station_wind_records`);
    3. ``ALTER TABLE station_wind_days ADD COLUMN refused_at TEXT`` when the
       column is absent. On a fresh file :func:`create_tables` made it, last
       among the columns, so a fresh and a migrated database agree on column
       order.

    As in :func:`record_precip_hour_start_boundary`, the savepoint is the
    whole transaction on the writer, and the in_transaction check covers an
    error that has already ended it. Any failure, a failed foreign key check
    included, rolls every step back and is re-raised, so the open fails
    before the ``user_version`` write and the next open retries. One INFO
    line is logged only when the table was rebuilt.
    """
    started = time.perf_counter()
    copied: int | None = None
    conn.execute("SAVEPOINT migrate_v12")
    try:
        if "source" not in _table_columns(conn, "station_wind_records"):
            copied = _rebuild_station_wind_records(conn)
        if "refused_at" not in _table_columns(conn, "station_wind_days"):
            conn.execute("ALTER TABLE station_wind_days ADD COLUMN refused_at TEXT")
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK TO migrate_v12")
            conn.execute("RELEASE migrate_v12")
        raise
    conn.execute("RELEASE migrate_v12")
    if copied is not None:
        logger.info(
            "station_wind_records rebuilt for version 12: %d rows copied, %d ms",
            copied,
            int((time.perf_counter() - started) * 1000),
        )


def _rebuild_station_wind_records(conn: sqlite3.Connection) -> int:
    """Rebuild ``station_wind_records`` in the widened shape.

    Step 2 of :func:`migrate_v12_widen_raw_readings`, run inside its
    savepoint. The new table's statement must stay identical to the one in
    :func:`create_tables` but for the name and ``IF NOT EXISTS``, indentation
    included, so a rebuilt and a fresh table agree column for column. No
    table references ``station_wind_records``, so the drop and the rename
    touch no other table's keys. Raises ``RuntimeError`` when ``PRAGMA
    foreign_key_check`` then reports any row. Returns the rows copied.
    """
    conn.execute(
        """
        CREATE TABLE station_wind_records_v12 (
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
        ) WITHOUT ROWID
        """
    )
    copied = conn.execute(
        "INSERT INTO station_wind_records_v12 (station_id, obs_at, source, speed_kmh)"
        " SELECT station_id, obs_at, 'five_minute', speed_kmh"
        " FROM station_wind_records"
    ).rowcount
    conn.execute("DROP TABLE station_wind_records")
    conn.execute("ALTER TABLE station_wind_records_v12 RENAME TO station_wind_records")
    violation = conn.execute(
        "PRAGMA foreign_key_check(station_wind_records)"
    ).fetchone()
    if violation is not None:
        raise RuntimeError("station_wind_records rebuild failed its foreign key check")
    return copied


def migrate_v13_close_jobs(conn: sqlite3.Connection) -> None:
    """Widen the ``jobs`` type CHECK for ``day_close`` and ``day_publish``.

    The daily close's two job kinds must pass the CHECK, and
    :func:`create_tables` leaves an existing ``jobs`` table as it is, so a
    file at version 12 keeps the narrow CHECK until this rebuild widens it.
    The new table's column list and CHECK must stay identical to
    :func:`create_tables`' and to the ``jobs_new`` of :func:`migrate_v3` and
    :func:`migrate_v4`. The rebuild copies :func:`migrate_v4`'s: every row is
    copied as it is, retired kinds included, and the two ``jobs`` indexes
    are made again here; every other index comes back from
    :func:`create_indexes`. The five daily-close tables come from
    :func:`create_tables`, so this step does not name them.

    The re-run guard returns at once when the ``jobs`` statement already
    holds ``'day_publish'``: a fresh file, whose ``jobs`` :func:`migrate_v3`
    rebuilt from its own widened text, or a rebuild already committed.
    ``run_migrations`` writes ``PRAGMA user_version`` only after this
    returns, so a stop in between leaves user_version below 13 and the next
    open re-enters the ``current < 13`` gate and returns here.

    As in :func:`migrate_v12_widen_raw_readings`, one savepoint holds the
    rebuild, and the in_transaction check covers an error that has already
    ended it. Any failure rolls the rebuild back, leaving the old ``jobs``
    and no ``jobs_new``, and is re-raised, so the open fails before the
    ``user_version`` write and the next open runs the step again.
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'"
    ).fetchone()
    if row is not None and "'day_publish'" in str(row[0]):
        return
    conn.execute("SAVEPOINT migrate_v13")
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
                                 'verification_run','timezone_correction',
                                 'day_close','day_publish')
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
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK TO migrate_v13")
            conn.execute("RELEASE migrate_v13")
        raise
    conn.execute("RELEASE migrate_v13")


# Value: when the download record (``forecast_downloads``) starts, a
# canonical whole-second UTC ``...Z`` stamp. Downloads before it are
# inferred from ``forecast_samples`` and the job rows instead.
DOWNLOAD_RECORD_SINCE_KEY: Final = "download_record_since"


def migrate_v14_daily_close_scores(conn: sqlite3.Connection) -> None:
    """Add the four P4 columns and start the download record.

    :func:`create_tables` makes P4's twelve tables before the gates and
    :func:`create_indexes` the ``forecast_downloads`` index after them, so
    this step only adds ``verification_runs.worksheet_kept`` (1 for every
    existing run) and ``forecast_of_record``'s ``publication_id``,
    ``publication_cutoff`` and ``publication_age_days`` (NULL for every
    existing record), then writes :data:`DOWNLOAD_RECORD_SINCE_KEY` with
    the migration's time when the key is absent. It changes no existing row
    beyond the new columns.

    Re-entry, following :func:`migrate_v8_last_usable_fetch_at`:
    ``run_migrations`` writes ``PRAGMA user_version`` only after this
    returns, so a stop in between leaves user_version below 14 and the next
    open re-enters the ``current < 14`` gate. Each column probe skips a
    column already there -- on a fresh database :func:`create_tables` made
    it, last in its table, so a fresh and a migrated database agree on
    column order -- and the key is written only when absent, so a re-entry
    keeps the first time. The clock is read through this module's
    ``utc_now``, truncated to whole seconds.
    """
    if "worksheet_kept" not in _table_columns(conn, "verification_runs"):
        conn.execute(
            "ALTER TABLE verification_runs"
            " ADD COLUMN worksheet_kept INTEGER NOT NULL DEFAULT 1"
        )
    record_columns = _table_columns(conn, "forecast_of_record")
    if "publication_id" not in record_columns:
        conn.execute("ALTER TABLE forecast_of_record ADD COLUMN publication_id INTEGER")
    if "publication_cutoff" not in record_columns:
        conn.execute(
            "ALTER TABLE forecast_of_record ADD COLUMN publication_cutoff TEXT"
        )
    if "publication_age_days" not in record_columns:
        conn.execute(
            "ALTER TABLE forecast_of_record ADD COLUMN publication_age_days INTEGER"
        )
    if get_runtime_state(conn, DOWNLOAD_RECORD_SINCE_KEY) is None:
        set_runtime_state(
            conn,
            DOWNLOAD_RECORD_SINCE_KEY,
            isoformat_utc(utc_now().replace(microsecond=0)),
        )


def migrate_v16_rain_release(conn: sqlite3.Connection) -> None:
    """The rain release's one migration (``plans/2026-11-01-rain-release.md``).

    :func:`create_tables` makes ``station_rain_checks``, ``forecast_scored``
    and Part C's four rain tables before the gates. Part A adds
    ``site_day_stations.rain_counted`` when the table lacks it, 1 for every
    existing list row: nothing else is carried over, and the first close
    after the upgrade judges its day. Part B's carry-over then runs in the
    plan's order: step 1 writes the scored
    values of the open days and the coming days into ``forecast_scored``
    (``carry_over_scored_values``); Part C's C6 fills ``rain_days`` and
    ``rain_forecast_days`` for the 60 days before the oldest open day
    (``fill_rain_history``); step 2 keeps one ``forecast_samples``
    row per hour, the row the Forecast page showed
    (``keep_newest_samples``), and step 3 makes ``idx_samples_slot``, which
    the extra rows of step 2 would have refused. The clock is read through
    this module's ``utc_now``.

    The carry-over lives in ``collection/forecast_store.py`` and is
    imported here: ``scoring/downloads.py``, which it calls, imports this
    module.

    Two index steps make the upgrade faster and change no row. Step 1 and
    the fill run with ``V16_FETCHED_INDEX``, dropped right after the fill.
    Before step 2 the four ``V16_DROPPED_SAMPLE_INDEXES`` are dropped (IF
    EXISTS), so the delete updates only the table and its ``UNIQUE`` index;
    :func:`create_indexes` makes all four again once the version
    migrations are done, before ``user_version`` is written. Its
    ``executescript`` commits first, so a stop before it ends leaves a
    version-15 file without them; the next open runs this migration again
    and then makes them.

    One SAVEPOINT holds every step, following :func:`migrate_v4`: any error
    rolls them all back and is re-raised, so the open fails before the
    ``user_version`` write and the next open runs the migration again. As
    in :func:`migrate_v12_widen_raw_readings`, the in_transaction check
    covers an error that has already ended the transaction. Each step finds
    its work done on a repeat (the column is added only when missing, the
    scored and rain history rows are written only where none is stored, and
    the index is IF NOT EXISTS), so a stop between this step and the version write
    converges on the next open.
    """
    from wxverify.collection.forecast_store import (
        carry_over_scored_values,
        fill_rain_history,
        keep_newest_samples,
    )

    conn.execute("SAVEPOINT migrate_v16")
    try:
        # Part A: the list rows' rain flag; every existing row is counted.
        if "rain_counted" not in _table_columns(conn, "site_day_stations"):
            conn.execute(
                "ALTER TABLE site_day_stations ADD COLUMN rain_counted"
                " INTEGER NOT NULL DEFAULT 1 CHECK(rain_counted IN (0,1))"
            )
        # The carry-over's and the fill's own index, gone again before the
        # delete: see V16_FETCHED_INDEX.
        started = time.perf_counter()
        conn.execute(f"DROP INDEX IF EXISTS {V16_FETCHED_INDEX}")
        conn.execute(V16_FETCHED_INDEX_DDL)
        logger.info(
            "index %s built in %d ms",
            V16_FETCHED_INDEX,
            int((time.perf_counter() - started) * 1000),
        )
        # Part B, step 1: the scored values of the open days.
        carry_over_scored_values(conn, now_utc=utc_now())
        # Part C, C6: the rain history of the 60 days before the oldest open
        # day, before Part B's step 2, which deletes the older labels the
        # fill's scored_version reads.
        fill_rain_history(conn)
        conn.execute(f"DROP INDEX {V16_FETCHED_INDEX}")
        # The delete need not keep these up to date; create_indexes makes
        # them again after the version migrations.
        for name in V16_DROPPED_SAMPLE_INDEXES:
            conn.execute(f"DROP INDEX IF EXISTS {name}")
        # Part B, step 2: one forecast_samples row per hour.
        keep_newest_samples(conn)
        # Part B, step 3: the index, after step 2's delete.
        conn.execute(SAMPLES_SLOT_INDEX_DDL)
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK TO migrate_v16")
            conn.execute("RELEASE migrate_v16")
        raise
    conn.execute("RELEASE migrate_v16")


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")}
