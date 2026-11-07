-- Schema of a fresh wxverify 0.16.17 database (user_version 17), dumped from sqlite_master.

CREATE TABLE api_budget (
            source TEXT NOT NULL REFERENCES sources(source) ON DELETE RESTRICT,
            billing_day TEXT NOT NULL,
            calls INTEGER NOT NULL DEFAULT 0,
            credits INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(source, billing_day)
        );

CREATE TABLE close_attempts (
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

CREATE TABLE close_publications (
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

CREATE TABLE close_record_downloads (
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

CREATE TABLE close_records (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            attempt INTEGER NOT NULL,
            truth_version TEXT NOT NULL,
            scoring_rule TEXT NOT NULL,
            scored_at TEXT NOT NULL,
            PRIMARY KEY (site_id, local_date)
        );

CREATE TABLE close_stage_log (
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

CREATE TABLE close_station_days (
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

CREATE TABLE close_try_log (
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

CREATE TABLE daily_truth (
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

CREATE TABLE day_scores (
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

CREATE TABLE domain_backoffs (
            domain TEXT PRIMARY KEY NOT NULL,
            next_attempt_at TEXT NOT NULL,
            retry_count INTEGER NOT NULL DEFAULT 0
        );

CREATE TABLE feeds (
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

CREATE TABLE forecast_downloads (
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

CREATE TABLE forecast_history (
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

CREATE TABLE forecast_samples (
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

CREATE TABLE forecast_scored (
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

CREATE TABLE forecast_value_changes (
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
                    type IN ('fetch_feed','fetch_obs','fetch_current_obs',
                             'pair_and_score','backfill_site',
                             'forecast_record','record_gap_scan',
                             'verification_run','timezone_correction',
                             'day_close','day_publish')
                    AND site_id IS NOT NULL
                )
            )
        );

CREATE TABLE observations (
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

CREATE TABLE publication_coverage (
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

CREATE TABLE publication_rain_ranks (
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

CREATE TABLE publication_rankings (
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

CREATE TABLE publication_wind_weights (
            publication_id INTEGER NOT NULL
                REFERENCES publications(id) ON DELETE CASCADE,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            mae_ms REAL NOT NULL,
            days INTEGER NOT NULL,
            weight REAL NOT NULL,
            PRIMARY KEY (publication_id, day_ahead, feed_id)
        );

CREATE TABLE publications (
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

CREATE TABLE rain_days (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            outcome TEXT NOT NULL CHECK(outcome IN ('rain','dry','unknown')),
            stations_full INTEGER NOT NULL,
            stations_partial INTEGER NOT NULL,
            PRIMARY KEY (site_id, local_date)
        );

CREATE TABLE rain_forecast_days (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            total_mm REAL NOT NULL,
            PRIMARY KEY (site_id, local_date, feed_id, day_ahead)
        ) WITHOUT ROWID;

CREATE TABLE rain_picks (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            cutoff TEXT NOT NULL,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            feed_id INTEGER REFERENCES feeds(id) ON DELETE SET NULL,
            PRIMARY KEY (site_id, cutoff, day_ahead)
        );

CREATE TABLE runtime_state (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );

CREATE TABLE score_cache (
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

CREATE TABLE settings (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        );

CREATE TABLE site_day_stations (
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

CREATE TABLE site_days (
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

CREATE TABLE site_feed_state (
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

CREATE TABLE site_zone_changes (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            timezone TEXT NOT NULL,
            effective_date TEXT NOT NULL,
            entered_at TEXT NOT NULL
        );

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
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            last_obs_cycle_at TEXT
        );

CREATE TABLE sources (
            source TEXT PRIMARY KEY NOT NULL,
            daily_call_limit INTEGER NOT NULL,
            daily_credit_limit INTEGER,
            billing_tz TEXT NOT NULL
        );

CREATE TABLE starting_scores (
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

CREATE TABLE starting_wind_days (
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            local_date TEXT NOT NULL,
            feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE RESTRICT,
            day_ahead INTEGER NOT NULL CHECK(day_ahead BETWEEN 0 AND 7),
            forecast_high REAL,
            observed_high REAL,
            PRIMARY KEY (site_id, local_date, feed_id, day_ahead)
        );

CREATE TABLE station_changes (
            id INTEGER PRIMARY KEY,
            site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
            station_id INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
            change TEXT NOT NULL
                CHECK(change IN ('added','turned_on','turned_off','retired')),
            effective_date TEXT NOT NULL,
            reason TEXT NOT NULL,
            changed_at TEXT NOT NULL
        );

CREATE TABLE station_current_obs (
            station_id INTEGER PRIMARY KEY REFERENCES stations(id) ON DELETE CASCADE,
            obs_time_utc TEXT,
            temp REAL, humidity REAL, dewpt REAL,
            wind_speed REAL, wind_gust REAL, wind_dir REAL,
            pressure REAL, precip_rate REAL, precip_total REAL, uv REAL,
            neighborhood TEXT,
            fetched_at TEXT NOT NULL
        );

CREATE TABLE station_observations (
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

CREATE TABLE station_poll_state (
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

CREATE TABLE station_rain_checks (
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

CREATE TABLE station_wind_days (
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

CREATE TABLE station_wind_records (
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
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            history_next_attempt_at TEXT,
            history_last_error TEXT,
            history_error_count INTEGER NOT NULL DEFAULT 0,
            retired_at TEXT
        );

CREATE TABLE timezone_generations (
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

CREATE INDEX idx_close_stage_log_day
            ON close_stage_log(site_id, local_date, stage);

CREATE INDEX idx_close_stage_log_ended
            ON close_stage_log(ended_at);

CREATE INDEX idx_close_try_log_day
            ON close_try_log(site_id, local_date, station_id);

CREATE INDEX idx_close_try_log_tried
            ON close_try_log(tried_at);

CREATE INDEX idx_daily_truth_stale
            ON daily_truth(site_id, local_date, tz_generation_id)
            WHERE stale = 1;

CREATE INDEX idx_forecast_downloads_day
            ON forecast_downloads(site_id, feed_id, local_date, ended_at, id);

CREATE UNIQUE INDEX idx_jobs_active_dedupe
            ON jobs(type, COALESCE(site_id, -1), job_key)
            WHERE status IN ('pending','running') AND job_key IS NOT NULL;

CREATE INDEX idx_jobs_type_key_site
            ON jobs(type, job_key, site_id, id);

CREATE INDEX idx_samples_invalid ON forecast_samples(site_id, feed_id) WHERE (
        (forecast_samples.variable='temperature' AND (forecast_samples.value < -90 OR forecast_samples.value > 70)) OR (forecast_samples.variable='wind' AND (forecast_samples.value < 0 OR forecast_samples.value > 150)) OR (forecast_samples.variable='precip' AND (forecast_samples.value < 0 OR forecast_samples.value > 500)) OR (forecast_samples.variable='cloud_cover' AND (forecast_samples.value < 0 OR forecast_samples.value > 100)) OR (forecast_samples.variable='wind_gust' AND (forecast_samples.value < 0 OR forecast_samples.value > 150)) OR (forecast_samples.variable='wind_direction' AND (forecast_samples.value < 0 OR forecast_samples.value > 360))
        OR forecast_samples.variable NOT IN ('temperature', 'wind', 'precip', 'cloud_cover', 'wind_gust', 'wind_direction')
        OR forecast_samples.lead_hours < 1
        OR forecast_samples.issued_at NOT LIKE '____-__-__T__:__:__Z'
        OR forecast_samples.valid_at NOT LIKE '____-__-__T__:__:__Z'
    );

CREATE INDEX idx_samples_recent ON forecast_samples(site_id, feed_id, issued_at, valid_at, variable, model_run_id);

CREATE INDEX idx_samples_runs
            ON forecast_samples(site_id, feed_id, model_run_id);

CREATE INDEX idx_samples_site_var_valid
            ON forecast_samples(site_id, variable, valid_at);

CREATE UNIQUE INDEX idx_samples_slot ON forecast_samples(site_id, feed_id, variable, valid_at);

CREATE INDEX idx_site_days_state
            ON site_days(site_id, state, local_date);

CREATE INDEX idx_site_days_writable
            ON site_days(site_id, local_date)
            WHERE state IN ('open','closing');

CREATE INDEX idx_site_zone_changes_site
            ON site_zone_changes(site_id, effective_date);

CREATE INDEX idx_station_changes_station
            ON station_changes(station_id, effective_date);

CREATE INDEX idx_tz_generations_site
            ON timezone_generations(site_id, state);

CREATE TRIGGER trg_sites_backfill_status_insert_check
        BEFORE INSERT ON sites
        FOR EACH ROW
        WHEN NEW.backfill_status IS NOT NULL
         AND NEW.backfill_status NOT IN ('pending','in_progress','complete')
        BEGIN
            SELECT RAISE(ABORT, 'invalid backfill_status');
        END;

CREATE TRIGGER trg_sites_backfill_status_insert_default
        AFTER INSERT ON sites
        FOR EACH ROW
        WHEN NEW.backfill_status IS NULL
        BEGIN
            UPDATE sites SET backfill_status='pending' WHERE id=NEW.id;
        END;

CREATE TRIGGER trg_sites_backfill_status_update_check
        BEFORE UPDATE OF backfill_status ON sites
        FOR EACH ROW
        WHEN NEW.backfill_status IS NULL
          OR NEW.backfill_status NOT IN ('pending','in_progress','complete')
        BEGIN
            SELECT RAISE(ABORT, 'invalid backfill_status');
        END;
