"""Interval scheduler due queries."""

from __future__ import annotations

import logging
import sqlite3
from datetime import timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify.core.hashing import obs_jitter_minutes
from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.queue import enqueue_if_absent_with_cooldown
from wxverify.settings.keys import get_number_setting
from wxverify.worker.cadence import parse_fetch_interval_minutes

logger = logging.getLogger(__name__)

# Metered external provider calls on a machine-driven path: nobody is
# waiting, so a false-suppress costs ~nothing while a false-permit burns
# quota. One hour keeps the suppression inside the default 180-minute obs
# cadence and only bites the shorter-interval fetch_feed/fetch_current_obs
# paths, and only after several consecutive failures -- a strong signal the
# endpoint is genuinely down, not a blip. Three layers now bound quota
# exhaustion for a provider with a small daily budget and a short poll
# interval, each closing a different part of the risk: this cooldown bounds
# how often a new failure episode can start at all; max_retries (reduced to
# 3 at the fetch_feed enqueue sites) bounds how many calls one episode can
# spend; and the cadence floor derived in fetch_feed_retry_floor_seconds
# bounds the spacing between retries inside an episode, so a hard-failing
# feed cannot retry faster than it would ever have been polled.
_DUE_JOB_FAILURE_COOLDOWN = timedelta(hours=1)


def scheduler_tick(conn: sqlite3.Connection) -> None:
    logger.debug("scheduler tick")
    _enqueue_due_feeds(conn)
    _enqueue_due_obs(conn)
    _enqueue_due_current_obs(conn)
    _enqueue_due_forecast_records(conn)
    _enqueue_due_verification_runs(conn)


# Site-local wall-clock time at/after which the nightly verification trigger
# fires (§14). A scheduling default, not §17 methodology — it cannot change
# any score. Late (02:00) so the day's record and truth writes land first.
VERIFICATION_TRIGGER_LOCAL_TIME = "02:00"


def _enqueue_due_verification_runs(conn: sqlite3.Connection) -> None:
    """Enqueue the nightly verification chain per site (§14).

    Due when the site's local time has passed the trigger time and no
    trigger decision exists yet for (site, local today). An active chain
    suppresses the trigger with a DURABLE ``suppressed_because_active``
    decision row — that row also gates re-enqueue for the rest of the day.
    Bad tz config fails closed per site, like the record trigger above.
    """
    from wxverify.verification.record import resolve_snapshot_utc
    from wxverify.verification.runs import (
        record_trigger_decision,
        trigger_decision_exists,
    )
    from wxverify.worker.verification_run import verification_job_key

    now = utc_now()
    rows = conn.execute("SELECT id, timezone FROM sites WHERE enabled = 1").fetchall()
    for row in rows:
        site_id = int(row["id"])
        timezone = str(row["timezone"])
        try:
            tz = ZoneInfo(timezone)
            local_today = now.astimezone(tz).date()
            trigger_utc = resolve_snapshot_utc(
                timezone, local_today, VERIFICATION_TRIGGER_LOCAL_TIME
            )
        except (ZoneInfoNotFoundError, ValueError):
            logger.warning(
                "scheduler: unusable verification trigger config site=%s; skipping",
                site_id,
            )
            continue
        if now < trigger_utc:
            continue
        day_iso = local_today.isoformat()
        if trigger_decision_exists(conn, site_id, day_iso):
            continue
        active = conn.execute(
            """
            SELECT 1 FROM jobs
            WHERE type = 'verification_run' AND site_id = ? AND job_key = ?
              AND status IN ('pending', 'running')
            LIMIT 1
            """,
            (site_id, verification_job_key(site_id)),
        ).fetchone()
        if active is not None:
            record_trigger_decision(
                conn,
                site_id,
                trigger_date=day_iso,
                decision="suppressed_because_active",
                reason="verification chain already active",
            )
            continue
        enqueue_if_absent_with_cooldown(
            conn,
            "verification_run",
            site_id,
            verification_job_key(site_id),
            {"trigger_date": day_iso},
            cooldown=_DUE_JOB_FAILURE_COOLDOWN,
        )


def _enqueue_due_forecast_records(conn: sqlite3.Connection) -> None:
    """Enqueue the daily forecast_record job (and gap scan) per site (§14).

    Due when the site's local time has reached today's snapshot time T and
    no record/missed row exists yet for (site, current generation, today).
    The date-scoped ``job_key`` plus ``idx_jobs_active_dedupe`` guarantees
    one active job per day even across the doubled autumn hour. The gap
    scan is enqueued once per local day after T; a completed jobs row for
    the day's key gates re-enqueue.
    """
    from wxverify.db.tz_generations import ensure_published_generation
    from wxverify.verification.record import (
        record_rows_exist,
        resolve_snapshot_utc,
        snapshot_wall_clock,
    )

    now = utc_now()
    rows = conn.execute("SELECT id, timezone FROM sites WHERE enabled = 1").fetchall()
    for row in rows:
        site_id = int(row["id"])
        timezone = str(row["timezone"])
        try:
            tz = ZoneInfo(timezone)
            local_today = now.astimezone(tz).date()
            wall_clock = snapshot_wall_clock(conn, site_id)
            snapshot_utc = resolve_snapshot_utc(timezone, local_today, wall_clock)
        except (ZoneInfoNotFoundError, ValueError):
            # Foreign/corrupt timezone or wall clock: fail closed (skip this
            # site for this tick) -- inventing a snapshot instant would stamp
            # records on an invented schedule. Every other site still runs.
            logger.warning(
                "scheduler: unusable snapshot config site=%s; skipping", site_id
            )
            continue
        if now < snapshot_utc:
            continue
        day_iso = local_today.isoformat()
        generation_id = ensure_published_generation(conn, site_id)
        if not record_rows_exist(conn, site_id, generation_id, day_iso):
            enqueue_if_absent_with_cooldown(
                conn,
                "forecast_record",
                site_id,
                f"record:{day_iso}",
                {"snapshot_local_date": day_iso},
                cooldown=_DUE_JOB_FAILURE_COOLDOWN,
            )
        gap_key = f"gapscan:{day_iso}"
        done = conn.execute(
            """
            SELECT 1 FROM jobs
            WHERE type = 'record_gap_scan' AND site_id = ? AND job_key = ?
            LIMIT 1
            """,
            (site_id, gap_key),
        ).fetchone()
        if done is None:
            enqueue_if_absent_with_cooldown(
                conn,
                "record_gap_scan",
                site_id,
                gap_key,
                {},
                cooldown=_DUE_JOB_FAILURE_COOLDOWN,
            )


def _enqueue_due_feeds(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT s.id AS site_id, f.id AS feed_id, f.fetch_interval_minutes,
               sfs.last_run_at
        FROM sites s
        JOIN feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = s.id AND sfs.feed_id = f.id
        WHERE s.enabled = 1
          AND f.enabled = 1
          AND f.is_virtual = 0
          AND NOT (f.source='meteoblue' AND f.model != 'multimodel')
          AND COALESCE(sfs.enabled, f.default_subscribed) = 1
        """
    ).fetchall()
    now = utc_now()
    for row in rows:
        site_id = int(row["site_id"])
        feed_id = int(row["feed_id"])
        interval = parse_fetch_interval_minutes(
            row["fetch_interval_minutes"],
            context=f"scheduler site={site_id} feed={feed_id}",
        )
        if interval is None:
            # Foreign/corrupt or out-of-range cadence: fail closed (skip
            # this feed for this tick) BEFORE any due decision -- a
            # never-run feed would otherwise be due unconditionally and get
            # paid provider calls on an invented schedule. Every other feed
            # still runs.
            continue
        last_run_at = row["last_run_at"]
        due = last_run_at is None
        if last_run_at is not None:
            try:
                last_run_dt = parse_utc(str(last_run_at))
            except ValueError:
                # Foreign/corrupt last_run_at: fail open (treat as due) so a
                # single unreadable row cannot kill the tick. Self-healing --
                # the next successful run rewrites this column.
                logger.warning(
                    "scheduler: unreadable last_run_at site=%s feed=%s;"
                    " treating feed as due",
                    site_id,
                    feed_id,
                )
                due = True
            else:
                minutes = (now - last_run_dt).total_seconds() / 60
                due = minutes >= interval
        if due:
            logger.debug(
                "scheduler due feed site=%s feed=%s",
                int(row["site_id"]),
                int(row["feed_id"]),
            )
            enqueue_if_absent_with_cooldown(
                conn,
                "fetch_feed",
                int(row["site_id"]),
                f"fetch:{int(row['feed_id'])}",
                {"feed_id": int(row["feed_id"])},
                cooldown=_DUE_JOB_FAILURE_COOLDOWN,
                max_retries=3,
            )


def _enqueue_due_obs(conn: sqlite3.Connection) -> None:
    interval = get_number_setting(conn, "obs_interval_minutes", 180, minimum=30)
    jitter_cap = get_number_setting(conn, "obs_jitter_minutes", 20, minimum=0)
    now = utc_now()
    rows = conn.execute(
        """
        SELECT s.id, s.last_obs_at
        FROM sites s
        WHERE s.enabled=1
          AND EXISTS (
              SELECT 1 FROM stations st
              WHERE st.site_id=s.id AND st.enabled=1
          )
        """
    ).fetchall()
    for row in rows:
        last = row["last_obs_at"]
        due = last is None
        if last is not None:
            try:
                last_dt = parse_utc(str(last))
            except ValueError:
                # Foreign/corrupt last_obs_at: fail open (treat as due), same
                # rationale as the due-feed loop's last_run_at guard above.
                logger.warning(
                    "scheduler: unreadable last_obs_at site=%s; treating obs as due",
                    int(row["id"]),
                )
                due = True
            else:
                cycle_bucket = int(last_dt.timestamp() // (interval * 60))
                jitter = obs_jitter_minutes(int(row["id"]), cycle_bucket, jitter_cap)
                elapsed = (now - last_dt).total_seconds() / 60
                due = elapsed >= interval + jitter
        if due:
            logger.debug("scheduler due obs site=%s", int(row["id"]))
            enqueue_if_absent_with_cooldown(
                conn,
                "fetch_obs",
                int(row["id"]),
                "obs",
                {},
                cooldown=_DUE_JOB_FAILURE_COOLDOWN,
            )


def _enqueue_due_current_obs(conn: sqlite3.Connection) -> None:
    now = isoformat_utc()
    rows = conn.execute(
        """
        SELECT st.id, st.site_id, st.pws_station_id
        FROM stations st
        LEFT JOIN station_poll_state sps ON sps.station_id = st.id
        WHERE st.enabled = 1
          AND (sps.next_poll_at IS NULL OR sps.next_poll_at <= ?)
        """,
        (now,),
    ).fetchall()
    for row in rows:
        station_id = int(row["id"])
        try:
            site_id = int(row["site_id"])
        except (ValueError, OverflowError):
            # Foreign/corrupt site_id: fail closed (skip this station this
            # tick). site_id is the job's identity, not a schedule hint --
            # inventing one would attach a station's observations to whatever
            # site now owns that id. Every other station still runs.
            logger.warning(
                "scheduler: unreadable site_id station=%s; skipping station this tick",
                station_id,
            )
            continue
        logger.debug(
            "scheduler due current_obs site=%s station=%s", site_id, station_id
        )
        enqueue_if_absent_with_cooldown(
            conn,
            "fetch_current_obs",
            site_id,
            f"curobs:{station_id}",
            {"station_id": station_id},
            cooldown=_DUE_JOB_FAILURE_COOLDOWN,
        )
