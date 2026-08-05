"""Interval scheduler due queries."""

from __future__ import annotations

import logging
import sqlite3
from datetime import timedelta

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
