"""Interval scheduler due queries."""

from __future__ import annotations

import logging
import sqlite3

from wxverify.core.hashing import obs_jitter_minutes
from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.queue import enqueue_if_absent
from wxverify.settings.keys import get_number_setting

logger = logging.getLogger(__name__)


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
        last_run_at = row["last_run_at"]
        due = last_run_at is None
        if last_run_at is not None:
            minutes = (now - parse_utc(str(last_run_at))).total_seconds() / 60
            due = minutes >= int(row["fetch_interval_minutes"])
        if due:
            logger.debug(
                "scheduler due feed site=%s feed=%s",
                int(row["site_id"]),
                int(row["feed_id"]),
            )
            enqueue_if_absent(
                conn,
                "fetch_feed",
                int(row["site_id"]),
                f"fetch:{int(row['feed_id'])}",
                {"feed_id": int(row["feed_id"])},
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
        if last is None:
            logger.debug("scheduler due obs site=%s", int(row["id"]))
            enqueue_if_absent(conn, "fetch_obs", int(row["id"]), "obs", {})
            continue
        last_dt = parse_utc(str(last))
        cycle_bucket = int(last_dt.timestamp() // (interval * 60))
        jitter = obs_jitter_minutes(int(row["id"]), cycle_bucket, jitter_cap)
        elapsed = (now - last_dt).total_seconds() / 60
        if elapsed >= interval + jitter:
            logger.debug("scheduler due obs site=%s", int(row["id"]))
            enqueue_if_absent(conn, "fetch_obs", int(row["id"]), "obs", {})


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
        logger.debug(
            "scheduler due current_obs site=%s station=%s",
            int(row["site_id"]),
            station_id,
        )
        enqueue_if_absent(
            conn,
            "fetch_current_obs",
            int(row["site_id"]),
            f"curobs:{station_id}",
            {"station_id": station_id},
        )
