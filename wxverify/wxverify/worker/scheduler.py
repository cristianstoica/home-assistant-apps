"""Interval scheduler due queries."""

from __future__ import annotations

import logging
import sqlite3
from datetime import timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.constant_wind import assess_constant_wind
from wxverify.db.queue import (
    LATEST_JOB_SQL,
    enqueue_if_absent_with_cooldown,
)
from wxverify.db.site_days import daily_close_on
from wxverify.worker.close_chain import enqueue_due_close_work
from wxverify.worker.round import (
    enqueue_due_daytime_downloads,
    enqueue_due_round_tries,
)
from wxverify.worker.wind_days import (
    WIND_DAYS_JOB_KEY,
    WIND_DAYS_SPACING,
    wind_evening_due,
    wind_lane_due,
)

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
    # While the daily close is off the tick enqueues nothing. The key is read
    # in the tick's own transaction.
    if not daily_close_on(conn):
        return
    # The forward downloads: the scored round and the fixed daytime slots
    # (daily-close P4 4.4, 5.1).
    now = utc_now()
    enqueue_due_round_tries(conn, now_utc=now)
    enqueue_due_daytime_downloads(conn, now_utc=now)
    # The stuck wind station check: at most once an hour, in this write.
    assess_constant_wind(conn, now_utc=now)
    _enqueue_due_wind_days(conn)
    # The close's chain: scores are written only by its stage 5.
    enqueue_due_close_work(conn, now_utc=utc_now())


def _enqueue_due_wind_days(conn: sqlite3.Connection) -> None:
    """Enqueue the wind-history lane for each site with work (plan §8.13).

    One ``fetch_obs`` job with the ``wind-days`` key per enabled site with an
    enabled station, spaced by ``WIND_DAYS_SPACING`` after a success, except
    while a today row of the site is due in the evening window (plan 4.2),
    where the spacing would block the 23:57 to 23:59 slots. An unreadable
    success stamp fails open here: the wrapper's success cooldown fails
    closed on it, which would stop the lane for good.
    """
    now = utc_now()
    rows = conn.execute(
        """
        SELECT s.id, s.timezone
        FROM sites s
        WHERE s.enabled=1
          AND EXISTS (
              SELECT 1 FROM stations st
              WHERE st.site_id=s.id AND st.enabled=1
          )
        """
    ).fetchall()
    for row in rows:
        site_id = int(row["id"])
        try:
            tz = ZoneInfo(str(row["timezone"]))
        except (ZoneInfoNotFoundError, ValueError):
            logger.warning(
                "scheduler: unusable time zone site=%s; skipping the wind lane",
                site_id,
            )
            continue
        if not wind_lane_due(conn, site_id, tz, now=now):
            continue
        success_cooldown: timedelta | None = WIND_DAYS_SPACING
        if wind_evening_due(conn, site_id, tz, now=now):
            success_cooldown = None
        latest = conn.execute(
            LATEST_JOB_SQL, ("fetch_obs", WIND_DAYS_JOB_KEY, site_id)
        ).fetchone()
        if latest is not None and str(latest["status"]) == "completed":
            try:
                parse_utc(str(latest["updated_at"]))
            except ValueError:
                logger.warning(
                    "scheduler: unreadable updated_at on the latest wind lane job"
                    " site=%s; enqueuing without the success spacing",
                    site_id,
                )
                success_cooldown = None
        logger.debug("scheduler due wind lane site=%s", site_id)
        enqueue_if_absent_with_cooldown(
            conn,
            "fetch_obs",
            site_id,
            WIND_DAYS_JOB_KEY,
            {},
            cooldown=_DUE_JOB_FAILURE_COOLDOWN,
            success_cooldown=success_cooldown,
        )


def enqueue_due_current_obs(conn: sqlite3.Connection) -> None:
    """Enqueue a ``fetch_current_obs`` job for every due, enabled station.

    Runs inside the current-obs lane's claim transaction
    (worker.current_obs_poller), not in ``scheduler_tick``.
    """
    now = isoformat_utc()
    rows = conn.execute(
        """
        SELECT st.id, st.site_id, st.pws_station_id, s.id AS site_row
        FROM stations st
        LEFT JOIN sites s ON s.id = st.site_id
        LEFT JOIN station_poll_state sps ON sps.station_id = st.id
        WHERE st.enabled = 1
          AND (s.id IS NULL OR s.enabled = 1)
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
        if row["site_row"] is None:
            logger.warning(
                "scheduler: station=%s references a missing site; "
                "skipping station this tick",
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
