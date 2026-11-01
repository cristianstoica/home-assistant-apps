"""The v16 carry-over of the stored forecast values (rain release, Parts B, C).

``plans/2026-11-01-rain-release.md``, Part B, "Upgrade carry-over", and Part
C, C6. A file below version 16 keeps every label of an hour in
``forecast_samples`` and has no ``forecast_scored``, ``rain_days`` or
``rain_forecast_days`` rows. ``migrate_v16_rain_release``
(``db/migrations.py``) runs, in order, :func:`carry_over_scored_values`
(step 1), the Part C rain history fill (:func:`fill_rain_history`), and
:func:`keep_newest_samples` (step 2), then makes ``idx_samples_slot``
(step 3).

The migration imports this module inside its body: ``scoring/downloads.py``,
whose 0.16.14 reads step 1 repeats, imports ``db/migrations.py``. Every
function runs in the caller's transaction and never commits.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from datetime import UTC, date, datetime, timedelta
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify.collection.forecast_fetcher import SCORED_DAYS_AHEAD, site_local_dates
from wxverify.collection.forecast_validation import invalid_forecast_sample_sql
from wxverify.collection.precip_labels import precip_label_eligible_sql
from wxverify.core.timeutil import isoformat_utc, local_day_slots, parse_utc
from wxverify.scoring.close_scores import day_rain_total, rain_nearby
from wxverify.scoring.downloads import scored_download, scored_version

logger = logging.getLogger(__name__)

# The feeds the close scores: the real feeds less the meteoblue package row,
# as scoring/close_scores.py's _REAL_FEEDS_SQL lists them.
_REAL_FEEDS_SQL: Final = (
    "SELECT id FROM feeds WHERE is_virtual = 0"
    " AND NOT (source = 'meteoblue' AND model = 'multimodel')"
    " ORDER BY id"
)
_OLDEST_UNDONE_SQL: Final = (
    "SELECT local_date, day_start_utc FROM site_days"
    " WHERE site_id = ? AND state NOT IN ('closed', 'missed')"
    " ORDER BY local_date LIMIT 1"
)
_INSERT_CARRIED_SQL: Final = """
    INSERT INTO forecast_scored
        (site_id, feed_id, variable, valid_at, day_ahead, value, lead_hours,
         issued_at, download_id, download_date, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (site_id, feed_id, variable, valid_at, day_ahead) DO NOTHING
"""
# A scored version's hours lie on S to S + 7; nine UTC days from S's UTC
# midnight hold them in any zone.
_VERSION_SPAN: Final = timedelta(days=9)
_DAYS_BEFORE_OLDEST: Final = 7
# Part C, C6: the rain history fill's window, and its reads and writes.
_RAIN_HISTORY_DAYS: Final = 60
_HISTORY_STATION_RAIN_SQL: Final = (
    "SELECT value FROM station_observations"
    " WHERE station_id = ? AND variable = 'precip' AND qc_flag = 'ok'"
    " AND valid_at >= ? AND valid_at < ?"
)
_INSERT_HISTORY_RAIN_DAY_SQL: Final = """
    INSERT OR IGNORE INTO rain_days
        (site_id, local_date, outcome, stations_full, stations_partial)
    VALUES (?, ?, ?, ?, ?)
"""
_INSERT_HISTORY_RAIN_FORECAST_SQL: Final = """
    INSERT OR IGNORE INTO rain_forecast_days
        (site_id, local_date, feed_id, day_ahead, total_mm)
    VALUES (?, ?, ?, ?, ?)
"""


def carry_over_scored_values(conn: sqlite3.Connection, *, now_utc: datetime) -> int:
    """Step 1: write the open days' scored values into ``forecast_scored``.

    For each site with a day not yet done, each real feed and each date S
    from the site's oldest such day O minus 7 to the site's current date,
    the scored download of S and its version as 0.16.14 read them
    (``scored_download``, ``scored_version``), kept for hours on O or later
    at their day ahead: the site-local date of ``valid_at``
    (``site_local_dates``) minus S, only 0 to 7. Each row carries the
    download's identity: its ``download_id`` (``None`` for an inferred
    download), S and its ``fetched_at``. That is what the close would have
    scored for O and every later day. A row already stored is kept, so a
    repeat writes nothing new. The current date is the date of the record
    that holds ``now_utc``, else ``now_utc``'s date in ``sites.timezone``.
    Returns the rows written.
    """
    before = conn.total_changes
    feed_ids = [int(row[0]) for row in conn.execute(_REAL_FEEDS_SQL).fetchall()]
    for site_row in conn.execute("SELECT id FROM sites ORDER BY id").fetchall():
        site_id = int(site_row[0])
        oldest = conn.execute(_OLDEST_UNDONE_SQL, (site_id,)).fetchone()
        if oldest is None:
            continue
        oldest_date = date.fromisoformat(str(oldest[0]))
        oldest_start = parse_utc(str(oldest[1]))
        today = _current_date(conn, site_id, now_utc)
        if today is None:
            continue
        first = oldest_date - timedelta(days=_DAYS_BEFORE_OLDEST)
        for feed_id in feed_ids:
            for offset in range((today - first).days + 1):
                _carry_over_day(
                    conn,
                    site_id=site_id,
                    feed_id=feed_id,
                    download_date=first + timedelta(days=offset),
                    oldest_start=oldest_start,
                )
    written = conn.total_changes - before
    logger.info("v16 carry-over: %d scored value row(s) written", written)
    return written


def _carry_over_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    download_date: date,
    oldest_start: datetime,
) -> None:
    """One feed's scored values of one date S, for hours from
    ``oldest_start`` on (step 1)."""
    download = scored_download(
        conn, site_id=site_id, feed_id=feed_id, local_date=download_date
    )
    if download is None:
        return
    s_midnight = datetime(
        download_date.year, download_date.month, download_date.day, tzinfo=UTC
    )
    version = scored_version(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        download=download,
        start_utc=oldest_start,
        end_utc=s_midnight + _VERSION_SPAN,
    )
    if not version:
        return
    dates = site_local_dates(conn, site_id, (s.valid_at for s in version.values()))
    params: list[tuple[object, ...]] = []
    for sample in version.values():
        target = dates.get(sample.valid_at)
        if target is None:
            continue
        day_ahead = (target - download_date).days
        if day_ahead not in SCORED_DAYS_AHEAD:
            continue
        params.append(
            (
                site_id,
                feed_id,
                sample.variable,
                sample.valid_at,
                day_ahead,
                sample.value,
                sample.lead_hours,
                sample.issued_at,
                download.download_id,
                download_date.isoformat(),
                download.fetched_at,
            )
        )
    conn.executemany(_INSERT_CARRIED_SQL, params)


def _current_date(
    conn: sqlite3.Connection, site_id: int, now_utc: datetime
) -> date | None:
    """The site's current local date by :func:`site_local_dates`' rule;
    ``None`` for a site that does not exist."""
    now = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=UTC)
    stamp = isoformat_utc(now)
    return site_local_dates(conn, site_id, [stamp]).get(stamp)


def fill_rain_history(conn: sqlite3.Connection) -> int:
    """C6: fill ``rain_days`` and ``rain_forecast_days`` for the past 60 days.

    Part C's carry-over (owner's answer to Q1, 2026-11-01: "b"). For each
    site with a day not yet done, the 60 dates T from its oldest such day O
    minus 60 to O minus 1, each with its record's bounds and zone when it
    has one, else its local day in ``sites.timezone``:

    - **Truth** (:func:`rain_nearby`): from every station of the site's
      ``ok`` rain rows in T; no set-aside existed before the upgrade, so
      every station counts. A date with no station row is left out
      (unknown).
    - **Forecasts**: for each real feed and each date S from O - 67 to
      O - 1, the scored download of S and its version as 0.16.14 read them
      (``scored_download``, ``scored_version``); each T = S + k (k 0 to 7)
      in the window whose rain those values cover exactly gets its total
      at day ahead k (:func:`day_rain_total`). ``scored_version`` applies
      the rain-label boundary (``precip_label_eligible_sql``), so
      Open-Meteo and meteoblue rain stored before 0.16.12 is left out.

    Rows already stored are kept (INSERT OR IGNORE), so a repeat writes
    nothing new and a close's own row is never replaced. A model with fewer
    than 20 filled days stays provisional: the fill writes only the days it
    finds. Must run before :func:`keep_newest_samples`, whose delete removes
    the older labels ``scored_version`` reads. Returns the rows written.
    """
    before = conn.total_changes
    feed_ids = [int(row[0]) for row in conn.execute(_REAL_FEEDS_SQL).fetchall()]
    sites = conn.execute("SELECT id, timezone FROM sites ORDER BY id").fetchall()
    for site_row in sites:
        site_id = int(site_row[0])
        oldest = conn.execute(_OLDEST_UNDONE_SQL, (site_id,)).fetchone()
        if oldest is None:
            continue
        oldest_date = date.fromisoformat(str(oldest[0]))
        first = oldest_date - timedelta(days=_RAIN_HISTORY_DAYS)
        days = {
            first + timedelta(days=offset): _history_day(
                conn, site_id, first + timedelta(days=offset), str(site_row[1])
            )
            for offset in range(_RAIN_HISTORY_DAYS)
        }
        station_ids = [
            int(row[0])
            for row in conn.execute(
                "SELECT id FROM stations WHERE site_id = ? ORDER BY id", (site_id,)
            ).fetchall()
        ]
        for target, (start, end, _zone) in days.items():
            stations: list[tuple[float, int]] = []
            for station_id in station_ids:
                values = [
                    float(row[0])
                    for row in conn.execute(
                        _HISTORY_STATION_RAIN_SQL,
                        (station_id, isoformat_utc(start), isoformat_utc(end)),
                    ).fetchall()
                ]
                if values:
                    stations.append((math.fsum(values), len(values)))
            if not stations:
                continue
            outcome, full, partial = rain_nearby(stations)
            conn.execute(
                _INSERT_HISTORY_RAIN_DAY_SQL,
                (site_id, target.isoformat(), outcome, full, partial),
            )
        for feed_id in feed_ids:
            for offset in range(_RAIN_HISTORY_DAYS + _DAYS_BEFORE_OLDEST):
                _fill_rain_forecasts(
                    conn,
                    site_id=site_id,
                    feed_id=feed_id,
                    download_date=first
                    - timedelta(days=_DAYS_BEFORE_OLDEST)
                    + timedelta(days=offset),
                    days=days,
                )
    written = conn.total_changes - before
    logger.info("v16 carry-over: %d rain history row(s) written", written)
    return written


def _history_day(
    conn: sqlite3.Connection, site_id: int, local_date: date, site_zone: str
) -> tuple[datetime, datetime, str]:
    """A history date's bounds and zone: its record's when it has one, else
    its local day in the site's zone (UTC when that zone does not load)."""
    row = conn.execute(
        "SELECT day_start_utc, day_end_utc, timezone FROM site_days"
        " WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    if row is not None:
        try:
            ZoneInfo(str(row[2]))
            return parse_utc(str(row[0])), parse_utc(str(row[1])), str(row[2])
        except (ZoneInfoNotFoundError, ValueError):
            pass
    try:
        ZoneInfo(site_zone)
        zone = site_zone
    except (ZoneInfoNotFoundError, ValueError):
        zone = "UTC"
    start, end, _slots = local_day_slots(local_date, zone)
    return start, end, zone


def _fill_rain_forecasts(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    download_date: date,
    days: dict[date, tuple[datetime, datetime, str]],
) -> None:
    """One feed's daily rain totals from the scored download of one date S
    (C6), for the window's dates S + 0 to S + 7."""
    targets = [
        (k, download_date + timedelta(days=k))
        for k in SCORED_DAYS_AHEAD
        if download_date + timedelta(days=k) in days
    ]
    if not targets:
        return
    download = scored_download(
        conn, site_id=site_id, feed_id=feed_id, local_date=download_date
    )
    if download is None:
        return
    version = scored_version(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        download=download,
        start_utc=min(days[target][0] for _k, target in targets),
        end_utc=max(days[target][1] for _k, target in targets),
    )
    rain = [
        (parse_utc(sample.valid_at), sample)
        for sample in version.values()
        if sample.variable == "precip"
    ]
    for k, target in targets:
        start, end, zone = days[target]
        day_total = day_rain_total(
            [sample for instant, sample in rain if start <= instant < end],
            local_date=target,
            timezone=zone,
        )
        if day_total is not None:
            conn.execute(
                _INSERT_HISTORY_RAIN_FORECAST_SQL,
                (site_id, target.isoformat(), feed_id, k, day_total),
            )


def keep_newest_samples(conn: sqlite3.Connection) -> int:
    """Step 2: keep one ``forecast_samples`` row per hour, delete the rest.

    In each ``(site_id, feed_id, variable, valid_at)`` the row the Forecast
    page showed (``forecast/data.py`` ``load_future_samples``): the newest
    ``issued_at`` among the rows that pass the shared validator and the
    rain-label boundary, or else the newest row. The old ``UNIQUE(...,
    issued_at, valid_at)`` makes ``issued_at`` unique within an hour; the
    row id breaks a tie should there be one. Returns the rows deleted.
    """
    deleted = conn.execute(
        f"""
        DELETE FROM forecast_samples
        WHERE id IN (
            SELECT id FROM (
                SELECT fs.id,
                       ROW_NUMBER() OVER (
                           PARTITION BY fs.site_id, fs.feed_id, fs.variable,
                                        fs.valid_at
                           ORDER BY
                               CASE WHEN COALESCE(
                                   (NOT {invalid_forecast_sample_sql("fs")})
                                   AND {precip_label_eligible_sql("fs", "f")},
                                   0
                               ) THEN 0 ELSE 1 END,
                               fs.issued_at DESC,
                               fs.id DESC
                       ) AS rn
                FROM forecast_samples fs
                LEFT JOIN feeds f ON f.id = fs.feed_id
            )
            WHERE rn > 1
        )
        """
    ).rowcount
    logger.info("v16 carry-over: %d older forecast_samples row(s) deleted", deleted)
    return deleted
