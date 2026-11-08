"""The raw station readings in ``station_wind_records``, and their writer.

Every raw reading the add-on keeps is one row, keyed by station, instant and
source: the 5-minute download (``five_minute``), the current-conditions poll
(``current_obs``) and the gap fill's hourly download (``hourly``). A row
holds the reading's own local date, temperature, rain running total and
wind, and when the download that last wrote it returned.

``store_readings`` is the table's one writer apart from the retention delete
(``prune_raw_readings``) and the migration. Both writers run in the caller's
transaction and never commit; the clock is read only through their
``now_utc`` arguments.

``next_five_minute_try`` reads the 5-minute download's schedule, which the
wind lane keeps in ``station_wind_days.next_attempt_at``. ``reading_times``,
``last_five_minute_store`` and ``raw_readings`` read the stored readings;
like ``next_five_minute_try`` they write nothing.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final

from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.day_guard_log import warn_refused_instants
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.site_days import (
    WriteKind,
    check_date_write,
    current_day,
    filter_day_writes,
)
from wxverify.obs.hourly_values import Quantity, Source, StationReading

logger = logging.getLogger(__name__)

#: The closed-day guard's write kind for each source: the two collection
#: downloads write readings, the gap fill writes after its day has ended.
_SOURCE_KINDS: Final[dict[Source, WriteKind]] = {
    "five_minute": WriteKind.READING,
    "current_obs": WriteKind.READING,
    "hourly": WriteKind.GAP_FILL,
}


@dataclass(frozen=True)
class StoreResult:
    """The counts of one ``store_readings`` call, over the whole call.

    ``kept`` readings added their row or changed one of its values;
    ``already_stored`` readings matched a stored row and changed no value
    (only the row's ``fetched_at`` moved). ``refused`` readings were declined
    by the closed-day guard, or belong to no station; ``dropped`` readings
    carried no value, a value that is not finite, or a negative rain total or
    wind. ``kept_by_quantity`` counts, per quantity, the kept readings that
    carry a value of it; every quantity has a key, zero included.
    """

    kept: int
    already_stored: int
    refused: int
    dropped: int
    kept_by_quantity: dict[Quantity, int]


def _stamp(value: datetime) -> str:
    """``value`` as a canonical whole-second UTC stamp."""
    return isoformat_utc(value.replace(microsecond=0))


def _storable(reading: StationReading) -> bool:
    """Whether ``reading`` can be stored without failing a CHECK of the table.

    It needs at least one value; each value present must be finite, and the
    rain total and the wind at least 0.
    """
    values = (reading.temp_c, reading.precip_total_mm, reading.speed_kmh)
    if all(value is None for value in values):
        return False
    if any(value is not None and not math.isfinite(value) for value in values):
        return False
    return all(
        value is None or value >= 0
        for value in (reading.precip_total_mm, reading.speed_kmh)
    )


def store_readings(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    source: Source,
    readings: Sequence[StationReading],
    fetched_at: datetime,
    now_utc: datetime,
) -> StoreResult:
    """Store one download's readings of one station (plan 6.3, 6.4).

    Each reading's site-day is asked through the closed-day guard's batch
    check (``READING`` for ``five_minute`` and ``current_obs``, ``GAP_FILL``
    for ``hourly``); a refused reading is not stored and is counted, and a
    refused batch is logged once. A reading with no value, a value that is
    not finite, or a negative rain total or wind is dropped and counted, so
    no CHECK of the table can fail the caller's transaction. The rest are
    upserted on ``(station_id, obs_at, source)``: each value column and
    ``local_date`` take the new value where the reading has one and keep the
    stored one otherwise, and ``fetched_at`` takes ``fetched_at``.

    Every row is written with ``source``; a reading whose own ``source``
    differs raises ``ValueError`` before anything is written. Runs in the
    caller's transaction: no BEGIN, no commit.
    """
    kind = _SOURCE_KINDS[source]
    for reading in readings:
        if reading.source != source:
            raise ValueError(
                f"reading source {reading.source!r} differs from the call's {source!r}"
            )
    kept_by_quantity: dict[Quantity, int] = {"temperature": 0, "precip": 0, "wind": 0}
    if not readings:
        return StoreResult(
            kept=0,
            already_stored=0,
            refused=0,
            dropped=0,
            kept_by_quantity=kept_by_quantity,
        )
    station = conn.execute(
        "SELECT site_id FROM stations WHERE id = ?", (station_id,)
    ).fetchone()
    if station is None:
        logger.debug(
            "raw readings refused: no station station=%s source=%s count=%d",
            station_id,
            source,
            len(readings),
        )
        return StoreResult(
            kept=0,
            already_stored=0,
            refused=len(readings),
            dropped=0,
            kept_by_quantity=kept_by_quantity,
        )
    site_id = int(station[0])

    stamps = [_stamp(reading.obs_at) for reading in readings]
    allowed = filter_day_writes(conn, site_id, stamps, kind, now_utc=now_utc)
    refused_stamps = [stamp for stamp in stamps if stamp not in allowed]
    warn_refused_instants(
        logger,
        conn,
        kind,
        writer="store_readings",
        site_id=site_id,
        instants=refused_stamps,
    )

    fetched_stamp = _stamp(fetched_at)
    kept = 0
    already_stored = 0
    dropped = 0
    for reading, stamp in zip(readings, stamps, strict=True):
        if stamp not in allowed:
            continue
        if not _storable(reading):
            dropped += 1
            continue
        values = (reading.temp_c, reading.precip_total_mm, reading.speed_kmh)
        stored = conn.execute(
            "SELECT temp_c, precip_total_mm, speed_kmh FROM station_wind_records"
            " WHERE station_id = ? AND obs_at = ? AND source = ?",
            (station_id, stamp, source),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO station_wind_records (
                station_id, obs_at, source, local_date,
                speed_kmh, temp_c, precip_total_mm, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (station_id, obs_at, source) DO UPDATE SET
                local_date = COALESCE(excluded.local_date, local_date),
                speed_kmh = COALESCE(excluded.speed_kmh, speed_kmh),
                temp_c = COALESCE(excluded.temp_c, temp_c),
                precip_total_mm = COALESCE(excluded.precip_total_mm, precip_total_mm),
                fetched_at = excluded.fetched_at
            """,
            (
                station_id,
                stamp,
                source,
                None if reading.local_date is None else reading.local_date.isoformat(),
                reading.speed_kmh,
                reading.temp_c,
                reading.precip_total_mm,
                fetched_stamp,
            ),
        )
        # changes() is 1 for every upsert (fetched_at always moves), so the
        # stored values read before the write decide kept from already stored.
        changed = stored is None or any(
            new is not None and new != old
            for new, old in zip(values, (stored[0], stored[1], stored[2]), strict=True)
        )
        if not changed:
            already_stored += 1
            continue
        kept += 1
        if reading.temp_c is not None:
            kept_by_quantity["temperature"] += 1
        if reading.precip_total_mm is not None:
            kept_by_quantity["precip"] += 1
        if reading.speed_kmh is not None:
            kept_by_quantity["wind"] += 1

    refused = len(refused_stamps)
    logger.debug(
        "raw readings stored station=%s source=%s kept=%d already_stored=%d"
        " refused=%d dropped=%d",
        station_id,
        source,
        kept,
        already_stored,
        refused,
        dropped,
    )
    return StoreResult(
        kept=kept,
        already_stored=already_stored,
        refused=refused,
        dropped=dropped,
        kept_by_quantity=kept_by_quantity,
    )


# One second in Julian days: the margin of the SQL prefetch of the first
# record after an instant; the exact comparison is made on parsed instants.
_JULIAN_SECOND: Final = 1.0 / 86400.0


def _first_day_start_after(
    conn: sqlite3.Connection, site_id: int, now_utc: datetime
) -> datetime | None:
    """The ``day_start_utc`` of the site's first day record after ``now_utc``."""
    rows = conn.execute(
        "SELECT day_start_utc FROM site_days WHERE site_id = ?"
        " AND julianday(day_start_utc) >= julianday(?) - ?"
        " ORDER BY local_date LIMIT 3",
        (site_id, isoformat_utc(now_utc), _JULIAN_SECOND),
    ).fetchall()
    for row in rows:
        start = parse_utc(str(row[0]))
        if start > now_utc:
            return start
    return None


def next_five_minute_try(
    conn: sqlite3.Connection, *, site_id: int, now_utc: datetime
) -> datetime | None:
    """When the site's next 5-minute download is scheduled (plan 12.2).

    The earliest ``next_attempt_at`` among the site's enabled stations' rows
    for the current day that lies before the day's end; a retryable row
    whose stamp is NULL or unreadable is due now and gives ``now_utc``. When
    no row has one (every row is done for the day), the day's end. The
    current day is P1's record holding ``now_utc``; with no record holding
    ``now_utc``, the ``day_start_utc`` of the site's first record after it
    (None when there is none). None when the site has no enabled station
    or does not exist. Read-only.
    """
    if (
        conn.execute(
            "SELECT 1 FROM stations WHERE site_id = ? AND enabled = 1 LIMIT 1",
            (site_id,),
        ).fetchone()
        is None
    ):
        return None
    record = current_day(conn, site_id, now_utc=now_utc)
    if record is None:
        return _first_day_start_after(conn, site_id, now_utc)
    local_date, day_end = record.local_date, record.day_end_utc
    rows = conn.execute(
        """
        SELECT d.next_attempt_at
        FROM station_wind_days d JOIN stations st ON st.id = d.station_id
        WHERE st.site_id = ? AND st.enabled = 1 AND d.local_date = ?
          AND (d.status IN ('pending','partial')
               OR (d.status = 'failed' AND d.attempts < 3))
        """,
        (site_id, local_date.isoformat()),
    ).fetchall()
    earliest: datetime | None = None
    for row in rows:
        raw: object = row[0]
        due: datetime | None = None
        if isinstance(raw, str):
            try:
                due = parse_utc(raw)
            except ValueError:
                due = None
        if due is None:
            due = now_utc
        if due < day_end and (earliest is None or due < earliest):
            earliest = due
    return day_end if earliest is None else earliest


# --- the reads (plan 12.2, 12.3) -------------------------------------------

_SOURCES: Final[dict[str, Source]] = {
    "five_minute": "five_minute",
    "current_obs": "current_obs",
    "hourly": "hourly",
}


@dataclass(frozen=True)
class RawReading:
    """One stored raw reading, as ``raw_readings`` returns it (plan 12.3).

    Each field is the row's column of that name: ``obs_at`` and
    ``fetched_at`` as UTC instants, ``local_date`` as a date, and a NULL
    column as None.
    """

    obs_at: datetime
    source: Source
    local_date: date | None
    temp_c: float | None
    precip_total_mm: float | None
    speed_kmh: float | None
    fetched_at: datetime | None


def _bound_stamp(value: datetime) -> str:
    """``value`` as a canonical stamp for a range bound on ``obs_at``.

    ``obs_at`` holds whole seconds, so a bound with a fraction of a second
    is rounded up to the next whole second, which selects the same readings
    both as an included start and as an excluded end.
    """
    if value.microsecond:
        value = value.replace(microsecond=0) + timedelta(seconds=1)
    return isoformat_utc(value)


def _stored_value(raw: object) -> float | None:
    """A stored value as a float; ``ValueError`` when it is not a number."""
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        raise ValueError("not a number")
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError("not finite")
    return value


def _instant_and_source(row: sqlite3.Row) -> tuple[datetime, Source]:
    """A row's ``obs_at`` and ``source``; ``ValueError`` when unreadable."""
    raw_at: object = row[0]
    raw_source: object = row[1]
    if not isinstance(raw_at, str) or not isinstance(raw_source, str):
        raise ValueError("unreadable stamp or source")
    source = _SOURCES.get(raw_source)
    if source is None:
        raise ValueError("unknown source")
    return parse_utc(raw_at), source


def _raw_reading_from_row(row: sqlite3.Row) -> RawReading:
    """One row of ``raw_readings``' query; ``ValueError`` when unreadable."""
    obs_at, source = _instant_and_source(row)
    raw_date: object = row[2]
    raw_fetched: object = row[6]
    if raw_date is None:
        local_date = None
    elif isinstance(raw_date, str):
        local_date = date.fromisoformat(raw_date)
    else:
        raise ValueError("unreadable local date")
    if raw_fetched is None:
        fetched_at = None
    elif isinstance(raw_fetched, str):
        fetched_at = parse_utc(raw_fetched)
    else:
        raise ValueError("unreadable fetched_at")
    return RawReading(
        obs_at=obs_at,
        source=source,
        local_date=local_date,
        temp_c=_stored_value(row[3]),
        precip_total_mm=_stored_value(row[4]),
        speed_kmh=_stored_value(row[5]),
        fetched_at=fetched_at,
    )


def _warn_unreadable(station_id: int, count: int) -> None:
    if count:
        logger.warning(
            "unreadable raw readings skipped station=%s count=%d", station_id, count
        )


def reading_times(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    start_utc: datetime,
    end_utc: datetime,
    source: Source | None = None,
) -> list[tuple[datetime, Source]]:
    """The station's reading instants and sources in a range (plan 12.2).

    ``start_utc`` is included and ``end_utc`` excluded; with ``source``, only
    that source's readings. In time order, and by source name at one
    instant. A row with an unreadable stamp or source is left out; the call
    logs them once at WARNING. Read-only.
    """
    rows = conn.execute(
        "SELECT obs_at, source FROM station_wind_records"
        " WHERE station_id = ? AND obs_at >= ? AND obs_at < ?"
        " AND (? IS NULL OR source = ?)"
        " ORDER BY obs_at, source",
        (station_id, _bound_stamp(start_utc), _bound_stamp(end_utc), source, source),
    ).fetchall()
    times: list[tuple[datetime, Source]] = []
    unreadable = 0
    for row in rows:
        try:
            obs_at, row_source = _instant_and_source(row)
        except ValueError:
            unreadable += 1
            continue
        if start_utc <= obs_at < end_utc:
            times.append((obs_at, row_source))
    _warn_unreadable(station_id, unreadable)
    return times


def last_five_minute_store(
    conn: sqlite3.Connection, *, site_id: int
) -> datetime | None:
    """The latest ``fetched_at`` of a ``five_minute`` row of the site (plan 12.2).

    Every station of the site counts, switched off ones included; rows of
    other sources and rows with no ``fetched_at`` do not. None when the site
    has no such row. The latest is picked on parsed instants. A
    ``fetched_at`` that SQLite reads as an instant but ``parse_utc`` does
    not (a bare number, say) is skipped, and the next latest is tried; the
    call logs the skipped rows once at WARNING. None when no row parses.
    Read-only.
    """
    cursor = conn.execute(
        "SELECT r.fetched_at FROM station_wind_records r"
        " JOIN stations st ON st.id = r.station_id"
        " WHERE st.site_id = ? AND r.source = 'five_minute'"
        " AND julianday(r.fetched_at) IS NOT NULL"
        " ORDER BY julianday(r.fetched_at) DESC",
        (site_id,),
    )
    latest: datetime | None = None
    unreadable = 0
    try:
        for row in cursor:
            raw: object = row[0]
            if not isinstance(raw, str):
                unreadable += 1
                continue
            try:
                latest = parse_utc(raw)
            except ValueError:
                unreadable += 1
                continue
            break
    finally:
        cursor.close()
    if unreadable:
        logger.warning(
            "unreadable raw readings skipped site=%s count=%d", site_id, unreadable
        )
    return latest


def raw_readings(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    start_utc: datetime,
    end_utc: datetime,
) -> list[RawReading]:
    """The station's stored readings in a range, every column (plan 12.3).

    ``start_utc`` is included and ``end_utc`` excluded. In time order, and
    by source name at one instant. A row with an unreadable field is left
    out; the call logs them once at WARNING. Read-only.
    """
    rows = conn.execute(
        "SELECT obs_at, source, local_date, temp_c, precip_total_mm, speed_kmh,"
        " fetched_at FROM station_wind_records"
        " WHERE station_id = ? AND obs_at >= ? AND obs_at < ?"
        " ORDER BY obs_at, source",
        (station_id, _bound_stamp(start_utc), _bound_stamp(end_utc)),
    ).fetchall()
    readings: list[RawReading] = []
    unreadable = 0
    for row in rows:
        try:
            reading = _raw_reading_from_row(row)
        except ValueError:
            unreadable += 1
            continue
        if start_utc <= reading.obs_at < end_utc:
            readings.append(reading)
    _warn_unreadable(station_id, unreadable)
    return readings


# --- retention (plan 10.3) -------------------------------------------------

#: The ``runtime_state`` key prefix of each site's last pruned site-day; the
#: site id follows it, the value is a local date, ``YYYY-MM-DD``.
_PRUNED_THROUGH_PREFIX: Final = "raw_readings_pruned_through:"


def _pruned_through(conn: sqlite3.Connection, site_id: int) -> date | None:
    """The site's last pruned site-day; None when none is recorded.

    A value that does not parse counts as none, logged at WARNING: the walk
    then starts again at the site's first record, and a day already pruned
    deletes nothing a second time.
    """
    raw = get_runtime_state(conn, f"{_PRUNED_THROUGH_PREFIX}{site_id}")
    if raw is None:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        logger.warning(
            "raw readings prune marker unreadable site=%s; walking from the first day",
            site_id,
        )
        return None


def _days_after(
    conn: sqlite3.Connection, site_id: int, after: date | None
) -> list[tuple[date, datetime, datetime]]:
    """The site's records after ``after``, oldest first, with their bounds."""
    rows = conn.execute(
        "SELECT local_date, day_start_utc, day_end_utc FROM site_days"
        " WHERE site_id = ? AND local_date > ? ORDER BY local_date",
        (site_id, "" if after is None else after.isoformat()),
    ).fetchall()
    return [
        (
            date.fromisoformat(str(row[0])),
            parse_utc(str(row[1])),
            parse_utc(str(row[2])),
        )
        for row in rows
    ]


def prune_raw_readings(conn: sqlite3.Connection, *, now_utc: datetime) -> int:
    """Delete the raw readings of site-days past retention (plan 10.1, 10.3).

    For each site, walks its site-days after the last one pruned (the
    ``runtime_state`` key ``raw_readings_pruned_through:<site_id>``), oldest
    first, and asks the closed-day guard about each with ``RAW_PRUNE``. A day
    it accepts loses every reading inside its bounds, every source, for each
    station of the site (switched off ones included), and becomes the site's
    last pruned day. The walk stops at the first day the guard refuses, which
    is the normal end, and leaves that day and every later one alone.
    Readings no tracked site-day holds are never touched.

    Returns the number of rows deleted. Runs in the caller's transaction: no
    BEGIN, no commit. Safe to repeat.
    """
    deleted = 0
    site_ids = [
        int(row[0])
        for row in conn.execute("SELECT id FROM sites ORDER BY id").fetchall()
    ]
    for site_id in site_ids:
        station_ids = [
            int(row[0])
            for row in conn.execute(
                "SELECT id FROM stations WHERE site_id = ? ORDER BY id", (site_id,)
            ).fetchall()
        ]
        site_rows = 0
        days = 0
        stopped_at: date | None = None
        for local_date, day_start, day_end in _days_after(
            conn, site_id, _pruned_through(conn, site_id)
        ):
            if not check_date_write(
                conn, site_id, local_date, WriteKind.RAW_PRUNE, now_utc=now_utc
            ):
                stopped_at = local_date
                break
            start, end = _bound_stamp(day_start), _bound_stamp(day_end)
            for station_id in station_ids:
                cur = conn.execute(
                    "DELETE FROM station_wind_records"
                    " WHERE station_id = ? AND obs_at >= ? AND obs_at < ?",
                    (station_id, start, end),
                )
                site_rows += cur.rowcount
            set_runtime_state(
                conn, f"{_PRUNED_THROUGH_PREFIX}{site_id}", local_date.isoformat()
            )
            days += 1
        if days or stopped_at is not None:
            logger.debug(
                "raw readings pruned site=%s days=%d rows=%d stopped_at=%s",
                site_id,
                days,
                site_rows,
                "none" if stopped_at is None else stopped_at.isoformat(),
            )
        deleted += site_rows
    return deleted
