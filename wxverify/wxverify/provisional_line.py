"""The provisional observed line of the Overlay (daily-close plan P5, §8, §9.1).

While a day is not yet closed, the Overlay draws its observed values from
P2's raw table with the close's own method, unchanged: ``hourly_values`` per
day, then each station value's jump check (``qc_flag``) and the site's
consensus (``compute_consensus``), built in memory the way the close builds
the site's hourly truth. Nothing here writes: no station value, consensus
hour, pair or daily truth is stored, and the close's writing functions are
never called.

The pages read the line as ``provisional_site_hours(conn, site_id=...,
variable=..., now_utc=...)`` and the days of a range as ``day_spans``. The
close, pairing, scoring, the rankings, the publication and Station Trust
never import this module (plan §8.5). Day states come only through
``wxverify.db.site_days``'s functions; this module names none of P1's
tables. ``now`` is passed in by the caller.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Final

from wxverify.core.timeutil import floor_hour, isoformat_utc
from wxverify.db.site_days import (
    DONE_STATES,
    DayRecord,
    DayState,
    MissedReason,
    closed_through,
    current_day,
    daily_close_on,
    day_for_instant,
    day_rain_station_ids,
    day_record,
    zone_flagged,
)
from wxverify.obs.hourly_values import QUANTITIES, Quantity, day_hours
from wxverify.obs.qc import qc_flag
from wxverify.scoring.consensus import MAD_FLOORS, compute_consensus
from wxverify.scoring.consensus import StationReading as ConsensusReading
from wxverify.worker.day_values import SiteDayValues, hourly_values

_ONE_DAY: Final = timedelta(days=1)
#: The smallest step of a ``datetime``: a range's last instant is its end less
#: one tick.
_TICK: Final = timedelta(microseconds=1)
#: The variables the line draws, each P2's quantity of the same name.
_QUANTITY_BY_VARIABLE: Final[dict[str, Quantity]] = {q: q for q in QUANTITIES}


@dataclass(frozen=True)
class DaySpan:
    """One P1 day record as the pages draw it (plan §9.1).

    ``start_utc`` and ``end_utc`` are the record's stored bounds, the day
    being ``[start_utc, end_utc)``. ``state``, ``timezone``,
    ``zone_changeover`` and ``missed_reason`` are the record's own;
    ``zone_flagged`` is P1's ``zone_flagged`` for the day: whether the day's
    zone differs from the site's zone.
    """

    local_date: date
    start_utc: datetime
    end_utc: datetime
    state: DayState
    timezone: str
    zone_flagged: bool
    zone_changeover: bool
    missed_reason: MissedReason | None


@dataclass(frozen=True)
class ProvisionalLine:
    """The provisional observed line of one site and variable (plan §8, §9.1).

    ``valid_at`` holds every hour drawn, oldest first: the ended hours of the
    days not yet done. ``values`` holds the site value of each of those hours
    that has one, in °C, mm or m/s; an hour without one is absent, never 0.
    ``latest_reading_at`` is the latest of P2's ``latest_reading_at`` over the
    stations and days drawn (plan §8.4), ``None`` when there is none. With the
    close off all three are empty.
    """

    valid_at: tuple[datetime, ...]
    values: dict[datetime, float]
    latest_reading_at: datetime | None


def _as_utc(value: datetime) -> datetime:
    """``value`` as an aware UTC datetime; a naive value is taken as UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _empty_line() -> ProvisionalLine:
    """The line with nothing drawn."""
    return ProvisionalLine(valid_at=(), values={}, latest_reading_at=None)


def _span(conn: sqlite3.Connection, record: DayRecord) -> DaySpan:
    """The record as a ``DaySpan``, with P1's ``zone_flagged`` for its day."""
    return DaySpan(
        local_date=record.local_date,
        start_utc=record.day_start_utc,
        end_utc=record.day_end_utc,
        state=record.state,
        timezone=record.timezone,
        zone_flagged=zone_flagged(conn, record.site_id, record.local_date),
        zone_changeover=record.zone_changeover,
        missed_reason=record.missed_reason,
    )


def _first_overlapping(
    conn: sqlite3.Connection, site_id: int, start: datetime, end: datetime
) -> DayRecord | None:
    """The oldest record whose bounds overlap ``[start, end)``, or ``None``.

    The record that holds ``start``, when one does. Otherwise the range
    starts before the site's first record or after its last one, and P1 has
    no public read of a site's first record, so this finds it among the
    records P1 does return. P1's records follow one another without a gap,
    one date apart, so a record that overlaps the range without holding
    ``start`` lies wholly after it. When a record holds the range's last
    instant, the walk goes down by date from it and reads only records in
    the range. When no record holds either end, every record that overlaps
    lies inside the range, and the dates are read one by one from the day
    before ``start``'s UTC date to the day after ``end``'s: a day's local
    date is within a day of the UTC date of its start.
    """
    held = day_for_instant(conn, site_id, isoformat_utc(start))
    if held is not None:
        return held
    last = day_for_instant(conn, site_id, isoformat_utc(end - _TICK))
    if last is not None:
        while True:
            earlier = day_record(conn, site_id, last.local_date - _ONE_DAY)
            if earlier is None or earlier.day_end_utc <= start:
                return last
            last = earlier
    local_date = start.date() - _ONE_DAY
    while local_date <= end.date() + _ONE_DAY:
        record = day_record(conn, site_id, local_date)
        if record is not None and record.day_end_utc > start:
            return record if record.day_start_utc < end else None
        local_date += _ONE_DAY
    return None


def day_spans(
    conn: sqlite3.Connection, site_id: int, start_utc: datetime, end_utc: datetime
) -> list[DaySpan]:
    """The site's day records whose bounds overlap ``[start_utc, end_utc)``.

    One ``DaySpan`` per record, oldest first (plan §9.1). The first is the
    record that holds ``start_utc`` (P1's ``day_for_instant``) or, when the
    range starts before the site's first record, that first record; the
    walk then reads each next date with ``day_record`` until a day starts at
    or after ``end_utc``. Instants before the site's first record, the
    starting history, give no span. Records are read as they are, whatever
    the switch: a caller asks ``daily_close_on`` itself. Empty when
    ``end_utc`` is not after ``start_utc``. Read-only.
    """
    start = _as_utc(start_utc)
    end = _as_utc(end_utc)
    spans: list[DaySpan] = []
    if end <= start:
        return spans
    record = _first_overlapping(conn, site_id, start, end)
    while record is not None and record.day_start_utc < end:
        spans.append(_span(conn, record))
        record = day_record(conn, site_id, record.local_date + _ONE_DAY)
    return spans


def _undone_spans(
    conn: sqlite3.Connection, site_id: int, now: datetime
) -> list[DaySpan]:
    """The site's days that have started by ``now`` and are not done (plan §8.1).

    Days become done in date order, so these follow ``closed_through``: from
    the end of that day to ``now``, read with ``day_spans``. While no day is
    done they run from the site's first record, found by walking down by
    date from the record that holds ``now``; when no record holds ``now``,
    the first record has not started and there is none.
    """
    through = closed_through(conn, site_id)
    closed = None if through is None else day_record(conn, site_id, through)
    if closed is not None:
        start = closed.day_end_utc
    else:
        first = current_day(conn, site_id, now_utc=now)
        if first is None:
            return []
        earlier = day_record(conn, site_id, first.local_date - _ONE_DAY)
        while earlier is not None:
            first = earlier
            earlier = day_record(conn, site_id, first.local_date - _ONE_DAY)
        start = first.day_start_utc
    return [
        span
        for span in day_spans(conn, site_id, start, now)
        if span.state not in DONE_STATES
    ]


def _stored_before(
    conn: sqlite3.Connection, station_id: int, variable: str, hour: datetime
) -> float | None:
    """The station's latest stored value of ``variable`` before ``hour``.

    The same read the close's jump check makes before it stores a station
    value (``write_station_observation``), whatever that row's flag;
    ``None`` when there is no earlier row.
    """
    row = conn.execute(
        "SELECT value FROM station_observations"
        " WHERE station_id = ? AND variable = ? AND valid_at < ?"
        " ORDER BY valid_at DESC LIMIT 1",
        (station_id, variable, isoformat_utc(hour)),
    ).fetchone()
    return None if row is None else float(row[0])


def _dem_elevation(conn: sqlite3.Connection, station_id: int) -> float | None:
    """The station's ``dem_elevation_m``; ``None`` when it has no row."""
    row = conn.execute(
        "SELECT dem_elevation_m FROM stations WHERE id = ?", (station_id,)
    ).fetchone()
    return None if row is None else float(row[0])


def _ok_readings(
    conn: sqlite3.Connection,
    day: SiteDayValues,
    quantity: Quantity,
    hour: datetime,
    previous: dict[int, float | None],
    elevations: dict[int, float | None],
    counted: frozenset[int] | None,
) -> list[ConsensusReading]:
    """The hour's station values that pass the close's jump check (plan §8.3).

    Each listed station's value of the hour is checked with ``qc_flag``
    against ``previous[station]``: the station's previous value on the line,
    or, for its first value on the line, its latest stored value of the
    variable before the hour. The value then becomes the station's previous
    value whatever its flag, as a stored row does for the close. Only ``ok``
    values are kept, each with its station's ``dem_elevation_m``, as
    ``materialize_consensus`` builds its input; a station with no
    ``stations`` row is left out, as that function's join leaves it out.
    When ``counted`` is given (rain), a station outside it is left out too:
    the rain check set it aside for the day, as the close leaves it out.
    """
    readings: list[ConsensusReading] = []
    for station_id in sorted(day.stations):
        hour_value = day.stations[station_id].values[quantity].get(hour)
        if hour_value is None:
            continue
        if station_id not in previous:
            previous[station_id] = _stored_before(conn, station_id, quantity, hour)
        flag = qc_flag(quantity, hour_value.value, previous[station_id])
        previous[station_id] = hour_value.value
        if station_id not in elevations:
            elevations[station_id] = _dem_elevation(conn, station_id)
        dem_elevation_m = elevations[station_id]
        if flag != "ok" or dem_elevation_m is None:
            continue
        if counted is not None and station_id not in counted:
            continue
        readings.append(
            ConsensusReading(
                station_id=station_id,
                dem_elevation_m=dem_elevation_m,
                variable=quantity,
                valid_at=isoformat_utc(hour),
                value=hour_value.value,
            )
        )
    return readings


def provisional_site_hours(
    conn: sqlite3.Connection, *, site_id: int, variable: str, now_utc: datetime
) -> ProvisionalLine:
    """Section 8's provisional line for one site and variable (plan §8, §9.1).

    Empty with the close off (``daily_close_on``), for a variable other than
    ``temperature``, ``precip`` or ``wind``, and for a site that does not
    exist. Otherwise it covers the site's days that have started and are not
    done (``open``, ``closing`` or ``retry_due``), oldest first. Per day,
    ``hourly_values`` gives each listed station's values: for the day that
    holds ``now``, only the hours before ``now`` rounded down to the whole
    UTC hour; for an earlier day, every hour. Each hour's station values go
    through the close's jump check and, the ``ok`` ones, to
    ``compute_consensus`` with the stations' and the site's elevations and
    ``MAD_FLOORS``; an hour with no consensus has no value. Rain reads only
    the day's rain-counted stations (``day_rain_station_ids``), as the close
    does. A station's previous value carries across the days drawn.
    Read-only.
    """
    now = _as_utc(now_utc)
    quantity = _QUANTITY_BY_VARIABLE.get(variable)
    if quantity is None or not daily_close_on(conn):
        return _empty_line()
    site = conn.execute(
        "SELECT elevation_m FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    if site is None:
        return _empty_line()
    site_elevation_m = float(site[0])
    mad_floor = MAD_FLOORS.get(quantity, 0.0)
    drawn: list[datetime] = []
    values: dict[datetime, float] = {}
    stamps: list[datetime] = []
    previous: dict[int, float | None] = {}
    elevations: dict[int, float | None] = {}
    for span in _undone_spans(conn, site_id, now):
        through = None if span.end_utc <= now else floor_hour(now)
        day = hourly_values(
            conn, site_id=site_id, local_date=span.local_date, through_utc=through
        )
        counted = (
            day_rain_station_ids(conn, site_id, span.local_date)
            if quantity == "precip"
            else None
        )
        stamps.extend(
            station.latest_reading_at
            for station in day.stations.values()
            if station.latest_reading_at is not None
        )
        for hour in day_hours(span.start_utc, span.end_utc):
            if through is not None and hour >= through:
                break
            drawn.append(hour)
            readings = _ok_readings(
                conn, day, quantity, hour, previous, elevations, counted
            )
            result = compute_consensus(
                readings,
                variable=quantity,
                site_elevation_m=site_elevation_m,
                mad_floor=mad_floor,
            )
            if result is not None:
                values[hour] = result.value
    return ProvisionalLine(
        valid_at=tuple(drawn),
        values=values,
        latest_reading_at=max(stamps, default=None),
    )
