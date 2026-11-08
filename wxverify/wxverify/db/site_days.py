"""Site-day state records, their settings, and the closed-day guard.

One ``site_days`` row per site and local date records the state of that
day's close (``open`` -> ``closing`` -> ``retry_due``/``closed``/``missed``),
its exact UTC bounds and zone, fixed when the row is made, and a saved
station list (``site_day_stations``). The guard functions answer, inside a
writer's own transaction, whether a write of a given kind may touch the day
an instant or date belongs to.

Every function runs in the caller's transaction and never commits. The clock
is read only through the ``now_utc`` arguments. Instants are compared as
parsed instants (or ``julianday`` on both sides), never as text. Days map to
instants through their stored bounds only; no past day's bounds are
recomputed from a zone name.

This module imports only the standard library, ``wxverify.core.timeutil``
and ``wxverify.db.runtime_state``.
"""

from __future__ import annotations

import bisect
import logging
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify.core.timeutil import (
    isoformat_utc,
    isoformat_utc_micro,
    local_day_slots,
    parse_utc,
)
from wxverify.db.runtime_state import get_runtime_state

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Types and constants
# ---------------------------------------------------------------------------

DayState = Literal["open", "closing", "retry_due", "closed", "missed"]
DONE_STATES: Final = frozenset({"closed", "missed"})
MissedReason = Literal[
    "never_attempted", "retry_failed", "retry_not_run", "zone_changeover"
]
StationChange = Literal["added", "turned_on", "turned_off", "retired"]


class WriteKind(StrEnum):
    """The kind of a guarded day write."""

    READING = "reading"
    GAP_FILL = "gap_fill"
    HOURLY = "hourly"
    TRUTH = "truth"
    DAY_SCORES = "day_scores"
    CLOSE_RECORD = "close_record"
    RAW_PRUNE = "raw_prune"


# Value: the switch time, a canonical UTC ``...Z`` stamp.
DAILY_CLOSE_ON_KEY: Final = "daily_close_on_at"
#: What every retired control answers with (switch-over plan 7.3).
RETIRED_CONTROL_MESSAGE: Final = (
    "This control was retired when the daily close turned on. "
    "Each day is collected and closed once after it ends, "
    "and past days are not fetched again."
)
RETRY_DELAY: Final = timedelta(hours=1)
RAW_KEEP: Final = timedelta(days=7)


class DayStateError(Exception):
    """A day-state move or record creation that is not allowed."""


@dataclass(frozen=True, slots=True)
class DayRecord:
    """One ``site_days`` row, with its stamps parsed."""

    site_id: int
    local_date: date
    state: DayState
    day_start_utc: datetime
    day_end_utc: datetime
    timezone: str
    zone_changeover: bool
    attempts: int  # 0, 1 or 2
    retry_at: datetime | None
    last_failure: str | None
    missed_reason: MissedReason | None
    done_at: str | None


_DAY_STATES: Final[tuple[DayState, ...]] = (
    "open",
    "closing",
    "retry_due",
    "closed",
    "missed",
)
_MISSED_REASONS: Final[tuple[MissedReason, ...]] = (
    "never_attempted",
    "retry_failed",
    "retry_not_run",
    "zone_changeover",
)
_STATION_CHANGES: Final[tuple[StationChange, ...]] = (
    "added",
    "turned_on",
    "turned_off",
    "retired",
)
_JOINING_CHANGES: Final[frozenset[str]] = frozenset({"added", "turned_on"})

# Table 5.2: the kinds the guard allows in each state that is not done.
# ``retry_due`` allows nothing; a done day allows only RAW_PRUNE, by the 5.4
# rule.
_STATE_KINDS: Final[dict[str, frozenset[WriteKind]]] = {
    "open": frozenset({WriteKind.READING, WriteKind.HOURLY, WriteKind.TRUTH}),
    "closing": frozenset(
        {
            WriteKind.GAP_FILL,
            WriteKind.HOURLY,
            WriteKind.TRUTH,
            WriteKind.DAY_SCORES,
            WriteKind.CLOSE_RECORD,
        }
    ),
}

_ONE_DAY: Final = timedelta(days=1)
_MAX_CHANGEOVER: Final = timedelta(hours=48)
# One second in Julian days: a wide margin around the SQL prefetch of
# records by bounds; the exact test is done in Python on parsed instants.
_JULIAN_MARGIN: Final = 1.0 / 86400.0

_SELECT_DAY: Final = (
    "SELECT site_id, local_date, state, day_start_utc, day_end_utc, timezone,"
    " zone_changeover, attempts, retry_at, last_failure, missed_reason, done_at"
    " FROM site_days"
)

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _as_utc(value: datetime) -> datetime:
    """Return ``value`` as an aware UTC datetime (a naive value is UTC)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _as_state(value: object) -> DayState:
    for state in _DAY_STATES:
        if value == state:
            return state
    raise ValueError(f"unknown day state {value!r}")


def _as_missed_reason(value: object) -> MissedReason | None:
    if value is None:
        return None
    for reason in _MISSED_REASONS:
        if value == reason:
            return reason
    raise ValueError(f"unknown missed reason {value!r}")


def _record_from_row(row: Any) -> DayRecord:
    retry_at = row[8]
    last_failure = row[9]
    done_at = row[11]
    return DayRecord(
        site_id=int(row[0]),
        local_date=date.fromisoformat(str(row[1])),
        state=_as_state(row[2]),
        day_start_utc=parse_utc(str(row[3])),
        day_end_utc=parse_utc(str(row[4])),
        timezone=str(row[5]),
        zone_changeover=bool(row[6]),
        attempts=int(row[7]),
        retry_at=None if retry_at is None else parse_utc(str(retry_at)),
        last_failure=None if last_failure is None else str(last_failure),
        missed_reason=_as_missed_reason(row[10]),
        done_at=None if done_at is None else str(done_at),
    )


def _site_timezone(conn: sqlite3.Connection, site_id: int) -> str | None:
    row = conn.execute("SELECT timezone FROM sites WHERE id = ?", (site_id,)).fetchone()
    return None if row is None else str(row[0])


def _require_site_timezone(conn: sqlite3.Connection, site_id: int) -> str:
    site_tz = _site_timezone(conn, site_id)
    if site_tz is None:
        raise DayStateError(f"site {site_id} does not exist")
    return site_tz


def _latest_record(conn: sqlite3.Connection, site_id: int) -> DayRecord | None:
    row = conn.execute(
        f"{_SELECT_DAY} WHERE site_id = ? ORDER BY local_date DESC LIMIT 1",
        (site_id,),
    ).fetchone()
    return None if row is None else _record_from_row(row)


def _records_between(
    conn: sqlite3.Connection, site_id: int, low: datetime, high: datetime
) -> list[DayRecord]:
    """Records of the site that may hold an instant in ``[low, high]``.

    The SQL prefetch is widened by one second on each side; callers decide
    containment exactly on the parsed bounds. Sorted by ``day_start_utc``.
    """
    rows = conn.execute(
        f"{_SELECT_DAY} WHERE site_id = ?"
        " AND julianday(day_start_utc) <= julianday(?) + ?"
        " AND julianday(?) - ? < julianday(day_end_utc)"
        " ORDER BY local_date",
        (
            site_id,
            isoformat_utc(high),
            _JULIAN_MARGIN,
            isoformat_utc(low),
            _JULIAN_MARGIN,
        ),
    ).fetchall()
    records = [_record_from_row(row) for row in rows]
    records.sort(key=lambda record: record.day_start_utc)
    return records


def _holding_record(
    conn: sqlite3.Connection, site_id: int, instant: datetime
) -> DayRecord | None:
    """The record whose stored bounds hold ``instant``, if any."""
    for record in _records_between(conn, site_id, instant, instant):
        if record.day_start_utc <= instant < record.day_end_utc:
            return record
    return None


def _current_local_date(
    conn: sqlite3.Connection, site_id: int, now: datetime, site_tz: str
) -> date:
    """The site's current local date (4.6).

    The date of the record that holds ``now``; when none does, ``now``'s
    date in ``sites.timezone``.
    """
    holder = _holding_record(conn, site_id, now)
    if holder is not None:
        return holder.local_date
    return now.astimezone(ZoneInfo(site_tz)).date()


def _has_earlier_undone(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> bool:
    row = conn.execute(
        "SELECT 1 FROM site_days WHERE site_id = ? AND local_date < ?"
        " AND state NOT IN ('closed','missed') LIMIT 1",
        (site_id, local_date.isoformat()),
    ).fetchone()
    return row is not None


def _validate_zone(name: str) -> None:
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, OSError) as exc:
        raise ValueError(f"unknown time zone {name!r}") from exc


# ---------------------------------------------------------------------------
# Settings per day (6.1, 6.2)
# ---------------------------------------------------------------------------


def _zone_for_date(
    conn: sqlite3.Connection, site_id: int, local_date: date, site_tz: str
) -> str:
    """The zone a new record dated ``local_date`` runs under (6.1).

    The zone of the latest change with ``effective_date <= local_date``,
    latest by ``entered_at`` (parsed), then by ``id``; else
    ``sites.timezone``.
    """
    rows = conn.execute(
        "SELECT id, timezone, entered_at FROM site_zone_changes"
        " WHERE site_id = ? AND effective_date <= ?",
        (site_id, local_date.isoformat()),
    ).fetchall()
    best: tuple[datetime, int] | None = None
    zone = site_tz
    for row in rows:
        key = (parse_utc(str(row[2])), int(row[0]))
        if best is None or key > best:
            best = key
            zone = str(row[1])
    return zone


def _station_list(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> list[int]:
    """The list rule (6.2): stations in the day's list, sorted by id.

    For each station, among its changes with ``effective_date <= local_date``,
    the one with the latest ``changed_at`` (parsed), then the highest ``id``,
    decides; ``added`` or ``turned_on`` puts the station in the list.
    """
    rows = conn.execute(
        "SELECT id, station_id, change, changed_at FROM station_changes"
        " WHERE site_id = ? AND effective_date <= ?",
        (site_id, local_date.isoformat()),
    ).fetchall()
    latest: dict[int, tuple[datetime, int, str]] = {}
    for row in rows:
        station_id = int(row[1])
        candidate = (parse_utc(str(row[3])), int(row[0]), str(row[2]))
        held = latest.get(station_id)
        if held is None or candidate[:2] > held[:2]:
            latest[station_id] = candidate
    return sorted(
        station_id
        for station_id, (_, _, change) in latest.items()
        if change in _JOINING_CHANGES
    )


def _write_station_list(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> None:
    """Write the day's list rows (6.2) with each station's rain flag.

    ``rain_counted`` is the ``counted_after`` of the station's latest
    ``station_rain_checks`` row dated before the day, or 1 when there is
    none (rain release, Part A).
    """
    day = local_date.isoformat()
    conn.executemany(
        "INSERT OR IGNORE INTO site_day_stations"
        " (site_id, local_date, station_id, rain_counted)"
        " VALUES (?, ?, ?, COALESCE((SELECT c.counted_after"
        " FROM station_rain_checks AS c"
        " WHERE c.site_id = ? AND c.station_id = ? AND c.local_date < ?"
        " ORDER BY c.local_date DESC LIMIT 1), 1))",
        [
            (site_id, day, station_id, site_id, station_id, day)
            for station_id in _station_list(conn, site_id, local_date)
        ],
    )


def _insert_record(
    conn: sqlite3.Connection,
    site_id: int,
    local_date: date,
    *,
    start: datetime,
    previous: DayRecord | None,
    site_tz: str,
    now: datetime,
) -> None:
    """Create one ``open`` record with its zone, bounds, flag and list (6.1)."""
    zone = _zone_for_date(conn, site_id, local_date, site_tz)
    midnight, end, _ = local_day_slots(local_date, zone)
    if end <= start:
        raise DayStateError(
            f"site {site_id} day {local_date.isoformat()} would end before it starts"
        )
    changeover = (
        previous is not None
        and zone != previous.timezone
        and previous.day_end_utc != midnight
    )
    stamp = isoformat_utc_micro(now)
    cur = conn.execute(
        "INSERT OR IGNORE INTO site_days"
        " (site_id, local_date, state, day_start_utc, day_end_utc, timezone,"
        " zone_changeover, attempts, created_at, state_changed_at)"
        " VALUES (?, ?, 'open', ?, ?, ?, ?, 0, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            isoformat_utc(start),
            isoformat_utc(end),
            zone,
            1 if changeover else 0,
            stamp,
            stamp,
        ),
    )
    if cur.rowcount == 1:
        _write_station_list(conn, site_id, local_date)
    elif day_record(conn, site_id, local_date) is None:
        raise DayStateError(
            f"site {site_id} day {local_date.isoformat()} was not stored"
        )


def _fill(conn: sqlite3.Connection, site_id: int, now: datetime, site_tz: str) -> None:
    """Add every missing date after the latest record through the next date."""
    while True:
        latest = _latest_record(conn, site_id)
        if latest is None:
            return
        target = _current_local_date(conn, site_id, now, site_tz) + _ONE_DAY
        if latest.local_date >= target:
            return
        _insert_record(
            conn,
            site_id,
            latest.local_date + _ONE_DAY,
            start=latest.day_end_utc,
            previous=latest,
            site_tz=site_tz,
            now=now,
        )


def _start(
    conn: sqlite3.Connection,
    site_id: int,
    first_date: date,
    *,
    now: datetime,
    site_tz: str,
) -> None:
    zone = _zone_for_date(conn, site_id, first_date, site_tz)
    start = local_day_slots(first_date, zone)[0]
    _insert_record(
        conn,
        site_id,
        first_date,
        start=start,
        previous=None,
        site_tz=site_tz,
        now=now,
    )
    _fill(conn, site_id, now, site_tz)


def _ensure(conn: sqlite3.Connection, site_id: int, now: datetime) -> None:
    """``ensure_site_days`` on a UTC ``now``; a missing site is a no-op."""
    site_tz = _site_timezone(conn, site_id)
    if site_tz is None:
        return
    if _latest_record(conn, site_id) is None:
        next_date = _current_local_date(conn, site_id, now, site_tz) + _ONE_DAY
        _start(conn, site_id, next_date, now=now, site_tz=site_tz)
        return
    _fill(conn, site_id, now, site_tz)


# ---------------------------------------------------------------------------
# The switch time (11.1)
# ---------------------------------------------------------------------------


def daily_close_on_at(conn: sqlite3.Connection) -> datetime:
    """The switch time, parsed from ``DAILY_CLOSE_ON_KEY``.

    Raises ``ValueError`` when the key is missing or its stored value does
    not parse. Read-only.
    """
    value = get_runtime_state(conn, DAILY_CLOSE_ON_KEY)
    if value is None:
        raise ValueError("daily_close_on_at is missing")
    return parse_utc(value)


# ---------------------------------------------------------------------------
# Record creation (4.6)
# ---------------------------------------------------------------------------


def ensure_site_days(
    conn: sqlite3.Connection, site_id: int, *, now_utc: datetime
) -> None:
    """Create the site's missing records through its next local date (4.6).

    Idempotent. For a site with no records, starts it at its next local
    date. Does nothing for a site that does not exist.
    """
    _ensure(conn, site_id, _as_utc(now_utc))


def start_site_days(
    conn: sqlite3.Connection, site_id: int, first_date: date, *, now_utc: datetime
) -> None:
    """Create the site's first record, dated ``first_date``, then fill (4.6).

    Raises ``DayStateError`` when the site does not exist, already has a
    record, or ``first_date`` is before the site's current local date.
    """
    now = _as_utc(now_utc)
    site_tz = _require_site_timezone(conn, site_id)
    if _latest_record(conn, site_id) is not None:
        raise DayStateError(f"site {site_id} already has day records")
    current = _current_local_date(conn, site_id, now, site_tz)
    if first_date < current:
        raise DayStateError(
            f"site {site_id}: first day {first_date.isoformat()} is before the"
            f" current local date {current.isoformat()}"
        )
    _start(conn, site_id, first_date, now=now, site_tz=site_tz)


# ---------------------------------------------------------------------------
# Reads (12.2)
# ---------------------------------------------------------------------------


def day_record(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> DayRecord | None:
    """The site's record dated ``local_date``, or ``None``. Read-only."""
    row = conn.execute(
        f"{_SELECT_DAY} WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    return None if row is None else _record_from_row(row)


def day_for_instant(
    conn: sqlite3.Connection, site_id: int, instant_utc: str
) -> DayRecord | None:
    """The record whose stored bounds hold ``instant_utc``. Read-only.

    ``None`` when no record holds it, or when it does not parse.
    """
    try:
        instant = parse_utc(instant_utc)
    except ValueError:
        return None
    return _holding_record(conn, site_id, instant)


def current_day(
    conn: sqlite3.Connection, site_id: int, *, now_utc: datetime
) -> DayRecord | None:
    """The record that holds ``now_utc``, or ``None``. Read-only."""
    return _holding_record(conn, site_id, _as_utc(now_utc))


def day_station_ids(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> frozenset[int]:
    """The day's saved station list (6.2). Read-only."""
    rows = conn.execute(
        "SELECT station_id FROM site_day_stations WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchall()
    return frozenset(int(row[0]) for row in rows)


def day_rain_station_ids(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> frozenset[int]:
    """The listed stations whose rain the day counts (rain release, A1).

    The day's saved list without the stations the rain check set aside
    (``rain_counted = 0``). Read-only.
    """
    rows = conn.execute(
        "SELECT station_id FROM site_day_stations"
        " WHERE site_id = ? AND local_date = ? AND rain_counted = 1",
        (site_id, local_date.isoformat()),
    ).fetchall()
    return frozenset(int(row[0]) for row in rows)


def set_rain_counted_after(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    local_date: date,
    *,
    counted: int,
) -> None:
    """Set the station's rain flag on its list rows dated after ``local_date``.

    The rain check's step 3 (rain release, A4): the day's own row is left
    as it is, so the change takes effect from the next day.
    """
    conn.execute(
        "UPDATE site_day_stations SET rain_counted = ?"
        " WHERE site_id = ? AND station_id = ? AND local_date > ?",
        (1 if counted else 0, site_id, station_id, local_date.isoformat()),
    )


def local_clock_utc(record: DayRecord, hour: int, minute: int) -> datetime:
    """The UTC instant of a local wall time on the record's date and zone.

    A wall time that occurs twice takes the earlier instant; one that does
    not occur (the clocks go forward) takes the instant the clocks jump.
    Pure. Raises ``ValueError`` when the result lies outside the record's
    bounds, or for an invalid hour or minute.
    """
    tz = ZoneInfo(record.timezone)
    day = record.local_date
    wall = datetime(day.year, day.month, day.day, hour, minute)
    first = wall.replace(tzinfo=tz, fold=0).astimezone(UTC)
    result = first
    if first.astimezone(tz).replace(tzinfo=None) != wall:
        # In a gap fold=0 maps after the jump and fold=1 before it. Find the
        # first whole second whose local wall time reaches ``wall``.
        before = wall.replace(tzinfo=tz, fold=1).astimezone(UTC)
        low = 0
        high = int((first - before).total_seconds())
        while high - low > 1:
            middle = (low + high) // 2
            probe = before + timedelta(seconds=middle)
            if probe.astimezone(tz).replace(tzinfo=None) >= wall:
                high = middle
            else:
                low = middle
        result = before + timedelta(seconds=high)
    if not record.day_start_utc <= result < record.day_end_utc:
        raise ValueError(
            f"{hour:02d}:{minute:02d} on {day.isoformat()} lies outside the day"
        )
    return result


def closed_through(conn: sqlite3.Connection, site_id: int) -> date | None:
    """The latest date such that it and every earlier record are done (4.5).

    ``None`` when the site's first record is not done. Read-only.
    """
    row = conn.execute(
        "SELECT MAX(local_date) FROM site_days WHERE site_id = ? AND local_date <"
        " COALESCE((SELECT MIN(local_date) FROM site_days WHERE site_id = ?"
        " AND state NOT IN ('closed','missed')), '9999-12-31')",
        (site_id, site_id),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return date.fromisoformat(str(row[0]))


def zone_flagged(conn: sqlite3.Connection, site_id: int, local_date: date) -> bool:
    """Whether the day's zone differs from ``sites.timezone`` (6.5).

    ``False`` when the day has no record. Read-only.
    """
    row = conn.execute(
        "SELECT site_days.timezone, sites.timezone FROM site_days"
        " JOIN sites ON sites.id = site_days.site_id"
        " WHERE site_days.site_id = ? AND site_days.local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    return False if row is None else str(row[0]) != str(row[1])


# ---------------------------------------------------------------------------
# Transitions (4.7)
# ---------------------------------------------------------------------------


def _require_record(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> DayRecord:
    record = day_record(conn, site_id, local_date)
    if record is None:
        raise DayStateError(
            f"site {site_id} has no record for {local_date.isoformat()}"
        )
    return record


def _move(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[object, ...],
    record: DayRecord,
) -> None:
    cur = conn.execute(sql, params)
    if cur.rowcount != 1:
        raise DayStateError(
            f"site {record.site_id} day {record.local_date.isoformat()}"
            " changed state during the move"
        )


def _refuse(record: DayRecord, why: str) -> DayStateError:
    return DayStateError(
        f"site {record.site_id} day {record.local_date.isoformat()}"
        f" ({record.state}): {why}"
    )


def begin_close(
    conn: sqlite3.Connection, site_id: int, local_date: date, *, now_utc: datetime
) -> int:
    """Start a close attempt; returns the attempt now running, 1 or 2 (4.7).

    ``open`` -> ``closing`` once the day has ended, every earlier record is
    done and the day is not a changeover day; ``retry_due`` -> ``closing``
    once ``retry_at`` has come. Raises ``DayStateError`` otherwise.
    """
    now = _as_utc(now_utc)
    record = _require_record(conn, site_id, local_date)
    stamp = isoformat_utc_micro(now)
    key = (site_id, local_date.isoformat())
    if record.state == "open":
        if now < record.day_end_utc:
            raise _refuse(record, "the day has not ended")
        if record.zone_changeover:
            raise _refuse(record, "a changeover day is never closed")
        if _has_earlier_undone(conn, site_id, local_date):
            raise _refuse(record, "an earlier day is not done")
        _move(
            conn,
            "UPDATE site_days SET state = 'closing', attempts = 1,"
            " state_changed_at = ? WHERE site_id = ? AND local_date = ?"
            " AND state = 'open'",
            (stamp, *key),
            record,
        )
        return 1
    if record.state == "retry_due":
        if record.retry_at is None or now < record.retry_at:
            raise _refuse(record, "the retry is not due")
        _move(
            conn,
            "UPDATE site_days SET state = 'closing', attempts = 2,"
            " state_changed_at = ? WHERE site_id = ? AND local_date = ?"
            " AND state = 'retry_due'",
            (stamp, *key),
            record,
        )
        return 2
    raise _refuse(record, "no close attempt can start")


def fail_close(
    conn: sqlite3.Connection,
    site_id: int,
    local_date: date,
    *,
    now_utc: datetime,
    reason: str,
) -> DayState:
    """Record a failed close attempt; returns the new state (4.7).

    After attempt 1 the day is ``retry_due`` with ``retry_at`` one hour on;
    after attempt 2 it is ``missed`` (``retry_failed``). ``reason`` is
    stored as ``last_failure``. Raises ``DayStateError`` unless ``closing``.
    """
    now = _as_utc(now_utc)
    record = _require_record(conn, site_id, local_date)
    if record.state != "closing":
        raise _refuse(record, "only a closing day can fail")
    stamp = isoformat_utc_micro(now)
    key = (site_id, local_date.isoformat())
    if record.attempts == 1:
        _move(
            conn,
            "UPDATE site_days SET state = 'retry_due', retry_at = ?,"
            " last_failure = ?, state_changed_at = ?"
            " WHERE site_id = ? AND local_date = ? AND state = 'closing'"
            " AND attempts = 1",
            (isoformat_utc(now + RETRY_DELAY), reason, stamp, *key),
            record,
        )
        return "retry_due"
    if record.attempts == 2:
        _move(
            conn,
            "UPDATE site_days SET state = 'missed', missed_reason = 'retry_failed',"
            " last_failure = ?, done_at = ?, state_changed_at = ?"
            " WHERE site_id = ? AND local_date = ? AND state = 'closing'"
            " AND attempts = 2",
            (reason, stamp, stamp, *key),
            record,
        )
        return "missed"
    raise _refuse(record, f"unexpected attempt count {record.attempts}")


def finish_close(
    conn: sqlite3.Connection, site_id: int, local_date: date, *, now_utc: datetime
) -> None:
    """Move a ``closing`` day to ``closed`` (4.7).

    Raises ``DayStateError`` unless the day is ``closing``, or when a
    ``daily_truth`` row of the day is marked stale.
    """
    now = _as_utc(now_utc)
    record = _require_record(conn, site_id, local_date)
    if record.state != "closing":
        raise _refuse(record, "only a closing day can be closed")
    stale = conn.execute(
        "SELECT 1 FROM daily_truth WHERE site_id = ? AND local_date = ?"
        " AND stale = 1 LIMIT 1",
        (site_id, local_date.isoformat()),
    ).fetchone()
    if stale is not None:
        raise _refuse(record, "the day's truth is stale")
    stamp = isoformat_utc_micro(now)
    _move(
        conn,
        "UPDATE site_days SET state = 'closed', done_at = ?, state_changed_at = ?"
        " WHERE site_id = ? AND local_date = ? AND state = 'closing'",
        (stamp, stamp, site_id, local_date.isoformat()),
        record,
    )


def mark_missed(
    conn: sqlite3.Connection,
    site_id: int,
    local_date: date,
    *,
    now_utc: datetime,
    reason: MissedReason,
) -> None:
    """Move a day to ``missed`` with ``reason`` (4.7).

    ``open`` takes ``never_attempted`` (not a changeover day) or
    ``zone_changeover`` (a changeover day), once the day has ended and every
    earlier record is done. ``retry_due`` takes ``retry_not_run`` once
    ``retry_at`` has come. Raises ``DayStateError`` otherwise.
    """
    now = _as_utc(now_utc)
    record = _require_record(conn, site_id, local_date)
    stamp = isoformat_utc_micro(now)
    key = (site_id, local_date.isoformat())
    if record.state == "open":
        if reason == "never_attempted":
            if record.zone_changeover:
                raise _refuse(record, "a changeover day is missed as zone_changeover")
        elif reason == "zone_changeover":
            if not record.zone_changeover:
                raise _refuse(record, "not a changeover day")
        else:
            raise _refuse(record, f"reason {reason!r} does not fit an open day")
        if now < record.day_end_utc:
            raise _refuse(record, "the day has not ended")
        if _has_earlier_undone(conn, site_id, local_date):
            raise _refuse(record, "an earlier day is not done")
        _move(
            conn,
            "UPDATE site_days SET state = 'missed', missed_reason = ?,"
            " done_at = ?, state_changed_at = ?"
            " WHERE site_id = ? AND local_date = ? AND state = 'open'",
            (reason, stamp, stamp, *key),
            record,
        )
        return
    if record.state == "retry_due":
        if reason != "retry_not_run":
            raise _refuse(record, f"reason {reason!r} does not fit a retry_due day")
        if record.retry_at is None or now < record.retry_at:
            raise _refuse(record, "the retry is not due")
        _move(
            conn,
            "UPDATE site_days SET state = 'missed', missed_reason = ?,"
            " done_at = ?, state_changed_at = ?"
            " WHERE site_id = ? AND local_date = ? AND state = 'retry_due'",
            (reason, stamp, stamp, *key),
            record,
        )
        return
    raise _refuse(record, "this day cannot be moved to missed")


# ---------------------------------------------------------------------------
# The guard (7.1)
# ---------------------------------------------------------------------------


def _log_declined(
    site_id: int, kind: WriteKind, days: Iterable[date], count: int
) -> None:
    if count and logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "site-day check declined kind=%s site=%s days=%s count=%d",
            str(kind),
            site_id,
            ",".join(sorted({day.isoformat() for day in days})),
            count,
        )


def _raw_prune_due(conn: sqlite3.Connection, record: DayRecord, now: datetime) -> bool:
    """Rule 5.4: the day and the next are done, 7 days after the later done_at."""
    following = day_record(conn, record.site_id, record.local_date + _ONE_DAY)
    if following is None or following.state not in DONE_STATES:
        return False
    if record.done_at is None or following.done_at is None:
        return False
    later = max(parse_utc(record.done_at), parse_utc(following.done_at))
    return now >= later + RAW_KEEP


def _record_allows(
    conn: sqlite3.Connection, record: DayRecord, kind: WriteKind, now: datetime
) -> bool:
    if record.state in DONE_STATES:
        return kind == WriteKind.RAW_PRUNE and _raw_prune_due(conn, record, now)
    return kind in _STATE_KINDS.get(record.state, frozenset[WriteKind]())


def filter_day_writes(
    conn: sqlite3.Connection,
    site_id: int,
    instants: Iterable[str],
    kind: WriteKind,
    *,
    now_utc: datetime,
) -> frozenset[str]:
    """The subset of ``instants`` that may be written as ``kind`` (7.1).

    Creates the site's missing records, then allows an instant only when
    table 5.2 allows ``kind`` for the record whose bounds hold it. An
    instant that does not parse is refused. Each day is decided once. Never
    raises for a refusal.
    """
    items = list(instants)
    now = _as_utc(now_utc)
    _ensure(conn, site_id, now)

    parsed: dict[str, datetime] = {}
    refused = 0
    for item in set(items):
        try:
            parsed[item] = parse_utc(item)
        except ValueError:
            refused += 1
    if not parsed:
        _log_declined(site_id, kind, (), refused)
        return frozenset()

    records = _records_between(
        conn, site_id, min(parsed.values()), max(parsed.values())
    )
    starts = [record.day_start_utc for record in records]
    verdicts: dict[date, bool] = {}
    allowed: set[str] = set()
    refused_days: list[date] = []
    for item, instant in parsed.items():
        index = bisect.bisect_right(starts, instant) - 1
        record = records[index] if index >= 0 else None
        if record is not None and not instant < record.day_end_utc:
            record = None
        if record is None:
            ok = False
        else:
            verdict = verdicts.get(record.local_date)
            if verdict is None:
                verdict = _record_allows(conn, record, kind, now)
                verdicts[record.local_date] = verdict
            ok = verdict
            if not ok:
                refused_days.append(record.local_date)
        if ok:
            allowed.add(item)
        else:
            refused += 1
    _log_declined(site_id, kind, refused_days, refused)
    return frozenset(allowed)


def check_day_write(
    conn: sqlite3.Connection,
    site_id: int,
    instant_utc: str,
    kind: WriteKind,
    *,
    now_utc: datetime,
) -> bool:
    """Whether one write keyed by ``instant_utc`` may go ahead (7.1).

    The single-instant form of :func:`filter_day_writes`. Never raises for a
    refusal.
    """
    return instant_utc in filter_day_writes(
        conn, site_id, (instant_utc,), kind, now_utc=now_utc
    )


def check_date_write(
    conn: sqlite3.Connection,
    site_id: int,
    local_date: date,
    kind: WriteKind,
    *,
    now_utc: datetime,
) -> bool:
    """Whether one write keyed by ``local_date`` may go ahead (7.1).

    Creates the site's missing records, then follows table 5.2 for the
    record dated ``local_date``; ``RAW_PRUNE`` on a done day follows rule
    5.4, and with no record nothing is allowed. Never raises for a refusal.
    """
    now = _as_utc(now_utc)
    _ensure(conn, site_id, now)
    record = day_record(conn, site_id, local_date)
    allowed = False if record is None else _record_allows(conn, record, kind, now)
    if not allowed:
        _log_declined(site_id, kind, (local_date,), 1)
    return allowed


# ---------------------------------------------------------------------------
# Station changes (8.2, 8.5)
# ---------------------------------------------------------------------------


def record_station_change(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    change: StationChange,
    *,
    reason: str,
    now_utc: datetime,
) -> str:
    """Log a station change and return its effective local date (8.2).

    ``added``/``turned_on`` take effect on the site's next local date,
    ``turned_off``/``retired`` on its current one. First creates the site's
    missing records and afterwards recomputes the lists of its ``open``
    records dated on or after the effective date. Raises ``ValueError`` for
    an unknown change and ``DayStateError`` for a site that does not exist.
    """
    if change not in _STATION_CHANGES:
        raise ValueError(f"unknown station change {change!r}")
    now = _as_utc(now_utc)
    site_tz = _require_site_timezone(conn, site_id)
    _ensure(conn, site_id, now)
    current = _current_local_date(conn, site_id, now, site_tz)
    effective = current + _ONE_DAY if change in _JOINING_CHANGES else current
    conn.execute(
        "INSERT INTO station_changes"
        " (site_id, station_id, change, effective_date, reason, changed_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (
            site_id,
            station_id,
            change,
            effective.isoformat(),
            reason,
            isoformat_utc_micro(now),
        ),
    )
    rows = conn.execute(
        "SELECT local_date FROM site_days WHERE site_id = ? AND state = 'open'"
        " AND local_date >= ? ORDER BY local_date",
        (site_id, effective.isoformat()),
    ).fetchall()
    for row in rows:
        day = date.fromisoformat(str(row[0]))
        conn.execute(
            "DELETE FROM site_day_stations WHERE site_id = ? AND local_date = ?",
            (site_id, day.isoformat()),
        )
        _write_station_list(conn, site_id, day)
    return effective.isoformat()


def current_day_has_other_station(
    conn: sqlite3.Connection, site_id: int, station_id: int, *, now_utc: datetime
) -> bool:
    """Whether removing ``station_id`` leaves today's list non-empty (8.5).

    ``True`` when no record holds ``now_utc``, and when the station is not
    in that day's list. Read-only.
    """
    holder = _holding_record(conn, site_id, _as_utc(now_utc))
    if holder is None:
        return True
    listed = day_station_ids(conn, site_id, holder.local_date)
    if station_id not in listed:
        return True
    return len(listed - {station_id}) > 0


# ---------------------------------------------------------------------------
# Zone change (6.4)
# ---------------------------------------------------------------------------


def enter_zone_change(
    conn: sqlite3.Connection, site_id: int, timezone: str, *, now_utc: datetime
) -> date:
    """Enter a zone change from the next full local day; returns C (6.4).

    Replaces the site's records dated C and later (none has started) under
    the new zone and sets ``sites.timezone``. Raises ``ValueError`` for an
    unknown zone, for the zone C already runs under, or when C's span from
    the current day's end would be 0 h or less, or 48 h or more.
    """
    now = _as_utc(now_utc)
    site_tz = _require_site_timezone(conn, site_id)
    _ensure(conn, site_id, now)
    current = _holding_record(conn, site_id, now)
    _validate_zone(timezone)
    if current is not None:
        change_date = current.local_date + _ONE_DAY
    else:
        change_date = now.astimezone(ZoneInfo(timezone)).date() + _ONE_DAY
    if _zone_for_date(conn, site_id, change_date, site_tz) == timezone:
        raise ValueError(
            f"{change_date.isoformat()} already runs under the requested zone"
        )
    change_text = change_date.isoformat()
    if current is not None:
        span = local_day_slots(change_date, timezone)[1] - current.day_end_utc
        if span <= timedelta(0) or span >= _MAX_CHANGEOVER:
            raise ValueError(
                f"the changeover day {change_text} would last {span}; it must be"
                " more than 0 h and less than 48 h"
            )
        conn.execute(
            "DELETE FROM site_day_stations WHERE site_id = ? AND local_date >= ?",
            (site_id, change_text),
        )
        conn.execute(
            "DELETE FROM site_days WHERE site_id = ? AND local_date >= ?",
            (site_id, change_text),
        )
    else:
        for row in conn.execute(
            "SELECT day_start_utc FROM site_days WHERE site_id = ?", (site_id,)
        ).fetchall():
            if parse_utc(str(row[0])) <= now:
                raise DayStateError(
                    f"site {site_id} has a started day but none holds the current time"
                )
        conn.execute("DELETE FROM site_day_stations WHERE site_id = ?", (site_id,))
        conn.execute("DELETE FROM site_days WHERE site_id = ?", (site_id,))
    conn.execute(
        "INSERT INTO site_zone_changes (site_id, timezone, effective_date, entered_at)"
        " VALUES (?, ?, ?, ?)",
        (site_id, timezone, change_text, isoformat_utc_micro(now)),
    )
    conn.execute("UPDATE sites SET timezone = ? WHERE id = ?", (timezone, site_id))
    _ensure(conn, site_id, now)
    return change_date
