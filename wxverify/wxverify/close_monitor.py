"""Read-only health alerts of the daily close (daily-close plan P5, §5.1).

Every query of the close's alerts lives in this module, and none of them
writes. ``wxverify.monitor`` imports this module and turns each
``CloseAlert`` into a pipeline ``Condition``; this module imports nothing
from ``monitor``, so there is no import cycle, and its stamp helper is a copy
of ``monitor._parse_or_none``'s body rather than an import.

Every stamp is parsed before it is compared: comparisons are made on parsed
instants, never on text. ``now`` is passed in by the caller. A value this
module parses that does not read makes its item trip, with the detail
``<item>: unreadable <field>`` in place of the alert's message, so its own
reading never raises; a ``sqlite3.Error`` propagates to the caller, which
reports it as ``db_readable``. Day states are read only through
``wxverify.db.site_days``'s functions, and P2's, P3's and P4's data only
through their public readers; a row that one of those readers cannot
parse raises its ``ValueError``, which the caller reports as
``unexpected_error``.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final
from zoneinfo import ZoneInfoNotFoundError

from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.runtime_state import get_runtime_state
from wxverify.db.site_days import (
    DayRecord,
    closed_through,
    current_day,
    daily_close_on_at,
    day_record,
    day_station_ids,
)
from wxverify.db.station_readings import last_five_minute_store, next_five_minute_try
from wxverify.scoring.publication import Publication, active_publication
from wxverify.settings.keys import get_number_setting
from wxverify.verification.snapshot_time import (
    resolve_snapshot_utc,
    snapshot_wall_clock,
)
from wxverify.worker.access_hold import AccessKind, AccessRecord, refused_kinds
from wxverify.worker.close_chain import (
    CLOSE_LOG_KEEP_DAYS,
    AttemptRecord,
    CloseStatus,
    CollectionTry,
    PublicationRecord,
    close_status,
    collection_tries,
    publication_state,
)
from wxverify.worker.day_values import day_presence

#: Alert 5's silence threshold, in hours (plan §7.3). Provisional until the
#: owner chooses the value from the review of the poll's reading times.
STATION_SILENT_HOURS: Final[int] = 2

#: While True, alert 5's line says "provisional" (plan §7.3).
STATION_SILENT_PROVISIONAL: Final[bool] = True

#: Alerts 1 and 2 show a done day for this many days after its ``done_at``
#: (plan §5.1, "The alert window").
CLOSE_ALERT_DAYS: Final[int] = 7

#: Alert 3 reads a closed day for this many days after its ``done_at``
#: (plan §5.4).
STATION_ALERT_DAYS: Final[int] = 7

#: Alert 3 trips for a station and variable when the station's newest this
#: many counted days all have nothing usable for it (plan §5.4, Q12).
STATION_NOTHING_USABLE_DAYS: Final[int] = 3

#: A closing day whose current attempt has made no progress for more than
#: this many hours has stalled (plan §6.2 (c)).
CLOSE_STALL_HOURS: Final[int] = 6

#: Alert 7's "recovered at" line shows for this many hours after the first
#: answered try of the slot that ended a run of down slots (plan §6.6).
CLOSE_RECOVERED_SHOW_HOURS: Final[int] = 24

#: The close's stages by number, P3 §8.1's stage titles in lower case (plan
#: §5.1, "Stage names"). Every line that names a stage prints it through
#: ``stage_text``.
CLOSE_STAGE_NAMES: Final[Mapping[int, str]] = {
    1: "the round settles",
    2: "gap check and fill",
    3: "hourly values",
    4: "verdict and truth",
    5: "score the day",
    6: "save the closure",
    7: "publish",
    8: "worksheet cleanup",
}

# A detail names at most this many items, then "and N more" (plan §5.1).
_DETAIL_MAX_ITEMS: Final = 5

_STATION_SILENT_ID: Final = "station_silent_today"

# Alert 6's days affected name at most this many days, then "and N more", so
# the line stays near the 300 characters Home Assistant shows (plan §6.4).
_ACCESS_DAYS_MAX_ITEMS: Final = 3

# Alert 6's word for each day state (plan §6.4); ``retry_due`` reads
# "retry due <retry_at>" (``_access_day_item``).
_ACCESS_DAY_STATES: Final[Mapping[str, str]] = {
    "open": "collecting",
    "closing": "closing",
    "closed": "closed",
    "missed": "missed",
}

# Day states whose close still downloads after the day's bounds end, so
# alert 6 counts them as affected by any run that ends after the day starts
# (plan §6.4).
_ACCESS_STILL_CLOSING: Final = frozenset({"closing", "retry_due"})

_ONE_DAY: Final = timedelta(days=1)

# The daytime-download warning trips after this many refreshes of
# ``obs_interval_minutes`` with nothing stored (plan §6.5).
_DAYTIME_MISSED_REFRESHES: Final = 2

# Alert 7's try kinds (plan §6.6): an answered try's outcomes; an outage
# try's reasons, and a ``provider`` try's detail that makes it one.
_ANSWERED_OUTCOMES: Final = frozenset({"stored", "no_readings"})
_OUTAGE_REASONS: Final = frozenset({"network", "backoff"})
_OUTAGE_HTTP: Final = re.compile(r"http (?:429|5[0-9]{2})")

# Alert 2's quantities in ``daily_truth``, in its line's order, with the label
# each prints (plan §5.3).
_TRUTH_LABELS: Final[tuple[tuple[str, str], ...]] = (
    ("temperature_high", "high"),
    ("temperature_low", "low"),
    ("precip_total", "rain total"),
    ("precip_occurrence", "rain or not"),
    ("wind_max", "wind"),
)

# Alert 3's variables in ``station_observations``, in its line's order, with
# the word each prints (plan §5.4).
_STATION_VARIABLES: Final[tuple[tuple[str, str], ...]] = (
    ("temperature", "temperature"),
    ("precip", "rain"),
    ("wind", "wind"),
)


def stage_text(stage: int | None) -> str:
    """A close stage as "stage <n> <name>" (plan §5.1, "Stage names").

    "stage <n>" for a number not in ``CLOSE_STAGE_NAMES``, "stage unknown"
    for None. Never raises.
    """
    if stage is None:
        return "stage unknown"
    name = CLOSE_STAGE_NAMES.get(stage)
    if name is None:
        return f"stage {stage}"
    return f"stage {stage} {name}"


@dataclass(frozen=True)
class CloseAlert:
    """One close alert's result, as ``monitor`` turns it into a ``Condition``.

    ``count`` is the number of items tripped (0 while ok); ``detail`` names
    them in a fixed order, at most five then "and N more", and is None while
    ok, except for alert 7, whose "recovered at <time>" line shows while ok
    for ``CLOSE_RECOVERED_SHOW_HOURS`` from the first answered try of the
    slot that ended the last run of down slots, whatever answered slots
    follow it. ``recovered_at`` is that recovery time, None for every other
    alert and outside that window.
    """

    id: str
    tripped: bool
    count: int
    detail: str | None = None
    recovered_at: datetime | None = None


def _parse_or_none(value: object) -> datetime | None:
    """``parse_utc(str(value))``, or ``None`` for NULL or a value that fails.

    ``str()`` first, so a non-text value such as an imported BLOB gives
    ``None``; ``parse_utc`` raises only ``ValueError``.
    """
    if value is None:
        return None
    try:
        return parse_utc(str(value))
    except ValueError:
        return None


def _items_detail(
    items: list[str], *, limit: int = _DETAIL_MAX_ITEMS, sep: str = "; "
) -> str | None:
    """``items`` joined by ``sep``, at most ``limit``, then "and N more";
    None when there are none."""
    if not items:
        return None
    shown = items[:limit]
    if len(items) > limit:
        shown.append(f"and {len(items) - limit} more")
    return sep.join(shown)


def _age_text(age: timedelta) -> str:
    """``age`` in whole minutes, as "2 h", "2 h 15 min" or "45 min"."""
    minutes = max(0, int(age.total_seconds() // 60))
    hours, rest = divmod(minutes, 60)
    if hours and rest:
        return f"{hours} h {rest} min"
    if hours:
        return f"{hours} h"
    return f"{rest} min"


def _and_join(words: tuple[str, ...]) -> str:
    """``words`` as "a", "a and b" or "a, b and c"."""
    if len(words) < 2:
        return "".join(words)
    return f"{', '.join(words[:-1])} and {words[-1]}"


def _enabled_sites(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    """The enabled sites as (id, name), in the details' order (plan §5.1)."""
    return [
        (int(row[0]), str(row[1]))
        for row in conn.execute(
            "SELECT id, name FROM sites WHERE enabled = 1 ORDER BY name, id"
        )
    ]


def _done_days(
    conn: sqlite3.Connection, site_id: int, now: datetime, days: int
) -> list[tuple[DayRecord, datetime | None]]:
    """The site's done days whose ``done_at`` is within ``days`` of ``now``.

    Newest first, each with its parsed ``done_at`` (None when it does not
    read). Walks ``day_record`` date by date down from ``closed_through``:
    every record up to that date is done and none after it is. A day is
    closed or missed only once every earlier day is done, so ``done_at``
    never falls as the date rises, and the walk stops at the first day done
    more than ``days`` before ``now`` or at the first date with no record
    (before the site's first record, the starting history, which is never
    read). A day whose ``done_at`` does not read is kept, so its item
    trips, and does not stop the walk.
    """
    newest = closed_through(conn, site_id)
    if newest is None:
        return []
    window = timedelta(days=days)
    found: list[tuple[DayRecord, datetime | None]] = []
    local_date = newest
    while True:
        record = day_record(conn, site_id, local_date)
        if record is None:
            break
        done_at = _parse_or_none(record.done_at)
        if done_at is not None and now - done_at > window:
            break
        found.append((record, done_at))
        local_date -= timedelta(days=1)
    return found


def _failure_text(record: DayRecord) -> str:
    """The day's ``last_failure``, or "no reason recorded" when it is NULL."""
    if record.last_failure is None:
        return "no reason recorded"
    return record.last_failure


def _missed_line(
    conn: sqlite3.Connection, item: str, record: DayRecord, now: datetime
) -> str:
    """Alert 1's line for a missed day other than a changeover day (§5.2)."""
    reason = record.missed_reason
    if reason == "never_attempted":
        why = "the close never ran (the add-on was not running when it was due)"
    elif reason == "retry_not_run":
        why = (
            "the retry fell due while the add-on was not running;"
            f" attempt 1 failed: {_failure_text(record)}"
        )
    elif reason == "retry_failed":
        # The retry's stage is P3's blocking stage for the day (R3.4): the
        # failed_stage of the attempt numbered ``attempts``, attempt 2 here.
        status = close_status(
            conn, site_id=record.site_id, local_date=record.local_date, now_utc=now
        )
        stage = None if status is None else status.blocking_stage
        why = (
            f"both attempts failed, the retry at {stage_text(stage)}:"
            f" {_failure_text(record)}"
        )
    else:
        return f"{item}: unreadable missed_reason"
    return f"{item}: missed, {why}"


def _thin_line(conn: sqlite3.Connection, item: str, record: DayRecord) -> str | None:
    """Alert 2's line for a closed day, or None when every value counted.

    A quantity counts when it has a ``daily_truth`` row and every row of it
    is eligible; one with no row reads "(no verdict)". No generation
    filter: the plan reads one row per site, quantity and date (§5.3).
    """
    reasons: dict[str, str | None] = {}
    for row in conn.execute(
        "SELECT quantity, eligible, exclusion_reason FROM daily_truth"
        " WHERE site_id = ? AND local_date = ? ORDER BY id",
        (record.site_id, record.local_date.isoformat()),
    ):
        quantity = str(row[0])
        if row[1] == 1:
            reasons.setdefault(quantity, None)
        elif reasons.get(quantity) is None:
            reasons[quantity] = "no reason recorded" if row[2] is None else str(row[2])
    parts: list[str] = []
    for quantity, label in _TRUTH_LABELS:
        if quantity not in reasons:
            parts.append(f"{label} (no verdict)")
        elif (reason := reasons[quantity]) is not None:
            parts.append(f"{label} ({reason})")
    if not parts:
        return None
    return f"{item}: not counted: {'; '.join(parts)}"


@dataclass(frozen=True)
class _NothingUsable:
    """Alert 3's finding for one station (plan §5.4).

    ``variables`` are the words of the variables it names, in
    ``_STATION_VARIABLES`` order; ``since`` is the oldest counted day of the
    run they share.
    """

    station_id: int
    pws_station_id: str
    variables: tuple[str, ...]
    since: date


def _usable_variables(
    conn: sqlite3.Connection, station_id: int, record: DayRecord
) -> frozenset[str]:
    """The variables the station has a ``qc_flag = 'ok'`` row for in the day.

    "Usable" means what the close could use: a ``station_observations`` row
    at an hour inside the day's bounds. The text range in the query only
    narrows the index scan, widened by a day on each side; whether a row is
    inside the day is decided on its parsed ``valid_at``, and a row whose
    ``valid_at`` does not read is not usable.
    """
    margin = timedelta(days=1)
    names = [name for name, _ in _STATION_VARIABLES]
    rows = conn.execute(
        "SELECT variable, valid_at FROM station_observations"
        " WHERE station_id = ? AND variable IN (?, ?, ?) AND qc_flag = 'ok'"
        " AND valid_at >= ? AND valid_at < ?",
        (
            station_id,
            *names,
            isoformat_utc(record.day_start_utc - margin),
            isoformat_utc(record.day_end_utc + margin),
        ),
    ).fetchall()
    found: set[str] = set()
    for row in rows:
        valid_at = _parse_or_none(row[1])
        if valid_at is None:
            continue
        if record.day_start_utc <= valid_at < record.day_end_utc:
            found.add(str(row[0]))
    return frozenset(found)


def _site_nothing_usable(
    conn: sqlite3.Connection, site_id: int, now: datetime
) -> list[_NothingUsable]:
    """Alert 3's findings for one enabled site, in station order (§5.4).

    A station's expected days are the closed days within
    ``STATION_ALERT_DAYS`` of their ``done_at`` whose saved station list
    holds it (``day_presence``'s keys are that list); missed days are
    skipped, and so is a day whose ``done_at`` does not read (alert 2
    names it). Of those, a day counts when the station has a reading from
    any source or an answered 5-minute try; any other day is skipped, never
    a break in a run. Enabled stations only.
    """
    enabled = {
        int(row[0]): str(row[1])
        for row in conn.execute(
            "SELECT id, pws_station_id FROM stations WHERE site_id = ? AND enabled = 1",
            (site_id,),
        )
    }
    counted: dict[int, list[tuple[date, frozenset[str]]]] = {}
    for record, done_at in _done_days(conn, site_id, now, STATION_ALERT_DAYS):
        if record.state != "closed" or done_at is None:
            continue
        presence = day_presence(conn, site_id=site_id, local_date=record.local_date)
        for station_id, seen in presence.items():
            if station_id not in enabled:
                continue
            if not seen.answered and not any(seen.readings.values()):
                continue
            counted.setdefault(station_id, []).append(
                (record.local_date, _usable_variables(conn, station_id, record))
            )
    findings: list[_NothingUsable] = []
    for station_id, days in counted.items():
        named: list[str] = []
        starts: list[date] = []
        for name, word in _STATION_VARIABLES:
            run: list[date] = []
            for local_date, usable in days:
                if name in usable:
                    break
                run.append(local_date)
            if len(run) >= STATION_NOTHING_USABLE_DAYS:
                named.append(word)
                starts.append(run[-1])
        if named:
            # Every named variable has had nothing usable since the latest
            # of their run starts, so the one "since" holds for each.
            findings.append(
                _NothingUsable(
                    station_id=station_id,
                    pws_station_id=enabled[station_id],
                    variables=tuple(named),
                    since=max(starts),
                )
            )
    findings.sort(key=lambda finding: (finding.pws_station_id, finding.station_id))
    return findings


def _nothing_usable(conn: sqlite3.Connection, now: datetime) -> list[_NothingUsable]:
    """Alert 3's findings over every enabled site, in the details' order."""
    findings: list[_NothingUsable] = []
    for site_id, _ in _enabled_sites(conn):
        findings.extend(_site_nothing_usable(conn, site_id, now))
    return findings


def _alert(alert_id: str, items: list[str]) -> CloseAlert:
    """One close alert from its tripped items, one per item counted."""
    return CloseAlert(
        id=alert_id,
        tripped=bool(items),
        count=len(items),
        detail=_items_detail(items),
    )


def _current_attempt(status: CloseStatus) -> AttemptRecord | None:
    """The day's current attempt, the one numbered ``attempts``, or None."""
    return next(
        (row for row in status.attempt_records if row.attempt == status.attempts),
        None,
    )


def _stalled_since(status: CloseStatus | None, now: datetime) -> datetime | None:
    """A stalled close's ``last_progress_at``, or None (plan §6.2 (c)).

    A ``closing`` day has stalled when its current attempt's
    ``last_progress_at`` is more than ``CLOSE_STALL_HOURS`` before ``now``.
    A day with no record of its current attempt is not judged.
    """
    if status is None or status.state != "closing":
        return None
    current = _current_attempt(status)
    if current is None:
        return None
    if now - current.last_progress_at <= timedelta(hours=CLOSE_STALL_HOURS):
        return None
    return current.last_progress_at


def _late_line(
    conn: sqlite3.Connection,
    site_name: str,
    record: DayRecord,
    now: datetime,
    publication: PublicationRecord | None,
    active: Publication | None,
) -> str | None:
    """Alert 4's line for a site whose current day is ``record`` (§6.1).

    T is the day's snapshot time in the day's zone; the expected day E is
    the day before from T on, two days before until T. None while the
    active publication reaches E, for a day before the site's first
    record, and for a day another alert names: ``missed`` (alert 1),
    ``retry_due``, its publication ``retry_due`` or ``failed``, or a
    stalled close (§6.2). ``publication`` is the site's latest publication
    record (R3.7), E's own when its cutoff is E or later.
    """
    try:
        deadline = resolve_snapshot_utc(
            record.timezone,
            record.local_date,
            snapshot_wall_clock(conn, record.site_id),
        )
    except (ValueError, ZoneInfoNotFoundError):
        return f"{site_name}: unreadable timezone"
    expected = record.local_date - (_ONE_DAY if now >= deadline else 2 * _ONE_DAY)
    if active is not None and active.cutoff >= expected:
        return None
    day = day_record(conn, record.site_id, expected)
    if day is None or day.state in ("missed", "retry_due"):
        return None
    if publication is not None and publication.cutoff < expected:
        publication = None
    if publication is not None and publication.state in ("retry_due", "failed"):
        return None
    if day.state == "closed":
        stage: int | None = 7
        if publication is None:
            # No publication record opened for E yet: stage 7 has waited
            # since the day was closed.
            since = _parse_or_none(day.done_at)
            if since is None:
                return f"{site_name}: unreadable done_at"
        elif publication.state == "building" and publication.started_at is not None:
            since = publication.started_at
        else:
            since = publication.opened_at
    else:
        status = close_status(
            conn, site_id=record.site_id, local_date=expected, now_utc=now
        )
        if _stalled_since(status, now) is not None:
            return None
        stage = None if status is None else status.blocking_stage
        current = None if status is None else _current_attempt(status)
        since = day.day_end_utc if current is None else current.last_progress_at
    if active is None:
        reach = "no active publication"
    else:
        age = (record.local_date - active.cutoff).days
        reach = (
            f"active publication through {active.cutoff.isoformat()}, {age} days old"
        )
    return (
        f"{site_name}: {expected.isoformat()} not published at"
        f" {isoformat_utc(deadline)}; {reach}; blocked at {stage_text(stage)}"
        f" since {isoformat_utc(since)}"
    )


def _undone_days(
    conn: sqlite3.Connection, site_id: int, current: DayRecord | None
) -> list[DayRecord]:
    """The site's records not yet done, in date order.

    They follow ``closed_through``: a day is done only once every earlier
    day is. With no day done yet, they start at the site's first record,
    found by walking down from the current day; with no current day either
    (before the first record starts) there is none to judge.
    """
    through = closed_through(conn, site_id)
    if through is not None:
        local_date = through + _ONE_DAY
    elif current is not None:
        local_date = current.local_date
        while day_record(conn, site_id, local_date - _ONE_DAY) is not None:
            local_date -= _ONE_DAY
    else:
        return []
    found: list[DayRecord] = []
    while (record := day_record(conn, site_id, local_date)) is not None:
        found.append(record)
        local_date += _ONE_DAY
    return found


def _stage_failed_parts(
    conn: sqlite3.Connection,
    undone: list[DayRecord],
    publication: PublicationRecord | None,
    active: Publication | None,
    now: datetime,
) -> list[str]:
    """The failed-attempt alert's parts for one site, in date order (§6.2).

    (a) a ``retry_due`` day, its stage from R3.4; (b) the latest
    publication record in ``retry_due`` or ``failed`` while the active
    publication does not reach a later day; (c) a stalled ``closing`` day.
    """
    parts: list[tuple[date, str]] = []
    for record in undone:
        if record.state not in ("retry_due", "closing"):
            continue
        status = close_status(
            conn, site_id=record.site_id, local_date=record.local_date, now_utc=now
        )
        day = record.local_date.isoformat()
        stage = None if status is None else status.blocking_stage
        if record.state == "retry_due":
            retry = (
                "unknown" if record.retry_at is None else isoformat_utc(record.retry_at)
            )
            parts.append(
                (
                    record.local_date,
                    f"{day}: attempt 1 failed at {stage_text(stage)}:"
                    f" {_failure_text(record)}; retry due {retry}",
                )
            )
        elif (since := _stalled_since(status, now)) is not None:
            parts.append(
                (
                    record.local_date,
                    f"{day}: {stage_text(stage)} has made no progress since"
                    f" {isoformat_utc(since)}",
                )
            )
    passed = (
        publication is not None
        and active is not None
        and active.cutoff > publication.cutoff
    )
    if publication is not None and not passed:
        cutoff = publication.cutoff.isoformat()
        reason = (
            "no reason recorded"
            if publication.last_failure is None
            else publication.last_failure
        )
        if publication.state == "retry_due":
            retry = (
                "unknown"
                if publication.retry_at is None
                else isoformat_utc(publication.retry_at)
            )
            parts.append(
                (
                    publication.cutoff,
                    f"{cutoff}: publication attempt 1 failed: {reason};"
                    f" retry due {retry}",
                )
            )
        elif publication.state == "failed":
            failed = (
                ""
                if publication.finished_at is None
                else f" at {isoformat_utc(publication.finished_at)}"
            )
            parts.append(
                (
                    publication.cutoff,
                    f"{cutoff}: publication failed{failed}: {reason}; the"
                    " previous publication stays in use",
                )
            )
    parts.sort(key=lambda part: part[0])
    return [text for _, text in parts]


def _all_answered_empty(
    conn: sqlite3.Connection,
    record: DayRecord,
    stations: frozenset[int],
    start: datetime,
) -> bool:
    """Whether every station's latest 5-minute try since ``start`` ended
    ``no_readings`` (plan §6.5).

    Reads the try log of the current day ``record`` and each earlier day
    whose end lies less than a day before ``start`` (a day's evening tries
    can end after the day does), up to ``CLOSE_LOG_KEEP_DAYS`` days, the
    log's own retention. A station with no try since ``start`` makes it
    False.
    """
    days: list[list[CollectionTry]] = []
    local_date = record.local_date
    for _ in range(CLOSE_LOG_KEEP_DAYS + 1):
        day = day_record(conn, record.site_id, local_date)
        if day is None or day.day_end_utc + _ONE_DAY <= start:
            break
        days.append(
            collection_tries(conn, site_id=record.site_id, local_date=local_date)
        )
        local_date -= _ONE_DAY
    latest: dict[int, CollectionTry] = {}
    for tries in reversed(days):
        for attempt in tries:
            if attempt.kind != "five_minute" or attempt.tried_at < start:
                continue
            held = latest.get(attempt.station_id)
            if held is None or attempt.tried_at >= held.tried_at:
                latest[attempt.station_id] = attempt
    return all(
        station_id in latest and latest[station_id].outcome == "no_readings"
        for station_id in stations
    )


def _daytime_line(
    conn: sqlite3.Connection,
    site_name: str,
    record: DayRecord,
    now: datetime,
    interval: int,
    floors: list[datetime],
) -> str | None:
    """The daytime-download warning's line for one site, or None (§6.5).

    Its start of count is the latest of the site's last 5-minute store,
    ``floors`` (the worker's start and the close's turn-on) and, on the
    site's first record, that record's start. It trips when ``now`` is
    more than ``_DAYTIME_MISSED_REFRESHES`` refreshes past it, for a site
    with a station on the current day's list, unless ``five_minute`` is
    refused (alert 6's) or every station answered with no data (alerts 3
    and 5's).
    """
    stations = day_station_ids(conn, record.site_id, record.local_date)
    if not stations:
        return None
    starts = [*floors]
    store = last_five_minute_store(conn, site_id=record.site_id)
    if store is not None:
        starts.append(store)
    if day_record(conn, record.site_id, record.local_date - _ONE_DAY) is None:
        starts.append(record.day_start_utc)
    if not starts:
        return None
    start = max(starts)
    if now - start <= timedelta(minutes=_DAYTIME_MISSED_REFRESHES * interval):
        return None
    if any(held.kind == "five_minute" for held in refused_kinds(conn, now)):
        return None
    if _all_answered_empty(conn, record, stations, start):
        return None
    return (
        f"{site_name}: no 5-minute readings stored since {isoformat_utc(start)}"
        f" ({_DAYTIME_MISSED_REFRESHES} refreshes of {interval} min missed)"
    )


@dataclass(frozen=True)
class _Slot:
    """One judged collection slot of alert 7 (plan §6.6).

    ``time`` is its earliest try's ``tried_at``; ``answered_at`` its
    earliest answered try's, None for a down slot (an outage try and no
    answered one).
    """

    time: datetime
    answered_at: datetime | None


def _is_outage(attempt: CollectionTry) -> bool:
    """A network or back-off try, or a ``provider`` 429 or 5xx (§6.6)."""
    if attempt.reason in _OUTAGE_REASONS:
        return True
    return (
        attempt.reason == "provider"
        and attempt.detail is not None
        and _OUTAGE_HTTP.fullmatch(attempt.detail) is not None
    )


def _day_slots(
    record: DayRecord, tries: list[CollectionTry], interval: int
) -> list[_Slot]:
    """The judged slots of one site-day's tries (plan §6.6).

    An evening slot is one ``slot`` value; a gap-fill slot one close
    attempt; a daytime slot the tries in one refresh interval counted from
    the day's start. A slot with only tries that are neither answered nor
    an outage (held, refused, ``call_limit``, another status) is not
    judged and not returned.
    """
    step = timedelta(minutes=interval)
    groups: dict[tuple[str, int], list[CollectionTry]] = {}
    for attempt in tries:
        if attempt.slot == "daytime":
            key = ("daytime", (attempt.tried_at - record.day_start_utc) // step)
        elif attempt.slot == "gap_fill":
            key = ("gap_fill", attempt.close_attempt or 0)
        else:
            key = (attempt.slot, 0)
        groups.setdefault(key, []).append(attempt)
    slots: list[_Slot] = []
    for group in groups.values():
        answered = [a.tried_at for a in group if a.outcome in _ANSWERED_OUTCOMES]
        if answered or any(_is_outage(a) for a in group):
            slots.append(
                _Slot(
                    time=min(a.tried_at for a in group),
                    answered_at=min(answered) if answered else None,
                )
            )
    return slots


def _slots_newest_first(
    conn: sqlite3.Connection, current: DayRecord, interval: int
) -> Iterator[_Slot]:
    """The site's judged slots, newest first, read day by day on demand.

    Walks down from the current day, up to ``CLOSE_LOG_KEEP_DAYS`` days
    (the try log's retention) or the site's first record. A day's tries
    can end after the day does (its evening and gap-fill tries), so once a
    day is read only its slots timed at or after its end are yielded; the
    rest wait for the earlier day.
    """
    pending: list[_Slot] = []
    local_date = current.local_date
    for _ in range(CLOSE_LOG_KEEP_DAYS + 1):
        record = day_record(conn, current.site_id, local_date)
        if record is None:
            break
        pending.extend(
            _day_slots(
                record,
                collection_tries(conn, site_id=current.site_id, local_date=local_date),
                interval,
            )
        )
        ready = [slot for slot in pending if slot.time >= record.day_end_utc]
        pending = [slot for slot in pending if slot.time < record.day_end_utc]
        yield from sorted(ready, key=lambda slot: slot.time, reverse=True)
        local_date -= _ONE_DAY
    yield from sorted(pending, key=lambda slot: slot.time, reverse=True)


def _provider_down(
    conn: sqlite3.Connection, current: DayRecord, now: datetime, interval: int
) -> tuple[tuple[datetime, int] | None, datetime | None]:
    """Alert 7 for one site: ``(since, down slots)`` and ``recovered_at``.

    The first is set when the latest judged slot is down: ``since`` the
    earliest slot of the run of down judged slots, counted. The second is
    set when the latest judged slot is answered: walking back through the
    answered slots to the first down slot, it is the first answered try of
    the slot that ended that down run, while at most
    ``CLOSE_RECOVERED_SHOW_HOURS`` before ``now`` (plan §6.6), so later
    answered slots do not remove it. The walk stops with no recovery time
    at an answered slot whose first answered try is older than that, or
    when the slots run out before a down slot.
    """
    slots = _slots_newest_first(conn, current, interval)
    latest = next(slots, None)
    if latest is None:
        return None, None
    if latest.answered_at is None:
        since, count = latest.time, 1
        for slot in slots:
            if slot.answered_at is not None:
                break
            since, count = slot.time, count + 1
        return (since, count), None
    window = timedelta(hours=CLOSE_RECOVERED_SHOW_HOURS)
    recovered_at = latest.answered_at
    while now - recovered_at <= window:
        slot = next(slots, None)
        if slot is None:
            return None, None
        if slot.answered_at is None:
            return None, recovered_at
        recovered_at = slot.answered_at
    return None, None


def close_alerts(conn: sqlite3.Connection, now: datetime) -> list[CloseAlert]:
    """The close's alerts, one ``CloseAlert`` per id in plan §5.1's order.

    Alert 1, ``close_day_missed`` (§5.2): a missed day, other than a
    changeover day, whose ``done_at`` is within ``CLOSE_ALERT_DAYS``.
    Alert 2, ``close_day_thin`` (§5.3): a closed day in the same window
    with a daily value not counted. Alert 3, ``station_nothing_usable``
    (§5.4): a station whose newest ``STATION_NOTHING_USABLE_DAYS`` counted
    days all have nothing usable for a variable. Alert 4,
    ``publication_late`` (§6.1): at the snapshot time the active
    publication does not reach the expected day. ``close_stage_failed``
    (§6.2): one line per site listing a failed first attempt, a failed
    publication and a stalled close. ``daytime_download_missed`` (§6.5):
    no 5-minute readings stored for two refreshes. Alert 7,
    ``provider_down`` (§6.6): the latest judged collection slot is down;
    while ok, its "recovered at" line shows for
    ``CLOSE_RECOVERED_SHOW_HOURS``. Enabled sites only, and only days with
    a P1 record, so before any day has a state every alert is ok; a site
    with no record holding ``now`` has no current day, and alerts 4 and 7
    and the daytime warning leave it out. Read-only.
    """
    missed: list[str] = []
    thin: list[str] = []
    sites = _enabled_sites(conn)
    for site_id, site_name in sites:
        for record, done_at in reversed(
            _done_days(conn, site_id, now, CLOSE_ALERT_DAYS)
        ):
            item = f"{site_name} {record.local_date.isoformat()}"
            if record.state == "missed":
                if record.missed_reason == "zone_changeover":
                    continue
                missed.append(
                    f"{item}: unreadable done_at"
                    if done_at is None
                    else _missed_line(conn, item, record, now)
                )
            elif record.state == "closed":
                line = (
                    f"{item}: unreadable done_at"
                    if done_at is None
                    else _thin_line(conn, item, record)
                )
                if line is not None:
                    thin.append(line)
    nothing_usable = [
        f"station {finding.pws_station_id}: nothing usable for"
        f" {_and_join(finding.variables)} since {finding.since.isoformat()}"
        for finding in _nothing_usable(conn, now)
    ]

    interval = get_number_setting(conn, "obs_interval_minutes", 180, minimum=30)
    # The daytime warning's floors (§6.5): the worker's start, read as the
    # start-up grace reads it (an unreadable stamp is no floor), and the
    # time the close was turned on.
    floors = [
        stamp
        for stamp in (
            _parse_or_none(get_runtime_state(conn, "worker_started_at")),
            daily_close_on_at(conn),
        )
        if stamp is not None
    ]
    late: list[str] = []
    stage_failed: list[str] = []
    daytime: list[str] = []
    down: list[str] = []
    recovered: list[datetime] = []
    for site_id, site_name in sites:
        current = current_day(conn, site_id, now_utc=now)
        publication = publication_state(conn, site_id=site_id)
        active = active_publication(conn, site_id=site_id, at_utc=isoformat_utc(now))
        parts = _stage_failed_parts(
            conn, _undone_days(conn, site_id, current), publication, active, now
        )
        if parts:
            stage_failed.append(f"{site_name}: {'; '.join(parts)}")
        if current is None:
            continue
        line = _late_line(conn, site_name, current, now, publication, active)
        if line is not None:
            late.append(line)
        line = _daytime_line(conn, site_name, current, now, interval, floors)
        if line is not None:
            daytime.append(line)
        run, recovered_at = _provider_down(conn, current, now, interval)
        if run is not None:
            down.append(
                f"{site_name}: weather.com unreachable or failing since"
                f" {isoformat_utc(run[0])}; down slots: {run[1]}"
            )
        elif recovered_at is not None:
            recovered.append(recovered_at)

    provider = _alert("provider_down", down)
    if not down and recovered:
        # Ok, with the recovery line(s) while they show (§6.6).
        provider = CloseAlert(
            id="provider_down",
            tripped=False,
            count=0,
            detail=_items_detail(
                [f"recovered at {isoformat_utc(stamp)}" for stamp in recovered]
            ),
            recovered_at=max(recovered),
        )
    return [
        _alert("close_day_missed", missed),
        _alert("close_day_thin", thin),
        _alert("station_nothing_usable", nothing_usable),
        _alert("publication_late", late),
        _alert("close_stage_failed", stage_failed),
        _alert("daytime_download_missed", daytime),
        provider,
    ]


# Alert 5's judged stations (plan §6.3): enabled stations of enabled sites
# whose latest written poll answer was about the station (online or
# offline), in the detail's order (site name, then station).
_STATION_SILENT_SQL: Final = """
    SELECT st.id, st.pws_station_id, ps.health_state, ps.last_obstime,
           ps.last_poll_at
    FROM stations st
    JOIN sites s ON s.id = st.site_id
    JOIN station_poll_state ps ON ps.station_id = st.id
    WHERE s.enabled = 1 AND st.enabled = 1
      AND ps.health_state IN ('online', 'offline')
    ORDER BY s.name, s.id, st.pws_station_id, st.id
"""


def _silent_line(
    station: str,
    health: object,
    raw_obstime: object,
    raw_poll_at: object,
    now: datetime,
) -> str | None:
    """One judged station's alert 5 line, or None when it is not silent.

    The age is measured from the reading's own observation time: for
    ``online``, ``last_poll_at`` minus ``last_obstime`` (the age the poll saw
    at its latest answer, so a back-off never grows it); for ``offline``,
    ``now`` minus ``last_obstime``. It trips at ``STATION_SILENT_HOURS`` or
    more, or when a stamp it needs is NULL or does not read.
    """
    item = f"station {station}"
    if raw_obstime is None:
        return f"{item}: no observation yet"
    obstime = _parse_or_none(raw_obstime)
    if obstime is None:
        return f"{item}: unreadable last_obstime"
    threshold = timedelta(hours=STATION_SILENT_HOURS)
    if health == "offline":
        age = now - obstime
        if age < threshold:
            return None
        return (
            f"{item}: weather.com reports no current data;"
            f" newest observation {_age_text(age)} old"
        )
    polled = _parse_or_none(raw_poll_at)
    if polled is None:
        return f"{item}: unreadable last_poll_at"
    age = polled - obstime
    if age < threshold:
        return None
    mark = ", provisional" if STATION_SILENT_PROVISIONAL else ""
    return (
        f"{item}: newest observation {_age_text(age)} old at"
        f" {isoformat_utc(polled)} (threshold {STATION_SILENT_HOURS} h{mark})"
    )


def station_silent(conn: sqlite3.Connection, now: datetime) -> CloseAlert:
    """Alert 5, ``station_silent_today``: stations whose poll sees them silent.

    Judges each enabled station of an enabled site from its
    ``station_poll_state`` row, ``online`` and ``offline`` only: ``transient``
    and ``terminal`` are failures, not silence. While ``current_obs`` is
    among ``refused_kinds`` no station is judged, since the refused key is
    alert 6's. Never reads the full-day readings (plan §6.3, K3). A station
    alert 3 names for every variable is left out, so it shows once, as
    alert 3's standing line. Read-only.
    """
    if any(record.kind == "current_obs" for record in refused_kinds(conn, now)):
        return CloseAlert(id=_STATION_SILENT_ID, tripped=False, count=0)
    found: list[tuple[int, str]] = []
    for row in conn.execute(_STATION_SILENT_SQL):
        line = _silent_line(str(row[1]), row[2], row[3], row[4], now)
        if line is not None:
            found.append((int(row[0]), line))
    if found:
        every_variable = {
            finding.station_id
            for finding in _nothing_usable(conn, now)
            if len(finding.variables) == len(_STATION_VARIABLES)
        }
        found = [item for item in found if item[0] not in every_variable]
    lines = [line for _, line in found]
    return CloseAlert(
        id=_STATION_SILENT_ID,
        tripped=bool(lines),
        count=len(lines),
        detail=_items_detail(lines),
    )


@dataclass(frozen=True)
class AccessDays:
    """The close's part of alert 6, ``weathercom_access_refused`` (plan §6.4).

    ``next_try`` holds one clause per refused kind whose hold is in force,
    in ``ACCESS_KINDS`` order. ``days`` holds every day affected as
    "<site> <date> (<state>)", by site name then date, with no cap: the
    close status panel shows the full list (plan §10.2). Neither names a
    station.
    """

    next_try: tuple[str, ...] = ()
    days: tuple[str, ...] = ()

    def detail_suffix(self) -> str:
        """The text the budget group appends to 0.16.13's line.

        "; next try: <clauses>" and "; days affected: <days>", each only
        when it has an item, the items joined by ", "; the days at most
        ``_ACCESS_DAYS_MAX_ITEMS``, then "and N more". Empty when both
        are empty.
        """
        parts: list[str] = []
        if self.next_try:
            parts.append(f"; next try: {', '.join(self.next_try)}")
        days = _items_detail(list(self.days), limit=_ACCESS_DAYS_MAX_ITEMS, sep=", ")
        if days is not None:
            parts.append(f"; days affected: {days}")
        return "".join(parts)


def _access_day_item(site_name: str, record: DayRecord) -> str:
    """One day affected, "<site> <date> (<state>)" (plan §6.4)."""
    if record.state == "retry_due":
        retry = "unknown" if record.retry_at is None else isoformat_utc(record.retry_at)
        state = f"retry due {retry}"
    else:
        state = _ACCESS_DAY_STATES.get(record.state, record.state)
    return f"{site_name} {record.local_date.isoformat()} ({state})"


def _next_try_clause(
    conn: sqlite3.Connection,
    kind: AccessKind,
    until: datetime,
    sites: list[tuple[int, str]],
    undone: list[DayRecord],
    now: datetime,
) -> str:
    """The next-try clause of one kind whose hold is in force (plan §6.4).

    ``current_obs``: a held poll is deferred to the hold's end. ``hourly``:
    the gap fill's next try is the close's retry, the earliest ``retry_at``
    of the enabled sites' ``retry_due`` days, else the next close.
    ``five_minute``: the earliest of the enabled sites' next scheduled
    5-minute downloads when it lies after ``until``; at or before it (a try
    the hold will skip), or with none, the first one after ``until``.
    """
    held_until = isoformat_utc(until)
    if kind == "current_obs":
        return f"current conditions at {held_until}"
    if kind == "hourly":
        retries = [record for record in undone if record.state == "retry_due"]
        if not retries:
            return "hourly gap fill at the next close"
        stamps = [record.retry_at for record in retries if record.retry_at is not None]
        if not stamps:
            return "hourly gap fill: unreadable retry_at"
        return f"hourly gap fill at {isoformat_utc(min(stamps))}"
    tries = [
        stamp
        for site_id, _ in sites
        if (stamp := next_five_minute_try(conn, site_id=site_id, now_utc=now))
        is not None
    ]
    earliest = min(tries, default=None)
    if earliest is not None and earliest > until:
        return f"5-minute readings at {isoformat_utc(earliest)}"
    return f"5-minute readings at the first scheduled download after {held_until}"


def _affected_days(
    conn: sqlite3.Connection,
    refused: list[AccessRecord],
    sites: list[tuple[int, str]],
    undone: dict[int, list[DayRecord]],
    now: datetime,
) -> list[str]:
    """Every day affected by the refusal run, by site name then date (§6.4).

    The run is from the earliest readable ``since`` to the latest readable
    ``until``, or ``now`` if later, over the rows whose ``until`` reads, as
    0.16.13's line takes its times. A day is affected when its record's
    bounds overlap the run and it is not done or was done within
    ``CLOSE_ALERT_DAYS``. A ``closing`` or ``retry_due`` day still needs
    the key for its close's downloads after its bounds end, so it counts
    as overlapping any run that ends after the day starts. With no
    readable ``until`` or ``since`` the run is unknown, and the one item
    says which does not read.
    """
    readable = [record for record in refused if record.until is not None]
    untils = [
        stamp
        for record in readable
        if (stamp := _parse_or_none(record.until)) is not None
    ]
    sinces = [
        stamp
        for record in readable
        if (stamp := _parse_or_none(record.since)) is not None
    ]
    if not untils:
        return ["unreadable until"]
    if not sinces:
        return ["unreadable since"]
    start = min(sinces)
    end = max(now, *untils)
    found: list[str] = []
    for site_id, site_name in sites:
        done = [
            record
            for record, _ in reversed(_done_days(conn, site_id, now, CLOSE_ALERT_DAYS))
        ]
        for record in (*done, *undone[site_id]):
            if record.day_start_utc >= end:
                continue
            if record.state in _ACCESS_STILL_CLOSING or record.day_end_utc > start:
                found.append(_access_day_item(site_name, record))
    return found


def access_days_affected(conn: sqlite3.Connection, now: datetime) -> AccessDays:
    """The close's part of alert 6, ``weathercom_access_refused`` (§6.4).

    Reads ``refused_kinds``. The next try: one clause per listed kind whose
    hold is in force (``until`` after ``now``), "current conditions at
    <until>", "hourly gap fill at <retry_at>" (or "at the next close") and
    "5-minute readings at <time>" (or "at the first scheduled download
    after <until>"); a kind whose hold has ended, or does not read, has no
    clause and keeps 0.16.13's "waiting for the next try". The days
    affected: for each enabled site, the days whose record's bounds overlap
    the refusal run and that are not done or were done within
    ``CLOSE_ALERT_DAYS``, with the state as "collecting", "closing", "retry
    due <retry_at>", "closed" or "missed". Never judges or names a station.
    Empty while no kind is listed. Read-only.
    """
    refused = refused_kinds(conn, now)
    if not refused:
        return AccessDays()
    sites = _enabled_sites(conn)
    undone = {
        site_id: _undone_days(conn, site_id, current_day(conn, site_id, now_utc=now))
        for site_id, _ in sites
    }
    every_undone = [record for days in undone.values() for record in days]
    clauses: list[str] = []
    for record in refused:
        until = _parse_or_none(record.until)
        if until is None or until <= now:
            continue
        clauses.append(
            _next_try_clause(conn, record.kind, until, sites, every_undone, now)
        )
    return AccessDays(
        next_try=tuple(clauses),
        days=tuple(_affected_days(conn, refused, sites, undone, now)),
    )
