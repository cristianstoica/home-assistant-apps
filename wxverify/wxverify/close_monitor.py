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
``wxverify.db.site_days``'s functions; a row that P1's or P3's readers
cannot parse raises their ``ValueError``, which the caller reports as
``unexpected_error``.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final

from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.site_days import DayRecord, closed_through, day_record
from wxverify.worker.access_hold import refused_kinds
from wxverify.worker.close_chain import close_status
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
    ok. ``recovered_at`` is alert 7's recovery time, None for every other
    alert.
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


def _items_detail(items: list[str]) -> str | None:
    """``items`` joined by "; ", at most ``_DETAIL_MAX_ITEMS``, then
    "and N more"; None when there are none."""
    if not items:
        return None
    shown = items[:_DETAIL_MAX_ITEMS]
    if len(items) > _DETAIL_MAX_ITEMS:
        shown.append(f"and {len(items) - _DETAIL_MAX_ITEMS} more")
    return "; ".join(shown)


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


def close_alerts(conn: sqlite3.Connection, now: datetime) -> list[CloseAlert]:
    """The close's alerts, one ``CloseAlert`` per id in plan §5.1's order.

    Alert 1, ``close_day_missed`` (§5.2): a missed day, other than a
    changeover day, whose ``done_at`` is within ``CLOSE_ALERT_DAYS``.
    Alert 2, ``close_day_thin`` (§5.3): a closed day in the same window
    with a daily value not counted. Alert 3, ``station_nothing_usable``
    (§5.4): a station whose newest ``STATION_NOTHING_USABLE_DAYS`` counted
    days all have nothing usable for a variable. Enabled sites only, and
    only days with a P1 record, so before any day is done every alert is
    ok. Called only while the close is on. Read-only.
    """
    missed: list[str] = []
    thin: list[str] = []
    for site_id, site_name in _enabled_sites(conn):
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
    return [
        _alert("close_day_missed", missed),
        _alert("close_day_thin", thin),
        _alert("station_nothing_usable", nothing_usable),
        # Step 4 piece 3 builds these four (plan §6.1, §6.2, §6.5, §6.6).
        CloseAlert(id="publication_late", tripped=False, count=0),
        CloseAlert(id="close_stage_failed", tripped=False, count=0),
        CloseAlert(id="daytime_download_missed", tripped=False, count=0),
        CloseAlert(id="provider_down", tripped=False, count=0),
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


def station_silent(
    conn: sqlite3.Connection, now: datetime, *, close_on: bool
) -> CloseAlert:
    """Alert 5, ``station_silent_today``: stations whose poll sees them silent.

    Judges each enabled station of an enabled site from its
    ``station_poll_state`` row, ``online`` and ``offline`` only: ``transient``
    and ``terminal`` are failures, not silence. While ``current_obs`` is
    among ``refused_kinds`` no station is judged, since the refused key is
    alert 6's. Never reads the full-day readings (plan §6.3, K3). Called with
    the close on and off; ``close_on`` is the switch as the caller read it.
    With it on, a station alert 3 names for every variable is left out, so
    it shows once, as alert 3's standing line; with it off there is no
    alert 3 and nothing is left out. Read-only.
    """
    if any(record.kind == "current_obs" for record in refused_kinds(conn, now)):
        return CloseAlert(id=_STATION_SILENT_ID, tripped=False, count=0)
    found: list[tuple[int, str]] = []
    for row in conn.execute(_STATION_SILENT_SQL):
        line = _silent_line(str(row[1]), row[2], row[3], row[4], now)
        if line is not None:
            found.append((int(row[0]), line))
    if close_on and found:
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
