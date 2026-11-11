"""The scored round's schedule and the fixed daytime downloads (P4 plan 4.4,
4.5, 5.1).

Each night's round fetches every scheduled feed at the setting
``scoring_round_local_time`` with up to three quick retries; each feed's
daytime downloads follow at fixed local times counted from the round, except
the meteoblue package's, which use ``METEOBLUE_DAYTIME_LOCAL_TIMES``. Every
round and slot instant is a local wall time on one day's ``schedule_day``,
through P1's ``local_clock_utc``.

Every function runs in the caller's transaction and never commits, and reads
the clock only through ``now_utc``. ``worker/scheduler.py`` and
``worker/processor.py`` import this module, so it imports neither of them nor
``worker/close_chain.py`` (plan 16.1); it takes P3's ``RoundState`` from
``worker/close_p4.py``.
"""

from __future__ import annotations

import dataclasses
import logging
import re
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Final
from zoneinfo import ZoneInfoNotFoundError

from wxverify.core.timeutil import isoformat_utc, local_day_slots
from wxverify.db.queue import enqueue_if_absent, enqueue_if_absent_with_cooldown
from wxverify.db.site_days import (
    DayRecord,
    day_record,
    local_clock_utc,
)
from wxverify.settings.keys import get_setting, set_setting
from wxverify.worker.cadence import parse_fetch_interval_minutes
from wxverify.worker.close_p4 import RoundState

logger = logging.getLogger(__name__)

#: The round's local start time, ``HH:MM`` (4.4). No stored default: readers
#: use ``ROUND_DEFAULT_LOCAL_TIME`` when the key is absent (15.4).
SCORING_ROUND_LOCAL_TIME_KEY: Final = "scoring_round_local_time"
ROUND_DEFAULT_LOCAL_TIME: Final = "00:10"
_ROUND_DEFAULT: Final = (0, 10)  # ROUND_DEFAULT_LOCAL_TIME as (hour, minute)
# The setting's range, 00:05 to 05:00 local, in minutes after midnight: after
# the evening collection, which ends at 23:59, and early enough that the
# close's first attempt, which waits for the round's bound, can publish
# before the 07:00 default snapshot time, the deadline of alert 4,
# publication_late (4.4).
_ROUND_EARLIEST_MINUTES: Final = 5
_ROUND_LATEST_MINUTES: Final = 5 * 60

#: Tries 2, 3 and 4, the quick retries, start this long after the round (4.4).
ROUND_RETRY_OFFSETS: Final = (
    timedelta(minutes=5),
    timedelta(minutes=12),
    timedelta(minutes=20),
)
#: A try not claimed by the round's start plus this is not run (4.4, 4.5).
ROUND_BOUND: Final = timedelta(minutes=30)
ROUND_TRIES: Final = 1 + len(ROUND_RETRY_OFFSETS)
_TRY_OFFSETS: Final = (timedelta(0), *ROUND_RETRY_OFFSETS)

# The daytime download's job keeps the retry cap and failure cooldown of the
# due-feed loop it replaces (5.1); the cooldown is worker/scheduler.py's
# _DUE_JOB_FAILURE_COOLDOWN, which this module cannot import (16.1).
DAYTIME_MAX_RETRIES: Final = 3
DAYTIME_FAILURE_COOLDOWN: Final = timedelta(hours=1)

#: The meteoblue package's daytime calls, as site-local (hour, minute), in
#: clock order: the round is the day's first call, and the latest round
#: time (05:00) is before the first of these. Retries give up the last one
#: first. A trial; each change ships with a release.
METEOBLUE_DAYTIME_LOCAL_TIMES: Final = ((9, 10), (12, 10), (21, 10))

_LOCAL_TIME_RE: Final = re.compile(r"([01]\d|2[0-3]):([0-5]\d)")
_MINUTES_PER_DAY: Final = 24 * 60
_ONE_DAY: Final = timedelta(days=1)
# A round try's row is dated by its end in the day record's zone or in
# sites.timezone (4.2): within two days of the round date for any two zones,
# so its lookups stay on idx_forecast_downloads_day's (site, feed, date) range.
_ROUND_ROW_DAYS: Final = timedelta(days=2)


@dataclass(frozen=True, slots=True)
class _ScheduleFeed:
    """One feed the schedule fetches for a site (4.4, 5.1)."""

    feed_id: int
    interval_minutes: int
    package: bool


def _as_utc(value: datetime) -> datetime:
    """``value`` as an aware UTC datetime (a naive value is UTC)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


# ---------------------------------------------------------------------------
# The setting (4.4)
# ---------------------------------------------------------------------------


def _parse_round_time(value: str) -> tuple[int, int] | None:
    """``(hour, minute)`` of an ``HH:MM`` value inside the setting's range."""
    match = _LOCAL_TIME_RE.fullmatch(value)
    if match is None:
        return None
    hour, minute = int(match.group(1)), int(match.group(2))
    if not _ROUND_EARLIEST_MINUTES <= hour * 60 + minute <= _ROUND_LATEST_MINUTES:
        return None
    return hour, minute


def set_scoring_round_local_time_sync(conn: sqlite3.Connection, value: str) -> None:
    """Store the round's local start time (4.4).

    ``value`` is ``HH:MM`` from ``00:05`` to ``05:00``. Raises ``ValueError``,
    storing nothing, for any other value. The change moves the next round;
    the scored-download rule does not change.
    """
    if _parse_round_time(value) is None:
        raise ValueError(
            f"{SCORING_ROUND_LOCAL_TIME_KEY} must be HH:MM from 00:05 to 05:00"
        )
    set_setting(conn, SCORING_ROUND_LOCAL_TIME_KEY, value)


def scoring_round_local_time(conn: sqlite3.Connection) -> tuple[int, int]:
    """The round's local start time as ``(hour, minute)`` (4.4). Read-only.

    ``ROUND_DEFAULT_LOCAL_TIME`` when the key is absent, and, with a warning,
    when its stored value does not read or lies outside the range.
    """
    raw = get_setting(conn, SCORING_ROUND_LOCAL_TIME_KEY)
    if raw is None:
        return _ROUND_DEFAULT
    parsed = _parse_round_time(raw)
    if parsed is None:
        logger.warning(
            "round: unusable %s=%r; using %s",
            SCORING_ROUND_LOCAL_TIME_KEY,
            raw,
            ROUND_DEFAULT_LOCAL_TIME,
        )
        return _ROUND_DEFAULT
    return parsed


# ---------------------------------------------------------------------------
# The day every instant is taken from (4.4)
# ---------------------------------------------------------------------------


def _site_timezone(conn: sqlite3.Connection, site_id: int) -> str:
    row = conn.execute("SELECT timezone FROM sites WHERE id = ?", (site_id,)).fetchone()
    if row is None:
        raise ValueError(f"site {site_id} does not exist")
    return str(row[0])


def _latest_record(conn: sqlite3.Connection, site_id: int) -> DayRecord | None:
    row = conn.execute(
        "SELECT MAX(local_date) FROM site_days WHERE site_id = ?", (site_id,)
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return day_record(conn, site_id, date.fromisoformat(str(row[0])))


def schedule_day(
    conn: sqlite3.Connection, *, site_id: int, local_date: date
) -> DayRecord:
    """The day every round and daytime instant of ``local_date`` is taken from (4.4).

    Read-only; makes no record. The date's record when it exists; else, when
    the site's latest record L is dated before it, L with the date and
    bounds of ``local_date`` in L's zone (from L's end when the date follows
    L), the record ``ensure_site_days`` will make. Otherwise (a date before
    the site's first record, or a site with no record) a stand-in in
    ``sites.timezone``. Raises ``ValueError`` for a missing site, and
    ``ValueError`` or ``ZoneInfoNotFoundError`` for a zone that does not load.
    """
    record = day_record(conn, site_id, local_date)
    if record is not None:
        return record
    latest = _latest_record(conn, site_id)
    if latest is not None and latest.local_date < local_date:
        start, end, _ = local_day_slots(local_date, latest.timezone)
        if local_date == latest.local_date + _ONE_DAY:
            start = latest.day_end_utc
        return dataclasses.replace(
            latest,
            local_date=local_date,
            day_start_utc=start,
            day_end_utc=end,
            zone_changeover=False,
        )
    timezone = _site_timezone(conn, site_id)
    start, end, _ = local_day_slots(local_date, timezone)
    return DayRecord(
        site_id=site_id,
        local_date=local_date,
        state="open",
        day_start_utc=start,
        day_end_utc=end,
        timezone=timezone,
        zone_changeover=False,
        attempts=0,
        retry_at=None,
        last_failure=None,
        missed_reason=None,
        done_at=None,
    )


def _tick_day(
    conn: sqlite3.Connection, site_id: int, now: datetime
) -> DayRecord | None:
    """The ``schedule_day`` that holds ``now``, among now's UTC date and the
    dates either side (every zone lies within a day of UTC) (4.4, 5.1)."""
    today = now.date()
    for offset in (-1, 0, 1):
        day = schedule_day(conn, site_id=site_id, local_date=today + offset * _ONE_DAY)
        if day.day_start_utc <= now < day.day_end_utc:
            return day
    return None


def _round_start_utc(conn: sqlite3.Connection, day: DayRecord) -> datetime | None:
    """The round's start on ``day``, or None on a day with no round (4.4).

    The setting's local time by ``local_clock_utc`` on a copy of ``day``
    bounded by the zone's whole local day, which holds every time in the
    setting's range; None when that instant lies outside ``day``'s own
    bounds, as on a changeover day after a move east. Catches no exception,
    so a zone that does not load still raises ``ValueError`` or
    ``ZoneInfoNotFoundError``.
    """
    hour, minute = scoring_round_local_time(conn)
    whole_start, whole_end, _ = local_day_slots(day.local_date, day.timezone)
    whole_day = dataclasses.replace(
        day, day_start_utc=whole_start, day_end_utc=whole_end
    )
    start = local_clock_utc(whole_day, hour, minute)
    if not day.day_start_utc <= start < day.day_end_utc:
        return None
    return start


# ---------------------------------------------------------------------------
# Who is fetched, and what the record holds (4.4, 5.1)
# ---------------------------------------------------------------------------


def _enabled_site_ids(conn: sqlite3.Connection) -> list[int]:
    rows = conn.execute("SELECT id FROM sites WHERE enabled = 1 ORDER BY id").fetchall()
    return [int(row[0]) for row in rows]


def _schedule_feeds(conn: sqlite3.Connection, site_id: int) -> list[_ScheduleFeed]:
    """The feeds the schedule fetches for the site: real, enabled, subscribed,
    meteoblue's members through the package. A feed whose interval does not
    read is left out, with the cadence parser's warning (fail closed)."""
    rows = conn.execute(
        """
        SELECT f.id AS feed_id, f.source, f.model, f.fetch_interval_minutes
        FROM sites s
        JOIN feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = s.id AND sfs.feed_id = f.id
        WHERE s.id = ?
          AND s.enabled = 1
          AND f.enabled = 1
          AND f.is_virtual = 0
          AND NOT (f.source='meteoblue' AND f.model != 'multimodel')
          AND COALESCE(sfs.enabled, f.default_subscribed) = 1
        ORDER BY f.id
        """,
        (site_id,),
    ).fetchall()
    feeds: list[_ScheduleFeed] = []
    for row in rows:
        feed_id = int(row["feed_id"])
        interval = parse_fetch_interval_minutes(
            row["fetch_interval_minutes"],
            context=f"scheduler site={site_id} feed={feed_id}",
        )
        if interval is None:
            continue
        feeds.append(
            _ScheduleFeed(
                feed_id=feed_id,
                interval_minutes=interval,
                package=str(row["source"]) == "meteoblue"
                and str(row["model"]) == "multimodel",
            )
        )
    return feeds


def _round_row_dates(round_date: date) -> tuple[str, str]:
    return (
        (round_date - _ROUND_ROW_DAYS).isoformat(),
        (round_date + _ROUND_ROW_DAYS).isoformat(),
    )


def _round_progress(
    conn: sqlite3.Connection, site_id: int, feed_id: int, round_date: date
) -> tuple[int, bool]:
    """The feed's last recorded try of the round, and whether a try ended it:
    ``stored``, ``budget``, ``ineligible``, ``unavailable`` or ``refused``
    (4.4)."""
    low, high = _round_row_dates(round_date)
    row = conn.execute(
        """
        SELECT MAX(round_try),
               MAX(outcome IN ('stored', 'budget', 'ineligible', 'unavailable',
                               'refused'))
        FROM forecast_downloads
        WHERE site_id = ? AND feed_id = ?
          AND local_date BETWEEN ? AND ?
          AND slot = 'round' AND round_date = ?
        """,
        (site_id, feed_id, low, high, round_date.isoformat()),
    ).fetchone()
    if row is None:
        return 0, False
    last_try = 0 if row[0] is None else int(row[0])
    return last_try, bool(row[1])


def _round_retries(
    conn: sqlite3.Connection, site_id: int, feed_id: int, round_date: date
) -> int:
    """How many quick retries (tries 2 to 4) the feed's round made (4.4)."""
    low, high = _round_row_dates(round_date)
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT round_try)
        FROM forecast_downloads
        WHERE site_id = ? AND feed_id = ?
          AND local_date BETWEEN ? AND ?
          AND slot = 'round' AND round_date = ? AND round_try >= 2
        """,
        (site_id, feed_id, low, high, round_date.isoformat()),
    ).fetchone()
    return 0 if row is None else int(row[0])


# ---------------------------------------------------------------------------
# The round (4.4, 4.5)
# ---------------------------------------------------------------------------


def round_state(
    conn: sqlite3.Connection, *, site_id: int, round_date: date
) -> RoundState:
    """Whether the round of ``round_date`` has settled, and its bound (4.5).

    Read-only. ``bound_utc`` is the round's start, the setting's local time
    on ``schedule_day``, plus ``ROUND_BOUND``: it exists for any date, a
    round that never ran and a date with no record yet included. ``settled``
    is whether every feed of the round has ended it: a ``stored`` try, a try
    ending ``budget``, ``ineligible``, ``unavailable`` or ``refused``, or
    try 4 recorded.
    A feed with no try, or with a further try still due, is not settled.
    On a day with no round (``schedule_day`` excludes the setting's local
    time, 4.4) ``settled`` is True and ``bound_utc`` the day's
    ``day_start_utc``: there is nothing to wait for.
    """
    day = schedule_day(conn, site_id=site_id, local_date=round_date)
    start = _round_start_utc(conn, day)
    if start is None:
        return RoundState(settled=True, bound_utc=day.day_start_utc)
    bound = start + ROUND_BOUND
    settled = True
    for feed in _schedule_feeds(conn, site_id):
        last_try, ended = _round_progress(conn, site_id, feed.feed_id, round_date)
        if not ended and last_try < ROUND_TRIES:
            settled = False
            break
    return RoundState(settled=settled, bound_utc=bound)


def round_try_stale(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    round_date: date,
    now_utc: datetime,
) -> bool:
    """Whether a round try of ``round_date`` must end before any call (4.4).

    True when ``now_utc`` is at or after the round's bound (``round_state``'s
    ``bound_utc``), or the feed already has a ``stored`` round row for the
    date, and always on a day with no round (4.4). Read-only. The caller
    cancels such a try with no call and no row, which also stops a try put
    back to pending after a restart.
    """
    day = schedule_day(conn, site_id=site_id, local_date=round_date)
    start = _round_start_utc(conn, day)
    if start is None or _as_utc(now_utc) >= start + ROUND_BOUND:
        return True
    low, high = _round_row_dates(round_date)
    row = conn.execute(
        """
        SELECT 1 FROM forecast_downloads
        WHERE site_id = ? AND feed_id = ?
          AND local_date BETWEEN ? AND ?
          AND slot = 'round' AND round_date = ? AND outcome = 'stored'
        LIMIT 1
        """,
        (site_id, feed_id, low, high, round_date.isoformat()),
    ).fetchone()
    return row is not None


def enqueue_due_round_tries(conn: sqlite3.Connection, *, now_utc: datetime) -> None:
    """Enqueue each feed's due round try, from the record of its earlier tries (4.4).

    Per enabled site, the round of the date whose ``schedule_day`` holds
    ``now_utc``, from its start until its bound. Try 1 is due at the start;
    try n + 1 at its offset, once try n is recorded without ending the
    round. Each try is one ``fetch_feed`` job keyed ``round:{feed_id}``
    with ``max_retries`` 0; an enqueue that meets a live try of the feed
    creates nothing. A site whose zone does not load is skipped this tick; a
    day with no round (4.4) is skipped with no warning.
    """
    now = _as_utc(now_utc)
    for site_id in _enabled_site_ids(conn):
        try:
            day = _tick_day(conn, site_id, now)
            if day is None:
                continue
            start = _round_start_utc(conn, day)
        except (ZoneInfoNotFoundError, ValueError):
            logger.warning(
                "scheduler: unusable round schedule site=%s; skipping", site_id
            )
            continue
        if start is None:
            continue
        bound = start + ROUND_BOUND
        if not start <= now < bound:
            continue
        for feed in _schedule_feeds(conn, site_id):
            last_try, ended = _round_progress(
                conn, site_id, feed.feed_id, day.local_date
            )
            if ended or last_try >= ROUND_TRIES:
                continue
            next_try = last_try + 1
            if now < start + _TRY_OFFSETS[next_try - 1]:
                continue
            logger.debug(
                "scheduler due round try site=%s feed=%s try=%s",
                site_id,
                feed.feed_id,
                next_try,
            )
            enqueue_if_absent(
                conn,
                "fetch_feed",
                site_id,
                f"round:{feed.feed_id}",
                {
                    "feed_id": feed.feed_id,
                    "slot": "round",
                    "round_date": day.local_date.isoformat(),
                    "round_try": next_try,
                },
                max_retries=0,
            )


# ---------------------------------------------------------------------------
# Daytime downloads (5.1)
# ---------------------------------------------------------------------------


def _slots_at(
    day: DayRecord, clock_minutes: Iterable[int]
) -> list[tuple[str, datetime]]:
    """Each minutes-after-midnight as ``("HH:MM", instant)``. A wall time
    outside the day's stored bounds is not a slot of the day."""
    slots: list[tuple[str, datetime]] = []
    for minutes in clock_minutes:
        hour, minute = divmod(minutes, 60)
        try:
            instant = local_clock_utc(day, hour, minute)
        except ValueError:
            continue
        slots.append((f"{hour:02d}:{minute:02d}", instant))
    return slots


def _daytime_slots(
    day: DayRecord, round_minutes: int, interval_minutes: int
) -> list[tuple[str, datetime]]:
    """The feed's daytime slots on ``day``: the round's local time plus n times
    its interval, n = 1, 2, ..., while the slot falls on the day, each as
    ``("HH:MM", instant)``. A wall time outside the day's stored bounds is
    not a slot of the day."""
    return _slots_at(
        day, range(round_minutes + interval_minutes, _MINUTES_PER_DAY, interval_minutes)
    )


def _package_daytime_slots(day: DayRecord) -> list[tuple[str, datetime]]:
    """The meteoblue package's daytime slots on ``day``."""
    return _slots_at(day, [h * 60 + m for h, m in METEOBLUE_DAYTIME_LOCAL_TIMES])


def _slot_covered(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    local_date: date,
    slot: str,
    instant: datetime,
) -> bool:
    """Whether a try of the feed has ``slot`` on the date, or an
    ``unscheduled`` try on the date started at or after the slot's instant
    (an enable-time or command-line fetch stands for the slots before it)."""
    row = conn.execute(
        """
        SELECT 1 FROM forecast_downloads
        WHERE site_id = ? AND feed_id = ? AND local_date = ?
          AND (slot = ?
               OR (slot = 'unscheduled'
                   AND julianday(started_at) >= julianday(?)))
        LIMIT 1
        """,
        (site_id, feed_id, local_date.isoformat(), slot, isoformat_utc(instant)),
    ).fetchone()
    return row is not None


def enqueue_due_daytime_downloads(
    conn: sqlite3.Connection, *, now_utc: datetime
) -> None:
    """Enqueue each feed's latest due daytime slot (5.1).

    Per enabled site, the slots of the date whose ``schedule_day`` holds
    ``now_utc``: the meteoblue package's set times, every other feed's times
    counted from the round by its interval. A slot is due when its instant
    has passed, no try of the feed has that slot on the date, and no
    ``unscheduled`` try of the feed on the date started at or after it. Only
    the latest due slot is fetched, so
    earlier missed slots are skipped. meteoblue's package loses one slot per
    round retry of the date, latest first (4.4). The job is ``fetch_feed``
    keyed ``fetch:{feed_id}``, with the due-feed loop's retry cap and failure
    cooldown. A site whose zone does not load is skipped this tick.
    """
    now = _as_utc(now_utc)
    for site_id in _enabled_site_ids(conn):
        try:
            day = _tick_day(conn, site_id, now)
            if day is None:
                continue
            hour, minute = scoring_round_local_time(conn)
            planned = [
                (
                    feed,
                    _package_daytime_slots(day)
                    if feed.package
                    else _daytime_slots(day, hour * 60 + minute, feed.interval_minutes),
                )
                for feed in _schedule_feeds(conn, site_id)
            ]
        except (ZoneInfoNotFoundError, ValueError):
            logger.warning(
                "scheduler: unusable daytime schedule site=%s; skipping", site_id
            )
            continue
        for feed, slots in planned:
            if feed.package and slots:
                dropped = _round_retries(conn, site_id, feed.feed_id, day.local_date)
                slots = slots[: max(0, len(slots) - dropped)]
            due = [slot for slot in slots if slot[1] <= now]
            if not due:
                continue
            label, instant = due[-1]
            if _slot_covered(
                conn, site_id, feed.feed_id, day.local_date, label, instant
            ):
                continue
            logger.debug(
                "scheduler due daytime slot site=%s feed=%s slot=%s",
                site_id,
                feed.feed_id,
                label,
            )
            enqueue_if_absent_with_cooldown(
                conn,
                "fetch_feed",
                site_id,
                f"fetch:{feed.feed_id}",
                {
                    "feed_id": feed.feed_id,
                    "slot": label,
                    "local_date": day.local_date.isoformat(),
                },
                cooldown=DAYTIME_FAILURE_COOLDOWN,
                max_retries=DAYTIME_MAX_RETRIES,
            )
