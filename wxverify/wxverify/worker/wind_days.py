"""The wind-history lane: raw station wind records, day by day.

One ``fetch_obs`` job with the job key ``wind-days`` runs one bounded chunk
for one site. It fetches each station-day's raw 5-minute records and derives
the pair-max hourly station wind (``max_adjacent_pair_mean_wind``). It keeps
today and yesterday current, today's row on the daytime cadence and the
evening window's slots (``EVENING_SLOTS``), and while the ``history_all`` hold
is held it finishes past days with the records they have.

Today's 5-minute download takes the refused-access hold
(:mod:`wxverify.worker.access_hold`), checked per row inside the reservation;
only ``history_all`` keeps the lane's own hold. Every try of today's row is
logged to the collection try log
(:func:`wxverify.worker.close_chain.record_collection_try`) and in one INFO
line.

The lane's share of the weather.com budget is in
:mod:`wxverify.collection.wind_quota`.
"""

from __future__ import annotations

import contextlib
import logging
import sqlite3
import statistics
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from wxverify.collection.budget import (
    Reservation,
    is_refundable_transport_error,
    next_billing_window,
    refund_budget,
    reserve_budget,
    write_after_reservation,
)
from wxverify.collection.wind_quota import (
    add_lane_calls,
    backfill_headroom,
    lane_counter_key,
)
from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.secrets import resolve_secret
from wxverify.core.timeutil import (
    floor_hour,
    isoformat_utc,
    isoformat_utc_micro,
    local_day_slots,
    parse_utc,
    utc_now,
)
from wxverify.core.units import kmh_to_ms
from wxverify.db.connection import Database, FencedWriter, StaleGenerationError
from wxverify.db.day_guard_log import warn_refused_dates, warn_refused_instants
from wxverify.db.queue import enqueue_if_absent
from wxverify.db.site_days import (
    DayRecord,
    WriteKind,
    check_date_write,
    check_day_write,
    current_day,
    daily_close_on,
    day_record,
    ensure_site_days,
    filter_day_writes,
    local_clock_utc,
)
from wxverify.db.station_readings import StoreResult, store_readings
from wxverify.db.tz_generations import published_generation_clause
from wxverify.db.wind_basis import (
    PAIR_MAX_WIND_SQL,
    AuthHold,
    AuthHoldEndpoint,
    WindEndpoint,
    clear_auth_hold,
    count_missed_final,
    read_auth_hold,
    read_auth_holds,
    write_auth_hold,
)
from wxverify.forecast.wind_blend import TRAINING_DAYS
from wxverify.obs.hourly_values import StationReading
from wxverify.obs.pws_adapter import (
    UpstreamPayloadError,
    fetch_all_1day,
    fetch_history_all,
    is_wind_history_no_content,
    wait_for_backfill_slot,
)
from wxverify.obs.wind_pairs import MAX_PAIR_GAP_SECONDS, WindRecord, pair_max_by_hour
from wxverify.scoring.consensus import materialize_consensus, write_station_observation
from wxverify.settings.keys import get_number_setting
from wxverify.verification.truth import mark_daily_truth_stale
from wxverify.worker.access_hold import (
    AccessHeld,
    check_access_hold,
    clear_access_refused,
    record_access_refused,
    refused_kinds,
)
from wxverify.worker.close_chain import record_collection_try
from wxverify.worker.control import JobCancelled, JobControl, JobDeferred
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    clear_domain_backoff,
    record_http_backoff,
    source_domain,
)
from wxverify.worker.station_pacing import pace_station_call, weathercom_call_lock

logger = logging.getLogger(__name__)

WIND_DAYS_JOB_KEY: Final = "wind-days"
#: The scheduler's success cooldown between two lane jobs of one site.
WIND_DAYS_SPACING: Final = timedelta(minutes=5)

#: The evening window's slots, local (hour, minute), in order (plan 4.2):
#: 23:00; ten retries 23:06 to 23:51; 23:56; three retries 23:57 to 23:59.
EVENING_SLOTS: Final[tuple[tuple[int, int], ...]] = (
    (23, 0),
    (23, 6),
    (23, 11),
    (23, 16),
    (23, 21),
    (23, 26),
    (23, 31),
    (23, 36),
    (23, 41),
    (23, 46),
    (23, 51),
    (23, 56),
    (23, 57),
    (23, 58),
    (23, 59),
)
#: The slot that always runs, even after a good download (plan 4.2).
_LATE_SLOT: Final = (23, 56)

_CHUNK_CALLS: Final = 6
_CHUNK_ROWS: Final = 24
_MAX_ATTEMPTS: Final = 3
_TRANSPORT_DEFER: Final = timedelta(minutes=15)
_HEADROOM_RETRY: Final = timedelta(hours=1)
_STORE_MARGIN: Final = timedelta(hours=24)
_FUTURE_SLACK: Final = timedelta(minutes=5)
_PAIR_LOOKBACK: Final = timedelta(seconds=MAX_PAIR_GAP_SECONDS)
_FIRST_RETRY: Final = timedelta(hours=1)
_LATER_RETRY: Final = timedelta(hours=4)
_EMPTY_YESTERDAY_RETRY: Final = timedelta(hours=4)
_RECONCILE_RATIO: Final = 0.9
_RECONCILE_POOL: Final = 7
_RECONCILE_MIN: Final = 3
_ONE_HOUR: Final = timedelta(hours=1)
_ONE_DAY: Final = timedelta(days=1)
_PAYLOAD_ROW_FAULTS: Final = frozenset(
    {"json_decode", "invalid_structure", "provider_error"}
)

#: The due selector, shared by the lane, the scheduler and ``wind_days_due``.
#: An unreadable ``next_attempt_at`` or ``refetch_at`` fails open.
WIND_DUE_SQL: Final = """
SELECT d.station_id, d.local_date, d.status, d.attempts, st.pws_station_id,
       (d.local_date >= :yesterday
        OR (d.refetch_at IS NOT NULL AND d.refetched = 0)) AS exempt
FROM station_wind_days d JOIN stations st ON st.id = d.station_id
WHERE st.site_id = :site_id AND st.enabled = 1
  AND d.local_date <= :today
  AND (((d.status IN ('pending','partial') OR (d.status='failed' AND d.attempts < 3))
        AND (d.next_attempt_at IS NULL OR julianday(d.next_attempt_at) IS NULL
             OR julianday(d.next_attempt_at) <= julianday(:now)))
    OR (d.status IN ('fetched','unavailable') AND d.refetched = 0
        AND d.refetch_at IS NOT NULL
        AND (julianday(d.refetch_at) IS NULL
             OR julianday(d.refetch_at) <= julianday(:now))))
  AND (CASE WHEN d.local_date = :today THEN :today_open ELSE :history_open END) = 1
ORDER BY exempt DESC, d.local_date ASC, d.station_id ASC
LIMIT :limit
"""

#: A due live obs job ends a chunk before its next non-exempt call.
_LIVE_JOB_DUE_SQL: Final = """
SELECT 1 FROM jobs
WHERE status = 'pending'
  AND ((type = 'fetch_obs' AND job_key = 'obs') OR type = 'fetch_current_obs')
  AND (next_attempt_at IS NULL OR julianday(next_attempt_at) <= julianday(:now))
LIMIT 1
"""

_RETRYABLE_SQL: Final = (
    "(d.status IN ('pending','partial') OR (d.status = 'failed' AND d.attempts < 3))"
)

#: The past rows the finish step closes while ``history_all`` is held, shared
#: by the finish step, its re-check and ``wind_lane_due``. The status clause is
#: the unsettled-days clause of wind training; a non-canonical date never matches.
_FINISHABLE_SQL: Final = """
SELECT d.station_id, d.local_date, st.pws_station_id
FROM station_wind_days d JOIN stations st ON st.id = d.station_id
WHERE st.site_id = :site_id AND st.enabled = 1
  AND d.local_date < :today AND d.local_date >= :floor
  AND date(d.local_date) = d.local_date
  AND (d.status IN ('pending','partial') OR (d.status = 'failed' AND d.attempts < 3)
       OR (d.refetch_at IS NOT NULL AND d.refetched = 0))
  AND (:station_id IS NULL OR d.station_id = :station_id)
  AND (:local_date IS NULL OR d.local_date = :local_date)
ORDER BY d.local_date, d.station_id
LIMIT :limit
"""

_RowOutcome = Literal["skipped", "fetched", "closed"]
_FailureOutcome = Literal["fetched", "closed", "reraise"]
DeferralReason = Literal["backoff", "call_limit"]


class WindHeadroomExhausted(Exception):
    """The backfill headroom is below one call. Nothing has been written."""


class ReservationDeferred(JobDeferred):
    """A reservation met the domain back-off or the call limit (plan 4.4).

    ``reason`` is ``backoff`` (``check_domain_backoff``) or ``call_limit``
    (``reserve_budget``); ``next_attempt_at`` is the deferral's end. A
    subclass of ``JobDeferred``, so a past row's deferral still defers the
    job; ``_run_row`` catches it for today's row.
    """

    def __init__(self, reason: DeferralReason, next_attempt_at: str) -> None:
        super().__init__(next_attempt_at)
        self.reason: DeferralReason = reason


@dataclass(frozen=True, slots=True)
class _DayClock:
    """One site-day's evening slots and end, for today's row (plan 4.2).

    ``slots`` are the UTC instants of ``EVENING_SLOTS`` that fall inside the
    day, in order, each once; ``labels`` their local ``HH:MM``. ``late`` is
    the 23:56 slot (the first kept slot at or after it), or None.
    """

    slots: tuple[datetime, ...]
    labels: tuple[str, ...]
    late: datetime | None
    end: datetime

    @property
    def cap(self) -> datetime:
        """The daytime cap: the 23:00 slot, or the day's end with no slot."""
        return self.slots[0] if self.slots else self.end

    @property
    def done(self) -> datetime:
        """Where a row done for the day waits: the day's end plus one hour."""
        return self.end + _ONE_HOUR


@dataclass(frozen=True, slots=True)
class _TodayTry:
    """One try of today's row, read before the try's outcome is written."""

    site_id: int
    station_id: int
    local_date: date
    tried: datetime
    clock: _DayClock | None
    slot: str


@dataclass(frozen=True, slots=True)
class _TryLog:
    """A logged try of today's row, for its INFO line after the commit."""

    site_id: int
    station_id: int
    local_date: date
    slot: str
    outcome: str
    reason: str | None
    readings: int
    #: The try ran at or after the daytime cap (the evening window).
    in_window: bool
    cap: datetime | None


@dataclass(frozen=True)
class DueWindDay:
    """One station-day the selector returned."""

    station_id: int
    local_date: date
    status: str
    attempts: int
    pws_station_id: str
    #: On or after yesterday, or a pending reconcile refetch: no headroom check.
    exempt: bool


def endpoint_for(local_date: date, today: date) -> WindEndpoint:
    """Today's row calls ``all_1day``; every past date calls ``history_all``."""
    return "all_1day" if local_date == today else "history_all"


def open_endpoints(
    holds: Mapping[AuthHoldEndpoint, AuthHold],
    *,
    probing_open: bool,
) -> frozenset[WindEndpoint]:
    """The endpoints the lane may call now.

    ``history_all`` is open with no lane hold, or with a ``probing`` one when
    asked. ``all_1day`` has no lane hold and is always open: the
    ``five_minute`` refused-access hold is checked per row inside the
    reservation (``_reserve_wind_day_call``, plan 4.4), not here.
    """
    opened: set[WindEndpoint] = {"all_1day"}
    history = holds.get("history_all")
    if history is None or (probing_open and history.status == "probing"):
        opened.add("history_all")
    return frozenset(opened)


def wind_days_due(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    today: date,
    now: datetime,
    open_endpoints: Collection[WindEndpoint],
    limit: int,
) -> list[DueWindDay]:
    """Run ``WIND_DUE_SQL``: exempt rows first, then by date, then by station."""
    rows = conn.execute(
        WIND_DUE_SQL,
        {
            "site_id": site_id,
            "today": today.isoformat(),
            "yesterday": (today - _ONE_DAY).isoformat(),
            "now": isoformat_utc(now),
            "today_open": 1 if "all_1day" in open_endpoints else 0,
            "history_open": 1 if "history_all" in open_endpoints else 0,
            "limit": limit,
        },
    ).fetchall()
    due: list[DueWindDay] = []
    for row in rows:
        try:
            local_date = date.fromisoformat(str(row["local_date"]))
        except ValueError:
            logger.warning(
                "wind day with an unreadable date skipped site=%s station=%s",
                site_id,
                row["station_id"],
            )
            continue
        due.append(
            DueWindDay(
                station_id=int(row["station_id"]),
                local_date=local_date,
                status=str(row["status"]),
                attempts=int(row["attempts"]),
                pws_station_id=str(row["pws_station_id"]),
                exempt=bool(row["exempt"]),
            )
        )
    return due


def _canonical_date(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def ensure_wind_days(
    conn: sqlite3.Connection, site_id: int, tz: ZoneInfo, today: date
) -> None:
    """Give every enabled station a row for each date from the first observation.

    The range runs from the local date of the site's earliest observation (any
    variable), capped at today, through today. A NULL or unreadable MIN gives
    today only, with a WARNING. Rows whose ``local_date`` is not a canonical
    ``YYYY-MM-DD`` are deleted, with a WARNING and the count.
    """
    first = today
    row = conn.execute(
        "SELECT MIN(valid_at) AS first FROM observations WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    raw: object = None if row is None else row["first"]
    if isinstance(raw, str):
        try:
            first = min(today, parse_utc(raw).astimezone(tz).date())
        except (ValueError, OverflowError):
            logger.warning(
                "wind days start at today: unreadable first observation site=%s",
                site_id,
            )
    else:
        logger.warning(
            "wind days start at today: no first observation site=%s", site_id
        )
    stations = [
        int(station["id"])
        for station in conn.execute(
            "SELECT id FROM stations WHERE site_id = ? AND enabled = 1 ORDER BY id",
            (site_id,),
        )
    ]
    stamp = isoformat_utc(utc_now())
    dates = [
        (first + timedelta(days=offset)).isoformat()
        for offset in range((today - first).days + 1)
    ]
    conn.executemany(
        "INSERT OR IGNORE INTO station_wind_days"
        " (station_id, local_date, status, updated_at) VALUES (?, ?, 'pending', ?)",
        [(station_id, day, stamp) for station_id in stations for day in dates],
    )
    stored = conn.execute(
        """
        SELECT DISTINCT d.local_date AS local_date
        FROM station_wind_days d JOIN stations st ON st.id = d.station_id
        WHERE st.site_id = ?
        """,
        (site_id,),
    ).fetchall()
    deleted = 0
    for value in (row["local_date"] for row in stored):
        if _canonical_date(value):
            continue
        deleted += conn.execute(
            "DELETE FROM station_wind_days WHERE local_date IS ?"
            " AND station_id IN (SELECT id FROM stations WHERE site_id = ?)",
            (value, site_id),
        ).rowcount
    if deleted:
        logger.warning(
            "wind days with an unreadable date deleted site=%s rows=%s",
            site_id,
            deleted,
        )


def _missing_today_row(conn: sqlite3.Connection, site_id: int, today: date) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM stations st
        WHERE st.site_id = ? AND st.enabled = 1
          AND NOT EXISTS (
              SELECT 1 FROM station_wind_days d
              WHERE d.station_id = st.id AND d.local_date = ?
          )
        LIMIT 1
        """,
        (site_id, today.isoformat()),
    ).fetchone()
    return row is not None


def _lane_today(
    conn: sqlite3.Connection,
    site_id: int,
    tz: ZoneInfo,
    now: datetime,
    *,
    ensure: bool,
) -> tuple[date, bool]:
    """The lane's today, and whether today's row may be tried (plan 4.2).

    While the close is off, today is ``now``'s local date in the site's zone
    ``tz``, always open. While it is on, today is the date of P1's record
    that holds ``now`` (with ``ensure``, after creating the site's missing
    records). With no such record the site has no current day: today's row
    is closed, and today stays ``now``'s date in ``tz`` for the past-day
    paths only, as before the close.
    """
    if not daily_close_on(conn):
        return now.astimezone(tz).date(), True
    if ensure:
        ensure_site_days(conn, site_id, now_utc=now)
    record = current_day(conn, site_id, now_utc=now)
    if record is None:
        return now.astimezone(tz).date(), False
    return record.local_date, True


def _clock_from_record(record: DayRecord) -> _DayClock:
    """The evening slots of a day record, by P1's ``local_clock_utc`` rule.

    A slot that lies outside the day is left out; one that maps onto an
    instant already kept (a wall time the clocks skipped) is kept once.
    """
    slots: list[datetime] = []
    labels: list[str] = []
    late: datetime | None = None
    for hour, minute in EVENING_SLOTS:
        try:
            instant = local_clock_utc(record, hour, minute)
        except ValueError:
            continue
        if slots and instant <= slots[-1]:
            continue
        slots.append(instant)
        labels.append(f"{hour:02d}:{minute:02d}")
        if late is None and (hour, minute) >= _LATE_SLOT:
            late = instant
    return _DayClock(
        slots=tuple(slots), labels=tuple(labels), late=late, end=record.day_end_utc
    )


def _close_off_record(local_date: date, tz_name: str) -> DayRecord:
    """A stand-in day record for a close-off day: ``local_day_slots``' bounds
    in ``tz_name``, so P1's ``local_clock_utc`` converts its times."""
    start, end, _ = local_day_slots(local_date, tz_name)
    return DayRecord(
        site_id=0,
        local_date=local_date,
        state="open",
        day_start_utc=start,
        day_end_utc=end,
        timezone=tz_name,
        zone_changeover=False,
        attempts=0,
        retry_at=None,
        last_failure=None,
        missed_reason=None,
        done_at=None,
    )


def evening_slot_instants(local_date: date, tz_name: str) -> tuple[datetime, ...]:
    """The UTC instants of the evening slots of ``local_date`` in ``tz_name``.

    The close-off conversion (plan 4.2): each ``EVENING_SLOTS`` wall time on
    the date in the zone, by P1's ``local_clock_utc`` rule (a skipped wall
    time takes the instant the clocks jump, a repeated one the earlier
    instant), in order and each once. Pure.
    """
    return _clock_from_record(_close_off_record(local_date, tz_name)).slots


def _day_clock(
    conn: sqlite3.Connection, site_id: int, local_date: date, tz_name: str
) -> _DayClock | None:
    """The clock of today's row dated ``local_date`` (plan 4.2).

    While the close is on, from P1's record of that date, in the zone saved
    on it; None when the site has no such record (no slot is computed and
    nothing falls back to ``sites.timezone``). While it is off, the date in
    ``tz_name`` (``sites.timezone``).
    """
    if daily_close_on(conn):
        record = day_record(conn, site_id, local_date)
        return None if record is None else _clock_from_record(record)
    return _clock_from_record(_close_off_record(local_date, tz_name))


def wind_evening_due(
    conn: sqlite3.Connection, site_id: int, tz: ZoneInfo, *, now: datetime
) -> bool:
    """True while one of the site's today rows is due in the evening window.

    The scheduler passes no success spacing then (plan 4.2), so the 5-minute
    spacing after the 23:56 job cannot block 23:57 to 23:59. Read-only.
    """
    today, today_open = _lane_today(conn, site_id, tz, now, ensure=False)
    if not today_open:
        return False
    clock = _day_clock(conn, site_id, today, tz.key)
    if clock is None or not clock.cap <= now < clock.end:
        return False
    return bool(
        wind_days_due(
            conn, site_id, today=today, now=now, open_endpoints=("all_1day",), limit=1
        )
    )


def wind_lane_due(
    conn: sqlite3.Connection, site_id: int, tz: ZoneInfo, *, now: datetime
) -> bool:
    """True when the site's lane has work: the scheduler's due test (plan §8.13).

    Today's rows are due on their own schedule only; the ``five_minute``
    hold is not read here (plan 4.4). While the close is on and no record
    holds ``now``, no today row is due (4.2).
    """
    today, today_open = _lane_today(conn, site_id, tz, now, ensure=False)
    holds = read_auth_holds(conn)
    opened = set(open_endpoints(holds, probing_open=True))
    if not today_open:
        opened.discard("all_1day")
    if wind_days_due(
        conn, site_id, today=today, now=now, open_endpoints=opened, limit=1
    ):
        return True
    history = holds.get("history_all")
    if (
        history is not None
        and history.status == "held"
        and conn.execute(
            _FINISHABLE_SQL,
            {
                "site_id": site_id,
                "today": today.isoformat(),
                "floor": _finish_floor(today),
                "station_id": None,
                "local_date": None,
                "limit": 1,
            },
        ).fetchone()
        is not None
    ):
        return True
    return _missing_today_row(conn, site_id, today)


# --- the job ---------------------------------------------------------------


@dataclass(frozen=True)
class _JobStart:
    tz_name: str
    holds: dict[AuthHoldEndpoint, AuthHold]
    today: date
    #: False while the close is on and no record holds the start time.
    today_open: bool


def _start_job(conn: sqlite3.Connection, site_id: int) -> _JobStart:
    row = conn.execute(
        "SELECT timezone FROM sites WHERE id = ? AND enabled = 1", (site_id,)
    ).fetchone()
    if row is None:
        raise JobCancelled()
    tz_name = str(row["timezone"])
    try:
        tz = ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError):
        logger.warning("wind lane cancelled: unusable time zone site=%s", site_id)
        raise JobCancelled() from None
    today, today_open = _lane_today(conn, site_id, tz, utc_now(), ensure=True)
    ensure_wind_days(conn, site_id, tz, today)
    holds = read_auth_holds(conn)
    return _JobStart(tz_name=tz_name, holds=holds, today=today, today_open=today_open)


async def run_wind_days(
    db: Database,
    writer: FencedWriter,
    site_id: int,
    *,
    client: httpx.AsyncClient | None = None,
) -> None:
    """Run one bounded chunk of the site's wind-history lane (plan §8.3).

    ``client`` is for tests; the job opens its own ``httpx.AsyncClient``.
    """
    start = await writer.write(lambda conn: _start_job(conn, site_id))
    if client is not None:
        await _run_fetch_chunk(db, writer, site_id, start, client)
        return
    async with httpx.AsyncClient() as owned:
        await _run_fetch_chunk(db, writer, site_id, start, owned)


# --- fetch -----------------------------------------------------------------


@dataclass
class _Chunk:
    db: Database
    writer: FencedWriter
    client: httpx.AsyncClient
    site_id: int
    tz_name: str
    today: date
    api_key: str | None = None
    calls: int = 0

    def key(self) -> str:
        if self.api_key is None:
            resolved = resolve_secret("weathercom")
            if not resolved:
                raise RuntimeError("weathercom key is not configured")
            self.api_key = resolved
        return self.api_key


async def _run_fetch_chunk(
    db: Database,
    writer: FencedWriter,
    site_id: int,
    start: _JobStart,
    client: httpx.AsyncClient,
) -> None:
    now = utc_now()
    today = start.today
    chunk = _Chunk(
        db=db,
        writer=writer,
        client=client,
        site_id=site_id,
        tz_name=start.tz_name,
        today=today,
    )
    await _finish_waiting_days(chunk, start)
    # Only a lane hold can be probing, so only history_all is ever probed.
    for endpoint, hold in start.holds.items():
        if hold.status != "probing":
            continue
        probe_rows = await db.read(
            lambda conn, ep=endpoint: wind_days_due(
                conn, site_id, today=today, now=now, open_endpoints=(ep,), limit=1
            )
        )
        if not probe_rows:
            continue
        outcome = await _run_row(
            chunk, probe_rows[0], exempt=True, probe=endpoint, position=0, total=1
        )
        if outcome != "skipped":
            chunk.calls += 1
    # A probing endpoint gets its one probe per chunk; its other rows wait.
    holds = await db.read(read_auth_holds)
    opened = set(open_endpoints(holds, probing_open=False))
    if not start.today_open:
        # The close is on and no record holds now: no today try (4.2).
        opened.discard("all_1day")
    rows = await db.read(
        lambda conn: wind_days_due(
            conn,
            site_id,
            today=today,
            now=now,
            open_endpoints=frozenset(opened),
            limit=_CHUNK_ROWS,
        )
    )
    exhausted = False
    for position, row in enumerate(rows):
        if chunk.calls >= _CHUNK_CALLS:
            break
        endpoint = endpoint_for(row.local_date, today)
        if endpoint not in opened:
            continue
        if not row.exempt:
            if exhausted:
                break
            if await db.read(
                lambda conn: (
                    conn.execute(
                        _LIVE_JOB_DUE_SQL, {"now": isoformat_utc(utc_now())}
                    ).fetchone()
                    is not None
                )
            ):
                break
        try:
            outcome = await _run_row(
                chunk,
                row,
                exempt=row.exempt,
                probe=None,
                position=position,
                total=len(rows),
            )
        except WindHeadroomExhausted:
            # Exempt rows sort first, so every row left is non-exempt.
            exhausted = True
            break
        if outcome != "skipped":
            chunk.calls += 1
        if outcome == "closed":
            opened.discard(endpoint)
    if chunk.calls == 0 and exhausted:
        wake = await db.read(
            lambda conn: _headroom_wake(
                conn, site_id, today=today, now=now, opened=frozenset(opened)
            )
        )
        raise JobDeferred(isoformat_utc(wake))


def _headroom_wake(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    today: date,
    now: datetime,
    opened: frozenset[WindEndpoint],
) -> datetime:
    """min(now + 1 h, the next billing window, the earliest future exempt due)."""
    candidates = [now + _HEADROOM_RETRY]
    source = conn.execute(
        "SELECT billing_tz FROM sources WHERE source = 'weathercom'"
    ).fetchone()
    if source is not None:
        # An unloadable billing time zone drops the term; one hour still bounds it.
        with contextlib.suppress(ZoneInfoNotFoundError, ValueError):
            window = next_billing_window(str(source["billing_tz"]))
            candidates.append(parse_utc(window))
    rows = conn.execute(
        f"""
        SELECT d.local_date, d.next_attempt_at, d.refetch_at,
               {_RETRYABLE_SQL} AS retryable,
               (d.status IN ('fetched','unavailable') AND d.refetched = 0
                AND d.refetch_at IS NOT NULL) AS refetch_pending
        FROM station_wind_days d JOIN stations st ON st.id = d.station_id
        WHERE st.site_id = ? AND st.enabled = 1 AND d.local_date <= ?
          AND (d.local_date >= ? OR (d.refetch_at IS NOT NULL AND d.refetched = 0))
        """,
        (site_id, today.isoformat(), (today - _ONE_DAY).isoformat()),
    ).fetchall()
    for row in rows:
        try:
            local_date = date.fromisoformat(str(row["local_date"]))
        except ValueError:
            continue
        if endpoint_for(local_date, today) not in opened:
            continue
        for due_col, flag in (
            ("next_attempt_at", "retryable"),
            ("refetch_at", "refetch_pending"),
        ):
            raw = row[due_col]
            if not row[flag] or raw is None:
                continue
            try:
                due = parse_utc(str(raw))
            except ValueError:
                continue
            if due > now:
                candidates.append(due)
    return min(candidates)


def _reserve_wind_day_call(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    *,
    exempt: bool,
    todays_row: bool = False,
) -> Reservation | None:
    """Reserve one weather.com call for a station-day, in one transaction.

    Site disabled -> ``JobCancelled``; station disabled or moved -> None (skip
    the row); then domain backoff (``ReservationDeferred`` ``backoff``), for
    today's row the ``five_minute`` refused-access hold (``AccessHeld``, plan
    4.4), the backfill headroom (non-exempt only, ``WindHeadroomExhausted``
    below one call), the budget (``ReservationDeferred`` ``call_limit``), and
    the lane counter that the reservation belongs to.
    """
    site = conn.execute(
        "SELECT 1 FROM sites WHERE id = ? AND enabled = 1", (site_id,)
    ).fetchone()
    if site is None:
        raise JobCancelled()
    station = conn.execute(
        "SELECT 1 FROM stations WHERE id = ? AND site_id = ? AND enabled = 1",
        (station_id, site_id),
    ).fetchone()
    if station is None:
        return None
    try:
        check_domain_backoff(conn, source_domain("weathercom"))
    except JobDeferred as exc:
        raise ReservationDeferred("backoff", exc.next_attempt_at) from exc
    if todays_row:
        check_access_hold(conn, "five_minute")
    if not exempt:
        headroom = backfill_headroom(conn, now=utc_now())
        if headroom is None or headroom < 1:
            raise WindHeadroomExhausted()
    try:
        reservation = reserve_budget(conn, "weathercom", 1)
    except JobDeferred as exc:
        raise ReservationDeferred("call_limit", exc.next_attempt_at) from exc
    add_lane_calls(conn, lane_counter_key(exempt=exempt), reservation.billing_day, 1)
    return reservation


async def _run_row(
    chunk: _Chunk,
    row: DueWindDay,
    *,
    exempt: bool,
    probe: AuthHoldEndpoint | None,
    position: int,
    total: int,
) -> _RowOutcome:
    """Reserve, call and persist one station-day; the error table of §8.7.

    For today's row a try that meets the ``five_minute`` hold (``AccessHeld``)
    or a reservation deferral (``ReservationDeferred``) sends nothing: one
    write moves the row and logs the try (plan 4.4, 4.5), and the row is
    ``"skipped"``, not counted as a call. A past row's deferral propagates.
    """
    key = chunk.key()
    await pace_station_call(chunk.site_id, row.station_id, chunk.calls)
    if not exempt:
        await wait_for_backfill_slot()
    site_id = chunk.site_id
    tz_name = chunk.tz_name
    todays_row = row.local_date == chunk.today
    async with weathercom_call_lock():
        try:
            reservation = await chunk.writer.write(
                lambda conn: _reserve_wind_day_call(
                    conn,
                    site_id,
                    row.station_id,
                    exempt=exempt,
                    todays_row=todays_row,
                )
            )
        except AccessHeld as held:
            if not todays_row:
                raise
            until = held.until
            entry = await chunk.writer.write(
                lambda conn: _record_held_try(
                    conn,
                    site_id=site_id,
                    station_id=row.station_id,
                    local_date=row.local_date,
                    tz_name=tz_name,
                    until=until,
                )
            )
            _log_try(entry)
            return "skipped"
        except ReservationDeferred as deferred:
            if not todays_row:
                raise
            reason = deferred.reason
            deferral_end = deferred.next_attempt_at
            entry = await chunk.writer.write(
                lambda conn: _record_deferred_try(
                    conn,
                    site_id=site_id,
                    station_id=row.station_id,
                    local_date=row.local_date,
                    tz_name=tz_name,
                    reason=reason,
                    deferral_end=deferral_end,
                )
            )
            _log_try(entry)
            return "skipped"
        if reservation is None:
            return "skipped"
        logger.debug(
            "wind day fetch site=%s station=%s date=%s exempt=%s probe=%s",
            site_id,
            row.station_id,
            row.local_date,
            exempt,
            probe,
        )
        records: Sequence[WindRecord | StationReading]
        no_content = False
        try:
            if row.local_date == chunk.today:
                parsed = await fetch_all_1day(
                    row.pws_station_id,
                    key,
                    client=chunk.client,
                    backfill=not exempt,
                )
                records = parsed.readings
            else:
                records = await fetch_history_all(
                    row.pws_station_id,
                    key,
                    local_date=row.local_date,
                    client=chunk.client,
                    backfill=not exempt,
                )
        except Exception as exc:
            if not is_wind_history_no_content(exc):
                message = _row_message(row, exc)
                if probe is not None:
                    await _probe_failed(chunk, row, probe, reservation, exc, message)
                    return "closed"
                outcome = await _fetch_failed(
                    chunk, row, reservation, exc, message, exempt=exempt
                )
                if outcome == "reraise":
                    exc.add_note(
                        f"station={row.pws_station_id} progress={position + 1}/{total}"
                    )
                    raise
                return outcome
            records = []
            no_content = True
        try:
            entry = await write_after_reservation(
                chunk.db,
                chunk.writer,
                lambda conn: _persist_day(
                    conn,
                    site_id=site_id,
                    station_id=row.station_id,
                    local_date=row.local_date,
                    tz_name=chunk.tz_name,
                    today=chunk.today,
                    records=records,
                    probe=probe,
                    no_content=no_content,
                ),
                reservation,
            )
        except (StaleGenerationError, sqlite3.OperationalError, JobControl):
            raise
        except Exception as exc:
            message = _row_message(row, exc)
            await write_after_reservation(
                chunk.db,
                chunk.writer,
                lambda conn: _row_failure(
                    conn,
                    site_id=site_id,
                    station_id=row.station_id,
                    local_date=row.local_date,
                    tz_name=chunk.tz_name,
                    today=chunk.today,
                    message=message,
                ),
                reservation,
            )
            exc.add_note(
                f"station={row.pws_station_id} progress={position + 1}/{total}"
            )
            raise
    _log_try(entry)
    return "fetched"


def _row_message(row: DueWindDay, exc: BaseException) -> str:
    return (
        f"station={row.pws_station_id} date={row.local_date.isoformat()}: "
        f"{sanitized_exception(exc)}"
    )


async def _fetch_failed(
    chunk: _Chunk,
    row: DueWindDay,
    reservation: Reservation,
    exc: Exception,
    message: str,
    *,
    exempt: bool,
) -> _FailureOutcome:
    """Record a failed call (not a probe). Raises ``JobDeferred`` where §8.7 says.

    For today's row the write that records the outcome also moves the row to
    its next try and logs the try (plan 4.2, 4.5). A 401/403 there returns
    ``"fetched"``, so the run's other today rows meet the new hold in their
    reservation. A 429 or 5xx defers the job to the back-off's end, in the
    evening window as in the daytime; in the window a transport error
    returns ``"fetched"`` instead of raising, so the run goes on. In the
    daytime a deferral (429, 5xx, transport) moves today's row to
    ``min(deferral end, 23:00)``, and the raised wake is capped the same way.
    """
    station_id = row.station_id
    local_date = row.local_date
    tz_name = chunk.tz_name
    today = chunk.today
    site_id = chunk.site_id
    todays_row = local_date == today

    def begin(conn: sqlite3.Connection) -> _TodayTry | None:
        # Read before any write of the outcome: the slot is the one the row
        # held when the try started (4.2).
        if not todays_row:
            return None
        current = _read_day(conn, station_id, local_date)
        if current is None:
            return None
        return _today_try(
            conn,
            current,
            site_id=site_id,
            station_id=station_id,
            local_date=local_date,
            tz_name=tz_name,
            tried=utc_now(),
        )

    def failure(
        conn: sqlite3.Connection,
        *,
        daytime_until: datetime | None = None,
        not_before: datetime | None = None,
    ) -> None:
        _row_failure(
            conn,
            site_id=site_id,
            station_id=station_id,
            local_date=local_date,
            tz_name=tz_name,
            today=today,
            message=message,
            daytime_until=daytime_until,
            not_before=not_before,
        )

    def failed_try(
        conn: sqlite3.Connection, attempt: _TodayTry | None, reason: str, detail: str
    ) -> _TryLog | None:
        if attempt is None:
            return None
        return _log_today_try(
            conn, attempt, outcome="failed", reason=reason, detail=detail
        )

    if isinstance(exc, httpx.HTTPStatusError):
        response = exc.response
        status = response.status_code
        if status in (401, 403):
            endpoint = endpoint_for(local_date, today)
            if endpoint == "all_1day":
                # Today's 5-minute download was refused: hold the five_minute
                # kind for every station. The call was made, so nothing is
                # refunded; the row keeps its status and moves to its next try.
                refused = (
                    f"station={row.pws_station_id} date={local_date.isoformat()}: "
                    f"access refused (HTTP {status})"
                )

                def record_refusal(conn: sqlite3.Connection) -> _TryLog | None:
                    attempt = begin(conn)
                    record_access_refused(conn, "five_minute", status)
                    stamp = isoformat_utc(utc_now())
                    moved = (
                        None
                        if attempt is None
                        else _next_try_at(conn, attempt, success=False)
                    )
                    conn.execute(
                        "UPDATE station_wind_days SET last_error = ?, refused_at = ?,"
                        " next_attempt_at = COALESCE(?, next_attempt_at),"
                        " updated_at = ? WHERE station_id = ? AND local_date = ?",
                        (
                            refused,
                            stamp,
                            moved,
                            stamp,
                            station_id,
                            local_date.isoformat(),
                        ),
                    )
                    if attempt is None:
                        return None
                    return _log_today_try(
                        conn,
                        attempt,
                        outcome="refused",
                        reason="refused_access",
                        detail=f"http {status}",
                    )

                entry = await write_after_reservation(
                    chunk.db, chunk.writer, record_refusal, reservation
                )
                _log_try(entry)
                return "fetched"
            await write_after_reservation(
                chunk.db,
                chunk.writer,
                lambda conn: _auth_refused(
                    conn, station_id, local_date, endpoint, message
                ),
                reservation,
            )
            logger.warning(
                "wind history auth hold set endpoint=%s status=%s", endpoint, status
            )
            return "closed"
        if status == 429:

            def rate_limited(
                conn: sqlite3.Connection,
            ) -> tuple[str, _TryLog | None]:
                attempt = begin(conn)
                wake = record_http_backoff(conn, response) or _transport_wake()
                _set_last_error(conn, station_id, local_date, message)
                if attempt is None:
                    return wake, None
                end = _parse_due(wake)
                moved = _next_try_at(
                    conn, attempt, success=False, daytime_until=end, not_before=end
                )
                conn.execute(
                    "UPDATE station_wind_days"
                    " SET next_attempt_at = COALESCE(?, next_attempt_at)"
                    " WHERE station_id = ? AND local_date = ?",
                    (moved, station_id, local_date.isoformat()),
                )
                return wake, failed_try(conn, attempt, "provider", "http 429")

            wake, entry = await write_after_reservation(
                chunk.db, chunk.writer, rate_limited, reservation
            )
            _log_try(entry)
            raise JobDeferred(_capped_wake(wake, entry)) from exc
        if status >= 500:

            def server_error(
                conn: sqlite3.Connection,
            ) -> tuple[str, _TryLog | None]:
                attempt = begin(conn)
                wake = record_http_backoff(conn, response) or _transport_wake()
                end = _parse_due(wake)
                failure(conn, daytime_until=end, not_before=end)
                return wake, failed_try(conn, attempt, "provider", f"http {status}")

            # In the window as in the daytime (4.2): the job waits for the
            # back-off's end, so the run's other today rows keep their slot.
            wake, entry = await write_after_reservation(
                chunk.db, chunk.writer, server_error, reservation
            )
            _log_try(entry)
            raise JobDeferred(_capped_wake(wake, entry)) from exc

        def http_failure(conn: sqlite3.Connection) -> _TryLog | None:
            attempt = begin(conn)
            failure(conn)
            return failed_try(conn, attempt, "provider", f"http {status}")

        _log_try(
            await write_after_reservation(
                chunk.db, chunk.writer, http_failure, reservation
            )
        )
        return "fetched"
    if (
        isinstance(exc, UpstreamPayloadError)
        and exc.diagnostics.kind in _PAYLOAD_ROW_FAULTS
    ):
        fault = str(exc.diagnostics.kind)

        def payload_failure(conn: sqlite3.Connection) -> _TryLog | None:
            attempt = begin(conn)
            failure(conn)
            return failed_try(conn, attempt, "provider", fault)

        _log_try(
            await write_after_reservation(
                chunk.db, chunk.writer, payload_failure, reservation
            )
        )
        return "fetched"
    if isinstance(exc, (httpx.TransportError, TimeoutError)):
        refund = is_refundable_transport_error(exc)

        def transport_failure(
            conn: sqlite3.Connection,
        ) -> tuple[str, _TryLog | None]:
            attempt = begin(conn)
            wake = _transport_wake()
            if refund:
                _refund(conn, reservation, exempt=exempt)
            # A daytime deferral moves today's row to min(try + 15 min,
            # 23:00); in the window it goes to its next slot (4.2).
            failure(conn, daytime_until=_parse_due(wake))
            return wake, failed_try(conn, attempt, "network", "network")

        wake, entry = await write_after_reservation(
            chunk.db, chunk.writer, transport_failure, reservation
        )
        _log_try(entry)
        if entry is not None and entry.in_window:
            # 4.2: in the window the row is at its next slot and the run goes
            # on to its other today rows instead of deferring 15 minutes.
            return "fetched"
        raise JobDeferred(_capped_wake(wake, entry)) from exc
    await write_after_reservation(chunk.db, chunk.writer, failure, reservation)
    return "reraise"


async def _probe_failed(
    chunk: _Chunk,
    row: DueWindDay,
    endpoint: AuthHoldEndpoint,
    reservation: Reservation,
    exc: Exception,
    message: str,
) -> None:
    """A failed auth check: ``held`` again with the new error; nothing re-raised."""
    station_id = row.station_id
    local_date = row.local_date
    refund = is_refundable_transport_error(exc)
    response = exc.response if isinstance(exc, httpx.HTTPStatusError) else None

    def probe_failure(conn: sqlite3.Connection) -> None:
        if response is not None:
            record_http_backoff(conn, response)
        if refund:
            _refund(conn, reservation, exempt=True)
        write_auth_hold(
            conn,
            endpoint,
            status="held",
            since=isoformat_utc(utc_now()),
            error=message,
        )
        _set_last_error(conn, station_id, local_date, message)

    await write_after_reservation(chunk.db, chunk.writer, probe_failure, reservation)
    logger.warning("wind history auth check failed endpoint=%s", endpoint)


def _transport_wake() -> str:
    return isoformat_utc(utc_now() + _TRANSPORT_DEFER)


def _capped_wake(wake: str, entry: _TryLog | None) -> str:
    """``wake``, capped at the day's 23:00 slot for a daytime try of today's
    row (4.2: a daytime deferral is still capped at 23:00)."""
    if entry is None or entry.in_window or entry.cap is None:
        return wake
    at = _parse_due(wake)
    if at is None or at <= entry.cap:
        return wake
    return isoformat_utc(entry.cap)


def _log_try(entry: _TryLog | None) -> None:
    """The try's INFO line (plan 4.5), once its write has committed."""
    if entry is None:
        return
    logger.info(
        "five-minute try site=%s station=%s date=%s slot=%s outcome=%s"
        " reason=%s readings=%s",
        entry.site_id,
        entry.station_id,
        entry.local_date.isoformat(),
        entry.slot,
        entry.outcome,
        "-" if entry.reason is None else entry.reason,
        entry.readings,
    )


def _refund(
    conn: sqlite3.Connection, reservation: Reservation, *, exempt: bool
) -> None:
    refund_budget(conn, reservation)
    add_lane_calls(conn, lane_counter_key(exempt=exempt), reservation.billing_day, -1)


def _set_last_error(
    conn: sqlite3.Connection, station_id: int, local_date: date, message: str
) -> None:
    conn.execute(
        "UPDATE station_wind_days SET last_error = ?, updated_at = ?"
        " WHERE station_id = ? AND local_date = ?",
        (message, isoformat_utc(utc_now()), station_id, local_date.isoformat()),
    )


def _auth_refused(
    conn: sqlite3.Connection,
    station_id: int,
    local_date: date,
    endpoint: AuthHoldEndpoint,
    message: str,
) -> None:
    _set_last_error(conn, station_id, local_date, message)
    write_auth_hold(
        conn,
        endpoint,
        status="held",
        since=isoformat_utc(utc_now()),
        error=message,
    )


# --- persist ---------------------------------------------------------------


def _day_hours(start: datetime, end: datetime) -> list[datetime]:
    """Every on-the-hour UTC instant in ``[start, end)``."""
    hour = floor_hour(start)
    if hour < start:
        hour += _ONE_HOUR
    hours: list[datetime] = []
    while hour < end:
        hours.append(hour)
        hour += _ONE_HOUR
    return hours


def _latest_record(conn: sqlite3.Connection, station_id: int) -> datetime | None:
    row = conn.execute(
        "SELECT MAX(obs_at) AS latest FROM station_wind_records WHERE station_id = ?"
        " AND source = 'five_minute' AND speed_kmh IS NOT NULL",
        (station_id,),
    ).fetchone()
    if row is None or row["latest"] is None:
        return None
    try:
        return parse_utc(str(row["latest"]))
    except ValueError:
        return None


def _derive_hours(
    conn: sqlite3.Connection,
    station_id: int,
    local_date: date,
    tz_name: str,
    today: date,
) -> list[datetime]:
    """The day's hours; for today only the complete ones (H + 1 h <= newest)."""
    start, end, _ = local_day_slots(local_date, tz_name)
    hours = _day_hours(start, end)
    if local_date != today:
        return hours
    latest = _latest_record(conn, station_id)
    if latest is None:
        return []
    return [hour for hour in hours if hour + _ONE_HOUR <= latest]


def _read_records(
    conn: sqlite3.Connection, station_id: int, low: datetime, high: datetime
) -> list[WindRecord]:
    rows = conn.execute(
        """
        SELECT obs_at, speed_kmh FROM station_wind_records
        WHERE station_id = ? AND obs_at >= ? AND obs_at < ?
          AND source = 'five_minute' AND speed_kmh IS NOT NULL
        ORDER BY obs_at
        """,
        (station_id, isoformat_utc(low), isoformat_utc(high)),
    ).fetchall()
    records: list[WindRecord] = []
    unreadable = 0
    for row in rows:
        try:
            obs_at = parse_utc(str(row["obs_at"]))
        except ValueError:
            unreadable += 1
            continue
        records.append(WindRecord(obs_at=obs_at, speed_kmh=float(row["speed_kmh"])))
    if unreadable:
        logger.warning(
            "unreadable wind records skipped station=%s rows=%s",
            station_id,
            unreadable,
        )
    return records


def _today_next_attempt(
    conn: sqlite3.Connection,
    now: datetime,
    *,
    cap: datetime,
) -> str:
    """The next daytime try after one at ``now`` (plan 4.2).

    ``min(now + obs_interval_minutes, cap)``, whatever the try's outcome.
    ``cap`` is the day's 23:00 slot; every try at or after it belongs to the
    evening window (``_window_next``).
    """
    interval = get_number_setting(conn, "obs_interval_minutes", 180, minimum=30)
    return isoformat_utc(min(now + timedelta(minutes=interval), cap))


def _parse_due(value: object) -> datetime | None:
    """A stored stamp as an instant; None for NULL or an unreadable value."""
    if not isinstance(value, str):
        return None
    try:
        return parse_utc(value)
    except ValueError:
        return None


def _slot_label(clock: _DayClock | None, due: datetime | None, tried: datetime) -> str:
    """The try log's slot for a try of today's row (plan 4.5).

    The slot the row held when the try started (``due``; the try time when
    the row held none): ``daytime`` before the 23:00 slot, else the latest
    evening slot at or before it as local ``HH:MM``. A late try is still
    that slot's try (4.2).
    """
    at = tried if due is None else due
    if clock is None or not clock.slots or at < clock.slots[0]:
        return "daytime"
    label = clock.labels[0]
    for slot, name in zip(clock.slots, clock.labels, strict=True):
        if slot > at:
            break
        label = name
    return label


def _today_try(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    tz_name: str,
    tried: datetime,
) -> _TodayTry:
    """A try of today's row, from its row as read before the outcome write."""
    clock = _day_clock(conn, site_id, local_date, tz_name)
    return _TodayTry(
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tried=tried,
        clock=clock,
        slot=_slot_label(clock, _parse_due(row["next_attempt_at"]), tried),
    )


def _window_next(
    clock: _DayClock,
    tried: datetime,
    *,
    success: bool,
    not_before: datetime | None,
) -> datetime:
    """The next try after one at or after the 23:00 slot (plan 4.2).

    A success before the 23:56 slot moves to it; a success at or after it is
    done for the day. Otherwise the first slot after ``tried`` (and, for a
    deferral, at or after its end ``not_before``); with none left, done for
    the day (the day's end plus one hour).
    """
    if success:
        if clock.late is not None and tried < clock.late:
            return clock.late
        return clock.done
    for slot in clock.slots:
        if slot > tried and (not_before is None or slot >= not_before):
            return slot
    return clock.done


def _next_try_at(
    conn: sqlite3.Connection,
    attempt: _TodayTry,
    *,
    success: bool,
    daytime_until: datetime | None = None,
    not_before: datetime | None = None,
) -> str | None:
    """The stamp today's row moves to after ``attempt`` (plan 4.2, 4.4).

    In the daytime ``min(tried + interval, 23:00)``, whatever the outcome,
    except a deferral: given its end ``daytime_until`` (a 429's or 5xx's
    back-off end, a transport error's try + 15 min, a reservation
    deferral's end), ``min(daytime_until, 23:00)``. In the evening window
    ``_window_next``, with ``not_before`` (a back-off's or reservation
    deferral's end) when given. None when the row has no clock (the close is
    on and its date has no record): the row keeps its stamp.
    """
    clock = attempt.clock
    if clock is None:
        return None
    tried = attempt.tried
    if tried < clock.cap:
        if daytime_until is not None:
            return isoformat_utc(min(daytime_until, clock.cap))
        return _today_next_attempt(conn, tried, cap=clock.cap)
    return isoformat_utc(
        _window_next(clock, tried, success=success, not_before=not_before)
    )


def _log_today_try(
    conn: sqlite3.Connection,
    attempt: _TodayTry,
    *,
    outcome: str,
    reason: str | None,
    detail: str | None,
    kept: int = 0,
    refused: int = 0,
) -> _TryLog:
    """Write the try's ``close_try_log`` row in the caller's transaction."""
    record_collection_try(
        conn,
        site_id=attempt.site_id,
        station_id=attempt.station_id,
        local_date=attempt.local_date.isoformat(),
        kind="five_minute",
        slot=attempt.slot,
        close_attempt=None,
        tried_at=isoformat_utc_micro(attempt.tried),
        outcome=outcome,
        reason=reason,
        detail=detail,
        readings_kept=kept,
        readings_refused=refused,
    )
    clock = attempt.clock
    return _TryLog(
        site_id=attempt.site_id,
        station_id=attempt.station_id,
        local_date=attempt.local_date,
        slot=attempt.slot,
        outcome=outcome,
        reason=reason,
        readings=kept,
        in_window=clock is not None and attempt.tried >= clock.cap,
        cap=None if clock is None else clock.cap,
    )


def _record_held_try(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    tz_name: str,
    until: str,
) -> _TryLog | None:
    """A try of today's row that met the ``five_minute`` hold (plan 4.4).

    Moves the row to its next try as a try that did not succeed, sets its
    ``refused_at`` and logs outcome ``held``. Writes nothing to the hold.
    """
    row = _read_day(conn, station_id, local_date)
    if row is None:
        return None
    attempt = _today_try(
        conn,
        row,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=tz_name,
        tried=utc_now(),
    )
    stamp = isoformat_utc(attempt.tried)
    conn.execute(
        "UPDATE station_wind_days SET next_attempt_at = COALESCE(?, next_attempt_at),"
        " refused_at = ?, updated_at = ? WHERE station_id = ? AND local_date = ?",
        (
            _next_try_at(conn, attempt, success=False),
            stamp,
            stamp,
            station_id,
            local_date.isoformat(),
        ),
    )
    return _log_today_try(
        conn, attempt, outcome="held", reason="refused_access", detail=until
    )


def _record_deferred_try(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    tz_name: str,
    reason: DeferralReason,
    deferral_end: str,
) -> _TryLog | None:
    """A try of today's row whose reservation was deferred (plan 4.4).

    Moves the row: in the daytime to ``min(deferral end, 23:00)``, in the
    window to the first slot at or after the deferral's end, else done for
    the day. Logs outcome ``failed`` with ``reason`` as reason and detail.
    The row's status is left as it is.
    """
    row = _read_day(conn, station_id, local_date)
    if row is None:
        return None
    attempt = _today_try(
        conn,
        row,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=tz_name,
        tried=utc_now(),
    )
    end = _parse_due(deferral_end)
    moved = _next_try_at(
        conn, attempt, success=False, daytime_until=end, not_before=end
    )
    conn.execute(
        "UPDATE station_wind_days SET next_attempt_at = COALESCE(?, next_attempt_at),"
        " updated_at = ? WHERE station_id = ? AND local_date = ?",
        (moved, isoformat_utc(attempt.tried), station_id, local_date.isoformat()),
    )
    return _log_today_try(conn, attempt, outcome="failed", reason=reason, detail=reason)


def _stored_detail(result: StoreResult) -> str:
    """The ``stored`` outcome's detail (plan 4.5): the readings kept per
    quantity, those already stored, and those P1's guard refused."""
    per = result.kept_by_quantity
    return (
        f"temperature={per['temperature']} precip={per['precip']}"
        f" wind={per['wind']} already_stored={result.already_stored}"
        f" refused={result.refused}"
    )


def _is_refetch(row: sqlite3.Row) -> bool:
    return (
        str(row["status"]) in ("fetched", "unavailable")
        and row["refetch_at"] is not None
        and int(row["refetched"]) == 0
    )


def _read_day(
    conn: sqlite3.Connection, station_id: int, local_date: date
) -> sqlite3.Row | None:
    row: sqlite3.Row | None = conn.execute(
        """
        SELECT status, attempts, refetch_at, refetched, next_attempt_at
        FROM station_wind_days
        WHERE station_id = ? AND local_date = ?
        """,
        (station_id, local_date.isoformat()),
    ).fetchone()
    return row


def _as_reading(record: WindRecord | StationReading) -> StationReading:
    """``record`` as a ``five_minute`` reading for ``store_readings``.

    A ``WindRecord`` (the dated download's wind-only parse) becomes a reading
    with its wind and no local date, temperature or rain total.
    """
    if isinstance(record, StationReading):
        return record
    return StationReading(
        obs_at=record.obs_at,
        source="five_minute",
        local_date=None,
        temp_c=None,
        precip_total_mm=None,
        speed_kmh=record.speed_kmh,
    )


def _today_window(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    today: date,
    tz_name: str,
    now: datetime,
) -> tuple[datetime, datetime] | None:
    """The bounds of today's row's window: yesterday's start to today's end
    (4.1). ``_persist_day`` caps the upper end at now plus ``_FUTURE_SLACK``.

    While the close is off, today and yesterday are local dates in the site's
    zone ``tz_name`` (``sites.timezone``), bounded by ``local_day_slots``.
    While it is on, they are P1's record holding ``now`` and the site's record
    dated the day before it, by their stored bounds, after creating the
    site's missing records as the guard would; with no record the day before,
    today's bounds alone. ``None`` while the close is on and no record holds
    ``now`` (the site has no current day, 4.2): nothing is kept.
    """
    if not daily_close_on(conn):
        return (
            local_day_slots(today - _ONE_DAY, tz_name)[0],
            local_day_slots(today, tz_name)[1],
        )
    ensure_site_days(conn, site_id, now_utc=now)
    current = current_day(conn, site_id, now_utc=now)
    if current is None:
        return None
    previous = day_record(conn, site_id, current.local_date - _ONE_DAY)
    low = current.day_start_utc if previous is None else previous.day_start_utc
    return low, current.day_end_utc


def _persist_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    tz_name: str,
    today: date,
    records: Sequence[WindRecord | StationReading],
    probe: AuthHoldEndpoint | None,
    no_content: bool = False,
) -> _TryLog | None:
    """Store one fetched station-day, derive its pair-max station rows, and
    advance its row.

    Today's row is a successful try (plan 4.2): it stays ``partial``, moves
    to its next try, and its try is logged in this transaction (4.5) with
    outcome ``stored``, or ``no_readings`` (``no_content`` for a 204, set by
    the caller, ``empty`` for an empty list); the log entry is returned for
    the INFO line. A past row returns None.

    ``records`` are the download's readings: ``StationReading``s with every
    quantity, or ``WindRecord``s (wind only, no local date). Those inside the
    stored window go through ``store_readings`` with source ``five_minute``,
    which asks the closed-day guard (``READING``) at each ``obs_at``; a
    refused reading is not stored. Today's row keeps only readings inside
    yesterday's and today's bounds (``_today_window``); a past row keeps
    ``_STORE_MARGIN`` either side of its day. Both are capped at now plus
    ``_FUTURE_SLACK``, so a station clock running ahead stores nothing in the
    future.
    """
    now = utc_now()
    yesterday = today - _ONE_DAY
    start, end, _ = local_day_slots(local_date, tz_name)
    window: tuple[datetime, datetime] | None
    if local_date == today:
        bounds = _today_window(conn, site_id, today=today, tz_name=tz_name, now=now)
        window = (
            None if bounds is None else (bounds[0], min(bounds[1], now + _FUTURE_SLACK))
        )
    else:
        window = (start - _STORE_MARGIN, min(end + _STORE_MARGIN, now + _FUTURE_SLACK))
    readings = [_as_reading(record) for record in records]
    kept = (
        []
        if window is None
        else [r for r in readings if window[0] <= r.obs_at < window[1]]
    )
    if len(kept) != len(readings):
        logger.debug(
            "wind records outside the stored window dropped station=%s date=%s n=%s",
            station_id,
            local_date,
            len(readings) - len(kept),
        )
    stored = store_readings(
        conn,
        station_id=station_id,
        source="five_minute",
        readings=kept,
        fetched_at=now,
        now_utc=now,
    )
    _derive_days(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=tz_name,
        today=today,
        now_utc=now,
    )
    row = _read_day(conn, station_id, local_date)
    if row is None:
        logger.warning(
            "wind day row missing at persist station=%s date=%s",
            station_id,
            local_date,
        )
        return None
    attempts = int(row["attempts"])
    refetch = _is_refetch(row)
    count_row = conn.execute(
        "SELECT COUNT(*) AS n FROM station_wind_records"
        " WHERE station_id = ? AND obs_at >= ? AND obs_at < ?"
        " AND source = 'five_minute' AND speed_kmh IS NOT NULL",
        (station_id, isoformat_utc(start), isoformat_utc(end)),
    ).fetchone()
    record_count = int(count_row["n"])
    next_attempt_at: str | None = None
    attempt: _TodayTry | None = None
    if local_date == today:
        status = "partial"
        attempt = _today_try(
            conn,
            row,
            site_id=site_id,
            station_id=station_id,
            local_date=local_date,
            tz_name=tz_name,
            tried=now,
        )
        moved = _next_try_at(conn, attempt, success=True)
        # No clock (close on, no record of the date): the row keeps its stamp.
        kept_stamp = row["next_attempt_at"]
        next_attempt_at = (
            moved
            if moved is not None
            else None
            if kept_stamp is None
            else str(kept_stamp)
        )
    elif record_count > 0:
        status = "fetched"
    elif local_date == yesterday and attempts < _MAX_ATTEMPTS:
        status = "pending"
        attempts += 1
        next_attempt_at = isoformat_utc(now + _EMPTY_YESTERDAY_RETRY)
    else:
        status = "unavailable"
    stamp = isoformat_utc(now)
    conn.execute(
        """
        UPDATE station_wind_days
        SET status = ?, attempts = ?, next_attempt_at = ?, record_count = ?,
            last_ok_at = ?, last_error = NULL, updated_at = ?,
            refetched = CASE WHEN ? THEN 1 ELSE refetched END
        WHERE station_id = ? AND local_date = ?
        """,
        (
            status,
            attempts,
            next_attempt_at,
            record_count,
            stamp,
            stamp,
            1 if refetch else 0,
            station_id,
            local_date.isoformat(),
        ),
    )
    if local_date == yesterday and status in ("fetched", "unavailable") and not refetch:
        _reconcile(
            conn, site_id, station_id, local_date, tz_name, record_count, now_utc=now
        )
    clear_domain_backoff(conn, source_domain("weathercom"))
    if local_date == today:
        # A 2xx on today's 5-minute download: the five_minute kind is granted.
        clear_access_refused(conn, "five_minute")
    if probe is not None:
        clear_auth_hold(conn, probe)
    if attempt is None:
        return None
    if no_content:
        outcome, detail = "no_readings", "no_content"
    elif not records:
        outcome, detail = "no_readings", "empty"
    else:
        outcome, detail = "stored", _stored_detail(stored)
    return _log_today_try(
        conn,
        attempt,
        outcome=outcome,
        reason=None,
        detail=detail,
        kept=stored.kept,
        refused=stored.refused,
    )


def _derive_days(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    tz_name: str,
    today: date,
    now_utc: datetime | None = None,
) -> None:
    """Pair hours for d and its fetched/partial neighbours, and write, delete
    and materialize the station rows.

    The closed-day guard (``HOURLY``) cuts the write hours and the delete
    hours in one pass before either loop, with one WARNING for both; a
    refused hour's row is neither written nor deleted. ``now_utc`` defaults
    to ``utc_now()``.
    """
    now = utc_now() if now_utc is None else now_utc
    days = [local_date]
    neighbours = conn.execute(
        """
        SELECT local_date FROM station_wind_days
        WHERE station_id = ? AND local_date IN (?, ?)
          AND status IN ('fetched','partial')
        """,
        (
            station_id,
            (local_date - _ONE_DAY).isoformat(),
            (local_date + _ONE_DAY).isoformat(),
        ),
    ).fetchall()
    for neighbour in neighbours:
        try:
            days.append(date.fromisoformat(str(neighbour["local_date"])))
        except ValueError:
            continue
    days.sort()
    hours_by_day = {
        day: _derive_hours(conn, station_id, day, tz_name, today) for day in days
    }
    all_hours = [hour for day in days for hour in hours_by_day[day]]
    records: list[WindRecord] = []
    if all_hours:
        records = _read_records(
            conn,
            station_id,
            min(all_hours) - _PAIR_LOOKBACK,
            max(all_hours) + _ONE_HOUR,
        )
    pairs = pair_max_by_hour(records, all_hours)
    write_hours = sorted(pairs)
    delete_hours = sorted(set(all_hours) - set(pairs))
    instants = [isoformat_utc(hour) for hour in (*write_hours, *delete_hours)]
    allowed = filter_day_writes(conn, site_id, instants, WriteKind.HOURLY, now_utc=now)
    warn_refused_instants(
        logger,
        conn,
        WriteKind.HOURLY,
        writer="_derive_days",
        site_id=site_id,
        instants=[instant for instant in instants if instant not in allowed],
    )
    changed: set[datetime] = set()
    for hour in write_hours:
        valid_at = isoformat_utc(hour)
        if valid_at not in allowed:
            continue
        speed = pairs[hour]
        if write_station_observation(
            conn,
            station_id=station_id,
            variable="wind",
            valid_at=valid_at,
            value=kmh_to_ms(speed),
            source_raw=f"pair-max {round(speed, 2)} km/h",
            now_utc=now,
        ):
            changed.add(hour)
    for hour in delete_hours:
        valid_at = isoformat_utc(hour)
        if valid_at not in allowed:
            continue
        deleted = conn.execute(
            "DELETE FROM station_observations WHERE id IN ("
            " SELECT so.id FROM station_observations so"
            " WHERE so.station_id = ? AND so.variable = 'wind'"
            f" AND so.valid_at = ? AND {PAIR_MAX_WIND_SQL})",
            (station_id, valid_at),
        ).rowcount
        if deleted:
            changed.add(hour)
    for hour in sorted(changed):
        _materialize(conn, site_id, isoformat_utc(hour), now_utc=now)
    if changed:
        enqueue_if_absent(
            conn, "pair_and_score", site_id, "score", {"site_id": site_id}
        )
    for day in days:
        conn.execute(
            "UPDATE station_wind_days SET pair_hours = ?"
            " WHERE station_id = ? AND local_date = ?",
            (
                sum(1 for hour in hours_by_day[day] if hour in pairs),
                station_id,
                day.isoformat(),
            ),
        )


def _reconcile(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    local_date: date,
    tz_name: str,
    record_count: int,
    *,
    now_utc: datetime | None = None,
) -> None:
    """Schedule one refetch of a short yesterday (plan §8.14).

    The closed-day guard (``READING``, at ``local_date``) is asked before
    the update: a day that takes no readings gets no refetch. It sits behind
    ``_persist_day``'s cut, so a refusal is logged at DEBUG only.
    ``now_utc`` defaults to ``utc_now()``.
    """
    pool = [
        int(row["record_count"])
        for row in conn.execute(
            """
            SELECT record_count FROM station_wind_days
            WHERE station_id = ? AND local_date < ? AND status = 'fetched'
            ORDER BY local_date DESC LIMIT ?
            """,
            (station_id, local_date.isoformat(), _RECONCILE_POOL),
        )
    ]
    if len(pool) < _RECONCILE_MIN:
        return
    if record_count >= _RECONCILE_RATIO * statistics.median(pool):
        return
    if not check_date_write(
        conn,
        site_id,
        local_date,
        WriteKind.READING,
        now_utc=utc_now() if now_utc is None else now_utc,
    ):
        return
    refetch_start = local_day_slots(local_date + timedelta(days=2), tz_name)[0]
    conn.execute(
        "UPDATE station_wind_days SET refetch_at = ?"
        " WHERE station_id = ? AND local_date = ?",
        (
            isoformat_utc(refetch_start + _ONE_HOUR),
            station_id,
            local_date.isoformat(),
        ),
    )


def _finish_floor(today: date) -> str:
    """The oldest local date the finish step touches: the first day of the
    wind training window (``TRAINING_DAYS``)."""
    return (today - timedelta(days=TRAINING_DAYS)).isoformat()


async def _finish_waiting_days(chunk: _Chunk, start: _JobStart) -> None:
    """Finish the past rows that wait on a held ``history_all`` (plan §4).

    No API call and no budget. Each row is finished in its own transaction; a
    row whose finish raises goes through ``_row_failure``, with a WARNING, and
    the step moves on. After a row dated yesterday is finished, a missed final
    collection is logged (plan §7) unless the ``five_minute`` kind is refused.
    """
    history = start.holds.get("history_all")
    if history is None or history.status != "held":
        return
    site_id = chunk.site_id
    tz_name = chunk.tz_name
    today = chunk.today
    yesterday = today - _ONE_DAY
    rows = await chunk.db.read(
        lambda conn: conn.execute(
            _FINISHABLE_SQL,
            {
                "site_id": site_id,
                "today": today.isoformat(),
                "floor": _finish_floor(today),
                "station_id": None,
                "local_date": None,
                "limit": _CHUNK_ROWS,
            },
        ).fetchall()
    )
    finished_yesterday = False
    for row in rows:
        station_id = int(row["station_id"])
        pws_station_id = str(row["pws_station_id"])
        local_date = date.fromisoformat(str(row["local_date"]))
        try:
            finished = await chunk.writer.write(
                lambda conn, sid=station_id, day=local_date: _finish_day(
                    conn,
                    site_id=site_id,
                    station_id=sid,
                    local_date=day,
                    tz_name=tz_name,
                    today=today,
                )
            )
        except (StaleGenerationError, sqlite3.OperationalError, JobControl):
            raise
        except Exception as exc:
            error = sanitized_exception(exc)
            message = f"station={pws_station_id} date={local_date.isoformat()}: {error}"
            await chunk.writer.write(
                lambda conn, sid=station_id, day=local_date, text=message: _row_failure(
                    conn,
                    site_id=site_id,
                    station_id=sid,
                    local_date=day,
                    tz_name=tz_name,
                    today=today,
                    message=text,
                )
            )
            logger.warning(
                "wind day finish failed site=%s station=%s date=%s: %s",
                site_id,
                pws_station_id,
                local_date,
                error,
            )
            continue
        if finished and local_date == yesterday:
            finished_yesterday = True
    if finished_yesterday:

        def final_check(conn: sqlite3.Connection) -> tuple[int, int, bool]:
            missed, total = count_missed_final(conn, site_id, yesterday, tz_name)
            five_minute_refused = any(
                record.kind == "five_minute"
                for record in refused_kinds(conn, utc_now())
            )
            return missed, total, five_minute_refused

        missed, total, five_minute_refused = await chunk.db.read(final_check)
        if missed > 0 and not five_minute_refused:
            logger.warning(
                "wind final collection missed site=%s date=%s stations=%s/%s",
                site_id,
                yesterday,
                missed,
                total,
            )


def _finish_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    tz_name: str,
    today: date,
) -> bool:
    """Finish one past row with the records it has (plan §4); False if gated.

    The gate is an allowlist read in this transaction: the ``history_all``
    hold is ``held`` and the row matches ``_FINISHABLE_SQL``. A pending
    refetch on a fetched or unavailable row is closed; any other row becomes
    fetched or unavailable and is derived as a past day. ``attempts``,
    ``last_ok_at``, ``last_error``, the records and the hold are left as they
    are.

    The closed-day guard (``HOURLY``, at ``local_date``) is asked before the
    first write: a refused day returns False, as a gated row does, with the
    refusal WARNING.
    """
    hold = read_auth_hold(conn, "history_all")
    if hold is None or hold.status != "held":
        return False
    day = local_date.isoformat()
    finishable = conn.execute(
        _FINISHABLE_SQL,
        {
            "site_id": site_id,
            "today": today.isoformat(),
            "floor": _finish_floor(today),
            "station_id": station_id,
            "local_date": day,
            "limit": 1,
        },
    ).fetchone()
    if finishable is None:
        return False
    now = utc_now()
    if not check_date_write(conn, site_id, local_date, WriteKind.HOURLY, now_utc=now):
        warn_refused_dates(
            logger,
            WriteKind.HOURLY,
            writer="_finish_day",
            site_id=site_id,
            days=[local_date],
            count=1,
        )
        return False
    stamp = isoformat_utc(now)
    row = _read_day(conn, station_id, local_date)
    if row is not None and _is_refetch(row):
        conn.execute(
            "UPDATE station_wind_days SET refetched = 1, updated_at = ?"
            " WHERE station_id = ? AND local_date = ?",
            (stamp, station_id, day),
        )
        return True
    start, end, _ = local_day_slots(local_date, tz_name)
    count_row = conn.execute(
        "SELECT COUNT(*) AS n FROM station_wind_records"
        " WHERE station_id = ? AND obs_at >= ? AND obs_at < ?"
        " AND source = 'five_minute' AND speed_kmh IS NOT NULL",
        (station_id, isoformat_utc(start), isoformat_utc(end)),
    ).fetchone()
    conn.execute(
        """
        UPDATE station_wind_days
        SET status = CASE WHEN :n > 0 THEN 'fetched' ELSE 'unavailable' END,
            next_attempt_at = NULL, record_count = :n,
            refetched = CASE WHEN refetch_at IS NOT NULL THEN 1 ELSE refetched END,
            updated_at = :now
        WHERE station_id = :station_id AND local_date = :local_date
          AND (status IN ('pending','partial')
               OR (status = 'failed' AND attempts < 3))
        """,
        {
            "n": int(count_row["n"]),
            "now": stamp,
            "station_id": station_id,
            "local_date": day,
        },
    )
    _derive_days(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=tz_name,
        today=today,
        now_utc=now,
    )
    return True


def _row_failure(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    tz_name: str,
    today: date,
    message: str,
    daytime_until: datetime | None = None,
    not_before: datetime | None = None,
) -> None:
    """The row-failure rule (plan §8.7): refetch, today, then a past date.

    Today's row stays ``partial`` and moves to its next try as a try that
    did not succeed (plan 4.2): in the daytime a deferral's end
    ``daytime_until`` moves it to ``min(that end, 23:00)``; in the evening
    window ``not_before``, a back-off's end, moves it to the first slot at
    or after that end.
    """
    row = _read_day(conn, station_id, local_date)
    if row is None:
        return
    now = utc_now()
    stamp = isoformat_utc(now)
    if _is_refetch(row):
        conn.execute(
            "UPDATE station_wind_days SET refetched = 1, last_error = ?,"
            " updated_at = ? WHERE station_id = ? AND local_date = ?",
            (message, stamp, station_id, local_date.isoformat()),
        )
        return
    if local_date == today:
        attempt = _today_try(
            conn,
            row,
            site_id=site_id,
            station_id=station_id,
            local_date=local_date,
            tz_name=tz_name,
            tried=now,
        )
        conn.execute(
            "UPDATE station_wind_days SET status = 'partial',"
            " next_attempt_at = COALESCE(?, next_attempt_at), last_error = ?,"
            " updated_at = ? WHERE station_id = ? AND local_date = ?",
            (
                _next_try_at(
                    conn,
                    attempt,
                    success=False,
                    daytime_until=daytime_until,
                    not_before=not_before,
                ),
                message,
                stamp,
                station_id,
                local_date.isoformat(),
            ),
        )
    else:
        attempts = int(row["attempts"]) + 1
        retry = _FIRST_RETRY if attempts == 1 else _LATER_RETRY
        conn.execute(
            "UPDATE station_wind_days SET status = 'failed', attempts = ?,"
            " next_attempt_at = ?, last_error = ?, updated_at = ?"
            " WHERE station_id = ? AND local_date = ?",
            (
                attempts,
                isoformat_utc(now + retry),
                message,
                stamp,
                station_id,
                local_date.isoformat(),
            ),
        )


def _materialize(
    conn: sqlite3.Connection,
    site_id: int,
    valid_at: str,
    *,
    now_utc: datetime | None = None,
) -> None:
    """``materialize_consensus`` for one wind hour.

    A string that will not parse (an imported row) cannot go through
    ``materialize_consensus`` when the persistence feed exists, because that
    parses it. Such a string takes the same invalidation steps directly and
    then deletes the observation, with a WARNING, so one such hour cannot
    stop ``_derive_days``. ``now_utc`` is the closed-day guard's clock.
    """
    try:
        parse_utc(valid_at)
    except ValueError:
        _invalidate_unparseable(conn, site_id, valid_at, now_utc=now_utc)
        return
    try:
        materialize_consensus(
            conn, site_id=site_id, variable="wind", valid_at=valid_at, now_utc=now_utc
        )
    except OverflowError:
        _invalidate_unparseable(conn, site_id, valid_at, now_utc=now_utc)


def _invalidate_unparseable(
    conn: sqlite3.Connection,
    site_id: int,
    valid_at: str,
    *,
    now_utc: datetime | None = None,
) -> None:
    """Invalidate one wind hour whose time is unusable, without parsing it.

    The closed-day guard (``HOURLY``, at ``valid_at``) is asked first. An
    instant that does not parse belongs to no day, so with the close on it
    is always refused: nothing is deleted and the refusal WARNING names no
    date. ``now_utc`` defaults to ``utc_now()``.
    """
    if not check_day_write(
        conn,
        site_id,
        valid_at,
        WriteKind.HOURLY,
        now_utc=utc_now() if now_utc is None else now_utc,
    ):
        warn_refused_instants(
            logger,
            conn,
            WriteKind.HOURLY,
            writer="_invalidate_unparseable",
            site_id=site_id,
            instants=[valid_at],
        )
        return
    logger.warning(
        "wind hour with an unusable time invalidated directly site=%s", site_id
    )
    conn.execute(
        f"""
        DELETE FROM forecast_pairs
        WHERE site_id = ? AND variable = 'wind'
          AND (valid_at = ?
               OR (issued_at = ? AND feed_id IN (
                   SELECT id FROM feeds
                   WHERE source = 'virtual' AND model = '_persistence')))
          AND {published_generation_clause("forecast_pairs")}
        """,
        (site_id, valid_at, valid_at),
    )
    conn.execute(
        "DELETE FROM score_cache WHERE site_id = ? AND variable = 'wind'", (site_id,)
    )
    mark_daily_truth_stale(conn, site_id=site_id, variable="wind", valid_at=valid_at)
    conn.execute(
        "DELETE FROM observations WHERE site_id = ? AND variable = 'wind'"
        " AND valid_at = ?",
        (site_id, valid_at),
    )
