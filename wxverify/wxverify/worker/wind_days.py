"""The wind-history lane: today's raw 5-minute station readings.

One ``fetch_obs`` job with the job key ``wind-days`` runs one bounded chunk
for one site. It downloads today's 5-minute readings for each station of the
site and stores them, today's row on the daytime cadence and the evening
window's slots (``EVENING_SLOTS``). The lane creates and selects only the row
of the current day; no past day is fetched or finished, and no hourly
station row is derived here (the close writes those).

Today's 5-minute download takes the refused-access hold
(:mod:`wxverify.worker.access_hold`), checked per row inside the reservation.
Every try of today's row is logged to the collection try log
(:func:`wxverify.worker.close_chain.record_collection_try`) and in one INFO
line.

The lane's share of the weather.com budget is in
:mod:`wxverify.collection.wind_quota`.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from wxverify.collection.budget import (
    Reservation,
    is_refundable_transport_error,
    refund_budget,
    reserve_budget,
    write_after_reservation,
)
from wxverify.collection.wind_quota import (
    add_lane_calls,
    backfill_headroom,
    lane_counter_key,
)
from wxverify.core.error_sanitize import provider_reason, sanitized_exception
from wxverify.core.secrets import resolve_secret
from wxverify.core.timeutil import (
    isoformat_utc,
    isoformat_utc_micro,
    local_day_slots,
    parse_utc,
    utc_now,
)
from wxverify.db.connection import Database, FencedWriter
from wxverify.db.site_days import (
    DayRecord,
    current_day,
    day_record,
    ensure_site_days,
    local_clock_utc,
)
from wxverify.db.station_readings import StoreResult, store_readings
from wxverify.obs.hourly_values import StationReading
from wxverify.obs.pws_adapter import (
    UpstreamPayloadError,
    fetch_all_1day,
    is_wind_history_no_content,
)
from wxverify.settings.keys import get_number_setting
from wxverify.worker.access_hold import (
    AccessHeld,
    check_access_hold,
    clear_access_refused,
    record_access_refused,
)
from wxverify.worker.close_chain import record_collection_try
from wxverify.worker.control import JobCancelled, JobControl, JobDeferred
from wxverify.worker.current_obs import end_poll_pause_on_fresh_reading
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
_TRANSPORT_DEFER: Final = timedelta(minutes=15)
_FUTURE_SLACK: Final = timedelta(minutes=5)
_ONE_HOUR: Final = timedelta(hours=1)
_ONE_DAY: Final = timedelta(days=1)
_PAYLOAD_ROW_FAULTS: Final = frozenset(
    {"json_decode", "invalid_structure", "provider_error"}
)

#: The due selector, shared by the lane, the scheduler and ``wind_days_due``:
#: the rows of the current day only (switch-over plan 7.2, R4). An unreadable
#: ``next_attempt_at`` fails open.
WIND_DUE_SQL: Final = """
SELECT d.station_id, d.local_date, d.status, d.attempts, st.pws_station_id
FROM station_wind_days d JOIN stations st ON st.id = d.station_id
WHERE st.site_id = :site_id AND st.enabled = 1
  AND d.local_date = :today
  AND (d.status IN ('pending','partial') OR (d.status='failed' AND d.attempts < 3))
  AND (d.next_attempt_at IS NULL OR julianday(d.next_attempt_at) IS NULL
       OR julianday(d.next_attempt_at) <= julianday(:now))
ORDER BY d.station_id ASC
LIMIT :limit
"""

_RowOutcome = Literal["skipped", "fetched"]
_FailureOutcome = Literal["fetched", "reraise"]
DeferralReason = Literal["backoff", "call_limit"]


class WindHeadroomExhausted(Exception):
    """The backfill headroom is below one call. Nothing has been written."""


class ReservationDeferred(JobDeferred):
    """A reservation met the domain back-off or the call limit (plan 4.4).

    ``reason`` is ``backoff`` (``check_domain_backoff``) or ``call_limit``
    (``reserve_budget``); ``next_attempt_at`` is the deferral's end. A
    subclass of ``JobDeferred``; ``_run_row`` catches it for today's row.
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


def wind_days_due(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    today: date,
    now: datetime,
    limit: int,
) -> list[DueWindDay]:
    """Run ``WIND_DUE_SQL``: the due rows dated ``today``, by station.

    The ``five_minute`` refused-access hold is checked per row inside the
    reservation (``_reserve_wind_day_call``, plan 4.4), not here.
    """
    rows = conn.execute(
        WIND_DUE_SQL,
        {
            "site_id": site_id,
            "today": today.isoformat(),
            "now": isoformat_utc(now),
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
    """Give every enabled station a row dated ``today``, and no other.

    The lane creates only the row of the current day (switch-over plan 7.2,
    R4); past rows are left as they stand. ``tz``, the site's zone, is not
    read: ``today`` already names the day. Rows whose ``local_date`` is not
    a canonical ``YYYY-MM-DD`` are deleted, with a WARNING and the count.
    """
    stations = [
        int(station["id"])
        for station in conn.execute(
            "SELECT id FROM stations WHERE site_id = ? AND enabled = 1 ORDER BY id",
            (site_id,),
        )
    ]
    stamp = isoformat_utc(utc_now())
    day = today.isoformat()
    conn.executemany(
        "INSERT OR IGNORE INTO station_wind_days"
        " (station_id, local_date, status, updated_at) VALUES (?, ?, 'pending', ?)",
        [(station_id, day, stamp) for station_id in stations],
    )
    _delete_unreadable_days(conn, site_id)


def _delete_unreadable_days(conn: sqlite3.Connection, site_id: int) -> None:
    """Delete the site's rows whose ``local_date`` is not a canonical
    ``YYYY-MM-DD``, with a WARNING and the count."""
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

    Today is the date of P1's record that holds ``now`` (with ``ensure``,
    after creating the site's missing records). With no such record the
    site has no current day: today's row is closed (no row is created or
    tried), and the date returned is ``now``'s date in the site's zone
    ``tz``.
    """
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


def _day_clock(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> _DayClock | None:
    """The clock of today's row dated ``local_date`` (plan 4.2).

    From P1's record of that date, in the zone saved on it; None when the
    site has no such record (no slot is computed and nothing falls back to
    ``sites.timezone``).
    """
    record = day_record(conn, site_id, local_date)
    return None if record is None else _clock_from_record(record)


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
    clock = _day_clock(conn, site_id, today)
    if clock is None or not clock.cap <= now < clock.end:
        return False
    return bool(wind_days_due(conn, site_id, today=today, now=now, limit=1))


def wind_lane_due(
    conn: sqlite3.Connection, site_id: int, tz: ZoneInfo, *, now: datetime
) -> bool:
    """True when the site's lane has work: the scheduler's due test (plan §8.13).

    Today's rows are due on their own schedule only; the ``five_minute``
    hold is not read here (plan 4.4). While no record holds ``now``, no
    today row is due and none is missing (4.2).
    """
    today, today_open = _lane_today(conn, site_id, tz, now, ensure=False)
    if not today_open:
        return False
    if wind_days_due(conn, site_id, today=today, now=now, limit=1):
        return True
    return _missing_today_row(conn, site_id, today)


# --- the job ---------------------------------------------------------------


@dataclass(frozen=True)
class _JobStart:
    tz_name: str
    today: date
    #: False while no record holds the start time.
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
    if today_open:
        ensure_wind_days(conn, site_id, tz, today)
    else:
        # No current day: no row is created (switch-over plan 7.2, R4).
        _delete_unreadable_days(conn, site_id)
    return _JobStart(tz_name=tz_name, today=today, today_open=today_open)


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
    if not start.today_open:
        # No record holds now: no today try (4.2).
        return
    chunk = _Chunk(
        db=db,
        writer=writer,
        client=client,
        site_id=site_id,
        tz_name=start.tz_name,
    )
    rows = await db.read(
        lambda conn: wind_days_due(
            conn, site_id, today=today, now=now, limit=_CHUNK_ROWS
        )
    )
    for position, row in enumerate(rows):
        if chunk.calls >= _CHUNK_CALLS:
            break
        outcome = await _run_row(chunk, row, position=position, total=len(rows))
        if outcome != "skipped":
            chunk.calls += 1


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
    the lane counter that the reservation belongs to. The lane tries only
    today's row (switch-over plan 7.2, R4), so it passes ``exempt=True`` and
    ``todays_row=True``.
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
    position: int,
    total: int,
) -> _RowOutcome:
    """Reserve, call and persist one station-day of today; §8.7's error table.

    A try that meets the ``five_minute`` hold (``AccessHeld``) or a
    reservation deferral (``ReservationDeferred``) sends nothing: one write
    moves the row and logs the try (plan 4.4, 4.5), and the row is
    ``"skipped"``, not counted as a call.
    """
    key = chunk.key()
    await pace_station_call(chunk.site_id, row.station_id, chunk.calls)
    site_id = chunk.site_id
    async with weathercom_call_lock():
        try:
            reservation = await chunk.writer.write(
                lambda conn: _reserve_wind_day_call(
                    conn,
                    site_id,
                    row.station_id,
                    exempt=True,
                    todays_row=True,
                )
            )
        except AccessHeld as held:
            until = held.until
            entry = await chunk.writer.write(
                lambda conn: _record_held_try(
                    conn,
                    site_id=site_id,
                    station_id=row.station_id,
                    local_date=row.local_date,
                    until=until,
                )
            )
            _log_try(entry)
            return "skipped"
        except ReservationDeferred as deferred:
            reason = deferred.reason
            deferral_end = deferred.next_attempt_at
            entry = await chunk.writer.write(
                lambda conn: _record_deferred_try(
                    conn,
                    site_id=site_id,
                    station_id=row.station_id,
                    local_date=row.local_date,
                    reason=reason,
                    deferral_end=deferral_end,
                )
            )
            _log_try(entry)
            return "skipped"
        if reservation is None:
            return "skipped"
        logger.debug(
            "wind day fetch site=%s station=%s date=%s",
            site_id,
            row.station_id,
            row.local_date,
        )
        records: Sequence[StationReading]
        no_content = False
        try:
            parsed = await fetch_all_1day(
                row.pws_station_id,
                key,
                client=chunk.client,
                backfill=False,
            )
            records = parsed.readings
        except Exception as exc:
            if not is_wind_history_no_content(exc):
                message = _row_message(row, exc)
                outcome = await _fetch_failed(chunk, row, reservation, exc, message)
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
                    records=records,
                    no_content=no_content,
                ),
            )
        except (sqlite3.OperationalError, JobControl):
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
                    message=message,
                ),
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
) -> _FailureOutcome:
    """Record a failed call. Raises ``JobDeferred`` where §8.7 says.

    The write that records the outcome also moves today's row to its next
    try and logs the try (plan 4.2, 4.5). A 401/403 returns ``"fetched"``,
    so the run's other today rows meet the new hold in their reservation. A
    429 or 5xx defers the job to the back-off's end, in the evening window
    as in the daytime; in the window a transport error returns
    ``"fetched"`` instead of raising, so the run goes on. In the daytime a
    deferral (429, 5xx, transport) moves today's row to
    ``min(deferral end, 23:00)``, and the raised wake is capped the same way.
    """
    station_id = row.station_id
    local_date = row.local_date
    site_id = chunk.site_id

    def begin(conn: sqlite3.Connection) -> _TodayTry | None:
        # Read before any write of the outcome: the slot is the one the row
        # held when the try started (4.2).
        current = _read_day(conn, station_id, local_date)
        if current is None:
            return None
        return _today_try(
            conn,
            current,
            site_id=site_id,
            station_id=station_id,
            local_date=local_date,
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
            # Today's 5-minute download was refused: hold the five_minute
            # kind for every station. The call was made, so nothing is
            # refunded; the row keeps its status and moves to its next try.
            # The provider's own reason is read here, outside the write.
            reason = provider_reason(response)
            refused = (
                f"station={row.pws_station_id} date={local_date.isoformat()}: "
                f"access refused (HTTP {status})"
            )
            if reason is not None:
                refused += f'; provider says "{reason}"'

            def record_refusal(conn: sqlite3.Connection) -> _TryLog | None:
                attempt = begin(conn)
                record_access_refused(conn, "five_minute", status, reason=reason)
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
                chunk.db, chunk.writer, record_refusal
            )
            _log_try(entry)
            return "fetched"
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
                chunk.db, chunk.writer, rate_limited
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
                chunk.db, chunk.writer, server_error
            )
            _log_try(entry)
            raise JobDeferred(_capped_wake(wake, entry)) from exc

        def http_failure(conn: sqlite3.Connection) -> _TryLog | None:
            attempt = begin(conn)
            failure(conn)
            return failed_try(conn, attempt, "provider", f"http {status}")

        _log_try(await write_after_reservation(chunk.db, chunk.writer, http_failure))
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

        _log_try(await write_after_reservation(chunk.db, chunk.writer, payload_failure))
        return "fetched"
    if isinstance(exc, (httpx.TransportError, TimeoutError)):
        refund = is_refundable_transport_error(exc)

        def transport_failure(
            conn: sqlite3.Connection,
        ) -> tuple[str, _TryLog | None]:
            attempt = begin(conn)
            wake = _transport_wake()
            if refund:
                _refund(conn, reservation)
            # A daytime deferral moves today's row to min(try + 15 min,
            # 23:00); in the window it goes to its next slot (4.2).
            failure(conn, daytime_until=_parse_due(wake))
            return wake, failed_try(conn, attempt, "network", "network")

        wake, entry = await write_after_reservation(
            chunk.db, chunk.writer, transport_failure
        )
        _log_try(entry)
        if entry is not None and entry.in_window:
            # 4.2: in the window the row is at its next slot and the run goes
            # on to its other today rows instead of deferring 15 minutes.
            return "fetched"
        raise JobDeferred(_capped_wake(wake, entry)) from exc
    await write_after_reservation(chunk.db, chunk.writer, failure)
    return "reraise"


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


def _refund(conn: sqlite3.Connection, reservation: Reservation) -> None:
    refund_budget(conn, reservation)
    add_lane_calls(conn, lane_counter_key(exempt=True), reservation.billing_day, -1)


def _set_last_error(
    conn: sqlite3.Connection, station_id: int, local_date: date, message: str
) -> None:
    conn.execute(
        "UPDATE station_wind_days SET last_error = ?, updated_at = ?"
        " WHERE station_id = ? AND local_date = ?",
        (message, isoformat_utc(utc_now()), station_id, local_date.isoformat()),
    )


# --- persist ---------------------------------------------------------------


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
    tried: datetime,
) -> _TodayTry:
    """A try of today's row, from its row as read before the outcome write."""
    clock = _day_clock(conn, site_id, local_date)
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
    deferral's end) when given. None when the row has no clock (its date has
    no record): the row keeps its stamp.
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


def _read_day(
    conn: sqlite3.Connection, station_id: int, local_date: date
) -> sqlite3.Row | None:
    row: sqlite3.Row | None = conn.execute(
        """
        SELECT status, attempts, next_attempt_at
        FROM station_wind_days
        WHERE station_id = ? AND local_date = ?
        """,
        (station_id, local_date.isoformat()),
    ).fetchone()
    return row


def _today_window(
    conn: sqlite3.Connection, site_id: int, *, now: datetime
) -> tuple[datetime, datetime] | None:
    """The bounds of today's row's window: yesterday's start to today's end
    (4.1). ``_persist_day`` caps the upper end at now plus ``_FUTURE_SLACK``.

    Today and yesterday are P1's record holding ``now`` and the site's
    record dated the day before it, by their stored bounds, after creating
    the site's missing records as the guard would; with no record the day
    before, today's bounds alone. ``None`` while no record holds ``now``
    (the site has no current day, 4.2): nothing is kept.
    """
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
    records: Sequence[StationReading],
    no_content: bool = False,
) -> _TryLog | None:
    """Store today's fetched readings and advance today's row (plan 4.1, 4.2).

    The lane selects only the row of the current day (switch-over plan 7.2,
    R4), so ``local_date`` is today's date. The try is a successful one: the
    row stays ``partial``, moves to its next try, and its try is logged in
    this transaction (4.5) with outcome ``stored``, or ``no_readings``
    (``no_content`` for a 204, set by the caller, ``empty`` for an empty
    list); the log entry is returned for the INFO line. No hourly station
    row is derived here: the close writes those (R5).

    ``records`` are the download's readings. Those inside yesterday's and
    today's bounds (``_today_window``), capped at now plus ``_FUTURE_SLACK``
    so that a station clock running ahead stores nothing in the future, go
    through ``store_readings`` with source ``five_minute``, which asks the
    closed-day guard (``READING``) at each ``obs_at``; a refused reading is
    not stored. Then ``end_poll_pause_on_fresh_reading`` ends the station's
    current-conditions poll pause when a stored ``five_minute`` reading is
    fresh.
    """
    now = utc_now()
    start, end, _ = local_day_slots(local_date, tz_name)
    bounds = _today_window(conn, site_id, now=now)
    window = (
        None if bounds is None else (bounds[0], min(bounds[1], now + _FUTURE_SLACK))
    )
    kept = (
        []
        if window is None
        else [r for r in records if window[0] <= r.obs_at < window[1]]
    )
    if len(kept) != len(records):
        logger.debug(
            "wind records outside the stored window dropped station=%s date=%s n=%s",
            station_id,
            local_date,
            len(records) - len(kept),
        )
    stored = store_readings(
        conn,
        station_id=station_id,
        source="five_minute",
        readings=kept,
        fetched_at=now,
        now_utc=now,
    )
    end_poll_pause_on_fresh_reading(conn, station_id, now_utc=now)
    row = _read_day(conn, station_id, local_date)
    if row is None:
        logger.warning(
            "wind day row missing at persist station=%s date=%s",
            station_id,
            local_date,
        )
        return None
    count_row = conn.execute(
        "SELECT COUNT(*) AS n FROM station_wind_records"
        " WHERE station_id = ? AND obs_at >= ? AND obs_at < ?"
        " AND source = 'five_minute' AND speed_kmh IS NOT NULL",
        (station_id, isoformat_utc(start), isoformat_utc(end)),
    ).fetchone()
    record_count = int(count_row["n"])
    attempt = _today_try(
        conn,
        row,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tried=now,
    )
    moved = _next_try_at(conn, attempt, success=True)
    # No clock (no record of the date): the row keeps its stamp.
    kept_stamp = row["next_attempt_at"]
    next_attempt_at = (
        moved if moved is not None else None if kept_stamp is None else str(kept_stamp)
    )
    stamp = isoformat_utc(now)
    conn.execute(
        """
        UPDATE station_wind_days
        SET status = 'partial', next_attempt_at = ?, record_count = ?,
            last_ok_at = ?, last_error = NULL, updated_at = ?
        WHERE station_id = ? AND local_date = ?
        """,
        (
            next_attempt_at,
            record_count,
            stamp,
            stamp,
            station_id,
            local_date.isoformat(),
        ),
    )
    clear_domain_backoff(conn, source_domain("weathercom"))
    # A 2xx on today's 5-minute download: the five_minute kind is granted.
    clear_access_refused(conn, "five_minute")
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


def _row_failure(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    message: str,
    daytime_until: datetime | None = None,
    not_before: datetime | None = None,
) -> None:
    """The row-failure rule (plan §8.7) for today's row.

    Today's row stays ``partial`` and moves to its next try as a try that
    did not succeed (plan 4.2): in the daytime a deferral's end
    ``daytime_until`` moves it to ``min(that end, 23:00)``; in the evening
    window ``not_before``, a back-off's end, moves it to the first slot at
    or after that end. The lane tries no other row (switch-over plan 7.2,
    R4).
    """
    row = _read_day(conn, station_id, local_date)
    if row is None:
        return
    now = utc_now()
    stamp = isoformat_utc(now)
    attempt = _today_try(
        conn,
        row,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
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
