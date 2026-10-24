"""The wind-history lane: raw station wind records, day by day.

One ``fetch_obs`` job with the job key ``wind-days`` runs one bounded chunk
for one site. It fetches each station-day's raw 5-minute records and derives
the pair-max hourly station wind (``max_adjacent_pair_mean_wind``). It keeps
today and yesterday current, with a final call shortly before local midnight,
and while the ``history_all`` hold is held it finishes past days with the
records they have.

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
    local_day_slots,
    parse_utc,
    utc_now,
)
from wxverify.core.units import kmh_to_ms
from wxverify.db.connection import Database, FencedWriter, StaleGenerationError
from wxverify.db.queue import enqueue_if_absent
from wxverify.db.tz_generations import published_generation_clause
from wxverify.db.wind_basis import (
    PAIR_MAX_WIND_SQL,
    WIND_FINAL_LEAD,
    AuthHold,
    AuthHoldEndpoint,
    clear_auth_hold,
    count_missed_final,
    read_auth_hold,
    read_auth_holds,
    write_auth_hold,
)
from wxverify.forecast.wind_blend import TRAINING_DAYS
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

_CHUNK_CALLS: Final = 6
_CHUNK_ROWS: Final = 24
_MAX_ATTEMPTS: Final = 3
_TRANSPORT_DEFER: Final = timedelta(minutes=15)
_FINAL_RETRY_LEAD: Final = timedelta(minutes=20)
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
_ENDPOINTS: Final[tuple[AuthHoldEndpoint, ...]] = ("history_all", "all_1day")
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


class WindHeadroomExhausted(Exception):
    """The backfill headroom is below one call. Nothing has been written."""


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


def endpoint_for(local_date: date, today: date) -> AuthHoldEndpoint:
    """Today's row calls ``all_1day``; every past date calls ``history_all``."""
    return "all_1day" if local_date == today else "history_all"


def open_endpoints(
    holds: Mapping[AuthHoldEndpoint, AuthHold], *, probing_open: bool
) -> frozenset[AuthHoldEndpoint]:
    """The endpoints with no hold; a ``probing`` hold is open when asked."""
    opened: set[AuthHoldEndpoint] = set()
    for endpoint in _ENDPOINTS:
        hold = holds.get(endpoint)
        if hold is None or (probing_open and hold.status == "probing"):
            opened.add(endpoint)
    return frozenset(opened)


def wind_days_due(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    today: date,
    now: datetime,
    open_endpoints: Collection[AuthHoldEndpoint],
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


def wind_lane_due(
    conn: sqlite3.Connection, site_id: int, tz: ZoneInfo, *, now: datetime
) -> bool:
    """True when the site's lane has work: the scheduler's due test (plan §8.13)."""
    today = now.astimezone(tz).date()
    holds = read_auth_holds(conn)
    opened = open_endpoints(holds, probing_open=True)
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
    ensure_wind_days(conn, site_id, tz, utc_now().astimezone(tz).date())
    holds = read_auth_holds(conn)
    return _JobStart(tz_name=tz_name, holds=holds)


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
    today = now.astimezone(ZoneInfo(start.tz_name)).date()
    chunk = _Chunk(
        db=db,
        writer=writer,
        client=client,
        site_id=site_id,
        tz_name=start.tz_name,
        today=today,
    )
    await _finish_waiting_days(chunk, start)
    for endpoint in _ENDPOINTS:
        hold = start.holds.get(endpoint)
        if hold is None or hold.status != "probing":
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
    opened = set(open_endpoints(await db.read(read_auth_holds), probing_open=False))
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
    opened: frozenset[AuthHoldEndpoint],
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
    conn: sqlite3.Connection, site_id: int, station_id: int, *, exempt: bool
) -> Reservation | None:
    """Reserve one weather.com call for a station-day, in one transaction.

    Site disabled -> ``JobCancelled``; station disabled or moved -> None (skip
    the row); then domain backoff, the backfill headroom (non-exempt only,
    ``WindHeadroomExhausted`` below one call), the budget, and the lane
    counter that the reservation belongs to.
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
    check_domain_backoff(conn, source_domain("weathercom"))
    if not exempt:
        headroom = backfill_headroom(conn, now=utc_now())
        if headroom is None or headroom < 1:
            raise WindHeadroomExhausted()
    reservation = reserve_budget(conn, "weathercom", 1)
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
    """Reserve, call and persist one station-day; the error table of §8.7."""
    key = chunk.key()
    await pace_station_call(chunk.site_id, row.station_id, chunk.calls)
    if not exempt:
        await wait_for_backfill_slot()
    site_id = chunk.site_id
    async with weathercom_call_lock():
        reservation = await chunk.writer.write(
            lambda conn: _reserve_wind_day_call(
                conn, site_id, row.station_id, exempt=exempt
            )
        )
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
        try:
            if row.local_date == chunk.today:
                records = await fetch_all_1day(
                    row.pws_station_id,
                    key,
                    client=chunk.client,
                    backfill=not exempt,
                )
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
        try:
            await write_after_reservation(
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
    """Record a failed call (not a probe). Raises ``JobDeferred`` where §8.7 says."""
    station_id = row.station_id
    local_date = row.local_date
    tz_name = chunk.tz_name
    today = chunk.today

    def failure(conn: sqlite3.Connection) -> None:
        _row_failure(
            conn,
            station_id=station_id,
            local_date=local_date,
            tz_name=tz_name,
            today=today,
            message=message,
        )

    if isinstance(exc, httpx.HTTPStatusError):
        response = exc.response
        status = response.status_code
        if status in (401, 403):
            endpoint = endpoint_for(local_date, today)
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

            def rate_limited(conn: sqlite3.Connection) -> str | None:
                next_at = record_http_backoff(conn, response)
                _set_last_error(conn, station_id, local_date, message)
                return next_at

            next_at = await write_after_reservation(
                chunk.db, chunk.writer, rate_limited, reservation
            )
            raise JobDeferred(next_at or _transport_wake()) from exc
        if status >= 500:

            def server_error(conn: sqlite3.Connection) -> str | None:
                next_at = record_http_backoff(conn, response)
                failure(conn)
                return next_at

            next_at = await write_after_reservation(
                chunk.db, chunk.writer, server_error, reservation
            )
            raise JobDeferred(next_at or _transport_wake()) from exc
        await write_after_reservation(chunk.db, chunk.writer, failure, reservation)
        return "fetched"
    if (
        isinstance(exc, UpstreamPayloadError)
        and exc.diagnostics.kind in _PAYLOAD_ROW_FAULTS
    ):
        await write_after_reservation(chunk.db, chunk.writer, failure, reservation)
        return "fetched"
    if isinstance(exc, (httpx.TransportError, TimeoutError)):
        refund = is_refundable_transport_error(exc)

        def transport_failure(conn: sqlite3.Connection) -> None:
            if refund:
                _refund(conn, reservation, exempt=exempt)
            failure(conn)

        await write_after_reservation(
            chunk.db, chunk.writer, transport_failure, reservation
        )
        raise JobDeferred(_transport_wake()) from exc
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
        "SELECT MAX(obs_at) AS latest FROM station_wind_records WHERE station_id = ?",
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
    conn: sqlite3.Connection, now: datetime, end: datetime, *, failed: bool
) -> str:
    """Next today call: the obs cadence capped at the final call (end - 35 min,
    before the one-day product resets); one retry at end - 20 min after a
    failed final call; otherwise end + 1 h."""
    interval = get_number_setting(conn, "obs_interval_minutes", 180, minimum=30)
    final = end - WIND_FINAL_LEAD
    if now < final:
        next_at = min(now + timedelta(minutes=interval), final)
    elif failed and now < end - _FINAL_RETRY_LEAD:
        next_at = end - _FINAL_RETRY_LEAD
    else:
        next_at = end + _ONE_HOUR
    return isoformat_utc(next_at)


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
        SELECT status, attempts, refetch_at, refetched FROM station_wind_days
        WHERE station_id = ? AND local_date = ?
        """,
        (station_id, local_date.isoformat()),
    ).fetchone()
    return row


def _persist_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    tz_name: str,
    today: date,
    records: Sequence[WindRecord],
    probe: AuthHoldEndpoint | None,
) -> None:
    """Store one fetched station-day, derive its pair-max station rows, and
    advance its row."""
    now = utc_now()
    yesterday = today - _ONE_DAY
    start, end, _ = local_day_slots(local_date, tz_name)
    low = start - _STORE_MARGIN
    high = min(end + _STORE_MARGIN, now + _FUTURE_SLACK)
    kept = [record for record in records if low <= record.obs_at < high]
    if len(kept) != len(records):
        logger.debug(
            "wind records outside the stored window dropped station=%s date=%s n=%s",
            station_id,
            local_date,
            len(records) - len(kept),
        )
    conn.executemany(
        """
        INSERT INTO station_wind_records (station_id, obs_at, speed_kmh)
        VALUES (?, ?, ?)
        ON CONFLICT(station_id, obs_at) DO UPDATE SET speed_kmh=excluded.speed_kmh
        """,
        [(station_id, isoformat_utc(r.obs_at), r.speed_kmh) for r in kept],
    )
    _derive_days(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=tz_name,
        today=today,
    )
    row = _read_day(conn, station_id, local_date)
    if row is None:
        logger.warning(
            "wind day row missing at persist station=%s date=%s",
            station_id,
            local_date,
        )
        return
    attempts = int(row["attempts"])
    refetch = _is_refetch(row)
    count_row = conn.execute(
        "SELECT COUNT(*) AS n FROM station_wind_records"
        " WHERE station_id = ? AND obs_at >= ? AND obs_at < ?",
        (station_id, isoformat_utc(start), isoformat_utc(end)),
    ).fetchone()
    record_count = int(count_row["n"])
    next_attempt_at: str | None = None
    if local_date == today:
        status = "partial"
        next_attempt_at = _today_next_attempt(conn, now, end, failed=False)
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
        _reconcile(conn, station_id, local_date, tz_name, record_count)
    clear_domain_backoff(conn, source_domain("weathercom"))
    if probe is not None:
        clear_auth_hold(conn, probe)


def _derive_days(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    tz_name: str,
    today: date,
) -> None:
    """Pair hours for d and its fetched/partial neighbours, and write, delete
    and materialize the station rows."""
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
    changed: set[datetime] = set()
    for hour in sorted(pairs):
        speed = pairs[hour]
        if write_station_observation(
            conn,
            station_id=station_id,
            variable="wind",
            valid_at=isoformat_utc(hour),
            value=kmh_to_ms(speed),
            source_raw=f"pair-max {round(speed, 2)} km/h",
        ):
            changed.add(hour)
    for hour in sorted(set(all_hours) - set(pairs)):
        deleted = conn.execute(
            "DELETE FROM station_observations WHERE id IN ("
            " SELECT so.id FROM station_observations so"
            " WHERE so.station_id = ? AND so.variable = 'wind'"
            f" AND so.valid_at = ? AND {PAIR_MAX_WIND_SQL})",
            (station_id, isoformat_utc(hour)),
        ).rowcount
        if deleted:
            changed.add(hour)
    for hour in sorted(changed):
        _materialize(conn, site_id, isoformat_utc(hour))
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
    station_id: int,
    local_date: date,
    tz_name: str,
    record_count: int,
) -> None:
    """Schedule one refetch of a short yesterday (plan §8.14)."""
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
    collection is logged (plan §7) unless ``all_1day`` has a hold.
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
    if finished_yesterday and "all_1day" not in start.holds:
        missed, total = await chunk.db.read(
            lambda conn: count_missed_final(conn, site_id, yesterday, tz_name)
        )
        if missed > 0:
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
    stamp = isoformat_utc(utc_now())
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
        " WHERE station_id = ? AND obs_at >= ? AND obs_at < ?",
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
    )
    return True


def _row_failure(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    local_date: date,
    tz_name: str,
    today: date,
    message: str,
) -> None:
    """The row-failure rule (plan §8.7): refetch, today, then a past date."""
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
        end = local_day_slots(local_date, tz_name)[1]
        conn.execute(
            "UPDATE station_wind_days SET status = 'partial', next_attempt_at = ?,"
            " last_error = ?, updated_at = ? WHERE station_id = ? AND local_date = ?",
            (
                _today_next_attempt(conn, now, end, failed=True),
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


def _materialize(conn: sqlite3.Connection, site_id: int, valid_at: str) -> None:
    """``materialize_consensus`` for one wind hour.

    A string that will not parse (an imported row) cannot go through
    ``materialize_consensus`` when the persistence feed exists, because that
    parses it. Such a string takes the same invalidation steps directly and
    then deletes the observation, with a WARNING, so the switch cannot wedge.
    """
    try:
        parse_utc(valid_at)
    except ValueError:
        _invalidate_unparseable(conn, site_id, valid_at)
        return
    try:
        materialize_consensus(conn, site_id=site_id, variable="wind", valid_at=valid_at)
    except OverflowError:
        _invalidate_unparseable(conn, site_id, valid_at)


def _invalidate_unparseable(
    conn: sqlite3.Connection, site_id: int, valid_at: str
) -> None:
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
