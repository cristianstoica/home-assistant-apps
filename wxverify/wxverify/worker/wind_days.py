"""The wind-history lane: raw station wind records, day by day, and the switch.

One ``fetch_obs`` job with the job key ``wind-days`` runs one bounded chunk
for one site. In ``staging`` it fetches every past station-day's raw 5-minute
records (counts only), then switches the site's station wind to
``max_adjacent_pair_mean_wind``: pass (i) removes the old wind, pass (ii)
installs the new, and a rescore follows. In ``pair_max`` it keeps today and
yesterday current. ``switching`` and ``rescoring`` never call the provider.

The state machine is in :mod:`wxverify.db.wind_basis`; the lane's share of
the weather.com budget is in :mod:`wxverify.collection.wind_quota`.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import sqlite3
import statistics
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final, Literal, LiteralString
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
from wxverify.db.runtime_state import (
    delete_runtime_state,
    get_runtime_state,
    set_runtime_state,
    set_runtime_state_now,
)
from wxverify.db.tz_generations import published_generation_clause
from wxverify.db.wind_basis import (
    LEGACY_WIND_SQL,
    PAIR_MAX_WIND_SQL,
    STATES,
    AuthHold,
    AuthHoldEndpoint,
    WindCursor,
    clear_auth_hold,
    json_object,
    julian_to_utc,
    read_auth_holds,
    read_wind_cursor,
    set_wind_basis_state,
    stamp_wind_progress,
    transition_wind_basis,
    wind_basis_state,
    wind_blocked_key,
    wind_cursor_key,
    wind_done_at_key,
    wind_report_key,
    write_auth_hold,
    write_wind_cursor,
)
from wxverify.forecast.wind_blend import MIN_OBS_HOURS
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
from wxverify.worker.score_batches import (
    SCORING_CAS_ATTEMPTS,
    ScoringInputsBusy,
    run_batched_scoring,
    run_split_pair_phases,
)
from wxverify.worker.station_pacing import pace_station_call, weathercom_call_lock

logger = logging.getLogger(__name__)

WIND_DAYS_JOB_KEY: Final = "wind-days"
#: The scheduler's success cooldown between two lane jobs of one site.
WIND_DAYS_SPACING: Final = timedelta(minutes=5)
#: Free space the switch needs beside the database: max(2 x the peak growth
#: measured in the copy rehearsal, 256 MiB), rounded up to a MiB (plan §17).
WIND_SWITCH_MIN_FREE_BYTES: Final = 374 * 1024 * 1024

_RESCORE_BUSY_DEFER: Final = timedelta(seconds=60)
_CHUNK_CALLS: Final = 6
_CHUNK_ROWS: Final = 24
_DAY_TRANSACTIONS: Final = 14
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
_FAILED_FINAL_SHARE: Final = 0.10
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


def _has_pair_max_rows(conn: sqlite3.Connection, site_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM station_observations so"
        " JOIN stations s ON s.id = so.station_id"
        " WHERE s.site_id = ? AND so.variable = 'wind'"
        f" AND {PAIR_MAX_WIND_SQL} LIMIT 1",
        (site_id,),
    ).fetchone()
    return row is not None


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
    state = wind_basis_state(conn, site_id)
    if state not in STATES or state in ("switching", "rescoring"):
        return True
    if state == "staging" and _has_pair_max_rows(conn, site_id):
        return True
    today = now.astimezone(tz).date()
    opened = open_endpoints(read_auth_holds(conn), probing_open=True)
    if wind_days_due(
        conn, site_id, today=today, now=now, open_endpoints=opened, limit=1
    ):
        return True
    return _missing_today_row(conn, site_id, today)


# --- the job ---------------------------------------------------------------


@dataclass(frozen=True)
class _JobStart:
    tz_name: str
    state: str
    has_pair_max: bool
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
    state = wind_basis_state(conn, site_id)
    holds = read_auth_holds(conn)
    if state not in STATES:
        logger.warning(
            "wind basis unknown value reset to staging site=%s value=%r",
            site_id,
            state,
        )
        set_wind_basis_state(conn, site_id, "staging")
        state = "staging"
    has_pair_max = state == "staging" and _has_pair_max_rows(conn, site_id)
    return _JobStart(
        tz_name=tz_name, state=state, has_pair_max=has_pair_max, holds=holds
    )


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
    if start.state == "switching":
        await _run_switching(writer, site_id, start.tz_name)
        return
    if start.state == "rescoring":
        await _run_rescoring(writer, site_id)
        return
    if start.state == "staging":
        if start.has_pair_max:
            await _run_staging_purge(writer, site_id, start.tz_name)
            return
        free = _free_bytes(db.path)
        try:
            switched = await writer.write(
                lambda conn: _switch_check(conn, site_id, start.tz_name, free)
            )
        except (sqlite3.Error, ValueError) as exc:
            logger.warning(
                "wind switch check failed, retried next job site=%s: %s",
                site_id,
                sanitized_exception(exc),
            )
            switched = False
        if switched:
            return
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
    site_id = chunk.site_id
    station_id = row.station_id
    local_date = row.local_date
    tz_name = chunk.tz_name
    today = chunk.today

    def failure(conn: sqlite3.Connection) -> None:
        _row_failure(
            conn,
            site_id=site_id,
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


def _today_next_attempt(conn: sqlite3.Connection, now: datetime, end: datetime) -> str:
    """now + the obs interval; at or past the day's end, end + 1 h."""
    interval = get_number_setting(conn, "obs_interval_minutes", 180, minimum=30)
    next_at = now + timedelta(minutes=interval)
    if next_at >= end:
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
    """Store one fetched station-day and advance its row (plan §8.5).

    The site's state, read here, picks the persist: ``pair_max`` writes and
    materializes pair-max station rows; any other state updates counts only.
    """
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
    pair_max = wind_basis_state(conn, site_id) == "pair_max"
    _derive_days(
        conn,
        site_id=site_id,
        station_id=station_id,
        local_date=local_date,
        tz_name=tz_name,
        today=today,
        write_rows=pair_max,
    )
    if not pair_max:
        legacy = conn.execute(
            "SELECT COUNT(*) AS n FROM station_observations so"
            " WHERE so.station_id = ? AND so.variable = 'wind'"
            f" AND {LEGACY_WIND_SQL}"
            " AND julianday(so.valid_at) >= julianday(?)"
            " AND julianday(so.valid_at) < julianday(?)",
            (station_id, isoformat_utc(start), isoformat_utc(end)),
        ).fetchone()
        conn.execute(
            "UPDATE station_wind_days SET legacy_hours = ?"
            " WHERE station_id = ? AND local_date = ?",
            (int(legacy["n"]), station_id, local_date.isoformat()),
        )
    row = _read_day(conn, station_id, local_date)
    if row is None:
        logger.warning(
            "wind day row missing at persist station=%s date=%s",
            station_id,
            local_date,
        )
        return
    old_status = str(row["status"])
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
        next_attempt_at = _today_next_attempt(conn, now, end)
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
    if not refetch and local_date < yesterday and status != old_status:
        stamp_wind_progress(conn, site_id)


def _derive_days(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: date,
    tz_name: str,
    today: date,
    write_rows: bool,
) -> None:
    """Pair hours for d and its fetched/partial neighbours; in ``pair_max``
    also write, delete and materialize the station rows."""
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
    if write_rows:
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


def _row_failure(
    conn: sqlite3.Connection,
    *,
    site_id: int,
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
    old_status = str(row["status"])
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
                _today_next_attempt(conn, now, end),
                message,
                stamp,
                station_id,
                local_date.isoformat(),
            ),
        )
        new_status = "partial"
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
        new_status = "failed"
    if local_date < today - _ONE_DAY and new_status != old_status:
        stamp_wind_progress(conn, site_id)


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


# --- day windows -----------------------------------------------------------


def _local_today(tz_name: str) -> date:
    return utc_now().astimezone(ZoneInfo(tz_name)).date()


def _julian_local_window(julian: float, tz_name: str) -> tuple[date, str, str] | None:
    """The local day holding a julianday value, and its UTC bounds."""
    try:
        day = julian_to_utc(julian).astimezone(ZoneInfo(tz_name)).date()
        start, end, _ = local_day_slots(day, tz_name)
    except (ZoneInfoNotFoundError, ValueError, OverflowError, OSError):
        return None
    return day, isoformat_utc(start), isoformat_utc(end)


def _range_sql(column: LiteralString) -> LiteralString:
    """``[start, end)`` by julianday, or exactly the MIN; NULL bounds -> MIN only."""
    return (
        f"((julianday({column}) >= julianday(?)"
        f" AND julianday({column}) < julianday(?))"
        f" OR julianday({column}) = ?)"
    )


def _materialize_all(
    conn: sqlite3.Connection, site_id: int, valid_ats: Collection[str]
) -> None:
    for valid_at in sorted(valid_ats):
        _materialize(conn, site_id, valid_at)


def _delete_station_rows(conn: sqlite3.Connection, ids: Sequence[int]) -> None:
    conn.executemany(
        "DELETE FROM station_observations WHERE id = ?", [(row_id,) for row_id in ids]
    )


# --- staging purge ---------------------------------------------------------


async def _run_staging_purge(writer: FencedWriter, site_id: int, tz_name: str) -> None:
    for _ in range(_DAY_TRANSACTIONS):
        more = await writer.write(
            lambda conn: _staging_purge_day(conn, site_id, tz_name)
        )
        if not more:
            return


def _staging_purge_day(conn: sqlite3.Connection, site_id: int, tz_name: str) -> bool:
    """Delete one local day of pair-max rows in ``staging`` and re-materialize.

    True while more days may remain.
    """
    if wind_basis_state(conn, site_id) != "staging":
        return False
    row = conn.execute(
        "SELECT MIN(julianday(so.valid_at)) AS j FROM station_observations so"
        " JOIN stations s ON s.id = so.station_id"
        " WHERE s.site_id = ? AND so.variable = 'wind'"
        f" AND {PAIR_MAX_WIND_SQL}",
        (site_id,),
    ).fetchone()
    if row is None or row["j"] is None:
        stray = conn.execute(
            "SELECT so.id, so.valid_at FROM station_observations so"
            " JOIN stations s ON s.id = so.station_id"
            " WHERE s.site_id = ? AND so.variable = 'wind'"
            f" AND {PAIR_MAX_WIND_SQL} AND julianday(so.valid_at) IS NULL",
            (site_id,),
        ).fetchall()
        if not stray:
            return False
        _delete_station_rows(conn, [int(r["id"]) for r in stray])
        _materialize_all(conn, site_id, {str(r["valid_at"]) for r in stray})
        logger.warning(
            "pair-max wind rows with an unreadable time purged site=%s rows=%s",
            site_id,
            len(stray),
        )
        stamp_wind_progress(conn, site_id)
        return False
    julian = float(row["j"])
    window = _julian_local_window(julian, tz_name)
    lo, hi = (None, None) if window is None else (window[1], window[2])
    rows = conn.execute(
        "SELECT so.id, so.valid_at FROM station_observations so"
        " JOIN stations s ON s.id = so.station_id"
        " WHERE s.site_id = ? AND so.variable = 'wind'"
        f" AND {PAIR_MAX_WIND_SQL} AND {_range_sql('so.valid_at')}",
        (site_id, lo, hi, julian),
    ).fetchall()
    _delete_station_rows(conn, [int(r["id"]) for r in rows])
    _materialize_all(conn, site_id, {str(r["valid_at"]) for r in rows})
    stamp_wind_progress(conn, site_id)
    return True


# --- switch check (plan §8.9) ----------------------------------------------


@dataclass(frozen=True)
class _SwitchCounts:
    retry_before_yesterday: int
    retry_before_today: int
    yesterday_fetched: int
    yesterday_usable: int
    failed_final: int
    before_today: int


def _switch_counts(
    conn: sqlite3.Connection, site_id: int, today: date
) -> _SwitchCounts:
    yesterday = (today - _ONE_DAY).isoformat()
    row = conn.execute(
        f"""
        SELECT
          SUM(CASE WHEN d.local_date < :yesterday AND {_RETRYABLE_SQL}
                   THEN 1 ELSE 0 END) AS retry_before_yesterday,
          SUM(CASE WHEN d.local_date < :today AND {_RETRYABLE_SQL}
                   THEN 1 ELSE 0 END) AS retry_before_today,
          SUM(CASE WHEN d.local_date = :yesterday AND d.status = 'fetched'
                   THEN 1 ELSE 0 END) AS yesterday_fetched,
          SUM(CASE WHEN d.local_date = :yesterday AND d.status = 'fetched'
                        AND d.pair_hours >= :min_pair_hours
                   THEN 1 ELSE 0 END) AS yesterday_usable,
          SUM(CASE WHEN d.local_date < :today AND d.status = 'failed'
                        AND d.attempts >= 3
                   THEN 1 ELSE 0 END) AS failed_final,
          SUM(CASE WHEN d.local_date < :today THEN 1 ELSE 0 END) AS before_today
        FROM station_wind_days d JOIN stations st ON st.id = d.station_id
        WHERE st.site_id = :site_id AND st.enabled = 1
        """,
        {
            "site_id": site_id,
            "today": today.isoformat(),
            "yesterday": yesterday,
            "min_pair_hours": MIN_OBS_HOURS,
        },
    ).fetchone()
    return _SwitchCounts(
        retry_before_yesterday=int(row["retry_before_yesterday"] or 0),
        retry_before_today=int(row["retry_before_today"] or 0),
        yesterday_fetched=int(row["yesterday_fetched"] or 0),
        yesterday_usable=int(row["yesterday_usable"] or 0),
        failed_final=int(row["failed_final"] or 0),
        before_today=int(row["before_today"] or 0),
    )


def _generation_building(conn: sqlite3.Connection, site_id: int) -> bool:
    row = conn.execute(
        """
        SELECT id FROM timezone_generations
        WHERE site_id = ? AND state = 'building'
        LIMIT 1
        """,
        (site_id,),
    ).fetchone()
    return row is not None


def _free_bytes(path: str) -> int | None:
    """Free bytes on the database's filesystem; None when it cannot be read."""
    try:
        return shutil.disk_usage(os.path.dirname(os.path.abspath(path))).free
    except OSError:
        return None


def _report_counts(
    conn: sqlite3.Connection, site_id: int, upper: date
) -> dict[str, object]:
    """The rebuild report's counts over enabled-station rows before ``upper``."""
    row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN d.status = 'fetched' THEN 1 ELSE 0 END) AS fetched,
          SUM(CASE WHEN d.status = 'unavailable' THEN 1 ELSE 0 END) AS unavailable,
          SUM(CASE WHEN d.status = 'failed' AND d.attempts >= 3
                   THEN 1 ELSE 0 END) AS failed_final,
          SUM(CASE WHEN d.status = 'unavailable' AND d.legacy_hours > 0
                   THEN 1 ELSE 0 END) AS unavailable_with_legacy,
          SUM(d.pair_hours) AS pair_hours,
          SUM(COALESCE(d.legacy_hours, 0)) AS legacy_hours,
          MIN(d.local_date) AS first_date,
          MAX(d.local_date) AS last_date
        FROM station_wind_days d JOIN stations st ON st.id = d.station_id
        WHERE st.site_id = ? AND st.enabled = 1 AND d.local_date < ?
        """,
        (site_id, upper.isoformat()),
    ).fetchone()
    return {
        "fetched": int(row["fetched"] or 0),
        "unavailable": int(row["unavailable"] or 0),
        "failed_final": int(row["failed_final"] or 0),
        "unavailable_with_legacy": int(row["unavailable_with_legacy"] or 0),
        "pair_hours": int(row["pair_hours"] or 0),
        "legacy_hours": int(row["legacy_hours"] or 0),
        "first_date": None if row["first_date"] is None else str(row["first_date"]),
        "last_date": None if row["last_date"] is None else str(row["last_date"]),
    }


def _switch_check(
    conn: sqlite3.Connection, site_id: int, tz_name: str, free: int | None
) -> bool:
    """Pass, waiting or blocked (plan §8.9). True only on a pass."""
    today = _local_today(tz_name)
    counts = _switch_counts(conn, site_id, today)
    p1 = counts.retry_before_yesterday == 0
    p2 = counts.yesterday_usable > 0
    p3 = counts.failed_final <= _FAILED_FINAL_SHARE * counts.before_today
    p4 = not _has_pair_max_rows(conn, site_id)
    p5 = not _generation_building(conn, site_id)
    p6 = not read_auth_holds(conn)
    if free is None:
        logger.warning("wind switch held: free disk space unreadable site=%s", site_id)
    p7 = free is not None and free >= WIND_SWITCH_MIN_FREE_BYTES
    exhausted = counts.retry_before_today == 0 and (
        counts.yesterday_usable == 0 or not p3
    )
    if not p7 or exhausted:
        if free is None:
            reason = "free disk space could not be read"
        elif not p7:
            reason = "not enough free disk space for the switch"
        elif not p3:
            reason = "too many station-days failed"
        elif counts.yesterday_fetched > 0:
            reason = (
                "no station had 22 hours of wind readings at most 10 minutes "
                "apart yesterday"
            )
        else:
            reason = "no station returned yesterday's data"
        _write_blocked(conn, site_id, today, reason)
        return False
    if not (p1 and p2 and p3 and p4 and p5 and p6):
        return False
    if not transition_wind_basis(conn, site_id, expected="staging", new="switching"):
        logger.warning("wind switch lost its compare-and-set site=%s", site_id)
        return False
    report: dict[str, object] = {
        "switched_at": isoformat_utc(utc_now()),
        **_report_counts(conn, site_id, today),
    }
    set_runtime_state(conn, wind_report_key(site_id), json.dumps(report))
    delete_runtime_state(conn, wind_blocked_key(site_id))
    write_wind_cursor(conn, site_id, phase="purge", cursor_date=None)
    stamp_wind_progress(conn, site_id)
    logger.info("wind switch started site=%s", site_id)
    return True


def _write_blocked(
    conn: sqlite3.Connection, site_id: int, today: date, reason: str
) -> None:
    """Write the blocked key; reset failed-final rows at most once per UTC day."""
    key = wind_blocked_key(site_id)
    raw = get_runtime_state(conn, key)
    existing = json_object(raw)
    now = utc_now()
    since: object = None if existing is None else existing.get("since")
    reset_on: object = None if existing is None else existing.get("reset_on")
    utc_day = now.date().isoformat()
    if reset_on != utc_day:
        scope = (
            " WHERE status = 'failed' AND attempts >= 3 AND local_date < ?"
            " AND station_id IN (SELECT id FROM stations"
            " WHERE site_id = ? AND enabled = 1)"
        )
        params = (today.isoformat(), site_id)
        earliest = conn.execute(
            "SELECT MIN(local_date) AS first FROM station_wind_days" + scope, params
        ).fetchone()
        reset = conn.execute(
            "UPDATE station_wind_days SET status = 'pending', attempts = 0,"
            " next_attempt_at = NULL, updated_at = ?" + scope,
            (isoformat_utc(now), *params),
        ).rowcount
        if reset:
            logger.warning(
                "wind switch blocked, failed station-days reset site=%s rows=%s",
                site_id,
                reset,
            )
            first = earliest["first"]
            if first is not None and str(first) < (today - _ONE_DAY).isoformat():
                stamp_wind_progress(conn, site_id)
        reset_on = utc_day
    value = {
        "reason": reason,
        "since": since if isinstance(since, str) else isoformat_utc(now),
        "reset_on": reset_on,
    }
    if existing != value:
        set_runtime_state(conn, key, json.dumps(value))
        if existing is None or existing.get("reason") != reason:
            logger.warning("wind switch blocked site=%s: %s", site_id, reason)


# --- switching (plan §8.10) ------------------------------------------------


async def _run_switching(writer: FencedWriter, site_id: int, tz_name: str) -> None:
    for _ in range(_DAY_TRANSACTIONS):
        more = await writer.write(lambda conn: _switching_step(conn, site_id, tz_name))
        if not more:
            return


def _switching_step(conn: sqlite3.Connection, site_id: int, tz_name: str) -> bool:
    """One pass (i) or pass (ii) transaction; True while the job may go on."""
    if wind_basis_state(conn, site_id) != "switching":
        return False
    cursor = read_wind_cursor(conn, site_id)
    if cursor.phase is None:
        logger.warning("unreadable wind switch cursor reset to purge site=%s", site_id)
        write_wind_cursor(conn, site_id, phase="purge", cursor_date=None)
        return True
    today = _local_today(tz_name)
    if cursor.phase == "purge":
        return _purge_step(conn, site_id, tz_name, today)
    return _install_step(conn, site_id, tz_name, today, cursor)


def _purge_step(
    conn: sqlite3.Connection, site_id: int, tz_name: str, today: date
) -> bool:
    row = conn.execute(
        """
        SELECT MIN(j) AS j FROM (
            SELECT MIN(julianday(so.valid_at)) AS j
            FROM station_observations so JOIN stations s ON s.id = so.station_id
            WHERE s.site_id = ? AND so.variable = 'wind'
            UNION ALL
            SELECT MIN(julianday(o.valid_at)) AS j
            FROM observations o WHERE o.site_id = ? AND o.variable = 'wind'
        )
        """,
        (site_id, site_id),
    ).fetchone()
    if row is None or row["j"] is None:
        _end_purge(conn, site_id, today)
        return True
    julian = float(row["j"])
    window = _julian_local_window(julian, tz_name)
    lo, hi = (None, None) if window is None else (window[1], window[2])
    if window is not None:
        conn.execute(
            f"""
            UPDATE station_wind_days
            SET legacy_hours = (
                    SELECT COUNT(*) FROM station_observations so
                    WHERE so.station_id = station_wind_days.station_id
                      AND so.variable = 'wind' AND {LEGACY_WIND_SQL}
                      AND julianday(so.valid_at) >= julianday(?)
                      AND julianday(so.valid_at) < julianday(?)),
                updated_at = ?
            WHERE local_date = ?
              AND station_id IN (SELECT id FROM stations
                                 WHERE site_id = ? AND enabled = 1)
            """,
            (lo, hi, isoformat_utc(utc_now()), window[0].isoformat(), site_id),
        )
    station_rows = conn.execute(
        "SELECT so.id, so.valid_at FROM station_observations so"
        " JOIN stations s ON s.id = so.station_id"
        " WHERE s.site_id = ? AND so.variable = 'wind'"
        f" AND {_range_sql('so.valid_at')}",
        (site_id, lo, hi, julian),
    ).fetchall()
    observation_rows = conn.execute(
        "SELECT o.valid_at FROM observations o"
        " WHERE o.site_id = ? AND o.variable = 'wind'"
        f" AND {_range_sql('o.valid_at')}",
        (site_id, lo, hi, julian),
    ).fetchall()
    _delete_station_rows(conn, [int(r["id"]) for r in station_rows])
    valid_ats = {str(r["valid_at"]) for r in station_rows}
    valid_ats.update(str(r["valid_at"]) for r in observation_rows)
    _materialize_all(conn, site_id, valid_ats)
    stamp_wind_progress(conn, site_id)
    return True


def _end_purge(conn: sqlite3.Connection, site_id: int, today: date) -> None:
    """Sweep what pass (i) could not date, confirm, and start pass (ii)."""
    stray_rows = conn.execute(
        "SELECT so.id, so.valid_at FROM station_observations so"
        " JOIN stations s ON s.id = so.station_id"
        " WHERE s.site_id = ? AND so.variable = 'wind'"
        " AND julianday(so.valid_at) IS NULL",
        (site_id,),
    ).fetchall()
    stray_observations = conn.execute(
        "SELECT valid_at FROM observations WHERE site_id = ? AND variable = 'wind'"
        " AND julianday(valid_at) IS NULL",
        (site_id,),
    ).fetchall()
    # Station rows first: materializing while a stray pair-max row stood would
    # re-create the observation the confirm step below must not find.
    _delete_station_rows(conn, [int(r["id"]) for r in stray_rows])
    valid_ats = {str(r["valid_at"]) for r in stray_rows}
    valid_ats.update(str(r["valid_at"]) for r in stray_observations)
    _materialize_all(conn, site_id, valid_ats)
    if stray_rows or stray_observations:
        logger.warning(
            "wind rows with an unreadable time purged site=%s station_rows=%s"
            " observations=%s",
            site_id,
            len(stray_rows),
            len(stray_observations),
        )
    pairs = conn.execute(
        "DELETE FROM forecast_pairs WHERE site_id = ? AND variable = 'wind'"
        f" AND {published_generation_clause('forecast_pairs')}",
        (site_id,),
    ).rowcount
    scores = conn.execute(
        "DELETE FROM score_cache WHERE site_id = ? AND variable = 'wind'", (site_id,)
    ).rowcount
    if pairs or scores:
        logger.warning(
            "wind switch swept leftover pairs site=%s pairs=%s scores=%s",
            site_id,
            pairs,
            scores,
        )
    left = conn.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM station_observations so
           JOIN stations s ON s.id = so.station_id
           WHERE s.site_id = ? AND so.variable = 'wind') AS station_rows,
          (SELECT COUNT(*) FROM observations
           WHERE site_id = ? AND variable = 'wind') AS observations
        """,
        (site_id, site_id),
    ).fetchone()
    if int(left["station_rows"]) or int(left["observations"]):
        raise RuntimeError(
            f"wind switch purge incomplete site={site_id}"
            f" station_rows={int(left['station_rows'])}"
            f" observations={int(left['observations'])}"
        )
    _refresh_report(conn, site_id, today)
    first = conn.execute(
        """
        SELECT MIN(d.local_date) AS first
        FROM station_wind_days d JOIN stations st ON st.id = d.station_id
        WHERE st.site_id = ? AND st.enabled = 1
        """,
        (site_id,),
    ).fetchone()
    write_wind_cursor(
        conn, site_id, phase="install", cursor_date=_date_or(first["first"], today)
    )
    stamp_wind_progress(conn, site_id)


def _date_or(value: object, fallback: date) -> date:
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return fallback
    return fallback


def _refresh_report(conn: sqlite3.Connection, site_id: int, today: date) -> None:
    """Refresh ``legacy_hours`` and ``unavailable_with_legacy`` after pass (i)."""
    key = wind_report_key(site_id)
    report = json_object(get_runtime_state(conn, key))
    if report is None:
        logger.warning("unreadable wind rebuild report rebuilt site=%s", site_id)
        rebuilt: dict[str, object] = {
            "switched_at": None,
            **_report_counts(conn, site_id, today),
        }
        set_runtime_state(conn, key, json.dumps(rebuilt))
        return
    last = report.get("last_date")
    upper = _date_or(last, today - _ONE_DAY) + _ONE_DAY if last else today
    counts = _report_counts(conn, site_id, upper)
    report["legacy_hours"] = counts["legacy_hours"]
    report["unavailable_with_legacy"] = counts["unavailable_with_legacy"]
    set_runtime_state(conn, key, json.dumps(report))


def _earliest_inventory_date(
    conn: sqlite3.Connection, site_id: int, today: date
) -> date:
    row = conn.execute(
        """
        SELECT MIN(d.local_date) AS first
        FROM station_wind_days d JOIN stations st ON st.id = d.station_id
        WHERE st.site_id = ? AND st.enabled = 1
        """,
        (site_id,),
    ).fetchone()
    return _date_or(row["first"], today)


def _install_step(
    conn: sqlite3.Connection,
    site_id: int,
    tz_name: str,
    today: date,
    cursor: WindCursor,
) -> bool:
    day = cursor.cursor_date
    if day is None:
        logger.warning(
            "unreadable wind switch cursor date reset to the first day site=%s",
            site_id,
        )
        write_wind_cursor(
            conn,
            site_id,
            phase="install",
            cursor_date=_earliest_inventory_date(conn, site_id, today),
        )
        return True
    if day > today:
        if not transition_wind_basis(
            conn, site_id, expected="switching", new="rescoring"
        ):
            logger.warning(
                "wind switch lost its compare-and-set to rescoring site=%s", site_id
            )
            return False
        delete_runtime_state(conn, wind_cursor_key(site_id))
        logger.info("wind switch installed, rescoring site=%s", site_id)
        return False
    station_ids = [
        int(row["station_id"])
        for row in conn.execute(
            """
            SELECT d.station_id
            FROM station_wind_days d JOIN stations st ON st.id = d.station_id
            WHERE st.site_id = ? AND st.enabled = 1 AND d.local_date = ?
              AND d.status IN ('fetched','partial')
            ORDER BY d.station_id
            """,
            (site_id, day.isoformat()),
        )
    ]
    written: set[str] = set()
    for station_id in station_ids:
        hours = _derive_hours(conn, station_id, day, tz_name, today)
        if not hours:
            continue
        records = _read_records(
            conn, station_id, hours[0] - _PAIR_LOOKBACK, hours[-1] + _ONE_HOUR
        )
        pairs = pair_max_by_hour(records, hours)
        for hour in sorted(pairs):
            speed = pairs[hour]
            valid_at = isoformat_utc(hour)
            write_station_observation(
                conn,
                station_id=station_id,
                variable="wind",
                valid_at=valid_at,
                value=kmh_to_ms(speed),
                source_raw=f"pair-max {round(speed, 2)} km/h",
            )
            written.add(valid_at)
    _materialize_all(conn, site_id, written)
    write_wind_cursor(conn, site_id, phase="install", cursor_date=day + _ONE_DAY)
    stamp_wind_progress(conn, site_id)
    return True


# --- rescoring (plan §8.11) ------------------------------------------------


async def _run_rescoring(writer: FencedWriter, site_id: int) -> None:
    try:
        await run_split_pair_phases(writer, site_id, require_enabled=True)
        await run_batched_scoring(writer, site_id)
    except ScoringInputsBusy as exc:
        logger.warning(
            "wind rescore deferred site=%s step=%s: inputs changed on every one"
            " of %d attempts",
            site_id,
            exc.step,
            SCORING_CAS_ATTEMPTS,
        )
        raise JobDeferred(isoformat_utc(utc_now() + _RESCORE_BUSY_DEFER)) from exc
    await writer.write(lambda conn: _finish_rescoring(conn, site_id))


def _finish_rescoring(conn: sqlite3.Connection, site_id: int) -> None:
    if not transition_wind_basis(conn, site_id, expected="rescoring", new="pair_max"):
        logger.warning(
            "wind rescore lost its compare-and-set to pair_max site=%s", site_id
        )
        return
    set_runtime_state_now(conn, wind_done_at_key(site_id))
    logger.info("wind switch complete site=%s", site_id)
