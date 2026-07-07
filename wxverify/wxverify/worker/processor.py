"""Async worker loop and job dispatch."""

from __future__ import annotations

import asyncio
import errno
import logging
import sqlite3
import time
from dataclasses import dataclass

import httpx

from wxverify.collection.budget import reserve_budget
from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.secrets import resolve_secret
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database
from wxverify.db.queue import (
    Job,
    claim_next_job,
    complete,
    defer_job,
    enqueue_if_absent,
    fail,
    purge_failed_jobs_older_than,
)
from wxverify.db.runtime_state import set_runtime_state_now
from wxverify.feeds.registry import build_adapter
from wxverify.obs.config import RECENT_REFRESH_HOURS
from wxverify.obs.pws_adapter import PwsObservation, fetch_hourly_history
from wxverify.scoring.consensus import insert_station_observation
from wxverify.scoring.engine import pair_and_score
from wxverify.worker.backfill import run_backfill_site
from wxverify.worker.catchup import run_catchup
from wxverify.worker.control import JobCancelled, JobContinuation, JobDeferred
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    clear_domain_backoff,
    record_http_backoff,
    source_domain,
)
from wxverify.worker.feed_fetch import (
    BackoffActive,
    BudgetExhausted,
    Ineligible,
    Unavailable,
    fetch_feed_once,
    mark_feed_unavailable,
)
from wxverify.worker.scheduler import scheduler_tick
from wxverify.worker.station_pacing import pace_station_call, station_call_limiter

POLL_INTERVAL = 1.0
FAILED_JOB_RETENTION_HOURS = 168
JOB_HOUSEKEEPING_INTERVAL_SECONDS = 3600
RUNTIME_HEARTBEAT_INTERVAL_SECONDS = 60.0

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StationFetchTarget:
    id: int
    pws_station_id: str


async def run_worker(db: Database) -> None:
    last_housekeeping_at = 0.0
    last_worker_heartbeat_at = 0.0
    last_scheduler_heartbeat_at = 0.0
    while True:
        now = time.monotonic()
        last_worker_heartbeat_at = await _maybe_stamp_runtime_heartbeat(
            db, "worker_last_loop_at", last_worker_heartbeat_at, now
        )
        await db.write(scheduler_tick)
        now = time.monotonic()
        last_scheduler_heartbeat_at = await _maybe_stamp_runtime_heartbeat(
            db, "scheduler_last_tick_at", last_scheduler_heartbeat_at, now
        )
        if now - last_housekeeping_at >= JOB_HOUSEKEEPING_INTERVAL_SECONDS:
            await db.write(
                lambda conn: purge_failed_jobs_older_than(
                    conn, FAILED_JOB_RETENTION_HOURS
                )
            )
            last_housekeeping_at = now
        job = await db.write(claim_next_job)
        if job is None:
            await asyncio.sleep(POLL_INTERVAL)
            continue
        job_id = job.id
        try:
            continuation = await dispatch(db, job)
            await db.write(lambda conn, jid=job_id: complete(conn, jid))
            if continuation is not None:
                await db.write(
                    lambda conn, cont=continuation: enqueue_if_absent(
                        conn,
                        cont.job_type,
                        cont.site_id,
                        cont.job_key,
                        cont.payload,
                    )
                )
        except JobDeferred as exc:
            next_attempt_at = exc.next_attempt_at
            await db.write(
                lambda conn, jid=job_id, attempt=next_attempt_at: defer_job(
                    conn, jid, attempt
                )
            )
        except JobCancelled:
            await db.write(lambda conn, jid=job_id: complete(conn, jid))
        except Exception as exc:
            if _is_process_fatal_permission_error(exc):
                logger.critical(
                    "fatal OS permission error while processing job id=%s type=%s; "
                    "terminating worker for process restart",
                    job.id,
                    job.type,
                    exc_info=True,
                )
                raise
            message = sanitized_exception(exc)
            await db.write(lambda conn, jid=job_id, err=message: fail(conn, jid, err))


async def _maybe_stamp_runtime_heartbeat(
    db: Database, key: str, last_stamp_at: float, now: float
) -> float:
    if last_stamp_at > 0 and now - last_stamp_at < RUNTIME_HEARTBEAT_INTERVAL_SECONDS:
        return last_stamp_at
    try:
        await db.write(
            lambda conn, state_key=key: set_runtime_state_now(conn, state_key)
        )
    except Exception:
        logger.exception("runtime heartbeat write failed key=%s", key)
    return now


async def dispatch(db: Database, job: Job) -> JobContinuation | None:
    if job.type == "pair_and_score":
        site_id = job.site_id
        if site_id is None:
            raise JobCancelled()
        await db.write(lambda conn: _pair_and_score_if_enabled(conn, site_id))
        return None
    if job.type == "fetch_obs":
        site_id = job.site_id
        if site_id is None:
            raise JobCancelled()
        await _fetch_obs(db, site_id)
        return None
    if job.type == "fetch_feed":
        site_id = job.site_id
        if site_id is None:
            raise JobCancelled()
        feed_id = _payload_int(job.payload, "feed_id")
        if feed_id is None:
            raise JobCancelled()
        await _fetch_feed(db, site_id, feed_id)
        return None
    if job.type == "backfill_site":
        site_id = job.site_id
        if site_id is None:
            raise JobCancelled()
        return await run_backfill_site(db, site_id, job.payload)
    if job.type == "catchup":
        return await run_catchup(db, job.payload)
    raise RuntimeError(f"unknown job type {job.type}")


def _pair_and_score_if_enabled(conn: sqlite3.Connection, site_id: int) -> None:
    row = conn.execute("SELECT enabled FROM sites WHERE id=?", (site_id,)).fetchone()
    if row is None or not bool(row["enabled"]):
        raise JobCancelled()
    pair_and_score(conn, site_id)


async def _fetch_obs(db: Database, site_id: int) -> None:
    api_key = resolve_secret("weathercom")
    if not api_key:
        raise RuntimeError("weathercom key is not configured")
    timezone = await db.read(lambda conn: _site_timezone(conn, site_id))
    if timezone is None:
        raise JobCancelled()
    stations = await db.read(lambda conn: _enabled_stations(conn, site_id))
    if not stations:
        raise JobCancelled()
    changed = False
    async with httpx.AsyncClient() as client:
        limiter = station_call_limiter()
        for index, station in enumerate(stations):
            await pace_station_call(site_id, station.id, index)
            async with limiter:
                await db.write(
                    lambda conn, station_id=station.id: _reserve_obs_call(
                        conn, site_id, station_id
                    )
                )
                try:
                    observations = await fetch_hourly_history(
                        station.pws_station_id,
                        api_key,
                        hours=RECENT_REFRESH_HOURS,
                        timezone=timezone,
                        client=client,
                    )
                except httpx.HTTPStatusError as exc:
                    error = sanitized_exception(exc)
                    response = exc.response
                    next_attempt_at = await db.write(
                        lambda conn, station_id=station.id, err=error, resp=response: (
                            _mark_station_error_and_backoff(conn, station_id, err, resp)
                        )
                    )
                    if next_attempt_at is not None:
                        raise JobDeferred(next_attempt_at) from exc
                    raise
                except Exception as exc:
                    error = sanitized_exception(exc)
                    await db.write(
                        lambda conn, station_id=station.id, err=error: (
                            _mark_station_error(conn, station_id, err)
                        )
                    )
                    raise
                station_changed = await db.write(
                    lambda conn, station_id=station.id, rows=observations: (
                        _persist_station_observations(conn, site_id, station_id, rows)
                    )
                )
                changed = changed or station_changed
    await db.write(lambda conn: _complete_obs_cycle(conn, site_id, changed))


async def _fetch_feed(db: Database, site_id: int, feed_id: int) -> None:
    outcome = await fetch_feed_once(db, site_id, feed_id, adapter_builder=build_adapter)
    if isinstance(outcome, BudgetExhausted):
        raise JobDeferred(outcome.next_window)
    if isinstance(outcome, BackoffActive):
        raise JobDeferred(outcome.next_attempt)
    if isinstance(outcome, Ineligible):
        raise JobCancelled()
    if isinstance(outcome, Unavailable):
        await db.write(
            lambda conn, result=outcome: mark_feed_unavailable(
                conn, result.target, result.error
            )
        )
        return


def _enabled_stations(
    conn: sqlite3.Connection, site_id: int
) -> list[StationFetchTarget]:
    rows = conn.execute(
        """
        SELECT st.id, st.pws_station_id
        FROM stations st
        JOIN sites s ON s.id = st.site_id
        WHERE s.id = ?
          AND s.enabled = 1
          AND st.enabled = 1
        ORDER BY st.pws_station_id
        """,
        (site_id,),
    ).fetchall()
    return [
        StationFetchTarget(id=int(row["id"]), pws_station_id=str(row["pws_station_id"]))
        for row in rows
    ]


def _site_timezone(conn: sqlite3.Connection, site_id: int) -> str | None:
    row = conn.execute(
        "SELECT timezone FROM sites WHERE id=? AND enabled=1", (site_id,)
    ).fetchone()
    return None if row is None else str(row["timezone"])


def _reserve_obs_call(conn: sqlite3.Connection, site_id: int, station_id: int) -> None:
    row = conn.execute(
        """
        SELECT 1
        FROM stations st
        JOIN sites s ON s.id = st.site_id
        WHERE s.id = ?
          AND st.id = ?
          AND s.enabled = 1
          AND st.enabled = 1
        """,
        (site_id, station_id),
    ).fetchone()
    if row is None:
        raise JobCancelled()
    check_domain_backoff(conn, source_domain("weathercom"))
    reserve_budget(conn, "weathercom", 1)


def _persist_station_observations(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    observations: list[PwsObservation],
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM stations
        WHERE id = ? AND site_id = ? AND enabled = 1
        """,
        (station_id, site_id),
    ).fetchone()
    if row is None:
        raise JobCancelled()
    changed = False
    for observation in observations:
        changed = (
            insert_station_observation(
                conn,
                station_id=station_id,
                variable=observation.variable,
                valid_at=observation.valid_at,
                value=observation.value,
                source_raw=observation.source_raw,
            )
            or changed
        )
    clear_domain_backoff(conn, source_domain("weathercom"))
    conn.execute(
        """
        UPDATE stations
        SET last_run_at=?, last_error=NULL, error_count=0
        WHERE id=?
        """,
        (isoformat_utc(), station_id),
    )
    return changed


def _mark_station_error_and_backoff(
    conn: sqlite3.Connection,
    station_id: int,
    error: str,
    response: httpx.Response,
) -> str | None:
    _mark_station_error(conn, station_id, error)
    return record_http_backoff(conn, response)


def _complete_obs_cycle(conn: sqlite3.Connection, site_id: int, changed: bool) -> None:
    cur = conn.execute(
        """
        UPDATE sites
        SET last_obs_at=?
        WHERE id=? AND enabled=1
        """,
        (isoformat_utc(), site_id),
    )
    if cur.rowcount != 1:
        raise JobCancelled()
    if changed:
        enqueue_if_absent(
            conn, "pair_and_score", site_id, "score", {"site_id": site_id}
        )


def _mark_station_error(conn: sqlite3.Connection, station_id: int, error: str) -> None:
    conn.execute(
        """
        UPDATE stations
        SET last_error=?, error_count=error_count + 1
        WHERE id=?
        """,
        (error, station_id),
    )


def _payload_int(payload: dict[str, object], key: str) -> int | None:
    value = payload.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    return None


def _is_process_fatal_permission_error(exc: BaseException) -> bool:
    for item in _exception_chain(exc):
        if isinstance(item, PermissionError):
            if item.errno in (None, errno.EPERM):
                return True
        elif isinstance(item, OSError) and item.errno == errno.EPERM:
            return True
        message = str(item)
        if "[Errno 1]" in message and "Operation not permitted" in message:
            return True
    return False


def _exception_chain(exc: BaseException) -> list[BaseException]:
    out: list[BaseException] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        out.append(current)
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return out
