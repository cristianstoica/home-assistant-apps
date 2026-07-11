"""Async worker loop and job dispatch."""

from __future__ import annotations

import asyncio
import errno
import logging
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from wxverify.collection.budget import (
    Reservation,
    is_refundable_transport_error,
    refund_budget,
    reserve_budget,
)
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
from wxverify.obs.pws_adapter import (
    PwsObservation,
    fetch_current_observation,
    fetch_hourly_history,
)
from wxverify.scoring.consensus import insert_station_observation
from wxverify.scoring.engine import PAIR_AND_SCORE_PHASES
from wxverify.settings.keys import get_number_setting
from wxverify.worker.backfill import run_backfill_site
from wxverify.worker.catchup import run_catchup
from wxverify.worker.control import JobCancelled, JobContinuation, JobDeferred
from wxverify.worker.current_obs import (
    Health,
    PollOutcome,
    classify_current_obs,
    persist_poll_result,
)
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
        logger.debug("job claimed id=%s type=%s site=%s", job.id, job.type, job.site_id)
        outcome = "completed"
        try:
            continuation = await dispatch(db, job)
            await db.write(lambda conn, jid=job_id: complete(conn, jid))
            logger.debug(
                "job completed id=%s type=%s site=%s", job.id, job.type, job.site_id
            )
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
            outcome = "deferred"
            next_attempt_at = exc.next_attempt_at
            await db.write(
                lambda conn, jid=job_id, attempt=next_attempt_at: defer_job(
                    conn, jid, attempt
                )
            )
            logger.debug(
                "job deferred id=%s type=%s site=%s until=%s",
                job.id,
                job.type,
                job.site_id,
                next_attempt_at,
            )
        except JobCancelled:
            outcome = "cancelled"
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
            disposition = await db.write(
                lambda conn, jid=job_id, err=message: fail(conn, jid, err)
            )
            if disposition is not None and disposition.terminal:
                outcome = "failed"
                logger.error(
                    "job failed permanently id=%s type=%s site=%s attempts=%d/%d: %s",
                    job.id,
                    job.type,
                    job.site_id,
                    disposition.retry_count,
                    disposition.max_retries,
                    message,
                )
            else:
                outcome = "retry"
                logger.warning(
                    "job failed id=%s type=%s site=%s attempt=%s/%s next=%s: %s",
                    job.id,
                    job.type,
                    job.site_id,
                    disposition.retry_count if disposition else "?",
                    disposition.max_retries if disposition else "?",
                    disposition.next_attempt_at if disposition else "?",
                    message,
                )
        logger.info(
            "cycle: job=%s type=%s site=%s outcome=%s",
            job.id,
            job.type,
            job.site_id,
            outcome,
        )


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
    logger.debug("dispatch type=%s site=%s", job.type, job.site_id)
    if job.type == "pair_and_score":
        site_id = job.site_id
        if site_id is None:
            raise JobCancelled()
        # One write transaction per phase so the event loop (and the Docker
        # healthcheck) gets scheduled between phases instead of stalling for
        # the whole pipeline.
        #
        # CONVERGENCE INVARIANT (do not weaken): the phase split converges to
        # the same end state as the monolithic run ONLY because no
        # observation write can interleave between phases:
        #   (a) this single worker loop is the only job executor, so no other
        #       job's observation write runs between these transactions; and
        #   (b) every HTTP route that writes observations (station PUT /
        #       DELETE, site rain-threshold PUT) runs the monolithic
        #       pair_and_score INLINE in its own write transaction — it never
        #       enqueues. Note enqueue_if_absent dedupes against BOTH
        #       'pending' AND 'running' jobs (db/queue.py), so an enqueue
        #       issued while this job is mid-split is SWALLOWED, not queued
        #       behind it: a future route that switches from inline scoring
        #       to enqueueing would break convergence silently. The dashboard
        #       _enqueue_score routes do not write observations and are safe.
        for phase in PAIR_AND_SCORE_PHASES:
            logger.debug("score phase=%s site=%s", phase.__name__, site_id)
            await db.write(
                lambda conn, run=phase: _run_score_phase_if_enabled(conn, site_id, run)
            )
        return None
    if job.type == "fetch_obs":
        site_id = job.site_id
        if site_id is None:
            raise JobCancelled()
        await _fetch_obs(db, site_id)
        return None
    if job.type == "fetch_current_obs":
        site_id = job.site_id
        if site_id is None:
            raise JobCancelled()
        station_id = _payload_int(job.payload, "station_id")
        if station_id is None:
            raise JobCancelled()
        await _fetch_current_obs(db, site_id, station_id)
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


def _run_score_phase_if_enabled(
    conn: sqlite3.Connection,
    site_id: int,
    phase: Callable[[sqlite3.Connection, int | None], object],
) -> None:
    row = conn.execute("SELECT enabled FROM sites WHERE id=?", (site_id,)).fetchone()
    if row is None or not bool(row["enabled"]):
        raise JobCancelled()
    phase(conn, site_id)


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
    logger.debug("fetch_obs site=%s stations=%s", site_id, len(stations))
    changed = False
    async with httpx.AsyncClient() as client:
        limiter = station_call_limiter()
        for index, station in enumerate(stations):
            await pace_station_call(site_id, station.id, index)
            logger.debug(
                "fetch_obs station attempt site=%s station=%s index=%s",
                site_id,
                station.id,
                index,
            )
            async with limiter:
                reservation = await db.write(
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
                    refund = reservation if is_refundable_transport_error(exc) else None
                    await db.write(
                        lambda conn, station_id=station.id, err=error, res=refund: (
                            _mark_station_error_and_refund(conn, station_id, err, res)
                        )
                    )
                    raise
                station_changed = await db.write(
                    lambda conn, station_id=station.id, rows=observations: (
                        _persist_station_observations(conn, site_id, station_id, rows)
                    )
                )
                logger.debug(
                    "fetch_obs station result site=%s station=%s changed=%s",
                    site_id,
                    station.id,
                    station_changed,
                )
                changed = changed or station_changed
    logger.debug("fetch_obs cycle done site=%s changed=%s", site_id, changed)
    await db.write(lambda conn: _complete_obs_cycle(conn, site_id, changed))


async def _fetch_current_obs(db: Database, site_id: int, station_id: int) -> None:
    """Poll ``/observations/current`` for one station, learn cadence, snapshot.

    Independent of the hourly ``_fetch_obs`` stream: touches only
    ``station_poll_state`` (diagnostics + cadence) and ``station_current_obs``
    (last-good snapshot), never ``stations`` (plan §6). One station per job, one
    provider call. On 429/>=500 the shared ``api.weather.com`` domain backoff is
    recorded and the job is deferred; on transport failure the poll is marked
    transient and deferred to the floor.
    """
    api_key = resolve_secret("weathercom")
    if not api_key:
        raise RuntimeError("weathercom key is not configured")
    pws_station_id = await db.read(
        lambda conn: _pws_station_id(conn, site_id, station_id)
    )
    if pws_station_id is None:
        raise JobCancelled()

    # Reserve in the SAME order and transaction as _reserve_obs_call: backoff
    # gate first (raises JobDeferred if active), then budget (raises JobDeferred
    # if exhausted). A single station ⇒ ordinal 0 ⇒ no station pacing
    # (station_pacing returns 0.0 at ordinal 0), so no pace_station_call here by
    # design (plan §3).
    await db.write(lambda conn: _reserve_current_obs_call(conn, site_id, station_id))

    # Operator-configurable read timeout (plan §10); default 30s, floored at 1s to
    # match the config.yaml int(1,300) schema.
    timeout_seconds = await db.read(
        lambda conn: get_number_setting(conn, "request_timeout_seconds", 30, minimum=1)
    )

    try:
        response = await fetch_current_observation(
            pws_station_id, api_key, timeout_seconds=timeout_seconds
        )
    except Exception as exc:
        # Transport-level failure (timeout / connect / read): transient, retry
        # at the floor. Do not record a domain backoff (no HTTP status to key on).
        error = sanitized_exception(exc)
        transient = PollOutcome(Health.TRANSIENT, error=error)
        await db.write(
            lambda conn, out=transient: persist_poll_result(
                conn, site_id, station_id, out
            )
        )
        raise

    outcome = classify_current_obs(response)
    status = response.status_code

    # 429 / >=500: record the shared domain backoff (single write with the
    # transient poll-state) and defer, exactly as the hourly stream does.
    # Classification already returned TRANSIENT for these codes.
    if status == 429 or status >= 500:
        next_attempt_at = await db.write(
            lambda conn, resp=response, out=outcome: _record_current_obs_backoff(
                conn, site_id, station_id, resp, out
            )
        )
        # record_http_backoff always returns a next-attempt for 429/>=500.
        raise JobDeferred(next_attempt_at or isoformat_utc())

    await db.write(
        lambda conn, out=outcome: persist_poll_result(conn, site_id, station_id, out)
    )


def _reserve_current_obs_call(
    conn: sqlite3.Connection, site_id: int, station_id: int
) -> None:
    """Domain-backoff gate then budget reservation (mirrors _reserve_obs_call).

    Raises ``JobDeferred`` if the shared weather.com backoff is active or the
    daily budget is exhausted; ``JobCancelled`` if the station is gone/disabled.
    """
    row = conn.execute(
        """
        SELECT 1
        FROM stations st
        JOIN sites s ON s.id = st.site_id
        WHERE s.id = ? AND st.id = ? AND s.enabled = 1 AND st.enabled = 1
        """,
        (site_id, station_id),
    ).fetchone()
    if row is None:
        raise JobCancelled()
    check_domain_backoff(conn, source_domain("weathercom"))
    reserve_budget(conn, "weathercom", 1)


def _record_current_obs_backoff(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    response: httpx.Response,
    outcome: PollOutcome,
) -> str | None:
    """Persist the transient poll-state AND record the shared domain backoff."""
    persist_poll_result(conn, site_id, station_id, outcome)
    return record_http_backoff(conn, response)


def _pws_station_id(
    conn: sqlite3.Connection, site_id: int, station_id: int
) -> str | None:
    row = conn.execute(
        """
        SELECT st.pws_station_id
        FROM stations st
        JOIN sites s ON s.id = st.site_id
        WHERE s.id = ? AND st.id = ? AND s.enabled = 1 AND st.enabled = 1
        """,
        (site_id, station_id),
    ).fetchone()
    return None if row is None else str(row["pws_station_id"])


async def _fetch_feed(db: Database, site_id: int, feed_id: int) -> None:
    logger.debug("fetch_feed dispatch site=%s feed=%s", site_id, feed_id)
    outcome = await fetch_feed_once(db, site_id, feed_id, adapter_builder=build_adapter)
    if isinstance(outcome, BudgetExhausted):
        raise JobDeferred(outcome.next_window)
    if isinstance(outcome, BackoffActive):
        raise JobDeferred(outcome.next_attempt)
    if isinstance(outcome, Ineligible):
        raise JobCancelled()
    if isinstance(outcome, Unavailable):
        logger.warning(
            "feed adapter unavailable site=%s feed=%s source=%s: %s",
            outcome.target.site_id,
            outcome.target.feed_id,
            outcome.target.source,
            outcome.error,
        )
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


def _reserve_obs_call(
    conn: sqlite3.Connection, site_id: int, station_id: int
) -> Reservation:
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
    return reserve_budget(conn, "weathercom", 1)


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


def _mark_station_error_and_refund(
    conn: sqlite3.Connection,
    station_id: int,
    error: str,
    reservation: Reservation | None,
) -> None:
    """Record the station failure and, atomically, refund a phantom reservation."""
    _mark_station_error(conn, station_id, error)
    if reservation is not None:
        refund_budget(conn, reservation)


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
