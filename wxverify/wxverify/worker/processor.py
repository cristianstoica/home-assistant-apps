"""Async worker loop and job dispatch."""

from __future__ import annotations

import asyncio
import errno
import logging
import re
import sqlite3
import time
from collections.abc import Callable
from datetime import date, datetime
from enum import Enum, auto
from typing import Final, Literal, NoReturn

import httpx

from wxverify.collection.budget import (
    Reservation,
    is_refundable_transport_error,
    refund_budget,
    reserve_budget,
    write_after_reservation,
)
from wxverify.core import stop
from wxverify.core.error_sanitize import provider_reason, sanitized_exception
from wxverify.core.secrets import resolve_secret
from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.connection import Database, FencedWriter
from wxverify.db.queue import (
    FailDisposition,
    Job,
    claim_next_job,
    complete,
    defer_job,
    fail,
    purge_failed_jobs_older_than,
    reclaim_all_stale,
)
from wxverify.db.runtime_state import (
    RUNTIME_HEARTBEAT_INTERVAL_SECONDS,
    set_runtime_state_now,
)
from wxverify.db.station_readings import store_readings
from wxverify.feeds.registry import build_adapter
from wxverify.obs.hourly_values import StationReading
from wxverify.obs.pws_adapter import (
    CurrentObservation,
    fetch_current_observation,
)
from wxverify.settings.keys import get_number_setting
from wxverify.worker.access_hold import (
    AccessHeld,
    check_access_hold,
    clear_access_refused,
    record_access_refused,
)
from wxverify.worker.close_chain import (
    fail_day_close_job,
    fail_day_publish_job,
    run_day_close,
    run_day_publish,
)
from wxverify.worker.control import JobCancelled, JobDeferred
from wxverify.worker.current_obs import (
    Health,
    PollOutcome,
    classify_current_obs,
    persist_poll_result,
)
from wxverify.worker.current_obs_poller import run_current_obs_poller
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    record_http_backoff,
    source_domain,
)
from wxverify.worker.feed_fetch import (
    BackoffActive,
    BudgetExhausted,
    DownloadOutcome,
    DownloadSlot,
    Ineligible,
    Refused,
    Unavailable,
    fetch_feed_once,
    fetch_feed_retry_floor_seconds,
    mark_feed_unavailable,
    record_download_try,
)
from wxverify.worker.om_archive_fill import OM_ARCHIVE_JOB_KEY, run_om_archive_job
from wxverify.worker.round import round_try_stale
from wxverify.worker.scheduler import scheduler_tick
from wxverify.worker.station_pacing import weathercom_call_lock
from wxverify.worker.wind_days import WIND_DAYS_JOB_KEY, run_wind_days

POLL_INTERVAL = 1.0
FAILED_JOB_RETENTION_HOURS = 168
JOB_HOUSEKEEPING_INTERVAL_SECONDS = 3600

logger = logging.getLogger(__name__)


async def run_worker(db: Database) -> None:
    """Supervise the two worker lanes until one ends or shutdown cancels.

    The main lane runs every job type except ``fetch_current_obs``; the
    current-obs lane runs only that type (worker.current_obs_poller). Not an
    ``asyncio.TaskGroup``, on purpose: a TaskGroup wraps a lane's exception
    in an ExceptionGroup, and callers depend on the lane's own exception
    object. A lane that ends on its own makes run_worker drain the other and
    re-raise the first failure by identity, with no reclaim -- as for any
    worker death, the next boot's reclaim_all_stale recovers running jobs.
    Every cancellation path drains both lanes, then runs exactly one
    shutdown reclaim, then raises CancelledError; once a stop has been
    requested, the drain and the reclaim each wait only within the stop
    budget (core.stop).
    """
    main = asyncio.create_task(_run_main_lane(db), name="worker-main-lane")
    poller = asyncio.create_task(
        run_current_obs_poller(db, run_job=run_claimed_job),
        name="worker-current-obs-lane",
    )
    lanes = (main, poller)
    try:
        await asyncio.wait(lanes, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        await _cancel_and_drain(lanes, db)
        _log_lane_failures(lanes)
        await _shutdown_reclaim(db)
        raise
    if await _cancel_and_drain(lanes, db):  # a cancel arrived during the drain
        _log_lane_failures(lanes)
        await _shutdown_reclaim(db)
        raise asyncio.CancelledError
    _raise_lane_outcome(main, poller)


async def _cancel_and_drain(
    lanes: tuple[asyncio.Task[None], ...], db: Database
) -> bool:
    """Cancel every lane and wait until each is done, within the stop budget.

    Returns True if run_worker was cancelled meanwhile. The same loop as
    core.aio.run_to_completion: ``asyncio.wait`` does not cancel what it
    awaits, so a repeated cancel costs one more iteration, never an
    abandoned lane (core.stop.wait_within_budget). Once a stop has been
    requested and its budget runs out, the writer is interrupted, and a lane
    still running after the grace is left to the event loop's final sweep;
    the next start's reclaim_all_stale recovers its job.
    """
    for lane in lanes:
        lane.cancel()
    cancelled = await stop.wait_within_budget(
        lanes,
        still_running=lambda: [lane.get_name() for lane in lanes if not lane.done()],
        # Looked up only when the budget runs out.
        interrupt_writer=lambda: db.interrupt_writer(),
        log=logger,
    )
    return cancelled is not None  # the caller re-raises it


async def _shutdown_reclaim(db: Database) -> None:
    """Run the shutdown reclaim write once and wait until it is done.

    The same loop as core.aio.run_to_completion: one task, waited on with
    ``asyncio.wait`` (core.stop.wait_within_budget) until it is done. A bare
    ``await db.write(...)`` is not enough -- ``db.write`` waits for the
    writer lock before its executor hop, so a second cancel landing in that
    wait would skip the reclaim. The task is never cancelled here and never
    restarted, so the write runs at most once. A cancel delivered meanwhile
    is absorbed, not re-raised: both callers raise CancelledError as soon as
    this returns. A failed write is logged. Any other outcome propagates,
    including a cancel of the task from outside (the event loop's final
    sweep). Once a stop has been requested the wait ends with the stop
    budget; a reclaim not done by then is left behind, and the
    next start's reclaim_all_stale does its work.
    """
    # Exempt: a shutdown-time bulk sweep over whatever jobs the
    # live database currently holds, not tied to any one job's read.
    reclaim = asyncio.create_task(
        db.write(reclaim_all_stale), name="worker-shutdown-reclaim"
    )
    # A cancel is absorbed; the caller raises CancelledError itself.
    await stop.wait_within_budget(
        (reclaim,),
        still_running=lambda: [reclaim.get_name()],
        interrupt_writer=lambda: db.interrupt_writer(),
        log=logger,
    )
    if not reclaim.done():
        return
    # exception() raises CancelledError, rather than returning it, if the
    # task was cancelled; nothing here cancels it.
    exc = reclaim.exception()
    if isinstance(exc, Exception):
        logger.warning("shutdown reclaim failed", exc_info=exc)
    elif exc is not None:
        raise exc


def _log_lane_failures(lanes: tuple[asyncio.Task[None], ...]) -> None:
    """Log every lane that ended on its own rather than by the drain's cancel.

    _stop_on_worker_done (api.app) returns silently for a cancelled worker,
    so a lane failure followed by a shutdown cancel would otherwise leave no
    trace.
    """
    for lane in lanes:
        if not lane.done() or lane.cancelled():
            continue
        exc = lane.exception()
        if exc is None:
            logger.error("worker lane %s returned unexpectedly", lane.get_name())
        else:
            logger.error(
                "worker lane %s failed",
                lane.get_name(),
                exc_info=(type(exc), exc, exc.__traceback__),
            )


def _raise_lane_outcome(
    main: asyncio.Task[None], poller: asyncio.Task[None]
) -> NoReturn:
    """Re-raise the first lane failure, by identity; both lanes are done.

    Precedence is main, then poller, and a second failure is logged rather
    than lost. Neither lane has an exit path, so a normal return is itself
    a failure. The last raise covers a lane that ended cancelled without a
    shutdown request (its own code raised CancelledError): re-raising that
    as a cancel would read as a clean shutdown and leave the process
    running with no worker.
    """
    failures: list[tuple[asyncio.Task[None], BaseException]] = []
    returned: list[asyncio.Task[None]] = []
    for lane in (main, poller):
        if lane.cancelled():
            continue
        exc = lane.exception()
        if exc is None:
            returned.append(lane)
        else:
            failures.append((lane, exc))
    if failures:
        for lane, exc in failures[1:]:
            logger.error(
                "worker lane %s failed",
                lane.get_name(),
                exc_info=(type(exc), exc, exc.__traceback__),
            )
        raise failures[0][1]
    if returned:
        raise RuntimeError(
            f"worker lane {returned[0].get_name()} returned unexpectedly"
        )
    raise RuntimeError("worker lanes ended cancelled without a shutdown request")


async def _run_main_lane(db: Database) -> None:
    """Tick the scheduler and run main-lane jobs one at a time, forever.

    Once a stop has begun (``core.stop.STOP``)
    the lane claims nothing: it goes back to the poll sleep, where the
    cancel lands. It never returns -- a lane that ends on its own is a
    crash to ``api.app._stop_on_worker_done``.
    """
    last_housekeeping_at = 0.0
    last_worker_heartbeat_at = 0.0
    last_scheduler_heartbeat_at = 0.0
    while True:
        now = time.monotonic()
        last_worker_heartbeat_at = await _maybe_stamp_runtime_heartbeat(
            db, "worker_last_loop_at", last_worker_heartbeat_at, now
        )
        # Plain writes: scheduler_tick, the housekeeping purge, and the claim
        # itself all run once per loop iteration, before any job is claimed.
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
        # Right before the claim: the awaits above can end after a stop began.
        if stop.STOP.is_set():
            await asyncio.sleep(POLL_INTERVAL)
            continue
        job = await db.write(claim_next_job)
        if job is None:
            await asyncio.sleep(POLL_INTERVAL)
            continue
        await run_claimed_job(db, job, lane="main")


async def run_claimed_job(db: Database, job: Job, *, lane: str) -> None:
    """Run one claimed job through dispatch to its disposition.

    ``lane`` names the calling lane in the log lines.
    """
    # Every write below goes through `writer`.
    writer = FencedWriter(db)
    claimed_at = time.monotonic()
    logger.info(
        "job claimed id=%s type=%s site=%s lane=%s",
        job.id,
        job.type,
        job.site_id,
        lane,
    )
    outcome = "completed"
    try:
        await dispatch(db, writer, job)
        # result='ok' is the success marker, written only here, on a normal
        # dispatch return. FAILED_SCOPES_SQL (wxverify.monitor) treats only a
        # marked row as resolving an earlier failure of its scope.
        recorded = await writer.write(
            lambda conn, j=job: _record_outcome(
                conn, j, lambda c: complete(c, j.id, result="ok")
            )
        )
        if recorded is _Dropped.OUTCOME:
            outcome = "dropped"
        else:
            logger.debug(
                "job completed id=%s type=%s site=%s",
                job.id,
                job.type,
                job.site_id,
            )
    except JobDeferred as exc:
        outcome = "deferred"
        next_attempt_at = exc.next_attempt_at
        recorded = await writer.write(
            lambda conn, j=job, attempt=next_attempt_at: _record_outcome(
                conn, j, lambda c: defer_job(c, j.id, attempt)
            )
        )
        if recorded is _Dropped.OUTCOME:
            outcome = "dropped"
        else:
            logger.debug(
                "job deferred id=%s type=%s site=%s until=%s",
                job.id,
                job.type,
                job.site_id,
                next_attempt_at,
            )
    except JobCancelled:
        outcome = "cancelled"
        # No success marker: a cancelled job resolved nothing
        # (the marker is written only on the normal return above).
        recorded = await writer.write(
            lambda conn, j=job: _record_outcome(conn, j, lambda c: complete(c, j.id))
        )
        if recorded is _Dropped.OUTCOME:
            outcome = "dropped"
    except stop.StopRequested:
        # A stop began before this job's work started (the close chain
        # checks before each stage). Nothing is recorded: the
        # row stays running, and the shutdown reclaim or the next start's
        # reclaim_all_stale puts it back to pending, to run once.
        outcome = "stopped"
    except Exception as exc:
        if _is_process_fatal_permission_error(exc):
            logger.critical(
                "fatal OS permission error while processing job id=%s "
                "type=%s; terminating worker for process restart",
                job.id,
                job.type,
                exc_info=True,
            )
            raise
        message = sanitized_exception(exc)
        disposition = await writer.write(
            lambda conn, j=job, err=message: _record_outcome(
                conn, j, lambda c: _fail_job(c, j, err)
            )
        )
        if disposition is _Dropped.OUTCOME or disposition is None:
            # Nothing was recorded for this run and fail() scheduled
            # no retry: the claimed row no longer stands
            # (_record_outcome logs that), or fail() found no row.
            outcome = "dropped"
        elif disposition.terminal:
            outcome = "failed"
            logger.error(
                "job failed permanently id=%s type=%s site=%s "
                "attempt %d of %d (retries exhausted): %s",
                job.id,
                job.type,
                job.site_id,
                disposition.retry_count,
                disposition.max_retries + 1,
                message,
            )
        else:
            outcome = "retry"
            logger.warning(
                "job failed id=%s type=%s site=%s attempt=%s/%s next=%s: %s",
                job.id,
                job.type,
                job.site_id,
                disposition.retry_count,
                disposition.max_retries,
                disposition.next_attempt_at,
                message,
            )
    logger.info(
        "cycle: job=%s type=%s site=%s outcome=%s elapsed=%.1fs lane=%s",
        job.id,
        job.type,
        job.site_id,
        outcome,
        time.monotonic() - claimed_at,
        lane,
    )


class _Dropped(Enum):
    """``_record_outcome``'s result when the claimed row no longer stands."""

    OUTCOME = auto()


def _still_claimed(conn: sqlite3.Connection, job: Job) -> bool:
    # jobs.id is INTEGER PRIMARY KEY without AUTOINCREMENT and a site delete
    # cascades a running job, so the id alone can name a different job.
    # type + status pin the original: each lane runs one job at a time, the
    # lanes claim disjoint types (wxverify.db.queue), and only a claim sets
    # 'running' -- so a running row of this id and type is this job's own.
    return (
        conn.execute(
            "SELECT 1 FROM jobs WHERE id = ? AND type = ? AND status = 'running'",
            (job.id, job.type),
        ).fetchone()
        is not None
    )


def _record_outcome[T](
    conn: sqlite3.Connection,
    job: Job,
    record: Callable[[sqlite3.Connection], T],
) -> T | Literal[_Dropped.OUTCOME]:
    """Run ``record`` only while ``job``'s claimed row still stands.

    Returns ``_Dropped.OUTCOME``, having written nothing, when it does not.
    """
    if not _still_claimed(conn, job):
        logger.info(
            "job outcome dropped id=%s type=%s site=%s: row no longer claimed",
            job.id,
            job.type,
            job.site_id,
        )
        return _Dropped.OUTCOME
    return record(conn)


def _fail_job(conn: sqlite3.Connection, job: Job, error: str) -> FailDisposition | None:
    """Record the failure.

    A ``day_close`` or ``day_publish`` job records its failure in the same
    transaction, but before ``fail()``: its stage-log row copies the job
    row's claim stamp, ``updated_at``, which ``fail()`` overwrites. Both
    kinds are enqueued with ``max_retries`` 0, so every failure of theirs is
    terminal.

    A ``fetch_feed`` job writes its try's ``failed`` download row there too,
    before ``fail()`` overwrites the claim stamp it takes as the try's start.
    """
    if job.type == "day_close":
        fail_day_close_job(conn, job, error, now_utc=utc_now())
    elif job.type == "day_publish":
        fail_day_publish_job(conn, job, error, now_utc=utc_now())
    elif job.type == "fetch_feed":
        _record_failed_fetch(conn, job, error)
    return fail(
        conn,
        job.id,
        error,
        min_delay_seconds=fetch_feed_retry_floor_seconds(conn, job),
    )


async def _maybe_stamp_runtime_heartbeat(
    db: Database, key: str, last_stamp_at: float, now: float
) -> float:
    if last_stamp_at > 0 and now - last_stamp_at < RUNTIME_HEARTBEAT_INTERVAL_SECONDS:
        return last_stamp_at
    try:
        # Exempt from generation fencing: a single global runtime_state
        # heartbeat stamp, called from the loop between jobs -- no job-scoped
        # read behind it.
        await db.write(
            lambda conn, state_key=key: set_runtime_state_now(conn, state_key)
        )
    except Exception:
        logger.exception("runtime heartbeat write failed key=%s", key)
    return now


async def dispatch(db: Database, writer: FencedWriter, job: Job) -> None:
    logger.debug("dispatch type=%s site=%s", job.type, job.site_id)
    if job.type == "fetch_obs" and job.job_key == WIND_DAYS_JOB_KEY:
        # The wind lane is the one fetch_obs job left; any other key is the
        # retired routine download and ends at the raise below.
        site_id = job.site_id
        if site_id is None:
            raise JobCancelled()
        await run_wind_days(db, writer, site_id)
        return None
    if job.type == "backfill_site" and job.job_key == OM_ARCHIVE_JOB_KEY:
        # The Open-Meteo archive fill reuses the retired type; any other key
        # is the retired backfill and ends at the raise below.
        await run_om_archive_job(db, writer, job)
        return None
    if job.type == "fetch_current_obs":
        site_id = job.site_id
        if site_id is None:
            raise JobCancelled()
        station_id = _payload_int(job.payload, "station_id")
        if station_id is None:
            raise JobCancelled()
        await _fetch_current_obs(db, writer, site_id, station_id)
        return None
    if job.type == "fetch_feed":
        site_id = job.site_id
        if site_id is None:
            raise JobCancelled()
        feed_id = _payload_int(job.payload, "feed_id")
        if feed_id is None:
            raise JobCancelled()
        await _fetch_feed(db, writer, site_id, feed_id, payload=job.payload)
        return None
    if job.type == "day_close":
        await run_day_close(db, writer, job)
        return None
    if job.type == "day_publish":
        await run_day_publish(db, writer, job)
        return None
    raise RuntimeError(f"unknown job type {job.type}")


async def _fetch_current_obs(
    db: Database, writer: FencedWriter, site_id: int, station_id: int
) -> None:
    """Poll ``/observations/current`` for one station, learn cadence, snapshot.

    Touches only ``station_poll_state`` (diagnostics + cadence) and
    ``station_current_obs`` (last-good snapshot), never ``stations``. One
    station per job, one provider call. On 429/>=500 the shared
    ``api.weather.com`` domain backoff is recorded and the job is deferred;
    on transport failure the poll is marked transient and deferred to the
    floor. On 401/403 the ``current_obs``
    refused-access hold (``worker.access_hold``) is recorded in place of any
    poll state and the job is deferred to the hold's end, as is a try that
    meets the hold; a 2xx reply clears it.
    """
    api_key = resolve_secret("weathercom")
    if not api_key:
        raise RuntimeError("weathercom key is not configured")
    pws_station_id = await db.read(
        lambda conn: _pws_station_id(conn, site_id, station_id)
    )
    if pws_station_id is None:
        raise JobCancelled()

    # Operator-configurable read timeout; default 30s, floored at 1s to
    # match the config.yaml int(1,300) schema. Read before the call lock is
    # taken, so it does not hold up the other weather.com callers.
    timeout_seconds = await db.read(
        lambda conn: get_number_setting(conn, "request_timeout_seconds", 30, minimum=1)
    )

    # The shared weather.com call lock covers the reserve, the call and the
    # outcome write (see worker.station_pacing.weathercom_call_lock).
    async with weathercom_call_lock():
        # Reserve in one transaction: backoff gate first (raises JobDeferred
        # if active), then the current_obs refused-access hold (raises
        # AccessHeld), then budget (raises JobDeferred if exhausted). A single
        # station ⇒ ordinal 0 ⇒ no station pacing (station_pacing returns 0.0
        # at ordinal 0), so no pace_station_call here by design.
        try:
            reservation = await writer.write(
                lambda conn: _reserve_current_obs_call(conn, site_id, station_id)
            )
        except AccessHeld as held:
            # A held try: the reservation write rolled back, so no call, no
            # budget and no poll state. The job waits, pending, for the hold's
            # end, which is the poll's next try.
            raise JobDeferred(held.until) from None

        try:
            response = await fetch_current_observation(
                pws_station_id, api_key, timeout_seconds=timeout_seconds
            )
            # Classifying inside the try, not after: a malformed 2xx body raises
            # from classify_current_obs (response.json()), and that failure must
            # land on the same transient-floor path as a transport error rather
            # than escape uncaught to the job-level retry, which never touches
            # station_poll_state.next_poll_at.
            outcome = classify_current_obs(response)
        except Exception as exc:
            # Transport-level failure (timeout / connect / read) or a malformed
            # response body: transient, retry at the floor. Do not record a
            # domain backoff (no reliable HTTP status to key on here).
            error = sanitized_exception(exc)
            transient = PollOutcome(Health.TRANSIENT, error=error)
            refund = reservation if is_refundable_transport_error(exc) else None
            await write_after_reservation(
                db,
                writer,
                lambda conn, out=transient, res=refund: _persist_poll_result_and_refund(
                    conn, site_id, station_id, out, res
                ),
            )
            raise

        status = response.status_code

        # 401 / 403: weather.com refused access. Hold the current_obs kind for
        # every station and defer to the hold's end. The call was made, so the
        # reservation is not refunded; the poll state is not written, so the
        # station keeps what its last real reply left (classify_current_obs's
        # TERMINAL for these codes is never persisted).
        if status in (401, 403):
            # The provider's own reason is read here, outside the write.
            reason = provider_reason(response)
            until = await write_after_reservation(
                db,
                writer,
                lambda conn, s=status, r=reason: record_access_refused(
                    conn, "current_obs", s, reason=r
                ),
            )
            raise JobDeferred(until)

        # 429 / >=500: record the shared domain backoff (single write with the
        # transient poll-state) and defer.
        # Classification already returned TRANSIENT for these codes.
        if status == 429 or status >= 500:
            next_attempt_at = await write_after_reservation(
                db,
                writer,
                lambda conn, resp=response, out=outcome: _record_current_obs_backoff(
                    conn, site_id, station_id, resp, out
                ),
            )
            # record_http_backoff always returns a next-attempt for 429/>=500.
            raise JobDeferred(next_attempt_at or isoformat_utc())

        await write_after_reservation(
            db,
            writer,
            lambda conn, out=outcome, s=status: _persist_poll_result_clearing(
                conn, site_id, station_id, out, s
            ),
        )


def _reserve_current_obs_call(
    conn: sqlite3.Connection, site_id: int, station_id: int
) -> Reservation:
    """Domain-backoff gate then budget reservation.

    Raises ``JobDeferred`` if the shared weather.com backoff is active or the
    daily budget is exhausted; ``AccessHeld`` if the ``current_obs``
    refused-access hold is in force; ``JobCancelled`` if the station is
    gone/disabled.
    Returns the ``Reservation`` so a refundable transport failure can refund it.
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
    check_access_hold(conn, "current_obs")
    return reserve_budget(conn, "weathercom", 1)


def _persist_poll_result_clearing(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    outcome: PollOutcome,
    status: int,
) -> None:
    """Persist the poll-state, store an online reply's raw reading, and, on a
    2xx reply, clear the current_obs hold.

    One transaction, so the clear, the persist and the raw reading land
    together. A granted call clears its own kind only; the other kinds' holds
    are left as they are. The raw reading is stored only when the poll-state
    was saved (not for a station deleted, disabled or moved meanwhile), and
    it cannot fail the poll's own write: its values passed the per-value check
    when the reply was parsed, and ``store_readings`` drops whatever a CHECK
    of its table would refuse. An offline reply stores none.
    """
    saved = persist_poll_result(conn, site_id, station_id, outcome)
    if saved and outcome.health is Health.ONLINE and outcome.obs is not None:
        _store_current_obs_reading(conn, station_id, outcome.obs)
    if 200 <= status < 300:
        clear_access_refused(conn, "current_obs")


def _store_current_obs_reading(
    conn: sqlite3.Connection, station_id: int, obs: CurrentObservation
) -> None:
    """Store one online poll reply as a raw reading, source ``current_obs``.

    The instant is ``obsTimeUtc`` in whole seconds and the local date
    ``obsTimeLocal``'s, or none. The values are ``obs``' ``reading_*`` fields:
    the reply's own ``metric.temp``, ``metric.precipTotal`` and
    ``metric.windSpeed`` checked by ``reading_value`` in
    ``current_obs_from_payload``. A rejected value is stored NULL and counted
    in a DEBUG line. A reply with no valid value, or whose instant does not
    parse, stores nothing. A poll repeating the stored reading adds no row
    (``store_readings``' upsert on the instant).
    """
    if obs.obs_time_utc is None:
        return
    try:
        obs_at = parse_utc(obs.obs_time_utc).replace(microsecond=0)
    except ValueError:
        return
    if obs.reading_unusable:
        logger.debug(
            "current_obs reading values unusable station=%s"
            " temperature=%d precip=%d wind=%d",
            station_id,
            *(
                int(quantity in obs.reading_unusable)
                for quantity in ("temperature", "precip", "wind")
            ),
        )
    temp_c = obs.reading_temp_c
    precip_total_mm = obs.reading_precip_total_mm
    speed_kmh = obs.reading_speed_kmh
    if temp_c is None and precip_total_mm is None and speed_kmh is None:
        return
    now = utc_now()
    store_readings(
        conn,
        station_id=station_id,
        source="current_obs",
        readings=[
            StationReading(
                obs_at=obs_at,
                source="current_obs",
                local_date=obs.local_date,
                temp_c=temp_c,
                precip_total_mm=precip_total_mm,
                speed_kmh=speed_kmh,
            )
        ],
        fetched_at=now,
        now_utc=now,
    )


def _persist_poll_result_and_refund(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    outcome: PollOutcome,
    reservation: Reservation | None,
) -> None:
    """Persist the transient poll-state and refund a phantom reservation."""
    persist_poll_result(conn, site_id, station_id, outcome)
    if reservation is not None:
        refund_budget(conn, reservation)


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


async def _fetch_feed(
    db: Database,
    writer: FencedWriter,
    site_id: int,
    feed_id: int,
    *,
    payload: dict[str, object],
) -> None:
    """Run one ``fetch_feed`` try and record its download row.

    The slot comes from ``payload``; a payload with no ``slot`` runs
    ``unscheduled``. A persisted reply's row is written with its samples
    (``fetch_feed_once``), and so is a refused answer's ``refused`` row,
    after which the job is cancelled; ``budget``, ``backoff``,
    ``ineligible`` and ``unavailable`` are written here before the job is
    deferred or cancelled, and ``failed`` by ``_fail_job`` for a raised
    error.

    A round try is cancelled before any call, with no row, at
    or after its round's bound or once the feed has a ``stored`` round row
    for the date. Its ``budget`` or ``backoff`` row is followed by
    ``JobCancelled``, not ``JobDeferred``: a deferred try would run again
    later, past the bound.
    """
    logger.debug("fetch_feed dispatch site=%s feed=%s", site_id, feed_id)
    slot = _download_slot(payload, started_at=utc_now())
    round_date = slot.round_date if slot.slot == "round" else None
    if round_date is not None and await db.read(
        lambda conn: round_try_stale(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            round_date=round_date,
            now_utc=slot.started_at,
        )
    ):
        logger.info(
            "fetch_feed round try cancelled site=%s feed=%s round_date=%s",
            site_id,
            feed_id,
            round_date.isoformat(),
        )
        raise JobCancelled()
    outcome = await fetch_feed_once(
        db, site_id, feed_id, writer=writer, adapter_builder=build_adapter, slot=slot
    )
    if isinstance(outcome, BudgetExhausted):
        await _record_fetch_try(writer, site_id, feed_id, slot, "budget")
        if round_date is not None:
            raise JobCancelled()
        raise JobDeferred(outcome.next_window)
    if isinstance(outcome, BackoffActive):
        await _record_fetch_try(writer, site_id, feed_id, slot, "backoff")
        if round_date is not None:
            raise JobCancelled()
        raise JobDeferred(outcome.next_attempt)
    if isinstance(outcome, Ineligible):
        await _record_fetch_try(writer, site_id, feed_id, slot, "ineligible")
        raise JobCancelled()
    if isinstance(outcome, Unavailable):
        logger.warning(
            "feed adapter unavailable site=%s feed=%s source=%s: %s",
            outcome.target.site_id,
            outcome.target.feed_id,
            outcome.target.source,
            outcome.error,
        )
        await writer.write(
            lambda conn, result=outcome: _mark_feed_unavailable_and_record(
                conn, result, slot
            )
        )
        # Complete through the cancelled branch, without the success marker:
        # the adapter could not be built, so no fetch ran and nothing about
        # an earlier failure of this scope has been resolved.
        raise JobCancelled()
    if isinstance(outcome, Refused):
        logger.warning(
            "feed answer refused site=%s feed=%s source=%s: %s",
            outcome.target.site_id,
            outcome.target.feed_id,
            outcome.target.source,
            outcome.error,
        )
        # fetch_feed_once already wrote the feed's state and the refused
        # download row. Complete through the cancelled branch, without the
        # success marker and without a retry: the same request gets the
        # same answer, so the next try is the feed's next scheduled one.
        raise JobCancelled()


# A daytime slot in a fetch_feed payload: local "HH:MM".
_DAYTIME_SLOT_RE: Final = re.compile(r"(?:[01]\d|2[0-3]):[0-5]\d")


def _download_slot(payload: dict[str, object], *, started_at: datetime) -> DownloadSlot:
    """The try's slot from a ``fetch_feed`` payload.

    No ``slot`` key gives ``unscheduled``: the enable-time and command-line
    fetches, and any job queued before the upgrade. ``"round"`` needs a
    ``round_date`` (``YYYY-MM-DD``) and a ``round_try``; a daytime slot is
    local ``"HH:MM"``. A slot that does not read is logged and recorded as
    ``unscheduled``, so the try still has its row.
    """
    value = payload.get("slot")
    if value is None or value == "unscheduled":
        return DownloadSlot(
            slot="unscheduled", round_date=None, round_try=None, started_at=started_at
        )
    if value == "round":
        round_date = _payload_date(payload, "round_date")
        round_try = _payload_int(payload, "round_try")
        if round_date is not None and round_try is not None:
            return DownloadSlot(
                slot="round",
                round_date=round_date,
                round_try=round_try,
                started_at=started_at,
            )
    elif isinstance(value, str) and _DAYTIME_SLOT_RE.fullmatch(value):
        return DownloadSlot(
            slot=value, round_date=None, round_try=None, started_at=started_at
        )
    logger.warning(
        "fetch_feed payload slot does not read; the try is recorded as unscheduled"
    )
    return DownloadSlot(
        slot="unscheduled", round_date=None, round_try=None, started_at=started_at
    )


def _payload_date(payload: dict[str, object], key: str) -> date | None:
    value = payload.get(key)
    if not isinstance(value, str):
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


async def _record_fetch_try(
    writer: FencedWriter,
    site_id: int,
    feed_id: int,
    slot: DownloadSlot,
    outcome: DownloadOutcome,
) -> None:
    """Write the download row of a try that ended without a reply."""
    await writer.write(
        lambda conn: record_download_try(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            slot=slot,
            outcome=outcome,
            ended_at=utc_now(),
        )
    )


def _mark_feed_unavailable_and_record(
    conn: sqlite3.Connection, result: Unavailable, slot: DownloadSlot
) -> None:
    """Stamp the unavailable feed's state and write the try's row, in one
    transaction."""
    mark_feed_unavailable(conn, result.target, result.error)
    record_download_try(
        conn,
        site_id=result.target.site_id,
        feed_id=result.target.feed_id,
        slot=slot,
        outcome="unavailable",
        ended_at=utc_now(),
    )


def _record_failed_fetch(conn: sqlite3.Connection, job: Job, error: str) -> None:
    """``_fail_job``'s ``fetch_feed`` branch: the try's ``failed`` download
    row, with ``error`` (already sanitized) as its reason.

    The try started when the job was claimed: the row's ``updated_at`` holds
    the claim stamp until ``fail()`` overwrites it, so this runs first.
    """
    site_id = job.site_id
    feed_id = _payload_int(job.payload, "feed_id")
    if site_id is None or feed_id is None:
        return
    try:
        record_download_try(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            slot=_download_slot(job.payload, started_at=_claim_instant(conn, job.id)),
            outcome="failed",
            ended_at=utc_now(),
            reason=error,
        )
    except Exception:
        # Broad on purpose, as fetch_feed_retry_floor_seconds is: this runs
        # inside fail()'s failure-recording write, and anything escaping
        # aborts that write before retry_count advances, so the job would be
        # re-pended at the same count and crash-loop. A missing download row
        # is the smaller loss. CancelledError is BaseException and still
        # propagates.
        logger.warning(
            "fetch_feed failure: download row not written job=%s",
            job.id,
            exc_info=True,
        )


def _claim_instant(conn: sqlite3.Connection, job_id: int) -> datetime:
    """The job row's claim stamp (its ``updated_at`` while running), or now
    when it does not read."""
    row = conn.execute("SELECT updated_at FROM jobs WHERE id = ?", (job_id,)).fetchone()
    stamp: object = None if row is None else row["updated_at"]
    if isinstance(stamp, str):
        try:
            return parse_utc(stamp)
        except ValueError:
            pass
    return utc_now()


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
