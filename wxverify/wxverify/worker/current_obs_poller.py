"""The current-obs lane: claims and runs ``fetch_current_obs`` jobs only.

Runs beside the main worker lane under ``run_worker``'s supervision
(worker.processor), so a long main-lane job cannot hold up a current-obs
poll. The job runner arrives as the required ``run_job`` argument instead of
an import: worker.processor imports this module, never the reverse.

Each claim logs one ``current-obs claim`` line with the job's lateness,
measured from values read inside the claim transaction.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from wxverify.core.timeutil import parse_utc, utc_now
from wxverify.db.connection import Database
from wxverify.db.queue import Job, claim_next_current_obs_job
from wxverify.db.runtime_state import (
    RUNTIME_HEARTBEAT_INTERVAL_SECONDS,
    set_runtime_state_now,
)
from wxverify.worker.scheduler import enqueue_due_current_obs

logger = logging.getLogger(__name__)

POLL_SECONDS: Final = 5.0
HEARTBEAT_KEY: Final = "current_obs_poller_last_loop_at"

# A claim line is INFO when its total lateness or queue wait reaches this.
_LATENESS_INFO_SECONDS: Final = 60.0
# next_attempt_at must pass created_at by more than this to count as
# rescheduled: it absorbs the ms-vs-us precision gap between the two writers.
_RESCHEDULE_TOLERANCE: Final = timedelta(seconds=1)
# SQLite's INTEGER range. Binding an int outside it raises OverflowError.
_SQLITE_INT_MIN: Final = -(2**63)
_SQLITE_INT_MAX: Final = 2**63 - 1

_LATENESS_SQL: Final = """
    SELECT j.created_at, j.next_attempt_at, sps.next_poll_at, sps.health_state
    FROM jobs j LEFT JOIN station_poll_state sps ON sps.station_id = ?
    WHERE j.id = ?
"""

_PREV_JOB_SQL: Final = """
    SELECT status FROM jobs
    WHERE type = 'fetch_current_obs' AND job_key = ? AND site_id IS ? AND id < ?
    ORDER BY id DESC LIMIT 1
"""

# run_claimed_job's shape: (db, job, *, lane) -> None, keyword ``lane`` required.
RunJob = Callable[..., Awaitable[None]]


@dataclass(frozen=True)
class ClaimedCurrentObs:
    """A claimed job and the lateness inputs read in its claim transaction.

    The stamps are the stored values through ``str()``, unparsed, so nothing
    in the transaction can raise on foreign data whose text decodes as UTF-8.
    ``None`` is NULL, a missing row, or a read skipped for a malformed
    payload. ``prev_job`` is the status of the latest earlier job with this
    key, ``None`` when there is none.
    """

    job: Job
    due_at: str | None
    created_at: str | None
    next_attempt_at: str | None
    retry_count: int
    health_state: str | None
    prev_job: str | None


async def run_current_obs_poller(db: Database, *, run_job: RunJob) -> None:
    """Enqueue due current-obs jobs and run them one at a time, forever.

    Sleeps only when a pass claims nothing; a claimed job, whatever its
    outcome, loops straight back. ``run_job`` is awaited directly after the
    claim returns; only the synchronous claim line runs in between, so there
    is no await and its fence capture observes the claim's generation. An
    error from the loop's own write propagates to the supervisor.
    """
    last_heartbeat = 0.0
    while True:
        now_mono = time.monotonic()
        stamp = (
            last_heartbeat == 0.0
            or now_mono - last_heartbeat >= RUNTIME_HEARTBEAT_INTERVAL_SECONDS
        )
        # Exempt: the enqueue, the claim and the heartbeat run with no
        # job-scoped read behind them yet -- there is no generation to fence
        # against until a job is claimed, and run_job captures it first.
        claimed = await db.write(
            lambda conn, s=stamp: _enqueue_and_claim(conn, stamp_heartbeat=s)
        )
        if stamp:
            last_heartbeat = now_mono
        if claimed is None:
            await asyncio.sleep(POLL_SECONDS)
            continue
        # Sync: no await may sit between the claim and run_job's fence capture.
        _log_claim_lateness(claimed)
        await run_job(db, claimed.job, lane="current_obs")


def _enqueue_and_claim(
    conn: sqlite3.Connection, *, stamp_heartbeat: bool
) -> ClaimedCurrentObs | None:
    """One write transaction: enqueue, claim one, read lateness, heartbeat.

    The lateness and prior-job reads run only when a job was claimed, and the
    heartbeat only when ``stamp_heartbeat`` is set.
    """
    enqueue_due_current_obs(conn)
    job = claim_next_current_obs_job(conn)
    claimed = None if job is None else _read_claim_inputs(conn, job)
    if stamp_heartbeat:
        set_runtime_state_now(conn, HEARTBEAT_KEY)
    return claimed


def _read_claim_inputs(conn: sqlite3.Connection, job: Job) -> ClaimedCurrentObs:
    """The lateness read and the prior-job read for a just-claimed job.

    Total on foreign data whose text decodes as UTF-8: a raise here would
    roll back the claim and stop the lane with the job still pending, so
    every bound value fits SQLite's integer range and no stored value is
    parsed.
    """
    due_at: str | None = None
    created_at: str | None = None
    next_attempt_at: str | None = None
    health_state: str | None = None
    station_id = _payload_station_id(job)
    if station_id is not None:
        row = conn.execute(_LATENESS_SQL, (station_id, job.id)).fetchone()
        if row is not None:
            created_at = _text_or_none(row[0])
            next_attempt_at = _text_or_none(row[1])
            due_at = _text_or_none(row[2])
            health_state = _text_or_none(row[3])
    prev_job: str | None = None
    if job.site_id is None or _fits_sqlite_int(job.site_id):
        prev = conn.execute(_PREV_JOB_SQL, (job.job_key, job.site_id, job.id))
        prev_row = prev.fetchone()
        prev_job = None if prev_row is None else _text_or_none(prev_row[0])
    return ClaimedCurrentObs(
        job=job,
        due_at=due_at,
        created_at=created_at,
        next_attempt_at=next_attempt_at,
        retry_count=job.retry_count,
        health_state=health_state,
        prev_job=prev_job,
    )


def poll_lateness_seconds(due_at: str | None, now: datetime) -> float | None:
    """Seconds from ``due_at`` to ``now``, clamped at 0.

    ``None`` when ``due_at`` is missing or does not parse.
    """
    due = _parse_stamp(due_at)
    if due is None:
        return None
    return max(0.0, (now - due).total_seconds())


def _log_claim_lateness(claimed: ClaimedCurrentObs) -> None:
    """Log the ``current-obs claim`` line for a claimed job.

    Synchronous and total, since it runs between the claim and ``run_job``:
    a stamp that does not parse renders as ``unparseable``, and a value that
    cannot be computed renders as ``unknown``. The lateness is measured to
    the claim-observation time, read here after the claim transaction
    returns, so it includes the transaction's completion and the task's
    resumption delay. INFO when the total lateness or the queue wait reaches
    60 s, otherwise DEBUG.
    """
    now = utc_now()
    created = _parse_stamp(claimed.created_at)
    next_attempt = _parse_stamp(claimed.next_attempt_at)
    total_lateness = poll_lateness_seconds(claimed.due_at, now)
    if total_lateness is None:
        total_lateness = poll_lateness_seconds(claimed.created_at, now)
    if created is None or (next_attempt is not None and next_attempt > created):
        eligible, eligible_text = next_attempt, claimed.next_attempt_at
    else:
        eligible, eligible_text = created, claimed.created_at
    queue_wait = (
        None if eligible is None else max(0.0, (now - eligible).total_seconds())
    )
    rescheduled = claimed.retry_count > 0 or (
        created is not None
        and next_attempt is not None
        and next_attempt - created > _RESCHEDULE_TOLERANCE
    )
    late = any(
        value is not None and value >= _LATENESS_INFO_SECONDS
        for value in (total_lateness, queue_wait)
    )
    station_id = _payload_station_id(claimed.job)
    logger.log(
        logging.INFO if late else logging.DEBUG,
        "current-obs claim job=%s station=%s due=%s created=%s eligible=%s"
        " total_lateness=%s queue_wait=%s rescheduled=%s prev_job=%s"
        " prev_health=%s lane=current_obs",
        claimed.job.id,
        "unknown" if station_id is None else int(station_id),
        _render_stamp(claimed.due_at),
        _render_stamp(claimed.created_at),
        "unknown" if eligible is None else eligible_text,
        _render_seconds(total_lateness),
        _render_seconds(queue_wait),
        "yes" if rescheduled else "no",
        claimed.prev_job or "none",
        claimed.health_state or "none",
    )


def _payload_station_id(job: Job) -> int | None:
    """The payload's ``station_id`` as dispatch reads it, or ``None``.

    Mirrors dispatch's ``_payload_int`` (worker.processor), which this module
    cannot import because processor imports it: an ``int`` as it is, a
    ``str`` only when ``str.isdecimal()`` holds, anything else ``None``.
    ``None`` too when the conversion fails, as a very long digit string does
    against Python's int-string digit limit, or when the value falls outside
    SQLite's integer range, where binding it would raise.
    """
    value = job.payload.get("station_id")
    if isinstance(value, int):
        station_id = value
    elif isinstance(value, str) and value.isdecimal():
        try:
            station_id = int(value)
        except ValueError:
            return None
    else:
        return None
    return station_id if _fits_sqlite_int(station_id) else None


def _fits_sqlite_int(value: int) -> bool:
    return _SQLITE_INT_MIN <= value <= _SQLITE_INT_MAX


def _text_or_none(value: object) -> str | None:
    return None if value is None else str(value)


def _parse_stamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return parse_utc(value)
    except ValueError:
        return None


def _render_stamp(value: str | None) -> str:
    if value is None:
        return "none"
    return value if _parse_stamp(value) is not None else "unparseable"


def _render_seconds(value: float | None) -> str:
    return "unknown" if value is None else f"{value:.3f}"
