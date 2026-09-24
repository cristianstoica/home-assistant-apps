"""The current-obs lane: claims and runs ``fetch_current_obs`` jobs only.

Runs beside the main worker lane under ``run_worker``'s supervision
(worker.processor), so a long main-lane job cannot hold up a current-obs
poll. The job runner arrives as the required ``run_job`` argument instead of
an import: worker.processor imports this module, never the reverse.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from collections.abc import Awaitable, Callable
from typing import Final

from wxverify.db.connection import Database
from wxverify.db.queue import Job, claim_next_current_obs_job
from wxverify.db.runtime_state import (
    RUNTIME_HEARTBEAT_INTERVAL_SECONDS,
    set_runtime_state_now,
)
from wxverify.worker.scheduler import enqueue_due_current_obs

POLL_SECONDS: Final = 5.0
HEARTBEAT_KEY: Final = "current_obs_poller_last_loop_at"

# run_claimed_job's shape: (db, job, *, lane) -> None, keyword ``lane`` required.
RunJob = Callable[..., Awaitable[None]]


async def run_current_obs_poller(db: Database, *, run_job: RunJob) -> None:
    """Enqueue due current-obs jobs and run them one at a time, forever.

    Sleeps only when a pass claims nothing; a claimed job, whatever its
    outcome, loops straight back. ``run_job`` is awaited directly after the
    claim returns, with no await in between, so its fence capture observes
    the claim's generation. An error from the loop's own write propagates to
    the supervisor.
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
        job = await db.write(
            lambda conn, s=stamp: _enqueue_and_claim(conn, stamp_heartbeat=s)
        )
        if stamp:
            last_heartbeat = now_mono
        if job is None:
            await asyncio.sleep(POLL_SECONDS)
            continue
        await run_job(db, job, lane="current_obs")


def _enqueue_and_claim(
    conn: sqlite3.Connection, *, stamp_heartbeat: bool
) -> Job | None:
    """One write transaction: enqueue due jobs, claim one, maybe heartbeat."""
    enqueue_due_current_obs(conn)
    job = claim_next_current_obs_job(conn)
    if stamp_heartbeat:
        set_runtime_state_now(conn, HEARTBEAT_KEY)
    return job
