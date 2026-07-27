"""Fire-and-forget scheduling for the best-effort post-read rescore enqueue."""

from __future__ import annotations

import asyncio
import logging

from wxverify.db.connection import get_db
from wxverify.scoring.composite import enqueue_score_rescore

logger = logging.getLogger(__name__)

_in_flight_sites: set[int] = set()
_pending_tasks: set[asyncio.Task[None]] = set()  # strong refs (GC guard)


def schedule_score_rescore(site_id: int) -> None:
    """Schedule the cooldown-guarded rescore enqueue without gating the caller.

    Per-site in-flight dedupe: while one enqueue task for this site is still
    pending (typically queued on the write lock), further calls are no-ops —
    the queue-level pending/running dedupe would swallow them anyway; this
    just stops tasks piling up behind a long write-lock hold.
    """
    if site_id in _in_flight_sites:
        return
    _in_flight_sites.add(site_id)
    task = asyncio.get_running_loop().create_task(_run(site_id))
    _pending_tasks.add(task)
    task.add_done_callback(lambda t: _cleanup(site_id, t))


def _cleanup(site_id: int, task: asyncio.Task[None]) -> None:
    _in_flight_sites.discard(site_id)
    _pending_tasks.discard(task)


async def _run(site_id: int) -> None:
    try:
        await get_db().write(lambda conn: enqueue_score_rescore(conn, site_id))
    except Exception:
        logger.warning("rescore enqueue failed site=%s", site_id, exc_info=True)


async def drain_pending_rescores() -> None:
    """Await a snapshot of the pending enqueue tasks (test determinism seam).

    Production code never calls this; tests use it to make the
    fire-and-forget path deterministic. It is a real seam over the real
    registry, not a fake.
    """
    await asyncio.gather(*list(_pending_tasks), return_exceptions=True)
