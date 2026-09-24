"""Bounded pacing and the shared call lock for per-station PWS calls."""

from __future__ import annotations

import asyncio
import hashlib
import weakref
from typing import Final

PWS_STATION_MIN_DELAY_SECONDS: Final = 0.05
PWS_STATION_MAX_DELAY_SECONDS: Final = 0.25

# One lock serializes every weather.com call in the process, across the worker
# and the add-station route: the reserve, the call and the outcome write run
# under it; the pace sleep stays outside. Lock order: always this lock, then
# the database write lock; never taken while the write lock is held. Created
# lazily, one per running event loop, because a module-level asyncio.Lock()
# binds to the first loop that contends it. The loop is held through a weak
# reference, so a new loop that reuses a dead loop's id gets a fresh lock.
_call_lock: asyncio.Lock | None = None
_call_lock_loop: weakref.ref[asyncio.AbstractEventLoop] | None = None


def weathercom_call_lock() -> asyncio.Lock:
    """The one weather.com call lock for the running event loop."""
    global _call_lock, _call_lock_loop
    loop = asyncio.get_running_loop()
    if _call_lock is None or _call_lock_loop is None or _call_lock_loop() is not loop:
        _call_lock = asyncio.Lock()
        _call_lock_loop = weakref.ref(loop)
    return _call_lock


async def acquire_within(lock: asyncio.Lock, timeout_s: float) -> bool:
    """Acquire ``lock`` within ``timeout_s``.

    True: held by the caller. False: not held. A cancellation while queued
    propagates unchanged, and the caller does not hold the lock.
    """
    try:
        async with asyncio.timeout(timeout_s):
            await lock.acquire()
    except TimeoutError:
        return False
    return True


def station_call_delay_seconds(
    site_id: int,
    station_id: int,
    ordinal: int,
    *,
    seed: int = 1729,
    min_seconds: float = PWS_STATION_MIN_DELAY_SECONDS,
    max_seconds: float = PWS_STATION_MAX_DELAY_SECONDS,
) -> float:
    if ordinal <= 0 or max_seconds <= 0:
        return 0.0
    lower = max(0.0, min_seconds)
    upper = max(lower, max_seconds)
    span_ms = int(round((upper - lower) * 1000))
    if span_ms <= 0:
        return lower
    digest = hashlib.blake2b(
        f"{seed}:{site_id}:{station_id}:{ordinal}".encode(), digest_size=8
    ).digest()
    offset_ms = int.from_bytes(digest, "big") % (span_ms + 1)
    return lower + (offset_ms / 1000)


async def pace_station_call(site_id: int, station_id: int, ordinal: int) -> None:
    delay = station_call_delay_seconds(site_id, station_id, ordinal)
    if delay > 0:
        await asyncio.sleep(delay)
