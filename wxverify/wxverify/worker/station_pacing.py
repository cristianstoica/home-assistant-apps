"""Bounded pacing for per-station PWS calls."""

from __future__ import annotations

import asyncio
import hashlib
from typing import Final

PWS_STATION_CONCURRENCY: Final = 1
PWS_STATION_MIN_DELAY_SECONDS: Final = 0.05
PWS_STATION_MAX_DELAY_SECONDS: Final = 0.25


def station_call_limiter() -> asyncio.Semaphore:
    return asyncio.Semaphore(PWS_STATION_CONCURRENCY)


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
