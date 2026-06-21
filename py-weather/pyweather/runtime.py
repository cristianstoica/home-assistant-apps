# pyright: strict
"""Production `Clock` / `WallClock` / `Sleeper` seams for the live scheduler.

The live sleeper is backed by a single `threading.Event` (the *only* thread
primitive in the package). A SIGTERM/SIGINT handler sets the event, so an
in-flight cadence sleep, settle wait, or freshness re-read wait aborts promptly
and the loop starts no new poll/read.

`monotonic` drives the per-station scheduling cadence (immune to wall-clock
jumps); `SystemWallClock` is the freshness ``t0`` source — a tz-aware UTC
instant comparable to Home Assistant's ISO-8601 state timestamps (the cadence
clock must NOT be used for freshness, and vice versa).
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone


def monotonic() -> float:
    """Production `Clock`: monotonic seconds (immune to wall-clock jumps)."""
    return time.monotonic()


class SystemWallClock:
    """Production `WallClock`: ``datetime.now(timezone.utc)`` (tz-aware UTC).

    The freshness check compares this ``t0`` against parsed HA state timestamps,
    so it must be a real wall-clock instant — never ``time.monotonic()``, which
    is not comparable to a calendar timestamp.
    """

    def now(self) -> datetime:
        """Return the current tz-aware UTC instant."""
        return datetime.now(timezone.utc)


class EventSleeper:
    """An interruptible `Sleeper` backed by a `threading.Event`.

    ``__call__(seconds)`` waits up to `seconds`, returning ``True`` immediately
    if (or as soon as) `request_stop` is called — so the scheduler aborts before
    its next poll/read. This is the single ``threading.Event`` the package owns.
    """

    def __init__(self) -> None:
        self._stop = threading.Event()

    def request_stop(self) -> None:
        """Signal any in-flight and all future sleeps to return immediately."""
        self._stop.set()

    def __call__(self, seconds: float) -> bool:
        """Sleep up to `seconds`; return ``True`` iff stop was/has been signalled."""
        return self._stop.wait(timeout=seconds)
