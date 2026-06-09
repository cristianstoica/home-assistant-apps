# pyright: strict
"""Production `Clock` / `Sleeper` seams for the live updater loop.

The live sleeper is backed by a single `threading.Event` (the *only* thread
primitive in the package — production already holds it; no ``Timer``, no pool, no
thread-per-resolve). A SIGTERM/SIGINT handler sets the event, so an in-flight
interval or backoff sleep aborts promptly and the loop starts no new attempt.
"""

from __future__ import annotations

import threading
import time


def monotonic() -> float:
    """Production `Clock`: monotonic seconds (immune to wall-clock jumps)."""
    return time.monotonic()


class EventSleeper:
    """An interruptible `Sleeper` backed by a `threading.Event`.

    ``__call__(seconds)`` waits up to `seconds`, returning ``True`` immediately
    if (or as soon as) `request_stop` is called — so the updater aborts before its
    next attempt. This is the single ``threading.Event`` the package owns.
    """

    def __init__(self) -> None:
        self._stop = threading.Event()

    def request_stop(self) -> None:
        """Signal any in-flight and all future sleeps to return immediately."""
        self._stop.set()

    def __call__(self, seconds: float) -> bool:
        """Sleep up to `seconds`; return ``True`` iff stop was/has been signalled."""
        return self._stop.wait(timeout=seconds)
