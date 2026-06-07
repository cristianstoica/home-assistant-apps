# pyright: strict
"""Bounded interruptible backoff check — driven synchronously, no real thread/Timer."""

from __future__ import annotations

import threading

from ..errors import TerminalError, TransientError
from ..updater import (
    BASE_DELAY_S,
    JITTER_FRACTION,
    MAX_ATTEMPTS,
    MAX_DELAY_S,
    RETRY_AFTER_CAP_S,
    RetryRunner,
    backoff_delays,
)
from .fakes import FakeClock, FakeSleeper
from .report import report


def _within_jitter(actual: float, base: float) -> bool:
    """True if `actual` sits within the ±jitter band of `base` (clamped at 0)."""
    span = base * JITTER_FRACTION
    return max(0.0, base - span) <= actual <= base + span


def check_backoff() -> bool:
    """Assert the bounded interruptible backoff: sequence, jitter, cap, stop, no thread.

    * **Bounded sequence** — `backoff_delays(None)` is ``2 -> 4`` (length
      ``MAX_ATTEMPTS - 1 == 2``), the doubling capped at ``MAX_DELAY_S``.
    * **Exhaustion** — an always-transient op runs exactly ``MAX_ATTEMPTS``
      times, sleeps ``MAX_ATTEMPTS - 1`` times, each delay inside the ±20%
      jitter band of the base sequence, and returns ``False``.
    * **No thread** — ``threading.active_count()`` is unchanged across the run
      (the backoff uses the injected `FakeSleeper`, never a real ``Timer``/pool).
    * **Stop mid-backoff** — when the sleeper signals stop on its first sleep,
      the runner returns ``False`` **before** the next attempt (attempts stop at
      the in-flight one; no further ``op`` call).
    * **Retry-After** — a transient carrying ``retry_after`` drives the delay,
      capped at ``RETRY_AFTER_CAP_S``.
    * **Terminal short-circuit** — a `TerminalError` propagates on the first
      attempt (no sleep, no retry).
    """
    checks: list[tuple[str, bool]] = []

    # --- bounded base sequence ---
    seq = backoff_delays(None)
    checks += [
        (
            f"base sequence length is MAX_ATTEMPTS-1 ({MAX_ATTEMPTS - 1})",
            len(seq) == MAX_ATTEMPTS - 1,
        ),
        (
            "base sequence starts at 2 and doubles",
            seq[0] == BASE_DELAY_S and seq[1] == BASE_DELAY_S * 2,
        ),
        ("base sequence is capped at MAX_DELAY_S", all(d <= MAX_DELAY_S for d in seq)),
    ]
    capped = backoff_delays(100.0)  # Retry-After above the cap
    checks.append(
        ("retry-after delays capped at 60", all(d == RETRY_AFTER_CAP_S for d in capped))
    )

    # --- exhaustion + jitter band + no thread ---
    base_threads = threading.active_count()

    def _always_transient() -> None:
        raise TransientError("always fails")

    sleeper = FakeSleeper()
    runner = RetryRunner(FakeClock(), sleeper)
    result = runner.run(_always_transient)
    checks += [
        ("exhausted op returns False", result is False),
        (
            f"op tried exactly MAX_ATTEMPTS times ({MAX_ATTEMPTS})",
            runner.attempts == MAX_ATTEMPTS,
        ),
        (
            f"slept MAX_ATTEMPTS-1 times ({MAX_ATTEMPTS - 1})",
            len(sleeper.slept) == MAX_ATTEMPTS - 1,
        ),
        (
            "each sleep is within the ±20% jitter band of the base sequence",
            all(
                _within_jitter(sleeper.slept[i], seq[i])
                for i in range(len(sleeper.slept))
            ),
        ),
        (
            "no thread spawned (active_count unchanged)",
            threading.active_count() == base_threads,
        ),
    ]

    # --- stop mid-backoff aborts before the next attempt ---
    calls: list[int] = []

    def _count_then_transient() -> None:
        calls.append(1)
        raise TransientError("fails")

    stop_sleeper = FakeSleeper(stop_at=0)  # stop signalled on the first sleep
    stop_runner = RetryRunner(FakeClock(), stop_sleeper)
    stop_result = stop_runner.run(_count_then_transient)
    checks += [
        ("stop mid-backoff returns False", stop_result is False),
        ("stop mid-backoff makes no further attempt (op called once)", len(calls) == 1),
        (
            "stop mid-backoff slept exactly once before aborting",
            len(stop_sleeper.slept) == 1,
        ),
    ]

    # --- retry-after drives the delay ---
    ra_sleeper = FakeSleeper()
    ra_runner = RetryRunner(FakeClock(), ra_sleeper)

    def _transient_retry_after() -> None:
        raise TransientError("rate limited", retry_after=9.0)

    ra_runner.run(_transient_retry_after)
    checks.append(
        (
            "retry-after value drives the delay (within jitter of 9)",
            _within_jitter(ra_sleeper.slept[0], 9.0),
        )
    )

    # --- terminal short-circuits with no sleep ---
    term_sleeper = FakeSleeper()
    term_runner = RetryRunner(FakeClock(), term_sleeper)

    def _terminal() -> None:
        raise TerminalError("bad credential")

    terminal_propagated = False
    try:
        term_runner.run(_terminal)
    except TerminalError:
        terminal_propagated = True
    checks += [
        ("TerminalError propagates from the runner", terminal_propagated),
        (
            "TerminalError causes no sleep and a single attempt",
            term_sleeper.slept == [] and term_runner.attempts == 1,
        ),
    ]
    return report("BACKOFF", "backoff", checks)
