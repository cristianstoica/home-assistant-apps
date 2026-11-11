"""One process-wide stop.

A stop begins with ``request_stop()``. The SIGTERM/SIGINT wrapper that
``api.app.lifespan`` installs while ``init_db`` runs calls it, and so does
``lifespan``'s ``finally``, before the reap. From then on ``STOP`` is set:

- startup work stops: ``begin_step`` raises ``StopRequested`` before each
  migration step, and the statement running on the one connection doing
  startup work (``register``) is interrupted;
- running work starts nothing new: the worker lanes claim nothing and the
  close chain starts no stage (they read ``STOP`` or call ``check``);
- the shutdown waits are bounded by the stop budget
  (``wait_within_budget``).

A stop belongs to one app run: ``lifespan`` calls ``reset_stop`` when it
starts and again once its ``finally`` has closed the database, because one
process can run many lifespans (the test suite does).

This module imports nothing else from the package, so every layer can
import it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import threading
import time
from collections.abc import Callable, Collection, Iterable
from typing import Any, Final

STOP: Final = threading.Event()

# From the longest measured step (the slowest close stage or job write),
# L = 58.084 s: STOP_BUDGET_S = L + 5, rounded up to whole seconds. The s6
# grace times in Dockerfile and the Supervisor's `timeout:` in config.yaml
# are set from this value; change all three together. Always read through
# this module at the point of use, never bound to a local name at import
# time, so a changed value takes effect.
STOP_BUDGET_S: float = 64.0

# Once the budget has run out and the writer has been
# interrupted, the waits go on for up to this much longer. Measured from the
# same start, so the whole stop ends by STOP_BUDGET_S + GRACE_S however many
# waits are nested.
GRACE_S: Final = 2.0

# SQLite VM instructions between two looks at STOP on the registered
# connection (see `register`).
_PROGRESS_OPS: Final = 10_000

_registered: sqlite3.Connection | None = None
_step: str | None = None
_requested_at: float | None = None


class StopRequested(Exception):
    """A stop has begun; raised by ``check`` and ``begin_step``."""


def register(conn: sqlite3.Connection) -> None:
    """Make ``conn`` the one connection doing startup work.

    ``request_stop`` interrupts the statement it is running. ``interrupt()``
    reaches only a statement already running, so ``conn`` also gets a
    progress handler that ends any statement running while ``STOP`` is set:
    one that starts just after the interrupt stops too, instead of running
    to the end. Call it on the thread that owns ``conn``. Clears the
    recorded step.
    """
    global _registered, _step
    conn.set_progress_handler(_stop_progress, _PROGRESS_OPS)
    _step = None
    _registered = conn


def unregister(conn: sqlite3.Connection) -> None:
    """Undo ``register(conn)``: drop it and its progress handler.

    The progress handler must not outlive startup: on the running writer it
    would interrupt the job write a stop lets finish. A closed
    ``conn`` has no handler left to remove.
    """
    global _registered
    if _registered is conn:
        _registered = None
    with contextlib.suppress(sqlite3.ProgrammingError):
        conn.set_progress_handler(None, 0)


def request_stop() -> None:
    """Begin the stop: set ``STOP``, then interrupt the registered connection.

    The first call starts the stop budget. Callable from any thread and
    from a signal handler; never raises (a connection closed meanwhile has
    no statement left to interrupt).
    """
    global _requested_at
    if not STOP.is_set():
        _requested_at = time.monotonic()
        STOP.set()
    conn = _registered
    if conn is not None:
        with contextlib.suppress(sqlite3.ProgrammingError):
            conn.interrupt()


def check() -> None:
    """Raise ``StopRequested`` when a stop has begun, else return."""
    if STOP.is_set():
        raise StopRequested


def begin_step(name: str) -> None:
    """Record ``name`` as the startup step about to run, then ``check``.

    ``current_step`` returns it, for the log line of a stop during startup.
    """
    global _step
    _step = name
    check()


def current_step() -> str | None:
    """The last step ``begin_step`` recorded since ``register``, if any."""
    return _step


def stopped_by(exc: BaseException) -> bool:
    """Whether ``exc`` is a stop ending startup work.

    That is ``StopRequested``, or a statement interrupted while ``STOP`` is
    set (``sqlite3.OperationalError: interrupted``). An ``OperationalError``
    raised from Python code has no ``sqlite_errorcode``.
    """
    if isinstance(exc, StopRequested):
        return True
    return (
        isinstance(exc, sqlite3.OperationalError)
        and getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_INTERRUPT
        and STOP.is_set()
    )


async def wait_within_budget(
    waiting: Collection[asyncio.Future[Any]],
    *,
    still_running: Callable[[], Iterable[str]],
    interrupt_writer: Callable[[], None],
    log: logging.Logger,
) -> asyncio.CancelledError | None:
    """Wait until everything in ``waiting`` is done, within the stop budget.

    The wait is ``asyncio.wait``, never ``asyncio.wait_for``: it never
    cancels what it waits on, and a cancel of the caller costs one more
    iteration, never an abandoned wait. The first such cancel is returned
    once the wait is over, for the caller to re-raise or absorb.

    With no stop requested, the wait has no limit. Once ``request_stop``
    has run, the budget is ``STOP_BUDGET_S`` measured from that call. When
    it runs out: one ERROR names what is still running
    (``still_running``, logged on the caller's ``log``); ``interrupt_writer``
    ends the writer's running statement, so its transaction rolls back
    rather than being killed mid-commit; and the wait goes on for up to
    ``GRACE_S`` more. Then it returns, done or not; the caller checks.
    """
    cancelled: asyncio.CancelledError | None = None
    overran = False
    while True:
        pending = [fut for fut in waiting if not fut.done()]
        if not pending:
            return cancelled
        left = _seconds_left(GRACE_S if overran else 0.0)
        if left is not None and left <= 0:
            if overran:
                return cancelled
            overran = True
            log.error(
                "stop budget exceeded; still running: %s",
                ", ".join(still_running()),
            )
            interrupt_writer()
            continue
        try:
            await asyncio.wait(pending, timeout=left)
        except asyncio.CancelledError as exc:
            if cancelled is None:
                cancelled = exc  # remember the FIRST one, keep waiting


def reset_stop() -> None:
    """Forget the stop: clear ``STOP``, the budget start, the step, and the
    registered connection. Called at both ends of ``lifespan`` (see the
    module docstring)."""
    global _requested_at, _step
    conn = _registered
    if conn is not None:
        unregister(conn)
    STOP.clear()
    _requested_at = None
    _step = None


def _seconds_left(extra: float) -> float | None:
    """Seconds until the budget (plus ``extra``) runs out; ``None`` while no
    stop has been requested."""
    requested_at = _requested_at
    if requested_at is None or not STOP.is_set():
        return None
    return requested_at + STOP_BUDGET_S + extra - time.monotonic()


def _stop_progress() -> bool:
    return STOP.is_set()
