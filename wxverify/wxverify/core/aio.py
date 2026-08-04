"""Cancellation-safe helper for running a blocking call in the executor.

Every executor hop in this codebase that holds a lock or owns a shared
resource (a SQLite connection, a file being renamed underneath it) needs
the same guarantee: the calling coroutine must never resume past that hop
while the underlying thread is still running, no matter how many times it
is cancelled. A hand-rolled ``await asyncio.to_thread(...)`` does not give
that guarantee -- cancelling the await returns control to the caller
immediately while the thread keeps running invisibly in the background,
which is how a released lock or a recycled connection ends up shared with
a query that is still in flight.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)


async def run_to_completion[T](fn: Callable[..., T], /, *args: object) -> T:
    """Run ``fn`` in the default executor; the thread ALWAYS runs to completion.

    Cancellation is deferred, never dropped: a ``CancelledError`` delivered
    while the thread is alive is remembered and re-raised once the thread
    has actually finished. Repeated cancellation is survivable -- the loop
    below is the entire reason this exists instead of a plain
    ``await asyncio.to_thread(fn, *args)``.
    """
    task = asyncio.create_task(asyncio.to_thread(fn, *args))
    cancelled: asyncio.CancelledError | None = None
    # A single `await asyncio.wait([task])` looks equivalent and is NOT:
    # that await is itself cancellable, so a second cancel() arriving while
    # it is in flight propagates straight through, past any shield, and
    # the caller's `finally` then runs over a thread that is still
    # executing. The loop re-establishes the shield after every delivered
    # cancellation, so N cancels cost N iterations and the guarantee holds
    # under all of them, not just the first.
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if cancelled is None:
                cancelled = exc  # remember the FIRST one, keep waiting
        except BaseException:
            # The child raised. task.done() is now True, so the loop ends
            # and task.result() below re-raises whatever it was.
            pass
    if cancelled is not None:
        # Retrieve the child's outcome even though it is about to be
        # discarded: an exception nobody ever reads raises a "Task
        # exception was never retrieved" warning at GC time, attached to
        # nothing actionable. Logging it here, on the cancelled path, is
        # what keeps a failure inside the thread from going unseen.
        #
        # This call cannot itself raise CancelledError: the task above is
        # never cancelled directly (nothing here ever calls task.cancel()),
        # only awaited under shield, so it can only be done because `fn`
        # returned or raised.
        child = task.exception()
        if child is not None:
            logger.error("executor call failed while cancelled", exc_info=child)
        # Deferred, not swallowed: the first cancellation is re-raised to
        # the caller now that the thread is confirmed finished. Do NOT
        # call task.uncancel() here -- the cancel count must keep
        # reflecting that a cancellation was requested.
        raise cancelled
    return task.result()
