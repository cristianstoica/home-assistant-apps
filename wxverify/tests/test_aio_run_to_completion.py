"""Tests for ``run_to_completion``: the cancellation-safe executor helper.

Every scenario here drives the coroutine directly (never through the
database layer that consumes it), using a real background thread that is
observably still running -- via its own independent ``threading.Event``,
never a mocked clock or a mocked task -- so each assertion is a genuine
rendezvous rather than a timing guess. ``ThreadPoolExecutor`` reuses its
worker threads across calls, so a thread never "terminates" in a way
``Thread.is_alive()`` could observe; every "no thread left behind" check
below instead relies on the callable setting its own completion flag as
the last thing it does, from inside a ``finally``.
"""

from __future__ import annotations

import asyncio
import logging
import threading

import pytest

from wxverify.core.aio import run_to_completion


async def _settle() -> None:
    """Give the loop three ticks -- enough for a delivered cancellation to
    reach ``run_to_completion``'s except-block and re-establish a fresh
    ``asyncio.shield`` await within the same callback batch. No real time
    passes and the loop is single-threaded, so extra ticks are simply
    unused, never racy.
    """
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await asyncio.sleep(0)


@pytest.mark.parametrize("n_cancels", [1, 2, 3, 8])
def test_repeated_cancellation_never_releases_the_caller_before_the_thread_finishes(
    n_cancels: int,
) -> None:
    """A single cancel passes even against a broken implementation that only
    re-awaits the raw (unshielded) task once after the first
    ``CancelledError``: the discriminating case only shows up starting at
    the second cancel, when that second, unshielded await lets the
    cancellation straight through while the thread is still running.
    """

    async def _drive() -> None:
        entered = threading.Event()
        release = threading.Event()
        thread_finished = threading.Event()

        def _blocking() -> str:
            entered.set()
            try:
                if not release.wait(timeout=5.0):
                    raise TimeoutError("release was never set")
                return "done"
            finally:
                thread_finished.set()

        caller_task = asyncio.create_task(run_to_completion(_blocking))
        assert await asyncio.to_thread(entered.wait, 5.0), "blocking call never entered"

        for i in range(n_cancels):
            caller_task.cancel()
            await _settle()
            assert not caller_task.done(), (
                f"caller must still be parked after cancel {i + 1}/{n_cancels}"
            )
            assert not thread_finished.is_set(), (
                f"the thread must still be running after cancel {i + 1}/{n_cancels}"
            )

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await caller_task
        assert thread_finished.is_set(), (
            "the thread must finish before the caller is allowed to resume"
        )

    asyncio.run(_drive())


def test_cancellation_is_delivered_once_strictly_after_the_thread_finishes() -> None:
    """However many cancels arrive, the caller sees exactly one
    ``CancelledError``, and only once the thread's own completion is
    already on record -- never interleaved with it.
    """

    async def _drive() -> None:
        entered = threading.Event()
        release = threading.Event()
        order: list[str] = []

        def _blocking() -> str:
            entered.set()
            try:
                if not release.wait(timeout=5.0):
                    raise TimeoutError("release was never set")
                return "done"
            finally:
                order.append("thread-finished")

        caller_task = asyncio.create_task(run_to_completion(_blocking))
        assert await asyncio.to_thread(entered.wait, 5.0)

        caller_task.cancel()
        caller_task.cancel()
        caller_task.cancel()
        await _settle()
        assert not caller_task.done()

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await caller_task
        order.append("cancellation-delivered")

        assert order == ["thread-finished", "cancellation-delivered"], (
            f"expected the thread to finish strictly before the cancellation "
            f"reached the caller, got {order}"
        )

    asyncio.run(_drive())


def test_thread_exception_propagates_when_not_cancelled() -> None:
    async def _drive() -> None:
        def _boom() -> None:
            raise ValueError("synthetic thread failure")

        with pytest.raises(ValueError, match="synthetic thread failure"):
            await run_to_completion(_boom)

    asyncio.run(_drive())


def test_thread_exception_is_logged_on_the_cancelled_path(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The child's exception must be retrieved and logged even though the
    caller never sees it directly (a pending ``CancelledError`` takes
    precedence) -- this is the general form of the composed, realistic case
    below: any exception the thread raises while cancellation is pending,
    not only the specific unrecoverable-shaped one.

    Deliberately checked via the log record's content, not via an absent
    "exception was never retrieved" warning: ``asyncio.shield`` already
    retrieves an inner task's exception itself once its own outer future is
    cancelled, purely to suppress that warning -- so an empty GC-warning
    handler holds regardless of whether ``run_to_completion`` retrieves and
    logs it again, and would not actually discriminate a broken
    implementation that dropped this codebase's own log call.
    """

    async def _drive() -> None:
        entered = threading.Event()
        release = threading.Event()
        thread_finished = threading.Event()

        def _boom_after_release() -> None:
            entered.set()
            try:
                if not release.wait(timeout=5.0):
                    raise TimeoutError("release was never set")
                raise ValueError("synthetic thread failure during cancellation")
            finally:
                thread_finished.set()

        caller_task = asyncio.create_task(run_to_completion(_boom_after_release))
        assert await asyncio.to_thread(entered.wait, 5.0)

        caller_task.cancel()
        await _settle()
        assert not caller_task.done()

        with caplog.at_level(logging.ERROR, logger="wxverify.core.aio"):
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await caller_task
        assert thread_finished.is_set()

        matching = [
            r
            for r in caplog.records
            if r.exc_info is not None and isinstance(r.exc_info[1], ValueError)
        ]
        assert len(matching) == 1, (
            f"expected exactly one logged ValueError, got {caplog.records}"
        )
        assert matching[0].message == "executor call failed while cancelled"

    asyncio.run(_drive())


def test_an_unrecoverable_style_error_from_the_thread_is_logged_despite_cancellation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Composed case: a fatal, unrecoverable-shaped error raised by the
    thread while a cancellation is pending must still reach the log --
    the one path where silently losing the error would be worst, since
    there is no other record of what went wrong.
    """

    async def _drive() -> None:
        entered = threading.Event()
        release = threading.Event()
        thread_finished = threading.Event()

        def _unrecoverable_after_release() -> None:
            entered.set()
            try:
                if not release.wait(timeout=5.0):
                    raise TimeoutError("release was never set")
                raise RuntimeError(
                    "database unrecoverable after failed import; restore "
                    "the .bak in /data manually"
                )
            finally:
                thread_finished.set()

        caller_task = asyncio.create_task(
            run_to_completion(_unrecoverable_after_release)
        )
        assert await asyncio.to_thread(entered.wait, 5.0)

        caller_task.cancel()
        await _settle()
        assert not caller_task.done()

        with caplog.at_level(logging.ERROR, logger="wxverify.core.aio"):
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await caller_task
        assert thread_finished.is_set()

        matching = [
            r
            for r in caplog.records
            if r.exc_info is not None and isinstance(r.exc_info[1], RuntimeError)
        ]
        assert len(matching) == 1, (
            f"expected exactly one logged RuntimeError, got {caplog.records}"
        )
        assert "database unrecoverable" in str(matching[0].exc_info[1])
        assert matching[0].message == "executor call failed while cancelled"

    asyncio.run(_drive())
