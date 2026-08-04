"""Tests for the bounded read-connection pool's concurrency, fairness, and
connection-lifecycle guarantees.

The pool replaces a single shared read connection with a fixed number of
interchangeable connections handed out from an ``asyncio.Queue``. These
tests exist to catch three different ways that replacement could regress
back toward the old, effectively-serial behavior: connections not actually
being usable concurrently, waiters being served out of arrival order, and a
connection being reused (or lost) before the read that checked it out has
actually finished with it.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path

import pytest

from tests.helpers import assert_read_pool_at_rest
from wxverify.db.connection import _READ_POOL_SIZE, Database


def test_pool_serves_n_concurrent_reads_and_holds_back_the_extra_one(
    tmp_path: Path,
) -> None:
    """All ``_READ_POOL_SIZE`` reads must be able to run at the same time,
    and a read beyond that count must wait rather than run.

    Construction: every dispatched read blocks (via a ``threading.Event``)
    until released, and each records its own entry into a per-index event.
    Only once every one of the N events is observed set do we know all N
    are genuinely running concurrently -- a pool that actually serialized
    access (a single connection, or a lock instead of a queue) would leave
    at least one of these events unset, and the wait for it would time out.
    A single-connection implementation deadlocks this test outright, which
    is exactly the discrimination wanted: this is not something a narrower
    pool can pass by chance.
    """

    async def _drive() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            entered = [threading.Event() for _ in range(_READ_POOL_SIZE)]
            release = threading.Event()

            def _make_blocking(index: int):
                def _fn(conn: sqlite3.Connection) -> int:
                    entered[index].set()
                    assert release.wait(timeout=5.0), "release was never observed"
                    return index

                return _fn

            tasks = [
                asyncio.create_task(db.read(_make_blocking(i)))
                for i in range(_READ_POOL_SIZE)
            ]
            for i, event in enumerate(entered):
                assert await asyncio.to_thread(event.wait, 5.0), (
                    f"read {i} never entered -- the pool did not let all "
                    f"{_READ_POOL_SIZE} reads run concurrently"
                )

            # The pool is now fully checked out. One more read must queue
            # rather than run.
            extra_entered = threading.Event()

            def _extra(conn: sqlite3.Connection) -> str:
                extra_entered.set()
                return "extra"

            extra_task = asyncio.create_task(db.read(_extra))
            await asyncio.sleep(0.05)
            assert not extra_task.done()
            assert not extra_entered.is_set(), (
                "a read beyond the pool's size must not start while every "
                "connection is checked out"
            )

            release.set()
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)
            assert sorted(results) == list(range(_READ_POOL_SIZE))
            extra_result = await asyncio.wait_for(extra_task, timeout=5.0)
            assert extra_result == "extra"
            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_drive())


def test_pool_serves_queued_reads_in_arrival_order(tmp_path: Path) -> None:
    """Once the pool is saturated, additional reads must be served in the
    order they arrived.

    Construction: saturate the pool with ``_READ_POOL_SIZE`` blocked
    holders, then queue three more reads behind it in a known order.
    Releasing the holders one at a time and recording completion order
    catches a fairness regression: ``asyncio.Queue`` wakes its earliest
    waiter first, but a reimplementation built on, say, a LIFO stack of
    parked readers would serve "third" before "first" -- this test fails
    against that reordering while passing against the real FIFO queue.
    """

    async def _drive() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            holder_entered = [threading.Event() for _ in range(_READ_POOL_SIZE)]
            holder_release = [threading.Event() for _ in range(_READ_POOL_SIZE)]

            def _make_holder(index: int):
                def _fn(conn: sqlite3.Connection) -> str:
                    holder_entered[index].set()
                    assert holder_release[index].wait(timeout=5.0)
                    return f"holder-{index}"

                return _fn

            holder_tasks = [
                asyncio.create_task(db.read(_make_holder(i)))
                for i in range(_READ_POOL_SIZE)
            ]
            for event in holder_entered:
                assert await asyncio.to_thread(event.wait, 5.0)

            names = ["first", "second", "third"]

            def _make_queued(name: str):
                def _fn(conn: sqlite3.Connection) -> str:
                    return name

                return _fn

            queued_tasks = [
                asyncio.create_task(db.read(_make_queued(name))) for name in names
            ]
            # Every queued read's synchronous run-up (an already-open gate,
            # then an empty pool) suspends without yielding real work, so one
            # checkpoint is enough to let all three reach the pool's FIFO
            # waiter queue in creation order.
            await asyncio.sleep(0)
            for task in queued_tasks:
                assert not task.done()

            completion_order: list[str] = []

            async def _record(task: asyncio.Task[str]) -> None:
                completion_order.append(await asyncio.wait_for(task, timeout=5.0))

            recorders = [asyncio.create_task(_record(t)) for t in queued_tasks]

            # Release one holder at a time. FIFO fairness means each release
            # unblocks the earliest-arrived queued read next.
            for i in range(len(names)):
                holder_release[i].set()
                await asyncio.wait_for(recorders[i], timeout=5.0)

            assert completion_order == names

            for i in range(len(names), _READ_POOL_SIZE):
                holder_release[i].set()
            await asyncio.wait_for(asyncio.gather(*holder_tasks), timeout=5.0)
            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_drive())


def test_pool_connections_are_returned_after_a_raising_read(tmp_path: Path) -> None:
    """A connection whose read raised must still be returned to the pool.

    Construction: run ``_READ_POOL_SIZE`` reads that all raise, then run
    ``_READ_POOL_SIZE`` more that succeed. If the connection return were
    placed outside the read's own inner ``finally`` -- reached only on the
    success path -- the second batch would find the pool permanently short
    and hang waiting for connections that will never come back; wrapping
    that wait in a bounded ``asyncio.wait_for`` turns that hang into a
    reported failure instead of stalling the suite.
    """

    async def _drive() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:

            class _Boom(Exception):
                pass

            def _raise(conn: sqlite3.Connection) -> None:
                raise _Boom("synthetic read failure")

            raising_tasks = [
                asyncio.create_task(db.read(_raise)) for _ in range(_READ_POOL_SIZE)
            ]
            for task in raising_tasks:
                with pytest.raises(_Boom):
                    await asyncio.wait_for(task, timeout=5.0)

            def _ok(conn: sqlite3.Connection) -> str:
                return "ok"

            ok_tasks = [
                asyncio.create_task(db.read(_ok)) for _ in range(_READ_POOL_SIZE)
            ]
            results = await asyncio.wait_for(asyncio.gather(*ok_tasks), timeout=5.0)
            assert results == ["ok"] * _READ_POOL_SIZE
            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_drive())


def test_pool_connection_is_returned_after_a_cancelled_read(tmp_path: Path) -> None:
    """A connection checked out by a read that gets cancelled must still
    come back to the pool -- this is the leak that degrades silently,
    shrinking the effective pool size by one on every cancellation instead
    of failing loudly.

    "The pool still serves a full batch of reads" alone does not reliably
    catch this: a batch of fast, non-blocking reads can cycle through
    fewer connections than the pool claims to have without ever needing
    them all at once, so a single leaked connection can go unnoticed by
    that check. What actually catches it is the trailing at-rest count,
    which is why that assertion is not optional here.
    """

    async def _drive() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            entered = threading.Event()
            release = threading.Event()

            def _blocking(conn: sqlite3.Connection) -> str:
                entered.set()
                assert release.wait(timeout=5.0)
                return "done"

            task = asyncio.create_task(db.read(_blocking))
            assert await asyncio.to_thread(entered.wait, 5.0)

            task.cancel()
            # Deferred cancellation means this await cannot resolve until
            # the executor thread actually finishes, so the thread must be
            # released in this same synchronous block, before awaiting.
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task

            def _ok(conn: sqlite3.Connection) -> str:
                return "ok"

            ok_tasks = [
                asyncio.create_task(db.read(_ok)) for _ in range(_READ_POOL_SIZE)
            ]
            results = await asyncio.wait_for(asyncio.gather(*ok_tasks), timeout=5.0)
            assert results == ["ok"] * _READ_POOL_SIZE

            # The discriminating check: a leaked connection from the cancelled
            # read above leaves the pool one short here, even though the
            # batch of fast reads just above completed fine on the rest.
            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_drive())


def test_cancelled_read_connection_is_not_recycled_before_its_callback_returns(
    tmp_path: Path,
) -> None:
    """A cancelled read's connection must not reach the next waiting reader
    until the callback that was using it has actually returned.

    "The pool still serves N reads" does not discriminate here -- a bare
    ``finally: put_nowait(conn)`` placed around a non-deferred await would
    also eventually return the connection and let later reads proceed, just
    too early. Only recorded *order* catches that: this test instruments
    the cancelled callback to record its own exit, and the next reader to
    record its own start, then asserts the exit happened first.

    The releasing thread is held open on a deliberate delay (not released
    in the same synchronous block as ``cancel()``) so that a broken,
    non-deferred implementation has a wide, reliable window in which to
    recycle the connection early -- released immediately, a fast enough
    broken implementation could race its own bug to completion before the
    next reader even asks for a connection, passing this test for the
    wrong reason. A loaded machine only widens this margin, never narrows
    it, so the wait is not a flaky guess at how long "enough" is.
    """

    async def _drive() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            order: list[str] = []

            # Saturate every OTHER connection first, so exactly one
            # connection is in flight (the one the cancelled read is using)
            # and the next read has nowhere else to go -- it must wait for
            # that specific connection, which is what makes recorded order
            # meaningful rather than a coincidence of which free connection
            # it happened to grab.
            others_entered = [threading.Event() for _ in range(_READ_POOL_SIZE - 1)]
            others_release = threading.Event()

            def _make_other(index: int):
                def _fn(conn: sqlite3.Connection) -> str:
                    others_entered[index].set()
                    assert others_release.wait(timeout=5.0)
                    return f"other-{index}"

                return _fn

            other_tasks = [
                asyncio.create_task(db.read(_make_other(i)))
                for i in range(_READ_POOL_SIZE - 1)
            ]
            for event in others_entered:
                assert await asyncio.to_thread(event.wait, 5.0)

            slow_entered = threading.Event()
            slow_release = threading.Event()

            def _slow(conn: sqlite3.Connection) -> str:
                slow_entered.set()
                assert slow_release.wait(timeout=5.0)
                order.append("slow-callback-exit")
                return "slow"

            slow_task = asyncio.create_task(db.read(_slow))
            assert await asyncio.to_thread(slow_entered.wait, 5.0)

            async def _release_after_a_delay() -> None:
                await asyncio.sleep(0.05)
                slow_release.set()

            slow_task.cancel()
            delayed_release = asyncio.create_task(_release_after_a_delay())
            with pytest.raises(asyncio.CancelledError):
                await slow_task

            def _next(conn: sqlite3.Connection) -> str:
                order.append("next-reader-acquired")
                return "next"

            next_result = await asyncio.wait_for(db.read(_next), timeout=5.0)
            assert next_result == "next"
            await delayed_release  # already done; collects it cleanly

            assert order == ["slow-callback-exit", "next-reader-acquired"], (
                "the connection must not be handed to the next reader "
                f"before the cancelled callback actually returned, got {order}"
            )

            others_release.set()
            await asyncio.wait_for(asyncio.gather(*other_tasks), timeout=5.0)
            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_drive())
