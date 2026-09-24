"""Tests for the 0.16.3 write-lock timing instrumentation (D1.1, connection.py).

Covers the `slow db write` INFO line `Database.write` and `Database.write_fenced`
emit via `_log_slow_write`: `hold` is measured across the whole locked section
-- including the exception path, because the stop time is taken in a
`try/finally` INSIDE `async with self._write_lock` -- `lock_wait` is measured
from before lock acquisition, and a write that keeps both under the threshold
logs nothing at all.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from wxverify.db.connection import Database

_SLOW_WRITE_LOG_RE = re.compile(r"^slow db write \S+ lock_wait=(\d+) hold=(\d+)$")


def _slow_write(conn: sqlite3.Connection) -> None:
    time.sleep(1.1)


def _fast_write(conn: sqlite3.Connection) -> None:
    pass


def _quick_write(conn: sqlite3.Connection) -> None:
    time.sleep(0.01)


def _slow_raising_write(conn: sqlite3.Connection) -> None:
    time.sleep(1.1)
    raise RuntimeError("synthetic write failure")


def _blocking_write(
    release_event: threading.Event,
) -> Callable[[sqlite3.Connection], None]:
    """A write `fn` that blocks in its executor thread until released.

    Unlike `_slow_write`'s fixed `time.sleep(1.1)`, this lets a test confirm
    the waiter has actually queued behind the held lock BEFORE it starts
    measuring out the threshold interval -- so the interval provably covers
    the waiter's whole wait instead of racing a fixed sleep against however
    long the waiter's own poll loop and task-start take to run.
    """

    def _fn(conn: sqlite3.Connection) -> None:
        release_event.wait()

    return _fn


def _slow_write_records(
    caplog: pytest.LogCaptureFixture,
) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == "wxverify.db.connection"
        and record.getMessage().startswith("slow db write")
    ]


def test_slow_write_logs_hold_at_or_above_threshold(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A `write` whose `fn` sleeps 1.1s in the thread logs `slow db write`
    with `hold=` >= 1000 (plan 14.3 bullet 1)."""

    async def _drive() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            with caplog.at_level(logging.INFO, logger="wxverify.db.connection"):
                await db.write(_slow_write)
        finally:
            db.close()

    asyncio.run(_drive())
    records = _slow_write_records(caplog)
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    match = _SLOW_WRITE_LOG_RE.match(records[0].getMessage())
    assert match is not None
    hold_ms = int(match.group(2))
    assert hold_ms >= 1000


def test_waiter_behind_a_slow_write_logs_lock_wait_at_or_above_threshold(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A waiter queued behind a held write lock logs `lock_wait=` >= 1000
    once it finally acquires (plan 14.3 bullet 2).

    The holder blocks on a `threading.Event` rather than a fixed sleep, so
    the test can PROVE the waiter has genuinely queued behind the held lock
    before it starts measuring out the >=1000ms interval -- only then does
    it hold the holder for >=1.1s and release it, so the waiter's lock_wait
    provably covers the whole interval instead of racing a fixed sleep
    against whatever the waiter's own poll loop and task-start happen to
    cost on this machine.
    """
    release_event = threading.Event()

    async def _drive() -> list[logging.LogRecord]:
        db = Database(str(tmp_path / "db.sqlite"))
        holder: asyncio.Task[None] | None = None
        waiter: asyncio.Task[None] | None = None
        try:
            with caplog.at_level(logging.INFO, logger="wxverify.db.connection"):
                holder = asyncio.create_task(db.write(_blocking_write(release_event)))
                deadline = time.monotonic() + 2.0
                while not db._write_lock.locked():  # noqa: SLF001 - seam under test
                    if time.monotonic() > deadline:
                        raise TimeoutError("write lock was never observed held")
                    await asyncio.sleep(0.01)

                waiter = asyncio.create_task(db.write(_fast_write))
                await asyncio.sleep(0)
                assert not waiter.done(), (
                    "the waiter must genuinely queue behind the held lock, "
                    "or its lock_wait proves nothing"
                )

                # Only now, with the waiter provably queued, hold the lock
                # for a real >=1.1s before releasing it -- this is what
                # makes the waiter's eventual lock_wait deterministic,
                # rather than the fixed 1.1s sleep the holder used to run
                # unsupervised from the moment it acquired the lock.
                await asyncio.sleep(1.1)
                release_event.set()

                await holder
                await waiter
            return _slow_write_records(caplog)
        finally:
            # Release the holder first so it (and then the waiter) can
            # actually finish -- otherwise a failed assertion above would
            # leave both tasks parked on the lock and close() would run
            # out from under them, masking the real failure with an
            # unrelated one.
            release_event.set()
            for task in (holder, waiter):
                if task is not None and not task.done():
                    # cleanup only -- never masks the original failure
                    with contextlib.suppress(Exception):
                        await task
            db.close()

    records = asyncio.run(_drive())
    assert len(records) == 2

    # The holder's own finally runs (and logs) while it still holds the
    # lock, before release() hands it to the waiter -- so the holder's line
    # is emitted strictly before the waiter's.
    holder_match = _SLOW_WRITE_LOG_RE.match(records[0].getMessage())
    waiter_match = _SLOW_WRITE_LOG_RE.match(records[1].getMessage())
    assert holder_match is not None
    assert waiter_match is not None
    assert int(holder_match.group(2)) >= 1000  # holder's own hold
    assert int(waiter_match.group(1)) >= 1000  # waiter's lock_wait


def test_log_slow_write_logs_at_exactly_the_threshold_on_either_side(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """`_log_slow_write` fires with `hold` or `lock_wait` at EXACTLY
    `SLOW_WRITE_MS`, not only strictly above it -- pinning the `>=` in both
    halves of the condition directly, rather than only ever seeing them
    crossed by a wide margin the way the sleep-driven tests above do.

    Kills `>=` -> `>` on either comparison: the first two cases below land
    exactly on 1000.0ms and must still log under `>=`, but would not log
    under `>`.
    """
    from wxverify.db.connection import (
        _log_slow_write,  # noqa: SLF001 -- seam under test
    )

    # These particular float literals must land on EXACTLY 1000.0ms before
    # they can pin a boundary -- float subtraction is not otherwise
    # guaranteed to.
    assert (1.0 - 0.0) * 1000 == 1000.0
    assert (0.999 - 0.0) * 1000 < 1000.0

    with caplog.at_level(logging.INFO, logger="wxverify.db.connection"):
        # hold == 1000.0 exactly, lock_wait == 0 -- must log.
        _log_slow_write(_fast_write, 0.0, 0.0, 1.0)
    assert len(_slow_write_records(caplog)) == 1
    caplog.clear()

    with caplog.at_level(logging.INFO, logger="wxverify.db.connection"):
        # lock_wait == 1000.0 exactly, hold == 0 -- must log.
        _log_slow_write(_fast_write, 0.0, 1.0, 1.0)
    assert len(_slow_write_records(caplog)) == 1
    caplog.clear()

    with caplog.at_level(logging.INFO, logger="wxverify.db.connection"):
        # Both just under the threshold -- must not log.
        _log_slow_write(_fast_write, 0.0, 0.0, 0.999)
    assert _slow_write_records(caplog) == []


def test_quick_write_under_threshold_logs_nothing(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A 10ms write logs nothing (plan 14.3 bullet 3) -- the paired positive
    to the two slow-write cases above: this is the same helper and the same
    threshold gate, just under it, so the gate is genuinely exercised on
    both sides rather than only ever seen firing."""

    async def _drive() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            with caplog.at_level(logging.INFO, logger="wxverify.db.connection"):
                await db.write(_quick_write)
        finally:
            db.close()

    asyncio.run(_drive())
    assert _slow_write_records(caplog) == []


def test_raising_write_still_logs_hold(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """An `fn` that raises still logs `hold` (plan 14.3 bullet 4): the stop
    time is taken in a `try/finally` inside the lock, so the exception path
    is measured too."""

    async def _drive() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            with (
                caplog.at_level(logging.INFO, logger="wxverify.db.connection"),
                pytest.raises(RuntimeError, match="synthetic write failure"),
            ):
                await db.write(_slow_raising_write)
        finally:
            db.close()

    asyncio.run(_drive())
    records = _slow_write_records(caplog)
    assert len(records) == 1
    match = _SLOW_WRITE_LOG_RE.match(records[0].getMessage())
    assert match is not None
    assert int(match.group(2)) >= 1000


def test_raising_write_fenced_still_logs_hold(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Same as above through `write_fenced`, `FencedWriter`'s delegate and
    the other of the two call sites the plan names."""

    async def _drive() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            with (
                caplog.at_level(logging.INFO, logger="wxverify.db.connection"),
                pytest.raises(RuntimeError, match="synthetic write failure"),
            ):
                await db.write_fenced(_slow_raising_write, generation=db.generation)
        finally:
            db.close()

    asyncio.run(_drive())
    records = _slow_write_records(caplog)
    assert len(records) == 1
    match = _SLOW_WRITE_LOG_RE.match(records[0].getMessage())
    assert match is not None
    assert int(match.group(2)) >= 1000
