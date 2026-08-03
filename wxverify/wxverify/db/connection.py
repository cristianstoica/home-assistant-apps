"""SQLite WAL connection facade with a single serialized writer."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations

logger = logging.getLogger(__name__)

T = TypeVar("T")
_db_instance: Database | None = None

# Any single read-lock wait, executor dispatch or query execution at or above
# this is worth a log line: /sites, the fastest page, is ~100 ms end to end
# INCLUDING transport, so 250 ms of DB work alone is already an outlier.
SLOW_READ_MS = 250.0


@dataclass
class ReadTiming:
    calls: int = 0
    errors: int = 0
    wait_ms: float = 0.0
    dispatch_ms: float = 0.0
    exec_ms: float = 0.0
    max_wait_ms: float = 0.0
    max_dispatch_ms: float = 0.0
    max_exec_ms: float = 0.0


def _read_label(fn: object) -> str:
    label = getattr(fn, "__qualname__", None) or type(fn).__name__
    code = getattr(fn, "__code__", None)
    # Line number, not a counter: it separates every lambda pair that shares a
    # qualname (each collides on a different line) into one label per callback
    # DEFINITION rather than per call site, and takes the operator from a
    # WARNING straight to the defining line.
    return label if code is None else f"{label}:{code.co_firstlineno}"


class Database:
    def __init__(self, path: str) -> None:
        config.ensure_parent_dir(path)
        if sqlite3.sqlite_version_info < (3, 35, 0):
            raise RuntimeError("sqlite 3.35.0 or newer is required")
        self.path = path
        # The locks are created exactly once, here, and are deliberately NOT
        # part of _open(): replace_from() holds both locks across _open(), so
        # recreating them there would let a coroutine that starts waiting
        # during the swap window capture a different lock object than the one
        # being held — two writers could then interleave on the shared write
        # connection.
        self._write_lock = asyncio.Lock()
        self._read_lock = asyncio.Lock()
        # Written only from the event-loop thread (see `read`'s `finally`),
        # so it needs no lock of its own. Cumulative for the process lifetime
        # of this Database instance; never reset.
        self._read_stats: dict[str, ReadTiming] = {}
        self._read_stats_since = isoformat_utc()
        self._open()

    def _open(self) -> None:
        """(Re)open both connections on ``self.path``; locks are untouched."""
        self._conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._assert_pragmas(self._conn)
        self._run_immediate(run_migrations)
        self._read_conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._read_conn.row_factory = sqlite3.Row
        self._assert_reader_pragmas(self._read_conn)

    @staticmethod
    def _assert_pragmas(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        row = conn.execute("PRAGMA foreign_keys").fetchone()
        if row is None or int(row[0]) != 1:
            raise RuntimeError("foreign_keys not enabled")

    @staticmethod
    def _assert_reader_pragmas(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")

    def _run_immediate(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        logger.debug("db txn begin")
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            result = fn(self._conn)
        except BaseException:
            logger.debug("db txn rollback")
            self._conn.rollback()
            raise
        self._conn.commit()
        logger.debug("db txn commit")
        return result

    async def write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        async with self._write_lock:
            return await asyncio.to_thread(self._run_immediate, fn)

    async def read(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        label = _read_label(fn)
        requested = time.perf_counter()
        # None means "this phase was never reached": a read cancelled while
        # still queued for the lock knows only its wait; a read that raises
        # inside the thread knows its wait but not its exec time.
        acquired: float | None = None
        started: float | None = None
        finished: float | None = None
        try:
            async with self._read_lock:
                acquired = time.perf_counter()

                # `started` is captured INSIDE the worker thread so executor
                # dispatch delay is attributed separately from query cost —
                # under a saturated thread pool the two are otherwise
                # indistinguishable.
                def _timed(conn: sqlite3.Connection) -> tuple[float, float, T]:
                    started_at = time.perf_counter()
                    result = fn(conn)
                    return started_at, time.perf_counter(), result

                started, finished, result = await asyncio.to_thread(
                    _timed, self._read_conn
                )
            return result
        finally:
            # The `finally` wraps the `async with`, so on BOTH paths the lock
            # is already released when this runs: bookkeeping must never
            # extend the hold it is measuring. This runs on the event-loop
            # thread, which is single-threaded, so `_read_stats` needs no
            # lock of its own.
            self._record_read(label, requested, acquired, started, finished)

    def _record_read(
        self,
        label: str,
        requested: float,
        acquired: float | None,
        started: float | None,
        finished: float | None,
    ) -> None:
        # A read cancelled while still queued for the lock never sets
        # `acquired`, so its wait is unknowable exactly; `now` stands in for
        # it, making `wait_ms` a lower bound rather than missing entirely.
        wait_end = acquired if acquired is not None else time.perf_counter()
        wait_ms = (wait_end - requested) * 1000
        dispatch_ms = (
            None if acquired is None or started is None else (started - acquired) * 1000
        )
        exec_ms = (
            None if started is None or finished is None else (finished - started) * 1000
        )
        failed = finished is None

        timing = self._read_stats.setdefault(label, ReadTiming())
        timing.calls += 1
        if failed:
            timing.errors += 1
        timing.wait_ms += wait_ms
        timing.max_wait_ms = max(timing.max_wait_ms, wait_ms)
        if dispatch_ms is not None:
            timing.dispatch_ms += dispatch_ms
            timing.max_dispatch_ms = max(timing.max_dispatch_ms, dispatch_ms)
        if exec_ms is not None:
            timing.exec_ms += exec_ms
            timing.max_exec_ms = max(timing.max_exec_ms, exec_ms)

        if failed:
            logger.warning(
                "db read failed or cancelled %s wait=%.0fms dispatch=%s exec=%s",
                label,
                wait_ms,
                "---" if dispatch_ms is None else f"{dispatch_ms:.0f}ms",
                "---" if exec_ms is None else f"{exec_ms:.0f}ms",
            )
        elif (
            wait_ms >= SLOW_READ_MS
            or (dispatch_ms or 0) >= SLOW_READ_MS
            or (exec_ms or 0) >= SLOW_READ_MS
        ):
            logger.warning(
                "slow db read %s wait=%.0fms dispatch=%.0fms exec=%.0fms",
                label,
                wait_ms,
                dispatch_ms or 0,
                exec_ms or 0,
            )
        else:
            logger.debug(
                "db read %s wait=%.0fms dispatch=%.0fms exec=%.0fms",
                label,
                wait_ms,
                dispatch_ms or 0,
                exec_ms or 0,
            )

    def read_timing_snapshot(self) -> dict[str, dict[str, float]]:
        """Cumulative per-label read timing, since ``self.read_timing_since``.

        Counters never reset for the life of this ``Database`` instance —
        there is no reset endpoint. ``calls`` counts attempts, not successes;
        ``errors`` counts the attempts that never produced an ``exec_ms``
        (raised or were cancelled). A label whose ``errors`` tracks its
        ``calls`` is a read that is failing or being cancelled, not one that
        is slow.
        """
        return {
            label: {
                "calls": timing.calls,
                "errors": timing.errors,
                "wait_ms": timing.wait_ms,
                "dispatch_ms": timing.dispatch_ms,
                "exec_ms": timing.exec_ms,
                "max_wait_ms": timing.max_wait_ms,
                "max_dispatch_ms": timing.max_dispatch_ms,
                "max_exec_ms": timing.max_exec_ms,
            }
            for label, timing in self._read_stats.items()
        }

    @property
    def read_timing_since(self) -> str:
        return self._read_stats_since

    def write_sync(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return self._run_immediate(fn)

    def read_sync(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return fn(self._read_conn)

    def close(self) -> None:
        self._read_conn.close()
        self._conn.close()

    async def replace_from(self, new_db: Path, backup: Path) -> None:
        """Replace the live DB file with ``new_db``, backing up the current DB.

        Lock order is fixed: write lock, then read lock. No other code path
        acquires both, so no deadlock ordering exists to violate. Holding
        both locks quiesces every DB access for the swap window; the locks
        themselves are never recreated (see ``__init__``), so mutual
        exclusion across the swap holds by construction.
        """
        async with self._write_lock, self._read_lock:
            await asyncio.to_thread(self._replace_sync, new_db, backup)

    def _replace_sync(self, new_db: Path, backup: Path) -> None:
        # 1. Flush the WAL into the main file. Fail -> raise; nothing changed.
        self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        # 2. Consistent, self-contained backup of the CURRENT DB. Direct
        # execute on the autocommit write conn, NOT via _run_immediate:
        # VACUUM INTO cannot run inside a transaction. The exists pre-check
        # keeps the unlink-on-failure below from deleting a COMPLETE prior
        # backup when a same-second import collides on the timestamped name
        # (VACUUM INTO refuses an existing output file).
        if backup.exists():
            raise FileExistsError(f"backup target already exists: {backup}")
        try:
            self._conn.execute("VACUUM INTO ?", (str(backup),))
        except BaseException:
            backup.unlink(missing_ok=True)
            raise
        # 3. On the last close after a checkpoint, SQLite itself removes the
        # -wal/-shm sidecars.
        self._read_conn.close()
        self._conn.close()
        # 4. Atomic rename, same filesystem. Fail -> reopen the untouched
        # live file and re-raise.
        try:
            os.replace(new_db, self.path)
        except BaseException:
            try:
                self._open()
            except BaseException:
                self._close_quietly()
                raise
            raise
        # 5. WAL-sidecar rule: a stale sidecar beside a new main file
        # corrupts it. Step 3 normally removes them already; this covers a
        # leftover from a previously crashed process.
        self._unlink_sidecars()
        # 6. Reopen on the new file; run_migrations upgrades an
        # older-user_version import here.
        try:
            self._open()
        except BaseException:
            # 7. Rollback: close any half-open connection (after a failed
            # _open(), _read_conn may already be closed — the suppressed
            # double-close is expected), restore the backup by COPY (the
            # backup must survive as the reversibility artifact), reopen,
            # and re-raise the original error.
            self._close_quietly()
            try:
                shutil.copy2(backup, self.path)
                self._unlink_sidecars()
                self._open()
            except BaseException as restore_exc:
                logger.critical(
                    "database unrecoverable after failed import; "
                    "restore the .bak in /data manually"
                )
                raise RuntimeError(
                    "database unrecoverable after failed import; "
                    "restore the .bak in /data manually"
                ) from restore_exc
            raise

    def _close_quietly(self) -> None:
        for conn in (self._conn, self._read_conn):
            with contextlib.suppress(Exception):
                conn.close()

    def _unlink_sidecars(self) -> None:
        for suffix in ("-wal", "-shm"):
            Path(f"{self.path}{suffix}").unlink(missing_ok=True)


def init_db(path: str | None = None) -> Database:
    global _db_instance
    if _db_instance is not None:
        _db_instance.close()
    _db_instance = Database(path or config.db_path)
    return _db_instance


def get_db() -> Database:
    if _db_instance is None:
        return init_db(config.db_path)
    return _db_instance


def close_db() -> None:
    global _db_instance
    if _db_instance is not None:
        _db_instance.close()
        _db_instance = None
