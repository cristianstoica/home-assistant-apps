"""SQLite WAL connection facade: a single serialized writer and a bounded
pool of readers."""

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
from wxverify.core.aio import run_to_completion
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations

logger = logging.getLogger(__name__)

T = TypeVar("T")
_db_instance: Database | None = None

# Any single gate wait, pool wait, executor dispatch or query execution at
# or above this is worth a log line: /sites, the fastest page, is ~100 ms
# end to end INCLUDING transport, so 250 ms of DB work alone is already an
# outlier.
SLOW_READ_MS = 250.0

# SQLite in WAL mode lets any number of readers run concurrently with no
# reader blocking another, but this pool shares the interpreter's default
# executor with every other asyncio.to_thread() call (writes, transfer
# hops, the startup sweep) -- an oversized pool would just convert
# connection contention into thread-dispatch contention rather than
# removing it. Four covers the observed simultaneous-reader shape (the
# worker's own job loop, the page request being served, and the handful of
# chart JSON fetches a page kicks off) with one spare.
_READ_POOL_SIZE = 4


@dataclass
class ReadTiming:
    calls: int = 0
    errors: int = 0
    gate_ms: float = 0.0
    wait_ms: float = 0.0
    dispatch_ms: float = 0.0
    exec_ms: float = 0.0
    max_gate_ms: float = 0.0
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


class StaleGenerationError(Exception):
    """A fenced write's captured generation no longer matches the live database.

    Raised by ``Database.write_fenced`` when the database has been replaced
    (see ``replace_from``) since the caller captured ``Database.generation``.
    Whatever the caller read before is no longer known to describe the
    current database, so the write is rejected outright instead of risking
    it landing against unrelated data.
    """

    def __init__(self, requested: int, current: int) -> None:
        super().__init__(
            f"write rejected: requested generation {requested}, "
            f"database is now at generation {current}"
        )
        self.requested_generation = requested
        self.current_generation = current


class Database:
    def __init__(self, path: str) -> None:
        config.ensure_parent_dir(path)
        if sqlite3.sqlite_version_info < (3, 35, 0):
            raise RuntimeError("sqlite 3.35.0 or newer is required")
        self.path = path
        # The write lock, read gate, and read pool are created exactly once,
        # here, and are deliberately NOT part of _open(): replace_from()
        # holds the write lock and closes the gate across _open(), so
        # recreating any of them there would let a coroutine that starts
        # waiting during the swap window capture a different lock/event/queue
        # object than the one being held — two writers could then interleave
        # on the shared write connection, or a reader could queue on a gate
        # nobody is watching. _open() only ever rebuilds their CONTENTS (the
        # connections); publishing those into the pool is _stock_pool()'s job.
        self._write_lock = asyncio.Lock()
        self._read_gate = asyncio.Event()
        self._read_gate.set()
        self._read_pool: asyncio.Queue[sqlite3.Connection] = asyncio.Queue(
            maxsize=_READ_POOL_SIZE
        )
        # Bumped once per (re)open the swap path performs, including a
        # rollback-and-reopen of the untouched file: even that counts,
        # because a job that read before the swap window and is about to
        # write after it must be treated as spanning the swap, the same as
        # if content had actually changed underneath it.
        self._generation = 0
        # Written only from the event-loop thread (see `read`'s `finally`),
        # so it needs no lock of its own. Cumulative for the process lifetime
        # of this Database instance; never reset.
        self._read_stats: dict[str, ReadTiming] = {}
        self._read_stats_since = isoformat_utc()
        self._open()
        self._stock_pool()

    def _connect_reader(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        self._assert_reader_pragmas(conn)
        return conn

    def _open(self) -> None:
        """(Re)open the writer, sync reader, and pooled readers on
        ``self.path``. Locks, the gate, and the pool object are untouched.

        Creation only: callers MUST call ``_stock_pool()`` afterward to
        publish the new pooled connections. ``_open()`` deliberately does
        not do this itself -- see ``_stock_pool``'s own docstring for why.
        """
        self._conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._assert_pragmas(self._conn)
        self._run_immediate(run_migrations)
        self._read_conns = [self._connect_reader() for _ in range(_READ_POOL_SIZE)]
        self._read_sync_conn = self._connect_reader()

    def _stock_pool(self) -> None:
        """Publish ``self._read_conns`` into the pool queue.

        Callable ONLY when every pooled connection is accounted for outside
        the queue -- the two call sites are ``__init__`` (nothing has ever
        been handed out yet) and ``replace_from`` (the drain took all
        ``_READ_POOL_SIZE`` of them back before the swap ran). Emptying the
        queue first does not make this safe to call at an arbitrary moment:
        a connection a live reader currently holds is not IN the queue to be
        emptied, so calling this while one is checked out publishes it a
        second time.
        """
        while not self._read_pool.empty():
            self._read_pool.get_nowait()
        for conn in self._read_conns:
            self._read_pool.put_nowait(conn)

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
            # A nested zero-arg closure, not `self._run_immediate` passed
            # directly: `_run_immediate` is itself generic, and
            # run_to_completion's `Callable[..., T]` erases per-argument
            # types, so a bare method reference loses the binding between
            # its own T and this T. Closing over the already-bound `fn`
            # here resolves `_run_immediate`'s T against it first.
            def _call() -> T:
                return self._run_immediate(fn)

            return await run_to_completion(_call)

    @property
    def generation(self) -> int:
        """Generation counter, bumped by ``replace_from`` after each reopen.

        Safe to read from the event loop with no lock: it is only ever
        mutated while ``_write_lock`` is held (inside ``_replace_sync``), and
        ``write_fenced`` re-checks it again under that same lock at write
        time. An unlocked read here is just an optimistic snapshot for the
        caller's own bookkeeping -- the actual guarantee comes from the
        locked recheck, not from this read.
        """
        return self._generation

    async def write_fenced(
        self, fn: Callable[[sqlite3.Connection], T], *, generation: int
    ) -> T:
        """Like ``write``, but rejects a write submitted against a generation
        the database has since moved past.

        The comparison happens after acquiring the write lock, not before:
        a ``replace_from`` racing the caller between its generation read and
        this call is exactly what the lock boundary is meant to close.
        """
        async with self._write_lock:
            if generation != self._generation:
                raise StaleGenerationError(generation, self._generation)

            def _call() -> T:
                return self._run_immediate(fn)

            return await run_to_completion(_call)

    async def read(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        label = _read_label(fn)
        requested = time.perf_counter()
        # None means "this phase was never reached": a read cancelled before
        # the gate opens never sets `gated`; one cancelled while still queued
        # for a pooled connection never sets `acquired`; one cancelled or
        # failed inside the thread never sets `started`/`finished`.
        gated: float | None = None
        acquired: float | None = None
        started: float | None = None
        finished: float | None = None
        try:
            # The gate is open unless an import swap is draining or
            # rebuilding the pool; waiting on it here, before touching the
            # pool at all, is what lets replace_from() take back every
            # pooled connection without a new reader sneaking in ahead of it.
            await self._read_gate.wait()
            gated = time.perf_counter()
            conn = await self._read_pool.get()
            acquired = time.perf_counter()

            # `started` is captured INSIDE the worker thread so executor
            # dispatch delay is attributed separately from query cost —
            # under a saturated thread pool the two are otherwise
            # indistinguishable.
            def _timed(conn: sqlite3.Connection) -> tuple[float, float, T]:
                started_at = time.perf_counter()
                result = fn(conn)
                return started_at, time.perf_counter(), result

            try:
                # A thread cannot be interrupted from the outside.
                # run_to_completion holds this coroutine here until the
                # thread actually stops, however many cancels arrive, so the
                # connection is never returned to the pool while another
                # task is still running a query against it.
                started, finished, result = await run_to_completion(_timed, conn)
            finally:
                self._read_pool.put_nowait(conn)
            return result
        finally:
            # This runs on the event-loop thread, which is single-threaded,
            # so `_read_stats` needs no lock of its own.
            self._record_read(label, requested, gated, acquired, started, finished)

    def _record_read(
        self,
        label: str,
        requested: float,
        gated: float | None,
        acquired: float | None,
        started: float | None,
        finished: float | None,
    ) -> None:
        # A read cancelled before the gate opens never sets `gated`; one
        # cancelled while still queued for a pooled connection never sets
        # `acquired`. Either boundary is unknowable exactly when it's
        # missing, so `now` stands in for it, making `gate_ms`/`wait_ms` a
        # lower bound rather than missing entirely. `wait_ms` is 0 when the
        # gate itself was never passed: that phase never started.
        now = time.perf_counter()
        gate_end = gated if gated is not None else now
        gate_ms = (gate_end - requested) * 1000
        if gated is None:
            wait_ms = 0.0
        else:
            wait_end = acquired if acquired is not None else now
            wait_ms = (wait_end - gate_end) * 1000
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
        timing.gate_ms += gate_ms
        timing.max_gate_ms = max(timing.max_gate_ms, gate_ms)
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
                "db read failed or cancelled %s gate=%.0fms wait=%.0fms "
                "dispatch=%s exec=%s",
                label,
                gate_ms,
                wait_ms,
                "---" if dispatch_ms is None else f"{dispatch_ms:.0f}ms",
                "---" if exec_ms is None else f"{exec_ms:.0f}ms",
            )
        elif (
            gate_ms >= SLOW_READ_MS
            or wait_ms >= SLOW_READ_MS
            or (dispatch_ms or 0) >= SLOW_READ_MS
            or (exec_ms or 0) >= SLOW_READ_MS
        ):
            logger.warning(
                "slow db read %s gate=%.0fms wait=%.0fms dispatch=%.0fms exec=%.0fms",
                label,
                gate_ms,
                wait_ms,
                dispatch_ms or 0,
                exec_ms or 0,
            )
        else:
            logger.debug(
                "db read %s gate=%.0fms wait=%.0fms dispatch=%.0fms exec=%.0fms",
                label,
                gate_ms,
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

        ``gate_ms`` is time spent parked behind a closed import-swap gate,
        tracked separately so it is never confused with ``wait_ms``, which
        is now "every pooled connection was busy" rather than "queued
        behind the one reader" -- a genuine saturation signal with a pool of
        concurrent readers, rather than an artifact of whichever read
        happened to be running. Because reads now overlap, a label's summed
        ``exec_ms`` can exceed the wall-clock window it was measured over,
        by up to the pool size -- never divide it by elapsed time and call
        the result a share of wall time.
        """
        return {
            label: {
                "calls": timing.calls,
                "errors": timing.errors,
                "gate_ms": timing.gate_ms,
                "wait_ms": timing.wait_ms,
                "dispatch_ms": timing.dispatch_ms,
                "exec_ms": timing.exec_ms,
                "max_gate_ms": timing.max_gate_ms,
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
        return fn(self._read_sync_conn)

    def close(self) -> None:
        self._read_sync_conn.close()
        for conn in self._read_conns:
            conn.close()
        self._conn.close()

    async def replace_from(self, new_db: Path, backup: Path) -> None:
        """Replace the live DB file with ``new_db``, backing up the current DB.

        Lock order is fixed: write lock first. Closing the gate and draining
        the pool then quiesces every reader for the swap window; neither the
        lock, the gate, nor the pool object is ever recreated (see
        ``__init__``), so mutual exclusion across the swap holds by
        construction.
        """
        async with self._write_lock:
            self._read_gate.clear()
            drained: list[sqlite3.Connection] = []
            try:
                for _ in range(_READ_POOL_SIZE):
                    drained.append(await self._read_pool.get())
            except BaseException:
                # Cancelled mid-drain: return exactly what was taken, reopen
                # the gate, and let the caller's cancellation propagate. The
                # swap itself never started, so there is nothing else to
                # unwind.
                for conn in drained:
                    self._read_pool.put_nowait(conn)
                self._read_gate.set()
                raise

            # From here on, cancellation is deferred rather than honored:
            # the drain succeeded, so every pooled connection is now
            # unaccounted for until _stock_pool() runs. A cancellation that
            # unwound this coroutine before that happened would leave the
            # pool permanently short.
            try:
                await run_to_completion(self._replace_sync, new_db, backup)
            finally:
                self._stock_pool()
                self._read_gate.set()

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
        # -wal/-shm sidecars. Every open connection must close, not just the
        # writer: a live pooled or sync reader still holding the file is
        # what keeps a sidecar (or the file itself, on some platforms) from
        # being replaceable underneath it.
        self._read_sync_conn.close()
        for conn in self._read_conns:
            conn.close()
        self._conn.close()
        # 4. Atomic rename, same filesystem. Fail -> reopen the untouched
        # live file and re-raise.
        try:
            os.replace(new_db, self.path)
        except BaseException:
            try:
                self._open()
                self._generation += 1
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
            self._generation += 1
        except BaseException:
            # 7. Rollback: close any half-open connection (after a failed
            # _open(), some connections may already be closed — the
            # suppressed double-close is expected), restore the backup by
            # COPY (the backup must survive as the reversibility artifact),
            # reopen, and re-raise the original error.
            self._close_quietly()
            try:
                shutil.copy2(backup, self.path)
                self._unlink_sidecars()
                self._open()
                self._generation += 1
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
        for conn in (self._conn, self._read_sync_conn, *self._read_conns):
            with contextlib.suppress(Exception):
                conn.close()

    def _unlink_sidecars(self) -> None:
        for suffix in ("-wal", "-shm"):
            Path(f"{self.path}{suffix}").unlink(missing_ok=True)


class FencedWriter:
    """A write handle bound to one ``Database`` generation.

    Obtained once (via ``Database.generation``) at the point a caller's read
    is known to be current, then threaded through everything downstream that
    writes based on that read. Every write through this handle is rejected
    with ``StaleGenerationError`` if the database has since been replaced;
    reads are unaffected by generation and keep using ``Database.read``
    directly.
    """

    def __init__(self, db: Database, generation: int) -> None:
        self._db = db
        self.generation = generation

    async def write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return await self._db.write_fenced(fn, generation=self.generation)


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
