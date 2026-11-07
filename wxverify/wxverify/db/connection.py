"""SQLite WAL connection facade: a single serialized writer and a bounded
pool of readers."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, TypeVar

from wxverify import config
from wxverify.core import stop
from wxverify.core.aio import run_to_completion
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.compaction import compact_storage
from wxverify.db.migrations import run_migrations
from wxverify.db.sanitize import sanitize_wedge_prone_timestamps
from wxverify.db.snapshot import read_only_snapshot

logger = logging.getLogger(__name__)

T = TypeVar("T")
_db_instance: Database | None = None

# Any single pool wait, executor dispatch or query execution at
# or above this is worth a log line: /sites, the fastest page, is ~100 ms
# end to end INCLUDING transport, so 250 ms of DB work alone is already an
# outlier.
SLOW_READ_MS = 250.0

# A write whose wait for the write lock, or whose hold of it, reaches this
# gets an INFO line. INFO rather than the slow-read WARNING: a slow write is
# diagnostic, not a fault. The two halves are checked separately because
# they have different causes -- `lock_wait` is time queued behind other
# writers, `hold` is this write's own transaction. With INFO enabled, a log
# with no such line means every `write` that acquired the lock and reached
# its timing `finally` kept both under a second.
SLOW_WRITE_MS = 1000.0

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


def _log_slow_write(
    fn: object, requested: float, acquired: float, released: float
) -> None:
    """Log a write whose lock wait or lock hold reached ``SLOW_WRITE_MS``.

    ``requested`` is taken just before ``async with self._write_lock``,
    ``acquired`` as the first statement inside it, and ``released`` in a
    ``finally`` inside it, so a write that raises or is cancelled while
    holding the lock is measured too. A write cancelled while still queued
    for the lock never holds it and logs nothing. There is deliberately no
    per-write DEBUG line, for volume.
    """
    lock_wait_ms = (acquired - requested) * 1000
    hold_ms = (released - acquired) * 1000
    if lock_wait_ms >= SLOW_WRITE_MS or hold_ms >= SLOW_WRITE_MS:
        logger.info(
            "slow db write %s lock_wait=%.0f hold=%.0f",
            _read_label(fn),
            lock_wait_ms,
            hold_ms,
        )


# The journal modes the writer can run in without WAL and still keep the
# database file safe: slower (readers and the writer block each other), so
# _check_journal_mode logs them and lets the open go on. Every other mode
# (off, memory, or one nobody listed) is refused.
_ROLLBACK_JOURNAL_MODES: Final = frozenset({"delete", "truncate", "persist"})


def _check_journal_mode(mode: str | None) -> None:
    if mode == "wal":
        return
    if mode in _ROLLBACK_JOURNAL_MODES:
        logger.error(
            "database journal_mode is %s, not wal: readers and the writer"
            " block each other and pages slow down; the add-on keeps running",
            mode,
        )
        return
    logger.error(
        "database journal_mode is %s, not wal: unsafe for the database file;"
        " the open fails",
        mode,
    )
    raise RuntimeError(f"journal_mode {mode} is unsafe; refusing to open")


class Database:
    def __init__(self, path: str) -> None:
        config.ensure_parent_dir(path)
        if sqlite3.sqlite_version_info < (3, 35, 0):
            raise RuntimeError("sqlite 3.35.0 or newer is required")
        self.path = path
        # The write lock and the read pool are created here; _open() creates
        # the connections, and publishing those into the pool is
        # _stock_pool()'s job.
        self._write_lock = asyncio.Lock()
        self._read_pool: asyncio.Queue[sqlite3.Connection] = asyncio.Queue(
            maxsize=_READ_POOL_SIZE
        )
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
        """Open the writer, sync reader and pooled readers on ``self.path``.
        The write lock and the pool object are ``__init__``'s.

        Creation only: ``__init__`` calls ``_stock_pool()`` afterward to
        publish the pooled connections.
        """
        self._conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        # A refused open (journal mode, foreign_keys) holds no connection.
        try:
            self._assert_pragmas(self._conn)
        except BaseException:
            self._conn.close()
            raise
        # Registered while the migrations run, so a stop interrupts them
        # (storage-release plan 6.2); unregistered before anything else.
        stop.register(self._conn)
        try:
            try:
                self._run_immediate(run_migrations)
            finally:
                stop.unregister(self._conn)
        except Exception as exc:
            if not stop.stopped_by(exc):
                raise
            # The step the stop landed in has rolled back (its savepoint, or
            # _run_immediate's rollback). user_version is unchanged and every
            # step converges on re-entry, so the next start reruns from it.
            self._conn.close()
            logger.info(
                "stop during startup step %s; rolled back, reruns at the next start",
                stop.current_step() or "open",
            )
            raise
        # Runs on every open, not just first boot, so a database swapped in
        # by hand outside the app is caught here too. Cheap and idempotent
        # when there is nothing to fix.
        #
        # Best-effort: this is a repair, not a requirement to open the
        # database. Its worst-case failure must degrade to the pre-repair
        # status quo (a row that may still wedge, which monitor's overdue-job
        # scan now surfaces to the operator) rather than to an unbootable
        # process -- a sanitize bug must never be worse than not sanitizing.
        try:
            self._run_immediate(sanitize_wedge_prone_timestamps)
        except Exception:
            logger.exception("sanitize_wedge_prone_timestamps failed; continuing open")
            # Clears a transaction left open on this connection during
            # initialization. _run_immediate rolls back on every failure, a
            # failed COMMIT included, before it logs anything; what it cannot
            # clear is a rollback that itself raises (logged there; the
            # original error is what reaches here). This is the only caller
            # that swallows the error and keeps using the connection, so it
            # rolls back here; if that raises, _open fails instead of booting
            # a writer that holds the write lock and cannot start another
            # transaction.
            if self._conn.in_transaction:
                self._conn.rollback()
        # The one-time compaction (storage-release plan 4.2), before any
        # reader opens. Registered like the migrations, so a stop interrupts
        # it; every other failure is logged inside and the open carries on.
        stop.register(self._conn)
        try:
            try:
                compact_storage(self._conn, self.path)
            finally:
                stop.unregister(self._conn)
        except Exception as exc:
            if not stop.stopped_by(exc):
                raise
            # No marker was written and an interrupted VACUUM leaves the file
            # as it was, so the next start runs the compaction again.
            self._conn.close()
            logger.info(
                "stop during startup step %s; rolled back, reruns at the next start",
                stop.current_step() or "open",
            )
            raise
        self._read_conns = [self._connect_reader() for _ in range(_READ_POOL_SIZE)]
        self._read_sync_conn = self._connect_reader()

    def _stock_pool(self) -> None:
        """Publish ``self._read_conns`` into the pool queue.

        Callable ONLY when every pooled connection is accounted for outside
        the queue -- the one call site is ``__init__``, where nothing has
        ever been handed out yet. Emptying the queue first does not make this
        safe to call at an arbitrary moment: a connection a live reader
        currently holds is not IN the queue to be emptied, so calling this
        while one is checked out publishes it a second time.
        """
        while not self._read_pool.empty():
            self._read_pool.get_nowait()
        for conn in self._read_conns:
            self._read_pool.put_nowait(conn)

    @staticmethod
    def _assert_pragmas(conn: sqlite3.Connection) -> None:
        # The mode SQLite returns, not the one asked for: it can quietly
        # keep a rollback journal (no shared memory, for one).
        row = conn.execute("PRAGMA journal_mode=WAL").fetchone()
        mode = None if row is None else str(row[0]).lower()
        _check_journal_mode(mode)
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
            # Inside the try: a COMMIT that fails and leaves the transaction
            # open (e.g. a deferred foreign-key violation) must roll back like
            # any other failure, or the writer stays wedged inside it.
            self._conn.commit()
        except BaseException:
            # Roll back before logging anything. Callers branch on the
            # ORIGINAL error's type (IntegrityError, a stop's interrupt), so
            # neither a failed rollback nor a raising log call may replace it.
            try:
                self._conn.rollback()
            except Exception:
                with contextlib.suppress(Exception):
                    logger.exception(
                        "db txn rollback failed; re-raising the original error"
                    )
            else:
                with contextlib.suppress(Exception):
                    logger.debug("db txn rollback")
            raise
        logger.debug("db txn commit")
        return result

    async def write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        requested = time.perf_counter()
        async with self._write_lock:
            acquired = time.perf_counter()
            try:
                # A nested zero-arg closure, not `self._run_immediate` passed
                # directly: `_run_immediate` is itself generic, and
                # run_to_completion's `Callable[..., T]` erases per-argument
                # types, so a bare method reference loses the binding between
                # its own T and this T. Closing over the already-bound `fn`
                # here resolves `_run_immediate`'s T against it first.
                def _call() -> T:
                    return self._run_immediate(fn)

                return await run_to_completion(_call)
            finally:
                # The stop time is taken here, inside the lock, so the raise
                # and cancellation paths are measured too.
                _log_slow_write(fn, requested, acquired, time.perf_counter())

    async def read(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        label = _read_label(fn)
        requested = time.perf_counter()
        # None means "this phase was never reached": a read cancelled while
        # still queued for a pooled connection never sets `acquired`; one
        # cancelled or failed inside the thread never sets
        # `started`/`finished`.
        acquired: float | None = None
        started: float | None = None
        finished: float | None = None
        try:
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
                self._read_pool.put_nowait(self._settle_reader(conn))
            return result
        finally:
            # This runs on the event-loop thread, which is single-threaded,
            # so `_read_stats` needs no lock of its own.
            self._record_read(label, requested, acquired, started, finished)

    def _settle_reader(self, conn: sqlite3.Connection) -> sqlite3.Connection:
        """The connection this read is returning, guaranteed snapshot-free.

        NEVER raises and always returns exactly one connection, so the pool
        holds exactly ``_READ_POOL_SIZE`` at rest on every path. Fewer would
        make ``close_if_idle`` read "a reader is checked out" for the rest of
        the process lifetime and skip the graceful close forever; a
        duplicate would hand one handle to two readers.

        Runs on the event-loop thread, after ``run_to_completion`` (see
        ``wxverify.core.aio``) has confirmed the executor thread stopped, so
        no thread can be mid-statement on this connection -- and nothing
        else that walks ``_read_conns`` can interleave with the swap below:
        ``close``/``close_if_idle`` run on this same thread and the latter
        refuses while this reader is out.

        Failure policy, in order. A connection that will not roll back is
        closed, replaced by a fresh reader from ``_connect_reader`` at the
        same ``_read_conns`` index, and the fresh one is returned -- the
        queue and ``_read_conns`` keep naming the same objects. If the
        reconnect itself fails, the CLOSED handle goes back: every later
        read that draws it raises ``ProgrammingError`` -- loud, roughly one
        draw in ``_READ_POOL_SIZE`` under sequential load, until the add-on
        is restarted -- and never a stale snapshot. A ``close()`` that fails
        is logged at ERROR and the reconnect proceeds anyway; a real reader
        cannot reach that arm (``check_same_thread=False``, autocommit).
        """
        try:
            if not conn.in_transaction:
                return conn
        except sqlite3.ProgrammingError:
            # Closed: by the reconnect-failure arm below on an earlier
            # return (the reachable case), or underneath us (no server path
            # does that). The read that drew it has already failed loudly.
            # It goes back so the pool keeps its size, and the reconnect is
            # NOT retried here -- the ERROR line that arm logged says restart.
            return conn
        logger.warning("reader returned mid-transaction; rolling back")
        try:
            conn.rollback()
        except Exception:
            logger.exception("reader rollback failed")
        else:
            return conn
        # Closing is the only remaining way to drop the read mark. A
        # connection that cannot roll back would otherwise serve its stale
        # snapshot to every later reader, silently -- the worst outcome on
        # this path. Close, then replace: the pool must not shrink (see the
        # docstring) and must not keep handing out a dead handle.
        try:
            conn.close()
        except Exception:
            logger.exception("reader close failed after a failed rollback")
        else:
            logger.error("reader closed after a failed rollback")
        try:
            fresh = self._connect_reader()
        except Exception:
            # The closed handle stays in the pool AND in _read_conns, so the
            # pool keeps its size and close() still walks a real object.
            logger.exception(
                "reader reconnect failed; the unusable handle stays pooled "
                "and every read that draws it fails until restart"
            )
            return conn
        for i, pooled in enumerate(self._read_conns):
            if pooled is conn:
                self._read_conns[i] = fresh
                break
        logger.error("reader replaced after a failed rollback")
        return fresh

    def _record_read(
        self,
        label: str,
        requested: float,
        acquired: float | None,
        started: float | None,
        finished: float | None,
    ) -> None:
        # A read cancelled while still queued for a pooled connection never
        # sets `acquired`. That boundary is unknowable exactly when it's
        # missing, so `now` stands in for it, making `wait_ms` a lower bound
        # rather than missing entirely.
        now = time.perf_counter()
        wait_end = acquired if acquired is not None else now
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

        ``wait_ms`` is "every pooled connection was busy" rather than
        "queued behind the one reader" -- a genuine saturation signal with a
        pool of concurrent readers, rather than an artifact of whichever read
        happened to be running. Because reads overlap, a label's summed
        ``exec_ms`` can exceed the wall-clock window it was measured over,
        by up to the pool size -- never divide it by elapsed time and call
        the result a share of wall time.
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

    def interrupt_writer(self) -> None:
        """End the statement running on the writer, if any (plan 6.4).

        Safe from any thread. The statement fails with
        ``sqlite3.OperationalError: interrupted`` and ``_run_immediate``
        rolls its transaction back. With nothing running it does nothing,
        and a closed writer is ignored.
        """
        with contextlib.suppress(sqlite3.ProgrammingError):
            self._conn.interrupt()

    def read_sync(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return fn(self._read_sync_conn)

    def close(self) -> None:
        self._read_sync_conn.close()
        for conn in self._read_conns:
            conn.close()
        self._conn.close()

    def close_if_idle(self) -> bool:
        """Close every connection, but only while none of them is in use.

        Returns ``True`` when the connections were closed, ``False`` when a
        write was in flight or a pooled reader was checked out.

        Closing a connection another thread is mid-statement on is a
        use-after-close at the C level, not a Python error: every connection
        here is opened ``check_same_thread=False`` (``_connect_reader``,
        ``_open``) and every query runs in an executor thread, so a
        ``close()`` and a live query can genuinely overlap. ``close()``
        itself does not drain the pool, so the caller is what has to
        establish that overlap is impossible.

        Both checks run on the event-loop thread with no ``await`` between
        them and the ``close()`` they guard, so what they report cannot go
        stale before it is used: a writer holds ``_write_lock`` for the
        whole of ``write``, and a reader holds its connection OUT of
        ``_read_pool`` for the whole of ``read``.

        State what that buys exactly, because it is narrower than
        "nothing is going on": when both predicates read idle, no thread
        is EXECUTING on any connection, so this close cannot land
        mid-statement. It does NOT mean no one can acquire afterwards.
        ``asyncio.Lock.locked()`` reports the HOLDER only and never
        consults the wait queue, so a writer already queued on
        ``_write_lock`` is invisible here and may take it the moment this
        returns; that caller gets a ``ProgrammingError`` on a closed
        connection -- the same clean failure as a late reader, not
        corruption.

        ``_read_sync_conn`` is covered by neither check and needs no cover:
        ``read_sync``/``write_sync`` have no caller inside the server process
        (only ``__main__``'s CLI subcommands and tests).

        A reader arriving after this returns draws a closed connection from
        the pool and fails loudly with ``ProgrammingError``.
        """
        if self._write_lock.locked():
            logger.warning("close skipped: a write is in flight")
            return False
        idle = self._read_pool.qsize()
        if idle != _READ_POOL_SIZE:
            logger.warning(
                "close skipped: %d of %d pooled readers checked out",
                _READ_POOL_SIZE - idle,
                _READ_POOL_SIZE,
            )
            return False
        self.close()
        return True


class FencedWriter:
    """A write handle on one ``Database``, threaded through a job's writes.

    ``read_snapshot_only`` is its read: one read-only snapshot on a pooled
    reader, for a job that builds a later write from what it read.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    async def write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return await self._db.write(fn)

    async def read_snapshot_only(
        self, fn: Callable[[sqlite3.Connection], T], *, label: str
    ) -> T:
        """Run ``fn`` in one read-only snapshot and return its result."""

        def _snap(conn: sqlite3.Connection) -> T:
            with read_only_snapshot(conn, label=label):
                return fn(conn)

        return await self._db.read(_snap)


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


def current_db() -> Database | None:
    """The process database, or ``None`` when none exists.

    Deliberately does **not** call ``get_db()``, which would open a database
    against ``config.db_path`` inside a caller that only holds a bare
    ``sqlite3.Connection``.
    """
    return _db_instance


def close_db() -> None:
    global _db_instance
    if _db_instance is not None:
        _db_instance.close()
        _db_instance = None
