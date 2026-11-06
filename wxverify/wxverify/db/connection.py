"""SQLite WAL connection facade: a single serialized writer and a bounded
pool of readers."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import threading
import time
from collections.abc import Callable, Generator
from dataclasses import dataclass
from typing import Final, Literal, TypeVar

from wxverify import config
from wxverify.core import stop
from wxverify.core.aio import run_to_completion
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.compaction import compact_storage
from wxverify.db.migrations import run_migrations
from wxverify.db.sanitize import sanitize_wedge_prone_timestamps
from wxverify.db.snapshot import read_only_snapshot, read_snapshot

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
# with no such line means every `write`/`write_if_current` that acquired the
# lock and reached its timing `finally` kept both under a second.
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


@dataclass(frozen=True, slots=True)
class InputCounts:
    """The weights cache's key counts, read external-commit count first, then epoch."""

    ext: int
    epoch: int


# What `Database.pinned_snapshot` registers for its connection: the counts its
# snapshot is exactly described by, or why there are none.
PinRefusal = Literal["probe_error", "counts_moved"]
SnapshotPin = InputCounts | PinRefusal


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


class EpochMoved:
    """The outcome of a ``write_if_current`` whose input epoch had moved.

    Returned, never raised: a miss is an expected outcome that the caller
    answers by recomputing from committed state. ``fn`` did not run and the
    transaction changed nothing. Compare against the ``EPOCH_MOVED``
    singleton or with ``isinstance``.
    """

    __slots__ = ()


EPOCH_MOVED: Final = EpochMoved()


class _EpochMovedSignal(Exception):
    """Raised inside a ``write_if_current`` transaction, before ``fn`` runs,
    when the input epoch no longer matches; caught by ``write_if_current``."""


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
        # The input epoch moves on every commit that could change a scoring
        # input: a non-exempt write on the writer connection that changed a
        # row, a commit by any other connection (seen through
        # `PRAGMA data_version`, tracked in `_dv_seen`), and every `_open`.
        # See `_run_epoch_txn`.
        self._input_epoch = 0
        self._dv_seen = 0
        # The external-commit count (`external_commit_seq`) and the lock that
        # guards it together with the probe's baseline `_probe_dv`. The count
        # only grows, so no count stored earlier can recur.
        self._probe_lock = threading.Lock()
        self._external_seq = 0
        # Pooled readers inside `pinned_snapshot`, each with its pin.
        self._snapshot_pins: dict[sqlite3.Connection, SnapshotPin] = {}
        self._open()
        self._stock_pool()

    def _connect_reader(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        self._assert_reader_pragmas(conn)
        return conn

    def _open(self) -> None:
        """Open the writer, sync reader, pooled readers and probe on
        ``self.path``. The write lock and the pool object are ``__init__``'s.

        Creation only: ``__init__`` calls ``_stock_pool()`` afterward to
        publish the pooled connections.
        """
        self._conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._assert_pragmas(self._conn)
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
        # The external-commit probe: one more reader, in neither `_read_conns`
        # nor the pool, so no request can draw it. It never writes, so every
        # commit on the file moves its `data_version`. Its baseline is read
        # here, after the migrations, the sanitizer and the compaction
        # committed, and only ever compared with its own later values.
        self._probe = self._connect_reader()
        with self._probe_lock:
            self._probe_dv = int(
                self._probe.execute("PRAGMA data_version").fetchone()[0]
            )
        # The migrations and the sanitizer above ran through `_run_immediate`
        # directly, and the compaction on `_conn` itself; this bump covers
        # all three.
        self._dv_seen = int(self._conn.execute("PRAGMA data_version").fetchone()[0])
        self._input_epoch += 1

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
            # Inside the try: a COMMIT that fails and leaves the transaction
            # open (e.g. a deferred foreign-key violation) must roll back like
            # any other failure, or the writer stays wedged inside it.
            self._conn.commit()
        except BaseException:
            # Roll back before logging anything. Callers branch on the
            # ORIGINAL error's type (the epoch signal, IntegrityError), so
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

    def _run_epoch_txn(
        self,
        fn: Callable[[sqlite3.Connection], T],
        *,
        expected: int | None,
        exempt: bool,
    ) -> T:
        """Run ``fn`` in one ``_run_immediate`` transaction, moving the epoch.

        Every write transaction on ``self._conn`` except ``_open``'s own
        passes through here. Inside ``BEGIN IMMEDIATE`` a changed
        ``PRAGMA data_version`` (a commit by another connection) moves the
        epoch; then, when ``expected`` is given and no longer matches, the
        transaction ends before ``fn`` runs (``_EpochMovedSignal``). After
        the commit or rollback, a non-exempt ``fn`` that changed at least one
        row (``total_changes`` moved) moves the epoch once more, and then, on
        every exit, ``_absorb_own_commit`` acknowledges this transaction's own
        commit on the external-commit probe.

        Invariant: every commit on ``self._conn`` goes through here, except
        ``_open``'s own, which run before the probe exists. The absorb takes
        every commit it sees on the probe for its own, so a commit made on
        ``_conn`` any other way would be acknowledged without moving either
        count, and a caller comparing the counts would not see that commit.
        """
        # `_body` records the connection and its `total_changes` just before
        # `fn` runs. A list, not `nonlocal` flags: pyright does not see a
        # nested function's writes, narrows such flags to their initial
        # values here, and would treat the bump below as unreachable.
        ran: list[tuple[sqlite3.Connection, int]] = []

        def _body(conn: sqlite3.Connection) -> T:
            dv = int(conn.execute("PRAGMA data_version").fetchone()[0])
            if dv != self._dv_seen:
                self._dv_seen = dv
                self._input_epoch += 1
            if expected is not None and expected != self._input_epoch:
                raise _EpochMovedSignal
            ran.append((conn, conn.total_changes))
            return fn(conn)

        try:
            # Looked up on the instance: tests and the bench patch it there.
            return self._run_immediate(_body)
        finally:
            if ran and not exempt:
                used, before = ran[-1]
                if used.total_changes != before:
                    self._input_epoch += 1
            # Last, after the bump: a lookup that reads the count after the
            # absorb also sees the bump. Looked up on the instance, like
            # `_run_immediate`. Never raises (see its docstring).
            self._absorb_own_commit()

    def _absorb_own_commit(self) -> None:
        """Acknowledge this process's own commit on the probe, uncounted.

        Runs in ``_run_epoch_txn``'s ``finally`` on every exit. Within one
        hold of ``_probe_lock``: (1) read the probe's ``data_version``; (2)
        read ``_conn``'s, which moves only for other connections' commits and
        was stored in ``_dv_seen`` inside this transaction's ``BEGIN
        IMMEDIATE``; (3) if it differs, another connection committed after
        this transaction, so count it; (4) store the probe's value as the
        baseline. Steps 3 and 4 run only after both reads succeed, so a
        failed read changes nothing and the next ``external_commit_seq``
        counts the unacknowledged change (a conservative miss). The probe is
        read before ``_conn``: the other way round, a foreign commit landing
        between the two reads would be stored as the baseline with neither
        count moved.

        Never changes ``_dv_seen`` or ``_input_epoch``: the next write's
        ``_body`` must still see a foreign commit and move the epoch for it.

        Never raises: an exception escaping the ``finally`` would turn a
        committed write into a failure, or replace the original exception
        (including ``_EpochMovedSignal``). A ``sqlite3.Error`` is logged at
        DEBUG, anything else at WARNING, both after ``_probe_lock`` is
        released; ``BaseException`` outside ``Exception`` propagates.
        """
        try:
            with self._probe_lock:
                seen = int(self._probe.execute("PRAGMA data_version").fetchone()[0])
                own = int(self._conn.execute("PRAGMA data_version").fetchone()[0])
                if own != self._dv_seen:
                    self._external_seq += 1
                self._probe_dv = seen
        except sqlite3.Error:
            logger.debug(
                "probe absorb failed; the next lookup will miss", exc_info=True
            )
        except Exception:
            logger.warning(
                "probe absorb failed; the next lookup will miss", exc_info=True
            )

    def external_commit_seq(self) -> int | None:
        """A count that moves after every commit by another connection.

        Reads the probe's ``PRAGMA data_version`` and, when it differs from
        the stored baseline, stores it and adds 1 to the count, all within one
        hold of ``_probe_lock``. This process's own commits are absorbed
        uncounted by ``_absorb_own_commit``. Returns ``None`` when the probe
        cannot be read: whether another connection committed can't be told.

        ``.fetchone()`` on the one-row ``PRAGMA`` resets the statement, so
        the probe holds no read mark between calls and never blocks a
        checkpoint.
        """
        with self._probe_lock:
            try:
                seen = int(self._probe.execute("PRAGMA data_version").fetchone()[0])
            except sqlite3.Error:
                return None
            if seen != self._probe_dv:
                self._probe_dv = seen
                self._external_seq += 1
            return self._external_seq

    def owns_pooled_reader(self, conn: sqlite3.Connection) -> bool:
        """Whether ``conn`` is one of this database's current pooled readers.

        An identity test against ``_read_conns``, which ``_settle_reader``
        keeps naming the pooled objects when it replaces one. ``read_sync``'s
        connection and the probe are not pooled readers.
        """
        return any(conn is pooled for pooled in self._read_conns)

    def input_counts(self) -> InputCounts | None:
        """The external-commit count, then the input epoch; ``None`` when the
        probe cannot be read.

        The count first: the writer bumps the epoch and then absorbs, so a
        reading whose count follows the absorb also sees the bump.
        """
        # Looked up on the instance: tests patch it there.
        ext = self.external_commit_seq()
        if ext is None:
            return None
        return InputCounts(ext=ext, epoch=self.input_epoch)

    def snapshot_pin(self, conn: sqlite3.Connection) -> SnapshotPin | None:
        """The pin ``pinned_snapshot`` holds for ``conn``, or ``None`` outside one."""
        return self._snapshot_pins.get(conn)

    @contextlib.contextmanager
    def pinned_snapshot(
        self, conn: sqlite3.Connection, *, label: str
    ) -> Generator[sqlite3.Connection]:
        """``read_snapshot`` on ``conn`` that also pins the weights cache's counts.

        The counts are read before ``BEGIN DEFERRED`` and again after the
        priming read, both times from the probe, never ``conn``. Equal, the
        snapshot is exactly the state they describe and the pin is those
        counts; otherwise the pin is ``"counts_moved"``, or ``"probe_error"``
        when either reading failed. ``snapshot_pin`` returns it inside the
        block. A ``conn`` that is not one of this database's pooled readers
        gets a plain ``read_snapshot``: no pin is registered, no counts read.
        """
        if not self.owns_pooled_reader(conn):
            with read_snapshot(conn, label=label):
                yield conn
            return
        before = self.input_counts()
        with read_snapshot(conn, label=label):
            after = self.input_counts()
            pin: SnapshotPin
            if before is None or after is None:
                pin = "probe_error"
            elif after != before:
                pin = "counts_moved"
            else:
                pin = before
            # Registered inside the snapshot, so a nesting refusal on entry
            # never reaches the `finally` and never pops an outer pin.
            self._snapshot_pins[conn] = pin
            try:
                yield conn
            finally:
                # Before read_snapshot's ROLLBACK.
                del self._snapshot_pins[conn]

    async def write(
        self,
        fn: Callable[[sqlite3.Connection], T],
        *,
        epoch_exempt: bool = False,
    ) -> T:
        requested = time.perf_counter()
        async with self._write_lock:
            acquired = time.perf_counter()
            try:
                # A nested zero-arg closure, not `self._run_epoch_txn` passed
                # directly: `_run_epoch_txn` is itself generic, and
                # run_to_completion's `Callable[..., T]` erases per-argument
                # types, so a bare method reference loses the binding between
                # its own T and this T. Closing over the already-bound `fn`
                # here resolves `_run_epoch_txn`'s T against it first.
                def _call() -> T:
                    return self._run_epoch_txn(fn, expected=None, exempt=epoch_exempt)

                return await run_to_completion(_call)
            finally:
                # The stop time is taken here, inside the lock, so the raise
                # and cancellation paths are measured too.
                _log_slow_write(fn, requested, acquired, time.perf_counter())

    @property
    def input_epoch(self) -> int:
        """Input epoch: moves on every commit that could change a scoring input.

        Safe to read from the event loop with no lock: an unlocked read is an
        optimistic snapshot for the caller's bookkeeping, and the guarantee
        comes from the recheck that ``_run_epoch_txn`` makes under the write
        lock, inside ``BEGIN IMMEDIATE``, in ``write_if_current``.
        """
        return self._input_epoch

    async def write_if_current(
        self,
        fn: Callable[[sqlite3.Connection], T],
        *,
        epoch: int,
    ) -> tuple[T, int] | EpochMoved:
        """Apply ``fn`` only if the input epoch is still ``epoch``.

        Two outcomes. ``EPOCH_MOVED`` when a commit that could change a
        scoring input landed since ``epoch`` was read; ``fn`` did not run.
        Otherwise ``(result, epoch_after)``, where ``epoch_after`` includes
        this write's own bump, so a caller applying the next chunk of the
        same delta passes it and its own earlier chunk does not count as a
        foreign change.
        """
        requested = time.perf_counter()
        async with self._write_lock:
            acquired = time.perf_counter()
            try:

                def _call() -> T:
                    return self._run_epoch_txn(fn, expected=epoch, exempt=False)

                try:
                    result = await run_to_completion(_call)
                except _EpochMovedSignal:
                    return EPOCH_MOVED
                # Read under the lock: includes this write's own bump.
                return result, self._input_epoch
            finally:
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
        return self._run_epoch_txn(fn, expected=None, exempt=False)

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
        self._probe.close()
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
        whole of ``write``/``write_if_current``, and a reader holds its
        connection OUT of ``_read_pool`` for the whole of ``read``.

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

    A read whose result feeds a later ``write_if_current`` must come from
    ``read_at_epoch``, which pairs it with the input epoch it was read at.
    ``epoch_exempt`` marks every write through this handle as unable to
    change a scoring input, so none of them moves the input epoch.
    """

    def __init__(self, db: Database, *, epoch_exempt: bool = False) -> None:
        self._db = db
        self._epoch_exempt = epoch_exempt

    @property
    def input_epoch(self) -> int:
        """The database's current input epoch (``Database.input_epoch``)."""
        return self._db.input_epoch

    async def write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return await self._db.write(fn, epoch_exempt=self._epoch_exempt)

    async def read_at_epoch(
        self, fn: Callable[[sqlite3.Connection], T], *, label: str
    ) -> tuple[int, T]:
        """Run ``fn`` in one read-only snapshot; return ``(epoch, result)``.

        The epoch is read BEFORE the read is submitted, so it always precedes
        the snapshot: a commit between the two lies inside the snapshot but
        after the epoch, and the later ``write_if_current`` misses spuriously
        (safe). The reverse order would let a commit land after the snapshot
        and before the epoch read, and a stale apply would pass.
        """
        epoch = self._db.input_epoch

        def _snap(conn: sqlite3.Connection) -> T:
            with read_only_snapshot(conn, label=label):
                return fn(conn)

        return epoch, await self._db.read(_snap)

    async def write_if_current(
        self, fn: Callable[[sqlite3.Connection], T], *, epoch: int
    ) -> tuple[T, int] | EpochMoved:
        """``Database.write_if_current`` on this handle's database."""
        return await self._db.write_if_current(fn, epoch=epoch)


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


@contextlib.contextmanager
def pinned_read_snapshot(
    conn: sqlite3.Connection, *, label: str
) -> Generator[sqlite3.Connection]:
    """read_snapshot that also pins the weights cache's counts on a pooled reader.

    A connection that is not a pooled reader of the process database (or no
    process database) gets a plain ``read_snapshot``. Uses ``current_db``,
    never ``get_db``, so it never opens a database.
    """
    db = current_db()
    if db is None or not db.owns_pooled_reader(conn):
        with read_snapshot(conn, label=label):
            yield conn
        return
    with db.pinned_snapshot(conn, label=label):
        yield conn


def close_db() -> None:
    global _db_instance
    if _db_instance is not None:
        _db_instance.close()
        _db_instance = None
