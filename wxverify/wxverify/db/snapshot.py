"""One WAL read snapshot on one connection: BEGIN DEFERRED ... ROLLBACK."""

from __future__ import annotations

import logging
import sqlite3
import time
from collections.abc import Generator
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class SnapshotNestingError(RuntimeError):
    """A read snapshot was requested on a connection already in a transaction."""


@contextmanager
def read_snapshot(
    conn: sqlite3.Connection, *, label: str
) -> Generator[sqlite3.Connection]:
    """Pin one WAL read snapshot on ``conn`` for the duration of the block.

    Every statement inside the block observes the same database state, however
    many times the single writer commits meanwhile -- and the writer is never
    blocked by this transaction (WAL readers and the writer do not contend).

    Read-only by contract. The block must issue no writes, no HTTP, no
    template rendering, no cache warming and no ``await`` -- it runs inside
    one ``Database.read`` executor callback, which is synchronous, and an open
    read snapshot blocks WAL checkpointing for as long as it is held.

    Refuses to nest, BEFORE issuing any SQL. SQLite would refuse the nested
    ``BEGIN`` itself, but its refusal leaves the OUTER transaction open, and
    this context manager's ``finally`` would then roll back a snapshot it does
    not own. The pre-check runs outside the ``try`` so a refusal cannot reach
    that ``finally`` at all. The priming statement, by contrast, runs INSIDE
    it: once ``BEGIN`` has run there is a transaction to end, and a priming
    failure must end it before propagating.

    Failure policy: nothing here is caught. A body or priming error
    propagates unchanged after the ``ROLLBACK``. If the ``ROLLBACK`` itself
    raises while one is in flight, Python chains the two -- the rollback
    error propagates with the original as ``__context__`` -- and the
    transaction it failed to end is left to the pool's own settle step
    (``Database._settle_reader``), which is the net for this manager, not
    part of it.
    """
    if conn.in_transaction:
        raise SnapshotNestingError(
            f"read_snapshot({label!r}) on a connection already in a transaction"
        )
    conn.execute("BEGIN DEFERRED")
    started = time.perf_counter()
    try:
        # DEFERRED does not acquire the snapshot; the first statement that
        # reads the database file does. Pinning it here makes "the snapshot
        # is open on entry" a property of this context manager rather than
        # of whatever the caller's first query happens to be.
        conn.execute("PRAGMA user_version").fetchone()
        yield conn
    finally:
        logger.debug(
            "db snapshot %s held=%.1fms", label, (time.perf_counter() - started) * 1000
        )
        conn.rollback()
