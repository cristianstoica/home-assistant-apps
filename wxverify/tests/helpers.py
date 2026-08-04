"""Shared test helpers for wxverify's pytest suite.

Kept deliberately small: only helpers actually reused across multiple test
modules belong here. Anything used by a single file stays local to it.
"""

from __future__ import annotations

from wxverify.db.connection import _READ_POOL_SIZE, Database  # noqa: SLF001


def assert_read_pool_at_rest(db: Database) -> None:
    """Assert the read pool is back to its steady state: exactly
    ``_READ_POOL_SIZE`` connections queued, all of them distinct objects.

    Only valid once every dispatched read or drain has actually settled --
    calling this while a connection is still checked out (a read in flight,
    or a drain mid-cancellation-recovery) fails even against a correct
    implementation, since the pool is legitimately short during that window.
    A new recovery branch that forgets to publish a connection back (or
    publishes the same one twice) has nowhere to hide from this check.
    """
    pool = db._read_pool  # noqa: SLF001
    assert pool.qsize() == _READ_POOL_SIZE, (
        f"expected {_READ_POOL_SIZE} connections at rest, found {pool.qsize()}"
    )
    conns = list(pool._queue)  # noqa: SLF001
    ids = {id(conn) for conn in conns}
    assert len(ids) == len(conns) == _READ_POOL_SIZE, (
        "pooled connections must all be distinct objects -- a duplicate id "
        "means the same connection was published to the pool more than once"
    )
