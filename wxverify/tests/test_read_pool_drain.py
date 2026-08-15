"""Tests for the import-swap drain: the mechanics of taking every pooled
read connection back, replacing the database file underneath them, and
handing a fresh pool back out.

These tests are about the DRAIN itself -- ordering, gating, and recovery on
cancellation -- not about the data-migration content of an import, which is
covered elsewhere. Every scenario here builds its own live database and its
own tiny, fully-migrated replacement file rather than reusing fixtures
across files, matching how the rest of this suite is organized.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path

import pytest

from tests.helpers import assert_read_pool_at_rest
from wxverify.db.connection import _READ_POOL_SIZE, Database


def _make_site(conn: sqlite3.Connection, name: str) -> None:
    conn.execute(
        "INSERT INTO sites "
        "(name, forecast_lat, forecast_lon, elevation_m, timezone, enabled) "
        "VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)",
        (name,),
    )


async def _seed_live_site(db: Database, name: str) -> None:
    def _fn(conn: sqlite3.Connection) -> None:
        _make_site(conn, name)

    await db.write(_fn)


def _build_replacement_db(tmp_path: Path, filename: str, site_name: str) -> Path:
    """A standalone, fully-migrated database file suitable as a
    ``replace_from`` swap target -- built via a throwaway ``Database``
    instance (never the process-wide singleton) so migrations run exactly
    as they would on a real import.
    """
    path = tmp_path / filename
    db = Database(str(path))
    try:
        _make_site(db._conn, site_name)  # noqa: SLF001
        db._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        db._conn.commit()  # noqa: SLF001
    finally:
        db.close()
    return path


async def _read_site_names(db: Database) -> list[str]:
    return await db.read(
        lambda conn: [row[0] for row in conn.execute("SELECT name FROM sites")]
    )


def test_swap_waits_for_an_in_flight_read(tmp_path: Path) -> None:
    """``replace_from`` must not complete while a dispatched read is still
    using one of the pooled connections it needs to drain.

    Construction: one read blocks inside its callback until released. The
    swap is started while that read is still in flight and must still be
    unfinished a beat later -- releasing the read is what lets it proceed.
    Fails against any implementation that swaps the underlying file while a
    connection is still checked out and in active use.
    """

    async def _run() -> None:
        db = Database(str(tmp_path / "live.db"))
        try:
            await _seed_live_site(db, "Old Content")

            entered = threading.Event()
            release = threading.Event()

            def _slow(conn: sqlite3.Connection) -> None:
                entered.set()
                assert release.wait(timeout=5.0)

            read_task = asyncio.create_task(db.read(_slow))
            assert await asyncio.to_thread(entered.wait, 5.0)

            new_path = _build_replacement_db(tmp_path, "new.db", "New Content")
            backup_path = tmp_path / "backup.db.bak"
            swap_task = asyncio.create_task(db.replace_from(new_path, backup_path))
            await asyncio.sleep(0.05)
            assert not swap_task.done(), (
                "the swap must not complete while a dispatched read is still "
                "in flight on one of the pooled connections"
            )

            release.set()
            await asyncio.wait_for(read_task, timeout=5.0)
            await asyncio.wait_for(swap_task, timeout=5.0)

            assert await _read_site_names(db) == ["New Content"]
            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_run())


def test_gate_blocks_new_reads_during_a_swap(tmp_path: Path) -> None:
    """A read dispatched after the gate closes must never be served the
    pre-swap database -- it must wait for the swap to finish, however many
    connections happen to cycle back through the pool in the meantime.

    Construction: two connections are held open, so the drain must suspend
    twice (once per connection it still needs) instead of collecting
    everything it needs in one uninterrupted pass. A new read is then
    dispatched and the two held connections released *with a yield between
    them*, not back to back: releasing both without letting the loop run in
    between lets both connections' returns resolve before either waiter's
    continuation does, which happens to hand both of them to whichever
    waiter was already queued deepest -- masking exactly the FIFO
    overtake this test exists to catch. Released one at a time, with the
    loop actually scheduling a continuation between them, a gateless read's
    already-queued pool request can legitimately win the first connection
    ahead of the drain's own next request (both are ordinary FIFO waiters
    at that point), letting it read the OLD file's content instead. With
    the gate enforced, the new read cannot reach the pool at all -- it
    stays parked at the gate no matter what cycles past -- so it can only
    ever observe the finished swap. That difference in what was actually
    read -- not the order two tasks finish in -- is what this test checks.
    """

    async def _run() -> None:
        db = Database(str(tmp_path / "live.db"))
        try:
            await _seed_live_site(db, "Old Content")

            held_entered = [threading.Event() for _ in range(2)]
            held_release = [threading.Event() for _ in range(2)]

            def _make_holder(index: int):
                def _fn(conn: sqlite3.Connection) -> None:
                    held_entered[index].set()
                    assert held_release[index].wait(timeout=5.0)

                return _fn

            holder_tasks = [
                asyncio.create_task(db.read(_make_holder(i))) for i in range(2)
            ]
            for event in held_entered:
                assert await asyncio.to_thread(event.wait, 5.0)

            new_path = _build_replacement_db(tmp_path, "new.db", "New Content")
            backup_path = tmp_path / "backup.db.bak"
            swap_task = asyncio.create_task(db.replace_from(new_path, backup_path))
            await asyncio.sleep(0)
            assert db._read_gate.is_set() is False  # noqa: SLF001

            new_read_task = asyncio.create_task(db.read(_site_names_fn))
            await asyncio.sleep(0)
            assert not new_read_task.done()

            held_release[0].set()
            await asyncio.sleep(0.02)
            held_release[1].set()
            await asyncio.wait_for(swap_task, timeout=5.0)
            rows = await asyncio.wait_for(new_read_task, timeout=5.0)

            assert rows == ["New Content"], (
                "a read dispatched while the gate was closed must never "
                f"observe the pre-swap database, got {rows}"
            )

            await asyncio.wait_for(asyncio.gather(*holder_tasks), timeout=5.0)
            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_run())


def _site_names_fn(conn: sqlite3.Connection) -> list[str]:
    return [row[0] for row in conn.execute("SELECT name FROM sites")]


def test_cancelled_drain_reopens_the_gate_and_the_pool_recovers(
    tmp_path: Path,
) -> None:
    """Cancelling ``replace_from`` while it is still draining must leave the
    database usable afterward: the gate reopens, and once the connection
    that was holding the drain up comes back, the pool serves a full batch
    of concurrent reads again.

    Post-conditions are checked only once everything has settled (the held
    connection has been released and collected) -- a cancelled drain
    legitimately leaves the pool short until that happens, so asserting
    pool-at-rest any earlier would fail against a correct implementation.
    """

    async def _run() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            held_entered = threading.Event()
            held_release = threading.Event()

            def _holder(conn: sqlite3.Connection) -> None:
                held_entered.set()
                assert held_release.wait(timeout=5.0)

            holder_task = asyncio.create_task(db.read(_holder))
            assert await asyncio.to_thread(held_entered.wait, 5.0)

            new_path = _build_replacement_db(tmp_path, "new.db", "New Content")
            backup_path = tmp_path / "backup.db.bak"
            swap_task = asyncio.create_task(db.replace_from(new_path, backup_path))
            await asyncio.sleep(0)
            assert db._read_gate.is_set() is False  # noqa: SLF001

            swap_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await swap_task

            held_release.set()
            await asyncio.wait_for(holder_task, timeout=5.0)

            assert db._read_gate.is_set() is True  # noqa: SLF001

            entered = [threading.Event() for _ in range(_READ_POOL_SIZE)]
            release = threading.Event()

            def _make_blocking(index: int):
                def _fn(conn: sqlite3.Connection) -> int:
                    entered[index].set()
                    assert release.wait(timeout=5.0)
                    return index

                return _fn

            tasks = [
                asyncio.create_task(db.read(_make_blocking(i)))
                for i in range(_READ_POOL_SIZE)
            ]
            for event in entered:
                assert await asyncio.to_thread(event.wait, 5.0), (
                    "the pool did not recover its full concurrency after a "
                    "cancelled drain"
                )
            release.set()
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)
            assert sorted(results) == list(range(_READ_POOL_SIZE))

            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_run())


def test_cancelled_mid_drain_does_not_restock_connections_still_checked_out(
    tmp_path: Path,
) -> None:
    """Cancelling ``replace_from`` mid-drain must return exactly the
    connections it actually took -- never every connection the pool is
    meant to have.

    Construction: two reads are parked, holding two of the four connections
    open. The drain takes the other two immediately and suspends waiting
    for a third; cancelling it at that point must put back only the two it
    actually drained. A recovery that instead unconditionally republishes
    every underlying connection (ignoring which ones are still legitimately
    checked out) would leave the queue at capacity -- and when the two
    parked reads later try to return their own connections, ``put_nowait``
    on an already-full bounded queue raises ``QueueFull`` instead of
    succeeding, which is the concrete, checkable symptom of that bug.
    """

    async def _run() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            k = 2
            parked_entered = [threading.Event() for _ in range(k)]
            parked_release = [threading.Event() for _ in range(k)]

            def _make_parked(index: int):
                def _fn(conn: sqlite3.Connection) -> None:
                    parked_entered[index].set()
                    assert parked_release[index].wait(timeout=5.0)

                return _fn

            parked_tasks = [
                asyncio.create_task(db.read(_make_parked(i))) for i in range(k)
            ]
            for event in parked_entered:
                assert await asyncio.to_thread(event.wait, 5.0)

            new_path = _build_replacement_db(tmp_path, "new.db", "New Content")
            backup_path = tmp_path / "backup.db.bak"
            swap_task = asyncio.create_task(db.replace_from(new_path, backup_path))
            await asyncio.sleep(0)
            # The drain took the two free connections instantly and is now
            # suspended waiting for a third -- the two the parked reads are
            # still holding.
            assert db._read_pool.qsize() == 0  # noqa: SLF001

            swap_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await swap_task

            assert db._read_pool.qsize() == _READ_POOL_SIZE - k, (  # noqa: SLF001
                f"expected exactly {_READ_POOL_SIZE - k} connections back "
                "(what the drain actually took), found "
                f"{db._read_pool.qsize()}"  # noqa: SLF001
            )
            assert db._read_gate.is_set() is True  # noqa: SLF001

            for event in parked_release:
                event.set()
            await asyncio.wait_for(asyncio.gather(*parked_tasks), timeout=5.0)
            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_run())


def test_replace_sync_deferred_cancellation_keeps_gate_closed_until_it_returns(
    tmp_path: Path,
) -> None:
    """Cancelling ``replace_from`` once the drain has already succeeded must
    not reopen the gate or restock the pool until the swap thread has
    actually finished -- the same deferred-cancellation guarantee every
    other caller of ``run_to_completion`` gets, applied to the highest-
    stakes one: the thread that is mid file-swap.

    Construction: ``_replace_sync`` is wrapped, not replaced -- it signals
    its own entry and then waits for a release before doing its real work,
    turning "cancel while the swap thread is running" from a timing guess
    into a rendezvous. Checking state only once the awaited task actually
    raises ``CancelledError`` would not discriminate here: this suite's own
    ``finally`` block (restock, reopen) runs before that exception
    propagates out, so by the time the caller observes the cancellation the
    state has already been restored correctly either way. The state has to
    be inspected while the swap thread is still genuinely running, which is
    what the rendezvous buys.
    """

    async def _run() -> None:
        db = Database(str(tmp_path / "live.db"))
        try:
            await _seed_live_site(db, "Old Content")

            entered = threading.Event()
            release = threading.Event()
            real_replace_sync = Database._replace_sync

            def _wrapped(self: Database, new_db: Path, backup: Path) -> None:
                entered.set()
                assert release.wait(timeout=5.0)
                real_replace_sync(self, new_db, backup)

            Database._replace_sync = _wrapped  # type: ignore[method-assign]
            try:
                new_path = _build_replacement_db(tmp_path, "new.db", "New Content")
                backup_path = tmp_path / "backup.db.bak"
                swap_task = asyncio.create_task(db.replace_from(new_path, backup_path))
                assert await asyncio.to_thread(entered.wait, 5.0)

                swap_task.cancel()
                await asyncio.sleep(0.05)
                assert db._read_gate.is_set() is False, (  # noqa: SLF001
                    "the gate must stay closed while _replace_sync is still "
                    "running, even after cancellation has been requested"
                )
                assert db._read_pool.qsize() == 0, (  # noqa: SLF001
                    "the pool must stay empty while _replace_sync is still "
                    "running, even after cancellation has been requested"
                )

                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await swap_task

                assert db._read_gate.is_set() is True  # noqa: SLF001
                assert db._read_pool.qsize() == _READ_POOL_SIZE  # noqa: SLF001
            finally:
                Database._replace_sync = real_replace_sync  # type: ignore[method-assign]

            assert await _read_site_names(db) == ["New Content"], (
                "the swap itself must have gone through even though the "
                "caller's await was cancelled -- deferred cancellation means "
                "the swap still runs to completion; only the caller's "
                "notification of that is what raises CancelledError"
            )
            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_run())


def test_no_read_observes_a_database_mid_swap_even_after_cancellation(
    tmp_path: Path,
) -> None:
    """A read dispatched after ``replace_from`` has been cancelled -- but
    while the swap thread is still actually running -- must still wait for
    the swap to finish, and must see the new database once it does.

    A drain recovery that reopens the gate as soon as cancellation is
    merely requested, without waiting for the swap thread itself to finish,
    would let this read through mid-swap: at best it reads the pre-swap
    file moments before it is replaced, at worst it catches the file
    mid-rename.
    """

    async def _run() -> None:
        db = Database(str(tmp_path / "live.db"))
        try:
            await _seed_live_site(db, "Old Content")

            entered = threading.Event()
            release = threading.Event()
            real_replace_sync = Database._replace_sync

            def _wrapped(self: Database, new_db: Path, backup: Path) -> None:
                entered.set()
                assert release.wait(timeout=5.0)
                real_replace_sync(self, new_db, backup)

            Database._replace_sync = _wrapped  # type: ignore[method-assign]
            try:
                new_path = _build_replacement_db(tmp_path, "new.db", "New Content")
                backup_path = tmp_path / "backup.db.bak"
                swap_task = asyncio.create_task(db.replace_from(new_path, backup_path))
                assert await asyncio.to_thread(entered.wait, 5.0)

                swap_task.cancel()
                await asyncio.sleep(0.05)

                read_task = asyncio.create_task(db.read(_site_names_fn))
                await asyncio.sleep(0.05)
                assert not read_task.done(), (
                    "a read dispatched while the swap thread is still "
                    "running must not complete before it does, cancellation "
                    "or not"
                )

                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await swap_task

                rows = await asyncio.wait_for(read_task, timeout=5.0)
                assert rows == ["New Content"]
            finally:
                Database._replace_sync = real_replace_sync  # type: ignore[method-assign]

            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_run())


def test_unrecoverable_swap_failure_still_fails_reads_promptly(
    tmp_path: Path,
) -> None:
    """When both the post-swap reopen and the rollback's own reopen fail,
    the database is left unrecoverable -- but that must still fail FAST,
    not hang.

    A gate that stayed closed forever in this situation would be at least
    as bad as the data loss it was trying to avoid: every future request
    would simply stop responding, with nothing to point at. The production
    recovery path republishes whatever it has (here, already-closed
    connections) and reopens the gate regardless, so the next read fails
    immediately with a concrete, diagnosable error instead of hanging.
    """

    async def _run() -> None:
        db = Database(str(tmp_path / "live.db"))
        try:
            await _seed_live_site(db, "Old Content")

            new_path = _build_replacement_db(tmp_path, "new.db", "New Content")
            backup_path = tmp_path / "backup.db.bak"

            real_open = Database._open

            def _always_fail(self: Database) -> None:
                raise RuntimeError("synthetic: every reopen fails")

            Database._open = _always_fail  # type: ignore[method-assign]
            try:
                with pytest.raises(RuntimeError, match="unrecoverable"):
                    await asyncio.wait_for(
                        db.replace_from(new_path, backup_path), timeout=5.0
                    )
            finally:
                Database._open = real_open  # type: ignore[method-assign]

            with pytest.raises(sqlite3.ProgrammingError):
                await asyncio.wait_for(
                    db.read(lambda conn: conn.execute("SELECT 1").fetchone()),
                    timeout=5.0,
                )
        finally:
            db.close()

    asyncio.run(_run())


def test_swap_closes_every_old_connection_across_repeated_cycles(
    tmp_path: Path,
) -> None:
    """Every connection open before a swap -- every pooled reader, the sync
    reader, and the writer -- must actually be closed by the swap, not
    merely replaced by a new reference while the old handle leaks.

    Checked across two swap cycles, since a leak that only shows up on a
    SECOND swap (say, a set of connections captured once and never
    refreshed) would pass a single-cycle check by coincidence. A closed
    ``sqlite3.Connection`` raises ``ProgrammingError`` on any further use;
    that -- not a count of anything -- is what proves the close actually
    happened, rather than just that a new connection replaced an old
    reference.
    """

    async def _run() -> None:
        db = Database(str(tmp_path / "live.db"))
        try:
            await _seed_live_site(db, "Generation 0")

            for cycle in range(2):
                old_conns = list(db._read_conns)  # noqa: SLF001
                old_sync_conn = db._read_sync_conn  # noqa: SLF001
                old_write_conn = db._conn  # noqa: SLF001

                new_path = _build_replacement_db(
                    tmp_path, f"new-{cycle}.db", f"Generation {cycle + 1}"
                )
                backup_path = tmp_path / f"backup-{cycle}.db.bak"
                await asyncio.wait_for(
                    db.replace_from(new_path, backup_path), timeout=5.0
                )

                for old_conn in old_conns:
                    with pytest.raises(sqlite3.ProgrammingError):
                        old_conn.execute("SELECT 1")
                with pytest.raises(sqlite3.ProgrammingError):
                    old_sync_conn.execute("SELECT 1")
                with pytest.raises(sqlite3.ProgrammingError):
                    old_write_conn.execute("SELECT 1")

                assert await _read_site_names(db) == [f"Generation {cycle + 1}"]

            assert_read_pool_at_rest(db)
        finally:
            db.close()

    asyncio.run(_run())
