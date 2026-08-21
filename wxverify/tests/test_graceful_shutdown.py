"""Tests for the 0.8.9 graceful-shutdown job-reclaim fix.

`run_worker`'s loop is wrapped in a single loop-level
``except asyncio.CancelledError`` that runs ``reclaim_all_stale`` and
re-raises (never swallows). These tests drive a REAL ``jobs`` row through a
real sqlite-backed ``Database`` (per-test tmp DB, no mocked persistence) and
assert on-disk row state after cancellation.

Harness idioms are copied verbatim from ``tests/test_write_lock_serialization.py``
(``_init_tmp_db``, ``_patch_worker_infra``) and ``tests/test_db_transfer.py``
(the ``TestClient`` context-manager lifespan-driving pattern). This repo has
no ``pytest-asyncio``: every test wraps its coroutine body in
``asyncio.run(_run())``, except the one test that drives the real ASGI
lifespan via ``TestClient`` (which runs its own event loop on a background
thread and must stay a plain sync test, per ``test_db_transfer.py``'s idiom).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from starlette.testclient import TestClient

from wxverify import config
from wxverify.api.app import _cancel_and_reap, create_app, lifespan
from wxverify.db.connection import Database, close_db, get_db, init_db
from wxverify.db.queue import claim_next_job
from wxverify.worker.processor import run_worker

# ---------------------------------------------------------------------------
# Harness (verbatim idiom from test_write_lock_serialization.py).
# ---------------------------------------------------------------------------


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


def _patch_worker_infra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Silence the per-iteration housekeeping calls unrelated to shutdown.

    Never patches ``reclaim_all_stale`` or ``claim_next_job`` -- those are
    exactly the seams under test.
    """
    monkeypatch.setattr(
        "wxverify.worker.processor.set_runtime_state_now", lambda _c, _k: None
    )
    monkeypatch.setattr("wxverify.worker.processor.scheduler_tick", lambda _c: None)
    monkeypatch.setattr(
        "wxverify.worker.processor.purge_failed_jobs_older_than", lambda _c, _h: None
    )


def _insert_job(
    conn: sqlite3.Connection, *, status: str = "pending", job_key: str = "k1"
) -> int:
    """Insert a minimal, synthetic ``catchup`` job row (no site needed)."""
    cur = conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, payload, status)
        VALUES ('catchup', NULL, ?, '{}', ?)
        """,
        (job_key, status),
    )
    conn.commit()
    job_id = cur.lastrowid
    assert job_id is not None, "INSERT must produce a rowid"
    return job_id


def _job_row(conn: sqlite3.Connection, job_id: int) -> dict[str, Any]:
    row = conn.execute(
        "SELECT status, next_attempt_at FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    assert row is not None, f"job {job_id} vanished"
    return {"status": row["status"], "next_attempt_at": row["next_attempt_at"]}


def _job_status(conn: sqlite3.Connection, job_id: int) -> str:
    return str(_job_row(conn, job_id)["status"])


async def _await_status(
    conn: sqlite3.Connection, job_id: int, expected: str, *, timeout: float = 2.0
) -> None:
    deadline = time.monotonic() + timeout
    while _job_status(conn, job_id) != expected:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"job {job_id} never reached status={expected!r} "
                f"(last={_job_status(conn, job_id)!r})"
            )
        await asyncio.sleep(0.01)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_cancellation_reclaims_claimed_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancelling a worker mid-job resets the claimed row to pending.

    Catches: a regression that removes or narrows the loop-level
    ``except asyncio.CancelledError`` clause, leaving the row orphaned
    ``running``.
    """
    conn = _init_tmp_db(tmp_path)
    _patch_worker_infra(monkeypatch)
    job_id = _insert_job(conn)
    before = _job_row(conn, job_id)
    db = get_db()

    block = asyncio.Event()

    async def _blocked_dispatch(_db: Any, _writer: Any, _job: Any) -> None:
        await block.wait()

    monkeypatch.setattr("wxverify.worker.processor.dispatch", _blocked_dispatch)

    async def _run() -> None:
        task = asyncio.create_task(run_worker(db))
        await _await_status(conn, job_id, "running")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        after = _job_row(conn, job_id)
        assert after["status"] == "pending"
        assert after["next_attempt_at"] == before["next_attempt_at"], (
            "reclaim must not touch next_attempt_at (bare pending, not "
            "defer_job's fresh next_attempt_at=now)"
        )

    asyncio.run(_run())


def test_cancellation_reraises_not_swallowed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reclaim write's own inner try/except must never swallow the
    CancelledError it wraps.

    Catches: a broad ``except Exception``/bare ``except:`` accidentally
    wrapping the whole handler (not just the reclaim write), which would
    flip ``_stop_on_worker_done`` onto its harder ``os._exit(1)`` path.
    """
    conn = _init_tmp_db(tmp_path)
    _patch_worker_infra(monkeypatch)
    job_id = _insert_job(conn)
    db = get_db()

    block = asyncio.Event()

    async def _blocked_dispatch(_db: Any, _writer: Any, _job: Any) -> None:
        await block.wait()

    monkeypatch.setattr("wxverify.worker.processor.dispatch", _blocked_dispatch)

    async def _run() -> None:
        task = asyncio.create_task(run_worker(db))
        await _await_status(conn, job_id, "running")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled() is True

    asyncio.run(_run())


def test_cancellation_with_no_claimed_job_is_a_noop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancelling an idle worker (parked in the poll sleep) touches no row.

    The loop-level handler still issues one zero-row ``UPDATE`` on every
    shutdown (``WHERE status='running'`` matches nothing), so the assertion
    is "no row CHANGED status", not "no write was attempted" -- the latter
    would be a false failure against a correct implementation.

    Catches: a handler that reclaims something it shouldn't, or that raises
    on the idle path.
    """
    conn = _init_tmp_db(tmp_path)
    _patch_worker_infra(monkeypatch)
    completed_id = _insert_job(conn, status="completed", job_key="k-completed")
    failed_id = _insert_job(conn, status="failed", job_key="k-failed")
    db = get_db()

    async def _run() -> None:
        task = asyncio.create_task(run_worker(db))
        # Let the loop run one full iteration (claim finds nothing, parks in
        # asyncio.sleep(POLL_INTERVAL)); cancellation is immediate regardless
        # of how much of the sleep remains.
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert _job_status(conn, completed_id) == "completed"
        assert _job_status(conn, failed_id) == "failed"

    asyncio.run(_run())


def test_cancellation_during_claim_write_leaves_no_running_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancellation delivered before ``job_id`` even exists is still caught.

    ``job_id = job.id`` (processor.py) sits OUTSIDE the per-job try block --
    a per-job-scoped handler cannot see a cancellation delivered during the
    claim write itself, because at that point there is no ``job_id`` to
    react to. Only a loop-level handler (which needs no ``job_id``) collapses
    this window.

    Construction: wraps ``Database.write`` so that, immediately AFTER the
    real ``claim_next_job`` write has committed (so the row genuinely reads
    ``running`` on disk) but BEFORE control returns to ``run_worker``'s
    ``job = await db.write(claim_next_job)`` line, the coroutine blocks on a
    plain ``asyncio.Event``. Cancelling there lands squarely in the claim
    window without needing to hold a sqlite transaction open across threads
    (which would independently and unavoidably fail any second write on the
    same connection -- a different, non-discriminating failure mode).

    Catches: a handler scoped to the per-job block instead of the whole loop.
    """
    conn = _init_tmp_db(tmp_path)
    _patch_worker_infra(monkeypatch)
    job_id = _insert_job(conn)
    db = get_db()

    claimed = asyncio.Event()
    release = asyncio.Event()
    real_write = Database.write

    async def _write_with_barrier(self: Database, fn: Any) -> Any:
        result = await real_write(self, fn)
        if fn is claim_next_job:
            claimed.set()
            await release.wait()
        return result

    monkeypatch.setattr(Database, "write", _write_with_barrier)

    async def _run() -> None:
        task = asyncio.create_task(run_worker(db))
        await asyncio.wait_for(claimed.wait(), timeout=2.0)
        # The row is committed 'running' on disk right now, but run_worker's
        # coroutine has not yet resumed past `db.write(claim_next_job)` --
        # `job_id` does not exist in that frame yet.
        assert _job_status(conn, job_id) == "running"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert _job_status(conn, job_id) == "pending"

    asyncio.run(_run())


def test_lifespan_shutdown_reclaims_claimed_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end wiring: the real ASGI lifespan shutdown path reclaims a
    claimed job, not just ``run_worker`` driven as a bare task.

    Every other test in this file drives ``run_worker`` directly and never
    enters ``lifespan()`` -- this is the only construction that pins the
    production wiring (``lifespan()``'s ``finally:`` -> ``worker.cancel()`` /
    ``await worker`` -> the handler under test). Must be RED against a
    ``run_worker`` with the ``except asyncio.CancelledError`` clause removed.

    Catches: the reclaim being correct in isolation but never actually
    reached through the production shutdown path.

    Harness note: unlike every other test in this file, the worker loop is
    LIVE here (running real `BEGIN IMMEDIATE`/`commit` transactions on the
    shared writer connection from executor threads) while the test thread
    also wants to write and poll. `Database._write_lock` is an asyncio lock,
    so raw access to `db._conn` from the test thread bypasses it entirely —
    two threads then race pysqlite's not-atomic "txn open? -> COMMIT"
    sequence and the loser dies with "cannot commit - no transaction is
    active" (observed as a rare full-suite flake). All test-side SQL
    therefore uses a DEDICATED side connection to the same file DB; the
    shared writer connection is never touched from this thread.
    """
    _init_tmp_db(tmp_path)

    block = asyncio.Event()

    async def _blocked_dispatch(_db: Any, _writer: Any, _job: Any) -> None:
        await block.wait()

    monkeypatch.setattr("wxverify.worker.processor.dispatch", _blocked_dispatch)

    stopped: list[None] = []
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))
    side = sqlite3.connect(config.db_path, timeout=5.0)
    side.row_factory = sqlite3.Row
    try:
        with TestClient(app) as client:
            job_id = _insert_job(side)
            deadline = time.monotonic() + 2.0
            while _job_status(side, job_id) != "running":
                if time.monotonic() > deadline:
                    raise TimeoutError("job never reached status=running")
                time.sleep(0.02)
            del client  # unused past this point; the `with` drives shutdown
        # Exiting the `with` block above drove ASGI lifespan shutdown through
        # the real `finally:` (`worker.cancel()` + `await worker`).
        row = side.execute(
            "SELECT status, next_attempt_at FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        assert row is not None
        assert row["status"] == "pending"
        assert stopped == [], "the default hard-kill stop_process must never fire"
    finally:
        side.close()


def test_cancellation_inside_claim_transaction_reclaims_cleanly_without_a_boot_sweep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A cancellation delivered while the CLAIM's own write transaction is
    still open (uncommitted) no longer collides with the loop-level
    handler's own ``db.write(reclaim_all_stale)``: the write lock is now
    held until the claim's executor thread actually finishes (commits or
    rolls back), so the reclaim can never start a second transaction against
    a connection the claim is still using. The same construction that used
    to produce a benign "shutdown reclaim failed" WARNING and defer recovery
    to the next boot now reclaims the row cleanly, in place, during shutdown
    itself.

    Construction: ``claim_next_job`` is wrapped to perform the REAL claim
    (which commits nothing yet -- ``_run_immediate`` commits only after the
    wrapped callable returns) and then block on a ``threading.Event`` INSIDE
    the executor thread, holding the transaction open. Cancelling the worker
    task at that point lands the ``CancelledError`` at the claim's own
    deferred-cancellation await; it is not actually delivered to the caller
    until the executor thread finishes, so the blocking event is released
    from a background task after a deliberate delay, scheduled right after
    the cancel rather than awaited immediately -- otherwise the test would
    hang waiting on a thread nothing else would ever unblock. The delay
    itself is deliberate, not incidental: releasing immediately would also
    happen to avoid a regressed, pre-fix write (one that drops back to
    releasing the write lock the instant its own await is cancelled, without
    waiting for the thread) racing its collision to completion before the
    thread wakes, which would make this test pass for the wrong reason
    against exactly the regression it exists to catch. Holding the thread
    blocked for a fixed, generous interval instead gives that regression's
    reclaim every opportunity to reach its own ``BEGIN IMMEDIATE`` and
    collide while the transaction is still open -- a loaded machine only
    widens that margin, never narrows it, so this is not a flaky wait for
    the "real" work to finish. An ``asyncio.Event`` would not do here (it is
    not thread-safe to signal from the executor thread), and blocking at the
    ``asyncio`` level (as
    ``test_cancellation_during_claim_write_leaves_no_running_row`` does)
    would let cancellation reach the block directly instead of leaving a
    transaction open for the reclaim to (no longer) collide with.

    Catches: any regression that goes back to releasing the write lock
    before the claim's thread has actually committed or rolled back --
    silently reintroducing the collision and the warning this asserts is
    now absent.
    """
    conn = _init_tmp_db(tmp_path)
    _patch_worker_infra(monkeypatch)
    job_id = _insert_job(conn)
    db = get_db()

    claimed = threading.Event()
    release = threading.Event()
    real_claim_next_job = claim_next_job

    def _blocking_claim(c: sqlite3.Connection) -> Any:
        job = real_claim_next_job(c)
        claimed.set()
        release.wait()
        return job

    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _blocking_claim)

    async def _await_committed(expected: str, *, timeout: float = 2.0) -> None:
        """Poll via a FRESH connection, so only a truly COMMITTED write is
        observed (unlike ``conn``, which shares thread A's still-open
        transaction and would see the write before it is durable)."""
        check_conn = sqlite3.connect(config.db_path)
        try:
            deadline = time.monotonic() + timeout
            while True:
                row = check_conn.execute(
                    "SELECT status FROM jobs WHERE id = ?", (job_id,)
                ).fetchone()
                if row is not None and row[0] == expected:
                    return
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"job {job_id} never committed status={expected!r} "
                        f"(last={None if row is None else row[0]!r})"
                    )
                await asyncio.sleep(0.01)
        finally:
            check_conn.close()

    async def _release_after_a_delay() -> None:
        # 50 ms is a large margin over the microsecond-scale, purely
        # in-process chain a regressed write's cancellation propagation and
        # its reclaim's dispatch would need -- see the docstring above for
        # why this delay is deliberate rather than incidental.
        await asyncio.sleep(0.05)
        release.set()

    async def _run() -> None:
        task = asyncio.create_task(run_worker(db))
        delayed_release: asyncio.Task[None] | None = None
        try:
            while not claimed.is_set():
                await asyncio.sleep(0.01)
            # The claim's UPDATE has run but not committed -- visible only via
            # the SAME connection (thread A is idle in `release.wait()`, no SQL
            # in flight, so this same-connection read is safe here).
            assert _job_status(conn, job_id) == "running"

            with caplog.at_level(logging.WARNING, logger="wxverify.worker.processor"):
                task.cancel()
                delayed_release = asyncio.create_task(_release_after_a_delay())
                with pytest.raises(asyncio.CancelledError):
                    await task
            warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
            assert not any(
                "shutdown reclaim failed" in r.getMessage() for r in warnings
            ), (
                f"the write lock now defers to the claim's thread finishing, so "
                f"the reclaim must never collide with it: "
                f"{[r.getMessage() for r in warnings]}"
            )
        finally:
            # Belt and braces: release is already set on every path above,
            # but re-set it, cancel the delayed-release task if it is still
            # pending, and make sure the worker task is not left running if
            # an assertion above failed before reaching that point --
            # skipping this would hang the whole test process on teardown
            # instead of reporting a clean failure.
            release.set()
            if delayed_release is not None and not delayed_release.done():
                delayed_release.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await delayed_release
            if not task.done():
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        # The claim commits, and the shutdown-time reclaim -- no longer
        # blocked out by a collision -- reaches the row directly: no
        # next-boot sweep is needed to recover it.
        await _await_committed("pending")
        assert _job_status(conn, job_id) == "pending"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Lifespan task-ownership fix: every background task is registered the
# moment it exists and reaped (cancelled AND awaited) unconditionally,
# whether startup fails after creation or one sibling task itself fails.
# ---------------------------------------------------------------------------


def test_startup_failure_after_task_creation_still_reaps_every_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A startup failure AFTER all three tasks exist must still cancel and
    await every one of them to completion.

    Catches: the pre-fix shape where the three ``create_task`` calls sit
    ABOVE the ``try:``. There, a raise from ``publish_discovery`` (which
    runs after all three creations, before ``yield``) never enters the
    ``try``, so the ``finally`` never runs and all three tasks leak --
    cancelled never, awaited never.
    """
    _init_tmp_db(tmp_path)
    reaped: list[str] = []

    def _make_stub(name: str):
        async def _stub(*_args: object) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # The extra turn is not padding: it is the whole mechanism
                # under test. `cancel()` only REQUESTS cancellation; the
                # marker below is unreachable unless the task was awaited
                # PAST the delivery turn, which is exactly what discriminates
                # "awaited to completion" from "cancellation merely
                # requested".
                await asyncio.sleep(0)
                reaped.append(name)
                raise

        return _stub

    monkeypatch.setattr("wxverify.api.app.run_worker", _make_stub("worker"))
    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper",
        _make_stub("export sweeper"),
    )
    monkeypatch.setattr(
        "wxverify.api.app.warm_read_cache", _make_stub("read-cache warm")
    )

    async def _boom(_port: int) -> None:
        # The leading `await asyncio.sleep(0)` is not padding: `create_task`
        # only SCHEDULES a task via `call_soon`, it does not run it. Without
        # an intervening scheduling turn here, a synchronous raise never
        # yields control back to the loop, so the three just-created tasks
        # never reach their first `await` before `.cancel()` lands on them
        # in the `finally`. Cancelling a task that has never been stepped
        # throws `CancelledError` in at the very top of its coroutine --
        # before it ever enters its own `try` -- so the stub's marker would
        # never fire even against CORRECT code, silently destroying this
        # test's ability to discriminate (verified empirically: dropping
        # this line collapses `reaped` to `[]` under the fix too, not only
        # under the base). One scheduling turn is enough for all three
        # tasks to reach their `asyncio.Event().wait()`.
        await asyncio.sleep(0)
        raise RuntimeError("discovery boom")

    monkeypatch.setattr("wxverify.api.app.publish_discovery", _boom)

    stopped: list[None] = []
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))

    async def _run() -> None:
        cm = lifespan(app)
        with pytest.raises(RuntimeError, match="discovery boom"):
            await cm.__aenter__()
        # No await above this line since the raise: a cancel-without-await
        # regression cannot have completed a handler that itself awaits, so
        # asserting here (rather than after asyncio.run) is load-bearing --
        # asyncio.run's own loop teardown would reap the leaked tasks too
        # and make a post-run assertion pass vacuously.
        assert sorted(reaped) == ["export sweeper", "read-cache warm", "worker"]

    asyncio.run(_run())
    assert stopped == [], "the default hard-kill stop_process must never fire"


def test_a_failing_task_does_not_prevent_sibling_reaping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A task completing with a non-CancelledError must be captured and
    reported by name, and must not prevent its siblings from being
    cancelled and awaited to completion.

    Catches: the pre-fix sequential ``finally`` (``worker.cancel()`` /
    ``await worker`` / ``export_sweeper.cancel()`` / ``await export_sweeper``
    / ...), where a ``RuntimeError`` surfacing from the first await skips
    every await behind it and escapes the lifespan. Also catches the
    partial-fix mutant ``gather(*handles)`` without ``return_exceptions=True``,
    which resolves on the first exception -- leaving siblings unawaited and
    still raising out of shutdown.
    """
    _init_tmp_db(tmp_path)
    reaped: list[str] = []
    sweeper_reached_raise = asyncio.Event()

    def _make_stub(name: str):
        async def _stub(*_args: object) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0)
                reaped.append(name)
                raise

        return _stub

    async def _crashing_sweeper() -> None:
        sweeper_reached_raise.set()
        raise RuntimeError("sweeper boom")

    monkeypatch.setattr("wxverify.api.app.run_worker", _make_stub("worker"))
    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper", _crashing_sweeper
    )
    monkeypatch.setattr(
        "wxverify.api.app.warm_read_cache", _make_stub("read-cache warm")
    )

    async def _noop_discovery(_port: int) -> None:
        return None

    monkeypatch.setattr("wxverify.api.app.publish_discovery", _noop_discovery)

    stopped: list[None] = []
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        # Rendezvous, not a sleep: the sweeper task must already be DONE
        # with a stored RuntimeError before shutdown starts, or
        # `_cancel_and_reap`'s up-front `.cancel()` would land on a
        # not-yet-started task and it would finish CANCELLED instead --
        # silently destroying the construction this test relies on.
        await sweeper_reached_raise.wait()
        with caplog.at_level(logging.ERROR, logger="wxverify.api.app"):
            await cm.__aexit__(None, None, None)
        # No await since __aexit__ returned: the assertion must sit before
        # asyncio.run's own teardown can reap anything on its own.
        assert sorted(reaped) == ["read-cache warm", "worker"]

    asyncio.run(_run())

    recs = [
        r
        for r in caplog.records
        if r.name == "wxverify.api.app"
        and r.getMessage() == "shutdown: export sweeper failed"
    ]
    assert len(recs) == 1, [r.getMessage() for r in caplog.records]
    exc = recs[0].exc_info
    assert exc is not None and isinstance(exc[1], RuntimeError)
    assert str(exc[1]) == "sweeper boom"
    assert stopped == []


# ---------------------------------------------------------------------------
# `_cancel_and_reap` shielding fix: a SECOND cancellation of the lifespan
# (e.g. a second SIGTERM, or the ASGI server cancelling shutdown on its own
# timeout) must not reach the still-cleaning-up children, and the reap must
# still finish and report before the deferred cancellation is re-raised.
#
# Every test below drives `lifespan()` by hand inside a single
# `asyncio.run(_run())`, never `TestClient`: `TestClient`'s anyio portal
# closes via `Runner.close()` -> `_cancel_all_tasks`, which itself cancels
# AND awaits any leaked task, so an assertion made after the portal closes
# would pass whether or not `_cancel_and_reap` shields anything. Every
# assertion therefore sits directly inside `_run`, with no `await` between
# the observed event and the assertion.
#
# The "second cancellation" is delivered by wrapping `cm.__aexit__(...)` in
# its own task (`shutdown`) and cancelling THAT task, rather than wrapping
# the whole `__aenter__`/`__aexit__` drive in one task. Both are faithful
# analogues of a second real cancellation reaching `lifespan()`'s `finally`
# while it awaits `_cancel_and_reap`; this one is chosen because it keeps
# `__aenter__` (task creation) off the task being cancelled, so the
# cancellation is guaranteed to land exactly at the `await
# asyncio.shield(reap)` inside `_cancel_and_reap` and nowhere earlier.
# ---------------------------------------------------------------------------


def _make_gated_worker_stub(
    order: list[str], entered: asyncio.Event, release: asyncio.Event
):
    """A worker stand-in whose cancellation handler parks mid-cleanup until
    the test releases it -- the rendezvous that turns "cancel a second time
    while children are still cleaning up" into a deterministic window
    instead of a race."""

    async def _stub(*_args: object) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            entered.set()
            await release.wait()
            order.append("worker-cleanup-done")
            raise

    return _stub


def _make_immediate_stub(_name: str):
    """A sibling stand-in (export sweeper / read-cache warm) that reacts to
    cancellation immediately, with no gating -- only the worker stub above
    needs to be held open for these tests' rendezvous."""

    async def _stub(*_args: object) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise

    return _stub


def _wire_gated_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[str], asyncio.Event, asyncio.Event, list[None]]:
    """Shared wiring for T3, T5 and T7: a gated worker plus two immediately
    re-raising siblings, with a noop discovery publish and a captured
    ``stop_process`` (the production default is ``os._exit(1)``, which would
    kill the test process outright)."""
    order: list[str] = []
    entered = asyncio.Event()
    release = asyncio.Event()
    monkeypatch.setattr(
        "wxverify.api.app.run_worker", _make_gated_worker_stub(order, entered, release)
    )
    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper",
        _make_immediate_stub("export sweeper"),
    )
    monkeypatch.setattr(
        "wxverify.api.app.warm_read_cache", _make_immediate_stub("read-cache warm")
    )

    # This coroutine has no `await` in its body, so awaiting it never
    # yields control back to the loop -- `__aenter__` therefore runs
    # straight through to `yield` without ever stepping the three
    # `create_task`ed tasks. It is the test's later `await entered.wait()`
    # / `await sweeper_reached_raise.wait()` that first suspends and lets
    # the loop run them, which is what gets every stub past its own first
    # `await` before any `.cancel()` lands.
    async def _noop_discovery(_port: int) -> None:
        return None

    monkeypatch.setattr("wxverify.api.app.publish_discovery", _noop_discovery)
    stopped: list[None] = []
    return order, entered, release, stopped


def test_second_cancellation_is_shielded_from_children(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A second cancellation of shutdown must not reach the worker mid-reap,
    and shutdown cannot finish before the worker's own cleanup does.

    Catches: the shield-removal mutant, i.e. `_cancel_and_reap`'s body as it
    was before this fix -- a bare `await asyncio.gather(*handles,
    return_exceptions=True)` with no shield, no loop, and no re-raise. Under
    that shape, cancelling the `shutdown` task cancels the `_GatheringFuture`
    directly, which re-cancels every unfinished child -- the worker's
    `await release.wait()` raises `CancelledError` there instead of
    returning normally, so its handler aborts and `"worker-cleanup-done"` is
    never appended.
    """
    _init_tmp_db(tmp_path)
    order, entered, release, stopped = _wire_gated_lifespan(monkeypatch)
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        shutdown = asyncio.create_task(cm.__aexit__(None, None, None))
        await entered.wait()
        shutdown.cancel()
        await asyncio.sleep(0)
        assert order == []
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await shutdown
        order.append("shutdown-returned")
        assert order == ["worker-cleanup-done", "shutdown-returned"]

    asyncio.run(_run())
    assert stopped == [], "the default hard-kill stop_process must never fire"


def test_outcome_report_survives_second_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A sibling failure discovered before shutdown even starts must still
    be reported by name after a second cancellation lands mid-reap.

    Catches: the same shield-removal mutant as
    `test_second_cancellation_is_shielded_from_children`, through a
    different mechanism -- under it, the second `.cancel()` makes the bare
    `await gather(...)` raise instead of returning the results list, so the
    reporting loop below it never runs and zero records are emitted. Also
    catches a "return early once cancelled" mutant that skips the report on
    any path where a cancellation was observed.
    """
    _init_tmp_db(tmp_path)
    order, entered, release, stopped = _wire_gated_lifespan(monkeypatch)
    sweeper_reached_raise = asyncio.Event()

    async def _crashing_sweeper() -> None:
        sweeper_reached_raise.set()
        raise RuntimeError("sweeper boom")

    # Reused from _wire_gated_lifespan except the sweeper, which must crash
    # with a stored outcome BEFORE shutdown starts -- otherwise
    # `_cancel_and_reap`'s up-front `.cancel()` would land on a not-yet-
    # started task and it would finish CANCELLED instead of with a reportable
    # RuntimeError, silently destroying this test's construction.
    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper", _crashing_sweeper
    )
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        await sweeper_reached_raise.wait()
        shutdown = asyncio.create_task(cm.__aexit__(None, None, None))
        await entered.wait()
        shutdown.cancel()
        release.set()
        with (
            caplog.at_level(logging.ERROR, logger="wxverify.api.app"),
            pytest.raises(asyncio.CancelledError),
        ):
            await shutdown
        # Pinned by logger name, not just message: `read_cache.py:407` emits
        # the colliding literal "read-cache warm failed" from inside the
        # warm's own contract-violation path, under a different logger.
        recs = [
            r
            for r in caplog.records
            if r.name == "wxverify.api.app"
            and r.getMessage() == "shutdown: export sweeper failed"
        ]
        assert len(recs) == 1, [r.getMessage() for r in caplog.records]
        exc = recs[0].exc_info
        assert exc is not None and isinstance(exc[1], RuntimeError)
        assert str(exc[1]) == "sweeper boom"

    asyncio.run(_run())
    assert stopped == [], "the default hard-kill stop_process must never fire"


def test_second_cancellation_is_deferred_not_swallowed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The cancellation IS re-raised once the reap finishes -- it is
    deferred, never swallowed.

    Only the `pytest.raises(asyncio.CancelledError)` line below is a
    preservation invariant: it is already green against the code as it
    was BEFORE this change, because a bare `await gather(...)` also
    propagates a `CancelledError` delivered to the awaiting task. The
    `order` assertion is NOT -- pre-fix, the second cancel reaches the
    worker at `release.wait()`, its handler never appends, and this
    test fails. So the test as a whole IS red against the pre-fix
    helper, and `test_second_cancellation_is_shielded_from_children`
    subsumes both of its assertions. What it adds over that test is the
    interleaving: the cancel and the release land in the same
    synchronous turn, before the shutdown task has resumed even once.

    Catches: deleting `if cancelled is not None: raise cancelled` (the
    `__aexit__` task then completes normally instead of cancelled --
    `Task.cancelled()` is `False` because `_must_cancel` was consumed at
    delivery -- so `pytest.raises` below fails). Also catches an
    `uncancel()`-instead-of-re-raise mutant by the identical assertion.
    """
    _init_tmp_db(tmp_path)
    order, entered, release, stopped = _wire_gated_lifespan(monkeypatch)
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        shutdown = asyncio.create_task(cm.__aexit__(None, None, None))
        await entered.wait()
        shutdown.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await shutdown
        assert order == ["worker-cleanup-done"]

    asyncio.run(_run())
    assert stopped == [], "the default hard-kill stop_process must never fire"


@pytest.mark.parametrize("cancel_count", [1, 2])
def test_shielded_reap_survives_repeated_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancel_count: int
) -> None:
    """The shield holds across N cancellations, not just the first.

    Catches: the partial-fix mutant `try: await asyncio.shield(reap) except
    asyncio.CancelledError: ...` WITHOUT the enclosing `while not
    reap.done():`. Empirically (verified against a scratch mutant, not just
    theorized) that mutant falls straight through to `reap.result()` the
    moment ONE cancellation is caught, and this harness's worker stub is
    still deliberately parked on `release.wait()` at that point -- so
    `reap.result()` raises `InvalidStateError` on the very first cancel,
    before a second is ever issued. Both parametrized cases (`cancel_count`
    1 and 2) therefore kill this mutant at the same divergence point for the
    same reason; they are not two different discriminators here.
    `test_second_cancellation_is_shielded_from_children` kills that same
    mutant at that same `reap.result()`, so this test is not its only
    pin; and no construction can restore a 1-vs-2 split against it,
    because the mutant reaches `reap.result()` in the same synchronous
    step in which it catches the cancellation, while a `reap` that is
    already done makes `asyncio.shield` return the inner future
    outright, leaving no await at which a cancellation could be
    delivered. The parametrize is kept because it is still the direct expression of the
    property the `while` loop provides -- "retry across N cancels, not just
    one" -- and this is no longer hypothetical: a single-retry mutant
    (`try: await asyncio.shield(reap) except CancelledError: cancelled =
    exc; await asyncio.shield(reap)`, i.e. one manual retry instead of the
    `while` loop, so it DOES reach `reap.result()` only once `reap` is
    actually done rather than mid-cancel) was built and measured, not
    predicted. `cancel_count=1` is the matched control and PASSES against
    it; `cancel_count=2` FAILS against it, with `order ==
    ["shutdown-returned"]` instead of `["worker-cleanup-done",
    "shutdown-returned"]` -- the second cancel outruns the retry's own
    reap. `cancel_count=2` is therefore this test's discriminator for that
    mutant, distinct from the shield-removal mutant both cases catch
    identically above.
    """
    _init_tmp_db(tmp_path)
    order, entered, release, stopped = _wire_gated_lifespan(monkeypatch)
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        shutdown = asyncio.create_task(cm.__aexit__(None, None, None))
        await entered.wait()
        for _ in range(cancel_count):
            shutdown.cancel()
            await asyncio.sleep(0)
        assert order == []
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await shutdown
        order.append("shutdown-returned")
        assert order == ["worker-cleanup-done", "shutdown-returned"]

    asyncio.run(_run())
    assert stopped == [], "the default hard-kill stop_process must never fire"


def test_cancel_and_reap_with_no_tasks_is_a_noop(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """`_cancel_and_reap([])` completes, returns `None`, and emits no
    `wxverify.api.app` records.

    Shape guard, not a shielding discriminator: `gather()` with zero
    arguments returns an already-resolved plain future rather than a
    `_GatheringFuture`, so `reap.done()` is `True` immediately and the
    shield loop's body never runs -- this test cannot tell a shielded reap
    from an unshielded one. It exists only to catch a `zip(...,
    strict=True)` mis-pairing regression and any rewrite that indexes
    `handles[0]` or awaits unconditionally in a way that raises on an empty
    task list.
    """

    async def _run() -> None:
        result = await _cancel_and_reap([])
        assert result is None

    with caplog.at_level(logging.ERROR, logger="wxverify.api.app"):
        asyncio.run(_run())
    assert not any(r.name == "wxverify.api.app" for r in caplog.records)
