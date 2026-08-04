"""Tests for the 0.8.9 graceful-shutdown job-reclaim fix (plan §2.4).

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
from wxverify.api.app import create_app
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
    """
    _init_tmp_db(tmp_path)

    block = asyncio.Event()

    async def _blocked_dispatch(_db: Any, _writer: Any, _job: Any) -> None:
        await block.wait()

    monkeypatch.setattr("wxverify.worker.processor.dispatch", _blocked_dispatch)

    stopped: list[None] = []
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))
    with TestClient(app) as client:
        conn = get_db()._conn  # noqa: SLF001 - fresh conn opened by lifespan startup
        job_id = _insert_job(conn)
        deadline = time.monotonic() + 2.0
        while _job_status(conn, job_id) != "running":
            if time.monotonic() > deadline:
                raise TimeoutError("job never reached status=running")
            time.sleep(0.02)
        del client  # unused past this point; the `with` block drives shutdown
    # Exiting the `with` block above drove ASGI lifespan shutdown through the
    # real `finally:` (`worker.cancel()` + `await worker`). `lifespan()` never
    # closes the DB connection, so `conn` is still valid to inspect here.
    row = conn.execute(
        "SELECT status, next_attempt_at FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    assert row is not None
    assert row["status"] == "pending"
    assert stopped == [], "the default hard-kill stop_process must never fire"


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
