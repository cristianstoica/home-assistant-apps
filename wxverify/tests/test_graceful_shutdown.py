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
import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from wxverify import config
from wxverify.api.app import _cancel_and_reap, create_app, lifespan
from wxverify.collection.budget import is_refundable_transport_error
from wxverify.db.connection import Database, close_db, get_db, init_db
from wxverify.db.queue import (
    claim_next_current_obs_job,
    claim_next_job,
    reclaim_all_stale,
)
from wxverify.obs.pws_adapter import ProviderDeadlineExceeded
from wxverify.worker.processor import (
    _shutdown_reclaim,  # noqa: PLC2701
    run_claimed_job,
    run_worker,
)
from wxverify.worker.station_pacing import weathercom_call_lock

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


async def _await_task_done(task: asyncio.Task[Any], *, timeout: float = 5.0) -> None:
    """Bound a completion wait for a task that may absorb cancellation.

    ``asyncio.wait_for(task, timeout=...)`` is not sufficient here: cancelling
    the ``wait_for`` itself does not stop ``task`` when ``task`` catches and
    holds through its own ``CancelledError`` (e.g. behind a shield) -- the
    wait_for would raise on schedule but the task keeps running unobserved.
    Instead, observe completion with a bounded ``asyncio.wait`` first; only
    once ``task`` is actually done does the caller ``await`` it (which
    returns/raises immediately, no further blocking). On a genuine hang,
    cancel ``task`` and wait up to 1s more for it before failing; a task
    that still absorbs the cancel (e.g. behind a shield) can remain
    pending after that.
    """
    done, pending = await asyncio.wait({task}, timeout=timeout)
    if pending:
        task.cancel()
        with contextlib.suppress(BaseException):
            await asyncio.wait({task}, timeout=1.0)
        raise AssertionError(f"task did not complete within {timeout}s")
    assert task in done


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


# ---------------------------------------------------------------------------
# DB pool close-on-shutdown: `Database.close_if_idle()` and
# `_shutdown_database`. The nested `finally` at `lifespan`'s shutdown --
# `try: await _cancel_and_reap(tasks) / finally: _shutdown_database(db)` --
# closes the six connections only after the reap's own DB write has
# committed, and only while `close_if_idle`'s two predicates both read idle.
# ---------------------------------------------------------------------------


def test_close_runs_only_after_the_reaps_write_commits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The pool closes only after the reap's own write has committed --
    direct sequence proof.

    A worker stub's ``except asyncio.CancelledError`` handler performs a
    REAL ``db.write`` before re-raising, and ``Database.close_if_idle`` is
    wrapped with a recorder, so one ``order`` list observes both events in
    the sequence they actually happen in, not merely that both happened.

    Catches: M1 (the close hoisted above the reap). Under that ordering the
    handler's own write lands against an already-closed database and
    raises ``sqlite3.ProgrammingError`` from inside its own
    ``except asyncio.CancelledError`` handler, so ``order`` never gains
    ``"reap-write-committed"`` at all -- and `_cancel_and_reap` separately
    reports the worker as failed, a second independent signal not asserted
    here but visible in the mutant's own terminal output.
    """
    conn = _init_tmp_db(tmp_path)
    job_id = _insert_job(conn)

    order: list[str] = []

    async def _worker_stub(*_args: object) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await get_db().write(
                lambda c: c.execute(
                    "UPDATE jobs SET status = 'completed' WHERE id = ?", (job_id,)
                )
            )
            order.append("reap-write-committed")
            raise

    monkeypatch.setattr("wxverify.api.app.run_worker", _worker_stub)
    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper",
        _make_immediate_stub("export sweeper"),
    )
    monkeypatch.setattr(
        "wxverify.api.app.warm_read_cache", _make_immediate_stub("read-cache warm")
    )

    async def _noop_discovery(_port: int) -> None:
        # The leading `await asyncio.sleep(0)` is not padding: `create_task`
        # only SCHEDULES a task via `call_soon`, it does not run it. Without
        # an intervening scheduling turn here, the worker stub's cancel
        # lands before its coroutine has ever been stepped, and Python
        # delivers `CancelledError` at the very top of the coroutine --
        # before it ever enters its own `try` -- so `_worker_stub`'s
        # `except` branch would never run, silently destroying this test's
        # ability to observe the write at all (verified empirically:
        # dropping this line collapses `order` to `["db-closed"]`, matching
        # `test_startup_failure_after_task_creation_still_reaps_every_task`'s
        # identical finding for the same reason).
        await asyncio.sleep(0)

    monkeypatch.setattr("wxverify.api.app.publish_discovery", _noop_discovery)

    real_close_if_idle = Database.close_if_idle

    def _recording_close_if_idle(self: Database) -> bool:
        result = real_close_if_idle(self)
        order.append("db-closed" if result else "db-close-skipped")
        return result

    monkeypatch.setattr(Database, "close_if_idle", _recording_close_if_idle)

    stopped: list[None] = []
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        db = get_db()
        with caplog.at_level(logging.INFO, logger="wxverify.api.app"):
            await cm.__aexit__(None, None, None)
        # (a) Ordering, asserted with no await since __aexit__ returned.
        assert order == ["reap-write-committed", "db-closed"]
        # (b) The pool is genuinely gone: a post-shutdown read fails loudly.
        with pytest.raises(sqlite3.ProgrammingError):
            await db.read(lambda c: c.execute("SELECT 1").fetchone())
        # (d)/(e): D5's logging contract -- NOT M1 discriminators. Both hold
        # under the pre-fix ordering too (the close still runs, merely too
        # early), so they pin the log shape, not the sequence.
        info_records = [
            r
            for r in caplog.records
            if r.name == "wxverify.api.app" and r.levelno == logging.INFO
        ]
        closed_records = [
            r for r in info_records if r.getMessage() == "database closed"
        ]
        assert len(closed_records) == 1, [r.getMessage() for r in caplog.records]
        skip_records = [
            r
            for r in caplog.records
            if r.name == "wxverify.api.app"
            and r.getMessage().startswith("close skipped: ")
        ]
        assert skip_records == []

    asyncio.run(_run())
    assert stopped == [], "the default hard-kill stop_process must never fire"

    # (c) After asyncio.run returns, a FRESH side connection shows the
    # handler's row change on disk.
    side = sqlite3.connect(config.db_path)
    try:
        row = side.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
        assert row is not None and row[0] == "completed"
    finally:
        side.close()


def test_lifespan_shutdown_closes_the_pool_after_reclaim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Same ordering as the previous oracle, through the real production
    path: real ``run_worker``, a real claimed job, a real ``TestClient``
    lifespan drive.

    Never issues a page or dashboard request inside the ``TestClient``
    block: those routes arm the fire-and-forget rescore task
    (``schedule_score_rescore``) whenever the composite reads ``stale`` or
    ``rebuilding`` -- an unowned writer racing shutdown, and exactly the
    flake this oracle must not carry.

    Catches: M1, by consequence rather than instrumentation. With the close
    hoisted above the reap, ``run_worker``'s cancellation handler writes
    against an already-closed database, so the reclaim never lands: the job
    row stays ``running`` and the reap additionally logs
    ``"shutdown reclaim failed"``. Closing the side connection BEFORE
    checking for the WAL/SHM sidecars does not discriminate M1 -- that
    mutant's close still runs, merely too early, so it removes the
    sidecars all the same; what it catches is a close that is called but
    ineffective (refused, or one connection missed).
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
        with (
            caplog.at_level(logging.WARNING, logger="wxverify.worker.processor"),
            TestClient(app) as client,
        ):
            job_id = _insert_job(side)
            deadline = time.monotonic() + 2.0
            while _job_status(side, job_id) != "running":
                if time.monotonic() > deadline:
                    raise TimeoutError("job never reached status=running")
                time.sleep(0.02)
            del client  # unused past this point; the `with` drives shutdown
        row = side.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
        assert row is not None
        assert row["status"] == "pending"
        reclaim_failed = [
            r
            for r in caplog.records
            if r.name == "wxverify.worker.processor"
            and "shutdown reclaim failed" in r.getMessage()
        ]
        assert reclaim_failed == [], [r.getMessage() for r in caplog.records]
        assert stopped == [], "the default hard-kill stop_process must never fire"
    finally:
        # Closing the side connection FIRST is this assertion's own
        # precondition, not incidental: SQLite only checkpoints and removes
        # the WAL/SHM sidecars on the LAST connection close, so a still-open
        # side connection would keep them alive regardless of what the
        # process database did.
        side.close()
    wal_path = Path(f"{config.db_path}-wal")
    shm_path = Path(f"{config.db_path}-shm")
    assert not wal_path.exists()
    assert not shm_path.exists()


def test_close_if_idle_refuses_while_a_reader_is_checked_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The guard refuses while a pooled reader is checked out.

    Parks a real ``db.read`` on a callback that blocks on a
    ``threading.Event`` INSIDE the executor thread, before it issues any
    SQL, so an unconditional-close mutant here closes an IDLE connection,
    never one mid-statement.

    Catches: M3 (``close_if_idle`` replaced by ``close()`` + ``return
    True``). The kill is the ``assert db.close_if_idle() is False`` line
    itself raising ``AssertionError`` while the read is still parked --
    never a crash: a SIGSEGV under this construction would mean the
    construction itself is wrong (SQL issued before the signal, or the
    block released early), not that the mutant was killed.
    """
    _init_tmp_db(tmp_path)
    db = get_db()

    entered = threading.Event()
    release = threading.Event()

    def _blocking_read(conn: sqlite3.Connection) -> int:
        entered.set()
        release.wait()
        row = conn.execute("SELECT 1").fetchone()
        return int(row[0])

    async def _run() -> None:
        task = asyncio.create_task(db.read(_blocking_read))
        while not entered.is_set():
            await asyncio.sleep(0.01)
        result: int | None = None
        try:
            with caplog.at_level(logging.WARNING, logger="wxverify.db.connection"):
                assert db.close_if_idle() is False
            # Match on the "close skipped: " prefix, never on "one WARNING
            # from this logger": a parked read easily exceeds
            # SLOW_READ_MS, so a "slow db read" WARNING from the same
            # logger is expected alongside it once the read completes.
            skip_records = [
                r
                for r in caplog.records
                if r.name == "wxverify.db.connection"
                and r.getMessage().startswith("close skipped: ")
            ]
            assert len(skip_records) == 1, [r.getMessage() for r in caplog.records]
            assert (
                skip_records[0].getMessage()
                == "close skipped: 1 of 4 pooled readers checked out"
            )
        finally:
            # Reap the parked task in the SAME try/finally: without this,
            # run_to_completion keeps the executor thread alive underneath
            # asyncio.run's own teardown, and a failing assertion above
            # would hang the run instead of reporting the failure.
            release.set()
            with contextlib.suppress(Exception):
                result = await task
        assert result == 1, "the read must return its real value unharmed"
        assert db.close_if_idle() is True

    asyncio.run(_run())


def test_close_if_idle_refuses_while_a_write_is_in_flight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The guard refuses while a write is in flight -- the paired predicate
    to the previous oracle's pool check. The two paired message tails
    (``"1 of 4 pooled readers checked out"`` here vs. ``"a write is in
    flight"``) together prove WHICH predicate refused, not merely that one
    did.

    Catches: M4 (the pool-only guard, dropping the ``_write_lock.locked()``
    check). Same shape as the read oracle: the kill is
    ``assert db.close_if_idle() is False`` raising ``AssertionError`` while
    the write is still parked, never a crash. This predicate is also
    ``replace_from``'s backstop, since an import swap holds ``_write_lock``
    across its whole body -- but in production a close during a swap is
    unreachable for an independent reason (uvicorn drains the ``POST
    /import`` request before lifespan shutdown starts), so this oracle
    proves the backstop, not the unreachability.
    """
    _init_tmp_db(tmp_path)
    db = get_db()

    entered = threading.Event()
    release = threading.Event()

    def _blocking_write(conn: sqlite3.Connection) -> int:
        entered.set()
        release.wait()
        row = conn.execute("SELECT 1").fetchone()
        return int(row[0])

    async def _run() -> None:
        task = asyncio.create_task(db.write(_blocking_write))
        while not entered.is_set():
            await asyncio.sleep(0.01)
        result: int | None = None
        try:
            with caplog.at_level(logging.WARNING, logger="wxverify.db.connection"):
                assert db.close_if_idle() is False
            skip_records = [
                r
                for r in caplog.records
                if r.name == "wxverify.db.connection"
                and r.getMessage().startswith("close skipped: ")
            ]
            assert len(skip_records) == 1, [r.getMessage() for r in caplog.records]
            assert skip_records[0].getMessage() == "close skipped: a write is in flight"
        finally:
            release.set()
            with contextlib.suppress(Exception):
                result = await task
        assert result == 1, "the write must return its real value unharmed"
        assert db.close_if_idle() is True

    asyncio.run(_run())


def test_close_if_idle_and_close_db_are_idempotent(tmp_path: Path) -> None:
    """Double close is a no-op -- the invariant ~70 existing tests already
    depend on via their trailing ``close_db()`` after a ``TestClient``
    block.

    Catches: a ``_closed``-flag implementation that raises or returns
    ``False`` on a second call, which would make every one of those
    trailing calls newly meaningful (and newly failing).
    """
    _init_tmp_db(tmp_path)
    db = get_db()
    assert db.close_if_idle() is True
    assert db.close_if_idle() is True
    db.close()
    close_db()


def test_close_survives_the_reaps_deferred_re_raise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The close survives the reap's deferred re-raise: the nested
    ``finally`` runs even though ``_cancel_and_reap`` re-raises the
    ``CancelledError`` it deferred.

    Catches: M2 (close written as a plain statement AFTER
    ``await _cancel_and_reap(tasks)`` instead of in a nested ``finally``).
    That mutant passes the two ordering oracles above -- the reap always
    finishes and always returns normally there -- and fails only here,
    where the reap's own re-raise skips a bare follow-on statement
    entirely.
    """
    _init_tmp_db(tmp_path)
    order, entered, release, stopped = _wire_gated_lifespan(monkeypatch)

    real_close_if_idle = Database.close_if_idle

    def _recording_close_if_idle(self: Database) -> bool:
        result = real_close_if_idle(self)
        order.append("db-closed" if result else "db-close-skipped")
        return result

    monkeypatch.setattr(Database, "close_if_idle", _recording_close_if_idle)

    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        db = get_db()
        shutdown = asyncio.create_task(cm.__aexit__(None, None, None))
        await entered.wait()
        shutdown.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await shutdown
        assert order == ["worker-cleanup-done", "db-closed"]
        with pytest.raises(sqlite3.ProgrammingError):
            await db.read(lambda c: c.execute("SELECT 1").fetchone())

    asyncio.run(_run())
    assert stopped == [], "the default hard-kill stop_process must never fire"


def test_close_failure_reported_not_propagated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A close failure is reported, never propagated, and is not named
    like a task failure.

    Catches: M5 (dropping ``_shutdown_database``'s ``try/except``), and any
    rename of the ERROR record into the ``"shutdown: "`` family, which
    would silently redefine ``tests/test_m1_m5.py``'s complete-set
    assertion on that prefix.
    """
    _init_tmp_db(tmp_path)

    monkeypatch.setattr("wxverify.api.app.run_worker", _make_immediate_stub("worker"))
    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper",
        _make_immediate_stub("export sweeper"),
    )
    monkeypatch.setattr(
        "wxverify.api.app.warm_read_cache", _make_immediate_stub("read-cache warm")
    )

    async def _noop_discovery(_port: int) -> None:
        return None

    monkeypatch.setattr("wxverify.api.app.publish_discovery", _noop_discovery)

    def _raising_close_if_idle(self: Database) -> bool:
        raise sqlite3.OperationalError("boom")

    monkeypatch.setattr(Database, "close_if_idle", _raising_close_if_idle)

    stopped: list[None] = []
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        with caplog.at_level(logging.ERROR, logger="wxverify.api.app"):
            await cm.__aexit__(None, None, None)
        error_records = [
            r
            for r in caplog.records
            if r.name == "wxverify.api.app" and r.levelno == logging.ERROR
        ]
        close_failed = [
            r
            for r in error_records
            if r.getMessage() == "database close failed at shutdown"
        ]
        assert len(close_failed) == 1, [r.getMessage() for r in error_records]
        exc_info = close_failed[0].exc_info
        assert exc_info is not None
        assert isinstance(exc_info[1], sqlite3.OperationalError)
        assert not any(r.getMessage().startswith("shutdown: ") for r in error_records)

    asyncio.run(_run())
    assert stopped == [], "the default hard-kill stop_process must never fire"


def test_startup_failure_after_task_creation_still_closes_the_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A startup failure AFTER all three tasks exist must still close the
    database, not merely reap the tasks -- and pins F6's boundary: this is
    the failure case that IS covered, since it fires after the
    task-creation ``try`` opens.

    Catches: a close reachable only on the clean-shutdown path -- e.g. one
    hung off the ``yield``'s own return instead of the outer ``finally``.
    """
    _init_tmp_db(tmp_path)
    reaped: list[str] = []
    order: list[str] = []

    def _make_stub(name: str):
        async def _stub(*_args: object) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
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

    real_close_if_idle = Database.close_if_idle

    def _recording_close_if_idle(self: Database) -> bool:
        result = real_close_if_idle(self)
        order.append("db-closed" if result else "db-close-skipped")
        return result

    monkeypatch.setattr(Database, "close_if_idle", _recording_close_if_idle)

    async def _boom(_port: int) -> None:
        await asyncio.sleep(0)
        raise RuntimeError("discovery boom")

    monkeypatch.setattr("wxverify.api.app.publish_discovery", _boom)

    stopped: list[None] = []
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))

    async def _run() -> None:
        cm = lifespan(app)
        with pytest.raises(RuntimeError, match="discovery boom"):
            await cm.__aenter__()
        assert sorted(reaped) == ["export sweeper", "read-cache warm", "worker"]
        assert order == ["db-closed"]

    asyncio.run(_run())
    assert stopped == [], "the default hard-kill stop_process must never fire"


# ---------------------------------------------------------------------------
# D1.2 two-lane supervisor contracts (plan §6.3.1, §14.2 C1-C5 and the two
# further supervisor cases) plus the app-level stop test and DL4/DL5 (the
# provider-call deadline observed through the history lane and under
# cancellation). `run_worker` now supervises two lanes -- `_run_main_lane`
# and `run_current_obs_poller` -- via `asyncio.wait(FIRST_COMPLETED)` +
# `_cancel_and_drain` + `_shutdown_reclaim` + `_raise_lane_outcome`, never an
# `asyncio.TaskGroup` (which would wrap a lane's exception in an
# ExceptionGroup). C1/C2/the two further cases/the app-level stop test drive
# `run_worker` with both lanes replaced by fakes (patched at their
# `wxverify.worker.processor` import-site attributes, the same seam
# `idle_current_obs_poller` uses elsewhere); C3's plain-cancel/interrupted-
# drain cases do the same; C3's repeated-cancel case unit-tests
# `_shutdown_reclaim` directly; C4's stale-generation case and DL4/DL5 drive
# a REAL tmp `Database` end to end, matching this file's existing idiom.
# ---------------------------------------------------------------------------


class _StopLoop(Exception):
    """Local stand-in exception a fake lane raises to end a test
    deterministically -- same idiom as ``test_011_patch.py``'s
    module-private ``_StopLoop``, not a shared production symbol."""


class _RecordingDb:
    """Fake ``Database``: the only method the supervisor calls on it is
    ``_shutdown_reclaim``'s ``db.write(reclaim_all_stale)``. Records call
    order into a shared list so a test can assert the reclaim landed AFTER
    both lanes' own cancellation markers, not merely that it happened."""

    def __init__(self, order: list[str] | None = None) -> None:
        self.order: list[str] = order if order is not None else []
        self.reclaim_calls = 0

    async def write(self, fn: Any) -> None:
        assert fn is reclaim_all_stale, fn
        self.reclaim_calls += 1
        self.order.append("reclaim")


def test_c1_lane_failure_raises_by_identity_after_draining_the_other(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C1 -- Failure. A lane that raises makes `run_worker` raise that same
    exception object, after the other lane is done. No reclaim runs.

    Mutant: re-raise without draining -- the other lane would still be
    pending (`other_cancelled` unset) when `run_worker` raises.
    """
    raised: dict[str, BaseException] = {}
    other_cancelled = asyncio.Event()

    async def _failing_main(_db: Any) -> None:
        exc = _StopLoop("boom")
        raised["main"] = exc
        raise exc

    async def _blocking_poller(_db: Any, *, run_job: Any) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            other_cancelled.set()
            raise

    monkeypatch.setattr("wxverify.worker.processor._run_main_lane", _failing_main)
    monkeypatch.setattr(
        "wxverify.worker.processor.run_current_obs_poller", _blocking_poller
    )
    db = _RecordingDb()

    async def _run() -> None:
        with pytest.raises(_StopLoop) as excinfo:
            await run_worker(db)  # type: ignore[arg-type]
        assert excinfo.value is raised["main"]
        assert other_cancelled.is_set()
        assert db.reclaim_calls == 0

    asyncio.run(_run())


def test_c2_repeated_cancellation_still_drains_both_lanes_before_raising(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C2 -- Cancellation. Cancelling `run_worker` any number of times ends
    with both lanes done before `CancelledError` leaves it.

    Mutant: a single `await asyncio.wait(...)` in place of `_cancel_and_drain`'s
    loop -- the SECOND cancel would land inside that lone `asyncio.wait` and
    propagate `CancelledError` straight out, skipping `_shutdown_reclaim` and
    leaving `done` at `{"main": False, "poller": False}` when `run_worker`
    finishes.
    """
    gate = asyncio.Event()
    done = {"main": False, "poller": False}

    async def _gated_main(_db: Any) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await gate.wait()
            done["main"] = True
            raise

    async def _gated_poller(_db: Any, *, run_job: Any) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await gate.wait()
            done["poller"] = True
            raise

    monkeypatch.setattr("wxverify.worker.processor._run_main_lane", _gated_main)
    monkeypatch.setattr(
        "wxverify.worker.processor.run_current_obs_poller", _gated_poller
    )
    db = _RecordingDb()

    async def _run() -> None:
        task = asyncio.create_task(run_worker(db))  # type: ignore[arg-type]
        # Let run_worker create both lane tasks and reach `await
        # asyncio.wait(lanes, FIRST_COMPLETED)` before the first cancel.
        await asyncio.sleep(0)
        try:
            for _ in range(3):
                task.cancel()
                await asyncio.sleep(0)
            assert done == {"main": False, "poller": False}, (
                "both lanes must still be parked on the shut gate"
            )
            assert not task.done(), (
                "run_worker must not finish before both lanes are done -- a "
                "single asyncio.wait in place of _cancel_and_drain's loop "
                "would let the second cancel escape and finish run_worker "
                "while both lanes are still parked on the gate"
            )
        finally:
            gate.set()
        await _await_task_done(task)
        with pytest.raises(asyncio.CancelledError):
            await task
        assert done == {"main": True, "poller": True}

    asyncio.run(_run())


def test_c3_plain_cancel_reclaims_exactly_once_after_both_lanes_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C3 -- Shutdown and reclaim, plain-cancel case. Exactly one
    `reclaim_all_stale` runs on a cancellation, after both lanes are done.

    Mutant: reclaim before the drain -- the "reclaim" marker would appear
    before one or both of the lane-cancelled markers in `order`.
    """
    order: list[str] = []

    async def _idle_main(_db: Any) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            order.append("main-cancelled")
            raise

    async def _idle_poller(_db: Any, *, run_job: Any) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            order.append("poller-cancelled")
            raise

    monkeypatch.setattr("wxverify.worker.processor._run_main_lane", _idle_main)
    monkeypatch.setattr(
        "wxverify.worker.processor.run_current_obs_poller", _idle_poller
    )
    db = _RecordingDb(order)

    async def _run() -> None:
        task = asyncio.create_task(run_worker(db))  # type: ignore[arg-type]
        await asyncio.sleep(0)
        task.cancel()
        await _await_task_done(task)
        with pytest.raises(asyncio.CancelledError):
            await task
        assert order[-1] == "reclaim"
        assert set(order[:-1]) == {"main-cancelled", "poller-cancelled"}
        assert db.reclaim_calls == 1

    asyncio.run(_run())


def test_c3_cancel_during_a_failure_drain_logs_reclaims_once_and_raises_cancelled(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """C3 -- Shutdown and reclaim, interrupted-drain case. A cancel that
    arrives while a lane failure is being drained logs the failure, reclaims
    once, and raises `CancelledError` (not the failure).

    Mutant: drop `_log_lane_failures` from the interrupted path -- no ERROR
    record would be emitted for the main lane's failure.
    """
    order: list[str] = []
    main_exc = RuntimeError("main boom")

    async def _failing_main(_db: Any) -> None:
        raise main_exc

    gate = asyncio.Event()
    poller_cancel_entered = asyncio.Event()

    async def _gated_poller(_db: Any, *, run_job: Any) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            poller_cancel_entered.set()
            await gate.wait()
            order.append("poller-cancelled")
            raise

    monkeypatch.setattr("wxverify.worker.processor._run_main_lane", _failing_main)
    monkeypatch.setattr(
        "wxverify.worker.processor.run_current_obs_poller", _gated_poller
    )
    db = _RecordingDb(order)

    async def _run() -> None:
        task = asyncio.create_task(run_worker(db))  # type: ignore[arg-type]
        # Rendezvous, not a sleep: this fires only once run_worker's own
        # `_cancel_and_drain(lanes)` (the non-except, lane-failure branch) has
        # already cancelled the poller and is awaiting it -- exactly the
        # window "a cancel arrives while a lane failure is being drained".
        try:
            await asyncio.wait_for(poller_cancel_entered.wait(), timeout=2.0)
            with caplog.at_level(logging.ERROR, logger="wxverify.worker.processor"):
                task.cancel()
                await asyncio.sleep(0)
        finally:
            gate.set()
        with caplog.at_level(logging.ERROR, logger="wxverify.worker.processor"):
            await _await_task_done(task)
            with pytest.raises(asyncio.CancelledError):
                await task
        assert order == ["poller-cancelled", "reclaim"]
        assert db.reclaim_calls == 1
        failed = [
            r
            for r in caplog.records
            if r.name == "wxverify.worker.processor"
            and r.getMessage() == "worker lane worker-main-lane failed"
        ]
        assert len(failed) == 1, [r.getMessage() for r in caplog.records]
        exc_info = failed[0].exc_info
        assert exc_info is not None and exc_info[1] is main_exc

    asyncio.run(_run())


def test_c3_repeated_cancel_reclaims_exactly_once_under_a_shielded_write() -> None:
    """C3 (repeated-cancel, code-review finding): `_shutdown_reclaim`'s
    single retained write task stays shielded across N cancellations of the
    caller -- the write is issued exactly once and finishes, however many
    times the awaiting task is cancelled while it is still in flight.

    Mutants: (a) a bare, unshielded `await db.write(reclaim_all_stale)` in
    place of the shield loop -- the first cancel would cancel the write's
    own task directly, so it would never reach `release.wait()` a second
    time and `write_calls` would never advance past being cancelled
    mid-write; (b) re-issuing the write on each absorbed cancel instead of
    retaining the one task -- `write_calls` would rise to 3 (one per
    cancel), not stay at 1.
    """
    entered = asyncio.Event()
    release = asyncio.Event()
    write_calls = 0

    class _SlowReclaimDb:
        async def write(self, fn: Any) -> None:
            nonlocal write_calls
            assert fn is reclaim_all_stale, fn
            write_calls += 1
            entered.set()
            await release.wait()

    db = _SlowReclaimDb()

    async def _run() -> None:
        nonlocal write_calls
        task = asyncio.create_task(_shutdown_reclaim(db))  # type: ignore[arg-type]
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        try:
            for _ in range(3):
                task.cancel()
                await asyncio.sleep(0)
            assert not task.done(), "the shield must keep the caller loop alive"
            assert write_calls == 1
        finally:
            release.set()
        await _await_task_done(task)
        assert task.done() and not task.cancelled()
        assert write_calls == 1

    asyncio.run(_run())


def test_both_lanes_raise_main_precedence_poller_logged_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Both lanes raise: `run_worker` raises main's exception object; the
    poller's is logged exactly once at ERROR with `exc_info` carrying that
    object.

    Mutant: poller-first precedence -- `excinfo.value is main_exc` would
    fail (poller_exc would be raised instead), and the logged failure would
    name the main lane instead of the poller.
    """
    main_exc = RuntimeError("main boom")
    poller_exc = RuntimeError("poller boom")

    async def _failing_main(_db: Any) -> None:
        raise main_exc

    async def _failing_poller(_db: Any, *, run_job: Any) -> None:
        raise poller_exc

    monkeypatch.setattr("wxverify.worker.processor._run_main_lane", _failing_main)
    monkeypatch.setattr(
        "wxverify.worker.processor.run_current_obs_poller", _failing_poller
    )
    db = _RecordingDb()

    async def _run() -> None:
        with (
            caplog.at_level(logging.ERROR, logger="wxverify.worker.processor"),
            pytest.raises(RuntimeError) as excinfo,
        ):
            await run_worker(db)  # type: ignore[arg-type]
        assert excinfo.value is main_exc
        error_records = [
            r
            for r in caplog.records
            if r.name == "wxverify.worker.processor" and r.levelno == logging.ERROR
        ]
        poller_records = [
            r
            for r in error_records
            if r.getMessage() == "worker lane worker-current-obs-lane failed"
        ]
        assert len(poller_records) == 1, [r.getMessage() for r in error_records]
        exc_info = poller_records[0].exc_info
        assert exc_info is not None and exc_info[1] is poller_exc
        assert db.reclaim_calls == 0

    asyncio.run(_run())


def test_a_lane_returning_normally_raises_a_named_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lane returns normally: `RuntimeError` whose message names the lane.

    Mutant: treat a normal return as success, so `run_worker` returns
    `None` -- `pytest.raises(RuntimeError)` would fail with no exception
    raised at all.
    """

    async def _idle_main(_db: Any) -> None:
        await asyncio.Event().wait()

    async def _returning_poller(_db: Any, *, run_job: Any) -> None:
        return None

    monkeypatch.setattr("wxverify.worker.processor._run_main_lane", _idle_main)
    monkeypatch.setattr(
        "wxverify.worker.processor.run_current_obs_poller", _returning_poller
    )
    db = _RecordingDb()

    async def _run() -> None:
        with pytest.raises(
            RuntimeError, match="worker-current-obs-lane returned unexpectedly"
        ):
            await run_worker(db)  # type: ignore[arg-type]

    asyncio.run(_run())


def test_app_level_stop_reports_the_failing_lanes_exception_by_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """App-level stop: a lane failure surfaces through the real ASGI wiring
    -- `_stop_on_worker_done` (wxverify.api.app) reports the FAILING lane's
    exception object, never the supervisor's own machinery.

    Mutants: a supervisor that awaits only the main lane -- it would never
    observe the poller's failure, so `stopped` would stay empty and the
    deadline loop below would time out; and one that wraps the lane
    exception (e.g. in a `RuntimeError`) instead of re-raising it by
    identity -- `exc_info[1] is poller_exc` would then fail.
    """
    _init_tmp_db(tmp_path)

    async def _idle_main(_db: Any) -> None:
        await asyncio.Event().wait()

    poller_exc = RuntimeError("poller boom")

    async def _failing_poller(_db: Any, *, run_job: Any) -> None:
        raise poller_exc

    monkeypatch.setattr("wxverify.worker.processor._run_main_lane", _idle_main)
    monkeypatch.setattr(
        "wxverify.worker.processor.run_current_obs_poller", _failing_poller
    )

    stopped: list[None] = []
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))
    with (
        caplog.at_level(logging.CRITICAL, logger="wxverify.api.app"),
        TestClient(app) as client,
    ):
        deadline = time.monotonic() + 5.0
        while not stopped:
            if time.monotonic() > deadline:
                raise TimeoutError("worker task crash was never reported")
            time.sleep(0.02)
        del client  # unused past this point; the with drives shutdown

    assert stopped == [None]
    crashed = [
        r
        for r in caplog.records
        if r.name == "wxverify.api.app" and r.getMessage() == "worker task crashed"
    ]
    assert len(crashed) == 1, [r.getMessage() for r in caplog.records]
    exc_info = crashed[0].exc_info
    assert exc_info is not None and exc_info[1] is poller_exc


def test_c4_stale_generation_between_reserve_and_outcome_abandons_only_that_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """C4 -- the stale-generation case: a database replacement (import) that
    lands between the current-obs lane's reserve and its outcome write
    abandons only that job, releases the call lock, and does not disturb
    the other lane's ability to keep claiming.

    Construction: `wxverify.worker.processor.fetch_current_observation` --
    the provider call that runs strictly BETWEEN the reserve write and the
    outcome write inside `_fetch_current_obs` -- bumps `db._generation` as a
    side effect before returning its canned reply, landing the swap in
    exactly that window without a real import.

    NOTE on the "ordinary failure" mutant (`run_claimed_job` letting
    `StaleGenerationError` fall into the generic `except Exception` branch
    instead of the immediate `raise`): empirically verified EQUIVALENT under
    every assertion in THIS test. `FencedWriter.write` rejects with
    `StaleGenerationError` before its callback ever runs (the generation
    check happens inside `write_fenced`, before `_call()`), so the generic
    branch's own `_fail_job` write raises the identical
    `StaleGenerationError`, which is caught by this same outer handler --
    same INFO log, same untouched job row, same released lock. The one real
    divergence is write-lock contention (the mutant's doomed write briefly
    queues on `Database._write_lock`; the immediate raise never touches it)
    -- see `test_c4_ordinary_failure_mutant_would_block_on_the_write_lock`
    below for the oracle that samples inside that window.
    """
    conn = _init_tmp_db(tmp_path)
    Path(config.options_path).write_text(
        json.dumps({"weathercom_key": "ci-placeholder"}), encoding="utf-8"
    )
    db = get_db()

    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone, "
        "enabled) VALUES ('C4 Site', 40.0, -105.0, 900.0, 'UTC', 1)"
    )
    site_id = cur.lastrowid
    assert site_id is not None
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m) "
        "VALUES (?, 'ISTATION-C4', 40.0, -105.0, 900.0)",
        (site_id,),
    )
    station_id = cur.lastrowid
    assert station_id is not None
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, status, max_retries) "
        "VALUES ('fetch_current_obs', ?, ?, ?, 'pending', 3)",
        (site_id, f"curobs:{station_id}", json.dumps({"station_id": station_id})),
    )
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, status, max_retries) "
        "VALUES ('fetch_obs', ?, 'obs', '{}', 'pending', 3)",
        (site_id,),
    )
    conn.commit()

    def _online_body() -> bytes:
        return json.dumps(
            {
                "observations": [
                    {
                        "obsTimeUtc": "2026-07-10T11:55:00Z",
                        "humidity": 50.0,
                        "winddir": 180.0,
                        "uv": 1.0,
                        "neighborhood": "Test Quarter",
                        "metric": {
                            "temp": 20.0,
                            "dewpt": 10.0,
                            "windSpeed": 5.0,
                            "windGust": 8.0,
                            "pressure": 1012.0,
                            "precipRate": 0.0,
                            "precipTotal": 0.0,
                        },
                    }
                ]
            }
        ).encode()

    async def _bump_generation_then_reply(
        pws_station_id: str,
        api_key: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 10.0,
    ) -> httpx.Response:
        db._generation += 1  # noqa: SLF001 -- simulates an import replacing the db
        return httpx.Response(
            200,
            content=_online_body(),
            headers={"content-type": "application/json"},
            request=httpx.Request(
                "GET", "https://api.weather.com/v2/pws/observations/current"
            ),
        )

    monkeypatch.setattr(
        "wxverify.worker.processor.fetch_current_observation",
        _bump_generation_then_reply,
    )

    async def _run() -> None:
        lock = weathercom_call_lock()
        job = await db.write(claim_next_current_obs_job)
        assert job is not None

        with caplog.at_level(logging.INFO, logger="wxverify.worker.processor"):
            await run_claimed_job(db, job, lane="current_obs")

        row = conn.execute("SELECT status FROM jobs WHERE id = ?", (job.id,)).fetchone()
        assert row is not None and row["status"] == "running", (
            "an abandoned job is left exactly as claimed -- no further write "
            "is safe against a generation it no longer belongs to"
        )
        assert lock.locked() is False
        abandoned = [r for r in caplog.records if "job abandoned id=" in r.getMessage()]
        assert len(abandoned) == 1, [r.getMessage() for r in caplog.records]

        # The other lane keeps claiming: the unrelated main-lane job seeded
        # above is still claimable after the current-obs lane's abandon.
        other = await db.write(claim_next_job)
        assert other is not None and other.type == "fetch_obs"

    asyncio.run(_run())


def test_c4_ordinary_failure_mutant_attempts_a_second_fenced_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C4 discriminator: exactly two fenced writes are attempted while
    abandoning a stale-generation job -- the reserve (which still matches
    the generation it was captured under) and the outcome write that
    discovers the swap and raises `StaleGenerationError`. Nothing
    downstream retries a further write against a generation the job no
    longer belongs to.

    This is the oracle that samples INSIDE the window the main C4 test
    cannot see: the "ordinary failure" mutant (letting `StaleGenerationError`
    fall into the generic `except Exception` branch instead of the immediate
    `raise`) is equivalent to the correct code at every END state the main
    C4 test asserts -- same log, same job row, same released call lock --
    because `FencedWriter.write` rejects with `StaleGenerationError` before
    its callback ever runs, so the generic branch's own `_fail_job` write
    raises the identical error, caught by the same outer handler. The
    divergence is COUNT, not outcome: the mutant issues a second, doomed
    `writer.write` call (the generic branch's `_fail_job` attempt) that the
    correct code never reaches.

    Mutant: `run_claimed_job` letting `StaleGenerationError` propagate via
    the generic branch instead of raising immediately -- `write_fenced`
    call count would be 3, not 2.
    """
    conn = _init_tmp_db(tmp_path)
    Path(config.options_path).write_text(
        json.dumps({"weathercom_key": "ci-placeholder"}), encoding="utf-8"
    )
    db = get_db()

    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone, "
        "enabled) VALUES ('C4b Site', 40.0, -105.0, 900.0, 'UTC', 1)"
    )
    site_id = cur.lastrowid
    assert site_id is not None
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m) "
        "VALUES (?, 'ISTATION-C4B', 40.0, -105.0, 900.0)",
        (site_id,),
    )
    station_id = cur.lastrowid
    assert station_id is not None
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, status, max_retries) "
        "VALUES ('fetch_current_obs', ?, ?, ?, 'pending', 3)",
        (site_id, f"curobs:{station_id}", json.dumps({"station_id": station_id})),
    )
    conn.commit()

    def _online_body() -> bytes:
        return json.dumps(
            {
                "observations": [
                    {
                        "obsTimeUtc": "2026-07-10T11:55:00Z",
                        "humidity": 50.0,
                        "winddir": 180.0,
                        "uv": 1.0,
                        "neighborhood": "Test Quarter",
                        "metric": {
                            "temp": 20.0,
                            "dewpt": 10.0,
                            "windSpeed": 5.0,
                            "windGust": 8.0,
                            "pressure": 1012.0,
                            "precipRate": 0.0,
                            "precipTotal": 0.0,
                        },
                    }
                ]
            }
        ).encode()

    async def _bump_generation_then_reply(
        pws_station_id: str,
        api_key: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 10.0,
    ) -> httpx.Response:
        db._generation += 1  # noqa: SLF001 -- simulates an import replacing the db
        return httpx.Response(
            200,
            content=_online_body(),
            headers={"content-type": "application/json"},
            request=httpx.Request(
                "GET", "https://api.weather.com/v2/pws/observations/current"
            ),
        )

    monkeypatch.setattr(
        "wxverify.worker.processor.fetch_current_observation",
        _bump_generation_then_reply,
    )

    fenced_write_calls = 0
    real_write_fenced = type(db).write_fenced

    async def _counting_write_fenced(self: Any, fn: Any, *, generation: int) -> Any:
        nonlocal fenced_write_calls
        fenced_write_calls += 1
        return await real_write_fenced(self, fn, generation=generation)

    monkeypatch.setattr(type(db), "write_fenced", _counting_write_fenced)

    async def _run() -> None:
        job = await db.write(claim_next_current_obs_job)
        assert job is not None

        nonlocal fenced_write_calls
        fenced_write_calls = 0  # exclude the claim write above
        await run_claimed_job(db, job, lane="current_obs")

        assert fenced_write_calls == 2, (
            "exactly two fenced writes (the reserve, then the outcome write "
            f"that discovers the stale generation) should be attempted; "
            f"saw {fenced_write_calls}"
        )

    asyncio.run(_run())


def test_dl4_history_call_deadline_is_not_refunded_and_marks_the_station(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL4: a provider-call deadline through the main lane's history call
    site (`_fetch_obs` -> `fetch_hourly_history`) behaves like DL3's
    current-obs case -- no refund (`ProviderDeadlineExceeded` is a
    `TimeoutError`, not one of `_REFUNDABLE_TRANSPORT_ERRORS`), the job
    fails/retries, the call lock is released -- and additionally the
    station's `error_count` rises by exactly 1 with `last_error` set
    (`_mark_station_error`, the generic-exception branch of `_fetch_obs`).

    Mutant: accepting `TimeoutError` into `is_refundable_transport_error` --
    the same mutant DL3 targets, killed here through the history call site.
    """
    conn = _init_tmp_db(tmp_path)
    Path(config.options_path).write_text(
        json.dumps({"weathercom_key": "ci-placeholder"}), encoding="utf-8"
    )
    db = get_db()

    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone, "
        "enabled) VALUES ('DL4 Site', 40.0, -105.0, 900.0, 'UTC', 1)"
    )
    site_id = cur.lastrowid
    assert site_id is not None
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m) "
        "VALUES (?, 'ISTATION-DL4', 40.0, -105.0, 900.0)",
        (site_id,),
    )
    station_id = cur.lastrowid
    assert station_id is not None
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, status, max_retries) "
        "VALUES ('fetch_obs', ?, 'obs', '{}', 'pending', 3)",
        (site_id,),
    )
    conn.commit()

    async def _raise_deadline(*_args: object, **_kwargs: object) -> list[object]:
        raise ProviderDeadlineExceeded("provider call exceeded its deadline")

    monkeypatch.setattr(
        "wxverify.worker.processor.fetch_hourly_history", _raise_deadline
    )

    async def _run() -> None:
        lock = weathercom_call_lock()

        def _weathercom_calls() -> int:
            row = conn.execute(
                "SELECT COALESCE(SUM(calls), 0) AS c FROM api_budget"
                " WHERE source = 'weathercom'"
            ).fetchone()
            return int(row["c"])

        before = _weathercom_calls()
        job = await db.write(claim_next_job)
        assert job is not None and job.type == "fetch_obs"

        assert not is_refundable_transport_error(
            ProviderDeadlineExceeded("provider call exceeded its deadline")
        )

        await run_claimed_job(db, job, lane="main")

        after = _weathercom_calls()
        assert after == before + 1, "a refund would bring the call count back down by 1"

        station_row = conn.execute(
            "SELECT error_count, last_error FROM stations WHERE id=?", (station_id,)
        ).fetchone()
        assert station_row["error_count"] == 1
        assert station_row["last_error"] is not None

        job_row = conn.execute(
            "SELECT status, retry_count FROM jobs WHERE id=?", (job.id,)
        ).fetchone()
        assert job_row["status"] in ("pending", "failed")
        assert job_row["retry_count"] == 1

        assert lock.locked() is False

    asyncio.run(_run())


def test_dl5_cancel_mid_call_releases_the_lock_and_reclaims_the_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DL5: cancelling `run_worker` while the current-obs lane is mid-call
    (holding the shared weathercom call lock) still releases the lock and
    reclaims the claimed job -- the one shutdown reclaim (C3), reached
    through the real lane, not a fake.

    Mutants: holding the lock through a bare `acquire()` with no
    `try/finally` -- the lock would stay held after cancellation; and
    dropping the reclaim from the cancel path -- the job would stay
    `running`.
    """
    conn = _init_tmp_db(tmp_path)
    Path(config.options_path).write_text(
        json.dumps({"weathercom_key": "ci-placeholder"}), encoding="utf-8"
    )
    db = get_db()

    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone, "
        "enabled) VALUES ('DL5 Site', 40.0, -105.0, 900.0, 'UTC', 1)"
    )
    site_id = cur.lastrowid
    assert site_id is not None
    conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m) "
        "VALUES (?, 'ISTATION-DL5', 40.0, -105.0, 900.0)",
        (site_id,),
    )
    conn.commit()

    async def _idle_main(_db: Any) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr("wxverify.worker.processor._run_main_lane", _idle_main)

    entered = asyncio.Event()
    finally_ran: list[bool] = []

    async def _hanging_current_obs(
        pws_station_id: str,
        api_key: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 10.0,
    ) -> httpx.Response:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            finally_ran.append(True)
        raise AssertionError("unreachable")

    monkeypatch.setattr(
        "wxverify.worker.processor.fetch_current_observation", _hanging_current_obs
    )

    async def _run() -> None:
        lock = weathercom_call_lock()
        task = asyncio.create_task(run_worker(db))
        await asyncio.wait_for(entered.wait(), timeout=2.0)
        assert lock.locked() is True
        task.cancel()
        await _await_task_done(task)
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finally_ran == [True]
        assert lock.locked() is False
        row = conn.execute(
            "SELECT status FROM jobs WHERE type='fetch_current_obs'"
        ).fetchone()
        assert row is not None and row["status"] == "pending"

    asyncio.run(_run())
