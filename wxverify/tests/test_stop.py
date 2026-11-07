"""Tests for W1, stop handling (the storage-release plan, §6.2-6.4 and §6.6,
work item W1).

Written before the module these tests import exists, and fails first for
that reason: ``wxverify.core.stop`` does not exist at this commit, so every
test below fails on ``ModuleNotFoundError`` until it is built. The build
must match this API, read from plan §6.2-6.4:

- ``STOP: threading.Event()`` -- one process-wide stop flag.
- ``register(conn: sqlite3.Connection) -> None`` / ``unregister(conn) ->
  None`` -- track the one connection doing startup work, so
  ``request_stop()`` can interrupt it.
- ``request_stop() -> None`` -- sets ``STOP`` and calls ``conn.interrupt()``
  on the registered connection, if any (thread-safe; ends the running
  statement with ``sqlite3.OperationalError: interrupted``).
- ``check() -> None`` -- raises ``StopRequested`` (a plain ``Exception``
  subclass) when ``STOP`` is set, else returns.
- ``StopRequested(Exception)``.
- ``STOP_BUDGET_S: float`` -- the stop budget in seconds, read THROUGH the
  module at call time (the pattern ``wxverify.worker.processor`` already
  documents for ``cutover_module.CUTOVER_RETRY_SECONDS``: "Read through the
  module at each wait, not bound here"), so a test can
  ``monkeypatch.setattr(stop_module, "STOP_BUDGET_S", ...)`` and have it
  take effect. A module-load-time ``from wxverify.core.stop import
  STOP_BUDGET_S`` import elsewhere would defeat that -- the budget must be
  read as ``stop.STOP_BUDGET_S`` (or re-imported) at the point it is used,
  never bound to a local name at import time.

Running code (6.3-6.4) is assumed to interrupt the *running* writer
connection directly (``db._conn.interrupt()``, via the already-public
``get_db()``), not through ``register``/``unregister`` -- those two are
named in §6.2 for *startup* only ("the one connection doing startup work").

Harness idioms are copied from ``tests/test_graceful_shutdown.py``
(``_init_tmp_db``, ``_patch_worker_infra``, ``_insert_job``, the
``create_app``/``lifespan`` drive, the gated-stub rendezvous pattern) --
local, trimmed copies live below rather than importing that module, since
none of its names are exported as shared helpers and this file must not
depend on another test file's internals.

Real sqlite only: every connection is a real file-backed or ``:memory:``
database, no mocked persistence. Station/site ids, names and coordinates
below are synthetic placeholders (letters A-F, made-up lat/lon), per the
project's synthetic-data rule.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from wxverify import config
from wxverify.api.app import create_app, lifespan
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.queue import reclaim_all_stale
from wxverify.worker.processor import run_worker

# ---------------------------------------------------------------------------
# Fixtures: STOP must never leak between tests -- it is a process-wide
# ``threading.Event()`` per §6.2, so one test setting it would otherwise
# leave every later test observing a stop already in progress.
# ---------------------------------------------------------------------------

_test_registered_conns: list[sqlite3.Connection] = []


def _register_for_test(conn: sqlite3.Connection) -> None:
    """Register ``conn`` with ``wxverify.core.stop`` and track it so the
    ``_reset_stop`` fixture below unregisters it even if the test raises
    before reaching its own cleanup.
    """
    from wxverify.core.stop import register

    register(conn)
    _test_registered_conns.append(conn)


@pytest.fixture(autouse=True)
def _reset_stop() -> Iterator[None]:
    """Unregister any connection registered via ``_register_for_test``,
    before AND after the test -- before, in case an earlier test crashed
    past its own cleanup; after, for the common case. ``STOP`` itself (and
    the startup connection it tracks globally) is reset before each test by
    ``tests/conftest.py``'s ``_reset_core_stop`` fixture; this fixture only
    covers what that one does not: this file's own tracked connections.
    """
    with contextlib.suppress(ModuleNotFoundError):
        from wxverify.core.stop import unregister

        for conn in _test_registered_conns:
            with contextlib.suppress(Exception):
                unregister(conn)
        _test_registered_conns.clear()
    yield
    with contextlib.suppress(ModuleNotFoundError):
        from wxverify.core.stop import unregister

        for conn in _test_registered_conns:
            with contextlib.suppress(Exception):
                unregister(conn)
        _test_registered_conns.clear()


# ---------------------------------------------------------------------------
# Local harness (trimmed copies of tests/test_graceful_shutdown.py's idiom).
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
    """Silence the per-iteration housekeeping unrelated to stop handling."""
    monkeypatch.setattr(
        "wxverify.worker.processor.set_runtime_state_now", lambda _c, _k: None
    )
    monkeypatch.setattr("wxverify.worker.processor.scheduler_tick", lambda _c: None)
    monkeypatch.setattr(
        "wxverify.worker.processor.purge_failed_jobs_older_than", lambda _c, _h: None
    )


def _insert_job(
    conn: sqlite3.Connection, *, job_type: str = "fetch_feed", job_key: str = "k1"
) -> int:
    """A minimal, synthetic job against a disabled seeded site (the pattern
    of ``tests/test_graceful_shutdown.py``'s ``_insert_job``): disabled so a
    live ``scheduler_tick`` never queues per-site work ahead of this job.
    """
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES (?, 40.0, -105.0, 900.0, 'UTC', 0)",
        (f"stop-test-site-{job_key}",),
    )
    site_id = cur.lastrowid
    assert site_id is not None, "INSERT must produce a rowid"
    cur = conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, payload, status)
        VALUES (?, ?, ?, '{}', 'pending')
        """,
        (job_type, site_id, job_key),
    )
    conn.commit()
    job_id = cur.lastrowid
    assert job_id is not None, "INSERT must produce a rowid"
    return job_id


def _job_status(conn: sqlite3.Connection, job_id: int) -> str:
    row = conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert row is not None, f"job {job_id} vanished"
    return str(row["status"])


def _make_immediate_stub(_name: str):
    """A sibling task stand-in that reacts to cancellation immediately, with
    no gating (the pattern of ``tests/test_graceful_shutdown.py``'s
    ``_make_immediate_stub``)."""

    async def _stub(*_args: object) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise

    return _stub


# ---------------------------------------------------------------------------
# Case 1 (§6.2): register(conn) + request_stop() from another thread
# interrupts a long-running query within 1s.
# ---------------------------------------------------------------------------


def test_request_stop_interrupts_a_long_running_query_on_the_registered_connection(
    tmp_path: Path,
) -> None:
    """W1 case 1. A connection registered via ``register(conn)`` runs a long
    recursive CTE on its own thread; ``request_stop()``, called from the
    main thread, must end that statement with
    ``sqlite3.OperationalError: interrupted`` within 1 second.

    The CTE has no row bound other than ``x > 999999999999`` -- effectively
    unbounded within any test timeout -- so the test can only pass if
    ``request_stop()`` actually reaches ``conn.interrupt()``; it cannot pass
    by the query finishing on its own first. The ``from wxverify.core.stop
    import ...`` line below runs before the thread is ever started, so a
    missing module fails this test immediately rather than hanging it.
    """
    from wxverify.core.stop import request_stop, unregister

    db_path = tmp_path / "stop-cte.db"
    started = threading.Event()
    done = threading.Event()
    outcome: dict[str, BaseException | None] = {"error": None}

    def _run_long_query() -> None:
        conn = sqlite3.connect(str(db_path))
        try:
            from wxverify.core.stop import register

            register(conn)
            started.set()
            conn.execute(
                "WITH RECURSIVE cnt(x) AS ("
                "SELECT 1 UNION ALL SELECT x + 1 FROM cnt LIMIT -1"
                ") SELECT x FROM cnt WHERE x > 999999999999"
            ).fetchone()
        except sqlite3.OperationalError as exc:
            outcome["error"] = exc
        finally:
            unregister(conn)
            conn.close()
            done.set()

    thread = threading.Thread(target=_run_long_query, daemon=True)
    thread.start()
    try:
        assert started.wait(timeout=2.0), "worker thread never started its query"
        start = time.monotonic()
        request_stop()
        assert done.wait(timeout=1.0), (
            "request_stop() did not interrupt the running query within 1s"
        )
        elapsed = time.monotonic() - start
        assert elapsed < 1.0, f"interrupt took {elapsed}s, not under 1s"
        assert outcome["error"] is not None, (
            "the query must end on an OperationalError, not finish on its own"
        )
        assert "interrupted" in str(outcome["error"])
    finally:
        thread.join(timeout=1.0)


# ---------------------------------------------------------------------------
# Case 2 (§6.2): check() inside a savepoint leaves user_version and rows as
# they were, after the caller rolls back.
# ---------------------------------------------------------------------------


def test_check_inside_a_savepoint_leaves_user_version_and_rows_unchanged_after_rollback(
    tmp_path: Path,
) -> None:
    """W1 case 2. A step that opens a savepoint, changes ``user_version``
    and inserts a row, then calls ``check()`` with ``STOP`` already set,
    must see ``check()`` raise ``StopRequested`` -- and rolling back to the
    savepoint on that exception (as ``run_migrations``/``migrate_v18``
    will, per §6.2) leaves both ``user_version`` and the row exactly as they
    were beforehand.

    Paired positive: the same step run with ``STOP`` clear completes,
    raising nothing, and the change DOES land -- without this pairing, a
    ``check()`` that raises unconditionally (never checking ``STOP`` at all)
    would still pass the negative half.
    """
    from wxverify.core.stop import STOP, StopRequested, check, register

    db_path = tmp_path / "stop-savepoint.db"
    conn = sqlite3.connect(str(db_path))
    register(conn)
    try:
        conn.execute("PRAGMA user_version = 5")
        conn.execute("CREATE TABLE t (x INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()

        # Negative: STOP set, the step is rolled back.
        STOP.set()
        conn.execute("SAVEPOINT step")
        conn.execute("PRAGMA user_version = 6")
        conn.execute("INSERT INTO t VALUES (2)")
        with pytest.raises(StopRequested):
            check()
        conn.execute("ROLLBACK TO step")
        conn.execute("RELEASE step")
        conn.commit()

        assert conn.execute("PRAGMA user_version").fetchone()[0] == 5
        assert [r[0] for r in conn.execute("SELECT x FROM t ORDER BY x")] == [1]

        # Positive: STOP clear, the identically-shaped step completes and lands.
        STOP.clear()
        conn.execute("SAVEPOINT step")
        conn.execute("PRAGMA user_version = 6")
        conn.execute("INSERT INTO t VALUES (2)")
        check()  # must not raise
        conn.execute("RELEASE step")
        conn.commit()

        assert conn.execute("PRAGMA user_version").fetchone()[0] == 6
        assert [r[0] for r in conn.execute("SELECT x FROM t ORDER BY x")] == [1, 2]
    finally:
        unregister_conn = conn
        with contextlib.suppress(Exception):
            from wxverify.core.stop import unregister

            unregister(unregister_conn)
        conn.close()


# ---------------------------------------------------------------------------
# Case 3 (§6.3): with STOP set, neither lane claims a pending job, and
# neither lane returns before it is cancelled.
# ---------------------------------------------------------------------------


def test_stop_set_blocks_both_lanes_from_claiming_and_neither_lane_returns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """W1 case 3. With ``STOP`` set before ``run_worker`` starts: a pending
    main-lane job (claimed by ``_run_main_lane``, ``worker/processor.py:263``)
    and a pending current-obs job (claimed by ``run_current_obs_poller``,
    ``worker/current_obs_poller.py:82``) must both stay ``pending`` --
    neither lane may claim while a stop is pending. Also: the worker task
    must still be running (not returned) right up to the moment it is
    cancelled, since a lane that returns on its own is treated as a crash
    by ``_stop_on_worker_done`` (§6.3's "must not return").

    Paired positive is case 6's crash-path test and the whole existing
    ``tests/test_graceful_shutdown.py`` suite, which claims and completes
    these same job shapes once ``STOP`` is clear.
    """
    from wxverify.core.stop import STOP

    conn = _init_tmp_db(tmp_path)
    from wxverify.db.runtime_state import set_runtime_state
    from wxverify.db.site_days import DAILY_CLOSE_ON_KEY

    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(utc_now()))
    conn.commit()
    _patch_worker_infra(monkeypatch)
    main_job_id = _insert_job(conn, job_type="fetch_feed", job_key="main-k1")
    obs_job_id = _insert_job(conn, job_type="fetch_current_obs", job_key="obs-k1")
    db = get_db()

    STOP.set()

    async def _run() -> None:
        task = asyncio.create_task(run_worker(db))
        await asyncio.sleep(0.2)
        assert not task.done(), "a lane must not return while STOP is set"
        assert _job_status(conn, main_job_id) == "pending", (
            "the main lane claimed a pending job while STOP was set"
        )
        assert _job_status(conn, obs_job_id) == "pending", (
            "the current-obs lane claimed a pending job while STOP was set"
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Case 4 (§6.3): the close chain starts no stage once STOP is set.
# ---------------------------------------------------------------------------


def test_stop_set_before_a_close_stage_starts_no_stage_body(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """W1 case 4. ``run_day_close`` dispatched to stage 6 (closure) with
    ``STOP`` already set must never reach the stage body
    (``close_chain._closure_stage``) -- "the close chain before each stage"
    (§6.3) refuses to start work, the same contract as the lanes' claim
    guard in case 3, at the stage-dispatch boundary instead of the claim.

    The job's payload is ``{"attempt": 1}``, a shape ``close_chain.
    _stage_job`` accepts, so a ``STOP``-caused ``StopRequested`` is the ONLY
    exception this run can raise -- unlike a malformed payload (e.g. ``{}``),
    which ``_parse_or_cancel`` would reject with ``JobCancelled`` regardless
    of ``STOP``, making the negative half pass even with ``run_day_close``'s
    ``stop.check()`` removed. ``_closure_stage`` is replaced with a recorder
    rather than driven for real, so the assertion is a direct call count.

    Paired positive: the identically-shaped job run with ``STOP`` clear
    reaches the stage body exactly once -- without this pairing, a
    ``_stage_job`` that always returned ``None`` (so every job looks
    malformed) would also make the negative half pass.
    """
    from wxverify.core.stop import STOP, StopRequested
    from wxverify.db.connection import Database, FencedWriter
    from wxverify.db.queue import Job
    from wxverify.worker import close_chain
    from wxverify.worker.close_chain import run_day_close

    conn = _init_tmp_db(tmp_path)
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES ('stop-test-close-site', 40.0, -105.0,"
        " 900.0, 'UTC', 0)"
    )
    site_id = cur.lastrowid
    assert site_id is not None
    conn.commit()

    calls: list[None] = []

    async def _recorder(*_args: object, **_kwargs: object) -> None:
        calls.append(None)

    monkeypatch.setattr(close_chain, "_closure_stage", _recorder)

    db: Database = get_db()
    writer = FencedWriter(db)
    job = Job(
        id=1,
        type="day_close",
        site_id=site_id,
        job_key=f"close:{site_id}:2024-06-10:6",
        payload={"attempt": 1},
        status="running",
        retry_count=0,
        max_retries=0,
    )

    STOP.set()

    async def _run_stopped() -> None:
        await run_day_close(db, writer, job)

    with pytest.raises(StopRequested):
        asyncio.run(_run_stopped())
    assert calls == [], "the closure stage body must not start while STOP is set"

    STOP.clear()

    async def _run_clear() -> None:
        await run_day_close(db, writer, job)

    asyncio.run(_run_clear())
    assert calls == [None], (
        "the closure stage body must run once STOP is clear, for the"
        " identically-shaped job"
    )


# ---------------------------------------------------------------------------
# Case 5 (§6.4): a job that outlasts the stop budget gives the ERROR line
# and an interrupt() on the writer; reclaim_all_stale makes it pending.
# ---------------------------------------------------------------------------


def test_stop_budget_exceeded_logs_interrupts_writer_and_job_is_pending_after_reclaim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """W1 case 5. ``STOP_BUDGET_S`` monkeypatched to 1 second. The worker
    lane's cancellation handler stands in for "a job write already running"
    (§6.3) by performing a REAL long-running write on the process's writer
    connection (the same unbounded recursive CTE as case 1) instead of
    returning promptly. Once the 1s budget elapses, §6.4 requires:

    1. one ERROR "stop budget exceeded; still running: ..." naming the
       worker task;
    2. ``interrupt()`` on the writer -- observed here by the long write
       itself raising ``sqlite3.OperationalError: interrupted``, not simply
       being cancelled (a bare task-cancel would not touch a statement
       already running inside ``db.write``'s executor thread);
    3. a job row left ``running`` by the interrupted write reverts to
       ``pending`` once ``reclaim_all_stale`` runs (the next boot's call,
       §6.4's last sentence) -- simulated here by seeding one such row and
       reclaiming it directly after shutdown completes.

    This test imports ``wxverify.core.stop`` and patches its
    ``STOP_BUDGET_S`` before creating the long-running stand-in, so a
    missing module fails fast rather than letting an un-budgeted shutdown
    wait on the CTE forever.
    """
    import wxverify.core.stop as stop_module

    monkeypatch.setattr(stop_module, "STOP_BUDGET_S", 1)

    conn = _init_tmp_db(tmp_path)
    job_id = _insert_job(conn, job_type="fetch_feed", job_key="budget-k1")
    conn.execute("UPDATE jobs SET status = 'running' WHERE id = ?", (job_id,))
    conn.commit()

    interrupted = threading.Event()
    entered = asyncio.Event()

    async def _worker_stub(_db: Any) -> None:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            try:
                await get_db().write(
                    lambda c: c.execute(
                        "WITH RECURSIVE cnt(x) AS ("
                        "SELECT 1 UNION ALL SELECT x + 1 FROM cnt LIMIT -1"
                        ") SELECT x FROM cnt WHERE x > 999999999999"
                    ).fetchone()
                )
            except sqlite3.OperationalError as exc:
                if "interrupted" in str(exc):
                    interrupted.set()
            raise

    monkeypatch.setattr("wxverify.api.app.run_worker", _worker_stub)
    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper",
        _make_immediate_stub("export sweeper"),
    )

    async def _noop_discovery(_port: int) -> None:
        return None

    monkeypatch.setattr("wxverify.api.app.publish_discovery", _noop_discovery)

    stopped: list[None] = []
    app = create_app(root_path="", _stop_process=lambda: stopped.append(None))

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        # ``_noop_discovery`` has no ``await`` in its body, so awaiting
        # ``__aexit__`` straight away would never yield control back to the
        # loop before ``_cancel_and_reap`` cancels ``_worker_stub`` -- the
        # worker task created during ``__aenter__`` would never be stepped
        # even once, so the cancel lands before its first ``await`` and the
        # ``except CancelledError`` body (the long write this test depends
        # on) never runs (see the identical rendezvous and its comment in
        # ``test_graceful_shutdown.py``, lines 807-813). ``_worker_stub``
        # sets ``entered`` as its very first line, right before its own
        # first ``await``, so waiting on it here forces the loop to step
        # the worker to that suspension point deterministically, instead of
        # racing ``_cancel_and_reap`` for it.
        await entered.wait()
        with caplog.at_level(logging.ERROR, logger="wxverify.api.app"):
            await asyncio.wait_for(cm.__aexit__(None, None, None), timeout=5.0)

    asyncio.run(_run())

    assert interrupted.is_set(), "interrupt() never reached the long-running write"
    budget_errors = [
        r
        for r in caplog.records
        if r.name == "wxverify.api.app"
        and r.getMessage().startswith("stop budget exceeded; still running:")
    ]
    assert len(budget_errors) == 1, [r.getMessage() for r in caplog.records]
    assert "worker" in budget_errors[0].getMessage()

    conn2 = sqlite3.connect(config.db_path)
    try:
        reclaim_all_stale(conn2)
        conn2.commit()
        row = conn2.execute(
            "SELECT status FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        assert row is not None and row[0] == "pending", (
            "reclaim_all_stale must put the still-running job back to pending"
        )
    finally:
        conn2.close()


def test_run_claimed_job_interrupted_by_stop_records_nothing_then_reclaim_repends(
    tmp_path: Path,
) -> None:
    """W1 case 5, the "pending after reclaim" half, driven for real.

    A claimed ``day_close`` job run through ``processor.run_claimed_job``
    with ``STOP`` already set: ``run_day_close``'s ``stop.check()`` (close
    chain, §6.3) raises ``StopRequested`` before any write starts, so
    ``run_claimed_job``'s ``except stop.StopRequested`` branch records
    nothing -- the job row is left exactly as claimed (``running``,
    ``retry_count`` unchanged), per its own comment ("the row stays running,
    and the shutdown reclaim or the next start's reclaim_all_stale puts it
    back to pending"). Only then does ``reclaim_all_stale`` put it back to
    ``pending`` -- unlike the case 5 test above, which seeds a running row
    and reclaims it directly without any job code ever touching it, so it
    cannot tell "nothing was recorded by run_claimed_job" from "nothing ran
    at all".
    """
    from wxverify.core.stop import STOP
    from wxverify.db.queue import Job
    from wxverify.worker.processor import run_claimed_job

    conn = _init_tmp_db(tmp_path)
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES ('stop-test-claimed-site', 40.0, -105.0,"
        " 900.0, 'UTC', 0)"
    )
    site_id = cur.lastrowid
    assert site_id is not None
    job_key = f"close:{site_id}:2024-06-10:3"
    cur = conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, status, retry_count)"
        " VALUES ('day_close', ?, ?, '{\"attempt\": 1}', 'running', 0)",
        (site_id, job_key),
    )
    job_id = cur.lastrowid
    assert job_id is not None
    conn.commit()

    job = Job(
        id=job_id,
        type="day_close",
        site_id=site_id,
        job_key=job_key,
        payload={"attempt": 1},
        status="running",
        retry_count=0,
        max_retries=0,
    )

    db = get_db()
    STOP.set()
    asyncio.run(run_claimed_job(db, job, lane="main"))

    row = conn.execute(
        "SELECT status, retry_count FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    assert row is not None
    assert row["status"] == "running", (
        "a job stopped before its write started must not be recorded as"
        " failed, succeeded or retried -- the row must stay 'running'"
    )
    assert row["retry_count"] == 0, (
        "nothing must be recorded for a stopped job, so retry_count must not advance"
    )

    STOP.clear()
    changed = reclaim_all_stale(conn)
    conn.commit()
    assert changed == 1
    row = conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
    assert row is not None and row["status"] == "pending", (
        "reclaim_all_stale must put the stopped job back to pending"
    )


# ---------------------------------------------------------------------------
# Case 6 (§6.6): a lane that raises still reaches stop_process at once,
# through _stop_on_worker_done, with no budget wait.
# ---------------------------------------------------------------------------


def test_a_crashing_lane_reaches_stop_process_immediately_with_no_budget_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """W1 case 6 (§6.6). ``STOP_BUDGET_S`` monkeypatched to 5 seconds -- a
    worker lane that CRASHES (raises, rather than being cancelled) must
    still reach ``_stop_on_worker_done`` -> ``stop_process()`` well under
    that budget, since the crash path "keeps its exit path ... with no
    budget" (§6.6). If a future change wrapped the crash-detection path in
    the same budget wait the running-shutdown path uses, this test would
    see ``stop_process()`` fire only after ~5s (or time out waiting for it
    within 1s) instead of immediately.
    """
    import wxverify.core.stop as stop_module

    monkeypatch.setattr(stop_module, "STOP_BUDGET_S", 5)

    _init_tmp_db(tmp_path)

    async def _crashing_worker(_db: Any) -> None:
        raise RuntimeError("lane boom")

    monkeypatch.setattr("wxverify.api.app.run_worker", _crashing_worker)
    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper",
        _make_immediate_stub("export sweeper"),
    )

    async def _noop_discovery(_port: int) -> None:
        return None

    monkeypatch.setattr("wxverify.api.app.publish_discovery", _noop_discovery)

    stopped = asyncio.Event()

    def _stop_process() -> None:
        stopped.set()

    app = create_app(root_path="", _stop_process=_stop_process)

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        start = time.monotonic()
        await asyncio.wait_for(stopped.wait(), timeout=1.0)
        elapsed = time.monotonic() - start
        assert elapsed < 1.0, (
            f"stop_process() took {elapsed}s -- the crash path must not wait"
            " on STOP_BUDGET_S"
        )
        await cm.__aexit__(None, None, None)

    asyncio.run(_run())
