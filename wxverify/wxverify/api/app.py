"""FastAPI application factory."""
# pyright: reportUnusedFunction=false

from __future__ import annotations

import asyncio
import logging
import os
import platform
import signal
import sqlite3
import threading
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from types import FrameType
from typing import Final
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles
from starlette.datastructures import MutableHeaders

from wxverify import __version__, config
from wxverify.api.csrf import csrf_pair_for, set_csrf_cookie
from wxverify.api.discovery import publish_discovery
from wxverify.api.errors import register_error_handlers
from wxverify.api.guard import MutationGuard
from wxverify.api.ingress import IngressPathMiddleware
from wxverify.api.routes import (
    backfill,
    dashboard,
    db_transfer,
    feeds,
    forecast,
    health,
    settings,
    sites,
    stations,
    timeseries,
)
from wxverify.collection.budget import effective_daily_call_limit, set_source_cap
from wxverify.core import stop
from wxverify.core.aio import run_to_completion
from wxverify.core.options import load_runtime_options
from wxverify.core.timeutil import utc_now
from wxverify.db.connection import Database, current_db, init_db
from wxverify.db.queue import reclaim_all_stale
from wxverify.db.runtime_state import set_runtime_state_now
from wxverify.settings.service import apply_plain_settings, set_rolling_window_days
from wxverify.web.render import static_dir
from wxverify.web.routes import router as web_router
from wxverify.worker.close_chain import settle_interrupted_closes
from wxverify.worker.processor import run_worker

StopProcess = Callable[[], None]
SignalHandler = Callable[[int, FrameType | None], object]
logger = logging.getLogger(__name__)

# The signals uvicorn handles (uvicorn.server.HANDLED_SIGNALS, off Windows).
_STOP_SIGNALS: Final = (signal.SIGTERM, signal.SIGINT)


def _default_stop_process() -> None:
    os._exit(1)


def create_app(
    *,
    root_path: str = "",
    bind_port: int = 8099,
    _stop_process: StopProcess = _default_stop_process,
) -> FastAPI:
    config.ingress_root_path = root_path or ""
    app = FastAPI(
        title=config.APP_TITLE,
        root_path=config.ingress_root_path,
        lifespan=lifespan,
    )
    app.state.stop_process = _stop_process
    app.state.bind_port = bind_port
    app.add_middleware(MutationGuard, standalone_origin=config.standalone_origin)
    app.add_middleware(IngressPathMiddleware)
    register_error_handlers(app)
    # Version-prefixed static mount: the HA frontend service worker caches
    # /static/ paths cache-first with ignoreSearch, so a query-string buster
    # never invalidates. A new version yields new asset PATHS, guaranteeing a
    # cache miss on every release. No bare /static mount: nothing references
    # it, and a stray bare-path link should 404 loudly rather than resurrect
    # the stale-cache bug.
    app.mount(
        f"/static/{__version__}",
        StaticFiles(directory=static_dir()),
        name="static",
    )
    app.include_router(sites.router)
    app.include_router(stations.router)
    app.include_router(feeds.router)
    app.include_router(dashboard.router)
    app.include_router(forecast.router)
    app.include_router(timeseries.router)
    app.include_router(health.router)
    app.include_router(backfill.router)
    app.include_router(db_transfer.router)
    app.include_router(settings.router)
    app.include_router(web_router)

    @app.middleware("http")
    async def no_store_gets(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        response = await call_next(request)
        if request.method == "GET":
            headers: MutableHeaders = response.headers
            headers["Cache-Control"] = "no-store"
        return response

    @app.get("/api/csrf")
    async def csrf(request: Request, response: Response) -> dict[str, str]:
        pair = csrf_pair_for(request)
        set_csrf_cookie(response, pair, request.scope.get("root_path", "") or "")
        return {"csrf_token": pair.token}

    return app


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    # Versions only (no path, key or id): ties this run's SQLite to the release.
    logger.info(
        "wxverify %s; sqlite %s; python %s",
        __version__,
        sqlite3.sqlite_version,
        platform.python_version(),
    )
    # `core.stop.STOP` belongs to one app run. The `finally` below sets it,
    # and while it is set every later database open stops at its first
    # migration step. So each run starts with no stop, and clears its own
    # once the database is closed: one process can run many lifespans.
    stop.reset_stop()
    ZoneInfo("America/Denver")
    # Off the main thread: a Python signal handler runs only between bytecodes
    # on the main thread, so with `init_db` there a stop would wait for the
    # whole upgrade. While it runs, SIGTERM and SIGINT also begin the stop,
    # which interrupts the startup statement. `run_to_completion` is
    # `asyncio.to_thread` that never lets this coroutine move on, and so put
    # the handlers back, while the thread runs.
    restore_signals = _wrap_stop_signals()
    try:
        db = await run_to_completion(init_db, config.db_path)
    finally:
        restore_signals()
    options = load_runtime_options()
    await db.write(reclaim_all_stale)
    # Close work a stop cut short is settled before the workers start and
    # before this start's `worker_started_at`.
    await db.write(lambda conn: settle_interrupted_closes(conn, now_utc=utc_now()))
    await db.write(lambda conn: set_runtime_state_now(conn, "worker_started_at"))
    if options.rolling_window_days is not None:
        await set_rolling_window_days(options.rolling_window_days)
    await apply_plain_settings(options)
    await db.write(
        lambda conn: set_source_cap(
            conn,
            "weathercom",
            daily_call_limit=options.weathercom_daily_call_limit,
        )
    )
    # The stored value stays the configured one; every reader clamps it to the
    # provisional provider allowance. Say so once when it bites.
    weathercom_cap = effective_daily_call_limit(
        "weathercom", options.weathercom_daily_call_limit
    )
    if weathercom_cap < options.weathercom_daily_call_limit:
        logger.warning(
            "weathercom_daily_call_limit %s is above the provisional provider"
            " allowance %s; the effective daily cap is %s",
            options.weathercom_daily_call_limit,
            weathercom_cap,
            weathercom_cap,
        )
    # Every task below is registered the moment it exists, and the `finally`
    # cancels and AWAITS every registered task. The `try` opens BEFORE the
    # first `create_task` so a failure anywhere between the creations cannot
    # leave an already-created task unowned.
    tasks: list[tuple[str, asyncio.Task[None]]] = []
    try:
        worker = asyncio.create_task(run_worker(db))
        tasks.append(("worker", worker))
        logger.info("worker started")
        worker.add_done_callback(
            lambda task: _stop_on_worker_done(task, app.state.stop_process)
        )
        # Retain-and-resume: downloaded exports are NOT deleted post-send, so the
        # export TTL sweep must run on a timer (not only on `/begin`) to reap a
        # retained snapshot when no further export is ever started.
        export_sweeper = asyncio.create_task(db_transfer.run_export_sweeper())
        tasks.append(("export sweeper", export_sweeper))
        export_sweeper.add_done_callback(db_transfer.on_export_sweeper_done)
        await publish_discovery(app.state.bind_port)
        yield
    finally:
        # First: from here no lane claims and no close stage starts, and the
        # stop budget the reap waits within starts.
        stop.request_stop()
        logger.info("worker stopping")
        try:
            await _cancel_and_reap(tasks)
        finally:
            _shutdown_database(db)
            # Last: this run's stop ends with it (see the top of `lifespan`).
            stop.reset_stop()


async def _cancel_and_reap(tasks: list[tuple[str, asyncio.Task[None]]]) -> None:
    """Cancel every registered task, then await each one to completion.

    That distinction is the whole point: `Task.cancel()` only REQUESTS
    cancellation. The `CancelledError` is delivered on the task's next
    scheduling turn, and a handler that itself awaits -- `run_worker` first
    drains both lanes, each bounded by its own in-flight transaction (every
    `db.write` runs through `run_to_completion`), and then runs the one
    shutdown job reclaim, an `await db.write(...)` -- needs several more
    turns to finish. Nothing keeps the loop running to grant them once the
    lifespan has returned, so a cancelled-but-unawaited task can be torn
    down with its reclaim half-done, leaving a claimed job stranded at
    `running` until the next boot's own `reclaim_all_stale` sweep. Awaiting
    every task makes the handler's COMPLETION, not merely its scheduling,
    part of shutdown.

    `asyncio.TaskGroup` is the structured-concurrency answer to task
    ownership and is the wrong tool here. Its `__aexit__` waits for every
    child to finish on its own, and both of these children are endless loops
    that finish only when cancelled -- so the block could be left only by
    cancelling them by hand first, which is exactly the work done below. It
    would also cancel the surviving children and raise an `ExceptionGroup`
    the moment one child fails, turning one broken child into an exception
    escaping shutdown instead of a logged report.

    A SECOND cancellation of the lifespan must not reach the children, and
    an unshielded `await` of the gathering future does not stop it.
    `_GatheringFuture.cancel()` re-cancels every unfinished child, so the
    request lands INSIDE a cancellation handler that is itself awaiting --
    delivering a fresh `CancelledError` into `run_worker`'s lane drain or
    its reclaim write -- and it additionally makes `gather` raise instead
    of returning results, discarding every outcome reported below. Waiting
    with `asyncio.wait` (`core.stop.wait_within_budget`) leaves the
    children untouched, and re-entering the wait after each delivered
    cancellation is what makes that hold for the second, third and Nth
    cancel rather than only the first: N cancels cost N iterations, never a
    dropped reap. The cancellation is deferred, not swallowed -- it is
    re-raised once the reap is complete. `core/aio.py` runs the same loop
    for the same reason one layer down, around the executor thread.

    A hand-rolled `for task in handles: await task` loop has neither
    property -- cancel it and every task it has not reached yet is
    abandoned unawaited, which is the failure this helper exists to
    prevent.

    The wait is bounded by the stop budget:
    `lifespan` requests the stop before calling this, and when the budget
    runs out one ERROR names the tasks still running, the writer's running
    statement is interrupted so its transaction rolls back instead of being
    killed mid-commit, and the wait goes on for up to `core.stop.GRACE_S`
    more. A task still running after that is left to the event loop's final
    sweep, and the next boot's `reclaim_all_stale` recovers its job; only
    the finished tasks are reported. With no stop requested (a direct call)
    the wait has no limit.

    Absorbing cancels gives up the escalation path: no second cancel can
    push along a child that SWALLOWED cancellation, and only the stop
    budget would end the wait. Neither of the two does. `run_worker` first
    drains both lanes, each bounded by its own in-flight transaction (every
    `db.write`
    runs through `run_to_completion`), and then re-raises after the one
    reclaim, whose SQLite wait is capped by `PRAGMA busy_timeout=30000`;
    and `run_export_sweeper` does not handle `CancelledError` at all, so the
    wait is bounded. A third task added to `tasks` owes that same check.

    Each entry in `tasks` pairs a task handle with the name it is reported
    under.

    In normal operation neither task reaches a non-cancelled outcome: both
    are endless loops that return only when cancelled. Such an outcome is
    not impossible -- `run_worker` can crash (that is what
    `_stop_on_worker_done` reports) and `run_export_sweeper` does work
    before its first `try` -- so every non-cancelled outcome is reported by
    name rather than assumed away.

    A crashed worker is the one outcome this helper cannot actually
    report in production. `_stop_on_worker_done` fires the moment the
    worker task completes and its `stop_process` -- `os._exit(1)` by
    default -- ends the process on the spot. A worker that crashes
    during the yield therefore exits before this helper is ever
    entered; one that crashes inside its own cancellation handler exits
    mid-reap, and there the ordering settles it: `add_done_callback` is
    registered on the worker before `gather` registers its own on the
    same task and callbacks fire in registration order, so
    `_stop_on_worker_done` runs first -- the sweeper is never awaited and
    no line below is ever emitted. Either way that
    fail-fast is deliberate. The worker is the service; a nonzero exit
    is the unambiguous signal the supervisor restarts on, and jobs left
    `running` are recovered by the next boot's `reclaim_all_stale`. The
    worker's entry below is therefore reached only when `stop_process`
    is injected, as the tests do.

    Failure policy: a non-cancelled outcome is REPORTED, never propagated.
    Re-raising one task's exception is what the sequential shape did, and
    it is what abandoned the tasks behind it -- the defect this helper
    exists to remove. It would also replace, rather than add to, the report:
    `return_exceptions=True` yields every outcome, and raising surfaces one.
    And it buys nothing downstream: a lifespan SHUTDOWN failure changes no
    exit code, so the service supervisor cannot see it either way. The
    named ERROR line below is the whole operator-visible difference.

    `exc_info=result` rather than `logger.exception` is required, not
    stylistic. `gather` RETURNS the exception instead of raising it, so no
    active exception here belongs to `result`; and because this helper runs
    from a `finally` that may be unwinding a startup failure,
    `sys.exc_info()` is not even empty -- `logger.exception` would silently
    attach THAT traceback instead. `core.aio.run_to_completion` uses the
    same idiom for the same reason.
    """
    names = [name for name, _ in tasks]
    handles = [task for _, task in tasks]
    for task in handles:
        task.cancel()
    reap = asyncio.gather(*handles, return_exceptions=True)
    cancelled = await stop.wait_within_budget(
        (reap,),
        still_running=lambda: [name for name, task in tasks if not task.done()],
        interrupt_writer=_interrupt_writer,
        log=logger,
    )
    # The annotation is a tripwire, not decoration: it pins `gather`'s
    # result type so a drift to `list[Any]` cannot silently disarm the
    # `strict=True` pairing below.
    results: list[BaseException | None]
    if reap.done():
        results = reap.result()
    else:
        # The stop budget and its grace ran out: the tasks still running
        # were named in the ERROR line; the finished ones are reported here.
        results = [
            None if not task.done() or task.cancelled() else task.exception()
            for task in handles
        ]
    for name, result in zip(names, results, strict=True):
        if result is None or isinstance(result, asyncio.CancelledError):
            continue
        logger.error("shutdown: %s failed", name, exc_info=result)
    if cancelled is not None:
        # Re-raised AFTER the report, and never absorbed with
        # `Task.uncancel()`: this helper did not issue that cancellation,
        # so the request must stay visible to whoever did.
        raise cancelled


def _interrupt_writer() -> None:
    """Interrupt the process writer's running statement, if a database is
    open. ``current_db``, never ``get_db``, which would open one."""
    db = current_db()
    if db is not None:
        db.interrupt_writer()


def _wrap_stop_signals() -> Callable[[], None]:
    """Make SIGTERM and SIGINT also begin the stop while ``init_db`` runs.

    Each current handler that is a Python callable
    (uvicorn's ``handle_exit`` in the add-on) is replaced by one that calls
    ``core.stop.request_stop`` -- which interrupts the startup statement --
    and then the original, so uvicorn still records the signal and exits
    once startup has failed. ``SIG_DFL``, ``SIG_IGN`` and a handler not set
    from Python are left alone. Off the main thread nothing is wrapped:
    ``signal.signal`` works only there, and uvicorn skips its own signal
    capture there for the same reason.

    Returns the function that puts the originals back.
    """
    if threading.current_thread() is not threading.main_thread():
        return lambda: None
    originals: list[tuple[signal.Signals, SignalHandler]] = []
    for sig in _STOP_SIGNALS:
        original = signal.getsignal(sig)
        if callable(original):
            signal.signal(sig, _stop_then(original))
            originals.append((sig, original))

    def _restore() -> None:
        for sig, original in reversed(originals):
            signal.signal(sig, original)

    return _restore


def _stop_then(original: SignalHandler) -> SignalHandler:
    """A signal handler that begins the stop, then runs ``original``."""

    def _handler(signum: int, frame: FrameType | None) -> None:
        try:
            stop.request_stop()
        finally:
            original(signum, frame)

    return _handler


def _shutdown_database(db: Database) -> None:
    """Close the process database now that the reap has released it.

    Synchronous on purpose: no ``await`` between ``close_if_idle``'s
    quiescence check and the close it guards, and no cancellation point on
    a path that may already be unwinding one.

    Never raises. A shutdown failure changes no exit code (see
    ``_cancel_and_reap``'s "Failure policy"), and propagating here would
    REPLACE the exception this ``finally`` may be unwinding -- including
    the deferred ``CancelledError`` the reap deliberately kept visible.
    """
    try:
        if db.close_if_idle():
            logger.info("database closed")
    except Exception:
        logger.exception("database close failed at shutdown")


def _stop_on_worker_done(task: asyncio.Task[None], stop_process: StopProcess) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is None:
        logger.critical("worker task returned unexpectedly (no exception)")
    else:
        logger.critical(
            "worker task crashed",
            exc_info=(type(exc), exc, exc.__traceback__),
        )
    stop_process()
