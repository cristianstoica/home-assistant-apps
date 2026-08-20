"""FastAPI application factory."""
# pyright: reportUnusedFunction=false

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles
from starlette.datastructures import MutableHeaders

from wxverify import __version__, config
from wxverify.api.csrf import issue_csrf_pair, set_csrf_cookie
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
    verification,
)
from wxverify.collection.budget import set_source_cap
from wxverify.core.options import load_runtime_options
from wxverify.db.connection import init_db
from wxverify.db.queue import reclaim_all_stale
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state_now
from wxverify.settings.service import apply_plain_settings, set_rolling_window_days
from wxverify.verification.read_cache import warm_read_cache
from wxverify.web.render import static_dir
from wxverify.web.routes import router as web_router
from wxverify.worker.processor import run_worker

StopProcess = Callable[[], None]
logger = logging.getLogger(__name__)


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
    app.include_router(verification.router)
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
        pair = issue_csrf_pair()
        set_csrf_cookie(response, pair, request.scope.get("root_path", "") or "")
        return {"csrf_token": pair.token}

    return app


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    ZoneInfo("America/Denver")
    db = init_db(config.db_path)
    options = load_runtime_options()
    await db.write(reclaim_all_stale)
    try:
        await asyncio.to_thread(
            db_transfer.sweep_bak_files, Path(config.db_path).parent
        )
    except Exception:
        logger.exception("startup: bak retention sweep failed")
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
    # An import's derived-table rebuild normally runs as a post-response
    # background task; a process restart between the import response and
    # that task's completion leaves it stuck at "pending". Resume it here,
    # inline, before the worker starts -- never via the jobs queue, since
    # the jobs queue itself depends on derived tables being rebuilt. It must
    # also stay after the settings applied above (`set_rolling_window_days`,
    # `apply_plain_settings`): the rebuild's `pair_and_score` reads `min_n`
    # from settings, so resuming any earlier would rebuild against stale
    # settings instead of this boot's configured values.
    rebuild_state = await db.read(
        lambda conn: get_runtime_state(conn, "import_rebuild_state")
    )
    if rebuild_state == "pending":
        logger.info("startup: resuming interrupted import rebuild")
        await db_transfer.run_rebuild_all()
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
        # A run published before this process started is never warmed by a publish
        # event, so without this leg the first request after every restart pays the
        # full cold derivation. It does not block startup: `db.read` dispatches the
        # derivations to an executor thread.
        warm = asyncio.create_task(warm_read_cache(db))
        tasks.append(("read-cache warm", warm))
        await publish_discovery(app.state.bind_port)
        yield
    finally:
        logger.info("worker stopping")
        await _cancel_and_reap(tasks)


async def _cancel_and_reap(tasks: list[tuple[str, asyncio.Task[None]]]) -> None:
    """Cancel every registered task, then await each one to completion.

    That distinction is the whole point: `Task.cancel()` only REQUESTS
    cancellation. The `CancelledError` is delivered on the task's next
    scheduling turn, and a handler that itself awaits -- `run_worker`'s is
    the shutdown job reclaim, an `await db.write(...)` -- needs several more
    turns to finish. Nothing keeps the loop running to grant them once the
    lifespan has returned, so a cancelled-but-unawaited task can be torn
    down with its reclaim half-done, leaving a claimed job stranded at
    `running` until the next boot's own `reclaim_all_stale` sweep. Awaiting
    every task makes the handler's COMPLETION, not merely its scheduling,
    part of shutdown.

    `asyncio.TaskGroup` is the structured-concurrency answer to task
    ownership and is the wrong tool here. Its `__aexit__` waits for every
    child to finish on its own, and two of these children are endless loops
    that finish only when cancelled -- so the block could be left only by
    cancelling them by hand first, which is exactly the work done below. It
    would also cancel the surviving children and raise an `ExceptionGroup`
    the moment one child fails, turning a broken read-cache warm into an
    exception escaping shutdown instead of a logged report.

    `gather` additionally keeps its guarantee under a SECOND cancellation.
    Cancelling the future `gather` returns does not resolve that future: the
    request is forwarded to the children and merely recorded on the outer
    one, which completes only once every child has actually finished. So a
    supervisor impatient enough to cancel shutdown while the reap is still
    in flight gets every reclaim run to the end anyway. A hand-rolled
    `for task in handles: await task` loop has no such property -- cancel it
    and every task it has not reached yet is abandoned unawaited, which is
    the failure this helper exists to prevent.

    Each entry in `tasks` pairs a task handle with the name it is reported
    under.

    In normal operation only the read-cache warm reaches a non-cancelled
    outcome: its two neighbours are endless loops that return only when
    cancelled, and the warm's contract is to never raise, so an exception
    from it means that contract broke. The other two are not impossible --
    `run_worker` can crash (that is what `_stop_on_worker_done` reports) and
    `run_export_sweeper` does work before its first `try` -- so every
    non-cancelled outcome is reported by name rather than assumed away.

    `exc_info=result` rather than `logger.exception` is required, not
    stylistic. `gather` RETURNS the exception instead of raising it, so no
    active exception here belongs to `result`; and because this helper runs
    from a `finally` that may be unwinding a startup failure,
    `sys.exc_info()` is not even empty -- `logger.exception` would silently
    attach THAT traceback instead. `wxverify/core/aio.py:64` uses the same
    idiom for the same reason.
    """
    names = [name for name, _ in tasks]
    handles = [task for _, task in tasks]
    for task in handles:
        task.cancel()
    # The annotation is a tripwire, not decoration: it pins `gather`'s return
    # type so a drift to `list[Any]` cannot silently disarm the `strict=True`
    # pairing below.
    results: list[BaseException | None] = await asyncio.gather(
        *handles, return_exceptions=True
    )
    for name, result in zip(names, results, strict=True):
        if result is None or isinstance(result, asyncio.CancelledError):
            continue
        logger.error("shutdown: %s failed", name, exc_info=result)


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
