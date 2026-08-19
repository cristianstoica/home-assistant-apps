"""FastAPI application factory."""
# pyright: reportUnusedFunction=false

from __future__ import annotations

import asyncio
import contextlib
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
    worker = asyncio.create_task(run_worker(db))
    logger.info("worker started")
    worker.add_done_callback(
        lambda task: _stop_on_worker_done(task, app.state.stop_process)
    )
    # Retain-and-resume: downloaded exports are NOT deleted post-send, so the
    # export TTL sweep must run on a timer (not only on `/begin`) to reap a
    # retained snapshot when no further export is ever started.
    export_sweeper = asyncio.create_task(db_transfer.run_export_sweeper())
    # A run published before this process started is never warmed by a publish
    # event, so without this leg the first request after every restart pays the
    # full cold derivation. It does not block startup: `db.read` dispatches the
    # derivations to an executor thread.
    warm = asyncio.create_task(warm_read_cache(db))
    await publish_discovery(app.state.bind_port)
    try:
        yield
    finally:
        logger.info("worker stopping")
        export_sweeper.cancel()
        worker.cancel()
        warm.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await export_sweeper
        with contextlib.suppress(asyncio.CancelledError):
            await worker
        # Awaited LAST, and CAUGHT rather than suppressed. Unlike its
        # neighbours -- which never complete -- the warm is a task that
        # finishes, so a stored exception would re-raise out of this `finally`
        # and skip every await ordered behind it; ordering it last means an
        # escape cannot skip another task's cleanup. Catching instead of
        # `suppress(asyncio.CancelledError, Exception)` keeps the only signal
        # that the warm broke its never-raise contract.
        try:
            await warm
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("shutdown: read-cache warm failed")


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
