"""FastAPI application factory."""
# pyright: reportUnusedFunction=false

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles
from starlette.datastructures import MutableHeaders

from wxverify import config
from wxverify.api.csrf import issue_csrf_pair, set_csrf_cookie
from wxverify.api.errors import register_error_handlers
from wxverify.api.guard import MutationGuard
from wxverify.api.ingress import IngressPathMiddleware
from wxverify.api.routes import (
    backfill,
    dashboard,
    feeds,
    health,
    sites,
    stations,
    timeseries,
)
from wxverify.core.options import load_runtime_options
from wxverify.db.connection import init_db
from wxverify.db.queue import reclaim_all_stale
from wxverify.db.runtime_state import set_runtime_state_now
from wxverify.settings.service import apply_plain_settings, set_rolling_window_days
from wxverify.web.render import static_dir
from wxverify.web.routes import router as web_router
from wxverify.worker.processor import run_worker

StopProcess = Callable[[], None]
logger = logging.getLogger(__name__)


def _default_stop_process() -> None:
    os._exit(1)


def create_app(
    *, root_path: str = "", _stop_process: StopProcess = _default_stop_process
) -> FastAPI:
    config.ingress_root_path = root_path or ""
    app = FastAPI(
        title=config.APP_TITLE,
        root_path=config.ingress_root_path,
        lifespan=lifespan,
    )
    app.state.stop_process = _stop_process
    app.add_middleware(MutationGuard, standalone_origin=config.standalone_origin)
    app.add_middleware(IngressPathMiddleware)
    register_error_handlers(app)
    app.mount("/static", StaticFiles(directory=static_dir()), name="static")
    app.include_router(sites.router)
    app.include_router(stations.router)
    app.include_router(feeds.router)
    app.include_router(dashboard.router)
    app.include_router(timeseries.router)
    app.include_router(health.router)
    app.include_router(backfill.router)
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
    ZoneInfo("Europe/Bucharest")
    db = init_db(config.db_path)
    options = load_runtime_options()
    await db.write(reclaim_all_stale)
    await db.write(lambda conn: set_runtime_state_now(conn, "worker_started_at"))
    if options.rolling_window_days is not None:
        await set_rolling_window_days(options.rolling_window_days)
    await apply_plain_settings(options)
    worker = asyncio.create_task(run_worker(db))
    logger.info("worker started")
    worker.add_done_callback(
        lambda task: _stop_on_worker_done(task, app.state.stop_process)
    )
    try:
        yield
    finally:
        logger.info("worker stopping")
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker


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
