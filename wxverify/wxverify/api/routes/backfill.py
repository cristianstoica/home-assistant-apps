"""Backfill/catchup enqueue routes."""

from __future__ import annotations

import logging
import sqlite3
from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from wxverify.api.errors import ApiError
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import get_db
from wxverify.db.queue import enqueue_if_absent
from wxverify.db.wind_basis import read_auth_hold, write_auth_hold

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["backfill"])


@router.post("/sites/{site_id}/backfill", response_model=None)
async def backfill_site(
    request: Request, site_id: int
) -> dict[str, object] | HTMLResponse:
    def _write(conn: sqlite3.Connection) -> dict[str, object]:
        if (
            conn.execute("SELECT 1 FROM sites WHERE id=?", (site_id,)).fetchone()
            is None
        ):
            raise ApiError(404, "site not found")
        result = enqueue_if_absent(
            conn, "backfill_site", site_id, f"backfill:{site_id}", {"site_id": site_id}
        )
        return {"created": result.created, "job_id": result.job_id}

    result = await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_backfill

        return await render_backfill(request)
    return result


@router.post("/catchup", response_model=None)
async def catchup(request: Request) -> dict[str, object] | HTMLResponse:
    def _write(conn: sqlite3.Connection) -> dict[str, object]:
        result = enqueue_if_absent(conn, "catchup", None, "catchup", {})
        return {"created": result.created, "job_id": result.job_id}

    result = await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_backfill

        return await render_backfill(request)
    return result


class WindAuthHoldIn(BaseModel):
    """Payload for the ops "Try again" control on a wind-history auth hold."""

    endpoint: Literal["history_all", "all_1day"]
    confirm: bool


@router.put("/wind-history/auth-hold")
async def retry_wind_auth_hold(body: WindAuthHoldIn) -> dict[str, object]:
    """Ask the wind lane to check a refused weather.com endpoint once more.

    A ``held`` (or unreadable) hold becomes ``probing``, keeping its last
    error, and the lane is enqueued for every enabled site; the next fetch
    chunk makes one check call. Only a successful call to that endpoint
    clears the hold. An endpoint with no hold answers ``{"status": "clear"}``.
    Behind the existing ``MutationGuard`` like every mutating route.
    """
    if body.confirm is not True:
        raise ApiError(400, "confirmation required")
    endpoint = body.endpoint

    def _write(conn: sqlite3.Connection) -> dict[str, object]:
        hold = read_auth_hold(conn, endpoint)
        if hold is None:
            return {"status": "clear"}
        if hold.status == "held":
            write_auth_hold(
                conn,
                endpoint,
                status="probing",
                since=isoformat_utc(utc_now()),
                error=hold.error,
            )
            logger.info("wind history auth hold set to probing endpoint=%s", endpoint)
        site_ids = [
            int(row["id"])
            for row in conn.execute(
                "SELECT id FROM sites WHERE enabled = 1 ORDER BY id"
            )
        ]
        for site_id in site_ids:
            enqueue_if_absent(conn, "fetch_obs", site_id, "wind-days", {})
        return {"endpoint": endpoint, "status": "probing"}

    return await get_db().write(_write)


def _wants_html(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
