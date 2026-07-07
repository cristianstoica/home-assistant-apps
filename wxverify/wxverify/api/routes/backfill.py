"""Backfill/catchup enqueue routes."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from wxverify.api.errors import ApiError
from wxverify.db.connection import get_db
from wxverify.db.queue import enqueue_if_absent

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


def _wants_html(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
