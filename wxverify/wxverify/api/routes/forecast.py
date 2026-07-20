"""Forecast-page JSON routes (hourly drill-down data)."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, HTTPException, Query

from wxverify.db.connection import get_db
from wxverify.forecast.service import build_hourly
from wxverify.web.context import load_site

router = APIRouter(prefix="/api", tags=["forecast"])


@router.get("/forecast/hourly")
async def forecast_hourly(
    site: int = Query(...),
    day: int = Query(0),
) -> dict[str, object]:
    """Blended hourly series (plus per-feed series) for one display day.

    ``day`` is now-relative (0 = Today ... 7) and clamps into range rather
    than erroring — the chart is a read-only view, so a crafted ?day=99
    degrades to the horizon edge instead of a 500.
    """
    day_clamped = max(0, min(7, day))

    def _read(conn: sqlite3.Connection) -> dict[str, object] | None:
        site_view = load_site(conn, site)
        if site_view is None:
            return None
        return build_hourly(
            conn,
            site_id=site_view.id,
            timezone=site_view.timezone,
            day=day_clamped,
        )

    payload = await get_db().read(_read)
    if payload is None:
        raise HTTPException(status_code=404, detail="site not found")
    return payload
