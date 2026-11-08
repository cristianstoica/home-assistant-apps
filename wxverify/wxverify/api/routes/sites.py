"""Site routes."""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from wxverify.api.errors import ApiError
from wxverify.api.schemas import SiteCreate, SiteOut, SiteUpdate
from wxverify.core.timeutil import utc_now
from wxverify.db.connection import get_db
from wxverify.db.site_days import start_site_days
from wxverify.db.switch import ensure_site_generation
from wxverify.verification.snapshot_time import SNAPSHOT_TIME_KEY

router = APIRouter(prefix="/api/sites", tags=["sites"])


def _site_out(row: sqlite3.Row) -> SiteOut:
    return SiteOut(
        id=int(row["id"]),
        name=str(row["name"]),
        forecast_lat=float(row["forecast_lat"]),
        forecast_lon=float(row["forecast_lon"]),
        elevation_m=float(row["elevation_m"]),
        timezone=str(row["timezone"]),
        enabled=bool(row["enabled"]),
        rain_threshold_mm=float(row["rain_threshold_mm"]),
    )


@router.get("", response_model=list[SiteOut])
async def list_sites(include_disabled: bool = False) -> list[SiteOut]:
    def _read(conn: sqlite3.Connection) -> list[SiteOut]:
        where = "" if include_disabled else "WHERE enabled=1"
        return [
            _site_out(row)
            for row in conn.execute(f"SELECT * FROM sites {where} ORDER BY name")
        ]

    return await get_db().read(_read)


@router.post("", response_model=SiteOut)
async def create_site(request: Request, body: SiteCreate) -> SiteOut | HTMLResponse:
    def _write(conn: sqlite3.Connection) -> SiteOut:
        cur = conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone,
                 rain_threshold_mm)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                body.name,
                body.forecast_lat,
                body.forecast_lon,
                body.elevation_m,
                body.timezone,
                body.rain_threshold_mm,
            ),
        )
        row = conn.execute(
            "SELECT * FROM sites WHERE id=?", (cur.lastrowid,)
        ).fetchone()
        if row is None:
            raise RuntimeError("site insert failed")
        site_id = int(row["id"])
        # A reused id may still carry keys a pre-0.16.7 delete left behind.
        _clear_site_state(conn, site_id)
        # The site's one time zone generation, which every pair and truth
        # row of the site carries as its key.
        ensure_site_generation(conn, site_id)
        # A new site starts fresh at its next local date. It has no record
        # yet, so its current local date is today's date in its own zone.
        now = utc_now()
        today = now.astimezone(ZoneInfo(str(row["timezone"]))).date()
        start_site_days(conn, site_id, today + timedelta(days=1), now_utc=now)
        return _site_out(row)

    site = await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_site_cards

        return await render_site_cards(request)
    return site


@router.get("/{site_id}", response_model=SiteOut)
async def get_site(site_id: int) -> SiteOut:
    def _read(conn: sqlite3.Connection) -> SiteOut:
        row = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
        if row is None:
            raise ApiError(404, "site not found")
        return _site_out(row)

    return await get_db().read(_read)


@router.put("/{site_id}", response_model=SiteOut)
async def update_site(
    request: Request, site_id: int, body: SiteUpdate
) -> SiteOut | HTMLResponse:
    def _write(conn: sqlite3.Connection) -> SiteOut:
        row = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
        if row is None:
            raise ApiError(404, "site not found")
        if body.name is not None:
            conn.execute("UPDATE sites SET name=? WHERE id=?", (body.name, site_id))
        if body.enabled is not None:
            conn.execute(
                "UPDATE sites SET enabled=? WHERE id=?",
                (1 if body.enabled else 0, site_id),
            )
        if body.rain_threshold_mm is not None:
            conn.execute(
                "UPDATE sites SET rain_threshold_mm=? WHERE id=?",
                (body.rain_threshold_mm, site_id),
            )
        updated = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
        if updated is None:
            raise RuntimeError("site update failed")
        return _site_out(updated)

    site = await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_site_cards

        return await render_site_cards(request)
    return site


def _clear_site_state(conn: sqlite3.Connection, site_id: int) -> None:
    """Drop the settings row keyed by ``site_id``.

    Site ids are reused (INTEGER PRIMARY KEY, no AUTOINCREMENT) and the
    settings table has no foreign key, so the sites cascade never reaches it.
    """
    conn.execute(
        "DELETE FROM settings WHERE key = ?", (f"{SNAPSHOT_TIME_KEY}:{site_id}",)
    )


@router.delete("/{site_id}", response_model=None)
async def delete_site(request: Request, site_id: int) -> dict[str, bool] | HTMLResponse:
    def _write(conn: sqlite3.Connection) -> None:
        cur = conn.execute("DELETE FROM sites WHERE id=?", (site_id,))
        if cur.rowcount == 0:
            raise ApiError(404, "site not found")
        _clear_site_state(conn, site_id)

    await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_site_cards

        return await render_site_cards(request)
    return {"deleted": True}


def _wants_html(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
