"""Site routes."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from wxverify.api.errors import ApiError
from wxverify.api.schemas import SiteCreate, SiteOut, SiteUpdate
from wxverify.db.connection import get_db
from wxverify.scoring.engine import pair_and_score

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
            conn.execute(
                "DELETE FROM forecast_pairs WHERE site_id=? AND variable='precip'",
                (site_id,),
            )
            conn.execute(
                "DELETE FROM score_cache WHERE site_id=? AND variable='precip'",
                (site_id,),
            )
            pair_and_score(conn, site_id)
        updated = conn.execute("SELECT * FROM sites WHERE id=?", (site_id,)).fetchone()
        if updated is None:
            raise RuntimeError("site update failed")
        return _site_out(updated)

    site = await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_site_cards

        return await render_site_cards(request)
    return site


@router.delete("/{site_id}", response_model=None)
async def delete_site(request: Request, site_id: int) -> dict[str, bool] | HTMLResponse:
    def _write(conn: sqlite3.Connection) -> None:
        cur = conn.execute("DELETE FROM sites WHERE id=?", (site_id,))
        if cur.rowcount == 0:
            raise ApiError(404, "site not found")

    await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_site_cards

        return await render_site_cards(request)
    return {"deleted": True}


def _wants_html(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
