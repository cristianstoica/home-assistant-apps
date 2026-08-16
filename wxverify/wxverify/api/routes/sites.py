"""Site routes."""

from __future__ import annotations

import logging
import sqlite3

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from wxverify.api.errors import ApiError
from wxverify.api.schemas import (
    SiteCreate,
    SiteOut,
    SiteUpdate,
    TimezoneCorrectionIn,
)
from wxverify.db.connection import get_db
from wxverify.db.tz_generations import (
    CorrectionAlreadyBuilding,
    TimezoneSiteNotFound,
    UnknownTimezone,
    ensure_published_generation,
    published_generation_clause,
    start_retrospective_correction,
)
from wxverify.scoring.engine import pair_and_score

logger = logging.getLogger(__name__)

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
        # New sites get their initial published timezone generation and
        # published-pointer row immediately — the same seed migrate_v4
        # applies to pre-existing sites.
        ensure_published_generation(conn, int(row["id"]))
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
            # Published generation only (§13): a building correction
            # generation's precip pairs belong to the correction chain;
            # deleting them here mid-chain would corrupt the rebuild.
            # Known limitation: a threshold change landing DURING a
            # correction build leaves already-rebuilt building days on the
            # old threshold. The post-flip rescore does NOT heal them:
            # pair_and_score's real-pair lane is INSERT OR IGNORE, so
            # existing generation-tagged pairs keep their stale cat_*
            # flags into the published generation (only the
            # multimodel-mean lane recomputes). They stay stale until the
            # next threshold edit's published-scoped delete-and-recreate
            # here, or a consensus mutation of those specific hours.
            conn.execute(
                f"""
                DELETE FROM forecast_pairs
                WHERE site_id=? AND variable='precip'
                  AND {published_generation_clause("forecast_pairs")}
                """,
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


@router.post("/{site_id}/timezone-correction", response_model=None)
async def start_timezone_correction(
    site_id: int, body: TimezoneCorrectionIn
) -> dict[str, object]:
    """Start a retrospective timezone correction for one site.

    Behind the app-level ``MutationGuard`` by construction; this route adds
    no exemption. ``confirm`` mitigates accidental activation and is not
    authorization -- see ADR-0003, which classifies this action tier (c),
    HTTP-eligible.

    Refusals are mapped by exception TYPE, never by message text, and there
    is deliberately no ``except ValueError`` catch-all: a refusal this route
    has not reasoned about surfaces as a 500 rather than being guessed at.
    """
    if body.confirm is not True:
        raise ApiError(400, "confirmation required")

    def _write(conn: sqlite3.Connection) -> dict[str, object]:
        from wxverify.worker.verification_run import verification_chain_active

        # Route-owned refusal (ADR-0003 clause 1(b)): a correction's flip
        # invalidates a verification run pinned to the generation it started
        # on, so refuse while one is queued or running for this site. Runs
        # inside the same write transaction as the domain call, so it cannot
        # go stale. The CLI path is deliberately NOT subject to it.
        if verification_chain_active(conn, site_id):
            raise ApiError(
                409, "a verification run is active for this site; correction refused"
            )
        try:
            generation_id = start_retrospective_correction(conn, site_id, body.timezone)
        except UnknownTimezone as exc:
            raise ApiError(400, str(exc)) from exc
        except TimezoneSiteNotFound as exc:
            raise ApiError(404, str(exc)) from exc
        except CorrectionAlreadyBuilding as exc:
            raise ApiError(409, str(exc)) from exc
        logger.info(
            "timezone correction started site=%s generation=%s timezone=%s source=ops",
            site_id,
            generation_id,
            body.timezone,
        )
        return {
            "site_id": site_id,
            "generation_id": generation_id,
            "timezone": body.timezone,
            "state": "building",
        }

    return await get_db().write(_write)


def _wants_html(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
