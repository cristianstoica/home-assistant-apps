"""HTML routes and fragment renderers for the wxverify UI."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse

from wxverify.db.connection import get_db
from wxverify.forecast.service import ForecastView, build_forecast
from wxverify.scoring.composite import enqueue_composite_rescore
from wxverify.web.context import (
    SiteView,
    load_dashboard,
    load_ops,
    load_overlay,
    load_site,
    load_sites,
)
from wxverify.web.render import render, render_fragment

router = APIRouter(include_in_schema=False)


def _load_forecast_context(
    conn: sqlite3.Connection, site_id: int | None
) -> dict[str, object]:
    """Resolve the site (first enabled when unspecified) and build the view."""
    sites = load_sites(conn, include_disabled=False)
    site = (
        load_site(conn, site_id)
        if site_id is not None
        else (sites[0] if sites else None)
    )
    view: ForecastView | None = None
    if site is not None:
        view = build_forecast(
            conn,
            site_id=site.id,
            timezone=site.timezone,
            rain_threshold_mm=site.rain_threshold_mm,
        )
    return {"sites": sites, "site": site, "view": view}


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, site: int | None = None) -> HTMLResponse:
    context = await get_db().read(lambda conn: _load_forecast_context(conn, site))
    return render(request, "forecast/show.html", **context)


@router.get("/forecast", response_class=HTMLResponse)
async def forecast_page(request: Request, site: int | None = None) -> HTMLResponse:
    context = await get_db().read(lambda conn: _load_forecast_context(conn, site))
    return render(request, "forecast/show.html", **context)


@router.get("/forecast/tiles")
async def forecast_tiles(
    request: Request, site: int, fingerprint: str = ""
) -> Response:
    """Auto-poll target: 204 (no swap) unless newer samples have landed.

    On a 204 htmx leaves the DOM untouched; when the data changed, the
    ``outerHTML`` swap replaces only ``#forecast-tiles``, so an open day
    detail (a sibling element) is left intact across a tile poll.
    """
    context = await get_db().read(lambda conn: _load_forecast_context(conn, site))
    view = context.get("view")
    if not isinstance(view, ForecastView) or view.fingerprint == fingerprint:
        return Response(status_code=204)
    return render_fragment(request, "forecast/_tiles.html", **context)


@router.get("/forecast/day", response_class=HTMLResponse)
async def forecast_day(request: Request, site: int, day: int) -> HTMLResponse:
    """Inline hourly drill-down fragment for one tile."""
    day = max(0, min(7, day))
    site_view = await get_db().read(lambda conn: load_site(conn, site))
    return render_fragment(
        request, "forecast/_day_detail.html", site=site_view, day=day
    )


@router.get("/sites", response_class=HTMLResponse)
async def sites_page(request: Request) -> HTMLResponse:
    sites = await get_db().read(load_sites)
    return render(request, "sites/list.html", sites=sites)


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(
    request: Request,
    site: int | None = None,
    variable: str = "temperature",
    window: str = "rolling",
    lead: str = "D+1",
) -> HTMLResponse:
    context = await get_db().read(
        lambda conn: load_dashboard(
            conn,
            site_id=site,
            variable=variable,
            window=window,
            lead=lead,
        )
    )
    # Second composite enqueue site (mirrors /api/composite): the read above is
    # closed before this write, and the cooldown-guarded composite-only helper
    # does the enqueue. The resolved site comes from the context because
    # load_dashboard defaults to the first enabled site when `site` is None.
    resolved_site = context.get("site")
    if context.get("composite_status") in ("stale", "rebuilding") and isinstance(
        resolved_site, SiteView
    ):
        resolved_site_id = resolved_site.id
        await get_db().write(
            lambda conn: enqueue_composite_rescore(conn, resolved_site_id)
        )
    return render(request, "dashboard/show.html", **context)


@router.get("/ops", response_class=HTMLResponse)
async def ops_page(request: Request) -> HTMLResponse:
    context = await get_db().read(load_ops)
    return render(request, "ops/show.html", **context)


@router.get("/overlay", response_class=HTMLResponse)
async def overlay_page(
    request: Request,
    site: int | None = None,
    variable: str = "temperature",
    feed_id: int | None = None,
) -> HTMLResponse:
    context = await get_db().read(
        lambda conn: load_overlay(
            conn,
            site_id=site,
            variable=variable,
            feed_id=feed_id,
        )
    )
    return render(request, "overlay/show.html", **context)


async def render_site_cards(request: Request) -> HTMLResponse:
    sites = await get_db().read(load_sites)
    return render_fragment(request, "sites/_cards.html", sites=sites)


async def render_station_cluster(request: Request, site_id: int) -> HTMLResponse:
    site = await get_db().read(lambda conn: load_site(conn, site_id))
    return render_fragment(request, "sites/_station_cluster.html", site=site)


async def render_feed_toggles(request: Request, site_id: int) -> HTMLResponse:
    site = await get_db().read(lambda conn: load_site(conn, site_id))
    return render_fragment(request, "sites/_feed_toggles.html", site=site)


async def render_backfill(request: Request) -> HTMLResponse:
    context = await get_db().read(load_ops)
    return render_fragment(
        request, "ops/_backfill.html", sites=context["sites"], rows=context["backfill"]
    )


def wants_html_fragment(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
