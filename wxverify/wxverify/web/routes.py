"""HTML routes and fragment renderers for the wxverify UI."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse

from wxverify.db.connection import get_db
from wxverify.forecast.data import samples_fingerprint
from wxverify.forecast.service import ForecastView, build_forecast
from wxverify.scoring.rescore import schedule_score_rescore
from wxverify.web.context import (
    SiteView,
    load_backfill,
    load_dashboard,
    load_ops,
    load_overlay,
    load_site,
    load_sites,
)
from wxverify.web.render import render, render_fragment

router = APIRouter(include_in_schema=False)


def _resolve_site(
    conn: sqlite3.Connection,
    site_id: int | None,
    enabled_sites: list[SiteView] | None = None,
) -> SiteView | None:
    """Site resolution shared by the page and poll paths.

    Contract preserved verbatim from _load_forecast_context: an EXPLICIT
    site_id resolves through load_site, which does not filter on `enabled`
    (web/context.py:219) -- a disabled site resolves normally on both the page
    and the poll, and the enabled-site list is never loaded for this branch.
    Only the implicit (site_id is None) branch is restricted to enabled
    sites, via load_sites(include_disabled=False) -- reusing the caller's
    already-loaded list when one is supplied via `enabled_sites`, otherwise
    loading it here. None is returned only for an unknown id, or when no
    enabled site exists at all.
    """
    if site_id is not None:
        return load_site(conn, site_id)
    sites = (
        enabled_sites
        if enabled_sites is not None
        else load_sites(conn, include_disabled=False)
    )
    return sites[0] if sites else None


def _load_forecast_context(
    conn: sqlite3.Connection, site_id: int | None
) -> dict[str, object]:
    """Resolve the site (first enabled when unspecified) and build the view."""
    sites = load_sites(conn, include_disabled=False)
    site = _resolve_site(conn, site_id, enabled_sites=sites)
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

    The fingerprint is computed BEFORE the view is built. It is a single
    MAX(id) over the site's samples and is the same value the full build would
    have reported, so an unchanged fingerprint is answered without paying for a
    build whose result would be discarded. On a 204 htmx leaves the DOM
    untouched -- including the hx-get that carries the old fingerprint, which
    stays correct precisely because nothing changed. When the data did change,
    the outerHTML swap replaces only #forecast-tiles, so an open day detail (a
    sibling element) is left intact across a tile poll.
    """

    def _poll(conn: sqlite3.Connection) -> dict[str, object] | None:
        site_view = _resolve_site(conn, site)
        if site_view is None:
            return None
        if samples_fingerprint(conn, site_id=site_view.id) == fingerprint:
            return None
        return _load_forecast_context(conn, site)

    context = await get_db().read(_poll)
    view = context.get("view") if context is not None else None
    if (
        context is None
        or not isinstance(view, ForecastView)
        or view.fingerprint == fingerprint
    ):
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
    # closed; the enqueue is scheduled post-response via the fire-and-forget
    # scheduler (cooldown-guarded), so it never gates the render. The resolved
    # site comes from the context because load_dashboard defaults to the first
    # enabled site when `site` is None.
    resolved_site = context.get("site")
    if context.get("composite_status") in ("stale", "rebuilding") and isinstance(
        resolved_site, SiteView
    ):
        schedule_score_rescore(resolved_site.id)
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
    rows = await get_db().read(load_backfill)
    return render_fragment(request, "ops/_backfill.html", rows=rows)


def wants_html_fragment(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
