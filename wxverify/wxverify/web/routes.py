"""HTML routes and fragment renderers for the wxverify UI."""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from wxverify.db.connection import get_db
from wxverify.scoring.composite import enqueue_composite_rescore
from wxverify.web.context import (
    SiteView,
    load_dashboard,
    load_ops,
    load_overlay,
    load_site,
    load_sites,
)
from wxverify.web.render import ingress_url, render, render_fragment

router = APIRouter(include_in_schema=False)


@router.get("/")
async def index(request: Request) -> RedirectResponse:
    return RedirectResponse(ingress_url(request, "/dashboard"))


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
