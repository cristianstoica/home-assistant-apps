"""HTML routes and fragment renderers for the wxverify UI."""

from __future__ import annotations

import sqlite3
from typing import Literal

from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse

from wxverify.db.connection import get_db
from wxverify.db.snapshot import read_snapshot
from wxverify.forecast.data import forecast_fingerprint
from wxverify.forecast.page import ForecastPage, build_forecast_page
from wxverify.forecast.service import DAY_COUNT
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


def _clamp_day(day: int) -> int:
    """A requested Forecast day clamped to 0-7 (0 is today)."""
    return max(0, min(DAY_COUNT - 1, day))


def _resolve_site(
    conn: sqlite3.Connection,
    site_id: int | None,
    enabled_sites: list[SiteView] | None = None,
) -> SiteView | None:
    """Site resolution shared by the page and poll paths.

    Contract preserved verbatim from _load_forecast_context: an EXPLICIT
    site_id resolves through `load_site`, which selects by id without applying
    the enabled-site filter that `load_sites` uses -- a disabled site resolves
    normally on both the page and the poll, and the enabled-site list is never
    loaded for this branch.
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
    conn: sqlite3.Connection, site_id: int | None, day: int
) -> dict[str, object]:
    """Resolve the site (first enabled when unspecified) and build its page.

    The page (:func:`build_forecast_page`) opens day ``day``. Every read
    here, and the fingerprint the page carries, must come from one database
    state, so the caller must already hold a read snapshot on ``conn``:
    ``_read_forecast_context``, or the tiles poll's own snapshot. Raises
    RuntimeError when no transaction is open.
    """
    if not conn.in_transaction:
        raise RuntimeError("_load_forecast_context needs the caller's read snapshot")
    sites = load_sites(conn, include_disabled=False)
    site = _resolve_site(conn, site_id, enabled_sites=sites)
    page: ForecastPage | None = None
    if site is not None:
        page = build_forecast_page(
            conn,
            site_id=site.id,
            timezone=site.timezone,
            rain_threshold_mm=site.rain_threshold_mm,
            selected_day=day,
        )
    return {"sites": sites, "site": site, "page": page, "day": day}


def _read_forecast_context(
    conn: sqlite3.Connection, site_id: int | None, day: int, *, label: str
) -> dict[str, object]:
    """Run ``_load_forecast_context`` inside one read snapshot on ``conn``.

    The page's reads (site list, site, samples, fingerprint, freshness, depths
    and rankings) then describe one database state, so the fingerprint in the
    page's poll URL is the one its region was built from.
    """
    with read_snapshot(conn, label=label):
        return _load_forecast_context(conn, site_id, day)


@router.get("/", response_class=HTMLResponse)
async def index(
    request: Request, site: int | None = None, day: int = 1
) -> HTMLResponse:
    day = _clamp_day(day)
    context = await get_db().read(
        lambda conn: _read_forecast_context(conn, site, day, label="forecast_page")
    )
    return render(request, "forecast/show.html", **context)


@router.get("/forecast", response_class=HTMLResponse)
async def forecast_page(
    request: Request, site: int | None = None, day: int = 1
) -> HTMLResponse:
    day = _clamp_day(day)
    context = await get_db().read(
        lambda conn: _read_forecast_context(conn, site, day, label="forecast_page")
    )
    return render(request, "forecast/show.html", **context)


@router.get("/forecast/tiles")
async def forecast_tiles(
    request: Request, site: int, fingerprint: str = "", day: int = 1
) -> Response:
    """Auto-poll target: 204 (no swap) unless the site's forecast fingerprint moved.

    The fingerprint is compared BEFORE the page is built, and the comparison
    and the build run inside one read snapshot, so the fingerprint a rebuilt
    fragment carries is the one its region was built from. It is
    :func:`forecast_fingerprint`, which moves when a new sample is stored for
    the site or one of its last-usable-fetch stamps moves to another whole
    second. A stamp move within the same second leaves it unchanged, and stamp
    moves in opposite directions can cancel out. A write that commits while
    this request runs is not seen by it. The next poll sees that write only if
    it moved the fingerprint; any other change shows on the next page load. An
    unchanged fingerprint is answered without paying for a build whose result
    would be discarded. On a 204 htmx leaves the DOM untouched -- including
    the hx-get that carries the old fingerprint, which retains the token for
    subsequent comparisons. A rebuilt region opens day ``day`` (clamped to
    0-7), the day the region's own poll URL carries. A site that no longer
    exists answers 200 with the "Site not found." region, which has no
    hx-get, so the poll stops.
    """
    day = _clamp_day(day)

    def _poll(
        conn: sqlite3.Connection,
    ) -> Literal["unknown", "unchanged"] | dict[str, object]:
        with read_snapshot(conn, label="forecast_tiles"):
            site_view = _resolve_site(conn, site)
            if site_view is None:
                return "unknown"
            if forecast_fingerprint(conn, site_id=site_view.id) == fingerprint:
                return "unchanged"
            return _load_forecast_context(conn, site, day)

    outcome = await get_db().read(_poll)
    if outcome == "unknown":
        return render_fragment(
            request, "forecast/_tiles.html", site=None, page=None, day=day
        )
    if outcome == "unchanged":
        return Response(status_code=204)
    page = outcome.get("page")
    if not isinstance(page, ForecastPage) or page.fingerprint == fingerprint:
        return Response(status_code=204)
    return render_fragment(request, "forecast/_tiles.html", **outcome)


@router.get("/forecast/day", response_class=HTMLResponse)
async def forecast_day(request: Request, site: int, day: int) -> HTMLResponse:
    """The whole Forecast region with day ``day`` (clamped to 0-7) open.

    A day click swaps it in for ``#forecast-tiles``; it is built in one read
    snapshot, as the page is. An unknown site gets the "Site not found."
    region.
    """
    day = _clamp_day(day)
    context = await get_db().read(
        lambda conn: _read_forecast_context(conn, site, day, label="forecast_day")
    )
    return render_fragment(request, "forecast/_tiles.html", **context)


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
    lead: str = "D+1",
) -> HTMLResponse:
    context = await get_db().read(
        lambda conn: load_overlay(
            conn,
            site_id=site,
            variable=variable,
            feed_id=feed_id,
            lead=lead,
        )
    )
    return render(request, "overlay/show.html", **context)


async def render_site_cards(request: Request) -> HTMLResponse:
    sites = await get_db().read(load_sites)
    return render_fragment(request, "sites/_cards.html", sites=sites)


async def render_station_cluster(request: Request, site_id: int) -> HTMLResponse:
    site = await get_db().read(lambda conn: load_site(conn, site_id))
    return render_fragment(request, "sites/_station_cluster.html", site=site)


async def render_feed_toggles(
    request: Request, site_id: int, *, feed_id: int, enabled: bool, changed: bool
) -> HTMLResponse:
    """Render a site's subscription section after a toggle write.

    The section's status line names the feed and its new state, and says
    whether the write changed it (``changed``) or found it already set.
    """
    site = await get_db().read(lambda conn: load_site(conn, site_id))
    toggle_notice: str | None = None
    if site is not None:
        label = next(
            (feed.label for feed in site.feeds if feed.id == feed_id),
            f"Feed {feed_id}",
        )
        state = "on" if enabled else "off"
        toggle_notice = (
            f"Saved. {label} is now {state} for this site."
            if changed
            else f"No change. {label} was already {state} for this site."
        )
    return render_fragment(
        request, "sites/_feed_toggles.html", site=site, toggle_notice=toggle_notice
    )


def wants_html_fragment(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
