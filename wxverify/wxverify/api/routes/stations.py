"""Station cluster routes."""

from __future__ import annotations

import sqlite3

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from wxverify.api.errors import ApiError
from wxverify.api.schemas import StationCreate, StationOut, StationUpdate
from wxverify.collection.budget import reserve_budget
from wxverify.core.secrets import resolve_secret
from wxverify.db.connection import get_db
from wxverify.db.queue import enqueue_if_absent
from wxverify.obs.elevation import lookup_elevation_m
from wxverify.obs.pws_adapter import validate_station
from wxverify.scoring.consensus import materialize_consensus
from wxverify.scoring.engine import pair_and_score
from wxverify.worker.control import JobDeferred
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    clear_domain_backoff,
    record_http_backoff,
    source_domain,
)

router = APIRouter(prefix="/api/sites/{site_id}/stations", tags=["stations"])


def _station_out(row: sqlite3.Row) -> StationOut:
    return StationOut(
        id=int(row["id"]),
        site_id=int(row["site_id"]),
        pws_station_id=str(row["pws_station_id"]),
        lat=float(row["lat"]),
        lon=float(row["lon"]),
        dem_elevation_m=float(row["dem_elevation_m"]),
        enabled=bool(row["enabled"]),
    )


@router.get("", response_model=list[StationOut])
async def list_stations(site_id: int) -> list[StationOut]:
    def _read(conn: sqlite3.Connection) -> list[StationOut]:
        return [
            _station_out(row)
            for row in conn.execute(
                "SELECT * FROM stations WHERE site_id=? ORDER BY pws_station_id",
                (site_id,),
            )
        ]

    return await get_db().read(_read)


@router.post("", response_model=StationOut)
async def create_station(
    request: Request, site_id: int, body: StationCreate
) -> StationOut | HTMLResponse:
    api_key = resolve_secret("weathercom")
    if not api_key:
        raise ApiError(503, "weathercom key is not configured")

    def _reserve(conn: sqlite3.Connection) -> None:
        if (
            conn.execute("SELECT 1 FROM sites WHERE id=?", (site_id,)).fetchone()
            is None
        ):
            raise ApiError(404, "site not found")
        check_domain_backoff(conn, source_domain("weathercom"))
        reserve_budget(conn, "weathercom", 1)

    await get_db().write(_reserve)
    try:
        pws = await validate_station(body.pws_station_id, api_key)
    except httpx.HTTPStatusError as exc:
        next_attempt_at = await get_db().write(
            lambda conn, response=exc.response: record_http_backoff(conn, response)
        )
        if next_attempt_at is not None:
            raise JobDeferred(next_attempt_at) from exc
        raise

    def _reserve_elevation(conn: sqlite3.Connection) -> None:
        if (
            conn.execute("SELECT 1 FROM sites WHERE id=?", (site_id,)).fetchone()
            is None
        ):
            raise ApiError(404, "site not found")
        check_domain_backoff(conn, source_domain("open-meteo"))
        reserve_budget(conn, "open-meteo", 1)

    await get_db().write(_reserve_elevation)
    try:
        dem = await lookup_elevation_m(pws.lat, pws.lon)
    except httpx.HTTPStatusError as exc:
        next_attempt_at = await get_db().write(
            lambda conn, response=exc.response: record_http_backoff(conn, response)
        )
        if next_attempt_at is not None:
            raise JobDeferred(next_attempt_at) from exc
        raise

    def _write(conn: sqlite3.Connection) -> StationOut:
        if (
            conn.execute("SELECT 1 FROM sites WHERE id=?", (site_id,)).fetchone()
            is None
        ):
            raise ApiError(404, "site not found")
        cur = conn.execute(
            """
            INSERT INTO stations
                (site_id, pws_station_id, lat, lon, dem_elevation_m)
            VALUES (?, ?, ?, ?, ?)
            """,
            (site_id, pws.station_id, pws.lat, pws.lon, dem),
        )
        clear_domain_backoff(conn, source_domain("weathercom"))
        clear_domain_backoff(conn, source_domain("open-meteo"))
        enqueue_if_absent(
            conn, "backfill_site", site_id, f"backfill:{site_id}", {"site_id": site_id}
        )
        enqueue_if_absent(conn, "fetch_obs", site_id, "obs", {})
        row = conn.execute(
            "SELECT * FROM stations WHERE id=?", (cur.lastrowid,)
        ).fetchone()
        if row is None:
            raise RuntimeError("station insert failed")
        return _station_out(row)

    station = await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_station_cluster

        return await render_station_cluster(request, site_id)
    return station


@router.put("/{station_id}", response_model=StationOut)
async def update_station(
    request: Request, site_id: int, station_id: int, body: StationUpdate
) -> StationOut | HTMLResponse:
    def _write(conn: sqlite3.Connection) -> StationOut:
        row = _get_station(conn, site_id, station_id)
        if (
            not body.enabled
            and _enabled_station_count(conn, site_id) <= 1
            and row["enabled"]
        ):
            raise ApiError(409, "site must retain at least one enabled station")
        conn.execute(
            "UPDATE stations SET enabled=? WHERE id=? AND site_id=?",
            (1 if body.enabled else 0, station_id, site_id),
        )
        _rematerialize_station_hours(conn, site_id, station_id)
        pair_and_score(conn, site_id)
        updated = _get_station(conn, site_id, station_id)
        return _station_out(updated)

    station = await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_station_cluster

        return await render_station_cluster(request, site_id)
    return station


@router.delete("/{station_id}", response_model=None)
async def delete_station(
    request: Request, site_id: int, station_id: int
) -> dict[str, bool] | HTMLResponse:
    def _write(conn: sqlite3.Connection) -> None:
        row = _get_station(conn, site_id, station_id)
        if bool(row["enabled"]) and _enabled_station_count(conn, site_id) <= 1:
            raise ApiError(409, "site must retain at least one enabled station")
        keys = conn.execute(
            """
            SELECT DISTINCT variable, valid_at
            FROM station_observations
            WHERE station_id=?
            """,
            (station_id,),
        ).fetchall()
        conn.execute(
            "DELETE FROM stations WHERE id=? AND site_id=?", (station_id, site_id)
        )
        for key in keys:
            materialize_consensus(
                conn,
                site_id=site_id,
                variable=str(key["variable"]),
                valid_at=str(key["valid_at"]),
            )
        pair_and_score(conn, site_id)

    await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_station_cluster

        return await render_station_cluster(request, site_id)
    return {"deleted": True}


def _get_station(
    conn: sqlite3.Connection, site_id: int, station_id: int
) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM stations WHERE id=? AND site_id=?", (station_id, site_id)
    ).fetchone()
    if row is None:
        raise ApiError(404, "station not found")
    return row


def _enabled_station_count(conn: sqlite3.Connection, site_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM stations WHERE site_id=? AND enabled=1", (site_id,)
    ).fetchone()
    return 0 if row is None else int(row["n"])


def _rematerialize_station_hours(
    conn: sqlite3.Connection, site_id: int, station_id: int
) -> None:
    keys = conn.execute(
        """
        SELECT DISTINCT variable, valid_at
        FROM station_observations
        WHERE station_id=?
        """,
        (station_id,),
    ).fetchall()
    for key in keys:
        materialize_consensus(
            conn,
            site_id=site_id,
            variable=str(key["variable"]),
            valid_at=str(key["valid_at"]),
        )


def _wants_html(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
