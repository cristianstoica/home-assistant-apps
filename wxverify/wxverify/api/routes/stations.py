"""Station cluster routes."""

from __future__ import annotations

import logging
import os
import sqlite3
import traceback
from collections.abc import Awaitable
from datetime import datetime
from typing import Final, Literal

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from wxverify.api.errors import ApiError
from wxverify.api.schemas import StationCreate, StationOut, StationUpdate
from wxverify.collection.budget import reserve_budget
from wxverify.core.secrets import resolve_secret
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro, utc_now
from wxverify.db.connection import get_db
from wxverify.db.queue import EnqueueResult, enqueue_if_absent
from wxverify.db.site_days import (
    StationChange,
    current_day_has_other_station,
    record_station_change,
)
from wxverify.obs.elevation import lookup_elevation_m
from wxverify.obs.pws_adapter import UpstreamPayloadError, validate_station
from wxverify.worker.control import JobDeferred
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    clear_domain_backoff,
    record_http_backoff,
    source_domain,
)
from wxverify.worker.station_pacing import acquire_within, weathercom_call_lock

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sites/{site_id}/stations", tags=["stations"])

# How long create_station waits for the weather.com call lock before giving up
# with a 503. Provisional: meant to sit inside common reverse-proxy request
# timeouts; the ingress proxy's own timeout is not verified. Read at call time
# from the module, never bound as a default argument, so tests can shorten it.
ADD_STATION_CALL_WAIT_SECONDS: Final = 30.0

# The answer to an add of an id held at another site, removed there or not.
# It names no workaround: a station cannot move between sites.
_ANOTHER_SITE_TEXT: Final = (
    "station belongs to another site: a station stays with the site it was "
    "first added to, even after it is removed"
)

_Provider = Literal["weathercom", "open-meteo"]
_FailureReason = Literal[
    "key_rejected",
    "unknown_station",
    "refused",
    "unavailable",
    "timeout",
    "unreachable",
    "unreadable",
]

# Never 401/403: the web page reads a 403 as an expired session.
_ANSWER_STATUS: Final[dict[_FailureReason, int]] = {
    "key_rejected": 502,
    "unknown_station": 422,
    "refused": 502,
    "unavailable": 502,
    "timeout": 504,
    "unreachable": 502,
    "unreadable": 502,
}

# Fixed texts only: no exception text, URL, key, station id or coordinate.
# key_rejected and unknown_station are never produced for open-meteo.
_ANSWER_TEXT: Final[dict[tuple[_Provider, _FailureReason], str]] = {
    ("weathercom", "key_rejected"): (
        "weather.com rejected the API key (HTTP {status}); station was not "
        "added. Check the weather.com API key in the add-on configuration."
    ),
    ("weathercom", "unknown_station"): (
        "weather.com has no station with this ID, or the station is not "
        "reporting; station was not added. Check the station ID."
    ),
    ("weathercom", "refused"): (
        "weather.com refused the request (HTTP {status}); station was not added."
    ),
    ("weathercom", "unavailable"): (
        "weather.com is unavailable (HTTP {status}); station was not added. "
        "Try again later."
    ),
    ("weathercom", "timeout"): (
        "weather.com did not answer in time; station was not added. Try again shortly."
    ),
    ("weathercom", "unreachable"): (
        "Could not reach weather.com; station was not added. Try again shortly."
    ),
    ("weathercom", "unreadable"): (
        "weather.com sent a response that could not be read; station was not added."
    ),
    ("open-meteo", "refused"): (
        "Open-Meteo refused the elevation lookup (HTTP {status}); station was "
        "not added."
    ),
    ("open-meteo", "unavailable"): (
        "Open-Meteo is unavailable (HTTP {status}); station was not added. "
        "Try again later."
    ),
    ("open-meteo", "timeout"): (
        "Open-Meteo did not answer the elevation lookup in time; station was "
        "not added. Try again shortly."
    ),
    ("open-meteo", "unreachable"): (
        "Could not reach Open-Meteo for the elevation lookup; station was not "
        "added. Try again shortly."
    ),
    ("open-meteo", "unreadable"): (
        "Open-Meteo sent an elevation response that could not be read; station "
        "was not added."
    ),
}


def _classify_provider_failure(
    exc: Exception, provider: _Provider
) -> tuple[_FailureReason, int | None]:
    """Reason and upstream status for a failed station-add provider call.

    Reads only the exception's type, its HTTP status and the payload
    diagnostics' enum fields -- never its text, request or URL.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status == 429 or status >= 500:
            return "unavailable", status
        if provider == "weathercom" and status in (401, 403):
            return "key_rejected", status
        if provider == "weathercom" and status == 404:
            return "unknown_station", status
        return "refused", status
    if isinstance(exc, UpstreamPayloadError):
        diagnostics = exc.diagnostics
        if provider == "weathercom" and (
            diagnostics.kind == "no_content" or diagnostics.body == "empty"
        ):
            return "unknown_station", diagnostics.status
        return "unreadable", diagnostics.status
    # Timeout first: httpx.ReadTimeout is also an httpx.TransportError.
    if isinstance(exc, httpx.TimeoutException | TimeoutError):
        return "timeout", None
    if isinstance(exc, httpx.TransportError):
        return "unreachable", None
    return "unreadable", None


_CAUSE_NAMES_LIMIT: Final = 8


def _cause_names(exc: BaseException) -> str:
    """Class names of the retained chain below exc, suppressed context included.

    Names only, never text. Prefers __cause__, else __context__ even when
    __suppress_context__ is set; stops at None, a repeat or the limit.
    ExceptionGroup members are not walked.
    """
    names: list[str] = []
    seen = {id(exc)}
    current = exc
    while len(names) < _CAUSE_NAMES_LIMIT:
        nxt = current.__cause__
        if nxt is None:
            nxt = current.__context__
        if nxt is None or id(nxt) in seen:
            break
        seen.add(id(nxt))
        names.append(type(nxt).__name__)
        current = nxt
    return "<-".join(names) or "-"


def _innermost_package_frame(exc: BaseException) -> str:
    """`<file>:<line> in <function>` of the innermost wxverify frame; no values.

    Attribute reads only: no frame locals, no source line, no StackSummary.
    """
    where = "-"
    for frame, lineno in traceback.walk_tb(exc.__traceback__):
        module = frame.f_globals.get("__name__")
        if isinstance(module, str) and module.startswith("wxverify."):
            code = frame.f_code
            where = f"{os.path.basename(code.co_filename)}:{lineno} in {code.co_name}"
    return where


async def _provider_call[T](call: Awaitable[T], *, provider: _Provider) -> T:
    """Await one station-add provider call; answer any failure safely.

    A provider error's text and traceback can carry the request URL (the
    weather.com key and station id, or the station's coordinates). The except
    block only records facts; the backoff write and every raise come after
    it, so nothing raised here has the provider error as __cause__ or
    __context__. CancelledError is a BaseException and passes through.
    """
    try:
        return await call
    except Exception as exc:
        reason, upstream_status = _classify_provider_failure(exc, provider)
        error_type = type(exc).__name__
        kind = exc.diagnostics.kind if isinstance(exc, UpstreamPayloadError) else "-"
        cause = _cause_names(exc)
        where = _innermost_package_frame(exc) if reason == "unreadable" else "-"
        http_response = exc.response if isinstance(exc, httpx.HTTPStatusError) else None
    # Reached only through the except block: the try body returns.
    if http_response is not None:
        next_attempt_at = await get_db().write(
            lambda conn, response=http_response: record_http_backoff(conn, response)
        )
        if next_attempt_at is not None:
            raise JobDeferred(next_attempt_at)
    logger.warning(
        "station add failed provider=%s reason=%s status=%s error_type=%s "
        "kind=%s cause=%s where=%s",
        provider,
        reason,
        "-" if upstream_status is None else upstream_status,
        error_type,
        kind,
        cause,
        where,
    )
    raise ApiError(
        _ANSWER_STATUS[reason],
        _ANSWER_TEXT[(provider, reason)].format(status=upstream_status),
    )


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
                "SELECT * FROM stations WHERE site_id=? AND retired_at IS NULL"
                " ORDER BY pws_station_id",
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

    db = get_db()

    def _reserve(conn: sqlite3.Connection) -> None:
        if (
            conn.execute("SELECT 1 FROM sites WHERE id=?", (site_id,)).fetchone()
            is None
        ):
            raise ApiError(404, "site not found")
        check_domain_backoff(conn, source_domain("weathercom"))
        reserve_budget(conn, "weathercom", 1)

    # The shared weather.com call lock covers the reserve, the call and the
    # backoff write, and is taken before any of them, so a timed-out wait
    # means no provider request, no budget change and no station row. The
    # lock's release() does not check ownership, so it runs only in the
    # finally of the try entered straight after a True result, with no await
    # in between. The Open-Meteo elevation lookup below stays outside it.
    lock = weathercom_call_lock()
    if not await acquire_within(lock, ADD_STATION_CALL_WAIT_SECONDS):
        raise ApiError(
            503, "Weather provider busy; station was not added. Try again shortly."
        )
    try:
        await db.write(_reserve)
        pws = await _provider_call(
            validate_station(body.pws_station_id, api_key), provider="weathercom"
        )
    finally:
        lock.release()

    def _reserve_elevation(conn: sqlite3.Connection) -> None:
        if (
            conn.execute("SELECT 1 FROM sites WHERE id=?", (site_id,)).fetchone()
            is None
        ):
            raise ApiError(404, "site not found")
        check_domain_backoff(conn, source_domain("open-meteo"))
        reserve_budget(conn, "open-meteo", 1)

    await db.write(_reserve_elevation)
    dem = await _provider_call(
        lookup_elevation_m(pws.lat, pws.lon), provider="open-meteo"
    )

    def _write(conn: sqlite3.Connection) -> StationOut:
        if (
            conn.execute("SELECT 1 FROM sites WHERE id=?", (site_id,)).fetchone()
            is None
        ):
            raise ApiError(404, "site not found")
        # The id is unique across all sites, and a removed station keeps its
        # row, so the id stays taken after a remove.
        held = conn.execute(
            "SELECT id, site_id, retired_at FROM stations WHERE pws_station_id=?",
            (pws.station_id,),
        ).fetchone()
        if held is not None:
            if int(held["site_id"]) != site_id:
                raise ApiError(409, _ANOTHER_SITE_TEXT)
            if held["retired_at"] is None:
                raise ApiError(409, "station already exists")
        now = utc_now()
        if held is None:
            cur = conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m)
                VALUES (?, ?, ?, ?, ?)
                """,
                (site_id, pws.station_id, pws.lat, pws.lon, dem),
            )
            if cur.lastrowid is None:
                raise RuntimeError("station insert failed")
            station_id = int(cur.lastrowid)
            record_station_change(
                conn, site_id, station_id, "added", reason="create_station", now_utc=now
            )
        else:
            # Re-add of a station removed at this site: bring it back. It
            # counts from the next local day; no past day changes, and no job
            # is queued.
            station_id = int(held["id"])
            conn.execute(
                "UPDATE stations SET enabled=1, retired_at=NULL"
                " WHERE id=? AND site_id=?",
                (station_id, site_id),
            )
            record_station_change(
                conn,
                site_id,
                station_id,
                "turned_on",
                reason="create_station",
                now_utc=now,
            )
        clear_domain_backoff(conn, source_domain("weathercom"))
        clear_domain_backoff(conn, source_domain("open-meteo"))
        # No job is queued: a new station counts from its next full local day
        # and is collected by the daily schedule.
        row = conn.execute(
            "SELECT * FROM stations WHERE id=?", (station_id,)
        ).fetchone()
        if row is None:
            raise RuntimeError("station insert failed")
        return _station_out(row)

    station = await db.write(_write)
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
        if body.enabled == bool(row["enabled"]):
            return _station_out(row)
        now = utc_now()
        if body.enabled:
            change: StationChange = "turned_on"
        else:
            _refuse_removal(conn, site_id, station_id, row, now)
            change = "turned_off"
        conn.execute(
            "UPDATE stations SET enabled=? WHERE id=? AND site_id=?",
            (1 if body.enabled else 0, station_id, site_id),
        )
        record_station_change(
            conn, site_id, station_id, change, reason="update_station", now_utc=now
        )
        updated = _get_station(conn, site_id, station_id)
        return _station_out(updated)

    station = await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_station_cluster

        return await render_station_cluster(request, site_id)
    return station


@router.post("/{station_id}/retry-now", response_model=None)
async def retry_station_now(
    request: Request, site_id: int, station_id: int
) -> dict[str, bool] | HTMLResponse:
    """Check a paused station's current conditions now (Retry now).

    One write. Refused before anything is written: 404 for a missing or
    retired station; 409 ``station is disabled`` or ``site is disabled``;
    409 ``station is not paused`` when its poll state has no row or an
    evidence count of 0, so a scripted press spends no call on a healthy
    station. Otherwise ``next_poll_at`` becomes now and a
    ``fetch_current_obs`` job is queued through ``enqueue_if_absent``, never
    the cooldown variant, which operator paths must not use; a job already
    queued for the station absorbs the press. No provider call here: the
    job's own reservation applies the domain backoff, the refused-access
    hold and the daily budget, and its reply is judged by
    ``persist_poll_result``. Answers the station cluster fragment to an
    htmx request, else ``{"queued": <whether a job was created>}``.
    """

    def _write(conn: sqlite3.Connection) -> EnqueueResult:
        row = _get_station(conn, site_id, station_id)
        if not bool(row["enabled"]):
            raise ApiError(409, "station is disabled")
        site = conn.execute(
            "SELECT enabled FROM sites WHERE id=?", (site_id,)
        ).fetchone()
        if site is None:
            raise ApiError(404, "site not found")
        if not bool(site["enabled"]):
            raise ApiError(409, "site is disabled")
        state = conn.execute(
            "SELECT pause_evidence_count FROM station_poll_state WHERE station_id=?",
            (station_id,),
        ).fetchone()
        if state is None or int(state["pause_evidence_count"]) < 1:
            raise ApiError(409, "station is not paused")
        conn.execute(
            "UPDATE station_poll_state SET next_poll_at=? WHERE station_id=?",
            (isoformat_utc(utc_now()), station_id),
        )
        return enqueue_if_absent(
            conn,
            "fetch_current_obs",
            site_id,
            f"curobs:{station_id}",
            {"station_id": station_id},
        )

    result = await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_station_cluster

        return await render_station_cluster(request, site_id)
    return {"queued": result.created}


@router.delete("/{station_id}", response_model=None)
async def delete_station(
    request: Request, site_id: int, station_id: int
) -> dict[str, bool] | HTMLResponse:
    # A remove is a retire: the row, its readings and its wind days stay, so
    # no cascade runs and no past day changes.
    def _write(conn: sqlite3.Connection) -> None:
        row = _get_station(conn, site_id, station_id)
        now = utc_now()
        _refuse_removal(conn, site_id, station_id, row, now)
        conn.execute(
            "UPDATE stations SET enabled=0, retired_at=? WHERE id=? AND site_id=?",
            (isoformat_utc_micro(now), station_id, site_id),
        )
        record_station_change(
            conn, site_id, station_id, "retired", reason="delete_station", now_utc=now
        )

    await get_db().write(_write)
    if _wants_html(request):
        from wxverify.web.routes import render_station_cluster

        return await render_station_cluster(request, site_id)
    return {"deleted": True}


def _get_station(
    conn: sqlite3.Connection, site_id: int, station_id: int
) -> sqlite3.Row:
    # A retired station is gone to the station routes.
    row = conn.execute(
        "SELECT * FROM stations WHERE id=? AND site_id=? AND retired_at IS NULL",
        (station_id, site_id),
    ).fetchone()
    if row is None:
        raise ApiError(404, "station not found")
    return row


def _enabled_station_count(conn: sqlite3.Connection, site_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM stations WHERE site_id=? AND enabled=1", (site_id,)
    ).fetchone()
    return 0 if row is None else int(row["n"])


def _refuse_removal(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    row: sqlite3.Row,
    now: datetime,
) -> None:
    """Refuse a turn-off or remove with 409 when it would leave the site short.

    Two checks, both before any write. First, the station is enabled and no
    other enabled station remains: the flag is the list of the next local
    day and every later one. Second, the station is the only one in the
    current local day's list (a station added today counts only from
    tomorrow).
    """
    if bool(row["enabled"]) and _enabled_station_count(conn, site_id) <= 1:
        raise ApiError(409, "site must retain at least one enabled station")
    if not current_day_has_other_station(conn, site_id, station_id, now_utc=now):
        raise ApiError(409, "site must keep at least one station counting today")


def _wants_html(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
