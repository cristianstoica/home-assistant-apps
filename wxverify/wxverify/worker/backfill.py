"""Historical backfill worker routines."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

import httpx

from wxverify.collection.budget import reserve_budget
from wxverify.collection.forecast_fetcher import (
    PersistOutcome,
    persist_fetch_result,
)
from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.secrets import resolve_secret
from wxverify.core.timeutil import floor_hour, isoformat_utc, parse_utc, utc_now
from wxverify.db.connection import Database
from wxverify.db.queue import enqueue_if_absent
from wxverify.feeds.registry import build_adapter
from wxverify.feeds.seam import CostEstimate, FetchResult, ForecastRequest
from wxverify.obs.pws_adapter import (
    PwsObservation,
    fetch_hourly_history,
    fetch_hourly_history_range,
)
from wxverify.scoring.consensus import insert_station_observation
from wxverify.scoring.engine import pair_and_score
from wxverify.worker.control import JobCancelled, JobContinuation, JobDeferred
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    clear_domain_backoff,
    record_http_backoff,
    source_domain,
)
from wxverify.worker.station_pacing import pace_station_call, station_call_limiter

SETUP_BACKFILL_DAYS = 30
BACKFILL_CHUNK_DAYS = 7
BACKFILL_VARIABLES = ("temperature", "wind", "precip")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SiteBackfillTarget:
    site_id: int
    lat: float
    lon: float
    timezone: str
    backfill_status: str
    backfill_through: str | None


@dataclass(frozen=True)
class StationHistoryTarget:
    id: int
    pws_station_id: str


@dataclass(frozen=True)
class HistoricalFeedTarget:
    site_id: int
    feed_id: int
    lat: float
    lon: float
    source: str
    model: str
    max_lead_hours: int


async def run_backfill_site(
    db: Database, site_id: int, payload: dict[str, object]
) -> JobContinuation | None:
    target = await db.read(lambda conn: _site_target(conn, site_id))
    if target is None:
        raise JobCancelled()
    window_start, window_end, chunk_start = _backfill_window(target, payload)
    chunk_end = min(chunk_start + timedelta(days=BACKFILL_CHUNK_DAYS), window_end)
    logger.debug(
        "backfill site=%s window=%s..%s chunk=%s..%s",
        site_id,
        isoformat_utc(window_start),
        isoformat_utc(window_end),
        isoformat_utc(chunk_start),
        isoformat_utc(chunk_end),
    )
    forecast_complete = target.backfill_status == "complete"
    if not forecast_complete:
        await db.write(lambda conn: _mark_backfill_started(conn, site_id))
    obs_changed = False
    if not bool(payload.get("station_history_complete")):
        obs_changed = await fetch_station_history_window(
            db,
            site_id,
            window_start=isoformat_utc(window_start),
            window_end=isoformat_utc(window_end),
            timezone=target.timezone,
        )
    logger.debug("backfill station history site=%s changed=%s", site_id, obs_changed)
    forecast_written = 0
    if not forecast_complete:
        forecast_written = await _fetch_historical_forecasts(
            db,
            target,
            window_start=isoformat_utc(chunk_start),
            window_end=isoformat_utc(chunk_end),
        )
    logger.debug("backfill forecasts site=%s written=%s", site_id, forecast_written)
    complete = forecast_complete or chunk_end >= window_end
    logger.debug(
        "backfill chunk done site=%s through=%s complete=%s",
        site_id,
        isoformat_utc(chunk_end),
        complete,
    )
    await db.write(
        lambda conn: _finish_backfill_chunk(
            conn,
            site_id=site_id,
            backfill_through=isoformat_utc(chunk_end),
            complete=complete,
            forecast_complete=forecast_complete,
            should_score=obs_changed or forecast_written > 0,
        )
    )
    if complete:
        return None
    return JobContinuation(
        job_type="backfill_site",
        site_id=site_id,
        job_key=f"backfill:{site_id}",
        payload={
            "site_id": site_id,
            "window_start": isoformat_utc(window_start),
            "window_end": isoformat_utc(window_end),
            "cursor_start": isoformat_utc(chunk_end),
            "station_history_complete": True,
        },
    )


async def fetch_station_history(db: Database, site_id: int, *, hours: int) -> bool:
    api_key = resolve_secret("weathercom")
    if not api_key:
        raise RuntimeError("weathercom key is not configured")
    timezone = await db.read(lambda conn: _site_timezone(conn, site_id))
    if timezone is None:
        raise JobCancelled()
    stations = await db.read(lambda conn: _enabled_stations(conn, site_id))
    if not stations:
        raise JobCancelled()
    changed = False
    async with httpx.AsyncClient() as client:
        limiter = station_call_limiter()
        for index, station in enumerate(stations):
            await pace_station_call(site_id, station.id, index)
            async with limiter:
                await db.write(
                    lambda conn, station_id=station.id: _reserve_station_history_call(
                        conn, site_id, station_id
                    )
                )
                try:
                    observations = await fetch_hourly_history(
                        station.pws_station_id,
                        api_key,
                        hours=hours,
                        timezone=timezone,
                        client=client,
                    )
                except httpx.HTTPStatusError as exc:
                    error = sanitized_exception(exc)
                    response = exc.response
                    next_attempt_at = await db.write(
                        lambda conn, station_id=station.id, err=error, resp=response: (
                            _mark_station_error_and_backoff(conn, station_id, err, resp)
                        )
                    )
                    if next_attempt_at is not None:
                        raise JobDeferred(next_attempt_at) from exc
                    raise
                except Exception as exc:
                    error = sanitized_exception(exc)
                    await db.write(
                        lambda conn, station_id=station.id, err=error: (
                            _mark_station_error(conn, station_id, err)
                        )
                    )
                    raise
                station_changed = await db.write(
                    lambda conn, station_id=station.id, rows=observations: (
                        _persist_station_observations(conn, site_id, station_id, rows)
                    )
                )
                changed = changed or station_changed
    return changed


async def fetch_station_history_window(
    db: Database,
    site_id: int,
    *,
    window_start: str,
    window_end: str,
    timezone: str,
) -> bool:
    api_key = resolve_secret("weathercom")
    if not api_key:
        raise RuntimeError("weathercom key is not configured")
    stations = await db.read(lambda conn: _enabled_stations(conn, site_id))
    if not stations:
        raise JobCancelled()
    changed = False
    async with httpx.AsyncClient() as client:
        limiter = station_call_limiter()
        for index, station in enumerate(stations):
            await pace_station_call(site_id, station.id, index)
            async with limiter:
                await db.write(
                    lambda conn, station_id=station.id: _reserve_station_history_call(
                        conn, site_id, station_id
                    )
                )
                try:
                    observations = await fetch_hourly_history_range(
                        station.pws_station_id,
                        api_key,
                        window_start=window_start,
                        window_end=window_end,
                        timezone=timezone,
                        client=client,
                    )
                except httpx.HTTPStatusError as exc:
                    error = sanitized_exception(exc)
                    response = exc.response
                    next_attempt_at = await db.write(
                        lambda conn, station_id=station.id, err=error, resp=response: (
                            _mark_station_error_and_backoff(conn, station_id, err, resp)
                        )
                    )
                    if next_attempt_at is not None:
                        raise JobDeferred(next_attempt_at) from exc
                    raise
                except Exception as exc:
                    error = sanitized_exception(exc)
                    await db.write(
                        lambda conn, station_id=station.id, err=error: (
                            _mark_station_error(conn, station_id, err)
                        )
                    )
                    raise
                station_changed = await db.write(
                    lambda conn, station_id=station.id, rows=observations: (
                        _persist_station_observations(conn, site_id, station_id, rows)
                    )
                )
                changed = changed or station_changed
    return changed


def _site_target(conn: sqlite3.Connection, site_id: int) -> SiteBackfillTarget | None:
    row = conn.execute(
        """
        SELECT id, forecast_lat, forecast_lon, timezone,
               backfill_status, backfill_through
        FROM sites
        WHERE id=? AND enabled=1
        """,
        (site_id,),
    ).fetchone()
    if row is None:
        return None
    return SiteBackfillTarget(
        site_id=int(row["id"]),
        lat=float(row["forecast_lat"]),
        lon=float(row["forecast_lon"]),
        timezone=str(row["timezone"]),
        backfill_status=str(row["backfill_status"]),
        backfill_through=None
        if row["backfill_through"] is None
        else str(row["backfill_through"]),
    )


def _backfill_window(
    target: SiteBackfillTarget, payload: dict[str, object]
) -> tuple[datetime, datetime, datetime]:
    raw_window_end = payload.get("window_end")
    if isinstance(raw_window_end, str):
        window_end = parse_utc(raw_window_end)
    else:
        window_end = floor_hour(utc_now())
    raw_window_start = payload.get("window_start")
    if isinstance(raw_window_start, str):
        window_start = parse_utc(raw_window_start)
    else:
        window_start = window_end - timedelta(days=SETUP_BACKFILL_DAYS)
    raw_cursor = payload.get("cursor_start")
    if isinstance(raw_cursor, str):
        chunk_start = parse_utc(raw_cursor)
    elif target.backfill_status == "in_progress" and target.backfill_through:
        chunk_start = parse_utc(target.backfill_through)
    else:
        chunk_start = window_start
    if chunk_start < window_start:
        chunk_start = window_start
    if chunk_start > window_end:
        chunk_start = window_end
    return window_start, window_end, chunk_start


def _mark_backfill_started(conn: sqlite3.Connection, site_id: int) -> None:
    cur = conn.execute(
        """
        UPDATE sites
        SET backfill_status='in_progress'
        WHERE id=? AND enabled=1 AND backfill_status != 'complete'
        """,
        (site_id,),
    )
    if cur.rowcount not in (0, 1):
        raise RuntimeError("unexpected backfill status update")


async def _fetch_historical_forecasts(
    db: Database, target: SiteBackfillTarget, *, window_start: str, window_end: str
) -> int:
    feeds = await db.read(lambda conn: _historical_feed_targets(conn, target.site_id))
    written = 0
    async with httpx.AsyncClient() as client:
        for feed in feeds:
            logger.debug(
                "backfill feed fetch site=%s feed=%s source=%s",
                feed.site_id,
                feed.feed_id,
                feed.source,
            )
            adapter = build_adapter(feed.source, client)
            req = ForecastRequest(
                lat=feed.lat,
                lon=feed.lon,
                model=feed.model,
                variables=BACKFILL_VARIABLES,
                max_lead_hours=feed.max_lead_hours,
            )
            cost = adapter.estimate_cost(req)
            await db.write(lambda conn, f=feed, c=cost: _reserve_feed_call(conn, f, c))
            try:
                result = await adapter.fetch_historical(
                    req, window_start=window_start, window_end=window_end
                )
            except httpx.HTTPStatusError as exc:
                error = sanitized_exception(exc)
                response = exc.response
                next_attempt_at = await db.write(
                    lambda conn, f=feed, err=error, resp=response: (
                        _mark_feed_error_and_backoff(conn, f, err, resp)
                    )
                )
                logger.debug(
                    "backfill feed http error site=%s feed=%s source=%s backoff=%s: %s",
                    feed.site_id,
                    feed.feed_id,
                    feed.source,
                    next_attempt_at is not None,
                    error,
                )
                if next_attempt_at is not None:
                    raise JobDeferred(next_attempt_at) from exc
                raise
            except Exception as exc:
                error = sanitized_exception(exc)
                await db.write(
                    lambda conn, f=feed, err=error: _mark_feed_error(conn, f, err)
                )
                logger.debug(
                    "backfill feed error site=%s feed=%s source=%s: %s",
                    feed.site_id,
                    feed.feed_id,
                    feed.source,
                    error,
                )
                raise
            if result is None:
                continue
            outcome = await db.write(
                lambda conn, f=feed, r=result: _persist_historical_fetch_success(
                    conn, f, r
                )
            )
            written += outcome.inserted_count
    return written


def _historical_feed_targets(
    conn: sqlite3.Connection, site_id: int
) -> list[HistoricalFeedTarget]:
    rows = conn.execute(
        """
        SELECT s.id AS site_id, s.forecast_lat, s.forecast_lon,
               f.id AS feed_id, f.source, f.model, f.max_lead_hours
        FROM sites s
        JOIN feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = s.id AND sfs.feed_id = f.id
        WHERE s.id = ?
          AND s.enabled = 1
          AND f.enabled = 1
          AND f.source = 'open-meteo'
          AND f.is_virtual = 0
          AND COALESCE(sfs.enabled, f.default_subscribed) = 1
        ORDER BY f.id
        """,
        (site_id,),
    ).fetchall()
    return [
        HistoricalFeedTarget(
            site_id=int(row["site_id"]),
            feed_id=int(row["feed_id"]),
            lat=float(row["forecast_lat"]),
            lon=float(row["forecast_lon"]),
            source=str(row["source"]),
            model=str(row["model"]),
            max_lead_hours=int(row["max_lead_hours"]),
        )
        for row in rows
    ]


def _reserve_feed_call(
    conn: sqlite3.Connection, feed: HistoricalFeedTarget, cost: CostEstimate
) -> None:
    active = conn.execute(
        """
        SELECT 1
        FROM sites s
        JOIN feeds f ON f.id = ?
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = s.id AND sfs.feed_id = f.id
        WHERE s.id = ?
          AND s.enabled = 1
          AND f.enabled = 1
          AND f.source = 'open-meteo'
          AND f.is_virtual = 0
          AND COALESCE(sfs.enabled, f.default_subscribed) = 1
        """,
        (feed.feed_id, feed.site_id),
    ).fetchone()
    if active is None:
        raise JobCancelled()
    check_domain_backoff(conn, source_domain(feed.source, historical=True))
    reserve_budget(conn, feed.source, cost.calls, cost.credits)


def _mark_feed_error(
    conn: sqlite3.Connection, feed: HistoricalFeedTarget, error: str
) -> None:
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_error, error_count)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET
            last_error=excluded.last_error,
            error_count=site_feed_state.error_count + 1
        """,
        (feed.site_id, feed.feed_id, error),
    )


def _enabled_stations(
    conn: sqlite3.Connection, site_id: int
) -> list[StationHistoryTarget]:
    rows = conn.execute(
        """
        SELECT st.id, st.pws_station_id
        FROM stations st
        JOIN sites s ON s.id = st.site_id
        WHERE s.id = ?
          AND s.enabled = 1
          AND st.enabled = 1
        ORDER BY st.pws_station_id
        """,
        (site_id,),
    ).fetchall()
    return [
        StationHistoryTarget(
            id=int(row["id"]), pws_station_id=str(row["pws_station_id"])
        )
        for row in rows
    ]


def _site_timezone(conn: sqlite3.Connection, site_id: int) -> str | None:
    row = conn.execute(
        "SELECT timezone FROM sites WHERE id=? AND enabled=1", (site_id,)
    ).fetchone()
    return None if row is None else str(row["timezone"])


def _reserve_station_history_call(
    conn: sqlite3.Connection, site_id: int, station_id: int
) -> None:
    row = conn.execute(
        """
        SELECT 1
        FROM stations st
        JOIN sites s ON s.id = st.site_id
        WHERE s.id = ?
          AND st.id = ?
          AND s.enabled = 1
          AND st.enabled = 1
        """,
        (site_id, station_id),
    ).fetchone()
    if row is None:
        raise JobCancelled()
    check_domain_backoff(conn, source_domain("weathercom"))
    reserve_budget(conn, "weathercom", 1)


def _persist_station_observations(
    conn: sqlite3.Connection,
    site_id: int,
    station_id: int,
    observations: list[PwsObservation],
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM stations
        WHERE id = ? AND site_id = ? AND enabled = 1
        """,
        (station_id, site_id),
    ).fetchone()
    if row is None:
        raise JobCancelled()
    changed = False
    for observation in observations:
        changed = (
            insert_station_observation(
                conn,
                station_id=station_id,
                variable=observation.variable,
                valid_at=observation.valid_at,
                value=observation.value,
                source_raw=observation.source_raw,
            )
            or changed
        )
    clear_domain_backoff(conn, source_domain("weathercom"))
    conn.execute(
        """
        UPDATE stations
        SET last_run_at=?, last_error=NULL, error_count=0
        WHERE id=?
        """,
        (isoformat_utc(), station_id),
    )
    return changed


def _mark_station_error_and_backoff(
    conn: sqlite3.Connection,
    station_id: int,
    error: str,
    response: httpx.Response,
) -> str | None:
    _mark_station_error(conn, station_id, error)
    return record_http_backoff(conn, response)


def _mark_feed_error_and_backoff(
    conn: sqlite3.Connection,
    feed: HistoricalFeedTarget,
    error: str,
    response: httpx.Response,
) -> str | None:
    _mark_feed_error(conn, feed, error)
    return record_http_backoff(conn, response)


def _persist_historical_fetch_success(
    conn: sqlite3.Connection, feed: HistoricalFeedTarget, result: FetchResult
) -> PersistOutcome:
    clear_domain_backoff(conn, source_domain(feed.source, historical=True))
    return persist_fetch_result(
        conn,
        site_id=feed.site_id,
        source=feed.source,
        fetch_feed_id=feed.feed_id,
        result=result,
        advance_last_run_at=False,
    )


def _mark_station_error(conn: sqlite3.Connection, station_id: int, error: str) -> None:
    conn.execute(
        """
        UPDATE stations
        SET last_error=?, error_count=error_count + 1
        WHERE id=?
        """,
        (error, station_id),
    )


def _finish_backfill_chunk(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    backfill_through: str,
    complete: bool,
    forecast_complete: bool,
    should_score: bool,
) -> None:
    if not forecast_complete:
        status = "complete" if complete else "in_progress"
        conn.execute(
            """
            UPDATE sites
            SET backfill_status=?, backfill_through=?
            WHERE id=? AND enabled=1
            """,
            (status, backfill_through, site_id),
        )
    if should_score:
        pair_and_score(conn, site_id)
        enqueue_if_absent(
            conn, "pair_and_score", site_id, "score", {"site_id": site_id}
        )
