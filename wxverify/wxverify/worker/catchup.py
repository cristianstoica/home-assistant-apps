"""Global catch-up worker routines."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

import httpx

from wxverify.collection.budget import (
    Reservation,
    is_refundable_transport_error,
    refund_budget,
    reserve_budget,
)
from wxverify.collection.forecast_fetcher import (
    PersistOutcome,
    persist_fetch_result,
)
from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.secrets import resolve_secret
from wxverify.core.timeutil import floor_hour, isoformat_utc, parse_utc, utc_now
from wxverify.db.connection import Database
from wxverify.feeds.registry import build_adapter
from wxverify.feeds.seam import CostEstimate, FetchResult, ForecastRequest
from wxverify.obs.pws_adapter import PwsObservation, fetch_hourly_history_range
from wxverify.obs.qc import TARGET_VARIABLES
from wxverify.scoring.consensus import insert_station_observation
from wxverify.scoring.engine import PAIR_AND_SCORE_PHASES
from wxverify.settings.keys import get_setting
from wxverify.worker.backfill import BACKFILL_VARIABLES, SETUP_BACKFILL_DAYS
from wxverify.worker.control import JobCancelled, JobContinuation, JobDeferred
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    clear_domain_backoff,
    record_http_backoff,
    source_domain,
)
from wxverify.worker.scheduler import scheduler_tick
from wxverify.worker.station_pacing import pace_station_call, station_call_limiter

CATCHUP_SITE_CHUNK = 2
TARGET_VARIABLE_LIST = tuple(sorted(TARGET_VARIABLES))

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CatchupPlan:
    window_start: datetime
    window_end: datetime
    cursor_site_id: int


@dataclass(frozen=True)
class CatchupSite:
    site_id: int
    lat: float
    lon: float
    timezone: str


@dataclass(frozen=True)
class StationTarget:
    id: int
    pws_station_id: str


@dataclass(frozen=True)
class ForecastTarget:
    site_id: int
    feed_id: int
    lat: float
    lon: float
    source: str
    model: str
    max_lead_hours: int


async def run_catchup(
    db: Database, payload: dict[str, object]
) -> JobContinuation | None:
    await db.write(scheduler_tick)
    plan = await db.read(lambda conn: _catchup_plan(conn, payload))
    sites, has_more = await db.read(
        lambda conn: _catchup_sites(conn, plan.cursor_site_id)
    )
    logger.debug(
        "catchup sites=%s cursor=%s has_more=%s",
        len(sites),
        plan.cursor_site_id,
        has_more,
    )
    changed_sites: set[int] = set()
    for site in sites:
        logger.debug("catchup site start site=%s", site.site_id)
        try:
            changed = await _catchup_site(db, site, plan)
        except JobDeferred:
            raise
        except (JobCancelled, sqlite3.IntegrityError):
            continue
        except Exception:
            continue
        logger.debug("catchup site result site=%s changed=%s", site.site_id, changed)
        if changed:
            changed_sites.add(site.site_id)
    logger.debug("catchup rescoring sites=%s", len(changed_sites))
    for site_id in changed_sites:
        # One write transaction per phase; runs inside the single worker job
        # executor, so the convergence invariant documented at the
        # pair_and_score dispatch site (worker/processor.py) applies here too.
        for phase in PAIR_AND_SCORE_PHASES:
            await db.write(lambda conn, sid=site_id, run=phase: run(conn, sid))
    if has_more and sites:
        return JobContinuation(
            job_type="catchup",
            site_id=None,
            job_key="catchup",
            payload={
                "window_start": isoformat_utc(plan.window_start),
                "window_end": isoformat_utc(plan.window_end),
                "cursor_site_id": sites[-1].site_id,
            },
        )
    logger.debug("catchup complete through=%s", isoformat_utc(plan.window_end))
    await db.write(
        lambda conn: _mark_catchup_complete(conn, isoformat_utc(plan.window_end))
    )
    return None


async def _catchup_site(db: Database, site: CatchupSite, plan: CatchupPlan) -> bool:
    window_start = isoformat_utc(plan.window_start)
    window_end = isoformat_utc(plan.window_end)
    station_changed = await _fetch_missing_station_history(
        db, site, window_start=window_start, window_end=window_end
    )
    forecast_written = await _fetch_due_open_meteo(
        db, site, window_start=window_start, window_end=window_end
    )
    return station_changed or forecast_written > 0


async def _fetch_missing_station_history(
    db: Database, site: CatchupSite, *, window_start: str, window_end: str
) -> bool:
    api_key = resolve_secret("weathercom")
    if not api_key:
        raise RuntimeError("weathercom key is not configured")
    stations = await db.read(lambda conn: _enabled_stations(conn, site.site_id))
    changed = False
    async with httpx.AsyncClient() as client:
        limiter = station_call_limiter()
        for index, station in enumerate(stations):
            await pace_station_call(site.site_id, station.id, index)
            async with limiter:
                has_gap = await db.read(
                    lambda conn, st=station: _station_has_gap(
                        conn, st.id, window_start=window_start, window_end=window_end
                    )
                )
                if not has_gap:
                    continue
                logger.debug(
                    "catchup station gap site=%s station=%s",
                    site.site_id,
                    station.id,
                )
                reservation = await db.write(
                    lambda conn, station_id=station.id: _reserve_station_history_call(
                        conn, site.site_id, station_id
                    )
                )
                try:
                    observations = await fetch_hourly_history_range(
                        station.pws_station_id,
                        api_key,
                        window_start=window_start,
                        window_end=window_end,
                        timezone=site.timezone,
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
                    refund = reservation if is_refundable_transport_error(exc) else None
                    await db.write(
                        lambda conn, station_id=station.id, err=error, res=refund: (
                            _mark_station_error_and_refund(conn, station_id, err, res)
                        )
                    )
                    raise
                station_changed = await db.write(
                    lambda conn, station_id=station.id, rows=observations: (
                        _persist_station_observations(
                            conn, site.site_id, station_id, rows
                        )
                    )
                )
                changed = changed or station_changed
    return changed


async def _fetch_due_open_meteo(
    db: Database, site: CatchupSite, *, window_start: str, window_end: str
) -> int:
    targets = await db.read(
        lambda conn: _due_open_meteo_targets(
            conn,
            site=site,
            window_end=parse_utc(window_end),
        )
    )
    written = 0
    async with httpx.AsyncClient() as client:
        for target in targets:
            logger.debug(
                "catchup due open-meteo site=%s feed=%s",
                target.site_id,
                target.feed_id,
            )
            adapter = build_adapter(target.source, client)
            req = ForecastRequest(
                lat=target.lat,
                lon=target.lon,
                model=target.model,
                variables=BACKFILL_VARIABLES,
                max_lead_hours=target.max_lead_hours,
            )
            cost = adapter.estimate_cost(req)
            reservation = await db.write(
                lambda conn, feed=target, reserve=cost: _reserve_feed_call(
                    conn, feed, reserve
                )
            )
            try:
                result = await adapter.fetch_historical(
                    req, window_start=window_start, window_end=window_end
                )
            except httpx.HTTPStatusError as exc:
                error = sanitized_exception(exc)
                response = exc.response
                next_attempt_at = await db.write(
                    lambda conn, feed=target, err=error, resp=response: (
                        _mark_feed_error_and_backoff(conn, feed, err, resp)
                    )
                )
                if next_attempt_at is not None:
                    raise JobDeferred(next_attempt_at) from exc
                continue
            except Exception as exc:
                error = sanitized_exception(exc)
                refund = reservation if is_refundable_transport_error(exc) else None
                await db.write(
                    lambda conn, feed=target, err=error, res=refund: (
                        _mark_feed_error_and_refund(conn, feed, err, res)
                    )
                )
                continue
            if result is None:
                continue
            outcome = await db.write(
                lambda conn, feed=target, fetched=result: (
                    _persist_historical_fetch_success(conn, feed, fetched)
                )
            )
            written += outcome.inserted_count
    return written


def _catchup_plan(conn: sqlite3.Connection, payload: dict[str, object]) -> CatchupPlan:
    raw_window_end = payload.get("window_end")
    if isinstance(raw_window_end, str):
        window_end = floor_hour(parse_utc(raw_window_end))
    else:
        window_end = floor_hour(utc_now())
    raw_window_start = payload.get("window_start")
    if isinstance(raw_window_start, str):
        window_start = floor_hour(parse_utc(raw_window_start))
    else:
        window_start = _default_window_start(conn, window_end)
    if window_start < window_end - timedelta(days=SETUP_BACKFILL_DAYS):
        window_start = window_end - timedelta(days=SETUP_BACKFILL_DAYS)
    if window_start > window_end:
        window_start = window_end
    cursor = _payload_int(payload, "cursor_site_id") or 0
    return CatchupPlan(
        window_start=window_start, window_end=window_end, cursor_site_id=cursor
    )


def _default_window_start(conn: sqlite3.Connection, window_end: datetime) -> datetime:
    raw = get_setting(conn, "last_catchup_at")
    if raw:
        try:
            return floor_hour(parse_utc(raw))
        except ValueError:
            pass
    return window_end - timedelta(days=SETUP_BACKFILL_DAYS)


def _catchup_sites(
    conn: sqlite3.Connection, cursor_site_id: int
) -> tuple[list[CatchupSite], bool]:
    rows = conn.execute(
        """
        SELECT id, forecast_lat, forecast_lon, timezone
        FROM sites
        WHERE enabled = 1 AND id > ?
        ORDER BY id
        LIMIT ?
        """,
        (cursor_site_id, CATCHUP_SITE_CHUNK + 1),
    ).fetchall()
    has_more = len(rows) > CATCHUP_SITE_CHUNK
    return [
        CatchupSite(
            site_id=int(row["id"]),
            lat=float(row["forecast_lat"]),
            lon=float(row["forecast_lon"]),
            timezone=str(row["timezone"]),
        )
        for row in rows[:CATCHUP_SITE_CHUNK]
    ], has_more


def _enabled_stations(conn: sqlite3.Connection, site_id: int) -> list[StationTarget]:
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
        StationTarget(id=int(row["id"]), pws_station_id=str(row["pws_station_id"]))
        for row in rows
    ]


def _station_has_gap(
    conn: sqlite3.Connection, station_id: int, *, window_start: str, window_end: str
) -> bool:
    start = parse_utc(window_start)
    end = parse_utc(window_end)
    expected_hours = int((end - start).total_seconds() // 3600)
    if expected_hours <= 0:
        return False
    placeholders = ",".join("?" for _ in TARGET_VARIABLE_LIST)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM (
            SELECT variable, valid_at
            FROM station_observations
            WHERE station_id = ?
              AND valid_at >= ?
              AND valid_at < ?
              AND variable IN ({placeholders})
            GROUP BY variable, valid_at
        )
        """,
        (station_id, window_start, window_end, *TARGET_VARIABLE_LIST),
    ).fetchone()
    actual = 0 if row is None else int(row["n"])
    return actual < expected_hours * len(TARGET_VARIABLE_LIST)


def _reserve_station_history_call(
    conn: sqlite3.Connection, site_id: int, station_id: int
) -> Reservation:
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
    return reserve_budget(conn, "weathercom", 1)


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


def _mark_station_error(conn: sqlite3.Connection, station_id: int, error: str) -> None:
    conn.execute(
        """
        UPDATE stations
        SET last_error=?, error_count=error_count + 1
        WHERE id=?
        """,
        (error, station_id),
    )


def _mark_station_error_and_refund(
    conn: sqlite3.Connection,
    station_id: int,
    error: str,
    reservation: Reservation | None,
) -> None:
    """Record the station failure and, atomically, refund a phantom reservation."""
    _mark_station_error(conn, station_id, error)
    if reservation is not None:
        refund_budget(conn, reservation)


def _due_open_meteo_targets(
    conn: sqlite3.Connection, *, site: CatchupSite, window_end: datetime
) -> list[ForecastTarget]:
    rows = conn.execute(
        """
        SELECT s.id AS site_id, s.forecast_lat, s.forecast_lon,
               f.id AS feed_id, f.source, f.model, f.max_lead_hours,
               f.fetch_interval_minutes, sfs.last_run_at
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
        (site.site_id,),
    ).fetchall()
    targets: list[ForecastTarget] = []
    for row in rows:
        last_run_at = row["last_run_at"]
        due = last_run_at is None
        if last_run_at is not None:
            elapsed = (window_end - parse_utc(str(last_run_at))).total_seconds() / 60
            due = elapsed >= int(row["fetch_interval_minutes"])
        if not due:
            continue
        targets.append(
            ForecastTarget(
                site_id=int(row["site_id"]),
                feed_id=int(row["feed_id"]),
                lat=float(row["forecast_lat"]),
                lon=float(row["forecast_lon"]),
                source=str(row["source"]),
                model=str(row["model"]),
                max_lead_hours=int(row["max_lead_hours"]),
            )
        )
    return targets


def _reserve_feed_call(
    conn: sqlite3.Connection, feed: ForecastTarget, cost: CostEstimate
) -> Reservation:
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
    return reserve_budget(conn, feed.source, cost.calls, cost.credits)


def _mark_feed_error(
    conn: sqlite3.Connection, feed: ForecastTarget, error: str
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


def _mark_feed_error_and_refund(
    conn: sqlite3.Connection,
    feed: ForecastTarget,
    error: str,
    reservation: Reservation | None,
) -> None:
    """Record the fetch failure and, atomically, refund a phantom reservation."""
    _mark_feed_error(conn, feed, error)
    if reservation is not None:
        refund_budget(conn, reservation)


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
    feed: ForecastTarget,
    error: str,
    response: httpx.Response,
) -> str | None:
    _mark_feed_error(conn, feed, error)
    return record_http_backoff(conn, response)


def _persist_historical_fetch_success(
    conn: sqlite3.Connection, feed: ForecastTarget, result: FetchResult
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


def _mark_catchup_complete(conn: sqlite3.Connection, caught_up_through: str) -> None:
    conn.execute(
        """
        INSERT INTO settings (key, value)
        VALUES ('last_catchup_at', ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (caught_up_through,),
    )


def _payload_int(payload: dict[str, object], key: str) -> int | None:
    value = payload.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None
