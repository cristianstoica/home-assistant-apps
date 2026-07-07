"""Reusable forward forecast fetch path."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from wxverify.collection.budget import reserve_budget
from wxverify.collection.forecast_fetcher import (
    PersistOutcome,
    persist_fetch_result,
)
from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database
from wxverify.db.queue import enqueue_if_absent
from wxverify.feeds.registry import build_adapter
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastAdapter,
    ForecastRequest,
)
from wxverify.worker.control import JobDeferred
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    clear_domain_backoff,
    record_http_backoff,
    source_domain,
)

AdapterBuilder = Callable[[str, httpx.AsyncClient], ForecastAdapter]


@dataclass(frozen=True)
class FeedFetchTarget:
    site_id: int
    feed_id: int
    lat: float
    lon: float
    source: str
    model: str
    max_lead_hours: int


@dataclass(frozen=True)
class FetchFeedSuccess:
    inserted: int
    usable: int


@dataclass(frozen=True)
class FetchFeedNoOp:
    inserted: int = 0
    usable: int = 0


@dataclass(frozen=True)
class BudgetExhausted:
    next_window: str


@dataclass(frozen=True)
class BackoffActive:
    next_attempt: str


@dataclass(frozen=True)
class Ineligible:
    reason: str


@dataclass(frozen=True)
class Unavailable:
    target: FeedFetchTarget
    error: str


FetchFeedOutcome = (
    FetchFeedSuccess
    | FetchFeedNoOp
    | BudgetExhausted
    | BackoffActive
    | Ineligible
    | Unavailable
)


async def fetch_feed_once(
    db: Database,
    site_id: int,
    feed_id: int,
    *,
    adapter_builder: AdapterBuilder | None = None,
) -> FetchFeedOutcome:
    adapter_builder = adapter_builder or build_adapter
    target = await db.read(lambda conn: feed_fetch_target(conn, site_id, feed_id))
    if target is None:
        return Ineligible("site/feed is not eligible for fetch")
    req = ForecastRequest(
        lat=target.lat,
        lon=target.lon,
        model=target.model,
        variables=("temperature", "wind", "precip"),
        max_lead_hours=target.max_lead_hours,
    )
    async with httpx.AsyncClient() as client:
        try:
            adapter = adapter_builder(target.source, client)
        except Exception as exc:
            return Unavailable(target=target, error=sanitized_exception(exc))
        cost = adapter.estimate_cost(req)
        reserve_outcome = await db.write(
            lambda conn, tgt=target, estimate=cost: _reserve_feed_call(
                conn, tgt, estimate
            )
        )
        if reserve_outcome is not None:
            return reserve_outcome
        try:
            result = await adapter.fetch_forecast(req)
        except httpx.HTTPStatusError as exc:
            error = sanitized_exception(exc)
            response = exc.response
            next_attempt_at = await db.write(
                lambda conn, err=error, resp=response: mark_feed_error_and_backoff(
                    conn, target, err, resp
                )
            )
            if next_attempt_at is not None:
                return BackoffActive(next_attempt_at)
            raise
        except Exception as exc:
            error = sanitized_exception(exc)
            await db.write(lambda conn, err=error: mark_feed_error(conn, target, err))
            raise
    persist_outcome = await db.write(
        lambda conn, fetch_result=result: _persist_fetch_success(
            conn, target, fetch_result
        )
    )
    if persist_outcome.inserted_count:
        await db.write(
            lambda conn: enqueue_if_absent(
                conn, "pair_and_score", site_id, "score", {"site_id": site_id}
            )
        )
    if persist_outcome.usable_sample_count == 0:
        return FetchFeedNoOp()
    return FetchFeedSuccess(
        inserted=persist_outcome.inserted_count,
        usable=persist_outcome.usable_sample_count,
    )


def feed_fetch_target(
    conn: sqlite3.Connection, site_id: int, feed_id: int
) -> FeedFetchTarget | None:
    row = conn.execute(
        """
        SELECT s.id AS site_id, s.forecast_lat, s.forecast_lon,
               f.id AS feed_id, f.source, f.model, f.max_lead_hours,
               f.enabled AS feed_enabled, f.is_virtual,
               COALESCE(sfs.enabled, f.default_subscribed) AS subscribed
        FROM sites s
        JOIN feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = s.id AND sfs.feed_id = f.id
        WHERE s.id = ?
          AND f.id = ?
          AND s.enabled = 1
        """,
        (site_id, feed_id),
    ).fetchone()
    if row is None:
        return None
    source = str(row["source"])
    model = str(row["model"])
    if (
        not bool(row["feed_enabled"])
        or bool(row["is_virtual"])
        or not bool(row["subscribed"])
        or (source == "meteoblue" and model != "multimodel")
    ):
        return None
    return FeedFetchTarget(
        site_id=int(row["site_id"]),
        feed_id=int(row["feed_id"]),
        lat=float(row["forecast_lat"]),
        lon=float(row["forecast_lon"]),
        source=source,
        model=model,
        max_lead_hours=int(row["max_lead_hours"]),
    )


def mark_feed_error(
    conn: sqlite3.Connection, target: FeedFetchTarget, error: str
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
        (target.site_id, target.feed_id, error),
    )


def mark_feed_unavailable(
    conn: sqlite3.Connection, target: FeedFetchTarget, error: str
) -> None:
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_run_at, last_error, error_count)
        VALUES (?, ?, ?, ?, 1)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET
            last_run_at=excluded.last_run_at,
            last_error=excluded.last_error,
            error_count=site_feed_state.error_count + 1
        """,
        (target.site_id, target.feed_id, isoformat_utc(), error),
    )


def mark_feed_error_and_backoff(
    conn: sqlite3.Connection,
    target: FeedFetchTarget,
    error: str,
    response: httpx.Response,
) -> str | None:
    mark_feed_error(conn, target, error)
    return record_http_backoff(conn, response)


def _reserve_feed_call(
    conn: sqlite3.Connection, target: FeedFetchTarget, cost: CostEstimate
) -> BudgetExhausted | BackoffActive | Ineligible | None:
    if feed_fetch_target(conn, target.site_id, target.feed_id) is None:
        return Ineligible("site/feed became ineligible before fetch")
    try:
        check_domain_backoff(conn, source_domain(target.source))
    except JobDeferred as exc:
        return BackoffActive(exc.next_attempt_at)
    try:
        reserve_budget(conn, target.source, cost.calls, cost.credits)
    except JobDeferred as exc:
        return BudgetExhausted(exc.next_attempt_at)
    return None


def _persist_fetch_success(
    conn: sqlite3.Connection, target: FeedFetchTarget, result: FetchResult
) -> PersistOutcome:
    clear_domain_backoff(conn, source_domain(target.source))
    return persist_fetch_result(
        conn,
        site_id=target.site_id,
        source=target.source,
        fetch_feed_id=target.feed_id,
        result=result,
    )
