"""Feed and subscription routes."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from wxverify.api.errors import ApiError
from wxverify.api.schemas import FeedOut, FeedUpdate, SubscriptionUpdate
from wxverify.db.connection import get_db
from wxverify.provider_ops import (
    ProviderOpsError,
    SubscriptionResult,
    set_site_subscription,
)
from wxverify.worker.cadence import parse_fetch_interval_minutes

router = APIRouter(tags=["feeds"])


def _feed_out(row: sqlite3.Row) -> FeedOut:
    return FeedOut(
        id=int(row["id"]),
        source=str(row["source"]),
        model=str(row["model"]),
        enabled=bool(row["enabled"]),
        disabled_reason=None
        if row["disabled_reason"] is None
        else str(row["disabled_reason"]),
        default_subscribed=bool(row["default_subscribed"]),
        fetch_interval_minutes=parse_fetch_interval_minutes(
            row["fetch_interval_minutes"], context=f"feed_out id={int(row['id'])}"
        ),
        max_lead_hours=int(row["max_lead_hours"]),
        is_virtual=bool(row["is_virtual"]),
    )


@router.get("/api/feeds", response_model=list[FeedOut])
async def list_feeds() -> list[FeedOut]:
    def _read(conn: sqlite3.Connection) -> list[FeedOut]:
        return [
            _feed_out(row)
            for row in conn.execute("SELECT * FROM feeds ORDER BY source, model")
        ]

    return await get_db().read(_read)


@router.put("/api/feeds/{feed_id}", response_model=FeedOut)
async def update_feed(feed_id: int, body: FeedUpdate) -> FeedOut:
    def _write(conn: sqlite3.Connection) -> FeedOut:
        row = conn.execute("SELECT * FROM feeds WHERE id=?", (feed_id,)).fetchone()
        if row is None:
            raise ApiError(404, "feed not found")
        if body.enabled is not None:
            conn.execute(
                "UPDATE feeds SET enabled=?, disabled_reason=? WHERE id=?",
                (
                    1 if body.enabled else 0,
                    body.disabled_reason if not body.enabled else None,
                    feed_id,
                ),
            )
        if body.fetch_interval_minutes is not None:
            conn.execute(
                "UPDATE feeds SET fetch_interval_minutes=? WHERE id=?",
                (body.fetch_interval_minutes, feed_id),
            )
        if body.default_subscribed is not None:
            conn.execute(
                "UPDATE feeds SET default_subscribed=? WHERE id=?",
                (1 if body.default_subscribed else 0, feed_id),
            )
        updated = conn.execute("SELECT * FROM feeds WHERE id=?", (feed_id,)).fetchone()
        if updated is None:
            raise RuntimeError("feed update failed")
        return _feed_out(updated)

    return await get_db().write(_write)


@router.put("/api/sites/{site_id}/feeds/{feed_id}", response_model=None)
async def update_subscription(
    request: Request, site_id: int, feed_id: int, body: SubscriptionUpdate
) -> dict[str, object] | HTMLResponse:
    def _write(conn: sqlite3.Connection) -> SubscriptionResult:
        return set_site_subscription(
            conn,
            site_id,
            feed_id,
            enabled=body.enabled,
            enqueue_on_enable=True,
        )

    try:
        result = await get_db().write(_write)
    except ProviderOpsError as exc:
        raise ApiError(exc.status_code, exc.message) from exc
    if _wants_html(request):
        from wxverify.web.routes import render_feed_toggles

        return await render_feed_toggles(
            request,
            site_id,
            feed_id=feed_id,
            enabled=result.enabled,
            changed=result.changed,
        )
    return {
        "site_id": result.site_id,
        "feed_id": result.feed_id,
        "enabled": result.enabled,
        "changed": result.changed,
        "fetch_enqueued": None if result.fetch is None else result.fetch.enqueued,
    }


def _wants_html(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
