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
    set_site_subscription,
)
from wxverify.provider_ops import (
    rebuild_mean_for_site as _rebuild_mean_for_site,
)

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
        fetch_interval_minutes=int(row["fetch_interval_minutes"]),
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
        affected_sites: set[int] = set()
        if body.enabled is not None:
            if body.enabled != bool(row["enabled"]):
                affected_sites.update(
                    _affected_sites_for_feed_change(conn, row, inherited_only=False)
                )
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
            if body.default_subscribed != bool(row["default_subscribed"]):
                affected_sites.update(
                    _affected_sites_for_feed_change(conn, row, inherited_only=True)
                )
            conn.execute(
                "UPDATE feeds SET default_subscribed=? WHERE id=?",
                (1 if body.default_subscribed else 0, feed_id),
            )
        for site_id in sorted(affected_sites):
            _rebuild_mean_for_site(conn, site_id)
        updated = conn.execute("SELECT * FROM feeds WHERE id=?", (feed_id,)).fetchone()
        if updated is None:
            raise RuntimeError("feed update failed")
        return _feed_out(updated)

    return await get_db().write(_write)


@router.put("/api/sites/{site_id}/feeds/{feed_id}", response_model=None)
async def update_subscription(
    request: Request, site_id: int, feed_id: int, body: SubscriptionUpdate
) -> dict[str, object] | HTMLResponse:
    def _write(conn: sqlite3.Connection) -> dict[str, object]:
        result = set_site_subscription(
            conn,
            site_id,
            feed_id,
            enabled=body.enabled,
            enqueue_on_enable=True,
        )
        return {
            "site_id": result.site_id,
            "feed_id": result.feed_id,
            "enabled": result.enabled,
            "fetch_enqueued": None if result.fetch is None else result.fetch.enqueued,
        }

    try:
        result = await get_db().write(_write)
    except ProviderOpsError as exc:
        raise ApiError(exc.status_code, exc.message) from exc
    if _wants_html(request):
        from wxverify.web.routes import render_feed_toggles

        return await render_feed_toggles(request, site_id)
    return result


def _affected_sites_for_feed_change(
    conn: sqlite3.Connection, feed: sqlite3.Row, *, inherited_only: bool
) -> set[int]:
    if bool(feed["is_virtual"]):
        return set()
    source = str(feed["source"])
    model = str(feed["model"])
    feed_id = int(feed["id"])
    if source == "meteoblue" and model == "multimodel":
        return _affected_sites_for_meteoblue_package(
            conn, feed_id, inherited_only=inherited_only
        )
    if source == "meteoblue" and model != "multimodel" and inherited_only:
        return set()
    rows = conn.execute(
        f"""
        SELECT DISTINCT fp.site_id
        FROM forecast_pairs fp
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = ?
        WHERE fp.feed_id = ?
          {_inheriting_filter("sfs") if inherited_only else ""}
        """,
        (feed_id, feed_id),
    ).fetchall()
    return {int(row["site_id"]) for row in rows}


def _affected_sites_for_meteoblue_package(
    conn: sqlite3.Connection, package_feed_id: int, *, inherited_only: bool
) -> set[int]:
    rows = conn.execute(
        f"""
        WITH candidate_sites AS (
            SELECT DISTINCT fp.site_id
            FROM forecast_pairs fp
            JOIN feeds member ON member.id = fp.feed_id
            WHERE member.source = 'meteoblue'
              AND member.model != 'multimodel'
            UNION
            SELECT site_id
            FROM site_feed_state
            WHERE feed_id = ?
        )
        SELECT cs.site_id
        FROM candidate_sites cs
        LEFT JOIN site_feed_state pkg_sfs
          ON pkg_sfs.site_id = cs.site_id AND pkg_sfs.feed_id = ?
        WHERE 1=1
          {_inheriting_filter("pkg_sfs") if inherited_only else ""}
        """,
        (package_feed_id, package_feed_id),
    ).fetchall()
    return {int(row["site_id"]) for row in rows}


def _inheriting_filter(state_alias: str) -> str:
    return f"AND {state_alias}.enabled IS NULL"


def _wants_html(request: Request) -> bool:
    return request.headers.get("hx-request", "").lower() == "true"
