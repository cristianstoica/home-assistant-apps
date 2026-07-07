"""Health and ops routes."""

from __future__ import annotations

import sqlite3
from typing import Annotated

from fastapi import APIRouter, Query

from wxverify.collection.budget import current_billing_day
from wxverify.collection.forecast_fetcher import NO_USABLE_SAMPLES_SENTINEL
from wxverify.core.secrets import key_status
from wxverify.db.connection import get_db
from wxverify.db.runtime_state import runtime_status
from wxverify.provider_ops import provider_health

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health/keys")
async def health_keys() -> dict[str, bool]:
    return key_status()


@router.get("/health/budget")
async def health_budget() -> list[dict[str, object]]:
    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        rows = conn.execute(
            """
            SELECT source, daily_call_limit, daily_credit_limit, billing_tz
            FROM sources s
            ORDER BY s.source
            """
        ).fetchall()
        out: list[dict[str, object]] = []
        for row in rows:
            source = str(row["source"])
            budget = conn.execute(
                """
                SELECT calls, credits
                FROM api_budget
                WHERE source = ? AND billing_day = ?
                """,
                (source, current_billing_day(str(row["billing_tz"]))),
            ).fetchone()
            out.append(
                {
                    "source": source,
                    "daily_call_limit": int(row["daily_call_limit"]),
                    "daily_credit_limit": row["daily_credit_limit"],
                    "calls": 0 if budget is None else int(budget["calls"]),
                    "credits": 0 if budget is None else int(budget["credits"]),
                }
            )
        return out

    return await get_db().read(_read)


@router.get("/health/feeds")
async def health_feeds() -> list[dict[str, object]]:
    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        rows = conn.execute(
            """
            SELECT s.id AS site_id, s.name AS site_name, f.id AS feed_id,
                   s.enabled AS site_enabled,
                   f.source, f.model, f.enabled AS feed_enabled,
                   f.default_subscribed, f.disabled_reason,
                   sfs.enabled AS override_enabled, sfs.last_run_at, sfs.last_error,
                   sfs.error_count,
                   COALESCE(sample_counts.n, 0) AS sample_count
            FROM sites s
            JOIN feeds f
            LEFT JOIN site_feed_state sfs
              ON sfs.site_id = s.id AND sfs.feed_id = f.id
            LEFT JOIN (
                SELECT fs.site_id,
                       CASE
                         WHEN sf.source='meteoblue' AND sf.model!='multimodel'
                         THEN pkg.id
                         ELSE fs.feed_id
                       END AS feed_id,
                       COUNT(*) AS n
                FROM forecast_samples fs
                JOIN feeds sf ON sf.id = fs.feed_id
                LEFT JOIN feeds pkg
                  ON pkg.source='meteoblue' AND pkg.model='multimodel'
                GROUP BY fs.site_id,
                         CASE
                           WHEN sf.source='meteoblue' AND sf.model!='multimodel'
                           THEN pkg.id
                           ELSE fs.feed_id
                         END
            ) sample_counts
              ON sample_counts.site_id = s.id AND sample_counts.feed_id = f.id
            WHERE f.is_virtual = 0
              AND NOT (f.source='meteoblue' AND f.model != 'multimodel')
            ORDER BY s.name, f.source, f.model
            """
        ).fetchall()
        out: list[dict[str, object]] = []
        for row in rows:
            subscribed = bool(
                row["override_enabled"]
                if row["override_enabled"] is not None
                else row["default_subscribed"]
            )
            if not bool(row["site_enabled"]):
                status = "site disabled"
            elif not bool(row["feed_enabled"]):
                status = "disabled"
            elif not subscribed:
                status = "not subscribed / available"
            elif row["last_error"] == NO_USABLE_SAMPLES_SENTINEL:
                status = "fetched, 0 usable"
            elif row["last_error"] is not None:
                status = "error"
            elif row["last_run_at"] is None:
                status = "never run / due"
            elif int(row["sample_count"]) == 0:
                status = "ran / no usable data"
            else:
                status = "ok"
            out.append(
                {
                    "site_id": int(row["site_id"]),
                    "site_name": str(row["site_name"]),
                    "feed_id": int(row["feed_id"]),
                    "source": str(row["source"]),
                    "model": str(row["model"]),
                    "subscribed": subscribed,
                    "status": status,
                    "disabled_reason": row["disabled_reason"],
                    "last_run_at": row["last_run_at"],
                    "last_error": row["last_error"],
                    "error_count": int(row["error_count"] or 0),
                    "feed_enabled": bool(row["feed_enabled"]),
                    "site_enabled": bool(row["site_enabled"]),
                    "sample_count": int(row["sample_count"]),
                }
            )
        return out

    return await get_db().read(_read)


@router.get("/health/providers")
async def health_providers(
    site_id: int | None = None,
    source: Annotated[list[str] | None, Query()] = None,
) -> list[dict[str, object]]:
    return await get_db().read(
        lambda conn: provider_health(conn, site_id=site_id, sources=source or [])
    )


@router.get("/health/backfill")
async def health_backfill() -> list[dict[str, object]]:
    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        return [
            {
                "site_id": int(row["id"]),
                "status": row["backfill_status"],
                "through": row["backfill_through"],
            }
            for row in conn.execute(
                "SELECT id, backfill_status, backfill_through FROM sites ORDER BY id"
            )
        ]

    return await get_db().read(_read)


@router.get("/health/backoffs")
async def health_backoffs() -> list[dict[str, object]]:
    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        return [
            {
                "domain": str(row["domain"]),
                "next_attempt_at": str(row["next_attempt_at"]),
                "retry_count": int(row["retry_count"]),
            }
            for row in conn.execute(
                """
                SELECT domain, next_attempt_at, retry_count
                FROM domain_backoffs
                ORDER BY next_attempt_at
                """
            )
        ]

    return await get_db().read(_read)


@router.get("/worker/status")
async def worker_status() -> dict[str, object]:
    def _read(conn: sqlite3.Connection) -> dict[str, object]:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM jobs GROUP BY status"
        ).fetchall()
        status: dict[str, object] = {
            "jobs": {str(row["status"]): int(row["n"]) for row in rows}
        }
        status.update(runtime_status(conn))
        for job_type in ("fetch_feed", "fetch_obs", "pair_and_score"):
            row = conn.execute(
                """
                SELECT MAX(updated_at) AS completed_at
                FROM jobs
                WHERE status='completed' AND type=?
                """,
                (job_type,),
            ).fetchone()
            status[f"last_completed_{job_type}_at"] = (
                None if row is None else row["completed_at"]
            )
        return status

    return await get_db().read(_read)
