"""Forecast-vs-observed timeseries route."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Query

from wxverify.db.connection import get_db

router = APIRouter(prefix="/api", tags=["timeseries"])


@router.get("/timeseries")
async def timeseries(
    site: int = Query(...),
    variable: str = Query("temperature"),
    feed_id: int = Query(...),
    issued_at: str | None = None,
    from_: str | None = Query(default=None, alias="from"),
    to: str | None = None,
) -> dict[str, object]:
    def _read(conn: sqlite3.Connection) -> dict[str, object]:
        clauses = ["site_id=?", "variable=?", "feed_id=?"]
        params: list[object] = [site, variable, feed_id]
        if issued_at is not None:
            clauses.append("issued_at=?")
            params.append(issued_at)
        if from_ is not None:
            clauses.append("valid_at>=?")
            params.append(from_)
        if to is not None:
            clauses.append("valid_at<=?")
            params.append(to)
        rows = conn.execute(
            f"""
            SELECT valid_at, forecast, observed
            FROM forecast_pairs
            WHERE {" AND ".join(clauses)}
            ORDER BY valid_at
            """,
            params,
        ).fetchall()
        return {
            "site": site,
            "variable": variable,
            "feed_id": feed_id,
            "valid_at": [str(row["valid_at"]) for row in rows],
            "forecast": [float(row["forecast"]) for row in rows],
            "observed": [float(row["observed"]) for row in rows],
        }

    return await get_db().read(_read)
