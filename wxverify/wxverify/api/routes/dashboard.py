"""Dashboard JSON routes."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, Query

from wxverify.api.schemas import LeaderboardOut
from wxverify.core.lead import parse_day_ahead
from wxverify.db.connection import get_db
from wxverify.db.queue import enqueue_if_absent
from wxverify.scoring.composite import composite as composite_query
from wxverify.scoring.leaderboard import leaderboard_with_status
from wxverify.scoring.winrate import winrate as winrate_query

router = APIRouter(prefix="/api", tags=["dashboard"])


@router.get("/leaderboard", response_model=list[LeaderboardOut])
async def leaderboard(
    site: int = Query(...),
    variable: str = Query("temperature"),
    window: str = Query("rolling"),
    lead: str = Query("D+1"),
) -> list[LeaderboardOut]:
    day_ahead = _lead_to_day(lead)

    def _read(conn: sqlite3.Connection) -> tuple[list[LeaderboardOut], bool]:
        result = leaderboard_with_status(
            conn,
            site_id=site,
            variable=variable,
            day_ahead=day_ahead,
            window=window,
        )
        return [
            LeaderboardOut(**row.__dict__) for row in result.rows
        ], result.cache_miss

    result, cache_miss = await get_db().read(_read)

    if cache_miss:
        await get_db().write(lambda conn: _enqueue_score(conn, site))

    return result


@router.get("/curve")
async def curve(
    site: int = Query(...),
    variable: str = Query("temperature"),
    window: str = Query("rolling"),
) -> dict[str, object]:
    def _read(conn: sqlite3.Connection) -> tuple[dict[str, object], bool]:
        results = [
            leaderboard_with_status(
                conn,
                site_id=site,
                variable=variable,
                day_ahead=day,
                window=window,
            )
            for day in range(0, 8)
        ]
        return {
            "site": site,
            "variable": variable,
            "window": window,
            "window_key": results[0].window_key if results else None,
            "window_days": results[0].window_days if results else None,
            "rows": [row.__dict__ for result in results for row in result.rows],
        }, any(result.cache_miss for result in results)

    result, cache_miss = await get_db().read(_read)
    if cache_miss:
        await get_db().write(lambda conn: _enqueue_score(conn, site))
    return result


@router.get("/winrate")
async def winrate(
    site: int = Query(...),
    variable: str = Query("temperature"),
    lead: str = Query("D+1"),
    window: str = Query("rolling"),
) -> list[dict[str, object]]:
    def _read(conn: sqlite3.Connection) -> list[dict[str, object]]:
        return winrate_query(
            conn,
            site_id=site,
            variable=variable,
            day_ahead=_lead_to_day(lead),
            window=window,
        )

    return await get_db().read(_read)


@router.get("/composite")
async def composite(
    site: int = Query(...), window: str = Query("rolling")
) -> list[dict[str, object]]:
    return await get_db().read(
        lambda conn: composite_query(conn, site_id=site, window=window)
    )


def _lead_to_day(lead: str) -> int:
    return parse_day_ahead(lead)


def _enqueue_score(conn: sqlite3.Connection, site_id: int) -> None:
    enqueue_if_absent(conn, "pair_and_score", site_id, "score", {"site_id": site_id})
