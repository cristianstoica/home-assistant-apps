"""Dashboard JSON routes."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from fastapi import APIRouter, Query

from wxverify.api.schemas import LeaderboardOut
from wxverify.core.lead import parse_day_ahead
from wxverify.db.connection import get_db
from wxverify.db.queue import enqueue_if_absent
from wxverify.scoring.composite import composite as composite_query
from wxverify.scoring.leaderboard import LeaderboardResult, leaderboard_with_status
from wxverify.scoring.winrate import winrate as winrate_query
from wxverify.web.context import feed_label

_CURVE_LEADS: list[int] = list(range(0, 8))
_MAX_SERIES = 6  # number of --chart-* palette tokens

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
    lead: str = Query("D+1"),
    top: int = Query(5),
) -> dict[str, object]:
    # Selected lead resolves by membership in the lead universe: an unparseable
    # value coerces to the D+1 default, and an out-of-range value (e.g. D+99)
    # is treated as having no eligible point at the selected lead so the whole
    # series list sorts nulls-last — never a ValueError/IndexError 500.
    selected_day = _coerce_lead_day(lead)
    selected_index = selected_day if selected_day in _CURVE_LEADS else None
    # Clamp to [1, _MAX_SERIES]: the upper bound keeps series colours from
    # cycling/colliding; the lower bound stops a crafted ?top=0/-1 from
    # emptying the chart or, via naive slicing, blowing past the guarantee.
    top_clamped = max(1, min(_MAX_SERIES, top))

    def _read(conn: sqlite3.Connection) -> tuple[dict[str, object], bool]:
        results = [
            leaderboard_with_status(
                conn,
                site_id=site,
                variable=variable,
                day_ahead=day,
                window=window,
            )
            for day in _CURVE_LEADS
        ]
        series = _build_series(results, selected_index, top_clamped)
        return {
            "site": site,
            "variable": variable,
            "window": window,
            "leads": list(_CURVE_LEADS),
            "series": series,
            "window_key": results[0].window_key if results else None,
            "window_days": results[0].window_days if results else None,
        }, any(result.cache_miss for result in results)

    result, cache_miss = await get_db().read(_read)
    if cache_miss:
        await get_db().write(lambda conn: _enqueue_score(conn, site))
    return result


def _coerce_lead_day(lead: str, default: int = 1) -> int:
    try:
        return parse_day_ahead(lead)
    except (ValueError, TypeError):
        return default


@dataclass
class _FeedCurve:
    feed_id: int
    source: str
    model: str
    skill: list[float | None]


def _build_series(
    results: list[LeaderboardResult],
    selected_index: int | None,
    top: int,
) -> list[dict[str, object]]:
    """Assemble per-feed skill curves across the lead universe.

    Series universe is the union of feeds appearing at any lead; each feed's
    ``skill`` array is indexed by lead and carries the skill value only where
    the underlying leaderboard row is eligible (``confident``: ``n >= min_n``
    and ``skill_score`` non-None), ``null`` elsewhere. Feeds whose ``skill``
    is entirely ``null`` are dropped before ordering/``top`` so an undrawable
    feed never consumes a slot or displaces a drawable one.
    """
    feeds: dict[int, _FeedCurve] = {}
    for lead_idx, result in enumerate(results):
        for row in result.rows:
            curve = feeds.setdefault(
                row.feed_id,
                _FeedCurve(
                    feed_id=row.feed_id,
                    source=row.source,
                    model=row.model,
                    skill=[None] * len(results),
                ),
            )
            if row.confident:
                curve.skill[lead_idx] = row.skill_score

    # Explicit is-not-None test: a skill value of exactly 0.0 is a valid
    # eligible point (feed MSE == persistence MSE, or ETS 0), and a truthiness
    # test would silently drop it.
    drawable = [
        curve
        for curve in feeds.values()
        if any(value is not None for value in curve.skill)
    ]

    def _sort_key(curve: _FeedCurve) -> tuple[int, float, str]:
        selected = None if selected_index is None else curve.skill[selected_index]
        label = feed_label(curve.source, curve.model)
        # Nulls last, skill DESC within the drawable bucket, label ASC tiebreak.
        return (
            1 if selected is None else 0,
            0.0 if selected is None else -selected,
            label,
        )

    drawable.sort(key=_sort_key)
    return [
        {
            "feed_id": curve.feed_id,
            "label": feed_label(curve.source, curve.model),
            "skill": curve.skill,
        }
        for curve in drawable[:top]
    ]


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
