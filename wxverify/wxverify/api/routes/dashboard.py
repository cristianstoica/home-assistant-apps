"""Dashboard JSON routes.

With the daily close on, ``/leaderboard``, ``/composite`` and ``/winrate``
send the active publication's cutoff as an ``X-Evidence-Through`` header
(ISO date) and keep their bodies' shape; ``/curve`` carries it in its body
as ``evidence_through`` (9.6). With the close off no header is sent and
``evidence_through`` is None.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response

from wxverify.api.schemas import LeaderboardOut
from wxverify.core.lead import parse_day_ahead
from wxverify.db.connection import get_db, pinned_read_snapshot
from wxverify.scoring.composite import composite_with_status
from wxverify.scoring.leaderboard import (
    LeaderboardResult,
    LeaderboardStatus,
    leaderboard_in_snapshot,
    leaderboard_with_status,
)
from wxverify.scoring.rescore import schedule_score_rescore
from wxverify.scoring.winrate import winrate_with_evidence
from wxverify.web.context import feed_label

_CURVE_LEADS: list[int] = list(range(0, 8))
_MAX_SERIES = 6  # number of --chart-* palette tokens
_EVIDENCE_HEADER = "X-Evidence-Through"

router = APIRouter(prefix="/api", tags=["dashboard"])


async def _request_response(response: Response) -> Response:
    """The request's ``Response``, which FastAPI injects here.

    FastAPI injects a parameter annotated ``Response`` but not one annotated
    ``Response | None``, so the handlers take it through this dependency:
    that keeps their ``response`` optional, and a direct call with no
    request returns the same body and sends no header.
    """
    return response


# A handler's ``response``: the request's, or None on a direct call.
_OptionalResponse = Annotated[Response | None, Depends(_request_response)]


@router.get("/leaderboard", response_model=list[LeaderboardOut])
async def leaderboard(
    response: _OptionalResponse = None,
    site: int = Query(...),
    variable: str = Query("temperature"),
    window: str = Query("rolling"),
    lead: str = Query("D+1"),
) -> list[LeaderboardOut]:
    day_ahead = _lead_to_day(lead)

    def _read(
        conn: sqlite3.Connection,
    ) -> tuple[list[LeaderboardOut], LeaderboardStatus, date | None]:
        result = leaderboard_with_status(
            conn,
            site_id=site,
            variable=variable,
            day_ahead=day_ahead,
            window=window,
        )
        return (
            [LeaderboardOut(**row.__dict__) for row in result.rows],
            result.status,
            result.evidence_through,
        )

    result, status, evidence_through = await get_db().read(_read)
    _set_evidence_header(response, evidence_through)

    if status in ("stale", "rebuilding"):
        schedule_score_rescore(site)

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
        # The eight leads in one snapshot, so with the close on every one
        # reads the same publication (9.3).
        with pinned_read_snapshot(conn, label="curve"):
            results = [
                leaderboard_in_snapshot(
                    conn,
                    site_id=site,
                    variable=variable,
                    day_ahead=day,
                    window=window,
                )
                for day in _CURVE_LEADS
            ]
        series = _build_series(results, selected_index, top_clamped)
        evidence_through = results[0].evidence_through if results else None
        return {
            "site": site,
            "variable": variable,
            "window": window,
            "leads": list(_CURVE_LEADS),
            "series": series,
            "window_key": results[0].window_key if results else None,
            "window_days": results[0].window_days if results else None,
            "evidence_through": (
                None if evidence_through is None else evidence_through.isoformat()
            ),
        }, any(result.status in ("stale", "rebuilding") for result in results)

    result, needs_rescore = await get_db().read(_read)
    # `empty` leads do not gate: a genuinely inapplicable curve slice must not
    # turn into a rebuild request of its own.
    if needs_rescore:
        schedule_score_rescore(site)
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
    response: _OptionalResponse = None,
    site: int = Query(...),
    variable: str = Query("temperature"),
    lead: str = Query("D+1"),
    window: str = Query("rolling"),
) -> list[dict[str, object]]:
    result = await get_db().read(
        lambda conn: winrate_with_evidence(
            conn,
            site_id=site,
            variable=variable,
            day_ahead=_lead_to_day(lead),
            window=window,
        )
    )
    _set_evidence_header(response, result.evidence_through)
    return result.rows


@router.get("/composite")
async def composite(
    response: _OptionalResponse = None,
    site: int = Query(...),
    window: str = Query("rolling"),
) -> list[dict[str, object]]:
    result = await get_db().read(
        lambda conn: composite_with_status(conn, site_id=site, window=window)
    )
    _set_evidence_header(response, result.evidence_through)
    # The read connection is already released; the fire-and-forget scheduler
    # carries the terminal-failure cooldown and never gates the response.
    if result.status in ("stale", "rebuilding"):
        schedule_score_rescore(site)
    return result.rows


def _set_evidence_header(
    response: Response | None, evidence_through: date | None
) -> None:
    """Send the cutoff as ``X-Evidence-Through`` when there is one (9.6).

    ``response`` is None on a direct call, which sends nothing.
    """
    if response is not None and evidence_through is not None:
        response.headers[_EVIDENCE_HEADER] = evidence_through.isoformat()


def _lead_to_day(lead: str) -> int:
    return parse_day_ahead(lead)
