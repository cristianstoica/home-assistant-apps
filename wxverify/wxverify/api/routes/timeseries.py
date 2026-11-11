"""Forecast-vs-observed timeseries route.

The reply is the feed's forecast history (``forecast_history`` +
``observations``, the closed days and the days before the site's first day
record together, persistence rebuilt at the longest lead), the provisional
observed line on the days not yet done, and the missed and zone-flagged days
of the range. Every part comes from one read snapshot, so the history and
the provisional part meet at the same instants. Each hour gives one forecast
of one day ahead, ``lead`` (default "D+1"), the Overlay's pick.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Final

from fastapi import APIRouter, HTTPException, Query

from wxverify.core.lead import parse_day_ahead
from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.connection import get_db
from wxverify.db.site_days import (
    DayRecord,
    closed_through,
    current_day,
    day_record,
)
from wxverify.db.snapshot import read_snapshot
from wxverify.provisional_line import DaySpan, day_spans, provisional_site_hours
from wxverify.scoring.history import chart_pairs

router = APIRouter(prefix="/api", tags=["timeseries"])

_ONE_DAY: Final = timedelta(days=1)
#: The smallest step of a ``datetime``: the inclusive ``to`` as a half-open end.
_TICK: Final = timedelta(microseconds=1)
#: The largest day ahead a pair holds (the ``day_ahead`` CHECK of ``forecast_history``).
_MAX_DAY_AHEAD: Final = 7
_LEAD_REFUSED: Final = "lead is not a day ahead from D+0 to D+7"

#: One pair as the reply lists it: ``valid_at``, forecast, observed.
_PairRow = tuple[str, float, float]


@router.get("/timeseries")
async def timeseries(
    site: int = Query(...),
    variable: str = Query("temperature"),
    feed_id: int = Query(...),
    from_: str | None = Query(default=None, alias="from"),
    to: str | None = None,
    lead: str = Query("D+1"),
) -> dict[str, object]:
    def _read(conn: sqlite3.Connection) -> dict[str, object]:
        if conn.in_transaction:
            return _reply(conn, site, variable, feed_id, from_, to, lead)
        with read_snapshot(conn, label="timeseries"):
            return _reply(conn, site, variable, feed_id, from_, to, lead)

    return await get_db().read(_read)


def _reply(
    conn: sqlite3.Connection,
    site: int,
    variable: str,
    feed_id: int,
    from_: str | None,
    to: str | None,
    lead: str,
) -> dict[str, object]:
    """The route's reply."""
    head: dict[str, object] = {"site": site, "variable": variable, "feed_id": feed_id}
    return {
        **head,
        **_reply_lists(
            conn,
            site=site,
            variable=variable,
            feed_id=feed_id,
            from_=from_,
            to=to,
            lead=lead,
        ),
    }


def _reply_lists(
    conn: sqlite3.Connection,
    *,
    site: int,
    variable: str,
    feed_id: int,
    from_: str | None,
    to: str | None,
    lead: str,
) -> dict[str, object]:
    """The reply's lists.

    The pairs are ``chart_pairs``: one range read of
    the feed's ``forecast_history`` at ``lead``'s day ahead, the ``closed``
    days and the days before the site's first record together, within
    ``from`` and ``to``; for persistence, rebuilt at the longest lead. Days
    that are ``open``, ``closing``, ``retry_due`` or ``missed`` give no
    pair: the provisional line draws the first three, and a missed day has
    no observed line. ``now`` is read once.

    ``lead`` is checked first, next to ``from`` and ``to``; each hour then
    gives one forecast of that day ahead, with no fill from another.
    """
    now = utc_now()
    low = _instant(from_, "from")
    high = _instant(to, "to")
    day_ahead = _day_ahead(lead)
    rows = chart_pairs(
        conn,
        site_id=site,
        feed_id=feed_id,
        variable=variable,
        day_ahead=day_ahead,
        low=low,
        high=high,
    )
    through = closed_through(conn, site)
    top = None if through is None else day_record(conn, site, through)
    first = _first_record_start(conn, site, top, now)
    spans = _done_spans(conn, site, first=first, top=top, low=low, high=high)
    line = provisional_site_hours(conn, site_id=site, variable=variable, now_utc=now)
    hours = [hour for hour in line.valid_at if _inside(hour, low, high)]
    latest = line.latest_reading_at if hours else None
    return {
        **_pair_lists(rows),
        "provisional_valid_at": [isoformat_utc(hour) for hour in hours],
        "provisional": [line.values.get(hour) for hour in hours],
        "provisional_latest_reading_at": None
        if latest is None
        else isoformat_utc(latest),
        "missed_days": [
            {
                "local_date": span.local_date.isoformat(),
                "start": isoformat_utc(span.start_utc),
                "end": isoformat_utc(span.end_utc),
                "reason": "zone changeover" if span.zone_changeover else "missed",
            }
            for span in spans
            if span.state == "missed"
        ],
        "zone_days": [
            {"local_date": span.local_date.isoformat(), "timezone": span.timezone}
            for span in spans
            if span.state == "closed" and span.zone_flagged
        ],
    }


def _pair_lists(rows: list[_PairRow]) -> dict[str, object]:
    """The reply's three parallel pair lists."""
    return {
        "valid_at": [row[0] for row in rows],
        "forecast": [row[1] for row in rows],
        "observed": [row[2] for row in rows],
    }


def _instant(value: str | None, name: str) -> datetime | None:
    """``value`` parsed as a UTC instant; 422 when it does not parse."""
    if value is None:
        return None
    try:
        return parse_utc(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"{name} is not a UTC timestamp"
        ) from exc


def _day_ahead(lead: str) -> int:
    """``lead`` as a day ahead from 0 to 7, through the dashboard's parser;
    422 when it does not parse or lies outside that range."""
    try:
        day_ahead = parse_day_ahead(lead)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=_LEAD_REFUSED) from exc
    if not 0 <= day_ahead <= _MAX_DAY_AHEAD:
        raise HTTPException(status_code=422, detail=_LEAD_REFUSED)
    return day_ahead


def _inside(instant: datetime, low: datetime | None, high: datetime | None) -> bool:
    """Whether ``instant`` lies within ``from`` and ``to``, both included."""
    return (low is None or instant >= low) and (high is None or instant <= high)


def _first_record_start(
    conn: sqlite3.Connection, site_id: int, top: DayRecord | None, now: datetime
) -> datetime | None:
    """Where the site's starting history ends: its first record's start.

    Found by walking down by date from the latest done day or, with none
    done, from the day that holds ``now``: the day records follow one another
    one date apart. ``None`` when neither exists; then no record has started
    and every stored pair is starting history.
    """
    record = top if top is not None else current_day(conn, site_id, now_utc=now)
    if record is None:
        return None
    earlier = day_record(conn, site_id, record.local_date - _ONE_DAY)
    while earlier is not None:
        record = earlier
        earlier = day_record(conn, site_id, record.local_date - _ONE_DAY)
    return record.day_start_utc


def _done_spans(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    first: datetime | None,
    top: DayRecord | None,
    low: datetime | None,
    high: datetime | None,
) -> list[DaySpan]:
    """The done days that overlap ``from`` and ``to``, oldest first.

    ``day_spans`` from the site's first record through its latest done day,
    ``top``. Days become done in date order, so each span is ``closed`` or
    ``missed``; the days after ``top`` are the provisional line's. Empty
    with no done day.
    """
    if first is None or top is None:
        return []
    start = first if low is None or low < first else low
    end = top.day_end_utc if high is None or high >= top.day_end_utc else high + _TICK
    return day_spans(conn, site_id, start, end)
