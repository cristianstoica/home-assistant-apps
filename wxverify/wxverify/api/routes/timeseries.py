"""Forecast-vs-observed timeseries route.

With the daily close off, the reply is the site's stored pairs, as before,
plus the close's keys, empty. With it on (daily-close plan P5, §9.2), the
stored pairs before the site's first day record (the starting history), P4's
closed-day pairs on each closed day, the provisional observed line on the
days not yet done, and the missed and zone-flagged days of the range. Every
part comes from one read snapshot, so the closed part and the provisional
part meet at the same instants.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from itertools import groupby
from typing import Final

from fastapi import APIRouter, HTTPException, Query

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.connection import get_db
from wxverify.db.site_days import (
    DayRecord,
    closed_through,
    current_day,
    daily_close_on,
    day_record,
)
from wxverify.db.snapshot import read_snapshot
from wxverify.db.tz_generations import published_generation_clause
from wxverify.provisional_line import DaySpan, day_spans, provisional_site_hours
from wxverify.scoring.publication import ClosedPair, closed_day_pairs

router = APIRouter(prefix="/api", tags=["timeseries"])

_ONE_DAY: Final = timedelta(days=1)
#: The smallest step of a ``datetime``: the inclusive ``to`` as a half-open end.
_TICK: Final = timedelta(microseconds=1)

#: One pair as the reply lists it: ``valid_at``, forecast, observed.
_PairRow = tuple[str, float, float]


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
        if conn.in_transaction:
            return _reply(conn, site, variable, feed_id, issued_at, from_, to)
        with read_snapshot(conn, label="timeseries"):
            return _reply(conn, site, variable, feed_id, issued_at, from_, to)

    return await get_db().read(_read)


def _reply(
    conn: sqlite3.Connection,
    site: int,
    variable: str,
    feed_id: int,
    issued_at: str | None,
    from_: str | None,
    to: str | None,
) -> dict[str, object]:
    """The route's reply; asks ``daily_close_on`` once (plan §9)."""
    head: dict[str, object] = {"site": site, "variable": variable, "feed_id": feed_id}
    if not daily_close_on(conn):
        rows = _stored_pairs(
            conn,
            site=site,
            variable=variable,
            feed_id=feed_id,
            issued_at=issued_at,
            from_=from_,
            to=to,
            before=None,
        )
        return {
            **head,
            **_pair_lists(rows),
            "provisional_valid_at": [],
            "provisional": [],
            "provisional_latest_reading_at": None,
            "missed_days": [],
            "zone_days": [],
        }
    return {
        **head,
        **_close_on_keys(
            conn,
            site=site,
            variable=variable,
            feed_id=feed_id,
            issued_at=issued_at,
            from_=from_,
            to=to,
        ),
    }


def _close_on_keys(
    conn: sqlite3.Connection,
    *,
    site: int,
    variable: str,
    feed_id: int,
    issued_at: str | None,
    from_: str | None,
    to: str | None,
) -> dict[str, object]:
    """The reply's lists with the daily close on (plan §9.2).

    The stored pairs keep today's query, limited to the starting history.
    The closed days' pairs are P4's ``closed_day_pairs``, one call per
    ``closed`` day in range; none while ``issued_at`` is given, since a
    closed-day pair carries no issue time to match. Pairs on days that are
    ``open``, ``closing``, ``retry_due`` or ``missed`` are never returned:
    the provisional line draws the first three, and a missed day has no
    observed line. ``now`` is read once.
    """
    now = utc_now()
    low = _instant(from_, "from")
    high = _instant(to, "to")
    through = closed_through(conn, site)
    top = None if through is None else day_record(conn, site, through)
    first = _first_record_start(conn, site, top, now)
    rows = _stored_pairs(
        conn,
        site=site,
        variable=variable,
        feed_id=feed_id,
        issued_at=issued_at,
        from_=from_,
        to=to,
        before=None if first is None else isoformat_utc(first),
    )
    spans = _done_spans(conn, site, first=first, top=top, low=low, high=high)
    if issued_at is None:
        for span in spans:
            if span.state != "closed":
                continue
            rows.extend(
                (pair.valid_at, pair.forecast, pair.observed)
                for pair in _closed_rows(
                    conn,
                    site=site,
                    feed_id=feed_id,
                    variable=variable,
                    span=span,
                    low=low,
                    high=high,
                )
            )
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


def _stored_pairs(
    conn: sqlite3.Connection,
    *,
    site: int,
    variable: str,
    feed_id: int,
    issued_at: str | None,
    from_: str | None,
    to: str | None,
    before: str | None,
) -> list[_PairRow]:
    """The feed's stored ``forecast_pairs`` rows, oldest first.

    ``before``, when given, keeps only rows whose ``valid_at`` is earlier:
    with the close on, the instants before the site's first record.
    """
    clauses = [
        "site_id=?",
        "variable=?",
        "feed_id=?",
        published_generation_clause("forecast_pairs"),
    ]
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
    if before is not None:
        clauses.append("valid_at<?")
        params.append(before)
    rows = conn.execute(
        f"""
        SELECT valid_at, forecast, observed
        FROM forecast_pairs
        WHERE {" AND ".join(clauses)}
        ORDER BY valid_at
        """,
        params,
    ).fetchall()
    return [
        (str(row["valid_at"]), float(row["forecast"]), float(row["observed"]))
        for row in rows
    ]


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


def _inside(instant: datetime, low: datetime | None, high: datetime | None) -> bool:
    """Whether ``instant`` lies within ``from`` and ``to``, both included."""
    return (low is None or instant >= low) and (high is None or instant <= high)


def _first_record_start(
    conn: sqlite3.Connection, site_id: int, top: DayRecord | None, now: datetime
) -> datetime | None:
    """Where the site's starting history ends: its first record's start.

    Found by walking down by date from the latest done day or, with none
    done, from the day that holds ``now``: P1's records follow one another
    one date apart. ``None`` when neither exists; then no record has started
    and every stored pair is starting history (plan §9.1, §12.2).
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


def _closed_rows(
    conn: sqlite3.Connection,
    *,
    site: int,
    feed_id: int,
    variable: str,
    span: DaySpan,
    low: datetime | None,
    high: datetime | None,
) -> list[ClosedPair]:
    """One closed day's pairs for the reply, oldest first (plan §9.2).

    P4's closed-day pairs of the feed and variable whose ``valid_at`` lies
    in the day's span and within ``from`` and ``to``; then, hour by hour,
    the rows ``_closed_hour_pick`` keeps.
    """
    kept: list[ClosedPair] = []
    for pair in closed_day_pairs(
        conn, site_id=site, local_date=span.local_date, feed_id=feed_id
    ):
        if pair.variable != variable:
            continue
        valid_at = parse_utc(pair.valid_at)
        if span.start_utc <= valid_at < span.end_utc and _inside(valid_at, low, high):
            kept.append(pair)
    return [
        pair
        for _, hour_rows in groupby(kept, key=lambda pair: pair.valid_at)
        for pair in _closed_hour_pick(list(hour_rows))
    ]


def _closed_hour_pick(rows: list[ClosedPair]) -> list[ClosedPair]:
    """The forecasts the Overlay draws at one closed hour (plan §9.2).

    A closed hour holds every forecast its day scored: for a real feed one
    per day ahead, persistence one per lead. Which of them to draw (every
    one, or one day ahead) is the owner's open question in plan §17.2, and
    the owner's answer decides this pick. Until then every scored forecast
    of the hour passes through unchanged.
    """
    return rows
