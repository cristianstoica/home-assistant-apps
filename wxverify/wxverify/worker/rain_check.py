"""The close's rain check: set aside a gauge that reads zero in rain.

Plan ``2026-11-01-rain-release.md``, Part A (A2, A4). Close stage 3 calls
``check_station_rain`` right after ``write_hourly_values``, in the same
transaction. It judges every listed station of the day with the pure rules
of ``obs/rain_check.py`` over the close's own hourly values, writes one
``station_rain_checks`` row per station and sets each station's rain flag
on the list rows of the days after. The judged day's own figures never
change: a set-aside or take-back counts from the next day.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timedelta
from typing import Final

from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro, parse_utc
from wxverify.db.site_days import (
    day_rain_station_ids,
    day_record,
    day_station_ids,
    set_rain_counted_after,
)
from wxverify.obs.hourly_values import day_hours
from wxverify.obs.rain_check import (
    BEFORE_HOURS,
    DECISIONS,
    OUTCOMES,
    Decision,
    FoldResult,
    JudgeResult,
    Outcome,
    RainCheckRow,
    StationDayReadings,
    fold,
    judge_day,
)
from wxverify.scoring.consensus import materialize_consensus
from wxverify.worker.day_values import StationDayValues, hourly_values

logger = logging.getLogger(__name__)

_OUTCOMES: Final[dict[str, Outcome]] = {outcome: outcome for outcome in OUTCOMES}
_DECISIONS: Final[dict[str, Decision]] = {decision: decision for decision in DECISIONS}
_NO_READINGS: Final = StationDayReadings(
    precip_mm={}, temperature_c={}, gap_hours=frozenset()
)


def _readings(values: StationDayValues | None) -> StationDayReadings:
    """One station's rain, temperature and gap hours from the close's values."""
    if values is None:
        return _NO_READINGS
    return StationDayReadings(
        precip_mm={
            hour: value.value for hour, value in values.values["precip"].items()
        },
        temperature_c={
            hour: value.value for hour, value in values.values["temperature"].items()
        },
        gap_hours=frozenset(values.gap_hours),
    )


def _site_temperatures(
    conn: sqlite3.Connection, site_id: int, low: datetime, high: datetime
) -> dict[datetime, float | None]:
    """The site's ``observations`` temperature by hour, ``low <= hour < high``.

    Text bounds on the dates, widened by a day each side, let the read seek
    the (site, variable, hour) key; the julianday conditions are the exact
    check. A row whose stamp does not parse is left out.
    """
    rows = conn.execute(
        "SELECT valid_at, value FROM observations"
        " WHERE site_id = ? AND variable = 'temperature'"
        " AND valid_at >= ? AND valid_at < ?"
        " AND julianday(valid_at) >= julianday(?)"
        " AND julianday(valid_at) < julianday(?)",
        (
            site_id,
            isoformat_utc(low - timedelta(days=1))[:10],
            isoformat_utc(high + timedelta(days=2))[:10],
            isoformat_utc(low),
            isoformat_utc(high),
        ),
    ).fetchall()
    temps: dict[datetime, float | None] = {}
    for row in rows:
        try:
            hour = parse_utc(str(row[0]))
        except ValueError:
            continue
        temps[hour] = float(row[1])
    return temps


def _history(
    conn: sqlite3.Connection, site_id: int, station_id: int, local_date: date
) -> list[RainCheckRow]:
    """The station's check rows dated before ``local_date``, in date order."""
    rows = conn.execute(
        "SELECT local_date, outcome, decision FROM station_rain_checks"
        " WHERE site_id = ? AND station_id = ? AND local_date < ?"
        " ORDER BY local_date",
        (site_id, station_id, local_date.isoformat()),
    ).fetchall()
    history: list[RainCheckRow] = []
    for row in rows:
        outcome = _OUTCOMES.get(str(row[1]))
        decision = None if row[2] is None else _DECISIONS.get(str(row[2]))
        if outcome is None or (row[2] is not None and decision is None):
            continue
        history.append(
            RainCheckRow(
                local_date=date.fromisoformat(str(row[0])),
                outcome=outcome,
                decision=decision,
            )
        )
    return history


def _rematerialize_rain_after(
    conn: sqlite3.Connection, site_id: int, day_end_utc: datetime, now_utc: datetime
) -> int:
    """Re-materialize every site rain hour from ``day_end_utc`` on that has a
    station row (normally none). Returns the hours re-materialized."""
    rows = conn.execute(
        "SELECT DISTINCT so.valid_at FROM station_observations AS so"
        " JOIN stations AS s ON s.id = so.station_id"
        " WHERE s.site_id = ? AND so.variable = 'precip' AND so.valid_at >= ?"
        " AND julianday(so.valid_at) >= julianday(?)"
        " ORDER BY so.valid_at",
        (
            site_id,
            isoformat_utc(day_end_utc - timedelta(days=1))[:10],
            isoformat_utc(day_end_utc),
        ),
    ).fetchall()
    for row in rows:
        materialize_consensus(
            conn,
            site_id=site_id,
            variable="precip",
            valid_at=str(row[0]),
            now_utc=now_utc,
        )
    return len(rows)


def rain_not_counted_since(conn: sqlite3.Connection, site_id: int) -> dict[int, date]:
    """The site's stations the rain check holds set aside (A5), by station id.

    Each maps to the first day its rain is not counted: the day after the
    check row that set it aside. A station whose latest row counts it is
    left out. Read-only.
    """
    rows = conn.execute(
        "SELECT station_id, local_date, counted_after FROM station_rain_checks"
        " WHERE site_id = ? ORDER BY station_id, local_date",
        (site_id,),
    ).fetchall()
    since: dict[int, date | None] = {}
    for row in rows:
        station_id = int(row[0])
        if int(row[2]) == 1:
            since[station_id] = None
        elif since.get(station_id) is None:
            since[station_id] = date.fromisoformat(str(row[1])) + timedelta(days=1)
    return {sid: day for sid, day in since.items() if day is not None}


def check_station_rain(
    conn: sqlite3.Connection, *, site_id: int, local_date: date, now_utc: datetime
) -> None:
    """Judge the site-day's listed stations and fold each one's state (A4).

    1. Deletes the day's own ``station_rain_checks`` rows, so a stage-3
       retry gives the same result.
    2. Judges every listed station against the day's other counted
       stations, folds it with its earlier rows, applies the site limits,
       and writes one row per station.
    3. Sets each station's ``rain_counted`` to its ``counted_after`` on its
       list rows dated after the day.
    4. Re-materializes every site rain hour from the day's end on that has
       a station row.

    The values are the close's own (``hourly_values``); the temperatures
    before the day are the site's ``observations``. Does nothing when the
    site-day has no record. Runs in the caller's transaction: no BEGIN, no
    commit; reads the clock only through ``now_utc``.
    """
    record = day_record(conn, site_id, local_date)
    if record is None:
        return
    day = local_date.isoformat()
    conn.execute(
        "DELETE FROM station_rain_checks WHERE site_id = ? AND local_date = ?",
        (site_id, day),
    )
    listed = sorted(day_station_ids(conn, site_id, local_date))
    counted = day_rain_station_ids(conn, site_id, local_date)
    hours = day_hours(record.day_start_utc, record.day_end_utc)
    values = hourly_values(conn, site_id=site_id, local_date=local_date)
    readings = {sid: _readings(values.stations.get(sid)) for sid in listed}
    temps = _site_temperatures(
        conn,
        site_id,
        record.day_start_utc - timedelta(hours=BEFORE_HOURS),
        record.day_end_utc,
    )
    judged: dict[int, JudgeResult] = {
        sid: judge_day(
            hours,
            readings[sid],
            [readings[peer] for peer in listed if peer != sid and peer in counted],
            site_temps_before=temps,
        )
        for sid in listed
    }
    histories = {sid: _history(conn, site_id, sid, local_date) for sid in listed}
    folded: dict[int, FoldResult] = {
        sid: fold(
            histories[sid],
            local_date,
            judged[sid].outcome,
            counted_before=int(sid in counted),
        )
        for sid in listed
    }
    candidates = [sid for sid in listed if folded[sid].decision == "set_aside"]
    for sid in candidates:
        folded[sid] = fold(
            histories[sid],
            local_date,
            judged[sid].outcome,
            counted_before=1,
            other_new_candidates_today=len(candidates) - 1,
            other_station_set_aside_today=any(
                other != sid and other not in counted for other in listed
            ),
        )
    stamp = isoformat_utc_micro(now_utc)
    for sid in listed:
        result, state = judged[sid], folded[sid]
        conn.execute(
            "INSERT INTO station_rain_checks (site_id, local_date, station_id,"
            " outcome, reason, station_mm, peer_median_mm, peers, decision,"
            " counted_after, checked_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                site_id,
                day,
                sid,
                result.outcome,
                result.reason,
                result.station_mm,
                result.peer_median_mm,
                result.peers,
                state.decision,
                state.counted_after,
                stamp,
            ),
        )
        set_rain_counted_after(
            conn, site_id, sid, local_date, counted=state.counted_after
        )
        if state.decision in ("set_aside", "taken_back"):
            logger.info(
                "rain check %s site=%s station=%s date=%s",
                state.decision,
                site_id,
                sid,
                day,
            )
    rematerialized = _rematerialize_rain_after(
        conn, site_id, record.day_end_utc, now_utc
    )
    logger.debug(
        "rain check site=%s date=%s stations=%d rematerialized=%d",
        site_id,
        day,
        len(listed),
        rematerialized,
    )
