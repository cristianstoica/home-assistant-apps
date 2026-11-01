"""The close's day values: the gap check, the gap fill and the hourly rows.

The functions P3's close calls for one site-day (plan 7.5, 7.7, 12.1), and
the read P5's provisional line shares with it (12.2):

- ``gap_plan`` (read-only): each listed station's gap hours, no-pair hours
  and the hours the gap fill should ask for.
- ``fill_gaps`` (async): at most one hourly download per station that has
  hours to fill, stored as ``hourly`` raw readings.
- ``hourly_values`` (read-only): the method of ``obs/hourly_values.py`` over
  the raw table, wind in m/s.
- ``write_hourly_values`` (in the caller's transaction): the day's station
  rows, exactly the method's, and their consensus hours.
- ``day_presence`` (read-only): what each station delivered all day, for
  P3's nightly gap-fill pause.

The day's hours, bounds and station list are P1's record for the site-day.
None of these functions enqueues a job; writers read the clock only through
their ``now_utc`` argument.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from typing import Final, Literal

import httpx

from wxverify.collection.budget import (
    Reservation,
    is_refundable_transport_error,
    refund_budget,
    reserve_budget,
    write_after_reservation,
)
from wxverify.core.secrets import resolve_secret
from wxverify.core.timeutil import floor_hour, isoformat_utc, parse_utc
from wxverify.core.units import kmh_to_ms
from wxverify.db.connection import Database, FencedWriter
from wxverify.db.site_days import (
    DayRecord,
    WriteKind,
    check_date_write,
    day_record,
    day_station_ids,
)
from wxverify.db.station_readings import StoreResult, store_readings
from wxverify.db.wind_basis import PAIR_MAX_SOURCE_PREFIX
from wxverify.obs.hourly_values import (
    QUANTITIES,
    HourValue,
    Quantity,
    Source,
    StationHours,
    StationReading,
    station_hours,
)
from wxverify.obs.pws_adapter import (
    UpstreamPayloadError,
    fetch_hourly_history_payload,
    is_hourly_history_no_content,
    readings_from_payload,
)
from wxverify.scoring.consensus import materialize_consensus, write_station_observation
from wxverify.worker.access_hold import (
    AccessHeld,
    check_access_hold,
    clear_access_refused,
    record_access_refused,
)
from wxverify.worker.control import JobDeferred
from wxverify.worker.domain_backoff import (
    check_domain_backoff,
    record_http_backoff,
    source_domain,
)
from wxverify.worker.station_pacing import pace_station_call, weathercom_call_lock

logger = logging.getLogger(__name__)

#: The outcomes of one station's gap fill (plan 4.5, 12.1).
FillOutcome = Literal[
    "nothing_to_fill",
    "filled",
    "no_readings",
    "held",
    "refused",
    "failed",
    "not_on_day",
    "skipped",
]

_SOURCES: Final[dict[str, Source]] = {
    "five_minute": "five_minute",
    "current_obs": "current_obs",
    "hourly": "hourly",
}
#: The raw readings read before the day's start: the hour whose source order
#: picks the wind lookback readings (plan 8.1).
_LOOKBACK: Final = timedelta(hours=1)


# --- results ---------------------------------------------------------------


@dataclass(frozen=True)
class GapPlan:
    """One station's gap check for a site-day (plan 7.5).

    ``gap_hours`` have no ``five_minute`` and no ``current_obs`` reading;
    ``no_pair_hours`` have readings but no wind pair; ``to_fill`` are the gap
    hours with no ``hourly`` reading yet and the no-pair hours with no
    ``hourly`` wind yet. Each is in time order.
    """

    station_id: int
    gap_hours: tuple[datetime, ...]
    no_pair_hours: tuple[datetime, ...]
    to_fill: tuple[datetime, ...]


@dataclass(frozen=True)
class StationFill:
    """One station's gap-fill result (plan 4.5, 12.1).

    ``hours_filled`` holds, per quantity, the ``to_fill`` hours this call
    stored an hourly reading of that quantity for; ``hours_left`` the hours
    still to fill after it. ``next_allowed_at`` is when a held or deferred
    try may call again, None when no hold or back-off is known.
    ``readings_kept`` and ``readings_refused`` are the store's own counts for
    this call's readings, 0 when nothing was stored (plan 11.2).
    """

    outcome: FillOutcome
    reason: str | None
    detail: str | None
    hours_filled: dict[Quantity, tuple[datetime, ...]]
    hours_left: tuple[datetime, ...]
    next_allowed_at: datetime | None
    readings_kept: int
    readings_refused: int


@dataclass(frozen=True)
class GapFillResult:
    """``fill_gaps``' result: False with no station before the day's end."""

    ended: bool
    stations: dict[int, StationFill]


@dataclass(frozen=True)
class StationDayValues:
    """One station's hourly values for a site-day, wind in m/s (plan 12.1).

    ``latest_reading_at`` is the latest raw reading inside ``hours``, of any
    source, None when there is none.
    """

    hours: tuple[datetime, ...]
    values: dict[Quantity, dict[datetime, HourValue]]
    gap_hours: tuple[datetime, ...]
    latest_reading_at: datetime | None


@dataclass(frozen=True)
class SiteDayValues:
    """``hourly_values``' result, one entry per station on the day's list."""

    stations: dict[int, StationDayValues]


@dataclass(frozen=True)
class WriteResult:
    """``write_hourly_values``' result (plan 7.7).

    ``rows_written`` counts station rows added or changed, ``rows_removed``
    the station rows removed at hours without a value, and
    ``hours_materialized`` the (variable, hour) pairs re-materialized.
    """

    outcome: Literal["written", "refused"]
    rows_written: int
    rows_removed: int
    hours_materialized: int


@dataclass(frozen=True)
class StationPresence:
    """What one station delivered on a site-day (plan 12.1).

    ``readings`` counts the raw readings inside the day per source, every
    source a key; ``answered`` is whether any 5-minute try of the day was
    answered (the day row's ``last_ok_at``); ``refused_at`` is the day row's
    ``refused_at``.
    """

    readings: dict[Source, int]
    answered: bool
    refused_at: datetime | None


# --- reading the raw table -------------------------------------------------


def _value(raw: object) -> float | None:
    """A stored value as a float; ``ValueError`` when it is not a number."""
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        raise ValueError("not a number")
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError("not finite")
    return value


def _reading_from_row(row: sqlite3.Row) -> StationReading:
    """One raw row as a reading; ``ValueError`` when a field is unreadable."""
    raw_at: object = row[0]
    raw_source: object = row[1]
    raw_date: object = row[2]
    if not isinstance(raw_at, str) or not isinstance(raw_source, str):
        raise ValueError("unreadable stamp or source")
    source = _SOURCES.get(raw_source)
    if source is None:
        raise ValueError("unknown source")
    if raw_date is None:
        local_date = None
    elif isinstance(raw_date, str):
        local_date = date.fromisoformat(raw_date)
    else:
        raise ValueError("unreadable local date")
    return StationReading(
        obs_at=parse_utc(raw_at),
        source=source,
        local_date=local_date,
        temp_c=_value(row[3]),
        precip_total_mm=_value(row[4]),
        speed_kmh=_value(row[5]),
    )


def _station_readings(
    conn: sqlite3.Connection, station_id: int, low: datetime, high: datetime
) -> list[StationReading]:
    """The station's raw readings with ``low <= obs_at < high``, in time order.

    A row with an unreadable field is left out; the call logs them once at
    WARNING.
    """
    rows = conn.execute(
        "SELECT obs_at, source, local_date, temp_c, precip_total_mm, speed_kmh"
        " FROM station_wind_records"
        " WHERE station_id = ? AND obs_at >= ? AND obs_at < ?"
        " ORDER BY obs_at, source",
        (station_id, isoformat_utc(low), isoformat_utc(high)),
    ).fetchall()
    readings: list[StationReading] = []
    unreadable = 0
    for row in rows:
        try:
            reading = _reading_from_row(row)
        except ValueError:
            unreadable += 1
            continue
        if low <= reading.obs_at < high:
            readings.append(reading)
    if unreadable:
        logger.warning(
            "unreadable raw readings skipped station=%s count=%d",
            station_id,
            unreadable,
        )
    return readings


def _station_day(
    conn: sqlite3.Connection, record: DayRecord, station_id: int
) -> tuple[StationHours, list[StationReading]]:
    """The station's hourly values for the record's day, and the readings.

    The readings run from an hour before the day's start, for the wind
    lookback (plan 8.1), to the day's end.
    """
    readings = _station_readings(
        conn,
        station_id,
        record.day_start_utc - _LOOKBACK,
        record.day_end_utc,
    )
    hours = station_hours(
        readings,
        day_start_utc=record.day_start_utc,
        day_end_utc=record.day_end_utc,
        local_date=record.local_date,
        station_id=station_id,
    )
    return hours, readings


def _to_fill(
    hours: StationHours, readings: list[StationReading]
) -> tuple[datetime, ...]:
    """The gap hours with no hourly reading and the no-pair hours with no
    hourly wind (plan 7.5), in time order."""
    hourly = {floor_hour(r.obs_at) for r in readings if r.source == "hourly"}
    hourly_wind = {
        floor_hour(r.obs_at)
        for r in readings
        if r.source == "hourly" and r.speed_kmh is not None
    }
    wanted = {hour for hour in hours.gap_hours if hour not in hourly}
    wanted.update(hour for hour in hours.no_pair_hours if hour not in hourly_wind)
    return tuple(hour for hour in hours.hours if hour in wanted)


# --- the gap check ---------------------------------------------------------


def gap_plan(
    conn: sqlite3.Connection, *, site_id: int, local_date: date
) -> dict[int, GapPlan]:
    """Each listed station's gap check for the site-day (plan 7.5, 12.1).

    One entry per station on P1's day station list; empty when the site-day
    has no record. Read-only.
    """
    record = day_record(conn, site_id, local_date)
    if record is None:
        return {}
    plans: dict[int, GapPlan] = {}
    for station_id in sorted(day_station_ids(conn, site_id, local_date)):
        hours, readings = _station_day(conn, record, station_id)
        plans[station_id] = GapPlan(
            station_id=station_id,
            gap_hours=hours.gap_hours,
            no_pair_hours=hours.no_pair_hours,
            to_fill=_to_fill(hours, readings),
        )
    return plans


# --- the gap fill ----------------------------------------------------------


@dataclass(frozen=True)
class _Declined:
    """A reservation that sent nothing, and the station's outcome for it."""

    outcome: FillOutcome
    reason: str | None
    detail: str | None
    next_allowed_at: datetime | None = None


@dataclass(frozen=True)
class _Reserved:
    reservation: Reservation
    pws_station_id: str


def _stamp_or_none(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return parse_utc(value)
    except ValueError:
        return None


def _no_hours() -> dict[Quantity, tuple[datetime, ...]]:
    return {quantity: () for quantity in QUANTITIES}


def _day_refusal(
    conn: sqlite3.Connection, site_id: int, local_date: date, now_utc: datetime
) -> str | None:
    """None when P1's guard takes a gap-fill write to the day, else why not."""
    if check_date_write(conn, site_id, local_date, WriteKind.GAP_FILL, now_utc=now_utc):
        return None
    record = day_record(conn, site_id, local_date)
    return "no day" if record is None else f"day {record.state}"


def _reserve_fill_call(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    station_id: int,
    now_utc: datetime,
) -> _Reserved | _Declined:
    """Check and reserve one hourly call for a station, in one write (7.5).

    In order: P1's guard for a gap-fill write to the day, the station's place
    on the day's list, the domain back-off, the ``hourly`` hold and the
    budget. A refusal returns before the budget, so it spends nothing.
    """
    refusal = _day_refusal(conn, site_id, local_date, now_utc)
    if refusal is not None:
        return _Declined("refused", "day_guard", refusal)
    if station_id not in day_station_ids(conn, site_id, local_date):
        return _Declined("not_on_day", None, None)
    station = conn.execute(
        "SELECT pws_station_id FROM stations WHERE id = ? AND site_id = ?",
        (station_id, site_id),
    ).fetchone()
    if station is None:
        return _Declined("not_on_day", None, "no_station")
    try:
        check_domain_backoff(conn, source_domain("weathercom"))
    except JobDeferred as exc:
        return _Declined(
            "failed", "backoff", "backoff", _stamp_or_none(exc.next_attempt_at)
        )
    try:
        check_access_hold(conn, "hourly")
    except AccessHeld as held:
        return _Declined(
            "held", "refused_access", held.until, _stamp_or_none(held.until)
        )
    try:
        reservation = reserve_budget(conn, "weathercom", 1)
    except JobDeferred as exc:
        return _Declined(
            "failed", "call_limit", "call_limit", _stamp_or_none(exc.next_attempt_at)
        )
    return _Reserved(reservation=reservation, pws_station_id=str(station[0]))


def _stored_detail(result: StoreResult) -> str:
    """The detail of a try that stored readings (plan 4.5)."""
    per = result.kept_by_quantity
    return (
        f"temperature={per['temperature']} precip={per['precip']}"
        f" wind={per['wind']} already_stored={result.already_stored}"
        f" refused={result.refused}"
    )


def _hours_with(
    readings: tuple[StationReading, ...],
) -> dict[Quantity, tuple[datetime, ...]]:
    """Per quantity, the hours of ``readings`` that carry it, in time order."""
    carried: dict[Quantity, set[datetime]] = {q: set() for q in QUANTITIES}
    for reading in readings:
        hour = floor_hour(reading.obs_at)
        if reading.temp_c is not None:
            carried["temperature"].add(hour)
        if reading.precip_total_mm is not None:
            carried["precip"].add(hour)
        if reading.speed_kmh is not None:
            carried["wind"].add(hour)
    return {q: tuple(sorted(carried[q])) for q in QUANTITIES}


class _Fill:
    """The per-station calls of one ``fill_gaps`` run."""

    def __init__(
        self,
        db: Database,
        writer: FencedWriter,
        *,
        site_id: int,
        record: DayRecord,
        now_utc: datetime,
    ) -> None:
        self.db = db
        self.writer = writer
        self.site_id = site_id
        self.record = record
        self.now_utc = now_utc
        self.calls = 0
        self._key: str | None = None
        self._client: httpx.AsyncClient | None = None

    def key(self) -> str:
        if self._key is None:
            resolved = resolve_secret("weathercom")
            if not resolved:
                raise RuntimeError("weathercom key is not configured")
            self._key = resolved
        return self._key

    def client(self) -> httpx.AsyncClient:
        """The run's client, opened at its first call; ``aclose`` closes it."""
        if self._client is None:
            self._client = httpx.AsyncClient()
        return self._client

    async def aclose(self) -> None:
        """Close the run's client, if a call opened one."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def station(self, plan: GapPlan) -> StationFill:
        """Reserve, call and store one station's hourly download (7.5)."""
        key = self.key()
        await pace_station_call(self.site_id, plan.station_id, self.calls)
        record = self.record
        site_id = self.site_id
        now_utc = self.now_utc
        left = plan.to_fill
        async with weathercom_call_lock():
            decision = await self.writer.write(
                lambda conn: _reserve_fill_call(
                    conn,
                    site_id=site_id,
                    local_date=record.local_date,
                    station_id=plan.station_id,
                    now_utc=now_utc,
                )
            )
            if isinstance(decision, _Declined):
                return StationFill(
                    outcome=decision.outcome,
                    reason=decision.reason,
                    detail=decision.detail,
                    hours_filled=_no_hours(),
                    hours_left=left,
                    next_allowed_at=decision.next_allowed_at,
                    readings_kept=0,
                    readings_refused=0,
                )
            reservation = decision.reservation
            self.calls += 1
            client = self.client()
            try:
                payload = await fetch_hourly_history_payload(
                    decision.pws_station_id, key, client=client
                )
            except httpx.HTTPStatusError as exc:
                return await self._http_failed(exc.response, reservation, left)
            except UpstreamPayloadError as exc:
                if is_hourly_history_no_content(exc):
                    await write_after_reservation(
                        self.db,
                        self.writer,
                        lambda conn: clear_access_refused(conn, "hourly"),
                        reservation,
                    )
                    return StationFill(
                        outcome="no_readings",
                        reason=None,
                        detail="no_content",
                        hours_filled=_no_hours(),
                        hours_left=left,
                        next_allowed_at=None,
                        readings_kept=0,
                        readings_refused=0,
                    )
                return StationFill(
                    outcome="failed",
                    reason="provider",
                    detail=str(exc.diagnostics.kind),
                    hours_filled=_no_hours(),
                    hours_left=left,
                    next_allowed_at=None,
                    readings_kept=0,
                    readings_refused=0,
                )
            except (httpx.TransportError, TimeoutError) as exc:
                if is_refundable_transport_error(exc):
                    await write_after_reservation(
                        self.db,
                        self.writer,
                        lambda conn: refund_budget(conn, reservation),
                        reservation,
                    )
                return StationFill(
                    outcome="failed",
                    reason="network",
                    detail="network",
                    hours_filled=_no_hours(),
                    hours_left=left,
                    next_allowed_at=None,
                    readings_kept=0,
                    readings_refused=0,
                )
            return await self._store(plan, payload, reservation)

    async def _http_failed(
        self,
        response: httpx.Response,
        reservation: Reservation,
        left: tuple[datetime, ...],
    ) -> StationFill:
        """A non-2xx reply: 401/403 hold ``hourly``, 429/5xx back off (4.5)."""
        status = response.status_code
        detail = f"http {status}"
        if status in (401, 403):
            until = await write_after_reservation(
                self.db,
                self.writer,
                lambda conn: record_access_refused(conn, "hourly", status),
                reservation,
            )
            return StationFill(
                outcome="refused",
                reason="refused_access",
                detail=detail,
                hours_filled=_no_hours(),
                hours_left=left,
                next_allowed_at=_stamp_or_none(until),
                readings_kept=0,
                readings_refused=0,
            )
        wake: str | None = None
        if status == 429 or status >= 500:
            wake = await write_after_reservation(
                self.db,
                self.writer,
                lambda conn: record_http_backoff(conn, response),
                reservation,
            )
        return StationFill(
            outcome="failed",
            reason="provider",
            detail=detail,
            hours_filled=_no_hours(),
            hours_left=left,
            next_allowed_at=_stamp_or_none(wake),
            readings_kept=0,
            readings_refused=0,
        )

    async def _store(
        self,
        plan: GapPlan,
        payload: dict[str, object],
        reservation: Reservation,
    ) -> StationFill:
        """Store a 2xx reply's readings of the hours to fill (7.5, 6.4).

        One write clears the ``hourly`` hold, stores the readings whose hour
        is in ``to_fill`` and inside the day, and recomputes the station's
        hours left to fill.
        """
        record = self.record
        now_utc = self.now_utc
        station_id = plan.station_id
        parsed = readings_from_payload(payload, source="hourly")
        needed = frozenset(plan.to_fill)
        kept = tuple(
            reading
            for reading in parsed.readings
            if floor_hour(reading.obs_at) in needed
            and record.day_start_utc <= reading.obs_at < record.day_end_utc
        )

        def persist(
            conn: sqlite3.Connection,
        ) -> tuple[StoreResult, tuple[datetime, ...]]:
            clear_access_refused(conn, "hourly")
            stored = store_readings(
                conn,
                station_id=station_id,
                source="hourly",
                readings=kept,
                fetched_at=now_utc,
                now_utc=now_utc,
            )
            hours, readings = _station_day(conn, record, station_id)
            return stored, _to_fill(hours, readings)

        stored, left = await write_after_reservation(
            self.db, self.writer, persist, reservation
        )
        outcome: FillOutcome = "no_readings"
        reason: str | None = None
        detail = _stored_detail(stored)
        if not parsed.readings:
            detail = "empty"
        elif not kept:
            detail = "no_needed_hours"
        elif stored.refused:
            outcome, reason = "refused", "day_guard"
        elif stored.kept + stored.already_stored > 0:
            outcome = "filled"
        return StationFill(
            outcome=outcome,
            reason=reason,
            detail=detail,
            hours_filled=_hours_with(kept) if outcome == "filled" else _no_hours(),
            hours_left=left,
            next_allowed_at=None,
            readings_kept=stored.kept,
            readings_refused=stored.refused,
        )


async def fill_gaps(
    db: Database,
    writer: FencedWriter,
    *,
    site_id: int,
    local_date: date,
    now_utc: datetime,
    skip_station_ids: frozenset[int] = frozenset(),
) -> GapFillResult:
    """Fill the site-day's gaps from the hourly download (plan 7.5, 12.1).

    Before the day's end, or with no record for the site-day, returns
    ``ended`` False and calls nothing. Otherwise the gap check is recomputed,
    so a station already filled is not called again. When P1's guard takes
    no gap-fill write to the day, every listed station is ``refused``. Else,
    in station order: a station in ``skip_station_ids`` is ``skipped`` with
    no call, no reservation and no write; one with nothing to fill is
    ``nothing_to_fill``; any other gets at most one call, paced, under the
    shared call lock, after a reservation write that checks the guard, the
    day's list, the domain back-off, the ``hourly`` hold and the budget.
    Never raises for a provider outcome; a database error or a bug
    propagates.
    """
    record = await db.read(lambda conn: day_record(conn, site_id, local_date))
    if record is None or now_utc < record.day_end_utc:
        return GapFillResult(ended=False, stations={})
    refusal = await writer.write(
        lambda conn: _day_refusal(conn, site_id, local_date, now_utc)
    )
    plans = await db.read(
        lambda conn: gap_plan(conn, site_id=site_id, local_date=local_date)
    )
    stations: dict[int, StationFill] = {}
    run = _Fill(db, writer, site_id=site_id, record=record, now_utc=now_utc)
    try:
        for station_id in sorted(plans):
            plan = plans[station_id]
            if station_id in skip_station_ids:
                fill = StationFill(
                    outcome="skipped",
                    reason=None,
                    detail=None,
                    hours_filled=_no_hours(),
                    hours_left=plan.to_fill,
                    next_allowed_at=None,
                    readings_kept=0,
                    readings_refused=0,
                )
            elif refusal is not None:
                fill = StationFill(
                    outcome="refused",
                    reason="day_guard",
                    detail=refusal,
                    hours_filled=_no_hours(),
                    hours_left=plan.to_fill,
                    next_allowed_at=None,
                    readings_kept=0,
                    readings_refused=0,
                )
            elif not plan.to_fill:
                fill = StationFill(
                    outcome="nothing_to_fill",
                    reason=None,
                    detail=None,
                    hours_filled=_no_hours(),
                    hours_left=(),
                    next_allowed_at=None,
                    readings_kept=0,
                    readings_refused=0,
                )
            else:
                fill = await run.station(plan)
            stations[station_id] = fill
            logger.debug(
                "gap fill site=%s station=%s date=%s outcome=%s reason=%s"
                " detail=%s to_fill=%d left=%d",
                site_id,
                station_id,
                local_date.isoformat(),
                fill.outcome,
                fill.reason or "-",
                fill.detail or "-",
                len(plan.to_fill),
                len(fill.hours_left),
            )
    finally:
        await run.aclose()
    return GapFillResult(ended=True, stations=stations)


# --- the hourly values -----------------------------------------------------


def hourly_values(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    through_utc: datetime | None = None,
) -> SiteDayValues:
    """The site-day's hourly values per listed station (plan 7.1-7.4, 12.1).

    The method of ``obs/hourly_values.py`` over the raw table, with wind in
    m/s. With ``through_utc`` only the hours that start before it are
    returned; with None every hour of the day. Empty when the site-day has
    no record. Read-only.
    """
    record = day_record(conn, site_id, local_date)
    if record is None:
        return SiteDayValues(stations={})
    stations: dict[int, StationDayValues] = {}
    for station_id in sorted(day_station_ids(conn, site_id, local_date)):
        computed, readings = _station_day(conn, record, station_id)
        hours = tuple(
            hour for hour in computed.hours if through_utc is None or hour < through_utc
        )
        wanted = frozenset(hours)
        values: dict[Quantity, dict[datetime, HourValue]] = {}
        for quantity in QUANTITIES:
            values[quantity] = {
                hour: (
                    replace(value, value=kmh_to_ms(value.value))
                    if quantity == "wind"
                    else value
                )
                for hour, value in computed.values[quantity].items()
                if hour in wanted
            }
        stations[station_id] = StationDayValues(
            hours=hours,
            values=values,
            gap_hours=tuple(hour for hour in computed.gap_hours if hour in wanted),
            latest_reading_at=max(
                (r.obs_at for r in readings if floor_hour(r.obs_at) in wanted),
                default=None,
            ),
        )
    return SiteDayValues(stations=stations)


def _rounded(value: float) -> str:
    """``value`` to 2 decimals as ``source_raw`` shows it (plan 7.7)."""
    return str(round(value, 2) + 0.0)


def _source_raw(quantity: Quantity, value: HourValue) -> str:
    """The row's ``source_raw``: the method, the value and its source (7.7)."""
    text = _rounded(value.value)
    if quantity == "temperature":
        return f"hour-mean {text} C n={value.readings} src={value.source}"
    if quantity == "precip":
        return f"hour-rise {text} mm src={value.source}"
    if value.source == "hourly":
        return f"hourly-avg {text} km/h src=hourly"
    return f"{PAIR_MAX_SOURCE_PREFIX}{text} km/h src={value.source}"


def _canonical(valid_at: str) -> str:
    """``valid_at`` as a canonical stamp, unchanged when it does not parse."""
    try:
        return isoformat_utc(parse_utc(valid_at))
    except ValueError:
        return valid_at


def write_hourly_values(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    now_utc: datetime,
) -> WriteResult:
    """Write the site-day's station rows from its raw readings (plan 7.7).

    Returns ``refused`` and writes nothing when P1's guard takes no hourly
    write to the day, or the site-day has no record. Otherwise, per listed
    station: the station's rows of the day (temperature, rain and wind) at
    hours where the method has no value are removed first, then each value
    is written with ``write_station_observation`` in time order per
    quantity, so the jump check sees the hour before as written in this
    run. Each variable and hour where a row was added, changed or removed
    is then re-materialized once. Enqueues nothing. Runs in the caller's
    transaction: no BEGIN, no commit.
    """
    if not check_date_write(
        conn, site_id, local_date, WriteKind.HOURLY, now_utc=now_utc
    ):
        return WriteResult(
            outcome="refused", rows_written=0, rows_removed=0, hours_materialized=0
        )
    record = day_record(conn, site_id, local_date)
    if record is None:
        return WriteResult(
            outcome="refused", rows_written=0, rows_removed=0, hours_materialized=0
        )
    start = isoformat_utc(record.day_start_utc)
    end = isoformat_utc(record.day_end_utc)
    # Text bounds on ``valid_at`` so the stale-row read can range-scan the
    # index on (station_id, variable, valid_at); the julianday conditions
    # stay the exact check. Dates only, widened by one day each side: a
    # stored stamp's own date is at most a day from its UTC date (an offset
    # such as -05:00), and any stamp that starts with a date inside the
    # window sorts between the bounds whatever follows the date (a space for
    # the T, +00:00, fractional seconds).
    low_date = isoformat_utc(record.day_start_utc - timedelta(days=1))[:10]
    high_date = isoformat_utc(record.day_end_utc + timedelta(days=2))[:10]
    rows_written = 0
    rows_removed = 0
    changed: set[tuple[str, str]] = set()
    for station_id in sorted(day_station_ids(conn, site_id, local_date)):
        computed, _readings = _station_day(conn, record, station_id)
        wanted: dict[tuple[Quantity, str], tuple[float, str]] = {}
        for quantity in QUANTITIES:
            by_hour = computed.values[quantity]
            for hour in sorted(by_hour):
                value = by_hour[hour]
                stored = kmh_to_ms(value.value) if quantity == "wind" else value.value
                wanted[(quantity, isoformat_utc(hour))] = (
                    stored,
                    _source_raw(quantity, value),
                )
        existing = conn.execute(
            "SELECT variable, valid_at FROM station_observations"
            " WHERE station_id = ?"
            " AND variable IN ('temperature', 'precip', 'wind')"
            " AND valid_at >= ? AND valid_at < ?"
            " AND julianday(valid_at) >= julianday(?)"
            " AND julianday(valid_at) < julianday(?)",
            (station_id, low_date, high_date, start, end),
        ).fetchall()
        stale = sorted(
            (str(row[0]), str(row[1]))
            for row in existing
            if (str(row[0]), str(row[1])) not in wanted
        )
        for variable, valid_at in stale:
            conn.execute(
                "DELETE FROM station_observations"
                " WHERE station_id = ? AND variable = ? AND valid_at = ?",
                (station_id, variable, valid_at),
            )
            rows_removed += 1
            changed.add((variable, _canonical(valid_at)))
        for (quantity, valid_at), (stored, source_raw) in wanted.items():
            if write_station_observation(
                conn,
                station_id=station_id,
                variable=quantity,
                valid_at=valid_at,
                value=stored,
                source_raw=source_raw,
                now_utc=now_utc,
            ):
                rows_written += 1
                changed.add((quantity, valid_at))
    for variable, valid_at in sorted(changed):
        materialize_consensus(
            conn, site_id=site_id, variable=variable, valid_at=valid_at, now_utc=now_utc
        )
    logger.debug(
        "hourly values written site=%s date=%s written=%d removed=%d materialized=%d",
        site_id,
        local_date.isoformat(),
        rows_written,
        rows_removed,
        len(changed),
    )
    return WriteResult(
        outcome="written",
        rows_written=rows_written,
        rows_removed=rows_removed,
        hours_materialized=len(changed),
    )


# --- presence --------------------------------------------------------------


def day_presence(
    conn: sqlite3.Connection, *, site_id: int, local_date: date
) -> dict[int, StationPresence]:
    """What each listed station delivered on the site-day (plan 12.1).

    One entry per station on P1's day station list: the raw readings inside
    the day per source (0 for every source once retention has removed
    them), whether a 5-minute try of the day was answered (the day row's
    ``last_ok_at`` is set) and the day row's ``refused_at``. Read-only.
    """
    record = day_record(conn, site_id, local_date)
    presence: dict[int, StationPresence] = {}
    for station_id in sorted(day_station_ids(conn, site_id, local_date)):
        counts: dict[Source, int] = {"five_minute": 0, "current_obs": 0, "hourly": 0}
        if record is not None:
            rows = conn.execute(
                "SELECT source, COUNT(*) FROM station_wind_records"
                " WHERE station_id = ? AND obs_at >= ? AND obs_at < ?"
                " GROUP BY source",
                (
                    station_id,
                    isoformat_utc(record.day_start_utc),
                    isoformat_utc(record.day_end_utc),
                ),
            ).fetchall()
            for row in rows:
                source = _SOURCES.get(str(row[0]))
                if source is not None:
                    counts[source] = int(row[1])
        day_row = conn.execute(
            "SELECT last_ok_at, refused_at FROM station_wind_days"
            " WHERE station_id = ? AND local_date = ?",
            (station_id, local_date.isoformat()),
        ).fetchone()
        answered = day_row is not None and day_row[0] is not None
        refused_raw: object = None if day_row is None else day_row[1]
        presence[station_id] = StationPresence(
            readings=counts,
            answered=answered,
            refused_at=(
                _stamp_or_none(refused_raw) if isinstance(refused_raw, str) else None
            ),
        )
    return presence
