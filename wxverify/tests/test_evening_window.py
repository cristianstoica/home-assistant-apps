"""Tests: the 5-minute download's schedule (EW1-EW15).

``EVENING_SLOTS`` and ``ReservationDeferred`` live in
``wxverify.worker.wind_days``; ``next_five_minute_try`` in
``wxverify.db.station_readings``; ``record_collection_try`` in
``wxverify.worker.close_chain``.

Isolation: a real tmp-path ``Database``/``FencedWriter`` + ``httpx.
MockTransport``, mirroring ``tests/test_wind_days_lane.py``'s ``_t97_*``
harness. The site comes from ``day_close_site`` (the daily close is
always on). Stations and zones are synthetic:
``ISTATION01``-``ISTATION06`` standing for A-F, zones ``UTC`` and
``Etc/GMT-14`` only; the API key is the CI placeholder.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from tests.helpers import D as _HELPERS_D
from tests.helpers import day_close_site, evening_slots_utc
from wxverify import config
from wxverify.core.timeutil import isoformat_utc, local_day_slots, parse_utc
from wxverify.db.connection import Database, FencedWriter, close_db, init_db
from wxverify.db.queue import claim_next_job
from wxverify.db.site_days import current_day, start_site_days
from wxverify.db.station_readings import next_five_minute_try
from wxverify.worker.close_chain import record_collection_try  # noqa: F401
from wxverify.worker.control import JobDeferred
from wxverify.worker.scheduler import _enqueue_due_wind_days  # noqa: PLC2701
from wxverify.worker.wind_days import (  # noqa: PLC2701 -- private, exercised directly
    EVENING_SLOTS,
    WIND_DAYS_JOB_KEY,
    _day_clock,
    run_wind_days,
)

_TZ = "UTC"
_TZ_14 = "Etc/GMT-14"
_ZONES = (_TZ, _TZ_14)
_API_KEY = "0123456789abcdef0123456789abcdef"
_DAY = date(2026, 3, 10)


# ---------------------------------------------------------------------------
# Shared harness (mirrors tests/test_wind_days_lane.py's `_t97_*` helpers)
# ---------------------------------------------------------------------------


def _init_tmp_db(tmp_path: Path, name: str = "wxverify") -> Database:
    close_db()
    db_path = tmp_path / f"{name}.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / f"{name}-missing-options.json")
    return init_db(str(db_path))


def _freeze(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    monkeypatch.setattr("wxverify.worker.wind_days.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.domain_backoff.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.collection.budget.utc_now", lambda: when)


def _run(
    db: Database, site_id: int, handler: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)
    real_client = httpx.AsyncClient
    writer = FencedWriter(db)
    with patch(
        "wxverify.worker.wind_days.httpx.AsyncClient",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),  # type: ignore[arg-type]
    ):
        asyncio.run(run_wind_days(db, writer, site_id, client=None))


def _make_station(
    conn: sqlite3.Connection, site_id: int, pws_id: str, *, enabled: bool = True
) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
        " dem_elevation_m, enabled) VALUES (?, ?, 0.0, 0.0, 0.0, ?)",
        (site_id, pws_id, 1 if enabled else 0),
    )
    assert cur.lastrowid is not None
    conn.commit()
    return int(cur.lastrowid)


def _station_id(conn: sqlite3.Connection, site_id: int, pws_id: str) -> int:
    """The id of a station ``day_close_site`` already created for ``pws_id``
    (its stations are ``ISTATION01``/``ISTATION02`` by default, or
    ``station_ids`` when overridden) -- never a second insert of the same
    ``pws_station_id``, which ``stations.pws_station_id``'s UNIQUE
    constraint refuses."""
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? AND pws_station_id = ?",
        (site_id, pws_id),
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _disable_station(conn: sqlite3.Connection, site_id: int, pws_id: str) -> None:
    """Disable one of ``day_close_site``'s own stations, so a test that
    wants exactly one enabled station does not also get a today row (and an
    extra call) for the other."""
    conn.execute(
        "UPDATE stations SET enabled = 0 WHERE site_id = ? AND pws_station_id = ?",
        (site_id, pws_id),
    )
    conn.commit()


def _insert_day(
    conn: sqlite3.Connection,
    station_id: int,
    local_date: date,
    *,
    status: str = "partial",
    next_attempt_at: str | None = None,
) -> None:
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, record_count,"
        "  pair_hours, next_attempt_at, refetched, updated_at)"
        " VALUES (?, ?, ?, 0, 0, 0, ?, 0, '2026-01-01T00:00:00Z')",
        (station_id, local_date.isoformat(), status, next_attempt_at),
    )
    conn.commit()


def _day_row(
    conn: sqlite3.Connection, station_id: int, local_date: date
) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM station_wind_days WHERE station_id = ? AND local_date = ?",
        (station_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None
    return row


def _close_try_rows(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM close_try_log WHERE site_id = ? AND local_date = ? ORDER BY id",
        (site_id, local_date.isoformat()),
    ).fetchall()


def _empty_ok() -> httpx.Response:
    return httpx.Response(200, json={"observations": []})


def _give_today_record(conn: sqlite3.Connection, site_id: int, tz: str) -> None:
    """Give the site a day record holding ``_DAY`` (the close is always on;
    without a record holding it, the lane's guard refuses the row, per
    EW14), mirroring ``tests/test_access_hold_five_minute.py``."""
    start_of_day = local_day_slots(_DAY, tz)[0]
    start_site_days(conn, site_id, _DAY, now_utc=start_of_day)


# ---------------------------------------------------------------------------
# EW1-EW9 -- the live schedule, driven end to end through run_wind_days.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tz", _ZONES)
def test_ew1_daytime_cap_at_23_00(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """EW1: daytime tries at 05:10 and 21:40 with the default interval (180
    min): next at 08:10, then 23:00 -- not the old 23:25 final call."""
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    _give_today_record(conn, site_id, tz)
    _disable_station(conn, site_id, "ISTATION02")
    station_id = _station_id(conn, site_id, "ISTATION01")
    start, end, _tzname = local_day_slots(_DAY, tz)
    # Built from `start` by addition, not `.replace(hour=...)`: `start` is
    # the UTC instant of LOCAL midnight, whose own UTC hour is not 0 outside
    # UTC (e.g. Etc/GMT-14's local midnight is the prior day's 10:00 UTC),
    # so `.replace(hour=5)` would silently give the wrong instant there.
    morning = start + timedelta(hours=5, minutes=10)
    evening = start + timedelta(hours=21, minutes=40)
    _insert_day(conn, station_id, _DAY, next_attempt_at=isoformat_utc(morning))

    _freeze(monkeypatch, morning)
    _run(db, site_id, lambda req: _empty_ok(), monkeypatch=monkeypatch)
    row = _day_row(conn, station_id, _DAY)
    assert row["next_attempt_at"] == isoformat_utc(morning + timedelta(minutes=180)), (
        "mutant -> at this assertion: correct ="
        f" {isoformat_utc(morning + timedelta(minutes=180))} (05:10 + 180 min"
        " = 08:10, well short of the cap), mutant (the cap applied too"
        f" early) = {row['next_attempt_at']!r}"
    )

    conn.execute(
        "UPDATE station_wind_days SET next_attempt_at = ?"
        " WHERE station_id = ? AND local_date = ?",
        (isoformat_utc(evening), station_id, _DAY.isoformat()),
    )
    conn.commit()
    _freeze(monkeypatch, evening)
    _run(db, site_id, lambda req: _empty_ok(), monkeypatch=monkeypatch)
    row = _day_row(conn, station_id, _DAY)
    assert row["next_attempt_at"] == isoformat_utc(end - timedelta(hours=1)), (
        "mutant -> at this assertion: correct = 23:00 (21:40 + 180 min ="
        " 00:40, capped at the daytime cap), mutant (the old 23:25 cap, or"
        " no cap at all, M33) ="
        f" {row['next_attempt_at']!r}"
    )
    close_db()


@pytest.mark.parametrize("tz", _ZONES)
def test_ew2_no_retries_after_a_success_before_23_56(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """EW2: 23:00 succeeds -> next 23:56; 23:00 fails, 23:06 succeeds ->
    next 23:56 too; no try runs between a success and 23:56."""
    for responses in ([200], [404, 200]):
        db = _init_tmp_db(tmp_path, name=f"db-{len(responses)}")
        conn = db._conn  # noqa: SLF001
        site_id = day_close_site(conn, timezone=tz, with_records=False)
        _give_today_record(conn, site_id, tz)
        _disable_station(conn, site_id, "ISTATION02")
        station_id = _station_id(conn, site_id, "ISTATION01")
        start, end, _ = local_day_slots(_DAY, tz)
        cap = end - timedelta(hours=1)
        _insert_day(conn, station_id, _DAY, next_attempt_at=isoformat_utc(cap))
        queue = list(responses)

        def handler(request: httpx.Request, queue: list[int] = queue) -> httpx.Response:
            status = queue.pop(0)
            if status == 200:
                return _empty_ok()
            return httpx.Response(status, request=request)

        now = cap
        for _ in responses:
            _freeze(monkeypatch, now)
            _run(db, site_id, handler, monkeypatch=monkeypatch)
            row = _day_row(conn, station_id, _DAY)
            now = parse_utc(str(row["next_attempt_at"]))
        assert now == end - timedelta(minutes=4), (
            "mutant -> at this assertion: correct = 23:56 (a success before"
            " 23:56 jumps straight there, M34), mutant (a further"
            f" retry slot scheduled before 23:56) = {now.isoformat()}"
        )
        assert queue == [], "fixture bug: every seeded response must be consumed"
        close_db()


@pytest.mark.parametrize("tz", _ZONES)
def test_ew3_every_try_fails_runs_all_15_slots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """EW3: every try from 23:00 fails with an error that sets no back-off
    (an HTTP 404): tries land at the 15 documented slots
    in order, then the row is done for the day with no 16th try."""
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    _give_today_record(conn, site_id, tz)
    _disable_station(conn, site_id, "ISTATION02")
    station_id = _station_id(conn, site_id, "ISTATION01")
    start, end, _ = local_day_slots(_DAY, tz)
    cap = end - timedelta(hours=1)
    _insert_day(conn, station_id, _DAY, next_attempt_at=isoformat_utc(cap))

    def failing(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, request=request)

    slots = evening_slots_utc(_DAY, tz)
    assert len(slots) == 15
    now = cap
    seen: list[datetime] = []
    for _ in range(15):
        _freeze(monkeypatch, now)
        _run(db, site_id, failing, monkeypatch=monkeypatch)
        seen.append(now)
        row = _day_row(conn, station_id, _DAY)
        next_at = row["next_attempt_at"]
        if next_at is None or parse_utc(str(next_at)) >= end:
            break
        now = parse_utc(str(next_at))
    assert seen == list(slots), (
        "mutant -> at this assertion: correct = the 15 documented slots in"
        " order (M35: a slot dropped or doubled would shorten/lengthen"
        f" this list), mutant = {[s.isoformat() for s in seen]}"
    )
    row = _day_row(conn, station_id, _DAY)
    assert row["next_attempt_at"] == isoformat_utc(end + timedelta(hours=1)), (
        "mutant -> at this assertion: correct = end + 1h (done for the day"
        " after the 23:59 try), mutant (a 16th try still scheduled) ="
        f" {row['next_attempt_at']!r}"
    )
    close_db()


@pytest.mark.parametrize("tz", _ZONES)
def test_ew4_23_56_always_runs_then_stops_on_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """EW4: 23:56 succeeds -> done after 23:56; 23:56 fails and 23:57
    succeeds -> done after 23:57 (M36: 23:57 must not run after a
    SUCCESSFUL 23:56)."""
    end = local_day_slots(_DAY, tz)[1]
    late_slot = end - timedelta(minutes=4)

    db = _init_tmp_db(tmp_path, name="a")
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    _give_today_record(conn, site_id, tz)
    _disable_station(conn, site_id, "ISTATION02")
    station_id = _station_id(conn, site_id, "ISTATION01")
    _insert_day(conn, station_id, _DAY, next_attempt_at=isoformat_utc(late_slot))
    calls: list[str] = []

    def succeeding(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return _empty_ok()

    _freeze(monkeypatch, late_slot)
    _run(db, site_id, succeeding, monkeypatch=monkeypatch)
    row = _day_row(conn, station_id, _DAY)
    assert row["next_attempt_at"] == isoformat_utc(end + timedelta(hours=1)), (
        "mutant -> at this assertion: correct = end + 1h (a success at"
        " 23:56 closes the day there), mutant (M36: 23:57 still scheduled"
        f" after a successful 23:56) = {row['next_attempt_at']!r}"
    )
    close_db()

    db2 = _init_tmp_db(tmp_path, name="b")
    conn2 = db2._conn  # noqa: SLF001
    site_id2 = day_close_site(conn2, timezone=tz, with_records=False)
    _give_today_record(conn2, site_id2, tz)
    _disable_station(conn2, site_id2, "ISTATION02")
    station_id2 = _station_id(conn2, site_id2, "ISTATION01")
    _insert_day(conn2, station_id2, _DAY, next_attempt_at=isoformat_utc(late_slot))
    responses = [404, 200]

    def handler(request: httpx.Request) -> httpx.Response:
        status = responses.pop(0)
        if status == 200:
            return _empty_ok()
        return httpx.Response(status, request=request)

    _freeze(monkeypatch, late_slot)
    _run(db2, site_id2, handler, monkeypatch=monkeypatch)
    row2 = _day_row(conn2, station_id2, _DAY)
    assert row2["next_attempt_at"] == isoformat_utc(late_slot + timedelta(minutes=1)), (
        "mutant -> at this assertion: correct = 23:57 (a failed 23:56 moves"
        " to the next slot), mutant = " + repr(row2["next_attempt_at"])
    )
    _freeze(monkeypatch, late_slot + timedelta(minutes=1))
    _run(db2, site_id2, handler, monkeypatch=monkeypatch)
    row2 = _day_row(conn2, station_id2, _DAY)
    assert row2["next_attempt_at"] == isoformat_utc(end + timedelta(hours=1)), (
        "mutant -> at this assertion: correct = end + 1h (a success at"
        " 23:57 closes the day there too), mutant (another retry"
        f" scheduled) = {row2['next_attempt_at']!r}"
    )
    assert responses == []
    close_db()


@pytest.mark.parametrize("tz", _ZONES)
def test_ew5_204_and_empty_list_are_both_successes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """EW5: a 204 for one station and an empty-``observations`` reply for
    another are both successes: both go to 23:56, not into the retry
    chain (M37)."""
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    _give_today_record(conn, site_id, tz)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    start, end, _ = local_day_slots(_DAY, tz)
    cap = end - timedelta(hours=1)
    _insert_day(conn, station_a, _DAY, next_attempt_at=isoformat_utc(cap))
    _insert_day(conn, station_b, _DAY, next_attempt_at=isoformat_utc(cap))

    def handler(request: httpx.Request) -> httpx.Response:
        if "stationId=ISTATION01" in str(request.url):
            return httpx.Response(204)
        return httpx.Response(200, json={"observations": []})

    _freeze(monkeypatch, cap)
    _run(db, site_id, handler, monkeypatch=monkeypatch)
    for station_id in (station_a, station_b):
        row = _day_row(conn, station_id, _DAY)
        assert row["next_attempt_at"] == isoformat_utc(end - timedelta(minutes=4)), (
            "mutant -> at this assertion: correct = 23:56 (both a 204 and"
            " an empty list are successes, M37), mutant (one or both read"
            f" as a failure, starting the retry chain at 23:06) ="
            f" {row['next_attempt_at']!r}"
        )
    close_db()


@pytest.mark.parametrize("tz", _ZONES)
def test_ew6_an_overrun_slot_is_skipped_not_run_late_or_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """EW6: a try for 23:11 starts at 23:23 and fails: next is 23:26 (the
    first slot after the ACTUAL try time); 23:16 and 23:21 never run
    (M38)."""
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    _give_today_record(conn, site_id, tz)
    _disable_station(conn, site_id, "ISTATION02")
    station_id = _station_id(conn, site_id, "ISTATION01")
    start, end, _ = local_day_slots(_DAY, tz)
    due_at_2311 = end - timedelta(minutes=49)
    actual_run_at = end - timedelta(minutes=37)  # 23:23
    _insert_day(conn, station_id, _DAY, next_attempt_at=isoformat_utc(due_at_2311))
    calls: list[str] = []

    def failing(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(404, request=request)

    _freeze(monkeypatch, actual_run_at)
    _run(db, site_id, failing, monkeypatch=monkeypatch)
    assert len(calls) == 1, "the overrun try still runs exactly once"
    row = _day_row(conn, station_id, _DAY)
    assert row["next_attempt_at"] == isoformat_utc(end - timedelta(minutes=34)), (
        "mutant -> at this assertion: correct = 23:26 (the first slot"
        " after the ACTUAL try time, 23:23, not after the due slot,"
        " 23:11), mutant (M38: next computed from the due slot, giving"
        f" 23:16) = {row['next_attempt_at']!r}"
    )
    close_db()


@pytest.mark.parametrize("tz", _ZONES)
def test_ew7_window_deferrals_and_a_transport_error_does_not_end_the_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """EW7: a transport error at 23:16 with a second station also due at
    23:16; a 429 at 23:31 with a back-off to 23:44; then a transport error
    at 23:56. The transport error moves the row and the run continues to
    the second station's row in the same chunk (M39, M77); the 429 moves
    the row to the first slot at or after the back-off's end, not 15
    minutes ahead (M39); the lane job is not left pending until 23:31."""
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    _give_today_record(conn, site_id, tz)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    start, end, _ = local_day_slots(_DAY, tz)
    slot_2316 = end - timedelta(minutes=44)
    _insert_day(conn, station_a, _DAY, next_attempt_at=isoformat_utc(slot_2316))
    _insert_day(conn, station_b, _DAY, next_attempt_at=isoformat_utc(slot_2316))

    calls: list[str] = []

    def transport_handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        raise httpx.ReadTimeout("boom", request=request)

    _freeze(monkeypatch, slot_2316)
    _run(db, site_id, transport_handler, monkeypatch=monkeypatch)  # must not raise
    assert len(calls) == 2, (
        "mutant -> at this assertion: correct = 2 (M77: a transport error"
        " on today's row in the window must not raise JobDeferred and end"
        " the run -- the second station's row is still tried in the same"
        f" chunk), mutant = {len(calls)}"
    )
    for station_id in (station_a, station_b):
        row = _day_row(conn, station_id, _DAY)
        assert row["next_attempt_at"] == isoformat_utc(end - timedelta(minutes=39)), (
            "mutant -> at this assertion: correct = 23:21 (the next slot"
            " after a transport error in the window, not the 15-minute"
            " defer), mutant (M39) = " + repr(row["next_attempt_at"])
        )
    pending = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type = 'fetch_obs'"
        " AND job_key = ? AND status = 'pending'",
        (WIND_DAYS_JOB_KEY,),
    ).fetchone()["n"]
    assert pending == 0, "no JobDeferred means no job left pending for the scheduler"

    slot_2331 = end - timedelta(minutes=29)
    conn.executemany(
        "UPDATE station_wind_days SET next_attempt_at = ?"
        " WHERE station_id = ? AND local_date = ?",
        [
            (isoformat_utc(slot_2331), station_a, _DAY.isoformat()),
            (isoformat_utc(slot_2331), station_b, _DAY.isoformat()),
        ],
    )
    conn.commit()

    def backoff_429(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"Retry-After": "780"},
            request=request,  # ends ~23:44
        )

    _freeze(monkeypatch, slot_2331)
    with pytest.raises(JobDeferred):
        _run(db, site_id, backoff_429, monkeypatch=monkeypatch)
    row = _day_row(conn, station_a, _DAY)
    assert row["next_attempt_at"] == isoformat_utc(end - timedelta(minutes=14)), (
        "mutant -> at this assertion: correct = 23:46 (the first evening"
        " slot at or after the back-off's end, ~23:44), mutant (M39: a"
        f" flat 15-minute defer instead) = {row['next_attempt_at']!r}"
    )

    slot_2356 = end - timedelta(minutes=4)
    conn.execute(
        "UPDATE station_wind_days SET next_attempt_at = ?"
        " WHERE station_id = ? AND local_date = ?",
        (isoformat_utc(slot_2356), station_a, _DAY.isoformat()),
    )
    conn.commit()
    transport_calls: list[str] = []

    def transport_again(request: httpx.Request) -> httpx.Response:
        transport_calls.append(str(request.url))
        raise httpx.ReadTimeout("boom", request=request)

    _freeze(monkeypatch, slot_2356)
    _run(db, site_id, transport_again, monkeypatch=monkeypatch)  # must still run 23:57
    row = _day_row(conn, station_a, _DAY)
    assert row["next_attempt_at"] == isoformat_utc(slot_2356 + timedelta(minutes=1)), (
        "mutant -> at this assertion: correct = 23:57 (the 23:57 try still"
        " applies after a failed 23:56), mutant (M77: the run ends instead"
        " of recording the window failure) = " + repr(row["next_attempt_at"])
    )
    close_db()


@pytest.mark.parametrize("tz", _ZONES)
def test_ew8_evening_slots_are_not_spaced_by_the_success_cooldown(
    monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """EW8: the lane completed at 23:56; a row due at 23:57: the
    scheduler's 5-minute success-spacing (``WIND_DAYS_SPACING``) must not
    block the enqueue (M40). The close is permanent; ``start_site_days``
    gives the site its ``_DAY`` record, so
    ``_lane_today`` finds today open."""
    import wxverify.db.queue as queue_module
    import wxverify.worker.scheduler as scheduler_module

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    from wxverify.db.migrations import run_migrations

    run_migrations(conn)
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, ?)",
        (tz,),
    )
    site_id = int(cur.lastrowid)  # type: ignore[arg-type]
    station_id = _make_station(conn, site_id, "ISTATION01")
    _, end, _ = local_day_slots(_DAY, tz)
    late_slot = end - timedelta(minutes=4)
    now = late_slot + timedelta(minutes=1)
    start_site_days(conn, site_id, _DAY, now_utc=now)
    _insert_day(conn, station_id, _DAY, next_attempt_at=isoformat_utc(late_slot))
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status, updated_at)"
        " VALUES ('fetch_obs', ?, ?, 'completed', ?)",
        (site_id, WIND_DAYS_JOB_KEY, isoformat_utc(late_slot)),
    )
    conn.commit()

    monkeypatch.setattr(scheduler_module, "utc_now", lambda: now)
    monkeypatch.setattr(queue_module, "utc_now", lambda: now)

    _enqueue_due_wind_days(conn)

    pending = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type = 'fetch_obs'"
        " AND job_key = ? AND status = 'pending'",
        (WIND_DAYS_JOB_KEY,),
    ).fetchone()["n"]
    assert pending == 1, (
        "mutant -> at this assertion: correct = 1 (M40: an evening-slot due"
        " row is enqueued even 1 minute after the lane's last success,"
        " inside the normal 5-minute cooldown), mutant (the success"
        f" cooldown still applies at evening slots) = {pending}"
    )


def test_ew9_the_lanes_job_claims_in_tier_0() -> None:
    """EW9: a pending wind-days job and an older pending tier-1 job: the
    wind-days job still claims first (M41 -- the claim tier left at 1)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    from wxverify.db.migrations import run_migrations

    run_migrations(conn)
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Testsite', 0.0, 0.0, 0.0, 'UTC')"
    )
    site_id = int(cur.lastrowid)  # type: ignore[arg-type]
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status, created_at,"
        " updated_at) VALUES ('pair_and_score', ?, NULL,"
        " 'pending', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        (site_id,),
    )
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status, created_at,"
        " updated_at) VALUES ('fetch_obs', ?, ?, 'pending',"
        " '2026-01-01T00:00:05Z', '2026-01-01T00:00:05Z')",
        (site_id, WIND_DAYS_JOB_KEY),
    )
    conn.commit()

    claimed = claim_next_job(conn)
    assert claimed is not None and claimed.job_key == WIND_DAYS_JOB_KEY, (
        "mutant -> at this assertion: correct = the wind-days job (claim"
        " tier 0 beats the older tier-1 job), mutant (M41: the claim"
        " tier left at 1, so the older pair_and_score job claims first) ="
        f" {claimed.job_key if claimed is not None else None!r}"
    )


# ---------------------------------------------------------------------------
# EW10 -- the pure slot list.
# ---------------------------------------------------------------------------


def test_ew10_the_slot_list_has_15_distinct_ordered_instants(tmp_path: Path) -> None:
    """EW10: the slot list for a day: 15 distinct instants in order, from
    local 23:00 to 23:59 (M35: a slot lost or doubled)."""
    db = _init_tmp_db(tmp_path, name="ew10")
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=_TZ)
    clock = _day_clock(conn, site_id, _HELPERS_D)
    assert clock is not None, "fixture bug: day_close_site must record _HELPERS_D"
    slots = clock.slots
    assert len(slots) == 15, (
        f"mutant -> at this assertion: correct = 15, mutant = {len(slots)}"
    )
    assert len(set(slots)) == 15, "mutant -> at this assertion: a duplicate slot"
    assert list(slots) == sorted(slots), (
        "mutant -> at this assertion: correct = already in order, mutant ="
        " a slot out of order"
    )
    start, end, _ = local_day_slots(_HELPERS_D, _TZ)
    assert slots[0] == end - timedelta(hours=1), (
        f"mutant -> at this assertion: correct = 23:00, mutant = {slots[0].isoformat()}"
    )
    assert slots[-1] == end - timedelta(minutes=1), (
        "mutant -> at this assertion: correct = 23:59, mutant ="
        f" {slots[-1].isoformat()}"
    )
    assert len(EVENING_SLOTS) == 15, (
        "mutant -> at this assertion: correct = 15 (hour, minute) pairs,"
        f" mutant = {len(EVENING_SLOTS)}"
    )
    assert slots == evening_slots_utc(_HELPERS_D, _TZ), (
        "mutant -> at this assertion: correct = the written-out slot"
        f" minutes, mutant = {[s.isoformat() for s in slots]}"
    )
    close_db()

    # Etc/GMT-14: the slots are LOCAL wall
    # clock, 14 hours ahead of UTC, not a UTC-hardcoded 23:00-23:59.
    db_14 = _init_tmp_db(tmp_path, name="ew10-14")
    conn_14 = db_14._conn  # noqa: SLF001
    site_id_14 = day_close_site(conn_14, timezone=_TZ_14)
    clock_14 = _day_clock(conn_14, site_id_14, _HELPERS_D)
    assert clock_14 is not None, "fixture bug: day_close_site must record _HELPERS_D"
    slots_14 = clock_14.slots
    start_14, end_14, _ = local_day_slots(_HELPERS_D, _TZ_14)
    assert slots_14[0] == end_14 - timedelta(hours=1), (
        "mutant -> at this assertion: correct = end_14 - 1h (the zone's own"
        " local 23:00), mutant (the slots computed on UTC regardless of"
        f" the site's own timezone) = {slots_14[0].isoformat()}"
    )
    assert slots_14[0] != slots[0], (
        "fixture bug: Etc/GMT-14 and UTC must give different UTC instants"
        " for the same local slot"
    )
    assert slots_14 == evening_slots_utc(_HELPERS_D, _TZ_14), (
        "mutant -> at this assertion: correct = the written-out slot"
        f" minutes for Etc/GMT-14, mutant = {[s.isoformat() for s in slots_14]}"
    )
    close_db()


# ---------------------------------------------------------------------------
# EW11 -- the try log and the INFO line.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tz", _ZONES)
def test_ew11_every_try_logs_once_and_never_twice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    tz: str,
) -> None:
    """EW11: a daytime try that stores readings; a try at 23:06 that gets a
    500, which now defers the job (``JobDeferred``) after its write; a try
    whose outcome write raises, so its transaction rolls back: one
    ``close_try_log`` row for each of the first two, the INFO line for
    each, and no row for the third (M72, M88)."""
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    _give_today_record(conn, site_id, tz)
    _disable_station(conn, site_id, "ISTATION02")
    station_id = _station_id(conn, site_id, "ISTATION01")
    start, end, _ = local_day_slots(_DAY, tz)
    # `start + timedelta(...)`, not `.replace(hour=...)` -- see EW1's note.
    morning = start + timedelta(hours=5, minutes=10)
    _insert_day(conn, station_id, _DAY, next_attempt_at=isoformat_utc(morning))

    def reading_ok(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "observations": [
                    {
                        "obsTimeUtc": isoformat_utc(morning),
                        "metric": {
                            "tempAvg": 5.0,
                            "precipTotal": 0.0,
                            "windspeedAvg": 3.0,
                        },
                    }
                ]
            },
        )

    with caplog.at_level(logging.INFO, logger="wxverify.worker.wind_days"):
        _freeze(monkeypatch, morning)
        _run(db, site_id, reading_ok, monkeypatch=monkeypatch)

    rows = _close_try_rows(conn, site_id, _DAY)
    assert len(rows) == 1 and rows[0]["slot"] == "daytime", (
        "mutant -> at this assertion: correct = one row, slot='daytime'"
        " (M72: record_collection_try not called), mutant ="
        f" {[dict(r) for r in rows]}"
    )
    assert rows[0]["outcome"] == "stored" and rows[0]["readings_kept"] == 1, (
        "mutant -> at this assertion: correct = outcome='stored',"
        " readings_kept=1 (a reply with readings is stored), mutant"
        " (a reply with readings read as no_readings/empty) ="
        f" outcome={rows[0]['outcome']!r} readings_kept={rows[0]['readings_kept']!r}"
    )
    assert any(
        rec.message.startswith(f"five-minute try site={site_id} station=")
        and "slot=daytime" in rec.message
        and "outcome=stored" in rec.message
        for rec in caplog.records
    ), "the documented INFO line for the daytime try is missing"

    slot_2306 = end - timedelta(minutes=54)
    conn.execute(
        "UPDATE station_wind_days SET next_attempt_at = ?"
        " WHERE station_id = ? AND local_date = ?",
        (isoformat_utc(slot_2306), station_id, _DAY.isoformat()),
    )
    conn.commit()
    with caplog.at_level(logging.INFO, logger="wxverify.worker.wind_days"):
        _freeze(monkeypatch, slot_2306)
        with pytest.raises(JobDeferred):
            _run(
                db,
                site_id,
                lambda req: httpx.Response(500, request=req),
                monkeypatch=monkeypatch,
            )
    rows = _close_try_rows(conn, site_id, _DAY)
    assert len(rows) == 2 and rows[1]["slot"] == "23:06", (
        "mutant -> at this assertion: correct = a second row, slot='23:06',"
        f" outcome='failed', mutant = {[dict(r) for r in rows]}"
    )
    assert any(
        rec.message.startswith(f"five-minute try site={site_id} station=")
        and "slot=23:06" in rec.message
        and "outcome=failed" in rec.message
        for rec in caplog.records
    ), (
        "mutant -> at this assertion: correct = the 23:06 try's INFO line"
        " present (M88: a 5xx in the window must still log and defer, not"
        " skip the write), mutant = line missing"
    )

    slot_2311 = end - timedelta(minutes=49)
    conn.execute(
        "UPDATE station_wind_days SET next_attempt_at = ?"
        " WHERE station_id = ? AND local_date = ?",
        (isoformat_utc(slot_2311), station_id, _DAY.isoformat()),
    )
    conn.commit()

    import wxverify.worker.wind_days as wind_days_module

    def _raise(*args: object, **kwargs: object) -> None:
        raise sqlite3.OperationalError("injected: outcome write fails")

    monkeypatch.setattr(wind_days_module, "record_collection_try", _raise)
    _freeze(monkeypatch, slot_2311)
    with pytest.raises(sqlite3.OperationalError):
        _run(db, site_id, lambda req: _empty_ok(), monkeypatch=monkeypatch)
    rows = _close_try_rows(conn, site_id, _DAY)
    assert len(rows) == 2, (
        "mutant -> at this assertion: correct = still 2 rows (the third"
        " try's outcome write rolled back whole, so no stray log row),"
        f" mutant (a log row written outside the outcome transaction) ="
        f" {len(rows)}"
    )
    close_db()


# ---------------------------------------------------------------------------
# EW12 -- next_five_minute_try.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tz", _ZONES)
def test_ew12_next_five_minute_try(tmp_path: Path, tz: str) -> None:
    """EW12: B due at 23:11 and C at 23:06 -> 23:06 (the earliest); every
    row done for the day -> the day's end; a site with no enabled station
    -> None; the close on with no record holding NOW -> that first
    record's ``day_start_utc`` (M75, M76, M81)."""
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(
        conn,
        timezone=tz,
        with_records=False,
        station_ids=("ISTATION02", "ISTATION03"),
    )
    _give_today_record(conn, site_id, tz)
    station_b = _station_id(conn, site_id, "ISTATION02")
    station_c = _station_id(conn, site_id, "ISTATION03")
    start, end, _ = local_day_slots(_DAY, tz)
    slot_2311 = end - timedelta(minutes=49)
    slot_2306 = end - timedelta(minutes=54)
    _insert_day(conn, station_b, _DAY, next_attempt_at=isoformat_utc(slot_2311))
    _insert_day(conn, station_c, _DAY, next_attempt_at=isoformat_utc(slot_2306))
    conn.commit()

    # `start + timedelta(hours=12)`, not `.replace(hour=12)` -- see EW1's
    # note; this is only an arbitrary daytime instant for the query.
    now = start + timedelta(hours=12)
    got = next_five_minute_try(conn, site_id=site_id, now_utc=now)
    assert got == slot_2306, (
        "mutant -> at this assertion: correct = 23:06 (the earliest of the"
        " two rows' next_attempt_at, M75), mutant (the latest taken"
        f" instead) = {got}"
    )

    conn.execute(
        "UPDATE station_wind_days SET next_attempt_at = ?"
        " WHERE station_id IN (?, ?) AND local_date = ?",
        (
            isoformat_utc(end + timedelta(hours=1)),
            station_b,
            station_c,
            _DAY.isoformat(),
        ),
    )
    conn.commit()
    got_done = next_five_minute_try(conn, site_id=site_id, now_utc=now)
    assert got_done == end, (
        "mutant -> at this assertion: correct = the day's end (every row"
        " is done for the day, M76), mutant (a done row's own"
        f" next_attempt_at returned instead) = {got_done}"
    )

    conn.execute("UPDATE stations SET enabled = 0 WHERE site_id = ?", (site_id,))
    conn.commit()
    assert next_five_minute_try(conn, site_id=site_id, now_utc=now) is None, (
        "mutant -> at this assertion: correct = None (no enabled station),"
        " mutant (a value returned anyway)"
    )
    close_db()


@pytest.mark.parametrize("tz", _ZONES)
def test_ew12_close_on_cutover_uses_the_first_record(tmp_path: Path, tz: str) -> None:
    """EW12 (close on, cutover): the close is on but no record holds NOW
    (the cutover day before the site's first record starts): the result is
    that first record's ``day_start_utc``, never a fallback to
    ``sites.timezone`` (M81)."""
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    first_start, _, _ = local_day_slots(date(2026, 3, 11), tz)
    # Built relative to the first record's own local day boundary, not a
    # fixed UTC literal: a literal noon/midday UTC instant is not "the next
    # day" in every zone (e.g. Etc/GMT-14's local day runs 14h ahead of
    # UTC), so a hardcoded instant can silently fall on the wrong local
    # date and either fail start_site_days's own-day guard or break the
    # "before the first record" precondition below.
    start_of_first_day = first_start + timedelta(hours=1)
    start_site_days(conn, site_id, date(2026, 3, 11), now_utc=start_of_first_day)
    conn.commit()

    before_cutover = first_start - timedelta(hours=1)
    assert current_day(conn, site_id, now_utc=before_cutover) is None, (
        "fixture bug: before_cutover must fall before the first record"
    )
    got = next_five_minute_try(conn, site_id=site_id, now_utc=before_cutover)
    assert got == first_start, (
        "mutant -> at this assertion: correct = the first record's"
        " day_start_utc (M81: with no record holding NOW, the slots fall"
        " back to today's local date in sites.timezone instead), mutant ="
        f" {got}"
    )
    close_db()


# ---------------------------------------------------------------------------
# EW13 -- a reservation JobDeferred is caught per row, not left to escape.
# ---------------------------------------------------------------------------


def test_ew13_a_reservation_deferral_is_caught_per_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EW13: the UTC site, B and C's today rows due at 23:06: a domain
    back-off in force to 23:14; then, instead, the call limit reached with
    the source's billing_tz set to UTC so the deferral ends at 00:00: in
    both, no request, the budget unchanged, one close_try_log row per row
    (outcome 'failed', reason 'backoff' then 'call_limit'), and the lane
    job not deferred; after the back-off each row's next try is 23:16;
    after the call limit each row is done for the day (M78)."""
    import wxverify.worker.domain_backoff as domain_backoff_module

    for scenario in ("backoff", "call_limit"):
        db = _init_tmp_db(tmp_path, name=scenario)
        conn = db._conn  # noqa: SLF001
        site_id = day_close_site(
            conn,
            timezone=_TZ,
            with_records=False,
            station_ids=("ISTATION02", "ISTATION03"),
        )
        _give_today_record(conn, site_id, _TZ)
        station_b = _station_id(conn, site_id, "ISTATION02")
        station_c = _station_id(conn, site_id, "ISTATION03")
        start, end, _ = local_day_slots(_DAY, _TZ)
        slot_2306 = end - timedelta(minutes=54)
        _insert_day(conn, station_b, _DAY, next_attempt_at=isoformat_utc(slot_2306))
        _insert_day(conn, station_c, _DAY, next_attempt_at=isoformat_utc(slot_2306))

        before_calls = conn.execute(
            "SELECT COALESCE(SUM(calls), 0) AS n FROM api_budget"
            " WHERE source = 'weathercom'"
        ).fetchone()["n"]

        _freeze(monkeypatch, slot_2306)

        if scenario == "backoff":
            domain_backoff_module.record_http_backoff(
                conn,
                httpx.Response(
                    429,
                    headers={"Retry-After": "480"},  # ends 23:14
                    request=httpx.Request("GET", "https://api.weather.com/x"),
                ),
            )
        else:
            conn.execute(
                "UPDATE sources SET billing_tz = 'UTC', daily_call_limit = 0"
                " WHERE source = 'weathercom'"
            )
        conn.commit()

        calls: list[str] = []

        def handler(request: httpx.Request, calls: list[str] = calls) -> httpx.Response:
            calls.append(str(request.url))
            return _empty_ok()

        _run(db, site_id, handler, monkeypatch=monkeypatch)  # must not raise

        assert calls == [], (
            "mutant -> at this assertion"
            f" (scenario={scenario}): correct = no request, mutant"
            " (M78: the reservation JobDeferred escaped _run_row before"
            f" the budget/backoff check even ran) = {calls}"
        )
        after_calls = conn.execute(
            "SELECT COALESCE(SUM(calls), 0) AS n FROM api_budget"
            " WHERE source = 'weathercom'"
        ).fetchone()["n"]
        assert after_calls == before_calls, "the budget must be unchanged"

        for station_id in (station_b, station_c):
            rows = _close_try_rows(conn, site_id, _DAY)
            station_rows = [r for r in rows if r["station_id"] == station_id]
            assert len(station_rows) == 1 and station_rows[0]["outcome"] == "failed", (
                f"mutant -> at this assertion (scenario={scenario}):"
                " correct = one close_try_log row, outcome='failed',"
                f" mutant = {[dict(r) for r in station_rows]}"
            )
            assert station_rows[0]["reason"] == scenario

        pending = conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE type = 'fetch_obs'"
            " AND job_key = ? AND status = 'pending'",
            (WIND_DAYS_JOB_KEY,),
        ).fetchone()["n"]
        assert pending == 0, (
            f"mutant -> at this assertion (scenario={scenario}): correct ="
            " 0 (the lane job is not left pending/deferred), mutant"
            f" (the JobDeferred still escapes to the scheduler) = {pending}"
        )

        for station_id in (station_b, station_c):
            row = _day_row(conn, station_id, _DAY)
            if scenario == "backoff":
                assert row["next_attempt_at"] == isoformat_utc(
                    end - timedelta(minutes=44)
                ), (
                    "mutant -> at this assertion: correct = 23:16 (the"
                    " first evening slot at or after the back-off's end,"
                    f" 23:14), mutant = {row['next_attempt_at']!r}"
                )
            else:
                assert row["next_attempt_at"] == isoformat_utc(
                    end + timedelta(hours=1)
                ), (
                    "mutant -> at this assertion: correct = end + 1h (no"
                    " evening slot survives a midnight billing boundary,"
                    f" so the row is done for the day), mutant ="
                    f" {row['next_attempt_at']!r}"
                )
        close_db()


# ---------------------------------------------------------------------------
# EW14 -- the cutover day, no record holding NOW: no tries placed on
# sites.timezone.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tz", _ZONES)
def test_ew14_cutover_with_no_record_makes_no_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """EW14: the close on, the site's first record starting tomorrow,
    today's row due (on the old sites.timezone schedule) at 23:00: no
    request, the budget unchanged, the row unchanged -- the closed-day guard refuses
    every reading of the cutover day, so no try may be placed on it at all
    (M81)."""
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    _disable_station(conn, site_id, "ISTATION02")
    station_id = _station_id(conn, site_id, "ISTATION01")
    # Built relative to the first record's own local day, not a fixed UTC
    # literal -- see test_ew12_close_on_cutover_uses_the_first_record's note.
    first_start, _, _ = local_day_slots(date(2026, 3, 11), tz)
    start_of_first_day = first_start + timedelta(hours=1)
    start_site_days(conn, site_id, date(2026, 3, 11), now_utc=start_of_first_day)

    start, end, _ = local_day_slots(_DAY, tz)
    cap = end - timedelta(hours=1)
    seeded = isoformat_utc(cap)
    _insert_day(conn, station_id, _DAY, next_attempt_at=seeded)
    conn.commit()

    before_calls = conn.execute(
        "SELECT COALESCE(SUM(calls), 0) AS n FROM api_budget"
        " WHERE source = 'weathercom'"
    ).fetchone()["n"]
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return _empty_ok()

    _freeze(monkeypatch, cap)
    _run(db, site_id, handler, monkeypatch=monkeypatch)

    assert calls == [], (
        "mutant -> at this assertion: correct = no request (the closed-day guard"
        " refuses every reading of the cutover day, so the row is never"
        " tried), mutant (M81: a try placed using sites.timezone's own"
        f" schedule) = {calls}"
    )
    after_calls = conn.execute(
        "SELECT COALESCE(SUM(calls), 0) AS n FROM api_budget"
        " WHERE source = 'weathercom'"
    ).fetchone()["n"]
    assert after_calls == before_calls
    row = _day_row(conn, station_id, _DAY)
    assert row["next_attempt_at"] == seeded, (
        "mutant -> at this assertion: correct = unchanged (no slot was"
        " ever computed for a cutover day with no holding record), mutant"
        f" (the row moved anyway) = {row['next_attempt_at']!r}"
    )
    close_db()


# ---------------------------------------------------------------------------
# EW15 -- daytime deferrals: a back-off end, a transport error's capped
# wake, and a plain failure's ordinary interval.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tz", _ZONES)
def test_ew15_daytime_deferrals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """EW15: a 429 with no Retry-After at 14:00 moves the row and the
    JobDeferred to the default 60 s back-off's end, 14:01, not the 180-min
    interval (M87); a transport error at 22:50 moves both to 22:50 plus 15
    min, capped at 23:00 (M89); an HTTP 404 at 10:00 -- a plain failure
    that sets no back-off -- moves the row by the ordinary interval, 13:00,
    with no JobDeferred at all. Each case gets its own database, so one
    case's domain back-off cannot leak into the next."""
    start, end, _ = local_day_slots(_DAY, tz)
    cap = end - timedelta(hours=1)

    db = _init_tmp_db(tmp_path, name="ew15-429")
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    _give_today_record(conn, site_id, tz)
    _disable_station(conn, site_id, "ISTATION02")
    station_id = _station_id(conn, site_id, "ISTATION01")
    at_1400 = start + timedelta(hours=14)
    _insert_day(conn, station_id, _DAY, next_attempt_at=isoformat_utc(at_1400))
    _freeze(monkeypatch, at_1400)
    with pytest.raises(JobDeferred) as exc_info:
        _run(
            db,
            site_id,
            lambda req: httpx.Response(429, request=req),
            monkeypatch=monkeypatch,
        )
    expected_1401 = isoformat_utc(at_1400 + timedelta(seconds=60))
    assert exc_info.value.next_attempt_at == expected_1401, (
        "mutant -> at this assertion: correct ="
        f" {expected_1401} (M87: a daytime 429 with no Retry-After defers"
        " the job to the default 60 s back-off's own end, 14:01), mutant"
        " (the JobDeferred moved by the 180-min interval instead) ="
        f" {exc_info.value.next_attempt_at!r}"
    )
    row = _day_row(conn, station_id, _DAY)
    assert row["next_attempt_at"] == expected_1401, (
        "mutant -> at this assertion: correct ="
        f" {expected_1401} (M87: the row itself moves to the back-off's"
        " own end, not the 180-min interval), mutant ="
        f" {row['next_attempt_at']!r}"
    )
    close_db()

    db = _init_tmp_db(tmp_path, name="ew15-transport")
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    _give_today_record(conn, site_id, tz)
    _disable_station(conn, site_id, "ISTATION02")
    station_id = _station_id(conn, site_id, "ISTATION01")
    at_2250 = cap - timedelta(minutes=10)
    _insert_day(conn, station_id, _DAY, next_attempt_at=isoformat_utc(at_2250))

    def transport(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    _freeze(monkeypatch, at_2250)
    with pytest.raises(JobDeferred) as exc_info:
        _run(db, site_id, transport, monkeypatch=monkeypatch)
    assert exc_info.value.next_attempt_at == isoformat_utc(cap), (
        "mutant -> at this assertion: correct ="
        f" {isoformat_utc(cap)} (M89: 22:50 plus 15 min is 23:05, capped"
        " at the 23:00 daytime cap), mutant (the uncapped 23:05, or a"
        f" fallback to the interval) = {exc_info.value.next_attempt_at!r}"
    )
    row = _day_row(conn, station_id, _DAY)
    assert row["next_attempt_at"] == isoformat_utc(cap), (
        "mutant -> at this assertion: correct ="
        f" {isoformat_utc(cap)} (M89: the row's own next try is capped at"
        f" 23:00 too), mutant = {row['next_attempt_at']!r}"
    )
    close_db()

    db = _init_tmp_db(tmp_path, name="ew15-404")
    conn = db._conn  # noqa: SLF001
    site_id = day_close_site(conn, timezone=tz, with_records=False)
    _give_today_record(conn, site_id, tz)
    _disable_station(conn, site_id, "ISTATION02")
    station_id = _station_id(conn, site_id, "ISTATION01")
    at_1000 = start + timedelta(hours=10)
    _insert_day(conn, station_id, _DAY, next_attempt_at=isoformat_utc(at_1000))
    _freeze(monkeypatch, at_1000)
    _run(
        db,
        site_id,
        lambda req: httpx.Response(404, request=req),
        monkeypatch=monkeypatch,
    )  # a plain failure that sets no back-off must not raise JobDeferred
    expected_1300 = isoformat_utc(at_1000 + timedelta(minutes=180))
    row = _day_row(conn, station_id, _DAY)
    assert row["next_attempt_at"] == expected_1300, (
        "mutant -> at this assertion: correct ="
        f" {expected_1300} (10:00 plus the ordinary 180-min interval,"
        " 13:00: a plain failure is not retried early), mutant ="
        f" {row['next_attempt_at']!r}"
    )
    close_db()
