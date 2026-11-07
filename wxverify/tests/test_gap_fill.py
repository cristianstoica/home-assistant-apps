"""The gap fill (plan 2026-10-21-daily-close-download-and-raw-store.md
section 7.5, 12.1 and 14.8): ``gap_plan`` and ``fill_gaps``, tests GF1-GF9.

The site is built with ``tests.helpers.day_close_site`` (A and B), extended
with four more synthetic stations (C-F, ``ISTATION03``-``ISTATION06``), per
plan 14.1: each added with its own ``station_changes`` 'added' row dated
D-10, before the site's day records are started, so every station sits on
every day's station list (P1 DS13: a started day never gains a station).
D-3 is closed and D-2 is missed exactly as ``day_close_site``'s own
``with_records=True`` path does; this file additionally closes D-1 (named
``GAP_DAY`` below) through ``begin_close``, so ``GAP_DAY`` -- the day whose
gap fill these tests exercise -- is ``closing`` by the time ``NOW`` (D at
noon UTC, ``tests.helpers.NOW``) is reached. Zone is ``UTC`` throughout;
every station id is synthetic (``ISTATION01``-``ISTATION06``); the API key
is the CI placeholder.

Raw readings are seeded directly into ``station_wind_records`` (plan 11.1's
shape, with ``source``), since the download that would otherwise produce
them is out of this file's scope. The six stations' days (inside the
shared ``site`` fixture):

- A (``ISTATION01``): nothing at all -- every hour a gap.
- B (``ISTATION02``): hours 5 and 7 are pure gaps; hour 6 carries one
  isolated ``current_obs`` reading only (no ``five_minute`` reading), so it
  is NOT a gap (a reading of either source is enough, 7.1) but has no wind
  pair (no_pair_hours); hour 23, the day's last hour, carries dense
  ``five_minute`` readings every 5 minutes up to :49 (7.6: the 5-minute
  download never delivers a day's last few minutes), so it pairs within
  itself exactly like any other full hour and is neither a gap nor a
  no-pair hour -- no special carve-out is needed in ``gap_plan`` for it
  (GF8). Every other hour has two readings 5 minutes apart (an easy pair).
- C, E, F: a full day of paired readings, no gaps, nothing to fill.
- D (``ISTATION04``): a full day of paired readings except hour 10, whose
  one reading at :55 pairs forward into hour 11 (the later reading's hour,
  7.4) and so gets no pair of its own -- a no-pair hour, not a gap.

Isolation: a fresh ``tmp_path`` SQLite file and a real ``Database``/
``FencedWriter`` per test (the ``site`` fixture is function-scoped); the
provider is an ``httpx.MockTransport`` patched onto
``wxverify.worker.day_values.httpx.AsyncClient`` (the module that, by this
plan's placement of ``fill_gaps``, is where its own client would be built --
the one choice beyond the written contract this file makes; see the
hand-back message).
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from tests.helpers import NOW, D, day_close_site
from wxverify import config
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro, parse_utc
from wxverify.db.connection import Database, FencedWriter, close_db, init_db
from wxverify.db.site_days import (
    begin_close,
    day_record,
    ensure_site_days,
    fail_close,
    finish_close,
    mark_missed,
    start_site_days,
)
from wxverify.worker.access_hold import read_access_record, record_access_refused
from wxverify.worker.day_values import fill_gaps, gap_plan

_ZONE = "UTC"
STATION_PWS_IDS: tuple[str, str, str, str, str, str] = (
    "ISTATION01",
    "ISTATION02",
    "ISTATION03",
    "ISTATION04",
    "ISTATION05",
    "ISTATION06",
)

# The day this file closes and fills. D-3 and D-2 are closed/missed by
# ``day_close_site`` exactly as it already does; GAP_DAY (D-1) is the one
# this file additionally moves to ``closing`` via ``begin_close``.
GAP_DAY: date = D - timedelta(days=1)


def _reading_at(hour: int, minute: int = 0) -> datetime:
    return datetime(GAP_DAY.year, GAP_DAY.month, GAP_DAY.day, hour, minute, tzinfo=UTC)


def _init_tmp_db(base: Path) -> Database:
    close_db()
    base.mkdir(parents=True, exist_ok=True)
    db_path = base / "wxverify.db"
    config.db_path = str(db_path)
    options_path = base / "options.json"
    options_path.write_text(
        json.dumps({"weathercom_key": "ci-placeholder"}), encoding="utf-8"
    )
    config.options_path = str(options_path)
    return init_db(str(db_path))


def _add_station(  # type: ignore[no-untyped-def]
    conn, site_id: int, pws_id: str
) -> int:
    cur = conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
        " VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
        (site_id, pws_id),
    )
    station_id = int(cur.lastrowid)
    seed_date = (D - timedelta(days=10)).isoformat()
    seed_changed_at = isoformat_utc_micro(
        datetime(D.year, D.month, D.day, tzinfo=UTC) - timedelta(days=10)
    )
    conn.execute(
        "INSERT INTO station_changes"
        " (site_id, station_id, change, effective_date, reason, changed_at)"
        " VALUES (?, ?, 'added', ?, 'test_seed', ?)",
        (site_id, station_id, seed_date, seed_changed_at),
    )
    return station_id


def _close_earlier_days(conn, site_id: int) -> None:  # type: ignore[no-untyped-def]
    """Close D-3 and mark D-2 missed, exactly as ``day_close_site`` does."""
    d_minus_3 = D - timedelta(days=3)
    row3 = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id=? AND local_date=?",
        (site_id, d_minus_3.isoformat()),
    ).fetchone()
    t1 = parse_utc(row3[0]) + timedelta(minutes=30)
    begin_close(conn, site_id, d_minus_3, now_utc=t1)
    finish_close(conn, site_id, d_minus_3, now_utc=t1)

    d_minus_2 = D - timedelta(days=2)
    row2 = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id=? AND local_date=?",
        (site_id, d_minus_2.isoformat()),
    ).fetchone()
    t2 = parse_utc(row2[0]) + timedelta(minutes=30)
    mark_missed(conn, site_id, d_minus_2, now_utc=t2, reason="never_attempted")


def _insert_reading(
    conn,  # type: ignore[no-untyped-def]
    station_id: int,
    when: datetime,
    source: str,
    *,
    speed_kmh: float | None = None,
    temp_c: float | None = None,
    precip_total_mm: float | None = None,
) -> None:
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, local_date, speed_kmh, temp_c,"
        "  precip_total_mm, fetched_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            station_id,
            isoformat_utc(when),
            source,
            GAP_DAY.isoformat(),
            speed_kmh,
            temp_c,
            precip_total_mm,
            isoformat_utc(when),
        ),
    )


def _seed_full_day(conn, station_id: int) -> None:  # type: ignore[no-untyped-def]
    """Every hour paired (minutes 0 and 5): no gaps, no no-pair hours."""
    for hour in range(24):
        _insert_reading(
            conn,
            station_id,
            _reading_at(hour, 0),
            "five_minute",
            speed_kmh=5.0,
            temp_c=10.0,
            precip_total_mm=0.0,
        )
        _insert_reading(
            conn,
            station_id,
            _reading_at(hour, 5),
            "five_minute",
            speed_kmh=5.5,
            temp_c=10.1,
            precip_total_mm=0.0,
        )


def _seed_station_b(conn, station_id: int) -> None:  # type: ignore[no-untyped-def]
    """Hours 5, 7 are pure gaps; hour 6 is current_obs-only (no pair); hour
    23 (the day's last hour) carries dense five_minute readings every 5
    minutes up to :49 (7.6), which pair within the hour like any other
    full hour -- pinning that no last-hour carve-out is needed in
    gap_plan (GF8, M53)."""
    for hour in range(24):
        if hour in (5, 7):
            continue
        if hour == 6:
            _insert_reading(
                conn,
                station_id,
                _reading_at(6, 30),
                "current_obs",
                speed_kmh=4.0,
                temp_c=9.0,
                precip_total_mm=0.1,
            )
            continue
        if hour == 23:
            for minute in range(0, 50, 5):
                _insert_reading(
                    conn,
                    station_id,
                    _reading_at(23, minute),
                    "five_minute",
                    speed_kmh=6.0,
                    temp_c=8.0,
                    precip_total_mm=0.2,
                )
            continue
        _insert_reading(
            conn,
            station_id,
            _reading_at(hour, 0),
            "five_minute",
            speed_kmh=5.0,
            temp_c=10.0,
            precip_total_mm=0.0,
        )
        _insert_reading(
            conn,
            station_id,
            _reading_at(hour, 5),
            "five_minute",
            speed_kmh=5.5,
            temp_c=10.1,
            precip_total_mm=0.0,
        )


def _seed_station_d(conn, station_id: int) -> None:  # type: ignore[no-untyped-def]
    """Every hour paired except hour 10: its one reading at :55 pairs
    forward into hour 11 (the later reading's hour, 7.4), so hour 10 gets
    no pair of its own -- a no-pair hour, not a gap (it has a reading)."""
    for hour in range(24):
        if hour == 10:
            _insert_reading(
                conn,
                station_id,
                _reading_at(10, 55),
                "five_minute",
                speed_kmh=7.0,
                temp_c=11.0,
                precip_total_mm=0.0,
            )
            continue
        _insert_reading(
            conn,
            station_id,
            _reading_at(hour, 0),
            "five_minute",
            speed_kmh=5.0,
            temp_c=10.0,
            precip_total_mm=0.0,
        )
        _insert_reading(
            conn,
            station_id,
            _reading_at(hour, 5),
            "five_minute",
            speed_kmh=5.5,
            temp_c=10.1,
            precip_total_mm=0.0,
        )


def _build_base_site(
    conn,  # type: ignore[no-untyped-def]
    *,
    seed_rest: bool = True,
    close_gap_day: bool = True,
) -> tuple[int, dict[str, int]]:
    site_id = day_close_site(
        conn,
        timezone=_ZONE,
        close_on=True,
        with_records=False,
        station_ids=(STATION_PWS_IDS[0], STATION_PWS_IDS[1]),
    )
    stations: dict[str, int] = {}
    for pws_id in STATION_PWS_IDS[:2]:
        row = conn.execute(
            "SELECT id FROM stations WHERE pws_station_id=?", (pws_id,)
        ).fetchone()
        stations[pws_id] = int(row[0])
    for pws_id in STATION_PWS_IDS[2:]:
        stations[pws_id] = _add_station(conn, site_id, pws_id)

    d_minus_3 = D - timedelta(days=3)
    start_site_days(
        conn,
        site_id,
        d_minus_3,
        now_utc=datetime(d_minus_3.year, d_minus_3.month, d_minus_3.day, tzinfo=UTC),
    )
    ensure_site_days(conn, site_id, now_utc=NOW)
    _close_earlier_days(conn, site_id)

    if seed_rest:
        _seed_station_b(conn, stations[STATION_PWS_IDS[1]])
        _seed_full_day(conn, stations[STATION_PWS_IDS[2]])  # C
        _seed_station_d(conn, stations[STATION_PWS_IDS[3]])
        _seed_full_day(conn, stations[STATION_PWS_IDS[4]])  # E
        _seed_full_day(conn, stations[STATION_PWS_IDS[5]])  # F
        # A (index 0) is left with nothing all day.

    if close_gap_day:
        begin_close(conn, site_id, GAP_DAY, now_utc=NOW)

    return site_id, stations


def _minimal_site(
    base: Path, *, close: bool = True
) -> tuple[Database, FencedWriter, int, int]:
    """A site with only A needing a fill; B is a full day (nothing to fill),
    so a run only ever calls the provider for A. Used by the single-outcome
    tests (GF4, GF5) to keep each assertion to one station."""
    db = _init_tmp_db(base)
    conn = db._conn  # noqa: SLF001 -- tests inspect the real writer connection
    site_id, stations = _build_base_site(conn, seed_rest=False, close_gap_day=False)
    _seed_full_day(conn, stations[STATION_PWS_IDS[1]])
    if close:
        begin_close(conn, site_id, GAP_DAY, now_utc=NOW)
    return db, FencedWriter(db), site_id, stations[STATION_PWS_IDS[0]]


@pytest.fixture
def site(tmp_path: Path) -> tuple[Database, FencedWriter, int, dict[str, int]]:
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001 -- tests inspect the real writer connection
    site_id, stations = _build_base_site(conn)
    return db, FencedWriter(db), site_id, stations


def _budget_calls(conn) -> int:  # type: ignore[no-untyped-def]
    row = conn.execute(
        "SELECT COALESCE(SUM(calls), 0) FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    return int(row[0])


def _hourly_week_body(skip_hours: set[datetime] | None = None) -> dict[str, object]:
    """An hourly reply holding every hour of the 7 days up to and including
    GAP_DAY, minus ``skip_hours``."""
    skip = skip_hours or set()
    observations: list[dict[str, object]] = []
    start = datetime(GAP_DAY.year, GAP_DAY.month, GAP_DAY.day, tzinfo=UTC) - timedelta(
        days=6
    )
    end = datetime(GAP_DAY.year, GAP_DAY.month, GAP_DAY.day, tzinfo=UTC) + timedelta(
        days=1
    )
    hour = start
    while hour < end:
        if hour not in skip:
            observations.append(
                {
                    "valid_time_gmt": int(hour.timestamp()),
                    "obsTimeLocal": hour.strftime("%Y-%m-%d %H:%M:%S"),
                    "metric": {
                        "tempAvg": 12.0,
                        "precipTotal": 0.5,
                        "windspeedAvg": 9.0,
                    },
                }
            )
        hour += timedelta(hours=1)
    return {"observations": observations}


def _patched_client(handler):  # type: ignore[no-untyped-def]
    real_client = httpx.AsyncClient
    return patch(
        "wxverify.worker.day_values.httpx.AsyncClient",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# GF1
# ---------------------------------------------------------------------------


def test_gf1_gap_plan_identifies_gaps_no_pair_and_to_fill(
    site: tuple[Database, FencedWriter, int, dict[str, int]],
) -> None:
    """GF1 (plan 14.8): B's 3-hour window, D's no-pair hour, A with nothing.

    Kills M46 (gap_plan would count hour 6 -- which carries only a
    current_obs reading -- as a gap too): at the gap_hours(B) assertion,
    correct = {5, 7}, mutant = {5, 6, 7} (B's current_obs-only hour 6 is
    counted when it should not be).
    """
    db, _writer, site_id, stations = site
    plans = gap_plan(db._conn, site_id=site_id, local_date=GAP_DAY)  # noqa: SLF001

    a_id = stations[STATION_PWS_IDS[0]]
    b_id = stations[STATION_PWS_IDS[1]]
    d_id = stations[STATION_PWS_IDS[3]]

    assert set(plans.keys()) == set(stations.values())

    all_hours = {_reading_at(h) for h in range(24)}
    assert set(plans[a_id].gap_hours) == all_hours
    assert set(plans[a_id].no_pair_hours) == set()
    assert set(plans[a_id].to_fill) == all_hours

    assert set(plans[b_id].gap_hours) == {_reading_at(5), _reading_at(7)}
    assert set(plans[b_id].no_pair_hours) == {_reading_at(6)}
    assert set(plans[b_id].to_fill) == {
        _reading_at(5),
        _reading_at(6),
        _reading_at(7),
    }

    assert set(plans[d_id].gap_hours) == set()
    assert set(plans[d_id].no_pair_hours) == {_reading_at(10)}
    assert set(plans[d_id].to_fill) == {_reading_at(10)}


# ---------------------------------------------------------------------------
# GF2
# ---------------------------------------------------------------------------


def test_gf2_fill_gaps_stores_only_the_hours_to_fill(
    site: tuple[Database, FencedWriter, int, dict[str, int]],
) -> None:
    """GF2: one request per station with a non-empty to_fill; only those
    hours, inside D, are stored (source hourly); C, whose plan needs
    nothing, draws no request.

    Kills M47 (fill_gaps would store every row of the reply inside the
    day): at the stored-rows assertion for A, correct = the 24 gap hours,
    mutant = every one of the 7 days' worth of rows the reply carried (it
    never filters by ``to_fill``).
    """
    db, writer, site_id, stations = site
    conn = db._conn  # noqa: SLF001
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url.params["stationId"]))
        return httpx.Response(200, content=json.dumps(_hourly_week_body()).encode())

    with _patched_client(handler):
        result = asyncio.run(
            fill_gaps(db, writer, site_id=site_id, local_date=GAP_DAY, now_utc=NOW)
        )

    assert result.ended is True
    needing_fill = {STATION_PWS_IDS[0], STATION_PWS_IDS[1], STATION_PWS_IDS[3]}
    assert set(requested) == needing_fill
    assert STATION_PWS_IDS[2] not in requested
    assert STATION_PWS_IDS[4] not in requested
    assert STATION_PWS_IDS[5] not in requested

    expected_hours = {
        STATION_PWS_IDS[0]: {_reading_at(h) for h in range(24)},
        STATION_PWS_IDS[1]: {_reading_at(5), _reading_at(6), _reading_at(7)},
        STATION_PWS_IDS[3]: {_reading_at(10)},
    }
    for pws_id, hours in expected_hours.items():
        rows = conn.execute(
            "SELECT obs_at FROM station_wind_records"
            " WHERE station_id=? AND source='hourly'",
            (stations[pws_id],),
        ).fetchall()
        stored = {parse_utc(row[0]) for row in rows}
        assert stored == hours, pws_id


# ---------------------------------------------------------------------------
# GF3
# ---------------------------------------------------------------------------


def test_gf3_a_204_and_an_incomplete_reply_leave_hours_unfilled(
    site: tuple[Database, FencedWriter, int, dict[str, int]],
) -> None:
    """GF3: a 204 for A; a reply lacking one of B's gap hours.

    Kills M48 (a 204 from the hourly download marks the hours filled): at
    the outcome assertion for A, correct = ``"no_readings"`` with every
    hour still in hours_left, mutant marks them filled instead.
    """
    db, writer, site_id, stations = site
    conn = db._conn  # noqa: SLF001

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == STATION_PWS_IDS[0]:
            return httpx.Response(204)
        if station == STATION_PWS_IDS[1]:
            body = _hourly_week_body(skip_hours={_reading_at(7)})
            return httpx.Response(200, content=json.dumps(body).encode())
        return httpx.Response(200, content=json.dumps(_hourly_week_body()).encode())

    with _patched_client(handler):
        result = asyncio.run(
            fill_gaps(db, writer, site_id=site_id, local_date=GAP_DAY, now_utc=NOW)
        )

    a = result.stations[stations[STATION_PWS_IDS[0]]]
    assert a.outcome == "no_readings"
    assert set(a.hours_left) == {_reading_at(h) for h in range(24)}
    a_rows = conn.execute(
        "SELECT 1 FROM station_wind_records WHERE station_id=? AND source='hourly'",
        (stations[STATION_PWS_IDS[0]],),
    ).fetchall()
    assert a_rows == []

    b = result.stations[stations[STATION_PWS_IDS[1]]]
    assert set(b.hours_left) == {_reading_at(7)}


# ---------------------------------------------------------------------------
# GF4
# ---------------------------------------------------------------------------


def test_gf4_held_refused_failed_and_network_outcomes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """GF4: the hourly hold, a 401, a 500, then a ConnectError -- each its
    own fresh single-station site (A always needs a fill; B never does).

    Kills M49 (a held fill makes the call): at the "held" case's
    ``called`` assertion, correct = ``[]`` (no request at all), mutant
    makes the call anyway, so ``called`` holds A's station id.
    """
    # 1. held: the hourly hold is in force, so fill_gaps must not call out.
    db, writer, site_id, station_a = _minimal_site(tmp_path / "held")
    conn = db._conn  # noqa: SLF001
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: NOW)
    record_access_refused(conn, "hourly", 403, reason=None)
    before = _budget_calls(conn)
    called: list[str] = []

    def handler_held(request: httpx.Request) -> httpx.Response:
        called.append(str(request.url.params["stationId"]))
        return httpx.Response(200, content=b'{"observations": []}')

    with _patched_client(handler_held):
        result = asyncio.run(
            fill_gaps(db, writer, site_id=site_id, local_date=GAP_DAY, now_utc=NOW)
        )
    assert called == []
    assert _budget_calls(conn) == before
    a = result.stations[station_a]
    assert a.outcome == "held"
    assert a.reason == "refused_access"

    # 2. 401: refused, and the hold is written.
    db2, writer2, site_id2, station_a2 = _minimal_site(tmp_path / "refused")
    conn2 = db2._conn  # noqa: SLF001

    def handler_401(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401)

    with _patched_client(handler_401):
        result2 = asyncio.run(
            fill_gaps(db2, writer2, site_id=site_id2, local_date=GAP_DAY, now_utc=NOW)
        )
    a2 = result2.stations[station_a2]
    assert a2.outcome == "refused"
    assert a2.reason == "refused_access"
    assert a2.detail == "http 401"
    hold = read_access_record(conn2, "hourly")
    assert hold is not None and hold.until is not None

    # 3. 500: failed, provider, the call still counted.
    db3, writer3, site_id3, station_a3 = _minimal_site(tmp_path / "provider")
    conn3 = db3._conn  # noqa: SLF001
    before3 = _budget_calls(conn3)

    def handler_500(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    with _patched_client(handler_500):
        result3 = asyncio.run(
            fill_gaps(db3, writer3, site_id=site_id3, local_date=GAP_DAY, now_utc=NOW)
        )
    a3 = result3.stations[station_a3]
    assert a3.outcome == "failed"
    assert a3.reason == "provider"
    assert a3.detail == "http 500"
    assert _budget_calls(conn3) == before3 + 1

    # 4. ConnectError: failed, network, the call refunded.
    db4, writer4, site_id4, station_a4 = _minimal_site(tmp_path / "network")
    conn4 = db4._conn  # noqa: SLF001
    before4 = _budget_calls(conn4)

    def handler_network(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with _patched_client(handler_network):
        result4 = asyncio.run(
            fill_gaps(db4, writer4, site_id=site_id4, local_date=GAP_DAY, now_utc=NOW)
        )
    a4 = result4.stations[station_a4]
    assert a4.outcome == "failed"
    assert a4.reason == "network"
    assert a4.detail == "network"
    assert _budget_calls(conn4) == before4


# ---------------------------------------------------------------------------
# GF5
# ---------------------------------------------------------------------------


def test_gf5_fill_gaps_refuses_before_the_day_ends(tmp_path: Path) -> None:
    """GF5: ``now_utc`` one second before GAP_DAY's end makes no request.

    Kills M50 (the day-ended check dropped): at the ``result.ended``
    assertion, correct = ``False`` with ``called == []``; the mutant would
    proceed to reserve and call for A regardless.
    """
    db, writer, site_id, station_a = _minimal_site(tmp_path / "not-ended", close=False)
    conn = db._conn  # noqa: SLF001
    day = day_record(conn, site_id, GAP_DAY)
    assert day is not None
    before = _budget_calls(conn)
    called: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        called.append(str(request.url.params["stationId"]))
        return httpx.Response(200, content=b'{"observations": []}')

    with _patched_client(handler):
        result = asyncio.run(
            fill_gaps(
                db,
                writer,
                site_id=site_id,
                local_date=GAP_DAY,
                now_utc=day.day_end_utc - timedelta(seconds=1),
            )
        )

    assert result.ended is False
    assert called == []
    assert _budget_calls(conn) == before


# ---------------------------------------------------------------------------
# GF6
# ---------------------------------------------------------------------------


def test_gf6_a_retry_only_calls_what_is_left(
    site: tuple[Database, FencedWriter, int, dict[str, int]],
) -> None:
    """GF6: a second ``fill_gaps`` skips a station already filled and tries
    again only for one whose reply lacked an hour.

    Kills M51 (fill_gaps calls every station on the list): at the second
    run's requested-stations assertion, correct = ``[ISTATION02]`` only (B,
    the one station still missing hour 7); the mutant would request all
    six stations again.
    """
    db, writer, site_id, stations = site

    def handler_first(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == STATION_PWS_IDS[1]:
            body = _hourly_week_body(skip_hours={_reading_at(7)})
        else:
            body = _hourly_week_body()
        return httpx.Response(200, content=json.dumps(body).encode())

    with _patched_client(handler_first):
        asyncio.run(
            fill_gaps(db, writer, site_id=site_id, local_date=GAP_DAY, now_utc=NOW)
        )

    requested_second: list[str] = []

    def handler_second(request: httpx.Request) -> httpx.Response:
        requested_second.append(str(request.url.params["stationId"]))
        return httpx.Response(200, content=json.dumps(_hourly_week_body()).encode())

    with _patched_client(handler_second):
        asyncio.run(
            fill_gaps(db, writer, site_id=site_id, local_date=GAP_DAY, now_utc=NOW)
        )

    assert requested_second == [STATION_PWS_IDS[1]]


# ---------------------------------------------------------------------------
# GF7
# ---------------------------------------------------------------------------


def test_gf7_a_closed_or_missed_day_refuses_every_gap_fill_write(
    tmp_path: Path,
) -> None:
    """GF7: once GAP_DAY is closed, and separately once it is missed, every
    station's gap-fill write is refused by P1's guard and nothing is
    stored, even though every station still has hours to fill.

    This pins the guard refusal itself (a write-past-the-guard regression
    would show up here as stored rows); P1's guard refuses a closed or
    missed day's WriteKind.GAP_FILL and WriteKind.READING identically
    (``_record_allows``'s DONE_STATES branch answers only RAW_PRUNE), so
    this fixture cannot itself tell GAP_FILL and READING apart -- see the
    hand-back message for where that distinction is actually pinned.
    """
    for close_kind in ("closed", "missed"):
        base = tmp_path / close_kind
        db = _init_tmp_db(base)
        conn = db._conn  # noqa: SLF001
        site_id, stations = _build_base_site(conn, close_gap_day=False)
        before = _budget_calls(conn)
        t = NOW + timedelta(minutes=5)
        if close_kind == "closed":
            begin_close(conn, site_id, GAP_DAY, now_utc=t)
            finish_close(conn, site_id, GAP_DAY, now_utc=t)
        else:
            begin_close(conn, site_id, GAP_DAY, now_utc=t)
            t2 = t + timedelta(minutes=1)
            fail_close(conn, site_id, GAP_DAY, now_utc=t2, reason="test")
            t3 = t2 + timedelta(hours=1, minutes=1)
            begin_close(conn, site_id, GAP_DAY, now_utc=t3)
            t4 = t3 + timedelta(minutes=1)
            fail_close(conn, site_id, GAP_DAY, now_utc=t4, reason="test")

        requested: list[str] = []

        def handler(
            request: httpx.Request, requested: list[str] = requested
        ) -> httpx.Response:
            requested.append(str(request.url.params["stationId"]))
            return httpx.Response(200, content=json.dumps(_hourly_week_body()).encode())

        writer = FencedWriter(db)
        with _patched_client(handler):
            result = asyncio.run(
                fill_gaps(db, writer, site_id=site_id, local_date=GAP_DAY, now_utc=NOW)
            )

        for station_id in stations.values():
            outcome = result.stations[station_id].outcome
            assert outcome == "refused", (close_kind, station_id, outcome)
        rows = conn.execute(
            "SELECT 1 FROM station_wind_records WHERE source='hourly'"
        ).fetchall()
        assert rows == [], close_kind
        assert _budget_calls(conn) == before, close_kind


# ---------------------------------------------------------------------------
# GF8
# ---------------------------------------------------------------------------


def test_gf8_the_days_last_hour_is_never_a_gap_fill_target(
    site: tuple[Database, FencedWriter, int, dict[str, int]],
) -> None:
    """GF8: B's last hour has dense five_minute readings to 23:49 (7.6), so
    it pairs within itself like any other full hour and sits in neither
    ``gap_hours`` nor ``to_fill`` -- no special last-hour exemption is
    needed or taken.

    Kills M53 (gap_plan adds the day's last hour): at the ``to_fill``
    membership assertion, correct = hour 23 absent (its own 5-minute
    readings, 5 minutes apart, already pair), mutant = present (a
    special-case forces the day's last hour into ``to_fill``/
    ``no_pair_hours`` regardless of whether it actually has a pair).
    """
    db, _writer, site_id, stations = site
    b_id = stations[STATION_PWS_IDS[1]]

    plan = gap_plan(db._conn, site_id=site_id, local_date=GAP_DAY)[b_id]  # noqa: SLF001

    last_hour = _reading_at(23)
    assert last_hour not in plan.gap_hours
    assert last_hour not in plan.no_pair_hours
    assert last_hour not in plan.to_fill


# ---------------------------------------------------------------------------
# GF9
# ---------------------------------------------------------------------------


def test_gf9_a_paused_station_gets_no_call(
    site: tuple[Database, FencedWriter, int, dict[str, int]],
) -> None:
    """GF9: ``skip_station_ids`` holding B skips it with no request and no
    write; A, also needing a fill, still gets its call.

    Kills M71 (fill_gaps ignores ``skip_station_ids``): at the requested-
    stations assertion, correct excludes B, mutant includes it.
    """
    db, writer, site_id, stations = site
    conn = db._conn  # noqa: SLF001
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url.params["stationId"]))
        return httpx.Response(200, content=json.dumps(_hourly_week_body()).encode())

    b_id = stations[STATION_PWS_IDS[1]]
    with _patched_client(handler):
        result = asyncio.run(
            fill_gaps(
                db,
                writer,
                site_id=site_id,
                local_date=GAP_DAY,
                now_utc=NOW,
                skip_station_ids=frozenset({b_id}),
            )
        )

    assert STATION_PWS_IDS[1] not in requested
    assert STATION_PWS_IDS[0] in requested

    b = result.stations[b_id]
    assert b.outcome == "skipped"
    assert set(b.hours_left) == {_reading_at(5), _reading_at(6), _reading_at(7)}

    rows = conn.execute(
        "SELECT 1 FROM station_wind_records WHERE station_id=? AND source='hourly'",
        (b_id,),
    ).fetchall()
    assert rows == []
