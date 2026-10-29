"""Writing the hourly values for the close (plan
2026-10-21-daily-close-download-and-raw-store.md section 7.7, 12.1 and
14.9): ``write_hourly_values`` and ``day_presence``, tests DV1-DV6.

The site is built with the same shape as ``tests.helpers.day_close_site``
(P1 §13.1), but with ``with_records=False``: each test picks its own close
state for ``WRITE_DAY`` (D-1) instead of the fixture's own D-3
closed/D-2 missed pair, since DV4 needs a CLOSED day and the rest need a
CLOSING one. D-3 and D-2 are always closed/missed first, as the guard
requires every earlier day to be done before D-1 can move at all. Every
station id is synthetic (``ISTATION01``-``ISTATION05``, standing for A, B,
D and E of the plan's naming); the API key is the CI placeholder; zone is
``UTC`` throughout.

Raw readings are seeded directly into ``station_wind_records`` (plan
11.1's shape, with ``source``), since the download that would otherwise
produce them is out of this file's scope.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import NOW, D, day_close_site
from wxverify import config
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro, parse_utc
from wxverify.core.units import kmh_to_ms
from wxverify.db.connection import Database, close_db, init_db
from wxverify.db.site_days import (
    begin_close,
    ensure_site_days,
    finish_close,
    mark_missed,
    start_site_days,
)
from wxverify.worker.day_values import day_presence, write_hourly_values

_ZONE = "UTC"
STATION_PWS_IDS: tuple[str, str, str, str] = (
    "ISTATION01",  # A
    "ISTATION02",  # B
    "ISTATION04",  # D
    "ISTATION05",  # E
)

# The day these tests write to. D-3 and D-2 are always closed/missed first
# (the guard requires every earlier day done); WRITE_DAY's own state is
# each test's business.
WRITE_DAY: date = D - timedelta(days=1)


def _hour_at(hour: int) -> datetime:
    return datetime(WRITE_DAY.year, WRITE_DAY.month, WRITE_DAY.day, hour, tzinfo=UTC)


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
            WRITE_DAY.isoformat(),
            speed_kmh,
            temp_c,
            precip_total_mm,
            isoformat_utc(when),
        ),
    )


def _build_site(
    conn,  # type: ignore[no-untyped-def]
    *,
    pws_ids: tuple[str, ...],
    close_write_day: bool = True,
) -> tuple[int, dict[str, int]]:
    site_id = day_close_site(
        conn,
        timezone=_ZONE,
        close_on=True,
        with_records=False,
        station_ids=(pws_ids[0], pws_ids[1]),
    )
    stations: dict[str, int] = {}
    for pws_id in pws_ids[:2]:
        row = conn.execute(
            "SELECT id FROM stations WHERE pws_station_id=?", (pws_id,)
        ).fetchone()
        stations[pws_id] = int(row[0])
    for pws_id in pws_ids[2:]:
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

    if close_write_day:
        begin_close(conn, site_id, WRITE_DAY, now_utc=NOW)

    return site_id, stations


def _seed_full_day(
    conn,  # type: ignore[no-untyped-def]
    station_id: int,
    *,
    minutes: tuple[int, ...],
    temp_c: float,
    speed_kmh: float,
    rain_step_mm: float,
) -> None:
    """Every hour the same constant readings: a plain mean of ``temp_c``, a
    rain rise of ``rain_step_mm`` and a wind pair-max of ``speed_kmh`` (the
    readings are all equal, so the pair mean and the plain mean both come
    out to the constant itself)."""
    total = 0.0
    for hour in range(24):
        total += rain_step_mm
        for minute in minutes:
            _insert_reading(
                conn,
                station_id,
                datetime(
                    WRITE_DAY.year,
                    WRITE_DAY.month,
                    WRITE_DAY.day,
                    hour,
                    minute,
                    tzinfo=UTC,
                ),
                "five_minute",
                speed_kmh=speed_kmh,
                temp_c=temp_c,
                precip_total_mm=round(total, 2),
            )


# ---------------------------------------------------------------------------
# DV1
# ---------------------------------------------------------------------------


def test_dv1_writes_hourly_values_with_7_7_source_raw_forms(tmp_path: Path) -> None:
    """DV1 (plan 14.9): a full day for B and D, WRITE_DAY closing.

    Kills M54 (write_hourly_values enqueues pair_and_score): at the
    ``jobs`` count assertion, correct = unchanged (0), mutant = at least
    one new row.
    Kills M55 (it skips materialize_consensus): at the ``observations``
    count assertion, correct = 72 (24 hours x 3 variables, one consensus
    row per changed hour/variable), mutant = 0 (nothing in ``observations``
    is ever written).
    The wind assertion (``b_wind is not None``) also catches a mutant that
    routes writes through ``insert_station_observation`` instead of
    ``write_station_observation`` directly: that function silently drops
    every wind row (named in the ``Catches`` column, no numbered mutant).
    """
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id, stations = _build_site(conn, pws_ids=STATION_PWS_IDS[1:3])
    b_id = stations[STATION_PWS_IDS[1]]
    d_id = stations[STATION_PWS_IDS[2]]

    _seed_full_day(
        conn,
        b_id,
        minutes=tuple(range(0, 60, 5)),
        temp_c=11.25,
        speed_kmh=15.5,
        rain_step_mm=0.4,
    )
    _seed_full_day(
        conn,
        d_id,
        minutes=(0, 15, 30, 45),
        temp_c=9.0,
        speed_kmh=8.0,
        rain_step_mm=0.2,
    )

    before_jobs = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type = 'pair_and_score'"
    ).fetchone()[0]

    result = write_hourly_values(
        conn, site_id=site_id, local_date=WRITE_DAY, now_utc=NOW
    )

    assert result.outcome == "written"
    assert result.rows_written == 144  # 2 stations x 24 hours x 3 quantities
    assert result.rows_removed == 0
    assert result.hours_materialized == 72  # 24 hours x 3 variables

    rows = conn.execute(
        "SELECT COUNT(*) FROM station_observations WHERE station_id IN (?, ?)",
        (b_id, d_id),
    ).fetchone()[0]
    assert rows == 144

    hour10 = isoformat_utc(_hour_at(10))
    b_temp = conn.execute(
        "SELECT value, source_raw FROM station_observations"
        " WHERE station_id=? AND variable='temperature' AND valid_at=?",
        (b_id, hour10),
    ).fetchone()
    assert b_temp is not None
    assert b_temp["source_raw"] == "hour-mean 11.25 C n=12 src=five_minute"
    assert b_temp["value"] == pytest.approx(11.25)

    b_rain = conn.execute(
        "SELECT value, source_raw FROM station_observations"
        " WHERE station_id=? AND variable='precip' AND valid_at=?",
        (b_id, hour10),
    ).fetchone()
    assert b_rain is not None
    assert b_rain["source_raw"] == "hour-rise 0.4 mm src=five_minute"
    assert b_rain["value"] == pytest.approx(0.4)

    b_wind = conn.execute(
        "SELECT value, source_raw FROM station_observations"
        " WHERE station_id=? AND variable='wind' AND valid_at=?",
        (b_id, hour10),
    ).fetchone()
    assert b_wind is not None
    assert b_wind["source_raw"] == "pair-max 15.5 km/h src=five_minute"
    assert b_wind["value"] == pytest.approx(kmh_to_ms(15.5))

    observations_count = conn.execute(
        "SELECT COUNT(*) FROM observations WHERE site_id=?", (site_id,)
    ).fetchone()[0]
    assert observations_count == 72

    after_jobs = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type = 'pair_and_score'"
    ).fetchone()[0]
    assert after_jobs == before_jobs


# ---------------------------------------------------------------------------
# DV2
# ---------------------------------------------------------------------------


def test_dv2_removes_a_stale_row_at_an_hour_with_no_value(tmp_path: Path) -> None:
    """DV2 (plan 14.9): an older row at a gap hour is removed, and that
    hour's ``observations`` row is rebuilt away.

    Kills M56 (write_hourly_values keeps rows at hours without a value):
    at the post-write station_observations assertion, correct = no row for
    B's hour 15 (a gap all day: no reading of any source), mutant = the
    stale 99.0 row from before this release still present.
    """
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id, stations = _build_site(conn, pws_ids=STATION_PWS_IDS[1:3])
    b_id = stations[STATION_PWS_IDS[1]]

    for hour in range(24):
        if hour == 15:
            continue
        _insert_reading(
            conn,
            b_id,
            datetime(
                WRITE_DAY.year, WRITE_DAY.month, WRITE_DAY.day, hour, 0, tzinfo=UTC
            ),
            "five_minute",
            temp_c=10.0,
            speed_kmh=5.0,
            precip_total_mm=0.0,
        )

    hour15 = isoformat_utc(_hour_at(15))
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)"
        " VALUES (?, 'temperature', ?, 99.0, 'ok', 'stale-old-row', ?)",
        (b_id, hour15, hour15),
    )
    conn.execute(
        "INSERT INTO observations"
        " (site_id, variable, valid_at, value, n_stations, computed_at)"
        " VALUES (?, 'temperature', ?, 99.0, 1, ?)",
        (site_id, hour15, hour15),
    )

    result = write_hourly_values(
        conn, site_id=site_id, local_date=WRITE_DAY, now_utc=NOW
    )
    assert result.outcome == "written"
    assert result.rows_removed == 1

    station_row = conn.execute(
        "SELECT 1 FROM station_observations"
        " WHERE station_id=? AND variable='temperature' AND valid_at=?",
        (b_id, hour15),
    ).fetchone()
    assert station_row is None

    site_row = conn.execute(
        "SELECT 1 FROM observations"
        " WHERE site_id=? AND variable='temperature' AND valid_at=?",
        (site_id, hour15),
    ).fetchone()
    assert site_row is None


# ---------------------------------------------------------------------------
# DV3
# ---------------------------------------------------------------------------


def test_dv3_writing_in_time_order_lets_the_jump_check_see_the_hour_before(
    tmp_path: Path,
) -> None:
    """DV3 (plan 14.9): temperature 10.0 every hour except 30.0 at hour 11.

    Kills M57 (write_hourly_values writes in reverse time order): at the
    hour-11-and-12 qc_flag assertion, correct = both ``spike`` (hour 11's
    jump from 10.0 to 30.0, and hour 12's drop back, each exceed the 15 C
    spike limit against the hour actually before them), mutant = both
    ``ok`` (processing hours newest-first means every ``valid_at <
    this-hour`` lookup in ``write_station_observation`` finds nothing yet
    written, so ``previous_value`` is always ``None``).
    """
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id, stations = _build_site(conn, pws_ids=STATION_PWS_IDS[1:3])
    b_id = stations[STATION_PWS_IDS[1]]

    for hour in range(24):
        temp_c = 30.0 if hour == 11 else 10.0
        _insert_reading(
            conn,
            b_id,
            datetime(
                WRITE_DAY.year, WRITE_DAY.month, WRITE_DAY.day, hour, 0, tzinfo=UTC
            ),
            "five_minute",
            temp_c=temp_c,
            speed_kmh=5.0,
            precip_total_mm=0.0,
        )

    result = write_hourly_values(
        conn, site_id=site_id, local_date=WRITE_DAY, now_utc=NOW
    )
    assert result.outcome == "written"

    rows = conn.execute(
        "SELECT valid_at, qc_flag FROM station_observations"
        " WHERE station_id=? AND variable='temperature' ORDER BY valid_at",
        (b_id,),
    ).fetchall()
    flags = {row["valid_at"]: row["qc_flag"] for row in rows}
    assert flags[isoformat_utc(_hour_at(11))] == "spike"
    assert flags[isoformat_utc(_hour_at(12))] == "spike"
    for hour in range(24):
        if hour in (11, 12):
            continue
        assert flags[isoformat_utc(_hour_at(hour))] == "ok", hour


# ---------------------------------------------------------------------------
# DV4
# ---------------------------------------------------------------------------


def test_dv4_a_closed_day_refuses_the_write_and_changes_nothing(
    tmp_path: Path,
) -> None:
    """DV4 (plan 14.9): WRITE_DAY closed (not closing).

    Kills M58 (write_hourly_values skips P1's guard): at the
    station_observations-row-unchanged assertion, correct = the
    pre-existing row's value untouched (81.0), mutant = overwritten by the
    method's own computed value (10.0) for an hour the guard should have
    refused outright.
    """
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id, stations = _build_site(
        conn, pws_ids=STATION_PWS_IDS[1:3], close_write_day=False
    )
    b_id = stations[STATION_PWS_IDS[1]]
    begin_close(conn, site_id, WRITE_DAY, now_utc=NOW)
    finish_close(conn, site_id, WRITE_DAY, now_utc=NOW)

    for hour in range(24):
        _insert_reading(
            conn,
            b_id,
            datetime(
                WRITE_DAY.year, WRITE_DAY.month, WRITE_DAY.day, hour, 0, tzinfo=UTC
            ),
            "five_minute",
            temp_c=10.0,
            speed_kmh=5.0,
            precip_total_mm=0.0,
        )
    hour5 = isoformat_utc(_hour_at(5))
    conn.execute(
        "INSERT INTO station_observations"
        " (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)"
        " VALUES (?, 'temperature', ?, 81.0, 'ok', 'kept-from-before-close', ?)",
        (b_id, hour5, hour5),
    )

    before_jobs = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type = 'pair_and_score'"
    ).fetchone()[0]

    result = write_hourly_values(
        conn, site_id=site_id, local_date=WRITE_DAY, now_utc=NOW
    )

    assert result.outcome == "refused"
    assert result.rows_written == 0
    assert result.rows_removed == 0
    assert result.hours_materialized == 0

    kept = conn.execute(
        "SELECT value, source_raw FROM station_observations"
        " WHERE station_id=? AND variable='temperature' AND valid_at=?",
        (b_id, hour5),
    ).fetchone()
    assert kept is not None
    assert kept["value"] == pytest.approx(81.0)
    assert kept["source_raw"] == "kept-from-before-close"

    total_rows = conn.execute(
        "SELECT COUNT(*) FROM station_observations WHERE station_id=?", (b_id,)
    ).fetchone()[0]
    assert total_rows == 1

    after_jobs = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type = 'pair_and_score'"
    ).fetchone()[0]
    assert after_jobs == before_jobs


# ---------------------------------------------------------------------------
# DV5
# ---------------------------------------------------------------------------


def test_dv5_a_rerun_on_unchanged_readings_changes_no_row(tmp_path: Path) -> None:
    """DV5 (plan 14.9): write_hourly_values twice on the same readings.

    No mutant of 15.2 is named for DV5 (14.11's footnote: its fault is
    already made by the mutants above); this pins the idempotency
    contract directly (7.7: "a rerun on unchanged readings changes no
    row"), as a regression guard for whatever makes the writes above
    non-idempotent.
    """
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id, stations = _build_site(conn, pws_ids=STATION_PWS_IDS[1:3])
    b_id = stations[STATION_PWS_IDS[1]]
    d_id = stations[STATION_PWS_IDS[2]]

    _seed_full_day(
        conn,
        b_id,
        minutes=tuple(range(0, 60, 5)),
        temp_c=11.25,
        speed_kmh=15.5,
        rain_step_mm=0.4,
    )
    _seed_full_day(
        conn,
        d_id,
        minutes=(0, 15, 30, 45),
        temp_c=9.0,
        speed_kmh=8.0,
        rain_step_mm=0.2,
    )

    first = write_hourly_values(
        conn, site_id=site_id, local_date=WRITE_DAY, now_utc=NOW
    )
    assert first.outcome == "written"
    assert first.rows_written == 144

    before_rows = conn.execute(
        "SELECT station_id, variable, valid_at, value, qc_flag, source_raw"
        " FROM station_observations WHERE station_id IN (?, ?)"
        " ORDER BY station_id, variable, valid_at",
        (b_id, d_id),
    ).fetchall()

    second = write_hourly_values(
        conn,
        site_id=site_id,
        local_date=WRITE_DAY,
        now_utc=NOW + timedelta(minutes=5),
    )
    assert second.outcome == "written"
    assert second.rows_written == 0
    assert second.rows_removed == 0
    assert second.hours_materialized == 0

    after_rows = conn.execute(
        "SELECT station_id, variable, valid_at, value, qc_flag, source_raw"
        " FROM station_observations WHERE station_id IN (?, ?)"
        " ORDER BY station_id, variable, valid_at",
        (b_id, d_id),
    ).fetchall()
    assert [tuple(row) for row in after_rows] == [tuple(row) for row in before_rows]


# ---------------------------------------------------------------------------
# DV6
# ---------------------------------------------------------------------------


def test_dv6_day_presence_tells_a_refusal_from_a_silent_station(
    tmp_path: Path,
) -> None:
    """DV6 (plan 14.9): B has readings; A answered with only 204s all day;
    E's every try was held.

    Kills M59 (day_presence reports ``answered`` from any try, held ones
    included): at E's ``answered`` assertion, correct = False (E's day row
    has no ``last_ok_at``: every try was held, none answered), mutant =
    True (it derives ``answered`` from E's ``attempts`` count -- which is
    1, because a held try still records an attempt -- instead of from
    ``last_ok_at``).
    """
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id, stations = _build_site(
        conn, pws_ids=(STATION_PWS_IDS[0], STATION_PWS_IDS[1], STATION_PWS_IDS[3])
    )
    a_id = stations[STATION_PWS_IDS[0]]
    b_id = stations[STATION_PWS_IDS[1]]
    e_id = stations[STATION_PWS_IDS[3]]

    _insert_reading(
        conn,
        b_id,
        _hour_at(0),
        "five_minute",
        temp_c=10.0,
        speed_kmh=5.0,
        precip_total_mm=0.0,
    )
    _insert_reading(
        conn,
        b_id,
        _hour_at(0) + timedelta(minutes=5),
        "five_minute",
        temp_c=10.1,
        speed_kmh=5.5,
        precip_total_mm=0.0,
    )
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, last_ok_at, refused_at,"
        "  updated_at)"
        " VALUES (?, ?, 'fetched', 1, ?, NULL, ?)",
        (b_id, WRITE_DAY.isoformat(), isoformat_utc(NOW), isoformat_utc(NOW)),
    )

    # A: every try answered (2xx, 204 included) but produced no readings.
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, last_ok_at, refused_at,"
        "  updated_at)"
        " VALUES (?, ?, 'fetched', 3, ?, NULL, ?)",
        (a_id, WRITE_DAY.isoformat(), isoformat_utc(NOW), isoformat_utc(NOW)),
    )

    # E: every try held or refused; never answered.
    refused_stamp = isoformat_utc(NOW - timedelta(hours=2))
    conn.execute(
        "INSERT INTO station_wind_days"
        " (station_id, local_date, status, attempts, last_ok_at, refused_at,"
        "  updated_at)"
        " VALUES (?, ?, 'unavailable', 1, NULL, ?, ?)",
        (e_id, WRITE_DAY.isoformat(), refused_stamp, isoformat_utc(NOW)),
    )

    presence = day_presence(conn, site_id=site_id, local_date=WRITE_DAY)

    assert presence[b_id].readings["five_minute"] == 2
    assert presence[b_id].readings["current_obs"] == 0
    assert presence[b_id].readings["hourly"] == 0

    assert presence[a_id].readings == {"five_minute": 0, "current_obs": 0, "hourly": 0}
    assert presence[a_id].answered is True
    assert presence[a_id].refused_at is None

    assert presence[e_id].readings == {"five_minute": 0, "current_obs": 0, "hourly": 0}
    assert presence[e_id].answered is False
    assert presence[e_id].refused_at == parse_utc(refused_stamp)
