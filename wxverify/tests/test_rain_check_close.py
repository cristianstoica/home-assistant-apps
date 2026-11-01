"""The rain check at the database level, through close stage 3 (plan
2026-11-01-rain-release.md, Part A; test IDs A1, A7).

A2-A6 are covered purely, at the ``obs/rain_check`` level, in
``tests/test_rain_check_rules.py``; this file only needs what a real close
proves: a real ``site_day_stations`` list row and a real ``observations``
row. Real SQLite throughout (``tests.helpers.asof_conn``, the real
``wxverify.db.site_days``/``wxverify.worker.day_values`` writers); nothing
is mocked. Synthetic data only: 6 stations A-F (``RAINSTA``-``RAINSTF``),
site name ``rain-release-site``, zone ``UTC``, dates from
``tests.helpers.D`` (2024-06-10) onward.

Written before ``wxverify.worker.rain_check`` (``check_station_rain``) and
the ``rain_counted``/``day_rain_station_ids`` additions exist: every test
here fails -- at ``ImportError``, or at ``sqlite3.OperationalError: no such
column: rain_counted`` / ``no such table: station_rain_checks`` -- until
the migration and the worker module are built.

Assumed API, for the builder to match:

    # wxverify/worker/rain_check.py
    def check_station_rain(
        conn: sqlite3.Connection, *, site_id: int, local_date: date,
        now_utc: datetime,
    ) -> None: ...

    # wxverify/db/site_days.py, next to day_station_ids
    def day_rain_station_ids(
        conn: sqlite3.Connection, site_id: int, local_date: date,
    ) -> frozenset[int]: ...
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from tests.helpers import D, asof_conn
from wxverify.core.timeutil import isoformat_utc, isoformat_utc_micro, parse_utc
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.site_days import (
    DAILY_CLOSE_ON_KEY,
    begin_close,
    day_rain_station_ids,
    day_station_ids,
    ensure_site_days,
    finish_close,
    start_site_days,
)
from wxverify.db.switch import ensure_site_generation
from wxverify.worker.day_values import write_hourly_values
from wxverify.worker.rain_check import check_station_rain

_STATION_NAMES: tuple[str, ...] = ("A", "B", "C", "D", "E", "F")
_WET_HOUR = 10


def _asof_site(conn, name: str) -> int:
    """A bare site row (mirrors ``tests.helpers.asof_make_site``, kept
    local to this file per the no-shared-helper-edits rule)."""
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 40.0, -105.0, 900.0, 'UTC')",
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _create_site_and_stations(conn) -> tuple[int, dict[str, int], date]:
    """Site ``rain-release-site`` with 6 stations A-F, all effective
    before ``day1``. No ``site_days`` records yet -- callers that need to
    seed a station_rain_checks row dated before day1 (A7b) must do so
    before calling :func:`_open_site_days`.
    """
    site_id = _asof_site(conn, "rain-release-site")
    ensure_site_generation(conn, site_id)
    day1 = D
    seed_date = (day1 - timedelta(days=1)).isoformat()
    seed_changed_at = isoformat_utc_micro(
        datetime(day1.year, day1.month, day1.day, tzinfo=UTC) - timedelta(days=1)
    )
    stations: dict[str, int] = {}
    for letter in _STATION_NAMES:
        cur = conn.execute(
            "INSERT INTO stations"
            " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
            " VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
            (site_id, f"RAINST{letter}"),
        )
        assert cur.lastrowid is not None
        station_id = int(cur.lastrowid)
        conn.execute(
            "INSERT INTO station_changes"
            " (site_id, station_id, change, effective_date, reason, changed_at)"
            " VALUES (?, ?, 'added', ?, 'test_seed', ?)",
            (site_id, station_id, seed_date, seed_changed_at),
        )
        stations[letter] = station_id
    return site_id, stations, day1


def _open_site_days(conn, site_id: int, day1: date, *, through: date) -> None:
    """Turn the close on and pre-create every record from ``day1`` through
    one day past ``through`` in one shot, so every date this file's tests
    touch already has a ``site_day_stations`` row (default
    ``rain_counted = 1``) before any close runs -- ``check_station_rain``'s
    own step 3 (a plain ``UPDATE ... WHERE local_date > D``) is what is
    meant to flip it, not record-creation timing.
    """
    set_runtime_state(
        conn,
        DAILY_CLOSE_ON_KEY,
        isoformat_utc(datetime(day1.year, day1.month, day1.day, tzinfo=UTC)),
    )
    day1_start = datetime(day1.year, day1.month, day1.day, tzinfo=UTC)
    start_site_days(conn, site_id, day1, now_utc=day1_start)
    ensure_site_days(
        conn,
        site_id,
        now_utc=datetime(through.year, through.month, through.day, 12, tzinfo=UTC),
    )
    conn.commit()


def _build_site(conn, *, through: date) -> tuple[int, dict[str, int], date]:
    site_id, stations, day1 = _create_site_and_stations(conn)
    _open_site_days(conn, site_id, day1, through=through)
    return site_id, stations, day1


def _insert_reading(
    conn,
    station_id: int,
    when: datetime,
    *,
    temp_c: float,
    precip_total_mm: float,
    speed_kmh: float = 5.0,
) -> None:
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, local_date, speed_kmh, temp_c,"
        "  precip_total_mm, fetched_at)"
        " VALUES (?, ?, 'five_minute', ?, ?, ?, ?, ?)",
        (
            station_id,
            isoformat_utc(when),
            when.date().isoformat(),
            speed_kmh,
            temp_c,
            round(precip_total_mm, 2),
            isoformat_utc(when),
        ),
    )


def _seed_day(
    conn,
    stations: dict[str, int],
    local_date: date,
    rain_mm: dict[str, float],
    *,
    wet_hour: int = _WET_HOUR,
    temp_c: float = 20.0,
) -> None:
    """Every station gets a full day of readings (2 per hour, so a real
    wind pair-max forms too): all dry except a single rise of
    ``rain_mm[letter]`` at ``wet_hour``, same for every station not named.
    """
    for letter, station_id in stations.items():
        total = 0.0
        rise = rain_mm.get(letter, 0.0)
        for hour in range(24):
            if hour == wet_hour:
                total += rise
            for minute in (0, 15):
                when = datetime(
                    local_date.year,
                    local_date.month,
                    local_date.day,
                    hour,
                    minute,
                    tzinfo=UTC,
                )
                _insert_reading(
                    conn, station_id, when, temp_c=temp_c, precip_total_mm=total
                )
    conn.commit()


def _day_bounds(local_date: date) -> tuple[str, str]:
    start = f"{local_date.isoformat()}T00:00:00Z"
    end = f"{(local_date + timedelta(days=1)).isoformat()}T00:00:00Z"
    return start, end


def _observations_snapshot(
    conn, site_id: int, local_date: date, variables: tuple[str, ...] | None = None
) -> list[tuple[object, ...]]:
    start, end = _day_bounds(local_date)
    if variables is None:
        return [
            tuple(row)
            for row in conn.execute(
                "SELECT variable, valid_at, value, n_stations, rejected_stations"
                " FROM observations"
                " WHERE site_id = ? AND valid_at >= ? AND valid_at < ?"
                " ORDER BY variable, valid_at",
                (site_id, start, end),
            )
        ]
    placeholders = ", ".join("?" for _ in variables)
    query = (
        "SELECT variable, valid_at, value, n_stations, rejected_stations"
        " FROM observations"
        f" WHERE site_id = ? AND variable IN ({placeholders})"  # noqa: S608 -- `?` placeholders only
        " AND valid_at >= ? AND valid_at < ?"
        " ORDER BY variable, valid_at"
    )
    return [
        tuple(row) for row in conn.execute(query, (site_id, *variables, start, end))
    ]


def _day_scores_snapshot(
    conn, site_id: int, local_date: date
) -> list[tuple[object, ...]]:
    return [
        tuple(row)
        for row in conn.execute(
            "SELECT * FROM day_scores WHERE site_id = ? AND local_date = ?"
            " ORDER BY feed_id, variable, day_ahead",
            (site_id, local_date.isoformat()),
        )
    ]


def _rain_check_snapshot(
    conn, site_id: int, local_date: date
) -> list[tuple[object, ...]]:
    """Every substantive column of the day's ``station_rain_checks`` rows,
    deliberately excluding ``checked_at`` -- a retry is free to re-stamp
    its own clock reading; the rows that matter are everything else.
    """
    return [
        tuple(row)
        for row in conn.execute(
            "SELECT station_id, outcome, reason, station_mm, peer_median_mm,"
            "  peers, decision, counted_after"
            " FROM station_rain_checks WHERE site_id = ? AND local_date = ?"
            " ORDER BY station_id",
            (site_id, local_date.isoformat()),
        )
    ]


def _site_day_stations_snapshot(
    conn, site_id: int, local_date: date
) -> list[tuple[int, int]]:
    return [
        (int(row[0]), int(row[1]))
        for row in conn.execute(
            "SELECT station_id, rain_counted FROM site_day_stations"
            " WHERE site_id = ? AND local_date = ? ORDER BY station_id",
            (site_id, local_date.isoformat()),
        )
    ]


def _begin_and_close(conn, site_id: int, local_date: date) -> datetime:
    """``begin_close`` -> ``write_hourly_values`` -> ``check_station_rain``
    -> ``finish_close`` for one already-seeded, already-listed day.
    Returns the ``now_utc`` it used.
    """
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, local_date.isoformat()),
    ).fetchone()
    assert row is not None, f"no open record for {local_date.isoformat()}"
    now = parse_utc(row[0]) + timedelta(minutes=30)
    begin_close(conn, site_id, local_date, now_utc=now)
    write_hourly_values(conn, site_id=site_id, local_date=local_date, now_utc=now)
    check_station_rain(conn, site_id=site_id, local_date=local_date, now_utc=now)
    finish_close(conn, site_id, local_date, now_utc=now)
    conn.commit()
    return now


def _seed_site_temps_before(conn, site_id: int, day1: date) -> None:
    """48 hours of made-up site temperature (15 °C, no frost) before ``day1``.

    Rule 7 reads the site's ``observations`` temperature for the 48 hours
    before the first wet hour; the site's first day is ``day1``, so without
    these rows day1 and day2 would be ``cant_judge (cold)``.
    """
    day1_start = datetime(day1.year, day1.month, day1.day, tzinfo=UTC)
    for back in range(1, 49):
        conn.execute(
            "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
            " VALUES (?, 'temperature', ?, 15.0, 5)",
            (site_id, isoformat_utc(day1_start - timedelta(hours=back))),
        )
    conn.commit()


def _seed_prior_set_aside(conn, site_id: int, station_id: int, before: date) -> None:
    """A plain INSERT of one ``station_rain_checks`` row dated before the
    site's first day, already set aside -- what ``_write_station_list``
    reads to seed a brand-new day's ``rain_counted`` (plan A1's "or 1 when
    there is none").
    """
    conn.execute(
        "INSERT INTO station_rain_checks"
        " (site_id, local_date, station_id, outcome, reason, station_mm,"
        "  peer_median_mm, peers, decision, counted_after, checked_at)"
        " VALUES (?, ?, ?, 'zero', NULL, 0.0, 3.0, 5, 'set_aside', 0, ?)",
        (
            site_id,
            before.isoformat(),
            station_id,
            isoformat_utc(datetime(before.year, before.month, before.day, tzinfo=UTC)),
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# A1
# ---------------------------------------------------------------------------


def test_a1_third_zero_day_in_two_spells_sets_a_aside() -> None:
    """2 zero days (spell 1), a dry day, then a 3rd zero day (spell 2):
    A is set aside at that close. The NEXT day's list row drops A from
    the rain-counted stations (B-F stay); the judged day's own
    ``observations``/``day_scores`` rows are untouched by the check."""
    conn = asof_conn()
    site_id, stations, day1 = _build_site(conn, through=D + timedelta(days=10))
    _seed_site_temps_before(conn, site_id, day1)
    a_id = stations["A"]
    day2 = day1 + timedelta(days=1)
    day3 = day1 + timedelta(days=2)
    day4 = day1 + timedelta(days=3)
    day5 = day1 + timedelta(days=4)

    wet_bf = {"B": 3.0, "C": 3.0, "D": 3.0, "E": 3.0, "F": 3.0, "A": 0.0}
    dry_all = dict.fromkeys(_STATION_NAMES, 0.0)

    _seed_day(conn, stations, day1, wet_bf)
    _begin_and_close(conn, site_id, day1)

    _seed_day(conn, stations, day2, wet_bf)
    _begin_and_close(conn, site_id, day2)

    _seed_day(conn, stations, day3, dry_all)
    _begin_and_close(conn, site_id, day3)

    _seed_day(conn, stations, day4, wet_bf)
    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, day4.isoformat()),
    ).fetchone()
    now4 = parse_utc(row[0]) + timedelta(minutes=30)
    begin_close(conn, site_id, day4, now_utc=now4)
    write_hourly_values(conn, site_id=site_id, local_date=day4, now_utc=now4)
    before_obs = _observations_snapshot(conn, site_id, day4)
    before_scores = _day_scores_snapshot(conn, site_id, day4)

    check_station_rain(conn, site_id=site_id, local_date=day4, now_utc=now4)

    after_obs = _observations_snapshot(conn, site_id, day4)
    after_scores = _day_scores_snapshot(conn, site_id, day4)
    assert after_obs == before_obs, (
        "the judged day's own observations must be unchanged by the check"
    )
    assert before_scores == []
    assert after_scores == before_scores

    finish_close(conn, site_id, day4, now_utc=now4)
    conn.commit()

    # A is still counted for day4 itself -- the change is forward-only.
    assert a_id in day_rain_station_ids(conn, site_id, day4)

    # The NEXT day's list drops A from the rain-counted stations.
    day5_rain_ids = day_rain_station_ids(conn, site_id, day5)
    day5_all_ids = day_station_ids(conn, site_id, day5)
    assert a_id not in day5_rain_ids
    assert a_id in day5_all_ids, "temperature/wind still read every listed station"
    for letter in ("B", "C", "D", "E", "F"):
        assert stations[letter] in day5_rain_ids
    row = conn.execute(
        "SELECT rain_counted FROM site_day_stations"
        " WHERE site_id = ? AND local_date = ? AND station_id = ?",
        (site_id, day5.isoformat(), a_id),
    ).fetchone()
    assert row is not None
    assert int(row[0]) == 0

    # That day's site rain is the median of B-F, with A's own gauge (an
    # extreme, but inlier-range, value) never entering the query at all:
    # rejected_stations == 0 is the discriminating check -- a filter that
    # forgot the rain_counted clause would let A's value in and the
    # MAD estimator would then itself reject it as an outlier, landing on
    # the same value and station count but rejected_stations == 1.
    _seed_day(
        conn,
        stations,
        day5,
        {"A": 50.0, "B": 2.8, "C": 2.9, "D": 3.0, "E": 3.1, "F": 3.2},
    )
    _begin_and_close(conn, site_id, day5)
    wet_at = isoformat_utc(
        datetime(day5.year, day5.month, day5.day, _WET_HOUR, tzinfo=UTC)
    )
    obs_row = conn.execute(
        "SELECT value, n_stations, rejected_stations FROM observations"
        " WHERE site_id = ? AND variable = 'precip' AND valid_at = ?",
        (site_id, wet_at),
    ).fetchone()
    assert obs_row is not None
    value, n_stations, rejected_stations = obs_row
    assert value == pytest.approx(3.0)
    assert n_stations == 5
    assert rejected_stations == 0


# ---------------------------------------------------------------------------
# A7
# ---------------------------------------------------------------------------


def test_a7_stage3_retry_gives_the_same_rows() -> None:
    """A stage-3 retry (``write_hourly_values`` + ``check_station_rain``
    run again for the same day, as a real retried stage-3 job would)
    produces the same ``station_rain_checks`` rows and the same later
    ``site_day_stations`` list row as the first run."""
    conn = asof_conn()
    site_id, stations, day1 = _build_site(conn, through=D + timedelta(days=2))
    day2 = day1 + timedelta(days=1)
    _seed_day(conn, stations, day1, dict.fromkeys(_STATION_NAMES, 0.0))

    row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, day1.isoformat()),
    ).fetchone()
    now = parse_utc(row[0]) + timedelta(minutes=30)
    begin_close(conn, site_id, day1, now_utc=now)
    write_hourly_values(conn, site_id=site_id, local_date=day1, now_utc=now)
    check_station_rain(conn, site_id=site_id, local_date=day1, now_utc=now)
    conn.commit()
    first_checks = _rain_check_snapshot(conn, site_id, day1)
    first_list = _site_day_stations_snapshot(conn, site_id, day2)
    assert first_checks != [], "the seeded day must actually produce rows"

    # The retry: both stage-3 writers run again for the same day, same now.
    write_hourly_values(conn, site_id=site_id, local_date=day1, now_utc=now)
    check_station_rain(conn, site_id=site_id, local_date=day1, now_utc=now)
    conn.commit()
    second_checks = _rain_check_snapshot(conn, site_id, day1)
    second_list = _site_day_stations_snapshot(conn, site_id, day2)

    assert second_checks == first_checks
    assert second_list == first_list


def test_a7_temperature_and_wind_identical_with_and_without_set_aside() -> None:
    """The same day's temperature and wind consensus are identical whether
    A is counted or already set aside -- Part A only ever filters
    ``precip``."""
    rain_mm = {"A": 0.0, "B": 3.0, "C": 3.0, "D": 3.0, "E": 3.0, "F": 3.0}

    conn1 = asof_conn()
    site1, stations1, day1 = _build_site(conn1, through=D + timedelta(days=1))
    _seed_day(conn1, stations1, day1, rain_mm)
    _begin_and_close(conn1, site1, day1)
    other1 = _observations_snapshot(conn1, site1, day1, ("temperature", "wind"))

    conn2 = asof_conn()
    site2, stations2, day1b = _create_site_and_stations(conn2)
    assert day1b == day1
    _seed_prior_set_aside(conn2, site2, stations2["A"], day1 - timedelta(days=1))
    _open_site_days(conn2, site2, day1, through=D + timedelta(days=1))
    _seed_day(conn2, stations2, day1, rain_mm)
    _begin_and_close(conn2, site2, day1)
    other2 = _observations_snapshot(conn2, site2, day1, ("temperature", "wind"))

    # Sanity: the two sites really do differ on A's rain_counted for day1
    # -- this is the paired positive that the comparison below is not
    # vacuously comparing two identical setups.
    assert stations1["A"] in day_rain_station_ids(conn1, site1, day1)
    assert stations2["A"] not in day_rain_station_ids(conn2, site2, day1)

    assert other2 == other1
