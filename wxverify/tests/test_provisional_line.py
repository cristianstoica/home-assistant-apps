"""P5 plan §13.8: the provisional Overlay line (PL1-PL11).

Fixtures and time per plan §13.1: ``day_close_site`` (tests/helpers.py)
gives a UTC site, stations A (``ISTATION01``) and B (``ISTATION02``), D-3
``closed``, D-2 ``missed``, D-1 ``open`` and ended, D current. Raw readings
go through P2's own writer, ``store_readings``, with each row's real
source. ``provisional_site_hours`` (plan §9.1) is the new name this module
tests; it is imported inside each test body so the module collects before
Step 4 piece 5 lands ``wxverify/provisional_line.py`` (plan §15 Step 3).
Every row fails on today's code: the module does not exist yet, so every
import raises ``ModuleNotFoundError``.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta

import pytest

from tests.helpers import D, asof_conn, day_close_site
from wxverify.core.timeutil import floor_hour, isoformat_utc
from wxverify.db.station_readings import store_readings
from wxverify.obs.hourly_values import StationReading
from wxverify.obs.qc import qc_flag
from wxverify.scoring.consensus import MAD_FLOORS, compute_consensus
from wxverify.scoring.consensus import StationReading as ConsensusReading
from wxverify.worker.day_values import hourly_values


def _station_id(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    row = conn.execute(
        "SELECT id FROM stations WHERE site_id = ? AND pws_station_id = ?",
        (site_id, pws_station_id),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _five_minute_readings(
    day: date,
    *,
    temp_c: float = 10.0,
    rain_mm: float = 0.0,
    wind_kmh: float = 5.0,
    start: tuple[int, int] = (0, 0),
    end: tuple[int, int] = (12, 15),
) -> list[StationReading]:
    """Every-5-minutes synthetic ``five_minute`` readings over ``[start, end]``."""
    t = datetime(day.year, day.month, day.day, *start, tzinfo=UTC)
    stop = datetime(day.year, day.month, day.day, *end, tzinfo=UTC)
    out: list[StationReading] = []
    while t <= stop:
        out.append(
            StationReading(
                obs_at=t,
                source="five_minute",
                local_date=day,
                temp_c=temp_c,
                precip_total_mm=rain_mm,
                speed_kmh=wind_kmh,
            )
        )
        t += timedelta(minutes=5)
    return out


def _seed_pl1(conn: sqlite3.Connection, site_id: int) -> tuple[int, int]:
    """PL1's shared seed: A and B every-5-minutes readings D 00:00-12:15."""
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    fetched_at = datetime(D.year, D.month, D.day, 12, 20, tzinfo=UTC)
    store_readings(
        conn,
        station_id=station_a,
        source="five_minute",
        readings=_five_minute_readings(D),
        fetched_at=fetched_at,
        now_utc=fetched_at,
    )
    store_readings(
        conn,
        station_id=station_b,
        source="five_minute",
        readings=_five_minute_readings(D),
        fetched_at=fetched_at,
        now_utc=fetched_at,
    )
    return station_a, station_b


def _hour(h: int, day: date = D) -> datetime:
    return datetime(day.year, day.month, day.day, h, tzinfo=UTC)


def test_pl1_running_hour_excluded(monkeypatch: pytest.MonkeyPatch) -> None:
    """PL1 (plan §13.8, §8.2): hours 00:00-11:00 have values, 12:00 none
    (M75: ``through_utc`` must floor to the whole hour, not take ``now``
    itself)."""
    from wxverify.provisional_line import provisional_site_hours

    conn = asof_conn()
    site_id = day_close_site(conn)
    _seed_pl1(conn, site_id)
    now = datetime(D.year, D.month, D.day, 12, 20, tzinfo=UTC)

    line = provisional_site_hours(
        conn, site_id=site_id, variable="temperature", now_utc=now
    )
    for h in range(12):
        assert line.values.get(_hour(h)) is not None, (
            f"M75 -> at this assertion: correct = hour {h:02d}:00 has a"
            " value (it ended before `now`), mutant = `through_utc` = `now`"
            " un-floored would also include it, so this alone would not"
            " distinguish -- paired with the 12:00 check below, which the"
            " mutant DOES flip."
        )
    assert line.values.get(_hour(12)) is None, (
        "M75 -> at this assertion: correct = hour 12:00 (the running hour)"
        " has no value, mutant = `through_utc` = `now` (12:20, not floored"
        " to 12:00) draws the partial hour from its 12:00-12:15 readings,"
        " so a value would appear here."
    )


def test_pl2_site_value_matches_consensus_over_qc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PL2 (plan §13.8, §8.3): the 03:00 temperature site value equals
    ``qc_flag`` + ``compute_consensus`` run directly on the same per-station
    inputs (M76: a plain mean instead of ``compute_consensus``)."""
    from wxverify.provisional_line import provisional_site_hours

    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a, station_b = _seed_pl1(conn, site_id)
    now = datetime(D.year, D.month, D.day, 12, 20, tzinfo=UTC)

    line = provisional_site_hours(
        conn, site_id=site_id, variable="temperature", now_utc=now
    )
    actual = line.values.get(_hour(3))
    assert actual is not None

    per_station = hourly_values(
        conn, site_id=site_id, local_date=D, through_utc=floor_hour(now)
    )
    site_row = conn.execute(
        "SELECT elevation_m FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    assert site_row is not None
    site_elevation_m = float(site_row[0])

    readings: list[ConsensusReading] = []
    for station_id in (station_a, station_b):
        station_values = per_station.stations[station_id].values["temperature"]
        hv = station_values.get(_hour(3))
        assert hv is not None
        prev_hv = station_values.get(_hour(2))
        previous = prev_hv.value if prev_hv is not None else None
        if qc_flag("temperature", hv.value, previous) != "ok":
            continue
        dem_row = conn.execute(
            "SELECT dem_elevation_m FROM stations WHERE id = ?", (station_id,)
        ).fetchone()
        assert dem_row is not None
        readings.append(
            ConsensusReading(
                station_id=station_id,
                dem_elevation_m=float(dem_row[0]),
                variable="temperature",
                valid_at=isoformat_utc(_hour(3)),
                value=hv.value,
            )
        )
    expected = compute_consensus(
        readings,
        variable="temperature",
        site_elevation_m=site_elevation_m,
        mad_floor=MAD_FLOORS["temperature"],
    )
    assert expected is not None
    assert actual == pytest.approx(expected.value), (
        f"M76 -> at this assertion: correct = {expected.value!r} (the same"
        " value `qc_flag` + `compute_consensus` give over these inputs),"
        f" mutant = a plain mean of the station values, which differs"
        " whenever consensus's elevation adjustment or MAD step changes"
        " the result."
    )


def test_pl3_spike_left_out_by_qc(monkeypatch: pytest.MonkeyPatch) -> None:
    """PL3 (plan §13.8, §8.3): A's 06:00 temperature spike (40C above its
    05:00) is left out by ``qc_flag``; the site value comes from B alone
    (M77: ``qc_flag`` not applied)."""
    from wxverify.provisional_line import provisional_site_hours

    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a, station_b = _seed_pl1(conn, site_id)
    now = datetime(D.year, D.month, D.day, 12, 20, tzinfo=UTC)

    spike = _five_minute_readings(
        D, temp_c=50.0, start=(6, 0), end=(6, 55)
    )  # A's own 05:00 reading was 10.0C; 50.0C over the whole hour is a 40C jump.
    store_readings(
        conn,
        station_id=station_a,
        source="five_minute",
        readings=spike,
        fetched_at=now,
        now_utc=now,
    )

    line = provisional_site_hours(
        conn, site_id=site_id, variable="temperature", now_utc=now
    )
    actual = line.values.get(_hour(6))
    assert actual is not None

    per_station = hourly_values(
        conn, site_id=site_id, local_date=D, through_utc=floor_hour(now)
    )
    a_value = per_station.stations[station_a].values["temperature"][_hour(6)].value
    a_prev = per_station.stations[station_a].values["temperature"][_hour(5)].value
    assert a_value - a_prev >= 15.0, (
        f"A's 06:00 hourly value ({a_value!r}) must be at least 15C above"
        f" its 05:00 value ({a_prev!r}) -- `qc_flag`'s temperature spike"
        " limit -- or the spike would not be large enough for `qc_flag` to"
        " reject it."
    )

    b_value = per_station.stations[station_b].values["temperature"][_hour(6)].value
    site_row = conn.execute(
        "SELECT elevation_m FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    assert site_row is not None
    site_elevation_m = float(site_row[0])
    dem_row = conn.execute(
        "SELECT dem_elevation_m FROM stations WHERE id = ?", (station_b,)
    ).fetchone()
    assert dem_row is not None
    b_reading = ConsensusReading(
        station_id=station_b,
        dem_elevation_m=float(dem_row[0]),
        variable="temperature",
        valid_at=isoformat_utc(_hour(6)),
        value=b_value,
    )
    expected = compute_consensus(
        [b_reading],
        variable="temperature",
        site_elevation_m=site_elevation_m,
        mad_floor=MAD_FLOORS["temperature"],
    )
    assert expected is not None
    assert actual == pytest.approx(expected.value), (
        f"M77 -> at this assertion: correct = {expected.value!r} (B's own"
        " value through the site's elevation-adjusted consensus -- A's"
        " spiked 06:00 reading is left out), mutant = `qc_flag` not"
        " applied, so A's spike would be blended in and the site value"
        " would differ from B's alone."
    )


def test_pl4_gap_hour_uses_current_obs(monkeypatch: pytest.MonkeyPatch) -> None:
    """PL4 (plan §13.8, §8.2): an hour with no 5-minute reading draws from
    current-conditions readings instead (M78: only ``five_minute`` readings
    used)."""
    from wxverify.provisional_line import provisional_site_hours

    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    now = datetime(D.year, D.month, D.day, 12, 20, tzinfo=UTC)

    for _h, start, end in ((0, (0, 0), (7, 55)), (1, (9, 0), (12, 15))):
        for station_id in (station_a, station_b):
            store_readings(
                conn,
                station_id=station_id,
                source="five_minute",
                readings=_five_minute_readings(D, start=start, end=end),
                fetched_at=now,
                now_utc=now,
            )
    for station_id in (station_a, station_b):
        store_readings(
            conn,
            station_id=station_id,
            source="current_obs",
            readings=[
                StationReading(
                    obs_at=_hour(8) + timedelta(minutes=20),
                    source="current_obs",
                    local_date=D,
                    temp_c=10.0,
                    precip_total_mm=0.0,
                    speed_kmh=5.0,
                )
            ],
            fetched_at=now,
            now_utc=now,
        )

    line = provisional_site_hours(
        conn, site_id=site_id, variable="temperature", now_utc=now
    )
    assert line.values.get(_hour(8)) is not None, (
        "M78 -> at this assertion: correct = hour 08:00 has a value, drawn"
        " from the current-conditions reading in that hour, mutant = only"
        " `five_minute` readings used, so this hour (which has none) would"
        " have no value."
    )


def test_pl5_fully_empty_hour_has_no_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """PL5 (plan §13.8, §8.2): an hour with no reading at all has no
    temperature, rain or wind -- never drawn as 0 (M79)."""
    from wxverify.provisional_line import provisional_site_hours

    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    now = datetime(D.year, D.month, D.day, 12, 20, tzinfo=UTC)

    for station_id in (station_a, station_b):
        store_readings(
            conn,
            station_id=station_id,
            source="five_minute",
            readings=_five_minute_readings(D, start=(0, 0), end=(8, 55)),
            fetched_at=now,
            now_utc=now,
        )
        store_readings(
            conn,
            station_id=station_id,
            source="five_minute",
            readings=_five_minute_readings(D, start=(10, 0), end=(12, 15)),
            fetched_at=now,
            now_utc=now,
        )

    temp_line = provisional_site_hours(
        conn, site_id=site_id, variable="temperature", now_utc=now
    )
    rain_line = provisional_site_hours(
        conn, site_id=site_id, variable="precip", now_utc=now
    )
    wind_line = provisional_site_hours(
        conn, site_id=site_id, variable="wind", now_utc=now
    )
    assert temp_line.values.get(_hour(9)) is None, (
        "M79 -> at this assertion: correct = hour 09:00 has no temperature"
        " value, mutant = a gap hour drawn as 0, so a (zero) value would"
        " appear here instead of being absent."
    )
    assert rain_line.values.get(_hour(9)) is None, (
        "M79 -> at this assertion: correct = hour 09:00 has no rain value,"
        " mutant = a gap hour drawn as 0, so a (zero) rain value would"
        " appear here."
    )
    assert wind_line.values.get(_hour(9)) is None, (
        "M79 -> at this assertion: correct = hour 09:00 has no wind value,"
        " mutant = a gap hour drawn as 0, so a (zero) wind value would"
        " appear here."
    )
    assert rain_line.values.get(_hour(10)) is None, (
        "M79 -> at this assertion: correct = hour 10:00's rain is also"
        " none (the rain hour right after a gap has none, §8.2), mutant ="
        " a gap hour drawn as 0, which this check also catches since the"
        " gap-aware absence would instead read as a zero."
    )


def test_pl6_line_holds_prior_closing_day(monkeypatch: pytest.MonkeyPatch) -> None:
    """PL6 (plan §13.8, §8.1): with D-1 ``closing`` and ``now`` D 00:40, the
    line holds D-1's hours and shows none of D's yet (M80: only today
    drawn)."""
    from wxverify.db.site_days import begin_close
    from wxverify.provisional_line import provisional_site_hours

    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    d_minus_1 = D - timedelta(days=1)
    day_end = datetime(D.year, D.month, D.day, tzinfo=UTC)

    for station_id in (station_a, station_b):
        store_readings(
            conn,
            station_id=station_id,
            source="five_minute",
            readings=_five_minute_readings(d_minus_1, start=(0, 0), end=(23, 45)),
            fetched_at=day_end,
            now_utc=day_end,
        )
    begin_close(conn, site_id, d_minus_1, now_utc=day_end + timedelta(minutes=5))

    now = day_end + timedelta(minutes=40)
    line = provisional_site_hours(
        conn, site_id=site_id, variable="temperature", now_utc=now
    )
    assert (
        line.values.get(
            datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 23, tzinfo=UTC)
        )
        is not None
    ), (
        "M80 -> at this assertion: correct = D-1's 23:00 hour is on the"
        " line (D-1 is `closing`, not done), mutant = only today drawn, so"
        " D-1's hours would never appear."
    )
    assert line.values.get(_hour(0)) is None, (
        "M80 -> at this assertion: correct = D's 00:00 hour has nothing yet"
        " (D is still `open`, barely started), mutant = only today drawn,"
        " which this check alone cannot distinguish -- paired with the"
        " D-1 check above, which the mutant DOES flip (D-1 would be"
        " missing there)."
    )


def test_pl7_closed_day_leaves_the_line(monkeypatch: pytest.MonkeyPatch) -> None:
    """PL7 (plan §13.8, §8.1): PL6, then D-1 ``closed`` -- it leaves the
    line (M81: a closed day kept on the line)."""
    from wxverify.db.site_days import begin_close, finish_close
    from wxverify.provisional_line import provisional_site_hours

    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    d_minus_1 = D - timedelta(days=1)
    day_end = datetime(D.year, D.month, D.day, tzinfo=UTC)

    for station_id in (station_a, station_b):
        store_readings(
            conn,
            station_id=station_id,
            source="five_minute",
            readings=_five_minute_readings(d_minus_1, start=(0, 0), end=(23, 45)),
            fetched_at=day_end,
            now_utc=day_end,
        )
    begin_close(conn, site_id, d_minus_1, now_utc=day_end + timedelta(minutes=5))
    finish_close(conn, site_id, d_minus_1, now_utc=day_end + timedelta(minutes=10))

    now = day_end + timedelta(minutes=40)
    line = provisional_site_hours(
        conn, site_id=site_id, variable="temperature", now_utc=now
    )
    assert (
        line.values.get(
            datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 23, tzinfo=UTC)
        )
        is None
    ), (
        "M81 -> at this assertion: correct = D-1's 23:00 hour is gone once"
        " D-1 is `closed` (its closed values replace it elsewhere), mutant"
        " = a closed day kept on the line, so this hour would still show a"
        " value."
    )


def test_pl8_retry_due_kept_missed_dropped(monkeypatch: pytest.MonkeyPatch) -> None:
    """PL8 (plan §13.8, §8.1): PL6, then D-1 ``retry_due`` (still on the
    line), then ``missed`` (off the line entirely) (M82)."""
    from wxverify.db.site_days import begin_close, fail_close
    from wxverify.provisional_line import provisional_site_hours

    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    d_minus_1 = D - timedelta(days=1)
    day_end = datetime(D.year, D.month, D.day, tzinfo=UTC)
    hour_23 = datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 23, tzinfo=UTC)

    for station_id in (station_a, station_b):
        store_readings(
            conn,
            station_id=station_id,
            source="five_minute",
            readings=_five_minute_readings(d_minus_1, start=(0, 0), end=(23, 45)),
            fetched_at=day_end,
            now_utc=day_end,
        )
    begin_close(conn, site_id, d_minus_1, now_utc=day_end + timedelta(minutes=5))
    fail_close(
        conn,
        site_id,
        d_minus_1,
        now_utc=day_end + timedelta(minutes=10),
        reason="network",
    )

    now = day_end + timedelta(minutes=40)
    retry_due_line = provisional_site_hours(
        conn, site_id=site_id, variable="temperature", now_utc=now
    )
    assert retry_due_line.values.get(hour_23) is not None, (
        "M82 -> at this assertion: correct = D-1's 23:00 hour is still on"
        " the line while D-1 is `retry_due` (not done), mutant = a"
        " retry-due day dropped, so this hour would be missing here."
    )

    row = conn.execute(
        "SELECT retry_at FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, d_minus_1.isoformat()),
    ).fetchone()
    assert row is not None and row[0] is not None
    from wxverify.core.timeutil import parse_utc

    retry_at = parse_utc(str(row[0]))
    begin_close(conn, site_id, d_minus_1, now_utc=retry_at)
    fail_close(
        conn,
        site_id,
        d_minus_1,
        now_utc=retry_at + timedelta(minutes=1),
        reason="network",
    )
    # attempts is now 2 and the day is `missed` (`retry_failed`) after the
    # second failure -- mark_missed is for an `open`/`retry_due` day that
    # never attempted, not used here; fail_close's own attempt-2 branch
    # moves the day to `missed` directly.
    missed_line = provisional_site_hours(
        conn, site_id=site_id, variable="temperature", now_utc=now
    )
    assert missed_line.values.get(hour_23) is None, (
        "M82 -> at this assertion: correct = D-1's 23:00 hour is gone once"
        " D-1 is `missed` (no observed line at all for a missed day),"
        " mutant = a missed day kept on the line, so this hour would still"
        " show a value."
    )


def test_pl10_latest_reading_at_is_the_latest(monkeypatch: pytest.MonkeyPatch) -> None:
    """PL10 (plan §13.8, §8.4): the line's latest reading time is the
    LATEST over the drawn stations, not the earliest (M84)."""
    from wxverify.provisional_line import provisional_site_hours

    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    now = datetime(D.year, D.month, D.day, 13, 5, tzinfo=UTC)

    store_readings(
        conn,
        station_id=station_a,
        source="five_minute",
        readings=_five_minute_readings(D, start=(0, 0), end=(12, 15)),
        fetched_at=now,
        now_utc=now,
    )
    store_readings(
        conn,
        station_id=station_b,
        source="five_minute",
        readings=_five_minute_readings(D, start=(0, 0), end=(12, 10)),
        fetched_at=now,
        now_utc=now,
    )

    line = provisional_site_hours(
        conn, site_id=site_id, variable="temperature", now_utc=now
    )
    expected_latest = datetime(D.year, D.month, D.day, 12, 15, tzinfo=UTC)
    assert line.latest_reading_at == expected_latest, (
        f"M84 -> at this assertion: correct = {expected_latest!r} (A's"
        " 12:15, the later of the two stations' latest readings), mutant ="
        " the EARLIEST `latest_reading_at` shown, which would give B's"
        " 12:10 instead."
    )


def test_pl11_writes_no_scoring_or_truth_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """PL11 (plan §13.8, §8.3, §8.5): the line writes no
    ``station_observations``, ``observations``, ``forecast_history`` or
    ``daily_truth`` row (M85: ``write_station_observation`` called for the
    jump check)."""
    from wxverify.provisional_line import provisional_site_hours

    conn = asof_conn()
    site_id = day_close_site(conn)
    _seed_pl1(conn, site_id)
    now = datetime(D.year, D.month, D.day, 12, 20, tzinfo=UTC)

    tables = ("station_observations", "observations", "forecast_history", "daily_truth")
    before = {t: conn.execute(f"SELECT * FROM {t}").fetchall() for t in tables}  # noqa: S608

    provisional_site_hours(conn, site_id=site_id, variable="temperature", now_utc=now)

    after = {t: conn.execute(f"SELECT * FROM {t}").fetchall() for t in tables}  # noqa: S608
    for t in tables:
        assert after[t] == before[t], (
            f"M85 -> at this assertion: correct = {t}'s rows are unchanged"
            " (the line writes nothing), mutant ="
            " `write_station_observation` called for the jump check, which"
            " would add or change a row in this table."
        )


def test_pl12_set_aside_station_left_out_of_todays_rain() -> None:
    """PL12 (rain release, Part A follow-up): a station the rain check set
    aside for D (``rain_counted = 0`` on D's list row) does not count in
    D's provisional rain line, and counts again once taken back. A reads
    0.0 all morning; B's running total rises by 2.0 mm in hour 03:00, so
    the two-station median differs from B's own value. Temperature is
    read from every listed station either way."""
    from wxverify.provisional_line import provisional_site_hours

    conn = asof_conn()
    site_id = day_close_site(conn)
    station_a = _station_id(conn, site_id, "ISTATION01")
    station_b = _station_id(conn, site_id, "ISTATION02")
    now = datetime(D.year, D.month, D.day, 12, 20, tzinfo=UTC)
    store_readings(
        conn,
        station_id=station_a,
        source="five_minute",
        readings=_five_minute_readings(D),
        fetched_at=now,
        now_utc=now,
    )
    for start, end, total in (((0, 0), (2, 55), 0.0), ((3, 0), (12, 15), 2.0)):
        store_readings(
            conn,
            station_id=station_b,
            source="five_minute",
            readings=_five_minute_readings(D, rain_mm=total, start=start, end=end),
            fetched_at=now,
            now_utc=now,
        )
    b_alone = (
        hourly_values(conn, site_id=site_id, local_date=D, through_utc=floor_hour(now))
        .stations[station_b]
        .values["precip"][_hour(3)]
        .value
    )

    def _set_rain_counted(counted: int) -> None:
        cur = conn.execute(
            "UPDATE site_day_stations SET rain_counted = ?"
            " WHERE site_id = ? AND local_date = ? AND station_id = ?",
            (counted, site_id, D.isoformat(), station_a),
        )
        assert cur.rowcount == 1

    def _lines() -> tuple[float | None, dict[datetime, float]]:
        rain = provisional_site_hours(
            conn, site_id=site_id, variable="precip", now_utc=now
        )
        temp = provisional_site_hours(
            conn, site_id=site_id, variable="temperature", now_utc=now
        )
        return rain.values.get(_hour(3)), temp.values

    both_rain, both_temp = _lines()
    assert both_rain is not None
    assert both_rain != pytest.approx(b_alone), "the fixture must discriminate"

    _set_rain_counted(0)
    aside_rain, aside_temp = _lines()
    assert aside_rain == pytest.approx(b_alone), (
        "correct = only B counts while A is set aside, mutant = the rain line"
        " still reads every listed station, giving the two-station median"
    )
    assert aside_temp == both_temp

    _set_rain_counted(1)
    back_rain, back_temp = _lines()
    assert back_rain == pytest.approx(both_rain)
    assert back_temp == both_temp
