"""Raw retention (plan 2026-10-21-daily-close-download-and-raw-store.md
section 10, 12.1 and 14.10): ``prune_raw_readings``, tests RR1-RR4; and the
reads of 12.2/12.3 (``reading_times``, ``last_five_minute_store``,
``raw_readings``), tests RD1-RD3.

Readings of every source on D-3 to D: D-3 closed at a chosen instant, D-2
closed an hour later, D-1 and D left open unless a test moves them. The
site is built with ``with_records=False`` and P1's own functions
(``start_site_days``, ``ensure_site_days``, ``begin_close``, ``finish_close``,
``mark_missed``), since ``tests.helpers.day_close_site``'s own
``with_records=True`` path sets different close times and states than this
section needs. Every station id is synthetic (``ISTATION01``-``ISTATION06``);
the API key is the CI placeholder; zone is ``UTC`` throughout.

RD1-RD3 are pure reads with no day-state to set up, so they skip P1's
machinery and build their own minimal site (``asof_make_site``) and
stations (``_make_station``), using ``ISTATION04``-``ISTATION06`` to stay
clear of RR1-RR4's ``ISTATION01``-``ISTATION03``. Their readings are
inserted directly into ``station_wind_records`` (``_insert_raw_reading``),
with an explicit ``fetched_at`` independent of ``obs_at``, except RD2's
repeat store, which goes through the real ``store_readings``.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import (
    NOW,
    D,
    asof_make_site,
    close_off,
    day_close_site,
    note_day_clock,
)
from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import Database, close_db, init_db
from wxverify.db.runtime_state import get_runtime_state
from wxverify.db.site_days import (
    RAW_KEEP,
    begin_close,
    ensure_site_days,
    finish_close,
    mark_missed,
    start_site_days,
)
from wxverify.db.station_readings import prune_raw_readings

_ZONE = "UTC"
STATION_PWS_IDS: tuple[str, str] = ("ISTATION01", "ISTATION02")
D_MINUS_3: date = D - timedelta(days=3)
D_MINUS_2: date = D - timedelta(days=2)
D_MINUS_1: date = D - timedelta(days=1)


def _day_start(local_date: date) -> datetime:
    return datetime(local_date.year, local_date.month, local_date.day, tzinfo=UTC)


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


def _build_site(conn) -> tuple[int, dict[str, int]]:  # type: ignore[no-untyped-def]
    site_id = day_close_site(
        conn,
        timezone=_ZONE,
        close_on=True,
        with_records=False,
        station_ids=STATION_PWS_IDS,
    )
    stations: dict[str, int] = {}
    for pws_id in STATION_PWS_IDS:
        row = conn.execute(
            "SELECT id FROM stations WHERE pws_station_id=?", (pws_id,)
        ).fetchone()
        stations[pws_id] = int(row[0])

    start_site_days(
        conn,
        site_id,
        D_MINUS_3,
        now_utc=_day_start(D_MINUS_3),
    )
    ensure_site_days(conn, site_id, now_utc=NOW)
    return site_id, stations


def _close(  # type: ignore[no-untyped-def]
    conn, site_id: int, local_date: date, *, now_utc: datetime
) -> None:
    begin_close(conn, site_id, local_date, now_utc=now_utc)
    finish_close(conn, site_id, local_date, now_utc=now_utc)


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
            when.date().isoformat(),
            speed_kmh,
            temp_c,
            precip_total_mm,
            isoformat_utc(when),
        ),
    )


def _seed_day_every_source(
    conn,  # type: ignore[no-untyped-def]
    stations: dict[str, int],
    local_date: date,
) -> int:
    """One reading of every source, for both stations, on ``local_date``.

    Returns the row count inserted (2 stations x 3 sources = 6).
    """
    count = 0
    for station_id in stations.values():
        for hour, source in ((2, "five_minute"), (10, "current_obs"), (18, "hourly")):
            when = datetime(
                local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
            )
            _insert_reading(
                conn,
                station_id,
                when,
                source,
                temp_c=10.0,
                speed_kmh=5.0,
                precip_total_mm=0.1,
            )
            count += 1
    return count


def _count_between(  # type: ignore[no-untyped-def]
    conn, start: datetime, end: datetime
) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM station_wind_records WHERE obs_at >= ? AND obs_at < ?",
        (isoformat_utc(start), isoformat_utc(end)),
    ).fetchone()
    return int(row[0])


# ---------------------------------------------------------------------------
# RR1
# ---------------------------------------------------------------------------


def test_rr1_prunes_only_once_7_days_past_the_later_close(tmp_path: Path) -> None:
    """RR1 (plan 14.10): D-3 closed at T, D-2 closed at T+1h, D-1 and D open.

    Kills M60 (prune_raw_readings deletes without asking check_date_write,
    by D's own state only): at the ``before`` assertion, correct = 0 (D-3's
    own ``closed`` state alone is not enough; the 7-day clock runs from the
    LATER of D-3 and D-2's own ``done_at``, and D-2 closed an hour after
    D-3), mutant = 6 (it prunes D-3 as soon as D-3 itself is closed,
    ignoring D-2's later close time).
    Kills M63 (it asks the guard for D+1 in place of D): at the
    ``deleted`` assertion, correct = 6 (D-3 is genuinely due; the guard is
    asked about D-3 itself), mutant = 0 (asking about D-2 instead finds
    D-2 not yet due -- D-2's own neighbour D-1 is still open -- so the
    walk is refused on its very first candidate and nothing is ever
    pruned at this ``now_utc``).
    """
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id, stations = _build_site(conn)

    t = NOW
    _close(conn, site_id, D_MINUS_3, now_utc=t)
    t1 = t + timedelta(hours=1)
    _close(conn, site_id, D_MINUS_2, now_utc=t1)
    # D-1 and D stay open.

    expected = _seed_day_every_source(conn, stations, D_MINUS_3)
    assert expected == 6

    due_at = t1 + RAW_KEEP

    before = prune_raw_readings(
        conn, now_utc=note_day_clock(due_at - timedelta(seconds=1))
    )
    assert before == 0
    remaining = _count_between(conn, _day_start(D_MINUS_3), _day_start(D_MINUS_2))
    assert remaining == expected

    deleted = prune_raw_readings(conn, now_utc=note_day_clock(due_at))
    assert deleted == expected
    assert _count_between(conn, _day_start(D_MINUS_3), _day_start(D_MINUS_2)) == 0

    again = prune_raw_readings(conn, now_utc=note_day_clock(due_at))
    assert again == 0


# ---------------------------------------------------------------------------
# RR2
# ---------------------------------------------------------------------------


def test_rr2_the_next_day_must_also_be_done(tmp_path: Path) -> None:
    """RR2 (plan 14.10): D-2 with D-1 open, at T+30d; then D-1 missed at
    T2, at T2+7d.

    Kills M62 (prune_raw_readings deletes the first day the guard refused,
    then stops): at the ``kept`` assertion (the first call, 30 days after
    D-2's own close, with D-1 still open), correct = 6 (D-2's rows
    untouched: the guard refuses RAW_PRUNE for D-2 because its own
    neighbour D-1 has not closed, and a refusal must leave the day
    alone), mutant = 0 (it deletes the very day the guard just refused,
    before halting the walk there).
    """
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id, stations = _build_site(conn)

    t = NOW
    _close(conn, site_id, D_MINUS_3, now_utc=t)
    t1 = t + timedelta(hours=1)
    _close(conn, site_id, D_MINUS_2, now_utc=t1)
    # D-1 and D stay open for now.

    _seed_day_every_source(conn, stations, D_MINUS_3)
    expected_d2 = _seed_day_every_source(conn, stations, D_MINUS_2)
    assert expected_d2 == 6

    first = prune_raw_readings(conn, now_utc=note_day_clock(t1 + timedelta(days=30)))
    assert first == 6  # only D-3; D-2's own neighbour D-1 is still open
    kept = _count_between(conn, _day_start(D_MINUS_2), _day_start(D_MINUS_1))
    assert kept == 6

    t2 = t1 + timedelta(days=31)
    mark_missed(conn, site_id, D_MINUS_1, now_utc=t2, reason="never_attempted")

    not_yet = prune_raw_readings(
        conn, now_utc=note_day_clock(t2 + RAW_KEEP - timedelta(seconds=1))
    )
    assert not_yet == 0

    second = prune_raw_readings(conn, now_utc=note_day_clock(t2 + RAW_KEEP))
    assert second == expected_d2
    assert _count_between(conn, _day_start(D_MINUS_2), _day_start(D_MINUS_1)) == 0


# ---------------------------------------------------------------------------
# RR3
# ---------------------------------------------------------------------------


def test_rr3_the_delete_is_bounded_below_and_by_site(tmp_path: Path) -> None:
    """RR3 (plan 14.10): a reading before D-3 (the site's first tracked
    day), and another site's rows at the same instants, are both untouched
    by pruning D-3.

    Kills M64 (the delete is not bounded below, ``obs_at < ?`` only): at
    the pre-tracked-reading assertion, correct = present (D-3's own
    ``day_start_utc`` is the delete's lower bound, so an instant before it
    is excluded), mutant = absent (a delete bounded only above sweeps it
    up too, along with every older row for that station).

    The other-site reading is asserted untouched too, as the plan's case
    names it, but this half is not claimed against any numbered mutant:
    the real implementation already deletes per ``station_id`` within the
    site being processed (10.3 step 3), so no single-field mutation in
    15.2 crosses a site boundary on its own. This stands as defensive
    coverage of the per-station scoping, not a claimed kill.
    """
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id, stations = _build_site(conn)
    b_id = stations[STATION_PWS_IDS[1]]

    before_start = _day_start(D_MINUS_3) - timedelta(hours=1)
    _insert_reading(conn, b_id, before_start, "five_minute", speed_kmh=5.0)

    site2_id = asof_make_site(conn, "raw-retention-other-site")
    cur = conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
        " VALUES (?, 'ISTATION03', 0.0, 0.0, 0.0, 1)",
        (site2_id,),
    )
    station2_id = int(cur.lastrowid)
    _insert_reading(conn, station2_id, before_start, "five_minute", speed_kmh=6.0)
    d3_instant = _day_start(D_MINUS_3) + timedelta(hours=2)
    _insert_reading(conn, station2_id, d3_instant, "five_minute", speed_kmh=7.0)

    t = NOW
    _close(conn, site_id, D_MINUS_3, now_utc=t)
    t1 = t + timedelta(hours=1)
    _close(conn, site_id, D_MINUS_2, now_utc=t1)
    _seed_day_every_source(conn, stations, D_MINUS_3)

    deleted = prune_raw_readings(conn, now_utc=note_day_clock(t1 + RAW_KEEP))
    assert deleted == 6

    assert (
        conn.execute(
            "SELECT 1 FROM station_wind_records WHERE station_id=? AND obs_at=?",
            (b_id, isoformat_utc(before_start)),
        ).fetchone()
        is not None
    )
    assert (
        conn.execute(
            "SELECT 1 FROM station_wind_records WHERE station_id=? AND obs_at=?",
            (station2_id, isoformat_utc(before_start)),
        ).fetchone()
        is not None
    )
    assert (
        conn.execute(
            "SELECT 1 FROM station_wind_records WHERE station_id=? AND obs_at=?",
            (station2_id, isoformat_utc(d3_instant)),
        ).fetchone()
        is not None
    )


# ---------------------------------------------------------------------------
# RR4
# ---------------------------------------------------------------------------


class _FaultAfterN(sqlite3.Connection):
    """sqlite3.Connection subclass that faults once a matching statement has
    been seen ``skip`` times already.

    Modelled on ``tests/test_sm1_migration.py``'s ``_FaultConn``, extended
    with a skip count so the Nth (not just the first) matching statement
    can be made to fail -- here, D-2's first per-station delete, after
    D-3's two per-station deletes (and its marker write) have already run.
    """

    def __init__(
        self, *args: object, fault_on: str, skip: int, **kwargs: object
    ) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[call-arg]
        self._fault_on = fault_on.upper()
        self._skip = skip
        self._seen = 0
        self._faulted = False

    def execute(  # type: ignore[override]
        self, sql: str, parameters: object = (), /
    ) -> sqlite3.Cursor:
        if not self._faulted and self._fault_on in sql.upper():
            if self._seen >= self._skip:
                self._faulted = True
                raise sqlite3.OperationalError(
                    f"_FaultAfterN: fault on statement matching {self._fault_on!r}"
                )
            self._seen += 1
        return super().execute(sql, parameters)  # type: ignore[call-arg]


def test_rr4_a_mid_walk_failure_leaves_the_marker_on_the_day_it_finished(
    tmp_path: Path,
) -> None:
    """RR4 (plan 14.10): the marker after RR1; a stop between two days (the
    second day's delete raising).

    Kills M65 (the marker is written before the day's deletes, in a
    separate transaction): at the ``marker`` assertion, correct = D-3's
    own date (the marker only advances once that day's deletes have
    already run), mutant = D-2's date (written ahead of the delete it is
    about to attempt -- the one this fixture makes raise -- so the marker
    would already name the very day whose rows the fault left untouched).
    """
    base = tmp_path
    db_path = base / "wxverify.db"
    db = _init_tmp_db(base)
    conn = db._conn  # noqa: SLF001
    site_id, stations = _build_site(conn)

    t0 = NOW
    _close(conn, site_id, D_MINUS_3, now_utc=t0)
    t1 = t0 + timedelta(hours=1)
    _close(conn, site_id, D_MINUS_2, now_utc=t1)
    t2 = t1 + timedelta(hours=1)
    mark_missed(conn, site_id, D_MINUS_1, now_utc=t2, reason="never_attempted")

    expected_d3 = _seed_day_every_source(conn, stations, D_MINUS_3)
    expected_d2 = _seed_day_every_source(conn, stations, D_MINUS_2)
    assert expected_d3 == 6
    assert expected_d2 == 6

    due_at = t2 + RAW_KEEP
    close_db()

    fault_conn: _FaultAfterN = sqlite3.connect(  # type: ignore[call-overload]
        str(db_path),
        isolation_level=None,
        factory=_FaultAfterN,
        fault_on="DELETE FROM station_wind_records",
        skip=2,  # let D-3's two per-station deletes through; fault on D-2's first
    )
    fault_conn.row_factory = sqlite3.Row
    fault_conn.execute("PRAGMA foreign_keys=ON")

    with pytest.raises(sqlite3.OperationalError, match="_FaultAfterN"):
        prune_raw_readings(fault_conn, now_utc=due_at)

    d3_rows = fault_conn.execute(
        "SELECT COUNT(*) FROM station_wind_records WHERE obs_at >= ? AND obs_at < ?",
        (isoformat_utc(_day_start(D_MINUS_3)), isoformat_utc(_day_start(D_MINUS_2))),
    ).fetchone()[0]
    assert d3_rows == 0

    d2_rows = fault_conn.execute(
        "SELECT COUNT(*) FROM station_wind_records WHERE obs_at >= ? AND obs_at < ?",
        (isoformat_utc(_day_start(D_MINUS_2)), isoformat_utc(_day_start(D_MINUS_1))),
    ).fetchone()[0]
    assert d2_rows == expected_d2

    marker = get_runtime_state(fault_conn, f"raw_readings_pruned_through:{site_id}")
    assert marker == D_MINUS_3.isoformat()

    fault_conn.close()


# ---------------------------------------------------------------------------
# RD1, RD2, RD3 (plan 14.10's second table; ``reading_times``,
# ``last_five_minute_store`` and ``raw_readings`` are new in
# ``db/station_readings.py``). Pure reads with no site_days/guard state to
# set up: each test builds its own minimal site and stations rather than
# reusing ``_build_site``.
# ---------------------------------------------------------------------------


def _make_station(conn, site_id: int, pws_id: str) -> int:  # type: ignore[no-untyped-def]
    cur = conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
        " VALUES (?, ?, 0.0, 0.0, 0.0, 1)",
        (site_id, pws_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_raw_reading(
    conn,  # type: ignore[no-untyped-def]
    station_id: int,
    obs_at: datetime,
    source: str,
    *,
    fetched_at: datetime | None,
    local_date: date | None = None,
    speed_kmh: float | None = None,
    temp_c: float | None = None,
    precip_total_mm: float | None = None,
) -> None:
    """Like ``_insert_reading``, but with ``fetched_at`` and ``local_date``
    under the caller's control -- RD2 and RD3 need ``fetched_at`` stamps
    independent of ``obs_at``, and RD3 needs a row with no ``fetched_at`` at
    all (NULL, as a pre-0.16.14 row would carry, plan 6.2).
    """
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, local_date, speed_kmh, temp_c,"
        "  precip_total_mm, fetched_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            station_id,
            isoformat_utc(obs_at),
            source,
            None if local_date is None else local_date.isoformat(),
            speed_kmh,
            temp_c,
            precip_total_mm,
            None if fetched_at is None else isoformat_utc(fetched_at),
        ),
    )


# ---------------------------------------------------------------------------
# RD1
# ---------------------------------------------------------------------------


def test_rd1_reading_times_time_order_and_source_filter(tmp_path: Path) -> None:
    """RD1 (plan 14.10): ``reading_times`` for B over 10:00-10:07: B has
    ``five_minute`` readings at 10:00 and 10:05, ``current_obs`` at 10:02
    and 10:07, ``hourly`` at 10:00; C has a reading at 10:03. Then the same
    range filtered to ``source="current_obs"``.

    Kills M90 (``reading_times`` includes a reading at ``end_utc``): at the
    ``unfiltered`` assertion, correct = ``[(10:00, five_minute), (10:00,
    hourly), (10:02, current_obs), (10:05, five_minute)]`` (``end_utc``,
    10:07, is excluded), mutant = the same list with a fifth entry
    ``(10:07, current_obs)`` appended (the end instant wrongly included).
    Kills M91 (``reading_times`` ignores ``source``): at the ``filtered``
    assertion, correct = ``[(10:02, current_obs)]`` (the source filter kept
    only ``current_obs``), mutant = ``[(10:00, five_minute), (10:00,
    hourly), (10:02, current_obs), (10:05, five_minute)]`` (the filter
    ignored, identical to the unfiltered call).
    """
    from wxverify.db.station_readings import reading_times

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = asof_make_site(conn, "rd1-site")
    station_b = _make_station(conn, site_id, "ISTATION04")
    station_c = _make_station(conn, site_id, "ISTATION05")

    base = datetime(D.year, D.month, D.day, 10, 0, tzinfo=UTC)
    t02 = base + timedelta(minutes=2)
    t03 = base + timedelta(minutes=3)
    t05 = base + timedelta(minutes=5)
    t07 = base + timedelta(minutes=7)

    _insert_raw_reading(
        conn, station_b, base, "five_minute", fetched_at=base, speed_kmh=5.0
    )
    _insert_raw_reading(
        conn, station_b, t05, "five_minute", fetched_at=t05, speed_kmh=5.0
    )
    _insert_raw_reading(
        conn, station_b, t02, "current_obs", fetched_at=t02, speed_kmh=5.0
    )
    _insert_raw_reading(
        conn, station_b, t07, "current_obs", fetched_at=t07, speed_kmh=5.0
    )
    _insert_raw_reading(conn, station_b, base, "hourly", fetched_at=base, speed_kmh=5.0)
    _insert_raw_reading(
        conn, station_c, t03, "five_minute", fetched_at=t03, speed_kmh=5.0
    )

    unfiltered = reading_times(conn, station_id=station_b, start_utc=base, end_utc=t07)
    assert unfiltered == [
        (base, "five_minute"),
        (base, "hourly"),
        (t02, "current_obs"),
        (t05, "five_minute"),
    ]

    filtered = reading_times(
        conn,
        station_id=station_b,
        start_utc=base,
        end_utc=t07,
        source="current_obs",
    )
    assert filtered == [(t02, "current_obs")]


# ---------------------------------------------------------------------------
# RD2
# ---------------------------------------------------------------------------


def test_rd2_last_five_minute_store_by_source_and_site(tmp_path: Path) -> None:
    """RD2 (plan 14.10): ``last_five_minute_store`` for a site with B's
    ``five_minute`` row (fetched 08:00) and C's ``five_minute`` (09:00),
    ``current_obs`` (10:00) and ``hourly`` (11:00) rows, and another
    site's ``five_minute`` row (fetched 12:00); then ``store_readings``
    repeats B's own reading at 13:00; then a site with no ``five_minute``
    row.

    Kills M92 (``last_five_minute_store`` counts every source's
    ``fetched_at``): at the ``first`` assertion, correct = 09:00 (the
    site's latest ``five_minute`` ``fetched_at`` is C's own ``five_minute``
    row; C's ``current_obs`` and ``hourly`` rows, fetched later, are not
    ``five_minute``), mutant = 11:00 (``hourly``'s ``fetched_at``, the
    latest of every source on the site).
    Kills M93 (``last_five_minute_store`` not bounded by the site's
    stations): at the same ``first`` assertion, correct = 09:00 (the other
    site's ``five_minute`` row, fetched 12:00, belongs to a different site
    and is ignored), mutant = 12:00 (the other site's later row counted
    in).
    """
    from wxverify.db.station_readings import last_five_minute_store, store_readings
    from wxverify.obs.hourly_values import StationReading

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    close_off(conn)
    site_id = asof_make_site(conn, "rd2-site")
    station_b = _make_station(conn, site_id, "ISTATION04")
    station_c = _make_station(conn, site_id, "ISTATION05")

    b_at = datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)
    b_fetched_08 = datetime(D.year, D.month, D.day, 8, 0, tzinfo=UTC)
    _insert_raw_reading(
        conn, station_b, b_at, "five_minute", fetched_at=b_fetched_08, speed_kmh=5.0
    )
    _insert_raw_reading(
        conn,
        station_c,
        b_at + timedelta(minutes=5),
        "five_minute",
        fetched_at=datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC),
        speed_kmh=5.0,
    )
    _insert_raw_reading(
        conn,
        station_c,
        b_at + timedelta(minutes=10),
        "current_obs",
        fetched_at=datetime(D.year, D.month, D.day, 10, 0, tzinfo=UTC),
        speed_kmh=5.0,
    )
    _insert_raw_reading(
        conn,
        station_c,
        b_at + timedelta(minutes=15),
        "hourly",
        fetched_at=datetime(D.year, D.month, D.day, 11, 0, tzinfo=UTC),
        speed_kmh=5.0,
    )

    other_site_id = asof_make_site(conn, "rd2-other-site")
    other_station = _make_station(conn, other_site_id, "ISTATION06")
    _insert_raw_reading(
        conn,
        other_station,
        b_at + timedelta(minutes=20),
        "five_minute",
        fetched_at=datetime(D.year, D.month, D.day, 12, 0, tzinfo=UTC),
        speed_kmh=5.0,
    )

    first = last_five_minute_store(conn, site_id=site_id)
    assert first == datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)

    repeat = store_readings(
        conn,
        station_id=station_b,
        source="five_minute",
        readings=[
            StationReading(
                obs_at=b_at,
                source="five_minute",
                local_date=None,
                temp_c=None,
                precip_total_mm=None,
                speed_kmh=5.0,
            )
        ],
        fetched_at=datetime(D.year, D.month, D.day, 13, 0, tzinfo=UTC),
        now_utc=NOW,
    )
    assert repeat.already_stored == 1
    assert repeat.kept == 0

    second = last_five_minute_store(conn, site_id=site_id)
    assert second == datetime(D.year, D.month, D.day, 13, 0, tzinfo=UTC)

    empty_site_id = asof_make_site(conn, "rd2-empty-site")
    _make_station(conn, empty_site_id, "ISTATION01")
    third = last_five_minute_store(conn, site_id=empty_site_id)
    assert third is None


# ---------------------------------------------------------------------------
# RD4
# ---------------------------------------------------------------------------


def test_rd4_last_five_minute_store_skips_an_unreadable_fetched_at(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """RD4 (0.16.14): ``last_five_minute_store`` walks the site's
    ``five_minute`` rows by ``julianday(fetched_at)`` descending, and
    returns the first ``fetched_at`` that ``parse_utc`` accepts -- a row
    whose ``fetched_at`` is a bare Julian-day-number string
    (``'2460900.5'``) is LATER by ``julianday()`` than any ordinary
    canonical 2024 stamp (sqlite3's ``julianday()`` treats a bare numeric
    string as a Julian day number directly, per
    https://www.sqlite.org/lang_datefunc.html), yet
    ``wxverify.core.timeutil.parse_utc`` requires an ISO-8601 stamp and
    raises ``ValueError`` on it -- so the walk must skip past it to an
    older, canonical row, logging one WARNING naming the skipped count.
    With only the unreadable row, it returns None.

    Today's code (``ORDER BY julianday(r.fetched_at) DESC LIMIT 1`` then a
    bare ``parse_utc`` of the one picked row) picks the unreadable row as
    the top ``julianday`` and raises ``ValueError`` straight out of
    ``last_five_minute_store``; that is expected to fail until the
    walk-and-skip behaviour lands.
    """
    import logging

    from wxverify.db.station_readings import last_five_minute_store

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = asof_make_site(conn, "rd4-site")
    station_b = _make_station(conn, site_id, "ISTATION04")

    valid_fetched = datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)
    _insert_raw_reading(
        conn,
        station_b,
        valid_fetched,
        "five_minute",
        fetched_at=valid_fetched,
        speed_kmh=5.0,
    )

    bad_at = datetime(D.year, D.month, D.day, 10, 0, tzinfo=UTC)
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, speed_kmh, fetched_at)"
        " VALUES (?, ?, 'five_minute', 5.0, '2460900.5')",
        (station_b, isoformat_utc(bad_at)),
    )

    with caplog.at_level(logging.WARNING):
        caplog.clear()
        result = last_five_minute_store(conn, site_id=site_id)

    assert result == valid_fetched, (
        "M96 -> at this assertion: correct = 09:00 (the valid older stamp;"
        " the unreadable row, latest by julianday, is skipped), mutant"
        " (today's code: the SQL's own ORDER BY ... LIMIT 1 picks the"
        " unreadable row and parse_utc is called on it directly) = raises"
        " ValueError instead of returning"
    )
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, (
        "M96 -> at this assertion: correct = exactly one WARNING for the"
        " one skipped row, mutant (no warning logged for a skipped"
        " unreadable row) = zero"
    )

    caplog.clear()
    only_bad_site_id = asof_make_site(conn, "rd4-only-bad-site")
    only_bad_station = _make_station(conn, only_bad_site_id, "ISTATION05")
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, speed_kmh, fetched_at)"
        " VALUES (?, ?, 'five_minute', 5.0, '2460900.5')",
        (only_bad_station, isoformat_utc(bad_at)),
    )
    with caplog.at_level(logging.WARNING):
        caplog.clear()
        only_bad_result = last_five_minute_store(conn, site_id=only_bad_site_id)
    assert only_bad_result is None, (
        "M96 -> at this assertion: correct = None (every five_minute row of"
        " the site is unreadable, so none parse), mutant (the walk stops at"
        " the first row regardless of readability) = raises ValueError"
        " instead of returning None"
    )


# ---------------------------------------------------------------------------
# RD3
# ---------------------------------------------------------------------------


def test_rd3_raw_readings_fields_bounds_and_nulls(tmp_path: Path) -> None:
    """RD3 (plan 14.10): ``raw_readings`` for B over 10:00-11:00: a row
    with every column set, a row with every value the table's CHECK
    allows NULL (local date, temperature, rain total, ``fetched_at`` --
    ``speed_kmh`` keeps a value, since the CHECK requires at least one of
    the three, plan 6.2), a row at 11:00, and C's row at 10:30.

    Kills M94 (``raw_readings`` swaps ``temp_c`` and ``precip_total_mm``):
    at the ``result`` assertion, correct = the first row with
    ``temp_c=15.0`` and ``precip_total_mm=2.0`` (as stored), mutant = the
    same row with ``temp_c=2.0`` and ``precip_total_mm=15.0`` (the two
    columns swapped).
    """
    from wxverify.db.station_readings import RawReading, raw_readings

    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = asof_make_site(conn, "rd3-site")
    station_b = _make_station(conn, site_id, "ISTATION04")
    station_c = _make_station(conn, site_id, "ISTATION05")

    start = datetime(D.year, D.month, D.day, 10, 0, tzinfo=UTC)
    end = datetime(D.year, D.month, D.day, 11, 0, tzinfo=UTC)
    row1_at = start
    row1_fetched = start + timedelta(minutes=5)
    row2_at = start + timedelta(minutes=15)

    _insert_raw_reading(
        conn,
        station_b,
        row1_at,
        "five_minute",
        fetched_at=row1_fetched,
        local_date=D,
        temp_c=15.0,
        precip_total_mm=2.0,
        speed_kmh=5.0,
    )
    _insert_raw_reading(
        conn,
        station_b,
        row2_at,
        "five_minute",
        fetched_at=None,
        local_date=None,
        temp_c=None,
        precip_total_mm=None,
        speed_kmh=0.0,
    )
    _insert_raw_reading(
        conn,
        station_b,
        end,
        "five_minute",
        fetched_at=end,
        local_date=D,
        temp_c=16.0,
        precip_total_mm=3.0,
        speed_kmh=6.0,
    )
    _insert_raw_reading(
        conn,
        station_c,
        start + timedelta(minutes=30),
        "five_minute",
        fetched_at=start,
        local_date=D,
        temp_c=17.0,
        precip_total_mm=4.0,
        speed_kmh=7.0,
    )

    result = raw_readings(conn, station_id=station_b, start_utc=start, end_utc=end)

    assert result == [
        RawReading(
            obs_at=row1_at,
            source="five_minute",
            local_date=D,
            temp_c=15.0,
            precip_total_mm=2.0,
            speed_kmh=5.0,
            fetched_at=row1_fetched,
        ),
        RawReading(
            obs_at=row2_at,
            source="five_minute",
            local_date=None,
            temp_c=None,
            precip_total_mm=None,
            speed_kmh=0.0,
            fetched_at=None,
        ),
    ]

    first, second = result
    assert type(first.local_date) is date
    assert isinstance(first.fetched_at, datetime)
    assert first.fetched_at.tzinfo is not None
    assert second.local_date is None
    assert second.temp_c is None
    assert second.precip_total_mm is None
    assert second.fetched_at is None
    assert second.speed_kmh == 0.0
