"""§6.7 / §15.3 — fault states as of T.

``RainEvents.states_at(T)`` builds the §6.7 fault states only from readings
fetched by T, for the days up to K(T) (``latest_knowable_target``). This
pins the plan's "fault states as of T" test (oracle file
``.tmp/fault-states-as-of-t-oracle.md``, §15.3 line 57), reusing the
"fault states use earlier days only" fixture from §15.2 (same file, lines
51-54) with its two named changes: every station's day-12 readings are
fetched after T, and on day 13 SYN-4 reads 0.5 mm in one hour.

Every expected value pinned below is a literal copied from the oracle —
never computed by calling ``RainEvents.states_at``/``RainEvents.as_of``/
``daily_method.station_fault_states``/``daily_method.rain_nearby_event`` or
any other production function.

Four synthetic stations (SYN-1 .. SYN-4), site time zone UTC, far-future
days (2035), every station with all 24 hours on every day 1-13:

* SYN-1, SYN-2 and SYN-3 read 2.0 mm (8 wet hours of 0.25 mm) on days
  1-3, 6-8 and 11-12, and 0.0 mm on days 4, 5, 9, 10 and 13.
* SYN-4 reads 0.0 mm on every day except day 13, where one hour reads
  0.5 mm (the other 23 hours 0.0 mm, running totals agreeing).

T is 07:00 UTC on day 14, so K (``latest_knowable_target``) is day 13
(``CONSENSUS_LAG_HOURS`` = 3: day 13's ``end_utc`` + 3h = day 14 03:00 <=
T; day 14's own +3h = day 15 03:00 > T).

Production names used — all public, none underscore-prefixed:
``daily_shadow.Site``, ``daily_shadow.load_site``,
``daily_shadow.load_station_rain``, ``daily_shadow.StationDayStore``,
``daily_shadow.RainEvents``, ``simulate.latest_knowable_target``,
``tests.helpers.asof_make_site``, ``timeutil.isoformat_utc``,
``migrations.run_migrations``, ``tz_generations.ensure_published_generation``.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification import daily_shadow
from wxverify.verification.daily_shadow import Site
from wxverify.verification.simulate import latest_knowable_target

# Day 1 of the fixture's 13-day window, far in the future (synthetic data).
_DAY1 = date(2035, 9, 1)
_DAYS = [_DAY1 + timedelta(days=n) for n in range(13)]  # days 1..13, index 0..12
_DAY12 = _DAYS[11]
_DAY13 = _DAYS[12]
_DAY14 = _DAY1 + timedelta(days=13)
_T = datetime(_DAY14.year, _DAY14.month, _DAY14.day, 7, tzinfo=UTC)

# §15.2's "fault states use earlier days only" wet days (1-indexed: 1-3,
# 6-8, 11-12) for SYN-1/SYN-2/SYN-3; every other day in 1-13 is dry.
_WET_DAY_INDEXES = {1, 2, 3, 6, 7, 8, 11, 12}
_WET_DAYS = {_DAYS[i - 1] for i in _WET_DAY_INDEXES}

_STATION_IDS = ("SYN-1", "SYN-2", "SYN-3", "SYN-4")
# The stations table's created_at defaults to the wall clock if not given.
_STATION_CREATED_AT = "2026-01-01T00:00:00.000Z"


def _rain_hours(*, wet_count: int) -> list[float]:
    return [0.25] * wet_count + [0.0] * (24 - wet_count)


def _make_copy(tmp_path: Path, *, name: str) -> tuple[Path, Site]:
    """A migrated sqlite copy with one UTC site and its published generation."""
    copy = tmp_path / name
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "fault-states-as-of-t-site")
        ensure_published_generation(conn, site_id)
        conn.commit()
    finally:
        conn.close()
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        site = daily_shadow.load_site(conn, site_id=site_id)
    finally:
        conn.close()
    return copy, site


def _make_station(
    conn: sqlite3.Connection, *, site_id: int, pws_station_id: str
) -> int:
    cur = conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled, created_at)"
        " VALUES (?, ?, 0.0, 0.0, 900.0, 1, ?)",
        (site_id, pws_station_id, _STATION_CREATED_AT),
    )
    station_id = cur.lastrowid
    assert station_id is not None
    return int(station_id)


def _insert_station_day(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    local_date: date,
    values: list[float],
    fetched_at: datetime,
    hour23_fetched_at: datetime | None = None,
) -> None:
    """24 ``precip`` readings for one station-day with a valid running total.

    ``source_raw`` is ``"<cumulative> mm precipTotal"`` for every hour (a
    non-decreasing running total), so ``station_day_total`` never raises
    ``unparsed_total`` or ``counter_reset``. Every reading is fetched at
    the same given ``fetched_at`` (no earlier than its hour's start, per
    the oracle's fixture text), so a single timestamp controls whether the
    whole day is known by a given T — except hour 23, which is fetched at
    ``hour23_fetched_at`` when given (the oracle's partial-day case: hours
    0-22 fetched by T, hour 23 fetched after T).
    """
    assert len(values) == 24
    cumulative = 0.0
    for hour, value in enumerate(values):
        cumulative += value
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        source_raw = f"{round(cumulative, 10)} mm precipTotal"
        hour_fetched_at = (
            hour23_fetched_at
            if hour == 23 and hour23_fetched_at is not None
            else fetched_at
        )
        conn.execute(
            """
            INSERT INTO station_observations
                (station_id, variable, valid_at, value, qc_flag, source_raw,
                 fetched_at)
            VALUES (?, 'precip', ?, ?, 'ok', ?, ?)
            """,
            (
                station_id,
                isoformat_utc(valid),
                value,
                source_raw,
                isoformat_utc(hour_fetched_at),
            ),
        )


def _normal_fetched_at(local_date: date) -> datetime:
    """The next local midnight plus 5 minutes — after every hour, well by T."""
    next_day = local_date + timedelta(days=1)
    return datetime(next_day.year, next_day.month, next_day.day, 0, 5, tzinfo=UTC)


_AFTER_T_FETCHED_AT = _T + timedelta(hours=1)


def _build_fixture(
    tmp_path: Path, *, name: str, day12_fetched_by_t: bool
) -> tuple[Path, Site, dict[str, int]]:
    """§15.2's "fault states use earlier days only" fixture, with the two
    §15.3 changes: every station's day-12 readings fetched after T (unless
    ``day12_fetched_by_t`` is set, for the positive control), and SYN-4
    reading 0.5 mm in one hour of day 13.
    """
    copy, site = _make_copy(tmp_path, name=name)
    conn = sqlite3.connect(copy)
    try:
        station_ids: dict[str, int] = {}
        for station in _STATION_IDS:
            station_ids[station] = _make_station(
                conn, site_id=site.site_id, pws_station_id=station
            )
        for station in _STATION_IDS:
            station_id = station_ids[station]
            for day in _DAYS:
                if station in ("SYN-1", "SYN-2", "SYN-3"):
                    values = _rain_hours(wet_count=8 if day in _WET_DAYS else 0)
                else:
                    values = [0.0] * 24
                    if day == _DAY13:
                        values[0] = 0.5
                if day == _DAY12 and not day12_fetched_by_t:
                    fetched_at = _AFTER_T_FETCHED_AT
                else:
                    fetched_at = _normal_fetched_at(day)
                _insert_station_day(
                    conn,
                    station_id=station_id,
                    local_date=day,
                    values=values,
                    fetched_at=fetched_at,
                )
        conn.commit()
    finally:
        conn.close()
    return copy, site, station_ids


def _build_store_and_events(
    copy: Path, site: Site
) -> tuple[daily_shadow.StationDayStore, daily_shadow.RainEvents]:
    """Load every reading (``known_by=None``) and build the store/events.

    The store itself does the as-of filtering (``StationDayStore.as_of`` /
    ``history``) from each reading's own ``fetched_at`` — loading
    everything here keeps every row available for that filtering.
    """
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        readings = daily_shadow.load_station_rain(
            conn,
            site_id=site.site_id,
            first_day=_DAYS[0],
            last_day=_DAY13,
            known_by=None,
        )
    finally:
        conn.close()
    store = daily_shadow.StationDayStore(readings, timezone=site.timezone)
    events = daily_shadow.RainEvents(store, timezone=site.timezone)
    return store, events


# The seven days SYN-4 qualifies as of T in the main case (day 12 excluded
# because no station's day-12 readings are fetched by T): days 1-3, 6-8, 11.
_EXPECTED_QUALIFYING_DAYS = frozenset(_DAYS[i - 1] for i in (1, 2, 3, 6, 7, 8, 11))


# ---------------------------------------------------------------------------
# Liveness: the fixture really holds what the oracle says, checked directly
# against production code before the main assertions.
# ---------------------------------------------------------------------------


def test_fixture_liveness_matches_the_oracles_literal_setup(tmp_path: Path) -> None:
    """K is day 13; every station-day is valid; SYN-4 has 7 qualifying days
    in 3 episodes as of T (oracle §15.3 line 57), before any state/event
    assertion is trusted.
    """
    assert latest_knowable_target("UTC", _T) == _DAY13

    copy, site, _station_ids = _build_fixture(
        tmp_path, name="liveness.db", day12_fetched_by_t=False
    )
    store, events = _build_store_and_events(copy, site)

    # Every station-day in the window is valid (not unparsed, not
    # incomplete): 24 hours read, no measurement problem.
    for station in _STATION_IDS:
        for day in _DAYS:
            total = store.full(station, day)
            assert total is not None, (station, day)
            assert total.hours == 24, (station, day, total)
            assert not total.invalid, (station, day, total)

    states = events.states_at(_T)
    qualifying_syn4_days = {
        day for (station, day) in states.qualifying if station == "SYN-4"
    }
    assert qualifying_syn4_days == _EXPECTED_QUALIFYING_DAYS, qualifying_syn4_days

    # FaultStates.log records only CHANGES of state (daily_method.py:1090-
    # 1104), so SYN-4's single ok->suspect change is on day 8 (the day its
    # qualifying days before it, 1-3 and 6-7, first reach the suspect rule's
    # minimum of 5); day 13 adds no new entry because the state was already
    # suspect.
    for day in _DAYS[:7]:
        assert states.states[("SYN-4", day)] == "ok", (day, states.states)
    for day in _DAYS[7:]:
        assert states.states[("SYN-4", day)] == "suspect", (day, states.states)

    changes = [
        c for c in states.log if c.station_id == "SYN-4" and c.state == "suspect"
    ]
    assert len(changes) == 1, states.log
    change = changes[0]
    assert change.day == _DAYS[7], change
    assert change.rule == "suspect", change
    assert change.qualifying_days == 5, change
    assert change.episodes == 2, change


# ---------------------------------------------------------------------------
# Main case — day 12 fetched after T: SYN-4 stays short of quarantine (7
# qualifying days, not 8) and is suspect; day 13 is rain nearby (one_at_0.5).
# ---------------------------------------------------------------------------


def test_fault_states_as_of_t_syn4_is_suspect_not_quarantined(
    tmp_path: Path,
) -> None:
    """Oracle: states_at(T) gives SYN-4 suspect on day 13, day 13 rain_nearby."""
    copy, site, _station_ids = _build_fixture(
        tmp_path, name="main.db", day12_fetched_by_t=False
    )
    _store, events = _build_store_and_events(copy, site)

    states = events.states_at(_T)
    assert states.states[("SYN-4", _DAY13)] == "suspect", states.states

    day13 = events.as_of(_DAY13, _T)
    assert day13.event.outcome == "rain_nearby", day13.event
    assert day13.event.trigger == "one_at_0.5", day13.event


# ---------------------------------------------------------------------------
# Positive control — day 12 fetched by T: SYN-4 reaches the eighth
# qualifying day and is quarantined; day 13 is dry (SYN-4 excluded).
# ---------------------------------------------------------------------------


def test_fault_states_as_of_t_positive_control_syn4_is_quarantined(
    tmp_path: Path,
) -> None:
    """Oracle: with day 12 known by T, SYN-4 is quarantined and day 13 is dry."""
    copy, site, _station_ids = _build_fixture(
        tmp_path, name="control.db", day12_fetched_by_t=True
    )
    _store, events = _build_store_and_events(copy, site)

    states = events.states_at(_T)
    assert states.states[("SYN-4", _DAY13)] == "quarantined", states.states
    # Oracle: "ok to day 7, suspect from day 8, quarantined from day 13".
    for day in _DAYS[:7]:
        assert states.states[("SYN-4", day)] == "ok", (day, states.states)
    for day in _DAYS[7:12]:
        assert states.states[("SYN-4", day)] == "suspect", (day, states.states)

    day13 = events.as_of(_DAY13, _T)
    assert day13.event.outcome == "dry", day13.event
    assert day13.event.trigger is None, day13.event


# ---------------------------------------------------------------------------
# Partial-day case — day 12's hour-23 reading (only) fetched after T: day 12
# holds hours 0-22 (23 hours, 0.0 mm) as of T, valid but not reliable, so it
# gives no qualifying day and SYN-4 stays at the main case's seven.
# ---------------------------------------------------------------------------


def _build_partial_day_fixture(
    tmp_path: Path, *, name: str
) -> tuple[Path, Site, dict[str, int]]:
    """The main case's fixture (``day12_fetched_by_t=False``), except day 12:
    SYN-1 to SYN-3 read their 2.0 mm in hour 23 only (0.0 mm in hours 0-22,
    running totals that agree), and each station's day-12 hours 0-22 are
    fetched by T (no earlier than their hour's start) while hour 23 is
    fetched after T.
    """
    copy, site = _make_copy(tmp_path, name=name)
    conn = sqlite3.connect(copy)
    try:
        station_ids: dict[str, int] = {}
        for station in _STATION_IDS:
            station_ids[station] = _make_station(
                conn, site_id=site.site_id, pws_station_id=station
            )
        for station in _STATION_IDS:
            station_id = station_ids[station]
            for day in _DAYS:
                if day == _DAY12:
                    values = (
                        [0.0] * 23 + [2.0]
                        if station in ("SYN-1", "SYN-2", "SYN-3")
                        else [0.0] * 24
                    )
                    _insert_station_day(
                        conn,
                        station_id=station_id,
                        local_date=day,
                        values=values,
                        fetched_at=_normal_fetched_at(day),
                        hour23_fetched_at=_AFTER_T_FETCHED_AT,
                    )
                    continue
                if station in ("SYN-1", "SYN-2", "SYN-3"):
                    values = _rain_hours(wet_count=8 if day in _WET_DAYS else 0)
                else:
                    values = [0.0] * 24
                    if day == _DAY13:
                        values[0] = 0.5
                _insert_station_day(
                    conn,
                    station_id=station_id,
                    local_date=day,
                    values=values,
                    fetched_at=_normal_fetched_at(day),
                )
        conn.commit()
    finally:
        conn.close()
    return copy, site, station_ids


def test_fault_states_as_of_t_partial_day_twelve_is_not_reliable(
    tmp_path: Path,
) -> None:
    """Oracle's partial-day case: day 12's hour-23 reading, fetched after T,
    does not count. Day 12's as-of-T total is 23 hours and 0.0 mm, valid but
    not reliable (reliable needs all 24 hours), so it gives no qualifying
    day for any station: SYN-4 keeps the main case's seven qualifying days
    and stays suspect, not quarantined.
    """
    copy, site, _station_ids = _build_partial_day_fixture(tmp_path, name="partial.db")
    store, events = _build_store_and_events(copy, site)

    states = events.states_at(_T)
    assert states.states[("SYN-4", _DAY13)] == "suspect", states.states

    day13 = events.as_of(_DAY13, _T)
    assert day13.event.outcome == "rain_nearby", day13.event
    assert day13.event.trigger == "one_at_0.5", day13.event

    day12 = events.as_of(_DAY12, _T)
    assert day12.event.outcome == "dry", day12.event
    assert day12.event.trigger is None, day12.event
    assert set(day12.event.adequate) == set(_STATION_IDS), day12.event
    assert all(s.hundredths < 20 for s in day12.stations), day12.stations

    # Landed checks: the day-12 setup took effect. As of T, SYN-1's day-12
    # total is partial (23 hours, 0 hundredths, valid, not reliable); in
    # full (every reading) the same total is the whole day (24 hours, 200
    # hundredths). Placed last so they cannot mask a mutant's real first
    # failure in the three ordered assertions above.
    syn1_as_of = store.as_of("SYN-1", _DAY12, _T)
    assert syn1_as_of is not None
    assert syn1_as_of.hours == 23, syn1_as_of
    assert syn1_as_of.hundredths == 0, syn1_as_of
    assert not syn1_as_of.invalid, syn1_as_of
    assert not syn1_as_of.reliable, syn1_as_of

    syn1_full = store.full("SYN-1", _DAY12)
    assert syn1_full is not None
    assert syn1_full.hours == 24, syn1_full
    assert syn1_full.hundredths == 200, syn1_full


@pytest.mark.skip(
    reason="deferred to stage two (plan §6.7, §15.3): a --rain-holdout "
    "intervals run against SYN-1's two held-out intervals, reporting "
    "exclusion_table, holdout_partial_days and holdout_comparison; plus "
    "the no-table (absent) and index-only (ERROR at exclusion_read) copies"
)
def test_interval_holdout_deferred_to_stage_two() -> None:
    raise NotImplementedError("Stage-two acceptance test not implemented")
