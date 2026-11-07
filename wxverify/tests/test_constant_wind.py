"""The stuck-wind-station warning (plan `2026-11-07-health-release.md` §1,
Part A; test IDs AT1-AT13).

Made-up readings in km/h on whole UTC hours, a site in ``UTC``, stations
named A-F in prose with obviously made-up ``pws_station_id``s (``TESTA01``
etc, public repo: no real station id, coordinate, site name or zone name).
"Others windy" (the plan's own term) is three other stations reading 7.2,
9.0 and 10.8 km/h, in that order, through an hour -- comfortably above the
trial median gate of 3.6 km/h (1 m/s).

AT1-AT5, AT7 and AT8 call ``wxverify.obs.constant_wind`` directly: no
database, no clock. AT6 and AT9-AT12 build a real, file-backed SQLite
database (``run_migrations``) and drive ``assess_constant_wind`` and
``build_verdict`` against it. AT13 drives a real ``scheduler_tick`` through
``tests.helpers``' close-chain fixtures, the same way the other scheduler-
tick tests in this suite do.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypeVar

import pytest

from tests.helpers import (
    NOW,
    MovingClock,
    asof_make_site,
    close_off,
    disable_other_enqueuers,
    patch_close_clock,
    standard_night,
    tick,
)
from wxverify.core.options import load_runtime_options
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.constant_wind import (
    ASSESSED_THROUGH_KEY,
    assess_constant_wind,
    read_rule,
)
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.site_days import daily_close_on
from wxverify.monitor import build_verdict
from wxverify.obs.constant_wind import Episode, Rule, assess, hour_winds
from wxverify.settings.keys import set_setting
from wxverify.settings.service import apply_plain_settings

#: The rule's trial values (plan §1 A4): 6 hours, 3 other stations, a 1 m/s
#: (3.6 km/h) median.
_RULE = Rule(hours=6, other_stations=3, median_kmh=3.6)

#: "Others windy": three stations' own per-hour value, in station order.
_WINDY: tuple[float, float, float] = (7.2, 9.0, 10.8)

_H0 = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)


def _h(n: int) -> datetime:
    """The ``n``-th whole UTC hour of this file's made-up timeline."""
    return _H0 + timedelta(hours=n)


def _windy_others(hours: list[datetime]) -> list[dict[datetime, tuple[float, ...]]]:
    """Three "others windy" stations, each holding its own value through
    every hour of ``hours``."""
    return [{hour: (v,) for hour in hours} for v in _WINDY]


def _windy_at(hour: datetime) -> list[dict[datetime, tuple[float, ...]]]:
    """Three "others windy" stations, each holding its own value at one hour."""
    return [{hour: (v,)} for v in _WINDY]


@dataclass(frozen=True)
class _Reading:
    """A bare ``WindReading`` for :func:`hour_winds` (AT8): no database."""

    obs_at: datetime
    source: str
    speed_kmh: float | None


_T = TypeVar("_T")


class _RealDb:
    """DB shim wrapping one real sqlite3 connection for AT11's
    ``apply_plain_settings`` call (the pattern of
    ``tests/test_forecast_blend_depth_option.py``'s own ``_RealDb``):
    ``write``/``read`` call the given function synchronously against the
    one real connection, no mock database involved."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    async def write(self, fn: Callable[[sqlite3.Connection], _T]) -> _T:
        return fn(self._conn)

    async def read(self, fn: Callable[[sqlite3.Connection], _T]) -> _T:
        return fn(self._conn)


# ---------------------------------------------------------------------------
# AT1-AT5, AT7: the pure rule, ``assess`` (plan §1 A1).
# ---------------------------------------------------------------------------


def test_at1_stuck_high_and_stuck_at_zero() -> None:
    """AT1: a 6-hour stuck run, others windy, opens an episode; 5 hours
    opens none, for a value stuck high and one stuck at zero alike.

    Catches: no detection; a run of ``hours - 1`` accepted.
    """
    hours6 = [_h(i) for i in range(6)]
    hours5 = [_h(i) for i in range(5)]
    others6 = _windy_others(hours6)
    others5 = _windy_others(hours5)
    for value in (30.0, 0.0):
        own6 = {hour: (value,) for hour in hours6}
        result = assess(
            own6, others6, window=hours6, new_hours=(), episode=None, rule=_RULE
        )
        assert result == Episode(value, hours6[0], "suspected")

        own5 = {hour: (value,) for hour in hours5}
        result5 = assess(
            own5, others5, window=hours5, new_hours=(), episode=None, rule=_RULE
        )
        assert result5 is None


def test_at2_calm_spell_and_median_boundary() -> None:
    """AT2: every station calm opens nothing; others' median exactly 3.6
    km/h opens an episode, 3.59 opens none.

    Catches: no median gate; ``>`` in place of ``>=``; m/s compared to km/h
    (3.6 times too strict or too loose).
    """
    hours8 = [_h(i) for i in range(8)]
    own8 = {hour: (0.0,) for hour in hours8}

    calm_others = [{hour: (0.0,) for hour in hours8} for _ in range(3)]
    assert (
        assess(own8, calm_others, window=hours8, new_hours=(), episode=None, rule=_RULE)
        is None
    )

    at_boundary = [{hour: (3.6,) for hour in hours8} for _ in range(3)]
    result = assess(
        own8, at_boundary, window=hours8, new_hours=(), episode=None, rule=_RULE
    )
    assert result == Episode(0.0, hours8[2], "suspected"), (
        "the run is the window's last 6 hours, so the episode starts at index 2"
    )

    below_boundary = [{hour: (3.59,) for hour in hours8} for _ in range(3)]
    assert (
        assess(
            own8, below_boundary, window=hours8, new_hours=(), episode=None, rule=_RULE
        )
        is None
    )


def test_at3_a_gap() -> None:
    """AT3: a gap inside the run breaks it; a gap outside it is absorbed,
    opening an episode from the hour after the gap.

    Catches: a gap bridged; a run counted over hours instead of consecutive
    hours.
    """
    hours7 = [_h(i) for i in range(7)]
    others7 = _windy_others(hours7)
    value = 30.0

    own_gap_in_run = {
        hour: (value,) for i, hour in enumerate(hours7) if i != 2
    }  # the gap is hour index 2 ("hour 3"), inside the run (indices 1-6)
    assert (
        assess(
            own_gap_in_run,
            others7,
            window=hours7,
            new_hours=(),
            episode=None,
            rule=_RULE,
        )
        is None
    )

    own_gap_before_run = {
        hour: (value,) for i, hour in enumerate(hours7) if i != 0
    }  # the gap is hour index 0 ("hour 1"), outside the run (indices 1-6)
    result = assess(
        own_gap_before_run,
        others7,
        window=hours7,
        new_hours=(),
        episode=None,
        rule=_RULE,
    )
    assert result == Episode(value, hours7[1], "suspected")


def test_at4_too_few_others() -> None:
    """AT4: too few others opens no episode; an open episode with too few
    others, or with no reading of its own station, goes to ``cant_assess``
    instead of clearing.

    Catches: "recovered" on missing data (the row deleted); a fourth state.
    """
    hours6 = [_h(i) for i in range(6)]
    value = 30.0
    own = {hour: (value,) for hour in hours6}
    two_others = [{hour: (9.0,) for hour in hours6} for _ in range(2)]  # B, C only
    assert (
        assess(own, two_others, window=hours6, new_hours=(), episode=None, rule=_RULE)
        is None
    )

    episode = Episode(value, hours6[0], "suspected")
    next_hour = _h(6)

    own_unchanged = {next_hour: (value,)}
    two_others_next = [{next_hour: (9.0,)} for _ in range(2)]
    result = assess(
        own_unchanged,
        two_others_next,
        window=[],
        new_hours=(next_hour,),
        episode=episode,
        rule=_RULE,
    )
    assert result == Episode(value, episode.since, "cant_assess")

    own_missing = {}
    windy_next = _windy_at(next_hour)
    result2 = assess(
        own_missing,
        windy_next,
        window=[],
        new_hours=(next_hour,),
        episode=episode,
        rule=_RULE,
    )
    assert result2 == Episode(value, episode.since, "cant_assess")


def test_at5_a_changed_reading() -> None:
    """AT5: a reading that differs by a float ULP ends an open episode;
    in a multi-hour catch-up a change in the first new hour clears it even
    though the later hours still hold the value.

    Catches: a rounded or text compare; only the newest hour checked; a
    rounded compare when a run opens.
    """
    episode = Episode(30.0, _h(0), "suspected")
    next_hour = _h(1)
    own_changed = {next_hour: (30.0, 30.000000000000004)}
    result = assess(
        own_changed,
        _windy_at(next_hour),
        window=[],
        new_hours=(next_hour,),
        episode=episode,
        rule=_RULE,
    )
    assert result is None

    h1, h2, h3 = _h(1), _h(2), _h(3)
    own_catchup = {h1: (30.000000000000004,), h2: (30.0,), h3: (30.0,)}
    others_catchup = [{h1: (v,), h2: (v,), h3: (v,)} for v in _WINDY]
    result2 = assess(
        own_catchup,
        others_catchup,
        window=[],
        new_hours=(h1, h2, h3),
        episode=episode,
        rule=_RULE,
    )
    assert result2 is None, (
        "the change in the first new hour must clear the episode even though"
        " the last two new hours still hold the old value"
    )

    hours6 = [_h(i) for i in range(6)]
    others6 = _windy_others(hours6)
    own_ulp_in_run = {
        hour: (30.0, 30.000000000000004, 30.0) if hour == _h(3) else (30.0,)
        for hour in hours6
    }
    result3 = assess(
        own_ulp_in_run, others6, window=hours6, new_hours=(), episode=None, rule=_RULE
    )
    assert result3 is None, (
        "a float-ULP-different reading in a middle hour must never open a run"
    )

    own_no_ulp = {hour: (30.0,) for hour in hours6}
    result4 = assess(
        own_no_ulp, others6, window=hours6, new_hours=(), episode=None, rule=_RULE
    )
    assert result4 == Episode(30.0, hours6[0], "suspected"), (
        "the same 6 hours with every value equal must still open the run"
    )


def test_at7_hour_values_not_aggregate() -> None:
    """AT7: a station whose readings alternate inside each hour (mean 30.0
    every hour) must not read as stuck.

    Catches: a compare of hourly means or of pre-aggregated values.
    """
    hours6 = [_h(i) for i in range(6)]
    own = {hour: (20.0, 40.0) for hour in hours6}
    others6 = _windy_others(hours6)
    assert (
        assess(own, others6, window=hours6, new_hours=(), episode=None, rule=_RULE)
        is None
    )


# ---------------------------------------------------------------------------
# AT8: source order, ``hour_winds`` (plan §1 A1).
# ---------------------------------------------------------------------------


def test_at8_source_order() -> None:
    """AT8: an hour with ``five_minute`` readings holds them, ignoring a
    ``current_obs`` reading of the same hour; with no ``five_minute``
    reading, the ``current_obs`` readings are used.

    Catches: sources mixed (the stuck run broken by a different endpoint).
    """
    hour = _h(0)
    mixed = [
        _Reading(hour, "five_minute", 30.0),
        _Reading(hour, "five_minute", 30.0),
        _Reading(hour, "current_obs", 31.0),
    ]
    winds = hour_winds(mixed)
    assert winds[hour] == (30.0, 30.0)

    hour2 = _h(1)
    current_only = [_Reading(hour2, "current_obs", 31.0)]
    winds2 = hour_winds(current_only)
    assert winds2[hour2] == (31.0,)


# ---------------------------------------------------------------------------
# AT6, AT9-AT12: the hourly run and the monitor rows (plan §1 A3, A5).
# A real, file-backed SQLite database for each test (``run_migrations``).
# ---------------------------------------------------------------------------


def _file_conn(tmp_path: Path, name: str) -> sqlite3.Connection:
    # Left close ON, production's permanent state (the v17 upgrade stamps
    # DAILY_CLOSE_ON_KEY on every fresh build, tests.helpers.close_off's own
    # docstring) -- AT6 and AT12's build_verdict calls must see it on, the
    # same as a live install. AT9-AT11 never call build_verdict, so the
    # close switch makes no difference to them.
    conn = sqlite3.connect(str(tmp_path / name))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    run_migrations(conn)
    conn.commit()
    return conn


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    return asof_make_site(conn, name)


def _make_station(
    conn: sqlite3.Connection, site_id: int, letter: str, *, enabled: int = 1
) -> int:
    pws_id = f"TEST{letter}01"
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
        " dem_elevation_m, enabled) VALUES (?, ?, 1.0, 1.0, 1.0, ?)",
        (site_id, pws_id, enabled),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_reading(
    conn: sqlite3.Connection,
    station_id: int,
    at: datetime,
    *,
    speed_kmh: float,
    source: str = "five_minute",
) -> None:
    conn.execute(
        "INSERT INTO station_wind_records"
        " (station_id, obs_at, source, local_date, speed_kmh)"
        " VALUES (?, ?, ?, ?, ?)",
        (station_id, isoformat_utc(at), source, at.date().isoformat(), speed_kmh),
    )


def _insert_episode_row(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    speed_kmh: float,
    since: datetime,
    state: str,
) -> None:
    conn.execute(
        "INSERT INTO station_wind_constant (station_id, speed_kmh, since, state)"
        " VALUES (?, ?, ?, ?)",
        (station_id, speed_kmh, isoformat_utc(since), state),
    )


def test_at6_one_alert_per_episode_one_station(tmp_path: Path) -> None:
    """AT6, first case: through ``assess_constant_wind`` on a real file
    database, ``build_verdict`` after each hourly step. A stuck, others
    windy; then three calm hours (A goes to "can't assess", never
    "recovered"); then windy again (A is still stuck).

    Catches: the episode closed by a non-assessable hour (two
    absent-to-failing turns instead of one); the row built only when the
    daily close is off.
    """
    conn = _file_conn(tmp_path, "at6-one.db")
    assert daily_close_on(conn) is True, "close on: a fresh database starts this way"
    site_id = _make_site(conn, "at6-one-site")
    station_a = _make_station(conn, site_id, "A")
    others = [_make_station(conn, site_id, letter) for letter in ("B", "C", "D")]
    pws_a = "TESTA01"
    cond_id = f"wind_station_constant:{pws_a}"

    present: list[bool] = []
    details: list[str | None] = []
    for i in range(12):
        hour = _h(i)
        _insert_reading(conn, station_a, hour, speed_kmh=30.0)
        windy = not (6 <= i <= 8)
        for other_id, value in zip(others, _WINDY, strict=True):
            _insert_reading(conn, other_id, hour, speed_kmh=value if windy else 0.0)
        conn.commit()

        now_utc = hour + timedelta(hours=1, minutes=15)
        assess_constant_wind(conn, now_utc=now_utc)
        conn.commit()

        verdict = build_verdict(
            conn,
            pipeline_enabled=True,
            budget_enabled=False,
            db_enabled=False,
            now=now_utc,
            export_sweeper_dead=None,
        )
        cond = next(
            (c for c in verdict["conditions"] if c["id"] == cond_id),
            None,  # type: ignore[index]
        )
        present.append(cond is not None)
        details.append(None if cond is None else str(cond["detail"]))  # type: ignore[index]

    transitions = sum(
        1 for i in range(len(present)) if present[i] and not (i and present[i - 1])
    )
    assert transitions == 1, (
        f"close on: the id must turn from absent to failing exactly once: {present!r}"
    )
    first = present.index(True)
    assert all(present[first:]), (
        f"close on: once failing, the id must stay failing through the calm"
        f" hours: {present!r}"
    )
    for i in (6, 7, 8):
        assert details[i] is not None and details[i].endswith("(can't assess)"), (
            f"close on: step {i} must show the can't-assess detail, got {details[i]!r}"
        )
    for i in range(first, 6):
        assert details[i] is not None and not details[i].endswith("(can't assess)"), (
            f"close on: step {i} must not show the can't-assess detail"
        )


def test_at6_two_stations_stuck_at_once(tmp_path: Path) -> None:
    """AT6, second case: two stations stuck at once, stepped the same way
    over 10 hours. A stuck from the first hour; B windy for two hours then
    stuck from the third; C, D, E windy throughout; in the tenth hour one
    of A's readings changes.

    Catches: one shared id (B's spell would add no id of its own); one
    station's clear dropping the other's id; the rows built only when the
    daily close is off.
    """
    conn = _file_conn(tmp_path, "at6-two.db")
    assert daily_close_on(conn) is True, "close on: a fresh database starts this way"
    site_id = _make_site(conn, "at6-two-site")
    station_a = _make_station(conn, site_id, "A")
    station_b = _make_station(conn, site_id, "B")
    others = [_make_station(conn, site_id, letter) for letter in ("C", "D", "E")]
    pws_a, pws_b = "TESTA01", "TESTB01"
    cond_a_id = f"wind_station_constant:{pws_a}"
    cond_b_id = f"wind_station_constant:{pws_b}"

    present_a: list[bool] = []
    present_b: list[bool] = []
    for i in range(10):
        hour = _h(i)
        _insert_reading(conn, station_a, hour, speed_kmh=31.0 if i == 9 else 30.0)
        _insert_reading(conn, station_b, hour, speed_kmh={0: 9.0, 1: 11.0}.get(i, 20.0))
        for other_id, value in zip(others, _WINDY, strict=True):
            _insert_reading(conn, other_id, hour, speed_kmh=value)
        conn.commit()

        now_utc = hour + timedelta(hours=1, minutes=15)
        assess_constant_wind(conn, now_utc=now_utc)
        conn.commit()

        verdict = build_verdict(
            conn,
            pipeline_enabled=True,
            budget_enabled=False,
            db_enabled=False,
            now=now_utc,
            export_sweeper_dead=None,
        )
        ids = {str(c["id"]) for c in verdict["conditions"]}  # type: ignore[index]
        present_a.append(cond_a_id in ids)
        present_b.append(cond_b_id in ids)

    def _turns_on_once(flags: list[bool]) -> bool:
        turns = sum(
            1 for i in range(len(flags)) if flags[i] and not (i and flags[i - 1])
        )
        return turns == 1

    assert _turns_on_once(present_a), f"close on: A: {present_a!r}"
    assert _turns_on_once(present_b), f"close on: B: {present_b!r}"
    assert any(a and b for a, b in zip(present_a, present_b, strict=True)), (
        "close on: both ids must be failing together for at least one step"
        f" (A={present_a!r}, B={present_b!r})"
    )
    assert not present_a[-1], (
        "close on: A must have left the failing list after the tenth hour"
    )
    assert present_b[-1], (
        "close on: B must still be in the failing list after the tenth hour"
    )


def test_at9_the_gate(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """AT9: a second call in the same hour reads only the key and writes
    nothing; an unreadable stored stamp warns once and the run still
    happens.

    Catches: a full assessment on every tick; a wedged gate.
    """
    conn = _file_conn(tmp_path, "at9.db")
    site_id = _make_site(conn, "at9-site")
    _make_station(conn, site_id, "A")
    conn.commit()

    now1 = _h(1) + timedelta(minutes=15)
    assess_constant_wind(conn, now_utc=now1)
    conn.commit()
    stored_after_first = get_runtime_state(conn, ASSESSED_THROUGH_KEY)
    assert stored_after_first is not None

    reads = {"n": 0}

    def _authorizer(
        action_code: int,
        arg1: str | None,
        arg2: str | None,
        dbname: str | None,
        source: str | None,
    ) -> int:
        if action_code == sqlite3.SQLITE_READ and arg1 == "station_wind_records":
            reads["n"] += 1
        return sqlite3.SQLITE_OK

    conn.set_authorizer(_authorizer)
    try:
        assess_constant_wind(conn, now_utc=now1)
    finally:
        conn.set_authorizer(None)

    assert reads["n"] == 0, (
        "the second call in the same hour must not read the raw readings"
    )
    assert get_runtime_state(conn, ASSESSED_THROUGH_KEY) == stored_after_first, (
        "the second call must write nothing"
    )

    set_runtime_state(conn, ASSESSED_THROUGH_KEY, "not-a-stamp")
    conn.commit()
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="wxverify.db.constant_wind"):
        assess_constant_wind(conn, now_utc=now1 + timedelta(hours=1))
    conn.commit()
    assert any("unreadable" in r.getMessage() for r in caplog.records), (
        "an unreadable stored stamp must log one WARNING"
    )
    new_value = get_runtime_state(conn, ASSESSED_THROUGH_KEY)
    assert new_value is not None and new_value != "not-a-stamp", (
        "the run must still happen (and write a readable value) on an"
        " unreadable stored stamp"
    )


def test_at10_rows_that_end(tmp_path: Path) -> None:
    """AT10: a switched-off station's open episode is deleted by the next
    run; a deleted station's row is removed at once by the cascade.

    Catches: a stale episode on a switched-off station or a reused id.
    """
    conn = _file_conn(tmp_path, "at10.db")
    site_id = _make_site(conn, "at10-site")
    station_off = _make_station(conn, site_id, "A")
    station_deleted = _make_station(conn, site_id, "B")
    _insert_episode_row(
        conn, station_off, speed_kmh=30.0, since=_h(0), state="suspected"
    )
    _insert_episode_row(
        conn, station_deleted, speed_kmh=30.0, since=_h(0), state="suspected"
    )
    conn.commit()

    conn.execute("UPDATE stations SET enabled = 0 WHERE id = ?", (station_off,))
    conn.commit()
    assess_constant_wind(conn, now_utc=_h(1) + timedelta(minutes=15))
    conn.commit()
    row = conn.execute(
        "SELECT 1 FROM station_wind_constant WHERE station_id = ?", (station_off,)
    ).fetchone()
    assert row is None, "a switched-off station's open episode must be deleted"

    conn.execute("DELETE FROM stations WHERE id = ?", (station_deleted,))
    conn.commit()
    row2 = conn.execute(
        "SELECT 1 FROM station_wind_constant WHERE station_id = ?",
        (station_deleted,),
    ).fetchone()
    assert row2 is None, "a deleted station's row must be removed by the cascade"


def test_at11_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """AT11: all three options.json keys reach the rule through
    ``load_runtime_options`` and ``apply_plain_settings``, driven through
    ``assess_constant_wind`` on a real file database; a stored value below
    the schema minimum reads the default.

    Catches: ``wind_constant_other_stations`` (or either of the other two
    keys) dropped from options.json, from ``load_runtime_options``, from
    ``apply_plain_settings``, or from ``read_rule``'s ``Rule``; the minimum
    not applied.
    """

    def _apply_options_json(conn: sqlite3.Connection, name: str) -> None:
        db = _RealDb(conn)
        monkeypatch.setattr("wxverify.settings.service.get_db", lambda: db)
        options_file = tmp_path / name
        options_file.write_text(
            json.dumps(
                {
                    "wind_constant_hours": 4,
                    "wind_constant_other_stations": 2,
                    "wind_constant_median_ms": 2,
                }
            ),
            encoding="utf-8",
        )
        asyncio.run(apply_plain_settings(load_runtime_options(str(options_file))))

    conn = _file_conn(tmp_path, "at11.db")
    _apply_options_json(conn, "options.json")
    rule = read_rule(conn)
    assert rule == Rule(hours=4, other_stations=2, median_kmh=7.2), (
        "all three options.json keys (hours, other_stations, median_ms) must"
        " reach the rule through load_runtime_options, apply_plain_settings"
        " and read_rule"
    )

    hours4 = [_h(i) for i in range(4)]
    site_id = _make_site(conn, "at11-hit-site")
    station_a = _make_station(conn, site_id, "A")
    others = [_make_station(conn, site_id, letter) for letter in ("B", "C")]
    conn.commit()
    for hour in hours4:
        _insert_reading(conn, station_a, hour, speed_kmh=30.0)
        for other_id in others:
            _insert_reading(conn, other_id, hour, speed_kmh=7.2)
    conn.commit()
    assess_constant_wind(conn, now_utc=hours4[-1] + timedelta(hours=1, minutes=15))
    conn.commit()
    row = conn.execute(
        "SELECT 1 FROM station_wind_constant WHERE station_id = ?", (station_a,)
    ).fetchone()
    assert row is not None, (
        "a 4-hour stuck run with exactly 2 others at the new 7.2 km/h median"
        " must open a row; the defaults (6 hours, 3 others) would not"
    )

    conn_miss = _file_conn(tmp_path, "at11-below-median.db")
    _apply_options_json(conn_miss, "options-below-median.json")
    site_id_miss = _make_site(conn_miss, "at11-miss-site")
    station_a_miss = _make_station(conn_miss, site_id_miss, "A")
    others_miss = [
        _make_station(conn_miss, site_id_miss, letter) for letter in ("B", "C")
    ]
    conn_miss.commit()
    for hour in hours4:
        _insert_reading(conn_miss, station_a_miss, hour, speed_kmh=30.0)
        for other_id in others_miss:
            _insert_reading(conn_miss, other_id, hour, speed_kmh=7.0)
    conn_miss.commit()
    assess_constant_wind(conn_miss, now_utc=hours4[-1] + timedelta(hours=1, minutes=15))
    conn_miss.commit()
    row_miss = conn_miss.execute(
        "SELECT 1 FROM station_wind_constant WHERE station_id = ?",
        (station_a_miss,),
    ).fetchone()
    assert row_miss is None, (
        "7.0 km/h must not be assessable against the new 7.2 km/h median"
    )

    conn_min = _file_conn(tmp_path, "at11-min.db")
    set_setting(conn_min, "wind_constant_hours", "1")
    conn_min.commit()
    rule_min = read_rule(conn_min)
    assert rule_min.hours == 6, (
        "a stored value below the schema minimum (2) must read the default (6)"
    )


def test_at12_monitor_rows(tmp_path: Path) -> None:
    """AT12: the ``wind_station_constant`` monitor rows, with the daily
    close on (production's permanent state) and again with it off.

    Catches: one shared id; an ``ok`` row with no episode; a detail naming
    another station; a disabled station's row counted; the rows built only
    when the daily close is off.
    """
    now = _h(100)
    conn = _file_conn(tmp_path, "at12.db")
    assert daily_close_on(conn) is True, "close on: a fresh database starts this way"
    site_id = _make_site(conn, "at12-site")
    disabled_site_id = _make_site(conn, "at12-disabled-site")
    conn.execute("UPDATE sites SET enabled = 0 WHERE id = ?", (disabled_site_id,))
    station_a = _make_station(conn, site_id, "A")
    station_b = _make_station(conn, site_id, "B")
    station_disabled = _make_station(conn, site_id, "C", enabled=0)
    station_on_disabled_site = _make_station(conn, disabled_site_id, "D")
    conn.commit()

    pws_a, pws_b = "TESTA01", "TESTB01"
    cond_a_id, cond_b_id = (
        f"wind_station_constant:{pws_a}",
        f"wind_station_constant:{pws_b}",
    )

    def _mode() -> str:
        return "close on" if daily_close_on(conn) else "close off"

    def _verdict(*, pipeline_enabled: bool = True) -> dict[str, object]:
        return build_verdict(
            conn,
            pipeline_enabled=pipeline_enabled,
            budget_enabled=False,
            db_enabled=False,
            now=now,
            export_sweeper_dead=None,
        )

    def _wind_ids(verdict: dict[str, object]) -> set[str]:
        return {
            str(c["id"])  # type: ignore[index]
            for c in verdict["conditions"]  # type: ignore[index]
            if str(c["id"]).startswith("wind_station_constant")  # type: ignore[index]
        }

    def _wind_ids_in_order() -> list[str]:
        return [
            str(c["id"])  # type: ignore[index]
            for c in _verdict()["conditions"]  # type: ignore[index]
            if str(c["id"]).startswith("wind_station_constant")  # type: ignore[index]
        ]

    assert _wind_ids(_verdict()) == set(), (
        f"{_mode()}: an empty table must build no condition"
    )

    _insert_episode_row(conn, station_a, speed_kmh=30.0, since=_h(0), state="suspected")
    _insert_episode_row(
        conn, station_b, speed_kmh=20.0, since=_h(1), state="cant_assess"
    )
    _insert_episode_row(
        conn, station_disabled, speed_kmh=25.0, since=_h(0), state="suspected"
    )
    _insert_episode_row(
        conn, station_on_disabled_site, speed_kmh=25.0, since=_h(0), state="suspected"
    )
    conn.commit()

    verdict = _verdict()
    conditions = [
        c
        for c in verdict["conditions"]  # type: ignore[index]
        if str(c["id"]).startswith("wind_station_constant")  # type: ignore[index]
    ]
    ids_in_order = [str(c["id"]) for c in conditions]  # type: ignore[index]
    assert ids_in_order == [cond_a_id, cond_b_id], (
        f"{_mode()}: exactly A's and B's ids, in station order; the disabled"
        " station and the station of the disabled site must have no"
        " condition"
    )
    cond_a, cond_b = conditions
    assert cond_a["ok"] is False and cond_b["ok"] is False, (  # type: ignore[index]
        f"{_mode()}: both rows must be failing"
    )
    assert pws_a in str(cond_a["detail"]) and pws_b not in str(cond_a["detail"]), (  # type: ignore[index]
        f"{_mode()}: A's detail must name only A"
    )
    assert pws_b in str(cond_b["detail"]) and pws_a not in str(cond_b["detail"]), (  # type: ignore[index]
        f"{_mode()}: B's detail must name only B"
    )
    assert str(cond_b["detail"]).endswith("(can't assess)"), (  # type: ignore[index]
        f"{_mode()}: B's detail must show the can't-assess suffix"
    )
    assert not str(cond_a["detail"]).endswith("(can't assess)"), (  # type: ignore[index]
        f"{_mode()}: A's detail must not show the can't-assess suffix"
    )

    assert _wind_ids(_verdict(pipeline_enabled=False)) == set(), (
        f"{_mode()}: monitor_pipeline off must show no wind_station_constant id"
    )

    set_runtime_state(
        conn, "worker_started_at", isoformat_utc(now - timedelta(minutes=5))
    )
    conn.commit()
    grace_conditions = {
        str(c["id"]): c  # type: ignore[index]
        for c in _verdict()["conditions"]  # type: ignore[index]
        if str(c["id"]).startswith("wind_station_constant")  # type: ignore[index]
    }
    assert grace_conditions[cond_a_id]["ok"] is True, (  # type: ignore[index]
        f"{_mode()}: the start-up grace must hold A's row ok"
    )
    assert grace_conditions[cond_b_id]["ok"] is True, (  # type: ignore[index]
        f"{_mode()}: the start-up grace must hold B's row ok"
    )

    close_off(conn)
    conn.commit()
    assert daily_close_on(conn) is False, "close_off must turn the close off"

    assert _wind_ids_in_order() == [cond_a_id, cond_b_id], (
        f"{_mode()}: exactly A's and B's ids, in station order, must still"
        " show with the close off"
    )
    assert _wind_ids(_verdict(pipeline_enabled=False)) == set(), (
        f"{_mode()}: monitor_pipeline off must show no wind_station_constant id"
    )


# ---------------------------------------------------------------------------
# AT13: the wiring -- scheduler_tick calls assess_constant_wind (not in the
# plan; added here).
# ---------------------------------------------------------------------------


def test_at13_scheduler_tick_writes_the_assessed_through_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AT13: a real ``scheduler_tick``, with the daily close on, writes
    ``wind_constant_assessed_through``.

    Catches: the call to ``assess_constant_wind`` dropped from
    ``scheduler_tick``.
    """

    async def _run() -> None:
        db, _writer, _site_id, _stations = standard_night(tmp_path)
        disable_other_enqueuers(monkeypatch)
        clock = MovingClock(NOW)
        patch_close_clock(monkeypatch, clock)

        await tick(db, clock)

        value = await db.read(lambda c: get_runtime_state(c, ASSESSED_THROUGH_KEY))
        assert value is not None, (
            "scheduler_tick must call assess_constant_wind, which writes"
            " wind_constant_assessed_through"
        )

    asyncio.run(_run())
