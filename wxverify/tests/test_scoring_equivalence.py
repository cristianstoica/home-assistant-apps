"""Equivalence oracle: 0.1.0 monolithic scoring rebuild vs the live pipeline.

``_ref_*`` below are verbatim copies of the 0.1.0 ``pair_real_models``,
``materialize_persistence`` (delete + full rebuild, no anti-join) and the
0.1.0 score tail. Bug 2 of the 0.1.1 patch replaces the live implementations
with an anti-joined pairing pass and an incremental persistence
materializer; this oracle asserts full end-state equivalence of
``forecast_pairs`` and ``score_cache`` between the frozen reference pipeline
and the live ``pair_and_score`` across representative mutation scenarios.

Because the reference rebuilds persistence from scratch on every run, it can
never carry a stale row — so any pair the incremental path wrongly retains
(i.e. any hole in the consensus-invalidation contract) shows up as a row
diff here.

All fixture data is synthetic.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import timedelta

from wxverify.core.timeutil import (
    day_ahead,
    floor_hour,
    isoformat_utc,
    parse_utc,
    utc_now,
    window_cutoff,
)
from wxverify.db.migrations import run_migrations
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.consensus import insert_station_observation, materialize_consensus
from wxverify.scoring.engine import pair_and_score
from wxverify.scoring.metrics import strategy_for
from wxverify.scoring.multimodel import materialize_multimodel_mean
from wxverify.scoring.pair_flags import precip_flags
from wxverify.settings.keys import get_number_setting

# --------------------------------------------------------------------------
# Reference implementations (verbatim 0.1.0 behavior — do not "improve").
# --------------------------------------------------------------------------


def _ref_pair_real_models(conn: sqlite3.Connection, site_id: int | None = None) -> int:
    params: tuple[object, ...]
    where_site = ""
    if site_id is None:
        params = ()
    else:
        where_site = "AND fs.site_id = ?"
        params = (site_id,)
    rows = conn.execute(
        f"""
        SELECT fs.site_id, fs.feed_id, fs.variable, fs.issued_at, fs.valid_at,
               fs.lead_hours, fs.value AS forecast, obs.value AS observed,
               s.timezone, s.rain_threshold_mm
        FROM forecast_samples fs
        JOIN observations obs
          ON obs.site_id = fs.site_id
         AND obs.variable = fs.variable
         AND obs.valid_at = fs.valid_at
        JOIN feeds f ON f.id = fs.feed_id
        JOIN sites s ON s.id = fs.site_id
        WHERE f.is_virtual = 0
          AND fs.lead_hours BETWEEN 1 AND f.max_lead_hours
          {where_site}
        """,
        params,
    ).fetchall()
    written = 0
    for row in rows:
        bucket = day_ahead(
            str(row["issued_at"]), str(row["valid_at"]), str(row["timezone"])
        )
        if bucket < 0 or bucket > 7:
            continue
        forecast = float(row["forecast"])
        observed = float(row["observed"])
        variable = str(row["variable"])
        rain_threshold = (
            float(row["rain_threshold_mm"]) if variable == "precip" else None
        )
        hit, false, miss, correct_neg = precip_flags(
            variable, forecast, observed, rain_threshold
        )
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 cat_hit, cat_false, cat_miss, cat_correct_neg,
                 rain_threshold_mm)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(row["site_id"]),
                int(row["feed_id"]),
                variable,
                str(row["issued_at"]),
                str(row["valid_at"]),
                int(row["lead_hours"]),
                bucket,
                forecast,
                observed,
                forecast - observed,
                abs(forecast - observed),
                (forecast - observed) ** 2,
                hit,
                false,
                miss,
                correct_neg,
                rain_threshold,
            ),
        )
        written += cur.rowcount
    return written


def _ref_materialize_persistence(
    conn: sqlite3.Connection, site_id: int | None = None
) -> int:
    feed = conn.execute(
        """
        SELECT id, max_lead_hours
        FROM feeds
        WHERE source='virtual' AND model='_persistence'
        """
    ).fetchone()
    if feed is None:
        return 0
    if site_id is None:
        conn.execute("DELETE FROM forecast_pairs WHERE feed_id=?", (int(feed["id"]),))
    else:
        conn.execute(
            "DELETE FROM forecast_pairs WHERE site_id=? AND feed_id=?",
            (site_id, int(feed["id"])),
        )
    where = "" if site_id is None else "WHERE site_id = ?"
    params: tuple[object, ...] = () if site_id is None else (site_id,)
    observations = conn.execute(
        f"""
        SELECT o.site_id, o.variable, o.valid_at, o.value, s.timezone,
               s.rain_threshold_mm
        FROM observations o
        JOIN sites s ON s.id = o.site_id
        {where}
        """,
        params,
    ).fetchall()
    written = 0
    max_lead = int(feed["max_lead_hours"])
    for obs in observations:
        valid = parse_utc(str(obs["valid_at"]))
        for lead in range(1, max_lead + 1):
            issued_at = isoformat_utc(valid - timedelta(hours=lead))
            source_valid = isoformat_utc(valid - timedelta(hours=lead))
            lagged = conn.execute(
                """
                SELECT value FROM observations
                WHERE site_id=? AND variable=? AND valid_at=?
                """,
                (int(obs["site_id"]), str(obs["variable"]), source_valid),
            ).fetchone()
            if lagged is None:
                continue
            bucket = day_ahead(issued_at, str(obs["valid_at"]), str(obs["timezone"]))
            if bucket < 0 or bucket > 7:
                continue
            forecast = float(lagged["value"])
            observed = float(obs["value"])
            variable = str(obs["variable"])
            rain_threshold = (
                float(obs["rain_threshold_mm"]) if variable == "precip" else None
            )
            hit, false, miss, correct_neg = precip_flags(
                variable, forecast, observed, rain_threshold
            )
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO forecast_pairs
                    (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                     day_ahead, forecast, observed, error, abs_error, sq_error,
                     cat_hit, cat_false, cat_miss, cat_correct_neg,
                     rain_threshold_mm)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(obs["site_id"]),
                    int(feed["id"]),
                    variable,
                    issued_at,
                    str(obs["valid_at"]),
                    lead,
                    bucket,
                    forecast,
                    observed,
                    forecast - observed,
                    abs(forecast - observed),
                    (forecast - observed) ** 2,
                    hit,
                    false,
                    miss,
                    correct_neg,
                    rain_threshold,
                ),
            )
            written += cur.rowcount
    return written


def _ref_clear_score_cache(conn: sqlite3.Connection, site_id: int | None) -> None:
    if site_id is None:
        conn.execute("DELETE FROM score_cache")
        return
    conn.execute("DELETE FROM score_cache WHERE site_id=?", (site_id,))


def _ref_score_window(
    conn: sqlite3.Connection,
    site_id: int | None,
    window_key: str,
    cutoff: str | None,
    min_n: int,
) -> None:
    params: tuple[object, ...]
    where = ""
    if site_id is None:
        params = ()
    else:
        where = "WHERE site_id = ?"
        params = (site_id,)
    cells = conn.execute(
        f"""
        SELECT DISTINCT site_id, feed_id, variable, day_ahead
        FROM forecast_pairs
        {where}
        """,
        params,
    ).fetchall()
    now = isoformat_utc()
    for cell in cells:
        result = strategy_for(str(cell["variable"])).aggregate(
            conn,
            site_id=int(cell["site_id"]),
            feed_id=int(cell["feed_id"]),
            variable=str(cell["variable"]),
            day_ahead=int(cell["day_ahead"]),
            window_cutoff=cutoff,
            min_n=min_n,
        )
        if result.n == 0:
            continue
        upsert_score_cache(
            conn,
            site_id=int(cell["site_id"]),
            feed_id=int(cell["feed_id"]),
            variable=str(cell["variable"]),
            day_ahead=int(cell["day_ahead"]),
            window_key=window_key,
            result=result,
            computed_at=now,
        )


def _ref_pair_and_score(conn: sqlite3.Connection, site_id: int | None = None) -> None:
    _ref_pair_real_models(conn, site_id)
    _ref_materialize_persistence(conn, site_id)
    # Multimodel stays delete+rebuild in 0.1.1 (load-bearing) — live import.
    materialize_multimodel_mean(conn, site_id)
    _ref_clear_score_cache(conn, site_id)
    rolling_days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    _ref_score_window(
        conn, site_id, f"w:{rolling_days}", window_cutoff(rolling_days), min_n
    )
    _ref_score_window(conn, site_id, "w:all", None, min_n)


# --------------------------------------------------------------------------
# Synthetic fixture (two sites, two stations each, three variables).
# --------------------------------------------------------------------------

_OBS_HOURS = 60
_BASE = floor_hour(utc_now()) - timedelta(hours=_OBS_HOURS + 12)
_SOURCE_RAW = '{"synthetic": true}'


def _hour(index: int) -> str:
    return isoformat_utc(_BASE + timedelta(hours=index))


def _obs_value(variable: str, station: int, hour: int) -> float:
    if variable == "temperature":
        return 10.0 + 0.3 * (hour % 9) + 0.1 * station
    if variable == "wind":
        return 3.0 + 0.2 * (hour % 5) + 0.05 * station
    # precip: mostly dry, periodic wet hours straddling the 0.2 mm threshold
    if hour % 6 == 0:
        return 0.5 + 0.01 * station
    if hour % 6 == 3:
        return 0.1
    return 0.0


def _forecast_value(variable: str, feed_index: int, lead: int, hour: int) -> float:
    if variable == "temperature":
        return 10.0 + 0.3 * (hour % 9) + 0.02 * lead + 0.5 * feed_index
    if variable == "wind":
        return 3.0 + 0.2 * (hour % 5) + 0.01 * lead
    return 0.4 if hour % 6 == 0 else 0.0


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)

    for name in ("Test Alpha", "Test Beta"):
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                               timezone, rain_threshold_mm)
            VALUES (?, 47.0, 25.0, 900.0, ?, 0.2)
            """,
            (name, "Europe/Bucharest" if name == "Test Alpha" else "UTC"),
        )
    station_ids: dict[int, list[int]] = {}
    for site_id in (1, 2):
        station_ids[site_id] = []
        for n in (1, 2):
            cur = conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, ?, 47.0, 25.0, ?, 1)
                """,
                (site_id, f"TESTPWS{site_id}{n:03d}", 900.0 + 5.0 * n),
            )
            station_ids[site_id].append(int(cur.lastrowid or 0))

    for stations in station_ids.values():
        for hour in range(_OBS_HOURS):
            for variable in ("temperature", "wind", "precip"):
                for offset, station_id in enumerate(stations):
                    insert_station_observation(
                        conn,
                        station_id=station_id,
                        variable=variable,
                        valid_at=_hour(hour),
                        value=_obs_value(variable, offset, hour),
                        source_raw=_SOURCE_RAW,
                    )

    feeds = conn.execute(
        """
        SELECT id FROM feeds
        WHERE source='open-meteo' AND is_virtual=0
        ORDER BY id LIMIT 2
        """
    ).fetchall()
    assert len(feeds) == 2
    for feed_index, feed in enumerate(feeds):
        for site_id in (1, 2):
            for issued_hour in (-24, 0, 24):
                for valid_hour in range(0, _OBS_HOURS, 3):
                    lead = valid_hour - issued_hour
                    if lead < 1 or lead > 168:
                        continue
                    for variable in ("temperature", "wind", "precip"):
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO forecast_samples
                                (site_id, feed_id, variable, issued_at, valid_at,
                                 lead_hours, value, source_raw, model_run_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                site_id,
                                int(feed["id"]),
                                variable,
                                _hour(issued_hour),
                                _hour(valid_hour),
                                lead,
                                _forecast_value(variable, feed_index, lead, valid_hour),
                                _SOURCE_RAW,
                                f"run-{issued_hour}",
                            ),
                        )
    conn.commit()
    return conn


# --------------------------------------------------------------------------
# Snapshots and scenario runner.
# --------------------------------------------------------------------------

_PAIR_COLS = (
    "site_id, feed_id, variable, issued_at, valid_at, lead_hours, day_ahead,"
    " forecast, observed, error, abs_error, sq_error, cat_hit, cat_false,"
    " cat_miss, cat_correct_neg, rain_threshold_mm, contributors"
)
_SCORE_COLS = (
    "site_id, feed_id, variable, day_ahead, window_key, n, bias, mae, rmse,"
    " pod, far, csi, ets, hss, skill_score"
)


def _pairs_snapshot(conn: sqlite3.Connection) -> list[tuple[object, ...]]:
    rows = conn.execute(
        f"""
        SELECT {_PAIR_COLS} FROM forecast_pairs
        ORDER BY site_id, feed_id, variable, issued_at, valid_at
        """
    ).fetchall()
    return [tuple(row) for row in rows]


def _scores_snapshot(conn: sqlite3.Connection) -> list[tuple[object, ...]]:
    rows = conn.execute(
        f"""
        SELECT {_SCORE_COLS} FROM score_cache
        ORDER BY site_id, feed_id, variable, day_ahead, window_key
        """
    ).fetchall()
    return [tuple(row) for row in rows]


Mutation = Callable[[sqlite3.Connection], None]


def _no_mutation(conn: sqlite3.Connection) -> None:
    del conn


def _add_obs_hour(conn: sqlite3.Connection) -> None:
    stations = conn.execute(
        "SELECT id FROM stations WHERE site_id=1 ORDER BY id"
    ).fetchall()
    for offset, row in enumerate(stations):
        for variable in ("temperature", "wind", "precip"):
            insert_station_observation(
                conn,
                station_id=int(row["id"]),
                variable=variable,
                valid_at=_hour(_OBS_HOURS),
                value=_obs_value(variable, offset, _OBS_HOURS),
                source_raw=_SOURCE_RAW,
            )


def _change_obs_value(conn: sqlite3.Connection) -> None:
    station = conn.execute(
        "SELECT id FROM stations WHERE site_id=1 ORDER BY id LIMIT 1"
    ).fetchone()
    assert station is not None
    insert_station_observation(
        conn,
        station_id=int(station["id"]),
        variable="temperature",
        valid_at=_hour(20),
        value=14.5,
        source_raw=_SOURCE_RAW,
    )


def _delete_obs_hour(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        DELETE FROM station_observations
        WHERE variable='wind' AND valid_at=?
          AND station_id IN (SELECT id FROM stations WHERE site_id=1)
        """,
        (_hour(30),),
    )
    materialize_consensus(conn, site_id=1, variable="wind", valid_at=_hour(30))


def _change_rain_threshold(conn: sqlite3.Connection) -> None:
    # Mirrors the sites rain-threshold PUT route (api/routes/sites.py).
    conn.execute("UPDATE sites SET rain_threshold_mm=0.4 WHERE id=1")
    conn.execute("DELETE FROM forecast_pairs WHERE site_id=1 AND variable='precip'")
    conn.execute("DELETE FROM score_cache WHERE site_id=1 AND variable='precip'")


def _set_station_enabled(enabled: int) -> Mutation:
    # Mirrors the station PUT route: toggle + rematerialize the station's hours.
    def mutate(conn: sqlite3.Connection) -> None:
        station = conn.execute(
            "SELECT id FROM stations WHERE site_id=1 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert station is not None
        station_id = int(station["id"])
        conn.execute("UPDATE stations SET enabled=? WHERE id=?", (enabled, station_id))
        keys = conn.execute(
            """
            SELECT DISTINCT variable, valid_at FROM station_observations
            WHERE station_id=?
            """,
            (station_id,),
        ).fetchall()
        for key in keys:
            materialize_consensus(
                conn,
                site_id=1,
                variable=str(key["variable"]),
                valid_at=str(key["valid_at"]),
            )

    return mutate


def _delete_station(conn: sqlite3.Connection) -> None:
    # Mirrors the station DELETE route.
    station = conn.execute(
        "SELECT id FROM stations WHERE site_id=2 ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert station is not None
    station_id = int(station["id"])
    keys = conn.execute(
        """
        SELECT DISTINCT variable, valid_at FROM station_observations
        WHERE station_id=?
        """,
        (station_id,),
    ).fetchall()
    conn.execute("DELETE FROM stations WHERE id=?", (station_id,))
    for key in keys:
        materialize_consensus(
            conn,
            site_id=2,
            variable=str(key["variable"]),
            valid_at=str(key["valid_at"]),
        )


_SCENARIOS: tuple[tuple[str, Mutation, int | None], ...] = (
    ("cold build all sites", _no_mutation, None),
    ("no-change rerun", _no_mutation, None),
    ("new obs hour", _add_obs_hour, 1),
    ("obs value change", _change_obs_value, 1),
    ("obs deletion", _delete_obs_hour, 1),
    ("rain threshold change", _change_rain_threshold, 1),
    ("station disable", _set_station_enabled(0), 1),
    ("station re-enable", _set_station_enabled(1), 1),
    ("station delete", _delete_station, 2),
    ("final all-sites rerun", _no_mutation, None),
)


def test_pipeline_equivalent_to_reference_rebuild() -> None:
    ref = _make_db()
    live = _make_db()
    try:
        for name, mutate, site_arg in _SCENARIOS:
            mutate(ref)
            mutate(live)
            _ref_pair_and_score(ref, site_arg)
            pair_and_score(live, site_arg)
            assert _pairs_snapshot(live) == _pairs_snapshot(ref), (
                f"forecast_pairs diverged after scenario: {name}"
            )
            assert _scores_snapshot(live) == _scores_snapshot(ref), (
                f"score_cache diverged after scenario: {name}"
            )
            assert len(_pairs_snapshot(ref)) > 0, f"empty oracle in scenario: {name}"
    finally:
        ref.close()
        live.close()
