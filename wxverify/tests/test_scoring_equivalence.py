"""Equivalence oracle: 0.1.0 monolithic scoring rebuild vs the live pipeline.

``_ref_*`` below are verbatim copies of the 0.1.0 ``pair_real_models``,
``materialize_persistence`` (delete + full rebuild, no anti-join) and the
0.1.0 score tail. The 0.1.1 patch replaces the live implementations
with an anti-joined pairing pass and an incremental persistence
materializer; this oracle asserts full end-state equivalence of
``forecast_pairs`` and ``score_cache`` between the frozen reference pipeline
and the live ``pair_and_score`` across representative mutation scenarios.

The multimodel reference is frozen: ``tests/scoring_ref_0164.py`` holds
``materialize_multimodel_mean_0164``, a byte-for-byte copy of the 0.16.4
(``b21f342``) multimodel mean body. It must never track the
live implementation.

Because the reference rebuilds persistence from scratch on every run, it can
never carry a stale row — so any pair the incremental path wrongly retains
(i.e. any hole in the consensus-invalidation contract) shows up as a row
diff here.

All fixture data is synthetic.
"""

from __future__ import annotations

import asyncio
import math
import sqlite3
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path

import pytest

from tests.scoring_ref_0164 import materialize_multimodel_mean_0164
from wxverify.core.timeutil import (
    day_ahead,
    floor_hour,
    isoformat_utc,
    parse_utc,
    utc_now,
    window_cutoff,
)
from wxverify.db.connection import FencedWriter, close_db, init_db
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import (
    ensure_published_generation,
    published_generation_id,
)
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.consensus import insert_station_observation, materialize_consensus
from wxverify.scoring.engine import pair_and_score
from wxverify.scoring.metrics import strategy_for
from wxverify.scoring.pair_flags import precip_flags
from wxverify.settings.keys import get_number_setting
from wxverify.worker.score_batches import run_batched_scoring, run_split_pair_phases

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
                 rain_threshold_mm, tz_generation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ensure_published_generation(conn, int(row["site_id"])),
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
                     rain_threshold_mm, tz_generation_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    ensure_published_generation(conn, int(obs["site_id"])),
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
    # Multimodel stays delete+rebuild (load-bearing): the 0.16.4 (b21f342)
    # body, frozen at tests/scoring_ref_0164.py.
    materialize_multimodel_mean_0164(conn, site_id)
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
            VALUES (?, 40.0, -105.0, 900.0, ?, 0.2)
            """,
            (name, "Europe/Athens" if name == "Test Alpha" else "UTC"),
        )
    station_ids: dict[int, list[int]] = {}
    for site_id in (1, 2):
        station_ids[site_id] = []
        for n in (1, 2):
            cur = conn.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, ?, 40.0, -105.0, ?, 1)
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
        ORDER BY site_id, feed_id, variable, issued_at, valid_at, tz_generation_id
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


def _assert_end_state_equal(
    actual: sqlite3.Connection, expected: sqlite3.Connection, *, label: str
) -> None:
    """§5.4: forecast_pairs/score_cache equivalence, against the frozen reference.

    ``tz_generation_id`` is not compared directly between the two snapshots:
    the live path and the reference seed generations in different orders, so
    the ids themselves can legitimately differ. Instead each arm's own pair
    rows are checked against that arm's own ``published_generation_id``.
    """
    for conn, which in ((actual, "actual"), (expected, "expected")):
        for row in conn.execute(
            "SELECT id, site_id, tz_generation_id FROM forecast_pairs"
        ).fetchall():
            expected_generation = published_generation_id(conn, int(row["site_id"]))
            assert row["tz_generation_id"] == expected_generation, (
                f"{label}: {which} pair id={row['id']} site={row['site_id']}"
                f" tz_generation_id={row['tz_generation_id']!r},"
                f" published_generation_id={expected_generation!r}"
            )

    expected_pairs = _pairs_snapshot(expected)
    assert len(expected_pairs) > 0, f"empty oracle in scenario: {label}"
    actual_pairs = _pairs_snapshot(actual)
    assert actual_pairs == expected_pairs, (
        f"forecast_pairs diverged after scenario: {label}"
    )

    actual_scores = _scores_snapshot(actual)
    expected_scores = _scores_snapshot(expected)
    assert len(actual_scores) == len(expected_scores), (
        f"score_cache row count diverged after scenario: {label}:"
        f" {len(actual_scores)} vs {len(expected_scores)}"
    )
    for a_row, e_row in zip(actual_scores, expected_scores, strict=True):
        for a_val, e_val in zip(a_row, e_row, strict=True):
            if isinstance(a_val, float) or isinstance(e_val, float):
                assert isinstance(a_val, float) and isinstance(e_val, float), (
                    f"score_cache diverged after scenario: {label}: {a_row} vs {e_row}"
                )
                assert math.isclose(a_val, e_val, rel_tol=1e-12, abs_tol=1e-12), (
                    f"score_cache diverged after scenario: {label}: {a_row} vs {e_row}"
                )
            else:
                assert a_val == e_val, (
                    f"score_cache diverged after scenario: {label}: {a_row} vs {e_row}"
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
            _assert_end_state_equal(live, ref, label=name)
    finally:
        ref.close()
        live.close()


_PAIR_CROSS_ARM_EXCLUDE = frozenset({"id", "created_at", "tz_generation_id"})


def _pairs_snapshot_excluding(
    conn: sqlite3.Connection, exclude: frozenset[str]
) -> list[dict[str, object]]:
    cols = [
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(forecast_pairs)").fetchall()
        if row["name"] not in exclude
    ]
    col_list = ", ".join(cols)
    rows = conn.execute(
        f"""
        SELECT {col_list} FROM forecast_pairs
        ORDER BY site_id, feed_id, variable, issued_at, valid_at
        """
    ).fetchall()
    return [dict(row) for row in rows]


def test_split_and_monolithic_equivalent_to_0164_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Three arms per scenario, all against the same frozen 0.16.4 reference:

    A. ``ref`` -- the frozen reference rebuild (``_ref_pair_and_score``).
    B. ``live`` -- the monolithic live pipeline (``pair_and_score``).
    C. ``split`` -- the same live pairing/persistence/multimodel/scoring
       logic run through the write-lock-fix split path
       (``run_split_pair_phases`` then ``run_batched_scoring``), on a real
       ``Database``/``FencedWriter`` rather than a bare connection.

    C vs A and B vs A are checked with the full ``_assert_end_state_equal``
    oracle (``forecast_pairs``/``score_cache``, §5.4). C vs B additionally
    checks every ``forecast_pairs`` column except ``id``, ``created_at`` and
    ``tz_generation_id`` -- the split path must not just agree with the
    frozen reference, it must reproduce the *live* incremental path's own
    output, not some other value that happens to also satisfy the reference.

    §5.3 scenario clock: ``utc_now`` is frozen to one instant per scenario
    (``t0`` for the initial build, ``t0 + k`` seconds for scenario ``k``) so
    all three arms see the same wall clock when they stamp
    ``first_known_at``/``source_computed_at`` (``persistence.py``). Without
    this, the three sequential ``_make_db()`` calls below would each read a
    genuinely different real ``utc_now()`` while seeding
    ``station_observations.fetched_at`` (via ``insert_station_observation`` /
    ``materialize_consensus``), and the arms would diverge on
    ``first_known_at`` for a reason that has nothing to do with the code
    under test.
    """
    t0 = utc_now()
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: t0)

    ref = _make_db()
    live = _make_db()
    seed = _make_db()
    db_path = tmp_path / "equivalence-split.db"
    seed.execute(f"VACUUM INTO '{db_path.as_posix()}'")
    seed.close()

    close_db()
    split_db = init_db(str(db_path))
    writer = FencedWriter(split_db, split_db.generation)
    split_conn = split_db._conn  # noqa: SLF001 - test inspects the real writer conn

    async def _run_split(site_arg: int | None) -> None:
        if site_arg is not None:
            site_ids: tuple[int, ...] = (site_arg,)
        else:
            site_ids = tuple(
                int(row["id"])
                for row in split_conn.execute(
                    "SELECT id FROM sites ORDER BY id"
                ).fetchall()
            )
        for site_id in site_ids:
            await run_split_pair_phases(writer, site_id, require_enabled=True)
            await run_batched_scoring(writer, site_id)

    async def _run_all_scenarios() -> None:
        for k, (name, mutate, site_arg) in enumerate(_SCENARIOS):
            monkeypatch.setattr(
                "wxverify.core.timeutil.utc_now",
                lambda k=k: t0 + timedelta(seconds=k),
            )

            mutate(ref)
            mutate(live)
            mutate(split_conn)

            _ref_pair_and_score(ref, site_arg)
            pair_and_score(live, site_arg)
            await _run_split(site_arg)

            _assert_end_state_equal(live, ref, label=f"{name} (live vs ref)")
            _assert_end_state_equal(split_conn, ref, label=f"{name} (split vs ref)")

            live_pairs = _pairs_snapshot_excluding(live, _PAIR_CROSS_ARM_EXCLUDE)
            split_pairs = _pairs_snapshot_excluding(split_conn, _PAIR_CROSS_ARM_EXCLUDE)
            assert split_pairs == live_pairs, (
                f"split forecast_pairs diverged from the live path: {name}"
            )

    try:
        asyncio.run(_run_all_scenarios())
    finally:
        close_db()
        ref.close()
        live.close()
