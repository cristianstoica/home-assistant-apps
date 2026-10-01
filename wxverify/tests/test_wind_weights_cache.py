"""Wind weights cache (plan §11.7, §15.1 T117-T137): T117-T126 and T127-T137.

The cache keys on the process ``Database`` object, its input epoch, its
external-commit count, ``site_id``, ``timezone`` and ``today``. This file
drives a real ``Database``/``init_db`` against a tmp-path file (never
``:memory:`` -- the cache's probe and epoch machinery is per-file), with a
fixed two-feed, 25-training-day site in ``pair_max`` mode. T127-T137 (the
bypass/stats/snapshot/absorb-ordering/concurrency/authorizer rows) reuse
this same fixture.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

import wxverify.forecast.service as forecast_service
import wxverify.web.context as web_context
import wxverify.web.routes as web_routes
from wxverify.api.app import create_app
from wxverify.db.connection import (
    EPOCH_MOVED,
    Database,
    FencedWriter,
    InputCounts,
    close_db,
    init_db,
    pinned_read_snapshot,
)
from wxverify.db.runtime_state import set_runtime_state, set_runtime_state_now
from wxverify.db.snapshot import SnapshotNestingError, read_snapshot
from wxverify.db.tz_generations import (
    ensure_published_generation,
    published_pointer_key,
)
from wxverify.forecast import wind_blend
from wxverify.forecast.wind_blend import (
    MIN_OBS_HOURS,
    MIN_RUN_HOURS,
    WindWeights,
    reset_wind_weights_cache,
)
from wxverify.worker.processor import _maybe_stamp_runtime_heartbeat

_TODAY = date(2026, 8, 20)
_DAYS = sorted(_TODAY - timedelta(days=offset) for offset in range(1, 26))


@dataclass(frozen=True)
class _Fixture:
    db: Database
    site_id: int
    feed_a: int
    feed_b: int
    virtual_feed: int
    excluded_feed: int
    station_id: int
    gen_id: int
    timezone: str = "UTC"
    today: date = _TODAY
    days: tuple[date, ...] = tuple(_DAYS)


def _seed_site(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES ('Testsite', 10.0, 20.0, 100.0, 'UTC')"
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_feed(
    conn: sqlite3.Connection,
    model: str,
    *,
    source: str = "open-meteo",
    is_virtual: int = 0,
) -> int:
    cur = conn.execute(
        "INSERT INTO feeds (source, model, default_subscribed,"
        " fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES (?, ?, 1, 360, 192, ?)",
        (source, model, is_virtual),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_station(conn: sqlite3.Connection, site_id: int, pws_id: str) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
        " dem_elevation_m, enabled) VALUES (?, ?, 10.0, 20.0, 50.0, 1)",
        (site_id, pws_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_pair_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    tz_generation_id: int,
    target: date,
    forecast_high: float,
    first_known_at: str = "2000-01-01T00:00:00Z",
) -> None:
    issued_at = f"{(target - timedelta(days=1)).isoformat()}T20:00:00Z"
    for h in range(MIN_RUN_HOURS):
        valid_at = f"{target.isoformat()}T{h:02d}:00:00Z"
        conn.execute(
            "INSERT INTO forecast_pairs (site_id, feed_id, variable, issued_at,"
            " valid_at, lead_hours, day_ahead, forecast, observed,"
            " tz_generation_id, first_known_at)"
            " VALUES (?, ?, 'wind', ?, ?, ?, 0, ?, 0.0, ?, ?)",
            (
                site_id,
                feed_id,
                issued_at,
                valid_at,
                h + 1,
                forecast_high,
                tz_generation_id,
                first_known_at,
            ),
        )


def _insert_obs_day(
    conn: sqlite3.Connection, *, site_id: int, target: date, high: float
) -> None:
    for h in range(MIN_OBS_HOURS):
        valid_at = f"{target.isoformat()}T{h:02d}:00:00Z"
        value = high if h == 0 else high - 1.0
        conn.execute(
            "INSERT INTO observations (site_id, variable, valid_at, value,"
            " n_stations, computed_at) VALUES (?, 'wind', ?, ?, 3, ?)",
            (site_id, valid_at, value, valid_at),
        )


def _insert_settled_wind_day(
    conn: sqlite3.Connection, *, station_id: int, target: date
) -> None:
    conn.execute(
        "INSERT INTO station_wind_days (station_id, local_date, status,"
        " attempts, refetch_at, refetched, updated_at)"
        " VALUES (?, ?, 'fetched', 3, NULL, 1, '2026-01-01T00:00:00Z')",
        (station_id, target.isoformat()),
    )


def _observed_high(i: int) -> float:
    return 10.0 + 0.4 * (i % 5)


def _forecast_a(i: int) -> float:
    return _observed_high(i) + 1.0 + 0.15 * (i % 4)


def _forecast_b(i: int) -> float:
    return _observed_high(i) + 2.0 - 0.2 * (i % 3)


def _seed_all(conn: sqlite3.Connection) -> tuple[int, int, int, int, int, int, int]:
    site_id = _seed_site(conn)
    gen_id = ensure_published_generation(conn, site_id)
    feed_a = _seed_feed(conn, "feed-a")
    feed_b = _seed_feed(conn, "feed-b")
    virtual_feed = _seed_feed(conn, "feed-virtual", is_virtual=1)
    excluded_row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'meteoblue' AND model = 'multimodel'"
    ).fetchone()
    assert excluded_row is not None
    excluded_feed = int(excluded_row["id"])
    station_id = _seed_station(conn, site_id, "KTEST001")
    for i, target in enumerate(_DAYS):
        observed = _observed_high(i)
        _insert_pair_day(
            conn,
            site_id=site_id,
            feed_id=feed_a,
            tz_generation_id=gen_id,
            target=target,
            forecast_high=_forecast_a(i),
        )
        _insert_pair_day(
            conn,
            site_id=site_id,
            feed_id=feed_b,
            tz_generation_id=gen_id,
            target=target,
            forecast_high=_forecast_b(i),
        )
        _insert_pair_day(
            conn,
            site_id=site_id,
            feed_id=virtual_feed,
            tz_generation_id=gen_id,
            target=target,
            forecast_high=_forecast_a(i),
        )
        _insert_pair_day(
            conn,
            site_id=site_id,
            feed_id=excluded_feed,
            tz_generation_id=gen_id,
            target=target,
            forecast_high=_forecast_a(i),
        )
        _insert_obs_day(conn, site_id=site_id, target=target, high=observed)
        _insert_settled_wind_day(conn, station_id=station_id, target=target)
    return (site_id, feed_a, feed_b, virtual_feed, excluded_feed, station_id, gen_id)


def _make_fixture(tmp_path: Path) -> _Fixture:
    close_db()
    db = init_db(str(tmp_path / "w.db"))
    ids = asyncio.run(db.write(_seed_all))
    site_id, feed_a, feed_b, virtual_feed, excluded_feed, station_id, gen_id = ids
    return _Fixture(
        db=db,
        site_id=site_id,
        feed_a=feed_a,
        feed_b=feed_b,
        virtual_feed=virtual_feed,
        excluded_feed=excluded_feed,
        station_id=station_id,
        gen_id=gen_id,
    )


def _serve(
    db: Database,
    fx: _Fixture,
    *,
    today: date | None = None,
    as_of: str | None = None,
) -> Any:
    tgt = fx.today if today is None else today
    return asyncio.run(
        db.read(
            lambda c: wind_blend.load_wind_serving(
                c, site_id=fx.site_id, timezone=fx.timezone, today=tgt, as_of=as_of
            )
        )
    )


def _uncached(
    db: Database,
    fx: _Fixture,
    *,
    today: date | None = None,
    as_of: str | None = None,
) -> WindWeights:
    """The reference (uncached) computation -- never through the spy.

    Routed directly through the true ``load_wind_training`` (bypassing the
    ``fx`` fixture's patched ``wind_blend.load_wind_training``) so that this
    sanity-check path never inflates ``_spy_of(fx).calls``, which exists to
    count only the cache's own real calls.
    """
    tgt = fx.today if today is None else today
    spy = fx.__dict__.get("spy")
    real = spy._real if spy is not None else wind_blend.load_wind_training  # noqa: SLF001

    def _run(conn: sqlite3.Connection) -> WindWeights:
        training = real(
            conn, site_id=fx.site_id, timezone=fx.timezone, today=tgt, as_of=as_of
        )
        return wind_blend.compute_wind_weights(training)

    return asyncio.run(db.read(_run))


def _training_update_sql(fx: _Fixture) -> tuple[str, tuple[object, ...]]:
    """The plan's canonical training UPDATE, bounded to the latest training day."""
    day = fx.days[-1]
    lo = f"{day.isoformat()}T00:00:00Z"
    hi = f"{(day + timedelta(days=1)).isoformat()}T00:00:00Z"
    sql = (
        "UPDATE observations SET value = value + 5.0 WHERE site_id = ?"
        " AND variable = 'wind' AND valid_at >= ? AND valid_at < ?"
    )
    return sql, (fx.site_id, lo, hi)


def _seed_future_wind_samples(fx: _Fixture) -> None:
    """Seed one future wind sample per feed so a tile/hourly page is non-empty.

    ``build_forecast``/``build_hourly`` return early (never reaching
    ``load_wind_serving``) with no rows in ``forecast_samples`` for today or
    later; the batch-1/2 fixture only seeds historical training data.
    """

    def _seed(c: sqlite3.Connection) -> None:
        for feed_id in (fx.feed_a, fx.feed_b):
            c.execute(
                "INSERT INTO forecast_samples (site_id, feed_id, variable,"
                " issued_at, valid_at, lead_hours, value, source_raw,"
                " model_run_id, fetched_at) VALUES (?, ?, 'wind', ?, ?, 1,"
                " 5.0, '{}', 't129', ?)",
                (
                    fx.site_id,
                    feed_id,
                    "2026-08-20T11:00:00Z",
                    "2026-08-20T13:00:00Z",
                    "2026-08-20T11:00:00Z",
                ),
            )

    asyncio.run(fx.db.write(_seed))


class _Spy:
    """Wraps ``wind_blend.load_wind_training``, counting real calls."""

    def __init__(self) -> None:
        self.calls = 0
        self._real = wind_blend.load_wind_training

    def __call__(self, conn: sqlite3.Connection, **kwargs: object) -> Any:
        self.calls += 1
        return self._real(conn, **kwargs)  # type: ignore[arg-type]


@pytest.fixture
def fx(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Fixture:
    fixture = _make_fixture(tmp_path)
    spy = _Spy()
    monkeypatch.setattr(wind_blend, "load_wind_training", spy)
    fixture.__dict__["spy"] = spy  # frozen dataclass: stash via __dict__
    return fixture


def _spy_of(fx: _Fixture) -> _Spy:
    return fx.__dict__["spy"]  # type: ignore[no-any-return]


async def _checkout_one(db: Database) -> sqlite3.Connection:
    """Dequeue one pooled reader directly, for a test that must hold a
    connection's snapshot open across other activity without risking
    ``db.read``/``_serve`` handing the SAME connection out again meanwhile."""
    return await db._read_pool.get()  # noqa: SLF001


async def _checkout_two(
    db: Database,
) -> tuple[sqlite3.Connection, sqlite3.Connection]:
    c1 = await db._read_pool.get()  # noqa: SLF001
    c2 = await db._read_pool.get()  # noqa: SLF001
    return c1, c2


def _release_one(db: Database, conn: sqlite3.Connection) -> None:
    db._read_pool.put_nowait(conn)  # noqa: SLF001


def _release_two(db: Database, c1: sqlite3.Connection, c2: sqlite3.Connection) -> None:
    db._read_pool.put_nowait(c1)  # noqa: SLF001
    db._read_pool.put_nowait(c2)  # noqa: SLF001


# --- T117: cached = uncached -----------------------------------------------


def test_t117_cached_equals_uncached(fx: _Fixture, tmp_path: Path) -> None:
    """Two serving calls hit the cache once; the result equals the uncached one.

    Plan T117. mutant_return_stored_dict -> at the ``popped`` assertion:
    correct = the second hit's weights still contain every key (a fresh
    ``dict(entry.weights)`` copy each time), mutant (returning
    ``entry.weights`` itself) = the popped key stays missing. mutant_drop_site_id
    -> at the second-site assertion: correct = ``{}`` (no training for
    site 2), mutant (key independent of ``site_id``) = site 1's weights.

    Site 2 is created BEFORE any serving call, not between the two sites'
    reads: inserting it is itself a non-exempt write that moves the epoch,
    which would invalidate site 1's cache entry on its own and mask a
    ``site_id``-independent key -- the entry would already miss on
    epoch/ext before the key's own identity is ever checked. With the
    insert done first and no write after it, site 1's and site 2's reads
    share one epoch/ext, so only the key field can tell them apart.
    """
    db = fx.db
    other_site_id = asyncio.run(
        db.write(
            lambda c: (
                c.execute(
                    "INSERT INTO sites (name, forecast_lat, forecast_lon,"
                    " elevation_m, timezone) VALUES ('Testsite2', 1.0, 2.0, 1.0,"
                    " 'UTC')"
                ).lastrowid
            )
        )
    )
    uncached = _uncached(db, fx)
    assert uncached  # fixture sanity: feed_a/feed_b both train

    first = _serve(db, fx)
    second = _serve(db, fx)
    stats = wind_blend.wind_weights_cache_stats()
    assert stats["misses"] == 1
    assert stats["hits"] == 1
    assert _spy_of(fx).calls == 1
    assert first.weights == uncached
    assert second.weights == uncached

    popped = _serve(db, fx)
    if popped.weights:
        key = next(iter(popped.weights))
        popped.weights.pop(key)
    third = _serve(db, fx)
    assert third.weights == uncached

    other_weights = asyncio.run(
        db.read(
            lambda c: wind_blend.live_wind_weights(
                c, site_id=int(other_site_id), timezone="UTC", today=fx.today
            )
        )
    )
    assert other_weights == {}
    assert wind_blend.wind_weights_cache_stats()["misses"] == 2


# --- T118: a training write invalidates ------------------------------------


def test_t118_training_write_invalidates(fx: _Fixture) -> None:
    """Each of 3 training writes is followed by a miss equal to the new uncached.

    Plan T118. mutant_drop_epoch_from_key -> at each post-write assertion:
    correct = the new uncached weights (observed high moved by the write, so
    both feeds' MAE moved too), mutant (epoch dropped from the key) = the
    stale hit's weights, which still equal the FIRST uncached snapshot.
    """
    db = fx.db
    first_uncached = _uncached(db, fx)
    _serve(db, fx)  # prime a hit
    hit = _serve(db, fx)
    assert hit.weights == first_uncached

    day = fx.days[-1]

    def _bump_obs(conn: sqlite3.Connection) -> None:
        sql, params = _training_update_sql(fx)
        conn.execute(sql, params)

    asyncio.run(db.write(_bump_obs))
    new_uncached_1 = _uncached(db, fx)
    assert new_uncached_1 != first_uncached
    served_1 = _serve(db, fx)
    assert served_1.weights == new_uncached_1

    def _bump_forecast(conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE forecast_pairs SET forecast = forecast + 3.0"
            " WHERE site_id = ? AND feed_id = ? AND variable = 'wind'"
            " AND valid_at >= ? AND valid_at < ?",
            (
                fx.site_id,
                fx.feed_a,
                f"{day.isoformat()}T00:00:00Z",
                f"{(day + timedelta(days=1)).isoformat()}T00:00:00Z",
            ),
        )

    asyncio.run(db.write(_bump_forecast))
    new_uncached_2 = _uncached(db, fx)
    assert new_uncached_2 != new_uncached_1
    served_2 = _serve(db, fx)
    assert served_2.weights == new_uncached_2

    def _unsettle(conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE station_wind_days SET status = 'pending'"
            " WHERE station_id = ? AND local_date = ?",
            (fx.station_id, day.isoformat()),
        )

    asyncio.run(db.write(_unsettle))
    new_uncached_3 = _uncached(db, fx)
    assert new_uncached_3 != new_uncached_2
    served_3 = _serve(db, fx)
    assert served_3.weights == new_uncached_3


# --- T119: counts read before the training statements ----------------------


def test_t119_counts_read_before_training_statements(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A write inside the (spied) loader's first call still invalidates.

    Plan T119. mutant_epoch_after_compute -> at the final assertion:
    correct = the fresh uncached weights for the moved data (the counts were
    read before ``_load`` ran, so the entry stores at the OLD counts and the
    next lookup's counts differ, forcing a second miss), mutant (reading the
    epoch after the compute instead) = the stale weights computed during
    this very call, served as a hit because the key now matches.
    """
    db = fx.db
    real = fx.__dict__["spy"]._real  # noqa: SLF001 - the true unwrapped loader
    state = {"armed": True}

    def _spy_with_write(conn: sqlite3.Connection, **kwargs: object) -> Any:
        _spy_of(fx).calls += 1
        result = real(conn, **kwargs)
        if state["armed"]:
            state["armed"] = False
            sql, params = _training_update_sql(fx)
            fx.db.write_sync(lambda c: c.execute(sql, params))
        return result

    monkeypatch.setattr(wind_blend, "load_wind_training", _spy_with_write)
    served = _serve(db, fx)

    fresh_uncached = _uncached(db, fx)
    next_served = _serve(db, fx)
    assert next_served.weights == fresh_uncached
    assert served.weights != fresh_uncached


# --- T120: the published-generation filter ---------------------------------


def test_t120_published_generation_filter(fx: _Fixture, tmp_path: Path) -> None:
    """A second, unpublished generation's pairs are excluded until published.

    Plan T120. mutant_drop_published_clause -> at ``assert still_hit.weights
    == g1_weights``: correct = G1's own weights (inserting G2 bumps the
    epoch regardless, so this is a genuine miss on both arms, but
    ``published_generation_clause`` still excludes G2's unpublished rows
    from the recompute, matching ``g1_weights``), mutant (dropping
    ``published_generation_clause`` from ``training_pairs_query``) = a
    different MAE, because G2's ``forecast + 1.0`` rows for every training
    day now also enter the recompute and change ``MAX(forecast)`` per day.
    Observed directly; the earlier ``served_g1.weights == g1_weights``
    assertion does NOT discriminate this mutant (G2 does not exist yet at
    that point, so the clause has nothing to exclude either way).

    mutant_hit_after_pointer_move: dropping ``entry.epoch == epoch`` from
    the cache key (the same code change as ``mutant_drop_epoch_from_key``)
    also fails this test, but -- observed directly -- it is caught by the
    earlier ``wind_weights_cache_stats()["misses"] == 2`` assertion (1
    instead of 2: the G2-insert write no longer registers as a miss at
    all), not by the final ``served_g2.weights == g2_uncached`` assertion
    the plan names; this test does not isolate which of the two assertions
    would catch a mutant that dropped ONLY the pointer-write's own
    invalidation while leaving the general epoch key intact.
    """
    db = fx.db
    g1_weights = _uncached(db, fx)
    served_g1 = _serve(db, fx)
    assert served_g1.weights == g1_weights

    def _make_g2(conn: sqlite3.Connection) -> int:
        cur = conn.execute(
            "INSERT INTO timezone_generations (site_id, timezone, mode, state)"
            " VALUES (?, 'UTC', 'retrospective_correction', 'building')",
            (fx.site_id,),
        )
        assert cur.lastrowid is not None
        g2 = int(cur.lastrowid)
        rows = conn.execute(
            "SELECT feed_id, issued_at, valid_at, lead_hours, day_ahead,"
            " forecast, observed, first_known_at FROM forecast_pairs"
            " WHERE site_id = ? AND variable = 'wind' AND tz_generation_id = ?",
            (fx.site_id, fx.gen_id),
        ).fetchall()
        for row in rows:
            conn.execute(
                "INSERT INTO forecast_pairs (site_id, feed_id, variable,"
                " issued_at, valid_at, lead_hours, day_ahead, forecast,"
                " observed, tz_generation_id, first_known_at)"
                " VALUES (?, ?, 'wind', ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    fx.site_id,
                    row["feed_id"],
                    row["issued_at"],
                    row["valid_at"],
                    row["lead_hours"],
                    row["day_ahead"],
                    row["forecast"] + 1.0,
                    row["observed"],
                    g2,
                    row["first_known_at"],
                ),
            )
        return g2

    g2 = asyncio.run(db.write(_make_g2))

    # Inserting G2's generation row and its copied forecast_pairs rows
    # changes rows on fx.db, so this IS a genuine miss (confirmed via
    # wind_weights_cache_stats()["misses"] moving 1 -> 2 here) -- the
    # published-generation filter, not an untouched cache entry, is what
    # keeps the recomputed value equal to g1_weights.
    still_hit = _serve(db, fx)
    assert still_hit.weights == g1_weights
    assert wind_blend.wind_weights_cache_stats()["misses"] == 2

    asyncio.run(
        db.write(
            lambda c: set_runtime_state(c, published_pointer_key(fx.site_id), str(g2))
        )
    )
    g2_uncached = _uncached(db, fx)
    assert g2_uncached != g1_weights
    served_g2 = _serve(db, fx)
    assert served_g2.weights == g2_uncached


# --- T121: the excluded feeds ----------------------------------------------


def test_t121_excluded_feeds_absent(fx: _Fixture) -> None:
    """A virtual feed and a meteoblue/multimodel feed never get a weight.

    Plan T121. mutant_drop_excluded_feeds_sql -> at the first assertion:
    correct = neither ``(virtual_feed, 0)`` nor ``(excluded_feed, 0)`` is a
    key of the weights (``EXCLUDED_FEEDS_SQL`` filters both out of
    ``training_pairs_query``), mutant (dropping that clause) = both keys
    present with a real weight, since each has full MIN_TRAINING_DAYS
    coverage. mutant_hit_after_feeds_write -> at the final assertion:
    correct = the virtual feed's key present after the ``is_virtual``
    write (a miss, new uncached), mutant (a stale hit) = it is still
    absent.
    """
    db = fx.db
    weights = _uncached(db, fx)
    assert all(key[0] != fx.virtual_feed for key in weights)
    assert all(key[0] != fx.excluded_feed for key in weights)

    served = _serve(db, fx)
    assert all(key[0] != fx.virtual_feed for key in served.weights)

    asyncio.run(
        db.write(
            lambda c: c.execute(
                "UPDATE feeds SET is_virtual = 0 WHERE id = ?", (fx.virtual_feed,)
            )
        )
    )
    new_uncached = _uncached(db, fx)
    assert any(key[0] == fx.virtual_feed for key in new_uncached)
    served_after = _serve(db, fx)
    assert served_after.weights == new_uncached
    assert any(key[0] == fx.virtual_feed for key in served_after.weights)


# --- T122: the heartbeat moves neither count --------------------------------


def test_t122_heartbeat_moves_neither_count(fx: _Fixture) -> None:
    """``epoch_exempt`` heartbeat writes never bump the epoch or probe count.

    Plan T122. mutant_drop_epoch_exempt -> at the ``epoch`` assertion after
    the two heartbeat calls: correct = unchanged (``epoch_exempt=True`` on
    ``_maybe_stamp_runtime_heartbeat``'s write), mutant (dropping the flag)
    = epoch bumped by 1 per call, since the write changes a row.
    mutant_drop_absorb -> at the ``ext`` assertion: correct = unchanged (the
    absorb reads the probe before ``_conn``, so the heartbeat's own commit
    on the probe is matched to ``_dv_seen`` and never counted), mutant
    (skipping ``_absorb_own_commit`` in ``_run_epoch_txn``'s ``finally``) =
    the NEXT lookup's ``external_commit_seq()`` sees the probe moved by the
    heartbeat and counts it, bumping ``ext`` by 1 even though nothing
    foreign happened.
    """
    db = fx.db
    asyncio.run(
        db.write(
            lambda c: (
                set_runtime_state(c, "worker_last_loop_at", "2000-01-01T00:00:00.000Z"),
                set_runtime_state(
                    c, "scheduler_last_tick_at", "2000-01-01T00:00:00.000Z"
                ),
            )
        )
    )
    hit_before = _serve(db, fx)
    epoch0 = db.input_epoch
    ext0 = db.external_commit_seq()

    asyncio.run(_maybe_stamp_runtime_heartbeat(db, "worker_last_loop_at", 0.0, 1.0))
    asyncio.run(_maybe_stamp_runtime_heartbeat(db, "scheduler_last_tick_at", 0.0, 1.0))

    assert db.input_epoch == epoch0
    assert db.external_commit_seq() == ext0

    hit_after = _serve(db, fx)
    assert hit_after.weights == hit_before.weights
    stats = wind_blend.wind_weights_cache_stats()
    assert stats["hits"] >= 1

    # control: a non-exempt write to the same table DOES move the epoch.
    epoch_before_control = db.input_epoch
    asyncio.run(db.write(lambda c: set_runtime_state_now(c, "worker_last_loop_at")))
    assert db.input_epoch == epoch_before_control + 1


# --- T123: midnight ---------------------------------------------------------


def test_t123_midnight_changes_today_in_the_key(fx: _Fixture) -> None:
    """``today`` rolling over at local midnight, with no write between, misses.

    Plan T123. mutant_drop_today_from_key -> at the final assertion:
    correct = the second call's weights equal the uncached weights computed
    for the NEW ``today`` (a day exactly ``TRAINING_DAYS`` before
    ``today_before`` sits at the training window's inclusive lower bound and
    falls out of it once ``today`` rolls forward, so the two ``today``
    values see different training coverage and so different weights),
    mutant (dropping ``today`` from the key) = the second call still
    returns the first call's (stale) weights.
    """
    db = fx.db
    today_before = fx.today
    today_after = fx.today + timedelta(days=1)

    # A training day exactly at the window's lower bound for today_before:
    # included there (lo is inclusive), excluded once today rolls forward
    # by one day (lo moves forward with it).
    boundary_day = today_before - timedelta(days=wind_blend.TRAINING_DAYS)

    def _seed_boundary(conn: sqlite3.Connection) -> None:
        _insert_pair_day(
            conn,
            site_id=fx.site_id,
            feed_id=fx.feed_a,
            tz_generation_id=fx.gen_id,
            target=boundary_day,
            forecast_high=_forecast_a(99),
        )
        _insert_pair_day(
            conn,
            site_id=fx.site_id,
            feed_id=fx.feed_b,
            tz_generation_id=fx.gen_id,
            target=boundary_day,
            forecast_high=_forecast_b(99),
        )
        _insert_obs_day(
            conn, site_id=fx.site_id, target=boundary_day, high=_observed_high(99)
        )

    asyncio.run(db.write(_seed_boundary))

    weights_before = _serve(db, fx, today=today_before)
    weights_after_uncached = _uncached(db, fx, today=today_after)
    assert weights_after_uncached != weights_before.weights

    served_after = _serve(db, fx, today=today_after)
    assert _spy_of(fx).calls == 2
    assert served_after.weights == weights_after_uncached


def test_l1_build_forecast_derives_today_from_local_midnight(fx: _Fixture) -> None:
    """Hoare L1: the real consumer, not ``_serve`` with an explicit ``today``.

    T123 above pins the cache key directly, through ``_serve(..., today=...)``.
    Nothing exercises how ``build_forecast`` (the actual caller) turns a UTC
    ``now`` into that ``today`` -- ``at.astimezone(tz).date()`` at
    ``forecast/service.py:216``. One leg either side of local midnight in a
    fixed, non-UTC offset (``Asia/Tokyo``, UTC+9, no DST, so the boundary is
    never ambiguous): 23:59 local on one calendar day, 00:01 local on the
    next. ``DayTile.date_iso`` for day 0 is exactly ``today``, so it is the
    oracle -- no boundary training day is needed, unlike T123's weights
    comparison.

    mutant_date_without_astimezone -> at ``view_after.tiles[0].date_iso``:
    correct = ``"2026-08-20"`` (``at.astimezone(tz).date()`` rolls the UTC
    instant into Tokyo's clock before taking the date, and 15:01 UTC is
    already 00:01 the next day there), mutant (``at.date()`` without the
    ``astimezone`` step) = ``"2026-08-19"`` (the UTC calendar date, one day
    behind). The "before" leg does not discriminate this mutant on its own
    (14:59 UTC is 2026-08-19 under both), which is why both legs are
    needed: the mutant's failure is visible only in the delta between them.
    """
    db = fx.db

    def _seed_future(c: sqlite3.Connection) -> None:
        c.execute(
            "INSERT INTO forecast_samples (site_id, feed_id, variable,"
            " issued_at, valid_at, lead_hours, value, source_raw,"
            " model_run_id, fetched_at) VALUES (?, ?, 'wind', ?, ?, 1,"
            " 5.0, '{}', 'l1', ?)",
            (
                fx.site_id,
                fx.feed_a,
                "2026-08-20T11:00:00Z",
                "2026-08-20T13:00:00Z",
                "2026-08-20T11:00:00Z",
            ),
        )

    asyncio.run(db.write(_seed_future))

    tz = "Asia/Tokyo"
    # Tokyo local midnight on 2026-08-20 is 2026-08-19T15:00:00Z.
    before_midnight = datetime(2026, 8, 19, 14, 59, tzinfo=UTC)
    after_midnight = datetime(2026, 8, 19, 15, 1, tzinfo=UTC)

    view_before = asyncio.run(
        db.read(
            lambda c: forecast_service.build_forecast(
                c,
                site_id=fx.site_id,
                timezone=tz,
                rain_threshold_mm=0.2,
                now=before_midnight,
            )
        )
    )
    view_after = asyncio.run(
        db.read(
            lambda c: forecast_service.build_forecast(
                c,
                site_id=fx.site_id,
                timezone=tz,
                rain_threshold_mm=0.2,
                now=after_midnight,
            )
        )
    )

    # Precondition: both legs actually reach the tile-building branch
    # (non-empty), so date_iso reflects a real build, not the early
    # ``empty=True`` return.
    assert not view_before.empty
    assert not view_after.empty
    assert view_before.tiles[0].date_iso == "2026-08-19"
    assert view_after.tiles[0].date_iso == "2026-08-20"
    # Mirrors T123's _spy_of(fx).calls == 2: the two legs sit on either side
    # of local midnight, so each gets its own cache key and its own real
    # load_wind_training call -- this pins the today-key leg of the cache
    # (service.py:217-218) end to end through the real build_forecast
    # caller, not just via _serve's explicit today=.
    assert _spy_of(fx).calls == 2


# --- T124: ``as_of`` is never cached -----------------------------------------


def test_t124_as_of_never_cached(fx: _Fixture) -> None:
    """A record (``as_of``) call always computes; it is never stored or hit.

    Plan T124. mutant_as_of_routed_through_cache -> at
    ``assert served_as_of.weights == as_of_uncached``: correct = the
    as_of-filtered weights (one training date's ``computed_at`` is pinned
    after ``as_of`` and must drop out), mutant (routing ``as_of is not
    None`` through ``live_wind_weights`` instead of computing directly) =
    the live entry's cached (unfiltered) weights, served as a stale hit
    because the live call just primed the same key. Observed directly.
    """
    db = fx.db
    live = _serve(db, fx)
    stats_after_live = wind_blend.wind_weights_cache_stats()

    as_of = f"{fx.days[-1].isoformat()}T23:59:59Z"
    pinned_day = fx.days[-1]

    def _after_as_of(conn: sqlite3.Connection) -> None:
        conn.execute(
            "UPDATE observations SET computed_at = ?"
            " WHERE site_id = ? AND variable = 'wind' AND valid_at >= ?"
            " AND valid_at < ?",
            (
                f"{(pinned_day + timedelta(days=2)).isoformat()}T00:00:00Z",
                fx.site_id,
                f"{pinned_day.isoformat()}T00:00:00Z",
                f"{(pinned_day + timedelta(days=1)).isoformat()}T00:00:00Z",
            ),
        )

    asyncio.run(db.write(_after_as_of))

    as_of_uncached = _uncached(db, fx, as_of=as_of)
    assert as_of_uncached != live.weights

    served_as_of = _serve(db, fx, as_of=as_of)
    assert served_as_of.weights == as_of_uncached
    assert _spy_of(fx).calls >= 2

    stats_after_as_of = wind_blend.wind_weights_cache_stats()
    assert stats_after_as_of["misses"] == stats_after_live["misses"]
    assert stats_after_as_of["hits"] == stats_after_live["hits"]


# --- T125: a replaced database ----------------------------------------------


def _build_other_db(path: Path, *, offset_days: int) -> None:
    """A second, closed database file whose training differs from ``fx``'s.

    Built via a bare ``Database(path)`` object, never through
    ``init_db``/``close_db`` -- those mutate the process-global
    ``_db_instance``, which at every call site here is still the caller's
    own live ``fx.db``, so routing through them would silently close it out
    from under the test.
    """
    other = Database(str(path))

    def _seed(conn: sqlite3.Connection) -> None:
        site_id = _seed_site(conn)
        gen_id = ensure_published_generation(conn, site_id)
        feed_a = _seed_feed(conn, "feed-a")
        feed_b = _seed_feed(conn, "feed-b")
        for i, target in enumerate(_DAYS):
            # offset_days shifts only the forecast side, so it changes the
            # forecast/observed MAE (not just both sides by the same
            # constant, which would leave the MAE -- and so the weights --
            # identical to fx's own database).
            observed = _observed_high(i)
            _insert_pair_day(
                conn,
                site_id=site_id,
                feed_id=feed_a,
                tz_generation_id=gen_id,
                target=target,
                forecast_high=_forecast_a(i) + offset_days,
            )
            _insert_pair_day(
                conn,
                site_id=site_id,
                feed_id=feed_b,
                tz_generation_id=gen_id,
                target=target,
                forecast_high=_forecast_b(i) + offset_days,
            )
            _insert_obs_day(conn, site_id=site_id, target=target, high=observed)

    asyncio.run(other.write(_seed))
    other.close()


def test_t125a_replace_from_invalidates(fx: _Fixture, tmp_path: Path) -> None:
    """After ``replace_from``, a call misses and equals the NEW file's uncached.

    Plan T125 (A). mutant_drop_epoch_from_key -> at the final assertion:
    correct = the new file's uncached weights (``replace_from`` reopens
    through ``_open``, which bumps the epoch), mutant (``entry.epoch ==
    epoch`` dropped from the key) = the pre-swap hit's weights, computed
    from the file that no longer backs ``db``. Observed directly; see the
    ledger. mutant_drop_identity_from_key does NOT kill this test:
    ``replace_from`` keeps the same ``Database`` object (only the file
    behind it changes), so dropping ``entry.db is db`` from the key changes
    nothing here -- the epoch bump alone still invalidates the entry. That
    mutant is killed by ``test_t125b_new_database_identity_invalidates``
    instead, which swaps in a genuinely different ``Database`` object with
    the SAME epoch/ext.
    """
    db = fx.db
    hit = _serve(db, fx)
    stale_weights = hit.weights

    new_path = tmp_path / "w-new.db"
    backup_path = tmp_path / "w-backup.db"
    _build_other_db(new_path, offset_days=7)

    asyncio.run(db.replace_from(new_path, backup_path))
    new_uncached = _uncached(db, fx)
    assert new_uncached != stale_weights
    served = _serve(db, fx)
    assert served.weights == new_uncached


def test_t125b_new_database_identity_invalidates(fx: _Fixture, tmp_path: Path) -> None:
    """A second ``init_db`` at the SAME epoch still misses: identity is in the key.

    Plan T125 (B). mutant_drop_identity_from_key -> at the final
    assertion: correct = database B's own uncached weights, mutant
    (dropping the ``Database`` object from the key, leaving only the
    epoch) = database A's stale weights, because the padded epoch is made
    to equal A's at the hit.
    """
    db_a = fx.db
    hit_a = _serve(db_a, fx)
    epoch_at_hit = db_a.input_epoch

    b_path = tmp_path / "w-b.db"
    _build_other_db(b_path, offset_days=3)

    db_b = init_db(str(b_path))
    assert db_b.input_epoch == 1

    pad = 0
    while db_b.input_epoch < epoch_at_hit:
        asyncio.run(db_b.write(lambda c, n=pad: set_runtime_state(c, f"pad{n}", "x")))
        pad += 1
    assert db_b.input_epoch == epoch_at_hit

    def _b_ids(conn: sqlite3.Connection) -> tuple[int, str]:
        row = conn.execute("SELECT id, timezone FROM sites LIMIT 1").fetchone()
        return int(row["id"]), str(row["timezone"])

    b_site_id, b_tz = asyncio.run(db_b.read(_b_ids))

    def _b_uncached(conn: sqlite3.Connection) -> WindWeights:
        training = wind_blend.load_wind_training(
            conn, site_id=b_site_id, timezone=b_tz, today=fx.today, as_of=None
        )
        return wind_blend.compute_wind_weights(training)

    b_uncached = asyncio.run(db_b.read(_b_uncached))

    served_b = asyncio.run(
        db_b.read(
            lambda c: wind_blend.load_wind_serving(
                c, site_id=b_site_id, timezone=b_tz, today=fx.today, as_of=None
            )
        )
    )
    assert served_b.weights == b_uncached
    assert served_b.weights != hit_a.weights

    # restore current_db() to fx.db for any later use in this test's scope
    close_db()
    import wxverify.db.connection as conn_mod

    conn_mod._db_instance = db_a  # noqa: SLF001


def test_t125c_probe_survives_replace_from(fx: _Fixture, tmp_path: Path) -> None:
    """The probe itself is replaced on ``replace_from``, with a fresh baseline.

    Plan T125 (C). mutant_old_probe_left_open -> observed directly: correct
    = the test passes with ``old_probe.execute`` raising
    ``sqlite3.ProgrammingError`` (the old probe was closed in
    ``_replace_sync``), mutant (never closing it, i.e. deleting
    ``self._probe.close()`` from step 3) = the test fails, but with a
    ``RuntimeError: database unrecoverable after failed import`` raised
    from inside ``replace_from`` itself -- NOT at the named assertion.
    Leaving the old probe connection open keeps this process's own handle
    on the pre-swap file's WAL/SHM sidecars, which step 3's comment says
    must all close for SQLite to remove them; with one left open,
    ``os.replace`` or the subsequent reopen hits stale sidecars beside the
    new file and the whole swap aborts before the test ever reaches the
    ``old_probe.execute`` line. Still a genuine kill (the test fails), just
    via an earlier exception than the plan names.
    mutant_probe_dv_carried_over -> at the
    ``db._probe_dv == new_dv`` assertion: correct = equal (``_open`` reads a
    fresh baseline under ``_probe_lock``), mutant (carrying the old value
    over) = unequal, since the precondition ``old_dv != new_dv`` holds.
    mutant_seq_reset -> at ``db.external_commit_seq() >= seq0``: correct =
    true (``_external_seq`` only grows), mutant (resetting it on reopen) =
    false. mutant_stale_probe_on_new_file -> observed directly: correct =
    ``db._probe is not old_probe`` (``_open`` always builds a fresh reader),
    mutant (carrying the pre-swap, now-closed probe connection object
    forward into the reopened ``Database`` instead of calling
    ``_connect_reader()`` again) = the SAME object identity, so the test
    fails at the earlier ``assert db._probe is not old_probe`` line -- NOT
    at the final ``served_after.weights`` assertion the plan names. The
    mutant never reaches that line: a stale, closed probe fails every
    later ``PRAGMA data_version`` read with ``sqlite3.ProgrammingError``,
    which ``external_commit_seq`` and ``_absorb_own_commit`` both swallow as
    a probe failure (returning ``None`` / logging and continuing), so the
    cache would in fact fall back to computing uncached on every call and
    still produce the right weights -- the probe's staleness is caught by
    identity, not by a wrong served value. Still a genuine kill, just one
    assertion earlier than predicted.

    The final section (raw2's commit, then one ``_serve``) has no process
    write between the commit and that call: it is this test's kill for "a
    probe that does not see the new file's commits" -- the reopened probe
    (``_open``, fresh baseline) must pick up raw2's commit on its own.
    mutant_reopened_probe_stale_baseline -> at ``stats_after["misses"] ==
    stats_before["misses"] + 1``: correct = 1 (the reopened probe's
    baseline is read fresh in ``_open``, so it differs from raw2's
    post-commit ``PRAGMA data_version`` and the lookup misses), mutant (the
    reopened probe's baseline carried over, or read before raw2's commit
    instead of after ``replace_from`` settles) = 0, a wrongly-served hit.
    """
    db = fx.db
    for i in range(3):
        asyncio.run(db.write(lambda c, n=i: set_runtime_state(c, f"t125c{n}", "x")))
    sql, params = _training_update_sql(fx)
    raw = sqlite3.connect(str(tmp_path / "w.db"))
    try:
        raw.execute(sql, params)
        raw.commit()
    finally:
        raw.close()
    missed = _serve(db, fx)
    hit = _serve(db, fx)
    assert hit.weights == missed.weights

    seq0 = db.external_commit_seq()
    assert seq0 is not None
    assert seq0 >= 1
    old_probe = db._probe  # noqa: SLF001
    old_dv = db._probe_dv  # noqa: SLF001

    new_path = tmp_path / "w125c-new.db"
    backup_path = tmp_path / "w125c-backup.db"
    _build_other_db(new_path, offset_days=11)

    asyncio.run(db.replace_from(new_path, backup_path))

    assert db._probe is not old_probe  # noqa: SLF001
    with pytest.raises(sqlite3.ProgrammingError):
        old_probe.execute("SELECT 1")

    new_dv = int(db._probe.execute("PRAGMA data_version").fetchone()[0])  # noqa: SLF001
    assert old_dv != new_dv
    assert db._probe_dv == new_dv  # noqa: SLF001

    assert db.external_commit_seq() is not None
    seq_after = db.external_commit_seq()
    assert seq_after is not None
    assert seq_after >= seq0

    first_after = _serve(db, fx)
    new_uncached = _uncached(db, fx)
    assert first_after.weights == new_uncached
    second_after = _serve(db, fx)
    assert second_after.weights == new_uncached
    stats = wind_blend.wind_weights_cache_stats()
    for reason, count in stats["bypasses"].items():  # type: ignore[union-attr]
        assert count == 0, reason

    # replace_from's last step is os.replace(new_db, self.path): new_path no
    # longer exists post-swap, the live file is db.path itself.
    raw2 = sqlite3.connect(db.path)
    sql2, params2 = _training_update_sql(fx)
    try:
        raw2.execute(sql2, params2)
        raw2.commit()
    finally:
        raw2.close()
    new_uncached_2 = _uncached(db, fx)
    assert new_uncached_2 != new_uncached

    # No process write between raw2's commit and this follow-up: the probe
    # that ``replace_from`` reopened (``_open``, with a fresh baseline) must
    # see raw2's commit on its own, with no epoch-moving write to help.
    stats_before = wind_blend.wind_weights_cache_stats()
    served_after = _serve(db, fx)
    assert served_after.weights == new_uncached_2
    stats_after = wind_blend.wind_weights_cache_stats()
    assert stats_after["misses"] == stats_before["misses"] + 1  # type: ignore[index]
    for reason, count in stats_after["bypasses"].items():  # type: ignore[union-attr]
        assert count == 0, reason


# --- T126: concurrent misses compute once -----------------------------------


def test_t126_concurrent_misses_compute_once(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Two concurrent misses on the same key single-flight through ``_LOCK``.

    Plan T126. mutant_lock_removed -> observed directly at
    ``assert stats["misses"] == 1``: correct = 1 (``_LOCK`` makes the
    second request wait for the first's result instead of racing it),
    mutant (removing ``_LOCK`` from ``_cached_weights``) = 2, both
    computing independently.
    mutant_call_timed_after_lock -> at the WARNING-count assertion: correct
    = exactly 2 records (the call is timed from ``live_wind_weights``'s
    entry, which includes the wait on ``_LOCK``, so B's ~1s wait plus its
    compute crosses the monkeypatched 500ms ``SLOW_CALL_MS``), mutant
    (timing from after ``_LOCK`` is acquired) = 1 record (only the miss;
    B's post-lock hit is fast and never crosses the threshold).
    """
    monkeypatch.setattr(wind_blend, "SLOW_CALL_MS", 500.0)
    db = fx.db
    event = threading.Event()
    real = wind_blend.load_wind_training

    def _spy_a(conn: sqlite3.Connection, **kwargs: object) -> Any:
        result = real(conn, **kwargs)  # type: ignore[arg-type]
        event.set()
        time.sleep(1.0)
        return result

    import wxverify.forecast.wind_blend as wb

    async def _read_a() -> Any:
        wb.load_wind_training = _spy_a  # type: ignore[assignment]
        try:
            return await db.read(
                lambda c: wind_blend.load_wind_serving(
                    c,
                    site_id=fx.site_id,
                    timezone=fx.timezone,
                    today=fx.today,
                    as_of=None,
                )
            )
        finally:
            wb.load_wind_training = real  # type: ignore[assignment]

    async def _read_b() -> Any:
        # event.wait() is a genuine OS-level blocking call: called bare
        # (unawaited) on the event-loop thread, it would starve the loop of
        # the chance to ever dispatch read A's executor task -- the thread
        # that would call event.set() never gets scheduled, so the wait
        # always times out. asyncio.to_thread offloads the wait itself so
        # the loop stays free to run A's dispatch concurrently.
        got = await asyncio.to_thread(event.wait, 5.0)
        if not got:
            raise AssertionError("read A never set the event")
        entered = time.perf_counter()
        result = await db.read(
            lambda c: wind_blend.load_wind_serving(
                c,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )
        )
        elapsed_ms = (time.perf_counter() - entered) * 1000.0
        assert elapsed_ms > 500.0
        return result

    caplog.set_level(logging.WARNING, logger="wxverify.forecast.wind_blend")

    async def _run_both() -> tuple[Any, Any]:
        return await asyncio.gather(_read_a(), _read_b())

    result_a, result_b = asyncio.run(_run_both())
    assert result_a.weights == result_b.weights

    stats = wind_blend.wind_weights_cache_stats()
    assert stats["misses"] == 1
    assert stats["hits"] == 1

    warnings = [
        r
        for r in caplog.records
        if r.name == "wxverify.forecast.wind_blend" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 2
    messages = sorted(r.getMessage() for r in warnings)
    assert "outcome=hit" in messages[0] or "outcome=hit" in messages[1]
    assert "outcome=miss" in messages[0] or "outcome=miss" in messages[1]


def _t137_authorizer(
    denials: list[tuple[int, str | None, str | None]],
    *,
    on_first: Any = None,
) -> Any:
    """``deny`` for T137: denies ``PRAGMA data_version``; records every call.

    ``on_first`` (when given) runs exactly once, the first time the denial
    fires -- leg 2b's "inject before returning SQLITE_DENY" mechanic.
    """
    fired = False

    def _deny(
        action: int,
        arg1: str | None,
        arg2: str | None,
        db_name: str | None,
        trigger: str | None,
    ) -> int:
        nonlocal fired
        if action == sqlite3.SQLITE_PRAGMA and arg1 == "data_version":
            denials.append((action, arg1, arg2))
            if on_first is not None and not fired:
                fired = True
                on_first()
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    return _deny


# --- T127: bypasses, one case per reason -----------------------------------


def test_t127_in_transaction_bypass(fx: _Fixture) -> None:
    """Plan T127, ``in_transaction``.

    mutant_bypass_dropped -> at ``after["hits"] == before["hits"]`` and
    ``after["misses"] == before["misses"]``: correct = both unchanged (the
    call is a bypass, not a hit or a miss), mutant (a cache read from a
    connection whose snapshot can be older than the counts, i.e. the bypass
    removed) = ``misses`` one higher instead.
    """
    db = fx.db
    reset_wind_weights_cache()

    def _run(conn: sqlite3.Connection) -> Any:
        conn.execute("BEGIN")
        try:
            assert conn.in_transaction
            result = wind_blend.load_wind_serving(
                conn,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )
            assert conn.in_transaction
        finally:
            conn.rollback()
        return result

    before = wind_blend.wind_weights_cache_stats()
    uncached = _uncached(db, fx)
    served = asyncio.run(db.read(_run))
    assert served.weights == uncached
    after = wind_blend.wind_weights_cache_stats()
    assert (
        after["bypasses"]["in_transaction"]  # type: ignore[index]
        == before["bypasses"]["in_transaction"] + 1  # type: ignore[index]
    )
    assert after["hits"] == before["hits"]
    assert after["misses"] == before["misses"]

    # Nothing was stored: a following pooled call misses.
    follow_before = after
    _serve(db, fx)
    follow_after = wind_blend.wind_weights_cache_stats()
    assert follow_after["misses"] == follow_before["misses"] + 1


def test_t127_foreign_connection_bypass(fx: _Fixture) -> None:
    """Plan T127, ``foreign_connection`` (three sub-cases).

    mutant_query_only_snapshot_on_foreign -> at
    ``raw2.execute("PRAGMA query_only").fetchone()[0] == 1``: correct = 1
    (``_load``'s ``pooled=False`` branch uses ``read_snapshot``, which never
    touches ``query_only``), mutant (``read_only_snapshot`` used instead) =
    0 (the cache's own ``finally`` would turn it back off).
    """
    db = fx.db
    reset_wind_weights_cache()
    uncached = _uncached(db, fx)

    # (a) a raw connection with row_factory = sqlite3.Row.
    raw = sqlite3.connect(db.path)
    raw.row_factory = sqlite3.Row
    try:
        before = wind_blend.wind_weights_cache_stats()
        served = wind_blend.load_wind_serving(
            raw, site_id=fx.site_id, timezone=fx.timezone, today=fx.today, as_of=None
        )
        assert served.weights == uncached
        assert not raw.in_transaction
        after = wind_blend.wind_weights_cache_stats()
        assert (
            after["bypasses"]["foreign_connection"]  # type: ignore[index]
            == before["bypasses"]["foreign_connection"] + 1  # type: ignore[index]
        )
        assert after["hits"] == before["hits"]
        assert after["misses"] == before["misses"]
    finally:
        raw.close()

    # (b) db.read_sync -- the dedicated sync-reader connection, never pooled.
    before2 = wind_blend.wind_weights_cache_stats()
    served2 = db.read_sync(
        lambda c: wind_blend.load_wind_serving(
            c, site_id=fx.site_id, timezone=fx.timezone, today=fx.today, as_of=None
        )
    )
    assert served2.weights == uncached
    after2 = wind_blend.wind_weights_cache_stats()
    assert (
        after2["bypasses"]["foreign_connection"]  # type: ignore[index]
        == before2["bypasses"]["foreign_connection"] + 1  # type: ignore[index]
    )

    # (c) a raw connection that already set PRAGMA query_only=ON: untouched.
    raw2 = sqlite3.connect(db.path)
    raw2.row_factory = sqlite3.Row
    try:
        raw2.execute("PRAGMA query_only=ON")
        before3 = wind_blend.wind_weights_cache_stats()
        served3 = wind_blend.load_wind_serving(
            raw2, site_id=fx.site_id, timezone=fx.timezone, today=fx.today, as_of=None
        )
        assert served3.weights == uncached
        assert raw2.execute("PRAGMA query_only").fetchone()[0] == 1
        after3 = wind_blend.wind_weights_cache_stats()
        assert (
            after3["bypasses"]["foreign_connection"]  # type: ignore[index]
            == before3["bypasses"]["foreign_connection"] + 1  # type: ignore[index]
        )
    finally:
        raw2.execute("PRAGMA query_only=OFF")
        raw2.close()

    # Nothing was stored: a following pooled call misses.
    stats_before_follow = wind_blend.wind_weights_cache_stats()
    _serve(db, fx)
    stats_after_follow = wind_blend.wind_weights_cache_stats()
    assert stats_after_follow["misses"] == stats_before_follow["misses"] + 1


def test_t127_probe_error_bypass(fx: _Fixture, monkeypatch: pytest.MonkeyPatch) -> None:
    """Plan T127, ``probe_error``.

    mutant_none_count_hits -> at ``served.weights == uncached`` (not the
    stored weights) and ``after["misses"] == before["misses"]``: correct =
    the bypass path recomputes and ``misses`` stays flat, mutant (a
    ``None`` count that reaches the hit check or gets stored) = either the
    stale stored weights are returned, or ``misses`` moves.
    """
    db = fx.db
    hit = _serve(db, fx)
    uncached = _uncached(db, fx)
    assert hit.weights == uncached

    sql, params = _training_update_sql(fx)
    raw = sqlite3.connect(db.path)
    try:
        raw.execute(sql, params)
        raw.commit()
    finally:
        raw.close()
    asyncio.run(db.write(lambda c: None))

    monkeypatch.setattr(db, "external_commit_seq", lambda: None)
    before = wind_blend.wind_weights_cache_stats()
    uncached_new = _uncached(db, fx)
    served = _serve(db, fx)
    assert served.weights == uncached_new
    assert served.weights != hit.weights
    after = wind_blend.wind_weights_cache_stats()
    assert (
        after["bypasses"]["probe_error"]  # type: ignore[index]
        == before["bypasses"]["probe_error"] + 1  # type: ignore[index]
    )
    assert after["misses"] == before["misses"]

    monkeypatch.undo()
    missed = _serve(db, fx)
    assert missed.weights == uncached_new
    rehit = _serve(db, fx)
    assert rehit.weights == uncached_new


def test_t127f_autocommit_second_reading_probe_error(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """New T127(f): the autocommit bypass's SECOND counts reading fails.

    ``_cached_weights``'s autocommit path (``wind_blend.py`` ~:664-696) reads
    ``before = db.input_counts()``, then inside ``read_only_snapshot`` reads
    ``after = db.input_counts()`` again. ``test_t127_probe_error_bypass``
    above patches ``external_commit_seq`` to ``None`` for every call, so it
    only ever exercises the ``before is None`` early return; this pins the
    OTHER leg -- the priming (second) reading failing while the first
    succeeds.

    mutant_only_after_not_none (``if after is not None and after != before:``)
    -> at ``after1["bypasses"]["probe_error"] == before1["bypasses"]
    ["probe_error"] + 1`` and ``after1["misses"] == before1["misses"]``:
    correct = ``probe_error`` bumps and ``misses`` stays flat (the bypass
    path never stores), mutant (gating the whole check on ``after is not
    None``) = neither bypass counter moves and ``misses`` bumps instead --
    with ``after`` literally ``None`` the mutant's condition is false, so it
    falls through to the miss path and stores the weights keyed on
    ``before``. Confirmed by the follow-up: after ``monkeypatch.undo()``, a
    plain ``_serve`` is a genuine miss under correct code (nothing was ever
    stored) but would wrongly HIT under this mutant (the stored entry's key
    still matches the restored real counts, since no training data
    changed).
    mutant_always_counts_moved (``_count_bypass("counts_moved")``
    unconditionally, dropping the ``after is None`` choice) -> at the same
    bypass assertions: correct = ``probe_error`` +1, ``counts_moved`` +0,
    mutant = ``counts_moved`` +1, ``probe_error`` +0.
    """
    db = fx.db
    reset_wind_weights_cache()
    uncached = _uncached(db, fx)
    real_ext = db.external_commit_seq
    calls = {"n": 0}

    def _wrapper() -> int | None:
        calls["n"] += 1
        if calls["n"] == 2:
            return None
        return real_ext()

    monkeypatch.setattr(db, "external_commit_seq", _wrapper)

    before1 = wind_blend.wind_weights_cache_stats()
    served = _serve(db, fx)
    after1 = wind_blend.wind_weights_cache_stats()

    assert calls["n"] == 2
    assert served.weights == uncached
    assert (
        after1["bypasses"]["probe_error"]  # type: ignore[index]
        == before1["bypasses"]["probe_error"] + 1  # type: ignore[index]
    )
    assert (
        after1["bypasses"]["counts_moved"]  # type: ignore[index]
        == before1["bypasses"]["counts_moved"]  # type: ignore[index]
    )
    assert after1["hits"] == before1["hits"]
    assert after1["misses"] == before1["misses"]

    monkeypatch.undo()
    before2 = wind_blend.wind_weights_cache_stats()
    missed = _serve(db, fx)
    after2 = wind_blend.wind_weights_cache_stats()
    assert missed.weights == uncached
    assert after2["misses"] == before2["misses"] + 1  # type: ignore[index]
    assert after2["hits"] == before2["hits"]


def test_t127a_plain_read_snapshot_is_not_a_pin(fx: _Fixture) -> None:
    """New T127(a): a plain ``read_snapshot`` on a pooled reader is not a pin.

    mutant_lazy_pin -> at ``result.weights == w_before`` and the unchanged
    hit/miss counters: correct = the call inside the plain ``read_snapshot``
    is counted an ``in_transaction`` bypass (``db.snapshot_pin`` returns
    ``None`` for a connection no ``pinned_snapshot`` ever registered, even
    though it IS in a transaction), mutant (treating any pooled reader
    inside a transaction as pinned, keyed on counts read lazily at lookup
    time) = the call wrongly hits/misses against those lazy counts instead
    of bypassing.
    """
    db = fx.db
    reset_wind_weights_cache()
    _serve(db, fx)
    hit = _serve(db, fx)
    w_before = hit.weights

    def _run(conn: sqlite3.Connection) -> Any:
        with read_snapshot(conn, label="t127_plain"):
            assert db.snapshot_pin(conn) is None
            sql, params = _training_update_sql(fx)
            raw = sqlite3.connect(db.path)
            try:
                raw.execute(sql, params)
                raw.commit()
            finally:
                raw.close()
            return wind_blend.load_wind_serving(
                conn,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )

    before = wind_blend.wind_weights_cache_stats()
    result = asyncio.run(db.read(_run))
    after = wind_blend.wind_weights_cache_stats()
    assert result.weights == w_before
    assert (
        after["bypasses"]["in_transaction"]  # type: ignore[index]
        == before["bypasses"]["in_transaction"] + 1  # type: ignore[index]
    )
    assert after["hits"] == before["hits"]
    assert after["misses"] == before["misses"]

    uncached_now = _uncached(db, fx)
    assert uncached_now != w_before
    follow = _serve(db, fx)
    assert follow.weights == uncached_now
    stats_after_follow = wind_blend.wind_weights_cache_stats()
    assert stats_after_follow["misses"] == after["misses"] + 1  # type: ignore[index]


def test_t127b_counts_moved_from_foreign_commit(fx: _Fixture) -> None:
    """New T127(b): ``counts_moved`` pin when another connection commits
    between the pin's two readings.

    mutant_drop_second_reading -> at the in-block
    ``assert db.snapshot_pin(conn) == "counts_moved"``: correct =
    ``"counts_moved"``, because the pin's second (post-priming) reading
    sees the foreign commit and disagrees with the first, mutant (a pin
    keyed on counts read once, before ``BEGIN``, never re-checked after the
    priming read) = the stable ``InputCounts`` from that one reading
    instead.
    mutant_before_read_after_priming -> at the same in-block
    ``assert db.snapshot_pin(conn) == "counts_moved"``: correct =
    ``"counts_moved"``, because the "before" reading lands ahead of the raw
    commit while the priming read (and so the "after" reading) lands after
    it, mutant (taking the "before" reading after the priming read too, so
    both readings see the commit) = the stable ``InputCounts`` pin instead.
    """
    db = fx.db
    reset_wind_weights_cache()
    _serve(db, fx)
    hit = _serve(db, fx)
    w_before = hit.weights
    fired = False

    def _cb(stmt: str) -> None:
        nonlocal fired
        if not fired and stmt.strip() == "BEGIN DEFERRED":
            fired = True
            sql, params = _training_update_sql(fx)
            raw = sqlite3.connect(db.path)
            try:
                raw.execute(sql, params)
                raw.commit()
            finally:
                raw.close()

    def _run(conn: sqlite3.Connection) -> Any:
        conn.set_trace_callback(_cb)
        try:
            with pinned_read_snapshot(conn, label="t127b_moved"):
                assert db.snapshot_pin(conn) == "counts_moved"
                result = wind_blend.load_wind_serving(
                    conn,
                    site_id=fx.site_id,
                    timezone=fx.timezone,
                    today=fx.today,
                    as_of=None,
                )
        finally:
            conn.set_trace_callback(None)
        assert db.snapshot_pin(conn) is None
        return result

    before = wind_blend.wind_weights_cache_stats()
    served = asyncio.run(db.read(_run))
    assert fired

    w_after = _uncached(db, fx)
    assert w_after != w_before
    assert served.weights == w_after

    after = wind_blend.wind_weights_cache_stats()
    assert (
        after["bypasses"]["counts_moved"]  # type: ignore[index]
        == before["bypasses"]["counts_moved"] + 1  # type: ignore[index]
    )
    assert after["hits"] == before["hits"]
    assert after["misses"] == before["misses"]

    follow = _serve(db, fx)
    assert follow.weights == w_after
    stats_after_follow = wind_blend.wind_weights_cache_stats()
    assert stats_after_follow["misses"] == after["misses"] + 1  # type: ignore[index]


def test_t127c_counts_moved_from_own_write(fx: _Fixture) -> None:
    """New T127(c): ``counts_moved`` pin from the process's OWN write.

    mutant_pin_compares_ext_only -> at ``w_after != w_before`` and
    ``served.weights == w_after``: correct = an own write bumps
    ``input_epoch`` directly (synchronously, before the absorb that would
    otherwise leave ``ext`` unmoved for an own commit), so the pin's epoch
    field alone already differs and the pin refuses as ``counts_moved``,
    mutant (a pin that compares only ``InputCounts.ext``, ignoring
    ``epoch``) = an own write between the two readings leaves ``ext``
    unchanged (own commits are absorbed uncounted), so the mutant pin
    wrongly holds stable and stores/serves the stale pre-write weights.
    """
    db = fx.db
    reset_wind_weights_cache()
    _serve(db, fx)
    hit = _serve(db, fx)
    w_before = hit.weights
    fired = False

    def _cb(stmt: str) -> None:
        nonlocal fired
        if not fired and stmt.strip() == "BEGIN DEFERRED":
            fired = True
            sql, params = _training_update_sql(fx)
            db.write_sync(lambda c: c.execute(sql, params))

    def _run(conn: sqlite3.Connection) -> Any:
        conn.set_trace_callback(_cb)
        try:
            with pinned_read_snapshot(conn, label="t127c_moved"):
                assert db.snapshot_pin(conn) == "counts_moved"
                result = wind_blend.load_wind_serving(
                    conn,
                    site_id=fx.site_id,
                    timezone=fx.timezone,
                    today=fx.today,
                    as_of=None,
                )
        finally:
            conn.set_trace_callback(None)
        return result

    before = wind_blend.wind_weights_cache_stats()
    served = asyncio.run(db.read(_run))
    assert fired

    w_after = _uncached(db, fx)
    assert w_after != w_before
    assert served.weights == w_after

    after = wind_blend.wind_weights_cache_stats()
    assert (
        after["bypasses"]["counts_moved"]  # type: ignore[index]
        == before["bypasses"]["counts_moved"] + 1  # type: ignore[index]
    )
    assert after["hits"] == before["hits"]
    assert after["misses"] == before["misses"]


@pytest.mark.parametrize("which_call", [1, 2])
def test_t127d_probe_error_through_pin(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch, which_call: int
) -> None:
    """New T127(d): ``probe_error`` pin, parametrized on which of the pin's
    two ``external_commit_seq()`` readings fails.

    mutant_only_checks_before -> (``which_call == 2``, the SECOND reading
    returns ``None``) at ``db.snapshot_pin(conn) == "probe_error"``:
    correct = ``"probe_error"`` (``after is None`` is still checked),
    mutant (drops the ``or after is None`` half of the check, testing only
    ``before is None``) = falls through to ``after != before`` with
    ``after`` literally ``None`` and ``before`` a real ``InputCounts``,
    which is true, so the pin is wrongly ``"counts_moved"`` instead.
    mutant_only_checks_after -> (``which_call == 1``, the FIRST reading
    returns ``None``) at the same assertion: correct = ``"probe_error"``
    (``before is None`` is still checked), mutant (drops the
    ``before is None`` half, testing only ``after is None``) = falls
    through to ``after != before`` with ``before`` literally ``None`` and
    ``after`` a real ``InputCounts``, again true, so the pin is wrongly
    ``"counts_moved"`` instead of ``"probe_error"``.
    """
    db = fx.db
    reset_wind_weights_cache()
    uncached = _uncached(db, fx)
    real_ext = db.external_commit_seq
    calls = {"n": 0}

    def _wrapper() -> int | None:
        calls["n"] += 1
        if calls["n"] == which_call:
            return None
        return real_ext()

    monkeypatch.setattr(db, "external_commit_seq", _wrapper)

    def _run(conn: sqlite3.Connection) -> Any:
        with pinned_read_snapshot(conn, label="t127d_probe_error"):
            assert db.snapshot_pin(conn) == "probe_error"
            return wind_blend.load_wind_serving(
                conn,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )

    before = wind_blend.wind_weights_cache_stats()
    served = asyncio.run(db.read(_run))
    after = wind_blend.wind_weights_cache_stats()

    assert calls["n"] == 2
    assert served.weights == uncached
    assert (
        after["bypasses"]["probe_error"]  # type: ignore[index]
        == before["bypasses"]["probe_error"] + 1  # type: ignore[index]
    )
    assert (
        after["bypasses"]["counts_moved"]  # type: ignore[index]
        == before["bypasses"]["counts_moved"]  # type: ignore[index]
    )
    assert after["hits"] == before["hits"]
    assert after["misses"] == before["misses"]


def test_t127e_pin_cleared_on_exception_exit(fx: _Fixture) -> None:
    """New T127(e), part 1: an exception out of a pinned block clears the pin.

    mutant_pin_not_cleared_on_exception -> at ``db.snapshot_pin(conn) is
    None`` (checked right after the raised exception exits the ``with``
    block): correct = ``None`` (the registry pop runs in ``pinned_snapshot``'s
    ``finally``, which an exception out of the block still reaches), mutant
    (the registry pop sequenced so an exception out of the ``with`` block
    skips it) = the stale pin (an ``InputCounts``) is still registered.
    Were that assertion absent, the kill would surface one step later, at
    ``after["bypasses"]["in_transaction"] == before["bypasses"]
    ["in_transaction"] + 1``: ``reset_wind_weights_cache()`` ran at the top
    of this test, so no entry exists yet for this key -- the stale pin
    would wrongly route the next plain transaction through
    ``_pinned_weights`` as a MISS (there is nothing to hit), leaving the
    ``in_transaction`` bypass counter flat instead of bumping by 1, not a
    wrongly-recorded hit. ``after["hits"] == before["hits"]`` holds either
    way and does not discriminate this mutant on its own.
    """

    class _Boom(Exception):
        pass

    db = fx.db
    reset_wind_weights_cache()

    def _run(conn: sqlite3.Connection) -> Any:
        with pytest.raises(_Boom), pinned_read_snapshot(conn, label="t127e_exc"):
            assert isinstance(db.snapshot_pin(conn), InputCounts)
            raise _Boom("synthetic failure inside the pin")
        assert db.snapshot_pin(conn) is None
        assert not conn.in_transaction

        before = wind_blend.wind_weights_cache_stats()
        conn.execute("BEGIN")
        try:
            result = wind_blend.load_wind_serving(
                conn,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )
        finally:
            conn.rollback()
        after = wind_blend.wind_weights_cache_stats()
        return result, before, after

    uncached = _uncached(db, fx)
    served, before, after = asyncio.run(db.read(_run))
    assert served.weights == uncached
    assert (
        after["bypasses"]["in_transaction"]  # type: ignore[index]
        == before["bypasses"]["in_transaction"] + 1  # type: ignore[index]
    )
    assert after["hits"] == before["hits"]


def test_t127e_nesting_refusal_preserves_outer_pin(fx: _Fixture) -> None:
    """New T127(e), part 2: a nesting refusal leaves the outer pin intact.

    mutant_nesting_refusal_pops_outer -> at
    ``db.snapshot_pin(conn) == outer`` and ``conn.in_transaction is True``
    (checked right after the refused inner attempt): correct = both (the
    pre-check in ``read_snapshot`` runs before any SQL and before the inner
    ``pinned_snapshot`` ever reaches its own registration/``finally``, so it
    cannot touch the outer entry), mutant (the inner attempt's cleanup runs
    regardless, e.g. a ``finally`` that unconditionally pops the registry
    entry for ``conn``) = the outer pin is gone and the outer transaction
    has been rolled back from under the caller.
    """
    db = fx.db
    reset_wind_weights_cache()
    _serve(db, fx)  # primes an entry at today's (pre-pin) counts

    def _run(conn: sqlite3.Connection) -> Any:
        with pinned_read_snapshot(conn, label="t127e_outer"):
            outer = db.snapshot_pin(conn)
            assert isinstance(outer, InputCounts)

            # A write after the outer pin is fixed: the inner attempt's own
            # (never-used) counts reading would differ from `outer`'s, so an
            # inner cleanup that wrongly touches the registry is caught by
            # VALUE, not just by presence.
            sql, params = _training_update_sql(fx)
            db.write_sync(lambda c: c.execute(sql, params))

            with (
                pytest.raises(SnapshotNestingError),
                pinned_read_snapshot(conn, label="t127e_inner"),
            ):
                pass
            assert db.snapshot_pin(conn) == outer
            assert conn.in_transaction is True

            before = wind_blend.wind_weights_cache_stats()
            result = wind_blend.load_wind_serving(
                conn,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )
            after = wind_blend.wind_weights_cache_stats()
        return result, before, after

    uncached = _uncached(db, fx)
    served, before, after = asyncio.run(db.read(_run))
    assert served.weights == uncached
    assert after["hits"] == before["hits"] + 1  # type: ignore[operator]
    assert after["misses"] == before["misses"]


def test_t127g_pinned_snapshot_foreign_connection_skips_pin(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """New T127(g): ``Database.pinned_snapshot`` on a connection it doesn't own.

    ``pinned_snapshot`` (``connection.py`` ~:465-500) now starts with
    ``if not self.owns_pooled_reader(conn): with read_snapshot(conn,
    label=label): yield conn; return`` -- a connection that is not one of
    this database's pooled readers gets a plain snapshot: no pin
    registered, no counts read.

    mutant_drop_foreign_guard (delete the guard, in a throwaway copy) -> at
    ``db.snapshot_pin(raw) is None``: correct = ``None`` (the guard routes
    straight to a plain ``read_snapshot``, which never touches
    ``_snapshot_pins``), mutant (the guard removed) = an ``InputCounts`` or
    a ``PinRefusal`` string, because the un-guarded code always registers a
    pin for whatever connection it is given -- and with the guard gone,
    ``input_counts_calls["n"] == 0``/``ext_calls["n"] == 0`` would also
    fail, since the un-guarded path reads the counts before even checking
    ownership.
    """
    db = fx.db

    input_counts_calls = {"n": 0}
    real_input_counts = db.input_counts

    def _spy_input_counts() -> InputCounts | None:
        input_counts_calls["n"] += 1
        return real_input_counts()

    monkeypatch.setattr(db, "input_counts", _spy_input_counts)

    ext_calls = {"n": 0}
    real_ext = db.external_commit_seq

    def _spy_ext() -> int | None:
        ext_calls["n"] += 1
        return real_ext()

    monkeypatch.setattr(db, "external_commit_seq", _spy_ext)

    raw = sqlite3.connect(db.path)
    try:
        assert not db.owns_pooled_reader(raw)
        with db.pinned_snapshot(raw, label="t127g_foreign"):
            assert db.snapshot_pin(raw) is None
            assert raw.in_transaction
            assert input_counts_calls["n"] == 0
            assert ext_calls["n"] == 0
        assert not raw.in_transaction

        # The nesting refusal still applies through the plain
        # read_snapshot path: a second pinned_snapshot on the same raw
        # connection while inside the first raises the same error
        # read_snapshot raises.
        with db.pinned_snapshot(raw, label="t127g_outer"):
            with (
                pytest.raises(SnapshotNestingError),
                db.pinned_snapshot(raw, label="t127g_inner"),
            ):
                pass
            assert db.snapshot_pin(raw) is None
    finally:
        raw.close()


def test_t127_no_database_bypass(fx: _Fixture) -> None:
    """Plan T127, ``no_database`` (last: it closes the database).

    mutant_bypass_dropped -> at ``served.weights == uncached``: correct =
    the uncached reference weights, computed BEFORE ``close_db()`` closes
    the live ``Database`` object, mutant (the bypass removed, so this path
    tries the cache's counts machinery against no process database) would
    raise instead of returning a value at all.
    """
    db = fx.db
    reset_wind_weights_cache()
    uncached = _uncached(db, fx)
    before = wind_blend.wind_weights_cache_stats()
    close_db()
    raw = sqlite3.connect(db.path)
    raw.row_factory = sqlite3.Row
    try:
        served = wind_blend.load_wind_serving(
            raw, site_id=fx.site_id, timezone=fx.timezone, today=fx.today, as_of=None
        )
        assert not raw.in_transaction
    finally:
        raw.close()
    assert served.weights == uncached
    after = wind_blend.wind_weights_cache_stats()
    assert (
        after["bypasses"]["no_database"]  # type: ignore[index]
        == before["bypasses"]["no_database"] + 1  # type: ignore[index]
    )
    assert after["hits"] == before["hits"]
    assert after["misses"] == before["misses"]


# --- T128: no failure caching -----------------------------------------------


def test_t128_no_failure_caching(fx: _Fixture) -> None:
    """Plan T128.

    mutant_error_counted_as_miss -> at ``stats["misses"] == 0``: correct =
    0 (the failed load stores nothing and is never counted as a miss),
    mutant (an error counted as a miss, or cached) = 1, or a later call
    wrongly hits a stale/failed entry.
    """
    db = fx.db
    reset_wind_weights_cache()
    spy = _spy_of(fx)
    real = spy._real  # noqa: SLF001
    raised = False

    def _raise_once(conn: sqlite3.Connection, **kwargs: object) -> Any:
        nonlocal raised
        if not raised:
            raised = True
            raise sqlite3.OperationalError("t128 injected")
        return real(conn, **kwargs)

    spy._real = _raise_once  # noqa: SLF001

    def _run(conn: sqlite3.Connection) -> None:
        with pytest.raises(sqlite3.OperationalError):
            wind_blend.load_wind_serving(
                conn,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )
        assert conn.in_transaction is False
        assert conn.execute("PRAGMA query_only").fetchone()[0] == 0

    asyncio.run(db.read(_run))

    stats = wind_blend.wind_weights_cache_stats()
    assert stats["errors"] == 1
    assert stats["misses"] == 0
    assert sum(stats["calls"].values()) == 1  # type: ignore[union-attr]
    assert not wind_blend._LOCK.locked()  # noqa: SLF001

    miss = _serve(db, fx)
    uncached = _uncached(db, fx)
    assert miss.weights == uncached
    hit = _serve(db, fx)
    assert hit.weights == uncached


# --- T129: one compute serves every consumer -------------------------------


def test_t129_one_compute_serves_every_consumer(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan T129.

    mutant_consumer_bypasses_cache -> at ``_spy_of(fx).calls == 1``: correct
    = 1 (every consumer -- forecast, hourly, dashboard -- routes through the
    SAME compute), mutant (a consumer loading weights without the cache, or
    on a connection the cache bypasses) = more than 1 spy call, or a
    nonzero bypass reason.
    """
    db = fx.db
    reset_wind_weights_cache()
    now = datetime(2026, 8, 20, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(web_context, "utc_now", lambda: now)
    monkeypatch.setattr(forecast_service, "utc_now", lambda: now)

    _seed_future_wind_samples(fx)

    site = asyncio.run(db.read(lambda c: web_context.load_site(c, fx.site_id)))
    assert site is not None

    forecast_view = asyncio.run(
        db.read(
            lambda c: forecast_service.build_forecast(
                c,
                site_id=fx.site_id,
                timezone=fx.timezone,
                rain_threshold_mm=site.rain_threshold_mm,
            )
        )
    )
    hourly = asyncio.run(
        db.read(
            lambda c: forecast_service.build_hourly(
                c, site_id=fx.site_id, timezone=fx.timezone, day=0
            )
        )
    )
    dashboard_rows = asyncio.run(
        db.read(lambda c: web_context.load_wind_weights(c, site, 0))
    )

    assert forecast_view is not None
    assert hourly is not None
    assert dashboard_rows is not None

    stats = wind_blend.wind_weights_cache_stats()
    assert _spy_of(fx).calls == 1
    assert stats["hits"] == 2
    assert stats["misses"] == 1
    for count in stats["bypasses"].values():  # type: ignore[union-attr]
        assert count == 0


# --- T130: the stats --------------------------------------------------------


def test_t130_stats_surface_and_reset(
    fx: _Fixture, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan T130.

    mutant_reset_keeps_counts -> at ``stats_after["hits"] == 0``: correct =
    0 (``reset_wind_weights_cache`` zeroes every counter), mutant (a reset
    that keeps counts) = the pre-reset value. mutant_since_wall_clock -> at
    ``stats_after["since"] != since0``: correct = different (``utc_now`` is
    monkeypatched forward by 5s between the two resets, so the inequality
    is forced by the fixture, never by real wall-clock timing), mutant (a
    reset that does not move ``since``) = equal.
    """
    db = fx.db
    reset_wind_weights_cache()
    _serve(db, fx)  # miss
    _serve(db, fx)  # hit

    app = create_app(root_path="")

    async def _status() -> dict[str, object]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.get("/api/worker/status")
            assert resp.status_code == 200
            return resp.json()

    body = asyncio.run(_status())
    cache = body["wind_weights_cache"]
    assert cache["hits"] == 1
    assert cache["misses"] == 1
    assert cache["errors"] == 0
    assert all(v == 0 for v in cache["bypasses"].values())
    assert sum(cache["calls"].values()) == 2
    assert cache["max_call_ms"] > 0
    assert cache["since"]

    t0 = datetime(2026, 8, 20, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(wind_blend, "utc_now", lambda: t0)
    reset_wind_weights_cache()
    since0 = wind_blend.wind_weights_cache_stats()["since"]

    caplog.set_level(logging.WARNING, logger="wxverify.forecast.wind_blend")
    wind_blend._record_call(fx.site_id, "hit", 10_500.0)  # noqa: SLF001
    stats = wind_blend.wind_weights_cache_stats()
    assert stats["calls"]["ge_10s"] == 1  # type: ignore[index]
    assert stats["max_call_ms"] == 10_500.0
    slow_warnings = [
        r
        for r in caplog.records
        if r.name == "wxverify.forecast.wind_blend" and r.levelno == logging.WARNING
    ]
    assert len(slow_warnings) == 1

    t1 = t0 + timedelta(seconds=5)
    monkeypatch.setattr(wind_blend, "utc_now", lambda: t1)
    reset_wind_weights_cache()
    stats_after = wind_blend.wind_weights_cache_stats()
    assert stats_after["hits"] == 0
    assert stats_after["misses"] == 0
    assert stats_after["errors"] == 0
    assert stats_after["max_call_ms"] == 0.0
    assert all(v == 0 for v in stats_after["bypasses"].values())
    assert all(v == 0 for v in stats_after["calls"].values())
    assert stats_after["since"] != since0


# --- T131: one snapshot under a concurrent commit --------------------------


def test_t131_pooled_reader_snapshot_order(fx: _Fixture) -> None:
    """Plan T131, pooled-reader leg.

    mutant_counts_read_after_load -> (see T135 for the direct counts-order
    kill; here) at ``order == [...]``: correct = the probe/begin/
    user_version/pairs sequence recorded strictly before the triggering
    observations statement starts, mutant (``BEGIN``/the priming read moved
    after the pairs statement, or the snapshot dropped) = a different
    statement -- or the observations statement itself -- in that window,
    and the served result would equal W_after instead of W_before.

    No process write runs between the raw commit and the follow-up
    ``_serve``: this leg's kill for "an outside change alone clears the
    cache" is the external-commit path alone, never the epoch (a no-op
    process write here would also move ``input_epoch``, since the raw
    commit already moved ``PRAGMA data_version`` on ``self._conn``, and the
    follow-up miss could no longer be attributed to ``ext`` alone).
    mutant_store_ext_after_load -> at ``follow.weights == uncached_after``:
    correct = ``uncached_after`` (the entry this test's own miss stores is
    keyed on the ``ext`` read before ``_load``, i.e. before the raw commit
    that lands mid-``_load``, so the follow-up ``_serve`` sees a changed
    ``ext`` and misses), mutant (keying the stored entry on
    ``external_commit_seq()`` re-read after ``_load`` instead) =
    ``baseline.weights`` (the re-read already reflects the raw commit, so
    the follow-up ``_serve``'s unchanged ``ext`` now matches the stored
    key and wrongly hits the stale pre-commit entry).
    """
    db = fx.db
    baseline = _serve(db, fx)
    before_commit: list[str] = []
    seen: list[str] = []
    triggered = False

    def _cb(stmt: str) -> None:
        nonlocal triggered
        normalized = stmt.strip()
        if not triggered and normalized.startswith(
            "SELECT valid_at, value FROM observations"
        ):
            triggered = True
            before_commit.extend(seen)
            sql, params = _training_update_sql(fx)
            raw = sqlite3.connect(db.path)
            try:
                raw.execute(sql, params)
                raw.commit()
            finally:
                raw.close()
        seen.append(normalized)

    def _run(conn: sqlite3.Connection) -> Any:
        conn.set_trace_callback(_cb)
        try:
            result = wind_blend.load_wind_serving(
                conn,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )
        finally:
            conn.set_trace_callback(None)
        assert conn.in_transaction is False
        assert conn.execute("PRAGMA query_only").fetchone()[0] == 0
        return result

    reset_wind_weights_cache()
    served = asyncio.run(db.read(_run))

    assert triggered
    order = before_commit[-4:]
    assert order[0] == "PRAGMA query_only=ON"
    assert order[1] == "BEGIN DEFERRED"
    assert order[2] == "PRAGMA user_version"
    assert order[3].startswith("SELECT fp.feed_id")
    assert served.weights == baseline.weights

    uncached_after = _uncached(db, fx)
    assert uncached_after != baseline.weights

    stats_before = wind_blend.wind_weights_cache_stats()
    assert stats_before["misses"] == 1, (
        "setup check: the triggering call above is the only miss since the"
        " reset, or the stats_after delta below proves nothing"
    )
    follow = _serve(db, fx)
    assert follow.weights == uncached_after
    stats_after = wind_blend.wind_weights_cache_stats()
    assert stats_after["misses"] == stats_before["misses"] + 1  # type: ignore[index]
    for reason, count in stats_after["bypasses"].items():  # type: ignore[union-attr]
        assert count == 0, reason


def test_t131_foreign_connection_snapshot_order(fx: _Fixture) -> None:
    """Plan T131, foreign-connection leg (no ``query_only`` on this path).

    No process write runs between the triggering commit and the follow-up
    ``_serve``, matching the pooled leg above: the follow-up miss is
    attributable to ``ext`` alone, not an epoch bump from an added no-op
    write. The triggering call itself goes through a foreign (non-pooled)
    connection, so it never stores a cache entry -- the follow-up miss is
    really invalidating the earlier ``baseline`` entry.
    """
    db = fx.db
    baseline = _serve(db, fx)
    before_commit: list[str] = []
    seen: list[str] = []
    triggered = False

    def _cb(stmt: str) -> None:
        nonlocal triggered
        normalized = stmt.strip()
        if not triggered and normalized.startswith(
            "SELECT valid_at, value FROM observations"
        ):
            triggered = True
            before_commit.extend(seen)
            sql, params = _training_update_sql(fx)
            raw2 = sqlite3.connect(db.path)
            try:
                raw2.execute(sql, params)
                raw2.commit()
            finally:
                raw2.close()
        seen.append(normalized)

    raw = sqlite3.connect(db.path)
    raw.row_factory = sqlite3.Row
    raw.set_trace_callback(_cb)
    try:
        served = wind_blend.load_wind_serving(
            raw, site_id=fx.site_id, timezone=fx.timezone, today=fx.today, as_of=None
        )
    finally:
        raw.set_trace_callback(None)
        assert not raw.in_transaction
        raw.close()

    assert triggered
    order = before_commit[-3:]
    assert order[0] == "BEGIN DEFERRED"
    assert order[1] == "PRAGMA user_version"
    assert order[2].startswith("SELECT fp.feed_id")
    assert served.weights == baseline.weights

    uncached_after = _uncached(db, fx)
    assert uncached_after != baseline.weights

    # The triggering call above went through a raw (foreign) connection, so
    # it already counted one "foreign_connection" bypass: compare bypasses
    # by delta, not by an absolute zero, or that pre-existing count would
    # fail this check regardless of what the follow-up call does.
    stats_before = wind_blend.wind_weights_cache_stats()
    follow = _serve(db, fx)
    assert follow.weights == uncached_after
    stats_after = wind_blend.wind_weights_cache_stats()
    assert stats_after["misses"] == stats_before["misses"] + 1  # type: ignore[index]
    bypasses_before = stats_before["bypasses"]
    bypasses_after = stats_after["bypasses"]
    for reason, count in bypasses_after.items():  # type: ignore[union-attr]
        assert count == bypasses_before[reason], reason  # type: ignore[index]


def test_t131_pinned_snapshot_order(fx: _Fixture) -> None:
    """New T131, pinned leg: ``load_wind_serving`` inside a caller-held pin.

    mutant_extra_snapshot -> at ``seen.count("BEGIN DEFERRED") == 1`` and
    ``"PRAGMA query_only=ON" not in seen``: correct = exactly one
    ``BEGIN DEFERRED`` and no ``query_only`` at all (``_pinned_weights``
    runs the compute INSIDE the caller's already-open pinned snapshot,
    issuing no snapshot of its own -- a nested one would raise
    ``SnapshotNestingError``), mutant (the pinned path opens its own
    ``read_snapshot``/``read_only_snapshot`` for the compute instead of
    reusing the caller's) = a second ``BEGIN DEFERRED``, or, respectively,
    a ``PRAGMA query_only=ON`` this path must never issue.
    mutant_pin_keyed_on_recount -> at ``follow.weights == uncached_after``:
    correct = the follow-up ``_serve`` MISSES and returns the fresh
    weights (the entry this test's miss stores is keyed on the PIN -- the
    counts read before ``BEGIN``/the priming read, i.e. before the raw
    commit that lands mid-``_load`` -- so the follow-up's current counts,
    which DO reflect the raw commit, no longer match), mutant (keying the
    stored entry on counts re-read AFTER the compute instead) =
    ``baseline.weights`` (the follow-up wrongly hits the stale pre-commit
    entry).
    """
    db = fx.db
    baseline = _serve(db, fx)
    seen: list[str] = []
    triggered = False

    def _cb(stmt: str) -> None:
        nonlocal triggered
        normalized = stmt.strip()
        if not triggered and normalized.startswith(
            "SELECT valid_at, value FROM observations"
        ):
            triggered = True
            sql, params = _training_update_sql(fx)
            raw = sqlite3.connect(db.path)
            try:
                raw.execute(sql, params)
                raw.commit()
            finally:
                raw.close()
        seen.append(normalized)

    def _run(conn: sqlite3.Connection) -> Any:
        conn.set_trace_callback(_cb)
        try:
            with pinned_read_snapshot(conn, label="t131_pinned"):
                result = wind_blend.load_wind_serving(
                    conn,
                    site_id=fx.site_id,
                    timezone=fx.timezone,
                    today=fx.today,
                    as_of=None,
                )
        finally:
            conn.set_trace_callback(None)
        return result

    reset_wind_weights_cache()
    served = asyncio.run(db.read(_run))

    assert triggered
    assert seen.count("BEGIN DEFERRED") == 1
    assert seen.count("PRAGMA user_version") == 1
    assert "PRAGMA query_only=ON" not in seen
    assert served.weights == baseline.weights

    uncached_after = _uncached(db, fx)
    assert uncached_after != baseline.weights

    stats_before = wind_blend.wind_weights_cache_stats()
    assert stats_before["misses"] == 1
    assert stats_before["hits"] == 0
    for reason, count in stats_before["bypasses"].items():  # type: ignore[union-attr]
        assert count == 0, reason

    follow = _serve(db, fx)
    assert follow.weights == uncached_after
    stats_after = wind_blend.wind_weights_cache_stats()
    assert stats_after["misses"] == stats_before["misses"] + 1  # type: ignore[index]


# --- T132: external commit, then a read, no process write between ---------


def test_t132_external_commit_then_cache_read(fx: _Fixture) -> None:
    """Plan T132.

    mutant_count_dropped_from_key -> at ``missed.weights == uncached_after``
    (not the stale ``baseline``): correct = the next lookup misses because
    the external-commit count moved, mutant (``external_commit_seq``
    returning ``_external_seq`` without reading the probe, or a fixed
    value) = the lookup wrongly hits the stale ``baseline`` weights.
    """
    db = fx.db
    baseline = _serve(db, fx)
    hit = _serve(db, fx)
    assert hit.weights == baseline.weights

    epoch0 = db.input_epoch
    sql, params = _training_update_sql(fx)
    raw = sqlite3.connect(db.path)
    try:
        raw.execute(sql, params)
        raw.commit()
    finally:
        raw.close()
    # No process write between the external commit and the next serving
    # call: the probe alone (read inside `external_commit_seq`, with no
    # absorb in between) must invalidate the entry.
    assert db.input_epoch == epoch0

    missed = _serve(db, fx)
    uncached_after = _uncached(db, fx)
    assert uncached_after != baseline.weights
    assert missed.weights == uncached_after

    rehit = _serve(db, fx)
    assert rehit.weights == uncached_after

    asyncio.run(db.write(lambda c: set_runtime_state(c, "t132_key", "x")))
    raw2 = sqlite3.connect(db.path)
    try:
        raw2.execute("PRAGMA busy_timeout=0")
        row = raw2.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        assert row is not None
        assert row[0] == 0
    finally:
        raw2.close()


# --- T133: the absorb --------------------------------------------------------


def test_t133a_exempt_write_leaves_counts_unchanged(fx: _Fixture) -> None:
    """Plan T133(a).

    mutant_absorb_dropped -> at
    ``db.external_commit_seq() == ext0``: correct = unchanged (an exempt
    write that changes a row still runs the absorb, which self-absorbs its
    own commit without bumping either count), mutant (the absorb dropped
    from ``_run_epoch_txn``'s ``finally``) would leave ``_probe_dv`` stale,
    so a LATER foreign commit's absorb would see an inflated delta -- not
    observable from this call alone, but the next hit is: it still returns
    the unchanged ``hit.weights``.
    """
    db = fx.db
    baseline = _serve(db, fx)
    hit = _serve(db, fx)
    assert hit.weights == baseline.weights

    epoch0 = db.input_epoch
    ext0 = db.external_commit_seq()
    writer = FencedWriter(db, db.generation, epoch_exempt=True)
    asyncio.run(writer.write(lambda c: set_runtime_state(c, "t133a_key", "x")))

    assert db.input_epoch == epoch0
    assert db.external_commit_seq() == ext0

    rehit = _serve(db, fx)
    assert rehit.weights == hit.weights


def test_t133b_absorb_reads_probe_before_conn(fx: _Fixture) -> None:
    """Plan T133(b).

    mutant_conn_before_probe -> at ``missed.weights == uncached_after``
    (not ``hit.weights``): correct = a miss with the new uncached weights
    (the foreign commit landing between the absorb's two reads is NOT
    silently absorbed), mutant (the absorb reading ``_conn`` before the
    probe) = the foreign commit lands between the two reads and is stored
    in ``_probe_dv`` with neither count moved, so this call wrongly hits
    the stale ``hit.weights``.
    """
    db = fx.db
    baseline = _serve(db, fx)
    hit = _serve(db, fx)
    assert hit.weights == baseline.weights
    epoch0 = db.input_epoch

    recorded: list[str] = []

    def _probe_cb(stmt: str) -> None:
        recorded.append(stmt.strip())
        if len(recorded) == 1:
            sql, params = _training_update_sql(fx)
            raw = sqlite3.connect(db.path)
            try:
                raw.execute(sql, params)
                raw.commit()
            finally:
                raw.close()

    db._probe.set_trace_callback(_probe_cb)  # noqa: SLF001
    try:
        writer = FencedWriter(db, db.generation, epoch_exempt=True)
        asyncio.run(writer.write(lambda c: set_runtime_state(c, "t133b_key", "x")))
    finally:
        db._probe.set_trace_callback(None)  # noqa: SLF001

    assert recorded == ["PRAGMA data_version"]
    assert db.input_epoch == epoch0

    missed = _serve(db, fx)
    uncached_after = _uncached(db, fx)
    assert uncached_after != hit.weights
    assert missed.weights == uncached_after


# --- T134: scoring still refuses a stale batch ------------------------------


def test_t134a_write_if_current_sees_heartbeat_exemption(fx: _Fixture) -> None:
    """Plan T134(A).

    mutant_exemption_too_broad -> at
    ``result2 is EPOCH_MOVED`` and ``marker == []``: correct = the training
    UPDATE (run through the non-exempt ``db.write``) moves the epoch, so
    ``write_if_current`` refuses and ``marker`` never runs; mutant (the
    heartbeats' exemption applied beyond the two heartbeat calls, e.g. to
    ``db.write`` by default) = ``result2`` is not ``EPOCH_MOVED`` and
    ``marker == ["ran"]`` (the stale batch wrongly applies).
    """
    db = fx.db
    writer = FencedWriter(db, db.generation)
    epoch, _ = asyncio.run(writer.read_at_epoch(lambda c: None, label="t134a"))

    asyncio.run(
        _maybe_stamp_runtime_heartbeat(
            db, "worker_last_loop_at", 0.0, time.perf_counter()
        )
    )
    asyncio.run(
        _maybe_stamp_runtime_heartbeat(
            db, "scheduler_last_tick_at", 0.0, time.perf_counter()
        )
    )
    assert db.input_epoch == epoch

    result = asyncio.run(writer.write_if_current(lambda c: None, epoch=epoch))
    assert result is not EPOCH_MOVED
    _, epoch_after = result
    assert epoch_after == epoch

    sql, params = _training_update_sql(fx)
    asyncio.run(db.write(lambda c, sql=sql, params=params: c.execute(sql, params)))
    assert db.input_epoch != epoch_after

    marker: list[str] = []

    def _marker(c: sqlite3.Connection) -> None:
        marker.append("ran")

    result2 = asyncio.run(writer.write_if_current(_marker, epoch=epoch_after))
    assert result2 is EPOCH_MOVED
    assert marker == []

    epoch2, _ = asyncio.run(writer.read_at_epoch(lambda c: None, label="t134a-2"))
    asyncio.run(
        db.write(
            lambda c: set_runtime_state(
                c, published_pointer_key(fx.site_id), str(fx.gen_id)
            )
        )
    )
    result3 = asyncio.run(writer.write_if_current(_marker, epoch=epoch2))
    assert result3 is EPOCH_MOVED
    assert marker == []


def test_t134b_absorb_never_stores_dv_seen(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan T134(B).

    mutant_absorb_stores_dv_seen -> at ``result2 is EPOCH_MOVED`` and
    ``chunk2_ran == []``: correct = EPOCH_MOVED and chunk2 never runs (the
    absorb's own read of ``_conn``'s ``data_version`` is never written into
    ``_dv_seen``, so the NEXT ``_body`` still sees the foreign commit as
    new), mutant (the absorb storing ``_conn``'s ``data_version`` in
    ``_dv_seen``, with or without a compensating epoch bump) = ``result2``
    is not ``EPOCH_MOVED`` and ``chunk2_ran == ["ran"]`` (chunk2 would pass).
    """
    db = fx.db
    writer = FencedWriter(db, db.generation)
    real_run_immediate = db._run_immediate  # noqa: SLF001
    injected = False

    def _patched_run_immediate(fn: Any) -> Any:
        nonlocal injected
        result = real_run_immediate(fn)
        if not injected:
            injected = True
            sql, params = _training_update_sql(fx)
            raw = sqlite3.connect(db.path)
            try:
                raw.execute(sql, params)
                raw.commit()
            finally:
                raw.close()
        return result

    monkeypatch.setattr(db, "_run_immediate", _patched_run_immediate)  # noqa: SLF001

    epoch, _ = asyncio.run(writer.read_at_epoch(lambda c: None, label="t134b"))

    chunk1_ran: list[str] = []

    def _chunk1(c: sqlite3.Connection) -> None:
        chunk1_ran.append("ran")
        set_runtime_state(c, "t134b_chunk1", "x")

    result1 = asyncio.run(writer.write_if_current(_chunk1, epoch=epoch))
    assert result1 is not EPOCH_MOVED
    assert chunk1_ran == ["ran"]
    _, epoch_after = result1

    chunk2_ran: list[str] = []

    def _chunk2(c: sqlite3.Connection) -> None:
        chunk2_ran.append("ran")

    result2 = asyncio.run(writer.write_if_current(_chunk2, epoch=epoch_after))
    assert result2 is EPOCH_MOVED
    assert chunk2_ran == []


# --- T135: the order on both sides ------------------------------------------


def test_t135a_writer_bumps_before_absorbing(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan T135(a).

    mutant_absorb_before_epoch_bump -> at
    ``stats_at_absorb["misses"] == stats_before["misses"] + 1`` and
    ``recorded_weights == uncached_after``: correct = the recorded call,
    made from inside the absorb (after the real absorb already ran), is a
    genuine miss with the new uncached weights, mutant (the absorb run
    before the epoch bump) = the recorded call hits the OLD entry instead
    (``stats_at_absorb["hits"]`` would move, not ``misses``).
    """
    db = fx.db
    baseline = _serve(db, fx)
    hit = _serve(db, fx)
    assert hit.weights == baseline.weights

    real_absorb = db._absorb_own_commit  # noqa: SLF001
    recorded: list[tuple[Any, dict[str, object]]] = []
    fired = False

    def _patched_absorb() -> None:
        nonlocal fired
        real_absorb()
        if not fired:
            fired = True
            weights = wind_blend.live_wind_weights(
                db._read_conns[0],  # noqa: SLF001
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
            )
            recorded.append((weights, wind_blend.wind_weights_cache_stats()))

    monkeypatch.setattr(db, "_absorb_own_commit", _patched_absorb)  # noqa: SLF001

    stats_before = wind_blend.wind_weights_cache_stats()
    sql, params = _training_update_sql(fx)
    asyncio.run(db.write(lambda c, sql=sql, params=params: c.execute(sql, params)))

    assert fired
    recorded_weights, stats_at_absorb = recorded[0]
    assert stats_at_absorb["misses"] == stats_before["misses"] + 1  # type: ignore[operator]
    assert stats_at_absorb["hits"] == stats_before["hits"]

    uncached_after = _uncached(db, fx)
    assert uncached_after != hit.weights
    assert recorded_weights == uncached_after


def test_t135b_reader_reads_count_before_epoch(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan T135(b).

    mutant_epoch_before_count -> at ``missed.weights == uncached_after``:
    correct = a miss with the new uncached weights (the count is read
    first, so by the time the epoch is read it already reflects the
    just-landed write too -- a consistent, post-write snapshot), mutant
    (the epoch read before the count) = the lookup holds the old epoch,
    the absorb self-absorbs the write before the count is read, the count
    never moves either, and the call wrongly hits the OLD entry.
    """
    db = fx.db
    baseline = _serve(db, fx)
    hit = _serve(db, fx)
    assert hit.weights == baseline.weights

    real_ext = db.external_commit_seq
    fired = False

    def _patched_ext() -> int | None:
        nonlocal fired
        if not fired:
            fired = True
            sql, params = _training_update_sql(fx)
            db.write_sync(lambda c: c.execute(sql, params))
        return real_ext()

    monkeypatch.setattr(db, "external_commit_seq", _patched_ext)

    missed = _serve(db, fx)
    uncached_after = _uncached(db, fx)
    assert uncached_after != hit.weights
    assert missed.weights == uncached_after


# --- T136: a real concurrent request path -----------------------------------


def test_t136_concurrent_request_path_leg1_sequential(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan T136, leg 1 (sequential).

    mutant_second_pooled_reader -> at
    ``qsize == db._read_pool.maxsize - 1``: correct = exactly one fewer
    (only the request's own reader is checked out; the compute runs on
    that SAME connection), mutant (a consumer that draws a second pooled
    reader to compute the weights) = one fewer again (two checked out).
    mutant_consumer_skips_cache -> at ``_spy_of(fx).calls == 1``: correct =
    1 (every consumer -- forecast, hourly, dashboard, tiles poll -- shares
    one compute), mutant (a consumer loading weights directly, or on a
    connection the cache bypasses) = more than 1, or a nonzero bypass.
    mutant_route_unpinned -> at ``for count in stats["bypasses"].values():
    assert count == 0``: correct = every bypass reason stays 0 (the tiles
    poll's connection carries a pin, so its hit on the already-stored entry
    goes through ``_pinned_weights``), mutant (the tiles route left on a
    plain ``read_snapshot`` instead of ``pinned_read_snapshot``) =
    ``snapshot_pin`` returns ``None`` on that connection, so
    ``_cached_weights`` falls through to the ``"in_transaction"`` bypass
    reason instead of a hit, moving that count off 0 and leaving ``hits``
    one short of 3.
    """
    db = fx.db
    now = datetime(2026, 8, 20, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(forecast_service, "utc_now", lambda: now)
    monkeypatch.setattr(web_context, "utc_now", lambda: now)
    monkeypatch.setattr(web_routes, "schedule_score_rescore", lambda site_id: None)
    _seed_future_wind_samples(fx)

    read_calls: list[str] = []
    real_read = db.read

    async def _tracking_read(fn: Any) -> Any:
        read_calls.append("read")
        return await real_read(fn)

    monkeypatch.setattr(db, "read", _tracking_read)

    pool_observations: list[tuple[int, bool]] = []
    real_load_training = _spy_of(fx)

    def _spy_sees_pool(conn: sqlite3.Connection, **kwargs: object) -> Any:
        pool_observations.append(
            (db._read_pool.qsize(), db.owns_pooled_reader(conn))  # noqa: SLF001
        )
        return real_load_training(conn, **kwargs)

    monkeypatch.setattr(wind_blend, "load_wind_training", _spy_sees_pool)

    app = create_app(root_path="")

    async def _run() -> None:
        reset_wind_weights_cache()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            r1 = await client.get(f"/forecast?site={fx.site_id}")
            r2 = await client.get(f"/api/forecast/hourly?site={fx.site_id}&day=0")
            r3 = await client.get(f"/dashboard?site={fx.site_id}&variable=wind")
            # An empty fingerprint never matches the built view's, so the
            # tiles poll always rebuilds (200), never answers 204 (precedent:
            # tests/test_forecast_last_fetched.py:737).
            r4 = await client.get(f"/forecast/tiles?site={fx.site_id}&fingerprint=")
            assert r1.status_code == 200
            assert r2.status_code == 200
            assert r3.status_code == 200
            assert r4.status_code == 200

    asyncio.run(_run())

    assert len(read_calls) == 4
    assert len(pool_observations) == 1
    qsize, owned = pool_observations[0]
    assert owned
    assert qsize == db._read_pool.maxsize - 1  # noqa: SLF001

    stats = wind_blend.wind_weights_cache_stats()
    assert stats["misses"] == 1
    assert stats["hits"] == 3
    for count in stats["bypasses"].values():  # type: ignore[union-attr]
        assert count == 0


def test_t136_concurrent_request_path_leg2_saturated(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan T136, leg 2 (saturated).

    mutant_lock_removed -> the three waiters never reach ``waiting >= 3``,
    the spy times out and raises ``AssertionError`` (correct = it never
    times out, since the lock serializes every concurrent compute attempt
    behind one holder plus waiters). mutant_consumer_skips_cache -> at
    ``_spy_of(fx).calls == 1``: correct = 1, mutant (a consumer that skips
    the cache) = more spy calls and fewer than 7 hits.
    """
    db = fx.db
    now = datetime(2026, 8, 20, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr(forecast_service, "utc_now", lambda: now)
    monkeypatch.setattr(web_context, "utc_now", lambda: now)
    monkeypatch.setattr(web_routes, "schedule_score_rescore", lambda site_id: None)
    _seed_future_wind_samples(fx)

    reset_wind_weights_cache()
    sql, params = _training_update_sql(fx)
    asyncio.run(db.write(lambda c, sql=sql, params=params: c.execute(sql, params)))

    class _CountingLock:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self._guard = threading.Lock()
            self.waiting = 0

        def __enter__(self) -> _CountingLock:
            with self._guard:
                self.waiting += 1
            self._lock.acquire()
            with self._guard:
                self.waiting -= 1
            return self

        def __exit__(self, *exc: object) -> None:
            self._lock.release()

        def locked(self) -> bool:
            return self._lock.locked()

    counting_lock = _CountingLock()
    monkeypatch.setattr(wind_blend, "_LOCK", counting_lock)

    real_load_training = _spy_of(fx)
    first_call_seen = False

    def _spy_waits(conn: sqlite3.Connection, **kwargs: object) -> Any:
        nonlocal first_call_seen
        if not first_call_seen:
            first_call_seen = True
            deadline = time.perf_counter() + 5.0
            while counting_lock.waiting < 3:
                if time.perf_counter() > deadline:
                    raise AssertionError(
                        f"waiting never reached 3 (got {counting_lock.waiting})"
                    )
                time.sleep(0.01)
        return real_load_training(conn, **kwargs)

    monkeypatch.setattr(wind_blend, "load_wind_training", _spy_waits)

    app = create_app(root_path="")

    async def _run() -> None:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            kinds = [
                lambda: client.get(f"/forecast?site={fx.site_id}"),
                lambda: client.get(f"/api/forecast/hourly?site={fx.site_id}&day=0"),
                lambda: client.get(f"/dashboard?site={fx.site_id}&variable=wind"),
            ]
            tasks = [asyncio.create_task(kinds[i % 3]()) for i in range(8)]
            done, pending = await asyncio.wait(tasks, timeout=60)
            assert not pending, f"stats={wind_blend.wind_weights_cache_stats()}"
            for task in done:
                exc = task.exception()
                assert exc is None, f"task failed: {exc!r}"
                assert task.result().status_code == 200

    asyncio.run(_run())

    assert _spy_of(fx).calls == 1
    stats = wind_blend.wind_weights_cache_stats()
    assert stats["misses"] == 1
    assert stats["hits"] == 7
    for count in stats["bypasses"].values():  # type: ignore[union-attr]
        assert count == 0


# --- T137: the absorb's failure path ----------------------------------------


def test_t137_leg1_own_commit_alone(
    fx: _Fixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Plan T137, leg 1.

    mutant_baseline_stored_regardless -> at ``db._probe_dv == dv0`` and
    ``db.input_epoch == epoch0``: correct = both unchanged (the denied
    absorb read never updates either), mutant (the baseline stored before,
    or regardless of, the ``_conn`` read) = ``_probe_dv`` moved anyway.
    mutant_catch_too_narrow -> at the write returning without raising:
    correct = it returns normally (``DatabaseError`` from the denial is
    caught), mutant (a catch of only ``sqlite3.OperationalError``) = this
    leg raises instead, since the denial is a ``DatabaseError``.
    """
    db = fx.db
    hit = _serve(db, fx)
    denials: list[tuple[int, str | None, str | None]] = []
    epoch0 = db.input_epoch
    dv0 = db._probe_dv  # noqa: SLF001

    def _fn(c: sqlite3.Connection) -> None:
        c.set_authorizer(_t137_authorizer(denials))
        set_runtime_state(c, "t137_key", "leg1")

    caplog.set_level(logging.DEBUG, logger="wxverify.db.connection")
    try:
        asyncio.run(FencedWriter(db, db.generation, epoch_exempt=True).write(_fn))
    finally:
        db._conn.set_authorizer(None)  # noqa: SLF001

    raw = sqlite3.connect(db.path)
    try:
        row = raw.execute(
            "SELECT value FROM runtime_state WHERE key = 't137_key'"
        ).fetchone()
    finally:
        raw.close()
    assert row is not None
    assert row[0] == "leg1"

    assert len(denials) == 1
    assert db._probe_dv == dv0  # noqa: SLF001
    assert db.input_epoch == epoch0

    debug_records = [
        r
        for r in caplog.records
        if r.name == "wxverify.db.connection" and r.exc_info is not None
    ]
    assert len(debug_records) == 1
    assert debug_records[0].levelno == logging.DEBUG
    assert issubclass(debug_records[0].exc_info[0], sqlite3.DatabaseError)  # type: ignore[index]

    uncached = _uncached(db, fx)
    stats_before = wind_blend.wind_weights_cache_stats()
    missed = _serve(db, fx)
    stats_mid = wind_blend.wind_weights_cache_stats()
    assert stats_mid["misses"] == stats_before["misses"] + 1  # type: ignore[operator]
    assert missed.weights == uncached
    rehit = _serve(db, fx)
    stats_after = wind_blend.wind_weights_cache_stats()
    assert stats_after["hits"] == stats_mid["hits"] + 1  # type: ignore[operator]
    assert rehit.weights == uncached
    assert hit.weights == uncached
    for count in stats_after["bypasses"].values():  # type: ignore[union-attr]
        assert count == 0


def test_t137_leg2a_foreign_commit_after_commit_before_probe_read(
    fx: _Fixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Plan T137, leg 2a."""
    db = fx.db
    hit = _serve(db, fx)
    denials: list[tuple[int, str | None, str | None]] = []
    epoch0 = db.input_epoch
    dv0 = db._probe_dv  # noqa: SLF001

    recorded: list[str] = []

    def _probe_cb(stmt: str) -> None:
        recorded.append(stmt.strip())
        if len(recorded) == 1:
            sql, params = _training_update_sql(fx)
            raw = sqlite3.connect(db.path)
            try:
                raw.execute(sql, params)
                raw.commit()
            finally:
                raw.close()

    def _fn(c: sqlite3.Connection) -> None:
        c.set_authorizer(_t137_authorizer(denials))
        set_runtime_state(c, "t137_key", "leg2a")

    caplog.set_level(logging.DEBUG, logger="wxverify.db.connection")
    db._probe.set_trace_callback(_probe_cb)  # noqa: SLF001
    try:
        asyncio.run(FencedWriter(db, db.generation, epoch_exempt=True).write(_fn))
    finally:
        db._conn.set_authorizer(None)  # noqa: SLF001
        db._probe.set_trace_callback(None)  # noqa: SLF001

    assert recorded == ["PRAGMA data_version"]
    assert len(denials) == 1
    assert db._probe_dv == dv0  # noqa: SLF001
    assert db.input_epoch == epoch0

    missed = _serve(db, fx)
    uncached_after = _uncached(db, fx)
    assert uncached_after != hit.weights
    assert missed.weights == uncached_after


def test_t137_leg2b_foreign_commit_between_absorbs_two_reads(
    fx: _Fixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Plan T137, leg 2b.

    mutant_baseline_stored_regardless -> at ``db._probe_dv == dv0``:
    correct = unchanged (the "misses" assertion alone does not
    discriminate here -- the next lookup sees this commit on the probe
    regardless of whether the absorb's own baseline moved), mutant (the
    baseline stored before, or regardless of, the ``_conn`` read) =
    ``_probe_dv`` moved to reflect this commit from inside the absorb.
    """
    db = fx.db
    hit = _serve(db, fx)
    denials: list[tuple[int, str | None, str | None]] = []
    epoch0 = db.input_epoch
    dv0 = db._probe_dv  # noqa: SLF001

    def _inject() -> None:
        sql, params = _training_update_sql(fx)
        raw = sqlite3.connect(db.path)
        try:
            raw.execute(sql, params)
            raw.commit()
        finally:
            raw.close()

    def _fn(c: sqlite3.Connection) -> None:
        c.set_authorizer(_t137_authorizer(denials, on_first=_inject))
        set_runtime_state(c, "t137_key", "leg2b")

    caplog.set_level(logging.DEBUG, logger="wxverify.db.connection")
    try:
        asyncio.run(FencedWriter(db, db.generation, epoch_exempt=True).write(_fn))
    finally:
        db._conn.set_authorizer(None)  # noqa: SLF001

    assert len(denials) == 1
    assert db._probe_dv == dv0  # noqa: SLF001
    assert db.input_epoch == epoch0

    uncached_after = _uncached(db, fx)
    assert uncached_after != hit.weights
    missed = _serve(db, fx)
    assert missed.weights == uncached_after


def test_t137_leg3_original_exception_survives(fx: _Fixture) -> None:
    """Plan T137, leg 3.

    mutant_absorb_raises_out_of_finally -> at
    ``pytest.raises(ValueError, match="t137")``: correct = the original
    ``ValueError`` propagates (the absorb's own denial is caught and
    logged, never re-raised), mutant (the absorb raising out of the
    ``finally``) = a ``DatabaseError`` replaces the ``ValueError`` instead.
    """
    db = fx.db
    denials: list[tuple[int, str | None, str | None]] = []

    raw0 = sqlite3.connect(db.path)
    try:
        raw0.execute(
            "INSERT INTO runtime_state(key, value) VALUES ('t137_key', 'before')"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value"
        )
        raw0.commit()
    finally:
        raw0.close()
    asyncio.run(db.write(lambda c: None))

    def _fn(c: sqlite3.Connection) -> None:
        c.set_authorizer(_t137_authorizer(denials))
        set_runtime_state(c, "t137_key", "leg3-new")
        raise ValueError("t137")

    try:
        with pytest.raises(ValueError, match="t137"):
            asyncio.run(db.write(_fn))
    finally:
        db._conn.set_authorizer(None)  # noqa: SLF001

    assert len(denials) == 1

    raw = sqlite3.connect(db.path)
    try:
        row = raw.execute(
            "SELECT value FROM runtime_state WHERE key = 't137_key'"
        ).fetchone()
    finally:
        raw.close()
    assert row is not None
    assert row[0] == "before"


def test_t137_leg4_non_sqlite_error_in_absorb(
    fx: _Fixture, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan T137, leg 4.

    mutant_catch_too_narrow -> at the write returning without raising:
    correct = it returns normally (a catch of only ``sqlite3.Error`` would
    still have to pass a ``RuntimeError`` through, since it is not a
    ``sqlite3.Error``), mutant (a catch of only ``sqlite3.Error``) = this
    leg raises instead.
    """
    db = fx.db
    dv0 = db._probe_dv  # noqa: SLF001

    class _BrokenProbe:
        def execute(self, *args: object, **kwargs: object) -> Any:
            raise RuntimeError("t137-probe")

    caplog.set_level(logging.WARNING, logger="wxverify.db.connection")
    with monkeypatch.context() as m:
        m.setattr(db, "_probe", _BrokenProbe())
        asyncio.run(
            FencedWriter(db, db.generation, epoch_exempt=True).write(
                lambda c: set_runtime_state(c, "t137_key", "leg4")
            )
        )

    assert db._probe_dv == dv0  # noqa: SLF001

    warning_records = [
        r
        for r in caplog.records
        if r.name == "wxverify.db.connection" and r.exc_info is not None
    ]
    assert len(warning_records) == 1
    assert warning_records[0].levelno == logging.WARNING
    assert warning_records[0].exc_info[0] is RuntimeError  # type: ignore[index]

    stats_before = wind_blend.wind_weights_cache_stats()
    _serve(db, fx)
    stats_after = wind_blend.wind_weights_cache_stats()
    assert stats_after["misses"] == stats_before["misses"] + 1  # type: ignore[operator]


# --- T145/T146: two pinned readers racing a write --------------------------


def test_t145_two_pinned_requests_race(fx: _Fixture) -> None:
    """New T145: two pinned readers racing a write between their snapshots.

    mutant_pinned_snapshot_drops_second_reading -> at
    ``db.snapshot_pin(c1) == "counts_moved"`` (checked inside c1's own
    ``with`` block): correct = ``"counts_moved"`` (``pinned_snapshot``'s two
    readings disagree, since the raw commit lands between them), mutant
    (``pinned_snapshot`` comparing only one of its two readings) = a real
    ``InputCounts`` pin instead of the refusal string. c1 still SERVES
    ``w_after`` either way -- its priming statement, not ``BEGIN DEFERRED``
    itself, is what fixes the WAL snapshot, and that priming statement runs
    after the raw commit under both arms -- so ``r1.weights == w_after``
    does NOT discriminate this mutant on its own; the ``snapshot_pin``
    assertion above it is the real kill point. Were that assertion absent,
    the kill would surface one step later, by one of two routes
    depending on which of ``pinned_snapshot``'s two readings the mutant
    uses as the pin. If the pin is the first reading, c1's pin is the
    pre-commit ``(ext, epoch)``; c1 misses and stores ``w_after`` under
    that key (``wind_blend.py:642``), c2's pin is the same key, so c2
    hits ``w_after`` and the test dies at ``r2.weights == w_before``
    (about :3268). If the pin is the second reading, c1's key correctly
    describes its own snapshot and c2 misses, computes ``w_before``,
    and the older-pin guard (``wind_blend.py:598-605``) refuses to
    store it, so r1 and r2 both pass and the test dies instead at the
    ``counts_moved == 1`` check (about :3271).
    """
    db = fx.db
    w_before = _uncached(db, fx)
    reset_wind_weights_cache()
    c1, c2 = asyncio.run(_checkout_two(db))
    try:
        with pinned_read_snapshot(c2, label="t145_c2"):
            fired = False

            def _cb(stmt: str) -> None:
                nonlocal fired
                if not fired and stmt.strip() == "BEGIN DEFERRED":
                    fired = True
                    sql, params = _training_update_sql(fx)
                    raw = sqlite3.connect(db.path)
                    try:
                        raw.execute(sql, params)
                        raw.commit()
                    finally:
                        raw.close()

            c1.set_trace_callback(_cb)
            try:
                with pinned_read_snapshot(c1, label="t145_c1"):
                    assert db.snapshot_pin(c1) == "counts_moved"
                    r1 = wind_blend.load_wind_serving(
                        c1,
                        site_id=fx.site_id,
                        timezone=fx.timezone,
                        today=fx.today,
                        as_of=None,
                    )
            finally:
                c1.set_trace_callback(None)

            r2 = wind_blend.load_wind_serving(
                c2,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )
    finally:
        _release_two(db, c1, c2)

    w_after = _uncached(db, fx)
    assert w_after != w_before
    assert r1.weights == w_after
    assert r2.weights == w_before

    stats = wind_blend.wind_weights_cache_stats()
    assert stats["bypasses"]["counts_moved"] == 1  # type: ignore[index]
    assert stats["misses"] == 1
    assert stats["hits"] == 0
    for reason, count in stats["bypasses"].items():  # type: ignore[union-attr]
        if reason != "counts_moved":
            assert count == 0, reason

    follow = _serve(db, fx)
    assert follow.weights == w_after
    stats_follow = wind_blend.wind_weights_cache_stats()
    assert stats_follow["misses"] == stats["misses"] + 1  # type: ignore[index]


def test_t146_dashboard_path_vs_pinned_race(fx: _Fixture) -> None:
    """New T146: a pinned reader racing the dashboard's own (unpinned, but
    still counts-consistent) no-transaction path.

    mutant_dashboard_drops_second_reading -> at ``r2.weights == w_before``:
    correct = the stale pre-commit weights, because the dashboard path's
    own (correctly moved-counts) call never stores, so c2's later pinned
    lookup -- pinned to the earlier, pre-commit counts -- recomputes fresh
    instead of hitting anything, mutant (removing the dashboard path's
    second reading, so it stores the post-commit weights keyed on the
    stale pre-commit counts) = the corrupted entry's wrongly stored
    ``w_after`` weights instead, because c2's pin matches that stale key.
    """
    db = fx.db
    w_before = _uncached(db, fx)
    reset_wind_weights_cache()
    c1, c2 = asyncio.run(_checkout_two(db))
    try:
        with pinned_read_snapshot(c2, label="t146_c2"):
            fired = False

            def _cb(stmt: str) -> None:
                nonlocal fired
                if not fired and stmt.strip() == "BEGIN DEFERRED":
                    fired = True
                    sql, params = _training_update_sql(fx)
                    raw = sqlite3.connect(db.path)
                    try:
                        raw.execute(sql, params)
                        raw.commit()
                    finally:
                        raw.close()

            c1.set_trace_callback(_cb)
            try:
                assert not c1.in_transaction
                r1 = wind_blend.load_wind_serving(
                    c1,
                    site_id=fx.site_id,
                    timezone=fx.timezone,
                    today=fx.today,
                    as_of=None,
                )
            finally:
                c1.set_trace_callback(None)

            r2 = wind_blend.load_wind_serving(
                c2,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )
    finally:
        _release_two(db, c1, c2)

    w_after = _uncached(db, fx)
    assert w_after != w_before
    assert r1.weights == w_after
    assert r2.weights == w_before

    stats = wind_blend.wind_weights_cache_stats()
    assert stats["bypasses"]["counts_moved"] == 1  # type: ignore[index]
    assert stats["misses"] == 1
    assert stats["hits"] == 0


# --- T147: C7 guard -- an older pin never evicts a newer entry -------------


def test_t147_older_pin_late_store_does_not_evict_newer(fx: _Fixture) -> None:
    """New T147 (cross-check): the C7 guard -- an older pin's late store
    must not evict a newer, already-held entry for the same site.

    mutant_c7_guard_removed -> at ``rehit.weights == w_new`` and
    ``stats_after_rehit["hits"] == stats_before_rehit["hits"] + 1``:
    correct = both (the guard refused the older pin's store, so the newer
    entry this test seeded survives untouched and a follow-up call HITS
    it), mutant (the C7 guard removed, so ``_store`` always overwrites) =
    the older pin's late store clobbers the newer entry with ``w_before``,
    so the follow-up wrongly misses (or hits stale data) against the
    lower counts.
    """
    db = fx.db
    w_before = _uncached(db, fx)
    reset_wind_weights_cache()

    c_old = asyncio.run(_checkout_one(db))
    try:
        with pinned_read_snapshot(c_old, label="t147_old"):
            pin_old = db.snapshot_pin(c_old)
            assert isinstance(pin_old, InputCounts)

            # A write moves the counts upward while the old pin stays open.
            sql, params = _training_update_sql(fx)
            db.write_sync(lambda c: c.execute(sql, params))
            w_new = _uncached(db, fx)
            assert w_new != w_before

            # An ordinary call stores the NEW entry at the NEW (higher) counts.
            newhit = _serve(db, fx)
            assert newhit.weights == w_new

            # The old pin's late call: its own snapshot still reflects the
            # pre-write state, but its store attempt must be refused.
            stats_before_late = wind_blend.wind_weights_cache_stats()
            late = wind_blend.load_wind_serving(
                c_old,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )
            assert late.weights == w_before
            stats_after_late = wind_blend.wind_weights_cache_stats()
            assert (
                stats_after_late["misses"]  # type: ignore[index]
                == stats_before_late["misses"] + 1  # type: ignore[index]
            )
    finally:
        _release_one(db, c_old)

    stats_before_rehit = wind_blend.wind_weights_cache_stats()
    rehit = _serve(db, fx)
    assert rehit.weights == w_new
    stats_after_rehit = wind_blend.wind_weights_cache_stats()
    assert (
        stats_after_rehit["hits"]  # type: ignore[index]
        == stats_before_rehit["hits"] + 1  # type: ignore[index]
    )


# --- T148: a commit between input_counts()'s two reads ---------------------


@pytest.mark.parametrize("kind", ["own_write", "raw_commit"])
def test_t148_commit_between_ext_and_epoch_reads(
    fx: _Fixture, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """New T148 (cross-check, invariant oracle): a commit landing between
    ``input_counts()``'s two reads (``external_commit_seq()`` then the
    ``input_epoch`` property) must never leave the cache serving or
    storing data that disagrees with the database's real, current state --
    whichever side of the race the resulting pin lands on.

    Deliberately an invariant check, not a fixed-branch oracle: both the
    "own write" sub-case (bumps ``input_epoch`` synchronously, absorbed
    uncounted so ``ext`` does not move) and the "raw external commit"
    sub-case (moves the probe's ``data_version`` but not ``epoch`` until a
    later write observes it) are legitimate races this oracle must
    tolerate -- it asserts the cache is never corrupted by either, not
    which specific pin value results.

    Invariant check: the cache never serves or stores weights that
    disagree with the database after this race. No mutant is known to
    fail here; the read-order swap is absorbed by the two-reading
    check.

    A hypothetical mutant this invariant is designed to catch -> at
    ``follow.weights == uncached_now``: correct = always equal (any entry
    this race stores, or any bypass it takes, must still agree with ground
    truth once the race has settled), a mutant that built ``input_counts``
    from a torn, inconsistent ``(ext, epoch)`` pair and trusted it as a
    valid pin/stored key would let a follow-up ordinary call wrongly HIT an
    entry keyed on a pair that never described any single real moment of
    the database, surfacing as stale or otherwise-wrong served weights --
    but this has not been empirically confirmed by mutating the
    implementation.
    """
    db = fx.db
    reset_wind_weights_cache()
    real_ext = db.external_commit_seq
    fired = False

    def _wrapper() -> int | None:
        nonlocal fired
        value = real_ext()
        if not fired:
            fired = True
            sql, params = _training_update_sql(fx)
            if kind == "own_write":
                db.write_sync(lambda c: c.execute(sql, params))
            else:
                raw = sqlite3.connect(db.path)
                try:
                    raw.execute(sql, params)
                    raw.commit()
                finally:
                    raw.close()
        return value

    monkeypatch.setattr(db, "external_commit_seq", _wrapper)

    def _run(conn: sqlite3.Connection) -> Any:
        with pinned_read_snapshot(conn, label="t148_race"):
            return wind_blend.load_wind_serving(
                conn,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )

    asyncio.run(db.read(_run))
    assert fired

    monkeypatch.undo()
    follow = _serve(db, fx)
    uncached_now = _uncached(db, fx)
    assert follow.weights == uncached_now


# --- T149: a priming-read failure inside a pin clears it cleanly -----------


def test_t149_priming_failure_clears_pin_and_transaction(fx: _Fixture) -> None:
    """New T149 (cross-check): a priming-read failure inside
    ``pinned_read_snapshot`` must clear the pin and the transaction,
    leaving the connection fully reusable.

    mutant_pin_registered_before_priming_fails -> at
    ``db.snapshot_pin(conn) is None`` and ``conn.in_transaction is False``
    (checked right after the failure): correct = both (``pinned_snapshot``
    only registers the pin AFTER ``read_snapshot``'s priming read
    succeeds, and ``read_snapshot``'s own ``finally`` always rolls back on
    a priming failure), mutant (registering the pin, or skipping the
    rollback, before the priming statement's own exception has a chance to
    propagate) = either a stale pin survives the failure, or the
    connection is left mid-transaction and a later use on it raises
    instead of running.
    """
    db = fx.db
    reset_wind_weights_cache()

    def _deny(action: int, arg1: str | None, *_rest: object) -> int:
        if action == sqlite3.SQLITE_PRAGMA and arg1 == "user_version":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def _run(conn: sqlite3.Connection) -> Any:
        conn.set_authorizer(_deny)
        try:
            with (
                pytest.raises(sqlite3.DatabaseError),
                pinned_read_snapshot(conn, label="t149_priming_fails"),
            ):
                pass
        finally:
            conn.set_authorizer(None)
        assert db.snapshot_pin(conn) is None
        assert conn.in_transaction is False

        # The connection is fully reusable: an ordinary call now succeeds.
        return wind_blend.load_wind_serving(
            conn,
            site_id=fx.site_id,
            timezone=fx.timezone,
            today=fx.today,
            as_of=None,
        )

    uncached = _uncached(db, fx)
    served = asyncio.run(db.read(_run))
    assert served.weights == uncached


# --- T150: a nesting refusal preserves a pre-existing plain transaction ----


def test_t150_nesting_refusal_from_plain_transaction_preserves_it(
    fx: _Fixture,
) -> None:
    """New T150 (cross-check): a nesting refusal on a connection already in
    a PLAIN (non-pinned) caller-opened transaction preserves that
    transaction and registers no pin.

    mutant_pinned_snapshot_ignores_pre_existing_transaction -> at
    ``db.snapshot_pin(conn) is None`` and ``conn.in_transaction is True``
    (checked right after the refused attempt): correct = both (the
    nesting pre-check in ``read_snapshot`` fires before any SQL, so
    ``pinned_snapshot`` never reaches its own ``_snapshot_pins`` assignment
    at all), mutant (``pinned_snapshot`` registering a pin for ``conn``
    speculatively before delegating to ``read_snapshot``) = a pin wrongly
    appears for a connection the cache never actually described a snapshot
    for, and/or the caller's own transaction ends up disturbed.
    """
    db = fx.db
    reset_wind_weights_cache()

    def _run(conn: sqlite3.Connection) -> Any:
        conn.execute("BEGIN")
        try:
            assert db.snapshot_pin(conn) is None
            with (
                pytest.raises(SnapshotNestingError),
                pinned_read_snapshot(conn, label="t150_inner"),
            ):
                pass
            assert db.snapshot_pin(conn) is None
            assert conn.in_transaction is True
            result = wind_blend.load_wind_serving(
                conn,
                site_id=fx.site_id,
                timezone=fx.timezone,
                today=fx.today,
                as_of=None,
            )
            assert conn.in_transaction is True
        finally:
            conn.rollback()
        return result

    before = wind_blend.wind_weights_cache_stats()
    uncached = _uncached(db, fx)
    served = asyncio.run(db.read(_run))
    assert served.weights == uncached
    after = wind_blend.wind_weights_cache_stats()
    assert (
        after["bypasses"]["in_transaction"]  # type: ignore[index]
        == before["bypasses"]["in_transaction"] + 1  # type: ignore[index]
    )
