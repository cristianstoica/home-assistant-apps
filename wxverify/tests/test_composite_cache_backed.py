"""Verification suite for the composite cache-backed read path (plan
"ground-yourself-in-home-assistant-apps-mutable-blanket", 0.4.2).

Covers ``composite_with_status``/``enqueue_composite_rescore`` in
``wxverify/scoring/composite.py``: the cached/live equivalence linchpin, the
perf property that ``hit``/``stale`` never fall through to a live recompute,
the full status matrix (``hit``/``stale``/``rebuilding``/``empty``/``live``)
plus enqueue dedup at both enqueue sites (``/api/composite`` and the
dashboard), the whole-window partial-snapshot contract, the terminal-failure
enqueue cooldown, the custom-window cache bypass, and the restart cache
preservation contract in ``wxverify/settings/service.py``.

Isolation: every test builds its own tmp DB via ``_init_tmp_db`` (scoring-level
tests) or ``_start_app`` (HTTP-level tests via ``TestClient`` + an idle
worker), mirroring ``tests/test_web_ui.py``'s harness -- including its real
empty ``options.json`` (not a missing path), which routes runtime options
through the file loader instead of falling back to ambient ``WXV_*`` env vars
that would otherwise clobber a test's DB-seeded settings on startup.

Freshness fixtures use a real "now" (``isoformat_utc()``) for ``hit`` rows and
a fixed past date (``2020-01-01T00:00:00Z``) for ``stale`` rows -- never the
``w:all``/2035-style always-fresh convention, which would silently skip the
staleness branch entirely for a rolling-window row.

Synthetic fixtures only -- fake site names and the repo's existing 47/25
lat-lon convention (already used throughout this suite), no real keys or
station IDs.
"""

from __future__ import annotations

import asyncio
import math
import sqlite3
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.composite import (
    _expected_active_cells,
    _live_composite,
    composite_with_status,
    enqueue_composite_rescore,
)
from wxverify.scoring.consensus import materialize_consensus
from wxverify.scoring.leaderboard import resolve_window
from wxverify.scoring.metrics import MetricResult, MetricStrategy, strategy_for
from wxverify.settings.keys import get_number_setting, set_setting
from wxverify.settings.service import set_rolling_window_days_sync

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_web_ui.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    """Drop-in run_worker shim that idles without touching the scheduler."""
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    # Real (empty-object) options file, not a missing path -- see module
    # docstring; a missing path falls back to ambient WXV_* env vars and
    # clobbers DB-seeded settings on the next lifespan startup.
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


def _start_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Bootstrap a fresh tmp DB + idle-worker app for HTTP-level tests.

    Seeding happens AFTER entering ``TestClient`` (via ``get_db().write_sync``)
    so it runs against the exact instance the running app uses -- the
    lifespan's own ``init_db`` call would otherwise close/replace a
    pre-built connection out from under a test.
    """
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _make_site(conn: sqlite3.Connection, name: str, *, enabled: int = 1) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 47.0, 25.0, 900.0, 'UTC', ?)
            """,
            (name, enabled),
        ).lastrowid
    )


def _open_meteo_feed_ids(conn: sqlite3.Connection, count: int) -> list[int]:
    return [
        int(row["id"])
        for row in conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT ?",
            (count,),
        )
    ]


def _persistence_feed_id(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
        ).fetchone()["id"]
    )


def _add_continuous_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    persistence_feed_id: int,
    variable: str,
    skill_score: float,
    day_ahead: int = 1,
    valid_hour: int,
) -> None:
    """Insert a feed pair + matching persistence pair realizing an exact skill.

    Mirrors ``tests/test_m1_m5.py``'s ``add_pair`` helper: the feed's
    ``sq_error`` is derived from ``skill_score`` via the persistence-MSE-ratio
    relationship the real ``ContinuousStrategy`` computes, so calling the real
    ``aggregate()`` against these rows reproduces the target skill exactly.
    """
    observed = 10.0
    persistence_sq_error = 4.0
    feed_sq_error = (1.0 - skill_score) * persistence_sq_error
    feed_error = math.sqrt(feed_sq_error)
    persistence_error = math.sqrt(persistence_sq_error)
    valid_at = f"2035-01-02T{valid_hour:02d}:00:00Z"
    issued_at = f"2035-01-01T{valid_hour:02d}:00:00Z"
    conn.execute(
        """
        INSERT OR IGNORE INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error)
        VALUES (?, ?, ?, ?, ?, 24, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            persistence_feed_id,
            variable,
            issued_at,
            valid_at,
            day_ahead,
            observed + persistence_error,
            observed,
            persistence_error,
            abs(persistence_error),
            persistence_sq_error,
        ),
    )
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error)
        VALUES (?, ?, ?, ?, ?, 24, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            day_ahead,
            observed + feed_error,
            observed,
            feed_error,
            abs(feed_error),
            feed_sq_error,
        ),
    )


def _add_precip_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    day_ahead: int = 1,
    valid_hour: int,
    cat_hit: int,
    cat_false: int,
    cat_miss: int,
    cat_correct_neg: int,
) -> None:
    valid_at = f"2035-01-02T{valid_hour:02d}:00:00Z"
    issued_at = f"2035-01-01T{valid_hour:02d}:00:00Z"
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             cat_hit, cat_false, cat_miss, cat_correct_neg)
        VALUES (?, ?, 'precip', ?, ?, 24, ?, 0.0, 0.0, 0.0, 0.0, 0.0, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            issued_at,
            valid_at,
            day_ahead,
            cat_hit,
            cat_false,
            cat_miss,
            cat_correct_neg,
        ),
    )


def _add_temperature_cell(
    conn: sqlite3.Connection, *, site_id: int, feed_id: int, valid_at: str
) -> None:
    """Minimal single-row cell: enough to populate ``_expected_active_cells``.

    No matching persistence pair, so live aggregation of this exact cell would
    yield ``skill_score=None`` -- fine for tests that only need the cell to
    exist (status-matrix / enqueue-dedup tests never inspect the numeric
    skill).
    """
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error)
        VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z', ?, 24, 1,
                11.0, 10.0, 1.0, 1.0, 1.0)
        """,
        (site_id, feed_id, valid_at),
    )


def _job_count(conn: sqlite3.Connection, site_id: int) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE type='pair_and_score' AND site_id=?",
            (site_id,),
        ).fetchone()["n"]
    )


def _insert_job(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    status: str,
    created_at: str,
    updated_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO jobs
            (type, site_id, job_key, payload, status, created_at, updated_at)
        VALUES ('pair_and_score', ?, 'score', '{}', ?, ?, ?)
        """,
        (site_id, status, created_at, updated_at),
    )


# ---------------------------------------------------------------------------
# Area 1 -- equivalence linchpin: cached Composite == live Composite.
# ---------------------------------------------------------------------------


def test_cached_composite_matches_live_composite_rolling_window(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "Equivalence Rolling")
    feed_ids = _open_meteo_feed_ids(conn, 4)
    persistence_id = _persistence_feed_id(conn)

    # feed 0: confident temperature (n=2) + confident NEGATIVE-skill wind (n=2).
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[0],
        persistence_feed_id=persistence_id,
        variable="temperature",
        skill_score=0.6,
        valid_hour=0,
    )
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[0],
        persistence_feed_id=persistence_id,
        variable="temperature",
        skill_score=0.6,
        valid_hour=1,
    )
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[0],
        persistence_feed_id=persistence_id,
        variable="wind",
        skill_score=-0.3,
        valid_hour=2,
    )
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[0],
        persistence_feed_id=persistence_id,
        variable="wind",
        skill_score=-0.3,
        valid_hour=3,
    )
    # feed 1: confident precip (n=2 rows, ETS from categorical counts).
    _add_precip_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[1],
        valid_hour=4,
        cat_hit=1,
        cat_false=0,
        cat_miss=0,
        cat_correct_neg=1,
    )
    _add_precip_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[1],
        valid_hour=5,
        cat_hit=1,
        cat_false=0,
        cat_miss=0,
        cat_correct_neg=1,
    )
    # feed 2: single pair (n=1 < min_n=2) -- must be excluded from BOTH paths.
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[2],
        persistence_feed_id=persistence_id,
        variable="temperature",
        skill_score=0.9,
        valid_hour=6,
    )
    # feed 3: disabled at this site -- must be excluded from BOTH paths.
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 0)",
        (site_id, feed_ids[3]),
    )
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[3],
        persistence_feed_id=persistence_id,
        variable="temperature",
        skill_score=0.9,
        valid_hour=7,
    )

    resolved = resolve_window(conn, "rolling")
    assert resolved.window_key == "w:14"
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    expected_cells = _expected_active_cells(conn, site_id=site_id, resolved=resolved)
    for expected_feed_id, variable, day_ahead in expected_cells:
        result = strategy_for(variable).aggregate(
            conn,
            site_id=site_id,
            feed_id=expected_feed_id,
            variable=variable,
            day_ahead=day_ahead,
            window_cutoff=resolved.cutoff,
            min_n=min_n,
        )
        upsert_score_cache(
            conn,
            site_id=site_id,
            feed_id=expected_feed_id,
            variable=variable,
            day_ahead=day_ahead,
            window_key=resolved.window_key,
            result=result,
            computed_at=isoformat_utc(),
        )

    cached_result = composite_with_status(conn, site_id=site_id, window="rolling")
    assert cached_result.status == "hit"
    live_rows = _live_composite(
        conn, site_id=site_id, window_key=resolved.window_key, cutoff=resolved.cutoff
    )
    assert cached_result.rows == live_rows

    served_feed_ids = {row["feed_id"] for row in cached_result.rows}
    assert feed_ids[0] in served_feed_ids
    assert feed_ids[1] in served_feed_ids
    assert feed_ids[2] not in served_feed_ids  # n < min_n, excluded both ways
    assert feed_ids[3] not in served_feed_ids  # disabled, excluded both ways


def test_cached_composite_matches_live_composite_all_window(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    site_id = _make_site(conn, "Equivalence All")
    feed_ids = _open_meteo_feed_ids(conn, 4)
    persistence_id = _persistence_feed_id(conn)

    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[0],
        persistence_feed_id=persistence_id,
        variable="temperature",
        skill_score=0.5,
        valid_hour=0,
    )
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[0],
        persistence_feed_id=persistence_id,
        variable="temperature",
        skill_score=0.5,
        valid_hour=1,
    )
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[0],
        persistence_feed_id=persistence_id,
        variable="wind",
        skill_score=-0.4,
        valid_hour=2,
    )
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[0],
        persistence_feed_id=persistence_id,
        variable="wind",
        skill_score=-0.4,
        valid_hour=3,
    )
    _add_precip_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[1],
        valid_hour=4,
        cat_hit=1,
        cat_false=1,
        cat_miss=0,
        cat_correct_neg=2,
    )
    _add_precip_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[1],
        valid_hour=5,
        cat_hit=1,
        cat_false=0,
        cat_miss=1,
        cat_correct_neg=2,
    )
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[2],
        persistence_feed_id=persistence_id,
        variable="temperature",
        skill_score=0.9,
        valid_hour=6,
    )
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (?, ?, 0)",
        (site_id, feed_ids[3]),
    )
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_ids[3],
        persistence_feed_id=persistence_id,
        variable="temperature",
        skill_score=0.9,
        valid_hour=7,
    )

    resolved = resolve_window(conn, "all")
    assert resolved.window_key == "w:all"
    assert resolved.cutoff is None
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    expected_cells = _expected_active_cells(conn, site_id=site_id, resolved=resolved)
    for expected_feed_id, variable, day_ahead in expected_cells:
        result = strategy_for(variable).aggregate(
            conn,
            site_id=site_id,
            feed_id=expected_feed_id,
            variable=variable,
            day_ahead=day_ahead,
            window_cutoff=resolved.cutoff,
            min_n=min_n,
        )
        upsert_score_cache(
            conn,
            site_id=site_id,
            feed_id=expected_feed_id,
            variable=variable,
            day_ahead=day_ahead,
            window_key=resolved.window_key,
            result=result,
            computed_at=isoformat_utc(),
        )

    cached_result = composite_with_status(conn, site_id=site_id, window="all")
    assert cached_result.status == "hit"
    live_rows = _live_composite(
        conn, site_id=site_id, window_key=resolved.window_key, cutoff=resolved.cutoff
    )
    assert cached_result.rows == live_rows

    served_feed_ids = {row["feed_id"] for row in cached_result.rows}
    assert feed_ids[0] in served_feed_ids
    assert feed_ids[1] in served_feed_ids
    assert feed_ids[2] not in served_feed_ids
    assert feed_ids[3] not in served_feed_ids


# ---------------------------------------------------------------------------
# Areas 2+3 -- perf property (hit AND stale never recompute live) plus the
# deterministic stale-row construction. This is the single most load-bearing
# test in the suite per the plan: asserting `hit` alone would let a
# stale-triggers-live-recompute regression ship green.
# ---------------------------------------------------------------------------


def test_composite_never_recomputes_live_on_hit_or_stale_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "Perf Property")
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
    )
    upsert_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_key="w:14",
        result=MetricResult(n=5, skill_score=0.6, confident=True),
        computed_at=isoformat_utc(),
    )

    calls: list[str] = []

    def _spy(variable: str) -> MetricStrategy:
        calls.append(variable)
        return strategy_for(variable)

    monkeypatch.setattr("wxverify.scoring.composite.strategy_for", _spy)

    hit = composite_with_status(conn, site_id=site_id, window="rolling")
    assert hit.status == "hit"
    assert calls == []

    # Deterministic stale-row construction: a fixed PAST date, never the
    # w:all/2035 always-fresh convention (that would silently skip staleness).
    conn.execute(
        "UPDATE score_cache SET computed_at='2020-01-01T00:00:00Z' "
        "WHERE site_id=? AND window_key='w:14'",
        (site_id,),
    )
    stale = composite_with_status(conn, site_id=site_id, window="rolling")
    assert stale.status == "stale"
    # The load-bearing assertion: stale is served from cache, never live.
    assert calls == []

    # Paired positive: the custom ("Nd") window genuinely IS live and DOES
    # invoke the spy -- proving the two empty-call assertions above are not a
    # vacuous/ambient absence (the spy demonstrably can and does fire).
    live = composite_with_status(conn, site_id=site_id, window="30d")
    assert live.status == "live"
    assert calls == ["temperature"]


# ---------------------------------------------------------------------------
# Area 4 -- status matrix + enqueue dedup (HTTP-level, first /api/composite
# tests -- also satisfies Area 9).
# ---------------------------------------------------------------------------


def test_api_composite_fresh_cache_returns_hit_with_zero_enqueue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> tuple[int, int]:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Fresh Hit")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            upsert_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="temperature",
                day_ahead=1,
                window_key="w:14",
                result=MetricResult(n=1, skill_score=0.5, confident=True),
                computed_at=isoformat_utc(),
            )
            return site_id, feed_id

        site_id, feed_id = db.write_sync(_seed)
        response = client.get(
            "/api/composite", params={"site": site_id, "window": "rolling"}
        )
        assert response.status_code == 200
        rows = response.json()
        assert any(row["feed_id"] == feed_id for row in rows)
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 0


def test_api_composite_stale_cache_enqueues_exactly_once_across_polls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Stale Dedup")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            upsert_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="temperature",
                day_ahead=1,
                window_key="w:14",
                result=MetricResult(n=1, skill_score=0.4, confident=True),
                computed_at="2020-01-01T00:00:00Z",  # fixed past date -> stale
            )
            return site_id

        site_id = db.write_sync(_seed)
        first = client.get(
            "/api/composite", params={"site": site_id, "window": "rolling"}
        )
        assert first.status_code == 200
        assert first.json()  # stale is still SERVED, never recomputed to empty
        second = client.get(
            "/api/composite", params={"site": site_id, "window": "rolling"}
        )
        assert second.status_code == 200
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1


def test_api_composite_missing_snapshot_rebuilds_and_enqueues_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Rebuilding Dedup")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            return site_id

        site_id = db.write_sync(_seed)
        first = client.get(
            "/api/composite", params={"site": site_id, "window": "rolling"}
        )
        assert first.status_code == 200
        assert first.json() == []
        second = client.get(
            "/api/composite", params={"site": site_id, "window": "rolling"}
        )
        assert second.json() == []
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1


def test_api_composite_historical_only_pairs_are_empty_with_within_cutoff_positive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No-input gate: pairs entirely outside the cutoff yield `empty`, never an
    enqueue -- paired against an otherwise-identical site whose pairs fall
    within the cutoff, which DOES enqueue. Without the positive, the "0
    enqueue" claim would be indistinguishable from "this route never
    enqueues anything."
    """
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> tuple[int, int]:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            site_historical = _make_site(conn, "Historical Only")
            site_current = _make_site(conn, "Within Cutoff")
            _add_temperature_cell(
                conn,
                site_id=site_historical,
                feed_id=feed_id,
                valid_at="2000-01-01T00:00:00Z",
            )
            _add_temperature_cell(
                conn,
                site_id=site_current,
                feed_id=feed_id,
                valid_at="2035-01-02T00:00:00Z",
            )
            return site_historical, site_current

        site_historical, site_current = db.write_sync(_seed)

        empty_resp = client.get(
            "/api/composite", params={"site": site_historical, "window": "rolling"}
        )
        assert empty_resp.json() == []
        assert db.read_sync(lambda conn: _job_count(conn, site_historical)) == 0

        rebuilding_resp = client.get(
            "/api/composite", params={"site": site_current, "window": "rolling"}
        )
        assert rebuilding_resp.json() == []
        assert db.read_sync(lambda conn: _job_count(conn, site_current)) == 1


def test_api_composite_disabled_site_is_empty_despite_fresh_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disabled site: even a valid FRESH full snapshot must not yield `hit` --
    paired against the same construction with the site enabled, which DOES
    serve the cached hit.
    """
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection, *, enabled: int, name: str) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, name, enabled=enabled)
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            upsert_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="temperature",
                day_ahead=1,
                window_key="w:14",
                result=MetricResult(n=1, skill_score=0.5, confident=True),
                computed_at=isoformat_utc(),
            )
            return site_id

        disabled_site = db.write_sync(
            lambda conn: _seed(conn, enabled=0, name="Disabled Site")
        )
        enabled_site = db.write_sync(
            lambda conn: _seed(conn, enabled=1, name="Enabled Site")
        )

        disabled_resp = client.get(
            "/api/composite", params={"site": disabled_site, "window": "rolling"}
        )
        assert disabled_resp.json() == []  # fresh cache must NOT yield hit
        assert db.read_sync(lambda conn: _job_count(conn, disabled_site)) == 0

        enabled_resp = client.get(
            "/api/composite", params={"site": enabled_site, "window": "rolling"}
        )
        assert enabled_resp.json()  # same construction, enabled -- genuine hit
        assert db.read_sync(lambda conn: _job_count(conn, enabled_site)) == 0


def test_api_composite_disabled_feed_yields_empty_with_enabled_feed_positive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Disabled feeds: no active-competitor cell => genuinely no input =>
    `empty`, never an enqueue -- paired against the identical data with the
    feed enabled, which counts as input and enqueues a rebuild.
    """
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection, *, feed_disabled: bool, name: str) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, name)
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            if feed_disabled:
                conn.execute(
                    "INSERT INTO site_feed_state (site_id, feed_id, enabled) "
                    "VALUES (?, ?, 0)",
                    (site_id, feed_id),
                )
            return site_id

        disabled_site = db.write_sync(
            lambda conn: _seed(conn, feed_disabled=True, name="Disabled Feed")
        )
        enabled_site = db.write_sync(
            lambda conn: _seed(conn, feed_disabled=False, name="Enabled Feed")
        )

        disabled_resp = client.get(
            "/api/composite", params={"site": disabled_site, "window": "rolling"}
        )
        assert disabled_resp.json() == []
        assert db.read_sync(lambda conn: _job_count(conn, disabled_site)) == 0

        enabled_resp = client.get(
            "/api/composite", params={"site": enabled_site, "window": "rolling"}
        )
        assert enabled_resp.json() == []  # still empty rows (no cache seeded)...
        # ...but IS input (rebuilding-eligible), unlike the disabled twin above.
        assert db.read_sync(lambda conn: _job_count(conn, enabled_site)) == 1


def test_api_composite_completed_zero_row_rebuild_never_reenqueues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            site_id = _make_site(conn, "Completed Zero Row")
            # Genuinely no forecast_pairs -- matches a rescore that already
            # ran and legitimately produced zero rows.
            now = isoformat_utc()
            _insert_job(
                conn,
                site_id=site_id,
                status="completed",
                created_at=now,
                updated_at=now,
            )
            return site_id

        site_id = db.write_sync(_seed)
        for _ in range(3):
            resp = client.get(
                "/api/composite", params={"site": site_id, "window": "rolling"}
            )
            assert resp.status_code == 200
            assert resp.json() == []
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1


# ---------------------------------------------------------------------------
# Area 5 -- partial snapshot after consensus invalidation -> rebuilding, never
# a partial aggregate over the surviving variable.
# ---------------------------------------------------------------------------


def test_api_composite_partial_snapshot_after_consensus_invalidation_is_rebuilding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            site_id = _make_site(conn, "Partial Snapshot")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            # Two valid_at hours per variable so the (feed, variable,
            # day_ahead) CELL survives a single-hour consensus invalidation.
            for variable, hour in (
                ("temperature", "02"),
                ("temperature", "03"),
                ("wind", "06"),
                ("wind", "07"),
            ):
                conn.execute(
                    """
                    INSERT INTO forecast_pairs
                        (site_id, feed_id, variable, issued_at, valid_at,
                         lead_hours, day_ahead, forecast, observed, error,
                         abs_error, sq_error)
                    VALUES (?, ?, ?, '2035-01-01T00:00:00Z', ?, 24, 1,
                            11.0, 10.0, 1.0, 1.0, 1.0)
                    """,
                    (site_id, feed_id, variable, f"2035-01-{hour}T00:00:00Z"),
                )
            fresh = isoformat_utc()
            upsert_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="temperature",
                day_ahead=1,
                window_key="w:all",
                result=MetricResult(n=2, skill_score=0.5, confident=True),
                computed_at=fresh,
            )
            upsert_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="wind",
                day_ahead=1,
                window_key="w:all",
                result=MetricResult(n=2, skill_score=0.3, confident=True),
                computed_at=fresh,
            )
            return site_id

        site_id = db.write_sync(_seed)

        pre = client.get("/api/composite", params={"site": site_id, "window": "all"})
        assert pre.status_code == 200
        pre_rows = pre.json()
        assert len(pre_rows) == 1
        assert pre_rows[0]["component_count"] == 2  # genuine full hit, both vars
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 0

        # Real production trigger: materialize_consensus (invoked by
        # insert_station_observation on every corrected/late observation)
        # unconditionally wipes ALL score_cache rows for the target variable
        # before touching forecast_pairs -- the LOAD-BEARING CONTRACT
        # documented on materialize_consensus itself. Only ONE wind hour is
        # invalidated; the OTHER wind forecast_pairs row survives, so the
        # (feed, wind, 1) cell still exists afterward -- proving the mismatch
        # that follows is genuinely cache-vs-cell, not "the cell vanished too".
        db.write_sync(
            lambda conn: materialize_consensus(
                conn, site_id=site_id, variable="wind", valid_at="2035-01-06T00:00:00Z"
            )
        )

        post = client.get("/api/composite", params={"site": site_id, "window": "all"})
        assert post.status_code == 200
        # NEVER a partial aggregate over the surviving temperature component.
        assert post.json() == []
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1


# ---------------------------------------------------------------------------
# Area 6 -- terminal-failure enqueue suppression with a 15-minute cooldown.
# ---------------------------------------------------------------------------


def test_enqueue_composite_rescore_suppressed_within_failure_cooldown(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Cooldown Suppressed")
    now = utc_now()
    _insert_job(
        conn,
        site_id=site_id,
        status="failed",
        created_at=isoformat_utc(now - timedelta(minutes=5)),
        updated_at=isoformat_utc(now - timedelta(minutes=5)),
    )
    enqueue_composite_rescore(conn, site_id)
    assert _job_count(conn, site_id) == 1  # suppressed: no new job


def test_enqueue_composite_rescore_fires_after_cooldown_expires(
    tmp_path: Path,
) -> None:
    """Paired positive for the suppression test above: once the failed job is
    older than the 15-minute cooldown, the next miss enqueues a fresh job.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Cooldown Expired")
    now = utc_now()
    _insert_job(
        conn,
        site_id=site_id,
        status="failed",
        created_at=isoformat_utc(now - timedelta(minutes=20)),
        updated_at=isoformat_utc(now - timedelta(minutes=20)),
    )
    enqueue_composite_rescore(conn, site_id)
    assert _job_count(conn, site_id) == 2
    newest = conn.execute(
        "SELECT status FROM jobs WHERE site_id=? ORDER BY id DESC LIMIT 1", (site_id,)
    ).fetchone()
    assert newest["status"] == "pending"


def test_enqueue_composite_rescore_completed_supersedes_older_failed(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Completed Supersedes")
    now = utc_now()
    _insert_job(
        conn,
        site_id=site_id,
        status="failed",
        created_at=isoformat_utc(now - timedelta(minutes=10)),
        updated_at=isoformat_utc(now - timedelta(minutes=10)),
    )
    _insert_job(
        conn,
        site_id=site_id,
        status="completed",
        created_at=isoformat_utc(now - timedelta(minutes=1)),
        updated_at=isoformat_utc(now - timedelta(minutes=1)),
    )
    enqueue_composite_rescore(conn, site_id)
    # Latest outcome (completed) supersedes the older failure -- proceeds.
    assert _job_count(conn, site_id) == 3


def test_api_composite_repeated_polls_during_cooldown_do_not_spawn_jobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Cooldown HTTP")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            now = utc_now()
            _insert_job(
                conn,
                site_id=site_id,
                status="failed",
                created_at=isoformat_utc(now - timedelta(minutes=5)),
                updated_at=isoformat_utc(now - timedelta(minutes=5)),
            )
            return site_id

        site_id = db.write_sync(_seed)
        for _ in range(3):
            resp = client.get(
                "/api/composite", params={"site": site_id, "window": "rolling"}
            )
            assert resp.status_code == 200
            assert resp.json() == []
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1


def test_api_leaderboard_enqueue_unaffected_by_composite_cooldown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression guard: /api/leaderboard's `_enqueue_score` is not gated by
    the composite-only cooldown, unlike /api/composite's enqueue path.
    """
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            site_id = _make_site(conn, "Leaderboard Cooldown")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            now = utc_now()
            _insert_job(
                conn,
                site_id=site_id,
                status="failed",
                created_at=isoformat_utc(now - timedelta(minutes=5)),
                updated_at=isoformat_utc(now - timedelta(minutes=5)),
            )
            return site_id

        site_id = db.write_sync(_seed)
        response = client.get(
            "/api/leaderboard",
            params={"site": site_id, "variable": "temperature", "lead": "D+1"},
        )
        assert response.status_code == 200
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 2


def test_api_curve_enqueue_unaffected_by_composite_cooldown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            site_id = _make_site(conn, "Curve Cooldown")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            now = utc_now()
            _insert_job(
                conn,
                site_id=site_id,
                status="failed",
                created_at=isoformat_utc(now - timedelta(minutes=5)),
                updated_at=isoformat_utc(now - timedelta(minutes=5)),
            )
            return site_id

        site_id = db.write_sync(_seed)
        response = client.get(
            "/api/curve",
            params={"site": site_id, "variable": "temperature", "lead": "D+1"},
        )
        assert response.status_code == 200
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 2


# ---------------------------------------------------------------------------
# Area 7 -- custom-window bypass (split oracle: scoring status/invocation,
# HTTP enqueue behavior).
# ---------------------------------------------------------------------------


def test_composite_custom_window_status_is_live_and_invokes_live_aggregate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    site_id = _make_site(conn, "Custom Window Live")
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
    )

    calls: list[str] = []

    def _spy(variable: str) -> MetricStrategy:
        calls.append(variable)
        return strategy_for(variable)

    monkeypatch.setattr("wxverify.scoring.composite.strategy_for", _spy)

    result = composite_with_status(conn, site_id=site_id, window="3d")
    assert result.status == "live"
    assert calls == ["temperature"]
    assert conn.execute("SELECT COUNT(*) AS n FROM score_cache").fetchone()["n"] == 0


def test_api_composite_custom_window_never_enqueues_regardless_of_cache_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> tuple[int, int]:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Custom Bypass")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            persistence_id = _persistence_feed_id(conn)
            _add_continuous_pair(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                persistence_feed_id=persistence_id,
                variable="temperature",
                skill_score=0.5,
                valid_hour=0,
            )
            return site_id, feed_id

        site_id, feed_id = db.write_sync(_seed)

        # Contrast positive: the SAME uncached data under "rolling" DOES
        # enqueue (no score_cache seeded -> rebuilding).
        rolling_resp = client.get(
            "/api/composite", params={"site": site_id, "window": "rolling"}
        )
        assert rolling_resp.json() == []
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1

        # Custom Nd window bypasses the cache entirely -- zero jobs, even
        # though the very same cache state just caused an enqueue above.
        custom_resp = client.get(
            "/api/composite", params={"site": site_id, "window": "3d"}
        )
        assert custom_resp.status_code == 200
        custom_rows = custom_resp.json()
        assert any(row["feed_id"] == feed_id for row in custom_rows)
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1  # unchanged


# ---------------------------------------------------------------------------
# Area 8 -- restart cache preservation contract (two separate scenarios).
# ---------------------------------------------------------------------------


def test_restart_unchanged_boot_preserves_rolling_and_all_cache(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "30")
    site_id = _make_site(conn, "Unchanged Boot")
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
    )
    fresh = isoformat_utc()
    for window_key in ("w:30", "w:all", "w:14"):  # w:14 = obsolete leftover
        upsert_score_cache(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            day_ahead=1,
            window_key=window_key,
            result=MetricResult(n=1, skill_score=0.5, confident=True),
            computed_at=fresh,
        )

    set_rolling_window_days_sync(conn, 30)  # unchanged re-apply

    keys = {
        row["window_key"]
        for row in conn.execute(
            "SELECT DISTINCT window_key FROM score_cache WHERE site_id=?", (site_id,)
        )
    }
    assert keys == {"w:30", "w:all"}

    result = composite_with_status(conn, site_id=site_id, window="rolling")
    assert result.status == "hit"  # the preserved rolling slice still serves


def test_restart_window_change_removes_old_slice_and_preserves_all(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "30")
    site_id = _make_site(conn, "Window Change")
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
    )
    fresh = isoformat_utc()
    for window_key in ("w:30", "w:all"):
        upsert_score_cache(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            day_ahead=1,
            window_key=window_key,
            result=MetricResult(n=1, skill_score=0.5, confident=True),
            computed_at=fresh,
        )

    set_rolling_window_days_sync(conn, 14)  # actual 30 -> 14 change

    keys = {
        row["window_key"]
        for row in conn.execute(
            "SELECT DISTINCT window_key FROM score_cache WHERE site_id=?", (site_id,)
        )
    }
    assert keys == {"w:all"}  # old w:30 removed, no stale w:14 leftover either
    assert get_number_setting(conn, "rolling_window_days", 30, minimum=1) == 14

    result = composite_with_status(conn, site_id=site_id, window="rolling")
    assert result.status == "rebuilding"  # w:14 absent -- next rescore rebuilds it
