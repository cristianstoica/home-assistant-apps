"""Verification suite for the leaderboard cache-backed read path (plan
"cache-miss-latency-and-coordinator-observability", 2026-07-25).

Covers ``leaderboard_with_status``/``_cached_leaderboard`` in
``wxverify/scoring/leaderboard.py``: the stale-while-revalidate status matrix
(``hit``/``stale``/``rebuilding``/``empty``/``live``) mirroring
``composite_with_status``'s state model, the no-input applicability gate
(availability vs applicability), the loop-completion pin for mixed-freshness
snapshots, the feed-set mismatch guard, the live custom-window bypass, the
shared terminal-failure enqueue cooldown (`enqueue_score_rescore`, §3), and
the best-effort enqueue contract at both the leaderboard API route and the
dashboard HTML route.

Isolation: every test builds its own tmp DB via ``_init_tmp_db`` (scoring-level
tests) or ``_start_app`` (HTTP-level tests via ``TestClient`` + an idle
worker), mirroring ``tests/test_composite_cache_backed.py``'s harness exactly
-- including its real empty ``options.json`` (not a missing path), which
routes runtime options through the file loader instead of falling back to
ambient ``WXV_*`` env vars that would otherwise clobber a test's DB-seeded
settings on startup.

Freshness fixtures use a real "now" (``isoformat_utc()``) for fresh rows and
a fixed past date (``2020-01-01T00:00:00Z``) for stale rows -- never the
``w:all``/2035-style always-fresh convention, which would silently skip the
staleness branch entirely for a rolling-window row.

Failing-test-first: T1, T2a, and T2c are written against the post-§2 API
shape (``LeaderboardResult.status``) and are confirmed to FAIL on current
(pre-implementation) code -- current code always falls through to
``_live_leaderboard`` on a cache miss/stale read, so monkeypatching it to
raise makes that fallthrough loud rather than silently wrong. T8's two
sub-tests monkeypatch a not-yet-existing ``enqueue_score_rescore`` symbol
(the §3.1 rename target) and are expected to fail with an ``AttributeError``
on the missing attribute until §3 lands -- they are NOT part of the
failing-test-first set.

Synthetic fixtures only -- fake site names and the repo's existing 47/25
lat-lon convention (already used throughout the sibling composite suite), no
real keys or station IDs.
"""

from __future__ import annotations

import asyncio
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
from wxverify.scoring.leaderboard import (
    _live_leaderboard,  # noqa: F401 - referenced by qualified monkeypatch path only
    leaderboard_with_status,
)
from wxverify.scoring.metrics import MetricResult
from wxverify.scoring.rescore import drain_pending_rescores
from wxverify.settings.keys import set_setting

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_composite_cache_backed.py).
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


def _add_temperature_cell(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    valid_at: str,
    day_ahead: int = 1,
) -> None:
    """Minimal single-row cell: enough to populate ``_expected_active_feed_ids``.

    No matching persistence pair, so live aggregation of this exact cell would
    yield ``skill_score=None`` -- fine for tests that only need the cell to
    exist (status-matrix / enqueue-dedup tests never inspect the numeric
    skill computed via the live path).
    """
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error)
        VALUES (?, ?, 'temperature', '2035-01-01T00:00:00Z', ?, 24, ?,
                11.0, 10.0, 1.0, 1.0, 1.0)
        """,
        (site_id, feed_id, valid_at, day_ahead),
    )


def _seed_score_cache(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    window_key: str,
    computed_at: str,
    variable: str = "temperature",
    day_ahead: int = 1,
    n: int = 1,
    skill_score: float | None = 0.5,
) -> None:
    upsert_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable=variable,
        day_ahead=day_ahead,
        window_key=window_key,
        result=MetricResult(n=n, skill_score=skill_score, confident=True),
        computed_at=computed_at,
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


def _raise_live_leaderboard_called(*_args: object, **_kwargs: object) -> list[object]:
    raise AssertionError(
        "_live_leaderboard must not be called for this cache-backed read"
    )


# ---------------------------------------------------------------------------
# T1 -- staleness serves stale, never recomputes (the bug pin / perf linchpin).
# ---------------------------------------------------------------------------


def test_stale_complete_cache_served_never_recomputes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T1. Fails on current code: a stale row makes ``_cached_leaderboard``
    return ``None`` (``is_cache_fresh`` fails), so ``leaderboard_with_status``
    falls through to ``_live_leaderboard``, which this test makes raise.
    """
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "Stale Perf")
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
    )
    _seed_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        window_key="w:14",
        computed_at="2020-01-01T00:00:00Z",
        n=3,
        skill_score=0.42,
    )

    monkeypatch.setattr(
        "wxverify.scoring.leaderboard._live_leaderboard", _raise_live_leaderboard_called
    )

    result = leaderboard_with_status(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )
    assert result.status == "stale"
    assert len(result.rows) == 1
    assert result.rows[0].feed_id == feed_id
    assert result.rows[0].n == 3
    assert result.rows[0].skill_score == 0.42


def test_mixed_freshness_complete_set_downgrades_to_stale_all_rows_served(
    tmp_path: Path,
) -> None:
    """T1b -- loop-completion pin: one stale + one fresh row in an otherwise
    COMPLETE feed-set snapshot must downgrade the whole result to ``stale``
    (never ``hit``), and BOTH rows must still be present -- proving the
    freshness loop completes rather than short-circuiting on the first stale
    row it encounters.
    """
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "Mixed Freshness")
    feed_stale, feed_fresh = _open_meteo_feed_ids(conn, 2)
    for feed_id in (feed_stale, feed_fresh):
        _add_temperature_cell(
            conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
        )
    _seed_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_stale,
        window_key="w:14",
        computed_at="2020-01-01T00:00:00Z",
        n=2,
        skill_score=0.1,
    )
    _seed_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_fresh,
        window_key="w:14",
        computed_at=isoformat_utc(),
        n=4,
        skill_score=0.7,
    )

    result = leaderboard_with_status(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )
    assert result.status == "stale"
    served_feed_ids = {row.feed_id for row in result.rows}
    assert served_feed_ids == {feed_stale, feed_fresh}


# ---------------------------------------------------------------------------
# T2a/T2b -- no-input applicability gate: availability is not applicability.
# ---------------------------------------------------------------------------


def test_no_applicable_input_is_empty_costed_noop_across_http_polls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T2a. Fails on current code: with no cache and no applicable input,
    ``leaderboard_with_status`` still falls through unconditionally to
    ``_live_leaderboard``, which this test makes raise.
    """
    app = _start_app(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "wxverify.scoring.leaderboard._live_leaderboard", _raise_live_leaderboard_called
    )
    with TestClient(app) as client:
        db = get_db()
        site_id = db.write_sync(lambda conn: _make_site(conn, "No Input"))

        for _ in range(3):
            resp = client.get(
                "/api/leaderboard",
                params={"site": site_id, "variable": "temperature", "lead": "D+1"},
            )
            assert resp.status_code == 200
            assert resp.json() == []
        for _ in range(3):
            resp = client.get(
                "/api/curve",
                params={"site": site_id, "variable": "temperature", "lead": "D+1"},
            )
            assert resp.status_code == 200

        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 0


def test_missing_or_disabled_site_is_empty_no_enqueue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T2b -- a nonexistent site id and a disabled site that retains
    historical pairs/cache both yield HTTP 200 with ``[]`` and zero jobs,
    proving availability is not confused with applicability.
    """
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed_disabled(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Disabled Leaderboard Site", enabled=0)
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            _seed_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                window_key="w:14",
                computed_at=isoformat_utc(),
                n=1,
                skill_score=0.5,
            )
            return site_id

        disabled_site_id = db.write_sync(_seed_disabled)
        missing_site_id = disabled_site_id + 999

        for site_id in (missing_site_id, disabled_site_id):
            resp = client.get(
                "/api/leaderboard",
                params={"site": site_id, "variable": "temperature", "lead": "D+1"},
            )
            assert resp.status_code == 200
            assert resp.json() == []
            assert db.read_sync(lambda conn, sid=site_id: _job_count(conn, sid)) == 0


def test_applicable_input_absent_cache_is_rebuilding_no_live_recompute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T2c. Fails on current code: an empty cache makes ``_cached_leaderboard``
    return ``None``, so ``leaderboard_with_status`` falls through to
    ``_live_leaderboard``, which this test makes raise.
    """
    app = _start_app(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "wxverify.scoring.leaderboard._live_leaderboard", _raise_live_leaderboard_called
    )
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Rebuilding No Live")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            return site_id

        site_id = db.write_sync(_seed)
        resp = client.get(
            "/api/leaderboard",
            params={"site": site_id, "variable": "temperature", "lead": "D+1"},
        )
        assert resp.status_code == 200
        assert resp.json() == []
        # The rescore enqueue is fire-and-forget (schedule_score_rescore);
        # TestClient.get() does not wait for it, so drain the scheduled task
        # via the running TestClient's portal before checking the jobs table.
        client.portal.call(drain_pending_rescores)
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1


# ---------------------------------------------------------------------------
# T3 -- fresh complete cache -> hit.
# ---------------------------------------------------------------------------


def test_fresh_complete_cache_returns_hit(tmp_path: Path) -> None:
    """T3 (route-level no-enqueue companion lives in T7)."""
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "Fresh Hit")
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
    )
    _seed_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        window_key="w:14",
        computed_at=isoformat_utc(),
        n=2,
        skill_score=0.55,
    )

    result = leaderboard_with_status(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )
    assert result.status == "hit"
    assert len(result.rows) == 1
    assert result.rows[0].feed_id == feed_id


# ---------------------------------------------------------------------------
# T4 -- feed-set mismatch -> rebuilding, never stale-served.
# ---------------------------------------------------------------------------


def test_feed_set_mismatch_is_rebuilding_never_stale_served(tmp_path: Path) -> None:
    """T4 -- a structurally-wrong snapshot (missing an expected active
    competitor) is NEVER served stale; it degrades to `rebuilding`.
    """
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "Feed Mismatch")
    feed_cached, feed_uncached = _open_meteo_feed_ids(conn, 2)
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed_cached, valid_at="2035-01-02T00:00:00Z"
    )
    _seed_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_cached,
        window_key="w:14",
        computed_at="2020-01-01T00:00:00Z",
        n=2,
        skill_score=0.3,
    )
    # A second active competitor gains an in-cutoff pair but no cache row --
    # cached_feed_ids != expected_feed_ids.
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed_uncached, valid_at="2035-01-02T00:00:00Z"
    )

    result = leaderboard_with_status(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )
    assert result.status == "rebuilding"
    assert result.rows == []


# ---------------------------------------------------------------------------
# T5 -- custom (Nd) window is genuinely live.
# ---------------------------------------------------------------------------


def test_custom_window_live_computes_live_no_enqueue(tmp_path: Path) -> None:
    """T5. Do NOT monkeypatch -- assert the live path genuinely computes and
    the result never touches ``score_cache``."""
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    site_id = _make_site(conn, "Custom Window Live")
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
    )

    result = leaderboard_with_status(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="7d"
    )
    assert result.status == "live"
    assert len(result.rows) == 1
    assert result.rows[0].feed_id == feed_id
    assert conn.execute("SELECT COUNT(*) AS n FROM score_cache").fetchone()["n"] == 0


def test_api_leaderboard_custom_window_never_enqueues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T5 route-level companion: live custom windows never touch the rescore
    queue -- contrast positive: the SAME data under ``rolling`` DOES enqueue
    (absent cache -> rebuilding).
    """
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> tuple[int, int]:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Leaderboard Custom Bypass")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            return site_id, feed_id

        site_id, feed_id = db.write_sync(_seed)

        rolling_resp = client.get(
            "/api/leaderboard",
            params={
                "site": site_id,
                "variable": "temperature",
                "lead": "D+1",
                "window": "rolling",
            },
        )
        assert rolling_resp.json() == []
        # The rescore enqueue is fire-and-forget (schedule_score_rescore);
        # TestClient.get() does not wait for it, so drain the scheduled task
        # via the running TestClient's portal before checking the jobs table.
        client.portal.call(drain_pending_rescores)
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1

        custom_resp = client.get(
            "/api/leaderboard",
            params={
                "site": site_id,
                "variable": "temperature",
                "lead": "D+1",
                "window": "3d",
            },
        )
        assert custom_resp.status_code == 200
        custom_rows = custom_resp.json()
        assert any(row["feed_id"] == feed_id for row in custom_rows)
        client.portal.call(drain_pending_rescores)
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1  # unchanged


# ---------------------------------------------------------------------------
# T6 -- the shared terminal-failure enqueue cooldown now guards a
# leaderboard-driven rescore.
# ---------------------------------------------------------------------------


def test_stale_read_enqueue_cooldown_suppresses_then_fires_after_expiry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T6 -- pins §3: the leaderboard-driven enqueue now shares the same
    terminal-failure cooldown as composite. A stale complete cache with a
    RECENT terminally-failed job suppresses the read's rescore enqueue (job
    count stays at 1); the paired positive ages that failure past the
    15-minute cooldown on an otherwise-identical site, where the enqueue
    proceeds (job count becomes 2).
    """
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection, *, name: str, minutes_ago: int) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, name)
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            _seed_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                window_key="w:14",
                computed_at="2020-01-01T00:00:00Z",
                n=1,
                skill_score=0.4,
            )
            now = utc_now()
            _insert_job(
                conn,
                site_id=site_id,
                status="failed",
                created_at=isoformat_utc(now - timedelta(minutes=minutes_ago)),
                updated_at=isoformat_utc(now - timedelta(minutes=minutes_ago)),
            )
            return site_id

        suppressed_site = db.write_sync(
            lambda conn: _seed(conn, name="Cooldown Suppressed", minutes_ago=5)
        )
        expired_site = db.write_sync(
            lambda conn: _seed(conn, name="Cooldown Expired", minutes_ago=20)
        )

        resp_suppressed = client.get(
            "/api/leaderboard",
            params={"site": suppressed_site, "variable": "temperature", "lead": "D+1"},
        )
        assert resp_suppressed.status_code == 200
        assert resp_suppressed.json()  # stale is still served
        # Drain BEFORE the suppression assertion too -- otherwise "stays at
        # 1" is vacuous (indistinguishable from "the enqueue task just
        # hasn't run yet"). Draining first makes the suppression real: the
        # cooldown-guarded enqueue genuinely ran and genuinely no-opped.
        client.portal.call(drain_pending_rescores)
        assert db.read_sync(lambda conn: _job_count(conn, suppressed_site)) == 1

        resp_expired = client.get(
            "/api/leaderboard",
            params={"site": expired_site, "variable": "temperature", "lead": "D+1"},
        )
        assert resp_expired.status_code == 200
        assert resp_expired.json()
        client.portal.call(drain_pending_rescores)
        assert db.read_sync(lambda conn: _job_count(conn, expired_site)) == 2


# ---------------------------------------------------------------------------
# T7 -- HTTP route coverage (TestClient).
# ---------------------------------------------------------------------------


def test_api_leaderboard_stale_cache_enqueues_exactly_once_across_polls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Leaderboard Stale Dedup")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            _seed_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                window_key="w:14",
                computed_at="2020-01-01T00:00:00Z",
                n=1,
                skill_score=0.4,
            )
            return site_id

        site_id = db.write_sync(_seed)
        first = client.get(
            "/api/leaderboard",
            params={"site": site_id, "variable": "temperature", "lead": "D+1"},
        )
        assert first.status_code == 200
        assert first.json()  # stale is served, never recomputed to empty
        # Drain after the FIRST read so the dedup check below (second read
        # still == 1) is comparing against a settled count, not racing the
        # first read's own still-in-flight enqueue task.
        client.portal.call(drain_pending_rescores)
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1
        second = client.get(
            "/api/leaderboard",
            params={"site": site_id, "variable": "temperature", "lead": "D+1"},
        )
        assert second.status_code == 200
        assert second.json()
        client.portal.call(drain_pending_rescores)
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1


def test_api_leaderboard_fresh_cache_enqueues_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> tuple[int, int]:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Leaderboard Fresh Hit")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            _seed_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                window_key="w:14",
                computed_at=isoformat_utc(),
                n=1,
                skill_score=0.5,
            )
            return site_id, feed_id

        site_id, feed_id = db.write_sync(_seed)
        resp = client.get(
            "/api/leaderboard",
            params={"site": site_id, "variable": "temperature", "lead": "D+1"},
        )
        assert resp.status_code == 200
        rows = resp.json()
        assert any(row["feed_id"] == feed_id for row in rows)
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 0


def test_leaderboard_read_functions_never_write_on_read_connection(
    tmp_path: Path,
) -> None:
    """Mirrors composite's write-through-read guard: ``leaderboard_with_status``
    must never attempt a write while resolving a stale snapshot. Real
    enforcement via ``PRAGMA query_only=ON`` -- SQLite itself raises
    ``sqlite3.OperationalError`` on any attempted INSERT/UPDATE/DELETE, so
    returning normally (and still reporting ``stale``) is the only way this
    test passes if a hidden write exists.
    """
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "Leaderboard Write Guard")
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    _add_temperature_cell(
        conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
    )
    _seed_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        window_key="w:14",
        computed_at="2020-01-01T00:00:00Z",
        n=1,
        skill_score=0.4,
    )

    conn.execute("PRAGMA query_only=ON")

    result = leaderboard_with_status(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )
    assert result.status == "stale"
    assert len(result.rows) == 1


def test_api_curve_enqueues_once_for_the_stale_lead_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T7 curve coverage: one lead (D+1) is stale, every other lead (D+0,
    D+2..D+7) has no forecast pairs at all -- genuinely ``empty``. The curve
    route's ``any(status in stale/rebuilding)`` gate must fire exactly once
    for the stale lead; the empty leads must not turn an inapplicable curve
    into a rebuild request of their own.
    """
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Curve Cross Lead")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                valid_at="2035-01-02T00:00:00Z",
                day_ahead=1,
            )
            _seed_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                window_key="w:14",
                computed_at="2020-01-01T00:00:00Z",
                n=1,
                skill_score=0.4,
            )
            return site_id

        site_id = db.write_sync(_seed)
        resp = client.get(
            "/api/curve", params={"site": site_id, "variable": "temperature"}
        )
        assert resp.status_code == 200
        client.portal.call(drain_pending_rescores)
        assert db.read_sync(lambda conn: _job_count(conn, site_id)) == 1


# ---------------------------------------------------------------------------
# T8 -- enqueue failure is best-effort, never fails the read.
# NOT part of the failing-test-first set: both sub-tests monkeypatch a
# not-yet-existing ``enqueue_score_rescore`` symbol (the §3.1 rename target)
# and are expected to fail with AttributeError until §3 lands.
# ---------------------------------------------------------------------------


def test_api_leaderboard_enqueue_failure_is_best_effort_still_returns_200(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T8 API route sub-test. Post-§2/§3, ``/api/leaderboard`` no longer calls
    ``enqueue_score_rescore`` directly -- it calls the synchronous
    ``schedule_score_rescore``, which schedules a fire-and-forget task that
    reaches ``enqueue_score_rescore`` via a further ``db.write`` hop inside
    ``wxverify.scoring.rescore``. Monkeypatch that single new seam (the
    routes no longer import the symbol at all) and drain the scheduled task
    via the running ``TestClient``'s portal before inspecting the spy --
    ``TestClient.get()`` does not wait for background tasks.
    """
    app = _start_app(tmp_path, monkeypatch)
    calls: list[int] = []

    def _raise(conn: sqlite3.Connection, site_id: int) -> None:
        calls.append(site_id)
        raise RuntimeError("simulated enqueue failure")

    monkeypatch.setattr("wxverify.scoring.rescore.enqueue_score_rescore", _raise)

    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Enqueue Failure Best Effort")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            _seed_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                window_key="w:14",
                computed_at="2020-01-01T00:00:00Z",
                n=1,
                skill_score=0.4,
            )
            return site_id

        site_id = db.write_sync(_seed)
        resp = client.get(
            "/api/leaderboard",
            params={"site": site_id, "variable": "temperature", "lead": "D+1"},
        )
        assert resp.status_code == 200
        assert resp.json()  # stale rows still served despite the enqueue raising
        client.portal.call(drain_pending_rescores)
        assert calls == [site_id]  # non-vacuity: the raising patch was reached


def test_dashboard_html_enqueue_failure_is_best_effort_still_renders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T8 dashboard HTML sub-test. Same best-effort contract at the
    ``web/routes.py`` enqueue site (``schedule_score_rescore`` call). The
    fixture must yield ``composite_status in ("stale", "rebuilding")`` --
    that is the gate at that call site, so a fresh/empty composite would
    never reach the wrapped call. Monkeypatches the single new
    ``wxverify.scoring.rescore.enqueue_score_rescore`` seam (the route no
    longer imports the symbol) and drains the scheduled task via the running
    ``TestClient``'s portal before inspecting the spy.
    """
    app = _start_app(tmp_path, monkeypatch)
    calls: list[int] = []

    def _raise(conn: sqlite3.Connection, site_id: int) -> None:
        calls.append(site_id)
        raise RuntimeError("simulated enqueue failure")

    monkeypatch.setattr("wxverify.scoring.rescore.enqueue_score_rescore", _raise)

    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Dashboard Enqueue Failure")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            _add_temperature_cell(
                conn, site_id=site_id, feed_id=feed_id, valid_at="2035-01-02T00:00:00Z"
            )
            _seed_score_cache(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                window_key="w:14",
                computed_at="2020-01-01T00:00:00Z",
                n=1,
                skill_score=0.4,
            )
            return site_id

        site_id = db.write_sync(_seed)
        resp = client.get("/dashboard", params={"site": site_id, "window": "rolling"})
        assert resp.status_code == 200
        assert "<html" in resp.text.lower()  # full page rendered despite the failure
        client.portal.call(drain_pending_rescores)
        assert calls == [site_id]  # non-vacuity: the raising patch was reached
