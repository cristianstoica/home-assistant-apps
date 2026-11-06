"""Tests for the ``forecast_blend_depth`` option's touch points.

Covers the option's 6 touch points
(config.yaml + schema, ``RuntimeOptions`` Field, ``_from_options_json``,
``_from_env``, guarded ``apply_plain_settings`` write, resolved read at the
selection consuming site).

Touch point coverage map:
  1. config.yaml options: + schema        -> NOT re-pinned here; the existing
     generic ``test_translations_key_parity`` in test_m1_m5.py is a full
     set-equality check across ALL option keys and already covers
     ``forecast_blend_depth`` (it's present in both config.yaml and
     translations/en.yaml on the production side already) -- a second,
     narrower pin here would be a near-duplicate of an already-exhaustive
     check, not new coverage.
  2. RuntimeOptions Field(ge=1, le=6)      -> test_pydantic_* below.
  3. _from_options_json                    -> covered by the same
     RuntimeOptions construction path (_from_options_json just forwards
     `options.get("forecast_blend_depth")` into the Field) -- not
     separately re-tested; see test_pydantic_accepts_boundary_values.
  4. _from_env                             -> test_from_env_* below.
  5. apply_plain_settings guard            -> test_apply_plain_settings_* below.
  6. Resolved value reaches selection      -> test_end_to_end_* below (via
     build_forecast, the real consuming site: get_number_setting call at
     wxverify/forecast/service.py:157).
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import TypeVar

import pytest
from pydantic import ValidationError

from tests.helpers import seed_active_publication, seed_publication_ranking
from wxverify.core.options import RuntimeOptions, _from_env
from wxverify.db.migrations import run_migrations
from wxverify.forecast.service import build_forecast, build_hourly
from wxverify.scoring.metrics import MetricSums, metrics_from_sums
from wxverify.settings.keys import get_number_setting, get_setting, set_setting
from wxverify.settings.service import apply_plain_settings

T = TypeVar("T")


class _RealDb:
    """DB shim wrapping one :memory: sqlite3 connection; write()/read() call
    the lambda synchronously (mirrors tests/test_sm4_backoff.py)."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    async def write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return fn(self._conn)

    async def read(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return fn(self._conn)


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    conn.execute(
        """
        INSERT INTO sites (id, name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (1, 'Test Site', 40.0, -105.0, 900.0, 'UTC')
        """
    )
    return conn


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
    ).fetchone()
    assert row is not None, f"seed feed not found: {source}/{model}"
    return int(row["id"])


# ---------------------------------------------------------------------------
# Touch point 2: RuntimeOptions Field(ge=1, le=6) -- a hard floor/ceiling,
# not a silent clamp.
# ---------------------------------------------------------------------------


def test_pydantic_rejects_below_floor_and_above_ceiling() -> None:
    with pytest.raises(ValidationError):
        RuntimeOptions(forecast_blend_depth=0)
    with pytest.raises(ValidationError):
        RuntimeOptions(forecast_blend_depth=7)


def test_pydantic_accepts_boundary_values() -> None:
    assert RuntimeOptions(forecast_blend_depth=1).forecast_blend_depth == 1
    assert RuntimeOptions(forecast_blend_depth=6).forecast_blend_depth == 6


def test_default_is_none() -> None:
    assert RuntimeOptions().forecast_blend_depth is None


# ---------------------------------------------------------------------------
# Touch point 4: _from_env.
# ---------------------------------------------------------------------------


def test_from_env_absent_is_none_present_parses_int(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WXV_FORECAST_BLEND_DEPTH", raising=False)
    assert _from_env().options.forecast_blend_depth is None

    # Paired positive: same call path, env var present -> parsed, not just
    # "happens to default to None regardless of the parse logic".
    monkeypatch.setenv("WXV_FORECAST_BLEND_DEPTH", "3")
    assert _from_env().options.forecast_blend_depth == 3


# ---------------------------------------------------------------------------
# Touch point 5: apply_plain_settings guard -- None leaves the DB value
# untouched (proven against a pre-seeded non-default value, not just
# "absent stays absent"); an explicit value overwrites it.
# ---------------------------------------------------------------------------


def test_apply_plain_settings_none_leaves_existing_value_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _make_db()
    set_setting(conn, "forecast_blend_depth", "5")
    db = _RealDb(conn)
    monkeypatch.setattr("wxverify.settings.service.get_db", lambda: db)

    asyncio.run(apply_plain_settings(RuntimeOptions()))
    assert get_setting(conn, "forecast_blend_depth") == "5"


def test_apply_plain_settings_explicit_value_overwrites(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _make_db()
    set_setting(conn, "forecast_blend_depth", "5")
    db = _RealDb(conn)
    monkeypatch.setattr("wxverify.settings.service.get_db", lambda: db)

    asyncio.run(apply_plain_settings(RuntimeOptions(forecast_blend_depth=4)))
    assert get_setting(conn, "forecast_blend_depth") == "4"


# ---------------------------------------------------------------------------
# Touch point 6: resolved value reaches selection (build_forecast, the real
# consuming site).
# ---------------------------------------------------------------------------


def _seed_two_confident_feeds(conn: sqlite3.Connection) -> None:
    set_setting(conn, "min_n", "3")
    ecmwf_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    gfs_id = _feed_id(conn, "open-meteo", "gfs_global")
    valid_ats = [f"2026-07-20T{h:02d}:00:00Z" for h in range(4, 24)]

    for feed_id, value in ((ecmwf_id, 11.0), (gfs_id, 12.0)):
        for i, valid_at in enumerate(valid_ats):
            conn.execute(
                """
                INSERT INTO forecast_samples
                    (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                     value, source_raw, model_run_id, fetched_at)
                VALUES (1, ?, 'temperature', ?, ?, ?, ?, '{}', 'run-1', ?)
                """,
                (
                    feed_id,
                    "2026-07-19T20:00:00Z",
                    valid_at,
                    i + 1,
                    value,
                    "2026-07-19T20:00:00Z",
                ),
            )

    # The three feeds' far-horizon (temperature, day_ahead=1) skill numbers
    # that used to be written here as forecast_pairs rows are now
    # hand-derived directly in `_seed_score_cache_for_fixture`'s published
    # ranking (no pair table is read for scoring any more, W6b, storage
    # release 3.1).


_TEST_PUBLICATION_WINDOW_DAYS = 30
_TEST_PUBLICATION_WINDOW_KEY = f"w:{_TEST_PUBLICATION_WINDOW_DAYS}"


def _ensure_test_publication(conn: sqlite3.Connection, *, min_n: int) -> int:
    """This connection's one site-1 publication, created on first use.

    ``leaderboard_with_status``'s ``rolling`` window now always reads the
    active publication (9.2), never a live recompute. Mirrors
    ``tests/test_forecast_service.py``'s identically-named helper.
    """
    row = conn.execute(
        "SELECT id FROM publications WHERE site_id = 1 ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row is not None:
        return int(row["id"])
    return seed_active_publication(
        conn,
        site_id=1,
        cutoff=date(2035, 6, 30),
        published_at=datetime(2035, 6, 30, tzinfo=UTC),
        window_days=_TEST_PUBLICATION_WINDOW_DAYS,
        min_n=min_n,
    )


def _seed_score_cache_for_fixture(conn: sqlite3.Connection) -> None:
    """Seed a fresh, complete published-ranking snapshot for the
    (temperature, day_ahead=1, w:30) cell `_seed_two_confident_feeds`
    populates.

    It publishes a ranking for THREE feeds -- the two competitors plus
    ``virtual/_persistence`` -- and `active_competitor_clause` counts virtual
    feeds unconditionally, so the expected feed universe for the published
    leaderboard lookup is all three; a cell missing persistence would fall
    through to 'rebuilding'-empty rather than the confident-feed selection
    this test exercises.

    The pair-aggregate SQL that used to compute these sums is gone (W6b);
    the sums below are hand-derived from `_seed_two_confident_feeds`'s 3
    far-horizon rows per feed (forecast/observed=10.0, persistence
    forecast=8.0), fed through the surviving pure `metrics_from_sums`:
    ecmwf (forecast 10.5): error 0.5 x3 -> err_sum=1.5, abs_sum=1.5,
    sq_sum=0.75; gfs (forecast 9.5): error -0.5 x3 -> err_sum=-1.5,
    abs_sum=1.5, sq_sum=0.75 -- both skill-paired against persistence's
    sq_error 4.0 x3 (skill_ref_sq_sum=12.0), giving skill 1-0.25/4=0.9375
    for both, confident at n=3 >= min_n=3. persistence's own row reuses its
    error (-2.0 x3) against itself as the reference, giving skill 0.0 --
    only its presence (not its value) matters for the fixture's comment
    above.
    """
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    pub_id = _ensure_test_publication(conn, min_n=min_n)
    competitor_sums: MetricSums = {
        "n": 3,
        "err_n": 3,
        "err_sum": 1.5,
        "abs_sum": 1.5,
        "sq_sum": 0.75,
        "skill_n": 3,
        "skill_sq_sum": 0.75,
        "skill_ref_sq_sum": 12.0,
    }
    persistence_sums: MetricSums = {
        "n": 3,
        "err_n": 3,
        "err_sum": -6.0,
        "abs_sum": 6.0,
        "sq_sum": 12.0,
        "skill_n": 3,
        "skill_sq_sum": 12.0,
        "skill_ref_sq_sum": 12.0,
    }
    for source, model, sums in (
        ("open-meteo", "ecmwf_ifs", competitor_sums),
        ("open-meteo", "gfs_global", competitor_sums),
        ("virtual", "_persistence", persistence_sums),
    ):
        feed_id = _feed_id(conn, source, model)
        result = metrics_from_sums(sums, variable="temperature", min_n=min_n)
        seed_publication_ranking(
            conn,
            publication_id=pub_id,
            feed_id=feed_id,
            variable="temperature",
            day_ahead=1,
            window_key=_TEST_PUBLICATION_WINDOW_KEY,
            result=result,
        )


def test_end_to_end_default_blends_two_explicit_setting_narrows_to_one() -> None:
    """Touch point 6: the resolved ``forecast_blend_depth`` setting reaches
    the BLEND-set selection. `_seed_two_confident_feeds` covers hours 04-23
    (20 hours) -- short of the complete local day Item F's extrema
    eligibility rule requires -- so for temperature `tile.temp.meta.feeds`
    now names the (empty) extrema set regardless of blend depth, not the
    depth-narrowed blend set this test is pinning. Read the blend set from
    ``build_hourly``'s per-feed series instead (drawn from
    ``selections[variable].feeds``, unaffected by extrema filtering) --
    the same substitution already used for the far-horizon suppression
    tests in test_forecast_service.py."""
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)

    conn_default = _make_db()
    _seed_two_confident_feeds(conn_default)
    _seed_score_cache_for_fixture(conn_default)
    conn_default.commit()
    view_default = build_forecast(
        conn_default, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    payload_default = build_hourly(
        conn_default, site_id=1, timezone="UTC", day=0, now=now
    )
    feeds_default = payload_default["feeds"]
    assert isinstance(feeds_default, list)
    assert len(feeds_default) == 2  # default depth = 2
    assert view_default.tiles[0].temp.meta.state != "low_confidence"

    conn_narrow = _make_db()
    _seed_two_confident_feeds(conn_narrow)
    _seed_score_cache_for_fixture(conn_narrow)
    set_setting(conn_narrow, "forecast_blend_depth", "1")
    conn_narrow.commit()
    view_narrow = build_forecast(
        conn_narrow, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    payload_narrow = build_hourly(
        conn_narrow, site_id=1, timezone="UTC", day=0, now=now
    )
    feeds_narrow = payload_narrow["feeds"]
    assert isinstance(feeds_narrow, list)
    assert len(feeds_narrow) == 1
    assert view_narrow.tiles[0].temp.meta.state != "low_confidence"
