"""Composite lead-coverage disclosure oracles — 0.12.0 §15 family 9 (§9, W6).

Covers ``CompositeParts.lead_counts`` in ``wxverify.scoring.composite``: the
per-variable count of ``day_ahead`` cells that entered a feed's composite
mean, populated identically in both the cached fold
(``composite_with_status``) and the live fold (``_live_composite``); the
per-cell ``min_n`` exclusion (a below-floor cell drops out of
``lead_counts`` exactly as it drops out of ``components``); and the
rendered ``/dashboard`` page's ``Leads`` column. §9's acceptance criterion
is the thing every oracle here is built to protect: **no oracle asserts a
change in ``score`` or ``raw_score``** -- W6 is additive disclosure, and a
moved score would be the fingerprint of an accidental estimator change, not
evidence this family should produce.

Fixture-construction helpers (``_add_continuous_pair``, ``_add_precip_pair``,
``_persistence_feed_id``, ``_open_meteo_feed_ids``) are reused from
``tests/test_composite_cache_backed.py`` rather than re-derived -- same
synthetic skill-score-to-error-value math, same real-datastore
(``score_cache`` + ``forecast_pairs``) construction discipline.

Isolation: ``tests/test_composite_cache_backed.py``'s ``_init_tmp_db`` /
``_start_app`` harness (a fresh ``tmp_path``-backed SQLite file per test).

Synthetic data only: the shared 40.0/-105.0 lat-lon convention, invented
site names.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.test_composite_cache_backed import (
    _add_continuous_pair,  # noqa: SLF001
    _add_precip_pair,  # noqa: SLF001
    _init_tmp_db,  # noqa: SLF001
    _make_site,  # noqa: SLF001
    _open_meteo_feed_ids,  # noqa: SLF001
    _persistence_feed_id,  # noqa: SLF001
    _start_app,  # noqa: SLF001
)
from wxverify.db.connection import get_db
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.composite import (
    MAX_DAY_AHEAD,
    _expected_active_cells,  # noqa: SLF001
    _live_composite,  # noqa: SLF001
    composite_with_status,
)
from wxverify.scoring.leaderboard import resolve_window
from wxverify.scoring.metrics import strategy_for
from wxverify.settings.keys import get_number_setting, set_setting


def _seed_and_cache_all_expected(
    conn: sqlite3.Connection, *, site_id: int, window: str = "rolling"
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Compute every expected cell live, upsert it into ``score_cache``
    (mirrors ``test_cached_composite_matches_live_composite_rolling_window``),
    then return (cached rows, live rows) for the same fixture."""
    resolved = resolve_window(conn, window)
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    expected_cells = _expected_active_cells(conn, site_id=site_id, resolved=resolved)
    for feed_id, variable, day_ahead in expected_cells:
        result = strategy_for(variable).aggregate(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            day_ahead=day_ahead,
            window_cutoff=resolved.cutoff,
            min_n=min_n,
        )
        upsert_score_cache(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            day_ahead=day_ahead,
            window_key=resolved.window_key,
            result=result,
            computed_at="2035-06-01T00:00:00Z",
        )
    cached_result = composite_with_status(conn, site_id=site_id, window=window)
    live_rows = _live_composite(
        conn, site_id=site_id, window_key=resolved.window_key, cutoff=resolved.cutoff
    )
    return cached_result.rows, live_rows


# ---------------------------------------------------------------------------
# Exact lead_counts shape + cached/live parity + bit-identical score.
# ---------------------------------------------------------------------------


def test_lead_counts_exact_shape_and_cached_live_parity(tmp_path: Path) -> None:
    """§9's worked example: a feed with pairs in three day_ahead cells for
    temperature and one for precip reports {"temperature": 3, "precip": 1}
    and lead_cells_max == 3 -- identically from both folds, with a
    bit-identical score/raw_score (== equality, not approx: both folds
    compute the SAME sum-of-floats over the SAME components dict)."""
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "1")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "Lead Disclosure Shape")
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    persistence_id = _persistence_feed_id(conn)

    for day_ahead, valid_hour in ((1, 0), (2, 1), (3, 2)):
        _add_continuous_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            persistence_feed_id=persistence_id,
            variable="temperature",
            skill_score=0.5,
            day_ahead=day_ahead,
            valid_hour=valid_hour,
        )
    _add_precip_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        day_ahead=1,
        valid_hour=3,
        cat_hit=1,
        cat_false=0,
        cat_miss=0,
        cat_correct_neg=1,
    )

    cached_rows, live_rows = _seed_and_cache_all_expected(conn, site_id=site_id)
    cached_row = next(r for r in cached_rows if r["feed_id"] == feed_id)
    live_row = next(r for r in live_rows if r["feed_id"] == feed_id)

    assert cached_row["lead_counts"] == {"temperature": 3, "precip": 1}
    assert cached_row["lead_cells_max"] == 3
    assert cached_row["lead_cells_total"] == MAX_DAY_AHEAD + 1

    # Cached/live parity: identical lead_counts, lead_cells_max.
    assert live_row["lead_counts"] == cached_row["lead_counts"]
    assert live_row["lead_cells_max"] == cached_row["lead_cells_max"]
    assert live_row["lead_cells_total"] == cached_row["lead_cells_total"]

    # §9 acceptance: score and raw_score bit-identical across both folds.
    assert live_row["score"] == cached_row["score"]
    assert live_row["raw_score"] == cached_row["raw_score"]


# ---------------------------------------------------------------------------
# min_n exclusion parity: a below-floor cell drops from lead_counts exactly
# as it drops from components.
# ---------------------------------------------------------------------------


def test_below_floor_cell_excluded_from_lead_counts_like_components(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    set_setting(conn, "min_n", "2")
    set_setting(conn, "rolling_window_days", "14")
    site_id = _make_site(conn, "Lead Disclosure Floor")
    feed_id = _open_meteo_feed_ids(conn, 1)[0]
    persistence_id = _persistence_feed_id(conn)

    # temperature: TWO pairs at the same cell -> n=2 meets the floor.
    for valid_hour in (0, 1):
        _add_continuous_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            persistence_feed_id=persistence_id,
            variable="temperature",
            skill_score=0.5,
            day_ahead=1,
            valid_hour=valid_hour,
        )
    # wind: ONE pair only -> n=1 < min_n=2, below the floor.
    _add_continuous_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        persistence_feed_id=persistence_id,
        variable="wind",
        skill_score=0.3,
        day_ahead=1,
        valid_hour=2,
    )

    cached_rows, live_rows = _seed_and_cache_all_expected(conn, site_id=site_id)
    cached_row = next(r for r in cached_rows if r["feed_id"] == feed_id)
    live_row = next(r for r in live_rows if r["feed_id"] == feed_id)

    for row in (cached_row, live_row):
        assert "wind" not in row["lead_counts"]  # type: ignore[operator]
        assert "wind" not in row["components"]  # type: ignore[operator]
        assert row["lead_counts"] == {"temperature": 1}
        assert "temperature" in row["components"]  # type: ignore[operator]

    assert live_row["score"] == cached_row["score"]
    assert live_row["raw_score"] == cached_row["raw_score"]


# ---------------------------------------------------------------------------
# Rendered-page oracle: the /dashboard Leads column.
# ---------------------------------------------------------------------------


def test_dashboard_page_renders_leads_column_header_and_max_over_total(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing template global or an undefined Jinja2 name is a
    render-time failure a unit test on the data function cannot catch --
    only a request-level assertion does."""
    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        db = get_db()

        def _seed(conn: sqlite3.Connection) -> int:
            set_setting(conn, "min_n", "1")
            set_setting(conn, "rolling_window_days", "14")
            site_id = _make_site(conn, "Dashboard Leads Column")
            feed_id = _open_meteo_feed_ids(conn, 1)[0]
            persistence_id = _persistence_feed_id(conn)
            for day_ahead, valid_hour in ((1, 0), (2, 1)):
                _add_continuous_pair(
                    conn,
                    site_id=site_id,
                    feed_id=feed_id,
                    persistence_feed_id=persistence_id,
                    variable="temperature",
                    skill_score=0.5,
                    day_ahead=day_ahead,
                    valid_hour=valid_hour,
                )
            resolved = resolve_window(conn, "rolling")
            min_n = get_number_setting(conn, "min_n", 30, minimum=0)
            expected_cells = _expected_active_cells(
                conn, site_id=site_id, resolved=resolved
            )
            for cell_feed_id, variable, day_ahead in expected_cells:
                result = strategy_for(variable).aggregate(
                    conn,
                    site_id=site_id,
                    feed_id=cell_feed_id,
                    variable=variable,
                    day_ahead=day_ahead,
                    window_cutoff=resolved.cutoff,
                    min_n=min_n,
                )
                upsert_score_cache(
                    conn,
                    site_id=site_id,
                    feed_id=cell_feed_id,
                    variable=variable,
                    day_ahead=day_ahead,
                    window_key=resolved.window_key,
                    result=result,
                    computed_at="2035-06-01T00:00:00Z",
                )
            return site_id

        site_id = db.write_sync(_seed)
        response = client.get(
            "/dashboard", params={"site": site_id, "window": "rolling"}
        )
        assert response.status_code == 200
        body = response.text
        assert "Leads" in body
        assert f"2 / {MAX_DAY_AHEAD + 1}" in body
