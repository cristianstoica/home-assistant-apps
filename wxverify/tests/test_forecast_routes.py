"""HTTP-level tests for the Forecast home page routes.

Covers: "/" returns 200, "/forecast/tiles"
returns 204 vs a fragment depending on the fingerprint, "/api/forecast/hourly"
404s for an unknown site and clamps an out-of-range day, and the exact
empty-state copy renders when a site has no forecast data yet.

Isolation: a real tmp-file SQLite DB via ``init_db``/``close_db`` + an idle
worker + ``TestClient`` (mirrors ``tests/test_web_ui.py``'s harness) — a
tmp-file DB, not ``:memory:``, because the app's WAL-mode reads come from a
SEPARATE pooled connection than whatever writes the fixture rows; a real
file is required for the read side to see the write side's committed data.

Synthetic fixtures only — fake site name/coords.
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
from wxverify.core.timeutil import floor_hour, isoformat_utc, utc_now
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.forecast.service import build_forecast, build_hourly
from wxverify.settings.keys import set_setting

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_web_ui.py / tests/test_static_ingress.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _make_site(
    conn: sqlite3.Connection, name: str = "Test Site", *, timezone: str = "UTC"
) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, ?, 1)
            """,
            (name, timezone),
        ).lastrowid
    )


# ---------------------------------------------------------------------------
# "/" and "/forecast" — 200 with the Forecast heading.
# ---------------------------------------------------------------------------


def test_root_returns_200_with_forecast_heading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/")
    assert response.status_code == 200
    assert "<h1>Forecast</h1>" in response.text


def test_forecast_route_returns_200_with_forecast_heading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/forecast")
    assert response.status_code == 200
    assert "<h1>Forecast</h1>" in response.text


def test_root_with_no_sites_shows_no_sites_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)  # no site inserted
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/")
    assert response.status_code == 200
    assert "No sites configured." in response.text


# ---------------------------------------------------------------------------
# Exact empty-state copy.
# ---------------------------------------------------------------------------


def test_empty_forecast_shows_exact_still_collecting_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn)  # site exists but has zero forecast_samples
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/forecast")
    assert response.status_code == 200
    assert "Still collecting forecasts — check back shortly." in response.text


# ---------------------------------------------------------------------------
# /forecast/tiles — 204 (no swap) vs a re-rendered fragment, keyed on
# whether the caller's fingerprint matches the current one.
# ---------------------------------------------------------------------------


def test_forecast_tiles_204_when_fingerprint_matches_fragment_when_it_differs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    # Zero samples, no stamps, no active publication -> "0-p0" (the daily
    # close is permanently on from the build, storage release 3.2, so the
    # token always carries the "-p<id>" publication suffix; "0" for none).
    site_id = _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        stale = client.get(f"/forecast/tiles?site={site_id}&fingerprint=")
        fresh = client.get(f"/forecast/tiles?site={site_id}&fingerprint=0-p0")

    assert stale.status_code == 200
    assert 'id="forecast-tiles"' in stale.text
    assert fresh.status_code == 204
    assert fresh.text == ""


def test_forecast_tiles_unknown_site_yields_200_not_found_region(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Renamed from "...yields_204": an unknown site now answers 200 with
    the "Site not found." region, which carries no ``hx-get`` so the
    auto-poll stops instead of looping against a site that no longer
    exists."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/forecast/tiles?site=999999&fingerprint=")
    assert response.status_code == 200
    assert "Site not found." in response.text
    assert "hx-get" not in response.text


# ---------------------------------------------------------------------------
# /api/forecast/hourly — 404 for an unknown site; day clamps into [0, 7]
# rather than erroring.
# ---------------------------------------------------------------------------


def test_api_forecast_hourly_404s_for_unknown_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/api/forecast/hourly?site=999999")
    assert response.status_code == 404


def test_api_forecast_hourly_day_clamps_above_and_below_range(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        above = client.get(f"/api/forecast/hourly?site={site_id}&day=99")
        below = client.get(f"/api/forecast/hourly?site={site_id}&day=-5")

    assert above.status_code == 200
    assert above.json()["day"] == 7
    assert below.status_code == 200
    assert below.json()["day"] == 0


def test_api_forecast_hourly_in_range_day_passes_through_unclamped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Paired positive for the clamp test above: an in-range day is NOT
    # coerced to an endpoint, proving the clamp is a min/max, not an
    # unconditional override.
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/api/forecast/hourly?site={site_id}&day=3")
    assert response.status_code == 200
    assert response.json()["day"] == 3


# ---------------------------------------------------------------------------
# /forecast/day — day clamps the SAME way, reflected in the embedded chart
# data-src URL the client-side JS reads.
# ---------------------------------------------------------------------------


def test_forecast_day_clamps_and_embeds_clamped_day_in_poll_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Renamed from "...embeds_clamped_day_in_chart_src": card 7 removed
    the per-day hourly chart /forecast/day used to embed, so the clamp is
    now pinned through the region's own auto-poll URL, which always
    carries the day it was built for (``&day=<n>``), rather than through a
    chart data-src that no longer exists."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast/day?site={site_id}&day=99")
    assert response.status_code == 200
    # Jinja autoescapes the literal `&` in the query string to `&amp;`.
    assert f"/forecast/tiles?site={site_id}&amp;fingerprint=0-p0&amp;day=7" in (
        response.text
    )
    assert "day=99" not in response.text


# ---------------------------------------------------------------------------
# New Forecast degradation test (deliberate no-score_cache case): pins the
# degrade-gracefully behaviour. This is the intentional counterpart to the score_cache
# seeding migration required elsewhere -- a rebuilding-empty ranking must
# degrade gracefully (rebuilding, never a crash or a silent stale
# "normal"), and only this test may run Forecast against an unseeded cache.
# ---------------------------------------------------------------------------


def test_forecast_degrades_to_rebuilding_without_score_cache_no_enqueue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """future_samples exist, but no publication is ever seeded. With no
    active publication, `leaderboard_with_status`'s verdict is `empty`, so
    every candidate is unconfident/unscored; the daily close being
    permanent means `_state_of` can no longer read this as `rebuilding` --
    that state required a `rebuilding` leaderboard status, which no
    longer exists -- so it reads `low_confidence` instead. No pair table
    or live recompute is read for scoring any more (storage release, table
    1.B), so an absent publication is the only thing that can produce this
    state; the only counterfactual worth keeping is the one already
    covered elsewhere (a seeded publication ranks this feed above
    persistence and yields `normal`).

    Asserts against OBSERVABLE output surfaces only -- `confident` /
    `skill_score` / `pair_n` are candidate-level fields the rendered views
    do not expose, so asserting them on view output would be a false
    oracle.

    Future samples are anchored to TOMORROW relative to the real wall
    clock (not a fixed calendar date) so the SAME fixture populates day
    tile 1 both for the direct `build_forecast`/`build_hourly` calls below
    (captured `now`, passed explicitly) and for the HTTP round trip (which
    has no `now` override and always reads the real clock) -- the two must
    exercise the identical degraded-ranking scenario, not a coincidentally
    already-empty day.

    The no-enqueue assertion is HTTP-level: a `rebuilding`-empty ranking
    must degrade gracefully with NO Forecast-side rescore enqueue -- a
    direct `build_forecast` call is a pure read, so a function-level
    zero-job assertion would be vacuous.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Forecast Degradation")
    set_setting(conn, "min_n", "1")
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' AND model='ecmwf_ifs'"
        ).fetchone()["id"]
    )

    now = utc_now()
    tomorrow = now.date() + timedelta(days=1)
    issued_at = isoformat_utc(floor_hour(now))
    for h in range(24):
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'temperature', ?, ?, ?, 11.0, '{}', 'run-1', ?)
            """,
            (
                site_id,
                feed_id,
                issued_at,
                f"{tomorrow.isoformat()}T{h:02d}:00:00Z",
                h + 1,
                issued_at,
            ),
        )
    # Deliberately NO publication seeded -- the point of this fixture.

    view = build_forecast(
        conn, site_id=site_id, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[1].temp.meta.state == "low_confidence"
    assert any(ref.feed_id == feed_id for ref in view.tiles[1].temp.meta.feeds)

    hourly = build_hourly(conn, site_id=site_id, timezone="UTC", day=1, now=now)
    assert hourly["states"]["temperature"] == "low_confidence"

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        tiles_resp = client.get(f"/forecast/tiles?site={site_id}&fingerprint=")
        assert tiles_resp.status_code == 200
        # O5: the rendered tile surface carries the warning that production
        # now returns with no publication -- and NEVER the old "ranking
        # updating" wording, retired with the per-cell "rebuilding"
        # leaderboard status it depended on. Card 7 dropped the per-tile
        # "state-<state>" CSS class, so the low-confidence signal is now
        # observable only through the day header's warning tooltip text
        # (day_warnings in forecast/page.py).
        rendered = " ".join(tiles_resp.text.split())
        assert "Low confidence" in rendered
        assert "Ranking updating" not in rendered

        hourly_resp = client.get(f"/api/forecast/hourly?site={site_id}&day=1")
        assert hourly_resp.status_code == 200
        assert hourly_resp.json()["states"]["temperature"] == "low_confidence"

        job_count = get_db().read_sync(
            lambda c: c.execute(
                "SELECT COUNT(*) AS n FROM jobs"
                " WHERE type='pair_and_score' AND site_id=?",
                (site_id,),
            ).fetchone()["n"]
        )
        assert job_count == 0


# ---------------------------------------------------------------------------
# O9: the no-clearing branch -- the agreement counterpart to O8
# (test_coverage_guard_splits_tile_and_drill_down_rebuilding_state in
# tests/test_forecast_service.py). A sibling of the fixture above, not an
# edit to it (that fixture's 24-hour coverage is load-bearing there): the
# hourly sample count is reduced to between 12 and 17, so the single
# selected feed no longer clears the coverage guard. `clearing_subset` then
# falls back to the full selection and reports `partial`, so the tile's
# STATE and the drill-down's coincide -- and the two surfaces must agree on
# `state`. (`clearing_subset` narrows only what wind aggregates and renders;
# for temperature and precipitation `meta.feeds` names the extrema set --
# feeds covering the whole local day for temperature, feeds supplying each
# of its hours exactly once for precipitation. The test seeds temperature
# only, so none of that is asserted here.)
# ---------------------------------------------------------------------------


def test_forecast_partial_coverage_rebuilding_agrees_across_both_surfaces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same no-publication scenario as the test above, but the feed's
    future samples cover only 14 hours -- clears the 12-hour selection
    floor but NOT the 18-hour coverage guard. `clearing_subset` falls back
    to the full (single-feed) selection and sets `partial=True`, so
    `agg_feeds` and `selection.feeds` are the SAME list here: per-surface
    width is a non-issue on this fixture, and both the tile and the
    drill-down must report the same `state`. If the tile's gate were
    resolved over the raw coverage-guard filter instead of over
    `clearing_subset`'s fallback return, it would see an empty rendered set
    here and read a different state than the numbers an unconfident feed
    produced. (14 hours is also short of the 24 the target local day needs,
    so the feed is not extrema-eligible; this test asserts `state`, not the
    now-suppressed temperature extrema, so that is not a concern here.)
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Forecast Partial Coverage")
    set_setting(conn, "min_n", "1")
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' AND model='ecmwf_ifs'"
        ).fetchone()["id"]
    )

    now = utc_now()
    tomorrow = now.date() + timedelta(days=1)
    issued_at = isoformat_utc(floor_hour(now))
    for h in range(14):  # 14 covered hours: >=12 (pool floor), <18 (coverage guard)
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'temperature', ?, ?, ?, 11.0, '{}', 'run-1', ?)
            """,
            (
                site_id,
                feed_id,
                issued_at,
                f"{tomorrow.isoformat()}T{h:02d}:00:00Z",
                h + 1,
                issued_at,
            ),
        )
    # Deliberately NO publication seeded, same as the fixture above.

    view = build_forecast(
        conn, site_id=site_id, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[1].temp.meta.state == "low_confidence"
    assert view.tiles[1].temp.meta.partial is True

    hourly = build_hourly(conn, site_id=site_id, timezone="UTC", day=1, now=now)
    assert hourly["states"]["temperature"] == "low_confidence"


# ---------------------------------------------------------------------------
# DELETED: test_forecast_hourly_chart_fallback_svg_hidden_and_summary_mount_
# outside_container pinned the per-day hourly uPlot chart's fallback <svg>
# and its [data-chart]/[data-summary] mounts on /forecast/day. Card 7
# replaced the whole Day Detail view with _tiles.html's table grids, so
# /forecast/day no longer renders a chart, an svg, or a data-summary mount
# at all -- there is nothing left on this route for the test's subject to
# pin, and no table-grid counterpart needs the same accessibility contract
# (no canvas, no uPlot instance, nothing JS mounts into). This lines up
# with tests/test_app_js_chart_a11y.py's app.js-level pin that the number of
# ``new uPlot(...)`` instantiations dropped from 3 to 2 (owned by the
# parallel test writer on this plan) -- the third chart this test exercised
# is the one that was removed.
# ---------------------------------------------------------------------------
