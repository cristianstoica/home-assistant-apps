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

from tests.helpers import assert_summary_mount_not_nested_in_chart, collect_tags
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
            VALUES (?, 47.0, 25.0, 900.0, ?, 1)
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
    site_id = _make_site(conn)  # zero samples -> fingerprint is always "0"
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        stale = client.get(f"/forecast/tiles?site={site_id}&fingerprint=")
        fresh = client.get(f"/forecast/tiles?site={site_id}&fingerprint=0")

    assert stale.status_code == 200
    assert 'id="forecast-tiles"' in stale.text
    assert fresh.status_code == 204
    assert fresh.text == ""


def test_forecast_tiles_unknown_site_yields_204(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/forecast/tiles?site=999999&fingerprint=")
    assert response.status_code == 204


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


def test_forecast_day_clamps_and_embeds_clamped_day_in_chart_src(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast/day?site={site_id}&day=99")
    assert response.status_code == 200
    # Jinja autoescapes the literal `&` in the query string to `&amp;`.
    assert f"/api/forecast/hourly?site={site_id}&amp;day=7" in response.text
    assert "day=99" not in response.text


# ---------------------------------------------------------------------------
# New Forecast degradation test (deliberate no-score_cache case): pins the
# degrade-gracefully behaviour. This is the intentional counterpart to the score_cache
# seeding migration required elsewhere -- a rebuilding-empty ranking must
# degrade gracefully (low_confidence, never a crash or a silent stale
# "normal"), and only this test may run Forecast against an unseeded cache.
# ---------------------------------------------------------------------------


def test_forecast_degrades_to_low_confidence_without_score_cache_no_enqueue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """forecast_pairs + future_samples exist -- enough for a live skill
    computation to be genuinely confident -- but `score_cache` is left
    EMPTY. Pre-fix, `leaderboard()`'s live-fallback on a cache miss makes
    this cell confident/`normal` regardless of the cache; post-fix, an
    absent cache-backed rolling-window snapshot degrades to a
    `rebuilding`-empty ranking, so every candidate is unconfident/unscored
    and the selection ladder falls to the future-sample-count rung ->
    `low_confidence`.

    A ``virtual/_persistence`` baseline with matching (site, variable,
    valid_at, lead_hours, day_ahead) `forecast_pairs` rows is REQUIRED for
    this contrast to be real: `_paired_skill` (metrics.py) returns
    ``skill_score=None`` -- and therefore ``confident=False`` -- whenever no
    persistence baseline pairs exist, live-recompute or not. Without the
    baseline, both pre- and post-fix code degrade to `low_confidence` for
    the same unrelated reason (no skill data), which would make this test
    pass vacuously regardless of the cache-miss bug -- confirmed by running
    it against pre-fix code with only the candidate feed's pairs seeded.

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
    already-empty day. `forecast_pairs` use a fixed far-future date
    (2035-06-30) instead: the rolling window's cutoff is a lower bound
    only, so a far-future pair stays "in window" regardless of which real
    day the suite runs on -- the same pattern
    `test_forecast_blend_depth_option.py::_seed_two_confident_feeds` relies
    on.

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
    persistence_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
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
    for i, valid_at in enumerate(
        ("2035-06-30T00:00:00Z", "2035-06-30T01:00:00Z", "2035-06-30T02:00:00Z")
    ):
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', '2035-06-29T00:00:00Z', ?, ?, 1,
                    11.0, 10.0, 1.0, 1.0, 1.0)
            """,
            (site_id, feed_id, valid_at, i + 1),
        )
    for i, valid_at in enumerate(
        ("2035-06-30T00:00:00Z", "2035-06-30T01:00:00Z", "2035-06-30T02:00:00Z")
    ):
        # Persistence baseline paired on the SAME (site, variable, valid_at,
        # lead_hours, day_ahead) as the candidate feed's rows above --
        # required by `_paired_skill`'s join. A worse forecast (15.0 vs
        # observed 10.0) than the candidate's (11.0 vs 10.0) gives a
        # nonzero, genuinely-confident skill_score under live recompute.
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error)
            VALUES (?, ?, 'temperature', '2035-06-29T00:00:00Z', ?, ?, 1,
                    15.0, 10.0, 5.0, 5.0, 25.0)
            """,
            (site_id, persistence_id, valid_at, i + 1),
        )
    # Deliberately NO upsert_score_cache call -- the point of this fixture.

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
# Chart accessibility: fallback SVG hidden, summary mount outside container.
# ---------------------------------------------------------------------------


def test_forecast_hourly_chart_fallback_svg_hidden_and_summary_mount_outside_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    # Non-UTC so a template that hardcoded "UTC" (or read the wrong field)
    # cannot pass the data-timezone assertion below by accident -- it must
    # observe this SITE's configured zone, not any default.
    site_id = _make_site(conn, "Hourly Chart Site", timezone="America/Denver")
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast/day?site={site_id}&day=0")
    assert response.status_code == 200
    html = response.text

    svgs = collect_tags(html, "svg")
    assert len(svgs) == 1, (
        f"expected exactly one <svg> on /forecast/day, found {len(svgs)}"
    )
    assert svgs[0].get("aria-hidden") == "true"
    assert "aria-label" not in svgs[0]
    assert "role" not in svgs[0]

    chart_divs = [
        a for a in collect_tags(html, "div") if a.get("data-chart") == "forecast-hourly"
    ]
    assert len(chart_divs) == 1
    assert "role" not in chart_divs[0], (
        "the [data-chart] container must carry no ARIA role -- it flattens "
        "uPlot's own legend table out of the accessibility tree"
    )
    # Same reasoning as the role check above, for aria-hidden: uPlot mounts
    # its own canvas + legend INSIDE this container, so hiding the container
    # itself would defeat the whole point of leaving it role-less.
    assert "aria-hidden" not in chart_divs[0]
    # A typo'd data-summary (e.g. "forecast-hourly-sumary") would make the
    # client's summaryEl() -> getElementById(id) return null and silently
    # disable the whole feature with no error -- pin the attribute's VALUE,
    # not just the existence of a same-named div (checked below).
    assert chart_divs[0].get("data-summary") == "forecast-hourly-summary"
    # hourFormatter() reads el.dataset.timezone to format every hour label in
    # the SITE's local time; a missing/wrong value silently degrades every
    # label to the BROWSER's local time with no error. Must equal the site's
    # actual configured zone, not merely be present.
    assert chart_divs[0].get("data-timezone") == "America/Denver"

    assert_summary_mount_not_nested_in_chart(html, summary_id="forecast-hourly-summary")
