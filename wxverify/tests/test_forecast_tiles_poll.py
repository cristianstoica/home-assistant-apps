"""HTTP-level tests for the ``/forecast/tiles`` auto-poll route: the
fingerprint is computed BEFORE the view is built, so a matching fingerprint
short-circuits without ever calling ``build_forecast``.

Isolation: a real tmp-file SQLite DB via ``init_db``/``close_db`` + an idle
worker + ``TestClient`` (mirrors ``tests/test_forecast_routes.py``'s
harness) -- a tmp-file DB, not ``:memory:``, because the app's WAL-mode
connection is a SEPARATE read connection from whatever writes the fixture
rows.

Synthetic fixtures only (public repo) -- fake site name/coords.
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import seed_active_publication, seed_publication_ranking
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import floor_hour, isoformat_utc, utc_now
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.forecast.data import forecast_fingerprint
from wxverify.scoring.metrics import MetricSums, metrics_from_sums
from wxverify.settings.keys import get_number_setting, set_setting
from wxverify.web.context import SiteView

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_forecast_routes.py).
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
    conn: sqlite3.Connection, name: str = "Test Site", *, enabled: int = 1
) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', ?)
            """,
            (name, enabled),
        ).lastrowid
    )


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    value: float,
) -> None:
    # Since the rain release (Part B) an hour holds one row
    # (idx_samples_slot): a later label replaces the hour's stored row.
    conn.execute(
        "DELETE FROM forecast_samples WHERE site_id = ? AND feed_id = ?"
        " AND variable = ? AND valid_at = ?",
        (site_id, feed_id, variable, valid_at),
    )
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-1', ?)
        """,
        (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value, issued_at),
    )


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
    ).fetchone()
    assert row is not None, f"seed feed not found: {source}/{model}"
    return int(row["id"])


def _current_fingerprint(site_id: int) -> str:
    return get_db().read_sync(lambda conn: forecast_fingerprint(conn, site_id=site_id))


def _count_load_sites_calls(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Counts real invocations of the ``load_sites`` binding routes.py
    resolves at call time (``wxverify.web.routes.load_sites``) -- not the
    definition in ``web.context``, which is never called directly by the
    route handlers. Delegates to the original function so behavior is
    unchanged and only the count is observed."""
    from wxverify.web import routes as routes_module

    original = routes_module.load_sites
    calls = {"n": 0}

    def _wrapped(*args: object, **kwargs: object) -> list[SiteView]:
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(routes_module, "load_sites", _wrapped)
    return calls


#: Each day header's High/Low span sits right after its <b> label
#: ("<b>Today</b><span>13° / 13°</span>"), card 7's rebuild of the markup
#: the old "High / Low</span><strong>" pair used to carry.
_HIGH_LOW_RE = re.compile(r"<b>.*?</b>\s*<span>([^<]*)</span>", re.DOTALL)


def _high_low_values(text: str) -> list[str]:
    """Rendered temperature High/Low strings, one per day header, in
    document order -- used to prove a tile body genuinely differs (not the
    whole page, which also carries a wall-clock-derived "Last fetched ..."
    line)."""
    return _HIGH_LOW_RE.findall(text)


# ---------------------------------------------------------------------------
# The point: a matching fingerprint never builds the view.
# ---------------------------------------------------------------------------


def test_tiles_returns_204_without_building_the_view(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)  # zero samples, no publication -> "0-p0"
    app = _make_app(monkeypatch)

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError(
            "build_forecast_page must not run when the fingerprint already matches"
        )

    monkeypatch.setattr("wxverify.web.routes.build_forecast_page", _boom)
    current_fp = _current_fingerprint(site_id)
    with TestClient(app) as client:
        response = client.get(
            f"/forecast/tiles?site={site_id}&fingerprint={current_fp}"
        )
    assert response.status_code == 204


def test_tiles_renders_fragment_when_fingerprint_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2030-01-01T00:00:00Z",
        valid_at="2030-01-01T00:00:00Z",
        lead_hours=1,
        value=10.0,
    )
    new_fp = _current_fingerprint(site_id)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast/tiles?site={site_id}&fingerprint=0")
    assert response.status_code == 200
    assert 'id="forecast-tiles"' in response.text
    assert f"fingerprint={new_fp}" in response.text


def test_tiles_200_not_found_region_for_unknown_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unknown site now answers 200 with the "Site not found." region,
    which carries no ``hx-get`` -- so the auto-poll stops on its own rather
    than looping against a site that no longer exists. Renamed from
    "...204_for_unknown_site": that status no longer holds."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/forecast/tiles?site=999999&fingerprint=")
    assert response.status_code == 200
    assert "Site not found." in response.text
    assert "hx-get" not in response.text


def test_tiles_disabled_site_resolves_like_the_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An EXPLICIT site_id resolves regardless of `enabled` -- for both the
    page and the poll -- exactly as today. This is deliberately NOT a "204
    for a disabled site" test: that behavior does not exist."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Disabled Site", enabled=0)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2030-01-01T00:00:00Z",
        valid_at="2030-01-01T00:00:00Z",
        lead_hours=1,
        value=10.0,
    )
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        page = client.get(f"/forecast?site={site_id}")
        assert page.status_code == 200

        tiles_stale = client.get(f"/forecast/tiles?site={site_id}&fingerprint=")
        assert tiles_stale.status_code == 200
        assert 'id="forecast-tiles"' in tiles_stale.text

        current_fp = _current_fingerprint(site_id)
        tiles_fresh = client.get(
            f"/forecast/tiles?site={site_id}&fingerprint={current_fp}"
        )
        assert tiles_fresh.status_code == 204


def test_tiles_empty_state_site_polls_without_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)  # zero samples
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        first = client.get(f"/forecast/tiles?site={site_id}&fingerprint=")
        assert first.status_code == 200
        assert "Still collecting forecasts" in first.text

        current_fp = _current_fingerprint(site_id)
        again = client.get(f"/forecast/tiles?site={site_id}&fingerprint={current_fp}")
        assert again.status_code == 204


# ---------------------------------------------------------------------------
# Accepted contract: a durable settings write with no new sample must
# still 204 on an unchanged fingerprint -- paired with the non-vacuity half
# (ii), which proves the setting genuinely reaches rendering, or the 204
# above would also pass against a build that ignores it entirely.
# ---------------------------------------------------------------------------


def test_tiles_204_when_only_non_sample_state_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Blend Depth Poll")
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "min_n", "1")
    set_setting(conn, "forecast_blend_depth", "2")

    now = utc_now()
    tomorrow = now.date() + timedelta(days=1)
    # floor_hour: issued_at must match FORECAST_TIMESTAMP_LIKE's whole-second
    # shape or invalid_forecast_sample_sql() marks every sample invalid,
    # silently emptying the view (utc_now() carries microseconds).
    issued_at = isoformat_utc(floor_hour(now))
    for feed_id, value in ((feed_a, 11.0), (feed_b, 15.0)):
        for h in range(24):
            _insert_sample(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="temperature",
                issued_at=issued_at,
                valid_at=f"{tomorrow.isoformat()}T{h:02d}:00:00Z",
                lead_hours=h + 1,
                value=value,
            )

    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    # leaderboard_with_status's `rolling` window now always reads the
    # active publication (9.2), never a live recompute, so the three feeds'
    # rankings land in one site publication instead of score_cache.
    pub_id = seed_active_publication(
        conn,
        site_id=site_id,
        cutoff=date(2035, 6, 30),
        published_at=datetime(2035, 6, 30, tzinfo=UTC),
        window_days=30,
        min_n=min_n,
    )
    # The pair-aggregate SQL that used to compute these (`strategy_for(
    # ...).aggregate`) is gone (W6b), and no pair table is read for scoring
    # any more (storage release 3.1): hand-derived here instead, as if from
    # 3 identical far-future hours (forecast/observed=10.0, persistence
    # forecast=8.0), through the surviving pure `metrics_from_sums`,
    # skill-paired 1:1 against persistence's own (also constant) error.
    forecasts = {persistence_id: 8.0, feed_a: 10.5, feed_b: 9.0}
    persistence_sq = (forecasts[persistence_id] - 10.0) ** 2
    for target_feed in (feed_a, feed_b, persistence_id):
        error = forecasts[target_feed] - 10.0
        sums: MetricSums = {
            "n": 3,
            "err_n": 3,
            "err_sum": error * 3,
            "abs_sum": abs(error) * 3,
            "sq_sum": error * error * 3,
            "skill_n": 3,
            "skill_sq_sum": error * error * 3,
            "skill_ref_sq_sum": persistence_sq * 3,
        }
        result = metrics_from_sums(sums, variable="temperature", min_n=min_n)
        seed_publication_ranking(
            conn,
            publication_id=pub_id,
            feed_id=target_feed,
            variable="temperature",
            day_ahead=1,
            window_key="w:30",
            result=result,
        )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        depth_two_page = client.get(f"/forecast?site={site_id}")
        assert depth_two_page.status_code == 200

        tiles_resp = client.get(f"/forecast/tiles?site={site_id}&fingerprint=")
        assert tiles_resp.status_code == 200
        current_fp = _current_fingerprint(site_id)

        # (i) A durable-settings-only write, no new sample: the fingerprint
        # is unchanged, so the poll must still 204. Written via
        # get_db().write_sync against the RUNNING instance -- the lifespan's
        # own init_db call already closed/replaced the pre-built `conn`
        # (mirrors test_composite_cache_backed.py's harness note).
        get_db().write_sync(lambda c: set_setting(c, "forecast_blend_depth", "1"))
        same_fp_response = client.get(
            f"/forecast/tiles?site={site_id}&fingerprint={current_fp}"
        )
        assert same_fp_response.status_code == 204

        # (ii) Non-vacuity: the setting DID take effect -- the rendered
        # temperature High/Low tile value (not the whole page, which also
        # carries a wall-clock-derived "Last fetched ..." line and a per-request
        # CSRF token) actually changed. depth=2 blends feed_a=11.0/feed_b=15.0
        # -> "13° / 13°"; depth=1 keeps only feed_a (lower
        # persistence-error rank) alone -> "11° / 11°". Both values
        # below were read off an actual run of this fixture, not
        # hand-computed. Without this half the 204 above would also pass
        # against a build that ignores forecast_blend_depth entirely.
        depth_one_page = client.get(f"/forecast?site={site_id}")
        assert depth_one_page.status_code == 200
        assert _high_low_values(depth_two_page.text)[1:2] == ["13° / 13°"]
        assert _high_low_values(depth_one_page.text)[1:2] == ["11° / 11°"]


# ---------------------------------------------------------------------------
# Read-amplification regression: ``_resolve_site`` must not re-load the
# enabled-site list that ``_load_forecast_context`` already loaded, and the
# poll path (which never needs the list for an explicit site_id) must not
# load it at all.
# ---------------------------------------------------------------------------


def test_index_default_site_calls_load_sites_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "Only Site")
    app = _make_app(monkeypatch)
    calls = _count_load_sites_calls(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/")
    assert response.status_code == 200
    assert calls["n"] == 1


def test_forecast_page_explicit_site_calls_load_sites_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Explicit Site")
    app = _make_app(monkeypatch)
    calls = _count_load_sites_calls(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/forecast?site={site_id}")
    assert response.status_code == 200
    assert calls["n"] == 1


def test_tiles_explicit_site_calls_load_sites_zero_times(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A matching fingerprint short-circuits before ``_load_forecast_context``
    ever runs, so an explicit-site_id poll that finds nothing new must not
    load the enabled-site list at all -- unlike the page path, which always
    needs it to build the site-picker nav."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Poll Site")  # zero samples, no publication
    app = _make_app(monkeypatch)
    calls = _count_load_sites_calls(monkeypatch)
    current_fp = _current_fingerprint(site_id)
    with TestClient(app) as client:
        response = client.get(
            f"/forecast/tiles?site={site_id}&fingerprint={current_fp}"
        )
    assert response.status_code == 204
    assert calls["n"] == 0


# ---------------------------------------------------------------------------
# Behavior preservation: the call-count reduction above must not change which
# site is resolved, or how an unknown/absent site is handled.
# ---------------------------------------------------------------------------


def test_index_default_site_prefers_enabled_over_earlier_named_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The rendered context's site list is the enabled-only one: a disabled
    site must never appear in the site picker, and the default pick (no
    ``site`` param) comes from that same enabled-only list."""
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "AAA Disabled", enabled=0)
    _make_site(conn, "ZZZ Enabled", enabled=1)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/")
    assert response.status_code == 200
    assert "ZZZ Enabled" in response.text
    assert "AAA Disabled" not in response.text


def test_index_with_no_enabled_sites_resolves_to_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "Only Disabled", enabled=0)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/")
    assert response.status_code == 200
    assert "No sites configured." in response.text


def test_unknown_site_id_resolves_to_none_on_forecast_page(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "Real Site")
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/forecast?site=999999")
    assert response.status_code == 200
    assert "No sites configured." in response.text


def test_explicit_site_id_resolves_disabled_site_via_page_and_poll(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An EXPLICIT site_id resolves through ``load_site``, which does not
    filter on ``enabled`` -- pinned on both the page and the poll, the two
    call sites the fix touches. Card 7 dropped the per-site subtitle that
    used to carry the site's own name and "- paused" wording, so a disabled
    site resolving normally is now pinned by the "Paused" indicator firing
    instead of the page falling back to "No sites configured.", which is
    what an unresolved site_id would show."""
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn, "Paused Village", enabled=0)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        page = client.get(f"/forecast?site={site_id}")
        assert page.status_code == 200
        assert "No sites configured." not in page.text
        assert '<p class="muted">Paused</p>' in page.text

        tiles = client.get(f"/forecast/tiles?site={site_id}&fingerprint=")
        assert tiles.status_code == 200
        assert 'id="forecast-tiles"' in tiles.text
