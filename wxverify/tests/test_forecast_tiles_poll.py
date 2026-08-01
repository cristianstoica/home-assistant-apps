"""HTTP-level tests for the ``/forecast/tiles`` auto-poll route (plan
2026-08-01-read-path-latency, §8.3, Change 6): the fingerprint is computed
BEFORE the view is built, so a matching fingerprint short-circuits without
ever calling ``build_forecast``.

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
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.timeutil import floor_hour, isoformat_utc, utc_now
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.forecast.data import samples_fingerprint
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.leaderboard import resolve_window
from wxverify.scoring.metrics import strategy_for
from wxverify.settings.keys import get_number_setting, set_setting

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
            VALUES (?, 47.0, 25.0, 900.0, 'UTC', ?)
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
    return get_db().read_sync(lambda conn: samples_fingerprint(conn, site_id=site_id))


_HIGH_LOW_RE = re.compile(r"High / Low</span>\s*<strong[^>]*>(.*?)</strong>", re.DOTALL)


def _high_low_values(text: str) -> list[str]:
    """Rendered temperature High/Low strings, one per tile, in document
    order -- used to prove a tile body genuinely differs (not the whole
    page, which also carries a wall-clock-derived "Updated ..." line)."""
    return _HIGH_LOW_RE.findall(text)


# ---------------------------------------------------------------------------
# The point of Change 6: a matching fingerprint never builds the view.
# ---------------------------------------------------------------------------


def test_tiles_returns_204_without_building_the_view(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)  # zero samples -> fingerprint is always "0"
    app = _make_app(monkeypatch)

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError(
            "build_forecast must not run when the fingerprint already matches"
        )

    monkeypatch.setattr("wxverify.web.routes.build_forecast", _boom)
    with TestClient(app) as client:
        response = client.get(f"/forecast/tiles?site={site_id}&fingerprint=0")
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


def test_tiles_204_for_unknown_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/forecast/tiles?site=999999&fingerprint=")
    assert response.status_code == 204


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

        again = client.get(f"/forecast/tiles?site={site_id}&fingerprint=0")
        assert again.status_code == 204


# ---------------------------------------------------------------------------
# §7.4 accepted contract: a durable settings write with no new sample must
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

    far_valid_ats = [
        "2035-06-30T00:00:00Z",
        "2035-06-30T01:00:00Z",
        "2035-06-30T02:00:00Z",
    ]
    far_lead_hours = [1, 2, 3]
    for target_feed, forecast in ((persistence_id, 8.0), (feed_a, 10.5), (feed_b, 9.0)):
        for valid_at, lead_hours in zip(far_valid_ats, far_lead_hours, strict=True):
            error = forecast - 10.0
            conn.execute(
                """
                INSERT INTO forecast_pairs
                    (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                     day_ahead, forecast, observed, error, abs_error, sq_error)
                VALUES (?, ?, 'temperature', '2035-06-29T00:00:00Z', ?, ?, 1, ?,
                        10.0, ?, ?, ?)
                """,
                (
                    site_id,
                    target_feed,
                    valid_at,
                    lead_hours,
                    forecast,
                    error,
                    abs(error),
                    error * error,
                ),
            )

    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    resolved = resolve_window(conn, "rolling")
    for target_feed in (feed_a, feed_b, persistence_id):
        result = strategy_for("temperature").aggregate(
            conn,
            site_id=site_id,
            feed_id=target_feed,
            variable="temperature",
            day_ahead=1,
            window_cutoff=resolved.cutoff,
            min_n=min_n,
        )
        upsert_score_cache(
            conn,
            site_id=site_id,
            feed_id=target_feed,
            variable="temperature",
            day_ahead=1,
            window_key=resolved.window_key,
            result=result,
            computed_at=isoformat_utc(),
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
        # carries a wall-clock-derived "Updated ..." line and a per-request
        # CSRF token) actually changed. depth=2 blends feed_a=11.0/feed_b=15.0
        # -> "13° / 13°"; depth=1 keeps only feed_a (lower
        # persistence-error rank) alone -> "11° / 11°". Both values
        # below were read off an actual run of this fixture, not
        # hand-computed. Without this half the 204 above would also pass
        # against a build that ignores forecast_blend_depth entirely.
        depth_one_page = client.get(f"/forecast?site={site_id}")
        assert depth_one_page.status_code == 200
        assert _high_low_values(depth_two_page.text) == ["13° / 13°"]
        assert _high_low_values(depth_one_page.text) == ["11° / 11°"]
