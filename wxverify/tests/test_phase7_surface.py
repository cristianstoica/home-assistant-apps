"""Phase-7 surface tests: §15 per-variable blend depth.

Covers the depth-resolution helper (override wins, invalid falls through,
provenance), the options→settings clearing rule, the
``record_snapshot_local_time`` setter endpoint, and the forecast-page
ride-alongs. (§16's verification UI/API -- the ``/api/verification/*``
endpoints, the ``/verification`` page, the run config snapshot/fingerprint
lockstep, and the published-run pointer -- was removed with the
verification-run layer, 0.16.14 Part B.)

Synthetic fixtures only — fake site names/coords, UTC timezone, invented
feed models; no real station IDs or keys.
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_conn, seed_rain_ranks
from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.options import RuntimeOptions, load_runtime_options
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, init_db
from wxverify.db.switch import ensure_site_generation
from wxverify.settings.depth import (
    DEPTH_VARIABLES,
    EffectiveDepth,
    depth_override_key,
    effective_blend_depth,
    effective_blend_depths,
)
from wxverify.settings.keys import get_setting, set_setting
from wxverify.settings.service import apply_plain_settings
from wxverify.verification.snapshot_time import SNAPSHOT_TIME_KEY, parse_wall_clock

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_web_ui.py / tests/test_db_transfer.py).
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


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": "http://testserver", "X-CSRF-Token": token}


def _make_site(conn: sqlite3.Connection, name: str = "Verify Town") -> int:
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)
            """,
            (name,),
        ).lastrowid
    )
    ensure_site_generation(conn, site_id)
    return site_id


def _seed_full_day_precip(
    conn: sqlite3.Connection, site_id: int, *, now: datetime
) -> None:
    """A complete 24-hour UTC local day of precip samples, from ``now``'s
    midnight, so ``/forecast`` both renders a real tile (instead of the
    "still collecting" empty state) AND the tile's precip feed is
    extrema-eligible under G.3 (``covers_local_day_exactly``) -- required
    to reach the Rain ``tile-row``'s tooltip markup, not just its
    unconditional ``<span>Rain</span>`` label (see
    ``test_forecast_page_ride_alongs``).

    ``now`` MUST be the same fixed instant the page handler's own
    ``utc_now()`` call is pinned to (see that test): seeding from a live
    ``utc_now()`` here while the handler reads a separately-called live
    ``utc_now()`` risks a UTC-midnight rollover landing between the two
    calls, which would seed one local day and render another.
    """
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = 'open-meteo' AND model = 'ecmwf_ifs'"
    ).fetchone()
    assert row is not None
    feed_id = int(row["id"])
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    issued_at = isoformat_utc(day_start - timedelta(hours=6))
    for hour in range(24):
        valid_at = day_start + timedelta(hours=hour)
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'precip', ?, ?, ?, 0.0, '{}', 'run-x', ?)
            """,
            (
                site_id,
                feed_id,
                issued_at,
                isoformat_utc(valid_at),
                hour + 6,
                issued_at,
            ),
        )


# ---------------------------------------------------------------------------
# §15 — depth resolution, provenance, clearing.
# ---------------------------------------------------------------------------


def test_effective_depth_global_default_and_provenance() -> None:
    conn = asof_conn()
    depths = effective_blend_depths(conn)
    assert set(depths) == set(DEPTH_VARIABLES)
    for d in depths.values():
        assert d == EffectiveDepth(depth=2, source="global")


def test_effective_depth_override_wins_and_others_stay_global() -> None:
    conn = asof_conn()
    set_setting(conn, "forecast_blend_depth", "3")
    set_setting(conn, depth_override_key("wind"), "5")
    depths = effective_blend_depths(conn)
    assert depths["wind"] == EffectiveDepth(depth=5, source="override")
    assert depths["temperature"] == EffectiveDepth(depth=3, source="global")
    assert depths["precip"] == EffectiveDepth(depth=3, source="global")


@pytest.mark.parametrize("bad", ["0", "7", "abc", "", "2.5", "-1"])
def test_effective_depth_invalid_override_falls_through_to_global(
    bad: str,
) -> None:
    conn = asof_conn()
    set_setting(conn, depth_override_key("precip"), bad)
    resolved = effective_blend_depth(conn, "precip")
    assert resolved == EffectiveDepth(depth=2, source="global")


def test_apply_plain_settings_clears_absent_depth_overrides(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    try:
        # Present key -> row written.
        asyncio.run(apply_plain_settings(RuntimeOptions(forecast_blend_depth_wind=4)))
        assert get_setting(conn, depth_override_key("wind")) == "4"
        # Absent key on the next apply -> row DELETED (clearing rule),
        # while the untouched global key semantics stay apply-when-present.
        asyncio.run(apply_plain_settings(RuntimeOptions()))
        assert get_setting(conn, depth_override_key("wind")) is None
        assert effective_blend_depth(conn, "wind").source == "global"
    finally:
        close_db()


def test_options_env_and_json_plumbing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Env path: options file absent.
    monkeypatch.setattr(config, "options_path", str(tmp_path / "missing.json"))
    monkeypatch.setenv("WXV_FORECAST_BLEND_DEPTH_TEMPERATURE", "6")
    opts = load_runtime_options()
    assert opts.forecast_blend_depth_temperature == 6
    assert opts.forecast_blend_depth_wind is None
    # File path: env ignored, keys read from options.json.
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps({"forecast_blend_depth_precip": 1}), encoding="utf-8"
    )
    monkeypatch.setattr(config, "options_path", str(options_path))
    opts = load_runtime_options()
    assert opts.forecast_blend_depth_precip == 1
    assert opts.forecast_blend_depth_temperature is None


# ---------------------------------------------------------------------------
# Obligation 2 — record_snapshot_local_time setter endpoint.
# ---------------------------------------------------------------------------


def _read_setting(key: str) -> str | None:
    """Fresh direct read: the seed conn is closed once the app starts."""
    direct = sqlite3.connect(config.db_path)
    try:
        row = direct.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row[0])
    finally:
        direct.close()


def test_snapshot_time_setter_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        # Global set, canonicalised.
        resp = client.put(
            "/api/settings/record-snapshot-time",
            json={"time": "6:05"},
            headers=headers,
        )
        assert resp.status_code == 200
        assert resp.json() == {"key": SNAPSHOT_TIME_KEY, "time": "06:05"}
        assert _read_setting(SNAPSHOT_TIME_KEY) == "06:05"

        # Per-site override.
        resp = client.put(
            "/api/settings/record-snapshot-time",
            json={"time": "08:30", "site_id": site_id},
            headers=headers,
        )
        assert resp.status_code == 200
        assert _read_setting(f"{SNAPSHOT_TIME_KEY}:{site_id}") == "08:30"

        # Validation (same parser the record reader uses).
        for bad in ("24:00", "07:60", "nope", "1200", ""):
            assert parse_wall_clock(bad) is None
            resp = client.put(
                "/api/settings/record-snapshot-time",
                json={"time": bad},
                headers=headers,
            )
            assert resp.status_code == 400

        # Unknown site.
        resp = client.put(
            "/api/settings/record-snapshot-time",
            json={"time": "07:00", "site_id": 999999},
            headers=headers,
        )
        assert resp.status_code == 404

        # Clearing the per-site override falls back to the global value.
        resp = client.delete(
            f"/api/settings/record-snapshot-time?site={site_id}",
            headers=headers,
        )
        assert resp.status_code == 200
        assert _read_setting(f"{SNAPSHOT_TIME_KEY}:{site_id}") is None
        assert _read_setting(SNAPSHOT_TIME_KEY) == "06:05"


# ---------------------------------------------------------------------------
# §16 — forecast ride-alongs.
# ---------------------------------------------------------------------------


def _precip_day_text(response_text: str) -> str:
    """The Precipitation grid's top-row day-column text (``page.rain_text``)
    out of a rendered ``/forecast`` page -- card 7's replacement for the old
    tile row's "Rain" ``<strong>``. Scoped to the Precipitation ``<table>``
    (the selected day's only), so it never picks up another grid's day cell.
    """
    section = response_text.split("Precipitation <span>mm/h</span>")[1]
    section = section.split("</table>")[0]
    start = section.index('<td class="day">') + len('<td class="day">')
    end = section.index("</td>", start)
    # The cell's visible text only -- the rain winner's markup (C4) wraps it
    # in a <small> and, with an agreement percentage, a
    # <span title="Agreement">: strip every tag, keep what a reader sees.
    return re.sub(r"<[^>]+>", "", section[start:end]).strip()


def test_forecast_page_ride_alongs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _make_site(conn)
    # ONE fixed instant for both the seeding below and the page handler's
    # own clock, pinned at its real seam (`wxverify.forecast.service`'s
    # module-level `utc_now` binding, which `build_forecast`/`build_hourly`
    # read via `now or utc_now()`): a live `utc_now()` read separately by
    # the seeding helper and by the handler could straddle a UTC-midnight
    # rollover and seed one local day while the handler renders another.
    fixed_now = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.forecast.service.utc_now", lambda: fixed_now)
    # build_forecast_page's own `now or utc_now()` reads page.py's own import
    # of utc_now, a separate binding from service.utc_now above -- both must
    # be pinned to the same instant or the page renders against the real
    # wall-clock date instead of fixed_now.
    monkeypatch.setattr("wxverify.forecast.page.utc_now", lambda: fixed_now)
    # A real tile is required to reach the Precipitation grid's rendered
    # row at all; an empty-state site (zero samples) never renders a
    # "Precipitation" grid, so the assertion below would silently never
    # execute.
    _seed_full_day_precip(conn, site_id, now=fixed_now)
    # Rain release, C4: the rain row shows the published rain ranking's
    # winner, so the seeded feed is ranked for today (day ahead 0).
    precip_feed = conn.execute(
        "SELECT id FROM feeds WHERE source = 'open-meteo' AND model = 'ecmwf_ifs'"
    ).fetchone()
    assert precip_feed is not None
    seed_rain_ranks(
        conn,
        site_id=site_id,
        published_at=fixed_now,
        ranked={0: [int(precip_feed["id"])]},
    )
    conn.commit()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        # Card 7 shows only the SELECTED day's grids (day=1, tomorrow, by
        # default) -- today (day=0) is where this fixture's precip lives.
        page = client.get("/forecast?day=0")
        assert page.status_code == 200
        # Relabel: the old "Chance of rain" copy is gone entirely (G.5).
        assert "Chance of rain" not in page.text
        # Fixture guard: the grid actually rendered, so the assertion below
        # is not vacuous.
        assert "Precipitation" in page.text
        row_text = _precip_day_text(page.text)
        # Wording regression (0.16.3): the rain row must never regain the
        # "wet" qualifier or the "~" approximation glyph on the wet-hours
        # figure -- a bare "<n> h" only.
        assert "h wet" not in row_text, "stale ' h wet' wording still rendered"
        assert "~" not in row_text, "stale '~' approximation glyph still rendered"
        assert row_text == "0.0 mm · 0 h", (
            "rain row did not render the bare '<n> h' wet-hours figure"
        )
