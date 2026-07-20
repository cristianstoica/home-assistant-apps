"""Tests for the ``forecast_blend_depth`` option's touch points.

Spec build-sequence step 4 verify hook: the option's 6 touch points
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
from datetime import UTC, datetime
from typing import TypeVar

import pytest
from pydantic import ValidationError

from wxverify.core.options import RuntimeOptions, _from_env
from wxverify.db.migrations import run_migrations
from wxverify.forecast.service import build_forecast
from wxverify.settings.keys import get_setting, set_setting
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
        VALUES (1, 'Test Site', 47.0, 25.0, 900.0, 'UTC')
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
    persistence_id = _feed_id(conn, "virtual", "_persistence")
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

    far_valid_ats = [
        "2035-07-01T00:00:00Z",
        "2035-07-01T01:00:00Z",
        "2035-07-01T02:00:00Z",
    ]
    far_lead_hours = [1, 2, 3]
    for target_feed, forecast in (
        (persistence_id, 8.0),
        (ecmwf_id, 10.5),
        (gfs_id, 9.5),
    ):
        for valid_at, lead_hours in zip(far_valid_ats, far_lead_hours, strict=True):
            error = forecast - 10.0
            conn.execute(
                """
                INSERT INTO forecast_pairs
                    (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                     day_ahead, forecast, observed, error, abs_error, sq_error)
                VALUES
                    (1, ?, 'temperature', '2035-06-30T00:00:00Z', ?, ?, 1, ?,
                     10.0, ?, ?, ?)
                """,
                (
                    target_feed,
                    valid_at,
                    lead_hours,
                    forecast,
                    error,
                    abs(error),
                    error * error,
                ),
            )


def test_end_to_end_default_blends_two_explicit_setting_narrows_to_one() -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)

    conn_default = _make_db()
    _seed_two_confident_feeds(conn_default)
    view_default = build_forecast(
        conn_default, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert len(view_default.tiles[0].temp.meta.feeds) == 2  # default depth = 2

    conn_narrow = _make_db()
    _seed_two_confident_feeds(conn_narrow)
    set_setting(conn_narrow, "forecast_blend_depth", "1")
    view_narrow = build_forecast(
        conn_narrow, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert len(view_narrow.tiles[0].temp.meta.feeds) == 1
