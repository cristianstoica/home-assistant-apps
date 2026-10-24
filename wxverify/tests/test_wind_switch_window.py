"""Tests: the wind history switch window (plan §8.12), T102.

- T102: `capture_config_snapshot` (verification/runs.py) always records
  `wind_basis` as `pair_max`, so a stored state that moves mid-run no longer
  trips `assert_inputs_unpinned_unchanged` or
  `assert_inputs_unchanged_readonly`; `_input_mismatches` still compares the
  field, so a run pinned without it still fails.

Isolation: all tests use an in-memory sqlite3 connection + `run_migrations`,
and a minimal synthetic site (no feeds/truth needed -- `capture_config_snapshot`
only requires a `sites` row and tolerates an empty roster). Synthetic data
only (public repo): site name "Testsite", `Etc/UTC` timezone.
"""

from __future__ import annotations

import sqlite3

import pytest

from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.wind_basis import set_wind_basis_state
from wxverify.verification.runs import (
    RunConfig,
    _parse_roster,  # noqa: PLC2701
    assert_inputs_unchanged_readonly,
    assert_inputs_unpinned_unchanged,
    capture_config_snapshot,
)


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, name: str = "Testsite") -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 0.0, 0.0, 0.0, 'Etc/UTC')
        """,
        (name,),
    )
    assert cur.lastrowid is not None
    conn.commit()
    return int(cur.lastrowid)


def _runconfig_from_snapshot(
    conn: sqlite3.Connection, site_id: int, snapshot: dict[str, object]
) -> RunConfig:
    """Build a RunConfig pinned to `snapshot`'s values, for the mismatch tests."""
    return RunConfig(
        site_id=site_id,
        run_id=1,
        timezone=str(snapshot["timezone"]),
        rain_threshold_mm=float(str(snapshot["rain_threshold_mm"])),
        wall_clock=str(snapshot["wall_clock"]),
        blend_depth=int(str(snapshot["blend_depth"])),
        blend_depths=dict(snapshot["blend_depths"]),  # type: ignore[arg-type]
        min_n=int(str(snapshot["min_n"])),
        window_days=int(str(snapshot["window_days"])),
        tz_generation_id=int(str(snapshot["tz_generation_id"])),
        roster=_parse_roster(snapshot.get("roster")),
        period_start="2026-06-01",
        period_end="2026-06-01",
        bootstrap_seed=1,
        bootstrap_resamples=40,
        wind_basis=str(snapshot["wind_basis"]) if snapshot["wind_basis"] else None,
    )


# ---------------------------------------------------------------------------
# T102 -- the snapshot's wind_basis is always pair_max
# ---------------------------------------------------------------------------


def test_assert_inputs_unpinned_unchanged_ignores_a_stored_state_change() -> None:
    """Nothing reads the stored key any more: the snapshot records
    `pair_max` whatever it holds, so a stored state that moves mid-run is no
    divergence.
    """
    conn = _make_db()
    site_id = _make_site(conn)
    set_runtime_state(conn, f"wind_basis:{site_id}", "staging")
    conn.commit()
    snapshot = capture_config_snapshot(conn, site_id)
    assert snapshot["wind_basis"] == "pair_max"
    cfg = _runconfig_from_snapshot(conn, site_id, snapshot)

    set_runtime_state(conn, f"wind_basis:{site_id}", "switching")
    conn.commit()

    assert_inputs_unpinned_unchanged(conn, cfg)  # must not raise


def test_assert_inputs_unchanged_readonly_ignores_a_stored_state_change() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    set_runtime_state(conn, f"wind_basis:{site_id}", "staging")
    conn.commit()
    snapshot = capture_config_snapshot(conn, site_id)
    cfg = _runconfig_from_snapshot(conn, site_id, snapshot)

    set_runtime_state(conn, f"wind_basis:{site_id}", "switching")
    conn.commit()

    assert_inputs_unchanged_readonly(conn, cfg)  # must not raise


def test_assert_inputs_unpinned_unchanged_does_not_raise_when_state_unchanged() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    set_wind_basis_state(conn, site_id, "staging")
    conn.commit()
    snapshot = capture_config_snapshot(conn, site_id)
    cfg = _runconfig_from_snapshot(conn, site_id, snapshot)

    assert_inputs_unpinned_unchanged(conn, cfg)  # must not raise
    assert_inputs_unchanged_readonly(conn, cfg)  # must not raise


def test_pinned_none_counts_as_wind_basis_divergence() -> None:
    """A run begun before the 0.16.6 upgrade pinned `wind_basis=None`; once
    resumed against the live snapshot's `pair_max`, that counts as a
    divergence (plan §8.12: "accepted" to fail once, not silently treated as
    a match).
    """
    conn = _make_db()
    site_id = _make_site(conn)
    snapshot = capture_config_snapshot(conn, site_id)
    cfg = _runconfig_from_snapshot(conn, site_id, snapshot)
    cfg_pre_upgrade = RunConfig(
        site_id=cfg.site_id,
        run_id=cfg.run_id,
        timezone=cfg.timezone,
        rain_threshold_mm=cfg.rain_threshold_mm,
        wall_clock=cfg.wall_clock,
        blend_depth=cfg.blend_depth,
        blend_depths=cfg.blend_depths,
        min_n=cfg.min_n,
        window_days=cfg.window_days,
        tz_generation_id=cfg.tz_generation_id,
        roster=cfg.roster,
        period_start=cfg.period_start,
        period_end=cfg.period_end,
        bootstrap_seed=cfg.bootstrap_seed,
        bootstrap_resamples=cfg.bootstrap_resamples,
        wind_basis=None,
    )

    with pytest.raises(RuntimeError, match="wind_basis"):
        assert_inputs_unpinned_unchanged(conn, cfg_pre_upgrade)
