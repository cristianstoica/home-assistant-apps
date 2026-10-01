"""Tests: the wind history switch window (plan §8.12), T101-T103.

Covers three independent seams that go quiet while a site's wind basis is
`switching` or `rescoring`:

- T101: `_blocking_gate` (worker/verification_run.py) skips a verification
  trigger on those two states only, via `_decide_phase` and `_start_phase`.
- T102: `_input_mismatches` (verification/runs.py) includes `wind_basis` in
  its divergence tuple, so `assert_inputs_unpinned_unchanged` and
  `assert_inputs_unchanged_readonly` raise when the stored state moves
  mid-run.
- T103: `_advance_correction_outside_wind_switch` (worker/processor.py)
  defers a `timezone_correction` chunk instead of calling `advance_correction`.

Isolation: all tests use an in-memory sqlite3 connection + `run_migrations`,
and a minimal synthetic site (no feeds/truth needed -- `capture_config_snapshot`
only requires a `sites` row and tolerates an empty roster). Synthetic data
only (public repo): site name "Testsite", `Etc/UTC` timezone.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest

from wxverify.db.migrations import run_migrations
from wxverify.db.wind_basis import set_wind_basis_state
from wxverify.verification.runs import (
    RunConfig,
    _parse_roster,  # noqa: PLC2701
    assert_inputs_unchanged_readonly,
    assert_inputs_unpinned_unchanged,
    capture_config_snapshot,
)
from wxverify.worker import processor as processor_module
from wxverify.worker.control import JobDeferred
from wxverify.worker.verification_run import (  # noqa: PLC2701
    _decide_phase,
    _start_phase,
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
# T101 -- _blocking_gate skips a trigger in switching/rescoring only
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("state", ["switching", "rescoring"])
def test_decide_phase_skips_on_wind_switch(state: str) -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    set_wind_basis_state(conn, site_id, state)
    conn.commit()

    more = _decide_phase(conn, site_id, {"trigger_date": "2026-06-01"}, {})

    assert more is False, "a blocked decide must not continue the chain"
    row = conn.execute(
        "SELECT decision, reason FROM verification_trigger_decisions WHERE site_id = ?"
        " ORDER BY id DESC LIMIT 1",
        (site_id,),
    ).fetchone()
    assert row is not None
    assert (row["decision"], row["reason"]) == (
        "skipped",
        "wind history switch in progress",
    )


@pytest.mark.parametrize("state", ["switching", "rescoring"])
def test_start_phase_skips_on_wind_switch(state: str) -> None:
    """Drive `_start_phase` via its forced-start fallthrough (redecide_attempts
    already at MAX_REDECIDE_ATTEMPTS=1), which re-evaluates `_blocking_gate`
    against the freshly derived fingerprint regardless of whether that
    fingerprint matches the blob's stale `decided` value -- exactly as the
    real forced-start path does when a prior redecide already happened.
    """
    conn = _make_db()
    site_id = _make_site(conn)
    set_wind_basis_state(conn, site_id, state)
    conn.commit()

    more = _start_phase(
        conn,
        site_id,
        {"trigger_date": "2026-06-01"},
        {"fingerprint": "stale-placeholder", "redecide_attempts": 1},
    )

    assert more is False, "a blocked start must not continue the chain"
    row = conn.execute(
        "SELECT decision, reason FROM verification_trigger_decisions WHERE site_id = ?"
        " ORDER BY id DESC LIMIT 1",
        (site_id,),
    ).fetchone()
    assert row is not None
    assert (row["decision"], row["reason"]) == (
        "skipped",
        "wind history switch in progress",
    )
    # No run row was ever created by the blocked start.
    runs = conn.execute(
        "SELECT COUNT(*) AS n FROM verification_runs WHERE site_id = ?", (site_id,)
    ).fetchone()
    assert runs["n"] == 0


@pytest.mark.parametrize("state", ["staging", "pair_max"])
def test_decide_phase_not_skipped_by_wind_switch_in_open_states(state: str) -> None:
    """staging and pair_max are never skipped by the wind-switch gate.

    Kills: a denylist `state != "pair_max"` (which would wrongly also skip
    `staging`). A bare empty-roster site has no settled truth, so this
    `_decide_phase` call still returns False overall -- but for the
    DIFFERENT reason "no settled truth under the published generation",
    which proves the wind-switch gate was never consulted.
    """
    conn = _make_db()
    site_id = _make_site(conn)
    set_wind_basis_state(conn, site_id, state)
    conn.commit()

    _decide_phase(conn, site_id, {"trigger_date": "2026-06-01"}, {})

    row = conn.execute(
        "SELECT decision, reason FROM verification_trigger_decisions WHERE site_id = ?"
        " ORDER BY id DESC LIMIT 1",
        (site_id,),
    ).fetchone()
    assert row is not None
    assert row["reason"] != "wind history switch in progress"


# ---------------------------------------------------------------------------
# T102 -- wind_basis divergence trips the pinned-inputs integrity check
# ---------------------------------------------------------------------------


def test_assert_inputs_unpinned_unchanged_raises_on_wind_basis_divergence() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    set_wind_basis_state(conn, site_id, "staging")
    conn.commit()
    snapshot = capture_config_snapshot(conn, site_id)
    assert snapshot["wind_basis"] == "staging"
    cfg = _runconfig_from_snapshot(conn, site_id, snapshot)

    set_wind_basis_state(conn, site_id, "switching")
    conn.commit()

    with pytest.raises(RuntimeError, match="wind_basis"):
        assert_inputs_unpinned_unchanged(conn, cfg)


def test_assert_inputs_unchanged_readonly_raises_on_wind_basis_divergence() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    set_wind_basis_state(conn, site_id, "staging")
    conn.commit()
    snapshot = capture_config_snapshot(conn, site_id)
    cfg = _runconfig_from_snapshot(conn, site_id, snapshot)

    set_wind_basis_state(conn, site_id, "switching")
    conn.commit()

    with pytest.raises(RuntimeError, match="wind_basis"):
        assert_inputs_unchanged_readonly(conn, cfg)


def test_assert_inputs_unpinned_unchanged_does_not_raise_when_state_unchanged() -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    set_wind_basis_state(conn, site_id, "staging")
    conn.commit()
    snapshot = capture_config_snapshot(conn, site_id)
    cfg = _runconfig_from_snapshot(conn, site_id, snapshot)

    assert_inputs_unpinned_unchanged(conn, cfg)  # must not raise
    assert_inputs_unchanged_readonly(conn, cfg)  # must not raise


def test_pinned_none_vs_stored_staging_counts_as_wind_basis_divergence() -> None:
    """A run begun before the 0.16.6 upgrade pinned `wind_basis=None`; once
    resumed against a live `staging` state, that counts as a divergence too
    (plan §8.12: "accepted" to fail once, not silently treated as a match).
    """
    conn = _make_db()
    site_id = _make_site(conn)
    set_wind_basis_state(conn, site_id, "staging")
    conn.commit()
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


# ---------------------------------------------------------------------------
# T103 -- timezone_correction defers during switching/rescoring
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("state", ["switching", "rescoring"])
def test_timezone_correction_defers_during_wind_switch(
    state: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    set_wind_basis_state(conn, site_id, state)
    conn.commit()
    calls: list[int] = []

    def _spy(*_args: Any, **_kwargs: Any) -> bool:
        calls.append(1)
        return False

    monkeypatch.setattr(processor_module, "advance_correction", _spy)

    with pytest.raises(JobDeferred):
        processor_module._advance_correction_outside_wind_switch(  # noqa: SLF001
            conn, site_id, {}
        )

    assert calls == [], "advance_correction must not be called during a wind switch"


@pytest.mark.parametrize("state", ["staging", "pair_max"])
def test_timezone_correction_proceeds_outside_wind_switch(
    state: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _make_db()
    site_id = _make_site(conn)
    set_wind_basis_state(conn, site_id, state)
    conn.commit()
    calls: list[int] = []

    def _spy(*_args: Any, **_kwargs: Any) -> bool:
        calls.append(1)
        return False

    monkeypatch.setattr(processor_module, "advance_correction", _spy)

    result = processor_module._advance_correction_outside_wind_switch(  # noqa: SLF001
        conn, site_id, {}
    )

    assert result is False
    assert calls == [1], "advance_correction must be called outside a wind switch"
