"""Roster horizon provenance oracles — 0.12.0 §15 family 8 (§8, W5).

Covers ``RosterFeed.max_lead_hours``'s rehydration contract in
``wxverify.verification.runs``: a pre-0.12.0 snapshot (no key) must rehydrate
to ``None`` -- "not recorded" -- and must NOT raise, paired with a 0.12.0
snapshot that round-trips the integer; the live column surfaced by
``capture_config_snapshot``; a roster whose only difference is
``max_lead_hours`` detected by ``assert_inputs_unpinned_unchanged``; and the
display projection in ``wxverify.web.verification._snapshot_view``, which
gets its own oracle rather than manual verification of the rendered column.

Isolation: ``tests.helpers.asof_conn`` (fresh fully-migrated in-memory
database), mirroring ``tests/test_verification_pairwise.py``.

Synthetic data only: ``site-alpha`` / ``site-alpha-src``, invented feed ids.
"""

from __future__ import annotations

import json

import pytest

from tests.helpers import asof_conn, asof_make_real_feed, asof_make_site
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.runs import (
    RosterFeed,
    RunConfig,
    _parse_roster,  # noqa: SLF001 - the rehydration function under test
    assert_inputs_unpinned_unchanged,
    capture_config_snapshot,
    run_config_from_row,
)
from wxverify.web.verification import _snapshot_view  # noqa: SLF001

# ---------------------------------------------------------------------------
# _parse_roster: pre-0.12.0 absence vs 0.12.0 presence (paired).
# ---------------------------------------------------------------------------


def test_parse_roster_pre_0_12_0_entry_rehydrates_max_lead_hours_none() -> None:
    """A snapshot written before 0.12.0 has no ``max_lead_hours`` key at all
    (not a null value -- an absent key): the injected precondition here is
    the withheld key itself, not an ambient default."""
    raw = [{"feed_id": 101, "source": "site-alpha-src", "model": "model-a"}]
    roster = _parse_roster(raw)
    assert roster == (
        RosterFeed(
            feed_id=101, source="site-alpha-src", model="model-a", max_lead_hours=None
        ),
    )


def test_parse_roster_0_12_0_entry_round_trips_the_integer() -> None:
    """Paired positive: the same shape WITH the key present round-trips the
    integer rather than falling back to None."""
    raw = [
        {
            "feed_id": 101,
            "source": "site-alpha-src",
            "model": "model-a",
            "max_lead_hours": 168,
        }
    ]
    roster = _parse_roster(raw)
    assert roster == (
        RosterFeed(
            feed_id=101, source="site-alpha-src", model="model-a", max_lead_hours=168
        ),
    )


# ---------------------------------------------------------------------------
# End-to-end DB path: a pre-0.12.0 run row must not raise on load.
# ---------------------------------------------------------------------------


def test_run_config_from_row_pre_0_12_0_snapshot_does_not_raise() -> None:
    """A real ``verification_runs`` row whose ``config_snapshot`` JSON
    predates 0.12.0 (roster entries with no ``max_lead_hours`` key at all)
    must rehydrate through the production read path without raising --
    §8's acceptance criterion "no pre-0.12.0 run fails to load."."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "site-alpha")
    generation_id = ensure_published_generation(conn, site_id)
    pre_0_12_0_snapshot = {
        "timezone": "UTC",
        "rain_threshold_mm": 0.2,
        "wall_clock": "07:00",
        "blend_depth": 2,
        "min_n": 30,
        "window_days": 30,
        "tz_generation_id": generation_id,
        "roster": [{"feed_id": 101, "source": "site-alpha-src", "model": "model-a"}],
    }
    run_id = int(
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 bootstrap_seed, bootstrap_resamples, input_fingerprint)
            VALUES (?, ?, 1, '0.11.3-test', 'running', 1, ?,
                    '2026-06-01', '2026-06-01', 1, 100, 'fp-pre-0.12.0')
            """,
            (site_id, generation_id, json.dumps(pre_0_12_0_snapshot)),
        ).lastrowid
    )
    cfg = run_config_from_row(conn, run_id)
    assert cfg.roster == (
        RosterFeed(
            feed_id=101, source="site-alpha-src", model="model-a", max_lead_hours=None
        ),
    )


# ---------------------------------------------------------------------------
# capture_config_snapshot reports the live column.
# ---------------------------------------------------------------------------


def test_capture_config_snapshot_reports_live_max_lead_hours() -> None:
    conn = asof_conn()
    site_id = asof_make_site(conn, "site-alpha")
    feed_id = asof_make_real_feed(conn, "model-alpha")  # max_lead_hours=48
    snapshot = capture_config_snapshot(conn, site_id)
    roster = snapshot["roster"]
    assert isinstance(roster, list)
    entry = next(r for r in roster if r["feed_id"] == feed_id)  # type: ignore[index]
    assert entry["max_lead_hours"] == 48  # type: ignore[index]


# ---------------------------------------------------------------------------
# assert_inputs_unpinned_unchanged: max_lead_hours-only roster divergence.
# ---------------------------------------------------------------------------


def test_assert_inputs_unpinned_unchanged_flags_max_lead_hours_only_roster_change() -> (
    None
):
    """Negative/absence discipline: the paired POSITIVE (an exactly-matching
    roster, including max_lead_hours) must raise nothing, and the NEGATIVE
    (only max_lead_hours differs) must be flagged as a ``roster`` mismatch --
    proving the comparison is genuinely sensitive to the new field rather
    than passing by coincidence."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "site-alpha")
    feed_id = asof_make_real_feed(conn, "model-alpha")  # max_lead_hours=48
    ensure_published_generation(conn, site_id)
    snapshot = capture_config_snapshot(conn, site_id)
    # asof_conn() is fully migrated and seeds every default-subscribed feed,
    # so the live roster is the WHOLE active-competitor set, not just our
    # feed -- assert our entry's shape rather than the tuple's exact size.
    live_roster = _parse_roster(snapshot["roster"])
    assert (
        RosterFeed(
            feed_id=feed_id,
            source="example-src",
            model="model-alpha",
            max_lead_hours=48,
        )
        in live_roster
    )

    def _cfg(roster: tuple[RosterFeed, ...]) -> RunConfig:
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
            roster=roster,
            period_start="2026-06-01",
            period_end="2026-06-01",
            bootstrap_seed=1,
            bootstrap_resamples=100,
        )

    # Paired positive: unchanged (max_lead_hours included) raises nothing.
    assert_inputs_unpinned_unchanged(conn, _cfg(live_roster))

    # Negative: ONLY our feed's max_lead_hours differs (every other member,
    # including every other feed's max_lead_hours, is left exactly as the
    # live roster reports it) -> flagged as "roster".
    changed_roster = tuple(
        RosterFeed(
            feed_id=r.feed_id, source=r.source, model=r.model, max_lead_hours=999
        )
        if r.feed_id == feed_id
        else r
        for r in live_roster
    )
    assert changed_roster != live_roster
    with pytest.raises(RuntimeError, match="roster"):
        assert_inputs_unpinned_unchanged(conn, _cfg(changed_roster))


# ---------------------------------------------------------------------------
# Display surface: _snapshot_view's roster projection (its own oracle).
# ---------------------------------------------------------------------------


def _bare_snapshot(roster: list[dict[str, object]]) -> dict[str, object]:
    return {
        "timezone": "UTC",
        "rain_threshold_mm": 0.2,
        "wall_clock": "07:00",
        "blend_depth": 2,
        "blend_depths": {},
        "blend_depth_sources": {},
        "min_n": 30,
        "window_days": 30,
        "tz_generation_id": 1,
        "roster": roster,
    }


def test_snapshot_view_roster_max_lead_hours_present_vs_absent() -> None:
    view_0_12_0 = _snapshot_view(
        _bare_snapshot(
            [
                {
                    "feed_id": 101,
                    "source": "site-alpha-src",
                    "model": "model-a",
                    "max_lead_hours": 168,
                }
            ]
        )
    )
    view_pre_0_12_0 = _snapshot_view(
        _bare_snapshot(
            [{"feed_id": 101, "source": "site-alpha-src", "model": "model-a"}]
        )
    )
    assert view_0_12_0 is not None
    assert view_pre_0_12_0 is not None
    assert view_0_12_0["roster"][0]["max_lead_hours"] == 168  # type: ignore[index]
    assert view_pre_0_12_0["roster"][0]["max_lead_hours"] is None  # type: ignore[index]
