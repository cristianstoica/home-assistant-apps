"""QA oracles for the pre-publish divergence gate (D10):
``divergent_truth_in_horizon`` (wxverify.verification.truth) and its caller
``publish_verified_run`` (wxverify.verification.engine).

A run's inputs are pinned at ``start_run``; every phase since consumes the
truth rows that existed then. If a late station observation regenerates one
of those rows AFTER the run started but BEFORE it publishes, the run's
scores were computed against a now-superseded truth value. The gate
re-derives every candidate (stale, in-horizon) day one more time and refuses
to publish if any quantity's stored value/eligible/covered_hours disagrees
with a fresh re-derivation -- the whole run is discarded rather than
published against silently stale inputs.

Fixture discipline: a raw ``INSERT INTO daily_truth`` is never used on any
path in this file. Every truth row is produced by a production function --
``materialize_daily_truth`` from raw ``observations`` seed rows (O11b, O11,
O13, O13b) or by ``insert_station_observation`` driving the real consensus
pipeline (O11c, O11d, O11e). O12 seeds a second, retired timezone
generation directly (permitted -- the fixture rule scopes ``daily_truth``
only) to prove the gate's stored-row fetch is generation-scoped.

Every raising oracle asserts ``pytest.raises(RuntimeError, match="truth
diverged after inputs were pinned")`` rather than a bare exception type: the
same function raises an identically-typed, identically-prefixed
``RuntimeError`` for the pre-existing verdict/result integrity check, and a
bare-type assertion would pass for the wrong reason.

All fixture data is synthetic: an invented site name, synthetic coordinates,
UTC (no real deployment timezone), fabricated model/station identifiers.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import asof_conn, asof_make_real_feed
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.scoring.consensus import insert_station_observation
from wxverify.verification.engine import publish_verified_run
from wxverify.verification.runs import published_run_id, run_config_from_row
from wxverify.verification.truth import (
    _evaluate_day,  # noqa: SLF001
    divergent_truth_in_horizon,
    mark_daily_truth_stale,
    materialize_daily_truth,
)
from wxverify.worker.verification_run import (
    _compute_verdicts,  # noqa: SLF001
    _load_state,  # noqa: SLF001
    _persist_verdicts,  # noqa: SLF001
    advance_verification,
)

# ---------------------------------------------------------------------------
# Shared fixture plumbing (adapted from tests/test_verification_no_change_
# oracles.py's house idiom for driving the real sync chain -- but every
# daily_truth row here is produced through materialize_daily_truth over raw
# observations rather than a direct INSERT).
# ---------------------------------------------------------------------------

_PERIOD_DAYS = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05"]

#: Temperature hourly profile: hour 6 is the day's low (9.0, inside the
#: local low-peak window [3,9)); hour 12 is the day's high (21.0, inside the
#: local high-peak window [12,18)); every other hour is a neutral 15.0. All
#: 24 hours covered, both peak windows satisfied independent of value.
_TEMP_LOW_HOUR = 6
_TEMP_HIGH_HOUR = 12
_TEMP_LOW_VALUE = 9.0
_TEMP_HIGH_VALUE = 21.0
_TEMP_NEUTRAL_VALUE = 15.0
_WIND_VALUE = 6.0
_PRECIP_VALUE = 0.0

_QUANTITY_VALUES = {
    "temperature_high": _TEMP_HIGH_VALUE,
    "temperature_low": _TEMP_LOW_VALUE,
    "wind_max": _WIND_VALUE,
    "precip_total": _PRECIP_VALUE,
    "precip_occurrence": _PRECIP_VALUE,
}


def _seed_day_observations(
    conn: sqlite3.Connection,
    site_id: int,
    day: str,
    *,
    precip_hour_overrides: dict[int, float] | None = None,
) -> None:
    """Raw ``observations`` rows for one local day (production-shaped, real
    datastore -- this is a seed, not the function under test)."""
    overrides = precip_hour_overrides or {}
    for hour in range(24):
        valid_at = f"{day}T{hour:02d}:00:00Z"
        computed_at = f"{day}T{hour:02d}:30:00Z"
        if hour == _TEMP_LOW_HOUR:
            temp = _TEMP_LOW_VALUE
        elif hour == _TEMP_HIGH_HOUR:
            temp = _TEMP_HIGH_VALUE
        else:
            temp = _TEMP_NEUTRAL_VALUE
        precip = overrides.get(hour, _PRECIP_VALUE)
        for variable, value in (
            ("temperature", temp),
            ("wind", _WIND_VALUE),
            ("precip", precip),
        ):
            conn.execute(
                """
                INSERT INTO observations
                    (site_id, variable, valid_at, value, n_stations, computed_at)
                VALUES (?, ?, ?, ?, 3, ?)
                """,
                (site_id, variable, valid_at, value, computed_at),
            )


def _make_site(
    conn: sqlite3.Connection,
    *,
    precip_overrides: dict[str, dict[int, float]] | None = None,
) -> tuple[int, list[int], int]:
    """Sites row + 2 real feeds + 5 days of published-generation truth,
    materialized from raw observations through ``materialize_daily_truth``
    (never a direct daily_truth insert) + forecast_samples covering the
    period plus 8 extra days (matching the house sim-chain fixture shape).

    Returns ``(site_id, feed_ids, tz_generation_id)``.
    """
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('gate-town', 40.0, -105.0, 900.0, 'UTC')
        """
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    feeds = [
        asof_make_real_feed(conn, "gate-model-alpha"),
        asof_make_real_feed(conn, "gate-model-beta"),
    ]
    generation_id = ensure_published_generation(conn, site_id)
    overrides = precip_overrides or {}
    for day in _PERIOD_DAYS:
        _seed_day_observations(
            conn, site_id, day, precip_hour_overrides=overrides.get(day)
        )
        materialize_daily_truth(
            conn, site_id=site_id, local_date=day, tz_generation_id=generation_id
        )
    values = {"temperature": 15.0, "wind": 5.0, "precip": 0.0}
    issued = "2026-05-31T05:00:00Z"
    for feed_index, feed_id in enumerate(feeds):
        for variable, base in values.items():
            for day_offset in range(len(_PERIOD_DAYS) + 8):
                for hour in range(24):
                    total_hours = day_offset * 24 + hour
                    valid = (
                        datetime(2026, 6, 1, tzinfo=UTC) + timedelta(hours=total_hours)
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
                    conn.execute(
                        """
                        INSERT INTO forecast_samples
                            (site_id, feed_id, variable, issued_at, valid_at,
                             lead_hours, value, source_raw, model_run_id,
                             fetched_at)
                        VALUES (?, ?, ?, ?, ?, 6, ?, '{}', 'run-gate', ?)
                        """,
                        (
                            site_id,
                            feed_id,
                            variable,
                            issued,
                            valid,
                            base + 0.5 * feed_index,
                            issued,
                        ),
                    )
    conn.commit()
    return site_id, feeds, generation_id


def _make_station(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    """An ENABLED station at the site's own elevation (zero lapse-rate
    correction), so a single station reading equals the resulting
    consensus value exactly."""
    cur = conn.execute(
        """
        INSERT INTO stations
            (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
        VALUES (?, ?, 40.0, -105.0, 900.0, 1)
        """,
        (site_id, pws_station_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _drive_until_publish_pending(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    *,
    resamples: int = 40,
    max_steps: int = 300,
) -> int:
    """Drive the real sync chain up to (not including) the ``publish``
    phase's own execution -- the run stays in state ``running`` so the
    caller can perturb truth and call ``publish_verified_run`` directly.
    Returns the pending run's id."""
    for _ in range(max_steps):
        blob = _load_state(conn, site_id)
        if blob is not None and blob.get("phase") == "publish":
            run_id = blob["run_id"]
            assert isinstance(run_id, int)
            return run_id
        if blob is not None and blob.get("phase") == "bootstrap":
            run_id = blob["run_id"]
            assert isinstance(run_id, int)
            cfg = run_config_from_row(conn, run_id)
            from wxverify.verification.engine import prepare_bootstrap_inputs

            inputs = prepare_bootstrap_inputs(conn, cfg)
            verdicts = _compute_verdicts(inputs, cfg.bootstrap_seed, resamples)
            _persist_verdicts(conn, site_id, cfg, verdicts)
            continue
        assert advance_verification(conn, site_id, payload)
    raise AssertionError("chain never reached a pending publish")


def _stale_quantities(
    conn: sqlite3.Connection, site_id: int, local_date: str
) -> set[str]:
    rows = conn.execute(
        """
        SELECT quantity FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND stale = 1
        """,
        (site_id, local_date),
    ).fetchall()
    return {str(r["quantity"]) for r in rows}


def _perturb_temperature_peak_hour(
    conn: sqlite3.Connection, site_id: int, day: str, new_value: float
) -> None:
    """Raw-observations mutation path: directly rewrite the day's peak
    temperature hour and mark it stale -- the O11/O13/O13b/O12 route."""
    conn.execute(
        """
        UPDATE observations SET value = ?
        WHERE site_id = ? AND variable = 'temperature'
          AND valid_at = ?
        """,
        (new_value, site_id, f"{day}T{_TEMP_HIGH_HOUR:02d}:00:00Z"),
    )
    marked = mark_daily_truth_stale(
        conn,
        site_id=site_id,
        variable="temperature",
        valid_at=f"{day}T{_TEMP_HIGH_HOUR:02d}:00:00Z",
    )
    assert marked == 2


_RAISE_MATCH = "truth diverged after inputs were pinned"


# ---------------------------------------------------------------------------
# O11b -- false-positive control: an untouched fixture publishes cleanly.
# Written FIRST: a wedged (never-publishes) false positive is the expensive
# failure mode, so its absence is pinned before anything that induces a
# real divergence.
# ---------------------------------------------------------------------------


def test_o11b_untouched_run_publishes_without_a_divergence_false_positive() -> None:
    conn = asof_conn()
    site_id, _feeds, generation_id = _make_site(conn)
    run_id = _drive_until_publish_pending(conn, site_id, {"trigger_date": "2026-06-06"})
    cfg = run_config_from_row(conn, run_id)

    # Precondition: nothing is stale going into the gate.
    for day in _PERIOD_DAYS:
        assert _stale_quantities(conn, site_id, day) == set()

    # Hand-set the mark with NO change to any observation -- a mark with no
    # content change is not a divergence; a flag-only gate fails here.
    marked = conn.execute(
        "UPDATE daily_truth SET stale = 1 WHERE site_id = ? AND local_date = ?",
        (site_id, "2026-06-02"),
    )
    assert marked.rowcount == 5  # the day's five quantity rows, all marked

    divergent = divergent_truth_in_horizon(
        conn,
        site_id=site_id,
        tz_generation_id=generation_id,
        period_start=cfg.period_start,
        period_end=cfg.period_end,
    )
    assert divergent == []

    publish_verified_run(conn, cfg)
    assert published_run_id(conn, site_id) == run_id


# ---------------------------------------------------------------------------
# O11 -- the paired positive: a value changed after the run started is
# caught, and reported by direct helper call BEFORE the end-to-end raise
# (nothing in the pre-existing suite executes a line inside the candidate
# loop, so this direct call is the only thing that would notice the loop
# itself going dead).
# ---------------------------------------------------------------------------


def test_o11_value_changed_after_pinning_is_caught_and_blocks_publish() -> None:
    conn = asof_conn()
    site_id, _feeds, generation_id = _make_site(conn)
    day = "2026-06-02"
    run_id = _drive_until_publish_pending(conn, site_id, {"trigger_date": "2026-06-06"})
    cfg = run_config_from_row(conn, run_id)

    _perturb_temperature_peak_hour(conn, site_id, day, 30.0)
    assert _stale_quantities(conn, site_id, day) == {
        "temperature_high",
        "temperature_low",
    }

    # Direct helper-level liveness assertion.
    divergent = divergent_truth_in_horizon(
        conn,
        site_id=site_id,
        tz_generation_id=generation_id,
        period_start=cfg.period_start,
        period_end=cfg.period_end,
    )
    assert divergent == [(day, "temperature_high")]

    with pytest.raises(RuntimeError, match=_RAISE_MATCH):
        publish_verified_run(conn, cfg)
    assert published_run_id(conn, site_id) is None
    state = conn.execute(
        "SELECT state FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert state is not None
    assert str(state["state"]) == "running"


# ---------------------------------------------------------------------------
# O12 -- generation scoping: a stale, genuinely divergent day sitting under
# a DIFFERENT (retired) timezone generation must never leak into the
# published generation's comparison. Tested at the helper level directly --
# generation scoping is a pure property of divergent_truth_in_horizon
# itself, independent of the run-publish integration the other oracles
# exercise.
# ---------------------------------------------------------------------------


def test_o12_a_retired_generations_stale_day_never_leaks_into_the_published_ones() -> (
    None
):
    conn = asof_conn()
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('gate-town-o12', 40.0, -105.0, 900.0, 'UTC')
        """
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    published_id = ensure_published_generation(conn, site_id)
    day = "2026-06-01"
    _seed_day_observations(conn, site_id, day)
    materialize_daily_truth(
        conn, site_id=site_id, local_date=day, tz_generation_id=published_id
    )
    published_high = conn.execute(
        """
        SELECT value FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND quantity = 'temperature_high'
          AND tz_generation_id = ?
        """,
        (site_id, day, published_id),
    ).fetchone()
    assert published_high is not None
    assert float(published_high["value"]) == _TEMP_HIGH_VALUE

    # A second, RETIRED generation materializes the SAME local day while the
    # peak hour is temporarily perturbed, then the observation is restored --
    # so the retired generation's stored row diverges from what current
    # observations would produce, while the published generation's own row
    # does not.
    cur = conn.execute(
        """
        INSERT INTO timezone_generations (site_id, timezone, mode, state)
        VALUES (?, 'UTC', 'retrospective_correction', 'retired')
        """,
        (site_id,),
    )
    assert cur.lastrowid is not None
    retired_id = int(cur.lastrowid)
    conn.execute(
        """
        UPDATE observations SET value = 99.0
        WHERE site_id = ? AND variable = 'temperature' AND valid_at = ?
        """,
        (site_id, f"{day}T{_TEMP_HIGH_HOUR:02d}:00:00Z"),
    )
    materialize_daily_truth(
        conn, site_id=site_id, local_date=day, tz_generation_id=retired_id
    )
    retired_high = conn.execute(
        """
        SELECT value FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND quantity = 'temperature_high'
          AND tz_generation_id = ?
        """,
        (site_id, day, retired_id),
    ).fetchone()
    assert retired_high is not None
    assert float(retired_high["value"]) == 99.0
    conn.execute(
        """
        UPDATE observations SET value = ?
        WHERE site_id = ? AND variable = 'temperature' AND valid_at = ?
        """,
        (_TEMP_HIGH_VALUE, site_id, f"{day}T{_TEMP_HIGH_HOUR:02d}:00:00Z"),
    )
    marked = mark_daily_truth_stale(
        conn, site_id=site_id, variable="temperature", valid_at=f"{day}T12:00:00Z"
    )
    # Both generations' rows share this day's UTC bounds (same timezone) so
    # a single call marks both -- 2 quantities x 2 generations.
    assert marked == 4
    conn.commit()

    divergent = divergent_truth_in_horizon(
        conn,
        site_id=site_id,
        tz_generation_id=published_id,
        period_start=day,
        period_end=day,
    )
    assert divergent == []


# ---------------------------------------------------------------------------
# O13 / O13b -- horizon scoping, paired: a divergence strictly outside
# [period_start, period_end] never blocks publish (O13); the identical
# perturbation applied to the boundary day itself (period_end) does (O13b).
# ---------------------------------------------------------------------------


def test_o13_a_divergence_outside_the_pinned_horizon_never_blocks_publish() -> None:
    conn = asof_conn()
    site_id, _feeds, generation_id = _make_site(conn)
    run_id = _drive_until_publish_pending(conn, site_id, {"trigger_date": "2026-06-06"})
    cfg = run_config_from_row(conn, run_id)
    assert cfg.period_start == "2026-06-01"
    assert cfg.period_end == "2026-06-05"

    outside_day = "2026-06-06"
    _seed_day_observations(conn, site_id, outside_day)
    materialize_daily_truth(
        conn, site_id=site_id, local_date=outside_day, tz_generation_id=generation_id
    )
    _perturb_temperature_peak_hour(conn, site_id, outside_day, 30.0)
    assert _stale_quantities(conn, site_id, outside_day) == {
        "temperature_high",
        "temperature_low",
    }

    divergent = divergent_truth_in_horizon(
        conn,
        site_id=site_id,
        tz_generation_id=generation_id,
        period_start=cfg.period_start,
        period_end=cfg.period_end,
    )
    assert divergent == []

    publish_verified_run(conn, cfg)
    assert published_run_id(conn, site_id) == run_id


def test_o13b_the_identical_divergence_at_the_horizon_boundary_blocks_publish() -> None:
    conn = asof_conn()
    site_id, _feeds, generation_id = _make_site(conn)
    run_id = _drive_until_publish_pending(conn, site_id, {"trigger_date": "2026-06-06"})
    cfg = run_config_from_row(conn, run_id)
    assert cfg.period_end == "2026-06-05"

    boundary_day = cfg.period_end
    _perturb_temperature_peak_hour(conn, site_id, boundary_day, 30.0)
    assert _stale_quantities(conn, site_id, boundary_day) == {
        "temperature_high",
        "temperature_low",
    }

    divergent = divergent_truth_in_horizon(
        conn,
        site_id=site_id,
        tz_generation_id=generation_id,
        period_start=cfg.period_start,
        period_end=cfg.period_end,
    )
    assert divergent == [(boundary_day, "temperature_high")]

    with pytest.raises(RuntimeError, match=_RAISE_MATCH):
        publish_verified_run(conn, cfg)
    assert published_run_id(conn, site_id) is None


# ---------------------------------------------------------------------------
# O11c / O11d / O11e -- seeded via insert_station_observation (the real
# consensus pipeline), each isolating exactly ONE of the three compared
# fields (value / eligible / covered_hours) so a mutant that drops any one
# comparison is caught by the oracle built to isolate it.
# ---------------------------------------------------------------------------


def test_o11c_a_station_driven_value_change_is_caught() -> None:
    conn = asof_conn()
    site_id, _feeds, generation_id = _make_site(conn)
    day = "2026-06-01"
    station_id = _make_station(conn, site_id, "gate-station-c")
    run_id = _drive_until_publish_pending(conn, site_id, {"trigger_date": "2026-06-06"})
    cfg = run_config_from_row(conn, run_id)

    changed = insert_station_observation(
        conn,
        station_id=station_id,
        variable="temperature",
        valid_at=f"{day}T{_TEMP_HIGH_HOUR:02d}:00:00Z",
        value=25.0,
        source_raw="{}",
    )
    assert changed
    assert _stale_quantities(conn, site_id, day) == {
        "temperature_high",
        "temperature_low",
    }

    divergent = divergent_truth_in_horizon(
        conn,
        site_id=site_id,
        tz_generation_id=generation_id,
        period_start=cfg.period_start,
        period_end=cfg.period_end,
    )
    assert divergent == [(day, "temperature_high")]

    with pytest.raises(RuntimeError, match=_RAISE_MATCH):
        publish_verified_run(conn, cfg)
    assert published_run_id(conn, site_id) is None


def test_o11c_adjacent_station_driven_eligibility_flip_is_caught() -> None:
    """Not O11d (see O11d below): a station-driven ELIGIBILITY flip, adjacent
    to O11c's value-change raise -- both prove the gate fires on a real
    divergence; useful coverage in its own right, kept but no longer
    claiming the o11d identifier."""
    conn = asof_conn()
    site_id, _feeds, generation_id = _make_site(conn)
    day = "2026-06-01"
    station_id = _make_station(conn, site_id, "gate-station-eligibility")
    run_id = _drive_until_publish_pending(conn, site_id, {"trigger_date": "2026-06-06"})
    cfg = run_config_from_row(conn, run_id)

    before = conn.execute(
        """
        SELECT eligible, covered_hours FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND quantity = 'wind_max'
        """,
        (site_id, day),
    ).fetchone()
    assert before is not None
    assert int(before["eligible"]) == 1
    assert int(before["covered_hours"]) == 24

    # Two out-of-range ([0,80]) wind readings drop wind's own two hours from
    # consensus (qc_flag='range' is filtered by the consensus SELECT):
    # covered_hours 24 -> 22, below the near-complete floor of 23.
    for hour in (3, 4):
        changed = insert_station_observation(
            conn,
            station_id=station_id,
            variable="wind",
            valid_at=f"{day}T{hour:02d}:00:00Z",
            value=999.0,
            source_raw="{}",
        )
        assert changed
    assert _stale_quantities(conn, site_id, day) == {"wind_max"}

    divergent = divergent_truth_in_horizon(
        conn,
        site_id=site_id,
        tz_generation_id=generation_id,
        period_start=cfg.period_start,
        period_end=cfg.period_end,
    )
    assert divergent == [(day, "wind_max")]
    fresh_after = conn.execute(
        """
        SELECT eligible, covered_hours, value FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND quantity = 'wind_max'
        """,
        (site_id, day),
    ).fetchone()
    assert fresh_after is not None
    # Stored row is still the PRE-divergence snapshot (eligible=1) -- the
    # gate must have compared it against a fresh re-derivation, not just
    # echoed what's on disk.
    assert int(fresh_after["eligible"]) == 1
    assert float(fresh_after["value"]) == _WIND_VALUE

    with pytest.raises(RuntimeError, match=_RAISE_MATCH):
        publish_verified_run(conn, cfg)
    assert published_run_id(conn, site_id) is None


def test_o11d_byte_identical_reinsert_is_not_a_divergence_false_positive() -> None:
    """False-positive control (paired with O11b): re-inserting an
    observation whose content is UNCHANGED is the routine every-night
    idempotent refetch, and must never wedge publication."""
    conn = asof_conn()
    site_id, _feeds, generation_id = _make_site(conn)
    day = "2026-06-01"
    station_id = _make_station(conn, site_id, "gate-station-d")
    run_id = _drive_until_publish_pending(conn, site_id, {"trigger_date": "2026-06-06"})
    cfg = run_config_from_row(conn, run_id)

    # Prime the station with a reading whose value matches the fixture's
    # existing consensus exactly -- establishes the station_observations
    # row this test's re-insert will be byte-identical against, without
    # itself changing what the day's truth re-derives to.
    primed = insert_station_observation(
        conn,
        station_id=station_id,
        variable="wind",
        valid_at=f"{day}T03:00:00Z",
        value=_WIND_VALUE,
        source_raw="{}",
    )
    assert primed is True

    stale_before = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id = ? AND stale = 1",
        (site_id,),
    ).fetchone()
    assert stale_before is not None
    # The priming call marked exactly one row stale -- this test's candidate.
    # Without it the gate's loop never runs and the control is vacuous.
    assert int(stale_before["n"]) == 1

    # Byte-identical re-insert: same station, same variable, same instant,
    # same value, same source_raw -- the routine idempotent refetch.
    changed = insert_station_observation(
        conn,
        station_id=station_id,
        variable="wind",
        valid_at=f"{day}T03:00:00Z",
        value=_WIND_VALUE,
        source_raw="{}",
    )
    assert changed is False

    stale_after = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id = ? AND stale = 1",
        (site_id,),
    ).fetchone()
    assert stale_after is not None
    assert int(stale_after["n"]) == int(stale_before["n"])

    divergent = divergent_truth_in_horizon(
        conn,
        site_id=site_id,
        tz_generation_id=generation_id,
        period_start=cfg.period_start,
        period_end=cfg.period_end,
    )
    assert divergent == []

    publish_verified_run(conn, cfg)
    assert published_run_id(conn, site_id) == run_id


def test_o11e_a_station_driven_covered_hours_only_change_is_caught() -> None:
    conn = asof_conn()
    site_id, _feeds, generation_id = _make_site(conn)
    day = "2026-06-01"
    station_id = _make_station(conn, site_id, "gate-station-e")
    run_id = _drive_until_publish_pending(conn, site_id, {"trigger_date": "2026-06-06"})
    cfg = run_config_from_row(conn, run_id)

    dropped_hour = 18  # neither the day's peak-high (12) nor peak-low (6)
    # hour, and outside both local peak windows [12,18) and [3,9) -- so
    # removing it changes covered_hours alone.
    changed = insert_station_observation(
        conn,
        station_id=station_id,
        variable="temperature",
        valid_at=f"{day}T{dropped_hour:02d}:00:00Z",
        value=999.0,  # out of [-90, 60] -> qc_flag='range', dropped by consensus
        source_raw="{}",
    )
    assert changed
    assert _stale_quantities(conn, site_id, day) == {
        "temperature_high",
        "temperature_low",
    }

    divergent = divergent_truth_in_horizon(
        conn,
        site_id=site_id,
        tz_generation_id=generation_id,
        period_start=cfg.period_start,
        period_end=cfg.period_end,
    )
    assert sorted(divergent) == [
        (day, "temperature_high"),
        (day, "temperature_low"),
    ]
    stored = conn.execute(
        """
        SELECT quantity, value, eligible, covered_hours FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND quantity IN
            ('temperature_high', 'temperature_low')
        ORDER BY quantity
        """,
        (site_id, day),
    ).fetchall()
    for row in stored:
        assert int(row["covered_hours"]) == 24  # stale on disk, pre-drop
        assert int(row["eligible"]) == 1
    high = next(r for r in stored if r["quantity"] == "temperature_high")
    low = next(r for r in stored if r["quantity"] == "temperature_low")
    assert float(high["value"]) == _TEMP_HIGH_VALUE
    assert float(low["value"]) == _TEMP_LOW_VALUE

    with pytest.raises(RuntimeError, match=_RAISE_MATCH):
        publish_verified_run(conn, cfg)
    assert published_run_id(conn, site_id) is None


# ---------------------------------------------------------------------------
# O11f -- the STORED-ROW fetch must compare every quantity of a candidate
# day, not only the ones that happen to be marked stale. Precip is diverged
# by flipping the LIVE rain_threshold_mm without ever marking a precip
# quantity stale; only temperature is marked stale. Assert the precondition
# explicitly (else this oracle proves nothing), then assert the gate STILL
# reports the precip divergence.
# ---------------------------------------------------------------------------


def test_o11f_stale_row_fetch_still_compares_every_quantity_of_the_candidate_day() -> (
    None
):
    conn = asof_conn()
    day = "2026-06-03"
    site_id, _feeds, generation_id = _make_site(conn, precip_overrides={day: {0: 0.15}})
    run_id = _drive_until_publish_pending(conn, site_id, {"trigger_date": "2026-06-06"})
    cfg = run_config_from_row(conn, run_id)

    precip_before = conn.execute(
        """
        SELECT quantity, value FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND quantity = 'precip_occurrence'
        """,
        (site_id, day),
    ).fetchone()
    assert precip_before is not None
    assert float(precip_before["value"]) == 0.0  # 0.15mm < 0.2mm threshold -> dry

    # Live threshold change: _evaluate_day reads sites.rain_threshold_mm at
    # re-derivation time, not the run's pinned config snapshot -- 0.15mm now
    # clears a 0.1mm threshold, flipping the day's occurrence to wet.
    conn.execute("UPDATE sites SET rain_threshold_mm = 0.1 WHERE id = ?", (site_id,))

    marked = mark_daily_truth_stale(
        conn, site_id=site_id, variable="temperature", valid_at=f"{day}T12:00:00Z"
    )
    assert marked == 2

    # Precondition: ONLY the two temperature quantities are marked stale --
    # no precip quantity. Without this assertion the oracle proves nothing.
    assert _stale_quantities(conn, site_id, day) == {
        "temperature_high",
        "temperature_low",
    }

    # The gate must nevertheless report the changed PRECIP pair, even though
    # precip was never marked stale -- this is the mutant the whole oracle
    # exists for (a stored-row fetch narrowed to `stale = 1` would silently
    # miss it).
    divergent = divergent_truth_in_horizon(
        conn,
        site_id=site_id,
        tz_generation_id=generation_id,
        period_start=cfg.period_start,
        period_end=cfg.period_end,
    )
    assert divergent == [(day, "precip_occurrence")]

    with pytest.raises(
        RuntimeError,
        match=_RAISE_MATCH + re.escape(f": {day}/precip_occurrence"),
    ):
        publish_verified_run(conn, cfg)
    assert published_run_id(conn, site_id) is None


# ---------------------------------------------------------------------------
# Additional gap-closing oracle (found empirically, not one of the required
# nine): every O11* fixture above changes ``covered_hours`` and ``eligible``
# together, so a mutant that drops ONLY the ``eligible`` comparison term is
# undetected by any of them -- ``eligible`` happens to be a direct function
# of ``covered_hours`` whenever a fixture only adds/removes hours. This
# oracle isolates ``eligible`` alone: the day starts at 18 covered
# temperature hours, exactly the TEMP_TRUTH_MIN_HOURS floor, with hours
# 13-17 and 20 absent so hour 12 is the high window's sole representative,
# holding a NEUTRAL value (not the day's max, which sits at a non-window
# hour). The perturbation removes the peak-window reading and adds back
# the previously-missing neutral hour -- covered_hours nets to the same 18,
# and the max-holding hour is untouched, so only ``peak_window_ok`` (and
# therefore ``eligible``) flips.
# ---------------------------------------------------------------------------


def test_eligible_only_divergence_isolated_from_covered_hours_is_caught() -> None:
    conn = asof_conn()
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('gate-town-eligible', 40.0, -105.0, 900.0, 'UTC')
        """
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    generation_id = ensure_published_generation(conn, site_id)
    day = "2026-06-10"

    max_hour = 21  # outside the high-peak window [12, 18) -- holds the max
    peak_hour = 12  # the high-peak window's ONLY reading -- a NEUTRAL value
    low_hour = 6  # inside the low-peak window [3, 9)
    skipped_hour = 20  # missing at baseline; added back by the perturbation
    # Every OTHER hour inside the high window [12, 18) -- 13, 14, 15, 16, 17
    # -- is absent from the start, so peak_hour is the window's sole
    # representative and removing it alone flips peak_window_ok.
    absent_hours = {13, 14, 15, 16, 17, skipped_hour}
    for hour in range(24):
        if hour in absent_hours:
            continue
        valid_at = f"{day}T{hour:02d}:00:00Z"
        if hour == low_hour:
            temp = 9.0
        elif hour == max_hour:
            temp = 21.0
        else:
            temp = 15.0  # includes peak_hour: present, but not the max
        conn.execute(
            """
            INSERT INTO observations
                (site_id, variable, valid_at, value, n_stations, computed_at)
            VALUES (?, 'temperature', ?, ?, 3, ?)
            """,
            (site_id, valid_at, temp, f"{day}T{hour:02d}:30:00Z"),
        )
        for variable, value in (("wind", 6.0), ("precip", 0.0)):
            conn.execute(
                """
                INSERT INTO observations
                    (site_id, variable, valid_at, value, n_stations, computed_at)
                VALUES (?, ?, ?, ?, 3, ?)
                """,
                (site_id, variable, valid_at, value, f"{day}T{hour:02d}:30:00Z"),
            )
    materialize_daily_truth(
        conn, site_id=site_id, local_date=day, tz_generation_id=generation_id
    )
    conn.commit()

    baseline = conn.execute(
        """
        SELECT value, eligible, covered_hours FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND quantity = 'temperature_high'
        """,
        (site_id, day),
    ).fetchone()
    assert baseline is not None
    # Positive control: with the peak-window reading present, the day starts
    # eligible at 18 covered hours (the TEMP_TRUTH_MIN_HOURS floor, exactly)
    # and the max at the non-window hour.
    assert int(baseline["eligible"]) == 1
    assert int(baseline["covered_hours"]) == 18
    assert float(baseline["value"]) == 21.0

    station_id = _make_station(conn, site_id, "gate-station-eligible-isolate")
    # Add back the previously-missing neutral hour FIRST (this station's own
    # observation history is still empty, so no spike comparison fires) --
    # then remove the peak-window reading, so covered_hours nets to the same
    # 18 as before rather than transiently dropping to 17 -- which would
    # fall below TEMP_TRUTH_MIN_HOURS and flip ``eligible`` for a second,
    # confounding reason.
    added = insert_station_observation(
        conn,
        station_id=station_id,
        variable="temperature",
        valid_at=f"{day}T{skipped_hour:02d}:00:00Z",
        value=15.0,
        source_raw="{}",
    )
    assert added
    # Remove the peak-window reading from consensus (qc_flag='range'). Its
    # own valid_at (12:00) precedes the just-added 20:00 reading, so the
    # spike check's "previous reading" lookup (valid_at < this one) does not
    # see it either.
    removed = insert_station_observation(
        conn,
        station_id=station_id,
        variable="temperature",
        valid_at=f"{day}T{peak_hour:02d}:00:00Z",
        value=999.0,
        source_raw="{}",
    )
    assert removed
    assert _stale_quantities(conn, site_id, day) == {
        "temperature_high",
        "temperature_low",
    }

    divergent = divergent_truth_in_horizon(
        conn,
        site_id=site_id,
        tz_generation_id=generation_id,
        period_start=day,
        period_end=day,
    )
    assert divergent == [(day, "temperature_high")]
    stored = conn.execute(
        """
        SELECT value, eligible, covered_hours FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND quantity = 'temperature_high'
        """,
        (site_id, day),
    ).fetchone()
    assert stored is not None
    # The stale, on-disk snapshot is UNCHANGED in value and covered_hours --
    # only eligible would differ on re-derivation, proving this oracle
    # isolates the eligible term rather than piggybacking on the other two.
    assert float(stored["value"]) == 21.0
    assert int(stored["covered_hours"]) == 18
    assert int(stored["eligible"]) == 1

    ev = _evaluate_day(
        conn, site_id=site_id, local_date=day, tz_generation_id=generation_id
    )
    outcome = ev.outcomes["temperature_high"]
    # The re-derivation matches the stored row on value and covered_hours and
    # differs ONLY on eligible -- the isolation this oracle rests on.
    assert outcome.value == 21.0
    assert outcome.covered_hours == 18
    assert outcome.eligible is False


# ---------------------------------------------------------------------------
# Malformed data: a non-coercible observation value raises ValueError from
# _evaluate_day's own coercion, and it must propagate UNTRANSLATED through
# publish_verified_run -- no publish occurs.
# ---------------------------------------------------------------------------


def test_a_non_coercible_observation_value_raises_valueerror_and_blocks_publish() -> (
    None
):
    conn = asof_conn()
    site_id, _feeds, generation_id = _make_site(conn)
    day = "2026-06-01"
    run_id = _drive_until_publish_pending(conn, site_id, {"trigger_date": "2026-06-06"})
    cfg = run_config_from_row(conn, run_id)

    conn.execute(
        """
        UPDATE observations SET value = 'not-a-number'
        WHERE site_id = ? AND variable = 'temperature' AND valid_at = ?
        """,
        (site_id, f"{day}T{_TEMP_HIGH_HOUR:02d}:00:00Z"),
    )
    marked = mark_daily_truth_stale(
        conn,
        site_id=site_id,
        variable="temperature",
        valid_at=f"{day}T{_TEMP_HIGH_HOUR:02d}:00:00Z",
    )
    assert marked == 2

    with pytest.raises(ValueError):
        publish_verified_run(conn, cfg)
    assert published_run_id(conn, site_id) is None
    state = conn.execute(
        "SELECT state FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert state is not None
    assert str(state["state"]) == "running"
