"""daily_truth materialization + per-quantity eligibility (plan §4/§5/§14.2).

Implementation-level coverage: gate boundaries, DST slot counts (23/24/25),
the asymmetric occurrence rule, consensus-mutation marking, regeneration
under the row's own generation, and generation-bound reads. qa-engineer's
§18.4/§18.9 oracle families build on the same seams. All fixture data is
synthetic.
"""

from __future__ import annotations

import sqlite3
from datetime import date

from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.scoring.consensus import materialize_consensus
from wxverify.verification.coverage import (
    EXCLUDE_BELOW_NEAR_COMPLETE,
    EXCLUDE_DRY_WITHOUT_NEAR_COMPLETE,
    EXCLUDE_INSUFFICIENT_COVERAGE,
    EXCLUDE_MISSING_PEAK_WINDOW,
    evaluate_precip,
    evaluate_temperature,
    local_day_bounds,
)
from wxverify.verification.truth import (
    load_daily_truth,
    mark_daily_truth_stale,
    materialize_daily_truth,
    regenerate_marked_truth,
)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, timezone: str = "UTC") -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('Truth Site', 47.0, 25.0, 900.0, ?)
        """,
        (timezone,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_obs(
    conn: sqlite3.Connection,
    site_id: int,
    variable: str,
    rows: list[tuple[str, float]],
) -> None:
    conn.executemany(
        """
        INSERT INTO observations
            (site_id, variable, valid_at, value, n_stations, computed_at)
        VALUES (?, ?, ?, ?, 3, '2026-06-11T02:00:00Z')
        ON CONFLICT(site_id, variable, valid_at) DO UPDATE SET
            value=excluded.value
        """,
        [(site_id, variable, valid_at, value) for valid_at, value in rows],
    )


def _utc_day(day: str, hours: range, values: list[float]) -> list[tuple[str, float]]:
    return [
        (f"{day}T{hour:02d}:00:00Z", value)
        for hour, value in zip(hours, values, strict=True)
    ]


def _truth_row(
    conn: sqlite3.Connection, site_id: int, local_date: str, quantity: str
) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT * FROM daily_truth
        WHERE site_id=? AND local_date=? AND quantity=?
        """,
        (site_id, local_date, quantity),
    ).fetchone()
    assert row is not None
    return row


# ---------------------------------------------------------------- expected slots


def test_expected_slots_ordinary_and_dst_days() -> None:
    tz = "Europe/Bucharest"
    assert local_day_bounds(date(2026, 6, 10), tz).expected_slots == 24
    assert local_day_bounds(date(2026, 3, 29), tz).expected_slots == 23
    assert local_day_bounds(date(2026, 10, 25), tz).expected_slots == 25
    assert local_day_bounds(date(2026, 6, 10), "UTC").expected_slots == 24


# ------------------------------------------------------------ full-day happy path


def test_full_day_truth_values_and_eligibility() -> None:
    conn = _conn()
    site_id = _make_site(conn)
    day = "2026-06-10"
    temps = [float(h) for h in range(24)]
    _seed_obs(conn, site_id, "temperature", _utc_day(day, range(24), temps))
    _seed_obs(conn, site_id, "wind", _utc_day(day, range(24), [5.0] * 23 + [12.5]))
    precip = [0.0] * 24
    precip[14] = 1.5
    precip[15] = 0.5
    _seed_obs(conn, site_id, "precip", _utc_day(day, range(24), precip))

    outcomes = materialize_daily_truth(conn, site_id=site_id, local_date=day)

    assert outcomes["temperature_high"].value == 23.0
    assert outcomes["temperature_low"].value == 0.0
    assert outcomes["wind_max"].value == 12.5
    assert outcomes["precip_total"].value == 2.0
    assert outcomes["precip_occurrence"].value == 1.0
    assert all(outcome.eligible for outcome in outcomes.values())
    assert all(outcome.covered_hours == 24 for outcome in outcomes.values())
    # Rows persisted per quantity with generation tag + provenance.
    row = _truth_row(conn, site_id, day, "precip_total")
    assert row["eligible"] == 1
    assert row["expected_slots"] == 24
    assert row["tz_generation_id"] is not None
    assert row["timezone"] == "UTC"
    assert row["source_max_computed_at"] == "2026-06-11T02:00:00Z"
    assert row["stale"] == 0


# ------------------------------------------------------------------- temp gates


def test_temperature_gates_first_failing_reason() -> None:
    # 17 covered hours -> insufficient coverage (checked before peak window).
    seventeen = [(f"2026-06-10T{h:02d}:00:00Z", 10.0) for h in range(17)]
    high, low = evaluate_temperature(
        seventeen, timezone="UTC", local_date=date(2026, 6, 10)
    )
    assert not high.eligible
    assert high.exclusion_reason == EXCLUDE_INSUFFICIENT_COVERAGE
    assert not low.eligible

    # 18 covered hours 00-17 UTC: high peak window (12-18) satisfied,
    # low peak window (03-09) satisfied too.
    eighteen = [(f"2026-06-10T{h:02d}:00:00Z", 10.0) for h in range(18)]
    high, low = evaluate_temperature(
        eighteen, timezone="UTC", local_date=date(2026, 6, 10)
    )
    assert high.eligible and low.eligible

    # 18 covered hours but none in 12-18 local: hours 00-11 + 18-23.
    no_afternoon = [
        (f"2026-06-10T{h:02d}:00:00Z", 10.0)
        for h in list(range(12)) + list(range(18, 24))
    ]
    high, low = evaluate_temperature(
        no_afternoon, timezone="UTC", local_date=date(2026, 6, 10)
    )
    assert not high.eligible
    assert high.exclusion_reason == EXCLUDE_MISSING_PEAK_WINDOW
    assert high.value == 10.0  # diagnostic value retained
    assert low.eligible


def test_peak_window_is_half_open() -> None:
    base = [(f"2026-06-10T{h:02d}:00:00Z", 10.0) for h in range(12)] + [
        (f"2026-06-10T{h:02d}:00:00Z", 10.0) for h in range(18, 24)
    ]
    at_end = base + [("2026-06-10T18:00:00Z", 11.0)]  # duplicate 18:00, still outside
    high, _ = evaluate_temperature(at_end, timezone="UTC", local_date=date(2026, 6, 10))
    assert high.exclusion_reason == EXCLUDE_MISSING_PEAK_WINDOW
    at_start = base + [("2026-06-10T12:00:00Z", 11.0)]
    high, _ = evaluate_temperature(
        at_start, timezone="UTC", local_date=date(2026, 6, 10)
    )
    assert high.eligible


# ------------------------------------------------------------ near-complete gates


def test_near_complete_gate_boundaries() -> None:
    conn = _conn()
    site_id = _make_site(conn)
    day = "2026-06-10"
    # 23 of 24 passes (>= expected - 1); 22 of 24 fails.
    _seed_obs(conn, site_id, "wind", _utc_day(day, range(23), [4.0] * 23))
    outcomes = materialize_daily_truth(conn, site_id=site_id, local_date=day)
    assert outcomes["wind_max"].eligible

    conn.execute("DELETE FROM observations WHERE variable='wind'")
    _seed_obs(conn, site_id, "wind", _utc_day(day, range(22), [4.0] * 22))
    outcomes = materialize_daily_truth(conn, site_id=site_id, local_date=day)
    assert not outcomes["wind_max"].eligible
    assert outcomes["wind_max"].exclusion_reason == EXCLUDE_BELOW_NEAR_COMPLETE
    assert outcomes["wind_max"].value == 4.0  # diagnostic retained
    assert outcomes["precip_total"].exclusion_reason == EXCLUDE_BELOW_NEAR_COMPLETE


# --------------------------------------------------------- occurrence asymmetry


def test_occurrence_wet_at_any_coverage_dry_needs_near_complete() -> None:
    day = date(2026, 6, 10)
    # One qualifying wet hour proves wet regardless of coverage.
    total, occurrence = evaluate_precip(
        [("2026-06-10T09:00:00Z", 0.4)],
        timezone="UTC",
        local_date=day,
        rain_threshold_mm=0.2,
    )
    assert occurrence.eligible and occurrence.value == 1.0
    assert occurrence.wet_hours == 1 and occurrence.dry_hours == 0
    assert not total.eligible  # single-hour sum is never exact truth

    # Sub-threshold slot is dry; partial dry day is unknowable.
    _, occurrence = evaluate_precip(
        [(f"2026-06-10T{h:02d}:00:00Z", 0.1) for h in range(5)],
        timezone="UTC",
        local_date=day,
        rain_threshold_mm=0.2,
    )
    assert not occurrence.eligible
    assert occurrence.exclusion_reason == EXCLUDE_DRY_WITHOUT_NEAR_COMPLETE
    assert occurrence.value is None

    # Near-complete dry day proves dry.
    _, occurrence = evaluate_precip(
        [(f"2026-06-10T{h:02d}:00:00Z", 0.0) for h in range(23)],
        timezone="UTC",
        local_date=day,
        rain_threshold_mm=0.2,
    )
    assert occurrence.eligible and occurrence.value == 0.0
    assert occurrence.dry_hours == 23

    # Threshold boundary is inclusive (matches production wet-share rule).
    _, occurrence = evaluate_precip(
        [("2026-06-10T09:00:00Z", 0.2)],
        timezone="UTC",
        local_date=day,
        rain_threshold_mm=0.2,
    )
    assert occurrence.value == 1.0


# --------------------------------------------------------------------- DST days


def test_fall_back_day_counts_both_fold_instants() -> None:
    conn = _conn()
    site_id = _make_site(conn, timezone="Europe/Bucharest")
    day = "2026-10-25"  # 25-hour local day: 2026-10-24T21:00Z .. 2026-10-25T22:00Z
    hours = [f"2026-10-24T{h:02d}:00:00Z" for h in range(21, 24)] + [
        f"2026-10-25T{h:02d}:00:00Z" for h in range(22)
    ]
    assert len(hours) == 25
    # Both UTC instants of the repeated 03:00 wall-clock hour (00:00Z and
    # 01:00Z on Oct 25) carry rain; each must contribute independently.
    values = [
        0.5 if h in ("2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z") else 0.0
        for h in hours
    ]
    _seed_obs(conn, site_id, "precip", list(zip(hours, values, strict=True)))

    outcomes = materialize_daily_truth(conn, site_id=site_id, local_date=day)

    total = outcomes["precip_total"]
    assert total.expected_slots == 25
    assert total.covered_hours == 25
    assert total.eligible
    assert total.value == 1.0  # both fold instants summed
    assert total.wet_hours == 2

    # 24 of 25 covered still passes near-complete on a 25-hour day.
    conn.execute(
        "DELETE FROM observations WHERE valid_at='2026-10-25T21:00:00Z'",
    )
    outcomes = materialize_daily_truth(conn, site_id=site_id, local_date=day)
    assert outcomes["precip_total"].covered_hours == 24
    assert outcomes["precip_total"].eligible


def test_spring_forward_day_has_23_expected_slots() -> None:
    conn = _conn()
    site_id = _make_site(conn, timezone="Europe/Bucharest")
    day = "2026-03-29"  # 23-hour local day: 2026-03-28T22:00Z .. 2026-03-29T21:00Z
    hours = [f"2026-03-28T{h:02d}:00:00Z" for h in (22, 23)] + [
        f"2026-03-29T{h:02d}:00:00Z" for h in range(21)
    ]
    assert len(hours) == 23
    _seed_obs(conn, site_id, "wind", [(h, 3.0) for h in hours[:22]])

    outcomes = materialize_daily_truth(conn, site_id=site_id, local_date=day)

    wind = outcomes["wind_max"]
    assert wind.expected_slots == 23
    assert wind.covered_hours == 22
    assert wind.eligible  # >= 23 - 1


# ------------------------------------------------- consensus-mutation lifecycle


def test_materialize_consensus_marks_affected_truth_rows() -> None:
    conn = _conn()
    site_id = _make_site(conn)
    day = "2026-06-10"
    _seed_obs(conn, site_id, "temperature", _utc_day(day, range(24), [10.0] * 24))
    _seed_obs(conn, site_id, "wind", _utc_day(day, range(24), [4.0] * 24))
    materialize_daily_truth(conn, site_id=site_id, local_date=day)
    materialize_daily_truth(conn, site_id=site_id, local_date="2026-06-11")

    # No stations -> delete branch; the mark must still fire.
    materialize_consensus(
        conn, site_id=site_id, variable="temperature", valid_at=f"{day}T05:00:00Z"
    )

    stale = {
        str(row["quantity"]): int(row["stale"])
        for row in conn.execute(
            "SELECT quantity, stale FROM daily_truth WHERE local_date=?", (day,)
        )
    }
    assert stale["temperature_high"] == 1
    assert stale["temperature_low"] == 1
    assert stale["wind_max"] == 0  # other variable untouched
    assert stale["precip_total"] == 0
    other_day = conn.execute(
        "SELECT MAX(stale) AS s FROM daily_truth WHERE local_date='2026-06-11'"
    ).fetchone()
    assert other_day["s"] == 0  # neighboring day untouched


def test_regenerate_marked_truth_recomputes_and_clears_stale() -> None:
    conn = _conn()
    site_id = _make_site(conn)
    day = "2026-06-10"
    _seed_obs(conn, site_id, "temperature", _utc_day(day, range(24), [10.0] * 24))
    materialize_daily_truth(conn, site_id=site_id, local_date=day)
    assert _truth_row(conn, site_id, day, "temperature_high")["value"] == 10.0

    # A consensus rewrite changes an hourly value and marks the day.
    conn.execute(
        "UPDATE observations SET value=31.0 WHERE valid_at=?",
        (f"{day}T14:00:00Z",),
    )
    marked = mark_daily_truth_stale(
        conn, site_id=site_id, variable="temperature", valid_at=f"{day}T14:00:00Z"
    )
    assert marked == 2  # high + low only

    regenerated = regenerate_marked_truth(conn)

    assert regenerated == 1
    row = _truth_row(conn, site_id, day, "temperature_high")
    assert row["value"] == 31.0
    assert row["stale"] == 0
    assert regenerate_marked_truth(conn) == 0  # idempotent once clean


def test_mark_ignores_unknown_variable_and_returns_rowcount() -> None:
    conn = _conn()
    site_id = _make_site(conn)
    assert (
        mark_daily_truth_stale(
            conn, site_id=site_id, variable="humidity", valid_at="2026-06-10T05:00:00Z"
        )
        == 0
    )


# --------------------------------------------------------- generation binding


def test_load_daily_truth_is_published_generation_bound() -> None:
    conn = _conn()
    site_id = _make_site(conn)
    day = "2026-06-10"
    _seed_obs(conn, site_id, "temperature", _utc_day(day, range(24), [10.0] * 24))
    published = ensure_published_generation(conn, site_id)
    materialize_daily_truth(conn, site_id=site_id, local_date=day)

    # A building correction generation writes rows alongside; reads must
    # never see them.
    cur = conn.execute(
        """
        INSERT INTO timezone_generations (site_id, timezone, mode, state)
        VALUES (?, 'Europe/Bucharest', 'retrospective_correction', 'building')
        """,
        (site_id,),
    )
    assert cur.lastrowid is not None
    building = int(cur.lastrowid)
    materialize_daily_truth(
        conn, site_id=site_id, local_date=day, tz_generation_id=building
    )

    rows = load_daily_truth(conn, site_id=site_id, local_date=day)
    assert len(rows) == 5
    assert {int(row["tz_generation_id"]) for row in rows} == {published}
    # Both generations' rows exist on disk (build-alongside, never in-place).
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE local_date=?", (day,)
    ).fetchone()
    assert total["n"] == 10


def test_regeneration_preserves_the_rows_own_generation() -> None:
    conn = _conn()
    site_id = _make_site(conn)
    day = "2026-06-10"
    _seed_obs(conn, site_id, "temperature", _utc_day(day, range(24), [10.0] * 24))
    cur = conn.execute(
        """
        INSERT INTO timezone_generations (site_id, timezone, mode, state)
        VALUES (?, 'UTC', 'retrospective_correction', 'building')
        """,
        (site_id,),
    )
    assert cur.lastrowid is not None
    building = int(cur.lastrowid)
    materialize_daily_truth(
        conn, site_id=site_id, local_date=day, tz_generation_id=building
    )
    mark_daily_truth_stale(
        conn, site_id=site_id, variable="temperature", valid_at=f"{day}T05:00:00Z"
    )

    regenerate_marked_truth(conn, site_id=site_id)

    generations = {
        int(row["tz_generation_id"])
        for row in conn.execute(
            "SELECT tz_generation_id FROM daily_truth WHERE quantity=?",
            ("temperature_high",),
        )
    }
    assert building in generations
