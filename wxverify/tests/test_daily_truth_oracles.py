"""§18.4/§18.9 oracle families for daily_truth + per-quantity eligibility.

Mutation-verified oracles over plan §4 (ground-truth gates), §5 (the shared
coverage evaluators), §14.2 (daily_truth storage, staleness, regeneration)
and §13 (generation-bound reads). Each test names the production mutation it
kills; kills were performed against the live files and the files restored
byte-identical (shasum-verified). All fixture data is synthetic.

Complements tests/test_daily_truth.py (implementer coverage) — no overlap in
fixture shapes: these probes are built specifically so each named mutant
flips an exact-value assertion.
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
    evaluate_variable,
    evaluate_wind,
)
from wxverify.verification.truth import (
    load_daily_truth,
    mark_daily_truth_stale,
    materialize_daily_truth,
    regenerate_marked_truth,
)

_TZ_BUCHAREST = "Europe/Bucharest"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(
    conn: sqlite3.Connection,
    timezone: str = "UTC",
    rain_threshold_mm: float = 0.2,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites
            (name, forecast_lat, forecast_lon, elevation_m, timezone,
             rain_threshold_mm)
        VALUES ('Oracle Site', 47.0, 25.0, 900.0, ?, ?)
        """,
        (timezone, rain_threshold_mm),
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
        VALUES (?, ?, ?, ?, 3, '2026-06-12T01:00:00Z')
        ON CONFLICT(site_id, variable, valid_at) DO UPDATE SET
            value=excluded.value
        """,
        [(site_id, variable, valid_at, value) for valid_at, value in rows],
    )


def _bucharest_june_local_hours(local_hours: list[int]) -> list[tuple[str, float]]:
    """UTC-stamped samples for the given LOCAL hours of 2026-06-10 (UTC+3)."""
    out: list[tuple[str, float]] = []
    for local_hour in local_hours:
        utc_hour = (local_hour - 3) % 24
        day = "2026-06-09" if local_hour < 3 else "2026-06-10"
        out.append((f"{day}T{utc_hour:02d}:00:00Z", 10.0))
    return out


def _truth_rows(
    conn: sqlite3.Connection, site_id: int, local_date: str
) -> dict[str, sqlite3.Row]:
    return {
        str(row["quantity"]): row
        for row in conn.execute(
            "SELECT * FROM daily_truth WHERE site_id=? AND local_date=?",
            (site_id, local_date),
        )
    }


# --------------------------------------------------------------- O1: 18h gate


def test_temp_min_hours_gate_exact_boundary_bucharest() -> None:
    """18 covered hours eligible, 17 excluded — kills M1 (`<` -> `<=`).

    Non-UTC timezone so the covered-hour count cannot come from a UTC
    calendar-day shortcut. Both probes keep BOTH peak windows covered, so
    the only discriminating gate is the hour count.
    """
    day = date(2026, 6, 10)
    eighteen = _bucharest_june_local_hours(list(range(18)))  # local 00..17
    high, low = evaluate_temperature(eighteen, timezone=_TZ_BUCHAREST, local_date=day)
    assert high.eligible and low.eligible
    assert high.covered_hours == 18

    seventeen = _bucharest_june_local_hours(list(range(17)))  # local 00..16
    high, low = evaluate_temperature(seventeen, timezone=_TZ_BUCHAREST, local_date=day)
    assert not high.eligible
    assert high.exclusion_reason == EXCLUDE_INSUFFICIENT_COVERAGE
    assert not low.eligible
    assert low.exclusion_reason == EXCLUDE_INSUFFICIENT_COVERAGE


# ------------------------------------------------------- O2: peak-window gates


def test_high_peak_window_start_boundary_single_slot() -> None:
    """Exactly one slot at local 12:00 satisfies the high window.

    Kills M2 (window start exclusive: `start <=` -> `start <`) — the sole
    peak slot sits ON the boundary, so the mutant flips high to ineligible.
    """
    day = date(2026, 6, 10)
    # local 00..11 and 19..23 (17 slots outside [12,18)) + exactly local 12.
    hours = list(range(12)) + list(range(19, 24)) + [12]
    high, low = evaluate_temperature(
        _bucharest_june_local_hours(hours), timezone=_TZ_BUCHAREST, local_date=day
    )
    assert high.covered_hours == 18
    assert high.eligible
    assert high.peak_window_ok is True
    assert low.eligible  # local 03..08 covered


def test_high_peak_window_end_exclusive_and_local_not_utc() -> None:
    """Local 18..23 coverage never satisfies the [12,18) high window.

    Kills M3 (end inclusive: `< end` -> `<= end`; local 18 present) AND M4
    (UTC hour used instead of local: local 18..20 are UTC 15..17, inside
    [12,18) in UTC terms — the mutant wrongly grants the peak).
    """
    day = date(2026, 6, 10)
    hours = list(range(12)) + list(range(18, 24))  # 18 slots, none in [12,18)
    high, low = evaluate_temperature(
        _bucharest_june_local_hours(hours), timezone=_TZ_BUCHAREST, local_date=day
    )
    assert high.covered_hours == 18
    assert not high.eligible
    assert high.exclusion_reason == EXCLUDE_MISSING_PEAK_WINDOW
    assert high.peak_window_ok is False
    assert high.value == 10.0  # diagnostic retained
    assert low.eligible


def test_low_peak_window_is_its_own_window() -> None:
    """No local 03..08 coverage excludes the low even when 12..18 is covered.

    Kills M5 (low evaluated against the HIGH window): covered local hours
    include 09..17, so a swapped-window mutant grants the low its peak.
    Paired positive: adding exactly local 03 (window start) flips the low
    eligible — the same probe pins the low window's inclusive start.
    """
    day = date(2026, 6, 10)
    hours = [0, 1, 2] + list(range(9, 24))  # 18 slots; [3,9) empty (9 excluded)
    high, low = evaluate_temperature(
        _bucharest_june_local_hours(hours), timezone=_TZ_BUCHAREST, local_date=day
    )
    assert high.eligible
    assert not low.eligible
    assert low.exclusion_reason == EXCLUDE_MISSING_PEAK_WINDOW

    with_three = [3] + [0, 1] + list(range(9, 24))  # 18 slots, exactly local 3
    high, low = evaluate_temperature(
        _bucharest_june_local_hours(with_three),
        timezone=_TZ_BUCHAREST,
        local_date=day,
    )
    assert low.eligible
    assert low.peak_window_ok is True


# ------------------------------------------------ O3: near-complete boundaries


def test_near_complete_gate_both_sides_of_boundary() -> None:
    """23-of-24 eligible, 22-of-24 excluded.

    Kills M6 (`>=` -> `>` on the near-complete comparison) via the
    23-eligible side and M7 (allowance 1 -> 2) via the 22-excluded side.
    """
    day = date(2026, 6, 10)
    twenty_three = [(f"2026-06-10T{h:02d}:00:00Z", 3.0 + h) for h in range(23)]
    outcome = evaluate_wind(twenty_three, timezone="UTC", local_date=day)
    assert outcome.covered_hours == 23
    assert outcome.expected_slots == 24
    assert outcome.eligible
    assert outcome.value == 25.0

    twenty_two = twenty_three[:22]
    outcome = evaluate_wind(twenty_two, timezone="UTC", local_date=day)
    assert not outcome.eligible
    assert outcome.exclusion_reason == EXCLUDE_BELOW_NEAR_COMPLETE
    assert outcome.value == 24.0  # diagnostic retained


# --------------------------------------------------------- O4: DST fold + slots


def _fall_back_hours() -> list[str]:
    """The 25 UTC hourly instants of Bucharest local day 2026-10-25."""
    hours = [f"2026-10-24T{h:02d}:00:00Z" for h in range(21, 24)]
    hours += [f"2026-10-25T{h:02d}:00:00Z" for h in range(22)]
    assert len(hours) == 25
    return hours


def test_fold_instants_contribute_independently() -> None:
    """Both UTC instants of the repeated 03:00 wall-clock hour count (§18.4).

    Rain sits ONLY on the two fold instants (2026-10-25T00:00Z and 01:00Z).
    Kills M8 (dedup on local wall-clock: both instants collapse to naive
    03:00, dropping covered to 24, total to 0.7, wet to 1) and M9
    (expected slots hardcoded 24) via the exact expected_slots pins.
    """
    samples = [
        (h, 0.7 if h in ("2026-10-25T00:00:00Z", "2026-10-25T01:00:00Z") else 0.0)
        for h in _fall_back_hours()
    ]
    total, occurrence = evaluate_precip(
        samples,
        timezone=_TZ_BUCHAREST,
        local_date=date(2026, 10, 25),
        rain_threshold_mm=0.2,
    )
    assert total.expected_slots == 25
    assert total.covered_hours == 25
    assert total.eligible
    assert total.value == 1.4  # both fold instants summed
    assert total.wet_hours == 2
    assert total.dry_hours == 23
    assert occurrence.value == 1.0

    # Spring-forward day has 23 expected slots; 22-of-23 passes near-complete.
    spring = [(f"2026-03-28T{h:02d}:00:00Z", 4.0) for h in (22, 23)] + [
        (f"2026-03-29T{h:02d}:00:00Z", 4.0) for h in range(20)
    ]
    wind = evaluate_wind(spring, timezone=_TZ_BUCHAREST, local_date=date(2026, 3, 29))
    assert wind.expected_slots == 23
    assert wind.covered_hours == 22
    assert wind.eligible


def test_25_hour_day_near_complete_needs_24() -> None:
    """On a 25-slot day: 24 covered passes, 23 does not (§18.4)."""
    hours = _fall_back_hours()
    day = date(2026, 10, 25)
    wind_24 = evaluate_wind(
        [(h, 6.0) for h in hours[:24]], timezone=_TZ_BUCHAREST, local_date=day
    )
    assert wind_24.covered_hours == 24
    assert wind_24.eligible
    wind_23 = evaluate_wind(
        [(h, 6.0) for h in hours[:23]], timezone=_TZ_BUCHAREST, local_date=day
    )
    assert not wind_23.eligible
    assert wind_23.exclusion_reason == EXCLUDE_BELOW_NEAR_COMPLETE


# ------------------------------------------------- O5: occurrence asymmetry


def test_occurrence_wet_threshold_inclusive_at_any_coverage() -> None:
    """One slot exactly AT the threshold proves wet at 1-hour coverage.

    Kills M10 (threshold `>=` -> `>`: the exact-boundary slot goes dry)
    and M11 (wet demoted to require near-complete coverage). Paired
    negative: the just-below slot yields the unknowable partial-dry state,
    killing M12 (partial dry accepted as a 0.0 verdict).
    """
    day = date(2026, 6, 10)
    _, occurrence = evaluate_precip(
        [("2026-06-10T07:00:00Z", 0.2)],
        timezone="UTC",
        local_date=day,
        rain_threshold_mm=0.2,
    )
    assert occurrence.eligible
    assert occurrence.value == 1.0
    assert occurrence.wet_hours == 1
    assert occurrence.covered_hours == 1

    _, occurrence = evaluate_precip(
        [("2026-06-10T07:00:00Z", 0.19)],
        timezone="UTC",
        local_date=day,
        rain_threshold_mm=0.2,
    )
    assert not occurrence.eligible
    assert occurrence.exclusion_reason == EXCLUDE_DRY_WITHOUT_NEAR_COMPLETE
    assert occurrence.value is None  # a partial dry day carries no value
    assert occurrence.dry_hours == 1

    # Paired positive for the dry verdict: near-complete dry coverage.
    _, occurrence = evaluate_precip(
        [(f"2026-06-10T{h:02d}:00:00Z", 0.0) for h in range(23)],
        timezone="UTC",
        local_date=day,
        rain_threshold_mm=0.2,
    )
    assert occurrence.eligible
    assert occurrence.value == 0.0


# ------------------------------------- O6: materialization window boundaries


def test_materialize_uses_local_day_bounds_with_boundary_decoys() -> None:
    """Half-open UTC window derived from the LOCAL day, with decoys outside.

    Bucharest 2026-06-10 spans 2026-06-09T21:00Z .. 2026-06-10T21:00Z. A
    +99 decoy one hour before the start and a -99 decoy exactly AT the end
    must both stay out. Kills M14 (SQL start `>=` -> `>`: the exact-start
    slot drops, covered 23) and M15 (bounds computed in UTC instead of the
    site timezone: the window shifts three hours, changing coverage and
    admitting the end decoy). The SQL end predicate (`< julianday(?)`) is
    redundantly guarded by `_hourly_slots`' half-open membership check, so
    its lone mutant is behavior-equivalent; the load-bearing end boundary
    is killed in test_hourly_slot_window_is_half_open_at_both_ends.
    """
    conn = _conn()
    site_id = _make_site(conn, timezone=_TZ_BUCHAREST)
    day = "2026-06-10"
    in_window = [f"2026-06-09T{h:02d}:00:00Z" for h in (21, 22, 23)] + [
        f"2026-06-10T{h:02d}:00:00Z" for h in range(21)
    ]
    assert len(in_window) == 24
    rows = [(stamp, 10.0) for stamp in in_window]
    rows[13] = ("2026-06-10T10:00:00Z", 25.0)  # local 13:00, high peak
    rows[5] = ("2026-06-10T02:00:00Z", 2.0)  # local 05:00, low peak
    decoys = [("2026-06-09T20:00:00Z", 99.0), ("2026-06-10T21:00:00Z", -99.0)]
    _seed_obs(conn, site_id, "temperature", rows + decoys)

    outcomes = materialize_daily_truth(conn, site_id=site_id, local_date=day)

    high = outcomes["temperature_high"]
    low = outcomes["temperature_low"]
    assert high.covered_hours == 24
    assert high.value == 25.0  # +99 decoy excluded
    assert low.value == 2.0  # -99 decoy excluded (end is exclusive)
    assert high.eligible and low.eligible

    persisted = _truth_rows(conn, site_id, day)["temperature_high"]
    assert persisted["day_start_utc"] == "2026-06-09T21:00:00Z"
    assert persisted["day_end_utc"] == "2026-06-10T21:00:00Z"
    assert persisted["timezone"] == _TZ_BUCHAREST


def test_hourly_slot_window_is_half_open_at_both_ends() -> None:
    """A sample exactly AT the day's UTC start counts; exactly AT the end
    does not.

    Exercised through the pure evaluator (the §5 forecast-side path), where
    NO SQL prefilter runs — this is the single load-bearing membership gate
    for caller-assembled samples. Kills M13a (`instant < bounds.end_utc` ->
    `<=`: covered 24 and the 99.0 end decoy becomes the max) and M13b
    (`bounds.start_utc <= instant` -> `<`: the exact-start slot drops,
    covered 22, below near-complete).
    """
    day = date(2026, 6, 10)  # Bucharest: 2026-06-09T21:00Z .. 2026-06-10T21:00Z
    samples = [("2026-06-09T21:00:00Z", 30.0)]  # exactly at start
    samples += [(f"2026-06-09T{h:02d}:00:00Z", 5.0) for h in (22, 23)]
    samples += [(f"2026-06-10T{h:02d}:00:00Z", 5.0) for h in range(20)]
    samples += [("2026-06-10T21:00:00Z", 99.0)]  # exactly at end — excluded
    outcome = evaluate_wind(samples, timezone=_TZ_BUCHAREST, local_date=day)
    assert outcome.covered_hours == 23
    assert outcome.value == 30.0
    assert outcome.eligible


# ------------------------------------------- O7: duplicate instants, last wins


def test_duplicate_hour_instant_last_wins_and_counts_once() -> None:
    """Two samples in the same UTC hour: one slot, last value wins.

    Kills M16 (values accumulated per slot: total would be 7.0) and M17
    (minute truncation dropped: two distinct instants, covered 2).
    """
    total, occurrence = evaluate_precip(
        [("2026-06-10T05:00:00Z", 2.0), ("2026-06-10T05:30:00Z", 5.0)],
        timezone="UTC",
        local_date=date(2026, 6, 10),
        rain_threshold_mm=0.2,
    )
    assert total.covered_hours == 1
    assert total.value == 5.0
    assert occurrence.wet_hours == 1


# --------------------------------------------- O8: staleness marking geometry


def test_stale_marking_boundary_adjacency_variable_and_site_scope() -> None:
    """A mutated hour exactly at a day boundary marks ONLY the day it opens.

    2026-06-11T00:00:00Z == June 11's day_start_utc == June 10's
    day_end_utc for a UTC site. Kills M18 (end-closed match: June 10 also
    marked), M19 (start-open match: June 11 not marked), M20 (variable
    scope dropped: June 11's wind/precip rows marked by a temperature
    mutation) and M21 (site scope dropped: the Bucharest decoy site's
    overlapping day marked).
    """
    conn = _conn()
    site_a = _make_site(conn, timezone="UTC")
    site_b = _make_site(conn, timezone=_TZ_BUCHAREST)
    _seed_obs(
        conn,
        site_a,
        "temperature",
        [(f"2026-06-10T{h:02d}:00:00Z", 10.0) for h in range(24)],
    )
    materialize_daily_truth(conn, site_id=site_a, local_date="2026-06-10")
    materialize_daily_truth(conn, site_id=site_a, local_date="2026-06-11")
    # Site B's local day 2026-06-11 (21:00Z June 10 .. 21:00Z June 11)
    # CONTAINS the mutated instant — only the site filter keeps it clean.
    materialize_daily_truth(conn, site_id=site_b, local_date="2026-06-11")

    marked = mark_daily_truth_stale(
        conn,
        site_id=site_a,
        variable="temperature",
        valid_at="2026-06-11T00:00:00Z",
    )

    assert marked == 2  # exactly temperature_high + temperature_low, June 11
    a_day11 = _truth_rows(conn, site_a, "2026-06-11")
    a_day10 = _truth_rows(conn, site_a, "2026-06-10")
    b_day11 = _truth_rows(conn, site_b, "2026-06-11")
    assert a_day11["temperature_high"]["stale"] == 1
    assert a_day11["temperature_low"]["stale"] == 1
    assert a_day11["wind_max"]["stale"] == 0  # variable scope
    assert a_day11["precip_total"]["stale"] == 0
    assert a_day10["temperature_high"]["stale"] == 0  # boundary: previous day
    assert all(int(row["stale"]) == 0 for row in b_day11.values())  # site scope


def test_stale_marking_compares_instants_not_spellings() -> None:
    """A `+00:00`-spelled instant at exactly day_start still marks the day.

    Kills M22 (julianday dropped for lexical comparison: `'...Z' >
    '...+00:00'` makes the stored day_start compare AFTER the equal
    instant, so the row escapes marking).
    """
    conn = _conn()
    site_id = _make_site(conn, timezone="UTC")
    materialize_daily_truth(conn, site_id=site_id, local_date="2026-06-11")

    marked = mark_daily_truth_stale(
        conn,
        site_id=site_id,
        variable="wind",
        valid_at="2026-06-11T00:00:00+00:00",
    )

    assert marked == 1
    rows = _truth_rows(conn, site_id, "2026-06-11")
    assert rows["wind_max"]["stale"] == 1


# --------------------------------------- O9: consensus funnel, both branches


def test_consensus_marks_on_upsert_and_delete_branches() -> None:
    """`materialize_consensus` marks truth on BOTH write branches.

    Paired positive (stations present -> upsert) and the zero-stations
    DELETE branch. Kills M23 (the mark call demoted to the upsert path
    only — the classic relocation below the early return).
    """
    conn = _conn()
    site_id = _make_site(conn, timezone="UTC")
    day = "2026-06-10"
    _seed_obs(
        conn,
        site_id,
        "temperature",
        [(f"{day}T{h:02d}:00:00Z", 10.0) for h in range(24)],
    )
    materialize_daily_truth(conn, site_id=site_id, local_date=day)
    for i in range(3):
        cur = conn.execute(
            """
            INSERT INTO stations
                (site_id, pws_station_id, lat, lon, dem_elevation_m)
            VALUES (?, ?, 47.0, 25.0, 900.0)
            """,
            (site_id, f"SYNTH{i:03d}"),
        )
        assert cur.lastrowid is not None
        conn.execute(
            """
            INSERT INTO station_observations
                (station_id, variable, valid_at, value, qc_flag)
            VALUES (?, 'temperature', ?, 11.0, 'ok')
            """,
            (int(cur.lastrowid), f"{day}T05:00:00Z"),
        )

    # Upsert branch: three stations feed a consensus value.
    materialize_consensus(
        conn, site_id=site_id, variable="temperature", valid_at=f"{day}T05:00:00Z"
    )
    rows = _truth_rows(conn, site_id, day)
    assert rows["temperature_high"]["stale"] == 1
    assert rows["temperature_low"]["stale"] == 1

    conn.execute("UPDATE daily_truth SET stale = 0")

    # Delete branch: no station rows at this hour -> consensus row deleted.
    materialize_consensus(
        conn, site_id=site_id, variable="temperature", valid_at=f"{day}T06:00:00Z"
    )
    rows = _truth_rows(conn, site_id, day)
    assert rows["temperature_high"]["stale"] == 1
    assert rows["temperature_low"]["stale"] == 1
    assert rows["wind_max"]["stale"] == 0


# --------------------------------------- O10/O11: generation ownership + reads


def _building_generation(conn: sqlite3.Connection, site_id: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO timezone_generations (site_id, timezone, mode, state)
        VALUES (?, 'UTC', 'retrospective_correction', 'building')
        """,
        (site_id,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def test_regeneration_rebuilds_only_under_the_rows_own_generation() -> None:
    """A marked building-generation day regenerates as BUILDING rows.

    Kills M24 (regeneration re-materializes under the published pointer:
    the published rows would flip to the new value while the building rows
    stay stale forever).
    """
    conn = _conn()
    site_id = _make_site(conn, timezone="UTC")
    day = "2026-06-10"
    _seed_obs(
        conn,
        site_id,
        "temperature",
        [(f"{day}T{h:02d}:00:00Z", 10.0) for h in range(24)],
    )
    published = ensure_published_generation(conn, site_id)
    materialize_daily_truth(conn, site_id=site_id, local_date=day)
    building = _building_generation(conn, site_id)
    materialize_daily_truth(
        conn, site_id=site_id, local_date=day, tz_generation_id=building
    )

    conn.execute(
        "UPDATE observations SET value=31.0 WHERE valid_at=?",
        (f"{day}T14:00:00Z",),
    )
    marked = mark_daily_truth_stale(
        conn, site_id=site_id, variable="temperature", valid_at=f"{day}T14:00:00Z"
    )
    assert marked == 4  # high + low in BOTH generations (own stored bounds)
    # Constructed precondition: only the building generation's day remains
    # marked, so regeneration has exactly one group to rebuild.
    conn.execute(
        "UPDATE daily_truth SET stale=0 WHERE tz_generation_id=?", (published,)
    )

    regenerated = regenerate_marked_truth(conn, site_id=site_id)

    assert regenerated == 1
    by_generation = {
        int(row["tz_generation_id"]): row
        for row in conn.execute(
            "SELECT * FROM daily_truth WHERE quantity='temperature_high'"
        )
    }
    assert set(by_generation) == {published, building}
    assert by_generation[building]["value"] == 31.0  # rebuilt in place
    assert by_generation[building]["stale"] == 0
    assert by_generation[published]["value"] == 10.0  # untouched
    remaining_stale = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE stale=1"
    ).fetchone()
    assert remaining_stale["n"] == 0


def test_load_daily_truth_returns_published_values_not_building() -> None:
    """Reads are generation-bound with VALUE-level discrimination.

    The building generation's rows carry a different truth value (44.0),
    so a generation-unbound read is loud even when row counts alone would
    pass. Kills M25 (published_generation_clause dropped from the load).
    """
    conn = _conn()
    site_id = _make_site(conn, timezone="UTC")
    day = "2026-06-10"
    _seed_obs(
        conn,
        site_id,
        "temperature",
        [(f"{day}T{h:02d}:00:00Z", 10.0) for h in range(24)],
    )
    published = ensure_published_generation(conn, site_id)
    materialize_daily_truth(conn, site_id=site_id, local_date=day)
    conn.execute("UPDATE observations SET value=44.0")
    building = _building_generation(conn, site_id)
    materialize_daily_truth(
        conn, site_id=site_id, local_date=day, tz_generation_id=building
    )
    # Decoy exists on disk with the diverged value.
    decoy = conn.execute(
        """
        SELECT value FROM daily_truth
        WHERE tz_generation_id=? AND quantity='temperature_high'
        """,
        (building,),
    ).fetchone()
    assert decoy["value"] == 44.0

    rows = load_daily_truth(conn, site_id=site_id, local_date=day)

    assert len(rows) == 5
    assert {int(row["tz_generation_id"]) for row in rows} == {published}
    by_quantity = {str(row["quantity"]): row for row in rows}
    assert by_quantity["temperature_high"]["value"] == 10.0


# ------------------------------------------ O12: all five rows always written


def test_sparse_day_persists_all_five_rows_with_diagnostics() -> None:
    """Ineligible outcomes are stored, labeled, and re-materialization is
    idempotent.

    Kills M26 (writer filters to eligible outcomes only: zero rows would
    land for this all-excluded day).
    """
    conn = _conn()
    site_id = _make_site(conn, timezone="UTC")
    day = "2026-06-10"
    _seed_obs(
        conn,
        site_id,
        "temperature",
        [(f"{day}T{h:02d}:00:00Z", float(h)) for h in range(5)],
    )

    materialize_daily_truth(conn, site_id=site_id, local_date=day)
    rows = _truth_rows(conn, site_id, day)

    assert len(rows) == 5
    high = rows["temperature_high"]
    assert high["eligible"] == 0
    assert high["exclusion_reason"] == EXCLUDE_INSUFFICIENT_COVERAGE
    assert high["value"] == 4.0  # diagnostic retained
    assert high["covered_hours"] == 5
    assert rows["temperature_low"]["value"] == 0.0
    wind = rows["wind_max"]
    assert wind["eligible"] == 0
    assert wind["exclusion_reason"] == EXCLUDE_BELOW_NEAR_COMPLETE
    assert wind["value"] is None
    assert wind["covered_hours"] == 0
    total = rows["precip_total"]
    assert total["value"] is None
    assert total["wet_hours"] == 0 and total["dry_hours"] == 0
    occurrence = rows["precip_occurrence"]
    assert occurrence["eligible"] == 0
    assert occurrence["exclusion_reason"] == EXCLUDE_DRY_WITHOUT_NEAR_COMPLETE
    assert occurrence["value"] is None
    assert occurrence["rain_threshold_mm"] == 0.2

    # Delete-and-recreate: a second run replaces, never duplicates.
    materialize_daily_truth(conn, site_id=site_id, local_date=day)
    count = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id=?", (site_id,)
    ).fetchone()
    assert count["n"] == 5


# ---------------------------------------------------- O13: dispatcher fidelity


def test_evaluate_variable_dispatches_to_the_matching_evaluator() -> None:
    """The shared §4/§5 entry point routes each variable to its evaluator.

    Kills M27 (precip routed to the wind evaluator — the outcomes differ
    in quantity names, wet/dry counts, and the occurrence row's presence).
    """
    day = date(2026, 6, 10)
    samples = [(f"2026-06-10T{h:02d}:00:00Z", 0.5) for h in range(24)]

    assert evaluate_variable(
        "precip", samples, timezone="UTC", local_date=day, rain_threshold_mm=0.2
    ) == evaluate_precip(samples, timezone="UTC", local_date=day, rain_threshold_mm=0.2)
    assert evaluate_variable(
        "wind", samples, timezone="UTC", local_date=day, rain_threshold_mm=0.2
    ) == (evaluate_wind(samples, timezone="UTC", local_date=day),)
    assert evaluate_variable(
        "temperature", samples, timezone="UTC", local_date=day, rain_threshold_mm=0.2
    ) == evaluate_temperature(samples, timezone="UTC", local_date=day)
    assert (
        evaluate_variable(
            "humidity", samples, timezone="UTC", local_date=day, rain_threshold_mm=0.2
        )
        == ()
    )
