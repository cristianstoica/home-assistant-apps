"""§7 (W4): the all-feed-mean baseline over the RESOLVED headline roster.

Pass 1 simulates depths, feeds and the cheap baselines; the `resolve` phase
persists the availability-only roster; pass 2 (`baseline`) rebuilds the
all-feed-mean product per cell over exactly the feeds that cleared the
availability floor, copying its truth from the pass-1 row for the same
cell-date (§7 step 4a) rather than re-reading `daily_truth`.

All fixture values are synthetic: a UTC site with invented coordinates and
three fake feeds, one of which publishes wind on only two days.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import asof_conn, asof_make_real_feed
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.engine import (
    PASS1_ROSTER_KEY,
    _stored_aggregate_state,  # noqa: SLF001
    pass1_baseline_feeds,
    prepare_bootstrap_inputs,
    preskipped_verdicts,
)
from wxverify.verification.runs import published_run_id, run_config_from_row
from wxverify.verification.simulate import simulate_baseline_day
from wxverify.worker.verification_run import (
    _compute_verdicts,  # noqa: SLF001
    _load_state,  # noqa: SLF001
    _persist_verdicts,  # noqa: SLF001
    advance_verification,
)

_PERIOD_DAYS = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05"]
_QUANTITY_VALUES = {
    "temperature_high": 21.0,
    "temperature_low": 9.0,
    "wind_max": 6.0,
    "precip_total": 0.0,
    "precip_occurrence": 0.0,
}
#: Per-feed constant sample values. The third feed publishes temperature on
#: every day but wind on only the first two, so it clears the availability
#: floor in temperature cells and falls below it in wind cells -- while
#: still HAVING wind samples on the cell-dates the baseline is checked on,
#: which is what gives the two-feed mean its kill power.
_FEED_BASES = (
    {"temperature": 15.0, "wind": 5.0, "precip": 0.0},
    {"temperature": 15.5, "wind": 5.5, "precip": 0.0},
    {"temperature": 16.0, "wind": 9.0},
)
#: ``(feed index, variable)`` pairs restricted to the first two days.
_SPARSE = {(2, "wind")}
_PAYLOAD: dict[str, object] = {
    "trigger_date": "2026-06-06",
    "snapshot_days_per_chunk": 2,
}


def _make_site(
    conn: sqlite3.Connection, *, wind_span: int | None = None
) -> tuple[int, list[int]]:
    """Seed the standard three-feed site.

    ``wind_span`` overrides the wind sample span for EVERY feed, which is
    how the no-feed-above-the-floor cell is built: the feeds still publish
    wind on the cell-dates they cover, so the cell has data — they simply
    fall below ``ROSTER_AVAILABILITY_FLOOR``.
    """
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('site-alpha', 47.0, 25.0, 900.0, 'UTC')
        """
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    feeds = [
        asof_make_real_feed(conn, "model-alpha"),
        asof_make_real_feed(conn, "model-beta"),
        asof_make_real_feed(conn, "model-gamma"),
    ]
    generation_id = ensure_published_generation(conn, site_id)
    for day in _PERIOD_DAYS:
        for quantity, value in _QUANTITY_VALUES.items():
            conn.execute(
                """
                INSERT INTO daily_truth
                    (site_id, local_date, quantity, value, eligible,
                     covered_hours, expected_slots, wet_hours, dry_hours,
                     rain_threshold_mm, day_start_utc, day_end_utc, timezone,
                     tz_generation_id)
                VALUES (?, ?, ?, ?, 1, 24, 24, ?, ?, 0.2, ?, ?, 'UTC', ?)
                """,
                (
                    site_id,
                    day,
                    quantity,
                    value,
                    0 if quantity.startswith("precip") else None,
                    24 if quantity.startswith("precip") else None,
                    f"{day}T00:00:00Z",
                    f"{day}T23:59:59Z",
                    generation_id,
                ),
            )
    issued = "2026-05-31T05:00:00Z"
    for index, (feed_id, bases) in enumerate(zip(feeds, _FEED_BASES, strict=True)):
        for variable, value in bases.items():
            if wind_span is not None and variable == "wind":
                span = wind_span
            else:
                span = 2 if (index, variable) in _SPARSE else len(_PERIOD_DAYS) + 8
            for day_offset in range(span):
                for hour in range(24):
                    valid = datetime(2026, 6, 1, tzinfo=UTC) + timedelta(
                        hours=day_offset * 24 + hour
                    )
                    conn.execute(
                        """
                        INSERT INTO forecast_samples
                            (site_id, feed_id, variable, issued_at, valid_at,
                             lead_hours, value, source_raw, model_run_id,
                             fetched_at)
                        VALUES (?, ?, ?, ?, ?, 6, ?, '{}', 'run-a', ?)
                        """,
                        (
                            site_id,
                            feed_id,
                            variable,
                            issued,
                            valid.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            value,
                            issued,
                        ),
                    )
    conn.commit()
    return site_id, feeds


def _step(conn: sqlite3.Connection, site_id: int, *, resamples: int = 40) -> bool:
    """One chain step, emulating the async bootstrap phase like the worker."""
    blob = _load_state(conn, site_id)
    if blob is not None and blob.get("phase") == "bootstrap":
        run_id = blob["run_id"]
        assert isinstance(run_id, int)
        cfg = run_config_from_row(conn, run_id)
        verdicts = _compute_verdicts(
            prepare_bootstrap_inputs(conn, cfg), cfg.bootstrap_seed, resamples
        )
        verdicts.extend(preskipped_verdicts(cfg))
        _persist_verdicts(conn, site_id, cfg, verdicts)
        return True
    return advance_verification(conn, site_id, _PAYLOAD)


def _drive(conn: sqlite3.Connection, site_id: int, *, commit: bool = False) -> None:
    for _ in range(300):
        if not _step(conn, site_id):
            if commit:
                conn.commit()
            return
        if commit:
            conn.commit()
    raise AssertionError("verification chain did not terminate")


def _drive_until(conn: sqlite3.Connection, site_id: int, phase: str) -> None:
    for _ in range(300):
        blob = _load_state(conn, site_id)
        if blob is not None and str(blob.get("phase")) == phase:
            return
        if not _step(conn, site_id):
            raise AssertionError(f"chain finished before reaching {phase!r}")
    raise AssertionError(f"chain never reached {phase!r}")


def _roster_cell(conn: sqlite3.Connection, run_id: int, key: str) -> dict[str, object]:
    roster = _stored_aggregate_state(conn, run_id)[PASS1_ROSTER_KEY]
    assert isinstance(roster, dict)
    cell = roster[key]
    assert isinstance(cell, dict)
    return cell


def _members(conn: sqlite3.Connection, run_id: int, key: str) -> set[tuple[str, str]]:
    members = _roster_cell(conn, run_id, key)["members"]
    assert isinstance(members, list)
    out: set[tuple[str, str]] = set()
    for member in members:
        assert isinstance(member, list)
        out.add((str(member[0]), str(member[1])))
    return out


def _baseline_row(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    snapshot: str,
    lead: int,
    variable: str,
    quantity: str,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT * FROM verification_evidence
        WHERE run_id = ? AND snapshot_local_date = ? AND lead = ?
          AND variable = ? AND quantity = ?
          AND entity_type = 'baseline_all_feed_mean'
        """,
        (run_id, snapshot, lead, variable, quantity),
    ).fetchone()


# ---------------------------------------------------------------------------
# §7 steps 2-3 — the resolved roster
# ---------------------------------------------------------------------------


def test_below_floor_feed_is_absent_from_the_resolved_roster() -> None:
    """The sparse-wind feed clears the floor in exactly one cell kind."""
    conn = asof_conn()
    site_id, feeds = _make_site(conn)
    _drive(conn, site_id)
    run_id = published_run_id(conn, site_id)
    assert run_id is not None

    gamma = ("feed", str(feeds[2]))
    # Same lead, same run: present for temperature, absent for wind. The
    # per-cell resolution is the point -- one global roster could not do this.
    assert gamma in _members(conn, run_id, "temperature|1|temperature_high")
    assert gamma not in _members(conn, run_id, "wind|1|wind_max")

    availability = _roster_cell(conn, run_id, "wind|1|wind_max")["availability"]
    assert isinstance(availability, dict)
    # 1 of the 4 truth-eligible target dates -- below the 0.70 floor, but
    # NOT zero: the feed is present on the very cell-date the baseline value
    # is asserted on below, so a roster-blind pass 2 would score differently.
    assert availability[f"feed|{feeds[2]}"] == pytest.approx(0.25)
    assert availability[f"feed|{feeds[0]}"] == 1.0

    # The floor applies only to feeds: depths and the cheap baselines are
    # unconditional members of every cell.
    assert ("depth", "1") in _members(conn, run_id, "wind|1|wind_max")


def test_resolved_roster_drives_the_baseline_feed_map() -> None:
    conn = asof_conn()
    site_id, feeds = _make_site(conn)
    _drive(conn, site_id)
    run_id = published_run_id(conn, site_id)
    assert run_id is not None

    rosters = pass1_baseline_feeds(conn, run_id)
    assert set(rosters[("temperature", 1, "temperature_high")]) == set(feeds)
    assert set(rosters[("wind", 1, "wind_max")]) == {feeds[0], feeds[1]}
    # Leads past the settled truth resolve to no feed at all and are ABSENT
    # from the mapping rather than present-and-empty.
    assert ("wind", 7, "wind_max") not in rosters


# ---------------------------------------------------------------------------
# §7 step 4 — the pass-2 product
# ---------------------------------------------------------------------------


def test_baseline_value_is_the_mean_of_the_resolved_feeds_only() -> None:
    """Three-feed vs two-feed hand-computed means, in the same run."""
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    _drive(conn, site_id)
    run_id = published_run_id(conn, site_id)
    assert run_id is not None

    warm = _baseline_row(
        conn,
        run_id,
        snapshot="2026-06-01",
        lead=1,
        variable="temperature",
        quantity="temperature_high",
    )
    assert warm is not None
    # (15.0 + 15.5 + 16.0) / 3
    assert float(warm["predicted"]) == pytest.approx(15.5)
    assert int(warm["realized_contributors"]) == 3

    windy = _baseline_row(
        conn,
        run_id,
        snapshot="2026-06-01",
        lead=1,
        variable="wind",
        quantity="wind_max",
    )
    assert windy is not None
    # (5.0 + 5.5) / 2. The below-floor feed HAS a 9.0 wind sample on this
    # very cell-date; a roster-blind pass 2 would read 6.5 here.
    assert float(windy["predicted"]) == pytest.approx(5.25)
    assert int(windy["realized_contributors"]) == 2


def test_baseline_quantities_share_one_entity_key() -> None:
    """A variable's quantities are separate rows, not separate entities."""
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    _drive(conn, site_id)
    run_id = published_run_id(conn, site_id)
    assert run_id is not None

    rows = conn.execute(
        """
        SELECT quantity, entity_key FROM verification_evidence
        WHERE run_id = ? AND snapshot_local_date = '2026-06-01' AND lead = 1
          AND variable = 'precip' AND entity_type = 'baseline_all_feed_mean'
        ORDER BY quantity
        """,
        (run_id,),
    ).fetchall()
    assert [str(r["quantity"]) for r in rows] == ["precip_occurrence", "precip_total"]
    assert {str(r["entity_key"]) for r in rows} == {"all_feed_mean"}


def test_baseline_reaches_results_and_bootstrap_inputs() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    _drive(conn, site_id)
    run_id = published_run_id(conn, site_id)
    assert run_id is not None

    results = conn.execute(
        """
        SELECT COUNT(*) AS n FROM verification_results
        WHERE run_id = ? AND entity_type = 'baseline_all_feed_mean'
        """,
        (run_id,),
    ).fetchone()
    assert int(results["n"]) > 0

    cfg = run_config_from_row(conn, run_id)
    inputs = prepare_bootstrap_inputs(conn, cfg)
    temperature = next(i for i in inputs if i.variable == "temperature")
    candidate = temperature.candidates[0]
    assert "baseline_all_feed_mean" in candidate.baseline_continuous
    precip = next(i for i in inputs if i.variable == "precip")
    assert "baseline_all_feed_mean" in precip.candidates[0].baseline_occurrence


def test_common_core_is_a_subset_of_baseline_eligible_dates() -> None:
    """§7 step 5: the core is recomputed with the baseline in the roster."""
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    _drive(conn, site_id)
    run_id = published_run_id(conn, site_id)
    assert run_id is not None

    row = conn.execute(
        """
        SELECT common_days FROM verification_results
        WHERE run_id = ? AND variable = 'temperature' AND lead = 1
          AND quantity = 'temperature_high' AND entity_type = 'depth'
          AND entity_key = '1'
        """,
        (run_id,),
    ).fetchone()
    assert row is not None
    core_days = int(row["common_days"])
    eligible = conn.execute(
        """
        SELECT COUNT(*) AS n FROM verification_evidence
        WHERE run_id = ? AND variable = 'temperature' AND lead = 1
          AND quantity = 'temperature_high'
          AND entity_type = 'baseline_all_feed_mean'
          AND forecast_eligible = 1 AND truth_eligible = 1
          AND predicted IS NOT NULL AND truth_value IS NOT NULL
        """,
        (run_id,),
    ).fetchone()
    assert core_days > 0
    assert core_days <= int(eligible["n"])


# ---------------------------------------------------------------------------
# §7 step 4a — one cell-date, one truth read
# ---------------------------------------------------------------------------


def test_baseline_copies_pass1_truth_across_a_mid_run_truth_change() -> None:
    """Truth mutated between two baseline chunks must not reach pass 2."""
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    _drive_until(conn, site_id, "baseline")
    blob = _load_state(conn, site_id)
    assert blob is not None
    run_id = blob["run_id"]
    assert isinstance(run_id, int)

    source = conn.execute(
        """
        SELECT * FROM verification_evidence
        WHERE run_id = ? AND snapshot_local_date = '2026-06-04'
          AND target_local_date = '2026-06-05' AND lead = 1
          AND variable = 'temperature' AND quantity = 'temperature_high'
          AND entity_type = 'depth' AND entity_key = '1'
        """,
        (run_id,),
    ).fetchone()
    assert source is not None
    assert float(source["truth_value"]) == pytest.approx(21.0)

    # First baseline chunk, then a truth change committed on its own.
    assert _step(conn, site_id)
    conn.commit()
    conn.execute(
        """
        UPDATE daily_truth SET value = 99.0
        WHERE site_id = ? AND local_date = '2026-06-05'
          AND quantity = 'temperature_high'
        """,
        (site_id,),
    )
    conn.commit()
    _drive(conn, site_id, commit=True)

    baseline = _baseline_row(
        conn,
        run_id,
        snapshot="2026-06-04",
        lead=1,
        variable="temperature",
        quantity="temperature_high",
    )
    assert baseline is not None
    for column in (
        "truth_value",
        "truth_eligible",
        "truth_exclusion_reason",
        "truth_covered_hours",
        "truth_wet_hours",
        "truth_dry_hours",
    ):
        assert baseline[column] == source[column]
    assert float(baseline["abs_error"]) == pytest.approx(
        abs(float(baseline["predicted"]) - 21.0)
    )


def test_baseline_fails_closed_when_the_pass1_source_row_is_missing() -> None:
    """No pass-1 row for the cell-date means no baseline row, not a re-read."""
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    _drive_until(conn, site_id, "baseline")
    blob = _load_state(conn, site_id)
    assert blob is not None
    run_id = blob["run_id"]
    assert isinstance(run_id, int)
    cfg = run_config_from_row(conn, run_id)

    conn.execute(
        """
        DELETE FROM verification_evidence
        WHERE run_id = ? AND snapshot_local_date = '2026-06-01'
          AND entity_type = 'depth' AND entity_key = '1'
          AND variable = 'temperature'
        """,
        (run_id,),
    )
    simulate_baseline_day(conn, cfg, "2026-06-01", pass1_baseline_feeds(conn, run_id))
    assert (
        _baseline_row(
            conn,
            run_id,
            snapshot="2026-06-01",
            lead=1,
            variable="temperature",
            quantity="temperature_high",
        )
        is None
    )
    # The untouched wind cell of the same snapshot day still gets its row.
    assert (
        _baseline_row(
            conn,
            run_id,
            snapshot="2026-06-01",
            lead=1,
            variable="wind",
            quantity="wind_max",
        )
        is not None
    )


# ---------------------------------------------------------------------------
# §3.2 — mutate-and-re-save across separate chunk transactions
# ---------------------------------------------------------------------------


def test_new_phases_survive_separate_chunk_transactions() -> None:
    """simulate → resolve → baseline → aggregate, committing every chunk.

    A fresh-dict write in `resolve` or `baseline` drops `run_id`, and the
    chain then cancels silently on every later night with no run at all --
    so the assertion that matters is that a published run row EXISTS.
    """
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    _drive(conn, site_id, commit=True)

    run_id = published_run_id(conn, site_id)
    assert run_id is not None
    run = conn.execute(
        "SELECT state FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert str(run["state"]) == "published"
    baselines = conn.execute(
        """
        SELECT COUNT(*) AS n FROM verification_evidence
        WHERE run_id = ? AND entity_type = 'baseline_all_feed_mean'
        """,
        (run_id,),
    ).fetchone()
    assert int(baselines["n"]) > 0


def test_a_cell_with_no_feed_above_the_floor_writes_no_baseline_row() -> None:
    """§7: an empty resolved roster means NO baseline row, not a mean of nothing.

    Every feed publishes wind on three of the five truth days — a 0.5
    availability rate, below `ROSTER_AVAILABILITY_FLOOR` — so no feed is a
    member of any wind cell, while the same feeds clear the floor in the
    temperature cells of the same run. That is what makes this distinct
    from a cell that simply had no data: the wind cells carry samples,
    depth entities and a strict common core; only the feed roster is empty.

    Kills: a pass 2 that treats an absent roster entry as an empty feed list
    and writes a `baseline_all_feed_mean` row anyway — a mean over zero
    contributors. Such a row joins the cell's member set and collapses the
    strict common core of every other entity in it.

    Scope, stated rather than implied: the gate's per-lead `insufficient`
    detail and its `passed = False` are NOT reachable on this fixture — its
    five-day period drops each wind lead for `thin_data` before the
    baseline check runs — and are pinned at the decision layer instead
    (`test_continuous_branch_requires_all_feed_mean`,
    `test_thin_data_and_absent_baseline_drops_are_distinguishable`). The
    persisted token here is `insufficient_evidence`, which this small
    fixture also yields for the other variables, so it is recorded for
    completeness and is not the discriminating assertion.
    """
    conn = asof_conn()
    site_id, feeds = _make_site(conn, wind_span=3)
    _drive(conn, site_id)
    run_id = published_run_id(conn, site_id)
    assert run_id is not None

    rosters = pass1_baseline_feeds(conn, run_id)
    assert not [key for key in rosters if key[0] == "wind"]
    # Paired positive, same run: the very same feeds DO clear the floor for
    # temperature, so an empty wind roster is a resolution result, not a
    # broken fixture.
    assert set(rosters[("temperature", 1, "temperature_high")]) == set(feeds)

    # (a) no baseline row anywhere in the variable...
    absent = conn.execute(
        """
        SELECT COUNT(*) AS n FROM verification_evidence
        WHERE run_id = ? AND variable = 'wind'
          AND entity_type = 'baseline_all_feed_mean'
        """,
        (run_id,),
    ).fetchone()
    assert int(absent["n"]) == 0
    # ...while temperature's rows are written as usual.
    present = conn.execute(
        """
        SELECT COUNT(*) AS n FROM verification_evidence
        WHERE run_id = ? AND variable = 'temperature'
          AND entity_type = 'baseline_all_feed_mean'
        """,
        (run_id,),
    ).fetchone()
    assert int(present["n"]) > 0

    # (b) the cell's other entities are unaffected: the depth entities are
    # still members and the core is non-empty. The recorded availability is
    # non-zero, which is the fixture's own proof that the feeds had data.
    cell = _roster_cell(conn, run_id, "wind|1|wind_max")
    members = _members(conn, run_id, "wind|1|wind_max")
    assert ("depth", "1") in members
    assert not [m for m in members if m[0] == "feed"]
    availability = cell["availability"]
    assert isinstance(availability, dict)
    rates = [float(str(availability[f"feed|{fid}"])) for fid in feeds]
    assert rates == [0.5, 0.5, 0.5]
    aggregate = _stored_aggregate_state(conn, run_id)["wind|1|wind_max"]
    assert isinstance(aggregate, dict)
    assert aggregate["common_dates"], "the cell must still have a common core"

    # (e) the persisted verdict token for the variable.
    outcome = conn.execute(
        "SELECT outcome FROM verification_verdicts"
        " WHERE run_id = ? AND variable = 'wind'",
        (run_id,),
    ).fetchone()
    assert str(outcome["outcome"]) == "insufficient_evidence"


def test_aggregate_merges_into_the_pass1_roster_and_never_replaces_it() -> None:
    """§7: the roster and the per-cell aggregate entries share one column.

    `aggregate_run` writes its per-cell entries into `aggregate_state`, the
    same column `resolve_pass1_roster` already populated. Two ways that goes
    wrong, and both are pinned here:

    - a fresh `state = {}` in `aggregate_run` deletes `pass1_roster`, so the
      NEXT night's pass 2 finds no roster and every all-feed-mean baseline
      silently disappears from the run;
    - the roster growing a `common_dates` key would give the provisional
      core — computed in pass 1, before any baseline row exists, and
      deliberately discarded there — a slot `_common_dates` can read, and
      the baseline's own coverage would stop constraining the strict common
      core.

    The per-cell entries are checked in the same breath, so a mutation that
    keeps the roster by dropping the aggregate write is not a pass here.
    """
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    _drive(conn, site_id, commit=True)
    run_id = published_run_id(conn, site_id)
    assert run_id is not None

    state = _stored_aggregate_state(conn, run_id)
    roster = state[PASS1_ROSTER_KEY]
    assert isinstance(roster, dict)
    assert roster, "the roster must survive aggregation non-empty"
    for key, cell in roster.items():
        assert isinstance(cell, dict)
        assert set(cell) == {"members", "availability"}, key
    # The per-cell aggregate entries sit BESIDE the roster under the same
    # top-level keys they always had, each carrying its own common core.
    cell_keys = {k for k in state if k != PASS1_ROSTER_KEY and k.count("|") == 2}
    assert cell_keys == set(roster), "roster and aggregate cells must agree"
    for key in cell_keys:
        entry = state[key]
        assert isinstance(entry, dict)
        assert "common_dates" in entry, key
        assert "members" in entry, key
