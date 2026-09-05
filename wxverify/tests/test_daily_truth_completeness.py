"""Oracles (plan §8) for DEF-11's daily-truth admission gate
(``docs/plans/2026-09-01-def11-daily-truth-completeness.md``), in two
groups.

**Pure-function oracles (step 2)** exercise
``wxverify.verification.completeness.decide_admission`` directly, with
fixed datetimes and no ``sqlite3``: the plan's eleven -- O1a, O1b, O1c,
O3, O3b, O5, O5b, O6, O7a, O7b, O8 -- plus O8b and O8c, thirteen in this
group. O8b
pins the ``bool(covered_hours) and`` fail-closed guard against Python's
``all([]) is True`` vacuity for the genuinely empty sequence, which O8's
``[0, 0, 0, 0, 0]`` does not exercise. O8c pins the
``max_computed_at is not None`` presence guard against a
``max_computed_at is None or (...)`` mutant, using the separating state
O8's docstring already discloses its own fixture cannot reach: C1-true
with ``max_computed_at is None``.

O3b and O7b additionally each carry an exact-boundary case (added at the
same sign-off) pinning C2's and C3's ``>=`` comparison sense against a
weakened ``>``, at the exact quiescence and deadline boundaries
respectively -- neither boundary is exercised elsewhere in this suite.

§8 defines both O3b and O5b as pure-function oracles and §6.1 files both
in this module; they stay. O5b in particular is the only oracle anywhere
that pins C1-and-C2 being evaluated before C3 (the branch order).

Only the three DST oracles (O1a, O1b, O1c) need a real expected-slot
count from a real zone, via ``local_day_bounds``.

**Integration oracles (step 4)** drive
``wxverify.verification.truth.materialize_admitted_day`` and
``materialize_missing_truth_days`` against a real, per-test in-memory
``sqlite3`` connection (``run_migrations`` against ``:memory:``): O2, O4,
O9, O10, O16, O17, O18 and O19 (five cases, O19a-O19e -- the D14
``max_computed_at`` string-to-datetime conversion contract). These need a
real connection because the admission decision they pin is reached only
by evaluating the CURRENT rows in ``observations`` and by writing, or
withholding, real ``daily_truth`` rows -- ``decide_admission`` itself,
covered above, never touches either.

Synthetic data only throughout: the timezones used here match the
existing ``tests/test_daily_truth_discovery.py`` convention
(``America/Denver``, ``Etc/GMT+7``) -- never a real place, and none is
needed by site name or coordinate since neither ``decide_admission`` nor
``materialize_admitted_day`` reads one beyond a synthetic site row.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, date, datetime, timedelta

import pytest

from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.completeness import (
    ADMISSION_COMPLETE,
    ADMISSION_DEADLINE,
    decide_admission,
)
from wxverify.verification.coverage import (
    DayBounds,
    evaluate_variable,
    evaluate_wind,
    local_day_bounds,
)
from wxverify.verification.truth import (
    mark_daily_truth_stale,
    materialize_admitted_day,
    materialize_daily_truth,
    materialize_missing_truth_days,
    regenerate_marked_truth_chunk,
)

TZ_DENVER = "America/Denver"
TZ_GMT7 = "Etc/GMT+7"

# A fixed non-DST day end shared by every oracle that does not need a real
# DST transition (O3, O3b, O5, O5b, O6, O7a, O7b, O8, O8b). 24 expected slots.
_DAY_END = datetime(2026, 6, 2, 0, 0, tzinfo=UTC)
_EXPECTED = 24


# ---------------------------------------------------------------------------
# O1a/O1b/O1c -- expected_slots is consumed from a real DST-aware bound,
# never a literal 24 (D1).
# ---------------------------------------------------------------------------


def test_o1a_spring_forward_23_expected_admits_complete_on_23_covered() -> None:
    """O1a -> at ``decision.admit``: correct = True, mutant = False. A
    literal-24 comparison makes 23 == 24 false, so a genuinely complete
    23-hour spring-forward day is deferred instead of admitted -- every
    March."""
    bounds = local_day_bounds(date(2026, 3, 8), TZ_DENVER)
    assert bounds.expected_slots == 23
    now = bounds.end_utc + timedelta(hours=5)  # first nightly pass

    decision = decide_admission(
        day_end_utc=bounds.end_utc,
        expected_slots=bounds.expected_slots,
        covered_hours=[23, 23, 23, 23, 23],
        max_computed_at=now - timedelta(hours=2),  # quiesced
        now=now,
    )

    assert decision.admit is True
    assert decision.basis == ADMISSION_COMPLETE


def test_o1b_fall_back_25_expected_pins_exact_completeness_not_literal_24() -> None:
    """O1b -> at ``admitted.admit`` / ``short.admit``: correct =
    (True, False), mutant = (False, True). A literal-24 comparison
    rejects a genuinely complete 25-hour fall-back day (25 != 24, so it
    defers instead of admitting) and admits a genuinely short one (24 ==
    24, so it wrongly stamps 'complete') -- the DEF-11 shape itself, once
    a year."""
    bounds = local_day_bounds(date(2026, 11, 1), TZ_DENVER)
    assert bounds.expected_slots == 25
    now = bounds.end_utc + timedelta(hours=5)
    quiesced_at = now - timedelta(hours=2)

    admitted = decide_admission(
        day_end_utc=bounds.end_utc,
        expected_slots=bounds.expected_slots,
        covered_hours=[25, 25, 25, 25, 25],
        max_computed_at=quiesced_at,
        now=now,
    )
    assert admitted.admit is True
    assert admitted.basis == ADMISSION_COMPLETE

    short = decide_admission(
        day_end_utc=bounds.end_utc,
        expected_slots=bounds.expected_slots,
        covered_hours=[24, 24, 24, 24, 24],
        max_computed_at=quiesced_at,
        now=now,
    )
    assert short.admit is False
    assert short.basis is None


def test_o1c_fixed_offset_zone_control_admits_complete_at_24_of_24() -> None:
    """O1c -> at ``decision.admit``: pinned = True. Positive control for a
    zone with no DST transitions at all -- ``decide_admission`` never
    receives a timezone, only the caller-derived ``expected_slots``, so
    this proves the ordinary path works uniformly rather than only for
    zones that happen to observe DST. No mutant of this module diverges
    from O1a/O1b's literal-24 mutant here (24 == 24 either way); see the
    mutation ledger in the QA report -- this oracle is a regression
    control, not an independent kill."""
    bounds = local_day_bounds(date(2026, 6, 1), TZ_GMT7)
    assert bounds.expected_slots == 24
    now = bounds.end_utc + timedelta(hours=5)

    decision = decide_admission(
        day_end_utc=bounds.end_utc,
        expected_slots=bounds.expected_slots,
        covered_hours=[24, 24, 24, 24, 24],
        max_computed_at=now - timedelta(hours=2),
        now=now,
    )

    assert decision.admit is True
    assert decision.basis == ADMISSION_COMPLETE


# ---------------------------------------------------------------------------
# O3/O3b -- quiescence (C2) is required, and its window is pinned exactly.
# ---------------------------------------------------------------------------


def test_o3_exact_coverage_alone_defers_without_quiescence() -> None:
    """O3 -> at ``decision.admit``: correct = False, mutant = True on a
    C1-only implementation that ignores C2 entirely. This is exactly the
    2026-08-18 live incident: precip complete at 23:14 local, then
    revised at 02:35 local -- a C1-only gate would have admitted it at
    23:14."""
    now = _DAY_END + timedelta(hours=5)

    decision = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=[_EXPECTED] * 5,
        max_computed_at=now - timedelta(minutes=30),  # not quiesced
        now=now,
    )

    assert decision.admit is False
    assert decision.basis is None


def test_o3b_quiescence_window_pinned_in_both_directions() -> None:
    """O3b -> at ``just_short.admit`` / ``just_past.admit`` /
    ``exact_boundary.admit``: correct = (False, True, True), mutant =
    (False, False, False) if the quiescence window is widened toward a
    full ingest round (180-200 min at defaults -- the D13 alternative
    this plan rejects). The ``admit`` half at 65 minutes is the
    load-bearing assertion: O3's 30-minute case defers under a widened
    window too and cannot detect the widening, but 65 minutes clears the
    pinned 60-minute constant while still falling well inside any widened
    one.

    ``exact_boundary`` additionally pins the comparison sense at C2's
    boundary itself: at exactly ``timedelta(hours=1)`` of silence, ``>=``
    admits and a weakened ``>`` would defer. Neither ``just_short`` (55
    min, inside the window either sense) nor ``just_past`` (65 min,
    outside the window either sense) crosses the exact boundary, so only
    this case can catch the weakened comparison."""
    now = _DAY_END + timedelta(hours=5)
    covered = [_EXPECTED] * 5

    just_short = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=covered,
        max_computed_at=now - timedelta(minutes=55),
        now=now,
    )
    assert just_short.admit is False
    assert just_short.basis is None

    just_past = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=covered,
        max_computed_at=now - timedelta(minutes=65),
        now=now,
    )
    assert just_past.admit is True
    assert just_past.basis == ADMISSION_COMPLETE

    exact_boundary = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=covered,
        max_computed_at=now - timedelta(hours=1),
        now=now,
    )
    assert exact_boundary.admit is True
    assert exact_boundary.basis == ADMISSION_COMPLETE


# ---------------------------------------------------------------------------
# O5/O5b -- C1 and C2 are both required, and evaluated before C3 (branch
# order, D3).
# ---------------------------------------------------------------------------


def test_o5_incomplete_coverage_defers_despite_quiescence() -> None:
    """O5 -> at ``decision.admit``: correct = False, mutant = True on a
    ``C1 or C2`` implementation, which admits a short day the instant it
    goes quiet regardless of how much of it is missing."""
    now = _DAY_END + timedelta(hours=8)

    decision = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=[_EXPECTED - 1] * 5,  # 23 of 24
        max_computed_at=now - timedelta(hours=6),  # quiesced
        now=now,
    )

    assert decision.admit is False
    assert decision.basis is None


def test_o5b_complete_and_quiesced_wins_over_an_elapsed_deadline() -> None:
    """O5b -> at ``decision.basis``: correct = 'complete', mutant =
    'deadline' if the deadline (C3) is tested before C1-and-C2. That
    would stamp a genuinely complete but late-evaluated day -- a backlog
    recovery, or a discovery chunk reaching a day long after it closed --
    as 'deadline', destroying the column's meaning for every such day.
    This is the only oracle in this suite that pins branch order rather
    than branch presence."""
    now = _DAY_END + timedelta(hours=100)  # far past the 24h deadline

    decision = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=[_EXPECTED] * 5,
        max_computed_at=now - timedelta(hours=2),  # quiesced
        now=now,
    )

    assert decision.admit is True
    assert decision.basis == ADMISSION_COMPLETE


# ---------------------------------------------------------------------------
# O6/O7a/O7b -- the deadline (C3) is bounded, self-clearing, and bracketed
# against the nightly trigger (D7).
# ---------------------------------------------------------------------------


def test_o6_deadline_admits_a_permanently_short_day() -> None:
    """O6 -> at ``decision.admit`` / ``decision.basis``: correct =
    (True, 'deadline'), mutant = (False, None) with the deadline branch
    deleted entirely -- a permanently short day is never materialized and
    the horizon freezes (design question 4)."""
    now = _DAY_END + timedelta(hours=25)

    decision = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=[_EXPECTED - 4] * 5,  # 20 of 24
        max_computed_at=now - timedelta(hours=6),
        now=now,
    )

    assert decision.admit is True
    assert decision.basis == ADMISSION_DEADLINE


def test_o7a_deadline_does_not_fire_on_the_first_nightly_pass() -> None:
    """O7a -> at ``decision.admit``: correct = False, mutant = True under
    ``TRUTH_COMPLETENESS_DEADLINE_HOURS = 4`` -- the deadline would admit
    on the very first nightly pass and the gate would never defer
    anything, leaving DEF-11 unfixed."""
    now = _DAY_END + timedelta(hours=5)  # first nightly pass

    decision = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=[_EXPECTED - 1] * 5,  # 23 of 24
        max_computed_at=now - timedelta(hours=6),
        now=now,
    )

    assert decision.admit is False
    assert decision.basis is None


def test_o7b_deadline_fires_on_the_second_nightly_pass() -> None:
    """O7b -> at ``decision.admit`` / ``decision.basis`` /
    ``exact_boundary.admit``: correct = (True, 'deadline', True), mutant =
    (True, 'deadline', False) under ``TRUTH_COMPLETENESS_DEADLINE_HOURS =
    30`` -- the horizon would slip a third night instead of clearing on
    the second.

    ``exact_boundary`` additionally pins the comparison sense at C3's
    boundary itself: at ``now == day_end_utc + timedelta(hours=24)``
    exactly, ``>=`` admits and a weakened ``>`` would defer. The 29-hour
    case above is one hour past the boundary and cannot catch the
    weakened comparison; only sitting exactly on the deadline can."""
    now = _DAY_END + timedelta(hours=29)  # second nightly pass

    decision = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=[_EXPECTED - 1] * 5,  # 23 of 24
        max_computed_at=now - timedelta(hours=6),
        now=now,
    )

    assert decision.admit is True
    assert decision.basis == ADMISSION_DEADLINE

    exact_boundary = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=[_EXPECTED - 1] * 5,  # 23 of 24
        max_computed_at=_DAY_END + timedelta(hours=24) - timedelta(hours=6),
        now=_DAY_END + timedelta(hours=24),
    )
    assert exact_boundary.admit is True
    assert exact_boundary.basis == ADMISSION_DEADLINE


# ---------------------------------------------------------------------------
# O8/O8b -- a day with no observations at all never raises, is never
# admitted early, and clears on the deadline like any other permanently
# short day.
# ---------------------------------------------------------------------------


def test_o8_zero_observations_defers_then_clears_on_the_deadline() -> None:
    """O8 -> at ``waiting.admit`` / ``expired.admit``: correct =
    (False, True). Guards against an implementation that raises on
    ``max_computed_at is None`` (subtracting ``None`` from ``now``
    without the presence guard), or that treats "no observations ever" as
    quiesced and admits immediately. ``covered_hours = [0, 0, 0, 0, 0]``
    is five evaluated-but-empty quantities, not the empty sequence -- see
    O8b for that case -- so C1 is independently false here regardless of
    C2; the mutation ledger in the QA report proves the raise-on-None
    divergence directly against a scratch copy, since this fixture cannot
    make ``.admit`` diverge from a quiesced-by-default mutant on its own
    (C1 already fails it)."""
    covered = [0, 0, 0, 0, 0]

    waiting = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=covered,
        max_computed_at=None,
        now=_DAY_END + timedelta(hours=5),
    )
    assert waiting.admit is False
    assert waiting.basis is None

    expired = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=covered,
        max_computed_at=None,
        now=_DAY_END + timedelta(hours=25),
    )
    assert expired.admit is True
    assert expired.basis == ADMISSION_DEADLINE


def test_o8b_empty_covered_hours_fails_closed_not_vacuously_true() -> None:
    """O8b pins that C1 means at least one
    evaluated quantity, AND every evaluated quantity complete -- plan §5
    D3's pseudocode states only the second half. Python's
    ``all([]) is True`` would otherwise let a caller that evaluated zero
    quantities sail through C1 vacuously.

    -> at ``decision.admit`` / ``decision.reason``: correct =
    (False, 'no_quantities_evaluated'), mutant = (True, 'complete') if
    ``complete`` drops the ``bool(covered_hours) and`` guard -- an empty
    list would then vacuously satisfy C1 and, once quiesced, admit a
    zero-quantity day as 'complete' instead of deferring it to the
    deadline like any other day with nothing evaluated."""
    now = _DAY_END + timedelta(hours=5)

    decision = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=[],
        max_computed_at=now - timedelta(hours=2),  # quiesced
        now=now,
    )

    assert decision.admit is False
    assert decision.basis is None
    assert decision.reason == "no_quantities_evaluated"


def test_o8c_complete_coverage_with_no_computed_at_still_awaits_quiescence() -> None:
    """O8c -- added on architect direction during DEF-11 step 2 sign-off.
    O8's docstring already discloses that its
    ``covered_hours = [0, 0, 0, 0, 0]`` fixture cannot separate pristine
    from a ``quiesced = max_computed_at is None or (...)`` mutant, because
    C1 already fails that fixture regardless of C2. This oracle supplies
    the missing separating state: C1-true (every quantity exactly
    complete) together with ``max_computed_at is None`` -- a day whose
    coverage is exact but which reports no newest-observation timestamp,
    structurally possible since ``observations.computed_at`` is nullable
    even though production always binds it at write time.

    -> at ``decision.admit`` / ``decision.basis`` / ``decision.reason``:
    correct = (False, None, 'awaiting_quiescence'), mutant = (True,
    'complete', 'complete_and_quiesced') if the presence guard
    ``max_computed_at is not None and (...)`` is flipped to
    ``max_computed_at is None or (...)``. Under the mutant this
    C1-complete, timestamp-absent day is admitted as 'complete' -- the
    one outcome D4 exists to prevent."""
    now = _DAY_END + timedelta(hours=5)  # before the 24h deadline

    decision = decide_admission(
        day_end_utc=_DAY_END,
        expected_slots=_EXPECTED,
        covered_hours=[_EXPECTED] * 5,
        max_computed_at=None,
        now=now,
    )

    assert decision.admit is False
    assert decision.basis is None
    assert decision.reason == "awaiting_quiescence"


# ---------------------------------------------------------------------------
# Integration harness (step 4) -- a real, per-test in-memory sqlite
# connection, migrated fresh. Site names, timezone and coordinates are
# synthetic (matches tests/test_daily_truth_discovery.py's convention).
# ---------------------------------------------------------------------------


def _conn() -> sqlite3.Connection:
    """Fresh fully-migrated in-memory database (real datastore, no mocks)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(
    conn: sqlite3.Connection,
    *,
    timezone: str = TZ_DENVER,
    name: str = "site-alpha",
    rain_threshold_mm: float = 0.2,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                            timezone, rain_threshold_mm)
        VALUES (?, 40.0, -105.0, 900.0, ?, ?)
        """,
        (name, timezone, rain_threshold_mm),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _samples_for_variable(
    conn: sqlite3.Connection,
    site_id: int,
    variable: str,
    bounds: DayBounds,
) -> list[tuple[str, float]]:
    """Re-query one variable's rows inside ``bounds``'s UTC window, the same
    shape ``wxverify.verification.truth._evaluate_day`` reads -- a hand
    copy of that query's day-window predicate, not a shared helper; keep
    it tracking ``truth.py:_evaluate_day`` if that predicate ever changes."""
    start = isoformat_utc(bounds.start_utc)
    end = isoformat_utc(bounds.end_utc)
    rows = conn.execute(
        """
        SELECT valid_at, value FROM observations
        WHERE site_id = ? AND variable = ?
          AND julianday(valid_at) >= julianday(?)
          AND julianday(valid_at) < julianday(?)
        """,
        (site_id, variable, start, end),
    ).fetchall()
    return [(str(row["valid_at"]), float(row["value"])) for row in rows]


def _seed_obs_full_day(
    conn: sqlite3.Connection,
    site_id: int,
    local_date: str,
    *,
    timezone: str = TZ_DENVER,
    skip_offset: int | None = None,
    computed_at: str | None = None,
    computed_at_null: bool = False,
    temperature: float = 10.0,
    wind: float = 5.0,
    precip: float = 0.0,
    rain_threshold_mm: float = 0.2,
) -> DayBounds:
    """Seed one full (or near-complete, with ``skip_offset``) day of hourly
    temperature/wind/precip consensus observations across every UTC hourly
    instant in the local day's window (``local_day_bounds``, never a literal
    24). ``skip_offset`` withholds one hour, 0-indexed from
    ``bounds.start_utc`` -- ``23`` withholds local 23:00 in
    ``America/Denver`` in June, outside both temperature peak windows
    (12-18 for the high, 03-09 for the low), so a near-complete day this
    helper seeds is still eligible on every quantity.

    ``computed_at`` binds one fixed value to every row (the caller's own
    quiescence control); ``computed_at_null=True`` overrides it to a
    literal NULL on every row instead. Neither name a per-row schedule --
    a caller that needs one stamp to diverge from the rest overwrites that
    single row directly with a follow-up ``UPDATE`` (matches O19c/O19e).

    Self-checks that the day it seeded truly reaches the intended coverage
    -- re-querying the rows just written and calling ``evaluate_variable``
    directly, asserting every one of the five quantities is both fully
    covered (``expected_slots`` or ``expected_slots - 1``, matching
    ``skip_offset``) AND eligible -- before returning, so a bounds or
    value mistake in this helper fails loudly in the fixture instead of
    silently weakening every oracle that calls it. Not added without a
    real call site (O2's near-complete form, O4's full form, and every
    integration oracle below).
    """
    day = date.fromisoformat(local_date)
    bounds = local_day_bounds(day, timezone)
    for offset in range(bounds.expected_slots):
        if offset == skip_offset:
            continue
        instant = bounds.start_utc + timedelta(hours=offset)
        valid_at = isoformat_utc(instant)
        if computed_at_null:
            stamp = None
        elif computed_at is not None:
            stamp = computed_at
        else:
            stamp = valid_at
        for variable, value in (
            ("temperature", temperature),
            ("wind", wind),
            ("precip", precip),
        ):
            conn.execute(
                """
                INSERT INTO observations (site_id, variable, valid_at, value,
                                           n_stations, computed_at)
                VALUES (?, ?, ?, ?, 3, ?)
                """,
                (site_id, variable, valid_at, value, stamp),
            )
    expected_covered = bounds.expected_slots - (0 if skip_offset is None else 1)
    checked = 0
    for variable in ("temperature", "wind", "precip"):
        samples = _samples_for_variable(conn, site_id, variable, bounds)
        for outcome in evaluate_variable(
            variable,
            samples,
            timezone=timezone,
            local_date=day,
            rain_threshold_mm=rain_threshold_mm,
        ):
            assert outcome.covered_hours == expected_covered, (
                "_seed_obs_full_day self-check failed: "
                f"{outcome.quantity} covered {outcome.covered_hours}, "
                f"expected {expected_covered}"
            )
            assert outcome.eligible is True, (
                "_seed_obs_full_day self-check failed: "
                f"{outcome.quantity} not eligible ({outcome.exclusion_reason})"
            )
            checked += 1
    assert checked == 5, (
        "_seed_obs_full_day self-check ran on "
        f"{checked} quantities, expected 5 -- an unrecognized variable name "
        "would make evaluate_variable's loop above iterate zero times and "
        "vacuously pass"
    )
    return bounds


# ---------------------------------------------------------------------------
# O2 -- exact completeness (C1), not near-complete eligibility, is what
# admission requires.
# ---------------------------------------------------------------------------


def test_o2_near_complete_23_of_24_defers_though_old_eligibility_gate_passes() -> None:
    """O2 -> at ``decision.admit`` / daily_truth row count: correct =
    (False, 0), mutant = (True, 5) under a gate implemented as "admit
    whatever the old §4 eligibility gate already accepts" (near-complete,
    >= expected - 1) rather than admission's own exact-equality C1. The
    anti-vacuity guard below, run BEFORE the count assertion, proves this
    day's near-complete coverage is NOT why it stops: every one of the
    five quantities is independently eligible = True at 23-of-24 under
    §4's own near/exact gates -- so a zero-row result that follows can
    only be C1's exact-equality requirement, never the pre-existing
    eligibility gate."""
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    local_date = "2026-06-02"
    bounds = local_day_bounds(date.fromisoformat(local_date), TZ_DENVER)
    now = bounds.end_utc + timedelta(hours=5)  # first nightly pass
    quiesced_at = isoformat_utc(now - timedelta(hours=2))
    seeded = _seed_obs_full_day(
        conn, site_id, local_date, skip_offset=23, computed_at=quiesced_at
    )
    assert seeded.expected_slots == 24  # anchors the "23 of 24" reading

    # Anti-vacuity guard FIRST (plan §8's own requirement for this oracle):
    # re-derive eligibility independently of the admission call below.
    for variable in ("temperature", "wind", "precip"):
        samples = _samples_for_variable(conn, site_id, variable, seeded)
        for outcome in evaluate_variable(
            variable,
            samples,
            timezone=TZ_DENVER,
            local_date=date.fromisoformat(local_date),
            rain_threshold_mm=0.2,
        ):
            assert outcome.eligible is True

    decision = materialize_admitted_day(
        conn,
        site_id=site_id,
        local_date=local_date,
        tz_generation_id=generation_id,
        now=now,
    )
    assert decision.admit is False
    assert decision.reason == "incomplete_coverage"

    count = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, local_date),
    ).fetchone()["n"]
    assert count == 0


# ---------------------------------------------------------------------------
# O4 -- the ordinary complete-and-quiesced path admits and writes.
# ---------------------------------------------------------------------------


def test_o4_full_coverage_quiesced_admits_complete_with_five_rows() -> None:
    """O4 -> at ``len(rows)`` / each row's ``admission_basis``/``stale``:
    correct = (5, 'complete', 0), mutant = (0, n/a, n/a) under an
    over-strict quiescence window (or an inverted C2 comparison) that
    defers a genuinely complete, ordinarily-quiesced day -- the
    horizon-freezing failure mode this oracle exists to catch."""
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    local_date = "2026-06-02"
    bounds = local_day_bounds(date.fromisoformat(local_date), TZ_DENVER)
    now = bounds.end_utc + timedelta(hours=5)
    quiesced_at = isoformat_utc(now - timedelta(hours=2))
    _seed_obs_full_day(conn, site_id, local_date, computed_at=quiesced_at)

    decision = materialize_admitted_day(
        conn,
        site_id=site_id,
        local_date=local_date,
        tz_generation_id=generation_id,
        now=now,
    )
    assert decision.admit is True
    assert decision.basis == ADMISSION_COMPLETE

    rows = conn.execute(
        "SELECT admission_basis, stale FROM daily_truth "
        "WHERE site_id = ? AND local_date = ?",
        (site_id, local_date),
    ).fetchall()
    assert len(rows) == 5
    assert all(row["admission_basis"] == ADMISSION_COMPLETE for row in rows)
    assert all(row["stale"] == 0 for row in rows)


# ---------------------------------------------------------------------------
# O9 -- regeneration is never gated by admission (D2).
# ---------------------------------------------------------------------------


def test_o9_regeneration_rewrites_a_short_day_ungated_by_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """O9 -> at each rewritten row's ``covered_hours``/``stale``: correct =
    (23, 0), mutant = (24, 1) if ``decide_admission`` were folded into
    ``materialize_daily_truth`` itself (the D2/D13-rejected alternative),
    instantiated here as gating on the day's own freshly-revised
    ``max_computed_at`` as the clock reading. That reading is exactly the
    revision's own computed_at, so quiescence is zero and C1 also fails at
    23-of-24: a gated ``materialize_daily_truth`` would defer, leaving the
    OLD 24-covered-hour values on disk with ``stale`` stuck at 1 forever --
    wedging the timezone-correction rebuild, which regenerates through
    this same function to advance (D2, S7).

    A second, clock-INDEPENDENT pin follows: ``regenerate_marked_truth_chunk``
    must never consult ``decide_admission`` at all, even via a real
    ``utc_now()`` clock read -- ``isoformat_utc()`` returns one with no new
    parameter (``truth.py``'s own ``_write_day`` already calls it that way
    for ``generated_at``), so a real implementer's natural mutant would gate
    regeneration on a live clock rather than on the revised row's own
    ``max_computed_at``. That mutant is invisible to the assertion above
    because this fixture's day is already far past its 24h deadline by any
    real clock read, so C3 admits and regeneration proceeds identically --
    the mutant survives unless the admission gate's own entry point is
    pinned as unreachable, structurally, from this function."""
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    local_date = "2026-06-02"
    bounds = local_day_bounds(date.fromisoformat(local_date), TZ_DENVER)
    initial_computed_at = isoformat_utc(bounds.end_utc + timedelta(hours=1))
    _seed_obs_full_day(conn, site_id, local_date, computed_at=initial_computed_at)
    materialize_daily_truth(
        conn, site_id=site_id, local_date=local_date, tz_generation_id=generation_id
    )
    before = conn.execute(
        "SELECT covered_hours, stale FROM daily_truth "
        "WHERE site_id = ? AND local_date = ?",
        (site_id, local_date),
    ).fetchall()
    assert all(row["covered_hours"] == 24 for row in before)
    assert all(row["stale"] == 0 for row in before)

    dropped_at = isoformat_utc(bounds.start_utc + timedelta(hours=23))
    conn.execute(
        "DELETE FROM observations WHERE site_id = ? AND valid_at = ?",
        (site_id, dropped_at),
    )
    revised_computed_at = isoformat_utc(bounds.end_utc + timedelta(hours=2))
    conn.execute(
        "UPDATE observations SET computed_at = ? WHERE site_id = ?",
        (revised_computed_at, site_id),
    )
    mark_daily_truth_stale(
        conn,
        site_id=site_id,
        variable="wind",
        valid_at=isoformat_utc(bounds.start_utc + timedelta(hours=12)),
    )

    regenerated = regenerate_marked_truth_chunk(conn, site_id=site_id, limit=10)
    assert regenerated == 1

    after = conn.execute(
        "SELECT covered_hours, stale FROM daily_truth "
        "WHERE site_id = ? AND local_date = ?",
        (site_id, local_date),
    ).fetchall()
    assert len(after) == 5
    assert all(row["covered_hours"] == 23 for row in after)
    assert all(row["stale"] == 0 for row in after)

    # Clock-independent structural pin: mark the same day stale again, then
    # explode if regeneration's per-group body ever reaches
    # ``decide_admission`` at all -- proving the gate is structurally
    # unreachable from this path rather than merely unreached by this
    # fixture's particular clock value.
    mark_daily_truth_stale(
        conn,
        site_id=site_id,
        variable="wind",
        valid_at=isoformat_utc(bounds.start_utc + timedelta(hours=12)),
    )
    monkeypatch.setattr(
        "wxverify.verification.truth.decide_admission",
        lambda **_: pytest.fail("regeneration must not consult the admission gate"),
    )
    assert regenerate_marked_truth_chunk(conn, site_id=site_id, limit=10) == 1


# ---------------------------------------------------------------------------
# O10 -- admission_basis is preserved across a regeneration, and never
# fabricated for a pre-gate row (D9).
# ---------------------------------------------------------------------------


def test_o10_regeneration_preserves_the_complete_basis() -> None:
    """O10 (positive) -> at each row's ``admission_basis`` after
    regeneration: correct = 'complete', mutant = None if ``_write_day``'s
    pre-DELETE read of the existing basis (or its preservation branch)
    were dropped -- naive delete-and-recreate would erase the recorded
    admission history on every routine regeneration."""
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    local_date = "2026-06-02"
    bounds = local_day_bounds(date.fromisoformat(local_date), TZ_DENVER)
    now = bounds.end_utc + timedelta(hours=5)
    quiesced_at = isoformat_utc(now - timedelta(hours=2))
    _seed_obs_full_day(conn, site_id, local_date, computed_at=quiesced_at)
    decision = materialize_admitted_day(
        conn,
        site_id=site_id,
        local_date=local_date,
        tz_generation_id=generation_id,
        now=now,
    )
    assert decision.admit is True
    assert decision.basis == ADMISSION_COMPLETE

    mark_daily_truth_stale(
        conn,
        site_id=site_id,
        variable="temperature",
        valid_at=isoformat_utc(bounds.start_utc + timedelta(hours=12)),
    )
    assert regenerate_marked_truth_chunk(conn, site_id=site_id, limit=10) == 1

    rows = conn.execute(
        "SELECT admission_basis FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, local_date),
    ).fetchall()
    assert len(rows) == 5
    assert all(row["admission_basis"] == ADMISSION_COMPLETE for row in rows)


def test_o10_negative_control_pre_gate_null_basis_stays_null_not_complete() -> None:
    """O10 (negative control) -> at each row's ``admission_basis`` after
    regeneration: correct = None, mutant = 'complete' if the preservation
    branch defaulted a missing recorded basis to 'complete' instead of
    leaving it None -- an over-eager preservation rule would fabricate
    admission history for a day that was never gated at all."""
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    local_date = "2026-06-02"
    bounds = local_day_bounds(date.fromisoformat(local_date), TZ_DENVER)
    _seed_obs_full_day(
        conn,
        site_id,
        local_date,
        computed_at=isoformat_utc(bounds.end_utc + timedelta(hours=1)),
    )
    # The pre-gate, ungated path -- never through materialize_admitted_day.
    materialize_daily_truth(
        conn, site_id=site_id, local_date=local_date, tz_generation_id=generation_id
    )
    before = conn.execute(
        "SELECT admission_basis FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, local_date),
    ).fetchall()
    assert all(row["admission_basis"] is None for row in before)

    marked = mark_daily_truth_stale(
        conn,
        site_id=site_id,
        variable="temperature",
        valid_at=isoformat_utc(bounds.start_utc + timedelta(hours=12)),
    )
    stale_count = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id = ? AND stale = 1",
        (site_id,),
    ).fetchone()["n"]
    assert stale_count == marked
    assert regenerate_marked_truth_chunk(conn, site_id=site_id, limit=10) == 1

    after = conn.execute(
        "SELECT admission_basis FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, local_date),
    ).fetchall()
    assert len(after) == 5
    assert all(row["admission_basis"] is None for row in after)


# ---------------------------------------------------------------------------
# O16 -- the §4 near-complete eligibility gate is untouched by DEF-11.
# ---------------------------------------------------------------------------


def test_o16_wind_near_complete_23_of_24_still_eligible() -> None:
    """O16 -> at ``outcome.eligible``: correct = True, mutant = False if
    ``NEAR_COMPLETE_SLOT_ALLOWANCE`` were set to 0 -- someone "fixing"
    DEF-11 by tightening the pre-existing near-complete gate to exact
    equality, which would silently re-classify every historical
    near-complete wind row as ineligible on its next regeneration -- a
    history rewrite disguised as a fix (S6)."""
    local_date = date(2026, 6, 2)
    bounds = local_day_bounds(local_date, TZ_DENVER)
    assert bounds.expected_slots == 24
    samples = [
        (isoformat_utc(bounds.start_utc + timedelta(hours=h)), 5.0)
        for h in range(bounds.expected_slots)
        if h != 23
    ]
    outcome = evaluate_wind(samples, timezone=TZ_DENVER, local_date=local_date)
    assert outcome.covered_hours == 23
    assert outcome.expected_slots == 24
    assert outcome.eligible is True


# ---------------------------------------------------------------------------
# O17 -- a deferred day logs exactly one INFO record; an admitted day logs
# none (operator visibility into an otherwise-silent freeze).
# ---------------------------------------------------------------------------


def test_o17_deferred_day_logs_exactly_one_info_record(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """O17 (positive) -> at the count of INFO records from
    ``wxverify.verification.truth`` naming this site/day: correct = 1,
    mutant = 0 if the deferral branch dropped its log call -- silent
    deferral makes chronic deferral invisible with no container shell to
    investigate."""
    conn = _conn()
    site_id = _make_site(conn)
    local_date = "2026-06-02"
    bounds = local_day_bounds(date.fromisoformat(local_date), TZ_DENVER)
    now = bounds.end_utc + timedelta(hours=5)
    _seed_obs_full_day(
        conn,
        site_id,
        local_date,
        skip_offset=23,
        computed_at=isoformat_utc(now - timedelta(hours=2)),
    )
    caplog.set_level(logging.INFO, logger="wxverify.verification.truth")

    materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=10)

    records = [
        record
        for record in caplog.records
        if record.name == "wxverify.verification.truth"
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    message = records[0].getMessage()
    assert f"site={site_id}" in message
    assert f"local_date={local_date}" in message
    assert "reason=incomplete_coverage" in message


def test_o17_admitted_day_logs_nothing(caplog: pytest.LogCaptureFixture) -> None:
    """O17 (paired negative) -> at the count of records from
    ``wxverify.verification.truth``: correct = 0, mutant >= 1 if an
    admitted day were logged unconditionally alongside deferred ones,
    which would make the deferral log meaningless noise on every routine
    night. Injected precondition: this day is deliberately seeded FULL
    (24-of-24) and quiesced, the admitted twin of the deferred fixture
    above -- the same driver call, the only difference is coverage."""
    conn = _conn()
    site_id = _make_site(conn)
    local_date = "2026-06-02"
    bounds = local_day_bounds(date.fromisoformat(local_date), TZ_DENVER)
    now = bounds.end_utc + timedelta(hours=5)
    _seed_obs_full_day(
        conn, site_id, local_date, computed_at=isoformat_utc(now - timedelta(hours=2))
    )
    caplog.set_level(logging.INFO, logger="wxverify.verification.truth")

    attempted = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=10)

    # Prove the day was actually offered and admitted before asserting silence
    # -- otherwise a window that silently stopped offering this day (zero
    # rows written) would also read as "admitted days log nothing".
    assert attempted == [local_date]
    rows = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, local_date),
    ).fetchone()["n"]
    assert rows == 5

    records = [
        record
        for record in caplog.records
        if record.name == "wxverify.verification.truth"
    ]
    assert records == []


# ---------------------------------------------------------------------------
# O18 -- every day in the window is appended to `attempted`, admitted or
# not (the chunk-exhaustion contract).
# ---------------------------------------------------------------------------


def test_o18_all_days_are_appended_to_attempted_even_when_some_are_deferred() -> None:
    """O18 -> at the return value of ``materialize_missing_truth_days``:
    correct = ``['2026-06-02', '2026-06-03', '2026-06-04']``, mutant =
    a shorter list (excluding the deferred day) if the append moved onto
    only the admit branch -- a window with even one deferred day would
    then return short, which ``materialize_missing_truth_days``'s own
    contract reads as "window exhausted" and would silently corrupt the
    chunk cursor rather than re-offering that day next chunk.

    ``now`` sits 5 h past the LAST day's close -- comfortably past the
    3 h ``CONSENSUS_LAG_HOURS`` settling lag -- which, given 3 calendar
    days spaced 24 h apart and a 24 h deadline, necessarily leaves the
    two EARLIER days already past their own deadlines (structural, and
    general: for any non-newest day ``D_i`` and the newest day ``D_n`` in
    ANY multi-day window, ``end_utc(D_n) >= end_utc(D_i) + 23h`` -- 23h
    being the shortest possible local day -- so ``D_n``'s own window
    membership (``now >= end_utc(D_n) + CONSENSUS_LAG_HOURS``) forces
    ``now >= end_utc(D_i) + 26h > end_utc(D_i) + 24h``, past every
    non-newest day's own deadline for any spacing and any timezone, not
    just this fixture's. "All N deferred" is therefore structurally
    unreachable for any ``N >= 2`` -- even a 2-day window -- a plan
    defect, logged separately. d1 and d2 are
    therefore legitimately admitted ``'deadline'`` -- each with all five
    quantity rows carrying that exact basis, asserted per-day below since
    an aggregate count of ten cannot distinguish "5 and 5" from "10 and
    0" and would stay green even if a future change admitted d1/d2 for
    the wrong reason (e.g. wrongly stamped ``'complete'``) or dropped one
    of their five rows. Only the last day, ``2026-06-04``, is still
    genuinely deferred (before its own deadline, incomplete at 23-of-24)
    -- which is exactly the day an "append only on admit" mutant would
    have to drop from the list to fail the ``result == dates``
    assertion."""
    conn = _conn()
    site_id = _make_site(conn)
    dates = ["2026-06-02", "2026-06-03", "2026-06-04"]
    last_bounds = local_day_bounds(date.fromisoformat(dates[-1]), TZ_DENVER)
    now = last_bounds.end_utc + timedelta(hours=5)
    for local_date in dates:
        _seed_obs_full_day(
            conn,
            site_id,
            local_date,
            skip_offset=23,
            computed_at=isoformat_utc(now - timedelta(hours=2)),
        )

    result = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=3)

    assert result == dates
    for local_date in dates[:2]:
        rows = conn.execute(
            "SELECT admission_basis FROM daily_truth "
            "WHERE site_id = ? AND local_date = ?",
            (site_id, local_date),
        ).fetchall()
        assert len(rows) == 5
        assert all(row["admission_basis"] == ADMISSION_DEADLINE for row in rows)
    last_day_count = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, dates[-1]),
    ).fetchone()["n"]
    assert last_day_count == 0  # 2026-06-04 is genuinely deferred, not yet at deadline


# ---------------------------------------------------------------------------
# O19 -- the D14 max_computed_at string -> datetime conversion contract,
# five cases.
# ---------------------------------------------------------------------------


def test_o19a_well_formed_z_stamps_admit_complete() -> None:
    """O19a (valid) -> at ``decision.admit``/``decision.basis``: correct =
    (True, 'complete'), mutant = raises, or (False, None), if the D14
    conversion of a well-formed ``Z``-suffixed stamp regressed -- the
    ordinary case every other oracle in this file also depends on, stated
    here as its own explicit positive control for the conversion
    contract."""
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    local_date = "2026-06-02"
    bounds = local_day_bounds(date.fromisoformat(local_date), TZ_DENVER)
    now = bounds.end_utc + timedelta(hours=5)
    stamp = isoformat_utc(now - timedelta(hours=2))
    assert stamp.endswith("Z")
    _seed_obs_full_day(conn, site_id, local_date, computed_at=stamp)

    decision = materialize_admitted_day(
        conn,
        site_id=site_id,
        local_date=local_date,
        tz_generation_id=generation_id,
        now=now,
    )
    assert decision.admit is True
    assert decision.basis == ADMISSION_COMPLETE


def test_o19b_null_computed_at_defers_without_exception() -> None:
    """O19b (null) -> at ``decision.admit``/``decision.reason``: correct =
    (False, 'awaiting_quiescence'), mutant = an uncaught ``TypeError``
    propagating out of ``materialize_admitted_day`` if the
    ``max_computed_at is not None`` presence guard were dropped
    (subtracting ``None`` from ``now`` directly) -- a day that has never
    recorded a stamp must defer cleanly, never crash the chunk."""
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    local_date = "2026-06-02"
    bounds = local_day_bounds(date.fromisoformat(local_date), TZ_DENVER)
    now = bounds.end_utc + timedelta(hours=5)  # before the 24h deadline
    _seed_obs_full_day(conn, site_id, local_date, computed_at_null=True)

    decision = materialize_admitted_day(
        conn,
        site_id=site_id,
        local_date=local_date,
        tz_generation_id=generation_id,
        now=now,
    )

    assert decision.admit is False
    assert decision.reason == "awaiting_quiescence"
    count = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id = ? AND local_date = ?",
        (site_id, local_date),
    ).fetchone()["n"]
    assert count == 0


def test_o19c_offset_bearing_stamp_wins_by_instant_not_lexical_string() -> None:
    """O19c (timezone-bearing) -> at ``decision.admit``/``decision.reason``:
    correct = (False, 'awaiting_quiescence'), mutant = (True,
    'complete_and_quiesced') if the running-max fold (or the D14
    conversion) ever compared stored STRINGS instead of parsed instants.
    ``decoy_stamp`` is chronologically 7 h stale (would read as quiesced)
    but a raw string comparison ranks it ABOVE ``true_stamp`` -- which is
    the day's real newest observation, only 30 minutes old (not
    quiesced) -- because ``'3' > '2'`` at the same character position.
    Both assertions below are pinned directly against the two literal
    strings, not just described, so the trap this fixture sets is
    checked rather than assumed."""
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    local_date = "2026-06-02"
    now = datetime(2026, 6, 3, 10, 0, 0, tzinfo=UTC)
    true_stamp = "2026-06-03T02:30:00-07:00"  # == 2026-06-03T09:30:00Z
    decoy_stamp = "2026-06-03T03:00:00Z"  # == 2026-06-03T03:00:00Z
    assert decoy_stamp > true_stamp  # the lexical trap: decoy sorts higher...
    assert parse_utc(decoy_stamp) < parse_utc(true_stamp)  # ...despite being earlier

    _seed_obs_full_day(conn, site_id, local_date, computed_at=true_stamp)
    one_valid_at = isoformat_utc(
        local_day_bounds(date.fromisoformat(local_date), TZ_DENVER).start_utc
    )
    conn.execute(
        "UPDATE observations SET computed_at = ? "
        "WHERE site_id = ? AND variable = 'temperature' AND valid_at = ?",
        (decoy_stamp, site_id, one_valid_at),
    )

    decision = materialize_admitted_day(
        conn,
        site_id=site_id,
        local_date=local_date,
        tz_generation_id=generation_id,
        now=now,
    )
    assert decision.admit is False
    assert decision.reason == "awaiting_quiescence"


def test_o19d_malformed_only_stamp_isolates_to_one_day_with_one_error_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """O19d (malformed) -> at ``attempted`` / ``COUNT(*) FROM daily_truth``
    / the count of ERROR records: correct = (['2026-06-02'], 0, 1), mutant
    = an uncaught ``TypeError`` propagating out of
    ``materialize_missing_truth_days`` entirely (no return value, no
    per-day isolation) if the D14 conversion in
    ``materialize_admitted_day`` were removed and the raw malformed string
    reached ``now - max_computed_at`` inside ``decide_admission``
    unconverted. This is the day's ONLY non-NULL stamp -- the running-max
    fold's own short-circuit (``max_computed_at is None or ...``) never
    calls ``parse_utc`` on the first non-NULL row, so this fixture proves
    the conversion in the CALLER is the guarantee, not the fold."""
    conn = _conn()
    site_id = _make_site(conn)
    local_date = "2026-06-02"
    bounds = local_day_bounds(date.fromisoformat(local_date), TZ_DENVER)
    now = bounds.end_utc + timedelta(hours=5)
    _seed_obs_full_day(conn, site_id, local_date, computed_at_null=True)
    conn.execute(
        "UPDATE observations SET computed_at = 'not-a-timestamp' "
        "WHERE site_id = ? AND variable = 'temperature' AND valid_at = ?",
        (site_id, isoformat_utc(bounds.start_utc)),
    )
    caplog.set_level(logging.ERROR, logger="wxverify.verification.truth")

    attempted = materialize_missing_truth_days(conn, site_id=site_id, now=now, limit=10)

    assert attempted == [local_date]
    count = conn.execute(
        "SELECT COUNT(*) AS n FROM daily_truth WHERE site_id = ?", (site_id,)
    ).fetchone()["n"]
    assert count == 0
    error_records = [
        record
        for record in caplog.records
        if record.name == "wxverify.verification.truth"
        and record.levelno == logging.ERROR
    ]
    assert len(error_records) == 1
    message = caplog.messages[
        next(i for i, r in enumerate(caplog.records) if r is error_records[0])
    ]
    assert f"site={site_id}" in message
    assert local_date in message


def test_o19e_max_selection_is_the_true_maximum_not_the_last_processed_row() -> None:
    """O19e (maximum-selection) -> at ``decision.admit``/``decision.reason``:
    correct = (False, 'awaiting_quiescence'), mutant = (True,
    'complete_and_quiesced') if the running-max fold kept whichever row's
    ``computed_at`` it processed LAST (an assignment) instead of comparing
    against the running max. 23 of the day's 24 rows carry an identical,
    already-quiesced stamp; only one early-``valid_at`` row carries the
    TRUE newest stamp, 30 minutes old (not quiesced). Any last-row-wins
    fold that isn't scanning that one early row last -- which 23 identical
    decoys make overwhelmingly likely regardless of the real scan order --
    settles on the older, quiesced stamp and wrongly reports the day
    quiesced."""
    conn = _conn()
    site_id = _make_site(conn)
    generation_id = ensure_published_generation(conn, site_id)
    local_date = "2026-06-02"
    bounds = local_day_bounds(date.fromisoformat(local_date), TZ_DENVER)
    now = bounds.end_utc + timedelta(hours=5)
    quiesced_stamp = isoformat_utc(now - timedelta(hours=2))  # outside the window
    _seed_obs_full_day(conn, site_id, local_date, computed_at=quiesced_stamp)
    recent_stamp = isoformat_utc(now - timedelta(minutes=30))  # inside the window
    conn.execute(
        "UPDATE observations SET computed_at = ? "
        "WHERE site_id = ? AND variable = 'temperature' AND valid_at = ?",
        (recent_stamp, site_id, isoformat_utc(bounds.start_utc)),
    )

    decision = materialize_admitted_day(
        conn,
        site_id=site_id,
        local_date=local_date,
        tz_generation_id=generation_id,
        now=now,
    )
    assert decision.admit is False
    assert decision.reason == "awaiting_quiescence"
