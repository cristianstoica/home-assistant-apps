"""Pure-function oracles (plan §8) for
``wxverify.verification.completeness.decide_admission`` -- step 2 of
DEF-11 (``docs/plans/2026-09-01-def11-daily-truth-completeness.md``).

Thirteen oracles: the plan's eleven pure-function oracles -- O1a, O1b,
O1c, O3, O3b, O5, O5b, O6, O7a, O7b, O8 -- plus O8b and O8c. O8b pins the
``bool(covered_hours) and`` fail-closed guard against Python's
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

§9 step 2 lists only "O1a-O1c, O3, O5, O6, O7a, O7b, O8", omitting O3b and
O5b -- a plan erratum, already logged for the operator. §8 defines both
as pure-function oracles and §6.1 files both in this module; they stay.
O5b in particular is the only oracle anywhere that pins C1-and-C2 being
evaluated before C3 (the branch order).

Step 4's integration oracles (O2, O4, O9, O10, O16, O17, O18) belong in
this same file per plan §6.1 but need ``materialize_admitted_day``, which
does not exist yet -- they are written when that step lands.

``decide_admission`` is pure (no ``sqlite3``, no I/O, no clock read), so
every oracle below calls it directly with fixed datetimes. Only the three
DST oracles (O1a, O1b, O1c) need a real expected-slot count from a real
zone, via ``local_day_bounds``. Synthetic data only: the timezones used
here match the existing ``tests/test_daily_truth_discovery.py``
convention (``America/Denver``, ``Etc/GMT+7``) -- never a real place, and
none is needed by site name or coordinate since ``decide_admission``
never sees a site.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from wxverify.verification.completeness import (
    ADMISSION_COMPLETE,
    ADMISSION_DEADLINE,
    decide_admission,
)
from wxverify.verification.coverage import local_day_bounds

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
