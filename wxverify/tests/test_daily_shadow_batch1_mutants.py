"""§15.4 Batch 1: killer tests for mutants M9, M10, M11, M12, M15, M20,
M21, M23 and M25.

Most tests here are direct tests that add to the named §15.3 tests
elsewhere. The M10 Levels, M11 Discarded-resamples and M25 F1 tests in
this file are the named §15.3 tests themselves (see the ledger below).
M21 and M21b are two distinct mutants at two code sites (see the ledger's
M21/M68 and M21b bullets); this file covers M21b's
``daily_shadow._observed`` site directly, via
``test_m21_an_unknown_days_amount_is_not_scored`` below.
Each mutant's own pure function or filesystem-level site is called directly,
rather than driven through the full pipeline: for M12, directly against
``F1Tally.passes()`` at the 90%/95% boundary §15.3's own "F1." test
(:1345) describes, and likewise for the others below. The site under
test is exercised with real inputs and no mock of the code under test,
but without the DB-backed case machinery that mutants whose divergence
needs multiple distinct cases (M22, in
``test_daily_shadow_heldout_wiring.py``) require.

Named §15.3 test coverage and remaining work:

Each entry gives three separate statuses: implemented, reviewed, and
mutation-verified (checked at checkpoint C, §15.4:1448).

- implemented, reviewed; mutation-verified: pending: M9's lighter direct
  kill test. This file pins the same change ("Arm A training window ends
  at K + 1") directly against ``temperature_window``, the one function
  the mutant changes -- a lighter kill at the same site, not a
  re-derivation of the leak test's broader byte-identical guarantee.
  implemented, reviewed; mutation-verified: pending: M9 -> "the
  leak test" itself (§15.3:1347), killed by change (i) under Arm A in
  ``test_daily_shadow_leak.py``.
- implemented, reviewed; mutation-verified: pending: M28 (Arm B
  training window not shifted one day earlier, still ends at K), killed
  by change (i) under Arm B in ``test_daily_shadow_leak.py``.
- implemented, reviewed; mutation-verified: pending: M32 (Arm A
  rebuild's ``known_by`` cutoff removed from ``ArmARebuilder.rebuild``, so
  a station reading fetched after ``T(s)`` is no longer excluded), killed
  by change (iii) under Arm A in ``test_daily_shadow_leak.py``.
- implemented, reviewed; mutation-verified: pending: M34
  (persistence reads the published ``daily_truth`` row of day K whatever
  the arm), killed by change (i) under Arm B and by change (iii) under
  Arm A in ``test_daily_shadow_leak.py``.
- implemented, reviewed; mutation-verified: pending: M150
  (``load_future_samples``'s ``as_of`` cutoff drops only the
  ``fetched_at`` half of its check, leaving ``issued_at`` alone -- a
  sample issued before T(s) but fetched after it is no longer excluded),
  killed by change (ii) in ``test_daily_shadow_leak.py`` (temperature,
  both arms).
- implemented, reviewed; mutation-verified: pending: M150 (rain), the
  same ``as_of``-cutoff change, killed by change (ii) in
  ``tests/test_daily_shadow_leak_rain.py`` (both arms).
- implemented, reviewed; mutation-verified: pending: M11 -> "Discarded
  resamples" (§15.3:1357). Covered by
  ``test_m11_draw_resamples_discard_over_1_percent_through_daily_bounds`` and
  its paired positive
  ``test_m11_draw_resamples_paired_positive_every_lead_on_every_date``, both
  through production ``draw_resamples(30)``, ``TILE_WEIGHTS`` and
  ``LEAD_WEIGHTS``, in addition to the existing direct-``Bounds``
  ``test_m11_discard_over_1_percent_fails`` tests below. The header's
  ``discarded_resamples`` entry (built inside ``_temperature_heldout``) is
  separately pinned, with the count reported, by
  ``test_m11_header_reports_discarded_resamples_per_candidate`` in
  ``tests/test_daily_shadow_heldout_wiring.py``: ``_make_fixture`` scores
  only lead 0 on a single day, so every other scored lead has no case for
  any candidate and every resample is discarded (kept 0, discarded
  10,000) -- a fixture shaped for the header line, not a re-derivation of
  the one-lead-missing-on-29-of-30-dates population above.
- implemented, reviewed; mutation-verified: pending: M16 -> plan row
  :1468 "manifest check skipped". Covered in
  ``tests/test_daily_shadow_batch2_mutants.py`` by
  ``test_m16a_check_note_matches_package_python_version_differs``,
  ``test_m16a_check_note_matches_package_files_differ``, their paired
  positive, and the ``test_m16b_*`` tests.
- implemented, reviewed; mutation-verified: n/a (no mutant row): the
  §15.3:1371 argparse case (``--heldout`` without ``--freeze-note`` or
  without ``--expect-sha256`` exits with an argparse error) is covered
  in ``tests/test_daily_shadow_batch2_mutants.py`` by
  ``test_freeze_enforcement_heldout_without_freeze_note_is_an_argparse_error``
  and
  ``test_freeze_enforcement_heldout_without_expect_sha256_is_an_argparse_error``.
- implemented, reviewed; mutation-verified: pending: M24 -> plan row
  :1477 "``--expect-sha256`` comparison skipped" (not M16, whose row is
  "manifest check skipped"), the hex-digit sentence. Covered in
  ``tests/test_daily_shadow_batch2_mutants.py`` by
  ``test_m24_expect_sha256_off_by_one_hex_digit_is_refused`` and its
  paired positive.
- still owed: atomic outputs (§15.3:1370), last sentence: "A
  counts-only run and then a ``--freeze-prepare`` run on one copy leave
  two separate run directories."
- implemented; reviewed: pending; mutation-verified: pending: M21 (plan
  :1473, an unknown day's amount scored as 0,
  ``daily_method.observed_amount``'s unknown branch) and M68 (plan :1521,
  a rain-nearby day with no contributing station given 0.0 instead of
  ``amount_not_available``) -> "Amount on known days" (§15.3:1411). Both
  are killed by ``test_amount_scored_on_known_days_only`` and its
  fixture-liveness check
  ``test_fixture_station_events_match_the_derived_values`` in
  ``tests/test_daily_shadow_amount_known_days.py``.
- implemented; reviewed: pending; mutation-verified: pending: M21b (plan
  :1474, ``daily_shadow._observed`` returning 0.0 instead of ``None`` for
  an unknown line, which gives that line a served amount loss) ->
  "Amount on known days" (§15.3:1411). Killed by
  ``test_amount_scored_on_known_days_only``'s
  ``d3_line["served_amount_loss"] is None`` assertion in
  ``tests/test_daily_shadow_amount_known_days.py``; also covered
  directly, against ``daily_shadow._observed`` itself, by
  ``test_m21_an_unknown_days_amount_is_not_scored`` in this file.
- implemented; reviewed: pending; mutation-verified: pending: M23 ->
  "The copy changed during the run" (§15.3:1382). The main case
  (``test_the_copy_changes_mid_run_with_no_other_failure``) kills M23's
  whole-block and comparison-only forms alike at its own
  ``assert exit_code == 1``. Case (a)
  (``test_case_a_the_copy_changes_and_the_hook_itself_raises``) never
  reaches the end-of-run check at all -- its hook raises while ``stage``
  is still ``"run"``, and ``error_after_open`` (``daily_shadow.py:1022-
  1035``) re-hashes the copy itself, detects the change and reports stage
  ``copy_changed`` -- so it does not kill M23;
  cases (a) and (b) together pin ``error_after_open``'s §15.3:1382
  behaviour (the "while handling" chain and the unreadable-second-hash
  wording) rather than killing the mutant. Case (b)
  (``test_case_b_the_second_sha256_cannot_be_computed``) additionally
  kills M23's whole-block form at its own ``assert len(calls) >= 2``, but
  does not kill the comparison-only form (equivalent under case (b)'s
  oracle -- see the module docstring in
  ``tests/test_daily_shadow_copy_changed.py``). Their paired positive is
  ``test_paired_positive_an_unmutated_copy_runs_clean``, all in
  ``tests/test_daily_shadow_copy_changed.py``. The main case is also
  covered here by the earlier batch1 test
  ``test_m23_a_copy_mutated_during_the_run_errors_with_copy_changed``
  (hooks ``load_site``, before any record is read) and its paired
  positive ``test_m23_paired_positive_an_unmutated_copy_runs_clean``.
- implemented, reviewed; mutation-verified: pending: M10 (the constant form,
  :183) -> the two existing killers:
  ``test_m10_second_killer_levels_unshifted_upper_at_0_975`` and
  ``test_m10_levels_shifted_tile_is_inconclusive_not_pass`` below, each
  reading the module's own ``LEVEL_TILE``.
- implemented, reviewed; mutation-verified: pending: the explicit-level
  pins added here -- ``test_m10_levels_unshifted_upper_at_0_95``,
  ``test_m10_levels_unshifted_upper_at_0_90`` and
  ``test_m10_levels_shifted_at_0_95_is_pass`` -- pin the percentile
  arithmetic (974.025 at 0.95, 949.05 at 0.90, and PASS with U = -5.975,
  §15.3:1358) at an explicit level, not ``LEVEL_TILE``; they kill no form
  of M10 by themselves.
- implemented, reviewed; mutation-verified: pending: M10 (the
  call-site form, daily_shadow.py:6484, ``level=LEVEL_TILE`` inside the
  heldout tile's ``daily_bounds(...)`` call) -> the ``tile.level == 0.975``
  assertion in ``test_m11_header_reports_discarded_resamples_per_candidate``
  in ``tests/test_daily_shadow_heldout_wiring.py``.
- implemented, reviewed; mutation-verified: n/a (no mutant row): "What
  stage one lacks is named" (§15.3:1390). Covered by
  ``test_temperature_freeze_prepare_not_built_names_the_missing_a_numbers``
  and ``test_rain_freeze_prepare_not_built_names_the_missing_a_numbers``
  in ``tests/test_daily_shadow_not_built.py``, and
  ``test_heldout_not_built_names_a11_and_d2_option_c`` in
  ``tests/test_daily_shadow_heldout_wiring.py``.
- implemented, reviewed; mutation-verified: pending: M22 -> plan row
  :1475 "warm-spell threshold frozen on the first window" ("Rolling
  warm-spell threshold", §15.3:1388). Covered in
  ``tests/test_daily_shadow_heldout_wiring.py`` by
  ``test_m22_warm_spell_threshold_is_not_frozen_on_the_first_window``,
  in both directions (cold-then-warm, and the plan's autumn-to-winter
  case).
- implemented, reviewed; mutation-verified: pending: M25 -> plan row
  :1478 "the ``on_time``-only F1 rule removed", killed by "F1 (38 of 40)"
  (§15.3:1345). Covered by
  ``test_m25_on_time_only_f1_rule_removed_38_of_40_fails_f1``, its
  paired positive
  ``test_m25_paired_positive_38_of_40_with_on_time_at_95_percent_passes``, and
  ``test_m25_f1_below_95_insufficient_through_data_checks`` at the
  ``data_checks``/``candidate_outcome`` level.
- Round-3 rain mutants (plan :1599-1616):
  - implemented, reviewed; mutation-verified: pending: M146 (rain window
    ends at K+1), shown by ``test_leak_change_i_arm_a_window_end`` in
    ``tests/test_daily_shadow_leak_rain.py``.
  - implemented, reviewed; mutation-verified: pending: M147 (rain's
    as-of station-day total counts every known reading of the day,
    ``count = len(fetched)`` in place of
    ``count = bisect_right(fetched, known_by)`` -- the whole-day form),
    shown by ``test_leak_change_iii_new_station_full_vs_asof`` in
    ``tests/test_daily_shadow_leak_rain.py`` and by
    ``test_fault_states_as_of_t_syn4_is_suspect_not_quarantined`` and
    ``test_fault_states_as_of_t_partial_day_twelve_is_not_reliable`` in
    ``tests/test_daily_shadow_fault_states_as_of.py``.
  - implemented, reviewed; mutation-verified: pending: M148 (rain's
    symbol persistence reads ``RainEvents.full``, not the events as of
    T(s)), shown by ``test_leak_change_iii_new_station_full_vs_asof`` in
    ``tests/test_daily_shadow_leak_rain.py``.
  - implemented, reviewed; mutation-verified: pending: M149 (rain's
    amount persistence reads the amounts in full, not as of T(s)), shown
    by ``test_leak_change_iii_new_station_full_vs_asof`` in
    ``tests/test_daily_shadow_leak_rain.py``.
  - implemented, reviewed; mutation-verified: pending: M151
    (``RainEvents.states_at`` builds the fault states from every reading
    up to K, ``known_by=None``), shown by
    ``test_fault_states_as_of_t_syn4_is_suspect_not_quarantined`` and
    ``test_fault_states_as_of_t_partial_day_twelve_is_not_reliable`` in
    ``tests/test_daily_shadow_fault_states_as_of.py`` (in both, day 12's
    readings that are fetched after T would be counted under the
    mutation, making day 12 a reliable qualifying day and quarantining
    SYN-4, so their ``== "suspect"`` assertions would fail; the positive
    control, ``test_fault_states_as_of_t_positive_control_syn4_is_quarantined``,
    already expects quarantined with day 12 known by T either way and
    does not tell M151 apart).
  - implemented, reviewed; mutation-verified: pending: M162 (the
    comparison looks published rows up at the line's snapshot date, not
    its target date), shown by
    ``test_leak_change_i_arm_b_published_rows_inert`` in
    ``tests/test_daily_shadow_leak_rain.py``.
  - implemented, reviewed; mutation-verified: pending: M163 (expected
    divergence from M147: rain's as-of station-day total takes the whole
    day once one reading is known, keeping ``count == 0`` but dropping
    the ``[:count]`` slice -- the slice-only form, distinct from M147's
    whole-day form), shown by
    ``test_fault_states_as_of_t_partial_day_twelve_is_not_reliable`` in
    ``tests/test_daily_shadow_fault_states_as_of.py``.
  - implemented, reviewed; mutation-verified: pending: M152 (expected
    divergence: Run 3, 4 usable cases) and M158 (Run 3, ``unmatched`` differs),
    both shown by ``test_run3_threshold_mismatch_on_d3``; M153 (Run 3d,
    ``threshold_mismatch`` on every case), shown by
    ``test_run3d_site_threshold_change_does_not_affect_reasons``; M154
    (Run 4, D2 usable), M155 (Run 4, ``truth_not_eligible`` 0) and M157
    (Run 4, an amount with 1 case on D4), all shown by
    ``test_run4_missing_stale_and_ineligible_published_rows``; M156
    (Run 5, 4 symbol cases), shown by
    ``test_run5_non_binary_occurrence_value_is_not_eligible``; M159
    (Run 2, ``share_agreeing`` 0.25), shown by
    ``test_run2_disagrees_on_every_case_day``; M160 (``decisions.symbol``
    differs between Run 1 and Run 2), shown by
    ``test_published_comparison_decides_nothing``; M161 (Run 3c), shown
    by ``test_run3c_stale_and_mismatched_gives_threshold_mismatch_first``
    -- all in ``tests/test_daily_shadow_published_comparison.py``.
- still owed (newly identified coverage, not a §15.3 named test):
  ``main()`` handing ``--heldout``'s arguments to ``run()`` --
  ``mode``, ``h_first`` (``DEFAULT_H_FIRST`` without a note, the note's
  ``first_h_date`` with one), the note's ``excluded_h_dates`` and
  ``last_complete``.
"""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import TypeGuard

import pytest

from tests.helpers import asof_make_site
from tests.test_daily_shadow_output_folder import (
    _run_main,  # pyright: ignore[reportPrivateUsage]
)
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast import daily_method
from wxverify.verification import daily_shadow

_PASSING_GUARD: dict[str, object] = {"pass": True}
_PASSING_TEMP_GUARD: dict[str, object] = {
    "temperature_high": {"pass": True},
    "temperature_low": {"pass": True},
}
_BASE_GUARDS: dict[str, object] = {
    "G2": _PASSING_TEMP_GUARD,
    "G4": _PASSING_TEMP_GUARD,
    "G3": _PASSING_GUARD,
    "frost": _PASSING_GUARD,
}
_BASE_CHECKS: dict[str, object] = {"f1_pass": True, "minimums_met": True}


# --- M9: Arm A training window ends at K + 1 ---------------------------------


def test_m9_arm_a_window_ends_at_k_not_k_plus_one() -> None:
    """M9: Arm A window ends at K + 1 -> at the window's end:
    correct = date(2030, 1, 31), mutant = date(2030, 2, 1).
    """
    k_day = date(2030, 1, 31)
    _, end = daily_shadow.temperature_window(k_day, "A")
    assert end == k_day


def test_m9_second_killer_arm_a_window_start_is_k_minus_29() -> None:
    """Second killer (not a paired positive): the window's start is still
    K - 29, and the window is still 30 days -> at ``(end - start).days``:
    correct = 29, mutant = 30 (the mutant shifts only the end forward, so
    this also fails under it).
    """
    k_day = date(2030, 1, 31)
    start, end = daily_shadow.temperature_window(k_day, "A")
    assert start == k_day - timedelta(days=29)
    assert (end - start).days == 29


# --- M10: tile level 0.95 instead of 0.975 -----------------------------------


def _levels_bounds(*, shift: float) -> daily_shadow.Bounds:
    """Reads ``daily_shadow.LEVEL_TILE`` at call time (not a module-level
    default baked in here), so a mutant on the constant still reaches the
    callers of this helper, via ``_levels_bounds_at``."""
    return _levels_bounds_at(shift=shift, level=daily_shadow.LEVEL_TILE)


def test_m10_second_killer_levels_unshifted_upper_at_0_975() -> None:
    """Second killer (not a paired positive): §15.3's "Levels" unshifted
    upper, pinned at the module's own ``LEVEL_TILE`` -> at ``bounds.upper``:
    correct = 986.5125 (at level 0.975), mutant = 974.025 (at level 0.95),
    so this also fails under the mutant.
    """
    bounds = _levels_bounds(shift=0.0)
    assert bounds.kept == 1000
    assert bounds.discarded == 0
    assert bounds.upper == pytest.approx(986.5125, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]


def test_m10_levels_shifted_tile_is_inconclusive_not_pass() -> None:
    """M10: tile level 0.95 instead of 0.975 -> at ``bounds.upper``:
    correct = 6.5125 (U >= 0, at level 0.975), mutant = -5.975 (U < 0, at
    level 0.95); the outcome follows: correct = "INCONCLUSIVE", mutant =
    "PASS".
    """
    bounds = _levels_bounds(shift=980.0)
    assert bounds.point == pytest.approx(-480.5, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]
    assert bounds.upper == pytest.approx(6.5125, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]
    result = daily_shadow.candidate_outcome(
        "X",
        tile=bounds,
        missing=0,
        guards=_BASE_GUARDS,
        twin=None,
        twin_missing=0,
        checks=_BASE_CHECKS,
        flip=False,
        station_set_changed=None,
        db_import_changed=False,
    )
    assert result.outcome == "INCONCLUSIVE"
    assert result.reasons == ("upper_bound_not_negative",)


def _levels_bounds_at(*, shift: float, level: float) -> daily_shadow.Bounds:
    dates = [date(2020, 1, 1) + timedelta(days=i) for i in range(1000)]
    index_lists = [[i] for i in range(1000)]
    deltas = {("q", dates[i], 0): float(i) - shift for i in range(1000)}
    return daily_shadow.daily_bounds(
        deltas,
        index_lists,
        dates=dates,
        quantity_weights={"q": 1.0},
        lead_weights={0: 1.0},
        level=level,
    )


def test_m10_levels_unshifted_upper_at_0_95() -> None:
    """Level pin, not an M10 killer by itself (this reads an explicit
    level, 0.95, not ``LEVEL_TILE``): the same unshifted population's
    ``upper`` at level 0.95 is 974.025 (±1e-9), from ``percentile_ci``'s
    linear interpolation (§15.3:1358, ``verification/stats.py:119-129``).
    """
    bounds = _levels_bounds_at(shift=0.0, level=0.95)
    assert bounds.kept == 1000
    assert bounds.discarded == 0
    assert bounds.upper is not None
    assert math.isclose(bounds.upper, 974.025, rel_tol=0.0, abs_tol=1e-9)


def test_m10_levels_unshifted_upper_at_0_90() -> None:
    """Level pin, not an M10 killer by itself (this reads an explicit
    level, 0.90, not ``LEVEL_TILE``): the same unshifted population's
    ``upper`` at level 0.90 is 949.05 (±1e-9) (§15.3:1358).
    """
    bounds = _levels_bounds_at(shift=0.0, level=0.90)
    assert bounds.kept == 1000
    assert bounds.discarded == 0
    assert bounds.upper is not None
    assert math.isclose(bounds.upper, 949.05, rel_tol=0.0, abs_tol=1e-9)


def test_m10_levels_shifted_at_0_95_is_pass() -> None:
    """Level pin, not an M10 killer by itself (this reads an explicit
    level, 0.95, not ``LEVEL_TILE``): shifted by -980.0, the tile's
    ``upper`` at level 0.95 is -5.975 (±1e-9), which the plan calls out
    as "at 0.95 it would be PASS" (§15.3:1358) -- ``candidate_outcome``
    confirms the outcome.
    """
    bounds = _levels_bounds_at(shift=980.0, level=0.95)
    assert bounds.upper is not None
    assert math.isclose(bounds.upper, -5.975, rel_tol=0.0, abs_tol=1e-9)
    result = daily_shadow.candidate_outcome(
        "X",
        tile=bounds,
        missing=0,
        guards=_BASE_GUARDS,
        twin=None,
        twin_missing=0,
        checks=_BASE_CHECKS,
        flip=False,
        station_set_changed=None,
        db_import_changed=False,
    )
    assert result.outcome == "PASS"


# --- M11: discard rule disabled -----------------------------------------------


def _bounds(*, kept: int, discarded: int) -> daily_shadow.Bounds:
    return daily_shadow.Bounds(
        point=-1.0, lower=-2.0, upper=-0.5, kept=kept, discarded=discarded, level=0.975
    )


def test_m11_discard_over_1_percent_fails() -> None:
    """M11: discard rule disabled -> at ``discard_ok``'s return for 2/100
    discarded: correct = False, mutant = True.
    """
    assert daily_shadow.discard_ok(_bounds(kept=98, discarded=2)) is False


def test_m11_paired_positive_discard_at_1_percent_passes() -> None:
    """Paired positive: exactly 1% discarded (the boundary) still passes."""
    assert daily_shadow.discard_ok(_bounds(kept=99, discarded=1)) is True


def test_m11_discard_over_1_percent_fails_the_candidate_outcome() -> None:
    """M11, at the outcome level: ``discard_ok`` disabled lets
    ``candidate_outcome`` pass a tile whose resamples are over the 1%
    discard limit (daily_shadow.py:3041-3042, via ``discard_ok`` at
    :2235) -> at the result: correct = ``("INSUFFICIENT",
    ("discarded_over_1_percent",))``, mutant (``discard_ok`` always
    ``True``) = ``("PASS", ())``.
    """
    result = daily_shadow.candidate_outcome(
        "X",
        tile=daily_shadow.Bounds(
            point=-1.0, lower=-2.0, upper=-0.5, kept=98, discarded=2, level=0.975
        ),
        missing=0,
        guards=_BASE_GUARDS,
        twin=None,
        twin_missing=0,
        checks=_BASE_CHECKS,
        flip=False,
        station_set_changed=None,
        db_import_changed=False,
    )
    assert result.outcome == "INSUFFICIENT"
    assert result.reasons == ("discarded_over_1_percent",)


def _one_lead_missing_deltas(
    *, dates: list[date], present_date: date, missing_lead: int
) -> dict[tuple[str, date, int], float]:
    deltas: dict[tuple[str, date, int], float] = {}
    for quantity in daily_shadow.TEMPERATURE_QUANTITIES:
        for lead in daily_shadow.SCORED_LEADS:
            if lead == missing_lead:
                deltas[(quantity, present_date, lead)] = -1.0
            else:
                for day in dates:
                    deltas[(quantity, day, lead)] = -1.0
    return deltas


def test_m11_draw_resamples_discard_over_1_percent_through_daily_bounds() -> None:
    """M11, through production ``draw_resamples(30)`` and the production
    tile weights (``TILE_WEIGHTS``, ``LEAD_WEIGHTS``), as the heldout tile
    builds it (§15.3:1357, "Discarded resamples") -- one lead present on
    only one of 30 H dates discards every resample whose drawn indices
    never land on that one date. The deltas are uniformly -1.0 (not 1.0),
    so the kept resamples' point estimate is negative and clears
    ``point_delta_not_negative``: with ``discard_ok`` disabled, the
    mutant would let ``candidate_outcome`` reach PASS, not FAIL, instead
    of the correct INSUFFICIENT. ``bounds.discarded``: correct = the
    independently-computed count of index lists missing that date's
    index.
    """
    dates = [date(2030, 1, 1) + timedelta(days=i) for i in range(30)]
    j = 5
    deltas = _one_lead_missing_deltas(
        dates=dates, present_date=dates[j], missing_lead=3
    )
    index_lists = daily_shadow.draw_resamples(30)
    expected_discarded = sum(1 for indices in index_lists if j not in indices)

    bounds = daily_shadow.daily_bounds(
        deltas,
        index_lists,
        dates=dates,
        quantity_weights=daily_shadow.TILE_WEIGHTS,
        lead_weights=daily_shadow.LEAD_WEIGHTS,
        level=daily_shadow.LEVEL_TILE,
    )

    assert bounds.discarded == expected_discarded
    assert bounds.kept + bounds.discarded == len(index_lists)
    assert 100 * bounds.discarded > 1 * (bounds.kept + bounds.discarded)
    assert daily_shadow.discard_ok(bounds) is False

    result = daily_shadow.candidate_outcome(
        "X",
        tile=bounds,
        missing=0,
        guards=_BASE_GUARDS,
        twin=None,
        twin_missing=0,
        checks=_BASE_CHECKS,
        flip=False,
        station_set_changed=None,
        db_import_changed=False,
    )
    assert result.outcome == "INSUFFICIENT"
    assert result.reasons == ("discarded_over_1_percent",)


def test_m11_draw_resamples_paired_positive_every_lead_on_every_date() -> None:
    """Paired positive: every lead present on all 30 dates -> no resample
    can be discarded, so ``discard_ok`` passes and the outcome reaches
    PASS cleanly, with no reasons at all -- the same -1.0 deltas as the
    main test above, so the two share a direction and only the discard
    behavior differs.
    """
    dates = [date(2030, 1, 1) + timedelta(days=i) for i in range(30)]
    deltas = {
        (quantity, day, lead): -1.0
        for quantity in daily_shadow.TEMPERATURE_QUANTITIES
        for lead in daily_shadow.SCORED_LEADS
        for day in dates
    }
    index_lists = daily_shadow.draw_resamples(30)

    bounds = daily_shadow.daily_bounds(
        deltas,
        index_lists,
        dates=dates,
        quantity_weights=daily_shadow.TILE_WEIGHTS,
        lead_weights=daily_shadow.LEAD_WEIGHTS,
        level=daily_shadow.LEVEL_TILE,
    )

    assert bounds.discarded == 0
    assert daily_shadow.discard_ok(bounds) is True

    result = daily_shadow.candidate_outcome(
        "X",
        tile=bounds,
        missing=0,
        guards=_BASE_GUARDS,
        twin=None,
        twin_missing=0,
        checks=_BASE_CHECKS,
        flip=False,
        station_set_changed=None,
        db_import_changed=False,
    )
    assert result.outcome == "PASS"
    assert result.reasons == ()


# --- M12: F1 threshold 0.90 instead of 0.95 (direct F1Tally, not via the
#          freeze note -- M12 also feeds a constant comparison inside
#          ``check_note_matches_package`` that is part of "Freeze
#          enforcement" (§15.3:1371), exercised in the batch-2 file) -------


def _tally_at(matched: int, rows: int) -> daily_shadow.F1Tally:
    tally = daily_shadow.F1Tally()
    tally.matched = matched
    tally.rows = rows
    tally.on_time_matched = matched
    tally.on_time_rows = rows
    return tally


def test_m12_f1_at_90_percent_fails_the_95_percent_rule() -> None:
    """M12: F1 threshold 0.90 instead of 0.95 -> at ``F1Tally.passes()``
    for 18/20 matched (90.0%): correct = False (90 < 95), mutant = True
    (90 >= 90).
    """
    assert _tally_at(matched=18, rows=20).passes() is False


def test_m12_paired_positive_f1_at_95_percent_passes() -> None:
    """Paired positive: 19/20 matched (95.0%, the boundary) passes."""
    assert _tally_at(matched=19, rows=20).passes() is True


# --- M25: the on_time-only F1 rule removed ------------------------------------


def _record_row(*, write_path: str) -> daily_shadow.RecordRow:
    return daily_shadow.RecordRow(
        snapshot_date=date(2035, 6, 10),
        target_date=date(2035, 6, 10),
        lead=0,
        snapshot_utc="2035-06-10T07:00:00Z",
        status="recorded",
        write_path=write_path,
        app_version="0.16.7",
        methodology_version=1,
        tz_generation_id=1,
        policy=None,
        candidates=None,
        daily_quantities=None,
    )


def test_m25_on_time_only_f1_rule_removed_38_of_40_fails_f1() -> None:
    """M25: the ``on_time``-only F1 rule removed, killed by "F1 (38 of
    40)" (§15.3:1345) -- 20 ``on_time`` rows with 18 matching plus 20
    ``late_reconstruction`` rows all matching, built through production
    ``F1Tally.add``: 38 of 40 overall (95%), but the ``on_time`` rows
    alone are at 90%. At ``F1Tally.passes()``: correct = False (the
    on_time-only rule applies), mutant (the rule removed) = True (95%
    overall clears the single rule).
    """
    tally = daily_shadow.F1Tally()
    for i in range(20):
        tally.add(_record_row(write_path="on_time"), ok=i < 18)
    for _ in range(20):
        tally.add(_record_row(write_path="late_reconstruction"), ok=True)

    assert tally.rows == 40
    assert tally.matched == 38
    assert tally.on_time_rows == 20
    assert tally.on_time_matched == 18

    assert tally.passes() is False


def test_m25_paired_positive_38_of_40_with_on_time_at_95_percent_passes() -> None:
    """Paired positive, same 38-of-40 overall population (§15.3:1345): 20
    ``on_time`` rows with 19 matching plus 20 ``late_reconstruction`` rows
    with 19 matching -- 38 of 40 overall (95%) and the on_time rows alone
    also at 95% -> ``passes()`` is True.
    """
    tally = daily_shadow.F1Tally()
    for i in range(20):
        tally.add(_record_row(write_path="on_time"), ok=i < 19)
    for i in range(20):
        tally.add(_record_row(write_path="late_reconstruction"), ok=i < 19)

    assert tally.rows == 40
    assert tally.matched == 38
    assert tally.on_time_rows == 20
    assert tally.on_time_matched == 19

    assert tally.passes() is True


def test_m25_f1_below_95_insufficient_through_data_checks() -> None:
    """M25 at the outcome level, through production ``data_checks``. No
    scored ``TempLine`` rows are fabricated: ``data_checks`` reads only
    ``f1.as_json()`` and ``f1.passes()`` off the tally for the ``"f1"``
    and ``"f1_pass"`` entries, so ``[]`` isolates the F1 rule (not a
    production-reachable combination) -- the empty ``lines`` also drives
    ``minimums_met`` to False, which this test pins exactly rather than
    ignoring. At ``candidate_outcome``'s result: correct = ``"INSUFFICIENT"``
    with both ``"f1_below_95"`` (the on_time rows at 90%, below
    ``F1_MIN_PERCENT``) and ``"minimums_not_met"`` among the reasons,
    mutant (the ``on_time``-only check removed) = this tally's overall
    rate (95%) would not raise ``"f1_below_95"`` at all.
    """
    tally = daily_shadow.F1Tally()
    for i in range(20):
        tally.add(_record_row(write_path="on_time"), ok=i < 18)
    for _ in range(20):
        tally.add(_record_row(write_path="late_reconstruction"), ok=True)
    assert tally.passes() is False

    checks = daily_shadow.data_checks([], f1=tally)
    assert checks["f1_pass"] is False
    assert checks["minimums_met"] is False

    result = daily_shadow.candidate_outcome(
        "X",
        tile=_bounds(kept=100, discarded=0),
        missing=0,
        guards=_BASE_GUARDS,
        twin=None,
        twin_missing=0,
        checks=checks,
        flip=False,
        station_set_changed=None,
        db_import_changed=False,
    )
    assert result.outcome == "INSUFFICIENT"
    assert result.reasons == ("f1_below_95", "minimums_not_met")


# --- M15: files written straight into the final run directory ---------------


def _listed(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


def test_m15_atomic_outputs_a_a_clean_write_lands_under_partial_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """§15.3 "Atomic outputs" (:1370), case (a). ``_write_file`` is replaced
    by a recorder that, before anything else, records the path it is given
    and whether ``final_dir`` exists, then calls the original
    ``daily_shadow._write_file`` (saved before the monkeypatch).

    M15: files written straight into the final run directory
    (``partial = final_dir`` in ``write_outputs_atomically``) -> at the
    recorder's first call: correct = ``(tmp_path/"out"/"run.partial"/
    "a.json", False)`` (``final_dir`` absent, write under ``.partial/``),
    mutant = ``(tmp_path/"out"/"run"/"a.json", True)`` (``final_dir``
    already exists, because ``partial.mkdir(parents=True)`` (:1059)
    creates it before the write loop starts (:1061). The end states
    alone (``run``, ``run/a.json`` = ``first``, ``run/b.json`` =
    ``second``) are identical under the mutant and do not themselves
    catch it.
    """
    out = tmp_path / "out"
    final_dir = out / "run"
    calls: list[tuple[Path, bool]] = []
    real_write_file = daily_shadow._write_file

    def recorder(path: Path, data: bytes) -> None:
        calls.append((path, final_dir.exists()))
        real_write_file(path, data)

    monkeypatch.setattr(daily_shadow, "_write_file", recorder)
    result = daily_shadow.write_outputs_atomically(
        final_dir, {"a.json": b"first", "b.json": b"second"}
    )

    assert calls == [
        (out / "run.partial" / "a.json", False),
        (out / "run.partial" / "b.json", False),
    ]
    assert result == final_dir
    assert _listed(out) == ["run", "run/a.json", "run/b.json"]
    assert (final_dir / "a.json").read_bytes() == b"first"
    assert (final_dir / "b.json").read_bytes() == b"second"


def test_m15_atomic_outputs_b_a_failed_second_write_leaves_only_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """§15.3 "Atomic outputs" (:1370), case (b). The recorder calls the
    original ``_write_file`` on its first call and raises
    ``OSError("forced write")`` on its second without calling the
    original, so ``b.json`` is never created.

    M15: at the recorder's first call: correct = ``(tmp_path/"out"/
    "run.partial"/"a.json", False)``, mutant = ``(tmp_path/"out"/"run"/
    "a.json", True)``. Again the end state alone (``run.failed``,
    ``run.failed/a.json`` = ``first``, no ``run``, no ``.partial``, no
    ``b.json`` anywhere) is identical under the mutant.
    """
    out = tmp_path / "out"
    final_dir = out / "run"
    calls: list[tuple[Path, bool]] = []
    real_write_file = daily_shadow._write_file
    call_count = 0

    def recorder(path: Path, data: bytes) -> None:
        nonlocal call_count
        call_count += 1
        calls.append((path, final_dir.exists()))
        if call_count == 2:
            raise OSError("forced write")
        real_write_file(path, data)

    monkeypatch.setattr(daily_shadow, "_write_file", recorder)
    with pytest.raises(OSError, match="forced write"):
        daily_shadow.write_outputs_atomically(
            final_dir, {"a.json": b"first", "b.json": b"second"}
        )

    assert calls == [
        (out / "run.partial" / "a.json", False),
        (out / "run.partial" / "b.json", False),
    ]
    assert _listed(out) == ["run.failed", "run.failed/a.json"]
    assert (out / "run.failed" / "a.json").read_bytes() == b"first"


def test_rename_stale_partials_renames_a_stale_partial_directory(
    tmp_path: Path,
) -> None:
    """§15.3:1370, the sentence on stale ``.partial/`` directories, as a
    direct call: "A stale ``.partial/`` directory from an earlier run is
    renamed ``.failed/`` before the run starts."
    ``tests/test_daily_shadow_prepare_open.py`` (c) (:131-147) and (f)
    (:250-299) already pin the rename inside ``main()``, where a later
    step is forced to fail; this test pins it on its own, with nothing
    forced. This is a direct call of the production function, not a kill
    claim for any numbered mutant.
    """
    out = tmp_path / "out"
    stale = out / "x.partial"
    stale.mkdir(parents=True)
    (stale / "a.json").write_bytes(b"left over")

    renamed = daily_shadow.rename_stale_partials(out)

    assert renamed == ["x.failed"]
    assert not stale.exists()
    assert (out / "x.failed" / "a.json").read_bytes() == b"left over"
    assert _listed(out) == ["x.failed", "x.failed/a.json"]


# --- M16: manifest check skipped ----------------------------------------------
#
# Tests for M16 (plan row :1468, "manifest check skipped") are in
# tests/test_daily_shadow_batch2_mutants.py: the manifest-differs case is
# test_m16a_check_note_matches_package_python_version_differs,
# test_m16a_check_note_matches_package_files_differ, their paired
# positive, and the test_m16b_* tests of main(). Kill verification at
# checkpoint C is still pending. No replacement test is added here.
#
# The §15.3:1371 argparse case ("--heldout" without "--freeze-note" or
# without "--expect-sha256" exits with an argparse error) is covered
# there too, by
# test_freeze_enforcement_heldout_without_freeze_note_is_an_argparse_error
# and
# test_freeze_enforcement_heldout_without_expect_sha256_is_an_argparse_error.
# No mutant row targets this check, so no kill claim is made for it.
#
# The hex-digit sentence of "Freeze enforcement" belongs to M24 (plan row
# :1477: "--expect-sha256 comparison skipped"), not M16 (whose row is
# "manifest check skipped"), and is covered there by
# test_m24_expect_sha256_off_by_one_hex_digit_is_refused and its paired
# positive. Kill verification at checkpoint C is still pending.


# --- M20: G4 removed for the low ----------------------------------------------


def _candidate(value: float | None) -> daily_method.CandidateValue:
    return daily_method.CandidateValue(
        name="X",
        value=value,
        regime="per_lead",
        eligible=(),
        weights={},
        biases={},
        ranks=(),
        flags=(),
        fallback_reason=None,
    )


def _temp_line(
    *, quantity: str, lead: int, index: int, control: float, truth: float, value: float
) -> daily_shadow.TempLine:
    target = date(2031, 1, 1) + timedelta(days=index)
    return daily_shadow.TempLine(
        key=daily_shadow.CaseKey(snapshot_date=target, lead=lead, target=target),
        quantity=quantity,
        row=None,
        unmatched=None,
        control=control,
        truth=truth,
        f1=True,
        candidates={"X": _candidate(value)},
    )


def _g4_fixture(*, warm_count: int) -> list[daily_shadow.TempLine]:
    """100 "temperature_low" cases, ``warm_count`` of them 2.01 degrees
    over truth (> ``G4_BAND_C``), the rest exact; the control sits at
    exactly the 2.0 band line, never over it, in every case. Plus 100
    clean "temperature_high" cases (candidate = control = truth) so every
    guard other than G4's low entry passes cleanly.
    """
    lines: list[daily_shadow.TempLine] = []
    for i in range(100):
        candidate_value = 20.0 + (2.01 if i < warm_count else 0.0)
        lines.append(
            _temp_line(
                quantity="temperature_low",
                lead=0,
                index=i,
                control=22.0,
                truth=20.0,
                value=candidate_value,
            )
        )
    for i in range(100):
        lines.append(
            _temp_line(
                quantity="temperature_high",
                lead=0,
                index=i,
                control=20.0,
                truth=20.0,
                value=20.0,
            )
        )
    return lines


def _is_dict(obj: object) -> TypeGuard[dict[object, object]]:
    """Narrows an ``object``-typed value from a guard report to a dict
    with unchecked key and value types -- ``isinstance(obj, dict)`` does
    not prove the keys are ``str`` or the values are any particular
    type. The exact dict assertion that follows this check is what
    checks the keys and values."""
    return isinstance(obj, dict)


def _assert_other_guards_pass(guards: dict[str, object], *, warm_count: int) -> None:
    """G2's own values (independent of G4): the candidate's mean loss per
    quantity is low = ``warm_count * 2.01 / 100`` (0.201 for 10, 0.1206
    for 6), high = 0.0, both well within control 2.0 * 1.02 = 2.04;
    ``_g4_fixture`` sets no ``warm_spell`` or ``frost`` case, so G3 and
    the frost guard are both inconclusive (``"pass": None``), which
    ``guards_pass`` also treats as passing.
    """
    g2 = guards["G2"]
    assert _is_dict(g2)
    temperature_low = g2["temperature_low"]
    assert _is_dict(temperature_low)
    candidate = temperature_low["candidate"]
    assert isinstance(candidate, float)
    assert math.isclose(candidate, warm_count * 2.01 / 100, rel_tol=0, abs_tol=1e-9)
    assert g2 == {
        "temperature_high": {"candidate": 0.0, "control": 0.0, "pass": True},
        "temperature_low": {
            "candidate": candidate,
            "control": 2.0,
            "pass": True,
        },
    }
    assert guards["G3"] == {
        "warm_spell_cases": 0,
        "candidate_under": None,
        "control_under": None,
        "pass": None,
    }
    assert guards["frost"] == {
        "frost_cases": 0,
        "lead0_dates": 0,
        "assessable": False,
        "candidate_too_warm": None,
        "control_too_warm": None,
        "pass": None,
    }


def test_m20_g4_on_the_low_fails_when_the_candidate_runs_warm() -> None:
    """M20: G4 removed for the low -> at ``guards_pass(guard_report(...))``
    with the candidate's low more than 2 degrees too warm on 10 of 100
    cases while the control never is: correct = False
    (``G4["temperature_low"]["pass"]`` fails, 1000 > 0 + 500), mutant =
    True (the low's G4 entry is never checked). The outcome follows:
    correct = ``candidate_outcome(...)`` returns
    ``("FAIL", ("guard_failed",))``.
    """
    lines = _g4_fixture(warm_count=10)
    guards = daily_shadow.guard_report(lines, "X")
    assert guards["G4"] == {
        "temperature_high": {
            "cases": 100,
            "candidate_too_warm": 0,
            "control_too_warm": 0,
            "pass": True,
        },
        "temperature_low": {
            "cases": 100,
            "candidate_too_warm": 10,
            "control_too_warm": 0,
            "pass": False,
        },
    }
    _assert_other_guards_pass(guards, warm_count=10)
    assert daily_shadow.guards_pass(guards) is False
    result = daily_shadow.candidate_outcome(
        "X",
        tile=daily_shadow.Bounds(
            point=-0.5, lower=-0.9, upper=-0.1, kept=1000, discarded=0, level=0.975
        ),
        missing=0,
        guards=guards,
        twin=None,
        twin_missing=0,
        checks=_BASE_CHECKS,
        flip=False,
        station_set_changed=None,
        db_import_changed=False,
    )
    assert result.outcome == "FAIL"
    assert result.reasons == ("guard_failed",)


def test_m20_second_killer_g4_margin_is_5_points_not_6() -> None:
    """Second killer, pinning the 5-percentage-point margin exactly: 6 of
    100 cases too warm -> at ``G4["temperature_low"]["pass"]``: correct =
    False (100 * 6 = 600 > 0 + 5 * 100 = 500, at the real margin of 5
    points), while at a margin of 6 points this same fixture would give
    600 <= 600 = True. Together with
    ``test_m20_paired_positive_g4_on_the_low_passes_within_the_margin``'s
    5-of-100 fixture (500 <= 500, True at margin 5), these two fixtures
    pin the margin at exactly 5: a margin of 6 would flip this test to
    PASS, and a margin of 4 would flip the paired positive to FAIL.
    """
    lines = _g4_fixture(warm_count=6)
    guards = daily_shadow.guard_report(lines, "X")
    assert guards["G4"] == {
        "temperature_high": {
            "cases": 100,
            "candidate_too_warm": 0,
            "control_too_warm": 0,
            "pass": True,
        },
        "temperature_low": {
            "cases": 100,
            "candidate_too_warm": 6,
            "control_too_warm": 0,
            "pass": False,
        },
    }
    _assert_other_guards_pass(guards, warm_count=6)
    assert daily_shadow.guards_pass(guards) is False
    result = daily_shadow.candidate_outcome(
        "X",
        tile=daily_shadow.Bounds(
            point=-0.5, lower=-0.9, upper=-0.1, kept=1000, discarded=0, level=0.975
        ),
        missing=0,
        guards=guards,
        twin=None,
        twin_missing=0,
        checks=_BASE_CHECKS,
        flip=False,
        station_set_changed=None,
        db_import_changed=False,
    )
    assert result.outcome == "FAIL"
    assert result.reasons == ("guard_failed",)


def test_m20_paired_positive_g4_on_the_low_passes_within_the_margin() -> None:
    """Paired positive: the same shape, but the candidate is too warm on
    only 5 of 100 cases (100 * 5 = 500 <= 0 + 5 * 100 = 500, the
    boundary) -- G4 for the low passes. Because the control sits at
    exactly the 2.0 band line in every case, this fixture also exercises
    a ``>`` -> ``>=`` change on the control side of G4_BAND_C
    (daily_shadow.py:2831): under ``>=`` ``control_too_warm`` becomes 100
    instead of 0, but the low's ``pass`` stays True
    (100 * 5 <= 100 * 100 + 5 * 100). What catches that change is the
    exact G4 dict assertion below (``control_too_warm`` 0 vs 100), not a
    flip of ``pass``.
    """
    lines = _g4_fixture(warm_count=5)
    guards = daily_shadow.guard_report(lines, "X")
    assert guards["G4"] == {
        "temperature_high": {
            "cases": 100,
            "candidate_too_warm": 0,
            "control_too_warm": 0,
            "pass": True,
        },
        "temperature_low": {
            "cases": 100,
            "candidate_too_warm": 5,
            "control_too_warm": 0,
            "pass": True,
        },
    }
    assert daily_shadow.guards_pass(guards) is True


# --- M21b: an unknown day's amount scored as 0 -------------------------------


def _rain_day(
    *, outcome: daily_method.EventOutcome, amount_value: float | None
) -> daily_shadow.RainDay:
    event = daily_method.RainEvent(
        outcome=outcome, trigger=None, adequate=(), partial=()
    )
    amount = daily_method.ObservedAmount(
        amount_value, None, (), (), daily_method.AMOUNT_METHOD
    )
    flags = daily_method.HighReadingFlags(
        large_total=None, large_total_hundredths=None, lone_reading=None
    )
    return daily_shadow.RainDay(
        day=date(2032, 1, 1), event=event, amount=amount, flags=flags, stations=()
    )


def _rain_line(day: daily_shadow.RainDay) -> daily_shadow.RainLine:
    key = daily_shadow.CaseKey(
        snapshot_date=date(2032, 1, 1), lead=0, target=date(2032, 1, 1)
    )
    return daily_shadow.RainLine(
        key=key,
        row=None,
        unmatched=None,
        control=None,
        day=day,
        f1=None,
        threshold_mm=None,
        decision_time=None,
        window=None,
        training_days=0,
        asof_differs=0,
        incumbent=None,
        selection=None,
        served=None,
        clim=None,
        issued={},
        symbols={},
        amounts={},
        grid=(),
        own_days=None,
        published={"precip_occurrence": None, "precip_total": None},
    )


def test_m21_an_unknown_days_amount_is_not_scored() -> None:
    """M21b (plan :1474): ``daily_shadow._observed`` returns 0.0 for an
    unknown line -> at ``_observed`` for an unknown day (injected via
    ``outcome="unknown"``, with the day's own ``ObservedAmount.amount``
    left at ``None`` as production code would leave it): correct = None,
    mutant = 0.0.
    """
    unknown_line = _rain_line(_rain_day(outcome="unknown", amount_value=None))
    assert daily_shadow._observed(unknown_line) is None  # pyright: ignore[reportPrivateUsage]


def test_m21_paired_positive_a_dry_days_amount_is_scored_as_0() -> None:
    """Paired positive: a dry day (injected via ``outcome="dry"``) is a
    known day and does score 0.0 -- so M21's bug is only visible on the
    unknown day, not on every zero-amount day.
    """
    dry_line = _rain_line(_rain_day(outcome="dry", amount_value=0.0))
    assert daily_shadow._observed(dry_line) == 0.0  # pyright: ignore[reportPrivateUsage]


# --- M23: end-of-run sha256 check removed -------------------------------------


def test_m23_a_copy_mutated_during_the_run_errors_with_copy_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M23: end-of-run sha256 check removed -> at ``main()``'s exit code,
    with the copy mutated mid-run (via a wrapper around ``load_site``, the
    first function the run calls after opening the read-only copy --
    this fixture's site holds no records, so ``control_value``,
    §15.3:1382's own hook, is never called here):
    correct = 1 (``CopyChangedError``, caught and written as ``error.json``),
    mutant = 0 (the run completes as if the copy never moved).
    """
    copy = tmp_path / "copy.db"
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "copy-changed-site")
        ensure_published_generation(conn, site_id)
        conn.commit()
    finally:
        conn.close()
    before_bytes = copy.read_bytes()

    real_load_site = daily_shadow.load_site

    def mutate_then_load(
        conn: sqlite3.Connection, *, site_id: int
    ) -> daily_shadow.Site:
        result = real_load_site(conn, site_id=site_id)
        # Simulate another process touching the copy mid-run: append past
        # the logical end of the file, which a real external write (e.g.
        # a checkpoint from a still-attached writer) could equally do.
        with copy.open("ab") as handle:
            handle.write(b"\x00")
        return result

    monkeypatch.setattr(daily_shadow, "load_site", mutate_then_load)
    out = tmp_path / "out"
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy, out=out, monkeypatch=monkeypatch
    )
    assert open_calls == 1
    assert copy.read_bytes() == before_bytes + b"\x00"
    assert exit_code == 1
    assert "ERROR at stage copy_changed" in err_text
    assert "CopyChangedError" in err_text
    # ``CopyChangedError`` comes from main()'s own post-run check
    # (daily_shadow.py:7113-7119), not from error_after_open: that
    # function returns a CopyChangedError unchanged (daily_shadow.py:
    # 1020-1021). Only a *different* exception raised after the copy's
    # hash changed takes error_after_open's chained branch
    # (daily_shadow.py:1030-1035), which adds "while handling" to the
    # message. So its absence here is a genuine, non-vacuous check of
    # which code path produced the error.
    assert "while handling" not in err_text
    assert daily_shadow.RESULT_PREFIX in out_text
    prefix = daily_shadow.RESULT_PREFIX
    result_line = next(
        line for line in out_text.splitlines() if line.startswith(prefix)
    )
    result_json = json.loads(result_line[len(prefix) :])
    run_dir_name = result_json["run_dir"]
    assert isinstance(run_dir_name, str)
    run_dir = out / run_dir_name
    assert sorted(p.name for p in run_dir.iterdir()) == ["error.json"]
    error_json = json.loads((run_dir / "error.json").read_bytes())
    assert error_json["stage"] == "copy_changed"


def test_m23_paired_positive_an_unmutated_copy_runs_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paired positive: with no mid-run mutation, the identical setup
    succeeds (exit 0), proving the error above is caused by the copy
    changing, not by anything else in the fixture.
    """
    copy = tmp_path / "copy.db"
    conn = sqlite3.connect(copy)
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(conn)
        site_id = asof_make_site(conn, "copy-unchanged-site")
        ensure_published_generation(conn, site_id)
        conn.commit()
    finally:
        conn.close()
    out = tmp_path / "out"
    exit_code, out_text, err_text, open_calls = _run_main(
        copy=copy, out=out, monkeypatch=monkeypatch
    )
    assert open_calls == 1
    assert exit_code == 0
    assert "ERROR" not in err_text
    assert daily_shadow.RESULT_PREFIX in out_text
