"""§15.4 Batch 1: killer tests for mutants M9, M10, M11, M12, M15, M20,
M21 and M23.

These are direct tests that ADD to the named §15.3 tests; they do not
replace them (§15.4:1403, "Each must be killed by the named test"). Each
mutant's own pure function or filesystem-level site is called directly,
rather than driven through the full pipeline: for M12, directly against
``F1Tally.passes()`` at the 90%/95% boundary §15.3's own "F1." test
(:1311) describes, and likewise for the others below. The site under
test is exercised with real inputs and no mock of the code under test,
but without the DB-backed case machinery that mutants whose divergence
needs multiple distinct cases (M22, in
``test_daily_shadow_heldout_wiring.py``) require.

Named §15.3 test coverage and remaining work:

- implemented: M9's lighter direct kill test. This file pins the same
  change ("Arm A training window ends at K + 1") directly against
  ``temperature_window``, the one function the mutant changes -- a
  lighter kill at the same site, not a re-derivation of the leak test's
  broader byte-identical guarantee. still owed: M9 -> "the leak test"
  itself (§15.3:1313; also kills M28, M32 and M34 per the mutant table
  at §15.4:1435, :1439 and :1441).
- still owed: M11 -> "Discarded resamples" (§15.3:1322).
- implemented: M16 -> plan row :1423 "manifest check skipped". Covered
  in ``tests/test_daily_shadow_batch2_mutants.py`` by
  ``test_m16a_check_note_matches_package_python_version_differs``,
  ``test_m16a_check_note_matches_package_files_differ``, their paired
  positive, and the ``test_m16b_*`` tests. Kill verification at
  checkpoint C is still pending.
- implemented: the §15.3:1336 argparse case (``--heldout`` without
  ``--freeze-note`` or without ``--expect-sha256`` exits with an
  argparse error) is covered in
  ``tests/test_daily_shadow_batch2_mutants.py`` by
  ``test_freeze_enforcement_heldout_without_freeze_note_is_an_argparse_error``
  and
  ``test_freeze_enforcement_heldout_without_expect_sha256_is_an_argparse_error``.
  No mutant row targets this check, so no kill claim is made for it.
- implemented: M24 -> plan row :1431 "``--expect-sha256`` comparison
  skipped" (not M16, whose row is "manifest check skipped"), the
  hex-digit sentence. Covered in
  ``tests/test_daily_shadow_batch2_mutants.py`` by
  ``test_m24_expect_sha256_off_by_one_hex_digit_is_refused`` and its
  paired positive. Kill verification at checkpoint C is still pending.
- still owed: atomic outputs (§15.3:1335), last sentence: "A
  counts-only run and then a ``--freeze-prepare`` run on one copy leave
  two separate run directories."
- still owed: M21 -> "Amount on known days" (§15.3:1376; also kills
  M68).
- still owed: M23 -> "The copy changed during the run" (§15.3:1347),
  cases (a) and (b).
- still owed: M10 -> the "Levels" pins (§15.3:1323): 974.025 at 0.95,
  949.05 at 0.90, and PASS with U = -5.975.
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
    dates = [date(2020, 1, 1) + timedelta(days=i) for i in range(1000)]
    index_lists = [[i] for i in range(1000)]
    deltas = {("q", dates[i], 0): float(i) - shift for i in range(1000)}
    return daily_shadow.daily_bounds(
        deltas,
        index_lists,
        dates=dates,
        quantity_weights={"q": 1.0},
        lead_weights={0: 1.0},
        level=daily_shadow.LEVEL_TILE,
    )


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
    discard limit (daily_shadow.py:3039-3040, via ``discard_ok`` at
    :2233) -> at the result: correct = ``("INSUFFICIENT",
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


# --- M12: F1 threshold 0.90 instead of 0.95 (direct F1Tally, not via the
#          freeze note -- M12 also feeds a constant comparison inside
#          ``check_note_matches_package`` that is part of "Freeze
#          enforcement" (§15.3:1336), exercised in the batch-2 file) -------


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


# --- M15: files written straight into the final run directory ---------------


def _listed(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


def test_m15_atomic_outputs_a_a_clean_write_lands_under_partial_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """§15.3 "Atomic outputs" (:1335), case (a). ``_write_file`` is replaced
    by a recorder that, before anything else, records the path it is given
    and whether ``final_dir`` exists, then calls the original
    ``daily_shadow._write_file`` (saved before the monkeypatch).

    M15: files written straight into the final run directory
    (``partial = final_dir`` in ``write_outputs_atomically``) -> at the
    recorder's first call: correct = ``(tmp_path/"out"/"run.partial"/
    "a.json", False)`` (``final_dir`` absent, write under ``.partial/``),
    mutant = ``(tmp_path/"out"/"run"/"a.json", True)`` (``final_dir``
    already exists, because ``partial.mkdir(parents=True)`` (:1057)
    creates it before the write loop starts (:1059). The end states
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
    """§15.3 "Atomic outputs" (:1335), case (b). The recorder calls the
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
    """§15.3:1335, the sentence on stale ``.partial/`` directories, as a
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
# Tests for M16 (plan row :1423, "manifest check skipped") are in
# tests/test_daily_shadow_batch2_mutants.py: the manifest-differs case is
# test_m16a_check_note_matches_package_python_version_differs,
# test_m16a_check_note_matches_package_files_differ, their paired
# positive, and the test_m16b_* tests of main(). Kill verification at
# checkpoint C is still pending. No replacement test is added here.
#
# The §15.3:1336 argparse case ("--heldout" without "--freeze-note" or
# without "--expect-sha256" exits with an argparse error) is covered
# there too, by
# test_freeze_enforcement_heldout_without_freeze_note_is_an_argparse_error
# and
# test_freeze_enforcement_heldout_without_expect_sha256_is_an_argparse_error.
# No mutant row targets this check, so no kill claim is made for it.
#
# The hex-digit sentence of "Freeze enforcement" belongs to M24 (plan row
# :1431: "--expect-sha256 comparison skipped"), not M16 (whose row is
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
    (daily_shadow.py:2829): under ``>=`` ``control_too_warm`` becomes 100
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


# --- M21: an unknown day's amount scored as 0 --------------------------------


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
    )


def test_m21_an_unknown_days_amount_is_not_scored() -> None:
    """M21: an unknown day's amount scored as 0 -> at ``_observed`` for an
    unknown day (injected via ``outcome="unknown"``, with the day's own
    ``ObservedAmount.amount`` left at ``None`` as production code would
    leave it): correct = None, mutant = 0.0.
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
    this fixture's site holds no records, so ``control_value``, §15.3:
    1347's own hook, is never called here):
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
    # (daily_shadow.py:6806-6812), not from error_after_open: that
    # function returns a CopyChangedError unchanged (daily_shadow.py:
    # 1018-1019). Only a *different* exception raised after the copy's
    # hash changed takes error_after_open's chained branch
    # (daily_shadow.py:1028-1033), which adds "while handling" to the
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
