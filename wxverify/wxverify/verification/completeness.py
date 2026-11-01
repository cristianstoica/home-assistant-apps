"""Admission basis for one closed local day's ``daily_truth`` rows (plan 7.1).

``daily_truth.admission_basis`` records whether all of a day's hours were
present when the daily close wrote the day: :data:`ADMISSION_COMPLETE` when
every evaluated quantity's covered_hours equals its expected_slots, else
:data:`ADMISSION_DEADLINE`. The close decides WHEN a day is written, so the
basis never decides whether it may be.

The basis never touches ``eligible``, which is the §4 coverage verdict
scoring consumes. It is per-day, never per-quantity: a day's five quantity
rows are written together and carry one basis.

This module is pure — no ``sqlite3``, no I/O, no clock read.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    # Type-only: the annotation below needs no runtime import.
    from wxverify.verification.truth import DayEvaluation

# The only two values ``daily_truth.admission_basis`` may hold. The migration
# DDL's CHECK constraint names the same pair; the two must not drift.
ADMISSION_COMPLETE: Final = "complete"
ADMISSION_DEADLINE: Final = "deadline"


def close_admission_basis(evaluation: DayEvaluation) -> str:
    """The ``admission_basis`` the daily close stores with a day's rows (7.1).

    :data:`ADMISSION_COMPLETE` when every evaluated quantity's covered hours
    equal its expected slots, else :data:`ADMISSION_DEADLINE`. An empty set
    of outcomes is not complete: nothing was evaluated, so nothing is vouched
    for. The basis says only whether all hours were present when the day
    closed, never whether it may be written.
    """
    complete = bool(evaluation.outcomes) and all(
        outcome.covered_hours == outcome.expected_slots
        for outcome in evaluation.outcomes.values()
    )
    return ADMISSION_COMPLETE if complete else ADMISSION_DEADLINE
