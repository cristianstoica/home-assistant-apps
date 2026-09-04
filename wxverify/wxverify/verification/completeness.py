"""Admission policy for CREATING one local day's ``daily_truth`` rows (DEF-11).

``materialize_daily_truth`` writes whatever observations are in the database
at the instant it runs, and ``missing_truth_days`` is a pure set difference
over days that already HAVE rows — so a day finalized before its last hour
arrived is never re-offered. This module holds the decision that gates
creation, and creation ONLY: regeneration and the retrospective
timezone-correction rebuild stay ungated, because gating them would strand a
``stale = 1`` row that could then never be rebuilt.

The rule, evaluated in this order (D3):

    C1 (exact)     every evaluated quantity's covered_hours == expected_slots
    C2 (quiesced)  the day's newest observations.computed_at is at least
                   TRUTH_ARRIVAL_QUIESCENCE_HOURS old
    C3 (deadline)  now >= day_end_utc + TRUTH_COMPLETENESS_DEADLINE_HOURS

    C1 and C2   -> admit, admission_basis 'complete'
    else C3     -> admit, admission_basis 'deadline'
    else        -> defer: write nothing. The next nightly pass re-offers the
                   day unchanged, precisely because a deferred day left no
                   rows behind.

The order is load-bearing. Testing C3 first would stamp a genuinely complete
but late-evaluated day — a backlog recovery, or a discovery chunk reaching a
day several days after it closed — as ``'deadline'``, destroying the column's
meaning for every such day.

Admission is a TIMING verdict and never touches ``eligible``, which is the §4
coverage verdict scoring consumes: a short day is deferred, never marked
ineligible. It is also per-day, never per-quantity — a day's five quantity
rows are written together or not at all.

Deferral costs at most one night and needs no state anywhere: a deferred day
has no rows, so the MAX behind ``settled_through`` simply does not see it.

This module is pure — no ``sqlite3``, no I/O, no clock read (``now`` is an
argument) — so every branch above is a directly testable unit.

TRUTH_ARRIVAL_QUIESCENCE_HOURS — what C2 is actually worth (D4)

C2 is a weak secondary guard; C1's exactness is the operative gate; C2
removes only the obvious "something wrote to this day moments ago" case;
quiescence-by-silence is unsound alone (a dead collector is also silent) and
is usable only because C1 requires exact completeness; D10's pre-publish
divergence gate is what catches a revision landing after admission.

There is no poll-round argument behind one hour, and none is claimed. The
routine writer of ``observations`` is the ``fetch_obs`` job at
``obs_interval_minutes`` (default 180, floor 30) plus a non-negative
``obs_jitter_minutes`` (default cap 20), so consecutive ingest rounds are
180-200 minutes apart at defaults and one hour of silence guarantees ZERO
complete rounds. Widening the window until it IS a round was worked and
rejected: the nightly trigger is only 300 minutes after day end, and the
interval is operator-settable to several times that band, which would
silently convert admission into a pure deadline gate.

The leg that does hold is fetch-window coverage, and it holds only inside a
band: the ``RECENT_REFRESH_HOURS = 6`` argument is exact at ``day_end + 5h``
and void for a late-evaluated day (an outage backlog, or a discovery chunk
reaching a day several days after it closed), where C2 degrades to "nothing
wrote to this day recently" and C1's exactness is the sole guard. At the
trigger the hourly-history cutoff is ``(day_end + 5h) - 6h = day_end - 1h``,
exactly the day's own final hour, so an ingest round that ran during the
silent hour had that hour inside its fetch window and produced no change.
Silence is evidence about OPPORTUNITY, not about round count.

Re-derive this constant if ``obs_interval_minutes`` or ``obs_jitter_minutes``
move materially (both are operator-settable), or if ``RECENT_REFRESH_HOURS``
or the ``05:00`` trigger move, which breaks the fetch-window leg outright.

TRUTH_COMPLETENESS_DEADLINE_HOURS — the bracketing inequality (D7)

A permanently missing hour must not block a day's truth forever; the deadline
is that bound, and the value is fixed by a bracket against the nightly
trigger rather than by taste:

    trigger_offset          =  5h   (day_end_utc -> the 05:00 local trigger)
    trigger_offset + 24h    = 29h   (the SECOND nightly run after day close)

    REQUIRED:   trigger_offset  <  DEADLINE  <  trigger_offset + 24h
    i.e.                5h      <    24h     <         29h

The lower bound must hold or the deadline fires on the FIRST nightly pass,
the gate never defers anything and DEF-11 is unfixed. The upper bound must
hold or a chronically short day slips a second night and the horizon falls
further behind than designed. At 24h a day that can never satisfy C1 is
materialized on the second nightly run after it closes, recorded as
``admission_basis = 'deadline'`` — at worst one night later than the pre-gate
behaviour, and attributably rather than silently.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

# The only two values ``daily_truth.admission_basis`` may hold. The migration
# DDL's CHECK constraint names the same pair; the two must not drift.
ADMISSION_COMPLETE: Final = "complete"
ADMISSION_DEADLINE: Final = "deadline"

# C2's window. A weak secondary guard only — C1's exact-completeness test is
# what actually gates admission. Derivation, the band in which the
# fetch-window argument holds, and the re-derive trigger: module docstring.
TRUTH_ARRIVAL_QUIESCENCE_HOURS: Final[int] = 1

# C3's bound, measured from ``day_end_utc``: a permanently short day must not
# block truth forever. Bracketed by 5h < 24h < 29h against the nightly
# trigger, so the deadline can neither fire on the first pass nor slip a
# second night; both bounds are worked in the module docstring.
TRUTH_COMPLETENESS_DEADLINE_HOURS: Final[int] = 24


@dataclass(frozen=True)
class AdmissionDecision:
    """One local day's admission verdict.

    ``basis`` is the value to store in ``daily_truth.admission_basis``:
    :data:`ADMISSION_COMPLETE`, :data:`ADMISSION_DEADLINE`, or ``None`` when
    the day is deferred and nothing is written at all. ``reason`` is a short
    log-safe token naming the branch taken — it carries no observation value,
    no timestamp and no site identity, so a caller can log it beside the site
    id and local date without leaking data into the record.
    """

    admit: bool
    basis: str | None
    reason: str


def decide_admission(
    *,
    day_end_utc: datetime,
    expected_slots: int,
    covered_hours: Sequence[int],
    max_computed_at: datetime | None,
    now: datetime,
) -> AdmissionDecision:
    """Decide whether one settled local day may be materialized now (D3).

    ``day_end_utc`` is the exclusive UTC end of the local day and
    ``expected_slots`` the number of UTC hourly instants it contains — both
    taken from the day's own ``DayBounds``, never a literal 24, so 23- and
    25-hour DST days are exact. ``covered_hours`` is every evaluated
    quantity's coverage count: five entries in production carrying three
    distinct values (the two temperature quantities share one, the two precip
    quantities share one, wind has its own), so "all equal ``expected_slots``"
    is exactly "every variable complete". ``max_computed_at`` is the newest
    ``observations.computed_at`` inside the day, or ``None`` when the day
    holds none. ``now`` is the caller's clock read; all three datetimes must
    be timezone-aware UTC.

    A deferring verdict names the FIRST failing condition, following the
    same first-failing-gate policy the §4 exclusion reasons use. An empty
    ``covered_hours`` fails C1 rather than passing it vacuously: nothing was
    evaluated, so nothing is vouched for — the same fail-closed stance D10
    takes for a quantity that re-derives to no outcome.
    """
    complete = bool(covered_hours) and all(
        covered == expected_slots for covered in covered_hours
    )
    quiesced = max_computed_at is not None and (
        now - max_computed_at >= timedelta(hours=TRUTH_ARRIVAL_QUIESCENCE_HOURS)
    )
    if complete and quiesced:
        return AdmissionDecision(
            admit=True,
            basis=ADMISSION_COMPLETE,
            reason="complete_and_quiesced",
        )
    deadline_utc = day_end_utc + timedelta(hours=TRUTH_COMPLETENESS_DEADLINE_HOURS)
    if now >= deadline_utc:
        return AdmissionDecision(
            admit=True,
            basis=ADMISSION_DEADLINE,
            reason="deadline_elapsed",
        )
    if not covered_hours:
        return AdmissionDecision(
            admit=False, basis=None, reason="no_quantities_evaluated"
        )
    if not complete:
        return AdmissionDecision(admit=False, basis=None, reason="incomplete_coverage")
    return AdmissionDecision(admit=False, basis=None, reason="awaiting_quiescence")
