"""O16 oracle: the near-complete eligibility gate still admits 23 of 24 hours.

The rest of this file's oracles drove
``wxverify.verification.completeness.decide_admission`` and
``wxverify.verification.truth.materialize_admitted_day`` /
``materialize_missing_truth_days`` / ``regenerate_marked_truth_chunk``,
retired with the daily close; O16 alone exercises the still-live evaluator
and so stays.

Synthetic data only: ``America/Denver``, never a real place.
"""

from __future__ import annotations

from datetime import date, timedelta

from wxverify.core.timeutil import isoformat_utc
from wxverify.verification.coverage import evaluate_wind, local_day_bounds

TZ_DENVER = "America/Denver"


def test_o16_wind_near_complete_23_of_24_still_eligible() -> None:
    """O16 -> at ``outcome.eligible``: correct = True, mutant = False if
    ``NEAR_COMPLETE_SLOT_ALLOWANCE`` were set to 0 -- someone "fixing"
    a completeness gap by tightening the pre-existing near-complete gate to exact
    equality, which would silently re-classify every historical
    near-complete wind row as ineligible on its next regeneration -- a
    history rewrite disguised as a fix."""
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
