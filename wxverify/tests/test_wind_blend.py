"""Wind training and weights (T50, T51, T56-T58).

The tests that drove ``load_wind_training``/``training_window``/
``training_pairs_query`` end to end, and the excluded-feeds filter check,
are deleted: served wind weights are now always the published ones or
``{}``, and those three names are gone from ``wxverify.forecast.wind_blend``.

T50, T51, T56, T57 and T58 are pure once a ``WindTraining``/candidate set is
in hand (``compute_wind_weights``, ``choose_wind_feeds``,
``weighted_daily_high``, ``weighted_hourly``) and unrelated to the removed
read path, so they stay as written.
"""

from __future__ import annotations

from datetime import date, timedelta

from wxverify.forecast.selection import CellCandidate
from wxverify.forecast.wind_blend import (
    FLOOR_MS,
    MIN_FEEDS,
    MIN_RUN_HOURS,
    MIN_TRAINING_DAYS,
    FeedWeight,
    WindTraining,
    choose_wind_feeds,
    compute_wind_weights,
    weighted_daily_high,
    weighted_hourly,
)

_TODAY = date(2026, 4, 15)


def _day(n: int) -> date:
    """``_TODAY - n`` days -- n in [1, TRAINING_DAYS-1] stays inside the window."""
    return _TODAY - timedelta(days=n)


def _candidate(feed_id: int, *, covered_hours: int) -> CellCandidate:
    return CellCandidate(
        feed_id=feed_id,
        source="example-src",
        model=f"m{feed_id}",
        confident=True,
        skill_score=None,
        pair_n=0,
        mae=None,
        future_sample_count=0,
        covered_hours=covered_hours,
        extrema_eligible=False,
    )


# --- T50: the floor ----------------------------------------------------


def test_t50_floor_applies_below_it_not_above() -> None:
    """MAE 0.0 -> weight 10.0; MAE 0.05 -> weight 10.0 (both floored at 0.1).

    mutant_km_floor -> at the second assertion: correct = 10.0 (``1 /
    max(0.05, 0.1)``), mutant (a floor value scaled for km/h, e.g. 0.36) =
    1/0.36 = 2.777... The fixture's 0.05 MAE sits strictly below both the
    correct and the mutant floor, so only the floor's VALUE, not whether a
    floor applies, is what the two literals diverge on.
    """
    days_a = [_day(n) for n in range(1, MIN_TRAINING_DAYS + 1)]
    feed_highs_a = {day: 10.0 for day in days_a}
    observed_a = {day: 10.0 for day in days_a}
    training_a = WindTraining(
        feed_highs={(1, 0): feed_highs_a}, observed_highs=observed_a
    )
    weights_a = compute_wind_weights(training_a)
    assert weights_a[(1, 0)].mae_ms == 0.0
    assert weights_a[(1, 0)].weight == 1.0 / FLOOR_MS

    days_b = [_day(n) for n in range(1, MIN_TRAINING_DAYS + 1)]
    feed_highs_b = {day: 10.05 for day in days_b}
    observed_b = {day: 10.0 for day in days_b}
    training_b = WindTraining(
        feed_highs={(1, 0): feed_highs_b}, observed_highs=observed_b
    )
    weights_b = compute_wind_weights(training_b)
    assert round(weights_b[(1, 0)].mae_ms, 2) == 0.05
    assert weights_b[(1, 0)].weight == 10.0


# --- T51: the minimum day count -----------------------------------------


def test_t51_19_days_absent_20_present() -> None:
    """mutant_off_by_one -> at the second assertion: correct = True (key
    present at 20 matched days), mutant (``<=`` in place of ``<`` on the
    day-count guard) = False (20 treated as still-too-few). The fixture's
    second case sits exactly at the stated threshold.
    """
    days_19 = [_day(n) for n in range(1, 20)]
    training_19 = WindTraining(
        feed_highs={(1, 0): {d: 5.0 for d in days_19}},
        observed_highs={d: 5.0 for d in days_19},
    )
    assert (1, 0) not in compute_wind_weights(training_19)

    days_20 = [_day(n) for n in range(1, 21)]
    training_20 = WindTraining(
        feed_highs={(1, 0): {d: 5.0 for d in days_20}},
        observed_highs={d: 5.0 for d in days_20},
    )
    assert (1, 0) in compute_wind_weights(training_20)


# --- T56: weighted_daily_high --------------------------------------------


def test_t56_weighted_daily_high_is_weighted_not_unweighted_mean() -> None:
    """weights {a:3, b:1}, maxima {a:10, b:20} -> 12.5.

    mutant_unweighted_mean -> at the assertion: correct = 12.5 (the
    weighted mean), mutant (an unweighted mean, ``sum(maxima)/n``) = 15.0.
    The fixture's weights are deliberately asymmetric (3 vs 1) so an
    unweighted mean lands on a different literal.
    """
    values = {1: [10.0, 8.0], 2: [20.0, 5.0]}
    weights = {1: 3.0, 2: 1.0}
    result = weighted_daily_high(values, weights)
    assert result == 12.5


def test_t56_weighted_daily_high_none_when_no_weighted_feed_has_values() -> None:
    assert weighted_daily_high({}, {1: 1.0}) is None
    assert weighted_daily_high({1: []}, {1: 1.0}) is None


# --- T57: weighted_hourly renormalizes ------------------------------------


def test_t57_weighted_hourly_renormalizes_when_a_feed_is_missing() -> None:
    """weights {a:1, b:3}; b is missing at h2.

    mutant_fixed_denominator -> at the h2 assertion: correct = 20.0 (a's
    lone value, renormalized over just a's weight), mutant (dividing by
    the FULL weight sum, 4, at every hour regardless of who contributed)
    = 5.0. h1 is unaffected by this mutant (both feeds present there), so
    only h2 discriminates.
    """
    series = {1: {"h1": 10.0, "h2": 20.0}, 2: {"h1": 30.0}}
    weights = {1: 1.0, 2: 3.0}
    result = weighted_hourly(series, weights)
    assert result["h1"] == 25.0
    assert result["h2"] == 20.0


def test_t57_weighted_hourly_absent_hour_has_no_contributing_feed() -> None:
    series = {1: {"h1": 10.0}}
    weights = {2: 1.0}
    assert weighted_hourly(series, weights) == {}


# --- T58: eligibility needs both a weight at rep_k and enough hours -----


def test_t58_eligibility_needs_weight_at_rep_k_and_covered_hours() -> None:
    """feed1: eligible (weight at rep_k=0, 20h covered). feed2: weight at
    rep_k=0 exists but only 19h covered -- excluded. feed3: 20h covered but
    its rep_k (5) has no weight -- excluded. feed4: eligible, makes the
    cell servable (MIN_FEEDS=2).

    mutant_drop_hours_check -> at the assertion: correct feed set =
    {1, 4} (feed2 excluded by hours), mutant (removing the
    ``candidate.covered_hours < MIN_RUN_HOURS`` guard) = {1, 2, 4} (feed2
    wrongly eligible, since it does have a weight at its rep_k). feed2's
    weight exists specifically so the hours guard, not a missing weight,
    is what the mutant disables.
    """
    candidates = [
        _candidate(1, covered_hours=MIN_RUN_HOURS),
        _candidate(2, covered_hours=MIN_RUN_HOURS - 1),
        _candidate(3, covered_hours=MIN_RUN_HOURS),
        _candidate(4, covered_hours=MIN_RUN_HOURS),
    ]
    rep_k = {1: 0, 2: 0, 3: 5, 4: 0}
    weights = {
        (1, 0): FeedWeight(mae_ms=1.0, days=25, weight=1.0),
        (2, 0): FeedWeight(mae_ms=2.0, days=25, weight=0.5),
        (4, 0): FeedWeight(mae_ms=0.5, days=25, weight=2.0),
    }
    choice = choose_wind_feeds(candidates, rep_k, weights)
    feed_ids = {c.feed_id for c in choice.selection.feeds}
    assert feed_ids == {1, 4}
    assert choice.note is None


def test_t58_fewer_than_min_feeds_is_unavailable_with_note() -> None:
    """A paired negative: with only feed1 eligible, the cell is unavailable."""
    candidates = [_candidate(1, covered_hours=MIN_RUN_HOURS)]
    rep_k = {1: 0}
    weights = {(1, 0): FeedWeight(mae_ms=1.0, days=25, weight=1.0)}
    choice = choose_wind_feeds(candidates, rep_k, weights)
    assert choice.selection.feeds == []
    assert choice.note is not None
    assert len(choice.weights) < MIN_FEEDS
