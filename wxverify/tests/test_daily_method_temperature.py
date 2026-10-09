"""§15.2 rows 1-12: temperature bias/calibration helpers and vote benchmarks.

Every expected value is derived here from the plan's §15.2 table and its
formulas (§8.1/§8.2/§9.8), never read off the implementation. Synthetic
only: no station/site identifiers are needed for these pure-function rows.
"""

from __future__ import annotations

import math

import pytest

from wxverify.forecast.daily_method import (
    ModelRef,
    asymmetric_loss,
    calibrated_chance,
    fit_calibration,
    glyph_from_votes,
    pooled_statistic,
    qualifying_sum,
    rank_by_loss,
    shrunk_bias,
    trimean,
    vote_logit,
    wet_vote,
)


def test_trimean_and_shrink() -> None:
    """Row 1: errors [0.0]*10 + [2.0]*10, shrink 10, limit 3.

    Quartiles (inclusive method) of the sorted 20 values are Q1=0.0,
    median=1.0, Q3=2.0, so trimean = (0 + 2*1 + 2) / 4 = 1.0.
    Shrink factor n/(n+shrink) = 20/30; bias = 20/30 * 1.0 = 0.6667.
    """
    errors = [0.0] * 10 + [2.0] * 10
    assert trimean(errors) == pytest.approx(1.0)  # pyright: ignore[reportUnknownMemberType]
    assert shrunk_bias(errors, shrink=10, limit=3) == pytest.approx(  # pyright: ignore[reportUnknownMemberType]
        20 / 30 * 1.0, abs=1e-4
    )


def test_limit_clamps_the_bias() -> None:
    """Row 2: errors [5.0]*30, shrink 10, limit 3.

    trimean of 30 equal values is 5.0; raw = 30/40 * 5.0 = 3.75, which the
    +-3 limit clamps down to 3.0.
    """
    errors = [5.0] * 30
    raw = 30 / 40 * trimean(errors)
    assert raw == pytest.approx(3.75)  # pyright: ignore[reportUnknownMemberType]
    assert shrunk_bias(errors, shrink=10, limit=3) == pytest.approx(3.0)  # pyright: ignore[reportUnknownMemberType]


def test_calibration_heavy_penalty_snaps_to_the_vote_share() -> None:
    """Row 3: lam=1e9, one query k=3, n=4.

    An overwhelming ridge penalty on (a, b-1) forces a -> 0 and b -> 1
    regardless of the single data point, so R1 collapses to sigmoid(x)
    where x = logit((3+0.5)/5) = logit(0.7) -- i.e. R1 = 0.7 exactly, the
    plain vote share (k+0.5)/(n+1) that the logit/sigmoid pair inverts.
    """
    x = vote_logit(3, 4)
    a, b, converged = fit_calibration(
        [x], [1], lam=1e9, max_iterations=50, tolerance=1e-9
    )
    assert converged
    assert a == pytest.approx(0.0, abs=1e-6)  # pyright: ignore[reportUnknownMemberType]
    assert b == pytest.approx(1.0, abs=1e-6)  # pyright: ignore[reportUnknownMemberType]
    assert calibrated_chance(x, a=a, b=b) == pytest.approx(0.7, abs=1e-6)  # pyright: ignore[reportUnknownMemberType]


def _row4_days() -> tuple[list[float], list[int]]:
    # (k, n, y) = (0,4,0), (1,4,0), (1,4,1), (2,4,1), (3,4,1), (4,4,1)
    days = [(0, 4, 0), (1, 4, 0), (1, 4, 1), (2, 4, 1), (3, 4, 1), (4, 4, 1)]
    xs = [vote_logit(k, n) for k, n, _y in days]
    ys = [y for _k, _n, y in days]
    return xs, ys


def test_calibration_fitted() -> None:
    """Row 4: lam=1 over six (k, n, y) days.

    Table: converged; a=0.408366, b=1.144587 (+-1e-5); R1 at k=2,n=4
    (x = logit(2.5/5) = logit(0.5) = 0, so R1 = sigmoid(a)) = 0.600696.
    """
    xs, ys = _row4_days()
    a, b, converged = fit_calibration(
        xs, ys, lam=1.0, max_iterations=100, tolerance=1e-10
    )
    assert converged
    assert a == pytest.approx(0.408366, abs=1e-5)  # pyright: ignore[reportUnknownMemberType]
    assert b == pytest.approx(1.144587, abs=1e-5)  # pyright: ignore[reportUnknownMemberType]
    x_at_2_of_4 = vote_logit(2, 4)
    assert x_at_2_of_4 == pytest.approx(0.0, abs=1e-12)  # pyright: ignore[reportUnknownMemberType]
    assert calibrated_chance(x_at_2_of_4, a=a, b=b) == pytest.approx(0.600696, abs=1e-6)  # pyright: ignore[reportUnknownMemberType]


def test_no_convergence_with_one_iteration() -> None:
    """Row 5: the same six days, max_iterations=1 -> converged is False.

    (The composer-level fallback to R0 with ``fallback_reason`` set belongs
    to a compose_* wrapper not exercised by this pure-function row; see the
    "not written" notes in the handback.)
    """
    xs, ys = _row4_days()
    _a, _b, converged = fit_calibration(
        xs, ys, lam=1.0, max_iterations=1, tolerance=1e-10
    )
    assert converged is False


@pytest.mark.parametrize(
    ("k", "n", "miss_weight", "expected_symbol"),
    [
        (5, 15, 2, True),
        (6, 15, 1.5, True),
        (4, 15, 2, False),
    ],
)
def test_exact_symbol_boundary(
    k: int, n: int, miss_weight: float, expected_symbol: bool
) -> None:
    """Row 6: k*(1+w_r) >= n, exactly at the boundary for the first two.

    5*(1+2)=15>=15 (symbol); 6*(1+1.5)=15>=15 (symbol); 4*(1+2)=12<15
    (no symbol).
    """
    assert glyph_from_votes(k, n, miss_weight=miss_weight) is expected_symbol


def test_counted_hour_sum() -> None:
    """Row 7: [0.05, 0.2, 0.0, 1.0], T=0.1 -> only 0.2 and 1.0 qualify -> 1.2."""
    assert qualifying_sum([0.05, 0.2, 0.0, 1.0], threshold_mm=0.1) == pytest.approx(1.2)  # pyright: ignore[reportUnknownMemberType]


def test_wet_vote_refuses_a_zero_threshold() -> None:
    """Row 8: wet_vote([0.0], threshold_mm=0.0) raises ValueError.

    Paired positive: a positive, in-range threshold does not raise, and
    correctly reports no wet vote for an all-zero series.
    """
    with pytest.raises(ValueError):
        wet_vote([0.0], threshold_mm=0.0)
    assert wet_vote([0.0], threshold_mm=0.1) is False


def test_pooled_per_lead_gives_equal_weight_to_each_lead() -> None:
    """Row 9: lead 0 has 20 days of error 1.0; lead 1 has 2 days of error 5.0.

    Equal weight per lead: mean(1.0, 5.0) = 3.0 -- not the day-weighted
    30/22 (which the paired "day-weighted" computation below shows would be
    a different, wrong, number if the pooling were weighted by day count).
    """
    per_lead = {0: [1.0] * 20, 1: [5.0] * 2}
    assert pooled_statistic(per_lead, min_days=1) == pytest.approx(3.0)  # pyright: ignore[reportUnknownMemberType]
    day_weighted = sum(sum(v) for v in per_lead.values()) / sum(
        len(v) for v in per_lead.values()
    )
    assert day_weighted == pytest.approx(30 / 22)  # pyright: ignore[reportUnknownMemberType]
    assert day_weighted != pytest.approx(3.0)  # pyright: ignore[reportUnknownMemberType]


def test_asymmetric_loss_the_amounts_weight() -> None:
    """Row 10: under_weight=w_amount=1.5, over_weight=1.

    F=20, y=22 (under-forecast by 2): 1.5 * 2 = 3.0.
    F=22, y=20 (over-forecast by 2): 1 * 2 = 2.0.
    """
    assert asymmetric_loss(20, 22, under_weight=1.5, over_weight=1) == pytest.approx(  # pyright: ignore[reportUnknownMemberType]
        3.0
    )
    assert asymmetric_loss(22, 20, under_weight=1.5, over_weight=1) == pytest.approx(  # pyright: ignore[reportUnknownMemberType]
        2.0
    )


def test_asymmetric_loss_the_highs_weight() -> None:
    """Row 11: under_weight=w_high=1, over_weight=1 -- symmetric, both 2.0."""
    assert asymmetric_loss(20, 22, under_weight=1, over_weight=1) == pytest.approx(2.0)  # pyright: ignore[reportUnknownMemberType]
    assert asymmetric_loss(22, 20, under_weight=1, over_weight=1) == pytest.approx(2.0)  # pyright: ignore[reportUnknownMemberType]


def test_tie_break_on_mean_under_forecast_beats_alphabetical_order() -> None:
    """Row 12: w_high=1 (symmetric loss); feed a vs feed b, both mean_loss=1.0.

    Feed a: errors (P-y) -0.5, +1.5 -> under=0.5,0; over=0,1.5 ->
    mean_under=0.25, mean_over=0.75, mean_loss=(0.5+1.5)/2=1.0.
    Feed b: errors -1.0, +1.0 -> under=1.0,0; over=0,1.0 ->
    mean_under=0.5, mean_over=0.5, mean_loss=1.0.
    Both mean losses tie at 1.0. By bare (source, model) order "SYN-a/SYN-1"
    sorts before "SYN-b/SYN-1" is false -- the plan picks feeds so that
    "feed b sorts first by (source, model)" -- reproduced here by naming
    feed a "SYN-b" and feed b "SYN-a" so plain alphabetical order would
    rank the 0.5-mean_under feed first; the under_forecast tie-break must
    override that and still choose the 0.25 feed.
    """
    feed_a = ModelRef("SYN-b", "SYN-1")  # the 0.25-mean_under feed
    feed_b = ModelRef("SYN-a", "SYN-1")  # sorts first by (source, model)
    histories = {
        feed_a: {0: [(19.5, 20.0)], 1: [(21.5, 20.0)]},  # P-y: -0.5, +1.5
        feed_b: {0: [(19.0, 20.0)], 1: [(21.0, 20.0)]},  # P-y: -1.0, +1.0
    }
    ranked = rank_by_loss(
        histories, under_weight=1, over_weight=1, tie_break="under_forecast"
    )
    assert ranked[0].mean_loss == pytest.approx(1.0)  # pyright: ignore[reportUnknownMemberType]
    assert ranked[1].mean_loss == pytest.approx(1.0)  # pyright: ignore[reportUnknownMemberType]
    assert ranked[0].feed == feed_a
    assert ranked[0].mean_under == pytest.approx(0.25)  # pyright: ignore[reportUnknownMemberType]

    # Paired control: under plain (source, model) tie-break, feed_b (which
    # sorts first alphabetically) wins instead -- proving the override is
    # real, not a fixture coincidence.
    by_name = rank_by_loss(
        histories, under_weight=1, over_weight=1, tie_break="source_model"
    )
    assert by_name[0].feed == feed_b


def test_vote_logit_is_finite_at_the_boundaries() -> None:
    """Sanity check used by the rows above: vote_logit never divides by zero."""
    assert math.isfinite(vote_logit(0, 4))
    assert math.isfinite(vote_logit(4, 4))
