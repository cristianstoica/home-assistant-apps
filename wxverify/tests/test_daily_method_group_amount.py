"""§15.2 rows 38-50: S-TOP2, the group vote and the amount benchmarks (§9.2-§9.4).

Expected values come straight from the plan's §15.2 table arithmetic.
"""

from __future__ import annotations

from fractions import Fraction

import pytest

from wxverify.forecast.daily_method import (
    amount_tradeoff,
    glyph_from_votes,
    group_amount_benchmarks,
    group_vote_chance,
    top_two_rain,
)


def test_s_top2_exact_comparison_float_trap() -> None:
    """Row 38: 0.11 + 0.29 == 0.39999999999999997 in float, but 11+29>=40 in int."""
    assert 0.11 + 0.29 == 0.39999999999999997  # noqa: PLR2004
    assert top_two_rain((0.11, 0.29), trigger_hundredths=20) is True


def test_group_vote_j7() -> None:
    """Row 42: group_fraction 3/4, J=7 (a-e at 0.0mm, f/g at 0.3mm).

    q = ceil(3/4 * 7) = ceil(5.25) = 6; the top 6 of 7 (a..f) hold one
    model (f) at or above the 0.2 mm trigger -> k=1, chance 1/6.
    """
    ranked = [
        ("SYN-a", 0.0),
        ("SYN-b", 0.0),
        ("SYN-c", 0.0),
        ("SYN-d", 0.0),
        ("SYN-e", 0.0),
        ("SYN-f", 0.3),
        ("SYN-g", 0.3),
    ]
    vote = group_vote_chance(
        ranked, group_fraction=Fraction(3, 4), trigger_hundredths=20
    )
    assert vote.j == 7
    assert vote.q == 6
    assert vote.k == 1
    assert vote.chance == Fraction(1, 6)


def test_group_vote_j4_by_rank_not_alphabetical() -> None:
    """Row 43: ranked d, c, b, a (reverse of (source, model) order); a=1.0mm.

    q = ceil(3/4 * 4) = 3; the top 3 by RANK are d, c, b -- a (the only wet
    model) is cut out of the group -> k=0, chance 0.
    """
    ranked = [("SYN-d", 0.0), ("SYN-c", 0.0), ("SYN-b", 0.0), ("SYN-a", 1.0)]
    vote = group_vote_chance(
        ranked, group_fraction=Fraction(3, 4), trigger_hundredths=20
    )
    assert vote.q == 3
    assert vote.chance == Fraction(0)


def test_few_models_one_wet_vote() -> None:
    """Row 45: J=1..4, the model at rank q (1,2,3,3) at 0.3mm, rest 0.0.

    group_fraction 3/4: q = ceil(3/4*J) -> J=1:1, J=2:2, J=3:3, J=4:3.
    Each case has exactly one wet model landing inside the group (at rank
    q) -> k=1 in every case, chance 1, 1/2, 1/3, 1/3.
    Then J=5: q = ceil(3/4*5) = 4, the wet model at rank 4 -> k=1,
    chance 1/4; glyph_from_votes(1, 4, miss_weight=2) = 1*3 >= 4 is False.
    """
    expected_q = {1: 1, 2: 2, 3: 3, 4: 3}
    expected_chance = {
        1: Fraction(1),
        2: Fraction(1, 2),
        3: Fraction(1, 3),
        4: Fraction(1, 3),
    }
    for j in (1, 2, 3, 4):
        q = expected_q[j]
        ranked = [(f"SYN-{i}", 0.0) for i in range(1, j + 1)]
        ranked[q - 1] = (ranked[q - 1][0], 0.3)
        vote = group_vote_chance(
            ranked, group_fraction=Fraction(3, 4), trigger_hundredths=20
        )
        assert vote.q == q, j
        assert vote.k == 1, j
        assert vote.chance == expected_chance[j], j
        assert glyph_from_votes(vote.k, vote.q, miss_weight=2) is True, j

    ranked5 = [(f"SYN-{i}", 0.0) for i in range(1, 6)]
    ranked5[3] = (ranked5[3][0], 0.3)
    vote5 = group_vote_chance(
        ranked5, group_fraction=Fraction(3, 4), trigger_hundredths=20
    )
    assert vote5.q == 4
    assert vote5.k == 1
    assert vote5.chance == Fraction(1, 4)
    assert glyph_from_votes(vote5.k, vote5.q, miss_weight=2) is False


def test_chance_at_one_third_shows_the_symbol() -> None:
    """Row 47: glyph_from_votes(2, 6, miss_weight=2): 2*3=6>=6 -> symbol.

    Paired negative: (1, 6): 1*3=3<6 -> no symbol.
    """
    assert glyph_from_votes(2, 6, miss_weight=2) is True
    assert glyph_from_votes(1, 6, miss_weight=2) is False


def test_amount_benchmarks_odd_q() -> None:
    """Row 48: group_fraction 3/4, a..d ranked, totals 0.0, 0.4, 2.0, 5.0.

    q = ceil(3/4*4) = 3; the group's totals are 0.0, 0.4, 2.0.
    median = 0.4; mean = (0.0+0.4+2.0)/3 = 0.8; mean - median = 0.4.
    """
    ranked = [("SYN-a", 0.0), ("SYN-b", 0.4), ("SYN-c", 2.0), ("SYN-d", 5.0)]
    benchmarks = group_amount_benchmarks(ranked, group_fraction=Fraction(3, 4))
    assert benchmarks.q == 3
    assert benchmarks.median == pytest.approx(0.4)
    assert benchmarks.mean == pytest.approx(0.8, abs=1e-9)
    assert (benchmarks.mean - benchmarks.median) == pytest.approx(0.4, abs=1e-9)


def test_amount_benchmarks_even_q() -> None:
    """Row 49: a..g ranked, totals 3.0, 0.0, 1.0, 0.0, 0.6, 0.0, 9.0.

    q = ceil(3/4*7) = 6; group totals 3.0, 0.0, 1.0, 0.0, 0.6, 0.0.
    Sorted: 0.0, 0.0, 0.0, 0.6, 1.0, 3.0 -> median = (0.0+0.6)/2 = 0.3.
    """
    ranked = [
        ("SYN-a", 3.0),
        ("SYN-b", 0.0),
        ("SYN-c", 1.0),
        ("SYN-d", 0.0),
        ("SYN-e", 0.6),
        ("SYN-f", 0.0),
        ("SYN-g", 9.0),
    ]
    benchmarks = group_amount_benchmarks(ranked, group_fraction=Fraction(3, 4))
    assert benchmarks.q == 6
    assert benchmarks.median == pytest.approx(0.3, abs=1e-9)


def test_amount_benchmarks_zeros_counted_and_amount_tradeoff() -> None:
    """Row 50: a..d ranked, totals 0.0, 0.8, 0.0, 0.0; group_fraction 3/4.

    q = ceil(3/4*4) = 3; group totals 0.0, 0.8, 0.0 -> median 0.0, mean
    0.8/3 ~= 0.26667. On a dry day the mean (>= 0.1mm) is dry_shown and the
    median (0.0 < 0.1mm) is not; on a rain-nearby day the median
    (< 0.2mm) is wet_short and the mean (>= 0.2mm) is not.
    """
    ranked = [("SYN-a", 0.0), ("SYN-b", 0.8), ("SYN-c", 0.0), ("SYN-d", 0.0)]
    benchmarks = group_amount_benchmarks(ranked, group_fraction=Fraction(3, 4))
    assert benchmarks.q == 3
    assert benchmarks.median == pytest.approx(0.0)
    assert benchmarks.mean == pytest.approx(0.8 / 3, abs=1e-9)

    assert amount_tradeoff(benchmarks.mean, outcome="dry") == "dry_shown"
    assert amount_tradeoff(benchmarks.median, outcome="dry") is None
    assert amount_tradeoff(benchmarks.median, outcome="rain_nearby") == "wet_short"
    assert amount_tradeoff(benchmarks.mean, outcome="rain_nearby") is None
