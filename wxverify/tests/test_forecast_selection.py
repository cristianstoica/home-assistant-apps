"""Unit tests for ``wxverify.forecast.selection`` — pure, no SQLite.

Spec build-sequence step 1 verify hook: "unit tests for selection, the
fallback ladder (>=N / 1 / 0 confident / no-reach), ... and that excluded
feeds never appear" (the excluded-feeds half of that hook is a data-layer
concern, covered in ``test_forecast_data.py`` — this file owns everything
`select_cell_feeds` and `representative_day_ahead` decide on their own,
given already-built ``CellCandidate`` rows).

All fixtures are plain dataclass construction — the module under test has no
I/O, so there is nothing to isolate beyond distinct ``CellCandidate`` inputs
per test.
"""

from __future__ import annotations

import pytest

from wxverify.forecast.aggregate import MIN_SPREAD_HOURS, MULTIPOINT_MIN_HOURS
from wxverify.forecast.selection import (
    CellCandidate,
    representative_day_ahead,
    select_cell_feeds,
)


def _candidate(
    feed_id: int,
    *,
    source: str = "open-meteo",
    model: str = "m",
    confident: bool = False,
    skill_score: float | None = None,
    pair_n: int = 0,
    mae: float | None = None,
    future_sample_count: int = 0,
    covered_hours: int = 24,
) -> CellCandidate:
    return CellCandidate(
        feed_id=feed_id,
        source=source,
        model=model,
        confident=confident,
        skill_score=skill_score,
        pair_n=pair_n,
        mae=mae,
        future_sample_count=future_sample_count,
        covered_hours=covered_hours,
    )


# ---------------------------------------------------------------------------
# Fallback ladder rung 1: >= N confident feeds -> blend the top N.
# ---------------------------------------------------------------------------


def test_ge_n_confident_feeds_blends_top_n_by_skill() -> None:
    candidates = [
        _candidate(1, model="a", confident=True, skill_score=0.9),
        _candidate(2, model="b", confident=True, skill_score=0.7),
        _candidate(3, model="c", confident=True, skill_score=0.5),
    ]
    selection = select_cell_feeds(candidates, blend_depth=2)
    assert [c.feed_id for c in selection.feeds] == [1, 2]
    assert selection.low_confidence is False
    assert selection.available is True


# ---------------------------------------------------------------------------
# Fallback ladder rung 2: exactly 1 confident feed -> shown alone, even when
# blend_depth allows more and a non-confident row has louder raw skill.
# ---------------------------------------------------------------------------


def test_exactly_one_confident_feed_shown_alone_ignores_louder_non_confident() -> None:
    candidates = [
        _candidate(1, model="quiet", confident=True, skill_score=0.5),
        _candidate(2, model="loud", confident=False, skill_score=0.99),
        _candidate(3, model="loudest", confident=False, skill_score=0.999),
    ]
    selection = select_cell_feeds(candidates, blend_depth=2)
    assert [c.feed_id for c in selection.feeds] == [1]
    assert selection.low_confidence is False


# ---------------------------------------------------------------------------
# Fallback ladder rung 3: 0 confident, but some scored (pair_n > 0) ->
# ignore the confidence gate, rank by pair_n desc then lowest MAE, flag
# low-confidence.
# ---------------------------------------------------------------------------


def test_zero_confident_falls_back_to_scored_ranked_by_pair_n_then_mae() -> None:
    candidates = [
        _candidate(1, model="few-pairs", pair_n=5, mae=1.0),
        _candidate(2, model="many-pairs-worse-mae", pair_n=10, mae=2.0),
        _candidate(3, model="many-pairs-better-mae", pair_n=10, mae=1.5),
    ]
    selection = select_cell_feeds(candidates, blend_depth=2)
    # pair_n=10 beats pair_n=5 regardless of MAE; among the pair_n=10 tie,
    # lower MAE (1.5) ranks ahead of higher MAE (2.0).
    assert [c.feed_id for c in selection.feeds] == [3, 2]
    assert selection.low_confidence is True


def test_scored_rung_none_mae_sorts_last() -> None:
    candidates = [
        _candidate(1, model="no-mae", pair_n=10, mae=None),
        _candidate(2, model="has-mae", pair_n=10, mae=0.5),
    ]
    selection = select_cell_feeds(candidates, blend_depth=2)
    assert [c.feed_id for c in selection.feeds] == [2, 1]
    assert selection.low_confidence is True


# ---------------------------------------------------------------------------
# Fallback ladder rung 4: 0 confident, 0 scored (fresh install, no scored
# pairs yet at all) -> rank by future-sample count desc, flag low-confidence.
# ---------------------------------------------------------------------------


def test_zero_confident_zero_scored_falls_back_to_sample_count() -> None:
    candidates = [
        _candidate(1, model="thin", future_sample_count=5),
        _candidate(2, model="thick", future_sample_count=24),
        _candidate(3, model="medium", future_sample_count=10),
    ]
    selection = select_cell_feeds(candidates, blend_depth=2)
    assert [c.feed_id for c in selection.feeds] == [2, 3]
    assert selection.low_confidence is True


# ---------------------------------------------------------------------------
# Fallback ladder rung 5: no candidates at all -> "not available".
# ---------------------------------------------------------------------------


def test_no_candidates_not_available() -> None:
    selection = select_cell_feeds([], blend_depth=2)
    assert selection.feeds == []
    assert selection.low_confidence is False
    assert selection.available is False


# Paired positive for the rung-5 negative above: a single reachable candidate
# on the cheapest rung (sample-count) IS available. Without this pair, the
# empty-candidates assertion above could pass vacuously if `available` were
# broken to always return False.
def test_single_candidate_on_cheapest_rung_is_available() -> None:
    selection = select_cell_feeds([_candidate(1, future_sample_count=1)], blend_depth=2)
    assert selection.available is True
    assert [c.feed_id for c in selection.feeds] == [1]


# ---------------------------------------------------------------------------
# Deterministic tie-break: exact-equal skill_score falls through to
# (source, model) alphabetical ordering.
# ---------------------------------------------------------------------------


def test_exact_skill_tie_breaks_alphabetically_by_source_then_model() -> None:
    candidates = [
        _candidate(1, source="zzz-source", model="a", confident=True, skill_score=0.5),
        _candidate(2, source="aaa-source", model="z", confident=True, skill_score=0.5),
    ]
    selection = select_cell_feeds(candidates, blend_depth=2)
    assert [c.feed_id for c in selection.feeds] == [2, 1]


# ---------------------------------------------------------------------------
# Defensive clamp: blend_depth < 1 (should never reach here past the
# Pydantic ge=1 gate, but the pure function clamps its own floor).
# ---------------------------------------------------------------------------


def test_blend_depth_non_positive_clamps_to_one() -> None:
    candidates = [
        _candidate(1, model="a", confident=True, skill_score=0.9),
        _candidate(2, model="b", confident=True, skill_score=0.7),
    ]
    selection = select_cell_feeds(candidates, blend_depth=0)
    assert [c.feed_id for c in selection.feeds] == [1]


# ---------------------------------------------------------------------------
# Coverage pool (pre-ladder): the two-tier adequate/multipoint gate that keeps
# a lone degenerate single-point feed from winning on skill while a feed with
# real intra-day spread is available. The pool is applied BEFORE the confidence
# ladder, so a high-skill but single-point feed can be demoted below a
# lower-skill well-covered one.
# ---------------------------------------------------------------------------


def test_coverage_gate_demotes_degenerate_high_skill_feed() -> None:
    # Two confident feeds. The higher-skill one covers only 1 hour (its daily
    # high == low -- a collapse); the lower-skill one covers 15h. The adequate
    # pool (>= MIN_SPREAD_HOURS) restricts to the 15h feed, so skill does NOT
    # win. Pre-fix (no pool) the confidence ladder ranked by skill would pick
    # the 1h feed first; this asserts coverage gates it out entirely.
    candidates = [
        _candidate(
            1, model="degenerate", confident=True, skill_score=0.99, covered_hours=1
        ),
        _candidate(
            2, model="covered", confident=True, skill_score=0.60, covered_hours=15
        ),
    ]
    # blend_depth=2 leaves room for both; the pool -- not the depth cap --
    # excludes the degenerate feed.
    selection = select_cell_feeds(candidates, blend_depth=2)
    assert [c.feed_id for c in selection.feeds] == [2]
    assert selection.low_confidence is False


def test_no_near_tile_regression_skill_decides_when_coverage_uniform() -> None:
    # Paired positive: when coverage is uniform (both adequate) the pool is a
    # no-op and skill decides, exactly as before the fix. Guards the gate from
    # over-firing on ordinary near tiles.
    candidates = [
        _candidate(1, model="high", confident=True, skill_score=0.9, covered_hours=24),
        _candidate(2, model="low", confident=True, skill_score=0.7, covered_hours=24),
    ]
    selection = select_cell_feeds(candidates, blend_depth=1)
    assert [c.feed_id for c in selection.feeds] == [1]
    assert selection.low_confidence is False


def test_min_spread_hours_boundary_pins_adequate_tier() -> None:
    # Boundary pinned against the imported constant, not a literal 12 (mirrors
    # the clears_coverage boundary style). A feed at exactly MIN_SPREAD_HOURS
    # is adequate -> alone in the pool -> wins over a higher-skill feed that
    # only reaches the multipoint tier; one hour below, it drops out of the
    # adequate tier and skill decides among the multipoint feeds instead.
    def pick(covered: int) -> int:
        boundary = _candidate(
            1, model="boundary", confident=True, skill_score=0.5, covered_hours=covered
        )
        multipoint = _candidate(
            2, model="multipoint", confident=True, skill_score=0.9, covered_hours=5
        )
        return select_cell_feeds([boundary, multipoint], blend_depth=1).feeds[0].feed_id

    assert pick(MIN_SPREAD_HOURS) == 1  # inclusive: == threshold is adequate
    assert pick(MIN_SPREAD_HOURS - 1) == 2  # just below: not adequate, skill wins


def test_multipoint_floor_selects_multipoint_over_single_slot() -> None:
    # hoare correction #2 (degeneracy floor): every feed below MIN_SPREAD_HOURS,
    # but a >= MULTIPOINT_MIN_HOURS multi-point feed present alongside a
    # 1-sample feed -> the multi-point feed is selected, the single-slot feed
    # absent, even though the single-slot out-skills it. Pins BOTH sides of the
    # MULTIPOINT_MIN_HOURS boundary: the multipoint feed sits exactly at the
    # constant (included) and the single-slot at constant-1 (excluded).
    multipoint = _candidate(
        1,
        model="multipoint",
        confident=True,
        skill_score=0.5,
        covered_hours=MULTIPOINT_MIN_HOURS,
    )
    single_slot = _candidate(
        2,
        model="single",
        confident=True,
        skill_score=0.99,
        covered_hours=MULTIPOINT_MIN_HOURS - 1,
    )
    selection = select_cell_feeds([multipoint, single_slot], blend_depth=2)
    assert [c.feed_id for c in selection.feeds] == [1]


def test_all_single_slot_falls_back_to_candidates_and_returns_a_feed() -> None:
    # Companion honest-degeneracy case: when EVERY feed is a single point both
    # tiers are empty, so the pool falls back to all candidates and selection
    # still returns the (skill-ranked) winner -- the truthful max == min tile.
    candidates = [
        _candidate(1, model="a", confident=True, skill_score=0.5, covered_hours=1),
        _candidate(2, model="b", confident=True, skill_score=0.9, covered_hours=1),
    ]
    selection = select_cell_feeds(candidates, blend_depth=1)
    assert [c.feed_id for c in selection.feeds] == [2]
    assert selection.available is True


def test_coverage_pool_sits_above_confidence_ladder() -> None:
    # The gate runs BEFORE the confidence ladder: an adequate (>=12h) feed that
    # only reaches the cheapest sample-count rung is chosen over a CONFIDENT
    # single-slot feed, and is flagged low_confidence. Pre-fix the confident
    # single-slot would win rung 1 (normal); the pool excludes it first.
    adequate_unscored = _candidate(
        1,
        model="covered",
        confident=False,
        skill_score=None,
        pair_n=0,
        future_sample_count=15,
        covered_hours=15,
    )
    confident_single = _candidate(
        2, model="degenerate", confident=True, skill_score=0.99, covered_hours=1
    )
    selection = select_cell_feeds([adequate_unscored, confident_single], blend_depth=2)
    assert [c.feed_id for c in selection.feeds] == [1]
    assert selection.low_confidence is True


# ---------------------------------------------------------------------------
# representative_day_ahead — modal day_ahead, ties resolve to the smaller
# (shorter-lead) value.
# ---------------------------------------------------------------------------


def test_representative_day_ahead_single_value() -> None:
    assert representative_day_ahead([3]) == 3


def test_representative_day_ahead_clear_majority_wins() -> None:
    assert representative_day_ahead([1, 1, 2]) == 1


def test_representative_day_ahead_tie_resolves_to_smaller_value() -> None:
    # counts equal (2 each) for day_ahead 1 and 2 -> smaller (1) wins.
    assert representative_day_ahead([1, 1, 2, 2]) == 1
    # Order-independence: same multiset, different arrival order.
    assert representative_day_ahead([2, 2, 1, 1]) == 1


def test_representative_day_ahead_distinguishes_majority_from_tie() -> None:
    # Paired against the tie case above: a genuine majority (3 vs 2) must
    # win even though it is NOT the smaller value, proving the tie-break
    # only fires on an actual count tie.
    assert representative_day_ahead([5, 5, 5, 3, 3]) == 5


def test_representative_day_ahead_empty_raises() -> None:
    with pytest.raises(ValueError, match="requires at least one value"):
        representative_day_ahead([])
