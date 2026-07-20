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
