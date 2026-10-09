"""Unit tests of ``daily_method``'s pooled-bias regime (plan §15.2).

Covers the ``pooled_bias`` rows and the two composer rows ("pooled bias in
the composers" and "...two feeds"). Every row is cited by section and row
name, not by line number, because the plan's line numbers drift between
commits.
"""

from __future__ import annotations

import pytest

from wxverify.forecast.daily_method import (
    FeedCase,
    ModelRef,
    compose_iw_bc,
    compose_td_k_bc,
    pooled_bias,
)


def test_pooled_bias_per_lead() -> None:
    """§15.2 "pooled bias per lead": each lead shrunk on its own count, averaged."""
    value = pooled_bias(
        {0: [0.0] * 30, 1: [2.0] * 10, 2: [], 3: [], 4: [], 5: []},
        shrink=10,
        limit=3,
    )
    # 30/40 * 0.0 = 0.0 and 10/20 * 2.0 = 1.0, mean 0.5.
    assert value == pytest.approx(0.5, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]


def test_pooled_bias_limit() -> None:
    """§15.2 "pooled bias limit": the mean is limited, not each lead's value.

    The row also pins that the clamped result can be the int ``3`` rather
    than the float ``3.0`` when ``min(limit, ...)`` binds: the assertion is
    by value only, never by type or repr.
    """
    value = pooled_bias(
        {0: [5.0] * 30, 1: [5.0] * 10, 2: [], 3: [], 4: [], 5: []},
        shrink=10,
        limit=3,
    )
    # 30/40 * 5.0 = 3.75 and 10/20 * 5.0 = 2.5, mean 3.125, limited to 3.0.
    assert value == pytest.approx(3.0, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]


def test_pooled_bias_one_day_at_a_lead() -> None:
    """§15.2 "pooled bias, one day at a lead": trimean of one value is that value."""
    value = pooled_bias({0: [0.0] * 30, 1: [2.2]}, shrink=10, limit=3)
    # 30/40 * 0.0 = 0.0 and 1/11 * 2.2 = 0.2, mean 0.1.
    assert value == pytest.approx(0.1, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]


def test_pooled_bias_shrinkage_sensitivity() -> None:
    """§15.2 "pooled bias, shrinkage sensitivity": shrink moves the per-lead weight."""
    per_lead = {0: [0.0] * 30, 1: [2.0] * 10, 2: [], 3: [], 4: [], 5: []}
    low_shrink = pooled_bias(per_lead, shrink=5, limit=3)
    high_shrink = pooled_bias(per_lead, shrink=20, limit=3)
    # shrink 5: 30/35 * 0.0 = 0.0 and 10/15 * 2.0 = 1.3333, mean 0.6667.
    assert low_shrink == pytest.approx(2.0 / 3.0, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]
    # shrink 20: 0.0 and 10/30 * 2.0 = 0.6667, mean 0.3333.
    assert high_shrink == pytest.approx(1.0 / 3.0, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]


def _two_feed_composer_fixture() -> tuple[FeedCase, FeedCase]:
    """The feeds of §15.2 "pooled bias in the composers, two feeds"."""
    syn_a = FeedCase(
        feed=ModelRef(source="SYN", model="a"),
        value=20.0,
        training={
            0: [(20.0, 20.0)] * 30,
            1: [(22.0, 20.0)] * 10,
        },
    )
    syn_b = FeedCase(
        feed=ModelRef(source="SYN", model="b"),
        value=22.0,
        training={
            0: [(21.0, 20.0)] * 30,
            3: [(24.0, 20.0)] * 10,
        },
    )
    return syn_a, syn_b


def test_pooled_bias_in_the_composers() -> None:
    """§15.2 "pooled bias in the composers": one feed, both composers agree."""
    syn_a = FeedCase(
        feed=ModelRef(source="SYN", model="a"),
        value=20.0,
        training={
            0: [(20.0, 20.0)] * 30,
            1: [(22.0, 20.0)] * 10,
        },
    )
    control = 18.0
    lead = 2
    min_days = 20
    pooled_leads = range(6)
    shrink = 10.0
    limit = 3.0
    iw_bc = compose_iw_bc(
        feeds=[syn_a],
        floor=0.1,
        control=control,
        lead=lead,
        min_days=min_days,
        pooled_leads=pooled_leads,
        shrink=shrink,
        limit=limit,
    )
    td_k_bc = compose_td_k_bc(
        feeds=[syn_a],
        k=1,
        under_weight=1.0,
        over_weight=1.0,
        tie_break="under_forecast",
        control=control,
        lead=lead,
        min_days=min_days,
        pooled_leads=pooled_leads,
        shrink=shrink,
        limit=limit,
    )
    assert iw_bc.regime == "pooled"
    assert td_k_bc.regime == "pooled"
    # SYN-a's bias is 0.5; both composers give 19.5 with a single feed.
    assert iw_bc.biases[syn_a.feed.key] == pytest.approx(0.5, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]
    assert iw_bc.value == pytest.approx(19.5, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]
    assert td_k_bc.value == pytest.approx(19.5, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]


def test_pooled_bias_in_the_composers_two_feeds() -> None:
    """§15.2 "pooled bias in the composers, two feeds": IW-BC vs TD-K-BC weights."""
    syn_a, syn_b = _two_feed_composer_fixture()
    control = 18.0
    lead = 2
    min_days = 20
    pooled_leads = range(6)
    shrink = 10.0
    limit = 3.0
    iw_bc = compose_iw_bc(
        feeds=[syn_a, syn_b],
        floor=0.1,
        control=control,
        lead=lead,
        min_days=min_days,
        pooled_leads=pooled_leads,
        shrink=shrink,
        limit=limit,
    )
    td_k_bc = compose_td_k_bc(
        feeds=[syn_a, syn_b],
        k=1,
        under_weight=1.0,
        over_weight=1.0,
        tie_break="under_forecast",
        control=control,
        lead=lead,
        min_days=min_days,
        pooled_leads=pooled_leads,
        shrink=shrink,
        limit=limit,
    )
    assert iw_bc.biases[syn_a.feed.key] == pytest.approx(0.5, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]
    assert iw_bc.biases[syn_b.feed.key] == pytest.approx(1.375, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]
    # IW-BC = (1.0 * 19.5 + 0.6667 * 20.625) / 1.6667 = 19.95.
    assert iw_bc.value == pytest.approx(19.95, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]
    # TD-K-BC ranks SYN-a first (the smaller corrected loss) and gives 19.5.
    assert td_k_bc.value == pytest.approx(19.5, abs=1e-9)  # pyright: ignore[reportUnknownMemberType]
    assert td_k_bc.ranks[0] == syn_a.feed.key
