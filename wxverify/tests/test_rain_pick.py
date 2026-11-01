"""Pure rain-model ranking (plan 2026-11-01-rain-release.md, Part C; test IDs
C1 and the switch-rule half of C2). The DB/service-level half of C2 (the
incumbent remembered across a 60-cutoff gap, lost at 61), C3-C7 and C8 are
in ``tests/test_rain_tile.py`` and ``tests/test_rain_history_fill.py``.

Written before ``wxverify.forecast.rain_pick`` exists: every test here fails
at ``ImportError`` until the module is built. No I/O, no clock, no SQLite --
every fixture is a plain in-memory value. Feed identifiers are synthetic
ints with made-up ``source``/``model`` strings; nothing real.

Assumed API (``wxverify/forecast/rain_pick.py``), for the builder to match --
the plan (§9.1 of ``plans/2026-09-29-forecast-method.md``, referenced by
Part C's C2) gives the ranking rule; this is the plainest reading of its
inputs/outputs as a pure function, since the module does not exist yet to
check against:

    @dataclass(frozen=True)
    class ModelSeries:
        feed_id: int
        source: str
        model: str
        # date -> whether THIS model forecast rain (total >= 0.2 mm,
        # round(total*100) >= 20) for that date, at the day_ahead rank_rain
        # is called for. Only "counted days" (plan C2's definition: the
        # date's truth is known AND this model has a rain_forecast_days row
        # for it at this day_ahead) are present as keys -- an unknown-truth
        # or no-forecast date is simply absent, never a False entry.
        counted: Mapping[date, bool]

    @dataclass(frozen=True)
    class RankedModel:
        feed_id: int
        rank: int             # 1-based, best first; only eligible models
        misses: int            # over the common days
        false_alarms: int      # over the common days
        common_days: int
        counted_days: int       # this model's own counted-day count
        in_group: bool          # first q = (3J+3) // 4 ranked models
        selected: bool

    @dataclass(frozen=True)
    class RainRanking:
        day_ahead: int
        provisional: bool                  # minimum off (no 2 models at 20+)
        ranked: tuple[RankedModel, ...]     # eligible models only, rank order
        selected_feed_id: int | None        # None when ranked is empty

    def rank_rain(
        *,
        day_ahead: int,
        truth: Mapping[date, bool],     # date -> is_rain; unknown dates absent
        models: Sequence[ModelSeries],
        incumbent_feed_id: int | None,
    ) -> RainRanking: ...
"""

from __future__ import annotations

from datetime import date, timedelta

from wxverify.forecast.rain_pick import ModelSeries, rank_rain

_BASE = date(2024, 1, 1)


def _days(n: int) -> list[date]:
    return [_BASE + timedelta(days=i) for i in range(n)]


def _series(
    feed_id: int,
    *,
    source: str = "synthetic-rp",
    model: str | None = None,
    counted: dict[date, bool],
) -> ModelSeries:
    return ModelSeries(
        feed_id=feed_id,
        source=source,
        model=model or f"m{feed_id}",
        counted=dict(counted),
    )


# ---------------------------------------------------------------------------
# C1: the score, the minimum/eligibility switch, common days and the drop
# rule, the tie order, and that each day ahead ranks independently.
# ---------------------------------------------------------------------------


def test_score_is_two_times_misses_plus_false_alarms() -> None:
    """3 counted days, too few for the minimum to switch on (provisional):
    model A agrees with truth every day (misses=0, false_alarms=0, score 0);
    model B misses once and false-alarms once (score 2*1 + 1 = 3). A must
    rank above B, each count exactly pinned.
    """
    d0, d1, d2 = _days(3)
    truth = {d0: True, d1: False, d2: True}
    a = _series(1, counted={d0: True, d1: False, d2: True})
    b = _series(2, counted={d0: False, d1: True, d2: True})

    ranking = rank_rain(day_ahead=0, truth=truth, models=[a, b], incumbent_feed_id=None)

    assert ranking.provisional is True  # neither model has reached 20 days
    assert [r.feed_id for r in ranking.ranked] == [1, 2]
    best, worst = ranking.ranked
    assert (best.misses, best.false_alarms) == (0, 0)
    assert (worst.misses, worst.false_alarms) == (1, 1)
    assert best.common_days == 3
    assert worst.common_days == 3


def test_minimum_switches_on_at_20_days_and_the_19_day_model_drops_out() -> None:
    """2 models reach 20 counted days: the minimum switches on (N=20), and a
    3rd model with only 19 counted days is ineligible -- excluded from the
    ranking entirely, not merely ranked last.
    """
    days20 = _days(20)
    days19 = _days(19)
    truth = {d: True for d in days20}
    a = _series(1, counted={d: True for d in days20})
    b = _series(2, counted={d: True for d in days20})
    c = _series(3, counted={d: True for d in days19})

    ranking = rank_rain(
        day_ahead=0, truth=truth, models=[a, b, c], incumbent_feed_id=None
    )

    assert ranking.provisional is False
    ranked_ids = {r.feed_id for r in ranking.ranked}
    assert ranked_ids == {1, 2}
    assert 3 not in ranked_ids


def test_common_days_drop_rule_removes_the_sparsest_model_to_reach_the_minimum() -> (
    None
):
    """3 models all reach the 20-day minimum (eligible), but their 3-way
    overlap is only 15 days -- short of N=20. The drop rule removes the
    model with the FEWEST counted days (A, exactly 20) first; the remaining
    2 models (B, C) then share 20 common days, which meets N, so dropping
    stops there and both rank.
    """
    all_days = _days(30)
    a_days = all_days[0:20]  # 20 days: 0-19
    b_days = all_days[0:25]  # 25 days: 0-24 (fewest-count tiebreak avoided)
    c_days = all_days[5:27]  # 22 days: 5-26
    truth = {d: True for d in all_days}
    a = _series(1, counted={d: True for d in a_days})
    b = _series(2, counted={d: True for d in b_days})
    c = _series(3, counted={d: True for d in c_days})

    ranking = rank_rain(
        day_ahead=0, truth=truth, models=[a, b, c], incumbent_feed_id=None
    )

    ranked_ids = {r.feed_id for r in ranking.ranked}
    assert ranked_ids == {2, 3}, "A (fewest counted days) must be dropped"
    for row in ranking.ranked:
        assert row.common_days == 20  # intersection of B's and C's days 5-24


def test_tie_breaks_on_fewer_misses_then_on_source_and_model() -> None:
    """Two separate rank_rain calls isolate the two tiebreak layers: equal
    score with unequal misses goes to fewer misses first (even against the
    (source, model) alphabetical order); equal score AND equal misses falls
    through to (source, model).
    """
    d0, d1, d2 = _days(3)
    truth = {d0: True, d1: False, d2: False}

    # Layer 1: equal score (2), unequal misses. m_fewer_misses has 0 misses,
    # 2 false alarms; m_more_misses has 1 miss, 0 false alarms. Sources are
    # chosen so the alphabetical order would pick the WRONG model if the
    # miss-count tiebreak were not applied first.
    m_fewer_misses = _series(
        10, source="zz-fewer-misses", counted={d0: True, d1: True, d2: True}
    )
    m_more_misses = _series(
        20, source="aa-more-misses", counted={d0: False, d1: False, d2: False}
    )
    ranking1 = rank_rain(
        day_ahead=0,
        truth=truth,
        models=[m_fewer_misses, m_more_misses],
        incumbent_feed_id=None,
    )
    assert [r.feed_id for r in ranking1.ranked] == [10, 20]
    assert ranking1.ranked[0].misses == 0
    assert ranking1.ranked[1].misses == 1

    # Layer 2: equal score AND equal misses -- (source, model) decides.
    m_bravo = _series(30, source="bravo", model="x", counted={d0: False, d1: True})
    m_alpha = _series(40, source="alpha", model="y", counted={d0: False, d1: True})
    ranking2 = rank_rain(
        day_ahead=0, truth=truth, models=[m_bravo, m_alpha], incumbent_feed_id=None
    )
    assert [r.feed_id for r in ranking2.ranked] == [40, 30]  # "alpha" < "bravo"


def test_each_day_ahead_is_ranked_on_its_own() -> None:
    """Two independent rank_rain calls, one per day ahead, with unrelated
    truth/model sets: each must echo its own ``day_ahead`` and reach its own
    winner, with no leakage between the calls.
    """
    d0, d1 = _days(2)
    truth_k0 = {d0: True, d1: False}
    a = _series(1, counted={d0: True, d1: False})  # perfect at k=0
    b = _series(2, counted={d0: False, d1: True})  # worst at k=0
    ranking_k0 = rank_rain(
        day_ahead=0, truth=truth_k0, models=[a, b], incumbent_feed_id=None
    )

    truth_k1 = {d0: False, d1: True}
    c = _series(3, counted={d0: False, d1: True})  # perfect at k=1
    d_ = _series(4, counted={d0: True, d1: False})  # worst at k=1
    ranking_k1 = rank_rain(
        day_ahead=1, truth=truth_k1, models=[c, d_], incumbent_feed_id=None
    )

    assert ranking_k0.day_ahead == 0
    assert ranking_k1.day_ahead == 1
    assert ranking_k0.ranked[0].feed_id == 1
    assert ranking_k1.ranked[0].feed_id == 3


def test_group_size_follows_the_q_formula() -> None:
    """4 eligible models (J=4): q = (3*4 + 3) // 4 = 3, so exactly the first
    3 ranked models are in the group.
    """
    d0, d1 = _days(2)
    truth = {d0: True, d1: True}
    models = [
        _series(i, source=f"src-{i:02d}", counted={d0: True, d1: True})
        for i in range(1, 5)
    ]
    ranking = rank_rain(day_ahead=0, truth=truth, models=models, incumbent_feed_id=None)

    assert len(ranking.ranked) == 4
    in_group = [r.in_group for r in ranking.ranked]
    assert in_group == [True, True, True, False]


# ---------------------------------------------------------------------------
# C2: the switch rule (the arithmetic itself; the incumbent-memory-across-
# cutoffs half lives in tests/test_rain_tile.py, since it depends on how
# scoring/publication.py reads rain_picks history -- not on rank_rain).
# ---------------------------------------------------------------------------


def _two_model_setup(
    *, incumbent_lead: int
) -> tuple[ModelSeries, ModelSeries, dict[date, bool]]:
    """Model BEST always agrees with truth (score 0); model INCUMBENT misses
    on every one of ``incumbent_lead`` counted days (score
    2*incumbent_lead), so BEST's score lead over INCUMBENT is exactly
    ``incumbent_lead`` points.
    """
    days = _days(incumbent_lead)
    truth = {d: True for d in days}
    best = _series(100, counted={d: True for d in days})
    incumbent = _series(200, counted={d: False for d in days})
    return best, incumbent, truth


def test_incumbent_stays_when_the_lead_is_exactly_two_points() -> None:
    best, incumbent, truth = _two_model_setup(incumbent_lead=1)  # 1 miss = 2 pts
    ranking = rank_rain(
        day_ahead=0,
        truth=truth,
        models=[best, incumbent],
        incumbent_feed_id=incumbent.feed_id,
    )
    assert ranking.selected_feed_id == incumbent.feed_id
    selected = {r.feed_id for r in ranking.ranked if r.selected}
    assert selected == {incumbent.feed_id}


def test_incumbent_switches_when_the_lead_reaches_three_points() -> None:
    # 1 miss (2 pts) + 1 false alarm (1 pt) over 2 days = 3 pts behind BEST.
    d0, d1 = _days(2)
    truth = {d0: True, d1: False}
    best = _series(100, counted={d0: True, d1: False})
    incumbent = _series(200, counted={d0: False, d1: True})
    ranking = rank_rain(
        day_ahead=0,
        truth=truth,
        models=[best, incumbent],
        incumbent_feed_id=incumbent.feed_id,
    )
    assert ranking.selected_feed_id == best.feed_id


def test_ineligible_incumbent_is_replaced_at_once_regardless_of_margin() -> None:
    """2 models reach the 20-day minimum (A, B); the incumbent, C, has only
    5 counted days and so is not eligible/ranked at all this round. The best
    eligible model must be selected outright, with no "2-point lead" grace
    for an incumbent that is not even on the board.
    """
    days20 = _days(20)
    days5 = _days(5)
    truth = {d: True for d in days20}
    a = _series(1, counted={d: True for d in days20})  # perfect, score 0
    b = _series(2, counted={d: (i != 0) for i, d in enumerate(days20)})  # 1 miss
    c = _series(3, counted={d: True for d in days5})  # ineligible: only 5 days

    ranking = rank_rain(
        day_ahead=0, truth=truth, models=[a, b, c], incumbent_feed_id=c.feed_id
    )
    assert {r.feed_id for r in ranking.ranked} == {1, 2}
    assert ranking.selected_feed_id == a.feed_id


def test_no_eligible_model_clears_the_selection() -> None:
    """No model has any counted day: ranked is empty and selected_feed_id is
    None, even though an incumbent id was passed in -- the pure contract for
    "a no-eligible row clears it".
    """
    ranking = rank_rain(
        day_ahead=0,
        truth={_BASE: True},
        models=[_series(1, counted={}), _series(2, counted={})],
        incumbent_feed_id=999,
    )
    assert ranking.ranked == ()
    assert ranking.selected_feed_id is None
