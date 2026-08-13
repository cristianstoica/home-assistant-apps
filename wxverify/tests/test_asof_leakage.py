"""§18.1 leakage oracles for the as-of machinery (plan §6).

The hard rule under test: data dated after T — pair rows, forecast runs,
observation materializations, roster entries — must have ZERO influence on
anything computed as of T. Every test here either (a) computes an as-of
result, injects post-T decoys a leaky implementation WOULD pick up, and
asserts the recomputed result is bit-identical, or (b) pins the knowability
predicate's exact semantics (julianday instant comparison across timestamp
spellings, the versioned Δ_consensus constant, first-failing exclusion
classification, published-generation binding) with exact-value assertions.

Every fixture value is synthetic. Decision time T is far-future (2035) so
``created_at`` defaults (wall-clock now) are always <= T — which is exactly
what makes the "trust created_at" wrong implementation loud, not quiet.
"""

from __future__ import annotations

import pytest

from tests.helpers import (
    asof_conn,
    asof_insert_observation,
    asof_insert_pair,
    asof_insert_sample,
    asof_make_real_feed,
    asof_make_site,
    asof_persistence_feed_id,
)
from wxverify.forecast.data import (
    count_null_availability_samples,
    forecast_ranking,
    load_future_samples,
)
from wxverify.scoring.leaderboard import asof_leaderboard
from wxverify.scoring.metrics import strategy_for
from wxverify.verification.asof import (
    PairKnowabilityExclusions,
    pair_knowability_exclusions,
)

# Canonical decision time for these fixtures.
_T = "2035-01-02T12:00:00Z"


def test_post_t_injections_leave_asof_ranking_bit_identical() -> None:
    """Core §18.1 statement: inject post-T pairs, a post-T roster entry, a
    post-T observation, and a non-published-generation decoy — the as-of
    ranking at T must be bit-identical before and after.

    Kills: the knowability clause dropped from `_live_leaderboard` feed
    discovery (the injected post-T feed would APPEAR — roster leakage), and
    the clause dropped from `ContinuousStrategy.aggregate` (alpha's n would
    inflate and its skill would move toward the injected sq_error-0 pairs).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Leakage Ranking Site")
    alpha = asof_make_real_feed(conn, "model-alpha")
    beta = asof_make_real_feed(conn, "model-beta")
    persistence = asof_persistence_feed_id(conn)
    v1, v2 = "2035-01-02T05:00:00Z", "2035-01-02T06:00:00Z"
    i1, i2 = "2035-01-01T23:00:00Z", "2035-01-02T00:00:00Z"
    known = "2035-01-02T07:00:00Z"
    for valid_at, issued_at, alpha_fc, beta_fc, pers_fc in (
        (v1, i1, 5.0, 6.0, 6.0),  # sq: alpha 1, beta 4, persistence 4
        (v2, i2, 5.0, 6.0, 5.0),  # sq: alpha 1, beta 4, persistence 1
    ):
        for feed_id, forecast in ((alpha, alpha_fc), (beta, beta_fc)):
            asof_insert_pair(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                valid_at=valid_at,
                issued_at=issued_at,
                forecast=forecast,
                observed=4.0,
                first_known_at=known,
            )
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=persistence,
            valid_at=valid_at,
            issued_at=issued_at,
            forecast=pers_fc,
            observed=4.0,
            first_known_at=known,
        )

    baseline = forecast_ranking(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        as_of=_T,
        declared_min_n=2,
        declared_window_days=None,
    )
    # Non-vacuous baseline: both real feeds present, scored, confident.
    assert set(baseline) == {alpha, beta}
    assert baseline[alpha].n == 2
    assert baseline[alpha].skill_score is not None
    assert abs(baseline[alpha].skill_score - (1.0 - 1.0 / 2.5)) < 1e-12
    assert baseline[alpha].confident and baseline[beta].confident
    assert pair_knowability_exclusions(
        conn, site_id=site_id, variable="temperature", day_ahead=1, as_of=_T
    ) == PairKnowabilityExclusions(0, 0, 0, 0)

    # --- Post-T injections (each one a decoy a leaky query WOULD pick up) ---
    post_t = "2035-01-03T07:00:00Z"
    # (a) A whole feed first known only after T: must not enter the roster.
    gamma = asof_make_real_feed(conn, "model-gamma-post-t")
    for valid_at, issued_at in ((v1, i1), (v2, i2)):
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=gamma,
            valid_at=valid_at,
            issued_at=issued_at,
            forecast=4.0,
            observed=4.0,
            first_known_at=post_t,
        )
    # (b) Perfect post-T pairs on alpha (sq_error 0 — would raise its skill).
    for valid_at in ("2035-01-02T07:00:00Z", "2035-01-02T08:00:00Z"):
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=alpha,
            valid_at=valid_at,
            issued_at=i2,
            forecast=4.0,
            observed=4.0,
            first_known_at="2035-01-02T13:00:00Z",
        )
    # (c) A catastrophic post-T persistence pair (sq_error 10000 — would
    # explode the paired-skill reference if it leaked in).
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=persistence,
        valid_at="2035-01-02T07:00:00Z",
        issued_at=i2,
        forecast=104.0,
        observed=4.0,
        first_known_at="2035-01-02T13:00:00Z",
    )
    # (d) A post-T observation materialization at a pair-free valid_at (a
    # post-T ingestion record; no legitimate influence path exists for it).
    asof_insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-02T09:00:00Z",
        value=4.0,
        computed_at="2035-01-02T15:00:00Z",
    )
    # (e) A fully-knowable, sq_error-0 alpha pair under a NON-published
    # generation: outside every read path, not an as-of exclusion.
    cur = conn.execute(
        """
        INSERT INTO timezone_generations (site_id, timezone, mode, state)
        VALUES (?, 'UTC', 'retrospective_correction', 'building')
        """,
        (site_id,),
    )
    assert cur.lastrowid is not None
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=alpha,
        valid_at="2035-01-02T03:00:00Z",
        issued_at=i1,
        forecast=4.0,
        observed=4.0,
        first_known_at="2035-01-02T04:00:00Z",
        generation_id=int(cur.lastrowid),
    )

    after = forecast_ranking(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        as_of=_T,
        declared_min_n=2,
        declared_window_days=None,
    )
    assert after == baseline
    # The injected published-generation post-T pairs are excluded with a
    # recorded reason — exactly once each, all under first_known_after_t.
    assert pair_knowability_exclusions(
        conn, site_id=site_id, variable="temperature", day_ahead=1, as_of=_T
    ) == PairKnowabilityExclusions(
        null_first_known_at=0,
        first_known_after_t=5,
        consensus_lag_after_t=0,
        computed_at_after_t=0,
    )


def test_post_t_sample_backfill_leaves_asof_selection_bit_identical() -> None:
    """Ingestion-backfill guard (§18.1): runs seeded with pre-T `issued_at`/
    `valid_at` but post-T `fetched_at` — and post-T-issued runs with
    adversarially pre-T `fetched_at` — must leave `load_future_samples(as_of=T)`
    bit-identical, and a feed whose only runs are post-T-fetched must be
    absent at the FEED level, not just outscored.

    Kills: the `issued_at <= T` bound dropped from the as-of clause (the
    post-T-issued run would win the latest-run pick), and the
    `fetched_at <= T` bound dropped (the backfilled newer run would win).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Backfill Site")
    feed_a = asof_make_real_feed(conn, "model-a")
    slot1, slot2 = "2035-01-03T06:00:00Z", "2035-01-03T07:00:00Z"
    # Baseline run R1: fully available at T.
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-01T00:00:00Z",
        valid_at=slot1,
        lead_hours=30,
        value=1.0,
        fetched_at="2035-01-01T00:10:00Z",
    )
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-01T00:00:00Z",
        valid_at=slot2,
        lead_hours=31,
        value=1.5,
        fetched_at="2035-01-01T00:10:00Z",
    )
    since = "2035-01-01T00:00:00Z"
    baseline = load_future_samples(
        conn, site_id=site_id, since_valid_at=since, as_of=_T
    )
    assert [row.value for row in baseline] == [1.0, 1.5]

    # --- Post-T injections ---
    # Backfilled newer run: issued before T, fetched after T (both slots).
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-02T00:00:00Z",
        valid_at=slot1,
        lead_hours=30,
        value=2.0,
        fetched_at="2035-01-02T18:00:00Z",
    )
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-02T00:00:00Z",
        valid_at=slot2,
        lead_hours=31,
        value=2.5,
        fetched_at="2035-01-02T18:00:00Z",
    )
    # Issued after T, fetched (adversarially) before T: the issued_at bound
    # must exclude it on its own.
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-02T13:00:00Z",
        valid_at=slot1,
        lead_hours=17,
        value=3.0,
        fetched_at="2035-01-02T11:00:00Z",
    )
    # NULL fetched_at: unplaceable on the as-of timeline, counted not silent.
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        issued_at="2035-01-02T01:00:00Z",
        valid_at=slot2,
        lead_hours=30,
        value=3.5,
        fetched_at=None,
    )
    # A second feed whose ONLY runs are backfill-shaped (pre-T issued/valid,
    # post-T fetched): absent from the as-of selection entirely.
    feed_b = asof_make_real_feed(conn, "model-b-backfilled")
    asof_insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_b,
        issued_at="2035-01-02T02:00:00Z",
        valid_at=slot1,
        lead_hours=28,
        value=4.0,
        fetched_at="2035-01-02T20:00:00Z",
    )

    after = load_future_samples(conn, site_id=site_id, since_valid_at=since, as_of=_T)
    assert after == baseline
    assert {row.feed_id for row in after} == {feed_a}
    # Paired positive: the injected rows ARE live-visible — the as-of absence
    # above is the bound at work, not invalid rows being dropped.
    live = load_future_samples(conn, site_id=site_id, since_valid_at=since)
    assert {row.value for row in live} == {3.0, 3.5, 4.0}
    assert (
        count_null_availability_samples(conn, site_id=site_id, since_valid_at=since)
        == 1
    )


def test_paired_skill_binds_knowability_to_each_join_side() -> None:
    """The knowability predicate applies to BOTH sides of the paired-skill
    join independently — dropping either side lands on a different, wrong
    skill value.

    v1: both sides knowable (fp sq 1, pp sq 4) — the only surviving row.
    v2: fp knowable, pp post-T (pp sq 1) — dropping the pp-side predicate
        admits it -> skill 1 - 101/5.
    v3: fp post-T (fp sq 225), pp knowable (pp sq 25) — dropping the fp-side
        predicate admits it -> skill 1 - 226/29.

    Kills: `asof_pp` dropped from `_paired_skill`; `asof_fp` dropped from
    `_paired_skill` (each lands on its own distinct wrong value).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Paired Sides Site")
    feed_id = asof_make_real_feed(conn, "model-sides")
    persistence = asof_persistence_feed_id(conn)
    pre_t, post_t = "2035-01-02T07:00:00Z", "2035-01-02T14:00:00Z"
    rows: tuple[tuple[str, float, str, float, str], ...] = (
        # (valid_at, fp forecast, fp known, pp forecast, pp known)
        ("2035-01-02T05:00:00Z", 5.0, pre_t, 6.0, pre_t),  # both knowable
        ("2035-01-02T06:00:00Z", 14.0, pre_t, 5.0, post_t),  # pp post-T
        ("2035-01-02T04:00:00Z", 19.0, post_t, 9.0, pre_t),  # fp post-T
    )
    for valid_at, fp_fc, fp_known, pp_fc, pp_known in rows:
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            valid_at=valid_at,
            issued_at="2035-01-01T20:00:00Z",
            forecast=fp_fc,
            observed=4.0,
            first_known_at=fp_known,
        )
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=persistence,
            valid_at=valid_at,
            issued_at="2035-01-01T20:00:00Z",
            forecast=pp_fc,
            observed=4.0,
            first_known_at=pp_known,
        )
    asof = strategy_for("temperature").aggregate(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_cutoff=None,
        min_n=1,
        as_of=_T,
    )
    # n counts fp-knowable rows (v1 + v2); skill pairs only v1: 1 - 1/4.
    assert asof.n == 2
    assert asof.skill_score is not None
    assert abs(asof.skill_score - 0.75) < 1e-12
    # Live control: all three rows join.
    live = strategy_for("temperature").aggregate(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_cutoff=None,
        min_n=1,
    )
    assert live.skill_score is not None
    expected_live = 1.0 - ((1.0 + 100.0 + 225.0) / 3.0) / ((4.0 + 1.0 + 25.0) / 3.0)
    assert abs(live.skill_score - expected_live) < 1e-12


def test_julianday_instant_semantics_across_timestamp_spellings() -> None:
    """Timestamp comparisons are instant comparisons via julianday(), never
    lexical: stored stamps mix `Z`, `+00:00`, and fractional-second spellings.

    Pair F: first_known `12:00:00.500Z` — half a second AFTER T spelled
    `12:00:00Z`. Lexically '.' < 'Z' makes it look <= T (anti-conservative
    LEAK); julianday excludes it.
    Pair G: first_known `12:00:00+00:00` — exactly T, differently spelled.
    Lexically '+' < 'Z' happens to admit it; the boundary `<=` must too.
    Pair I: first_known `12:00:00Z` probed with as_of spelled `+00:00` —
    lexically 'Z' > '+' would WRONGLY exclude it (over-exclusion direction).
    Pair H: fractional-second valid_at, lag boundary exactly at T.

    Kills: the first_known julianday comparison rewritten to a lexical
    string compare; the first_known boundary `<=` tightened to `<`.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Spellings Site")
    feed_id = asof_make_real_feed(conn, "model-spell")
    # F: sub-second after T.
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T05:00:00Z",
        issued_at="2035-01-01T23:00:00Z",
        forecast=13.0,
        observed=4.0,
        first_known_at="2035-01-02T12:00:00.500Z",
    )
    # G: exactly T, offset spelling.
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T06:00:00Z",
        issued_at="2035-01-02T00:00:00Z",
        forecast=6.0,
        observed=4.0,
        first_known_at="2035-01-02T12:00:00+00:00",
    )
    # H: fractional-second valid_at; valid_at + 3 h == T exactly.
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T09:00:00.000Z",
        issued_at="2035-01-02T03:00:00Z",
        forecast=5.0,
        observed=4.0,
        first_known_at="2035-01-02T10:00:00Z",
    )
    # I: Z-spelled first_known, exactly T.
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T07:00:00Z",
        issued_at="2035-01-02T01:00:00Z",
        forecast=7.0,
        observed=4.0,
        first_known_at="2035-01-02T12:00:00Z",
    )

    def n_and_mae(as_of: str) -> tuple[int, float | None]:
        result = strategy_for("temperature").aggregate(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            day_ahead=1,
            window_cutoff=None,
            min_n=1,
            as_of=as_of,
        )
        return result.n, result.mae

    # T spelled Z: G, H, I knowable (abs errors 2, 1, 3); F excluded.
    n, mae = n_and_mae(_T)
    assert n == 3
    assert mae is not None and abs(mae - 2.0) < 1e-12
    # T spelled +00:00: identical instant, identical result.
    n_offset, mae_offset = n_and_mae("2035-01-02T12:00:00+00:00")
    assert (n_offset, mae_offset) == (n, mae)
    # F is excluded with the right reason, exactly once.
    assert pair_knowability_exclusions(
        conn, site_id=site_id, variable="temperature", day_ahead=1, as_of=_T
    ) == PairKnowabilityExclusions(
        null_first_known_at=0,
        first_known_after_t=1,
        consensus_lag_after_t=0,
        computed_at_after_t=0,
    )


def test_consensus_lag_is_the_live_versioned_constant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Δ_consensus comes from `methodology.CONSENSUS_LAG_HOURS` at call time,
    never a baked-in literal, and the lag boundary is inclusive.

    pair1: valid 09:00, +3 h == T exactly -> knowable (inclusive boundary).
    pair2: valid 10:00, +3 h > T -> excluded under consensus_lag, even though
    first_known_at is comfortably before T.
    Re-running with the constant rebound to 1 admits both; rebound to 9
    excludes both — proving the SQL reads the live constant.

    Kills: `'+{CONSENSUS_LAG_HOURS} hours'` hardcoded to `'+3 hours'` in
    asof.py; the lag boundary `<=` tightened to `<`.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Lag Constant Site")
    feed_id = asof_make_real_feed(conn, "model-lag")
    for valid_at, known in (
        ("2035-01-02T09:00:00Z", "2035-01-02T09:30:00Z"),
        ("2035-01-02T10:00:00Z", "2035-01-02T10:30:00Z"),
    ):
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            valid_at=valid_at,
            issued_at="2035-01-02T03:00:00Z",
            forecast=5.0,
            observed=4.0,
            first_known_at=known,
        )

    def asof_n() -> int:
        return (
            strategy_for("temperature")
            .aggregate(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="temperature",
                day_ahead=1,
                window_cutoff=None,
                min_n=1,
                as_of=_T,
            )
            .n
        )

    def lag_bucket() -> int:
        return pair_knowability_exclusions(
            conn, site_id=site_id, variable="temperature", day_ahead=1, as_of=_T
        ).consensus_lag_after_t

    assert (asof_n(), lag_bucket()) == (1, 1)
    monkeypatch.setattr("wxverify.verification.asof.CONSENSUS_LAG_HOURS", 1)
    assert (asof_n(), lag_bucket()) == (2, 0)
    monkeypatch.setattr("wxverify.verification.asof.CONSENSUS_LAG_HOURS", 9)
    assert (asof_n(), lag_bucket()) == (0, 2)


def test_exclusion_reasons_count_each_row_exactly_once() -> None:
    """First-failing classification: rows built to fail SEVERAL knowability
    conditions at once count under exactly one reason — the first failing
    one in predicate order.

    P1 fails first_known AND lag AND computed-guard -> first_known_after_t.
    P2 passes first_known, fails lag AND computed-guard -> consensus_lag.
    P3 passes first_known and lag, fails computed-guard -> computed_at_after_t.
    P4 has NULL first_known and would also fail lag -> null_first_known_at.
    P5 is the knowable control.

    Kills: `{known} AND` dropped from the consensus bucket (P1 would count
    twice); any bucket-order/reclassification mutation moves an exact count.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Classification Site")
    feed_id = asof_make_real_feed(conn, "model-class")
    matrix: tuple[tuple[str, str | None, str | None], ...] = (
        # (valid_at, first_known_at, obs computed_at)
        ("2035-01-02T11:00:00Z", "2035-01-02T13:00:00Z", "2035-01-02T13:00:00Z"),
        ("2035-01-02T10:00:00Z", "2035-01-02T11:00:00Z", "2035-01-02T13:30:00Z"),
        ("2035-01-02T05:00:00Z", "2035-01-02T07:00:00Z", "2035-01-02T13:00:00Z"),
        ("2035-01-02T10:30:00Z", None, "2035-01-02T14:00:00Z"),
        ("2035-01-02T04:00:00Z", "2035-01-02T06:00:00Z", "2035-01-02T08:00:00Z"),
    )
    for valid_at, first_known_at, computed_at in matrix:
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            valid_at=valid_at,
            issued_at="2035-01-01T20:00:00Z",
            forecast=5.0,
            observed=4.0,
            first_known_at=first_known_at,
        )
        asof_insert_observation(
            conn,
            site_id=site_id,
            valid_at=valid_at,
            value=4.0,
            computed_at=computed_at,
        )
    exclusions = pair_knowability_exclusions(
        conn, site_id=site_id, variable="temperature", day_ahead=1, as_of=_T
    )
    assert exclusions == PairKnowabilityExclusions(
        null_first_known_at=1,
        first_known_after_t=1,
        consensus_lag_after_t=1,
        computed_at_after_t=1,
    )
    assert exclusions.total == 4
    assert sum(exclusions.as_reasons().values()) == 4
    # Exactly the control survives into the as-of aggregate.
    assert (
        strategy_for("temperature")
        .aggregate(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            day_ahead=1,
            window_cutoff=None,
            min_n=1,
            as_of=_T,
        )
        .n
        == 1
    )


def test_knowability_and_exclusions_are_published_generation_bound() -> None:
    """Both the as-of aggregate and the exclusion accounting see ONLY the
    site's published generation: retired-generation rows are outside every
    read path, not an as-of exclusion.

    Under the retired initial generation: a fully-knowable decoy (abs error
    0 — would drag mae down) and an unknowable decoy (would inflate the
    exclusion count). Under the published generation: one knowable pair
    (abs error 3) and one unknowable pair.

    Kills: `published_generation_clause` dropped from the
    `pair_knowability_exclusions` statement (count would become 2).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Generation Bound Site")
    feed_id = asof_make_real_feed(conn, "model-gen")
    from wxverify.db.runtime_state import set_runtime_state
    from wxverify.db.tz_generations import (
        ensure_published_generation,
        published_pointer_key,
    )

    gen_a = ensure_published_generation(conn, site_id)
    # Decoys under generation A (to be retired).
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T05:00:00Z",
        issued_at="2035-01-01T23:00:00Z",
        forecast=4.0,
        observed=4.0,
        first_known_at="2035-01-02T06:00:00Z",
        generation_id=gen_a,
    )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T06:00:00Z",
        issued_at="2035-01-01T23:00:00Z",
        forecast=4.0,
        observed=4.0,
        first_known_at="2035-01-02T14:00:00Z",
        generation_id=gen_a,
    )
    # Publish generation B and retire A.
    cur = conn.execute(
        """
        INSERT INTO timezone_generations
            (site_id, timezone, mode, state, published_at)
        VALUES (?, 'UTC', 'retrospective_correction', 'published',
                '2035-01-01T00:00:00Z')
        """,
        (site_id,),
    )
    assert cur.lastrowid is not None
    gen_b = int(cur.lastrowid)
    conn.execute("UPDATE timezone_generations SET state='retired' WHERE id=?", (gen_a,))
    set_runtime_state(conn, published_pointer_key(site_id), str(gen_b))
    # Published-generation rows: one knowable (abs error 3), one unknowable.
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T05:00:00Z",
        issued_at="2035-01-01T23:00:00Z",
        forecast=7.0,
        observed=4.0,
        first_known_at="2035-01-02T06:00:00Z",
        generation_id=gen_b,
    )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T06:00:00Z",
        issued_at="2035-01-01T23:00:00Z",
        forecast=5.0,
        observed=4.0,
        first_known_at="2035-01-02T13:00:00Z",
        generation_id=gen_b,
    )

    assert pair_knowability_exclusions(
        conn, site_id=site_id, variable="temperature", day_ahead=1, as_of=_T
    ) == PairKnowabilityExclusions(
        null_first_known_at=0,
        first_known_after_t=1,
        consensus_lag_after_t=0,
        computed_at_after_t=0,
    )
    result = strategy_for("temperature").aggregate(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_cutoff=None,
        min_n=1,
        as_of=_T,
    )
    assert result.n == 1
    assert result.mae is not None and abs(result.mae - 3.0) < 1e-12


def test_persistence_pair_with_post_t_source_observation_is_absent() -> None:
    """Spec-named §18.1 case: a persistence pair whose source observation was
    computed after T carries a post-T `first_known_at` and is absent from the
    as-of ranking — plus the mandated non-vacuity companion: with knowable
    persistence pairs present, the as-of continuous ranking returns non-NULL
    skill and a confident feed, so the NULL/knowability exclusions cannot
    silently empty the denominator.

    Kills: the knowability predicate rewritten to trust `created_at` (the
    consensus-rewritten column §6 forbids) — `created_at` defaults to
    wall-clock now (<= the 2035 T), so the post-T persistence pair would
    reappear (persistence n -> 2) AND the candidate's skill would move from
    0.75 to 0.6.
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "Persistence Source Site")
    feed_id = asof_make_real_feed(conn, "model-real")
    persistence = asof_persistence_feed_id(conn)
    v1, v2 = "2035-01-02T05:00:00Z", "2035-01-02T06:00:00Z"
    for valid_at, known in ((v1, "2035-01-02T06:30:00Z"), (v2, "2035-01-02T07:00:00Z")):
        asof_insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            valid_at=valid_at,
            issued_at="2035-01-01T23:00:00Z",
            forecast=5.0,
            observed=4.0,
            first_known_at=known,
        )
    # Persistence pair at v1: knowable (source observation computed pre-T).
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=persistence,
        valid_at=v1,
        issued_at="2035-01-01T23:00:00Z",
        forecast=6.0,
        observed=4.0,
        first_known_at="2035-01-02T06:45:00Z",
    )
    # Persistence pair at v2: source observation computed only after T ->
    # first_known_at (populated FROM that computed_at) is post-T.
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=persistence,
        valid_at=v2,
        issued_at="2035-01-01T23:00:00Z",
        forecast=5.0,
        observed=4.0,
        first_known_at="2035-01-02T14:00:00Z",
    )
    rows = asof_leaderboard(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        as_of=_T,
        min_n=1,
        window_days=None,
    )
    by_feed = {row.feed_id: row for row in rows}
    assert by_feed[persistence].n == 1  # the post-T-source pair is absent
    candidate = by_feed[feed_id]
    # Non-vacuity companion: persistence pairs are ADMITTED, skill computes.
    assert candidate.skill_score is not None
    assert abs(candidate.skill_score - 0.75) < 1e-12
    assert candidate.confident
