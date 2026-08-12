"""As-of machinery + outcome knowability (plan §6) — implementer tests.

Covers the phase-2 seams the §18.1/§18.6 oracle families will build on:

* ``resolve_window(now=T)`` anchors relative cutoffs at T, not wall clock;
* ``load_future_samples(as_of=T)`` bounds by ``issued_at``/``fetched_at``
  BEFORE the latest-run pick, and NULL-availability rows are excluded and
  countable (recorded-reason policy);
* the knowability predicate — ``first_known_at <= T AND valid_at +
  DELTA_consensus <= T`` plus the ``computed_at`` AND-guard — with
  per-reason exclusion accounting;
* paired skill applies knowability to BOTH sides of the join;
* ``asof_leaderboard`` is a declared-configuration counterfactual (frozen
  ``min_n``/window, no settings/score_cache reads) and ``forecast_ranking``
  applies the same eligibility exclusions on live and as-of paths.

All fixture values are synthetic.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast.data import (
    count_null_availability_samples,
    forecast_ranking,
    load_future_samples,
)
from wxverify.scoring.leaderboard import (
    LeaderboardRow,
    asof_leaderboard,
    resolve_window,
)
from wxverify.scoring.metrics import strategy_for
from wxverify.verification.asof import (
    PairKnowabilityExclusions,
    pair_knowability_exclusions,
)

# Canonical decision time for these fixtures.
_T = "2035-01-02T12:00:00Z"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 47.0, 25.0, 900.0, 'UTC')
        """,
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _make_real_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('example-src', ?, 1, 360, 48)
        """,
        (model,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _persistence_feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    value: float,
    fetched_at: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, ?, ?, '{}', 'run-x', ?)
        """,
        (site_id, feed_id, issued_at, valid_at, lead_hours, value, fetched_at),
    )


def _insert_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    valid_at: str,
    issued_at: str,
    forecast: float,
    observed: float,
    first_known_at: str | None,
    day_ahead: int = 1,
    lead_hours: int = 6,
) -> None:
    """Direct generation-bound pair insert to craft knowability scenarios."""
    generation_id = ensure_published_generation(conn, site_id)
    error = forecast - observed
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             first_known_at, tz_generation_id)
        VALUES (?, ?, 'temperature', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            issued_at,
            valid_at,
            lead_hours,
            day_ahead,
            forecast,
            observed,
            error,
            abs(error),
            error**2,
            first_known_at,
            generation_id,
        ),
    )


def _insert_observation(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    valid_at: str,
    value: float,
    computed_at: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO observations
            (site_id, variable, valid_at, value, n_stations, computed_at)
        VALUES (?, 'temperature', ?, ?, 3, ?)
        """,
        (site_id, valid_at, value, computed_at),
    )


# ---------------------------------------------------------------------------
# resolve_window(now=T)
# ---------------------------------------------------------------------------


def test_resolve_window_now_anchors_relative_cutoffs_at_t() -> None:
    conn = _conn()
    t = parse_utc(_T)
    assert resolve_window(conn, "7d", now=t).cutoff == isoformat_utc(
        t - timedelta(days=7)
    )
    # Default rolling window (30 d) anchored at T.
    assert resolve_window(conn, "rolling", now=t).cutoff == isoformat_utc(
        t - timedelta(days=30)
    )
    # "all" has no cutoff regardless of the anchor.
    assert resolve_window(conn, "all", now=t).cutoff is None


def test_resolve_window_default_now_is_wall_clock() -> None:
    conn = _conn()
    before = utc_now() - timedelta(days=7, minutes=1)
    cutoff = resolve_window(conn, "7d").cutoff
    after = utc_now() - timedelta(days=7) + timedelta(minutes=1)
    assert cutoff is not None
    assert before < parse_utc(cutoff) < after


# ---------------------------------------------------------------------------
# load_future_samples(as_of=T) + NULL-availability accounting
# ---------------------------------------------------------------------------


def test_load_future_samples_asof_bounds_before_latest_run_pick() -> None:
    conn = _conn()
    site_id = _make_site(conn, "AsOf Samples Site")
    feed_id = _make_real_feed(conn, "model-a")
    valid_at = "2035-01-03T06:00:00Z"
    # Older run: fully available at T.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        issued_at="2035-01-01T00:00:00Z",
        valid_at=valid_at,
        lead_hours=30,
        value=1.0,
        fetched_at="2035-01-01T00:10:00Z",
    )
    # Newer run for the SAME slot, fetched only after T: at T the older run
    # must win the latest-run pick; live (as_of=None) picks the newer one.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        issued_at="2035-01-02T00:00:00Z",
        valid_at=valid_at,
        lead_hours=30,
        value=2.0,
        fetched_at="2035-01-02T18:00:00Z",
    )
    live = load_future_samples(
        conn, site_id=site_id, since_valid_at="2035-01-01T00:00:00Z"
    )
    assert [row.value for row in live] == [2.0]
    asof = load_future_samples(
        conn, site_id=site_id, since_valid_at="2035-01-01T00:00:00Z", as_of=_T
    )
    assert [row.value for row in asof] == [1.0]


def test_load_future_samples_asof_excludes_post_t_issuance_and_null_fetched() -> None:
    conn = _conn()
    site_id = _make_site(conn, "AsOf Bounds Site")
    feed_null = _make_real_feed(conn, "model-null")
    feed_late = _make_real_feed(conn, "model-late-issue")
    # NULL fetched_at: cannot be placed on the as-of timeline — excluded
    # under as_of, still visible live, and counted with a recorded reason.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_null,
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-03T06:00:00Z",
        lead_hours=30,
        value=3.0,
        fetched_at=None,
    )
    # issued_at after T (fetched_at adversarially before T): the issued_at
    # bound must exclude it on its own.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_late,
        issued_at="2035-01-02T13:00:00Z",
        valid_at="2035-01-03T06:00:00Z",
        lead_hours=17,
        value=4.0,
        fetched_at="2035-01-02T11:00:00Z",
    )
    live = load_future_samples(
        conn, site_id=site_id, since_valid_at="2035-01-01T00:00:00Z"
    )
    assert {row.value for row in live} == {3.0, 4.0}
    asof = load_future_samples(
        conn, site_id=site_id, since_valid_at="2035-01-01T00:00:00Z", as_of=_T
    )
    assert asof == []
    assert (
        count_null_availability_samples(
            conn, site_id=site_id, since_valid_at="2035-01-01T00:00:00Z"
        )
        == 1
    )


# ---------------------------------------------------------------------------
# Knowability predicate + per-reason exclusion accounting
# ---------------------------------------------------------------------------


def _seed_knowability_matrix(conn: sqlite3.Connection) -> tuple[int, int]:
    """One pair per exclusion reason plus one knowable pair."""
    site_id = _make_site(conn, "Knowability Site")
    feed_id = _make_real_feed(conn, "model-k")
    # Knowable: first_known 06:00 <= T, valid 05:00 + 3 h = 08:00 <= T,
    # target obs computed 09:00 <= T.
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T05:00:00Z",
        issued_at="2035-01-01T23:00:00Z",
        forecast=5.0,
        observed=4.0,
        first_known_at="2035-01-02T06:00:00Z",
    )
    _insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-02T05:00:00Z",
        value=4.0,
        computed_at="2035-01-02T09:00:00Z",
    )
    # NULL first_known_at.
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T04:00:00Z",
        issued_at="2035-01-01T22:00:00Z",
        forecast=5.0,
        observed=4.0,
        first_known_at=None,
    )
    # first_known_at after T.
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T03:00:00Z",
        issued_at="2035-01-01T21:00:00Z",
        forecast=5.0,
        observed=4.0,
        first_known_at="2035-01-02T13:00:00Z",
    )
    # Consensus lag: valid 10:00 + 3 h = 13:00 > T even though known at 11:00.
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T10:00:00Z",
        issued_at="2035-01-02T04:00:00Z",
        forecast=5.0,
        observed=4.0,
        first_known_at="2035-01-02T11:00:00Z",
    )
    # computed_at guard: otherwise knowable, but the target observation was
    # (re)materialized after T.
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2035-01-02T06:00:00Z",
        issued_at="2035-01-02T00:00:00Z",
        forecast=5.0,
        observed=4.0,
        first_known_at="2035-01-02T07:00:00Z",
    )
    _insert_observation(
        conn,
        site_id=site_id,
        valid_at="2035-01-02T06:00:00Z",
        value=4.0,
        computed_at="2035-01-02T13:00:00Z",
    )
    return site_id, feed_id


def test_pair_knowability_exclusions_classify_by_first_failing_reason() -> None:
    conn = _conn()
    site_id, _ = _seed_knowability_matrix(conn)
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


def test_aggregate_asof_sees_only_knowable_pairs() -> None:
    conn = _conn()
    site_id, feed_id = _seed_knowability_matrix(conn)
    live = strategy_for("temperature").aggregate(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_cutoff=None,
        min_n=1,
    )
    assert live.n == 5
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
    assert asof.n == 1


def test_paired_skill_applies_knowability_to_both_sides() -> None:
    conn = _conn()
    site_id = _make_site(conn, "Paired Skill Site")
    feed_id = _make_real_feed(conn, "model-skill")
    persistence_id = _persistence_feed_id(conn)
    v1, v2 = "2035-01-02T05:00:00Z", "2035-01-02T06:00:00Z"
    # Candidate pairs: BOTH knowable at T.
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=v1,
        issued_at="2035-01-01T23:00:00Z",
        forecast=5.0,
        observed=4.0,  # sq_error 1
        first_known_at="2035-01-02T06:30:00Z",
    )
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=v2,
        issued_at="2035-01-02T00:00:00Z",
        forecast=14.0,
        observed=4.0,  # sq_error 100
        first_known_at="2035-01-02T07:00:00Z",
    )
    # Persistence references: v1 knowable, v2 known only AFTER T — the v2
    # row must drop from the paired join even though the candidate side is
    # knowable (both-sides rule, plan §6).
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=persistence_id,
        valid_at=v1,
        issued_at="2035-01-01T23:00:00Z",
        forecast=6.0,
        observed=4.0,  # sq_error 4
        first_known_at="2035-01-02T06:45:00Z",
    )
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=persistence_id,
        valid_at=v2,
        issued_at="2035-01-02T00:00:00Z",
        forecast=5.0,
        observed=4.0,  # sq_error 1
        first_known_at="2035-01-02T14:00:00Z",
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
    # Only the v1 row pairs: 1 - 1/4. An fp-only predicate would keep v2
    # (skill 1 - 101/5) and fail loudly here.
    assert asof.skill_score is not None
    assert abs(asof.skill_score - 0.75) < 1e-12
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
    assert abs(live.skill_score - (1.0 - 101.0 / 5.0)) < 1e-12


# ---------------------------------------------------------------------------
# asof_leaderboard: declared-configuration counterfactual
# ---------------------------------------------------------------------------


def _seed_two_feeds(conn: sqlite3.Connection) -> tuple[int, int, int]:
    site_id = _make_site(conn, "AsOf Ranking Site")
    feed_known = _make_real_feed(conn, "model-known")
    feed_future = _make_real_feed(conn, "model-future")
    for valid_at, issued_at in (
        ("2035-01-02T05:00:00Z", "2035-01-01T23:00:00Z"),
        ("2035-01-02T06:00:00Z", "2035-01-02T00:00:00Z"),
    ):
        _insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_known,
            valid_at=valid_at,
            issued_at=issued_at,
            forecast=5.0,
            observed=4.0,
            first_known_at="2035-01-02T07:00:00Z",
        )
        # Same cells for the other feed, but first known only after T: the
        # feed must not APPEAR in the as-of ranking at all (roster leakage).
        _insert_pair(
            conn,
            site_id=site_id,
            feed_id=feed_future,
            valid_at=valid_at,
            issued_at=issued_at,
            forecast=5.0,
            observed=4.0,
            first_known_at="2035-01-03T07:00:00Z",
        )
    return site_id, feed_known, feed_future


def test_asof_leaderboard_excludes_post_t_feeds_and_freezes_min_n() -> None:
    conn = _conn()
    site_id, feed_known, feed_future = _seed_two_feeds(conn)
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
    assert feed_known in by_feed
    assert feed_future not in by_feed
    assert by_feed[feed_known].n == 2
    # min_n is the DECLARED value, not the live setting (absent here, would
    # default to 30 and never confirm confidence at n=2). skill is None with
    # no knowable persistence reference, so confidence stays False either
    # way; pin the n-gate separately through the declared threshold.
    strict = asof_leaderboard(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        as_of=_T,
        min_n=5,
        window_days=None,
    )
    assert {row.feed_id: row.n for row in strict}[feed_known] == 2


def test_asof_leaderboard_window_days_anchored_at_t() -> None:
    conn = _conn()
    site_id, feed_known, _ = _seed_two_feeds(conn)
    # A knowable pair OLDER than the declared window at T must fall out.
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_known,
        valid_at="2034-12-20T05:00:00Z",
        issued_at="2034-12-19T23:00:00Z",
        forecast=5.0,
        observed=4.0,
        first_known_at="2034-12-20T07:00:00Z",
    )
    windowed = asof_leaderboard(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        as_of=_T,
        min_n=1,
        window_days=7,
    )
    assert {row.feed_id: row.n for row in windowed}[feed_known] == 2
    unwindowed = asof_leaderboard(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        as_of=_T,
        min_n=1,
        window_days=None,
    )
    assert {row.feed_id: row.n for row in unwindowed}[feed_known] == 3


def test_forecast_ranking_applies_same_exclusions_on_both_paths() -> None:
    conn = _conn()
    site_id, feed_known, _ = _seed_two_feeds(conn)
    persistence_id = _persistence_feed_id(conn)
    # Give the persistence virtual feed knowable pairs: it appears on the
    # leaderboard (virtual feeds are competitors) but the Forecast-page
    # eligibility exclusions must remove it on BOTH paths.
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=persistence_id,
        valid_at="2035-01-02T05:00:00Z",
        issued_at="2035-01-01T23:00:00Z",
        forecast=6.0,
        observed=4.0,
        first_known_at="2035-01-02T06:00:00Z",
    )
    asof_rows = asof_leaderboard(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        as_of=_T,
        min_n=1,
        window_days=None,
    )
    assert persistence_id in {row.feed_id for row in asof_rows}
    ranking_asof = forecast_ranking(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        as_of=_T,
        declared_min_n=1,
        declared_window_days=None,
    )
    assert persistence_id not in ranking_asof
    assert feed_known in ranking_asof
    ranking_live = forecast_ranking(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        window="30d",
    )
    assert persistence_id not in ranking_live


# ---------------------------------------------------------------------------
# Live-equivalence smoke at T = now (the §18.6 family pins this fully)
# ---------------------------------------------------------------------------


def test_asof_at_now_matches_live_leaderboard_for_fully_knowable_data() -> None:
    conn = _conn()
    site_id = _make_site(conn, "Equivalence Site")
    feed_id = _make_real_feed(conn, "model-eq")
    persistence_id = _persistence_feed_id(conn)
    now = utc_now()
    valid_at = isoformat_utc(now - timedelta(hours=6))
    known_at = isoformat_utc(now - timedelta(hours=2))
    issued_at = isoformat_utc(now - timedelta(hours=12))
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=valid_at,
        issued_at=issued_at,
        forecast=5.0,
        observed=4.0,
        first_known_at=known_at,
    )
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=persistence_id,
        valid_at=valid_at,
        issued_at=issued_at,
        forecast=6.0,
        observed=4.0,
        first_known_at=known_at,
    )
    live = forecast_ranking(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="30d"
    )
    asof = forecast_ranking(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        as_of=isoformat_utc(now),
        declared_min_n=30,
        declared_window_days=30,
    )
    assert set(live) == set(asof) == {feed_id}

    def strip(rows: dict[int, LeaderboardRow]) -> list[tuple[object, ...]]:
        # window_key/window_days label the path ("live:30d" vs "asof:30d")
        # and are excluded deliberately; every scored field must match.
        return [
            (r.feed_id, r.n, r.skill_score, r.bias, r.mae, r.rmse, r.confident)
            for r in rows.values()
        ]

    assert strip(live) == strip(asof)
