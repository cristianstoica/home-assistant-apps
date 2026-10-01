"""Wind training and weights (plan §11.1-11.3, §15.1 T49-T58).

``training_window`` and ``training_pairs_query`` build the statement;
``load_wind_training`` reads it plus the observed side; ``compute_wind_weights``,
``choose_wind_feeds``, ``weighted_daily_high`` and ``weighted_hourly`` are pure
once the training read is in hand. Two filter checks that are not plan
T-rows -- published-generation and excluded-feeds on the training path --
sit alongside T49-T58 with their own paired positive/negative fixtures.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast.selection import CellCandidate
from wxverify.forecast.wind_blend import (
    FLOOR_MS,
    MIN_FEEDS,
    MIN_OBS_HOURS,
    MIN_RUN_HOURS,
    MIN_TRAINING_DAYS,
    TRAINING_DAYS,
    FeedWeight,
    WindTraining,
    choose_wind_feeds,
    compute_wind_weights,
    load_wind_training,
    training_window,
    weighted_daily_high,
    weighted_hourly,
)

_TODAY = date(2026, 4, 15)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, *, timezone: str = "UTC") -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('Testsite', 10.0, 20.0, 100.0, ?)
        """,
        (timezone,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _make_feed(conn: sqlite3.Connection, model: str, *, is_virtual: int = 0) -> int:
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes, max_lead_hours, is_virtual)
        VALUES ('example-src', ?, 1, 360, 192, ?)
        """,
        (model, is_virtual),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _unpublished_generation(conn: sqlite3.Connection, site_id: int) -> int:
    cur = conn.execute(
        """
        INSERT INTO timezone_generations (site_id, timezone, mode, state)
        VALUES (?, 'UTC', 'retrospective_correction', 'building')
        """,
        (site_id,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_run(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    tz_generation_id: int,
    day_ahead: int,
    target_day: date,
    issued_at: str,
    n_hours: int,
    forecast_high: float,
    first_known_at: str | None = None,
) -> None:
    """Insert one run (one ``issued_at``) at lead ``day_ahead``, ``n_hours``
    distinct hourly ``valid_at`` rows inside ``target_day`` (UTC-aligned)."""
    for h in range(n_hours):
        valid_at = f"{target_day.isoformat()}T{h:02d}:00:00Z"
        conn.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, tz_generation_id, first_known_at)
            VALUES (?, ?, 'wind', ?, ?, ?, ?, ?, 0.0, ?, ?)
            """,
            (
                site_id,
                feed_id,
                issued_at,
                valid_at,
                day_ahead * 24 + h + 1,
                day_ahead,
                forecast_high,
                tz_generation_id,
                first_known_at,
            ),
        )


def _insert_obs_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    target_day: date,
    n_hours: int,
    high: float,
    computed_at: str | None = None,
) -> None:
    for h in range(n_hours):
        valid_at = f"{target_day.isoformat()}T{h:02d}:00:00Z"
        value = high if h == 0 else high - 1.0
        conn.execute(
            """
            INSERT INTO observations
                (site_id, variable, valid_at, value, n_stations, computed_at)
            VALUES (?, 'wind', ?, ?, 3, ?)
            """,
            (site_id, valid_at, value, computed_at),
        )


def _make_station(conn: sqlite3.Connection, site_id: int, pws_id: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO stations (site_id, pws_station_id, lat, lon,
                               dem_elevation_m, enabled)
        VALUES (?, ?, 10.0, 20.0, 50.0, 1)
        """,
        (site_id, pws_id),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_wind_day(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    local_date: date,
    status: str,
    attempts: int = 0,
    refetch_at: str | None = None,
    refetched: int = 0,
) -> None:
    conn.execute(
        """
        INSERT INTO station_wind_days
            (station_id, local_date, status, attempts, refetch_at,
             refetched, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, '2026-01-01T00:00:00Z')
        """,
        (station_id, local_date.isoformat(), status, attempts, refetch_at, refetched),
    )


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


def _insert_feed_training_days(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    tz_generation_id: int,
    day_ahead: int,
    days: list[date],
) -> None:
    """Insert one qualifying (>= MIN_RUN_HOURS) run per day in ``days``."""
    for i, target in enumerate(days):
        issued_at = f"2026-01-01T{i % 24:02d}:00:01Z"
        _insert_run(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            tz_generation_id=tz_generation_id,
            day_ahead=day_ahead,
            target_day=target,
            issued_at=issued_at,
            n_hours=MIN_RUN_HOURS,
            forecast_high=10.0,
        )


def _insert_obs_training_days(
    conn: sqlite3.Connection, *, site_id: int, days: list[date]
) -> None:
    """Insert one qualifying (>= MIN_OBS_HOURS) observed day per ``days``."""
    for target in days:
        _insert_obs_day(
            conn, site_id=site_id, target_day=target, n_hours=MIN_OBS_HOURS, high=10.0
        )


def _days_from(offset: int, n_days: int) -> list[date]:
    return [_day(offset + i) for i in range(n_days)]


# --- T49: training-inventory exclusion -------------------------------------


def test_t49_unsettled_station_day_excluded_settled_included() -> None:
    """A pending enabled-station day is excluded; the same shape, final, is included.

    mutant_drop_inventory_read -> at the final assertion: correct =
    {good_day} (bad_day's observed high absent), mutant (removing the
    ``and local_date not in unsettled`` filter) = {bad_day, good_day} (both
    present). The fixture's bad/good days carry distinct observed highs
    (10.0 vs 12.0) so presence, not value, is what the mutant flips.
    """
    conn = _conn()
    site_id = _make_site(conn)
    station_id = _make_station(conn, site_id, "KTEST001")
    bad_day = _day(10)
    good_day = _day(11)
    _insert_wind_day(conn, station_id=station_id, local_date=bad_day, status="pending")
    _insert_wind_day(
        conn,
        station_id=station_id,
        local_date=good_day,
        status="fetched",
        attempts=3,
        refetched=1,
    )
    _insert_obs_day(
        conn, site_id=site_id, target_day=bad_day, n_hours=MIN_OBS_HOURS, high=10.0
    )
    _insert_obs_day(
        conn, site_id=site_id, target_day=good_day, n_hours=MIN_OBS_HOURS, high=12.0
    )

    training = load_wind_training(
        conn, site_id=site_id, timezone="UTC", today=_TODAY, as_of=None
    )
    assert bad_day not in training.observed_highs
    assert good_day in training.observed_highs
    assert training.observed_highs[good_day] == 12.0


def test_t49_today_is_never_included() -> None:
    """A paired positive to the window check: today's own partial day never trains."""
    conn = _conn()
    site_id = _make_site(conn)
    _insert_obs_day(
        conn, site_id=site_id, target_day=_TODAY, n_hours=MIN_OBS_HOURS, high=99.0
    )
    training = load_wind_training(
        conn, site_id=site_id, timezone="UTC", today=_TODAY, as_of=None
    )
    assert _TODAY not in training.observed_highs


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
    second case sits exactly at the plan's stated threshold.
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


# --- T52: latest qualifying run wins -------------------------------------


def test_t52_latest_run_with_enough_hours_wins_over_earlier_and_short_later() -> None:
    """Three runs target one day: an earlier 20h run (H1), a later 20h run
    (H2) that must win, and the LATEST-issued run with only 19h (H3) that
    must be ignored despite being newest.

    mutant_keep_earliest -> at the assertion: correct = 20.0 (H2, the
    latest run with >= 20h), mutant (``issued < held[0]`` in place of
    ``issued > held[0]``) = 10.0 (H1, the earliest qualifying run). The
    fixture's H1/H2/H3 are pairwise distinct so only one literal can match.
    """
    conn = _conn()
    site_id = _make_site(conn)
    feed_id = _make_feed(conn, "m1")
    gen_id = ensure_published_generation(conn, site_id)
    target = _day(10)
    _insert_run(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=gen_id,
        day_ahead=0,
        target_day=target,
        issued_at="2026-01-01T00:00:00Z",
        n_hours=MIN_RUN_HOURS,
        forecast_high=10.0,
    )
    _insert_run(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=gen_id,
        day_ahead=0,
        target_day=target,
        issued_at="2026-01-01T06:00:00Z",
        n_hours=MIN_RUN_HOURS,
        forecast_high=20.0,
    )
    _insert_run(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=gen_id,
        day_ahead=0,
        target_day=target,
        issued_at="2026-01-01T12:00:00Z",
        n_hours=MIN_RUN_HOURS - 1,
        forecast_high=30.0,
    )
    training = load_wind_training(
        conn, site_id=site_id, timezone="UTC", today=_TODAY, as_of=None
    )
    assert training.feed_highs[(feed_id, 0)][target] == 20.0


# --- T53: observed-hours threshold ---------------------------------------


def test_t53_21_obs_hours_excluded_22_included() -> None:
    """mutant_off_by_one -> at the assertion: correct = True (22h day
    present), mutant (``>`` in place of ``>=`` on MIN_OBS_HOURS) = False
    (22h day treated as still short). Paired with the 21h day, which both
    the correct and the mutant code exclude.
    """
    conn = _conn()
    site_id = _make_site(conn)
    short_day = _day(10)
    full_day = _day(11)
    _insert_obs_day(
        conn, site_id=site_id, target_day=short_day, n_hours=MIN_OBS_HOURS - 1, high=5.0
    )
    _insert_obs_day(
        conn, site_id=site_id, target_day=full_day, n_hours=MIN_OBS_HOURS, high=7.0
    )
    training = load_wind_training(
        conn, site_id=site_id, timezone="UTC", today=_TODAY, as_of=None
    )
    assert short_day not in training.observed_highs
    assert full_day in training.observed_highs


# --- T54: as-of is instant-based (julianday), not string-based ----------


def test_t54_asof_observed_side_compares_instants_not_strings() -> None:
    """A non-canonical ``computed_at`` at the SAME instant as ``as_of`` is
    included; one strictly after is excluded.

    mutant_string_compare -> at the first assertion: correct = True
    (the equal-instant row is included, since ``T23:59:00+01:00`` ==
    ``T22:59:00Z``), mutant (plain ``computed_at <= ?`` string comparison
    in place of the julianday comparison) = False, because the string
    ``'...T23:59:00+01:00'`` sorts AFTER the string ``'...T22:59:00Z'``
    lexically even though the instants are equal. The fixture's offset
    stamp is chosen specifically to sort the wrong way as a string.
    """
    conn = _conn()
    site_id = _make_site(conn)
    target = _day(10)
    as_of = f"{target.isoformat()}T22:59:00Z"
    # Equal instant, non-canonical (offset) form -- sorts AFTER as_of as a
    # bare string, but is NOT after it as an instant.
    equal_instant = f"{target.isoformat()}T23:59:00+01:00"
    _insert_obs_day(
        conn,
        site_id=site_id,
        target_day=target,
        n_hours=MIN_OBS_HOURS,
        high=9.0,
        computed_at=equal_instant,
    )
    training = load_wind_training(
        conn, site_id=site_id, timezone="UTC", today=_TODAY, as_of=as_of
    )
    assert target in training.observed_highs

    conn2 = _conn()
    site_id2 = _make_site(conn2)
    later = f"{target.isoformat()}T23:00:01Z"  # genuinely 1s after as_of
    _insert_obs_day(
        conn2,
        site_id=site_id2,
        target_day=target,
        n_hours=MIN_OBS_HOURS,
        high=9.0,
        computed_at=later,
    )
    training2 = load_wind_training(
        conn2, site_id=site_id2, timezone="UTC", today=_TODAY, as_of=as_of
    )
    assert target not in training2.observed_highs


def test_t54_pair_side_knowable_predicate_compares_instants_not_strings() -> None:
    """Pair-side mirror of the observed-side T54 test above: a run's
    ``first_known_at`` at the SAME instant as ``as_of`` (written in a
    non-canonical offset form) is included; one instant strictly after is
    excluded. ``valid_at`` is held well inside the 3-hour consensus lag of
    ``as_of`` in both arms, so only ``first_known_at`` varies.

    mutant_drop_knowable_clause -> at the second assertion
    (``target not in training2.feed_highs.get((feed_id2, 1), {})``):
    correct = True (the strictly-after run is excluded, since its
    ``first_known_at`` is not yet knowable at ``as_of``), mutant (removing
    the ``knowable_pair_predicate`` clause from ``training_pairs_query``
    entirely) = False, because with no knowability filter the published,
    20-hour-covered run is included regardless of ``first_known_at``. The
    first assertion passes under both correct and mutant code (the
    equal-instant run is included either way), so it is the second arm
    that discriminates.
    """
    conn = _conn()
    site_id = _make_site(conn)
    feed_id = _make_feed(conn, "m1")
    gen_id = ensure_published_generation(conn, site_id)
    target = _day(10)
    as_of = f"{target.isoformat()}T22:59:00Z"
    # Equal instant, non-canonical (offset) form.
    equal_instant = f"{target.isoformat()}T23:59:00+01:00"
    _insert_run(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=gen_id,
        day_ahead=1,
        target_day=target,
        issued_at=f"{(target - timedelta(days=1)).isoformat()}T12:00:00Z",
        n_hours=MIN_RUN_HOURS,  # hours 00:00..19:00; +3h lag tops out at 22:00
        forecast_high=9.0,
        first_known_at=equal_instant,
    )
    training = load_wind_training(
        conn, site_id=site_id, timezone="UTC", today=_TODAY, as_of=as_of
    )
    assert target in training.feed_highs.get((feed_id, 1), {})

    conn2 = _conn()
    site_id2 = _make_site(conn2)
    feed_id2 = _make_feed(conn2, "m1")
    gen_id2 = ensure_published_generation(conn2, site_id2)
    later = f"{target.isoformat()}T23:00:01Z"  # genuinely 1s after as_of
    _insert_run(
        conn2,
        site_id=site_id2,
        feed_id=feed_id2,
        tz_generation_id=gen_id2,
        day_ahead=1,
        target_day=target,
        issued_at=f"{(target - timedelta(days=1)).isoformat()}T12:00:00Z",
        n_hours=MIN_RUN_HOURS,
        forecast_high=9.0,
        first_known_at=later,
    )
    training2 = load_wind_training(
        conn2, site_id=site_id2, timezone="UTC", today=_TODAY, as_of=as_of
    )
    assert target not in training2.feed_highs.get((feed_id2, 1), {})


# --- T55: the training window --------------------------------------------


def test_t55_window_boundary_is_local_midnight_with_dst() -> None:
    """``training_window`` is exact across a DST transition.

    mutant_wrong_slot -> at the assertion: correct = '2026-03-29T21:00:00Z'
    (local midnight of 2026-03-30 in Europe/Athens, already at the +3
    post-transition offset), mutant (``[1]`` in place of ``[0]`` on
    ``local_day_slots``) = the END of that local day instead of its start,
    a different literal entirely. The DST-shifted day (spring-forward on
    2026-03-29) makes the start/end literals asymmetric, unlike a plain day.
    """
    lo, hi = training_window(date(2026, 3, 30), "Europe/Athens")
    assert lo == "2026-02-27T22:00:00Z"
    assert hi == "2026-03-29T21:00:00Z"


def test_t55_day_before_window_excluded_today_excluded() -> None:
    """mutant_off_by_one_window -> paired with the pure boundary test above:
    a day one past the window (today-31) and today itself both train
    nothing, which the DB-level read must also honour.
    """
    conn = _conn()
    site_id = _make_site(conn)
    too_old = _day(TRAINING_DAYS + 1)
    _insert_obs_day(
        conn, site_id=site_id, target_day=too_old, n_hours=MIN_OBS_HOURS, high=1.0
    )
    _insert_obs_day(
        conn, site_id=site_id, target_day=_TODAY, n_hours=MIN_OBS_HOURS, high=2.0
    )
    inside = _day(TRAINING_DAYS - 1)
    _insert_obs_day(
        conn, site_id=site_id, target_day=inside, n_hours=MIN_OBS_HOURS, high=3.0
    )
    training = load_wind_training(
        conn, site_id=site_id, timezone="UTC", today=_TODAY, as_of=None
    )
    assert too_old not in training.observed_highs
    assert _TODAY not in training.observed_highs
    assert inside in training.observed_highs


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


# --- Training filters: not plan T-rows, pinned alongside T49-T58 --------


def test_published_generation_filters_training_pairs() -> None:
    """Pairs under a non-published generation never train; the same pairs
    under the published generation do.

    mutant_drop_generation_clause -> at the assertion: correct =
    {20 days} (only the published-generation feed's matched days), mutant
    (removing ``AND {published_generation_clause("fp")}`` from
    ``training_pairs_query``) = {25 days} (the 5 unpublished-generation
    days leak in too). The fixture's unpublished days are matched with
    observations as well, so the mutant's extra 5 days show up in the
    weight's own ``days`` count, not just in an unused side table.
    """
    conn = _conn()
    site_id = _make_site(conn)
    feed_id = _make_feed(conn, "m1")
    published_id = ensure_published_generation(conn, site_id)
    other_id = _unpublished_generation(conn, site_id)

    published_days = _days_from(1, MIN_TRAINING_DAYS)
    other_days = _days_from(1 + MIN_TRAINING_DAYS, 5)
    _insert_obs_training_days(conn, site_id=site_id, days=published_days + other_days)
    _insert_feed_training_days(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=published_id,
        day_ahead=0,
        days=published_days,
    )
    _insert_feed_training_days(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        tz_generation_id=other_id,
        day_ahead=0,
        days=other_days,
    )

    training = load_wind_training(
        conn, site_id=site_id, timezone="UTC", today=_TODAY, as_of=None
    )
    assert set(training.feed_highs[(feed_id, 0)]) == set(published_days)
    weights = compute_wind_weights(training)
    assert weights[(feed_id, 0)].days == MIN_TRAINING_DAYS


def test_excluded_feeds_sql_drops_virtual_feeds_from_training() -> None:
    """A virtual feed's full training history is absent from the weights;
    an otherwise-identical real feed's is present.

    mutant_drop_excluded_feeds_clause -> at the assertion: correct =
    {real_feed_id} (only the real feed has a weight), mutant (removing
    ``AND {EXCLUDED_FEEDS_SQL}`` from ``training_pairs_query``) =
    {real_feed_id, virtual_feed_id} (the virtual feed gets a weight too,
    from data that is otherwise byte-identical to the real feed's).
    """
    conn = _conn()
    site_id = _make_site(conn)
    real_feed_id = _make_feed(conn, "real-model", is_virtual=0)
    virtual_feed_id = _make_feed(conn, "virtual-model", is_virtual=1)
    gen_id = ensure_published_generation(conn, site_id)

    shared_days = _days_from(1, MIN_TRAINING_DAYS)
    _insert_obs_training_days(conn, site_id=site_id, days=shared_days)
    _insert_feed_training_days(
        conn,
        site_id=site_id,
        feed_id=real_feed_id,
        tz_generation_id=gen_id,
        day_ahead=0,
        days=shared_days,
    )
    _insert_feed_training_days(
        conn,
        site_id=site_id,
        feed_id=virtual_feed_id,
        tz_generation_id=gen_id,
        day_ahead=0,
        days=shared_days,
    )

    training = load_wind_training(
        conn, site_id=site_id, timezone="UTC", today=_TODAY, as_of=None
    )
    weights = compute_wind_weights(training)
    present_feeds = {feed_id for feed_id, _k in weights}
    assert present_feeds == {real_feed_id}
