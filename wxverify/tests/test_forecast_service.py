"""Integration tests for ``wxverify.forecast.service`` against a real SQLite DB.

Spec build-sequence step 1 verify hook (B2 audit gate): "...the skill-cell-
vs-display-tile mapping at a local-day boundary (a run issued the prior local
day landing in today's display tile must still rank by its ISSUE-relative
day_ahead, not the display day index)." This file's centerpiece,
``test_b2_...``, is that gate.

Also covers build-sequence step 5 (tile state precedence: empty(global) >
not-available > low-confidence > normal, plus orthogonal stale/partial
badges) end-to-end through the service layer, since the individual pieces
(``select_cell_feeds``, ``clears_coverage``) are already unit-pinned
elsewhere and the risk here is in how the service layer WIRES them together.

Isolation: fresh ``sqlite3.connect(":memory:")`` + ``run_migrations`` per
test (mirrors test_forecast_data.py / test_scoring_equivalence.py).
Far-future (2035) dates are used for forecast_pairs fixtures for the same
reason as test_forecast_data.py: ``forecast_ranking``'s default rolling
window cutoff is computed from the REAL wall clock, not an injectable "now".
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from wxverify.core.units import ms_to_kmh
from wxverify.db.migrations import run_migrations
from wxverify.forecast.service import (
    RAIN_GLYPH_MIN_CHANCE_PCT,
    build_forecast,
    build_hourly,
    relative_ago,
)
from wxverify.settings.keys import set_setting
from wxverify.web.context import feed_label

_FAR_FUTURE_VALID_ATS = (
    "2035-07-01T00:00:00Z",
    "2035-07-01T01:00:00Z",
    "2035-07-01T02:00:00Z",
)
_FAR_FUTURE_LEAD_HOURS = (1, 2, 3)


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    conn.execute(
        """
        INSERT INTO sites (id, name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (1, 'Test Site', 47.0, 25.0, 900.0, 'UTC')
        """
    )
    set_setting(conn, "min_n", "3")
    return conn


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
    ).fetchone()
    assert row is not None, f"seed feed not found: {source}/{model}"
    return int(row["id"])


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int = 6,
    value: float,
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-1', ?)
        """,
        (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value, issued_at),
    )


def _insert_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    day_ahead: int,
    forecast: float,
    observed: float = 10.0,
) -> None:
    error = forecast - observed
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            lead_hours,
            day_ahead,
            forecast,
            observed,
            error,
            abs(error),
            error * error,
        ),
    )


def _make_confident(
    conn: sqlite3.Connection, *, feed_id: int, variable: str, day_ahead: int
) -> None:
    """Give `feed_id` a real, positive skill score at (variable, day_ahead)
    against the seeded `virtual/_persistence` baseline, on far-future dates
    so the rolling-window cutoff (real wall clock) never excludes them."""
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    for target_feed, forecast in ((persistence_id, 8.0), (feed_id, 10.5)):
        for valid_at, lead_hours in zip(
            _FAR_FUTURE_VALID_ATS, _FAR_FUTURE_LEAD_HOURS, strict=True
        ):
            _insert_pair(
                conn,
                feed_id=target_feed,
                variable=variable,
                issued_at="2035-06-30T00:00:00Z",
                valid_at=valid_at,
                lead_hours=lead_hours,
                day_ahead=day_ahead,
                forecast=forecast,
            )


def _seed_hourly(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_ats: list[str],
    value: float = 10.0,
) -> None:
    for i, valid_at in enumerate(valid_ats):
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable=variable,
            issued_at=issued_at,
            valid_at=valid_at,
            lead_hours=i + 1,
            value=value,
        )


def _hours(day: str, start: int, count: int) -> list[str]:
    return [f"{day}T{h:02d}:00:00Z" for h in range(start, start + count)]


def _seed_varying(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_ats: list[str],
    values: list[float],
) -> None:
    """Seed N hourly samples with DISTINCT per-hour values (an intra-day spread).

    Unlike ``_seed_hourly`` (one constant value), this produces a real daily
    high/low so a multi-point feed's aggregate is not a degenerate max == min.
    """
    for i, (valid_at, value) in enumerate(zip(valid_ats, values, strict=True)):
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable=variable,
            issued_at=issued_at,
            valid_at=valid_at,
            lead_hours=i + 1,
            value=value,
        )


def _seed_scoring_pairs(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    day_ahead: int,
    forecast: float,
) -> None:
    """Seed 3 far-future scoring pairs for ``feed_id`` at ``day_ahead``.

    Observed is 10.0; ``forecast`` sets the error magnitude. For temperature/
    wind (ContinuousStrategy) a nearer forecast => lower MSE => higher skill vs
    the persistence baseline, so seeding persistence + two competitors with
    different forecasts orders them on skill. Far-future (2035) valid_ats keep
    the pairs inside ``forecast_ranking``'s real-wall-clock rolling cutoff
    (same reason as ``_make_confident``). n = 3 = seeded ``min_n`` => confident
    whenever a skill score exists (persistence present + Continuous strategy).
    """
    for valid_at, lead in zip(
        _FAR_FUTURE_VALID_ATS, _FAR_FUTURE_LEAD_HOURS, strict=True
    ):
        _insert_pair(
            conn,
            feed_id=feed_id,
            variable=variable,
            issued_at="2035-06-30T00:00:00Z",
            valid_at=valid_at,
            lead_hours=lead,
            day_ahead=day_ahead,
            forecast=forecast,
        )


def _seed_far_horizon_collapse(conn: sqlite3.Connection) -> None:
    """D+7 temperature tile that COLLAPSES under pre-fix selection.

    A single-slot ``gfs_global`` feed (one 25.0 sample) that OUT-skills a
    ``ecmwf_ifs`` feed carrying 15 hourly samples spanning 9.1..17.0. Both are
    confident at day_ahead=7 against the persistence baseline. Under blend
    depth 1 the pre-fix ladder picks the single-slot feed alone (higher skill)
    -> high == low == 25.0 (the collapse); the coverage pool instead prefers
    the 15h feed. Assumes ``now`` local date 2026-07-20 (so 2026-07-27 == D+7).
    """
    degenerate_id = _feed_id(conn, "open-meteo", "gfs_global")
    covered_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    day = "2026-07-27"
    issued = "2026-07-20T00:00:00Z"  # 2h before now -> day_ahead=7, not stale
    values = [
        9.1,
        10.0,
        11.0,
        12.0,
        13.0,
        14.0,
        15.0,
        16.0,
        17.0,
        16.5,
        15.5,
        14.5,
        13.5,
        12.5,
        11.5,
    ]  # 15 values, max 17.0, min 9.1
    _seed_varying(
        conn,
        feed_id=covered_id,
        variable="temperature",
        issued_at=issued,
        valid_ats=_hours(day, 0, 15),
        values=values,
    )
    _insert_sample(
        conn,
        feed_id=degenerate_id,
        variable="temperature",
        issued_at=issued,
        valid_at=f"{day}T12:00:00Z",
        lead_hours=12,
        value=25.0,  # outside the covered range: a pre-fix collapse is obvious
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=7, forecast=8.0
    )
    # degenerate OUT-skills covered (forecast nearer observed 10.0).
    _seed_scoring_pairs(
        conn, feed_id=degenerate_id, variable="temperature", day_ahead=7, forecast=10.2
    )
    _seed_scoring_pairs(
        conn, feed_id=covered_id, variable="temperature", day_ahead=7, forecast=11.0
    )


# ---------------------------------------------------------------------------
# B2 audit gate: a run issued the PRIOR local day, landing in TODAY's display
# tile, must rank by its issue-relative day_ahead (1), not the display day
# index (0).
# ---------------------------------------------------------------------------


def test_b2_prior_day_run_in_todays_tile_ranks_by_issue_relative_day_ahead() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)  # local date 2026-07-20 (UTC tz)

    # Issued the PRIOR local day (2026-07-19); valid_ats land TODAY
    # (2026-07-20) -> issue-relative day_ahead=1, but display_day_index=0.
    valid_ats = _hours("2026-07-20", 4, 20)  # 20 distinct hours -> clears coverage
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",  # 6h before `now` -> NOT stale (<12h)
        valid_ats=valid_ats,
    )
    # Confident ONLY at day_ahead=1 (the correct, issue-relative cell). If the
    # service wrongly ranked by the display day index (0) instead, this feed
    # would find no ranking row there and read as not-confident.
    _make_confident(conn, feed_id=feed_id, variable="temperature", day_ahead=1)

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )

    today = view.tiles[0]
    assert today.label == "Today"
    # confident -> normal, not low_confidence
    assert today.temp.meta.state == "normal"
    assert today.temp.meta.available is True
    # 20 covered hours clears MIN_COVERAGE_HOURS
    assert today.temp.meta.partial is False
    assert today.temp.meta.stale is False
    assert today.state == "normal"


def test_b2_paired_negative_wrong_cell_reads_low_confidence() -> None:
    """Sanity companion to the B2 gate above: if pairs are seeded at the
    DISPLAY day_ahead (0) instead of the issue-relative one (1), the SAME
    sample fixture reads low_confidence -- proving the positive test above is
    actually discriminating on which day_ahead was used, not vacuously green."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)

    valid_ats = _hours("2026-07-20", 4, 20)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
    )
    # Wrong cell on purpose: pairs at day_ahead=0 (display), not 1 (issue).
    _make_confident(conn, feed_id=feed_id, variable="temperature", day_ahead=0)

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].temp.meta.state == "low_confidence"


# ---------------------------------------------------------------------------
# Stale badge is orthogonal to state -- same confident/normal construction as
# the B2 positive, only the issued_at lag differs.
# ---------------------------------------------------------------------------


def test_stale_badge_orthogonal_to_normal_state() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)

    valid_ats = _hours("2026-07-20", 4, 20)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="temperature",
        # 18h before `now` -> stale (>=12h threshold for a 360min-interval feed)
        issued_at="2026-07-19T08:00:00Z",
        valid_ats=valid_ats,
    )
    _make_confident(conn, feed_id=feed_id, variable="temperature", day_ahead=1)

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "normal"  # still confident
    assert today.temp.meta.stale is True  # but flagged stale
    assert today.stale is True


# ---------------------------------------------------------------------------
# Minimum-coverage guard: a cell whose selected feeds don't reach
# MIN_COVERAGE_HOURS still aggregates (tile stays populated) but carries the
# "partial" badge.
# ---------------------------------------------------------------------------


def test_partial_badge_when_under_coverage_tile_stays_populated() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)

    # Only 5 covered hours -- under MIN_COVERAGE_HOURS (18).
    valid_ats = _hours("2026-07-20", 4, 5)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
        value=12.0,
    )
    _make_confident(conn, feed_id=feed_id, variable="temperature", day_ahead=1)

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.partial is True
    assert today.temp.meta.available is True  # tile stays populated
    # The partial data is still aggregated, not dropped.
    assert today.temp.high_c == 12.0
    assert today.temp.low_c == 12.0
    assert today.partial is True


# ---------------------------------------------------------------------------
# Tile-level precedence rollup: not_available cells are excluded from the
# rollup; among the rest, low_confidence beats normal.
# ---------------------------------------------------------------------------


def test_tile_precedence_low_confidence_beats_normal_not_available_excluded() -> None:
    conn = _make_db()
    ecmwf_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    gfs_id = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    valid_ats = _hours("2026-07-20", 4, 20)

    # temperature: confident -> normal.
    _seed_hourly(
        conn,
        feed_id=ecmwf_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
    )
    _make_confident(conn, feed_id=ecmwf_id, variable="temperature", day_ahead=1)

    # wind: samples present, but zero forecast_pairs anywhere -> low_confidence
    # (fresh-install sample-count rung).
    _seed_hourly(
        conn,
        feed_id=gfs_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
        value=5.0,
    )

    # precip: no samples at all -> not_available.

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "normal"
    assert today.wind.meta.state == "low_confidence"
    assert today.precip.meta.state == "not_available"
    assert today.precip.meta.available is False
    # Precedence: not_available (precip) excluded from the rollup; among the
    # populated cells, low_confidence (wind) beats normal (temp).
    assert today.state == "low_confidence"

    # Paired negative: a day with NO data anywhere is fully not_available at
    # the tile level (proves the tile-level rollup genuinely reflects
    # absence, not just "always non-not_available once anything exists").
    far_day = view.tiles[7]
    assert far_day.state == "not_available"
    assert far_day.temp.meta.available is False


# ---------------------------------------------------------------------------
# Global empty state.
# ---------------------------------------------------------------------------


def test_build_forecast_empty_when_no_samples_at_all() -> None:
    conn = _make_db()
    view = build_forecast(
        conn,
        site_id=1,
        timezone="UTC",
        rain_threshold_mm=0.2,
        now=datetime(2026, 7, 20, 2, 0, tzinfo=UTC),
    )
    assert view.empty is True
    assert view.tiles == []
    assert view.updated_at is None
    assert view.updated_ago is None


# ---------------------------------------------------------------------------
# Wind display conversion: raw samples are m/s; the tile's max_kmh is
# ms_to_kmh-converted, not a re-implemented/duplicated conversion.
# ---------------------------------------------------------------------------


def test_wind_tile_max_is_ms_to_kmh_converted() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 3),
        value=5.0,  # m/s
    )
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].wind.max_kmh == 18.0  # 5 m/s * 3.6


# ---------------------------------------------------------------------------
# Rain glyph threshold: sourced from RAIN_GLYPH_MIN_CHANCE_PCT, not a
# hardcoded literal.
# ---------------------------------------------------------------------------


def test_rain_glyph_shown_at_threshold_hidden_just_below() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    assert RAIN_GLYPH_MIN_CHANCE_PCT == 25

    # 1 of 4 covered hours wet -> wet_share exactly 0.25 -> chance_pct 25.
    valid_ats = _hours("2026-07-20", 4, 4)
    conn2 = conn
    for i, valid_at in enumerate(valid_ats):
        _insert_sample(
            conn2,
            feed_id=feed_id,
            variable="precip",
            issued_at="2026-07-19T20:00:00Z",
            valid_at=valid_at,
            lead_hours=i + 1,
            value=0.5 if i == 0 else 0.0,
        )
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    precip = view.tiles[0].precip
    assert precip.chance_pct == 25
    assert precip.show_rain_glyph is True


# ---------------------------------------------------------------------------
# relative_ago boundaries.
# ---------------------------------------------------------------------------


def test_relative_ago_just_now_boundary() -> None:
    now = datetime(2026, 7, 20, 12, 0, 59, tzinfo=UTC)
    assert relative_ago("2026-07-20T12:00:00Z", now=now) == "just now"


def test_relative_ago_one_minute_boundary_switches_to_minutes() -> None:
    now = datetime(2026, 7, 20, 12, 1, 0, tzinfo=UTC)
    assert relative_ago("2026-07-20T12:00:00Z", now=now) == "1 min ago"


def test_relative_ago_hours() -> None:
    now = datetime(2026, 7, 20, 15, 0, 0, tzinfo=UTC)
    assert relative_ago("2026-07-20T12:00:00Z", now=now) == "3 h ago"


def test_relative_ago_days() -> None:
    now = datetime(2026, 7, 22, 12, 0, 0, tzinfo=UTC)
    assert relative_ago("2026-07-20T12:00:00Z", now=now) == "2 d ago"


# ---------------------------------------------------------------------------
# build_hourly: hour axis + per-feed series reflect the SAME selection the
# tile used (not every feed with samples).
# ---------------------------------------------------------------------------


def test_build_hourly_hour_axis_reflects_selected_feed_only() -> None:
    conn = _make_db()
    winner_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    loser_id = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    set_setting(conn, "forecast_blend_depth", "1")  # force exactly 1 feed selected

    # Winner: confident, non-overlapping hour set from loser.
    _seed_hourly(
        conn,
        feed_id=winner_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 2),
    )
    _make_confident(conn, feed_id=winner_id, variable="temperature", day_ahead=1)
    # Loser: not confident (no pairs), disjoint hours.
    _seed_hourly(
        conn,
        feed_id=loser_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 10, 2),
    )

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["hours"] == _hours("2026-07-20", 4, 2)  # winner's hours only
    assert payload["states"]["temperature"] == "normal"
    feed_ids = {feed["feed_id"] for feed in payload["feeds"]}
    assert winner_id in feed_ids
    assert loser_id not in feed_ids


def test_build_hourly_wind_series_already_kmh_converted() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 1),
        value=5.0,
    )
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["blend"]["wind_kmh"] == [18.0]
    assert payload["feeds"][0]["wind_kmh"] == [18.0]


# ---------------------------------------------------------------------------
# Coverage-aware feed selection (far-horizon collapse fix). The pre-fix bug:
# in _build_tile, high_c = blend_mean([max(v) ...]) and low_c = blend_mean(
# [min(v) ...]); a lone selected single-slot feed has max == min, so the tile
# renders high == low == a single value. These pin that a multi-point feed is
# preferred over a higher-skill single-slot one.
# ---------------------------------------------------------------------------


def test_far_horizon_multipoint_feed_rescues_collapsed_tile() -> None:
    """Headline regression (FAILS pre-fix, PASSES post-fix).

    Pre-fix reasoning: with blend_depth=1 the selection pool == candidates, so
    the confidence ladder ranks the two confident feeds by skill and takes the
    top 1 -> the single-slot gfs_global feed (higher skill) is chosen ALONE ->
    high_c == low_c == 25.0. The high_c == 17.0 / low_c == 9.1 assertions and
    the "gfs absent" assertion all fail on that collapse.
    Post-fix: the >=12h adequate pool restricts to the 15h ecmwf_ifs feed
    (covered_hours 1 < 12 gates gfs out), so the real 9.1..17.0 spread renders.
    """
    conn = _make_db()
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)  # local date 2026-07-20
    set_setting(conn, "forecast_blend_depth", "1")
    _seed_far_horizon_collapse(conn)

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[7]  # D+7 far-horizon tile
    assert tile.temp.high_c == 17.0
    assert tile.temp.low_c == 9.1
    assert tile.temp.high_c != tile.temp.low_c  # a real spread, not a collapse
    assert tile.temp.meta.partial is True  # 15 covered hours < 18-hour badge gate
    labels = [ref.label for ref in tile.temp.meta.feeds]
    assert feed_label("open-meteo", "gfs_global") not in labels  # single-slot gated out
    assert feed_label("open-meteo", "ecmwf_ifs") in labels


def test_near_tile_skill_still_decides_end_to_end() -> None:
    """Paired near-tile positive: when both feeds are well-covered (>=12h) the
    coverage pool is a no-op and skill decides, so the fix does not regress the
    ordinary case. Higher-skill ecmwf_ifs wins over gfs_global."""
    conn = _make_db()
    high_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    low_id = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    set_setting(conn, "forecast_blend_depth", "1")

    day = "2026-07-21"  # D+1 near tile
    issued = "2026-07-20T00:00:00Z"  # day_ahead=1
    hrs = _hours(day, 0, 24)  # 24 covered hours -> both adequate, pool no-op
    _seed_varying(
        conn,
        feed_id=high_id,
        variable="temperature",
        issued_at=issued,
        valid_ats=hrs,
        values=[10.0 + i * 0.1 for i in range(24)],
    )
    _seed_varying(
        conn,
        feed_id=low_id,
        variable="temperature",
        issued_at=issued,
        valid_ats=hrs,
        values=[20.0 + i * 0.1 for i in range(24)],
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=1, forecast=8.0
    )
    _seed_scoring_pairs(
        conn, feed_id=high_id, variable="temperature", day_ahead=1, forecast=10.2
    )  # higher skill
    _seed_scoring_pairs(
        conn, feed_id=low_id, variable="temperature", day_ahead=1, forecast=11.0
    )  # lower skill

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[1]
    labels = [ref.label for ref in tile.temp.meta.feeds]
    assert feed_label("open-meteo", "ecmwf_ifs") in labels
    assert feed_label("open-meteo", "gfs_global") not in labels
    assert tile.temp.meta.partial is False  # 24h clears the coverage badge


def test_build_hourly_far_tile_is_not_a_single_point() -> None:
    """build_hourly for the collapsed far tile: the single-hourly-point symptom
    is gone. Pre-fix (blend_depth=1) the single-slot gfs feed is selected ->
    one hour, one non-None blend point. Post-fix the 15h ecmwf feed is selected
    -> a real multi-hour axis and blend series."""
    conn = _make_db()
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    set_setting(conn, "forecast_blend_depth", "1")
    _seed_far_horizon_collapse(conn)

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=7, now=now)
    hours = payload["hours"]
    assert isinstance(hours, list)
    assert len(hours) > 1
    blend = payload["blend"]
    assert isinstance(blend, dict)
    temp_c = blend["temp_c"]
    assert isinstance(temp_c, list)
    non_none = [v for v in temp_c if v is not None]
    assert len(non_none) > 1


def test_coverage_gate_is_variable_agnostic_precip_and_wind() -> None:
    """The gate is not temperature-only: for BOTH precip total and wind max a
    well-covered feed is preferred over a single-slot feed that out-ranks it.

    Discriminating construction: the single-slot gfs feed carries scoring pairs
    (pair_n = 3, no persistence => not confident but on the SCORED rung), the
    15h ecmwf feed has none (pair_n = 0). Pre-fix the pool == candidates, so
    the scored rung selects the single-slot feed alone -> wind.max_kmh 108.0
    (30 m/s) and precip.total 0.0. Post-fix the >=12h adequate pool excludes
    the single-slot feed, so the covered feed's aggregates render instead."""
    conn = _make_db()
    single_id = _feed_id(conn, "open-meteo", "gfs_global")
    covered_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    day = "2026-07-25"  # D+5
    issued = "2026-07-20T00:00:00Z"  # day_ahead=5
    hrs = _hours(day, 0, 15)

    # Covered feed: 15h of wind (max 10.0 m/s) and precip (total 7.5 mm).
    _seed_varying(
        conn,
        feed_id=covered_id,
        variable="wind",
        issued_at=issued,
        valid_ats=hrs,
        values=[10.0 - i * 0.5 for i in range(15)],  # max 10.0
    )
    _seed_varying(
        conn,
        feed_id=covered_id,
        variable="precip",
        issued_at=issued,
        valid_ats=hrs,
        values=[0.5] * 15,  # total 7.5
    )
    # Single-slot feed: one wind + one precip sample, but scored so it out-ranks
    # the unscored covered feed on the pre-fix ladder.
    _insert_sample(
        conn,
        feed_id=single_id,
        variable="wind",
        issued_at=issued,
        valid_at=f"{day}T12:00:00Z",
        lead_hours=12,
        value=30.0,
    )
    _insert_sample(
        conn,
        feed_id=single_id,
        variable="precip",
        issued_at=issued,
        valid_at=f"{day}T12:00:00Z",
        lead_hours=12,
        value=0.0,
    )
    _seed_scoring_pairs(
        conn, feed_id=single_id, variable="wind", day_ahead=5, forecast=10.5
    )
    _seed_scoring_pairs(
        conn, feed_id=single_id, variable="precip", day_ahead=5, forecast=10.5
    )

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[5]
    assert tile.wind.max_kmh == ms_to_kmh(10.0)  # 36.0, not the single-slot's 108
    assert tile.precip.total_mm == 7.5  # covered feed's total, not the 0.0 slot
    wind_labels = [ref.label for ref in tile.wind.meta.feeds]
    precip_labels = [ref.label for ref in tile.precip.meta.feeds]
    assert feed_label("open-meteo", "gfs_global") not in wind_labels
    assert feed_label("open-meteo", "gfs_global") not in precip_labels
    assert feed_label("open-meteo", "ecmwf_ifs") in wind_labels
    assert feed_label("open-meteo", "ecmwf_ifs") in precip_labels


def test_far_horizon_multipoint_tier_rescues_when_best_below_adequate() -> None:
    """hoare correction: far tile where the BEST-covered feed is only 8h (below
    the 12h adequate floor) and a 1-sample feed OUT-skills it. This pins the
    second (multipoint) tier of the pool.

    Pre-fix reasoning: pool == candidates, ladder ranks by skill under
    blend_depth=1 -> the single-slot gfs feed (higher skill) is selected alone
    -> high == low == 25.0. high != low and high == 16.0 fail on that collapse.
    Post-fix: adequate is empty (8h and 1h both < 12), so the pool falls to the
    multipoint tier = the 8h feed (>= MULTIPOINT_MIN_HOURS; the 1-sample feed is
    below it) -> the 8h feed's real 9.0..16.0 spread renders. WITHOUT the
    multipoint tier the pool would fall straight to all candidates and
    re-collapse -- which is exactly what this test guards."""
    conn = _make_db()
    degenerate_id = _feed_id(conn, "open-meteo", "gfs_global")  # 1 sample, higher skill
    partial_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")  # 8 samples, lower skill
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    set_setting(conn, "forecast_blend_depth", "1")

    day = "2026-07-27"  # D+7
    issued = "2026-07-20T00:00:00Z"
    _seed_varying(
        conn,
        feed_id=partial_id,
        variable="temperature",
        issued_at=issued,
        valid_ats=_hours(day, 0, 8),
        values=[9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0],  # 8h, min 9 max 16
    )
    _insert_sample(
        conn,
        feed_id=degenerate_id,
        variable="temperature",
        issued_at=issued,
        valid_at=f"{day}T12:00:00Z",
        lead_hours=12,
        value=25.0,
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=7, forecast=8.0
    )
    _seed_scoring_pairs(
        conn, feed_id=degenerate_id, variable="temperature", day_ahead=7, forecast=10.2
    )  # higher skill
    _seed_scoring_pairs(
        conn, feed_id=partial_id, variable="temperature", day_ahead=7, forecast=11.0
    )  # lower skill

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[7]
    assert tile.temp.high_c != tile.temp.low_c  # 8h feed's spread survives
    assert tile.temp.high_c == 16.0
    assert tile.temp.low_c == 9.0
    labels = [ref.label for ref in tile.temp.meta.feeds]
    assert feed_label("open-meteo", "gfs_global") not in labels  # single-slot gated out
    assert feed_label("open-meteo", "ecmwf_ifs") in labels


def test_far_horizon_two_single_slot_feeds_collapse_at_default_depth() -> None:
    """Depth-2 fidelity pin for the ACTUAL reported production collapse.

    Reproduced at the DEFAULT forecast_blend_depth=2: two single-slot confident
    feeds are blended and mean(maxes) == mean(mins), so high_c == low_c -- the
    mean-of-two collapse, distinct from the depth-1 single-feed collapse the
    tests above pin. Each single-slot feed's daily high == low (one point), and
    two feeds blend by mean-of-highs / mean-of-lows, so the tile collapses to
    mean(7.71, 10.43) == 9.07 for BOTH high and low. Two single-slot feeds
    (7.71, 10.43) out-skill a 15h covered feed on the pre-fix ladder.
    Pre-fix: pool == candidates -> top-2-by-skill picks both single slots ->
    high == low == 9.07. Post-fix: the >=12h adequate pool (1 member < depth 2,
    no top-up from outside the pool) restricts to the covered feed -> the real
    9.0..16.0 spread renders.
    """
    conn = _make_db()
    single_a = _feed_id(conn, "open-meteo", "gfs_global")
    single_b = _feed_id(conn, "open-meteo", "icon_global")
    covered = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)  # local date 2026-07-20
    # Deliberately DO NOT set forecast_blend_depth -> the default 2 is the point.
    day = "2026-07-27"  # D+7
    issued = "2026-07-20T00:00:00Z"  # 2h before now -> day_ahead=7, not stale
    _seed_varying(
        conn,
        feed_id=covered,
        variable="temperature",
        issued_at=issued,
        valid_ats=_hours(day, 0, 15),
        values=[9.0 + i * 0.5 for i in range(15)],  # 15h, min 9.0 max 16.0
    )
    _insert_sample(
        conn,
        feed_id=single_a,
        variable="temperature",
        issued_at=issued,
        valid_at=f"{day}T12:00:00Z",
        lead_hours=12,
        value=7.71,
    )
    _insert_sample(
        conn,
        feed_id=single_b,
        variable="temperature",
        issued_at=issued,
        valid_at=f"{day}T12:00:00Z",
        lead_hours=12,
        value=10.43,
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=7, forecast=8.0
    )
    # Both single-slot feeds out-skill the covered feed (forecast nearer the
    # seeded observed 10.0 => lower MSE => higher skill), so pre-fix the top-2
    # ladder selects BOTH of them and never the covered feed.
    _seed_scoring_pairs(
        conn, feed_id=single_a, variable="temperature", day_ahead=7, forecast=10.1
    )
    _seed_scoring_pairs(
        conn, feed_id=single_b, variable="temperature", day_ahead=7, forecast=10.2
    )
    _seed_scoring_pairs(
        conn, feed_id=covered, variable="temperature", day_ahead=7, forecast=11.0
    )

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[7]  # D+7 far-horizon tile
    assert tile.temp.high_c != tile.temp.low_c  # NOT the 9.07 mean-collapse
    assert tile.temp.high_c == 16.0
    assert tile.temp.low_c == 9.0
    labels = [ref.label for ref in tile.temp.meta.feeds]
    assert feed_label("open-meteo", "gfs_global") not in labels
    assert feed_label("open-meteo", "icon_global") not in labels
    assert feed_label("open-meteo", "ecmwf_ifs") in labels
