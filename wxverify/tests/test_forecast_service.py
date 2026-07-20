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

from wxverify.db.migrations import run_migrations
from wxverify.forecast.service import (
    RAIN_GLYPH_MIN_CHANCE_PCT,
    build_forecast,
    build_hourly,
    relative_ago,
)
from wxverify.settings.keys import set_setting

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
