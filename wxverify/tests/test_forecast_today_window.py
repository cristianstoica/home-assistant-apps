"""Regression tests for the today-window fix: ``build_forecast``/``build_hourly``
now load samples from today's LOCAL MIDNIGHT (``local_day_start``) instead of
``floor_hour(now)``. Day 0 becomes the "forecast of record": elapsed hours of
today are included (each resolved to the freshest run covering it), so the
daily high/max is the real full-day value and the >=18-distinct-local-hour
coverage guard can clear after local morning, ending the permanent "partial"
badge.

Fixture idioms (DB setup, ``_hours``, sample seeding) mirror
``test_forecast_service.py``; unlike that file's day-0 fixtures (which
deliberately keep samples outside the old floor_hour window), these
fixtures deliberately seed ELAPSED hours to exercise the widened window.

Isolation: fresh ``sqlite3.connect(":memory:")`` + ``run_migrations`` per
test, same as the rest of the forecast test suite.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from wxverify.core.timeutil import local_day_start
from wxverify.db.migrations import run_migrations
from wxverify.forecast.service import build_forecast, build_hourly

# ---------------------------------------------------------------------------
# Fixture helpers (duplicated from test_forecast_service.py's idioms).
# ---------------------------------------------------------------------------


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    conn.execute(
        """
        INSERT INTO sites (id, name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (1, 'Test Site', 40.0, -105.0, 900.0, 'UTC')
        """
    )
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
    lead_hours: int,
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


def _hours(day: str, start: int, count: int) -> list[str]:
    return [f"{day}T{h:02d}:00:00Z" for h in range(start, start + count)]


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


# ---------------------------------------------------------------------------
# 1. Real daily max from the full local day (elapsed hour carries the max).
# ---------------------------------------------------------------------------


def test_today_high_includes_elapsed_hour_max() -> None:
    """Headline differential: pre-fix the 01:00 elapsed sample is outside the
    floor_hour(12:00) window, so high_c comes from the cooler future hours
    only (20.0). Post-fix the window starts at local midnight, so the elapsed
    hour's 30.0 (the real daily max) is included."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)  # mid-day, UTC site

    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T01:00:00Z",  # elapsed: well before `now`
        lead_hours=1,
        value=30.0,  # the day's real max
    )
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T14:00:00Z",  # future
        lead_hours=14,
        value=20.0,
    )
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T15:00:00Z",  # future
        lead_hours=15,
        value=18.0,
    )

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].temp.high_c == 30.0


def test_today_high_without_elapsed_sample_is_future_only() -> None:
    """Positive control for the test above: with the elapsed 30.0 sample
    removed, the SAME fixture's high_c comes only from the future hours
    (20.0) -- proving the prior test is discriminating on the elapsed
    sample, not vacuously green."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)

    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T14:00:00Z",
        lead_hours=14,
        value=20.0,
    )
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T15:00:00Z",
        lead_hours=15,
        value=18.0,
    )

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].temp.high_c == 20.0


# ---------------------------------------------------------------------------
# 2. Partial badge clears once the full local day (>=18h) is in view.
# ---------------------------------------------------------------------------


def test_partial_badge_clears_after_local_morning_with_full_day_coverage() -> None:
    """Headline regression (verified differential). At 10:00 local
    the pre-fix window (floor_hour) only sees hours 10..23 -- 14 distinct
    hours, under MIN_COVERAGE_HOURS (18) -- so `partial` reads True pre-fix.
    Post-fix the window starts at local midnight, so all 24 hours are in
    view -- `partial` reads False."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)  # well after 06:00 local

    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T18:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),  # full day: 24 distinct hours
        value=15.0,
    )

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].temp.meta.partial is False


def test_partial_badge_stays_set_under_eighteen_hour_coverage() -> None:
    """Paired negative: a feed covering fewer than MIN_COVERAGE_HOURS (18)
    still reads partial True post-fix -- the suppression only kicks in once
    real coverage clears the guard, pinned from both directions."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)

    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T18:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 10),  # only 10 distinct hours
        value=15.0,
    )

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].temp.meta.partial is True


# ---------------------------------------------------------------------------
# 3. Freshest run wins for an elapsed hour, at the service (not just SQL)
#    layer -- the SQL half is pinned separately by
#    test_latest_run_pick_keeps_newest_issued_at_value in test_forecast_data.py.
# ---------------------------------------------------------------------------


def test_freshest_run_wins_for_elapsed_hour_via_build_hourly() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
    valid_at = "2026-07-20T05:00:00Z"  # elapsed local hour

    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",  # older run
        valid_at=valid_at,
        lead_hours=9,
        value=10.0,
    )
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-20T02:00:00Z",  # newer run
        valid_at=valid_at,
        lead_hours=3,
        value=25.0,
    )

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["hours"] == [valid_at]
    blend = payload["blend"]
    assert isinstance(blend, dict)
    assert blend["temp_c"] == [25.0]
    feeds = payload["feeds"]
    assert isinstance(feeds, list)
    assert feeds[0]["temp_c"] == [25.0]


# ---------------------------------------------------------------------------
# 4. Window lower bound pinned from BOTH sides in one test, on a non-UTC
#    site (Europe/Berlin, UTC+2 in July) so the boundary genuinely exercises
#    the local-midnight conversion rather than a naive UTC floor.
# ---------------------------------------------------------------------------


def test_window_lower_bound_excludes_yesterday_includes_local_midnight() -> None:
    """Berlin local midnight for 2026-07-20 is 2026-07-19T22:00:00Z. A sample
    one hour earlier (23:00 local yesterday) must not appear on ANY tile; a
    sample AT that exact instant (00:00 local today) must land on day 0.
    Also a real differential: pre-fix floor_hour(12:00 UTC)=12:00 UTC excludes
    BOTH samples (22:00Z < 12:00Z), so the boundary sample would be missing
    pre-fix too."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)  # Berlin local 14:00 (CEST)

    excluded_valid_at = "2026-07-19T21:00:00Z"  # Berlin 23:00 on 2026-07-19
    boundary_valid_at = "2026-07-19T22:00:00Z"  # Berlin 00:00 on 2026-07-20 (midnight)

    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T18:00:00Z",
        valid_at=excluded_valid_at,
        lead_hours=3,
        value=999.0,
    )
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T18:00:00Z",
        valid_at=boundary_valid_at,
        lead_hours=4,
        value=42.0,
    )

    payload = build_hourly(conn, site_id=1, timezone="Europe/Berlin", day=0, now=now)
    hours = payload["hours"]
    assert isinstance(hours, list)
    assert boundary_valid_at in hours
    assert excluded_valid_at not in hours

    # Defense in depth: the excluded sample cannot leak onto any tile at all.
    view = build_forecast(
        conn, site_id=1, timezone="Europe/Berlin", rain_threshold_mm=0.2, now=now
    )
    highs = [tile.temp.high_c for tile in view.tiles if tile.temp.high_c is not None]
    assert 999.0 not in highs


# ---------------------------------------------------------------------------
# 5. build_hourly day 0 includes elapsed valid_ats; every per-feed/blend
#    series stays lockstep with len(hours).
# ---------------------------------------------------------------------------


def test_build_hourly_day0_includes_elapsed_hours_with_lockstep_series() -> None:
    conn = _make_db()
    winner_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    other_id = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)

    # Elapsed hours from one feed, future hours from another -- both are
    # 2-sample "multipoint" feeds so the default blend_depth=2 selects both.
    _insert_sample(
        conn,
        feed_id=winner_id,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T02:00:00Z",
        lead_hours=2,
        value=9.0,
    )
    _insert_sample(
        conn,
        feed_id=winner_id,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T03:00:00Z",
        lead_hours=3,
        value=11.0,
    )
    _insert_sample(
        conn,
        feed_id=other_id,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T14:00:00Z",
        lead_hours=14,
        value=20.0,
    )
    _insert_sample(
        conn,
        feed_id=other_id,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T15:00:00Z",
        lead_hours=15,
        value=22.0,
    )

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    hours = payload["hours"]
    assert isinstance(hours, list)
    assert hours == [
        "2026-07-20T02:00:00Z",
        "2026-07-20T03:00:00Z",
        "2026-07-20T14:00:00Z",
        "2026-07-20T15:00:00Z",
    ]
    assert "2026-07-20T02:00:00Z" in hours  # elapsed valid_at present

    n = len(hours)
    blend = payload["blend"]
    assert isinstance(blend, dict)
    assert len(blend["temp_c"]) == n
    assert len(blend["wind_kmh"]) == n
    assert len(blend["precip_mm"]) == n
    feeds = payload["feeds"]
    assert isinstance(feeds, list)
    assert len(feeds) == 2
    for feed in feeds:
        assert len(feed["temp_c"]) == n
        assert len(feed["wind_kmh"]) == n
        assert len(feed["precip_mm"]) == n


# ---------------------------------------------------------------------------
# 6. local_day_start unit tests (pure function, no DB). Expected UTC instants
#    are computed independently from zoneinfo's fold semantics / documented
#    offsets, never by calling local_day_start itself.
# ---------------------------------------------------------------------------


def test_local_day_start_utc_identity() -> None:
    now = datetime(2026, 7, 20, 15, 30, tzinfo=UTC)
    assert local_day_start(now, "UTC") == datetime(2026, 7, 20, 0, 0, tzinfo=UTC)


def test_local_day_start_nonzero_offset_zone_tokyo() -> None:
    """Asia/Tokyo is a fixed UTC+9 zone (no DST): local midnight of
    2026-07-20 is independently 9h before UTC midnight, i.e. 2026-07-19
    15:00 UTC."""
    now = datetime(2026, 7, 20, 3, 0, tzinfo=UTC)  # Tokyo local: 2026-07-20 12:00 JST
    expected = datetime(2026, 7, 19, 15, 0, tzinfo=UTC)
    assert local_day_start(now, "Asia/Tokyo") == expected


def test_local_day_start_spring_forward_at_midnight_resolves_forward() -> None:
    """America/Havana springs forward AT local midnight on 2026-03-08: clocks
    jump 00:00:00-05:00 -> 01:00:00-04:00, so wall-clock 00:00-00:59 never
    occurs that day. Independently verified via zoneinfo (not by calling the
    function under test): constructing the nonexistent 2026-03-08T00:00:00
    Havana wall time with fold=0 (the ``.replace()`` default) resolves via
    the pre-transition offset to UTC 2026-03-08T05:00:00 -- exactly the
    zone's actual first instant of that local day (01:00:00-04:00)."""
    now = datetime(2026, 3, 8, 14, 0, tzinfo=UTC)  # Havana local: 10:00-04:00
    expected = datetime(2026, 3, 8, 5, 0, tzinfo=UTC)
    assert local_day_start(now, "America/Havana") == expected


def test_local_day_start_fall_back_ambiguous_midnight_resolves_earlier() -> None:
    """America/Havana falls back AT local midnight on 2026-11-01, so wall-clock
    00:00-00:59 occurs TWICE (00:00:00-04:00, then again as 00:00:00-05:00 an
    hour of UTC-time later) -- an ambiguous midnight. (Substituted for
    America/Sao_Paulo: independently scanning every
    Sao Paulo DST-end transition from 2000-2019 with zoneinfo shows its
    fall-back always lands on the PRIOR day's 23:00 hour, never on an
    ambiguous midnight, so it can't pin this branch -- Havana's own fall-back
    six months later does.) Independently verified via zoneinfo fold
    semantics: fold=0 (the ``.replace()`` default, PEP 495's "earlier"
    occurrence) resolves to the pre-transition DST offset -04:00, i.e.
    2026-11-01T04:00:00Z."""
    now = datetime(2026, 11, 1, 10, 0, tzinfo=UTC)  # Havana local: 05:00-05:00
    expected = datetime(2026, 11, 1, 4, 0, tzinfo=UTC)
    assert local_day_start(now, "America/Havana") == expected


def test_local_day_start_uses_local_calendar_date_across_date_line() -> None:
    """A UTC-date implementation (deriving the day from now's UTC date rather
    than the local calendar date) would return 2026-07-20T04:00:00Z here --
    a future instant relative to `now`. 2026-07-20T02:00:00Z in
    America/New_York is locally 2026-07-19 22:00 EDT (UTC-4), so local
    midnight of the LOCAL calendar day (2026-07-19) is
    2026-07-19T00:00:00-04:00 = 2026-07-19T04:00:00Z, independently
    constructed here."""
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)  # NY local: 2026-07-19 22:00 EDT
    expected = datetime(2026, 7, 19, 4, 0, tzinfo=UTC)
    assert local_day_start(now, "America/New_York") == expected


# ---------------------------------------------------------------------------
# 7. Empty-state consequence: a site whose ONLY samples are elapsed-today
#    hours now renders tiles instead of the empty state.
# ---------------------------------------------------------------------------


def test_only_elapsed_today_samples_render_tiles_not_empty() -> None:
    """Documented behavior change: pre-fix, an elapsed-only sample set is
    entirely outside the floor_hour(now) window, so `samples` is empty and
    the view reads `empty=True`. Post-fix the local-midnight window includes
    it, so tiles render (with the elapsed data)."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)

    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_at="2026-07-20T05:00:00Z",  # elapsed only
        lead_hours=5,
        value=14.0,
    )

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.empty is False
    assert view.tiles[0].temp.high_c == 14.0


def test_zero_samples_at_all_still_empty() -> None:
    """Paired negative: with truly zero samples the view stays empty --
    the fix widens the window, it doesn't remove the empty-state check."""
    conn = _make_db()
    now = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.empty is True
    assert view.tiles == []
