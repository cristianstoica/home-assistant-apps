"""Item F — daily extrema decided by coverage, before skill ranks (F.11).

F-T1 through F-T5 live in ``tests/test_forecast_selection.py`` (pure
``select_cell_feeds``/``CellCandidate`` unit coverage, already present
there). This file covers F-T6 through F-T31:

* F-T6 through F-T12 — the completeness rule itself (``local_day_slots``,
  ``local_day_bounds``, ``covers_local_day``), pure and DB-free.
* F-T13 through F-T17 — end to end through ``build_forecast``/
  ``build_hourly`` (the live Forecast page's surfaces).
* F-T18 through F-T21 — the persisted forecast-of-record row, the
  simulator's purity with respect to the extrema fields, and three-surface
  agreement.
* F-T22 through F-T25 — the stale and low-confidence/rebuilding badge
  rules: a stale contributor alone, an unscored extrema set beside a
  confident blend set, and suppressed extrema contributing neither warning
  (with a paired positive stale variant).
* F-T26 — confidence states are independent per side (blend vs. extrema)
  when their contributor sets differ, and roll up identically to
  ``today``'s state when the contributor set is shared, swept over fresh,
  stale, low-confidence and low-confidence-plus-rebuilding.
* F-T27 — wind and precip stay immune to temperature's extrema-side state,
  for both the warning badges and the stale badge.
* F-T28 through F-T30 — tile-level state/confidence-state rollup across
  variables, badge/tooltip/persisted-metadata agreement, and rebuilding
  precedence on the extrema set.
* F-T31 — Lord Howe Island (UTC+10:30/+11), whose DST shift needs every
  UTC-hour instant of the local day, not a fixed hour count.

All fixture identities are synthetic (``open-meteo``/``example-src``
models already seeded by migrations, or hand-picked feed ids), and DST
cases use ``Europe/Berlin`` (chosen only because Item F's own plan does;
no real station or city is implied). Every expected value is derived from
reading the named production function, never transcribed from the plan
document.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

from tests.helpers import asof_persistence_feed_id
from wxverify.core.timeutil import isoformat_utc, local_day_slots
from wxverify.core.units import ms_to_kmh
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast.aggregate import (
    EXTREMA_COVERAGE_INSUFFICIENT,
    covers_local_day,
    displayed_daily,
)
from wxverify.forecast.data import FutureSampleRow
from wxverify.forecast.service import build_forecast, build_hourly
from wxverify.scoring.leaderboard import leaderboard_with_status, resolve_window
from wxverify.scoring.metrics import strategy_for
from wxverify.settings.keys import get_number_setting, set_setting
from wxverify.verification.coverage import (
    EXCLUDE_INSUFFICIENT_COVERAGE,
    QUANTITY_TEMPERATURE_HIGH,
    QUANTITY_TEMPERATURE_LOW,
    DayBounds,
    local_day_bounds,
)
from wxverify.verification.record import build_forecast_record, resolve_snapshot_utc
from wxverify.verification.runs import (
    RosterFeed,
    RunConfig,
    capture_config_snapshot,
    input_fingerprint,
    result_basis_fingerprint,
)
from wxverify.verification.simulate import (
    _entities_for_selection,  # pyright: ignore[reportPrivateUsage]
    simulate_snapshot_day,
)
from wxverify.web.context import feed_label
from wxverify.web.render import env as jinja_env

# ---------------------------------------------------------------------------
# Shared DB harness (mirrors tests/test_forecast_service.py).
# ---------------------------------------------------------------------------


def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    conn.execute(
        """
        INSERT INTO sites (id, name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (1, 'Test Site', 0.0, 0.0, 0.0, 'UTC')
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
             day_ahead, forecast, observed, error, abs_error, sq_error,
             tz_generation_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ensure_published_generation(conn, site_id),
        ),
    )


_FAR_FUTURE_VALID_ATS = (
    "2035-07-01T00:00:00Z",
    "2035-07-01T01:00:00Z",
    "2035-07-01T02:00:00Z",
)
_FAR_FUTURE_LEAD_HOURS = (1, 2, 3)


def _seed_scoring_pairs(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    day_ahead: int,
    forecast: float,
) -> None:
    """3 far-future scoring pairs against a fixed observed=10.0 baseline."""
    pairs = zip(_FAR_FUTURE_VALID_ATS, _FAR_FUTURE_LEAD_HOURS, strict=True)
    for valid_at, lead in pairs:
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


def _seed_complete_score_cache(
    conn: sqlite3.Connection, *, variable: str, day_ahead: int
) -> None:
    """Fresh, complete score_cache snapshot for every feed with pairs at
    (site 1, `variable`, `day_ahead`) — required for cache-backed
    ``forecast_ranking`` to serve live numbers rather than 'rebuilding'.
    Mirrors ``tests/test_forecast_service.py``'s identically-named helper.
    """
    from wxverify.scoring.cache import upsert_score_cache

    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    resolved = resolve_window(conn, "rolling")
    feed_ids = {
        int(row["feed_id"])
        for row in conn.execute(
            "SELECT DISTINCT feed_id FROM forecast_pairs "
            "WHERE site_id=1 AND variable=? AND day_ahead=?",
            (variable, day_ahead),
        ).fetchall()
    }
    for feed_id in feed_ids:
        result = strategy_for(variable).aggregate(
            conn,
            site_id=1,
            feed_id=feed_id,
            variable=variable,
            day_ahead=day_ahead,
            window_cutoff=resolved.cutoff,
            min_n=min_n,
        )
        upsert_score_cache(
            conn,
            site_id=1,
            feed_id=feed_id,
            variable=variable,
            day_ahead=day_ahead,
            window_key=resolved.window_key,
            result=result,
            computed_at=isoformat_utc(),
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


def _seed_varying(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_ats: list[str],
    values: list[float],
) -> None:
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


# ===========================================================================
# F-T6 -- local_day_slots / local_day_bounds parity across DST transitions
# in two zones.
# ===========================================================================


def test_local_day_bounds_agrees_with_local_day_slots_across_dst_transitions() -> None:
    # Berlin: spring-forward 2026-03-29 (23h), fall-back 2026-10-25 (25h).
    # New York: spring-forward 2026-03-08 (23h), fall-back 2026-11-01 (25h).
    # Each sweep also covers a plain 24h day either side of the transition.
    cases = [
        ("Europe/Berlin", date(2026, 3, 28)),
        ("Europe/Berlin", date(2026, 3, 29)),
        ("Europe/Berlin", date(2026, 3, 30)),
        ("Europe/Berlin", date(2026, 10, 24)),
        ("Europe/Berlin", date(2026, 10, 25)),
        ("Europe/Berlin", date(2026, 10, 26)),
        ("America/New_York", date(2026, 3, 7)),
        ("America/New_York", date(2026, 3, 8)),
        ("America/New_York", date(2026, 3, 9)),
        ("America/New_York", date(2026, 10, 31)),
        ("America/New_York", date(2026, 11, 1)),
        ("America/New_York", date(2026, 11, 2)),
    ]
    for timezone, local_date in cases:
        start, end, expected = local_day_slots(local_date, timezone)
        bounds = local_day_bounds(local_date, timezone)
        assert isinstance(bounds, DayBounds)
        assert bounds.start_utc == start
        assert bounds.end_utc == end
        assert bounds.expected_slots == expected
        assert bounds.local_date == local_date
        assert bounds.timezone == timezone
    # Pin the transition-day slot counts as literals, not just parity: a bug
    # that shifts BOTH functions identically would pass every assertion
    # above while getting the actual DST shape wrong. Keep the parity check
    # above as wiring coverage only -- timezone correctness rests on these
    # independently written expected values, not on the parity assertion.
    assert local_day_slots(date(2026, 3, 29), "Europe/Berlin")[2] == 23
    assert local_day_slots(date(2026, 10, 25), "Europe/Berlin")[2] == 25
    assert local_day_slots(date(2026, 3, 8), "America/New_York")[2] == 23
    assert local_day_slots(date(2026, 11, 1), "America/New_York")[2] == 25
    assert local_day_slots(date(2026, 3, 28), "Europe/Berlin")[2] == 24


# ===========================================================================
# F-T7 -- 23-hour spring-forward local day.
# ===========================================================================

_BERLIN_SPRING_START = datetime(2026, 3, 28, 23, tzinfo=UTC)  # local midnight UTC
_BERLIN_SPRING_DATE = date(2026, 3, 29)
_BERLIN_FALL_START = datetime(2026, 10, 24, 22, tzinfo=UTC)
_BERLIN_FALL_DATE = date(2026, 10, 25)


def _instants(
    start: datetime, count: int, *, omit: set[int] | None = None
) -> list[str]:
    omit = omit or set()
    return [
        (start + timedelta(hours=h)).isoformat().replace("+00:00", "Z")
        for h in range(count)
        if h not in omit
    ]


def test_23_hour_spring_forward_day_needs_all_23() -> None:
    full = _instants(_BERLIN_SPRING_START, 23)
    assert len(full) == 23
    assert covers_local_day(
        full, local_date=_BERLIN_SPRING_DATE, timezone="Europe/Berlin"
    )

    short = _instants(_BERLIN_SPRING_START, 23, omit={5})
    assert len(short) == 22
    assert not covers_local_day(
        short, local_date=_BERLIN_SPRING_DATE, timezone="Europe/Berlin"
    )


# ===========================================================================
# F-T8 -- 25-hour fall-back local day, including the repeated-hour pair.
# ===========================================================================


def test_25_hour_fall_back_day_needs_all_25_including_the_repeated_hour() -> None:
    full = _instants(_BERLIN_FALL_START, 25)
    assert len(full) == 25
    assert covers_local_day(
        full, local_date=_BERLIN_FALL_DATE, timezone="Europe/Berlin"
    )

    # Drop a generic interior hour: 24 present, ineligible.
    generic_short = _instants(_BERLIN_FALL_START, 25, omit={10})
    assert len(generic_short) == 24
    assert not covers_local_day(
        generic_short, local_date=_BERLIN_FALL_DATE, timezone="Europe/Berlin"
    )

    # Drop one of the TWO UTC instants sharing local wall-clock 02:00 (index
    # 3 == 2026-10-25T01:00:00Z, the CET repeat of local 02:00 after index 2
    # == 2026-10-25T00:00:00Z, the CEST first occurrence) -- both are
    # distinct UTC instants and the rule counts UTC instants, not local
    # wall-clock hours, so omitting either one alone is still a genuine gap.
    fold_short = _instants(_BERLIN_FALL_START, 25, omit={3})
    assert len(fold_short) == 24
    assert not covers_local_day(
        fold_short, local_date=_BERLIN_FALL_DATE, timezone="Europe/Berlin"
    )


# ===========================================================================
# F-T9 -- interior gap: 23 of 24 with a midday hour absent is ineligible.
# ===========================================================================


def test_interior_midday_gap_is_ineligible() -> None:
    start = datetime(2026, 6, 15, 0, tzinfo=UTC)  # UTC, no DST complication
    target = date(2026, 6, 15)
    full = _instants(start, 24)
    assert covers_local_day(full, local_date=target, timezone="UTC")

    midday_gap = _instants(start, 24, omit={13})  # 13:00Z absent
    assert len(midday_gap) == 23
    assert not covers_local_day(midday_gap, local_date=target, timezone="UTC")


# ===========================================================================
# F-T10 -- both day boundaries: first-hour miss and last-hour miss.
# ===========================================================================


def test_missing_either_boundary_hour_is_ineligible() -> None:
    start = datetime(2026, 6, 15, 0, tzinfo=UTC)
    target = date(2026, 6, 15)

    missing_first = _instants(start, 24, omit={0})
    assert len(missing_first) == 23
    assert not covers_local_day(missing_first, local_date=target, timezone="UTC")

    missing_last = _instants(start, 24, omit={23})
    assert len(missing_last) == 23
    assert not covers_local_day(missing_last, local_date=target, timezone="UTC")


# ===========================================================================
# F-T11 -- an instant outside [start, end) does not count.
# ===========================================================================


def test_instant_outside_window_does_not_count_toward_coverage() -> None:
    start = datetime(2026, 6, 15, 0, tzinfo=UTC)
    target = date(2026, 6, 15)

    # 23 real in-window instants plus 1 from the adjacent day: still only
    # 23 DISTINCT in-window instants, so still ineligible -- the extra
    # instant cannot substitute for the missing one.
    in_window = _instants(start, 24, omit={12})
    adjacent_day = (start - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    mixed = in_window + [adjacent_day]
    assert len(mixed) == 24  # 23 in-window + 1 out-of-window
    assert not covers_local_day(mixed, local_date=target, timezone="UTC")


# ===========================================================================
# F-T12 -- two complementary partial feeds don't qualify on their union.
# ===========================================================================


def test_two_complementary_partial_feeds_never_qualify_on_the_union() -> None:
    # Feed 1 covers local hours 00-11 (12h), feed 2 covers 12-23 (12h).
    # Driven through build_forecast so eligibility is decided where the
    # candidate is actually built (service.py), not pre-set on a
    # hand-constructed CellCandidate -- eligibility is decided PER FEED,
    # never on a merged/union coverage (F.5's rule), so the pair together
    # still yields no extrema set even though their union spans the day.
    conn = _make_db()
    morning_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    evening_id = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 14, 0, tzinfo=UTC)

    morning_ats = _hours("2026-07-20", 0, 12)
    _seed_hourly(
        conn,
        feed_id=morning_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=morning_ats,
        value=10.0,
    )
    evening_ats = _hours("2026-07-20", 12, 12)
    _seed_hourly(
        conn,
        feed_id=evening_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=evening_ats,
        value=20.0,
    )
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_unavailable is True
    assert today.temp.high_c is None
    assert today.temp.low_c is None
    assert today.temp.meta.feeds == []

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    feed_labels_present = [f["label"] for f in payload["feeds"]]
    assert feed_label("open-meteo", "ecmwf_ifs") in feed_labels_present
    assert feed_label("open-meteo", "gfs_global") in feed_labels_present


# ===========================================================================
# F-T13 -- morning-only candidate through build_forecast: suppressed, and
# rendered with the exact F.6 label string.
# ===========================================================================


def _render_tiles(*, site_id: int, view: object) -> str:
    tmpl = jinja_env.get_template("forecast/_tiles.html")
    return tmpl.render(
        url=lambda p: str(p), site=SimpleNamespace(id=site_id), view=view
    )


def test_morning_only_candidate_suppresses_and_renders_the_f6_label() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 14, 0, tzinfo=UTC)

    # Morning-only: hours 00-11 (12h), well short of the full 24h local day.
    valid_ats = _hours("2026-07-20", 0, 12)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
        value=15.0,
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.high_c is None
    assert today.temp.low_c is None
    assert today.temp.meta.extrema_unavailable is True

    html = _render_tiles(site_id=1, view=view)
    assert "Daily high/low unavailable &mdash; partial coverage" in html
    # The High/Low row must not render a degree sign when suppressed.
    high_low_row_start = html.index("High / Low")
    high_low_row_end = html.index("</div>", high_low_row_start)
    assert "°" not in html[high_low_row_start:high_low_row_end]


# ===========================================================================
# F-T14 -- fully covered feed with a small spread still renders both values.
# ===========================================================================


def test_fully_covered_small_spread_still_renders_both_values() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 14, 0, tzinfo=UTC)

    valid_ats = _hours("2026-07-20", 0, 24)
    values = [15.0] * 24
    values[3] = 15.2
    values[16] = 14.9
    _seed_varying(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
        values=values,
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_unavailable is False
    assert today.temp.high_c == 15.2
    assert today.temp.low_c == 14.9


# ===========================================================================
# F-T15 -- lower-skill fully-covered feed beats higher-skill partial feed.
# ===========================================================================


def test_fully_covered_feed_beats_higher_skill_partial_feed() -> None:
    conn = _make_db()
    full_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    partial_id = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    now = datetime(2026, 7, 20, 14, 0, tzinfo=UTC)
    set_setting(conn, "forecast_blend_depth", "1")

    full_ats = _hours("2026-07-20", 0, 24)
    full_values = [10.0 + i * 0.1 for i in range(24)]
    _seed_varying(
        conn,
        feed_id=full_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=full_ats,
        values=full_values,
    )
    # Partial: 12 covered hours (>= MIN_SPREAD_HOURS so it clears the blend
    # coverage pool), but not the whole local day.
    partial_ats = _hours("2026-07-20", 4, 12)
    _seed_varying(
        conn,
        feed_id=partial_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=partial_ats,
        values=[20.0 + i * 0.1 for i in range(12)],
    )
    # Both confident; the partial feed has the HIGHER skill (closer forecast
    # to the fixed observed=10.0 baseline).
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=1, forecast=5.0
    )
    _seed_scoring_pairs(
        conn, feed_id=full_id, variable="temperature", day_ahead=1, forecast=11.0
    )
    _seed_scoring_pairs(
        conn, feed_id=partial_id, variable="temperature", day_ahead=1, forecast=10.2
    )
    _seed_complete_score_cache(conn, variable="temperature", day_ahead=1)
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_unavailable is False
    assert today.temp.high_c == round(full_values[-1], 10)
    assert today.temp.low_c == full_values[0]
    labels = [ref.label for ref in today.temp.meta.feeds]
    assert feed_label("open-meteo", "ecmwf_ifs") in labels
    assert feed_label("open-meteo", "gfs_global") not in labels

    html = _render_tiles(site_id=1, view=view)
    assert f'title="Feeds: {feed_label("open-meteo", "ecmwf_ifs")}"' in html


# ===========================================================================
# F-T16 -- the hourly drill-down survives suppression.
# ===========================================================================


def test_hourly_drilldown_survives_temperature_suppression() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 14, 0, tzinfo=UTC)

    valid_ats = _hours("2026-07-20", 0, 12)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
        value=15.0,
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].temp.meta.extrema_unavailable is True  # tile suppressed

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    feeds = payload["feeds"]
    assert isinstance(feeds, list)
    assert len(feeds) == 1
    assert feeds[0]["feed_id"] == feed_id
    temp_series = feeds[0]["temp_c"]
    assert isinstance(temp_series, list)
    assert any(v is not None for v in temp_series)
    blend = payload["blend"]
    assert isinstance(blend, dict)
    blend_temp = blend["temp_c"]
    assert isinstance(blend_temp, list)
    assert any(v is not None for v in blend_temp)


# ===========================================================================
# F-T17 -- wind and precipitation are untouched while temperature suppresses.
# ===========================================================================


def test_wind_and_precip_untouched_while_temperature_suppresses() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 14, 0, tzinfo=UTC)

    valid_ats = _hours("2026-07-20", 0, 12)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
        value=15.0,
    )
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
        value=5.0,
    )
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
        value=1.0,
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_unavailable is True  # temperature suppressed

    # Wind and precip never consult extrema_eligible: same 12h partial
    # fixture still aggregates for both, tagged "partial" rather than
    # suppressed.
    assert today.wind.meta.extrema_unavailable is False
    assert today.wind.max_kmh == ms_to_kmh(5.0)
    assert today.wind.meta.partial is True
    assert today.precip.meta.extrema_unavailable is False
    assert today.precip.total_mm == 12.0  # 12 hours * 1.0 mm
    # wet-hour share: all 12 seeded hours are 1.0mm, >= the 0.2mm threshold,
    # so the single feed's wet share is 12/12 == 1.0 -> chance_pct == 100.
    assert today.precip.chance_pct == 100
    assert today.precip.meta.partial is True
    assert today.partial is True  # tile-level badge still fires


# ===========================================================================
# F-T18 / F-T19 -- persisted forecast-of-record row.
# ===========================================================================

_RECORD_DAY = date(2035, 6, 15)


def _record_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _record_site(conn: sqlite3.Connection, name: str, timezone: str = "UTC") -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 0.0, 0.0, 0.0, ?)
        """,
        (name, timezone),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _record_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('example-src', ?, 1, 360, 192)
        """,
        (model,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _record_insert_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    issued_at: str,
    valid_ats: list[datetime],
    value: float,
    variable: str = "temperature",
) -> None:
    issued = datetime.fromisoformat(issued_at.replace("Z", "+00:00"))
    for valid in valid_ats:
        lead = max(1, int((valid - issued).total_seconds() // 3600))
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-x', ?)
            """,
            (
                site_id,
                feed_id,
                variable,
                issued_at,
                isoformat_utc(valid),
                lead,
                value,
                fetched_at_for(issued_at),
            ),
        )


def fetched_at_for(issued_at: str) -> str:
    return issued_at


def _record_snapshot_t(local_date: date = _RECORD_DAY) -> datetime:
    return resolve_snapshot_utc("UTC", local_date, "07:00")


def test_persisted_row_for_a_suppressed_temperature_cell() -> None:
    conn = _record_conn()
    site_id = _record_site(conn, "site-a")
    ensure_published_generation(conn, site_id)
    feed_id = _record_feed(conn, "model-a")

    # 10 covered hours: under MIN_COVERAGE_HOURS/TEMP_TRUTH_MIN_HOURS (18)
    # AND under the 24h extrema requirement.
    valid_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(0, 10)
    ]
    _record_insert_day(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=valid_ats,
        value=12.0,
    )
    t = _record_snapshot_t()
    build_forecast_record(
        conn, site_id, _RECORD_DAY.isoformat(), now=t + timedelta(minutes=5)
    )

    row = conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = ? AND variable = 'temperature' AND display_lead = 0
        """,
        (site_id,),
    ).fetchone()
    assert row is not None
    quantities = json.loads(str(row["daily_quantities"]))
    displayed = quantities["displayed"]
    assert displayed["high_c"] is None
    assert displayed["low_c"] is None
    assert displayed["extrema_feed_ids"] == []
    assert displayed["extrema_coverage"] == EXTREMA_COVERAGE_INSUFFICIENT
    # "0 appears under neither extremum key": None, not 0.0, is what a
    # suppressed cell writes.
    assert displayed["high_c"] != 0
    assert displayed["low_c"] != 0

    # outcomes/candidates participation: computed over the clearing subset,
    # a path extrema eligibility never reaches. With one feed under the
    # 18h clearing guard, the fallback keeps it as the sole clearing member
    # (partial=True), so it is still the scored entity -- unaffected by the
    # temperature extrema suppression above.
    outcomes = {o["quantity"]: o for o in quantities["outcomes"]}
    assert set(outcomes) == {QUANTITY_TEMPERATURE_HIGH, QUANTITY_TEMPERATURE_LOW}
    for outcome in outcomes.values():
        assert outcome["covered_hours"] == 10
        assert outcome["eligible"] is False
        assert outcome["exclusion_reason"] == EXCLUDE_INSUFFICIENT_COVERAGE

    candidates = json.loads(str(row["candidates"]))
    by_id = {c["feed_id"]: c for c in candidates}
    assert by_id[feed_id]["participation"] == "contributed"


def test_candidates_json_carries_extrema_eligible_for_real_and_no_sample_feeds() -> (
    None
):
    # F-T19: the hand-built dict at record.py's `_candidate_records` for a
    # feed with NO samples this cell -- the one spot `pyright` cannot check
    # for a missing field. A real candidate carries `extrema_eligible` from
    # its own dataclass; the no-sample feed must carry it too, explicitly
    # False.
    conn = _record_conn()
    site_id = _record_site(conn, "site-a")
    ensure_published_generation(conn, site_id)
    sampled_id = _record_feed(conn, "model-sampled")
    _record_feed(conn, "model-empty")  # configured (default_subscribed=1),
    # but never gets any forecast_samples row for this cell.

    valid_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(24)
    ]
    _record_insert_day(
        conn,
        site_id=site_id,
        feed_id=sampled_id,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=valid_ats,
        value=10.0,
    )
    t = _record_snapshot_t()
    build_forecast_record(
        conn, site_id, _RECORD_DAY.isoformat(), now=t + timedelta(minutes=5)
    )

    row = conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = ? AND variable = 'temperature' AND display_lead = 0
        """,
        (site_id,),
    ).fetchone()
    assert row is not None
    candidates = json.loads(str(row["candidates"]))
    by_id = {c["feed_id"]: c for c in candidates}
    empty_id = next(
        fid
        for fid, c in by_id.items()
        if fid != sampled_id and c["participation"] == "no_samples"
    )
    assert by_id[sampled_id]["extrema_eligible"] is True
    assert by_id[empty_id]["extrema_eligible"] is False


# ===========================================================================
# F-T20 -- nothing scored moved.
# ===========================================================================


def test_scored_temperature_entity_is_the_pre_f_clearing_subset_value() -> None:
    # M1 remediation: a self-comparison of two identical
    # _entities_for_selection calls proves determinism, not "unchanged by
    # Item F" -- there is no "before" to diff against, and it cannot see
    # simulate.py:727/734 (the actual mutant sites) because feed_ids is an
    # argument it hands in, never derived internally. Drive the real
    # pipeline instead: simulate_snapshot_day -> select_cell_feeds(...,
    # extrema_coverage_required=False) -> feed_ids=[c.feed_id for c in
    # selection.feeds] -> _entities_for_selection, with a fixture only that
    # path can produce -- one confident temperature feed covering 20 of 24
    # local hours (blend-eligible via clears_coverage/MIN_COVERAGE_HOURS=18,
    # NOT extrema-eligible via covers_local_day, which needs all 24).
    from wxverify.verification.methodology import METHODOLOGY_VERSION

    assert METHODOLOGY_VERSION == 2

    conn = _record_conn()
    site_id = _record_site(conn, "site-a")
    generation_id = ensure_published_generation(conn, site_id)
    feed_id = _record_feed(conn, "model-a")

    local_date = _RECORD_DAY  # 2035-06-15
    values = [10.0 + i * 0.1 for i in range(20)]  # hours 00-19 only (20h)
    for i, value in enumerate(values):
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'temperature', '2035-06-14T20:00:00Z', ?, ?, ?,
                    '{}', 'run-x', '2035-06-14T20:00:00Z')
            """,
            (
                site_id,
                feed_id,
                f"2035-06-15T{i:02d}:00:00Z",
                i + 1,
                value,
            ),
        )
    conn.commit()

    snapshot = capture_config_snapshot(conn, site_id)
    period_start = local_date.isoformat()
    period_end = local_date.isoformat()
    run_id = int(
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint)
            VALUES (?, ?, 2, '0.16.0-test', 'running', 1, ?, ?, ?, ?, 1, 40, ?)
            """,
            (
                site_id,
                generation_id,
                json.dumps(snapshot),
                period_start,
                period_end,
                period_end,
                input_fingerprint(conn, site_id, snapshot),
            ),
        ).lastrowid
    )
    conn.commit()

    cfg = RunConfig(
        site_id=site_id,
        run_id=run_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        wall_clock="07:00",
        blend_depth=1,
        blend_depths={"temperature": 1, "wind": 1, "precip": 1},
        min_n=3,
        window_days=30,
        tz_generation_id=generation_id,
        roster=(
            RosterFeed(
                feed_id=feed_id,
                source="example-src",
                model="model-a",
                max_lead_hours=192,
            ),
        ),
        period_start=period_start,
        period_end=period_end,
        bootstrap_seed=1,
        bootstrap_resamples=40,
    )

    # Fingerprint-purity check (E-T17/C-T8's pattern): supporting evidence
    # only, that build_forecast_record's extrema computation does not touch
    # what these fingerprints read -- NOT a proof of equivalence to any
    # pre-Item-F implementation. The literal predicted-high oracle below is
    # the real guard.
    before_input = input_fingerprint(conn, site_id, snapshot)
    before_basis = result_basis_fingerprint(
        conn, site_id, snapshot, period_start=period_start, period_end=period_end
    )

    simulate_snapshot_day(conn, cfg, local_date.isoformat())
    conn.commit()

    after_input = input_fingerprint(conn, site_id, snapshot)
    after_basis = result_basis_fingerprint(
        conn, site_id, snapshot, period_start=period_start, period_end=period_end
    )
    assert before_input == after_input
    assert before_basis == after_basis

    row = conn.execute(
        """
        SELECT * FROM verification_evidence
        WHERE run_id = ? AND snapshot_local_date = ? AND target_local_date = ?
          AND lead = 0 AND variable = 'temperature' AND quantity = 'temperature_high'
          AND entity_type = 'depth' AND entity_key = '1'
        """,
        (run_id, local_date.isoformat(), local_date.isoformat()),
    ).fetchone()
    assert row is not None
    # The clearing subset is just this one feed (its 20h clears the 18h
    # coverage guard), so displayed_daily's temperature body over its 20
    # values is max(values) for the high -- computed the same way
    # displayed_daily computes it, not transcribed as a bare literal.
    expected_high = displayed_daily("temperature", [values], rain_threshold_mm=0.2)[
        "high_c"
    ]
    assert expected_high == max(values)
    assert row["predicted"] == expected_high
    assert row["covered_hours"] == 20
    assert row["forecast_eligible"] == 1
    assert row["forecast_exclusion_reason"] is None


def test_input_fingerprint_unchanged_across_a_call_that_uses_extrema_logic() -> None:
    # E-T17's pattern (deliberate counterpart to C-T8): call
    # input_fingerprint on its own, run something that exercises Item F's
    # extrema logic (build_forecast_record, which computes CellCandidate.
    # extrema_eligible and runs select_cell_feeds(extrema_coverage_required
    # =True) internally), then call input_fingerprint again with the SAME
    # snapshot -- unchanged, because input_fingerprint/capture_config_
    # snapshot never read candidates, CellSelection or any extrema field
    # (verified by reading wxverify/verification/runs.py: the digest
    # covers only site config + roster + daily_truth/observations/sample
    # high-water, none of which build_forecast_record's extrema
    # computation writes to).
    conn = _record_conn()
    site_id = _record_site(conn, "site-a")
    feed_id = _record_feed(conn, "model-a")
    valid_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(24)
    ]
    _record_insert_day(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=valid_ats,
        value=10.0,
    )
    conn.commit()

    snapshot = capture_config_snapshot(conn, site_id)
    before = input_fingerprint(conn, site_id, snapshot)

    t = _record_snapshot_t()
    build_forecast_record(
        conn, site_id, _RECORD_DAY.isoformat(), now=t + timedelta(minutes=5)
    )

    after = input_fingerprint(conn, site_id, snapshot)
    assert before == after


# ===========================================================================
# F-T21 -- three-surface agreement.
#
# The tile uses the extrema set (temperature only); the simulator uses the
# clearing subset of the BLEND set. The two agree only when every candidate
# covers the whole day, so both fixtures below use a single feed -- with one
# feed, "the extrema set" and "the clearing subset of the blend set" name
# the same feed by construction, on both the fully-covered and the
# under-covered day.
# ===========================================================================


def test_three_surfaces_agree_on_a_fully_covered_day() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 14, 0, tzinfo=UTC)

    valid_ats = _hours("2026-07-20", 0, 24)
    values = [10.0 + i * 0.1 for i in range(24)]
    _seed_varying(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
        values=values,
    )
    conn.commit()

    # 1. Tile.
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[0]
    assert tile.temp.meta.extrema_unavailable is False

    # 2. Record.
    record_conn = _record_conn()
    site_id = _record_site(record_conn, "site-a", timezone="UTC")
    ensure_published_generation(record_conn, site_id)
    record_feed_id = _record_feed(record_conn, "model-a")
    record_valid_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(24)
    ]
    # Same varying values as the tile fixture, so the two are comparable.
    for i, valid in enumerate(record_valid_ats):
        record_conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'temperature', ?, ?, ?, ?, '{}', 'run-x', ?)
            """,
            (
                site_id,
                record_feed_id,
                "2035-06-15T06:00:00Z",
                isoformat_utc(valid),
                max(1, i),
                values[i],
                "2035-06-15T06:00:00Z",
            ),
        )
    t = _record_snapshot_t()
    build_forecast_record(
        record_conn, site_id, _RECORD_DAY.isoformat(), now=t + timedelta(minutes=5)
    )
    row = record_conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = ? AND variable = 'temperature' AND display_lead = 0
        """,
        (site_id,),
    ).fetchone()
    assert row is not None
    displayed = json.loads(str(row["daily_quantities"]))["displayed"]

    # 3. Simulator.
    sim_samples = [
        FutureSampleRow(
            feed_id=record_feed_id,
            source="example-src",
            model="model-a",
            variable="temperature",
            issued_at="2035-06-15T06:00:00Z",
            valid_at=isoformat_utc(valid),
            value=values[i],
        )
        for i, valid in enumerate(record_valid_ats)
    ]
    entities = dict(
        _entities_for_selection(
            entity_type="depth",
            entity_key="1",
            variable="temperature",
            feed_ids=[record_feed_id],
            feeds_samples={record_feed_id: sim_samples},
            timezone="UTC",
            target_date=_RECORD_DAY,
            rain_threshold_mm=0.2,
        )
    )

    expected_high = round(max(values), 10)
    expected_low = round(min(values), 10)
    assert tile.temp.high_c == expected_high
    assert tile.temp.low_c == expected_low
    assert displayed["high_c"] == expected_high
    assert displayed["low_c"] == expected_low
    assert entities[QUANTITY_TEMPERATURE_HIGH].predicted == expected_high
    assert entities[QUANTITY_TEMPERATURE_LOW].predicted == expected_low


def test_three_surfaces_report_unavailability_own_form_on_uncovered_day() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 14, 0, tzinfo=UTC)

    # 10 covered hours: under both the 18h clearing guard and the 24h
    # extrema requirement, so all three surfaces see genuine under-coverage.
    valid_ats = _hours("2026-07-20", 0, 10)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
        value=12.0,
    )
    conn.commit()

    # 1. Tile: suppresses.
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[0]
    assert tile.temp.high_c is None
    assert tile.temp.low_c is None
    assert tile.temp.meta.extrema_unavailable is True

    # 2. Record: nulls extrema + insufficient.
    record_conn = _record_conn()
    site_id = _record_site(record_conn, "site-a", timezone="UTC")
    ensure_published_generation(record_conn, site_id)
    record_feed_id = _record_feed(record_conn, "model-a")
    record_valid_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(10)
    ]
    for i, valid in enumerate(record_valid_ats):
        record_conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'temperature', ?, ?, ?, 12.0, '{}', 'run-x', ?)
            """,
            (
                site_id,
                record_feed_id,
                "2035-06-15T06:00:00Z",
                isoformat_utc(valid),
                max(1, i),
                "2035-06-15T06:00:00Z",
            ),
        )
    t = _record_snapshot_t()
    build_forecast_record(
        record_conn, site_id, _RECORD_DAY.isoformat(), now=t + timedelta(minutes=5)
    )
    row = record_conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = ? AND variable = 'temperature' AND display_lead = 0
        """,
        (site_id,),
    ).fetchone()
    assert row is not None
    displayed = json.loads(str(row["daily_quantities"]))["displayed"]
    assert displayed["high_c"] is None
    assert displayed["low_c"] is None
    assert displayed["extrema_coverage"] == EXTREMA_COVERAGE_INSUFFICIENT

    # 3. Simulator: its own recorded form -- an ineligible verdict with an
    # explicit exclusion reason, NOT None (QuantityOutcome retains value as
    # a labelled diagnostic even when ineligible).
    sim_samples = [
        FutureSampleRow(
            feed_id=record_feed_id,
            source="example-src",
            model="model-a",
            variable="temperature",
            issued_at="2035-06-15T06:00:00Z",
            valid_at=isoformat_utc(valid),
            value=12.0,
        )
        for valid in record_valid_ats
    ]
    entities = dict(
        _entities_for_selection(
            entity_type="depth",
            entity_key="1",
            variable="temperature",
            feed_ids=[record_feed_id],
            feeds_samples={record_feed_id: sim_samples},
            timezone="UTC",
            target_date=_RECORD_DAY,
            rain_threshold_mm=0.2,
        )
    )
    assert entities[QUANTITY_TEMPERATURE_HIGH].forecast_eligible is False
    assert (
        entities[QUANTITY_TEMPERATURE_HIGH].forecast_exclusion_reason
        == EXCLUDE_INSUFFICIENT_COVERAGE
    )
    # Its own form -- not None.
    assert entities[QUANTITY_TEMPERATURE_HIGH].predicted is not None


# ===========================================================================
# F-T22 -- F-T30: the two-feed shared-contributor set (F.11's fixture), the
# stale/confidence/rebuilding warning rules layered on top of the F.6
# extrema-coverage split.
# ===========================================================================

# Feed A: 20h coverage (blend-eligible, extrema-ineligible). Feed B: 24h
# coverage (extrema-eligible, and also blend-eligible since 24 >=
# MIN_SPREAD_HOURS). At the default blend_depth=1, A wins the blend set on
# skill; B is the ONLY extrema-eligible candidate so it wins the extrema
# set regardless of depth or skill ranking. Both feeds' issued_at falls on
# the SAME calendar day as their valid_ats, so both have representative
# day_ahead=0 unless a test overrides day_ahead explicitly (F-T30).
_AB_VALID_DAY = "2026-07-20"
# "now" stays on the SAME calendar day as the valid_ats above (so
# ``view.tiles[0]`` -- today, relative to "now" -- is this fixture's day) and
# late enough in that day that the stale issued_at below doesn't cross
# midnight into the day before (which would silently shift its
# representative day_ahead by one and break the day_ahead=0 lookups the
# tests below rely on).
_AB_NOW = datetime(2026, 7, 20, 23, 0, tzinfo=UTC)
_AB_FRESH_ISSUED = "2026-07-20T20:00:00Z"  # 3h before _AB_NOW -- fresh.
_AB_STALE_ISSUED = "2026-07-20T02:00:00Z"  # 21h before _AB_NOW -- stale.


def _seed_ab_fixture(
    conn: sqlite3.Connection,
    *,
    feed_a_issued_at: str = _AB_FRESH_ISSUED,
    feed_b_issued_at: str = _AB_FRESH_ISSUED,
    seed_b_scoring: bool = True,
) -> tuple[int, int]:
    """F.11's shared two-feed fixture. Returns (feed_a, feed_b).

    Both feeds' scoring pairs are seeded alongside matching persistence
    pairs at the SAME valid_ats -- ``ContinuousStrategy.aggregate``'s
    ``confident`` flag requires a non-None skill score, and
    ``_paired_skill`` only produces one when the persistence feed has
    pairs at the exact same ``valid_at`` (``wxverify/scoring/metrics.py``).
    Without a persistence counterpart, ``confident`` is False no matter how
    many pairs or how low ``min_n`` is set -- the trap this fixture avoids.
    """
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")

    a_ats = _hours(_AB_VALID_DAY, 0, 20)
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at=feed_a_issued_at,
        valid_ats=a_ats,
        values=[10.0 + i * 0.1 for i in range(20)],
    )
    b_ats = _hours(_AB_VALID_DAY, 0, 24)
    _seed_varying(
        conn,
        feed_id=feed_b,
        variable="temperature",
        issued_at=feed_b_issued_at,
        valid_ats=b_ats,
        values=[10.0 + i * 0.05 for i in range(24)],
    )

    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=0, forecast=8.0
    )
    # A's forecast is closer to the observed=10.0 baseline than B's -- A
    # wins the blend ladder on skill whenever both are confident, so tests
    # relying on "A is the blend winner" hold regardless of which of the
    # two feeds a reader might otherwise expect to rank higher.
    _seed_scoring_pairs(
        conn, feed_id=feed_a, variable="temperature", day_ahead=0, forecast=10.02
    )
    if seed_b_scoring:
        _seed_scoring_pairs(
            conn, feed_id=feed_b, variable="temperature", day_ahead=0, forecast=10.2
        )
    _seed_complete_score_cache(conn, variable="temperature", day_ahead=0)
    return feed_a, feed_b


def test_stale_extrema_contributor_alone_sets_the_stale_badge() -> None:
    # A (blend winner) fresh, B (extrema winner) stale -- the stale badge
    # must still fire because ``_cell_meta_and_values``'s ``stale_feeds``
    # for temperature is ``agg_feeds + selection.extrema_feeds``, not just
    # the rendered/blend set. Paired positive: both fresh proves the
    # negative isn't ambient (e.g. a fixture that always reads fresh).
    conn = _make_db()
    feed_a, feed_b = _seed_ab_fixture(
        conn, feed_a_issued_at=_AB_FRESH_ISSUED, feed_b_issued_at=_AB_STALE_ISSUED
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.stale is True
    html = _render_tiles(site_id=1, view=view)
    assert '<span class="badge warn">stale</span>' in html

    both_fresh = _make_db()
    _seed_ab_fixture(
        both_fresh, feed_a_issued_at=_AB_FRESH_ISSUED, feed_b_issued_at=_AB_FRESH_ISSUED
    )
    both_fresh.commit()
    fresh_view = build_forecast(
        both_fresh, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    assert fresh_view.tiles[0].temp.meta.stale is False
    fresh_html = _render_tiles(site_id=1, view=fresh_view)
    assert '<span class="badge warn">stale</span>' not in fresh_html


def test_stale_hourly_contributor_alone_still_sets_the_stale_badge() -> None:
    # The reverse split: A (blend/hourly winner) stale, B (extrema winner)
    # fresh. Same ``stale_feeds`` union means the badge fires from EITHER
    # side, not only the extrema side -- this is the paired positive/
    # negative pair for F-T22's own claim, isolating which half of the
    # union each half of that test exercised.
    conn = _make_db()
    _seed_ab_fixture(
        conn, feed_a_issued_at=_AB_STALE_ISSUED, feed_b_issued_at=_AB_FRESH_ISSUED
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    assert view.tiles[0].temp.meta.stale is True
    html = _render_tiles(site_id=1, view=view)
    assert '<span class="badge warn">stale</span>' in html


def test_confident_blend_set_with_unscored_extrema_set() -> None:
    # A (blend winner) confident; B (extrema winner) has NO scoring pairs
    # at all -- rung 3, low_confidence. The tile/cell state must read the
    # EXTREMA side's low_confidence, proving ``extrema_state`` (not the
    # blend-set-only ``state``) drives the "low confidence" badge here.
    conn = _make_db()
    feed_a, feed_b = _seed_ab_fixture(conn, seed_b_scoring=False)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    # The blend cell itself is confident/normal -- the badge below can only
    # be coming from the extrema side.
    assert today.temp.meta.state == "normal"
    assert today.temp.meta.extrema_state == "low_confidence"
    assert today.state == "normal"
    assert today.confidence_state == "low_confidence"
    html = _render_tiles(site_id=1, view=view)
    assert '<article class="tile state-normal">' in html
    assert '<span class="badge warn">low confidence</span>' in html
    assert '<span class="badge muted">ranking updating</span>' not in html

    # Record side: same shape, using knowable-at-T pairs (F.11's fixture
    # trap -- the far-future pairs above are invisible to the as-of read).
    record_conn = _record_conn()
    site_id = _record_site(record_conn, "site-a", timezone="UTC")
    ensure_published_generation(record_conn, site_id)
    set_setting(record_conn, "min_n", "3")
    rec_a = _record_feed(record_conn, "model-a")
    rec_b = _record_feed(record_conn, "model-b")
    full_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(24)
    ]
    partial_ats = full_ats[:20]
    _record_insert_day(
        record_conn,
        site_id=site_id,
        feed_id=rec_a,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=partial_ats,
        value=10.0,
    )
    _record_insert_day(
        record_conn,
        site_id=site_id,
        feed_id=rec_b,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=full_ats,
        value=10.05,
    )
    _seed_record_confident(record_conn, site_id=site_id, feed_id=rec_a)
    # rec_b gets NO scoring pairs at all -- rung 3.
    t = _record_snapshot_t()
    build_forecast_record(
        record_conn, site_id, _RECORD_DAY.isoformat(), now=t + timedelta(minutes=5)
    )
    row = record_conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = ? AND variable = 'temperature' AND display_lead = 0
        """,
        (site_id,),
    ).fetchone()
    assert row is not None
    displayed = json.loads(str(row["daily_quantities"]))["displayed"]
    assert displayed["extrema_feed_ids"] == [rec_b]
    assert displayed["extrema_low_confidence"] is True
    assert displayed["low_confidence"] is False


def test_suppressed_extrema_contribute_to_neither_warning() -> None:
    # Neither feed covers the whole local day -- extrema_coverage stays
    # INSUFFICIENT, extrema_feeds is empty, and extrema_state is
    # "not_available" (F.6's precedence: not_available short-circuits
    # before low_confidence/rebuilding is even considered). The blend set
    # (A alone, confident) must still drive the tile normally -- suppressed
    # extrema contribute NEITHER a stale NOR a low-confidence/rebuilding
    # signal on their own.
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")
    a_ats = _hours(_AB_VALID_DAY, 0, 20)
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at=_AB_FRESH_ISSUED,
        valid_ats=a_ats,
        values=[10.0 + i * 0.1 for i in range(20)],
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=0, forecast=8.0
    )
    _seed_scoring_pairs(
        conn, feed_id=feed_a, variable="temperature", day_ahead=0, forecast=10.1
    )
    _seed_complete_score_cache(conn, variable="temperature", day_ahead=0)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_unavailable is True
    assert today.temp.meta.extrema_state == "not_available"
    assert today.temp.meta.state == "normal"
    assert today.temp.meta.stale is False
    assert today.confidence_state == "normal"
    html = _render_tiles(site_id=1, view=view)
    assert '<span class="badge warn">low confidence</span>' not in html
    assert '<span class="badge muted">ranking updating</span>' not in html
    assert '<span class="badge warn">stale</span>' not in html

    # Record side: extrema_coverage stays INSUFFICIENT, extrema_low_confidence
    # is None (never resolved -- F.7's guard), high/low suppressed.
    record_conn = _record_conn()
    site_id = _record_site(record_conn, "site-a", timezone="UTC")
    ensure_published_generation(record_conn, site_id)
    set_setting(record_conn, "min_n", "3")
    rec_a = _record_feed(record_conn, "model-a")
    partial_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(20)
    ]
    _record_insert_day(
        record_conn,
        site_id=site_id,
        feed_id=rec_a,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=partial_ats,
        value=10.0,
    )
    _seed_record_confident(record_conn, site_id=site_id, feed_id=rec_a)
    t = _record_snapshot_t()
    build_forecast_record(
        record_conn, site_id, _RECORD_DAY.isoformat(), now=t + timedelta(minutes=5)
    )
    row = record_conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = ? AND variable = 'temperature' AND display_lead = 0
        """,
        (site_id,),
    ).fetchone()
    assert row is not None
    displayed = json.loads(str(row["daily_quantities"]))["displayed"]
    assert displayed["extrema_feed_ids"] == []
    assert displayed["extrema_coverage"] == EXTREMA_COVERAGE_INSUFFICIENT
    assert displayed["extrema_low_confidence"] is None
    assert displayed["high_c"] is None
    assert displayed["low_c"] is None


def test_stale_blend_set_alone_still_sets_the_stale_badge_with_extrema_suppressed() -> (
    None
):
    # Positive pair to the test above: same suppressed-extrema shape, but
    # the blend feed's issued_at is stale -- the stale badge is driven by
    # the blend set on its own, still independent of the suppressed
    # extrema side.
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")
    a_ats = _hours(_AB_VALID_DAY, 0, 20)
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at=_AB_STALE_ISSUED,
        valid_ats=a_ats,
        values=[10.0 + i * 0.1 for i in range(20)],
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=0, forecast=8.0
    )
    _seed_scoring_pairs(
        conn, feed_id=feed_a, variable="temperature", day_ahead=0, forecast=10.1
    )
    _seed_complete_score_cache(conn, variable="temperature", day_ahead=0)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_unavailable is True
    assert today.temp.meta.extrema_state == "not_available"
    assert today.temp.meta.state == "normal"
    assert today.temp.meta.stale is True
    html = _render_tiles(site_id=1, view=view)
    assert '<span class="badge warn">stale</span>' in html


def test_shared_contributor_set_confidence_states_are_independent_per_side() -> None:
    # F-T26: sweep the three REACHABLE (blend-side, extrema-side) confidence
    # combinations across the SAME two-feed fixture shape, proving the two
    # sides are read independently rather than one leaking into the other.
    # "blend low-confidence, extrema confident" is unreachable: eligible
    # (extrema) candidates are a subset of the >=12h blend coverage pool, so
    # whatever lowers the blend-side ranking's confidence lowers the
    # extrema-side ranking's confidence too whenever they share candidates.
    # "low confidence" and "ranking updating" are mutually exclusive badge
    # states (F.6's ``_state_of`` precedence), so each case pins exactly
    # one outcome and its absence. The plan's shared-CONTRIBUTOR-SET half
    # (extrema set == blend set, not just both confidence-linked) is
    # exercised separately below, in
    # ``test_shared_contributor_set_states_roll_up_identically_to_today``;
    # its stale+rebuilding-beside-low-confidence-wind case is F-T28(5).

    # Case 1: both confident -- normal/normal, no badges.
    conn = _make_db()
    _seed_ab_fixture(conn)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "normal"
    assert today.temp.meta.extrema_state == "normal"
    html = _render_tiles(site_id=1, view=view)
    assert '<span class="badge warn">low confidence</span>' not in html
    assert '<span class="badge muted">ranking updating</span>' not in html

    # Case 2: both low (min_n raised globally, so it lowers BOTH the
    # blend-side and the extrema-side ranking's confidence at once -- there
    # is no per-side min_n).
    conn = _make_db()
    _seed_ab_fixture(conn)
    set_setting(conn, "min_n", "10")
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "low_confidence"
    assert today.temp.meta.extrema_state == "low_confidence"  # min_n raised globally
    html = _render_tiles(site_id=1, view=view)
    assert '<span class="badge warn">low confidence</span>' in html
    assert '<span class="badge muted">ranking updating</span>' not in html

    # Case 3: extrema alone rebuilding (B has pairs but no score_cache row
    # at its cell), blend side (A) untouched and confident. A rebuilding
    # temperature side beside an otherwise-normal blend must still surface
    # "ranking updating", not silently pass as normal -- and must NOT ALSO
    # claim "low confidence" (mutually exclusive per _state_of).
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at=_AB_FRESH_ISSUED,
        valid_ats=_hours(_AB_VALID_DAY, 0, 20),
        values=[10.0 + i * 0.1 for i in range(20)],
    )
    # B is issued the PRIOR calendar day -- a DIFFERENT representative
    # day_ahead (1) from A's (0) -- so B's incomplete cache snapshot below
    # cannot also blank out A's own, separately-cached, ranking cell.
    # ``leaderboard_with_status``'s verdict is scoped to a single (variable,
    # day_ahead) cache snapshot: sharing A's day_ahead=0 here would make
    # B's missing row corrupt A's read too, an artifact of this fixture
    # rather than the rollup this case means to isolate.
    _seed_varying(
        conn,
        feed_id=feed_b,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours(_AB_VALID_DAY, 0, 24),
        values=[10.0 + i * 0.05 for i in range(24)],
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=0, forecast=8.0
    )
    _seed_scoring_pairs(
        conn, feed_id=feed_a, variable="temperature", day_ahead=0, forecast=10.02
    )
    _seed_complete_score_cache(conn, variable="temperature", day_ahead=0)
    # B has pairs at its OWN cell (day_ahead=1) but no score_cache row
    # there -- genuinely "rebuilding", never cached in the first place.
    _seed_scoring_pairs(
        conn, feed_id=feed_b, variable="temperature", day_ahead=1, forecast=9.0
    )
    conn.commit()
    assert (
        leaderboard_with_status(
            conn, site_id=1, variable="temperature", day_ahead=0, window="rolling"
        ).status
        == "hit"
    )
    assert (
        leaderboard_with_status(
            conn, site_id=1, variable="temperature", day_ahead=1, window="rolling"
        ).status
        == "rebuilding"
    )
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "normal"  # blend set (A) untouched
    assert today.temp.meta.extrema_state == "rebuilding"
    html = _render_tiles(site_id=1, view=view)
    assert '<span class="badge muted">ranking updating</span>' in html
    assert '<span class="badge warn">low confidence</span>' not in html


def _seed_shared_ab_fixture(
    conn: sqlite3.Connection,
    *,
    feed_a_issued_at: str = _AB_FRESH_ISSUED,
    feed_b_issued_at: str = _AB_FRESH_ISSUED,
    complete_cache: bool = True,
) -> tuple[int, int]:
    """The plan's SHARED-contributor-set fixture for F-T26's second half.

    Both feeds cover the full 24h local day, so ``extrema_feeds`` is not
    just non-empty but IS the blend set's clearing subset: both rankings
    are computed over the identical candidate pool, and the two sides must
    therefore read identically in every confidence state.
    """
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")
    ats = _hours(_AB_VALID_DAY, 0, 24)
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at=feed_a_issued_at,
        valid_ats=ats,
        values=[10.0 + i * 0.1 for i in range(24)],
    )
    _seed_varying(
        conn,
        feed_id=feed_b,
        variable="temperature",
        issued_at=feed_b_issued_at,
        valid_ats=ats,
        values=[10.0 + i * 0.05 for i in range(24)],
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=0, forecast=8.0
    )
    _seed_scoring_pairs(
        conn, feed_id=feed_a, variable="temperature", day_ahead=0, forecast=10.02
    )
    _seed_scoring_pairs(
        conn, feed_id=feed_b, variable="temperature", day_ahead=0, forecast=10.2
    )
    if complete_cache:
        _seed_complete_score_cache(conn, variable="temperature", day_ahead=0)
    return feed_a, feed_b


def test_shared_contributor_set_states_roll_up_identically_to_today() -> None:
    # M2 remediation: the plan's shared-CONTRIBUTOR-SET half of F-T26 --
    # every feed covers the day so ``extrema_feeds`` equals the blend set's
    # clearing subset exactly. Swept over fresh, stale, low-confidence and
    # low-confidence-plus-rebuilding: in every state the two sides read
    # identically, because they ARE the same ranking over the same
    # candidates. The plan's stale-rebuilding-beside-a-low-confidence-wind-
    # cell case is F-T28(5), not repeated here.

    # Fresh, both confident.
    conn = _make_db()
    _seed_shared_ab_fixture(conn)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_state == today.temp.meta.state == "normal"
    assert today.confidence_state == today.state == "normal"
    html = _render_tiles(site_id=1, view=view)
    assert '<article class="tile state-normal">' in html
    assert '<span class="badge warn">low confidence</span>' not in html
    assert '<span class="badge muted">ranking updating</span>' not in html
    assert '<span class="badge warn">stale</span>' not in html

    # Stale: both feeds issued stale, so both the blend and extrema sides
    # (and hence their union) see it, keeping the shared-set property.
    conn = _make_db()
    _seed_shared_ab_fixture(
        conn, feed_a_issued_at=_AB_STALE_ISSUED, feed_b_issued_at=_AB_STALE_ISSUED
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_state == today.temp.meta.state == "normal"
    assert today.confidence_state == today.state == "normal"
    assert today.temp.meta.stale is True
    html = _render_tiles(site_id=1, view=view)
    assert '<article class="tile state-normal">' in html
    assert '<span class="badge warn">stale</span>' in html
    assert '<span class="badge warn">low confidence</span>' not in html
    assert '<span class="badge muted">ranking updating</span>' not in html

    # Low-confidence: min_n raised globally -- both sides share the same
    # ranking, so both drop below it together.
    conn = _make_db()
    _seed_shared_ab_fixture(conn)
    set_setting(conn, "min_n", "10")
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_state == today.temp.meta.state == "low_confidence"
    assert today.confidence_state == today.state == "low_confidence"
    html = _render_tiles(site_id=1, view=view)
    assert '<article class="tile state-low_confidence">' in html
    assert '<span class="badge warn">low confidence</span>' in html
    assert '<span class="badge muted">ranking updating</span>' not in html

    # Low-confidence plus rebuilding: pairs exist for both feeds at the
    # SAME day_ahead but the score_cache row is never completed -- genuinely
    # rebuilding, and shared across both the blend and extrema rankings.
    conn = _make_db()
    _seed_shared_ab_fixture(conn, complete_cache=False)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_state == today.temp.meta.state == "rebuilding"
    assert today.confidence_state == today.state == "rebuilding"
    html = _render_tiles(site_id=1, view=view)
    assert '<article class="tile state-rebuilding">' in html
    assert '<span class="badge muted">ranking updating</span>' in html
    assert '<span class="badge warn">low confidence</span>' not in html


def test_wind_and_precip_warnings_unchanged_while_temperature_extrema_differs() -> None:
    # F-T27: wind and precip never have an extrema set (``extrema_state``
    # is always "not_available" for them per ``_cell_meta_and_values``), so
    # their own state/stale/badge reads must be driven purely by their
    # normal aggregate set, unaffected by the temperature extrema-side
    # low-confidence signal this fixture also carries.
    conn = _make_db()
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    feed_a, feed_b = _seed_ab_fixture(conn, seed_b_scoring=False)
    wind_ats = _hours(_AB_VALID_DAY, 0, 20)
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="wind",
        issued_at=_AB_FRESH_ISSUED,
        valid_ats=wind_ats,
        value=5.0,
    )
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="precip",
        issued_at=_AB_FRESH_ISSUED,
        valid_ats=wind_ats,
        value=1.0,
    )
    # Give wind ITS OWN confident ranking (own scoring pairs + cache, same
    # shape as temperature's) -- so its "normal" read below is a genuine
    # positive, not just the default absence of any wind scoring pairs
    # (which would read low_confidence regardless of temperature and prove
    # nothing about independence).
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="wind", day_ahead=0, forecast=3.0
    )
    _seed_scoring_pairs(
        conn, feed_id=feed_a, variable="wind", day_ahead=0, forecast=5.02
    )
    _seed_complete_score_cache(conn, variable="wind", day_ahead=0)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    # Temperature's extrema side IS low-confidence (paired control: proves
    # the fixture actually exercises the case wind/precip must stay
    # immune to).
    assert today.temp.meta.extrema_state == "low_confidence"
    assert today.wind.meta.extrema_state == "not_available"
    assert today.wind.meta.extrema_unavailable is False
    assert today.wind.meta.state == "normal"
    # Precip gets no scoring pairs of its own -- it stays low_confidence
    # for ITS OWN reason (rung 3, no scored pairs at all), a DIFFERENT
    # outcome from wind's "normal" and temperature's "low_confidence" via
    # the extrema side -- three independently-arrived-at states from the
    # same shared fixture, none inherited from another.
    assert today.precip.meta.extrema_state == "not_available"
    assert today.precip.meta.extrema_unavailable is False
    assert today.precip.meta.state == "low_confidence"


def test_wind_and_precip_stale_badge_unaffected_by_temperature_extrema_state() -> None:
    # L4: paired stale variant of the test above -- wind and precip's own
    # ``stale`` reads come from their own feeds' freshness, independent of
    # temperature's extrema-side state carried by the same shared fixture.
    # ``load_feed_freshness`` judges staleness PER FEED (MAX(issued_at)
    # across ALL that feed's variables), so wind/precip use a feed
    # EXCLUSIVE to them (icon_global) rather than feed_a, whose fresh
    # temperature issued_at would otherwise mask a stale wind/precip one.
    conn = _make_db()
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    _seed_ab_fixture(conn, seed_b_scoring=False)
    wind_feed = _feed_id(conn, "open-meteo", "icon_global")
    wind_ats = _hours(_AB_VALID_DAY, 0, 20)
    _seed_hourly(
        conn,
        feed_id=wind_feed,
        variable="wind",
        issued_at=_AB_STALE_ISSUED,
        valid_ats=wind_ats,
        value=5.0,
    )
    _seed_hourly(
        conn,
        feed_id=wind_feed,
        variable="precip",
        issued_at=_AB_STALE_ISSUED,
        valid_ats=wind_ats,
        value=1.0,
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="wind", day_ahead=0, forecast=3.0
    )
    _seed_scoring_pairs(
        conn, feed_id=wind_feed, variable="wind", day_ahead=0, forecast=5.02
    )
    _seed_complete_score_cache(conn, variable="wind", day_ahead=0)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    # Paired control: temperature's extrema side is still low-confidence,
    # proving this is the same shared fixture as the test above.
    assert today.temp.meta.extrema_state == "low_confidence"
    assert today.wind.meta.stale is True
    assert today.wind.meta.extrema_unavailable is False
    assert today.precip.meta.stale is True
    assert today.precip.meta.extrema_unavailable is False
    html = _render_tiles(site_id=1, view=view)
    assert '<span class="badge warn">stale</span>' in html


def _seed_wind_confident(
    conn: sqlite3.Connection, *, feed_id: int, issued_at: str, persistence_id: int
) -> None:
    """Wind's own confident ranking (F-T27's shape): a genuine "normal"
    read, not the rung-3 low_confidence default absent any scoring pairs.
    """
    wind_ats = _hours(_AB_VALID_DAY, 0, 20)
    _seed_hourly(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at=issued_at,
        valid_ats=wind_ats,
        value=5.0,
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="wind", day_ahead=0, forecast=3.0
    )
    _seed_scoring_pairs(
        conn, feed_id=feed_id, variable="wind", day_ahead=0, forecast=5.02
    )
    _seed_complete_score_cache(conn, variable="wind", day_ahead=0)


def test_tile_level_rollup_across_variables() -> None:
    # F-T28: ``DayTile.stale`` and ``DayTile.confidence_state`` roll up
    # across cells (temperature, wind, precip), not just within a single
    # cell's own blend/extrema split -- five independent sub-fixtures, each
    # isolating one source of a tile-level signal.

    # (1) Only temperature's extrema contributor (B) is stale -- the same
    # per-cell signal F-T22 pins, checked here at the TILE level.
    conn = _make_db()
    feed_a, _feed_b = _seed_ab_fixture(
        conn, feed_a_issued_at=_AB_FRESH_ISSUED, feed_b_issued_at=_AB_STALE_ISSUED
    )
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    _seed_wind_confident(
        conn, feed_id=feed_a, issued_at=_AB_FRESH_ISSUED, persistence_id=persistence_id
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    assert view.tiles[0].stale is True

    # (2) Only wind's own clearing subset is stale -- temperature fully
    # fresh, proving the tile-level ``stale`` union reaches wind's cell too,
    # not only temperature's. Wind uses a THIRD feed (never touched by
    # temperature) rather than feed_a: ``load_feed_freshness`` judges
    # staleness per FEED as the MAX ``issued_at`` across ALL that feed's
    # variables (``wxverify/forecast/data.py``), so a stale wind sample on
    # feed_a would be masked by that same feed's fresh temperature sample
    # -- the trap this isolation avoids.
    conn = _make_db()
    _feed_a, _feed_b = _seed_ab_fixture(conn)
    wind_feed = _feed_id(conn, "open-meteo", "icon_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    _seed_wind_confident(
        conn,
        feed_id=wind_feed,
        issued_at=_AB_STALE_ISSUED,
        persistence_id=persistence_id,
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.stale is False  # isolates which cell carries it
    assert today.stale is True

    # (3) Only a low-confidence WIND cell -- no scoring pairs at all for
    # wind (rung 3) beside a fully confident temperature. Because
    # ``DayTile.state`` rolls up cells' plain ``state`` (not ``extrema_state``),
    # a low-confidence BLEND-side cell (unlike case (4) below) moves both
    # ``state`` and ``confidence_state`` together.
    conn = _make_db()
    feed_a, _feed_b = _seed_ab_fixture(conn)
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="wind",
        issued_at=_AB_FRESH_ISSUED,
        valid_ats=_hours(_AB_VALID_DAY, 0, 20),
        value=5.0,
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.wind.meta.state == "low_confidence"
    assert today.state == "low_confidence"
    assert today.confidence_state == "low_confidence"
    html = _render_tiles(site_id=1, view=view)
    assert '<article class="tile state-low_confidence">' in html
    assert '<span class="badge warn">low confidence</span>' in html

    # (4) Only temperature's EXTREMA set is low-confidence (blend side
    # confident, wind confident) -- ``state`` (CSS class) stays "normal"
    # because it never looks at ``extrema_state``, while ``confidence_state``
    # (badges) picks it up, the pairing F.6 requires kept independent.
    conn = _make_db()
    feed_a, _feed_b = _seed_ab_fixture(conn, seed_b_scoring=False)
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    _seed_wind_confident(
        conn, feed_id=feed_a, issued_at=_AB_FRESH_ISSUED, persistence_id=persistence_id
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "normal"
    assert today.temp.meta.extrema_state == "low_confidence"
    assert today.wind.meta.state == "normal"
    assert today.state == "normal"
    assert today.confidence_state == "low_confidence"
    html = _render_tiles(site_id=1, view=view)
    assert '<article class="tile state-normal">' in html
    assert '<span class="badge warn">low confidence</span>' in html

    # (5) Temperature's extrema set is rebuilding (F-T30's construction)
    # BESIDE a low-confidence wind cell -- the rollup's precedence (low
    # confidence outranks rebuilding) must resolve ACROSS cells, not only
    # within one cell's own blend/extrema pair: the badge row shows "low
    # confidence" alone, never "ranking updating" beside it.
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")
    a_ats = _hours(_AB_VALID_DAY, 0, 20)
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at=_AB_FRESH_ISSUED,
        valid_ats=a_ats,
        values=[10.0 + i * 0.1 for i in range(20)],
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=0, forecast=8.0
    )
    _seed_scoring_pairs(
        conn, feed_id=feed_a, variable="temperature", day_ahead=0, forecast=10.02
    )
    _seed_complete_score_cache(conn, variable="temperature", day_ahead=0)
    # B: different representative day_ahead (issued the prior calendar
    # day) from A's, with pairs but no score_cache row at its own cell --
    # genuinely "rebuilding" (F-T30's construction, not repeated here).
    _seed_varying(
        conn,
        feed_id=feed_b,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours(_AB_VALID_DAY, 0, 24),
        values=[10.0 + i * 0.05 for i in range(24)],
    )
    _seed_scoring_pairs(
        conn, feed_id=feed_b, variable="temperature", day_ahead=1, forecast=9.0
    )
    # Wind: no scoring pairs at all -- low-confidence (rung 3).
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="wind",
        issued_at=_AB_FRESH_ISSUED,
        valid_ats=a_ats,
        value=5.0,
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_state == "rebuilding"
    assert today.wind.meta.state == "low_confidence"
    assert today.confidence_state == "low_confidence"
    html = _render_tiles(site_id=1, view=view)
    assert '<span class="badge warn">low confidence</span>' in html
    assert '<span class="badge muted">ranking updating</span>' not in html

    # (6) None of the above -- both temperature and wind fully confident
    # and fresh, precip left unpopulated (``not_available``, ignored by
    # both rollups): no badge row renders at all. Paired negative for every
    # positive badge assertion above -- proves the badge row isn't ambiently
    # present regardless of state.
    conn = _make_db()
    feed_a, _feed_b = _seed_ab_fixture(conn)
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    _seed_wind_confident(
        conn, feed_id=feed_a, issued_at=_AB_FRESH_ISSUED, persistence_id=persistence_id
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.stale is False
    assert today.confidence_state == "normal"
    html = _render_tiles(site_id=1, view=view)
    assert "tile-badges" not in html


_KNOWABLE_VALID_ATS = (
    "2035-06-14T10:00:00Z",
    "2035-06-14T11:00:00Z",
    "2035-06-14T12:00:00Z",
)
_KNOWABLE_FIRST_KNOWN_AT = "2035-06-14T13:00:00Z"


def _knowable_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    valid_at: str,
    first_known_at: str,
    forecast: float,
    day_ahead: int,
    variable: str = "temperature",
    observed: float = 10.0,
) -> None:
    """A ``forecast_pairs`` row genuinely knowable at the record's T
    (``knowable_pair_predicate``, ``wxverify/verification/asof.py``): both
    ``first_known_at`` and ``valid_at + CONSENSUS_LAG_HOURS`` are
    comfortably before T. Seeded a day BEFORE ``_RECORD_DAY`` -- unlike the
    page-side ``_seed_scoring_pairs``' far-future 2035-07-01 literals, which
    ``knowable_pair_predicate`` excludes (F.11's fixture trap: the record's
    as-of ranking, unlike the page's live ``score_cache`` read, never sees
    those).
    """
    error = forecast - observed
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             first_known_at, tz_generation_id)
        VALUES (?, ?, ?, ?, ?, 6, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            variable,
            first_known_at,
            valid_at,
            day_ahead,
            forecast,
            observed,
            error,
            abs(error),
            error * error,
            first_known_at,
            ensure_published_generation(conn, site_id),
        ),
    )


def _seed_record_confident(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    day_ahead: int = 0,
    seed_persistence: bool = True,
) -> None:
    """3 pairs (== the declared min_n=3) knowable at T for ``feed_id`` AND
    the persistence baseline at the SAME ``valid_at``s, so the record's
    as-of ranking (``knowable_pair_predicate``-gated) reads it confident.

    ``seed_persistence=False`` skips the persistence half: the persistence
    feed has only one row per (site, valid_at, issued_at) -- a second call
    against the same ``day_ahead`` for a different ``feed_id`` (as F-T29
    does, to make two feeds confident in the same fixture) must not
    re-insert it, or it collides on ``forecast_pairs``' UNIQUE constraint.
    """
    persistence_id = asof_persistence_feed_id(conn)
    targets = [(feed_id, 10.2)]
    if seed_persistence:
        targets.append((persistence_id, 8.0))
    for target_id, forecast in targets:
        for valid_at in _KNOWABLE_VALID_ATS:
            _knowable_pair(
                conn,
                site_id=site_id,
                feed_id=target_id,
                valid_at=valid_at,
                first_known_at=_KNOWABLE_FIRST_KNOWN_AT,
                forecast=forecast,
                day_ahead=day_ahead,
            )


def test_badge_tooltip_and_persisted_metadata_agree_on_confidence() -> None:
    # F-T29: run the same fixture shape twice (extrema side confident, then
    # low-confidence) and check that, WITHIN EACH RUN, the rendered badge,
    # the "Feeds:" tooltip naming the extrema set, and the persisted
    # ``extrema_low_confidence`` flag all agree with the single confidence
    # knob that produced them.
    for extrema_confident in (True, False):
        # Page side: badge + tooltip.
        conn = _make_db()
        _, feed_b = _seed_ab_fixture(conn, seed_b_scoring=extrema_confident)
        conn.commit()
        view = build_forecast(
            conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
        )
        today = view.tiles[0]
        assert today.temp.meta.extrema_state != "rebuilding"  # guard: not masked
        labels = {ref.label for ref in today.temp.meta.feeds}
        assert labels == {feed_label("open-meteo", "gfs_global")}
        html = _render_tiles(site_id=1, view=view)
        assert f'title="Feeds: {feed_label("open-meteo", "gfs_global")}"' in html
        has_badge = '<span class="badge warn">low confidence</span>' in html
        assert has_badge is (not extrema_confident)

        # Record side: same shape, persisted flag.
        record_conn = _record_conn()
        site_id = _record_site(record_conn, "site-a", timezone="UTC")
        ensure_published_generation(record_conn, site_id)
        set_setting(record_conn, "min_n", "3")
        rec_a = _record_feed(record_conn, "model-a")
        rec_b = _record_feed(record_conn, "model-b")
        full_ats = [
            datetime(
                _RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC
            )
            for h in range(24)
        ]
        partial_ats = full_ats[:20]
        _record_insert_day(
            record_conn,
            site_id=site_id,
            feed_id=rec_a,
            local_date=_RECORD_DAY,
            issued_at="2035-06-15T06:00:00Z",
            valid_ats=partial_ats,
            value=10.0,
        )
        _record_insert_day(
            record_conn,
            site_id=site_id,
            feed_id=rec_b,
            local_date=_RECORD_DAY,
            issued_at="2035-06-15T06:00:00Z",
            valid_ats=full_ats,
            value=10.05,
        )
        _seed_record_confident(record_conn, site_id=site_id, feed_id=rec_a)
        if extrema_confident:
            _seed_record_confident(
                record_conn, site_id=site_id, feed_id=rec_b, seed_persistence=False
            )
        t = _record_snapshot_t()
        build_forecast_record(
            record_conn, site_id, _RECORD_DAY.isoformat(), now=t + timedelta(minutes=5)
        )
        row = record_conn.execute(
            """
            SELECT * FROM forecast_of_record
            WHERE site_id = ? AND variable = 'temperature' AND display_lead = 0
            """,
            (site_id,),
        ).fetchone()
        assert row is not None
        displayed = json.loads(str(row["daily_quantities"]))["displayed"]
        assert displayed["extrema_feed_ids"] == [rec_b]
        assert (displayed["extrema_low_confidence"] is False) is extrema_confident
        assert (displayed["extrema_low_confidence"] is True) is (not extrema_confident)


def test_rebuilding_precedence_on_the_extrema_set() -> None:
    # F-T30: extrema set (B) low-confidence AND rebuilding -- _state_of's
    # precedence collapses that combination to "rebuilding", not "low
    # confidence". Blend set (A) stays confident/not-rebuilding throughout,
    # so the tile's own CSS-driving ``state`` stays "normal" while the
    # rollup ``confidence_state`` reads "rebuilding" -- proving the two
    # fields are computed independently (F.6).
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")

    # A: rep day_ahead=0 (issued the same calendar day as its valid_ats),
    # 20h coverage, confident with a complete score_cache row -- the blend
    # winner.
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at="2026-07-20T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 20),
        values=[10.0 + i * 0.1 for i in range(20)],
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=0, forecast=8.0
    )
    _seed_scoring_pairs(
        conn, feed_id=feed_a, variable="temperature", day_ahead=0, forecast=10.1
    )
    _seed_complete_score_cache(conn, variable="temperature", day_ahead=0)

    # B: rep day_ahead=1 (issued the PRIOR calendar day) -- a DIFFERENT
    # ranking cell from A's -- covers the whole local day (extrema-
    # eligible); has scoring pairs at day_ahead=1 but no score_cache row
    # there, so its own leaderboard read is genuinely "rebuilding".
    _seed_varying(
        conn,
        feed_id=feed_b,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
        values=[10.0 + i * 0.05 for i in range(24)],
    )
    # No persistence pairs seeded for B's cell: `leaderboard_with_status`'s
    # "rebuilding" verdict (unlike `confident`) needs only B's OWN
    # forecast_pairs and an absent score_cache row -- mirroring
    # test_forecast_service.py's O8
    # (`test_coverage_guard_splits_tile_and_drill_down_rebuilding_state`),
    # which establishes "rebuilding" the same way. A second persistence
    # insert at the same far-future valid_ats as A's day_ahead=0 pairs
    # would collide on `forecast_pairs`' UNIQUE constraint anyway (it does
    # not include day_ahead).
    _seed_scoring_pairs(
        conn, feed_id=feed_b, variable="temperature", day_ahead=1, forecast=9.0
    )
    conn.commit()

    assert (
        leaderboard_with_status(
            conn, site_id=1, variable="temperature", day_ahead=0, window="rolling"
        ).status
        == "hit"
    )
    # Guard: without this, a fixture that quietly left B "empty" instead of
    # genuinely "rebuilding" would pass the state assertions vacuously.
    assert (
        leaderboard_with_status(
            conn, site_id=1, variable="temperature", day_ahead=1, window="rolling"
        ).status
        == "rebuilding"
    )

    # Same calendar day as both feeds' valid_ats, so ``view.tiles[0]`` is
    # this fixture's day.
    now = _AB_NOW
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "normal"
    assert today.temp.meta.extrema_state == "rebuilding"
    assert today.state == "normal"
    assert today.confidence_state == "rebuilding"
    html = _render_tiles(site_id=1, view=view)
    assert '<article class="tile state-normal">' in html
    assert '<span class="badge muted">ranking updating</span>' in html
    assert '<span class="badge warn">low confidence</span>' not in html

    # Record side: B is genuinely SCORED (one knowable pair) but below the
    # declared min_n=3 -- unconfident without being simply absent.
    record_conn = _record_conn()
    site_id = _record_site(record_conn, "site-a", timezone="UTC")
    ensure_published_generation(record_conn, site_id)
    set_setting(record_conn, "min_n", "3")
    rec_a = _record_feed(record_conn, "model-a")
    rec_b = _record_feed(record_conn, "model-b")
    full_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(24)
    ]
    partial_ats = full_ats[:20]
    _record_insert_day(
        record_conn,
        site_id=site_id,
        feed_id=rec_a,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=partial_ats,
        value=10.0,
    )
    _record_insert_day(
        record_conn,
        site_id=site_id,
        feed_id=rec_b,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=full_ats,
        value=10.05,
    )
    _seed_record_confident(record_conn, site_id=site_id, feed_id=rec_a)
    _knowable_pair(
        record_conn,
        site_id=site_id,
        feed_id=rec_b,
        valid_at=_KNOWABLE_VALID_ATS[0],
        first_known_at=_KNOWABLE_FIRST_KNOWN_AT,
        forecast=9.0,
        day_ahead=0,
    )
    t = _record_snapshot_t()
    build_forecast_record(
        record_conn, site_id, _RECORD_DAY.isoformat(), now=t + timedelta(minutes=5)
    )
    row = record_conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = ? AND variable = 'temperature' AND display_lead = 0
        """,
        (site_id,),
    ).fetchone()
    assert row is not None
    displayed = json.loads(str(row["daily_quantities"]))["displayed"]
    assert displayed["extrema_low_confidence"] is True


# ===========================================================================
# F-T31 -- Australia/Lord_Howe 2026-04-05: a half-hour DST shift puts 25
# on-the-hour UTC instants in a 24.5-hour window, so the floored slot count
# (24, from ``local_day_slots``) undercounts the actual required set (25).
# Before the fix, ``covers_local_day`` compared ``len(in_window_hours)`` to
# that floored count, so it rejected a complete 25-hour feed, accepted any
# 24-hour feed missing an interior or boundary hour, and let a sample just
# outside the window stand in for a missing boundary hour. The fixed
# function compares the in-window hour set to the explicitly enumerated
# required set instead.
# ===========================================================================

_LORD_HOWE_TRANSITION_START = datetime(2026, 4, 4, 13, tzinfo=UTC)  # local midnight UTC
_LORD_HOWE_TRANSITION_DATE = date(2026, 4, 5)


def test_lord_howe_dst_shift_needs_all_25_utc_hour_instants() -> None:
    # F-T31: the window (2026-04-04T13:00Z -> 2026-04-05T13:30Z) holds 25
    # on-the-hour UTC instants over 24.5 hours; local_day_slots floors that
    # to 24. A fully covered feed must be accepted.
    full = _instants(_LORD_HOWE_TRANSITION_START, 25)
    assert len(full) == 25
    assert covers_local_day(
        full, local_date=_LORD_HOWE_TRANSITION_DATE, timezone="Australia/Lord_Howe"
    )


def test_lord_howe_dst_shift_rejects_every_single_utc_hour_instant_drop() -> None:
    # F-T31: every one of the 25 required UTC-hour instants is
    # load-bearing -- dropping any single one (interior or boundary) must
    # be rejected.
    for omit_index in range(25):
        variant = _instants(_LORD_HOWE_TRANSITION_START, 25, omit={omit_index})
        assert len(variant) == 24
        assert not covers_local_day(
            variant,
            local_date=_LORD_HOWE_TRANSITION_DATE,
            timezone="Australia/Lord_Howe",
        ), f"omit_index={omit_index} wrongly accepted"


def test_lord_howe_dst_shift_boundary_not_substitutable_from_outside_window() -> None:
    # F-T31: a sample just outside [start, end) cannot stand in for a
    # missing first or last required hour, even though doing so keeps the
    # in-window instant *count* numerically equal to the old floored-count
    # comparison's expectation (24).
    full = _instants(_LORD_HOWE_TRANSITION_START, 25)

    just_before_start = (
        (_LORD_HOWE_TRANSITION_START - timedelta(hours=1))
        .isoformat()
        .replace("+00:00", "Z")
    )
    missing_first = full[1:] + [just_before_start]
    assert len(missing_first) == 25
    assert not covers_local_day(
        missing_first,
        local_date=_LORD_HOWE_TRANSITION_DATE,
        timezone="Australia/Lord_Howe",
    )

    just_after_end = (
        (_LORD_HOWE_TRANSITION_START + timedelta(hours=25))
        .isoformat()
        .replace("+00:00", "Z")
    )
    missing_last = full[:-1] + [just_after_end]
    assert len(missing_last) == 25
    assert not covers_local_day(
        missing_last,
        local_date=_LORD_HOWE_TRANSITION_DATE,
        timezone="Australia/Lord_Howe",
    )


_LORD_HOWE_CONTROL_START = datetime(2026, 10, 3, 14, tzinfo=UTC)
_LORD_HOWE_CONTROL_DATE = date(2026, 10, 4)


def test_lord_howe_spring_forward_day_requires_all_23_utc_hour_instants() -> None:
    # F-T31 control: the spring-forward day in the same zone also has a
    # window that is a fractional number of hours (2026-10-03T13:30Z ->
    # 2026-10-04T13:00Z, 23.5 hours), but it starts off the hour, so its
    # floored count (23) matches the 23 required on-the-hour instants --
    # so this day does not distinguish old from new code. It pins that
    # the fix left ordinary (non-mismatching) days unchanged.
    full = _instants(_LORD_HOWE_CONTROL_START, 23)
    assert len(full) == 23
    assert covers_local_day(
        full, local_date=_LORD_HOWE_CONTROL_DATE, timezone="Australia/Lord_Howe"
    )

    short = _instants(_LORD_HOWE_CONTROL_START, 23, omit={5})
    assert len(short) == 22
    assert not covers_local_day(
        short, local_date=_LORD_HOWE_CONTROL_DATE, timezone="Australia/Lord_Howe"
    )
