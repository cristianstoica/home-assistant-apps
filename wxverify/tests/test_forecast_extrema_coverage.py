"""Item F — daily extrema decided by coverage, before skill ranks (F.11).

F-T1 through F-T5 live in ``tests/test_forecast_selection.py`` (pure
``select_cell_feeds``/``CellCandidate`` unit coverage, already present
there). This file covers F-T6 through F-T31:

* F-T6 through F-T12 — the completeness rule itself (``local_day_slots``,
  ``local_day_bounds``, ``covers_local_day``), pure and DB-free.
* F-T13 through F-T17 — end to end through ``build_forecast``/
  ``build_hourly`` (the live Forecast page's surfaces).
* F-T22 through F-T25 — the stale and low-confidence/rebuilding badge
  rules: a stale contributor alone, an unscored extrema set beside a
  confident blend set, and suppressed extrema contributing neither warning
  (with a paired positive stale variant).
* F-T26 — confidence states are independent per side (blend vs. extrema)
  when their contributor sets differ, and roll up identically to
  ``today``'s state when the contributor set is shared, swept over fresh,
  stale and low-confidence.
* F-T27 — retired with the old wind method: weighted wind's state ignores
  the ranking, and it has no extrema set. (Its precipitation half moved to
  ``tests/test_forecast_precip_coverage.py`` G-T23 through G-T26 once
  precipitation adopted its own extrema-coverage rule under Item G.)
* F-T28 and F-T29 — tile-level state/confidence-state rollup across
  variables and badge/tooltip/persisted-metadata agreement. F-T30
  (rebuilding precedence on the extrema set) is retired with the daily
  close being permanent: ``leaderboard_with_status`` never reports
  "rebuilding" once a publication is active, so the state it pinned can no
  longer occur.
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

import sqlite3
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

from tests.helpers import (
    seed_active_publication,
    seed_publication_ranking,
    seed_rain_ranks,
)
from wxverify.core.timeutil import local_day_slots
from wxverify.db.migrations import run_migrations
from wxverify.forecast.aggregate import covers_local_day
from wxverify.forecast.page import (
    FETCH_UNKNOWN_TEXT,
    LOW_CONFIDENCE_TEXT,
    RANKING_UPDATING_TEXT,
    STALE_TEXT,
    ForecastPage,
    build_forecast_page,
)
from wxverify.forecast.service import build_forecast, build_hourly
from wxverify.scoring.metrics import MetricSums, metrics_from_sums
from wxverify.settings.keys import get_number_setting, set_setting
from wxverify.verification.coverage import DayBounds, local_day_bounds
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


_TEST_PUBLICATION_WINDOW_DAYS = 30


def _ensure_test_publication(
    conn: sqlite3.Connection, *, min_n: int
) -> tuple[int, int]:
    """This connection's one site-1 publication, created on first use.

    Returns ``(publication id, its window_days)``: a caller seeded first by
    another helper (``seed_rain_ranks``, say) leaves a publication whose
    ``window_days`` is that helper's own default, not this file's constant,
    so callers must read it back rather than assume it.

    ``forecast_ranking_with_status``'s ``rolling`` window now always reads
    the active publication (9.2), never a live recompute. Mirrors
    ``tests/test_forecast_service.py``'s identically-named helper.
    """
    row = conn.execute(
        "SELECT id, window_days FROM publications WHERE site_id = 1"
        " ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row is not None:
        return int(row["id"]), int(row["window_days"])
    pub_id = seed_active_publication(
        conn,
        site_id=1,
        cutoff=date(2035, 6, 30),
        published_at=datetime(2035, 6, 30, tzinfo=UTC),
        window_days=_TEST_PUBLICATION_WINDOW_DAYS,
        min_n=min_n,
    )
    return pub_id, _TEST_PUBLICATION_WINDOW_DAYS


def _seed_score_cache(
    conn: sqlite3.Connection,
    *,
    variable: str,
    day_ahead: int,
    forecasts: dict[int, float],
) -> None:
    """Seed one published-ranking row per feed in ``forecasts`` (site 1,
    ``variable``, ``day_ahead``) -- required for `forecast_ranking_with_status`
    to serve live numbers rather than 'rebuilding'. Mirrors
    ``tests/test_forecast_service.py``'s identically-named helper.

    Every key of ``forecasts`` stands for 3 identical far-future pairs
    against a fixed observed=10.0 baseline (``n=3``), so bias/MAE/RMSE are
    exact, not approximated. A feed's skill is computed only when
    ``forecasts`` also carries the seeded ``virtual/_persistence`` feed
    id, paired 1:1 against persistence's own (also constant) error over
    the same 3 valid_ats. ``forecasts`` is local to the caller: no
    module-level state survives this call, so a stale ``id(conn)`` reuse
    across tests can never leak one test's values into another's.
    """
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    pub_id, window_days = _ensure_test_publication(conn, min_n=min_n)
    window_key = f"w:{window_days}"
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    persistence_forecast = forecasts.get(persistence_id)
    persistence_sq = (
        None if persistence_forecast is None else (persistence_forecast - 10.0) ** 2
    )
    for feed_id, forecast in forecasts.items():
        error = forecast - 10.0
        sums: MetricSums = {
            "n": 3,
            "err_n": 3,
            "err_sum": error * 3,
            "abs_sum": abs(error) * 3,
            "sq_sum": error * error * 3,
        }
        if persistence_sq is not None:
            sums = {
                **sums,
                "skill_n": 3,
                "skill_sq_sum": error * error * 3,
                "skill_ref_sq_sum": persistence_sq * 3,
            }
        result = metrics_from_sums(sums, variable=variable, min_n=min_n)
        seed_publication_ranking(
            conn,
            publication_id=pub_id,
            feed_id=feed_id,
            variable=variable,
            day_ahead=day_ahead,
            window_key=window_key,
            result=result,
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


def _build_page(
    conn: sqlite3.Connection, *, now: datetime, selected_day: int = 0
) -> ForecastPage:
    """``build_forecast_page`` with this file's fixed site params, so a
    render check reaches the real page/header/warnings the route builds,
    not a hand-built stand-in."""
    return build_forecast_page(
        conn,
        site_id=1,
        timezone="UTC",
        rain_threshold_mm=0.2,
        selected_day=selected_day,
        now=now,
    )


def _render_tiles(*, site_id: int, page: ForecastPage) -> str:
    tmpl = jinja_env.get_template("forecast/_tiles.html")
    return tmpl.render(
        url=lambda p: str(p),
        site=SimpleNamespace(id=site_id),
        page=page,
        day=page.selected_day,
    )


def _day0_header_html(html: str) -> str:
    """Today's (day index 0) day-header fragment: the button, its ⚠ warning
    button and tooltip if any -- split out by the per-header div marker so a
    check never reads another day's warnings."""
    return html.split('class="dh-cell ds"')[1]


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

    # Card 7: the old per-tile suppression text and "High / Low" row are
    # gone. The replacement is the day header's value span, which renders
    # "—" (never a degree sign) whenever extrema_unavailable is True.
    page = _build_page(conn, now=now)
    header = page.headers[0]
    assert header.high_c is None
    assert header.low_c is None
    assert header.extrema_unavailable is True
    html = _render_tiles(site_id=1, page=page)
    start = html.index("<span>") + len("<span>")
    end = html.index("</span>", start)
    assert html[start:end] == "—"


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
    _seed_score_cache(
        conn,
        variable="temperature",
        day_ahead=1,
        forecasts={persistence_id: 5.0, full_id: 11.0, partial_id: 10.2},
    )
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
    # The old tile's "Feeds: <names>" tooltip for the extrema (high/low)
    # contributor set has no replacement -- the detail grid's only
    # tooltip (`grid.blend_labels`) names the BLEND set instead, which
    # differs here (the higher-skill partial feed wins blend_depth=1's
    # ranking but is excluded from the extrema set used above). Flagged as
    # a possible gap: the user can no longer see which models back the
    # High/Low figures shown in this same row. No render check replaces the
    # dropped assertion; the data-level check above is this test's full
    # remaining intent.


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
# F-T17 -- precipitation needs a whole local day too (Item G; rain release
# C4: the winner's), so the SAME 12h partial fixture suppresses both.
# ===========================================================================


def test_temperature_and_precip_suppress_on_the_same_partial_day() -> None:
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
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
        value=1.0,
    )
    # Rain release, C4: the feed is the published rain ranking's only model.
    seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [feed_id]})
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_unavailable is True  # temperature suppressed

    # Precipitation (rain release, C4) needs a whole local day covered
    # exactly once from its winner: 12 of 24 hours is short of it, so the
    # ranked feed is no winner and the daily total/wet-hours are absent --
    # not aggregated from the 12h partial range.
    assert today.precip.meta.state == "not_available"
    assert today.precip.total_mm is None
    assert today.precip.wet_hours is None
    # The rain cell has no clearing subset, so it never sets "partial"; the
    # >=18h guard still fires on temperature's same 12h shape.
    assert today.precip.meta.partial is False
    assert today.temp.meta.partial is True
    assert today.partial is True  # tile-level badge still fires


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
# The fixtures below stamp each feed's last usable fetch at the issued_at they
# are given, so "fresh"/"stale" above are fetch ages (the stale badge's basis)
# as well as run ages.


def _stamp_usable_fetch(conn: sqlite3.Connection, *, feed_id: int, at: str) -> None:
    """Record ``at`` as site 1's last usable forward fetch of ``feed_id``."""
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_run_at, last_usable_fetch_at)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET
            last_run_at=excluded.last_run_at,
            last_usable_fetch_at=excluded.last_usable_fetch_at
        """,
        (feed_id, at, at),
    )


def _seed_ab_fixture(
    conn: sqlite3.Connection,
    *,
    feed_a_issued_at: str = _AB_FRESH_ISSUED,
    feed_b_issued_at: str = _AB_FRESH_ISSUED,
    seed_b_scoring: bool = True,
) -> tuple[int, int]:
    """F.11's shared two-feed fixture. Returns (feed_a, feed_b).

    Both feeds' ``forecasts`` entries are seeded alongside the seeded
    ``virtual/_persistence`` feed's own entry: ``metrics_from_sums``'s
    ``confident`` flag requires a non-None skill score, and
    ``_skill_from_sums`` only produces one when the persistence feed's
    forecast is also given (``wxverify/scoring/metrics.py``). Without a
    persistence counterpart, ``confident`` is False no matter how many
    pairs or how low ``min_n`` is set -- the trap this fixture avoids.
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
    _stamp_usable_fetch(conn, feed_id=feed_a, at=feed_a_issued_at)
    _stamp_usable_fetch(conn, feed_id=feed_b, at=feed_b_issued_at)

    # A's forecast is closer to the observed=10.0 baseline than B's -- A
    # wins the blend ladder on skill whenever both are confident, so tests
    # relying on "A is the blend winner" hold regardless of which of the
    # two feeds a reader might otherwise expect to rank higher.
    forecasts = {persistence_id: 8.0, feed_a: 10.02}
    if seed_b_scoring:
        forecasts[feed_b] = 10.2
    _seed_score_cache(conn, variable="temperature", day_ahead=0, forecasts=forecasts)
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
    page = _build_page(conn, now=_AB_NOW)
    assert STALE_TEXT in page.headers[0].warnings
    html = _render_tiles(site_id=1, page=page)
    assert STALE_TEXT in _day0_header_html(html)

    both_fresh = _make_db()
    _seed_ab_fixture(
        both_fresh, feed_a_issued_at=_AB_FRESH_ISSUED, feed_b_issued_at=_AB_FRESH_ISSUED
    )
    both_fresh.commit()
    fresh_view = build_forecast(
        both_fresh, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    assert fresh_view.tiles[0].temp.meta.stale is False
    fresh_page = _build_page(both_fresh, now=_AB_NOW)
    assert STALE_TEXT not in fresh_page.headers[0].warnings
    fresh_html = _render_tiles(site_id=1, page=fresh_page)
    assert STALE_TEXT not in _day0_header_html(fresh_html)


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
    page = _build_page(conn, now=_AB_NOW)
    assert STALE_TEXT in page.headers[0].warnings
    html = _render_tiles(site_id=1, page=page)
    assert STALE_TEXT in _day0_header_html(html)


def test_unknown_badge_fires_when_a_contributor_has_no_fetch_stamp() -> None:
    # A (blend winner) freshly stamped, B (extrema winner) has NEVER been
    # stamped (last_usable_fetch_at nulled after the fixture seeds it) --
    # fetch_unknown must fire without stale firing. Paired positive: both
    # stamped fresh (the base _seed_ab_fixture shape used elsewhere in this
    # file) shows neither badge, proving the unknown badge isn't ambiently
    # on.
    conn = _make_db()
    feed_a, feed_b = _seed_ab_fixture(
        conn, feed_a_issued_at=_AB_FRESH_ISSUED, feed_b_issued_at=_AB_FRESH_ISSUED
    )
    conn.execute(
        "UPDATE site_feed_state SET last_usable_fetch_at = NULL "
        "WHERE site_id=1 AND feed_id=?",
        (feed_b,),
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.stale is False
    assert today.temp.meta.fetch_unknown is True
    page = _build_page(conn, now=_AB_NOW)
    warnings = page.headers[0].warnings
    assert STALE_TEXT not in warnings
    assert FETCH_UNKNOWN_TEXT in warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert STALE_TEXT not in frag
    assert FETCH_UNKNOWN_TEXT in frag

    both_known = _make_db()
    _seed_ab_fixture(
        both_known, feed_a_issued_at=_AB_FRESH_ISSUED, feed_b_issued_at=_AB_FRESH_ISSUED
    )
    both_known.commit()
    known_view = build_forecast(
        both_known, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    assert known_view.tiles[0].temp.meta.fetch_unknown is False
    known_page = _build_page(both_known, now=_AB_NOW)
    assert FETCH_UNKNOWN_TEXT not in known_page.headers[0].warnings
    known_html = _render_tiles(site_id=1, page=known_page)
    assert FETCH_UNKNOWN_TEXT not in _day0_header_html(known_html)


def test_stale_plus_unknown_shows_only_the_stale_badge() -> None:
    # A (blend winner) stale, B (extrema winner) unknown (never stamped).
    # The template guards the unknown badge with ``and not tile.stale``, so
    # when both conditions hold simultaneously only "stale" renders -- this
    # pins that suppression rather than assuming "both fire".
    conn = _make_db()
    feed_a, feed_b = _seed_ab_fixture(
        conn, feed_a_issued_at=_AB_STALE_ISSUED, feed_b_issued_at=_AB_FRESH_ISSUED
    )
    conn.execute(
        "UPDATE site_feed_state SET last_usable_fetch_at = NULL "
        "WHERE site_id=1 AND feed_id=?",
        (feed_b,),
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.stale is True
    assert today.temp.meta.fetch_unknown is True
    page = _build_page(conn, now=_AB_NOW)
    warnings = page.headers[0].warnings
    assert STALE_TEXT in warnings
    assert FETCH_UNKNOWN_TEXT not in warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert STALE_TEXT in frag
    assert FETCH_UNKNOWN_TEXT not in frag


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
    # Card 7 dropped the old per-state CSS class ("tile state-normal") for
    # a plain not_available boolean; the low-confidence/ranking-updating
    # distinction now lives only in the day header's warning texts.
    page = _build_page(conn, now=_AB_NOW)
    header = page.headers[0]
    assert header.not_available is False
    assert LOW_CONFIDENCE_TEXT in header.warnings
    assert RANKING_UPDATING_TEXT not in header.warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert "state-not_available" not in frag
    assert LOW_CONFIDENCE_TEXT in frag
    assert RANKING_UPDATING_TEXT not in frag


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
    _stamp_usable_fetch(conn, feed_id=feed_a, at=_AB_FRESH_ISSUED)
    _seed_score_cache(
        conn,
        variable="temperature",
        day_ahead=0,
        forecasts={persistence_id: 8.0, feed_a: 10.1},
    )
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
    page = _build_page(conn, now=_AB_NOW)
    assert page.headers[0].warnings == ()
    html = _render_tiles(site_id=1, page=page)
    assert "day-warn" not in _day0_header_html(html)


def test_stale_blend_set_alone_still_sets_the_stale_badge_with_extrema_suppressed() -> (
    None
):
    # Positive pair to the test above: same suppressed-extrema shape, but
    # the blend feed's last fetch is stale -- the stale badge is driven by
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
    _stamp_usable_fetch(conn, feed_id=feed_a, at=_AB_STALE_ISSUED)
    _seed_score_cache(
        conn,
        variable="temperature",
        day_ahead=0,
        forecasts={persistence_id: 8.0, feed_a: 10.1},
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_unavailable is True
    assert today.temp.meta.extrema_state == "not_available"
    assert today.temp.meta.state == "normal"
    assert today.temp.meta.stale is True
    page = _build_page(conn, now=_AB_NOW)
    assert STALE_TEXT in page.headers[0].warnings
    html = _render_tiles(site_id=1, page=page)
    assert STALE_TEXT in _day0_header_html(html)


def test_shared_contributor_set_confidence_states_are_independent_per_side() -> None:
    # F-T26: sweep the two REACHABLE (blend-side, extrema-side) confidence
    # combinations across the SAME two-feed fixture shape, proving the two
    # sides are read independently rather than one leaking into the other.
    # "blend low-confidence, extrema confident" is unreachable: eligible
    # (extrema) candidates are a subset of the >=12h blend coverage pool, so
    # whatever lowers the blend-side ranking's confidence lowers the
    # extrema-side ranking's confidence too whenever they share candidates.
    # A third case used to cover "extrema alone rebuilding" (blend side
    # confident, extrema side with pairs but no score_cache row), but the
    # daily close being permanent means ``leaderboard_with_status`` can no
    # longer report "rebuilding" at all -- that case pinned removed
    # behaviour and was deleted outright rather than reworked. "low
    # confidence" and "ranking updating" are mutually exclusive badge
    # states (F.6's ``_state_of`` precedence), so each remaining case pins
    # exactly one outcome and its absence. The plan's shared-CONTRIBUTOR-SET half
    # (extrema set == blend set, not just both confidence-linked) is
    # exercised separately below, in
    # ``test_shared_contributor_set_states_roll_up_identically_to_today``;
    # its stale+rebuilding-beside-low-confidence-cell case is F-T28(5).

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
    page = _build_page(conn, now=_AB_NOW)
    header = page.headers[0]
    assert LOW_CONFIDENCE_TEXT not in header.warnings
    assert RANKING_UPDATING_TEXT not in header.warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert LOW_CONFIDENCE_TEXT not in frag
    assert RANKING_UPDATING_TEXT not in frag

    # Case 2: both low (min_n raised globally, so it lowers BOTH the
    # blend-side and the extrema-side ranking's confidence at once -- there
    # is no per-side min_n). min_n must be raised BEFORE seeding:
    # `_ensure_test_publication` bakes the live "min_n" setting into the
    # publication's own min_n at its first build and never re-reads it, so
    # raising the setting afterward has no effect on the published verdict.
    conn = _make_db()
    set_setting(conn, "min_n", "10")
    _seed_ab_fixture(conn)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "low_confidence"
    assert today.temp.meta.extrema_state == "low_confidence"  # min_n raised globally
    page = _build_page(conn, now=_AB_NOW)
    header = page.headers[0]
    assert LOW_CONFIDENCE_TEXT in header.warnings
    assert RANKING_UPDATING_TEXT not in header.warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert LOW_CONFIDENCE_TEXT in frag
    assert RANKING_UPDATING_TEXT not in frag


def _seed_shared_ab_fixture(
    conn: sqlite3.Connection,
    *,
    feed_a_issued_at: str = _AB_FRESH_ISSUED,
    feed_b_issued_at: str = _AB_FRESH_ISSUED,
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
    _stamp_usable_fetch(conn, feed_id=feed_a, at=feed_a_issued_at)
    _stamp_usable_fetch(conn, feed_id=feed_b, at=feed_b_issued_at)
    _seed_score_cache(
        conn,
        variable="temperature",
        day_ahead=0,
        forecasts={persistence_id: 8.0, feed_a: 10.02, feed_b: 10.2},
    )
    return feed_a, feed_b


def test_shared_contributor_set_states_roll_up_identically_to_today() -> None:
    # The shared-CONTRIBUTOR-SET half: every feed covers the day so
    # ``extrema_feeds`` equals the blend set's clearing subset exactly.
    # Swept over fresh, stale and low-confidence: in every state the two
    # sides read identically, because they ARE the same ranking over the
    # same candidates. The daily close is always on now, so there is no
    # "rebuilding" leaderboard verdict to sweep beside low-confidence (the
    # daily close is permanent; rebuilding was a close-off-only state).
    # The stale-rebuilding-beside-a-low-confidence-cell case is not
    # repeated here.

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
    page = _build_page(conn, now=_AB_NOW)
    header = page.headers[0]
    assert header.not_available is False
    assert header.warnings == ()
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert "state-not_available" not in frag
    assert "day-warn" not in frag

    # Stale: both feeds fetched stale, so both the blend and extrema sides
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
    page = _build_page(conn, now=_AB_NOW)
    header = page.headers[0]
    assert header.not_available is False
    assert STALE_TEXT in header.warnings
    assert LOW_CONFIDENCE_TEXT not in header.warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert "state-not_available" not in frag
    assert STALE_TEXT in frag
    assert LOW_CONFIDENCE_TEXT not in frag

    # Low-confidence: min_n raised globally -- both sides share the same
    # ranking, so both drop below it together. Raised BEFORE seeding:
    # `_ensure_test_publication` bakes the live "min_n" setting into the
    # publication's own min_n at its first build and never re-reads it.
    conn = _make_db()
    set_setting(conn, "min_n", "10")
    _seed_shared_ab_fixture(conn)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.extrema_state == today.temp.meta.state == "low_confidence"
    assert today.confidence_state == today.state == "low_confidence"
    page = _build_page(conn, now=_AB_NOW)
    header = page.headers[0]
    assert LOW_CONFIDENCE_TEXT in header.warnings
    assert RANKING_UPDATING_TEXT not in header.warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert LOW_CONFIDENCE_TEXT in frag
    assert RANKING_UPDATING_TEXT not in frag


def test_tile_level_rollup_across_variables() -> None:
    # F-T28: ``DayTile.stale`` and ``DayTile.confidence_state`` roll up
    # across cells (here temperature and precip), not just within a single
    # cell's own blend/extrema split -- five independent sub-fixtures, each
    # isolating one source of a tile-level signal.

    # (1) Only temperature's extrema contributor (B) is stale -- the same
    # per-cell signal F-T22 pins, checked here at the TILE level.
    conn = _make_db()
    _seed_ab_fixture(
        conn, feed_a_issued_at=_AB_FRESH_ISSUED, feed_b_issued_at=_AB_STALE_ISSUED
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    assert view.tiles[0].stale is True

    # (2) Only precipitation's own feed is stale -- temperature fully fresh,
    # proving the tile-level ``stale`` union reaches another cell too, not
    # only temperature's. Precipitation uses a THIRD feed (never touched by
    # temperature) rather than feed_a: ``load_feed_freshness`` judges
    # staleness per FEED on one last usable fetch shared by ALL that feed's
    # variables (``wxverify/forecast/data.py``), so a stale precipitation
    # cell on feed_a would be masked by that same feed's fresh stamp -- the
    # trap this isolation avoids. Rain release, C4: the feed is the rain
    # ranking's winner, so it needs a full day.
    # ``seed_rain_ranks`` runs BEFORE ``_seed_ab_fixture``: both create a
    # publication if none exists yet, and ``active_publication`` picks
    # whichever has the LATEST cutoff, never the most recently published --
    # seeding temperature second lets its far-future ``_ensure_test_publication``
    # cutoff (2035-06-30) reuse the rain fixture's publication instead of
    # out-cutoffing it, so the rain ranking stays visible to the same read.
    conn = _make_db()
    precip_feed = _feed_id(conn, "open-meteo", "icon_global")
    seed_rain_ranks(conn, site_id=1, published_at=_AB_NOW, ranked={0: [precip_feed]})
    _seed_ab_fixture(conn)
    _seed_hourly(
        conn,
        feed_id=precip_feed,
        variable="precip",
        issued_at=_AB_STALE_ISSUED,
        valid_ats=_hours(_AB_VALID_DAY, 0, 24),
        value=0.5,
    )
    _stamp_usable_fetch(conn, feed_id=precip_feed, at=_AB_STALE_ISSUED)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.stale is False  # isolates which cell carries it
    assert today.precip.meta.stale is True
    assert today.stale is True

    # (3) Rain release, C4: the rain cell never reads the leaderboard, so
    # precipitation with no scoring pairs at all (the old rung 3, which made
    # the cell low-confidence) is a normal cell under a settled rain
    # ranking. No temperature here: with the daily close on (the ranks are
    # read only then) and no published temperature ranking, a temperature
    # cell would be low-confidence on its own.
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="precip",
        issued_at=_AB_FRESH_ISSUED,
        valid_ats=_hours(_AB_VALID_DAY, 0, 24),
        value=0.5,
    )
    seed_rain_ranks(conn, site_id=1, published_at=_AB_NOW, ranked={0: [feed_a]})
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.precip.meta.state == "normal"
    assert today.precip.meta.extrema_state == "normal"
    assert today.state == "normal"
    assert today.confidence_state == "normal"
    page = _build_page(conn, now=_AB_NOW)
    header = page.headers[0]
    assert header.not_available is False
    assert LOW_CONFIDENCE_TEXT not in header.warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert "state-not_available" not in frag
    assert LOW_CONFIDENCE_TEXT not in frag

    # (4) Only temperature's EXTREMA set is low-confidence (blend side
    # confident) -- ``state`` (CSS class) stays "normal" because it never
    # looks at ``extrema_state``, while ``confidence_state`` (badges) picks
    # it up, the pairing F.6 requires kept independent.
    conn = _make_db()
    _seed_ab_fixture(conn, seed_b_scoring=False)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "normal"
    assert today.temp.meta.extrema_state == "low_confidence"
    assert today.state == "normal"
    assert today.confidence_state == "low_confidence"
    page = _build_page(conn, now=_AB_NOW)
    header = page.headers[0]
    assert header.not_available is False
    assert LOW_CONFIDENCE_TEXT in header.warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert "state-not_available" not in frag
    assert LOW_CONFIDENCE_TEXT in frag

    # (5) Rain release, C4: a low-confidence temperature cell BESIDE a
    # provisional rain ranking. The rollup's precedence (low confidence
    # outranks rebuilding) still resolves ACROSS cells for
    # ``confidence_state``, but the rain ranking's own "ranking updating"
    # badge shows beside "low confidence" (owner, C6: the badge stays until
    # a model really has 20 counted days). The old construction, a
    # rebuilding temperature extrema set beside a low-confidence rain cell,
    # cannot be built any more: the rain cell is never low-confidence.
    # Temperature is low-confidence here because the daily close is on (the
    # rain ranks are read only then) with no published temperature ranking.
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at=_AB_FRESH_ISSUED,
        valid_ats=_hours(_AB_VALID_DAY, 0, 24),
        values=[10.0 + i * 0.1 for i in range(24)],
    )
    _seed_hourly(
        conn,
        feed_id=feed_b,
        variable="precip",
        issued_at=_AB_FRESH_ISSUED,
        valid_ats=_hours(_AB_VALID_DAY, 0, 24),
        value=0.5,
    )
    seed_rain_ranks(
        conn,
        site_id=1,
        published_at=_AB_NOW,
        ranked={0: [feed_b]},
        provisional={0},
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "low_confidence"
    assert today.precip.meta.extrema_state == "rebuilding"
    assert today.confidence_state == "low_confidence"
    page = _build_page(conn, now=_AB_NOW)
    header = page.headers[0]
    assert LOW_CONFIDENCE_TEXT in header.warnings
    assert RANKING_UPDATING_TEXT in header.warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert LOW_CONFIDENCE_TEXT in frag
    assert RANKING_UPDATING_TEXT in frag

    # (6) None of the above -- temperature fully confident and fresh, wind
    # and precip left unpopulated (``not_available``, ignored by both
    # rollups): no badge row renders at all. Paired negative for every
    # positive badge assertion above -- proves the badge row isn't ambiently
    # present regardless of state.
    conn = _make_db()
    _seed_ab_fixture(conn)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.stale is False
    assert today.confidence_state == "normal"
    page = _build_page(conn, now=_AB_NOW)
    assert page.headers[0].warnings == ()
    html = _render_tiles(site_id=1, page=page)
    assert "day-warn" not in _day0_header_html(html)


def test_badge_tooltip_and_persisted_metadata_agree_on_confidence() -> None:
    # F-T29: run the same fixture shape twice (extrema side confident, then
    # low-confidence) and check that, WITHIN EACH RUN, the rendered warning
    # agrees with the single confidence knob that produced it. Card 7
    # removed the old "Feeds: <names>" tooltip naming the extrema set with
    # no replacement (flagged in test_fully_covered_feed_beats_higher_
    # skill_partial_feed above) -- only the persisted-metadata half of this
    # test's original claim (the data assertions below) survives; the
    # badge/tooltip agreement is now just badge presence.
    for extrema_confident in (True, False):
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
        page = _build_page(conn, now=_AB_NOW)
        has_warning = LOW_CONFIDENCE_TEXT in page.headers[0].warnings
        assert has_warning is (not extrema_confident)
        html = _render_tiles(site_id=1, page=page)
        frag = _day0_header_html(html)
        assert (LOW_CONFIDENCE_TEXT in frag) is (not extrema_confident)


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
