"""Item G -- daily rain from a fixed set of feeds (plan G.11, G-T1-G-T28).

Precipitation adopts the same coverage-before-ranking discipline temperature
got under Item F, but with a STRICTER predicate: an extrema-eligible feed
must supply each hour of the local day EXACTLY once
(:func:`wxverify.forecast.aggregate.covers_local_day_exactly`), because a
daily total and a wet-hour count need one value per hour, not merely "at
least one" the way an extremum does. ``wet_hours`` replaces the old
percentage-based ``wet_share``/``chance_pct`` with a plain count.

G-T14/G-T15 (``wet_hours``'s own arithmetic) live in
``tests/test_forecast_aggregate.py``, the successors to the deleted
``wet_share``/``predicted_wet_hour_share`` tests.

All fixtures are synthetic and offline (the autouse ``_deny_network`` fixture
in ``conftest.py`` fails any test that opens a socket): site timezone UTC
except the DST cases, which use ``Europe/Berlin`` (G-T4, G-T5, G-T11) and
``Australia/Lord_Howe`` (G-T27, G-T28) per G.11; coordinates ``0.0/0.0``;
``rain_threshold_mm=0.2``. Every expected value is derived from reading the
body of the named production function, never transcribed from the plan
document.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

from tests.helpers import (
    seed_active_publication,
    seed_publication_ranking,
    seed_rain_ranks,
)
from wxverify.core.timeutil import isoformat_utc, local_day_slots, parse_utc
from wxverify.db.migrations import run_migrations
from wxverify.forecast.aggregate import (
    EXTREMA_COVERAGE_COMPLETE,
    EXTREMA_COVERAGE_INSUFFICIENT,
    blend_mean,
    covers_local_day,
    covers_local_day_exactly,
    displayed_daily,
)
from wxverify.forecast.page import (
    LOW_CONFIDENCE_TEXT,
    NO_RAIN_TEXT,
    RANKING_UPDATING_TEXT,
    STALE_TEXT,
    ForecastPage,
    build_forecast_page,
)
from wxverify.forecast.selection import CellCandidate, select_cell_feeds
from wxverify.forecast.service import (
    build_forecast,
    build_hourly,
)
from wxverify.scoring.metrics import MetricResult, MetricSums, metrics_from_sums
from wxverify.scoring.pair_flags import precip_flags
from wxverify.settings.keys import get_number_setting, set_setting
from wxverify.web.render import env as jinja_env

# ---------------------------------------------------------------------------
# Shared DB harness (mirrors tests/test_forecast_extrema_coverage.py).
# ---------------------------------------------------------------------------


def _make_db(*, timezone: str = "UTC") -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    conn.execute(
        """
        INSERT INTO sites
            (id, name, forecast_lat, forecast_lon, elevation_m, timezone,
             rain_threshold_mm)
        VALUES (1, 'Test Site', 0.0, 0.0, 0.0, ?, 0.2)
        """,
        (timezone,),
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


# A wet/dry-mixed observed baseline for precip's categorical (hit/false/
# miss/correct-negative) skill computation (`metrics_from_sums`): three
# pairs of the same category (all-wet or all-dry) degenerate to ETS=None
# (``confident=False`` no matter how many pairs or how low ``min_n`` is),
# so every precip confidence fixture in this module needs a mixed observed
# sequence, never a repeated scalar.
_PRECIP_MIXED_OBSERVED = (1.0, 0.0, 1.0)
_PRECIP_ACCURATE_FORECAST = (1.0, 0.0, 1.0)  # matches observed -> high skill
_PRECIP_POOR_FORECAST = (0.0, 1.0, 0.0)  # opposite of observed -> poor skill

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


def _precip_metric_result(
    *,
    forecast: float | Sequence[float],
    observed: float | Sequence[float],
    min_n: int,
) -> MetricResult:
    """The precip ``MetricResult`` for one feed's 3 synthetic pairs.

    ``forecast``/``observed`` are each either one value repeated for all 3
    pairs, or a 3-element sequence for per-pair control, run through the
    real :func:`precip_flags` classifier.
    """
    forecasts = [forecast] * 3 if isinstance(forecast, float | int) else list(forecast)
    observeds = [observed] * 3 if isinstance(observed, float | int) else list(observed)
    n = err_n = hits = false_alarms = misses = correct_negs = 0
    err_sum = abs_sum = sq_sum = 0.0
    for fc, obs in zip(forecasts, observeds, strict=True):
        error = fc - obs
        n += 1
        err_n += 1
        err_sum += error
        abs_sum += abs(error)
        sq_sum += error * error
        hit, false, miss, correct_neg = precip_flags("precip", fc, obs, 0.2)
        hits += hit or 0
        false_alarms += false or 0
        misses += miss or 0
        correct_negs += correct_neg or 0
    sums: MetricSums = {
        "n": n,
        "err_n": err_n,
        "err_sum": err_sum,
        "abs_sum": abs_sum,
        "sq_sum": sq_sum,
        "hits": hits,
        "false_alarms": false_alarms,
        "misses": misses,
        "correct_negs": correct_negs,
    }
    return metrics_from_sums(sums, variable="precip", min_n=min_n)


def _seed_score_cache(
    conn: sqlite3.Connection,
    *,
    day_ahead: int,
    pairs: dict[int, tuple[float | Sequence[float], float | Sequence[float]]],
) -> None:
    """Seed one published precip ranking row per feed in ``pairs`` (site 1,
    ``day_ahead``), each a ``(forecast, observed)`` pair fed through
    :func:`_precip_metric_result`. ``pairs`` is local to the caller: no
    module-level state survives this call, so a stale ``id(conn)`` reuse
    across tests can never leak one test's values into another's.
    """
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    pub_id, window_days = _ensure_test_publication(conn, min_n=min_n)
    window_key = f"w:{window_days}"
    for feed_id, (forecast, observed) in pairs.items():
        result = _precip_metric_result(
            forecast=forecast, observed=observed, min_n=min_n
        )
        seed_publication_ranking(
            conn,
            publication_id=pub_id,
            feed_id=feed_id,
            variable="precip",
            day_ahead=day_ahead,
            window_key=window_key,
            result=result,
        )


def _hours(day: str, start: int, count: int) -> list[str]:
    return [f"{day}T{h:02d}:00:00Z" for h in range(start, start + count)]


def _instant_range(start: str, count: int) -> list[str]:
    """``count`` on-the-hour UTC instants starting at ``start`` (a literal
    UTC endpoint from G.11 -- never derived from the predicate under test).
    """
    dt = parse_utc(start)
    return [isoformat_utc(dt + timedelta(hours=i)) for i in range(count)]


def _seed_varying(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_ats: Sequence[str],
    values: Sequence[float],
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


def _seed_uniform(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_ats: Sequence[str],
    value: float,
) -> None:
    _seed_varying(
        conn,
        feed_id=feed_id,
        variable=variable,
        issued_at=issued_at,
        valid_ats=valid_ats,
        values=[value] * len(valid_ats),
    )


def _build_page(
    conn: sqlite3.Connection, *, now: datetime, selected_day: int = 0
) -> ForecastPage:
    """``build_forecast_page`` with this file's fixed site params (mirrors
    ``tests/test_forecast_extrema_coverage.py``'s identically-named
    helper)."""
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
    """Today's (day index 0) day-header fragment (mirrors
    ``tests/test_forecast_extrema_coverage.py``'s identically-named
    helper)."""
    return html.split('class="dh-cell ds"')[1]


def _precip_day_text(html: str) -> str:
    """The Precipitation grid's top-row day-column text (``page.rain_text``)
    -- card 7's replacement for the old tile row's "Rain" ``<strong>``.
    Scoped to the Precipitation ``<table>`` (the selected day's only)."""
    section = html.split("Precipitation <span>mm/h</span>")[1]
    section = section.split("</table>")[0]
    start = section.index('<td class="day">') + len('<td class="day">')
    end = section.index("</td>", start)
    # The cell's visible text only -- the rain winner's markup (C4) wraps it
    # in a <small> and, with an agreement percentage, a
    # <span title="Agreement">: strip every tag, keep what a reader sees.
    return re.sub(r"<[^>]+>", "", section[start:end]).strip()


def _candidate(
    feed_id: int,
    *,
    covered_hours: int,
    extrema_eligible: bool,
    confident: bool = True,
    skill_score: float | None = 1.0,
    pair_n: int = 0,
    mae: float | None = None,
    future_sample_count: int = 0,
    source: str = "open-meteo",
    model: str = "ecmwf_ifs",
) -> CellCandidate:
    return CellCandidate(
        feed_id=feed_id,
        source=source,
        model=model,
        confident=confident,
        skill_score=skill_score,
        pair_n=pair_n,
        mae=mae,
        future_sample_count=future_sample_count,
        covered_hours=covered_hours,
        extrema_eligible=extrema_eligible,
    )


# ===========================================================================
# G-T1 -- G-T2: eligibility precedes ranking and depth (pure, no DB).
# ===========================================================================


def test_complete_vs_partial_extrema_eligibility_pure() -> None:
    complete = _candidate(1, covered_hours=24, extrema_eligible=True)
    selection = select_cell_feeds(
        [complete], blend_depth=1, extrema_coverage_required=True
    )
    assert [c.feed_id for c in selection.extrema_feeds] == [1]
    assert selection.extrema_coverage == EXTREMA_COVERAGE_COMPLETE

    partial = _candidate(2, covered_hours=23, extrema_eligible=False)
    partial_selection = select_cell_feeds(
        [partial], blend_depth=1, extrema_coverage_required=True
    )
    assert partial_selection.extrema_feeds == []
    assert partial_selection.extrema_coverage == EXTREMA_COVERAGE_INSUFFICIENT


def test_eligibility_precedes_ranking_and_depth_pure() -> None:
    # Depth 1, a complete feed ranked BELOW a higher-skill incomplete one --
    # the incomplete feed wins the blend set, but the complete feed alone
    # supplies the daily rainfall.
    high_skill_incomplete = _candidate(
        1, covered_hours=20, extrema_eligible=False, confident=True, skill_score=0.9
    )
    lower_skill_complete = _candidate(
        2, covered_hours=24, extrema_eligible=True, confident=True, skill_score=0.1
    )
    selection = select_cell_feeds(
        [high_skill_incomplete, lower_skill_complete],
        blend_depth=1,
        extrema_coverage_required=True,
    )
    assert [c.feed_id for c in selection.feeds] == [1]
    assert [c.feed_id for c in selection.extrema_feeds] == [2]
    assert selection.extrema_coverage == EXTREMA_COVERAGE_COMPLETE


# ===========================================================================
# G-T3 -- mixed denominators.
# ===========================================================================


def test_mixed_denominators_partial_feed_never_reaches_displayed_values() -> None:
    conn = _make_db()
    complete_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    partial_id = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)

    complete_values = [0.5] * 6 + [0.0] * 18  # 24h, 6 wet
    _seed_varying(
        conn,
        feed_id=complete_id,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
        values=complete_values,
    )
    partial_values = [1.0] * 6 + [0.0] * 6  # 12h, 6 wet
    _seed_varying(
        conn,
        feed_id=partial_id,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 12),
        values=partial_values,
    )
    # Rain release, C4: the tile follows the published rain ranking's
    # winner. The partial feed is even the SELECTED model here; with no full
    # day it hands over to the complete feed, so it still never reaches the
    # displayed values.
    seed_rain_ranks(
        conn, site_id=1, published_at=now, ranked={0: [partial_id, complete_id]}
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    precip = view.tiles[0].precip

    assert precip.wet_hours == 6
    assert precip.total_mm == 3.0
    assert [ref.feed_id for ref in precip.meta.feeds] == [complete_id]

    # The pre-change path used a wet-hour SHARE (a proportion, not a count)
    # blended over both feeds; its magnitude and unit never coincide with the
    # shipped count.
    old_style_share = blend_mean([6 / 24, 6 / 12])
    assert old_style_share == 0.375
    assert old_style_share != precip.wet_hours

    # The pre-change path also aggregated over BOTH feeds' sums (the
    # clearing-subset path Item G retired for precip); the partial feed's
    # denominator must never leak into the shipped total.
    pooled_total = blend_mean([sum(complete_values), sum(partial_values)])
    assert pooled_total == 4.5
    assert precip.total_mm != pooled_total


# ===========================================================================
# G-T4 -- G-T5: 23h/25h DST days (Berlin).
# ===========================================================================


def test_23h_spring_forward_day_eligibility() -> None:
    start, end, _ = local_day_slots(date(2026, 3, 29), "Europe/Berlin")
    assert isoformat_utc(start) == "2026-03-28T23:00:00Z"
    assert isoformat_utc(end) == "2026-03-29T22:00:00Z"
    full = _instant_range("2026-03-28T23:00:00Z", 23)
    assert covers_local_day_exactly(
        full, local_date=date(2026, 3, 29), timezone="Europe/Berlin"
    )

    short = full[:-1]  # 22 instants
    assert not covers_local_day_exactly(
        short, local_date=date(2026, 3, 29), timezone="Europe/Berlin"
    )
    # The count is never scaled to 24: a feed supplying the 23 real hours
    # counts exactly 23 wet slots, not a fraction of 24.
    from wxverify.forecast.aggregate import wet_hours

    assert wet_hours([0.5] * 23, threshold_mm=0.2) == 23


def test_25h_fall_back_day_eligibility() -> None:
    start, end, _ = local_day_slots(date(2026, 10, 25), "Europe/Berlin")
    assert isoformat_utc(start) == "2026-10-24T22:00:00Z"
    assert isoformat_utc(end) == "2026-10-25T23:00:00Z"
    full = _instant_range("2026-10-24T22:00:00Z", 25)
    assert covers_local_day_exactly(
        full, local_date=date(2026, 10, 25), timezone="Europe/Berlin"
    )

    # 25 distinct on-the-hour UTC instants, including both of the fold's
    # repeated-local-02:00 instants (indices 2 and 3: 2026-10-25T00:00:00Z
    # is local 02:00 CEST, 2026-10-25T01:00:00Z is local 02:00 CET --
    # confirmed via zoneinfo, not assumed). 24 that omit either fold
    # instant are ineligible.
    for i in (2, 3):  # the two fold-hour UTC instants
        missing_one = full[:i] + full[i + 1 :]
        assert not covers_local_day_exactly(
            missing_one, local_date=date(2026, 10, 25), timezone="Europe/Berlin"
        )


# ===========================================================================
# G-T6 -- covers_local_day vs. covers_local_day_exactly diverge.
# ===========================================================================


def test_covers_local_day_vs_exactly_diverge_on_an_off_hour_extra() -> None:
    on_hour = _hours("2026-07-20", 0, 24)
    with_extra = [*on_hour, "2026-07-20T10:30:00Z"]
    assert covers_local_day(with_extra, local_date=date(2026, 7, 20), timezone="UTC")
    assert not covers_local_day_exactly(
        with_extra, local_date=date(2026, 7, 20), timezone="UTC"
    )

    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_uniform(
        conn,
        feed_id=feed_id,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=with_extra,
        value=1.0,
    )
    # Rain release, C4: the feed is the ranking's only model; with no exact
    # day it is no winner, so the tile has no rain forecast.
    seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [feed_id]})
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    precip = view.tiles[0].precip
    assert precip.total_mm is None
    assert precip.wet_hours is None
    assert precip.meta.state == "not_available"


# ===========================================================================
# G-T7 -- sub-hourly series is ineligible.
# ===========================================================================


def test_half_hourly_series_is_ineligible() -> None:
    half_hourly: list[str] = []
    dt = parse_utc("2026-07-20T00:00:00Z")
    for i in range(48):
        half_hourly.append(isoformat_utc(dt + timedelta(minutes=30 * i)))
    assert not covers_local_day_exactly(
        half_hourly, local_date=date(2026, 7, 20), timezone="UTC"
    )

    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_uniform(
        conn,
        feed_id=feed_id,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=half_hourly,
        value=1.0,
    )
    # Rain release, C4: the ranking's only model has no exact day -> no
    # winner.
    seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [feed_id]})
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    precip = view.tiles[0].precip
    assert precip.total_mm is None  # never 48.0 (a doubled sum)
    assert precip.meta.state == "not_available"


# ===========================================================================
# G-T8 -- a missing hour is not a dry hour.
# ===========================================================================


def test_missing_hour_is_not_a_dry_hour() -> None:
    all_but_one = _hours("2026-07-20", 0, 24)[:-1]  # 23 hours, every one dry
    assert not covers_local_day_exactly(
        all_but_one, local_date=date(2026, 7, 20), timezone="UTC"
    )

    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_uniform(
        conn,
        feed_id=feed_id,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=all_but_one,
        value=0.0,
    )
    # Rain release, C4: ranked, so the None below comes from the missing
    # hour, not from an absent ranking.
    seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [feed_id]})
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    precip = view.tiles[0].precip
    assert precip.total_mm is None
    assert precip.wet_hours is None
    assert precip.total_mm != 0
    assert precip.wet_hours != 0


# ===========================================================================
# G-T9 -- an all-dry complete day vs. insufficient coverage.
# ===========================================================================


def test_all_dry_complete_day_renders_differently_from_insufficient_coverage() -> None:
    """Rain release, C4: a day with no winner (insufficient coverage, or no
    rain candidate at all) reads "No rain forecast"; a winner's all-dry day
    reads "0.0 mm". The old "Daily rainfall unavailable" text and the plain
    dash are gone."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_uniform(
        conn,
        feed_id=feed_id,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
        value=0.0,
    )
    seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [feed_id]})
    conn.commit()
    dry_view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    dry = dry_view.tiles[0].precip
    assert dry.total_mm == 0.0
    assert dry.wet_hours == 0
    assert dry.meta.state == "normal"
    dry_page = _build_page(conn, now=now)
    dry_text = _precip_day_text(_render_tiles(site_id=1, page=dry_page))
    assert dry_text != NO_RAIN_TEXT
    assert dry_text == "0.0 mm · 0 h"

    empty_conn = _make_db()
    empty_view = build_forecast(
        empty_conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert empty_view.empty is True

    # Insufficient (not zero) coverage: the ranked feed is short of
    # ``covers_local_day_exactly``, so there is no winner.
    partial_conn = _make_db()
    partial_feed = _feed_id(partial_conn, "open-meteo", "ecmwf_ifs")
    _seed_uniform(
        partial_conn,
        feed_id=partial_feed,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 20),
        value=0.0,
    )
    # A temperature feed keeps the tile available: with no rain winner and
    # nothing else the whole tile reads "Not available yet".
    _seed_uniform(
        partial_conn,
        feed_id=_feed_id(partial_conn, "open-meteo", "gfs_global"),
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
        value=10.0,
    )
    seed_rain_ranks(
        partial_conn, site_id=1, published_at=now, ranked={0: [partial_feed]}
    )
    partial_conn.commit()
    partial_view = build_forecast(
        partial_conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    partial = partial_view.tiles[0].precip
    assert partial.total_mm is None
    assert partial.wet_hours is None
    assert partial.meta.state == "not_available"
    partial_text = _precip_day_text(
        _render_tiles(site_id=1, page=_build_page(partial_conn, now=now))
    )
    assert partial_text == NO_RAIN_TEXT
    assert " mm" not in partial_text
    # A forecast of no rain must never look the same as insufficient coverage.
    assert (dry.total_mm, dry.wet_hours) != (partial.total_mm, partial.wet_hours)

    # The OTHER end: a ranked feed with no rain candidate at all (its only
    # samples are temperature) reads the same "No rain forecast", never a
    # zero.
    no_precip_conn = _make_db()
    temp_feed = _feed_id(no_precip_conn, "open-meteo", "ecmwf_ifs")
    _seed_uniform(
        no_precip_conn,
        feed_id=temp_feed,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
        value=10.0,
    )
    seed_rain_ranks(
        no_precip_conn, site_id=1, published_at=now, ranked={0: [temp_feed]}
    )
    no_precip_conn.commit()
    no_precip_view = build_forecast(
        no_precip_conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    no_precip = no_precip_view.tiles[0].precip
    assert no_precip.meta.state == "not_available"
    assert no_precip.total_mm is None
    no_precip_text = _precip_day_text(
        _render_tiles(site_id=1, page=_build_page(no_precip_conn, now=now))
    )
    assert no_precip_text == NO_RAIN_TEXT


# ===========================================================================
# G-T10 -- the reconciliation invariant on a 24h day.
# ===========================================================================


def test_reconciliation_invariant_24h_two_feeds() -> None:
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    ats = _hours("2026-07-20", 0, 24)
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=ats,
        values=[0.1 * (i % 4) for i in range(24)],
    )
    _seed_varying(
        conn,
        feed_id=feed_b,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=ats,
        values=[0.05 * i for i in range(24)],
    )
    # Rain release, C4: the tile and the drill-down's line are the selected
    # model's (B here, ranked first), not a blend of both.
    seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [feed_b, feed_a]})
    conn.commit()

    import pytest

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[0]
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    series = payload["blend"]["precip_mm"]  # type: ignore[index]
    assert isinstance(series, list)
    total = sum(v for v in series if v is not None)
    assert total == pytest.approx(tile.precip.total_mm, abs=1e-9)
    assert total == pytest.approx(sum(0.05 * i for i in range(24)), abs=1e-9)

    precip_aggregate = payload["precip_aggregate"]  # type: ignore[index]
    assert isinstance(precip_aggregate, dict)
    assert precip_aggregate["feed_ids"] == [feed_b]
    assert [ref.feed_id for ref in tile.precip.meta.feeds] == [feed_b]

    # A same-sized but differently-composed ranking yields different ids.
    other_conn = _make_db()
    feed_c = _feed_id(other_conn, "open-meteo", "gfs_global")
    feed_d = _feed_id(other_conn, "open-meteo", "icon_global")
    _seed_varying(
        other_conn,
        feed_id=feed_c,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=ats,
        values=[0.1 * (i % 4) for i in range(24)],
    )
    _seed_varying(
        other_conn,
        feed_id=feed_d,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=ats,
        values=[0.05 * i for i in range(24)],
    )
    seed_rain_ranks(
        other_conn, site_id=1, published_at=now, ranked={0: [feed_d, feed_c]}
    )
    other_conn.commit()
    other_payload = build_hourly(other_conn, site_id=1, timezone="UTC", day=0, now=now)
    other_aggregate = other_payload["precip_aggregate"]  # type: ignore[index]
    assert isinstance(other_aggregate, dict)
    assert other_aggregate["feed_ids"] != precip_aggregate["feed_ids"]
    assert len(other_aggregate["feed_ids"]) == len(precip_aggregate["feed_ids"])


# ===========================================================================
# G-T11 -- the invariant on 23h and 25h local days.
# ===========================================================================


def test_reconciliation_invariant_23h_and_25h_days() -> None:
    import pytest

    for local_date, start, count in (
        (date(2026, 3, 29), "2026-03-28T23:00:00Z", 23),
        (date(2026, 10, 25), "2026-10-24T22:00:00Z", 25),
    ):
        conn = _make_db(timezone="Europe/Berlin")
        feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
        feed_b = _feed_id(conn, "open-meteo", "gfs_global")
        ats = _instant_range(start, count)
        issued_at = isoformat_utc(parse_utc(start) - timedelta(hours=3))
        _seed_varying(
            conn,
            feed_id=feed_a,
            variable="precip",
            issued_at=issued_at,
            valid_ats=ats,
            values=[0.1 * (i % 4) for i in range(count)],
        )
        _seed_varying(
            conn,
            feed_id=feed_b,
            variable="precip",
            issued_at=issued_at,
            valid_ats=ats,
            values=[0.05 * i for i in range(count)],
        )
        now = datetime(
            local_date.year, local_date.month, local_date.day, 12, tzinfo=UTC
        )
        # Rain release, C4: the line and the total are the winner's (B).
        seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [feed_b, feed_a]})
        conn.commit()
        view = build_forecast(
            conn, site_id=1, timezone="Europe/Berlin", rain_threshold_mm=0.2, now=now
        )
        payload = build_hourly(
            conn, site_id=1, timezone="Europe/Berlin", day=0, now=now
        )
        series = payload["blend"]["precip_mm"]  # type: ignore[index]
        assert isinstance(series, list)
        assert len(payload["hours"]) == count  # type: ignore[arg-type]
        total = sum(v for v in series if v is not None)
        assert total == pytest.approx(view.tiles[0].precip.total_mm, abs=1e-9)
        assert total == pytest.approx(sum(0.05 * i for i in range(count)), abs=1e-9)


# ===========================================================================
# G-T12 -- an axis instant no member supplies is null, and the drill-down's
# total agrees with the rendered tile (the underlying null-vs-renormalise
# rule is pinned directly against `fixed_membership_series` in
# test_forecast_aggregate.py; this off-hour instant comes from a temperature
# feed, so both precip members lack it and cannot distinguish null-by-rule
# from a renormalising bug here).
# ===========================================================================


def test_aggregate_null_axis_instant_and_total_agrees_with_tile() -> None:
    import pytest

    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    temp_feed = _feed_id(conn, "open-meteo", "icon_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    ats = _hours("2026-07-20", 0, 24)
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=ats,
        values=[0.1 * (i % 4) for i in range(24)],
    )
    _seed_varying(
        conn,
        feed_id=feed_b,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=ats,
        values=[0.05 * i for i in range(24)],
    )
    # An off-hour instant on a temperature feed: never a precip contributor,
    # but it widens the drill-down's hour axis.
    _insert_sample(
        conn,
        feed_id=temp_feed,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_at="2026-07-20T10:30:00Z",
        lead_hours=15,
        value=3.0,
    )
    # Rain release, C4: the line is the winner's (A).
    seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [feed_a, feed_b]})
    conn.commit()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    hours = payload["hours"]  # type: ignore[index]
    series = payload["blend"]["precip_mm"]  # type: ignore[index]
    assert isinstance(hours, list)
    assert isinstance(series, list)
    off_hour_index = hours.index("2026-07-20T10:30:00Z")
    assert series[off_hour_index] is None

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    total = sum(v for v in series if v is not None)
    assert total == pytest.approx(view.tiles[0].precip.total_mm, abs=1e-9)


# ===========================================================================
# G-T13 -- the axis reaches the rain winner outside the ladder's set (C4).
# ===========================================================================


def test_axis_widens_for_the_rain_winner_outside_the_ladder_set() -> None:
    import pytest

    # Rain release, C4: the axis reaches the rain WINNER when it is outside
    # the ladder's precipitation set (the per-feed lines). With the daily
    # close on and no published precipitation ranking, the ladder ranks by
    # sample count, so the 20-hour feed gets more samples (five extra
    # half-hours) and is the ladder's depth-1 pick. It is also the rain
    # ranking's selected model, but with no full day it hands over to the
    # 24-hour feed, the winner, whose hours 20-23 widen the axis.
    conn = _make_db()
    ineligible_high_skill = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    eligible_low_skill = _feed_id(conn, "open-meteo", "gfs_global")
    set_setting(conn, "forecast_blend_depth", "1")
    now = datetime(2026, 7, 20, 23, 0, tzinfo=UTC)
    ats20 = _hours("2026-07-20", 0, 20) + [
        f"2026-07-20T{h:02d}:30:00Z" for h in range(5)
    ]
    ats24 = _hours("2026-07-20", 0, 24)
    _seed_varying(
        conn,
        feed_id=ineligible_high_skill,
        variable="precip",
        issued_at="2026-07-20T20:00:00Z",
        valid_ats=ats20,
        values=[0.1 * i for i in range(25)],
    )
    _seed_varying(
        conn,
        feed_id=eligible_low_skill,
        variable="precip",
        issued_at="2026-07-20T20:00:00Z",
        valid_ats=ats24,
        values=[0.05 * i for i in range(24)],
    )
    seed_rain_ranks(
        conn,
        site_id=1,
        published_at=now,
        ranked={0: [ineligible_high_skill, eligible_low_skill]},
    )
    conn.commit()

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    hours = payload["hours"]  # type: ignore[index]
    assert isinstance(hours, list)
    # Fixture guard: the ladder's own precipitation set is the 20-hour feed
    # alone, so hours 20-23 can come only from the winner.
    feeds_payload = payload["feeds"]  # type: ignore[index]
    assert isinstance(feeds_payload, list)
    assert [int(f["feed_id"]) for f in feeds_payload] == [ineligible_high_skill]  # type: ignore[index]
    late_hours = _hours("2026-07-20", 20, 4)
    assert not set(late_hours) & set(ats20)
    assert set(late_hours) <= set(hours)  # widened for the winner
    assert len(hours) == 29  # the ladder feed's 25 instants + 4 of the winner's
    precip_aggregate = payload["precip_aggregate"]  # type: ignore[index]
    assert isinstance(precip_aggregate, dict)
    assert precip_aggregate["feed_ids"] == [eligible_low_skill]

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    series = payload["blend"]["precip_mm"]  # type: ignore[index]
    assert isinstance(series, list)
    total = sum(v for v in series if v is not None)
    assert total == pytest.approx(view.tiles[0].precip.total_mm, abs=1e-9)


# ===========================================================================
# G-T16 -- the rain glyph never shows on a suppressed cell.
# ===========================================================================


def test_rain_glyph_never_shows_on_a_suppressed_cell() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    # 20 hours, every one well above the wet threshold -- would show the
    # rain icon IF the feed were the winner, but it has no full day (only 20
    # of 24 hours), so the ranking's only model is no winner (rain release,
    # C4) and the cell must show no icon regardless.
    _seed_uniform(
        conn,
        feed_id=feed_id,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 20),
        value=5.0,
    )
    seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [feed_id]})
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    precip = view.tiles[0].precip
    assert precip.meta.state == "not_available"
    assert precip.wet_hours is None
    assert precip.icon is None
    assert precip.agreement_pct is None


# ===========================================================================
# G-T17 -- selections[variable].feeds unchanged when precip suppresses.
# ===========================================================================


def test_temperature_and_per_feed_series_survive_precip_suppression() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    partial_precip = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    ats = _hours("2026-07-20", 0, 24)
    _seed_varying(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=ats,
        values=[10.0 + i * 0.1 for i in range(24)],
    )
    partial_ats = _hours("2026-07-20", 0, 20)  # ineligible for precip's extrema
    _seed_varying(
        conn,
        feed_id=partial_precip,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=partial_ats,
        values=[0.1 * i for i in range(20)],
    )
    # Rain release, C4: ranked, but with no full day -> no winner.
    seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [partial_precip]})
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].precip.meta.state == "not_available"
    assert view.tiles[0].precip.total_mm is None

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    temp_blend = payload["blend"]["temp_c"]  # type: ignore[index]
    assert isinstance(temp_blend, list)
    assert any(v is not None for v in temp_blend)

    feeds_payload = payload["feeds"]  # type: ignore[index]
    assert isinstance(feeds_payload, list)
    feed_ids = {int(f["feed_id"]) for f in feeds_payload}  # type: ignore[index]
    assert feed_id in feed_ids  # temperature feed membership intact

    for f in feeds_payload:  # type: ignore[assignment]
        if int(f["feed_id"]) == partial_precip:  # type: ignore[index]
            precip_series = f["precip_mm"]  # type: ignore[index]
            assert isinstance(precip_series, list)
            assert any(v is not None for v in precip_series)  # per-feed line survives
            break
    else:
        raise AssertionError("partial precip feed missing from per-feed series")


# ===========================================================================
# G-T18 -- temperature reconciliation is NOT asserted (negative pin).
# ===========================================================================


def test_temperature_reconciliation_is_not_generalised() -> None:
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    ats = _hours("2026-07-20", 0, 24)
    # A peaks at hour 5 (30.0), B peaks at hour 15 (20.0).
    a_values = [30.0 if i == 5 else 10.0 for i in range(24)]
    b_values = [20.0 if i == 15 else 10.0 for i in range(24)]
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=ats,
        values=a_values,
    )
    _seed_varying(
        conn,
        feed_id=feed_b,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=ats,
        values=b_values,
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[0]

    mean_of_maxima = displayed_daily(
        "temperature", [a_values, b_values], rain_threshold_mm=0.2
    )["high_c"]
    assert mean_of_maxima == 25.0  # mean(30.0, 20.0)

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    temp_blend = payload["blend"]["temp_c"]  # type: ignore[index]
    assert isinstance(temp_blend, list)
    max_of_hourly_blend = max(v for v in temp_blend if v is not None)
    # The two figures genuinely differ for this fixture, so the tile keeping
    # the former is not vacuously true.
    assert mean_of_maxima != max_of_hourly_blend
    assert tile.temp.high_c == mean_of_maxima
    assert tile.temp.high_c != max_of_hourly_blend


# ===========================================================================
# G-T22 -- a partial-only day is never summed into a purported complete total.
# ===========================================================================


def test_partial_only_day_never_summed_into_a_complete_total() -> None:
    conn = _make_db()
    partial_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    partial_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    a_ats = _hours("2026-07-20", 0, 20)
    b_ats = _hours("2026-07-20", 0, 18)
    _seed_varying(
        conn,
        feed_id=partial_a,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=a_ats,
        values=[0.5] * 20,
    )
    _seed_varying(
        conn,
        feed_id=partial_b,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=b_ats,
        values=[0.5] * 18,
    )
    conn.commit()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    precip_aggregate = payload["precip_aggregate"]  # type: ignore[index]
    assert isinstance(precip_aggregate, dict)
    assert precip_aggregate["coverage"] == EXTREMA_COVERAGE_INSUFFICIENT
    assert precip_aggregate["feed_ids"] == []
    series = payload["blend"]["precip_mm"]  # type: ignore[index]
    assert isinstance(series, list)
    assert all(v is None for v in series)

    feeds_payload = payload["feeds"]  # type: ignore[index]
    assert isinstance(feeds_payload, list)
    for f in feeds_payload:  # type: ignore[assignment]
        precip_series = f["precip_mm"]  # type: ignore[index]
        assert isinstance(precip_series, list)
        assert any(v is not None for v in precip_series)

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile_precip = view.tiles[0].precip
    assert tile_precip.total_mm is None
    sum_of_per_feed = sum(
        v for f in feeds_payload for v in f["precip_mm"] if v is not None
    )  # type: ignore[index]
    assert sum_of_per_feed != 0  # non-vacuity: there IS a nonzero sum, just never shown
    assert tile_precip.total_mm != sum_of_per_feed


# ===========================================================================
# G-T23 -- G-T26: the tile's warnings for precipitation.
# ===========================================================================

_AB_VALID_DAY = "2026-07-20"
_AB_NOW = datetime(2026, 7, 20, 23, 0, tzinfo=UTC)
_AB_FRESH_ISSUED = "2026-07-20T20:00:00Z"
_AB_STALE_ISSUED = "2026-07-20T02:00:00Z"
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


def _seed_ab_precip_fixture(
    conn: sqlite3.Connection,
    *,
    feed_a_issued_at: str = _AB_FRESH_ISSUED,
    feed_b_issued_at: str = _AB_FRESH_ISSUED,
    seed_b_scoring: bool = True,
) -> tuple[int, int]:
    """G.11's shared precipitation fixture: A (20h, better skill, blend
    winner, NOT extrema-eligible) beside B (24h exactly-once, the
    precipitation contributor set). Mirrors
    ``tests/test_forecast_extrema_coverage.py``'s ``_seed_ab_fixture``.
    """
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")

    a_ats = _hours(_AB_VALID_DAY, 0, 20)
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="precip",
        issued_at=feed_a_issued_at,
        valid_ats=a_ats,
        values=[0.1 * i for i in range(20)],
    )
    b_ats = _hours(_AB_VALID_DAY, 0, 24)
    _seed_varying(
        conn,
        feed_id=feed_b,
        variable="precip",
        issued_at=feed_b_issued_at,
        valid_ats=b_ats,
        values=[0.05 * i for i in range(24)],
    )
    _stamp_usable_fetch(conn, feed_id=feed_a, at=feed_a_issued_at)
    _stamp_usable_fetch(conn, feed_id=feed_b, at=feed_b_issued_at)

    pairs: dict[int, tuple[float | Sequence[float], float | Sequence[float]]] = {
        persistence_id: (_PRECIP_POOR_FORECAST, _PRECIP_MIXED_OBSERVED),
        feed_a: (_PRECIP_ACCURATE_FORECAST, _PRECIP_MIXED_OBSERVED),
    }
    if seed_b_scoring:
        # Wet-always: mid-table skill, strictly below A's perfect skill.
        pairs[feed_b] = ((1.0, 1.0, 1.0), _PRECIP_MIXED_OBSERVED)
    _seed_score_cache(conn, day_ahead=0, pairs=pairs)
    return feed_a, feed_b


def test_stale_union_for_precipitation() -> None:
    """Rain release, C4: the rain cell's stale flag covers the feeds the
    tile's rain uses -- the winner and the agreement members -- and no
    other. Each case ranks A (20 h, the selected model, no full day) first
    and B (24 h, the winner) second.

    Every case seeds ``seed_rain_ranks`` BEFORE ``_seed_ab_precip_fixture``:
    both create a publication if none exists yet, and ``active_publication``
    picks whichever has the LATEST cutoff, never the most recently
    published -- seeding the AB fixture second lets its far-future
    ``_ensure_test_publication`` cutoff (2035-06-30) reuse the rain
    fixture's publication instead of out-cutoffing it, so the rain ranking
    stays visible to the same read.
    """
    # Case 1: the winner B is stale -> stale.
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    seed_rain_ranks(conn, site_id=1, published_at=_AB_NOW, ranked={0: [feed_a, feed_b]})
    _seed_ab_precip_fixture(
        conn, feed_a_issued_at=_AB_FRESH_ISSUED, feed_b_issued_at=_AB_STALE_ISSUED
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert [ref.feed_id for ref in today.precip.meta.feeds] == [feed_b]
    assert today.precip.meta.stale is True
    assert today.stale is True
    page = _build_page(conn, now=_AB_NOW)
    assert STALE_TEXT in page.headers[0].warnings
    html = _render_tiles(site_id=1, page=page)
    assert STALE_TEXT in _day0_header_html(html)

    # Case 2: only A is stale. A is no contributor (no full day), so the
    # rain is not stale; A is the selected model, though, and B stands in
    # for it, so the cell is a substitute.
    conn2 = _make_db()
    feed_a2 = _feed_id(conn2, "open-meteo", "ecmwf_ifs")
    feed_b2 = _feed_id(conn2, "open-meteo", "gfs_global")
    seed_rain_ranks(
        conn2, site_id=1, published_at=_AB_NOW, ranked={0: [feed_a2, feed_b2]}
    )
    _seed_ab_precip_fixture(
        conn2, feed_a_issued_at=_AB_STALE_ISSUED, feed_b_issued_at=_AB_FRESH_ISSUED
    )
    conn2.commit()
    view2 = build_forecast(
        conn2, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    assert [ref.feed_id for ref in view2.tiles[0].precip.meta.feeds] == [feed_b2]
    assert view2.tiles[0].precip.meta.stale is False
    assert view2.tiles[0].precip.meta.substitute is True

    # Case 3: the winner B is fresh, and a stale third feed C with a full day
    # is an agreement member -> stale.
    conn3 = _make_db()
    feed_a3 = _feed_id(conn3, "open-meteo", "ecmwf_ifs")
    feed_b3 = _feed_id(conn3, "open-meteo", "gfs_global")
    feed_c3 = _feed_id(conn3, "open-meteo", "icon_global")
    seed_rain_ranks(
        conn3, site_id=1, published_at=_AB_NOW, ranked={0: [feed_b3, feed_c3, feed_a3]}
    )
    _seed_ab_precip_fixture(
        conn3, feed_a_issued_at=_AB_FRESH_ISSUED, feed_b_issued_at=_AB_FRESH_ISSUED
    )
    _seed_varying(
        conn3,
        feed_id=feed_c3,
        variable="precip",
        issued_at=_AB_STALE_ISSUED,
        valid_ats=_hours(_AB_VALID_DAY, 0, 24),
        values=[0.0] * 24,
    )
    _stamp_usable_fetch(conn3, feed_id=feed_c3, at=_AB_STALE_ISSUED)
    conn3.commit()
    view3 = build_forecast(
        conn3, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    precip3 = view3.tiles[0].precip
    assert [ref.feed_id for ref in precip3.meta.feeds] == [feed_b3]
    assert precip3.agreement_pct is not None  # fixture guard: C is a member
    assert precip3.meta.stale is True
    page3 = _build_page(conn3, now=_AB_NOW)
    assert STALE_TEXT in page3.headers[0].warnings
    html3 = _render_tiles(site_id=1, page=page3)
    assert STALE_TEXT in _day0_header_html(html3)


def test_confident_blend_set_with_unscored_precip_extrema_set() -> None:
    """Tile half, rain release C4: the rain cell has one feed set, the
    winner, and no leaderboard verdict. B (no precipitation pairs, the old
    unscored extrema set) is the winner of a settled rain ranking: both of
    the cell's states read normal, and no confidence badge shows.

    ``seed_rain_ranks`` runs BEFORE ``_seed_ab_precip_fixture`` for the same
    reason given in ``test_stale_union_for_precipitation``: both create a
    publication if none exists, and the active one is picked by the
    LATEST cutoff, not by which was published more recently.
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    seed_rain_ranks(conn, site_id=1, published_at=_AB_NOW, ranked={0: [feed_a, feed_b]})
    _seed_ab_precip_fixture(conn, seed_b_scoring=False)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert [ref.feed_id for ref in today.precip.meta.feeds] == [feed_b]
    assert today.precip.meta.state == "normal"
    assert today.precip.meta.extrema_state == "normal"
    assert today.state == "normal"
    assert today.confidence_state == "normal"
    page = _build_page(conn, now=_AB_NOW)
    header = page.headers[0]
    assert header.not_available is False
    assert LOW_CONFIDENCE_TEXT not in header.warnings
    assert RANKING_UPDATING_TEXT not in header.warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert "state-not_available" not in frag
    assert LOW_CONFIDENCE_TEXT not in frag
    assert RANKING_UPDATING_TEXT not in frag


def test_rebuilding_precedence_on_the_precip_extrema_set() -> None:
    """Tile half, rain release C4: a PROVISIONAL rain ranking whose winner
    is B shows as the rain cell's rebuilding extrema state: the tile stays
    state-normal and shows "ranking updating", never "low confidence"."""
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")

    # The daily close is permanent now, so `leaderboard_with_status` never
    # reports "rebuilding" for an active publication (status is hit, live
    # or empty); the guard that used to distinguish day_ahead=0 ("hit") from
    # day_ahead=1 ("rebuilding") here pinned that removed per-cell status
    # and is gone with it. The rain cell never reads the leaderboard anyway
    # (rain release, C4) -- its own "rebuilding" below comes from the rain
    # ranking's own provisional flag, asserted further down.
    #
    # ``seed_rain_ranks`` runs BEFORE the ``_seed_score_cache``
    # setup below: both create a publication if none exists yet, and
    # ``active_publication`` picks whichever has the LATEST cutoff, never
    # the most recently published -- seeding the score-cache publication
    # second lets its far-future ``_ensure_test_publication`` cutoff
    # (2035-06-30) reuse the rain fixture's publication instead of
    # out-cutoffing it, so the rain ranking stays visible to the same read.
    seed_rain_ranks(
        conn,
        site_id=1,
        published_at=_AB_NOW,
        ranked={0: [feed_a, feed_b]},
        provisional={0},
    )

    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="precip",
        issued_at="2026-07-20T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 20),
        values=[0.1 * i for i in range(20)],
    )
    _seed_score_cache(
        conn,
        day_ahead=0,
        pairs={
            persistence_id: (_PRECIP_POOR_FORECAST, _PRECIP_MIXED_OBSERVED),
            feed_a: (_PRECIP_ACCURATE_FORECAST, _PRECIP_MIXED_OBSERVED),
        },
    )

    _seed_varying(
        conn,
        feed_id=feed_b,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
        values=[0.05 * i for i in range(24)],
    )
    # feed_b has no ranking row at day_ahead=1 -- unconfident there.
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert [ref.feed_id for ref in today.precip.meta.feeds] == [feed_b]
    assert today.precip.meta.state == "normal"
    assert today.precip.meta.extrema_state == "rebuilding"
    assert today.state == "normal"
    assert today.confidence_state == "rebuilding"
    page = _build_page(conn, now=_AB_NOW)
    header = page.headers[0]
    assert header.not_available is False
    assert RANKING_UPDATING_TEXT in header.warnings
    assert LOW_CONFIDENCE_TEXT not in header.warnings
    html = _render_tiles(site_id=1, page=page)
    frag = _day0_header_html(html)
    assert "state-not_available" not in frag
    assert RANKING_UPDATING_TEXT in frag
    assert LOW_CONFIDENCE_TEXT not in frag


def test_suppressed_precip_extrema_contribute_to_neither_warning() -> None:
    """Rain release, C4: a ranked model with no full day is no winner and
    feeds no warning. A is stale and its ranking provisional, yet the tile
    (kept available by a normal wind cell) shows neither the stale nor the
    "ranking updating" badge. Paired positive: the same A with a full day
    is the winner, and both badges show, so the negative is not a blanket
    "badges never fire" fixture."""
    for full_day in (False, True):
        conn = _make_db()
        feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
        wind_feeds = (
            _feed_id(conn, "open-meteo", "gfs_global"),
            _feed_id(conn, "open-meteo", "icon_global"),
        )
        hours = 24 if full_day else 20
        _seed_varying(
            conn,
            feed_id=feed_a,
            variable="precip",
            issued_at=_AB_STALE_ISSUED,
            valid_ats=_hours(_AB_VALID_DAY, 0, hours),
            values=[0.1 * i for i in range(hours)],
        )
        _stamp_usable_fetch(conn, feed_id=feed_a, at=_AB_STALE_ISSUED)
        for wind_feed in wind_feeds:
            _seed_uniform(
                conn,
                feed_id=wind_feed,
                variable="wind",
                issued_at=_AB_FRESH_ISSUED,
                valid_ats=_hours(_AB_VALID_DAY, 0, 24),
                value=5.0,
            )
            _stamp_usable_fetch(conn, feed_id=wind_feed, at=_AB_FRESH_ISSUED)
        publication_id = seed_rain_ranks(
            conn,
            site_id=1,
            published_at=_AB_NOW,
            ranked={0: [feed_a]},
            provisional={0},
        )
        for wind_feed in wind_feeds:
            conn.execute(
                "INSERT INTO publication_wind_weights"
                " (publication_id, feed_id, day_ahead, mae_ms, days, weight)"
                " VALUES (?, ?, 0, 1.0, 30, 1.0)",
                (publication_id, wind_feed),
            )
        conn.commit()
        view = build_forecast(
            conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
        )
        today = view.tiles[0]
        page = _build_page(conn, now=_AB_NOW)
        header = page.headers[0]
        html = _render_tiles(site_id=1, page=page)
        frag = _day0_header_html(html)
        assert today.wind.meta.state == "normal"  # fixture guard: tile shows
        assert today.wind.meta.stale is False
        if full_day:
            assert [ref.feed_id for ref in today.precip.meta.feeds] == [feed_a]
            assert today.precip.meta.stale is True
            assert today.precip.meta.extrema_state == "rebuilding"
            assert STALE_TEXT in header.warnings
            assert RANKING_UPDATING_TEXT in header.warnings
            assert STALE_TEXT in frag
            assert RANKING_UPDATING_TEXT in frag
        else:
            assert today.precip.meta.state == "not_available"
            assert today.precip.meta.extrema_state == "not_available"
            assert today.precip.meta.stale is False
            assert today.stale is False
            assert today.confidence_state == "normal"
            assert LOW_CONFIDENCE_TEXT not in header.warnings
            assert RANKING_UPDATING_TEXT not in header.warnings
            assert STALE_TEXT not in header.warnings
            assert LOW_CONFIDENCE_TEXT not in frag
            assert RANKING_UPDATING_TEXT not in frag
            assert STALE_TEXT not in frag


# ===========================================================================
# G-T27 -- G-T28: exact membership on a half-hour DST shift (Lord Howe).
# ===========================================================================


def test_lord_howe_fall_back_day_exact_membership() -> None:
    # 2026-04-05: window [2026-04-04T13:00:00Z, 2026-04-05T13:30:00Z),
    # requiring the 25 on-the-hour instants 2026-04-04T13:00Z..2026-04-05T13:00Z.
    start, end, floored = local_day_slots(date(2026, 4, 5), "Australia/Lord_Howe")
    assert isoformat_utc(start) == "2026-04-04T13:00:00Z"
    assert isoformat_utc(end) == "2026-04-05T13:30:00Z"
    assert floored == 24  # the trap: floors to 24, but 25 on-hour instants qualify

    required = _instant_range("2026-04-04T13:00:00Z", 25)
    local_date = date(2026, 4, 5)
    tz = "Australia/Lord_Howe"

    # (1) all 25 is eligible.
    assert covers_local_day_exactly(required, local_date=local_date, timezone=tz)

    # (2) dropping any single one of the 25 is ineligible, each in turn.
    for i in range(25):
        missing = required[:i] + required[i + 1 :]
        assert not covers_local_day_exactly(
            missing, local_date=local_date, timezone=tz
        ), f"instant {i} ({required[i]}) must be required"

    # (3a) a repeat standing in for the last instant: 25 samples, 24 distinct.
    repeat_in_place = required[:-1] + [required[-2]]
    assert not covers_local_day_exactly(
        repeat_in_place, local_date=local_date, timezone=tz
    )
    # (3b) all 25 plus a second copy of an interior instant: 26 samples, 25 distinct.
    repeat_extra = [*required, required[10]]
    assert not covers_local_day_exactly(
        repeat_extra, local_date=local_date, timezone=tz
    )

    # (4) an off-hour replacement: still 25 samples but one is not on-the-hour.
    off_hour = list(required)
    off_hour_index = required.index("2026-04-04T20:00:00Z")
    off_hour[off_hour_index] = "2026-04-04T20:30:00Z"
    assert not covers_local_day_exactly(off_hour, local_date=local_date, timezone=tz)
    # In the same test, covers_local_day (the looser predicate) accepts it:
    # it still supplies all 24 truncated-to-hour required instants via the
    # other 24 on-hour samples plus this one off-hour extra that lands
    # inside the window.
    assert covers_local_day(off_hour, local_date=local_date, timezone=tz)

    # (5) endpoint substitutions: each keeps 25 samples but is ineligible.
    first_swap = ["2026-04-04T12:00:00Z", *required[1:]]
    assert len(first_swap) == 25
    assert not covers_local_day_exactly(first_swap, local_date=local_date, timezone=tz)
    last_swap = [*required[:-1], "2026-04-05T14:00:00Z"]
    assert len(last_swap) == 25
    assert not covers_local_day_exactly(last_swap, local_date=local_date, timezone=tz)


def test_lord_howe_spring_forward_day_control() -> None:
    # 2026-10-04: window [2026-10-03T13:30:00Z, 2026-10-04T13:00:00Z),
    # requiring the 23 on-the-hour instants 2026-10-03T14:00Z..2026-10-04T12:00Z.
    # Floored count agrees with the required set here (both 23) -- this day
    # pins that a half-hour-start window is judged the same way G-T4 judges
    # a whole-hour one.
    start, end, floored = local_day_slots(date(2026, 10, 4), "Australia/Lord_Howe")
    assert isoformat_utc(start) == "2026-10-03T13:30:00Z"
    assert isoformat_utc(end) == "2026-10-04T13:00:00Z"
    assert floored == 23

    required = _instant_range("2026-10-03T14:00:00Z", 23)
    local_date = date(2026, 10, 4)
    tz = "Australia/Lord_Howe"
    assert covers_local_day_exactly(required, local_date=local_date, timezone=tz)

    interior_dropped = [at for at in required if at != "2026-10-04T01:00:00Z"]
    assert len(interior_dropped) == 22
    assert not covers_local_day_exactly(
        interior_dropped, local_date=local_date, timezone=tz
    )
