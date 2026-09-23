"""Item G -- daily rain from a fixed set of feeds (plan G.11, G-T1-G-T28).

Precipitation adopts the same coverage-before-ranking discipline temperature
got under Item F, but with a STRICTER predicate: an extrema-eligible feed
must supply each hour of the local day EXACTLY once
(:func:`wxverify.forecast.aggregate.covers_local_day_exactly`), because a
daily total and a wet-hour count need one value per hour, not merely "at
least one" the way an extremum does. ``wet_hours`` replaces the old
percentage-based ``wet_share``/``chance_pct`` with a plain count.

G-T29 through G-T31 (the forecast-of-record's precipitation
``hourly_values``) live in ``tests/test_record_oracles.py`` beside the
Item G record oracles that share their fixture. G-T14/G-T15 (``wet_hours``'s
own arithmetic) live in ``tests/test_forecast_aggregate.py``, the successors
to the deleted ``wet_share``/``predicted_wet_hour_share`` tests.

All fixtures are synthetic and offline (the autouse ``_deny_network`` fixture
in ``conftest.py`` fails any test that opens a socket): site timezone UTC
except the DST cases, which use ``Europe/Berlin`` (G-T4, G-T5, G-T11) and
``Australia/Lord_Howe`` (G-T27, G-T28) per G.11; coordinates ``0.0/0.0``;
``rain_threshold_mm=0.2``. Every expected value is derived from reading the
body of the named production function, never transcribed from the plan
document.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

from wxverify.core.timeutil import isoformat_utc, local_day_slots, parse_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.forecast.aggregate import (
    EXTREMA_COVERAGE_COMPLETE,
    EXTREMA_COVERAGE_INSUFFICIENT,
    blend_mean,
    covers_local_day,
    covers_local_day_exactly,
    displayed_daily,
)
from wxverify.forecast.selection import CellCandidate, select_cell_feeds
from wxverify.forecast.service import (
    build_forecast,
    build_hourly,
)
from wxverify.scoring.leaderboard import leaderboard_with_status, resolve_window
from wxverify.scoring.metrics import strategy_for
from wxverify.scoring.pair_flags import precip_flags
from wxverify.settings.keys import get_number_setting, set_setting
from wxverify.verification.coverage import (
    EXCLUDE_BELOW_NEAR_COMPLETE,
    QUANTITY_PRECIP_OCCURRENCE,
    QUANTITY_PRECIP_TOTAL,
)
from wxverify.verification.methodology import METHODOLOGY_VERSION
from wxverify.verification.record import build_forecast_record, resolve_snapshot_utc
from wxverify.verification.runs import (
    RosterFeed,
    RunConfig,
    capture_config_snapshot,
    input_fingerprint,
    result_basis_fingerprint,
)
from wxverify.verification.simulate import (
    simulate_snapshot_day,
)
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
    rain_threshold_mm: float = 0.2,
    first_known_at: str | None = None,
) -> None:
    """Insert one scoring pair.

    ``first_known_at`` defaults to ``issued_at`` as a fixture convention;
    production's ``pair_real_models()`` instead stamps it from the source
    sample's own ``fetched_at``. It must be non-NULL and <= an as-of snapshot's ``T``
    for :func:`wxverify.verification.asof.knowable_pair_predicate` to treat
    the pair as knowable; leaving it NULL silently zeroes out every
    as-of-filtered leaderboard read (the record path), even though the
    live path (no ``as_of``) never applies the predicate at all.
    """
    error = forecast - observed
    hit, false, miss, correct_neg = precip_flags(
        variable,
        forecast,
        observed,
        rain_threshold_mm if variable == "precip" else None,
    )
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             cat_hit, cat_false, cat_miss, cat_correct_neg,
             first_known_at, tz_generation_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            hit,
            false,
            miss,
            correct_neg,
            first_known_at if first_known_at is not None else issued_at,
            ensure_published_generation(conn, site_id),
        ),
    )


_FAR_FUTURE_VALID_ATS = (
    "2035-07-01T00:00:00Z",
    "2035-07-01T01:00:00Z",
    "2035-07-01T02:00:00Z",
)
_FAR_FUTURE_LEAD_HOURS = (1, 2, 3)

# The record path (`build_forecast_record`) always passes an `as_of` snapshot
# time, which routes the ranking through `knowable_pair_predicate` instead of
# `score_cache` (forecast_ranking_with_status's as-of branch never reads the
# cache -- see its own docstring). `_FAR_FUTURE_VALID_ATS` sits after every
# `_RECORD_DAY` snapshot, so pairs seeded with it are never "knowable" on
# that path and silently stay unscored/not-confident regardless of
# `_seed_complete_score_cache`. Record-path scoring fixtures must use dates
# that are actually knowable before `_record_snapshot_t()`.
_RECORD_KNOWABLE_VALID_ATS = (
    "2035-06-15T00:00:00Z",
    "2035-06-15T01:00:00Z",
    "2035-06-15T02:00:00Z",
)
_RECORD_KNOWABLE_LEAD_HOURS = (4, 5, 6)
_RECORD_KNOWABLE_ISSUED_AT = "2035-06-14T20:00:00Z"

# A wet/dry-mixed observed baseline for precip's categorical (hit/false/
# miss/correct-negative) skill strategy: three pairs of the same category
# (all-wet or all-dry) degenerate to ETS=None (``confident=False`` no
# matter how many pairs or how low ``min_n`` is), so every precip
# confidence fixture in this module needs a mixed observed sequence, never
# a repeated scalar.
_PRECIP_MIXED_OBSERVED = (1.0, 0.0, 1.0)
_PRECIP_ACCURATE_FORECAST = (1.0, 0.0, 1.0)  # matches observed -> high skill
_PRECIP_POOR_FORECAST = (0.0, 1.0, 0.0)  # opposite of observed -> poor skill


def _seed_scoring_pairs(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    day_ahead: int,
    forecast: float | Sequence[float],
    observed: float | Sequence[float] = 10.0,
    valid_ats: Sequence[str] = _FAR_FUTURE_VALID_ATS,
    lead_hours_seq: Sequence[int] = _FAR_FUTURE_LEAD_HOURS,
    issued_at: str = "2035-06-30T00:00:00Z",
) -> None:
    """3 scoring pairs, far-future by default.

    ``forecast``/``observed`` are each either one value repeated for all 3
    pairs, or a 3-element sequence for per-pair control. Precipitation's
    categorical skill (:class:`wxverify.scoring.metrics.PrecipStrategy`)
    degenerates to ``ets=None`` (``confident=False``) when every pair falls
    in the same hit/miss category, so precip fixtures need an ``observed``
    sequence that mixes wet and dry outcomes -- a fixed 10.0 baseline (fine
    for temperature) is never enough on its own.

    Default ``valid_ats``/``issued_at`` are far in the future of any
    ``_AB_NOW``-style "now" used by the live (`build_forecast`) path. The
    record path (`build_forecast_record`) instead reads pairs knowable as
    of a fixed historical snapshot -- pass ``_RECORD_KNOWABLE_VALID_ATS``/
    ``_RECORD_KNOWABLE_LEAD_HOURS``/``_RECORD_KNOWABLE_ISSUED_AT`` there.
    """
    forecasts = [forecast] * 3 if isinstance(forecast, float | int) else list(forecast)
    observeds = [observed] * 3 if isinstance(observed, float | int) else list(observed)
    pairs = zip(valid_ats, lead_hours_seq, forecasts, observeds, strict=True)
    for valid_at, lead, fc, obs in pairs:
        _insert_pair(
            conn,
            feed_id=feed_id,
            variable=variable,
            issued_at=issued_at,
            valid_at=valid_at,
            lead_hours=lead,
            day_ahead=day_ahead,
            forecast=fc,
            observed=obs,
        )


def _seed_complete_score_cache(
    conn: sqlite3.Connection, *, variable: str, day_ahead: int
) -> None:
    """Fresh, complete score_cache snapshot for every feed with pairs at
    (site 1, `variable`, `day_ahead`).
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


def _render_tiles(*, site_id: int, view: object) -> str:
    tmpl = jinja_env.get_template("forecast/_tiles.html")
    return tmpl.render(
        url=lambda p: str(p), site=SimpleNamespace(id=site_id), view=view
    )


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
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    precip = view.tiles[0].precip
    assert precip.total_mm is None
    assert precip.wet_hours is None
    assert precip.meta.extrema_unavailable is True


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
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    precip = view.tiles[0].precip
    assert precip.total_mm is None  # never 48.0 (a doubled sum)
    assert precip.meta.extrema_unavailable is True


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
    conn.commit()
    dry_view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    dry = dry_view.tiles[0].precip
    assert dry.total_mm == 0.0
    assert dry.wet_hours == 0
    assert dry.meta.extrema_unavailable is False
    dry_html = _render_tiles(site_id=1, view=dry_view)
    assert (
        "Daily rainfall unavailable"
        not in (dry_html.split("Rain</span>")[1].split("</div>")[0])
    )

    empty_conn = _make_db()
    empty_view = build_forecast(
        empty_conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert empty_view.empty is True

    # Insufficient (not zero) coverage: a feed present but short of
    # ``covers_local_day_exactly`` -- the case ``extrema_unavailable``
    # actually models, distinct from having no precip candidate at all
    # (which renders a plain "-" per the template, not this labeled string).
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
    partial_conn.commit()
    partial_view = build_forecast(
        partial_conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    partial = partial_view.tiles[0].precip
    assert partial.total_mm is None
    assert partial.wet_hours is None
    assert partial.meta.extrema_unavailable is True
    partial_html = _render_tiles(site_id=1, view=partial_view)
    assert (
        "Daily rainfall unavailable"
        in (partial_html.split("Rain</span>")[1].split("</div>")[0])
    )
    # A forecast of no rain must never look the same as insufficient coverage.
    assert (dry.total_mm, dry.wet_hours) != (partial.total_mm, partial.wet_hours)

    # Paired positive at the OTHER end: a precip candidate is completely
    # absent (no candidate at all, as opposed to an insufficient one) -- the
    # template shows a plain dash, never the "unavailable" string, proving
    # the string is specific to the insufficient-coverage case above.
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
    no_precip_conn.commit()
    no_precip_view = build_forecast(
        no_precip_conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    no_precip = no_precip_view.tiles[0].precip
    assert no_precip.meta.extrema_unavailable is False
    assert no_precip.total_mm is None
    no_precip_html = _render_tiles(site_id=1, view=no_precip_view)
    assert (
        "Daily rainfall unavailable"
        not in (no_precip_html.split("Rain</span>")[1].split("</div>")[0])
    )


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

    precip_aggregate = payload["precip_aggregate"]  # type: ignore[index]
    assert isinstance(precip_aggregate, dict)
    assert precip_aggregate["feed_ids"] == [feed_a, feed_b]
    assert [ref.feed_id for ref in tile.precip.meta.feeds] == [feed_a, feed_b]

    # A same-sized but differently-composed eligible set yields different ids.
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
        conn.commit()
        now = datetime(
            local_date.year, local_date.month, local_date.day, 12, tzinfo=UTC
        )
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


# ===========================================================================
# G-T12 -- an axis instant no member supplies is null, and the drill-down's
# total agrees with the rendered tile (the underlying null-vs-renormalise
# rule is pinned directly against `fixed_membership_series` in
# test_forecast_aggregate.py; this off-hour instant comes from a wind feed,
# so both precip members lack it and cannot distinguish null-by-rule from
# a renormalising bug here).
# ===========================================================================


def test_aggregate_null_axis_instant_and_total_agrees_with_tile() -> None:
    import pytest

    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    wind_feed = _feed_id(conn, "open-meteo", "icon_global")
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
    # An off-hour instant on a wind feed: never a precip contributor, but it
    # widens the drill-down's hour axis.
    _insert_sample(
        conn,
        feed_id=wind_feed,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_at="2026-07-20T10:30:00Z",
        lead_hours=15,
        value=3.0,
    )
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
# G-T13 -- the axis reaches an eligible contributor ranked below blend_depth.
# ===========================================================================


def test_axis_widens_for_a_contributor_ranked_below_blend_depth() -> None:
    import pytest

    conn = _make_db()
    ineligible_high_skill = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    eligible_low_skill = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")
    now = datetime(2026, 7, 20, 23, 0, tzinfo=UTC)
    ats20 = _hours("2026-07-20", 0, 20)
    ats24 = _hours("2026-07-20", 0, 24)
    _seed_varying(
        conn,
        feed_id=ineligible_high_skill,
        variable="precip",
        issued_at="2026-07-20T20:00:00Z",
        valid_ats=ats20,
        values=[0.1 * i for i in range(20)],
    )
    _seed_varying(
        conn,
        feed_id=eligible_low_skill,
        variable="precip",
        issued_at="2026-07-20T20:00:00Z",
        valid_ats=ats24,
        values=[0.05 * i for i in range(24)],
    )
    _seed_scoring_pairs(
        conn, feed_id=persistence_id, variable="precip", day_ahead=0, forecast=8.0
    )
    _seed_scoring_pairs(
        conn,
        feed_id=ineligible_high_skill,
        variable="precip",
        day_ahead=0,
        forecast=10.02,
    )
    _seed_scoring_pairs(
        conn, feed_id=eligible_low_skill, variable="precip", day_ahead=0, forecast=10.2
    )
    _seed_complete_score_cache(conn, variable="precip", day_ahead=0)
    conn.commit()

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    hours = payload["hours"]  # type: ignore[index]
    assert isinstance(hours, list)
    assert len(hours) == 24  # widened for the eligible-but-unselected feed
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
    # 20 hours, every one well above the wet threshold -- would clear the
    # glyph threshold many times over IF the cell were available, but the
    # feed is not extrema-eligible (only 20 of 24 hours), so the cell is
    # suppressed and must show no glyph regardless.
    _seed_uniform(
        conn,
        feed_id=feed_id,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 20),
        value=5.0,
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    precip = view.tiles[0].precip
    assert precip.meta.extrema_unavailable is True
    assert precip.wet_hours is None
    assert precip.show_rain_glyph is False


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
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].precip.meta.extrema_unavailable is True

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
# G-T19 -- G-T20: the persisted row for a complete/suppressed day.
# ===========================================================================

_RECORD_DAY = date(2035, 6, 15)


def _record_conn() -> sqlite3.Connection:
    return _make_db()


def _record_snapshot_t(local_date: date = _RECORD_DAY) -> datetime:
    return resolve_snapshot_utc("UTC", local_date, "07:00")


def _record_insert_day(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    local_date: date,
    issued_at: str,
    valid_ats: list[datetime],
    value: float,
    variable: str = "precip",
) -> None:
    issued = parse_utc(issued_at)
    for valid in valid_ats:
        lead = max(1, int((valid - issued).total_seconds() // 3600))
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable=variable,
            issued_at=issued_at,
            valid_at=isoformat_utc(valid),
            lead_hours=lead,
            value=value,
        )


def test_persisted_row_complete_day() -> None:
    conn = _record_conn()
    ensure_published_generation(conn, 1)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    full_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(24)
    ]
    _record_insert_day(
        conn,
        feed_id=feed_id,
        local_date=_RECORD_DAY,
        issued_at="2035-06-14T20:00:00Z",
        valid_ats=full_ats,
        value=0.5,
    )
    conn.commit()
    t = _record_snapshot_t()
    build_forecast_record(conn, 1, _RECORD_DAY.isoformat(), now=t)
    conn.commit()
    view = build_forecast(conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=t)
    tile = view.tiles[0]

    row = conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = 1 AND variable = 'precip' AND display_lead = 0
        """
    ).fetchone()
    assert row is not None
    displayed = json.loads(str(row["daily_quantities"]))["displayed"]
    assert displayed["total_mm"] == tile.precip.total_mm
    wet_hours_raw = displayed["wet_hours"]
    assert isinstance(wet_hours_raw, float)
    assert round(wet_hours_raw) == tile.precip.wet_hours
    assert displayed["extrema_feed_ids"] == [feed_id]
    assert displayed["extrema_coverage"] == EXTREMA_COVERAGE_COMPLETE
    selection = select_cell_feeds(
        [
            CellCandidate(
                feed_id=feed_id,
                source="open-meteo",
                model="ecmwf_ifs",
                confident=False,
                skill_score=None,
                pair_n=0,
                mae=None,
                future_sample_count=24,
                covered_hours=24,
                extrema_eligible=True,
            )
        ],
        blend_depth=2,
        extrema_coverage_required=True,
    )
    assert displayed["extrema_low_confidence"] == selection.extrema_low_confidence
    assert "chance" not in displayed


def test_persisted_row_suppressed_day() -> None:
    conn = _record_conn()
    ensure_published_generation(conn, 1)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    partial_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(20)
    ]
    _record_insert_day(
        conn,
        feed_id=feed_id,
        local_date=_RECORD_DAY,
        issued_at="2035-06-14T20:00:00Z",
        valid_ats=partial_ats,
        value=0.5,
    )
    conn.commit()
    t = _record_snapshot_t()
    build_forecast_record(conn, 1, _RECORD_DAY.isoformat(), now=t)

    row = conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = 1 AND variable = 'precip' AND display_lead = 0
        """
    ).fetchone()
    assert row is not None
    displayed = json.loads(str(row["daily_quantities"]))["displayed"]
    assert displayed["total_mm"] is None
    assert displayed["wet_hours"] is None
    assert displayed["extrema_feed_ids"] == []
    assert displayed["extrema_coverage"] == EXTREMA_COVERAGE_INSUFFICIENT
    assert displayed["extrema_low_confidence"] is None
    assert displayed["total_mm"] != 0
    assert displayed["wet_hours"] != 0


# ===========================================================================
# G-T21 -- nothing scored moved (the simulator replays the pre-Item-G path).
# ===========================================================================


def test_scored_precip_entities_are_the_pre_g_clearing_subset_values() -> None:
    assert METHODOLOGY_VERSION == 2

    conn = _record_conn()
    generation_id = ensure_published_generation(conn, 1)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    local_date = _RECORD_DAY
    # 20 hours (blend-eligible via clears_coverage, NOT extrema-eligible --
    # Item G's rule never reaches the simulator). 5 wet, 15 dry.
    values = [0.5 if i < 5 else 0.0 for i in range(20)]
    for i, value in enumerate(values):
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (1, ?, 'precip', '2035-06-14T20:00:00Z', ?, ?, ?,
                    '{}', 'run-x', '2035-06-14T20:00:00Z')
            """,
            (feed_id, f"2035-06-15T{i:02d}:00:00Z", i + 1, value),
        )
    conn.commit()

    snapshot = capture_config_snapshot(conn, 1)
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
            VALUES (1, ?, 2, '0.16.0-test', 'running', 1, ?, ?, ?, ?, 1, 40, ?)
            """,
            (
                generation_id,
                json.dumps(snapshot),
                period_start,
                period_end,
                period_end,
                input_fingerprint(conn, 1, snapshot),
            ),
        ).lastrowid
    )
    conn.commit()

    cfg = RunConfig(
        site_id=1,
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
                source="open-meteo",
                model="ecmwf_ifs",
                max_lead_hours=192,
            ),
        ),
        period_start=period_start,
        period_end=period_end,
        bootstrap_seed=1,
        bootstrap_resamples=40,
    )

    before_input = input_fingerprint(conn, 1, snapshot)
    before_basis = result_basis_fingerprint(
        conn, 1, snapshot, period_start=period_start, period_end=period_end
    )

    simulate_snapshot_day(conn, cfg, local_date.isoformat())
    conn.commit()

    after_input = input_fingerprint(conn, 1, snapshot)
    after_basis = result_basis_fingerprint(
        conn, 1, snapshot, period_start=period_start, period_end=period_end
    )
    assert before_input == after_input
    assert before_basis == after_basis

    # The asymmetric precip eligibility gate (evaluate_precip): 20 covered
    # hours never clears the near-complete threshold (24 expected slots - 1
    # allowance = 23), so the TOTAL is excluded -- but one qualifying wet
    # slot proves OCCURRENCE "wet" at any coverage, so it stays eligible.
    # Both read from the same 20-hour blended series; only the gate differs.
    total_row = conn.execute(
        """
        SELECT * FROM verification_evidence
        WHERE run_id = ? AND snapshot_local_date = ? AND target_local_date = ?
          AND lead = 0 AND variable = 'precip' AND quantity = ?
          AND entity_type = 'depth' AND entity_key = '1'
        """,
        (run_id, local_date.isoformat(), local_date.isoformat(), QUANTITY_PRECIP_TOTAL),
    ).fetchone()
    assert total_row is not None
    assert total_row["covered_hours"] == 20
    assert total_row["forecast_eligible"] == 0
    assert total_row["forecast_exclusion_reason"] == EXCLUDE_BELOW_NEAR_COMPLETE

    occurrence_row_gate = conn.execute(
        """
        SELECT * FROM verification_evidence
        WHERE run_id = ? AND snapshot_local_date = ? AND target_local_date = ?
          AND lead = 0 AND variable = 'precip' AND quantity = ?
          AND entity_type = 'depth' AND entity_key = '1'
        """,
        (
            run_id,
            local_date.isoformat(),
            local_date.isoformat(),
            QUANTITY_PRECIP_OCCURRENCE,
        ),
    ).fetchone()
    assert occurrence_row_gate is not None
    assert occurrence_row_gate["covered_hours"] == 20
    assert occurrence_row_gate["forecast_eligible"] == 1
    assert occurrence_row_gate["forecast_exclusion_reason"] is None

    total_row = conn.execute(
        """
        SELECT predicted FROM verification_evidence
        WHERE run_id = ? AND target_local_date = ? AND lead = 0 AND variable = 'precip'
          AND quantity = ? AND entity_type = 'depth' AND entity_key = '1'
        """,
        (run_id, local_date.isoformat(), QUANTITY_PRECIP_TOTAL),
    ).fetchone()
    expected_total = displayed_daily("precip", [values], rain_threshold_mm=0.2)[
        "total_mm"
    ]
    assert expected_total == 2.5
    assert total_row["predicted"] == expected_total

    occurrence_row = conn.execute(
        """
        SELECT predicted FROM verification_evidence
        WHERE run_id = ? AND target_local_date = ? AND lead = 0 AND variable = 'precip'
          AND quantity = ? AND entity_type = 'depth' AND entity_key = '1'
        """,
        (run_id, local_date.isoformat(), QUANTITY_PRECIP_OCCURRENCE),
    ).fetchone()
    assert occurrence_row["predicted"] == 1.0  # at least one wet slot


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

    _seed_scoring_pairs(
        conn,
        feed_id=persistence_id,
        variable="precip",
        day_ahead=0,
        forecast=_PRECIP_POOR_FORECAST,
        observed=_PRECIP_MIXED_OBSERVED,
    )
    _seed_scoring_pairs(
        conn,
        feed_id=feed_a,
        variable="precip",
        day_ahead=0,
        forecast=_PRECIP_ACCURATE_FORECAST,
        observed=_PRECIP_MIXED_OBSERVED,
    )
    if seed_b_scoring:
        # Wet-always: mid-table skill, strictly below A's perfect skill.
        _seed_scoring_pairs(
            conn,
            feed_id=feed_b,
            variable="precip",
            day_ahead=0,
            forecast=(1.0, 1.0, 1.0),
            observed=_PRECIP_MIXED_OBSERVED,
        )
    _seed_complete_score_cache(conn, variable="precip", day_ahead=0)
    return feed_a, feed_b


def test_stale_union_for_precipitation() -> None:
    conn = _make_db()
    _seed_ab_precip_fixture(
        conn, feed_a_issued_at=_AB_FRESH_ISSUED, feed_b_issued_at=_AB_STALE_ISSUED
    )
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.precip.meta.stale is True
    assert today.stale is True
    html = _render_tiles(site_id=1, view=view)
    assert '<span class="badge warn">stale</span>' in html

    conn2 = _make_db()
    _seed_ab_precip_fixture(
        conn2, feed_a_issued_at=_AB_STALE_ISSUED, feed_b_issued_at=_AB_FRESH_ISSUED
    )
    conn2.commit()
    view2 = build_forecast(
        conn2, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    assert view2.tiles[0].precip.meta.stale is True
    html2 = _render_tiles(site_id=1, view=view2)
    assert '<span class="badge warn">stale</span>' in html2


def test_confident_blend_set_with_unscored_precip_extrema_set() -> None:
    conn = _make_db()
    _seed_ab_precip_fixture(conn, seed_b_scoring=False)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.precip.meta.state == "normal"
    assert today.precip.meta.extrema_state == "low_confidence"
    assert today.state == "normal"
    assert today.confidence_state == "low_confidence"
    html = _render_tiles(site_id=1, view=view)
    assert '<article class="tile state-normal">' in html
    assert '<span class="badge warn">low confidence</span>' in html
    assert '<span class="badge muted">ranking updating</span>' not in html

    record_conn = _record_conn()
    ensure_published_generation(record_conn, 1)
    set_setting(record_conn, "min_n", "3")
    rec_a = _feed_id(record_conn, "open-meteo", "ecmwf_ifs")
    rec_b = _feed_id(record_conn, "open-meteo", "gfs_global")
    full_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(24)
    ]
    partial_ats = full_ats[:20]
    _record_insert_day(
        record_conn,
        feed_id=rec_a,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=partial_ats,
        value=0.1,
    )
    _record_insert_day(
        record_conn,
        feed_id=rec_b,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=full_ats,
        value=0.15,
    )
    persistence_id = _feed_id(record_conn, "virtual", "_persistence")
    # `first_known_at` is left at its default (== `issued_at`, here
    # `_RECORD_KNOWABLE_ISSUED_AT`): a fixture convention that makes these
    # pairs knowable at the record's as-of instant, not a production
    # identity -- production stamps `first_known_at` from the sample's own
    # `fetched_at`, which need not equal `issued_at`.
    _seed_scoring_pairs(
        record_conn,
        feed_id=persistence_id,
        variable="precip",
        day_ahead=0,
        forecast=_PRECIP_POOR_FORECAST,
        observed=_PRECIP_MIXED_OBSERVED,
        valid_ats=_RECORD_KNOWABLE_VALID_ATS,
        lead_hours_seq=_RECORD_KNOWABLE_LEAD_HOURS,
        issued_at=_RECORD_KNOWABLE_ISSUED_AT,
    )
    _seed_scoring_pairs(
        record_conn,
        feed_id=rec_a,
        variable="precip",
        day_ahead=0,
        forecast=_PRECIP_ACCURATE_FORECAST,
        observed=_PRECIP_MIXED_OBSERVED,
        valid_ats=_RECORD_KNOWABLE_VALID_ATS,
        lead_hours_seq=_RECORD_KNOWABLE_LEAD_HOURS,
        issued_at=_RECORD_KNOWABLE_ISSUED_AT,
    )
    _seed_complete_score_cache(record_conn, variable="precip", day_ahead=0)
    t = _record_snapshot_t()
    build_forecast_record(
        record_conn, 1, _RECORD_DAY.isoformat(), now=t + timedelta(minutes=5)
    )
    row = record_conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = 1 AND variable = 'precip' AND display_lead = 0
        """
    ).fetchone()
    assert row is not None
    displayed = json.loads(str(row["daily_quantities"]))["displayed"]
    assert displayed["extrema_feed_ids"] == [rec_b]
    assert displayed["extrema_low_confidence"] is True
    assert displayed["low_confidence"] is False


def test_rebuilding_precedence_on_the_precip_extrema_set() -> None:
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")

    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="precip",
        issued_at="2026-07-20T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 20),
        values=[0.1 * i for i in range(20)],
    )
    _seed_scoring_pairs(
        conn,
        feed_id=persistence_id,
        variable="precip",
        day_ahead=0,
        forecast=_PRECIP_POOR_FORECAST,
        observed=_PRECIP_MIXED_OBSERVED,
    )
    _seed_scoring_pairs(
        conn,
        feed_id=feed_a,
        variable="precip",
        day_ahead=0,
        forecast=_PRECIP_ACCURATE_FORECAST,
        observed=_PRECIP_MIXED_OBSERVED,
    )
    _seed_complete_score_cache(conn, variable="precip", day_ahead=0)

    _seed_varying(
        conn,
        feed_id=feed_b,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
        values=[0.05 * i for i in range(24)],
    )
    _seed_scoring_pairs(
        conn,
        feed_id=feed_b,
        variable="precip",
        day_ahead=1,
        forecast=_PRECIP_ACCURATE_FORECAST,
        observed=_PRECIP_MIXED_OBSERVED,
    )
    conn.commit()

    assert (
        leaderboard_with_status(
            conn, site_id=1, variable="precip", day_ahead=0, window="rolling"
        ).status
        == "hit"
    )
    assert (
        leaderboard_with_status(
            conn, site_id=1, variable="precip", day_ahead=1, window="rolling"
        ).status
        == "rebuilding"
    )

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.precip.meta.state == "normal"
    assert today.precip.meta.extrema_state == "rebuilding"
    assert today.state == "normal"
    assert today.confidence_state == "rebuilding"
    html = _render_tiles(site_id=1, view=view)
    assert '<article class="tile state-normal">' in html
    assert '<span class="badge muted">ranking updating</span>' in html
    assert '<span class="badge warn">low confidence</span>' not in html

    record_conn = _record_conn()
    ensure_published_generation(record_conn, 1)
    set_setting(record_conn, "min_n", "3")
    rec_a = _feed_id(record_conn, "open-meteo", "ecmwf_ifs")
    rec_b = _feed_id(record_conn, "open-meteo", "gfs_global")
    full_ats = [
        datetime(_RECORD_DAY.year, _RECORD_DAY.month, _RECORD_DAY.day, h, tzinfo=UTC)
        for h in range(24)
    ]
    partial_ats = full_ats[:20]
    _record_insert_day(
        record_conn,
        feed_id=rec_a,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=partial_ats,
        value=0.1,
    )
    _record_insert_day(
        record_conn,
        feed_id=rec_b,
        local_date=_RECORD_DAY,
        issued_at="2035-06-15T06:00:00Z",
        valid_ats=full_ats,
        value=0.15,
    )
    persistence_id_rec = _feed_id(record_conn, "virtual", "_persistence")
    _seed_scoring_pairs(
        record_conn,
        feed_id=persistence_id_rec,
        variable="precip",
        day_ahead=0,
        forecast=8.0,
    )
    _seed_scoring_pairs(
        record_conn, feed_id=rec_a, variable="precip", day_ahead=0, forecast=10.1
    )
    _seed_complete_score_cache(record_conn, variable="precip", day_ahead=0)
    t = _record_snapshot_t()
    build_forecast_record(
        record_conn, 1, _RECORD_DAY.isoformat(), now=t + timedelta(minutes=5)
    )
    row = record_conn.execute(
        """
        SELECT * FROM forecast_of_record
        WHERE site_id = 1 AND variable = 'precip' AND display_lead = 0
        """
    ).fetchone()
    assert row is not None
    displayed = json.loads(str(row["daily_quantities"]))["displayed"]
    assert displayed["extrema_low_confidence"] is True


def test_suppressed_precip_extrema_contribute_to_neither_warning() -> None:
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    set_setting(conn, "forecast_blend_depth", "1")
    a_ats = _hours(_AB_VALID_DAY, 0, 20)
    _seed_varying(
        conn,
        feed_id=feed_a,
        variable="precip",
        issued_at=_AB_FRESH_ISSUED,
        valid_ats=a_ats,
        values=[0.1 * i for i in range(20)],
    )
    _seed_scoring_pairs(
        conn,
        feed_id=persistence_id,
        variable="precip",
        day_ahead=0,
        forecast=_PRECIP_POOR_FORECAST,
        observed=_PRECIP_MIXED_OBSERVED,
    )
    _seed_scoring_pairs(
        conn,
        feed_id=feed_a,
        variable="precip",
        day_ahead=0,
        forecast=_PRECIP_ACCURATE_FORECAST,
        observed=_PRECIP_MIXED_OBSERVED,
    )
    _seed_complete_score_cache(conn, variable="precip", day_ahead=0)
    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    today = view.tiles[0]
    assert today.precip.meta.extrema_unavailable is True
    assert today.precip.meta.extrema_state == "not_available"
    assert today.precip.meta.state == "normal"
    assert today.precip.meta.stale is False
    assert today.confidence_state == "normal"
    html = _render_tiles(site_id=1, view=view)
    assert '<span class="badge warn">low confidence</span>' not in html
    assert '<span class="badge muted">ranking updating</span>' not in html
    assert '<span class="badge warn">stale</span>' not in html

    # Paired positive: the stale badge still shows when A itself is stale,
    # so the negative above is not a blanket "badges never fire" fixture.
    stale_conn = _make_db()
    stale_feed = _feed_id(stale_conn, "open-meteo", "ecmwf_ifs")
    stale_persistence = _feed_id(stale_conn, "virtual", "_persistence")
    set_setting(stale_conn, "forecast_blend_depth", "1")
    _seed_varying(
        stale_conn,
        feed_id=stale_feed,
        variable="precip",
        issued_at=_AB_STALE_ISSUED,
        valid_ats=a_ats,
        values=[0.1 * i for i in range(20)],
    )
    _seed_scoring_pairs(
        stale_conn,
        feed_id=stale_persistence,
        variable="precip",
        day_ahead=0,
        forecast=8.0,
    )
    _seed_scoring_pairs(
        stale_conn, feed_id=stale_feed, variable="precip", day_ahead=0, forecast=10.1
    )
    _seed_complete_score_cache(stale_conn, variable="precip", day_ahead=0)
    stale_conn.commit()
    stale_view = build_forecast(
        stale_conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=_AB_NOW
    )
    assert stale_view.tiles[0].precip.meta.extrema_unavailable is True
    assert stale_view.tiles[0].precip.meta.stale is True
    stale_html = _render_tiles(site_id=1, view=stale_view)
    assert '<span class="badge warn">stale</span>' in stale_html


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
