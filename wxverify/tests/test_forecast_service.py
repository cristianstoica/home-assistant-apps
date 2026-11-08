"""Integration tests for ``wxverify.forecast.service`` against a real SQLite DB.

This file covers the skill-cell-
vs-display-tile mapping at a local-day boundary (a run issued the prior local
day landing in today's display tile must still rank by its ISSUE-relative
day_ahead, not the display day index). This file's centerpiece,
``test_b2_...``, is that gate.

Also covers tile state precedence (empty(global) >
not-available > low-confidence > normal, plus orthogonal stale/partial
badges) end-to-end through the service layer, since the individual pieces
(``select_cell_feeds``, ``clears_coverage``) are already unit-pinned
elsewhere and the risk here is in how the service layer WIRES them together.

Isolation: fresh ``sqlite3.connect(":memory:")`` + ``run_migrations`` per
test (mirrors test_forecast_data.py / test_scoring_equivalence.py).
Far-future (2035) dates are used for the seeded test publication, so its
cutoff always precedes any ``now`` this file's live (`build_forecast`)
fixtures use.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime

import pytest

from tests.helpers import (
    seed_active_publication,
    seed_publication_ranking,
    seed_rain_ranks,
)
from wxverify.collection.forecast_fetcher import register_feed_if_needed
from wxverify.core.timeutil import isoformat_utc
from wxverify.core.units import ms_to_kmh
from wxverify.db.migrations import pause_dropped_meteoblue_members, run_migrations
from wxverify.db.runtime_state import set_runtime_state
from wxverify.forecast.page import build_forecast_page
from wxverify.forecast.service import (
    build_forecast,
    build_hourly,
    relative_ago,
)
from wxverify.forecast.wind_blend import NOT_ENOUGH_FEEDS_NOTE
from wxverify.scoring.leaderboard import leaderboard_with_status
from wxverify.scoring.metrics import MetricSums, metrics_from_sums
from wxverify.settings.keys import get_number_setting, set_setting
from wxverify.web.context import feed_label


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


def _stamp_usable_fetch(
    conn: sqlite3.Connection, *, site_id: int = 1, feed_id: int, at: str
) -> None:
    """Record ``at`` as the feed's last usable forward fetch (the v8 column)."""
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_run_at, last_usable_fetch_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET
            last_run_at=excluded.last_run_at,
            last_usable_fetch_at=excluded.last_usable_fetch_at
        """,
        (site_id, feed_id, at, at),
    )


def _make_confident(
    conn: sqlite3.Connection, *, feed_id: int, variable: str, day_ahead: int
) -> None:
    """Give `feed_id` a real, positive skill ranking at (variable,
    day_ahead) against a `virtual/_persistence` baseline of forecast=8.0
    (`feed_id` forecasts 10.5): both land in a `publication_rankings` row
    via :func:`_seed_score_cache`, confident (n=3 matches this file's
    seeded ``min_n``)."""
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    _seed_score_cache(
        conn,
        variable=variable,
        day_ahead=day_ahead,
        forecasts={persistence_id: 8.0, feed_id: 10.5},
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
    # degenerate OUT-skills covered (forecast nearer observed 10.0).
    _seed_score_cache(
        conn,
        variable="temperature",
        day_ahead=7,
        forecasts={persistence_id: 8.0, degenerate_id: 10.2, covered_id: 11.0},
    )


_TEST_PUBLICATION_WINDOW_DAYS = 30
_TEST_PUBLICATION_WINDOW_KEY = f"w:{_TEST_PUBLICATION_WINDOW_DAYS}"


def _ensure_test_publication(conn: sqlite3.Connection, *, min_n: int) -> int:
    """This connection's one site-1 publication, created on first use.

    ``leaderboard_with_status``'s ``rolling`` window now always reads the
    active publication (9.2), never a live recompute, so every ranking
    this file seeds must land in ONE publication -- its ``min_n`` is baked
    in at creation, matching the runtime ``min_n`` setting at that point,
    so the read-time confidence check agrees with what
    :func:`_seed_score_cache` computed.
    """
    row = conn.execute(
        "SELECT id FROM publications WHERE site_id = 1 ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row is not None:
        return int(row["id"])
    return seed_active_publication(
        conn,
        site_id=1,
        cutoff=date(2035, 6, 30),
        published_at=datetime(2035, 6, 30, tzinfo=UTC),
        window_days=_TEST_PUBLICATION_WINDOW_DAYS,
        min_n=min_n,
    )


def _seed_score_cache(
    conn: sqlite3.Connection,
    *,
    variable: str,
    day_ahead: int,
    forecasts: dict[int, float],
) -> None:
    """Seed one published-ranking row per feed in ``forecasts`` (site 1,
    ``variable``, ``day_ahead``), each hand-derived from an explicit
    ``MetricSums``: `leaderboard_with_status`'s ``rolling`` window always
    reads the active publication (9.2), never a live recompute.

    Every key of ``forecasts`` stands for 3 identical far-future pairs
    against a fixed observed=10.0 baseline (``n=3``, matching this file's
    seeded ``min_n``), so bias/MAE/RMSE are exact, not approximated. A
    feed's skill is computed only when ``forecasts`` also carries the
    seeded ``virtual/_persistence`` feed id, paired 1:1 against
    persistence's own (also constant) error over the same 3 valid_ats --
    a forecast nearer 10.0 than persistence's own reads a higher skill.
    ``forecasts`` is local to the caller: no module-level state survives
    this call, so a stale ``id(conn)`` reuse across tests can never leak
    one test's values into another's.
    """
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    pub_id = _ensure_test_publication(conn, min_n=min_n)
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
            window_key=_TEST_PUBLICATION_WINDOW_KEY,
            result=result,
        )


# ---------------------------------------------------------------------------
# A run issued the PRIOR local day, landing in TODAY's display
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
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
    )
    # Fetched 6h before `now` -> NOT stale (<12h for a 360min-interval feed).
    _stamp_usable_fetch(conn, feed_id=feed_id, at="2026-07-19T20:00:00Z")
    # Confident ONLY at day_ahead=1 (the correct, issue-relative cell). If the
    # service wrongly ranked by the display day index (0) instead, this feed
    # would find no ranking row there and read as not-confident.
    _make_confident(conn, feed_id=feed_id, variable="temperature", day_ahead=1)

    conn.commit()
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
    assert today.temp.meta.fetch_unknown is False
    assert today.state == "normal"


def test_b2_paired_negative_wrong_cell_reads_low_confidence() -> None:
    """Sanity companion to the B2 gate above: if a ranking is seeded at the
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

    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].temp.meta.state == "low_confidence"


# ---------------------------------------------------------------------------
# Stale badge is orthogonal to state -- same confident/normal construction as
# the B2 positive, only the last usable fetch lag differs.
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
        issued_at="2026-07-19T08:00:00Z",
        valid_ats=valid_ats,
    )
    # Fetched 18h before `now` -> stale (>12h threshold for a 360min-interval
    # feed).
    _stamp_usable_fetch(conn, feed_id=feed_id, at="2026-07-19T08:00:00Z")
    _make_confident(conn, feed_id=feed_id, variable="temperature", day_ahead=1)

    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "normal"  # still confident
    assert today.temp.meta.stale is True  # but flagged stale
    assert today.stale is True


# ---------------------------------------------------------------------------
# Minimum-coverage guard: a cell whose selected feeds don't reach
# MIN_COVERAGE_HOURS still carries the "partial" badge. For temperature
# (Item F) and precipitation (Item G), under-coverage means the feed is not
# extrema-eligible (it does not cover the whole local day exactly), so the
# daily high/low, and the daily total/wet-hours, are suppressed rather than
# aggregated from a partial range.
# ---------------------------------------------------------------------------


def test_temperature_extrema_suppressed_under_coverage_partial_badge_set() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)

    # Only 5 covered hours -- under MIN_COVERAGE_HOURS (18) and short of the
    # 24 the target local day needs, so the feed is not extrema-eligible.
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

    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.partial is True
    assert today.temp.meta.available is True  # cell/hourly product stays populated
    # The daily extrema are suppressed, not aggregated from the partial range.
    assert today.temp.high_c is None
    assert today.temp.low_c is None
    assert today.temp.meta.extrema_unavailable is True
    assert today.partial is True


# ---------------------------------------------------------------------------
# Tile-level precedence rollup: not_available cells are excluded from the
# rollup; among the rest, low_confidence beats normal.
# ---------------------------------------------------------------------------


def _seed_wind_weight(
    conn: sqlite3.Connection,
    *,
    publication_id: int,
    feed_id: int,
    day_ahead: int,
    weight: float = 1.0,
) -> None:
    """One published wind weight (``publication_wind_weights``)."""
    conn.execute(
        "INSERT INTO publication_wind_weights"
        " (publication_id, feed_id, day_ahead, mae_ms, days, weight)"
        " VALUES (?, ?, ?, 1.0, 30, ?)",
        (publication_id, feed_id, day_ahead, weight),
    )


def _seed_wind_publication(
    conn: sqlite3.Connection,
    *,
    weights_by_feed: dict[int, float],
    day_ahead: int = 1,
    site_id: int = 1,
) -> int:
    """Turn the daily close on and publish each feed's raw wind weight.

    Replaces the removed training-from-pairs pipeline for the tests whose
    subject is SERVING: wind serving now reads only the active
    publication's ``publication_wind_weights`` (``load_wind_serving``),
    never a live recompute from ``forecast_pairs``. ``day_ahead`` must
    match the representative lead ``_candidates`` computes for the seeded
    samples (1, for the usual "issued the evening before, 24h today"
    shape this file's wind fixtures use).
    """
    from wxverify.db.site_days import DAILY_CLOSE_ON_KEY

    cutoff = date(2026, 7, 19)
    published_at = datetime(2026, 7, 19, 21, 0, tzinfo=UTC)
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(published_at))
    publication_id = seed_active_publication(
        conn, site_id=site_id, cutoff=cutoff, published_at=published_at
    )
    for feed_id, weight in weights_by_feed.items():
        _seed_wind_weight(
            conn,
            publication_id=publication_id,
            feed_id=feed_id,
            day_ahead=day_ahead,
            weight=weight,
        )
    return publication_id


def test_tile_precedence_low_confidence_beats_normal_not_available_excluded() -> None:
    """The rain cell's state comes from the published rain ranking, not the
    leaderboard, read with the daily close on. So this runs with the close
    on, and the roles move: temperature has
    samples and no published ranking (low_confidence), rain's winner has a
    full day under a settled ranking (normal), wind has no published weights
    (not_available)."""
    conn = _make_db()
    ecmwf_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    gfs_id = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)

    # temperature: samples, no published ranking -> low_confidence.
    _seed_hourly(
        conn,
        feed_id=ecmwf_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 20),
    )
    # precip: a settled ranking whose winner has a full day -> normal.
    _seed_hourly(
        conn,
        feed_id=gfs_id,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
        value=0.0,
    )
    seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [gfs_id]})
    # wind: no samples and no published weights -> not_available.
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    today = view.tiles[0]
    assert today.temp.meta.state == "low_confidence"
    assert today.precip.meta.state == "normal"
    assert today.wind.meta.state == "not_available"
    assert today.wind.meta.available is False
    # Precedence: not_available (wind) excluded from the rollup; among the
    # populated cells, low_confidence (temp) beats normal (precip).
    assert today.state == "low_confidence"

    # Paired negative: a day with NO data anywhere is fully not_available at
    # the tile level (proves the tile-level rollup genuinely reflects
    # absence, not just "always non-not_available once anything exists").
    far_day = view.tiles[7]
    assert far_day.state == "not_available"
    assert far_day.temp.meta.available is False


# ---------------------------------------------------------------------------
# Cache-status display: a per-feed "rebuilding" ranking status can suppress
# the low_confidence label, but only over the feeds each surface actually
# renders. See wxverify/docs/plans/2026-09-08-cache-status-display.md.
# ---------------------------------------------------------------------------


def test_tile_rollup_precedence_low_confidence_then_rebuilding() -> None:
    """O4: tile rollup precedence over the new vocabulary member.

    Day tile 0: temp is `low_confidence` (no published ranking), precip's
    extrema state is `rebuilding` -- the rollup must still read
    `low_confidence`, because that caveat does not resolve on its own. Day
    tile 1: wind is `normal`, precip's extrema state is `rebuilding` and
    nothing else is degraded -- the rollup must read `rebuilding`.

    This is purely about D4's precedence, not D2's per-surface width --
    that is O8's job.

    Rain release, C4: the rain cell's `rebuilding` is now a provisional
    published rain ranking, read with the daily close on, and shows in its
    extrema state (the cell's hourly state is `normal`), so the rollup is
    the tile's `confidence_state`. The rain cell's state does not come from
    the close-off leaderboard, so this runs with the close on, where the normal
    cell of day tile 1 is wind (two feeds with published weights) and
    temperature has no samples there.
    """
    conn = _make_db()
    ecmwf_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    gfs_id = _feed_id(conn, "open-meteo", "gfs_global")
    icon_id = _feed_id(conn, "open-meteo", "icon_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)

    # --- Day tile 0 ("2026-07-20"): temperature with samples and no
    # published ranking -> low_confidence; rain provisional -> rebuilding.
    _seed_hourly(
        conn,
        feed_id=ecmwf_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 20),
    )
    _seed_hourly(
        conn,
        feed_id=gfs_id,
        variable="precip",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
        value=0.0,
    )

    # --- Day tile 1 ("2026-07-21"): wind from two feeds with published
    # weights at their representative day ahead (2: issued two days before)
    # -> normal; rain provisional -> rebuilding; no temperature samples.
    day1_valid_ats = _hours("2026-07-21", 0, 24)
    for feed_id in (ecmwf_id, gfs_id):
        _seed_hourly(
            conn,
            feed_id=feed_id,
            variable="wind",
            issued_at="2026-07-19T00:00:00Z",
            valid_ats=day1_valid_ats,
            value=5.0,
        )
    _seed_hourly(
        conn,
        feed_id=icon_id,
        variable="precip",
        issued_at="2026-07-19T00:00:00Z",
        valid_ats=day1_valid_ats,
        value=0.0,
    )
    publication_id = seed_rain_ranks(
        conn,
        site_id=1,
        published_at=now,
        ranked={0: [gfs_id], 1: [icon_id]},
        provisional={0, 1},
    )
    for feed_id in (ecmwf_id, gfs_id):
        _seed_wind_weight(
            conn, publication_id=publication_id, feed_id=feed_id, day_ahead=2
        )
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )

    today = view.tiles[0]
    assert today.temp.meta.state == "low_confidence"
    assert today.precip.meta.state == "normal"
    assert today.precip.meta.extrema_state == "rebuilding"
    assert today.wind.meta.state == "not_available"
    # low_confidence (a caveat that does not resolve on its own) outranks
    # rebuilding (a caveat that resolves itself) -- D4.
    assert today.confidence_state == "low_confidence"

    tomorrow = view.tiles[1]
    assert tomorrow.wind.meta.state == "normal"
    assert tomorrow.precip.meta.state == "normal"
    assert tomorrow.precip.meta.extrema_state == "rebuilding"
    assert tomorrow.temp.meta.state == "not_available"
    # No cell is low_confidence; the only degraded populated state is
    # rebuilding, so the rollup reads rebuilding, not normal.
    assert tomorrow.state == "normal"
    assert tomorrow.confidence_state == "rebuilding"


def test_mixed_representative_leads_stay_normal_confident_winner() -> None:
    """O3: an unconfident key on a feed the ladder never selects must not
    relabel a cell whose selected feed has a settled, confident ranking.

    Feed A (rep day_ahead=1) is confident with a complete score_cache
    snapshot; feed B (rep day_ahead=0) has samples but no ranking row
    there, so its leaderboard still reads "hit" (the published leaderboard
    has no other status now that there is an active publication) but with
    no row for feed B. Rung 1 selects the confident feed A alone, so B
    (the unconfident feed) never enters either surface's rendered set.

    ``uv run pytest tests/test_forecast_service.py -k mixed_lead``
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    valid_ats = _hours("2026-07-20", 4, 20)

    # Feed A: issued the previous evening -> representative day_ahead=1.
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
    )
    _make_confident(conn, feed_id=feed_a, variable="temperature", day_ahead=1)

    # Feed B: issued the same morning -> representative day_ahead=0. Has
    # samples (not an empty universe) but no ranking row -> unconfident
    # there.
    _seed_hourly(
        conn,
        feed_id=feed_b,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_ats=valid_ats,
    )

    conn.commit()
    assert (
        leaderboard_with_status(
            conn, site_id=1, variable="temperature", day_ahead=1, window="rolling"
        ).status
        == "hit"
    )
    # An active publication exists (seeded above for day_ahead=1), so every
    # cell's leaderboard status is "hit" -- there is no "rebuilding" status
    # any more -- even though day_ahead=0 has no ranking row for feed B.
    assert (
        leaderboard_with_status(
            conn, site_id=1, variable="temperature", day_ahead=0, window="rolling"
        ).status
        == "hit"
    )

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].temp.meta.state == "normal"

    hourly = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert hourly["states"]["temperature"] == "normal"


def test_mixed_representative_leads_stay_low_confidence_unconfident_winner() -> None:
    """O7: sibling of the test above with feed A scored but NOT confident
    (n > 0, below the raised min_n floor) rather than absent entirely. Rung
    1 finds no confident candidate and skips; rung 2 selects the scored
    feed A alone (feed B still reads pair_n=0, with no ranking row at its
    cell), so the cell must stay low_confidence on both surfaces.
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    valid_ats = _hours("2026-07-20", 4, 20)

    # Raise min_n above the n=3 _seed_score_cache seeds, so feed A's
    # cached row is scored (n=3 > 0) but not confident (n < min_n).
    set_setting(conn, "min_n", "4")

    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=valid_ats,
    )
    # No persistence forecast given -> no skill, but a ranking row still
    # exists (n=3, not confident since n < the raised min_n=4).
    _seed_score_cache(
        conn, variable="temperature", day_ahead=1, forecasts={feed_a: 10.5}
    )

    _seed_hourly(
        conn,
        feed_id=feed_b,
        variable="temperature",
        issued_at="2026-07-20T00:00:00Z",
        valid_ats=valid_ats,
    )

    conn.commit()
    assert (
        leaderboard_with_status(
            conn, site_id=1, variable="temperature", day_ahead=1, window="rolling"
        ).status
        == "hit"
    )
    # An active publication exists (seeded above for day_ahead=1), so every
    # cell's leaderboard status is "hit" -- there is no "rebuilding" status
    # any more -- even though day_ahead=0 has no ranking row for feed B.
    assert (
        leaderboard_with_status(
            conn, site_id=1, variable="temperature", day_ahead=0, window="rolling"
        ).status
        == "hit"
    )

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    assert view.tiles[0].temp.meta.state == "low_confidence"

    hourly = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert hourly["states"]["temperature"] == "low_confidence"


# The published leaderboard (scoring/leaderboard.py's
# ``_published_leaderboard``) only ever returns "hit", "live" or "empty",
# never "rebuilding", so there is no disagreement between the tile and the
# drill-down over a "rebuilding" leaderboard status left to pin.


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
# Rain icon and wet hours (rain release, C4): the icon follows the ranking's
# 0.2 mm trigger on the winner's total, and the hours are the winner's own
# count, not a blend. RAIN_GLYPH_MIN_WET_HOURS (the old six-wet-hour glyph
# rule) is deleted. The fixture is a complete 24-hour local day so the
# winner has a full day (covers_local_day_exactly).
# ---------------------------------------------------------------------------


def _seed_precip_day(
    conn: sqlite3.Connection, *, feed_id: int, values: list[float]
) -> None:
    for i, valid_at in enumerate(_hours("2026-07-20", 0, 24)):
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable="precip",
            issued_at="2026-07-19T20:00:00Z",
            valid_at=valid_at,
            lead_hours=i + 1,
            value=values[i],
        )


def test_rain_icon_shown_at_0_2_mm_hidden_at_0_19_mm() -> None:
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)

    # A winner whose day totals exactly 0.2 mm, in one hour -> icon shown
    # (light: the wettest hour is at most 1.0 mm/h).
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _seed_precip_day(conn, feed_id=feed_id, values=[0.2] + [0.0] * 23)
    seed_rain_ranks(conn, site_id=1, published_at=now, ranked={0: [feed_id]})
    conn.commit()
    precip = (
        build_forecast(conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now)
        .tiles[0]
        .precip
    )
    assert precip.total_mm == pytest.approx(0.2)
    assert precip.wet_hours == 1
    assert precip.icon == "light"

    # Paired negative on a fresh connection: 0.19 mm -> the amount is shown,
    # with no icon and no wet hour at the 0.2 mm threshold.
    conn_below = _make_db()
    feed_below = _feed_id(conn_below, "open-meteo", "ecmwf_ifs")
    _seed_precip_day(conn_below, feed_id=feed_below, values=[0.19] + [0.0] * 23)
    seed_rain_ranks(conn_below, site_id=1, published_at=now, ranked={0: [feed_below]})
    conn_below.commit()
    precip_below = (
        build_forecast(
            conn_below, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
        )
        .tiles[0]
        .precip
    )
    assert precip_below.total_mm == pytest.approx(0.19)
    assert precip_below.wet_hours == 0
    assert precip_below.icon is None


def test_wet_hours_are_the_winner_s_own_count_not_a_blend() -> None:
    """The old rule rounded a blend of the selected feeds' wet-hour counts
    (5 and 6 -> 5.5 -> 6). Now the count is the winner's alone: 5 while the
    5-hour feed is selected, 6 once the 6-hour feed is."""
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    for selected_hours in (5, 6):
        conn = _make_db()
        feed_5 = _feed_id(conn, "open-meteo", "ecmwf_ifs")
        feed_6 = _feed_id(conn, "open-meteo", "gfs_global")
        _seed_precip_day(conn, feed_id=feed_5, values=[0.2] * 5 + [0.0] * 19)
        _seed_precip_day(conn, feed_id=feed_6, values=[0.2] * 6 + [0.0] * 18)
        winner = feed_5 if selected_hours == 5 else feed_6
        seed_rain_ranks(
            conn,
            site_id=1,
            published_at=now,
            ranked={0: [feed_5, feed_6]},
            selected={0: winner},
        )
        conn.commit()
        precip = (
            build_forecast(
                conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
            )
            .tiles[0]
            .precip
        )
        assert [f.feed_id for f in precip.meta.feeds] == [winner]
        assert precip.wet_hours == selected_hours
        assert precip.total_mm == pytest.approx(0.2 * selected_hours)


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
    # Loser: not confident (no ranking row), disjoint hours.
    _seed_hourly(
        conn,
        feed_id=loser_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 10, 2),
    )

    conn.commit()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["hours"] == _hours("2026-07-20", 4, 2)  # winner's hours only
    assert payload["states"]["temperature"] == "normal"
    feed_ids = {feed["feed_id"] for feed in payload["feeds"]}
    assert winner_id in feed_ids
    assert loser_id not in feed_ids


def _seed_wind_hours_outside_axis_fixture() -> tuple[
    sqlite3.Connection, int, int, datetime, list[str]
]:
    """The base fixture of
    ``test_build_hourly_hour_axis_reflects_selected_feed_only`` plus
    ``winner_id`` wind samples at hours outside the temperature-only axis
    (Part E). With no wind track record no feed has a wind weight
    (``:1015-1035``), so the wind selection is empty and the axis is the
    two temperature hours only.
    """
    conn = _make_db()
    winner_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    loser_id = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    set_setting(conn, "forecast_blend_depth", "1")

    _seed_hourly(
        conn,
        feed_id=winner_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 2),
    )
    _make_confident(conn, feed_id=winner_id, variable="temperature", day_ahead=1)
    _seed_hourly(
        conn,
        feed_id=loser_id,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 10, 2),
    )
    off_axis_wind_stamps = _hours("2026-07-20", 8, 2)
    _seed_hourly(
        conn,
        feed_id=winner_id,
        variable="wind",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=off_axis_wind_stamps,
        value=5.0,
    )
    conn.commit()
    return conn, winner_id, loser_id, now, off_axis_wind_stamps


def test_e1_forecast_page_wind_grid_leaves_off_axis_hours_to_the_model_row_only() -> (
    None
):
    """E1: the Forecast page never raises on a wind sample outside the
    temperature-only axis. The BLEND row stays blank at those hours (the
    wind selection is empty, so the blend line never reaches them); the
    model row, read by exact ``valid_at`` key over the full day's
    columns (not the narrow axis), still shows the winner's own sample at
    its own hour.

    Catches: a later page change that builds a per-feed series on the
    axis (the card 7 plan's "Rain-only feed" catch, now for wind at an
    off-axis hour; ``test_forecast_page.py:412`` covers only an on-axis
    hour).
    """
    conn, winner_id, _loser_id, now, off_axis_wind_stamps = (
        _seed_wind_hours_outside_axis_fixture()
    )

    page = build_forecast_page(
        conn,
        site_id=1,
        timezone="UTC",
        rain_threshold_mm=0.2,
        selected_day=0,
        now=now,
    )
    assert page.empty is False
    detail = page.detail
    assert detail is not None
    assert all(value is None for value in detail.wind.top.values)

    winner_wind_row = next(row for row in detail.wind.rows if row.feed_id == winner_id)
    expected_kmh = ms_to_kmh(5.0)
    for column, value in zip(detail.columns, winner_wind_row.values, strict=True):
        if column.key in off_axis_wind_stamps:
            assert value == expected_kmh
        else:
            assert value is None


def test_e2_build_hourly_skips_off_axis_wind_hours_instead_of_raising() -> None:
    """E2: ``/api/forecast/hourly`` must not raise on a feed whose wind
    samples fall outside the temperature-only axis; the off-axis wind
    hours are left out of that feed's series, the axis is not widened to
    them, and the feed is not dropped.

    Fails on today's code with ``KeyError`` at ``service.py:812``
    (``_feed_series`` indexing an off-axis hour); passes once
    ``_feed_series`` skips the off-axis sample instead.

    Catches: no fix; a fix that widens the axis to the wind hours; a fix
    that drops the whole feed.
    """
    conn, winner_id, _loser_id, now, _off_axis_wind_stamps = (
        _seed_wind_hours_outside_axis_fixture()
    )

    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["hours"] == _hours("2026-07-20", 4, 2)

    winner_feed = next(
        feed for feed in payload["feeds"] if feed["feed_id"] == winner_id
    )
    assert winner_feed["wind_kmh"] == [None, None]
    assert winner_feed["temp_c"] == [10.0, 10.0]  # _seed_hourly's default value


def test_build_hourly_wind_untrained_feed_is_not_served() -> None:
    """A fresh site serves weighted wind (``pair_max``); a feed with no wind
    track record has no weight, so the wind line is empty and noted rather
    than falling back to the unweighted blend."""
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
    conn.commit()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["hours"] == []
    assert payload["blend"]["wind_kmh"] == []
    assert payload["feeds"] == []
    assert payload["weights"] == {"wind": {}}
    assert payload["notes"] == {"wind": NOT_ENOUGH_FEEDS_NOTE}


def _pause_feed(conn: sqlite3.Connection, pause_kind: str, feed_id: int) -> None:
    """Pause ``feed_id`` at site 1, per ``pause_kind`` ("none" is a no-op)."""
    if pause_kind == "none":
        return
    if pause_kind == "global":
        conn.execute("UPDATE feeds SET enabled = 0 WHERE id = ?", (feed_id,))
    else:
        conn.execute(
            "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (1, ?, 0)",
            (feed_id,),
        )


def test_build_hourly_wind_series_weighted_and_kmh_converted() -> None:
    """Two published feeds: the wind line is their accuracy-weighted mean.

    Feed A's published weight (1.0) stands in for an MAE of 1.0 and feed
    B's (0.5) for an MAE of 2.0, normalized to 2/3 and 1/3. Tomorrow A says
    4.0 m/s and B 7.0 m/s, so the weighted value is 5.0 m/s = 18.0 km/h
    (the unweighted mean would be 5.5 m/s = 19.8 km/h).
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_wind_publication(conn, weights_by_feed={feed_a: 1.0, feed_b: 0.5})
    hours = _hours("2026-07-20", 0, 24)
    for feed_id, value in ((feed_a, 4.0), (feed_b, 7.0)):
        _seed_hourly(
            conn,
            feed_id=feed_id,
            variable="wind",
            issued_at="2026-07-19T20:00:00Z",
            valid_ats=hours,
            value=value,
        )
    conn.commit()
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert payload["hours"] == hours
    assert payload["blend"]["wind_kmh"] == pytest.approx([18.0] * 24)
    assert payload["weights"] == {
        "wind": {str(feed_a): pytest.approx(2 / 3), str(feed_b): pytest.approx(1 / 3)}
    }
    assert payload["notes"] == {"wind": None}
    per_feed = {feed["feed_id"]: feed["wind_kmh"] for feed in payload["feeds"]}
    assert per_feed == {
        feed_a: pytest.approx([ms_to_kmh(4.0)] * 24),
        feed_b: pytest.approx([ms_to_kmh(7.0)] * 24),
    }


# ---------------------------------------------------------------------------
# A paused feed (global or site-level) never serves wind.
#
# There is no ``load_wind_training`` function: wind weights come only from
# the active publication's ``publication_wind_weights``, never a live
# recompute from training pairs, so there is no training-side pause test
# to pair with this one.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("pause_kind", ["global", "site"])
def test_build_hourly_wind_excludes_paused_feed(pause_kind: str) -> None:
    """Pausing feed C drops it from the served wind blend and its weights.

    Before the pause, A (weight 1.0), B (0.5), and C (2.0) blend tomorrow's
    4.0/7.0/9.0 m/s to 25.5/3.5 m/s = 26.23 km/h. After, only A and B remain
    and the blend/weights match the two-feed baseline above (18.0 km/h, 2/3
    and 1/3). Pausing a feed only narrows the candidate pool, never the
    publication's weight rows, so no re-seeding is needed for the "after"
    call.

    Catches: no filter on the serving path (the post-pause blend, weights,
    and ``payload["feeds"]`` would still include C, identical to the
    pre-pause call).
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    feed_c = _feed_id(conn, "open-meteo", "icon_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_wind_publication(
        conn, weights_by_feed={feed_a: 1.0, feed_b: 0.5, feed_c: 2.0}
    )
    hours = _hours("2026-07-20", 0, 24)
    for feed_id, value in ((feed_a, 4.0), (feed_b, 7.0), (feed_c, 9.0)):
        _seed_hourly(
            conn,
            feed_id=feed_id,
            variable="wind",
            issued_at="2026-07-19T20:00:00Z",
            valid_ats=hours,
            value=value,
        )
    conn.commit()

    before = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert before["blend"]["wind_kmh"] == pytest.approx([25.5 / 3.5 * 3.6] * 24)

    _pause_feed(conn, pause_kind, feed_c)
    conn.commit()

    after = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert after["blend"]["wind_kmh"] == pytest.approx([18.0] * 24)
    assert after["weights"] == {
        "wind": {str(feed_a): pytest.approx(2 / 3), str(feed_b): pytest.approx(1 / 3)}
    }
    feed_ids_after = {feed["feed_id"] for feed in after["feeds"]}
    assert feed_c not in feed_ids_after


# ---------------------------------------------------------------------------
# A paused feed is never picked by build_forecast/build_hourly, no matter
# which selection rung (sample count, coverage pool) would otherwise pick it.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("pause_kind", ["none", "global", "site"])
def test_paused_feed_not_picked_by_sample_count(pause_kind: str) -> None:
    """At blend depth 1, a paused B is never picked even with twice A's hours.

    Unpaused (``none``), B's 24 hours beat A's 12 on sample count and B is
    the lone selection. Paused (``global``/``site``), B drops out of the
    pool entirely and A -- the only feed left -- is selected instead.

    Catches: the loader filter missing at ``build_hourly``/``build_forecast``
    (the ``global``/``site`` cases would still pick B); a bare ``enabled``
    check (the ``site`` case alone would still pick B).
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    set_setting(conn, "forecast_blend_depth", "1")
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 12),
    )
    _seed_hourly(
        conn,
        feed_id=feed_b,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
    )
    _pause_feed(conn, pause_kind, feed_b)
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = next(t for t in view.tiles if t.day_index == 0)
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)

    if pause_kind == "none":
        assert {feed["feed_id"] for feed in payload["feeds"]} == {feed_b}
        assert payload["hours"] == _hours("2026-07-20", 0, 24)
        assert tile.temp.meta.contributor_ids == (feed_b,)
    else:
        assert {feed["feed_id"] for feed in payload["feeds"]} == {feed_a}
        assert payload["hours"] == _hours("2026-07-20", 0, 12)
        assert tile.temp.meta.contributor_ids == (feed_a,)


@pytest.mark.parametrize("pause_kind", ["none", "global", "site"])
def test_paused_feed_does_not_narrow_the_pool(pause_kind: str) -> None:
    """A confident 4-hour A beats an unconfident 24-hour B -- unless B is
    paused, in which case B never reaches the pool and the outcome is
    exactly what A alone produces.

    Unpaused, B is picked (24h, unconfident but present) and the tile is
    ``low_confidence``. Paused, only A remains: 4 hours, ``normal`` state.

    Catches: pausing B "narrowing" the pool by removing a competitor rather
    than removing B itself (the ``global``/``site`` cases would then still
    show B's hours/feed_id, just picked for a different reason).
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    set_setting(conn, "forecast_blend_depth", "1")
    _seed_hourly(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 4, 4),
    )
    _make_confident(conn, feed_id=feed_a, variable="temperature", day_ahead=1)
    _seed_hourly(
        conn,
        feed_id=feed_b,
        variable="temperature",
        issued_at="2026-07-19T20:00:00Z",
        valid_ats=_hours("2026-07-20", 0, 24),
    )
    _pause_feed(conn, pause_kind, feed_b)
    conn.commit()

    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = next(t for t in view.tiles if t.day_index == 0)
    payload = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)

    if pause_kind == "none":
        assert {feed["feed_id"] for feed in payload["feeds"]} == {feed_b}
        assert payload["states"]["temperature"] == "low_confidence"
        assert tile.temp.meta.contributor_ids == (feed_b,)
    else:
        assert {feed["feed_id"] for feed in payload["feeds"]} == {feed_a}
        assert payload["hours"] == _hours("2026-07-20", 4, 4)
        assert payload["states"]["temperature"] == "normal"
        assert tile.temp.meta.contributor_ids == (feed_a,)


def test_drop_takes_a_member_off_the_served_wind() -> None:
    """The dropped-member pause (plan Step 6) reaches the served wind blend,
    its weights, its per-feed listing and its training set -- not just the
    feed's own row.

    A real meteoblue member registers through the fetcher's own
    registration path, published alongside two open-meteo feeds, and is
    confirmed served and weighted BEFORE the pause (so the pass below is
    not vacuous); the pause then removes it from both.

    Catches: a pause that misses the member row the fetcher creates (wrong
    ``source`` or case -- the member would keep its control-era payload
    weight); Step 2 missing (the member's samples would stay in
    ``build_hourly``'s candidates and the payload would still include it).
    """
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    feed_b = _feed_id(conn, "open-meteo", "gfs_global")
    feed_m = register_feed_if_needed(
        conn, "meteoblue", "NEMS4", fetch_interval_minutes=60
    )
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (1, ?, 1)",
        (package_id,),
    )
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    _seed_wind_publication(
        conn, weights_by_feed={feed_a: 1.0, feed_b: 0.5, feed_m: 2.0}
    )
    hours = _hours("2026-07-20", 0, 24)
    for feed_id, value in ((feed_a, 4.0), (feed_b, 7.0), (feed_m, 9.0)):
        _seed_hourly(
            conn,
            feed_id=feed_id,
            variable="wind",
            issued_at="2026-07-19T20:00:00Z",
            valid_ats=hours,
            value=value,
        )
    conn.commit()

    before = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert before["weights"] == {
        "wind": {
            str(feed_a): pytest.approx(2 / 7),
            str(feed_b): pytest.approx(1 / 7),
            str(feed_m): pytest.approx(4 / 7),
        }
    }
    assert before["blend"]["wind_kmh"] == pytest.approx([25.5 / 3.5 * 3.6] * 24)
    assert feed_m in {feed["feed_id"] for feed in before["feeds"]}

    pause_dropped_meteoblue_members(conn)
    conn.commit()

    after = build_hourly(conn, site_id=1, timezone="UTC", day=0, now=now)
    assert after["weights"] == {
        "wind": {str(feed_a): pytest.approx(2 / 3), str(feed_b): pytest.approx(1 / 3)}
    }
    assert after["blend"]["wind_kmh"] == pytest.approx([18.0] * 24)
    assert after["notes"] == {"wind": None}
    assert feed_m not in {feed["feed_id"] for feed in after["feeds"]}


# ---------------------------------------------------------------------------
# Coverage-aware feed selection (far-horizon collapse fix). The pre-fix bug:
# in _build_tile, high_c = blend_mean([max(v) ...]) and low_c = blend_mean(
# [min(v) ...]); a lone selected single-slot feed has max == min, so the tile
# renders high == low == a single value. These pin that a multi-point feed is
# preferred over a higher-skill single-slot one.
# ---------------------------------------------------------------------------


def test_far_horizon_multipoint_feed_rescues_collapsed_tile() -> None:
    """Headline regression (FAILS pre-fix, PASSES post-fix), updated for Item F.

    Pre-fix reasoning: with blend_depth=1 the selection pool == candidates, so
    the confidence ladder ranks the two confident feeds by skill and takes the
    top 1 -> the single-slot gfs_global feed (higher skill) is chosen ALONE ->
    the BLEND would collapse to high == low == 25.0.
    Post-fix (pre-Item-F): the >=12h adequate pool restricts to the 15h
    ecmwf_ifs feed (covered_hours 1 < 12 gates gfs out), so the blend never
    collapses -- pinned below via ``build_hourly``'s per-feed series, since
    ``meta.feeds`` now names the extrema set for temperature, not the blend
    set (Item F).
    Item F: neither feed covers the whole 24-hour local day (ecmwf_ifs
    supplies 15, gfs_global 1), so neither is extrema-eligible; the daily
    high/low are suppressed rather than aggregated from ecmwf_ifs's partial
    9.1..17.0 range, and the tile's own coverage-guard "partial" badge (a
    property of the >=18h blend-set guard, unrelated to extrema eligibility)
    still fires.
    """
    conn = _make_db()
    now = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)  # local date 2026-07-20
    set_setting(conn, "forecast_blend_depth", "1")
    _seed_far_horizon_collapse(conn)

    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[7]  # D+7 far-horizon tile
    assert tile.temp.high_c is None
    assert tile.temp.low_c is None
    assert tile.temp.meta.extrema_unavailable is True
    assert tile.temp.meta.partial is True  # 15 covered hours < 18-hour badge gate
    # No feed is extrema-eligible, so meta.feeds (the extrema set) is empty --
    # this does NOT mean the blend selection collapsed onto the single-slot
    # feed; that is checked separately via build_hourly below.
    assert tile.temp.meta.feeds == []

    hourly = build_hourly(conn, site_id=1, timezone="UTC", day=7, now=now)
    hourly_labels = [feed["label"] for feed in hourly["feeds"]]
    assert feed_label("open-meteo", "gfs_global") not in hourly_labels
    assert feed_label("open-meteo", "ecmwf_ifs") in hourly_labels


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
    # high_id's forecast is nearer persistence's observed baseline than
    # low_id's -> higher skill.
    _seed_score_cache(
        conn,
        variable="temperature",
        day_ahead=1,
        forecasts={persistence_id: 8.0, high_id: 10.2, low_id: 11.0},
    )

    conn.commit()
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

    conn.commit()
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


def test_far_horizon_multipoint_tier_rescues_when_best_below_adequate() -> None:
    """Far tile where the BEST-covered feed is only 8h (below
    the 12h adequate floor) and a 1-sample feed OUT-skills it. This pins the
    second (multipoint) tier of the pool.

    Pre-fix reasoning: pool == candidates, ladder ranks by skill under
    blend_depth=1 -> the single-slot gfs feed (higher skill) is selected alone
    -> the BLEND would collapse to high == low == 25.0. Post-fix (pre-Item-F):
    adequate is empty (8h and 1h both < 12), so the pool falls to the
    multipoint tier = the 8h feed (>= MULTIPOINT_MIN_HOURS; the 1-sample feed
    is below it) -> the blend never collapses. WITHOUT the multipoint tier the
    pool would fall straight to all candidates and re-collapse -- which is
    exactly what this test guards, pinned via ``build_hourly``'s per-feed
    series since ``meta.feeds`` now names the extrema set for temperature
    (Item F), not the blend set.

    Item F: neither feed covers the whole 24-hour local day (partial_id
    supplies 8, degenerate_id 1), so neither is extrema-eligible and the
    daily high/low are suppressed rather than aggregated from partial_id's
    9.0..16.0 range."""
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
    # degenerate OUT-skills partial (forecast nearer observed 10.0).
    _seed_score_cache(
        conn,
        variable="temperature",
        day_ahead=7,
        forecasts={persistence_id: 8.0, degenerate_id: 10.2, partial_id: 11.0},
    )

    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[7]
    assert tile.temp.high_c is None
    assert tile.temp.low_c is None
    assert tile.temp.meta.extrema_unavailable is True
    assert tile.temp.meta.feeds == []

    hourly = build_hourly(conn, site_id=1, timezone="UTC", day=7, now=now)
    hourly_labels = [feed["label"] for feed in hourly["feeds"]]
    # single-slot gated out
    assert feed_label("open-meteo", "gfs_global") not in hourly_labels
    assert feed_label("open-meteo", "ecmwf_ifs") in hourly_labels


def test_far_horizon_two_single_slot_feeds_collapse_at_default_depth() -> None:
    """Depth-2 fidelity pin for the ACTUAL reported production collapse.

    Reproduced at the DEFAULT forecast_blend_depth=2: two single-slot confident
    feeds are blended and mean(maxes) == mean(mins), so high_c == low_c -- the
    mean-of-two collapse, distinct from the depth-1 single-feed collapse the
    tests above pin. Each single-slot feed's daily high == low (one point), and
    two feeds blend by mean-of-highs / mean-of-lows, so the BLEND would
    collapse to mean(7.71, 10.43) == 9.07 for BOTH high and low. Two
    single-slot feeds (7.71, 10.43) out-skill a 15h covered feed on the
    pre-fix ladder.
    Pre-fix: pool == candidates -> top-2-by-skill picks both single slots ->
    the blend collapses. Post-fix (pre-Item-F): the >=12h adequate pool
    (1 member < depth 2, no top-up from outside the pool) restricts to the
    covered feed -> the blend never collapses. Pinned via ``build_hourly``'s
    per-feed series since ``meta.feeds`` now names the extrema set for
    temperature (Item F), not the blend set.

    Item F: none of the three feeds covers the whole 24-hour local day (the
    best, ``covered``, supplies 15), so no candidate is extrema-eligible and
    the daily high/low are suppressed rather than aggregated from
    ``covered``'s partial 9.0..16.0 range.
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
    # Both single-slot feeds out-skill the covered feed (forecast nearer the
    # seeded observed 10.0 => lower MSE => higher skill), so pre-fix the top-2
    # ladder selects BOTH of them and never the covered feed.
    _seed_score_cache(
        conn,
        variable="temperature",
        day_ahead=7,
        forecasts={
            persistence_id: 8.0,
            single_a: 10.1,
            single_b: 10.2,
            covered: 11.0,
        },
    )

    conn.commit()
    view = build_forecast(
        conn, site_id=1, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile = view.tiles[7]  # D+7 far-horizon tile
    assert tile.temp.high_c is None
    assert tile.temp.low_c is None
    assert tile.temp.meta.extrema_unavailable is True
    assert tile.temp.meta.feeds == []

    hourly = build_hourly(conn, site_id=1, timezone="UTC", day=7, now=now)
    hourly_labels = [feed["label"] for feed in hourly["feeds"]]
    # NOT the mean-collapse
    assert feed_label("open-meteo", "gfs_global") not in hourly_labels
    assert feed_label("open-meteo", "icon_global") not in hourly_labels
    assert feed_label("open-meteo", "ecmwf_ifs") in hourly_labels
