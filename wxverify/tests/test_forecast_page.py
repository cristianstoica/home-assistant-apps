"""Tests for the Forecast page's data model (``wxverify.forecast.page``).

Pure checks (``block_icon``, ``hour_columns``) need no database. The rest
build a real, fresh ``:memory:`` database per test (``run_migrations``),
following the pattern already used by ``tests/test_forecast_service.py``
and ``tests/test_rain_tile.py``: plain ``INSERT`` for feeds and samples,
``tests.helpers.seed_active_publication``/``seed_rain_ranks`` for the
publication tables, never through ``publish()`` itself.

Every sample's ``fetched_at`` is set equal to its own ``valid_at`` (the
convention ``tests/test_rain_tile.py``'s ``_insert_hourly`` already uses),
so with the daily close on (the fresh-build default) every hour's
``sample_day_ahead`` resolves to 0 regardless of which display day it is
on -- one published wind weight or ranking at day_ahead=0 then applies
uniformly to display days 0-7, which is what most cases here need.

Synthetic data only: fake site name, synthetic feed sources
(``synthetic-pg-*``), UTC or the suite's established DST zone
(Europe/Athens, matching ``tests/test_forecast_aggregate.py``).
"""

from __future__ import annotations

import sqlite3
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, date, datetime, timedelta
from typing import cast

import pytest

from tests.helpers import seed_active_publication, seed_rain_ranks
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.site_days import DAILY_CLOSE_ON_KEY
from wxverify.forecast.page import (
    CLOUDY_PCT,
    HEAVY_RAIN_MM_PER_HOUR,
    NO_RAIN_TEXT,
    PARTLY_PCT,
    block_icon,
    build_forecast_page,
    hour_columns,
    rain_text,
)
from wxverify.forecast.service import build_hourly
from wxverify.settings.keys import set_setting
from wxverify.web.context import model_label

NOW = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Fixture helpers (own-helpers rule: no import from another test file's
# local helpers, only from tests/helpers.py).
# ---------------------------------------------------------------------------


def _make_db(*, timezone: str = "UTC") -> tuple[sqlite3.Connection, int]:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('Page Test Site', 40.0, -105.0, 900.0, ?)",
            (timezone,),
        ).lastrowid
    )
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(NOW))
    return conn, site_id


def _insert_feed(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES (?, ?, 1, 1, 360, 168, 0)",
        (f"synthetic-pg-{name.lower()}", name.lower()),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    valid_at: str,
    value: float,
    lead_hours: int = 1,
) -> None:
    """One sample with ``fetched_at == issued_at == valid_at`` (the
    day_ahead=0 trick every fixture in this file relies on)."""
    conn.execute(
        "INSERT INTO forecast_samples"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
        "  value, source_raw, model_run_id, fetched_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-1', ?)",
        (site_id, feed_id, variable, valid_at, valid_at, lead_hours, value, valid_at),
    )


def _partial_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    local_date: date,
    hours: Iterable[int],
    value_fn: Callable[[int], float],
    timezone: str = "UTC",
) -> None:
    columns = hour_columns(local_date, timezone)
    for h in hours:
        column = columns[h]
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            valid_at=column.key,
            value=value_fn(h),
            lead_hours=h + 1,
        )


def _full_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    local_date: date,
    value_fn: Callable[[int], float],
    timezone: str = "UTC",
) -> None:
    columns = hour_columns(local_date, timezone)
    _partial_day(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable=variable,
        local_date=local_date,
        hours=range(len(columns)),
        value_fn=value_fn,
        timezone=timezone,
    )


def _seed_wind_weights_into(
    conn: sqlite3.Connection,
    *,
    publication_id: int,
    weights: Mapping[int, float],
    day_ahead: int = 0,
) -> None:
    for feed_id, weight in weights.items():
        conn.execute(
            "INSERT INTO publication_wind_weights"
            " (publication_id, feed_id, day_ahead, mae_ms, days, weight)"
            " VALUES (?, ?, ?, 1.0, 30, ?)",
            (publication_id, feed_id, day_ahead, weight),
        )


def _seed_wind_weights(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    weights: Mapping[int, float],
) -> int:
    publication_id = seed_active_publication(
        conn,
        site_id=site_id,
        cutoff=NOW.date() - timedelta(days=1),
        published_at=NOW,
    )
    _seed_wind_weights_into(conn, publication_id=publication_id, weights=weights)
    return publication_id


# ---------------------------------------------------------------------------
# Icon boundaries (pure: block_icon). Catches: an off-by-one boundary, or a
# strict/non-strict swap.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("rain_values", "cloud_values", "expected"),
    [
        pytest.param([0.194], [], None, id="block-total-0-194-below-trigger"),
        pytest.param([0.195], [], "rain", id="block-total-0-195-at-trigger"),
        pytest.param([1.0], [], "rain", id="wettest-hour-1-0-not-heavy"),
        pytest.param([1.01], [], "heavy", id="wettest-hour-1-01-is-heavy"),
        pytest.param([], [75.0], "partly", id="cloud-75-0-is-partly-not-cloud"),
        pytest.param([], [75.1], "cloud", id="cloud-75-1-is-cloud"),
        pytest.param([], [35.0], "sun", id="cloud-35-0-is-sun-not-partly"),
        pytest.param([], [35.1], "partly", id="cloud-35-1-is-partly"),
    ],
)
def test_icon_boundaries(
    rain_values: list[float],
    cloud_values: list[float],
    expected: str | None,
) -> None:
    """Icon boundaries. Catches: an off-by-one boundary, or a
    strict/non-strict swap."""
    assert block_icon(rain_values, cloud_values) == expected
    # Fixture guard: pin the two constants this test's boundaries are
    # written against, so a changed threshold shows up here, not just as a
    # confusing case id.
    assert HEAVY_RAIN_MM_PER_HOUR == 1.0
    assert CLOUDY_PCT == 75.0
    assert PARTLY_PCT == 35.0


# ---------------------------------------------------------------------------
# DST (pure: hour_columns). Catches: columns built from local wall-clock
# hours.
# ---------------------------------------------------------------------------


def test_dst_spring_forward_day_has_23_columns_with_one_5_hour_block() -> None:
    """DST, spring-forward half. Catches: columns built from local
    wall-clock hours."""
    columns = hour_columns(date(2026, 3, 29), "Europe/Athens")
    assert len(columns) == 23
    block_sizes = Counter(column.block for column in columns)
    assert block_sizes == {0: 5, 1: 6, 2: 6, 3: 6}


def test_dst_fall_back_day_has_25_columns_with_a_7_hour_block() -> None:
    """DST, fall-back half. Catches: columns built from local wall-clock
    hours."""
    columns = hour_columns(date(2026, 10, 25), "Europe/Athens")
    assert len(columns) == 25
    block_sizes = Counter(column.block for column in columns)
    assert block_sizes == {0: 7, 1: 6, 2: 6, 3: 6}


def test_dst_fall_back_repeated_hour_keeps_its_own_two_instants() -> None:
    """DST, fall-back half. Catches: columns built from local wall-clock
    hours."""
    columns = hour_columns(date(2026, 10, 25), "Europe/Athens")
    repeated = [column for column in columns if column.label == "03"]
    assert len(repeated) == 2
    assert repeated[0].instant != repeated[1].instant
    assert repeated[0].key != repeated[1].key


# ---------------------------------------------------------------------------
# Parity: days 0-7, three feeds covering different hours. Catches: a page
# that recomputes a mean, a shifted hour index, or a cache that changes a
# selection.
# ---------------------------------------------------------------------------


def test_parity_blend_cells_and_day_totals_equal_build_hourly_and_the_tile() -> None:
    """Parity. Catches: a page that recomputes a mean, a shifted hour
    index, or a cache that changes a selection."""
    conn, site_id = _make_db()
    f = _insert_feed(conn, "F")
    g = _insert_feed(conn, "G")
    h = _insert_feed(conn, "H")
    set_setting(conn, "forecast_blend_depth", "3")
    publication_id = seed_rain_ranks(
        conn,
        site_id=site_id,
        published_at=NOW,
        ranked={d: [f] for d in range(8)},
    )
    _seed_wind_weights_into(
        conn,
        publication_id=publication_id,
        weights={f: 0.5, g: 0.3, h: 0.2},
    )

    for day in range(8):
        local_date = NOW.date() + timedelta(days=day)
        # Temperature: F the whole day, G the first 18h, H the last 18h.
        _full_day(
            conn,
            site_id=site_id,
            feed_id=f,
            variable="temperature",
            local_date=local_date,
            value_fn=lambda hr: 10.0 + hr,
        )
        _partial_day(
            conn,
            site_id=site_id,
            feed_id=g,
            variable="temperature",
            local_date=local_date,
            hours=range(0, 18),
            value_fn=lambda hr: 12.0 + hr,
        )
        _partial_day(
            conn,
            site_id=site_id,
            feed_id=h,
            variable="temperature",
            local_date=local_date,
            hours=range(6, 24),
            value_fn=lambda hr: 14.0 + hr,
        )
        # Wind: F all day; G the first 21h; H the last 21h (each clears the
        # >=20h weighted-wind eligibility floor on its own).
        _full_day(
            conn,
            site_id=site_id,
            feed_id=f,
            variable="wind",
            local_date=local_date,
            value_fn=lambda hr: 5.0 + hr * 0.1,
        )
        _partial_day(
            conn,
            site_id=site_id,
            feed_id=g,
            variable="wind",
            local_date=local_date,
            hours=range(0, 21),
            value_fn=lambda hr: 7.0 + hr * 0.1,
        )
        _partial_day(
            conn,
            site_id=site_id,
            feed_id=h,
            variable="wind",
            local_date=local_date,
            hours=range(3, 24),
            value_fn=lambda hr: 9.0 + hr * 0.1,
        )
        # Precip: F covers the whole day and is the rain winner every day.
        _full_day(
            conn,
            site_id=site_id,
            feed_id=f,
            variable="precip",
            local_date=local_date,
            value_fn=lambda hr: 0.3 if hr % 6 == 0 else 0.0,
        )

    conn.commit()

    for day in range(8):
        page = build_forecast_page(
            conn,
            site_id=site_id,
            timezone="UTC",
            rain_threshold_mm=0.2,
            selected_day=day,
            now=NOW,
        )
        detail = page.detail
        assert detail is not None
        hourly_payload = build_hourly(
            conn, site_id=site_id, timezone="UTC", day=day, now=NOW
        )
        hours = cast("list[str]", hourly_payload["hours"])
        blend = cast("dict[str, list[float | None]]", hourly_payload["blend"])
        temp_oracle = dict(zip(hours, blend["temp_c"], strict=True))
        wind_oracle = dict(zip(hours, blend["wind_kmh"], strict=True))
        rain_oracle = dict(zip(hours, blend["precip_mm"], strict=True))

        temp_cols = zip(detail.columns, detail.temperature.top.values, strict=True)
        for column, value in temp_cols:
            msg = f"day {day} hour {column.label}"
            assert value == temp_oracle.get(column.key), msg
        wind_cols = zip(detail.columns, detail.wind.top.values, strict=True)
        for column, value in wind_cols:
            msg = f"day {day} hour {column.label}"
            assert value == wind_oracle.get(column.key), msg
        rain_cols = zip(detail.columns, detail.precip.top.values, strict=True)
        for column, value in rain_cols:
            msg = f"day {day} hour {column.label}"
            assert value == rain_oracle.get(column.key), msg

        # BLEND Max/Min and Max, and the rain Total/text, equal the tile's.
        assert detail.temperature.top.day_high == detail.tile.temp.high_c
        assert detail.temperature.top.day_low == detail.tile.temp.low_c
        assert detail.wind.top.day_high == detail.tile.wind.max_kmh
        assert detail.precip.top.day_total == detail.tile.precip.total_mm
        assert detail.precip.top.day_text == rain_text(detail.tile.precip)

        # One embedded model_label check: the model
        # rows, the BLEND tooltip labels, and the rain top row all carry
        # model_label's text, never feed_label's "source / model" form.
        for row in detail.temperature.rows:
            assert " / " not in row.label
        for label in detail.temperature.blend_labels:
            assert " / " not in label
        assert " / " not in detail.precip.top.label
        assert detail.precip.top.label == model_label("synthetic-pg-f", "f")


# ---------------------------------------------------------------------------
# Rain-only feed (adjusted per dispatch: drive build_forecast_page directly,
# not the routes). Catches: a per-feed series built in hourly_day.
# ---------------------------------------------------------------------------


def test_rain_only_feed_stray_temperature_hour_adds_no_blend_value() -> None:
    """Rain-only feed. Catches: a per-feed series built in hourly_day."""
    conn, site_id = _make_db()
    t1 = _insert_feed(conn, "T1")
    t2 = _insert_feed(conn, "T2")
    r = _insert_feed(conn, "R")

    for day in range(8):
        local_date = NOW.date() + timedelta(days=day)
        _full_day(
            conn,
            site_id=site_id,
            feed_id=t1,
            variable="temperature",
            local_date=local_date,
            value_fn=lambda _hr: 10.0,
        )
        _full_day(
            conn,
            site_id=site_id,
            feed_id=t2,
            variable="temperature",
            local_date=local_date,
            value_fn=lambda _hr: 10.0,
        )
        _full_day(
            conn,
            site_id=site_id,
            feed_id=r,
            variable="precip",
            local_date=local_date,
            value_fn=lambda _hr: 0.0,
        )

    # R's one stray temperature sample, day 0, hour 5 -- an hour both T1 and
    # T2 also cover. 69.0 stays inside the valid temperature range
    # (-90..70) while being unmistakably different from the 10.0 baseline.
    stray_hour = 5
    stray_column = hour_columns(NOW.date(), "UTC")[stray_hour]
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=r,
        variable="temperature",
        valid_at=stray_column.key,
        value=69.0,
        lead_hours=stray_hour + 1,
    )
    seed_rain_ranks(
        conn,
        site_id=site_id,
        published_at=NOW,
        ranked={d: [r] for d in range(8)},
    )
    conn.commit()

    for day in range(8):
        page = build_forecast_page(
            conn,
            site_id=site_id,
            timezone="UTC",
            rain_threshold_mm=0.2,
            selected_day=day,
            now=NOW,
        )
        assert page.empty is False

    page0 = build_forecast_page(
        conn,
        site_id=site_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        selected_day=0,
        now=NOW,
    )
    assert page0.detail is not None
    assert page0.detail.temperature.top.values[stray_hour] == 10.0


# ---------------------------------------------------------------------------
# Order: temperature, wind and rain each follow their own ranking; unranked
# feeds come last; the rain winner appears once. Catches: one shared order
# for all grids; the winner listed twice.
# ---------------------------------------------------------------------------


def test_order_each_grid_follows_its_own_ranking_unranked_last_winner_once() -> None:
    """Order. Catches: one shared order for all grids; the winner listed
    twice."""
    conn, site_id = _make_db()
    a = _insert_feed(conn, "A")
    b = _insert_feed(conn, "B")
    c = _insert_feed(conn, "C")
    d = _insert_feed(conn, "D")
    assert a < b < c < d  # feed-id order, for the "differs from" assertions

    local_date = NOW.date()

    # Temperature: no scored pairs for any feed, so the ladder falls to
    # future-sample-count order. A=12h, B=18h, C=24h -> order [C, B, A].
    _partial_day(
        conn,
        site_id=site_id,
        feed_id=a,
        variable="temperature",
        local_date=local_date,
        hours=range(0, 12),
        value_fn=lambda _hr: 10.0,
    )
    _partial_day(
        conn,
        site_id=site_id,
        feed_id=b,
        variable="temperature",
        local_date=local_date,
        hours=range(0, 18),
        value_fn=lambda _hr: 10.0,
    )
    _full_day(
        conn,
        site_id=site_id,
        feed_id=c,
        variable="temperature",
        local_date=local_date,
        value_fn=lambda _hr: 10.0,
    )

    # Wind: full-day coverage for all three (clears the eligibility floor);
    # order comes from the published weight, B > A > C -> order [B, A, C].
    for feed_id in (a, b, c):
        _full_day(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="wind",
            local_date=local_date,
            value_fn=lambda _hr: 10.0,
        )
    publication_id = seed_rain_ranks(
        conn,
        site_id=site_id,
        published_at=NOW,
        ranked={0: [a, c, b]},
    )
    _seed_wind_weights_into(
        conn,
        publication_id=publication_id,
        weights={b: 0.6, a: 0.3, c: 0.1},
    )

    # Rain: A is ranked first and is the only full-day feed, so it is the
    # winner; C and B are ranked after it (partial precip, any hours);
    # D has precip samples but no rain rank at all -- it must still show,
    # last.
    _full_day(
        conn,
        site_id=site_id,
        feed_id=a,
        variable="precip",
        local_date=local_date,
        value_fn=lambda _hr: 0.0,
    )
    _partial_day(
        conn,
        site_id=site_id,
        feed_id=c,
        variable="precip",
        local_date=local_date,
        hours=range(0, 3),
        value_fn=lambda _hr: 0.0,
    )
    _partial_day(
        conn,
        site_id=site_id,
        feed_id=b,
        variable="precip",
        local_date=local_date,
        hours=range(0, 3),
        value_fn=lambda _hr: 0.0,
    )
    _partial_day(
        conn,
        site_id=site_id,
        feed_id=d,
        variable="precip",
        local_date=local_date,
        hours=range(0, 2),
        value_fn=lambda _hr: 0.0,
    )
    conn.commit()

    page = build_forecast_page(
        conn,
        site_id=site_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        selected_day=0,
        now=NOW,
    )
    detail = page.detail
    assert detail is not None

    temp_order = [row.feed_id for row in detail.temperature.rows]
    wind_order = [row.feed_id for row in detail.wind.rows]
    precip_order = [row.feed_id for row in detail.precip.rows]

    assert temp_order == [c, b, a]
    assert wind_order == [b, a, c]
    assert precip_order == [c, b, d]  # the winner, A, excluded; D unranked, last

    feed_id_order = [a, b, c]
    assert temp_order != feed_id_order
    assert wind_order != feed_id_order
    assert temp_order != wind_order

    # The winner appears exactly once: as the top row's label, never again
    # among the model rows.
    assert a not in precip_order
    assert detail.precip.top.label == model_label("synthetic-pg-a", "a")


# ---------------------------------------------------------------------------
# Direction: the top-weight member supplies every arrow unless it has none,
# in which case the next member does, and no lower member ever fills a gap
# the chosen one leaves. Catches: an hour-by-hour switch, or a "sunny"
# fallback (the direction half; the fallback half is covered by cloud).
# ---------------------------------------------------------------------------


def test_direction_skips_empty_top_member_never_backfills_from_a_lower_one() -> None:
    """Direction (half of "Direction and cloud"). Catches: an hour-by-hour
    switch."""
    conn, site_id = _make_db()
    m1 = _insert_feed(conn, "M1")  # top weight, no direction samples at all
    m2 = _insert_feed(conn, "M2")  # next weight, direction for every hour but one
    m3 = _insert_feed(conn, "M3")  # lowest weight, has data only at the gap

    local_date = NOW.date()
    for feed_id in (m1, m2, m3):
        _full_day(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="wind",
            local_date=local_date,
            value_fn=lambda _hr: 10.0,
        )
    _seed_wind_weights(conn, site_id=site_id, weights={m1: 0.6, m2: 0.3, m3: 0.1})

    gap_hour = 10
    columns = hour_columns(local_date, "UTC")
    for h, column in enumerate(columns):
        if h == gap_hour:
            continue
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=m2,
            variable="wind_direction",
            valid_at=column.key,
            value=45.0,
        )
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=m3,
        variable="wind_direction",
        valid_at=columns[gap_hour].key,
        value=315.0,
    )
    conn.commit()

    page = build_forecast_page(
        conn,
        site_id=site_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        selected_day=0,
        now=NOW,
    )
    detail = page.detail
    assert detail is not None
    for h, direction in enumerate(detail.directions):
        if h == gap_hour:
            assert direction is None, "M3's value must never fill M2's gap"
        else:
            assert direction == 45.0


# ---------------------------------------------------------------------------
# Cloud: the rain winner's cloud model, else the next by rain rank, else no
# icon. Catches: a "sunny" fallback.
# ---------------------------------------------------------------------------


def test_cloud_follows_the_rain_winner_then_rank_order_then_gives_no_icon() -> None:
    """Cloud (half of "Direction and cloud"). Catches: a "sunny" fallback."""
    conn, site_id = _make_db()
    w = _insert_feed(conn, "W")  # the rain winner every day
    r2 = _insert_feed(conn, "R2")
    r3 = _insert_feed(conn, "R3")

    for day in range(3):
        local_date = NOW.date() + timedelta(days=day)
        # W is dry and covers the whole day, so it is the winner every day
        # and the rain line never forecasts rain -- block_icon always takes
        # the cloud branch.
        _full_day(
            conn,
            site_id=site_id,
            feed_id=w,
            variable="precip",
            local_date=local_date,
            value_fn=lambda _hr: 0.0,
        )
    seed_rain_ranks(
        conn,
        site_id=site_id,
        published_at=NOW,
        ranked={day: [w, r2, r3] for day in range(3)},
    )

    day0_hour0 = hour_columns(NOW.date(), "UTC")[0].key
    day1_hour0 = hour_columns(NOW.date() + timedelta(days=1), "UTC")[0].key
    # Day 0: the winner (W) itself has cloud -> cloud follows it.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=w,
        variable="cloud_cover",
        valid_at=day0_hour0,
        value=90.0,
    )
    # Day 1: the winner has none, but the next-ranked model (R2) does.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=r2,
        variable="cloud_cover",
        valid_at=day1_hour0,
        value=10.0,
    )
    # Day 2: nobody has any cloud sample at all.
    conn.commit()

    page = build_forecast_page(
        conn,
        site_id=site_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        selected_day=0,
        now=NOW,
    )

    def _block0(day_index: int) -> str | None:
        cell = next(
            c for c in page.timeline if c.day_index == day_index and c.block == 0
        )
        return cell.icon

    assert _block0(0) == "cloud"  # winner's own cloud, 90% -> cloudy
    assert _block0(1) == "sun"  # falls to the next-ranked model, 10% -> sun
    assert _block0(2) is None  # no cloud model at all -> no icon


# ---------------------------------------------------------------------------
# No served rain model: the day's rain reads "No rain forecast", and the
# sky icon still comes from the cloud model where it has data. Catches: a
# rain-unknown day forced to no icon, or a rain icon from an unserved model.
# ---------------------------------------------------------------------------


def test_no_served_rain_model_still_shows_the_sky_icon_from_the_cloud_model() -> None:
    """No served rain model. Catches: a rain-unknown day forced to no icon,
    or a rain icon from an unserved model."""
    conn, site_id = _make_db()
    r1 = _insert_feed(conn, "R1")
    r2 = _insert_feed(conn, "R2")

    local_date = NOW.date()
    columns = hour_columns(local_date, "UTC")
    # Neither ranked model covers the whole local day -- there is no rain
    # winner at all.
    _partial_day(
        conn,
        site_id=site_id,
        feed_id=r1,
        variable="precip",
        local_date=local_date,
        hours=range(0, 10),
        value_fn=lambda _hr: 0.0,
    )
    seed_rain_ranks(conn, site_id=site_id, published_at=NOW, ranked={0: [r1, r2]})

    # R1 (the first-ranked model) has cloud at hour 0 (block 0) but none at
    # hour 6 (block 1).
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=r1,
        variable="cloud_cover",
        valid_at=columns[0].key,
        value=90.0,
    )
    conn.commit()

    page = build_forecast_page(
        conn,
        site_id=site_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        selected_day=0,
        now=NOW,
    )
    detail = page.detail
    assert detail is not None
    assert detail.tile.precip.total_mm is None
    assert rain_text(detail.tile.precip) == NO_RAIN_TEXT

    def _block(index: int) -> str | None:
        cell = next(c for c in page.timeline if c.day_index == 0 and c.block == index)
        return cell.icon

    assert _block(0) == "cloud"
    assert _block(1) is None


# ---------------------------------------------------------------------------
# Gust: a renormalized weighted mix, never below the served wind, absent
# with no member gust. Catches: unrescaled weights, or a missing max.
# ---------------------------------------------------------------------------


def test_gust_is_a_renormalized_mix_floored_at_the_wind_and_absent_with_none() -> None:
    """Gust. Catches: unrescaled weights, or a missing max."""
    conn, site_id = _make_db()
    m1 = _insert_feed(conn, "M1")  # weight 0.75
    m2 = _insert_feed(conn, "M2")  # weight 0.25

    local_date = NOW.date()
    columns = hour_columns(local_date, "UTC")
    for feed_id in (m1, m2):
        _full_day(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="wind",
            local_date=local_date,
            value_fn=lambda _hr: 10.0,  # served wind = 36 km/h, both members
        )
    _seed_wind_weights(conn, site_id=site_id, weights={m1: 0.75, m2: 0.25})

    def _gust(feed_id: int, hour: int, value: float) -> None:
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="wind_gust",
            valid_at=columns[hour].key,
            value=value,
        )

    _gust(m1, 0, 20.0)  # both present: 0.75*72 + 0.25*14.4 = 57.6 km/h
    _gust(m2, 0, 4.0)
    _gust(m2, 1, 12.0)  # M1 missing: the hour is M2's own gust, 43.2 km/h
    _gust(m1, 2, 2.0)  # both present, mix (7.2 km/h) below the served wind
    _gust(m2, 2, 2.0)
    # hour 3: neither member has a gust.
    conn.commit()

    page = build_forecast_page(
        conn,
        site_id=site_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        selected_day=0,
        now=NOW,
    )
    detail = page.detail
    assert detail is not None
    assert detail.gusts[0] == pytest.approx(57.6)
    assert detail.gusts[1] == pytest.approx(43.2)
    assert detail.gusts[2] == pytest.approx(36.0)  # floored to the served wind
    assert detail.gusts[3] is None
