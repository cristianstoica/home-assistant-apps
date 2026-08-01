"""Tests for the read-path latency rewrite's forecast/data.py changes (plan
2026-08-01-read-path-latency, §8.2): ``load_future_samples``'s ROW_NUMBER()
latest-run pick and ``load_feed_freshness``'s candidate-grid rewrite.

Isolation: every test opens its own fresh ``sqlite3.connect(":memory:")`` and
runs ``run_migrations`` (mirrors ``tests/test_forecast_data.py``).

Synthetic data only (public repo): fake site name, the repo's existing
47/25 lat-lon convention, no real station/device identifiers.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

from wxverify.db.migrations import run_migrations
from wxverify.forecast.data import load_feed_freshness, load_future_samples

_FIXED_NOW = datetime(2030, 1, 2, 12, 0, tzinfo=UTC)


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


def _insert_custom_feed(
    conn: sqlite3.Connection, *, model: str, fetch_interval_minutes: int
) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO feeds
                (source, model, enabled, default_subscribed,
                 fetch_interval_minutes, max_lead_hours, is_virtual)
            VALUES ('open-meteo', ?, 1, 1, ?, 168, 0)
            """,
            (model, fetch_interval_minutes),
        ).lastrowid
    )


# ---------------------------------------------------------------------------
# load_future_samples
# ---------------------------------------------------------------------------


def test_load_future_samples_picks_latest_run_per_slot() -> None:
    """Two runs covering the same (feed, variable, valid_at) slot; only the
    newer survives -- the ROW_NUMBER() rewrite's headline contract."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-06-25T00:00:00Z",
        valid_at="2026-07-01T00:00:00Z",
        value=10.0,
    )
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-06-30T00:00:00Z",
        valid_at="2026-07-01T00:00:00Z",
        value=20.0,
    )
    rows = load_future_samples(conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z")
    assert len(rows) == 1
    assert rows[0].value == 20.0
    assert rows[0].issued_at == "2026-06-30T00:00:00Z"


def test_load_future_samples_newer_invalid_does_not_shadow_valid_older() -> None:
    """The validity predicate is applied BEFORE the ROW_NUMBER() window, so
    an invalid sample from a newer run cannot shadow a valid older one --
    the query's own docstring invariant, pinned directly."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-06-25T00:00:00Z",
        valid_at="2026-07-01T00:00:00Z",
        value=20.0,
    )
    # Newer run, but out of FORECAST_VALUE_RANGES for temperature (> 70).
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-06-30T00:00:00Z",
        valid_at="2026-07-01T00:00:00Z",
        value=200.0,
    )
    rows = load_future_samples(conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z")
    assert len(rows) == 1
    assert rows[0].value == 20.0
    assert rows[0].issued_at == "2026-06-25T00:00:00Z"


def test_load_future_samples_excludes_virtual_and_package_feeds() -> None:
    conn = _make_db()
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    member_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    for feed_id, valid_at in (
        (persistence_id, "2026-07-01T00:00:00Z"),
        (package_id, "2026-07-01T01:00:00Z"),
        (member_id, "2026-07-01T02:00:00Z"),
    ):
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable="temperature",
            issued_at="2026-06-30T00:00:00Z",
            valid_at=valid_at,
            value=10.0,
        )

    rows = load_future_samples(conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z")
    feed_ids = {row.feed_id for row in rows}
    assert feed_ids == {member_id}


def test_load_future_samples_orders_by_valid_at_then_feed() -> None:
    conn = _make_db()
    feed_a = _feed_id(conn, "open-meteo", "gfs_global")
    feed_b = _feed_id(conn, "open-meteo", "ecmwf_ifs")  # lower feed id than gfs
    assert feed_b < feed_a

    # Insert out of (valid_at, feed_id) order to prove ORDER BY does the
    # sorting, not insertion order.
    _insert_sample(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at="2026-06-30T00:00:00Z",
        valid_at="2026-07-02T00:00:00Z",
        value=10.0,
    )
    _insert_sample(
        conn,
        feed_id=feed_a,
        variable="temperature",
        issued_at="2026-06-30T00:00:00Z",
        valid_at="2026-07-01T00:00:00Z",
        value=11.0,
    )
    _insert_sample(
        conn,
        feed_id=feed_b,
        variable="temperature",
        issued_at="2026-06-30T00:00:00Z",
        valid_at="2026-07-01T00:00:00Z",
        value=12.0,
    )

    rows = load_future_samples(conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z")
    assert [(row.valid_at, row.feed_id) for row in rows] == [
        ("2026-07-01T00:00:00Z", feed_b),
        ("2026-07-01T00:00:00Z", feed_a),
        ("2026-07-02T00:00:00Z", feed_a),
    ]


# ---------------------------------------------------------------------------
# load_feed_freshness
# ---------------------------------------------------------------------------


def test_load_feed_freshness_omits_feeds_with_no_valid_samples() -> None:
    conn = _make_db()
    silent_feed = _feed_id(conn, "open-meteo", "gfs_global")
    active_feed = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    # Only an INVALID sample for the silent feed (out of range).
    _insert_sample(
        conn,
        feed_id=silent_feed,
        variable="temperature",
        issued_at="2030-01-02T00:00:00Z",
        valid_at="2030-01-03T00:00:00Z",
        value=999.0,
    )
    # Paired positive: a genuinely valid sample for the active feed.
    _insert_sample(
        conn,
        feed_id=active_feed,
        variable="temperature",
        issued_at="2030-01-02T00:00:00Z",
        valid_at="2030-01-03T00:00:00Z",
        value=10.0,
    )
    freshness = load_feed_freshness(conn, site_id=1, now=_FIXED_NOW)
    assert silent_feed not in freshness
    assert active_feed in freshness


def test_load_feed_freshness_uses_per_feed_interval() -> None:
    """Staleness is judged per-feed against its OWN fetch_interval_minutes,
    never a global constant: at the SAME issued_at, a fast-cadence feed past
    its own 2x-interval threshold is genuinely stale, and a slow-cadence
    feed within its own (much larger) threshold is not falsely flagged."""
    conn = _make_db()
    fast_feed = _insert_custom_feed(conn, model="fast_test", fetch_interval_minutes=60)
    slow_feed = _insert_custom_feed(conn, model="slow_test", fetch_interval_minutes=720)
    issued_at = "2030-01-02T08:40:00Z"  # 200 minutes before _FIXED_NOW

    for feed_id in (fast_feed, slow_feed):
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable="temperature",
            issued_at=issued_at,
            valid_at="2030-01-03T00:00:00Z",
            value=10.0,
        )

    freshness = load_feed_freshness(conn, site_id=1, now=_FIXED_NOW)
    # fast: threshold = 2*60 = 120min; 200min old -> stale.
    assert freshness[fast_feed].stale is True
    # slow: threshold = 2*720 = 1440min; 200min old -> NOT stale.
    assert freshness[slow_feed].stale is False


def test_load_feed_freshness_skips_newest_invalid_run() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2030-01-01T00:00:00Z",
        valid_at="2030-01-03T00:00:00Z",
        value=10.0,
    )
    # Newer run, but invalid (out of range) -- must not be reported as the
    # latest issued_at.
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2030-01-02T00:00:00Z",
        valid_at="2030-01-03T01:00:00Z",
        value=999.0,
    )
    freshness = load_feed_freshness(conn, site_id=1, now=_FIXED_NOW)
    assert freshness[feed_id].latest_issued_at == "2030-01-01T00:00:00Z"


def test_load_feed_freshness_reports_the_newest_run_across_variables() -> None:
    """The GROUP BY deliberately excludes variable -- one feed's freshness is
    the MAX(issued_at) across ALL its variables. The newest run is placed on
    "temperature" because it is the middle element under BOTH plausible row
    orderings the old/new query could produce (alphabetical: precip <
    temperature < wind; grid/insertion order: temperature, wind, precip) --
    an implementation that picked the wrong ordering axis (e.g. `MAX` over
    only the FIRST or LAST grid row) would still report the middle element's
    issued_at only if the GROUP BY genuinely spans variables, not merely
    return SOME dict entry for the feed (the untestable "exactly one entry"
    the plan explicitly forbids asserting).

    Insert order is deliberately precip, temperature, wind -- NOT sorted by
    issued_at -- so this also cannot pass by coincidentally picking
    "whichever variable inserted last"."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    newest = "2030-01-02T12:00:00Z"
    older = "2030-01-02T10:00:00Z"
    oldest = "2030-01-02T08:00:00Z"

    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="precip",
        issued_at=oldest,
        valid_at="2030-01-03T00:00:00Z",
        value=1.0,
    )
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at=newest,
        valid_at="2030-01-03T00:00:00Z",
        value=10.0,
    )
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="wind",
        issued_at=older,
        valid_at="2030-01-03T00:00:00Z",
        value=5.0,
    )

    freshness = load_feed_freshness(conn, site_id=1, now=_FIXED_NOW)
    assert freshness[feed_id].latest_issued_at == newest
