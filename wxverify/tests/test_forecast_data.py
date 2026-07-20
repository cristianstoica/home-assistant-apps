"""Integration tests for ``wxverify.forecast.data`` against a real SQLite DB.

Spec build-sequence step 1 verify hook: "unit tests for ... latest-run pick,
[...] and that excluded feeds never appear." This file owns the SQL-facing
half of that hook (the fallback-ladder half is pure and lives in
test_forecast_selection.py).

Isolation: every test opens its own fresh ``sqlite3.connect(":memory:")`` and
runs ``run_migrations`` (mirrors ``tests/test_scoring_equivalence.py``'s
``_make_db``), so each test gets a real, empty, fully-seeded schema with
guaranteed per-test isolation (no teardown needed for an in-process
``:memory:`` handle — it is discarded with the connection object).

Dates: forecast_pairs valid_at/issued_at fixtures use the year 2035 (matching
``tests/test_web_ui.py``'s convention) because ``forecast_ranking``'s default
``window="rolling"`` computes its cutoff from the REAL wall clock
(``window_cutoff`` -> ``utc_now()``), not an injectable "now" — a same-year
fixture date would silently fall outside the rolling window and start
excluding rows once enough real time passes. A future-dated fixture is always
inside a "last 30 days" window relative to any real run date.

Synthetic data only (public repo): no real site/station identifiers.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.forecast.data import (
    forecast_ranking,
    load_feed_freshness,
    load_future_samples,
    samples_fingerprint,
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


def _seed_cell_pairs(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    variable: str,
    day_ahead: int,
    forecast: float,
) -> None:
    """Insert 3 forecast_pairs rows for one feed at one (variable, day_ahead)
    cell, on the canonical far-future valid_at/lead_hours trio shared by
    every feed in the ranking-exclusion fixture (so `_paired_skill`'s join
    against the persistence feed's OWN rows at the same trio lines up)."""
    for valid_at, lead_hours in zip(
        _FAR_FUTURE_VALID_ATS, _FAR_FUTURE_LEAD_HOURS, strict=True
    ):
        _insert_pair(
            conn,
            feed_id=feed_id,
            variable=variable,
            issued_at="2035-06-30T00:00:00Z",
            valid_at=valid_at,
            lead_hours=lead_hours,
            day_ahead=day_ahead,
            forecast=forecast,
        )


# ---------------------------------------------------------------------------
# load_future_samples
# ---------------------------------------------------------------------------


def test_latest_run_pick_keeps_newest_issued_at_value() -> None:
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


def test_stray_negative_precip_filtered_boundary_zero_included() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="precip",
        issued_at="2026-06-30T00:00:00Z",
        valid_at="2026-07-01T00:00:00Z",
        value=-1.0,
    )
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="precip",
        issued_at="2026-06-30T00:00:00Z",
        valid_at="2026-07-01T01:00:00Z",
        value=0.0,
    )
    rows = load_future_samples(conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z")
    values = [row.value for row in rows]
    assert -1.0 not in values
    assert 0.0 in values
    assert len(rows) == 1


def test_virtual_and_meteoblue_package_samples_excluded_member_included() -> None:
    conn = _make_db()
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    member_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    _insert_sample(
        conn,
        feed_id=persistence_id,
        variable="temperature",
        issued_at="2026-06-30T00:00:00Z",
        valid_at="2026-07-01T00:00:00Z",
        value=5.0,
    )
    _insert_sample(
        conn,
        feed_id=package_id,
        variable="temperature",
        issued_at="2026-06-30T00:00:00Z",
        valid_at="2026-07-01T01:00:00Z",
        value=5.0,
    )
    _insert_sample(
        conn,
        feed_id=member_id,
        variable="temperature",
        issued_at="2026-06-30T00:00:00Z",
        valid_at="2026-07-01T02:00:00Z",
        value=5.0,
    )

    rows = load_future_samples(conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z")
    feed_ids = {row.feed_id for row in rows}
    assert persistence_id not in feed_ids
    assert package_id not in feed_ids
    assert member_id in feed_ids


def test_since_valid_at_boundary_inclusive_at_exclusive_before() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    cutoff = "2026-07-01T00:00:00Z"
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-06-30T00:00:00Z",
        valid_at=cutoff,
        value=1.0,
    )
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-06-29T00:00:00Z",
        valid_at="2026-06-30T23:00:00Z",
        value=2.0,
    )
    rows = load_future_samples(conn, site_id=1, since_valid_at=cutoff)
    values = [row.value for row in rows]
    assert values == [1.0]


# ---------------------------------------------------------------------------
# load_feed_freshness
# ---------------------------------------------------------------------------


def test_stale_boundary_uses_2x_feeds_own_fetch_interval() -> None:
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    not_stale_feed = _feed_id(conn, "open-meteo", "ecmwf_ifs")  # 360 min interval
    stale_feed = _feed_id(conn, "open-meteo", "gfs_global")  # 360 min interval

    # exactly at the 2x threshold -> NOT stale (`<`, not `<=`).
    at_threshold = isoformat_utc(now - timedelta(minutes=720))
    # one minute past the threshold -> stale.
    past_threshold = isoformat_utc(now - timedelta(minutes=721))

    _insert_sample(
        conn,
        feed_id=not_stale_feed,
        variable="temperature",
        issued_at=at_threshold,
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )
    _insert_sample(
        conn,
        feed_id=stale_feed,
        variable="temperature",
        issued_at=past_threshold,
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )

    freshness = load_feed_freshness(conn, site_id=1, now=now)
    assert freshness[not_stale_feed].stale is False
    assert freshness[stale_feed].stale is True


def test_freshness_excludes_virtual_feed_includes_member_feed() -> None:
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    member_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    for feed_id in (persistence_id, member_id):
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable="temperature",
            issued_at=isoformat_utc(now),
            valid_at="2026-07-11T00:00:00Z",
            value=10.0,
        )
    freshness = load_feed_freshness(conn, site_id=1, now=now)
    assert persistence_id not in freshness
    assert member_id in freshness


# ---------------------------------------------------------------------------
# samples_fingerprint
# ---------------------------------------------------------------------------


def test_fingerprint_zero_with_no_samples() -> None:
    conn = _make_db()
    assert samples_fingerprint(conn, site_id=1) == "0"


def test_fingerprint_advances_on_new_sample_stable_otherwise() -> None:
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    fp0 = samples_fingerprint(conn, site_id=1)
    # No mutation -> unchanged (paired negative for the "advances" assertion
    # below: without this, a fingerprint that always changes would also pass).
    assert samples_fingerprint(conn, site_id=1) == fp0

    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-06-30T00:00:00Z",
        valid_at="2026-07-01T00:00:00Z",
        value=10.0,
    )
    fp1 = samples_fingerprint(conn, site_id=1)
    assert int(fp1) > int(fp0)


# ---------------------------------------------------------------------------
# forecast_ranking — exclusion is applied explicitly at the ranking step,
# proven against feeds that are otherwise genuinely eligible (real skill,
# real active-competitor status) so the exclusion cannot pass vacuously.
# ---------------------------------------------------------------------------


def _seed_ranking_exclusion_fixture(conn: sqlite3.Connection) -> dict[str, int]:
    set_setting(conn, "min_n", "3")
    persistence_id = _feed_id(conn, "virtual", "_persistence")
    multimodel_mean_id = _feed_id(conn, "virtual", "_multimodel_mean")
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    member_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    meteoblue_member_id = int(
        conn.execute(
            """
            INSERT INTO feeds
                (source, model, enabled, default_subscribed,
                 fetch_interval_minutes, max_lead_hours, is_virtual)
            VALUES ('meteoblue', 'nems_member', 1, 0, 360, 168, 0)
            """
        ).lastrowid
        or 0
    )
    # Subscribe the meteoblue package at this site so BOTH the package feed
    # AND its member feed clear `active_competitor_clause` -- otherwise their
    # absence from forecast_ranking would be incidental (never eligible in
    # the first place), not proof the explicit exclusion fired.
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled) VALUES (1, ?, 1)",
        (package_id,),
    )

    # Persistence gets a deliberately bad forecast so every other feed's
    # skill (computed against persistence as baseline) is a real, positive
    # number rather than a degenerate 0/0.
    _seed_cell_pairs(
        conn, feed_id=persistence_id, variable="temperature", day_ahead=0, forecast=8.0
    )
    for feed_id in (multimodel_mean_id, package_id, member_id, meteoblue_member_id):
        _seed_cell_pairs(
            conn, feed_id=feed_id, variable="temperature", day_ahead=0, forecast=10.5
        )

    return {
        "persistence": persistence_id,
        "multimodel_mean": multimodel_mean_id,
        "package": package_id,
        "member": member_id,
        "meteoblue_member": meteoblue_member_id,
    }


def test_forecast_ranking_excludes_virtual_and_meteoblue_package_feeds() -> None:
    conn = _make_db()
    ids = _seed_ranking_exclusion_fixture(conn)

    ranking = forecast_ranking(
        conn, site_id=1, variable="temperature", day_ahead=0, window="rolling"
    )

    # Negative: the three excluded categories never appear, even though each
    # is genuinely eligible and confidently scored.
    assert ids["persistence"] not in ranking
    assert ids["multimodel_mean"] not in ranking
    assert ids["package"] not in ranking

    # Paired positive: an ordinary member feed AND a meteoblue MEMBER model
    # (not the package) both appear and are confident -- proving the
    # exclusion targets exactly "virtual OR (meteoblue, multimodel)", not
    # "meteoblue" broadly and not "everything".
    assert ids["member"] in ranking
    assert ranking[ids["member"]].confident is True
    assert ids["meteoblue_member"] in ranking
    assert ranking[ids["meteoblue_member"]].confident is True


def test_forecast_ranking_is_keyed_per_day_ahead_cell() -> None:
    conn = _make_db()
    ids = _seed_ranking_exclusion_fixture(conn)
    # Pairs were seeded only at day_ahead=0; the neighboring cell must be
    # empty -- ranking is not accidentally shared across day_ahead cells.
    ranking_day1 = forecast_ranking(
        conn, site_id=1, variable="temperature", day_ahead=1, window="rolling"
    )
    assert ids["member"] not in ranking_day1
    assert ranking_day1 == {}
