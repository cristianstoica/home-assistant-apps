"""Integration tests for ``wxverify.forecast.data`` against a real SQLite DB.

This file covers unit tests for ... latest-run pick,
[...] and that excluded feeds never appear. This file owns the SQL-facing
half of that (the fallback-ladder half is pure and lives in
test_forecast_selection.py).

Isolation: every test opens its own fresh ``sqlite3.connect(":memory:")`` and
runs ``run_migrations`` (mirrors ``tests/test_scoring_equivalence.py``'s
``_make_db``), so each test gets a real, empty, fully-seeded schema with
guaranteed per-test isolation (no teardown needed for an in-process
``:memory:`` handle — it is discarded with the connection object).

Dates: publication cutoff/published_at fixtures use the year 2035 (matching
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
from datetime import UTC, date, datetime, timedelta

from tests.helpers import seed_active_publication, seed_publication_ranking
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.forecast.data import (
    forecast_ranking_with_status,
    load_feed_freshness,
    load_future_samples,
    samples_fingerprint,
)
from wxverify.scoring.metrics import MetricSums, metrics_from_sums
from wxverify.settings.keys import get_number_setting, set_setting


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


def _seed_cell_pairs(
    forecasts: dict[int, float],
    *,
    feed_id: int,
    forecast: float,
) -> None:
    """Record one feed's forecast at one (variable, day_ahead) cell, into
    the caller's OWN local ``forecasts`` dict, for
    ``_seed_complete_score_cache`` to hand-derive ``MetricSums`` from
    (the canonical far-future valid_at/lead_hours trio this fixture always
    uses, so persistence's own error over the same 3 valid_ats stays the
    skill baseline). No row is written: nothing reads a pair table for
    scoring any more. ``forecasts`` is local to
    the caller, never module-level state, so no connection's values can
    ever leak into another's."""
    forecasts[feed_id] = forecast


# ---------------------------------------------------------------------------
# load_future_samples
# ---------------------------------------------------------------------------


def test_latest_run_pick_keeps_newest_issued_at_value() -> None:
    conn = _make_db()
    # Two labels in one hour exist only in a file below v16 (the rain
    # release, Part B, keeps one row per hour): the pick is a no-op on a
    # v16 file, and the v16 carry-over keeps the row it picks.
    conn.execute("DROP INDEX idx_samples_slot")
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
    rows = load_future_samples(
        conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
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
    rows = load_future_samples(
        conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
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

    rows = load_future_samples(
        conn, site_id=1, since_valid_at="2026-01-01T00:00:00Z", served_only=False
    )
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
    rows = load_future_samples(
        conn, site_id=1, since_valid_at=cutoff, served_only=False
    )
    values = [row.value for row in rows]
    assert values == [1.0]


# ---------------------------------------------------------------------------
# load_feed_freshness
# ---------------------------------------------------------------------------


def test_stale_boundary_uses_2x_feeds_own_fetch_interval() -> None:
    """The boundary is on ``last_usable_fetch_at``, not on ``issued_at``.

    Both feeds' only run was issued two days ago, which the ``issued_at``
    rule would call stale for both; the stamps alone decide the verdict.
    """
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    not_stale_feed = _feed_id(conn, "open-meteo", "ecmwf_ifs")  # 360 min interval
    stale_feed = _feed_id(conn, "open-meteo", "gfs_global")  # 360 min interval

    # exactly at the 2x threshold -> NOT stale (`<`, not `<=`).
    at_threshold = isoformat_utc(now - timedelta(minutes=720))
    # one minute past the threshold -> stale.
    past_threshold = isoformat_utc(now - timedelta(minutes=721))
    old_run = isoformat_utc(now - timedelta(days=2))

    for feed_id, stamp in (
        (not_stale_feed, at_threshold),
        (stale_feed, past_threshold),
    ):
        _insert_sample(
            conn,
            feed_id=feed_id,
            variable="temperature",
            issued_at=old_run,
            valid_at="2026-07-11T00:00:00Z",
            value=10.0,
        )
        _stamp_usable_fetch(conn, feed_id=feed_id, at=stamp)

    freshness = load_feed_freshness(conn, site_id=1, now=now)
    assert freshness[not_stale_feed].fetch_state == "fresh"
    assert freshness[not_stale_feed].stale is False
    assert freshness[stale_feed].fetch_state == "stale"
    assert freshness[stale_feed].stale is True


def test_stale_boundary_one_second_past_2x_interval_flips_to_stale() -> None:
    """Plan §14.6 "Boundary": ``now == stamp + 2 x interval`` is fresh; one
    second later is stale -- the finer-grained sibling of
    ``test_stale_boundary_uses_2x_feeds_own_fetch_interval`` above, which
    pins the same ``<`` (not ``<=``) boundary at minute granularity."""
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")  # 360 min interval
    stamp = "2026-07-10T00:00:00Z"
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at=stamp,
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )
    _stamp_usable_fetch(conn, feed_id=feed_id, at=stamp)

    # +720 min exactly
    exactly_at_boundary = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)
    freshness = load_feed_freshness(conn, site_id=1, now=exactly_at_boundary)
    assert freshness[feed_id].fetch_state == "fresh"

    one_second_later = datetime(2026, 7, 10, 12, 0, 1, tzinfo=UTC)
    assert (
        load_feed_freshness(conn, site_id=1, now=one_second_later)[feed_id].fetch_state
        == "stale"
    )


def test_meteoblue_multimodel_feed_seed_fetch_interval_is_360() -> None:
    """Pin: the seeded ``meteoblue``/``multimodel`` row in ``FEED_SEEDS``
    keeps its ``fetch_interval_minutes`` at 360 -- the package's set call
    times do not depend on this value, but its own freshness boundary
    and retry floor still do, and this release does not change it."""
    from wxverify import config

    seed = next(
        s
        for s in config.FEED_SEEDS
        if s.source == "meteoblue" and s.model == "multimodel"
    )
    assert seed.fetch_interval_minutes == 360, (
        f"at this assertion: correct = 360, got = {seed.fetch_interval_minutes}"
    )


def test_meteoblue_package_fresh_across_the_autumn_gap_stale_past_12h() -> None:
    """A subscribed member feed of the package (``meteoblue``/
    ``multimodel``, the seeded 360-minute interval, a 12-hour 2x boundary),
    judged on the package's own stamp (its evidence feed) rather than its
    own. Aged 9 and 10 hours -- the longest gap an autumn clock-change
    day's three set slots can leave between calls -- the member (riding
    the package's cadence) stays fresh; at the 12-hour boundary itself
    still fresh; one second past it, stale."""
    conn = _make_db()
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    member_id = _insert_meteoblue_member(conn, "member_feed")
    stamp = datetime(2026, 7, 10, 0, 0, tzinfo=UTC)
    _insert_sample(
        conn,
        feed_id=member_id,
        variable="temperature",
        issued_at=isoformat_utc(stamp),
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )
    _stamp_usable_fetch(conn, feed_id=package_id, at=isoformat_utc(stamp))

    for hours, seconds, expected in (
        (9, 0, "fresh"),
        (10, 0, "fresh"),
        (12, 0, "fresh"),
        (12, 1, "stale"),
    ):
        now = stamp + timedelta(hours=hours, seconds=seconds)
        freshness = load_feed_freshness(conn, site_id=1, now=now)
        assert freshness[member_id].fetch_state == expected, (
            f"at this assertion ({hours}h{seconds}s): correct = {expected!r}"
            " (member, via the package's own evidence), got ="
            f" {freshness[member_id].fetch_state!r}"
        )


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


def test_freshness_survives_hostile_stored_cadence() -> None:
    """A feed with a rejected ``fetch_interval_minutes`` must still surface
    in the freshness map -- as ``stale`` with ``fetch_interval_minutes=None``
    -- rather than raising or silently dropping the row."""
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    bad_feed = _feed_id(conn, "open-meteo", "gfs_global")
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 0 WHERE id = ?", (bad_feed,)
    )
    _insert_sample(
        conn,
        feed_id=bad_feed,
        variable="temperature",
        issued_at=isoformat_utc(now),
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )

    freshness = load_feed_freshness(conn, site_id=1, now=now)

    assert freshness[bad_feed].stale is True
    assert freshness[bad_feed].fetch_interval_minutes is None


def test_freshness_survives_non_integral_real_stored_cadence() -> None:
    """A fractional REAL (e.g. an imported/hand-edited 1.9) must surface as
    stale with ``fetch_interval_minutes=None``, not be silently floored."""
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    bad_feed = _feed_id(conn, "open-meteo", "gfs_global")
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 1.9 WHERE id = ?", (bad_feed,)
    )
    _insert_sample(
        conn,
        feed_id=bad_feed,
        variable="temperature",
        issued_at=isoformat_utc(now),
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )

    freshness = load_feed_freshness(conn, site_id=1, now=now)

    assert freshness[bad_feed].stale is True
    assert freshness[bad_feed].fetch_interval_minutes is None


def test_freshness_accepts_exact_integral_real_stored_cadence() -> None:
    """Regression pin: a whole-number cadence written via a REAL literal
    (e.g. 360.0) -- stored as the integer 360 by this INTEGER-affinity
    column -- is a legitimate cadence and must surface with that interval,
    using it for the stale boundary rather than being rejected. The
    genuine REAL-carrier acceptance is pinned at the unit layer in
    test_cadence_parse.py::test_accepts_exact_integral_real."""
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 360.0 WHERE id = ?", (feed_id,)
    )
    at_threshold = isoformat_utc(now - timedelta(minutes=720))
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at=at_threshold,
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )
    _stamp_usable_fetch(conn, feed_id=feed_id, at=at_threshold)

    freshness = load_feed_freshness(conn, site_id=1, now=now)

    assert freshness[feed_id].fetch_interval_minutes == 360
    assert freshness[feed_id].fetch_state == "fresh"


def test_freshness_unparseable_text_stamp_reads_unknown_not_raise() -> None:
    """A syntactically-``str`` but unparseable stamp -- e.g. one arriving
    through the admin DB import path -- must fail toward ``unknown``, not
    raise. Distinct from ``test_freshness_blob_stamp_reads_unknown_not_stale``
    elsewhere in this module: that one pins the ``isinstance(str)`` guard on
    a non-str value (``stamp is None`` branch); this one pins ``parse_utc``'s own
    ``ValueError`` branch on a value that passes the isinstance check.

    Mutant (plan §14.6, MX4): removing the ``try/except ValueError`` around
    ``parse_utc(stamp)`` would raise instead of returning ``"unknown"``.
    """
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at=isoformat_utc(now),
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )
    _stamp_usable_fetch(conn, feed_id=feed_id, at="not-a-time")

    freshness = load_feed_freshness(conn, site_id=1, now=now)

    assert freshness[feed_id].fetch_state == "unknown"


def _insert_meteoblue_member(
    conn: sqlite3.Connection, model: str, *, fetch_interval_minutes: int = 360
) -> int:
    conn.execute(
        """
        INSERT INTO feeds
            (source, model, enabled, default_subscribed, fetch_interval_minutes,
             max_lead_hours, is_virtual)
        VALUES ('meteoblue', ?, 1, 0, ?, 168, 0)
        """,
        (model, fetch_interval_minutes),
    )
    return _feed_id(conn, "meteoblue", model)


def test_freshness_null_stamp_reads_unknown() -> None:
    """A feed with no ``last_usable_fetch_at`` row at all (never fetched
    forward) must read ``unknown``, not ``stale`` -- paired against the
    2x-boundary test above, which pins the ``stale`` branch on the same
    helper."""
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at=isoformat_utc(now),
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )
    # Deliberately no _stamp_usable_fetch call: site_feed_state has no row.

    freshness = load_feed_freshness(conn, site_id=1, now=now)

    assert freshness[feed_id].fetch_state == "unknown"
    assert freshness[feed_id].last_usable_fetch_at is None


def test_freshness_blob_stamp_reads_unknown_not_stale() -> None:
    """A corrupt/foreign BLOB in ``last_usable_fetch_at`` must fail toward
    ``unknown`` (isinstance(str) guard), never toward a str-only comparison
    blowing up or silently misreading as stale."""
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at=isoformat_utc(now),
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, last_usable_fetch_at)
        VALUES (1, ?, ?)
        """,
        (feed_id, b"\x00\x01binary-garbage"),
    )

    freshness = load_feed_freshness(conn, site_id=1, now=now)

    assert freshness[feed_id].fetch_state == "unknown"
    assert freshness[feed_id].last_usable_fetch_at is None


def test_meteoblue_member_follows_package_cadence_and_stamp() -> None:
    """Baseline meteoblue-package-as-evidence-feed case: package row exists
    and carries the stamp; the member is judged against ITS cadence too, not
    just its stamp.

    The package (360 min, 2x-boundary 12h) and the member (1440 min,
    2x-boundary 48h) are given deliberately different cadences, and the
    stamp is aged 13h -- stale under the package's 12h boundary but still
    fresh under the member's 48h one. A reader that read the package's
    stamp but the member's OWN ``fetch_interval_minutes`` would report
    fresh instead of stale.

    Mutant: the reader computes staleness against ``f.fetch_interval_minutes``
    (the member's own row) instead of ``e.fetch_interval_minutes`` (the
    evidence/package row). Must be killed.
    """
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 360 WHERE id = ?", (package_id,)
    )
    member_id = _insert_meteoblue_member(
        conn, "nems_member", fetch_interval_minutes=1440
    )
    stamp = isoformat_utc(now - timedelta(hours=13))
    _insert_sample(
        conn,
        feed_id=member_id,
        variable="temperature",
        issued_at=stamp,
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )
    _stamp_usable_fetch(conn, feed_id=package_id, at=stamp)

    freshness = load_feed_freshness(conn, site_id=1, now=now)

    assert freshness[member_id].evidence_feed_id == package_id
    assert freshness[member_id].fetch_interval_minutes == 360
    assert freshness[member_id].fetch_state == "stale"


def test_meteoblue_member_falls_back_to_own_state_when_package_absent() -> None:
    """Case 2(a): no ``(meteoblue, multimodel)`` package row exists at all
    (e.g. deleted -- a merely-disabled package row still exists and is
    still chosen by the join; only a genuinely MISSING row triggers this
    fallback). A member feed then reads its OWN site_feed_state row as
    evidence: ``evidence_feed_id == member id``.
    """
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    # Delete the default-seeded package before it accrues any state/sample
    # rows so the RESTRICT FK never fires.
    conn.execute("DELETE FROM feeds WHERE source='meteoblue' AND model='multimodel'")
    assert (
        conn.execute(
            "SELECT id FROM feeds WHERE source='meteoblue' AND model='multimodel'"
        ).fetchone()
        is None
    )

    member_id = _insert_meteoblue_member(conn, "nems_member")
    _insert_sample(
        conn,
        feed_id=member_id,
        variable="temperature",
        issued_at=isoformat_utc(now),
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )
    _stamp_usable_fetch(conn, feed_id=member_id, at=isoformat_utc(now))

    freshness = load_feed_freshness(conn, site_id=1, now=now)

    assert freshness[member_id].evidence_feed_id == member_id
    assert freshness[member_id].fetch_state == "fresh"


def test_meteoblue_member_reads_unknown_when_package_has_no_state_row() -> None:
    """Case 2(b): the package row exists but has no site_feed_state row for
    this site, while the member has its OWN recent stamp. The member must
    still read ``unknown`` with ``evidence_feed_id == package id`` -- it
    must never borrow its own stamp just because one exists."""
    conn = _make_db()
    now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    member_id = _insert_meteoblue_member(conn, "nems_member")
    _insert_sample(
        conn,
        feed_id=member_id,
        variable="temperature",
        issued_at=isoformat_utc(now),
        valid_at="2026-07-11T00:00:00Z",
        value=10.0,
    )
    # The member's OWN state row carries a fresh-looking stamp -- but it
    # must never be consulted, since its evidence feed is the package.
    _stamp_usable_fetch(conn, feed_id=member_id, at=isoformat_utc(now))
    # Deliberately no site_feed_state row for the package (no
    # _stamp_usable_fetch(feed_id=package_id, ...) call).
    assert (
        conn.execute(
            "SELECT 1 FROM site_feed_state WHERE site_id=1 AND feed_id=?",
            (package_id,),
        ).fetchone()
        is None
    )

    freshness = load_feed_freshness(conn, site_id=1, now=now)

    assert freshness[member_id].evidence_feed_id == package_id
    assert freshness[member_id].fetch_state == "unknown"
    assert freshness[member_id].last_usable_fetch_at is None


def test_polling_phase_boundary_pins_fresh_then_stale_across_days() -> None:
    """Plan §14.6 "Polling phase" bullet, verbatim scenario: a 360-min (6h)
    feed whose samples are issued at 06:00Z, fetched (stamped) at 12:44Z the
    same day.

    At 23:00Z it is fresh: 10h16m since the STAMP, under the 2x/12h
    threshold. The old issued_at-based rule would call this stale (17h
    since 06:00Z issued_at > 12h) -- this test pins the change to
    stamp-based judgment, not issued_at-based. At 00:45Z the next day it is
    stale: 12h01m since the stamp.

    Mutant: judging staleness from ``latest_issued_at`` (06:00Z) instead of
    the ``last_usable_fetch_at`` stamp (12:44Z) would already read stale at
    23:00Z (17h > 12h). Must be killed.
    """
    conn = _make_db()
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")  # 360 min interval
    issued_at = "2026-07-10T06:00:00Z"
    stamp = "2026-07-10T12:44:00Z"
    _insert_sample(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at=issued_at,
        valid_at="2026-07-11T06:00:00Z",
        value=10.0,
    )
    _stamp_usable_fetch(conn, feed_id=feed_id, at=stamp)

    still_fresh_now = datetime(2026, 7, 10, 23, 0, tzinfo=UTC)  # 10h16m since stamp
    assert (
        load_feed_freshness(conn, site_id=1, now=still_fresh_now)[feed_id].fetch_state
        == "fresh"
    )

    now_stale = datetime(2026, 7, 11, 0, 45, tzinfo=UTC)  # 12h01m since stamp
    assert (
        load_feed_freshness(conn, site_id=1, now=now_stale)[feed_id].fetch_state
        == "stale"
    )


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


def _seed_ranking_exclusion_fixture(
    conn: sqlite3.Connection,
) -> tuple[dict[str, int], dict[int, float]]:
    """Returns ``(ids, forecasts)``: ``forecasts`` is local to this call,
    for the caller to pass straight into ``_seed_complete_score_cache`` --
    never module-level state, so no connection's values can leak into
    another's."""
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
    forecasts: dict[int, float] = {}
    _seed_cell_pairs(forecasts, feed_id=persistence_id, forecast=8.0)
    for feed_id in (multimodel_mean_id, package_id, member_id, meteoblue_member_id):
        _seed_cell_pairs(forecasts, feed_id=feed_id, forecast=10.5)

    return (
        {
            "persistence": persistence_id,
            "multimodel_mean": multimodel_mean_id,
            "package": package_id,
            "member": member_id,
            "meteoblue_member": meteoblue_member_id,
        },
        forecasts,
    )


_TEST_PUBLICATION_WINDOW_DAYS = 30
_TEST_PUBLICATION_WINDOW_KEY = f"w:{_TEST_PUBLICATION_WINDOW_DAYS}"


def _ensure_test_publication(conn: sqlite3.Connection, *, min_n: int) -> int:
    """This connection's one site-1 publication, created on first use.

    ``forecast_ranking``'s ``rolling`` window now always reads the active
    publication (9.2), never a live recompute, so every ranking this file
    seeds must land in ONE publication -- its ``min_n`` is baked in at
    creation, matching the runtime ``min_n`` setting at that point, so the
    read-time confidence check agrees with what
    :func:`_seed_complete_score_cache` computed.
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


def _seed_complete_score_cache(
    conn: sqlite3.Connection,
    *,
    forecasts: dict[int, float],
    variable: str,
    day_ahead: int,
) -> None:
    """Seed a fresh, complete published-ranking snapshot for exactly the
    feeds in `forecasts` at one (variable, day_ahead) cell.

    `forecast_ranking_with_status`'s `rolling` window now always reads the
    active publication (9.2): a feed set that does not match the query's
    expected-active-feed set exactly degrades to a 'rebuilding' empty
    ranking rather than the live-recomputed values these tests assert on.
    The sums below are hand-derived exactly
    from the 3 identical far-future pairs `_seed_cell_pairs` wrote into
    `forecasts` for each feed (a fixed observed=10.0 baseline) through the
    surviving pure `metrics_from_sums`, skill-paired 1:1 against
    persistence's own (also constant) error over the same 3 valid_ats --
    reproducing the same numbers, just not via the removed reader.
    `forecasts` is local to the caller: no module-level state survives this
    call, so a stale `id(conn)` reuse across tests can never leak one
    test's values into another's.
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


def test_forecast_ranking_excludes_virtual_and_meteoblue_package_feeds() -> None:
    conn = _make_db()
    ids, forecasts = _seed_ranking_exclusion_fixture(conn)
    _seed_complete_score_cache(
        conn, forecasts=forecasts, variable="temperature", day_ahead=0
    )

    conn.commit()
    ranking = forecast_ranking_with_status(
        conn, site_id=1, variable="temperature", day_ahead=0, window="rolling"
    ).rows

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
    ids, forecasts = _seed_ranking_exclusion_fixture(conn)
    _seed_complete_score_cache(
        conn, forecasts=forecasts, variable="temperature", day_ahead=0
    )
    # Pairs (and the published ranking) were seeded only at day_ahead=0; the
    # neighboring cell must be empty -- ranking is not accidentally shared
    # across day_ahead cells.
    conn.commit()
    ranking_day0 = forecast_ranking_with_status(
        conn, site_id=1, variable="temperature", day_ahead=0, window="rolling"
    ).rows
    ranking_day1 = forecast_ranking_with_status(
        conn, site_id=1, variable="temperature", day_ahead=1, window="rolling"
    ).rows
    # Paired positive: cell 0, under the SAME publication, is non-empty --
    # the cell-1 emptiness above isn't a vacuous "nothing is ever seeded".
    assert ids["member"] in ranking_day0
    assert ids["member"] not in ranking_day1
    assert ranking_day1 == {}
