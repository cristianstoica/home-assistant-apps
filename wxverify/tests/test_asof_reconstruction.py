"""As-of sample reconstruction (plan §6) — implementer tests.

The daily close's day_scores rows are the only scoring path, so there is
no as-of pair-knowability query to cover here.

What this file covers:

* ``resolve_window(now=T)`` anchors relative cutoffs at T, not wall clock
  (``scoring.leaderboard``, still live under the publication-window path);
* ``load_future_samples(as_of=T)`` bounds by ``issued_at``/``fetched_at``
  BEFORE the latest-run pick, and NULL-availability rows are excluded and
  countable (recorded-reason policy) — the record still reconstructs a
  day's samples as they stood at T (9.5).

All fixture values are synthetic.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.migrations import run_migrations
from wxverify.forecast.data import count_null_availability_samples, load_future_samples
from wxverify.scoring.leaderboard import resolve_window

# Canonical decision time for these fixtures.
_T = "2035-01-02T12:00:00Z"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 40.0, -105.0, 900.0, 'UTC')
        """,
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _make_real_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('example-src', ?, 1, 360, 48)
        """,
        (model,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    value: float,
    fetched_at: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, ?, ?, '{}', 'run-x', ?)
        """,
        (site_id, feed_id, issued_at, valid_at, lead_hours, value, fetched_at),
    )


# ---------------------------------------------------------------------------
# resolve_window(now=T)
# ---------------------------------------------------------------------------


def test_resolve_window_now_anchors_relative_cutoffs_at_t() -> None:
    conn = _conn()
    t = parse_utc(_T)
    assert resolve_window(conn, "7d", now=t).cutoff == isoformat_utc(
        t - timedelta(days=7)
    )
    # Default rolling window (30 d) anchored at T.
    assert resolve_window(conn, "rolling", now=t).cutoff == isoformat_utc(
        t - timedelta(days=30)
    )
    # "all" has no cutoff regardless of the anchor.
    assert resolve_window(conn, "all", now=t).cutoff is None


def test_resolve_window_default_now_is_wall_clock() -> None:
    conn = _conn()
    before = utc_now() - timedelta(days=7, minutes=1)
    cutoff = resolve_window(conn, "7d").cutoff
    after = utc_now() - timedelta(days=7) + timedelta(minutes=1)
    assert cutoff is not None
    assert before < parse_utc(cutoff) < after


# ---------------------------------------------------------------------------
# load_future_samples(as_of=T) + NULL-availability accounting
# ---------------------------------------------------------------------------


def test_load_future_samples_asof_bounds_before_latest_run_pick() -> None:
    conn = _conn()
    # Two labels in one hour exist only in a file below v16 (the rain
    # release, Part B, keeps one row per hour and no interim values): the
    # pick is a no-op on a v16 file.
    conn.execute("DROP INDEX idx_samples_slot")
    site_id = _make_site(conn, "AsOf Samples Site")
    feed_id = _make_real_feed(conn, "model-a")
    valid_at = "2035-01-03T06:00:00Z"
    # Older run: fully available at T.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        issued_at="2035-01-01T00:00:00Z",
        valid_at=valid_at,
        lead_hours=30,
        value=1.0,
        fetched_at="2035-01-01T00:10:00Z",
    )
    # Newer run for the SAME slot, fetched only after T: at T the older run
    # must win the latest-run pick; live (as_of=None) picks the newer one.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        issued_at="2035-01-02T00:00:00Z",
        valid_at=valid_at,
        lead_hours=30,
        value=2.0,
        fetched_at="2035-01-02T18:00:00Z",
    )
    live = load_future_samples(
        conn, site_id=site_id, since_valid_at="2035-01-01T00:00:00Z", served_only=False
    )
    assert [row.value for row in live] == [2.0]
    asof = load_future_samples(
        conn,
        site_id=site_id,
        since_valid_at="2035-01-01T00:00:00Z",
        as_of=_T,
        served_only=False,
    )
    assert [row.value for row in asof] == [1.0]


def test_load_future_samples_asof_excludes_post_t_issuance_and_null_fetched() -> None:
    conn = _conn()
    site_id = _make_site(conn, "AsOf Bounds Site")
    feed_null = _make_real_feed(conn, "model-null")
    feed_late = _make_real_feed(conn, "model-late-issue")
    # NULL fetched_at: cannot be placed on the as-of timeline — excluded
    # under as_of, still visible live, and counted with a recorded reason.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_null,
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-03T06:00:00Z",
        lead_hours=30,
        value=3.0,
        fetched_at=None,
    )
    # issued_at after T (fetched_at adversarially before T): the issued_at
    # bound must exclude it on its own.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_late,
        issued_at="2035-01-02T13:00:00Z",
        valid_at="2035-01-03T06:00:00Z",
        lead_hours=17,
        value=4.0,
        fetched_at="2035-01-02T11:00:00Z",
    )
    live = load_future_samples(
        conn, site_id=site_id, since_valid_at="2035-01-01T00:00:00Z", served_only=False
    )
    assert {row.value for row in live} == {3.0, 4.0}
    asof = load_future_samples(
        conn,
        site_id=site_id,
        since_valid_at="2035-01-01T00:00:00Z",
        as_of=_T,
        served_only=False,
    )
    assert asof == []
    assert (
        count_null_availability_samples(
            conn, site_id=site_id, since_valid_at="2035-01-01T00:00:00Z"
        )
        == 1
    )
