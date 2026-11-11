"""Query-plan tests for the two forecast_samples reads the Forecast page
depends on: ``DISPLAY_SAMPLES_SQL`` (``forecast/data.py``) and
``future_samples_sql()``. Relationship checks, never one
planner's exact wording (the pattern of
``tests/test_query_plan_regressions.py``): both statements must plan a
SEARCH of ``forecast_samples`` (shown under its query alias, ``fs``) whose
key ends in a ``valid_at`` range bound, through one of the indexes the
loader can use for it, and neither statement may fall back to a bare SCAN.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime, timedelta

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.forecast.data import DISPLAY_SAMPLES_SQL, future_samples_sql

# The key's trailing column must be a range bound on valid_at, either ">?"
# or ">=?" (the adjustment: assert the relationship, never one planner's
# phrasing for the operator). "fs" is the query's own alias for
# forecast_samples -- EXPLAIN QUERY PLAN names the alias, not the table.
_SEEK_RE = re.compile(
    r"SEARCH fs USING (?:COVERING )?INDEX "
    r"(idx_samples_site_var_valid|idx_samples_slot) "
    r"\([^)]*valid_at>=?\?\)$"
)


def _fresh_conn() -> sqlite3.Connection:
    # cached_statements=0: EXPLAIN QUERY PLAN reports the plan baked into an
    # already-compiled statement, so a cached statement from a prior query on
    # this connection could otherwise report a stale plan (the pattern of
    # test_query_plan_regressions.py's own _fresh_conn).
    conn = sqlite3.connect(":memory:", cached_statements=0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _plan(conn: sqlite3.Connection, sql: str, params: tuple[object, ...]) -> list[str]:
    return [
        str(row["detail"]) for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params)
    ]


def _insert_feed(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES (?, ?, 1, 1, 360, 168, 0)",
        (f"synthetic-pgq-{name.lower()}", name.lower()),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_rows(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    count: int,
) -> None:
    """``count`` hourly rows from 2020-01-01T00:00Z, then ANALYZE so the
    planner sees real cardinality rather than an empty, statistics-free
    table (the pattern of test_query_plan_regressions.py's own seeding)."""
    base = datetime(2020, 1, 1, tzinfo=UTC)
    stamps = [isoformat_utc(base + timedelta(hours=i)) for i in range(count)]
    conn.executemany(
        "INSERT INTO forecast_samples"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
        "  value, source_raw, model_run_id, fetched_at)"
        " VALUES (?, ?, ?, ?, ?, 24, 10.0, '{}', 'run-1', ?)",
        [(site_id, feed_id, variable, ts, ts, ts) for ts in stamps],
    )
    conn.execute("ANALYZE")


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 40.0, -105.0, 900.0, 'UTC')",
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def test_display_samples_sql_seeks_forecast_samples_ending_in_valid_at() -> None:
    """Query plans (half: DISPLAY_SAMPLES_SQL). Catches: a SCAN, or a seek
    that stops before valid_at."""
    conn = _fresh_conn()
    site_id = _make_site(conn, "QueryPlanDisplay")
    feed_id = _insert_feed(conn, "F")
    _seed_rows(
        conn, site_id=site_id, feed_id=feed_id, variable="cloud_cover", count=300
    )

    plan = _plan(conn, DISPLAY_SAMPLES_SQL, (site_id, "2020-01-01T00:00:00Z"))
    assert any(_SEEK_RE.search(line) for line in plan), plan
    assert not any("SCAN fs" in line for line in plan), plan


def test_future_samples_sql_seeks_forecast_samples_ending_in_valid_at() -> None:
    """Query plans (half: future_samples_sql()). Catches: a
    SCAN, or a seek that stops before valid_at. Fixture: rain samples on
    both sides of the hour-start boundary used as the ``since_valid_at``
    parameter."""
    conn = _fresh_conn()
    site_id = _make_site(conn, "QueryPlanFuture")
    feed_id = _insert_feed(conn, "G")
    boundary = datetime(2020, 1, 5, 0, 0, tzinfo=UTC)
    rows = [
        (site_id, feed_id, "precip", isoformat_utc(boundary + timedelta(hours=offset)))
        for offset in range(-30, 30)
    ]
    conn.executemany(
        "INSERT INTO forecast_samples"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours,"
        "  value, source_raw, model_run_id, fetched_at)"
        " VALUES (?, ?, ?, ?, ?, 24, 0.3, '{}', 'run-1', ?)",
        [(s, fid, var, ts, ts, ts) for s, fid, var, ts in rows],
    )
    conn.execute("ANALYZE")

    sql = future_samples_sql()
    plan = _plan(conn, sql, (site_id, isoformat_utc(boundary)))
    assert any(_SEEK_RE.search(line) for line in plan), plan
    assert not any("SCAN fs" in line for line in plan), plan
