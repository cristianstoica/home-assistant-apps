"""Query-plan regression tests for the provider-health and win-rate hot reads.

Each statement is imported from production rather than retyped, and each
positive assertion is paired with a negative control that empirically
degrades to a worse plan -- proving the assertion is not vacuously true on
this fixture. Index structure is asserted via ``PRAGMA index_xinfo``, never a
hardcoded DDL string, so the test tracks the real index rather than a copy of
it.
"""

from __future__ import annotations

import sqlite3

import pytest

from wxverify.api.routes.health import (
    FORECAST_PAIRS_COUNT_SQL,
    FORECAST_SAMPLES_COUNT_SQL,
)
from wxverify.db.migrations import run_migrations
from wxverify.provider_ops import (
    bad_sample_count_sql,
    model_run_count_sql,
    sample_rollup_sql,
)
from wxverify.scoring.winrate import winrate_sql


def _fresh_conn() -> sqlite3.Connection:
    # cached_statements=0: several tests re-run the identical EXPLAIN QUERY
    # PLAN text on the same connection after a DDL change (e.g. DROP INDEX).
    # EXPLAIN QUERY PLAN reports the plan baked into an already-compiled
    # statement without re-verifying the schema, so Python's sqlite3
    # statement cache would otherwise silently serve a stale, pre-DDL plan.
    conn = sqlite3.connect(":memory:", cached_statements=0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _plan(conn: sqlite3.Connection, sql: str, params: tuple[object, ...]) -> list[str]:
    return [
        str(row["detail"])
        for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params)
    ]


def test_sample_rollup_sql_seeks_the_covering_unique_autoindex() -> None:
    conn = _fresh_conn()
    sql = sample_rollup_sql("?")
    plan = _plan(conn, sql, (1, 1))
    assert any(
        "SEARCH forecast_samples USING COVERING INDEX"
        " sqlite_autoindex_forecast_samples_1 (site_id=? AND feed_id=?)" in line
        for line in plan
    )

    # Negative control: widen the WHERE so the planner can no longer bind
    # site_id, proving the positive assertion is not trivially true here.
    degraded = sql.replace("WHERE site_id=?", "WHERE (site_id=? OR 1=1)")
    assert degraded != sql, "WHERE site_id=? not found; negative control is vacuous"
    degraded_plan = _plan(conn, degraded, (1, 1))
    assert not any(
        "SEARCH forecast_samples USING COVERING INDEX"
        " sqlite_autoindex_forecast_samples_1 (site_id=? AND feed_id=?)" in line
        for line in degraded_plan
    )


def test_model_run_count_sql_requires_idx_samples_runs() -> None:
    conn = _fresh_conn()
    sql = model_run_count_sql("?")
    plan = _plan(conn, sql, (1, 1))
    assert any(
        "SEARCH forecast_samples USING COVERING INDEX idx_samples_runs"
        " (site_id=? AND feed_id=?)" in line
        for line in plan
    )

    # On this trivial, ANALYZE-free fixture the planner picks idx_samples_runs
    # even without the INDEXED BY hint, so an EQP-text diff cannot show the
    # hint is load-bearing. Dropping the index it names is a stronger proof:
    # the statement, still carrying INDEXED BY, can no longer be satisfied at
    # all.
    conn.execute("DROP INDEX idx_samples_runs")
    with pytest.raises(sqlite3.OperationalError, match="no such index"):
        conn.execute(sql, (1, 1)).fetchone()


def test_bad_sample_count_sql_requires_idx_samples_invalid() -> None:
    conn = _fresh_conn()
    sql = bad_sample_count_sql("?")
    plan = _plan(conn, sql, (1, 1))
    assert any(
        "SEARCH forecast_samples USING INDEX idx_samples_invalid"
        " (site_id=? AND feed_id=?)" in line
        for line in plan
    )

    conn.execute("DROP INDEX idx_samples_invalid")
    with pytest.raises(sqlite3.OperationalError, match="no such index"):
        conn.execute(sql, (1, 1)).fetchone()


def test_winrate_sql_seeks_idx_pairs_winrate_and_needs_no_extra_sort() -> None:
    conn = _fresh_conn()
    sql = winrate_sql("")
    params = (1, 1, 1, "temperature", 1)
    plan = _plan(conn, sql, params)
    assert any(
        "SEARCH fp USING COVERING INDEX idx_pairs_winrate"
        " (site_id=? AND variable=? AND day_ahead=?)" in line
        for line in plan
    )
    assert not any("LAST 2 TERMS OF ORDER BY" in line for line in plan)

    conn.execute("DROP INDEX idx_pairs_winrate")
    degraded_plan = _plan(conn, sql, params)
    assert not any(
        "SEARCH fp USING COVERING INDEX idx_pairs_winrate" in line
        for line in degraded_plan
    )
    assert any("LAST 2 TERMS OF ORDER BY" in line for line in degraded_plan)


def test_idx_pairs_winrate_column_order_matches_the_query_shape() -> None:
    conn = _fresh_conn()
    columns = [
        (row["name"], row["desc"])
        for row in conn.execute("PRAGMA index_xinfo(idx_pairs_winrate)")
        if row["key"]
    ]
    assert columns == [
        ("site_id", 0),
        ("variable", 0),
        ("day_ahead", 0),
        ("feed_id", 0),
        ("valid_at", 0),
        ("issued_at", 1),
        ("abs_error", 0),
    ]


def test_worker_status_count_statements_stay_index_only_scans() -> None:
    conn = _fresh_conn()
    samples_plan = _plan(conn, FORECAST_SAMPLES_COUNT_SQL, ())
    pairs_plan = _plan(conn, FORECAST_PAIRS_COUNT_SQL, ())

    # Deliberately not pinned to a specific index name: a bare COUNT(*) with
    # no WHERE lets the planner pick whichever index is smallest, and that
    # choice is allowed to drift. What must hold is that it stays an
    # index-only scan, never a full table scan of forecast_samples/forecast_pairs.
    assert any(
        "SCAN forecast_samples USING COVERING INDEX" in line for line in samples_plan
    )
    assert any(
        "SCAN forecast_pairs USING COVERING INDEX" in line for line in pairs_plan
    )
