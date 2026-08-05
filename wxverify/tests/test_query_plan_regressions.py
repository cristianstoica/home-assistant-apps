"""Query-plan regression tests for the provider-health and win-rate hot reads.

Each statement is imported from production rather than retyped, and each
positive assertion is paired with a negative control that empirically
degrades to a worse plan -- proving the assertion is not vacuously true on
this fixture. Index structure is asserted via ``PRAGMA index_xinfo``, never a
hardcoded DDL string, so the test tracks the real index rather than a copy of
it.
"""

from __future__ import annotations

import datetime
import os
import sqlite3

import pytest

from wxverify.api.routes.health import (
    FORECAST_PAIRS_COUNT_SQL,
    FORECAST_SAMPLES_COUNT_SQL,
)
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import ACTIVE_JOB_SQL, LATEST_JOB_SQL
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
        str(row["detail"]) for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params)
    ]


def _seed_meteoblue_package_with_16_members(
    conn: sqlite3.Connection, *, site_id: int, rows_per_feed: int
) -> tuple[int, ...]:
    """Seed the meteoblue package feed plus 16 hand-inserted member feeds
    (17 keys total -- the real production `feed_id IN (...)` list width),
    with rows and a following ANALYZE so the planner has
    real statistics to work from rather than an empty, ANALYZE-free table.
    """
    package_row = conn.execute(
        "SELECT id FROM feeds WHERE source='meteoblue' AND model='multimodel'"
    ).fetchone()
    assert package_row is not None
    feed_ids = [int(package_row["id"])]
    for i in range(16):
        member_id = int(
            conn.execute(
                "INSERT INTO feeds (source, model, fetch_interval_minutes)"
                " VALUES ('meteoblue', ?, 360)",
                (f"member-{i}",),
            ).lastrowid
        )
        feed_ids.append(member_id)

    base = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
    rows = [
        (
            site_id,
            feed_id,
            (base + datetime.timedelta(hours=hour)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        for feed_id in feed_ids
        for hour in range(rows_per_feed)
    ]
    conn.executemany(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id)
        VALUES (?, ?, 'temperature', ?, ?, 24, 10.0, '{}', 'run-1')
        """,
        [(site_id, feed_id, ts, ts) for site_id, feed_id, ts in rows],
    )
    conn.execute("ANALYZE")
    return tuple(feed_ids)


@pytest.mark.skipif(
    os.environ.get("WXV_EQP_SHIPPING") != "1",
    reason="exact-plan pins are a contract with the shipping SQLite build;"
    " enforced by the wxverify-shipping-sqlite-plan CI job",
)
def test_sample_rollup_sql_seeks_the_covering_unique_autoindex() -> None:
    conn = _fresh_conn()
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('QueryPlanRollup', 47, 25, 900, 'UTC')"
        ).lastrowid
    )
    # 17-key meteoblue IN list, not a single-key feed — the real production
    # width. Rows + ANALYZE so the planner sees real cardinality rather than
    # an empty table.
    feed_ids = _seed_meteoblue_package_with_16_members(
        conn, site_id=site_id, rows_per_feed=200
    )
    placeholders = ", ".join("?" for _ in feed_ids)
    params = (site_id, *feed_ids)
    sql = sample_rollup_sql(placeholders)
    plan = _plan(conn, sql, params)
    assert any(
        "SEARCH forecast_samples USING COVERING INDEX"
        " sqlite_autoindex_forecast_samples_1 (site_id=? AND feed_id=?)" in line
        for line in plan
    )

    # Negative control: widen the WHERE so the planner can no longer bind
    # site_id, proving the positive assertion is not trivially true here.
    degraded = sql.replace("WHERE site_id=?", "WHERE (site_id=? OR 1=1)")
    assert degraded != sql, "WHERE site_id=? not found; negative control is vacuous"
    degraded_plan = _plan(conn, degraded, params)
    assert not any(
        "SEARCH forecast_samples USING COVERING INDEX"
        " sqlite_autoindex_forecast_samples_1 (site_id=? AND feed_id=?)" in line
        for line in degraded_plan
    )


def test_model_run_count_sql_requires_idx_samples_runs() -> None:
    conn = _fresh_conn()
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('QueryPlanRuns', 47, 25, 900, 'UTC')"
        ).lastrowid
    )
    feed_ids = _seed_meteoblue_package_with_16_members(
        conn, site_id=site_id, rows_per_feed=200
    )
    placeholders = ", ".join("?" for _ in feed_ids)
    params = (site_id, *feed_ids)
    sql = model_run_count_sql(placeholders)
    plan = _plan(conn, sql, params)
    assert any(
        "SEARCH forecast_samples USING COVERING INDEX idx_samples_runs"
        " (site_id=? AND feed_id=?)" in line
        for line in plan
    )

    # Measured at implementation time, with THIS 17-key/rows/ANALYZE
    # fixture: stripping "INDEXED BY idx_samples_runs" does not change the
    # chosen plan here -- the planner picks idx_samples_runs unprompted at
    # this row count and key width, so an EQP-text diff has zero
    # discriminating power for this statement on any fixture this suite can
    # build (measured the same on the 423k-row production
    # snapshot; reproducing the divergence needs real production
    # statistics this harness cannot synthesize). Recorded honestly rather
    # than implied: dropping the index it names is the strictly stronger
    # proof available -- the statement, still carrying INDEXED BY, can no
    # longer be satisfied at all.
    conn.execute("DROP INDEX idx_samples_runs")
    with pytest.raises(sqlite3.OperationalError, match="no such index"):
        conn.execute(sql, params).fetchone()


def test_bad_sample_count_sql_requires_idx_samples_invalid() -> None:
    conn = _fresh_conn()
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('QueryPlanBad', 47, 25, 900, 'UTC')"
        ).lastrowid
    )
    feed_ids = _seed_meteoblue_package_with_16_members(
        conn, site_id=site_id, rows_per_feed=200
    )
    placeholders = ", ".join("?" for _ in feed_ids)
    params = (site_id, *feed_ids)
    sql = bad_sample_count_sql(placeholders)
    plan = _plan(conn, sql, params)
    assert any(
        "SEARCH forecast_samples USING INDEX idx_samples_invalid"
        " (site_id=? AND feed_id=?)" in line
        for line in plan
    )

    # Same honest note as test_model_run_count_sql_requires_idx_samples_runs
    # above: measured on this 17-key/rows/ANALYZE fixture, stripping
    # "INDEXED BY idx_samples_invalid" does not move the plan away from
    # idx_samples_invalid (it stays free at 0 matching rows regardless of
    # the hint), so this fixture cannot reproduce the wrong-index selection
    # measured in production. DROP INDEX remains the available proof.
    conn.execute("DROP INDEX idx_samples_invalid")
    with pytest.raises(sqlite3.OperationalError, match="no such index"):
        conn.execute(sql, params).fetchone()


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

    conn.execute("DROP INDEX idx_pairs_winrate")
    degraded_plan = _plan(conn, sql, params)
    assert not any(
        "SEARCH fp USING COVERING INDEX idx_pairs_winrate" in line
        for line in degraded_plan
    )

    # idx_pairs_winrate's column order (site_id, variable, day_ahead, feed_id,
    # valid_at, issued_at DESC) matches the window function's PARTITION BY
    # fp.feed_id, fp.valid_at ORDER BY fp.issued_at DESC exactly, so seeking
    # it needs no extra sort step to compute the window. Without it, some
    # sort step is required to make up the missing ordering. The precise EQP
    # wording for that ("...LAST N TERMS OF ORDER BY" vs a plain "...ORDER
    # BY") is a SQLite-build-specific optimizer detail, not a stable
    # contract, but the number of "TEMP B-TREE" sort steps the plan needs is:
    # dropping the index must make the plan need strictly more of them.
    sort_steps = sum("TEMP B-TREE" in line for line in plan)
    degraded_sort_steps = sum("TEMP B-TREE" in line for line in degraded_plan)
    assert degraded_sort_steps > sort_steps, (plan, degraded_plan)


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

    # Negative control: the two statements above are plain, unqualified
    # `SELECT COUNT(*) FROM <table>` text with no table name baked into a
    # pin, so simply DROPping the named indexes on forecast_samples/
    # forecast_pairs would not discriminate -- each table's UNIQUE
    # constraint still leaves a sqlite_autoindex_* behind for the planner to
    # cover with. Prove the assertion actually distinguishes "index-only"
    # from "full scan" by running the identical SQL text against index-free
    # replica tables (same column shape, no UNIQUE constraint, no CREATE
    # INDEX): there, COUNT(*) has nothing to cover with and must fall back
    # to a bare table scan.
    conn.execute(
        "CREATE TABLE forecast_samples_noidx (site_id INTEGER, feed_id INTEGER)"
    )
    conn.execute("CREATE TABLE forecast_pairs_noidx (site_id INTEGER, feed_id INTEGER)")
    degraded_samples_plan = _plan(
        conn,
        FORECAST_SAMPLES_COUNT_SQL.replace(
            "forecast_samples", "forecast_samples_noidx"
        ),
        (),
    )
    degraded_pairs_plan = _plan(
        conn,
        FORECAST_PAIRS_COUNT_SQL.replace("forecast_pairs", "forecast_pairs_noidx"),
        (),
    )
    assert any(
        "SCAN forecast_samples_noidx" in line and "USING COVERING INDEX" not in line
        for line in degraded_samples_plan
    ), degraded_samples_plan
    assert any(
        "SCAN forecast_pairs_noidx" in line and "USING COVERING INDEX" not in line
        for line in degraded_pairs_plan
    ), degraded_pairs_plan


def _seed_jobs_across_two_sites(conn: sqlite3.Connection, *, rows: int) -> int:
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('QueryPlanJobsA', 47, 25, 900, 'UTC')"
        ).lastrowid
    )
    other_site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('QueryPlanJobsB', 48, 26, 900, 'UTC')"
        ).lastrowid
    )
    base = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
    for i in range(rows):
        ts = (base + datetime.timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        conn.execute(
            "INSERT INTO jobs (type, site_id, job_key, payload, status,"
            " created_at, updated_at, next_attempt_at)"
            " VALUES ('fetch_obs', ?, 'obs', '{}', 'failed', ?, ?, NULL)",
            (site_id if i % 2 == 0 else other_site_id, ts, ts),
        )
    conn.execute("ANALYZE")
    return site_id


@pytest.mark.skipif(
    os.environ.get("WXV_EQP_SHIPPING") != "1",
    reason="exact-plan pins are a contract with the shipping SQLite build;"
    " enforced by the wxverify-shipping-sqlite-plan CI job",
)
def test_cooldown_job_lookups_seek_idx_jobs_type_key_site() -> None:
    conn = _fresh_conn()
    site_id = _seed_jobs_across_two_sites(conn, rows=50)
    params = ("fetch_obs", "obs", site_id)

    active_plan = _plan(conn, ACTIVE_JOB_SQL, params)
    assert any(
        "SEARCH jobs USING INDEX idx_jobs_type_key_site"
        " (type=? AND job_key=? AND site_id=?)" in line
        for line in active_plan
    )
    # No separate sort step: id is the index's trailing column and already
    # ascending, so walking it backwards satisfies ORDER BY id DESC LIMIT 1.
    latest_plan = _plan(conn, LATEST_JOB_SQL, params)
    assert any(
        "SEARCH jobs USING INDEX idx_jobs_type_key_site"
        " (type=? AND job_key=? AND site_id=?)" in line
        for line in latest_plan
    )
    assert not any("TEMP B-TREE" in line for line in latest_plan)

    # Negative control: without idx_jobs_type_key_site, the active-probe
    # falls back to a type-only partial-index seek (idx_jobs_active_dedupe
    # can't bind site_id/job_key, per its COALESCE(site_id,-1) expression
    # shape) and the latest-row lookup degrades to a full table scan --
    # exactly the unindexed-scan regression this index exists to prevent.
    conn.execute("DROP INDEX idx_jobs_type_key_site")
    degraded_active_plan = _plan(conn, ACTIVE_JOB_SQL, params)
    assert not any("idx_jobs_type_key_site" in line for line in degraded_active_plan)
    degraded_latest_plan = _plan(conn, LATEST_JOB_SQL, params)
    assert any("SCAN jobs" in line for line in degraded_latest_plan)
