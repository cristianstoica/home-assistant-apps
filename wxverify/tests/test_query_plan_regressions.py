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
    FORECAST_HISTORY_COUNT_SQL,
    FORECAST_SAMPLES_COUNT_SQL,
)
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import (
    _CLAIM_ORDER_BY,  # noqa: PLC2701 -- the constant IS the plan input under test
    _CURRENT_OBS_LANE_CLAUSE,  # noqa: PLC2701
    _MAIN_LANE_CLAUSE,  # noqa: PLC2701
    ACTIVE_JOB_SQL,
    LATEST_JOB_SQL,
)
from wxverify.monitor import FAILED_SCOPES_SQL
from wxverify.provider_ops import (
    bad_sample_count_sql,
    model_run_count_sql,
    recent_model_run_count_sql,
    recent_sample_rollup_sql,
    sample_rollup_sql,
)
from wxverify.scoring.close_scores import (
    _DELETE_FORECAST_HISTORY_SQL,  # noqa: PLC2701 -- the constant IS the plan input under test
)
from wxverify.scoring.history import (
    _CHART_RANGE_SQL,  # noqa: PLC2701
    _WINRATE_RANGE_SQL,  # noqa: PLC2701
)


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
            " timezone) VALUES ('QueryPlanRollup', 40, -105, 900, 'UTC')"
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
    # The statement is deliberately unpinned, and two indexes now cover every
    # column it reads -- the UNIQUE autoindex and idx_samples_recent, whose
    # leading (site_id, feed_id) prefix seeks identically. Either is an
    # equally good plan; what must hold is a covering seek binding both keys.
    covering_seek = (
        "SEARCH forecast_samples USING COVERING INDEX"
        " sqlite_autoindex_forecast_samples_1 (site_id=? AND feed_id=?)",
        "SEARCH forecast_samples USING COVERING INDEX"
        " idx_samples_recent (site_id=? AND feed_id=?)",
    )
    assert any(pin in line for pin in covering_seek for line in plan), plan

    # Negative control: widen the WHERE so the planner can no longer bind
    # site_id, proving the positive assertion is not trivially true here.
    degraded = sql.replace("WHERE site_id=?", "WHERE (site_id=? OR 1=1)")
    assert degraded != sql, "WHERE site_id=? not found; negative control is vacuous"
    degraded_plan = _plan(conn, degraded, params)
    assert not any(pin in line for pin in covering_seek for line in degraded_plan)


def test_model_run_count_sql_requires_idx_samples_runs() -> None:
    conn = _fresh_conn()
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('QueryPlanRuns', 40, -105, 900, 'UTC')"
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
            " timezone) VALUES ('QueryPlanBad', 40, -105, 900, 'UTC')"
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


def test_worker_status_count_statements_stay_index_only_scans() -> None:
    conn = _fresh_conn()
    samples_plan = _plan(conn, FORECAST_SAMPLES_COUNT_SQL, ())
    history_plan = _plan(conn, FORECAST_HISTORY_COUNT_SQL, ())

    # Deliberately not pinned to a specific index name: a bare COUNT(*) with
    # no WHERE lets the planner pick whichever index is smallest, and that
    # choice is allowed to drift. What must hold is that it stays an
    # index-only scan, never a full table scan of forecast_samples.
    assert any(
        "SCAN forecast_samples USING COVERING INDEX" in line for line in samples_plan
    )

    # forecast_history (3.1) is WITHOUT ROWID with no secondary index -- its
    # one btree IS the primary key, so there is no separate covering index
    # for the planner to name and "USING COVERING INDEX" can never appear in
    # its plan. Pin what is actually true and useful instead: the exact-count
    # statement reads forecast_history alone (no other table, no temp
    # b-tree), in a single plan line.
    assert len(history_plan) == 1, history_plan
    assert "forecast_history" in history_plan[0], history_plan
    assert "SCAN" in history_plan[0], history_plan
    assert not any("USE TEMP B-TREE" in line for line in history_plan), history_plan

    # Negative control: the single-line assertion above is not vacuous --
    # joining a second table into the identical COUNT(*) forces a second
    # plan line and a table name the "forecast_history alone" pin above
    # would have caught.
    degraded_plan = _plan(
        conn,
        "SELECT COUNT(*) AS n FROM forecast_history"
        " JOIN sites ON sites.id = forecast_history.site_id",
        (),
    )
    assert len(degraded_plan) > 1, degraded_plan
    assert any("sites" in line for line in degraded_plan), degraded_plan

    # Negative control: the samples assertion above is plain, unqualified
    # `SELECT COUNT(*) FROM <table>` text with no table name baked into a
    # pin, so simply DROPping the named index on forecast_samples would not
    # discriminate -- its UNIQUE constraint still leaves a sqlite_autoindex_*
    # behind for the planner to cover with. Prove the assertion actually
    # distinguishes "index-only" from "full scan" by running the identical
    # SQL text against an index-free replica table (same column shape, no
    # UNIQUE constraint, no CREATE INDEX): there, COUNT(*) has nothing to
    # cover with and must fall back to a bare table scan.
    conn.execute(
        "CREATE TABLE forecast_samples_noidx (site_id INTEGER, feed_id INTEGER)"
    )
    degraded_samples_plan = _plan(
        conn,
        FORECAST_SAMPLES_COUNT_SQL.replace(
            "forecast_samples", "forecast_samples_noidx"
        ),
        (),
    )
    assert any(
        "SCAN forecast_samples_noidx" in line and "USING COVERING INDEX" not in line
        for line in degraded_samples_plan
    ), degraded_samples_plan


def _seed_jobs_across_two_sites(conn: sqlite3.Connection, *, rows: int) -> int:
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('QueryPlanJobsA', 40, -105, 900, 'UTC')"
        ).lastrowid
    )
    other_site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('QueryPlanJobsB', 41, -104, 900, 'UTC')"
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


def _seeded_recent_metrics_fixture() -> tuple[
    sqlite3.Connection, tuple[object, ...], str
]:
    """17-key/rows/ANALYZE fixture plus a mid-range window cutoff -- the
    parameter tuple the windowed statements take: (site_id, feeds..., cutoff).
    """
    conn = _fresh_conn()
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('QueryPlanRecent', 40, -105, 900, 'UTC')"
        ).lastrowid
    )
    feed_ids = _seed_meteoblue_package_with_16_members(
        conn, site_id=site_id, rows_per_feed=200
    )
    placeholders = ", ".join("?" for _ in feed_ids)
    params = (site_id, *feed_ids, "2020-01-05T00:00:00Z")
    return conn, params, placeholders


@pytest.mark.skipif(
    os.environ.get("WXV_EQP_SHIPPING") != "1",
    reason="exact-plan pins are a contract with the shipping SQLite build;"
    " enforced by the wxverify-shipping-sqlite-plan CI job",
)
def test_recent_metric_statements_seek_idx_samples_recent_with_issued_at() -> None:
    conn, params, placeholders = _seeded_recent_metrics_fixture()
    pinned = (
        "SEARCH forecast_samples USING COVERING INDEX idx_samples_recent"
        " (site_id=? AND feed_id=? AND issued_at>?)"
    )
    for sql in (
        recent_sample_rollup_sql(placeholders),
        recent_model_run_count_sql(placeholders),
    ):
        plan = _plan(conn, sql, params)
        assert any(pinned in line for line in plan), plan

        # Negative control: widen the WHERE so site_id can no longer bind,
        # proving the positive pin is not trivially true on this fixture.
        degraded = sql.replace("WHERE site_id=?", "WHERE (site_id=? OR 1=1)")
        assert degraded != sql, "WHERE site_id=? not found; control is vacuous"
        degraded_plan = _plan(conn, degraded, params)
        assert not any(pinned in line for line in degraded_plan), degraded_plan


def _degrades_forecast_samples_access(plan: list[str]) -> bool:
    """True when a plan reads forecast_samples by anything other than a seek
    that binds the index's leading column: a SCAN in any form (including a
    scan *of* the covering index), or a skip-scan, which SQLite reports as a
    SEARCH carrying ANY(<column>) rather than as a SCAN.
    """
    return any(
        "SCAN forecast_samples" in line
        or ("ANY(" in line and "forecast_samples" in line)
        for line in plan
    )


def test_recent_metric_statements_never_bare_scan_forecast_samples() -> None:
    """Build-independent floor under the guarded exact pins above: whatever
    the optimizer's wording, neither windowed statement's plan may contain a
    SCAN of forecast_samples in any form -- both statements carry INDEXED BY
    idx_samples_recent, so an unindexed scan cannot occur at all, and the
    reachable degradations -- a scan *of* that index instead of a seek into
    it, and a skip-scan that reads it without binding site_id -- are
    rejected too. Each windowed statement also needs no more TEMP B-TREE
    steps than its own lifetime counterpart -- the window must not make the
    plan costlier, not merely stay under some fixed count.
    """
    conn, params, placeholders = _seeded_recent_metrics_fixture()
    lifetime_params = params[:-1]
    pairs = (
        (recent_sample_rollup_sql(placeholders), sample_rollup_sql(placeholders)),
        (recent_model_run_count_sql(placeholders), model_run_count_sql(placeholders)),
    )
    for windowed_sql, lifetime_sql in pairs:
        windowed_plan = _plan(conn, windowed_sql, params)
        assert not _degrades_forecast_samples_access(windowed_plan), windowed_plan

        lifetime_plan = _plan(conn, lifetime_sql, lifetime_params)
        windowed_btree_steps = sum("TEMP B-TREE" in line for line in windowed_plan)
        lifetime_btree_steps = sum("TEMP B-TREE" in line for line in lifetime_plan)
        assert windowed_btree_steps <= lifetime_btree_steps, (
            windowed_plan,
            lifetime_plan,
        )


def test_bare_scan_predicate_rejects_a_genuine_scan_of_the_covering_index() -> None:
    """Negative control for the strict "no SCAN forecast_samples in any
    form" predicate above: a statement that reads forecast_samples through
    idx_samples_recent without binding site_id/feed_id genuinely plans as a
    SCAN of that index rather than a SEARCH, proving the strict predicate is
    not vacuously true on this fixture -- it would reject this plan.
    """
    conn, _params, _placeholders = _seeded_recent_metrics_fixture()
    unbound_sql = """
        SELECT COUNT(*) FROM forecast_samples INDEXED BY idx_samples_recent
        WHERE 1 = 1
        """
    plan = _plan(conn, unbound_sql, ())
    assert any(
        "SCAN forecast_samples USING COVERING INDEX idx_samples_recent" in line
        for line in plan
    ), plan


def test_windowed_floor_rejects_a_skip_scan_of_the_covering_index() -> None:
    """Negative control for the skip-scan half of the floor predicate. Dropping
    the site_id binding from either real windowed statement -- the exact edit
    the floor exists to catch -- makes SQLite skip-scan idx_samples_recent
    rather than seek into it, and the floor must reject that plan. Observed on
    CPython 3.13.14 / SQLite 3.53.1 as
    "SEARCH forecast_samples USING COVERING INDEX idx_samples_recent
    (ANY(site_id) AND feed_id=? AND issued_at>?)".
    """
    conn, params, placeholders = _seeded_recent_metrics_fixture()
    for builder in (recent_sample_rollup_sql, recent_model_run_count_sql):
        sql = builder(placeholders)
        degraded = sql.replace("WHERE site_id=? AND ", "WHERE ")
        assert degraded != sql, "WHERE site_id=? AND not found; control is vacuous"
        plan = _plan(conn, degraded, params[1:])
        assert _degrades_forecast_samples_access(plan), plan


def test_recent_metric_statements_require_idx_samples_recent() -> None:
    """Both windowed statements carry INDEXED BY idx_samples_recent, so with
    the index gone they must refuse to prepare at all rather than silently
    re-plan onto a worse access path.
    """
    conn, params, placeholders = _seeded_recent_metrics_fixture()
    conn.execute("DROP INDEX idx_samples_recent")
    for sql in (
        recent_sample_rollup_sql(placeholders),
        recent_model_run_count_sql(placeholders),
    ):
        with pytest.raises(sqlite3.OperationalError, match="no such index"):
            conn.execute(sql, params).fetchone()


def test_idx_samples_recent_column_order_matches_the_query_shape() -> None:
    conn = _fresh_conn()
    columns = [
        (row["name"], row["desc"])
        for row in conn.execute("PRAGMA index_xinfo(idx_samples_recent)")
        if row["key"]
    ]
    assert columns == [
        ("site_id", 0),
        ("feed_id", 0),
        ("issued_at", 0),
        ("valid_at", 0),
        ("variable", 0),
        ("model_run_id", 0),
    ]


# ---------------------------------------------------------------------------
# §14.6 "S" — Seed EQP (relationship, never a fixed-phrasing pin).
# ---------------------------------------------------------------------------


# The recent-samples query plan this section checks is pinned below, by
# ``test_recent_metric_statements_seek_idx_samples_recent_with_issued_at``
# and its neighbours.


# ---------------------------------------------------------------------------
# §3.6 — forecast_history's three readers/writer each seek it by primary
# key, never scan it.
# ---------------------------------------------------------------------------


def _assert_forecast_history_search_binds(
    plan: list[str], columns: tuple[str, ...]
) -> list[str]:
    """Relationship check, never the planner's exact wording (the CLI's
    SQLite and the app's build can word a plan differently): exactly one
    ``forecast_history`` line, a SEARCH (never a SCAN), whose constraint
    names every one of ``columns`` as ``col=?`` -- a bare "SEARCH ...
    USING PRIMARY KEY" prefix check would also accept a seek on ``site_id``
    alone, which is what the negative controls below produce. Returns
    ``plan`` for an index check by the caller.
    """
    rows = [line for line in plan if "forecast_history" in line]
    assert len(rows) == 1, plan
    line = rows[0]
    assert line.startswith("SEARCH forecast_history"), plan
    assert "SCAN forecast_history" not in line, plan
    for column in columns:
        assert f"{column}=?" in line, (line, columns)
    return plan


def test_winrate_range_sql_seeks_forecast_history_by_primary_key() -> None:
    """The win rate's one read (``scoring.history._WINRATE_RANGE_SQL``)
    searches ``forecast_history`` by site_id, feed_id, variable and
    day_ahead (its primary key, less the ``valid_at`` range) -- never a
    scan -- and does so AFTER the active-feeds step, since
    ``_WINRATE_RANGE_SQL``'s ``CROSS JOIN`` is written to keep the feeds
    the outer loop (each feed then seeks the whole key; a plain JOIN lets
    the planner put ``forecast_history`` outside instead, seeking
    ``site_id`` alone).
    """
    conn = _fresh_conn()
    params = (
        1,
        1,
        1,
        "temperature",
        1,
        "2024-01-01",
        "2024-01-03",
        "2024-01-01",
        "2024-01-02",
        "2024-01-01",
    )
    plan = _assert_forecast_history_search_binds(
        _plan(conn, _WINRATE_RANGE_SQL, params),
        ("site_id", "feed_id", "variable", "day_ahead"),
    )
    fh_line = next(line for line in plan if "forecast_history" in line)
    assert plan.index(fh_line) > 0, (
        "M-winrate-order -> at this assertion: correct = forecast_history"
        f" searched after the active-feeds step, mutant (searched first) = {plan}"
    )

    # Negative control: a plain JOIN (instead of CROSS JOIN) lets the
    # planner put forecast_history outside the feeds loop, seeking site_id
    # alone and running first -- proving the checks above are not
    # vacuously true on this fixture.
    degraded = _WINRATE_RANGE_SQL.replace(
        "CROSS JOIN forecast_history", "JOIN forecast_history"
    )
    assert degraded != _WINRATE_RANGE_SQL
    degraded_plan = _plan(conn, degraded, params)
    degraded_rows = [line for line in degraded_plan if "forecast_history" in line]
    assert len(degraded_rows) == 1, degraded_plan
    assert degraded_plan.index(degraded_rows[0]) == 0, degraded_plan
    assert "feed_id=?" not in degraded_rows[0], degraded_plan


def test_chart_range_sql_seeks_forecast_history_by_primary_key() -> None:
    """The history chart's one read (``scoring.history._CHART_RANGE_SQL``)
    searches ``forecast_history`` by site_id, feed_id, variable and
    day_ahead -- never a scan."""
    conn = _fresh_conn()
    _assert_forecast_history_search_binds(
        _plan(
            conn,
            _CHART_RANGE_SQL,
            (1, 1, "temperature", 1, "2024-01-01", "2024-01-03", "2024-01-01"),
        ),
        ("site_id", "feed_id", "variable", "day_ahead"),
    )


def test_stage5_retry_delete_seeks_forecast_history_by_primary_key() -> None:
    """Stage 5's retry delete (``scoring.close_scores.
    _DELETE_FORECAST_HISTORY_SQL``) searches ``forecast_history`` by
    site_id, feed_id, variable and day_ahead -- never a scan, for a
    statement that runs once per feed/variable/day-ahead on every close
    retry.

    This is the one copy of this pin -- CI's shipping-SQLite job
    (``WXV_EQP_SHIPPING=1``) runs only this file and
    ``test_index_predicate_reconciliation.py``, so a duplicate kept
    elsewhere would never run against the shipping build.
    """
    conn = _fresh_conn()
    _assert_forecast_history_search_binds(
        _plan(
            conn,
            _DELETE_FORECAST_HISTORY_SQL,
            (1, 1, "temperature", 1, "2024-01-01", "2024-01-03", "2024-01-01"),
        ),
        ("site_id", "feed_id", "variable", "day_ahead"),
    )


def test_failed_scopes_lookup_is_indexed_on_the_whole_scope() -> None:
    """O11 -- the scope lookup is indexed on the whole scope."""
    conn = _fresh_conn()
    plan = _plan(conn, FAILED_SCOPES_SQL, ("2026-07-07T12:00:00Z",))
    assert any(
        "SEARCH k" in line and "idx_jobs_type_key_site" in line and "site_id=?" in line
        for line in plan
    ), plan

    # Negative control (a): rewriting the job_key comparison so it is no
    # longer a bare column reference ends the usable index prefix at type,
    # so no line can carry the index together with site_id=? any more.
    degraded = FAILED_SCOPES_SQL.replace(
        "k.job_key IS j.job_key", "COALESCE(k.job_key, '') = COALESCE(j.job_key, '')"
    )
    assert degraded != FAILED_SCOPES_SQL
    degraded_plan = _plan(conn, degraded, ("2026-07-07T12:00:00Z",))
    assert not any(
        "SEARCH k" in line and "idx_jobs_type_key_site" in line and "site_id=?" in line
        for line in degraded_plan
    ), degraded_plan

    # Negative control (b): with the index gone, no line can name it at all.
    conn.execute("DROP INDEX idx_jobs_type_key_site")
    no_index_plan = _plan(conn, FAILED_SCOPES_SQL, ("2026-07-07T12:00:00Z",))
    assert not any("idx_jobs_type_key_site" in line for line in no_index_plan), (
        no_index_plan
    )


def _claim_subselect_plan(conn: sqlite3.Connection, type_clause: str) -> list[str]:
    """The claim's inner pending-row subselect, isolated from the outer
    UPDATE ... RETURNING (§6.3.4, ``db/queue.py``'s ``_claim``). The outer
    UPDATE always resolves by rowid regardless of ``type_clause``, so only
    this subselect's plan can move -- imports ``_CLAIM_ORDER_BY`` and uses
    the same ``type_clause`` splice point ``_claim`` uses, so this test
    drives the SAME clause CONSTANTS production claims with, not a
    hand-copied literal.
    """
    return _plan(
        conn,
        f"""
        SELECT id FROM jobs
        WHERE status = 'pending'
          AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
          {type_clause}
        {_CLAIM_ORDER_BY}
        LIMIT 1
        """,
        ("2026-07-07T12:00:00Z", "2026-07-07T11:55:00Z"),
    )


def test_eqp_claim_partition_clauses_keep_todays_claim_plan() -> None:
    """EQP -- for both lane clauses, the EQP rows equal today's (pre-split,
    no type clause at all) claim's EQP rows: a bare ``SCAN jobs`` plus a
    ``USE TEMP B-TREE FOR ORDER BY``, never a seek through
    ``idx_jobs_type_key_site``. This is a relationship assertion (plan
    equality), not a pin on the exact plan text.

    Mutant: dropping the unary ``+`` from ``_CURRENT_OBS_LANE_CLAUSE`` lets
    the planner choose ``idx_jobs_type_key_site`` instead (a ``SEARCH``
    line), which this test's baseline-equality assertion catches.
    """
    conn = _fresh_conn()
    baseline_plan = _claim_subselect_plan(conn, "")
    main_lane_plan = _claim_subselect_plan(conn, _MAIN_LANE_CLAUSE)
    current_obs_plan = _claim_subselect_plan(conn, _CURRENT_OBS_LANE_CLAUSE)

    assert main_lane_plan == baseline_plan
    assert current_obs_plan == baseline_plan
    assert any("SCAN jobs" in line for line in baseline_plan)
    assert not any("idx_jobs_type_key_site" in line for line in baseline_plan)

    # Negative control: dropping the unary `+` really does move the planner
    # onto idx_jobs_type_key_site, proving this fixture can distinguish the
    # two plans instead of the assertion being vacuously true here.
    degraded_clause = _CURRENT_OBS_LANE_CLAUSE.replace("+type", "type")
    assert degraded_clause != _CURRENT_OBS_LANE_CLAUSE
    degraded_plan = _claim_subselect_plan(conn, degraded_clause)
    assert degraded_plan != baseline_plan
    assert any("idx_jobs_type_key_site" in line for line in degraded_plan)
