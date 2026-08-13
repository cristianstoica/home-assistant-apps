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
import inspect
import os
import sqlite3

import pytest

import wxverify.api.routes.verification
from wxverify.api.routes.health import (
    FORECAST_PAIRS_COUNT_SQL,
    FORECAST_SAMPLES_COUNT_SQL,
)
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import ACTIVE_JOB_SQL, LATEST_JOB_SQL
from wxverify.provider_ops import (
    bad_sample_count_sql,
    model_run_count_sql,
    recent_model_run_count_sql,
    recent_sample_rollup_sql,
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
        ("tz_generation_id", 0),
    ]


def test_winrate_published_pointer_resolves_as_a_runtime_state_key_probe() -> None:
    # The published-generation binding embeds a correlated scalar subquery on
    # runtime_state ('tz_generation_published:' || fp.site_id). It runs once
    # per candidate pairs row, so it must stay a key probe of runtime_state's
    # primary key -- a per-row SCAN of runtime_state would make every
    # pairs-reading statement O(pairs x runtime_state). Asserted as a
    # structural relationship (probe present, scan absent), not exact EQP
    # phrasing, which is build-specific.
    conn = _fresh_conn()
    sql = winrate_sql("")
    params = (1, 1, 1, "temperature", 1)
    plan = _plan(conn, sql, params)
    assert any("SEARCH rs" in line and "key=?" in line for line in plan), plan
    assert not any("SCAN rs" in line for line in plan), plan

    # Negative control: defeat the key probe (CAST strips the equality from
    # the primary key) and the very same statement degrades to SCAN rs --
    # proving the healthy assertion above is discriminating, not vacuous.
    degraded_sql = sql.replace(
        "rs.key = 'tz_generation_published:' || fp.site_id",
        "CAST(rs.key AS TEXT) = 'tz_generation_published:' || fp.site_id",
    )
    assert degraded_sql != sql, "clause text drifted; update this control"
    degraded_plan = _plan(conn, degraded_sql, params)
    assert any("SCAN rs" in line for line in degraded_plan), degraded_plan


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
            " timezone) VALUES ('QueryPlanRecent', 47, 25, 900, 'UTC')"
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
# §18.12 — query-plan expectations for the 0.11.0 verification indexes.
#
# Every /api/verification/* collection read must SEEK into an index on its
# scoping key (run_id or site_id); a plan that SCANs one of these tables is
# unbounded across runs/sites and is exactly the regression this family
# exists to fail on. Each positive is paired with an empirically degrading
# negative control: dropping the index, or dropping the scoping conjunct.
#
# The statements are retyped here because the route builds them as f-strings
# around an optional filter clause rather than exporting a constant; a
# source-text tripwire below fails if the production text drifts away from
# what is pinned.
# ---------------------------------------------------------------------------

_EVIDENCE_PAGE_SQL = """
            SELECT * FROM verification_evidence
            WHERE run_id = ?
            ORDER BY id LIMIT ? OFFSET ?
            """

_RUNS_PAGE_SQL = """
            SELECT * FROM verification_runs
            ORDER BY id DESC LIMIT ? OFFSET ?
            """

_FAILED_NEWER_SQL = """
        SELECT 1 FROM verification_runs
        WHERE site_id = ? AND state = 'failed' AND id > ? LIMIT 1
        """


def test_verification_route_sql_still_matches_the_pinned_text() -> None:
    """Drift tripwire: the route composes its statements inline, so the
    pinned copies above are only trustworthy while the production text
    still contains them."""
    source = inspect.getsource(wxverify.api.routes.verification)
    assert "SELECT * FROM verification_evidence\n            WHERE run_id = ?" in source
    assert "ORDER BY id LIMIT ? OFFSET ?" in source
    assert "SELECT * FROM verification_runs {where}\n            ORDER BY id DESC" in (
        source
    )
    assert (
        "SELECT 1 FROM verification_runs\n"
        "        WHERE site_id = ? AND state = 'failed' AND id > ? LIMIT 1" in source
    )


def test_evidence_page_seeks_by_run_id_never_scans() -> None:
    conn = _fresh_conn()
    plan = _plan(conn, _EVIDENCE_PAGE_SQL, (1, 10, 0))
    assert any("SEARCH verification_evidence" in line for line in plan), plan
    assert any("(run_id=?)" in line for line in plan), plan
    assert not any(line.startswith("SCAN verification_evidence") for line in plan), plan
    # The run-scoping survives even without the secondary cell index: the
    # UNIQUE(run_id, ...) autoindex leads on run_id too. Pinning both
    # configurations records WHY the page can never scan all runs.
    conn.execute("DROP INDEX idx_verification_evidence_cell")
    fallback = _plan(conn, _EVIDENCE_PAGE_SQL, (1, 10, 0))
    assert any(
        "SEARCH verification_evidence USING INDEX"
        " sqlite_autoindex_verification_evidence_1 (run_id=?)" in line
        for line in fallback
    ), fallback


def test_evidence_page_without_run_scoping_degrades_to_a_scan() -> None:
    """Negative control: the exact regression §18.12 names — a detailed
    evidence read that loses its run_id binding — plans as a full scan, so
    the positive above is not vacuously true on this schema."""
    conn = _fresh_conn()
    degraded = _EVIDENCE_PAGE_SQL.replace("WHERE run_id = ?", "WHERE variable = ?")
    assert degraded != _EVIDENCE_PAGE_SQL
    plan = _plan(conn, degraded, ("wind", 10, 0))
    assert plan == ["SCAN verification_evidence"], plan


def test_runs_page_and_failed_newer_probe_use_idx_verification_runs_site() -> None:
    conn = _fresh_conn()
    site_sql = _RUNS_PAGE_SQL.replace(
        "FROM verification_runs", "FROM verification_runs WHERE site_id = ?"
    )
    plan = _plan(conn, site_sql, (1, 10, 0))
    assert any(
        "SEARCH verification_runs USING INDEX idx_verification_runs_site (site_id=?)"
        in line
        for line in plan
    ), plan
    # The status banner's "a newer attempt failed" probe binds all three
    # index columns and never touches the table.
    probe = _plan(conn, _FAILED_NEWER_SQL, (1, 0))
    assert probe == [
        "SEARCH verification_runs USING COVERING INDEX"
        " idx_verification_runs_site (site_id=? AND state=? AND id>?)"
    ], probe

    # Negative control: without the index the site page scans every run and
    # the status probe falls back to a rowid range over all sites.
    conn.execute("DROP INDEX idx_verification_runs_site")
    assert _plan(conn, site_sql, (1, 10, 0)) == ["SCAN verification_runs"]
    assert _plan(conn, _FAILED_NEWER_SQL, (1, 0)) == [
        "SEARCH verification_runs USING INTEGER PRIMARY KEY (rowid>?)"
    ]


def test_trigger_decision_lookup_uses_its_site_date_index() -> None:
    conn = _fresh_conn()
    sql = """
        SELECT * FROM verification_trigger_decisions
        WHERE site_id = ? AND trigger_date = ? ORDER BY id DESC LIMIT 1
        """
    plan = _plan(conn, sql, (1, "2026-05-01"))
    assert plan == [
        "SEARCH verification_trigger_decisions USING INDEX"
        " idx_verification_trigger_site_date (site_id=? AND trigger_date=?)"
    ], plan
    conn.execute("DROP INDEX idx_verification_trigger_site_date")
    assert _plan(conn, sql, (1, "2026-05-01")) == [
        "SCAN verification_trigger_decisions"
    ]


def test_new_verification_index_column_order_matches_the_query_shapes() -> None:
    """Structure asserted via PRAGMA index_xinfo, never a DDL string copy."""
    conn = _fresh_conn()

    def keyed(index: str) -> list[tuple[str, int]]:
        return [
            (str(row["name"]), int(row["desc"]))
            for row in conn.execute(f"PRAGMA index_xinfo({index})")  # noqa: S608
            if row["key"]
        ]

    assert keyed("idx_verification_runs_site") == [
        ("site_id", 0),
        ("state", 0),
        ("id", 0),
    ]
    assert keyed("idx_verification_evidence_cell") == [
        ("run_id", 0),
        ("entity_type", 0),
        ("quantity", 0),
        ("lead", 0),
        ("target_local_date", 0),
    ]
    assert keyed("idx_verification_trigger_site_date") == [
        ("site_id", 0),
        ("trigger_date", 0),
        ("id", 0),
    ]
