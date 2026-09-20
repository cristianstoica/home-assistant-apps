"""Failed-arm supersession oracles for problem_jobs (plan §6.3, §9).

A later genuine success of a scope -- a completed row carrying the
success marker ``result='ok'`` -- resolves an aged terminal failure of
the same ``(type, site_id, job_key)`` scope, for the single-row job
types only. Every fixture here is synthetic: one site named ``'S'`` at
the test suite's placeholder coordinates, job keys such as ``fetch:1``,
``obs`` and ``catchup``, and a fixed clock of 2026-07-09T12:00:00Z.

This module carries the §6.3 harness and O1-O10, O12, O13 and O17.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import asof_conn, asof_make_site
from wxverify.monitor import build_verdict

_NOW = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)
# failed_cutoff under _NOW is 2026-07-07T12:00:00Z (FAILED_JOB_AGE_HOURS=48).
_AGED = "2026-07-06T00:00:00Z"  # <= cutoff: past the floor
_RECENT = "2026-07-09T06:00:00Z"  # > cutoff: inside the floor
_LATER = "2026-07-08T00:00:00Z"  # stamp for rows inserted after a failure


def _insert_job(
    conn: sqlite3.Connection,
    *,
    job_type: str,
    site_id: int | None,
    job_key: str | None,
    status: str,
    updated_at: str,
    next_attempt_at: str | None = None,
    result: str | None = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, payload, status,
                          next_attempt_at, updated_at, result)
        VALUES (?, ?, ?, '{}', ?, ?, ?, ?)
        """,
        (job_type, site_id, job_key, status, next_attempt_at, updated_at, result),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _problem_jobs(
    conn: sqlite3.Connection, *, now: datetime = _NOW
) -> dict[str, object]:
    verdict = build_verdict(
        conn,
        pipeline_enabled=True,
        budget_enabled=False,
        db_enabled=False,
        now=now,
        export_sweeper_dead=None,
    )
    conditions = verdict["conditions"]
    assert isinstance(conditions, list)
    matches = [
        c for c in conditions if isinstance(c, dict) and c.get("id") == "problem_jobs"
    ]
    assert len(matches) == 1
    return matches[0]


def test_later_marked_success_clears_an_aged_failure() -> None:
    """O1 -- regression: a later same-scope success clears an aged failure."""
    conn = asof_conn()
    site = asof_make_site(conn, "S")
    j = _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    k = _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="completed",
        updated_at=_LATER,
        result="ok",
    )
    assert k > j

    cond = _problem_jobs(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0
    assert "detail" not in cond


def test_aged_failure_with_no_later_success_trips() -> None:
    """O2 -- negative: an aged failure with no later success trips."""
    conn = asof_conn()
    site = asof_make_site(conn, "S")
    _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )

    cond = _problem_jobs(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1
    assert cond["detail"] == "1 stuck/failed/overdue jobs"


@pytest.mark.parametrize(
    ("job_type", "use_site2", "job_key", "expected_count"),
    [
        pytest.param("fetch_feed", False, "fetch:2", 1, id="mismatched_job_key"),
        pytest.param("fetch_feed", True, "fetch:1", 1, id="mismatched_site_id"),
        pytest.param("fetch_obs", False, "fetch:1", 1, id="mismatched_type"),
        pytest.param("fetch_feed", False, "fetch:1", 0, id="exact_match_control"),
    ],
)
def test_scope_is_exact_on_all_three_columns(
    job_type: str, use_site2: bool, job_key: str, expected_count: int
) -> None:
    """O3 -- scope is exact on all three columns; the fourth case is the
    exact-match control."""
    conn = asof_conn()
    site = asof_make_site(conn, "S")
    site2 = asof_make_site(conn, "S2")
    j = _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    k = _insert_job(
        conn,
        job_type=job_type,
        site_id=site2 if use_site2 else site,
        job_key=job_key,
        status="completed",
        updated_at=_LATER,
        result="ok",
    )
    assert k > j

    cond = _problem_jobs(conn)
    assert cond["count"] == expected_count


def test_pending_and_running_rows_do_not_supersede() -> None:
    """O4 -- pending and running rows do not supersede."""
    conn_a = asof_conn()
    site_a = asof_make_site(conn_a, "S")
    _insert_job(
        conn_a,
        job_type="fetch_feed",
        site_id=site_a,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    _insert_job(
        conn_a,
        job_type="fetch_feed",
        site_id=site_a,
        job_key="fetch:1",
        status="pending",
        updated_at=_LATER,
        next_attempt_at="2026-07-10T00:00:00Z",
    )
    cond_a = _problem_jobs(conn_a)
    assert cond_a["count"] == 1

    conn_b = asof_conn()
    site_b = asof_make_site(conn_b, "S")
    _insert_job(
        conn_b,
        job_type="fetch_feed",
        site_id=site_b,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    _insert_job(
        conn_b,
        job_type="fetch_feed",
        site_id=site_b,
        job_key="fetch:1",
        status="running",
        updated_at=_RECENT,
    )
    cond_b = _problem_jobs(conn_b)
    assert cond_b["count"] == 2


def test_a_scope_counts_once() -> None:
    """O5 -- a scope counts once."""
    conn_a = asof_conn()
    site_a = asof_make_site(conn_a, "S")
    _insert_job(
        conn_a,
        job_type="fetch_feed",
        site_id=site_a,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    _insert_job(
        conn_a,
        job_type="fetch_feed",
        site_id=site_a,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    cond_a = _problem_jobs(conn_a)
    assert cond_a["count"] == 1

    conn_b = asof_conn()
    site_b = asof_make_site(conn_b, "S")
    _insert_job(
        conn_b,
        job_type="fetch_feed",
        site_id=site_b,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    _insert_job(
        conn_b,
        job_type="fetch_feed",
        site_id=site_b,
        job_key="fetch:1",
        status="failed",
        updated_at=_RECENT,
    )
    cond_b = _problem_jobs(conn_b)
    assert cond_b["count"] == 1


def test_recovered_scope_that_fails_again_counts_only_once_aged() -> None:
    """O6 -- a recovered scope that fails again counts only once the new
    failure is itself 48h old."""
    conn = asof_conn()
    site = asof_make_site(conn, "S")
    j1 = _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    k = _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="completed",
        updated_at=_LATER,
        result="ok",
    )
    assert k > j1
    j2 = _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="failed",
        updated_at=_RECENT,
    )
    assert j2 > k

    cond = _problem_jobs(conn)
    assert cond["count"] == 0
    assert cond["ok"] is True

    cond_later = _problem_jobs(conn, now=_NOW + timedelta(hours=48))
    assert cond_later["count"] == 1


def test_inside_the_floor_no_later_row_not_counted() -> None:
    """O7 -- inside the floor, no later row: not counted."""
    conn = asof_conn()
    site = asof_make_site(conn, "S")
    _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="failed",
        updated_at=_RECENT,
    )

    cond = _problem_jobs(conn)
    assert cond["count"] == 0
    assert cond["ok"] is True


def test_order_is_by_id_not_by_updated_at() -> None:
    """O8 -- order is by id, not by updated_at."""
    conn_a = asof_conn()
    site_a = asof_make_site(conn_a, "S")
    k_a = _insert_job(
        conn_a,
        job_type="fetch_feed",
        site_id=site_a,
        job_key="fetch:1",
        status="completed",
        updated_at=_LATER,
        result="ok",
    )
    j_a = _insert_job(
        conn_a,
        job_type="fetch_feed",
        site_id=site_a,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    assert j_a > k_a
    cond_a = _problem_jobs(conn_a)
    assert cond_a["count"] == 1

    conn_b = asof_conn()
    site_b = asof_make_site(conn_b, "S")
    j_b = _insert_job(
        conn_b,
        job_type="fetch_feed",
        site_id=site_b,
        job_key="fetch:1",
        status="failed",
        updated_at=_LATER,
    )
    k_b = _insert_job(
        conn_b,
        job_type="fetch_feed",
        site_id=site_b,
        job_key="fetch:1",
        status="completed",
        updated_at=_AGED,
        result="ok",
    )
    assert k_b > j_b
    cond_b = _problem_jobs(conn_b, now=_NOW + timedelta(days=2))
    assert cond_b["count"] == 0


def test_null_scope_columns_are_scopes() -> None:
    """O9 -- NULL scope columns are scopes."""
    conn_a1 = asof_conn()
    site_a1 = asof_make_site(conn_a1, "S")
    _insert_job(
        conn_a1,
        job_type="fetch_obs",
        site_id=site_a1,
        job_key=None,
        status="failed",
        updated_at=_AGED,
    )
    _insert_job(
        conn_a1,
        job_type="fetch_obs",
        site_id=site_a1,
        job_key=None,
        status="completed",
        updated_at=_LATER,
        result="ok",
    )
    cond_a1 = _problem_jobs(conn_a1)
    assert cond_a1["count"] == 0

    conn_a2 = asof_conn()
    site_a2 = asof_make_site(conn_a2, "S")
    _insert_job(
        conn_a2,
        job_type="fetch_obs",
        site_id=site_a2,
        job_key=None,
        status="failed",
        updated_at=_AGED,
    )
    _insert_job(
        conn_a2,
        job_type="fetch_obs",
        site_id=site_a2,
        job_key="obs",
        status="completed",
        updated_at=_LATER,
        result="ok",
    )
    cond_a2 = _problem_jobs(conn_a2)
    assert cond_a2["count"] == 1

    conn_b = asof_conn()
    _insert_job(
        conn_b,
        job_type="catchup",
        site_id=None,
        job_key="catchup",
        status="failed",
        updated_at=_AGED,
    )
    _insert_job(
        conn_b,
        job_type="catchup",
        site_id=None,
        job_key="catchup",
        status="completed",
        updated_at=_LATER,
        result="ok",
    )
    cond_b = _problem_jobs(conn_b)
    assert cond_b["count"] == 1


def test_three_arms_still_sum() -> None:
    """O10 -- three arms still sum."""
    conn = asof_conn()
    site = asof_make_site(conn, "S")
    _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:2",
        status="running",
        updated_at=_RECENT,
    )
    _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:3",
        status="pending",
        updated_at="2020-01-01T00:00:00Z",
        next_attempt_at="2020-01-01T00:00:00Z",
    )
    cond = _problem_jobs(conn)
    assert cond["count"] == 3
    assert cond["ok"] is False

    _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="completed",
        updated_at=_LATER,
        result="ok",
    )
    cond_after = _problem_jobs(conn)
    assert cond_after["count"] == 2


def test_alternating_failure_and_success_never_leaves_unresolved_row() -> None:
    """O12 -- alternating failure and success never leaves an unresolved
    row."""
    conn = asof_conn()
    site = asof_make_site(conn, "S")
    j1 = _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    k1 = _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="completed",
        updated_at=_LATER,
        result="ok",
    )
    j2 = _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    k2 = _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="completed",
        updated_at=_LATER,
        result="ok",
    )
    assert j1 < k1 < j2 < k2

    cond = _problem_jobs(conn)
    assert cond["count"] == 0
    assert cond["ok"] is True

    j3 = _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    assert j3 > k2

    cond_after = _problem_jobs(conn)
    assert cond_after["count"] == 1


@pytest.mark.parametrize(
    ("job_type", "site_scoped", "job_key", "expected_count", "expected_ok"),
    [
        pytest.param(
            "verification_run", True, "verify:1", 1, False, id="verification_run"
        ),
        pytest.param("backfill_site", True, "backfill:1", 1, False, id="backfill_site"),
        pytest.param(
            "timezone_correction",
            True,
            "tzcorr:1",
            1,
            False,
            id="tzcorr-membership-pin",
        ),
        pytest.param(
            "record_gap_scan", True, "gapscan:cont:1", 1, False, id="record_gap_scan"
        ),
        pytest.param("catchup", False, "catchup", 1, False, id="catchup"),
        pytest.param("fetch_feed", True, "fetch:1", 0, True, id="fetch_feed"),
        pytest.param("fetch_obs", True, "obs", 0, True, id="fetch_obs"),
        pytest.param(
            "fetch_current_obs", True, "curobs:1", 0, True, id="fetch_current_obs"
        ),
        pytest.param("pair_and_score", True, "score", 0, True, id="pair_and_score"),
        pytest.param(
            "forecast_record",
            True,
            "record:2026-07-06",
            0,
            True,
            id="forecast_record",
        ),
    ],
)
def test_chain_types_do_not_supersede_single_row_types_do(
    job_type: str,
    site_scoped: bool,
    job_key: str,
    expected_count: int,
    expected_ok: bool,
) -> None:
    """O13 -- a chain type's later completed chunks do not supersede; a
    single-row type's later completion does."""
    conn = asof_conn()
    site = asof_make_site(conn, "S") if site_scoped else None
    a = _insert_job(
        conn,
        job_type=job_type,
        site_id=site,
        job_key=job_key,
        status="completed",
        updated_at="2026-07-06T11:00:00Z",
        result="ok",
    )
    a1 = _insert_job(
        conn,
        job_type=job_type,
        site_id=site,
        job_key=job_key,
        status="failed",
        updated_at="2026-07-06T12:00:00Z",
    )
    b = _insert_job(
        conn,
        job_type=job_type,
        site_id=site,
        job_key=job_key,
        status="completed",
        updated_at="2026-07-07T12:00:00Z",
        result="ok",
    )
    b1 = _insert_job(
        conn,
        job_type=job_type,
        site_id=site,
        job_key=job_key,
        status="failed",
        updated_at="2026-07-08T12:00:00Z",
    )
    assert a < a1 < b < b1

    cond = _problem_jobs(conn)
    assert cond["count"] == expected_count
    assert cond["ok"] is expected_ok


def test_only_a_marked_completion_supersedes() -> None:
    """O17 -- only a marked completion supersedes."""
    conn_a = asof_conn()
    site_a = asof_make_site(conn_a, "S")
    j_a = _insert_job(
        conn_a,
        job_type="fetch_feed",
        site_id=site_a,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    k_a = _insert_job(
        conn_a,
        job_type="fetch_feed",
        site_id=site_a,
        job_key="fetch:1",
        status="completed",
        updated_at=_LATER,
        result=None,
    )
    assert k_a > j_a
    cond_a = _problem_jobs(conn_a)
    assert cond_a["count"] == 1
    assert cond_a["ok"] is False

    conn_b = asof_conn()
    site_b = asof_make_site(conn_b, "S")
    j_b = _insert_job(
        conn_b,
        job_type="fetch_feed",
        site_id=site_b,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    k1_b = _insert_job(
        conn_b,
        job_type="fetch_feed",
        site_id=site_b,
        job_key="fetch:1",
        status="completed",
        updated_at=_LATER,
        result=None,
    )
    cond_b_mid = _problem_jobs(conn_b)
    assert cond_b_mid["count"] == 1
    k2_b = _insert_job(
        conn_b,
        job_type="fetch_feed",
        site_id=site_b,
        job_key="fetch:1",
        status="completed",
        updated_at=_LATER,
        result="ok",
    )
    assert j_b < k1_b < k2_b
    cond_b_after = _problem_jobs(conn_b)
    assert cond_b_after["count"] == 0

    conn_c = asof_conn()
    site_c = asof_make_site(conn_c, "S")
    _insert_job(
        conn_c,
        job_type="fetch_feed",
        site_id=site_c,
        job_key="fetch:1",
        status="failed",
        updated_at=_AGED,
    )
    _insert_job(
        conn_c,
        job_type="fetch_feed",
        site_id=site_c,
        job_key="fetch:1",
        status="completed",
        updated_at=_LATER,
        result="cancelled",
    )
    cond_c = _problem_jobs(conn_c)
    assert cond_c["count"] == 1
