"""problem_jobs progress evidence for running verification_run chunks
(plan §10: P1-P13).

A running verification_run row counts toward problem_jobs only when both its
claim and its site's heartbeat are stale (plan §5.1). Every fixture here is
synthetic: sites named 'A' and 'B' at tests.helpers.asof_make_site's
placeholder coordinates, and a fixed clock of 2026-07-09T12:00:00Z shared
with tests.test_problem_jobs_supersession, whose cutoffs this module reuses:
claim cutoff and progress cutoff both 11:40:00, pending cutoff 11:45:00,
future limit 12:05:00.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import asof_conn, asof_make_site
from tests.test_problem_jobs_supersession import _NOW, _insert_job, _problem_jobs
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.monitor import build_verdict
from wxverify.worker import verification_run
from wxverify.worker.verification_run import (
    verification_heartbeat_key,
    verification_job_key,
)


def _running_chunk(conn: sqlite3.Connection, site_id: int, claimed_at: str) -> int:
    return _insert_job(
        conn,
        job_type="verification_run",
        site_id=site_id,
        job_key=verification_job_key(site_id),
        status="running",
        updated_at=claimed_at,
    )


def _beat(conn: sqlite3.Connection, site_id: int, value: str) -> None:
    set_runtime_state(conn, verification_heartbeat_key(site_id), value)


def test_p1_progressing() -> None:
    """A chunk with a stale claim but a fresh heartbeat is not counted."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    _running_chunk(conn, site_id, "2026-07-09T11:35:00Z")
    _beat(conn, site_id, "2026-07-09T11:58:00.000Z")
    cond = _problem_jobs(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0


def test_p2_stalled() -> None:
    """A chunk with a stale claim and a stale heartbeat is counted, and its
    detail folds into the same string as the pre-existing arm."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    _running_chunk(conn, site_id, "2026-07-09T11:35:00Z")
    _beat(conn, site_id, "2026-07-09T11:38:00.000Z")
    cond = _problem_jobs(conn)
    assert cond["ok"] is False
    assert cond["count"] == 1
    assert cond["detail"] == "0 unresolved failed scopes, 1 stuck/overdue jobs"


@pytest.mark.parametrize(
    ("heartbeat", "expected_count"),
    [
        ("2026-07-09T11:40:00.000Z", 1),
        ("2026-07-09T11:40:01.000Z", 0),
        ("2026-07-09T11:40:00.500Z", 0),
    ],
    ids=["a-at-cutoff", "b-one-second-after", "c-half-second-after"],
)
def test_p3_heartbeat_boundary(heartbeat: str, expected_count: int) -> None:
    """The progress cutoff (now - 20m = 11:40:00) is parsed, not compared as
    text: a) exactly at the cutoff still counts as stale; b) one second past
    it is fresh; c) a fractional-second stamp that would sort earlier than
    a whole-second stamp under text comparison is still correctly read as
    later once parsed."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    _running_chunk(conn, site_id, "2026-07-09T11:35:00Z")
    _beat(conn, site_id, heartbeat)
    cond = _problem_jobs(conn)
    assert cond["count"] == expected_count


@pytest.mark.parametrize(
    ("claimed_at", "expected_count"),
    [
        ("2026-07-09T11:35:00Z", 1),
        ("2026-07-09T11:40:00Z", 1),
        ("2026-07-09T11:40:01Z", 0),
    ],
    ids=["a-old-claim", "b-at-claim-cutoff", "c-young-claim"],
)
def test_p4_no_heartbeat(claimed_at: str, expected_count: int) -> None:
    """No heartbeat row at all (a fresh site that has never committed a day)
    reads as no progress once the claim itself is stale; a claim younger
    than the claim cutoff is not counted regardless."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    _running_chunk(conn, site_id, claimed_at)
    cond = _problem_jobs(conn)
    assert cond["count"] == expected_count


@pytest.mark.parametrize(
    "heartbeat",
    ["zzzz", "9999-12-31T23:59:59-05:00"],
    ids=["a-not-a-timestamp", "b-out-of-range-date"],
)
def test_p5_unreadable_heartbeat(heartbeat: str) -> None:
    """A heartbeat value that parse_utc rejects (garbage text, or a
    well-formed but out-of-range date raising ValueError) is no evidence of
    progress and reads exactly like a missing one."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    _running_chunk(conn, site_id, "2026-07-09T11:35:00Z")
    _beat(conn, site_id, heartbeat)
    cond = _problem_jobs(conn)
    assert cond["count"] == 1


def test_p6_keyed_by_site() -> None:
    """The heartbeat join is scoped to the row's own site: a fresh heartbeat
    under a different site's id (a decoy for site A, which has no running
    chunk at all) must not leak into site B's stale, heartbeat-less chunk."""
    conn = asof_conn()
    a = asof_make_site(conn, "A")
    b = asof_make_site(conn, "B")
    assert (a, b) == (1, 2)
    # B's chunk is inserted first; its job id lands on 1, which coincides
    # with site A's id -- confirming the heartbeat is matched by site_id,
    # not by job id or insertion order.
    job_id = _running_chunk(conn, b, "2026-07-09T11:35:00Z")
    assert job_id == a
    _beat(conn, a, "2026-07-09T11:58:00.000Z")
    cond = _problem_jobs(conn)
    assert cond["count"] == 1


@pytest.mark.parametrize(
    "heartbeat",
    ["2026-07-09T11:58:00.000Z", None],
    ids=["a-fresh-heartbeat-present", "b-no-heartbeat"],
)
def test_p7_other_types_unchanged(heartbeat: str | None) -> None:
    """A running row of a different job type is unaffected by Edit A's
    exclusion and by the heartbeat, in either direction."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    _insert_job(
        conn,
        job_type="fetch_feed",
        site_id=site_id,
        job_key=f"fetch:{site_id}",
        status="running",
        updated_at="2026-07-09T11:35:00Z",
    )
    if heartbeat is not None:
        _beat(conn, site_id, heartbeat)
    cond = _problem_jobs(conn)
    assert cond["count"] == 1


def test_p8_real_writer() -> None:
    """The reader agrees with the writer's actual key and stamp format:
    verification_run._heartbeat, read back through get_runtime_state and
    parse_utc, drives the same progress rule as the seeded fixtures above."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    verification_run._heartbeat(conn, site_id)  # noqa: SLF001
    raw = get_runtime_state(conn, verification_heartbeat_key(site_id))
    assert raw is not None
    h = parse_utc(raw)
    # Without this, a writer that stores a fixed old stamp would pass, because
    # every later step here is anchored on h rather than on the wall clock.
    assert abs(h - datetime.now(UTC)) < timedelta(seconds=60)
    _running_chunk(conn, site_id, isoformat_utc(h - timedelta(minutes=25)))
    cond_fresh = _problem_jobs(conn, now=h + timedelta(minutes=1))
    assert cond_fresh["ok"] is True
    assert cond_fresh["count"] == 0
    # The progress cutoff is exactly h, and h <= h.
    cond_stale = _problem_jobs(conn, now=h + timedelta(minutes=20))
    assert cond_stale["count"] == 1


def test_p9_young_claim_leftover_heartbeat() -> None:
    """A claim younger than the claim cutoff is never counted, even with a
    stale, leftover heartbeat from an earlier chunk of the same site."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    _running_chunk(conn, site_id, "2026-07-09T11:55:00Z")
    _beat(conn, site_id, "2026-07-09T11:30:00.000Z")
    cond = _problem_jobs(conn)
    assert cond["count"] == 0


def test_p10_pending_arm_still_counts_this_type() -> None:
    """The pending arm is untouched by Edit A/B: an overdue pending
    verification_run row still counts, heartbeat or not."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    _insert_job(
        conn,
        job_type="verification_run",
        site_id=site_id,
        job_key=verification_job_key(site_id),
        status="pending",
        updated_at="2026-07-09T11:30:00Z",
        next_attempt_at="2026-07-09T11:44:00Z",
    )
    _beat(conn, site_id, "2026-07-09T11:58:00.000Z")
    cond = _problem_jobs(conn)
    assert cond["count"] == 1
    assert cond["detail"] == "0 unresolved failed scopes, 1 stuck/overdue jobs"


def test_p11_one_statement_reads_row_and_heartbeat_together() -> None:
    """The job row and its heartbeat come from one compound statement (the
    WL15 pattern, tests/test_monitor.py's set_trace_callback trace around
    build_verdict), so a terminal failure between two reads cannot pair a
    stale 'running' row with an already-deleted heartbeat."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    _running_chunk(conn, site_id, "2026-07-09T11:35:00Z")
    _beat(conn, site_id, "2026-07-09T11:58:00.000Z")
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        build_verdict(
            conn,
            pipeline_enabled=True,
            budget_enabled=False,
            db_enabled=False,
            now=_NOW,
            export_sweeper_dead=None,
        )
    finally:
        conn.set_trace_callback(None)
    matches = [s for s in statements if "verification_heartbeat:" in s]
    assert len(matches) == 1
    (statement,) = matches
    assert "FROM jobs" in statement
    assert "runtime_state" in statement
    assert "'verification_run'" in statement


def test_p12_completed_row_not_counted() -> None:
    """A completed verification_run row is never running, so the status
    filter -- not the progress rule -- keeps it out regardless of its
    (missing) heartbeat."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    _insert_job(
        conn,
        job_type="verification_run",
        site_id=site_id,
        job_key=verification_job_key(site_id),
        status="completed",
        updated_at="2026-07-09T11:00:00Z",
        result="ok",
    )
    cond = _problem_jobs(conn)
    assert cond["ok"] is True
    assert cond["count"] == 0


@pytest.mark.parametrize(
    ("heartbeat", "expected_count"),
    [
        ("2026-07-09T12:00:30.000Z", 0),
        ("2026-07-09T12:05:00.000Z", 0),
        ("2026-07-09T12:05:00.001Z", 1),
        ("2027-07-09T12:00:00.000Z", 1),
    ],
    ids=["a-inside-limit", "b-at-limit", "c-just-past-limit", "d-far-future"],
)
def test_p13_future_heartbeat(heartbeat: str, expected_count: int) -> None:
    """A heartbeat later than now still reads as progress up to the future
    tolerance (a: a day committed while the read waited; b: exactly at the
    limit still counts as progress, the comparison is strict '>'), but past
    it is treated as no evidence of progress at all (c: 1ms past the limit,
    which as text sorts *before* the whole-second limit stamp, pinning that
    the limit is applied to the parsed value, not compared as text; d: a
    clock a year ahead, treated the same as c)."""
    conn = asof_conn()
    site_id = asof_make_site(conn, "A")
    _running_chunk(conn, site_id, "2026-07-09T11:35:00Z")
    _beat(conn, site_id, heartbeat)
    cond = _problem_jobs(conn)
    assert cond["count"] == expected_count
