"""Worker-path oracles for the result='ok' success marker (plan §6.5, §9).

O14-O16 drive the marker through the real worker loop -- run_worker,
claim_next_job, dispatch, _fetch_feed, fetch_feed_once,
_complete_and_continue / the JobCancelled branch -- with only the HTTP
adapter and the loop's housekeeping stubbed. Every fixture is synthetic:
one site named 'PatchTest' (via tests.test_011_patch's helpers) at the
test suite's placeholder coordinates, and the open-meteo feed already
seeded by init.

This module carries the §6.5 harness and O14, O15 and O16.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.test_011_patch import (
    _init_tmp_db,
    _insert_site,
    _open_meteo_feed_id,
    _patch_worker_infra,
    _StopLoop,
)
from tests.test_problem_jobs_supersession import _problem_jobs
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import get_db
from wxverify.db.queue import Job, claim_next_job, enqueue_if_absent
from wxverify.feeds.seam import CostEstimate, FetchResult
from wxverify.worker.processor import run_worker


class _NoOpAdapter:
    """Builds, meters one call, returns no samples: the FetchFeedNoOp path."""

    supports_historical = False

    def estimate_cost(self, req: Any) -> CostEstimate:
        return CostEstimate(calls=1)

    async def fetch_forecast(self, req: Any) -> FetchResult:
        return FetchResult(samples=[], grid=None)


def _build_noop(source: str, client: httpx.AsyncClient) -> _NoOpAdapter:
    return _NoOpAdapter()


def _build_raises(source: str, client: httpx.AsyncClient) -> _NoOpAdapter:
    raise RuntimeError("synthetic adapter construction failure")


def _claim_real_once() -> Callable[[sqlite3.Connection], Job | None]:
    """Run the real claim_next_job exactly once, then stop the loop."""
    calls: list[int] = []

    def claim(conn: sqlite3.Connection) -> Job | None:
        calls.append(1)
        if len(calls) == 1:
            return claim_next_job(conn)
        raise _StopLoop()

    return claim


def _enqueue_fetch(conn: sqlite3.Connection, site_id: int, feed_id: int) -> int:
    created = enqueue_if_absent(
        conn, "fetch_feed", site_id, f"fetch:{feed_id}", {"feed_id": feed_id}
    )
    assert created.created and created.job_id is not None
    return created.job_id


def _run_one_cycle(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_real_once())
    with pytest.raises(_StopLoop):
        asyncio.run(run_worker(get_db()))


def _job_row(conn: sqlite3.Connection, job_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT status, result, retry_count FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    assert row is not None
    return row


def test_genuine_success_writes_the_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O14 (a) -- a genuine success writes the marker through the real loop."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _open_meteo_feed_id(conn)
    conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, payload, status, updated_at)
        VALUES ('fetch_feed', ?, ?, '{}', 'failed', ?)
        """,
        (
            site_id,
            f"fetch:{feed_id}",
            isoformat_utc(utc_now() - timedelta(hours=72)),
        ),
    )
    job_id = _enqueue_fetch(conn, site_id, feed_id)
    monkeypatch.setattr("wxverify.worker.processor.build_adapter", _build_noop)
    _run_one_cycle(monkeypatch)

    row = _job_row(conn, job_id)
    assert row["status"] == "completed"
    assert row["result"] == "ok"
    assert row["retry_count"] == 0


def test_genuine_success_supersedes_the_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O14 (b) -- a genuine success writes the marker through the real
    loop, and it supersedes the standing failure in the verdict."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _open_meteo_feed_id(conn)
    conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, payload, status, updated_at)
        VALUES ('fetch_feed', ?, ?, '{}', 'failed', ?)
        """,
        (
            site_id,
            f"fetch:{feed_id}",
            isoformat_utc(utc_now() - timedelta(hours=72)),
        ),
    )
    _enqueue_fetch(conn, site_id, feed_id)
    monkeypatch.setattr("wxverify.worker.processor.build_adapter", _build_noop)
    _run_one_cycle(monkeypatch)

    cond = _problem_jobs(conn, now=utc_now())
    assert cond["count"] == 0


def _build_counting(calls: list[str]) -> Callable[[str, httpx.AsyncClient], Any]:
    def build(source: str, client: httpx.AsyncClient) -> _NoOpAdapter:
        calls.append(source)
        return _NoOpAdapter()

    return build


def test_ineligible_job_completes_without_the_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """O15 -- a cancelled job completes without the marker."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _open_meteo_feed_id(conn)
    job_id = _enqueue_fetch(conn, site_id, feed_id)
    conn.execute("UPDATE sites SET enabled = 0 WHERE id = ?", (site_id,))
    calls: list[str] = []
    monkeypatch.setattr(
        "wxverify.worker.processor.build_adapter", _build_counting(calls)
    )

    with caplog.at_level(logging.INFO, logger="wxverify.worker.processor"):
        _run_one_cycle(monkeypatch)

    assert calls == []
    row = _job_row(conn, job_id)
    assert row["status"] == "completed"
    assert row["result"] is None
    assert row["retry_count"] == 0

    cycle_records = [
        r for r in caplog.records if r.getMessage().startswith("cycle: job=")
    ]
    assert len(cycle_records) == 1
    assert "outcome=cancelled" in cycle_records[0].getMessage()


def test_unavailable_feed_completes_without_the_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """O16 -- an unavailable feed completes without the marker, through the
    cancelled branch."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _open_meteo_feed_id(conn)
    job_id = _enqueue_fetch(conn, site_id, feed_id)
    monkeypatch.setattr("wxverify.worker.processor.build_adapter", _build_raises)

    with caplog.at_level(logging.INFO, logger="wxverify.worker.processor"):
        _run_one_cycle(monkeypatch)

    row = _job_row(conn, job_id)
    assert row["status"] == "completed"
    assert row["result"] is None
    assert row["retry_count"] == 0

    cycle_records = [
        r for r in caplog.records if r.getMessage().startswith("cycle: job=")
    ]
    assert len(cycle_records) == 1
    assert "outcome=cancelled" in cycle_records[0].getMessage()

    state = conn.execute(
        """
        SELECT last_error, error_count FROM site_feed_state
        WHERE site_id = ? AND feed_id = ?
        """,
        (site_id, feed_id),
    ).fetchone()
    assert state is not None
    assert state["last_error"] is not None
    assert state["error_count"] == 1
