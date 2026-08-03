"""Tests for per-read lock/dispatch/exec timing and its /api/worker/status surface.

Covers label disambiguation and aggregation on ``Database.read``, the slow-
read and failed-read log lines, the semantics of a read cancelled while still
queued for the lock, and the exact key set ``/api/worker/status`` publishes
with and without ``?counts=exact``.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import Database, close_db, get_db


def _shared_read(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT 1").fetchone()[0])


def test_read_label_disambiguates_colocated_lambdas_by_definition_line(
    tmp_path: Path,
) -> None:
    async def _drive() -> dict[str, dict[str, float]]:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            await db.read(lambda conn: conn.execute("SELECT 1").fetchone())
            await db.read(lambda conn: conn.execute("SELECT 2").fetchone())
            return db.read_timing_snapshot()
        finally:
            db.close()

    snapshot = asyncio.run(_drive())
    assert len(snapshot) == 2, (
        "two lambdas sharing a qualname but defined on different lines must "
        "get distinct labels, not collapse into one"
    )
    assert all(timing["calls"] == 1 for timing in snapshot.values())


def test_read_label_collapses_repeat_calls_of_the_same_callback(
    tmp_path: Path,
) -> None:
    async def _drive() -> dict[str, dict[str, float]]:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            await db.read(_shared_read)
            await db.read(_shared_read)
            return db.read_timing_snapshot()
        finally:
            db.close()

    snapshot = asyncio.run(_drive())
    assert len(snapshot) == 1
    timing = next(iter(snapshot.values()))
    assert timing["calls"] == 2
    assert timing["errors"] == 0


def test_slow_read_logs_a_warning_with_all_three_phases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr("wxverify.db.connection.SLOW_READ_MS", -1.0)

    async def _drive() -> None:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            with caplog.at_level(logging.WARNING, logger="wxverify.db.connection"):
                await db.read(lambda conn: conn.execute("SELECT 1").fetchone())
        finally:
            db.close()

    asyncio.run(_drive())
    records = [
        record
        for record in caplog.records
        if record.name == "wxverify.db.connection" and record.levelno == logging.WARNING
    ]
    assert len(records) == 1
    message = records[0].getMessage()
    assert message.startswith("slow db read")
    assert "wait=" in message
    assert "dispatch=" in message
    assert "exec=" in message


def test_failed_read_still_records_a_wait_reraises_and_logs(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    class _Boom(Exception):
        pass

    def _raise(conn: sqlite3.Connection) -> None:
        raise _Boom("read failed")

    async def _drive() -> dict[str, dict[str, float]]:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            with (
                caplog.at_level(logging.WARNING, logger="wxverify.db.connection"),
                pytest.raises(_Boom),
            ):
                await db.read(_raise)
            return db.read_timing_snapshot()
        finally:
            db.close()

    snapshot = asyncio.run(_drive())
    assert len(snapshot) == 1
    timing = next(iter(snapshot.values()))
    assert timing["calls"] == 1
    assert timing["errors"] == 1
    assert timing["wait_ms"] >= 0.0
    records = [
        record
        for record in caplog.records
        if record.name == "wxverify.db.connection" and record.levelno == logging.WARNING
    ]
    assert any(
        record.getMessage().startswith("db read failed or cancelled")
        for record in records
    )


def test_read_cancelled_while_waiting_for_the_lock_still_records_a_lower_bound_wait(
    tmp_path: Path,
) -> None:
    async def _drive() -> dict[str, dict[str, float]]:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            await db._read_lock.acquire()  # noqa: SLF001
            task = asyncio.create_task(
                db.read(lambda conn: conn.execute("SELECT 1").fetchone())
            )
            await asyncio.sleep(0)  # let the task run up to the held lock
            assert not task.done(), "read must still be queued for the lock"
            # Hold the lock a measurable interval before cancelling so wait_ms
            # has something to lower-bound: a hard 0.0 would prove nothing.
            await asyncio.sleep(0.02)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            db._read_lock.release()  # noqa: SLF001
            return db.read_timing_snapshot()
        finally:
            db.close()

    snapshot = asyncio.run(_drive())
    assert len(snapshot) == 1
    timing = next(iter(snapshot.values()))
    # A read cancelled before the lock is even acquired never reaches the
    # dispatch or exec phases, but it is still recorded as a failed call with
    # a (lower-bound) wait rather than dropped from the snapshot entirely.
    assert timing["calls"] == 1
    assert timing["errors"] == 1
    assert timing["dispatch_ms"] == 0.0
    assert timing["exec_ms"] == 0.0
    # 5 ms margin below the 20 ms hold: one-sided (a loaded machine only
    # makes the sleep longer, never shorter), so this should not be flaky.
    assert timing["wait_ms"] >= 15.0


_BASE_WORKER_STATUS_KEYS = frozenset(
    {
        "jobs",
        "worker_started_at",
        "worker_last_loop_at",
        "scheduler_last_tick_at",
        "last_completed_fetch_feed_at",
        "last_completed_fetch_obs_at",
        "last_completed_pair_and_score_at",
    }
)


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


def _seed_one_sample_and_one_pair(conn: sqlite3.Connection) -> None:
    site_id = int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('WorkerStatusCounts', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )
    feed_row = conn.execute(
        "SELECT id FROM feeds WHERE is_virtual = 0 ORDER BY id LIMIT 1"
    ).fetchone()
    assert feed_row is not None
    feed_id = int(feed_row["id"])
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id)
        VALUES (?, ?, 'temperature', '2026-01-01T00:00:00Z',
                '2026-01-01T06:00:00Z', 24, 10.0, '{}', 'run-1')
        """,
        (site_id, feed_id),
    )
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error)
        VALUES (?, ?, 'temperature', '2026-01-01T00:00:00Z',
                '2026-01-01T06:00:00Z', 24, 1, 10.0, 10.0, 0.0, 0.0, 0.0)
        """,
        (site_id, feed_id),
    )


def test_worker_status_default_response_gains_read_timing_unconditionally(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "worker-status.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        response = client.get("/api/worker/status")
    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) == _BASE_WORKER_STATUS_KEYS | {
        "read_timing",
        "read_timing_since",
    }
    assert isinstance(body["read_timing"], dict)
    assert isinstance(body["read_timing_since"], str)


def test_worker_status_counts_exact_adds_the_two_row_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "worker-status-counts.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(_seed_one_sample_and_one_pair)
        response = client.get("/api/worker/status", params={"counts": "exact"})
    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) == (
        _BASE_WORKER_STATUS_KEYS
        | {
            "read_timing",
            "read_timing_since",
            "forecast_samples_rows",
            "forecast_pairs_rows",
        }
    )
    assert body["forecast_samples_rows"] == 1
    assert body["forecast_pairs_rows"] == 1
