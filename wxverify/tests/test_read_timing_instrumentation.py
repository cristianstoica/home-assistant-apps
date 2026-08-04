"""Tests for per-read lock/dispatch/exec timing and its /api/worker/status surface.

Covers label disambiguation and aggregation on ``Database.read``, the slow-
read and failed-read log lines, the semantics of a read cancelled while still
queued for the lock, and the exact key set ``/api/worker/status`` publishes
with and without ``?counts=exact``.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import sqlite3
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import (  # noqa: SLF001
    _READ_POOL_SIZE,
    Database,
    close_db,
    get_db,
)


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
            # Drain every pooled connection so the read below has none left to
            # acquire and must queue -- the pool's equivalent of holding the
            # old single lock.
            held = [db._read_pool.get_nowait() for _ in range(_READ_POOL_SIZE)]  # noqa: SLF001
            task = asyncio.create_task(
                db.read(lambda conn: conn.execute("SELECT 1").fetchone())
            )
            await asyncio.sleep(0)  # let the task run up to the empty pool
            assert not task.done(), "read must still be queued for a pooled connection"
            # Hold the pool empty a measurable interval before cancelling so
            # wait_ms has something to lower-bound: a hard 0.0 would prove nothing.
            await asyncio.sleep(0.02)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            for conn in held:
                db._read_pool.put_nowait(conn)  # noqa: SLF001
            return db.read_timing_snapshot()
        finally:
            db.close()

    snapshot = asyncio.run(_drive())
    assert len(snapshot) == 1
    timing = next(iter(snapshot.values()))
    # A read cancelled before a pooled connection is even acquired never
    # reaches the dispatch or exec phases, but it is still recorded as a
    # failed call with a (lower-bound) wait rather than dropped from the
    # snapshot entirely.
    assert timing["calls"] == 1
    assert timing["errors"] == 1
    assert timing["dispatch_ms"] == 0.0
    assert timing["exec_ms"] == 0.0
    # 5 ms margin below the 20 ms hold: one-sided (a loaded machine only
    # makes the sleep longer, never shorter), so this should not be flaky.
    assert timing["wait_ms"] >= 15.0


def test_gate_ms_is_near_zero_normally_and_bounded_below_during_a_swap(
    tmp_path: Path,
) -> None:
    """gate_ms must track time parked behind a closed import-swap gate
    separately from wait_ms: a read served while the gate is already open
    should show ~0 gate_ms, and a read parked behind an in-progress swap
    should show a bounded-below gate_ms without that same time leaking
    into wait_ms once it reaches the (by then fully restocked) pool.
    """

    def _build_replacement_db(path: Path) -> Path:
        placeholder = Database(str(path))
        placeholder.close()
        return path

    async def _drive() -> tuple[dict[str, float], dict[str, float]]:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            await db.read(lambda conn: conn.execute("SELECT 1").fetchone())
            before_labels = set(db.read_timing_snapshot())

            real_replace_sync = Database._replace_sync  # noqa: SLF001

            def _slow_replace_sync(self: Database, new_db: Path, backup: Path) -> None:
                # A fixed, deterministic hold so the gate-closed interval
                # has a known lower bound to check gate_ms against.
                time.sleep(0.02)
                real_replace_sync(self, new_db, backup)

            Database._replace_sync = _slow_replace_sync  # type: ignore[method-assign]  # noqa: SLF001
            try:
                new_path = _build_replacement_db(tmp_path / "new.db")
                backup_path = tmp_path / "backup.db.bak"
                swap_task = asyncio.create_task(db.replace_from(new_path, backup_path))
                await asyncio.sleep(0)
                assert db._read_gate.is_set() is False, (  # noqa: SLF001
                    "the gate must already be closed before the gated "
                    "read below is dispatched, or it proves nothing"
                )

                await db.read(lambda conn: conn.execute("SELECT 2").fetchone())
                await swap_task
            finally:
                Database._replace_sync = real_replace_sync  # type: ignore[method-assign]  # noqa: SLF001

            snapshot = db.read_timing_snapshot()
            normal_label = next(iter(before_labels))
            gated_label = next(iter(set(snapshot) - before_labels))
            return snapshot[normal_label], snapshot[gated_label]
        finally:
            db.close()

    normal, gated = asyncio.run(_drive())
    assert normal["gate_ms"] < 15.0, (
        "a read served while the gate is already open must not show a "
        "meaningful gate_ms"
    )
    # 5 ms margin below the 20 ms hold: one-sided (a loaded machine only
    # makes the sleep longer, never shorter), so this should not be flaky.
    assert gated["gate_ms"] >= 15.0, (
        "a read parked behind an in-progress swap must show that wait in gate_ms"
    )
    assert gated["wait_ms"] < 15.0, (
        "gate time must not leak into wait_ms -- by the time the gated "
        "read passes the gate, the pool has already been restocked, so "
        "its pool wait should stay near zero"
    )


def test_wait_ms_is_near_zero_within_pool_capacity_and_bounded_below_beyond_it(
    tmp_path: Path,
) -> None:
    """wait_ms tracks time spent queued for a pooled connection once past
    the gate. The first _READ_POOL_SIZE concurrent reads should each get a
    connection immediately (near-zero wait); a read dispatched once the
    pool is fully checked out must queue and show a bounded-below wait.
    """
    entered = [threading.Event() for _ in range(_READ_POOL_SIZE)]
    counter = itertools.count()
    release = threading.Event()

    def _held(conn: sqlite3.Connection) -> None:
        entered[next(counter)].set()
        # Held until the test releases it, NOT for a fixed interval: the
        # measured interval has to start AFTER the extra read is already
        # queued, or the setup can consume it and the extra read finds a
        # free connection.
        assert release.wait(timeout=5.0)

    async def _drive() -> tuple[dict[str, float], dict[str, float]]:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            holder_tasks = [
                asyncio.create_task(db.read(_held)) for _ in range(_READ_POOL_SIZE)
            ]
            for event in entered:
                assert await asyncio.to_thread(event.wait, 5.0), (
                    "every holder must actually check out a pooled "
                    "connection and start running, or the pool is not "
                    "genuinely saturated yet"
                )

            extra_task = asyncio.create_task(
                db.read(lambda conn: conn.execute("SELECT 1").fetchone())
            )
            await asyncio.sleep(0)
            assert not extra_task.done(), (
                "with the pool fully checked out, the extra read must "
                "queue rather than complete immediately"
            )
            assert db._read_pool.qsize() == 0, (  # noqa: SLF001
                "the pool must be genuinely empty for the extra read's "
                "wait to mean anything"
            )
            # The lower bound starts HERE -- after the extra read is
            # confirmed queued -- so load can only lengthen it.
            await asyncio.sleep(0.02)
            release.set()

            await asyncio.gather(*holder_tasks, extra_task)
            snapshot = db.read_timing_snapshot()
            assert len(snapshot) == 2
            held_label = next(
                label
                for label, timing in snapshot.items()
                if timing["calls"] == _READ_POOL_SIZE
            )
            extra_label = next(
                label for label, timing in snapshot.items() if timing["calls"] == 1
            )
            return snapshot[held_label], snapshot[extra_label]
        finally:
            db.close()

    held, extra = asyncio.run(_drive())
    assert held["wait_ms"] < 15.0, (
        "each of the first _READ_POOL_SIZE reads must get a pooled "
        "connection immediately -- their combined wait should stay near "
        "zero"
    )
    # 5 ms margin below the 20 ms hold: the interval starts only after the
    # extra read is confirmed queued (see release.set() above), so load
    # can only lengthen it, never shorten it -- this should not be flaky.
    assert extra["wait_ms"] >= 15.0, (
        "a read dispatched while the pool is fully checked out must queue "
        "for a connection, and that queueing time belongs in wait_ms"
    )


def test_read_cancelled_while_waiting_for_the_gate_still_records_an_error(
    tmp_path: Path,
) -> None:
    """A read cancelled while parked behind a closed gate -- the new
    failure phase this pool introduces -- must still be counted as a
    failed call, exactly like a read cancelled while queued for a pooled
    connection already is.
    """

    async def _drive() -> dict[str, dict[str, float]]:
        db = Database(str(tmp_path / "db.sqlite"))
        try:
            db._read_gate.clear()  # noqa: SLF001
            task = asyncio.create_task(
                db.read(lambda conn: conn.execute("SELECT 1").fetchone())
            )
            await asyncio.sleep(0)
            assert not task.done(), "read must be parked behind the closed gate"
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            return db.read_timing_snapshot()
        finally:
            db._read_gate.set()  # noqa: SLF001
            db.close()

    snapshot = asyncio.run(_drive())
    assert len(snapshot) == 1
    timing = next(iter(snapshot.values()))
    assert timing["calls"] == 1
    assert timing["errors"] == 1
    assert timing["dispatch_ms"] == 0.0
    assert timing["exec_ms"] == 0.0
    assert timing["wait_ms"] == 0.0, (
        "a read cancelled before ever passing the gate never reached the "
        "pool-wait phase, so wait_ms must stay at its untouched default "
        "rather than borrowing time from the gate phase"
    )


_BASE_WORKER_STATUS_KEYS = frozenset(
    {
        "jobs",
        "worker_started_at",
        "worker_last_loop_at",
        "scheduler_last_tick_at",
        "import_rebuild_done_at",
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


def test_worker_status_import_rebuild_done_at_present_and_none_before_any_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The key is present-and-None before any import, not absent entirely."""
    close_db()
    config.db_path = str(tmp_path / "worker-status-rebuild-none.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        response = client.get("/api/worker/status")
    assert response.status_code == 200
    body = response.json()
    assert "import_rebuild_done_at" in body
    assert body["import_rebuild_done_at"] is None


def test_worker_status_import_rebuild_done_at_stamped_after_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Once a rebuild has run, the key surfaces a timestamp, not None."""
    from wxverify.db.runtime_state import set_runtime_state_now

    close_db()
    config.db_path = str(tmp_path / "worker-status-rebuild-stamped.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        db = get_db()
        db.write_sync(
            lambda conn: set_runtime_state_now(conn, "import_rebuild_done_at")
        )
        response = client.get("/api/worker/status")
    assert response.status_code == 200
    body = response.json()
    assert body["import_rebuild_done_at"] is not None
    assert isinstance(body["import_rebuild_done_at"], str)


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
