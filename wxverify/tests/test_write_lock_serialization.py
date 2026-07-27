"""Tests for the 0.8.8 write-lock-serialization fix.

Two families, per docs/plans/2026-07-27-write-lock-serialization-fix.md:

- R1-R4 (S2): the leaderboard route's fire-and-forget rescore scheduling no
  longer blocks the read behind a held write lock.
- E1-E8 (S3): ``run_batched_scoring`` (bounded per-window write transactions)
  reaches the same end state as the monolithic ``_score_all_windows`` run it
  replaces, never exposes a half-populated cache mid-run, and survives a
  mid-run crash, site-disable, or an in-flight concurrent writer.

R5 (cooldown) and the E1-E8 plan's composite_with_status cross-check are out
of scope for this file; see the final report for both calls.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from wxverify import config
from wxverify.api.routes.dashboard import leaderboard
from wxverify.core.timeutil import isoformat_utc_micro
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.queue import Job
from wxverify.scoring import rescore as rescore_module
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.engine import (
    _score_all_windows,  # noqa: SLF001 - the monolithic sibling under test
    discover_score_work,
    sweep_score_orphans,
)
from wxverify.scoring.leaderboard import leaderboard_with_status
from wxverify.scoring.metrics import MetricResult
from wxverify.scoring.rescore import drain_pending_rescores
from wxverify.settings.keys import set_setting
from wxverify.worker.catchup import run_catchup
from wxverify.worker.control import JobCancelled
from wxverify.worker.processor import run_worker
from wxverify.worker.score_batches import run_batched_scoring

_FROZEN_NOW = datetime(2035, 6, 15, 12, 0, 0, tzinfo=UTC)


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


def _freeze_now(monkeypatch: pytest.MonkeyPatch, when: datetime = _FROZEN_NOW) -> None:
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: when)


def _make_site(conn: sqlite3.Connection, name: str, *, enabled: int = 1) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites
            (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
        VALUES (?, 47.0, 25.0, 900.0, 'UTC', ?)
        """,
        (name, enabled),
    )
    return int(cur.lastrowid)


def _open_meteo_feed_ids(conn: sqlite3.Connection, count: int) -> list[int]:
    rows = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT ?",
        (count,),
    ).fetchall()
    return [int(row["id"]) for row in rows]


def _virtual_persistence_feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
    ).fetchone()
    return int(row["id"])


def _seed_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    valid_at: str,
    day_ahead: int = 1,
    issued_at: str = "2035-06-01T00:00:00Z",
    forecast: float = 11.0,
    observed: float = 10.0,
) -> None:
    error = forecast - observed
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours, day_ahead,
             forecast, observed, error, abs_error, sq_error)
        VALUES (?, ?, ?, ?, ?, 24, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            day_ahead,
            forecast,
            observed,
            error,
            abs(error),
            error * error,
        ),
    )


def _seed_score_cache(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    window_key: str,
    computed_at: str,
    variable: str = "temperature",
    day_ahead: int = 1,
    n: int = 1,
    skill_score: float = 0.5,
) -> None:
    upsert_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable=variable,
        day_ahead=day_ahead,
        window_key=window_key,
        result=MetricResult(n=n, skill_score=skill_score, confident=True),
        computed_at=computed_at,
    )


def _job_count(conn: sqlite3.Connection, site_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type='pair_and_score' AND site_id=?",
        (site_id,),
    ).fetchone()
    return int(row["n"])


def _insert_pair_and_score_job(conn: sqlite3.Connection, *, site_id: int) -> int:
    cur = conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload) "
        "VALUES ('pair_and_score', ?, 'score', '{}')",
        (site_id,),
    )
    return int(cur.lastrowid)


def _dump_score_cache(conn: sqlite3.Connection) -> list[dict[str, object]]:
    """Full ``score_cache`` snapshot excluding ``computed_at`` (the only
    column two independent runs may legitimately differ on)."""
    rows = conn.execute(
        """
        SELECT site_id, feed_id, variable, day_ahead, window_key,
               n, bias, mae, rmse, pod, far, csi, ets, hss, skill_score
        FROM score_cache
        ORDER BY site_id, feed_id, variable, day_ahead, window_key
        """
    ).fetchall()
    return [dict(row) for row in rows]


async def _start_write_hold(db: Any, seconds: float) -> asyncio.Task[None]:
    """Start a task holding the write lock for ``seconds``; return once the
    lock is confirmed actually held (bounded poll, per plan's R1 wording)."""
    task = asyncio.create_task(db.write(lambda conn: time.sleep(seconds)))
    deadline = time.monotonic() + 2.0
    while not db._write_lock.locked():  # noqa: SLF001 - the seam under test
        if time.monotonic() > deadline:
            raise TimeoutError("write lock was never observed held")
        await asyncio.sleep(0.01)
    return task


class _StopLoop(Exception):
    """Stops ``run_worker``'s loop after exactly one claimed job."""


def _claim_once(job: Job) -> Any:
    calls: list[int] = []

    def _claim(conn: sqlite3.Connection) -> Job:
        calls.append(1)
        if len(calls) == 1:
            return job
        raise _StopLoop()

    return _claim


def _patch_worker_infra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "wxverify.worker.processor.set_runtime_state_now", lambda c, k: None
    )
    monkeypatch.setattr("wxverify.worker.processor.scheduler_tick", lambda c: None)
    monkeypatch.setattr(
        "wxverify.worker.processor.purge_failed_jobs_older_than", lambda c, h: None
    )


# --------------------------------------------------------------------------
# R1-R4: leaderboard route rescore scheduling
# --------------------------------------------------------------------------


def test_leaderboard_read_not_blocked_by_write_lock_hold_r1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(conn, "min_n", "1")
        set_setting(conn, "rolling_window_days", "14")
        site_id = _make_site(conn, "r1-site")
        (feed_id,) = _open_meteo_feed_ids(conn, 1)
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            valid_at="2035-06-15T06:00:00Z",
        )
        _seed_score_cache(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            window_key="w:14",
            computed_at="2035-06-14T09:00:00.000000Z",  # yesterday -> stale
        )

        assert not rescore_module._in_flight_sites  # noqa: SLF001
        db = get_db()
        hold = await _start_write_hold(db, 3.0)

        # The stale snapshot returns well within 1s even while the write
        # lock is held for 3s: on pre-fix code (route awaits the enqueue
        # behind the hold) this times out instead.
        result = await asyncio.wait_for(
            leaderboard(
                site=site_id, variable="temperature", window="rolling", lead="D+1"
            ),
            timeout=1.0,
        )
        assert len(result) == 1

        await hold
        await drain_pending_rescores()
        assert _job_count(conn, site_id) == 1

    asyncio.run(_run())


def test_leaderboard_concurrent_stale_reads_dedupe_to_one_rescore_task_r2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(conn, "min_n", "1")
        set_setting(conn, "rolling_window_days", "14")
        site_id = _make_site(conn, "r2-site")
        (feed_id,) = _open_meteo_feed_ids(conn, 1)
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            valid_at="2035-06-15T06:00:00Z",
        )
        _seed_score_cache(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            window_key="w:14",
            computed_at="2035-06-14T09:00:00.000000Z",
        )

        assert not rescore_module._in_flight_sites  # noqa: SLF001
        db = get_db()
        hold = await _start_write_hold(db, 3.0)

        for _ in range(3):
            result = await asyncio.wait_for(
                leaderboard(
                    site=site_id, variable="temperature", window="rolling", lead="D+1"
                ),
                timeout=1.0,
            )
            assert len(result) == 1

        # Dedupe: three stale reads while the lock is held still schedule
        # exactly one task, not three.
        assert len(rescore_module._pending_tasks) == 1  # noqa: SLF001

        await hold
        await drain_pending_rescores()
        assert _job_count(conn, site_id) == 1

    asyncio.run(_run())


def test_leaderboard_hit_status_schedules_no_rescore_r3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(conn, "min_n", "1")
        set_setting(conn, "rolling_window_days", "14")
        site_id = _make_site(conn, "r3-hit-site")
        (feed_id,) = _open_meteo_feed_ids(conn, 1)
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            valid_at="2035-06-15T06:00:00Z",
        )
        _seed_score_cache(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            window_key="w:14",
            computed_at="2035-06-15T10:00:00.000000Z",  # today -> fresh
        )

        assert not rescore_module._in_flight_sites  # noqa: SLF001
        result = await leaderboard(
            site=site_id, variable="temperature", window="rolling", lead="D+1"
        )
        assert len(result) == 1
        assert not rescore_module._in_flight_sites  # noqa: SLF001
        assert not rescore_module._pending_tasks  # noqa: SLF001
        assert _job_count(conn, site_id) == 0

    asyncio.run(_run())


def test_leaderboard_empty_status_schedules_no_rescore_r3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        site_id = _make_site(conn, "r3-empty-site")
        # No forecast_pairs at all for this site/variable: the no-input gate
        # fires before any cache branch, regardless of cache contents.
        assert not rescore_module._in_flight_sites  # noqa: SLF001
        result = await leaderboard(
            site=site_id, variable="temperature", window="rolling", lead="D+1"
        )
        assert result == []
        assert not rescore_module._in_flight_sites  # noqa: SLF001
        assert not rescore_module._pending_tasks  # noqa: SLF001
        assert _job_count(conn, site_id) == 0

    asyncio.run(_run())


def test_leaderboard_live_window_schedules_no_rescore_r3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(conn, "min_n", "1")
        site_id = _make_site(conn, "r3-live-site")
        (feed_id,) = _open_meteo_feed_ids(conn, 1)
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            valid_at="2035-06-15T06:00:00Z",
        )
        # A custom "Nd" window always computes live and bypasses the cache.
        assert not rescore_module._in_flight_sites  # noqa: SLF001
        result = await leaderboard(
            site=site_id, variable="temperature", window="7d", lead="D+1"
        )
        assert len(result) == 1
        assert not rescore_module._in_flight_sites  # noqa: SLF001
        assert not rescore_module._pending_tasks  # noqa: SLF001
        assert _job_count(conn, site_id) == 0

    asyncio.run(_run())


def test_leaderboard_rescore_enqueue_failure_logged_response_unaffected_r4(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(conn, "min_n", "1")
        set_setting(conn, "rolling_window_days", "14")
        site_id = _make_site(conn, "r4-site")
        (feed_id,) = _open_meteo_feed_ids(conn, 1)
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            valid_at="2035-06-15T06:00:00Z",
        )
        _seed_score_cache(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            window_key="w:14",
            computed_at="2035-06-14T09:00:00.000000Z",  # stale
        )

        calls: list[int] = []

        def _raise(inner_conn: sqlite3.Connection, sid: int) -> None:
            calls.append(sid)
            raise RuntimeError("synthetic enqueue failure")

        monkeypatch.setattr("wxverify.scoring.rescore.enqueue_score_rescore", _raise)

        assert not rescore_module._in_flight_sites  # noqa: SLF001
        with caplog.at_level(logging.WARNING, logger="wxverify.scoring.rescore"):
            result = await leaderboard(
                site=site_id, variable="temperature", window="rolling", lead="D+1"
            )
            assert len(result) == 1
            await drain_pending_rescores()

        assert calls == [site_id]  # non-vacuity: the patched callable ran
        assert any(
            record.levelno == logging.WARNING
            and "rescore enqueue failed" in record.getMessage()
            for record in caplog.records
        )
        assert _job_count(conn, site_id) == 0

    asyncio.run(_run())


# --------------------------------------------------------------------------
# E1: end-state equivalence (the linchpin oracle) + stamp-width regression
# --------------------------------------------------------------------------


def test_batched_scoring_matches_monolithic_end_state_e1(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        source_conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(source_conn, "min_n", "1")
        set_setting(source_conn, "rolling_window_days", "14")

        site_a = _make_site(source_conn, "e1-site-a")
        site_b = _make_site(source_conn, "e1-site-b")
        feed1, feed2, feed3 = _open_meteo_feed_ids(source_conn, 3)
        feed_v = _virtual_persistence_feed_id(source_conn)

        # Cell A1: in-window -> n=1 both windows.
        _seed_pair(
            source_conn,
            site_id=site_a,
            feed_id=feed1,
            variable="temperature",
            day_ahead=1,
            valid_at="2035-06-15T06:00:00Z",
        )
        # Cell A2: in-window, second variable.
        _seed_pair(
            source_conn,
            site_id=site_a,
            feed_id=feed2,
            variable="wind",
            day_ahead=1,
            valid_at="2035-06-15T06:00:00Z",
        )
        # Cell A3: only pair is outside the 14d cutoff -> n=0 in w:14
        # (skip-then-sweep), n=1 in w:all.
        _seed_pair(
            source_conn,
            site_id=site_a,
            feed_id=feed3,
            variable="temperature",
            day_ahead=2,
            valid_at="2035-05-01T06:00:00Z",
        )
        # Cell A4: virtual persistence feed, in-window.
        _seed_pair(
            source_conn,
            site_id=site_a,
            feed_id=feed_v,
            variable="temperature",
            day_ahead=1,
            valid_at="2035-06-15T07:00:00Z",
        )
        # site_b: independent in-window cell, proves no cross-site bleed.
        _seed_pair(
            source_conn,
            site_id=site_b,
            feed_id=feed1,
            variable="temperature",
            day_ahead=1,
            valid_at="2035-06-15T06:00:00Z",
        )

        # (i) orphan: no forecast_pairs row exists for this cell at all.
        _seed_score_cache(
            source_conn,
            site_id=site_a,
            feed_id=feed1,
            variable="temperature",
            day_ahead=99,
            window_key="w:14",
            computed_at="2035-06-10T00:00:00.000000Z",
        )
        # (ii) stale sentinel value on a live cell (A1/w:14) -> must be
        # overwritten with the freshly-computed value, not left in place.
        _seed_score_cache(
            source_conn,
            site_id=site_a,
            feed_id=feed1,
            variable="temperature",
            day_ahead=1,
            window_key="w:14",
            skill_score=-99.0,
            computed_at="2035-06-10T00:00:00.000000Z",
        )
        # (iii) abandoned window_key on a live cell -> swept regardless of
        # window (sweep has no window_key filter).
        _seed_score_cache(
            source_conn,
            site_id=site_a,
            feed_id=feed1,
            variable="temperature",
            day_ahead=1,
            window_key="w:999",
            computed_at="2035-06-10T00:00:00.000000Z",
        )
        # (iv) stale row for A3/w:14, which legitimately aggregates to n==0
        # this run -> skip-then-sweep, not skip-then-linger.
        _seed_score_cache(
            source_conn,
            site_id=site_a,
            feed_id=feed3,
            variable="temperature",
            day_ahead=2,
            window_key="w:14",
            computed_at="2035-06-10T00:00:00.000000Z",
        )

        copy_a = tmp_path / "run_a.db"
        copy_b = tmp_path / "run_b.db"
        source_conn.execute("VACUUM INTO ?", (str(copy_a),))
        source_conn.execute("VACUUM INTO ?", (str(copy_b),))
        close_db()

        expected_w14 = {
            (site_a, feed1, "temperature", 1, "w:14"),
            (site_a, feed2, "wind", 1, "w:14"),
            (site_a, feed_v, "temperature", 1, "w:14"),
            (site_b, feed1, "temperature", 1, "w:14"),
        }
        expected_w_all = {
            (site_a, feed1, "temperature", 1, "w:all"),
            (site_a, feed2, "wind", 1, "w:all"),
            (site_a, feed3, "temperature", 2, "w:all"),
            (site_a, feed_v, "temperature", 1, "w:all"),
            (site_b, feed1, "temperature", 1, "w:all"),
        }
        expected_keys = expected_w14 | expected_w_all

        def _keys(conn: sqlite3.Connection) -> set[tuple[int, int, str, int, str]]:
            rows = conn.execute(
                "SELECT site_id, feed_id, variable, day_ahead, window_key "
                "FROM score_cache"
            ).fetchall()
            return {
                (
                    int(r["site_id"]),
                    int(r["feed_id"]),
                    str(r["variable"]),
                    int(r["day_ahead"]),
                    str(r["window_key"]),
                )
                for r in rows
            }

        # Run A: monolithic, all sites in one write transaction.
        db_a = init_db(str(copy_a))
        await db_a.write(lambda conn: _score_all_windows(conn, None))
        end_a = _dump_score_cache(db_a._conn)  # noqa: SLF001
        keys_a = _keys(db_a._conn)  # noqa: SLF001
        close_db()

        # Run B: batched, per site, with SCORE_BATCH_CELLS forced small so
        # batching is actually exercised.
        monkeypatch.setattr("wxverify.worker.score_batches.SCORE_BATCH_CELLS", 2)
        db_b = init_db(str(copy_b))
        await run_batched_scoring(db_b, site_a)
        await run_batched_scoring(db_b, site_b)
        end_b = _dump_score_cache(db_b._conn)  # noqa: SLF001
        keys_b = _keys(db_b._conn)  # noqa: SLF001

        overwritten = db_b._conn.execute(  # noqa: SLF001
            "SELECT skill_score FROM score_cache WHERE site_id=? AND feed_id=? "
            "AND variable='temperature' AND day_ahead=1 AND window_key='w:14'",
            (site_a, feed1),
        ).fetchone()

        # Equality clause: identical end states modulo computed_at.
        assert end_a == end_b
        # Ground-truth clause: hand-computed expected survivors, checked
        # against both runs independently.
        assert keys_a == expected_keys
        assert keys_b == expected_keys
        # The stale sentinel was overwritten, not left in place.
        assert overwritten["skill_score"] != -99.0

    asyncio.run(_run())


def test_score_run_stamp_fixed_width_spares_same_second_inline_row_e1b(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        whole_second = datetime(2035, 6, 15, 12, 0, 0, tzinfo=UTC)
        _freeze_now(monkeypatch, whole_second)
        site_id = _make_site(conn, "e1b-site")
        (feed_id,) = _open_meteo_feed_ids(conn, 1)

        db = get_db()
        work = await db.write(lambda c: discover_score_work(c, site_id))
        # A whole-second capture must still carry the fixed-width fractional
        # field: the plain isoformat_utc formatter would collapse this to
        # "...:00Z", which sorts AFTER a later same-second stamp because
        # '.' < 'Z' lexically.
        assert work.run_stamp == "2035-06-15T12:00:00.000000Z"

        later_same_second = whole_second.replace(microsecond=512345)
        inline_stamp = isoformat_utc_micro(later_same_second)
        _seed_score_cache(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            window_key="w:14",
            computed_at=inline_stamp,
        )

        removed = await db.write(
            lambda c: sweep_score_orphans(c, site_id, work.run_stamp)
        )
        assert removed == 0
        survivor = conn.execute(
            "SELECT computed_at FROM score_cache WHERE site_id=?", (site_id,)
        ).fetchone()
        assert survivor is not None
        assert survivor["computed_at"] == inline_stamp

    asyncio.run(_run())


# --------------------------------------------------------------------------
# E2: bounded transactions (structural)
# --------------------------------------------------------------------------


class _UpsertCountingDb:
    """Counts real ``score_cache`` upserts per ``db.write`` call via a real
    SQL trace on the real connection -- not a mock of ``score_cell_batch``.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.per_call_upserts: list[int] = []
        self._current: list[str] = []
        inner._conn.set_trace_callback(self._trace)  # noqa: SLF001

    def _trace(self, sql: str) -> None:
        if sql.strip().upper().startswith("INSERT INTO SCORE_CACHE"):
            self._current.append(sql)

    async def write(self, fn: Any) -> Any:
        self._current = []
        result = await self._inner.write(fn)
        self.per_call_upserts.append(len(self._current))
        return result

    async def read(self, fn: Any) -> Any:
        return await self._inner.read(fn)


def test_batched_scoring_bounds_upserts_per_transaction_e2(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(conn, "min_n", "1")
        set_setting(conn, "rolling_window_days", "14")
        monkeypatch.setattr("wxverify.worker.score_batches.SCORE_BATCH_CELLS", 2)

        site_id = _make_site(conn, "e2-site")
        feed_ids = _open_meteo_feed_ids(conn, 5)
        for i, feed_id in enumerate(feed_ids):
            _seed_pair(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="temperature",
                day_ahead=1,
                valid_at=f"2035-06-15T0{i}:00:00Z",
            )

        spy = _UpsertCountingDb(get_db())
        await run_batched_scoring(spy, site_id)

        batch_calls = [c for c in spy.per_call_upserts if c > 0]
        assert all(c <= 2 for c in batch_calls)
        # ceil(5/2)=3 batches per window * 2 windows.
        assert len(batch_calls) == 6
        assert sum(batch_calls) == 10  # 5 cells * 2 windows, all n>=1
        # discovery + 6 batch transactions + final sweep.
        assert len(spy.per_call_upserts) == 8

    asyncio.run(_run())


# --------------------------------------------------------------------------
# E3: mid-run completeness (invariant a)
# --------------------------------------------------------------------------


def test_batched_scoring_mid_run_leaderboard_reads_never_partial_e3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(conn, "min_n", "1")
        set_setting(conn, "rolling_window_days", "14")
        monkeypatch.setattr("wxverify.worker.score_batches.SCORE_BATCH_CELLS", 1)

        site_id = _make_site(conn, "e3-site")
        feed_ids = _open_meteo_feed_ids(conn, 3)
        for i, feed_id in enumerate(feed_ids):
            _seed_pair(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="temperature",
                day_ahead=1,
                valid_at=f"2035-06-15T0{i}:00:00Z",
            )

        db = get_db()
        observed: list[int] = []

        async def _on_batch_committed() -> None:
            result = await db.read(
                lambda c: leaderboard_with_status(
                    c,
                    site_id=site_id,
                    variable="temperature",
                    day_ahead=1,
                    window="rolling",
                )
            )
            observed.append(len(result.rows))

        await run_batched_scoring(db, site_id, _on_batch_committed)

        assert observed  # non-vacuity: the hook actually fired
        assert all(count in (0, 3) for count in observed)
        assert 0 in observed  # w:14's early batches genuinely show rebuilding
        assert 3 in observed  # and a complete snapshot genuinely occurs mid-run

        final = await db.read(
            lambda c: leaderboard_with_status(
                c,
                site_id=site_id,
                variable="temperature",
                day_ahead=1,
                window="rolling",
            )
        )
        assert final.status == "hit"
        assert len(final.rows) == 3

    asyncio.run(_run())


# --------------------------------------------------------------------------
# E4: crash/resume convergence
# --------------------------------------------------------------------------


def test_batched_scoring_crash_mid_run_marks_job_retry_then_converges_e4(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(conn, "min_n", "1")
        set_setting(conn, "rolling_window_days", "14")

        site_id = _make_site(conn, "e4-site")
        feed_ids = _open_meteo_feed_ids(conn, 4)
        for i, feed_id in enumerate(feed_ids):
            _seed_pair(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable="temperature",
                day_ahead=1,
                valid_at=f"2035-06-15T0{i}:00:00Z",
            )
        job_id = _insert_pair_and_score_job(conn, site_id=site_id)

        clean_path = tmp_path / "e4_clean.db"
        conn.execute("VACUUM INTO ?", (str(clean_path),))

        monkeypatch.setattr("wxverify.worker.score_batches.SCORE_BATCH_CELLS", 2)
        _patch_worker_infra(monkeypatch)

        job = Job(
            id=job_id,
            type="pair_and_score",
            site_id=site_id,
            job_key="score",
            payload={},
            status="running",
            retry_count=0,
            max_retries=5,
        )
        db = get_db()
        batches_seen = 0

        async def _crashing_run_batched_scoring(inner_db: Any, sid: int) -> None:
            nonlocal batches_seen

            async def _hook() -> None:
                nonlocal batches_seen
                batches_seen += 1
                if batches_seen == 1:
                    raise RuntimeError("synthetic mid-run crash")

            await run_batched_scoring(inner_db, sid, _hook)

        monkeypatch.setattr(
            "wxverify.worker.processor.run_batched_scoring",
            _crashing_run_batched_scoring,
        )
        monkeypatch.setattr(
            "wxverify.worker.processor.claim_next_job", _claim_once(job)
        )

        with pytest.raises(_StopLoop):
            await run_worker(db)

        # First batch (2 of 4 cells, w:14 only) committed for real before
        # the crash; identity of the two cells is non-deterministic
        # (_distinct_cells has no ORDER BY) so only count + window are pinned.
        rows = conn.execute(
            "SELECT window_key FROM score_cache WHERE site_id=?", (site_id,)
        ).fetchall()
        assert len(rows) == 2
        assert {r["window_key"] for r in rows} == {"w:14"}

        job_row = conn.execute(
            "SELECT status, retry_count, next_attempt_at FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
        assert job_row["status"] == "pending"
        assert job_row["retry_count"] == 1
        assert job_row["next_attempt_at"] is not None
        assert not db._write_lock.locked()  # noqa: SLF001 - lock cleanly released

        # Retry: un-crash, re-claim, run to completion.
        monkeypatch.setattr(
            "wxverify.worker.processor.run_batched_scoring", run_batched_scoring
        )
        retry_job = Job(
            id=job_id,
            type="pair_and_score",
            site_id=site_id,
            job_key="score",
            payload={},
            status="running",
            retry_count=1,
            max_retries=5,
        )
        monkeypatch.setattr(
            "wxverify.worker.processor.claim_next_job", _claim_once(retry_job)
        )
        with pytest.raises(_StopLoop):
            await run_worker(db)

        job_row2 = conn.execute(
            "SELECT status FROM jobs WHERE id=?", (job_id,)
        ).fetchone()
        assert job_row2["status"] == "completed"

        end_after_resume = _dump_score_cache(conn)
        close_db()

        db_clean = init_db(str(clean_path))
        await db_clean.write(lambda c: _score_all_windows(c, None))
        end_clean = _dump_score_cache(db_clean._conn)  # noqa: SLF001

        assert end_after_resume == end_clean

    asyncio.run(_run())


# --------------------------------------------------------------------------
# E5: same-day relaxation is bounded
# --------------------------------------------------------------------------


def test_batched_scoring_same_day_rerun_mixed_generation_bounded_e5(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(conn, "min_n", "1")
        set_setting(conn, "rolling_window_days", "14")

        site_id = _make_site(conn, "e5-site")
        feed1, feed2 = _open_meteo_feed_ids(conn, 2)
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed1,
            variable="temperature",
            day_ahead=1,
            valid_at="2035-06-15T01:00:00Z",
        )
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed2,
            variable="temperature",
            day_ahead=1,
            valid_at="2035-06-15T01:00:00Z",
        )

        db = get_db()
        # Generation 1: score to completion monolithically.
        await db.write(lambda c: _score_all_windows(c, None))

        # Generation 2: both cells gain an additional pair (n: 1 -> 2).
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed1,
            variable="temperature",
            day_ahead=1,
            valid_at="2035-06-15T02:00:00Z",
            forecast=12.0,
            observed=10.5,
        )
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed2,
            variable="temperature",
            day_ahead=1,
            valid_at="2035-06-15T02:00:00Z",
            forecast=12.0,
            observed=10.5,
        )

        monkeypatch.setattr("wxverify.worker.score_batches.SCORE_BATCH_CELLS", 1)
        observed_mixed = False

        async def _on_batch_committed() -> None:
            nonlocal observed_mixed
            result = await db.read(
                lambda c: leaderboard_with_status(
                    c,
                    site_id=site_id,
                    variable="temperature",
                    day_ahead=1,
                    window="rolling",
                )
            )
            # Both rows always exist continuously across the rerun (upsert
            # never deletes mid-run), so the read is always complete/hit.
            assert result.status == "hit"
            assert len(result.rows) == 2
            ns = sorted(row.n for row in result.rows)
            if ns == [1, 2]:
                observed_mixed = True

        await run_batched_scoring(db, site_id, _on_batch_committed)

        assert observed_mixed  # the accepted same-day mixed-generation window

        final = await db.read(
            lambda c: leaderboard_with_status(
                c,
                site_id=site_id,
                variable="temperature",
                day_ahead=1,
                window="rolling",
            )
        )
        assert all(row.n == 2 for row in final.rows)

    asyncio.run(_run())


# --------------------------------------------------------------------------
# E6: mid-run site-disable
# --------------------------------------------------------------------------


def test_batched_scoring_mid_run_site_disable_cancels_e6(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(conn, "min_n", "1")
        set_setting(conn, "rolling_window_days", "14")
        monkeypatch.setattr("wxverify.worker.score_batches.SCORE_BATCH_CELLS", 1)

        site_id = _make_site(conn, "e6-site")
        feed1, feed2 = _open_meteo_feed_ids(conn, 2)
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed1,
            variable="temperature",
            day_ahead=1,
            valid_at="2035-06-15T01:00:00Z",
        )
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed2,
            variable="temperature",
            day_ahead=1,
            valid_at="2035-06-15T02:00:00Z",
        )

        db = get_db()
        batches_seen = 0

        async def _disable_after_first_batch() -> None:
            nonlocal batches_seen
            batches_seen += 1
            if batches_seen == 1:
                await db.write(
                    lambda c: c.execute(
                        "UPDATE sites SET enabled=0 WHERE id=?", (site_id,)
                    )
                )

        with pytest.raises(JobCancelled):
            await run_batched_scoring(db, site_id, _disable_after_first_batch)

        # No further writes occurred: the guard's raise rolled back the
        # transaction that would have scored the second cell.
        rows = conn.execute(
            "SELECT window_key FROM score_cache WHERE site_id=?", (site_id,)
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["window_key"] == "w:14"

    asyncio.run(_run())


# --------------------------------------------------------------------------
# E7: inline writer in flight at discovery
# --------------------------------------------------------------------------


def test_batched_scoring_discovery_blocks_on_inline_writer_e7(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        set_setting(conn, "min_n", "1")
        set_setting(conn, "rolling_window_days", "14")

        site_id = _make_site(conn, "e7-site")
        feed1, feed2 = _open_meteo_feed_ids(conn, 2)
        _seed_pair(
            conn,
            site_id=site_id,
            feed_id=feed1,
            variable="temperature",
            day_ahead=1,
            valid_at="2035-06-15T01:00:00Z",
        )
        await get_db().write(lambda c: _score_all_windows(c, None))  # baseline gen

        db = get_db()
        entered = threading.Event()
        release = threading.Event()

        def _inline_write(c: sqlite3.Connection) -> None:
            _seed_pair(
                c,
                site_id=site_id,
                feed_id=feed2,
                variable="temperature",
                day_ahead=1,
                valid_at="2035-06-15T02:00:00Z",
            )
            entered.set()
            if not release.wait(timeout=5.0):
                raise TimeoutError("release was never set")

        hold_task = asyncio.create_task(db.write(_inline_write))
        entered_ok = await asyncio.to_thread(entered.wait, 5.0)
        assert entered_ok  # the inline write is genuinely mid-transaction

        run_task = asyncio.create_task(run_batched_scoring(db, site_id))
        await asyncio.sleep(0.05)
        assert not run_task.done()  # discovery is genuinely blocked on the lock

        release.set()
        await hold_task
        await run_task

        rows = conn.execute(
            "SELECT feed_id, window_key FROM score_cache WHERE site_id=?", (site_id,)
        ).fetchall()
        feed_windows = {(r["feed_id"], r["window_key"]) for r in rows}
        # Both the pre-existing and the inline-created cell survive in both
        # windows: the sweep did not delete the inline run's rows.
        assert (feed1, "w:14") in feed_windows
        assert (feed1, "w:all") in feed_windows
        assert (feed2, "w:14") in feed_windows
        assert (feed2, "w:all") in feed_windows

    asyncio.run(_run())


# --------------------------------------------------------------------------
# E8: catchup routes through the batched lane
# --------------------------------------------------------------------------


def test_catchup_routes_scoring_through_batched_lane_e8(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        site_a = _make_site(conn, "e8-site-a")
        site_b = _make_site(conn, "e8-site-b")

        monkeypatch.setattr("wxverify.worker.catchup.scheduler_tick", lambda c: None)

        async def _fake_catchup_site(*args: Any, **kwargs: Any) -> bool:
            return True

        monkeypatch.setattr("wxverify.worker.catchup._catchup_site", _fake_catchup_site)

        calls: list[int] = []

        async def _spy_run_batched_scoring(inner_db: Any, sid: int) -> None:
            calls.append(sid)

        monkeypatch.setattr(
            "wxverify.worker.catchup.run_batched_scoring", _spy_run_batched_scoring
        )

        db = get_db()
        await run_catchup(db, {})

        assert sorted(calls) == sorted([site_a, site_b])

    asyncio.run(_run())


def test_catchup_continues_other_sites_after_one_cancelled_e8b(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        _freeze_now(monkeypatch)
        site_a = _make_site(conn, "e8b-site-a")
        site_b = _make_site(conn, "e8b-site-b")

        monkeypatch.setattr("wxverify.worker.catchup.scheduler_tick", lambda c: None)

        async def _fake_catchup_site(*args: Any, **kwargs: Any) -> bool:
            return True

        monkeypatch.setattr("wxverify.worker.catchup._catchup_site", _fake_catchup_site)

        calls: list[int] = []

        async def _spy_run_batched_scoring(inner_db: Any, sid: int) -> None:
            calls.append(sid)
            if sid == site_a:
                raise JobCancelled()

        monkeypatch.setattr(
            "wxverify.worker.catchup.run_batched_scoring", _spy_run_batched_scoring
        )

        db = get_db()
        await run_catchup(db, {})

        # Both sites were attempted despite site_a's cancellation.
        assert set(calls) == {site_a, site_b}

    asyncio.run(_run())
