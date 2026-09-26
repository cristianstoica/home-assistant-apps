"""A verification `simulate` day computes on a read snapshot, never under
`Database._write_lock`; a current-obs write must complete while that compute
is in flight.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import date, timedelta
from pathlib import Path

import pytest

from tests.helpers import build_synthetic_verification_site
from wxverify import config
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.verification import simulate as simulate_module
from wxverify.verification.simulate import DayEvidence
from wxverify.worker import verification_run as verification_run_module
from wxverify.worker.control import JobContinuation
from wxverify.worker.current_obs import Health, PollOutcome, persist_poll_result
from wxverify.worker.verification_run import (
    _load_state,  # noqa: SLF001
    advance_verification,
    run_verification_chunk,
)


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - test inspects the real writer connection


class CurrentObsWriteStarved(Exception):
    """Raised only when a confirmed-in-flight chunk starved a real write.

    Never raised by a gate-entry failure, the blocking stub's own internal
    timeout, or an exception surfaced from the chunk task itself — those
    stay ordinary test failures so a broken fixture cannot be mistaken for
    the bug this test pins.
    """


def test_current_obs_write_completes_during_verification_simulate_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, station_id = build_synthetic_verification_site(conn)

        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload: dict[str, object] = {
            "trigger_date": "2026-01-25",
            "snapshot_days_per_chunk": 2,
        }

        # Drive the chain to (not through) the `simulate` phase.
        for _ in range(20):
            blob = await db.read(lambda c: _load_state(c, site_id))
            if blob is not None and blob.get("phase") == "simulate":
                break
            await writer.write(lambda c: advance_verification(c, site_id, payload))
        else:
            raise AssertionError("chain never reached the simulate phase")

        entered = threading.Event()
        release = threading.Event()

        def _blocking_compute_snapshot_day(
            conn: sqlite3.Connection, cfg: object, day: str
        ) -> DayEvidence:
            entered.set()
            if not release.wait(timeout=5.0):
                raise TimeoutError("release was never set")
            return DayEvidence((), None)

        for binding in (verification_run_module, simulate_module):
            monkeypatch.setattr(
                binding, "compute_snapshot_day", _blocking_compute_snapshot_day
            )

        chunk_task = asyncio.create_task(
            run_verification_chunk(db, writer, site_id, payload)
        )
        try:
            entered_ok = await asyncio.to_thread(entered.wait, 5.0)
            assert entered_ok  # the simulate day is genuinely mid-compute

            outcome = PollOutcome(health=Health.OFFLINE, error="no data")
            # The oracle: this write must complete WHILE the day above is
            # still computing, not after it releases. Only a timeout reached
            # AFTER `entered` is confirmed counts as the starvation bug --
            # everything else propagates as-is.
            try:
                await asyncio.wait_for(
                    db.write(
                        lambda c: persist_poll_result(c, site_id, station_id, outcome)
                    ),
                    timeout=2.0,
                )
            except TimeoutError as exc:
                raise CurrentObsWriteStarved(
                    "current-obs write did not complete within 2s while the "
                    "verification simulate chunk held the write lock"
                ) from exc

            # Success path: the write must have actually landed, and the
            # chunk must still be genuinely in flight -- not merely racing
            # past a gate that never blocked anything.
            assert not release.is_set()
            assert not chunk_task.done()
            row = conn.execute(
                "SELECT health_state, last_error FROM station_poll_state"
                " WHERE station_id = ?",
                (station_id,),
            ).fetchone()
            assert row is not None
            assert row["health_state"] == "offline"
            assert row["last_error"] == "no data"
            # Nothing of the gated day has committed yet.
            gated = await db.read(lambda c: _load_state(c, site_id))
            assert gated is not None
            assert gated.get("cursor") == "2026-01-01"
        finally:
            release.set()
            result = await chunk_task

        assert isinstance(result, JobContinuation)
        after = await db.read(lambda c: _load_state(c, site_id))
        assert after is not None
        assert after.get("cursor") == "2026-01-03"

    asyncio.run(_run())


def test_current_obs_write_completes_during_verification_baseline_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The `baseline` counterpart of the `simulate` starvation test above."""

    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, station_id = build_synthetic_verification_site(conn)

        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload: dict[str, object] = {
            "trigger_date": "2026-01-25",
            "snapshot_days_per_chunk": 2,
        }

        # Drive the chain to (not through) the `simulate` phase.
        for _ in range(20):
            blob = await db.read(lambda c: _load_state(c, site_id))
            if blob is not None and blob.get("phase") == "simulate":
                break
            await writer.write(lambda c: advance_verification(c, site_id, payload))
        else:
            raise AssertionError("chain never reached the simulate phase")

        # Run every `simulate` day through the SYNC reference path (this is
        # setup, not the behavior under test) until the chain reaches
        # `baseline`.
        for _ in range(20):
            blob = await db.read(lambda c: _load_state(c, site_id))
            if blob is not None and blob.get("phase") == "baseline":
                break
            await writer.write(lambda c: advance_verification(c, site_id, payload))
        else:
            raise AssertionError("chain never reached the baseline phase")

        period_start = await db.read(lambda c: _load_state(c, site_id))
        assert period_start is not None
        start_cursor = str(period_start["cursor"])

        entered = threading.Event()
        release = threading.Event()

        def _blocking_compute_baseline_day(
            conn: sqlite3.Connection, cfg: object, day: str, rosters: object
        ) -> DayEvidence:
            entered.set()
            if not release.wait(timeout=5.0):
                raise TimeoutError("release was never set")
            return DayEvidence((), None)

        for binding in (verification_run_module, simulate_module):
            monkeypatch.setattr(
                binding, "compute_baseline_day", _blocking_compute_baseline_day
            )

        chunk_task = asyncio.create_task(
            run_verification_chunk(db, writer, site_id, payload)
        )
        try:
            entered_ok = await asyncio.to_thread(entered.wait, 5.0)
            assert entered_ok  # the baseline day is genuinely mid-compute

            outcome = PollOutcome(health=Health.OFFLINE, error="no data")
            try:
                await asyncio.wait_for(
                    db.write(
                        lambda c: persist_poll_result(c, site_id, station_id, outcome)
                    ),
                    timeout=2.0,
                )
            except TimeoutError as exc:
                raise CurrentObsWriteStarved(
                    "current-obs write did not complete within 2s while the "
                    "verification baseline chunk held the write lock"
                ) from exc

            assert not release.is_set()
            assert not chunk_task.done()
            row = conn.execute(
                "SELECT health_state, last_error FROM station_poll_state"
                " WHERE station_id = ?",
                (station_id,),
            ).fetchone()
            assert row is not None
            assert row["health_state"] == "offline"
            assert row["last_error"] == "no data"
            gated = await db.read(lambda c: _load_state(c, site_id))
            assert gated is not None
            assert gated.get("cursor") == start_cursor
        finally:
            release.set()
            result = await chunk_task

        assert isinstance(result, JobContinuation)
        after = await db.read(lambda c: _load_state(c, site_id))
        assert after is not None
        start_date = date.fromisoformat(start_cursor)
        assert after.get("cursor") == (start_date + timedelta(days=2)).isoformat()

    asyncio.run(_run())
