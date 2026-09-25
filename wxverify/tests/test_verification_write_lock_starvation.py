"""Regression: a verification `simulate` chunk starves the current-obs lane.

``run_verification_chunk`` (worker/verification_run.py) runs
``simulate_snapshot_day`` for several days inside ONE ``writer.write``
transaction while holding ``Database._write_lock`` — the SAME lock every
current-obs write (worker/current_obs_poller.py's claim,
worker/current_obs.py's ``persist_poll_result``) must also take. In
production a chunk has held the lock for up to 1732 s (observed:
``lock_wait=1731986 ms`` against a ``hold=1732069 ms`` write), during which
every current-observation write for every station queues behind it.

The oracle here samples INSIDE the window: it blocks a simulate chunk
mid-transaction with a controlled fake, then proves a real current-obs
write cannot complete until the chunk releases. A wall-clock timing
assertion taken after the chunk ends would miss this entirely.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path

import pytest

from tests.test_verification_run import _make_verification_site
from wxverify import config
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.worker import verification_run as verification_run_module
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


def _seed_station(conn: sqlite3.Connection, site_id: int) -> int:
    conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
        " VALUES (?, 'ISTARVE01', 40.0, -105.0, 900.0, 1)",
        (site_id,),
    )
    row = conn.execute("SELECT last_insert_rowid()").fetchone()
    conn.commit()
    return int(row[0])


class CurrentObsWriteStarved(Exception):
    """Raised only when a confirmed-in-flight chunk starved a real write.

    Never raised by a gate-entry failure, the blocking stub's own internal
    timeout, or an exception surfaced from the chunk task itself — those
    stay ordinary test failures so a broken fixture cannot be mistaken for
    the bug this test pins.
    """


@pytest.mark.xfail(
    strict=True,
    raises=CurrentObsWriteStarved,
    reason="verification simulate chunk holds the write lock; fix pending on"
    " this branch",
)
def test_current_obs_write_completes_during_verification_simulate_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds = _make_verification_site(conn)
        station_id = _seed_station(conn, site_id)

        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload: dict[str, object] = {
            "trigger_date": "2026-06-06",
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

        def _blocking_simulate_snapshot_day(
            conn: sqlite3.Connection, cfg: object, day: str
        ) -> None:
            entered.set()
            if not release.wait(timeout=5.0):
                raise TimeoutError("release was never set")

        monkeypatch.setattr(
            verification_run_module,
            "simulate_snapshot_day",
            _blocking_simulate_snapshot_day,
        )

        chunk_task = asyncio.create_task(
            run_verification_chunk(db, writer, site_id, payload)
        )
        try:
            entered_ok = await asyncio.to_thread(entered.wait, 5.0)
            assert entered_ok  # the simulate chunk is genuinely mid-transaction

            outcome = PollOutcome(health=Health.OFFLINE, error="no data")
            # The oracle: this write must complete WHILE the chunk above is
            # still holding the write lock, not after it releases. Only a
            # timeout reached AFTER `entered` is confirmed counts as the
            # starvation bug -- everything else propagates as-is.
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

            # Success path (post-fix): the write must have actually landed,
            # and the chunk must still be genuinely in flight -- not merely
            # racing past a gate that never blocked anything.
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
        finally:
            release.set()
            await chunk_task

    asyncio.run(_run())
