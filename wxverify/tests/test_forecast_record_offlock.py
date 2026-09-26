"""Behavior pins for the async `forecast_record` job path (write-lock-fix
plan §5.5: TR1-TR9).

Every test drives the record job through the real `processor.dispatch`,
never a stand-in for it. The build reads and ranks on a read snapshot
(`compute_forecast_record_readonly`); only its <= 24-row insert takes the
write lock (`persist_forecast_record`).
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import threading
from datetime import timedelta
from pathlib import Path

import pytest

import wxverify.verification.record as record_mod
from tests.helpers import assert_read_pool_at_rest
from tests.test_forecast_record import (
    _DAY,
    _insert_full_grid,
    _insert_temp_day,
    _make_feed,
    _make_site,
    _snapshot_t,
)
from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import (
    FencedWriter,
    StaleGenerationError,
    close_db,
    get_db,
    init_db,
)
from wxverify.db.queue import Job
from wxverify.db.tz_generations import (
    ensure_published_generation,
    published_pointer_key,
)
from wxverify.worker import processor
from wxverify.worker.control import JobCancelled, JobDeferred
from wxverify.worker.current_obs import Health, PollOutcome, persist_poll_result


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - test inspects the real writer connection


def _seed(conn: sqlite3.Connection) -> tuple[int, int]:
    """Common setup (§5.5): a site with a published generation, two feeds
    with a full sample grid for ``_DAY``, and a station."""
    site_id = _make_site(conn, "site-a")
    ensure_published_generation(conn, site_id)
    for model in ("model-a", "model-b"):
        feed_id = _make_feed(conn, model)
        _insert_full_grid(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            start_date=_DAY,
            issued_at="2035-06-15T06:00:00Z",
            fetched_at="2035-06-15T06:05:00Z",
        )
    cur = conn.execute(
        """
        INSERT INTO stations
            (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
        VALUES (?, 'SYNTH-RECORD-01', 0.0, 0.0, 0.0, 1)
        """,
        (site_id,),
    )
    assert cur.lastrowid is not None
    return site_id, int(cur.lastrowid)


def _job(site_id: int, job_id: int = 1, snapshot_local_date: str = "2035-06-15") -> Job:
    return Job(
        id=job_id,
        type="forecast_record",
        site_id=site_id,
        job_key=f"record:{snapshot_local_date}",
        payload={"snapshot_local_date": snapshot_local_date},
        status="running",
        retry_count=0,
        max_retries=3,
    )


def _row_count(conn: sqlite3.Connection, site_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_of_record WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    return int(row["n"])


def _dump(conn: sqlite3.Connection, site_id: int) -> list[tuple[object, ...]]:
    rows = conn.execute(
        "SELECT * FROM forecast_of_record WHERE site_id = ? ORDER BY id",
        (site_id,),
    ).fetchall()
    return [
        tuple(v for k, v in dict(r).items() if k not in ("id", "created_at"))
        for r in rows
    ]


def _make_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[threading.Event, threading.Event]:
    """A DELEGATING gate on ``compute_forecast_record``: it calls the real
    function FIRST, then blocks. ``compute_forecast_record_readonly``
    resolves the name through the module globals, so the patch reaches the
    job path. Gated after delegating: the build already holds its rows
    when the test's interleaving lands (§5.5, "The order matters")."""
    entered = threading.Event()
    release = threading.Event()
    real_compute = record_mod.compute_forecast_record

    def _gated(
        conn: sqlite3.Connection,
        site_id: int,
        snapshot_local_date: str,
        *,
        now: object = None,
        resolve_generation: object,
    ) -> object:
        result = real_compute(
            conn,
            site_id,
            snapshot_local_date,
            now=now,  # type: ignore[arg-type]
            resolve_generation=resolve_generation,  # type: ignore[arg-type]
        )
        entered.set()
        if not release.wait(timeout=5.0):
            raise TimeoutError("release was never set")
        return result

    monkeypatch.setattr(record_mod, "compute_forecast_record", _gated)
    return entered, release


# ---------------------------------------------------------------------------
# TR1 -- a current-obs write completes during the record build.
# ---------------------------------------------------------------------------


def test_current_obs_write_completes_during_record_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, station_id = _seed(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        t = _snapshot_t()
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=5))
        entered, release = _make_gate(monkeypatch)

        dispatch_task = asyncio.create_task(
            processor.dispatch(db, writer, _job(site_id))
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)

            outcome = PollOutcome(health=Health.OFFLINE, error="no data")
            try:
                await asyncio.wait_for(
                    db.write(
                        lambda c: persist_poll_result(c, site_id, station_id, outcome)
                    ),
                    timeout=2.0,
                )
            except TimeoutError as exc:
                raise AssertionError(
                    "current-obs write did not complete within 2s while the"
                    " forecast_record build held the write lock"
                ) from exc

            assert not release.is_set()
            assert not dispatch_task.done()
            row = conn.execute(
                "SELECT health_state, last_error FROM station_poll_state"
                " WHERE station_id = ?",
                (station_id,),
            ).fetchone()
            assert row is not None
            assert row["health_state"] == "offline"
            assert row["last_error"] == "no data"
            assert _row_count(conn, site_id) == 0
        finally:
            release.set()
            result = await dispatch_task

        assert result is None
        rows = conn.execute(
            "SELECT * FROM forecast_of_record WHERE site_id = ? ORDER BY id",
            (site_id,),
        ).fetchall()
        assert len(rows) == 24
        assert all(str(r["status"]) == "recorded" for r in rows)
        assert all(str(r["write_path"]) == "on_time" for r in rows)
        assert all(int(r["write_latency_seconds"]) == 300 for r in rows)
        assert all(str(r["snapshot_utc"]) == isoformat_utc(t) for r in rows)
        emitted = [(str(r["variable"]), int(r["display_lead"])) for r in rows]
        assert emitted == [
            (v, d) for d in range(8) for v in ("temperature", "wind", "precip")
        ]

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TR2 -- read-only, snapshot-pinned, reader-only build.
# ---------------------------------------------------------------------------


def test_record_build_is_readonly_snapshot_pinned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _station_id = _seed(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        t = _snapshot_t()
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=5))

        real_compute = record_mod.compute_forecast_record
        records: list[dict[str, object]] = []

        def _delegating(
            conn: sqlite3.Connection,
            site_id: int,
            snapshot_local_date: str,
            *,
            now: object = None,
            resolve_generation: object,
        ) -> object:
            query_only = conn.execute("PRAGMA query_only").fetchone()[0]
            record: dict[str, object] = {
                "query_only": query_only,
                "in_transaction": conn.in_transaction,
                "is_writer_conn": conn is db._conn,  # noqa: SLF001
            }
            try:
                conn.execute("CREATE TABLE main.qo_probe(x INTEGER)")
            except sqlite3.OperationalError as exc:
                record["readonly_error"] = str(exc)
            else:
                record["readonly_error"] = None
            records.append(record)
            return real_compute(
                conn,
                site_id,
                snapshot_local_date,
                now=now,  # type: ignore[arg-type]
                resolve_generation=resolve_generation,  # type: ignore[arg-type]
            )

        monkeypatch.setattr(record_mod, "compute_forecast_record", _delegating)

        result = await processor.dispatch(db, writer, _job(site_id))
        assert result is None

        assert records, "the delegating fake was never called"
        for record in records:
            assert record["query_only"] == 1
            assert record["in_transaction"] is True
            assert record["is_writer_conn"] is False
            assert record["readonly_error"] is not None
            assert "readonly" in str(record["readonly_error"])

        for reader in db._read_conns:  # noqa: SLF001
            assert reader.execute("PRAGMA query_only").fetchone()[0] == 0
        assert_read_pool_at_rest(db)

    asyncio.run(_run())


def test_record_build_readonly_variant_raises_and_resets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _station_id = _seed(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        t = _snapshot_t()
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=5))

        def _boom(
            conn: sqlite3.Connection,
            site_id: int,
            snapshot_local_date: str,
            *,
            now: object = None,
            resolve_generation: object,
        ) -> object:
            raise ValueError("injected build failure")

        monkeypatch.setattr(record_mod, "compute_forecast_record", _boom)

        with pytest.raises(ValueError, match="injected build failure"):
            await processor.dispatch(db, writer, _job(site_id))

        assert _row_count(conn, site_id) == 0
        for reader in db._read_conns:  # noqa: SLF001
            assert reader.execute("PRAGMA query_only").fetchone()[0] == 0
        assert_read_pool_at_rest(db)

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TR3 -- differential against the synchronous build.
# ---------------------------------------------------------------------------


def test_record_differential_sync_vs_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        t = _snapshot_t()

        conn_a = _init_tmp_db(tmp_path / "a")
        site_id_a, _station_a = _seed(conn_a)
        db_a = get_db()
        await db_a.write(
            lambda c: record_mod.build_forecast_record(
                c, site_id_a, _DAY.isoformat(), now=t + timedelta(minutes=5)
            )
        )
        rows_a = _dump(conn_a, site_id_a)
        assert len(rows_a) == 24

        conn_b = _init_tmp_db(tmp_path / "b")
        site_id_b, _station_b = _seed(conn_b)
        assert site_id_b == site_id_a  # identical fresh-db seeding
        db_b = get_db()
        writer_b = FencedWriter(db_b, db_b.generation)
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=5))
        result = await processor.dispatch(db_b, writer_b, _job(site_id_b))
        assert result is None
        rows_b = _dump(conn_b, site_id_b)

        assert rows_a == rows_b

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TR4 -- site disabled or deleted during the build.
# ---------------------------------------------------------------------------


def test_record_build_site_disabled_mid_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _station_id = _seed(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        t = _snapshot_t()
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=5))
        entered, release = _make_gate(monkeypatch)

        dispatch_task = asyncio.create_task(
            processor.dispatch(db, writer, _job(site_id))
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)
            await db.write(
                lambda c: c.execute(
                    "UPDATE sites SET enabled = 0 WHERE id = ?", (site_id,)
                )
            )
        finally:
            release.set()
            with pytest.raises(JobCancelled):
                await dispatch_task

        assert _row_count(conn, site_id) == 0

    asyncio.run(_run())


def test_record_build_site_deleted_mid_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _station_id = _seed(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        t = _snapshot_t()
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=5))
        entered, release = _make_gate(monkeypatch)

        dispatch_task = asyncio.create_task(
            processor.dispatch(db, writer, _job(site_id))
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)
            await db.write(
                lambda c: c.execute("DELETE FROM sites WHERE id = ?", (site_id,))
            )
        finally:
            release.set()
            with pytest.raises(JobCancelled):
                await dispatch_task

        assert _row_count(conn, site_id) == 0

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TR5 -- no published generation.
# ---------------------------------------------------------------------------


def test_record_build_cancelled_without_published_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _station_id = _seed(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        t = _snapshot_t()
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=5))

        gen_count_before = conn.execute(
            "SELECT COUNT(*) FROM timezone_generations"
        ).fetchone()[0]
        conn.execute(
            "DELETE FROM runtime_state WHERE key = ?",
            (published_pointer_key(site_id),),
        )

        caplog.set_level(logging.WARNING, logger="wxverify.verification.record")
        with pytest.raises(JobCancelled):
            await processor.dispatch(db, writer, _job(site_id))

        expected_message = (
            f"forecast record cancelled site={site_id} date=2035-06-15:"
            " no published timezone generation"
        )
        record_warnings = [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.WARNING and r.name == "wxverify.verification.record"
        ]
        assert record_warnings == [expected_message]
        assert _row_count(conn, site_id) == 0
        gen_count_after = conn.execute(
            "SELECT COUNT(*) FROM timezone_generations"
        ).fetchone()[0]
        assert gen_count_after == gen_count_before
        pointer_row = conn.execute(
            "SELECT value FROM runtime_state WHERE key = ?",
            (published_pointer_key(site_id),),
        ).fetchone()
        assert pointer_row is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TR6 -- database replacement during the build.
# ---------------------------------------------------------------------------


def test_record_build_database_replacement_raises_stale_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _station_id = _seed(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        t = _snapshot_t()
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=5))

        backup_src = tmp_path / "pre.db"

        def _backup(c: sqlite3.Connection) -> None:
            dest = sqlite3.connect(str(backup_src))
            try:
                c.backup(dest)
            finally:
                dest.close()

        await db.read(_backup)

        entered, release = _make_gate(monkeypatch)

        dispatch_task = asyncio.create_task(
            processor.dispatch(db, writer, _job(site_id))
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)
            unused_backup = tmp_path / "unused-backup.db"
            replace_task = asyncio.create_task(
                db.replace_from(backup_src, unused_backup)
            )
            # replace_from takes the write lock uncontended (nothing else
            # holds it during a read), clears the read gate, then drains
            # every pooled read connection -- and blocks on the one
            # connection the still-gated compute above has checked out.
            # Poll on the pool actually reaching that drained state instead
            # of guessing a fixed sleep long enough for the drain to run.
            deadline = asyncio.get_event_loop().time() + 5.0
            while db._read_gate.is_set() or db._read_pool.qsize() > 0:  # noqa: SLF001
                if asyncio.get_event_loop().time() > deadline:
                    pytest.fail("replace_from never reached its blocked-on-drain state")
                await asyncio.sleep(0)
            assert not db._read_gate.is_set()  # noqa: SLF001
            assert db._write_lock.locked()  # noqa: SLF001
            assert not replace_task.done()
        finally:
            release.set()
            with pytest.raises(StaleGenerationError):
                await dispatch_task
            await replace_task

        assert await db.read(lambda c: _row_count(c, site_id)) == 0

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TR7 -- cancellation.
# ---------------------------------------------------------------------------


def test_record_build_cancel_during_compute_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _station_id = _seed(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        t = _snapshot_t()
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=5))
        entered, release = _make_gate(monkeypatch)

        dispatch_task = asyncio.create_task(
            processor.dispatch(db, writer, _job(site_id))
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)
            dispatch_task.cancel()
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await dispatch_task

        assert _row_count(conn, site_id) == 0
        for reader in db._read_conns:  # noqa: SLF001
            assert reader.execute("PRAGMA query_only").fetchone()[0] == 0
        assert_read_pool_at_rest(db)

    asyncio.run(_run())


def test_record_build_cancel_during_persist_lets_commit_land(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _station_id = _seed(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        t = _snapshot_t()
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=5))

        entered = threading.Event()
        release = threading.Event()
        real_persist = processor.persist_forecast_record

        def _gated_persist(conn: sqlite3.Connection, build: object) -> None:
            entered.set()
            if not release.wait(timeout=5.0):
                raise TimeoutError("release was never set")
            return real_persist(conn, build)  # type: ignore[arg-type]

        monkeypatch.setattr(processor, "persist_forecast_record", _gated_persist)

        dispatch_task = asyncio.create_task(
            processor.dispatch(db, writer, _job(site_id))
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)
            dispatch_task.cancel()
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await dispatch_task

        assert _row_count(conn, site_id) == 24

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TR8 -- outside the window.
# ---------------------------------------------------------------------------


def test_record_build_outside_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _station_id = _seed(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        t = _snapshot_t()

        monkeypatch.setattr(record_mod, "utc_now", lambda: t - timedelta(minutes=5))
        with pytest.raises(JobDeferred) as excinfo:
            await processor.dispatch(db, writer, _job(site_id, job_id=1))
        assert excinfo.value.next_attempt_at == isoformat_utc(t)
        assert _row_count(conn, site_id) == 0

        gen_count_before = conn.execute(
            "SELECT COUNT(*) FROM timezone_generations"
        ).fetchone()[0]
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(hours=25))
        with pytest.raises(JobCancelled):
            await processor.dispatch(db, writer, _job(site_id, job_id=2))
        assert _row_count(conn, site_id) == 0
        gen_count_after = conn.execute(
            "SELECT COUNT(*) FROM timezone_generations"
        ).fetchone()[0]
        assert gen_count_after == gen_count_before

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# TR9 -- a partial day fills in, first write wins.
# ---------------------------------------------------------------------------


def test_record_build_partial_day_fills_in_first_write_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id = _make_site(conn, "site-a")
        ensure_published_generation(conn, site_id)
        feed_id = _make_feed(conn, "model-a")
        cur = conn.execute(
            """
            INSERT INTO stations
                (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
            VALUES (?, 'SYNTH-RECORD-01', 0.0, 0.0, 0.0, 1)
            """,
            (site_id,),
        )
        assert cur.lastrowid is not None
        _insert_temp_day(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            local_date=_DAY,
            issued_at="2035-06-15T06:00:00Z",
            fetched_at="2035-06-15T06:05:00Z",
            value=10.0,
        )

        db = get_db()
        writer = FencedWriter(db, db.generation)
        t = _snapshot_t()
        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=5))
        result = await processor.dispatch(db, writer, _job(site_id, job_id=1))
        assert result is None

        rows = conn.execute(
            "SELECT id, variable, display_lead, write_path FROM forecast_of_record"
            " WHERE site_id = ? ORDER BY id",
            (site_id,),
        ).fetchall()
        assert [(str(r["variable"]), int(r["display_lead"])) for r in rows] == [
            ("temperature", 0)
        ]
        assert str(rows[0]["write_path"]) == "on_time"
        temp_id = int(rows[0]["id"])

        _insert_temp_day(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            local_date=_DAY,
            issued_at="2035-06-15T06:00:00Z",
            fetched_at="2035-06-15T06:05:00Z",
            value=5.0,
            variable="wind",
        )
        _insert_temp_day(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            local_date=_DAY,
            issued_at="2035-06-15T06:00:00Z",
            fetched_at="2035-06-15T06:05:00Z",
            value=0.0,
            variable="precip",
        )
        conn.execute(
            "UPDATE forecast_samples SET value = 99.0 WHERE variable = 'temperature'"
        )

        monkeypatch.setattr(record_mod, "utc_now", lambda: t + timedelta(minutes=65))
        result2 = await processor.dispatch(db, writer, _job(site_id, job_id=2))
        assert result2 is None

        rows2 = conn.execute(
            "SELECT id, variable, display_lead, write_path, hourly_values"
            " FROM forecast_of_record WHERE site_id = ? ORDER BY id",
            (site_id,),
        ).fetchall()
        assert len(rows2) == 3
        temp_row = next(r for r in rows2 if str(r["variable"]) == "temperature")
        assert int(temp_row["id"]) == temp_id
        assert str(temp_row["write_path"]) == "on_time"
        hourly = json.loads(str(temp_row["hourly_values"]))
        assert len(hourly) == 24
        assert all(v == 10.0 for _, v in hourly)
        for variable in ("wind", "precip"):
            row = next(r for r in rows2 if str(r["variable"]) == variable)
            assert int(row["display_lead"]) == 0
            assert str(row["write_path"]) == "late_reconstruction"

    asyncio.run(_run())
