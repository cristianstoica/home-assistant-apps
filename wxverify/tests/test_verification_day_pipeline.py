"""Behavior pins for the async `simulate`-day pipeline (write-lock-fix
plan §5.5: T3, T3b, T3c, T6, T7a-T7f, T8, T9, T10, T12).

Every test drives the chain to (not through) the `simulate` phase with the
existing sync-path loop, then exercises the real async `_run_day_chunk`
path -- never a stand-in for it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import threading
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pytest

from tests.helpers import (
    assert_read_pool_at_rest,
    build_synthetic_verification_site,
    evidence_digest,
)
from wxverify import config
from wxverify.db.connection import (
    Database,
    FencedWriter,
    StaleGenerationError,
    close_db,
    get_db,
    init_db,
)
from wxverify.db.queue import claim_next_job, enqueue_if_absent
from wxverify.db.runtime_state import get_runtime_state
from wxverify.settings.depth import depth_override_key
from wxverify.settings.keys import set_setting
from wxverify.verification import simulate as simulate_module
from wxverify.verification.record import SNAPSHOT_TIME_KEY
from wxverify.verification.runs import (
    assert_inputs_unchanged_readonly,
    assert_inputs_unpinned_unchanged,
    run_config_from_row,
)
from wxverify.verification.simulate import DayEvidence
from wxverify.worker import verification_run as verification_run_module
from wxverify.worker.control import JobCancelled
from wxverify.worker.processor import run_claimed_job
from wxverify.worker.verification_run import (
    _load_state,  # noqa: SLF001
    advance_verification,
    run_verification_chunk,
    verification_chain_active,
    verification_job_key,
)


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


async def _drive_to_simulate(
    db: Database, writer: FencedWriter, site_id: int, payload: dict[str, object]
) -> None:
    """Sync-path setup: reach (not enter) the `simulate` phase."""
    for _ in range(20):
        blob = await db.read(lambda c: _load_state(c, site_id))
        if blob is not None and blob.get("phase") == "simulate":
            return
        await writer.write(lambda c: advance_verification(c, site_id, payload))
    raise AssertionError("chain never reached the simulate phase")


def _payload(days_per_chunk: int = 7) -> dict[str, object]:
    return {
        "trigger_date": "2026-01-25",
        "resamples": 40,
        "snapshot_days_per_chunk": days_per_chunk,
    }


# ---------------------------------------------------------------------------
# T3 -- read-only, snapshot-pinned, reader-only compute.
# ---------------------------------------------------------------------------


def test_simulate_day_computes_readonly_on_a_pooled_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=1)
        await _drive_to_simulate(db, writer, site_id, payload)

        real_compute = simulate_module.compute_snapshot_day
        records: list[dict[str, object]] = []

        def _delegating(conn: sqlite3.Connection, cfg: object, day: str) -> DayEvidence:
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
            return real_compute(conn, cfg, day)

        for binding in (verification_run_module, simulate_module):
            monkeypatch.setattr(binding, "compute_snapshot_day", _delegating)

        result = await run_verification_chunk(db, writer, site_id, payload)
        assert result is not None

        assert records, "the delegating fake was never called"
        for record in records:
            assert record["query_only"] == 1
            # `read_snapshot` pins the day's compute inside a transaction so
            # the whole day reads one consistent view -- this is expected,
            # not a leak of the writer's transaction.
            assert record["in_transaction"] is True
            assert record["is_writer_conn"] is False
            assert record["readonly_error"] is not None
            assert "readonly" in record["readonly_error"]

        for reader in db._read_conns:  # noqa: SLF001
            assert reader.execute("PRAGMA query_only").fetchone()[0] == 0
        assert_read_pool_at_rest(db)

    asyncio.run(_run())


def test_simulate_day_compute_failure_still_resets_pragma_and_pool(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=1)
        await _drive_to_simulate(db, writer, site_id, payload)

        def _boom(conn: sqlite3.Connection, cfg: object, day: str) -> DayEvidence:
            raise ValueError("injected compute failure")

        for binding in (verification_run_module, simulate_module):
            monkeypatch.setattr(binding, "compute_snapshot_day", _boom)

        with pytest.raises(ValueError, match="injected compute failure"):
            await run_verification_chunk(db, writer, site_id, payload)

        for reader in db._read_conns:  # noqa: SLF001
            assert reader.execute("PRAGMA query_only").fetchone()[0] == 0
        assert_read_pool_at_rest(db)

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T3b -- one snapshot per day.
# ---------------------------------------------------------------------------


def test_simulate_day_snapshot_does_not_see_a_commit_mid_compute(
    tmp_path: Path,
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, feeds, _station = build_synthetic_verification_site(conn)
        feed_a = feeds[0]
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        blob = await db.read(lambda c: _load_state(c, site_id))
        assert blob is not None
        run_id = int(blob["run_id"])  # type: ignore[arg-type]

        real_fn = simulate_module.count_null_availability_samples
        call_count = {"n": 0}
        entered = threading.Event()
        release = threading.Event()

        def _wrapper(*args: object, **kwargs: object) -> object:
            call_count["n"] += 1
            if call_count["n"] == 1:
                entered.set()
                if not release.wait(timeout=5.0):
                    raise TimeoutError("release was never set")
            return real_fn(*args, **kwargs)

        simulate_module.count_null_availability_samples = _wrapper  # type: ignore[assignment]
        try:
            chunk_task = asyncio.create_task(
                run_verification_chunk(db, writer, site_id, payload)
            )
            try:
                assert await asyncio.to_thread(entered.wait, 5.0)
                await db.write(
                    lambda c: c.execute(
                        "UPDATE daily_truth SET value = 115.0"
                        " WHERE local_date = '2026-01-02'"
                        " AND quantity = 'temperature_high'"
                    )
                )
            finally:
                release.set()
                await chunk_task
        finally:
            simulate_module.count_null_availability_samples = real_fn  # type: ignore[assignment]

        def _truth_value(
            c: sqlite3.Connection, snapshot: str, target: str
        ) -> float | None:
            row = c.execute(
                "SELECT truth_value FROM verification_evidence"
                " WHERE run_id = ? AND snapshot_local_date = ?"
                " AND target_local_date = ? AND quantity = 'temperature_high'"
                " AND entity_type = 'feed' AND entity_key = ?",
                (run_id, snapshot, target, str(feed_a)),
            ).fetchone()
            assert row is not None
            return None if row[0] is None else float(row[0])

        before = await db.read(lambda c: _truth_value(c, "2026-01-01", "2026-01-02"))
        after = await db.read(lambda c: _truth_value(c, "2026-01-02", "2026-01-02"))
        assert before == 15.0
        assert after == 115.0

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T3c -- result-affecting pragma and plan parity between writer and reader.
# ---------------------------------------------------------------------------

_PARITY_PRAGMAS = (
    "automatic_index",
    "cache_size",
    "foreign_keys",
    "recursive_triggers",
    "reverse_unordered_selects",
    "temp_store",
    "trusted_schema",
)


def test_writer_and_reader_share_result_affecting_pragmas_and_query_plans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=1)
        await _drive_to_simulate(db, writer, site_id, payload)

        for pragma in _PARITY_PRAGMAS:
            writer_value = conn.execute(f"PRAGMA {pragma}").fetchone()[0]
            reader = db._read_conns[0]  # noqa: SLF001
            reader_value = reader.execute(f"PRAGMA {pragma}").fetchone()[0]
            assert writer_value == reader_value, pragma

        real_compute = simulate_module.compute_snapshot_day
        recorded: list[str] = []

        def _delegating(conn: sqlite3.Connection, cfg: object, day: str) -> DayEvidence:
            def _tracer(statement: str) -> None:
                recorded.append(statement)

            conn.set_trace_callback(_tracer)
            try:
                return real_compute(conn, cfg, day)
            finally:
                conn.set_trace_callback(None)

        for binding in (verification_run_module, simulate_module):
            monkeypatch.setattr(binding, "compute_snapshot_day", _delegating)

        result = await run_verification_chunk(db, writer, site_id, payload)
        assert result is not None

        selects = [
            s for s in recorded if s.lstrip().upper().startswith(("SELECT", "WITH"))
        ]
        assert selects, "no SELECT/WITH statement was recorded"
        for statement in selects:
            assert "?" not in statement, statement

        for statement in set(selects):
            writer_plan = [
                tuple(r) for r in conn.execute(f"EXPLAIN QUERY PLAN {statement}")
            ]
            reader = db._read_conns[0]  # noqa: SLF001
            reader_plan = [
                tuple(r) for r in reader.execute(f"EXPLAIN QUERY PLAN {statement}")
            ]
            assert writer_plan == reader_plan, statement

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T6 -- per-day atomicity.
# ---------------------------------------------------------------------------


def test_persist_day_failure_leaves_no_partial_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=7)
        await _drive_to_simulate(db, writer, site_id, payload)

        blob = await db.read(lambda c: _load_state(c, site_id))
        assert blob is not None
        run_id = int(blob["run_id"])  # type: ignore[arg-type]

        real_save_state = verification_run_module._save_state  # noqa: SLF001

        def _wrapped_save_state(
            c: sqlite3.Connection, sid: int, state: dict[str, object]
        ) -> None:
            if state.get("cursor") == "2026-01-04":
                raise RuntimeError("injected")
            return real_save_state(c, sid, state)

        monkeypatch.setattr(verification_run_module, "_save_state", _wrapped_save_state)

        with pytest.raises(RuntimeError, match="injected"):
            await run_verification_chunk(db, writer, site_id, payload)

        def _counts(c: sqlite3.Connection, day: str) -> tuple[int, int]:
            ev = c.execute(
                "SELECT COUNT(*) FROM verification_evidence"
                " WHERE run_id = ? AND snapshot_local_date = ?",
                (run_id, day),
            ).fetchone()[0]
            ctx = c.execute(
                "SELECT COUNT(*) FROM verification_day_context"
                " WHERE run_id = ? AND snapshot_local_date = ?",
                (run_id, day),
            ).fetchone()[0]
            return int(ev), int(ctx)

        day3 = await db.read(lambda c: _counts(c, "2026-01-03"))
        day2 = await db.read(lambda c: _counts(c, "2026-01-02"))
        assert day3 == (0, 0)
        assert day2[0] > 0
        assert day2[1] == 1

        after = await db.read(lambda c: _load_state(c, site_id))
        assert after is not None
        assert after.get("cursor") == "2026-01-03"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T7a -- compare-and-set.
# ---------------------------------------------------------------------------


def test_chain_state_change_during_compute_discards_the_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        entered = threading.Event()
        release = threading.Event()
        real_compute = simulate_module.compute_snapshot_day

        def _gated(conn: sqlite3.Connection, cfg: object, day: str) -> DayEvidence:
            entered.set()
            if not release.wait(timeout=5.0):
                raise TimeoutError("release was never set")
            return real_compute(conn, cfg, day)

        for binding in (verification_run_module, simulate_module):
            monkeypatch.setattr(binding, "compute_snapshot_day", _gated)

        caplog.set_level(logging.WARNING, logger="wxverify.worker.verification_run")
        chunk_task = asyncio.create_task(
            run_verification_chunk(db, writer, site_id, payload)
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)
            raw = await db.read(
                lambda c: get_runtime_state(
                    c, verification_run_module.verification_state_key(site_id)
                )
            )
            assert raw is not None
            same_content = json.dumps(json.loads(raw))  # default separators
            assert same_content != raw
            await db.write(
                lambda c: verification_run_module.set_runtime_state(
                    c,
                    verification_run_module.verification_state_key(site_id),
                    same_content,
                )
            )
        finally:
            release.set()
            with pytest.raises(JobCancelled):
                await chunk_task

        expected_message = (
            f"verification day discarded site={site_id} day=2026-01-01:"
            " chain state changed during compute"
        )
        assert any(
            r.levelno == logging.WARNING and r.getMessage() == expected_message
            for r in caplog.records
        )

        final_raw = await db.read(
            lambda c: get_runtime_state(
                c, verification_run_module.verification_state_key(site_id)
            )
        )
        assert final_raw == same_content

        def _counts(c: sqlite3.Connection) -> tuple[int, int]:
            ev = c.execute(
                "SELECT COUNT(*) FROM verification_evidence"
                " WHERE snapshot_local_date = '2026-01-01'"
            ).fetchone()[0]
            ctx = c.execute(
                "SELECT COUNT(*) FROM verification_day_context"
                " WHERE snapshot_local_date = '2026-01-01'"
            ).fetchone()[0]
            return int(ev), int(ctx)

        assert await db.read(_counts) == (0, 0)

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T7b -- input check per day.
# ---------------------------------------------------------------------------


def test_input_change_before_the_claim_fails_the_chunk(tmp_path: Path) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        cursor_before = await db.read(lambda c: _load_state(c, site_id))
        assert cursor_before is not None

        await db.write(lambda c: set_setting(c, "min_n", "31"))

        with pytest.raises(RuntimeError, match="min_n"):
            await run_verification_chunk(db, writer, site_id, payload)

        def _has_evidence(c: sqlite3.Connection) -> bool:
            row = c.execute(
                "SELECT COUNT(*) FROM verification_evidence"
                " WHERE snapshot_local_date = '2026-01-01'"
            ).fetchone()
            return int(row[0]) > 0

        def _has_day_context(c: sqlite3.Connection) -> bool:
            row = c.execute(
                "SELECT COUNT(*) FROM verification_day_context"
                " WHERE snapshot_local_date = '2026-01-01'"
            ).fetchone()
            return int(row[0]) > 0

        assert not await db.read(_has_evidence)
        assert not await db.read(_has_day_context)
        after = await db.read(lambda c: _load_state(c, site_id))
        assert after == cursor_before

    asyncio.run(_run())


def test_input_change_between_days_fails_only_the_later_day(tmp_path: Path) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        real_persist = verification_run_module.persist_day_evidence
        called = {"n": 0}

        def _wrapped_persist(c: sqlite3.Connection, evidence: DayEvidence) -> None:
            called["n"] += 1
            if called["n"] == 1:
                set_setting(c, "min_n", "31")
            return real_persist(c, evidence)

        pipeline_patch = pytest.MonkeyPatch()
        pipeline_patch.setattr(
            verification_run_module, "persist_day_evidence", _wrapped_persist
        )
        try:
            with pytest.raises(RuntimeError, match="min_n"):
                await run_verification_chunk(db, writer, site_id, payload)
        finally:
            pipeline_patch.undo()

        def _counts(c: sqlite3.Connection, day: str) -> int:
            return int(
                c.execute(
                    "SELECT COUNT(*) FROM verification_evidence"
                    " WHERE snapshot_local_date = ?",
                    (day,),
                ).fetchone()[0]
            )

        def _day_context_counts(c: sqlite3.Connection, day: str) -> int:
            return int(
                c.execute(
                    "SELECT COUNT(*) FROM verification_day_context"
                    " WHERE snapshot_local_date = ?",
                    (day,),
                ).fetchone()[0]
            )

        assert await db.read(lambda c: _counts(c, "2026-01-01")) > 0
        assert await db.read(lambda c: _counts(c, "2026-01-02")) == 0
        assert await db.read(lambda c: _day_context_counts(c, "2026-01-01")) > 0
        assert await db.read(lambda c: _day_context_counts(c, "2026-01-02")) == 0
        after = await db.read(lambda c: _load_state(c, site_id))
        assert after is not None
        assert after.get("cursor") == "2026-01-02"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T7c -- message parity, with literals.
# ---------------------------------------------------------------------------


def test_message_parity_single_key(tmp_path: Path) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        blob = await db.read(lambda c: _load_state(c, site_id))
        assert blob is not None
        run_id = int(blob["run_id"])  # type: ignore[arg-type]

        def _write_path_message(c: sqlite3.Connection) -> str:
            cfg = run_config_from_row(c, run_id)
            try:
                assert_inputs_unpinned_unchanged(c, cfg)
            except RuntimeError as exc:
                return str(exc)
            raise AssertionError("expected a RuntimeError")

        def _read_path_message(c: sqlite3.Connection) -> str:
            cfg = run_config_from_row(c, run_id)
            try:
                assert_inputs_unchanged_readonly(c, cfg)
            except RuntimeError as exc:
                return str(exc)
            raise AssertionError("expected a RuntimeError")

        await db.write(lambda c: set_setting(c, "min_n", "31"))

        expected = f"verification run {run_id} inputs changed mid-run: min_n"

        def _write_path_message_rolled_back(c: sqlite3.Connection) -> str:
            c.execute("SAVEPOINT probe")
            try:
                return _write_path_message(c)
            finally:
                c.execute("ROLLBACK TO probe")
                c.execute("RELEASE probe")

        wp = await db.write(_write_path_message_rolled_back)
        rp = await db.read(_read_path_message)
        assert wp == expected
        assert rp == expected

    asyncio.run(_run())


def test_message_parity_multi_key(tmp_path: Path) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        blob = await db.read(lambda c: _load_state(c, site_id))
        assert blob is not None
        run_id = int(blob["run_id"])  # type: ignore[arg-type]

        def _mutate(c: sqlite3.Connection) -> None:
            c.execute("UPDATE feeds SET enabled = 0 WHERE id = ?", (feeds[2],))
            set_setting(c, depth_override_key("temperature"), "3")
            set_setting(
                c,
                f"{SNAPSHOT_TIME_KEY}:{site_id}",
                "08:30",
            )
            set_setting(c, "min_n", "31")

        await db.write(_mutate)

        expected = (
            f"verification run {run_id} inputs changed mid-run: "
            "roster, blend_depths, wall_clock, blend_depth, min_n"
        )

        def _write_path_message(c: sqlite3.Connection) -> str:
            cfg = run_config_from_row(c, run_id)
            c.execute("SAVEPOINT probe")
            try:
                assert_inputs_unpinned_unchanged(c, cfg)
                raise AssertionError("expected a RuntimeError")
            except RuntimeError as exc:
                return str(exc)
            finally:
                c.execute("ROLLBACK TO probe")
                c.execute("RELEASE probe")

        def _read_path_message(c: sqlite3.Connection) -> str:
            cfg = run_config_from_row(c, run_id)
            try:
                assert_inputs_unchanged_readonly(c, cfg)
                raise AssertionError("expected a RuntimeError")
            except RuntimeError as exc:
                return str(exc)

        # `blend_depth` alone: the plan's expected message names both
        # `blend_depth` (the pinned scalar) and `blend_depths` (the pinned
        # per-variable resolution). The override on "temperature" plus the
        # global blend_depth change gives `blend_depths, blend_depth`. The
        # global depth is unset by the builder (default 2); set it too so
        # `blend_depth` itself differs from the pinned value.
        await db.write(lambda c: set_setting(c, "forecast_blend_depth", "3"))

        wp = await db.write(_write_path_message)
        rp = await db.read(_read_path_message)
        assert wp == expected
        assert rp == expected

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T7d -- per-key parity. One independent mutation per key (plus the two
# roster variants and the two wall-clock variants T7d calls out), each
# asserted against the full literal message on both functions.
# ---------------------------------------------------------------------------


def _mutate_roster_disabled(
    c: sqlite3.Connection, site_id: int, feeds: list[int]
) -> None:
    del site_id
    c.execute("UPDATE feeds SET enabled = 0 WHERE id = ?", (feeds[2],))


def _mutate_roster_max_lead_hours(
    c: sqlite3.Connection, site_id: int, feeds: list[int]
) -> None:
    del site_id
    c.execute("UPDATE feeds SET max_lead_hours = 200 WHERE id = ?", (feeds[2],))


def _mutate_blend_depth_and_depths(
    c: sqlite3.Connection, site_id: int, feeds: list[int]
) -> None:
    del site_id, feeds
    set_setting(c, "forecast_blend_depth", "3")


def _mutate_blend_depths_only(
    c: sqlite3.Connection, site_id: int, feeds: list[int]
) -> None:
    del site_id, feeds
    set_setting(c, depth_override_key("temperature"), "4")


def _setup_blend_depth_alone(
    c: sqlite3.Connection, site_id: int, feeds: list[int]
) -> None:
    """Pin every per-variable override at the current global default (2).

    A later change to `forecast_blend_depth` alone then moves `blend_depth`
    without moving `blend_depths`: every override still resolves to 2, so
    the pinned and current `blend_depths` maps stay equal.
    """
    del site_id, feeds
    for variable in ("temperature", "wind", "precip"):
        set_setting(c, depth_override_key(variable), "2")


def _mutate_blend_depth_only(
    c: sqlite3.Connection, site_id: int, feeds: list[int]
) -> None:
    del site_id, feeds
    set_setting(c, "forecast_blend_depth", "3")


def _mutate_timezone(c: sqlite3.Connection, site_id: int, feeds: list[int]) -> None:
    del feeds
    c.execute("UPDATE sites SET timezone = 'Etc/GMT+1' WHERE id = ?", (site_id,))


def _mutate_rain_threshold(
    c: sqlite3.Connection, site_id: int, feeds: list[int]
) -> None:
    del feeds
    c.execute("UPDATE sites SET rain_threshold_mm = 0.3 WHERE id = ?", (site_id,))


def _mutate_wall_clock_per_site(
    c: sqlite3.Connection, site_id: int, feeds: list[int]
) -> None:
    del feeds
    set_setting(c, f"{SNAPSHOT_TIME_KEY}:{site_id}", "08:30")


def _mutate_wall_clock_global(
    c: sqlite3.Connection, site_id: int, feeds: list[int]
) -> None:
    del site_id, feeds
    set_setting(c, SNAPSHOT_TIME_KEY, "08:00")


def _mutate_window_days(c: sqlite3.Connection, site_id: int, feeds: list[int]) -> None:
    del site_id, feeds
    set_setting(c, "rolling_window_days", "20")


def _mutate_min_n(c: sqlite3.Connection, site_id: int, feeds: list[int]) -> None:
    del site_id, feeds
    set_setting(c, "min_n", "31")


def _mutate_tz_generation_pointer_reset(
    c: sqlite3.Connection, site_id: int, feeds: list[int]
) -> None:
    del feeds
    from wxverify.db.tz_generations import published_pointer_key

    site = c.execute("SELECT timezone FROM sites WHERE id = ?", (site_id,)).fetchone()
    assert site is not None
    cur = c.execute(
        """
        INSERT INTO timezone_generations
            (site_id, timezone, mode, state, published_at)
        VALUES (?, ?, 'initial', 'published', ?)
        """,
        (site_id, str(site["timezone"]), "2026-01-01T00:00:00Z"),
    )
    assert cur.lastrowid is not None
    new_id = int(cur.lastrowid)
    verification_run_module.set_runtime_state(
        c, published_pointer_key(site_id), str(new_id)
    )


_Mutator = Callable[[sqlite3.Connection, int, list[int]], None]

# (case id, optional pre-drive setup, post-drive mutation, expected key text)
_T7CD_CASES: list[tuple[str, _Mutator | None, _Mutator, str]] = [
    ("roster_disabled", None, _mutate_roster_disabled, "roster"),
    ("roster_max_lead_hours", None, _mutate_roster_max_lead_hours, "roster"),
    (
        "blend_depth_and_depths",
        None,
        _mutate_blend_depth_and_depths,
        "blend_depths, blend_depth",
    ),
    ("blend_depths_only", None, _mutate_blend_depths_only, "blend_depths"),
    (
        "blend_depth_only",
        _setup_blend_depth_alone,
        _mutate_blend_depth_only,
        "blend_depth",
    ),
    ("timezone", None, _mutate_timezone, "timezone"),
    ("rain_threshold_mm", None, _mutate_rain_threshold, "rain_threshold_mm"),
    ("wall_clock_per_site", None, _mutate_wall_clock_per_site, "wall_clock"),
    ("wall_clock_global", None, _mutate_wall_clock_global, "wall_clock"),
    ("window_days", None, _mutate_window_days, "window_days"),
    ("min_n", None, _mutate_min_n, "min_n"),
    (
        "tz_generation_id_pointer_reset",
        None,
        _mutate_tz_generation_pointer_reset,
        "tz_generation_id",
    ),
]


@pytest.mark.parametrize(
    ("case_id", "setup", "mutate", "key"),
    _T7CD_CASES,
    ids=[c[0] for c in _T7CD_CASES],
)
def test_message_parity_per_key(
    tmp_path: Path,
    case_id: str,
    setup: _Mutator | None,
    mutate: _Mutator,
    key: str,
) -> None:
    del case_id  # carried only for the readable parametrize id

    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, feeds, _station = build_synthetic_verification_site(conn)
        feed_list = list(feeds)
        if setup is not None:
            setup(conn, site_id, feed_list)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        blob = await db.read(lambda c: _load_state(c, site_id))
        assert blob is not None
        run_id = int(blob["run_id"])  # type: ignore[arg-type]

        expected = f"verification run {run_id} inputs changed mid-run: {key}"

        def _write_path_message(c: sqlite3.Connection) -> str:
            cfg = run_config_from_row(c, run_id)
            c.execute("SAVEPOINT probe")
            try:
                assert_inputs_unpinned_unchanged(c, cfg)
                raise AssertionError("expected a RuntimeError")
            except RuntimeError as exc:
                return str(exc)
            finally:
                c.execute("ROLLBACK TO probe")
                c.execute("RELEASE probe")

        def _read_path_message(c: sqlite3.Connection) -> str:
            cfg = run_config_from_row(c, run_id)
            try:
                assert_inputs_unchanged_readonly(c, cfg)
                raise AssertionError("expected a RuntimeError")
            except RuntimeError as exc:
                return str(exc)

        await db.write(lambda c: mutate(c, site_id, feed_list))

        wp = await db.write(_write_path_message)
        rp = await db.read(_read_path_message)
        assert wp == expected
        assert rp == expected

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T7e -- missing pointer.
# ---------------------------------------------------------------------------


def test_missing_published_pointer_is_flagged_readonly_only(tmp_path: Path) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        blob = await db.read(lambda c: _load_state(c, site_id))
        assert blob is not None
        run_id = int(blob["run_id"])  # type: ignore[arg-type]

        before = await db.read(
            lambda c: c.execute("SELECT COUNT(*) FROM timezone_generations").fetchone()[
                0
            ]
        )

        from wxverify.db.tz_generations import published_pointer_key

        await db.write(
            lambda c: c.execute(
                "DELETE FROM runtime_state WHERE key = ?",
                (published_pointer_key(site_id),),
            )
        )

        def _read_path_message(c: sqlite3.Connection) -> str:
            cfg = run_config_from_row(c, run_id)
            try:
                assert_inputs_unchanged_readonly(c, cfg)
            except RuntimeError as exc:
                return str(exc)
            raise AssertionError("expected a RuntimeError")

        message = await db.read(_read_path_message)
        assert message.endswith("tz_generation_id")

        after = await db.read(
            lambda c: c.execute("SELECT COUNT(*) FROM timezone_generations").fetchone()[
                0
            ]
        )
        assert after == before

    asyncio.run(_run())


# T5-generated golden (base commit 4454267, pre-write-lock-fix
# `wxverify.worker.verification_run`): sha256 over `verification_evidence`
# (ordered by id) and `verification_day_context` (ordered by
# snapshot_local_date), both quoted and filtered to the synthetic run's
# run_id -- see `tests/test_verification_day_differential.py`'s own
# `GOLDEN_0163`, generated identically and by the same script
# (`.tmp/myers/t5_golden.py`, 2026-09-26) against the same
# `build_synthetic_verification_site` fixture. Duplicated here (not moved to
# `tests/helpers.py`) because it is only used by these two modules.
GOLDEN_0163 = "b1d62aa76d5b8f4bd9166f0fcec4be115375ee4a53886cefc9a1c43f0b7a875e"


# ---------------------------------------------------------------------------
# T7f -- a discarded day resumes on the next trigger, from the same day.
# ---------------------------------------------------------------------------


def test_discarded_day_resumes_from_the_same_day_on_the_next_trigger(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=7)
        await _drive_to_simulate(db, writer, site_id, payload)

        real_compute = simulate_module.compute_snapshot_day
        entered = threading.Event()
        release = threading.Event()

        def _gated(conn: sqlite3.Connection, cfg: object, day: str) -> DayEvidence:
            entered.set()
            if not release.wait(timeout=5.0):
                raise TimeoutError("release was never set")
            return real_compute(conn, cfg, day)

        patch = pytest.MonkeyPatch()
        patch.setattr(verification_run_module, "compute_snapshot_day", _gated)
        patch.setattr(simulate_module, "compute_snapshot_day", _gated)

        job_id_row = await db.write(
            lambda c: enqueue_if_absent(
                c,
                "verification_run",
                site_id,
                verification_job_key(site_id),
                payload,
            )
        )
        assert job_id_row.created
        job = await db.write(claim_next_job)
        assert job is not None

        caplog.set_level(logging.WARNING, logger="wxverify.worker.verification_run")
        caplog.set_level(logging.WARNING, logger="wxverify.worker.processor")
        task = asyncio.create_task(run_claimed_job(db, job, lane="main"))
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)
            raw = await db.read(
                lambda c: get_runtime_state(
                    c, verification_run_module.verification_state_key(site_id)
                )
            )
            assert raw is not None
            same_content = json.dumps(json.loads(raw))
            await db.write(
                lambda c: verification_run_module.set_runtime_state(
                    c,
                    verification_run_module.verification_state_key(site_id),
                    same_content,
                )
            )
        finally:
            release.set()
            try:
                await task
            finally:
                patch.undo()

        # (a)
        warning_records = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING
            and r.name == "wxverify.worker.verification_run"
        ]
        assert len(warning_records) == 1
        assert warning_records[0].getMessage() == (
            f"verification day discarded site={site_id} day=2026-01-01:"
            " chain state changed during compute"
        )
        assert not any(
            "db read failed or cancelled" in r.getMessage() for r in caplog.records
        )

        # (b)
        def _job_status(c: sqlite3.Connection) -> str:
            row = c.execute(
                "SELECT status FROM jobs WHERE id = ?", (job.id,)
            ).fetchone()
            assert row is not None
            return str(row["status"])

        assert await db.read(_job_status) == "completed"
        assert not await db.read(lambda c: verification_chain_active(c, site_id))

        # (c)
        recorded_days: list[str] = []
        real_compute2 = simulate_module.compute_snapshot_day

        def _recording(conn: sqlite3.Connection, cfg: object, day: str) -> DayEvidence:
            recorded_days.append(day)
            return real_compute2(conn, cfg, day)

        patch2 = pytest.MonkeyPatch()
        patch2.setattr(verification_run_module, "compute_snapshot_day", _recording)
        patch2.setattr(simulate_module, "compute_snapshot_day", _recording)
        digest_at_aggregate: str | None = None
        last_phase: str | None = None
        caplog.clear()
        caplog.set_level(logging.INFO, logger="wxverify.worker.verification_run")
        try:
            next_payload = {
                "trigger_date": "2026-01-26",
                "resamples": 40,
                "snapshot_days_per_chunk": 7,
            }
            fresh_writer = FencedWriter(db, db.generation)
            for _ in range(400):
                result = await run_verification_chunk(
                    db, fresh_writer, site_id, next_payload
                )
                blob = await db.read(lambda c: _load_state(c, site_id))
                phase = None if blob is None else blob.get("phase")
                if (
                    digest_at_aggregate is None
                    and phase == "aggregate"
                    and last_phase != "aggregate"
                ):
                    resumed_run_id = int(blob["run_id"])  # type: ignore[index]
                    digest_at_aggregate = await db.read(
                        lambda c, r=resumed_run_id: evidence_digest(c, r)
                    )
                last_phase = phase
                if result is None:
                    break
            else:
                raise AssertionError("chain never terminated")
        finally:
            patch2.undo()

        assert recorded_days
        assert recorded_days[0] == "2026-01-01"

        # (d) the resumed run's evidence, once aggregated, matches the T5
        # golden -- the discard-and-replay does not change the SETTLED
        # evidence the run ultimately publishes.
        assert digest_at_aggregate is not None
        assert digest_at_aggregate == GOLDEN_0163

        # (e) the resumed chain still reaches publish.
        assert result is None
        assert any(
            record.name == "wxverify.worker.verification_run"
            and record.levelno == logging.INFO
            and record.getMessage()
            == f"verification run={resumed_run_id} published for site={site_id}"
            for record in caplog.records
        )

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T8 -- database replacement mid-compute.
# ---------------------------------------------------------------------------


def test_database_replacement_mid_compute_raises_stale_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        pre_blob_raw = await db.read(
            lambda c: get_runtime_state(
                c, verification_run_module.verification_state_key(site_id)
            )
        )

        backup_src = tmp_path / "pre.db"

        def _backup(c: sqlite3.Connection) -> None:
            dest = sqlite3.connect(str(backup_src))
            try:
                c.backup(dest)
            finally:
                dest.close()

        await db.read(_backup)

        entered = threading.Event()
        release = threading.Event()
        real_compute = simulate_module.compute_snapshot_day

        def _gated(conn: sqlite3.Connection, cfg: object, day: str) -> DayEvidence:
            entered.set()
            if not release.wait(timeout=5.0):
                raise TimeoutError("release was never set")
            return real_compute(conn, cfg, day)

        for binding in (verification_run_module, simulate_module):
            monkeypatch.setattr(binding, "compute_snapshot_day", _gated)

        chunk_task = asyncio.create_task(
            run_verification_chunk(db, writer, site_id, payload)
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)
            unused_backup = tmp_path / "unused-backup.db"
            replace_task = asyncio.create_task(
                db.replace_from(backup_src, unused_backup)
            )
            await asyncio.sleep(0.05)
            assert not db._read_gate.is_set()  # noqa: SLF001
            assert db._write_lock.locked()  # noqa: SLF001
            assert not replace_task.done()
        finally:
            release.set()
            with pytest.raises(StaleGenerationError):
                await chunk_task
            await replace_task

        def _live_evidence(c: sqlite3.Connection) -> int:
            return int(
                c.execute(
                    "SELECT COUNT(*) FROM verification_evidence"
                    " WHERE snapshot_local_date = '2026-01-01'"
                ).fetchone()[0]
            )

        assert await db.read(_live_evidence) == 0
        live_blob_raw = await db.read(
            lambda c: get_runtime_state(
                c, verification_run_module.verification_state_key(site_id)
            )
        )
        assert live_blob_raw == pre_blob_raw

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T9 -- cancellation.
# ---------------------------------------------------------------------------


def test_cancel_during_compute_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        cursor_before = await db.read(lambda c: _load_state(c, site_id))
        assert cursor_before is not None

        real_compute = simulate_module.compute_snapshot_day
        entered = threading.Event()
        release = threading.Event()

        def _gated(conn: sqlite3.Connection, cfg: object, day: str) -> DayEvidence:
            # Delegate to the real compute FIRST, then block -- so "nothing
            # persisted" after cancellation means the cancel discarded a
            # real, non-empty result, not that the gate never produced one.
            result = real_compute(conn, cfg, day)  # type: ignore[arg-type]
            entered.set()
            if not release.wait(timeout=5.0):
                raise TimeoutError("release was never set")
            return result

        for binding in (verification_run_module, simulate_module):
            monkeypatch.setattr(binding, "compute_snapshot_day", _gated)

        chunk_task = asyncio.create_task(
            run_verification_chunk(db, writer, site_id, payload)
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)
            chunk_task.cancel()
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await chunk_task

        def _evidence(c: sqlite3.Connection) -> int:
            return int(
                c.execute(
                    "SELECT COUNT(*) FROM verification_evidence"
                    " WHERE snapshot_local_date = '2026-01-01'"
                ).fetchone()[0]
            )

        def _day_context(c: sqlite3.Connection) -> int:
            return int(
                c.execute(
                    "SELECT COUNT(*) FROM verification_day_context"
                    " WHERE snapshot_local_date = '2026-01-01'"
                ).fetchone()[0]
            )

        assert await db.read(_evidence) == 0
        assert await db.read(_day_context) == 0
        after = await db.read(lambda c: _load_state(c, site_id))
        assert after == cursor_before
        for reader in db._read_conns:  # noqa: SLF001
            assert reader.execute("PRAGMA query_only").fetchone()[0] == 0
        assert_read_pool_at_rest(db)

    asyncio.run(_run())


def test_cancel_during_persist_lets_the_commit_land(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        entered = threading.Event()
        release = threading.Event()
        real_persist = verification_run_module.persist_day_evidence

        def _gated(c: sqlite3.Connection, evidence: DayEvidence) -> None:
            entered.set()
            if not release.wait(timeout=5.0):
                raise TimeoutError("release was never set")
            return real_persist(c, evidence)

        monkeypatch.setattr(verification_run_module, "persist_day_evidence", _gated)

        chunk_task = asyncio.create_task(
            run_verification_chunk(db, writer, site_id, payload)
        )
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)
            chunk_task.cancel()
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await chunk_task

        def _counts(c: sqlite3.Connection) -> tuple[int, str | None]:
            ev = int(
                c.execute(
                    "SELECT COUNT(*) FROM verification_evidence"
                    " WHERE snapshot_local_date = '2026-01-01'"
                ).fetchone()[0]
            )
            blob = _load_state(c, site_id)
            cursor = None if blob is None else blob.get("cursor")
            return ev, cursor

        ev_count, cursor = await db.read(_counts)
        assert ev_count > 0
        assert cursor == "2026-01-02"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T10 -- chunk sizing.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("payload_value", "expected_days"),
    [
        (1, 1),
        (3, 3),
        (7, 7),
        (None, 7),
        (0, 7),
        (True, 7),
        ("7", 7),
    ],
)
def test_chunk_sizing_days_per_first_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload_value: object,
    expected_days: int,
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        setup_payload = _payload(days_per_chunk=99)
        await _drive_to_simulate(db, writer, site_id, setup_payload)

        real_heartbeat = verification_run_module._heartbeat  # noqa: SLF001
        calls = {"n": 0}

        def _counting(c: sqlite3.Connection, sid: int) -> None:
            calls["n"] += 1
            return real_heartbeat(c, sid)

        monkeypatch.setattr(verification_run_module, "_heartbeat", _counting)

        payload: dict[str, object] = {"trigger_date": "2026-01-25"}
        if payload_value is not None:
            payload["snapshot_days_per_chunk"] = payload_value
        result = await run_verification_chunk(db, writer, site_id, payload)
        assert result is not None
        assert calls["n"] == expected_days

    asyncio.run(_run())


def test_chunk_sizing_24_day_period_takes_four_claims_at_7(
    tmp_path: Path,
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=7)
        await _drive_to_simulate(db, writer, site_id, payload)

        claim_days: list[int] = []
        for _ in range(6):
            blob_before = await db.read(lambda c: _load_state(c, site_id))
            if blob_before is not None and blob_before.get("phase") != "simulate":
                break
            cursor_before = date.fromisoformat(str(blob_before["cursor"]))
            fresh_writer = FencedWriter(db, db.generation)
            result = await run_verification_chunk(db, fresh_writer, site_id, payload)
            assert result is not None
            blob_after = await db.read(lambda c: _load_state(c, site_id))
            if blob_after is not None and blob_after.get("phase") == "simulate":
                cursor_after = date.fromisoformat(str(blob_after["cursor"]))
                claim_days.append((cursor_after - cursor_before).days)
            else:
                claim_days.append(24 - (cursor_before - date(2026, 1, 1)).days)
                break

        assert claim_days == [7, 7, 7, 3]

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T12 -- the per-claim INFO line.
# ---------------------------------------------------------------------------


def test_per_claim_info_line(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=7)
        await _drive_to_simulate(db, writer, site_id, payload)

        # Advance three 7-day claims to reach the 4th (3 days remaining).
        for _ in range(3):
            fresh_writer = FencedWriter(db, db.generation)
            result = await run_verification_chunk(db, fresh_writer, site_id, payload)
            assert result is not None

        caplog.clear()
        caplog.set_level(logging.INFO, logger="wxverify.worker.verification_run")
        fresh_writer = FencedWriter(db, db.generation)
        result = await run_verification_chunk(db, fresh_writer, site_id, payload)
        assert result is not None

        prefix = f"verification chunk site={site_id} phase=simulate"
        info_lines = [
            r
            for r in caplog.records
            if r.levelno == logging.INFO and r.getMessage().startswith(prefix)
        ]
        assert len(info_lines) == 1
        message = info_lines[0].getMessage()
        assert "days=3" in message
        assert "last_day=2026-01-24" in message

    asyncio.run(_run())


def test_per_claim_info_line_absent_on_raise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        writer = FencedWriter(db, db.generation)
        payload = _payload(days_per_chunk=2)
        await _drive_to_simulate(db, writer, site_id, payload)

        def _boom(conn: sqlite3.Connection, cfg: object, day: str) -> DayEvidence:
            raise ValueError("injected")

        for binding in (verification_run_module, simulate_module):
            monkeypatch.setattr(binding, "compute_snapshot_day", _boom)

        caplog.clear()
        caplog.set_level(logging.INFO, logger="wxverify.worker.verification_run")
        with pytest.raises(ValueError, match="injected"):
            await run_verification_chunk(db, writer, site_id, payload)

        assert not any(
            r.getMessage().startswith("verification chunk site=")
            for r in caplog.records
        )

    asyncio.run(_run())
