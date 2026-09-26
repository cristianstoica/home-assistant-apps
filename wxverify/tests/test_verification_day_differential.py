"""Differential parity between the sync reference path and the async
day-pipeline (write-lock-fix plan §5.5: P0-P7, T4, T4b, T4c, T4d).

Every arm below drives the real `run_verification_chunk` entry point --
never a stand-in for it -- from a fresh `FencedWriter` per claim, exactly as
production does. "Arm A"/"arm A'" force the day phases onto the pre-fix sync
path by clearing `verification_run_module._DAY_PHASES`; "arm B"/"arm C" use
the real async day pipeline at different chunk sizes.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
from pathlib import Path

import pytest

from tests.helpers import build_synthetic_verification_site, evidence_digest
from wxverify import config
from wxverify.db.connection import Database, FencedWriter, close_db, get_db, init_db
from wxverify.db.runtime_state import get_runtime_state
from wxverify.forecast.data import forecast_ranking
from wxverify.verification.runs import run_config_from_row
from wxverify.verification.simulate import _daily_rank_order  # noqa: SLF001
from wxverify.worker import verification_run as verification_run_module
from wxverify.worker.verification_run import (
    _load_state,  # noqa: SLF001
    run_verification_chunk,
    verification_state_key,
)


def _init_tmp_db(tmp_path: Path, name: str) -> sqlite3.Connection:
    close_db()
    db_dir = tmp_path / name
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "wxverify.db"
    config.db_path = str(db_path)
    options_path = db_dir / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _payload(days_per_chunk: int) -> dict[str, object]:
    return {
        "trigger_date": "2026-01-25",
        "resamples": 40,
        "snapshot_days_per_chunk": days_per_chunk,
    }


# ---------------------------------------------------------------------------
# §5.4 comparator
# ---------------------------------------------------------------------------

# P7: built empirically (two arm-A runs on fresh databases >= 1.1s apart,
# 2026-09-26, `.tmp/myers/p7_probe.py`) and reviewed here. Every entry is a
# column stamped with `wxverify.core.timeutil.utc_now()` (or an
# equivalent wall-clock write) at insert/update time -- never content the
# compute path derives from truth/forecast data. See
# `test_p7_mask_covers_only_wall_clock_divergence` below, which re-derives
# this same set at test-run time and asserts it is a subset of MASK.
MASK: dict[str, set[str]] = {
    # `daily_truth.generated_at`: stamped by the truth-materialization write
    # path (`verification/runs.py` settlement / truth backfill).
    "daily_truth": {"generated_at"},
    # `forecast_pairs.created_at`: stamped by `asof_insert_pair` at builder
    # time -- differs because the two arm-A runs are two independent
    # `build_synthetic_verification_site` calls, seconds apart.
    "forecast_pairs": {"created_at"},
    # `runtime_state.updated_at`: every `set_runtime_state`/
    # `set_runtime_state_now` write (chain blob, heartbeat, correction
    # markers, published pointers) stamps this column.
    "runtime_state": {"updated_at"},
    "sites": {"created_at"},
    "stations": {"created_at"},
    # `timezone_generations.created_at`/`published_at`: stamped by
    # `ensure_published_generation` at builder time.
    "timezone_generations": {"created_at", "published_at"},
    # `verification_runs.created_at`/`published_at`: stamped by `start_run`
    # and `publish_verified_run` respectively.
    "verification_runs": {"created_at", "published_at"},
    # `verification_trigger_decisions.decided_at`: stamped by
    # `record_trigger_decision`.
    "verification_trigger_decisions": {"decided_at"},
}


def masked_db_dump(
    conn: sqlite3.Connection, mask: dict[str, set[str]] | None = None
) -> list[tuple[str, list[tuple[str, ...]]]]:
    """Every user table, every column, masked cells replaced by a sentinel.

    Rows are ordered by all columns (not rowid), which keeps the dump stable
    under upsert churn.
    """
    mask = mask or {}
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
            " AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    dump: list[tuple[str, list[tuple[str, ...]]]] = []
    for table in tables:
        cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
        masked_cols = mask.get(table, set())
        select_list = ", ".join(
            "'<masked>'" if col in masked_cols else f"quote({col})" for col in cols
        )
        order_list = ", ".join(str(i + 1) for i in range(len(cols)))
        rows = conn.execute(
            f"SELECT {select_list} FROM {table} ORDER BY {order_list}"  # noqa: S608
        ).fetchall()
        dump.append((table, [tuple(row) for row in rows]))
    return dump


def _full_dump_by_rowid(
    conn: sqlite3.Connection,
) -> dict[str, tuple[list[str], list[tuple[object, ...]]]]:
    """Unmasked, rowid-ordered dump -- for building/verifying MASK itself.

    rowid order (not sorted-by-all-columns order) is required here so that
    positionally-paired rows across two independent runs correspond to the
    SAME insert, even when a wall-clock column is among the columns diffed.
    """
    tables = [
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
            " AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    out: dict[str, tuple[list[str], list[tuple[object, ...]]]] = {}
    for table in tables:
        cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
        rows = conn.execute(
            f"SELECT {', '.join(cols)} FROM {table} ORDER BY rowid"  # noqa: S608
        ).fetchall()
        out[table] = (cols, [tuple(row) for row in rows])
    return out


# ---------------------------------------------------------------------------
# Arm drivers
# ---------------------------------------------------------------------------


async def _drive_arm(
    db: Database, site_id: int, payload: dict[str, object], *, sync: bool
) -> list[str | None]:
    """Drive one arm to completion; returns the raw-blob sequence (T4b)."""
    original = verification_run_module._DAY_PHASES  # noqa: SLF001
    if sync:
        verification_run_module._DAY_PHASES = frozenset()  # noqa: SLF001
    blobs: list[str | None] = []
    state_key = verification_state_key(site_id)
    try:
        for _ in range(400):
            writer = FencedWriter(db, db.generation)
            result = await run_verification_chunk(db, writer, site_id, payload)
            blobs.append(await db.read(lambda c: get_runtime_state(c, state_key)))
            if result is None:
                break
        else:
            raise AssertionError("chain never published within 400 claims")
    finally:
        verification_run_module._DAY_PHASES = original  # noqa: SLF001
    return blobs


async def _build_and_drive(
    tmp_path: Path, name: str, *, chunk: int, sync: bool
) -> tuple[sqlite3.Connection, int, tuple[int, int, int], int, list[str | None]]:
    conn = _init_tmp_db(tmp_path, name)
    site_id, feeds, station_id = build_synthetic_verification_site(conn)
    db = get_db()
    payload = _payload(chunk)
    blobs = await _drive_arm(db, site_id, payload, sync=sync)
    return conn, site_id, tuple(feeds), station_id, blobs  # type: ignore[return-value]


def _run_id_of(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM verification_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    return int(row[0])


# ---------------------------------------------------------------------------
# P0-P6: preconditions, on one arm-A run driven to publish.
# ---------------------------------------------------------------------------


def test_p0_run_period_is_the_full_24_day_truth_span(tmp_path: Path) -> None:
    async def _run() -> None:
        conn, site_id, _feeds, _station, _blobs = await _build_and_drive(
            tmp_path, "p0", chunk=7, sync=True
        )
        run_id = _run_id_of(conn)
        cfg = run_config_from_row(conn, run_id)
        assert cfg.period_start == "2026-01-01"
        assert cfg.period_end == "2026-01-24"

    asyncio.run(_run())


def test_p1_builder_commits_with_no_foreign_key_violations(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path, "p1")
    build_synthetic_verification_site(conn)
    violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert violations == []


def test_p2_every_snapshot_day_has_exactly_one_day_context_row(
    tmp_path: Path,
) -> None:
    async def _run() -> None:
        conn, _site_id, _feeds, _station, _blobs = await _build_and_drive(
            tmp_path, "p2", chunk=7, sync=True
        )
        run_id = _run_id_of(conn)
        rows = conn.execute(
            "SELECT snapshot_local_date, COUNT(*) FROM verification_day_context"
            " WHERE run_id = ? GROUP BY snapshot_local_date",
            (run_id,),
        ).fetchall()
        counts = {r[0]: r[1] for r in rows}
        expected_days = {f"2026-01-{d:02d}" for d in range(1, 25)}
        assert set(counts) == expected_days
        assert all(c == 1 for c in counts.values())

    asyncio.run(_run())


def test_p3_every_snapshot_day_has_lead1_forecast_eligible_feed_evidence(
    tmp_path: Path,
) -> None:
    async def _run() -> None:
        conn, _site_id, _feeds, _station, _blobs = await _build_and_drive(
            tmp_path, "p3", chunk=7, sync=True
        )
        run_id = _run_id_of(conn)
        rows = conn.execute(
            "SELECT snapshot_local_date, COUNT(*) FROM verification_evidence"
            " WHERE run_id = ? AND entity_type = 'feed' AND lead = 1"
            " AND forecast_eligible = 1 GROUP BY snapshot_local_date",
            (run_id,),
        ).fetchall()
        counts = {r[0]: r[1] for r in rows}
        expected_days = {f"2026-01-{d:02d}" for d in range(1, 25)}
        assert set(counts) == expected_days
        assert all(c >= 1 for c in counts.values())

    asyncio.run(_run())


def test_p4_daily_rank_order_depends_on_the_within_chunk_days_it_reads(
    tmp_path: Path,
) -> None:
    """Hard gate (§5.3 P4), grounded in `.tmp/myers/p4_probe.py`'s empirical
    run against this exact fixture (2026-09-26).

    Before any delete: at S = 2026-01-20, knowable_through = 2026-01-19,
    `_daily_rank_order` on temperature_high/temperature_low/wind_max ranks
    `[feed_b, feed_c, feed_a]` -- feed a is worst, with mean abs_error
    (14 * 0.5 + 4 * 30.5) / 18 over 18 scorable days.

    After deleting this run's `verification_evidence` for
    `snapshot_local_date BETWEEN '2026-01-15' AND '2026-01-18'` (the plan's
    literal text, §5.3 P4): the same three quantities re-rank to
    `[feed_a, feed_b, feed_c]` -- feed a is now best, with mean abs_error
    exactly 0.5 over the 14 rows the delete left it. An earlier
    investigation pass also tried deleting by
    `target_local_date BETWEEN '2026-01-16' AND '2026-01-19'` and got the
    IDENTICAL before/after result (at lead 1, target_local_date =
    snapshot_local_date + 1, so both ranges select the same lead=1
    evidence rows here) -- confirming the plan's delete-range text, as
    literally written, is correct; there is no plan defect to report.
    """

    async def _run() -> None:
        conn, _site_id, feeds, _station, _blobs = await _build_and_drive(
            tmp_path, "p4", chunk=7, sync=True
        )
        feed_a, feed_b, feed_c = feeds
        run_id = _run_id_of(conn)
        cfg = run_config_from_row(conn, run_id)
        knowable_through = "2026-01-19"
        as_of_row = conn.execute(
            "SELECT snapshot_utc FROM verification_day_context"
            " WHERE run_id = ? AND snapshot_local_date = '2026-01-20'",
            (run_id,),
        ).fetchone()
        assert as_of_row is not None
        as_of = as_of_row[0]

        def _order(quantity: str) -> list[int]:
            return _daily_rank_order(
                conn,
                cfg,
                quantity=quantity,
                knowable_through=knowable_through,
                as_of=as_of,
            )

        def _mean_a(quantity: str) -> tuple[float | None, int]:
            row = conn.execute(
                """
                SELECT AVG(e.abs_error), COUNT(*) FROM verification_evidence e
                JOIN daily_truth dt
                  ON dt.site_id = ? AND dt.tz_generation_id = ?
                 AND dt.quantity = e.quantity AND dt.local_date = e.target_local_date
                WHERE e.run_id = ? AND e.entity_type = 'feed' AND e.entity_key = ?
                  AND e.quantity = ? AND e.lead = 1 AND e.forecast_eligible = 1
                  AND e.truth_eligible = 1 AND e.abs_error IS NOT NULL
                  AND e.target_local_date <= ?
                """,
                (
                    cfg.site_id,
                    cfg.tz_generation_id,
                    run_id,
                    str(feed_a),
                    quantity,
                    knowable_through,
                ),
            ).fetchone()
            return row[0], row[1]

        temp_wind = ("temperature_high", "temperature_low", "wind_max")
        for quantity in temp_wind:
            assert _order(quantity) == [feed_b, feed_c, feed_a], quantity
        mean_a, n_a = _mean_a("temperature_high")
        assert n_a == 18
        assert mean_a == pytest.approx((14 * 0.5 + 4 * 30.5) / 18)

        # precip_total/precip_occurrence: recorded, not asserted to flip.
        precip_total_mean, precip_total_n = _mean_a("precip_total")
        assert precip_total_n == 18
        assert precip_total_mean == pytest.approx((14 * 12 + 4 * 48) / 18)

        conn.execute("SAVEPOINT p4_delete")
        conn.execute(
            "DELETE FROM verification_evidence WHERE run_id = ?"
            " AND snapshot_local_date BETWEEN '2026-01-15' AND '2026-01-18'",
            (run_id,),
        )
        try:
            for quantity in temp_wind:
                assert _order(quantity) == [feed_a, feed_b, feed_c], quantity
            mean_a_after, n_a_after = _mean_a("temperature_high")
            assert n_a_after == 14
            assert mean_a_after == pytest.approx(0.5)
        finally:
            conn.execute("ROLLBACK TO p4_delete")
            conn.execute("RELEASE p4_delete")

    asyncio.run(_run())


def test_p5_temperature_rankings_are_confident_with_a_skill_score(
    tmp_path: Path,
) -> None:
    async def _run() -> None:
        conn, _site_id, _feeds, _station, _blobs = await _build_and_drive(
            tmp_path, "p5", chunk=7, sync=True
        )
        run_id = _run_id_of(conn)
        cfg = run_config_from_row(conn, run_id)
        as_of_row = conn.execute(
            "SELECT snapshot_utc FROM verification_day_context"
            " WHERE run_id = ? AND snapshot_local_date = '2026-01-20'",
            (run_id,),
        ).fetchone()
        assert as_of_row is not None
        rows = forecast_ranking(
            conn,
            site_id=cfg.site_id,
            variable="temperature",
            day_ahead=1,
            as_of=as_of_row[0],
            declared_min_n=cfg.min_n,
            declared_window_days=cfg.window_days,
        )
        assert rows
        for row in rows.values():
            assert row.confident is True
            assert row.skill_score is not None

    asyncio.run(_run())


def test_p6_every_arm_publishes(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path, "p6")
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        payload = _payload(7)
        with caplog.at_level(logging.INFO, logger="wxverify.worker.verification_run"):
            for _ in range(400):
                writer = FencedWriter(db, db.generation)
                result = await run_verification_chunk(db, writer, site_id, payload)
                if result is None:
                    break
            else:
                raise AssertionError("chain never published within 400 claims")
        assert result is None
        assert any(
            f"verification run={_run_id_of(conn)} published for site={site_id}"
            in record.message
            for record in caplog.records
        )

    asyncio.run(_run())


def test_p7_mask_covers_only_wall_clock_divergence(tmp_path: Path) -> None:
    """§5.3 P7: two independent arm-A runs, >=1.1s apart, full cell diff.

    Every differing (table, column) found here must already be listed in
    MASK, and every masked column must actually exist in this schema --
    otherwise a future migration silently dropping a masked column (or a
    regression that makes a NEW column non-deterministic) would go
    unnoticed by the T4 comparator.
    """
    import time

    async def _run() -> None:
        _conn1, _site1, _feeds1, _station1, _blobs1 = await _build_and_drive(
            tmp_path, "p7a", chunk=7, sync=True
        )
        # Snapshot arm A #1's database with a standalone connection BEFORE
        # driving arm A #2 -- `_init_tmp_db` closes the shared pooled
        # connection every run's `db._conn` aliases, so the live `conn1`
        # object would go dead as soon as arm A #2's `close_db()` fires.
        db_path1 = tmp_path / "p7a" / "wxverify.db"
        with contextlib.closing(sqlite3.connect(str(db_path1))) as raw_conn1:
            dump1 = _full_dump_by_rowid(raw_conn1)

        time.sleep(1.1)
        _conn2, _site2, _feeds2, _station2, _blobs2 = await _build_and_drive(
            tmp_path, "p7b", chunk=7, sync=True
        )
        db_path2 = tmp_path / "p7b" / "wxverify.db"
        with contextlib.closing(sqlite3.connect(str(db_path2))) as raw_conn2:
            dump2 = _full_dump_by_rowid(raw_conn2)

        assert set(dump1) == set(dump2)

        diff: dict[str, set[str]] = {}
        for table, (cols1, rows1) in dump1.items():
            cols2, rows2 = dump2[table]
            assert cols1 == cols2, table
            assert len(rows1) == len(rows2), table
            for r1, r2 in zip(rows1, rows2, strict=True):
                for col, v1, v2 in zip(cols1, r1, r2, strict=True):
                    if v1 != v2:
                        diff.setdefault(table, set()).add(col)

        for table, cols in diff.items():
            assert cols <= MASK.get(table, set()), (
                f"{table}: differing columns {cols} not fully covered by "
                f"MASK entry {MASK.get(table, set())}"
            )

        with contextlib.closing(sqlite3.connect(str(db_path2))) as schema_conn:
            for table, cols in MASK.items():
                schema_cols = {
                    row[1]
                    for row in schema_conn.execute(
                        f"PRAGMA table_info({table})"  # noqa: S608
                    )
                }
                assert cols <= schema_cols, f"MASK names a column {table} no longer has"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# T4/T4b/T4c/T4d
# ---------------------------------------------------------------------------


def test_t4_differential_across_arms(tmp_path: Path) -> None:
    async def _run() -> None:
        conn_a, _sa, _fa, _oa, _ba = await _build_and_drive(
            tmp_path, "t4a", chunk=7, sync=True
        )
        dump_a = masked_db_dump(conn_a, MASK)

        conn_ap, _sap, _fap, _oap, _bap = await _build_and_drive(
            tmp_path, "t4ap", chunk=1, sync=True
        )
        dump_ap = masked_db_dump(conn_ap, MASK)

        conn_b, _sb, _fb, _ob, _bb = await _build_and_drive(
            tmp_path, "t4b", chunk=7, sync=False
        )
        dump_b = masked_db_dump(conn_b, MASK)

        conn_c, _sc, _fc, _oc, _bc = await _build_and_drive(
            tmp_path, "t4c", chunk=3, sync=False
        )
        dump_c = masked_db_dump(conn_c, MASK)

        assert dump_a == dump_ap
        assert dump_a == dump_b
        assert dump_a == dump_c

    asyncio.run(_run())


def test_t4b_blob_sequence_identical_between_sync_and_async_arms(
    tmp_path: Path,
) -> None:
    async def _run() -> None:
        _conn_a, _sa, _fa, _oa, blobs_a = await _build_and_drive(
            tmp_path, "t4ba", chunk=7, sync=True
        )
        _conn_b, _sb, _fb, _ob, blobs_b = await _build_and_drive(
            tmp_path, "t4bb", chunk=7, sync=False
        )
        assert blobs_a
        assert blobs_a == blobs_b

    asyncio.run(_run())


def test_t4c_persist_order_matches_evidence_id_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path, "t4c_persist")
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        payload = _payload(7)

        # The conflict key is the full UNIQUE constraint (run_id,
        # snapshot_local_date, lead, variable, quantity, entity_type,
        # entity_key) -- `quantity` alone collides across variables/entity
        # types that reuse small integer entity_keys (e.g. a feed's entity_key
        # and a blend composite's entity_key can both be "1").
        Key = tuple[str, int, str, str, str, str]
        recorded: list[Key] = []
        real_persist = verification_run_module.persist_day_evidence

        def _spy(c: sqlite3.Connection, evidence: object) -> None:
            # `_EVIDENCE_INSERT_SQL` column order (simulate.py):
            # (run_id, snapshot_local_date, target_local_date, lead,
            #  variable, quantity, entity_type, entity_key, ...).
            for row in evidence.evidence_rows:  # type: ignore[attr-defined]
                recorded.append((row[1], row[3], row[4], row[5], row[6], row[7]))
            real_persist(c, evidence)

        monkeypatch.setattr(verification_run_module, "persist_day_evidence", _spy)

        for _ in range(400):
            writer = FencedWriter(db, db.generation)
            result = await run_verification_chunk(db, writer, site_id, payload)
            if result is None:
                break
        else:
            raise AssertionError("chain never published within 400 claims")

        run_id = _run_id_of(conn)
        seen: set[Key] = set()
        first_wins: list[Key] = []
        for key in recorded:
            if key not in seen:
                seen.add(key)
                first_wins.append(key)

        rows = conn.execute(
            "SELECT snapshot_local_date, lead, variable, quantity,"
            " entity_type, entity_key FROM verification_evidence"
            " WHERE run_id = ? ORDER BY id",
            (run_id,),
        ).fetchall()
        actual_order = [tuple(row) for row in rows]

        assert first_wins == actual_order

    asyncio.run(_run())


# T5-generated golden (base commit 4454267, `wxverify.worker.verification_run`
# pre-fix): sha256 over `verification_evidence` (ordered by id) and
# `verification_day_context` (ordered by snapshot_local_date), both quoted
# and filtered to the synthetic run's run_id. Generated 2026-09-26 via
# `.tmp/myers/t5_golden.py` against a `git archive 4454267` checkout.
GOLDEN_0163 = "b1d62aa76d5b8f4bd9166f0fcec4be115375ee4a53886cefc9a1c43f0b7a875e"


def test_t4d_golden_digest_matches_0163(tmp_path: Path) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path, "t4d")
        site_id, _feeds, _station = build_synthetic_verification_site(conn)
        db = get_db()
        payload = _payload(7)

        digest_at_aggregate: str | None = None
        last_phase: str | None = None

        for _ in range(400):
            writer = FencedWriter(db, db.generation)
            result = await run_verification_chunk(db, writer, site_id, payload)
            blob = await db.read(lambda c: _load_state(c, site_id))
            phase = None if blob is None else blob.get("phase")
            if (
                digest_at_aggregate is None
                and phase == "aggregate"
                and last_phase != "aggregate"
            ):
                run_id = int(blob["run_id"])  # type: ignore[index]
                digest_at_aggregate = await db.read(
                    lambda c, r=run_id: evidence_digest(c, r)
                )
            last_phase = phase
            if result is None:
                break
        else:
            raise AssertionError("chain never published within 400 claims")

        assert digest_at_aggregate is not None
        assert digest_at_aggregate == GOLDEN_0163

    asyncio.run(_run())
