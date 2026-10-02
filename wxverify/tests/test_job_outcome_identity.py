"""F9/W1 regression: a job's outcome lands only on the claimed row (§5.5).

These tests run the REAL ``_still_claimed`` guard. None of them imports
``_patch_worker_infra`` (test_011_patch.py, test_debug_logging.py), the
offlock ``_claimed`` fixture, or any other fixture that patches
``_still_claimed`` -- a patched check would let every test here pass on
unguarded code. Every outcome, continuation and generation check reads raw
SQL after the real write path has run, never a mock's call record.

Synthetic fixtures only -- fake site names and station ids.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest

import wxverify.verification.record as record_mod
from tests.test_forecast_record import _snapshot_t
from tests.test_forecast_record_offlock import _make_gate, _row_count, _seed
from tests.test_site_delete_wind_state import _init_tmp_db, _make_site
from wxverify.db.connection import Database, FencedWriter, close_db, get_db
from wxverify.db.queue import Job, claim_next_job
from wxverify.db.runtime_state import delete_runtime_state
from wxverify.db.tz_generations import (
    correction_job_key,
    ensure_published_generation,
    published_generation_id,
    published_pointer_key,
)
from wxverify.worker.control import JobCancelled, JobContinuation, JobDeferred
from wxverify.worker.processor import dispatch, run_claimed_job

_SNAP = (
    "SELECT status, retry_count, last_error, result, next_attempt_at, updated_at"
    " FROM jobs WHERE id = ?"
)
_DEFER_AT = "2099-01-01T00:00:00.000Z"


def _produce(outcome: str, site_x: int) -> JobContinuation | None:
    if outcome == "continue":
        return JobContinuation(
            "record_gap_scan",
            site_x,
            f"gapscan:cont:{site_x}",
            {"after_date": "2026-03-08"},
        )
    if outcome == "cancel":
        raise JobCancelled()
    if outcome == "defer":
        raise JobDeferred(_DEFER_AT)
    if outcome == "error":
        raise RuntimeError("synthetic dispatch failure")
    raise AssertionError(f"unknown outcome {outcome!r}")


def _setup(tmp_path: Path) -> tuple[sqlite3.Connection, int, int, Job]:
    conn = _init_tmp_db(tmp_path)
    conn.execute("DELETE FROM jobs")
    site_y = _make_site(conn, "Testsite Y")
    site_x = _make_site(conn, "Testsite X")
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload)"
        " VALUES ('record_gap_scan', ?, ?, '{}')",
        (site_x, f"gapscan:{site_x}"),
    )
    conn.commit()
    job = claim_next_job(conn)
    assert job is not None
    assert job.site_id == site_x and job.type == "record_gap_scan"
    return conn, site_y, site_x, job


def _make_dispatch_stub(outcome: str, site_x: int, swap):  # noqa: ANN001
    async def _dispatch(
        db: Database, writer: FencedWriter, j: Job
    ) -> JobContinuation | None:
        if swap is not None:
            await db.write(swap)
        return _produce(outcome, site_x)

    return _dispatch


# ---------------------------------------------------------------------------
# E1: an outcome never lands on a reused job id.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("outcome", ["continue", "cancel", "defer", "error"])
@pytest.mark.parametrize(
    "reused_shape",
    [
        pytest.param(
            ("record_gap_scan", "same_type", "pending"), id="pending-same-type"
        ),
        pytest.param(
            ("fetch_current_obs", "other_type", "running"), id="running-other-type"
        ),
    ],
)
def test_outcome_never_lands_on_a_reused_job_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    reused_shape: tuple[str, str, str],
) -> None:
    conn, site_y, site_x, job = _setup(tmp_path)
    reused_type, _label, reused_status = reused_shape
    try:

        def _swap(c: sqlite3.Connection) -> tuple[int, tuple[object, ...]]:
            c.execute("DELETE FROM sites WHERE id = ?", (site_x,))
            if reused_type == "fetch_current_obs":
                job_key = "curobs:KTEST001"
            else:
                job_key = f"gapscan:{site_y}"
            cur = c.execute(
                "INSERT INTO jobs (type, site_id, job_key, payload, status)"
                " VALUES (?, ?, ?, '{}', ?)",
                (reused_type, site_y, job_key, reused_status),
            )
            rid = int(cur.lastrowid)
            return rid, tuple(c.execute(_SNAP, (rid,)).fetchone())

        captured: dict[str, tuple[int, tuple[object, ...]]] = {}

        def _swap_capturing(c: sqlite3.Connection) -> tuple[int, tuple[object, ...]]:
            result = _swap(c)
            captured["before"] = result
            return result

        monkeypatch.setattr(
            "wxverify.worker.processor.dispatch",
            _make_dispatch_stub(outcome, site_x, _swap_capturing),
        )

        asyncio.run(run_claimed_job(get_db(), job, lane="main"))

        rid, before = captured["before"]
        assert rid == job.id, "job id was not reused; rebuild, don't weaken"
        assert tuple(conn.execute(_SNAP, (rid,)).fetchone()) == before
        assert (
            conn.execute(
                "SELECT 1 FROM jobs WHERE job_key = ?",
                (f"gapscan:cont:{site_x}",),
            ).fetchone()
            is None
        )
    finally:
        close_db()


# ---------------------------------------------------------------------------
# E2: a continuation never attaches to a recreated site.
# ---------------------------------------------------------------------------


def test_continuation_never_attaches_to_a_recreated_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, site_y, site_x, job = _setup(tmp_path)
    try:
        captured: dict[str, tuple[int, tuple[object, ...]]] = {}

        def _swap(c: sqlite3.Connection) -> tuple[int, tuple[object, ...]]:
            c.execute("DELETE FROM sites WHERE id = ?", (site_x,))
            new_site = _make_site(c, "Testsite X")
            assert new_site == site_x, "site id was not reused; rebuild, don't weaken"
            cur = c.execute(
                "INSERT INTO jobs (type, site_id, job_key, payload, status)"
                " VALUES ('catchup', NULL, 'catchup:test', '{}', 'pending')"
            )
            rid = int(cur.lastrowid)
            result = (rid, tuple(c.execute(_SNAP, (rid,)).fetchone()))
            captured["before"] = result
            return result

        monkeypatch.setattr(
            "wxverify.worker.processor.dispatch",
            _make_dispatch_stub("continue", site_x, _swap),
        )

        asyncio.run(run_claimed_job(get_db(), job, lane="main"))

        rid, before = captured["before"]
        assert rid == job.id, "job id was not reused; rebuild, don't weaken"
        assert tuple(conn.execute(_SNAP, (rid,)).fetchone()) == before
        assert (
            conn.execute(
                "SELECT 1 FROM jobs WHERE job_key = ?",
                (f"gapscan:cont:{site_x}",),
            ).fetchone()
            is None
        )
    finally:
        close_db()


# ---------------------------------------------------------------------------
# E3: positive control -- the outcome lands when the claim still stands.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("outcome", ["continue", "cancel", "defer", "error"])
def test_outcome_lands_when_the_claim_still_stands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    conn, _site_y, site_x, job = _setup(tmp_path)
    try:
        monkeypatch.setattr(
            "wxverify.worker.processor.dispatch",
            _make_dispatch_stub(outcome, site_x, None),
        )

        asyncio.run(run_claimed_job(get_db(), job, lane="main"))

        row = conn.execute(_SNAP, (job.id,)).fetchone()
        assert row is not None
        if outcome == "continue":
            assert row["status"] == "completed"
            assert row["result"] == "ok"
            cont = conn.execute(
                "SELECT status FROM jobs WHERE job_key = ?",
                (f"gapscan:cont:{site_x}",),
            ).fetchone()
            assert cont is not None and cont["status"] == "pending"
        elif outcome == "cancel":
            assert row["status"] == "completed"
            assert row["result"] is None
        elif outcome == "defer":
            assert row["status"] == "pending"
            assert row["next_attempt_at"] == _DEFER_AT
        else:
            assert row["status"] == "pending"
            assert row["retry_count"] == 1
            assert row["next_attempt_at"] is not None
    finally:
        close_db()


# ---------------------------------------------------------------------------
# E4: terminal hooks never hit a recreated site's generation.
# ---------------------------------------------------------------------------


def test_terminal_hook_never_hits_a_recreated_sites_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _init_tmp_db(tmp_path)
    conn.execute("DELETE FROM jobs")
    _site_y = _make_site(conn, "Testsite Y")
    site_x = _make_site(conn, "Testsite X")
    g_old = int(
        conn.execute(
            "INSERT INTO timezone_generations (site_id, timezone, mode, state)"
            " VALUES (?, 'UTC', 'retrospective_correction', 'building')",
            (site_x,),
        ).lastrowid
    )
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, max_retries)"
        " VALUES ('timezone_correction', ?, ?, ?, 0)",
        (site_x, correction_job_key(g_old), json.dumps({"generation_id": g_old})),
    )
    conn.commit()
    job = claim_next_job(conn)
    assert job is not None
    assert job.type == "timezone_correction" and job.site_id == site_x

    try:
        captured: dict[str, tuple[int, tuple[object, ...]]] = {}

        def _swap(c: sqlite3.Connection) -> tuple[int, tuple[object, ...]]:
            c.execute("DELETE FROM sites WHERE id = ?", (site_x,))
            new_site = _make_site(c, "Testsite X")
            assert new_site == site_x
            gid = int(
                c.execute(
                    "INSERT INTO timezone_generations"
                    " (site_id, timezone, mode, state)"
                    " VALUES (?, 'UTC', 'retrospective_correction', 'building')",
                    (site_x,),
                ).lastrowid
            )
            assert gid == g_old, "generation id was not reused; rebuild, don't weaken"
            cur = c.execute(
                "INSERT INTO jobs"
                " (type, site_id, job_key, payload, status, max_retries)"
                " VALUES ('timezone_correction', ?, ?, ?, 'pending', 0)",
                (site_x, correction_job_key(gid), json.dumps({"generation_id": gid})),
            )
            rid = int(cur.lastrowid)
            assert rid == job.id, "job id was not reused; rebuild, don't weaken"
            result = (rid, tuple(c.execute(_SNAP, (rid,)).fetchone()))
            captured["before"] = result
            return result

        monkeypatch.setattr(
            "wxverify.worker.processor.dispatch",
            _make_dispatch_stub("error", site_x, _swap),
        )

        asyncio.run(run_claimed_job(get_db(), job, lane="main"))

        rid, before = captured["before"]
        assert tuple(conn.execute(_SNAP, (rid,)).fetchone()) == before
        gen_row = conn.execute(
            "SELECT state FROM timezone_generations WHERE id = ?", (g_old,)
        ).fetchone()
        assert gen_row is not None and gen_row["state"] == "building"
    finally:
        close_db()


# ---------------------------------------------------------------------------
# E5: the in-dispatch forecast-record write never persists into a recreated
# site.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("recreate", [True, False])
def test_forecast_record_never_persists_into_a_recreated_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recreate: bool
) -> None:
    async def _run() -> None:
        conn = _init_tmp_db(tmp_path)
        conn.execute("DELETE FROM jobs")
        site_id, _station_id = _seed(conn)
        old_gen = published_generation_id(conn, site_id)

        conn.execute(
            "INSERT INTO jobs (type, site_id, job_key, payload)"
            " VALUES ('forecast_record', ?, 'record:2035-06-15', ?)",
            (site_id, json.dumps({"snapshot_local_date": "2035-06-15"})),
        )
        conn.commit()
        job = claim_next_job(conn)
        assert job is not None
        assert job.type == "forecast_record" and job.site_id == site_id

        db = get_db()
        writer = FencedWriter(db, db.generation)
        monkeypatch.setattr(
            record_mod, "utc_now", lambda: _snapshot_t() + timedelta(minutes=5)
        )
        entered, release = _make_gate(monkeypatch)

        task = asyncio.create_task(dispatch(db, writer, job))
        try:
            assert await asyncio.to_thread(entered.wait, 5.0)

            if recreate:

                def _swap(
                    c: sqlite3.Connection,
                ) -> tuple[int, int, int, int]:
                    c.execute("DELETE FROM sites WHERE id = ?", (site_id,))
                    delete_runtime_state(c, published_pointer_key(site_id))
                    new_site = _make_site(c, "Testsite X")
                    new_gen = ensure_published_generation(c, new_site)
                    gen_site = c.execute(
                        "SELECT site_id FROM timezone_generations WHERE id = ?",
                        (new_gen,),
                    ).fetchone()["site_id"]
                    job_count = c.execute(
                        "SELECT COUNT(*) AS n FROM jobs WHERE id = ?", (job.id,)
                    ).fetchone()["n"]
                    return new_site, new_gen, int(gen_site), int(job_count)

                new_site, new_gen, gen_site, job_count = await db.write(_swap)
                assert new_site == site_id, "rebuild, don't weaken"
                assert new_gen == old_gen, "rebuild, don't weaken"
                assert gen_site == site_id, "rebuild, don't weaken"
                assert job_count == 0, "rebuild, don't weaken"
        finally:
            release.set()
            if recreate:
                with pytest.raises(JobCancelled):
                    await task
            else:
                result = await task
                assert result is None

        if recreate:
            assert _row_count(conn, site_id) == 0
        else:
            assert _row_count(conn, site_id) > 0

    try:
        asyncio.run(_run())
    finally:
        close_db()


# ---------------------------------------------------------------------------
# E6: the in-dispatch gap-scan write never runs against a recreated site.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("recreate", [True, False])
def test_gap_scan_never_runs_against_a_recreated_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recreate: bool
) -> None:
    conn, _site_y, site_x, job = _setup(tmp_path)
    try:
        calls: list[int] = []

        def _rec(
            _conn: sqlite3.Connection,
            sid: int,
            _payload: dict[str, object],
            *,
            now: object = None,
        ) -> dict[str, object] | None:
            calls.append(sid)
            return None

        monkeypatch.setattr("wxverify.worker.processor.run_record_gap_scan", _rec)

        if recreate:
            conn.execute("DELETE FROM sites WHERE id = ?", (site_x,))
            new_site = _make_site(conn, "Testsite X")
            assert new_site == site_x, "site id was not reused; rebuild, don't weaken"
            conn.commit()

        db = get_db()

        if recreate:
            with pytest.raises(JobCancelled):
                asyncio.run(dispatch(db, FencedWriter(db, db.generation), job))
            assert calls == []
        else:
            result = asyncio.run(dispatch(db, FencedWriter(db, db.generation), job))
            assert result is None
            assert calls == [site_x]
    finally:
        close_db()
