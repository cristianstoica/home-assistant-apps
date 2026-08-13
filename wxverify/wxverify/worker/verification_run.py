"""Resumable ``verification_run`` job chain (§14).

Mirrors the ``timezone_correction`` chain shape: one claimed job executes
exactly ONE chunk — a single short write transaction — then returns a
:class:`JobContinuation`, so other job types interleave between chunks.
Chain state lives in one ``runtime_state`` JSON blob
(``verification_state:<site_id>``), rewritten INSIDE the same transaction
as the chunk's work; a progress heartbeat
(``verification_heartbeat:<site_id>``) is refreshed each chunk.

Phases: ``regen`` (chunked stale-truth regeneration BEFORE the fingerprint,
§14.2) → ``decide`` (input fingerprint vs the last published run; the
durable trigger-decision row lands here, before any run row exists) →
``start`` (one transaction: prior incomplete attempts wiped, the new run
row pins config + roster + generation + period + seed) → ``simulate``
(chunked walk-forward evidence; each chunk first re-checks the pinned
inputs against the live tables) → ``aggregate`` (cell resolution +
results, one transaction) → ``bootstrap`` (the ONLY async phase: series
prepared on a read connection, the CPU-bound bootstrap runs in
``asyncio.to_thread`` — never inside ``db.write`` — then one write
persists the verdicts) → ``publish`` (integrity check + atomic pointer
flip + blob cleanup).
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from datetime import date, timedelta
from typing import cast

from wxverify.core.timeutil import utc_now
from wxverify.db.connection import Database, FencedWriter
from wxverify.db.queue import Job
from wxverify.db.runtime_state import (
    delete_runtime_state,
    get_runtime_state,
    set_runtime_state,
    set_runtime_state_now,
)
from wxverify.verification.decision import VariableInputs, Verdict, decide_variable
from wxverify.verification.engine import (
    aggregate_run,
    finalize_verdicts,
    prepare_bootstrap_inputs,
    preskipped_verdicts,
    publish_verified_run,
)
from wxverify.verification.runs import (
    RunConfig,
    assert_inputs_unpinned_unchanged,
    capture_config_snapshot,
    failed_attempts_for_fingerprint,
    input_fingerprint,
    mark_run_failed,
    published_fingerprint,
    record_trigger_decision,
    run_config_from_row,
    settled_through,
    start_run,
)
from wxverify.verification.simulate import simulate_snapshot_day
from wxverify.verification.truth import regenerate_marked_truth_chunk
from wxverify.worker.control import JobCancelled, JobContinuation

logger = logging.getLogger(__name__)

# Stale (local day, generation) truth groups regenerated per chunk
# (payload-overridable, like tz_correction's days_per_chunk).
REGEN_CHUNK_GROUPS = 20
# Snapshot days simulated per chunk transaction (payload-overridable).
SNAPSHOT_DAYS_PER_CHUNK = 7
# Failed attempts on ONE fingerprint before the trigger stops retrying (§14:
# a third start without completion marks the effort failed, not re-pended).
MAX_FAILED_ATTEMPTS = 2

_STATE_KEY_PREFIX = "verification_state:"
_HEARTBEAT_KEY_PREFIX = "verification_heartbeat:"


def verification_state_key(site_id: int) -> str:
    """``runtime_state`` key of the chain-state JSON blob."""
    return f"{_STATE_KEY_PREFIX}{site_id}"


def verification_heartbeat_key(site_id: int) -> str:
    """``runtime_state`` key of the chain progress heartbeat."""
    return f"{_HEARTBEAT_KEY_PREFIX}{site_id}"


def verification_job_key(site_id: int) -> str:
    """Queue dedupe key of the site's verification chain."""
    return f"verify:{site_id}"


def mark_verification_failed(conn: sqlite3.Connection, job: Job) -> None:
    """Terminal-failure hook: fail the site's running attempt, drop the blob.

    The failed run keeps its metadata row (attempt accounting reads it);
    the NEXT nightly trigger starts a fresh chain — whose run-start
    transaction also wipes the failed attempt's partial evidence.
    """
    site_id = job.site_id
    if site_id is None:
        return
    mark_run_failed(conn, site_id, "terminal job failure")
    delete_runtime_state(
        conn, verification_state_key(site_id), verification_heartbeat_key(site_id)
    )
    logger.error("verification run for site=%s marked failed (terminal job)", site_id)


async def run_verification_chunk(
    db: Database, writer: FencedWriter, site_id: int, payload: dict[str, object]
) -> JobContinuation | None:
    """Execute one chain chunk; returns the continuation while chunks remain."""
    blob = await db.read(lambda conn: _load_state(conn, site_id))
    phase = str((blob or {}).get("phase", ""))
    if blob is not None and phase == "bootstrap":
        run_id = _blob_int(blob, "run_id")
        if run_id is None:
            raise JobCancelled()
        cfg = await db.read(lambda conn: run_config_from_row(conn, run_id))
        inputs = await db.read(lambda conn: prepare_bootstrap_inputs(conn, cfg))
        resamples = _chunk_size(payload, "resamples", cfg.bootstrap_resamples)
        # CPU-bound (~seconds to minutes at 10k resamples): never inside
        # db.write — the write lane must stay available to other jobs.
        verdicts = await asyncio.to_thread(
            _compute_verdicts, inputs, cfg.bootstrap_seed, resamples
        )
        # §15/F-3: variables whose incumbent depth is outside SIM_DEPTHS
        # were skipped by prepare_bootstrap_inputs; persist their explicit
        # 'skipped' verdicts so publish integrity still sees one per
        # variable and nothing all-insufficient publishes silently.
        verdicts.extend(preskipped_verdicts(cfg))
        await writer.write(lambda conn: _persist_verdicts(conn, site_id, cfg, verdicts))
        return _continuation(site_id, payload)
    more = await writer.write(lambda conn: advance_verification(conn, site_id, payload))
    return _continuation(site_id, payload) if more else None


def _compute_verdicts(
    inputs: list[VariableInputs], seed: int, resamples: int
) -> list[Verdict]:
    return [
        decide_variable(variable_inputs, seed=seed, resamples=resamples)
        for variable_inputs in inputs
    ]


def _persist_verdicts(
    conn: sqlite3.Connection,
    site_id: int,
    cfg: RunConfig,
    verdicts: list[Verdict],
) -> None:
    """One write transaction: verdicts land and the blob advances to publish."""
    blob = _load_state(conn, site_id)
    if blob is None or str(blob.get("phase")) != "bootstrap":
        raise JobCancelled()
    finalize_verdicts(conn, cfg, verdicts)
    blob["phase"] = "publish"
    _save_state(conn, site_id, blob)
    _heartbeat(conn, site_id)


def advance_verification(
    conn: sqlite3.Connection, site_id: int, payload: dict[str, object]
) -> bool:
    """Execute one SYNC chain chunk inside the caller's write transaction."""
    loaded = _load_state(conn, site_id)
    blob: dict[str, object] = {"phase": "regen"} if loaded is None else loaded
    phase = str(blob.get("phase", ""))
    if phase == "regen":
        limit = _chunk_size(payload, "regen_chunk_groups", REGEN_CHUNK_GROUPS)
        done = regenerate_marked_truth_chunk(conn, site_id=site_id, limit=limit)
        if done < limit:
            blob["phase"] = "decide"
        _save_state(conn, site_id, blob)
        _heartbeat(conn, site_id)
        return True
    if phase == "decide":
        return _decide_phase(conn, site_id, payload, blob)
    if phase == "start":
        return _start_phase(conn, site_id, blob)
    if phase == "simulate":
        return _simulate_chunk(conn, site_id, payload, blob)
    if phase == "aggregate":
        cfg = _blob_config(conn, blob)
        aggregate_run(conn, cfg)
        blob["phase"] = "bootstrap"
        _save_state(conn, site_id, blob)
        _heartbeat(conn, site_id)
        return True
    if phase == "publish":
        cfg = _blob_config(conn, blob)
        publish_verified_run(conn, cfg)
        _clear_state(conn, site_id)
        logger.info("verification run=%s published for site=%s", cfg.run_id, site_id)
        return False
    if phase == "bootstrap":
        # The async orchestrator owns this phase; reaching the sync path
        # means the phase check above raced a crash — re-run next chunk.
        return True
    raise RuntimeError(
        f"verification chain site={site_id} has an unknown phase {phase!r}"
    )


def _decide_phase(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    blob: dict[str, object],
) -> bool:
    trigger_date = payload.get("trigger_date")
    if not isinstance(trigger_date, str):
        raise JobCancelled()
    try:
        snapshot = capture_config_snapshot(conn, site_id)
    except ValueError as exc:
        raise JobCancelled() from exc
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    generation_id = int(str(snapshot["tz_generation_id"]))
    if published_fingerprint(conn, site_id) == fingerprint:
        record_trigger_decision(
            conn,
            site_id,
            trigger_date=trigger_date,
            decision="no_change_skip",
            reason="input fingerprint matches the published run",
            fingerprint=fingerprint,
        )
        _clear_state(conn, site_id)
        return False
    if (
        failed_attempts_for_fingerprint(conn, site_id, fingerprint)
        >= MAX_FAILED_ATTEMPTS
    ):
        record_trigger_decision(
            conn,
            site_id,
            trigger_date=trigger_date,
            decision="skipped",
            reason="attempt cap reached for this fingerprint",
            fingerprint=fingerprint,
        )
        _clear_state(conn, site_id)
        return False
    if (
        settled_through(
            conn, site_id=site_id, tz_generation_id=generation_id, now=utc_now()
        )
        is None
    ):
        record_trigger_decision(
            conn,
            site_id,
            trigger_date=trigger_date,
            decision="skipped",
            reason="no settled truth under the published generation",
            fingerprint=fingerprint,
        )
        _clear_state(conn, site_id)
        return False
    # The durable §14 decision row: committed in THIS transaction, before
    # any run row exists.
    record_trigger_decision(
        conn,
        site_id,
        trigger_date=trigger_date,
        decision="run_started",
        reason=None,
        fingerprint=fingerprint,
    )
    decision_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
    _save_state(
        conn,
        site_id,
        {
            "phase": "start",
            "fingerprint": fingerprint,
            "decision_id": int(decision_id["id"]),
        },
    )
    _heartbeat(conn, site_id)
    return True


def _start_phase(
    conn: sqlite3.Connection, site_id: int, blob: dict[str, object]
) -> bool:
    fingerprint = str(blob.get("fingerprint", ""))
    if not fingerprint:
        raise JobCancelled()
    try:
        snapshot = capture_config_snapshot(conn, site_id)
    except ValueError as exc:
        raise JobCancelled() from exc
    cfg = start_run(
        conn, site_id, snapshot=snapshot, fingerprint=fingerprint, now=utc_now()
    )
    if cfg is None:
        # Settled truth vanished between decide and start (regeneration
        # race) — fail loudly rather than publish an empty run.
        raise RuntimeError(f"verification start for site={site_id}: no settled truth")
    decision_id = _blob_int(blob, "decision_id")
    if decision_id is not None:
        conn.execute(
            "UPDATE verification_trigger_decisions SET run_id = ? WHERE id = ?",
            (cfg.run_id, decision_id),
        )
    _save_state(
        conn,
        site_id,
        {"phase": "simulate", "run_id": cfg.run_id, "cursor": cfg.period_start},
    )
    _heartbeat(conn, site_id)
    return True


def _simulate_chunk(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    blob: dict[str, object],
) -> bool:
    cfg = _blob_config(conn, blob)
    # Obligation: §8 snapshot semantics — every chunk re-checks the pinned
    # roster/config against the live tables and fails the run on divergence.
    assert_inputs_unpinned_unchanged(conn, cfg)
    days = _chunk_size(payload, "snapshot_days_per_chunk", SNAPSHOT_DAYS_PER_CHUNK)
    cursor = date.fromisoformat(str(blob["cursor"]))
    end = date.fromisoformat(cfg.period_end)
    for _ in range(days):
        if cursor > end:
            break
        simulate_snapshot_day(conn, cfg, cursor.isoformat())
        cursor = cursor + timedelta(days=1)
    if cursor > end:
        blob["phase"] = "aggregate"
        blob.pop("cursor", None)
    else:
        blob["cursor"] = cursor.isoformat()
    _save_state(conn, site_id, blob)
    _heartbeat(conn, site_id)
    return True


def _blob_config(conn: sqlite3.Connection, blob: dict[str, object]) -> RunConfig:
    run_id = _blob_int(blob, "run_id")
    if run_id is None:
        raise JobCancelled()
    return run_config_from_row(conn, run_id)


def _continuation(site_id: int, payload: dict[str, object]) -> JobContinuation:
    return JobContinuation(
        job_type="verification_run",
        site_id=site_id,
        job_key=verification_job_key(site_id),
        payload=dict(payload),
    )


def _load_state(conn: sqlite3.Connection, site_id: int) -> dict[str, object] | None:
    raw = get_runtime_state(conn, verification_state_key(site_id))
    if raw is None:
        return None
    try:
        parsed: object = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    return cast(dict[str, object], parsed)


def _save_state(
    conn: sqlite3.Connection, site_id: int, state: dict[str, object]
) -> None:
    set_runtime_state(
        conn,
        verification_state_key(site_id),
        json.dumps(state, separators=(",", ":")),
    )


def _clear_state(conn: sqlite3.Connection, site_id: int) -> None:
    delete_runtime_state(
        conn, verification_state_key(site_id), verification_heartbeat_key(site_id)
    )


def _heartbeat(conn: sqlite3.Connection, site_id: int) -> None:
    set_runtime_state_now(conn, verification_heartbeat_key(site_id))


def _blob_int(blob: dict[str, object], key: str) -> int | None:
    value = blob.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _chunk_size(payload: dict[str, object], key: str, default: int) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return default
    return value
