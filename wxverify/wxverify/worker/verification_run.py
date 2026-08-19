"""Resumable ``verification_run`` job chain (§14).

Mirrors the ``timezone_correction`` chain shape: one claimed job executes
exactly ONE chunk — a single short write transaction — then returns a
:class:`JobContinuation`, so other job types interleave between chunks.
Chain state lives in one ``runtime_state`` JSON blob
(``verification_state:<site_id>``), rewritten INSIDE the same transaction
as the chunk's work; a progress heartbeat
(``verification_heartbeat:<site_id>``) is refreshed each chunk.

Phases: ``discover`` (chunked discovery and materialization of settled local
days missing from ``daily_truth`` — the only path that CREATES a truth row
outside a retrospective timezone correction, §4/D1) → ``regen`` (chunked
stale-truth regeneration BEFORE the fingerprint, §14.2) → ``decide`` (input
fingerprint vs the last published run; the durable trigger-decision row lands
here, before any run row exists) →
``start`` (one transaction: prior incomplete attempts wiped, the new run
row pins config + roster + generation + period + seed) → ``simulate``
(chunked walk-forward evidence; each chunk first re-checks the pinned
inputs against the live tables) → ``resolve`` (§7 pass-1 availability-only
resolution, one chunk) → ``baseline`` (§7 pass 2: the all-feed-mean
baseline over the resolved headline roster, chunked like ``simulate``) →
``aggregate`` (cell resolution +
results, one transaction) → ``bootstrap`` (the ONLY async phase: series
prepared on a read connection, the CPU-bound bootstrap runs in
``asyncio.to_thread`` — never inside ``db.write`` — then one write
persists the verdicts) → ``pairwise`` (§9: below-floor rows gain their
comparison against the RECOMMENDED blend, one chunk, after the verdicts
exist) → ``publish`` (integrity check + atomic pointer
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
from wxverify.db.queue import ACTIVE_JOB_SQL, Job
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
    pass1_baseline_feeds,
    prepare_bootstrap_inputs,
    preskipped_verdicts,
    publish_verified_run,
    resolve_pass1_roster,
    write_pairwise_comparisons,
)
from wxverify.verification.read_cache import warm_read_cache
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
from wxverify.verification.simulate import simulate_baseline_day, simulate_snapshot_day
from wxverify.verification.truth import (
    materialize_missing_truth_days,
    regenerate_marked_truth_chunk,
)
from wxverify.worker.control import JobCancelled, JobContinuation

logger = logging.getLogger(__name__)

# Missing settled local days materialized per `discover` chunk
# (payload-overridable). Matches REGEN_CHUNK_GROUPS because the per-unit work
# is identical: one materialize_daily_truth call, dominated by a single scan
# of the site's observations. tz_correction's DAYS_PER_CHUNK = 14 is not the
# precedent — a correction day also rebuilds pairs, so it costs strictly more.
TRUTH_DISCOVERY_DAYS_PER_CHUNK = 20
# Stale (local day, generation) truth groups regenerated per chunk
# (payload-overridable, like tz_correction's days_per_chunk).
REGEN_CHUNK_GROUPS = 20
# Snapshot days simulated per chunk transaction (payload-overridable).
SNAPSHOT_DAYS_PER_CHUNK = 7
# Failed attempts on ONE fingerprint before the trigger stops retrying (§14:
# a third start without completion marks the effort failed, not re-pended).
MAX_FAILED_ATTEMPTS = 2
# §11/W8: re-decide rounds allowed when the fingerprint derived in `start`
# differs from the one the trigger decision was made against. The divergence
# drivers (`sample_high_water`, the observation count) advance continuously,
# so an unbounded loop would live-lock the chain and never start a run. On
# the round-2 divergence the run starts against its OWN freshly derived
# fingerprint and snapshot — a self-consistent pair, which is the property
# this item exists to guarantee. The published-fingerprint and attempt-cap
# gates are re-evaluated against the freshly derived fingerprint on both the
# re-decide branch and the forced-start branch; the settled-truth gate is
# enforced by `start_run` (runs.py).
MAX_REDECIDE_ATTEMPTS = 1
SUPERSEDED_REASON = "superseded: input fingerprint changed between decide and start"
FORCED_START_REASON = "started after 2 fingerprint re-derivations"

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


def verification_chain_active(conn: sqlite3.Connection, site_id: int) -> bool:
    """Whether a verification chain is queued or running for this site."""
    row = conn.execute(
        ACTIVE_JOB_SQL, ("verification_run", verification_job_key(site_id), site_id)
    ).fetchone()
    return row is not None


def any_verification_chain_active(conn: sqlite3.Connection) -> bool:
    """Whether any site has a queued or running verification chain."""
    return any(
        verification_chain_active(conn, int(row["id"]))
        for row in conn.execute("SELECT id FROM sites").fetchall()
    )


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
    if more:
        return _continuation(site_id, payload)
    # Terminal chunk: `publish` has just moved the published-run pointer, so
    # the page would otherwise serve the new run with no cache entry. This is
    # a read and runs AFTER the write returned, never inside the transaction;
    # `warm_read_cache` never raises `Exception`, so it cannot fail the run.
    await warm_read_cache(db)
    return None


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
    # §3.2 mutate-and-re-save: `run_id` must survive this write.
    blob["phase"] = "pairwise"
    _save_state(conn, site_id, blob)
    _heartbeat(conn, site_id)


def advance_verification(
    conn: sqlite3.Connection, site_id: int, payload: dict[str, object]
) -> bool:
    """Execute one SYNC chain chunk inside the caller's write transaction."""
    loaded = _load_state(conn, site_id)
    blob: dict[str, object] = {"phase": "discover"} if loaded is None else loaded
    phase = str(blob.get("phase", ""))
    if phase == "discover":
        site = conn.execute(
            "SELECT rain_threshold_mm FROM sites WHERE id = ?", (site_id,)
        ).fetchone()
        if site is None:
            # The ONE condition this phase cancels for (§14). Every other
            # fault propagates: chain-level to the retry ladder, per-day
            # contained inside materialize_missing_truth_days.
            raise JobCancelled()
        # Site-wide precondition, probed at chain level so a corrupt value is
        # a LOUD terminal failure instead of one contained ERROR per day of
        # the backlog. The value is NOT passed on: materialize_daily_truth
        # reads and uses its own.
        float(site["rain_threshold_mm"])
        limit = _chunk_size(
            payload, "truth_discovery_days", TRUTH_DISCOVERY_DAYS_PER_CHUNK
        )
        cursor = blob.get("truth_cursor")
        attempted = materialize_missing_truth_days(
            conn,
            site_id=site_id,
            now=utc_now(),
            limit=limit,
            after_local_date=cursor if isinstance(cursor, str) else None,
        )
        if len(attempted) < limit:
            blob.pop("truth_cursor", None)
            blob["phase"] = "regen"
        else:
            blob["truth_cursor"] = attempted[-1]
        _save_state(conn, site_id, blob)
        _heartbeat(conn, site_id)
        return True
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
        return _start_phase(conn, site_id, payload, blob)
    if phase == "simulate":
        return _simulate_chunk(conn, site_id, payload, blob)
    if phase == "resolve":
        return _resolve_phase(conn, site_id, blob)
    if phase == "baseline":
        return _baseline_chunk(conn, site_id, payload, blob)
    if phase == "aggregate":
        cfg = _blob_config(conn, blob)
        aggregate_run(conn, cfg)
        blob["phase"] = "bootstrap"
        _save_state(conn, site_id, blob)
        _heartbeat(conn, site_id)
        return True
    if phase == "pairwise":
        cfg = _blob_config(conn, blob)
        write_pairwise_comparisons(conn, cfg)
        # §3.2 mutate-and-re-save, NOT a fresh dict: dropping `run_id` here
        # would wedge the chain silently on every subsequent night.
        blob["phase"] = "publish"
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


def _blocking_gate(
    conn: sqlite3.Connection, site_id: int, fingerprint: str
) -> tuple[str, str] | None:
    """The (decision, reason) of the first blocking pre-start gate, or None."""
    if published_fingerprint(conn, site_id) == fingerprint:
        return ("no_change_skip", "input fingerprint matches the published run")
    if (
        failed_attempts_for_fingerprint(conn, site_id, fingerprint)
        >= MAX_FAILED_ATTEMPTS
    ):
        return ("skipped", "attempt cap reached for this fingerprint")
    return None


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
    blocked = _blocking_gate(conn, site_id, fingerprint)
    if blocked is not None:
        decision, reason = blocked
        record_trigger_decision(
            conn,
            site_id,
            trigger_date=trigger_date,
            decision=decision,
            reason=reason,
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
    # `redecide_attempts` is the ONE key that crosses the start → decide
    # boundary (§3.2), and this fresh dict would otherwise destroy it on the
    # way back into `start`, unbounding the re-decide loop.
    _save_state(
        conn,
        site_id,
        {
            "phase": "start",
            "fingerprint": fingerprint,
            "decision_id": int(decision_id["id"]),
            "redecide_attempts": _blob_int(blob, "redecide_attempts") or 0,
        },
    )
    # Informational only (§11): what the trigger decision was made against.
    # It is NEVER the comparand — `_continuation` enqueues after the state
    # write commits, so a crash-resume replays the ORIGINAL scheduler
    # payload and a payload-carried comparand would read as a divergence
    # that never happened.
    payload["decision_fingerprint"] = fingerprint
    _heartbeat(conn, site_id)
    return True


def _start_phase(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    blob: dict[str, object],
) -> bool:
    decided = str(blob.get("fingerprint", ""))
    if not decided:
        raise JobCancelled()
    trigger_date = payload.get("trigger_date")
    if not isinstance(trigger_date, str):
        raise JobCancelled()
    try:
        snapshot = capture_config_snapshot(conn, site_id)
    except ValueError as exc:
        raise JobCancelled() from exc
    # §11/W8: the run's fingerprint is derived from the snapshot the run
    # actually stores, in this transaction — never the decide-phase value
    # computed against a different sample high-water mark.
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    attempts = _blob_int(blob, "redecide_attempts") or 0
    diverged = fingerprint != decided
    if diverged and attempts < MAX_REDECIDE_ATTEMPTS:
        # No run starts on a divergent pass. Re-entering `decide` re-runs
        # every gate (published-fingerprint, attempt cap, settled truth)
        # against the new value; the superseding row keeps the audit trail
        # honest, since the round-1 `run_started` row's run_id never lands.
        record_trigger_decision(
            conn,
            site_id,
            trigger_date=trigger_date,
            decision="skipped",
            reason=SUPERSEDED_REASON,
            fingerprint=fingerprint,
        )
        _save_state(
            conn, site_id, {"phase": "decide", "redecide_attempts": attempts + 1}
        )
        _heartbeat(conn, site_id)
        return True
    # §11/W8: the forced start reaches `start_run` without ever re-entering
    # `decide`, so the two gates `decide` owns are re-evaluated here against
    # the freshly derived fingerprint. The bail writes NO state dict (§3.2):
    # `_clear_state` is the same terminal action every `decide` gate takes.
    blocked = _blocking_gate(conn, site_id, fingerprint)
    if blocked is not None:
        decision, reason = blocked
        record_trigger_decision(
            conn,
            site_id,
            trigger_date=trigger_date,
            decision=decision,
            reason=reason,
            fingerprint=fingerprint,
        )
        _clear_state(conn, site_id)
        return False
    cfg = start_run(
        conn, site_id, snapshot=snapshot, fingerprint=fingerprint, now=utc_now()
    )
    if cfg is None:
        # Settled truth vanished between decide and start (regeneration
        # race) — fail loudly rather than publish an empty run.
        raise RuntimeError(f"verification start for site={site_id}: no settled truth")
    decision_id = _blob_int(blob, "decision_id")
    if decision_id is not None and diverged:
        conn.execute(
            """
            UPDATE verification_trigger_decisions
            SET run_id = ?, reason = ?, input_fingerprint = ?
            WHERE id = ?
            """,
            (cfg.run_id, FORCED_START_REASON, fingerprint, decision_id),
        )
    elif decision_id is not None:
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
        blob["phase"] = "resolve"
        blob.pop("cursor", None)
    else:
        blob["cursor"] = cursor.isoformat()
    # MUTATE-AND-RE-SAVE (§3.2): a fresh dict here drops `run_id`, and the
    # chain then cancels silently on every subsequent night with no run.
    _save_state(conn, site_id, blob)
    _heartbeat(conn, site_id)
    return True


def _resolve_phase(
    conn: sqlite3.Connection, site_id: int, blob: dict[str, object]
) -> bool:
    """§7 steps 2-3: availability-only resolution, one bounded chunk."""
    cfg = _blob_config(conn, blob)
    resolve_pass1_roster(conn, cfg)
    blob["phase"] = "baseline"
    blob["cursor"] = cfg.period_start
    _save_state(conn, site_id, blob)
    _heartbeat(conn, site_id)
    return True


def _baseline_chunk(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    blob: dict[str, object],
) -> bool:
    """§7 step 4: pass 2, chunked over snapshot days exactly as pass 1 is."""
    cfg = _blob_config(conn, blob)
    assert_inputs_unpinned_unchanged(conn, cfg)
    rosters = pass1_baseline_feeds(conn, cfg.run_id)
    days = _chunk_size(payload, "snapshot_days_per_chunk", SNAPSHOT_DAYS_PER_CHUNK)
    cursor = date.fromisoformat(str(blob["cursor"]))
    end = date.fromisoformat(cfg.period_end)
    for _ in range(days):
        if cursor > end:
            break
        simulate_baseline_day(conn, cfg, cursor.isoformat(), rosters)
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
