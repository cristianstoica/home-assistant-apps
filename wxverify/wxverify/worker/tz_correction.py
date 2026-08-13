"""Resumable ``timezone_correction`` job chain (§13 / §14).

One claimed job executes exactly ONE chunk — a single short write
transaction — then returns a :class:`JobContinuation` so the worker loop
re-enqueues the chain and other job types get scheduled between chunks
(``claim_next_job``'s priority tier lets ``forecast_record`` /
``record_gap_scan`` claim ahead of chain chunks).

Chain state lives in a single ``runtime_state`` JSON blob
(``tz_correction_state:<generation>``), rewritten INSIDE the same
transaction as the chunk's work — resume after a crash re-executes at most
one chunk, and every chunk is idempotent (delete-and-recreate per local
day). A progress heartbeat (``tz_correction_heartbeat:<generation>``) is
refreshed each chunk.

Phases: ``start`` (chain-bound attempt accounting per §14 — attempt++,
first action wipes the prior incomplete attempt's building rows; a third
start without completion marks the generation ``failed`` and cancels) →
``days`` (chunked whole-history rebuild under the building generation,
accumulating §13 reconciliation counts exactly once per day) → ``rescan``
(days whose consensus observations mutated during the build, bounded
passes) → ``flip`` (atomic: final residue rebuild, counts persisted,
building→published, ALL prior published generations→retired, pointer flip,
site ``score_cache`` delete, ``sites.timezone`` update, rescore enqueue) →
``cleanup`` (chunked deletion of retired-generation rows, strictly after
the flip).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, timedelta
from typing import cast

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.queue import Job, enqueue_if_absent
from wxverify.db.runtime_state import (
    delete_runtime_state,
    get_runtime_state,
    set_runtime_state,
    set_runtime_state_now,
)
from wxverify.db.tz_generations import (
    correction_job_key,
    published_generation_id,
    published_pointer_key,
)
from wxverify.scoring.tz_rebuild import (
    DayReconciliation,
    delete_building_rows,
    generation_day_range,
    mutated_local_days,
    rebuild_generation_day,
)
from wxverify.worker.control import JobCancelled, JobContinuation

logger = logging.getLogger(__name__)

# Local days rebuilt per chunk transaction (payload-overridable: qa's §18.7
# oracles inject 1 to force chunk boundaries deterministically).
DAYS_PER_CHUNK = 14
# Retired-generation rows deleted per cleanup chunk (per table).
CLEANUP_CHUNK_ROWS = 5000
# Rescan passes before the flip transaction absorbs any residue itself.
MAX_RESCAN_PASSES = 3
# Chain starts allowed before the run is marked failed (§14: "on the third
# chain start without completion the run is marked failed").
MAX_CHAIN_STARTS = 2

_STATE_KEY_PREFIX = "tz_correction_state:"
_HEARTBEAT_KEY_PREFIX = "tz_correction_heartbeat:"


def correction_state_key(generation_id: int) -> str:
    """``runtime_state`` key of the chain-state JSON blob."""
    return f"{_STATE_KEY_PREFIX}{generation_id}"


def correction_heartbeat_key(generation_id: int) -> str:
    """``runtime_state`` key of the chain progress heartbeat."""
    return f"{_HEARTBEAT_KEY_PREFIX}{generation_id}"


def payload_generation_id(payload: dict[str, object]) -> int | None:
    """The ``generation_id`` carried by a timezone_correction payload."""
    value = payload.get("generation_id")
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    return None


def mark_correction_failed(conn: sqlite3.Connection, job: Job) -> None:
    """Terminal-failure hook: a timezone_correction job going terminally
    failed marks its still-building generation ``failed`` (§14: a mid-chain
    chunk failure fails the run under the existing retry rules — the
    generation must not linger as ``building`` forever).
    """
    generation_id = payload_generation_id(job.payload)
    if generation_id is None:
        return
    cur = conn.execute(
        """
        UPDATE timezone_generations
        SET state = 'failed'
        WHERE id = ? AND state = 'building'
        """,
        (generation_id,),
    )
    if cur.rowcount:
        logger.error(
            "timezone correction generation=%s marked failed (terminal job)",
            generation_id,
        )


def advance_correction(
    conn: sqlite3.Connection, site_id: int, payload: dict[str, object]
) -> bool:
    """Execute one chain chunk; returns True when another chunk remains.

    Runs entirely inside the caller's single write transaction. Raises
    :class:`JobCancelled` when the chain cannot or must not proceed
    (generation gone / failed attempt cap).
    """
    generation_id = payload_generation_id(payload)
    if generation_id is None:
        raise JobCancelled()
    row = conn.execute(
        """
        SELECT state, timezone, mode FROM timezone_generations
        WHERE id = ? AND site_id = ?
        """,
        (generation_id, site_id),
    ).fetchone()
    if row is None or str(row["mode"]) != "retrospective_correction":
        raise JobCancelled()
    gen_state = str(row["state"])
    timezone = str(row["timezone"])
    if gen_state == "retired":
        raise JobCancelled()
    blob = _load_state(conn, generation_id)
    if blob is None or gen_state == "failed":
        # A chain start is legitimate only while the run can still be
        # (re)built: a fresh correction creates its generation as
        # 'building' with no blob, and a 'failed' run restarts under the
        # §14 attempt cap. Any other state reaching here is a stale
        # re-delivery — in particular 'published' with no blob is the
        # terminal crash window (final cleanup chunk committed and
        # dropped the blob, the job-completion write was lost, boot
        # reclaim re-pended the job). Starting would delete the LIVE
        # generation's rows and demote it to 'building' while the
        # pointer still serves it (§18.7); refuse instead. 'retired' is
        # already refused above for the same reason.
        if gen_state not in ("building", "failed"):
            raise JobCancelled()
        return _chain_start(conn, site_id, generation_id, timezone, blob)
    phase = str(blob.get("phase", ""))
    if gen_state == "published":
        # Only cleanup runs after the flip; anything else is a stale blob.
        if phase != "cleanup":
            raise JobCancelled()
        return _cleanup_chunk(conn, site_id, generation_id, blob, payload)
    if phase == "days":
        return _days_chunk(conn, site_id, generation_id, timezone, blob, payload)
    if phase == "rescan":
        return _rescan_chunk(conn, site_id, generation_id, timezone, blob, payload)
    if phase == "flip":
        return _flip(conn, site_id, generation_id, timezone, blob)
    if phase == "cleanup":
        # Crash window: flip committed but the generation row shows
        # 'published' next read — handled above; a 'building' row with a
        # cleanup blob cannot happen (same transaction). Defensive cancel.
        raise JobCancelled()
    raise RuntimeError(
        f"timezone correction generation={generation_id} has an unknown "
        f"chain phase {phase!r}"
    )


def _chain_start(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    timezone: str,
    blob: dict[str, object] | None,
) -> bool:
    attempts = _blob_int(blob or {}, "attempts", 0) + 1
    if attempts > MAX_CHAIN_STARTS:
        conn.execute(
            """
            UPDATE timezone_generations SET state = 'failed'
            WHERE id = ? AND state IN ('building', 'failed')
            """,
            (generation_id,),
        )
        _save_state(conn, generation_id, {"phase": "failed", "attempts": attempts})
        logger.error(
            "timezone correction generation=%s failed: chain start %d "
            "without completion",
            generation_id,
            attempts,
        )
        raise JobCancelled()
    # First action of a new attempt: remove the prior incomplete attempt's
    # evidence (§14), then restart from a clean building generation.
    delete_building_rows(conn, generation_id)
    conn.execute(
        """
        UPDATE timezone_generations
        SET state = 'building', examined_count = 0, changed_count = 0,
            unchanged_count = 0, excluded_count = 0
        WHERE id = ?
        """,
        (generation_id,),
    )
    day_range = generation_day_range(conn, site_id, timezone)
    scan_stamp = isoformat_utc()
    if day_range is None:
        # Nothing to rebuild: go straight to the flip on the next chunk.
        state: dict[str, object] = {
            "phase": "flip",
            "attempts": attempts,
            "scan_stamp": scan_stamp,
        }
    else:
        state = {
            "phase": "days",
            "attempts": attempts,
            "cursor": day_range[0].isoformat(),
            "end": day_range[1].isoformat(),
            "scan_stamp": scan_stamp,
            "scan_passes": 0,
        }
    _save_state(conn, generation_id, state)
    _heartbeat(conn, generation_id)
    return True


def _days_chunk(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    timezone: str,
    blob: dict[str, object],
    payload: dict[str, object],
) -> bool:
    cursor = date.fromisoformat(str(blob["cursor"]))
    end = date.fromisoformat(str(blob["end"]))
    chunk_days = _chunk_size(payload, "days_per_chunk", DAYS_PER_CHUNK)
    totals = DayReconciliation(0, 0, 0, 0)
    day = cursor
    for _ in range(chunk_days):
        if day > end:
            break
        result = rebuild_generation_day(
            conn,
            site_id=site_id,
            generation_id=generation_id,
            timezone=timezone,
            day=day,
            count=True,
        )
        totals = DayReconciliation(
            examined=totals.examined + result.examined,
            changed=totals.changed + result.changed,
            unchanged=totals.unchanged + result.unchanged,
            excluded=totals.excluded + result.excluded,
        )
        day = day + timedelta(days=1)
    conn.execute(
        """
        UPDATE timezone_generations
        SET examined_count = examined_count + ?,
            changed_count = changed_count + ?,
            unchanged_count = unchanged_count + ?,
            excluded_count = excluded_count + ?
        WHERE id = ?
        """,
        (
            totals.examined,
            totals.changed,
            totals.unchanged,
            totals.excluded,
            generation_id,
        ),
    )
    if day > end:
        blob["phase"] = "rescan"
        blob.pop("cursor", None)
        blob["pending"] = []
    else:
        blob["cursor"] = day.isoformat()
    _save_state(conn, generation_id, blob)
    _heartbeat(conn, generation_id)
    return True


def _rescan_chunk(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    timezone: str,
    blob: dict[str, object],
    payload: dict[str, object],
) -> bool:
    pending = [str(item) for item in _blob_list(blob, "pending")]
    chunk_days = _chunk_size(payload, "days_per_chunk", DAYS_PER_CHUNK)
    if pending:
        batch, rest = pending[:chunk_days], pending[chunk_days:]
        for day_str in batch:
            rebuild_generation_day(
                conn,
                site_id=site_id,
                generation_id=generation_id,
                timezone=timezone,
                day=date.fromisoformat(day_str),
                count=False,
            )
        blob["pending"] = rest
        _save_state(conn, generation_id, blob)
        _heartbeat(conn, generation_id)
        return True
    passes = _blob_int(blob, "scan_passes", 0)
    if passes >= MAX_RESCAN_PASSES:
        blob["phase"] = "flip"
        _save_state(conn, generation_id, blob)
        _heartbeat(conn, generation_id)
        return True
    new_stamp = isoformat_utc()
    mutated = mutated_local_days(conn, site_id, timezone, str(blob["scan_stamp"]))
    blob["scan_stamp"] = new_stamp
    blob["scan_passes"] = passes + 1
    if mutated:
        blob["pending"] = [day.isoformat() for day in mutated]
    else:
        blob["phase"] = "flip"
    _save_state(conn, generation_id, blob)
    _heartbeat(conn, generation_id)
    return True


def _flip(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    timezone: str,
    blob: dict[str, object],
) -> bool:
    """The §13 flip: one transaction, no half-flipped observable state.

    Writes serialize on the single write connection, so any consensus
    mutation since the last rescan committed before this transaction began;
    the residue it left is rebuilt here, inside the flip itself.
    """
    for day in mutated_local_days(conn, site_id, timezone, str(blob["scan_stamp"])):
        rebuild_generation_day(
            conn,
            site_id=site_id,
            generation_id=generation_id,
            timezone=timezone,
            day=day,
            count=False,
        )
    counts = conn.execute(
        """
        SELECT examined_count, changed_count, unchanged_count, excluded_count
        FROM timezone_generations WHERE id = ?
        """,
        (generation_id,),
    ).fetchone()
    if counts is None:
        raise JobCancelled()
    examined = int(counts["examined_count"])
    parts = (
        int(counts["changed_count"])
        + int(counts["unchanged_count"])
        + int(counts["excluded_count"])
    )
    if examined != parts:
        raise RuntimeError(
            f"timezone correction generation={generation_id} reconciliation "
            f"mismatch: examined={examined} != changed+unchanged+excluded={parts}"
        )
    previous = published_generation_id(conn, site_id)
    conn.execute(
        """
        UPDATE timezone_generations
        SET state = 'retired'
        WHERE site_id = ? AND state = 'published' AND id != ?
        """,
        (site_id, generation_id),
    )
    conn.execute(
        """
        UPDATE timezone_generations
        SET state = 'published', published_at = ?
        WHERE id = ?
        """,
        (isoformat_utc(), generation_id),
    )
    set_runtime_state(conn, published_pointer_key(site_id), str(generation_id))
    conn.execute("DELETE FROM score_cache WHERE site_id = ?", (site_id,))
    conn.execute("UPDATE sites SET timezone = ? WHERE id = ?", (timezone, site_id))
    enqueue_if_absent(conn, "pair_and_score", site_id, "score", {"site_id": site_id})
    _save_state(
        conn,
        generation_id,
        {"phase": "cleanup", "attempts": _blob_int(blob, "attempts", 1)},
    )
    _heartbeat(conn, generation_id)
    logger.info(
        "timezone correction generation=%s published for site=%s "
        "(previous=%s, timezone=%s, examined=%d)",
        generation_id,
        site_id,
        previous,
        timezone,
        examined,
    )
    return True


def _cleanup_chunk(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    blob: dict[str, object],
    payload: dict[str, object],
) -> bool:
    """Chunked post-flip deletion of the site's dead-generation rows —
    forecast_pairs AND daily_truth (including rows ``mark_daily_truth_stale``
    flagged on retired generations, dissolving the wasted-regen window).

    'failed' generations are swept alongside 'retired' ones: a failed
    attempt that never restarts would otherwise strand its building rows
    forever (a restart's ``delete_building_rows`` only runs if the chain
    starts again), and the truth regenerators already skip both states.
    """
    limit = _chunk_size(payload, "cleanup_chunk_rows", CLEANUP_CHUNK_ROWS)
    deleted = 0
    for table in ("forecast_pairs", "daily_truth"):
        cur = conn.execute(
            f"""
            DELETE FROM {table}
            WHERE id IN (
                SELECT t.id FROM {table} t
                JOIN timezone_generations tg ON tg.id = t.tz_generation_id
                WHERE t.site_id = ? AND tg.state IN ('retired', 'failed')
                LIMIT ?
            )
            """,
            (site_id, limit),
        )
        deleted += cur.rowcount
    if deleted:
        _save_state(conn, generation_id, blob)
        _heartbeat(conn, generation_id)
        return True
    delete_runtime_state(conn, correction_state_key(generation_id))
    delete_runtime_state(conn, correction_heartbeat_key(generation_id))
    logger.info(
        "timezone correction generation=%s cleanup complete site=%s",
        generation_id,
        site_id,
    )
    return False


def build_continuation(site_id: int, payload: dict[str, object]) -> JobContinuation:
    """The next-chunk continuation for a timezone_correction chain."""
    generation_id = payload_generation_id(payload)
    if generation_id is None:
        raise JobCancelled()
    return JobContinuation(
        job_type="timezone_correction",
        site_id=site_id,
        job_key=correction_job_key(generation_id),
        payload=dict(payload),
    )


def _load_state(
    conn: sqlite3.Connection, generation_id: int
) -> dict[str, object] | None:
    raw = get_runtime_state(conn, correction_state_key(generation_id))
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
    conn: sqlite3.Connection, generation_id: int, state: dict[str, object]
) -> None:
    set_runtime_state(
        conn,
        correction_state_key(generation_id),
        json.dumps(state, separators=(",", ":")),
    )


def _heartbeat(conn: sqlite3.Connection, generation_id: int) -> None:
    set_runtime_state_now(conn, correction_heartbeat_key(generation_id))


def _blob_int(blob: dict[str, object], key: str, default: int) -> int:
    value = blob.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return default
    return value


def _blob_list(blob: dict[str, object], key: str) -> list[object]:
    value = blob.get(key)
    if not isinstance(value, list):
        return []
    return cast(list[object], value)


def _chunk_size(payload: dict[str, object], key: str, default: int) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return default
    return value
