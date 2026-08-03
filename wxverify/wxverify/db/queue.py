"""SQLite-backed job queue."""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import timedelta
from typing import cast

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Job:
    id: int
    type: str
    site_id: int | None
    job_key: str | None
    payload: dict[str, object]
    status: str
    retry_count: int
    max_retries: int


@dataclass(frozen=True)
class EnqueueResult:
    created: bool
    job_id: int | None = None


@dataclass(frozen=True)
class FailDisposition:
    """Outcome of ``fail``: terminal 'failed' or a scheduled retry."""

    terminal: bool
    retry_count: int
    max_retries: int
    next_attempt_at: str | None


def _job_from_row(row: sqlite3.Row) -> Job:
    payload_raw = str(row["payload"])
    payload_obj: object = json.loads(payload_raw) if payload_raw else {}
    if not isinstance(payload_obj, dict):
        # Same "can't trust this payload" class as a JSON parse failure below:
        # a syntactically valid but wrong-shaped payload must not silently run
        # as {} (e.g. it would walk backfill_site straight into a window
        # clamp that can skip days). Raise so claim_next_job's disposition
        # catches it instead of substituting a default.
        raise TypeError("job payload is not a JSON object")
    payload = cast(dict[str, object], payload_obj)
    return Job(
        id=int(row["id"]),
        type=str(row["type"]),
        site_id=None if row["site_id"] is None else int(row["site_id"]),
        job_key=None if row["job_key"] is None else str(row["job_key"]),
        payload=payload,
        status=str(row["status"]),
        retry_count=int(row["retry_count"]),
        max_retries=int(row["max_retries"]),
    )


def enqueue_if_absent(
    conn: sqlite3.Connection,
    job_type: str,
    site_id: int | None,
    job_key: str,
    payload: dict[str, object] | None = None,
) -> EnqueueResult:
    params = (job_type, job_key, site_id, site_id)
    row = conn.execute(
        """
        SELECT id FROM jobs
        WHERE type = ?
          AND job_key = ?
          AND (site_id IS ? OR (site_id IS NULL AND ? IS NULL))
          AND status IN ('pending','running')
        LIMIT 1
        """,
        params,
    ).fetchone()
    if row is not None:
        logger.debug(
            "job enqueue deduped type=%s site=%s key=%s existing_id=%s",
            job_type,
            site_id,
            job_key,
            int(row["id"]),
        )
        return EnqueueResult(created=False, job_id=int(row["id"]))
    try:
        cur = conn.execute(
            """
            INSERT INTO jobs (type, site_id, job_key, payload, status, next_attempt_at)
            VALUES (?, ?, ?, ?, 'pending', ?)
            """,
            (
                job_type,
                site_id,
                job_key,
                json.dumps(payload or {}, separators=(",", ":")),
                isoformat_utc(),
            ),
        )
    except sqlite3.IntegrityError:
        return EnqueueResult(created=False)
    job_id = int(cur.lastrowid or 0)
    logger.debug(
        "job enqueued type=%s site=%s key=%s id=%s",
        job_type,
        site_id,
        job_key,
        job_id,
    )
    return EnqueueResult(created=True, job_id=job_id)


ACTIVE_JOB_SQL = """
    SELECT 1 FROM jobs
    WHERE type = ? AND job_key = ? AND site_id IS ?
      AND status IN ('pending','running')
    LIMIT 1
"""

LATEST_JOB_SQL = """
    SELECT status, updated_at FROM jobs
    WHERE type = ? AND job_key = ? AND site_id IS ?
    ORDER BY id DESC
    LIMIT 1
"""


def enqueue_if_absent_with_cooldown(
    conn: sqlite3.Connection,
    job_type: str,
    site_id: int | None,
    job_key: str,
    payload: dict[str, object] | None,
    *,
    cooldown: timedelta,
) -> EnqueueResult | None:
    """Like ``enqueue_if_absent``, but suppressed while the latest job for
    this (type, site_id, job_key) is a terminal failure inside ``cooldown``.

    Opt-in wrapper for automatic/scheduled call sites only (scheduler due-job
    checks, post-read rescore triggers) -- never call this from an
    operator-initiated path (an API route or CLI command): a manual retry
    must never be silently dropped by a stale cooldown. Those callers keep
    using ``enqueue_if_absent`` directly.

    The cooldown lookup (``LATEST_JOB_SQL``) only runs when no active row
    already exists: on the dedupe-hit path (the common case) this early-out
    skips just that query, not all extra work -- the wrapper's own
    ``ACTIVE_JOB_SQL`` probe plus ``enqueue_if_absent``'s own dedupe select
    still cost two index seeks. ``jobs(type, job_key, site_id, id)`` keeps
    each of those seeks an index seek rather than a scan (``id`` is
    monotonic, so ``ORDER BY id DESC LIMIT 1`` needs no separate sort step).
    """
    active = conn.execute(ACTIVE_JOB_SQL, (job_type, job_key, site_id)).fetchone()
    if active is None:
        latest = conn.execute(LATEST_JOB_SQL, (job_type, job_key, site_id)).fetchone()
        if latest is not None and str(latest["status"]) == "failed":
            updated_at = parse_utc(str(latest["updated_at"]))
            if utc_now() - updated_at < cooldown:
                logger.debug(
                    "enqueue suppressed type=%s site=%s key=%s"
                    " (terminal failure within cooldown)",
                    job_type,
                    site_id,
                    job_key,
                )
                return None
    return enqueue_if_absent(conn, job_type, site_id, job_key, payload)


def claim_next_job(conn: sqlite3.Connection) -> Job | None:
    """Claim and disposition exactly one pending row.

    Design goal: no unreadable row may ever be re-claimed. An unreadable
    ``jobs`` row (corrupt payload, or a BLOB surviving into a TEXT/INTEGER
    column) must never be handed to the worker, since it would fail the same
    way on every claim and drive the process into a restart loop rather than
    a single failed job -- so the disposition happens here, not in the
    dispatcher.
    """
    now = isoformat_utc()
    row = conn.execute(
        """
        UPDATE jobs
        SET status = 'running', updated_at = ?
        WHERE id = (
            SELECT id FROM jobs
            WHERE status = 'pending'
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            ORDER BY created_at, id
            LIMIT 1
        )
        RETURNING *
        """,
        (now, now),
    ).fetchone()
    if row is None:
        return None
    try:
        return _job_from_row(row)
    except (json.JSONDecodeError, TypeError, ValueError):
        # A row that can't be read back at all (corrupt payload, or a BLOB
        # surviving an INTEGER-affinity column) is dispositioned here, in the
        # same transaction as the claim; row["id"] stays safe to read even
        # when every other column is corrupt, since id is a rowid alias and
        # SQLite guarantees it's an integer. Never route this through
        # fail() — fail() reads retry_count/max_retries from the same row we
        # just declared unreadable, so corruption there could keep the row
        # re-claimed forever instead of going terminal.
        job_id = int(row["id"])
        logger.error("unreadable job row id=%s; marking failed", job_id, exc_info=True)
        conn.execute(
            "UPDATE jobs SET status='failed', last_error=?, updated_at=? WHERE id=?",
            ("unreadable job row", isoformat_utc(), job_id),
        )
        return None


def complete(conn: sqlite3.Connection, job_id: int, result: str | None = None) -> None:
    conn.execute(
        """
        UPDATE jobs
        SET status='completed', result=?, updated_at=?
        WHERE id=?
        """,
        (result, isoformat_utc(), job_id),
    )
    logger.debug("job row completed id=%s", job_id)


def fail(conn: sqlite3.Connection, job_id: int, error: str) -> FailDisposition | None:
    """Record a job failure; returns the disposition, or None if the job is gone."""
    row = conn.execute(
        "SELECT retry_count, max_retries FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    if row is None:
        return None
    retry_count = int(row["retry_count"]) + 1
    max_retries = int(row["max_retries"])
    if retry_count > max_retries:
        conn.execute(
            """
            UPDATE jobs
            SET status='failed', retry_count=?, last_error=?, updated_at=?
            WHERE id=?
            """,
            (retry_count, error, isoformat_utc(), job_id),
        )
        logger.debug(
            "job row failed id=%s retry=%s/%s", job_id, retry_count, max_retries
        )
        return FailDisposition(
            terminal=True,
            retry_count=retry_count,
            max_retries=max_retries,
            next_attempt_at=None,
        )
    delay = min(3600, 2 ** min(retry_count, 10))
    next_attempt = isoformat_utc(utc_now() + timedelta(seconds=delay))
    conn.execute(
        """
        UPDATE jobs
        SET status='pending', retry_count=?, last_error=?, next_attempt_at=?,
            updated_at=?
        WHERE id=?
        """,
        (retry_count, error, next_attempt, isoformat_utc(), job_id),
    )
    logger.debug("job row retry id=%s next=%s", job_id, next_attempt)
    return FailDisposition(
        terminal=False,
        retry_count=retry_count,
        max_retries=max_retries,
        next_attempt_at=next_attempt,
    )


def defer_job(conn: sqlite3.Connection, job_id: int, next_attempt_at: str) -> None:
    conn.execute(
        """
        UPDATE jobs
        SET status='pending', next_attempt_at=?, updated_at=?
        WHERE id=?
        """,
        (next_attempt_at, isoformat_utc(), job_id),
    )
    logger.debug("job row deferred id=%s next=%s", job_id, next_attempt_at)


def count_failed_jobs_older_than(conn: sqlite3.Connection, hours: int) -> int:
    cutoff = isoformat_utc(utc_now() - timedelta(hours=hours))
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM jobs
        WHERE status='failed'
          AND updated_at < ?
        """,
        (cutoff,),
    ).fetchone()
    return 0 if row is None else int(row["n"])


def purge_failed_jobs_older_than(conn: sqlite3.Connection, hours: int) -> int:
    cutoff = isoformat_utc(utc_now() - timedelta(hours=hours))
    cur = conn.execute(
        """
        DELETE FROM jobs
        WHERE status='failed'
          AND updated_at < ?
        """,
        (cutoff,),
    )
    logger.debug("job purge older_than_hours=%s rows=%s", hours, cur.rowcount)
    return cur.rowcount


def reclaim_all_stale(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        """
        UPDATE jobs
        SET status='pending', updated_at=?
        WHERE status='running'
        """,
        (isoformat_utc(),),
    )
    return cur.rowcount
