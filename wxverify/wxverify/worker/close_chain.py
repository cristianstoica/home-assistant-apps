"""The daily close's night chain: the close of each site-day and its logs.

Holds the chain's constants; the bodies of stages 2 to 8 (``day_close``
stages 2 to 6, ``day_publish`` stages 7 and 8) under the hand-off rule
(plan 8.1-8.3), entered through ``run_day_close`` and ``run_day_publish``;
the failure writes ``fail_attempt`` and ``fail_publication`` (9.1, 9.4) and
the hooks ``_fail_job`` calls for them; ``run_close_retention`` (8.4);
``publication_record``; and ``record_collection_try``, the writer of the
collection try log (11.2). Stage 1, ``enqueue_due_close_work`` and
``settle_interrupted_closes`` come in a later piece.

P4's four call points are always called through ``close_p4``, never
imported by name, so P4 binds them in that one module.

``worker/wind_days.py`` imports ``record_collection_try`` at load time, so
this module must never import ``wind_days`` at load time, directly or
through another module (nor ``processor`` or ``scheduler``, which do).
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final, Literal

from wxverify.core.timeutil import (
    isoformat_utc,
    isoformat_utc_micro,
    parse_utc,
    utc_now,
)
from wxverify.db.connection import Database, FencedWriter
from wxverify.db.queue import Job, complete, enqueue_if_absent
from wxverify.db.site_days import (
    RETRY_DELAY,
    DayRecord,
    DayState,
    closed_through,
    daily_close_on,
    day_record,
    fail_close,
    finish_close,
)
from wxverify.db.station_readings import prune_raw_readings
from wxverify.verification.completeness import close_admission_basis
from wxverify.verification.coverage import DayBounds, local_day_bounds
from wxverify.verification.truth import evaluate_day_window, write_close_truth
from wxverify.worker import close_p4
from wxverify.worker.control import JobCancelled
from wxverify.worker.day_values import (
    StationFill,
    day_presence,
    fill_gaps,
    write_hourly_values,
)

logger = logging.getLogger(__name__)

#: The longest a stage may wait in the queue, and how late after a due
#: instant the add-on still counts as running (plan 5.2, 8.3). Equal to
#: ``RETRY_DELAY``, so a late start never runs past the retry's instant.
CLOSE_LATE_LIMIT: Final = timedelta(hours=1)
#: How long the stage log and the try log keep a row (plan 11.6).
CLOSE_LOG_KEEP_DAYS: Final = 90

#: The gap-fill outcome and reason pairs with which the attempt goes on
#: (plan 5.3, 10.1). An allowlist: any other pair, a named outcome with
#: another reason included, fails the attempt.
_GAP_FILL_GOES_ON: Final[frozenset[tuple[str, str | None]]] = frozenset(
    {
        ("filled", None),
        ("nothing_to_fill", None),
        ("no_readings", None),
        ("not_on_day", None),
        ("held", "refused_access"),
        ("refused", "refused_access"),
        ("skipped", None),
    }
)

#: The 13 ``daily_truth`` columns of the truth version, per row (plan 11.4).
_TRUTH_VERSION_SQL: Final = (
    "SELECT quantity, value, eligible, exclusion_reason, covered_hours,"
    " expected_slots, peak_window_ok, wet_hours, dry_hours, rain_threshold_mm,"
    " day_start_utc, day_end_utc, timezone FROM daily_truth"
    " WHERE site_id = ? AND local_date = ? ORDER BY quantity"
)
_TRUTH_VERSION_FIELDS: Final = (
    "quantity",
    "value",
    "eligible",
    "exclusion_reason",
    "covered_hours",
    "expected_slots",
    "peak_window_ok",
    "wet_hours",
    "dry_hours",
    "rain_threshold_mm",
    "day_start_utc",
    "day_end_utc",
    "timezone",
)

PublicationState = Literal[
    "pending", "building", "published", "retry_due", "failed", "superseded"
]
_PUBLICATION_STATES: Final[tuple[PublicationState, ...]] = (
    "pending",
    "building",
    "published",
    "retry_due",
    "failed",
    "superseded",
)
_StageOutcome = Literal["completed", "failed", "superseded", "skipped"]


@dataclass(frozen=True, slots=True)
class PublicationRecord:
    """One ``close_publications`` row: a site's publication up to ``cutoff``.

    The states move ``pending`` -> ``building`` -> ``published``, or on a
    failure to ``retry_due`` and then ``failed``; an older cutoff passed by
    a newer open one ends ``superseded`` (plan 8.6, 9.4, 11.5).
    """

    site_id: int
    cutoff: date
    attempt: int
    state: PublicationState
    opened_at: datetime
    started_at: datetime | None
    retry_at: datetime | None
    finished_at: datetime | None
    publication_id: str | None
    last_failure: str | None


@dataclass(frozen=True, slots=True)
class _StageJob:
    """A stage job's identity, from its key and payload (plan 8.1).

    ``local_date`` is the site-day for stages 2 to 6 and the publication's
    cutoff for stages 7 and 8; ``attempt`` the close attempt or the
    publication attempt.
    """

    job_id: int
    site_id: int
    local_date: date
    stage: int
    attempt: int


def record_collection_try(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: str,
    kind: str,
    slot: str,
    close_attempt: int | None,
    tried_at: str,
    outcome: str,
    reason: str | None,
    detail: str | None,
    readings_kept: int,
    readings_refused: int,
) -> None:
    """Insert one ``close_try_log`` row for a weather.com collection try.

    One row per try of a station's site-day: P2's daytime and evening
    5-minute tries (``kind`` ``five_minute``, ``slot`` ``daytime`` or the
    evening slot as local ``HH:MM``, ``close_attempt`` None) and the close's
    gap fill (``kind`` ``hourly``, ``slot`` ``gap_fill``, ``close_attempt``
    1 or 2). ``local_date`` is the day as ``YYYY-MM-DD`` and ``tried_at``
    when the try ended, in ``isoformat_utc_micro``'s form. ``outcome``,
    ``reason`` and ``detail`` are stored as given; ``readings_kept`` counts
    the readings stored and ``readings_refused`` those the closed-day guard
    refused.

    Does one INSERT and nothing else: it neither commits nor reads whether
    the close is on, so the log is stored from install, and it runs in the
    caller's transaction. A row that breaks a CHECK of the table raises
    ``sqlite3.IntegrityError`` unchanged.
    """
    conn.execute(
        "INSERT INTO close_try_log (site_id, local_date, station_id, kind,"
        " slot, close_attempt, tried_at, outcome, reason, detail,"
        " readings_kept, readings_refused)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            local_date,
            station_id,
            kind,
            slot,
            close_attempt,
            tried_at,
            outcome,
            reason,
            detail,
            readings_kept,
            readings_refused,
        ),
    )


# --- records ---------------------------------------------------------------


def _as_publication_state(value: object) -> PublicationState:
    for state in _PUBLICATION_STATES:
        if value == state:
            return state
    raise ValueError(f"unknown publication state {value!r}")


def _instant_or_none(value: object) -> datetime | None:
    return None if value is None else parse_utc(str(value))


def publication_record(
    conn: sqlite3.Connection, *, site_id: int, cutoff: date
) -> PublicationRecord | None:
    """The publication record of ``site_id`` and ``cutoff``, or None (11.5).

    Read-only; runs in the caller's transaction.
    """
    row = conn.execute(
        "SELECT attempt, state, opened_at, started_at, retry_at, finished_at,"
        " publication_id, last_failure FROM close_publications"
        " WHERE site_id = ? AND cutoff = ?",
        (site_id, cutoff.isoformat()),
    ).fetchone()
    if row is None:
        return None
    return PublicationRecord(
        site_id=site_id,
        cutoff=cutoff,
        attempt=int(row[0]),
        state=_as_publication_state(row[1]),
        opened_at=parse_utc(str(row[2])),
        started_at=_instant_or_none(row[3]),
        retry_at=_instant_or_none(row[4]),
        finished_at=_instant_or_none(row[5]),
        publication_id=None if row[6] is None else str(row[6]),
        last_failure=None if row[7] is None else str(row[7]),
    )


# --- keys, jobs and the stage log ------------------------------------------


def _close_key(site_id: int, local_date: date, stage: int) -> str:
    return f"close:{site_id}:{local_date.isoformat()}:{stage}"


def _enqueue_close_stage(
    conn: sqlite3.Connection, site_id: int, local_date: date, stage: int, attempt: int
) -> None:
    """Enqueue one ``day_close`` stage; the queue never retries it (8.1)."""
    enqueue_if_absent(
        conn,
        "day_close",
        site_id,
        _close_key(site_id, local_date, stage),
        {"attempt": attempt},
        max_retries=0,
    )


def _enqueue_publish_stage(
    conn: sqlite3.Connection, site_id: int, cutoff: date, stage: int, attempt: int
) -> None:
    """Enqueue one ``day_publish`` stage; the queue never retries it (8.1)."""
    enqueue_if_absent(
        conn,
        "day_publish",
        site_id,
        f"publish:{site_id}:{stage}",
        {"cutoff": cutoff.isoformat(), "attempt": attempt},
        max_retries=0,
    )


def _stage_job(job: Job) -> _StageJob | None:
    """The job's site, date, stage and attempt; None when malformed (8.1)."""
    attempt = job.payload.get("attempt")
    if job.site_id is None or job.job_key is None:
        return None
    if not isinstance(attempt, int) or isinstance(attempt, bool):
        return None
    if attempt not in (1, 2):
        return None
    parts = job.job_key.split(":")
    try:
        if job.type == "day_close" and len(parts) == 4 and parts[0] == "close":
            site_id, local_date, stage = int(parts[1]), parts[2], int(parts[3])
            valid = 2 <= stage <= 6
        elif job.type == "day_publish" and len(parts) == 3 and parts[0] == "publish":
            site_id, stage = int(parts[1]), int(parts[2])
            local_date = job.payload.get("cutoff")
            valid = stage in (7, 8)
        else:
            return None
        if not valid or site_id != job.site_id or not isinstance(local_date, str):
            return None
        day = date.fromisoformat(local_date)
    except ValueError:
        return None
    return _StageJob(job.id, site_id, day, stage, attempt)


def _job_stamps(
    conn: sqlite3.Connection, job_id: int, now_utc: datetime
) -> tuple[str, str]:
    """The job row's ``created_at`` and claim-time ``updated_at`` (11.1).

    Read before the queue writes the job's outcome, which overwrites
    ``updated_at``. A row no longer there gives ``now_utc`` for both.
    """
    row = conn.execute(
        "SELECT created_at, updated_at FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    if row is None:
        stamp = isoformat_utc_micro(now_utc)
        return stamp, stamp
    return str(row[0]), str(row[1])


def _stalled(enqueued_at: str, started_at: str) -> bool:
    """Whether the job was claimed more than ``CLOSE_LATE_LIMIT`` late (8.3)."""
    return parse_utc(started_at) - parse_utc(enqueued_at) > CLOSE_LATE_LIMIT


def _write_stage_row(
    conn: sqlite3.Connection,
    job: _StageJob,
    stamps: tuple[str, str],
    *,
    now_utc: datetime,
    outcome: _StageOutcome,
    reason: str | None = None,
    detail: dict[str, object] | None = None,
) -> None:
    """Insert one ``close_stage_log`` row for a stage 2 to 8 job (11.1).

    ``enqueued_at`` and ``started_at`` are the job row's stamps as stored;
    ``ended_at`` is ``now_utc``, when the completing or failing write runs.
    """
    enqueued_at, started_at = stamps
    started = parse_utc(started_at)
    conn.execute(
        "INSERT INTO close_stage_log (site_id, local_date, attempt, stage,"
        " job_id, enqueued_at, started_at, ended_at, queue_wait_s, duration_s,"
        " outcome, reason, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            job.site_id,
            job.local_date.isoformat(),
            job.attempt,
            job.stage,
            job.job_id,
            enqueued_at,
            started_at,
            isoformat_utc_micro(now_utc),
            (started - parse_utc(enqueued_at)).total_seconds(),
            (now_utc - started).total_seconds(),
            outcome,
            reason,
            json.dumps(detail or {}, sort_keys=True),
        ),
    )


def _require_close_on(conn: sqlite3.Connection, job: Job) -> None:
    """End a stray stage job with one WARNING while the close is off (13.1)."""
    if not daily_close_on(conn):
        logger.warning(
            "daily close is off: %s job %s of site %s not run",
            job.type,
            job.id,
            job.site_id,
        )
        raise JobCancelled()


# --- the hand-off rule, stages 2 to 6 ---------------------------------------


@dataclass(frozen=True, slots=True)
class _Running:
    """A stage 2 to 6 job cleared to run: its day and attempt state (8.2)."""

    record: DayRecord
    truth_version: str | None
    stamps: tuple[str, str]


def _attempt_progress(
    conn: sqlite3.Connection, job: _StageJob
) -> tuple[int, str | None] | None:
    """The attempt record's ``last_stage`` and ``truth_version`` (11.3)."""
    row = conn.execute(
        "SELECT last_stage, truth_version FROM close_attempts"
        " WHERE site_id = ? AND local_date = ? AND attempt = ?",
        (job.site_id, job.local_date.isoformat(), job.attempt),
    ).fetchone()
    if row is None:
        return None
    return int(row[0]), None if row[1] is None else str(row[1])


def _current(
    conn: sqlite3.Connection, job: _StageJob
) -> tuple[DayRecord, int, str | None] | None:
    """The day and its attempt progress when the job is current, else None.

    Current: the day is ``closing`` with ``attempts`` equal to the job's
    attempt (8.2). The day's state is read before the attempt record, so a
    stale job is never taken for a repeated one.
    """
    record = day_record(conn, job.site_id, job.local_date)
    if record is None or record.state != "closing" or record.attempts != job.attempt:
        return None
    progress = _attempt_progress(conn, job)
    if progress is None:
        raise RuntimeError("the closing attempt has no attempt record")
    return record, progress[0], progress[1]


def _open_stage(
    conn: sqlite3.Connection, raw: Job, job: _StageJob, now_utc: datetime
) -> _Running | None:
    """Steps 1 and 2 of the hand-off rule for a stage 2 to 6 job (8.2, 8.3).

    None when the stage must not run: a stale job (nothing written), a
    repeated one (only its ``skipped`` stage-log row written, the attempt
    record left as it is), or one claimed too late (the attempt failed as
    ``stalled``).
    """
    _require_close_on(conn, raw)
    current = _current(conn, job)
    if current is None:
        return None
    record, last_stage, truth_version = current
    stamps = _job_stamps(conn, job.job_id, now_utc)
    if last_stage >= job.stage:
        _write_stage_row(conn, job, stamps, now_utc=now_utc, outcome="skipped")
        return None
    if _stalled(*stamps):
        _fail_stage(conn, job, "stalled", now_utc)
        return None
    return _Running(record, truth_version, stamps)


def _fail_stage(
    conn: sqlite3.Connection, job: _StageJob, reason: str, now_utc: datetime
) -> None:
    fail_attempt(
        conn,
        site_id=job.site_id,
        local_date=job.local_date,
        attempt=job.attempt,
        stage=job.stage,
        reason=reason,
        now_utc=now_utc,
        job_id=job.job_id,
    )


def _complete_stage(
    conn: sqlite3.Connection,
    job: _StageJob,
    stamps: tuple[str, str],
    *,
    now_utc: datetime,
    detail: dict[str, object] | None = None,
) -> None:
    """Step 4 for stages 2 to 5: save the completion, enqueue the next (8.2)."""
    _write_stage_row(
        conn, job, stamps, now_utc=now_utc, outcome="completed", detail=detail
    )
    conn.execute(
        "UPDATE close_attempts SET last_stage = ?, last_progress_at = ?"
        " WHERE site_id = ? AND local_date = ? AND attempt = ?",
        (
            job.stage,
            isoformat_utc_micro(now_utc),
            job.site_id,
            job.local_date.isoformat(),
            job.attempt,
        ),
    )
    _enqueue_close_stage(conn, job.site_id, job.local_date, job.stage + 1, job.attempt)


# --- stage 2: the gap check and fill ----------------------------------------


def _paused_station_ids(
    conn: sqlite3.Connection, *, site_id: int, local_date: date
) -> frozenset[int]:
    """The stations paused for the day's gap fill (plan 5.6).

    The nightly gap-fill pause is a later piece of P3; until it lands, no
    station is paused.
    """
    return frozenset()


def _fill_label(fill: StationFill) -> str:
    return fill.outcome if fill.reason is None else f"{fill.outcome}/{fill.reason}"


def _gap_fill_failure(ended: bool, stations: dict[int, StationFill]) -> str | None:
    """Why the gap fill fails the attempt, or None when it goes on (5.3, 8.3).

    Sorted by outcome AND reason against an allowlist. The reason names the
    failing outcomes, never a station.
    """
    if not ended:
        return "gap fill ran before the day's end"
    failing = sorted(
        {
            _fill_label(fill)
            for fill in stations.values()
            if (fill.outcome, fill.reason) not in _GAP_FILL_GOES_ON
        }
    )
    return "gap fill " + ", ".join(failing) if failing else None


def _write_presence(
    conn: sqlite3.Connection, job: _StageJob, paused: frozenset[int]
) -> None:
    """Replace the day's station presence records (plan 5.6, 11.3)."""
    local_date = job.local_date.isoformat()
    conn.execute(
        "DELETE FROM close_station_days WHERE site_id = ? AND local_date = ?",
        (job.site_id, local_date),
    )
    presence = day_presence(conn, site_id=job.site_id, local_date=job.local_date)
    for station_id, station in presence.items():
        counts = station.readings
        refused_lost = (
            sum(counts.values()) == 0
            and not station.answered
            and station.refused_at is not None
        )
        conn.execute(
            "INSERT INTO close_station_days (site_id, local_date, station_id,"
            " five_minute, current_obs, hourly, answered, refused_lost, paused)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                job.site_id,
                local_date,
                station_id,
                counts["five_minute"],
                counts["current_obs"],
                counts["hourly"],
                int(station.answered),
                int(refused_lost),
                int(station_id in paused),
            ),
        )


async def _gap_fill_stage(
    db: Database, writer: FencedWriter, raw: Job, job: _StageJob
) -> None:
    """Stage 2: the gap fill, then its try rows and presence records (8.1).

    Not one transaction: ``fill_gaps`` makes its calls between its own
    writes (8.1). An exception from it propagates and fails the attempt.
    """

    def begin(conn: sqlite3.Connection) -> frozenset[int] | None:
        if _open_stage(conn, raw, job, utc_now()) is None:
            return None
        return _paused_station_ids(conn, site_id=job.site_id, local_date=job.local_date)

    paused = await writer.write(begin)
    if paused is None:
        return
    result = await fill_gaps(
        db,
        writer,
        site_id=job.site_id,
        local_date=job.local_date,
        now_utc=utc_now(),
        skip_station_ids=paused,
    )
    stations = dict(sorted(result.stations.items()))

    def finish(conn: sqlite3.Connection) -> None:
        now = utc_now()
        current = _current(conn, job)
        if current is None or current[1] >= job.stage:
            return
        for station_id, fill in stations.items():
            record_collection_try(
                conn,
                site_id=job.site_id,
                station_id=station_id,
                local_date=job.local_date.isoformat(),
                kind="hourly",
                slot="gap_fill",
                close_attempt=job.attempt,
                tried_at=isoformat_utc_micro(now),
                outcome=fill.outcome,
                reason=fill.reason,
                detail=fill.detail,
                readings_kept=fill.readings_kept,
                readings_refused=fill.readings_refused,
            )
        failure = _gap_fill_failure(result.ended, stations)
        if failure is not None:
            _fail_stage(conn, job, failure, now)
            return
        _write_presence(conn, job, paused)
        detail: dict[str, object] = {
            "stations": [
                {"station_id": sid, "outcome": fill.outcome, "reason": fill.reason}
                for sid, fill in stations.items()
            ]
        }
        stamps = _job_stamps(conn, job.job_id, now)
        _complete_stage(conn, job, stamps, now_utc=now, detail=detail)

    await writer.write(finish)


# --- stages 3 to 5: one transaction each ------------------------------------


def _hourly_values_stage(conn: sqlite3.Connection, raw: Job, job: _StageJob) -> None:
    """Stage 3: the day's hourly values and site hours (8.1, 8.3)."""
    now = utc_now()
    running = _open_stage(conn, raw, job, now)
    if running is None:
        return
    result = write_hourly_values(
        conn, site_id=job.site_id, local_date=job.local_date, now_utc=now
    )
    if result.outcome == "refused":
        _fail_stage(conn, job, "hourly values refused by the closed-day guard", now)
        return
    _complete_stage(conn, job, running.stamps, now_utc=now)


def _truth_version(conn: sqlite3.Connection, site_id: int, local_date: date) -> str:
    """SHA-256 of the day's ``daily_truth`` rows as JSON (plan 11.4).

    One JSON array of the rows in ``quantity`` order, each an object of the
    13 columns, with sorted keys and no spaces.
    """
    rows = conn.execute(_TRUTH_VERSION_SQL, (site_id, local_date.isoformat()))
    payload = [dict(zip(_TRUTH_VERSION_FIELDS, row, strict=True)) for row in rows]
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()


def _truth_stage(conn: sqlite3.Connection, raw: Job, job: _StageJob) -> None:
    """Stage 4: the verdict, the bounds check and the truth (6.1, 11.4)."""
    now = utc_now()
    running = _open_stage(conn, raw, job, now)
    if running is None:
        return
    record = running.record
    zone_bounds = local_day_bounds(record.local_date, record.timezone)
    if (zone_bounds.start_utc, zone_bounds.end_utc) != (
        record.day_start_utc,
        record.day_end_utc,
    ):
        _fail_stage(conn, job, "the day's bounds differ from its zone's day", now)
        return
    bounds = DayBounds(
        local_date=record.local_date,
        timezone=record.timezone,
        start_utc=record.day_start_utc,
        end_utc=record.day_end_utc,
        expected_slots=zone_bounds.expected_slots,
    )
    evaluation = evaluate_day_window(
        conn,
        site_id=job.site_id,
        local_date=record.local_date,
        timezone=record.timezone,
        bounds=bounds,
    )
    write_close_truth(
        conn,
        evaluation,
        site_id=job.site_id,
        admission_basis=close_admission_basis(evaluation),
        now_utc=now,
    )
    conn.execute(
        "UPDATE close_attempts SET truth_version = ?"
        " WHERE site_id = ? AND local_date = ? AND attempt = ?",
        (
            _truth_version(conn, job.site_id, job.local_date),
            job.site_id,
            job.local_date.isoformat(),
            job.attempt,
        ),
    )
    _complete_stage(conn, job, running.stamps, now_utc=now)


def _score_stage(conn: sqlite3.Connection, raw: Job, job: _StageJob) -> None:
    """Stage 5: P4 scores the day in this transaction (plan 2.3, 8.3)."""
    now = utc_now()
    running = _open_stage(conn, raw, job, now)
    if running is None:
        return
    if running.truth_version is None:
        raise RuntimeError("stage 4 saved no truth version for the attempt")
    summary = close_p4.score_day(
        conn,
        record=running.record,
        attempt=job.attempt,
        truth_version=running.truth_version,
        now_utc=now,
    )
    detail: dict[str, object] = {
        "scored": dict(summary.scored),
        "absent": dict(summary.absent),
    }
    _complete_stage(conn, job, running.stamps, now_utc=now, detail=detail)


# --- stage 6: the closure ----------------------------------------------------


def _closure(conn: sqlite3.Connection, raw: Job, job: _StageJob) -> bool:
    """Stage 6's one write: the closure, its publication, stage 7 (8.1, 8.6).

    ``finish_close``, the attempt closed, the stage-log row, the publication
    record of cutoff ``closed_through`` opened and stage 7 enqueued all
    commit together or not at all. True when the day was closed.
    """
    now = utc_now()
    running = _open_stage(conn, raw, job, now)
    if running is None:
        return False
    finish_close(conn, job.site_id, job.local_date, now_utc=now)
    stamp = isoformat_utc_micro(now)
    conn.execute(
        "UPDATE close_attempts SET outcome = 'closed', last_stage = 6,"
        " last_progress_at = ?, ended_at = ?"
        " WHERE site_id = ? AND local_date = ? AND attempt = ?",
        (stamp, stamp, job.site_id, job.local_date.isoformat(), job.attempt),
    )
    _write_stage_row(conn, job, running.stamps, now_utc=now, outcome="completed")
    cutoff = closed_through(conn, job.site_id)
    if cutoff is None:
        raise RuntimeError("no closed day after finish_close")
    conn.execute(
        "INSERT INTO close_publications (site_id, cutoff, attempt, state,"
        " opened_at) VALUES (?, ?, 1, 'pending', ?)",
        (job.site_id, cutoff.isoformat(), stamp),
    )
    _enqueue_publish_stage(conn, job.site_id, cutoff, 7, 1)
    return True


async def _closure_stage(writer: FencedWriter, raw: Job, job: _StageJob) -> None:
    """Stage 6: the closure, then raw retention in a write of its own (8.4)."""
    if await writer.write(lambda conn: _closure(conn, raw, job)):
        await writer.write(lambda conn: run_close_retention(conn, now_utc=utc_now()))


# --- stages 7 and 8: the publication -----------------------------------------


def _newer_open_publication(conn: sqlite3.Connection, job: _StageJob) -> bool:
    """Whether a later cutoff of the site has an open record (plan 8.6)."""
    row = conn.execute(
        "SELECT 1 FROM close_publications WHERE site_id = ? AND cutoff > ?"
        " AND state IN ('pending', 'building') LIMIT 1",
        (job.site_id, job.local_date.isoformat()),
    ).fetchone()
    return row is not None


def _begin_publication(conn: sqlite3.Connection, raw: Job, job: _StageJob) -> bool:
    """Stage 7's short first write: the checks, then ``building`` (8.2, 9.4).

    False, with nothing written, when the job's record is not open (its
    cutoff, its attempt, ``pending`` or ``building``). An older cutoff with
    a newer open record ends ``superseded``, with no call to P4. A job
    claimed too late fails the publication as ``stalled`` (8.3).
    """
    now = utc_now()
    _require_close_on(conn, raw)
    record = publication_record(conn, site_id=job.site_id, cutoff=job.local_date)
    if (
        record is None
        or record.attempt != job.attempt
        or record.state not in ("pending", "building")
    ):
        return False
    stamps = _job_stamps(conn, job.job_id, now)
    stamp = isoformat_utc_micro(now)
    if _newer_open_publication(conn, job):
        conn.execute(
            "UPDATE close_publications SET state = 'superseded', finished_at = ?"
            " WHERE site_id = ? AND cutoff = ?",
            (stamp, job.site_id, job.local_date.isoformat()),
        )
        _write_stage_row(conn, job, stamps, now_utc=now, outcome="superseded")
        return False
    conn.execute(
        "UPDATE close_publications SET state = 'building', started_at = ?"
        " WHERE site_id = ? AND cutoff = ?",
        (stamp, job.site_id, job.local_date.isoformat()),
    )
    if _stalled(*stamps):
        fail_publication(
            conn,
            site_id=job.site_id,
            cutoff=job.local_date,
            attempt=job.attempt,
            reason="stalled",
            now_utc=now,
            job_id=job.job_id,
        )
        return False
    return True


async def _publish_stage(
    db: Database, writer: FencedWriter, raw: Job, job: _StageJob
) -> None:
    """Stage 7: publish up to the cutoff, then stage 8 (8.1, 9.4).

    An exception from P4 propagates; ``fail_day_publish_job`` records it.
    """
    if not await writer.write(lambda conn: _begin_publication(conn, raw, job)):
        return
    publication_id = await close_p4.publish(
        db=db,
        writer=writer,
        site_id=job.site_id,
        cutoff=job.local_date,
        attempt=job.attempt,
        now_utc=utc_now(),
    )

    def finish(conn: sqlite3.Connection) -> None:
        now = utc_now()
        record = publication_record(conn, site_id=job.site_id, cutoff=job.local_date)
        if (
            record is None
            or record.state != "building"
            or record.attempt != job.attempt
        ):
            return
        conn.execute(
            "UPDATE close_publications SET state = 'published',"
            " publication_id = ?, finished_at = ? WHERE site_id = ? AND cutoff = ?",
            (
                publication_id,
                isoformat_utc_micro(now),
                job.site_id,
                job.local_date.isoformat(),
            ),
        )
        _write_stage_row(
            conn,
            job,
            _job_stamps(conn, job.job_id, now),
            now_utc=now,
            outcome="completed",
            detail={"publication_id": publication_id},
        )
        _enqueue_publish_stage(conn, job.site_id, job.local_date, 8, job.attempt)

    await writer.write(finish)


def _check_cleanup(conn: sqlite3.Connection, raw: Job, job: _StageJob) -> None:
    """Stage 8's checks: a ``published`` record of its cutoff and attempt.

    Otherwise ``JobCancelled``, with nothing written, so the job completes
    without the success marker (8.2, 9.6). A job claimed too late raises,
    so its failure is recorded as any stage-8 failure (8.3, 9.4).
    """
    _require_close_on(conn, raw)
    record = publication_record(conn, site_id=job.site_id, cutoff=job.local_date)
    if record is None or record.state != "published" or record.attempt != job.attempt:
        raise JobCancelled()
    if _stalled(*_job_stamps(conn, job.job_id, utc_now())):
        raise RuntimeError("stalled")


async def _cleanup_stage(
    db: Database, writer: FencedWriter, raw: Job, job: _StageJob
) -> None:
    """Stage 8: P4 deletes older runs' worksheets; nothing follows (8.1)."""
    await db.read(lambda conn: _check_cleanup(conn, raw, job))
    runs = await close_p4.clean_worksheets(
        db=db, writer=writer, site_id=job.site_id, now_utc=utc_now()
    )

    def finish(conn: sqlite3.Connection) -> None:
        now = utc_now()
        _write_stage_row(
            conn,
            job,
            _job_stamps(conn, job.job_id, now),
            now_utc=now,
            outcome="completed",
            detail={"runs_finished": runs},
        )

    await writer.write(finish)


# --- the entry points --------------------------------------------------------


def _parse_or_cancel(job: Job, kind: str) -> _StageJob:
    stage_job = _stage_job(job)
    if stage_job is None or job.type != kind:
        logger.warning("malformed %s job %s not run", job.type, job.id)
        raise JobCancelled()
    return stage_job


async def run_day_close(db: Database, writer: FencedWriter, job: Job) -> None:
    """Run one ``day_close`` job: stage 2, 3, 4, 5 or 6 (plan 8.1, 8.2).

    ``dispatch`` calls it as ``await run_day_close(db, writer, job)``. A
    malformed job, or one found while the close is off (one WARNING, 13.1),
    ends with ``JobCancelled``. A stale job returns at once; a stage that
    decides it failed records the failure itself and returns. An exception
    propagates, for ``_fail_job`` to record through ``fail_day_close_job``.
    """
    stage_job = _parse_or_cancel(job, "day_close")
    if stage_job.stage == 2:
        await _gap_fill_stage(db, writer, job, stage_job)
    elif stage_job.stage == 3:
        await writer.write(lambda conn: _hourly_values_stage(conn, job, stage_job))
    elif stage_job.stage == 4:
        await writer.write(lambda conn: _truth_stage(conn, job, stage_job))
    elif stage_job.stage == 5:
        await writer.write(lambda conn: _score_stage(conn, job, stage_job))
    else:
        await _closure_stage(writer, job, stage_job)


async def run_day_publish(db: Database, writer: FencedWriter, job: Job) -> None:
    """Run one ``day_publish`` job: stage 7 or 8 (plan 8.1, 8.2, 9.4).

    ``dispatch`` calls it as ``await run_day_publish(db, writer, job)``. A
    malformed job, or one found while the close is off (one WARNING, 13.1),
    ends with ``JobCancelled``, as does a stage 8 with no ``published``
    record of its attempt. An exception propagates, for ``_fail_job`` to
    record through ``fail_day_publish_job``.
    """
    stage_job = _parse_or_cancel(job, "day_publish")
    if stage_job.stage == 7:
        await _publish_stage(db, writer, job, stage_job)
    else:
        await _cleanup_stage(db, writer, job, stage_job)


# --- failures ----------------------------------------------------------------


def fail_attempt(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    attempt: int,
    stage: int,
    reason: str,
    now_utc: datetime,
    job_id: int,
) -> DayState:
    """Record a failed close attempt, in the caller's transaction (plan 9.1).

    P1's ``fail_close`` moves the day to ``retry_due`` (``retry_at`` =
    ``now_utc`` + ``RETRY_DELAY``) after attempt 1, or to ``missed``
    (``retry_failed``) after attempt 2; then the attempt record is closed
    with ``stage`` and ``reason``, the stage-log row of job ``job_id``
    written as ``failed``, every other ``pending`` ``day_close`` job of the
    site-day completed without running and, after a move to ``missed``,
    raw retention run in its own savepoint. ``reason`` is a short sanitized
    text, with no station id, coordinates or key. Returns the day's new
    state; raises ``DayStateError`` unless the day is ``closing``.
    """
    state = fail_close(conn, site_id, local_date, now_utc=now_utc, reason=reason)
    conn.execute(
        "UPDATE close_attempts SET outcome = 'failed', failed_stage = ?,"
        " failure_reason = ?, ended_at = ?"
        " WHERE site_id = ? AND local_date = ? AND attempt = ?",
        (
            stage,
            reason,
            isoformat_utc_micro(now_utc),
            site_id,
            local_date.isoformat(),
            attempt,
        ),
    )
    job = _StageJob(job_id, site_id, local_date, stage, attempt)
    _write_stage_row(
        conn,
        job,
        _job_stamps(conn, job_id, now_utc),
        now_utc=now_utc,
        outcome="failed",
        reason=reason,
    )
    pending = conn.execute(
        "SELECT id FROM jobs WHERE type = 'day_close' AND site_id = ?"
        " AND status = 'pending' AND job_key LIKE ?",
        (site_id, f"close:{site_id}:{local_date.isoformat()}:%"),
    ).fetchall()
    for row in pending:
        complete(conn, int(row[0]))
    if state == "missed":
        run_close_retention(conn, now_utc=now_utc)
    return state


def fail_publication(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    cutoff: date,
    attempt: int,
    reason: str,
    now_utc: datetime,
    job_id: int,
) -> None:
    """Record a failed stage 7, in the caller's transaction (plan 9.4).

    Acts only on a ``building`` record of ``cutoff`` whose ``attempt`` is
    the job's: after attempt 1 it goes to ``retry_due`` with ``retry_at`` =
    ``now_utc`` + ``RETRY_DELAY``, after attempt 2 to ``failed``; either way
    with ``reason`` as ``last_failure`` and the stage-7 stage-log row of job
    ``job_id`` written as ``failed``. On any other record it changes
    nothing and writes one WARNING line. Nothing about the day changes.
    """
    record = publication_record(conn, site_id=site_id, cutoff=cutoff)
    if record is None or record.state != "building" or record.attempt != attempt:
        logger.warning(
            "publication failure not recorded: site=%s cutoff=%s attempt=%s record=%s",
            site_id,
            cutoff.isoformat(),
            attempt,
            "none" if record is None else f"{record.state}/{record.attempt}",
        )
        return
    if attempt == 1:
        conn.execute(
            "UPDATE close_publications SET state = 'retry_due', retry_at = ?,"
            " last_failure = ? WHERE site_id = ? AND cutoff = ?",
            (
                isoformat_utc(now_utc + RETRY_DELAY),
                reason,
                site_id,
                cutoff.isoformat(),
            ),
        )
    else:
        conn.execute(
            "UPDATE close_publications SET state = 'failed', finished_at = ?,"
            " last_failure = ? WHERE site_id = ? AND cutoff = ?",
            (isoformat_utc_micro(now_utc), reason, site_id, cutoff.isoformat()),
        )
    _write_stage_row(
        conn,
        _StageJob(job_id, site_id, cutoff, 7, attempt),
        _job_stamps(conn, job_id, now_utc),
        now_utc=now_utc,
        outcome="failed",
        reason=reason,
    )


def fail_day_close_job(
    conn: sqlite3.Connection, job: Job, reason: str, *, now_utc: datetime
) -> None:
    """``_fail_job``'s ``day_close`` branch, in its transaction (plan 9.1).

    Calls ``fail_attempt`` with the job's stage and ``reason`` (the
    sanitized message) only when the day is ``closing`` with ``attempts``
    equal to the job's attempt. Otherwise (the day already closed, waiting
    for its retry, missed, or in a later attempt) it changes nothing and
    writes one WARNING line with the site, the date, the stage and the
    day's state. Call it before ``fail()``: ``fail()`` overwrites the job
    row's ``updated_at``, the claim stamp the stage-log row copies; with
    ``max_retries`` 0 every failure of the kind is final.
    """
    stage_job = _stage_job(job)
    if stage_job is None or job.type != "day_close":
        logger.warning("malformed day_close job %s failed; no day changed", job.id)
        return
    record = day_record(conn, stage_job.site_id, stage_job.local_date)
    if (
        record is None
        or record.state != "closing"
        or record.attempts != stage_job.attempt
    ):
        logger.warning(
            "stale day_close job failed: site=%s date=%s stage=%s state=%s"
            " attempts=%s job_attempt=%s; no day changed",
            stage_job.site_id,
            stage_job.local_date.isoformat(),
            stage_job.stage,
            "no record" if record is None else record.state,
            "-" if record is None else record.attempts,
            stage_job.attempt,
        )
        return
    fail_attempt(
        conn,
        site_id=stage_job.site_id,
        local_date=stage_job.local_date,
        attempt=stage_job.attempt,
        stage=stage_job.stage,
        reason=reason,
        now_utc=now_utc,
        job_id=job.id,
    )


def fail_day_publish_job(
    conn: sqlite3.Connection, job: Job, reason: str, *, now_utc: datetime
) -> None:
    """``_fail_job``'s ``day_publish`` branch, in its transaction (plan 9.4).

    Stage 7: ``fail_publication`` with ``reason``. Stage 8: only stage 8's
    stage-log row, ``failed``; the record stays ``published``. Call it
    before ``fail()``, as ``fail_day_close_job``.
    """
    stage_job = _stage_job(job)
    if stage_job is None or job.type != "day_publish":
        logger.warning("malformed day_publish job %s failed", job.id)
        return
    if stage_job.stage == 7:
        fail_publication(
            conn,
            site_id=stage_job.site_id,
            cutoff=stage_job.local_date,
            attempt=stage_job.attempt,
            reason=reason,
            now_utc=now_utc,
            job_id=job.id,
        )
        return
    _write_stage_row(
        conn,
        stage_job,
        _job_stamps(conn, job.id, now_utc),
        now_utc=now_utc,
        outcome="failed",
        reason=reason,
    )


# --- raw retention -----------------------------------------------------------


def run_close_retention(conn: sqlite3.Connection, *, now_utc: datetime) -> None:
    """Prune raw readings and trim the close's two logs (plan 8.4, 11.6).

    Runs P2's ``prune_raw_readings`` and deletes the stage-log and try-log
    rows older than ``CLOSE_LOG_KEEP_DAYS``, inside ``SAVEPOINT
    close_retention`` in the caller's transaction, for every site. An
    exception is re-raised when the transaction is gone or it is a
    ``sqlite3.OperationalError``; any other is rolled back to the savepoint
    with one ERROR line naming its type, so the caller's writes commit.
    """
    conn.execute("SAVEPOINT close_retention")
    try:
        prune_raw_readings(conn, now_utc=now_utc)
        cutoff = isoformat_utc_micro(now_utc - timedelta(days=CLOSE_LOG_KEEP_DAYS))
        conn.execute("DELETE FROM close_stage_log WHERE ended_at < ?", (cutoff,))
        conn.execute("DELETE FROM close_try_log WHERE tried_at < ?", (cutoff,))
    except Exception as exc:
        if not conn.in_transaction or isinstance(exc, sqlite3.OperationalError):
            raise
        conn.execute("ROLLBACK TO close_retention")
        conn.execute("RELEASE close_retention")
        logger.error(
            "close retention failed (%s); the surrounding write goes on",
            type(exc).__name__,
        )
        return
    conn.execute("RELEASE close_retention")
