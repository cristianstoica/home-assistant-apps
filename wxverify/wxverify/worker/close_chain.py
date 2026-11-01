"""The daily close's night chain: the close of each site-day and its logs.

Holds the chain's constants; the bodies of stages 2 to 8 (``day_close``
stages 2 to 6, ``day_publish`` stages 7 and 8) under the hand-off rule
(plan 8.1-8.3), entered through ``run_day_close`` and ``run_day_publish``;
the failure writes ``fail_attempt`` and ``fail_publication`` (9.1, 9.4) and
the hooks ``_fail_job`` calls for them; ``run_close_retention`` (8.4);
``publication_record``; ``close_status``, ``stage_log``,
``collection_tries`` and ``publication_state``, the read-only interface for
P5 and P6 (14.2); ``record_collection_try``, the writer of the
collection try log (11.2); and ``enqueue_due_close_work``, the tick's part
(8.5): stage 1, the missed days, the repair and the publication retry; and
``settle_interrupted_closes``, which settles the closes and publications a
stop cut short, at each start and on an imported file (5.5, 9.4, 12.6);
and ``begin_offline_close`` and ``run_offline_stage``, with which the
``close-day`` command closes one past day on a database copy (8.8).

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
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import partial
from typing import Final, Literal, cast

from wxverify.core.timeutil import (
    isoformat_utc,
    isoformat_utc_micro,
    parse_utc,
    utc_now,
)
from wxverify.db.connection import Database, FencedWriter
from wxverify.db.queue import Job, complete, enqueue_if_absent
from wxverify.db.runtime_state import get_runtime_state
from wxverify.db.site_days import (
    RETRY_DELAY,
    DayRecord,
    DayState,
    MissedReason,
    begin_close,
    closed_through,
    daily_close_on,
    day_record,
    ensure_site_days,
    fail_close,
    finish_close,
    mark_missed,
)
from wxverify.db.station_readings import prune_raw_readings
from wxverify.db.switch import site_generation_id
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
from wxverify.worker.rain_check import check_station_rain

logger = logging.getLogger(__name__)

#: The longest a stage may wait in the queue, and how late after a due
#: instant the add-on still counts as running (plan 5.2, 8.3). Equal to
#: ``RETRY_DELAY``, so a late start never runs past the retry's instant.
CLOSE_LATE_LIMIT: Final = timedelta(hours=1)
#: How long the stage log and the try log keep a row (plan 11.6).
CLOSE_LOG_KEEP_DAYS: Final = 90
#: How many counted days with nothing from any source pause a station's
#: nightly gap fill (plan 5.6, :137; the owner's value).
CLOSE_PAUSE_DAYS: Final = 1

#: The tick's per-site ERROR line is written at most once per site and step
#: in this span of ``now_utc`` (plan 8.5).
_SITE_ERROR_EVERY: Final = timedelta(hours=1)
#: (site id, step) -> the ``now_utc`` of its last per-site ERROR line (8.5).
_LAST_SITE_STEP_ERROR: dict[tuple[int, int], datetime] = {}
#: The order key of a site with no due instant: after every due one (8.5).
_NEVER_DUE: Final = datetime.max.replace(tzinfo=UTC)

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

#: The 13 ``daily_truth`` columns of the truth version, per row (plan 11.4),
#: of the one generation stage 4 writes under: the site's published one.
_TRUTH_VERSION_SQL: Final = (
    "SELECT quantity, value, eligible, exclusion_reason, covered_hours,"
    " expected_slots, peak_window_ok, wet_hours, dry_hours, rain_threshold_mm,"
    " day_start_utc, day_end_utc, timezone FROM daily_truth"
    " WHERE site_id = ? AND local_date = ? AND tz_generation_id = ?"
    " ORDER BY quantity"
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
    a newer open or published one ends ``superseded`` (plan 8.6, 9.4, 11.5).
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


# --- the read-only interface for P5 and P6 (14.2) ---------------------------


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    """One ``close_attempts`` row: one close attempt of a site-day (11.3).

    ``outcome`` is ``running``, ``closed`` or ``failed``. ``last_stage`` is
    the last stage saved as complete (1 when the attempt starts), and
    ``last_progress_at`` when. ``ended_at`` is set when the attempt ends,
    ``failed_stage`` and ``failure_reason`` when it fails, and
    ``truth_version`` once stage 4 has saved it.
    """

    attempt: int
    started_at: datetime
    last_stage: int
    last_progress_at: datetime
    outcome: str
    failed_stage: int | None
    failure_reason: str | None
    ended_at: datetime | None
    truth_version: str | None


@dataclass(frozen=True, slots=True)
class StationDayRecord:
    """One ``close_station_days`` row: a station's presence on a day (11.3).

    The readings in the day's bounds per source after the gap fill; whether
    a 5-minute try of the station-day was answered; whether the day was
    lost to refused access; whether the station was paused for the day. A
    paused station is never one whose values were thrown away (5.6, 14.2).
    """

    station_id: int
    five_minute: int
    current_obs: int
    hourly: int
    answered: bool
    refused_lost: bool
    paused: bool


@dataclass(frozen=True, slots=True)
class StageRun:
    """One ``close_stage_log`` row: one run of a stage (11.1).

    ``attempt`` is the close attempt for stages 1 to 6 and the publication
    attempt for stages 7 and 8, whose rows are dated by the publication's
    cutoff. ``job_id`` is None for stage 1 only. ``outcome`` is
    ``completed``, ``failed``, ``superseded`` or ``skipped``; ``detail`` is
    the stored JSON object text.
    """

    stage: int
    attempt: int
    job_id: int | None
    enqueued_at: datetime
    started_at: datetime
    ended_at: datetime
    queue_wait_s: float
    duration_s: float
    outcome: str
    reason: str | None
    detail: str


@dataclass(frozen=True, slots=True)
class CollectionTry:
    """One ``close_try_log`` row: one weather.com collection try (11.2).

    ``slot`` is an evening slot as local ``HH:MM``, ``daytime`` or
    ``gap_fill``; ``close_attempt`` is set for a gap fill only. A held or
    refused try is an outage of access, never a station offline (14.2).
    """

    station_id: int
    kind: str
    slot: str
    close_attempt: int | None
    tried_at: datetime
    outcome: str
    reason: str | None
    detail: str | None
    readings_kept: int
    readings_refused: int


@dataclass(frozen=True, slots=True)
class CloseStatus:
    """Everything about the close of one site-day, from stored rows (14.2).

    ``state`` to ``done_at`` are P1's ``site_days`` columns as stored.
    ``blocking_stage`` is derived (``_blocking_stage``). ``attempt_records``
    are the day's attempts in attempt order; ``stations`` its presence
    records in station order (after stage 2 of an attempt); ``publication``
    the publication whose cutoff is this day (a ``closed`` day only).
    """

    state: DayState
    attempts: int
    retry_at: datetime | None
    last_failure: str | None
    missed_reason: MissedReason | None
    done_at: datetime | None
    blocking_stage: int | None
    attempt_records: tuple[AttemptRecord, ...]
    stations: tuple[StationDayRecord, ...]
    publication: PublicationRecord | None


def _attempt_records(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> tuple[AttemptRecord, ...]:
    rows = conn.execute(
        "SELECT attempt, started_at, last_stage, last_progress_at, outcome,"
        " failed_stage, failure_reason, ended_at, truth_version"
        " FROM close_attempts WHERE site_id = ? AND local_date = ?"
        " ORDER BY attempt",
        (site_id, local_date.isoformat()),
    ).fetchall()
    return tuple(
        AttemptRecord(
            attempt=int(row[0]),
            started_at=parse_utc(str(row[1])),
            last_stage=int(row[2]),
            last_progress_at=parse_utc(str(row[3])),
            outcome=str(row[4]),
            failed_stage=None if row[5] is None else int(row[5]),
            failure_reason=None if row[6] is None else str(row[6]),
            ended_at=_instant_or_none(row[7]),
            truth_version=None if row[8] is None else str(row[8]),
        )
        for row in rows
    )


def _station_records(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> tuple[StationDayRecord, ...]:
    rows = conn.execute(
        "SELECT station_id, five_minute, current_obs, hourly, answered,"
        " refused_lost, paused FROM close_station_days"
        " WHERE site_id = ? AND local_date = ? ORDER BY station_id",
        (site_id, local_date.isoformat()),
    ).fetchall()
    return tuple(
        StationDayRecord(
            station_id=int(row[0]),
            five_minute=int(row[1]),
            current_obs=int(row[2]),
            hourly=int(row[3]),
            answered=bool(row[4]),
            refused_lost=bool(row[5]),
            paused=bool(row[6]),
        )
        for row in rows
    )


def _blocking_stage(
    record: DayRecord, attempts: tuple[AttemptRecord, ...], now_utc: datetime
) -> int | None:
    """The stage the day waits on, or None (14.2).

    ``open`` from its ``day_end_utc`` on: 1, the wait for the round; only
    here is ``now_utc``, the caller's clock, used. ``closing``: the
    current attempt's ``last_stage`` + 1. ``retry_due`` and ``missed``
    (``retry_failed``): the failed attempt's ``failed_stage``. The current
    or failed attempt is the one numbered ``attempts``, P1's count. None in
    every other case, and when that attempt has no record.
    """
    if record.state == "open":
        return 1 if now_utc >= record.day_end_utc else None
    current = next((row for row in attempts if row.attempt == record.attempts), None)
    if current is None:
        return None
    if record.state == "closing":
        return current.last_stage + 1
    if record.state == "retry_due" or (
        record.state == "missed" and record.missed_reason == "retry_failed"
    ):
        return current.failed_stage
    return None


def close_status(
    conn: sqlite3.Connection, *, site_id: int, local_date: date, now_utc: datetime
) -> CloseStatus | None:
    """Everything about the close of ``site_id``'s day ``local_date`` (14.2).

    None when the site has no P1 record dated ``local_date`` (before its
    first record, or with the close off). Each field is present or None as
    the stored rows hold it; nothing is cached. Read-only: runs in the
    caller's transaction and never commits, so a caller that needs its
    four SELECTs in one snapshot holds one around the call. Reads no
    clock: ``now_utc``, the caller's clock, is used only for an ``open``
    day, to tell whether the day has ended.
    """
    record = day_record(conn, site_id, local_date)
    if record is None:
        return None
    attempts = _attempt_records(conn, site_id, local_date)
    return CloseStatus(
        state=record.state,
        attempts=record.attempts,
        retry_at=record.retry_at,
        last_failure=record.last_failure,
        missed_reason=record.missed_reason,
        done_at=_instant_or_none(record.done_at),
        blocking_stage=_blocking_stage(record, attempts, now_utc),
        attempt_records=attempts,
        stations=_station_records(conn, site_id, local_date),
        publication=publication_record(conn, site_id=site_id, cutoff=local_date),
    )


def stage_log(
    conn: sqlite3.Connection, *, site_id: int, local_date: date
) -> list[StageRun]:
    """The stage-log rows of ``site_id`` dated ``local_date`` (11.1, 14.2).

    Stages 1 to 6 of the day's close attempts and, for a ``closed`` day,
    stages 7 and 8 of the publication whose cutoff it is, in the order the
    rows were written. Rows older than ``CLOSE_LOG_KEEP_DAYS`` are gone.
    Read-only; runs in the caller's transaction.
    """
    rows = conn.execute(
        "SELECT stage, attempt, job_id, enqueued_at, started_at, ended_at,"
        " queue_wait_s, duration_s, outcome, reason, detail FROM close_stage_log"
        " WHERE site_id = ? AND local_date = ? ORDER BY id",
        (site_id, local_date.isoformat()),
    ).fetchall()
    return [
        StageRun(
            stage=int(row[0]),
            attempt=int(row[1]),
            job_id=None if row[2] is None else int(row[2]),
            enqueued_at=parse_utc(str(row[3])),
            started_at=parse_utc(str(row[4])),
            ended_at=parse_utc(str(row[5])),
            queue_wait_s=float(row[6]),
            duration_s=float(row[7]),
            outcome=str(row[8]),
            reason=None if row[9] is None else str(row[9]),
            detail=str(row[10]),
        )
        for row in rows
    ]


def collection_tries(
    conn: sqlite3.Connection, *, site_id: int, local_date: date
) -> list[CollectionTry]:
    """The collection try-log rows of ``site_id`` dated ``local_date`` (11.2, 14.2).

    Every station's 5-minute tries and gap-fill calls of the day, whatever
    its state, in the order the rows were written. Rows older than
    ``CLOSE_LOG_KEEP_DAYS`` are gone. Read-only; runs in the caller's
    transaction.
    """
    rows = conn.execute(
        "SELECT station_id, kind, slot, close_attempt, tried_at, outcome, reason,"
        " detail, readings_kept, readings_refused FROM close_try_log"
        " WHERE site_id = ? AND local_date = ? ORDER BY id",
        (site_id, local_date.isoformat()),
    ).fetchall()
    return [
        CollectionTry(
            station_id=int(row[0]),
            kind=str(row[1]),
            slot=str(row[2]),
            close_attempt=None if row[3] is None else int(row[3]),
            tried_at=parse_utc(str(row[4])),
            outcome=str(row[5]),
            reason=None if row[6] is None else str(row[6]),
            detail=None if row[7] is None else str(row[7]),
            readings_kept=int(row[8]),
            readings_refused=int(row[9]),
        )
        for row in rows
    ]


def publication_state(
    conn: sqlite3.Connection, *, site_id: int
) -> PublicationRecord | None:
    """The site's publication record with the latest cutoff, or None (11.5, 14.2).

    Whatever its state. Read-only; runs in the caller's transaction.
    """
    row = conn.execute(
        "SELECT MAX(cutoff) FROM close_publications WHERE site_id = ?", (site_id,)
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return publication_record(
        conn, site_id=site_id, cutoff=date.fromisoformat(str(row[0]))
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


def _insert_stage_row(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    attempt: int,
    stage: int,
    job_id: int | None,
    enqueued_at: str,
    started_at: str,
    now_utc: datetime,
    outcome: _StageOutcome,
    reason: str | None = None,
    detail: dict[str, object] | None = None,
) -> None:
    """Insert one ``close_stage_log`` row (11.1).

    ``enqueued_at`` and ``started_at`` are stored as given and parsed for
    the queue wait and the duration; ``ended_at`` is ``now_utc``, when the
    completing or failing write runs. ``job_id`` is None for stage 1 only.
    """
    started = parse_utc(started_at)
    conn.execute(
        "INSERT INTO close_stage_log (site_id, local_date, attempt, stage,"
        " job_id, enqueued_at, started_at, ended_at, queue_wait_s, duration_s,"
        " outcome, reason, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            local_date.isoformat(),
            attempt,
            stage,
            job_id,
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
    _insert_stage_row(
        conn,
        site_id=job.site_id,
        local_date=job.local_date,
        attempt=job.attempt,
        stage=job.stage,
        job_id=job.job_id,
        enqueued_at=enqueued_at,
        started_at=started_at,
        now_utc=now_utc,
        outcome=outcome,
        reason=reason,
        detail=detail,
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
    """Step 4 for stages 2 to 5: save the completion, enqueue the next (8.2).

    ``now_utc`` is the stage's end: the caller reads the clock after the
    stage's work, inside the completing write, so the stage-log row's
    ``ended_at`` and ``duration_s`` and the attempt record's
    ``last_progress_at`` include that work. The next stage is enqueued
    after them.
    """
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

    A station on the day's list is paused when P2's ``day_presence`` counts
    no ``five_minute`` and no ``current_obs`` reading for it inside the
    day's bounds, and its latest ``CLOSE_PAUSE_DAYS`` counted days before
    the day each had nothing from any source on their presence record
    (11.3). A counted day is a ``closed`` day whose saved station list holds
    the station (P1 §6.2) and that was not lost to refused access
    (``refused_lost``); missed days and days lost to refused access are
    passed over. A ``closed`` day that lists the station but has no presence
    record for it (one closed before the records were kept) ends the walk
    with the station not paused: no stored fact says it had nothing.

    Read-only, in the caller's transaction; reads no clock. At most one walk
    back per station, and only for a station with nothing on the day.
    """
    day = local_date.isoformat()
    paused: set[int] = set()
    presence = day_presence(conn, site_id=site_id, local_date=local_date)
    for station_id, station in presence.items():
        if station.readings["five_minute"] or station.readings["current_obs"]:
            continue
        # CROSS JOIN keeps site_days in the outer loop: the closed days are
        # walked back from the day on idx_site_days_state, newest first, and
        # the list and the presence record are point reads on their keys. A
        # day with no presence record gives NULL counts, which end the walk.
        rows = conn.execute(
            "SELECT p.five_minute, p.current_obs, p.hourly"
            " FROM site_days AS d INDEXED BY idx_site_days_state"
            " CROSS JOIN site_day_stations AS l"
            " LEFT JOIN close_station_days AS p ON p.site_id = d.site_id"
            " AND p.local_date = d.local_date AND p.station_id = l.station_id"
            " WHERE d.site_id = ? AND d.state = 'closed' AND d.local_date < ?"
            " AND l.site_id = d.site_id AND l.local_date = d.local_date"
            " AND l.station_id = ? AND p.refused_lost IS NOT 1"
            " ORDER BY d.local_date DESC LIMIT ?",
            (site_id, day, station_id, CLOSE_PAUSE_DAYS),
        ).fetchall()
        if len(rows) == CLOSE_PAUSE_DAYS and all(
            tuple(row) == (0, 0, 0) for row in rows
        ):
            paused.add(station_id)
    return frozenset(paused)


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
        _complete_stage(conn, job, stamps, now_utc=utc_now(), detail=detail)

    await writer.write(finish)


# --- stages 3 to 5: one transaction each ------------------------------------


def _hourly_values_stage(conn: sqlite3.Connection, raw: Job, job: _StageJob) -> None:
    """Stage 3: the day's hourly values and site hours (8.1, 8.3), then the
    rain check over them (rain release, A2), in the one transaction."""
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
    check_station_rain(
        conn, site_id=job.site_id, local_date=job.local_date, now_utc=now
    )
    _complete_stage(conn, job, running.stamps, now_utc=utc_now())


def _truth_version(conn: sqlite3.Connection, site_id: int, local_date: date) -> str:
    """SHA-256 of the day's ``daily_truth`` rows as JSON (plan 11.4).

    The rows as stage 4 wrote them: those of the site's one time zone
    generation, read through ``site_generation_id`` as
    ``write_close_truth`` reads it, so another generation's rows of the
    same date never enter the version. One JSON array of the rows in
    ``quantity`` order, each an object of the 13 columns, with sorted keys
    and no spaces.
    """
    generation_id = site_generation_id(conn, site_id)
    if generation_id is None:
        raise RuntimeError("the site has no published timezone generation")
    rows = conn.execute(
        _TRUTH_VERSION_SQL, (site_id, local_date.isoformat(), generation_id)
    )
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
    _complete_stage(conn, job, running.stamps, now_utc=utc_now())


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
    _complete_stage(conn, job, running.stamps, now_utc=utc_now(), detail=detail)


# --- stage 6: the closure ----------------------------------------------------


def _closure(
    conn: sqlite3.Connection, raw: Job, job: _StageJob, *, publish: bool = True
) -> bool:
    """Stage 6's one write: the closure, its publication, stage 7 (8.1, 8.6).

    ``finish_close``, the attempt closed, the stage-log row, the publication
    record of cutoff ``closed_through`` opened and stage 7 enqueued all
    commit together or not at all. True when the day was closed. With
    ``publish`` False (the offline close, 8.8) no publication record is
    opened and no stage 7 is enqueued. ``finish_close`` runs at the write's
    start time; the attempt's ``last_progress_at`` and ``ended_at``, the
    stage-log row's ``ended_at`` and the publication record's ``opened_at``
    take the clock read after it, before stage 7 is enqueued.
    """
    now = utc_now()
    running = _open_stage(conn, raw, job, now)
    if running is None:
        return False
    finish_close(conn, job.site_id, job.local_date, now_utc=now)
    ended = utc_now()
    stamp = isoformat_utc_micro(ended)
    conn.execute(
        "UPDATE close_attempts SET outcome = 'closed', last_stage = 6,"
        " last_progress_at = ?, ended_at = ?"
        " WHERE site_id = ? AND local_date = ? AND attempt = ?",
        (stamp, stamp, job.site_id, job.local_date.isoformat(), job.attempt),
    )
    _write_stage_row(conn, job, running.stamps, now_utc=ended, outcome="completed")
    if not publish:
        return True
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


def _newer_publication(conn: sqlite3.Connection, job: _StageJob) -> bool:
    """Whether a later cutoff of the site is open or published (plan 8.6).

    Open is ``pending`` or ``building``. A ``published`` later cutoff
    counts too, so an older cutoff's retry, or its repaired stage 7, never
    publishes after a newer cutoff has.
    """
    row = conn.execute(
        "SELECT 1 FROM close_publications WHERE site_id = ? AND cutoff > ?"
        " AND state IN ('pending', 'building', 'published') LIMIT 1",
        (job.site_id, job.local_date.isoformat()),
    ).fetchone()
    return row is not None


def _begin_publication(conn: sqlite3.Connection, raw: Job, job: _StageJob) -> bool:
    """Stage 7's short first write: the checks, then ``building`` (8.2, 9.4).

    False, with nothing written, when the job's record is not open (its
    cutoff, its attempt, ``pending`` or ``building``). An older cutoff with
    a newer open or published record ends ``superseded``, with no call to
    P4. A job claimed too late fails the publication as ``stalled`` (8.3).
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
    if _newer_publication(conn, job):
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


# --- the offline close of one past day (8.8) ---------------------------------


def begin_offline_close(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    first_stage: int,
    now_utc: datetime,
) -> None:
    """Start the offline close's attempt, in the caller's transaction (8.8).

    ``begin_close`` and the attempt record (``last_stage`` 1), as the tick
    starts an attempt (``_start_attempt``), but with no stage-1 row and no
    stage 2: ``first_stage`` is enqueued instead, 3 to rebuild the day's
    hours from the raw readings or 4 to keep its stored ``observations``.
    Raises ``DayStateError`` as ``begin_close`` does. Never commits.
    """
    if first_stage not in (3, 4):
        raise ValueError(f"the offline close starts at stage 3 or 4, not {first_stage}")
    attempt = begin_close(conn, site_id, local_date, now_utc=now_utc)
    stamp = isoformat_utc_micro(now_utc)
    conn.execute(
        "INSERT INTO close_attempts (site_id, local_date, attempt, started_at,"
        " last_progress_at) VALUES (?, ?, ?, ?, ?)",
        (site_id, local_date.isoformat(), attempt, stamp, stamp),
    )
    _enqueue_close_stage(conn, site_id, local_date, first_stage, attempt)


def run_offline_stage(
    conn: sqlite3.Connection, *, site_id: int, local_date: date, stage: int
) -> bool:
    """Run one stage, 3 to 6, of the offline close, in the caller's transaction.

    Plan 8.8. Claims the site-day's ``pending`` ``day_close`` job of
    ``stage`` as the queue's claim does (``status`` ``running``,
    ``updated_at`` the claim time), runs that stage's body as
    ``run_day_close`` would and completes the job row with the success
    marker. Stages 3 to 5 enqueue the next stage as they do in the add-on;
    stage 6 saves the closure with no publication record and no stage 7,
    and no raw retention follows. Returns False, with nothing written, when
    no such job is pending: an earlier stage failed the attempt. Never
    commits.
    """
    if stage not in (3, 4, 5, 6):
        raise ValueError(f"the offline close runs stages 3 to 6, not {stage}")
    key = _close_key(site_id, local_date, stage)
    row = conn.execute(
        "UPDATE jobs SET status = 'running', updated_at = ?"
        " WHERE type = 'day_close' AND site_id = ? AND job_key = ?"
        " AND status = 'pending' RETURNING id, payload, retry_count, max_retries",
        (isoformat_utc(utc_now()), site_id, key),
    ).fetchone()
    if row is None:
        return False
    payload: object = json.loads(str(row[1]))
    if not isinstance(payload, dict):
        raise RuntimeError(f"day_close job {row[0]} has no object payload")
    raw = Job(
        id=int(row[0]),
        type="day_close",
        site_id=site_id,
        job_key=key,
        payload=cast(dict[str, object], payload),
        status="running",
        retry_count=int(row[2]),
        max_retries=int(row[3]),
    )
    stage_job = _parse_or_cancel(raw, "day_close")
    if stage == 3:
        _hourly_values_stage(conn, raw, stage_job)
    elif stage == 4:
        _truth_stage(conn, raw, stage_job)
    elif stage == 5:
        _score_stage(conn, raw, stage_job)
    else:
        _closure(conn, raw, stage_job, publish=False)
    complete(conn, raw.id, result="ok")
    return True


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
                isoformat_utc_micro(now_utc + RETRY_DELAY),
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


def _database_failing(exc: Exception) -> bool:
    """Whether ``exc`` must fail the caller's write (plan 8.4, 8.5).

    Every ``sqlite3.DatabaseError`` but ``sqlite3.IntegrityError``: the
    database is locked or failing (``OperationalError``), or the file is
    corrupt or not a database (a plain ``DatabaseError``). A constraint
    refusal (``IntegrityError``) is rolled back with its savepoint like any
    other exception. Shared by the three savepoint catches.
    """
    return isinstance(exc, sqlite3.DatabaseError) and not isinstance(
        exc, sqlite3.IntegrityError
    )


#: Rain release Part B (B6): each site's oldest day not yet done, by its
#: start. A site's scored values for hours before it have been read by
#: their closes; a site with no such day keeps all of its rows.
_OLDEST_UNDONE_STARTS_SQL: Final = """
    SELECT d.site_id, d.day_start_utc
    FROM site_days AS d
    WHERE d.state NOT IN ('closed', 'missed')
      AND d.local_date = (
          SELECT MIN(u.local_date)
          FROM site_days AS u
          WHERE u.site_id = d.site_id
            AND u.state NOT IN ('closed', 'missed')
      )
"""
_DELETE_OLD_SCORED_SQL: Final = (
    "DELETE FROM forecast_scored"
    " WHERE site_id = ? AND julianday(valid_at) < julianday(?)"
)


def run_close_retention(conn: sqlite3.Connection, *, now_utc: datetime) -> None:
    """Prune raw readings, the old scored values and the close's two logs
    (plan 8.4, 11.6; rain release Part B, B6).

    Runs P2's ``prune_raw_readings``, deletes the ``forecast_scored`` rows
    of hours before each site's oldest day not yet done, and deletes the
    stage-log and try-log rows older than ``CLOSE_LOG_KEEP_DAYS``, inside
    ``SAVEPOINT close_retention`` in the caller's transaction, for every
    site. An exception is re-raised when the transaction is gone or it is a
    database error (``_database_failing``); any other is rolled back to the
    savepoint with one ERROR line naming its type, so the caller's writes
    commit.
    """
    conn.execute("SAVEPOINT close_retention")
    try:
        prune_raw_readings(conn, now_utc=now_utc)
        for site_id, day_start in conn.execute(_OLDEST_UNDONE_STARTS_SQL).fetchall():
            conn.execute(_DELETE_OLD_SCORED_SQL, (site_id, day_start))
        cutoff = isoformat_utc_micro(now_utc - timedelta(days=CLOSE_LOG_KEEP_DAYS))
        conn.execute("DELETE FROM close_stage_log WHERE ended_at < ?", (cutoff,))
        conn.execute("DELETE FROM close_try_log WHERE tried_at < ?", (cutoff,))
    except Exception as exc:
        if not conn.in_transaction or _database_failing(exc):
            raise
        conn.execute("ROLLBACK TO close_retention")
        conn.execute("RELEASE close_retention")
        logger.error(
            "close retention failed (%s); the surrounding write goes on",
            type(exc).__name__,
        )
        return
    conn.execute("RELEASE close_retention")


# --- the tick: enqueue_due_close_work ----------------------------------------


@dataclass(slots=True)
class _SiteStep:
    """The step of plan 8.5 a site's part of the tick is in (1 to 5), or of
    5.5 a day or publication record being settled is in (1 to 4)."""

    number: int = 1


def _log_site_error(site_id: int, step: int, exc: Exception, now_utc: datetime) -> None:
    """One ERROR line per site and step per ``_SITE_ERROR_EVERY`` (8.5)."""
    last = _LAST_SITE_STEP_ERROR.get((site_id, step))
    if last is not None and now_utc - last < _SITE_ERROR_EVERY:
        return
    _LAST_SITE_STEP_ERROR[(site_id, step)] = now_utc
    logger.error(
        "daily close: site %s failed at step %s (%s); it begins nothing this tick",
        site_id,
        step,
        type(exc).__name__,
    )


def _in_site_savepoint[T](
    conn: sqlite3.Connection,
    site_id: int,
    now_utc: datetime,
    steps: Callable[[_SiteStep], T],
) -> T | None:
    """Run one site's ``steps`` under ``SAVEPOINT close_site`` (plan 8.5).

    ``steps`` sets the step number as it goes. An exception is re-raised
    when the transaction is gone or it is a database error
    (``_database_failing``); any other is rolled back to the savepoint,
    logged by ``_log_site_error`` and gives None, so the other sites go on.
    """
    step = _SiteStep()
    conn.execute("SAVEPOINT close_site")
    try:
        result = steps(step)
    except Exception as exc:
        if not conn.in_transaction or _database_failing(exc):
            raise
        conn.execute("ROLLBACK TO close_site")
        conn.execute("RELEASE close_site")
        _log_site_error(site_id, step.number, exc, now_utc)
        return None
    conn.execute("RELEASE close_site")
    return result


def _running_at(
    due_utc: datetime, worker_started_at: str | None, now_utc: datetime
) -> bool:
    """Whether the add-on was running at ``due_utc`` (plan 5.2).

    True when this process started at or before ``due_utc`` (the stored
    ``worker_started_at``; none stored counts as not running) and this tick
    runs no later than ``due_utc`` + ``CLOSE_LATE_LIMIT``. Raises
    ``ValueError`` when ``worker_started_at`` does not parse.
    """
    if worker_started_at is None or parse_utc(worker_started_at) > due_utc:
        return False
    return now_utc - due_utc <= CLOSE_LATE_LIMIT


def _undone_days(conn: sqlite3.Connection, site_id: int) -> list[DayRecord]:
    """The site's days not yet done, oldest first (P1 §4.5; plan 5.2)."""
    rows = conn.execute(
        "SELECT local_date FROM site_days WHERE site_id = ?"
        " AND state NOT IN ('closed', 'missed') ORDER BY local_date",
        (site_id,),
    ).fetchall()
    records = (day_record(conn, site_id, date.fromisoformat(str(r[0]))) for r in rows)
    return [record for record in records if record is not None]


def _round(
    conn: sqlite3.Connection,
    rounds: dict[tuple[int, date], close_p4.RoundState],
    record: DayRecord,
) -> close_p4.RoundState:
    """P4's answer for the round of R = D + 1, asked once per tick (5.1, 14.1)."""
    key = (record.site_id, record.local_date + timedelta(days=1))
    if key not in rounds:
        rounds[key] = close_p4.round_state(conn, site_id=key[0], round_date=key[1])
    return rounds[key]


def _wind_lane_due(conn: sqlite3.Connection, site_id: int, now_utc: datetime) -> bool:
    """Whether the site's wind-lane job is ``pending`` and due (plan 5.1).

    Due as the queue's claim reads it. The key is a literal: this module
    cannot import ``wind_days``.
    """
    row = conn.execute(
        "SELECT 1 FROM jobs WHERE type = 'fetch_obs' AND job_key = 'wind-days'"
        " AND site_id = ? AND status = 'pending'"
        " AND (next_attempt_at IS NULL OR next_attempt_at <= ?) LIMIT 1",
        (site_id, isoformat_utc(now_utc)),
    ).fetchone()
    return row is not None


def _retry_due_publications(
    conn: sqlite3.Connection, site_id: int
) -> list[PublicationRecord]:
    """The site's ``retry_due`` publication records, oldest cutoff first."""
    rows = conn.execute(
        "SELECT cutoff FROM close_publications WHERE site_id = ?"
        " AND state = 'retry_due' ORDER BY cutoff",
        (site_id,),
    ).fetchall()
    records = (
        publication_record(conn, site_id=site_id, cutoff=date.fromisoformat(str(r[0])))
        for r in rows
    )
    return [record for record in records if record is not None]


def _site_due_instant(
    conn: sqlite3.Connection,
    site_id: int,
    now_utc: datetime,
    rounds: dict[tuple[int, date], close_p4.RoundState],
    step: _SiteStep,
) -> datetime:
    """The earliest due instant among the site's candidates (plan 4.5, 8.5).

    The candidates: the oldest day not done (once it has ended, its round's
    bound; or its ``retry_at``) and each ``retry_due`` publication record's
    ``retry_at``. ``_NEVER_DUE`` when there is none. Read-only.
    """
    step.number = 3
    due: list[datetime] = []
    days = _undone_days(conn, site_id)
    if days:
        oldest = days[0]
        if oldest.state == "retry_due" and oldest.retry_at is not None:
            due.append(oldest.retry_at)
        elif oldest.state == "open" and now_utc >= oldest.day_end_utc:
            due.append(_round(conn, rounds, oldest).bound_utc)
    step.number = 5
    due.extend(
        record.retry_at
        for record in _retry_due_publications(conn, site_id)
        if record.retry_at is not None
    )
    return min(due, default=_NEVER_DUE)


def _start_attempt(
    conn: sqlite3.Connection, record: DayRecord, now_utc: datetime
) -> None:
    """Start a due attempt, in the tick's write (plan 5.1, 8.1, 11.3).

    ``begin_close``, then the attempt record (``last_stage`` 1); for attempt
    1 only, stage 1's stage-log row: no job, ``enqueued_at`` and
    ``started_at`` the day's ``day_end_utc`` as P1 stores it, so its wait is
    0 and its duration runs from the day's end (11.1). Then stage 2 is
    enqueued with the attempt.
    """
    site_id, local_date = record.site_id, record.local_date
    attempt = begin_close(conn, site_id, local_date, now_utc=now_utc)
    stamp = isoformat_utc_micro(now_utc)
    conn.execute(
        "INSERT INTO close_attempts (site_id, local_date, attempt, started_at,"
        " last_progress_at) VALUES (?, ?, ?, ?, ?)",
        (site_id, local_date.isoformat(), attempt, stamp, stamp),
    )
    if attempt == 1:
        day_end = isoformat_utc(record.day_end_utc)
        _insert_stage_row(
            conn,
            site_id=site_id,
            local_date=local_date,
            attempt=attempt,
            stage=1,
            job_id=None,
            enqueued_at=day_end,
            started_at=day_end,
            now_utc=now_utc,
            outcome="completed",
        )
    _enqueue_close_stage(conn, site_id, local_date, 2, attempt)


def _close_due_days(
    conn: sqlite3.Connection,
    site_id: int,
    now_utc: datetime,
    worker_started_at: str | None,
    rounds: dict[tuple[int, date], close_p4.RoundState],
    step: _SiteStep,
) -> bool:
    """Steps 2 and 3: the missed days, then a due attempt (plan 5.1, 5.2).

    Walks the site's days not done, oldest first. A changeover day at its
    first due instant (the round's bound), or a day whose attempt fell due
    while the add-on was not running, is moved to ``missed`` and the walk
    goes on; the first other day ends it, after its attempt starts when one
    is due. The first attempt waits for the round to settle or its bound,
    and for a due wind-lane job; the retry waits for ``retry_at`` only.
    True when a day moved to ``missed``.
    """
    moved = False
    for record in _undone_days(conn, site_id):
        reason: MissedReason
        if record.state == "open":
            if now_utc < record.day_end_utc:
                break
            step.number = 3
            answer = _round(conn, rounds, record)
            due = answer.bound_utc
            if record.zone_changeover:
                # Never attempted: missed at its first due instant (4.4).
                if now_utc < due:
                    break
                reason = "zone_changeover"
            elif not answer.settled and now_utc < due:
                break
            else:
                reason = "never_attempted"
        elif record.state == "retry_due" and record.retry_at is not None:
            due = record.retry_at
            if now_utc < due:
                break
            reason = "retry_not_run"
        else:
            break
        step.number = 2
        if reason == "zone_changeover" or not _running_at(
            due, worker_started_at, now_utc
        ):
            mark_missed(
                conn, site_id, record.local_date, now_utc=now_utc, reason=reason
            )
            moved = True
            continue
        step.number = 3
        if record.state == "open" and _wind_lane_due(conn, site_id, now_utc):
            break
        _start_attempt(conn, record, now_utc)
        break
    return moved


def _has_live_job(conn: sqlite3.Connection, job_type: str, site_id: int) -> bool:
    """Whether the site has a ``pending`` or ``running`` keyed job of the type.

    Every stage job has a key (8.1). The status and key terms repeat
    ``idx_jobs_active_dedupe``'s partial-index predicate word for word, and
    the site term uses that index's ``COALESCE(site_id, -1)`` expression,
    so the read can probe the live jobs on that index rather than walk every
    stored job of the type; it runs on each tick. The bound site id is
    never NULL, so the COALESCE does not change the result.
    """
    # COALESCE(site_id, -1) matches idx_jobs_active_dedupe's key expression;
    # a bare ``site_id = ?`` lets the planner pick idx_jobs_type_key_site.
    row = conn.execute(
        "SELECT 1 FROM jobs WHERE type = ? AND COALESCE(site_id, -1) = ?"
        " AND status IN ('pending','running') AND job_key IS NOT NULL LIMIT 1",
        (job_type, site_id),
    ).fetchone()
    return row is not None


def _repair(conn: sqlite3.Connection, site_id: int) -> None:
    """Step 4: enqueue again a stage job that was lost (plan 8.2, 8.5).

    A ``closing`` day with no ``day_close`` job of the site gets the stage
    after its attempt record's ``last_stage``, with its attempt. An open
    publication record (``pending`` or ``building``) with no ``day_publish``
    job of the site gets stage 7 with its cutoff and attempt, the oldest
    open cutoff first. A ``retry_due``, ``published`` or ``failed`` record
    is never repaired.
    """
    if not _has_live_job(conn, "day_close", site_id):
        closing = conn.execute(
            "SELECT local_date, attempts FROM site_days"
            " WHERE site_id = ? AND state = 'closing' ORDER BY local_date",
            (site_id,),
        ).fetchall()
        for row in closing:
            local_date, attempt = date.fromisoformat(str(row[0])), int(row[1])
            progress = conn.execute(
                "SELECT last_stage FROM close_attempts"
                " WHERE site_id = ? AND local_date = ? AND attempt = ?",
                (site_id, local_date.isoformat(), attempt),
            ).fetchone()
            if progress is None:
                raise RuntimeError("the closing attempt has no attempt record")
            stage = int(progress[0]) + 1
            _enqueue_close_stage(conn, site_id, local_date, stage, attempt)
    if not _has_live_job(conn, "day_publish", site_id):
        open_record = conn.execute(
            "SELECT cutoff, attempt FROM close_publications WHERE site_id = ?"
            " AND state IN ('pending', 'building') ORDER BY cutoff LIMIT 1",
            (site_id,),
        ).fetchone()
        if open_record is not None:
            cutoff = date.fromisoformat(str(open_record[0]))
            _enqueue_publish_stage(conn, site_id, cutoff, 7, int(open_record[1]))


def _retry_publications(
    conn: sqlite3.Connection,
    site_id: int,
    now_utc: datetime,
    worker_started_at: str | None,
) -> None:
    """Step 5: each publication retry whose time has come (plan 9.4).

    A ``retry_due`` record whose ``retry_at`` has passed, when the add-on
    was running at ``retry_at`` (5.2), becomes ``pending`` with ``attempt``
    2 and no ``retry_at``, and stage 7 is enqueued with that attempt;
    otherwise it goes to ``failed`` with reason ``retry_not_run``.
    """
    for record in _retry_due_publications(conn, site_id):
        if record.retry_at is None or now_utc < record.retry_at:
            continue
        key = (site_id, record.cutoff.isoformat())
        if _running_at(record.retry_at, worker_started_at, now_utc):
            conn.execute(
                "UPDATE close_publications SET state = 'pending', attempt = 2,"
                " retry_at = NULL WHERE site_id = ? AND cutoff = ?",
                key,
            )
            _enqueue_publish_stage(conn, site_id, record.cutoff, 7, 2)
        else:
            conn.execute(
                "UPDATE close_publications SET state = 'failed', finished_at = ?,"
                " last_failure = 'retry_not_run' WHERE site_id = ? AND cutoff = ?",
                (isoformat_utc_micro(now_utc), *key),
            )


def _site_steps(
    conn: sqlite3.Connection,
    site_id: int,
    now_utc: datetime,
    worker_started_at: str | None,
    rounds: dict[tuple[int, date], close_p4.RoundState],
    step: _SiteStep,
) -> bool:
    """Steps 1 to 5 of plan 8.5 for one site; True when a day moved to missed."""
    ensure_site_days(conn, site_id, now_utc=now_utc)
    moved = _close_due_days(conn, site_id, now_utc, worker_started_at, rounds, step)
    step.number = 4
    _repair(conn, site_id)
    step.number = 5
    _retry_publications(conn, site_id, now_utc, worker_started_at)
    return moved


def enqueue_due_close_work(conn: sqlite3.Connection, *, now_utc: datetime) -> None:
    """The tick's close work, in ``scheduler_tick``'s transaction (plan 8.5).

    Does nothing while the close is off (13.1). Each enabled site, in order
    of the earliest due instant among its candidates, then site id (4.5),
    runs under ``SAVEPOINT close_site``: step 1 ``ensure_site_days``; steps
    2 and 3 the missed days and a due attempt, oldest day first (5.1, 5.2);
    step 4 the repair (8.2); step 5 a due publication retry (9.4). A site
    whose due instant or steps raise begins nothing this tick and the other
    sites go on (``_in_site_savepoint``). When the tick moved a day to
    ``missed``, raw retention runs once after the last site (8.4).
    """
    if not daily_close_on(conn):
        return
    worker_started_at = get_runtime_state(conn, "worker_started_at")
    rounds: dict[tuple[int, date], close_p4.RoundState] = {}
    order: list[tuple[datetime, int]] = []
    sites = conn.execute("SELECT id FROM sites WHERE enabled = 1 ORDER BY id")
    for row in sites.fetchall():
        site_id = int(row[0])
        due = _in_site_savepoint(
            conn,
            site_id,
            now_utc,
            partial(_site_due_instant, conn, site_id, now_utc, rounds),
        )
        if due is not None:
            order.append((due, site_id))
    moved = False
    for _due, site_id in sorted(order):
        steps = partial(_site_steps, conn, site_id, now_utc, worker_started_at, rounds)
        if _in_site_savepoint(conn, site_id, now_utc, steps):
            moved = True
    if moved:
        run_close_retention(conn, now_utc=now_utc)


# --- a stop or restart: settle_interrupted_closes ----------------------------


def _in_settle_savepoint(
    conn: sqlite3.Connection,
    what: str,
    site_id: int,
    local_date: str,
    settle: Callable[[_SiteStep], None],
) -> None:
    """Run one day's or one record's settling under ``SAVEPOINT close_settle``.

    The catch of plan 8.5, as 5.5 copies it: an exception is re-raised when
    the transaction is gone or it is a database error
    (``_database_failing``); any other is rolled back to the savepoint with
    one ERROR line naming the site, the date and the step, and the day or
    record is left as it was.
    """
    step = _SiteStep()
    conn.execute("SAVEPOINT close_settle")
    try:
        settle(step)
    except Exception as exc:
        if not conn.in_transaction or _database_failing(exc):
            raise
        conn.execute("ROLLBACK TO close_settle")
        conn.execute("RELEASE close_settle")
        logger.error(
            "daily close: settling the %s %s of site %s failed at step %s (%s);"
            " it is left as it was",
            what,
            local_date,
            site_id,
            step.number,
            type(exc).__name__,
        )
        return
    conn.execute("RELEASE close_settle")


def _settle_day(
    conn: sqlite3.Connection,
    site_id: int,
    day_text: str,
    last_loop_at: str | None,
    now_utc: datetime,
    step: _SiteStep,
) -> None:
    """Steps 1 to 4 of plan 5.5 for one ``closing`` day.

    1. L, the stop's time: the later of ``worker_last_loop_at`` and the
       attempt record's ``last_progress_at``. A day with no attempt record
       (a P1 record only) has the day's ``state_changed_at``, when
       ``begin_close`` ran, in place of ``last_progress_at``.
    2. ``fail_close`` at L with reason "stopped during stage N", N the stage
       after the attempt's ``last_stage``; the attempt record is closed with
       N, the reason and L (11.3). No stage-log row: a stage cut short by a
       stop writes none (11.1).
    3. After attempt 1, a ``retry_at`` this start is later than was not
       covered by a running add-on (5.2): ``mark_missed`` ``retry_not_run``.
    4. Every ``pending`` or ``running`` ``day_close`` job of the day is
       completed without running (``complete``, no success marker).
    """
    local_date = date.fromisoformat(day_text)
    key = (site_id, local_date.isoformat())
    row = conn.execute(
        "SELECT d.attempts, d.state_changed_at, a.last_stage, a.last_progress_at"
        " FROM site_days d LEFT JOIN close_attempts a ON a.site_id = d.site_id"
        " AND a.local_date = d.local_date AND a.attempt = d.attempts"
        " WHERE d.site_id = ? AND d.local_date = ?",
        key,
    ).fetchone()
    attempt = int(row[0])
    progress_at = row[1] if row[3] is None else row[3]
    known = [parse_utc(str(progress_at))]
    if last_loop_at is not None:
        known.append(parse_utc(last_loop_at))
    stop_at = max(known)
    step.number = 2
    stage = None if row[2] is None else int(row[2]) + 1
    reason = (
        "stopped during the attempt"
        if stage is None
        else f"stopped during stage {stage}"
    )
    state = fail_close(conn, site_id, local_date, now_utc=stop_at, reason=reason)
    conn.execute(
        "UPDATE close_attempts SET outcome = 'failed', failed_stage = ?,"
        " failure_reason = ?, ended_at = ?"
        " WHERE site_id = ? AND local_date = ? AND attempt = ?",
        (stage, reason, isoformat_utc_micro(stop_at), *key, attempt),
    )
    if state == "retry_due":
        step.number = 3
        record = day_record(conn, site_id, local_date)
        if record is None or record.retry_at is None:
            raise RuntimeError("the failed attempt left no retry time")
        if now_utc > record.retry_at:
            mark_missed(
                conn, site_id, local_date, now_utc=now_utc, reason="retry_not_run"
            )
    step.number = 4
    jobs = conn.execute(
        "SELECT id FROM jobs WHERE type = 'day_close' AND site_id = ?"
        " AND status IN ('pending', 'running') AND job_key LIKE ?",
        (site_id, f"close:{site_id}:{local_date.isoformat()}:%"),
    ).fetchall()
    for job in jobs:
        complete(conn, int(job[0]))


def _settle_publication(
    conn: sqlite3.Connection,
    site_id: int,
    cutoff_text: str,
    last_loop_at: str | None,
    now_utc: datetime,
    step: _SiteStep,
) -> None:
    """Plan 9.4's stop during stage 7: a ``building`` record failed at L.

    1. L: the later of ``worker_last_loop_at`` and the record's
       ``started_at``, stage 7's saved start (its ``opened_at`` if none).
    2. After attempt 1, ``retry_due`` with ``retry_at`` = L + ``RETRY_DELAY``;
       after attempt 2, ``failed``. Reason "stopped during stage 7"; no
       stage-log row (11.1).
    3. A ``retry_at`` this start is later than was not covered by a running
       add-on (5.2): ``failed`` with reason ``retry_not_run``, as the tick's
       publication retry writes it (8.5 step 5).
    """
    cutoff = date.fromisoformat(cutoff_text)
    record = publication_record(conn, site_id=site_id, cutoff=cutoff)
    if record is None or record.state != "building":
        raise RuntimeError("the publication record is no longer building")
    known = [record.started_at or record.opened_at]
    if last_loop_at is not None:
        known.append(parse_utc(last_loop_at))
    stop_at = max(known)
    step.number = 2
    key = (site_id, cutoff.isoformat())
    reason = "stopped during stage 7"
    if record.attempt != 1:
        conn.execute(
            "UPDATE close_publications SET state = 'failed', finished_at = ?,"
            " last_failure = ? WHERE site_id = ? AND cutoff = ?",
            (isoformat_utc_micro(stop_at), reason, *key),
        )
        return
    retry_at = stop_at + RETRY_DELAY
    conn.execute(
        "UPDATE close_publications SET state = 'retry_due', retry_at = ?,"
        " last_failure = ? WHERE site_id = ? AND cutoff = ?",
        (isoformat_utc_micro(retry_at), reason, *key),
    )
    step.number = 3
    if now_utc > retry_at:
        conn.execute(
            "UPDATE close_publications SET state = 'failed', finished_at = ?,"
            " last_failure = 'retry_not_run' WHERE site_id = ? AND cutoff = ?",
            (isoformat_utc_micro(now_utc), *key),
        )


def settle_interrupted_closes(conn: sqlite3.Connection, *, now_utc: datetime) -> None:
    """Settle the close work a stop cut short, in the caller's transaction.

    Plan 5.5 and 9.4. Does nothing while the close is off (13.1). ``lifespan``
    calls it at each start, after ``reclaim_all_stale`` and before the
    ``worker_started_at`` write; ``import_db`` calls it on the staged file
    before the swap (12.6), so ``now_utc`` is the start or the import's
    time. Each ``closing`` day, oldest first, then each ``building``
    publication record, oldest cutoff first, is settled under its own
    ``SAVEPOINT close_settle`` (``_settle_day``, ``_settle_publication``);
    one that raises is left as it was, with one ERROR line, and the next
    goes on. Then raw retention runs once (8.4). Never commits.
    """
    if not daily_close_on(conn):
        return
    last_loop_at = get_runtime_state(conn, "worker_last_loop_at")
    days = conn.execute(
        "SELECT site_id, local_date FROM site_days WHERE state = 'closing'"
        " ORDER BY local_date, site_id"
    ).fetchall()
    for row in days:
        site_id, day_text = int(row[0]), str(row[1])
        settle = partial(_settle_day, conn, site_id, day_text, last_loop_at, now_utc)
        _in_settle_savepoint(conn, "day", site_id, day_text, settle)
    records = conn.execute(
        "SELECT site_id, cutoff FROM close_publications WHERE state = 'building'"
        " ORDER BY cutoff, site_id"
    ).fetchall()
    for row in records:
        site_id, cutoff_text = int(row[0]), str(row[1])
        settle = partial(
            _settle_publication, conn, site_id, cutoff_text, last_loop_at, now_utc
        )
        _in_settle_savepoint(conn, "publication", site_id, cutoff_text, settle)
    run_close_retention(conn, now_utc=now_utc)
