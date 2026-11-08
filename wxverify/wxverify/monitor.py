"""HA-native monitor: on-request threshold verdict over the add-on's SQLite DB.

Pure module — it owns no process state and starts nothing. The one process
fact it reports, ``export_sweeper_dead``, is a caller-supplied INPUT, exactly
as ``now`` is. The DB groups' checks are read-only COUNT/EXISTS queries; the
always-on ``process`` group runs none. ``build_verdict`` assembles the verdict
envelope, honours the per-group toggles (which cover the DB groups only —
``process`` has no query cost for a toggle to remove), applies the 10-min
post-start grace to group 1, and maps a genuine ``sqlite3.Error`` on read to
``db_readable:false`` / ``overall:critical``.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final

from wxverify.close_monitor import (
    CloseAlert,
    access_days_affected,
    close_alerts,
    station_silent,
)
from wxverify.collection.budget import (
    current_billing_day,
    effective_daily_call_limit,
)
from wxverify.collection.forecast_fetcher import NO_USABLE_SAMPLES_SENTINEL
from wxverify.core.secrets import resolve_secret
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.queue import MAIN_LANE_TYPE_SQL
from wxverify.worker.access_hold import (
    ACCESS_KIND_LABELS,
    AccessRecord,
    refused_kinds,
)

# --- Hardcoded thresholds (standalone's proven defaults) ---------------------
FEED_STALE_HOURS = 12
FETCH_FEED_LIVE_HOURS = 12
FAILED_JOB_AGE_HOURS = 48
STUCK_RUNNING_MINUTES = 20
PENDING_OVERDUE_MINUTES = 15
MAIN_WORKER_LIVENESS_MINUTES: Final = 15
OBS_COLLECTION_DELAY_MINUTES: Final = 15
GRACE_MINUTES = 10
COSTED_NOOP_MIN_ERRORS = 3

_SEVERITY_RANK = {"ok": 0, "warning": 1, "critical": 2}

# Keyed forecast providers whose feed rows appear in `feeds.source`. `open-meteo`
# is keyless (absent from SECRET_ENV) and never trips key_missing. `weathercom`
# is the PWS/observation provider (no forecast feed rows) — handled separately.
_KEYED_FORECAST_SOURCES = (
    "meteoblue",
    "visualcrossing",
    "openweathermap",
    "weatherapi",
    "meteosource",
    "google",
)


@dataclass(frozen=True)
class Condition:
    id: str
    group: str
    ok: bool
    skipped: bool
    severity: str
    count: int | None = None
    detail: str | None = None

    def as_dict(self) -> dict[str, object]:
        out: dict[str, object] = {
            "id": self.id,
            "group": self.group,
            "ok": self.ok,
            "skipped": self.skipped,
            "severity": self.severity,
        }
        if self.count is not None:
            out["count"] = self.count
        if self.detail is not None:
            out["detail"] = self.detail
        return out


def _skipped(cond_id: str, group: str, severity: str) -> Condition:
    return Condition(id=cond_id, group=group, ok=True, skipped=True, severity=severity)


def _close_error_condition(exc: Exception) -> Condition:
    """The row a close-alert call's own ``try`` adds for a non-SQLite fault.

    The ``unexpected_error`` shape ``error_verdict`` builds (daily-close plan
    P5 §5.1); the group's other rows stay in place. Nothing is logged.
    """
    return Condition(
        id="unexpected_error",
        group="monitor",
        ok=False,
        skipped=False,
        severity="critical",
        detail=f"close alerts: {type(exc).__name__}: {exc}",
    )


# Filled in by Tasks 4-6. Each returns a list[Condition]; grace_active is passed
# to the pipeline group so it can force ok=True during the post-start window.
_ELIGIBLE_FEED_WHERE = """
    s.enabled = 1
    AND f.enabled = 1
    AND f.is_virtual = 0
    AND NOT (f.source='meteoblue' AND f.model != 'multimodel')
    AND COALESCE(sfs.enabled, f.default_subscribed) = 1
"""

# Failed arm of problem_jobs: one row per (type, site_id, job_key) scope that
# holds an unresolved terminal failure at or before the bound cutoff
# (failed_cutoff, from FAILED_JOB_AGE_HOURS). "Unresolved" depends on the
# type. The NOT IN list is the ALLOWLIST of single-row job types -- the ones
# for which the worker's dispatch returns None, so one job row is one unit
# of work and a later GENUINE success in the scope is a real recovery. A
# genuine success is a completed row carrying result='ok', which the worker
# writes only on a normal dispatch return; the worker's
# JobCancelled branch (target deleted or disabled, a feed adapter that cannot
# be built, or an hourly cycle stopped by a refused-access hold) completes
# with result NULL, as did every completion before the marker existed, and
# none of those rows resolves anything.
# Start-up deletes the rows of every job type dispatch does not run, so no
# other type is left to classify.
# The daily close's two kinds are classified apart (daily-close P3 9.6). A
# failed day_close stage is left out of the arm: the day's state and attempt
# record report it, and its scope (close:{site_id}:{local_date}:{stage}) is
# the same for both attempts, so a later completion says nothing about the
# day. A failed day_publish job, the chain's last stage 7 (key
# publish:{site_id}:7), is left out too: a failure fail_publication records
# moves the publication record, whose state reports it, and a stale job's
# failure has its WARNING line.
# A feed's two download keys are one scope (daily-close P4 4.4). A round try
# (key round:{feed_id}) has max_retries 0, so a try that raises ends failed,
# while the feed's daytime downloads complete under fetch:{feed_id}; resolved
# only by a later ok row of its own key, a failed round try would count from
# FAILED_JOB_AGE_HOURS until the purge even after the feed's next download
# stored. So a failed fetch_feed row keyed with either prefix is resolved
# also by a later completed result='ok' row keyed with the other (both
# prefixes are six characters, so substr(job_key, 7) is the feed id; the
# lookup r probes idx_jobs_type_key_site), and a round: key groups as
# fetch:, so a feed counts once. A failed round try thus stops counting once
# any later download of the feed completes ok, round try or daytime, and a
# feed whose downloads keep failing counts once.
# A new job type stays OFF this list until it is classified. The
# rule is explained where it is used, in _pipeline_conditions. Module-level so
# the query-plan regression suite plans the statement that ships, not a copy.
FAILED_SCOPES_SQL = """
    SELECT COUNT(*) FROM (
        SELECT 1 FROM jobs AS j
        WHERE j.status = 'failed' AND j.updated_at <= ?
          AND j.type != 'day_publish'
          AND j.type != 'day_close'
          AND (j.type NOT IN ('fetch_feed', 'fetch_obs', 'fetch_current_obs')
               OR NOT EXISTS (
                   SELECT 1 FROM jobs AS k
                   WHERE k.type = j.type
                     AND k.site_id IS j.site_id
                     AND k.job_key IS j.job_key
                     AND k.status = 'completed'
                     AND k.result = 'ok'
                     AND k.id > j.id))
          AND NOT (j.type = 'fetch_feed'
                   AND substr(j.job_key, 1, 6) IN ('fetch:', 'round:')
                   AND EXISTS (
                       SELECT 1 FROM jobs AS r
                       WHERE r.type = 'fetch_feed'
                         AND r.site_id IS j.site_id
                         AND r.job_key = (CASE substr(j.job_key, 1, 6)
                                          WHEN 'round:' THEN 'fetch:' ELSE 'round:' END)
                                         || substr(j.job_key, 7)
                         AND r.status = 'completed'
                         AND r.result = 'ok'
                         AND r.id > j.id))
        GROUP BY j.type, j.site_id,
                 CASE WHEN j.type = 'fetch_feed' AND substr(j.job_key, 1, 6) = 'round:'
                      THEN 'fetch:' || substr(j.job_key, 7) ELSE j.job_key END
    )
"""


def _count(conn: sqlite3.Connection, sql: str, params: tuple[object, ...]) -> int:
    row = conn.execute(sql, params).fetchone()
    return 0 if row is None else int(row[0])


# The due-scan's failure cooldown for fetch_current_obs, mirrored from
# worker.scheduler._DUE_JOB_FAILURE_COOLDOWN (enqueue_due_current_obs).
_CURRENT_OBS_FAILURE_COOLDOWN: Final = timedelta(hours=1)

_OBS_COLLECTION_SQL: Final = """
    SELECT st.id,
           COALESCE(sps.next_poll_at, sps.updated_at, st.created_at) AS due_basis,
           (SELECT j.next_attempt_at FROM jobs j
            WHERE j.type = 'fetch_current_obs' AND j.job_key = 'curobs:' || st.id
              AND j.site_id IS st.site_id AND j.status = 'pending'
            ORDER BY j.id DESC LIMIT 1) AS pending_attempt,
           latest.status AS latest_status, latest.updated_at AS latest_updated_at,
           EXISTS (SELECT 1 FROM jobs a
                   WHERE a.type = 'fetch_current_obs'
                     AND a.job_key = 'curobs:' || st.id
                     AND a.site_id IS st.site_id
                     AND a.status IN ('pending', 'running')) AS has_active
    FROM stations st JOIN sites s ON s.id = st.site_id
    LEFT JOIN station_poll_state sps ON sps.station_id = st.id
    LEFT JOIN jobs latest ON latest.id = (
        SELECT j2.id FROM jobs j2
        WHERE j2.type = 'fetch_current_obs' AND j2.job_key = 'curobs:' || st.id
          AND j2.site_id IS st.site_id
        ORDER BY j2.id DESC LIMIT 1)
    WHERE st.enabled = 1 AND s.enabled = 1
"""


def _parse_or_none(value: object) -> datetime | None:
    """``parse_utc(str(value))``, or ``None`` for NULL or a value that fails.

    ``str()`` first, so a non-text value such as an imported BLOB gives
    ``None``; ``parse_utc`` raises only ``ValueError``.
    """
    if value is None:
        return None
    try:
        return parse_utc(str(value))
    except ValueError:
        return None


def _obs_collection_delays(
    conn: sqlite3.Connection, now: datetime
) -> tuple[int, int, datetime | None]:
    """Current-obs collection lateness over enabled stations of enabled sites.

    Returns ``(overdue, in_backoff, oldest)``. A station's effective next
    attempt is the latest parseable of its due basis, its pending job's
    ``next_attempt_at`` and, when its latest job failed and none is active,
    the failure-cooldown end. A cooldown end past ``datetime``'s range
    saturates at ``datetime.max``, a cooldown that has not ended, as the
    scheduler's own check still suppresses the enqueue there. It is overdue
    once ``now`` reaches the effective time plus
    ``OBS_COLLECTION_DELAY_MINUTES``, or when its due basis does not parse
    (fail toward alerting; such a station has no stamp). It is in backoff
    while its due basis alone is overdue but the effective time is not, and
    is then excluded from ``overdue``. ``oldest`` is the earliest effective
    time among the overdue stations that have one.
    """
    cutoff = now - timedelta(minutes=OBS_COLLECTION_DELAY_MINUTES)
    overdue = 0
    in_backoff = 0
    oldest: datetime | None = None
    for row in conn.execute(_OBS_COLLECTION_SQL):
        due = _parse_or_none(row[1])
        if due is None:
            overdue += 1
            continue
        effective = due
        pending = _parse_or_none(row[2])
        if pending is not None and pending > effective:
            effective = pending
        failed_at = _parse_or_none(row[4]) if row[3] == "failed" else None
        if failed_at is not None and not row[5]:
            try:
                cooldown_end = failed_at + _CURRENT_OBS_FAILURE_COOLDOWN
            except OverflowError:
                # Past datetime's range: saturate, since the cooldown has not
                # ended -- the scheduler's utc_now() - updated_at < cooldown
                # still holds for a stamp this far ahead.
                cooldown_end = datetime.max.replace(tzinfo=UTC)
            if cooldown_end > effective:
                effective = cooldown_end
        if effective <= cutoff:
            overdue += 1
            if oldest is None or effective < oldest:
                oldest = effective
        elif due <= cutoff:
            in_backoff += 1
    return (overdue, in_backoff, oldest)


# The main lane's liveness evidence, read in ONE compound statement so every
# value comes from one read snapshot: with two reads, a long job could finish
# between them, and a pre-job loop stamp would pair with no running row -- a
# trip on a healthy lane. Each runtime_state row is ('state', key, value,
# updated_at); each running main-lane row is ('job', NULL, NULL, updated_at).
# The lane filter is the claim partition's own MAIN_LANE_TYPE_SQL, so a
# running fetch_current_obs row never counts, and the poller heartbeat is not
# read.
_LIVENESS_EVIDENCE_SQL: Final = f"""
    SELECT 'state', key, value, updated_at FROM runtime_state
     WHERE key IN ('worker_started_at', 'worker_last_loop_at')
    UNION ALL
    SELECT 'job', NULL, NULL, updated_at FROM jobs
     WHERE status = 'running' AND {MAIN_LANE_TYPE_SQL}
"""


@dataclass(frozen=True)
class _LivenessEvidence:
    started: datetime | None  # W: worker_started_at's value
    last_loop: datetime | None  # S: worker_last_loop_at's value
    running_claims: tuple[datetime, ...]  # C: running main-lane rows' updated_at


def _main_liveness_evidence(conn: sqlite3.Connection) -> _LivenessEvidence:
    """Read the main lane's liveness evidence with ``_LIVENESS_EVIDENCE_SQL``.

    Rows are read by position, so tuple rows and ``sqlite3.Row`` both work.
    Every value goes through ``_parse_or_none``, so a missing or unparseable
    stamp is ``None``, and a running row whose ``updated_at`` does not parse
    is left out of ``running_claims`` (fail toward alerting). A
    ``sqlite3.Error`` is not caught: ``build_verdict`` reports it as
    ``db_readable`` false.
    """
    started: datetime | None = None
    last_loop: datetime | None = None
    running_claims: list[datetime] = []
    for row in conn.execute(_LIVENESS_EVIDENCE_SQL):
        if row[0] == "job":
            claimed = _parse_or_none(row[3])
            if claimed is not None:
                running_claims.append(claimed)
        elif row[1] == "worker_started_at":
            started = _parse_or_none(row[2])
        elif row[1] == "worker_last_loop_at":
            last_loop = _parse_or_none(row[2])
    return _LivenessEvidence(
        started=started,
        last_loop=last_loop,
        running_claims=tuple(running_claims),
    )


def _main_liveness_check(
    evidence: _LivenessEvidence, now: datetime
) -> tuple[bool, str]:
    """``(tripped, detail)`` for ``main_worker_liveness``; pure, no I/O.

    F is the worker start, if it parses. S* is the loop stamp if it parses
    and is not older than F, and the basis B is S*, else F. A running
    main-lane row is current if it was claimed at or after B, with no upper
    cap: how long a job may run is for the running arm of ``problem_jobs``
    to judge. Tripped when B is missing,
    or when B is at least ``MAIN_WORKER_LIVENESS_MINUTES`` old and no
    running row is current. Evidence in the future is not clamped, so it
    reads ok. The detail says which evidence is stale or missing; it never
    says the worker is hung, since a lane whose stamp write fails may still
    be running jobs.
    """
    floor = evidence.started
    last_loop = evidence.last_loop
    if last_loop is not None and (floor is None or last_loop >= floor):
        basis = last_loop
        detail = (
            "main-worker liveness evidence stale: last loop stamp"
            f" {isoformat_utc(last_loop)};"
            " no main-lane job claimed since then is running"
        )
    elif floor is not None:
        basis = floor
        detail = (
            "main-worker liveness evidence missing: no loop stamp since worker"
            f" start at {isoformat_utc(floor)};"
            " no main-lane job claimed since then is running"
        )
    else:
        return (
            True,
            "main-worker liveness evidence missing: no loop stamp or worker start time",
        )
    current = any(claimed >= basis for claimed in evidence.running_claims)
    stale = now - basis >= timedelta(minutes=MAIN_WORKER_LIVENESS_MINUTES)
    return (stale and not current, detail)


def _has_completed_within(conn: sqlite3.Connection, job_type: str, cutoff: str) -> bool:
    """A completed ``job_type`` row since ``cutoff``."""
    row = conn.execute(
        """
        SELECT 1 FROM jobs
        WHERE status='completed' AND type=? AND updated_at >= ?
        LIMIT 1
        """,
        (job_type, cutoff),
    ).fetchone()
    return row is not None


def _pipeline_conditions(
    conn: sqlite3.Connection, now: datetime, *, grace_active: bool
) -> list[Condition]:
    feed_cutoff = isoformat_utc(now - timedelta(hours=FEED_STALE_HOURS))
    fetch_feed_cutoff = isoformat_utc(now - timedelta(hours=FETCH_FEED_LIVE_HOURS))

    # feed_stale: eligible feed with last_run_at > 12h ago OR NULL.
    feed_stale_n = _count(
        conn,
        f"""
        SELECT COUNT(*) FROM sites s
        JOIN feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = s.id AND sfs.feed_id = f.id
        WHERE {_ELIGIBLE_FEED_WHERE}
          AND (sfs.last_run_at IS NULL OR sfs.last_run_at < ?)
        """,
        (feed_cutoff,),
    )

    eligible_feeds = _count(
        conn,
        f"""
        SELECT COUNT(*) FROM sites s
        JOIN feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = s.id AND sfs.feed_id = f.id
        WHERE {_ELIGIBLE_FEED_WHERE}
        """,
        (),
    )
    fetch_feed_live_tripped = eligible_feeds > 0 and not _has_completed_within(
        conn, "fetch_feed", fetch_feed_cutoff
    )

    # problem_jobs = failed scopes + in-flight problems. The failed arm counts
    # scopes -- (type, site_id, job_key), the queue's own identity in
    # LATEST_JOB_SQL and idx_jobs_active_dedupe -- that hold an unresolved
    # terminal failure older than FAILED_JOB_AGE_HOURS. For the single-row job
    # types listed in FAILED_SCOPES_SQL a later completed row in the same scope
    # that carries the success marker (result='ok', written only on a normal
    # dispatch return) supersedes the failure, but only a genuine success: a
    # cancelled or unavailable completion, and every row written before the
    # marker existed, has result NULL and resolves nothing.
    # The daily close's failed day_close stages and failed day_publish jobs (its
    # last stage 7, key publish:{site_id}:7) are left out of the arm: the
    # day's state and attempt record, or the publication record's state,
    # report them, and a stale job's failure its WARNING line. The failed
    # rows themselves stay until purge_failed_jobs_older_than removes
    # them, so retention is unchanged and the count cannot be cleared by
    # anything but a real success. "Later" is id order, matched NULL-safely
    # with IS because job_key and site_id are nullable. The in-flight arm
    # below counts per row: pending overdue by at least 15 min, and running
    # claimed at least 20 min ago, every kind alike.
    # The pending arm mirrors claim_next_job's claim contract, which
    # treats `next_attempt_at IS NULL OR next_attempt_at <= now` as claimable —
    # so a stuck pending job can carry next_attempt_at=NULL. COALESCE(...,
    # updated_at) folds that NULL case in (a NULL-attempt pending job is overdue
    # once its updated_at is older than the cutoff), where a bare
    # `next_attempt_at <= ?` would silently miss it. The typeof/GLOB clause
    # folds in the sibling blind spot: a non-ISO next_attempt_at (e.g. 'zzzz')
    # sorts AFTER every cutoff string, so a plain `<=` comparison would never
    # flag it either -- this surfaces that row as overdue instead of letting
    # it go uncounted forever. db.sanitize's boot pass fixes such rows
    # going forward; this is the operator-visible net for one that slips in
    # between those passes (a live write racing the pass, or a value crafted
    # to sort before "now" and pass the pass's own `parse_utc` check trivially
    # while still confusing some other consumer). Guarded by `IS NOT NULL`:
    # typeof(NULL) is 'NULL', not 'text', so without that guard this arm
    # would also match every NULL-attempt row unconditionally -- NULL is
    # already handled, on its own recency cutoff, by the COALESCE arm above.
    # The shape check is deliberately the full canonical prefix (date, 'T',
    # time) rather than the year alone: a year-prefixed garbage value like
    # '9999-not-a-timestamp' passes the loose check yet still sorts after
    # every cutoff, escaping both arms. The trailing wildcard admits both
    # live stamp shapes (with and without fractional seconds) and the +00:00
    # offset a foreign database can carry; the accepted cost is that a
    # parseable-but-non-canonical stamp (e.g. a space in place of the `T` separator,
    # which the sanitizer leaves in place because it parses) is over-flagged
    # for the remaining window of the job's own delay.
    failed_cutoff = isoformat_utc(now - timedelta(hours=FAILED_JOB_AGE_HOURS))
    stuck_cutoff = isoformat_utc(now - timedelta(minutes=STUCK_RUNNING_MINUTES))
    pending_cutoff = isoformat_utc(now - timedelta(minutes=PENDING_OVERDUE_MINUTES))
    failed_scopes_n = _count(conn, FAILED_SCOPES_SQL, (failed_cutoff,))
    inflight_n = _count(
        conn,
        """
        SELECT COUNT(*) FROM jobs
        WHERE (status='running' AND updated_at <= ?)
           OR (status='pending'
               AND (COALESCE(next_attempt_at, updated_at) <= ?
                    OR (next_attempt_at IS NOT NULL
                        AND (typeof(next_attempt_at) != 'text'
                             OR next_attempt_at NOT GLOB
                                '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]*'))))
        """,
        (stuck_cutoff, pending_cutoff),
    )
    problem_jobs_n = failed_scopes_n + inflight_n

    # obs_collection_delayed (D1.3): enabled stations of enabled sites whose
    # current-obs poll is overdue. Reads schedule and job rows only, never the
    # poller heartbeat, so a dead or stalled poller cannot hold it green.
    (
        collection_overdue_n,
        collection_backoff_n,
        collection_oldest,
    ) = _obs_collection_delays(conn, now)
    collection_oldest_text = (
        "unknown" if collection_oldest is None else isoformat_utc(collection_oldest)
    )

    # main_worker_liveness: the main lane's loop stamp is at least
    # MAIN_WORKER_LIVENESS_MINUTES old and no main-lane job claimed since it
    # is still running. That can indicate a loop stalled between jobs, which
    # leaves no row running for problem_jobs to see; a wedge inside a job
    # stays with the running arm above. The main lane only: the poller lane is
    # covered by obs_collection_delayed.
    liveness_tripped, liveness_detail = _main_liveness_check(
        _main_liveness_evidence(conn), now
    )

    # station_silent_today (daily-close plan P5 §6.3) and close_alerts
    # (§5.1). Each call's own `try` keeps a fault in it from dropping the
    # group's other rows: a sqlite3.Error still reaches the group's `try` in
    # build_verdict (db_readable), and any other exception becomes one
    # unexpected_error row in place of that call's rows.
    silent: CloseAlert | None = None
    close_rows: list[CloseAlert] = []
    close_errors: list[Condition] = []
    try:
        silent = station_silent(conn, now)
    except sqlite3.Error:
        raise
    except Exception as exc:
        close_errors.append(_close_error_condition(exc))
    try:
        close_rows = close_alerts(conn, now)
    except sqlite3.Error:
        raise
    except Exception as exc:
        close_errors.append(_close_error_condition(exc))

    def _cond(cid: str, tripped: bool, count: int | None, detail: str) -> Condition:
        if grace_active:
            return Condition(
                id=cid,
                group="pipeline",
                ok=True,
                skipped=False,
                severity="warning",
                count=count,
            )
        return Condition(
            id=cid,
            group="pipeline",
            ok=not tripped,
            skipped=False,
            severity="warning",
            count=count,
            detail=detail if tripped else None,
        )

    def _provider_down_cond(alert: CloseAlert) -> Condition:
        # Alert 7's own builder (§5.1, §6.6): `_cond` drops `detail` while
        # ok, and alert 7's "recovered at" line shows while ok. The start-up
        # grace still applies: during it the row is `_cond`'s grace row.
        if grace_active:
            return _cond(alert.id, alert.tripped, alert.count, alert.detail or "")
        return Condition(
            id=alert.id,
            group="pipeline",
            ok=not alert.tripped,
            skipped=False,
            severity="warning",
            count=alert.count,
            detail=alert.detail,
        )

    conditions = [
        _cond(
            "feed_stale",
            feed_stale_n > 0,
            feed_stale_n,
            f"{feed_stale_n} feeds not run >12h",
        ),
        _cond(
            "fetch_feed_live",
            fetch_feed_live_tripped,
            None,
            "no completed fetch_feed in 12h",
        ),
        _cond(
            "problem_jobs",
            problem_jobs_n > 0,
            problem_jobs_n,
            f"{failed_scopes_n} unresolved failed scopes,"
            f" {inflight_n} stuck/overdue jobs",
        ),
        _cond(
            "obs_collection_delayed",
            collection_overdue_n > 0,
            collection_overdue_n,
            f"{collection_overdue_n} stations overdue"
            f" at least {OBS_COLLECTION_DELAY_MINUTES} min;"
            f" oldest due {collection_oldest_text};"
            f" {collection_backoff_n} in backoff",
        ),
        _cond("main_worker_liveness", liveness_tripped, None, liveness_detail),
    ]
    if silent is not None:
        conditions.append(
            _cond(
                "station_silent_today",
                silent.tripped,
                silent.count,
                silent.detail or "",
            )
        )
    # wind_station_constant:<station>: one failing row per open stuck-wind
    # episode of an enabled station of an enabled site; a station with no
    # episode has no row, so its id enters the failing list when its episode
    # opens and leaves it when it clears.
    for row in conn.execute(
        """
        SELECT st.pws_station_id, w.speed_kmh, w.since, w.state
        FROM station_wind_constant w
        JOIN stations st ON st.id = w.station_id
        JOIN sites s ON s.id = st.site_id
        WHERE st.enabled = 1 AND s.enabled = 1
        ORDER BY st.id
        """
    ).fetchall():
        pws = str(row["pws_station_id"])
        cant_assess = " (can't assess)" if row["state"] == "cant_assess" else ""
        conditions.append(
            _cond(
                f"wind_station_constant:{pws}",
                True,
                None,
                "suspected: constant reading while other stations report wind:"
                f" station {pws} {row['speed_kmh']} km/h since {row['since']}"
                f"{cant_assess}",
            )
        )
    # Each close alert as a pipeline row, severity warning, through `_cond`,
    # so the start-up grace holds for it as for every pipeline row (§5.1);
    # alert 7 through its own builder, which keeps its recovered line.
    conditions.extend(
        _provider_down_cond(alert)
        if alert.id == "provider_down"
        else _cond(alert.id, alert.tripped, alert.count, alert.detail or "")
        for alert in close_rows
    )
    conditions.extend(close_errors)
    return conditions


_ACTIVE_FEED_WHERE = """
    s.enabled = 1
    AND f.enabled = 1
    AND COALESCE(sfs.enabled, f.default_subscribed) = 1
"""


def _budget_conditions(conn: sqlite3.Connection, now: datetime) -> list[Condition]:
    # budget_calls / budget_credits: current billing-day row per source.
    sources = conn.execute(
        "SELECT source, daily_call_limit, daily_credit_limit, billing_tz FROM sources"
    ).fetchall()
    calls_tripped = 0
    credits_tripped = 0
    # Each tripped source by name; one whose effective cap sits below its
    # configured limit names both (plan §8.6).
    calls_tripped_names: dict[str, str] = {}
    for row in sources:
        source = str(row["source"])
        day = current_billing_day(str(row["billing_tz"]))
        budget = conn.execute(
            "SELECT calls, credits FROM api_budget WHERE source=? AND billing_day=?",
            (source, day),
        ).fetchone()
        calls = 0 if budget is None else int(budget["calls"])
        credits = 0 if budget is None else int(budget["credits"])
        configured = int(row["daily_call_limit"])
        cap = effective_daily_call_limit(source, configured)
        if calls >= cap:
            calls_tripped += 1
            calls_tripped_names[source] = (
                source
                if cap == configured
                else f"{source} (provisional provider allowance {cap};"
                f" configured {configured})"
            )
        if row["daily_credit_limit"] is not None and credits >= int(
            row["daily_credit_limit"]
        ):
            credits_tripped += 1

    # domain_backoffs: any active backoff (no active-provider filter, by design).
    # Counted in Python, not with SQL `next_attempt_at > ?`: that comparison is
    # lexical, so an unreadable carrier from an imported database ('abc', a BLOB)
    # sorts above every ISO stamp and would be reported as an active backoff that
    # check_domain_backoff deliberately ignores. One row per provider domain, so
    # the row-by-row read is bounded by the provider count.
    backoffs_n = 0
    for row in conn.execute("SELECT next_attempt_at FROM domain_backoffs"):
        try:
            due = parse_utc(str(row["next_attempt_at"]))
        except ValueError:
            continue
        if due > now:
            backoffs_n += 1

    # feed_errors: active feed with last_error set and != sentinel.
    feed_errors_n = _count(
        conn,
        f"""
        SELECT COUNT(*) FROM sites s
        JOIN feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = s.id AND sfs.feed_id = f.id
        WHERE {_ACTIVE_FEED_WHERE}
          AND sfs.last_error IS NOT NULL
          AND sfs.last_error != ?
        """,
        (NO_USABLE_SAMPLES_SENTINEL,),
    )

    # costed_noop: active feed with sentinel AND error_count >= N.
    costed_noop_n = _count(
        conn,
        f"""
        SELECT COUNT(*) FROM sites s
        JOIN feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = s.id AND sfs.feed_id = f.id
        WHERE {_ACTIVE_FEED_WHERE}
          AND sfs.last_error = ?
          AND sfs.error_count >= ?
        """,
        (NO_USABLE_SAMPLES_SENTINEL, COSTED_NOOP_MIN_ERRORS),
    )

    key_missing_n = _key_missing_count(conn)

    # weathercom_access_refused: every download kind whose latest answer was a
    # 401/403, listed until 24 h after its hold ended (``refused_kinds``).
    refused = refused_kinds(conn, now)
    access_detail = _access_refused_detail(refused, now) if refused else ""
    # The daily close's part of the row is appended: the next try and the
    # days affected (daily-close plan P5 §6.4), read only while a kind is
    # listed, since only then does the row show a detail. The call's own
    # `try` keeps a fault in it from dropping the group's other rows (§5.1):
    # a sqlite3.Error still reaches the group's `try` in build_verdict
    # (db_readable), and any other exception becomes one unexpected_error row
    # and leaves 0.16.13's line as it is.
    close_errors: list[Condition] = []
    if refused:
        try:
            access_detail += access_days_affected(conn, now).detail_suffix()
        except sqlite3.Error:
            raise
        except Exception as exc:
            close_errors.append(_close_error_condition(exc))

    def _c(cid: str, sev: str, n: int, detail: str) -> Condition:
        return Condition(
            id=cid,
            group="budget",
            ok=(n == 0),
            skipped=False,
            severity=sev,
            count=n,
            detail=detail if n else None,
        )

    return [
        _c(
            "budget_calls",
            "critical",
            calls_tripped,
            "daily call budget exhausted: "
            + ", ".join(
                calls_tripped_names[name] for name in sorted(calls_tripped_names)
            ),
        ),
        _c(
            "budget_credits",
            "critical",
            credits_tripped,
            "daily credit budget exhausted",
        ),
        _c("domain_backoffs", "warning", backoffs_n, "active provider backoff"),
        _c("feed_errors", "warning", feed_errors_n, "active feed error"),
        _c(
            "costed_noop",
            "warning",
            costed_noop_n,
            "active feed paying for zero usable data",
        ),
        _c(
            "key_missing",
            "warning",
            key_missing_n,
            "keyed provider exercised without a key",
        ),
        _c(
            "weathercom_access_refused",
            "warning",
            len(refused),
            access_detail,
        ),
        *close_errors,
    ]


def _access_refused_detail(refused: list[AccessRecord], now: datetime) -> str:
    """One line naming the refused kinds, the HTTP status and the hold's times.

    Labels follow ``refused`` (``ACCESS_KINDS`` order). Distinct readable
    statuses are joined in ascending order (``401/403``); with none, the
    status part is left out. An unreadable row (``until`` None) is named
    ``<label> (unreadable hold)`` and gives no time: ``since`` is the earliest
    and ``held until`` the latest of the readable rows, and with no readable
    ``until`` still in the future the line ends ``waiting for the next try``.
    """
    statuses = sorted({r.status for r in refused if r.status is not None})
    head = "weather.com access refused"
    if statuses:
        head += " (HTTP " + "/".join(str(s) for s in statuses) + ")"
    labels = ", ".join(
        ACCESS_KIND_LABELS[r.kind]
        if r.until is not None
        else f"{ACCESS_KIND_LABELS[r.kind]} (unreadable hold)"
        for r in refused
    )
    parts = [f"{head}: {labels}"]
    sinces = [r.since for r in refused if r.until is not None and r.since is not None]
    if sinces:
        parts.append("since " + min(sinces, key=parse_utc))
    untils = [r.until for r in refused if r.until is not None]
    latest = max(untils, key=parse_utc) if untils else None
    if latest is not None and parse_utc(latest) > now:
        parts.append("held until " + latest)
    else:
        parts.append("waiting for the next try")
    return "; ".join(parts)


def _key_missing_count(conn: sqlite3.Connection) -> int:
    missing = 0
    # Forecast providers: keyed source with an enabled+subscribed feed on an
    # enabled site, and no resolved key.
    for source in _KEYED_FORECAST_SOURCES:
        exercised = conn.execute(
            """
            SELECT 1 FROM sites s
            JOIN feeds f
            LEFT JOIN site_feed_state sfs
              ON sfs.site_id = s.id AND sfs.feed_id = f.id
            WHERE s.enabled = 1
              AND f.source = ?
              AND f.enabled = 1
              AND COALESCE(sfs.enabled, f.default_subscribed) = 1
            LIMIT 1
            """,
            (source,),
        ).fetchone()
        if exercised is not None and not resolve_secret(source):
            missing += 1
    # weathercom: PWS/obs provider — trip when >=1 enabled station on an
    # enabled site and no key.
    wc_exercised = conn.execute(
        """
        SELECT 1 FROM sites s
        WHERE s.enabled = 1
          AND EXISTS (
              SELECT 1 FROM stations st
              WHERE st.site_id = s.id AND st.enabled = 1
          )
        LIMIT 1
        """
    ).fetchone()
    if wc_exercised is not None and not resolve_secret("weathercom"):
        missing += 1
    return missing


def _db_conditions(conn: sqlite3.Connection, now: datetime) -> list[Condition]:
    # A trivial read. Reaching the return means the DB is readable. If the read
    # raises sqlite3.Error, this function never returns; build_verdict's db-group
    # `except sqlite3.Error` sets db_read_failed, drops any db_readable condition,
    # and appends db_readable:false / severity=critical (→ overall:critical).
    conn.execute("SELECT 1").fetchone()
    return [
        Condition(
            id="db_readable",
            group="db",
            ok=True,
            skipped=False,
            severity="critical",
        )
    ]


def _grace_active(conn: sqlite3.Connection, now: datetime) -> bool:
    row = conn.execute(
        "SELECT value FROM runtime_state WHERE key='worker_started_at'"
    ).fetchone()
    if row is None or row["value"] is None:
        return False
    from wxverify.core.timeutil import parse_utc

    try:
        started = parse_utc(str(row["value"]))
    except ValueError:
        # A corrupt (non-ISO) worker_started_at must not blow the whole verdict.
        # The outer route guard would map it to error_verdict, but degrading to
        # "grace not active" here keeps every other condition reportable; the
        # grace-suppression is fail-safe (worst case: group-1 conditions are
        # evaluated live rather than held ok for 10 min after a real start).
        return False
    return now < started + timedelta(minutes=GRACE_MINUTES)


# The pipeline group's skipped list while monitor_pipeline is off (daily-close
# plan P5 §5.1): the five kept pipeline ids, station_silent_today and
# close_alerts' seven ids, none of the four retired.
_PIPELINE_SKIPPED: Final[tuple[str, ...]] = (
    "feed_stale",
    "fetch_feed_live",
    "problem_jobs",
    "obs_collection_delayed",
    "main_worker_liveness",
    "station_silent_today",
    "close_day_missed",
    "close_day_thin",
    "station_nothing_usable",
    "publication_late",
    "close_stage_failed",
    "provider_down",
    "daytime_download_missed",
)


def build_verdict(
    conn: sqlite3.Connection,
    *,
    pipeline_enabled: bool,
    budget_enabled: bool,
    db_enabled: bool,
    now: datetime,
    export_sweeper_dead: str | None,
) -> dict[str, object]:
    """Assemble the verdict envelope.

    ``export_sweeper_dead`` is the rendered death line for the export sweeper,
    or ``None`` while it is alive. It carries NO default on purpose: a caller
    that forgot it would silently report the sweeper as alive, which is the
    exact class of silence this condition exists to remove.
    """
    conditions: list[Condition] = []
    grace_active = False
    db_read_failed = False

    if pipeline_enabled:
        try:
            grace_active = _grace_active(conn, now)
            conditions.extend(
                _pipeline_conditions(conn, now, grace_active=grace_active)
            )
        except sqlite3.Error:
            db_read_failed = True
    else:
        conditions.extend(
            _skipped(cid, "pipeline", "warning") for cid in _PIPELINE_SKIPPED
        )

    if budget_enabled:
        try:
            conditions.extend(_budget_conditions(conn, now))
        except sqlite3.Error:
            db_read_failed = True
    else:
        conditions.extend(
            _skipped(cid, "budget", sev)
            for cid, sev in (
                ("budget_calls", "critical"),
                ("budget_credits", "critical"),
                ("domain_backoffs", "warning"),
                ("feed_errors", "warning"),
                ("costed_noop", "warning"),
                ("key_missing", "warning"),
                ("weathercom_access_refused", "warning"),
            )
        )

    if db_enabled:
        try:
            conditions.extend(_db_conditions(conn, now))
        except sqlite3.Error:
            db_read_failed = True
    else:
        conditions.append(_skipped("db_readable", "db", "critical"))

    if db_read_failed:
        # A genuine sqlite3.Error on read: emit the db_readable failure and drop
        # any db_readable added by _db_conditions (which won't have run on error).
        conditions = [c for c in conditions if c.id != "db_readable"]
        conditions.append(
            Condition(
                id="db_readable",
                group="db",
                ok=False,
                skipped=False,
                severity="critical",
                detail="database read raised sqlite3.Error",
            )
        )

    # Evaluated OUTSIDE every `try` and every toggle: it touches no `conn`, so
    # it must survive a `sqlite3.Error` short-circuit, and it runs no query for
    # a toggle to remove. Appended before the fold so a latched death reaches
    # `overall`.
    conditions.append(
        Condition(
            id="export_sweeper_dead",
            group="process",
            ok=export_sweeper_dead is None,
            skipped=False,
            severity="critical",
            detail=export_sweeper_dead,
        )
    )

    overall = "ok"
    for cond in conditions:
        if cond.skipped or cond.ok:
            continue
        if _SEVERITY_RANK[cond.severity] > _SEVERITY_RANK[overall]:
            overall = cond.severity

    return {
        "overall": overall,
        "generated_at": isoformat_utc(now),
        "grace_active": grace_active,
        "conditions": [c.as_dict() for c in conditions],
    }


def error_verdict(now: datetime, detail: str) -> dict[str, object]:
    """Always-200 failure envelope for the route's outer guard.

    Returned when ANY unexpected exception escapes ``build_verdict`` or the
    options load — e.g. a malformed ``/data/options.json`` (``json.JSONDecodeError``
    / ``ValueError``) from ``load_runtime_options``, or a later re-read of that
    same file failing inside ``_key_missing_count``'s ``resolve_secret``, which
    sits outside the narrow ``except sqlite3.Error`` and so is not caught there.
    Reports ``overall:critical`` via a dedicated ``unexpected_error`` condition —
    kept DISTINCT from ``db_readable`` so an internal error is not misreported as
    a DB-read failure and the narrow inner ``except sqlite3.Error`` need never be
    widened.

    A non-ISO ``worker_started_at`` is NOT one of these: ``_grace_active``
    catches the ``ValueError`` itself and degrades to grace-inactive, which
    deliberately keeps every other condition reportable.
    """
    return {
        "overall": "critical",
        "generated_at": isoformat_utc(now),
        "grace_active": False,
        "conditions": [
            Condition(
                id="unexpected_error",
                group="monitor",
                ok=False,
                skipped=False,
                severity="critical",
                detail=detail,
            ).as_dict()
        ],
    }
