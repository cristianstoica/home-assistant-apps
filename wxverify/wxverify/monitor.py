"""HA-native monitor: on-request threshold verdict over the add-on's SQLite DB.

Pure module — no process, task, or loop. Each group's checks are read-only
COUNT/EXISTS queries; ``build_verdict`` assembles the verdict envelope, honours
the per-group toggles, applies the 10-min post-start grace to group 1, and maps
a genuine ``sqlite3.Error`` on read to ``db_readable:false`` / ``overall:critical``.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from wxverify.core.timeutil import isoformat_utc

# --- Hardcoded thresholds (standalone's proven defaults) ---------------------
FEED_STALE_HOURS = 12
OBS_STALE_HOURS = 12
FETCH_OBS_LIVE_HOURS = 8
FETCH_FEED_LIVE_HOURS = 12
PAIR_SCORE_LIVE_HOURS = 12
FAILED_JOB_AGE_HOURS = 48
STUCK_RUNNING_MINUTES = 20
PENDING_OVERDUE_MINUTES = 15
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
    return Condition(
        id=cond_id, group=group, ok=True, skipped=True, severity=severity
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

_ELIGIBLE_OBS_WHERE = """
    s.enabled = 1
    AND EXISTS (
        SELECT 1 FROM stations st WHERE st.site_id = s.id AND st.enabled = 1
    )
"""


def _count(conn: sqlite3.Connection, sql: str, params: tuple[object, ...]) -> int:
    row = conn.execute(sql, params).fetchone()
    return 0 if row is None else int(row[0])


def _has_completed_within(
    conn: sqlite3.Connection, job_type: str, cutoff: str
) -> bool:
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
    obs_cutoff = isoformat_utc(now - timedelta(hours=OBS_STALE_HOURS))
    fetch_obs_cutoff = isoformat_utc(now - timedelta(hours=FETCH_OBS_LIVE_HOURS))
    fetch_feed_cutoff = isoformat_utc(now - timedelta(hours=FETCH_FEED_LIVE_HOURS))
    pair_cutoff = isoformat_utc(now - timedelta(hours=PAIR_SCORE_LIVE_HOURS))

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

    # obs_stale: enabled site with >=1 enabled station and last_obs_at >12h/NULL.
    obs_stale_n = _count(
        conn,
        f"""
        SELECT COUNT(*) FROM sites s
        WHERE {_ELIGIBLE_OBS_WHERE}
          AND (s.last_obs_at IS NULL OR s.last_obs_at < ?)
        """,
        (obs_cutoff,),
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
    eligible_obs = _count(
        conn,
        f"SELECT COUNT(*) FROM sites s WHERE {_ELIGIBLE_OBS_WHERE}",
        (),
    )

    fetch_obs_live_tripped = eligible_obs > 0 and not _has_completed_within(
        conn, "fetch_obs", fetch_obs_cutoff
    )
    fetch_feed_live_tripped = eligible_feeds > 0 and not _has_completed_within(
        conn, "fetch_feed", fetch_feed_cutoff
    )
    pair_score_live_tripped = eligible_feeds > 0 and not _has_completed_within(
        conn, "pair_and_score", pair_cutoff
    )

    def _cond(cid: str, tripped: bool, count: int | None, detail: str) -> Condition:
        if grace_active:
            return Condition(
                id=cid, group="pipeline", ok=True, skipped=False,
                severity="warning", count=count,
            )
        return Condition(
            id=cid, group="pipeline", ok=not tripped, skipped=False,
            severity="warning", count=count,
            detail=detail if tripped else None,
        )

    return [
        _cond("feed_stale", feed_stale_n > 0, feed_stale_n,
              f"{feed_stale_n} feeds not run >12h"),
        _cond("obs_stale", obs_stale_n > 0, obs_stale_n,
              f"{obs_stale_n} sites not observed >12h"),
        _cond("fetch_obs_live", fetch_obs_live_tripped, None,
              "no completed fetch_obs in 8h"),
        _cond("fetch_feed_live", fetch_feed_live_tripped, None,
              "no completed fetch_feed in 12h"),
        _cond("pair_score_live", pair_score_live_tripped, None,
              "no completed pair_and_score in 12h"),
    ]


def _budget_conditions(conn: sqlite3.Connection, now: datetime) -> list[Condition]:
    return []


def _db_conditions(conn: sqlite3.Connection, now: datetime) -> list[Condition]:
    return []


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


def build_verdict(
    conn: sqlite3.Connection,
    *,
    pipeline_enabled: bool,
    budget_enabled: bool,
    db_enabled: bool,
    now: datetime,
) -> dict[str, object]:
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
            _skipped(cid, "pipeline", "warning")
            for cid in (
                "feed_stale",
                "obs_stale",
                "fetch_obs_live",
                "fetch_feed_live",
                "pair_score_live",
                "problem_jobs",
            )
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
    / ``ValueError``) from ``load_runtime_options``, a ``resolve_secret`` failure
    inside ``_key_missing_count``, or a non-ISO ``worker_started_at`` (``ValueError``)
    inside ``_grace_active``. Reports ``overall:critical`` via a dedicated
    ``unexpected_error`` condition — kept DISTINCT from ``db_readable`` so an
    internal error is not misreported as a DB-read failure and the narrow inner
    ``except sqlite3.Error`` need never be widened.
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
