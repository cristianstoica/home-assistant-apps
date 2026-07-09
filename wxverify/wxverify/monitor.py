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
def _pipeline_conditions(
    conn: sqlite3.Connection, now: datetime, *, grace_active: bool
) -> list[Condition]:
    return []


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
