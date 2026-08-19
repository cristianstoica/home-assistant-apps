"""Verification-run rows, config/roster pinning, fingerprint, publish (§14).

A run row pins everything a published verdict depends on: methodology and
app versions, the site's configuration and frozen feed roster (captured in
ONE write transaction at run start — §8's snapshot-semantics obligation),
the timezone generation, the evaluation period and settled-through
watermark, the bootstrap seed/count, and the input fingerprint the nightly
trigger decided on. The published pointer is a ``runtime_state`` row per
site (``verification_published_run:<site_id>``), flipped in the publish
transaction only — until then the previous published run keeps serving.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import cast

from wxverify import __version__
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.tz_generations import (
    ensure_published_generation,
    published_generation_id,
)
from wxverify.scoring.effective import active_competitor_clause
from wxverify.settings.depth import DEPTH_VARIABLES, effective_blend_depths
from wxverify.settings.keys import get_number_setting
from wxverify.verification.methodology import (
    BOOTSTRAP_RESAMPLES,
    CONSENSUS_LAG_HOURS,
    METHODOLOGY_VERSION,
)
from wxverify.verification.record import snapshot_wall_clock

PUBLISHED_RUN_KEY_PREFIX = "verification_published_run:"


def published_run_key(site_id: int) -> str:
    """``runtime_state`` key holding the site's published verification run."""
    return f"{PUBLISHED_RUN_KEY_PREFIX}{site_id}"


def published_run_id(conn: sqlite3.Connection, site_id: int) -> int | None:
    """The site's published verification run id, or None when none published."""
    value = get_runtime_state(conn, published_run_key(site_id))
    return None if value is None else int(value)


@dataclass(frozen=True)
class RosterFeed:
    """One pinned real feed of the run's frozen roster.

    ``max_lead_hours`` carries no default on purpose: every construction
    site must declare the horizon it means, and ``None`` means "the
    snapshot did not record it" (a pre-0.12.0 run), never a fabricated
    value.
    """

    feed_id: int
    source: str
    model: str
    max_lead_hours: int | None


@dataclass(frozen=True)
class RunConfig:
    """The pinned inputs a run simulates under — never live tables mid-run."""

    site_id: int
    run_id: int
    timezone: str
    rain_threshold_mm: float
    wall_clock: str
    blend_depth: int
    blend_depths: dict[str, int]
    min_n: int
    window_days: int
    tz_generation_id: int
    roster: tuple[RosterFeed, ...]
    period_start: str
    period_end: str
    bootstrap_seed: int
    bootstrap_resamples: int

    def incumbent_depth(self, variable: str) -> int:
        """The variable's pinned effective depth (§15 lockstep)."""
        return self.blend_depths.get(variable, self.blend_depth)


def _dumps(obj: object) -> str:
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def roster_feeds(conn: sqlite3.Connection, site_id: int) -> tuple[RosterFeed, ...]:
    """The site's active real competitor feeds, in stable (source, model) order.

    Same membership rule as the live scheduler/leaderboard (enabled +
    subscribed real feeds; meteoblue members resolve through the package
    feed; virtual feeds excluded), evaluated ONCE — the run start pins the
    result, and every simulate chunk re-checks it against the live tables,
    failing the run on divergence instead of mixing two configurations.
    """
    clause = active_competitor_clause(site_expr=str(int(site_id)))
    rows = conn.execute(
        f"""
        SELECT f.id, f.source, f.model, f.max_lead_hours
        FROM feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = ? AND sfs.feed_id = f.id
        WHERE f.is_virtual = 0
          AND NOT (f.source = 'meteoblue' AND f.model = 'multimodel')
          AND {clause}
        ORDER BY f.source, f.model
        """,
        (site_id,),
    ).fetchall()
    return tuple(
        RosterFeed(
            feed_id=int(row["id"]),
            source=str(row["source"]),
            model=str(row["model"]),
            max_lead_hours=int(row["max_lead_hours"]),
        )
        for row in rows
    )


def capture_config_snapshot(
    conn: sqlite3.Connection, site_id: int
) -> dict[str, object]:
    """The run's pinned configuration + roster, as one canonical JSON-able dict.

    WRITE PATH ONLY: seeds the site's initial timezone generation when the
    published pointer is absent, so it needs a write connection. The
    read-only staleness check uses :func:`current_input_fingerprint`.
    """
    return _config_snapshot(
        conn, site_id, tz_generation_id=ensure_published_generation(conn, site_id)
    )


def _config_snapshot(
    conn: sqlite3.Connection, site_id: int, *, tz_generation_id: int
) -> dict[str, object]:
    """Snapshot body against an already-resolved timezone generation.

    Every statement here reads; the caller supplies the generation id, which
    is the only part of the snapshot that can require a write.
    """
    site = conn.execute(
        "SELECT timezone, rain_threshold_mm FROM sites WHERE id = ?",
        (site_id,),
    ).fetchone()
    if site is None:
        raise ValueError(f"site {site_id} does not exist")
    depths = effective_blend_depths(conn)
    return {
        "timezone": str(site["timezone"]),
        "rain_threshold_mm": float(site["rain_threshold_mm"]),
        "wall_clock": snapshot_wall_clock(conn, site_id),
        "blend_depth": get_number_setting(conn, "forecast_blend_depth", 2, minimum=1),
        # §15: per-variable effective depth + provenance, resolved through
        # the same helper the live page and the record builder use.
        "blend_depths": {v: d.depth for v, d in depths.items()},
        "blend_depth_sources": {v: d.source for v, d in depths.items()},
        "min_n": get_number_setting(conn, "min_n", 30, minimum=0),
        "window_days": get_number_setting(conn, "rolling_window_days", 30, minimum=1),
        "tz_generation_id": tz_generation_id,
        "roster": [
            {
                "feed_id": f.feed_id,
                "source": f.source,
                "model": f.model,
                "max_lead_hours": f.max_lead_hours,
            }
            for f in roster_feeds(conn, site_id)
        ],
    }


def input_fingerprint(
    conn: sqlite3.Connection, site_id: int, snapshot: dict[str, object]
) -> str:
    """sha256 fingerprint of everything a run's result depends on (§14).

    Covers consensus content (per-site max ``observations.computed_at`` plus
    row hashes over scorable truth), configuration + roster (the snapshot),
    methodology version, the timezone generation, and the raw forecast
    sample high-water mark. Deterministic given identical inputs.
    """
    generation_id = int(str(snapshot["tz_generation_id"]))
    obs = conn.execute(
        """
        SELECT COUNT(*) AS n, MAX(computed_at) AS latest
        FROM observations WHERE site_id = ?
        """,
        (site_id,),
    ).fetchone()
    samples = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS hi FROM forecast_samples WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    digest = hashlib.sha256()
    digest.update(_dumps(snapshot).encode())
    digest.update(
        _dumps(
            {
                "methodology_version": METHODOLOGY_VERSION,
                "obs_count": int(obs["n"]),
                "obs_latest_computed_at": None
                if obs["latest"] is None
                else str(obs["latest"]),
                "sample_high_water": int(samples["hi"]),
            }
        ).encode()
    )
    truth_rows = conn.execute(
        """
        SELECT local_date, quantity, value, eligible, covered_hours, stale
        FROM daily_truth
        WHERE site_id = ? AND tz_generation_id = ?
        ORDER BY local_date, quantity
        """,
        (site_id, generation_id),
    ).fetchall()
    for row in truth_rows:
        digest.update(
            (
                f"{row['local_date']}|{row['quantity']}|{row['value']}|"
                f"{row['eligible']}|{row['covered_hours']}|{row['stale']}\n"
            ).encode()
        )
    return digest.hexdigest()


def current_input_fingerprint(conn: sqlite3.Connection, site_id: int) -> str | None:
    """Today's input fingerprint for a staleness check — READ PATH, never writes.

    Resolves the timezone generation with the non-seeding
    :func:`published_generation_id`, so the request path cannot INSERT on a
    read-pool connection no matter what the pointer state is. Returns None
    when the site has no published generation yet: the comparison is then
    unknown rather than stale, and the caller says so instead of guessing.
    """
    generation_id = published_generation_id(conn, site_id)
    if generation_id is None:
        return None
    snapshot = _config_snapshot(conn, site_id, tz_generation_id=generation_id)
    return input_fingerprint(conn, site_id, snapshot)


def seed_from_fingerprint(fingerprint: str) -> int:
    """Deterministic bootstrap seed: identical inputs ⇒ identical seed (§18.6)."""
    # Mask to 63 bits so the seed always fits SQLite's signed 64-bit INTEGER
    # (values >= 2**63 raise OverflowError on INSERT in start_run).
    return int.from_bytes(hashlib.sha256(fingerprint.encode()).digest()[:8], "big") & (
        2**63 - 1
    )


def settled_through(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    tz_generation_id: int,
    now: datetime,
) -> str | None:
    """Latest local date whose truth exists and was settled at ``now`` (§14).

    Settled = the local day has fully ended plus the consensus knowability
    lag, and no later source revision is pending knowledge-wise (revisions
    after the run start change the fingerprint and drive a fresh run).
    """
    row = conn.execute(
        f"""
        SELECT MAX(local_date) AS latest
        FROM daily_truth
        WHERE site_id = ? AND tz_generation_id = ?
          AND julianday(day_end_utc, '+{CONSENSUS_LAG_HOURS} hours')
              <= julianday(?)
        """,
        (site_id, tz_generation_id, isoformat_utc(now)),
    ).fetchone()
    return None if row is None or row["latest"] is None else str(row["latest"])


def truth_period_start(
    conn: sqlite3.Connection, *, site_id: int, tz_generation_id: int
) -> str | None:
    """Earliest truth day under the run's generation, or None when empty."""
    row = conn.execute(
        """
        SELECT MIN(local_date) AS first FROM daily_truth
        WHERE site_id = ? AND tz_generation_id = ?
        """,
        (site_id, tz_generation_id),
    ).fetchone()
    return None if row is None or row["first"] is None else str(row["first"])


def failed_attempts_for_fingerprint(
    conn: sqlite3.Connection, site_id: int, fingerprint: str
) -> int:
    """Failed runs with this fingerprint newer than the published run (§14)."""
    published = published_run_id(conn, site_id)
    return int(
        conn.execute(
            """
            SELECT COUNT(*) AS n FROM verification_runs
            WHERE site_id = ? AND state = 'failed'
              AND input_fingerprint = ? AND id > ?
            """,
            (site_id, fingerprint, published if published is not None else 0),
        ).fetchone()["n"]
    )


def fail_incomplete_attempts(
    conn: sqlite3.Connection, site_id: int, *, error: str
) -> None:
    """Fail the site's non-published attempts and drop their partial evidence.

    Extracted from `start_run`'s inline cleanup (§7.9) so the import
    neutralizer (D13) can reuse this domain's own semantics instead of a
    second hand-written cleanup. Reproduces the original block exactly:
    every non-published run's evidence is deleted regardless of state, but
    only `running` runs are marked `failed` — a run already `failed` keeps
    its own error via `COALESCE`, never this one.
    """
    stale = [
        int(row["id"])
        for row in conn.execute(
            """
            SELECT id FROM verification_runs
            WHERE site_id = ? AND state != 'published'
            """,
            (site_id,),
        ).fetchall()
    ]
    if not stale:
        return
    marks = ",".join("?" for _ in stale)
    for table in (
        "verification_evidence",
        "verification_day_context",
        "verification_results",
        "verification_verdicts",
    ):
        conn.execute(f"DELETE FROM {table} WHERE run_id IN ({marks})", tuple(stale))
    conn.execute(
        f"""
        UPDATE verification_runs SET state = 'failed',
            error = COALESCE(error, ?)
        WHERE id IN ({marks}) AND state = 'running'
        """,
        (error, *stale),
    )


def start_run(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    snapshot: dict[str, object],
    fingerprint: str,
    now: datetime,
) -> RunConfig | None:
    """Create the run row and wipe prior incomplete attempts' evidence (§14).

    Runs inside ONE write transaction: mark any prior non-published run of
    the site failed, delete its evidence (failed attempts keep their run
    metadata, never their partial evidence), then insert the new 'running'
    row pinning the snapshot. Returns None when the pinned generation has no
    settled truth at all (nothing to simulate).
    """
    generation_id = int(str(snapshot["tz_generation_id"]))
    end = settled_through(
        conn, site_id=site_id, tz_generation_id=generation_id, now=now
    )
    start = truth_period_start(conn, site_id=site_id, tz_generation_id=generation_id)
    if end is None or start is None or start > end:
        return None
    fail_incomplete_attempts(conn, site_id, error="superseded by a newer attempt")
    attempt = failed_attempts_for_fingerprint(conn, site_id, fingerprint) + 1
    seed = seed_from_fingerprint(fingerprint)
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, period_start, period_end,
             settled_through, bootstrap_seed, bootstrap_resamples,
             input_fingerprint, created_at)
        VALUES (?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            generation_id,
            METHODOLOGY_VERSION,
            __version__,
            attempt,
            _dumps(snapshot),
            start,
            end,
            end,
            seed,
            BOOTSTRAP_RESAMPLES,
            fingerprint,
            isoformat_utc(now),
        ),
    )
    if cur.lastrowid is None:
        raise RuntimeError("verification run insert failed")
    return run_config_from_row(conn, int(cur.lastrowid))


def _parse_roster(raw: object) -> tuple[RosterFeed, ...]:
    """Rehydrate a pinned roster list from JSON-shaped data; skips foreign items.

    ``max_lead_hours`` is parsed tolerantly: a snapshot written before
    0.12.0 genuinely does not record the horizon, so it rehydrates as
    ``None`` -- "not recorded" -- rather than a fabricated 168, and no
    pre-0.12.0 run fails to load. Same reasoning as `_parse_blend_depths`.
    """
    roster: list[RosterFeed] = []
    if isinstance(raw, list):
        for item in cast("list[object]", raw):
            if not isinstance(item, dict):
                continue
            entry = {str(k): v for k, v in cast("dict[object, object]", item).items()}
            roster.append(
                RosterFeed(
                    feed_id=int(str(entry["feed_id"])),
                    source=str(entry["source"]),
                    model=str(entry["model"]),
                    max_lead_hours=(
                        None
                        if (v := entry.get("max_lead_hours")) is None
                        else int(str(v))
                    ),
                )
            )
    return tuple(roster)


def _parse_blend_depths(raw: object, blend_depth: int) -> dict[str, int]:
    """Rehydrate the pinned per-variable depth map from JSON-shaped data.

    A snapshot written before §15 lacks the key; synthesizing the map from
    the pinned global depth preserves the pre-§15 incumbent semantics.
    """
    out: dict[str, int] = dict.fromkeys(DEPTH_VARIABLES, blend_depth)
    if isinstance(raw, dict):
        for key, value in cast("dict[object, object]", raw).items():
            if str(key) in out:
                out[str(key)] = int(str(value))
    return out


def run_config_from_row(conn: sqlite3.Connection, run_id: int) -> RunConfig:
    """Rehydrate the pinned :class:`RunConfig` from a run row."""
    row = conn.execute(
        "SELECT * FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"verification run {run_id} does not exist")
    snapshot_raw: object = json.loads(str(row["config_snapshot"]))
    if not isinstance(snapshot_raw, dict):
        raise ValueError(f"verification run {run_id} has a foreign config snapshot")
    snapshot: dict[str, object] = {
        str(k): v for k, v in cast("dict[object, object]", snapshot_raw).items()
    }
    roster = _parse_roster(snapshot.get("roster"))
    blend_depth = int(str(snapshot["blend_depth"]))
    return RunConfig(
        site_id=int(row["site_id"]),
        run_id=run_id,
        timezone=str(snapshot["timezone"]),
        rain_threshold_mm=float(str(snapshot["rain_threshold_mm"])),
        wall_clock=str(snapshot["wall_clock"]),
        blend_depth=blend_depth,
        blend_depths=_parse_blend_depths(snapshot.get("blend_depths"), blend_depth),
        min_n=int(str(snapshot["min_n"])),
        window_days=int(str(snapshot["window_days"])),
        tz_generation_id=int(row["tz_generation_id"]),
        roster=roster,
        period_start=str(row["period_start"]),
        period_end=str(row["period_end"]),
        bootstrap_seed=int(row["bootstrap_seed"]),
        bootstrap_resamples=int(row["bootstrap_resamples"]),
    )


def assert_inputs_unpinned_unchanged(conn: sqlite3.Connection, cfg: RunConfig) -> None:
    """Fail the chunk when live config/roster diverged from the pinned run.

    §8 snapshot semantics: the as-of ranking's feed discovery reads the LIVE
    ``feeds``/``site_feed_state`` tables, so a mid-run subscription or
    settings change could silently mix two configurations. Rather than
    thread a parallel roster through every production query, each simulate
    chunk re-derives the pinned inputs and raises on divergence — the job
    fails, the run is marked failed, and the next nightly trigger re-runs
    under the NEW fingerprint. Data growth (new samples/observations) is
    expected mid-run and deliberately not checked here.
    """
    current = capture_config_snapshot(conn, cfg.site_id)
    current_roster = _parse_roster(current.get("roster"))
    mismatches: list[str] = []
    if current_roster != cfg.roster:
        mismatches.append("roster")
    # Per-variable effective depths (§15): compare depths only — a
    # provenance flip that leaves every effective depth unchanged (e.g. an
    # override set equal to the global) cannot change results mid-run.
    if current.get("blend_depths") != cfg.blend_depths:
        mismatches.append("blend_depths")
    for key, pinned in (
        ("timezone", cfg.timezone),
        ("rain_threshold_mm", cfg.rain_threshold_mm),
        ("wall_clock", cfg.wall_clock),
        ("blend_depth", cfg.blend_depth),
        ("min_n", cfg.min_n),
        ("window_days", cfg.window_days),
        ("tz_generation_id", cfg.tz_generation_id),
    ):
        if current.get(key) != pinned:
            mismatches.append(key)
    if mismatches:
        raise RuntimeError(
            f"verification run {cfg.run_id} inputs changed mid-run: "
            + ", ".join(mismatches)
        )


def mark_run_failed(conn: sqlite3.Connection, site_id: int, error: str) -> None:
    """Terminal-failure hook: mark the site's running attempt failed (§14)."""
    conn.execute(
        """
        UPDATE verification_runs SET state = 'failed', error = ?
        WHERE site_id = ? AND state = 'running'
        """,
        (error, site_id),
    )


def publish_run(conn: sqlite3.Connection, site_id: int, run_id: int) -> None:
    """Atomic publish: run → published + pointer flip, one transaction (§14)."""
    cur = conn.execute(
        """
        UPDATE verification_runs
        SET state = 'published', published_at = ?
        WHERE id = ? AND site_id = ? AND state = 'running'
        """,
        (isoformat_utc(), run_id, site_id),
    )
    if cur.rowcount != 1:
        raise RuntimeError(f"verification run {run_id} is not publishable")
    set_runtime_state(conn, published_run_key(site_id), str(run_id))


def record_trigger_decision(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    trigger_date: str,
    decision: str,
    reason: str | None,
    fingerprint: str | None = None,
    run_id: int | None = None,
) -> None:
    """Durable nightly-trigger decision row (§14) — written BEFORE any run
    row exists for the decision it describes."""
    conn.execute(
        """
        INSERT INTO verification_trigger_decisions
            (site_id, trigger_date, decided_at, decision, reason,
             input_fingerprint, run_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            trigger_date,
            isoformat_utc(),
            decision,
            reason,
            fingerprint,
            run_id,
        ),
    )


#: Prefix every superseding trigger-decision reason carries (§12/W8). The
#: `decision` column is CHECK-constrained to four values, so a superseded
#: start reuses `skipped` and is told apart from a gate skip only by this
#: prefix on `reason`.
SUPERSEDED_REASON_PREFIX = "superseded:"

#: Reason the scheduler records on the durable decision row when the §3.1
#: publish hold suppressed the nightly enqueue, mirrored here for the two
#: reader surfaces. The scheduler owns the write; this is the read-side name.
PUBLISH_HOLD_REASON = "publish_hold"

#: §D13 reason recorded on a `verification_run` job/run neutralized because it
#: arrived via import while still active in the donor. One definition, used by
#: `db_transfer.py` and by the tests — no caller may spell the string literally.
IMPORT_SUPPRESSED_REASON = "suppressed: imported active verification chain"

#: §12 trigger-status values carried on both the API status payload and the
#: /verification page context.
TRIGGER_STATUS_TRIGGERED = "triggered"
TRIGGER_STATUS_SKIPPED = "skipped"
TRIGGER_STATUS_NO_DECISION = "no_decision_recorded"
TRIGGER_STATUS_DATE_UNKNOWN = "trigger_date_unknown"


def expected_trigger_date(
    conn: sqlite3.Connection, site_id: int, now: datetime
) -> str | None:
    """Local date of the trigger cycle a reader should be looking at (§12).

    The site-local date once local time is at or past the nightly trigger
    time, the previous local date before it. Returns ``None`` — never
    raises — when the site's timezone or trigger wall clock is
    unresolvable, matching how every other resolution of ``sites.timezone``
    degrades per site.

    This is a READER-side derivation ("which trigger cycle is current"),
    deliberately distinct from the scheduler's fail-closed "may the trigger
    fire now" guard, which keeps its own rule.
    """
    from datetime import timedelta
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    from wxverify.verification.record import resolve_snapshot_utc
    from wxverify.worker.scheduler import VERIFICATION_TRIGGER_LOCAL_TIME

    row = conn.execute("SELECT timezone FROM sites WHERE id = ?", (site_id,)).fetchone()
    if row is None or row["timezone"] is None:
        return None
    timezone = str(row["timezone"])
    try:
        local_today = now.astimezone(ZoneInfo(timezone)).date()
        trigger_utc = resolve_snapshot_utc(
            timezone, local_today, VERIFICATION_TRIGGER_LOCAL_TIME
        )
    except (ZoneInfoNotFoundError, ValueError):
        return None
    if now >= trigger_utc:
        return local_today.isoformat()
    return (local_today - timedelta(days=1)).isoformat()


@dataclass(frozen=True)
class TriggerDecisionRead:
    """Latest decision for one exact trigger date, plus the supersede tally."""

    decision: str
    reason: str | None
    superseded_count: int
    superseded_reason: str | None


def latest_trigger_decision(
    conn: sqlite3.Connection, site_id: int, trigger_date: str
) -> TriggerDecisionRead | None:
    """Highest-``id`` decision for THAT exact date, plus its supersedes (§12).

    Latest-``id`` alone would mask the W8 chain's supersede: that chain
    emits a second ``run_started`` after the superseding ``skipped``, so the
    newest row on a divergent night is byte-identical to a clean one. The
    count and newest reason of the date's ``superseded:`` rows are therefore
    returned alongside it.
    """
    row = conn.execute(
        """
        SELECT decision, reason FROM verification_trigger_decisions
        WHERE site_id = ? AND trigger_date = ?
        ORDER BY id DESC LIMIT 1
        """,
        (site_id, trigger_date),
    ).fetchone()
    if row is None:
        return None
    superseded = conn.execute(
        """
        SELECT COUNT(*) AS n, MAX(id) AS newest
        FROM verification_trigger_decisions
        WHERE site_id = ? AND trigger_date = ? AND decision = 'skipped'
          AND reason LIKE ? || '%'
        """,
        (site_id, trigger_date, SUPERSEDED_REASON_PREFIX),
    ).fetchone()
    count = int(superseded["n"])
    newest_reason: str | None = None
    if count:
        newest = conn.execute(
            "SELECT reason FROM verification_trigger_decisions WHERE id = ?",
            (int(superseded["newest"]),),
        ).fetchone()
        newest_reason = None if newest is None else str(newest["reason"])
    return TriggerDecisionRead(
        decision=str(row["decision"]),
        reason=None if row["reason"] is None else str(row["reason"]),
        superseded_count=count,
        superseded_reason=newest_reason,
    )


def trigger_status(
    conn: sqlite3.Connection, site_id: int, now: datetime
) -> dict[str, object]:
    """The §12 trigger status for one site, plus the §3.1 publish-hold state.

    ONE derivation feeding both surfaces — ``GET
    /api/verification/status`` and the ``/verification`` page — so the two
    can never disagree. Degrades per site (``trigger_date_unknown``); never
    raises, because both callers build their payload over every enabled
    site and one unusable timezone must not remove the operator's whole
    diagnostic window.
    """
    from wxverify.worker.scheduler import verification_publish_held

    held = verification_publish_held(conn)
    out: dict[str, object] = {
        "status": TRIGGER_STATUS_DATE_UNKNOWN,
        "trigger_date": None,
        "reason": None,
        "superseded_count": 0,
        "superseded_reason": None,
        "publish_hold": {
            "held": held,
            "reason": PUBLISH_HOLD_REASON if held else None,
        },
    }
    trigger_date = expected_trigger_date(conn, site_id, now)
    if trigger_date is None:
        return out
    out["trigger_date"] = trigger_date
    read = latest_trigger_decision(conn, site_id, trigger_date)
    if read is None:
        out["status"] = TRIGGER_STATUS_NO_DECISION
        return out
    out["status"] = (
        TRIGGER_STATUS_TRIGGERED
        if read.decision == "run_started"
        else TRIGGER_STATUS_SKIPPED
    )
    # The `reason` string is the payload, not the `decision` enum: a gate
    # skip and a superseded start share the enum and differ only here.
    out["reason"] = read.reason
    out["superseded_count"] = read.superseded_count
    out["superseded_reason"] = read.superseded_reason
    return out


def trigger_decision_blocks(
    conn: sqlite3.Connection, site_id: int, trigger_date: str, *, held: bool
) -> bool:
    """Whether an existing decision for this local date must stop a new enqueue.

    A publish-hold skip is the ONE decision that stops blocking once the hold
    is released; every other decision — including a skip with any other reason
    — stays terminal for the date.
    """
    # `reason IS ?` not `=` is deliberate: NULL-safe, though equivalent here.
    row = conn.execute(
        """
        SELECT
            COALESCE(MAX(CASE WHEN decision = 'skipped' AND reason IS ?
                              THEN 1 ELSE 0 END), 0) AS hold_rows,
            COALESCE(MAX(CASE WHEN decision = 'skipped' AND reason IS ?
                              THEN 0 ELSE 1 END), 0) AS other_rows
        FROM verification_trigger_decisions
        WHERE site_id = ? AND trigger_date = ?
        """,
        (PUBLISH_HOLD_REASON, PUBLISH_HOLD_REASON, site_id, trigger_date),
    ).fetchone()
    if int(row["other_rows"]) == 1:
        return True
    return held and int(row["hold_rows"]) == 1


def published_fingerprint(conn: sqlite3.Connection, site_id: int) -> str | None:
    """Fingerprint of the last PUBLISHED run — the §14 comparison baseline."""
    run_id = published_run_id(conn, site_id)
    if run_id is None:
        return None
    row = conn.execute(
        "SELECT input_fingerprint FROM verification_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    return None if row is None else str(row["input_fingerprint"])
