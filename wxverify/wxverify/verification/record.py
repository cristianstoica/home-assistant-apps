"""Forecast-of-record construction, gap scan, and due helpers (plan §7/§14).

One immutable snapshot per site per local day at the site's snapshot time T
(§3): the record job rebuilds the production Forecast selection *as of T*
(§6 parameterization only — never today's loaders, never the mutable
``score_cache``) and appends it to the append-only ``forecast_of_record``
table. Retries are idempotent (``ON CONFLICT ... DO NOTHING`` on the row
identity); nothing here ever UPDATEs or DELETEs a record row.

Late writes: a record job may still reconstruct within
``LATE_WRITE_WINDOW_HOURS`` of T; beyond that window only the gap-scan job
writes, and what it writes is explicit ``missed`` rows — a day is never
silently absent and never backfilled from post-T data.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify import __version__
from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.timeutil import day_ahead as issue_day_ahead
from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.connection import StaleGenerationError
from wxverify.db.runtime_state import (
    delete_runtime_state,
    get_runtime_state,
    set_runtime_state,
)
from wxverify.forecast.aggregate import (
    blend_mean,
    clearing_subset,
    covered_hours,
    display_day_index,
    displayed_daily,
)
from wxverify.forecast.data import (
    FutureSampleRow,
    count_null_availability_samples,
    forecast_ranking,
    load_future_samples,
)
from wxverify.forecast.selection import (
    CellCandidate,
    representative_day_ahead,
    select_cell_feeds,
)
from wxverify.scoring.leaderboard import LeaderboardRow, leaderboard_with_status
from wxverify.settings.depth import DEPTH_VARIABLES, effective_blend_depths
from wxverify.settings.keys import get_number_setting, get_setting
from wxverify.verification.coverage import evaluate_variable, local_day_bounds
from wxverify.verification.methodology import (
    LATE_WRITE_WINDOW_HOURS,
    METHODOLOGY_VERSION,
    SNAPSHOT_LOCAL_TIME,
)
from wxverify.worker.control import JobCancelled, JobDeferred

logger = logging.getLogger(__name__)

#: Record identity spans the production display horizon: day 0..7.
RECORD_DAY_COUNT = 8
RECORD_VARIABLES: tuple[str, ...] = DEPTH_VARIABLES

#: Global settings key for the snapshot wall-clock time; a per-site override
#: lives at ``record_snapshot_local_time:<site_id>`` (§15).
SNAPSHOT_TIME_KEY = "record_snapshot_local_time"

#: ``missed_reason`` for an identity that SHOULD have been recorded at T —
#: candidates were knowable and the write never happened.
MISSED_WINDOW_CLOSED = "window_closed"

#: ``missed_reason`` for an identity where nothing was knowable at T. The
#: honest "there was nothing to record", as opposed to a lost write.
MISSED_NO_CANDIDATES = "no_candidates"

#: Gap-scan chunk size: dates examined per claim before yielding a
#: continuation (§14 — long work never holds the write lock unbounded).
GAP_SCAN_MAX_DATES = 30

#: How far back a single gap scan reaches from today. A depth, NOT a chunk
#: size (that is ``GAP_SCAN_MAX_DATES``): it bounds the nightly walk so it
#: does not grow with the age of the log. Chosen against an unattended
#: worker outage, not against the repair window — past
#: ``LATE_WRITE_WINDOW_HOURS`` the only available action is writing
#: ``missed`` rows, and the monitor only ever scores today or yesterday.
GAP_SCAN_LOOKBACK_DAYS = 30

#: Grace after T inside which a record write still counts as ``on_time``.
#: Cross-reference ``monitor.PENDING_OVERDUE_MINUTES``, which encodes the
#: same idea from the other side: the on-time job is still legitimately
#: queued this long after T.
RECORD_ON_TIME_GRACE = timedelta(minutes=15)

#: runtime_state key prefix for the gap scan's durable per-date failures
#: (§4 change 7). Merged per date, deleted only when ``dates`` empties.
GAP_SCAN_FAILURES_KEY_PREFIX = "record_gap_scan_failures"


def _dumps(obj: object) -> str:
    return json.dumps(obj, separators=(",", ":"))


def parse_wall_clock(raw: str) -> tuple[int, int] | None:
    parts = raw.strip().split(":")
    if len(parts) != 2:
        return None
    try:
        hour, minute = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return hour, minute
    return None


def snapshot_wall_clock(conn: sqlite3.Connection, site_id: int) -> str:
    """The site's snapshot wall-clock time ``HH:MM`` (§15 resolution order).

    Per-site setting, then the global setting, then the methodology default.
    Unparseable values fall through to the next layer rather than erroring —
    a foreign settings row must not stop the daily record.
    """
    for key in (f"{SNAPSHOT_TIME_KEY}:{site_id}", SNAPSHOT_TIME_KEY):
        raw = get_setting(conn, key)
        if raw is not None and parse_wall_clock(raw) is not None:
            return raw.strip()
    return SNAPSHOT_LOCAL_TIME


def resolve_snapshot_utc(timezone: str, local_date: date, wall_clock: str) -> datetime:
    """UTC instant of T: first instant at/after ``wall_clock`` on the local day.

    PEP 495 semantics with ``fold=0``: an ambiguous fall-back time maps to
    its first occurrence, exactly the §3 rule. A wall-clock time inside a
    spring-forward gap maps to the gap-SHIFTED instant (02:30 -> 03:30), an
    architect-accepted approximation of §3's first-instant-after-the-gap
    (03:00) — within the hour, deterministic, and identical for the default
    07:00 snapshot time, which no real-world DST gap touches.
    """
    parsed = parse_wall_clock(wall_clock)
    if parsed is None:
        raise ValueError(f"invalid snapshot wall clock {wall_clock!r}")
    hour, minute = parsed
    local = datetime(
        local_date.year,
        local_date.month,
        local_date.day,
        hour,
        minute,
        tzinfo=ZoneInfo(timezone),
    )
    return local.astimezone(UTC)


def record_day_has_any_row(
    conn: sqlite3.Connection,
    site_id: int,
    tz_generation_id: int,
    snapshot_local_date: str,
) -> bool:
    """Whether ANY record/missed row exists for the day's identity.

    A presence test, and named as one. Its single remaining caller is the
    ``forecast_record_gap`` monitor condition, which asks "did this site's
    record run at all today" — never "is the day complete". Everything that
    asks the completeness question uses :func:`record_day_complete`.
    """
    row = conn.execute(
        """
        SELECT 1 FROM forecast_of_record
        WHERE site_id = ? AND tz_generation_id = ? AND snapshot_local_date = ?
        LIMIT 1
        """,
        (site_id, tz_generation_id, snapshot_local_date),
    ).fetchone()
    return row is not None


def expected_record_identities() -> set[tuple[str, int]]:
    """The full ``(variable, display_lead)`` identity set for one day."""
    return {
        (variable, day)
        for variable in RECORD_VARIABLES
        for day in range(RECORD_DAY_COUNT)
    }


def record_day_complete(
    conn: sqlite3.Connection,
    site_id: int,
    tz_generation_id: int,
    snapshot_local_date: str,
) -> bool:
    """Whether the day holds its FULL expected identity set.

    An exact set test, never a count: a day holding 24 rows with one lead
    duplicated and another missing is exactly the partial shape this
    predicate exists to catch. Allowlist form — proceed only when the day is
    provably complete; absent, partial and unknown are all incomplete and
    therefore eligible for reconstruction. A day whose full identity set is
    ``missed`` rows is complete: ``missed`` at window close is terminal.
    """
    rows = conn.execute(
        """
        SELECT variable, display_lead FROM forecast_of_record
        WHERE site_id = ? AND tz_generation_id = ? AND snapshot_local_date = ?
        """,
        (site_id, tz_generation_id, snapshot_local_date),
    ).fetchall()
    present = {(str(r["variable"]), int(r["display_lead"])) for r in rows}
    return expected_record_identities() <= present


def _site_row(conn: sqlite3.Connection, site_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT timezone, rain_threshold_mm, enabled FROM sites WHERE id = ?",
        (site_id,),
    ).fetchone()
    if row is None or not bool(row["enabled"]):
        raise JobCancelled()
    return row


def _current_generation(
    conn: sqlite3.Connection, site_id: int, instant_utc: str
) -> int:
    from wxverify.db.tz_generations import (
        ensure_published_generation,
        resolve_generation_for_instant,
    )

    pointer = ensure_published_generation(conn, site_id)
    resolved = resolve_generation_for_instant(conn, site_id, instant_utc)
    return pointer if resolved is None else resolved


def _rebuild_in_progress(conn: sqlite3.Connection, site_id: int) -> int:
    row = conn.execute(
        """
        SELECT 1 FROM timezone_generations
        WHERE site_id = ? AND state = 'building' LIMIT 1
        """,
        (site_id,),
    ).fetchone()
    return 0 if row is None else 1


def _group_samples(
    samples: list[FutureSampleRow], *, timezone: str, now: datetime
) -> dict[str, dict[int, dict[int, list[FutureSampleRow]]]]:
    """variable -> display day -> feed_id -> samples, anchored at ``now=T``.

    Mirrors the production grouping (forecast.service) with the display-day
    index computed against T, so the record's day 0..7 axis is the axis the
    page showed at T.
    """
    grouped: dict[str, dict[int, dict[int, list[FutureSampleRow]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    for sample in samples:
        day = display_day_index(sample.valid_at, timezone=timezone, now=now)
        if 0 <= day < RECORD_DAY_COUNT:
            grouped[sample.variable][day][sample.feed_id].append(sample)
    return grouped


#: Candidate disposition axis 1 (§13): how a feed took part in THIS cell.
PARTICIPATION_CONTRIBUTED = "contributed"
PARTICIPATION_SELECTED_NOT_CLEARING = "selected_not_clearing"
PARTICIPATION_NOT_SELECTED = "not_selected"
PARTICIPATION_NO_SAMPLES = "no_samples"
PARTICIPATION_NOT_LOADABLE = "not_loadable"


@dataclass(frozen=True)
class _UnionFeed:
    """One member of the day's candidate universe (§13).

    Record-local on purpose: ``configured`` and the participation axis must
    never reach ``forecast.selection.CellCandidate``, whose third
    construction site is the live Forecast page.
    """

    feed_id: int
    source: str
    model: str
    configured: bool
    loadable: bool


def _candidate_universe(
    conn: sqlite3.Connection, site_id: int, sampled_feed_ids: set[int]
) -> dict[int, _UnionFeed]:
    """The day's candidate universe: configured ∪ sampled ∪ not-loadable.

    ``configured`` reuses the scheduler's active-feed predicate verbatim
    (``worker/scheduler.py``): ``f.enabled = 1 AND COALESCE(sfs.enabled,
    f.default_subscribed) = 1`` over a LEFT JOIN, so a feed with no per-site
    row falls back to ``default_subscribed`` rather than vanishing. The
    loader's own exclusions (virtual, meteoblue package) are deliberately
    NOT folded in: they are the other axis.
    """
    rows = conn.execute(
        """
        SELECT f.id AS feed_id, f.source, f.model, f.is_virtual,
               CASE WHEN f.enabled = 1
                     AND COALESCE(sfs.enabled, f.default_subscribed) = 1
                    THEN 1 ELSE 0 END AS configured
        FROM feeds f
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = ? AND sfs.feed_id = f.id
        """,
        (site_id,),
    ).fetchall()
    universe: dict[int, _UnionFeed] = {}
    for row in rows:
        feed_id = int(row["feed_id"])
        source = str(row["source"])
        model = str(row["model"])
        # Mirrors _EXCLUDED_FEEDS_SQL (forecast/data.py): what the loader can
        # never return, and therefore what can never be selected.
        loadable = not bool(row["is_virtual"]) and not (
            source == "meteoblue" and model == "multimodel"
        )
        configured = bool(row["configured"])
        if not (configured or not loadable or feed_id in sampled_feed_ids):
            continue
        universe[feed_id] = _UnionFeed(
            feed_id=feed_id,
            source=source,
            model=model,
            configured=configured,
            loadable=loadable,
        )
    return universe


def _candidate_records(
    universe: dict[int, _UnionFeed],
    candidates: list[CellCandidate],
    *,
    selected_ids: list[int],
    agg_ids: list[int],
) -> list[dict[str, object]]:
    """One provenance record per universe member, for the cell (§13).

    Every union member appears exactly once and carries both orthogonal
    fields; ``selected_feed_ids`` is exactly the ids whose participation is
    ``contributed`` or ``selected_not_clearing``.
    """
    by_id = {c.feed_id: c for c in candidates}
    selected = set(selected_ids)
    clearing = set(agg_ids)
    members = dict(universe)
    for feed_id, candidate in by_id.items():
        if feed_id not in members:
            members[feed_id] = _UnionFeed(
                feed_id=feed_id,
                source=candidate.source,
                model=candidate.model,
                configured=False,
                loadable=True,
            )
    out: list[dict[str, object]] = []
    for feed_id in sorted(members):
        member = members[feed_id]
        candidate = by_id.get(feed_id)
        if not member.loadable:
            participation = PARTICIPATION_NOT_LOADABLE
        elif feed_id in clearing:
            participation = PARTICIPATION_CONTRIBUTED
        elif feed_id in selected:
            participation = PARTICIPATION_SELECTED_NOT_CLEARING
        elif candidate is not None:
            participation = PARTICIPATION_NOT_SELECTED
        else:
            participation = PARTICIPATION_NO_SAMPLES
        record: dict[str, object] = (
            asdict(candidate)
            if candidate is not None
            else {
                "feed_id": feed_id,
                "source": member.source,
                "model": member.model,
                "confident": False,
                "skill_score": None,
                "pair_n": 0,
                "mae": None,
                "future_sample_count": 0,
                "covered_hours": 0,
            }
        )
        record["participation"] = participation
        record["configured"] = member.configured
        out.append(record)
    return out


def _blend_hourly(
    feed_ids: list[int], feeds_samples: dict[int, list[FutureSampleRow]]
) -> list[tuple[str, float]]:
    """Blended hourly series for one cell: mean of the given feeds per hour."""
    per_hour: dict[str, list[float]] = defaultdict(list)
    for feed_id in feed_ids:
        for sample in feeds_samples.get(feed_id, []):
            per_hour[sample.valid_at].append(sample.value)
    out: list[tuple[str, float]] = []
    for valid_at in sorted(per_hour):
        mean = blend_mean(per_hour[valid_at])
        if mean is not None:
            out.append((valid_at, mean))
    return out


def _leaderboard_status_cell(
    conn: sqlite3.Connection, *, site_id: int, variable: str, day_ahead: int
) -> dict[str, object]:
    """Observed production leaderboard status for one effective score cell.

    §7: the record stores what the production leaderboard cache looked like
    at build time (status + window key + the cell's oldest snapshot stamp) —
    diagnostics only, never an input to the as-of ranking.
    """
    result = leaderboard_with_status(
        conn, site_id=site_id, variable=variable, day_ahead=day_ahead, window="rolling"
    )
    stamp_row = conn.execute(
        """
        SELECT MIN(computed_at) AS oldest FROM score_cache
        WHERE site_id = ? AND variable = ? AND day_ahead = ? AND window_key = ?
        """,
        (site_id, variable, day_ahead, result.window_key),
    ).fetchone()
    oldest = stamp_row["oldest"] if stamp_row is not None else None
    return {
        "status": result.status,
        "window_key": result.window_key,
        "computed_at": None if oldest is None else str(oldest),
    }


def build_forecast_record(
    conn: sqlite3.Connection,
    site_id: int,
    snapshot_local_date: str,
    *,
    now: datetime | None = None,
) -> None:
    """Build and append the site's forecast-of-record rows for one local day.

    Idempotent per identity: each (variable x target day) row inserts with
    ``DO NOTHING`` on its identity, so a run against a partially written day
    fills only what is absent and a retry can only confirm, never replace.
    Runs entirely inside the caller's write transaction. Raises
    :class:`JobDeferred` before T and :class:`JobCancelled` beyond the
    late-write window (the gap scan owns ``missed``).

    A cell for which nothing was knowable at T writes NO row: the day stays
    reconstructible instead of being sealed as a successful empty grid.
    ``write_path`` is derived here from ``at`` against T, never passed in.
    """
    at = now or utc_now()
    site = _site_row(conn, site_id)
    timezone = str(site["timezone"])
    rain_threshold_mm = float(site["rain_threshold_mm"])
    try:
        local_date = date.fromisoformat(snapshot_local_date)
    except ValueError as exc:
        raise JobCancelled() from exc
    wall_clock = snapshot_wall_clock(conn, site_id)
    try:
        snapshot_utc = resolve_snapshot_utc(timezone, local_date, wall_clock)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise JobCancelled() from exc
    if at < snapshot_utc:
        raise JobDeferred(isoformat_utc(snapshot_utc))
    if at > snapshot_utc + timedelta(hours=LATE_WRITE_WINDOW_HOURS):
        # Beyond the late-write window nothing may reconstruct; the gap
        # scan is the single writer of 'missed'.
        raise JobCancelled()
    as_of = isoformat_utc(snapshot_utc)
    generation_id = _current_generation(conn, site_id, as_of)
    if record_day_complete(conn, site_id, generation_id, snapshot_local_date):
        return
    write_path = (
        "on_time"
        if at <= snapshot_utc + RECORD_ON_TIME_GRACE
        else "late_reconstruction"
    )

    bounds = local_day_bounds(local_date, timezone)
    since_valid_at = isoformat_utc(bounds.start_utc)
    samples = load_future_samples(
        conn, site_id=site_id, since_valid_at=since_valid_at, as_of=as_of
    )
    null_availability = count_null_availability_samples(
        conn, site_id=site_id, since_valid_at=since_valid_at
    )
    grouped = _group_samples(samples, timezone=timezone, now=snapshot_utc)
    universe = _candidate_universe(conn, site_id, {s.feed_id for s in samples})

    # §15 lockstep: the record resolves depth through the SAME per-variable
    # helper the live Forecast page and the run snapshot use.
    depths = effective_blend_depths(conn)
    declared_min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    declared_window_days = get_number_setting(
        conn, "rolling_window_days", 30, minimum=1
    )
    policy = _dumps(
        {
            "blend_depth": get_number_setting(
                conn, "forecast_blend_depth", 2, minimum=1
            ),
            "blend_depths": {v: d.depth for v, d in depths.items()},
            "blend_depth_sources": {v: d.source for v, d in depths.items()},
            "min_n": declared_min_n,
            "window_days": declared_window_days,
            "rain_threshold_mm": rain_threshold_mm,
            "null_fetched_at_samples": null_availability,
        }
    )
    latency = int((at - snapshot_utc).total_seconds())
    rebuild_flag = _rebuild_in_progress(conn, site_id)

    rank_cache: dict[tuple[str, int], dict[int, LeaderboardRow]] = {}
    status_cache: dict[tuple[str, int], dict[str, object]] = {}
    for day in range(RECORD_DAY_COUNT):
        target_date = local_date + timedelta(days=day)
        for variable in RECORD_VARIABLES:
            feeds_samples = grouped.get(variable, {}).get(day, {})
            if not feeds_samples:
                # Nothing was knowable at T for this identity: write no row,
                # so the day stays reconstructible in-window and the gap
                # scan can close it honestly at window close. Gated on
                # sample presence, NOT on ``selection.available`` — a cell
                # whose candidates all lost selection is a real product
                # state and must still be recorded.
                continue
            candidates: list[CellCandidate] = []
            effective_cells: dict[str, int] = {}
            for feed_id, feed_samples in feeds_samples.items():
                rep = representative_day_ahead(
                    [
                        issue_day_ahead(s.issued_at, s.valid_at, timezone)
                        for s in feed_samples
                    ]
                )
                effective_cells[str(feed_id)] = rep
                key = (variable, rep)
                if key not in rank_cache:
                    rank_cache[key] = forecast_ranking(
                        conn,
                        site_id=site_id,
                        variable=variable,
                        day_ahead=rep,
                        as_of=as_of,
                        declared_min_n=declared_min_n,
                        declared_window_days=declared_window_days,
                    )
                if key not in status_cache:
                    status_cache[key] = _leaderboard_status_cell(
                        conn, site_id=site_id, variable=variable, day_ahead=rep
                    )
                row = rank_cache[key].get(feed_id)
                candidates.append(
                    CellCandidate(
                        feed_id=feed_id,
                        source=feed_samples[0].source,
                        model=feed_samples[0].model,
                        confident=row.confident if row is not None else False,
                        skill_score=row.skill_score if row is not None else None,
                        pair_n=row.n if row is not None else 0,
                        mae=row.mae if row is not None else None,
                        future_sample_count=len(feed_samples),
                        covered_hours=covered_hours(s.valid_at for s in feed_samples),
                    )
                )
            selection = select_cell_feeds(
                candidates, blend_depth=depths[variable].depth
            )
            selected_ids = [c.feed_id for c in selection.feeds]
            weight = 1.0 / len(selected_ids) if selected_ids else None
            # ``hourly`` is the FULL-selection blend on purpose: it exists to
            # reproduce the Forecast page's hourly drill-down, which blends
            # the whole selection (forecast/service.py) while only the tile's
            # daily value uses the clearing subset. Do not unify it with the
            # outcomes blend below.
            hourly = _blend_hourly(selected_ids, feeds_samples)
            # The DISPLAYED daily quantities, computed exactly the production
            # way (aggregate per feed over the clearing subset, then blend) —
            # via the same shared helpers the Forecast page uses (§6/§7).
            if selection.available:
                agg_ids, partial = clearing_subset(
                    selected_ids,
                    {c.feed_id: c.covered_hours for c in selection.feeds},
                )
            else:
                agg_ids, partial = [], False
            displayed: dict[str, object] = dict(
                displayed_daily(
                    variable,
                    [[s.value for s in feeds_samples[fid]] for fid in agg_ids],
                    rain_threshold_mm=rain_threshold_mm,
                )
            )
            displayed["partial"] = partial
            displayed["low_confidence"] = selection.low_confidence
            # Outcomes describe the SCORED product, so they are computed over
            # the clearing subset — the same feed set ``displayed`` uses
            # (simulate.py parity, W3).
            outcomes = evaluate_variable(
                variable,
                _blend_hourly(agg_ids, feeds_samples),
                timezone=timezone,
                local_date=target_date,
                rain_threshold_mm=rain_threshold_mm,
            )
            source_runs = {
                str(c.feed_id): max(s.issued_at for s in feeds_samples[c.feed_id])
                for c in selection.feeds
            }
            statuses = {
                str(effective_cells[str(fid)]): status_cache[
                    (variable, effective_cells[str(fid)])
                ]
                for fid in selected_ids
            }
            conn.execute(
                """
                INSERT INTO forecast_of_record
                    (site_id, tz_generation_id, timezone,
                     tz_rebuild_in_progress, snapshot_local_date,
                     snapshot_local_time, snapshot_utc, target_local_date,
                     variable, display_lead, status, missed_reason,
                     write_path, write_latency_seconds, policy,
                     methodology_version, app_version, candidates,
                     selected_feed_ids, feed_weights, effective_cells,
                     source_runs, hourly_values, daily_quantities,
                     leaderboard_status)
                VALUES (?,?,?,?,?,?,?,?,?,?, 'recorded', NULL,
                        ?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(site_id, tz_generation_id, snapshot_local_date,
                            variable, target_local_date) DO NOTHING
                """,
                (
                    site_id,
                    generation_id,
                    timezone,
                    rebuild_flag,
                    snapshot_local_date,
                    wall_clock,
                    as_of,
                    target_date.isoformat(),
                    variable,
                    day,
                    write_path,
                    latency,
                    policy,
                    METHODOLOGY_VERSION,
                    __version__,
                    _dumps(
                        _candidate_records(
                            universe,
                            candidates,
                            selected_ids=selected_ids,
                            agg_ids=agg_ids,
                        )
                    ),
                    _dumps(selected_ids),
                    _dumps({str(fid): weight for fid in selected_ids}),
                    _dumps(effective_cells),
                    _dumps(source_runs),
                    _dumps(hourly),
                    _dumps(
                        {
                            "displayed": displayed,
                            "outcomes": [asdict(o) for o in outcomes],
                        }
                    ),
                    _dumps(statuses),
                ),
            )


def _write_missed_rows(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    generation_id: int,
    timezone: str,
    wall_clock: str,
    local_date: date,
    snapshot_utc: datetime,
    reasons: Mapping[tuple[str, int], str],
) -> None:
    """Write one ``missed`` row per identity, each with its OWN reason.

    ``reasons`` must be total over the day's identity set: ``missed_reason``
    is NOT NULL for a ``missed`` row under the schema CHECK, so an unmapped
    identity is an IntegrityError inside the single writer of ``missed`` on
    exactly the day it exists to close.
    """
    for day in range(RECORD_DAY_COUNT):
        target_date = local_date + timedelta(days=day)
        for variable in RECORD_VARIABLES:
            conn.execute(
                """
                INSERT INTO forecast_of_record
                    (site_id, tz_generation_id, timezone,
                     tz_rebuild_in_progress, snapshot_local_date,
                     snapshot_local_time, snapshot_utc, target_local_date,
                     variable, display_lead, status, missed_reason,
                     write_path, methodology_version, app_version)
                VALUES (?,?,?,0,?,?,?,?,?,?, 'missed', ?, NULL, ?, ?)
                ON CONFLICT(site_id, tz_generation_id, snapshot_local_date,
                            variable, target_local_date) DO NOTHING
                """,
                (
                    site_id,
                    generation_id,
                    timezone,
                    local_date.isoformat(),
                    wall_clock,
                    isoformat_utc(snapshot_utc),
                    target_date.isoformat(),
                    variable,
                    day,
                    reasons[(variable, day)],
                    METHODOLOGY_VERSION,
                    __version__,
                ),
            )


def assess_record_day(
    conn: sqlite3.Connection,
    site_id: int,
    local_date: date,
    *,
    as_of: str,
) -> dict[tuple[str, int], bool]:
    """Per identity, whether ANYTHING was knowable at T. Read-only.

    A non-mutating sibling of :func:`build_forecast_record` performing the
    same as-of-T load and grouping: it touches no ``forecast_of_record``
    row, advances no cursor, and is never reached through the builder's
    beyond-window guard. It produces the evidence the gap scan needs to
    choose between the two missed reasons.
    """
    site = _site_row(conn, site_id)
    timezone = str(site["timezone"])
    snapshot_utc = parse_utc(as_of)
    bounds = local_day_bounds(local_date, timezone)
    samples = load_future_samples(
        conn,
        site_id=site_id,
        since_valid_at=isoformat_utc(bounds.start_utc),
        as_of=as_of,
    )
    grouped = _group_samples(samples, timezone=timezone, now=snapshot_utc)
    return {
        (variable, day): bool(grouped.get(variable, {}).get(day))
        for variable, day in expected_record_identities()
    }


def gap_scan_failures_key(site_id: int) -> str:
    return f"{GAP_SCAN_FAILURES_KEY_PREFIX}:{site_id}"


def _merge_gap_scan_failures(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    failed: dict[str, str],
    cleared: set[str],
    at: datetime,
    window_start: date,
) -> None:
    """Read-modify-write the site's durable gap-scan failure signal.

    Merged per date, never replaced per chunk: a clean continuation chunk
    must not erase the failure an earlier chunk of the same scan recorded,
    and a chain that dies mid-way must leave the signal standing. Only
    dates this chunk visited are removed. The key is deleted if and only if
    the merged ``dates`` object is empty.
    """
    key = gap_scan_failures_key(site_id)
    raw = get_runtime_state(conn, key)
    dates: dict[str, object] = {}
    if raw is not None:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            existing = cast("dict[str, object]", parsed).get("dates")
            if isinstance(existing, dict):
                dates = {
                    str(k): v for k, v in cast("dict[str, object]", existing).items()
                }
    if raw is None and not failed and not cleared:
        return
    stamp = isoformat_utc(at)
    for day_iso in cleared:
        dates.pop(day_iso, None)
    for day_iso, error in failed.items():
        dates[day_iso] = {"error": error, "last_failed_at": stamp}
    # Bounded by construction: nothing outside the current lookback window
    # can be assessed again, so it cannot be cleared and must not linger.
    horizon = window_start.isoformat()
    dates = {k: v for k, v in dates.items() if k >= horizon}
    if not dates:
        if raw is not None:
            delete_runtime_state(conn, key)
        return
    set_runtime_state(conn, key, _dumps({"as_of": stamp, "dates": dates}))


def gap_scan_degraded_sites(conn: sqlite3.Connection) -> tuple[int, str | None]:
    """Sites with a standing gap-scan failure, and the newest failure stamp.

    Presence IS the freshness signal: the key is deleted the moment its
    dates clear, so no recency window is applied (the scan runs once per
    local day, and a 12-hour window would blank a standing failure for half
    of every day).
    """
    rows = conn.execute(
        "SELECT key, value FROM runtime_state WHERE key LIKE ?",
        (f"{GAP_SCAN_FAILURES_KEY_PREFIX}:%",),
    ).fetchall()
    sites = 0
    newest: str | None = None
    for row in rows:
        try:
            parsed = json.loads(str(row["value"]))
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, dict):
            continue
        dates = cast("dict[str, object]", parsed).get("dates")
        if not isinstance(dates, dict) or not dates:
            continue
        sites += 1
        for entry in cast("dict[str, object]", dates).values():
            if not isinstance(entry, dict):
                continue
            stamp = cast("dict[str, object]", entry).get("last_failed_at")
            if isinstance(stamp, str) and (newest is None or stamp > newest):
                newest = stamp
    return sites, newest


def run_record_gap_scan(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    *,
    now: datetime | None = None,
) -> dict[str, object] | None:
    """Close record gaps for one site; the SINGLE writer of ``missed`` rows.

    Traverses the log rather than only its tail: the origin is the site's
    FIRST record/missed row under the current generation, capped at
    ``today - GAP_SCAN_LOOKBACK_DAYS``, and every date that is not provably
    complete is visited. Dates still inside the late-write window get a
    late reconstruction; closed windows get per-identity ``missed`` rows,
    each carrying the reason the as-of-T assessment supports. A fresh site
    with no rows at all has no gap — the log begins with its first on-time
    record (the no-backfill rule). Returns a continuation payload when more
    dates remain than one chunk covers.

    A date whose assessment fails is contained: its writes are rolled back
    to a per-date SAVEPOINT, the chunk still commits, and the failure is
    recorded durably under ``record_gap_scan_failures:<site_id>``. Worker
    control signals are re-raised unchanged so a mid-chunk deferral is
    still a deferral.
    """
    at = now or utc_now()
    site = _site_row(conn, site_id)
    timezone = str(site["timezone"])
    try:
        tz = ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise JobCancelled() from exc
    wall_clock = snapshot_wall_clock(conn, site_id)
    generation_id = _current_generation(conn, site_id, isoformat_utc(at))

    today = at.astimezone(tz).date()
    window_start = today - timedelta(days=GAP_SCAN_LOOKBACK_DAYS)
    cursor = payload.get("after_date")
    if isinstance(cursor, str):
        try:
            start = date.fromisoformat(cursor) + timedelta(days=1)
        except ValueError as exc:
            raise JobCancelled() from exc
    else:
        first = conn.execute(
            """
            SELECT MIN(snapshot_local_date) AS first FROM forecast_of_record
            WHERE site_id = ? AND tz_generation_id = ?
            """,
            (site_id, generation_id),
        ).fetchone()
        if first is None or first["first"] is None:
            # No rows for this generation: the log has not begun, and there
            # is no gap to close. Deriving an origin from site creation or
            # the generation's effective_from would back-fill 'missed' rows
            # across every local day since — the no-backfill rule forbids it.
            return None
        start = max(date.fromisoformat(str(first["first"])), window_start)

    scanned = 0
    current = start
    failed: dict[str, str] = {}
    cleared: set[str] = set()
    continuation: dict[str, object] | None = None
    while current <= today:
        if scanned >= GAP_SCAN_MAX_DATES:
            continuation = {"after_date": (current - timedelta(days=1)).isoformat()}
            break
        scanned += 1
        day_iso = current.isoformat()
        if record_day_complete(conn, site_id, generation_id, day_iso):
            cleared.add(day_iso)
            current += timedelta(days=1)
            continue
        snapshot_utc = resolve_snapshot_utc(timezone, current, wall_clock)
        if at < snapshot_utc:
            break  # today's T not reached yet; the daily job owns it
        savepoint = "gap_scan_date"
        conn.execute(f"SAVEPOINT {savepoint}")
        try:
            if at <= snapshot_utc + timedelta(hours=LATE_WRITE_WINDOW_HOURS):
                build_forecast_record(conn, site_id, day_iso, now=at)
            else:
                knowable = assess_record_day(
                    conn, site_id, current, as_of=isoformat_utc(snapshot_utc)
                )
                _write_missed_rows(
                    conn,
                    site_id=site_id,
                    generation_id=generation_id,
                    timezone=timezone,
                    wall_clock=wall_clock,
                    local_date=current,
                    snapshot_utc=snapshot_utc,
                    reasons={
                        identity: (
                            MISSED_WINDOW_CLOSED
                            if had_candidates
                            else MISSED_NO_CANDIDATES
                        )
                        for identity, had_candidates in knowable.items()
                    },
                )
        except (JobDeferred, JobCancelled, StaleGenerationError):
            # Worker control signals and a database swap are NOT contained:
            # absorbing one into durable state would turn a deferral into a
            # recorded failure and lose the processor's ordered dispatch.
            raise
        except Exception as exc:  # noqa: BLE001 - contained per date, by design
            conn.execute(f"ROLLBACK TO {savepoint}")
            conn.execute(f"RELEASE {savepoint}")
            # No rows of any kind for this date: 'window_closed' asserts a
            # write was lost, and the failed assessment is no evidence for
            # that claim on a log with no repair path past the window.
            failed[day_iso] = sanitized_exception(exc)
            logger.warning(
                "record gap scan: date assessment failed site=%s date=%s",
                site_id,
                day_iso,
            )
        else:
            conn.execute(f"RELEASE {savepoint}")
            cleared.add(day_iso)
        current += timedelta(days=1)
    _merge_gap_scan_failures(
        conn,
        site_id,
        failed=failed,
        cleared=cleared,
        at=at,
        window_start=window_start,
    )
    return continuation


def sites_with_record_gap(
    conn: sqlite3.Connection, now: datetime, *, slack: timedelta = timedelta(minutes=15)
) -> int:
    """Count enabled sites missing rows for the most recent expected T (§16).

    The expected snapshot day is today once local time is past T plus a
    short slack (mirroring the monitor's pending-overdue allowance so the
    condition doesn't flap while the on-time job is still queued), else
    yesterday. Sites whose record log has not begun (no published pointer
    or no rows at all) are skipped — read-only safe.
    """
    from wxverify.db.tz_generations import published_generation_id

    gaps = 0
    rows = conn.execute("SELECT id, timezone FROM sites WHERE enabled = 1").fetchall()
    for row in rows:
        site_id = int(row["id"])
        timezone = str(row["timezone"])
        generation_id = published_generation_id(conn, site_id)
        if generation_id is None:
            continue
        any_row = conn.execute(
            """
            SELECT 1 FROM forecast_of_record
            WHERE site_id = ? AND tz_generation_id = ? LIMIT 1
            """,
            (site_id, generation_id),
        ).fetchone()
        if any_row is None:
            continue  # log not begun under this generation
        try:
            tz = ZoneInfo(timezone)
            local_today = now.astimezone(tz).date()
            wall_clock = snapshot_wall_clock(conn, site_id)
            snapshot_utc = resolve_snapshot_utc(timezone, local_today, wall_clock)
        except (ZoneInfoNotFoundError, ValueError):
            continue
        expected = (
            local_today
            if now >= snapshot_utc + slack
            else local_today - timedelta(days=1)
        )
        if not record_day_has_any_row(
            conn, site_id, generation_id, expected.isoformat()
        ):
            gaps += 1
    return gaps
