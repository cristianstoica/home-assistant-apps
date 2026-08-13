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
import sqlite3
from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify import __version__
from wxverify.core.timeutil import day_ahead as issue_day_ahead
from wxverify.core.timeutil import isoformat_utc, utc_now
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
    CellSelection,
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

#: Record identity spans the production display horizon: day 0..7.
RECORD_DAY_COUNT = 8
RECORD_VARIABLES: tuple[str, ...] = DEPTH_VARIABLES

#: Global settings key for the snapshot wall-clock time; a per-site override
#: lives at ``record_snapshot_local_time:<site_id>`` (§15).
SNAPSHOT_TIME_KEY = "record_snapshot_local_time"

#: The only ``missed_reason`` the gap scan writes for a closed window.
MISSED_WINDOW_CLOSED = "window_closed"

#: Gap-scan chunk size: dates examined per claim before yielding a
#: continuation (§14 — long work never holds the write lock unbounded).
GAP_SCAN_MAX_DATES = 30


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


def record_rows_exist(
    conn: sqlite3.Connection,
    site_id: int,
    tz_generation_id: int,
    snapshot_local_date: str,
) -> bool:
    """Whether ANY record/missed row exists for the day's identity."""
    row = conn.execute(
        """
        SELECT 1 FROM forecast_of_record
        WHERE site_id = ? AND tz_generation_id = ? AND snapshot_local_date = ?
        LIMIT 1
        """,
        (site_id, tz_generation_id, snapshot_local_date),
    ).fetchone()
    return row is not None


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


def _blend_hourly(
    selection: CellSelection, feeds_samples: dict[int, list[FutureSampleRow]]
) -> list[tuple[str, float]]:
    """Blended hourly series for one cell: mean of selected feeds per hour."""
    per_hour: dict[str, list[float]] = defaultdict(list)
    for candidate in selection.feeds:
        for sample in feeds_samples.get(candidate.feed_id, []):
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
    write_path: str = "on_time",
    now: datetime | None = None,
) -> None:
    """Build and append the site's forecast-of-record rows for one local day.

    Idempotent: each of the 24 (variable x target day) rows inserts with
    ``DO NOTHING`` on its identity, so a retry can only confirm, never
    replace. Runs entirely inside the caller's write transaction. Raises
    :class:`JobDeferred` before T and :class:`JobCancelled` beyond the
    late-write window (the gap scan owns ``missed``).
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
    if record_rows_exist(conn, site_id, generation_id, snapshot_local_date):
        return

    bounds = local_day_bounds(local_date, timezone)
    since_valid_at = isoformat_utc(bounds.start_utc)
    samples = load_future_samples(
        conn, site_id=site_id, since_valid_at=since_valid_at, as_of=as_of
    )
    null_availability = count_null_availability_samples(
        conn, site_id=site_id, since_valid_at=since_valid_at
    )
    grouped = _group_samples(samples, timezone=timezone, now=snapshot_utc)

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
            hourly = _blend_hourly(selection, feeds_samples)
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
            outcomes = evaluate_variable(
                variable,
                hourly,
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
                    _dumps([asdict(c) for c in candidates]),
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
    reason: str,
) -> None:
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
                    reason,
                    METHODOLOGY_VERSION,
                    __version__,
                ),
            )


def run_record_gap_scan(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    *,
    now: datetime | None = None,
) -> dict[str, object] | None:
    """Close record gaps for one site; the SINGLE writer of ``missed`` rows.

    Enumerates local dates from the day after the site's last record/missed
    row up to today (under the CURRENT published generation — §14). Dates
    still inside the late-write window get a full late reconstruction;
    closed windows get explicit ``missed`` rows. A fresh site with no rows
    at all has no gap — the log begins with its first on-time record (the
    no-backfill rule). Returns a continuation payload when more dates
    remain than one chunk covers.
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

    cursor = payload.get("after_date")
    if isinstance(cursor, str):
        try:
            start = date.fromisoformat(cursor) + timedelta(days=1)
        except ValueError as exc:
            raise JobCancelled() from exc
    else:
        last = conn.execute(
            """
            SELECT MAX(snapshot_local_date) AS last FROM forecast_of_record
            WHERE site_id = ? AND tz_generation_id = ?
            """,
            (site_id, generation_id),
        ).fetchone()
        if last is None or last["last"] is None:
            return None
        start = date.fromisoformat(str(last["last"])) + timedelta(days=1)

    today = at.astimezone(tz).date()
    scanned = 0
    current = start
    while current <= today:
        if scanned >= GAP_SCAN_MAX_DATES:
            return {"after_date": (current - timedelta(days=1)).isoformat()}
        scanned += 1
        day_iso = current.isoformat()
        if not record_rows_exist(conn, site_id, generation_id, day_iso):
            snapshot_utc = resolve_snapshot_utc(timezone, current, wall_clock)
            if at < snapshot_utc:
                break  # today's T not reached yet; the daily job owns it
            if at <= snapshot_utc + timedelta(hours=LATE_WRITE_WINDOW_HOURS):
                build_forecast_record(
                    conn,
                    site_id,
                    day_iso,
                    write_path="late_reconstruction",
                    now=at,
                )
            else:
                _write_missed_rows(
                    conn,
                    site_id=site_id,
                    generation_id=generation_id,
                    timezone=timezone,
                    wall_clock=wall_clock,
                    local_date=current,
                    snapshot_utc=snapshot_utc,
                    reason=MISSED_WINDOW_CLOSED,
                )
        current += timedelta(days=1)
    return None


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
        if not record_rows_exist(conn, site_id, generation_id, expected.isoformat()):
            gaps += 1
    return gaps
