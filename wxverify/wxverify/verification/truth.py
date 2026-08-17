"""daily_truth materialization, staleness marking, and regeneration (§4/§14.2).

``daily_truth`` persists one row per (site, local date, quantity, timezone
generation): value, per-quantity eligibility, exclusion reason, coverage
counts, wet/dry counts, guard flags, and provenance (timezone, UTC day
bounds, max source ``computed_at``, generation tag). Rows are regenerable
from the CURRENT consensus observations — a retrospective timezone
correction writes a new generation's rows alongside the old, never mutates.

Consensus-mutation contract: every consensus mutation funnels through
``scoring.consensus.materialize_consensus`` (the sole ``observations``
writer), which calls :func:`mark_daily_truth_stale` for the mutated hour —
affected rows get ``stale = 1`` and the nightly trigger regenerates marked
days (:func:`regenerate_marked_truth`) before computing its input
fingerprint (§14). Regeneration is strictly regenerative — it rewrites days
that already have rows; the CREATIVE path is
:func:`materialize_missing_truth_days`, driven by the nightly chain's
``discover`` phase, which materializes settled local days ``daily_truth``
has never held.

Reads are generation-bound through the shared
``published_generation_clause`` accessor — a partially built correction
generation is never read.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from wxverify.core.error_sanitize import sanitized_exception
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.connection import StaleGenerationError
from wxverify.db.tz_generations import (
    ensure_published_generation,
    published_generation_clause,
)
from wxverify.verification.coverage import (
    VARIABLE_QUANTITIES,
    QuantityOutcome,
    evaluate_variable,
    local_day_bounds,
)
from wxverify.verification.methodology import CONSENSUS_LAG_HOURS
from wxverify.worker.control import JobCancelled, JobDeferred

logger = logging.getLogger(__name__)

_TRUTH_VARIABLES: tuple[str, ...] = ("temperature", "wind", "precip")


def _generation_timezone(conn: sqlite3.Connection, generation_id: int) -> str:
    row = conn.execute(
        "SELECT timezone FROM timezone_generations WHERE id = ?",
        (generation_id,),
    ).fetchone()
    if row is None:
        raise ValueError(f"timezone generation {generation_id} does not exist")
    return str(row["timezone"])


def materialize_daily_truth(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: str,
    tz_generation_id: int | None = None,
) -> dict[str, QuantityOutcome]:
    """Delete-and-recreate the five truth rows for one (site, local day).

    ``local_date`` is an ISO calendar date in the generation's timezone.
    When ``tz_generation_id`` is None the site's PUBLISHED generation is
    used (seeded on first use); regeneration passes the row's own tag so a
    correction generation's rows stay in their generation. All five
    quantity rows are always written — an excluded outcome stays visible
    with its coverage count and exclusion reason (§4). Must run on a write
    connection inside the caller's transaction.
    """
    site = conn.execute(
        "SELECT rain_threshold_mm FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    if site is None:
        raise ValueError(f"site {site_id} does not exist")
    generation_id = (
        ensure_published_generation(conn, site_id)
        if tz_generation_id is None
        else tz_generation_id
    )
    timezone = _generation_timezone(conn, generation_id)
    day = date.fromisoformat(local_date)
    # Bind the CANONICAL extended-format date, never the caller's raw
    # string: Python 3.11's date.fromisoformat accepts basic-format input
    # ("20260610"), which would defeat the UNIQUE(site, quantity,
    # local_date, generation) dedup and the DELETE below.
    local_date = day.isoformat()
    bounds = local_day_bounds(day, timezone)
    start = isoformat_utc(bounds.start_utc)
    end = isoformat_utc(bounds.end_utc)
    rows = conn.execute(
        """
        SELECT variable, valid_at, value, computed_at
        FROM observations
        WHERE site_id = ?
          AND variable IN ('temperature','wind','precip')
          AND julianday(valid_at) >= julianday(?)
          AND julianday(valid_at) < julianday(?)
        """,
        (site_id, start, end),
    ).fetchall()
    by_variable: dict[str, list[tuple[str, float]]] = {
        variable: [] for variable in _TRUTH_VARIABLES
    }
    max_computed_at: str | None = None
    for row in rows:
        by_variable[str(row["variable"])].append(
            (str(row["valid_at"]), float(row["value"]))
        )
        computed_at = row["computed_at"]
        if computed_at is not None and (
            max_computed_at is None
            or parse_utc(str(computed_at)) > parse_utc(max_computed_at)
        ):
            max_computed_at = str(computed_at)
    threshold = float(site["rain_threshold_mm"])
    outcomes: dict[str, QuantityOutcome] = {}
    for variable in _TRUTH_VARIABLES:
        for outcome in evaluate_variable(
            variable,
            by_variable[variable],
            timezone=timezone,
            local_date=day,
            rain_threshold_mm=threshold,
        ):
            outcomes[outcome.quantity] = outcome
    conn.execute(
        """
        DELETE FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND tz_generation_id = ?
        """,
        (site_id, local_date, generation_id),
    )
    generated_at = isoformat_utc()
    conn.executemany(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, exclusion_reason,
             covered_hours, expected_slots, peak_window_ok, wet_hours,
             dry_hours, rain_threshold_mm, day_start_utc, day_end_utc,
             timezone, source_max_computed_at, stale, generated_at,
             tz_generation_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
        """,
        [
            (
                site_id,
                local_date,
                outcome.quantity,
                outcome.value,
                1 if outcome.eligible else 0,
                outcome.exclusion_reason,
                outcome.covered_hours,
                outcome.expected_slots,
                None
                if outcome.peak_window_ok is None
                else (1 if outcome.peak_window_ok else 0),
                outcome.wet_hours,
                outcome.dry_hours,
                threshold if outcome.quantity.startswith("precip") else None,
                start,
                end,
                timezone,
                max_computed_at,
                generated_at,
                generation_id,
            )
            for outcome in outcomes.values()
        ],
    )
    return outcomes


def mark_daily_truth_stale(
    conn: sqlite3.Connection, *, site_id: int, variable: str, valid_at: str
) -> int:
    """Mark truth rows whose local day contains a mutated consensus hour.

    Called by every consensus mutation path (all of which funnel through
    ``materialize_consensus``). The match is by each row's OWN stored UTC
    day bounds, so rows of every generation — published or building — are
    marked under their own timezone; only the mutated variable's quantities
    are touched. Returns the number of rows marked. Comparisons go through
    ``julianday`` on both sides (mixed timestamp spellings).
    """
    quantities = VARIABLE_QUANTITIES.get(variable)
    if not quantities:
        return 0
    placeholders = ",".join("?" for _ in quantities)
    cur = conn.execute(
        f"""
        UPDATE daily_truth
        SET stale = 1
        WHERE site_id = ?
          AND quantity IN ({placeholders})
          AND julianday(day_start_utc) <= julianday(?)
          AND julianday(?) < julianday(day_end_utc)
        """,
        (site_id, *quantities, valid_at, valid_at),
    )
    return cur.rowcount


def regenerate_marked_truth(
    conn: sqlite3.Connection, *, site_id: int | None = None
) -> int:
    """Regenerate every (site, local day, generation) with stale truth rows.

    Each marked day is rebuilt delete-and-recreate under ITS OWN timezone
    generation from the current consensus observations, clearing the stale
    flag by construction. Returns the number of regenerated day-groups.
    Intended to run before the nightly trigger computes its input
    fingerprint (§14).
    """
    params: tuple[object, ...] = ()
    site_clause = ""
    if site_id is not None:
        site_clause = "AND dt.site_id = ?"
        params = (site_id,)
    # Only published/building generations regenerate: a RETIRED (or failed)
    # generation's stale rows are awaiting the correction chain's chunked
    # post-flip cleanup — recreating them here would resurrect rows the
    # cleanup already deleted and waste a full day materialization per row.
    groups = conn.execute(
        f"""
        SELECT DISTINCT dt.site_id, dt.local_date, dt.tz_generation_id
        FROM daily_truth dt
        JOIN timezone_generations tg ON tg.id = dt.tz_generation_id
        WHERE dt.stale = 1
          AND tg.state IN ('published', 'building')
          {site_clause}
        ORDER BY dt.site_id, dt.local_date, dt.tz_generation_id
        """,
        params,
    ).fetchall()
    for group in groups:
        materialize_daily_truth(
            conn,
            site_id=int(group["site_id"]),
            local_date=str(group["local_date"]),
            tz_generation_id=int(group["tz_generation_id"]),
        )
    return len(groups)


def regenerate_marked_truth_chunk(
    conn: sqlite3.Connection, *, site_id: int, limit: int
) -> int:
    """Regenerate up to ``limit`` stale (local day, generation) groups.

    Chunked variant of :func:`regenerate_marked_truth` for the nightly
    verification chain (§14): each chain chunk rebuilds a bounded number of
    marked day-groups in ONE short write transaction instead of holding the
    write lock across every marked day at once. Same generation policy as
    the full pass (published/building only — retired and failed generations
    are the correction chain's cleanup problem). Returns the number of
    regenerated groups; a return < ``limit`` means no stale group remains.
    """
    groups = conn.execute(
        """
        SELECT DISTINCT dt.site_id, dt.local_date, dt.tz_generation_id
        FROM daily_truth dt
        JOIN timezone_generations tg ON tg.id = dt.tz_generation_id
        WHERE dt.stale = 1
          AND tg.state IN ('published', 'building')
          AND dt.site_id = ?
        ORDER BY dt.local_date, dt.tz_generation_id
        LIMIT ?
        """,
        (site_id, limit),
    ).fetchall()
    for group in groups:
        materialize_daily_truth(
            conn,
            site_id=site_id,
            local_date=str(group["local_date"]),
            tz_generation_id=int(group["tz_generation_id"]),
        )
    return len(groups)


def settled_ceiling_local_date(timezone: str, now: datetime) -> date:
    """Newest local day that is SETTLED at ``now`` under ``timezone``.

    The Python mirror of the SQL predicate in
    ``verification.runs.settled_through`` — ``julianday(day_end_utc, '+lag
    hours') <= julianday(now)`` with ``lag = CONSENSUS_LAG_HOURS``. If either
    side changes, both change.

    Derivation. Let ``C = local_date(now - lag)``. A day ``D`` is settled
    exactly when ``end_utc(D) <= now - lag``, and ``end_utc(D) ==
    start_utc(D + 1)`` exactly, for every zone and every day (both come from
    local midnight in ``coverage.local_day_bounds``). By the definition of a
    local date, ``start_utc(C) <= now - lag < start_utc(C + 1)`` — so
    substituting ``D = C - 1`` shows ``C - 1`` is always settled, and
    ``D = C`` shows ``C`` never is. The ceiling is therefore exactly
    ``C - 1``, with no loop and no dependence on day length: the 23- and
    25-hour DST days are absorbed because the proof uses only boundary
    contiguity.

    ``now`` is normalized to UTC BEFORE the lag is subtracted. ``timedelta``
    arithmetic on an aware datetime is wall-clock arithmetic in that
    datetime's own zone, so across an offset transition it would not be
    three ABSOLUTE hours; the SQL side has no such ambiguity because
    ``settled_through`` binds ``now`` through ``isoformat_utc``. A naive
    ``now`` is read as UTC for the same reason — matching ``isoformat_utc``,
    never system-local.
    """
    instant = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)
    lagged = instant - timedelta(hours=CONSENSUS_LAG_HOURS)
    return lagged.astimezone(ZoneInfo(timezone)).date() - timedelta(days=1)


def observation_day_extent(
    conn: sqlite3.Connection, site_id: int, timezone: str
) -> tuple[date, date] | None:
    """Inclusive local-day range spanning the site's observations, or None.

    Deliberately NOT ``scoring.tz_rebuild.generation_day_range``, which is
    otherwise the same shape. That one also spans ``forecast_samples``, whose
    ``valid_at`` reaches a forecast horizon into the FUTURE, and discovery
    must be bounded by what has actually been observed. Importing it here
    would also invert the ``scoring -> verification`` edge — ``tz_rebuild``
    imports :func:`materialize_daily_truth`.
    """
    tz = ZoneInfo(timezone)
    stamps: list[str] = []
    for order in ("ASC", "DESC"):
        row = conn.execute(
            f"""
            SELECT valid_at FROM observations
            WHERE site_id = ?
            ORDER BY julianday(valid_at) {order}
            LIMIT 1
            """,
            (site_id,),
        ).fetchone()
        if row is not None:
            stamps.append(str(row["valid_at"]))
    if not stamps:
        return None
    instants = [parse_utc(stamp) for stamp in stamps]
    return (
        min(instants).astimezone(tz).date(),
        max(instants).astimezone(tz).date(),
    )


def missing_truth_days(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    tz_generation_id: int,
    timezone: str,
    now: datetime,
    limit: int,
    after_local_date: date | None = None,
) -> list[date]:
    """Up to ``limit`` settled observed local days holding no truth row yet.

    The window is ``[first observed day .. min(last observed day, settled
    ceiling)]`` and the result is that window MINUS every day already holding
    a row under ``tz_generation_id`` — a set difference, so a forward gap
    past existing rows and an interior hole are the same operation.
    ``after_local_date`` raises the lower bound to the day after it (the
    caller's chunk cursor).

    Clamping the upper bound by the last observed day matters during a fetch
    outage: the ceiling keeps advancing while observations stop, and without
    the clamp discovery would materialize an unbounded run of zero-coverage
    days, advancing the settled frontier over days nothing ever measured.
    The lower bound anchors on the DATA, not on ``MIN(daily_truth
    .local_date)``, so a late import of older observations is discoverable.

    Nothing filters on ``eligible`` or ``stale``: an ineligible day is a
    legitimate answer that must not be rebuilt every night, and staleness
    belongs to :func:`regenerate_marked_truth_chunk`. Read-only and
    limit-bounded.
    """
    extent = observation_day_extent(conn, site_id, timezone)
    if extent is None:
        return []
    first_observed, last_observed = extent
    lower = first_observed
    if after_local_date is not None:
        lower = max(lower, after_local_date + timedelta(days=1))
    upper = min(last_observed, settled_ceiling_local_date(timezone, now))
    if lower > upper:
        return []
    # A STRING range is a date range here: `materialize_daily_truth` binds
    # the canonical extended-format `local_date` before every write.
    present = {
        str(row["local_date"])
        for row in conn.execute(
            """
            SELECT DISTINCT local_date FROM daily_truth
            WHERE site_id = ? AND tz_generation_id = ?
              AND local_date >= ? AND local_date <= ?
            """,
            (site_id, tz_generation_id, lower.isoformat(), upper.isoformat()),
        ).fetchall()
    }
    missing: list[date] = []
    day = lower
    while day <= upper and len(missing) < limit:
        if day.isoformat() not in present:
            missing.append(day)
        day += timedelta(days=1)
    return missing


def materialize_missing_truth_days(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    now: datetime,
    limit: int,
    after_local_date: str | None = None,
) -> list[str]:
    """Materialize up to ``limit`` missing settled local days (one chunk).

    The structural analogue of :func:`regenerate_marked_truth_chunk`: one
    bounded batch per chain chunk. Must run on a WRITE connection inside the
    caller's transaction — :func:`materialize_daily_truth`'s own contract.
    The published generation is resolved (and seeded on first use) ONCE per
    chunk and passed explicitly to every per-day call, and the timezone comes
    from that generation, never from ``sites.timezone``: a ceiling computed
    under a different zone would disagree with the rows it creates. The
    window is recomputed every chunk, never frozen in chain state, so
    observations arriving mid-chain and a ceiling that steps over a local
    midnight are both picked up by the next chunk.

    Returns one entry per day selected from the window, ascending, whether or
    not it materialized. ``len(result) < limit`` therefore means the window
    is exhausted, never that days failed.

    Failure boundary. The preamble — generation, zone, observation extent,
    ceiling, presence query — is site- or generation-wide: every day in the
    window would fail identically, so nothing catches it and it propagates to
    the retry ladder, which is loud, bounded and self-clearing. Inside one
    day's work only ``ValueError`` is contained, on a per-day ``SAVEPOINT``:
    that is the entire failure vocabulary a single day's ROWS can produce (a
    non-numeric ``observations.value``, a malformed ``computed_at``). Such a
    day is rolled back, logged at ERROR with the site id and local date, and
    re-selected by the next night's set difference. Anything else — a
    ``sqlite3.Error``, ``ZoneInfoNotFoundError``, ``TypeError`` or any
    programming defect — is systemic, recurs identically for every day in the
    window, and must reach the retry ladder instead of silently skipping the
    whole window.
    """
    generation_id = ensure_published_generation(conn, site_id)
    timezone = _generation_timezone(conn, generation_id)
    days = missing_truth_days(
        conn,
        site_id=site_id,
        tz_generation_id=generation_id,
        timezone=timezone,
        now=now,
        limit=limit,
        after_local_date=(
            None if after_local_date is None else date.fromisoformat(after_local_date)
        ),
    )
    attempted: list[str] = []
    for day in days:
        conn.execute("SAVEPOINT truth_discovery_day")
        try:
            materialize_daily_truth(
                conn,
                site_id=site_id,
                local_date=day.isoformat(),
                tz_generation_id=generation_id,
            )
        except (JobDeferred, JobCancelled, StaleGenerationError):
            # Worker control signals and a database swap are not data faults.
            # None of the three subclasses ValueError, so the catch below
            # already lets them past; this clause states the boundary.
            raise
        except ValueError as exc:
            # Day DATA only. Every carrier inside materialize_daily_truth
            # that a single day's rows can produce is a ValueError; a
            # sqlite3.Error, KeyError/ZoneInfoNotFoundError, TypeError or any
            # programming defect is systemic and must reach the retry ladder.
            # No `finally`: on the propagating path the savepoint is
            # discarded with the whole transaction by `Database._run_immediate`.
            conn.execute("ROLLBACK TO truth_discovery_day")
            conn.execute("RELEASE truth_discovery_day")
            logger.error(
                "daily_truth discovery: day data failed site=%s local_date=%s: %s",
                site_id,
                day.isoformat(),
                sanitized_exception(exc),
            )
        else:
            conn.execute("RELEASE truth_discovery_day")
        attempted.append(day.isoformat())
    return attempted


def load_daily_truth(
    conn: sqlite3.Connection, *, site_id: int, local_date: str
) -> list[sqlite3.Row]:
    """Published-generation truth rows for one (site, local day).

    Generation-bound through the shared accessor — rows of a building or
    retired generation are never returned.
    """
    return conn.execute(
        f"""
        SELECT *
        FROM daily_truth dt
        WHERE dt.site_id = ?
          AND dt.local_date = ?
          AND {published_generation_clause("dt")}
        ORDER BY dt.quantity
        """,
        (site_id, local_date),
    ).fetchall()
