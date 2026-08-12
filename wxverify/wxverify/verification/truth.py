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
fingerprint (§14).

Reads are generation-bound through the shared
``published_generation_clause`` accessor — a partially built correction
generation is never read.
"""

from __future__ import annotations

import sqlite3
from datetime import date

from wxverify.core.timeutil import isoformat_utc, parse_utc
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
