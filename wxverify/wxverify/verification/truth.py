"""daily_truth: the close's truth write and staleness marking.

``daily_truth`` persists one row per (site, local date, quantity, timezone
generation): value, per-quantity eligibility, exclusion reason, coverage
counts, wet/dry counts, guard flags, and provenance (timezone, UTC day
bounds, max source ``computed_at``, generation tag).

A day's rows are written by the daily close's stage 4, through
:func:`write_close_truth`: only for a ``closing`` day, from
:func:`evaluate_day_window` over the day record's zone and bounds, under the
basis of ``completeness.close_admission_basis``.

Consensus-mutation contract: every consensus mutation funnels through
``scoring.consensus.materialize_consensus`` (the sole ``observations``
writer), which calls :func:`mark_daily_truth_stale` for the mutated hour —
affected rows get ``stale = 1``. Nothing regenerates a marked row: the
nightly replay that did was retired with the daily close.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime

from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.site_days import (
    DayStateError,
    WriteKind,
    check_date_write,
    day_record,
)
from wxverify.db.switch import site_generation_id
from wxverify.verification.coverage import (
    VARIABLE_QUANTITIES,
    DayBounds,
    QuantityOutcome,
    evaluate_variable,
)

_TRUTH_VARIABLES: tuple[str, ...] = ("temperature", "wind", "precip")


@dataclass(frozen=True)
class DayEvaluation:
    """One (site, local day)'s evaluated truth, before any write.

    The seam between the evaluation (:func:`evaluate_day_window`), which only
    reads, and the writer (:func:`write_close_truth`), which only writes:
    everything the INSERT needs is carried here as data, so a caller can
    evaluate a day WITHOUT materializing it.

    ``local_date`` is the CANONICAL extended-format spelling of the date the
    caller passed, and ``max_computed_at`` is the newest
    ``observations.computed_at`` behind the day as its stored timestamp
    string — None when no observation in the day carries one.
    """

    timezone: str
    local_date: str
    bounds: DayBounds
    threshold: float
    outcomes: dict[str, QuantityOutcome]
    max_computed_at: str | None


def evaluate_day_window(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    local_date: date,
    timezone: str,
    bounds: DayBounds,
) -> DayEvaluation:
    """Evaluate one (site, local day) in the zone and bounds the caller gives.

    The close's stage 4 calls it with the zone and bounds of the day's record.
    Reads the site's rain threshold and the site's observations in
    ``[bounds.start_utc, bounds.end_utc)``, nothing else.

    READ-ONLY by construction: every statement here is a SELECT and no
    generation is involved.

    Raises ``ValueError`` for a missing site or a value in the day's rows
    that will not coerce.
    """
    site = conn.execute(
        "SELECT rain_threshold_mm FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    if site is None:
        raise ValueError(f"site {site_id} does not exist")
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
            local_date=local_date,
            rain_threshold_mm=threshold,
        ):
            outcomes[outcome.quantity] = outcome
    return DayEvaluation(
        timezone=timezone,
        local_date=local_date.isoformat(),
        bounds=bounds,
        threshold=threshold,
        outcomes=outcomes,
        max_computed_at=max_computed_at,
    )


def write_close_truth(
    conn: sqlite3.Connection,
    evaluation: DayEvaluation,
    *,
    site_id: int,
    admission_basis: str,
    now_utc: datetime,
) -> None:
    """Delete-and-recreate one closing day's five truth rows.

    The close's stage 4 writer. Two checks run first, in this order, and
    either refusal raises ``DayStateError`` with nothing written: the day's
    record must exist and be ``closing`` (the closed-day guard alone would
    not do, since it takes ``TRUTH`` for an ``open`` day too); only then is
    the closed-day guard asked, and its refusal fails the attempt as well.

    The rows take the ``daily_truth`` shape through this function's own
    statements: ``stale`` 0, the evaluation's threshold in
    ``rain_threshold_mm`` (a threshold changed later never reaches the
    day), the record's bounds and zone, and ``admission_basis`` as given —
    ``completeness.close_admission_basis``; nothing on disk is
    preserved. They are keyed by the site's one time zone generation
    (``site_generation_id``), read here. A rerun replaces the rows.
    ``generated_at`` is ``now_utc``, the close's clock.

    Raises ``ValueError`` when the site has no time zone generation. Must
    run on a write connection inside the caller's transaction.
    """
    local_date = evaluation.local_date
    day = date.fromisoformat(local_date)
    record = day_record(conn, site_id, day)
    if record is None or record.state != "closing":
        state = "no record" if record is None else record.state
        raise DayStateError(
            f"site {site_id} day {local_date} ({state}): close truth is"
            " written only for a closing day"
        )
    if not check_date_write(conn, site_id, day, WriteKind.TRUTH, now_utc=now_utc):
        raise DayStateError(
            f"site {site_id} day {local_date}: the guard refused the close truth"
        )
    generation_id = site_generation_id(conn, site_id)
    if generation_id is None:
        raise ValueError(f"site {site_id} has no published timezone generation")
    conn.execute(
        """
        DELETE FROM daily_truth
        WHERE site_id = ? AND local_date = ? AND tz_generation_id = ?
        """,
        (site_id, local_date, generation_id),
    )
    generated_at = isoformat_utc(now_utc)
    start = isoformat_utc(record.day_start_utc)
    end = isoformat_utc(record.day_end_utc)
    conn.executemany(
        """
        INSERT INTO daily_truth
            (site_id, local_date, quantity, value, eligible, exclusion_reason,
             covered_hours, expected_slots, peak_window_ok, wet_hours,
             dry_hours, rain_threshold_mm, day_start_utc, day_end_utc,
             timezone, source_max_computed_at, stale, generated_at,
             tz_generation_id, admission_basis)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
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
                evaluation.threshold if outcome.quantity.startswith("precip") else None,
                start,
                end,
                record.timezone,
                evaluation.max_computed_at,
                generated_at,
                generation_id,
                admission_basis,
            )
            for outcome in evaluation.outcomes.values()
        ],
    )


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
