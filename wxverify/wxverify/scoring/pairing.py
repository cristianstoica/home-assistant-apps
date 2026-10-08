"""Materialize forecast pairs from samples and consensus observations."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Sequence
from typing import Final

from wxverify.core.timeutil import day_ahead
from wxverify.db.tz_generations import published_generation_clause
from wxverify.db.wind_basis import wind_open_clause
from wxverify.scoring.pair_flags import precip_flags
from wxverify.scoring.split import InsertOp, PairDelta, apply_delta

logger = logging.getLogger(__name__)

# The real-model pairing candidates: a sample with a matching observation, a
# lead in the feed's range, no published pair yet, and an open wind basis for
# wind. compute_real_model_pairs runs it whole or for one site;
# real_model_pair_pending runs it for one sample id, so both read the same
# predicate.
_CANDIDATE_PAIRS_SQL: Final = f"""
        SELECT fs.site_id, fs.feed_id, fs.variable, fs.issued_at, fs.valid_at,
               fs.lead_hours, fs.value AS forecast, fs.fetched_at,
               obs.value AS observed, s.timezone, s.rain_threshold_mm
        FROM forecast_samples fs
        JOIN observations obs
          ON obs.site_id = fs.site_id
         AND obs.variable = fs.variable
         AND obs.valid_at = fs.valid_at
        JOIN feeds f ON f.id = fs.feed_id
        JOIN sites s ON s.id = fs.site_id
        WHERE f.is_virtual = 0
          AND fs.lead_hours BETWEEN 1 AND f.max_lead_hours
          AND NOT EXISTS (
              SELECT 1 FROM forecast_pairs fp
              WHERE fp.site_id = fs.site_id
                AND fp.feed_id = fs.feed_id
                AND fp.variable = fs.variable
                AND fp.issued_at = fs.issued_at
                AND fp.valid_at = fs.valid_at
                AND {published_generation_clause("fp")}
          )
          AND {wind_open_clause("fs.variable", "fs.site_id")}
        """
REAL_MODEL_PAIR_PROBE_SQL: Final = _CANDIDATE_PAIRS_SQL + " AND fs.id = ?"
_PAIR_MAX_DAY_AHEAD: Final = 7

_PAIR_INSERT_SQL: Final = """
            INSERT OR IGNORE INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 cat_hit, cat_false, cat_miss, cat_correct_neg,
                 rain_threshold_mm, first_known_at, tz_generation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """


def pair_real_models(conn: sqlite3.Connection, site_id: int | None = None) -> int:
    """Insert the missing real-model pairs; return the number inserted."""
    return apply_delta(conn, compute_real_model_pairs(conn, site_id))


def compute_real_model_pairs(
    conn: sqlite3.Connection, site_id: int | None = None
) -> PairDelta:
    """Read-only: the real-model pairs ``pair_real_models`` would insert.

    Every candidate sample (a matching observation, a lead in range, no
    published pair yet) that passes the day-ahead bucket filter becomes one
    ``InsertOp``; the seed sites are those rows' sites, in first-row order.
    """
    sql: str
    params: tuple[object, ...]
    if site_id is None:
        sql = _CANDIDATE_PAIRS_SQL
        params = ()
    else:
        sql = _CANDIDATE_PAIRS_SQL + " AND fs.site_id = ?"
        params = (site_id,)
    rows = conn.execute(sql, params)
    seeds: dict[int, None] = {}
    ops: list[InsertOp] = []
    for row in rows:
        bucket = _pair_bucket(row)
        if bucket is None:
            continue
        forecast = float(row["forecast"])
        observed = float(row["observed"])
        variable = str(row["variable"])
        rain_threshold = (
            float(row["rain_threshold_mm"]) if variable == "precip" else None
        )
        hit, false, miss, correct_neg = precip_flags(
            variable, forecast, observed, rain_threshold
        )
        row_site_id = int(row["site_id"])
        seeds.setdefault(row_site_id)
        # Availability (outcome knowability): the source sample's ingestion
        # time. NULL fetched_at stays NULL — never invented; downstream as-of
        # reads exclude NULL-availability pairs with a recorded reason.
        first_known_at = None if row["fetched_at"] is None else str(row["fetched_at"])
        ops.append(
            InsertOp(
                row_site_id,
                (
                    row_site_id,
                    int(row["feed_id"]),
                    variable,
                    str(row["issued_at"]),
                    str(row["valid_at"]),
                    int(row["lead_hours"]),
                    bucket,
                    forecast,
                    observed,
                    forecast - observed,
                    abs(forecast - observed),
                    (forecast - observed) ** 2,
                    hit,
                    false,
                    miss,
                    correct_neg,
                    rain_threshold,
                    first_known_at,
                ),
            )
        )
    return PairDelta(tuple(seeds), tuple(ops), _PAIR_INSERT_SQL, count=None)


def _pair_bucket(row: sqlite3.Row) -> int | None:
    """The row's day-ahead bucket when pairing keeps the row, else None."""
    bucket = day_ahead(
        str(row["issued_at"]), str(row["valid_at"]), str(row["timezone"])
    )
    if bucket < 0 or bucket > _PAIR_MAX_DAY_AHEAD:
        return None
    return bucket


def real_model_pair_pending(
    conn: sqlite3.Connection, sample_ids: Sequence[int]
) -> bool:
    """Whether compute_real_model_pairs would pair any of sample_ids right now.

    Allowlist: False only when every id was probed and none would pair.
    A bucket that cannot be computed returns True, so the score run meets
    the error it meets today. Database errors propagate and roll back
    the caller's transaction.
    """
    for sample_id in sample_ids:
        row = conn.execute(REAL_MODEL_PAIR_PROBE_SQL, (sample_id,)).fetchone()
        if row is None:
            continue
        try:
            if _pair_bucket(row) is not None:
                return True
        except (ValueError, KeyError, OverflowError):
            logger.warning(
                "fetch score gate: day-ahead bucket failed site=%s sample=%s; "
                "queueing the score run",
                row["site_id"],
                sample_id,
            )
            return True
    return False
