"""Outcome knowability at a canonical decision time T (plan §6).

A ``forecast_pairs`` row is *knowable* at T iff::

    first_known_at IS NOT NULL
    AND first_known_at <= T
    AND valid_at + DELTA_consensus <= T
    AND (target observation's computed_at, where present) <= T

``first_known_at`` is producer-defined availability (sample ``fetched_at``
for real pairs, source-observation ``computed_at`` for persistence pairs);
``created_at`` is rewritten by consensus rescore and is never trusted.
DELTA_consensus is the versioned §17 constant
(:data:`~wxverify.verification.methodology.CONSENSUS_LAG_HOURS`). The
``computed_at`` AND-guard applies only where the target observation carries
one — later timestamps only *exclude more*, the conservative-safe direction.

Pairs with NULL ``first_known_at`` (multimodel-written virtual pairs by
design, pre-v4 backfilled rows, NULL-availability sources) are excluded from
as-of evaluation with a recorded reason; virtual products are recomputed from
as-of member samples by the backtest engine instead of read from pairs.

Timestamp comparisons go through ``julianday(...)`` on BOTH sides so mixed
spellings (``Z`` vs ``+00:00``, with/without fractional seconds) compare as
instants, never as strings.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from wxverify.db.tz_generations import published_generation_clause
from wxverify.verification.methodology import CONSENSUS_LAG_HOURS

# Recorded exclusion reasons (plan §6). Persisted verbatim by the run
# evidence layer; classification is by FIRST failing condition, in the same
# order the predicate states them.
EXCLUDE_NULL_FIRST_KNOWN_AT = "null_first_known_at"
EXCLUDE_FIRST_KNOWN_AFTER_T = "first_known_after_t"
EXCLUDE_CONSENSUS_LAG = "consensus_lag_after_t"
EXCLUDE_COMPUTED_AT_AFTER_T = "computed_at_after_t"


def knowable_pair_predicate(alias: str, *, as_of: str) -> tuple[str, tuple[str, ...]]:
    """SQL predicate (and its positional params) for pair knowability at T.

    ``alias`` is the ``forecast_pairs`` alias (or bare table name) in the
    enclosing statement. The returned params bind T once per comparison, in
    textual order; append them to the statement's params in the position the
    clause occupies.
    """
    obs = f"ko_{alias}"
    sql = (
        f"({alias}.first_known_at IS NOT NULL"
        f" AND julianday({alias}.first_known_at) <= julianday(?)"
        f" AND julianday({alias}.valid_at, '+{CONSENSUS_LAG_HOURS} hours')"
        " <= julianday(?)"
        " AND NOT EXISTS ("
        f"SELECT 1 FROM observations {obs}"
        f" WHERE {obs}.site_id = {alias}.site_id"
        f" AND {obs}.variable = {alias}.variable"
        f" AND {obs}.valid_at = {alias}.valid_at"
        f" AND {obs}.computed_at IS NOT NULL"
        f" AND julianday({obs}.computed_at) > julianday(?)))"
    )
    return sql, (as_of, as_of, as_of)


@dataclass(frozen=True)
class PairKnowabilityExclusions:
    """Per-reason counts of pairs excluded from as-of evaluation at T."""

    null_first_known_at: int
    first_known_after_t: int
    consensus_lag_after_t: int
    computed_at_after_t: int

    @property
    def total(self) -> int:
        return (
            self.null_first_known_at
            + self.first_known_after_t
            + self.consensus_lag_after_t
            + self.computed_at_after_t
        )

    def as_reasons(self) -> dict[str, int]:
        """Reason-keyed counts, ready for run-evidence persistence."""
        return {
            EXCLUDE_NULL_FIRST_KNOWN_AT: self.null_first_known_at,
            EXCLUDE_FIRST_KNOWN_AFTER_T: self.first_known_after_t,
            EXCLUDE_CONSENSUS_LAG: self.consensus_lag_after_t,
            EXCLUDE_COMPUTED_AT_AFTER_T: self.computed_at_after_t,
        }


def pair_knowability_exclusions(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    as_of: str,
    window_cutoff: str | None = None,
) -> PairKnowabilityExclusions:
    """Count generation-bound pairs excluded from as-of evaluation at T.

    Classification is by FIRST failing knowability condition so each excluded
    row is counted under exactly one reason. Only published-generation rows
    are considered — retired-generation rows are outside every read path, not
    an as-of exclusion.
    """
    lag = f"julianday(fp.valid_at, '+{CONSENSUS_LAG_HOURS} hours') <= julianday(?)"
    known = "julianday(fp.first_known_at) <= julianday(?)"
    computed_after = (
        "EXISTS (SELECT 1 FROM observations ko"
        " WHERE ko.site_id = fp.site_id"
        " AND ko.variable = fp.variable"
        " AND ko.valid_at = fp.valid_at"
        " AND ko.computed_at IS NOT NULL"
        " AND julianday(ko.computed_at) > julianday(?))"
    )
    window_clause = "" if window_cutoff is None else "AND fp.valid_at >= ?"
    row = conn.execute(
        f"""
        SELECT
            SUM(CASE WHEN fp.first_known_at IS NULL THEN 1 ELSE 0 END)
                AS null_first_known_at,
            SUM(CASE WHEN fp.first_known_at IS NOT NULL
                      AND NOT {known} THEN 1 ELSE 0 END)
                AS first_known_after_t,
            SUM(CASE WHEN fp.first_known_at IS NOT NULL
                      AND {known} AND NOT {lag} THEN 1 ELSE 0 END)
                AS consensus_lag_after_t,
            SUM(CASE WHEN fp.first_known_at IS NOT NULL
                      AND {known} AND {lag}
                      AND {computed_after} THEN 1 ELSE 0 END)
                AS computed_at_after_t
        FROM forecast_pairs fp
        WHERE fp.site_id = ? AND fp.variable = ? AND fp.day_ahead = ?
          AND {published_generation_clause("fp")}
          {window_clause}
        """,
        (
            as_of,
            as_of,
            as_of,
            as_of,
            as_of,
            as_of,
            site_id,
            variable,
            day_ahead,
            *(() if window_cutoff is None else (window_cutoff,)),
        ),
    ).fetchone()
    return PairKnowabilityExclusions(
        null_first_known_at=int(row["null_first_known_at"] or 0),
        first_known_after_t=int(row["first_known_after_t"] or 0),
        consensus_lag_after_t=int(row["consensus_lag_after_t"] or 0),
        computed_at_after_t=int(row["computed_at_after_t"] or 0),
    )
