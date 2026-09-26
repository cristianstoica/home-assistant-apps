"""Multimodel mean virtual competitor."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Final

from wxverify.db.tz_generations import published_generation_clause
from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.pair_flags import precip_flags
from wxverify.scoring.split import (
    DeleteOp,
    InsertOp,
    PairDelta,
    PairValues,
    ReplaceOp,
    apply_delta,
)

_MEAN_INSERT_SQL: Final = """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 cat_hit, cat_false, cat_miss, cat_correct_neg,
                 rain_threshold_mm, contributors, tz_generation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """


@dataclass(frozen=True, slots=True)
class _ExistingMean:
    """One published mean row read before the diff, keyed by its unique key."""

    row_id: int
    values: PairValues  # the stored row in ``_MEAN_INSERT_SQL`` order
    first_known_at: str | None


def existing_mean_rows_sql(*, site_scoped: bool) -> str:
    """The published mean rows of one feed: ``(feed_id,)`` or ``(feed_id, site_id)``."""
    site_filter = "AND site_id = ?" if site_scoped else ""
    return f"""
        SELECT id, site_id, variable, issued_at, valid_at, lead_hours, day_ahead,
               forecast, observed, error, abs_error, sq_error,
               cat_hit, cat_false, cat_miss, cat_correct_neg,
               rain_threshold_mm, contributors, first_known_at
        FROM forecast_pairs
        WHERE feed_id = ?
          AND {published_generation_clause("forecast_pairs")}
          {site_filter}
        """


def materialize_multimodel_mean(
    conn: sqlite3.Connection, site_id: int | None = None
) -> int:
    """Refresh the published mean rows; return the number of distinct mean keys."""
    return apply_delta(conn, compute_multimodel_mean(conn, site_id))


def compute_multimodel_mean(
    conn: sqlite3.Connection, site_id: int | None = None
) -> PairDelta:
    """Read-only: the changes that bring the published mean rows up to date.

    Reads the existing published mean rows (X) and streams the recomputed
    means (D). A D key missing from X is an ``InsertOp``; a D key whose X
    row differs, or carries a ``first_known_at``, is a ``ReplaceOp``; an
    unchanged row emits nothing. The first D group of a key wins and later
    ones are ignored. X rows no D group matched become ``DeleteOp``s, in
    ascending id order, ahead of the D-ordered writes. ``count`` is the
    number of distinct D keys; the seed sites are every D group's site.
    """
    feed = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    if feed is None:
        return PairDelta((), (), _MEAN_INSERT_SQL, count=0)
    feed_id = int(feed["id"])
    # The X read and the diff are scoped to the PUBLISHED generation (§13): a
    # building correction generation's mean rows belong to the correction
    # chain (scoring.tz_rebuild derives them per rebuilt day) and are never
    # read or written here; retired rows are removed by the chain's
    # post-flip cleanup, not here.
    existing: dict[tuple[int, str, str, str], _ExistingMean | None] = {}
    for row in conn.execute(
        existing_mean_rows_sql(site_scoped=site_id is not None),
        (feed_id,) if site_id is None else (feed_id, site_id),
    ):
        key = (
            int(row["site_id"]),
            str(row["variable"]),
            str(row["issued_at"]),
            str(row["valid_at"]),
        )
        if key in existing:
            # UNIQUE(site_id, feed_id, variable, issued_at, valid_at,
            # tz_generation_id) makes this impossible under a correct
            # published clause and key.
            raise RuntimeError(f"duplicate published mean key {key!r}")
        existing[key] = _ExistingMean(
            row_id=int(row["id"]),
            values=(
                row["site_id"],
                feed_id,
                row["variable"],
                row["issued_at"],
                row["valid_at"],
                row["lead_hours"],
                row["day_ahead"],
                row["forecast"],
                row["observed"],
                row["error"],
                row["abs_error"],
                row["sq_error"],
                row["cat_hit"],
                row["cat_false"],
                row["cat_miss"],
                row["cat_correct_neg"],
                row["rain_threshold_mm"],
                row["contributors"],
            ),
            first_known_at=(
                None if row["first_known_at"] is None else str(row["first_known_at"])
            ),
        )
    if site_id is not None:
        params: tuple[object, ...] = (site_id,)
        site_filter = "AND fp.site_id = ?"
    else:
        params = ()
        site_filter = ""
    groups = conn.execute(
        f"""
        SELECT fp.site_id, fp.variable, fp.issued_at, fp.valid_at, fp.lead_hours,
               fp.day_ahead, fp.observed, AVG(fp.forecast) AS forecast,
               COUNT(*) AS contributors, s.rain_threshold_mm
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        JOIN sites s ON s.id = fp.site_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE f.is_virtual = 0
          AND {active_competitor_clause(site_expr="fp.site_id")}
          AND {published_generation_clause("fp")}
          {site_filter}
        GROUP BY fp.site_id, fp.variable, fp.issued_at, fp.valid_at, fp.lead_hours,
                 fp.day_ahead, fp.observed, s.rain_threshold_mm
        HAVING COUNT(*) >= 2
        """,
        params,
    )
    seeds: dict[int, None] = {}
    writes: list[InsertOp | ReplaceOp] = []
    count = 0
    for row in groups:
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
        # first_known_at is intentionally NULL for the virtual mean: its
        # availability is not defined by a single source sample, and NULL is
        # never invented — as-of reads exclude it with a recorded reason.
        values: PairValues = (
            row_site_id,
            feed_id,
            variable,
            str(row["issued_at"]),
            str(row["valid_at"]),
            int(row["lead_hours"]),
            int(row["day_ahead"]),
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
            int(row["contributors"]),
        )
        key = (row_site_id, variable, str(row["issued_at"]), str(row["valid_at"]))
        if key not in existing:
            existing[key] = None
            count += 1
            writes.append(InsertOp(row_site_id, values))
            continue
        current = existing[key]
        if current is None:
            # Already consumed by an earlier group of this key: the first
            # group wins and later ones are ignored.
            continue
        existing[key] = None
        count += 1
        if current.values == values and current.first_known_at is None:
            continue
        writes.append(ReplaceOp(current.row_id, row_site_id, values))
    deletes = [
        DeleteOp(row_id)
        for row_id in sorted(
            leftover.row_id for leftover in existing.values() if leftover is not None
        )
    ]
    return PairDelta(tuple(seeds), (*deletes, *writes), _MEAN_INSERT_SQL, count)
