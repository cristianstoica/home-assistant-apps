"""Multimodel mean virtual competitor."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Final

from wxverify.core.timeutil import utc_now
from wxverify.db.site_days import open_day_clause
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
    key: tuple[int, str, str, str]  # X's dict key; a replace reuses its strings
    values: PairValues  # the stored row in ``_MEAN_INSERT_SQL`` order
    first_known_at: str | None


def existing_mean_rows_sql(*, site_scoped: bool, day_clause: str) -> str:
    """The published mean rows of one feed: ``(feed_id,)`` or ``(feed_id, site_id)``.

    ``day_clause`` is a SQL predicate over ``forecast_pairs`` that keeps the
    rows on days taking pairs (``open_day_clause``; ``"1"`` keeps every row).
    """
    site_filter = "AND site_id = ?" if site_scoped else ""
    return f"""
        SELECT id, site_id, variable, issued_at, valid_at, lead_hours, day_ahead,
               forecast, observed, error, abs_error, sq_error,
               cat_hit, cat_false, cat_miss, cat_correct_neg,
               rain_threshold_mm, contributors, first_known_at
        FROM forecast_pairs
        WHERE feed_id = ?
          {site_filter} AND {day_clause}
        """


def materialize_multimodel_mean(
    conn: sqlite3.Connection, site_id: int | None = None
) -> int:
    """Refresh the published mean rows; return the number of distinct mean keys."""
    return apply_delta(conn, compute_multimodel_mean(conn, site_id), now_utc=utc_now())


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
    While the daily close is on, both reads keep only the days that take
    pairs (``open_day_clause``, built at call time), so no op is proposed
    for a done day.
    """
    feed = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    if feed is None:
        return PairDelta((), (), _MEAN_INSERT_SQL, count=0)
    feed_id = int(feed["id"])
    existing: dict[tuple[int, str, str, str], _ExistingMean | None] = {}
    existing_day_clause = open_day_clause(
        conn,
        site_expr="forecast_pairs.site_id",
        instant_expr="forecast_pairs.valid_at",
    )
    for row in conn.execute(
        existing_mean_rows_sql(
            site_scoped=site_id is not None, day_clause=existing_day_clause
        ),
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
            # tz_generation_id) makes this impossible: every row of a site
            # carries the site's one generation.
            raise RuntimeError(f"duplicate published mean key {key!r}")
        existing[key] = _ExistingMean(
            row_id=int(row["id"]),
            key=key,
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
    groups_day_clause = open_day_clause(
        conn, site_expr="fp.site_id", instant_expr="fp.valid_at"
    )
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
          AND {groups_day_clause}
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
            writes.append(InsertOp(row_site_id, values, str(row["valid_at"])))
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
        writes.append(
            ReplaceOp(
                current.row_id,
                row_site_id,
                (row_site_id, feed_id, *current.key[1:], *values[5:]),
                current.key[3],
            )
        )
    deletes = [
        DeleteOp(leftover.row_id, leftover.key[0], leftover.key[3])
        for leftover in sorted(
            (leftover for leftover in existing.values() if leftover is not None),
            key=lambda leftover: leftover.row_id,
        )
    ]
    return PairDelta(tuple(seeds), (*deletes, *writes), _MEAN_INSERT_SQL, count)
