"""Read-side queries for the Forecast page.

All queries here are read-only and run on the read connection. Three shared
exclusions apply to every sample-facing query (applied explicitly, not
incidentally via intersections):

* virtual feeds (``is_virtual = 1``) never surface on the Forecast page —
  they are scoring baselines, not forward forecasts a user would consume;
* the ``(meteoblue, multimodel)`` package feed is the subscription unit and
  carries no forward samples of its own — the member-model feeds do;
* samples failing :func:`invalid_forecast_sample_sql` (out-of-range values,
  unknown variables, malformed timestamps, ``lead_hours < 1``) are dropped,
  which is also what filters negative precip out of daily totals.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from wxverify.collection.forecast_validation import (
    FORECAST_VARIABLES,
    invalid_forecast_sample_sql,
)
from wxverify.core.timeutil import parse_utc
from wxverify.scoring.leaderboard import LeaderboardRow, asof_leaderboard, leaderboard
from wxverify.worker.cadence import parse_fetch_interval_minutes

_EXCLUDED_FEEDS_SQL = (
    "f.is_virtual = 0 AND NOT (f.source = 'meteoblue' AND f.model = 'multimodel')"
)

# Recorded exclusion reason (plan §6): a forecast run whose availability
# timestamp is NULL cannot be placed on the as-of timeline.
SAMPLE_EXCLUDE_NULL_FETCHED_AT = "null_fetched_at"


@dataclass(frozen=True)
class FutureSampleRow:
    """One validated, latest-run forecast sample with feed identity attached."""

    feed_id: int
    source: str
    model: str
    variable: str
    issued_at: str
    valid_at: str
    value: float


@dataclass(frozen=True)
class FeedFreshness:
    """Per-feed freshest run, judged against the feed's own cadence."""

    feed_id: int
    latest_issued_at: str
    fetch_interval_minutes: int | None
    stale: bool


def load_future_samples(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    since_valid_at: str,
    as_of: str | None = None,
) -> list[FutureSampleRow]:
    """Load the latest-run future samples for a site.

    Latest-run pick: for each ``(feed, variable, valid_at)`` slot only the
    sample from the newest run survives. ``UNIQUE(site_id, feed_id, variable,
    issued_at, valid_at)`` makes ``issued_at`` unique within a slot, so
    ``ROW_NUMBER() = 1`` over ``issued_at DESC`` selects exactly the row the old
    ``issued_at = (SELECT MAX(...))`` form selected -- with no ties possible and
    no correlated rescan per row. The validity predicate is applied before the
    window function, so an invalid sample from a newer run still cannot shadow a
    valid older one. ``variable IN (...)`` is implied by ``NOT invalid`` and is
    stated only to make idx_samples_site_var_valid's valid_at range reachable.

    ``as_of`` (plan §6): restrict to runs available at T — ``issued_at <= T``
    AND ``fetched_at <= T`` — applied BEFORE the latest-run pick, so the
    newest run *at T* wins, not today's newest. Rows with NULL ``fetched_at``
    are excluded (count them via :func:`count_null_availability_samples` and
    record :data:`SAMPLE_EXCLUDE_NULL_FETCHED_AT`). With ``as_of=None`` the
    statement is byte-identical to the production one.
    """
    invalid = invalid_forecast_sample_sql("fs")
    variables = ", ".join(f"'{variable}'" for variable in FORECAST_VARIABLES)
    asof_clause = ""
    asof_params: tuple[object, ...] = ()
    if as_of is not None:
        asof_clause = (
            "AND fs.fetched_at IS NOT NULL "
            "AND julianday(fs.fetched_at) <= julianday(?) "
            "AND julianday(fs.issued_at) <= julianday(?)"
        )
        asof_params = (as_of, as_of)
    rows = conn.execute(
        f"""
        SELECT feed_id, source, model, variable, issued_at, valid_at, value
        FROM (
            SELECT fs.feed_id, f.source, f.model, fs.variable, fs.issued_at,
                   fs.valid_at, fs.value,
                   ROW_NUMBER() OVER (
                       PARTITION BY fs.feed_id, fs.variable, fs.valid_at
                       ORDER BY fs.issued_at DESC
                   ) AS rn
            FROM forecast_samples fs
            JOIN feeds f ON f.id = fs.feed_id
            WHERE fs.site_id = ?
              AND fs.variable IN ({variables})
              AND fs.valid_at >= ?
              AND {_EXCLUDED_FEEDS_SQL}
              AND NOT {invalid}
              {asof_clause}
        )
        WHERE rn = 1
        ORDER BY valid_at, feed_id
        """,
        (site_id, since_valid_at, *asof_params),
    ).fetchall()
    return [
        FutureSampleRow(
            feed_id=int(row["feed_id"]),
            source=str(row["source"]),
            model=str(row["model"]),
            variable=str(row["variable"]),
            issued_at=str(row["issued_at"]),
            valid_at=str(row["valid_at"]),
            value=float(row["value"]),
        )
        for row in rows
    ]


def count_null_availability_samples(
    conn: sqlite3.Connection, *, site_id: int, since_valid_at: str
) -> int:
    """Count otherwise-eligible samples excluded for NULL ``fetched_at``.

    The as-of path (:func:`load_future_samples` with ``as_of``) cannot place
    a NULL-availability run on the timeline; plan §6 requires the exclusion
    to be recorded, not silent. Same eligibility predicates as the loader
    (variables, valid_at floor, feed exclusions, validity) so the count is
    exactly the rows the as-of restriction dropped for this reason.
    """
    invalid = invalid_forecast_sample_sql("fs")
    variables = ", ".join(f"'{variable}'" for variable in FORECAST_VARIABLES)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM forecast_samples fs
        JOIN feeds f ON f.id = fs.feed_id
        WHERE fs.site_id = ?
          AND fs.variable IN ({variables})
          AND fs.valid_at >= ?
          AND {_EXCLUDED_FEEDS_SQL}
          AND NOT {invalid}
          AND fs.fetched_at IS NULL
        """,
        (site_id, since_valid_at),
    ).fetchone()
    return int(row["n"])


def load_feed_freshness(
    conn: sqlite3.Connection, *, site_id: int, now: datetime
) -> dict[int, FeedFreshness]:
    """Per-feed freshest ``issued_at`` vs 2x that feed's own fetch interval.

    Staleness is judged per feed against its OWN ``fetch_interval_minutes``
    (never a global constant) so a slow-cadence feed is not falsely
    flagged and a fast one is not silently excused.
    """
    invalid = invalid_forecast_sample_sql("fs")
    grid = ", ".join(f"('{variable}')" for variable in FORECAST_VARIABLES)
    rows = conn.execute(
        f"""
        WITH grid_variables(variable) AS (VALUES {grid}),
        candidates AS (
            SELECT f.id AS feed_id, f.fetch_interval_minutes, v.variable
            FROM feeds f, grid_variables v
            WHERE {_EXCLUDED_FEEDS_SQL}
        )
        SELECT c.feed_id, c.fetch_interval_minutes,
               MAX((
                   SELECT fs.issued_at
                   FROM forecast_samples fs
                   WHERE fs.site_id = ?
                     AND fs.feed_id = c.feed_id
                     AND fs.variable = c.variable
                     AND NOT {invalid}
                   ORDER BY fs.issued_at DESC
                   LIMIT 1
               )) AS latest_issued_at
        FROM candidates c
        GROUP BY c.feed_id, c.fetch_interval_minutes
        HAVING latest_issued_at IS NOT NULL
        """,
        (site_id,),
    ).fetchall()
    out: dict[int, FeedFreshness] = {}
    for row in rows:
        feed_id = int(row["feed_id"])
        latest = str(row["latest_issued_at"])
        interval = parse_fetch_interval_minutes(
            row["fetch_interval_minutes"],
            context=f"feed freshness feed_id={feed_id}",
        )
        if interval is None:
            # Foreign/corrupt or out-of-range cadence: fail closed (stale)
            # rather than silently dropping the feed from the freshness map
            # or inventing a cadence to judge it against.
            out[feed_id] = FeedFreshness(
                feed_id=feed_id,
                latest_issued_at=latest,
                fetch_interval_minutes=None,
                stale=True,
            )
            continue
        stale = parse_utc(latest) < now - timedelta(minutes=2 * interval)
        out[feed_id] = FeedFreshness(
            feed_id=feed_id,
            latest_issued_at=latest,
            fetch_interval_minutes=interval,
            stale=stale,
        )
    return out


def samples_fingerprint(conn: sqlite3.Connection, *, site_id: int) -> str:
    """Monotonic change token for the auto-poll: MAX(rowid) of site samples.

    Every fetch inserts new rows (the unique key includes ``issued_at``), so
    any new run advances the fingerprint; an unchanged fingerprint means the
    tiles fragment can answer 204 and leave the open drill-down untouched.
    """
    row = conn.execute(
        "SELECT COALESCE(MAX(id), 0) AS fp FROM forecast_samples WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    return str(int(row["fp"]))


def forecast_ranking(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str = "rolling",
    as_of: str | None = None,
    declared_min_n: int | None = None,
    declared_window_days: int | None = None,
) -> dict[int, LeaderboardRow]:
    """Skill ranking for one (variable, day_ahead) cell, keyed by feed id.

    Reuses the leaderboard skill computation, then applies the Forecast-page
    exclusions AT the ranking step by design: virtual feeds and the meteoblue
    package feed are removed here explicitly, not left to the intersection
    with fresh samples. Keep the filter here even if it looks redundant.

    ``as_of`` (plan §6): the ranking is recomputed live from pairs knowable
    at T under the run's declared configuration (``declared_min_n``,
    ``declared_window_days`` — never live settings, never ``score_cache``;
    ``window`` is ignored on this path). The eligibility exclusions below
    apply IDENTICALLY on both paths — one filter, two row sources.
    """
    excluded = {
        int(row["id"])
        for row in conn.execute(
            """
            SELECT id FROM feeds
            WHERE is_virtual = 1
               OR (source = 'meteoblue' AND model = 'multimodel')
            """
        ).fetchall()
    }
    if as_of is not None:
        if declared_min_n is None:
            raise ValueError("as-of forecast_ranking requires declared_min_n")
        rows = asof_leaderboard(
            conn,
            site_id=site_id,
            variable=variable,
            day_ahead=day_ahead,
            as_of=as_of,
            min_n=declared_min_n,
            window_days=declared_window_days,
        )
    else:
        rows = leaderboard(
            conn,
            site_id=site_id,
            variable=variable,
            day_ahead=day_ahead,
            window=window,
        )
    return {row.feed_id: row for row in rows if row.feed_id not in excluded}
