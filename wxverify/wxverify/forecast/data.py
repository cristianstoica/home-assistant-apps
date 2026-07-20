"""Read-side queries for the Forecast page.

All queries here are read-only and run on the read connection. Three shared
exclusions apply to every sample-facing query (spec: applied explicitly, not
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

from wxverify.collection.forecast_validation import invalid_forecast_sample_sql
from wxverify.core.timeutil import parse_utc
from wxverify.scoring.leaderboard import LeaderboardRow, leaderboard

_EXCLUDED_FEEDS_SQL = (
    "f.is_virtual = 0 AND NOT (f.source = 'meteoblue' AND f.model = 'multimodel')"
)


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
    fetch_interval_minutes: int
    stale: bool


def load_future_samples(
    conn: sqlite3.Connection, *, site_id: int, since_valid_at: str
) -> list[FutureSampleRow]:
    """Load the latest-run future samples for a site.

    Latest-run pick: for each ``(feed, variable, valid_at)`` slot only the
    sample from the newest run (``MAX(issued_at)``) survives — a correlated
    subquery, so an older run's hours never mix into a newer run's day. The
    inner subquery repeats the validity predicate so an invalid sample from a
    newer run cannot shadow a valid older one.
    """
    invalid = invalid_forecast_sample_sql("fs")
    invalid_inner = invalid_forecast_sample_sql("fs2")
    rows = conn.execute(
        f"""
        SELECT fs.feed_id, f.source, f.model, fs.variable, fs.issued_at,
               fs.valid_at, fs.value
        FROM forecast_samples fs
        JOIN feeds f ON f.id = fs.feed_id
        WHERE fs.site_id = ?
          AND fs.valid_at >= ?
          AND {_EXCLUDED_FEEDS_SQL}
          AND NOT {invalid}
          AND fs.issued_at = (
              SELECT MAX(fs2.issued_at)
              FROM forecast_samples fs2
              WHERE fs2.site_id = fs.site_id
                AND fs2.feed_id = fs.feed_id
                AND fs2.variable = fs.variable
                AND fs2.valid_at = fs.valid_at
                AND NOT {invalid_inner}
          )
        ORDER BY fs.valid_at, fs.feed_id
        """,
        (site_id, since_valid_at),
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


def load_feed_freshness(
    conn: sqlite3.Connection, *, site_id: int, now: datetime
) -> dict[int, FeedFreshness]:
    """Per-feed freshest ``issued_at`` vs 2x that feed's own fetch interval.

    Staleness is judged per feed against its OWN ``fetch_interval_minutes``
    (spec: never a global constant) so a slow-cadence feed is not falsely
    flagged and a fast one is not silently excused.
    """
    invalid = invalid_forecast_sample_sql("fs")
    rows = conn.execute(
        f"""
        SELECT fs.feed_id, MAX(fs.issued_at) AS latest_issued_at,
               f.fetch_interval_minutes
        FROM forecast_samples fs
        JOIN feeds f ON f.id = fs.feed_id
        WHERE fs.site_id = ?
          AND {_EXCLUDED_FEEDS_SQL}
          AND NOT {invalid}
        GROUP BY fs.feed_id
        """,
        (site_id,),
    ).fetchall()
    out: dict[int, FeedFreshness] = {}
    for row in rows:
        latest = str(row["latest_issued_at"])
        interval = int(row["fetch_interval_minutes"])
        stale = parse_utc(latest) < now - timedelta(minutes=2 * interval)
        out[int(row["feed_id"])] = FeedFreshness(
            feed_id=int(row["feed_id"]),
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
) -> dict[int, LeaderboardRow]:
    """Skill ranking for one (variable, day_ahead) cell, keyed by feed id.

    Reuses the leaderboard skill computation, then applies the Forecast-page
    exclusions AT the ranking step (spec requirement): virtual feeds and the
    meteoblue package feed are removed here explicitly, not left to the
    intersection with fresh samples.
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
    rows = leaderboard(
        conn,
        site_id=site_id,
        variable=variable,
        day_ahead=day_ahead,
        window=window,
    )
    return {row.feed_id: row for row in rows if row.feed_id not in excluded}
