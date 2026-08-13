"""Per-local-day rebuild of a BUILDING timezone generation (§13).

A retrospective timezone correction rebuilds the whole verification history
under a new IANA zone as a build-alongside generation: for each local day
(under the NEW timezone) this module deletes any building-generation rows in
the day's UTC window and re-derives them from the raw inputs — real pairs
from ``forecast_samples`` × ``observations`` (the same join the live pairing
path uses), persistence pairs from lagged observations, the multimodel mean
from the freshly built real pairs, and ``daily_truth`` via the shared
materializer with the building generation's tag.

Published-generation rows are never touched here; the chain runner
(``worker.tz_correction``) owns the flip and cleanup.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import day_ahead, isoformat_utc, parse_utc
from wxverify.db.tz_generations import published_generation_clause
from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.pair_flags import precip_flags
from wxverify.scoring.persistence import insert_persistence_pair
from wxverify.verification.coverage import local_day_bounds
from wxverify.verification.truth import materialize_daily_truth

_MAX_DAY_AHEAD = 7


@dataclass(frozen=True)
class DayReconciliation:
    """§13 reconciliation counts for one rebuilt local day.

    Tallied over the site's PUBLISHED pairs in the day's UTC window, each
    row's day bucket recomputed under the new timezone: ``changed`` /
    ``unchanged`` compare the recomputed bucket against the stored one,
    ``excluded`` counts rows whose recomputed bucket falls outside 0..7.
    ``examined == changed + unchanged + excluded`` by construction.
    """

    examined: int
    changed: int
    unchanged: int
    excluded: int


def generation_day_range(
    conn: sqlite3.Connection, site_id: int, timezone: str
) -> tuple[date, date] | None:
    """Inclusive local-day range (under ``timezone``) spanning every
    observation and forecast-sample ``valid_at`` the site has, or None when
    the site holds no data at all.
    """
    tz = ZoneInfo(timezone)
    stamps: list[str] = []
    for table in ("observations", "forecast_samples"):
        for order in ("ASC", "DESC"):
            row = conn.execute(
                f"""
                SELECT valid_at FROM {table}
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


def mutated_local_days(
    conn: sqlite3.Connection, site_id: int, timezone: str, since_utc: str
) -> list[date]:
    """Local days (new timezone) holding an observation recomputed at or
    after ``since_utc`` — the rescan set for a long-running rebuild during
    which live consensus mutations kept landing. ``>=`` (not ``>``): julianday
    float granularity can collapse a mutation's computed_at to equality with
    the scan stamp; rebuilds are idempotent, so over-matching is cheap.
    """
    tz = ZoneInfo(timezone)
    rows = conn.execute(
        """
        SELECT DISTINCT valid_at FROM observations
        WHERE site_id = ?
          AND computed_at IS NOT NULL
          AND julianday(computed_at) >= julianday(?)
        """,
        (site_id, since_utc),
    ).fetchall()
    days = {parse_utc(str(row["valid_at"])).astimezone(tz).date() for row in rows}
    return sorted(days)


def delete_building_rows(conn: sqlite3.Connection, generation_id: int) -> None:
    """Wipe every row of a building generation (chain-start reset, §14:
    a new chain attempt's first action removes the prior incomplete
    attempt's evidence).
    """
    conn.execute(
        "DELETE FROM forecast_pairs WHERE tz_generation_id = ?", (generation_id,)
    )
    conn.execute("DELETE FROM daily_truth WHERE tz_generation_id = ?", (generation_id,))


def rebuild_generation_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    generation_id: int,
    timezone: str,
    day: date,
    count: bool,
) -> DayReconciliation:
    """Delete-and-recreate one local day of the building generation.

    Idempotent: safe to re-run for the same day (crash-resume re-executes a
    whole chunk). ``count=False`` (rescan/flip passes) skips the published-
    pairs reconciliation tally so a re-processed day is never double-counted.
    """
    bounds = local_day_bounds(day, timezone)
    start = isoformat_utc(bounds.start_utc)
    end = isoformat_utc(bounds.end_utc)
    reconciliation = (
        _reconcile_published_window(conn, site_id, timezone, start, end)
        if count
        else DayReconciliation(0, 0, 0, 0)
    )
    conn.execute(
        """
        DELETE FROM forecast_pairs
        WHERE site_id = ? AND tz_generation_id = ?
          AND julianday(valid_at) >= julianday(?)
          AND julianday(valid_at) < julianday(?)
        """,
        (site_id, generation_id, start, end),
    )
    _rebuild_real_pairs(conn, site_id, generation_id, timezone, start, end)
    _rebuild_persistence_pairs(conn, site_id, generation_id, timezone, start, end)
    _rebuild_multimodel_mean(conn, site_id, generation_id, start, end)
    materialize_daily_truth(
        conn,
        site_id=site_id,
        local_date=day.isoformat(),
        tz_generation_id=generation_id,
    )
    return reconciliation


def _reconcile_published_window(
    conn: sqlite3.Connection, site_id: int, timezone: str, start: str, end: str
) -> DayReconciliation:
    rows = conn.execute(
        f"""
        SELECT fp.issued_at, fp.valid_at, fp.day_ahead
        FROM forecast_pairs fp
        WHERE fp.site_id = ?
          AND julianday(fp.valid_at) >= julianday(?)
          AND julianday(fp.valid_at) < julianday(?)
          AND {published_generation_clause("fp")}
        """,
        (site_id, start, end),
    ).fetchall()
    changed = unchanged = excluded = 0
    for row in rows:
        bucket = day_ahead(str(row["issued_at"]), str(row["valid_at"]), timezone)
        if bucket < 0 or bucket > _MAX_DAY_AHEAD:
            excluded += 1
        elif bucket != int(row["day_ahead"]):
            changed += 1
        else:
            unchanged += 1
    return DayReconciliation(
        examined=len(rows), changed=changed, unchanged=unchanged, excluded=excluded
    )


def _rebuild_real_pairs(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    timezone: str,
    start: str,
    end: str,
) -> None:
    rows = conn.execute(
        """
        SELECT fs.feed_id, fs.variable, fs.issued_at, fs.valid_at,
               fs.lead_hours, fs.value AS forecast, fs.fetched_at,
               obs.value AS observed, s.rain_threshold_mm
        FROM forecast_samples fs
        JOIN observations obs
          ON obs.site_id = fs.site_id
         AND obs.variable = fs.variable
         AND obs.valid_at = fs.valid_at
        JOIN feeds f ON f.id = fs.feed_id
        JOIN sites s ON s.id = fs.site_id
        WHERE fs.site_id = ?
          AND f.is_virtual = 0
          AND fs.lead_hours BETWEEN 1 AND f.max_lead_hours
          AND julianday(fs.valid_at) >= julianday(?)
          AND julianday(fs.valid_at) < julianday(?)
        """,
        (site_id, start, end),
    ).fetchall()
    for row in rows:
        bucket = day_ahead(str(row["issued_at"]), str(row["valid_at"]), timezone)
        if bucket < 0 or bucket > _MAX_DAY_AHEAD:
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
        first_known_at = None if row["fetched_at"] is None else str(row["fetched_at"])
        conn.execute(
            """
            INSERT OR IGNORE INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 cat_hit, cat_false, cat_miss, cat_correct_neg,
                 rain_threshold_mm, first_known_at, tz_generation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
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
                generation_id,
            ),
        )


def _rebuild_persistence_pairs(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    timezone: str,
    start: str,
    end: str,
) -> None:
    feed = conn.execute(
        """
        SELECT id, max_lead_hours
        FROM feeds
        WHERE source='virtual' AND model='_persistence'
        """
    ).fetchone()
    if feed is None:
        return
    feed_id = int(feed["id"])
    max_lead = int(feed["max_lead_hours"])
    site = conn.execute(
        "SELECT rain_threshold_mm FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    if site is None:
        return
    threshold = float(site["rain_threshold_mm"])
    targets = conn.execute(
        """
        SELECT variable, valid_at, value FROM observations
        WHERE site_id = ?
          AND julianday(valid_at) >= julianday(?)
          AND julianday(valid_at) < julianday(?)
        """,
        (site_id, start, end),
    ).fetchall()
    for target in targets:
        variable = str(target["variable"])
        valid_at = str(target["valid_at"])
        observed = float(target["value"])
        rain_threshold = threshold if variable == "precip" else None
        valid = parse_utc(valid_at)
        for lead in range(1, max_lead + 1):
            issued_at = isoformat_utc(valid - timedelta(hours=lead))
            lagged = conn.execute(
                """
                SELECT value, computed_at FROM observations
                WHERE site_id=? AND variable=? AND valid_at=?
                """,
                (site_id, variable, issued_at),
            ).fetchone()
            if lagged is None:
                continue
            bucket = day_ahead(issued_at, valid_at, timezone)
            if bucket < 0 or bucket > _MAX_DAY_AHEAD:
                continue
            insert_persistence_pair(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable=variable,
                issued_at=issued_at,
                valid_at=valid_at,
                lead=lead,
                bucket=bucket,
                forecast=float(lagged["value"]),
                observed=observed,
                rain_threshold=rain_threshold,
                first_known_at=(
                    None
                    if lagged["computed_at"] is None
                    else str(lagged["computed_at"])
                ),
                generation_id=generation_id,
            )


def _rebuild_multimodel_mean(
    conn: sqlite3.Connection,
    site_id: int,
    generation_id: int,
    start: str,
    end: str,
) -> None:
    """Multimodel mean over the building generation's real pairs in the
    day's UTC window (the day's building rows, mean included, were deleted
    at the top of :func:`rebuild_generation_day`).
    """
    feed = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    if feed is None:
        return
    groups = conn.execute(
        f"""
        SELECT fp.variable, fp.issued_at, fp.valid_at, fp.lead_hours,
               fp.day_ahead, fp.observed, AVG(fp.forecast) AS forecast,
               COUNT(*) AS contributors, s.rain_threshold_mm
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        JOIN sites s ON s.id = fp.site_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE fp.site_id = ?
          AND fp.tz_generation_id = ?
          AND f.is_virtual = 0
          AND julianday(fp.valid_at) >= julianday(?)
          AND julianday(fp.valid_at) < julianday(?)
          AND {active_competitor_clause(site_expr="fp.site_id")}
        GROUP BY fp.variable, fp.issued_at, fp.valid_at, fp.lead_hours,
                 fp.day_ahead, fp.observed, s.rain_threshold_mm
        HAVING COUNT(*) >= 2
        """,
        (site_id, generation_id, start, end),
    ).fetchall()
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
        conn.execute(
            """
            INSERT OR IGNORE INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 cat_hit, cat_false, cat_miss, cat_correct_neg,
                 rain_threshold_mm, contributors, tz_generation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                int(feed["id"]),
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
                generation_id,
            ),
        )
