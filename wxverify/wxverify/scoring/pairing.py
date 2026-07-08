"""Materialize forecast pairs from samples and consensus observations."""

from __future__ import annotations

import sqlite3

from wxverify.core.timeutil import day_ahead
from wxverify.scoring.pair_flags import precip_flags


def pair_real_models(conn: sqlite3.Connection, site_id: int | None = None) -> int:
    params: tuple[object, ...]
    where_site = ""
    if site_id is None:
        params = ()
    else:
        where_site = "AND fs.site_id = ?"
        params = (site_id,)
    rows = conn.execute(
        f"""
        SELECT fs.site_id, fs.feed_id, fs.variable, fs.issued_at, fs.valid_at,
               fs.lead_hours, fs.value AS forecast, obs.value AS observed,
               s.timezone, s.rain_threshold_mm
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
          )
          {where_site}
        """,
        params,
    ).fetchall()
    written = 0
    for row in rows:
        bucket = day_ahead(
            str(row["issued_at"]), str(row["valid_at"]), str(row["timezone"])
        )
        if bucket < 0 or bucket > 7:
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
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 cat_hit, cat_false, cat_miss, cat_correct_neg,
                 rain_threshold_mm)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(row["site_id"]),
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
            ),
        )
        written += cur.rowcount
    return written
