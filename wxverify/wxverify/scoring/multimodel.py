"""Multimodel mean virtual competitor."""

from __future__ import annotations

import sqlite3

from wxverify.db.tz_generations import (
    ensure_published_generation,
    published_generation_clause,
)
from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.pair_flags import precip_flags


def materialize_multimodel_mean(
    conn: sqlite3.Connection, site_id: int | None = None
) -> int:
    feed = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_multimodel_mean'"
    ).fetchone()
    if feed is None:
        return 0
    # Delete-and-recreate is scoped to the PUBLISHED generation (§13): a
    # building correction generation's mean rows belong to the correction
    # chain (scoring.tz_rebuild derives them per rebuilt day) and must not
    # be deleted out from under it mid-chain; retired rows are removed by
    # the chain's post-flip cleanup, not here.
    if site_id is not None:
        conn.execute(
            f"""
            DELETE FROM forecast_pairs
            WHERE site_id=? AND feed_id=?
              AND {published_generation_clause("forecast_pairs")}
            """,
            (site_id, int(feed["id"])),
        )
        params: tuple[object, ...] = (site_id,)
        site_filter = "AND fp.site_id = ?"
    else:
        conn.execute(
            f"""
            DELETE FROM forecast_pairs
            WHERE feed_id=?
              AND {published_generation_clause("forecast_pairs")}
            """,
            (int(feed["id"]),),
        )
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
    ).fetchall()
    written = 0
    generation_ids: dict[int, int] = {}
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
        generation_id = generation_ids.get(row_site_id)
        if generation_id is None:
            generation_id = ensure_published_generation(conn, row_site_id)
            generation_ids[row_site_id] = generation_id
        # first_known_at is intentionally NULL for the virtual mean: its
        # availability is not defined by a single source sample, and NULL is
        # never invented — as-of reads exclude it with a recorded reason.
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 cat_hit, cat_false, cat_miss, cat_correct_neg,
                 rain_threshold_mm, contributors, tz_generation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row_site_id,
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
        written += cur.rowcount
    return written
