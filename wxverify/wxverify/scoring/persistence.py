"""Lead-specific persistence baseline pairs."""

from __future__ import annotations

import sqlite3
from datetime import timedelta

from wxverify.core.timeutil import day_ahead, isoformat_utc, parse_utc
from wxverify.scoring.pair_flags import precip_flags


def materialize_persistence(
    conn: sqlite3.Connection, site_id: int | None = None
) -> int:
    feed = conn.execute(
        """
        SELECT id, max_lead_hours
        FROM feeds
        WHERE source='virtual' AND model='_persistence'
        """
    ).fetchone()
    if feed is None:
        return 0
    if site_id is None:
        conn.execute("DELETE FROM forecast_pairs WHERE feed_id=?", (int(feed["id"]),))
    else:
        conn.execute(
            "DELETE FROM forecast_pairs WHERE site_id=? AND feed_id=?",
            (site_id, int(feed["id"])),
        )
    where = "" if site_id is None else "WHERE site_id = ?"
    params: tuple[object, ...] = () if site_id is None else (site_id,)
    observations = conn.execute(
        f"""
        SELECT o.site_id, o.variable, o.valid_at, o.value, s.timezone,
               s.rain_threshold_mm
        FROM observations o
        JOIN sites s ON s.id = o.site_id
        {where}
        """,
        params,
    ).fetchall()
    written = 0
    max_lead = int(feed["max_lead_hours"])
    for obs in observations:
        valid = parse_utc(str(obs["valid_at"]))
        for lead in range(1, max_lead + 1):
            issued_at = isoformat_utc(valid - timedelta(hours=lead))
            source_valid = isoformat_utc(valid - timedelta(hours=lead))
            lagged = conn.execute(
                """
                SELECT value FROM observations
                WHERE site_id=? AND variable=? AND valid_at=?
                """,
                (int(obs["site_id"]), str(obs["variable"]), source_valid),
            ).fetchone()
            if lagged is None:
                continue
            bucket = day_ahead(issued_at, str(obs["valid_at"]), str(obs["timezone"]))
            if bucket < 0 or bucket > 7:
                continue
            forecast = float(lagged["value"])
            observed = float(obs["value"])
            variable = str(obs["variable"])
            rain_threshold = (
                float(obs["rain_threshold_mm"]) if variable == "precip" else None
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
                    int(obs["site_id"]),
                    int(feed["id"]),
                    variable,
                    issued_at,
                    str(obs["valid_at"]),
                    lead,
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
