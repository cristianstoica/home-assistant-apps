"""The daily close's night chain: the close of each site-day and its logs.

This module will hold the chain's constants, the bodies of its eight
stages (``day_close`` stages 2 to 6, ``day_publish`` stages 7 and 8) with
the hand-off rule, ``fail_attempt`` and ``fail_publication``,
``run_close_retention``, ``enqueue_due_close_work``,
``settle_interrupted_closes``, and the read-only ``close_status`` and lists
that P5 and P6 read. For now it holds only ``record_collection_try``, the
writer of the collection try log.

``worker/wind_days.py`` imports ``record_collection_try`` at load time, so
this module must never import ``wind_days`` at load time, directly or
through another module.
"""

from __future__ import annotations

import sqlite3


def record_collection_try(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    station_id: int,
    local_date: str,
    kind: str,
    slot: str,
    close_attempt: int | None,
    tried_at: str,
    outcome: str,
    reason: str | None,
    detail: str | None,
    readings_kept: int,
    readings_refused: int,
) -> None:
    """Insert one ``close_try_log`` row for a weather.com collection try.

    One row per try of a station's site-day: P2's daytime and evening
    5-minute tries (``kind`` ``five_minute``, ``slot`` ``daytime`` or the
    evening slot as local ``HH:MM``, ``close_attempt`` None) and the close's
    gap fill (``kind`` ``hourly``, ``slot`` ``gap_fill``, ``close_attempt``
    1 or 2). ``local_date`` is the day as ``YYYY-MM-DD`` and ``tried_at``
    when the try ended, in ``isoformat_utc_micro``'s form. ``outcome``,
    ``reason`` and ``detail`` are stored as given; ``readings_kept`` counts
    the readings stored and ``readings_refused`` those the closed-day guard
    refused.

    Does one INSERT and nothing else: it neither commits nor reads whether
    the close is on, so the log is stored from install, and it runs in the
    caller's transaction. A row that breaks a CHECK of the table raises
    ``sqlite3.IntegrityError`` unchanged.
    """
    conn.execute(
        "INSERT INTO close_try_log (site_id, local_date, station_id, kind,"
        " slot, close_attempt, tried_at, outcome, reason, detail,"
        " readings_kept, readings_refused)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            site_id,
            local_date,
            station_id,
            kind,
            slot,
            close_attempt,
            tried_at,
            outcome,
            reason,
            detail,
            readings_kept,
            readings_refused,
        ),
    )
