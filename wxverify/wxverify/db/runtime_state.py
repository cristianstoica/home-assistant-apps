"""Persisted runtime health state."""

from __future__ import annotations

import sqlite3

RUNTIME_STATE_KEYS = (
    "worker_started_at",
    "worker_last_loop_at",
    "scheduler_last_tick_at",
)


def set_runtime_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO runtime_state(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value=excluded.value,
            updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
        """,
        (key, value),
    )


def set_runtime_state_now(conn: sqlite3.Connection, key: str) -> None:
    conn.execute(
        """
        INSERT INTO runtime_state(key, value)
        VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        ON CONFLICT(key) DO UPDATE SET
            value=excluded.value,
            updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
        """,
        (key,),
    )


def runtime_status(conn: sqlite3.Connection) -> dict[str, str | None]:
    values: dict[str, str | None] = {key: None for key in RUNTIME_STATE_KEYS}
    rows = conn.execute(
        """
        SELECT key, value
        FROM runtime_state
        WHERE key IN ('worker_started_at', 'worker_last_loop_at',
                      'scheduler_last_tick_at')
        """
    ).fetchall()
    for row in rows:
        values[str(row["key"])] = str(row["value"])
    return values
