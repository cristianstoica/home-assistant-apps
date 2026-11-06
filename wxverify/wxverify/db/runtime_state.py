"""Persisted runtime health state."""

from __future__ import annotations

import sqlite3
from typing import NamedTuple

RUNTIME_STATE_KEYS = (
    "worker_started_at",
    "worker_last_loop_at",
    "scheduler_last_tick_at",
    "current_obs_poller_last_loop_at",
)

RUNTIME_HEARTBEAT_INTERVAL_SECONDS = 60.0


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


def get_runtime_state(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute(
        "SELECT value FROM runtime_state WHERE key = ?", (key,)
    ).fetchone()
    return None if row is None else str(row["value"])


class RuntimeStateEntry(NamedTuple):
    """A runtime_state row's value alongside its own last-write timestamp."""

    value: str
    updated_at: str


def get_runtime_state_entry(
    conn: sqlite3.Connection, key: str
) -> RuntimeStateEntry | None:
    row = conn.execute(
        "SELECT value, updated_at FROM runtime_state WHERE key = ?", (key,)
    ).fetchone()
    if row is None:
        return None
    return RuntimeStateEntry(value=str(row["value"]), updated_at=str(row["updated_at"]))


def delete_runtime_state(conn: sqlite3.Connection, *keys: str) -> None:
    placeholders = ", ".join("?" for _ in keys)
    conn.execute(f"DELETE FROM runtime_state WHERE key IN ({placeholders})", keys)


def runtime_status(conn: sqlite3.Connection) -> dict[str, str | None]:
    values: dict[str, str | None] = {key: None for key in RUNTIME_STATE_KEYS}
    placeholders = ", ".join("?" for _ in RUNTIME_STATE_KEYS)
    rows = conn.execute(
        f"SELECT key, value FROM runtime_state WHERE key IN ({placeholders})",
        RUNTIME_STATE_KEYS,
    ).fetchall()
    for row in rows:
        values[str(row["key"])] = str(row["value"])
    return values
