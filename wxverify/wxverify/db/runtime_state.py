"""Persisted runtime health state."""

from __future__ import annotations

import sqlite3

RUNTIME_STATE_KEYS = (
    "worker_started_at",
    "worker_last_loop_at",
    "scheduler_last_tick_at",
    "import_rebuild_done_at",
    "import_rebuild_state",
    "import_rebuild_error",
)

# Mirrors create_schema()'s runtime_state DDL in db/migrations.py -- update
# both if this table's shape ever changes. Needed here because a caller may
# write to this table before migrations have run against it (import
# staging, ahead of promotion); once migrations have run, create_schema has
# always already ensured it.
_RUNTIME_STATE_DDL = """
CREATE TABLE IF NOT EXISTS runtime_state (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
)
"""


def ensure_runtime_state_table(conn: sqlite3.Connection) -> None:
    conn.execute(_RUNTIME_STATE_DDL)


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
