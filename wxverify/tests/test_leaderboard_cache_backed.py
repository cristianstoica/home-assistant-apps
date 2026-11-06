"""Shared harness for the leaderboard cache-backed read path.

The close-off leaderboard cache-status tests (T1-T8) that used to live here
tested ``leaderboard_with_status``/``_cached_leaderboard`` in
``wxverify/scoring/leaderboard.py``, which is reached only when the daily
close is off -- never on the owner's only install (close on, 0.16.14). They
were removed; this module now holds only the fixture-construction helpers
that ``tests/test_leaderboard_read_snapshot.py``, ``tests/test_close_triggers.py``
and ``tests/test_forecast_snapshot_read.py`` still import.

Synthetic fixtures only -- fake site names and the repo's existing 40/-105
lat-lon convention, no real keys or station IDs.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, init_db

# ---------------------------------------------------------------------------
# Harness (mirrors tests/test_composite_cache_backed.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    """Drop-in run_worker shim that idles without touching the scheduler."""
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    # Real (empty-object) options file, not a missing path -- see module
    # docstring; a missing path falls back to ambient WXV_* env vars and
    # clobbers DB-seeded settings on the next lifespan startup.
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


def _start_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Bootstrap a fresh tmp DB + idle-worker app for HTTP-level tests.

    Seeding happens AFTER entering ``TestClient`` (via ``get_db().write_sync``)
    so it runs against the exact instance the running app uses -- the
    lifespan's own ``init_db`` call would otherwise close/replace a
    pre-built connection out from under a test.
    """
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _make_site(conn: sqlite3.Connection, name: str, *, enabled: int = 1) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', ?)
            """,
            (name, enabled),
        ).lastrowid
    )


def _open_meteo_feed_ids(conn: sqlite3.Connection, count: int) -> list[int]:
    return [
        int(row["id"])
        for row in conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' ORDER BY id LIMIT ?",
            (count,),
        )
    ]


__all__ = [
    "_init_tmp_db",
    "_make_site",
    "_open_meteo_feed_ids",
    "_start_app",
]
