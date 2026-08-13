"""Settings API: forecast-of-record snapshot wall-clock time (§15).

Sets or clears ``record_snapshot_local_time`` — the global default or a
per-site override (key ``record_snapshot_local_time:<site_id>``). The
record builder resolves per-site first, then global, then the
methodology default, skipping unparseable values, so writes here are
validated with the same parser the reader uses.
"""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter
from pydantic import BaseModel

from wxverify.api.errors import ApiError
from wxverify.db.connection import get_db
from wxverify.settings.keys import set_setting
from wxverify.verification.record import SNAPSHOT_TIME_KEY, parse_wall_clock

router = APIRouter(prefix="/api/settings", tags=["settings"])


class SnapshotTimeIn(BaseModel):
    """Payload for setting the record snapshot wall-clock time."""

    time: str
    site_id: int | None = None


def _settings_key(conn: sqlite3.Connection, site_id: int | None) -> str:
    if site_id is None:
        return SNAPSHOT_TIME_KEY
    row = conn.execute("SELECT id FROM sites WHERE id = ?", (site_id,)).fetchone()
    if row is None:
        raise ApiError(404, "site not found")
    return f"{SNAPSHOT_TIME_KEY}:{site_id}"


@router.put("/record-snapshot-time")
async def set_record_snapshot_time(body: SnapshotTimeIn) -> dict[str, object]:
    parsed = parse_wall_clock(body.time)
    if parsed is None:
        raise ApiError(400, "time must be HH:MM (00:00-23:59)")
    hour, minute = parsed
    canonical = f"{hour:02d}:{minute:02d}"

    def _write(conn: sqlite3.Connection) -> dict[str, object]:
        key = _settings_key(conn, body.site_id)
        set_setting(conn, key, canonical)
        return {"key": key, "time": canonical}

    return await get_db().write(_write)


@router.delete("/record-snapshot-time")
async def clear_record_snapshot_time(site: int | None = None) -> dict[str, object]:
    def _write(conn: sqlite3.Connection) -> dict[str, object]:
        key = _settings_key(conn, site)
        conn.execute("DELETE FROM settings WHERE key = ?", (key,))
        return {"key": key, "cleared": True}

    return await get_db().write(_write)
