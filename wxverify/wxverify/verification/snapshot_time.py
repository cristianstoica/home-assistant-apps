"""The snapshot wall-clock time T: its settings key and its resolvers.

The settings route (``wxverify.api.routes.settings``) writes the time, and
alert 4 (``publication_late``, ``wxverify.close_monitor``) reads it as its
deadline. The stored key keeps its ``record_`` prefix.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from wxverify.settings.keys import get_setting
from wxverify.verification.methodology import SNAPSHOT_LOCAL_TIME

#: Global settings key for the snapshot wall-clock time; a per-site override
#: lives at ``record_snapshot_local_time:<site_id>`` (§15).
SNAPSHOT_TIME_KEY = "record_snapshot_local_time"


def parse_wall_clock(raw: str) -> tuple[int, int] | None:
    parts = raw.strip().split(":")
    if len(parts) != 2:
        return None
    try:
        hour, minute = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return hour, minute
    return None


def snapshot_wall_clock(conn: sqlite3.Connection, site_id: int) -> str:
    """The site's snapshot wall-clock time ``HH:MM`` (§15 resolution order).

    Per-site setting, then the global setting, then the methodology default.
    Unparseable values fall through to the next layer rather than erroring —
    a foreign settings row must not stop alert 4.
    """
    for key in (f"{SNAPSHOT_TIME_KEY}:{site_id}", SNAPSHOT_TIME_KEY):
        raw = get_setting(conn, key)
        if raw is not None and parse_wall_clock(raw) is not None:
            return raw.strip()
    return SNAPSHOT_LOCAL_TIME


def resolve_snapshot_utc(timezone: str, local_date: date, wall_clock: str) -> datetime:
    """UTC instant of T: first instant at/after ``wall_clock`` on the local day.

    PEP 495 semantics with ``fold=0``: an ambiguous fall-back time maps to
    its first occurrence, exactly the §3 rule. A wall-clock time inside a
    spring-forward gap maps to the gap-SHIFTED instant (02:30 -> 03:30), an
    architect-accepted approximation of §3's first-instant-after-the-gap
    (03:00) — within the hour, deterministic, and identical for the default
    07:00 snapshot time, which no real-world DST gap touches.
    """
    parsed = parse_wall_clock(wall_clock)
    if parsed is None:
        raise ValueError(f"invalid snapshot wall clock {wall_clock!r}")
    hour, minute = parsed
    local = datetime(
        local_date.year,
        local_date.month,
        local_date.day,
        hour,
        minute,
        tzinfo=ZoneInfo(timezone),
    )
    return local.astimezone(UTC)
