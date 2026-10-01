"""The wind-history lane's share of the weather.com daily budget.

Backfill (the rebuild's past station-days) spends only the headroom left
under the daily cap after today's spend, the live demand still expected
before the billing day ends, and a fixed reserve. The effective cap in
``reserve_budget`` stays the hard stop for all traffic; this headroom only
paces backfill underneath it.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from datetime import UTC, date, datetime, timedelta
from typing import Final, Literal, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify.collection.budget import effective_daily_call_limit
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.settings.keys import get_number_setting

logger = logging.getLogger(__name__)

LIVE_RESERVE_CALLS: Final = 200

WIND_BACKFILL_CALLS_KEY: Final = "wind_backfill_calls"
WIND_LIVE_CALLS_KEY: Final = "wind_live_calls"
LaneCounterKey = Literal["wind_backfill_calls", "wind_live_calls"]

_SECONDS_PER_DAY: Final = 86400


def lane_live_calls(conn: sqlite3.Connection) -> int:
    """Enabled stations on enabled sites * (ceil(1440 / obs_interval) + 1)."""
    interval = get_number_setting(conn, "obs_interval_minutes", 180, minimum=30)
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM stations st JOIN sites s ON s.id = st.site_id
        WHERE st.enabled = 1 AND s.enabled = 1
        """
    ).fetchone()
    stations = int(row["n"]) if row is not None else 0
    return stations * (math.ceil(1440 / interval) + 1)


def _billing_clock(now: datetime, tz: ZoneInfo) -> tuple[date, float]:
    """The billing day of ``now`` and the fraction of it still to run (0..1)."""
    local = now.astimezone(tz)
    today = local.date()
    tomorrow = today + timedelta(days=1)
    midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, tzinfo=tz)
    # Subtract in UTC: two datetimes sharing one tzinfo subtract as wall time.
    remaining = (midnight.astimezone(UTC) - now.astimezone(UTC)).total_seconds()
    return today, min(1.0, max(0.0, remaining / _SECONDS_PER_DAY))


def backfill_headroom(conn: sqlite3.Connection, *, now: datetime) -> int | None:
    """cap - spent_today - reserved_live - LIVE_RESERVE_CALLS, where
    cap = effective_daily_call_limit("weathercom", daily_call_limit) and
    reserved_live = ceil((L_prev + lane_live_calls(conn))
                         * clamp((next_billing_window - now) / 86400, 0, 1)).

    ``L_prev`` is the previous billing day's calls less the lane's own
    counters for that day. None (no backfill) when there is no weathercom
    ``sources`` row, the billing time zone does not load, or the previous
    billing day has no ``api_budget`` row. The billing day and the window are
    taken from ``now``.
    """
    source_row = conn.execute(
        "SELECT daily_call_limit AS stored_limit, billing_tz"
        " FROM sources WHERE source = 'weathercom'"
    ).fetchone()
    if source_row is None:
        return None
    try:
        tz = ZoneInfo(str(source_row["billing_tz"]))
    except (ZoneInfoNotFoundError, ValueError):
        return None
    cap = effective_daily_call_limit("weathercom", int(source_row["stored_limit"]))
    today, remaining_fraction = _billing_clock(now, tz)
    previous = (today - timedelta(days=1)).isoformat()
    previous_row = conn.execute(
        "SELECT calls FROM api_budget WHERE source = 'weathercom' AND billing_day = ?",
        (previous,),
    ).fetchone()
    if previous_row is None:
        return None
    today_row = conn.execute(
        "SELECT calls FROM api_budget WHERE source = 'weathercom' AND billing_day = ?",
        (today.isoformat(),),
    ).fetchone()
    spent_today = 0 if today_row is None else int(today_row["calls"])
    backfill = read_lane_counter(conn, WIND_BACKFILL_CALLS_KEY).get(previous, 0)
    live = read_lane_counter(conn, WIND_LIVE_CALLS_KEY).get(previous, 0)
    live_prev = max(0, int(previous_row["calls"]) - backfill - live)
    reserved_live = math.ceil((live_prev + lane_live_calls(conn)) * remaining_fraction)
    return cap - spent_today - reserved_live - LIVE_RESERVE_CALLS


def _parse_counter(raw: str | None) -> dict[str, int] | None:
    """``{billing_day: n}``; None when the stored value is unreadable."""
    if raw is None:
        return {}
    try:
        parsed: object = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    counter: dict[str, int] = {}
    for key, value in cast(dict[object, object], parsed).items():
        if not isinstance(key, str) or isinstance(value, bool):
            return None
        if not isinstance(value, int):
            return None
        try:
            date.fromisoformat(key)
        except ValueError:
            return None
        counter[key] = value
    return counter


def read_lane_counter(conn: sqlite3.Connection, key: LaneCounterKey) -> dict[str, int]:
    """The counter's entries; an unreadable value reads as ``{}`` with a WARNING."""
    counter = _parse_counter(get_runtime_state(conn, key))
    if counter is None:
        logger.warning("unreadable wind lane counter read as empty key=%s", key)
        return {}
    return counter


def add_lane_calls(
    conn: sqlite3.Connection, key: LaneCounterKey, billing_day: str, delta: int
) -> None:
    """Add ``delta`` (negative for a refund) to one billing day, floored at 0.

    Keeps only the two newest billing days. An unreadable value is reset to
    ``{}`` with a WARNING before the update.
    """
    counter = _parse_counter(get_runtime_state(conn, key))
    if counter is None:
        logger.warning("unreadable wind lane counter reset key=%s", key)
        counter = {}
    counter[billing_day] = max(0, counter.get(billing_day, 0) + delta)
    kept = sorted(counter)[-2:]
    set_runtime_state(conn, key, json.dumps({day: counter[day] for day in kept}))


def lane_counter_key(*, exempt: bool) -> LaneCounterKey:
    """``wind_live_calls`` for an exempt reservation, else ``wind_backfill_calls``."""
    return WIND_LIVE_CALLS_KEY if exempt else WIND_BACKFILL_CALLS_KEY
