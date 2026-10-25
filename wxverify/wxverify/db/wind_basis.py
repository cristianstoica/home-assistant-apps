"""Wind lane helpers shared by the lane, the monitor and the web pages.

It holds the pair-max station row predicate, the weather.com auth holds that
pause the lane's calls to an endpoint, and the final collection's lead before
local midnight with its count of stations whose final call was missed.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Final, Literal, LiteralString, cast

from wxverify.core.timeutil import isoformat_utc, local_day_slots
from wxverify.db.runtime_state import (
    delete_runtime_state,
    get_runtime_state,
    set_runtime_state,
)

logger = logging.getLogger(__name__)

#: ``source_raw`` prefix of a pair-max station row (9 characters). The old
#: figure always stores ``"<n> km/h"``, so no old row can start with it.
PAIR_MAX_SOURCE_PREFIX: Final = "pair-max "

#: A NULL ``source_raw`` evaluates to NULL here and is excluded.
PAIR_MAX_WIND_SQL: Final[LiteralString] = "(substr(so.source_raw, 1, 9) = 'pair-max ')"

AUTH_HOLD_ENDPOINTS: Final = ("history_all", "all_1day")
AuthHoldEndpoint = Literal["history_all", "all_1day"]
AuthHoldStatus = Literal["held", "probing"]


# --- keys ------------------------------------------------------------------


def auth_hold_key(endpoint: AuthHoldEndpoint) -> str:
    return f"wind_history_auth_hold:{endpoint}"


# --- auth hold -------------------------------------------------------------


def json_object(raw: str | None) -> dict[str, object] | None:
    """Parse a stored JSON object; None when absent, unparseable or not an object."""
    if raw is None:
        return None
    try:
        parsed: object = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    return cast(dict[str, object], parsed)


@dataclass(frozen=True)
class AuthHold:
    endpoint: AuthHoldEndpoint
    status: AuthHoldStatus
    since: str | None
    error: str | None


def read_auth_hold(
    conn: sqlite3.Connection, endpoint: AuthHoldEndpoint
) -> AuthHold | None:
    """The endpoint's hold, or None. An unreadable value counts as ``held``."""
    raw = get_runtime_state(conn, auth_hold_key(endpoint))
    if raw is None:
        return None
    fields = json_object(raw)
    if fields is not None:
        status = fields.get("status")
        since = fields.get("since")
        error = fields.get("error")
        if status in ("held", "probing"):
            return AuthHold(
                endpoint=endpoint,
                status="probing" if status == "probing" else "held",
                since=since if isinstance(since, str) else None,
                error=error if isinstance(error, str) else None,
            )
    logger.warning(
        "unreadable wind history auth hold counts as held endpoint=%s", endpoint
    )
    return AuthHold(endpoint=endpoint, status="held", since=None, error=None)


def write_auth_hold(
    conn: sqlite3.Connection,
    endpoint: AuthHoldEndpoint,
    *,
    status: AuthHoldStatus,
    since: str,
    error: str | None,
) -> None:
    value = {"status": status, "since": since, "error": error}
    set_runtime_state(conn, auth_hold_key(endpoint), json.dumps(value))


def clear_auth_hold(conn: sqlite3.Connection, endpoint: AuthHoldEndpoint) -> None:
    delete_runtime_state(conn, auth_hold_key(endpoint))


def read_auth_holds(conn: sqlite3.Connection) -> dict[AuthHoldEndpoint, AuthHold]:
    holds: dict[AuthHoldEndpoint, AuthHold] = {}
    for endpoint in ("history_all", "all_1day"):
        hold = read_auth_hold(conn, endpoint)
        if hold is not None:
            holds[endpoint] = hold
    return holds


#: Today's final ``all_1day`` call is due this long before the local day ends,
#: before the one-day product resets at local midnight.
WIND_FINAL_LEAD: Final = timedelta(minutes=35)


def count_missed_final(
    conn: sqlite3.Connection, site_id: int, local_date: date, tz_name: str
) -> tuple[int, int]:
    """(missed, total) over the site's enabled stations' rows for local_date.
    Missed = no successful call in [end - WIND_FINAL_LEAD, end). Read-only."""
    _, end, _ = local_day_slots(local_date, tz_name)
    row = conn.execute(
        "SELECT COUNT(*) AS total, COALESCE(SUM(NOT COALESCE("
        " julianday(d.last_ok_at) >= julianday(?)"
        " AND julianday(d.last_ok_at) < julianday(?), 0)), 0) AS missed"
        " FROM station_wind_days d JOIN stations st ON st.id = d.station_id"
        " WHERE st.site_id = ? AND st.enabled = 1 AND d.local_date = ?",
        (
            isoformat_utc(end - WIND_FINAL_LEAD),
            isoformat_utc(end),
            site_id,
            local_date.isoformat(),
        ),
    ).fetchone()
    return int(row["missed"]), int(row["total"])
