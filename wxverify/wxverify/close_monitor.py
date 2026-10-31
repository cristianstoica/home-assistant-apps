"""Read-only health alerts of the daily close (daily-close plan P5, §5.1).

Every query of the close's alerts lives in this module, and none of them
writes. ``wxverify.monitor`` imports this module and turns each
``CloseAlert`` into a pipeline ``Condition``; this module imports nothing
from ``monitor``, so there is no import cycle, and its stamp helper is a copy
of ``monitor._parse_or_none``'s body rather than an import.

Every stamp is parsed before it is compared: comparisons are made on parsed
instants, never on text. ``now`` is passed in by the caller. A value that
does not read makes its item trip, with the detail
``<item>: unreadable <field>`` in place of the alert's message, so reading
never raises; a ``sqlite3.Error`` propagates to the caller, which reports it
as ``db_readable``.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.worker.access_hold import refused_kinds

#: Alert 5's silence threshold, in hours (plan §7.3). Provisional until the
#: owner chooses the value from the review of the poll's reading times.
STATION_SILENT_HOURS: Final[int] = 2

#: While True, alert 5's line says "provisional" (plan §7.3).
STATION_SILENT_PROVISIONAL: Final[bool] = True

#: Alerts 1 and 2 show a done day for this many days after its ``done_at``
#: (plan §5.1, "The alert window").
CLOSE_ALERT_DAYS: Final[int] = 7

# A detail names at most this many items, then "and N more" (plan §5.1).
_DETAIL_MAX_ITEMS: Final = 5

_STATION_SILENT_ID: Final = "station_silent_today"


@dataclass(frozen=True)
class CloseAlert:
    """One close alert's result, as ``monitor`` turns it into a ``Condition``.

    ``count`` is the number of items tripped (0 while ok); ``detail`` names
    them in a fixed order, at most five then "and N more", and is None while
    ok. ``recovered_at`` is alert 7's recovery time, None for every other
    alert.
    """

    id: str
    tripped: bool
    count: int
    detail: str | None = None
    recovered_at: datetime | None = None


def _parse_or_none(value: object) -> datetime | None:
    """``parse_utc(str(value))``, or ``None`` for NULL or a value that fails.

    ``str()`` first, so a non-text value such as an imported BLOB gives
    ``None``; ``parse_utc`` raises only ``ValueError``.
    """
    if value is None:
        return None
    try:
        return parse_utc(str(value))
    except ValueError:
        return None


def _items_detail(items: list[str]) -> str | None:
    """``items`` joined by "; ", at most ``_DETAIL_MAX_ITEMS``, then
    "and N more"; None when there are none."""
    if not items:
        return None
    shown = items[:_DETAIL_MAX_ITEMS]
    if len(items) > _DETAIL_MAX_ITEMS:
        shown.append(f"and {len(items) - _DETAIL_MAX_ITEMS} more")
    return "; ".join(shown)


def _age_text(age: timedelta) -> str:
    """``age`` in whole minutes, as "2 h", "2 h 15 min" or "45 min"."""
    minutes = max(0, int(age.total_seconds() // 60))
    hours, rest = divmod(minutes, 60)
    if hours and rest:
        return f"{hours} h {rest} min"
    if hours:
        return f"{hours} h"
    return f"{rest} min"


# Alert 5's judged stations (plan §6.3): enabled stations of enabled sites
# whose latest written poll answer was about the station (online or
# offline), in the detail's order (site name, then station).
_STATION_SILENT_SQL: Final = """
    SELECT st.pws_station_id, ps.health_state, ps.last_obstime, ps.last_poll_at
    FROM stations st
    JOIN sites s ON s.id = st.site_id
    JOIN station_poll_state ps ON ps.station_id = st.id
    WHERE s.enabled = 1 AND st.enabled = 1
      AND ps.health_state IN ('online', 'offline')
    ORDER BY s.name, s.id, st.pws_station_id, st.id
"""


def _silent_line(
    station: str,
    health: object,
    raw_obstime: object,
    raw_poll_at: object,
    now: datetime,
) -> str | None:
    """One judged station's alert 5 line, or None when it is not silent.

    The age is measured from the reading's own observation time: for
    ``online``, ``last_poll_at`` minus ``last_obstime`` (the age the poll saw
    at its latest answer, so a back-off never grows it); for ``offline``,
    ``now`` minus ``last_obstime``. It trips at ``STATION_SILENT_HOURS`` or
    more, or when a stamp it needs is NULL or does not read.
    """
    item = f"station {station}"
    if raw_obstime is None:
        return f"{item}: no observation yet"
    obstime = _parse_or_none(raw_obstime)
    if obstime is None:
        return f"{item}: unreadable last_obstime"
    threshold = timedelta(hours=STATION_SILENT_HOURS)
    if health == "offline":
        age = now - obstime
        if age < threshold:
            return None
        return (
            f"{item}: weather.com reports no current data;"
            f" newest observation {_age_text(age)} old"
        )
    polled = _parse_or_none(raw_poll_at)
    if polled is None:
        return f"{item}: unreadable last_poll_at"
    age = polled - obstime
    if age < threshold:
        return None
    mark = ", provisional" if STATION_SILENT_PROVISIONAL else ""
    return (
        f"{item}: newest observation {_age_text(age)} old at"
        f" {isoformat_utc(polled)} (threshold {STATION_SILENT_HOURS} h{mark})"
    )


def station_silent(
    conn: sqlite3.Connection, now: datetime, *, close_on: bool
) -> CloseAlert:
    """Alert 5, ``station_silent_today``: stations whose poll sees them silent.

    Judges each enabled station of an enabled site from its
    ``station_poll_state`` row, ``online`` and ``offline`` only: ``transient``
    and ``terminal`` are failures, not silence. While ``current_obs`` is
    among ``refused_kinds`` no station is judged, since the refused key is
    alert 6's. Never reads the full-day readings (plan §6.3, K3). Called with
    the close on and off; ``close_on`` is the switch as the caller read it.
    Read-only.
    """
    if any(record.kind == "current_obs" for record in refused_kinds(conn, now)):
        return CloseAlert(id=_STATION_SILENT_ID, tripped=False, count=0)
    lines: list[str] = []
    for row in conn.execute(_STATION_SILENT_SQL):
        line = _silent_line(str(row[0]), row[1], row[2], row[3], now)
        if line is not None:
            lines.append(line)
    # Step 4 piece 2 adds the close-on leave-out here (plan §6.3): with
    # ``close_on``, a station alert 3 names for every variable is not listed.
    return CloseAlert(
        id=_STATION_SILENT_ID,
        tripped=bool(lines),
        count=len(lines),
        detail=_items_detail(lines),
    )
