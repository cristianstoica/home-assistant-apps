"""Weather.com refused-access hold, one per download kind.

A 401 or 403 from weather.com holds the kind of download that was refused, for
every station, for ``ACCESS_HOLD_SECONDS``; the hold then ends by itself. Each
kind has its own ``runtime_state`` row, so a refusal of one kind never holds
another, and a 2xx reply of a kind clears only that kind's row. The row stays
after the hold has ended until that 2xx deletes it, so a reader can tell
"refused and not yet answered" from "working" (``refused_kinds``).
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, Literal

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.runtime_state import (
    delete_runtime_state,
    get_runtime_state,
    set_runtime_state,
)
from wxverify.db.wind_basis import json_object

logger = logging.getLogger(__name__)

AccessKind = Literal["current_obs", "hourly", "five_minute"]

#: Every kind, in the order readers list them.
ACCESS_KINDS: Final[tuple[AccessKind, ...]] = ("current_obs", "hourly", "five_minute")

#: Each kind's plain name, as the alert and the Ops page show it.
ACCESS_KIND_LABELS: Final[dict[AccessKind, str]] = {
    "current_obs": "current conditions",
    "hourly": "hourly readings",
    "five_minute": "5-minute readings",
}

#: How long a refusal holds its kind, counted from the latest refusal.
ACCESS_HOLD_SECONDS: Final = 3600

# A row whose ``until`` is this much older than now no longer counts in
# ``refused_kinds``, so a kind that is never tried again (every station
# switched off, say) cannot keep the alert up for ever. The row itself stays
# for the next 2xx reply of its kind to delete.
_REFUSED_LISTED_FOR: Final = timedelta(hours=24)


class AccessHeld(Exception):
    """A try of ``kind`` met its refused-access hold, in force until ``until``.

    Deliberately not a ``JobControl``: each caller catches it and decides what
    a held try means for its job, so it can never escape as a job outcome by
    accident.
    """

    def __init__(self, kind: AccessKind, until: str) -> None:
        super().__init__(kind, until)
        self.kind = kind
        self.until = until


@dataclass(frozen=True)
class AccessRecord:
    """One kind's stored hold. ``until`` is None when the row is unreadable."""

    kind: AccessKind
    since: str | None
    until: str | None
    status: int | None
    message: str | None


# --- keys ------------------------------------------------------------------


def access_hold_key(kind: AccessKind) -> str:
    """The ``runtime_state`` key of ``kind``'s hold."""
    return f"weathercom_access_hold:{kind}"


# --- reads -----------------------------------------------------------------


def _stamp_or_none(value: object) -> str | None:
    """``value`` when it is a string ``parse_utc`` reads, else None."""
    if not isinstance(value, str):
        return None
    try:
        parse_utc(value)
    except ValueError:
        return None
    return value


def read_access_record(
    conn: sqlite3.Connection, kind: AccessKind
) -> AccessRecord | None:
    """``kind``'s stored hold, or None when it has no row.

    A value that is not a JSON object, or whose ``until`` does not parse, is
    returned with ``until`` None and logged at WARNING. ``since`` is likewise
    None unless it parses, and ``status`` and ``message`` unless they have
    their stored types.
    """
    raw = get_runtime_state(conn, access_hold_key(kind))
    if raw is None:
        return None
    fields = json_object(raw)
    if fields is None:
        fields = {}
    until = _stamp_or_none(fields.get("until"))
    if until is None:
        logger.warning("unreadable weather.com access hold kind=%s", kind)
    status = fields.get("status")
    message = fields.get("message")
    return AccessRecord(
        kind=kind,
        since=_stamp_or_none(fields.get("since")),
        until=until,
        status=(
            status if isinstance(status, int) and not isinstance(status, bool) else None
        ),
        message=message if isinstance(message, str) else None,
    )


def access_held(
    conn: sqlite3.Connection, kind: AccessKind, now: datetime
) -> str | None:
    """The stored ``until`` text while ``now < until``, else None.

    An unreadable ``until`` gives None. Failing OPEN is self-healing, as in
    ``check_domain_backoff``: the next try makes its call, and either it
    succeeds and its 2xx deletes the row, or it is refused and
    ``record_access_refused`` rewrites the row in readable form. Failing
    closed would hold the kind for good, because no call could clear it.
    """
    record = read_access_record(conn, kind)
    if record is None or record.until is None:
        return None
    if now < parse_utc(record.until):
        return record.until
    return None


def check_access_hold(conn: sqlite3.Connection, kind: AccessKind) -> None:
    """Raise ``AccessHeld`` while ``kind``'s hold is in force.

    Writes nothing, so a held try never extends or touches the hold.
    """
    until = access_held(conn, kind, utc_now())
    if until is not None:
        raise AccessHeld(kind, until)


def refused_kinds(conn: sqlite3.Connection, now: datetime) -> list[AccessRecord]:
    """Every kind whose latest answer was a refusal, in ``ACCESS_KINDS`` order.

    Lists each kind whose row exists and whose ``until`` is unreadable or
    later than ``now`` minus 24 h, whether or not its hold is still in force:
    between a hold's end and the next refused try the row still says the last
    answer was a refusal. Read-only.
    """
    cutoff = now - _REFUSED_LISTED_FOR
    listed: list[AccessRecord] = []
    for kind in ACCESS_KINDS:
        record = read_access_record(conn, kind)
        if record is None:
            continue
        if record.until is None or parse_utc(record.until) > cutoff:
            listed.append(record)
    return listed


# --- writes ----------------------------------------------------------------


def record_access_refused(
    conn: sqlite3.Connection, kind: AccessKind, status: int
) -> str:
    """Hold ``kind`` after a refused (401/403) call; return the new ``until``.

    ``until`` is now plus ``ACCESS_HOLD_SECONDS``. ``since`` is kept from an
    existing readable row, the first refusal of the current run, else set to
    now. ``status`` and ``message`` come from this refusal.
    """
    now = utc_now().replace(microsecond=0)
    until = isoformat_utc(now + timedelta(seconds=ACCESS_HOLD_SECONDS))
    existing = read_access_record(conn, kind)
    since = (
        existing.since
        if existing is not None
        and existing.until is not None
        and existing.since is not None
        else isoformat_utc(now)
    )
    value = {
        "since": since,
        "until": until,
        "status": status,
        "message": f"access refused (HTTP {status})",
    }
    set_runtime_state(conn, access_hold_key(kind), json.dumps(value))
    logger.warning(
        "weather.com access refused kind=%s status=%d held until %s",
        kind,
        status,
        until,
    )
    return until


def clear_access_refused(conn: sqlite3.Connection, kind: AccessKind) -> None:
    """Delete ``kind``'s row: a 2xx reply of that kind was granted."""
    delete_runtime_state(conn, access_hold_key(kind))
