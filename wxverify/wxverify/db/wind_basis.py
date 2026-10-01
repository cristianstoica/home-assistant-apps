"""Per-site wind basis state: the staged move from the old station wind figure
to ``max_adjacent_pair_mean_wind``.

Each site moves forward only, ``staging -> switching -> rescoring ->
pair_max``. The state lives in ``runtime_state`` under ``wind_basis:<site>``;
an absent key means ``pair_max``. Only :func:`init_wind_basis`, which runs at
every database open, may move a site backwards, and only when the stored wind
rows prove the stored state wrong.

The SQL builders here gate every wind writer and reader on that one state, so
no observation, pair, score or trust figure mixes the two figures. Every
builder takes its column arguments from literals at the call site, never from
input.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Final, Literal, LiteralString, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify.core.timeutil import utc_now
from wxverify.db.runtime_state import (
    delete_runtime_state,
    get_runtime_state,
    set_runtime_state,
    set_runtime_state_now,
)

logger = logging.getLogger(__name__)

STATES: Final = ("staging", "switching", "rescoring", "pair_max")
#: The states in which the scoring readers see wind.
OPEN_STATES: Final = frozenset({"staging", "rescoring", "pair_max"})
#: The one legal move out of each state. Everything else is refused.
_FORWARD: Final[dict[str, str]] = {
    "staging": "switching",
    "switching": "rescoring",
    "rescoring": "pair_max",
}

#: ``source_raw`` prefix of a pair-max station row (9 characters). The old
#: figure always stores ``"<n> km/h"``, so no old row can start with it.
PAIR_MAX_SOURCE_PREFIX: Final = "pair-max "

#: Any row not provably pair-max is old. Alias ``so`` = station_observations.
LEGACY_WIND_SQL: Final[LiteralString] = (
    "(so.source_raw IS NULL OR substr(so.source_raw, 1, 9) != 'pair-max ')"
)
#: A NULL ``source_raw`` evaluates to NULL here and is excluded.
PAIR_MAX_WIND_SQL: Final[LiteralString] = "(substr(so.source_raw, 1, 9) = 'pair-max ')"

AUTH_HOLD_ENDPOINTS: Final = ("history_all", "all_1day")
AuthHoldEndpoint = Literal["history_all", "all_1day"]
AuthHoldStatus = Literal["held", "probing"]


def wind_state_sql(site_col: LiteralString) -> LiteralString:
    """The stored state of the site in ``site_col``; absent reads ``pair_max``."""
    return (
        "COALESCE((SELECT value FROM runtime_state"
        f" WHERE key = 'wind_basis:' || {site_col}), 'pair_max')"
    )


def wind_open_clause(
    variable_col: LiteralString, site_col: LiteralString
) -> LiteralString:
    """True for every non-wind row, and for wind only in an open state."""
    return (
        f"({variable_col} != 'wind' OR {wind_state_sql(site_col)}"
        " IN ('staging','rescoring','pair_max'))"
    )


def wind_station_row_clause(site_col: LiteralString) -> LiteralString:
    """Admit the one basis of the site's state; an unknown state admits no wind.

    The station_observations alias must be ``so``.
    """
    return (
        f"(so.variable != 'wind' OR CASE {wind_state_sql(site_col)}"
        f" WHEN 'staging' THEN {LEGACY_WIND_SQL}"
        f" WHEN 'switching' THEN {PAIR_MAX_WIND_SQL}"
        f" WHEN 'rescoring' THEN {PAIR_MAX_WIND_SQL}"
        f" WHEN 'pair_max' THEN {PAIR_MAX_WIND_SQL}"
        " ELSE 0 END)"
    )


# --- keys ------------------------------------------------------------------


def wind_basis_key(site_id: int) -> str:
    return f"wind_basis:{site_id}"


def wind_progress_key(site_id: int) -> str:
    return f"wind_basis_progress_at:{site_id}"


def wind_cursor_key(site_id: int) -> str:
    return f"wind_basis_cursor:{site_id}"


def wind_blocked_key(site_id: int) -> str:
    return f"wind_basis_blocked:{site_id}"


def wind_report_key(site_id: int) -> str:
    return f"wind_rebuild_report:{site_id}"


def wind_done_at_key(site_id: int) -> str:
    return f"wind_rebuild_done_at:{site_id}"


def auth_hold_key(endpoint: AuthHoldEndpoint) -> str:
    return f"wind_history_auth_hold:{endpoint}"


# --- state -----------------------------------------------------------------


def wind_basis_state(conn: sqlite3.Connection, site_id: int) -> str:
    """The stored state; absent -> ``pair_max``; any other value is returned raw."""
    value = get_runtime_state(conn, wind_basis_key(site_id))
    return "pair_max" if value is None else value


def wind_serving_mode(state: str) -> Literal["legacy", "weighted", "closed"]:
    """Allowlist map: ``staging`` -> legacy, ``pair_max`` -> weighted, else closed."""
    if state == "staging":
        return "legacy"
    if state == "pair_max":
        return "weighted"
    return "closed"


def stamp_wind_progress(conn: sqlite3.Connection, site_id: int) -> None:
    set_runtime_state_now(conn, wind_progress_key(site_id))


def transition_wind_basis(
    conn: sqlite3.Connection, site_id: int, *, expected: str, new: str
) -> bool:
    """Compare-and-set one forward move; False unless the stored value is ``expected``.

    Only ``staging->switching``, ``switching->rescoring`` and
    ``rescoring->pair_max`` are legal; any other pair raises ``ValueError``.
    A successful move stamps progress.
    """
    if _FORWARD.get(expected) != new:
        raise ValueError(f"illegal wind basis transition {expected!r} -> {new!r}")
    cursor = conn.execute(
        "UPDATE runtime_state SET value = ?,"
        " updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')"
        " WHERE key = ? AND value = ?",
        (new, wind_basis_key(site_id), expected),
    )
    if cursor.rowcount != 1:
        return False
    stamp_wind_progress(conn, site_id)
    return True


def set_wind_basis_state(conn: sqlite3.Connection, site_id: int, state: str) -> None:
    """Unconditional setter, for :func:`init_wind_basis` and unknown-value recovery."""
    if state not in STATES:
        raise ValueError(f"unknown wind basis state {state!r}")
    set_runtime_state(conn, wind_basis_key(site_id), state)
    stamp_wind_progress(conn, site_id)


def _has_legacy_wind(conn: sqlite3.Connection, site_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM station_observations so"
        " JOIN stations s ON s.id = so.station_id"
        " WHERE s.site_id = ? AND so.variable = 'wind'"
        f" AND {LEGACY_WIND_SQL} LIMIT 1",
        (site_id,),
    ).fetchone()
    return row is not None


def init_wind_basis(conn: sqlite3.Connection) -> None:
    """Land every site (enabled or not) in the state its stored wind rows prove.

    Runs at every open, right after the settings seed. A savepoint keeps each
    database's writes together: ``executescript`` in the migration has already
    committed the outer transaction by the time this runs.
    """
    conn.execute("SAVEPOINT init_wind_basis")
    try:
        site_ids = [
            int(row["id"]) for row in conn.execute("SELECT id FROM sites ORDER BY id")
        ]
        for site_id in site_ids:
            _init_site(conn, site_id)
    except BaseException:
        conn.execute("ROLLBACK TO init_wind_basis")
        conn.execute("RELEASE init_wind_basis")
        raise
    conn.execute("RELEASE init_wind_basis")


def _init_site(conn: sqlite3.Connection, site_id: int) -> None:
    stored = get_runtime_state(conn, wind_basis_key(site_id))
    if stored == "staging":
        return
    if stored is not None and stored not in STATES:
        logger.warning(
            "wind basis unknown value reset to staging site=%s value=%r",
            site_id,
            stored,
        )
        set_wind_basis_state(conn, site_id, "staging")
        return
    legacy = _has_legacy_wind(conn, site_id)
    if stored is None:
        set_wind_basis_state(conn, site_id, "staging" if legacy else "pair_max")
        return
    if not legacy:
        return
    if stored in ("switching", "rescoring"):
        logger.info(
            "wind basis switch restarted: old wind rows found site=%s state=%s",
            site_id,
            stored,
        )
        set_wind_basis_state(conn, site_id, "switching")
        write_wind_cursor(conn, site_id, phase="purge", cursor_date=None)
        return
    # stored == "pair_max" with old rows: a downgrade/upgrade or an old import.
    logger.info("wind basis reset to staging: old wind rows found site=%s", site_id)
    set_wind_basis_state(conn, site_id, "staging")
    delete_runtime_state(
        conn,
        wind_cursor_key(site_id),
        wind_blocked_key(site_id),
        wind_report_key(site_id),
        wind_done_at_key(site_id),
    )


# --- cursor ----------------------------------------------------------------


def write_wind_cursor(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    phase: Literal["purge", "install"],
    cursor_date: date | None,
) -> None:
    value = {
        "phase": phase,
        "date": None if cursor_date is None else cursor_date.isoformat(),
    }
    set_runtime_state(conn, wind_cursor_key(site_id), json.dumps(value))


@dataclass(frozen=True)
class WindCursor:
    """The switch cursor as stored; ``None`` fields are missing or unreadable."""

    phase: Literal["purge", "install"] | None
    cursor_date: date | None
    date_present: bool


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


def read_wind_cursor(conn: sqlite3.Connection, site_id: int) -> WindCursor:
    fields = json_object(get_runtime_state(conn, wind_cursor_key(site_id)))
    if fields is None:
        return WindCursor(phase=None, cursor_date=None, date_present=False)
    phase_value = fields.get("phase")
    phase: Literal["purge", "install"] | None
    if phase_value == "purge":
        phase = "purge"
    elif phase_value == "install":
        phase = "install"
    else:
        phase = None
    date_value = fields.get("date")
    cursor_date: date | None = None
    if isinstance(date_value, str):
        try:
            cursor_date = date.fromisoformat(date_value)
        except ValueError:
            cursor_date = None
    return WindCursor(
        phase=phase, cursor_date=cursor_date, date_present="date" in fields
    )


# --- auth hold -------------------------------------------------------------


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


# --- progress --------------------------------------------------------------

_STAGING_NOTE: Final = (
    "Wind history is being rebuilt in the background: {n} of {total} "
    "station-days fetched. Wind values are still on the old basis until the "
    "switch."
)
_CLOSED_NOTE: Final = "Wind is being recomputed on the new basis: {progress}"


@dataclass(frozen=True)
class WindRebuildProgress:
    """What the notes, the banner and the ops page show about one site's rebuild."""

    site_id: int
    state: str
    #: Rows before today that are fetched, unavailable or failed-final.
    done_days: int
    #: All rows before today (enabled stations).
    total_days: int
    #: "purge", "install", "rescore" or None.
    phase: str | None
    day: int | None
    days: int | None
    progress_at: str | None

    @property
    def progress_text(self) -> str:
        if self.phase == "rescore":
            return "rescoring feeds"
        if self.phase in ("purge", "install") and self.day and self.days:
            verb = (
                "clearing old values"
                if self.phase == "purge"
                else "recomputing site wind"
            )
            return f"{verb}, day {self.day} of {self.days}"
        return "starting"

    @property
    def closed_note(self) -> str:
        """The note for wind served closed, whatever the stored state reads.

        The serving double read (plan §11.4) closes wind for one request when
        the state moved under it, which can leave a non-closed stored state.
        """
        return _CLOSED_NOTE.format(progress=self.progress_text)

    @property
    def note(self) -> str | None:
        """The plain-words note for this state; None in ``pair_max``."""
        mode = wind_serving_mode(self.state)
        if mode == "legacy":
            return _STAGING_NOTE.format(n=self.done_days, total=self.total_days)
        if mode == "closed":
            return self.closed_note
        return None


def _site_today(conn: sqlite3.Connection, site_id: int) -> date | None:
    row = conn.execute("SELECT timezone FROM sites WHERE id = ?", (site_id,)).fetchone()
    if row is None:
        return None
    try:
        tz = ZoneInfo(str(row["timezone"]))
    except (ZoneInfoNotFoundError, ValueError):
        return None
    return utc_now().astimezone(tz).date()


def _earliest_local_day(
    conn: sqlite3.Connection, site_id: int, tz_name: str
) -> date | None:
    """The local date of the site's earliest wind station row or observation."""
    row = conn.execute(
        """
        SELECT MIN(j) AS j FROM (
            SELECT MIN(julianday(so.valid_at)) AS j
            FROM station_observations so JOIN stations s ON s.id = so.station_id
            WHERE s.site_id = ? AND so.variable = 'wind'
            UNION ALL
            SELECT MIN(julianday(o.valid_at)) AS j
            FROM observations o WHERE o.site_id = ? AND o.variable = 'wind'
        )
        """,
        (site_id, site_id),
    ).fetchone()
    if row is None or row["j"] is None:
        return None
    try:
        moment = julian_to_utc(float(row["j"]))
        return moment.astimezone(ZoneInfo(tz_name)).date()
    except (ZoneInfoNotFoundError, ValueError, OverflowError, OSError):
        return None


def julian_to_utc(julian: float) -> datetime:
    """The UTC instant of an SQLite ``julianday`` value, to the nearest second."""
    # 2440587.5 is the Julian day of the Unix epoch.
    return datetime.fromtimestamp(round((julian - 2440587.5) * 86400), UTC)


def wind_rebuild_progress(
    conn: sqlite3.Connection, site_id: int
) -> WindRebuildProgress:
    """Read one site's rebuild progress for the notes, the banner and the ops page."""
    state = wind_basis_state(conn, site_id)
    today = _site_today(conn, site_id)
    done_days = 0
    total_days = 0
    first_date: date | None = None
    if today is not None:
        counts = conn.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN d.status IN ('fetched','unavailable')
                             OR (d.status = 'failed' AND d.attempts >= 3)
                            THEN 1 ELSE 0 END) AS done,
                   MIN(d.local_date) AS first_date
            FROM station_wind_days d JOIN stations st ON st.id = d.station_id
            WHERE st.site_id = ? AND st.enabled = 1 AND d.local_date < ?
            """,
            (site_id, today.isoformat()),
        ).fetchone()
        total_days = int(counts["total"] or 0)
        done_days = int(counts["done"] or 0)
        if counts["first_date"] is not None:
            try:
                first_date = date.fromisoformat(str(counts["first_date"]))
            except ValueError:
                first_date = None
    phase: str | None = None
    day: int | None = None
    days: int | None = None
    if state == "rescoring":
        phase = "rescore"
    elif state == "switching":
        cursor = read_wind_cursor(conn, site_id)
        phase = cursor.phase
        if today is not None and first_date is not None:
            days = (today - first_date).days + 1
            current: date | None = None
            if phase == "install":
                current = cursor.cursor_date
            elif phase == "purge":
                tz_row = conn.execute(
                    "SELECT timezone FROM sites WHERE id = ?", (site_id,)
                ).fetchone()
                if tz_row is not None:
                    current = _earliest_local_day(
                        conn, site_id, str(tz_row["timezone"])
                    )
                    if current is None:
                        current = today
            if current is not None:
                day = max(1, min(days, (current - first_date).days + 1))
    return WindRebuildProgress(
        site_id=site_id,
        state=state,
        done_days=done_days,
        total_days=total_days,
        phase=phase,
        day=day,
        days=days,
        progress_at=get_runtime_state(conn, wind_progress_key(site_id)),
    )
