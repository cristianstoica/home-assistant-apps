"""Sanitize passes for timestamp columns a foreign database can carry.

``jobs.next_attempt_at`` and ``station_poll_state.next_poll_at`` both gate a
``<= isoformat_utc()`` due-comparison (``db.queue.claim_next_job``,
``worker.scheduler.enqueue_due_current_obs``, and ``monitor``'s overdue-job
scan). A value that sorts AFTER the current ISO-8601 stamp -- a garbage
string like ``'zzzz'``, or a plausible far-future date -- is never SELECTED
by that comparison, so none of those paths' own unreadable-row handling ever
runs on it: the row is simply never picked up again. An imported, restored,
or hand-edited database can carry exactly such a value, and left alone it
wedges the row permanently. These passes catch it before that comparison
ever runs: once on a staged upload before promotion, and once at boot for a
database swapped in by hand outside the app.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import timedelta
from typing import Final

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.runtime_state import delete_runtime_state, get_runtime_state
from wxverify.settings.keys import get_number_setting

logger = logging.getLogger(__name__)

#: The wind lane's hold on today's 5-minute download, written before 0.16.13.
#: That download is now held under the ``five_minute`` refused-access kind, so
#: nothing reads or writes this key any more and a leftover row is deleted.
_RETIRED_WIND_HOLD_KEY: Final = "wind_history_auth_hold:all_1day"

# Mirrors worker.current_obs.MIN_INTERVAL_SECONDS / _MIN_INTERVAL_FLOOR. Not
# imported directly -- the db layer must not depend on the worker package --
# this is only the fallback poll delay for a station whose next_poll_at
# could not be parsed at all.
_MIN_INTERVAL_SECONDS_DEFAULT = 300
_MIN_INTERVAL_SECONDS_FLOOR = 60


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    """Whether a table ``name`` exists, matched the way SQLite resolves names.

    SQLite matches identifiers ASCII-case-insensitively, and ``COLLATE
    NOCASE`` folds ASCII only, the same rule, so ``jobs`` finds a table
    stored as ``Jobs``. Only ``type = 'table'`` rows count: a view reads as
    absent, as before.
    """
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? COLLATE NOCASE",
        (name,),
    ).fetchone()
    return row is not None


def sanitize_wedge_prone_timestamps(conn: sqlite3.Connection) -> None:
    """Rewrite unparseable ``next_attempt_at`` / ``next_poll_at`` values.

    A minimal hand-built upload may lack either table (only ``sites``,
    ``stations``, and ``station_observations`` are required at import), so
    each pass is skipped rather than failing when its table is absent. Then
    ``release_key_refused_polls`` runs, with its own table guards.
    """
    if table_exists(conn, "jobs"):
        _sanitize_jobs_next_attempt_at(conn)
    if table_exists(conn, "station_poll_state"):
        _sanitize_station_poll_next_poll_at(conn, table_exists(conn, "settings"))
    release_key_refused_polls(conn)


def release_key_refused_polls(conn: sqlite3.Connection) -> None:
    """Release current-obs polls an earlier version parked on a 401/403.

    Before 0.16.13 a refused current-conditions poll left its station
    ``terminal`` with ``last_error`` ``'http 401'`` or ``'http 403'`` and
    ``next_poll_at`` up to 24 h ahead. Each such poll still in the future is
    made due now. Only ``next_poll_at`` is written: ``health_state``,
    ``last_error`` and ``error_count`` stay for the station's next poll to
    replace. Then the retired ``all_1day`` wind lane hold row is deleted.

    The release is skipped when ``station_poll_state`` or one of the three
    columns it reads is absent (a minimal hand-built upload), and the delete
    when ``runtime_state`` is absent; each runs without the other.
    Idempotent: nothing from 0.16.13 on writes such a poll or such a row, so
    after the first run neither matches.
    """
    if table_exists(conn, "station_poll_state"):
        columns = {
            str(row[1]).lower()
            for row in conn.execute("PRAGMA table_info(station_poll_state)")
        }
        if {"health_state", "last_error", "next_poll_at"} <= columns:
            released = conn.execute(
                "UPDATE station_poll_state SET next_poll_at = :now"
                " WHERE health_state = 'terminal'"
                " AND last_error IN ('http 401', 'http 403')"
                " AND next_poll_at > :now",
                {"now": isoformat_utc(utc_now())},
            ).rowcount
            if released > 0:
                logger.info(
                    "sanitize: released %d current-obs polls parked by a refused key",
                    released,
                )
    if (
        table_exists(conn, "runtime_state")
        and get_runtime_state(conn, _RETIRED_WIND_HOLD_KEY) is not None
    ):
        delete_runtime_state(conn, _RETIRED_WIND_HOLD_KEY)
        logger.info(
            "sanitize: removed the retired wind lane hold %s", _RETIRED_WIND_HOLD_KEY
        )


def _sanitize_jobs_next_attempt_at(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, next_attempt_at FROM jobs WHERE next_attempt_at IS NOT NULL"
    ).fetchall()
    fixed = 0
    for row in rows:
        try:
            parse_utc(str(row["next_attempt_at"]))
        except Exception:  # see db.queue._claim's
            # comment: the carrier set here is not enumerable -- a value
            # shaped like a timestamp is not guaranteed to parse as one
            # (an enumerated allowlist has already missed a carrier once),
            # and this pass must not abort on one row. Kept broad
            # deliberately even though parse_utc now raises ValueError for
            # every str input.
            #
            # NULL is already the claimable state (_claim's WHERE
            # clause), so the row goes straight back through that function's
            # own unreadable-row disposition instead of being discarded here
            # on the strength of one column. This disposition is its own
            # try/except so that a row whose disposition itself fails
            # cannot abort the pass for every other row.
            try:
                conn.execute(
                    "UPDATE jobs SET next_attempt_at = NULL WHERE id = ?",
                    (row["id"],),
                )
                fixed += 1
            except Exception:
                logger.exception(
                    "sanitize: failed to clear jobs.next_attempt_at id=%s",
                    row["id"],
                )
    if fixed:
        logger.warning(
            "sanitize: cleared unparseable jobs.next_attempt_at rows=%d", fixed
        )


def _sanitize_station_poll_next_poll_at(
    conn: sqlite3.Connection, settings_table_exists: bool
) -> None:
    min_interval = _MIN_INTERVAL_SECONDS_DEFAULT
    if settings_table_exists:
        try:
            min_interval = get_number_setting(
                conn,
                "min_interval_seconds",
                _MIN_INTERVAL_SECONDS_DEFAULT,
                minimum=_MIN_INTERVAL_SECONDS_FLOOR,
            )
        except Exception:
            # A `settings` table that exists but has the wrong shape (e.g.
            # no `value` column) raises sqlite3.OperationalError from this
            # same SELECT; fall back to the default rather than letting a
            # malformed settings table abort the whole sanitize pass.
            logger.exception(
                "sanitize: failed to read min_interval_seconds setting;"
                " using default=%d",
                _MIN_INTERVAL_SECONDS_DEFAULT,
            )
    rows = conn.execute(
        "SELECT station_id, next_poll_at FROM station_poll_state"
        " WHERE next_poll_at IS NOT NULL"
    ).fetchall()
    fixed = 0
    for row in rows:
        try:
            parse_utc(str(row["next_poll_at"]))
        except Exception:  # see the jobs pass above; the
            # carrier set is not enumerable, so the catch stays broad even
            # though parse_utc now raises ValueError for every str input.
            #
            # NULL would make the station instantly due on every future scan
            # (the current-obs due query treats NULL as due-now), so this
            # rewrites to a bounded near-future delay instead of clearing it.
            # station_id is bound directly, matching the jobs pass above:
            # a foreign database's station_id (or jobs.id) can hold TEXT
            # that a coercion would raise on, and the WHERE comparison
            # doesn't need it either way. Own try/except so a row whose
            # disposition itself fails cannot abort the pass for every
            # other row.
            try:
                fallback = isoformat_utc(utc_now() + timedelta(seconds=min_interval))
                conn.execute(
                    "UPDATE station_poll_state SET next_poll_at = ?"
                    " WHERE station_id = ?",
                    (fallback, row["station_id"]),
                )
                fixed += 1
            except Exception:
                logger.exception(
                    "sanitize: failed to rewrite station_poll_state"
                    ".next_poll_at station_id=%s",
                    row["station_id"],
                )
    if fixed:
        logger.warning(
            "sanitize: rewrote unparseable station_poll_state.next_poll_at rows=%d",
            fixed,
        )
