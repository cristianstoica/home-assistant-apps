"""Sanitize passes for timestamp columns a foreign database can carry.

``jobs.next_attempt_at`` and ``station_poll_state.next_poll_at`` both gate a
``<= isoformat_utc()`` due-comparison (``db.queue.claim_next_job``,
``worker.scheduler._enqueue_due_current_obs``, and ``monitor``'s overdue-job
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

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.settings.keys import get_number_setting

logger = logging.getLogger(__name__)

# Mirrors worker.current_obs.MIN_INTERVAL_SECONDS / _MIN_INTERVAL_FLOOR. Not
# imported directly -- the db layer must not depend on the worker package --
# this is only the fallback poll delay for a station whose next_poll_at
# could not be parsed at all.
_MIN_INTERVAL_SECONDS_DEFAULT = 300
_MIN_INTERVAL_SECONDS_FLOOR = 60


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def sanitize_wedge_prone_timestamps(conn: sqlite3.Connection) -> None:
    """Rewrite unparseable ``next_attempt_at`` / ``next_poll_at`` values.

    A minimal hand-built upload may lack either table (only ``sites``,
    ``stations``, and ``station_observations`` are required at import), so
    each pass is skipped rather than failing when its table is absent.
    """
    if _table_exists(conn, "jobs"):
        _sanitize_jobs_next_attempt_at(conn)
    if _table_exists(conn, "station_poll_state"):
        _sanitize_station_poll_next_poll_at(conn, _table_exists(conn, "settings"))


def _sanitize_jobs_next_attempt_at(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, next_attempt_at FROM jobs WHERE next_attempt_at IS NOT NULL"
    ).fetchall()
    fixed = 0
    for row in rows:
        try:
            parse_utc(str(row["next_attempt_at"]))
        except Exception:  # see db.queue.claim_next_job's
            # comment: an enumerated allowlist here has already missed
            # OverflowError (near-datetime.max/.min with a UTC offset,
            # `.astimezone` overflows) once; a value shaped like a
            # timestamp is not guaranteed to parse as one.
            #
            # NULL is already the claimable state (claim_next_job's WHERE
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
        except Exception:  # see the jobs pass above; a
            # near-datetime.max/.min value with a UTC offset raises
            # OverflowError, not ValueError, from parse_utc's astimezone.
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
