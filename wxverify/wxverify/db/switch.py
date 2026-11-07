"""The one time zone generation of a site, its first record, and the
one-time prune of the wind history.

``daily_truth`` carries ``tz_generation_id`` as a key.
:func:`site_generation_id` reads the site's one ``published``
generation and :func:`ensure_site_generation` makes it for a site that has
none.

:func:`first_record_date` reads the site's first closed day once the site
has its first record: every date before it is starting history.

:func:`prune_wind_history` is the one-time prune of the wind history: the
rows of ``station_wind_records`` from before 0.16.14 (``fetched_at`` NULL)
dated more than the wind pair limit before their site's first closed day.
It runs once, in the version 18 upgrade (``migrate_v18``), and nothing
else calls it.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Final

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.site_days import DONE_STATES, DayRecord, day_record
from wxverify.obs.wind_pairs import MAX_PAIR_GAP_SECONDS


def site_generation_id(conn: sqlite3.Connection, site_id: int) -> int | None:
    """The id of the site's ``published`` time zone generation, or None.

    Read-only. A site has at most one such row: :func:`ensure_site_generation`
    makes one only when there is none. Should a file hold several, the
    highest id is returned.
    """
    row = conn.execute(
        "SELECT id FROM timezone_generations"
        " WHERE site_id = ? AND state = 'published'"
        " ORDER BY id DESC LIMIT 1",
        (site_id,),
    ).fetchone()
    return None if row is None else int(row[0])


def first_record_date(conn: sqlite3.Connection, site_id: int) -> date | None:
    """The date of the site's first ``site_days`` record, or None (plan 5.4).

    The earliest ``local_date`` of the site's records: its first closed day,
    before which every date is starting history. None while the site has no
    record. Read-only.
    """
    row = conn.execute(
        "SELECT MIN(local_date) FROM site_days WHERE site_id = ?", (site_id,)
    ).fetchone()
    return None if row is None or row[0] is None else date.fromisoformat(str(row[0]))


def ensure_site_generation(conn: sqlite3.Connection, site_id: int) -> int:
    """Return the site's ``published`` generation id, making it when absent.

    The row made is mode ``initial``, state ``published``, in the site's
    ``sites.timezone``, published now, with no effective bounds, so it
    covers the whole history. No ``runtime_state`` key is written. Raises
    ``ValueError`` when the site does not exist. Must run on a write
    connection, inside the caller's transaction.
    """
    existing = site_generation_id(conn, site_id)
    if existing is not None:
        return existing
    site = conn.execute(
        "SELECT timezone FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    if site is None:
        raise ValueError(f"site {site_id} does not exist")
    cur = conn.execute(
        """
        INSERT INTO timezone_generations
            (site_id, timezone, mode, state, published_at)
        VALUES (?, ?, 'initial', 'published', ?)
        """,
        (site_id, str(site[0]), isoformat_utc()),
    )
    if cur.lastrowid is None:
        raise RuntimeError("timezone generation insert failed")
    return int(cur.lastrowid)


# ---------------------------------------------------------------------------
# The one-time prune of the wind history (plan section 12)
# ---------------------------------------------------------------------------

# Plan 12.2: what is kept before each site's first closed day F, the wind
# pair limit, so F's first pair still finds the reading before F's start.
_HISTORY_MARGIN: Final = timedelta(seconds=MAX_PAIR_GAP_SECONDS)


@dataclass(frozen=True, slots=True)
class WindHistoryPrune:
    """What :func:`prune_wind_history` did."""

    deleted: int  # rows deleted
    kept_sites: tuple[int, ...]  # sites with such rows whose F is missing or not done


def _site_first_record(conn: sqlite3.Connection, site_id: int) -> DayRecord | None:
    """The site's first ``site_days`` record F, or None while it has none."""
    first = first_record_date(conn, site_id)
    return None if first is None else day_record(conn, site_id, first)


def _sites_with_history(conn: sqlite3.Connection) -> list[int]:
    """The sites that own a row with ``fetched_at`` NULL, by ``stations.site_id``."""
    return [
        int(row[0])
        for row in conn.execute(
            "SELECT DISTINCT s.site_id FROM station_wind_records r"
            " JOIN stations s ON s.id = r.station_id"
            " WHERE r.fetched_at IS NULL ORDER BY s.site_id"
        ).fetchall()
    ]


def prune_wind_history(conn: sqlite3.Connection) -> WindHistoryPrune:
    """Delete the old wind history from before each site's first day (12.2).

    For each site that owns a ``station_wind_records`` row with
    ``fetched_at`` NULL (through ``stations.site_id``), with F its first
    ``site_days`` record: when F's state is in ``DONE_STATES``, deletes
    those rows of the site's stations whose ``obs_at`` is before F's
    ``day_start_utc`` minus the wind pair limit (1200 s). A site whose F is
    missing or not done keeps its rows and is named in ``kept_sites``; it
    does not stop the other sites. Rows with ``fetched_at`` set are never
    touched, and the bound depends only on F, so a second call deletes
    nothing. Asks no closed-day guard (the bound is always before F) and
    creates no day record. Plain statements only, in the caller's
    transaction: the version 18 upgrade (``migrate_v18``) is the one caller.
    Returns the rows deleted and the sites kept, by site id.
    """
    deleted = 0
    kept: list[int] = []
    for site_id in _sites_with_history(conn):
        first = _site_first_record(conn, site_id)
        if first is None or first.state not in DONE_STATES:
            kept.append(site_id)
            continue
        bound = isoformat_utc(first.day_start_utc - _HISTORY_MARGIN)
        deleted += conn.execute(
            "DELETE FROM station_wind_records"
            " WHERE fetched_at IS NULL AND julianday(obs_at) < julianday(?)"
            " AND station_id IN (SELECT id FROM stations WHERE site_id = ?)",
            (bound, site_id),
        ).rowcount
    return WindHistoryPrune(deleted=deleted, kept_sites=tuple(kept))
