"""Part A of the switch to the daily close, and the one time zone generation left.

0.16.14 moves every site to the daily close. Part A runs once per file, at
open, in :func:`migrate_v15_switch` (database version 15), before any reader
or worker runs. It removes what the release no longer reads or runs:

* A1, :func:`keep_read_generations`: of ``forecast_pairs`` and
  ``daily_truth`` only the rows of the generation each site's readers read
  now are kept, and that generation is left the site's only ``published``
  one;
* A2, :func:`settle_retired_jobs`: pending and running jobs of the retired
  kinds are deleted (the cutover settles them once more before it writes the
  key);
* A3: each site's unfinished replay runs are failed and their partial rows
  dropped, through ``fail_incomplete_attempts``;
* A4: every ``runtime_state`` key under a retired prefix is deleted.

The four tables that carry ``tz_generation_id`` keep the column as a key.
:func:`site_generation_id` reads the site's one ``published`` generation and
:func:`ensure_site_generation` makes it for a site that has none.

:func:`first_record_date` reads the site's first closed day once the cutover
has made its first record: every date before it is starting history.

:func:`history_prune_refusals`, :func:`history_prune_stations` and
:func:`prune_station_history` are the owner's one-time prune of the wind
history (plan section 12): the rows of ``station_wind_records`` from before
0.16.14 (``fetched_at`` NULL) dated more than the wind pair limit before
their site's first closed day. Only the CLI's ``wind-history prune`` calls
the prune.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.site_days import (
    DONE_STATES,
    DayRecord,
    WriteKind,
    check_day_write,
    daily_close_on,
    day_record,
)
from wxverify.obs.wind_pairs import MAX_PAIR_GAP_SECONDS

logger = logging.getLogger(__name__)

# The retired time zone pointer: ``<prefix><site_id>`` held the id of the
# generation every reader of the site selected. A1 reads it; A4 deletes it.
_POINTER_KEY_PREFIX: Final = "tz_generation_published:"

# A4's prefixes: the old wind history's refused-access hold, the time zone
# pointer, the correction's chain state and heartbeat, and the replay's chain
# state and heartbeat. No reader of them is left in this release.
_RETIRED_KEY_PREFIXES: Final = (
    "wind_history_auth_hold:",
    _POINTER_KEY_PREFIX,
    "tz_correction_state:",
    "tz_correction_heartbeat:",
    "verification_state:",
    "verification_heartbeat:",
)

# A3's error for a running replay run; a run that already holds an error
# keeps its own.
_RETIRED_RUN_ERROR: Final = "retired at the switch to the daily close"

# A2's rows: pending or running, of a retired kind, or ``fetch_obs`` under
# any key but ``wind-days`` (that one runs the daily close's collection).
_RETIRED_JOBS_WHERE: Final = """
    status IN ('pending', 'running')
    AND (type IN ('catchup', 'backfill_site', 'timezone_correction',
                  'verification_run', 'pair_and_score')
         OR (type = 'fetch_obs' AND job_key IS NOT 'wind-days'))
"""


def site_generation_id(conn: sqlite3.Connection, site_id: int) -> int | None:
    """The id of the site's ``published`` time zone generation, or None.

    Read-only. After Part A a site has exactly one such row and nothing in
    this release makes another. On a file Part A has not yet run on, which
    can hold several, the highest id is returned.
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


def _kept_generation(conn: sqlite3.Connection, site_id: int) -> int:
    """A1's K for one site: the generation its readers read now.

    The pointer's generation when the pointer's text is exactly the decimal
    id of a ``published`` generation of this site; else the site's
    ``published`` generation with the highest id; else a new one. Either
    fallback is logged at WARNING with the site.
    """
    row = conn.execute(
        """
        SELECT g.id
        FROM timezone_generations g
        JOIN runtime_state rs
          ON rs.key = ? AND rs.value = CAST(g.id AS TEXT)
        WHERE g.site_id = ? AND g.state = 'published'
        """,
        (f"{_POINTER_KEY_PREFIX}{site_id}", site_id),
    ).fetchone()
    if row is not None:
        return int(row[0])
    newest = site_generation_id(conn, site_id)
    if newest is not None:
        logger.warning(
            "switch A1: site %d has no time zone pointer naming one of its "
            "published generations; keeping its newest published generation %d",
            site_id,
            newest,
        )
        return newest
    made = ensure_site_generation(conn, site_id)
    logger.warning(
        "switch A1: site %d had no published time zone generation; made generation %d",
        site_id,
        made,
    )
    return made


def keep_read_generations(conn: sqlite3.Connection) -> None:
    """A1: keep only the pair and truth rows each site's readers read now.

    Every reader of ``forecast_pairs`` and ``daily_truth`` selected the rows
    of the site's pointer generation. For each site, with K that generation
    (:func:`_kept_generation`), its rows of the two tables under any other
    generation are deleted, and its other ``building`` and ``published``
    generations are marked ``retired``, so K is left its only ``published``
    one. One INFO line per site names K and the counts.

    Nothing else changes: no ``forecast_of_record``, ``verification_runs``
    or ``timezone_generations`` row is deleted, and K's own rows are not
    touched. A repeat matches no row. Runs before A4 deletes the pointers,
    inside the caller's transaction.
    """
    site_ids = [
        int(row[0])
        for row in conn.execute("SELECT id FROM sites ORDER BY id").fetchall()
    ]
    for site_id in site_ids:
        kept = _kept_generation(conn, site_id)
        pairs = conn.execute(
            "DELETE FROM forecast_pairs WHERE site_id = ? AND tz_generation_id <> ?",
            (site_id, kept),
        ).rowcount
        truth = conn.execute(
            "DELETE FROM daily_truth WHERE site_id = ? AND tz_generation_id <> ?",
            (site_id, kept),
        ).rowcount
        retired = conn.execute(
            "UPDATE timezone_generations SET state = 'retired'"
            " WHERE site_id = ? AND id <> ? AND state IN ('building', 'published')",
            (site_id, kept),
        ).rowcount
        logger.info(
            "switch A1: site %d keeps time zone generation %d; %d pair row(s) "
            "and %d truth row(s) of other generations deleted, %d "
            "generation(s) retired",
            site_id,
            kept,
            pairs,
            truth,
            retired,
        )


def settle_retired_jobs(conn: sqlite3.Connection) -> int:
    """A2: delete the pending and running jobs of the retired kinds.

    The kinds are ``catchup``, ``backfill_site``, ``timezone_correction``,
    ``verification_run`` and ``pair_and_score``, and ``fetch_obs`` under any
    key but ``wind-days``. Completed and failed rows of every kind, every
    row of a kept kind and the ``wind-days`` rows are kept. Returns the
    number of rows deleted, logged at INFO with the count per kind. Runs
    inside the caller's transaction.
    """
    per_kind = [
        f"{row[0]} {int(row[1])}"
        for row in conn.execute(
            f"SELECT type, COUNT(*) FROM jobs WHERE {_RETIRED_JOBS_WHERE}"
            " GROUP BY type ORDER BY type"
        ).fetchall()
    ]
    deleted = conn.execute(f"DELETE FROM jobs WHERE {_RETIRED_JOBS_WHERE}").rowcount
    logger.info(
        "switch A2: %d job row(s) of retired kinds deleted (%s)",
        deleted,
        ", ".join(per_kind) or "none",
    )
    return deleted


def migrate_v15_switch(conn: sqlite3.Connection) -> None:
    """Part A of the switch to the daily close, for a file below version 15.

    In order: A1 :func:`keep_read_generations`, A2
    :func:`settle_retired_jobs`, A3 ``fail_incomplete_attempts`` for every
    site, A4 the retired ``runtime_state`` keys. It writes no key, and
    changes no pair or truth row outside A1. ``run_migrations`` calls it
    behind ``current < 15`` and writes the version last.

    One SAVEPOINT holds all four parts, following ``migrate_v4``: any error
    rolls every part back and is re-raised, so the open fails and the next
    open runs Part A again. As in ``migrate_v12_widen_raw_readings``, the
    in_transaction check covers an error that has already ended the
    transaction. Each part finds its work done on a repeat, so a stop
    between this step and the version write converges on the next open.
    """
    # Imported here, as bootstrap_publish_hold imports the publish hold: the
    # runs module imports the 07:00 record module, which imports
    # db.connection, which imports the migrations and so this module.
    from wxverify.verification.runs import fail_incomplete_attempts

    keys_deleted = 0
    conn.execute("SAVEPOINT migrate_v15")
    try:
        keep_read_generations(conn)
        settle_retired_jobs(conn)
        for row in conn.execute("SELECT id FROM sites ORDER BY id").fetchall():
            fail_incomplete_attempts(conn, int(row[0]), error=_RETIRED_RUN_ERROR)
        for prefix in _RETIRED_KEY_PREFIXES:
            keys_deleted += conn.execute(
                "DELETE FROM runtime_state WHERE substr(key, 1, ?) = ?",
                (len(prefix), prefix),
            ).rowcount
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK TO migrate_v15")
            conn.execute("RELEASE migrate_v15")
        raise
    conn.execute("RELEASE migrate_v15")
    logger.info("switch A4: %d retired runtime_state key(s) deleted", keys_deleted)


# ---------------------------------------------------------------------------
# The one-time prune of the wind history (plan section 12)
# ---------------------------------------------------------------------------

# Plan 12.2: what is kept before each site's first closed day F, the wind
# pair limit, so F's first pair still finds the reading before F's start.
_HISTORY_MARGIN: Final = timedelta(seconds=MAX_PAIR_GAP_SECONDS)
# Plan 12.5: the guard is asked one second before the delete's bound.
_GUARD_STEP: Final = timedelta(seconds=1)


@dataclass(frozen=True, slots=True)
class StationHistory:
    """One station's rows the prune would delete, as ``--dry-run`` shows them.

    ``first_obs_at`` and ``last_obs_at`` are the earliest and the latest of
    those rows' canonical ``obs_at`` stamps.
    """

    station_id: int
    pws_station_id: str
    site_id: int
    rows: int
    first_obs_at: str
    last_obs_at: str


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


def history_prune_refusals(conn: sqlite3.Connection) -> list[str]:
    """Every reason the wind-history prune must refuse now, one line each (12.5).

    One line when the daily close is off, and one for each site that owns a
    row with ``fetched_at`` NULL (through ``stations.site_id``) while it has
    no record, or while its first record F is neither ``closed`` nor
    ``missed``. An empty list means the prune may run. Read-only; runs in the
    caller's transaction.
    """
    refusals: list[str] = []
    if not daily_close_on(conn):
        refusals.append("the daily close is off")
    for site_id in _sites_with_history(conn):
        first = _site_first_record(conn, site_id)
        if first is None:
            refusals.append(f"site {site_id} has no day record yet")
        elif first.state not in DONE_STATES:
            refusals.append(
                f"site {site_id}: its first day, {first.local_date.isoformat()},"
                f" is {first.state}, not closed or missed"
            )
    return refusals


def history_prune_stations(conn: sqlite3.Connection) -> list[StationHistory]:
    """Each station's rows the prune would delete now, by station id (12.5).

    The rows with ``fetched_at`` NULL and ``obs_at`` before the day start of
    their site's first record F minus the wind pair limit: the rows
    :func:`prune_station_history` deletes. At a site with no record yet,
    every row of its stations with ``fetched_at`` NULL (an upper bound; the
    prune refuses that site). A station with no such row is left out, so the
    sum of ``rows`` is the count still to delete. Read-only; runs in the
    caller's transaction.
    """
    stations: list[StationHistory] = []
    for site_id in _sites_with_history(conn):
        first = _site_first_record(conn, site_id)
        bound = (
            None
            if first is None
            else isoformat_utc(first.day_start_utc - _HISTORY_MARGIN)
        )
        rows = conn.execute(
            "SELECT r.station_id, s.pws_station_id, COUNT(*), MIN(r.obs_at),"
            " MAX(r.obs_at)"
            " FROM station_wind_records r JOIN stations s ON s.id = r.station_id"
            " WHERE s.site_id = ? AND r.fetched_at IS NULL"
            " AND (? IS NULL OR julianday(r.obs_at) < julianday(?))"
            " GROUP BY r.station_id, s.pws_station_id",
            (site_id, bound, bound),
        ).fetchall()
        stations.extend(
            StationHistory(
                station_id=int(row[0]),
                pws_station_id=str(row[1]),
                site_id=site_id,
                rows=int(row[2]),
                first_obs_at=str(row[3]),
                last_obs_at=str(row[4]),
            )
            for row in rows
        )
    stations.sort(key=lambda station: station.station_id)
    return stations


def prune_station_history(
    conn: sqlite3.Connection, *, station_id: int, now_utc: datetime
) -> int:
    """Delete one station's wind history from before its site's first day (12.5).

    Calls :func:`history_prune_refusals` first and raises ``ValueError`` with
    its lines, deleting nothing, if there are any. Then, with F the first
    record of the station's site, asks P1's guard for
    ``WriteKind.HISTORY_PRUNE`` at F's ``day_start_utc`` minus 1201 s, raising
    ``ValueError`` when it refuses, and deletes the station's rows with
    ``fetched_at`` NULL and ``obs_at`` before F's start minus the wind pair
    limit (1200 s). Returns the number of rows deleted. A station whose site
    has no record yet owns no such row (the refusals would name its site), so
    0. Raises ``ValueError`` when the station does not exist. Runs in the
    caller's transaction.
    """
    refusals = history_prune_refusals(conn)
    if refusals:
        raise ValueError("\n".join(refusals))
    station = conn.execute(
        "SELECT site_id FROM stations WHERE id = ?", (station_id,)
    ).fetchone()
    if station is None:
        raise ValueError(f"station {station_id} does not exist")
    site_id = int(station[0])
    first = _site_first_record(conn, site_id)
    if first is None:
        return 0
    bound = first.day_start_utc - _HISTORY_MARGIN
    if not check_day_write(
        conn,
        site_id,
        isoformat_utc(bound - _GUARD_STEP),
        WriteKind.HISTORY_PRUNE,
        now_utc=now_utc,
    ):
        raise ValueError(
            f"site {site_id}: the closed-day guard refused the prune before"
            f" {isoformat_utc(bound)}"
        )
    return conn.execute(
        "DELETE FROM station_wind_records"
        " WHERE station_id = ? AND fetched_at IS NULL"
        " AND julianday(obs_at) < julianday(?)",
        (station_id, isoformat_utc(bound)),
    ).rowcount
