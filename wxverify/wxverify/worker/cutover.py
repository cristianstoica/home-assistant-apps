"""Part B of the switch to the daily close: the cutover on the main lane.

Daily-close switch-over plan (P6), sections 4, 6 and 9. Part A runs at open
(``db/switch.py``). While ``DAILY_CLOSE_ON_KEY`` is absent the main lane runs
nothing but :class:`Cutover` (plan 6.2), one unit per call of
:meth:`Cutover.advance`:

* B1, one unit per site, in ``sites.id`` order: the site's starting history
  seeded and its first publication built, by P4's two calls behind
  :func:`build_first_publication`, unless :func:`first_publication_ready`
  already finds it current (plan 9.1);
* B2, one transaction: the retired jobs settled again, each site's first
  closed day fixed, its first publication checked, its first record made,
  and the key written with the switch time (plan 6.4).

Each unit saves its own work in the file, so a dropped attempt costs nothing
and the next one starts again at B1. ``utc_now``, :func:`build_first_publication`
and :func:`first_publication_ready` are module attributes, read at call time.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from typing import Final
from zoneinfo import ZoneInfo

from wxverify.core.error_sanitize import safe_detail
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import Database, FencedWriter
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.site_days import DAILY_CLOSE_ON_KEY, daily_close_on, start_site_days
from wxverify.db.switch import settle_retired_jobs, site_generation_id
from wxverify.scoring.publication import (
    active_publication,
    publish_first,
    seed_starting_history,
)

logger = logging.getLogger(__name__)

#: How long the main lane waits after a failed unit before a new attempt.
CUTOVER_RETRY_SECONDS: Final = 300

#: What every retired control answers with (plan 7.3).
RETIRED_CONTROL_MESSAGE: Final = (
    "This control was retired when the daily close turned on. "
    "Each day is collected and closed once after it ends, "
    "and past days are not fetched again."
)


class CutoverStep(Enum):
    """What one :meth:`Cutover.advance` call did (plan 6.1)."""

    ON = "on"  # B2 committed the key
    MORE = "more"  # one unit done; advance again at once
    WAIT = "wait"  # a unit failed; a new attempt after CUTOVER_RETRY_SECONDS


def first_closed_day(timezone: str, now_utc: datetime) -> date:
    """The site's first closed day F for a switch at ``now_utc`` (plan 4.2).

    The local date of ``now_utc`` in ``timezone``, plus one day, whatever the
    time of day: the first whole day whose own evening collection runs after
    the switch. A naive ``now_utc`` is UTC. Raises ``ZoneInfoNotFoundError``
    or ``ValueError`` when ``timezone`` does not read.
    """
    instant = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=UTC)
    return instant.astimezone(ZoneInfo(timezone)).date() + timedelta(days=1)


async def build_first_publication(
    db: Database, writer: FencedWriter, site_id: int, first_record_date: date
) -> None:
    """B1 for one site: P4's seed, then P4's first publication (plan 9.1).

    The site's ``sites.timezone`` and its one time zone generation are read
    in a snapshot; ``seed_starting_history`` then reads the site's starting
    history in a snapshot and writes it once; then ``publish_first`` builds
    the publication whose cutoff is the day before ``first_record_date``.
    Raises ``ValueError`` when the site is gone or has no time zone
    generation, and whatever P4's calls raise.
    """

    def _site(conn: sqlite3.Connection) -> tuple[str, int]:
        row = conn.execute(
            "SELECT timezone FROM sites WHERE id = ?", (site_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"site {site_id} does not exist")
        generation_id = site_generation_id(conn, site_id)
        if generation_id is None:
            raise ValueError(f"site {site_id} has no time zone generation")
        return str(row[0]), generation_id

    _epoch, (timezone, generation_id) = await writer.read_at_epoch(
        _site, label="first_publication_site"
    )
    await seed_starting_history(
        writer,
        site_id=site_id,
        first_record_date=first_record_date,
        generation_id=generation_id,
        timezone=timezone,
    )
    await publish_first(
        db,
        writer,
        site_id=site_id,
        first_record_date=first_record_date,
        now_utc=utc_now(),
    )


def first_publication_ready(
    conn: sqlite3.Connection, site_id: int, first_record_date: date
) -> bool:
    """Whether the site's first publication is current for ``first_record_date``.

    True when P4's active publication with the latest cutoff has cutoff
    ``first_record_date`` - 1 (plan 6.4 step 4, 9.1). Read-only, in the
    caller's transaction.
    """
    publication = active_publication(conn, site_id=site_id, at_utc=None)
    return publication is not None and publication.cutoff == (
        first_record_date - timedelta(days=1)
    )


@dataclass(frozen=True, slots=True)
class _Switched:
    """What B2 committed: the switch time and each site's first closed day."""

    switch_time: datetime
    first_days: tuple[tuple[int, date], ...]


class _KeyRefused(Exception):
    """B2 found sites it cannot switch, each with its reason.

    Raised inside the key transaction, so the transaction rolls back and
    writes nothing.
    """

    def __init__(self, refusals: tuple[tuple[int, str], ...]) -> None:
        super().__init__(f"{len(refusals)} site(s) refused")
        self.refusals = refusals


def _site_ids(conn: sqlite3.Connection) -> list[int]:
    return [
        int(row[0])
        for row in conn.execute("SELECT id FROM sites ORDER BY id").fetchall()
    ]


def _key_transaction(conn: sqlite3.Connection) -> _Switched | None:
    """B2, inside one write (plan 6.4); None when the key is already set.

    In order: the key check; the retired jobs settled again; T read once and
    each site's first closed day F computed from it; each site's first
    publication checked for its F; each site's first record made, in id
    order, enabled or not; the key written with T. A site whose zone does
    not read, or whose publication is not current, raises ``_KeyRefused``
    naming every such site, so nothing is written.
    """
    if daily_close_on(conn):
        return None
    settle_retired_jobs(conn)
    switch_time = utc_now()
    sites = [
        (int(row[0]), str(row[1]))
        for row in conn.execute("SELECT id, timezone FROM sites ORDER BY id").fetchall()
    ]
    first_days: list[tuple[int, date]] = []
    unreadable: list[tuple[int, str]] = []
    for site_id, timezone in sites:
        try:
            first_days.append((site_id, first_closed_day(timezone, switch_time)))
        except Exception as exc:  # only the zone read is inside the try
            unreadable.append(
                (site_id, f"its time zone does not read ({safe_detail(exc)})")
            )
    if unreadable:
        raise _KeyRefused(tuple(unreadable))
    not_ready = tuple(
        (
            site_id,
            "its first publication is not current for first closed day "
            f"{first_day.isoformat()}",
        )
        for site_id, first_day in first_days
        if not first_publication_ready(conn, site_id, first_day)
    )
    if not_ready:
        raise _KeyRefused(not_ready)
    for site_id, first_day in first_days:
        start_site_days(conn, site_id, first_day, now_utc=switch_time)
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(switch_time))
    return _Switched(switch_time=switch_time, first_days=tuple(first_days))


class Cutover:
    """One attempt at the cutover (plan 6.1).

    Every read and write of the attempt goes through one ``FencedWriter``
    taken at construction, so an import that replaces the file part-way
    makes the next one raise ``StaleGenerationError``, which ends the
    attempt like any failure. The site list is read on the first
    :meth:`advance`; each later call does the next unit.
    """

    def __init__(self, db: Database) -> None:
        self._db = db
        self._writer = FencedWriter(db, db.generation)
        self._site_ids: list[int] | None = None
        self._next = 0

    async def advance(self) -> CutoverStep:
        """Do one unit: MORE after a B1 unit, ON once B2 commits the key.

        A failed unit is logged at ERROR with the unit, its site and the
        reason, and returns WAIT. Raises nothing but ``asyncio.CancelledError``
        (and the other non-``Exception`` interrupts).
        """
        unit = "reading the site list"
        try:
            if self._site_ids is None:
                self._site_ids = await self._writer.write(_site_ids)
            if self._next < len(self._site_ids):
                site_id = self._site_ids[self._next]
                unit = f"the first publication of site={site_id}"
                await self._first_publication(site_id)
                self._next += 1
                return CutoverStep.MORE
            unit = "the key transaction"
            switched = await self._writer.write(_key_transaction)
        except _KeyRefused as refused:
            for site_id, reason in refused.refusals:
                logger.error(
                    "daily close cutover: the key transaction refused site=%d: %s",
                    site_id,
                    reason,
                )
            return CutoverStep.WAIT
        except Exception as exc:
            logger.error("daily close cutover: %s failed: %s", unit, safe_detail(exc))
            return CutoverStep.WAIT
        if switched is None:
            logger.info("daily close cutover: the key was already set; wrote nothing")
            return CutoverStep.ON
        switch_time = isoformat_utc(switched.switch_time)
        for site_id, first_day in switched.first_days:
            logger.info(
                "daily close on: site=%d first_closed_day=%s switch_time=%s",
                site_id,
                first_day.isoformat(),
                switch_time,
            )
        return CutoverStep.ON

    async def _first_publication(self, site_id: int) -> None:
        """B1 for one site: F_B read at the unit's start, then build if needed.

        The site's zone and the ready check are read through the fenced
        writer, so a replaced file fails the unit before anything is built.
        A site deleted since the site list was read is skipped.
        """
        now = utc_now()

        def _read(conn: sqlite3.Connection) -> tuple[date, bool] | None:
            row = conn.execute(
                "SELECT timezone FROM sites WHERE id = ?", (site_id,)
            ).fetchone()
            if row is None:
                return None
            first_day = first_closed_day(str(row[0]), now)
            return first_day, first_publication_ready(conn, site_id, first_day)

        found = await self._writer.write(_read)
        if found is None:
            logger.info("daily close cutover: site=%d is gone; nothing built", site_id)
            return
        first_day, ready = found
        if ready:
            logger.info(
                "daily close cutover: site=%d first publication already current "
                "for first closed day %s",
                site_id,
                first_day.isoformat(),
            )
            return
        await build_first_publication(self._db, self._writer, site_id, first_day)
        logger.info(
            "daily close cutover: site=%d first publication built "
            "for first closed day %s",
            site_id,
            first_day.isoformat(),
        )
