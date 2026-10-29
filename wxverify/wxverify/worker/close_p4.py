"""P4's four call points in the daily close (plan 14.1).

The close calls the scored round and publication plan (P4) only through
these names, always through this module, so P4 binds them here. Until it
does, each raises ``NotImplementedError``: a build without P4 fails its
first close attempt loudly instead of closing a day unscored.

Inside every call point P4 changes no day state, enqueues no job, reads a
day's scores and record only once the day is ``closed``, and never fetches
station readings (plan 14.1).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime

from wxverify.db.connection import Database, FencedWriter
from wxverify.db.site_days import DayRecord


@dataclass(frozen=True, slots=True)
class RoundState:
    """Stage 1's answer for one round date (plan 2.3, 14.1).

    ``settled`` is whether every feed of the round has succeeded or reached
    its limit; ``bound_utc`` is the round's scheduled start plus 30 minutes,
    the latest instant the close may wait for it.
    """

    settled: bool
    bound_utc: datetime


@dataclass(frozen=True, slots=True)
class ScoreSummary:
    """Stage 5's result: forecasts scored and absent, per feed and yardstick."""

    scored: dict[str, int]
    absent: dict[str, int]


def round_state(
    conn: sqlite3.Connection, *, site_id: int, round_date: date
) -> RoundState:
    """Whether the round of ``round_date`` has settled, and its bound (stage 1).

    Read-only. The bound exists for every round date, a round that never
    ran included (plan 5.2).
    """
    raise NotImplementedError("round_state is bound by the daily close's P4")


def score_day(
    conn: sqlite3.Connection,
    *,
    record: DayRecord,
    attempt: int,
    truth_version: str,
    now_utc: datetime,
) -> ScoreSummary:
    """Score one ``closing`` day and store its scores and close record (stage 5).

    Writes through P1's guard in the caller's transaction, replacing any
    rows of an earlier attempt. An exception, or a guard refusal of its
    writes, fails the attempt (plan 5.3).
    """
    raise NotImplementedError("score_day is bound by the daily close's P4")


async def publish(
    db: Database,
    writer: FencedWriter,
    *,
    site_id: int,
    cutoff: date,
    attempt: int,
    now_utc: datetime,
) -> str:
    """Publish rankings from the stored scores up to ``cutoff`` (stage 7).

    Returns the publication's identifier; for a cutoff already published,
    returns that identifier and publishes nothing new. Commits in its own
    writes and is safe to repeat.
    """
    raise NotImplementedError("publish is bound by the daily close's P4")


async def clean_worksheets(
    db: Database, writer: FencedWriter, *, site_id: int, now_utc: datetime
) -> int:
    """Delete the worksheets of runs older than the latest published (stage 8).

    Works in small saves and returns how many runs it finished. Commits in
    its own writes and is safe to repeat.
    """
    raise NotImplementedError("clean_worksheets is bound by the daily close's P4")
