"""The daily close's three call points into the scored round and publication.

The close calls the scored round and the publication only through these
names, always through this module, so their bodies are bound here. Each is
a one-line call into its body, imported inside the function at call time:
this module's own imports stay ``db.*``, so loading it loads none of those
modules, and they import ``RoundState`` and ``ScoreSummary`` from here
without a cycle. The names stay module attributes, so a test can patch them.

Inside every call point the body changes no day state, enqueues no job,
reads a day's scores and record only once the day is ``closed``, and never
fetches station readings.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime

from wxverify.db.connection import Database, FencedWriter
from wxverify.db.site_days import DayRecord


@dataclass(frozen=True, slots=True)
class RoundState:
    """Stage 1's answer for one round date.

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
    ran included. Body: ``worker/round.py`` ``round_state``.
    """
    from wxverify.worker.round import round_state as body

    return body(conn, site_id=site_id, round_date=round_date)


def score_day(
    conn: sqlite3.Connection,
    *,
    record: DayRecord,
    attempt: int,
    truth_version: str,
    now_utc: datetime,
) -> ScoreSummary:
    """Score one ``closing`` day and store its scores and close record (stage 5).

    Writes through the closed-day guard in the caller's transaction,
    replacing any rows of an earlier attempt. An exception, or a guard
    refusal of its writes, fails the attempt. Body:
    ``scoring/close_scores.py`` ``score_day``.
    """
    from wxverify.scoring.close_scores import score_day as body

    return body(
        conn,
        record=record,
        attempt=attempt,
        truth_version=truth_version,
        now_utc=now_utc,
    )


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
    writes and is safe to repeat. Body: ``scoring/publication.py``
    ``publish``.
    """
    from wxverify.scoring.publication import publish as body

    return await body(
        db,
        writer,
        site_id=site_id,
        cutoff=cutoff,
        attempt=attempt,
        now_utc=now_utc,
    )
