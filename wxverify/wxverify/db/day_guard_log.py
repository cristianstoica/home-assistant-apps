"""The closed-day guard's refusal WARNING, shared by the guarded writers.

A writer whose rows the guard refused logs one WARNING per call, in one
form: ``closed-day guard refused <KIND> writer=<name> site=<id>
days=<dates> count=<n>``. It names the site id and local dates only, never a
station id or a coordinate. Each writer passes its own module logger.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterable, Sequence
from datetime import date

from wxverify.db.site_days import WriteKind, day_for_instant


def warn_refused_dates(
    log: logging.Logger,
    kind: WriteKind,
    *,
    writer: str,
    site_id: int,
    days: Iterable[date],
    count: int,
) -> None:
    """Log the refusal WARNING for ``count`` refused rows on ``days``.

    ``days`` is sorted and de-duplicated. Nothing is logged when ``count``
    is 0.
    """
    if not count:
        return
    log.warning(
        "closed-day guard refused %s writer=%s site=%s days=%s count=%d",
        kind.name,
        writer,
        site_id,
        ",".join(sorted({day.isoformat() for day in days})),
        count,
    )


def warn_refused_instants(
    log: logging.Logger,
    conn: sqlite3.Connection,
    kind: WriteKind,
    *,
    writer: str,
    site_id: int,
    instants: Sequence[str],
) -> None:
    """Log the refusal WARNING for refused rows keyed by ``instants``.

    One entry per refused row, so ``count`` is ``len(instants)``. A row's
    day is the record whose stored bounds hold its instant; an instant that
    does not parse, or that no record holds, counts but names no date.
    Read-only. Nothing is logged when ``instants`` is empty.
    """
    if not instants:
        return
    days: list[date] = []
    for instant in set(instants):
        record = day_for_instant(conn, site_id, instant)
        if record is not None:
            days.append(record.local_date)
    warn_refused_dates(
        log, kind, writer=writer, site_id=site_id, days=days, count=len(instants)
    )
