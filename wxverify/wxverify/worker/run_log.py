"""One INFO line per model run a reply carried.

Each line says when the reply came back, the model's run time, the
provider's own update time for the run when the reply has one, the latest
run stored for the model before this reply is saved, whether the run is
new, and the call's site time. The lines are built only from those fields
and the site's integer id, so no key, address, location or reply text can
appear in them.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Final
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.connection import Database
from wxverify.db.site_days import day_for_instant
from wxverify.feeds.seam import ModelRun

logger = logging.getLogger(__name__)

#: Per feed of the source, its latest stored run: one ``idx_samples_runs``
#: seek each. ``MAX`` over the text is the latest run because every stored
#: run id is ``<model>:`` plus a whole-second canonical stamp.
STORED_RUNS_SQL: Final = """
SELECT f.model,
       (SELECT MAX(fs.model_run_id) FROM forecast_samples fs
        WHERE fs.site_id = ? AND fs.feed_id = f.id) AS latest
FROM feeds f
WHERE f.source = ? AND f.is_virtual = 0
"""

#: Every character a model token may keep; any other becomes ``_``.
_MODEL_UNSAFE_RE: Final = re.compile(r"[^A-Za-z0-9_.-]")
_MODEL_MAX_CHARS: Final = 40


def run_lines(
    *,
    source: str,
    call_at: str,
    call_local: str,
    site_id: int,
    runs: Sequence[ModelRun],
    stored: Mapping[str, str | None] | None,
) -> list[str]:
    """One log line per run, in the order of ``runs``. Pure.

    ``stored`` maps each feed's raw model name to its latest stored
    ``model_run_id`` (``None`` when the feed has no rows); ``stored=None``
    means the read failed. ``prev`` is the stored run's stamp; ``none`` when
    the model has no stored run; ``unread`` when the read failed or the
    stored id is not ``<model>:`` plus a parseable UTC stamp. ``new`` is
    ``yes`` when the run is later than ``prev``, ``no`` when it is the same
    or earlier, and ``unknown`` when ``prev`` is ``none`` or ``unread``;
    the two are compared as instants, not as text. ``call_local`` is written
    as given, last on the line, as ``local``.
    """
    lines: list[str] = []
    for run in runs:
        prev, prev_at = _previous_run(run.model, stored)
        run_at = _instant(run.run_at)
        if prev_at is None or run_at is None:
            new = "unknown"
        elif run_at > prev_at:
            new = "yes"
        else:
            new = "no"
        lines.append(
            f"{source} run: call={call_at} site={site_id}"
            f" model={_model_token(run.model)} run={run.run_at}"
            f" updated={run.updated} prev={prev} new={new} local={call_local}"
        )
    return lines


async def log_model_runs(
    db: Database,
    *,
    source: str,
    site_id: int,
    runs: Sequence[ModelRun],
    call_at: str,
) -> None:
    """Log one INFO line per run (``run_lines``); never raises an ``Exception``.

    Reads the latest stored run of each feed of ``source`` at ``site_id``
    first. A failed read logs one WARNING and the lines go on with every
    ``prev`` ``unread``. The call's site time is a read of its own: if it
    fails, one WARNING and the lines go on with ``local`` ``unread``. Any
    other failure logs one WARNING and writes no line. Only the exception's
    type name is logged.
    """
    try:
        stored: dict[str, str | None] | None
        try:
            stored = await db.read(
                lambda conn: _stored_runs(conn, site_id=site_id, source=source)
            )
        except Exception as exc:
            logger.warning(
                "%s run log: stored runs not read (%s)", source, type(exc).__name__
            )
            stored = None
        call_local: str
        try:
            call_local = await db.read(
                lambda conn: _local_stamp(conn, site_id=site_id, call_at=call_at)
            )
        except Exception as exc:
            logger.warning(
                "%s run log: site time not read (%s)", source, type(exc).__name__
            )
            call_local = "unread"
        for line in run_lines(
            source=source,
            call_at=call_at,
            call_local=call_local,
            site_id=site_id,
            runs=runs,
            stored=stored,
        ):
            logger.info("%s", line)
    except Exception as exc:
        logger.warning("%s run log: not written (%s)", source, type(exc).__name__)


def _stored_runs(
    conn: sqlite3.Connection, *, site_id: int, source: str
) -> dict[str, str | None]:
    """``{model: latest stored model_run_id or None}`` for every real feed of
    ``source``. Read-only."""
    rows = conn.execute(STORED_RUNS_SQL, (site_id, source)).fetchall()
    return {str(row[0]): None if row[1] is None else str(row[1]) for row in rows}


def _local_stamp(conn: sqlite3.Connection, *, site_id: int, call_at: str) -> str:
    """``call_at`` as the site's wall time, in whole seconds. Read-only.

    The zone is that of the stored day holding the call, else
    ``sites.timezone``. A site that does not exist gives ``unread``; a zone
    that does not load raises.
    """
    record = day_for_instant(conn, site_id, call_at)
    if record is not None:
        zone = record.timezone
    else:
        row = conn.execute(
            "SELECT timezone FROM sites WHERE id = ?", (site_id,)
        ).fetchone()
        if row is None:
            return "unread"
        zone = str(row[0])
    local = parse_utc(call_at).astimezone(ZoneInfo(zone)).replace(tzinfo=None)
    return local.isoformat(timespec="seconds")


def _previous_run(
    model: str, stored: Mapping[str, str | None] | None
) -> tuple[str, datetime | None]:
    """The ``prev`` token and, when it is a stamp, its instant."""
    if stored is None:
        return "unread", None
    run_id = stored.get(model)
    if run_id is None:
        return "none", None
    prefix = f"{model}:"
    if not run_id.startswith(prefix):
        return "unread", None
    instant = _instant(run_id[len(prefix) :])
    if instant is None:
        return "unread", None
    return isoformat_utc(instant), instant


def _instant(text: str) -> datetime | None:
    """``text`` as a UTC instant, or ``None`` when it does not parse."""
    try:
        return parse_utc(text)
    except ValueError:
        return None


def _model_token(model: str) -> str:
    """The model name with every character outside ``A-Za-z0-9_.-``
    replaced by ``_``, cut to 40 characters."""
    return _MODEL_UNSAFE_RE.sub("_", model)[:_MODEL_MAX_CHARS]
