"""Post-release coverage check for the Open-Meteo ``icon_eu`` feed (owner-run).

Looks at every forward ``icon_eu`` fetch the add-on has stored and asks one
question: did each fetch store usable, gap-free coverage of at least 96 hours
for every variable? It measures usable coverage only, not which model run
supplied it or whether runs are stitched.

Run from the add-on directory (``uv sync`` does not install the package)::

    PYTHONPATH=. uv run python scripts/icon_eu_post_release_check.py \
        --db <exported-wxverify.db>

``--db`` is a Database Export copy. Only the owner runs this script, 7 days
after the add-on first runs the release that adds ``icon_eu``, then weekly
while the verdict is CAN'T TELL. It sends no request and writes nothing: the
file is opened read-only, and only ``forecast_samples`` joined to ``feeds`` is
read, never ``sites``.

Only fixed lines print: none carries a coordinate, a station, the database
path or exception text. Exit codes: 0 PASS, 1 FAIL, 3 CAN'T TELL, 2 after an
``error:`` line.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

from wxverify.collection.forecast_validation import invalid_forecast_sample_sql
from wxverify.core.timeutil import floor_hour, parse_utc

VARIABLES: Final[tuple[str, ...]] = ("temperature", "wind", "precip")
MIN_HOURS: Final = 96
MIN_IN_WINDOW_FETCHES: Final = 8
#: A stored row younger than this at storage time is a forward row; history
#: rows from backfill and catch-up are at least 24 h old.
FORWARD_AGE: Final = timedelta(hours=12)

_HOUR: Final = timedelta(hours=1)
_WINDOW_START_HOURS: Final = (1, 7, 13, 19)
_WINDOW_LENGTH: Final = timedelta(minutes=90)

QUERY: Final = f"""
SELECT fs.site_id, fs.issued_at, fs.valid_at, fs.variable, fs.lead_hours,
       fs.fetched_at
FROM forecast_samples AS fs
JOIN feeds AS f ON f.id = fs.feed_id
WHERE f.source = 'open-meteo' AND f.model = 'icon_eu'
  AND fs.fetched_at IS NOT NULL
  AND NOT {invalid_forecast_sample_sql("fs")}
"""

#: (site_id, issued_at, valid_at, variable, lead_hours, fetched_at), as read.
Row = tuple[object, object, object, object, object, object]


@dataclass(frozen=True)
class Fetch:
    """One qualifying forward fetch: a (site, label) group with one stamp."""

    site_id: int
    label: datetime
    fetched: datetime
    in_window: bool
    full: bool
    hours: int
    max_lead: int


def in_side_window(now: datetime) -> bool:
    """Whether ``now`` is in [01:00, 02:30), [07:00, 08:30), [13:00, 14:30)
    or [19:00, 20:30) UTC.

    A local copy of the coverage script's rule of the same name: importing
    that script would load ``httpx``. A test pins the two minute by minute.
    """
    current = now.replace(tzinfo=UTC) if now.tzinfo is None else now.astimezone(UTC)
    midnight = current.replace(hour=0, minute=0, second=0, microsecond=0)
    since_midnight = current - midnight
    return any(
        timedelta(hours=hour) <= since_midnight < timedelta(hours=hour) + _WINDOW_LENGTH
        for hour in _WINDOW_START_HOURS
    )


def _stamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return parse_utc(value)
    except ValueError:
        return None


def _int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _minute(value: datetime) -> str:
    """``YYYY-MM-DDTHH:MM`` in UTC, zero-padded whatever the year."""
    v = value.astimezone(UTC)
    return f"{v.year:04d}-{v.month:02d}-{v.day:02d}T{v.hour:02d}:{v.minute:02d}"


def _is_forward(row: Row) -> bool:
    """Whether a row is kept for grouping.

    A row whose ``issued_at`` or ``fetched_at`` does not parse cannot be aged,
    so it is kept: its group is then excluded as unparseable.
    """
    issued = _stamp(row[1])
    fetched = _stamp(row[5])
    if issued is None or fetched is None:
        return True
    return fetched - issued < FORWARD_AGE


def _is_full(stamps: Sequence[datetime]) -> bool:
    """Whole hours (minute, second and microsecond zero), at least 96 of them,
    and no gap between first and last."""
    if len(stamps) < MIN_HOURS:
        return False
    if any(
        stamp.minute != 0 or stamp.second != 0 or stamp.microsecond != 0
        for stamp in stamps
    ):
        return False
    span = int((max(stamps) - min(stamps)) // _HOUR) + 1
    return len(stamps) == span


def _hours_after(stamps: Sequence[datetime], base: datetime) -> int:
    if not stamps:
        return 0
    return max(0, int((max(stamps) - base) // _HOUR))


def _fetch(
    site_id: int, label: datetime, fetched: datetime, members: Sequence[Row]
) -> Fetch:
    by_variable: dict[str, list[datetime]] = {variable: [] for variable in VARIABLES}
    leads: list[int] = []
    for row in members:
        valid = _stamp(row[2])
        variable = row[3]
        if valid is not None and isinstance(variable, str) and variable in by_variable:
            by_variable[variable].append(valid)
        lead = _int(row[4])
        if lead is not None:
            leads.append(lead)
    base = floor_hour(fetched)
    return Fetch(
        site_id=site_id,
        label=label,
        fetched=fetched,
        in_window=in_side_window(fetched),
        full=all(_is_full(by_variable[variable]) for variable in VARIABLES),
        hours=min(_hours_after(by_variable[variable], base) for variable in VARIABLES),
        max_lead=max(leads, default=0),
    )


def qualifying_fetches(rows: Iterable[Row]) -> tuple[list[Fetch], int]:
    """Group forward rows into fetches; returns (fetches, excluded labels).

    Rows are grouped by (site, label = ``issued_at``). A group is excluded
    when its rows carry more than one distinct ``fetched_at`` (two fetches
    under one label keep a mix of rows) or when any of its stamps does not
    parse. The fetches are ordered by fetch time, then site.
    """
    groups: dict[tuple[object, object], list[Row]] = {}
    for row in rows:
        if _is_forward(row):
            groups.setdefault((row[0], row[1]), []).append(row)
    fetches: list[Fetch] = []
    excluded = 0
    for (site_value, label_value), members in groups.items():
        site_id = _int(site_value)
        label = _stamp(label_value)
        fetched_values = {row[5] for row in members}
        fetched = (
            _stamp(next(iter(fetched_values))) if len(fetched_values) == 1 else None
        )
        if (
            site_id is None
            or label is None
            or fetched is None
            or any(_stamp(row[2]) is None for row in members)
        ):
            excluded += 1
            continue
        fetches.append(_fetch(site_id, label, fetched, members))
    fetches.sort(key=lambda fetch: (fetch.fetched, fetch.site_id))
    return fetches, excluded


def verdict(fetches: Sequence[Fetch]) -> tuple[str, str, int]:
    """(verdict, reason, exit code), pooled across all sites."""
    if any(not fetch.full for fetch in fetches):
        return "FAIL", "short coverage", 1
    if sum(1 for fetch in fetches if fetch.in_window) >= MIN_IN_WINDOW_FETCHES:
        return "PASS", "coverage ok", 0
    return "CAN'T TELL", "too few samples", 3


def _read_rows(db: str) -> list[Row] | None:
    try:
        uri = Path(db).resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        try:
            rows: list[Row] = conn.execute(QUERY).fetchall()
        finally:
            conn.close()
    except (sqlite3.Error, OSError, ValueError):
        return None
    return rows


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"


def main(argv: Sequence[str] | None = None) -> int:
    """Run the check; returns the exit code."""
    parser = argparse.ArgumentParser(
        description="Owner-run icon_eu post-release coverage check (read-only)."
    )
    parser.add_argument("--db", required=True, help="Database Export copy")
    args = parser.parse_args(argv)
    rows = _read_rows(str(args.db))
    if rows is None:
        print("error: database unreadable", file=sys.stderr, flush=True)
        return 2
    fetches, excluded = qualifying_fetches(rows)
    for fetch in fetches:
        print(
            f"post-release fetch: site {fetch.site_id}"
            f" fetched {_minute(fetch.fetched)} label {_minute(fetch.label)}"
            f" window {_yes_no(fetch.in_window)} full {_yes_no(fetch.full)}"
            f" hours {fetch.hours} max lead {fetch.max_lead}"
        )
    in_window = sum(1 for fetch in fetches if fetch.in_window)
    print(f"post-release qualifying fetches: {len(fetches)}")
    print(f"post-release in-window fetches: {in_window}")
    print(f"post-release excluded labels: {excluded}")
    result, reason, code = verdict(fetches)
    print(f"post-release verdict: {result} ({reason})", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
