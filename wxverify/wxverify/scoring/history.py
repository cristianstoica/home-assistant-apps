"""Persistence's one shared definition, and the readers of the forecast
history.

Persistence, the yardstick that forecasts an hour with an earlier hour's own
value, has one definition here: ``persistence_cells``, which stage 5's day
scores (``scoring.close_scores.score_day``) use at every day ahead and lead.
Its two picks, ``shortest_lead_cells`` and ``longest_lead_cells``, define the
readers' pick. The readers that rebuild persistence on read (the win rate,
the history chart) need one pick at one day ahead, so they call
``persistence_picks_at``, which gives the same cells as ``persistence_cells``
kept at that day ahead and then picked, so the day scores and the readers
agree. These functions touch no database, clock or file.

The two readers, ``winrate_errors`` (the win rate) and ``chart_pairs`` (the
history chart), read ``forecast_history`` + ``observations`` and work the
errors out on read: a site's ``closed`` days and its pre-close days
(no record, dated before its first record) together, never a ``missed`` day
or one not yet done. Persistence is rebuilt from ``observations``:
on a closed day only where its persistence ``day_scores`` row of the
variable and day ahead is ``scored``, before the first record at every hour
with an observation; the win rate takes the shortest lead, the chart the
longest. Both read inside the caller's read and never write.
"""

from __future__ import annotations

import sqlite3
from bisect import bisect_left, bisect_right
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify.core.timeutil import parse_utc
from wxverify.db.switch import first_record_date
from wxverify.scoring.effective import active_feed_cte

#: Persistence's longest lead in hours; leads run from 1 to this.
PERSISTENCE_MAX_LEAD_HOURS: Final = 168
# A cell is kept only at day ahead 0 to 7.
_MAX_DAY_AHEAD: Final = 7
# The whole days persistence's sources reach back from a target hour.
_SOURCE_DAYS: Final = -(-PERSISTENCE_MAX_LEAD_HOURS // 24)
# The pre-close bound while a site has no record yet: every date is before
# it, so all of its history is pre-close. A real date, never NULL:
# ``local_date < NULL`` would drop every pre-close row.
_NO_RECORD_BOUND: Final = date(9999, 12, 31)

# The win rate's one read: one site, variable and day ahead, each
# active feed's stored forecast against the hour's observation, closed and
# pre-close days together. The primary key makes each hour one row, so
# nothing is ranked. The ``valid_at`` bounds are the window's dates widened
# by one day each side and only let the key seek; ``local_date`` decides. A
# date counts when its record is ``closed``, or when it has no record and
# lies before the pre-close bound (a ``missed`` date has a record).
# Binds: the two of ``active_feed_cte()``, then site, variable, day ahead,
# the two ``valid_at`` bounds, the window's first and last date, the
# pre-close bound. ``CROSS JOIN`` keeps the feeds the outer loop, so each
# feed seeks the whole key; with a plain JOIN the planner (no statistics)
# put ``forecast_history`` outside and sought ``site_id`` alone. The table
# keeps its own name, as the query plan names it.
_WINRATE_RANGE_SQL: Final = f"""
    WITH {active_feed_cte()}
    SELECT forecast_history.feed_id, f.source, f.model,
           forecast_history.valid_at,
           abs(forecast_history.value - o.value) AS abs_error
    FROM active_feeds a
    CROSS JOIN forecast_history
      ON forecast_history.site_id = ?
     AND forecast_history.feed_id = a.feed_id
     AND forecast_history.variable = ?
     AND forecast_history.day_ahead = ?
     AND forecast_history.valid_at >= ? AND forecast_history.valid_at < ?
    JOIN observations o
      ON o.site_id = forecast_history.site_id
     AND o.variable = forecast_history.variable
     AND o.valid_at = forecast_history.valid_at
    JOIN feeds f ON f.id = forecast_history.feed_id
    LEFT JOIN site_days sd
      ON sd.site_id = forecast_history.site_id
     AND sd.local_date = forecast_history.local_date
    WHERE forecast_history.local_date BETWEEN ? AND ?
      AND (sd.state = 'closed'
           OR (sd.site_id IS NULL AND forecast_history.local_date < ?))
"""
# The history chart's one range read: one feed, variable and day
# ahead, with the win rate's two joins and its closed/pre-close condition.
# The ``valid_at`` bounds only let the key seek; the caller's instants
# decide. Binds: site, feed, variable, day ahead, the two ``valid_at``
# bounds, the pre-close bound.
_CHART_RANGE_SQL: Final = """
    SELECT forecast_history.valid_at, forecast_history.value AS forecast,
           o.value AS observed
    FROM forecast_history
    JOIN observations o
      ON o.site_id = forecast_history.site_id
     AND o.variable = forecast_history.variable
     AND o.valid_at = forecast_history.valid_at
    LEFT JOIN site_days sd
      ON sd.site_id = forecast_history.site_id
     AND sd.local_date = forecast_history.local_date
    WHERE forecast_history.site_id = ? AND forecast_history.feed_id = ?
      AND forecast_history.variable = ? AND forecast_history.day_ahead = ?
      AND forecast_history.valid_at >= ? AND forecast_history.valid_at < ?
      AND (sd.state = 'closed'
           OR (sd.site_id IS NULL AND forecast_history.local_date < ?))
"""
# The closed dates whose persistence ``day_scores`` row of one variable and
# day ahead is ``scored``, with their bounds and zone. Binds: the
# persistence feed, variable, day ahead, site, first and last date.
_SCORED_PERSISTENCE_DAYS_SQL: Final = """
    SELECT sd.day_start_utc, sd.day_end_utc, sd.timezone
    FROM site_days sd
    JOIN day_scores ds
      ON ds.site_id = sd.site_id AND ds.local_date = sd.local_date
     AND ds.feed_id = ? AND ds.variable = ? AND ds.day_ahead = ?
    WHERE sd.site_id = ? AND sd.state = 'closed'
      AND sd.local_date BETWEEN ? AND ? AND ds.status = 'scored'
"""
_OBSERVATIONS_SQL: Final = (
    "SELECT valid_at, value FROM observations"
    " WHERE site_id = ? AND variable = ? AND valid_at >= ? AND valid_at < ?"
)
_PERSISTENCE_FEED_SQL: Final = (
    "SELECT id, source, model FROM feeds"
    " WHERE source = 'virtual' AND model = '_persistence'"
)
# The zone of the site's published time zone generation, the pre-close
# days' zone; the site's own zone while it has none.
_PRE_CLOSE_ZONE_SQL: Final = """
    SELECT COALESCE(
        (SELECT g.timezone FROM timezone_generations g
         WHERE g.site_id = s.id AND g.state = 'published'
         ORDER BY g.id DESC LIMIT 1),
        s.timezone) AS timezone
    FROM sites s
    WHERE s.id = ?
"""


@dataclass(frozen=True, slots=True)
class PersistenceCell:
    """One persistence cell: the value ``lead_hours`` before the target hour
    ``instant`` stands in for a forecast of ``observed``, at day ahead
    ``day_ahead``.

    ``valid_at`` and ``source_valid_at`` are the target's and the source's
    own text as found in the rows, whatever text the target was asked by.
    """

    instant: datetime
    valid_at: str
    source_valid_at: str
    lead_hours: int
    day_ahead: int
    forecast: float
    observed: float


@dataclass(frozen=True, slots=True)
class HistoryError:
    """One feed's absolute error at one hour, as the win rate tallies it.

    ``valid_at`` is the hour's ``observations`` text, the same for every
    feed at that hour.
    """

    feed_id: int
    source: str
    model: str
    valid_at: str
    abs_error: float


def persistence_cells(
    rows: Iterable[tuple[str, float]],
    *,
    target_valid_at: Iterable[str],
    zone: ZoneInfo,
    max_lead_hours: int = PERSISTENCE_MAX_LEAD_HOURS,
) -> list[PersistenceCell]:
    """Persistence's cells for the requested target hours of one variable.

    ``rows`` are one hourly variable's ``(valid_at, value)`` pairs in any
    order and any ``parse_utc`` text form; rows and targets are matched by
    parsed instant, never by text, and a later row at an instant replaces an
    earlier one. For each target instant h with a row, and each lead L from
    1 to ``max_lead_hours`` with a row at h - L, one cell at day ahead =
    local date of h minus local date of h - L in ``zone``, kept only at day
    ahead 0 to 7. A target or source with no row gives no cell; a text that
    does not parse is left out, like a row with no value; a target asked
    twice (in any text form) is counted once. The cells come in no promised
    order. Pure: no I/O.
    """
    values: dict[datetime, tuple[str, float]] = {}
    for text, value in rows:
        try:
            instant = parse_utc(text)
        except ValueError:
            continue
        values[instant] = (text, value)
    targets: set[datetime] = set()
    for text in target_valid_at:
        try:
            targets.add(parse_utc(text))
        except ValueError:
            continue

    cells: list[PersistenceCell] = []
    for instant in sorted(targets):
        target = values.get(instant)
        if target is None:
            continue
        valid_at, observed = target
        target_date = instant.astimezone(zone).date()
        for lead in range(1, max_lead_hours + 1):
            source_instant = instant - timedelta(hours=lead)
            source = values.get(source_instant)
            if source is None:
                continue
            day_ahead = (target_date - source_instant.astimezone(zone).date()).days
            if not 0 <= day_ahead <= _MAX_DAY_AHEAD:
                continue
            source_valid_at, forecast = source
            cells.append(
                PersistenceCell(
                    instant=instant,
                    valid_at=valid_at,
                    source_valid_at=source_valid_at,
                    lead_hours=lead,
                    day_ahead=day_ahead,
                    forecast=forecast,
                    observed=observed,
                )
            )
    return cells


def shortest_lead_cells(
    cells: Iterable[PersistenceCell],
) -> dict[tuple[datetime, int], PersistenceCell]:
    """Per (target instant, day ahead), the cell with the smallest
    ``lead_hours``: the win rate's pick. Pure."""
    return _pick_by_lead(cells, longest=False)


def longest_lead_cells(
    cells: Iterable[PersistenceCell],
) -> dict[tuple[datetime, int], PersistenceCell]:
    """Per (target instant, day ahead), the cell with the largest
    ``lead_hours``: the history chart's pick. Pure."""
    return _pick_by_lead(cells, longest=True)


def _pick_by_lead(
    cells: Iterable[PersistenceCell], *, longest: bool
) -> dict[tuple[datetime, int], PersistenceCell]:
    """One cell per (target instant, day ahead), by lead; on a tie the first
    cell seen stays."""
    picked: dict[tuple[datetime, int], PersistenceCell] = {}
    for cell in cells:
        key = (cell.instant, cell.day_ahead)
        current = picked.get(key)
        if (
            current is None
            or (longest and cell.lead_hours > current.lead_hours)
            or (not longest and cell.lead_hours < current.lead_hours)
        ):
            picked[key] = cell
    return picked


def persistence_picks_at(
    rows: Iterable[tuple[str, float]],
    *,
    target_valid_at: Iterable[str],
    zone: ZoneInfo,
    day_ahead: int,
    longest: bool,
) -> list[PersistenceCell]:
    """Persistence's one cell per requested target hour at ``day_ahead``, at
    its longest lead (``longest``) or its shortest, oldest first.

    The same cells, in instant order, as ``longest_lead_cells`` (or
    ``shortest_lead_cells``) of the ``persistence_cells`` cells at
    ``day_ahead``, with no cell built at any other lead or day ahead. Rows
    and targets are read as there: matched by parsed instant, a later row
    at an instant replaces an earlier one, a text that does not parse is
    left out, a target asked twice is counted once. Each instant's local
    date in ``zone`` is worked out once. A target h with a row, at local
    date D, looks only at the instants dated D - ``day_ahead``: back from
    the last before h (shortest) or forward from the first at or after h -
    ``PERSISTENCE_MAX_LEAD_HOURS`` hours (longest), and picks the first
    instant s with h - s a whole number of hours from 1 to
    ``PERSISTENCE_MAX_LEAD_HOURS``, at that lead. A day ahead outside 0 to 7
    gives no cell. Pure: no I/O.
    """
    if not 0 <= day_ahead <= _MAX_DAY_AHEAD:
        return []
    values: dict[datetime, tuple[str, float]] = {}
    for text, value in rows:
        try:
            instant = parse_utc(text)
        except ValueError:
            continue
        values[instant] = (text, value)
    targets: set[datetime] = set()
    for text in target_valid_at:
        try:
            targets.add(parse_utc(text))
        except ValueError:
            continue
    # Each instant's local date, and each local date's instants, oldest first.
    local_dates: dict[datetime, date] = {}
    by_date: dict[date, list[datetime]] = {}
    for instant in values:
        local = instant.astimezone(zone).date()
        local_dates[instant] = local
        by_date.setdefault(local, []).append(instant)
    for instants in by_date.values():
        instants.sort()

    zero = timedelta(0)
    hour = timedelta(hours=1)
    max_lead = timedelta(hours=PERSISTENCE_MAX_LEAD_HOURS)
    days_back = timedelta(days=day_ahead)
    cells: list[PersistenceCell] = []
    for instant in sorted(targets):
        target = values.get(instant)
        if target is None:
            continue
        sources = by_date.get(local_dates[instant] - days_back)
        if sources is None:
            continue
        # Nearest the pick first: the shortest lead walks back from the last
        # instant before h, the longest forward from the first at or after
        # h - the longest lead; either way the walk ends outside the leads.
        if longest:
            order = range(bisect_left(sources, instant - max_lead), len(sources))
        else:
            order = range(bisect_left(sources, instant) - 1, -1, -1)
        for index in order:
            source_instant = sources[index]
            lead = instant - source_instant
            if not zero < lead <= max_lead:
                break
            if lead % hour != zero:
                continue
            valid_at, observed = target
            source_valid_at, forecast = values[source_instant]
            cells.append(
                PersistenceCell(
                    instant=instant,
                    valid_at=valid_at,
                    source_valid_at=source_valid_at,
                    lead_hours=lead // hour,
                    day_ahead=day_ahead,
                    forecast=forecast,
                    observed=observed,
                )
            )
            break
    return cells


def winrate_errors(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    start: date | None,
    end: date,
) -> list[HistoryError]:
    """Each active feed's error at every counted hour dated ``start`` (None:
    no lower bound) to ``end``, at ``day_ahead``, in no promised order.

    The stored feeds come from ``_WINRATE_RANGE_SQL``; persistence's errors
    are rebuilt at the shortest lead. Read-only.
    """
    low = date.min if start is None else start
    bound = _pre_close_bound(conn, site_id)
    lower, upper = _valid_at_bounds(low, end)
    errors = [
        HistoryError(
            feed_id=int(row["feed_id"]),
            source=str(row["source"]),
            model=str(row["model"]),
            valid_at=str(row["valid_at"]),
            abs_error=float(row["abs_error"]),
        )
        for row in conn.execute(
            _WINRATE_RANGE_SQL,
            (
                site_id,
                site_id,
                site_id,
                variable,
                day_ahead,
                lower,
                upper,
                low.isoformat(),
                end.isoformat(),
                bound.isoformat(),
            ),
        )
    ]
    persistence = conn.execute(_PERSISTENCE_FEED_SQL).fetchone()
    if persistence is not None:
        errors.extend(
            HistoryError(
                feed_id=int(persistence["id"]),
                source=str(persistence["source"]),
                model=str(persistence["model"]),
                valid_at=cell.valid_at,
                abs_error=abs(cell.forecast - cell.observed),
            )
            for cell in _persistence_picks(
                conn,
                site_id=site_id,
                persistence_id=int(persistence["id"]),
                variable=variable,
                day_ahead=day_ahead,
                bound=bound,
                low=low,
                high=end,
                longest=False,
            )
        )
    return errors


def chart_pairs(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    low: datetime | None,
    high: datetime | None,
) -> list[tuple[str, float, float]]:
    """The history chart's ``(valid_at, forecast, observed)`` rows of one
    feed at ``day_ahead``, at every counted hour from ``low`` to
    ``high`` (both included; None: no bound), oldest first.

    A stored feed's rows come from ``_CHART_RANGE_SQL``, kept by parsed
    instant. ``forecast_history`` holds no persistence row, so the
    persistence feed's rows are rebuilt at the longest lead. A
    ``valid_at`` that does not parse is left out. Read-only.
    """
    bound = _pre_close_bound(conn, site_id)
    # The UTC dates of the hours from low to high.
    low_utc = date.min if low is None else low.date()
    high_utc = date.max if high is None else high.date()
    series: list[tuple[datetime, str, float, float]] = []
    persistence = conn.execute(_PERSISTENCE_FEED_SQL).fetchone()
    if persistence is not None and int(persistence["id"]) == feed_id:
        series = [
            (cell.instant, cell.valid_at, cell.forecast, cell.observed)
            for cell in _persistence_picks(
                conn,
                site_id=site_id,
                persistence_id=feed_id,
                variable=variable,
                day_ahead=day_ahead,
                bound=bound,
                # The local dates those hours can fall on.
                low=_shift(low_utc, -1),
                high=_shift(high_utc, 1),
                longest=True,
            )
        ]
    else:
        lower, upper = _valid_at_bounds(low_utc, high_utc)
        for row in conn.execute(
            _CHART_RANGE_SQL,
            (
                site_id,
                feed_id,
                variable,
                day_ahead,
                lower,
                upper,
                bound.isoformat(),
            ),
        ):
            text = str(row["valid_at"])
            try:
                instant = parse_utc(text)
            except ValueError:
                continue
            series.append(
                (instant, text, float(row["forecast"]), float(row["observed"]))
            )
    series.sort(key=lambda item: item[0])
    return [
        (text, forecast, observed)
        for instant, text, forecast, observed in series
        if (low is None or instant >= low) and (high is None or instant <= high)
    ]


def _persistence_picks(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    persistence_id: int,
    variable: str,
    day_ahead: int,
    bound: date,
    low: date,
    high: date,
    longest: bool,
) -> list[PersistenceCell]:
    """Persistence's cells at ``day_ahead`` for the counted hours dated
    ``low`` to ``high``, one per hour at its longest or shortest lead,
    oldest first.

    A closed date's hours count only where its persistence ``day_scores``
    row of the variable and day ahead is ``scored``, in the record's zone;
    a date with no record before ``bound`` (in the generation's zone) at
    every hour with an observation. A zone that does not load gives no cell.
    """
    spans = sorted(
        (
            parse_utc(str(row["day_start_utc"])),
            parse_utc(str(row["day_end_utc"])),
            str(row["timezone"]),
        )
        for row in conn.execute(
            _SCORED_PERSISTENCE_DAYS_SQL,
            (
                persistence_id,
                variable,
                day_ahead,
                site_id,
                low.isoformat(),
                high.isoformat(),
            ),
        )
    )
    starts = [span[0] for span in spans]
    lower, upper = _valid_at_bounds(_shift(low, -_SOURCE_DAYS), high)
    rows = [
        (str(row["valid_at"]), float(row["value"]))
        for row in conn.execute(_OBSERVATIONS_SQL, (site_id, variable, lower, upper))
    ]
    zone_row = conn.execute(_PRE_CLOSE_ZONE_SQL, (site_id,)).fetchone()
    pre_close_zone = None if zone_row is None else str(zone_row["timezone"])
    pre_close = None if pre_close_zone is None else _zone(pre_close_zone)

    targets: dict[str, list[str]] = {}
    for text, _value in rows:
        try:
            instant = parse_utc(text)
        except ValueError:
            continue
        index = bisect_right(starts, instant) - 1
        if index >= 0 and instant < spans[index][1]:
            targets.setdefault(spans[index][2], []).append(text)
        elif pre_close_zone is not None and pre_close is not None:
            local = instant.astimezone(pre_close).date()
            if low <= local <= high and local < bound:
                targets.setdefault(pre_close_zone, []).append(text)

    # Each instant goes to one zone, so the zones' picks never share an hour.
    picked: list[PersistenceCell] = []
    for name, texts in targets.items():
        zone = _zone(name)
        if zone is None:
            continue
        picked.extend(
            persistence_picks_at(
                rows,
                target_valid_at=texts,
                zone=zone,
                day_ahead=day_ahead,
                longest=longest,
            )
        )
    return sorted(picked, key=lambda cell: cell.instant)


def _pre_close_bound(conn: sqlite3.Connection, site_id: int) -> date:
    """The date the pre-close days end before: the site's first record's,
    ``_NO_RECORD_BOUND`` while it has none."""
    first = first_record_date(conn, site_id)
    return _NO_RECORD_BOUND if first is None else first


def _valid_at_bounds(low: date, high: date) -> tuple[str, str]:
    """``valid_at`` text bounds ``[lower, upper)`` for the dates ``low`` to
    ``high`` widened by one day each side: a stored stamp's text
    starts with its UTC date, and a local date's hours fall on the UTC
    dates one before to one after it."""
    return _shift(low, -1).isoformat(), _shift(high, 2).isoformat()


def _shift(day: date, days: int) -> date:
    """``day`` moved by ``days``, held within ``date.min`` and ``date.max``."""
    ordinal = day.toordinal() + days
    return date.fromordinal(
        min(max(ordinal, date.min.toordinal()), date.max.toordinal())
    )


def _zone(name: str) -> ZoneInfo | None:
    """The zone ``name``, or None when it does not load."""
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return None
