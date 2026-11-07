"""The hourly check for a wind station stuck at one value, and its episodes.

``assess_constant_wind`` runs in ``scheduler_tick``'s transaction and never
commits. Once per whole UTC hour it reads the stored raw readings of every
assessed station, judges each one with ``wxverify.obs.constant_wind``, and
keeps one ``station_wind_constant`` row per open episode; the monitor turns
each row into a pipeline condition. On every other tick it reads one
``runtime_state`` key and nothing else. It is a warning only: nothing here
changes what the blend uses.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from datetime import datetime, timedelta
from typing import Final

from wxverify.core.timeutil import floor_hour, isoformat_utc, parse_utc
from wxverify.core.units import ms_to_kmh
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.station_readings import raw_readings
from wxverify.obs.constant_wind import (
    Episode,
    EpisodeState,
    HourWinds,
    Rule,
    assess,
    assessable,
    hour_winds,
    reporting_others,
)
from wxverify.settings.keys import get_number_setting

logger = logging.getLogger(__name__)

#: The ``runtime_state`` key holding the end of the last assessed hour, a
#: canonical UTC stamp; the hours before it are assessed.
ASSESSED_THROUGH_KEY: Final = "wind_constant_assessed_through"

#: The three settings the add-on options write, each with its default and the
#: minimum the options schema allows; a lower stored value reads the default.
HOURS_KEY: Final = "wind_constant_hours"
OTHER_STATIONS_KEY: Final = "wind_constant_other_stations"
MEDIAN_MS_KEY: Final = "wind_constant_median_ms"
_HOURS_DEFAULT: Final = 6
_HOURS_MINIMUM: Final = 2
_OTHER_STATIONS_DEFAULT: Final = 3
_OTHER_STATIONS_MINIMUM: Final = 1
_MEDIAN_MS_DEFAULT: Final = 1
_MEDIAN_MS_MINIMUM: Final = 0

#: Lets an hour's last current-conditions polls land; 5-minute history may come later.
_LAG: Final = timedelta(minutes=15)
#: The most hours one run assesses as new.
_MAX_NEW_HOURS: Final = 48
_ONE_HOUR: Final = timedelta(hours=1)
_STATES: Final[dict[str, EpisodeState]] = {
    "suspected": "suspected",
    "cant_assess": "cant_assess",
}

_ASSESSED_STATIONS_SQL: Final = """
    SELECT st.id, s.id AS site_id
    FROM stations st
    JOIN sites s ON s.id = st.site_id
    WHERE st.enabled = 1 AND st.retired_at IS NULL AND s.enabled = 1
    ORDER BY st.site_id, st.id
"""

_UPSERT_SQL: Final = """
    INSERT INTO station_wind_constant (station_id, speed_kmh, since, state)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(station_id) DO UPDATE SET
        speed_kmh = excluded.speed_kmh,
        since = excluded.since,
        state = excluded.state
"""


def read_rule(conn: sqlite3.Connection) -> Rule:
    """The rule's settings; the median from m/s to km/h."""
    median_ms = get_number_setting(
        conn, MEDIAN_MS_KEY, _MEDIAN_MS_DEFAULT, minimum=_MEDIAN_MS_MINIMUM
    )
    return Rule(
        hours=get_number_setting(
            conn, HOURS_KEY, _HOURS_DEFAULT, minimum=_HOURS_MINIMUM
        ),
        other_stations=get_number_setting(
            conn,
            OTHER_STATIONS_KEY,
            _OTHER_STATIONS_DEFAULT,
            minimum=_OTHER_STATIONS_MINIMUM,
        ),
        median_kmh=ms_to_kmh(median_ms),
    )


def _assessed_through(conn: sqlite3.Connection) -> datetime | None:
    """The stored end of the last assessed hour; None when absent or unreadable.

    An unreadable value is logged at WARNING; the run that sees it writes a
    readable one.
    """
    raw = get_runtime_state(conn, ASSESSED_THROUGH_KEY)
    if raw is None:
        return None
    try:
        return parse_utc(raw)
    except ValueError:
        logger.warning(
            "wind constant check: unreadable %s; assessing as if absent",
            ASSESSED_THROUGH_KEY,
        )
        return None


def _hours(start: datetime, end: datetime) -> list[datetime]:
    """The whole hours from ``start`` (included) to ``end`` (excluded)."""
    return [start + index * _ONE_HOUR for index in range((end - start) // _ONE_HOUR)]


def _episode_rows(conn: sqlite3.Connection) -> tuple[set[int], dict[int, Episode]]:
    """The stations that have a row, and each readable row as an ``Episode``.

    A row with an unreadable value or stamp is no episode; its station's row
    is then replaced or deleted. Such rows are logged once at WARNING.
    """
    present: set[int] = set()
    episodes: dict[int, Episode] = {}
    unreadable = 0
    for row in conn.execute(
        "SELECT station_id, speed_kmh, since, state FROM station_wind_constant"
    ).fetchall():
        station_id = int(row["station_id"])
        present.add(station_id)
        speed: object = row["speed_kmh"]
        since: object = row["since"]
        state = _STATES.get(str(row["state"]))
        if (
            isinstance(speed, bool)
            or not isinstance(speed, int | float)
            or not math.isfinite(speed)
            or not isinstance(since, str)
            or state is None
        ):
            unreadable += 1
            continue
        try:
            since_at = parse_utc(since)
        except ValueError:
            unreadable += 1
            continue
        episodes[station_id] = Episode(
            speed_kmh=float(speed), since=since_at, state=state
        )
    if unreadable:
        logger.warning(
            "wind constant check: unreadable episode rows ignored count=%d",
            unreadable,
        )
    return present, episodes


def assess_constant_wind(conn: sqlite3.Connection, *, now_utc: datetime) -> None:
    """Assess every assessed station's new whole hours, at most once an hour.

    ``end`` is the start of the hour 15 minutes before ``now_utc``; the hours
    before it are assessed. When the stored ``wind_constant_assessed_through``
    is at least ``end`` nothing else is read or written. Otherwise the new
    hours run from the stored value (absent: ``end`` less the rule's hours)
    to ``end``, at most 48 of them, and the no-episode window is the rule's
    hours before ``end``. An assessed station is enabled, not retired, of an
    enabled site; its others are the other assessed stations of its site.
    Each station's row is written, updated or deleted by ``assess``; the rows
    of stations that are no longer assessed are deleted. Last, ``end`` is
    stored and one INFO line sums the run up. Runs in the caller's
    transaction.
    """
    end = floor_hour(now_utc - _LAG)
    stored = _assessed_through(conn)
    if stored is not None and stored >= end:
        return
    rule = read_rule(conn)
    window_start = end - rule.hours * _ONE_HOUR
    start = window_start if stored is None else floor_hour(stored)
    start = max(start, end - _MAX_NEW_HOURS * _ONE_HOUR)
    new_hours = _hours(start, end)
    window = _hours(window_start, end)
    read_start = min(start, window_start)

    sites: dict[int, list[int]] = {}
    for row in conn.execute(_ASSESSED_STATIONS_SQL).fetchall():
        sites.setdefault(int(row["site_id"]), []).append(int(row["id"]))
    present, episodes = _episode_rows(conn)

    assessed_hours = 0
    too_few: list[int] = []
    for station_ids in sites.values():
        winds: dict[int, HourWinds] = {
            station_id: hour_winds(
                raw_readings(
                    conn, station_id=station_id, start_utc=read_start, end_utc=end
                )
            )
            for station_id in station_ids
        }
        for station_id in station_ids:
            others = [winds[other] for other in station_ids if other != station_id]
            assessed_hours += sum(
                1 for hour in new_hours if assessable(others, hour, rule=rule)
            )
            if any(
                reporting_others(others, hour) < rule.other_stations
                for hour in new_hours
            ):
                too_few.append(station_id)
            episode = episodes.get(station_id)
            result = assess(
                winds[station_id],
                others,
                window=window,
                new_hours=new_hours,
                episode=episode,
                rule=rule,
            )
            _store(conn, station_id, present=present, episode=episode, result=result)

    assessed = [station_id for ids in sites.values() for station_id in ids]
    placeholders = ", ".join("?" for _ in assessed)
    conn.execute(
        f"DELETE FROM station_wind_constant WHERE station_id NOT IN ({placeholders})",
        assessed,
    )
    set_runtime_state(conn, ASSESSED_THROUGH_KEY, isoformat_utc(end))
    logger.info(
        "wind constant check through=%s: %d station-hour(s) assessed,"
        " too few other stations: %s",
        isoformat_utc(end),
        assessed_hours,
        ", ".join(str(station_id) for station_id in too_few) or "none",
    )


def _store(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    present: set[int],
    episode: Episode | None,
    result: Episode | None,
) -> None:
    """Write one station's ``assess`` result to its row, and log the change.

    A new episode is written and logged at WARNING; a changed one is
    updated; a cleared one is deleted and logged at INFO.
    """
    if result is None:
        if station_id in present:
            conn.execute(
                "DELETE FROM station_wind_constant WHERE station_id = ?",
                (station_id,),
            )
        if episode is not None:
            logger.info(
                "wind constant reading cleared station=%s speed_kmh=%s since=%s",
                station_id,
                episode.speed_kmh,
                isoformat_utc(episode.since),
            )
        return
    if result == episode:
        return
    conn.execute(
        _UPSERT_SQL,
        (station_id, result.speed_kmh, isoformat_utc(result.since), result.state),
    )
    if episode is None:
        logger.warning(
            "wind constant reading suspected station=%s speed_kmh=%s since=%s",
            station_id,
            result.speed_kmh,
            isoformat_utc(result.since),
        )
