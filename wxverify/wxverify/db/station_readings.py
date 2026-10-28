"""The raw station readings in ``station_wind_records``, and their writer.

Every raw reading the add-on keeps is one row, keyed by station, instant and
source: the 5-minute download (``five_minute``), the current-conditions poll
(``current_obs``) and the gap fill's hourly download (``hourly``). A row
holds the reading's own local date, temperature, rain running total and
wind, and when the download that last wrote it returned.

``store_readings`` is the table's one writer apart from the retention delete
and the migration. It runs in the caller's transaction and never commits;
the clock is read only through its ``now_utc`` argument.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from wxverify.core.timeutil import isoformat_utc
from wxverify.db.day_guard_log import warn_refused_instants
from wxverify.db.site_days import WriteKind, filter_day_writes
from wxverify.obs.hourly_values import Quantity, Source, StationReading

logger = logging.getLogger(__name__)

#: The closed-day guard's write kind for each source: the two collection
#: downloads write readings, the gap fill writes after its day has ended.
_SOURCE_KINDS: Final[dict[Source, WriteKind]] = {
    "five_minute": WriteKind.READING,
    "current_obs": WriteKind.READING,
    "hourly": WriteKind.GAP_FILL,
}


@dataclass(frozen=True)
class StoreResult:
    """The counts of one ``store_readings`` call, over the whole call.

    ``kept`` readings added their row or changed one of its values;
    ``already_stored`` readings matched a stored row and changed no value
    (only the row's ``fetched_at`` moved). ``refused`` readings were declined
    by the closed-day guard, or belong to no station; ``dropped`` readings
    carried no value, a value that is not finite, or a negative rain total or
    wind. ``kept_by_quantity`` counts, per quantity, the kept readings that
    carry a value of it; every quantity has a key, zero included.
    """

    kept: int
    already_stored: int
    refused: int
    dropped: int
    kept_by_quantity: dict[Quantity, int]


def _stamp(value: datetime) -> str:
    """``value`` as a canonical whole-second UTC stamp."""
    return isoformat_utc(value.replace(microsecond=0))


def _storable(reading: StationReading) -> bool:
    """Whether ``reading`` can be stored without failing a CHECK of the table.

    It needs at least one value; each value present must be finite, and the
    rain total and the wind at least 0.
    """
    values = (reading.temp_c, reading.precip_total_mm, reading.speed_kmh)
    if all(value is None for value in values):
        return False
    if any(value is not None and not math.isfinite(value) for value in values):
        return False
    return all(
        value is None or value >= 0
        for value in (reading.precip_total_mm, reading.speed_kmh)
    )


def store_readings(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    source: Source,
    readings: Sequence[StationReading],
    fetched_at: datetime,
    now_utc: datetime,
) -> StoreResult:
    """Store one download's readings of one station (plan 6.3, 6.4).

    Each reading's site-day is asked through the closed-day guard's batch
    check (``READING`` for ``five_minute`` and ``current_obs``, ``GAP_FILL``
    for ``hourly``); a refused reading is not stored and is counted, and a
    refused batch is logged once. A reading with no value, a value that is
    not finite, or a negative rain total or wind is dropped and counted, so
    no CHECK of the table can fail the caller's transaction. The rest are
    upserted on ``(station_id, obs_at, source)``: each value column and
    ``local_date`` take the new value where the reading has one and keep the
    stored one otherwise, and ``fetched_at`` takes ``fetched_at``.

    Every row is written with ``source``; a reading whose own ``source``
    differs raises ``ValueError`` before anything is written. Runs in the
    caller's transaction: no BEGIN, no commit.
    """
    kind = _SOURCE_KINDS[source]
    for reading in readings:
        if reading.source != source:
            raise ValueError(
                f"reading source {reading.source!r} differs from the call's {source!r}"
            )
    kept_by_quantity: dict[Quantity, int] = {"temperature": 0, "precip": 0, "wind": 0}
    if not readings:
        return StoreResult(
            kept=0,
            already_stored=0,
            refused=0,
            dropped=0,
            kept_by_quantity=kept_by_quantity,
        )
    station = conn.execute(
        "SELECT site_id FROM stations WHERE id = ?", (station_id,)
    ).fetchone()
    if station is None:
        logger.debug(
            "raw readings refused: no station station=%s source=%s count=%d",
            station_id,
            source,
            len(readings),
        )
        return StoreResult(
            kept=0,
            already_stored=0,
            refused=len(readings),
            dropped=0,
            kept_by_quantity=kept_by_quantity,
        )
    site_id = int(station[0])

    stamps = [_stamp(reading.obs_at) for reading in readings]
    allowed = filter_day_writes(conn, site_id, stamps, kind, now_utc=now_utc)
    refused_stamps = [stamp for stamp in stamps if stamp not in allowed]
    warn_refused_instants(
        logger,
        conn,
        kind,
        writer="store_readings",
        site_id=site_id,
        instants=refused_stamps,
    )

    fetched_stamp = _stamp(fetched_at)
    kept = 0
    already_stored = 0
    dropped = 0
    for reading, stamp in zip(readings, stamps, strict=True):
        if stamp not in allowed:
            continue
        if not _storable(reading):
            dropped += 1
            continue
        values = (reading.temp_c, reading.precip_total_mm, reading.speed_kmh)
        stored = conn.execute(
            "SELECT temp_c, precip_total_mm, speed_kmh FROM station_wind_records"
            " WHERE station_id = ? AND obs_at = ? AND source = ?",
            (station_id, stamp, source),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO station_wind_records (
                station_id, obs_at, source, local_date,
                speed_kmh, temp_c, precip_total_mm, fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (station_id, obs_at, source) DO UPDATE SET
                local_date = COALESCE(excluded.local_date, local_date),
                speed_kmh = COALESCE(excluded.speed_kmh, speed_kmh),
                temp_c = COALESCE(excluded.temp_c, temp_c),
                precip_total_mm = COALESCE(excluded.precip_total_mm, precip_total_mm),
                fetched_at = excluded.fetched_at
            """,
            (
                station_id,
                stamp,
                source,
                None if reading.local_date is None else reading.local_date.isoformat(),
                reading.speed_kmh,
                reading.temp_c,
                reading.precip_total_mm,
                fetched_stamp,
            ),
        )
        # changes() is 1 for every upsert (fetched_at always moves), so the
        # stored values read before the write decide kept from already stored.
        changed = stored is None or any(
            new is not None and new != old
            for new, old in zip(values, (stored[0], stored[1], stored[2]), strict=True)
        )
        if not changed:
            already_stored += 1
            continue
        kept += 1
        if reading.temp_c is not None:
            kept_by_quantity["temperature"] += 1
        if reading.precip_total_mm is not None:
            kept_by_quantity["precip"] += 1
        if reading.speed_kmh is not None:
            kept_by_quantity["wind"] += 1

    refused = len(refused_stamps)
    logger.debug(
        "raw readings stored station=%s source=%s kept=%d already_stored=%d"
        " refused=%d dropped=%d",
        station_id,
        source,
        kept,
        already_stored,
        refused,
        dropped,
    )
    return StoreResult(
        kept=kept,
        already_stored=already_stored,
        refused=refused,
        dropped=dropped,
        kept_by_quantity=kept_by_quantity,
    )
