"""Cluster-consensus producer."""

from __future__ import annotations

import math
import sqlite3
import statistics
from collections.abc import Callable
from datetime import timedelta
from typing import Final

from pydantic import BaseModel, ConfigDict

from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.obs.qc import TARGET_VARIABLES, qc_flag

LAPSE: Final[float] = 0.0065
MAD_TO_SIGMA: Final[float] = 1.4826
MAD_FLOORS: Final[dict[str, float]] = {
    "temperature": 0.5,
    "wind": 1.0,
    "precip": 0.3,
}


class StationReading(BaseModel):
    model_config = ConfigDict(frozen=True)

    station_id: int
    dem_elevation_m: float
    variable: str
    valid_at: str
    value: float


class ConsensusResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    variable: str
    valid_at: str
    value: float
    n_stations: int
    rejected_stations: int


def _p90(values: list[float]) -> float:
    """Linear-interpolated 90th percentile of a non-empty list."""
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    k = (len(s) - 1) * 0.9
    f = math.floor(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def _low_trim(values: list[float]) -> float:
    """Mean after dropping the single lowest value; fewer than 2 -> plain mean."""
    s = sorted(values)
    core = s[1:] if len(s) >= 2 else s
    return sum(core) / len(core)


def _mean(values: list[float]) -> float:
    """Arithmetic mean of a non-empty list."""
    return sum(values) / len(values)


def _median_mad(values: list[float], mad_floor: float) -> tuple[float, int, int] | None:
    """Median of MAD-band inliers; returns (value, n_inliers, n_rejected)."""
    median = statistics.median(values)
    mad = max(statistics.median([abs(value - median) for value in values]), mad_floor)
    band = 3.0 * MAD_TO_SIGMA * mad
    inliers = [value for value in values if abs(value - median) <= band]
    if not inliers:
        return None
    return (
        float(statistics.median(inliers)),
        len(inliers),
        len(values) - len(inliers),
    )


def _wind_estimator(values: list[float], mad_floor: float) -> tuple[float, int, int]:
    """p90 over all stations; no MAD filtering, so ``mad_floor`` is ignored."""
    del mad_floor
    return (_p90(values), len(values), 0)


def _temperature_estimator(
    values: list[float], mad_floor: float
) -> tuple[float, int, int] | None:
    """Median + MAD inlier filtering, unchanged from 0.6.0."""
    return _median_mad(values, mad_floor)


def _precip_estimator(
    values: list[float], mad_floor: float
) -> tuple[float, int, int] | None:
    """Precip consensus — intentionally kept on median/MAD in this release.
    A low-trimmed-mean estimator is implemented and unit-tested but is
    deliberately not wired here until it has been validated against
    reference precipitation data. Switching precip to the validated
    estimator (_low_trim or _mean) is a one-line change to the return
    line below."""
    return _median_mad(values, mad_floor)


# Candidate precip estimators: implemented and unit-tested in this release,
# but NOT wired into _precip_estimator until they have been validated
# against reference precipitation data; a follow-up release picks one.
_GATED_PRECIP_CANDIDATES: Final[tuple[Callable[[list[float]], float], ...]] = (
    _low_trim,
    _mean,
)

_Estimator = Callable[[list[float], float], tuple[float, int, int] | None]

_ESTIMATORS: Final[dict[str, _Estimator]] = {
    "wind": _wind_estimator,
    "temperature": _temperature_estimator,
    "precip": _precip_estimator,
}


def compute_consensus(
    readings: list[StationReading],
    *,
    variable: str,
    site_elevation_m: float,
    mad_floor: float,
) -> ConsensusResult | None:
    if not readings:
        return None
    if variable == "temperature":
        values = [
            reading.value + LAPSE * (reading.dem_elevation_m - site_elevation_m)
            for reading in readings
        ]
    else:
        values = [reading.value for reading in readings]
    estimator = _ESTIMATORS.get(variable)
    if estimator is None:
        # Allowlist dispatch: an unknown variable must never silently
        # inherit the median/MAD path.
        return None
    estimate = estimator(values, mad_floor)
    if estimate is None:
        return None
    value, n_stations, rejected_stations = estimate
    return ConsensusResult(
        variable=variable,
        valid_at=readings[0].valid_at,
        value=value,
        n_stations=n_stations,
        rejected_stations=rejected_stations,
    )


def insert_station_observation(
    conn: sqlite3.Connection,
    *,
    station_id: int,
    variable: str,
    valid_at: str,
    value: float,
    source_raw: str | None,
) -> bool:
    if variable in TARGET_VARIABLES and not source_raw:
        raise ValueError("target observation writes require source_raw")
    previous = conn.execute(
        """
        SELECT value FROM station_observations
        WHERE station_id = ? AND variable = ?
          AND valid_at < ?
        ORDER BY valid_at DESC LIMIT 1
        """,
        (station_id, variable, valid_at),
    ).fetchone()
    flag = qc_flag(
        variable,
        value,
        None if previous is None else float(previous["value"]),
    )
    current = conn.execute(
        """
        SELECT value, qc_flag, source_raw
        FROM station_observations
        WHERE station_id = ? AND variable = ? AND valid_at = ?
        """,
        (station_id, variable, valid_at),
    ).fetchone()
    if (
        current is not None
        and float(current["value"]) == value
        and str(current["qc_flag"]) == flag
        and current["source_raw"] == source_raw
    ):
        return False
    conn.execute(
        """
        INSERT INTO station_observations
            (station_id, variable, valid_at, value, qc_flag, source_raw, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(station_id, variable, valid_at) DO UPDATE SET
            value=excluded.value,
            qc_flag=excluded.qc_flag,
            source_raw=excluded.source_raw,
            fetched_at=excluded.fetched_at
        """,
        (station_id, variable, valid_at, value, flag, source_raw, isoformat_utc()),
    )
    site_row = conn.execute(
        "SELECT site_id FROM stations WHERE id = ?", (station_id,)
    ).fetchone()
    if site_row is not None:
        materialize_consensus(
            conn, site_id=int(site_row["site_id"]), variable=variable, valid_at=valid_at
        )
    return True


def materialize_consensus(
    conn: sqlite3.Connection, *, site_id: int, variable: str, valid_at: str
) -> None:
    """Recompute the consensus observation for one (site, variable, hour).

    LOAD-BEARING CONTRACT: this function ALWAYS runs
    ``_invalidate_consensus_dependents`` before it writes or deletes the
    ``observations`` row, and every write/delete of ``observations`` goes
    through here. The incremental persistence materializer
    (``wxverify.scoring.persistence``) relies on this to guarantee that any
    existing forecast pair is derivable from current observations — that is
    what makes its pair-count-equality skip sound. Bypassing the
    invalidation step makes the incremental materializer silently retain
    stale pairs.
    """
    site = conn.execute(
        "SELECT elevation_m FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    if site is None:
        return
    rows = conn.execute(
        """
        SELECT so.station_id, s.dem_elevation_m, so.variable, so.valid_at, so.value
        FROM station_observations so
        JOIN stations s ON s.id = so.station_id
        WHERE s.site_id = ?
          AND s.enabled = 1
          AND so.variable = ?
          AND so.valid_at = ?
          AND so.qc_flag = 'ok'
        """,
        (site_id, variable, valid_at),
    ).fetchall()
    readings = [
        StationReading(
            station_id=int(row["station_id"]),
            dem_elevation_m=float(row["dem_elevation_m"]),
            variable=str(row["variable"]),
            valid_at=str(row["valid_at"]),
            value=float(row["value"]),
        )
        for row in rows
    ]
    result = compute_consensus(
        readings,
        variable=variable,
        site_elevation_m=float(site["elevation_m"]),
        mad_floor=MAD_FLOORS.get(variable, 0.0),
    )
    _invalidate_consensus_dependents(
        conn, site_id=site_id, variable=variable, valid_at=valid_at
    )
    if result is None:
        conn.execute(
            "DELETE FROM observations WHERE site_id=? AND variable=? AND valid_at=?",
            (site_id, variable, valid_at),
        )
        return
    conn.execute(
        """
        INSERT INTO observations
            (site_id, variable, valid_at, value, n_stations, rejected_stations,
             computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_id, variable, valid_at) DO UPDATE SET
            value=excluded.value,
            n_stations=excluded.n_stations,
            rejected_stations=excluded.rejected_stations,
            computed_at=excluded.computed_at
        """,
        (
            site_id,
            result.variable,
            result.valid_at,
            result.value,
            result.n_stations,
            result.rejected_stations,
            isoformat_utc(),
        ),
    )


def _invalidate_consensus_dependents(
    conn: sqlite3.Connection, *, site_id: int, variable: str, valid_at: str
) -> None:
    # Deletes every pair that depends on the observation at (site, variable,
    # valid_at): pairs where the hour is the pair's valid_at (all feeds) and
    # persistence pairs where the hour is the pair's SOURCE (issued_at), plus
    # the affected score-cache rows. Part of the load-bearing contract
    # documented on materialize_consensus.
    conn.execute(
        "DELETE FROM forecast_pairs WHERE site_id=? AND variable=? AND valid_at=?",
        (site_id, variable, valid_at),
    )
    _delete_future_persistence_pairs(
        conn, site_id=site_id, variable=variable, source_valid_at=valid_at
    )
    conn.execute(
        "DELETE FROM score_cache WHERE site_id=? AND variable=?",
        (site_id, variable),
    )


def _delete_future_persistence_pairs(
    conn: sqlite3.Connection, *, site_id: int, variable: str, source_valid_at: str
) -> None:
    feed = conn.execute(
        """
        SELECT id, max_lead_hours
        FROM feeds
        WHERE source='virtual' AND model='_persistence'
        """
    ).fetchone()
    if feed is None:
        return
    source_valid = parse_utc(source_valid_at)
    feed_id = int(feed["id"])
    for lead in range(1, int(feed["max_lead_hours"]) + 1):
        target_valid_at = isoformat_utc(source_valid + timedelta(hours=lead))
        conn.execute(
            """
            DELETE FROM forecast_pairs
            WHERE site_id=?
              AND feed_id=?
              AND variable=?
              AND issued_at=?
              AND valid_at=?
              AND lead_hours=?
            """,
            (site_id, feed_id, variable, source_valid_at, target_valid_at, lead),
        )
