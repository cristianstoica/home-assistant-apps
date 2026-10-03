"""Forecast fetch persistence helpers."""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Final

from wxverify.collection.budget import reserve_budget
from wxverify.collection.forecast_validation import invalid_forecast_sample_sql
from wxverify.core.timeutil import isoformat_utc
from wxverify.feeds.seam import FetchResult, GridProvenance, NormalizedSample

logger = logging.getLogger(__name__)

# Distinguishable sentinel stamped into site_feed_state.last_error when a
# forward fetch returns HTTP 200 but yields zero usable canonical samples.
NO_USABLE_SAMPLES_SENTINEL = "200 / 0 usable samples"

# The shared read-side validator evaluated on one sample's own bound values, so
# the writer and the readers agree on what "usable" means. The expression is 1
# for a valid sample, 0 for an out-of-range one and NULL for a NaN value
# (sqlite3 binds a float NaN as NULL); only 1 counts.
_SAMPLE_IS_VALID_SQL: Final[str] = (
    f"SELECT NOT {invalid_forecast_sample_sql('v')} "
    "FROM (SELECT ? AS variable, ? AS value, ? AS lead_hours, "
    "? AS issued_at, ? AS valid_at) v"
)


@dataclass(frozen=True)
class PersistOutcome:
    """Two distinct counts returned by :func:`persist_fetch_result`.

    ``usable_sample_count`` counts samples with ``lead_hours >= 1`` the fetch
    produced (the forward-path no-op predicate); ``inserted_count`` is the
    ``INSERT OR IGNORE`` rowcount sum (an idempotent re-fetch inserts 0 while
    still being usable). Callers gate scoring on ``inserted_count``.
    """

    usable_sample_count: int
    inserted_count: int


def register_feed_if_needed(
    conn: sqlite3.Connection, source: str, model: str, *, fetch_interval_minutes: int
) -> int:
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO feeds
            (source, model, enabled, default_subscribed, fetch_interval_minutes,
             max_lead_hours, is_virtual)
        VALUES (?, ?, 1, 0, ?, 168, 0)
        """,
        (source, model, fetch_interval_minutes),
    )
    inserted = cur.rowcount
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = ? AND model = ?", (source, model)
    ).fetchone()
    if row is None:
        raise RuntimeError("feed registration failed")
    feed_id = int(row["id"])
    if inserted:
        logger.debug("feed registered source=%s model=%s id=%s", source, model, feed_id)
    return feed_id


def persist_fetch_result(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    source: str,
    fetch_feed_id: int,
    result: FetchResult,
    fetched_at: str | None = None,
    advance_last_run_at: bool = True,
) -> PersistOutcome:
    inserted = 0
    usable = 0
    has_valid = False
    fetched_at = fetched_at or isoformat_utc()
    for sample in result.samples:
        feed_id = _feed_id_for_sample(conn, source, fetch_feed_id, sample)
        if sample.lead_hours < 1:
            continue
        usable += 1
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                site_id,
                feed_id,
                sample.variable,
                sample.issued_at,
                sample.valid_at,
                sample.lead_hours,
                sample.value,
                sample.source_raw,
                sample.model_run_id,
                fetched_at,
            ),
        )
        inserted += cur.rowcount
        if not has_valid:
            has_valid = _sample_is_valid(conn, sample)
    grid = result.grid
    # No-op handling is the FORWARD-FETCH path only (advance_last_run_at=True);
    # the historical callers keep the unconditional last_error / error_count
    # clear below.
    if advance_last_run_at and usable == 0:
        _stamp_no_op(conn, site_id, fetch_feed_id, fetched_at, grid)
    else:
        _clear_feed_state(
            conn, site_id, fetch_feed_id, fetched_at, grid, advance_last_run_at
        )
        # A forward fetch that returned at least one usable sample stamps the
        # collection-freshness clock, even when every sample was already stored
        # (inserted == 0). The row exists: _clear_feed_state UPSERTs it.
        if advance_last_run_at and has_valid:
            conn.execute(
                "UPDATE site_feed_state SET last_usable_fetch_at = ? "
                "WHERE site_id = ? AND feed_id = ?",
                (fetched_at, site_id, fetch_feed_id),
            )
    logger.debug(
        "persist site=%s feed=%s usable=%s inserted=%s",
        site_id,
        fetch_feed_id,
        usable,
        inserted,
    )
    return PersistOutcome(usable_sample_count=usable, inserted_count=inserted)


def _sample_is_valid(conn: sqlite3.Connection, sample: NormalizedSample) -> bool:
    """Return True only when the shared validator reads exactly 1 for ``sample``.

    ``row[0] == 1`` is the only sound reading: 0 means out of range and NULL
    means a NaN or missing value, and both are not usable.
    """
    row = conn.execute(
        _SAMPLE_IS_VALID_SQL,
        (
            sample.variable,
            sample.value,
            sample.lead_hours,
            sample.issued_at,
            sample.valid_at,
        ),
    ).fetchone()
    return row is not None and row[0] == 1


def _stamp_no_op(
    conn: sqlite3.Connection,
    site_id: int,
    fetch_feed_id: int,
    fetched_at: str,
    grid: GridProvenance | None,
) -> None:
    grid_lat, grid_lon, grid_elevation_m = _grid_columns(grid)
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_run_at, last_error, error_count, grid_lat,
             grid_lon, grid_elevation_m)
        VALUES (?, ?, ?, ?, 1, ?, ?, ?)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET
            last_run_at=excluded.last_run_at,
            last_error=excluded.last_error,
            error_count=site_feed_state.error_count + 1,
            grid_lat=COALESCE(excluded.grid_lat, site_feed_state.grid_lat),
            grid_lon=COALESCE(excluded.grid_lon, site_feed_state.grid_lon),
            grid_elevation_m=COALESCE(
                excluded.grid_elevation_m,
                site_feed_state.grid_elevation_m
            )
        """,
        (
            site_id,
            fetch_feed_id,
            fetched_at,
            NO_USABLE_SAMPLES_SENTINEL,
            grid_lat,
            grid_lon,
            grid_elevation_m,
        ),
    )


def _clear_feed_state(
    conn: sqlite3.Connection,
    site_id: int,
    fetch_feed_id: int,
    fetched_at: str,
    grid: GridProvenance | None,
    advance_last_run_at: bool,
) -> None:
    grid_lat, grid_lon, grid_elevation_m = _grid_columns(grid)
    state_last_run_at = fetched_at if advance_last_run_at else None
    conn.execute(
        """
        INSERT INTO site_feed_state
            (site_id, feed_id, last_run_at, error_count, grid_lat, grid_lon,
             grid_elevation_m)
        VALUES (?, ?, ?, 0, ?, ?, ?)
        ON CONFLICT(site_id, feed_id) DO UPDATE SET
            last_run_at=CASE
                WHEN ? THEN excluded.last_run_at
                ELSE site_feed_state.last_run_at
            END,
            last_error=NULL,
            error_count=0,
            grid_lat=COALESCE(excluded.grid_lat, site_feed_state.grid_lat),
            grid_lon=COALESCE(excluded.grid_lon, site_feed_state.grid_lon),
            grid_elevation_m=COALESCE(
                excluded.grid_elevation_m,
                site_feed_state.grid_elevation_m
            )
        """,
        (
            site_id,
            fetch_feed_id,
            state_last_run_at,
            grid_lat,
            grid_lon,
            grid_elevation_m,
            1 if advance_last_run_at else 0,
        ),
    )


def _grid_columns(
    grid: GridProvenance | None,
) -> tuple[float | None, float | None, float | None]:
    if grid is None:
        return None, None, None
    return grid.grid_lat, grid.grid_lon, grid.grid_elevation_m


def reserve_for_fetch(
    conn: sqlite3.Connection,
    source: str,
    cost_calls: int,
    cost_credits: int | None,
) -> None:
    reserve_budget(conn, source, cost_calls, cost_credits)


def _feed_id_for_sample(
    conn: sqlite3.Connection, source: str, fetch_feed_id: int, sample: NormalizedSample
) -> int:
    row = conn.execute(
        "SELECT model FROM feeds WHERE id = ?", (fetch_feed_id,)
    ).fetchone()
    if row is None:
        raise RuntimeError("fetch feed not found")
    if str(row["model"]) == sample.model:
        return fetch_feed_id
    return register_feed_if_needed(
        conn, source, sample.model, fetch_interval_minutes=1440
    )
