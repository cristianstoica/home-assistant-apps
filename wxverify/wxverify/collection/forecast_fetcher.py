"""Forecast fetch persistence helpers."""

from __future__ import annotations

import logging
import math
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wxverify.collection.budget import reserve_budget
from wxverify.collection.forecast_validation import invalid_forecast_sample_sql
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.site_days import daily_close_on
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


#: The newest value (rain release, Part B, B1): one ``forecast_samples`` row
#: per hour. A sample for a stored hour replaces that row, so a download that
#: repeats a label updates the hour. The narrower conflict target holds the
#: old ``UNIQUE(..., issued_at, valid_at)``, so that key never fails on its
#: own. The row is written only when the sample differs from it, so an
#: identical re-fetch writes nothing and keeps the row's ``fetched_at``. A
#: changed row takes the next row id, as a new row would: the readers that
#: find new forecast rows by id (``verification/manifest.py``'s arrivals
#: probe, the run watermark in ``verification/runs.py``,
#: ``forecast.data.samples_fingerprint``) then see the change.
_UPSERT_SAMPLE_SQL: Final = """
    INSERT INTO forecast_samples
        (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
         value, source_raw, model_run_id, fetched_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(site_id, feed_id, variable, valid_at) DO UPDATE SET
        id = (SELECT MAX(id) FROM forecast_samples) + 1,
        issued_at = excluded.issued_at,
        lead_hours = excluded.lead_hours,
        value = excluded.value,
        source_raw = excluded.source_raw,
        model_run_id = excluded.model_run_id,
        fetched_at = excluded.fetched_at
    WHERE forecast_samples.issued_at IS NOT excluded.issued_at
       OR forecast_samples.lead_hours IS NOT excluded.lead_hours
       OR forecast_samples.value IS NOT excluded.value
       OR forecast_samples.source_raw IS NOT excluded.source_raw
       OR forecast_samples.model_run_id IS NOT excluded.model_run_id
"""

#: The scored value (B2): one ``forecast_scored`` row for a valid sample of a
#: day's first stored download, the shared validator evaluated on the
#: sample's own bound values, as ``_SAMPLE_IS_VALID_SQL`` does. A NaN value
#: binds as NULL, so the validator reads NULL and no row is written. A row
#: already stored for the hour and day ahead is kept: the first download's.
_INSERT_SCORED_SQL: Final = f"""
    INSERT INTO forecast_scored
        (site_id, feed_id, variable, valid_at, day_ahead, value, lead_hours,
         issued_at, download_id, download_date, fetched_at)
    SELECT ?, ?, v.variable, v.valid_at, ?, v.value, v.lead_hours,
           v.issued_at, ?, ?, ?
    FROM (SELECT ? AS variable, ? AS value, ? AS lead_hours,
                 ? AS issued_at, ? AS valid_at) v
    WHERE NOT {invalid_forecast_sample_sql("v")}
    ON CONFLICT (site_id, feed_id, variable, valid_at, day_ahead) DO NOTHING
"""

#: The day ahead the scored value is kept for: D+0 to D+7 (B2).
SCORED_DAYS_AHEAD: Final = range(8)


@dataclass(frozen=True)
class PersistOutcome:
    """What :func:`persist_fetch_result` stored.

    ``usable_sample_count`` counts samples with ``lead_hours >= 1`` the fetch
    produced (the forward-path no-op predicate); ``inserted_count`` counts
    the ``forecast_samples`` rows written, new or changed (an identical
    re-fetch writes 0 while still being usable).

    ``labels`` maps each feed whose samples the reply carried (a meteoblue
    package's members under their own feed ids) to the sorted, distinct run
    labels (``issued_at``) of its usable samples; a feed the reply carried
    with no usable sample maps to an empty tuple. The download record
    stores it (daily-close P4 plan 4.2).

    ``samples`` maps each feed to the reply's sample for each hour it
    upserted, one per hour, whether or not the row changed; the fetch path
    writes a day's first download's scored values from them (rain release,
    Part B).
    """

    usable_sample_count: int
    inserted_count: int
    labels: dict[int, tuple[str, ...]]
    samples: dict[int, tuple[NormalizedSample, ...]]


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
    """Store a reply's samples, one row per hour (rain release, Part B, B1).

    Each usable sample (``lead_hours >= 1``) is upserted on its hour
    (``_UPSERT_SAMPLE_SQL``), invalid values included: the newest value is
    what the feed last said, and readers filter invalid values out. Where
    the reply gives one hour twice, the newer label's sample is written
    (the later one on a tie). A NaN value cannot be stored (it binds as
    NULL) and is skipped, as the old ``INSERT OR IGNORE`` skipped it, so the
    hour keeps its stored row. Then the feed state is stamped.
    """
    usable = 0
    has_valid = False
    labels: dict[int, set[str]] = {}
    newest: dict[tuple[int, str, str], NormalizedSample] = {}
    fetched_at = fetched_at or isoformat_utc()
    for sample in result.samples:
        feed_id = _feed_id_for_sample(conn, source, fetch_feed_id, sample)
        feed_labels = labels.setdefault(feed_id, set())
        if sample.lead_hours < 1:
            continue
        usable += 1
        feed_labels.add(sample.issued_at)
        if not has_valid:
            has_valid = _sample_is_valid(conn, sample)
        if math.isnan(sample.value):
            continue
        key = (feed_id, sample.variable, sample.valid_at)
        prior = newest.get(key)
        if prior is None or sample.issued_at >= prior.issued_at:
            newest[key] = sample
    inserted = 0
    written: dict[int, list[NormalizedSample]] = {}
    for (feed_id, _variable, _valid_at), sample in newest.items():
        cur = conn.execute(
            _UPSERT_SAMPLE_SQL,
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
        written.setdefault(feed_id, []).append(sample)
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
    return PersistOutcome(
        usable_sample_count=usable,
        inserted_count=inserted,
        labels={feed_id: tuple(sorted(issued)) for feed_id, issued in labels.items()},
        samples={feed_id: tuple(rows) for feed_id, rows in written.items()},
    )


def write_scored_values(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    samples: Iterable[NormalizedSample],
    download_id: int | None,
    download_date: date,
    fetched_at: str,
) -> int:
    """Write the scored values of one feed's download of ``download_date``
    into ``forecast_scored`` (rain release, Part B, B2); returns the rows
    written.

    The caller has decided the download is that date's first stored one for
    the feed. Each valid sample (the shared validator) is kept at its day
    ahead: the site-local date of its ``valid_at`` (:func:`site_local_dates`)
    minus ``download_date``, only 0 to 7. A row already stored for the hour
    and day ahead stays. Runs in the caller's transaction.
    """
    rows = list(samples)
    dates = site_local_dates(conn, site_id, (sample.valid_at for sample in rows))
    params: list[tuple[object, ...]] = []
    for sample in rows:
        target = dates.get(sample.valid_at)
        if target is None:
            continue
        day_ahead = (target - download_date).days
        if day_ahead not in SCORED_DAYS_AHEAD:
            continue
        params.append(
            (
                site_id,
                feed_id,
                day_ahead,
                download_id,
                download_date.isoformat(),
                fetched_at,
                sample.variable,
                sample.value,
                sample.lead_hours,
                sample.issued_at,
                sample.valid_at,
            )
        )
    before = conn.total_changes
    conn.executemany(_INSERT_SCORED_SQL, params)
    return conn.total_changes - before


def site_local_dates(
    conn: sqlite3.Connection, site_id: int, instants: Iterable[str]
) -> dict[str, date]:
    """The site-local date of each UTC stamp in ``instants``.

    The rule the download record dates a try's end by
    (``worker.feed_fetch._download_local_date``): while the daily close is
    on, the date of the record whose stored bounds hold the instant; else,
    or with no such record, the date in ``sites.timezone`` (in UTC when that
    zone does not load). A stamp that does not parse is left out, as is
    every stamp for a site that does not exist. Read-only.
    """
    parsed: dict[str, datetime] = {}
    for stamp in instants:
        try:
            parsed[stamp] = parse_utc(stamp)
        except ValueError:
            continue
    if not parsed:
        return {}
    zone_row = conn.execute(
        "SELECT timezone FROM sites WHERE id = ?", (site_id,)
    ).fetchone()
    if zone_row is None:
        return {}
    try:
        zone: ZoneInfo | None = ZoneInfo(str(zone_row[0]))
    except (ZoneInfoNotFoundError, ValueError):
        zone = None
    records: list[tuple[datetime, datetime, date]] = []
    if daily_close_on(conn):
        low = min(parsed.values())
        high = max(parsed.values())
        for row in conn.execute(
            "SELECT local_date, day_start_utc, day_end_utc FROM site_days"
            " WHERE site_id = ?"
            " AND julianday(day_start_utc) <= julianday(?)"
            " AND julianday(day_end_utc) > julianday(?)",
            (site_id, isoformat_utc(high), isoformat_utc(low)),
        ):
            records.append(
                (
                    parse_utc(str(row[1])),
                    parse_utc(str(row[2])),
                    date.fromisoformat(str(row[0])),
                )
            )
    dates: dict[str, date] = {}
    for stamp, instant in parsed.items():
        held = next(
            (local for start, end, local in records if start <= instant < end), None
        )
        if held is not None:
            dates[stamp] = held
        elif zone is None:
            dates[stamp] = instant.date()
        else:
            dates[stamp] = instant.astimezone(zone).date()
    return dates


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
