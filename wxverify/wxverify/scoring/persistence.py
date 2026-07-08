"""Lead-specific persistence baseline pairs (incremental materializer).

The 0.1.0 implementation deleted every persistence pair and rebuilt from
scratch on each run (~9 s at 500k pairs, blocking the single writer and the
event loop). This version is insert-only and skips target hours whose pairs
are already complete.

Why insert-only is safe (the consensus invalidation contract): every write
or delete of an ``observations`` row goes through
``wxverify.scoring.consensus.materialize_consensus``, which ALWAYS runs
``_invalidate_consensus_dependents`` first — deleting all pairs where the
hour is the pair's ``valid_at`` (any feed) and every persistence pair where
the hour is the pair's source (``issued_at``). Therefore any existing
persistence pair is derivable from the current observations, i.e.
existing pairs are a SUBSET of derivable pairs, so per-target pair-count
equality implies set equality and the target can be skipped.
"""

from __future__ import annotations

import sqlite3
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import day_ahead, isoformat_utc, parse_utc
from wxverify.scoring.pair_flags import precip_flags

_MAX_DAY_AHEAD = 7


@dataclass
class _Group:
    """Observations for one (site, variable), split into targets and sources."""

    timezone: str
    rain_threshold_mm: float
    # (epoch_seconds, stored valid_at string, value, is_canonical)
    targets: list[tuple[int, str, float, bool]]
    # canonical hour sources only: epoch_seconds -> value
    sources: dict[int, float]


def materialize_persistence(
    conn: sqlite3.Connection, site_id: int | None = None
) -> int:
    feed = conn.execute(
        """
        SELECT id, max_lead_hours
        FROM feeds
        WHERE source='virtual' AND model='_persistence'
        """
    ).fetchone()
    if feed is None:
        return 0
    feed_id = int(feed["id"])
    max_lead = int(feed["max_lead_hours"])
    where = "" if site_id is None else "WHERE site_id = ?"
    params: tuple[object, ...] = () if site_id is None else (site_id,)
    observations = conn.execute(
        f"""
        SELECT o.site_id, o.variable, o.valid_at, o.value, s.timezone,
               s.rain_threshold_mm
        FROM observations o
        JOIN sites s ON s.id = o.site_id
        {where}
        """,
        params,
    ).fetchall()
    if not observations:
        return 0
    existing_counts = _existing_pair_counts(conn, feed_id, site_id)
    groups = _group_observations(observations)
    written = 0
    for (obs_site_id, variable), group in groups.items():
        rain_threshold = group.rain_threshold_mm if variable == "precip" else None
        written += _materialize_group(
            conn,
            feed_id=feed_id,
            max_lead=max_lead,
            site_id=obs_site_id,
            variable=variable,
            group=group,
            rain_threshold=rain_threshold,
            existing_counts=existing_counts,
        )
    return written


def _existing_pair_counts(
    conn: sqlite3.Connection, feed_id: int, site_id: int | None
) -> dict[tuple[int, str, str], int]:
    count_where = "" if site_id is None else "AND site_id = ?"
    count_params: tuple[object, ...] = (
        (feed_id,) if site_id is None else (feed_id, site_id)
    )
    counts: dict[tuple[int, str, str], int] = {}
    for row in conn.execute(
        f"""
        SELECT site_id, variable, valid_at, COUNT(*) AS n
        FROM forecast_pairs
        WHERE feed_id = ? {count_where}
        GROUP BY site_id, variable, valid_at
        """,
        count_params,
    ):
        key = (int(row["site_id"]), str(row["variable"]), str(row["valid_at"]))
        counts[key] = int(row["n"])
    return counts


def _group_observations(
    observations: list[sqlite3.Row],
) -> dict[tuple[int, str], _Group]:
    groups: dict[tuple[int, str], _Group] = {}
    for obs in observations:
        valid_at = str(obs["valid_at"])
        dt = parse_utc(valid_at)
        # Lag lookups match on the exact canonical isoformat_utc string, so
        # only canonically-stored rows can act as in-memory sources; anything
        # else falls back to per-lead point lookups (0.1.0 semantics).
        canonical = dt.microsecond == 0 and isoformat_utc(dt) == valid_at
        key = (int(obs["site_id"]), str(obs["variable"]))
        group = groups.get(key)
        if group is None:
            group = _Group(
                timezone=str(obs["timezone"]),
                rain_threshold_mm=float(obs["rain_threshold_mm"]),
                targets=[],
                sources={},
            )
            groups[key] = group
        epoch = int(dt.timestamp())
        group.targets.append((epoch, valid_at, float(obs["value"]), canonical))
        if canonical:
            group.sources[epoch] = float(obs["value"])
    return groups


def _materialize_group(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    max_lead: int,
    site_id: int,
    variable: str,
    group: _Group,
    rain_threshold: float | None,
    existing_counts: dict[tuple[int, str, str], int],
) -> int:
    tz = ZoneInfo(group.timezone)
    # Sources bucketed by their sub-hour remainder: a target only ever lags
    # onto sources at exact whole-hour offsets, i.e. the same remainder.
    by_remainder: dict[int, tuple[list[int], list[int]]] = {}
    for epoch in sorted(group.sources):
        epochs, date_ordinals = by_remainder.setdefault(epoch % 3600, ([], []))
        epochs.append(epoch)
        date_ordinals.append(datetime.fromtimestamp(epoch, tz).date().toordinal())
    written = 0
    for epoch, valid_at, observed, canonical in group.targets:
        if not canonical:
            written += _materialize_target_fallback(
                conn,
                feed_id=feed_id,
                max_lead=max_lead,
                site_id=site_id,
                variable=variable,
                valid_at=valid_at,
                observed=observed,
                timezone=group.timezone,
                rain_threshold=rain_threshold,
            )
            continue
        arrays = by_remainder.get(epoch % 3600)
        if arrays is None:
            continue
        epochs, date_ordinals = arrays
        target_ordinal = datetime.fromtimestamp(epoch, tz).date().toordinal()
        # Candidate sources: lead 1..max_lead hours back, day_ahead 0..7.
        # Local date is monotonic in UTC time, so date_ordinals is sorted and
        # every candidate in [left, right) has 0 <= day_ahead <= 7.
        left = max(
            bisect_left(epochs, epoch - max_lead * 3600),
            bisect_left(date_ordinals, target_ordinal - _MAX_DAY_AHEAD),
        )
        right = bisect_right(epochs, epoch - 3600)
        if right <= left:
            continue
        expected = right - left
        if existing_counts.get((site_id, variable, valid_at), 0) == expected:
            # Invalidation contract (see module docstring): existing pairs are
            # a subset of derivable pairs, so count equality => set equality.
            continue
        valid_dt = datetime.fromtimestamp(epoch, UTC)
        for index in range(left, right):
            source_epoch = epochs[index]
            lead = (epoch - source_epoch) // 3600
            issued_at = isoformat_utc(valid_dt - timedelta(hours=lead))
            written += _insert_persistence_pair(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                variable=variable,
                issued_at=issued_at,
                valid_at=valid_at,
                lead=lead,
                bucket=target_ordinal - date_ordinals[index],
                forecast=group.sources[source_epoch],
                observed=observed,
                rain_threshold=rain_threshold,
            )
    return written


def _materialize_target_fallback(
    conn: sqlite3.Connection,
    *,
    feed_id: int,
    max_lead: int,
    site_id: int,
    variable: str,
    valid_at: str,
    observed: float,
    timezone: str,
    rain_threshold: float | None,
) -> int:
    """0.1.0 per-lead point lookups for non-canonical target timestamps."""
    valid = parse_utc(valid_at)
    written = 0
    for lead in range(1, max_lead + 1):
        issued_at = isoformat_utc(valid - timedelta(hours=lead))
        lagged = conn.execute(
            """
            SELECT value FROM observations
            WHERE site_id=? AND variable=? AND valid_at=?
            """,
            (site_id, variable, issued_at),
        ).fetchone()
        if lagged is None:
            continue
        bucket = day_ahead(issued_at, valid_at, timezone)
        if bucket < 0 or bucket > _MAX_DAY_AHEAD:
            continue
        written += _insert_persistence_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            issued_at=issued_at,
            valid_at=valid_at,
            lead=lead,
            bucket=bucket,
            forecast=float(lagged["value"]),
            observed=observed,
            rain_threshold=rain_threshold,
        )
    return written


def _insert_persistence_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead: int,
    bucket: int,
    forecast: float,
    observed: float,
    rain_threshold: float | None,
) -> int:
    hit, false, miss, correct_neg = precip_flags(
        variable, forecast, observed, rain_threshold
    )
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             cat_hit, cat_false, cat_miss, cat_correct_neg,
             rain_threshold_mm)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            lead,
            bucket,
            forecast,
            observed,
            forecast - observed,
            abs(forecast - observed),
            (forecast - observed) ** 2,
            hit,
            false,
            miss,
            correct_neg,
            rain_threshold,
        ),
    )
    return cur.rowcount
