"""Weighted wind forecast: every eligible feed, weighted by past accuracy (§11).

In the ``pair_max`` state the served wind is no longer the blend-depth
selection. Each feed's weight at a lead ``k`` is ``1 / max(MAE, FLOOR_MS)``,
where the MAE compares the feed's forecast daily high (the latest run with at
least ``MIN_RUN_HOURS`` hours for the target day) with the observed daily
high (a local day with at least ``MIN_OBS_HOURS`` hours) over the last
``TRAINING_DAYS`` complete local days. A feed needs ``MIN_TRAINING_DAYS``
matched days to get a weight, and a cell needs ``MIN_FEEDS`` weighted feeds
with ``MIN_RUN_HOURS`` hours to be served at all; otherwise it is shown as
unavailable with a note, never silently narrowed.

The serving mode comes from the stored wind state
(:func:`wxverify.db.wind_basis.wind_serving_mode`): ``staging`` serves the
legacy blend, ``pair_max`` serves the weighted one, and every other state
serves wind closed with the rebuild's progress note. Units are m/s
throughout, like storage; the callers convert for display.

Live weights come from an in-process cache (§11.7) keyed on the process
database, its input epoch and its external-commit count, so a cached result
always equals what an uncached load would return. A record build's weights
(``as_of`` given) are never cached.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Final, Literal
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import isoformat_utc, local_day_slots, parse_utc, utc_now
from wxverify.db.connection import Database, InputCounts, current_db
from wxverify.db.snapshot import read_only_snapshot, read_snapshot
from wxverify.db.tz_generations import published_generation_clause
from wxverify.db.wind_basis import (
    wind_basis_state,
    wind_rebuild_progress,
    wind_serving_mode,
)
from wxverify.forecast.aggregate import EXTREMA_COVERAGE_NOT_EVALUATED
from wxverify.forecast.data import EXCLUDED_FEEDS_SQL
from wxverify.forecast.selection import CellCandidate, CellSelection
from wxverify.verification.asof import knowable_pair_predicate

logger = logging.getLogger(__name__)

FLOOR_MS: Final = 0.1
TRAINING_DAYS: Final = 30
MIN_OBS_HOURS: Final = 22
MIN_RUN_HOURS: Final = 20
MIN_TRAINING_DAYS: Final = 20
MIN_FEEDS: Final = 2
DAY_AHEADS: Final = range(8)
WIND_METHOD: Final = "inverse_mae_daily_high_v1"

NOT_ENOUGH_FEEDS_NOTE: Final = "Not enough feeds with a wind track record yet"
#: ``displayed.fallback_reason`` values in the forecast record (§11.5).
FALLBACK_NOT_ENOUGH_FEEDS: Final = "not_enough_feeds"
FALLBACK_SWITCH_IN_PROGRESS: Final = "wind_switch_in_progress"

WindServingMode = Literal["legacy", "weighted", "closed"]

# Target dates whose station inventory is not settled yet: an enabled
# station's row is still to fetch, retryable, or waiting on a reconcile
# refetch. Such a day's observed high can still change, so it never trains.
_UNSETTLED_DAYS_SQL: Final = """
    SELECT DISTINCT d.local_date
    FROM station_wind_days d JOIN stations st ON st.id = d.station_id
    WHERE st.site_id = ? AND st.enabled = 1
      AND d.local_date >= ? AND d.local_date < ?
      AND (d.status IN ('pending', 'partial')
           OR (d.status = 'failed' AND d.attempts < 3)
           OR (d.refetch_at IS NOT NULL AND d.refetched = 0))
"""


@dataclass(frozen=True)
class WindTraining:
    """The daily highs the weights are trained on, in m/s."""

    #: (feed_id, k) -> target local date -> the forecast daily high of the
    #: latest run at lead k with at least ``MIN_RUN_HOURS`` hours.
    feed_highs: dict[tuple[int, int], dict[date, float]]
    #: Target local date -> the observed daily high, for days with at least
    #: ``MIN_OBS_HOURS`` hours whose station inventory is settled.
    observed_highs: dict[date, float]


@dataclass(frozen=True)
class FeedWeight:
    """One (feed, lead) weight and the evidence behind it."""

    mae_ms: float
    days: int
    weight: float


#: (feed_id, k) -> weight; pairs with fewer than ``MIN_TRAINING_DAYS`` are absent.
WindWeights = dict[tuple[int, int], FeedWeight]


@dataclass(frozen=True)
class WindChoice:
    """The served wind feeds for one cell, their normalized weights and a note."""

    selection: CellSelection
    #: feed_id -> weight, normalized to sum 1; empty when not served.
    weights: dict[int, float]
    note: str | None


@dataclass(frozen=True)
class WindServing:
    """What one request (or one record build) serves for wind."""

    mode: WindServingMode
    #: Loaded only in ``weighted`` mode.
    weights: WindWeights | None
    #: The progress note in ``closed`` mode; None otherwise.
    note: str | None
    state: str


def training_window(today: date, timezone: str) -> tuple[str, str]:
    """``[local midnight of today - TRAINING_DAYS, local midnight of today)``."""
    lo = local_day_slots(today - timedelta(days=TRAINING_DAYS), timezone)[0]
    hi = local_day_slots(today, timezone)[0]
    return isoformat_utc(lo), isoformat_utc(hi)


def training_pairs_query(
    *, site_id: int, lo: str, hi: str, as_of: str | None
) -> tuple[str, tuple[object, ...]]:
    """The feed-side training statement and its parameters.

    One row per run and lead with at least ``MIN_RUN_HOURS`` distinct hours.
    The ``day_ahead IN (...)`` list keeps ``idx_pairs_leaderboard`` usable as
    eight equality probes with a ``valid_at`` range. ``as_of`` adds the
    knowability predicate at T.

    ``INDEXED BY idx_pairs_leaderboard`` pins the access path: without
    statistics (no ``ANALYZE``, so no ``sqlite_stat1``) the planner picks
    ``idx_pairs_winrate`` instead and walks every wind pair of the site, while
    the leaderboard index's ``valid_at`` range bounds the scan to the window.
    """
    knowable = ""
    knowable_params: tuple[str, ...] = ()
    if as_of is not None:
        clause, knowable_params = knowable_pair_predicate("fp", as_of=as_of)
        knowable = f"AND {clause}"
    day_aheads = ",".join(str(k) for k in DAY_AHEADS)
    sql = f"""
        SELECT fp.feed_id, fp.issued_at, fp.day_ahead,
               COUNT(DISTINCT fp.valid_at) AS n, MAX(fp.forecast) AS high,
               MIN(fp.valid_at) AS first_valid
        FROM forecast_pairs fp INDEXED BY idx_pairs_leaderboard
        JOIN feeds f ON f.id = fp.feed_id
        WHERE fp.site_id = ? AND fp.variable = 'wind'
          AND {published_generation_clause("fp")}
          AND {EXCLUDED_FEEDS_SQL}
          AND fp.day_ahead IN ({day_aheads})
          AND fp.valid_at >= ? AND fp.valid_at < ?
          {knowable}
        GROUP BY fp.feed_id, fp.issued_at, fp.day_ahead
        HAVING COUNT(DISTINCT fp.valid_at) >= {MIN_RUN_HOURS}
    """
    return sql, (site_id, lo, hi, *knowable_params)


def load_wind_training(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    timezone: str,
    today: date,
    as_of: str | None,
) -> WindTraining:
    """Read the forecast and observed daily highs of the training window.

    Only complete days train: the window ends at today's local midnight, and
    a target date with any unsettled enabled-station inventory row is left
    out of the observed side. A row whose stamp does not parse is skipped.
    """
    tz = ZoneInfo(timezone)
    lo, hi = training_window(today, timezone)
    sql, params = training_pairs_query(site_id=site_id, lo=lo, hi=hi, as_of=as_of)
    latest: dict[tuple[int, int, date], tuple[datetime, float]] = {}
    for row in conn.execute(sql, params):
        try:
            issued = parse_utc(str(row["issued_at"]))
            target = parse_utc(str(row["first_valid"])).astimezone(tz).date()
        except ValueError:
            continue
        key = (int(row["feed_id"]), int(row["day_ahead"]), target)
        held = latest.get(key)
        if held is None or issued > held[0]:
            latest[key] = (issued, float(row["high"]))
    feed_highs: dict[tuple[int, int], dict[date, float]] = {}
    for (feed_id, k, target), (_, high) in sorted(latest.items()):
        feed_highs.setdefault((feed_id, k), {})[target] = high

    obs_sql = (
        "SELECT valid_at, value FROM observations"
        " WHERE site_id = ? AND variable = 'wind'"
        " AND valid_at >= ? AND valid_at < ?"
    )
    obs_params: tuple[object, ...] = (site_id, lo, hi)
    if as_of is not None:
        obs_sql += (
            " AND computed_at IS NOT NULL AND julianday(computed_at) <= julianday(?)"
        )
        obs_params = (*obs_params, as_of)
    hours_by_day: dict[date, set[datetime]] = {}
    high_by_day: dict[date, float] = {}
    for row in conn.execute(obs_sql, obs_params):
        try:
            moment = parse_utc(str(row["valid_at"]))
        except ValueError:
            continue
        local_date = moment.astimezone(tz).date()
        hours_by_day.setdefault(local_date, set()).add(moment)
        value = float(row["value"])
        previous = high_by_day.get(local_date)
        if previous is None or value > previous:
            high_by_day[local_date] = value

    unsettled: set[date] = set()
    for row in conn.execute(
        _UNSETTLED_DAYS_SQL,
        (
            site_id,
            (today - timedelta(days=TRAINING_DAYS)).isoformat(),
            today.isoformat(),
        ),
    ):
        try:
            unsettled.add(date.fromisoformat(str(row["local_date"])))
        except ValueError:
            continue
    observed_highs = {
        local_date: high_by_day[local_date]
        for local_date in sorted(high_by_day)
        if len(hours_by_day[local_date]) >= MIN_OBS_HOURS
        and local_date not in unsettled
    }
    return WindTraining(feed_highs=feed_highs, observed_highs=observed_highs)


def compute_wind_weights(training: WindTraining) -> WindWeights:
    """Inverse-MAE weight per (feed, k) over the days both sides have. Pure."""
    weights: WindWeights = {}
    for key in sorted(training.feed_highs):
        highs = training.feed_highs[key]
        days = sorted(day for day in highs if day in training.observed_highs)
        if len(days) < MIN_TRAINING_DAYS:
            continue
        mae = sum(abs(highs[day] - training.observed_highs[day]) for day in days) / len(
            days
        )
        weights[key] = FeedWeight(
            mae_ms=mae, days=len(days), weight=1.0 / max(mae, FLOOR_MS)
        )
    return weights


def empty_wind_selection() -> CellSelection:
    """The selection of a wind cell that is not served (``available`` False)."""
    return CellSelection(
        feeds=[],
        low_confidence=False,
        extrema_feeds=[],
        extrema_coverage=EXTREMA_COVERAGE_NOT_EVALUATED,
        extrema_low_confidence=False,
    )


def choose_wind_feeds(
    candidates: Sequence[CellCandidate],
    rep_k: Mapping[int, int],
    weights: WindWeights,
) -> WindChoice:
    """Every eligible feed, ordered by weight; unavailable below ``MIN_FEEDS``.

    A candidate is eligible when it has a weight at its representative lead
    ``rep_k[feed_id]`` and at least ``MIN_RUN_HOURS`` covered hours.
    """
    eligible: list[tuple[CellCandidate, float]] = []
    for candidate in candidates:
        k = rep_k.get(candidate.feed_id)
        if k is None or candidate.covered_hours < MIN_RUN_HOURS:
            continue
        feed_weight = weights.get((candidate.feed_id, k))
        if feed_weight is None:
            continue
        eligible.append((candidate, feed_weight.weight))
    if len(eligible) < MIN_FEEDS:
        return WindChoice(
            selection=empty_wind_selection(), weights={}, note=NOT_ENOUGH_FEEDS_NOTE
        )
    eligible.sort(key=lambda item: (-item[1], item[0].feed_id))
    total = sum(weight for _, weight in eligible)
    return WindChoice(
        selection=CellSelection(
            feeds=[candidate for candidate, _ in eligible],
            low_confidence=False,
            extrema_feeds=[],
            extrema_coverage=EXTREMA_COVERAGE_NOT_EVALUATED,
            extrema_low_confidence=False,
        ),
        weights={candidate.feed_id: weight / total for candidate, weight in eligible},
        note=None,
    )


def weighted_daily_high(
    values: Mapping[int, Sequence[float]], weights: Mapping[int, float]
) -> float | None:
    """Σ w·max(v) / Σ w over the weighted feeds with values; None when none.

    Summed in feed-id order, so the result does not depend on mapping order.
    """
    numerator = 0.0
    denominator = 0.0
    for feed_id in sorted(values):
        feed_values = values[feed_id]
        weight = weights.get(feed_id)
        if not feed_values or weight is None:
            continue
        numerator += weight * max(feed_values)
        denominator += weight
    if denominator <= 0:
        return None
    return numerator / denominator


def weighted_hourly(
    series: Mapping[int, Mapping[str, float]], weights: Mapping[int, float]
) -> dict[str, float]:
    """Per-hour weighted mean over the feeds present at that hour.

    ``series`` maps each feed to its values by ``valid_at``. The weights are
    renormalized at every hour over the feeds that have a value there; an
    hour with no weighted feed is absent. Keys come back in hour order.
    """
    numerator: dict[str, float] = {}
    denominator: dict[str, float] = {}
    for feed_id in sorted(series):
        weight = weights.get(feed_id)
        if weight is None:
            continue
        for hour, value in series[feed_id].items():
            numerator[hour] = numerator.get(hour, 0.0) + weight * value
            denominator[hour] = denominator.get(hour, 0.0) + weight
    return {
        hour: numerator[hour] / denominator[hour]
        for hour in sorted(numerator)
        if denominator[hour] > 0
    }


def read_wind_state(conn: sqlite3.Connection, site_id: int) -> str:
    """The stored wind state, read once at each end of the serving double read.

    Both reads go through this one function so the check has a single seam.
    """
    return wind_basis_state(conn, site_id)


def closed_wind_note(conn: sqlite3.Connection, site_id: int) -> str:
    """The progress note shown while wind is served closed."""
    return wind_rebuild_progress(conn, site_id).closed_note


# --- Weights cache (§11.7) ---------------------------------------------------
#
# The live weights depend only on database rows (forecast_pairs, feeds, the
# published pointer in runtime_state, observations, station_wind_days joined
# to stations) and on site_id, timezone and today. They read no clock and no
# setting, and the constants change only with the code (a new process starts
# with an empty cache). So an entry is keyed on the Database object, its
# input epoch (every own write that can change a training row moves it) and
# its external-commit count (every commit by another connection moves it),
# plus the three arguments. Any new input to `load_wind_training` or
# `compute_wind_weights` that is not a database row must join the key.

#: A ``live_wind_weights`` call at or above this, timed from entry and so
#: including the wait on ``_LOCK``, logs one WARNING. Read at call time.
SLOW_CALL_MS = 10_000
_MAX_ENTRIES: Final = 16

WindCacheOutcome = Literal["hit", "miss", "bypass", "error"]
WindCacheBypass = Literal[
    "in_transaction", "no_database", "foreign_connection", "probe_error", "counts_moved"
]
_BYPASS_REASONS: Final[tuple[WindCacheBypass, ...]] = (
    "in_transaction",
    "no_database",
    "foreign_connection",
    "probe_error",
    "counts_moved",
)
_CALL_BUCKETS: Final = ("lt_1s", "1_3s", "3_10s", "ge_10s")


@dataclass(frozen=True)
class _Entry:
    db: Database
    epoch: int
    ext: int
    timezone: str
    today: date
    weights: WindWeights


@dataclass
class _Stats:
    hits: int = 0
    misses: int = 0
    errors: int = 0
    miss_ms_total: float = 0.0
    bypasses: dict[str, int] = field(
        default_factory=lambda: dict.fromkeys(_BYPASS_REASONS, 0)
    )
    calls: dict[str, int] = field(
        default_factory=lambda: dict.fromkeys(_CALL_BUCKETS, 0)
    )
    max_call_ms: float = 0.0
    since: str = field(default_factory=lambda: isoformat_utc(utc_now()))


# Held across the whole compute on purpose (single-flight), unlike
# read_cache's lock: a second request for the same key waits, then hits. The
# compute uses only the caller's own pooled reader and the probe, never a
# second pooled reader, so the holder never waits on the pool. A pinned
# caller waits here with its snapshot open. The pool bounds the waiters (at
# most 3), but not the wait to one compute: requests pinned at different
# counts can each miss and compute in turn. Read as a module global at every
# call, so a test can replace it. Lock order: _LOCK, then
# Database._probe_lock or _STATS_LOCK, never the reverse.
_LOCK = threading.Lock()
# Guards _stats only; never held across I/O or while acquiring another lock,
# so wind_weights_cache_stats() never waits on a compute.
_STATS_LOCK = threading.Lock()
_ENTRIES: dict[int, _Entry] = {}
_stats = _Stats()


def _count_hit() -> None:
    with _STATS_LOCK:
        _stats.hits += 1


def _record_miss(ms: float) -> None:
    with _STATS_LOCK:
        _stats.misses += 1
        _stats.miss_ms_total += ms


def _count_bypass(reason: WindCacheBypass) -> None:
    with _STATS_LOCK:
        _stats.bypasses[reason] += 1


def _call_bucket(ms: float) -> str:
    if ms < 1_000:
        return "lt_1s"
    if ms < 3_000:
        return "1_3s"
    if ms < 10_000:
        return "3_10s"
    return "ge_10s"


def _record_call(site_id: int, outcome: WindCacheOutcome, ms: float) -> None:
    """Count one ``live_wind_weights`` call; WARNING when it was slow."""
    with _STATS_LOCK:
        if outcome == "error":
            _stats.errors += 1
        _stats.calls[_call_bucket(ms)] += 1
        if ms > _stats.max_call_ms:
            _stats.max_call_ms = ms
    if ms >= SLOW_CALL_MS:
        logger.warning(
            "slow wind weights call site=%s outcome=%s ms=%.0f", site_id, outcome, ms
        )


def wind_weights_cache_stats() -> dict[str, object]:
    """A snapshot of the cache's counters, for ``/api/worker/status``.

    Taken under ``_STATS_LOCK`` only, never ``_LOCK``: the status route calls
    this on the event loop, so it must never wait on a compute.
    """
    with _STATS_LOCK:
        return {
            "hits": _stats.hits,
            "misses": _stats.misses,
            "errors": _stats.errors,
            "miss_ms_total": _stats.miss_ms_total,
            "bypasses": dict(_stats.bypasses),
            "calls": dict(_stats.calls),
            "max_call_ms": _stats.max_call_ms,
            "since": _stats.since,
        }


def reset_wind_weights_cache() -> None:
    """Empty the entries and zero every counter, ``max_call_ms`` included.

    ``since`` moves to now. The entries are cleared under ``_LOCK``, which
    guards every other change to them.
    """
    global _stats
    with _LOCK:
        _ENTRIES.clear()
        with _STATS_LOCK:
            _stats = _Stats()


def live_wind_weights(
    conn: sqlite3.Connection, *, site_id: int, timezone: str, today: date
) -> WindWeights:
    """The live (``as_of is None``) weights, through the cache.

    Always equal to ``compute_wind_weights(load_wind_training(...,
    as_of=None))`` on the same connection, as far as the training inputs the
    counts track: matching counts mean every count-moving write is the same,
    and a write exempt from the epoch is by design invisible to the counts.
    Inside a ``pinned_read_snapshot`` the cache keys on the snapshot's pin.
    Any other transaction, a connection that is not a pooled reader of the
    process database (or no process database), a failed probe read and
    counts that moved across the snapshot's start compute uncached and are
    counted as a bypass by reason. Every call is timed from entry, so the
    wait on ``_LOCK`` is included.
    """
    entered = time.perf_counter()
    outcome: WindCacheOutcome = "error"
    try:
        weights, outcome = _cached_weights(conn, site_id, timezone, today)
        return weights
    finally:
        _record_call(site_id, outcome, (time.perf_counter() - entered) * 1000.0)


def _load(
    conn: sqlite3.Connection,
    site_id: int,
    timezone: str,
    today: date,
    *,
    pooled: bool,
) -> WindTraining:
    # One snapshot for all three statements. A pooled reader also gets
    # query_only (read_only_snapshot, the read_at_epoch precedent); a foreign
    # connection gets read_snapshot, so the cache never resets a query_only
    # the caller set.
    snapshot = read_only_snapshot if pooled else read_snapshot
    with snapshot(conn, label="wind_weights"):
        return load_wind_training(
            conn, site_id=site_id, timezone=timezone, today=today, as_of=None
        )


def _lookup(
    db: Database, counts: InputCounts, site_id: int, timezone: str, today: date
) -> WindWeights | None:
    # Called with _LOCK held. A copy, so a caller cannot change the entry.
    entry = _ENTRIES.get(site_id)
    if (
        entry is not None
        and entry.db is db
        and entry.epoch == counts.epoch
        and entry.ext == counts.ext
        and entry.timezone == timezone
        and entry.today == today
    ):
        return dict(entry.weights)
    return None


def _store(
    db: Database,
    counts: InputCounts,
    site_id: int,
    timezone: str,
    today: date,
    weights: WindWeights,
) -> None:
    # Called with _LOCK held. A pin older than a held entry stores nothing,
    # so it never evicts the newer one: both counts only grow, and the
    # caller's own weights stay correct for its snapshot.
    for held in _ENTRIES.values():
        if (
            held.db is db
            and held.epoch >= counts.epoch
            and held.ext >= counts.ext
            and (held.epoch, held.ext) != (counts.epoch, counts.ext)
        ):
            return
    stale = [
        key
        for key, held in _ENTRIES.items()
        if held.db is not db or held.epoch != counts.epoch or held.ext != counts.ext
    ]
    for key in stale:
        del _ENTRIES[key]
    if site_id in _ENTRIES or len(_ENTRIES) < _MAX_ENTRIES:
        _ENTRIES[site_id] = _Entry(
            db, counts.epoch, counts.ext, timezone, today, dict(weights)
        )


def _pinned_weights(
    db: Database,
    conn: sqlite3.Connection,
    pin: InputCounts,
    site_id: int,
    timezone: str,
    today: date,
) -> tuple[WindWeights, WindCacheOutcome]:
    # Keyed on the pin, never on counts re-read here: the pin is what the
    # caller's snapshot is exactly described by.
    with _LOCK:
        hit = _lookup(db, pin, site_id, timezone, today)
        if hit is not None:
            _count_hit()
            return hit, "hit"
        started = time.perf_counter()
        # Inside the caller's pinned snapshot: no BEGIN, no query_only, no
        # _load (it would raise SnapshotNestingError).
        training = load_wind_training(
            conn, site_id=site_id, timezone=timezone, today=today, as_of=None
        )
        weights = compute_wind_weights(training)
        _record_miss((time.perf_counter() - started) * 1000.0)
        _store(db, pin, site_id, timezone, today, weights)
        return weights, "miss"


def _cached_weights(
    conn: sqlite3.Connection, site_id: int, timezone: str, today: date
) -> tuple[WindWeights, WindCacheOutcome]:
    db = current_db()
    if conn.in_transaction:
        # A pinned snapshot keys on its pin. Any other transaction pins one
        # snapshot that may predate the counts: compute inside it, store
        # nothing.
        pin = db.snapshot_pin(conn) if db is not None else None
        if db is not None and isinstance(pin, InputCounts):
            return _pinned_weights(db, conn, pin, site_id, timezone, today)
        reason: WindCacheBypass = pin if isinstance(pin, str) else "in_transaction"
        _count_bypass(reason)
        training = load_wind_training(
            conn, site_id=site_id, timezone=timezone, today=today, as_of=None
        )
        return compute_wind_weights(training), "bypass"
    if db is None or not db.owns_pooled_reader(conn):
        _count_bypass("no_database" if db is None else "foreign_connection")
        training = _load(conn, site_id, timezone, today, pooled=False)
        return compute_wind_weights(training), "bypass"
    with _LOCK:
        # The counts before the snapshot, and again after its priming read;
        # store only if equal. Each reading takes the count first: the writer
        # bumps the epoch and then absorbs, so a reading whose count follows
        # the absorb also sees the bump.
        before = db.input_counts()
        if before is None:
            # Whether another connection committed can't be told: neither hit
            # nor store.
            _count_bypass("probe_error")
            training = _load(conn, site_id, timezone, today, pooled=True)
            return compute_wind_weights(training), "bypass"
        hit = _lookup(db, before, site_id, timezone, today)
        if hit is not None:
            _count_hit()
            return hit, "hit"
        started = time.perf_counter()
        with read_only_snapshot(conn, label="wind_weights"):
            # First in the body, so after the priming read. Reads the probe,
            # not conn.
            after = db.input_counts()
            training = load_wind_training(
                conn, site_id=site_id, timezone=timezone, today=today, as_of=None
            )
        # Pure, and outside the snapshot.
        weights = compute_wind_weights(training)
        if after != before:
            _count_bypass("probe_error" if after is None else "counts_moved")
            return weights, "bypass"
        _record_miss((time.perf_counter() - started) * 1000.0)
        _store(db, before, site_id, timezone, today, weights)
        return weights, "miss"


def load_wind_serving(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    timezone: str,
    today: date,
    as_of: str | None,
) -> WindServing:
    """Read the state once and what its mode needs: weights or the note.

    The state is read on every call and never cached. Live weights
    (``as_of is None``) come through the cache (``live_wind_weights``); a
    record build's (``as_of`` given) are computed uncached.
    """
    state = read_wind_state(conn, site_id)
    mode = wind_serving_mode(state)
    if mode == "weighted":
        if as_of is None:
            weights = live_wind_weights(
                conn, site_id=site_id, timezone=timezone, today=today
            )
        else:
            weights = compute_wind_weights(
                load_wind_training(
                    conn, site_id=site_id, timezone=timezone, today=today, as_of=as_of
                )
            )
        return WindServing(mode=mode, weights=weights, note=None, state=state)
    if mode == "closed":
        return WindServing(
            mode=mode,
            weights=None,
            note=closed_wind_note(conn, site_id),
            state=state,
        )
    return WindServing(mode=mode, weights=None, note=None, state=state)
