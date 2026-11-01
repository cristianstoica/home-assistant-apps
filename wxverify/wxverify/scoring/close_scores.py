"""Scoring the closed day: P3's stage 5.

Daily-close P4 plan (``plans/2026-10-22-daily-close-pairs-and-publication.md``)
section 6. ``score_day`` scores one ``closing`` day D in the transaction P3
passes: for every real feed considered at D (6.2) and day ahead k it takes
the scored download of D - k (4.1, 4.3, ``scoring/downloads.py``), pairs
that download's forecasts with D's hourly values, makes the two yardsticks
(4.6), and stores the pairs (``close_pairs``), the day's sums
(``day_scores``) and the per-close record (``close_records``,
``close_record_downloads``), replacing an earlier attempt's rows. It never
commits, enqueues nothing, changes no day state, fetches nothing and reads
the clock only through ``now_utc``.

``metrics_from_sums`` lives in ``scoring/metrics.py`` beside
``MetricResult``, so today's strategies call it without importing this
module; it is re-exported here beside the sums it reads (6.4).
"""

from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final, Literal
from zoneinfo import ZoneInfo

from wxverify.collection.forecast_validation import FORECAST_VARIABLES
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.site_days import DayRecord, WriteKind, check_date_write
from wxverify.db.switch import site_generation_id
from wxverify.scoring.downloads import (
    ScoredDownload,
    StoredSample,
    scored_download,
    scored_version,
)
from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.metrics import metrics_from_sums as metrics_from_sums
from wxverify.scoring.pair_flags import precip_flags
from wxverify.verification.methodology import TEMP_TRUTH_MIN_HOURS
from wxverify.worker.close_p4 import ScoreSummary

#: The scoring rule each close record names (6.7); a later change to the
#: rule writes a new text.
SCORING_RULE: Final = "first-stored-download-of-local-day/1"

#: ``ScoreSummary`` keys of the two yardsticks (6.8); real feeds are keyed
#: ``feed:{feed_id}``.
PERSISTENCE_KEY: Final = "persistence"
MULTIMODEL_MEAN_KEY: Final = "multimodel_mean"

DayScoreStatus = Literal["scored", "absent", "no_hours", "not_counted"]

_DAYS_AHEAD: Final = tuple(range(8))
# Persistence's leads, 1 to 168 hours (4.6).
_PERSISTENCE_MAX_LEAD_HOURS: Final = 168
# The mean needs at least two real feeds at an hour and day ahead (4.6).
_MEAN_MIN_CONTRIBUTORS: Final = 2
_TRUTH_QUANTITIES: Final = frozenset(
    {
        "temperature_high",
        "temperature_low",
        "wind_max",
        "precip_total",
        "precip_occurrence",
    }
)
_TEMPERATURE_QUANTITIES: Final = ("temperature_high", "temperature_low")

_TRUTH_SQL: Final = (
    "SELECT quantity, value, eligible, covered_hours, rain_threshold_mm"
    " FROM daily_truth"
    " WHERE site_id = ? AND local_date = ? AND tz_generation_id = ?"
)
_HOURLY_SQL: Final = (
    "SELECT valid_at, value FROM observations"
    " WHERE site_id = ? AND variable = ? AND valid_at >= ? AND valid_at < ?"
)
_REAL_FEEDS_SQL: Final = (
    "SELECT id FROM feeds WHERE is_virtual = 0"
    " AND NOT (source = 'meteoblue' AND model = 'multimodel')"
    " ORDER BY id"
)
_YARDSTICK_SQL: Final = "SELECT id FROM feeds WHERE source = 'virtual' AND model = ?"

# Step 3 of 6.1: an earlier attempt's rows for D, so a retry replaces.
_DELETE_EARLIER_SQL: Final = (
    "DELETE FROM close_pairs WHERE site_id = ? AND local_date = ?",
    "DELETE FROM day_scores WHERE site_id = ? AND local_date = ?",
    "DELETE FROM close_record_downloads WHERE site_id = ? AND local_date = ?",
    "DELETE FROM close_records WHERE site_id = ? AND local_date = ?",
)
_INSERT_PAIR_SQL: Final = """
    INSERT INTO close_pairs
        (site_id, local_date, feed_id, variable, day_ahead, valid_at,
         lead_hours, forecast, observed, error, abs_error, sq_error,
         cat_hit, cat_false, cat_miss, cat_correct_neg, rain_threshold_mm,
         ref_sq_error, download_id, download_date, issued_at, fetched_at,
         contributors)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_INSERT_DAY_SCORE_SQL: Final = """
    INSERT INTO day_scores
        (site_id, local_date, feed_id, variable, day_ahead, status,
         download_id, download_date, fetched_at,
         n, err_n, err_sum, abs_sum, sq_sum,
         hits, false_alarms, misses, correct_negs,
         skill_n, skill_sq_sum, skill_ref_sq_sum,
         wind_fc_high, wind_fc_hours, wind_obs_high)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_INSERT_RECORD_DOWNLOAD_SQL: Final = """
    INSERT INTO close_record_downloads
        (site_id, local_date, feed_id, day_ahead, download_id, download_source,
         download_date, fetched_at, labels)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
_INSERT_RECORD_SQL: Final = """
    INSERT INTO close_records
        (site_id, local_date, attempt, truth_version, scoring_rule, scored_at)
    VALUES (?, ?, ?, ?, ?, ?)
"""


class ScoreDayRefused(Exception):
    """P1's guard refused one of ``score_day``'s write kinds for the day (6.1).

    ``score_day`` checks ``DAY_SCORES`` and ``CLOSE_RECORD`` before any other
    work; for a ``closing`` day with the close on both are accepted, so a
    refusal is a programming error, or the close is off (17).
    """

    def __init__(self, kind: WriteKind) -> None:
        super().__init__(f"the closed-day guard refused {kind} for the day")
        self.kind = kind


class ScoreDayInputMissing(Exception):
    """The day has fewer than its five ``daily_truth`` rows (6.1)."""


@dataclass(frozen=True, slots=True)
class TruthRow:
    """One of D's ``daily_truth`` rows, as ``score_day`` reads it (6.1)."""

    quantity: str
    value: float | None
    eligible: bool
    covered_hours: int
    rain_threshold_mm: float | None


@dataclass(frozen=True, slots=True)
class _Hour:
    """One ``observations`` row: its stored stamp and its value."""

    stamp: str
    value: float


@dataclass(frozen=True, slots=True)
class _Pair:
    """One closed-day pair before it is stored (6.3)."""

    variable: str
    day_ahead: int
    instant: datetime
    valid_at: str
    lead_hours: int
    forecast: float
    observed: float
    ref_sq_error: float | None
    issued_at: str | None
    contributors: int | None


@dataclass(frozen=True, slots=True)
class _Forecast:
    """A real feed's scored forecast of D at one day ahead (6.2)."""

    download: ScoredDownload
    version: Mapping[tuple[str, str], StoredSample]


_Hourly = dict[str, dict[datetime, _Hour]]
_Cells = dict[tuple[int, str, int], list[_Pair]]


def scored_variables(truth_rows: Iterable[TruthRow]) -> frozenset[str]:
    """The hourly variables scored on D, from its truth rows (6.5).

    Rain when ``precip_total`` is counted, wind when ``wind_max`` is
    counted, and temperature when D's temperature rows cover at least
    ``TEMP_TRUTH_MIN_HOURS`` hours. Temperature has no time-of-day window:
    neither row's ``eligible`` is read, only ``covered_hours`` (both rows
    carry the same count; the smaller is taken should they differ). Pure.
    """
    by_quantity = {row.quantity: row for row in truth_rows}
    scored: set[str] = set()
    rain = by_quantity.get("precip_total")
    if rain is not None and rain.eligible:
        scored.add("precip")
    wind = by_quantity.get("wind_max")
    if wind is not None and wind.eligible:
        scored.add("wind")
    covered = [
        by_quantity[quantity].covered_hours
        for quantity in _TEMPERATURE_QUANTITIES
        if quantity in by_quantity
    ]
    if covered and min(covered) >= TEMP_TRUTH_MIN_HOURS:
        scored.add("temperature")
    return frozenset(scored)


def score_day(
    conn: sqlite3.Connection,
    *,
    record: DayRecord,
    attempt: int,
    truth_version: str,
    now_utc: datetime,
) -> ScoreSummary:
    """Score the ``closing`` day ``record`` and store its scores and record.

    P3's stage 5 call point (6.1). In the caller's transaction, never
    committing: P1's guard for ``DAY_SCORES`` and ``CLOSE_RECORD`` (a
    refusal raises ``ScoreDayRefused``), D's five truth rows of the site's
    published generation (fewer raise ``ScoreDayInputMissing``), the removal
    of an earlier attempt's rows, then the scored forecasts (6.2), the pairs
    (6.3), the day scores (6.4) and the per-close record (6.7). The day's
    zone and bounds come from ``record`` only. ``scored_at`` is ``now_utc``
    to the whole second (15.2). Returns, per feed considered at D (6.2) and
    yardstick, how many of D's 8 forecasts were scored and how many absent
    (6.8).
    """
    site_id = record.site_id
    day_text = record.local_date.isoformat()
    for kind in (WriteKind.DAY_SCORES, WriteKind.CLOSE_RECORD):
        if not check_date_write(
            conn, site_id, record.local_date, kind, now_utc=now_utc
        ):
            raise ScoreDayRefused(kind)
    truth = _read_truth(conn, site_id, record.local_date)
    variables = scored_variables(truth.values())
    for sql in _DELETE_EARLIER_SQL:
        conn.execute(sql, (site_id, day_text))

    # 6.2: every candidate's scored download of D - k and its version. The
    # candidates are the real feeds less the meteoblue package row; the
    # active ones are those switched on at the site now, the mean's members.
    real_feeds = [int(row["id"]) for row in conn.execute(_REAL_FEEDS_SQL)]
    active = _active_real_feeds(conn, site_id) & set(real_feeds)
    forecasts: dict[tuple[int, int], _Forecast] = {}
    for feed_id in real_feeds:
        for k in _DAYS_AHEAD:
            download = scored_download(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                local_date=record.local_date - timedelta(days=k),
            )
            if download is None:
                continue
            forecasts[(feed_id, k)] = _Forecast(
                download=download,
                version=scored_version(
                    conn,
                    site_id=site_id,
                    feed_id=feed_id,
                    download=download,
                    start_utc=record.day_start_utc,
                    end_utc=record.day_end_utc,
                ),
            )
    # A candidate is considered at D when it is switched on now or has a
    # scored download on D-7 to D; any other gets no row at D (6.2).
    considered = [
        feed_id
        for feed_id in real_feeds
        if feed_id in active or any((feed_id, k) in forecasts for k in _DAYS_AHEAD)
    ]

    # D's hourly values, and the earlier hours persistence and each pair's
    # skill reference read (value at valid_at - lead_hours, 6.3).
    max_lead = max(
        [
            _PERSISTENCE_MAX_LEAD_HOURS,
            *(
                sample.lead_hours
                for forecast in forecasts.values()
                for sample in forecast.version.values()
            ),
        ]
    )
    hourly = _hourly_values(
        conn,
        site_id,
        variables,
        start_utc=record.day_start_utc - timedelta(hours=max_lead),
        end_utc=record.day_end_utc,
    )

    # 6.3: the pairs of each (feed, variable, day ahead) cell.
    cells: _Cells = {}
    for (feed_id, k), forecast in forecasts.items():
        for pair in _real_pairs(forecast, k, variables, hourly, record):
            cells.setdefault((feed_id, pair.variable, k), []).append(pair)
    persistence_id = _yardstick_id(conn, "_persistence")
    if persistence_id is not None:
        for pair in _persistence_pairs(variables, hourly, record):
            cells.setdefault(
                (persistence_id, pair.variable, pair.day_ahead), []
            ).append(pair)
    mean_id = _yardstick_id(conn, "_multimodel_mean")
    if mean_id is not None:
        for pair in _mean_pairs(cells, active, hourly):
            cells.setdefault((mean_id, pair.variable, pair.day_ahead), []).append(pair)

    rain = truth["precip_total"]
    threshold = (
        rain.rain_threshold_mm
        if rain.rain_threshold_mm is not None
        else truth["precip_occurrence"].rain_threshold_mm
    )
    for (feed_id, _variable, k), pairs in cells.items():
        forecast = forecasts.get((feed_id, k))
        conn.executemany(
            _INSERT_PAIR_SQL,
            [
                _pair_values(
                    pair,
                    site_id=site_id,
                    day_text=day_text,
                    feed_id=feed_id,
                    download=None if forecast is None else forecast.download,
                    download_date=record.local_date - timedelta(days=k),
                    threshold=threshold,
                )
                for pair in pairs
            ],
        )

    # 6.4: one day_scores row per (feed, variable, day ahead) considered.
    wind = truth["wind_max"]
    wind_obs_high = wind.value if wind.eligible else None
    entries = [(f"feed:{feed_id}", feed_id) for feed_id in considered]
    if persistence_id is not None:
        entries.append((PERSISTENCE_KEY, persistence_id))
    if mean_id is not None:
        entries.append((MULTIMODEL_MEAN_KEY, mean_id))
    scored_counts: dict[str, int] = {}
    absent_counts: dict[str, int] = {}
    for key, feed_id in entries:
        scored_counts[key] = 0
        absent_counts[key] = 0
        for k in _DAYS_AHEAD:
            # A yardstick has no download (4.1), so it is never in forecasts.
            forecast = forecasts.get((feed_id, k))
            download = None if forecast is None else forecast.download
            day_rows: list[tuple[object, ...]] = []
            any_scored = False
            for variable in FORECAST_VARIABLES:
                pairs = cells.get((feed_id, variable, k), [])
                status = _status(
                    counted=variable in variables,
                    paired=bool(pairs),
                    has_download=download is not None,
                )
                any_scored = any_scored or status == "scored"
                day_rows.append(
                    _day_score_values(
                        pairs,
                        site_id=site_id,
                        day_text=day_text,
                        feed_id=feed_id,
                        variable=variable,
                        day_ahead=k,
                        status=status,
                        download=download,
                        threshold=threshold,
                        wind_obs_high=wind_obs_high,
                    )
                )
            conn.executemany(_INSERT_DAY_SCORE_SQL, day_rows)
            # 6.8: a forecast counts once per day ahead, scored when any
            # variable scored on D has a pair, absent with no download (no
            # pair, for a yardstick); a variable not scored on D counts
            # neither way, and a download with no hour paired neither.
            if not variables:
                continue
            if any_scored:
                scored_counts[key] += 1
            elif download is None:
                absent_counts[key] += 1

    # 6.7: the per-close record, one download row per considered feed and
    # day ahead.
    for feed_id in considered:
        for k in _DAYS_AHEAD:
            forecast = forecasts.get((feed_id, k))
            conn.execute(
                _INSERT_RECORD_DOWNLOAD_SQL,
                _record_download_values(
                    None if forecast is None else forecast.download,
                    site_id=site_id,
                    day_text=day_text,
                    feed_id=feed_id,
                    day_ahead=k,
                ),
            )
    conn.execute(
        _INSERT_RECORD_SQL,
        (
            site_id,
            day_text,
            attempt,
            truth_version,
            SCORING_RULE,
            isoformat_utc(now_utc.replace(microsecond=0)),
        ),
    )
    return ScoreSummary(scored=scored_counts, absent=absent_counts)


def _read_truth(
    conn: sqlite3.Connection, site_id: int, local_date: date
) -> dict[str, TruthRow]:
    """D's five truth rows of the site's one time zone generation, by
    quantity, as P3's ``_truth_version`` reads them; ``ScoreDayInputMissing``
    when any is missing (or the site has no time zone generation)."""
    generation_id = site_generation_id(conn, site_id)
    rows: dict[str, TruthRow] = {}
    if generation_id is not None:
        for row in conn.execute(
            _TRUTH_SQL, (site_id, local_date.isoformat(), generation_id)
        ):
            quantity = str(row["quantity"])
            rows[quantity] = TruthRow(
                quantity=quantity,
                value=None if row["value"] is None else float(row["value"]),
                eligible=int(row["eligible"]) == 1,
                covered_hours=int(row["covered_hours"]),
                rain_threshold_mm=(
                    None
                    if row["rain_threshold_mm"] is None
                    else float(row["rain_threshold_mm"])
                ),
            )
    present = _TRUTH_QUANTITIES & rows.keys()
    if len(present) < len(_TRUTH_QUANTITIES):
        raise ScoreDayInputMissing(
            f"the day has {len(present)} of its {len(_TRUTH_QUANTITIES)} truth rows"
        )
    return rows


def _hourly_values(
    conn: sqlite3.Connection,
    site_id: int,
    variables: frozenset[str],
    *,
    start_utc: datetime,
    end_utc: datetime,
) -> _Hourly:
    """The site's hourly values of each scored variable with ``valid_at`` in
    ``[start_utc, end_utc)``, keyed by instant; a stamp that does not parse
    is left out."""
    hourly: _Hourly = {}
    for variable in sorted(variables):
        values: dict[datetime, _Hour] = {}
        for row in conn.execute(
            _HOURLY_SQL,
            (site_id, variable, isoformat_utc(start_utc), isoformat_utc(end_utc)),
        ):
            stamp = str(row["valid_at"])
            try:
                instant = parse_utc(stamp)
            except ValueError:
                continue
            values[instant] = _Hour(stamp=stamp, value=float(row["value"]))
        hourly[variable] = values
    return hourly


def _reference_sq_error(
    values: Mapping[datetime, _Hour], instant: datetime, lead: int, observed: float
) -> float | None:
    """Persistence's squared error at ``lead``: the value at ``instant`` -
    ``lead`` hours against ``observed``; ``None`` with no such value (6.3)."""
    source = values.get(instant - timedelta(hours=lead))
    if source is None:
        return None
    return (source.value - observed) ** 2


def _in_day(instant: datetime, record: DayRecord) -> bool:
    return record.day_start_utc <= instant < record.day_end_utc


def _real_pairs(
    forecast: _Forecast,
    k: int,
    variables: frozenset[str],
    hourly: _Hourly,
    record: DayRecord,
) -> list[_Pair]:
    """A real feed's pairs at day ahead ``k``: each scored-version hour of a
    scored variable that D has a value for, one per hour (6.3)."""
    chosen: dict[tuple[str, datetime], StoredSample] = {}
    for sample in forecast.version.values():
        if sample.variable not in variables:
            continue
        try:
            instant = parse_utc(sample.valid_at)
        except ValueError:
            continue
        key = (sample.variable, instant)
        current = chosen.get(key)
        # Two spellings of one hour: the newer label's row, as 4.3 keeps.
        if current is None or sample.issued_at > current.issued_at:
            chosen[key] = sample
    pairs: list[_Pair] = []
    for (variable, instant), sample in sorted(chosen.items(), key=lambda i: i[0]):
        values = hourly.get(variable, {})
        hour = values.get(instant)
        if hour is None or not _in_day(instant, record):
            continue
        pairs.append(
            _Pair(
                variable=variable,
                day_ahead=k,
                instant=instant,
                valid_at=hour.stamp,
                lead_hours=sample.lead_hours,
                forecast=sample.value,
                observed=hour.value,
                ref_sq_error=_reference_sq_error(
                    values, instant, sample.lead_hours, hour.value
                ),
                issued_at=sample.issued_at,
                contributors=None,
            )
        )
    return pairs


def _persistence_pairs(
    variables: frozenset[str], hourly: _Hourly, record: DayRecord
) -> list[_Pair]:
    """Persistence (4.6): for each hour h of D with a value and each lead L
    from 1 to 168, the value at h - L where there is one, at day ahead the
    local-date difference between h - L and h in the record's zone."""
    zone = ZoneInfo(record.timezone)
    pairs: list[_Pair] = []
    for variable in sorted(variables):
        values = hourly.get(variable, {})
        for instant, hour in sorted(values.items(), key=lambda i: i[0]):
            if not _in_day(instant, record):
                continue
            target_date = instant.astimezone(zone).date()
            for lead in range(1, _PERSISTENCE_MAX_LEAD_HOURS + 1):
                source_instant = instant - timedelta(hours=lead)
                source = values.get(source_instant)
                if source is None:
                    continue
                k = (target_date - source_instant.astimezone(zone).date()).days
                if k not in _DAYS_AHEAD:
                    continue
                pairs.append(
                    _Pair(
                        variable=variable,
                        day_ahead=k,
                        instant=instant,
                        valid_at=hour.stamp,
                        lead_hours=lead,
                        forecast=source.value,
                        observed=hour.value,
                        ref_sq_error=(source.value - hour.value) ** 2,
                        issued_at=source.stamp,
                        contributors=None,
                    )
                )
    return pairs


def _mean_pairs(cells: _Cells, active: set[int], hourly: _Hourly) -> list[_Pair]:
    """The multi-model mean (4.6): at each hour and day ahead, the mean of
    the active real feeds' scored forecasts where at least two exist, at
    the smallest of their leads."""
    groups: dict[tuple[str, int, datetime], list[_Pair]] = {}
    for (feed_id, _variable, _k), pairs in sorted(cells.items(), key=lambda i: i[0]):
        if feed_id not in active:
            continue
        for pair in pairs:
            groups.setdefault((pair.variable, pair.day_ahead, pair.instant), []).append(
                pair
            )
    means: list[_Pair] = []
    for (variable, k, instant), members in sorted(groups.items(), key=lambda i: i[0]):
        if len(members) < _MEAN_MIN_CONTRIBUTORS:
            continue
        lead = min(member.lead_hours for member in members)
        observed = members[0].observed
        means.append(
            _Pair(
                variable=variable,
                day_ahead=k,
                instant=instant,
                valid_at=members[0].valid_at,
                lead_hours=lead,
                forecast=math.fsum(member.forecast for member in members)
                / len(members),
                observed=observed,
                ref_sq_error=_reference_sq_error(
                    hourly.get(variable, {}), instant, lead, observed
                ),
                issued_at=None,
                contributors=len(members),
            )
        )
    return means


def _yardstick_id(conn: sqlite3.Connection, model: str) -> int | None:
    row = conn.execute(_YARDSTICK_SQL, (model,)).fetchone()
    return None if row is None else int(row["id"])


def _active_real_feeds(conn: sqlite3.Connection, site_id: int) -> set[int]:
    """The real feeds active at the site now (``active_competitor_clause``)."""
    return {
        int(row["id"])
        for row in conn.execute(
            f"""
            SELECT f.id
            FROM feeds f
            LEFT JOIN site_feed_state sfs
              ON sfs.site_id = ? AND sfs.feed_id = f.id
            WHERE f.is_virtual = 0
              AND {active_competitor_clause(site_expr="?")}
            """,
            (site_id, site_id),
        )
    }


def _pair_values(
    pair: _Pair,
    *,
    site_id: int,
    day_text: str,
    feed_id: int,
    download: ScoredDownload | None,
    download_date: date,
    threshold: float | None,
) -> tuple[object, ...]:
    """One ``close_pairs`` row in ``_INSERT_PAIR_SQL`` order (6.3)."""
    rain_threshold = threshold if pair.variable == "precip" else None
    hit, false, miss, correct_neg = precip_flags(
        pair.variable, pair.forecast, pair.observed, rain_threshold
    )
    error = pair.forecast - pair.observed
    return (
        site_id,
        day_text,
        feed_id,
        pair.variable,
        pair.day_ahead,
        pair.valid_at,
        pair.lead_hours,
        pair.forecast,
        pair.observed,
        error,
        abs(error),
        error**2,
        hit,
        false,
        miss,
        correct_neg,
        rain_threshold,
        pair.ref_sq_error,
        None if download is None else download.download_id,
        (download_date if download is None else download.local_date).isoformat(),
        pair.issued_at,
        None if download is None else download.fetched_at,
        pair.contributors,
    )


def _status(*, counted: bool, paired: bool, has_download: bool) -> DayScoreStatus:
    """A day-score row's status (6.4). A yardstick has no download, so with
    no pair it is ``absent``: there was no forecast to score."""
    if not counted:
        return "not_counted"
    if paired:
        return "scored"
    if has_download:
        return "no_hours"
    return "absent"


def _sql_sum(values: list[float]) -> float | None:
    """SQL ``SUM`` over non-NULL values: ``None`` when there are none."""
    return math.fsum(values) if values else None


def _day_score_values(
    pairs: list[_Pair],
    *,
    site_id: int,
    day_text: str,
    feed_id: int,
    variable: str,
    day_ahead: int,
    status: DayScoreStatus,
    download: ScoredDownload | None,
    threshold: float | None,
    wind_obs_high: float | None,
) -> tuple[object, ...]:
    """One ``day_scores`` row in ``_INSERT_DAY_SCORE_SQL`` order (6.4): the
    count and sums of the cell's pairs, as SQL would sum the stored rows."""
    errors = [pair.forecast - pair.observed for pair in pairs]
    rain_threshold = threshold if variable == "precip" else None
    flag_columns: list[list[int]] = [[], [], [], []]
    for pair in pairs:
        flags = precip_flags(variable, pair.forecast, pair.observed, rain_threshold)
        for column, flag in zip(flag_columns, flags, strict=True):
            if flag is not None:
                column.append(flag)
    contingency = [sum(column) if column else None for column in flag_columns]
    skill_sq: list[float] = []
    skill_ref: list[float] = []
    for pair in pairs:
        if pair.ref_sq_error is not None:
            skill_sq.append((pair.forecast - pair.observed) ** 2)
            skill_ref.append(pair.ref_sq_error)
    is_wind = variable == "wind"
    return (
        site_id,
        day_text,
        feed_id,
        variable,
        day_ahead,
        status,
        None if download is None else download.download_id,
        None if download is None else download.local_date.isoformat(),
        None if download is None else download.fetched_at,
        len(pairs),
        len(errors),
        _sql_sum(errors),
        _sql_sum([abs(error) for error in errors]),
        _sql_sum([error**2 for error in errors]),
        *contingency,
        len(skill_sq),
        _sql_sum(skill_sq),
        _sql_sum(skill_ref),
        (max(pair.forecast for pair in pairs) if pairs else None) if is_wind else None,
        len({pair.instant for pair in pairs})
        if is_wind and status in ("scored", "no_hours")
        else None,
        wind_obs_high if is_wind else None,
    )


def _record_download_values(
    download: ScoredDownload | None,
    *,
    site_id: int,
    day_text: str,
    feed_id: int,
    day_ahead: int,
) -> tuple[object, ...]:
    """One ``close_record_downloads`` row (6.7): NULL download fields for an
    absent forecast, so the record says what was missing."""
    if download is None:
        return (site_id, day_text, feed_id, day_ahead, None, None, None, None, None)
    labels = {str(feed_id): download.labels.get(str(feed_id), [])}
    return (
        site_id,
        day_text,
        feed_id,
        day_ahead,
        download.download_id,
        download.source,
        download.local_date.isoformat(),
        download.fetched_at,
        json.dumps(labels, sort_keys=True, separators=(",", ":")),
    )
