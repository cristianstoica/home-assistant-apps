"""Scoring the closed day: P3's stage 5.

Daily-close P4 plan (``plans/2026-10-22-daily-close-pairs-and-publication.md``)
section 6. ``score_day`` scores one ``closing`` day D in the transaction P3
passes: for every real feed considered at D (6.2) and day ahead k it takes
the scored values the first stored download of D - k left in
``forecast_scored`` (4.1, 4.3; rain release Part B, ``scoring/downloads.py``
``scored_values``), pairs them with D's hourly values, makes the two yardsticks
(4.6), and stores the scored hours (``forecast_history``: real feeds and the
mean, never persistence; storage release 3.3), the day's sums
(``day_scores``) and the per-close record (``close_records``,
``close_record_downloads``), replacing an earlier attempt's rows. Since the
rain release (Part C, C1) it also writes D's rain nearby (``rain_days``) and
each real feed's daily rain total per day ahead (``rain_forecast_days``),
which the publication's rain ranking reads. It never
commits, enqueues nothing, changes no day state, fetches nothing and reads
the clock only through ``now_utc``.

``metrics_from_sums`` lives in ``scoring/metrics.py`` beside
``MetricResult``; it is re-exported here beside the sums it reads (6.4).
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
from wxverify.db.site_days import (
    DayRecord,
    WriteKind,
    check_date_write,
    day_rain_station_ids,
)
from wxverify.db.switch import site_generation_id
from wxverify.forecast.aggregate import covers_local_day_exactly
from wxverify.scoring.downloads import (
    ScoredDownload,
    StoredSample,
    scored_values,
    scored_values_download,
)
from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.history import PERSISTENCE_MAX_LEAD_HOURS, persistence_cells
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
# ``forecast_history`` has its own delete below.
_DELETE_EARLIER_SQL: Final = (
    "DELETE FROM day_scores WHERE site_id = ? AND local_date = ?",
    "DELETE FROM close_record_downloads WHERE site_id = ? AND local_date = ?",
    "DELETE FROM close_records WHERE site_id = ? AND local_date = ?",
    "DELETE FROM rain_days WHERE site_id = ? AND local_date = ?",
    "DELETE FROM rain_forecast_days WHERE site_id = ? AND local_date = ?",
)
# Storage release 3.6: an earlier attempt's ``forecast_history`` rows for D,
# one primary-key seek per feed, variable and day ahead. The ``valid_at``
# bounds are D's widened by one day each side and only let the key seek;
# ``local_date`` decides. Parameters: site, feed, variable, day ahead, the
# two bounds, D.
_DELETE_FORECAST_HISTORY_SQL: Final = (
    "DELETE FROM forecast_history"
    " WHERE site_id = ? AND feed_id = ? AND variable = ? AND day_ahead = ?"
    " AND valid_at >= ? AND valid_at < ? AND local_date = ?"
)
_ALL_FEEDS_SQL: Final = "SELECT id FROM feeds ORDER BY id"
# Rain nearby (rain release, C1): the day's ``ok`` station rain rows. The
# station list is bound in Python, so the read seeks each station's key.
_STATION_RAIN_SQL: Final = (
    "SELECT valid_at, value FROM station_observations"
    " WHERE station_id = ? AND variable = 'precip' AND qc_flag = 'ok'"
    " AND valid_at >= ? AND valid_at < ?"
)
_INSERT_RAIN_DAY_SQL: Final = """
    INSERT INTO rain_days
        (site_id, local_date, outcome, stations_full, stations_partial)
    VALUES (?, ?, ?, ?, ?)
"""
_INSERT_RAIN_FORECAST_SQL: Final = """
    INSERT INTO rain_forecast_days
        (site_id, local_date, feed_id, day_ahead, total_mm)
    VALUES (?, ?, ?, ?, ?)
"""
#: Rain nearby: this many stations at ``RAIN_NEARBY_PAIR_HUNDREDTHS`` or
#: more, or one at ``RAIN_NEARBY_SINGLE_HUNDREDTHS`` or more, in hundredths.
RAIN_NEARBY_PAIR_STATIONS: Final = 2
RAIN_NEARBY_PAIR_HUNDREDTHS: Final = 20
RAIN_NEARBY_SINGLE_HUNDREDTHS: Final = 50
#: A dry day needs a station with this many hours of rain readings.
RAIN_DRY_MIN_HOURS: Final = 22

RainOutcome = Literal["rain", "dry", "unknown"]
_INSERT_HISTORY_SQL: Final = """
    INSERT INTO forecast_history
        (site_id, feed_id, variable, day_ahead, valid_at, local_date,
         value, lead_hours, issued_at, contributors)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    work; for a ``closing`` day both are accepted, so a refusal is a
    programming error.
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


def rain_nearby(stations: Iterable[tuple[float, int]]) -> tuple[RainOutcome, int, int]:
    """A day's rain nearby from its stations' (day total, hours) (C1).

    Totals are compared in hundredths (``round(total * 100)``). Rain: at
    least ``RAIN_NEARBY_PAIR_STATIONS`` stations at 0.2 mm or more, or one
    at 0.5 mm or more; a station with partial coverage counts. Dry: not
    rain, and a station has ``RAIN_DRY_MIN_HOURS`` hours or more. Unknown
    otherwise, never dry. Returns the outcome, the stations with that many
    hours (full) and the stations with fewer (partial). Pure.
    """
    pair = single = full = partial = 0
    for total, hours in stations:
        if hours <= 0:
            continue
        hundredths = round(total * 100)
        if hundredths >= RAIN_NEARBY_PAIR_HUNDREDTHS:
            pair += 1
        if hundredths >= RAIN_NEARBY_SINGLE_HUNDREDTHS:
            single += 1
        if hours >= RAIN_DRY_MIN_HOURS:
            full += 1
        else:
            partial += 1
    outcome: RainOutcome
    if pair >= RAIN_NEARBY_PAIR_STATIONS or single >= 1:
        outcome = "rain"
    elif full >= 1:
        outcome = "dry"
    else:
        outcome = "unknown"
    return outcome, full, partial


def day_rain_total(
    samples: Iterable[StoredSample], *, local_date: date, timezone: str
) -> float | None:
    """A model's daily rain total for ``local_date``, or ``None`` (C1).

    The sum of its ``precip`` values when they cover each hour of the local
    day exactly once (``covers_local_day_exactly``); ``None`` otherwise, so a
    partial day is never a total. Pure.
    """
    rain = [sample for sample in samples if sample.variable == "precip"]
    if not rain or not covers_local_day_exactly(
        (sample.valid_at for sample in rain), local_date=local_date, timezone=timezone
    ):
        return None
    return math.fsum(sample.value for sample in rain)


def _write_rain_day(conn: sqlite3.Connection, record: DayRecord) -> None:
    """D's ``rain_days`` row from its rain-counted stations' ``ok`` rows (C1)."""
    start = isoformat_utc(record.day_start_utc)
    end = isoformat_utc(record.day_end_utc)
    stations: list[tuple[float, int]] = []
    for station_id in sorted(
        day_rain_station_ids(conn, record.site_id, record.local_date)
    ):
        values: dict[str, float] = {}
        for row in conn.execute(_STATION_RAIN_SQL, (station_id, start, end)):
            values[str(row["valid_at"])] = float(row["value"])
        stations.append((math.fsum(values.values()), len(values)))
    outcome, full, partial = rain_nearby(stations)
    conn.execute(
        _INSERT_RAIN_DAY_SQL,
        (record.site_id, record.local_date.isoformat(), outcome, full, partial),
    )


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
    zone and bounds come from ``record`` only. D's ``rain_days`` row and its
    ``rain_forecast_days`` rows (rain release, C1) are written in the same
    call. ``scored_at`` is ``now_utc``
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
    _delete_earlier_history(conn, record)

    # 6.2: every candidate's scored values of D at each day ahead k, from
    # forecast_scored (rain release, Part B, B4), with the download they
    # came from. The candidates are the real feeds less the meteoblue
    # package row; the active ones are those switched on at the site now,
    # the mean's members.
    real_feeds = [int(row["id"]) for row in conn.execute(_REAL_FEEDS_SQL)]
    active = active_real_feeds(conn, site_id) & set(real_feeds)
    forecasts: dict[tuple[int, int], _Forecast] = {}
    for feed_id in real_feeds:
        for k in _DAYS_AHEAD:
            download = scored_values_download(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                day_ahead=k,
                start_utc=record.day_start_utc,
                end_utc=record.day_end_utc,
            )
            if download is None:
                continue
            forecasts[(feed_id, k)] = _Forecast(
                download=download,
                version=scored_values(
                    conn,
                    site_id=site_id,
                    feed_id=feed_id,
                    day_ahead=k,
                    start_utc=record.day_start_utc,
                    end_utc=record.day_end_utc,
                ),
            )
    # A candidate is considered at D when it is switched on now or has
    # scored values for D at some day ahead; any other gets no row at D
    # (6.2). A download that does not reach D counts as no download for D.
    considered = [
        feed_id
        for feed_id in real_feeds
        if feed_id in active or any((feed_id, k) in forecasts for k in _DAYS_AHEAD)
    ]

    # C1 (rain release): D's rain nearby, and each real feed's daily rain
    # total at each day ahead whose scored values cover D's every hour.
    _write_rain_day(conn, record)
    rain_totals: list[tuple[object, ...]] = []
    for (feed_id, k), forecast in sorted(forecasts.items()):
        total = day_rain_total(
            forecast.version.values(),
            local_date=record.local_date,
            timezone=record.timezone,
        )
        if total is not None:
            rain_totals.append((site_id, day_text, feed_id, k, total))
    conn.executemany(_INSERT_RAIN_FORECAST_SQL, rain_totals)

    # D's hourly values, and the earlier hours persistence and each pair's
    # skill reference read (value at valid_at - lead_hours, 6.3).
    max_lead = max(
        [
            PERSISTENCE_MAX_LEAD_HOURS,
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
    # Storage release 3.3: the scored hours of the real feeds and the mean
    # go to forecast_history. Persistence's pairs feed the day scores only;
    # its history is rebuilt on read (3.5).
    conn.executemany(
        _INSERT_HISTORY_SQL,
        [
            _history_values(pair, site_id=site_id, day_text=day_text, feed_id=feed_id)
            for (feed_id, _variable, _k), pairs in cells.items()
            if feed_id != persistence_id
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


def _delete_earlier_history(conn: sqlite3.Connection, record: DayRecord) -> None:
    """Remove an earlier attempt's ``forecast_history`` rows for D (storage
    release 3.6): ``_DELETE_FORECAST_HISTORY_SQL`` for every feed, variable
    and day ahead, so each delete seeks the primary key and none scans."""
    low = isoformat_utc(record.day_start_utc - timedelta(days=1))
    high = isoformat_utc(record.day_end_utc + timedelta(days=1))
    day_text = record.local_date.isoformat()
    feed_ids = [int(row["id"]) for row in conn.execute(_ALL_FEEDS_SQL)]
    conn.executemany(
        _DELETE_FORECAST_HISTORY_SQL,
        [
            (record.site_id, feed_id, variable, k, low, high, day_text)
            for feed_id in feed_ids
            for variable in FORECAST_VARIABLES
            for k in _DAYS_AHEAD
        ],
    )


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
    """A real feed's pairs at day ahead ``k``: each scored hour of a scored
    variable that D has a value for, one per hour (6.3); ``forecast_scored``
    holds one row per hour (rain release, Part B, B4)."""
    chosen: dict[tuple[str, datetime], StoredSample] = {}
    for sample in forecast.version.values():
        if sample.variable not in variables:
            continue
        try:
            instant = parse_utc(sample.valid_at)
        except ValueError:
            continue
        chosen[(sample.variable, instant)] = sample
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
    local-date difference between h - L and h in the record's zone. The
    cells come from the shared definition (``scoring.history``
    ``persistence_cells``), taken by variable, hour and lead."""
    zone = ZoneInfo(record.timezone)
    pairs: list[_Pair] = []
    for variable in sorted(variables):
        values = hourly.get(variable, {})
        cells = persistence_cells(
            [(hour.stamp, hour.value) for hour in values.values()],
            target_valid_at=[
                hour.stamp
                for instant, hour in values.items()
                if _in_day(instant, record)
            ],
            zone=zone,
            max_lead_hours=PERSISTENCE_MAX_LEAD_HOURS,
        )
        for cell in sorted(cells, key=lambda c: (c.instant, c.lead_hours)):
            pairs.append(
                _Pair(
                    variable=variable,
                    day_ahead=cell.day_ahead,
                    instant=cell.instant,
                    valid_at=cell.valid_at,
                    lead_hours=cell.lead_hours,
                    forecast=cell.forecast,
                    observed=cell.observed,
                    ref_sq_error=(cell.forecast - cell.observed) ** 2,
                    issued_at=cell.source_valid_at,
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


def active_real_feeds(conn: sqlite3.Connection, site_id: int) -> set[int]:
    """The real feeds active at the site now (``active_competitor_clause``),
    less the meteoblue package row: the feeds the close scores that are
    switched on. The close's multi-model mean and the publication's rain
    ranking (C3) read them."""
    return {
        int(row["id"])
        for row in conn.execute(
            f"""
            SELECT f.id
            FROM feeds f
            LEFT JOIN site_feed_state sfs
              ON sfs.site_id = ? AND sfs.feed_id = f.id
            WHERE f.is_virtual = 0
              AND NOT (f.source = 'meteoblue' AND f.model = 'multimodel')
              AND {active_competitor_clause(site_expr="?")}
            """,
            (site_id, site_id),
        )
    }


def _history_values(
    pair: _Pair, *, site_id: int, day_text: str, feed_id: int
) -> tuple[object, ...]:
    """One ``forecast_history`` row in ``_INSERT_HISTORY_SQL`` order
    (storage release 3.1, 3.3): ``valid_at`` is the observation hour's stored
    text; the mean's ``issued_at`` is ``None`` and its ``contributors`` set."""
    return (
        site_id,
        feed_id,
        pair.variable,
        pair.day_ahead,
        pair.valid_at,
        day_text,
        pair.forecast,
        pair.lead_hours,
        pair.issued_at,
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
