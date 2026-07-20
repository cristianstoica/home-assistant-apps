"""View-model builder for the Forecast home page.

Glue layer: pulls latest-run future samples (:mod:`wxverify.forecast.data`),
selects winners per (variable, display-day) cell
(:mod:`wxverify.forecast.selection`), aggregates and blends
(:mod:`wxverify.forecast.aggregate`), and shapes the result for templates and
the hourly JSON API. Pure read side — never writes.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import (
    day_ahead as issue_day_ahead,
)
from wxverify.core.timeutil import (
    floor_hour,
    isoformat_utc,
    parse_utc,
    utc_now,
)
from wxverify.core.units import ms_to_kmh
from wxverify.forecast.aggregate import (
    blend_mean,
    chance_of_rain,
    clears_coverage,
    covered_hours,
    display_day_index,
    wet_share,
)
from wxverify.forecast.data import (
    FutureSampleRow,
    forecast_ranking,
    load_feed_freshness,
    load_future_samples,
    samples_fingerprint,
)
from wxverify.forecast.selection import (
    CellCandidate,
    CellSelection,
    representative_day_ahead,
    select_cell_feeds,
)
from wxverify.scoring.leaderboard import LeaderboardRow
from wxverify.settings.keys import get_number_setting
from wxverify.web.context import feed_label

DAY_COUNT = 8
VARIABLES = ("temperature", "wind", "precip")
# Rain glyph appears when the blended chance is meaningful, i.e. a nontrivial
# share of the day is expected wet — not on every 3% drizzle share.
RAIN_GLYPH_MIN_CHANCE_PCT = 25


@dataclass(frozen=True)
class FeedRef:
    feed_id: int
    label: str


@dataclass(frozen=True)
class CellMeta:
    """Shared per-variable cell state: availability, ladder use, badges."""

    state: str  # "normal" | "low_confidence" | "not_available"
    feeds: list[FeedRef]
    partial: bool
    stale: bool

    @property
    def available(self) -> bool:
        return self.state != "not_available"

    @property
    def feed_labels(self) -> str:
        return ", ".join(ref.label for ref in self.feeds)


@dataclass(frozen=True)
class TempCell:
    meta: CellMeta
    high_c: float | None
    low_c: float | None


@dataclass(frozen=True)
class WindCell:
    meta: CellMeta
    max_kmh: float | None


@dataclass(frozen=True)
class PrecipCell:
    meta: CellMeta
    total_mm: float | None
    chance_pct: int | None
    show_rain_glyph: bool


@dataclass(frozen=True)
class DayTile:
    day_index: int
    label: str
    date_iso: str
    temp: TempCell
    wind: WindCell
    precip: PrecipCell
    state: str  # tile-level: "normal" | "low_confidence" | "not_available"
    stale: bool
    partial: bool


@dataclass(frozen=True)
class ForecastView:
    empty: bool
    tiles: list[DayTile]
    updated_at: str | None
    updated_ago: str | None
    fingerprint: str


# variable -> display day -> feed_id -> samples
_Grouped = dict[str, dict[int, dict[int, list[FutureSampleRow]]]]
_RankCache = dict[tuple[str, int], dict[int, LeaderboardRow]]


def build_forecast(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    timezone: str,
    rain_threshold_mm: float,
    now: datetime | None = None,
) -> ForecastView:
    """Build the full 8-tile Forecast view model for one site."""
    at = now or utc_now()
    samples = load_future_samples(
        conn, site_id=site_id, since_valid_at=isoformat_utc(floor_hour(at))
    )
    fingerprint = samples_fingerprint(conn, site_id=site_id)
    if not samples:
        return ForecastView(
            empty=True,
            tiles=[],
            updated_at=None,
            updated_ago=None,
            fingerprint=fingerprint,
        )
    grouped = _group_samples(samples, timezone=timezone, now=at)
    freshness = load_feed_freshness(conn, site_id=site_id, now=at)
    stale_ids = {feed_id for feed_id, row in freshness.items() if row.stale}
    blend_depth = get_number_setting(conn, "forecast_blend_depth", 2, minimum=1)
    rank_cache: _RankCache = {}

    tz = ZoneInfo(timezone)
    today = at.astimezone(tz).date()
    tiles: list[DayTile] = []
    for day in range(DAY_COUNT):
        cells: dict[str, tuple[CellMeta, CellSelection, dict[int, list[float]]]] = {}
        for variable in VARIABLES:
            feeds_samples = grouped.get(variable, {}).get(day, {})
            selection = _select(
                conn,
                site_id=site_id,
                variable=variable,
                timezone=timezone,
                feeds_samples=feeds_samples,
                blend_depth=blend_depth,
                rank_cache=rank_cache,
            )
            meta, values = _cell_meta_and_values(
                selection,
                feeds_samples=feeds_samples,
                timezone=timezone,
                stale_ids=stale_ids,
            )
            cells[variable] = (meta, selection, values)
        tiles.append(
            _build_tile(
                day,
                date_iso=(today + timedelta(days=day)).isoformat(),
                label=_day_label(day, today + timedelta(days=day)),
                cells=cells,
                rain_threshold_mm=rain_threshold_mm,
            )
        )
    updated_at = max(sample.issued_at for sample in samples)
    return ForecastView(
        empty=False,
        tiles=tiles,
        updated_at=updated_at,
        updated_ago=relative_ago(updated_at, now=at),
        fingerprint=fingerprint,
    )


def build_hourly(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    timezone: str,
    day: int,
    now: datetime | None = None,
) -> dict[str, object]:
    """Blended hourly drill-down payload for one display day.

    Per-variable winner sets are the SAME selections the tile used; the blend
    at each hour averages the selected feeds that cover that hour. Per-feed
    series ride along for the "show individual feeds" toggle.
    """
    at = now or utc_now()
    samples = load_future_samples(
        conn, site_id=site_id, since_valid_at=isoformat_utc(floor_hour(at))
    )
    grouped = _group_samples(samples, timezone=timezone, now=at)
    blend_depth = get_number_setting(conn, "forecast_blend_depth", 2, minimum=1)
    rank_cache: _RankCache = {}

    selections: dict[str, CellSelection] = {}
    for variable in VARIABLES:
        feeds_samples = grouped.get(variable, {}).get(day, {})
        selections[variable] = _select(
            conn,
            site_id=site_id,
            variable=variable,
            timezone=timezone,
            feeds_samples=feeds_samples,
            blend_depth=blend_depth,
            rank_cache=rank_cache,
        )

    # Hour axis: union of covered hours across every selected feed/variable.
    hour_set: set[str] = set()
    for variable in VARIABLES:
        feeds_samples = grouped.get(variable, {}).get(day, {})
        for candidate in selections[variable].feeds:
            for sample in feeds_samples.get(candidate.feed_id, []):
                hour_set.add(sample.valid_at)
    hours = sorted(hour_set)
    index = {valid_at: i for i, valid_at in enumerate(hours)}

    def series_for(variable: str, feed_id: int) -> list[float | None]:
        feeds_samples = grouped.get(variable, {}).get(day, {})
        out: list[float | None] = [None] * len(hours)
        for sample in feeds_samples.get(feed_id, []):
            value = sample.value
            if variable == "wind":
                value = ms_to_kmh(value)
            out[index[sample.valid_at]] = value
        return out

    def blend_series(variable: str) -> list[float | None]:
        per_feed = [
            series_for(variable, candidate.feed_id)
            for candidate in selections[variable].feeds
        ]
        out: list[float | None] = []
        for i in range(len(hours)):
            values = [s[i] for s in per_feed if s[i] is not None]
            out.append(blend_mean([v for v in values if v is not None]))
        return out

    feed_order: list[tuple[int, str]] = []
    seen: set[int] = set()
    for variable in VARIABLES:
        for candidate in selections[variable].feeds:
            if candidate.feed_id not in seen:
                seen.add(candidate.feed_id)
                feed_order.append(
                    (candidate.feed_id, feed_label(candidate.source, candidate.model))
                )

    tz = ZoneInfo(timezone)
    today = at.astimezone(tz).date()
    return {
        "site_id": site_id,
        "day": day,
        "label": _day_label(day, today + timedelta(days=day)),
        "hours": hours,
        "blend": {
            "temp_c": blend_series("temperature"),
            "wind_kmh": blend_series("wind"),
            "precip_mm": blend_series("precip"),
        },
        "feeds": [
            {
                "feed_id": feed_id,
                "label": label,
                "temp_c": series_for("temperature", feed_id),
                "wind_kmh": series_for("wind", feed_id),
                "precip_mm": series_for("precip", feed_id),
            }
            for feed_id, label in feed_order
        ],
        "states": {variable: _state_of(selections[variable]) for variable in VARIABLES},
    }


def _state_of(selection: CellSelection) -> str:
    if not selection.available:
        return "not_available"
    return "low_confidence" if selection.low_confidence else "normal"


def relative_ago(timestamp: str, *, now: datetime) -> str:
    """Human 'Updated X ago' text for a UTC ISO timestamp."""
    seconds = (now - parse_utc(timestamp)).total_seconds()
    if seconds < 60:
        return "just now"
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes} min ago"
    hours = int(seconds // 3600)
    if hours < 24:
        return f"{hours} h ago"
    return f"{int(seconds // 86400)} d ago"


def _group_samples(
    samples: list[FutureSampleRow], *, timezone: str, now: datetime
) -> _Grouped:
    grouped: _Grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for sample in samples:
        day = display_day_index(sample.valid_at, timezone=timezone, now=now)
        if 0 <= day < DAY_COUNT:
            grouped[sample.variable][day][sample.feed_id].append(sample)
    return grouped


def _select(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    timezone: str,
    feeds_samples: dict[int, list[FutureSampleRow]],
    blend_depth: int,
    rank_cache: _RankCache,
) -> CellSelection:
    """Build candidates for one cell and run the fallback ladder."""
    candidates: list[CellCandidate] = []
    for feed_id, feed_samples in feeds_samples.items():
        rep = representative_day_ahead(
            [
                issue_day_ahead(sample.issued_at, sample.valid_at, timezone)
                for sample in feed_samples
            ]
        )
        key = (variable, rep)
        if key not in rank_cache:
            rank_cache[key] = forecast_ranking(
                conn, site_id=site_id, variable=variable, day_ahead=rep
            )
        row = rank_cache[key].get(feed_id)
        candidates.append(
            CellCandidate(
                feed_id=feed_id,
                source=feed_samples[0].source,
                model=feed_samples[0].model,
                confident=row.confident if row is not None else False,
                skill_score=row.skill_score if row is not None else None,
                pair_n=row.n if row is not None else 0,
                mae=row.mae if row is not None else None,
                future_sample_count=len(feed_samples),
            )
        )
    return select_cell_feeds(candidates, blend_depth=blend_depth)


def _cell_meta_and_values(
    selection: CellSelection,
    *,
    feeds_samples: dict[int, list[FutureSampleRow]],
    timezone: str,
    stale_ids: set[int],
) -> tuple[CellMeta, dict[int, list[float]]]:
    """Apply the coverage guard; return cell meta + per-feed value lists.

    Feeds clearing the >= 18-hour guard aggregate alone; when NO selected feed
    clears it, the partial data still aggregates (the tile stays populated)
    and the cell carries the orthogonal "partial" badge.
    """
    if not selection.available:
        return (
            CellMeta(state="not_available", feeds=[], partial=False, stale=False),
            {},
        )
    clearing = [
        candidate
        for candidate in selection.feeds
        if clears_coverage(
            covered_hours(
                (s.valid_at for s in feeds_samples[candidate.feed_id]),
                timezone=timezone,
            )
        )
    ]
    partial = not clearing
    agg_feeds = clearing if clearing else selection.feeds
    values = {
        candidate.feed_id: [s.value for s in feeds_samples[candidate.feed_id]]
        for candidate in agg_feeds
    }
    meta = CellMeta(
        state="low_confidence" if selection.low_confidence else "normal",
        feeds=[
            FeedRef(
                feed_id=candidate.feed_id,
                label=feed_label(candidate.source, candidate.model),
            )
            for candidate in agg_feeds
        ],
        partial=partial,
        stale=any(candidate.feed_id in stale_ids for candidate in agg_feeds),
    )
    return meta, values


def _build_tile(
    day: int,
    *,
    date_iso: str,
    label: str,
    cells: dict[str, tuple[CellMeta, CellSelection, dict[int, list[float]]]],
    rain_threshold_mm: float,
) -> DayTile:
    temp_meta, _, temp_values = cells["temperature"]
    wind_meta, _, wind_values = cells["wind"]
    precip_meta, _, precip_values = cells["precip"]

    temp = TempCell(
        meta=temp_meta,
        high_c=blend_mean([max(v) for v in temp_values.values() if v]),
        low_c=blend_mean([min(v) for v in temp_values.values() if v]),
    )
    wind_max_ms = blend_mean([max(v) for v in wind_values.values() if v])
    wind = WindCell(
        meta=wind_meta,
        max_kmh=None if wind_max_ms is None else ms_to_kmh(wind_max_ms),
    )
    shares = [
        share
        for share in (
            wet_share(v, threshold_mm=rain_threshold_mm) for v in precip_values.values()
        )
        if share is not None
    ]
    chance = chance_of_rain(shares)
    chance_pct = None if chance is None else round(chance * 100)
    precip = PrecipCell(
        meta=precip_meta,
        total_mm=blend_mean([sum(v) for v in precip_values.values() if v]),
        chance_pct=chance_pct,
        show_rain_glyph=chance_pct is not None
        and chance_pct >= RAIN_GLYPH_MIN_CHANCE_PCT,
    )
    metas = (temp_meta, wind_meta, precip_meta)
    populated = [meta for meta in metas if meta.available]
    if not populated:
        state = "not_available"
    elif any(meta.state == "low_confidence" for meta in populated):
        state = "low_confidence"
    else:
        state = "normal"
    return DayTile(
        day_index=day,
        label=label,
        date_iso=date_iso,
        temp=temp,
        wind=wind,
        precip=precip,
        state=state,
        stale=any(meta.stale for meta in populated),
        partial=any(meta.partial for meta in populated),
    )


def _day_label(day: int, local_date: date) -> str:
    if day == 0:
        return "Today"
    if day == 1:
        return "Tomorrow"
    return local_date.strftime("%A")
