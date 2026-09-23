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
    isoformat_utc,
    local_day_start,
    parse_utc,
    utc_now,
)
from wxverify.core.units import ms_to_kmh
from wxverify.forecast.aggregate import (
    EXTREMA_COVERAGE_COMPLETE,
    EXTREMA_COVERAGE_INSUFFICIENT,
    blend_mean,
    clearing_subset,
    covered_hours,
    covers_local_day,
    covers_local_day_exactly,
    display_day_index,
    displayed_daily,
    fixed_membership_series,
)
from wxverify.forecast.data import (
    ForecastRanking,
    FutureSampleRow,
    forecast_ranking_with_status,
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
from wxverify.settings.depth import DEPTH_VARIABLES, effective_blend_depths
from wxverify.web.context import feed_label

DAY_COUNT = 8
#: Page column order = the canonical roster (NB-4).
VARIABLES = DEPTH_VARIABLES
# Rain glyph appears when a nontrivial part of the day is expected wet — not
# on a single drizzly hour: when the displayed, rounded wet-hour count is at
# least six, so the glyph always agrees with the "~N h wet" figure. Six hours
# is a quarter of a 24-hour day, but the rule is not equivalent to the 25%
# trigger it replaces: a blended count of 5.5 rounds to 6 and shows the
# glyph, where the old rounded percentage (23%) did not.
RAIN_GLYPH_MIN_WET_HOURS = 6


@dataclass(frozen=True)
class FeedRef:
    feed_id: int
    label: str


@dataclass(frozen=True)
class CellMeta:
    """Shared per-variable cell state: availability, ladder use, badges.

    ``extrema_unavailable`` is True when the cell is available but its
    displayed daily values are suppressed: a temperature cell's high and low
    when no feed covers the whole local day, a precipitation cell's total and
    wet-hour count when no feed supplies each hour of the local day exactly
    once. Temperature and precipitation can set it; wind cannot. It is a
    separate flag rather than a ``state`` value because ``state`` describes
    the cell's hourly product: suppression leaves ``state`` and the per-feed
    series untouched for every variable, but precipitation's drill-down
    aggregate line is drawn from the extrema set and is uniformly null when
    the cell is suppressed.

    ``extrema_state`` is the same four-valued verdict as ``state``, by the
    same precedence (:func:`_state_of`), for the feeds the cell's displayed
    daily values come from (``CellSelection.extrema_feeds``). It is
    "not_available" for wind, a suppressed cell and an unavailable cell. The
    tile rolls it up with the cells' states into
    ``DayTile.confidence_state``; it never moves ``state``.
    """

    state: str  # "normal" | "low_confidence" | "rebuilding" | "not_available"
    feeds: list[FeedRef]
    partial: bool
    stale: bool
    extrema_unavailable: bool
    extrema_state: str  # "normal" | "low_confidence" | "rebuilding" | "not_available"

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
    wet_hours: int | None
    show_rain_glyph: bool


@dataclass(frozen=True)
class DayTile:
    day_index: int
    label: str
    date_iso: str
    temp: TempCell
    wind: WindCell
    precip: PrecipCell
    # tile-level: "normal" | "low_confidence" | "rebuilding" | "not_available"
    state: str
    confidence_state: str  # tile.state's rollup over cell and extrema states
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
_RankCache = dict[tuple[str, int], ForecastRanking]


def build_forecast(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    timezone: str,
    rain_threshold_mm: float,
    now: datetime | None = None,
) -> ForecastView:
    """Build the full 8-tile Forecast view model for one site.

    The sample window starts at today's local midnight, so day 0 is the
    "forecast of record": each elapsed hour shows the freshest run that
    covered it, and the daily aggregate spans the full local day. As a
    consequence, a site with only elapsed-today samples renders tiles (with
    stale badges) instead of the empty state until local midnight — intended
    forecast-of-record behavior.
    """
    at = now or utc_now()
    samples = load_future_samples(
        conn,
        site_id=site_id,
        since_valid_at=isoformat_utc(local_day_start(at, timezone)),
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
    depths = effective_blend_depths(conn)
    rank_cache: _RankCache = {}

    tz = ZoneInfo(timezone)
    today = at.astimezone(tz).date()
    tiles: list[DayTile] = []
    for day in range(DAY_COUNT):
        cells: dict[str, tuple[CellMeta, CellSelection, dict[int, list[float]]]] = {}
        for variable in VARIABLES:
            feeds_samples = grouped.get(variable, {}).get(day, {})
            selection, rebuilding_by_feed = _select(
                conn,
                site_id=site_id,
                variable=variable,
                timezone=timezone,
                local_date=today + timedelta(days=day),
                feeds_samples=feeds_samples,
                blend_depth=depths[variable].depth,
                rank_cache=rank_cache,
            )
            meta, values = _cell_meta_and_values(
                selection,
                variable=variable,
                feeds_samples=feeds_samples,
                stale_ids=stale_ids,
                rebuilding_by_feed=rebuilding_by_feed,
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

    Per-variable winner sets are the SAME selections the tile used. For
    temperature and wind the blend at each hour averages the selected feeds
    that cover that hour. Precipitation's blend line averages its contributor
    set (``selections["precip"].extrema_feeds``) with fixed membership and is
    null at any hour a member does not supply; ``precip_aggregate`` names
    those feeds. Per-feed series, drawn from ``feeds`` for every variable,
    ride along for the "show individual feeds" toggle.

    The sample window starts at today's local midnight, so day 0 is the
    "forecast of record": each elapsed hour shows the freshest run that
    covered it. A site with only elapsed-today samples thus yields hourly
    data (with stale badges) instead of an empty payload until local
    midnight — intended forecast-of-record behavior.
    """
    at = now or utc_now()
    samples = load_future_samples(
        conn,
        site_id=site_id,
        since_valid_at=isoformat_utc(local_day_start(at, timezone)),
    )
    grouped = _group_samples(samples, timezone=timezone, now=at)
    depths = effective_blend_depths(conn)
    rank_cache: _RankCache = {}
    tz = ZoneInfo(timezone)
    today = at.astimezone(tz).date()

    selections: dict[str, CellSelection] = {}
    rebuilding: dict[str, dict[int, bool]] = {}
    for variable in VARIABLES:
        feeds_samples = grouped.get(variable, {}).get(day, {})
        selections[variable], rebuilding[variable] = _select(
            conn,
            site_id=site_id,
            variable=variable,
            timezone=timezone,
            local_date=today + timedelta(days=day),
            feeds_samples=feeds_samples,
            blend_depth=depths[variable].depth,
            rank_cache=rank_cache,
        )

    # Hour axis: union of covered hours across every selected feed/variable,
    # plus the hours of precipitation's aggregate contributors
    # (``extrema_feeds``), which can lie outside ``feeds``. Widened for
    # precipitation only: no temperature or wind line is drawn from them.
    hour_set: set[str] = set()
    for variable in VARIABLES:
        feeds_samples = grouped.get(variable, {}).get(day, {})
        axis_candidates = list(selections[variable].feeds)
        if variable == "precip":
            axis_candidates += selections[variable].extrema_feeds
        for candidate in axis_candidates:
            for sample in feeds_samples.get(candidate.feed_id, []):
                hour_set.add(sample.valid_at)
    hours = sorted(hour_set)
    precip_samples = grouped.get("precip", {}).get(day, {})
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

    states = {
        variable: _state_of(
            available=selections[variable].available,
            low_confidence=selections[variable].low_confidence,
            ranking_rebuilding=_any_rebuilding(
                selections[variable].feeds, rebuilding[variable]
            ),
        )
        for variable in VARIABLES
    }
    return {
        "site_id": site_id,
        "day": day,
        "label": _day_label(day, today + timedelta(days=day)),
        "hours": hours,
        "blend": {
            "temp_c": blend_series("temperature"),
            "wind_kmh": blend_series("wind"),
            "precip_mm": fixed_membership_series(
                hours,
                [
                    {s.valid_at: s.value for s in precip_samples.get(c.feed_id, [])}
                    for c in selections["precip"].extrema_feeds
                ],
            ),
        },
        "precip_aggregate": {
            "feed_ids": [c.feed_id for c in selections["precip"].extrema_feeds],
            "coverage": selections["precip"].extrema_coverage,
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
        "states": states,
    }


def _state_of(
    *, available: bool, low_confidence: bool, ranking_rebuilding: bool
) -> str:
    """One feed set's display state from the three facts it is decided on.

    A low-confidence verdict whose ranking is rebuilding reads "rebuilding":
    a rebuilding ranking has no rows, so its feeds are unconfident because of
    the rebuild. Shared by the cell ``state``, the drill-down ``states`` and
    the temperature and precipitation ``extrema_state``, so all three apply
    one precedence.
    """
    if not available:
        return "not_available"
    if low_confidence and ranking_rebuilding:
        return "rebuilding"
    return "low_confidence" if low_confidence else "normal"


def _any_rebuilding(
    feeds: list[CellCandidate], rebuilding_by_feed: dict[int, bool]
) -> bool:
    return any(rebuilding_by_feed[c.feed_id] for c in feeds)


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
    local_date: date,
    feeds_samples: dict[int, list[FutureSampleRow]],
    blend_depth: int,
    rank_cache: _RankCache,
) -> tuple[CellSelection, dict[int, bool]]:
    """Build candidates for one cell and run the fallback ladder.

    ``local_date`` is the cell's target local day; each candidate's
    ``extrema_eligible`` is whether its own samples satisfy the coverage rule
    its variable's displayed daily values need for that day —
    :func:`covers_local_day_exactly` for precipitation and
    :func:`covers_local_day` otherwise. Temperature and precipitation ask for
    the extrema set, leaving wind alone on the clearing-subset path.
    """
    covers = covers_local_day_exactly if variable == "precip" else covers_local_day
    candidates: list[CellCandidate] = []
    rebuilding_by_feed: dict[int, bool] = {}
    for feed_id, feed_samples in feeds_samples.items():
        rep = representative_day_ahead(
            [
                issue_day_ahead(sample.issued_at, sample.valid_at, timezone)
                for sample in feed_samples
            ]
        )
        key = (variable, rep)
        if key not in rank_cache:
            rank_cache[key] = forecast_ranking_with_status(
                conn, site_id=site_id, variable=variable, day_ahead=rep
            )
        rebuilding_by_feed[feed_id] = rank_cache[key].status == "rebuilding"
        row = rank_cache[key].rows.get(feed_id)
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
                covered_hours=covered_hours(s.valid_at for s in feed_samples),
                extrema_eligible=covers(
                    (s.valid_at for s in feed_samples),
                    local_date=local_date,
                    timezone=timezone,
                ),
            )
        )
    selection = select_cell_feeds(
        candidates,
        blend_depth=blend_depth,
        extrema_coverage_required=variable in ("temperature", "precip"),
    )
    return selection, rebuilding_by_feed


def _cell_meta_and_values(
    selection: CellSelection,
    *,
    variable: str,
    feeds_samples: dict[int, list[FutureSampleRow]],
    stale_ids: set[int],
    rebuilding_by_feed: dict[int, bool],
) -> tuple[CellMeta, dict[int, list[float]]]:
    """Apply the coverage rules; return cell meta + per-feed value lists.

    The >= 18-hour guard (:func:`clearing_subset` over the blend set) sets the
    orthogonal "partial" badge, the ``state`` rebuilding scan and ``stale``
    for every variable. For wind alone it also picks the values: feeds
    clearing it aggregate alone, and when NO selected feed clears it the
    partial data still aggregates (the tile stays populated).

    Temperature and precipitation are different: a temperature cell's daily
    high/low and a precipitation cell's total and wet-hour count come from
    the selection's ``extrema_feeds`` — for temperature, feeds covering the
    whole local day; for precipitation, feeds supplying each of its hours
    exactly once — and ``meta.feeds`` names those feeds. When none qualifies
    the values are empty (rendered as unavailable, never as a partial range
    or a partial sum) and ``extrema_unavailable`` is True; the partial data
    is NOT aggregated. Because those feeds can lie outside the clearing
    subset, for both variables ``stale`` also covers every extrema feed, and
    ``extrema_state`` is the extrema set's own verdict under
    :func:`_state_of`'s precedence.
    """
    if not selection.available:
        return (
            CellMeta(
                state="not_available",
                feeds=[],
                partial=False,
                stale=False,
                extrema_unavailable=False,
                extrema_state="not_available",
            ),
            {},
        )
    agg_ids, partial = clearing_subset(
        [candidate.feed_id for candidate in selection.feeds],
        {
            candidate.feed_id: covered_hours(
                s.valid_at for s in feeds_samples[candidate.feed_id]
            )
            for candidate in selection.feeds
        },
    )
    agg_feeds = [c for c in selection.feeds if c.feed_id in set(agg_ids)]
    if variable in ("temperature", "precip"):
        value_feeds = selection.extrema_feeds
        extrema_unavailable = (
            selection.extrema_coverage == EXTREMA_COVERAGE_INSUFFICIENT
        )
        stale_feeds = agg_feeds + selection.extrema_feeds
        extrema_state = _state_of(
            available=selection.extrema_coverage == EXTREMA_COVERAGE_COMPLETE,
            low_confidence=selection.extrema_low_confidence,
            ranking_rebuilding=_any_rebuilding(
                selection.extrema_feeds, rebuilding_by_feed
            ),
        )
    else:
        value_feeds = agg_feeds
        extrema_unavailable = False
        stale_feeds = agg_feeds
        extrema_state = "not_available"
    values = {
        candidate.feed_id: [s.value for s in feeds_samples[candidate.feed_id]]
        for candidate in value_feeds
    }
    meta = CellMeta(
        state=_state_of(
            available=selection.available,
            low_confidence=selection.low_confidence,
            ranking_rebuilding=_any_rebuilding(agg_feeds, rebuilding_by_feed),
        ),
        feeds=[
            FeedRef(
                feed_id=candidate.feed_id,
                label=feed_label(candidate.source, candidate.model),
            )
            for candidate in value_feeds
        ],
        partial=partial,
        stale=any(candidate.feed_id in stale_ids for candidate in stale_feeds),
        extrema_unavailable=extrema_unavailable,
        extrema_state=extrema_state,
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

    temp_daily = displayed_daily(
        "temperature", list(temp_values.values()), rain_threshold_mm=rain_threshold_mm
    )
    temp = TempCell(
        meta=temp_meta,
        high_c=temp_daily["high_c"],
        low_c=temp_daily["low_c"],
    )
    wind_daily = displayed_daily(
        "wind", list(wind_values.values()), rain_threshold_mm=rain_threshold_mm
    )
    wind_max_ms = wind_daily["max_ms"]
    wind = WindCell(
        meta=wind_meta,
        max_kmh=None if wind_max_ms is None else ms_to_kmh(wind_max_ms),
    )
    precip_daily = displayed_daily(
        "precip", list(precip_values.values()), rain_threshold_mm=rain_threshold_mm
    )
    raw_wet_hours = precip_daily["wet_hours"]
    hours_wet = None if raw_wet_hours is None else round(raw_wet_hours)
    precip = PrecipCell(
        meta=precip_meta,
        total_mm=precip_daily["total_mm"],
        wet_hours=hours_wet,
        show_rain_glyph=hours_wet is not None and hours_wet >= RAIN_GLYPH_MIN_WET_HOURS,
    )
    metas = (temp_meta, wind_meta, precip_meta)
    populated = [meta for meta in metas if meta.available]
    # ``state`` rolls up the cells' hourly states and names the CSS class;
    # ``confidence_state`` adds each cell's extrema state and drives the
    # "low confidence" / "ranking updating" badges, by the same precedence.
    state = _roll_up([meta.state for meta in metas])
    confidence_state = _roll_up(
        [meta.state for meta in metas] + [meta.extrema_state for meta in metas]
    )
    return DayTile(
        day_index=day,
        label=label,
        date_iso=date_iso,
        temp=temp,
        wind=wind,
        precip=precip,
        state=state,
        confidence_state=confidence_state,
        stale=any(meta.stale for meta in populated),
        partial=any(meta.partial for meta in populated),
    )


def _roll_up(states: list[str]) -> str:
    """Tile-level state over cell (and extrema) states.

    "not_available" states are ignored; low confidence outranks rebuilding,
    which outranks normal.
    """
    present = [state for state in states if state != "not_available"]
    if not present:
        return "not_available"
    if "low_confidence" in present:
        return "low_confidence"
    if "rebuilding" in present:
        return "rebuilding"
    return "normal"


def _day_label(day: int, local_date: date) -> str:
    if day == 0:
        return "Today"
    if day == 1:
        return "Tomorrow"
    return local_date.strftime("%A")
