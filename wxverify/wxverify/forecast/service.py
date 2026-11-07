"""View-model builder for the Forecast home page.

Glue layer: pulls latest-run future samples (:mod:`wxverify.forecast.data`),
selects winners per (variable, display-day) cell
(:mod:`wxverify.forecast.selection`), aggregates and blends
(:mod:`wxverify.forecast.aggregate`), and shapes the result for templates and
the hourly JSON API. Pure read side — never writes.
"""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final, Literal, cast
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import (
    isoformat_utc,
    local_day_start,
    parse_utc,
    utc_now,
)
from wxverify.core.units import ms_to_kmh
from wxverify.db.site_days import daily_close_on
from wxverify.forecast import wind_blend
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
    wet_hours,
)
from wxverify.forecast.data import (
    FeedFreshness,
    ForecastRanking,
    FutureSampleRow,
    forecast_fingerprint,
    forecast_ranking_with_status,
    load_feed_freshness,
    load_future_samples,
    sample_day_ahead,
)
from wxverify.forecast.rain_pick import forecasts_rain
from wxverify.forecast.selection import (
    CellCandidate,
    CellSelection,
    confident_order_key,
    ladder_order,
    representative_day_ahead,
    select_cell_feeds,
)
from wxverify.scoring.publication import RainRanks, published_rain_ranks
from wxverify.settings.depth import (
    DEPTH_VARIABLES,
    EffectiveDepth,
    effective_blend_depths,
)
from wxverify.web.context import feed_label

DAY_COUNT = 8
#: Page column order = the canonical roster (NB-4).
VARIABLES = DEPTH_VARIABLES
#: The rain icon is "heavy" when the winner's wettest hour is above this,
#: "light" at or below it (rain release, C4).
HEAVY_RAIN_MM_PER_HOUR: Final = 1.0
#: Agreement % is shown only with this many group members with a full day.
MIN_AGREEMENT_MEMBERS: Final = 2

RainIcon = Literal["light", "heavy"]


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

    ``contributor_ids`` are the feeds whose collection freshness the cell
    reports (the clearing subset plus, for temperature and precipitation, the
    extrema feeds), in selection order without repeats. ``stale`` is True if
    any of them is stale; ``fetch_unknown`` is True if any of them has no
    usable fetch on record. A wind cell has no clearing subset, so its
    contributors are all of its chosen feeds.

    ``substitute`` (11.3) is True when a substitute serves: a feed of the
    cell's choice is stale and not in the cell's blend set. The choice is the
    first blend-depth confident rows at the cell's representative day ahead,
    in the ladder's order; for weighted wind, every feed with a published
    weight there. Always False with the daily close off.
    """

    state: str  # "normal" | "low_confidence" | "rebuilding" | "not_available"
    feeds: list[FeedRef]
    partial: bool
    stale: bool
    extrema_unavailable: bool
    extrema_state: str  # "normal" | "low_confidence" | "rebuilding" | "not_available"
    contributor_ids: tuple[int, ...]
    fetch_unknown: bool
    substitute: bool

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
    #: Why the value is "—": too few feeds with a wind track record (§11.6).
    #: None when served.
    note: str | None


@dataclass(frozen=True)
class PrecipCell:
    """The rain cell: the winner's own rain (rain release, C4).

    The winner is the published rain ranking's selected model at the tile's
    day ahead when its newest samples cover the local day exactly, else the
    first ranked model that does. ``total_mm`` is its daily sum (shown to
    one decimal) and ``wet_hours`` its hours at or above the site's rain
    threshold; both are ``None`` with no winner ("No rain forecast"), never
    0.0. ``icon`` is shown at a total of 0.2 mm or more: ``light`` when the
    wettest hour is at most ``HEAVY_RAIN_MM_PER_HOUR``, ``heavy`` above it.
    ``agreement_pct`` is the share of the ranking's group members with a
    full day that forecast rain, shown with at least
    ``MIN_AGREEMENT_MEMBERS`` such members. ``feed_label`` names the winner.
    A provisional ranking sets ``meta.extrema_state`` to ``rebuilding``.
    """

    meta: CellMeta
    total_mm: float | None
    wet_hours: int | None
    icon: RainIcon | None
    agreement_pct: int | None
    feed_label: str | None


@dataclass(frozen=True)
class _RainChoice:
    """One tile's rain winner, chosen from the published ranks (C4)."""

    winner_id: int | None
    selected_id: int | None
    label: str | None
    values: tuple[float, ...]
    provisional: bool
    agreement_pct: int | None
    contributor_ids: tuple[int, ...]


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
    fetch_unknown: bool
    #: True when any populated cell's ``substitute`` is (11.3).
    substitute: bool


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
# One cell: its meta, the selection behind it and the per-feed daily values.
_Cell = tuple[CellMeta, CellSelection, dict[int, list[float]]]


@dataclass(frozen=True)
class PageInputs:
    """Everything the Forecast view and its hourly days read, loaded once.

    :func:`load_page_inputs` reads it; :func:`build_view` and every
    :func:`hourly_day` compute from it, so a page that shows both runs the
    code that serves each. ``samples`` start at today's local midnight and
    are served feeds only; ``grouped`` is them by variable, display day and
    feed. ``stale_ids`` and ``unknown_ids`` come from ``freshness``.
    ``rank_cache`` is shared: the builders fill it as they select.
    """

    site_id: int
    timezone: str
    at: datetime
    today: date
    samples: list[FutureSampleRow]
    fingerprint: str
    grouped: _Grouped
    freshness: dict[int, FeedFreshness]
    stale_ids: set[int]
    unknown_ids: set[int]
    depths: dict[str, EffectiveDepth]
    rank_cache: _RankCache
    close_on: bool
    serving: wind_blend.WindServing
    rain_ranks: dict[int, RainRanks]


@dataclass(frozen=True)
class HourlyDay:
    """One display day's served hourly values (:func:`hourly_day`).

    ``payload`` is the ``/api/forecast/hourly`` JSON without ``"feeds"``,
    which :func:`build_hourly` adds. ``selections`` are the per-variable
    blend sets the tile used, ``wind_weights`` the served wind weights by
    feed and ``rain`` the tile's rain winner. ``ladder`` maps each variable
    to every candidate feed id in the ladder's order (:func:`ladder_order`).
    """

    payload: dict[str, object]
    selections: dict[str, CellSelection]
    wind_weights: dict[int, float]
    rain: _RainChoice
    ladder: dict[str, list[int]]


def load_page_inputs(
    conn: sqlite3.Connection, *, site_id: int, timezone: str, at: datetime
) -> PageInputs:
    """Read the Forecast page's inputs for one site, once, at ``at``.

    The sample window starts at today's local midnight in ``timezone``, with
    served feeds only. The reads run in :func:`build_forecast`'s order:
    samples, fingerprint, freshness, depths, the daily-close switch, wind
    serving, then the published rain ranks.
    """
    samples = load_future_samples(
        conn,
        site_id=site_id,
        since_valid_at=isoformat_utc(local_day_start(at, timezone)),
        served_only=True,
    )
    fingerprint = forecast_fingerprint(conn, site_id=site_id)
    grouped = _group_samples(samples, timezone=timezone, now=at)
    freshness = load_feed_freshness(conn, site_id=site_id, now=at)
    stale_ids = {feed_id for feed_id, row in freshness.items() if row.stale}
    unknown_ids = {
        feed_id for feed_id, row in freshness.items() if row.fetch_state == "unknown"
    }
    depths = effective_blend_depths(conn)
    # One switch per read (9.1): the substitute flag is computed only with
    # the close on, and stays False otherwise (11.3).
    close_on = daily_close_on(conn)
    today = at.astimezone(ZoneInfo(timezone)).date()
    serving = wind_blend.load_wind_serving(
        conn, site_id=site_id, timezone=timezone, today=today
    )
    # Rain follows the published rain ranking (rain release, C4), read once.
    rain_ranks = published_rain_ranks(conn, site_id)
    return PageInputs(
        site_id=site_id,
        timezone=timezone,
        at=at,
        today=today,
        samples=samples,
        fingerprint=fingerprint,
        grouped=grouped,
        freshness=freshness,
        stale_ids=stale_ids,
        unknown_ids=unknown_ids,
        depths=depths,
        rank_cache={},
        close_on=close_on,
        serving=serving,
        rain_ranks=rain_ranks,
    )


def build_forecast(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    timezone: str,
    rain_threshold_mm: float,
    now: datetime | None = None,
) -> ForecastView:
    """Build the full 8-tile Forecast view model for one site.

    The sample window starts at today's local midnight, so day 0 covers the
    whole local day: each elapsed hour shows the freshest run that covered
    it, and the daily aggregate spans the full local day. As a consequence,
    a site with only elapsed-today samples renders tiles (with stale badges)
    instead of the empty state until local midnight — intended behavior.

    Rain does not go through the selection ladder: each tile shows the rain
    of its day ahead's rain winner from the published rain ranking
    (rain release, C4; :class:`PrecipCell`).

    It is :func:`build_view` over :func:`load_page_inputs`.
    """
    at = now or utc_now()
    return build_view(
        conn,
        load_page_inputs(conn, site_id=site_id, timezone=timezone, at=at),
        rain_threshold_mm=rain_threshold_mm,
    )


def build_view(
    conn: sqlite3.Connection, inputs: PageInputs, *, rain_threshold_mm: float
) -> ForecastView:
    """The 8-tile Forecast view model from one load of the page's inputs.

    :func:`build_forecast`'s rules: the empty view with no sample, else one
    tile per display day. Rankings are read through ``inputs.rank_cache``.
    """
    if not inputs.samples:
        return ForecastView(
            empty=True,
            tiles=[],
            updated_at=None,
            updated_ago=None,
            fingerprint=inputs.fingerprint,
        )
    site_id = inputs.site_id
    timezone = inputs.timezone
    at = inputs.at
    today = inputs.today
    grouped = inputs.grouped
    freshness = inputs.freshness
    stale_ids = inputs.stale_ids
    unknown_ids = inputs.unknown_ids
    depths = inputs.depths
    rank_cache = inputs.rank_cache
    close_on = inputs.close_on
    serving = inputs.serving
    rain_ranks = inputs.rain_ranks
    fingerprint = inputs.fingerprint
    # Per day: the cells, the rain choice, then the wind weights and note
    # for _build_tile.
    day_cells: list[
        tuple[dict[str, _Cell], _RainChoice, dict[int, float], str | None]
    ] = []
    for day in range(DAY_COUNT):
        cells: dict[str, _Cell] = {}
        wind_weights: dict[int, float] = {}
        wind_note: str | None = None
        rain = _rain_choice(
            rain_ranks.get(day),
            grouped.get("precip", {}).get(day, {}),
            local_date=today + timedelta(days=day),
            timezone=timezone,
        )
        for variable in VARIABLES:
            if variable == "precip":
                continue
            feeds_samples = grouped.get(variable, {}).get(day, {})
            cell_k = _cell_day_ahead(feeds_samples, timezone) if close_on else None
            if variable == "wind":
                choice = _select_wind(
                    conn,
                    site_id=site_id,
                    timezone=timezone,
                    local_date=today + timedelta(days=day),
                    feeds_samples=feeds_samples,
                    rank_cache=rank_cache,
                    weights=serving.weights,
                )
                meta, values = _wind_meta_and_values(
                    choice.selection,
                    feeds_samples=feeds_samples,
                    stale_ids=stale_ids,
                    unknown_ids=unknown_ids,
                    substitute=cell_k is not None
                    and _substitute_serves(
                        [feed for feed, k in serving.weights if k == cell_k],
                        choice.selection,
                        stale_ids,
                    ),
                )
                cells[variable] = (meta, choice.selection, values)
                wind_weights, wind_note = choice.weights, choice.note
                continue
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
                feeds_samples=feeds_samples,
                stale_ids=stale_ids,
                unknown_ids=unknown_ids,
                rebuilding_by_feed=rebuilding_by_feed,
                substitute=cell_k is not None
                and _substitute_serves(
                    _ranked_choice(
                        conn,
                        site_id=site_id,
                        variable=variable,
                        day_ahead=cell_k,
                        blend_depth=depths[variable].depth,
                        rank_cache=rank_cache,
                    ),
                    selection,
                    stale_ids,
                ),
            )
            cells[variable] = (meta, selection, values)
        day_cells.append((cells, rain, wind_weights, wind_note))
    tiles = [
        _build_tile(
            day,
            date_iso=(today + timedelta(days=day)).isoformat(),
            label=_day_label(day, today + timedelta(days=day)),
            cells=cells,
            precip=_precip_cell(
                rain,
                rain_threshold_mm=rain_threshold_mm,
                stale_ids=stale_ids,
                unknown_ids=unknown_ids,
            ),
            rain_threshold_mm=rain_threshold_mm,
            wind_weights=wind_weights,
            wind_note=wind_note,
        )
        for day, (cells, rain, wind_weights, wind_note) in enumerate(day_cells)
    ]
    updated_at = _last_fetched_at(tiles, freshness)
    return ForecastView(
        empty=False,
        tiles=tiles,
        updated_at=updated_at,
        updated_ago=None if updated_at is None else relative_ago(updated_at, now=at),
        fingerprint=fingerprint,
    )


def _last_fetched_at(
    tiles: list[DayTile], freshness: dict[int, FeedFreshness]
) -> str | None:
    """The "Last fetched" instant: the newest usable-fetch stamp across the
    forecast tiles.

    Takes the union of ``contributor_ids`` over every populated cell of every
    tile and maps each feed to its evidence feed's ``last_usable_fetch_at``
    through ``freshness``. Stamps that do not parse are skipped. Stamps are
    compared as parsed UTC instants, never as strings, so stamps with
    different UTC offsets are ordered chronologically, and the newest instant
    is chosen before it is cut to the whole second. It is returned as a
    whole-second ``isoformat_utc`` stamp, which the browser's ``Date.parse``
    reads as-is; ``None`` when no stamp parses.
    """
    contributor_ids = {
        feed_id
        for tile in tiles
        for meta in (tile.temp.meta, tile.wind.meta, tile.precip.meta)
        if meta.available
        for feed_id in meta.contributor_ids
    }
    newest: datetime | None = None
    for feed_id in contributor_ids:
        row = freshness.get(feed_id)
        stamp = None if row is None else row.last_usable_fetch_at
        if stamp is None:
            continue
        try:
            fetched = parse_utc(stamp)
        except ValueError:
            continue
        if newest is None or fetched > newest:
            newest = fetched
    if newest is None:
        return None
    return isoformat_utc(newest.replace(microsecond=0))


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
    that cover that hour. Precipitation's blend line is the tile's rain
    winner alone (rain release, C4: :func:`_rain_choice` over the published
    rain ranks), all null with no winner; ``precip_aggregate`` names it and
    ``states["precip"]`` is the tile's (``normal``, or ``not_available``
    with no winner).
    Per-feed series, drawn from ``feeds`` for every variable, ride along in
    the ``/api/forecast/hourly`` payload for API clients that want each
    feed beside the blend; the Forecast page does not read them.

    The sample window starts at today's local midnight, so day 0 covers the
    whole local day: each elapsed hour shows the freshest run that covered
    it. A site with only elapsed-today samples thus yields hourly data (with
    stale badges) instead of an empty payload until local midnight —
    intended behavior.

    The wind line is the per-hour weighted mean of the eligible feeds (which
    join the hour axis and ``feeds``), ``states["wind"]`` ignores the ranking
    rebuild, and the payload carries ``weights`` and ``notes``.

    It is :func:`hourly_day` over :func:`load_page_inputs`, plus ``feeds``:
    every feed of any variable's selection, in first-selected order, with
    its series for each variable on the hour axis.
    """
    at = now or utc_now()
    inputs = load_page_inputs(conn, site_id=site_id, timezone=timezone, at=at)
    view = hourly_day(conn, inputs, day)
    # hourly_day sets "hours" to the sorted hour axis, a list of stamps.
    hours = cast(list[str], view.payload["hours"])
    index = {valid_at: i for i, valid_at in enumerate(hours)}

    feed_order: list[tuple[int, str]] = []
    seen: set[int] = set()
    for variable in VARIABLES:
        for candidate in view.selections[variable].feeds:
            if candidate.feed_id not in seen:
                seen.add(candidate.feed_id)
                feed_order.append(
                    (candidate.feed_id, feed_label(candidate.source, candidate.model))
                )
    feeds = [
        {
            "feed_id": feed_id,
            "label": label,
            "temp_c": _feed_series(
                inputs.grouped,
                variable="temperature",
                day=day,
                feed_id=feed_id,
                index=index,
            ),
            "wind_kmh": _feed_series(
                inputs.grouped, variable="wind", day=day, feed_id=feed_id, index=index
            ),
            "precip_mm": _feed_series(
                inputs.grouped,
                variable="precip",
                day=day,
                feed_id=feed_id,
                index=index,
            ),
        }
        for feed_id, label in feed_order
    ]
    # "feeds" goes back between "precip_aggregate" and "states", so the JSON
    # keeps its key order.
    payload: dict[str, object] = {}
    for key, value in view.payload.items():
        payload[key] = value
        if key == "precip_aggregate":
            payload["feeds"] = feeds
    return payload


def hourly_day(conn: sqlite3.Connection, inputs: PageInputs, day: int) -> HourlyDay:
    """One display day's served hourly values, from the page's one load.

    :func:`build_hourly`'s rules without its per-feed ``feeds`` list: the
    same selections, rain winner, hour axis, blend lines, ``states``,
    ``weights`` and ``notes``. ``ladder`` orders each variable's candidates
    for the day by :func:`ladder_order`, over the rankings the selections
    cached in ``inputs.rank_cache``.
    """
    site_id = inputs.site_id
    timezone = inputs.timezone
    today = inputs.today
    grouped = inputs.grouped
    depths = inputs.depths
    rank_cache = inputs.rank_cache
    serving = inputs.serving
    wind_note: str | None = None
    wind_weights: dict[int, float] = {}

    selections: dict[str, CellSelection] = {}
    rebuilding: dict[str, dict[int, bool]] = {}
    for variable in VARIABLES:
        feeds_samples = grouped.get(variable, {}).get(day, {})
        if variable == "wind":
            choice = _select_wind(
                conn,
                site_id=site_id,
                timezone=timezone,
                local_date=today + timedelta(days=day),
                feeds_samples=feeds_samples,
                rank_cache=rank_cache,
                weights=serving.weights,
            )
            selections[variable], rebuilding[variable] = choice.selection, {}
            wind_weights, wind_note = choice.weights, choice.note
            continue
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

    # The ranked grid's order (card 7): every candidate of each variable,
    # over the rankings the selections above cached.
    ladder = {
        variable: ladder_order(
            _candidates(
                conn,
                site_id=site_id,
                variable=variable,
                timezone=timezone,
                local_date=today + timedelta(days=day),
                feeds_samples=grouped.get(variable, {}).get(day, {}),
                rank_cache=rank_cache,
            )[0]
        )
        for variable in VARIABLES
    }

    # The hourly rain line is the tile's rain winner only (rain release, C4).
    precip_samples = grouped.get("precip", {}).get(day, {})
    rain = _rain_choice(
        inputs.rain_ranks.get(day),
        precip_samples,
        local_date=today + timedelta(days=day),
        timezone=timezone,
    )
    rain_feeds = [] if rain.winner_id is None else [rain.winner_id]

    # Hour axis: union of covered hours across every selected feed/variable,
    # plus the rain winner's hours, which can lie outside ``feeds``. Widened
    # for precipitation only: no temperature or wind line is drawn from them.
    hour_set: set[str] = set()
    for variable in VARIABLES:
        feeds_samples = grouped.get(variable, {}).get(day, {})
        axis_ids = [candidate.feed_id for candidate in selections[variable].feeds]
        if variable == "precip":
            axis_ids += rain_feeds
        for feed_id in axis_ids:
            for sample in feeds_samples.get(feed_id, []):
                hour_set.add(sample.valid_at)
    hours = sorted(hour_set)
    index = {valid_at: i for i, valid_at in enumerate(hours)}

    def blend_series(variable: str) -> list[float | None]:
        per_feed = [
            _feed_series(
                grouped,
                variable=variable,
                day=day,
                feed_id=candidate.feed_id,
                index=index,
            )
            for candidate in selections[variable].feeds
        ]
        out: list[float | None] = []
        for i in range(len(hours)):
            values = [s[i] for s in per_feed if s[i] is not None]
            out.append(blend_mean([v for v in values if v is not None]))
        return out

    def wind_line() -> list[float | None]:
        wind_samples = grouped.get("wind", {}).get(day, {})
        weighted = wind_blend.weighted_hourly(
            {
                c.feed_id: {
                    s.valid_at: ms_to_kmh(s.value)
                    for s in wind_samples.get(c.feed_id, [])
                }
                for c in selections["wind"].feeds
            },
            wind_weights,
        )
        return [weighted.get(hour) for hour in hours]

    states = {
        variable: _state_of(
            available=selections[variable].available,
            low_confidence=selections[variable].low_confidence,
            # The leaderboard ranking does not drive weighted wind (§11.4).
            ranking_rebuilding=(
                False
                if variable == "wind"
                else _any_rebuilding(selections[variable].feeds, rebuilding[variable])
            ),
        )
        for variable in VARIABLES
    }
    # The rain state is the tile's (C4): normal with a winner, else none.
    states["precip"] = "normal" if rain_feeds else "not_available"
    payload: dict[str, object] = {
        "site_id": site_id,
        "day": day,
        "label": _day_label(day, today + timedelta(days=day)),
        "hours": hours,
        "blend": {
            "temp_c": blend_series("temperature"),
            "wind_kmh": wind_line(),
            "precip_mm": fixed_membership_series(
                hours,
                [
                    {s.valid_at: s.value for s in precip_samples.get(feed_id, [])}
                    for feed_id in rain_feeds
                ],
            ),
        },
        "precip_aggregate": {
            "feed_ids": rain_feeds,
            "coverage": (
                EXTREMA_COVERAGE_COMPLETE
                if rain_feeds
                else EXTREMA_COVERAGE_INSUFFICIENT
            ),
        },
        "states": states,
    }
    payload["weights"] = {
        "wind": {str(feed_id): weight for feed_id, weight in wind_weights.items()}
    }
    payload["notes"] = {"wind": wind_note}
    return HourlyDay(
        payload=payload,
        selections=selections,
        wind_weights=wind_weights,
        rain=rain,
        ladder=ladder,
    )


def _feed_series(
    grouped: _Grouped,
    *,
    variable: str,
    day: int,
    feed_id: int,
    index: Mapping[str, int],
) -> list[float | None]:
    """One feed's hourly values for a display day on an hour axis.

    ``index`` maps each axis ``valid_at`` to its column. Wind is in km/h;
    a column the feed has no sample for is null.
    """
    feeds_samples = grouped.get(variable, {}).get(day, {})
    out: list[float | None] = [None] * len(index)
    for sample in feeds_samples.get(feed_id, []):
        value = sample.value
        if variable == "wind":
            value = ms_to_kmh(value)
        out[index[sample.valid_at]] = value
    return out


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
    """Human "X ago" text for the "Last fetched" label, from a UTC ISO stamp."""
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
    the extrema set; wind never comes through here (see :func:`_select_wind`).
    """
    candidates, rebuilding_by_feed, _ = _candidates(
        conn,
        site_id=site_id,
        variable=variable,
        timezone=timezone,
        local_date=local_date,
        feeds_samples=feeds_samples,
        rank_cache=rank_cache,
    )
    selection = select_cell_feeds(
        candidates,
        blend_depth=blend_depth,
        extrema_coverage_required=variable in ("temperature", "precip"),
    )
    return selection, rebuilding_by_feed


def _candidates(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    timezone: str,
    local_date: date,
    feeds_samples: dict[int, list[FutureSampleRow]],
    rank_cache: _RankCache,
) -> tuple[list[CellCandidate], dict[int, bool], dict[int, int]]:
    """One cell's candidates, their ranking-rebuild flags and representative k.

    Returns ``(candidates, rebuilding_by_feed, rep_k)``; ``rep_k`` maps each
    feed to the representative day-ahead of its samples, the lead its ranking
    row (and, for weighted wind, its weight) is looked up at. Each sample's
    day ahead is :func:`sample_day_ahead`'s: from its download date with the
    daily close on, else from its run label (9.5).
    """
    covers = covers_local_day_exactly if variable == "precip" else covers_local_day
    candidates: list[CellCandidate] = []
    rebuilding_by_feed: dict[int, bool] = {}
    rep_k: dict[int, int] = {}
    for feed_id, feed_samples in feeds_samples.items():
        rep = representative_day_ahead(
            [sample_day_ahead(sample, timezone) for sample in feed_samples]
        )
        rep_k[feed_id] = rep
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
    return candidates, rebuilding_by_feed, rep_k


def _select_wind(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    timezone: str,
    local_date: date,
    feeds_samples: dict[int, list[FutureSampleRow]],
    rank_cache: _RankCache,
    weights: wind_blend.WindWeights,
) -> wind_blend.WindChoice:
    """The weighted wind choice for one cell."""
    candidates, _, rep_k = _candidates(
        conn,
        site_id=site_id,
        variable="wind",
        timezone=timezone,
        local_date=local_date,
        feeds_samples=feeds_samples,
        rank_cache=rank_cache,
    )
    return wind_blend.choose_wind_feeds(candidates, rep_k, weights)


def _cell_day_ahead(
    feeds_samples: dict[int, list[FutureSampleRow]], timezone: str
) -> int | None:
    """The cell's representative day ahead over every sample in it, or None
    for a cell with no sample (11.3)."""
    leads = [
        sample_day_ahead(sample, timezone)
        for samples in feeds_samples.values()
        for sample in samples
    ]
    return representative_day_ahead(leads) if leads else None


def _ranked_choice(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    blend_depth: int,
    rank_cache: _RankCache,
) -> list[int]:
    """The cell's choice (11.3): the first ``blend_depth`` confident rows of
    the ranking at ``day_ahead``, in the ladder's order."""
    key = (variable, day_ahead)
    if key not in rank_cache:
        rank_cache[key] = forecast_ranking_with_status(
            conn, site_id=site_id, variable=variable, day_ahead=day_ahead
        )
    confident = sorted(
        (row for row in rank_cache[key].rows.values() if row.confident),
        key=lambda row: confident_order_key(row.skill_score, row.source, row.model),
    )
    return [row.feed_id for row in confident[:blend_depth]]


def _substitute_serves(
    chosen: list[int], selection: CellSelection, stale_ids: set[int]
) -> bool:
    """Whether a feed of the cell's choice is stale and out of its blend set."""
    served = {candidate.feed_id for candidate in selection.feeds}
    return any(feed_id in stale_ids and feed_id not in served for feed_id in chosen)


def _wind_meta_and_values(
    selection: CellSelection,
    *,
    feeds_samples: dict[int, list[FutureSampleRow]],
    stale_ids: set[int],
    unknown_ids: set[int],
    substitute: bool,
) -> tuple[CellMeta, dict[int, list[float]]]:
    """Weighted wind cell: every chosen feed contributes, none is dropped.

    Wind does not use the clearing-subset path: the values come from all of
    ``selection.feeds`` (each already has at least ``MIN_RUN_HOURS`` hours),
    so ``partial`` is never set, and the ranking rebuild does not move the
    state because the leaderboard does not choose these feeds. ``substitute``
    is the caller's (11.3); an unavailable cell has none serving, so False.
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
                contributor_ids=(),
                fetch_unknown=False,
                substitute=False,
            ),
            {},
        )
    values = {
        candidate.feed_id: [s.value for s in feeds_samples[candidate.feed_id]]
        for candidate in selection.feeds
    }
    contributor_ids = tuple(
        dict.fromkeys(candidate.feed_id for candidate in selection.feeds)
    )
    meta = CellMeta(
        state=_state_of(
            available=selection.available,
            low_confidence=False,
            ranking_rebuilding=False,
        ),
        feeds=[
            FeedRef(
                feed_id=candidate.feed_id,
                label=feed_label(candidate.source, candidate.model),
            )
            for candidate in selection.feeds
        ],
        partial=False,
        stale=any(candidate.feed_id in stale_ids for candidate in selection.feeds),
        extrema_unavailable=False,
        extrema_state="not_available",
        contributor_ids=contributor_ids,
        fetch_unknown=any(feed_id in unknown_ids for feed_id in contributor_ids),
        substitute=substitute,
    )
    return meta, values


def _cell_meta_and_values(
    selection: CellSelection,
    *,
    feeds_samples: dict[int, list[FutureSampleRow]],
    stale_ids: set[int],
    unknown_ids: set[int],
    rebuilding_by_feed: dict[int, bool],
    substitute: bool,
) -> tuple[CellMeta, dict[int, list[float]]]:
    """Apply the coverage rules; return cell meta + per-feed value lists.

    The >= 18-hour guard (:func:`clearing_subset` over the blend set) sets the
    orthogonal "partial" badge, the ``state`` rebuilding scan and ``stale``.
    Wind never comes through here (see :func:`_wind_meta_and_values`).

    A temperature cell's daily high/low and a precipitation cell's total and
    wet-hour count come from the selection's ``extrema_feeds`` — for
    temperature, feeds covering the whole local day; for precipitation, feeds
    supplying each of its hours exactly once — and ``meta.feeds`` names those
    feeds. When none qualifies the values are empty (rendered as unavailable,
    never as a partial range or a partial sum) and ``extrema_unavailable`` is
    True; the partial data is NOT aggregated. Because those feeds can lie
    outside the clearing subset, for both variables ``stale`` also covers
    every extrema feed, and ``extrema_state`` is the extrema set's own verdict
    under :func:`_state_of`'s precedence. ``fetch_unknown`` covers the same
    contributor feeds as ``stale``. ``substitute`` is the caller's (11.3);
    an unavailable cell has none serving, so False.
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
                contributor_ids=(),
                fetch_unknown=False,
                substitute=False,
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
    value_feeds = selection.extrema_feeds
    extrema_unavailable = selection.extrema_coverage == EXTREMA_COVERAGE_INSUFFICIENT
    stale_feeds = agg_feeds + selection.extrema_feeds
    extrema_state = _state_of(
        available=selection.extrema_coverage == EXTREMA_COVERAGE_COMPLETE,
        low_confidence=selection.extrema_low_confidence,
        ranking_rebuilding=_any_rebuilding(selection.extrema_feeds, rebuilding_by_feed),
    )
    values = {
        candidate.feed_id: [s.value for s in feeds_samples[candidate.feed_id]]
        for candidate in value_feeds
    }
    contributor_ids = tuple(
        dict.fromkeys(candidate.feed_id for candidate in stale_feeds)
    )
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
        contributor_ids=contributor_ids,
        fetch_unknown=any(feed_id in unknown_ids for feed_id in contributor_ids),
        substitute=substitute,
    )
    return meta, values


def _rain_choice(
    ranks: RainRanks | None,
    feeds_samples: Mapping[int, list[FutureSampleRow]],
    *,
    local_date: date,
    timezone: str,
) -> _RainChoice:
    """One tile's rain winner from its day ahead's published ranks (C4).

    A ranked model has a full day when its newest samples cover the local
    day exactly (``covers_local_day_exactly``). The winner is the selected
    model when it has one, else the first ranked model that does; with no
    ranks, or no ranked model with a full day, there is none. Agreement %:
    round(100·k/q), half up, over the group members with a full day, k of
    them forecasting rain (0.2 mm or more); only with at least
    ``MIN_AGREEMENT_MEMBERS`` such members.
    """
    if ranks is None or not ranks.ranked:
        return _RainChoice(
            winner_id=None,
            selected_id=None,
            label=None,
            values=(),
            provisional=False,
            agreement_pct=None,
            contributor_ids=(),
        )
    full: dict[int, list[FutureSampleRow]] = {}
    for ranked in ranks.ranked:
        samples = feeds_samples.get(ranked.feed_id, [])
        if samples and covers_local_day_exactly(
            (s.valid_at for s in samples), local_date=local_date, timezone=timezone
        ):
            full[ranked.feed_id] = sorted(samples, key=lambda s: s.valid_at)
    selected_id = next((r.feed_id for r in ranks.ranked if r.selected), None)
    winner_id = (
        selected_id
        if selected_id in full
        else next((r.feed_id for r in ranks.ranked if r.feed_id in full), None)
    )
    members = [r.feed_id for r in ranks.ranked if r.in_group and r.feed_id in full]
    agreement_pct: int | None = None
    if len(members) >= MIN_AGREEMENT_MEMBERS:
        wet = sum(
            1
            for feed_id in members
            if forecasts_rain(math.fsum(s.value for s in full[feed_id]))
        )
        agreement_pct = (200 * wet + len(members)) // (2 * len(members))
    winner = None if winner_id is None else full[winner_id]
    contributors = ([] if winner_id is None else [winner_id]) + (
        members if agreement_pct is not None else []
    )
    return _RainChoice(
        winner_id=winner_id,
        selected_id=selected_id,
        label=None if winner is None else feed_label(winner[0].source, winner[0].model),
        values=() if winner is None else tuple(s.value for s in winner),
        provisional=ranks.provisional,
        agreement_pct=agreement_pct,
        contributor_ids=tuple(dict.fromkeys(contributors)),
    )


def _precip_cell(
    rain: _RainChoice,
    *,
    rain_threshold_mm: float,
    stale_ids: set[int],
    unknown_ids: set[int],
) -> PrecipCell:
    """The rain cell of :func:`_rain_choice`'s winner (C4).

    With no winner the cell is ``not_available`` and every value ``None``.
    With one, ``state`` is ``normal`` and ``extrema_state`` is
    ``rebuilding`` for a provisional ranking, else ``normal`` (the plan's
    C4: the provisional ranking shows through ``extrema_state``, as a
    rebuilding extrema set did); ``feeds`` is the winner; ``stale`` and
    ``fetch_unknown`` cover the winner and the agreement members.
    ``substitute`` (11.3) is set when the selected model is stale and
    another ranked model stands in for it.
    """
    if rain.winner_id is None:
        return PrecipCell(
            meta=CellMeta(
                state="not_available",
                feeds=[],
                partial=False,
                stale=False,
                extrema_unavailable=False,
                extrema_state="not_available",
                contributor_ids=(),
                fetch_unknown=False,
                substitute=False,
            ),
            total_mm=None,
            wet_hours=None,
            icon=None,
            agreement_pct=None,
            feed_label=None,
        )
    total = math.fsum(rain.values)
    icon: RainIcon | None = None
    if forecasts_rain(total):
        icon = "heavy" if max(rain.values) > HEAVY_RAIN_MM_PER_HOUR else "light"
    meta = CellMeta(
        state="normal",
        feeds=[FeedRef(feed_id=rain.winner_id, label=rain.label or "")],
        partial=False,
        stale=any(feed_id in stale_ids for feed_id in rain.contributor_ids),
        extrema_unavailable=False,
        extrema_state="rebuilding" if rain.provisional else "normal",
        contributor_ids=rain.contributor_ids,
        fetch_unknown=any(feed_id in unknown_ids for feed_id in rain.contributor_ids),
        substitute=rain.selected_id is not None
        and rain.selected_id != rain.winner_id
        and rain.selected_id in stale_ids,
    )
    return PrecipCell(
        meta=meta,
        total_mm=total,
        wet_hours=wet_hours(list(rain.values), threshold_mm=rain_threshold_mm),
        icon=icon,
        agreement_pct=rain.agreement_pct,
        feed_label=rain.label,
    )


def _build_tile(
    day: int,
    *,
    date_iso: str,
    label: str,
    cells: dict[str, _Cell],
    precip: PrecipCell,
    rain_threshold_mm: float,
    wind_weights: Mapping[int, float],
    wind_note: str | None,
) -> DayTile:
    """One day's tile. ``wind_weights`` gives the weighted daily wind high;
    it is empty when wind is closed, so the high is None. ``precip`` is the
    rain cell of the tile's rain winner (:func:`_precip_cell`)."""
    temp_meta, _, temp_values = cells["temperature"]
    wind_meta, _, wind_values = cells["wind"]
    precip_meta = precip.meta

    temp_daily = displayed_daily(
        "temperature", list(temp_values.values()), rain_threshold_mm=rain_threshold_mm
    )
    temp = TempCell(
        meta=temp_meta,
        high_c=temp_daily["high_c"],
        low_c=temp_daily["low_c"],
    )
    wind_max_ms = wind_blend.weighted_daily_high(wind_values, wind_weights)
    wind = WindCell(
        meta=wind_meta,
        max_kmh=None if wind_max_ms is None else ms_to_kmh(wind_max_ms),
        note=wind_note,
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
        fetch_unknown=any(meta.fetch_unknown for meta in populated),
        substitute=any(meta.substitute for meta in populated),
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
