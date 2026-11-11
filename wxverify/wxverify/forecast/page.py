"""The Forecast page's data model, built from served values only.

:func:`build_forecast_page` loads the page's inputs once
(:func:`~wxverify.forecast.service.load_page_inputs`), builds the tile view
(:func:`~wxverify.forecast.service.build_view`) and every day's served
hourly values (:func:`~wxverify.forecast.service.hourly_day`) with the code
that serves them, and reads the display samples: cloud cover, gust and wind
direction (:func:`~wxverify.forecast.data.load_display_samples`). It then
shapes 8 day headers, a timeline of four 6-hour blocks per day and, for the
selected day, the hour columns and the three grids.

Nothing served is recomputed: the BLEND rows and the rain top row read the
hourly payload by exact ``valid_at`` key, so they equal ``build_hourly`` by
construction. Values stay raw (wind in km/h; ``None`` means nothing to
show) and the template formats them, except for the texts this module
names: the day warnings and the rain summary. Pure read side, never writes.
"""

from __future__ import annotations

import math
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Final, Literal, cast
from zoneinfo import ZoneInfo

from wxverify.core.timeutil import isoformat_utc, local_day_start, utc_now
from wxverify.core.units import ms_to_kmh
from wxverify.forecast import wind_blend
from wxverify.forecast.aggregate import (
    clears_coverage,
    covered_hours,
    covers_local_day,
    covers_local_day_exactly,
    display_day_index,
    local_day_hour_instants,
)
from wxverify.forecast.data import FutureSampleRow, load_display_samples
from wxverify.forecast.rain_pick import forecasts_rain
from wxverify.forecast.selection import CellCandidate
from wxverify.forecast.service import (
    DAY_COUNT,
    HEAVY_RAIN_MM_PER_HOUR,
    DayTile,
    FeedRef,
    HourlyDay,
    PageInputs,
    PrecipCell,
    RainIcon,
    build_view,
    hourly_day,
    load_page_inputs,
)
from wxverify.web.context import model_label

BlockIcon = Literal["heavy", "rain", "cloud", "partly", "sun"]

#: Local hours per timeline block: the blocks start at local 00, 06, 12, 18.
BLOCK_HOURS: Final = 6
BLOCKS_PER_DAY: Final = 4
#: The sky icon on a block's mean cloud cover, in %: above ``CLOUDY_PCT``
#: "cloud", above ``PARTLY_PCT`` "partly", otherwise "sun".
CLOUDY_PCT: Final = 75.0
PARTLY_PCT: Final = 35.0
#: The label of the temperature and wind grids' top row.
BLEND_LABEL: Final = "BLEND"
#: The rain top row's summary with no served rain model, as the tile reads.
NO_RAIN_TEXT: Final = "No rain forecast"

#: The six day warnings, in :func:`day_warnings`' order.
LOW_CONFIDENCE_TEXT: Final = "Low confidence"
RANKING_UPDATING_TEXT: Final = "Ranking updating"
STALE_TEXT: Final = "Stale: a model has not updated on time"
SUBSTITUTE_TEXT: Final = "Substitute: a model stands in for one that stopped updating"
FETCH_UNKNOWN_TEXT: Final = "Fetch time unknown: a model has no recorded download yet"
PARTIAL_TEXT: Final = "Partial: the day is not fully covered"

#: The tile's rain symbols: light rain (U+1F327) and heavy rain (U+2614).
_RAIN_SYMBOLS: Final[dict[RainIcon, str]] = {"light": "\U0001f327", "heavy": "☔"}
#: The tile's separator between rain summary parts, a middle dot.
_SEP: Final = " · "


@dataclass(frozen=True)
class HourColumn:
    """One hour column of the selected day's grids.

    ``instant`` is an on-the-hour UTC instant of the local day
    (:func:`local_day_hour_instants`) and ``key`` its stored ``valid_at``
    form (:func:`isoformat_utc`), the key every value is read by. ``label``
    is its local hour as ``HH`` and ``block`` its timeline block, ``local
    hour // BLOCK_HOURS``. On a fall-back day the repeated hour has two
    columns with the same ``label`` and their own instants.
    """

    instant: datetime
    key: str
    label: str
    block: int


@dataclass(frozen=True)
class TimelineCell:
    """One 6-hour block of one day on the timeline.

    The block's representative is its first hour column: ``label`` is that
    column's local ``HH`` and ``key`` its ``valid_at`` (``None``, with the
    nominal label, for a block with no hour). ``temp_c`` and ``wind_kmh`` are
    the BLEND values at the representative, ``rain_mm`` the ``math.fsum`` of
    the served rain model's values in the block, ``gust_kmh`` the block's
    highest served gust, ``direction_deg`` the direction model's value at the
    representative and ``icon`` the block's :func:`block_icon`. ``None``
    means nothing to show.
    """

    day_index: int
    block: int
    label: str
    key: str | None
    temp_c: float | None
    wind_kmh: float | None
    rain_mm: float | None
    gust_kmh: float | None
    direction_deg: float | None
    icon: BlockIcon | None


@dataclass(frozen=True)
class DayHeader:
    """One day's timeline header, from its tile.

    ``label`` is the tile's ("Today", "Tomorrow" or a weekday) and
    ``short_label`` the weekday's abbreviation. ``date_iso`` is the local
    date and ``date_dm`` the same date as DD-MM. ``high_c`` and ``low_c`` are
    the tile's (``None`` when it has none); ``extrema_unavailable`` is the
    tile's temperature flag ("—" in place of the pair) and ``not_available``
    whether the tile's state is ``not_available`` (a muted header).
    ``warnings`` are :func:`day_warnings`.
    """

    day_index: int
    label: str
    short_label: str
    date_iso: str
    date_dm: str
    high_c: float | None
    low_c: float | None
    extrema_unavailable: bool
    not_available: bool
    selected: bool
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class GridRow:
    """One grid row: a value per hour column and the last column.

    ``feed_id`` is ``None`` on the BLEND rows and on the rain top row, which
    the feed picker never hides. ``values`` follow the day's hour columns
    (wind in km/h). The last column: ``day_high`` is temperature's Max and
    wind's Max, ``day_low`` temperature's Min and ``day_total``
    precipitation's Total; ``None`` shows "—". ``day_text`` is the rain top
    row's summary (:func:`rain_text`), ``None`` on every other row.
    """

    feed_id: int | None
    label: str
    values: tuple[float | None, ...]
    day_high: float | None
    day_low: float | None
    day_total: float | None
    day_text: str | None


@dataclass(frozen=True)
class Grid:
    """One weather type's grid for the selected day.

    ``top`` is the BLEND row (temperature, wind) or the served rain model's
    row (precipitation; labelled ``""`` with no rain model). ``rows`` are the
    model rows in that weather type's ranking order, each feed once and the
    rain model not again. ``blend_labels`` are the BLEND tooltip's models, by
    label; empty for precipitation.
    """

    variable: str
    top: GridRow
    rows: tuple[GridRow, ...]
    blend_labels: tuple[str, ...]


@dataclass(frozen=True)
class DayDetail:
    """The selected day's hour columns and grids.

    ``directions`` is the direction model's value per hour column, for the
    wind BLEND row's arrows (``None``: no arrow). ``gusts`` is the served
    gust per hour column in km/h, which the timeline's block maximum is
    taken over (``None``: no member gust). ``feeds`` is the feed picker's
    list: every model row's feed, once, ordered by label then id. ``tile``
    is the day's tile, for the rain summary's parts and the wind note.
    """

    day_index: int
    columns: tuple[HourColumn, ...]
    temperature: Grid
    precip: Grid
    wind: Grid
    directions: tuple[float | None, ...]
    gusts: tuple[float | None, ...]
    feeds: tuple[FeedRef, ...]
    tile: DayTile


@dataclass(frozen=True)
class ForecastPage:
    """The Forecast page's model (:func:`build_forecast_page`).

    With no sample ``empty`` is True and only ``fingerprint`` and
    ``selected_day`` carry values, as the empty tile view carries the
    fingerprint. Otherwise ``headers`` holds the 8 day headers, ``timeline``
    the 32 blocks in day order (four per day) and ``detail`` the selected
    day's grids. ``updated_at`` and ``updated_ago`` are the tile view's, for
    the "Last fetched" line.
    """

    empty: bool
    fingerprint: str
    selected_day: int
    updated_at: str | None
    updated_ago: str | None
    headers: tuple[DayHeader, ...]
    timeline: tuple[TimelineCell, ...]
    detail: DayDetail | None


# variable -> display day -> feed_id -> valid_at -> value (display samples)
_Display = dict[str, dict[int, dict[int, dict[str, float]]]]


@dataclass(frozen=True)
class _DayValues:
    """One day's hour columns and its values by ``valid_at``: the served
    temperature, wind (km/h), rain and gust (km/h), and the values of the
    day's direction and cloud models (empty with no such model)."""

    columns: tuple[HourColumn, ...]
    temp: dict[str, float | None]
    wind: dict[str, float | None]
    rain: dict[str, float | None]
    gust: dict[str, float]
    direction: dict[str, float]
    cloud: dict[str, float]


def build_forecast_page(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    timezone: str,
    rain_threshold_mm: float,
    selected_day: int,
    now: datetime | None = None,
) -> ForecastPage:
    """The Forecast page for one site, with ``selected_day``'s grids.

    One :func:`load_page_inputs` at ``now`` (the current UTC time when
    ``None``) feeds :func:`build_view` and :func:`hourly_day` for each of the
    8 days. With no sample the page is empty and carries the fingerprint.
    Otherwise the display samples are read from today's local midnight and
    grouped by variable, display day and feed. ``selected_day`` is clamped
    to 0-7.

    Per day, the direction model is the first wind member (weight order)
    with a direction sample that day, and the cloud model the first of the
    rain winner and the day ahead's ranked rain models, in rank order, with
    a cloud sample that day; each is chosen once for the day, never hour by
    hour. The served gust is the weighted mix of the wind members' gusts
    (:func:`_served_gust`).
    """
    at = now or utc_now()
    day = max(0, min(DAY_COUNT - 1, selected_day))
    inputs = load_page_inputs(conn, site_id=site_id, timezone=timezone, at=at)
    view = build_view(conn, inputs, rain_threshold_mm=rain_threshold_mm)
    if view.empty:
        return ForecastPage(
            empty=True,
            fingerprint=view.fingerprint,
            selected_day=day,
            updated_at=None,
            updated_ago=None,
            headers=(),
            timeline=(),
            detail=None,
        )
    hourly = [hourly_day(conn, inputs, d) for d in range(DAY_COUNT)]
    display = _group_display(
        load_display_samples(
            conn,
            site_id=site_id,
            since_valid_at=isoformat_utc(local_day_start(at, timezone)),
        ),
        timezone=timezone,
        now=at,
    )
    values = [_day_values(inputs, hourly[d], display, d) for d in range(DAY_COUNT)]
    return ForecastPage(
        empty=False,
        fingerprint=view.fingerprint,
        selected_day=day,
        updated_at=view.updated_at,
        updated_ago=view.updated_ago,
        headers=tuple(
            _header(tile, selected=tile.day_index == day) for tile in view.tiles
        ),
        timeline=tuple(
            cell for d in range(DAY_COUNT) for cell in _timeline(d, values[d])
        ),
        detail=_detail(inputs, hourly[day], values[day], view.tiles[day], day),
    )


def hour_columns(local_date: date, timezone: str) -> tuple[HourColumn, ...]:
    """The hour columns of one local day, in time order (23, 24 or 25).

    One per :func:`local_day_hour_instants` instant, keyed by its stored
    ``valid_at`` form, labelled with its local hour and placed in block
    ``local hour // BLOCK_HOURS`` (00, 06, 12, 18), so a DST day's block
    holds 5 or 7 hours.
    """
    tz = ZoneInfo(timezone)
    columns: list[HourColumn] = []
    for instant in local_day_hour_instants(local_date, timezone):
        local_hour = instant.astimezone(tz).hour
        columns.append(
            HourColumn(
                instant=instant,
                key=isoformat_utc(instant),
                label=f"{local_hour:02d}",
                block=local_hour // BLOCK_HOURS,
            )
        )
    return tuple(columns)


def block_icon(
    rain_values: Sequence[float], cloud_values: Sequence[float]
) -> BlockIcon | None:
    """One block's weather icon from the served rain and the cloud model.

    The rain icon when the block's rain forecasts rain (:func:`forecasts_rain`
    of its ``math.fsum``): "heavy" when its wettest hour is above
    ``HEAVY_RAIN_MM_PER_HOUR``, else "rain". Otherwise the sky icon from the
    mean of ``cloud_values``: above ``CLOUDY_PCT`` "cloud", above
    ``PARTLY_PCT`` "partly", else "sun"; ``None`` with no cloud value, never
    "sun". Empty ``rain_values`` (a day with no served rain model) take the
    sky rule.
    """
    if forecasts_rain(math.fsum(rain_values)):
        return "heavy" if max(rain_values) > HEAVY_RAIN_MM_PER_HOUR else "rain"
    if not cloud_values:
        return None
    mean = math.fsum(cloud_values) / len(cloud_values)
    if mean > CLOUDY_PCT:
        return "cloud"
    if mean > PARTLY_PCT:
        return "partly"
    return "sun"


def day_warnings(tile: DayTile) -> tuple[str, ...]:
    """The day's warning texts: the tile's six badge conditions, in order.

    Low confidence, ranking updating (a rebuilding confidence state or a
    provisional rain ranking), stale, substitute, fetch time unknown and
    partial, as the tile badges in ``forecast/_tiles.html`` decide them.
    "Fetch time unknown" is not listed when "Stale" is.
    """
    rain_provisional = tile.precip.meta.extrema_state == "rebuilding"
    checks = (
        (tile.confidence_state == "low_confidence", LOW_CONFIDENCE_TEXT),
        (
            tile.confidence_state == "rebuilding" or rain_provisional,
            RANKING_UPDATING_TEXT,
        ),
        (tile.stale, STALE_TEXT),
        (tile.substitute, SUBSTITUTE_TEXT),
        (tile.fetch_unknown and not tile.stale, FETCH_UNKNOWN_TEXT),
        (tile.partial, PARTIAL_TEXT),
    )
    return tuple(text for shown, text in checks if shown)


def rain_text(precip: PrecipCell) -> str:
    """The tile's rain summary as text, or ``NO_RAIN_TEXT`` with no winner.

    The parts of the tile's rain row: the total to one decimal with "mm",
    then the wet hours, the rain symbol and the agreement %, each when the
    tile has it, e.g. "1.2 mm · 3 h 🌧 · 67%".
    """
    if precip.total_mm is None:
        return NO_RAIN_TEXT
    text = f"{precip.total_mm:.1f} mm"
    if precip.wet_hours is not None:
        text += f"{_SEP}{precip.wet_hours} h"
    if precip.icon is not None:
        text += f" {_RAIN_SYMBOLS[precip.icon]}"
    if precip.agreement_pct is not None:
        text += f"{_SEP}{precip.agreement_pct}%"
    return text


def _group_display(
    samples: Iterable[FutureSampleRow], *, timezone: str, now: datetime
) -> _Display:
    """Display samples by variable, display day (0-7) and feed, keyed by
    ``valid_at``; the same day rule as the served samples' grouping."""
    grouped: _Display = {}
    for sample in samples:
        day = display_day_index(sample.valid_at, timezone=timezone, now=now)
        if not 0 <= day < DAY_COUNT:
            continue
        by_day = grouped.setdefault(sample.variable, {}).setdefault(day, {})
        by_day.setdefault(sample.feed_id, {})[sample.valid_at] = sample.value
    return grouped


def _day_values(
    inputs: PageInputs, hourly: HourlyDay, display: _Display, day: int
) -> _DayValues:
    """One day's columns and values: the served lines read from the hourly
    payload by ``valid_at``, the served gust, and the direction and cloud
    models' values (:func:`build_forecast_page` names the two rules)."""
    # hourly_day sets "hours" to the hour axis and "blend" to the served
    # lines on it, one value per hour.
    hours = cast(list[str], hourly.payload["hours"])
    blend = cast(dict[str, list[float | None]], hourly.payload["blend"])
    wind = dict(zip(hours, blend["wind_kmh"], strict=True))
    members = [candidate.feed_id for candidate in hourly.selections["wind"].feeds]
    directions = display.get("wind_direction", {}).get(day, {})
    direction_id = next((f for f in members if f in directions), None)
    clouds = display.get("cloud_cover", {}).get(day, {})
    winner = hourly.rain.winner_id
    ranks = inputs.rain_ranks.get(day)
    cloud_order = dict.fromkeys(
        ([] if winner is None else [winner])
        + ([] if ranks is None else [rank.feed_id for rank in ranks.ranked])
    )
    cloud_id = next((f for f in cloud_order if f in clouds), None)
    return _DayValues(
        columns=hour_columns(inputs.today + timedelta(days=day), inputs.timezone),
        temp=dict(zip(hours, blend["temp_c"], strict=True)),
        wind=wind,
        rain=dict(zip(hours, blend["precip_mm"], strict=True)),
        gust=_served_gust(
            display.get("wind_gust", {}).get(day, {}),
            members,
            hourly.wind_weights,
            wind,
        ),
        direction={} if direction_id is None else directions[direction_id],
        cloud={} if cloud_id is None else clouds[cloud_id],
    )


def _served_gust(
    gusts: Mapping[int, Mapping[str, float]],
    members: Sequence[int],
    weights: Mapping[int, float],
    served_wind: Mapping[str, float | None],
) -> dict[str, float]:
    """The served gust by ``valid_at``, in km/h.

    The weighted mix of the wind members' gusts with the served wind weights
    (:func:`wind_blend.weighted_hourly`, the served wind line's call),
    renormalized at each hour over the members with a gust there, then
    never below the served wind wherever that has the hour. An hour with no
    member gust is absent.
    """
    mixed = wind_blend.weighted_hourly(
        {
            feed_id: {
                valid_at: ms_to_kmh(gust)
                for valid_at, gust in gusts.get(feed_id, {}).items()
            }
            for feed_id in members
        },
        weights,
    )
    served: dict[str, float] = {}
    for valid_at, gust in mixed.items():
        wind = served_wind.get(valid_at)
        served[valid_at] = gust if wind is None else max(gust, wind)
    return served


def _timeline(day: int, values: _DayValues) -> list[TimelineCell]:
    """The day's four timeline blocks (:class:`TimelineCell`)."""
    cells: list[TimelineCell] = []
    for block in range(BLOCKS_PER_DAY):
        columns = [column for column in values.columns if column.block == block]
        keys = [column.key for column in columns]
        rep = keys[0] if keys else None
        rain = _present(values.rain, keys)
        gusts = _present(values.gust, keys)
        cells.append(
            TimelineCell(
                day_index=day,
                block=block,
                label=columns[0].label if columns else f"{block * BLOCK_HOURS:02d}",
                key=rep,
                temp_c=_at(values.temp, rep),
                wind_kmh=_at(values.wind, rep),
                rain_mm=math.fsum(rain) if rain else None,
                gust_kmh=max(gusts) if gusts else None,
                direction_deg=_at(values.direction, rep),
                icon=block_icon(rain, _present(values.cloud, keys)),
            )
        )
    return cells


def _detail(
    inputs: PageInputs,
    hourly: HourlyDay,
    values: _DayValues,
    tile: DayTile,
    day: int,
) -> DayDetail:
    """The selected day's grids.

    Temperature: the BLEND row (the tile's high and low last), then
    ``ladder["temperature"]``. Wind: the BLEND row (the tile's max last),
    then the weighted members in weight order, then ``ladder["wind"]``.
    Precipitation: the served rain model's row (the tile's total and rain
    text last), then the day ahead's ranked rain models in rank order
    without the winner, then ``ladder["precip"]``.
    """
    columns = values.columns
    local_date = inputs.today + timedelta(days=day)
    grouped = inputs.grouped
    ranks = inputs.rain_ranks.get(day)
    ranked = [] if ranks is None else [rank.feed_id for rank in ranks.ranked]
    temp_feeds = hourly.selections["temperature"].feeds
    wind_feeds = hourly.selections["wind"].feeds
    # The rain top row is named from the winner's own samples, which the
    # rain choice requires on the day.
    rain_winner = hourly.rain.winner_id
    winner_samples: list[FutureSampleRow] = (
        []
        if rain_winner is None
        else grouped.get("precip", {}).get(day, {}).get(rain_winner, [])
    )

    def model_rows(
        variable: str, groups: Sequence[Iterable[int]], exclude: int | None
    ) -> tuple[GridRow, ...]:
        return _model_rows(
            grouped.get(variable, {}).get(day, {}),
            groups,
            columns,
            variable=variable,
            local_date=local_date,
            timezone=inputs.timezone,
            exclude=exclude,
        )

    temperature = Grid(
        variable="temperature",
        top=GridRow(
            feed_id=None,
            label=BLEND_LABEL,
            values=_column_values(values.temp, columns),
            day_high=tile.temp.high_c,
            day_low=tile.temp.low_c,
            day_total=None,
            day_text=None,
        ),
        rows=model_rows("temperature", [hourly.ladder["temperature"]], None),
        blend_labels=_labels(temp_feeds),
    )
    precip = Grid(
        variable="precip",
        top=GridRow(
            feed_id=None,
            label=(
                model_label(winner_samples[0].source, winner_samples[0].model)
                if winner_samples
                else ""
            ),
            values=_column_values(values.rain, columns),
            day_high=None,
            day_low=None,
            day_total=tile.precip.total_mm,
            day_text=rain_text(tile.precip),
        ),
        rows=model_rows(
            "precip", [ranked, hourly.ladder["precip"]], hourly.rain.winner_id
        ),
        blend_labels=(),
    )
    wind = Grid(
        variable="wind",
        top=GridRow(
            feed_id=None,
            label=BLEND_LABEL,
            values=_column_values(values.wind, columns),
            day_high=tile.wind.max_kmh,
            day_low=None,
            day_total=None,
            day_text=None,
        ),
        rows=model_rows(
            "wind",
            [[candidate.feed_id for candidate in wind_feeds], hourly.ladder["wind"]],
            None,
        ),
        blend_labels=_labels(wind_feeds),
    )
    picker = {
        row.feed_id: row.label
        for grid in (temperature, precip, wind)
        for row in grid.rows
        if row.feed_id is not None
    }
    return DayDetail(
        day_index=day,
        columns=columns,
        temperature=temperature,
        precip=precip,
        wind=wind,
        directions=_column_values(values.direction, columns),
        gusts=_column_values(values.gust, columns),
        feeds=tuple(
            FeedRef(feed_id=feed_id, label=label)
            for feed_id, label in sorted(
                picker.items(), key=lambda item: (item[1], item[0])
            )
        ),
        tile=tile,
    )


def _model_rows(
    feeds_samples: Mapping[int, Sequence[FutureSampleRow]],
    groups: Sequence[Iterable[int]],
    columns: Sequence[HourColumn],
    *,
    variable: str,
    local_date: date,
    timezone: str,
    exclude: int | None,
) -> tuple[GridRow, ...]:
    """One grid's model rows: every feed with a sample of ``variable`` on the
    day, in the order ``groups`` first lists them, ``exclude`` never.

    A feed no group lists would come last, in feed-id order; there is none
    today, because each variable's ``ladder`` lists every feed with a sample.
    """
    present = {feed_id for feed_id, samples in feeds_samples.items() if samples}
    order: dict[int, None] = {}
    for group in (*groups, sorted(present)):
        for feed_id in group:
            if feed_id in present and feed_id != exclude:
                order.setdefault(feed_id, None)
    return tuple(
        _model_row(
            feed_id,
            feeds_samples[feed_id],
            columns,
            variable=variable,
            local_date=local_date,
            timezone=timezone,
        )
        for feed_id in order
    )


def _model_row(
    feed_id: int,
    samples: Sequence[FutureSampleRow],
    columns: Sequence[HourColumn],
    *,
    variable: str,
    local_date: date,
    timezone: str,
) -> GridRow:
    """One model's row.

    Its own sample at each column's ``valid_at`` (wind through
    :func:`ms_to_kmh`, as the hourly per-feed series reads it). The last
    column follows the tile's coverage rules: the high and low when the
    feed :func:`covers_local_day`, the wind max when it
    :func:`clears_coverage`, the total when it
    :func:`covers_local_day_exactly`; otherwise ``None``.
    """
    wind = variable == "wind"
    by_key = {s.valid_at: ms_to_kmh(s.value) if wind else s.value for s in samples}
    valid_ats = [s.valid_at for s in samples]
    raw = [s.value for s in samples]
    high: float | None = None
    low: float | None = None
    total: float | None = None
    if variable == "temperature" and covers_local_day(
        valid_ats, local_date=local_date, timezone=timezone
    ):
        high, low = max(raw), min(raw)
    elif wind and clears_coverage(covered_hours(valid_ats)):
        high = ms_to_kmh(max(raw))
    elif variable == "precip" and covers_local_day_exactly(
        valid_ats, local_date=local_date, timezone=timezone
    ):
        total = math.fsum(raw)
    return GridRow(
        feed_id=feed_id,
        label=model_label(samples[0].source, samples[0].model),
        values=_column_values(by_key, columns),
        day_high=high,
        day_low=low,
        day_total=total,
        day_text=None,
    )


def _header(tile: DayTile, *, selected: bool) -> DayHeader:
    """One day's header from its tile (:class:`DayHeader`)."""
    local_date = date.fromisoformat(tile.date_iso)
    return DayHeader(
        day_index=tile.day_index,
        label=tile.label,
        short_label=local_date.strftime("%a"),
        date_iso=tile.date_iso,
        date_dm=local_date.strftime("%d-%m"),
        high_c=tile.temp.high_c,
        low_c=tile.temp.low_c,
        extrema_unavailable=tile.temp.meta.extrema_unavailable,
        not_available=tile.state == "not_available",
        selected=selected,
        warnings=day_warnings(tile),
    )


def _labels(feeds: Sequence[CellCandidate]) -> tuple[str, ...]:
    return tuple(model_label(c.source, c.model) for c in feeds)


def _column_values(
    by_key: Mapping[str, float | None], columns: Sequence[HourColumn]
) -> tuple[float | None, ...]:
    return tuple(by_key.get(column.key) for column in columns)


def _present(by_key: Mapping[str, float | None], keys: Iterable[str]) -> list[float]:
    out: list[float] = []
    for key in keys:
        value = by_key.get(key)
        if value is not None:
            out.append(value)
    return out


def _at(by_key: Mapping[str, float | None], key: str | None) -> float | None:
    return None if key is None else by_key.get(key)
