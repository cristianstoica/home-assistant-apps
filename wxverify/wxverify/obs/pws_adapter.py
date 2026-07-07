"""Weather.com PWS adapter seam."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

import httpx

from wxverify.core.timeutil import floor_hour, isoformat_utc, parse_utc, utc_now
from wxverify.core.units import kmh_to_ms
from wxverify.obs.config import RECENT_REFRESH_HOURS


@dataclass(frozen=True)
class PwsStation:
    station_id: str
    lat: float
    lon: float
    neighborhood: str | None = None


@dataclass(frozen=True)
class PwsObservation:
    variable: str
    valid_at: str
    value: float
    source_raw: str


@dataclass(frozen=True)
class _ParsedObservation:
    index: int
    valid_at: str
    local_day: str
    metric: dict[str, object]


async def validate_station(
    station_id: str, api_key: str, *, lat: float | None = None, lon: float | None = None
) -> PwsStation:
    # Tests and local dry runs can pass explicit coordinates and still exercise
    # the station-create contract without reaching weather.com.
    if lat is not None and lon is not None:
        return PwsStation(station_id=station_id, lat=lat, lon=lon)
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://api.weather.com/v2/pws/observations/current",
            params={
                "stationId": station_id,
                "format": "json",
                "units": "m",
                "apiKey": api_key,
            },
            timeout=httpx.Timeout(10.0, connect=5.0),
        )
        response.raise_for_status()
        data = cast(dict[str, Any], response.json())
    observations_obj = data.get("observations")
    if not isinstance(observations_obj, list) or not observations_obj:
        raise RuntimeError("station returned no current observation")
    observations = cast(list[object], observations_obj)
    first = observations[0]
    if not isinstance(first, dict):
        raise RuntimeError("invalid station response")
    first_map = cast(dict[str, Any], first)
    return PwsStation(
        station_id=station_id,
        lat=float(first_map["lat"]),
        lon=float(first_map["lon"]),
        neighborhood=str(first_map.get("neighborhood"))
        if first_map.get("neighborhood") is not None
        else None,
    )


async def fetch_hourly_history(
    station_id: str,
    api_key: str,
    *,
    hours: int = RECENT_REFRESH_HOURS,
    timezone: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> list[PwsObservation]:
    if client is None:
        async with httpx.AsyncClient() as owned_client:
            return await fetch_hourly_history(
                station_id,
                api_key,
                hours=hours,
                timezone=timezone,
                client=owned_client,
            )
    response = await client.get(
        "https://api.weather.com/v2/pws/observations/hourly/7day",
        params={
            "stationId": station_id,
            "format": "json",
            "units": "m",
            "apiKey": api_key,
        },
        timeout=httpx.Timeout(10.0, connect=5.0),
    )
    response.raise_for_status()
    cutoff = utc_now() - timedelta(hours=hours)
    return [
        observation
        for observation in observations_from_payload(response.json(), timezone=timezone)
        if parse_utc(observation.valid_at) >= cutoff
    ]


async def fetch_hourly_history_range(
    station_id: str,
    api_key: str,
    *,
    window_start: str,
    window_end: str,
    timezone: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> list[PwsObservation]:
    start = parse_utc(window_start)
    end = parse_utc(window_end)
    if end <= start:
        return []
    start_date, end_date = _history_date_range(start, end, timezone)
    if client is None:
        async with httpx.AsyncClient() as owned_client:
            return await fetch_hourly_history_range(
                station_id,
                api_key,
                window_start=window_start,
                window_end=window_end,
                timezone=timezone,
                client=owned_client,
            )
    response = await client.get(
        "https://api.weather.com/v2/pws/history/hourly",
        params={
            "stationId": station_id,
            "format": "json",
            "units": "m",
            "startDate": start_date,
            "endDate": end_date,
            "numericPrecision": "decimal",
            "apiKey": api_key,
        },
        timeout=httpx.Timeout(20.0, connect=5.0),
    )
    response.raise_for_status()
    observations = observations_from_payload(response.json(), timezone=timezone)
    return [
        observation
        for observation in observations
        if start <= parse_utc(observation.valid_at) < end
    ]


def observations_from_payload(
    data: object, *, timezone: str | None = None
) -> list[PwsObservation]:
    if not isinstance(data, dict):
        return []
    payload = cast(dict[str, object], data)
    observations_obj = payload.get("observations")
    if not isinstance(observations_obj, list):
        return []
    observations = cast(list[object], observations_obj)
    parsed_rows: list[_ParsedObservation] = []
    for idx, item in enumerate(observations):
        if not isinstance(item, dict):
            continue
        row = cast(dict[str, object], item)
        valid_at = _valid_at(row)
        if valid_at is None:
            continue
        metric_obj = row.get("metric")
        if not isinstance(metric_obj, dict):
            continue
        parsed_rows.append(
            _ParsedObservation(
                index=idx,
                valid_at=valid_at,
                local_day=_local_day(row, valid_at, timezone),
                metric=cast(dict[str, object], metric_obj),
            )
        )
    precip_increments = _precip_increments(parsed_rows)
    out: list[PwsObservation] = []
    for row in parsed_rows:
        metric = row.metric
        temp = _number(metric, "temp", "tempAvg", "tempHigh", "tempLow")
        if temp is not None:
            out.append(
                PwsObservation(
                    variable="temperature",
                    valid_at=row.valid_at,
                    value=temp,
                    source_raw=f"{temp} C",
                )
            )
        wind = _number(
            metric,
            "windSpeed",
            "windSpeedAvg",
            "windspeedAvg",
            "windGust",
            "windgustHigh",
        )
        if wind is not None:
            out.append(
                PwsObservation(
                    variable="wind",
                    valid_at=row.valid_at,
                    value=kmh_to_ms(wind),
                    source_raw=f"{wind} km/h",
                )
            )
        precip_increment = precip_increments.get(row.index)
        precip_total = _number(metric, "precipTotal")
        if precip_increment is not None and precip_total is not None:
            out.append(
                PwsObservation(
                    variable="precip",
                    valid_at=row.valid_at,
                    value=precip_increment,
                    source_raw=f"{precip_total} mm precipTotal",
                )
            )
    return out


def _valid_at(row: dict[str, object]) -> str | None:
    raw_epoch = row.get("valid_time_gmt")
    if raw_epoch is None:
        raw_epoch = row.get("epoch")
    if isinstance(raw_epoch, int | float):
        return isoformat_utc(floor_hour(datetime.fromtimestamp(raw_epoch, tz=UTC)))
    if isinstance(raw_epoch, str) and raw_epoch.isdigit():
        return isoformat_utc(floor_hour(datetime.fromtimestamp(int(raw_epoch), tz=UTC)))

    raw_time = row.get("obsTimeUtc")
    if raw_time is None:
        raw_time = row.get("validTimeUtc")
    if not isinstance(raw_time, str) or not raw_time:
        return None
    stripped = raw_time.strip()
    if not stripped:
        return None
    if stripped.endswith(" UTC"):
        stripped = f"{stripped[:-4]}Z"
    normalized = stripped.replace(" ", "T")
    suffix = normalized[10:]
    if normalized[-1].isdigit() and "+" not in suffix and "-" not in suffix:
        normalized = f"{normalized}Z"
    try:
        return isoformat_utc(floor_hour(parse_utc(normalized)))
    except ValueError:
        return None


def _number(metric: dict[str, object], *keys: str) -> float | None:
    for key in keys:
        value = metric.get(key)
        if isinstance(value, int | float):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                continue
    return None


def _precip_increments(rows: list[_ParsedObservation]) -> dict[int, float]:
    increments: dict[int, float] = {}
    previous_total: float | None = None
    previous_day: str | None = None
    for row in sorted(rows, key=lambda item: (item.valid_at, item.index)):
        total = _number(row.metric, "precipTotal")
        if total is None:
            continue
        if previous_total is None or previous_day != row.local_day:
            increment = total
        else:
            increment = total - previous_total
            if increment < 0:
                increment = 0.0
        increments[row.index] = increment
        previous_total = total
        previous_day = row.local_day
    return increments


def _local_day(
    row: dict[str, object], valid_at: str, timezone: str | None = None
) -> str:
    local = _local_day_from_payload(row)
    if local is not None:
        return local
    valid = parse_utc(valid_at)
    if timezone is not None:
        return valid.astimezone(ZoneInfo(timezone)).date().isoformat()
    return valid.date().isoformat()


def _local_day_from_payload(row: dict[str, object]) -> str | None:
    for key in ("obsTimeLocal", "validTimeLocal"):
        value = row.get(key)
        if not isinstance(value, str):
            continue
        match = re.search(r"(\d{4})-?(\d{2})-?(\d{2})", value)
        if match is not None:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return None


def _history_date_range(
    start: datetime, end: datetime, timezone: str | None
) -> tuple[str, str]:
    tz = UTC if timezone is None else ZoneInfo(timezone)
    local_start = start.astimezone(tz)
    local_end = (end - timedelta(microseconds=1)).astimezone(tz)
    return local_start.strftime("%Y%m%d"), local_end.strftime("%Y%m%d")
