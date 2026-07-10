"""Weather.com PWS adapter seam."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from zoneinfo import ZoneInfo

import httpx

from wxverify.core.timeutil import floor_hour, isoformat_utc, parse_utc, utc_now
from wxverify.core.units import kmh_to_ms
from wxverify.obs.config import RECENT_REFRESH_HOURS

logger = logging.getLogger(__name__)


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


@dataclass(frozen=True)
class CurrentObservation:
    """A single current-observation snapshot mapped to ``station_current_obs``.

    Every measurement is optional: a missing or uncoercible upstream field maps
    to ``None`` (a NULL column), never a fabricated zero. ``obs_time_utc`` is the
    full-precision ISO-``Z`` instant from ``_obs_instant`` (never hour-floored),
    or ``None`` when the payload's timestamp is missing/unparseable.
    """

    obs_time_utc: str | None
    temp: float | None
    humidity: float | None
    dewpt: float | None
    wind_speed: float | None
    wind_gust: float | None
    wind_dir: float | None
    pressure: float | None
    precip_rate: float | None
    precip_total: float | None
    uv: float | None
    neighborhood: str | None


async def validate_station(
    station_id: str, api_key: str, *, lat: float | None = None, lon: float | None = None
) -> PwsStation:
    # Tests and local dry runs can pass explicit coordinates and still exercise
    # the station-create contract without reaching weather.com.
    if lat is not None and lon is not None:
        return PwsStation(station_id=station_id, lat=lat, lon=lon)
    logger.debug("pws validate_station station=%s", station_id)
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
    logger.debug("pws hourly_history request station=%s hours=%s", station_id, hours)
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
    observations = [
        observation
        for observation in observations_from_payload(response.json(), timezone=timezone)
        if parse_utc(observation.valid_at) >= cutoff
    ]
    logger.debug(
        "pws hourly_history response station=%s samples=%s",
        station_id,
        len(observations),
    )
    return observations


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
    logger.debug(
        "pws history_range request station=%s window=%s..%s",
        station_id,
        window_start,
        window_end,
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
    filtered = [
        observation
        for observation in observations
        if start <= parse_utc(observation.valid_at) < end
    ]
    logger.debug(
        "pws history_range response station=%s samples=%s", station_id, len(filtered)
    )
    return filtered


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


def _obs_datetime(row: dict[str, object]) -> datetime | None:
    """Tolerant parse of an obs timestamp to a tz-aware UTC ``datetime``, else None.

    The shared normalization behind ``_valid_at`` (hourly stream) and
    ``_obs_instant`` (current stream): prefer a numeric ``epoch``, else normalize
    ``obsTimeUtc`` (strip trailing `` UTC``, space→``T``, append ``Z`` when
    offset-less). The two callers differ ONLY in whether they hour-floor the
    result. This is the battle-tested normalization ``_valid_at`` has always run
    against the live weather.com PWS payload; ``_obs_instant`` reuses it verbatim.
    """
    raw_epoch = row.get("valid_time_gmt")
    if raw_epoch is None:
        raw_epoch = row.get("epoch")
    if isinstance(raw_epoch, int | float):
        return datetime.fromtimestamp(raw_epoch, tz=UTC)
    if isinstance(raw_epoch, str) and raw_epoch.isdigit():
        return datetime.fromtimestamp(int(raw_epoch), tz=UTC)

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
        return parse_utc(normalized)
    except ValueError:
        return None


def _valid_at(row: dict[str, object]) -> str | None:
    """Hourly-stream timestamp: shared normalization, then hour-floored ISO-``Z``.

    Behaviour is unchanged from before the ``_obs_datetime`` extraction — the
    hourly history/backfill path depends on the ``floor_hour`` bucketing.
    """
    parsed = _obs_datetime(row)
    if parsed is None:
        return None
    return isoformat_utc(floor_hour(parsed))


def _obs_instant(row: dict[str, object]) -> str | None:
    """Current-stream timestamp: shared normalization WITHOUT the hour-floor.

    Identical to ``_valid_at`` except the final ``floor_hour`` wrap is omitted, so
    the returned ISO-``Z`` instant keeps full precision — cadence learning needs
    real inter-obs gaps, not hour buckets. Returns ``None`` on missing/unparseable.
    """
    parsed = _obs_datetime(row)
    if parsed is None:
        return None
    return isoformat_utc(parsed)


def current_obs_from_payload(data: object) -> CurrentObservation | None:
    """Map ``observations[0]`` of a ``/observations/current`` payload to columns.

    Reads the top-level ``humidity``, ``winddir``, ``uv``, ``obsTimeUtc`` and
    ``neighborhood`` alongside the ``metric`` sub-object (``temp``, ``windSpeed``,
    ``windGust``, ``pressure``, ``precipRate``, ``precipTotal``, ``dewpt``). Values
    are km/h / mm / hPa in the station's native ``units:"m"`` form — this is the
    raw display snapshot table, NOT the SI-normalized scoring path, so no unit
    conversion is applied. Returns ``None`` when the payload has no first
    observation row (the caller treats that as OFFLINE). Missing fields → ``None``.
    """
    if not isinstance(data, dict):
        return None
    payload = cast(dict[str, object], data)
    observations_obj = payload.get("observations")
    if not isinstance(observations_obj, list) or not observations_obj:
        return None
    first = cast(list[object], observations_obj)[0]
    if not isinstance(first, dict):
        return None
    row = cast(dict[str, object], first)
    metric_obj = row.get("metric")
    metric = cast(dict[str, object], metric_obj) if isinstance(metric_obj, dict) else {}
    neighborhood_obj = row.get("neighborhood")
    neighborhood = str(neighborhood_obj) if isinstance(neighborhood_obj, str) else None
    return CurrentObservation(
        obs_time_utc=_obs_instant(row),
        temp=_number(metric, "temp"),
        humidity=_number(row, "humidity"),
        dewpt=_number(metric, "dewpt"),
        wind_speed=_number(metric, "windSpeed"),
        wind_gust=_number(metric, "windGust"),
        wind_dir=_number(row, "winddir"),
        pressure=_number(metric, "pressure"),
        precip_rate=_number(metric, "precipRate"),
        precip_total=_number(metric, "precipTotal"),
        uv=_number(row, "uv"),
        neighborhood=neighborhood,
    )


async def fetch_current_observation(
    pws_station_id: str,
    api_key: str,
    *,
    client: httpx.AsyncClient | None = None,
) -> httpx.Response:
    """GET ``/v2/pws/observations/current`` and return the raw response.

    The handler needs status-code granularity (204 vs 429 vs 401 vs 2xx-with-body)
    to drive the poll-state machine, so this seam returns the ``httpx.Response``
    un-parsed and does NOT call ``raise_for_status`` — the caller classifies. A
    transport-level failure propagates as the corresponding ``httpx`` exception.
    """
    if client is None:
        async with httpx.AsyncClient() as owned_client:
            return await fetch_current_observation(
                pws_station_id, api_key, client=owned_client
            )
    logger.debug("pws current_obs request station=%s", pws_station_id)
    return await client.get(
        "https://api.weather.com/v2/pws/observations/current",
        params={
            "stationId": pws_station_id,
            "format": "json",
            "units": "m",
            "apiKey": api_key,
        },
        timeout=httpx.Timeout(10.0, connect=5.0),
    )


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
