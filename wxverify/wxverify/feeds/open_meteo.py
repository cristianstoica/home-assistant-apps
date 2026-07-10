"""Open-Meteo forecast adapter."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import ClassVar, Final, cast

import httpx
from pydantic import BaseModel, ConfigDict

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.core.units import kmh_to_ms
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastRequest,
    GridProvenance,
    NormalizedSample,
)

RUN_CADENCE_HOURS: Final[dict[str, int]] = {
    "ecmwf_ifs": 6,
    "gfs_global": 6,
    "icon_global": 6,
    "gem_global": 6,
    "meteofrance_arpege_world": 6,
    "jma_gsm": 6,
    "ukmo_global_deterministic_10km": 6,
}
RUN_AVAILABILITY_LAG_MINUTES: Final[dict[str, int]] = {
    model: 90 for model in RUN_CADENCE_HOURS
}
VARIABLE_MAP: Final[dict[str, str]] = {
    "temperature": "temperature_2m",
    "wind": "wind_speed_10m",
    "precip": "precipitation",
}
TRACE_NEGATIVE_PRECIP_MIN: Final = -0.1

logger = logging.getLogger(__name__)


class OpenMeteoResponse(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    latitude: float
    longitude: float
    elevation: float | None = None
    hourly: dict[str, list[str | int | float | None]]


def _snap_run(model: str, fetch_time: str | None = None) -> str:
    now = parse_utc(fetch_time) if fetch_time else utc_now()
    lagged = now - timedelta(minutes=RUN_AVAILABILITY_LAG_MINUTES.get(model, 90))
    cadence = RUN_CADENCE_HOURS.get(model, 6)
    hour = (lagged.hour // cadence) * cadence
    snapped = lagged.replace(hour=hour, minute=0, second=0, microsecond=0)
    return isoformat_utc(snapped)


class OpenMeteoAdapter:
    supports_historical: ClassVar[bool] = True

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
        return CostEstimate(calls=1)

    async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
        hourly = [VARIABLE_MAP[v] for v in req.variables if v in VARIABLE_MAP]
        logger.debug(
            "open_meteo forecast request model=%s lat=%s lon=%s lead=%s",
            req.model,
            req.lat,
            req.lon,
            req.max_lead_hours,
        )
        response = await self._client.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": req.lat,
                "longitude": req.lon,
                "models": req.model,
                "hourly": ",".join(hourly),
                "timezone": "UTC",
                "forecast_hours": req.max_lead_hours,
            },
            timeout=httpx.Timeout(10.0, connect=5.0),
        )
        response.raise_for_status()
        payload = OpenMeteoResponse.model_validate(response.json())
        data = payload.model_dump()
        issued_at = _snap_run(req.model)
        samples = _samples_from_hourly(req.model, issued_at, data)
        logger.debug(
            "open_meteo forecast response status=%s samples=%s",
            response.status_code,
            len(samples),
        )
        grid = GridProvenance(
            grid_lat=payload.latitude,
            grid_lon=payload.longitude,
            grid_elevation_m=payload.elevation,
        )
        return FetchResult(samples=samples, grid=grid)

    async def fetch_historical(
        self, req: ForecastRequest, *, window_start: str, window_end: str
    ) -> FetchResult | None:
        hourly = _historical_hourly_names(req)
        if not hourly:
            return FetchResult(samples=[], grid=None)
        logger.debug(
            "open_meteo historical request model=%s window=%s..%s",
            req.model,
            window_start,
            window_end,
        )
        response = await self._client.get(
            "https://previous-runs-api.open-meteo.com/v1/forecast",
            params={
                "latitude": req.lat,
                "longitude": req.lon,
                "models": req.model,
                "hourly": ",".join(hourly),
                "timezone": "UTC",
                "start_date": parse_utc(window_start).date().isoformat(),
                "end_date": parse_utc(window_end).date().isoformat(),
            },
            timeout=httpx.Timeout(15.0, connect=5.0),
        )
        response.raise_for_status()
        payload = OpenMeteoResponse.model_validate(response.json())
        data = payload.model_dump()
        samples = _historical_samples_from_hourly(req, data, window_start, window_end)
        logger.debug(
            "open_meteo historical response status=%s samples=%s",
            response.status_code,
            len(samples),
        )
        grid = GridProvenance(
            grid_lat=payload.latitude,
            grid_lon=payload.longitude,
            grid_elevation_m=payload.elevation,
        )
        return FetchResult(samples=samples, grid=grid)


def _samples_from_hourly(
    model: str, issued_at: str, data: object
) -> list[NormalizedSample]:
    if not isinstance(data, dict):
        return []
    payload = cast(dict[str, object], data)
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        return []
    hourly_map = cast(dict[str, object], hourly)
    times = hourly_map.get("time")
    if not isinstance(times, list):
        return []
    time_values = cast(list[object], times)
    out: list[NormalizedSample] = []
    for variable, provider_name in VARIABLE_MAP.items():
        values = hourly_map.get(provider_name)
        if not isinstance(values, list):
            continue
        sample_values = cast(list[object], values)
        for idx, raw_time in enumerate(time_values):
            if idx >= len(sample_values) or sample_values[idx] is None:
                continue
            raw_obj = sample_values[idx]
            if not isinstance(raw_obj, str | int | float):
                continue
            valid_at = isoformat_utc(parse_utc(str(raw_time)))
            raw_value = float(raw_obj)
            value = _normalized_value(variable, raw_value)
            lead = int(
                (parse_utc(valid_at) - parse_utc(issued_at)).total_seconds() // 3600
            )
            if lead < 1:
                continue
            out.append(
                NormalizedSample(
                    model=model,
                    variable=variable,
                    issued_at=issued_at,
                    valid_at=valid_at,
                    lead_hours=lead,
                    value=value,
                    source_raw=f"{raw_value}",
                    model_run_id=f"{model}:{issued_at}",
                )
            )
    return out


def _normalized_value(variable: str, raw_value: float) -> float:
    if variable == "wind":
        return kmh_to_ms(raw_value)
    if variable == "precip" and TRACE_NEGATIVE_PRECIP_MIN <= raw_value < 0.0:
        # Open-Meteo can emit tiny negative JMA GSM precipitation artifacts
        # after interpolation. Precipitation is physically floored at zero,
        # while source_raw preserves the provider value for diagnostics.
        return 0.0
    return raw_value


def _historical_hourly_names(req: ForecastRequest) -> list[str]:
    days = range(1, min(7, req.max_lead_hours // 24) + 1)
    return [
        f"{provider_name}_previous_day{day}"
        for variable, provider_name in VARIABLE_MAP.items()
        if variable in req.variables
        for day in days
    ]


def _historical_samples_from_hourly(
    req: ForecastRequest, data: object, window_start: str, window_end: str
) -> list[NormalizedSample]:
    if not isinstance(data, dict):
        return []
    payload = cast(dict[str, object], data)
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        return []
    hourly_map = cast(dict[str, object], hourly)
    times = hourly_map.get("time")
    if not isinstance(times, list):
        return []
    start = parse_utc(window_start)
    end = parse_utc(window_end)
    out: list[NormalizedSample] = []
    for variable, provider_name in VARIABLE_MAP.items():
        if variable not in req.variables:
            continue
        for day in range(1, min(7, req.max_lead_hours // 24) + 1):
            values = hourly_map.get(f"{provider_name}_previous_day{day}")
            if not isinstance(values, list):
                continue
            out.extend(
                _historical_series_samples(
                    req=req,
                    variable=variable,
                    day=day,
                    times=cast(list[object], times),
                    values=cast(list[object], values),
                    start=start,
                    end=end,
                )
            )
    return out


def _historical_series_samples(
    *,
    req: ForecastRequest,
    variable: str,
    day: int,
    times: list[object],
    values: list[object],
    start: datetime,
    end: datetime,
) -> list[NormalizedSample]:
    out: list[NormalizedSample] = []
    for idx, raw_time in enumerate(times):
        if idx >= len(values) or values[idx] is None:
            continue
        raw_value = values[idx]
        if not isinstance(raw_value, str | int | float):
            continue
        valid_at = isoformat_utc(parse_utc(str(raw_time)))
        valid_dt = parse_utc(valid_at)
        if valid_dt < start or valid_dt >= end:
            continue
        issued_at = isoformat_utc(valid_dt - timedelta(days=day))
        value = _normalized_value(variable, float(raw_value))
        out.append(
            NormalizedSample(
                model=req.model,
                variable=variable,
                issued_at=issued_at,
                valid_at=valid_at,
                lead_hours=day * 24,
                value=value,
                source_raw=f"{raw_value} previous_day{day}",
                model_run_id=f"{req.model}:{issued_at}",
            )
        )
    return out
