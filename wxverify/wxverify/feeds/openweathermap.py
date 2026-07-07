"""OpenWeatherMap One Call API 3.0 adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar, Final

import httpx
from pydantic import BaseModel, ConfigDict, Field

from wxverify.core.timeutil import isoformat_utc, lead_hours
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastRequest,
    NormalizedSample,
)
from wxverify.feeds.synthetic_run import snap_run

_ENDPOINT: Final = "https://api.openweathermap.org/data/3.0/onecall"


class OwmRain(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    # `rain.1h` is omitted entirely when zero -- absent is treated as 0.0 mm.
    one_hour: float | None = Field(default=None, alias="1h")


class OwmHour(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    dt: int
    temp: float | None = None
    wind_speed: float | None = None
    rain: OwmRain | None = None


def _no_hours() -> list[OwmHour]:
    return []


class OwmResponse(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    hourly: list[OwmHour] = Field(default_factory=_no_hours)


class OpenWeatherMapAdapter:
    supports_historical: ClassVar[bool] = False

    def __init__(self, api_key: str, client: httpx.AsyncClient) -> None:
        self._api_key = api_key
        self._client = client

    def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
        return CostEstimate(calls=1)

    async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
        response = await self._client.get(
            _ENDPOINT,
            params={
                "lat": req.lat,
                "lon": req.lon,
                "units": "metric",
                "exclude": "current,minutely,daily,alerts",
                "appid": self._api_key,
            },
            timeout=httpx.Timeout(15.0, connect=5.0),
        )
        response.raise_for_status()
        payload = OwmResponse.model_validate(response.json())
        return _to_fetch_result(req, payload)

    async def fetch_historical(
        self, req: ForecastRequest, *, window_start: str, window_end: str
    ) -> FetchResult | None:
        return None


def _to_fetch_result(req: ForecastRequest, payload: OwmResponse) -> FetchResult:
    issued_at = snap_run()
    samples: list[NormalizedSample] = []
    for hour in payload.hourly:
        valid_at = isoformat_utc(datetime.fromtimestamp(hour.dt, tz=UTC))
        lead = lead_hours(issued_at, valid_at)
        if lead < 1 or lead > req.max_lead_hours:
            continue
        samples.extend(_hour_samples(req, issued_at, valid_at, lead, hour))
    return FetchResult(samples=samples, grid=None)


def _hour_samples(
    req: ForecastRequest,
    issued_at: str,
    valid_at: str,
    lead: int,
    hour: OwmHour,
) -> list[NormalizedSample]:
    precip = 0.0
    if hour.rain is not None and hour.rain.one_hour is not None:
        precip = hour.rain.one_hour
    # OWM metric wind_speed is already m/s, so no conversion. Precip is always
    # emitted (absent `rain.1h` resolves to 0.0 mm).
    specs: tuple[tuple[str, float | None, str], ...] = (
        ("temperature", hour.temp, "C"),
        ("wind", hour.wind_speed, "m/s"),
        ("precip", precip, "mm"),
    )
    out: list[NormalizedSample] = []
    for variable, raw_value, unit in specs:
        if variable not in req.variables or raw_value is None:
            continue
        out.append(
            NormalizedSample(
                model=req.model,
                variable=variable,
                issued_at=issued_at,
                valid_at=valid_at,
                lead_hours=lead,
                value=raw_value,
                source_raw=f"{raw_value} {unit}",
                model_run_id=f"{req.model}:{issued_at}",
            )
        )
    return out
