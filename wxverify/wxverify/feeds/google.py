"""Google Weather API hourly-forecast adapter.

Single page only: ``hours=24`` with ``pageSize`` left at its default 24, and
``nextPageToken`` is never followed, so ``estimate_cost`` stays one call.
"""

from __future__ import annotations

from typing import ClassVar, Final

import httpx
from pydantic import BaseModel, ConfigDict, Field

from wxverify.core.timeutil import isoformat_utc, lead_hours, parse_utc
from wxverify.core.units import kmh_to_ms
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastRequest,
    NormalizedSample,
)
from wxverify.feeds.synthetic_run import snap_run

_ENDPOINT: Final = "https://weather.googleapis.com/v1/forecast/hours:lookup"

# Google self-describes each value's unit; a non-metric unit is a hard error.
_EXPECTED_TEMPERATURE_UNIT: Final = "CELSIUS"
_EXPECTED_SPEED_UNIT: Final = "KILOMETERS_PER_HOUR"
_EXPECTED_PRECIP_UNIT: Final = "MILLIMETERS"


class GoogleInterval(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    startTime: str


class GoogleTemperature(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    degrees: float
    unit: str


class GoogleSpeed(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    value: float
    unit: str


class GoogleWind(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    speed: GoogleSpeed


class GoogleQpf(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    quantity: float
    unit: str


class GooglePrecipitation(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    qpf: GoogleQpf


class GoogleForecastHour(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    interval: GoogleInterval
    temperature: GoogleTemperature | None = None
    wind: GoogleWind | None = None
    precipitation: GooglePrecipitation | None = None


def _no_hours() -> list[GoogleForecastHour]:
    return []


class GoogleResponse(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    forecastHours: list[GoogleForecastHour] = Field(default_factory=_no_hours)


class GoogleAdapter:
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
                "key": self._api_key,
                "location.latitude": req.lat,
                "location.longitude": req.lon,
                "hours": 24,
                "unitsSystem": "METRIC",
            },
            timeout=httpx.Timeout(15.0, connect=5.0),
        )
        response.raise_for_status()
        payload = GoogleResponse.model_validate(response.json())
        return _to_fetch_result(req, payload)

    async def fetch_historical(
        self, req: ForecastRequest, *, window_start: str, window_end: str
    ) -> FetchResult | None:
        return None


def _to_fetch_result(req: ForecastRequest, payload: GoogleResponse) -> FetchResult:
    issued_at = snap_run()
    samples: list[NormalizedSample] = []
    for hour in payload.forecastHours:
        valid_at = isoformat_utc(parse_utc(hour.interval.startTime))
        lead = lead_hours(issued_at, valid_at)
        if lead < 1 or lead > req.max_lead_hours:
            continue
        samples.extend(_hour_samples(req, issued_at, valid_at, lead, hour))
    return FetchResult(samples=samples, grid=None)


def _assert_unit(actual: str, expected: str, field: str) -> None:
    if actual != expected:
        raise ValueError(
            f"google {field} unit {actual!r} is not the expected metric "
            f"unit {expected!r}"
        )


def _sample(
    req: ForecastRequest,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead: int,
    value: float,
    source_raw: str,
) -> NormalizedSample:
    return NormalizedSample(
        model=req.model,
        variable=variable,
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=lead,
        value=value,
        source_raw=source_raw,
        model_run_id=f"{req.model}:{issued_at}",
    )


def _hour_samples(
    req: ForecastRequest,
    issued_at: str,
    valid_at: str,
    lead: int,
    hour: GoogleForecastHour,
) -> list[NormalizedSample]:
    out: list[NormalizedSample] = []
    if "temperature" in req.variables and hour.temperature is not None:
        temperature = hour.temperature
        _assert_unit(temperature.unit, _EXPECTED_TEMPERATURE_UNIT, "temperature")
        out.append(
            _sample(
                req,
                "temperature",
                issued_at,
                valid_at,
                lead,
                temperature.degrees,
                f"{temperature.degrees} {temperature.unit}",
            )
        )
    if "wind" in req.variables and hour.wind is not None:
        speed = hour.wind.speed
        _assert_unit(speed.unit, _EXPECTED_SPEED_UNIT, "wind")
        out.append(
            _sample(
                req,
                "wind",
                issued_at,
                valid_at,
                lead,
                kmh_to_ms(speed.value),
                f"{speed.value} {speed.unit}",
            )
        )
    if "precip" in req.variables and hour.precipitation is not None:
        qpf = hour.precipitation.qpf
        _assert_unit(qpf.unit, _EXPECTED_PRECIP_UNIT, "precipitation")
        out.append(
            _sample(
                req,
                "precip",
                issued_at,
                valid_at,
                lead,
                qpf.quantity,
                f"{qpf.quantity} {qpf.unit}",
            )
        )
    return out
