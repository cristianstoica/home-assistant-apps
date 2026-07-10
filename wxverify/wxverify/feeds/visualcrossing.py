"""Visual Crossing Timeline Weather adapter."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import ClassVar, Final

import httpx
from pydantic import BaseModel, ConfigDict, Field

from wxverify.core.timeutil import isoformat_utc, lead_hours
from wxverify.core.units import kmh_to_ms
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastRequest,
    NormalizedSample,
)
from wxverify.feeds.synthetic_run import snap_run

_ENDPOINT: Final = (
    "https://weather.visualcrossing.com/VisualCrossingWebServices"
    "/rest/services/timeline/{lat},{lon}"
)

logger = logging.getLogger(__name__)


class VisualCrossingHour(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    datetimeEpoch: int
    temp: float | None = None
    windspeed: float | None = None
    precip: float | None = None


def _no_hours() -> list[VisualCrossingHour]:
    return []


class VisualCrossingDay(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    hours: list[VisualCrossingHour] = Field(default_factory=_no_hours)


def _no_days() -> list[VisualCrossingDay]:
    return []


class VisualCrossingResponse(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    days: list[VisualCrossingDay] = Field(default_factory=_no_days)


class VisualCrossingAdapter:
    supports_historical: ClassVar[bool] = False

    def __init__(self, api_key: str, client: httpx.AsyncClient) -> None:
        self._api_key = api_key
        self._client = client

    def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
        return CostEstimate(calls=1)

    async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
        logger.debug("visualcrossing forecast request lat=%s lon=%s", req.lat, req.lon)
        response = await self._client.get(
            _ENDPOINT.format(lat=req.lat, lon=req.lon),
            params={
                "unitGroup": "metric",
                "include": "hours",
                "key": self._api_key,
                "contentType": "json",
            },
            timeout=httpx.Timeout(15.0, connect=5.0),
        )
        response.raise_for_status()
        payload = VisualCrossingResponse.model_validate(response.json())
        result = _to_fetch_result(req, payload)
        logger.debug(
            "visualcrossing forecast response status=%s samples=%s",
            response.status_code,
            len(result.samples),
        )
        return result

    async def fetch_historical(
        self, req: ForecastRequest, *, window_start: str, window_end: str
    ) -> FetchResult | None:
        return None


def _to_fetch_result(
    req: ForecastRequest, payload: VisualCrossingResponse
) -> FetchResult:
    issued_at = snap_run()
    samples: list[NormalizedSample] = []
    for day in payload.days:
        for hour in day.hours:
            valid_at = isoformat_utc(datetime.fromtimestamp(hour.datetimeEpoch, tz=UTC))
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
    hour: VisualCrossingHour,
) -> list[NormalizedSample]:
    # (canonical variable, raw provider value, raw unit, convert km/h -> m/s)
    specs: tuple[tuple[str, float | None, str, bool], ...] = (
        ("temperature", hour.temp, "C", False),
        ("wind", hour.windspeed, "km/h", True),
        ("precip", hour.precip, "mm", False),
    )
    out: list[NormalizedSample] = []
    for variable, raw_value, unit, convert in specs:
        if variable not in req.variables or raw_value is None:
            continue
        value = kmh_to_ms(raw_value) if convert else raw_value
        out.append(
            NormalizedSample(
                model=req.model,
                variable=variable,
                issued_at=issued_at,
                valid_at=valid_at,
                lead_hours=lead,
                value=value,
                source_raw=f"{raw_value} {unit}",
                model_run_id=f"{req.model}:{issued_at}",
            )
        )
    return out
