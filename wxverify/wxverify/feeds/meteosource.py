"""Meteosource free point-forecast adapter."""

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

_ENDPOINT: Final = "https://www.meteosource.com/api/v1/free/point"


class MeteosourceWind(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    speed: float | None = None


class MeteosourcePrecipitation(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    total: float | None = None


class MeteosourceHour(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    date: str
    temperature: float | None = None
    wind: MeteosourceWind | None = None
    precipitation: MeteosourcePrecipitation | None = None


def _no_hours() -> list[MeteosourceHour]:
    return []


class MeteosourceHourly(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    data: list[MeteosourceHour] = Field(default_factory=_no_hours)


class MeteosourceResponse(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    hourly: MeteosourceHourly = Field(default_factory=MeteosourceHourly)


class MeteosourceAdapter:
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
                "sections": "hourly",
                "units": "metric",
                "key": self._api_key,
            },
            timeout=httpx.Timeout(15.0, connect=5.0),
        )
        response.raise_for_status()
        payload = MeteosourceResponse.model_validate(response.json())
        return _to_fetch_result(req, payload)

    async def fetch_historical(
        self, req: ForecastRequest, *, window_start: str, window_end: str
    ) -> FetchResult | None:
        return None


def _force_utc_iso(value: str) -> str:
    """Parse a Meteosource timestamp and force UTC.

    Meteosource's ``data[].date`` is timezone-naive (no ``Z`` / offset), so the
    parsed value is forced to UTC explicitly rather than being interpreted as
    local time -- otherwise ``valid_at`` and ``lead_hours`` would shift.
    """
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return isoformat_utc(parsed)


def _to_fetch_result(req: ForecastRequest, payload: MeteosourceResponse) -> FetchResult:
    issued_at = snap_run()
    samples: list[NormalizedSample] = []
    for hour in payload.hourly.data:
        valid_at = _force_utc_iso(hour.date)
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
    hour: MeteosourceHour,
) -> list[NormalizedSample]:
    wind_speed = None if hour.wind is None else hour.wind.speed
    precip_total = None if hour.precipitation is None else hour.precipitation.total
    # Meteosource metric wind is already m/s, so no conversion.
    specs: tuple[tuple[str, float | None, str], ...] = (
        ("temperature", hour.temperature, "C"),
        ("wind", wind_speed, "m/s"),
        ("precip", precip_total, "mm"),
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
