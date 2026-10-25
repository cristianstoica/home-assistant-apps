"""Forecast adapter protocol and typed envelopes."""

from __future__ import annotations

from datetime import timedelta
from typing import ClassVar, Protocol

from pydantic import BaseModel, ConfigDict

from wxverify.core.timeutil import isoformat_utc, parse_utc


class GridProvenance(BaseModel):
    model_config = ConfigDict(frozen=True)

    grid_lat: float
    grid_lon: float
    grid_elevation_m: float | None = None


class NormalizedSample(BaseModel):
    model_config = ConfigDict(frozen=True)

    model: str
    variable: str
    issued_at: str
    valid_at: str
    lead_hours: int
    value: float
    source_raw: str
    model_run_id: str


class FetchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    samples: list[NormalizedSample]
    grid: GridProvenance | None = None


class ForecastRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    lat: float
    lon: float
    model: str
    variables: tuple[str, ...]
    max_lead_hours: int


class CostEstimate(BaseModel):
    model_config = ConfigDict(frozen=True)

    calls: int
    credits: int | None = None


class ForecastAdapter(Protocol):
    supports_historical: ClassVar[bool]

    async def fetch_forecast(self, req: ForecastRequest) -> FetchResult: ...

    def estimate_cost(self, req: ForecastRequest) -> CostEstimate: ...

    async def fetch_historical(
        self, req: ForecastRequest, *, window_start: str, window_end: str
    ) -> FetchResult | None: ...

    def estimate_historical_cost(
        self, req: ForecastRequest, *, window_start: str, window_end: str
    ) -> CostEstimate: ...


def precip_hour_start(hour_end: str) -> str:
    """Canonical label for a rain value a provider files under the END of its hour.

    A stored rain value at ``valid_at = H`` is the rain in the hour that
    starts at H. Used by Open-Meteo and meteoblue only; Google already labels
    rain by the hour's start. Visual Crossing, OpenWeatherMap, WeatherAPI and
    Meteosource do not document which hour their rain covers; their labels
    are stored as received until settled (README, Site Field Notes).
    """
    return isoformat_utc(parse_utc(hour_end) - timedelta(hours=1))
