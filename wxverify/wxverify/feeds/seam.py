"""Forecast adapter protocol and typed envelopes."""

from __future__ import annotations

from typing import ClassVar, Protocol

from pydantic import BaseModel, ConfigDict


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
