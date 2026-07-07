"""Meteoblue multimodel adapter."""

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
    GridProvenance,
    NormalizedSample,
)

VARIABLE_MAP: Final[dict[str, tuple[str, str]]] = {
    "temperature": ("temperature", "C"),
    "wind": ("windspeed", "km/h"),
    "precip": ("precipitation", "mm"),
}
METEOBLUE_MULTIMODEL_PACKAGE: Final = "multimodel-1h"


class MeteoblueMetadata(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    models: list[str]
    modelrun_utc: list[str]
    latitude: float
    longitude: float
    height: float | None = None


def _empty_matrix() -> list[list[float | None]]:
    return []


class MeteoblueData1h(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    time: list[str]
    temperature: list[list[float | None]] = Field(default_factory=_empty_matrix)
    windspeed: list[list[float | None]] = Field(default_factory=_empty_matrix)
    precipitation: list[list[float | None]] = Field(default_factory=_empty_matrix)


class MeteoblueMultimodel(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    data_1h: MeteoblueData1h


class MeteoblueResponse(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    metadata: MeteoblueMetadata
    multimodel: MeteoblueMultimodel | None = None
    data_1h: MeteoblueData1h | None = None


class MeteoblueAdapter:
    supports_historical: ClassVar[bool] = False

    def __init__(self, api_key: str, client: httpx.AsyncClient) -> None:
        self._api_key = api_key
        self._client = client

    def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
        return CostEstimate(calls=1, credits=16000)

    async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
        response = await self._client.get(
            f"https://my.meteoblue.com/packages/{METEOBLUE_MULTIMODEL_PACKAGE}",
            params={
                "apikey": self._api_key,
                "lat": req.lat,
                "lon": req.lon,
                "tz": "utc",
                "format": "json",
            },
            timeout=httpx.Timeout(15.0, connect=5.0),
        )
        response.raise_for_status()
        payload = MeteoblueResponse.model_validate(response.json())
        return _to_fetch_result(req, payload)

    async def fetch_historical(
        self, req: ForecastRequest, *, window_start: str, window_end: str
    ) -> FetchResult | None:
        return None


def _to_fetch_result(req: ForecastRequest, payload: MeteoblueResponse) -> FetchResult:
    metadata = payload.metadata
    data_1h = (
        payload.data_1h
        if payload.data_1h is not None
        else None
        if payload.multimodel is None
        else payload.multimodel.data_1h
    )
    if data_1h is None:
        raise ValueError("meteoblue response missing hourly multimodel data")
    if len(metadata.models) != len(metadata.modelrun_utc):
        raise ValueError("meteoblue models/modelrun_utc length mismatch")
    samples: list[NormalizedSample] = []
    times = [isoformat_utc(parse_utc(raw_time)) for raw_time in data_1h.time]
    matrices = {
        "temperature": data_1h.temperature,
        "windspeed": data_1h.windspeed,
        "precipitation": data_1h.precipitation,
    }
    for model_index, model in enumerate(metadata.models):
        issued_at = isoformat_utc(parse_utc(metadata.modelrun_utc[model_index]))
        for variable in req.variables:
            provider_name, unit = VARIABLE_MAP.get(variable, ("", ""))
            if not provider_name:
                continue
            matrix = matrices[provider_name]
            if model_index >= len(matrix):
                continue
            values = matrix[model_index]
            for time_index, valid_at in enumerate(times):
                if time_index >= len(values):
                    continue
                raw_value = values[time_index]
                if raw_value is None:
                    continue
                value = _convert_value(variable, raw_value)
                lead = lead_hours(issued_at, valid_at)
                if lead < 1 or lead > req.max_lead_hours:
                    continue
                samples.append(
                    NormalizedSample(
                        model=model,
                        variable=variable,
                        issued_at=issued_at,
                        valid_at=valid_at,
                        lead_hours=lead,
                        value=value,
                        source_raw=f"{raw_value} {unit}",
                        model_run_id=f"{model}:{issued_at}",
                    )
                )
    return FetchResult(
        samples=samples,
        grid=GridProvenance(
            grid_lat=metadata.latitude,
            grid_lon=metadata.longitude,
            grid_elevation_m=metadata.height,
        ),
    )


def _convert_value(variable: str, raw_value: float) -> float:
    if variable == "wind":
        return kmh_to_ms(raw_value)
    return raw_value
