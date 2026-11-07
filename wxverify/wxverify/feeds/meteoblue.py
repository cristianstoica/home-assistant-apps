"""Meteoblue multimodel adapter."""

from __future__ import annotations

import logging
from typing import Final, cast

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from wxverify.core.timeutil import isoformat_utc, lead_hours, parse_utc
from wxverify.core.units import kmh_to_ms
from wxverify.feeds.parse_cap_probe import emit_parse_cap_probe
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastRequest,
    GridProvenance,
    NormalizedSample,
    display_float,
    precip_hour_start,
)

VARIABLE_MAP: Final[dict[str, tuple[str, str]]] = {
    "temperature": ("temperature", "C"),
    "wind": ("windspeed", "km/h"),
    "precip": ("precipitation", "mm"),
    # Display only (card 7), from the same multimodel-1h reply; it has no
    # gust key, so meteoblue gives no gust.
    "cloud_cover": ("cloudcover", "%"),
    "wind_direction": ("winddirection", "degree"),
}
METEOBLUE_MULTIMODEL_PACKAGE: Final = "multimodel-1h"

logger = logging.getLogger(__name__)


class MeteoblueMetadata(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    models: list[str]
    modelrun_utc: list[str]
    latitude: float
    longitude: float
    height: float | None = None


def _empty_matrix() -> list[list[float | None]]:
    return []


def _display_matrix(value: object) -> list[list[float | None]]:
    """A display matrix (card 7), read so it never rejects the reply: a
    matrix that is not a list is empty, a row that is not a list is empty,
    and each entry ``float()`` cannot read is ``None``."""
    if not isinstance(value, list):
        return []
    rows: list[list[float | None]] = []
    for row in cast(list[object], value):
        if isinstance(row, list):
            rows.append([display_float(item) for item in cast(list[object], row)])
        else:
            rows.append([])
    return rows


class MeteoblueData1h(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    time: list[str]
    temperature: list[list[float | None]] = Field(default_factory=_empty_matrix)
    windspeed: list[list[float | None]] = Field(default_factory=_empty_matrix)
    precipitation: list[list[float | None]] = Field(default_factory=_empty_matrix)
    cloudcover: list[list[float | None]] = Field(default_factory=_empty_matrix)
    winddirection: list[list[float | None]] = Field(default_factory=_empty_matrix)

    @field_validator("cloudcover", "winddirection", mode="before")
    @classmethod
    def _lenient_display(cls, value: object) -> list[list[float | None]]:
        return _display_matrix(value)


class MeteoblueMultimodel(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    data_1h: MeteoblueData1h


class MeteoblueResponse(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    metadata: MeteoblueMetadata
    multimodel: MeteoblueMultimodel | None = None
    data_1h: MeteoblueData1h | None = None


class MeteoblueAdapter:
    def __init__(
        self, api_key: str, client: httpx.AsyncClient, *, parse_cap_probe: bool = False
    ) -> None:
        self._api_key = api_key
        self._client = client
        self._parse_cap_probe = parse_cap_probe

    def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
        return CostEstimate(calls=1, credits=16000)

    async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
        logger.debug("meteoblue forecast request lat=%s lon=%s", req.lat, req.lon)
        response = await self._client.get(
            f"https://my.meteoblue.com/packages/{METEOBLUE_MULTIMODEL_PACKAGE}",
            params={
                "apikey": self._api_key,
                "lat": req.lat,
                "lon": req.lon,
                "tz": "utc",
                "format": "json",
                "windspeed": "kmh",
                "temperature": "C",
                "precipitationamount": "mm",
                # Degrees, not a compass text, for the display direction.
                "windDirectionUnit": "degree",
            },
            timeout=httpx.Timeout(15.0, connect=5.0),
        )
        response.raise_for_status()
        payload = MeteoblueResponse.model_validate(response.json())
        result = _to_fetch_result(req, payload)
        if self._parse_cap_probe:
            emit_parse_cap_probe(
                source="meteoblue",
                req=req,
                models=tuple(payload.metadata.models),
                reparse=lambda shadow_req: _to_fetch_result(shadow_req, payload),
            )
        logger.debug(
            "meteoblue forecast response status=%s samples=%s",
            response.status_code,
            len(result.samples),
        )
        return result


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
        "cloudcover": data_1h.cloudcover,
        "winddirection": data_1h.winddirection,
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
            for time_index, raw_valid_at in enumerate(times):
                if time_index >= len(values):
                    continue
                raw_value = values[time_index]
                if raw_value is None:
                    continue
                value = _convert_value(variable, raw_value)
                valid_at = (
                    precip_hour_start(raw_valid_at)
                    if variable == "precip"
                    else raw_valid_at
                )
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
