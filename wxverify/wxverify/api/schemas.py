"""Pydantic request/response schemas."""

from __future__ import annotations

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SiteCreate(StrictModel):
    name: str
    forecast_lat: float
    forecast_lon: float
    elevation_m: float
    timezone: str
    rain_threshold_mm: float = Field(default=0.2, ge=0)

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("timezone must be a valid IANA timezone") from exc
        return value


class SiteUpdate(StrictModel):
    name: str | None = None
    enabled: bool | None = None
    rain_threshold_mm: float | None = Field(default=None, ge=0)


class StationCreate(StrictModel):
    pws_station_id: str


class StationUpdate(StrictModel):
    enabled: bool


class FeedUpdate(StrictModel):
    enabled: bool | None = None
    fetch_interval_minutes: int | None = Field(default=None, ge=1)
    default_subscribed: bool | None = None
    disabled_reason: str | None = None


class SubscriptionUpdate(StrictModel):
    enabled: bool


class SiteOut(BaseModel):
    id: int
    name: str
    forecast_lat: float
    forecast_lon: float
    elevation_m: float
    timezone: str
    enabled: bool
    rain_threshold_mm: float


class StationOut(BaseModel):
    id: int
    site_id: int
    pws_station_id: str
    lat: float
    lon: float
    dem_elevation_m: float
    enabled: bool


class FeedOut(BaseModel):
    id: int
    source: str
    model: str
    enabled: bool
    disabled_reason: str | None
    default_subscribed: bool
    fetch_interval_minutes: int
    max_lead_hours: int
    is_virtual: bool


class LeaderboardOut(BaseModel):
    feed_id: int
    source: str
    model: str
    n: int
    skill_score: float | None
    badge: int | None
    below_baseline: bool
    confident: bool
    bias: float | None
    mae: float | None
    rmse: float | None
    window_key: str
    window_days: int | None
