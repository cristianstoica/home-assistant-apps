"""Runtime options loaded from HA options.json or localhost environment."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Final, cast

from pydantic import BaseModel, ConfigDict, Field

from wxverify import config

SECRET_ENV: Final[dict[str, str]] = {
    "weathercom": "WXV_WEATHERCOM_KEY",
    "meteoblue": "WXV_METEOBLUE_KEY",
    "visualcrossing": "WXV_VISUALCROSSING_KEY",
    "openweathermap": "WXV_OPENWEATHERMAP_KEY",
    "weatherapi": "WXV_WEATHERAPI_KEY",
    "meteosource": "WXV_METEOSOURCE_KEY",
    "google": "WXV_GOOGLE_KEY",
}


class RuntimeOptions(BaseModel):
    model_config = ConfigDict(frozen=True)

    rolling_window_days: int | None = Field(default=None, ge=1, le=3650)
    min_n: int | None = Field(default=None, ge=0, le=100000)
    forecast_blend_depth: int | None = Field(default=None, ge=1, le=6)
    obs_interval_minutes: int | None = Field(default=None, ge=30, le=1440)
    obs_jitter_minutes: int | None = Field(default=None, ge=0, le=120)
    min_interval_seconds: int | None = Field(default=None, ge=60, le=1800)
    max_backoff_seconds: int | None = Field(default=None, ge=60, le=86400)
    request_timeout_seconds: int | None = Field(default=None, ge=1, le=300)
    # Explicit non-None default: set_source_cap no-ops on daily_call_limit is
    # None, so a None here would leave the seeded 1000 weathercom cap in force on
    # any boot path that omits the key — below the ~2368/day natural total,
    # deferring both weather.com streams (the LD-M8 breach this option prevents).
    weathercom_daily_call_limit: int = Field(default=3000, ge=1, le=20000)
    monitor_pipeline: bool = True
    monitor_budget: bool = True
    monitor_db: bool = True


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    secrets: dict[str, str | None]
    options: RuntimeOptions
    log_level: str | None = None


def _blank_to_none(value: object) -> str | None:
    if isinstance(value, str) and value != "":
        return value
    return None


def _env_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    return int(raw)


def _env_bool(name: str) -> bool | None:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _from_env() -> RuntimeConfig:
    return RuntimeConfig(
        secrets={
            provider: _blank_to_none(os.environ.get(env_name))
            for provider, env_name in SECRET_ENV.items()
        },
        options=RuntimeOptions(
            rolling_window_days=_env_int("WXV_ROLLING_WINDOW_DAYS"),
            min_n=_env_int("WXV_MIN_N"),
            forecast_blend_depth=_env_int("WXV_FORECAST_BLEND_DEPTH"),
            obs_interval_minutes=_env_int("WXV_OBS_INTERVAL_MINUTES"),
            obs_jitter_minutes=_env_int("WXV_OBS_JITTER_MINUTES"),
            min_interval_seconds=_env_int("WXV_MIN_INTERVAL_SECONDS"),
            max_backoff_seconds=_env_int("WXV_MAX_BACKOFF_SECONDS"),
            request_timeout_seconds=_env_int("WXV_REQUEST_TIMEOUT_SECONDS"),
            weathercom_daily_call_limit=_env_int("WXV_WEATHERCOM_DAILY_CALL_LIMIT")
            or 3000,
            monitor_pipeline=_env_bool("WXV_MONITOR_PIPELINE") is not False,
            monitor_budget=_env_bool("WXV_MONITOR_BUDGET") is not False,
            monitor_db=_env_bool("WXV_MONITOR_DB") is not False,
        ),
        log_level=os.environ.get("WXV_LOG_LEVEL"),
    )


def _from_options_json(path: Path) -> RuntimeConfig:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("options.json must contain an object")
    options = cast(dict[str, Any], data)
    return RuntimeConfig(
        secrets={
            "weathercom": _blank_to_none(options.get("weathercom_key")),
            "meteoblue": _blank_to_none(options.get("meteoblue_key")),
            "visualcrossing": _blank_to_none(options.get("visualcrossing_key")),
            "openweathermap": _blank_to_none(options.get("openweathermap_key")),
            "weatherapi": _blank_to_none(options.get("weatherapi_key")),
            "meteosource": _blank_to_none(options.get("meteosource_key")),
            "google": _blank_to_none(options.get("google_key")),
        },
        options=RuntimeOptions(
            rolling_window_days=options.get("rolling_window_days"),
            min_n=options.get("min_n"),
            forecast_blend_depth=options.get("forecast_blend_depth"),
            obs_interval_minutes=options.get("obs_interval_minutes"),
            obs_jitter_minutes=options.get("obs_jitter_minutes"),
            min_interval_seconds=options.get("min_interval_seconds"),
            max_backoff_seconds=options.get("max_backoff_seconds"),
            request_timeout_seconds=options.get("request_timeout_seconds"),
            weathercom_daily_call_limit=options.get("weathercom_daily_call_limit")
            or 3000,
            monitor_pipeline=options.get("monitor_pipeline", True),
            monitor_budget=options.get("monitor_budget", True),
            monitor_db=options.get("monitor_db", True),
        ),
        log_level=_blank_to_none(options.get("log_level")),
    )


def load_runtime_config(path: str | None = None) -> RuntimeConfig:
    options_path = Path(path or config.options_path)
    try:
        return _from_options_json(options_path)
    except FileNotFoundError:
        return _from_env()


def load_runtime_options(path: str | None = None) -> RuntimeOptions:
    return load_runtime_config(path).options
