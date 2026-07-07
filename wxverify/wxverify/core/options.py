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
    obs_interval_minutes: int | None = Field(default=None, ge=30, le=1440)
    obs_jitter_minutes: int | None = Field(default=None, ge=0, le=120)


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


def _from_env() -> RuntimeConfig:
    return RuntimeConfig(
        secrets={
            provider: _blank_to_none(os.environ.get(env_name))
            for provider, env_name in SECRET_ENV.items()
        },
        options=RuntimeOptions(
            rolling_window_days=_env_int("WXV_ROLLING_WINDOW_DAYS"),
            min_n=_env_int("WXV_MIN_N"),
            obs_interval_minutes=_env_int("WXV_OBS_INTERVAL_MINUTES"),
            obs_jitter_minutes=_env_int("WXV_OBS_JITTER_MINUTES"),
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
            obs_interval_minutes=options.get("obs_interval_minutes"),
            obs_jitter_minutes=options.get("obs_jitter_minutes"),
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
