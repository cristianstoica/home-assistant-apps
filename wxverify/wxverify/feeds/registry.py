"""Adapter registry."""

from __future__ import annotations

import httpx

from wxverify.core.secrets import resolve_secret
from wxverify.feeds.google import GoogleAdapter
from wxverify.feeds.meteoblue import MeteoblueAdapter
from wxverify.feeds.meteosource import MeteosourceAdapter
from wxverify.feeds.open_meteo import OpenMeteoAdapter
from wxverify.feeds.openweathermap import OpenWeatherMapAdapter
from wxverify.feeds.seam import ForecastAdapter
from wxverify.feeds.visualcrossing import VisualCrossingAdapter
from wxverify.feeds.weatherapi import WeatherApiAdapter


def build_adapter(source: str, client: httpx.AsyncClient) -> ForecastAdapter:
    if source == "open-meteo":
        return OpenMeteoAdapter(client)
    if source == "meteoblue":
        return MeteoblueAdapter(_require_key("meteoblue"), client)
    if source == "visualcrossing":
        return VisualCrossingAdapter(_require_key("visualcrossing"), client)
    if source == "openweathermap":
        return OpenWeatherMapAdapter(_require_key("openweathermap"), client)
    if source == "weatherapi":
        return WeatherApiAdapter(_require_key("weatherapi"), client)
    if source == "meteosource":
        return MeteosourceAdapter(_require_key("meteosource"), client)
    if source == "google":
        return GoogleAdapter(_require_key("google"), client)
    raise ValueError(f"no adapter for source {source}")


def _require_key(provider: str) -> str:
    key = resolve_secret(provider)
    if not key:
        raise RuntimeError(f"{provider} key is not configured")
    return key
