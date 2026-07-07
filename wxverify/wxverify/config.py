"""Static wxverify configuration and seed data.

This module deliberately performs no database writes. Migrations import the
plain seed values here and perform insert-only seeding inside their transaction.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final

APP_TITLE: Final = "Weather Verify"
ENV_PREFIX: Final = "WXV_"
DEFAULT_DB_PATH: Final = "/data/wxverify.db"
DEFAULT_OPTIONS_PATH: Final = "/data/options.json"

db_path: str = os.environ.get("WXV_DB_PATH", DEFAULT_DB_PATH)
options_path: str = os.environ.get("WXV_OPTIONS_PATH", DEFAULT_OPTIONS_PATH)
ingress_root_path: str = ""
standalone_origin: str | None = os.environ.get("WXV_STANDALONE_ORIGIN")


@dataclass(frozen=True)
class SourceSeed:
    source: str
    daily_call_limit: int
    daily_credit_limit: int | None
    billing_tz: str


@dataclass(frozen=True)
class FeedSeed:
    source: str
    model: str
    enabled: bool
    disabled_reason: str | None
    default_subscribed: bool
    fetch_interval_minutes: int
    max_lead_hours: int
    is_virtual: bool = False


OPEN_METEO_MODELS: Final[tuple[str, ...]] = (
    "ecmwf_ifs",
    "gfs_global",
    "icon_global",
    "gem_global",
    "meteofrance_arpege_world",
    "jma_gsm",
    "ukmo_global_deterministic_10km",
)

SOURCE_SEEDS: Final[tuple[SourceSeed, ...]] = (
    SourceSeed("open-meteo", 10000, None, "UTC"),
    SourceSeed("meteoblue", 5, 65000, "UTC"),
    SourceSeed("weathercom", 1000, None, "UTC"),
    SourceSeed("visualcrossing", 500, None, "UTC"),
    SourceSeed("openweathermap", 500, None, "UTC"),
    SourceSeed("weatherapi", 1000, None, "UTC"),
    SourceSeed("meteosource", 200, None, "UTC"),
    SourceSeed("google", 100, None, "UTC"),
)

NEW_PROVIDER_MAX_LEAD_HOURS: Final[tuple[tuple[str, int], ...]] = (
    ("visualcrossing", 168),
    ("openweathermap", 48),
    ("weatherapi", 72),
    ("meteosource", 24),
    ("google", 24),
)

FEED_SEEDS: Final[tuple[FeedSeed, ...]] = (
    tuple(
        FeedSeed(
            source="open-meteo",
            model=model,
            enabled=True,
            disabled_reason=None,
            default_subscribed=True,
            fetch_interval_minutes=360,
            max_lead_hours=168,
        )
        for model in OPEN_METEO_MODELS
    )
    + (
        FeedSeed(
            source="meteoblue",
            model="multimodel",
            enabled=True,
            disabled_reason=None,
            default_subscribed=False,
            fetch_interval_minutes=360,
            max_lead_hours=168,
        ),
    )
    + (
        FeedSeed(
            source="virtual",
            model="_persistence",
            enabled=True,
            disabled_reason=None,
            default_subscribed=False,
            fetch_interval_minutes=1440,
            max_lead_hours=168,
            is_virtual=True,
        ),
        FeedSeed(
            source="virtual",
            model="_multimodel_mean",
            enabled=True,
            disabled_reason=None,
            default_subscribed=False,
            fetch_interval_minutes=1440,
            max_lead_hours=168,
            is_virtual=True,
        ),
    )
    + tuple(
        FeedSeed(
            source=source,
            model="blend",
            enabled=True,
            disabled_reason=None,
            default_subscribed=False,
            fetch_interval_minutes=360,
            max_lead_hours=max_lead_hours,
        )
        for source, max_lead_hours in NEW_PROVIDER_MAX_LEAD_HOURS
    )
)


def ensure_parent_dir(path: str) -> None:
    parent = Path(path).expanduser().resolve().parent
    parent.mkdir(parents=True, exist_ok=True)
