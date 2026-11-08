"""Forecast adapter protocol and typed envelopes."""

from __future__ import annotations

from datetime import timedelta
from typing import Annotated, Protocol, cast

from pydantic import BaseModel, BeforeValidator, ConfigDict

from wxverify.core.timeutil import isoformat_utc, parse_utc


def display_float(value: object) -> float | None:
    """A display field's value (card 7) as a float, or ``None``.

    Anything ``float()`` cannot read (text such as ``"N/A"``, a list, an
    object, an integer too large for a float) is ``None``, so it stores
    nothing. Never raises: a bad display field must never fail a reply's
    validation and cost its scored values.
    """
    if not isinstance(value, int | float | str):
        return None
    try:
        return float(value)
    except (ValueError, OverflowError):
        return None


def display_mapping(value: object) -> object:
    """A display container (card 7) as given when it is an object, else
    ``None``, so a malformed container never fails a reply's validation."""
    if isinstance(value, dict | BaseModel):
        return cast(object, value)
    return None


#: Every display leaf of every adapter model: ``float | None``, defaulting
#: to ``None``, read leniently by :func:`display_float`.
DisplayFloat = Annotated[float | None, BeforeValidator(display_float)]


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


class ModelRun(BaseModel):
    """One model run a reply carried, for the run log (card 27).

    ``run_at`` is the canonical UTC stamp the model's samples carry as
    ``issued_at``. ``updated`` is a log token: a canonical UTC stamp (the
    provider's own update time for the run), ``none`` (the reply has no
    update time for the model), or ``unreadable-<kind>`` with kind one of
    ``text``, ``number``, ``boolean``, ``array``, ``object`` (the field is
    there but cannot be read; the value itself is never kept).
    """

    model_config = ConfigDict(frozen=True)

    model: str
    run_at: str
    updated: str


class FetchResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    samples: list[NormalizedSample]
    grid: GridProvenance | None = None
    # Filled by the meteoblue adapter only; every other adapter leaves it
    # empty.
    runs: tuple[ModelRun, ...] = ()


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
    async def fetch_forecast(self, req: ForecastRequest) -> FetchResult: ...

    def estimate_cost(self, req: ForecastRequest) -> CostEstimate: ...


def precip_hour_start(hour_end: str) -> str:
    """Canonical label for a rain value a provider files under the END of its hour.

    A stored rain value at ``valid_at = H`` is the rain in the hour that
    starts at H. Used by Open-Meteo and meteoblue only; Google already labels
    rain by the hour's start. Visual Crossing, OpenWeatherMap, WeatherAPI and
    Meteosource do not document which hour their rain covers; their labels
    are stored as received until settled (README, Site Field Notes).
    """
    return isoformat_utc(parse_utc(hour_end) - timedelta(hours=1))


class ProviderAnswerRefused(RuntimeError):
    """A provider answered, but the answer cannot be used. Recorded once; not
    retried before the feed's next scheduled try."""
