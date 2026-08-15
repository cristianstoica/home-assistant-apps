"""Google Weather API hourly-forecast adapter.

Cost model: the request horizon is derived from ``req.max_lead_hours``,
clamped at ``GOOGLE_MAX_HOURS``; ``pageSize`` is pinned to
``GOOGLE_PAGE_SIZE`` records per page, so one fetch costs
``ceil(hours / GOOGLE_PAGE_SIZE)`` provider calls. ``estimate_cost`` and the
page loop both take that number from ``_expected_pages``, so the reservation
and the spend cannot diverge, and the loop is bounded by the reservation
rather than by the presence of a ``nextPageToken``.

The accumulated page sequence is rejected with ``GooglePageSequenceError``
unless it is provably whole -- every page non-empty, a token on every page
before the last expected one, no echoed token, strictly hourly
``startTime`` values, and exactly the requested record count. Nothing is
persisted from a partial sequence: this module accumulates in memory and
never touches the database.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from typing import ClassVar, Final

import httpx
from pydantic import BaseModel, ConfigDict, Field

from wxverify.core.timeutil import isoformat_utc, lead_hours, parse_utc
from wxverify.core.units import kmh_to_ms
from wxverify.feeds.seam import (
    CostEstimate,
    FetchResult,
    ForecastRequest,
    NormalizedSample,
)
from wxverify.feeds.synthetic_run import snap_run

_ENDPOINT: Final = "https://weather.googleapis.com/v1/forecast/hours:lookup"

# Google self-describes each value's unit; a non-metric unit is a hard error.
_EXPECTED_TEMPERATURE_UNIT: Final = "CELSIUS"
_EXPECTED_SPEED_UNIT: Final = "KILOMETERS_PER_HOUR"
_EXPECTED_PRECIP_UNIT: Final = "MILLIMETERS"

GOOGLE_MAX_HOURS: Final = 240  # provider cap on `hours`
GOOGLE_PAGE_SIZE: Final = 24  # provider cap on `pageSize`

_ONE_HOUR: Final = timedelta(hours=1)

# Page >= 2 connect failures are re-raised as GooglePageSequenceError so the
# reservation is NOT refunded: earlier pages genuinely reached the provider.
_PAGE_CONNECT_ERRORS: Final = (httpx.ConnectError, httpx.ConnectTimeout)

logger = logging.getLogger(__name__)


class GooglePageSequenceError(RuntimeError):
    """The page sequence is not provably whole, so nothing may be persisted."""


def _requested_hours(req: ForecastRequest) -> int:
    """The horizon to request, clamped at the provider's own ``hours`` cap."""
    hours = min(req.max_lead_hours, GOOGLE_MAX_HOURS)
    if hours < 1:
        raise ValueError(f"google: unusable max_lead_hours {req.max_lead_hours!r}")
    return hours


def _expected_pages(req: ForecastRequest) -> int:
    """Pages one fetch costs -- the reservation AND the loop bound."""
    return math.ceil(_requested_hours(req) / GOOGLE_PAGE_SIZE)


class GoogleInterval(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    startTime: str


class GoogleTemperature(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    degrees: float
    unit: str


class GoogleSpeed(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    value: float
    unit: str


class GoogleWind(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    speed: GoogleSpeed


class GoogleQpf(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    quantity: float
    unit: str


class GooglePrecipitation(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    qpf: GoogleQpf


class GoogleForecastHour(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    interval: GoogleInterval
    temperature: GoogleTemperature | None = None
    wind: GoogleWind | None = None
    precipitation: GooglePrecipitation | None = None


def _no_hours() -> list[GoogleForecastHour]:
    return []


class GoogleResponse(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    forecastHours: list[GoogleForecastHour] = Field(default_factory=_no_hours)
    nextPageToken: str | None = None


class GoogleAdapter:
    supports_historical: ClassVar[bool] = False

    def __init__(self, api_key: str, client: httpx.AsyncClient) -> None:
        self._api_key = api_key
        self._client = client

    def estimate_cost(self, req: ForecastRequest) -> CostEstimate:
        return CostEstimate(calls=_expected_pages(req))

    async def fetch_forecast(self, req: ForecastRequest) -> FetchResult:
        hours = _requested_hours(req)
        pages = _expected_pages(req)
        logger.debug(
            "google forecast request lat=%s lon=%s hours=%s pages=%s",
            req.lat,
            req.lon,
            hours,
            pages,
        )
        # Snapped once, before page 1: the whole sequence is attributed to
        # the run current when collection began, never to whatever the clock
        # reads after several sequential round-trips.
        issued_at = snap_run()
        token: str | None = None
        collected: list[GoogleForecastHour] = []
        for page_index in range(pages):
            params: dict[str, str | int | float] = {
                "key": self._api_key,
                "location.latitude": req.lat,
                "location.longitude": req.lon,
                "hours": hours,
                "pageSize": GOOGLE_PAGE_SIZE,
                "unitsSystem": "METRIC",
            }
            if token is not None:
                params["pageToken"] = token
            try:
                response = await self._client.get(
                    _ENDPOINT,
                    params=params,
                    timeout=httpx.Timeout(15.0, connect=5.0),
                )
            except _PAGE_CONNECT_ERRORS as exc:
                if page_index == 0:
                    raise
                raise GooglePageSequenceError(
                    f"google: page {page_index} failed to connect after "
                    f"{page_index} page(s) reached the provider"
                ) from exc
            response.raise_for_status()
            payload = GoogleResponse.model_validate(response.json())
            _check_page(payload, page_index=page_index, pages=pages, sent_token=token)
            collected.extend(payload.forecastHours)
            token = payload.nextPageToken
        _check_sequence(collected, requested_hours=hours)
        result = _to_fetch_result(req, collected, issued_at)
        logger.debug(
            "google forecast response pages=%s records=%s samples=%s",
            pages,
            len(collected),
            len(result.samples),
        )
        return result

    async def fetch_historical(
        self, req: ForecastRequest, *, window_start: str, window_end: str
    ) -> FetchResult | None:
        return None


def _check_page(
    payload: GoogleResponse, *, page_index: int, pages: int, sent_token: str | None
) -> None:
    """Reject a page that cannot be part of a whole sequence."""
    if not payload.forecastHours:
        raise GooglePageSequenceError(
            f"google: page {page_index} returned an empty forecastHours"
        )
    if page_index < pages - 1 and payload.nextPageToken is None:
        raise GooglePageSequenceError(
            f"google: page {page_index} returned no nextPageToken before the "
            f"requested horizon ({pages} pages expected)"
        )
    if sent_token is not None and payload.nextPageToken == sent_token:
        raise GooglePageSequenceError(
            f"google: page {page_index} echoed the pageToken it was sent"
        )


def _check_sequence(hours: list[GoogleForecastHour], *, requested_hours: int) -> None:
    """Reject an accumulated sequence that is not provably whole.

    Asserted on the RAW records, never on the retained samples: ``hours``
    counts forward from now while ``lead`` is measured from the snapped run,
    so the tail of the requested window is legitimately dropped by the lead
    filter and a retained-count check would fail every healthy fetch.
    """
    previous: datetime | None = None
    for index, hour in enumerate(hours):
        current = parse_utc(hour.interval.startTime)
        if previous is not None and current - previous != _ONE_HOUR:
            raise GooglePageSequenceError(
                f"google: record {index} startTime {hour.interval.startTime!r} "
                f"is not exactly one hour after its predecessor"
            )
        previous = current
    if len(hours) != requested_hours:
        raise GooglePageSequenceError(
            f"google: accumulated {len(hours)} records for a "
            f"{requested_hours}-hour request"
        )


def _to_fetch_result(
    req: ForecastRequest, hours: list[GoogleForecastHour], issued_at: str
) -> FetchResult:
    samples: list[NormalizedSample] = []
    for hour in hours:
        valid_at = isoformat_utc(parse_utc(hour.interval.startTime))
        lead = lead_hours(issued_at, valid_at)
        if lead < 1 or lead > req.max_lead_hours:
            continue
        samples.extend(_hour_samples(req, issued_at, valid_at, lead, hour))
    return FetchResult(samples=samples, grid=None)


def _assert_unit(actual: str, expected: str, field: str) -> None:
    if actual != expected:
        raise ValueError(
            f"google {field} unit {actual!r} is not the expected metric "
            f"unit {expected!r}"
        )


def _sample(
    req: ForecastRequest,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead: int,
    value: float,
    source_raw: str,
) -> NormalizedSample:
    return NormalizedSample(
        model=req.model,
        variable=variable,
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=lead,
        value=value,
        source_raw=source_raw,
        model_run_id=f"{req.model}:{issued_at}",
    )


def _hour_samples(
    req: ForecastRequest,
    issued_at: str,
    valid_at: str,
    lead: int,
    hour: GoogleForecastHour,
) -> list[NormalizedSample]:
    out: list[NormalizedSample] = []
    if "temperature" in req.variables and hour.temperature is not None:
        temperature = hour.temperature
        _assert_unit(temperature.unit, _EXPECTED_TEMPERATURE_UNIT, "temperature")
        out.append(
            _sample(
                req,
                "temperature",
                issued_at,
                valid_at,
                lead,
                temperature.degrees,
                f"{temperature.degrees} {temperature.unit}",
            )
        )
    if "wind" in req.variables and hour.wind is not None:
        speed = hour.wind.speed
        _assert_unit(speed.unit, _EXPECTED_SPEED_UNIT, "wind")
        out.append(
            _sample(
                req,
                "wind",
                issued_at,
                valid_at,
                lead,
                kmh_to_ms(speed.value),
                f"{speed.value} {speed.unit}",
            )
        )
    if "precip" in req.variables and hour.precipitation is not None:
        qpf = hour.precipitation.qpf
        _assert_unit(qpf.unit, _EXPECTED_PRECIP_UNIT, "precipitation")
        out.append(
            _sample(
                req,
                "precip",
                issued_at,
                valid_at,
                lead,
                qpf.quantity,
                f"{qpf.quantity} {qpf.unit}",
            )
        )
    return out
