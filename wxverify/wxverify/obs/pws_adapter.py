"""Weather.com PWS adapter seam."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
import weakref
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final, Literal, cast
from zoneinfo import ZoneInfo

import httpx

from wxverify.core.timeutil import floor_hour, isoformat_utc, parse_utc, utc_now
from wxverify.obs.config import RECENT_REFRESH_HOURS
from wxverify.obs.hourly_values import Quantity, Source, StationReading

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PwsStation:
    station_id: str
    lat: float
    lon: float
    neighborhood: str | None = None


@dataclass(frozen=True)
class PwsObservation:
    variable: str
    valid_at: str
    value: float
    source_raw: str


@dataclass(frozen=True)
class _ParsedObservation:
    index: int
    valid_at: str
    local_day: str
    metric: dict[str, object]


@dataclass(frozen=True)
class CurrentObservation:
    """A single current-observation snapshot mapped to ``station_current_obs``.

    Every measurement is optional: a missing or uncoercible upstream field maps
    to ``None`` (a NULL column), never a fabricated zero. ``obs_time_utc`` is the
    full-precision ISO-``Z`` instant from ``_obs_instant`` (never hour-floored),
    or ``None`` when the payload's timestamp is missing/unparseable.
    ``local_date`` is the reading's own local date from ``obsTimeLocal``, or
    ``None`` when the reply carries none.

    The ``reading_*`` fields are the raw store's values (plan 5.5), read from
    the reply's own ``metric.temp``, ``metric.precipTotal`` and
    ``metric.windSpeed`` through ``reading_value``: ``None`` when absent or
    rejected. ``reading_unusable`` names the quantities whose value was
    present but rejected (``null`` included). The snapshot fields above are
    unaffected by them.
    """

    obs_time_utc: str | None
    temp: float | None
    humidity: float | None
    dewpt: float | None
    wind_speed: float | None
    wind_gust: float | None
    wind_dir: float | None
    pressure: float | None
    precip_rate: float | None
    precip_total: float | None
    uv: float | None
    neighborhood: str | None
    local_date: date | None = None
    reading_temp_c: float | None = None
    reading_precip_total_mm: float | None = None
    reading_speed_kmh: float | None = None
    reading_unusable: tuple[Quantity, ...] = ()


@dataclass(frozen=True)
class ParsedReadings:
    """The readings of one payload, from ``readings_from_payload``.

    ``readings`` are sorted by ``obs_at``; of two equal instants the first in
    the payload is kept. ``dropped`` counts the items left out: no instant, no
    valid value, or a repeat instant. ``unusable`` counts, per quantity, the
    values present but rejected by ``reading_value`` (``null`` included) over
    the items that have an instant; every quantity has a key, zero included.
    """

    readings: tuple[StationReading, ...]
    dropped: int
    unusable: dict[Quantity, int]


PayloadErrorKind = Literal[
    "no_content", "json_decode", "invalid_structure", "provider_error"
]
BodyKind = Literal["empty", "whitespace", "content"]
REQUEST_ID_HEADERS: tuple[str, ...] = ("x-request-id",)
PROVIDER_ERROR_KEYS = frozenset({"errors", "error"})
_CONTENT_TYPE_RE = re.compile(r"^[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+$")
_CONTENT_TYPE_MAX_LEN = 64
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:/+-]{1,128}$")

HOURLY_HISTORY_URL: Final = "https://api.weather.com/v2/pws/observations/hourly/7day"
HOURLY_HISTORY_PATH: Final = "/v2/pws/observations/hourly/7day"
ALL_1DAY_PATH: Final = "/v2/pws/observations/all/1day"
_WEATHERCOM_BASE: Final = "https://api.weather.com"


@dataclass(frozen=True)
class PayloadDiagnostics:
    """Bounded facts about a 2xx response that did not yield ``observations``.

    Every field is an enum, an int, the request path, or an allowlisted token:
    no byte of the body, no query string and no unlisted header is captured,
    so ``render()`` is safe to persist and to log as-is. ``reason``/``pos`` are
    populated only for ``json_decode``.
    """

    kind: PayloadErrorKind
    station_id: str | None
    endpoint: str
    status: int
    content_type: str
    body: BodyKind
    body_bytes: int
    elapsed_ms: int | None
    request_id: str | None
    reason: str | None
    pos: int | None

    def render(self) -> str:
        """One line of fixed-order ``key=value`` pairs; ``-`` marks an absent value."""
        station = "-" if self.station_id is None else self.station_id
        elapsed = "-" if self.elapsed_ms is None else str(self.elapsed_ms)
        request_id = "-" if self.request_id is None else self.request_id
        line = (
            f"upstream payload error kind={self.kind} station={station} "
            f"endpoint={self.endpoint} status={self.status} "
            f"content_type={self.content_type} body={self.body} "
            f"body_bytes={self.body_bytes} elapsed_ms={elapsed} "
            f"request_id={request_id}"
        )
        if self.reason is not None:
            line = f'{line} reason="{self.reason}"'
        if self.pos is not None:
            line = f"{line} pos={self.pos}"
        return line


class UpstreamPayloadError(Exception):
    """A 2xx PWS response whose body is not ``{"observations": [...]}``.

    ``str(exc)`` is ``diagnostics.render()``. Deliberately not a ``ValueError``
    subclass, so no ``except ValueError`` around a parse can swallow it.
    """

    def __init__(self, diagnostics: PayloadDiagnostics) -> None:
        super().__init__(diagnostics.render())
        self.diagnostics = diagnostics


def is_hourly_history_no_content(exc: BaseException) -> bool:
    """True only for a 204 from the 7-day hourly-history endpoint.

    An allowlist: the type, the diagnostic kind and the endpoint must all
    match. Every other payload failure -- json_decode, invalid_structure,
    provider_error -- and every 204 from another endpoint returns False and
    keeps its existing path.
    """
    return (
        isinstance(exc, UpstreamPayloadError)
        and exc.diagnostics.kind == "no_content"
        and exc.diagnostics.endpoint == HOURLY_HISTORY_PATH
    )


def is_wind_history_no_content(exc: BaseException) -> bool:
    """True only for a 204 from today's 5-minute endpoint (``ALL_1DAY_PATH``).

    The same allowlist shape as ``is_hourly_history_no_content``: the type,
    the ``no_content`` kind and the endpoint must all match.
    """
    return (
        isinstance(exc, UpstreamPayloadError)
        and exc.diagnostics.kind == "no_content"
        and exc.diagnostics.endpoint == ALL_1DAY_PATH
    )


def decode_observations_payload(
    response: httpx.Response, *, station_id: str | None
) -> dict[str, object]:
    """Decode a 2xx PWS response to its top-level object or raise a typed error.

    The success shape is an allowlist: a JSON object whose ``observations``
    value is a list (possibly empty). A 204 (``no_content``), a body that does
    not decode (``json_decode``), any other document shape
    (``invalid_structure``) or an error envelope without ``observations``
    (``provider_error``) raises ``UpstreamPayloadError``. The diagnostics are
    built only on the raising path; the success path touches nothing but
    ``status_code`` and ``json()``.
    """
    if response.status_code == 204:
        raise UpstreamPayloadError(_diagnose(response, "no_content", station_id))
    try:
        data: object = response.json()
    except ValueError as exc:
        raise UpstreamPayloadError(
            _diagnose(response, "json_decode", station_id, cause=exc)
        ) from exc
    if not isinstance(data, dict):
        raise UpstreamPayloadError(_diagnose(response, "invalid_structure", station_id))
    payload = cast(dict[str, object], data)
    if "observations" not in payload:
        kind: PayloadErrorKind = (
            "provider_error"
            if PROVIDER_ERROR_KEYS & payload.keys()
            else "invalid_structure"
        )
        raise UpstreamPayloadError(_diagnose(response, kind, station_id))
    if not isinstance(payload["observations"], list):
        raise UpstreamPayloadError(_diagnose(response, "invalid_structure", station_id))
    return payload


def _diagnose(
    response: httpx.Response,
    kind: PayloadErrorKind,
    station_id: str | None,
    *,
    cause: ValueError | None = None,
) -> PayloadDiagnostics:
    content = response.content
    if not content:
        body: BodyKind = "empty"
    elif content.isspace():
        body = "whitespace"
    else:
        body = "content"
    try:
        elapsed_ms: int | None = int(response.elapsed.total_seconds() * 1000)
    except RuntimeError:
        elapsed_ms = None
    reason: str | None = None
    pos: int | None = None
    if isinstance(cause, json.JSONDecodeError):
        reason = cause.msg
        pos = cause.pos
    elif cause is not None:
        reason = type(cause).__name__
    raw_content_type: str | None = response.headers.get("content-type")
    return PayloadDiagnostics(
        kind=kind,
        station_id=station_id,
        endpoint=response.request.url.path,
        status=response.status_code,
        content_type=_content_type(raw_content_type),
        body=body,
        body_bytes=len(content),
        elapsed_ms=elapsed_ms,
        request_id=_request_id(response.headers),
        reason=reason,
        pos=pos,
    )


def _content_type(raw: str | None) -> str:
    if raw is None:
        return "absent"
    media_type = raw.split(";", 1)[0].strip().lower()
    if len(media_type) <= _CONTENT_TYPE_MAX_LEN and _CONTENT_TYPE_RE.fullmatch(
        media_type
    ):
        return media_type
    return "unrecognized"


def _request_id(headers: httpx.Headers) -> str | None:
    for name in REQUEST_ID_HEADERS:
        value: str | None = headers.get(name)
        if value is not None and _REQUEST_ID_RE.fullmatch(value):
            return value
    return None


PROVIDER_DEADLINE_MARGIN_SECONDS: Final = 5.0


class ProviderDeadlineExceeded(TimeoutError):
    """A weather.com call ran past its total deadline (read timeout + margin)."""


PROVIDER_CALLS_PER_MINUTE: Final = 30  # UNVERIFIED provider rate
BACKFILL_CALLS_PER_MINUTE: Final = 20
_RATE_WINDOW_SECONDS: Final = 60.0


class WeathercomRateLimiter:
    """A sliding 60-second window over weather.com requests.

    At most ``PROVIDER_CALLS_PER_MINUTE`` requests in the window, of which at
    most ``BACKFILL_CALLS_PER_MINUTE`` may be backfill. The clock is
    injectable so the window can be driven without sleeping.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._requests: deque[tuple[float, bool]] = deque()

    def _prune(self, now: float) -> None:
        while self._requests and now - self._requests[0][0] >= _RATE_WINDOW_SECONDS:
            self._requests.popleft()

    def _wait_seconds(self, now: float, *, backfill: bool) -> float:
        self._prune(now)
        wait = 0.0
        if len(self._requests) >= PROVIDER_CALLS_PER_MINUTE:
            oldest = self._requests[len(self._requests) - PROVIDER_CALLS_PER_MINUTE]
            wait = max(wait, oldest[0] + _RATE_WINDOW_SECONDS - now)
        if backfill:
            backfill_times = [at for at, tagged in self._requests if tagged]
            if len(backfill_times) >= BACKFILL_CALLS_PER_MINUTE:
                oldest_at = backfill_times[
                    len(backfill_times) - BACKFILL_CALLS_PER_MINUTE
                ]
                wait = max(wait, oldest_at + _RATE_WINDOW_SECONDS - now)
        return wait

    def try_acquire(self, *, backfill: bool) -> float:
        """Record the request and return 0.0 when it fits; otherwise return the
        seconds until it would fit, without recording it."""
        now = self._clock()
        wait = self._wait_seconds(now, backfill=backfill)
        if wait > 0:
            return wait
        self._requests.append((now, backfill))
        return 0.0

    async def acquire(self, *, backfill: bool) -> None:
        """Wait until the request fits, then record it."""
        while (wait := self.try_acquire(backfill=backfill)) > 0:
            await asyncio.sleep(wait)


# One limiter per running event loop, held through a weak reference, as
# ``weathercom_call_lock`` does: a new loop that reuses a dead loop's id gets a
# fresh limiter.
_rate_limiter: WeathercomRateLimiter | None = None
_rate_limiter_loop: weakref.ref[asyncio.AbstractEventLoop] | None = None


def weathercom_rate_limiter() -> WeathercomRateLimiter:
    """The one weather.com rate limiter for the running event loop."""
    global _rate_limiter, _rate_limiter_loop
    loop = asyncio.get_running_loop()
    if (
        _rate_limiter is None
        or _rate_limiter_loop is None
        or _rate_limiter_loop() is not loop
    ):
        _rate_limiter = WeathercomRateLimiter()
        _rate_limiter_loop = weakref.ref(loop)
    return _rate_limiter


async def _get_with_deadline(
    client: httpx.AsyncClient,
    url: str,
    *,
    params: dict[str, str],
    read_seconds: float,
    backfill: bool,
) -> httpx.Response:
    """One weather.com GET, bounded by a total deadline of read timeout + margin.

    httpx timeouts apply per operation, so a body that delivers each chunk before
    the read timeout expires can keep the call open indefinitely. Only this
    deadline's own expiry is converted to ``ProviderDeadlineExceeded``; a
    transport ``TimeoutError`` re-raises unchanged and an external cancel stays
    ``CancelledError``. The message carries no URL, key or station id.

    Every weather.com request passes the process rate limiter first, tagged
    ``backfill`` or not; the limiter wait is outside the deadline.
    """
    await weathercom_rate_limiter().acquire(backfill=backfill)
    deadline = read_seconds + PROVIDER_DEADLINE_MARGIN_SECONDS
    cm = asyncio.timeout(deadline)
    try:
        async with cm:
            return await client.get(
                url, params=params, timeout=httpx.Timeout(read_seconds, connect=5.0)
            )
    except TimeoutError as exc:
        if cm.expired():
            raise ProviderDeadlineExceeded(
                f"weather.com call exceeded its {deadline:.0f} s deadline"
            ) from exc
        raise


async def validate_station(
    station_id: str, api_key: str, *, lat: float | None = None, lon: float | None = None
) -> PwsStation:
    # Tests and local dry runs can pass explicit coordinates and still exercise
    # the station-create contract without reaching weather.com.
    if lat is not None and lon is not None:
        return PwsStation(station_id=station_id, lat=lat, lon=lon)
    logger.debug("pws validate_station station=%s", station_id)
    async with httpx.AsyncClient() as client:
        response = await _get_with_deadline(
            client,
            "https://api.weather.com/v2/pws/observations/current",
            params={
                "stationId": station_id,
                "format": "json",
                "units": "m",
                "apiKey": api_key,
            },
            read_seconds=10.0,
            backfill=False,
        )
        response.raise_for_status()
        data = decode_observations_payload(response, station_id=station_id)
    observations_obj = data.get("observations")
    if not isinstance(observations_obj, list) or not observations_obj:
        raise RuntimeError("station returned no current observation")
    observations = cast(list[object], observations_obj)
    first = observations[0]
    if not isinstance(first, dict):
        raise RuntimeError("invalid station response")
    first_map = cast(dict[str, Any], first)
    return PwsStation(
        station_id=station_id,
        lat=float(first_map["lat"]),
        lon=float(first_map["lon"]),
        neighborhood=str(first_map.get("neighborhood"))
        if first_map.get("neighborhood") is not None
        else None,
    )


async def fetch_hourly_history_payload(
    station_id: str, api_key: str, *, client: httpx.AsyncClient
) -> dict[str, object]:
    """One station's decoded 7-day hourly payload, nothing dropped by age.

    The one request of the 7-day hourly product; ``fetch_hourly_history``
    applies its age cutoff on top. Raises ``httpx.HTTPStatusError`` on a
    non-2xx and ``UpstreamPayloadError`` on a 2xx without ``observations`` (a
    204 included).
    """
    response = await _get_with_deadline(
        client,
        HOURLY_HISTORY_URL,
        params={
            "stationId": station_id,
            "format": "json",
            "units": "m",
            "numericPrecision": "decimal",
            "apiKey": api_key,
        },
        read_seconds=10.0,
        backfill=False,
    )
    response.raise_for_status()
    return decode_observations_payload(response, station_id=station_id)


async def fetch_hourly_history(
    station_id: str,
    api_key: str,
    *,
    hours: int = RECENT_REFRESH_HOURS,
    timezone: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> list[PwsObservation]:
    if client is None:
        async with httpx.AsyncClient() as owned_client:
            return await fetch_hourly_history(
                station_id,
                api_key,
                hours=hours,
                timezone=timezone,
                client=owned_client,
            )
    logger.debug("pws hourly_history request station=%s hours=%s", station_id, hours)
    payload = await fetch_hourly_history_payload(station_id, api_key, client=client)
    cutoff = utc_now() - timedelta(hours=hours)
    observations = [
        observation
        for observation in observations_from_payload(payload, timezone=timezone)
        if parse_utc(observation.valid_at) >= cutoff
    ]
    logger.debug(
        "pws hourly_history response station=%s samples=%s",
        station_id,
        len(observations),
    )
    return observations


async def fetch_all_1day(
    station_id: str,
    api_key: str,
    *,
    client: httpx.AsyncClient,
    backfill: bool,
) -> ParsedReadings:
    """Today's raw readings since the station's local midnight, from
    ``/v2/pws/observations/all/1day``, parsed by ``readings_from_payload``
    with source ``five_minute``: every quantity, not only wind.

    Raises ``httpx.HTTPStatusError`` on a non-2xx and ``UpstreamPayloadError``
    on a 2xx without ``observations`` (a 204 included).
    """
    logger.debug("pws all_1day request station=%s", station_id)
    response = await _get_with_deadline(
        client,
        f"{_WEATHERCOM_BASE}{ALL_1DAY_PATH}",
        params={
            "stationId": station_id,
            "format": "json",
            "units": "m",
            "numericPrecision": "decimal",
            "apiKey": api_key,
        },
        read_seconds=20.0,
        backfill=backfill,
    )
    response.raise_for_status()
    parsed = readings_from_payload(
        decode_observations_payload(response, station_id=station_id),
        source="five_minute",
    )
    logger.debug(
        "pws all_1day response station=%s records=%s",
        station_id,
        len(parsed.readings),
    )
    return parsed


def _wind_record_instant(row: dict[str, object]) -> datetime | None:
    raw_epoch = row.get("valid_time_gmt")
    if raw_epoch is None:
        raw_epoch = row.get("epoch")
    if isinstance(raw_epoch, bool):
        return None
    try:
        parsed = _obs_datetime(row)
    except (ValueError, OverflowError, OSError):
        return None
    if parsed is None:
        return None
    return parsed.replace(microsecond=0)


def reading_value(value: object, quantity: Quantity) -> float | None:
    """One raw reading value checked on its own, else ``None``.

    Accepts a real number or a numeric string that is finite; for ``precip``
    and ``wind`` the value must also be at least 0. A bool, ``nan``, an
    infinity, a non-numeric string, ``None`` or any other type gives ``None``.
    The one check behind ``readings_from_payload`` and the poll's storage.
    """
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return None
    try:
        number = float(value)
    except (ValueError, OverflowError):
        return None
    if not math.isfinite(number):
        return None
    if quantity != "temperature" and number < 0:
        return None
    return number


def _checked_metric(
    metric: dict[str, object],
    quantity: Quantity,
    keys: tuple[str, ...],
    unusable: dict[Quantity, int],
) -> float | None:
    """The first of ``keys`` whose value passes ``reading_value``, else ``None``.

    An absent key is skipped. When no key gives a value but one is present,
    the quantity's count in ``unusable`` goes up by one.
    """
    present = False
    for key in keys:
        if key not in metric:
            continue
        value = reading_value(metric[key], quantity)
        if value is not None:
            return value
        present = True
    if present:
        unusable[quantity] += 1
    return None


def _reading_local_date(row: dict[str, object], *keys: str) -> date | None:
    """The first valid date in ``keys`` of the item, else ``None``.

    The same date match as ``_local_day_from_payload``; never derived from the
    UTC instant.
    """
    for key in keys:
        value = row.get(key)
        if not isinstance(value, str):
            continue
        match = re.search(r"(\d{4})-?(\d{2})-?(\d{2})", value)
        if match is None:
            continue
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            continue
    return None


def readings_from_payload(payload: object, *, source: Source) -> ParsedReadings:
    """The raw readings of a decoded observations payload, an allowlist.

    Each item is read on its own; a bad item drops only itself and nothing
    raises. The instant is ``_wind_record_instant``'s, whole seconds; the
    local date is ``obsTimeLocal``'s, then ``validTimeLocal``'s, else ``None``.
    Temperature is ``metric.tempAvg``, then ``metric.temp``, never
    ``tempHigh`` or ``tempLow``; rain is ``metric.precipTotal``, the running
    total; wind is ``metric.windspeedAvg``. A present value that
    ``reading_value`` rejects is ``None`` and counted in ``unusable``; an
    absent field is ``None`` and not counted. Either leaves the reading's other
    values; a reading with no valid value is dropped. Sorted by ``obs_at``,
    keeping the first of each duplicate instant.
    """
    unusable: dict[Quantity, int] = {"temperature": 0, "precip": 0, "wind": 0}
    if not isinstance(payload, dict):
        return ParsedReadings(readings=(), dropped=0, unusable=unusable)
    observations_obj = cast(dict[str, object], payload).get("observations")
    if not isinstance(observations_obj, list):
        return ParsedReadings(readings=(), dropped=0, unusable=unusable)
    dropped = 0
    parsed: list[StationReading] = []
    for item in cast(list[object], observations_obj):
        if not isinstance(item, dict):
            dropped += 1
            continue
        row = cast(dict[str, object], item)
        obs_at = _wind_record_instant(row)
        if obs_at is None:
            dropped += 1
            continue
        metric_obj = row.get("metric")
        metric: dict[str, object] = (
            cast(dict[str, object], metric_obj) if isinstance(metric_obj, dict) else {}
        )
        temp_c = _checked_metric(metric, "temperature", ("tempAvg", "temp"), unusable)
        precip_total_mm = _checked_metric(metric, "precip", ("precipTotal",), unusable)
        speed_kmh = _checked_metric(metric, "wind", ("windspeedAvg",), unusable)
        if temp_c is None and precip_total_mm is None and speed_kmh is None:
            dropped += 1
            continue
        parsed.append(
            StationReading(
                obs_at=obs_at,
                source=source,
                local_date=_reading_local_date(row, "obsTimeLocal", "validTimeLocal"),
                temp_c=temp_c,
                precip_total_mm=precip_total_mm,
                speed_kmh=speed_kmh,
            )
        )
    parsed.sort(key=lambda reading: reading.obs_at)
    readings: list[StationReading] = []
    for reading in parsed:
        if readings and readings[-1].obs_at == reading.obs_at:
            dropped += 1
            continue
        readings.append(reading)
    return ParsedReadings(readings=tuple(readings), dropped=dropped, unusable=unusable)


def observations_from_payload(
    data: object, *, timezone: str | None = None
) -> list[PwsObservation]:
    if not isinstance(data, dict):
        return []
    payload = cast(dict[str, object], data)
    observations_obj = payload.get("observations")
    if not isinstance(observations_obj, list):
        return []
    observations = cast(list[object], observations_obj)
    parsed_rows: list[_ParsedObservation] = []
    for idx, item in enumerate(observations):
        if not isinstance(item, dict):
            continue
        row = cast(dict[str, object], item)
        valid_at = _valid_at(row)
        if valid_at is None:
            continue
        metric_obj = row.get("metric")
        if not isinstance(metric_obj, dict):
            continue
        parsed_rows.append(
            _ParsedObservation(
                index=idx,
                valid_at=valid_at,
                local_day=_local_day(row, valid_at, timezone),
                metric=cast(dict[str, object], metric_obj),
            )
        )
    precip_increments = _precip_increments(parsed_rows)
    out: list[PwsObservation] = []
    for row in parsed_rows:
        metric = row.metric
        temp = _number(metric, "temp", "tempAvg", "tempHigh", "tempLow")
        if temp is not None:
            out.append(
                PwsObservation(
                    variable="temperature",
                    valid_at=row.valid_at,
                    value=temp,
                    source_raw=f"{temp} C",
                )
            )
        precip_increment = precip_increments.get(row.index)
        precip_total = _number(metric, "precipTotal")
        if precip_increment is not None and precip_total is not None:
            out.append(
                PwsObservation(
                    variable="precip",
                    valid_at=row.valid_at,
                    value=precip_increment,
                    source_raw=f"{precip_total} mm precipTotal",
                )
            )
    return out


def _obs_datetime(row: dict[str, object]) -> datetime | None:
    """Tolerant parse of an obs timestamp to a tz-aware UTC ``datetime``, else None.

    The shared normalization behind ``_valid_at`` (hourly stream) and
    ``_obs_instant`` (current stream): prefer a numeric ``epoch``, else normalize
    ``obsTimeUtc`` (strip trailing `` UTC``, space→``T``, append ``Z`` when
    offset-less). The two callers differ ONLY in whether they hour-floor the
    result. This is the battle-tested normalization ``_valid_at`` has always run
    against the live weather.com PWS payload; ``_obs_instant`` reuses it verbatim.
    """
    raw_epoch = row.get("valid_time_gmt")
    if raw_epoch is None:
        raw_epoch = row.get("epoch")
    if isinstance(raw_epoch, int | float):
        return datetime.fromtimestamp(raw_epoch, tz=UTC)
    if isinstance(raw_epoch, str) and raw_epoch.isdigit():
        return datetime.fromtimestamp(int(raw_epoch), tz=UTC)

    raw_time = row.get("obsTimeUtc")
    if raw_time is None:
        raw_time = row.get("validTimeUtc")
    if not isinstance(raw_time, str) or not raw_time:
        return None
    stripped = raw_time.strip()
    if not stripped:
        return None
    if stripped.endswith(" UTC"):
        stripped = f"{stripped[:-4]}Z"
    normalized = stripped.replace(" ", "T")
    suffix = normalized[10:]
    if normalized[-1].isdigit() and "+" not in suffix and "-" not in suffix:
        normalized = f"{normalized}Z"
    try:
        return parse_utc(normalized)
    except ValueError:
        return None


def _valid_at(row: dict[str, object]) -> str | None:
    """Hourly-stream timestamp: shared normalization, then hour-floored ISO-``Z``.

    Behaviour is unchanged from before the ``_obs_datetime`` extraction — the
    7-day hourly path depends on the ``floor_hour`` bucketing.
    """
    parsed = _obs_datetime(row)
    if parsed is None:
        return None
    return isoformat_utc(floor_hour(parsed))


def _obs_instant(row: dict[str, object]) -> str | None:
    """Current-stream timestamp: shared normalization WITHOUT the hour-floor.

    Identical to ``_valid_at`` except the final ``floor_hour`` wrap is omitted, so
    the returned ISO-``Z`` instant keeps full precision — cadence learning needs
    real inter-obs gaps, not hour buckets. Returns ``None`` on missing/unparseable.
    """
    parsed = _obs_datetime(row)
    if parsed is None:
        return None
    return isoformat_utc(parsed)


def current_obs_from_payload(data: object) -> CurrentObservation | None:
    """Map ``observations[0]`` of a ``/observations/current`` payload to columns.

    Reads the top-level ``humidity``, ``winddir``, ``uv``, ``obsTimeUtc``,
    ``obsTimeLocal`` (its date only) and ``neighborhood`` alongside the
    ``metric`` sub-object (``temp``, ``windSpeed``, ``windGust``, ``pressure``,
    ``precipRate``, ``precipTotal``, ``dewpt``). Values
    are km/h / mm / hPa in the station's native ``units:"m"`` form — this is the
    raw display snapshot table, NOT the SI-normalized scoring path, so no unit
    conversion is applied. Returns ``None`` when the payload has no first
    observation row (the caller treats that as OFFLINE). Missing fields → ``None``.
    The raw store's ``reading_*`` values pass ``reading_value`` on the same
    three raw ``metric`` values (a bool, ``nan`` or a negative rain or wind is
    ``None`` there), leaving the snapshot fields as they are.
    """
    if not isinstance(data, dict):
        return None
    payload = cast(dict[str, object], data)
    observations_obj = payload.get("observations")
    if not isinstance(observations_obj, list) or not observations_obj:
        return None
    first = cast(list[object], observations_obj)[0]
    if not isinstance(first, dict):
        return None
    row = cast(dict[str, object], first)
    metric_obj = row.get("metric")
    metric = cast(dict[str, object], metric_obj) if isinstance(metric_obj, dict) else {}
    neighborhood_obj = row.get("neighborhood")
    neighborhood = str(neighborhood_obj) if isinstance(neighborhood_obj, str) else None
    unusable: dict[Quantity, int] = {"temperature": 0, "precip": 0, "wind": 0}
    reading_temp_c = _checked_metric(metric, "temperature", ("temp",), unusable)
    reading_precip = _checked_metric(metric, "precip", ("precipTotal",), unusable)
    reading_speed = _checked_metric(metric, "wind", ("windSpeed",), unusable)
    return CurrentObservation(
        obs_time_utc=_obs_instant(row),
        temp=_number(metric, "temp"),
        humidity=_number(row, "humidity"),
        dewpt=_number(metric, "dewpt"),
        wind_speed=_number(metric, "windSpeed"),
        wind_gust=_number(metric, "windGust"),
        wind_dir=_number(row, "winddir"),
        pressure=_number(metric, "pressure"),
        precip_rate=_number(metric, "precipRate"),
        precip_total=_number(metric, "precipTotal"),
        uv=_number(row, "uv"),
        neighborhood=neighborhood,
        local_date=_reading_local_date(row, "obsTimeLocal"),
        reading_temp_c=reading_temp_c,
        reading_precip_total_mm=reading_precip,
        reading_speed_kmh=reading_speed,
        reading_unusable=tuple(
            quantity for quantity, count in unusable.items() if count
        ),
    )


async def fetch_current_observation(
    pws_station_id: str,
    api_key: str,
    *,
    client: httpx.AsyncClient | None = None,
    timeout_seconds: float = 10.0,
) -> httpx.Response:
    """GET ``/v2/pws/observations/current`` and return the raw response.

    The handler needs status-code granularity (204 vs 429 vs 401 vs 2xx-with-body)
    to drive the poll-state machine, so this seam returns the ``httpx.Response``
    un-parsed and does NOT call ``raise_for_status`` — the caller classifies. A
    transport-level failure propagates as the corresponding ``httpx`` exception.

    ``timeout_seconds`` is the read timeout (the connect timeout stays at 5.0s),
    and the whole call is bounded by ``timeout_seconds`` + 5 s, past which it
    raises ``ProviderDeadlineExceeded``; the caller passes the operator-configured
    ``request_timeout_seconds`` setting, defaulting to the previous 10.0s literal.
    """
    if client is None:
        async with httpx.AsyncClient() as owned_client:
            return await fetch_current_observation(
                pws_station_id,
                api_key,
                client=owned_client,
                timeout_seconds=timeout_seconds,
            )
    logger.debug("pws current_obs request station=%s", pws_station_id)
    return await _get_with_deadline(
        client,
        "https://api.weather.com/v2/pws/observations/current",
        params={
            "stationId": pws_station_id,
            "format": "json",
            "units": "m",
            "numericPrecision": "decimal",
            "apiKey": api_key,
        },
        read_seconds=timeout_seconds,
        backfill=False,
    )


def _number(metric: dict[str, object], *keys: str) -> float | None:
    for key in keys:
        value = metric.get(key)
        if isinstance(value, int | float):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                continue
    return None


def _precip_increments(rows: list[_ParsedObservation]) -> dict[int, float]:
    increments: dict[int, float] = {}
    previous_total: float | None = None
    previous_day: str | None = None
    for row in sorted(rows, key=lambda item: (item.valid_at, item.index)):
        total = _number(row.metric, "precipTotal")
        if total is None:
            continue
        if previous_total is None or previous_day != row.local_day:
            increment = total
        else:
            increment = total - previous_total
            if increment < 0:
                increment = 0.0
        increments[row.index] = increment
        previous_total = total
        previous_day = row.local_day
    return increments


def _local_day(
    row: dict[str, object], valid_at: str, timezone: str | None = None
) -> str:
    local = _local_day_from_payload(row)
    if local is not None:
        return local
    valid = parse_utc(valid_at)
    if timezone is not None:
        return valid.astimezone(ZoneInfo(timezone)).date().isoformat()
    return valid.date().isoformat()


def _local_day_from_payload(row: dict[str, object]) -> str | None:
    for key in ("obsTimeLocal", "validTimeLocal"):
        value = row.get(key)
        if not isinstance(value, str):
            continue
        match = re.search(r"(\d{4})-?(\d{2})-?(\d{2})", value)
        if match is not None:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return None
