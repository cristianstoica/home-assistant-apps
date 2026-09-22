"""Weather.com PWS adapter seam."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal, cast
from zoneinfo import ZoneInfo

import httpx

from wxverify.core.timeutil import floor_hour, isoformat_utc, parse_utc, utc_now
from wxverify.core.units import kmh_to_ms
from wxverify.obs.config import RECENT_REFRESH_HOURS

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


async def validate_station(
    station_id: str, api_key: str, *, lat: float | None = None, lon: float | None = None
) -> PwsStation:
    # Tests and local dry runs can pass explicit coordinates and still exercise
    # the station-create contract without reaching weather.com.
    if lat is not None and lon is not None:
        return PwsStation(station_id=station_id, lat=lat, lon=lon)
    logger.debug("pws validate_station station=%s", station_id)
    async with httpx.AsyncClient() as client:
        response = await client.get(
            "https://api.weather.com/v2/pws/observations/current",
            params={
                "stationId": station_id,
                "format": "json",
                "units": "m",
                "apiKey": api_key,
            },
            timeout=httpx.Timeout(10.0, connect=5.0),
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
    response = await client.get(
        HOURLY_HISTORY_URL,
        params={
            "stationId": station_id,
            "format": "json",
            "units": "m",
            "numericPrecision": "decimal",
            "apiKey": api_key,
        },
        timeout=httpx.Timeout(10.0, connect=5.0),
    )
    response.raise_for_status()
    cutoff = utc_now() - timedelta(hours=hours)
    observations = [
        observation
        for observation in observations_from_payload(
            decode_observations_payload(response, station_id=station_id),
            timezone=timezone,
        )
        if parse_utc(observation.valid_at) >= cutoff
    ]
    logger.debug(
        "pws hourly_history response station=%s samples=%s",
        station_id,
        len(observations),
    )
    return observations


async def fetch_hourly_history_range(
    station_id: str,
    api_key: str,
    *,
    window_start: str,
    window_end: str,
    timezone: str | None = None,
    client: httpx.AsyncClient | None = None,
) -> list[PwsObservation]:
    start = parse_utc(window_start)
    end = parse_utc(window_end)
    if end <= start:
        return []
    start_date, end_date = _history_date_range(start, end, timezone)
    if client is None:
        async with httpx.AsyncClient() as owned_client:
            return await fetch_hourly_history_range(
                station_id,
                api_key,
                window_start=window_start,
                window_end=window_end,
                timezone=timezone,
                client=owned_client,
            )
    logger.debug(
        "pws history_range request station=%s window=%s..%s",
        station_id,
        window_start,
        window_end,
    )
    response = await client.get(
        "https://api.weather.com/v2/pws/history/hourly",
        params={
            "stationId": station_id,
            "format": "json",
            "units": "m",
            "startDate": start_date,
            "endDate": end_date,
            "numericPrecision": "decimal",
            "apiKey": api_key,
        },
        timeout=httpx.Timeout(20.0, connect=5.0),
    )
    response.raise_for_status()
    observations = observations_from_payload(
        decode_observations_payload(response, station_id=station_id),
        timezone=timezone,
    )
    filtered = [
        observation
        for observation in observations
        if start <= parse_utc(observation.valid_at) < end
    ]
    logger.debug(
        "pws history_range response station=%s samples=%s", station_id, len(filtered)
    )
    return filtered


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
        wind = _number(
            metric,
            "windSpeed",
            "windSpeedAvg",
            "windspeedAvg",
            "windGust",
            "windgustHigh",
        )
        if wind is not None:
            out.append(
                PwsObservation(
                    variable="wind",
                    valid_at=row.valid_at,
                    value=kmh_to_ms(wind),
                    source_raw=f"{wind} km/h",
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
    hourly history/backfill path depends on the ``floor_hour`` bucketing.
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

    Reads the top-level ``humidity``, ``winddir``, ``uv``, ``obsTimeUtc`` and
    ``neighborhood`` alongside the ``metric`` sub-object (``temp``, ``windSpeed``,
    ``windGust``, ``pressure``, ``precipRate``, ``precipTotal``, ``dewpt``). Values
    are km/h / mm / hPa in the station's native ``units:"m"`` form — this is the
    raw display snapshot table, NOT the SI-normalized scoring path, so no unit
    conversion is applied. Returns ``None`` when the payload has no first
    observation row (the caller treats that as OFFLINE). Missing fields → ``None``.
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

    ``timeout_seconds`` is the overall/read timeout (the connect timeout stays at
    5.0s); the caller passes the operator-configured ``request_timeout_seconds``
    setting, defaulting to the previous 10.0s literal.
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
    return await client.get(
        "https://api.weather.com/v2/pws/observations/current",
        params={
            "stationId": pws_station_id,
            "format": "json",
            "units": "m",
            "numericPrecision": "decimal",
            "apiKey": api_key,
        },
        timeout=httpx.Timeout(timeout_seconds, connect=5.0),
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


def _history_date_range(
    start: datetime, end: datetime, timezone: str | None
) -> tuple[str, str]:
    tz = UTC if timezone is None else ZoneInfo(timezone)
    local_start = start.astimezone(tz)
    local_end = (end - timedelta(microseconds=1)).astimezone(tz)
    return local_start.strftime("%Y%m%d"), local_end.strftime("%Y%m%d")
