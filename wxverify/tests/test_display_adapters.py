"""Display-only fields (cloud_cover, wind_gust, wind_direction).

Covers:
  1. Each adapter stores the display fields in %, m/s and degrees (km/h
     gusts through kmh_to_ms); a null or absent field stores nothing.
  2. Each adapter's outgoing request is unchanged except for Open-Meteo's
     three extra ``hourly=`` names and meteoblue's ``windDirectionUnit``,
     and every ``estimate_cost`` result is unchanged.
  3. Lenient display parsing never rejects a reply and never corrupts the
     scored (FORECAST_VARIABLES) rows.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
import pytest

import wxverify.feeds.google as google_feed
import wxverify.feeds.meteosource as meteosource_feed
import wxverify.feeds.openweathermap as openweathermap_feed
import wxverify.feeds.visualcrossing as visualcrossing_feed
import wxverify.feeds.weatherapi as weatherapi_feed
from wxverify.collection.forecast_validation import (
    DISPLAY_VARIABLES,
    FORECAST_VARIABLES,
)
from wxverify.core.units import kmh_to_ms
from wxverify.feeds.google import GoogleAdapter
from wxverify.feeds.meteoblue import MeteoblueAdapter
from wxverify.feeds.meteosource import MeteosourceAdapter
from wxverify.feeds.open_meteo import OpenMeteoAdapter
from wxverify.feeds.openweathermap import OpenWeatherMapAdapter
from wxverify.feeds.seam import FetchResult, ForecastRequest, NormalizedSample
from wxverify.feeds.visualcrossing import VisualCrossingAdapter
from wxverify.feeds.weatherapi import WeatherApiAdapter

ALL_VARIABLES = FORECAST_VARIABLES + DISPLAY_VARIABLES

_LAT = 40.0
_LON = -105.0
_API_KEY = "test-key-wxv-synthetic"

_ISSUED_AT = "2026-06-01T00:00:00Z"
_VALID_AT = "2026-06-01T12:00:00Z"  # lead 12 from _ISSUED_AT


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def _scored(samples: list[NormalizedSample]) -> list[tuple[str, str, int, float]]:
    """Scored (FORECAST_VARIABLES) rows, order-independent."""
    return sorted(
        (s.variable, s.valid_at, s.lead_hours, s.value)
        for s in samples
        if s.variable in FORECAST_VARIABLES
    )


def _by_variable(
    samples: list[NormalizedSample], variable: str
) -> list[NormalizedSample]:
    return [s for s in samples if s.variable == variable]


def _handler(payload: dict[str, Any]) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        return httpx.Response(200, json=payload)

    return handler


async def _fetch(adapter: Any, req: ForecastRequest) -> FetchResult:
    return await adapter.fetch_forecast(req)


def _req(
    max_lead_hours: int = 24,
    variables: tuple[str, ...] = ALL_VARIABLES,
    model: str = "blend",
):
    return ForecastRequest(
        lat=_LAT,
        lon=_LON,
        model=model,
        variables=variables,
        max_lead_hours=max_lead_hours,
    )


# ---------------------------------------------------------------------------
# Open-Meteo
# ---------------------------------------------------------------------------


def _open_meteo_payload(
    *, gust: object = 36.0, direction: object = 123.4
) -> dict[str, Any]:
    hourly: dict[str, Any] = {
        "time": [_VALID_AT],
        "temperature_2m": [20.0],
        "wind_speed_10m": [18.0],
        "precipitation": [1.2],
        "cloud_cover": [55.5],
    }
    if gust is not _absent:
        hourly["wind_gusts_10m"] = [gust]
    if direction is not _absent:
        hourly["wind_direction_10m"] = [direction]
    return {"latitude": _LAT, "longitude": _LON, "hourly": hourly}


_absent = object()


def _open_meteo_adapter(payload: dict[str, Any]) -> OpenMeteoAdapter:
    client = httpx.AsyncClient(transport=httpx.MockTransport(_handler(payload)))
    return OpenMeteoAdapter(client)


def test_open_meteo_stores_display_fields_converted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches: a missing km/h->m/s gust conversion, or a truncated decimal."""
    import wxverify.feeds.open_meteo as open_meteo_module
    from wxverify.core.timeutil import parse_utc

    monkeypatch.setattr(
        open_meteo_module, "utc_now", lambda: parse_utc("2026-06-01T07:00:00Z")
    )
    adapter = _open_meteo_adapter(_open_meteo_payload())
    samples = _run(_fetch(adapter, _req(model="icon_eu"))).samples

    cloud = _by_variable(samples, "cloud_cover")
    gust = _by_variable(samples, "wind_gust")
    direction = _by_variable(samples, "wind_direction")
    assert [s.value for s in cloud] == [55.5]
    assert [s.value for s in gust] == [kmh_to_ms(36.0)] == [10.0]
    assert [s.value for s in direction] == [123.4]


@pytest.mark.parametrize("variant", ["null", "absent"])
def test_open_meteo_null_or_absent_gust_stores_nothing(
    monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    """Catches: a bad display value or a missing key stored as 0 instead of
    no sample at all (paired against the positive conversion test above)."""
    import wxverify.feeds.open_meteo as open_meteo_module
    from wxverify.core.timeutil import parse_utc

    monkeypatch.setattr(
        open_meteo_module, "utc_now", lambda: parse_utc("2026-06-01T07:00:00Z")
    )
    gust = None if variant == "null" else _absent
    adapter = _open_meteo_adapter(_open_meteo_payload(gust=gust))
    samples = _run(_fetch(adapter, _req(model="icon_eu"))).samples

    gust_samples = _by_variable(samples, "wind_gust")
    assert gust_samples == []
    assert all(s.value != 0.0 for s in samples)


def test_open_meteo_request_hourly_differs_only_by_three_display_names() -> None:
    """Catches: an extra or missing hourly= name beyond the three display
    fields, or a changed param elsewhere in the request."""
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_open_meteo_payload())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = OpenMeteoAdapter(client)
    _run(_fetch(adapter, _req(variables=FORECAST_VARIABLES, model="icon_eu")))
    _run(_fetch(adapter, _req(variables=ALL_VARIABLES, model="icon_eu")))
    assert len(captured) == 2

    plain = dict(captured[0].url.params)
    with_display = dict(captured[1].url.params)
    plain_names = set(plain["hourly"].split(","))
    display_names = set(with_display["hourly"].split(","))
    assert display_names - plain_names == {
        "cloud_cover",
        "wind_gusts_10m",
        "wind_direction_10m",
    }
    plain_without_hourly = {k: v for k, v in plain.items() if k != "hourly"}
    display_without_hourly = {k: v for k, v in with_display.items() if k != "hourly"}
    assert plain_without_hourly == display_without_hourly


def test_open_meteo_estimate_cost_and_metered_calls_unchanged() -> None:
    """Catches: a larger metered call count once the display names are
    added to the hourly request (Open-Meteo stays 1)."""
    adapter = OpenMeteoAdapter(httpx.AsyncClient())
    plain = adapter.estimate_cost(_req(variables=FORECAST_VARIABLES))
    with_display = adapter.estimate_cost(_req(variables=ALL_VARIABLES))
    assert plain.calls == with_display.calls == 1


# ---------------------------------------------------------------------------
# meteoblue
# ---------------------------------------------------------------------------

_METEOBLUE_B_PARAMS: dict[str, Any] = {
    # Copied by hand from ``wxverify.feeds.meteoblue``'s request parameters,
    # apikey replaced with a made-up value.
    "apikey": _API_KEY,
    "lat": _LAT,
    "lon": _LON,
    "tz": "utc",
    "format": "json",
    "windspeed": "kmh",
    "temperature": "C",
    "precipitationamount": "mm",
}


def _meteoblue_payload(
    *, cloudcover: object = 61.0, direction: object = 200.0
) -> dict[str, Any]:
    data_1h: dict[str, Any] = {
        "time": ["2026-06-01 12:00"],
        "temperature": [[20.0]],
        "windspeed": [[36.0]],
        "precipitation": [[1.2]],
    }
    if cloudcover is not _absent:
        data_1h["cloudcover"] = [[cloudcover]]
    if direction is not _absent:
        data_1h["winddirection"] = [[direction]]
    return {
        "metadata": {
            "models": ["gfs"],
            "modelrun_utc": ["2026-06-01 00:00"],
            "latitude": _LAT,
            "longitude": _LON,
        },
        "multimodel": {"data_1h": data_1h},
    }


def test_meteoblue_stores_display_fields_no_gust() -> None:
    """Catches: a mislabelled matrix key or a missing lenient read of
    winddirection/cloudcover."""

    def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        return httpx.Response(200, json=_meteoblue_payload())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = MeteoblueAdapter(_API_KEY, client)
    samples = _run(_fetch(adapter, _req())).samples

    assert [s.value for s in _by_variable(samples, "cloud_cover")] == [61.0]
    assert [s.value for s in _by_variable(samples, "wind_direction")] == [200.0]
    assert _by_variable(samples, "wind_gust") == []


@pytest.mark.parametrize("variant", ["null", "absent"])
def test_meteoblue_null_or_absent_direction_stores_nothing(variant: str) -> None:
    """Catches: a null/absent matrix entry stored as 0 instead of dropped."""
    direction = None if variant == "null" else _absent

    def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
        return httpx.Response(200, json=_meteoblue_payload(direction=direction))

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = MeteoblueAdapter(_API_KEY, client)
    samples = _run(_fetch(adapter, _req())).samples

    assert _by_variable(samples, "wind_direction") == []
    assert all(s.value != 0.0 for s in samples)


def test_meteoblue_request_params_equal_b_plus_wind_direction_unit() -> None:
    """Catches: a dropped/changed param, or a missing windDirectionUnit."""
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_meteoblue_payload())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = MeteoblueAdapter(_API_KEY, client)
    _run(_fetch(adapter, _req()))
    assert len(captured) == 1
    params = {k: str(v) for k, v in captured[0].url.params.items()}
    expected = {k: str(v) for k, v in _METEOBLUE_B_PARAMS.items()}
    expected["windDirectionUnit"] = "degree"
    assert params == expected


def test_meteoblue_estimate_cost_unchanged() -> None:
    adapter = MeteoblueAdapter(_API_KEY, httpx.AsyncClient())
    plain = adapter.estimate_cost(_req(variables=FORECAST_VARIABLES))
    with_display = adapter.estimate_cost(_req(variables=ALL_VARIABLES))
    assert plain == with_display


# ---------------------------------------------------------------------------
# The five "simple" adapters: Visual Crossing, OpenWeatherMap, WeatherAPI,
# Meteosource, Google. Their request params do not depend on req.variables
# at all, so the FORECAST_VARIABLES vs ALL_VARIABLES request is identical.
# ---------------------------------------------------------------------------

_SYNTHETIC_EPOCH = 1780315200  # 2026-06-01T12:00:00Z


def _vc_payload(*, gust: object = 36.0, cloudcover: object = 61.0) -> dict[str, Any]:
    hour: dict[str, Any] = {
        "datetimeEpoch": _SYNTHETIC_EPOCH,
        "temp": 20.0,
        "windspeed": 18.0,
        "precip": 1.2,
        "winddir": 123.4,
    }
    if gust is not _absent:
        hour["windgust"] = gust
    if cloudcover is not _absent:
        hour["cloudcover"] = cloudcover
    return {"days": [{"hours": [hour]}]}


def _owm_payload(*, gust: object = 12.3, direction: object = 123.4) -> dict[str, Any]:
    hour: dict[str, Any] = {
        "dt": _SYNTHETIC_EPOCH,
        "temp": 20.0,
        "wind_speed": 5.0,
        "rain": {"1h": 1.2},
        "clouds": 61.0,
    }
    if gust is not _absent:
        hour["wind_gust"] = gust
    if direction is not _absent:
        hour["wind_deg"] = direction
    return {"hourly": [hour]}


def _weatherapi_payload(
    *, gust: object = 36.0, direction: object = 123.4
) -> dict[str, Any]:
    hour: dict[str, Any] = {
        "time_epoch": _SYNTHETIC_EPOCH,
        "temp_c": 20.0,
        "wind_kph": 18.0,
        "precip_mm": 1.2,
        "cloud": 61.0,
    }
    if gust is not _absent:
        hour["gust_kph"] = gust
    if direction is not _absent:
        hour["wind_degree"] = direction
    return {"forecast": {"forecastday": [{"hour": [hour]}]}}


def _meteosource_payload(
    *, angle: object = 123.4, cloud_cover: object = 61.0
) -> dict[str, Any]:
    wind: dict[str, Any] = {"speed": 5.0}
    if angle is not _absent:
        wind["angle"] = angle
    hour: dict[str, Any] = {
        "date": "2026-06-01T12:00:00",
        "temperature": 20.0,
        "wind": wind,
        "precipitation": {"total": 1.2},
    }
    if cloud_cover is not _absent:
        hour["cloud_cover"] = {"total": cloud_cover}
    return {"timezone": "UTC", "hourly": {"data": [hour]}}


def _google_payload(
    *,
    gust: dict[str, Any] | None | object = ...,
    direction: object = 123.4,
    cloud: object = 61.4,
) -> dict[str, Any]:
    wind: dict[str, Any] = {"speed": {"value": 18.0, "unit": "KILOMETERS_PER_HOUR"}}
    if gust is not ...:
        if gust is not None:
            wind["gust"] = gust
    else:
        wind["gust"] = {"value": 36.0, "unit": "KILOMETERS_PER_HOUR"}
    if direction is not _absent:
        wind["direction"] = {"degrees": direction}
    hour: dict[str, Any] = {
        "interval": {"startTime": "2026-06-01T01:00:00Z"},
        "temperature": {"degrees": 20.0, "unit": "CELSIUS"},
        "wind": wind,
        "precipitation": {"qpf": {"quantity": 1.2, "unit": "MILLIMETERS"}},
    }
    if cloud is not _absent:
        hour["cloudCover"] = cloud
    return {"forecastHours": [hour]}


_SIMPLE_ADAPTER_CASES = (
    ("visualcrossing", VisualCrossingAdapter, visualcrossing_feed, _vc_payload()),
    ("openweathermap", OpenWeatherMapAdapter, openweathermap_feed, _owm_payload()),
    ("weatherapi", WeatherApiAdapter, weatherapi_feed, _weatherapi_payload()),
    ("meteosource", MeteosourceAdapter, meteosource_feed, _meteosource_payload()),
    ("google", GoogleAdapter, google_feed, _google_payload()),
)


def _build_simple_adapter(adapter_cls: type, client: httpx.AsyncClient) -> Any:
    """All five simple adapters share the (api_key, client) constructor."""
    return adapter_cls(_API_KEY, client)


_SIMPLE_IDS = [case[0] for case in _SIMPLE_ADAPTER_CASES]


@pytest.mark.parametrize(
    ("name", "adapter_cls", "module", "payload"), _SIMPLE_ADAPTER_CASES, ids=_SIMPLE_IDS
)
def test_simple_adapter_request_unaffected_by_display_variables(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    adapter_cls: type,
    module: Any,
    payload: dict[str, Any],
) -> None:
    """Catches: a display-variable branch leaking into the request builder
    of an adapter whose request must be the same for either variable set."""
    monkeypatch.setattr(module, "snap_run", lambda fetch_time=None: _ISSUED_AT)
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=payload)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = _build_simple_adapter(adapter_cls, client)
    max_lead = 1 if name == "google" else 24
    _run(_fetch(adapter, _req(max_lead_hours=max_lead, variables=FORECAST_VARIABLES)))
    _run(_fetch(adapter, _req(max_lead_hours=max_lead, variables=ALL_VARIABLES)))
    assert len(captured) == 2
    plain_params = {k: str(v) for k, v in captured[0].url.params.items()}
    display_params = {k: str(v) for k, v in captured[1].url.params.items()}
    assert plain_params == display_params


@pytest.mark.parametrize(
    ("name", "adapter_cls", "module", "payload"), _SIMPLE_ADAPTER_CASES, ids=_SIMPLE_IDS
)
def test_simple_adapter_estimate_cost_unaffected_by_display_variables(
    adapter_cls: type, module: Any, payload: dict[str, Any], name: str
) -> None:
    """Catches: estimate_cost drifting once DISPLAY_VARIABLES are added."""
    del module, payload
    client = httpx.AsyncClient()
    adapter = _build_simple_adapter(adapter_cls, client)
    max_lead = 1 if name == "google" else 24
    plain = adapter.estimate_cost(
        _req(max_lead_hours=max_lead, variables=FORECAST_VARIABLES)
    )
    with_display = adapter.estimate_cost(
        _req(max_lead_hours=max_lead, variables=ALL_VARIABLES)
    )
    assert plain == with_display


# ---------------------------------------------------------------------------
# Storage of the display fields themselves, per simple adapter.
# ---------------------------------------------------------------------------


def test_visualcrossing_stores_display_fields_converted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches: a missing km/h->m/s gust conversion."""
    monkeypatch.setattr(
        visualcrossing_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(_handler(_vc_payload())))
    adapter = VisualCrossingAdapter(_API_KEY, client)
    samples = _run(_fetch(adapter, _req(max_lead_hours=24))).samples
    assert [s.value for s in _by_variable(samples, "cloud_cover")] == [61.0]
    assert (
        [s.value for s in _by_variable(samples, "wind_gust")]
        == [kmh_to_ms(36.0)]
        == [10.0]
    )
    assert [s.value for s in _by_variable(samples, "wind_direction")] == [123.4]


@pytest.mark.parametrize("variant", ["null", "absent"])
def test_visualcrossing_null_or_absent_gust_stores_nothing(
    monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    """Catches: null/absent gust stored as 0 instead of dropped."""
    monkeypatch.setattr(
        visualcrossing_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT
    )
    gust = None if variant == "null" else _absent
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler(_vc_payload(gust=gust)))
    )
    adapter = VisualCrossingAdapter(_API_KEY, client)
    samples = _run(_fetch(adapter, _req(max_lead_hours=24))).samples
    assert _scored(samples) != []
    assert _by_variable(samples, "wind_gust") == []
    assert all(s.value != 0.0 for s in samples)


def test_visualcrossing_na_cloudcover_parses_scored_rows_unaffected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches: a non-lenient cloudcover reader that rejects "N/A" and loses
    the scored (temperature/wind/precip) rows with it."""
    monkeypatch.setattr(
        visualcrossing_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT
    )

    def _samples(cloudcover: object) -> list[NormalizedSample]:
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(_handler(_vc_payload(cloudcover=cloudcover)))
        )
        adapter = VisualCrossingAdapter(_API_KEY, client)
        return _run(_fetch(adapter, _req(max_lead_hours=24))).samples

    na_samples = _samples("N/A")
    absent_samples = _samples(_absent)
    assert _by_variable(na_samples, "cloud_cover") == []
    assert _scored(na_samples) != []
    assert _scored(na_samples) == _scored(absent_samples)


def test_openweathermap_stores_display_fields_no_conversion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches: an accidental km/h conversion applied to an already-m/s
    OWM gust."""
    monkeypatch.setattr(
        openweathermap_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(_handler(_owm_payload())))
    adapter = OpenWeatherMapAdapter(_API_KEY, client)
    samples = _run(_fetch(adapter, _req(max_lead_hours=24))).samples
    assert [s.value for s in _by_variable(samples, "cloud_cover")] == [61.0]
    assert [s.value for s in _by_variable(samples, "wind_gust")] == [12.3]
    assert [s.value for s in _by_variable(samples, "wind_direction")] == [123.4]


@pytest.mark.parametrize("variant", ["null", "absent"])
def test_openweathermap_null_or_absent_gust_stores_nothing(
    monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    monkeypatch.setattr(
        openweathermap_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT
    )
    gust = None if variant == "null" else _absent
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler(_owm_payload(gust=gust)))
    )
    adapter = OpenWeatherMapAdapter(_API_KEY, client)
    samples = _run(_fetch(adapter, _req(max_lead_hours=24))).samples
    assert _scored(samples) != []
    assert _by_variable(samples, "wind_gust") == []
    assert all(s.value != 0.0 for s in samples)


def test_weatherapi_stores_display_fields_converted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches: a missing km/h->m/s gust conversion."""
    monkeypatch.setattr(weatherapi_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT)
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler(_weatherapi_payload()))
    )
    adapter = WeatherApiAdapter(_API_KEY, client)
    samples = _run(_fetch(adapter, _req(max_lead_hours=24))).samples
    assert [s.value for s in _by_variable(samples, "cloud_cover")] == [61.0]
    assert (
        [s.value for s in _by_variable(samples, "wind_gust")]
        == [kmh_to_ms(36.0)]
        == [10.0]
    )
    assert [s.value for s in _by_variable(samples, "wind_direction")] == [123.4]


@pytest.mark.parametrize("variant", ["null", "absent"])
def test_weatherapi_null_or_absent_gust_stores_nothing(
    monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    monkeypatch.setattr(weatherapi_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT)
    gust = None if variant == "null" else _absent
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler(_weatherapi_payload(gust=gust)))
    )
    adapter = WeatherApiAdapter(_API_KEY, client)
    samples = _run(_fetch(adapter, _req(max_lead_hours=24))).samples
    assert _scored(samples) != []
    assert _by_variable(samples, "wind_gust") == []
    assert all(s.value != 0.0 for s in samples)


def test_meteosource_stores_display_fields_no_gust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches: a missing lenient read of wind.angle/cloud_cover.total."""
    monkeypatch.setattr(
        meteosource_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT
    )
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler(_meteosource_payload()))
    )
    adapter = MeteosourceAdapter(_API_KEY, client)
    samples = _run(_fetch(adapter, _req(max_lead_hours=24))).samples
    assert [s.value for s in _by_variable(samples, "cloud_cover")] == [61.0]
    assert [s.value for s in _by_variable(samples, "wind_direction")] == [123.4]
    assert _by_variable(samples, "wind_gust") == []


@pytest.mark.parametrize("variant", ["null", "absent"])
def test_meteosource_null_or_absent_direction_stores_nothing(
    monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    monkeypatch.setattr(
        meteosource_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT
    )
    angle = None if variant == "null" else _absent
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler(_meteosource_payload(angle=angle)))
    )
    adapter = MeteosourceAdapter(_API_KEY, client)
    samples = _run(_fetch(adapter, _req(max_lead_hours=24))).samples
    assert _by_variable(samples, "wind_direction") == []
    assert all(s.value != 0.0 for s in samples)


# ---------------------------------------------------------------------------
# Google: decimal direction/cloud cover, wrong-unit gust (1 WARNING, paired
# against a clean positive), gust with no value (0 WARNINGs).
# ---------------------------------------------------------------------------


def _google_adapter(
    payload: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> GoogleAdapter:
    monkeypatch.setattr(google_feed, "snap_run", lambda fetch_time=None: _ISSUED_AT)
    client = httpx.AsyncClient(transport=httpx.MockTransport(_handler(payload)))
    return GoogleAdapter(_API_KEY, client)


def test_google_stores_decimal_display_fields_converted(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Catches: an int-typed display leaf truncating 61.4/123.4, or a
    missing km/h->m/s gust conversion."""
    adapter = _google_adapter(_google_payload(), monkeypatch)
    with caplog.at_level(logging.WARNING, logger="wxverify.feeds.google"):
        samples = _run(_fetch(adapter, _req(max_lead_hours=1))).samples
    assert [s.value for s in _by_variable(samples, "cloud_cover")] == [61.4]
    assert [s.value for s in _by_variable(samples, "wind_direction")] == [123.4]
    assert (
        [s.value for s in _by_variable(samples, "wind_gust")]
        == [kmh_to_ms(36.0)]
        == [10.0]
    )
    assert [r for r in caplog.records if r.levelno == logging.WARNING] == []


def test_google_wrong_unit_gust_skipped_with_one_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Catches: the gust stored through GoogleSpeed / the speed unit assert
    instead of being skipped leniently, or more than one WARNING per reply."""
    wrong_unit_payload = _google_payload(gust={"value": 99.0, "unit": "MILES_PER_HOUR"})
    plain_payload = _google_payload(gust=None)
    adapter = _google_adapter(wrong_unit_payload, monkeypatch)
    with caplog.at_level(logging.WARNING, logger="wxverify.feeds.google"):
        samples = _run(_fetch(adapter, _req(max_lead_hours=1))).samples
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "skipped 1 wind gust value" in warnings[0].message
    assert _by_variable(samples, "wind_gust") == []

    plain_adapter = _google_adapter(plain_payload, monkeypatch)
    plain_samples = _run(_fetch(plain_adapter, _req(max_lead_hours=1))).samples
    assert _scored(samples) == _scored(plain_samples)


def test_google_gust_with_no_value_skipped_without_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Paired absence test: a gust with no 'value' must be silently dropped
    (0 WARNINGs), not counted as a wrong-unit skip."""
    no_value_payload = _google_payload(gust={"unit": "KILOMETERS_PER_HOUR"})
    adapter = _google_adapter(no_value_payload, monkeypatch)
    with caplog.at_level(logging.WARNING, logger="wxverify.feeds.google"):
        samples = _run(_fetch(adapter, _req(max_lead_hours=1))).samples
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []
    assert _by_variable(samples, "wind_gust") == []
    assert all(s.value != 0.0 for s in samples)
