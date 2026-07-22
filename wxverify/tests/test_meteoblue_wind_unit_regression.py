"""Regression pin: meteoblue requests must send windspeed=kmh.

Confirmed bug (systematic-debugging Phase 4): MeteoblueAdapter.fetch_forecast
issues its GET to packages/multimodel-1h with no `windspeed` unit parameter.
meteoblue's documented default unit for windspeed is m/s ("ms-1" in its own
units block), but VARIABLE_MAP labels the wind variable "km/h" and
_convert_value runs kmh_to_ms(raw) on every wind sample — dividing an
already-m/s value by 3.6.  Net effect: every meteoblue wind sample stored is
3.6x too low (a true 20 km/h forecast renders as ~5 km/h on the tile).

The fix (NOT implemented here — production code is out of scope for this
test file) is to add "windspeed": "kmh" to the outgoing request params so
meteoblue actually returns km/h, matching what kmh_to_ms assumes.

Convention: httpx.MockTransport with a synchronous handler function, matching
the pattern established in test_pws_adapter_params.py and the meteoblue
fetch_forecast call in test_m1_m5.py::test_meteoblue_parser_and_member_registration.
The handler asserts the expected params directly, making a wrong or absent
param an immediate test failure rather than a silent mismatch.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from wxverify.core.units import kmh_to_ms
from wxverify.feeds.meteoblue import MeteoblueAdapter
from wxverify.feeds.seam import ForecastRequest

# ---------------------------------------------------------------------------
# Test constants — synthetic placeholder data only
# ---------------------------------------------------------------------------

_API_KEY = "test-key-wxv-synthetic"
_LAT = 47.0
_LON = 25.0

# Minimal valid meteoblue-shaped payload: two models, one hour, wind-only
# focus.  Shape matches MeteoblueResponse/MeteoblueMetadata/MeteoblueData1h
# in wxverify/feeds/meteoblue.py.
_WIND_RAW = 72.0  # arbitrary synthetic raw reading in whatever unit meteoblue sent

_PAYLOAD = {
    "metadata": {
        "models": ["gfs"],
        "modelrun_utc": ["2026-01-01 00:00"],
        "latitude": 47.05,
        "longitude": 25.05,
        "height": 910.0,
    },
    "multimodel": {
        "data_1h": {
            "time": ["2026-01-01 01:00"],
            "temperature": [[1.0]],
            "windspeed": [[_WIND_RAW]],
            "precipitation": [[0.0]],
        }
    },
}


def _forecast_request() -> ForecastRequest:
    return ForecastRequest(
        lat=_LAT,
        lon=_LON,
        model="multimodel",
        variables=("temperature", "wind", "precip"),
        max_lead_hours=168,
    )


# ---------------------------------------------------------------------------
# Class 1: fetch_forecast — windspeed=kmh regression pin
# ---------------------------------------------------------------------------


class TestFetchForecastWindUnitParam:
    """MeteoblueAdapter.fetch_forecast must request windspeed in km/h.

    Without this param meteoblue defaults to m/s, but the adapter's
    VARIABLE_MAP/_convert_value pipeline unconditionally treats the raw
    value as km/h and divides by 3.6 again — silently deflating every wind
    sample by 3.6x.  This test fails today (param absent) and passes once
    the fix adds "windspeed": "kmh" to the request params.
    """

    def test_windspeed_kmh_param_sent(self) -> None:
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            assert request.url.params["windspeed"] == "kmh", (
                "MeteoblueAdapter.fetch_forecast must send windspeed=kmh so "
                "meteoblue returns wind in the unit _convert_value assumes "
                f"(kmh_to_ms); got params: {dict(request.url.params)}"
            )
            return httpx.Response(200, json=_PAYLOAD)

        async def run() -> None:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                await MeteoblueAdapter(_API_KEY, client).fetch_forecast(
                    _forecast_request()
                )

        asyncio.run(run())
        assert len(captured) == 1, "handler must be called exactly once"

    def test_all_required_params_sent(self) -> None:
        """apikey, lat, lon, tz, format, windspeed, temperature,
        precipitationamount all present.

        Bundled in one test so the regression pin is atomic — adding
        windspeed=kmh must not displace the other existing params.
        """
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            params = dict(request.url.params)
            assert params["apikey"] == _API_KEY
            assert params["lat"] == str(_LAT)
            assert params["lon"] == str(_LON)
            assert params["tz"] == "utc"
            assert params["format"] == "json"
            assert params["windspeed"] == "kmh"
            assert params["temperature"] == "C"
            assert params["precipitationamount"] == "mm"
            return httpx.Response(200, json=_PAYLOAD)

        async def run() -> None:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                await MeteoblueAdapter(_API_KEY, client).fetch_forecast(
                    _forecast_request()
                )

        asyncio.run(run())
        assert len(captured) == 1

    def test_endpoint_path_is_multimodel_1h(self) -> None:
        """Sanity: the request still targets packages/multimodel-1h.

        Paired guard against a handler that silently matches everything —
        if the path assertion below were vacuous this would still catch a
        broken URL, keeping the windspeed pin meaningful.
        """
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            assert request.url.path == "/packages/multimodel-1h", (
                f"unexpected path: {request.url.path}"
            )
            return httpx.Response(200, json=_PAYLOAD)

        async def run() -> None:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                await MeteoblueAdapter(_API_KEY, client).fetch_forecast(
                    _forecast_request()
                )

        asyncio.run(run())
        assert len(captured) == 1


# ---------------------------------------------------------------------------
# Class 2: wind value contract — kmh_to_ms is only correct once the request
# actually carries windspeed=kmh.  This documents the intended contract
# (paired positive) rather than re-proving the bug — it already passes today
# because _convert_value always assumes km/h input; it stays green after the
# fix because the fix makes that assumption true.
# ---------------------------------------------------------------------------


class TestWindValueConversionContract:
    """Stored wind NormalizedSample.value == kmh_to_ms(raw); source_raw
    carries the km/h label.

    This is the intended contract _convert_value implements.  It is only a
    correct contract once the request-level regression pin above is green —
    kept here so a future refactor of _convert_value can't silently drift
    from the km/h assumption without this test catching it.
    """

    def test_wind_sample_value_is_kmh_to_ms_of_raw(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
            return httpx.Response(200, json=_PAYLOAD)

        async def run() -> list:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                result = await MeteoblueAdapter(_API_KEY, client).fetch_forecast(
                    _forecast_request()
                )
                return result.samples

        samples = asyncio.run(run())
        wind_samples = [s for s in samples if s.variable == "wind"]
        assert len(wind_samples) == 1
        sample = wind_samples[0]
        assert sample.value == pytest.approx(kmh_to_ms(_WIND_RAW))
        assert sample.source_raw == f"{_WIND_RAW} km/h"
