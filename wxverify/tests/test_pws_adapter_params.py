"""Regression pins: numericPrecision="decimal" in every weather.com GET request.

weather.com silently integer-rounds temperature, dewpoint, and wind-speed when
numericPrecision is absent.  The param has been dropped twice historically, so
these tests capture the outgoing GET params for every call site that must carry
it:

  - fetch_hourly_history  (observations/hourly/7day)
  - fetch_hourly_history_range  (history/hourly)
  - fetch_current_observation  (observations/current)

validate_station is deliberately excluded: it is a station-existence probe and
is precision-insensitive by design.

Convention: httpx.MockTransport with a synchronous handler function, matching
the pattern established in test_m1_m5.py for fetch_hourly_history_range.
The handler asserts the expected params directly, making a wrong or absent param
an immediate test failure rather than a silent mismatch.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import patch

import httpx
import pytest

from wxverify.obs.pws_adapter import (
    fetch_current_observation,
    fetch_hourly_history,
    fetch_hourly_history_range,
)

# ---------------------------------------------------------------------------
# Test constants — synthetic placeholder data only
# ---------------------------------------------------------------------------

_STATION_ID = "ISTATION01"
_API_KEY = "test-key-wxv-synthetic"

# A fixed "now" that sits well within any observation window so the rolling
# cutoff in fetch_hourly_history does not filter out our synthetic record.
_NOW = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)
_NOW_ISO = "2026-07-10T12:00:00Z"

# A minimal valid hourly payload with an obsTimeUtc inside the window.
# RECENT_REFRESH_HOURS defaults to 6, so T-1h is safely within the window
# regardless of any utc_now patch — but we patch utc_now to be sure.
_OBS_TIME_RECENT = "2026-07-10T11:30:00Z"

_HOURLY_PAYLOAD = {
    "observations": [
        {
            "obsTimeUtc": _OBS_TIME_RECENT,
            "metric": {
                "temp": 18.3,
                "windSpeed": 12.0,
                "precipTotal": 0.5,
            },
        }
    ]
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _json_response(data: object, status: int = 200) -> httpx.Response:
    """Return a minimal valid httpx.Response carrying JSON body."""
    body = json.dumps(data).encode()
    return httpx.Response(
        status_code=status,
        content=body,
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", "https://api.weather.com/v2/pws/placeholder"),
    )


# ---------------------------------------------------------------------------
# Class 1: fetch_hourly_history — numericPrecision regression pin
# ---------------------------------------------------------------------------


class TestFetchHourlyHistoryParams:
    """fetch_hourly_history sends numericPrecision=decimal on every call."""

    def test_numeric_precision_decimal_sent(self) -> None:
        """numericPrecision=decimal present in the GET to observations/hourly/7day.

        Regression pin: absence of this param causes weather.com to
        integer-round temp/dewpoint/wind values silently.  The handler captures
        and asserts every required param so a dropped param fails immediately.
        """
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            assert request.url.params["numericPrecision"] == "decimal", (
                "fetch_hourly_history must send numericPrecision=decimal; "
                f"got params: {dict(request.url.params)}"
            )
            return _json_response(_HOURLY_PAYLOAD)

        async def run() -> object:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                with patch("wxverify.obs.pws_adapter.utc_now", return_value=_NOW):
                    return await fetch_hourly_history(
                        _STATION_ID, _API_KEY, client=client
                    )

        asyncio.run(run())
        assert len(captured) == 1, "handler must be called exactly once"

    def test_all_required_params_sent(self) -> None:
        """stationId, format, units, numericPrecision, apiKey all present.

        A missing any of these breaks the request.  Bundled in one test so the
        regression pin is atomic — adding numericPrecision must not displace
        the other params.
        """
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            params = dict(request.url.params)
            assert params["stationId"] == _STATION_ID
            assert params["format"] == "json"
            assert params["units"] == "m"
            assert params["numericPrecision"] == "decimal"
            assert params["apiKey"] == _API_KEY
            return _json_response(_HOURLY_PAYLOAD)

        async def run() -> object:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                with patch("wxverify.obs.pws_adapter.utc_now", return_value=_NOW):
                    return await fetch_hourly_history(
                        _STATION_ID, _API_KEY, client=client
                    )

        asyncio.run(run())
        assert len(captured) == 1

    def test_endpoint_path_is_observations_hourly_7day(self) -> None:
        """Request targets the observations/hourly/7day endpoint."""
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            assert "/pws/observations/hourly/7day" in request.url.path, (
                f"unexpected path: {request.url.path}"
            )
            return _json_response(_HOURLY_PAYLOAD)

        async def run() -> object:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                with patch("wxverify.obs.pws_adapter.utc_now", return_value=_NOW):
                    return await fetch_hourly_history(
                        _STATION_ID, _API_KEY, client=client
                    )

        asyncio.run(run())
        assert len(captured) == 1


# ---------------------------------------------------------------------------
# Class 2: fetch_hourly_history_range — numericPrecision regression pin
# ---------------------------------------------------------------------------


class TestFetchHourlyHistoryRangeParams:
    """fetch_hourly_history_range sends numericPrecision=decimal on every call."""

    # A narrow window that crosses midnight so date-range logic is exercised.
    _WINDOW_START = "2026-07-09T23:00:00Z"
    _WINDOW_END = "2026-07-10T01:00:00Z"

    # Payload with one record inside the window.
    _RANGE_PAYLOAD = {
        "observations": [
            {
                "obsTimeUtc": "2026-07-10T00:30:00Z",
                "metric": {
                    "tempAvg": 14.7,
                    "windspeedAvg": 10.0,
                    "precipTotal": 0.0,
                },
            }
        ]
    }

    def test_numeric_precision_decimal_sent(self) -> None:
        """numericPrecision=decimal present in the GET to history/hourly."""
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            assert request.url.params["numericPrecision"] == "decimal", (
                "fetch_hourly_history_range must send numericPrecision=decimal; "
                f"got params: {dict(request.url.params)}"
            )
            return _json_response(self._RANGE_PAYLOAD)

        async def run() -> object:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                return await fetch_hourly_history_range(
                    _STATION_ID,
                    _API_KEY,
                    window_start=self._WINDOW_START,
                    window_end=self._WINDOW_END,
                    client=client,
                )

        asyncio.run(run())
        assert len(captured) == 1

    def test_all_required_params_sent(self) -> None:
        """stationId, format, units, startDate, endDate, numericPrecision, apiKey all
        present."""
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            params = dict(request.url.params)
            assert params["stationId"] == _STATION_ID
            assert params["format"] == "json"
            assert params["units"] == "m"
            assert params["numericPrecision"] == "decimal"
            assert params["apiKey"] == _API_KEY
            # startDate and endDate must be present (date strings in YYYYMMDD form)
            assert "startDate" in params, "startDate must be present"
            assert "endDate" in params, "endDate must be present"
            return _json_response(self._RANGE_PAYLOAD)

        async def run() -> object:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                return await fetch_hourly_history_range(
                    _STATION_ID,
                    _API_KEY,
                    window_start=self._WINDOW_START,
                    window_end=self._WINDOW_END,
                    client=client,
                )

        asyncio.run(run())
        assert len(captured) == 1

    def test_endpoint_path_is_history_hourly(self) -> None:
        """Request targets the pws/history/hourly endpoint."""
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            assert "/pws/history/hourly" in request.url.path, (
                f"unexpected path: {request.url.path}"
            )
            return _json_response(self._RANGE_PAYLOAD)

        async def run() -> object:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                return await fetch_hourly_history_range(
                    _STATION_ID,
                    _API_KEY,
                    window_start=self._WINDOW_START,
                    window_end=self._WINDOW_END,
                    client=client,
                )

        asyncio.run(run())
        assert len(captured) == 1

    def test_empty_window_makes_no_request(self) -> None:
        """window_end <= window_start → short-circuit, no HTTP call made.

        Paired negative: if the guard were absent, the handler would be called
        and the window-range assertion above would be trivially vacuous.
        """
        called: list[bool] = []

        def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
            called.append(True)
            return _json_response({"observations": []})

        async def run() -> object:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                return await fetch_hourly_history_range(
                    _STATION_ID,
                    _API_KEY,
                    window_start="2026-07-10T01:00:00Z",
                    window_end="2026-07-10T01:00:00Z",  # equal → empty
                    client=client,
                )

        asyncio.run(run())
        assert called == [], "empty window must short-circuit without an HTTP call"


# ---------------------------------------------------------------------------
# Class 3: fetch_current_observation — numericPrecision regression pin
# ---------------------------------------------------------------------------


class TestFetchCurrentObservationParams:
    """fetch_current_observation sends numericPrecision=decimal on every call."""

    # A minimal 200 payload to prevent raise_for_status from firing.
    _CURRENT_PAYLOAD = {
        "observations": [
            {
                "obsTimeUtc": _NOW_ISO,
                "humidity": 70.0,
                "winddir": 180.0,
                "uv": 2.0,
                "metric": {
                    "temp": 20.1,
                    "dewpt": 14.5,
                    "windSpeed": 8.0,
                    "windGust": 12.0,
                    "pressure": 1012.0,
                    "precipRate": 0.0,
                    "precipTotal": 0.0,
                },
            }
        ]
    }

    def test_numeric_precision_decimal_sent(self) -> None:
        """numericPrecision=decimal present in the GET to observations/current.

        Regression pin: absence of this param causes weather.com to
        integer-round temp and dewpoint in the current-obs snapshot.
        """
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            assert request.url.params["numericPrecision"] == "decimal", (
                "fetch_current_observation must send numericPrecision=decimal; "
                f"got params: {dict(request.url.params)}"
            )
            return _json_response(self._CURRENT_PAYLOAD)

        async def run() -> object:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                return await fetch_current_observation(
                    _STATION_ID, _API_KEY, client=client
                )

        asyncio.run(run())
        assert len(captured) == 1

    def test_all_required_params_sent(self) -> None:
        """stationId, format, units, numericPrecision, apiKey all present."""
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            params = dict(request.url.params)
            assert params["stationId"] == _STATION_ID
            assert params["format"] == "json"
            assert params["units"] == "m"
            assert params["numericPrecision"] == "decimal"
            assert params["apiKey"] == _API_KEY
            return _json_response(self._CURRENT_PAYLOAD)

        async def run() -> object:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                return await fetch_current_observation(
                    _STATION_ID, _API_KEY, client=client
                )

        asyncio.run(run())
        assert len(captured) == 1

    def test_endpoint_path_is_observations_current(self) -> None:
        """Request targets the pws/observations/current endpoint."""
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            assert "/pws/observations/current" in request.url.path, (
                f"unexpected path: {request.url.path}"
            )
            return _json_response(self._CURRENT_PAYLOAD)

        async def run() -> object:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                return await fetch_current_observation(
                    _STATION_ID, _API_KEY, client=client
                )

        asyncio.run(run())
        assert len(captured) == 1

    def test_returns_raw_response_not_parsed(self) -> None:
        """fetch_current_observation returns httpx.Response, never the parsed body.

        The caller (processor) needs the raw status code to drive the poll
        state-machine.  This pins that the function does not call raise_for_status
        and does not parse the JSON — it returns the Response as-is.
        """

        def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
            return _json_response(self._CURRENT_PAYLOAD)

        async def run() -> httpx.Response:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                return await fetch_current_observation(
                    _STATION_ID, _API_KEY, client=client
                )

        result = asyncio.run(run())
        assert isinstance(result, httpx.Response), (
            "fetch_current_observation must return the raw httpx.Response"
        )
        assert result.status_code == 200

    @pytest.mark.parametrize(
        "status",
        [204, 401, 429, 503],
        ids=["204", "401", "429", "503"],
    )
    def test_non_200_status_returned_not_raised(self, status: int) -> None:
        """Error/non-200 statuses are returned, not raised.

        fetch_current_observation deliberately omits raise_for_status so the
        caller can classify 204 vs 429 vs 401 vs 503 separately.  A raised
        exception here would break the poll state-machine.
        """

        def handler(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
            return httpx.Response(
                status_code=status,
                content=b"",
                request=httpx.Request(
                    "GET",
                    "https://api.weather.com/v2/pws/observations/current",
                ),
            )

        async def run() -> httpx.Response:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                return await fetch_current_observation(
                    _STATION_ID, _API_KEY, client=client
                )

        result = asyncio.run(run())
        assert result.status_code == status, (
            f"status {status} must be returned as-is, not raised"
        )
