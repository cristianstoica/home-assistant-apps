"""The P2 collection-contract parsers (plan 2026-10-21-daily-close-download-
and-raw-store.md, section 5.2/5.5/5.6, test IDs PR1-PR7).

``readings_from_payload`` and ``current_obs_from_payload`` are pure: no
database, no clock, no network. ``fetch_hourly_history_payload`` is the new
split of ``fetch_hourly_history`` (5.6); its test (PR7) uses an
``httpx.MockTransport`` handler, mirroring ``tests/test_pws_adapter_params.py``.

Synthetic data only: station ids stand for K24's A-F (``ISTATION01`` to
``ISTATION06``), the API key is the CI placeholder.
"""

from __future__ import annotations

import asyncio
import json
import math
from datetime import UTC, date, datetime
from unittest.mock import patch

import httpx

from wxverify.obs.hourly_values import Quantity, Source, StationReading  # noqa: F401
from wxverify.obs.pws_adapter import (
    CurrentObservation,
    ParsedReadings,  # noqa: F401
    current_obs_from_payload,
    fetch_hourly_history,
    fetch_hourly_history_payload,
    readings_from_payload,
)

_API_KEY = "test-key-wxv-synthetic"


def _epoch(hour: int, minute: int, second: int = 0) -> int:
    return int(datetime(2026, 10, 27, hour, minute, second, tzinfo=UTC).timestamp())


def _payload(*observations: dict[str, object]) -> dict[str, object]:
    return {"observations": list(observations)}


# --- PR1 --------------------------------------------------------------


def test_pr1_tempavg_preferred_over_high_low_and_temp_not_required() -> None:
    """PR1 (14.3): ``tempAvg`` is read over ``tempHigh``/``tempLow``; a
    reading with no ``temp`` field still gets a temperature from it. A
    second item has both ``tempAvg`` and ``temp`` present, to pin that
    ``tempAvg`` is read before ``temp``.

    mutant_high_low -> at the ``temp_c`` assertion: correct = 11.0 (the
    ``tempAvg`` value), mutant (falling back to ``tempHigh``/``tempLow``
    when ``tempAvg`` is present) = 12.0 or 10.0 -- the fixture's three
    temperature fields are deliberately distinct so the three literals
    diverge.

    M4 -> at the second item's ``temp_c`` assertion: correct = 11.0 (the
    ``tempAvg`` value), mutant (reading ``temp`` before ``tempAvg``) =
    15.0.
    """
    item = {
        "valid_time_gmt": _epoch(10, 4),
        "obsTimeLocal": "2026-10-27 10:04:00",
        "metric": {
            "tempAvg": 11.0,
            "tempHigh": 12.0,
            "tempLow": 10.0,
            "precipTotal": 1.2,
            "windspeedAvg": 7.0,
        },
    }
    item_with_temp = {
        "valid_time_gmt": _epoch(10, 9),
        "obsTimeLocal": "2026-10-27 10:09:00",
        "metric": {
            "tempAvg": 11.0,
            "temp": 15.0,
            "precipTotal": 1.2,
            "windspeedAvg": 7.0,
        },
    }
    result = readings_from_payload(_payload(item, item_with_temp), source="five_minute")
    assert len(result.readings) == 2
    reading = result.readings[0]
    assert reading.temp_c == 11.0
    assert reading.precip_total_mm == 1.2
    assert reading.speed_kmh == 7.0
    second_reading = result.readings[1]
    assert second_reading.temp_c == 11.0


# --- PR2 --------------------------------------------------------------


def test_pr2_no_tempavg_leaves_temperature_absent_unless_temp_present() -> None:
    """PR2 (14.3): without ``tempAvg`` or ``temp``, temperature is absent
    (never ``tempHigh``/``tempLow``); rain is kept regardless.

    mutant_reads_high_or_low -> at the first reading's ``temp_c``
    assertion: correct = None (no ``tempAvg``/``temp`` field at all),
    mutant (reading ``tempHigh`` or ``tempLow`` as the value) = 12.0 or
    10.0.
    """
    no_avg = {
        "valid_time_gmt": _epoch(10, 4),
        "metric": {"tempHigh": 12.0, "tempLow": 10.0, "precipTotal": 0.4},
    }
    plain_temp = {
        "valid_time_gmt": _epoch(10, 9),
        "metric": {"temp": 9.5},
    }
    result = readings_from_payload(_payload(no_avg, plain_temp), source="five_minute")
    assert len(result.readings) == 2
    first, second = result.readings
    assert first.temp_c is None
    assert first.precip_total_mm == 0.4
    assert second.temp_c == 9.5


# --- PR3 --------------------------------------------------------------


def test_pr3_one_bad_value_per_item_keeps_the_rest_of_that_reading() -> None:
    """PR3 (14.3): each bad temperature/rain/wind value is counted invalid
    and leaves the reading's other, good values; a reading with no good
    value at all is dropped.

    mutant_number_unreused -> at the per-item ``temp_c``/``precip_total_mm``
    assertions: correct = the good companion value survives every bad-value
    item, mutant (``_number``'s bool/nan/inf acceptance not reused, so a
    bad value is read as a number) = a finite float where None is expected.
    mutant_one_bad_drops_reading -> at the ``len(result.readings)``
    assertion: correct = 7 (each item with >=1 good value survives), mutant
    (one bad value drops the whole reading) = fewer than 7.
    """
    bad_temp_items = [
        {
            "valid_time_gmt": _epoch(0, 0),
            "metric": {"tempAvg": True, "precipTotal": 1.0},
        },
        {
            "valid_time_gmt": _epoch(0, 5),
            "metric": {"tempAvg": "nan", "precipTotal": 1.0},
        },
        {
            "valid_time_gmt": _epoch(0, 10),
            "metric": {"tempAvg": "inf", "precipTotal": 1.0},
        },
        {
            "valid_time_gmt": _epoch(0, 15),
            "metric": {"tempAvg": math.nan, "precipTotal": 1.0},
        },
        {
            "valid_time_gmt": _epoch(0, 20),
            "metric": {"tempAvg": "abc", "precipTotal": 1.0},
        },
    ]
    bad_rain_item = {
        "valid_time_gmt": _epoch(0, 25),
        "metric": {"precipTotal": -0.1, "tempAvg": 5.0},
    }
    bad_wind_item = {
        "valid_time_gmt": _epoch(0, 30),
        "metric": {"windspeedAvg": -1.0, "tempAvg": 5.0},
    }
    all_bad_item = {"valid_time_gmt": _epoch(0, 35), "metric": {"tempAvg": True}}

    result = readings_from_payload(
        _payload(*bad_temp_items, bad_rain_item, bad_wind_item, all_bad_item),
        source="five_minute",
    )
    assert len(result.readings) == 7
    for reading in result.readings[:5]:
        assert reading.temp_c is None
        assert reading.precip_total_mm == 1.0
    assert result.readings[5].temp_c == 5.0
    assert result.readings[5].precip_total_mm is None
    assert result.readings[6].temp_c == 5.0
    assert result.readings[6].speed_kmh is None
    assert result.dropped == 1
    assert result.unusable.get("temperature", 0) == 6
    assert result.unusable.get("precip", 0) == 1
    assert result.unusable.get("wind", 0) == 1


# --- PR4 --------------------------------------------------------------


def test_pr4_local_date_from_obstimelocal_then_validtimelocal_then_none() -> None:
    """PR4 (14.3): the reading's own local date prefers ``obsTimeLocal``,
    falls back to ``validTimeLocal``, and is NULL when neither is present
    -- never derived from the UTC instant.

    mutant_utc_fallback -> at the ``with_neither`` reading's ``local_date``
    assertion: correct = None (neither field is present), mutant (falling
    back to the UTC date of ``obs_at``) = date(2026, 10, 27) -- the
    fixture's third item carries no local-date field at all, and its UTC
    instant falls on a specific, different-looking date than the other two
    items' local dates, so the fallback is distinguishable.
    """
    with_obs_local = {
        "valid_time_gmt": _epoch(23, 49),
        "obsTimeLocal": "2026-10-27 23:49:00",
        "metric": {"tempAvg": 5.0},
    }
    with_valid_local_only = {
        "valid_time_gmt": _epoch(23, 54),
        "validTimeLocal": "2026-10-28 00:04:00",
        "metric": {"tempAvg": 5.0},
    }
    with_neither = {
        "valid_time_gmt": _epoch(23, 59),
        "metric": {"tempAvg": 5.0},
    }
    result = readings_from_payload(
        _payload(with_obs_local, with_valid_local_only, with_neither),
        source="five_minute",
    )
    first, second, third = result.readings
    assert first.local_date == date(2026, 10, 27)
    assert second.local_date == date(2026, 10, 28)
    assert third.local_date is None


# --- PR5 --------------------------------------------------------------


def test_pr5_sorted_first_of_duplicate_kept_bad_instant_dropped_no_raise() -> None:
    """PR5 (14.3): readings are sorted by instant; of two readings sharing
    one instant the FIRST in the payload is kept; an item with no usable
    instant is dropped and counted, and nothing raises.

    mutant_last_kept -> at the ``temp_c`` list assertion: correct = [1.0,
    20.0] (the first of the duplicate pair, ``dup_first``, wins), mutant
    (keeping the last of two equal instants) = [2.0, 20.0] -- the fixture's
    two same-instant items carry distinct temperatures so the two literals
    diverge.
    """
    later = {"valid_time_gmt": _epoch(10, 5), "metric": {"tempAvg": 20.0}}
    dup_first = {"valid_time_gmt": _epoch(10, 0), "metric": {"tempAvg": 1.0}}
    dup_second = {"valid_time_gmt": _epoch(10, 0), "metric": {"tempAvg": 2.0}}
    no_instant = {"metric": {"tempAvg": 5.0}}
    bool_instant = {"valid_time_gmt": True, "metric": {"tempAvg": 5.0}}

    result = readings_from_payload(
        _payload(later, dup_first, dup_second, no_instant, bool_instant),
        source="five_minute",
    )
    assert [r.temp_c for r in result.readings] == [1.0, 20.0]
    assert [r.obs_at for r in result.readings] == sorted(
        r.obs_at for r in result.readings
    )
    assert result.dropped == 3


# --- PR6 --------------------------------------------------------------


def test_pr6_current_obs_carries_local_date_and_instant_wind() -> None:
    """PR6 (14.3): ``current_obs_from_payload`` reads ``obsTimeLocal`` into
    the new ``local_date`` field, and ``metric.windSpeed`` as the reading's
    instant wind.

    mutant_ignores_local -> at the ``local_date`` assertion: correct =
    date(2026, 10, 27) (``obsTimeLocal`` read), mutant (the field ignored,
    ``local_date`` always None) = None.
    """
    payload = {
        "observations": [
            {
                "obsTimeUtc": "2026-10-27T23:49:00Z",
                "obsTimeLocal": "2026-10-27 15:49:00",
                "metric": {"temp": 9.0, "precipTotal": 0.6, "windSpeed": 4.0},
            }
        ]
    }
    result = current_obs_from_payload(payload)
    assert isinstance(result, CurrentObservation)
    assert result.local_date == date(2026, 10, 27)
    assert result.wind_speed == 4.0


# --- PR7 --------------------------------------------------------------


def test_pr7_hourly_history_and_payload_split_share_the_same_request() -> None:
    """PR7 (14.3): ``fetch_hourly_history_payload`` makes exactly the same
    request as ``fetch_hourly_history`` and drops nothing by age;
    ``fetch_hourly_history`` keeps its own age cutoff unchanged.

    mutant_payload_drops_by_age -> at the ``readings_from_payload`` length
    assertion: correct = 2 (the payload function returns both the recent
    and the old row untouched), mutant (``fetch_hourly_history_payload``
    applying the age cutoff itself) = 1, the old row dropped before
    ``readings_from_payload`` ever sees it.
    mutant_history_loses_cutoff -> at the ``fetch_hourly_history`` result
    assertion: correct = 1 (the old row is still filtered by age there),
    mutant (the age cutoff lost in the split) = 2.
    """
    station_id = "ISTATION03"
    recent = "2026-10-27T11:30:00Z"
    old = "2026-10-20T00:00:00Z"
    hourly_payload = {
        "observations": [
            {"obsTimeUtc": recent, "metric": {"tempAvg": 10.0}},
            {"obsTimeUtc": old, "metric": {"tempAvg": 5.0}},
        ]
    }
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            status_code=200,
            content=json.dumps(hourly_payload).encode(),
            headers={"content-type": "application/json"},
            request=request,
        )

    async def run() -> tuple[list[object], dict[str, object]]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch(
                "wxverify.obs.pws_adapter.utc_now",
                return_value=datetime(2026, 10, 27, 12, 0, 0, tzinfo=UTC),
            ):
                observations = await fetch_hourly_history(
                    station_id, _API_KEY, client=client
                )
            payload = await fetch_hourly_history_payload(
                station_id, _API_KEY, client=client
            )
        return observations, payload

    observations, payload = asyncio.run(run())

    assert len(captured) == 2
    for request in captured:
        assert request.url.path == "/v2/pws/observations/hourly/7day"
        assert dict(request.url.params) == {
            "stationId": station_id,
            "format": "json",
            "units": "m",
            "numericPrecision": "decimal",
            "apiKey": _API_KEY,
        }

    assert len(observations) == 1

    readings = readings_from_payload(payload, source="hourly")
    assert len(readings.readings) == 2
