"""Downstream consequences of the obs-204 Open-Meteo horizon change.

Covers the request-shape side (``OpenMeteoAdapter.fetch_forecast`` in
``wxverify/feeds/open_meteo.py``) and the display side
(``wxverify.core.timeutil.day_ahead`` vs. ``wxverify.forecast.aggregate.
display_day_index``) of raising each feed's ``max_lead_hours``.

The request-shape tests use ``httpx.MockTransport``, the same driver
``tests/test_open_meteo_metering.py`` uses -- no network call is ever made.

Synthetic data only: a ``site-alpha`` site at generic coordinates in
``Europe/Berlin``/UTC, and the product's own public Open-Meteo model
identifiers.
"""

from __future__ import annotations

import asyncio

import httpx

from wxverify.core.timeutil import day_ahead
from wxverify.feeds.open_meteo import OpenMeteoAdapter
from wxverify.feeds.seam import ForecastRequest


def _req(*, model: str, max_lead_hours: int) -> ForecastRequest:
    return ForecastRequest(
        lat=50.0,
        lon=10.0,
        model=model,
        variables=("temperature",),
        max_lead_hours=max_lead_hours,
    )


# ---------------------------------------------------------------------------
# 8. Request length
# ---------------------------------------------------------------------------


def _fetch_forecast_params(req: ForecastRequest) -> dict[str, str]:
    captured: dict[str, str] = {}

    def _capturing_handler(request: httpx.Request) -> httpx.Response:
        captured.update(request.url.params)
        return httpx.Response(
            200,
            json={"latitude": 50.0, "longitude": 10.0, "hourly": {"time": []}},
            request=request,
        )

    async def _drive() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_capturing_handler)
        ) as client:
            adapter = OpenMeteoAdapter(client)
            await adapter.fetch_forecast(req)

    asyncio.run(_drive())
    assert captured  # anti-vacuity: the transport was actually hit
    return captured


def test_fetch_forecast_sends_forecast_hours_equal_to_max_lead_hours() -> None:
    """The outgoing ``forecast_hours`` param is the feed's OWN
    ``max_lead_hours`` (unlike the historical path, ``fetch_forecast``
    applies no ``min(7, ...)`` cap): a capped feed (217) and
    ``icon_global`` (180, below the ceiling) both flow straight through.
    """
    capped = _fetch_forecast_params(_req(model="ecmwf_ifs", max_lead_hours=217))
    assert capped["forecast_hours"] == "217"

    icon = _fetch_forecast_params(_req(model="icon_global", max_lead_hours=180))
    assert icon["forecast_hours"] == "180"


# ---------------------------------------------------------------------------
# 9. Pairing window discriminates
# ---------------------------------------------------------------------------


def test_day_ahead_bucket_rule_holds_across_dst_transitions() -> None:
    """The lead-180h-vs-204h discriminator above (``day_ahead`` 7 vs. 8)
    replayed on a Europe/Berlin fall-back date (2026-10-25, a
    25-local-hour day) and a Europe/Berlin spring-forward date
    (2026-03-29, a 23-local-hour day), both issued 00:30 local. The
    bucket rule holds on both -- 180h stays at ``day_ahead`` == 7, 204h
    moves to ``day_ahead`` == 8 -- confirming it is a genuine local
    calendar-day computation, not a fixed-hour threshold a DST transition
    could perturb enough to reach the 217h/218h boundary documented as
    untestable above.
    """
    cases = [
        (
            "Europe/Berlin",
            "2026-10-24T22:30:00Z",  # local 2026-10-25T00:30+02:00 (fall-back day)
            "2026-11-01T10:30:00Z",  # +180h
            "2026-11-02T10:30:00Z",  # +204h
        ),
        (
            "Europe/Berlin",
            "2026-03-28T23:30:00Z",  # local 2026-03-29T00:30+01:00 (spring-forward day)
            "2026-04-05T11:30:00Z",  # +180h
            "2026-04-06T11:30:00Z",  # +204h
        ),
    ]
    for tz, issued_at, valid_180, valid_204 in cases:
        assert day_ahead(issued_at, valid_180, tz) == 7
        assert day_ahead(issued_at, valid_204, tz) == 8


# ---------------------------------------------------------------------------
# 14. Calendar cases
# ---------------------------------------------------------------------------


def test_day_ahead_fall_back_dst_day_does_not_overcount() -> None:
    """Europe/Berlin's autumn 2026 fall-back lands on 2026-10-25, a
    25-local-hour day (clocks step back 03:00 -> 02:00). A sample issued
    at that day's local midnight and valid exactly 24 elapsed hours later
    is still on the SAME local calendar day -- ``day_ahead`` must read 0,
    not 1. An elapsed-hours implementation (``lead_hours // 24``) would
    read 1 here, which is precisely the extra hour ``DISPLAY_REQUEST_
    HOURS`` (217, not 216) exists to buy back.
    """
    issued_at = "2026-10-24T22:00:00Z"  # local 2026-10-25T00:00:00+02:00
    valid_at = "2026-10-25T22:00:00Z"  # local 2026-10-25T23:00:00+01:00, same date
    assert day_ahead(issued_at, valid_at, "Europe/Berlin") == 0


def test_day_ahead_local_midnight_rollover_with_a_short_lead() -> None:
    """A sample valid just after local midnight, issued the previous local
    evening, crosses a calendar-day boundary on only 2 elapsed hours --
    ``day_ahead`` must read 1. An elapsed-hours implementation
    (``lead_hours // 24``) would read 0 here, since 2 hours is far short
    of a full day.
    """
    issued_at = "2026-01-01T22:00:00Z"  # local 2026-01-01T23:00:00+01:00
    valid_at = "2026-01-02T00:00:00Z"  # local 2026-01-02T01:00:00+01:00
    assert day_ahead(issued_at, valid_at, "Europe/Berlin") == 1
