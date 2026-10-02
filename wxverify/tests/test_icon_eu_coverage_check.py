"""Item D1 -- ``scripts/icon_eu_coverage_check.py`` (plan §8.3/§4.7).

The owner-run pre-release coverage check. Pure helpers are driven directly;
``main()`` is driven end to end through a ``MockTransport`` (never real
network -- ``tests/conftest.py``'s autouse ``_deny_network`` fixture would
fail any attempt anyway) against a tmp SQLite "Database Export" copy.

Synthetic data only: all coordinates below are open-ocean synthetic points
in the North Atlantic (in-box) or Southern Ocean (FAIL-box), never a real
station location.
"""

from __future__ import annotations

import ast
import asyncio
import logging
import re
import signal
import sqlite3
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from scripts.icon_eu_coverage_check import (
    FORWARD_NAMES,
    MODEL,
    PREVIOUS_RUN_NAMES,
    VARIABLES,
    Summary,
    Verdict,
    applicability,
    classify,
    exit_code,
    in_side_window,
    main,
    next_window_start,
    previous_runs_verdict,
    summarize,
    verdict,
)
from tests.test_011_patch import _init_tmp_db
from wxverify import config
from wxverify.config import OPEN_METEO_MAX_LEAD_HOURS
from wxverify.db.connection import FencedWriter, close_db, get_db
from wxverify.db.migrations import create_schema
from wxverify.feeds.open_meteo import OpenMeteoAdapter, _historical_hourly_names
from wxverify.feeds.seam import ForecastRequest, NormalizedSample
from wxverify.worker.backfill import SiteBackfillTarget, _fetch_historical_forecasts
from wxverify.worker.feed_fetch import fetch_feed_once

# ---------------------------------------------------------------------------
# Synthetic fixture coordinates (plan §8.3: in-box, open North Atlantic,
# distinctive six-decimal tails; a Southern Ocean FAIL pair with its own
# distinctive tails).
# ---------------------------------------------------------------------------

_IN_BOX_LAT = 46.728193
_IN_BOX_LON = -19.314827
_FAIL_LAT = -58.481936
_FAIL_LON = 11.902764

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("lat", "lon", "expected"),
    [
        (50.0, 0.0, Verdict.PASS),  # well inside the box
        (30.5, 0.0, Verdict.PASS),  # exactly on the PASS latitude corner
        (30.49, 0.0, Verdict.CANT_TELL),  # just inside the margin band
        (29.49, 0.0, Verdict.FAIL),  # just past the margin band
        (69.5, 0.0, Verdict.PASS),  # exactly on the opposite PASS latitude
        (70.51, 0.0, Verdict.FAIL),  # just past the FAIL latitude line
        (50.0, -22.5, Verdict.PASS),  # exactly on the PASS longitude corner
        (50.0, -23.51, Verdict.FAIL),  # just past the FAIL longitude line
        (50.0, 44.0, Verdict.PASS),  # exactly on the PASS longitude edge
        (50.0, 44.01, Verdict.CANT_TELL),  # just past it: margin band
        (50.0, 62.51, Verdict.FAIL),  # just past the FAIL longitude line
        (float("nan"), 0.0, Verdict.CANT_TELL),  # invalid, checked before FAIL
        (float("inf"), 0.0, Verdict.CANT_TELL),
        (float("-inf"), 0.0, Verdict.CANT_TELL),
        ("not-a-number", 0.0, Verdict.CANT_TELL),
        (None, 0.0, Verdict.CANT_TELL),
        (True, 0.0, Verdict.CANT_TELL),  # bool is not a number here
        (91.0, 0.0, Verdict.CANT_TELL),  # out-of-range latitude
        (0.0, 181.0, Verdict.CANT_TELL),  # out-of-range longitude
    ],
)
def test_applicability_table(lat: object, lon: object, expected: Verdict) -> None:
    assert applicability(lat, lon) == expected


@pytest.mark.parametrize(
    ("hour", "minute", "expected"),
    [
        (1, 0, True),  # window open
        (2, 29, True),  # last minute still inside
        (2, 30, False),  # exactly at the close -- half-open interval
        (0, 59, False),  # just before the window opens
        (7, 0, True),
        (13, 0, True),
        (19, 0, True),
        (20, 31, False),
    ],
)
def test_in_side_window_table(hour: int, minute: int, expected: bool) -> None:
    now = datetime(2026, 6, 2, hour, minute, tzinfo=UTC)
    assert in_side_window(now) is expected


def test_in_side_window_naive_matches_aware_under_a_non_utc_system_zone() -> None:
    # Plan §8.3: a naive `now` must be read as UTC, not local time. Under a
    # UTC CI system zone this would pass even with that bug, so the system
    # zone is forced away from UTC first (precedent test_daily_truth_discovery
    # .py:314-325).
    import os

    old_env_tz = os.environ.get("TZ")
    os.environ["TZ"] = "Etc/GMT+7"
    time.tzset()
    try:
        for minute_of_day in range(0, 24 * 60):
            hour, minute = divmod(minute_of_day, 60)
            aware = datetime(2026, 6, 2, hour, minute, tzinfo=UTC)
            naive = datetime(2026, 6, 2, hour, minute)
            assert in_side_window(naive) is in_side_window(aware), (hour, minute)
    finally:
        if old_env_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old_env_tz
        time.tzset()


def test_next_window_start_rolls_over_to_the_next_day() -> None:
    now = datetime(2026, 6, 2, 21, 0, tzinfo=UTC)
    assert next_window_start(now) == datetime(2026, 6, 3, 1, 0, tzinfo=UTC)


def test_next_window_start_within_the_same_day() -> None:
    now = datetime(2026, 6, 2, 2, 0, tzinfo=UTC)
    assert next_window_start(now) == datetime(2026, 6, 2, 7, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (429, (Verdict.CANT_TELL, "rate limited")),
        (404, (Verdict.FAIL, "http 4xx")),
        (499, (Verdict.FAIL, "http 4xx")),
        (301, (Verdict.CANT_TELL, "redirect")),
        (500, (Verdict.CANT_TELL, "http 5xx")),
        (503, (Verdict.CANT_TELL, "http 5xx")),
        (599, (Verdict.CANT_TELL, "http 5xx")),
        (100, (Verdict.CANT_TELL, "unexpected http status")),
        (102, (Verdict.CANT_TELL, "unexpected http status")),
        (199, (Verdict.CANT_TELL, "unexpected http status")),
        (600, (Verdict.CANT_TELL, "unexpected http status")),
        (999, (Verdict.CANT_TELL, "unexpected http status")),
    ],
)
def test_classify_http_status_table(status: int, expected: tuple[Verdict, str]) -> None:
    request = httpx.Request("GET", "https://api.open-meteo.com/v1/forecast")
    response = httpx.Response(status, request=request)
    error = httpx.HTTPStatusError("boom", request=request, response=response)
    assert classify(error) == expected


def test_classify_transport_error_is_cant_tell_network() -> None:
    assert classify(httpx.ConnectError("boom")) == (Verdict.CANT_TELL, "network")


def test_classify_other_exception_is_fail_adapter_rejected() -> None:
    assert classify(ValueError("boom")) == (Verdict.FAIL, "adapter rejected")


def _summary(**overrides: object) -> Summary:
    base: dict[str, object] = {
        "steps": 150,
        "nulls": dict.fromkeys(FORWARD_NAMES, 0),
        "interior_null": dict.fromkeys(FORWARD_NAMES, False),
        "first_valid": datetime(2026, 6, 2, 6, tzinfo=UTC),
        "last_valid": datetime(2026, 6, 8, 11, tzinfo=UTC),
        "first_time": datetime(2026, 6, 2, 6, tzinfo=UTC),
        "consecutive": True,
    }
    base.update(overrides)
    return Summary(**base)  # type: ignore[arg-type]


def test_verdict_r1_bad_shape_wins_over_everything_else() -> None:
    assert verdict(None, None, datetime(2026, 6, 2, 6, tzinfo=UTC)) == (
        Verdict.FAIL,
        "bad shape",
    )


def test_verdict_r4_wrong_times_checked_before_r2_too_few_steps() -> None:
    # Both defects present: the fixture as written gives steps far below the
    # 120-step floor AND a first_time that does not match fetch_hour. The
    # function must report "wrong times" (R4), not "too few steps" (R2),
    # proving R4 is checked first.
    bad = _summary(steps=5, first_time=datetime(2026, 6, 2, 9, tzinfo=UTC))
    assert verdict(None, bad, datetime(2026, 6, 2, 6, tzinfo=UTC)) == (
        Verdict.FAIL,
        "wrong times",
    )


def test_verdict_r2_too_few_steps() -> None:
    bad = _summary(steps=100)
    assert verdict(None, bad, datetime(2026, 6, 2, 6, tzinfo=UTC)) == (
        Verdict.FAIL,
        "too few steps",
    )


def test_verdict_r3a_too_few_values() -> None:
    bad = _summary(nulls={name: 60 for name in FORWARD_NAMES})
    assert verdict(None, bad, datetime(2026, 6, 2, 6, tzinfo=UTC)) == (
        Verdict.FAIL,
        "too few values",
    )


@pytest.mark.parametrize(
    ("steps", "nulls", "expected"),
    [
        (119, 0, (Verdict.FAIL, "too few steps")),
        (120, 0, (Verdict.PASS, "ok")),
        (120, 25, (Verdict.FAIL, "too few values")),
        (120, 24, (Verdict.PASS, "ok")),
    ],
)
def test_verdict_r2_r3_exact_boundaries(
    steps: int, nulls: int, expected: tuple[Verdict, str]
) -> None:
    """``MIN_FORWARD_STEPS`` (120) and ``MIN_FORWARD_VALUES`` (96) at their
    exact off-by-one edges. Fixed literal counts (119/120, 24/25), not
    derived from the production thresholds, so a mutant that moves either
    cannot resize this fixture along with it.
    """
    fetch_hour = datetime(2026, 6, 2, 6, tzinfo=UTC)
    bad = _summary(steps=steps, nulls={name: nulls for name in FORWARD_NAMES})
    assert verdict(None, bad, fetch_hour) == expected


def test_verdict_r3b_gap_inside_data_checked_after_too_few_values() -> None:
    good_nulls = dict.fromkeys(FORWARD_NAMES, 0)
    gaps = {name: True for name in FORWARD_NAMES}
    bad = _summary(nulls=good_nulls, interior_null=gaps)
    assert verdict(None, bad, datetime(2026, 6, 2, 6, tzinfo=UTC)) == (
        Verdict.FAIL,
        "gap inside data",
    )


def test_verdict_r3_order_count_checked_before_gap_across_names() -> None:
    # R3 must check EVERY name's count before checking any name's gap. With
    # 120 steps: temperature_2m has one interior null and 119 non-null (count
    # passes, 119 >= 96); precipitation has 25 trailing nulls, 95 non-null, no
    # interior null (count fails, 95 < 96). A mutant that interleaves count
    # and gap per name would report "gap inside data" (from temperature_2m,
    # walked first) instead of "too few values" (from precipitation).
    nulls = {name: 0 for name in FORWARD_NAMES}
    interior_null = dict.fromkeys(FORWARD_NAMES, False)
    for name in FORWARD_NAMES:
        if "temperature" in name:
            nulls[name] = 1
            interior_null[name] = True
        elif "precipitation" in name:
            nulls[name] = 25
    bad = _summary(steps=120, nulls=nulls, interior_null=interior_null)
    assert verdict(None, bad, datetime(2026, 6, 2, 6, tzinfo=UTC)) == (
        Verdict.FAIL,
        "too few values",
    )


def test_verdict_pass_ok() -> None:
    good = _summary()
    assert verdict(None, good, datetime(2026, 6, 2, 6, tzinfo=UTC)) == (
        Verdict.PASS,
        "ok",
    )


def test_verdict_an_exception_outranks_a_present_summary() -> None:
    good = _summary()
    error = httpx.ConnectError("boom")
    assert verdict(error, good, datetime(2026, 6, 2, 6, tzinfo=UTC)) == (
        Verdict.CANT_TELL,
        "network",
    )


def _forward_hourly_body(
    *, start: datetime, hours: int, hour_offsets: list[timedelta] | None = None
) -> dict[str, object]:
    """A forward body whose ``hourly.time`` entries are ``start`` plus one
    hour per step, each additionally shifted by ``hour_offsets[i]`` (default
    no shift)."""
    offsets = hour_offsets or [timedelta(0)] * hours
    times = [
        (start + timedelta(hours=i) + offsets[i]).isoformat() for i in range(hours)
    ]
    return {
        "latitude": 0.0,
        "longitude": 0.0,
        "hourly": {
            "time": times,
            **{name: [1.0] * hours for name in FORWARD_NAMES},
        },
    }


def test_verdict_whole_hours_half_second_shift_on_every_entry_fails_wrong_times() -> (
    None
):
    # Moving every entry by the same half second keeps the 1h step and keeps
    # entry 0 within 1h of fetch_hour -- only the microsecond clause of the
    # whole-hour check can catch it.
    fetch_hour = datetime(2026, 6, 2, 6, tzinfo=UTC)
    offsets = [timedelta(microseconds=500000)] * 150
    body = _forward_hourly_body(start=fetch_hour, hours=150, hour_offsets=offsets)
    assert verdict(None, summarize(body), fetch_hour) == (Verdict.FAIL, "wrong times")


@pytest.mark.parametrize(
    ("offset_hours", "expected"),
    [
        (1, Verdict.PASS),
        (2, Verdict.FAIL),
        (-1, Verdict.PASS),
        (-2, Verdict.FAIL),
    ],
)
def test_verdict_r4_entry_zero_distance_from_fetch_hour(
    offset_hours: int, expected: Verdict
) -> None:
    fetch_hour = datetime(2026, 6, 2, 6, tzinfo=UTC)
    start = fetch_hour + timedelta(hours=offset_hours)
    body = _forward_hourly_body(start=start, hours=150)
    result, reason = verdict(None, summarize(body), fetch_hour)
    assert result == expected
    if expected is Verdict.FAIL:
        assert reason == "wrong times"


def test_verdict_r4_one_skipped_hour_fails_wrong_times() -> None:
    fetch_hour = datetime(2026, 6, 2, 6, tzinfo=UTC)
    offsets = [timedelta(0)] * 150
    for i in range(60, 150):
        offsets[i] = timedelta(hours=1)  # every entry from 60 on shifted +1h
    body = _forward_hourly_body(start=fetch_hour, hours=150, hour_offsets=offsets)
    assert verdict(None, summarize(body), fetch_hour) == (Verdict.FAIL, "wrong times")


def test_verdict_r4_an_unparseable_entry_fails_wrong_times() -> None:
    fetch_hour = datetime(2026, 6, 2, 6, tzinfo=UTC)
    body = _forward_hourly_body(start=fetch_hour, hours=150)
    hourly = body["hourly"]
    assert isinstance(hourly, dict)
    times = hourly["time"]
    assert isinstance(times, list)
    times[10] = "not-a-timestamp"
    assert verdict(None, summarize(body), fetch_hour) == (Verdict.FAIL, "wrong times")


def test_verdict_r4_three_hourly_axis_fails_wrong_times_not_too_few_steps() -> None:
    # 40 steps at a 3-hourly cadence: far below the 120-step floor (R2), but
    # R4 (not 1h steps) must be checked -- and fail -- first.
    fetch_hour = datetime(2026, 6, 2, 6, tzinfo=UTC)
    times = [(fetch_hour + timedelta(hours=3 * i)).isoformat() for i in range(40)]
    body: dict[str, object] = {
        "latitude": 0.0,
        "longitude": 0.0,
        "hourly": {
            "time": times,
            **{name: [1.0] * 40 for name in FORWARD_NAMES},
        },
    }
    assert verdict(None, summarize(body), fetch_hour) == (Verdict.FAIL, "wrong times")


# ---------------------------------------------------------------------------
# previous_runs_verdict: PR1-PR4 ordering. Samples are driven through the
# real adapter path (MockTransport -> OpenMeteoAdapter.fetch_historical),
# never hand-built, so the normalization is production's (plan line 659).
# ---------------------------------------------------------------------------


def _pr_payload(day: date, *, rows: int = 48, **overrides: object) -> dict[str, object]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    times = [(start + timedelta(hours=i)).isoformat() for i in range(rows)]
    hourly: dict[str, object] = {"time": times}
    units: dict[str, object] = {"time": "iso8601"}
    for name in PREVIOUS_RUN_NAMES:
        hourly[name] = [1.0] * rows
        provider = name.rsplit("_previous_day", 1)[0]
        units[name] = (
            "°C"
            if provider == "temperature_2m"
            else "km/h"
            if provider == "wind_speed_10m"
            else "mm"
        )
    body: dict[str, object] = {
        "latitude": _IN_BOX_LAT,
        "longitude": _IN_BOX_LON,
        "hourly": hourly,
        "hourly_units": units,
    }
    body.update(overrides)
    return body


def test_previous_runs_pr1_bad_shape() -> None:
    assert previous_runs_verdict({}, [], date(2026, 6, 1)) == (
        Verdict.FAIL,
        "bad shape",
    )


def test_previous_runs_pr2_wrong_times() -> None:
    day = date(2026, 6, 1)
    payload = _pr_payload(day, rows=47)  # one row short, symmetrically
    assert previous_runs_verdict(payload, [], day) == (Verdict.FAIL, "wrong times")


def test_previous_runs_pr3_wrong_units_checked_before_pr4_values() -> None:
    day = date(2026, 6, 1)
    payload = _pr_payload(day)
    units = payload["hourly_units"]
    assert isinstance(units, dict)
    units["temperature_2m_previous_day1"] = "K"  # wrong unit
    # No samples at all would also fail PR4 ("too few values"); PR3 must win.
    assert previous_runs_verdict(payload, [], day) == (Verdict.FAIL, "wrong units")


def _historical_samples_via_adapter(
    day: date, *, body_overrides: dict[str, object] | None = None
) -> list[NormalizedSample]:
    """Drive a synthetic 200 through the real adapter path, returning the
    samples the adapter actually normalized (plan line 659)."""
    payload = _pr_payload(day)
    if body_overrides:
        payload = {**payload, **body_overrides}

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload, request=request)

    async def _run() -> list[NormalizedSample]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(_handler)) as client:
            adapter = OpenMeteoAdapter(client)
            req = ForecastRequest(
                lat=_IN_BOX_LAT,
                lon=_IN_BOX_LON,
                model=MODEL,
                variables=VARIABLES,
                max_lead_hours=OPEN_METEO_MAX_LEAD_HOURS[MODEL],
            )
            result = await adapter.fetch_historical(
                req,
                window_start=f"{day}T00:00:00Z",
                window_end=f"{day + timedelta(days=1)}T00:00:00Z",
            )
            assert result is not None
            return list(result.samples)

    return asyncio.run(_run())


def test_previous_runs_implausible_value_temperature() -> None:
    day = date(2026, 6, 1)
    samples = _historical_samples_via_adapter(
        day,
        body_overrides={
            "hourly": {
                **_pr_payload(day)["hourly"],  # type: ignore[dict-item]
                "temperature_2m_previous_day1": [71.0] * 48,
            }
        },
    )
    payload = _pr_payload(day)
    assert previous_runs_verdict(payload, samples, day) == (
        Verdict.FAIL,
        "implausible value",
    )


def test_previous_runs_too_few_values_when_a_required_day_is_missing() -> None:
    day = date(2026, 6, 1)
    payload = _pr_payload(day)
    assert previous_runs_verdict(payload, [], day) == (
        Verdict.FAIL,
        "too few values",
    )


def test_previous_runs_pass_with_every_required_sample_present() -> None:
    day = date(2026, 6, 1)
    samples = _historical_samples_via_adapter(day)
    payload = _pr_payload(day)
    assert previous_runs_verdict(payload, samples, day) == (Verdict.PASS, "ok")


def test_previous_runs_pass_with_day_4_null_and_day_5_all_null() -> None:
    # Days 4 and 5 are not required (_REQUIRED_PREVIOUS_DAYS = (1, 2, 3)): a
    # null day 4 and an entirely-null day 5 must still PASS.
    day = date(2026, 6, 1)
    hourly_overrides: dict[str, object] = dict(_pr_payload(day)["hourly"])  # type: ignore[arg-type]
    for name in PREVIOUS_RUN_NAMES:
        if name.endswith("_previous_day4"):
            values = list(hourly_overrides[name])  # type: ignore[arg-type]
            values[0] = None
            hourly_overrides[name] = values
        if name.endswith("_previous_day5"):
            hourly_overrides[name] = [None] * 48
    samples = _historical_samples_via_adapter(
        day, body_overrides={"hourly": hourly_overrides}
    )
    payload = _pr_payload(day)
    assert previous_runs_verdict(payload, samples, day) == (Verdict.PASS, "ok")


def test_previous_runs_wind_540_kmh_passes_at_the_exact_bound() -> None:
    # 540 km/h == 150.0 m/s exactly, the FORECAST_VALUE_RANGES upper bound.
    day = date(2026, 6, 1)
    samples = _historical_samples_via_adapter(
        day,
        body_overrides={
            "hourly": {
                **_pr_payload(day)["hourly"],  # type: ignore[dict-item]
                "wind_speed_10m_previous_day1": [540.0] * 48,
            }
        },
    )
    payload = _pr_payload(day)
    assert previous_runs_verdict(payload, samples, day) == (Verdict.PASS, "ok")


def test_previous_runs_wind_541_kmh_fails_implausible_value() -> None:
    day = date(2026, 6, 1)
    samples = _historical_samples_via_adapter(
        day,
        body_overrides={
            "hourly": {
                **_pr_payload(day)["hourly"],  # type: ignore[dict-item]
                "wind_speed_10m_previous_day1": [541.0] * 48,
            }
        },
    )
    payload = _pr_payload(day)
    assert previous_runs_verdict(payload, samples, day) == (
        Verdict.FAIL,
        "implausible value",
    )


def test_previous_runs_precip_minus_point_05_passes_floored_to_zero() -> None:
    # Within TRACE_NEGATIVE_PRECIP_MIN (-0.1, 0): floored to 0.0 -> PASS.
    day = date(2026, 6, 1)
    samples = _historical_samples_via_adapter(
        day,
        body_overrides={
            "hourly": {
                **_pr_payload(day)["hourly"],  # type: ignore[dict-item]
                "precipitation_previous_day1": [-0.05] * 48,
            }
        },
    )
    payload = _pr_payload(day)
    assert previous_runs_verdict(payload, samples, day) == (Verdict.PASS, "ok")


def test_previous_runs_precip_minus_point_2_fails_implausible_value() -> None:
    # A mutant that checks value bounds before normalization would floor
    # this too; the real path leaves it below -0.1, so it stays negative and
    # falls outside [0, 500].
    day = date(2026, 6, 1)
    samples = _historical_samples_via_adapter(
        day,
        body_overrides={
            "hourly": {
                **_pr_payload(day)["hourly"],  # type: ignore[dict-item]
                "precipitation_previous_day1": [-0.2] * 48,
            }
        },
    )
    payload = _pr_payload(day)
    assert previous_runs_verdict(payload, samples, day) == (
        Verdict.FAIL,
        "implausible value",
    )


@pytest.mark.parametrize(
    ("verdicts", "expected"),
    [
        ([Verdict.PASS], 0),
        ([Verdict.PASS, Verdict.CANT_TELL], 3),
        ([Verdict.PASS, Verdict.FAIL], 1),
        ([Verdict.FAIL, Verdict.CANT_TELL], 1),  # FAIL wins over CAN'T TELL
        ([], 0),
    ],
)
def test_exit_code_worst_of(verdicts: list[Verdict], expected: int) -> None:
    assert exit_code(verdicts) == expected


def test_previous_run_names_match_icon_eus_adapter_names_exactly() -> None:
    # Full tuple equality, not just length: the script's own copy must match
    # the adapter's public-name builder element for element, in order.
    req = ForecastRequest(
        lat=0.0,
        lon=0.0,
        model="icon_eu",
        variables=("temperature", "wind", "precip"),
        max_lead_hours=OPEN_METEO_MAX_LEAD_HOURS["icon_eu"],
    )
    expected = tuple(_historical_hourly_names(req))
    assert len(expected) == len(VARIABLES) * 5
    assert expected == PREVIOUS_RUN_NAMES


# ---------------------------------------------------------------------------
# Import allowlist (§4.7): stdlib + httpx + exactly five wxverify modules.
# ---------------------------------------------------------------------------

_ALLOWED_WXVERIFY_MODULES = {
    "wxverify.config",
    "wxverify.feeds.open_meteo",
    "wxverify.feeds.seam",
    "wxverify.core.timeutil",
    "wxverify.collection.forecast_validation",
}


def test_import_allowlist_static() -> None:
    source = Path("scripts/icon_eu_coverage_check.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    wxverify_imports: set[str] = set()
    other_top_level: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module.startswith("wxverify"):
                wxverify_imports.add(node.module)
            else:
                other_top_level.add(node.module.split(".")[0])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("wxverify"):
                    wxverify_imports.add(alias.name)
                else:
                    other_top_level.add(alias.name.split(".")[0])
    assert wxverify_imports == _ALLOWED_WXVERIFY_MODULES
    import sys

    assert other_top_level <= (set(sys.stdlib_module_names) | {"httpx"})


#: §4.7's complete transitive closure: the five allowed modules plus
#: everything they are documented to pull in. A baseline-subtraction oracle
#: (pre-importing the five allowed modules, then diffing sys.modules before
#: and after the script import) cannot see a module one of THOSE modules
#: itself transitively loads -- exactly what this plan line pins. This is a
#: subset check instead: only the script is imported, and everything it
#: brings into sys.modules must fall inside this literal list.
_ALLOWED_TRANSITIVE_MODULES = {
    "wxverify",
    "wxverify.config",
    "wxverify.feeds",
    "wxverify.feeds.open_meteo",
    "wxverify.feeds.seam",
    "wxverify.core",
    "wxverify.core.timeutil",
    "wxverify.core.units",
    "wxverify.collection",
    "wxverify.collection.forecast_validation",
    "wxverify.settings",
    "wxverify.settings.depth",
    "wxverify.settings.keys",
}

_FORBIDDEN_SUBTREES = (
    "wxverify.worker",
    "wxverify.db",
    "wxverify.collection.budget",
    "wxverify.api",
)


def test_import_allowlist_runtime(tmp_path: Path) -> None:
    import ast as ast_module
    import os
    import subprocess
    import sys

    probe = tmp_path / "probe.py"
    probe.write_text(
        "import sys\n"
        "import scripts.icon_eu_coverage_check  # noqa: F401\n"
        "loaded = sorted(\n"
        "    m for m in sys.modules\n"
        "    if m == 'wxverify' or m.startswith('wxverify.')\n"
        ")\n"
        "print(loaded)\n",
        encoding="utf-8",
    )
    env = dict(os.environ, PYTHONPATH=str(Path.cwd()))
    result = subprocess.run(
        [sys.executable, str(probe)],
        cwd=Path.cwd(),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    loaded = set(ast_module.literal_eval(result.stdout.strip()))
    # Everything the script's import brings into sys.modules must be inside
    # the §4.7 transitive closure -- this sees a module pulled in through
    # one of the ALLOWED modules too, unlike a baseline-subtraction diff.
    assert loaded <= _ALLOWED_TRANSITIVE_MODULES
    for module in loaded:
        for forbidden in _FORBIDDEN_SUBTREES:
            assert module != forbidden
            assert not module.startswith(forbidden + ".")


# ---------------------------------------------------------------------------
# D1-T6: the forward request equals production's.
# ---------------------------------------------------------------------------


def test_d1_t6_forward_request_matches_production_fetch_feed_once(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES ('In-Box Synthetic', ?, ?, 0.0, 'UTC', 1)",
        (_IN_BOX_LAT, _IN_BOX_LON),
    )
    conn.commit()
    site_id = int(conn.execute("SELECT id FROM sites").fetchone()["id"])
    feed_id = int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' AND model='icon_eu'"
        ).fetchone()["id"]
    )

    now = _in_window_now()
    prod_request: httpx.Request | None = None

    def _prod_handler(request: httpx.Request) -> httpx.Response:
        nonlocal prod_request
        prod_request = request
        return httpx.Response(200, json=_forward_body(now), request=request)

    async def _run_production() -> None:
        db = get_db()
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_prod_handler)
        ) as mock_client:
            await fetch_feed_once(
                db,
                site_id,
                feed_id,
                adapter_builder=lambda _source, _c: OpenMeteoAdapter(mock_client),
            )

    asyncio.run(_run_production())
    assert prod_request is not None
    close_db()

    script_request: httpx.Request | None = None

    def _script_handler(request: httpx.Request) -> httpx.Response:
        nonlocal script_request
        if "previous-runs-api" in str(request.url):
            day = now.date() - timedelta(days=2)
            return httpx.Response(200, json=_historical_body(day), request=request)
        script_request = request
        return httpx.Response(200, json=_forward_body(now), request=request)

    # The script side needs its own tmp db file (close_db() above detached
    # the previous one, but the script reads by path, not through Database).
    script_db = tmp_path / "wxverify.db"
    code = main(
        ["--db", str(script_db), "--site-id", str(site_id)],
        transport=httpx.MockTransport(_script_handler),
        now=now,
    )
    assert code == 0
    assert script_request is not None

    assert script_request.url.scheme == prod_request.url.scheme == "https"
    assert script_request.url.host == prod_request.url.host == "api.open-meteo.com"
    assert script_request.url.path == prod_request.url.path
    assert sorted(script_request.url.params.multi_items()) == sorted(
        prod_request.url.params.multi_items()
    )
    expected_timeout = {"connect": 5.0, "read": 10.0, "write": 10.0, "pool": 10.0}
    assert script_request.extensions["timeout"] == expected_timeout
    assert prod_request.extensions["timeout"] == expected_timeout

    params = dict(script_request.url.params)
    assert params["models"] == "icon_eu"
    assert params["hourly"] == "temperature_2m,wind_speed_10m,precipitation"
    assert params["timezone"] == "UTC"
    assert params["forecast_hours"] == "120"


# ---------------------------------------------------------------------------
# D1-T8: the previous-runs request equals setup backfill's, and the script
# changes nothing.
# ---------------------------------------------------------------------------


def _hash_file(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_d1_t8_previous_runs_request_matches_backfill_and_writes_nothing(
    tmp_path: Path,
) -> None:
    script_dir = tmp_path / "script"
    prod_dir = tmp_path / "prod"
    script_dir.mkdir()
    prod_dir.mkdir()

    # --- Script database first ---
    conn = _init_tmp_db(script_dir)
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES ('In-Box Synthetic', ?, ?, 0.0, 'UTC', 1)",
        (_IN_BOX_LAT, _IN_BOX_LON),
    )
    conn.commit()
    site_id = int(conn.execute("SELECT id FROM sites").fetchone()["id"])
    from tests.test_open_meteo_metering import _isolate_open_meteo_feed

    _isolate_open_meteo_feed(conn, site_id, "icon_eu")
    conn.commit()
    close_db()

    script_db_path = Path(config.db_path)
    before_hash = _hash_file(script_db_path)
    ro = sqlite3.connect(script_db_path.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        before_jobs = ro.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        before_budget = ro.execute("SELECT COUNT(*) FROM api_budget").fetchone()[0]
        before_samples = ro.execute("SELECT COUNT(*) FROM forecast_samples").fetchone()[
            0
        ]
        before_status = ro.execute(
            "SELECT backfill_status FROM sites WHERE id = ?", (site_id,)
        ).fetchone()[0]
    finally:
        ro.close()

    now = _in_window_now()
    day = now.date() - timedelta(days=2)

    prod_request: httpx.Request | None = None

    def _prod_handler(request: httpx.Request) -> httpx.Response:
        nonlocal prod_request
        prod_request = request
        return httpx.Response(200, json=_historical_body(day), request=request)

    # --- Production database second ---
    conn2 = _init_tmp_db(prod_dir)
    conn2.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES ('In-Box Synthetic', ?, ?, 0.0, 'UTC', 1)",
        (_IN_BOX_LAT, _IN_BOX_LON),
    )
    conn2.commit()
    prod_site_id = int(conn2.execute("SELECT id FROM sites").fetchone()["id"])
    _isolate_open_meteo_feed(conn2, prod_site_id, "icon_eu")
    conn2.commit()
    db = get_db()
    writer = FencedWriter(db, db.generation)

    import wxverify.worker.backfill as backfill_module

    original_build_adapter = backfill_module.build_adapter

    async def _run_production() -> None:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(_prod_handler)
        ) as mock_client:
            backfill_module.build_adapter = lambda _source, _c: OpenMeteoAdapter(
                mock_client
            )
            try:
                target = SiteBackfillTarget(
                    site_id=prod_site_id,
                    lat=0.0,
                    lon=0.0,
                    timezone="UTC",
                    backfill_status="pending",
                    backfill_through=None,
                )
                await _fetch_historical_forecasts(
                    db,
                    writer,
                    target,
                    window_start=f"{day}T00:00:00Z",
                    window_end=f"{day + timedelta(days=1)}T00:00:00Z",
                )
            finally:
                backfill_module.build_adapter = original_build_adapter

    asyncio.run(_run_production())
    assert prod_request is not None
    close_db()

    # --- Script side ---
    forward_calls = 0
    previous_calls = 0
    script_previous_request: httpx.Request | None = None

    def _script_handler(request: httpx.Request) -> httpx.Response:
        nonlocal forward_calls, previous_calls, script_previous_request
        if "previous-runs-api" in str(request.url):
            previous_calls += 1
            script_previous_request = request
            return httpx.Response(200, json=_historical_body(day), request=request)
        forward_calls += 1
        return httpx.Response(200, json=_forward_body(now), request=request)

    code = main(
        ["--db", str(script_db_path), "--site-id", str(site_id)],
        transport=httpx.MockTransport(_script_handler),
        now=now,
    )
    assert code == 0
    assert forward_calls == 1
    assert previous_calls == 1
    assert script_previous_request is not None
    assert prod_request is not None

    assert script_previous_request.url.scheme == prod_request.url.scheme
    assert script_previous_request.url.host == prod_request.url.host
    assert script_previous_request.url.host == "previous-runs-api.open-meteo.com"
    assert script_previous_request.url.path == prod_request.url.path
    assert sorted(script_previous_request.url.params.multi_items()) == sorted(
        prod_request.url.params.multi_items()
    )
    expected_timeout = {"connect": 5.0, "read": 15.0, "write": 15.0, "pool": 15.0}
    assert script_previous_request.extensions["timeout"] == expected_timeout
    assert prod_request.extensions["timeout"] == expected_timeout

    # Nothing changed on the script's own database.
    assert _hash_file(script_db_path) == before_hash
    ro2 = sqlite3.connect(script_db_path.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        assert ro2.execute("SELECT COUNT(*) FROM jobs").fetchone()[0] == before_jobs
        assert (
            ro2.execute("SELECT COUNT(*) FROM api_budget").fetchone()[0]
            == before_budget
        )
        assert (
            ro2.execute("SELECT COUNT(*) FROM forecast_samples").fetchone()[0]
            == before_samples
        )
        assert (
            ro2.execute(
                "SELECT backfill_status FROM sites WHERE id = ?", (site_id,)
            ).fetchone()[0]
            == before_status
        )
    finally:
        ro2.close()


# ---------------------------------------------------------------------------
# End-to-end main(): MockTransport, tmp "Database Export" copy, no writes.
# ---------------------------------------------------------------------------

_MARKER_DIR_NAME = "d1-t7-marker-zzqy"

_REASON = (
    "ok|bad shape|wrong times|wrong units|too few steps|too few values"
    "|implausible value|gap inside data|http 4xx|rate limited|http 5xx"
    "|unexpected http status|redirect|network|adapter rejected|interrupted"
)

_OUTPUT_TEMPLATES = [
    re.compile(p)
    for p in (
        r"applicability: (PASS|FAIL|CAN'T TELL)",
        r"forward steps: \d+",
        r"forward first time: (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}|none)",
        r"forward nulls (temperature_2m|wind_speed_10m|precipitation): \d+",
        r"forward first valid time: (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}|none)",
        r"forward last valid time: (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}|none)",
        rf"forward verdict: (PASS|FAIL|CAN'T TELL) \(({_REASON})\)",
        r"previous-runs steps: \d+",
        r"previous-runs non-null "
        r"(temperature_2m|wind_speed_10m|precipitation)_previous_day[1-5]: \d+",
        rf"previous-runs verdict: (PASS|FAIL|CAN'T TELL) \(({_REASON})\)",
        r"error: database unreadable",
        r"error: no enabled site",
        r"error: \d+ enabled sites; pass --site-id",
        r"error: site not found or disabled",
        r"error: outside the test window; next window starts "
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2} UTC",
    )
]


def _assert_allowed_output(text: str) -> None:
    for line in text.splitlines():
        if not line:
            continue
        assert any(template.fullmatch(line) for template in _OUTPUT_TEMPLATES), line


#: Two additional finite margin-band coordinates (R2): a latitude margin
#: with a normal longitude, and longitude margins on both the east and west
#: edges of the PASS box, each paired with an in-box latitude.
_MARGIN_LAT_1 = 30.071482
_MARGIN_LON_1 = -19.314827
_MARGIN_LAT_2 = 46.728193
_MARGIN_LON_2 = 51.604183
_MARGIN_LON_3 = -22.917364


def _truncated_2dp(coord: float) -> str:
    """The integer part followed by the first two decimals, truncated (not
    rounded) -- e.g. ``46.72`` for ``46.728193``."""
    text = repr(coord)
    dot = text.index(".")
    return text[: dot + 3]


def _assert_no_leak(text: str) -> None:
    assert "://" not in text
    assert "open-meteo.com" not in text
    assert "latitude" not in text
    assert _MARKER_DIR_NAME not in text
    for coord in (
        _IN_BOX_LAT,
        _IN_BOX_LON,
        _FAIL_LAT,
        _FAIL_LON,
        _MARGIN_LAT_1,
        _MARGIN_LON_1,
        _MARGIN_LAT_2,
        _MARGIN_LON_2,
        _MARGIN_LON_3,
    ):
        assert repr(coord) not in text
        assert str(coord) not in text
        assert f"{coord:.2f}" not in text
        assert f"{coord:.4f}" not in text
        assert _truncated_2dp(coord) not in text


def _run_main_checked(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    now: datetime | None = None,
) -> tuple[int, str, str]:
    """Run main() inside a DEBUG capture, asserting no record leaks and the
    disable level is restored, that KeyboardInterrupt never escapes, and that
    BOTH stdout and stderr obey the output allowlist and leak checks."""
    with caplog.at_level(logging.DEBUG):
        before = logging.root.manager.disable
        caplog.clear()
        try:
            code = main(argv, transport=transport, now=now)
        except KeyboardInterrupt:
            pytest.fail("KeyboardInterrupt escaped main")
        assert caplog.records == []
        assert logging.root.manager.disable == before
    out, err = capsys.readouterr()
    _assert_allowed_output(out)
    _assert_allowed_output(err)
    _assert_no_leak(out)
    _assert_no_leak(err)
    return code, out, err


def test_positive_control_adapter_debug_line_is_visible_under_at_level(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Proves the capture mechanism actually sees the adapter's DEBUG line
    # outside of main()'s own logging.disable -- otherwise the "no leak"
    # assertion above would pass vacuously even if the suppression broke.
    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json=_forward_body(_in_window_now()), request=request
        )

    with caplog.at_level(logging.DEBUG):
        caplog.clear()

        async def _run() -> None:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(_handler)
            ) as client:
                adapter = OpenMeteoAdapter(client)
                req = ForecastRequest(
                    lat=_IN_BOX_LAT,
                    lon=_IN_BOX_LON,
                    model=MODEL,
                    variables=VARIABLES,
                    max_lead_hours=OPEN_METEO_MAX_LEAD_HOURS[MODEL],
                )
                await adapter.fetch_forecast(req)

        asyncio.run(_run())
        records = [
            r
            for r in caplog.records
            if r.name == "wxverify.feeds.open_meteo" and r.levelno == logging.DEBUG
        ]
        assert any("lat=" in r.getMessage() for r in records)


def _export_db(path: Path, *, lat: float, lon: float, enabled: int = 1) -> None:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES ('Export Site', ?, ?, 0.0, 'UTC', ?)",
        (lat, lon, enabled),
    )
    conn.commit()
    conn.close()


def _forward_body(now: datetime) -> dict[str, object]:
    start = now.replace(minute=0, second=0, microsecond=0)
    times = [(start + timedelta(hours=i)).isoformat() for i in range(150)]
    return {
        "latitude": 0.0,
        "longitude": 0.0,
        "hourly": {
            "time": times,
            **{name: [1.0] * 150 for name in FORWARD_NAMES},
        },
    }


def _historical_body(day: date) -> dict[str, object]:
    return _pr_payload(day)


def _in_window_now() -> datetime:
    return datetime(2026, 6, 2, 1, 15, tzinfo=UTC)


def _marker_db_path(tmp_path: Path) -> Path:
    marker_dir = tmp_path / _MARKER_DIR_NAME
    marker_dir.mkdir()
    return marker_dir / "export.db"


def test_main_pass_path_hits_every_rule_and_touches_no_db_row(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    before = db_path.read_bytes()
    now = _in_window_now()
    day = now.date() - timedelta(days=2)

    def _handler(request: httpx.Request) -> httpx.Response:
        if "previous-runs-api" in str(request.url):
            return httpx.Response(200, json=_historical_body(day), request=request)
        return httpx.Response(200, json=_forward_body(now), request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=now,
    )
    assert code == 0
    assert "applicability: PASS" in out
    assert f"forward verdict: {Verdict.PASS} (ok)" in out
    assert f"previous-runs verdict: {Verdict.PASS} (ok)" in out
    # Read-only export: not one byte changed.
    assert db_path.read_bytes() == before


def test_main_applicability_fail_sends_no_request(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_FAIL_LAT, lon=_FAIL_LON)  # Southern Ocean: FAIL
    calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"hourly": {"time": []}}, request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=_in_window_now(),
    )
    assert code == 1
    assert calls == 0
    assert "applicability: FAIL" in out
    assert out.strip() == "applicability: FAIL"


@pytest.mark.parametrize("bad_lat", [float("inf"), "not-a-number"])
def test_main_applicability_cant_tell_sends_no_request(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    bad_lat: object,
) -> None:
    db_path = _marker_db_path(tmp_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES ('Export Site', ?, ?, 0.0, 'UTC', 1)",
        (bad_lat, 0.0),
    )
    conn.commit()
    conn.close()
    calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"hourly": {"time": []}}, request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=_in_window_now(),
    )
    assert code == 3
    assert calls == 0
    assert "applicability: CAN'T TELL" in out


@pytest.mark.parametrize(
    ("lat", "lon"),
    [
        # Latitude margin (just under the PASS corner), ordinary longitude.
        (_MARGIN_LAT_1, _MARGIN_LON_1),
        # Longitude margin on the east edge, in-box latitude.
        (_MARGIN_LAT_2, _MARGIN_LON_2),
        # Longitude margin on the west edge (strictly between 22.5 and
        # 23.5 degrees W), in-box latitude.
        (_MARGIN_LAT_2, _MARGIN_LON_3),
    ],
)
def test_main_applicability_cant_tell_margin_band_sends_no_request(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    lat: float,
    lon: float,
) -> None:
    """A *finite* margin-band site (R2): unlike ``inf``/``"not-a-number"``,
    these values pass the script's second (``_finite_number``) guard, so
    only the applicability guard itself can be stopping the request.
    """
    assert applicability(lat, lon) is Verdict.CANT_TELL  # precondition
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=lat, lon=lon)
    calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"hourly": {"time": []}}, request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=_in_window_now(),
    )
    assert code == 3
    assert calls == 0
    assert out.strip() == "applicability: CAN'T TELL"


def test_main_outside_test_window_sends_no_request_and_exits_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={}, request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=datetime(2026, 6, 2, 3, 0, tzinfo=UTC),  # not in any test window
    )
    assert code == 2
    assert calls == 0
    assert "error: outside the test window" in err


def test_main_unreadable_db_exits_2_with_error_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    code, out, err = _run_main_checked(
        caplog, capsys, ["--db", str(tmp_path / "missing.db")], now=_in_window_now()
    )
    assert code == 2
    assert "error: database unreadable" in err


def test_main_db_path_with_embedded_nul_exits_2_with_error_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    marker_dir = tmp_path / _MARKER_DIR_NAME
    marker_dir.mkdir()
    bad_path = str(marker_dir) + "\x00x"
    calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={}, request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", bad_path],
        transport=httpx.MockTransport(_handler),
        now=_in_window_now(),
    )
    assert code == 2
    assert calls == 0
    assert "error: database unreadable" in err


def test_main_path_resolve_oserror_exits_2_with_error_line(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)

    class _BoomPath(type(Path())):  # type: ignore[misc]
        def resolve(self, strict: bool = False) -> _BoomPath:  # noqa: ARG002
            raise OSError("synthetic resolve failure")

    monkeypatch.setattr("scripts.icon_eu_coverage_check.Path", _BoomPath)
    calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={}, request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=_in_window_now(),
    )
    assert code == 2
    assert calls == 0
    assert "error: database unreadable" in err


def test_main_no_enabled_site_exits_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON, enabled=0)
    code, out, err = _run_main_checked(
        caplog, capsys, ["--db", str(db_path)], now=_in_window_now()
    )
    assert code == 2
    assert "error: no enabled site" in err


def test_main_two_enabled_sites_without_site_id_exits_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES ('Second Site', ?, ?, 0.0, 'UTC', 1)",
        (_IN_BOX_LAT + 1.0, _IN_BOX_LON),
    )
    conn.commit()
    conn.close()
    code, out, err = _run_main_checked(
        caplog, capsys, ["--db", str(db_path)], now=_in_window_now()
    )
    assert code == 2
    assert "error:" in err
    assert "enabled sites" in err


def test_main_bad_forward_shape_is_a_fail_not_a_crash(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    now = _in_window_now()

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"latitude": 0.0, "longitude": 0.0, "hourly": {}}, request=request
        )

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=now,
    )
    assert code == 1
    assert f"forward verdict: {Verdict.FAIL} (bad shape)" in out


@pytest.mark.parametrize("status", [404, 500])
def test_main_forward_http_error_is_classified(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    status: int,
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    now = _in_window_now()
    previous_calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal previous_calls
        if "previous-runs-api" in str(request.url):
            previous_calls += 1
            day = now.date() - timedelta(days=2)
            return httpx.Response(200, json=_historical_body(day), request=request)
        return httpx.Response(
            status, content=str(request.url).encode(), request=request
        )

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=now,
    )
    if status == 404:
        assert f"forward verdict: {Verdict.FAIL} (http 4xx)" in out
        assert previous_calls == 1  # forward failure does not skip previous-runs
        assert code == 1  # FAIL outranks a PASS previous-runs verdict
    else:
        assert f"forward verdict: {Verdict.CANT_TELL} (http 5xx)" in out
        assert previous_calls == 1
        assert code == 3


def test_main_forward_connect_error_is_cant_tell_network(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    now = _in_window_now()

    def _handler(request: httpx.Request) -> httpx.Response:
        if "previous-runs-api" in str(request.url):
            day = now.date() - timedelta(days=2)
            return httpx.Response(200, json=_historical_body(day), request=request)
        raise httpx.ConnectError(f"boom {request.url}", request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=now,
    )
    assert f"forward verdict: {Verdict.CANT_TELL} (network)" in out


def test_main_previous_runs_404_after_forward_pass(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    now = _in_window_now()
    previous_calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal previous_calls
        if "previous-runs-api" in str(request.url):
            previous_calls += 1
            return httpx.Response(
                404, content=str(request.url).encode(), request=request
            )
        return httpx.Response(200, json=_forward_body(now), request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=now,
    )
    assert f"forward verdict: {Verdict.PASS} (ok)" in out
    assert f"previous-runs verdict: {Verdict.FAIL} (http 4xx)" in out
    assert code == 1
    assert previous_calls == 1  # no retry on a previous-runs error


def test_main_previous_runs_500_after_forward_pass(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    now = _in_window_now()
    previous_calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal previous_calls
        if "previous-runs-api" in str(request.url):
            previous_calls += 1
            return httpx.Response(500, request=request)
        return httpx.Response(200, json=_forward_body(now), request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=now,
    )
    assert f"previous-runs verdict: {Verdict.CANT_TELL} (http 5xx)" in out
    assert code == 3
    assert previous_calls == 1  # no retry on a previous-runs error


def test_main_previous_runs_connect_error_after_forward_pass(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    now = _in_window_now()
    previous_calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal previous_calls
        if "previous-runs-api" in str(request.url):
            previous_calls += 1
            raise httpx.ConnectError(f"boom {request.url}", request=request)
        return httpx.Response(200, json=_forward_body(now), request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=now,
    )
    assert f"previous-runs verdict: {Verdict.CANT_TELL} (network)" in out
    assert code == 3
    assert previous_calls == 1  # no retry on a previous-runs error


def test_main_forward_500_still_sends_previous_runs_request_exactly_once(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    now = _in_window_now()
    previous_calls = 0

    def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal previous_calls
        if "previous-runs-api" in str(request.url):
            previous_calls += 1
            day = now.date() - timedelta(days=2)
            return httpx.Response(200, json=_historical_body(day), request=request)
        return httpx.Response(500, request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=now,
    )
    assert previous_calls == 1
    assert f"forward verdict: {Verdict.CANT_TELL} (http 5xx)" in out
    assert f"previous-runs verdict: {Verdict.PASS} (ok)" in out
    assert code == 3


def test_main_previous_runs_200_without_latitude_is_adapter_rejected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    now = _in_window_now()
    day = now.date() - timedelta(days=2)

    def _handler(request: httpx.Request) -> httpx.Response:
        if "previous-runs-api" in str(request.url):
            body = _historical_body(day)
            body = {k: v for k, v in body.items() if k != "latitude"}
            return httpx.Response(200, json=body, request=request)
        return httpx.Response(200, json=_forward_body(now), request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=now,
    )
    assert f"previous-runs verdict: {Verdict.FAIL} (adapter rejected)" in out
    assert "previous-runs steps" not in out
    assert "previous-runs non-null" not in out
    assert code == 1


# ---------------------------------------------------------------------------
# KeyboardInterrupt (plan §8.3 "Mechanics (load-bearing)"): the interrupting
# MockTransport handler is ASYNC and does signal.raise_signal(SIGINT) then
# await asyncio.sleep(0) -- a race turned into a deterministic rendezvous
# with the real asyncio signal-delivery mechanism, never a bare
# `raise KeyboardInterrupt` from a sync handler (which does not exercise
# that mechanism at all).
# ---------------------------------------------------------------------------


def test_main_keyboard_interrupt_during_forward_marks_both_requests_interrupted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    previous_calls = 0

    async def _handler(request: httpx.Request) -> httpx.Response:
        nonlocal previous_calls
        if "previous-runs-api" in str(request.url):
            previous_calls += 1
            return httpx.Response(200, json={}, request=request)
        signal.raise_signal(signal.SIGINT)
        await asyncio.sleep(0)
        return httpx.Response(200, json={}, request=request)  # pragma: no cover

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=_in_window_now(),
    )
    assert code == 3
    assert previous_calls == 0
    assert "applicability: PASS" in out
    assert f"forward verdict: {Verdict.CANT_TELL} (interrupted)" in out
    assert f"previous-runs verdict: {Verdict.CANT_TELL} (interrupted)" in out


def test_main_keyboard_interrupt_during_previous_runs_keeps_the_forward_pass(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    db_path = _marker_db_path(tmp_path)
    _export_db(db_path, lat=_IN_BOX_LAT, lon=_IN_BOX_LON)
    now = _in_window_now()

    async def _handler(request: httpx.Request) -> httpx.Response:
        if "previous-runs-api" in str(request.url):
            signal.raise_signal(signal.SIGINT)
            await asyncio.sleep(0)
            return httpx.Response(200, json={}, request=request)  # pragma: no cover
        return httpx.Response(200, json=_forward_body(now), request=request)

    code, out, err = _run_main_checked(
        caplog,
        capsys,
        ["--db", str(db_path)],
        transport=httpx.MockTransport(_handler),
        now=now,
    )
    assert code == 3
    assert "applicability: PASS" in out
    assert f"forward verdict: {Verdict.PASS} (ok)" in out
    assert f"previous-runs verdict: {Verdict.CANT_TELL} (interrupted)" in out
