"""Part B -- rain labelled by the start of its hour, not its end.

Open-Meteo and meteoblue document rain by the END of the hour; wxverify's
other feeds already label by the start. ``precip_hour_start`` converts a
raw end-of-hour label to wxverify's start-of-hour convention, and
``previous_run_lead_hours`` gives the previous-runs lead for a variable/day
pair under that convention. Both are new symbols, imported inside each
test body so a missing or misnamed symbol fails only that test.

Every new symbol is tried first at ``wxverify.feeds.open_meteo`` (where the
forward and historical normalization that calls it both live); if the
implementation lands elsewhere this import line is the one place to fix.

Synthetic data only: no place name, coordinate, or non-UTC zone appears.
"""

from __future__ import annotations

from pathlib import Path

import pytest


def test_precip_hour_start_subtracts_one_hour_across_a_month_boundary() -> None:
    from wxverify.feeds.open_meteo import precip_hour_start

    assert precip_hour_start("2026-03-01T00:00:00Z") == "2026-02-28T23:00:00Z"


def test_precip_hour_start_subtracts_one_hour_across_a_year_boundary() -> None:
    from wxverify.feeds.open_meteo import precip_hour_start

    assert precip_hour_start("2026-01-01T00:00:00Z") == "2025-12-31T23:00:00Z"


def test_open_meteo_forward_precip_dropped_at_lead_zero_shifted_to_the_prior_label(
) -> None:
    # Raw hourly rows at 01:00/02:00/03:00 against a 00:00Z issue time.
    # Precip's raw 01:00 row becomes label 00:00Z (lead 0) and is dropped;
    # the raw 02:00 row becomes label 01:00Z (lead 1); temperature at 01:00
    # stays at label 01:00Z (lead 1) -- the paired positive showing only
    # precip's label moved.
    from wxverify.feeds.open_meteo import _samples_from_hourly

    samples = _samples_from_hourly(
        "icon_eu",
        "2026-07-15T00:00:00Z",
        {
            "hourly": {
                "time": [
                    "2026-07-15T01:00",
                    "2026-07-15T02:00",
                    "2026-07-15T03:00",
                ],
                "precipitation": [1.0, 2.0, 3.0],
                "temperature_2m": [10.0, 11.0, 12.0],
            }
        },
    )
    precip = {s.valid_at: s for s in samples if s.variable == "precip"}
    temperature = {s.valid_at: s for s in samples if s.variable == "temperature"}

    assert "2026-07-15T00:00:00Z" not in precip
    assert precip["2026-07-15T01:00:00Z"].value == 2.0
    assert precip["2026-07-15T01:00:00Z"].lead_hours == 1
    assert precip["2026-07-15T02:00:00Z"].value == 3.0
    assert precip["2026-07-15T02:00:00Z"].lead_hours == 2
    assert "2026-07-15T03:00:00Z" not in precip

    assert temperature["2026-07-15T01:00:00Z"].value == 10.0
    assert temperature["2026-07-15T01:00:00Z"].lead_hours == 1
    assert temperature["2026-07-15T03:00:00Z"].value == 12.0
    assert temperature["2026-07-15T03:00:00Z"].lead_hours == 3


def test_previous_run_lead_hours_precip_is_one_less_than_temperature() -> None:
    from wxverify.feeds.open_meteo import previous_run_lead_hours

    assert previous_run_lead_hours("precip", 2) == 47
    assert previous_run_lead_hours("temperature", 2) == 48


def test_meteoblue_forward_precip_dropped_at_lead_zero_shifted_to_the_prior_label(
) -> None:
    # Same shift/drop rule, through meteoblue's own parser (one model, one
    # member row per matrix column).
    from wxverify.feeds.meteoblue import MeteoblueResponse, _to_fetch_result
    from wxverify.feeds.seam import ForecastRequest

    payload = MeteoblueResponse.model_validate(
        {
            "metadata": {
                "models": ["meteoblue_mix"],
                "modelrun_utc": ["2026-07-15 00:00"],
                "latitude": 0.0,
                "longitude": 0.0,
            },
            "data_1h": {
                "time": [
                    "2026-07-15 01:00",
                    "2026-07-15 02:00",
                    "2026-07-15 03:00",
                ],
                "precipitation": [[1.0, 2.0, 3.0]],
                "temperature": [[10.0, 11.0, 12.0]],
            },
        }
    )
    req = ForecastRequest(
        lat=0.0,
        lon=0.0,
        model="meteoblue_mix",
        variables=("precip", "temperature"),
        max_lead_hours=24,
    )
    result = _to_fetch_result(req, payload)
    precip = {s.valid_at: s for s in result.samples if s.variable == "precip"}
    temperature = {
        s.valid_at: s for s in result.samples if s.variable == "temperature"
    }

    assert "2026-07-15T00:00:00Z" not in precip
    assert precip["2026-07-15T01:00:00Z"].value == 2.0
    assert precip["2026-07-15T01:00:00Z"].lead_hours == 1
    assert precip["2026-07-15T02:00:00Z"].value == 3.0
    assert precip["2026-07-15T02:00:00Z"].lead_hours == 2
    assert "2026-07-15T03:00:00Z" not in precip
    assert temperature["2026-07-15T03:00:00Z"].value == 12.0
    assert temperature["2026-07-15T03:00:00Z"].lead_hours == 3


def test_historical_window_filter_runs_on_the_converted_precip_time() -> None:
    # The historical window is [window_start, window_end). A raw precip
    # hour exactly AT window_start converts to one hour BEFORE window_start
    # and must be excluded; the next raw hour converts to exactly
    # window_start and must be kept -- the filter must act on the converted
    # label, not the raw one.
    from wxverify.core.timeutil import parse_utc
    from wxverify.feeds.open_meteo import _historical_series_samples
    from wxverify.feeds.seam import ForecastRequest

    req = ForecastRequest(
        lat=0.0, lon=0.0, model="icon_eu", variables=("precip",), max_lead_hours=72
    )
    start = parse_utc("2026-07-10T00:00:00Z")
    end = parse_utc("2026-07-11T00:00:00Z")

    excluded = _historical_series_samples(
        req=req,
        variable="precip",
        day=2,
        times=["2026-07-10T00:00:00Z"],
        values=[5.0],
        start=start,
        end=end,
    )
    assert excluded == []

    kept = _historical_series_samples(
        req=req,
        variable="precip",
        day=2,
        times=["2026-07-10T01:00:00Z"],
        values=[5.0],
        start=start,
        end=end,
    )
    assert len(kept) == 1
    assert kept[0].valid_at == "2026-07-10T00:00:00Z"


def test_open_meteo_forward_temperature_and_wind_columns_unchanged() -> None:
    # Every column but valid_at/lead_hours, for a non-precip variable, is
    # unaffected by the rain-label shift.
    from wxverify.feeds.open_meteo import _samples_from_hourly

    samples = _samples_from_hourly(
        "icon_eu",
        "2026-07-15T00:00:00Z",
        {
            "hourly": {
                "time": ["2026-07-15T01:00"],
                "wind_speed_10m": [36.0],
            }
        },
    )
    assert len(samples) == 1
    sample = samples[0]
    assert sample.model == "icon_eu"
    assert sample.variable == "wind"
    assert sample.issued_at == "2026-07-15T00:00:00Z"
    assert sample.model_run_id == "icon_eu:2026-07-15T00:00:00Z"
    assert sample.source_raw == "36.0"


# ---------------------------------------------------------------------------
# B9 -- the four providers whose rain label is NOT part of this rework
# (SC3), named where an owner deciding how to settle them would look.
# ---------------------------------------------------------------------------

_FLAGGED_PROVIDERS = ("Google", "Visual Crossing", "OpenWeatherMap", "WeatherAPI")

_FLAGGED_ADAPTER_MODULES = (
    "wxverify.feeds.google",
    "wxverify.feeds.visualcrossing",
    "wxverify.feeds.openweathermap",
    "wxverify.feeds.weatherapi",
)


def test_precip_hour_start_docstring_names_the_four_flagged_providers() -> None:
    from wxverify.feeds.open_meteo import precip_hour_start

    doc = precip_hour_start.__doc__ or ""
    for provider in _FLAGGED_PROVIDERS:
        assert provider in doc, provider


def test_readme_names_the_four_flagged_providers_together() -> None:
    # The four names already appear scattered in README.md (one per
    # provider-key row in the settings table), so a bare substring check on
    # the whole file would pass vacuously whether or not B9's addition ever
    # lands. Require them on ONE line together, naming them as the group
    # left unconverted by this rework -- a line that does not exist yet.
    readme = Path(__file__).resolve().parent.parent / "README.md"
    lines = readme.read_text(encoding="utf-8").splitlines()
    assert any(
        all(provider in line for provider in _FLAGGED_PROVIDERS) for line in lines
    )


@pytest.mark.parametrize("module_name", _FLAGGED_ADAPTER_MODULES)
def test_flagged_adapter_does_not_reference_the_new_boundary_helpers(
    module_name: str,
) -> None:
    # Structural guard for SC3: none of the four flagged adapters' rain
    # labelling is touched by this rework, so none of them should come to
    # import the new start-of-hour conversion helpers.
    import importlib

    module = importlib.import_module(module_name)
    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "precip_hour_start" not in source
    assert "previous_run_lead_hours" not in source
