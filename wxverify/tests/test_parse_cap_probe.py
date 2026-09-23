"""Item P — parse-cap probe (§11 of the 0.16.0 plan).

Tests ``wxverify.feeds.parse_cap_probe``: the ``parse_cap_probe`` constructor
keyword on the three adapters, the ``RuntimeOptions.parse_cap_probe`` field,
the ``config.yaml`` schema line, and the registry wiring.

Every expected literal below was derived from the real ``_to_fetch_result``
of each adapter module and from P.2's field definitions — never
recomputed by calling the code under test.

Convention: ``httpx.MockTransport`` with a synchronous handler, matching
``tests/test_meteoblue_wind_unit_regression.py`` and
``tests/test_google_pagination.py``. All fixture data is synthetic: 2030
dates, ``lat``/``lon`` ``0.0``, API key ``"synthetic-key"``, meteoblue
members ``alpha_1`` and ``bad name``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sqlite3
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

import wxverify.feeds.meteosource as ms_feed
import wxverify.feeds.visualcrossing as vc_feed
from wxverify import config
from wxverify.core.options import load_runtime_options
from wxverify.db.connection import close_db, init_db
from wxverify.feeds import parse_cap_probe as pcp
from wxverify.feeds.meteoblue import MeteoblueAdapter
from wxverify.feeds.meteosource import MeteosourceAdapter
from wxverify.feeds.registry import build_adapter
from wxverify.feeds.seam import ForecastRequest, NormalizedSample
from wxverify.feeds.visualcrossing import VisualCrossingAdapter
from wxverify.worker.feed_fetch import FetchFeedSuccess, fetch_feed_once

_PROBE_LOGGER = "wxverify.feeds.parse_cap_probe"


def _probe_records(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name == _PROBE_LOGGER]


def _handler_returning(
    payload: dict[str, object],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    return handler


def _write_options(tmp_path: Path, name: str, payload: dict[str, object]) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Fixture payloads and expected records — P-T7's fixtures, reused by
# P-T5, P-T6, P-T8, P-T9, P-T10, P-T12.
# ---------------------------------------------------------------------------

_VC_PAYLOAD: dict[str, object] = {
    "days": [
        {
            "hours": [
                {"datetimeEpoch": 1893456000, "temp": 5.0},
                {"datetimeEpoch": 1893459600, "temp": 5.0, "windspeed": 10.0},
                {"datetimeEpoch": 1893463200, "temp": 5.0, "windspeed": 10.0},
            ]
        },
        {
            "hours": [
                {"datetimeEpoch": 1894060800, "temp": 5.0},
                {"datetimeEpoch": 1894064400, "temp": 5.0},
                {"datetimeEpoch": 1894075200, "temp": 5.0},
            ]
        },
    ]
}

_VC_EXPECTED_LINE = (
    "parse_cap_probe status=ok source=visualcrossing model=blend cap=168 "
    "uncapped=5 capped=3 min_lead=1 max_lead=172 gaps=2 gap_hours=167 "
    "temperature_uncapped=5 temperature_capped=3 temperature_max_lead=172 "
    "wind_uncapped=2 wind_capped=2 wind_max_lead=2 "
    "precip_uncapped=0 precip_capped=0 precip_max_lead=none"
)


def _vc_request(max_lead_hours: int = 168) -> ForecastRequest:
    return ForecastRequest(
        lat=0.0,
        lon=0.0,
        model="blend",
        variables=("temperature", "wind", "precip"),
        max_lead_hours=max_lead_hours,
    )


_MS_PAYLOAD: dict[str, object] = {
    "hourly": {
        "data": [
            {
                "date": "2030-01-01T00:00:00",
                "temperature": 5.0,
                "wind": {"speed": 2.0},
            },
            {
                "date": "2030-01-01T01:00:00",
                "temperature": 5.0,
                "wind": {"speed": 2.0},
            },
            {
                "date": "2030-01-02T00:00:00",
                "temperature": 5.0,
                "wind": {"speed": 2.0},
            },
            {
                "date": "2030-01-02T01:00:00",
                "temperature": 5.0,
                "wind": {"speed": 2.0},
            },
            {
                "date": "2030-01-03T00:00:00",
                "temperature": 5.0,
                "wind": {"speed": 2.0},
            },
        ]
    }
}

_MS_EXPECTED_LINE = (
    "parse_cap_probe status=ok source=meteosource model=blend cap=24 "
    "uncapped=4 capped=2 min_lead=1 max_lead=48 gaps=2 gap_hours=44 "
    "temperature_uncapped=4 temperature_capped=2 temperature_max_lead=48 "
    "wind_uncapped=4 wind_capped=2 wind_max_lead=48 "
    "precip_uncapped=0 precip_capped=0 precip_max_lead=none"
)


def _ms_request(max_lead_hours: int = 24) -> ForecastRequest:
    return ForecastRequest(
        lat=0.0,
        lon=0.0,
        model="blend",
        variables=("temperature", "wind", "precip"),
        max_lead_hours=max_lead_hours,
    )


_MB_PAYLOAD: dict[str, object] = {
    "metadata": {
        "models": ["alpha_1", "bad name"],
        "modelrun_utc": ["2030-01-01 00:00", "2030-01-01 12:00"],
        "latitude": 0.0,
        "longitude": 0.0,
    },
    "data_1h": {
        "time": [
            "2030-01-01 01:00",
            "2030-01-08 00:00",
            "2030-01-08 12:00",
            "2030-01-09 00:00",
        ],
        "temperature": [[5.0, 5.0, 5.0, 5.0], [6.0, 6.0, 6.0, None]],
        "windspeed": [[10.0, None, None, None]],
        "precipitation": [],
    },
}

_MB_EXPECTED_LINES = [
    "parse_cap_probe status=ok source=meteoblue model=alpha_1 cap=168 "
    "uncapped=4 capped=2 min_lead=1 max_lead=192 gaps=3 gap_hours=188 "
    "temperature_uncapped=4 temperature_capped=2 temperature_max_lead=192 "
    "wind_uncapped=1 wind_capped=1 wind_max_lead=1 "
    "precip_uncapped=0 precip_capped=0 precip_max_lead=none",
    "parse_cap_probe status=ok source=meteoblue model=member1 cap=168 "
    "uncapped=2 capped=2 min_lead=156 max_lead=168 gaps=1 gap_hours=11 "
    "temperature_uncapped=2 temperature_capped=2 temperature_max_lead=168 "
    "wind_uncapped=0 wind_capped=0 wind_max_lead=none "
    "precip_uncapped=0 precip_capped=0 precip_max_lead=none",
]


def _mb_request(max_lead_hours: int = 168) -> ForecastRequest:
    return ForecastRequest(
        lat=0.0,
        lon=0.0,
        model="multimodel",
        variables=("temperature", "wind", "precip"),
        max_lead_hours=max_lead_hours,
    )


# ---------------------------------------------------------------------------
# P-T1 — the option, through load_runtime_options
# ---------------------------------------------------------------------------


def test_pt1_options_json_only_literal_true_enables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("WXV_PARSE_CAP_PROBE", raising=False)
    options_path = tmp_path / "options.json"
    monkeypatch.setattr(config, "options_path", str(options_path))
    cases: list[tuple[dict[str, object], bool]] = [
        ({}, False),
        ({"parse_cap_probe": True}, True),
        ({"parse_cap_probe": "true"}, False),
        ({"parse_cap_probe": 1}, False),
        ({"parse_cap_probe": False}, False),
    ]
    for payload, expected in cases:
        options_path.write_text(json.dumps(payload), encoding="utf-8")
        options = load_runtime_options()
        assert options.parse_cap_probe is expected, f"payload={payload!r}"


def test_pt1_env_bool_gates_parse_cap_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    missing_path = tmp_path / "missing-options.json"
    monkeypatch.setattr(config, "options_path", str(missing_path))
    cases: list[tuple[str | None, bool]] = [
        (None, False),
        ("true", True),
        ("0", False),
    ]
    for raw, expected in cases:
        if raw is None:
            monkeypatch.delenv("WXV_PARSE_CAP_PROBE", raising=False)
        else:
            monkeypatch.setenv("WXV_PARSE_CAP_PROBE", raw)
        options = load_runtime_options()
        assert options.parse_cap_probe is expected, f"raw={raw!r}"


# ---------------------------------------------------------------------------
# P-T2 — config.yaml schema line, no options:/translations: entry
# ---------------------------------------------------------------------------


def test_pt2_schema_line_present_no_options_or_translations_entry() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_lines = (repo_root / "config.yaml").read_text(encoding="utf-8").splitlines()

    assert "  parse_cap_probe: bool?" in config_lines
    monitor_db_index = config_lines.index("  monitor_db: bool")
    assert config_lines[monitor_db_index + 1] == "  parse_cap_probe: bool?"

    options_keys: set[str] = set()
    in_options = False
    for line in config_lines:
        stripped = line.rstrip()
        if stripped == "options:":
            in_options = True
            continue
        if in_options:
            m = re.match(r"^  (\w+):", stripped)
            if m:
                options_keys.add(m.group(1))
            elif stripped and not stripped.startswith(" "):
                break
    assert "parse_cap_probe" not in options_keys

    en_yaml_text = (repo_root / "translations" / "en.yaml").read_text(encoding="utf-8")
    trans_lines = en_yaml_text.splitlines()
    configuration_keys: set[str] = set()
    in_configuration = False
    for line in trans_lines:
        stripped = line.rstrip()
        if stripped == "configuration:":
            in_configuration = True
            continue
        if in_configuration:
            m = re.match(r"^  (\w+):", stripped)
            if m:
                configuration_keys.add(m.group(1))
            elif stripped and not stripped.startswith(" "):
                break
    assert "parse_cap_probe" not in configuration_keys


# ---------------------------------------------------------------------------
# P-T3 — summarize_parse_cap arithmetic
# ---------------------------------------------------------------------------


def _sample(
    *, model: str, variable: str, valid_at: str, lead_hours: int
) -> NormalizedSample:
    return NormalizedSample(
        model=model,
        variable=variable,
        issued_at="2030-01-01T00:00:00Z",
        valid_at=valid_at,
        lead_hours=lead_hours,
        value=1.0,
        source_raw="synthetic",
        model_run_id=f"{model}:2030-01-01T00:00:00Z",
    )


def test_pt3_no_samples_gives_zeroed_summary() -> None:
    summaries = pcp.summarize_parse_cap(
        [], models=("blend",), variables=("temperature", "wind", "precip"), cap=4
    )
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.model == "blend"
    assert summary.uncapped == 0
    assert summary.capped == 0
    assert summary.min_lead is None
    assert summary.max_lead is None
    assert summary.gaps == 0
    assert summary.gap_hours == 0
    assert [
        (v.variable, v.uncapped, v.capped, v.max_lead) for v in summary.variables
    ] == [
        ("temperature", 0, 0, None),
        ("wind", 0, 0, None),
        ("precip", 0, 0, None),
    ]


def test_pt3_temperature_leads_1_2_5_8_cap_4() -> None:
    samples = [
        _sample(
            model="blend",
            variable="temperature",
            valid_at=f"2030-01-01T{hour:02d}:00:00Z",
            lead_hours=hour,
        )
        for hour in (1, 2, 5, 8)
    ]
    summary = pcp.summarize_parse_cap(
        samples, models=("blend",), variables=("temperature", "wind", "precip"), cap=4
    )[0]
    assert summary.uncapped == 4
    assert summary.capped == 2
    assert summary.min_lead == 1
    assert summary.max_lead == 8
    assert summary.gaps == 2
    assert summary.gap_hours == 4


def test_pt3_single_gap_between_two_instants() -> None:
    samples = [
        _sample(
            model="blend",
            variable="temperature",
            valid_at="2030-01-01T01:00:00Z",
            lead_hours=1,
        ),
        _sample(
            model="blend",
            variable="temperature",
            valid_at="2030-01-01T02:30:00Z",
            lead_hours=2,
        ),
    ]
    summary = pcp.summarize_parse_cap(
        samples, models=("blend",), variables=("temperature", "wind", "precip"), cap=24
    )[0]
    assert summary.gaps == 1
    assert summary.gap_hours == 1


def test_pt3_gap_boundary_exactly_one_hour_is_not_a_gap() -> None:
    samples = [
        _sample(
            model="blend",
            variable="temperature",
            valid_at="2030-01-01T01:00:00Z",
            lead_hours=1,
        ),
        _sample(
            model="blend",
            variable="temperature",
            valid_at="2030-01-01T02:00:00Z",
            lead_hours=2,
        ),
    ]
    summary = pcp.summarize_parse_cap(
        samples, models=("blend",), variables=("temperature", "wind", "precip"), cap=24
    )[0]
    assert summary.gaps == 0
    assert summary.gap_hours == 0


def test_pt3_gap_boundary_just_over_one_hour_is_a_gap() -> None:
    samples = [
        _sample(
            model="blend",
            variable="temperature",
            valid_at="2030-01-01T00:00:00Z",
            lead_hours=1,
        ),
        _sample(
            model="blend",
            variable="temperature",
            valid_at="2030-01-01T01:00:00.500000Z",
            lead_hours=2,
        ),
    ]
    summary = pcp.summarize_parse_cap(
        samples, models=("blend",), variables=("temperature", "wind", "precip"), cap=24
    )[0]
    assert summary.gaps == 1
    assert summary.gap_hours == 1


def test_pt3_two_variables_cap_2() -> None:
    samples = [
        _sample(
            model="blend",
            variable="temperature",
            valid_at="2030-01-01T01:00:00Z",
            lead_hours=1,
        ),
        _sample(
            model="blend",
            variable="temperature",
            valid_at="2030-01-01T02:00:00Z",
            lead_hours=2,
        ),
        _sample(
            model="blend",
            variable="temperature",
            valid_at="2030-01-01T03:00:00Z",
            lead_hours=3,
        ),
        _sample(
            model="blend",
            variable="wind",
            valid_at="2030-01-01T01:00:00Z",
            lead_hours=1,
        ),
    ]
    summary = pcp.summarize_parse_cap(
        samples, models=("blend",), variables=("temperature", "wind", "precip"), cap=2
    )[0]
    assert summary.uncapped == 3
    assert summary.capped == 2
    by_variable = {
        v.variable: (v.uncapped, v.capped, v.max_lead) for v in summary.variables
    }
    assert by_variable["temperature"] == (3, 2, 3)
    assert by_variable["wind"] == (1, 1, 1)
    assert by_variable["precip"] == (0, 0, None)


def test_pt3_duplicate_model_name_yields_one_summary_each() -> None:
    summaries = pcp.summarize_parse_cap(
        [], models=("b", "a", "b"), variables=("temperature", "wind", "precip"), cap=1
    )
    assert [s.model for s in summaries] == ["b", "a"]


# ---------------------------------------------------------------------------
# P-T4 — format_parse_cap_record: name allowlist and `none` rendering
# ---------------------------------------------------------------------------


def _zero_summary(model: str):
    return pcp.ParseCapSummary(
        model=model,
        cap=1,
        uncapped=0,
        capped=0,
        min_lead=None,
        max_lead=None,
        gaps=0,
        gap_hours=0,
        variables=(
            pcp.VariableCoverage(
                variable="temperature", uncapped=0, capped=0, max_lead=None
            ),
            pcp.VariableCoverage(variable="wind", uncapped=0, capped=0, max_lead=None),
            pcp.VariableCoverage(
                variable="precip", uncapped=0, capped=0, max_lead=None
            ),
        ),
    )


def test_pt4_allowed_name_prints_as_is() -> None:
    summary = _zero_summary("alpha_1")
    line = pcp.format_parse_cap_record("visualcrossing", 0, summary)
    assert line == (
        "parse_cap_probe status=ok source=visualcrossing model=alpha_1 cap=1 "
        "uncapped=0 capped=0 min_lead=none max_lead=none gaps=0 gap_hours=0 "
        "temperature_uncapped=0 temperature_capped=0 temperature_max_lead=none "
        "wind_uncapped=0 wind_capped=0 wind_max_lead=none "
        "precip_uncapped=0 precip_capped=0 precip_max_lead=none"
    )


def test_pt4_disallowed_characters_fall_back_to_member_index() -> None:
    summary = _zero_summary("bad name\nx")
    line = pcp.format_parse_cap_record("meteoblue", 1, summary)
    assert line == (
        "parse_cap_probe status=ok source=meteoblue model=member1 cap=1 "
        "uncapped=0 capped=0 min_lead=none max_lead=none gaps=0 gap_hours=0 "
        "temperature_uncapped=0 temperature_capped=0 temperature_max_lead=none "
        "wind_uncapped=0 wind_capped=0 wind_max_lead=none "
        "precip_uncapped=0 precip_capped=0 precip_max_lead=none"
    )
    assert "\n" not in line
    assert "bad name" not in line


def test_pt4_trailing_newline_is_not_matched_by_fullmatch() -> None:
    """A ``$``-anchored ``re.match`` would wrongly accept a name ending in a
    newline; ``re.fullmatch`` must reject it and fall back to member0.
    """
    summary = _zero_summary("alpha_1\n")
    line = pcp.format_parse_cap_record("visualcrossing", 0, summary)
    assert line == (
        "parse_cap_probe status=ok source=visualcrossing model=member0 cap=1 "
        "uncapped=0 capped=0 min_lead=none max_lead=none gaps=0 gap_hours=0 "
        "temperature_uncapped=0 temperature_capped=0 temperature_max_lead=none "
        "wind_uncapped=0 wind_capped=0 wind_max_lead=none "
        "precip_uncapped=0 precip_capped=0 precip_max_lead=none"
    )
    assert "\n" not in line


def test_pt4_over_length_name_falls_back_to_member_index() -> None:
    summary = _zero_summary("a" * 65)
    line = pcp.format_parse_cap_record("meteoblue", 2, summary)
    assert line == (
        "parse_cap_probe status=ok source=meteoblue model=member2 cap=1 "
        "uncapped=0 capped=0 min_lead=none max_lead=none gaps=0 gap_hours=0 "
        "temperature_uncapped=0 temperature_capped=0 temperature_max_lead=none "
        "wind_uncapped=0 wind_capped=0 wind_max_lead=none "
        "precip_uncapped=0 precip_capped=0 precip_max_lead=none"
    )


# ---------------------------------------------------------------------------
# P-T5 — the registry passes the flag
# ---------------------------------------------------------------------------


def test_pt5_registry_visualcrossing_reads_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    monkeypatch.setattr(
        vc_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    for enabled in (True, False):
        caplog.clear()
        options_path = _write_options(
            tmp_path,
            f"vc-options-{enabled}.json",
            {"visualcrossing_key": "synthetic-key", "parse_cap_probe": enabled},
        )
        monkeypatch.setattr(config, "options_path", str(options_path))
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(_handler_returning(_VC_PAYLOAD))
        )
        try:
            adapter = build_adapter("visualcrossing", client)
            asyncio.run(adapter.fetch_forecast(_vc_request()))
        finally:
            asyncio.run(client.aclose())
        records = _probe_records(caplog)
        if enabled:
            assert records == [_VC_EXPECTED_LINE]
        else:
            assert records == []


def test_pt5_registry_meteosource_reads_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    monkeypatch.setattr(
        ms_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    for enabled in (True, False):
        caplog.clear()
        options_path = _write_options(
            tmp_path,
            f"ms-options-{enabled}.json",
            {"meteosource_key": "synthetic-key", "parse_cap_probe": enabled},
        )
        monkeypatch.setattr(config, "options_path", str(options_path))
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(_handler_returning(_MS_PAYLOAD))
        )
        try:
            adapter = build_adapter("meteosource", client)
            asyncio.run(adapter.fetch_forecast(_ms_request()))
        finally:
            asyncio.run(client.aclose())
        records = _probe_records(caplog)
        if enabled:
            assert records == [_MS_EXPECTED_LINE]
        else:
            assert records == []


def test_pt5_registry_meteoblue_reads_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    for enabled in (True, False):
        caplog.clear()
        options_path = _write_options(
            tmp_path,
            f"mb-options-{enabled}.json",
            {"meteoblue_key": "synthetic-key", "parse_cap_probe": enabled},
        )
        monkeypatch.setattr(config, "options_path", str(options_path))
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(_handler_returning(_MB_PAYLOAD))
        )
        try:
            adapter = build_adapter("meteoblue", client)
            asyncio.run(adapter.fetch_forecast(_mb_request()))
        finally:
            asyncio.run(client.aclose())
        records = _probe_records(caplog)
        if enabled:
            assert records == _MB_EXPECTED_LINES
        else:
            assert records == []


# ---------------------------------------------------------------------------
# P-T6 — off means silent
# ---------------------------------------------------------------------------


def test_pt6_visualcrossing_default_construction_is_silent(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    monkeypatch.setattr(
        vc_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_VC_PAYLOAD))
    )
    try:
        adapter = VisualCrossingAdapter("synthetic-key", client)
        asyncio.run(adapter.fetch_forecast(_vc_request()))
    finally:
        asyncio.run(client.aclose())
    assert _probe_records(caplog) == []


def test_pt6_visualcrossing_explicit_false_is_silent(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    monkeypatch.setattr(
        vc_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_VC_PAYLOAD))
    )
    try:
        adapter = VisualCrossingAdapter("synthetic-key", client, parse_cap_probe=False)
        asyncio.run(adapter.fetch_forecast(_vc_request()))
    finally:
        asyncio.run(client.aclose())
    assert _probe_records(caplog) == []


def test_pt6_meteosource_default_construction_is_silent(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    monkeypatch.setattr(
        ms_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_MS_PAYLOAD))
    )
    try:
        adapter = MeteosourceAdapter("synthetic-key", client)
        asyncio.run(adapter.fetch_forecast(_ms_request()))
    finally:
        asyncio.run(client.aclose())
    assert _probe_records(caplog) == []


def test_pt6_meteosource_explicit_false_is_silent(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    monkeypatch.setattr(
        ms_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_MS_PAYLOAD))
    )
    try:
        adapter = MeteosourceAdapter("synthetic-key", client, parse_cap_probe=False)
        asyncio.run(adapter.fetch_forecast(_ms_request()))
    finally:
        asyncio.run(client.aclose())
    assert _probe_records(caplog) == []


def test_pt6_meteoblue_default_construction_is_silent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_MB_PAYLOAD))
    )
    try:
        adapter = MeteoblueAdapter("synthetic-key", client)
        asyncio.run(adapter.fetch_forecast(_mb_request()))
    finally:
        asyncio.run(client.aclose())
    assert _probe_records(caplog) == []


def test_pt6_meteoblue_explicit_false_is_silent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_MB_PAYLOAD))
    )
    try:
        adapter = MeteoblueAdapter("synthetic-key", client, parse_cap_probe=False)
        asyncio.run(adapter.fetch_forecast(_mb_request()))
    finally:
        asyncio.run(client.aclose())
    assert _probe_records(caplog) == []


# ---------------------------------------------------------------------------
# P-T7 — the exact records
# ---------------------------------------------------------------------------


def test_pt7_visualcrossing_exact_record(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    monkeypatch.setattr(
        vc_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_VC_PAYLOAD))
    )
    try:
        adapter = VisualCrossingAdapter("synthetic-key", client, parse_cap_probe=True)
        asyncio.run(adapter.fetch_forecast(_vc_request()))
    finally:
        asyncio.run(client.aclose())
    assert _probe_records(caplog) == [_VC_EXPECTED_LINE]


def test_pt7_meteosource_exact_record(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    monkeypatch.setattr(
        ms_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_MS_PAYLOAD))
    )
    try:
        adapter = MeteosourceAdapter("synthetic-key", client, parse_cap_probe=True)
        asyncio.run(adapter.fetch_forecast(_ms_request()))
    finally:
        asyncio.run(client.aclose())
    assert _probe_records(caplog) == [_MS_EXPECTED_LINE]


def test_pt7_meteoblue_exact_records_in_first_occurrence_order(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_MB_PAYLOAD))
    )
    try:
        adapter = MeteoblueAdapter("synthetic-key", client, parse_cap_probe=True)
        asyncio.run(adapter.fetch_forecast(_mb_request()))
    finally:
        asyncio.run(client.aclose())
    assert _probe_records(caplog) == _MB_EXPECTED_LINES


# ---------------------------------------------------------------------------
# P-T8 — the returned result stays capped
# ---------------------------------------------------------------------------


def test_pt8_visualcrossing_result_stays_capped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        vc_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_VC_PAYLOAD))
    )
    try:
        adapter = VisualCrossingAdapter("synthetic-key", client, parse_cap_probe=True)
        result = asyncio.run(adapter.fetch_forecast(_vc_request()))
    finally:
        asyncio.run(client.aclose())
    assert len({s.valid_at for s in result.samples}) == 3


def test_pt8_meteosource_result_stays_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ms_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_MS_PAYLOAD))
    )
    try:
        adapter = MeteosourceAdapter("synthetic-key", client, parse_cap_probe=True)
        result = asyncio.run(adapter.fetch_forecast(_ms_request()))
    finally:
        asyncio.run(client.aclose())
    assert len({s.valid_at for s in result.samples}) == 2


def test_pt8_meteoblue_result_stays_capped_per_member() -> None:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_MB_PAYLOAD))
    )
    try:
        adapter = MeteoblueAdapter("synthetic-key", client, parse_cap_probe=True)
        result = asyncio.run(adapter.fetch_forecast(_mb_request()))
    finally:
        asyncio.run(client.aclose())
    by_model: dict[str, set[str]] = {}
    for sample in result.samples:
        by_model.setdefault(sample.model, set()).add(sample.valid_at)
    assert set(by_model) == {"alpha_1", "bad name"}
    assert len(by_model["alpha_1"]) == 2
    assert len(by_model["bad name"]) == 2


# ---------------------------------------------------------------------------
# P-T9 — the flag changes nothing but the log
# ---------------------------------------------------------------------------


def test_pt9_visualcrossing_flag_does_not_change_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        vc_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    results = {}
    call_counts = {}
    params_by_flag = {}
    for flag in (False, True):
        captured: list[httpx.Request] = []

        def handler(
            request: httpx.Request, _captured: list[httpx.Request] = captured
        ) -> httpx.Response:
            _captured.append(request)
            return httpx.Response(200, json=_VC_PAYLOAD)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            adapter = VisualCrossingAdapter(
                "synthetic-key", client, parse_cap_probe=flag
            )
            results[flag] = asyncio.run(adapter.fetch_forecast(_vc_request()))
        finally:
            asyncio.run(client.aclose())
        call_counts[flag] = len(captured)
        params_by_flag[flag] = dict(captured[0].url.params) if captured else None

    assert results[False] == results[True]
    assert call_counts[False] == 1
    assert call_counts[True] == 1
    assert params_by_flag[False] == params_by_flag[True]


def test_pt9_meteosource_flag_does_not_change_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        ms_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )
    results = {}
    call_counts = {}
    params_by_flag = {}
    for flag in (False, True):
        captured: list[httpx.Request] = []

        def handler(
            request: httpx.Request, _captured: list[httpx.Request] = captured
        ) -> httpx.Response:
            _captured.append(request)
            return httpx.Response(200, json=_MS_PAYLOAD)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            adapter = MeteosourceAdapter("synthetic-key", client, parse_cap_probe=flag)
            results[flag] = asyncio.run(adapter.fetch_forecast(_ms_request()))
        finally:
            asyncio.run(client.aclose())
        call_counts[flag] = len(captured)
        params_by_flag[flag] = dict(captured[0].url.params) if captured else None

    assert results[False] == results[True]
    assert call_counts[False] == 1
    assert call_counts[True] == 1
    assert params_by_flag[False] == params_by_flag[True]


def test_pt9_meteoblue_flag_does_not_change_result() -> None:
    results = {}
    call_counts = {}
    params_by_flag = {}
    for flag in (False, True):
        captured: list[httpx.Request] = []

        def handler(
            request: httpx.Request, _captured: list[httpx.Request] = captured
        ) -> httpx.Response:
            _captured.append(request)
            return httpx.Response(200, json=_MB_PAYLOAD)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            adapter = MeteoblueAdapter("synthetic-key", client, parse_cap_probe=flag)
            results[flag] = asyncio.run(adapter.fetch_forecast(_mb_request()))
        finally:
            asyncio.run(client.aclose())
        call_counts[flag] = len(captured)
        params_by_flag[flag] = dict(captured[0].url.params) if captured else None

    assert results[False] == results[True]
    assert call_counts[False] == 1
    assert call_counts[True] == 1
    assert params_by_flag[False] == params_by_flag[True]


# ---------------------------------------------------------------------------
# P-T10 — failures stay inside the probe
# ---------------------------------------------------------------------------


def test_pt10_visualcrossing_probe_failure_contained(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    monkeypatch.setattr(
        vc_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )

    def _raise(*args: object, **kwargs: object) -> None:
        raise RuntimeError("synthetic-probe-failure")

    monkeypatch.setattr(pcp, "summarize_parse_cap", _raise)

    off_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_VC_PAYLOAD))
    )
    try:
        off_result = asyncio.run(
            VisualCrossingAdapter(
                "synthetic-key", off_client, parse_cap_probe=False
            ).fetch_forecast(_vc_request())
        )
    finally:
        asyncio.run(off_client.aclose())

    on_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_VC_PAYLOAD))
    )
    try:
        on_result = asyncio.run(
            VisualCrossingAdapter(
                "synthetic-key", on_client, parse_cap_probe=True
            ).fetch_forecast(_vc_request())
        )
    finally:
        asyncio.run(on_client.aclose())

    assert on_result == off_result
    records = _probe_records(caplog)
    assert records == [
        "parse_cap_probe status=error source=visualcrossing error_type=RuntimeError"
    ]
    assert "synthetic-probe-failure" not in records[0]


def test_pt10_meteosource_probe_failure_contained(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    calls = {"n": 0}

    def _snap_run(fetch_time: str | None = None) -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            return "2030-01-01T00:00:00Z"
        raise ValueError("synthetic-snap-failure")

    monkeypatch.setattr(ms_feed, "snap_run", _snap_run)

    off_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_MS_PAYLOAD))
    )
    try:
        off_result = asyncio.run(
            MeteosourceAdapter(
                "synthetic-key", off_client, parse_cap_probe=False
            ).fetch_forecast(_ms_request())
        )
    finally:
        asyncio.run(off_client.aclose())

    calls["n"] = 0
    on_client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_MS_PAYLOAD))
    )
    try:
        on_result = asyncio.run(
            MeteosourceAdapter(
                "synthetic-key", on_client, parse_cap_probe=True
            ).fetch_forecast(_ms_request())
        )
    finally:
        asyncio.run(on_client.aclose())

    assert on_result == off_result
    records = _probe_records(caplog)
    assert records == [
        "parse_cap_probe status=error source=meteosource error_type=ValueError"
    ]


def test_pt10_meteoblue_builds_every_record_before_writing_any(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    real_format = pcp.format_parse_cap_record

    def _format(source: str, index: int, summary: pcp.ParseCapSummary) -> str:
        if index == 1:
            raise RuntimeError("synthetic-format-failure")
        return real_format(source, index, summary)

    monkeypatch.setattr(pcp, "format_parse_cap_record", _format)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(_MB_PAYLOAD))
    )
    try:
        adapter = MeteoblueAdapter("synthetic-key", client, parse_cap_probe=True)
        asyncio.run(adapter.fetch_forecast(_mb_request()))
    finally:
        asyncio.run(client.aclose())

    records = _probe_records(caplog)
    assert records == [
        "parse_cap_probe status=error source=meteoblue error_type=RuntimeError"
    ]
    assert "synthetic-format-failure" not in records[0]


# ---------------------------------------------------------------------------
# P-T11 — a rejected payload is not masked
# ---------------------------------------------------------------------------


def test_pt11_rejected_meteoblue_payload_not_masked(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    bad_payload: dict[str, object] = {
        "metadata": {
            "models": ["alpha_1"],
            "modelrun_utc": ["2030-01-01 00:00"],
            "latitude": 0.0,
            "longitude": 0.0,
        }
    }
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(_handler_returning(bad_payload))
    )
    try:
        adapter = MeteoblueAdapter("synthetic-key", client, parse_cap_probe=True)
        with pytest.raises(
            ValueError, match=r"meteoblue response missing hourly multimodel data"
        ):
            asyncio.run(adapter.fetch_forecast(_mb_request()))
    finally:
        asyncio.run(client.aclose())
    assert _probe_records(caplog) == []


# ---------------------------------------------------------------------------
# P-T12 — end to end, through the database
# ---------------------------------------------------------------------------


def _init_tmp_db(db_dir: Path) -> tuple[object, sqlite3.Connection]:
    close_db()
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(db_dir / "missing-options.json")
    db = init_db(str(db_path))
    return db, db._conn  # noqa: SLF001


def _insert_site(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('ParseCapProbeSite', 0.0, 0.0, 0.0, 'UTC')
            """
        ).lastrowid
    )


def _visualcrossing_feed_id(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='visualcrossing' ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )


def _subscribe(conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)
        VALUES (?, ?, 1, 0)
        """,
        (site_id, feed_id),
    )


def test_pt12_probe_does_not_change_persisted_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=_PROBE_LOGGER)
    monkeypatch.setattr(
        vc_feed, "snap_run", lambda fetch_time=None: "2030-01-01T00:00:00Z"
    )

    def _run(db_dir: Path, flag: bool):
        db, conn = _init_tmp_db(db_dir)
        site_id = _insert_site(conn)
        feed_id = _visualcrossing_feed_id(conn)
        _subscribe(conn, site_id, feed_id)

        mock_client = httpx.AsyncClient(
            transport=httpx.MockTransport(_handler_returning(_VC_PAYLOAD))
        )

        def _build(source: str, client: httpx.AsyncClient) -> VisualCrossingAdapter:
            return VisualCrossingAdapter(
                "synthetic-key", mock_client, parse_cap_probe=flag
            )

        try:
            outcome = asyncio.run(
                fetch_feed_once(db, site_id, feed_id, adapter_builder=_build)
            )
        finally:
            asyncio.run(mock_client.aclose())

        assert outcome == FetchFeedSuccess(inserted=5, usable=5)

        forecast_projection = sorted(
            tuple(row)
            for row in conn.execute(
                "SELECT site_id, feed_id, variable, issued_at, valid_at, "
                "lead_hours, value, source_raw, model_run_id "
                "FROM forecast_samples WHERE feed_id=? "
                "ORDER BY variable, lead_hours",
                (feed_id,),
            ).fetchall()
        )
        assert sorted((row[2], row[5]) for row in forecast_projection) == [
            ("temperature", 1),
            ("temperature", 2),
            ("temperature", 168),
            ("wind", 1),
            ("wind", 2),
        ]

        budget_projection = sorted(
            tuple(row)
            for row in conn.execute(
                "SELECT source, calls, credits FROM api_budget "
                "WHERE source='visualcrossing'"
            ).fetchall()
        )
        jobs_projection = sorted(
            tuple(row)
            for row in conn.execute(
                "SELECT type, site_id, job_key, payload, status, retry_count, "
                "max_retries, last_error, result FROM jobs "
                "ORDER BY type, job_key"
            ).fetchall()
        )
        site_feed_state_projection = sorted(
            tuple(row)
            for row in conn.execute(
                "SELECT site_id, feed_id, enabled, last_error, error_count, "
                "grid_lat, grid_lon, grid_elevation_m FROM site_feed_state"
            ).fetchall()
        )

        close_db()
        return (
            forecast_projection,
            budget_projection,
            jobs_projection,
            site_feed_state_projection,
        )

    caplog.clear()
    off_projections = _run(tmp_path / "off-db", False)
    assert _probe_records(caplog) == []

    caplog.clear()
    on_projections = _run(tmp_path / "on-db", True)
    assert _probe_records(caplog) == [_VC_EXPECTED_LINE]

    assert off_projections == on_projections
