"""The `icon_eu` Open-Meteo feed (cases D1-T1 through D1-T9).

Fixtures use only the existing synthetic helpers (`asof_conn`,
`asof_make_site`, `asof_make_real_feed`) -- no real coordinates, station ids
or place names.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import timedelta

import httpx
import pytest

from tests.helpers import asof_conn
from wxverify import config
from wxverify.core.timeutil import floor_hour, isoformat_utc, parse_utc
from wxverify.db.migrations import (
    OPEN_METEO_HORIZON_CORRECTION_KEY,
    OPEN_METEO_INTERVAL_CORRECTION_KEY,
    create_schema,
    run_migrations,
)
from wxverify.feeds.open_meteo import (
    RUN_AVAILABILITY_LAG_MINUTES,
    RUN_CADENCE_HOURS,
    OpenMeteoAdapter,
    _run_shape,  # noqa: SLF001
    _snap_run,  # noqa: SLF001
)
from wxverify.feeds.seam import ForecastRequest
from wxverify.web.context import feed_description

# The seven models seeded before icon_eu was added.
# (test_open_meteo_horizon_correction.py:28-42)
_SEVEN_ORIGINAL_MODELS: tuple[str, ...] = (
    "ecmwf_ifs",
    "gfs_global",
    "icon_global",
    "gem_global",
    "meteofrance_arpege_world",
    "jma_gsm",
    "ukmo_global_deterministic_10km",
)


def _bare_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    return conn


def _icon_eu_row(conn: sqlite3.Connection) -> sqlite3.Row:
    rows = conn.execute(
        "SELECT id, enabled, disabled_reason, default_subscribed,"
        " fetch_interval_minutes, max_lead_hours, is_virtual"
        " FROM feeds WHERE source='open-meteo' AND model='icon_eu'"
    ).fetchall()
    assert len(rows) == 1
    return rows[0]


def _assert_icon_eu_seed_fields(row: sqlite3.Row) -> None:
    assert (
        row["enabled"],
        row["disabled_reason"],
        row["default_subscribed"],
        row["fetch_interval_minutes"],
        row["max_lead_hours"],
        row["is_virtual"],
    ) == (1, None, 1, 360, 120, 0)


def _icon_eu_feed_id(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' AND model='icon_eu'"
    ).fetchone()
    assert row is not None
    return int(row["id"])


# ---------------------------------------------------------------------------
# D1-T1 -- the seed row.
# ---------------------------------------------------------------------------


def test_d1_t1a_fresh_database_seeds_exactly_one_icon_eu_row() -> None:
    conn = asof_conn()
    _assert_icon_eu_seed_fields(_icon_eu_row(conn))


def test_d1_t1b_pre_icon_eu_database_gains_icon_eu_with_ids_otherwise_unchanged() -> (
    None
):
    conn = _bare_db()
    original_ids: list[int] = []
    for model in _SEVEN_ORIGINAL_MODELS:
        cur = conn.execute(
            """
            INSERT INTO feeds
                (source, model, enabled, default_subscribed,
                 fetch_interval_minutes, max_lead_hours, is_virtual)
            VALUES ('open-meteo', ?, 1, 1, 360, 168, 0)
            """,
            (model,),
        )
        assert cur.lastrowid is not None
        original_ids.append(int(cur.lastrowid))
    conn.commit()

    run_migrations(conn)

    for model, expected_id in zip(_SEVEN_ORIGINAL_MODELS, original_ids, strict=True):
        row = conn.execute(
            "SELECT id FROM feeds WHERE source='open-meteo' AND model=?", (model,)
        ).fetchone()
        assert row is not None
        assert int(row["id"]) == expected_id

    icon_eu_row = _icon_eu_row(conn)
    _assert_icon_eu_seed_fields(icon_eu_row)
    assert int(icon_eu_row["id"]) > max(original_ids)


def test_d1_t1c_deleting_the_icon_eu_row_then_remigrating_restores_it() -> None:
    conn = asof_conn()
    conn.execute("DELETE FROM feeds WHERE source='open-meteo' AND model='icon_eu'")
    conn.commit()
    assert (
        conn.execute(
            "SELECT COUNT(*) AS n FROM feeds"
            " WHERE source='open-meteo' AND model='icon_eu'"
        ).fetchone()["n"]
        == 0
    )

    run_migrations(conn)

    _assert_icon_eu_seed_fields(_icon_eu_row(conn))


# ---------------------------------------------------------------------------
# D1-T2 -- run labels.
# ---------------------------------------------------------------------------


def test_d1_t2_run_shape_is_3_hour_cadence_90_minute_lag() -> None:
    assert _run_shape("icon_eu") == (3, 90)


@pytest.mark.parametrize(
    ("t", "expected"),
    [
        ("2026-06-02T04:30:00Z", "2026-06-02T03:00:00Z"),
        ("2026-06-02T04:29:59Z", "2026-06-02T00:00:00Z"),
        ("2026-06-02T05:30:00Z", "2026-06-02T03:00:00Z"),
        ("2026-06-02T01:29:59Z", "2026-06-01T21:00:00Z"),
    ],
)
def test_d1_t2_snap_run_table(t: str, expected: str) -> None:
    assert _snap_run("icon_eu", t) == expected


# ---------------------------------------------------------------------------
# D1-T3 -- the three mappings and the description.
# ---------------------------------------------------------------------------


def test_d1_t3_icon_eu_mappings() -> None:
    assert RUN_CADENCE_HOURS["icon_eu"] == 3
    assert RUN_AVAILABILITY_LAG_MINUTES["icon_eu"] == 90
    assert config.OPEN_METEO_MAX_LEAD_HOURS["icon_eu"] == 120
    assert config.OPEN_METEO_FETCH_INTERVAL_MINUTES["icon_eu"] == 360


def test_d1_t3_every_open_meteo_model_has_a_non_empty_description() -> None:
    for model in RUN_CADENCE_HOURS:
        assert feed_description("open-meteo", model) != ""


# ---------------------------------------------------------------------------
# D1-T4 -- operator values survive migration.
# ---------------------------------------------------------------------------


def test_d1_t4_operator_values_survive_remigration_even_with_markers_deleted() -> None:
    conn = asof_conn()

    def _set_operator_values() -> None:
        conn.execute(
            "UPDATE feeds SET fetch_interval_minutes=180, enabled=0,"
            " default_subscribed=0 WHERE source='open-meteo' AND model='icon_eu'"
        )
        conn.commit()

    def _assert_operator_values_unchanged() -> None:
        row = conn.execute(
            "SELECT fetch_interval_minutes, enabled, default_subscribed FROM feeds"
            " WHERE source='open-meteo' AND model='icon_eu'"
        ).fetchone()
        assert row is not None
        assert (
            row["fetch_interval_minutes"],
            row["enabled"],
            row["default_subscribed"],
        ) == (180, 0, 0)

    _set_operator_values()
    run_migrations(conn)
    _assert_operator_values_unchanged()

    # Repeat with both correction markers deleted, so `correct_open_meteo_horizons`
    # and `correct_open_meteo_fetch_intervals` actually run their UPDATEs (not
    # short-circuited by the marker gate) -- and still leave icon_eu untouched,
    # because their WHERE clauses target the pre-correction values (168h /
    # 360min), which icon_eu's row no longer carries.
    conn.execute(
        "DELETE FROM runtime_state WHERE key IN (?, ?)",
        (OPEN_METEO_HORIZON_CORRECTION_KEY, OPEN_METEO_INTERVAL_CORRECTION_KEY),
    )
    conn.commit()
    run_migrations(conn)
    _assert_operator_values_unchanged()


# ---------------------------------------------------------------------------
# D1-T5 -- schedule and cost.
# ---------------------------------------------------------------------------


def test_d1_t5_cost_estimate_uses_the_seeded_max_lead_hours() -> None:
    conn = asof_conn()
    seeded_max_lead_hours = int(_icon_eu_row(conn)["max_lead_hours"])
    assert seeded_max_lead_hours == 120

    req = ForecastRequest(
        lat=0.0,
        lon=0.0,
        model="icon_eu",
        variables=("temperature", "wind", "precip"),
        max_lead_hours=seeded_max_lead_hours,
    )
    adapter = OpenMeteoAdapter(httpx.AsyncClient())
    assert adapter.estimate_cost(req).calls == 1


# ---------------------------------------------------------------------------
# D1-T9 -- labels are estimates: a label only sets the lead, and nothing cuts
# at a horizon derived from it.
# ---------------------------------------------------------------------------


def test_d1_t9a_label_arithmetic_across_one_day() -> None:
    start = parse_utc("2026-06-02T00:00:00Z")
    for i in range(288):
        t = start + timedelta(minutes=5 * i)
        label = parse_utc(_snap_run("icon_eu", isoformat_utc(t)))

        assert label.hour % 3 == 0
        assert (label.minute, label.second, label.microsecond) == (0, 0, 0)

        delta = t - label
        assert timedelta(minutes=90) <= delta < timedelta(minutes=270)

        floor_delta = floor_hour(t) - label
        assert floor_delta in (
            timedelta(hours=1),
            timedelta(hours=2),
            timedelta(hours=3),
            timedelta(hours=4),
        )

        next_label = parse_utc(
            _snap_run("icon_eu", isoformat_utc(t + timedelta(minutes=360)))
        )
        assert next_label == label + timedelta(hours=6)
        assert next_label.hour % 6 == label.hour % 6


def test_d1_t9b_adapter_normalization_keeps_every_lead_up_to_120(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Companion to D1-T9b above, one layer lower: this pins the adapter's
    # OWN lead cutoff against a mutant that caps icon_eu leads at a
    # hardcoded 117 instead of the true, config-driven 120 h horizon -- a
    # 121-step hourly body (hours 0..120) must normalize to exactly leads
    # 1..120, with nothing dropped above 117. Driven through
    # ``fetch_forecast`` (not ``_samples_from_hourly`` directly) so a
    # cut-off placed anywhere in ``feeds/open_meteo.py`` is caught, not
    # just one placed inside the normalization helper.
    import wxverify.feeds.open_meteo as open_meteo_module

    monkeypatch.setattr(
        open_meteo_module, "utc_now", lambda: parse_utc("2026-06-02T10:30:00Z")
    )
    label = parse_utc("2026-06-02T09:00:00Z")  # _snap_run("icon_eu") of the above
    times = [isoformat_utc(label + timedelta(hours=h)) for h in range(121)]

    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            200,
            json={
                "latitude": 0.0,
                "longitude": 0.0,
                "hourly": {
                    "time": times,
                    "temperature_2m": [10.0 for _ in times],
                    "wind_speed_10m": [5.0 for _ in times],
                    "precipitation": [0.0 for _ in times],
                },
            },
        )

    adapter = OpenMeteoAdapter(
        httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )
    req = ForecastRequest(
        lat=0.0,
        lon=0.0,
        model="icon_eu",
        variables=("temperature", "wind", "precip"),
        max_lead_hours=120,
    )
    result = asyncio.run(adapter.fetch_forecast(req))

    leads_by_variable: dict[str, list[int]] = {
        "temperature": [],
        "wind": [],
        "precip": [],
    }
    for sample in result.samples:
        leads_by_variable[sample.variable].append(sample.lead_hours)
    # Precip is labelled by the start of its hour, one hour
    # earlier than temperature/wind: its top raw hour (120) now labels lead
    # 119, and its lead-0 drop lands on raw hour 1 instead of raw hour 0, so
    # its normalized range is 1..119 while temperature and wind stay 1..120.
    for variable, leads in leads_by_variable.items():
        expected = range(1, 119 + 1) if variable == "precip" else range(1, 121)
        assert sorted(leads) == list(expected), variable
