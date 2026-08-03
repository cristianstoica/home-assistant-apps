"""Tests for per-feed sample metrics and the /api/health/providers contract.

Covers three shapes production data can never itself produce -- a non-zero
bad_sample_count, a zero-sample feed, and a stored ``variable`` value that
contains a comma -- plus the meteoblue-member roll-up and the exact key set
the route publishes per feed.

Synthetic data only (public repo): fake site names, the repo's existing
47/25 lat-lon convention, no real station or device identifiers.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.provider_ops import (
    SampleMetrics,
    sample_metrics,
    smoke_stored_sample_check,
)


def _init_tmp_db(tmp_path: Path, name: str = "wxverify.db") -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / name
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _insert_site(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('ProviderHealth', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _insert_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    value: float,
    model_run_id: str = "run-1",
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id)
        VALUES (?, ?, ?, ?, ?, 24, ?, '{}', ?)
        """,
        (site_id, feed_id, variable, issued_at, valid_at, value, model_run_id),
    )


def _insert_meteoblue_member(conn: sqlite3.Connection, model: str) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO feeds (source, model, fetch_interval_minutes)
            VALUES ('meteoblue', ?, 360)
            """,
            (model,),
        ).lastrowid
    )


def test_sample_metrics_rolls_up_meteoblue_members_into_the_package(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    # meteoblue member models are not among the default seeded feeds -- only
    # the multimodel package feed is -- so the members this test rolls up are
    # hand-inserted here.
    member_a = _insert_meteoblue_member(conn, "ecmwf")
    member_b = _insert_meteoblue_member(conn, "gfs")

    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=member_a,
        variable="temperature",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T06:00:00Z",
        value=10.0,
        model_run_id="run-a",
    )
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=member_b,
        variable="wind",
        issued_at="2026-01-01T01:00:00Z",
        valid_at="2026-01-01T07:00:00Z",
        value=5.0,
        model_run_id="run-b",
    )

    metrics = sample_metrics(conn, site_id, package_id)

    assert metrics == SampleMetrics(
        sample_count=2,
        variables=("temperature", "wind"),
        model_run_count=2,
        latest_issued_at="2026-01-01T01:00:00Z",
        valid_from="2026-01-01T06:00:00Z",
        valid_to="2026-01-01T07:00:00Z",
        bad_sample_count=0,
    )


def test_bad_sample_count_flags_out_of_range_value_and_unknown_variable(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T06:00:00Z",
        value=999.0,
    )
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="dewpoint",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T07:00:00Z",
        value=5.0,
    )

    metrics = sample_metrics(conn, site_id, feed_id)
    assert metrics.sample_count == 2
    assert metrics.bad_sample_count == 2

    check = smoke_stored_sample_check(conn, site_id, feed_id)
    assert not check.ok
    assert any("bad samples" in reason for reason in check.reasons)


def test_zero_sample_feed_reports_empty_metrics_and_no_stored_samples(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    metrics = sample_metrics(conn, site_id, feed_id)
    assert metrics == SampleMetrics(0, (), 0, None, None, None, 0)

    check = smoke_stored_sample_check(conn, site_id, feed_id)
    assert not check.ok
    assert "no stored samples" in check.reasons
    assert any(reason.startswith("missing variables:") for reason in check.reasons)


def test_variable_containing_a_comma_survives_json_group_array_distinct(
    tmp_path: Path,
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")

    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temp,extra",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T06:00:00Z",
        value=10.0,
    )

    metrics = sample_metrics(conn, site_id, feed_id)

    # A comma-delimited aggregate (GROUP_CONCAT) could not tell this single
    # comma-containing value apart from two separate variables; the
    # comma-containing string must survive whole, as exactly one element.
    assert metrics.variables == ("temp,extra",)
    assert metrics.sample_count == 1


_PUBLISHED_HEALTH_PROVIDERS_FEED_KEYS = frozenset(
    {
        "site_id",
        "site_name",
        "feed_id",
        "model",
        "feed_enabled",
        "site_enabled",
        "subscribed",
        "applicable",
        "status",
        "last_run_at",
        "last_error",
        "error_count",
        "sample_count",
        "variables",
        "model_run_count",
        "latest_issued_at",
        "valid_from",
        "valid_to",
        "bad_sample_count",
    }
)


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


def _seed_a_site(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES ('HealthProvidersRoute', 47, 25, 900, 'UTC')
        """
    )


def test_health_providers_route_publishes_its_documented_key_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "health-providers-keys.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        get_db().write_sync(_seed_a_site)
        response = client.get("/api/health/providers")
    assert response.status_code == 200
    groups = response.json()
    assert groups, "fixture produced no provider groups; the contract check is vacuous"
    saw_a_feed = False
    for group in groups:
        assert "source" in group
        for feed in group["feeds"]:
            saw_a_feed = True
            assert set(feed.keys()) == _PUBLISHED_HEALTH_PROVIDERS_FEED_KEYS
            assert "source" not in feed
    assert saw_a_feed, "no group carried any feed; the contract check is vacuous"
