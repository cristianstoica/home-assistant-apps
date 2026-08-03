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
    _provider_status,  # noqa: SLF001
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

    # member_a has the lower feed_id, so the underlying UNIQUE autoindex scan
    # (ordered by feed_id first) encounters "wind" before "temperature" --
    # the opposite of `variables`' sorted() order. member_a=temperature,
    # member_b=wind (the previous assignment) would have left index order
    # and sorted() order coincidentally identical, so dropping the
    # `sorted()` call in sample_metrics would still have passed this
    # assertion; swapping which member holds which variable makes it catch
    # that regression.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=member_a,
        variable="wind",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T06:00:00Z",
        value=10.0,
        model_run_id="run-a",
    )
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=member_b,
        variable="temperature",
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


def test_bad_sample_count_counts_all_three_invalidity_kinds_exactly(
    tmp_path: Path,
) -> None:
    """invalid_forecast_sample_sql has five OR-ed branches; the shipped test
    before this one only ever seeds the out-of-range-value and unknown-
    variable branches, leaving the two `NOT LIKE` timestamp branches --
    which are also part of the partial index's on-disk predicate -- with no
    row that trips them. Seed one row per kind (out-of-range value, unknown
    variable, malformed issued_at, malformed valid_at) and require the count
    to be exactly 4, not merely non-zero, so a branch that silently stops
    matching (e.g. a typo'd LIKE pattern) is caught rather than masked by
    the others.
    """
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
        variable="humidity",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T07:00:00Z",
        value=5.0,
    )
    # Malformed issued_at: no "T"/"Z", so it fails the LIKE pattern that
    # idx_samples_invalid's stored predicate also carries. variable/value
    # are otherwise valid so this trips only the issued_at branch.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-07-01 00:00:00",
        valid_at="2026-01-01T08:00:00Z",
        value=5.0,
    )
    # Malformed valid_at: same shape as above but on the other timestamp
    # column, so this trips only the valid_at NOT LIKE branch -- variable
    # and value are otherwise valid and issued_at is well-formed.
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="precip",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-07-01 09:00:00",
        value=5.0,
    )

    metrics = sample_metrics(conn, site_id, feed_id)
    assert metrics.sample_count == 4
    assert metrics.bad_sample_count == 4


def test_model_run_count_excludes_empty_model_run_id(tmp_path: Path) -> None:
    """model_run_count_sql's TRIM(model_run_id) != '' guard excludes rows
    with an empty (or whitespace-only) model_run_id from the distinct count.
    No fixture anywhere else uses an empty model_run_id, so deleting that
    guard currently breaks no test -- this seeds exactly that state.
    """
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
        value=10.0,
        model_run_id="run-1",
    )
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-01-01T01:00:00Z",
        valid_at="2026-01-01T07:00:00Z",
        value=11.0,
        model_run_id="",
    )

    metrics = sample_metrics(conn, site_id, feed_id)
    assert metrics.sample_count == 2
    assert metrics.model_run_count == 1


def test_zero_sample_feed_status_reports_never_run_and_ran_no_data(
    tmp_path: Path,
) -> None:
    """A zero-sample feed's SampleMetrics feeds into _provider_status, which
    must still distinguish "never run" (no last_run_at) from "ran but got
    nothing" (last_run_at set, sample_count still 0) -- both reachable from
    the same zero-sample metrics, distinguished only by last_run_at.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    metrics = sample_metrics(conn, site_id, feed_id)
    assert metrics.sample_count == 0

    never_run = _provider_status(
        site_enabled=True,
        applicable=True,
        subscribed=True,
        last_run_at=None,
        last_error=None,
        sample_count=metrics.sample_count,
    )
    assert never_run == "never run / due"

    ran_no_data = _provider_status(
        site_enabled=True,
        applicable=True,
        subscribed=True,
        last_run_at="2026-01-01T00:00:00Z",
        last_error=None,
        sample_count=metrics.sample_count,
    )
    assert ran_no_data == "ran / no usable data"


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

    # smoke_stored_sample_check intersects metrics.variables against
    # FORECAST_VARIABLES to report missing ones. A comma-delimited aggregate
    # would have split "temp,extra" into "temp" and "extra" -- neither a
    # real FORECAST_VARIABLES member -- which accidentally satisfies no
    # membership test either way; the real failure mode this guards is a
    # feed that genuinely never delivered a variable being reported healthy
    # because the corrupted string happened to intersect the expected set.
    # This feed has stored none of the real FORECAST_VARIABLES, so the check
    # must still report every one of them missing rather than going quiet.
    check = smoke_stored_sample_check(conn, site_id, feed_id)
    assert not check.ok
    missing_reason = next(
        reason for reason in check.reasons if reason.startswith("missing variables:")
    )
    assert "temperature" in missing_reason
    assert "wind" in missing_reason
    assert "precip" in missing_reason


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

_PUBLISHED_HEALTH_PROVIDERS_GROUP_KEYS = frozenset(
    {
        "source",
        "key_required",
        "key_present",
        "source_seeded",
        "budget",
        "feeds",
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
        assert set(group.keys()) == _PUBLISHED_HEALTH_PROVIDERS_GROUP_KEYS
        for feed in group["feeds"]:
            saw_a_feed = True
            assert set(feed.keys()) == _PUBLISHED_HEALTH_PROVIDERS_FEED_KEYS
            assert "source" not in feed
    assert saw_a_feed, "no group carried any feed; the contract check is vacuous"


def _insert_blob_variable_sample(
    conn: sqlite3.Connection, *, site_id: int, feed_id: int
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id)
        VALUES (?, ?, ?, '2026-01-01T00:00:00Z', '2026-01-01T06:00:00Z', 24,
                10.0, '{}', 'run-1')
        """,
        (site_id, feed_id, b"temperature"),
    )


def test_sample_metrics_survives_a_blob_variable_value(tmp_path: Path) -> None:
    """`variable` is declared TEXT but the table is not STRICT, and the
    DB-import validator only checks integrity/user_version/table presence,
    not storage classes, so a restored database can legitimately contain a
    BLOB in this column. Pre-fix, ``json_group_array(DISTINCT variable)``
    raised ``OperationalError: JSON cannot hold BLOB values`` on exactly
    this row -- and /api/health/providers has no exception handling around
    sample_metrics, so that raise 500'd the whole route.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_blob_variable_sample(conn, site_id=site_id, feed_id=feed_id)

    metrics = sample_metrics(conn, site_id, feed_id)

    assert metrics.sample_count == 1
    # BLOB values are hex-quoted via SQLite's quote() before being folded
    # into the JSON array; b"temperature" hex-encodes to this literal.
    assert metrics.variables == ("X'74656D7065726174757265'",)


def test_health_providers_route_returns_200_with_a_blob_variable_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_db()
    config.db_path = str(tmp_path / "health-providers-blob.db")
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")

    seeded_feed_id: list[int] = []

    def _seed(conn: sqlite3.Connection) -> None:
        _seed_a_site(conn)
        site_row = conn.execute(
            "SELECT id FROM sites WHERE name='HealthProvidersRoute'"
        ).fetchone()
        assert site_row is not None
        feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
        seeded_feed_id.append(feed_id)
        _insert_blob_variable_sample(conn, site_id=int(site_row["id"]), feed_id=feed_id)

    with TestClient(app) as client:
        get_db().write_sync(_seed)
        response = client.get("/api/health/providers")
    assert response.status_code == 200
    groups = response.json()
    feed_ids = {feed["feed_id"] for group in groups for feed in group["feeds"]}
    assert feed_ids, "fixture produced no feeds; the route-level oracle is vacuous"
    assert len(seeded_feed_id) == 1
    matches = [
        feed
        for group in groups
        for feed in group["feeds"]
        if feed["feed_id"] == seeded_feed_id[0]
    ]
    assert len(matches) == 1
    # Pins the BLOB path all the way through JSON serialization to the
    # response body the Home Assistant consumer reads, not merely that the
    # route didn't raise -- b"temperature" hex-quoted by quote().
    assert matches[0]["variables"] == ["X'74656D7065726174757265'"]
