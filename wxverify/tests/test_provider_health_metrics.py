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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.core.options import SECRET_ENV
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.provider_ops import (
    SampleMetrics,
    _provider_status,  # noqa: SLF001
    provider_doctor_failures,
    provider_health,
    recent_sample_metrics,
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
    """A zero-sample feed feeds into _provider_status, which must still
    distinguish "never run" (no last_run_at) from "ran but got nothing"
    (last_run_at set, still no stored sample anywhere in history) -- both
    reachable from the same sample-free state, distinguished only by
    last_run_at.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    metrics = recent_sample_metrics(
        conn, site_id, feed_id, window_start="2026-01-01T00:00:00Z"
    )
    assert metrics.sample_count == 0
    assert metrics.has_any_samples is False

    never_run = _provider_status(
        site_enabled=True,
        applicable=True,
        subscribed=True,
        last_run_at=None,
        last_error=None,
        has_any_samples=metrics.has_any_samples,
    )
    assert never_run == "never run / due"

    ran_no_data = _provider_status(
        site_enabled=True,
        applicable=True,
        subscribed=True,
        last_run_at="2026-01-01T00:00:00Z",
        last_error=None,
        has_any_samples=metrics.has_any_samples,
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
        "metrics_schema",
        "metrics_window_start",
        "recent_sample_count",
        "recent_variables",
        "recent_model_run_count",
        "recent_latest_issued_at",
        "recent_valid_from",
        "recent_valid_to",
        "bad_sample_count",
    }
)

# The lifetime-scoped spellings the bounded-window rename removed. A consumer
# still reading one of these must fail loudly on a missing key, never receive
# a silently re-scoped value under the old name -- so their absence is
# asserted explicitly, not merely implied by the exact-set equality above.
_REMOVED_HEALTH_PROVIDERS_FEED_KEYS = frozenset(
    {
        "sample_count",
        "variables",
        "model_run_count",
        "latest_issued_at",
        "valid_from",
        "valid_to",
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
            assert not (set(feed.keys()) & _REMOVED_HEALTH_PROVIDERS_FEED_KEYS)
            assert feed["metrics_schema"] == 2
            assert "source" not in feed
    assert saw_a_feed, "no group carried any feed; the contract check is vacuous"


def _health_providers_groups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, db_name: str
) -> list[dict[str, object]]:
    close_db()
    config.db_path = str(tmp_path / db_name)
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        get_db().write_sync(_seed_a_site)
        response = client.get("/api/health/providers")
    assert response.status_code == 200
    groups = response.json()
    assert isinstance(groups, list)
    return groups


def test_missing_required_key_predicate_matches_the_real_response_shape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Downstream consumers of /api/health/providers flag a source needing
    attention by reading exactly two group-level fields -- key_required and
    key_present. The predicate is reimplemented here inline, not imported,
    so a rename of either field or a demotion of either to per-feed scope
    breaks this test instead of silently tracking the source.
    """

    def _missing_required_key(group: dict[str, object] | None) -> bool | None:
        if group is None:
            return None
        return bool(group["key_required"]) and not bool(group["key_present"])

    assert _missing_required_key(None) is None

    for env_name in SECRET_ENV.values():
        monkeypatch.delenv(env_name, raising=False)
    unsatisfied_groups = _health_providers_groups(
        tmp_path, monkeypatch, "health-providers-no-keys.db"
    )
    assert unsatisfied_groups, "fixture produced no groups; the check is vacuous"
    for group in unsatisfied_groups:
        assert "key_required" in group
        assert "key_present" in group
    assert any(_missing_required_key(group) is True for group in unsatisfied_groups), (
        "no group required a missing key; the positive case is vacuous"
    )

    for env_name in SECRET_ENV.values():
        monkeypatch.setenv(env_name, "synthetic-test-key")
    satisfied_groups = _health_providers_groups(
        tmp_path, monkeypatch, "health-providers-all-keys.db"
    )
    assert satisfied_groups, "fixture produced no groups; the check is vacuous"
    for group in satisfied_groups:
        assert "key_required" in group
        assert "key_present" in group
        assert _missing_required_key(group) is False


def _insert_blob_variable_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    issued_at: str = "2026-01-01T00:00:00Z",
    valid_at: str = "2026-01-01T06:00:00Z",
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id)
        VALUES (?, ?, ?, ?, ?, 24, 10.0, '{}', 'run-1')
        """,
        (site_id, feed_id, b"temperature", issued_at, valid_at),
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
        # Far-future stamps keep the row inside the route's recent metrics
        # window regardless of the real clock the route computes it from.
        _insert_blob_variable_sample(
            conn,
            site_id=int(site_row["id"]),
            feed_id=feed_id,
            issued_at="2035-01-01T00:00:00Z",
            valid_at="2035-01-01T06:00:00Z",
        )

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
    assert matches[0]["recent_variables"] == ["X'74656D7065726174757265'"]


def test_recent_metrics_include_the_boundary_row_at_window_start(
    tmp_path: Path,
) -> None:
    """The window filter is inclusive (issued_at >= window_start): a row
    issued exactly at the cutoff instant belongs to the window, a row one
    day earlier does not, and a later row does. Pinning all three keeps an
    off-by-one drift to a strict `>` from passing silently.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2026-01-04T00:00:00Z",
        valid_at="2026-01-04T06:00:00Z",
        value=1.0,
        model_run_id="run-old",
    )
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="wind",
        issued_at="2026-01-05T00:00:00Z",
        valid_at="2026-01-05T06:00:00Z",
        value=2.0,
        model_run_id="run-boundary",
    )
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="precip",
        issued_at="2026-01-06T00:00:00Z",
        valid_at="2026-01-06T06:00:00Z",
        value=3.0,
        model_run_id="run-new",
    )

    metrics = recent_sample_metrics(
        conn, site_id, feed_id, window_start="2026-01-05T00:00:00Z"
    )

    assert metrics.sample_count == 2
    assert metrics.variables == ("precip", "wind")
    assert metrics.model_run_count == 2
    assert metrics.latest_issued_at == "2026-01-06T00:00:00Z"
    assert metrics.valid_from == "2026-01-05T06:00:00Z"
    assert metrics.valid_to == "2026-01-06T06:00:00Z"
    assert metrics.window_start == "2026-01-05T00:00:00Z"
    assert metrics.has_any_samples is True


def test_provider_health_shares_one_window_start_across_all_feeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The cutoff is computed once per call, not once per feed. The patched
    clock advances a full day on every read, so a per-feed recomputation
    would leak distinct metrics_window_start values across one response.
    """
    ticks = iter(range(1, 1000))

    def _advancing_now() -> datetime:
        return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=next(ticks))

    monkeypatch.setattr("wxverify.core.timeutil.utc_now", _advancing_now)
    conn = _init_tmp_db(tmp_path)
    _insert_site(conn)

    health = provider_health(conn)

    feeds = [
        feed
        for group in health
        for feed in cast(list[dict[str, object]], group["feeds"])
    ]
    assert len(feeds) >= 2, "single-feed fixture cannot detect per-feed recomputation"
    window_starts = {feed["metrics_window_start"] for feed in feeds}
    assert len(window_starts) == 1


def test_smoke_check_sees_samples_older_than_the_metrics_window(
    tmp_path: Path,
) -> None:
    """smoke_stored_sample_check stays lifetime-scoped: samples far older
    than any plausible recent window still count as stored, complete data
    there -- ok is True and there are no reasons at all -- even while the
    windowed metrics for the same feed report an empty window.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    for variable in ("temperature", "wind", "precip"):
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable=variable,
            issued_at="2026-01-01T00:00:00Z",
            valid_at="2026-01-01T06:00:00Z",
            value=5.0,
        )

    check = smoke_stored_sample_check(conn, site_id, feed_id)
    assert "no stored samples" not in check.reasons
    assert check.metrics.sample_count == 3
    assert check.ok is True
    assert check.reasons == ()

    metrics = recent_sample_metrics(
        conn, site_id, feed_id, window_start="2026-02-01T00:00:00Z"
    )
    assert metrics.sample_count == 0
    assert metrics.has_any_samples is True


def test_quiet_but_healthy_feed_reports_ok_with_empty_recent_metrics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A feed that ran successfully and stored data, but has been quiet
    longer than the metrics window, publishes empty recent metrics while its
    status stays "ok" (status reads lifetime existence, not the windowed
    count) and the doctor raises no failure for it.
    """
    close_db()
    config.db_path = str(tmp_path / "health-providers-quiet.db")
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
        site_id = int(site_row["id"])
        feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
        seeded_feed_id.append(feed_id)
        # Stored data well in the past -- outside any real-clock 7-day
        # window -- plus a clean successful-run record.
        _insert_sample(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            variable="temperature",
            issued_at="2026-01-01T00:00:00Z",
            valid_at="2026-01-01T06:00:00Z",
            value=5.0,
        )
        conn.execute(
            """
            INSERT INTO site_feed_state
                (site_id, feed_id, enabled, last_run_at, error_count)
            VALUES (?, ?, 1, '2026-01-01T00:05:00Z', 0)
            """,
            (site_id, feed_id),
        )

    with TestClient(app) as client:
        get_db().write_sync(_seed)
        response = client.get("/api/health/providers")
    assert response.status_code == 200
    groups = response.json()
    assert len(seeded_feed_id) == 1
    matches = [
        feed
        for group in groups
        for feed in group["feeds"]
        if feed["feed_id"] == seeded_feed_id[0]
    ]
    assert len(matches) == 1
    feed = matches[0]
    assert feed["recent_sample_count"] == 0
    assert feed["recent_variables"] == []
    assert feed["recent_latest_issued_at"] is None
    assert feed["status"] == "ok"

    failures = provider_doctor_failures(groups)
    prefix = f"open-meteo site={feed['site_id']} feed={feed['feed_id']}"
    assert not any(failure.startswith(prefix) for failure in failures)


def test_bad_sample_count_stays_lifetime_and_still_fails_the_doctor(
    tmp_path: Path,
) -> None:
    """bad_sample_count keeps its lifetime scope: an invalid stored row
    older than the metrics window still counts, and the doctor still raises
    "invalid stored samples" for the feed -- integrity findings must not age
    out of the report just because the row left the display window.
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

    metrics = recent_sample_metrics(
        conn, site_id, feed_id, window_start="2026-02-01T00:00:00Z"
    )
    assert metrics.sample_count == 0
    assert metrics.bad_sample_count == 1

    health = provider_health(conn, window_start="2026-02-01T00:00:00Z")
    failures = provider_doctor_failures(health)
    prefix = f"open-meteo site={site_id} feed={feed_id}"
    assert f"{prefix}: invalid stored samples" in failures


def test_recent_metrics_roll_up_only_in_window_meteoblue_members(
    tmp_path: Path,
) -> None:
    """The meteoblue package roll-up and the window filter compose: a member
    sample inside the window counts toward the package's recent metrics, a
    member sample outside it does not -- but both still register lifetime
    existence.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    member_a = _insert_meteoblue_member(conn, "ecmwf")
    member_b = _insert_meteoblue_member(conn, "gfs")
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=member_a,
        variable="temperature",
        issued_at="2026-01-10T00:00:00Z",
        valid_at="2026-01-10T06:00:00Z",
        value=5.0,
        model_run_id="run-in",
    )
    _insert_sample(
        conn,
        site_id=site_id,
        feed_id=member_b,
        variable="wind",
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-01T06:00:00Z",
        value=10.0,
        model_run_id="run-out",
    )

    metrics = recent_sample_metrics(
        conn, site_id, package_id, window_start="2026-01-05T00:00:00Z"
    )

    assert metrics.sample_count == 1
    assert metrics.variables == ("temperature",)
    assert metrics.model_run_count == 1
    assert metrics.latest_issued_at == "2026-01-10T00:00:00Z"
    assert metrics.has_any_samples is True


def test_empty_window_reports_zeroed_metrics_without_forgetting_history(
    tmp_path: Path,
) -> None:
    """A window that contains no rows at all yields the zero/None/() shape
    for every windowed field while both lifetime fields keep reporting the
    stored history: existence stays True and bad_sample_count keeps its
    lifetime tally.
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
        value=5.0,
    )

    metrics = recent_sample_metrics(
        conn, site_id, feed_id, window_start="2027-01-01T00:00:00Z"
    )

    assert metrics.sample_count == 0
    assert metrics.variables == ()
    assert metrics.model_run_count == 0
    assert metrics.latest_issued_at is None
    assert metrics.valid_from is None
    assert metrics.valid_to is None
    assert metrics.window_start == "2027-01-01T00:00:00Z"
    assert metrics.bad_sample_count == 0
    assert metrics.has_any_samples is True
