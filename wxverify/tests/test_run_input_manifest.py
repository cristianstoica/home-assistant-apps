"""§9 oracles for the explicit run-input manifest (``verification_run_inputs``).

O18 (the schema reaches a fresh database and an older one through
``create_schema`` alone, with no ``migrate_v*`` gate and no ``user_version``
bump -- D1), O19 (the import neutralizer's presence gate is unchanged --
D15), the write-path contract: O1 (exactly the declared components are
pinned), O2 (``config_truth_basis`` is the run row's own column, never a
recomputation -- D6), O20 (manifest rows are input provenance, not evidence,
so a failed attempt keeps them), O23 (``input_fingerprint`` and the
no-change gate are untouched by manifest rows -- D14); and the read path:
O3-O6 (the forecast gap closes, bounded on both sides and by the
watermark), O7/O8 (legacy runs: ``fresh`` is not claimable, ``changed``
survives -- D11), O9 (precedence -- D9), O10/O11 (the observed role is a
table lookup -- D8), O12/O13 (unrecognized names ignored, required names
checked), O14 (a untouched run is ``fresh`` end to end), O15 (read-path
purity), O21 (operator prose covers every component and note), O24 (the
value ladder), O25 (the import layering -- D17), O26 (one connection per
derivation).

Every fixture value is synthetic: invented site names, a hand-made 'UTC'
timezone, and ids that identify nothing. The site/truth builders come from
``tests.test_result_basis_fingerprint`` and the chain driver from
``tests.test_verification_run``, which already follow that convention.
"""

from __future__ import annotations

import ast
import json
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.helpers import asof_conn, asof_insert_pair, asof_insert_sample
from tests.test_phase7_surface import _make_app, _make_site, _seed_published_run
from tests.test_phase8_section18_oracles import (
    _by_v16,
    _fetch_page,
    _open_app_db,
    _read_only,
    _v16_markers,
)
from tests.test_publish_hold_bootstrap import (
    _evidence_row_counts,
    _insert_evidence_all_tables,
)
from tests.test_result_basis_fingerprint import (
    _A_PERIOD_DAYS,
    _insert_truth_day,
    _site_with_period,
    _status,
)
from tests.test_verification_run import (
    _W8_PAYLOAD,
    _decisions,
    _drive_chain,
    _make_verification_site,
)
from wxverify.collection.forecast_fetcher import persist_fetch_result
from wxverify.db.connection import Database
from wxverify.db.migrations import TARGET_USER_VERSION, create_schema, run_migrations
from wxverify.db.tz_generations import (
    ensure_published_generation,
    published_generation_id,
    published_pointer_key,
)
from wxverify.feeds.seam import FetchResult, NormalizedSample
from wxverify.verification import manifest
from wxverify.verification.freshness import published_input_freshness
from wxverify.verification.manifest import (
    FORECAST_ARRIVALS_ALGORITHM,
    MANIFEST_COMPONENT_CONFIG_TRUTH,
    MANIFEST_COMPONENT_FORECAST_ARRIVALS,
    MANIFEST_COMPONENT_ORDER,
    MANIFEST_COMPONENT_PAIR_ARRIVALS,
    MANIFEST_VERDICT_COMPONENTS,
    PAIR_ARRIVALS_ALGORITHM,
    run_input_freshness,
    write_run_manifest,
)
from wxverify.verification.runs import (
    RunConfig,
    capture_config_snapshot,
    fail_incomplete_attempts,
    input_fingerprint,
    published_run_id,
    result_basis_fingerprint,
    start_run,
)

# ---------------------------------------------------------------------------
# O18/O19 fixture builders.
# ---------------------------------------------------------------------------

# A database that predates the manifest, modelled the way the E-group models
# its "genuinely v4" starting point: the current schema minus the new table,
# stamped two versions back so ``run_migrations`` crosses real ``migrate_v*``
# gates on the way up. Kept symbolic so this file never carries a literal
# ``user_version`` and stays out of the next bump's edit list.
_PRE_MANIFEST_USER_VERSION = TARGET_USER_VERSION - 2


def _fresh_schema_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    return conn


def _pre_manifest_db(path: str = ":memory:") -> sqlite3.Connection:
    """A database built before this table existed, at an older version."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    conn.execute("DROP TABLE IF EXISTS verification_run_inputs")
    conn.execute(f"PRAGMA user_version = {_PRE_MANIFEST_USER_VERSION}")
    return conn


def _user_version(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA user_version").fetchone()[0])


def _run_inputs_shape(conn: sqlite3.Connection) -> list[tuple[str, str, int, int]]:
    """``(name, type, notnull, pk)`` per column, in declaration order."""
    return [
        (str(row["name"]), str(row["type"]), int(row["notnull"]), int(row["pk"]))
        for row in conn.execute("PRAGMA table_info(verification_run_inputs)")
    ]


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _insert_site(conn: sqlite3.Connection, name: str = "manifest-site") -> int:
    cur = conn.execute(
        """
        INSERT INTO sites
            (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
        VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)
        """,
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_run_row(
    conn: sqlite3.Connection, site_id: int, *, state: str, tz_generation_id: int
) -> int:
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, period_start, period_end,
             bootstrap_seed, bootstrap_resamples, input_fingerprint,
             result_basis_fingerprint)
        VALUES (?, ?, 1, '0.14.0', ?, 1, '{}', '2026-06-01', '2026-06-03',
                1, 1, 'fp-' || ?, ?)
        """,
        (site_id, tz_generation_id, state, state, "rb1:" + "0" * 64),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


# ---------------------------------------------------------------------------
# O18 -- migration: the table arrives through ``create_schema`` alone (D1).
# ---------------------------------------------------------------------------

_EXPECTED_SHAPE = [
    ("run_id", "INTEGER", 1, 1),
    ("component", "TEXT", 1, 2),
    ("value", "TEXT", 1, 0),
    ("scope", "TEXT", 1, 0),
]


def test_o18a_fresh_and_migrated_databases_agree_on_the_table_shape() -> None:
    """O18 (a) -> at ``fresh == migrated``: correct = identical
    ``table_info`` (names, order, types, NOT NULL, PK) on both paths;
    mutant M21 (DDL moved into a ``migrate_v7`` behind a bump) leaves the
    ``create_schema``-only database without the table, so the two lists
    diverge."""
    fresh = _fresh_schema_conn()
    migrated = _pre_manifest_db()
    run_migrations(migrated)

    assert _run_inputs_shape(fresh) == _EXPECTED_SHAPE
    assert _run_inputs_shape(migrated) == _run_inputs_shape(fresh)


def test_o18b_run_migrations_twice_in_a_row_is_clean() -> None:
    """O18 (b) -> at the second call not raising and the version holding:
    ``CREATE TABLE IF NOT EXISTS`` re-runs cleanly on every boot."""
    conn = _pre_manifest_db()
    run_migrations(conn)
    first = _user_version(conn)
    run_migrations(conn)
    assert _user_version(conn) == first == TARGET_USER_VERSION
    assert _run_inputs_shape(conn) == _EXPECTED_SHAPE


def test_o18c_migrating_a_database_with_published_runs_adds_an_empty_table() -> None:
    """O18 (c) -> at ``count == 0`` and the version pin: an existing install
    gains the table with zero rows (every prior run takes the D11 fallback)
    and lands on ``TARGET_USER_VERSION`` -- imported, never retyped, so this
    file is not on the next bump's edit list."""
    conn = _pre_manifest_db()
    site_id = _insert_site(conn)
    gen = ensure_published_generation(conn, site_id)
    _insert_run_row(conn, site_id, state="published", tz_generation_id=gen)
    conn.commit()
    assert "verification_run_inputs" not in _table_names(conn)

    run_migrations(conn)

    assert "verification_run_inputs" in _table_names(conn)
    count = conn.execute("SELECT COUNT(*) FROM verification_run_inputs").fetchone()
    assert int(count[0]) == 0
    assert _user_version(conn) == TARGET_USER_VERSION


def test_o18d_create_schema_alone_produces_the_table_without_any_gate() -> None:
    """O18 (d) -> at the shape equality on a database whose ``user_version``
    is still 0: no ``migrate_v*`` gate was crossed, so the table can only
    have come from ``create_schema``. Mutant M21 makes this list empty."""
    fresh = _fresh_schema_conn()
    assert _user_version(fresh) == 0, "create_schema must not touch user_version"

    migrated = _pre_manifest_db()
    run_migrations(migrated)

    assert _run_inputs_shape(fresh) != []
    assert _run_inputs_shape(fresh) == _run_inputs_shape(migrated)


# ---------------------------------------------------------------------------
# O19 -- the import neutralizer's presence gate is unchanged (D15).
# ---------------------------------------------------------------------------


def test_o19_pre_manifest_export_is_still_neutralized_on_import(
    tmp_path: Path,
) -> None:
    """O19 -> at ``state == 'failed'``: a staged upload carrying the five
    legacy verification tables and NO ``verification_run_inputs`` still
    has its running chain neutralized. Mutant M16 (adding the new table to
    ``_VERIFICATION_RUN_TABLES``) fails the all-of gate for every
    pre-manifest export, so the run stays ``running``."""
    from wxverify.api.routes.db_transfer import _neutralize_imported_verification_chains
    from wxverify.worker.verification_run import verification_job_key

    donor_path = tmp_path / "donor-o19.db"
    conn = _pre_manifest_db(str(donor_path))
    site_id = _insert_site(conn)
    gen = ensure_published_generation(conn, site_id)
    running_id = _insert_run_row(conn, site_id, state="running", tz_generation_id=gen)
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES "
        "('verification_run', ?, ?, 'pending')",
        (site_id, verification_job_key(site_id)),
    )
    conn.commit()
    names = _table_names(conn)
    conn.close()
    # The five tables the gate has always required, spelled out rather than
    # read from the tuple, so the assertion below is on behaviour.
    assert {
        "verification_runs",
        "verification_evidence",
        "verification_day_context",
        "verification_results",
        "verification_verdicts",
    } <= names
    assert "verification_run_inputs" not in names

    _neutralize_imported_verification_chains(donor_path)

    check = sqlite3.connect(str(donor_path))
    check.row_factory = sqlite3.Row
    try:
        run = check.execute(
            "SELECT state FROM verification_runs WHERE id = ?", (running_id,)
        ).fetchone()
        assert run is not None
        assert str(run["state"]) == "failed"
        job = check.execute(
            "SELECT status FROM jobs WHERE type = 'verification_run'"
        ).fetchone()
        assert job is not None
        assert str(job["status"]) == "failed"
    finally:
        check.close()


# ---------------------------------------------------------------------------
# Write-path fixtures (O1, O2, O20, O23 (a)).
# ---------------------------------------------------------------------------

_RUN_NOW = datetime(2027, 1, 1, tzinfo=UTC)


def _started_site(
    conn: sqlite3.Connection,
) -> tuple[int, int, list[int], dict[str, object]]:
    """A published site over ``_A_PERIOD_DAYS`` with a few forecast samples
    already on file, so the pinned watermarks are non-zero.

    Returns ``(site_id, generation_id, feed_ids, snapshot)``.
    """
    site_id, gen, feeds = _site_with_period(conn)
    for hour in ("06", "12"):
        asof_insert_sample(
            conn,
            site_id=site_id,
            feed_id=feeds[0],
            issued_at="2026-06-01T00:00:00Z",
            valid_at=f"2026-06-01T{hour}:00:00Z",
            lead_hours=int(hour),
            value=12.0,
            fetched_at="2026-06-01T00:05:00Z",
        )
    snapshot = capture_config_snapshot(conn, site_id)
    return site_id, gen, feeds, snapshot


def _start_only(
    conn: sqlite3.Connection, site_id: int, snapshot: dict[str, object]
) -> RunConfig:
    fingerprint = input_fingerprint(conn, site_id, snapshot)
    cfg = start_run(
        conn, site_id, snapshot=snapshot, fingerprint=fingerprint, now=_RUN_NOW
    )
    assert cfg is not None
    return cfg


def _manifest_rows(conn: sqlite3.Connection, run_id: int) -> dict[str, sqlite3.Row]:
    return {
        str(row["component"]): row
        for row in conn.execute(
            "SELECT component, value, scope FROM verification_run_inputs "
            "WHERE run_id = ?",
            (run_id,),
        ).fetchall()
    }


def _max_id(conn: sqlite3.Connection, table: str, site_id: int) -> int:
    row = conn.execute(
        f"SELECT COALESCE(MAX(id), 0) AS hi FROM {table} WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    return int(row["hi"])


# ---------------------------------------------------------------------------
# O1 -- the write pins exactly the declared set.
# ---------------------------------------------------------------------------


def test_o1_write_pins_exactly_the_three_declared_components() -> None:
    """O1 -> at the component-name set equality: correct = exactly the
    three declared names; mutant M1 (dropping ``forecast_arrivals`` from
    the write) leaves two. The forecast watermark is recomputed here
    independently, and both arrivals rows must carry one identical scope
    with the two horizon keys."""
    conn = asof_conn()
    site_id, _gen, _feeds, snapshot = _started_site(conn)
    cfg = _start_only(conn, site_id, snapshot)
    write_run_manifest(conn, cfg)

    rows = _manifest_rows(conn, cfg.run_id)
    assert set(rows) == {
        MANIFEST_COMPONENT_CONFIG_TRUTH,
        MANIFEST_COMPONENT_FORECAST_ARRIVALS,
        MANIFEST_COMPONENT_PAIR_ARRIVALS,
    }
    samples_hi = _max_id(conn, "forecast_samples", site_id)
    assert samples_hi > 0, "fixture must have samples so the watermark is real"
    assert (
        str(rows[MANIFEST_COMPONENT_FORECAST_ARRIVALS]["value"])
        == f"{FORECAST_ARRIVALS_ALGORITHM}:{samples_hi}"
    )
    pairs_hi = _max_id(conn, "forecast_pairs", site_id)
    assert (
        str(rows[MANIFEST_COMPONENT_PAIR_ARRIVALS]["value"])
        == f"{PAIR_ARRIVALS_ALGORITHM}:{pairs_hi}"
    )
    forecast_scope = str(rows[MANIFEST_COMPONENT_FORECAST_ARRIVALS]["scope"])
    assert forecast_scope == str(rows[MANIFEST_COMPONENT_PAIR_ARRIVALS]["scope"])
    assert set(json.loads(forecast_scope)) == {"horizon_start_utc", "horizon_end_utc"}


# ---------------------------------------------------------------------------
# O2 -- ``config_truth_basis`` is byte-identical to the run row's column.
# ---------------------------------------------------------------------------


def test_o2_config_truth_basis_is_the_run_rows_column_not_a_recomputation() -> None:
    """O2 -> at ``value == row['result_basis_fingerprint']``: correct =
    equal; mutant M2 (recomputing the basis inside the write) differs,
    because a ``daily_truth`` row is inserted BETWEEN ``start_run`` and the
    manifest write. The test first proves that insert has separating power
    -- a live recomputation no longer equals the stored column -- so the
    equality is not trivially satisfied by an injection that misses one of
    the SELECT's predicates."""
    conn = asof_conn()
    site_id, gen, _feeds, snapshot = _started_site(conn)
    cfg = _start_only(conn, site_id, snapshot)
    row = conn.execute(
        "SELECT result_basis_fingerprint FROM verification_runs WHERE id = ?",
        (cfg.run_id,),
    ).fetchone()
    stored = str(row["result_basis_fingerprint"])

    # Same generation as the run, a date strictly inside the horizon, and a
    # quantity not already present for that day -- every predicate the
    # basis SELECT filters on.
    assert cfg.tz_generation_id == gen
    inside = _A_PERIOD_DAYS[1]
    assert cfg.period_start < inside < cfg.period_end
    _insert_truth_day(conn, site_id, gen, inside, quantity="wind_max", value=7.0)
    live = result_basis_fingerprint(
        conn,
        site_id,
        snapshot,
        period_start=cfg.period_start,
        period_end=cfg.period_end,
    )
    assert live != stored, "the injected truth row must move a live recomputation"

    write_run_manifest(conn, cfg)
    rows = _manifest_rows(conn, cfg.run_id)
    assert str(rows[MANIFEST_COMPONENT_CONFIG_TRUTH]["value"]) == stored


# ---------------------------------------------------------------------------
# O20 -- manifest rows survive a failed attempt (contract pin).
# ---------------------------------------------------------------------------


def test_o20_manifest_rows_survive_fail_incomplete_attempts() -> None:
    """O20 -> at the manifest row count after the failure: correct = the
    rows are still there while the four evidence tables are emptied; mutant
    M17 (adding the manifest table to ``fail_incomplete_attempts``'s tuple)
    deletes them. Pins the design contract -- the manifest is pinned INPUT
    provenance, not derived evidence -- rather than a user-visible verdict."""
    conn = asof_conn()
    site_id, _gen, _feeds, snapshot = _started_site(conn)
    cfg = _start_only(conn, site_id, snapshot)
    write_run_manifest(conn, cfg)
    _insert_evidence_all_tables(conn, cfg.run_id)
    before = _manifest_rows(conn, cfg.run_id)
    assert len(before) == 3

    fail_incomplete_attempts(conn, site_id, error="synthetic failure")

    state = conn.execute(
        "SELECT state FROM verification_runs WHERE id = ?", (cfg.run_id,)
    ).fetchone()
    assert str(state["state"]) == "failed"
    assert _evidence_row_counts(conn, cfg.run_id) == {
        "verification_evidence": 0,
        "verification_day_context": 0,
        "verification_results": 0,
        "verification_verdicts": 0,
    }
    after = _manifest_rows(conn, cfg.run_id)
    assert {k: tuple(v) for k, v in after.items()} == {
        k: tuple(v) for k, v in before.items()
    }


# ---------------------------------------------------------------------------
# O23 (a) -- ``input_fingerprint`` is untouched by manifest rows (D14).
# ---------------------------------------------------------------------------


def test_o23a_input_fingerprint_is_identical_with_and_without_manifest_rows() -> None:
    """O23 (a) -> at ``with_rows == without_rows``: correct = equal; a
    "while we are here" migration of ``input_fingerprint`` onto the
    manifest would read the new rows and move."""
    conn = asof_conn()
    site_id, _gen, _feeds, snapshot = _started_site(conn)
    cfg = _start_only(conn, site_id, snapshot)
    without_rows = input_fingerprint(conn, site_id, snapshot)
    write_run_manifest(conn, cfg)
    assert len(_manifest_rows(conn, cfg.run_id)) == 3
    with_rows = input_fingerprint(conn, site_id, snapshot)
    assert with_rows == without_rows


# ---------------------------------------------------------------------------
# Read-path fixtures (O3-O15, O21, O23 (b), O24-O26).
# ---------------------------------------------------------------------------

# The chain fixture's horizon: ``_make_verification_site`` scores five UTC
# days, 2026-06-01..05, so the pinned arrivals bounds are these two stamps.
# Asserted by ``_chain_published_run`` rather than assumed.
_HORIZON_START = "2026-06-01T00:00:00Z"
_HORIZON_END = "2026-06-06T00:00:00Z"
# The LAST scored day: a row here is inside the horizon only while the end
# bound is the UTC instant the writer pinned, not a local date.
_INSIDE_HORIZON = "2026-06-05T06:00:00Z"
_LATE_ISSUE = "2026-06-01T00:00:00Z"

_NOT_RECORDED_PROSE = (
    "this run did not record what its results were based on, so there is "
    "nothing to check it against."
)
_FORECAST_ARRIVALS_PROSE = "new forecast rows inside the scored dates"


def _chain_published_run(conn: sqlite3.Connection) -> tuple[int, list[int], int]:
    """Drive the production chain to a published, manifest-bearing run.

    Returns ``(site_id, feed_ids, run_id)``. The manifest is complete and
    its arrivals horizon is ``[_HORIZON_START, _HORIZON_END)``.
    """
    site_id, feeds = _make_verification_site(conn)
    _drive_chain(conn, site_id, dict(_W8_PAYLOAD))
    conn.commit()
    run_id = published_run_id(conn, site_id)
    assert run_id is not None
    rows = _manifest_rows(conn, run_id)
    assert set(rows) == set(MANIFEST_COMPONENT_ORDER)
    scope = json.loads(str(rows[MANIFEST_COMPONENT_FORECAST_ARRIVALS]["scope"]))
    assert scope == {
        "horizon_start_utc": _HORIZON_START,
        "horizon_end_utc": _HORIZON_END,
    }
    return site_id, feeds, run_id


def _insert_sample_at(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    valid_at: str,
    *,
    sample_id: int | None = None,
) -> None:
    """One forecast row valid at ``valid_at``; an explicit id when asked.

    Issued after the chain fixture's own rows so the UNIQUE key never
    collides with them.
    """
    conn.execute(
        """
        INSERT INTO forecast_samples
            (id, site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, ?, 'temperature', ?, ?, 6, 15.0, '{}', 'run-late', ?)
        """,
        (sample_id, site_id, feed_id, _LATE_ISSUE, valid_at, _LATE_ISSUE),
    )


def _both_surfaces(
    monkeypatch: pytest.MonkeyPatch, site_id: int
) -> tuple[dict[str, Any], str]:
    """The API status entry and the rendered page, in that order.

    Each app lifespan closes the process-wide database on exit, so every
    write the test makes must land before this call.
    """
    with TestClient(_make_app(monkeypatch)) as client:
        entry = _status(client, site_id)
    return entry, _fetch_page(monkeypatch, site_id)


def _basis(entry: dict[str, Any]) -> dict[str, Any]:
    basis = entry["result_basis"]
    assert isinstance(basis, dict)
    return basis


def _components(entry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(c["component"]): c for c in _basis(entry)["components"]}


def _assert_fresh_everywhere(entry: dict[str, Any], page: str) -> None:
    assert _basis(entry)["state"] == "fresh"
    assert _basis(entry)["reason"] is None
    assert entry["warnings"]["stale_inputs"] is False
    assert _components(entry)[MANIFEST_COMPONENT_FORECAST_ARRIVALS]["state"] == "fresh"
    assert "16.1.warn_stale" not in _v16_markers(page)


def _assert_changed_everywhere(entry: dict[str, Any], page: str) -> None:
    assert _basis(entry)["state"] == "changed"
    assert entry["warnings"]["stale_inputs"] is True
    assert (
        _components(entry)[MANIFEST_COMPONENT_FORECAST_ARRIVALS]["state"] == "changed"
    )
    assert "16.1.warn_stale" in _v16_markers(page)


def _insert_manifest_row(
    conn: sqlite3.Connection, run_id: int, component: str, value: str, scope: str
) -> None:
    conn.execute(
        "INSERT INTO verification_run_inputs (run_id, component, value, scope) "
        "VALUES (?, ?, ?, ?)",
        (run_id, component, value, scope),
    )


def _scope_json(start: str, end: str) -> str:
    return json.dumps({"horizon_start_utc": start, "horizon_end_utc": end})


# ---------------------------------------------------------------------------
# O3 / O3b -- the gap closes, end to end.
# ---------------------------------------------------------------------------


def test_o3_a_forecast_row_inside_the_horizon_is_reported_on_both_surfaces(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O3 -> at ``state == 'changed'`` on the API and the ``16.1.warn_stale``
    marker on the page, with a ``forecast_arrivals`` component in state
    ``changed``: correct = both report the arrival; a manifest shipped
    without the forecast component reports nothing at all."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, feeds, _run_id = _chain_published_run(conn)
    _insert_sample_at(conn, site_id, feeds[0], _INSIDE_HORIZON)
    conn.commit()

    entry, page = _both_surfaces(monkeypatch, site_id)
    _assert_changed_everywhere(entry, page)


def test_o3b_an_in_horizon_historical_arrival_is_reported_and_that_is_correct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O3b -> at ``changed`` on both surfaces for a row written through
    ``persist_fetch_result`` with ``advance_last_run_at=False`` -- the
    historical-backfill path, the one arrival class production actually
    produces. A later 'quiet the backfill' filter on ``fetched_at``, the
    feed, or the last-run marker would flip this to a false ``fresh``."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, feeds, _run_id = _chain_published_run(conn)
    persist_fetch_result(
        conn,
        site_id=site_id,
        source="example-src",
        fetch_feed_id=feeds[0],
        result=FetchResult(
            samples=[
                NormalizedSample(
                    model="model-alpha",
                    variable="temperature",
                    issued_at="2026-06-02T00:00:00Z",
                    valid_at="2026-06-02T06:00:00Z",
                    lead_hours=6,
                    value=14.5,
                    source_raw="14.5",
                    model_run_id="run-backfill",
                )
            ]
        ),
        fetched_at="2026-06-20T00:00:00Z",
        advance_last_run_at=False,
    )
    conn.commit()

    entry, page = _both_surfaces(monkeypatch, site_id)
    _assert_changed_everywhere(entry, page)


# ---------------------------------------------------------------------------
# O4 / O5 / O6 -- the noise floor: both bounds and the watermark hold.
# ---------------------------------------------------------------------------


def test_o4_a_row_one_second_past_the_horizon_end_is_not_an_arrival(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O4 -> at ``fresh`` on both surfaces: mutant M3 (dropping
    ``valid_at < ?``) matches this row -- above the watermark, above the
    lower bound -- and flips to ``changed``. This is the permanently-true
    warning the parent plan removed; nothing else pins it."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, feeds, _run_id = _chain_published_run(conn)
    _insert_sample_at(conn, site_id, feeds[0], "2026-06-06T00:00:01Z")
    conn.commit()

    entry, page = _both_surfaces(monkeypatch, site_id)
    _assert_fresh_everywhere(entry, page)


def test_o5_a_row_one_second_before_the_horizon_start_is_not_an_arrival(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O5 -> at ``fresh`` on both surfaces: mutant M4 (dropping
    ``valid_at >= ?``) matches this row and flips to ``changed``."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, feeds, _run_id = _chain_published_run(conn)
    _insert_sample_at(conn, site_id, feeds[0], "2026-05-31T23:59:59Z")
    conn.commit()

    entry, page = _both_surfaces(monkeypatch, site_id)
    _assert_fresh_everywhere(entry, page)


def test_o6_a_row_below_the_pinned_watermark_is_not_an_arrival(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O6 -> at ``fresh`` on both surfaces for an in-horizon row whose id
    sits BELOW the watermark: mutant M5 (dropping ``id > ?``) matches every
    pre-existing in-horizon row, so every run reads ``changed`` the moment
    it publishes -- a false positive O3 cannot see because its row also
    satisfies the broken predicate."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, feeds, run_id = _chain_published_run(conn)
    watermark = int(
        str(
            _manifest_rows(conn, run_id)[MANIFEST_COMPONENT_FORECAST_ARRIVALS]["value"]
        ).removeprefix(f"{FORECAST_ARRIVALS_ALGORITHM}:")
    )
    # Free one id below the watermark, then reuse it for the in-horizon row.
    freed = watermark // 2
    assert 0 < freed < watermark
    conn.execute("DELETE FROM forecast_samples WHERE id = ?", (freed,))
    _insert_sample_at(conn, site_id, feeds[0], _INSIDE_HORIZON, sample_id=freed)
    conn.commit()

    entry, page = _both_surfaces(monkeypatch, site_id)
    _assert_fresh_everywhere(entry, page)


# ---------------------------------------------------------------------------
# O7 / O8 -- legacy runs (no manifest rows): D11's fallback.
# ---------------------------------------------------------------------------


def test_o7_a_legacy_run_whose_basis_still_matches_is_unknown_not_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O7 -> at ``unknown``/``not_recorded`` with the page's
    ``16.1.freshness_unknown`` notice carrying the existing prose and NO
    ``16.1.warn_stale``: mutant M7 (mapping a matching basis to ``fresh``)
    claims a forecast side that was never recorded."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id = _make_site(conn, "o7-legacy-site")
    run_id = _seed_published_run(conn, site_id)
    assert _manifest_rows(conn, run_id) == {}

    entry, page = _both_surfaces(monkeypatch, site_id)
    assert _basis(entry)["state"] == "unknown"
    assert _basis(entry)["reason"] == "not_recorded"
    assert entry["warnings"]["stale_inputs"] is False
    assert _components(entry)[MANIFEST_COMPONENT_CONFIG_TRUTH]["state"] == "fresh"
    markers = _v16_markers(page)
    assert "16.1.freshness_unknown" in markers
    assert "16.1.warn_stale" not in markers
    ((_attrs, text),) = _by_v16(page, "p", "16.1.freshness_unknown")
    assert _NOT_RECORDED_PROSE in text


def test_o8_a_legacy_run_whose_basis_moved_is_still_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O8 -> at ``changed`` on both surfaces after a ``daily_truth`` row
    lands inside the legacy run's horizon: mutant M8 (collapsing every
    legacy run to ``unknown``) drops the true positives the 0.13.x warning
    already produced."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id = _make_site(conn, "o8-legacy-site")
    run_id = _seed_published_run(conn, site_id)
    assert _manifest_rows(conn, run_id) == {}
    gen = published_generation_id(conn, site_id)
    assert gen is not None
    _insert_truth_day(conn, site_id, gen, "2026-05-10")
    conn.commit()

    entry, page = _both_surfaces(monkeypatch, site_id)
    assert _basis(entry)["state"] == "changed"
    assert _basis(entry)["reason"] is None
    assert entry["warnings"]["stale_inputs"] is True
    assert _components(entry)[MANIFEST_COMPONENT_CONFIG_TRUTH]["state"] == "changed"
    assert "16.1.warn_stale" in _v16_markers(page)


# ---------------------------------------------------------------------------
# O9 -- precedence (D9), driven through stored rows.
# ---------------------------------------------------------------------------

_O9_HORIZON = _scope_json("2026-06-01T00:00:00Z", "2026-06-04T00:00:00Z")


def _o9_run(conn: sqlite3.Connection) -> tuple[int, int, str, int]:
    """A published run over ``_A_PERIOD_DAYS`` with two in-horizon samples.

    Returns ``(site_id, run_id, live_basis, samples_hi)`` -- the live basis
    is what a ``fresh`` config row stores, ``samples_hi`` what a ``fresh``
    forecast row stores.
    """
    site_id, gen, _feeds, snapshot = _started_site(conn)
    run_id = _insert_run_row(conn, site_id, state="published", tz_generation_id=gen)
    live = result_basis_fingerprint(
        conn,
        site_id,
        snapshot,
        period_start=_A_PERIOD_DAYS[0],
        period_end=_A_PERIOD_DAYS[-1],
    )
    return site_id, run_id, live, _max_id(conn, "forecast_samples", site_id)


@pytest.mark.parametrize(
    ("config_kind", "forecast_kind", "expected"),
    [
        ("fresh", "changed", ("changed", None)),
        ("period_unknown", "fresh", ("unknown", "period_unknown")),
        ("changed", "unknown", ("changed", None)),
        ("period_unknown", "unknown", ("unknown", "period_unknown")),
    ],
)
def test_o9_precedence_changed_outranks_unknown_and_reason_follows_the_order(
    config_kind: str, forecast_kind: str, expected: tuple[str, str | None]
) -> None:
    """O9 -> at ``(state, reason)``: ``changed`` on any verdict component
    wins; otherwise the FIRST unknown required component's note is the
    reason. Mutant M9 (unknown outranks changed) flips row 3;
    mutant M20 (``reason=None`` on unknown) fails rows 2 and 4, whose
    forecast note differs from the config note so the fixed order is what
    is being asserted."""
    conn = asof_conn()
    site_id, run_id, live, samples_hi = _o9_run(conn)
    config_value = "rb1:" + "0" * 64 if config_kind == "changed" else live
    forecast_value = {
        "fresh": f"{FORECAST_ARRIVALS_ALGORITHM}:{samples_hi}",
        "changed": f"{FORECAST_ARRIVALS_ALGORITHM}:0",
        "unknown": "zz9:1",
    }[forecast_kind]
    _insert_manifest_row(
        conn, run_id, MANIFEST_COMPONENT_CONFIG_TRUTH, config_value, "{}"
    )
    _insert_manifest_row(
        conn, run_id, MANIFEST_COMPONENT_FORECAST_ARRIVALS, forecast_value, _O9_HORIZON
    )
    _insert_manifest_row(
        conn,
        run_id,
        MANIFEST_COMPONENT_PAIR_ARRIVALS,
        f"{PAIR_ARRIVALS_ALGORITHM}:0",
        _O9_HORIZON,
    )
    period = (
        (None, None)
        if config_kind == "period_unknown"
        else (
            _A_PERIOD_DAYS[0],
            _A_PERIOD_DAYS[-1],
        )
    )

    result = run_input_freshness(
        conn,
        site_id,
        run_id=run_id,
        recorded_basis=config_value,
        period_start=period[0],
        period_end=period[1],
    )

    by_name = {c.component: c for c in result.components}
    assert by_name[MANIFEST_COMPONENT_CONFIG_TRUTH].state == (
        "unknown" if config_kind == "period_unknown" else config_kind
    )
    assert by_name[MANIFEST_COMPONENT_FORECAST_ARRIVALS].state == forecast_kind
    assert (result.state, result.reason) == expected


# ---------------------------------------------------------------------------
# O10 / O11 -- the observed role is a table lookup (D8).
# ---------------------------------------------------------------------------


def _pair_moved_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[sqlite3.Connection, int]:
    """A chain-published run plus one in-horizon pair above its watermark."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, feeds, run_id = _chain_published_run(conn)
    pairs_hi = int(
        str(
            _manifest_rows(conn, run_id)[MANIFEST_COMPONENT_PAIR_ARRIVALS]["value"]
        ).removeprefix(f"{PAIR_ARRIVALS_ALGORITHM}:")
    )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feeds[0],
        valid_at=_INSIDE_HORIZON,
        issued_at=_LATE_ISSUE,
        forecast=15.0,
        observed=14.0,
        first_known_at=None,
    )
    assert _max_id(conn, "forecast_pairs", site_id) > pairs_hi
    conn.commit()
    return conn, site_id


def test_o10_an_observed_component_cannot_move_the_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O10 -> at overall ``fresh`` with ``pair_arrivals`` reported
    ``changed`` in the breakdown: mutant M10 (every component feeds the
    verdict) ships a warning whose false-positive rate is unmeasured."""
    _conn, site_id = _pair_moved_fixture(tmp_path, monkeypatch)

    entry, page = _both_surfaces(monkeypatch, site_id)
    _assert_fresh_everywhere(entry, page)
    assert _components(entry)[MANIFEST_COMPONENT_PAIR_ARRIVALS]["state"] == "changed"


def test_o11_promoting_pair_arrivals_in_the_role_table_moves_the_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O11 -> at ``changed`` for the identical O10 fixture once
    ``pair_arrivals`` is added to ``MANIFEST_VERDICT_COMPONENTS``: mutant
    M11 (verdict names hardcoded in the evaluator body) ignores the table,
    so the documented one-token promotion does nothing."""
    _conn, site_id = _pair_moved_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        manifest,
        "MANIFEST_VERDICT_COMPONENTS",
        frozenset({*MANIFEST_VERDICT_COMPONENTS, MANIFEST_COMPONENT_PAIR_ARRIVALS}),
    )

    entry, page = _both_surfaces(monkeypatch, site_id)
    assert _basis(entry)["state"] == "changed"
    assert entry["warnings"]["stale_inputs"] is True
    assert "16.1.warn_stale" in _v16_markers(page)


# ---------------------------------------------------------------------------
# O12 / O13 -- unrecognized names are ignored; required names are checked.
# ---------------------------------------------------------------------------


def test_o12_an_unrecognized_component_is_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O12 -> at overall ``fresh`` and a breakdown of known names only:
    mutant M12 (raising on an unknown name) is a 500 on the page; forcing
    ``unknown`` would be a warning nobody can clear."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, _feeds, run_id = _chain_published_run(conn)
    _insert_manifest_row(conn, run_id, "imaginary_component", "zz9:1", "{}")
    conn.commit()

    entry, page = _both_surfaces(monkeypatch, site_id)
    _assert_fresh_everywhere(entry, page)
    assert set(_components(entry)) == set(MANIFEST_COMPONENT_ORDER)


def test_o13_a_missing_required_row_is_unknown_not_recorded_never_fresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O13 -> at ``unknown``/``not_recorded`` after the ``forecast_arrivals``
    row is deleted from a complete manifest: mutant M13 (evaluate whatever
    rows are present) reads a dropped row as a clean bill of health."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, _feeds, run_id = _chain_published_run(conn)
    conn.execute(
        "DELETE FROM verification_run_inputs WHERE run_id = ? AND component = ?",
        (run_id, MANIFEST_COMPONENT_FORECAST_ARRIVALS),
    )
    conn.commit()

    entry, page = _both_surfaces(monkeypatch, site_id)
    assert _basis(entry)["state"] == "unknown"
    assert _basis(entry)["reason"] == "not_recorded"
    assert entry["warnings"]["stale_inputs"] is False
    forecast = _components(entry)[MANIFEST_COMPONENT_FORECAST_ARRIVALS]
    assert (forecast["state"], forecast["note"]) == ("unknown", "not_recorded")
    assert "16.1.freshness_unknown" in _v16_markers(page)


# ---------------------------------------------------------------------------
# O14 -- round trip with nothing changed.
# ---------------------------------------------------------------------------


def test_o14_a_run_published_through_the_production_path_is_fresh_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O14 -> at every component ``fresh`` and overall ``fresh`` for a run
    the production chain started, populated and published, with nothing
    changed since: mutant M14 (comparing ``valid_at`` against the local
    period dates instead of the stored UTC bounds) makes every run
    instantly ``changed``."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, _feeds, _run_id = _chain_published_run(conn)

    entry, page = _both_surfaces(monkeypatch, site_id)
    _assert_fresh_everywhere(entry, page)
    components = _components(entry)
    assert set(components) == set(MANIFEST_COMPONENT_ORDER)
    assert all(c["state"] == "fresh" and c["note"] is None for c in components.values())


# ---------------------------------------------------------------------------
# O15 -- read-path purity under SQLite's own write guard.
# ---------------------------------------------------------------------------


def test_o15_run_input_freshness_never_writes_even_without_a_generation_pointer() -> (
    None
):
    """O15 -> at every call returning under ``PRAGMA query_only=ON`` on a
    site whose published-generation pointer is gone: (a) a manifest-bearing
    run, (b) a legacy run, (c) a manifest-bearing run whose
    ``forecast_arrivals`` scope is not JSON -- the branch mutant M15 turns
    into a live horizon recompute through ``capture_config_snapshot``, a
    seeding WRITE that raises 'attempt to write a readonly database' here.
    (c) is anchored on ``unknown``/``malformed_record`` for that component,
    not merely on not raising."""
    conn = asof_conn()
    site_id = _make_site(conn, "o15-site")
    gen = published_generation_id(conn, site_id)
    assert gen is not None
    snapshot = capture_config_snapshot(conn, site_id)
    basis = result_basis_fingerprint(
        conn, site_id, snapshot, period_start="2026-06-01", period_end="2026-06-03"
    )
    horizon = _scope_json("2026-06-01T00:00:00Z", "2026-06-04T00:00:00Z")

    with_manifest = _insert_run_row(
        conn, site_id, state="published", tz_generation_id=gen
    )
    _insert_manifest_row(
        conn, with_manifest, MANIFEST_COMPONENT_CONFIG_TRUTH, basis, "{}"
    )
    _insert_manifest_row(
        conn, with_manifest, MANIFEST_COMPONENT_FORECAST_ARRIVALS, "fs1:0", horizon
    )
    _insert_manifest_row(
        conn, with_manifest, MANIFEST_COMPONENT_PAIR_ARRIVALS, "fp1:0", horizon
    )
    legacy = _insert_run_row(conn, site_id, state="published", tz_generation_id=gen)
    bad_scope = _insert_run_row(conn, site_id, state="published", tz_generation_id=gen)
    _insert_manifest_row(conn, bad_scope, MANIFEST_COMPONENT_CONFIG_TRUTH, basis, "{}")
    _insert_manifest_row(
        conn, bad_scope, MANIFEST_COMPONENT_FORECAST_ARRIVALS, "fs1:0", "not json"
    )
    conn.execute(
        "DELETE FROM runtime_state WHERE key = ?", (published_pointer_key(site_id),)
    )
    conn.commit()
    assert published_generation_id(conn, site_id) is None
    _read_only(conn)

    def evaluate(run_id: int) -> manifest.RunInputFreshness:
        return run_input_freshness(
            conn,
            site_id,
            run_id=run_id,
            recorded_basis=basis,
            period_start="2026-06-01",
            period_end="2026-06-03",
        )

    a = evaluate(with_manifest)
    assert (a.state, a.reason) == ("unknown", "no_published_generation")
    b = evaluate(legacy)
    assert (b.state, b.reason) == ("unknown", "no_published_generation")
    c = evaluate(bad_scope)
    assert c.state == "unknown"
    forecast = {x.component: x for x in c.components}[
        MANIFEST_COMPONENT_FORECAST_ARRIVALS
    ]
    assert (forecast.state, forecast.note) == ("unknown", "malformed_record")


# ---------------------------------------------------------------------------
# O21 (b) / (c) -- operator prose covers every verdict component.
# ---------------------------------------------------------------------------


def _template_source() -> str:
    import wxverify.web as web_module

    path = Path(str(web_module.__file__)).parent / "templates" / "verification"
    return (path / "show.html").read_text(encoding="utf-8")


def test_o21b_every_verdict_component_has_a_label_in_the_template() -> None:
    """O21 (b) -> at ``MANIFEST_VERDICT_COMPONENTS <= input_labels``, the
    role set imported and the label keys parsed from the template source:
    mutant M18 (a verdict component with no prose) would render the raw
    identifier, or nothing, to the operator."""
    block = re.search(
        r"input_labels\s*=\s*\{(.*?)\}\s*-%\}", _template_source(), re.DOTALL
    )
    assert block is not None, "input_labels dict literal not found"
    label_keys = set(re.findall(r"'([a-z_]+)':", block.group(1)))
    assert label_keys >= MANIFEST_VERDICT_COMPONENTS
    assert MANIFEST_COMPONENT_PAIR_ARRIVALS not in label_keys, (
        "an observed-only component never reaches the notice; a label for "
        "it would be dead prose"
    )


def test_o21c_the_notice_names_the_moved_input_in_prose_not_by_identifier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O21 (c) -> at the rendered ``16.1.warn_stale`` text: it carries the
    forecast component's prose and neither the raw ``forecast_arrivals``
    identifier nor an empty ``()`` -- the template's silent fallbacks."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, feeds, _run_id = _chain_published_run(conn)
    _insert_sample_at(conn, site_id, feeds[0], _INSIDE_HORIZON)
    conn.commit()

    page = _fetch_page(monkeypatch, site_id)
    ((_attrs, text),) = _by_v16(page, "p", "16.1.warn_stale")
    assert _FORECAST_ARRIVALS_PROSE in text
    assert MANIFEST_COMPONENT_FORECAST_ARRIVALS not in text
    assert "()" not in text
    assert text.startswith(
        "Current inputs no longer match the basis pinned for this run ("
    )
    assert text.endswith(") — results reflect that pinned basis, not current data.")


# ---------------------------------------------------------------------------
# O23 (b) -- the no-change gate still fires with a manifest in the database.
# ---------------------------------------------------------------------------


def test_o23b_an_unchanged_night_still_takes_no_change_skip_with_a_manifest() -> None:
    """O23 (b) -> at ``decision == 'no_change_skip'`` on the second trigger:
    the gate still reads ``input_fingerprint`` (D14); a gate migrated onto
    the manifest would start a run every night."""
    conn = asof_conn()
    site_id, _feeds, run_id = _chain_published_run(conn)
    assert len(_manifest_rows(conn, run_id)) == 3
    before = len(_decisions(conn, site_id))

    payload = dict(_W8_PAYLOAD)
    payload["trigger_date"] = "2026-06-07"
    _drive_chain(conn, site_id, payload)

    rows = _decisions(conn, site_id)[before:]
    assert rows[-1]["decision"] == "no_change_skip"
    assert published_run_id(conn, site_id) == run_id


# ---------------------------------------------------------------------------
# O24 -- the component value ladder.
# ---------------------------------------------------------------------------

_O24_HORIZON = _scope_json("2026-06-01T00:00:00Z", "2026-06-04T00:00:00Z")
_O24_REVERSED = _scope_json("2026-06-04T00:00:00Z", "2026-06-01T00:00:00Z")


@pytest.mark.parametrize(
    ("value", "scope", "expected"),
    [
        pytest.param(None, None, ("unknown", "not_recorded"), id="row-absent"),
        pytest.param(
            "zz9:12", _O24_HORIZON, ("unknown", "algorithm_changed"), id="prefix"
        ),
        pytest.param("fs1:", _O24_HORIZON, ("unknown", "malformed_record"), id="empty"),
        pytest.param(
            "fs1:12a", _O24_HORIZON, ("unknown", "malformed_record"), id="junk"
        ),
        pytest.param(
            "fs1:١٢",
            _O24_HORIZON,
            ("unknown", "malformed_record"),
            id="non-ascii-digits",
        ),
        pytest.param(
            "fs1:0", "not json", ("unknown", "malformed_record"), id="not-json"
        ),
        pytest.param(
            "fs1:0", "[1, 2]", ("unknown", "malformed_record"), id="json-array"
        ),
        pytest.param(
            "fs1:0",
            json.dumps({"horizon_start_utc": "2026-06-01T00:00:00Z"}),
            ("unknown", "malformed_record"),
            id="missing-bound",
        ),
        pytest.param("fs1:0", _O24_REVERSED, ("fresh", None), id="reversed-bounds"),
        pytest.param("fs1:0", _O24_HORIZON, ("changed", None), id="control-moved"),
    ],
)
def test_o24_the_forecast_arrivals_value_ladder(
    value: str | None, scope: str | None, expected: tuple[str, str | None]
) -> None:
    """O24 -> at ``(state, note)`` of the ``forecast_arrivals`` component for
    each stored shape. The non-ASCII-digit row kills mutant M19
    (``str.isdigit()`` without ``str.isascii()``: those digits pass and
    ``int()`` parses them). The reversed-bounds row is ``fresh`` because an
    empty horizon matches nothing -- the control row, same watermark and
    same samples with the bounds the right way round, is ``changed``, so
    that ``fresh`` is not vacuous."""
    conn = asof_conn()
    site_id, run_id, live, _samples_hi = _o9_run(conn)
    _insert_manifest_row(conn, run_id, MANIFEST_COMPONENT_CONFIG_TRUTH, live, "{}")
    if value is not None:
        assert scope is not None
        _insert_manifest_row(
            conn, run_id, MANIFEST_COMPONENT_FORECAST_ARRIVALS, value, scope
        )
    _insert_manifest_row(
        conn, run_id, MANIFEST_COMPONENT_PAIR_ARRIVALS, "fp1:0", _O24_HORIZON
    )

    result = run_input_freshness(
        conn,
        site_id,
        run_id=run_id,
        recorded_basis=live,
        period_start=_A_PERIOD_DAYS[0],
        period_end=_A_PERIOD_DAYS[-1],
    )

    forecast = {c.component: c for c in result.components}[
        MANIFEST_COMPONENT_FORECAST_ARRIVALS
    ]
    assert (forecast.state, forecast.note) == expected


# ---------------------------------------------------------------------------
# O25 -- the layering holds, including against deferred imports (D17).
# ---------------------------------------------------------------------------


def _imports_of(module_name: str) -> tuple[set[str], set[tuple[str, str]]]:
    """Every module imported anywhere in the module's source, plus every
    ``from X import name`` pair -- function-level imports included."""
    import importlib

    module = importlib.import_module(module_name)
    tree = ast.parse(Path(str(module.__file__)).read_text(encoding="utf-8"))
    modules: set[str] = set()
    bound: set[tuple[str, str]] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)
            bound.update((node.module, alias.name) for alias in node.names)
    return modules, bound


def test_o25_runs_never_imports_the_manifest_or_the_facade() -> None:
    """O25 (a) -> at the import set of ``runs.py`` having neither module,
    at any nesting depth: a function-local back edge would never surface
    as an import-time error."""
    modules, _bound = _imports_of("wxverify.verification.runs")
    assert "wxverify.verification.manifest" not in modules
    assert "wxverify.verification.freshness" not in modules


@pytest.mark.parametrize(
    "surface", ["wxverify.api.routes.verification", "wxverify.web.verification"]
)
def test_o25_each_surface_binds_the_facade_and_never_the_derivation(
    surface: str,
) -> None:
    """O25 (b) -> at the surface importing ``published_basis_report``
    from the facade and never ``wxverify.verification.manifest``: mutant
    M22 (calling ``run_input_freshness`` directly) is a second derivation
    outside the bracket the read-snapshot rebase will put in the facade."""
    modules, bound = _imports_of(surface)
    assert "wxverify.verification.manifest" not in modules
    assert ("wxverify.verification.freshness", "published_basis_report") in bound


def test_o25_the_derivation_never_imports_the_facade() -> None:
    """O25 (c) -> at ``manifest.py`` importing nothing from the facade."""
    modules, _bound = _imports_of("wxverify.verification.manifest")
    assert "wxverify.verification.freshness" not in modules


# ---------------------------------------------------------------------------
# O26 -- one connection, one snapshot.
# ---------------------------------------------------------------------------


def test_o26_the_facade_reads_everything_on_the_connection_handed_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """O26 -> at a normal return with every pooled reader and
    ``sqlite3.connect`` patched to raise, AND at the trace on the one
    connection carrying the manifest SELECT, a ``daily_truth`` statement
    from the delegated basis ladder, and both rowid-range probes: mutant
    M23 (a probe through ``get_db().read_sync``) raises. The pool's methods
    are patched rather than a ``get_db`` name because every module binds
    its own ``get_db`` reference. The trace half is liveness: without it a
    derivation that never touches the database passes vacuously."""
    conn = asof_conn()
    site_id, _feeds, run_id = _chain_published_run(conn)
    row = conn.execute(
        "SELECT result_basis_fingerprint, period_start, period_end "
        "FROM verification_runs WHERE id = ?",
        (run_id,),
    ).fetchone()

    def _raise(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("a second reader was opened")

    monkeypatch.setattr(Database, "read_sync", _raise)
    monkeypatch.setattr(Database, "read", _raise)
    monkeypatch.setattr(sqlite3, "connect", _raise)
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        result = published_input_freshness(
            conn,
            site_id,
            run_id=run_id,
            recorded_basis=row["result_basis_fingerprint"],
            period_start=row["period_start"],
            period_end=row["period_end"],
        )
    finally:
        conn.set_trace_callback(None)

    assert result.state == "fresh"
    joined = "\n".join(statements)
    assert "FROM verification_run_inputs" in joined
    assert "daily_truth" in joined
    assert "forecast_samples NOT INDEXED" in joined
    assert "forecast_pairs NOT INDEXED" in joined
