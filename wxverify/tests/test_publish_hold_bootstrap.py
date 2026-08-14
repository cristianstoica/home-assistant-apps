"""Publish-hold bootstrap/migration/import oracles (0.11.3, plan §8 O1-O5,
O13, O14, O17).

Covers: the migration-time bootstrap that arms the operator kill switch on
upgrade and leaves it untouched on fresh install (D1-D3); the presence-gated
marker's spine invariant across restarts; atomicity of the bootstrap's own
four writes under its own SAVEPOINT -- NOT atomicity of the whole migration,
which `create_schema`'s `executescript()` call has already committed by that
point; parity of the two extractions this release performs
(``verification_chain_active`` out of ``scheduler.py``,
``fail_incomplete_attempts`` out of ``start_run``); and the import path's
promotion-time neutralization of an in-flight verification chain (D13).

Isolation: in-memory ``sqlite3`` connections for the pure migration oracles
(O1-O4b, O13, O13b); ``tmp_path``-backed files plus a direct ``Database()``
construction for the seam oracles that drive ``run_migrations`` through a
real re-open or ``Database.replace_from`` (O5, O14, O17a-d) -- mirrors
``tests/test_db_transfer.py``'s ``_build_replacement_db`` idiom.

Synthetic data only (public repo): ``site-alpha`` / ``America/Denver``,
RFC-5737-flavoured coordinates, no real station or device identifiers.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from starlette.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import Database, close_db, get_db, init_db
from wxverify.db.migrations import (
    PUBLISH_HOLD_BOOTSTRAP_KEY,
    bootstrap_publish_hold,
    create_schema,
    run_migrations,
)
from wxverify.db.queue import claim_next_job, reclaim_all_stale
from wxverify.db.runtime_state import (
    get_runtime_state,
    set_runtime_state,
)
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.settings.keys import get_setting, set_setting
from wxverify.verification.publish_hold import (
    PUBLISH_HOLD_LAST_SOURCE_KEY,
    PUBLISH_HOLD_LAST_STATE_KEY,
)
from wxverify.verification.runs import (
    IMPORT_SUPPRESSED_REASON,
    fail_incomplete_attempts,
    published_run_key,
)
from wxverify.worker.scheduler import (
    VERIFICATION_PUBLISH_HOLD_KEY,
    verification_publish_held,
)
from wxverify.worker.verification_run import (
    verification_heartbeat_key,
    verification_job_key,
    verification_state_key,
)

_SITE_NAME = "site-alpha"
_SITE_TZ = "America/Denver"

# ---------------------------------------------------------------------------
# Fixture primitives (plan §8, verbatim).
# ---------------------------------------------------------------------------


def _fresh_db() -> sqlite3.Connection:
    """A brand-new database: user_version 0, no schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _emulated_0_11_0_db() -> sqlite3.Connection:
    """A database as a live pre-0.11.3 install presents it: full schema at
    user_version 4, and no bootstrap marker."""
    conn = _fresh_db()
    create_schema(conn)
    conn.execute("PRAGMA user_version = 4")
    return conn


def _emulated_0_11_0_donor(path: Path) -> sqlite3.Connection:
    """File-backed twin of ``_emulated_0_11_0_db`` for seam oracles that need
    a real file (``Database.replace_from`` / ``POST /api/import/db``)."""
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    conn.execute("PRAGMA user_version = 4")
    return conn


def _insert_site(
    conn: sqlite3.Connection,
    name: str = _SITE_NAME,
    *,
    timezone: str = _SITE_TZ,
    enabled: int = 1,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites
            (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
        VALUES (?, 39.7, -104.9, 1600.0, ?, ?)
        """,
        (name, timezone, enabled),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_run(
    conn: sqlite3.Connection, site_id: int, *, state: str, tz_generation_id: int
) -> int:
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, bootstrap_seed,
             bootstrap_resamples, input_fingerprint)
        VALUES (?, ?, 1, '0.11.3', ?, 1, '{}', 1, 1, 'fp-' || ?)
        """,
        (site_id, tz_generation_id, state, state),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_evidence_all_tables(conn: sqlite3.Connection, run_id: int) -> None:
    """One row into each of the four run-scoped evidence tables."""
    conn.execute(
        """
        INSERT INTO verification_evidence
            (run_id, snapshot_local_date, target_local_date, lead, variable,
             quantity, entity_type, entity_key, forecast_eligible,
             truth_eligible)
        VALUES (?, '2026-06-01', '2026-06-02', 1, 'temperature',
                'temperature_high', 'feed', 'feed:1', 1, 1)
        """,
        (run_id,),
    )
    conn.execute(
        """
        INSERT INTO verification_day_context
            (run_id, snapshot_local_date, snapshot_utc, knowability_exclusions,
             null_availability_samples)
        VALUES (?, '2026-06-01', '2026-06-01T02:00:00Z', '[]', 0)
        """,
        (run_id,),
    )
    conn.execute(
        """
        INSERT INTO verification_results
            (run_id, variable, lead, quantity, entity_type, entity_key,
             headline, common_days)
        VALUES (?, 'temperature', 1, 'temperature_high', 'feed', 'feed:1', 1, 5)
        """,
        (run_id,),
    )
    conn.execute(
        """
        INSERT INTO verification_verdicts
            (run_id, variable, outcome, incumbent_depth, tested_family)
        VALUES (?, 'temperature', 'retain_incumbent', 1, 'feed')
        """,
        (run_id,),
    )


def _evidence_row_counts(conn: sqlite3.Connection, run_id: int) -> dict[str, int]:
    return {
        table: int(
            conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE run_id = ?", (run_id,)
            ).fetchone()["n"]
        )
        for table in (
            "verification_evidence",
            "verification_day_context",
            "verification_results",
            "verification_verdicts",
        )
    }


def _run_state(conn: sqlite3.Connection, run_id: int) -> tuple[str, str | None]:
    row = conn.execute(
        "SELECT state, error FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert row is not None
    return str(row["state"]), (None if row["error"] is None else str(row["error"]))


# ---------------------------------------------------------------------------
# O1 - Upgrade arms the hold.
# ---------------------------------------------------------------------------


def test_upgrade_migration_arms_the_publish_hold() -> None:
    conn = _emulated_0_11_0_db()

    run_migrations(conn)

    assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) == "1"
    assert get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) == "armed"
    assert verification_publish_held(conn) is True


# ---------------------------------------------------------------------------
# O2 - Fresh install is not held.
# ---------------------------------------------------------------------------


def test_fresh_install_migration_leaves_the_hold_absent() -> None:
    conn = _fresh_db()

    run_migrations(conn)

    assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) is None
    assert verification_publish_held(conn) is False
    assert (
        get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) == "skipped_fresh_install"
    )


# ---------------------------------------------------------------------------
# O3 - Legacy user_version=0 with an enabled site is held (fail-safe clause).
# ---------------------------------------------------------------------------


def test_legacy_zero_version_with_enabled_site_is_held_by_the_failsafe() -> None:
    conn = _fresh_db()
    create_schema(conn)
    _insert_site(conn, enabled=1)
    # PRAGMA user_version stays 0 -- the first clause of `existing_installation`
    # contributes nothing; only the enabled-site count can arm the hold.

    run_migrations(conn)

    assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) == "1"
    assert get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) == "armed"


def test_legacy_zero_version_with_only_a_disabled_site_is_not_held() -> None:
    """Companion to O3: kills the over-broad `COUNT(*) FROM sites` mutant
    with no `WHERE enabled = 1`."""
    conn = _fresh_db()
    create_schema(conn)
    _insert_site(conn, enabled=0)

    run_migrations(conn)

    assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) is None
    assert (
        get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) == "skipped_fresh_install"
    )


# ---------------------------------------------------------------------------
# O4 - Spine invariant: release survives every restart, marker never resets.
# ---------------------------------------------------------------------------


def test_released_hold_survives_repeated_restarts_and_the_marker_never_resets() -> None:
    conn = _emulated_0_11_0_db()
    run_migrations(conn)  # armed
    set_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY, "0")  # operator release

    for _ in range(2):  # two more restarts
        run_migrations(conn)
        assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) == "0"
        assert verification_publish_held(conn) is False
        assert get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) == "armed"


# ---------------------------------------------------------------------------
# O4b - The marker gate is on presence, not on the value "armed".
# ---------------------------------------------------------------------------


def test_marker_gate_is_presence_based_not_value_based_against_armed() -> None:
    """The escaping sequence O4 cannot kill: this fixture's marker is
    deliberately `skipped_fresh_install`, the value a `!= "armed"` mutant
    would treat as "not yet done"."""
    conn = _fresh_db()
    run_migrations(conn)  # fresh branch
    assert (
        get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) == "skipped_fresh_install"
    )
    assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) is None

    _insert_site(conn, enabled=1)  # operator's first act on the new install

    for _ in range(2):  # the next restart, and a third boot
        run_migrations(conn)
        assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) is None
        assert verification_publish_held(conn) is False
        assert (
            get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY)
            == "skipped_fresh_install"
        )


# ---------------------------------------------------------------------------
# O5 - Bootstrap-write atomicity: the hold setting, its two last-transition
# rows, and the one-time marker commit together or not at all. This is
# atomicity of `bootstrap_publish_hold`'s own SAVEPOINT, NOT of the whole
# migration: `create_schema`'s `executescript()` call has already implicitly
# committed by the time `bootstrap_publish_hold` runs (that commit boundary
# is pre-existing and not something this savepoint reaches back to protect).
# ---------------------------------------------------------------------------


def test_bootstrap_marker_write_failure_rolls_back_all_four_bootstrap_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "atomic.db"
    donor = _emulated_0_11_0_donor(path)
    donor.close()

    real_set_runtime_state = set_runtime_state

    def _raise_on_marker_write(conn: sqlite3.Connection, key: str, value: str) -> None:
        if key == PUBLISH_HOLD_BOOTSTRAP_KEY:
            raise RuntimeError("simulated marker-write failure")
        real_set_runtime_state(conn, key, value)

    monkeypatch.setattr(
        "wxverify.db.migrations.set_runtime_state", _raise_on_marker_write
    )

    with pytest.raises(RuntimeError, match="simulated marker-write failure"):
        Database(str(path))

    # Re-open from disk -- never inspect the in-flight connection Database()
    # left behind, since that would observe the pre-rollback in-memory state
    # rather than what was actually persisted.
    direct = sqlite3.connect(str(path))
    direct.row_factory = sqlite3.Row
    try:
        assert get_setting(direct, VERIFICATION_PUBLISH_HOLD_KEY) is None, (
            "the hold setting must not be committed when the marker write in "
            "the same SAVEPOINT fails"
        )
        assert get_runtime_state(direct, PUBLISH_HOLD_BOOTSTRAP_KEY) is None, (
            "the bootstrap marker itself must not be committed on failure"
        )
        assert get_runtime_state(direct, PUBLISH_HOLD_LAST_STATE_KEY) is None, (
            "the last-state row must not be committed when the marker write "
            "in the same SAVEPOINT fails"
        )
        assert get_runtime_state(direct, PUBLISH_HOLD_LAST_SOURCE_KEY) is None, (
            "the last-source row must not be committed when the marker write "
            "in the same SAVEPOINT fails"
        )
    finally:
        direct.close()


def test_bootstrap_marker_write_failure_rolls_back_without_an_outer_transaction_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Companion to the above, calling `bootstrap_publish_hold` directly on a
    bare connection instead of through `Database`/`_run_immediate`.

    By the time `bootstrap_publish_hold` opens its SAVEPOINT, `create_schema`
    has already implicitly committed any outer `BEGIN IMMEDIATE` (confirmed
    directly: `conn.in_transaction` is `False` right after `executescript`
    returns), so the SAVEPOINT is the only transaction boundary in either
    call path -- there is no live outer transaction for `_run_immediate`'s
    `conn.rollback()` to fall back on in the `Database()` path either. This
    test exercises the SAVEPOINT's rollback-then-release ordering as the
    function's own, caller-independent contract, on a second, separate
    observation point (the same live connection, not a disk reopen).
    """
    conn = _emulated_0_11_0_db()

    real_set_runtime_state = set_runtime_state

    def _raise_on_marker_write(conn: sqlite3.Connection, key: str, value: str) -> None:
        if key == PUBLISH_HOLD_BOOTSTRAP_KEY:
            raise RuntimeError("simulated marker-write failure")
        real_set_runtime_state(conn, key, value)

    monkeypatch.setattr(
        "wxverify.db.migrations.set_runtime_state", _raise_on_marker_write
    )

    with pytest.raises(RuntimeError, match="simulated marker-write failure"):
        bootstrap_publish_hold(conn, pre_migration_user_version=4)

    assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) is None
    assert get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) is None
    assert get_runtime_state(conn, PUBLISH_HOLD_LAST_STATE_KEY) is None
    assert get_runtime_state(conn, PUBLISH_HOLD_LAST_SOURCE_KEY) is None


def test_bootstrap_marker_write_failure_rolls_back_on_a_non_exception_baseexception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`except BaseException:` at the bottom of `bootstrap_publish_hold` must
    also catch failures that are NOT `Exception` subclasses (`SystemExit`,
    `KeyboardInterrupt`) -- narrowing it to `except Exception:` would let a
    `SystemExit` raised mid-SAVEPOINT skip the `ROLLBACK TO`/`RELEASE` pair
    entirely, leaving the hold setting and its two last-transition rows
    committed-to-the-savepoint (visible on this connection) with no marker
    ever written. Every other failure-injection oracle in this module raises
    `RuntimeError`, an `Exception` subclass, so none of them can distinguish
    `except BaseException:` from `except Exception:` -- this is the one
    oracle that samples a non-`Exception` `BaseException`.
    """
    conn = _emulated_0_11_0_db()

    real_set_runtime_state = set_runtime_state

    def _raise_on_marker_write(conn: sqlite3.Connection, key: str, value: str) -> None:
        if key == PUBLISH_HOLD_BOOTSTRAP_KEY:
            raise SystemExit("simulated non-Exception BaseException")
        real_set_runtime_state(conn, key, value)

    monkeypatch.setattr(
        "wxverify.db.migrations.set_runtime_state", _raise_on_marker_write
    )

    with pytest.raises(SystemExit, match="simulated non-Exception BaseException"):
        bootstrap_publish_hold(conn, pre_migration_user_version=4)

    assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) is None
    assert get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) is None
    assert get_runtime_state(conn, PUBLISH_HOLD_LAST_STATE_KEY) is None
    assert get_runtime_state(conn, PUBLISH_HOLD_LAST_SOURCE_KEY) is None


# ---------------------------------------------------------------------------
# O13 - Extraction parity: verification_chain_active vs. the pre-0.11.3
# inline SQL, frozen here as the oracle (never updated to match the code).
# ---------------------------------------------------------------------------

_LEGACY_VERIFICATION_CHAIN_ACTIVE_SQL = """
    SELECT 1 FROM jobs
    WHERE type = 'verification_run' AND job_key = ? AND site_id IS ?
      AND status IN ('pending','running')
    LIMIT 1
"""


def _legacy_chain_active(conn: sqlite3.Connection, site_id: int) -> bool:
    row = conn.execute(
        _LEGACY_VERIFICATION_CHAIN_ACTIVE_SQL, (verification_job_key(site_id), site_id)
    ).fetchone()
    return row is not None


def test_verification_chain_active_extraction_matches_the_legacy_inline_sql() -> None:
    from wxverify.worker.verification_run import verification_chain_active

    conn = _fresh_db()
    create_schema(conn)
    site_a = _insert_site(conn, "site-alpha", enabled=1)
    site_b = _insert_site(conn, "site-beta", enabled=1)
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES "
        "('verification_run', ?, ?, 'pending')",
        (site_a, verification_job_key(site_a)),
    )
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES "
        "('verification_run', ?, ?, 'running')",
        (site_b, verification_job_key(site_b)),
    )
    # A different site with a completed run and a failed run -- neither active.
    site_c = _insert_site(conn, "site-gamma", enabled=1)
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES "
        "('verification_run', ?, ?, 'completed')",
        (site_c, verification_job_key(site_c)),
    )
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES "
        "('verification_run', ?, 'other:key', 'failed')",
        (site_c,),
    )
    # A different job type for a fourth site -- must not read as active.
    site_d = _insert_site(conn, "site-delta", enabled=1)
    conn.execute(
        "INSERT INTO jobs (type, site_id, status) VALUES "
        "('forecast_record', ?, 'pending')",
        (site_d,),
    )

    for site_id in (site_a, site_b, site_c, site_d):
        assert verification_chain_active(conn, site_id) == _legacy_chain_active(
            conn, site_id
        ), f"parity mismatch for site {site_id}"
    assert verification_chain_active(conn, site_a) is True
    assert verification_chain_active(conn, site_b) is True
    assert verification_chain_active(conn, site_c) is False
    assert verification_chain_active(conn, site_d) is False


# ---------------------------------------------------------------------------
# O13b - fail_incomplete_attempts extraction parity.
# ---------------------------------------------------------------------------


def test_fail_incomplete_attempts_extraction_parity() -> None:
    conn = _fresh_db()
    create_schema(conn)
    site_id = _insert_site(conn)
    other_site = _insert_site(conn, "site-other")
    gen = ensure_published_generation(conn, site_id)
    other_gen = ensure_published_generation(conn, other_site)

    published = _insert_run(conn, site_id, state="published", tz_generation_id=gen)
    running = _insert_run(conn, site_id, state="running", tz_generation_id=gen)
    # A pre-existing error on the RUNNING row (e.g. left by a prior stalled
    # heartbeat) makes the COALESCE branch itself observable: the UPDATE's
    # WHERE is narrowed to state='running', so a failed row's error is
    # preserved by that narrowing alone, never by COALESCE. Only a row that
    # is actually eligible for the UPDATE and already carries a non-NULL
    # error can distinguish `COALESCE(error, ?)` from a plain `error = ?`.
    conn.execute(
        "UPDATE verification_runs SET error = 'stale error from a prior attempt' "
        "WHERE id = ?",
        (running,),
    )
    failed_with_error = _insert_run(conn, site_id, state="failed", tz_generation_id=gen)
    conn.execute(
        "UPDATE verification_runs SET error = 'original error' WHERE id = ?",
        (failed_with_error,),
    )
    failed_no_error = _insert_run(conn, site_id, state="failed", tz_generation_id=gen)
    other_running = _insert_run(
        conn, other_site, state="running", tz_generation_id=other_gen
    )
    for run_id in (
        published,
        running,
        failed_with_error,
        failed_no_error,
        other_running,
    ):
        _insert_evidence_all_tables(conn, run_id)

    fail_incomplete_attempts(conn, site_id, error="X")

    # Published run: state, error, and all four evidence tables untouched.
    assert _run_state(conn, published) == ("published", None)
    assert _evidence_row_counts(conn, published) == {
        "verification_evidence": 1,
        "verification_day_context": 1,
        "verification_results": 1,
        "verification_verdicts": 1,
    }
    # The three non-published runs: evidence gone, in all four tables.
    for run_id in (running, failed_with_error, failed_no_error):
        assert _evidence_row_counts(conn, run_id) == {
            "verification_evidence": 0,
            "verification_day_context": 0,
            "verification_results": 0,
            "verification_verdicts": 0,
        }
    # The running run is now failed, but its own pre-existing error survives
    # (COALESCE(error, ?) branch): the new error "X" is used only when there
    # was no error before.
    assert _run_state(conn, running) == ("failed", "stale error from a prior attempt")
    # The already-failed run keeps its own error -- but only because the
    # UPDATE's WHERE is narrowed to state='running' and this row is already
    # 'failed', not because of COALESCE (see the running-row assertion above
    # for the row that actually exercises COALESCE).
    assert _run_state(conn, failed_with_error) == ("failed", "original error")
    # The NULL-error failed run stays NULL: update is narrowed to state='running'.
    assert _run_state(conn, failed_no_error) == ("failed", None)
    # The other site's running run and its evidence are untouched.
    assert _run_state(conn, other_running) == ("running", None)
    assert _evidence_row_counts(conn, other_running) == {
        "verification_evidence": 1,
        "verification_day_context": 1,
        "verification_results": 1,
        "verification_verdicts": 1,
    }


# ---------------------------------------------------------------------------
# O14 - The import path arms a pre-0.11.3 database, and does not re-arm a
# released one. Drives Database.replace_from directly (the seam, not the
# HTTP route).
# ---------------------------------------------------------------------------


def _init_live_fresh_db(tmp_path: Path, name: str = "live.db") -> Database:
    close_db()
    db_path = tmp_path / name
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db


def test_import_of_a_pre_0_11_3_database_arms_the_publish_hold(
    tmp_path: Path,
) -> None:
    donor_path = tmp_path / "donor-o14a.db"
    donor = _emulated_0_11_0_donor(donor_path)
    _insert_site(donor, enabled=1)
    donor.commit()
    donor.close()

    live = _init_live_fresh_db(tmp_path, "live-o14a.db")
    try:
        asyncio.run(live.replace_from(donor_path, tmp_path / "backup-o14a.db"))
        conn = live._conn  # noqa: SLF001
        assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) == "1"
        assert get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) == "armed"
        assert get_runtime_state(conn, PUBLISH_HOLD_LAST_SOURCE_KEY) == "bootstrap"
    finally:
        live.close()
        close_db()


def test_import_of_an_already_released_0_11_3_database_does_not_rearm(
    tmp_path: Path,
) -> None:
    donor_path = tmp_path / "donor-o14b.db"
    donor = _emulated_0_11_0_donor(donor_path)
    _insert_site(donor, enabled=1)
    set_runtime_state(donor, PUBLISH_HOLD_BOOTSTRAP_KEY, "armed")
    donor.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, '0')",
        (VERIFICATION_PUBLISH_HOLD_KEY,),
    )
    donor.commit()
    donor.close()

    live = _init_live_fresh_db(tmp_path, "live-o14b.db")
    try:
        asyncio.run(live.replace_from(donor_path, tmp_path / "backup-o14b.db"))
        conn = live._conn  # noqa: SLF001
        assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) == "0"
        assert get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) == "armed"
        assert verification_publish_held(conn) is False
    finally:
        live.close()
        close_db()


# ---------------------------------------------------------------------------
# O15 - The HTTP release preserves the bootstrap marker.
# ---------------------------------------------------------------------------
# (Kept in test_publish_hold_control.py alongside the rest of the API-route
#  oracles, per the plan's file split by surface -- O15 is HTTP-route-level
#  like O6-O12, not migration-seam-level like O14/O17.)


# ---------------------------------------------------------------------------
# O17 - An imported active chain cannot execute or publish after promotion.
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    await asyncio.Event().wait()


def _make_app(monkeypatch: pytest.MonkeyPatch) -> object:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _csrf_headers(client: TestClient) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {
        "Origin": "http://testserver",
        "X-CSRF-Token": token,
        "Content-Type": "application/octet-stream",
    }


def _build_o17_shared_donor(tmp_path: Path, filename: str) -> Path:
    """The shared O17 donor: an in-flight chain plus two anti-over-reach
    controls (a published run, an other-type job)."""
    path = tmp_path / filename
    conn = _emulated_0_11_0_donor(path)
    site_id = _insert_site(conn, enabled=1)
    gen = ensure_published_generation(conn, site_id)

    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES "
        "('verification_run', ?, ?, 'pending')",
        (site_id, verification_job_key(site_id)),
    )
    running = _insert_run(conn, site_id, state="running", tz_generation_id=gen)
    _insert_evidence_all_tables(conn, running)
    set_runtime_state(conn, verification_state_key(site_id), '{"phase":"scoring"}')
    set_runtime_state(conn, verification_heartbeat_key(site_id), "2026-06-01T02:05:00Z")

    published = _insert_run(conn, site_id, state="published", tz_generation_id=gen)
    _insert_evidence_all_tables(conn, published)
    set_runtime_state(conn, published_run_key(site_id), str(published))

    conn.execute(
        "INSERT INTO jobs (type, site_id, status) "
        "VALUES ('forecast_record', ?, 'pending')",
        (site_id,),
    )
    conn.commit()
    conn.close()
    return path


def test_o17a_imported_active_chain_neutralized_before_promotion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    donor_path = _build_o17_shared_donor(tmp_path, "donor-o17a.db")
    payload = donor_path.read_bytes()

    close_db()
    config.db_path = str(tmp_path / "live-o17a.db")
    config.options_path = str(tmp_path / "missing-options.json")
    init_db(config.db_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.post(
            "/api/import/db", content=payload, headers=_csrf_headers(client)
        )
        assert resp.status_code == 200, resp.text
        conn = get_db()._conn  # noqa: SLF001

        job = conn.execute(
            "SELECT status, last_error FROM jobs WHERE type = 'verification_run'"
        ).fetchone()
        assert job is not None
        assert str(job["status"]) == "failed"
        assert str(job["last_error"]) == IMPORT_SUPPRESSED_REASON

        site_id = int(conn.execute("SELECT id FROM sites LIMIT 1").fetchone()["id"])
        running_id, published_id = [
            int(r["id"])
            for r in conn.execute(
                "SELECT id FROM verification_runs ORDER BY id"
            ).fetchall()
        ]
        assert _run_state(conn, running_id)[0] == "failed"
        assert _evidence_row_counts(conn, running_id) == {
            "verification_evidence": 0,
            "verification_day_context": 0,
            "verification_results": 0,
            "verification_verdicts": 0,
        }

        assert get_runtime_state(conn, verification_state_key(site_id)) is None
        assert get_runtime_state(conn, verification_heartbeat_key(site_id)) is None

        assert _run_state(conn, published_id)[0] == "published"
        assert _evidence_row_counts(conn, published_id) == {
            "verification_evidence": 1,
            "verification_day_context": 1,
            "verification_results": 1,
            "verification_verdicts": 1,
        }
        assert get_runtime_state(conn, published_run_key(site_id)) == str(published_id)

        other_job = conn.execute(
            "SELECT status FROM jobs WHERE type = 'forecast_record'"
        ).fetchone()
        assert other_job is not None
        assert str(other_job["status"]) == "pending"
    close_db()


def test_o17b_neutralized_state_already_present_at_replace_from_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    donor_path = _build_o17_shared_donor(tmp_path, "donor-o17b.db")
    payload = donor_path.read_bytes()

    close_db()
    config.db_path = str(tmp_path / "live-o17b.db")
    config.options_path = str(tmp_path / "missing-options.json")
    init_db(config.db_path)

    captured: dict[str, object] = {}
    real_replace_from = Database.replace_from

    async def _wrapper(self: Database, new_db: Path, backup: Path) -> None:
        # Capture-only: no assertion here -- an exception raised inside this
        # wrapper is caught by import_db's `except BaseException` and turns
        # into a 500, which would fail the test for the wrong reason.
        staged = sqlite3.connect(f"file:{new_db}?mode=ro", uri=True)
        staged.row_factory = sqlite3.Row
        try:
            site_row = staged.execute("SELECT id FROM sites LIMIT 1").fetchone()
            site_id = int(site_row["id"])
            job_row = staged.execute(
                "SELECT status, last_error FROM jobs WHERE type = 'verification_run'"
            ).fetchone()
            captured["job_status"] = str(job_row["status"])
            captured["job_last_error"] = (
                None if job_row["last_error"] is None else str(job_row["last_error"])
            )
            run_row = staged.execute(
                "SELECT state FROM verification_runs WHERE state != 'published'"
            ).fetchone()
            captured["run_state"] = str(run_row["state"])
            state_present = staged.execute(
                "SELECT 1 FROM runtime_state WHERE key = ?",
                (verification_state_key(site_id),),
            ).fetchone()
            captured["state_key_present"] = state_present is not None
        finally:
            staged.close()
        await real_replace_from(self, new_db, backup)

    monkeypatch.setattr(Database, "replace_from", _wrapper)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.post(
            "/api/import/db", content=payload, headers=_csrf_headers(client)
        )
        assert resp.status_code == 200, resp.text

    assert captured, "replace_from was never entered"
    assert captured["job_status"] == "failed"
    assert captured["job_last_error"] == IMPORT_SUPPRESSED_REASON
    assert captured["run_state"] == "failed"
    assert captured["state_key_present"] is False
    close_db()


def test_o17b2_no_claimable_verification_run_survives_the_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-state confirmation, claims no unique kill (see O17a for the
    discriminating assertions)."""
    donor_path = _build_o17_shared_donor(tmp_path, "donor-o17b2.db")
    payload = donor_path.read_bytes()

    close_db()
    config.db_path = str(tmp_path / "live-o17b2.db")
    config.options_path = str(tmp_path / "missing-options.json")
    init_db(config.db_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.post(
            "/api/import/db", content=payload, headers=_csrf_headers(client)
        )
        assert resp.status_code == 200, resp.text
        conn = get_db()._conn  # noqa: SLF001
        reclaim_all_stale(conn)
        claimed = claim_next_job(conn)
        assert claimed is None or claimed.type != "verification_run"
        job = conn.execute(
            "SELECT status FROM jobs WHERE type = 'verification_run'"
        ).fetchone()
        assert job is not None
        assert str(job["status"]) == "failed"
    close_db()


def test_o17c_neutralizer_failure_leaves_the_live_database_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    donor_path = _build_o17_shared_donor(tmp_path, "donor-o17c.db")
    payload = donor_path.read_bytes()

    close_db()
    config.db_path = str(tmp_path / "live-o17c.db")
    config.options_path = str(tmp_path / "missing-options.json")
    init_db(config.db_path)
    live_conn = get_db()._conn  # noqa: SLF001
    own_site = _insert_site(live_conn, "site-own", enabled=1)
    live_conn.commit()

    def _explode(*_a: object, **_k: object) -> None:
        raise RuntimeError("simulated neutralizer failure")

    # _neutralize_imported_verification_chains imports fail_incomplete_attempts
    # LOCALLY (inside its own function body, at call time) rather than at
    # db_transfer module scope, so the module has no such attribute to patch --
    # patch the real owning module instead, which the local `from ... import`
    # resolves against on every call.
    monkeypatch.setattr("wxverify.verification.runs.fail_incomplete_attempts", _explode)
    app = _make_app(monkeypatch)
    # raise_server_exceptions=False: the neutralizer failure must surface as a
    # real ASGI 500 response (what a live server would return), not re-raise
    # into the test process the way TestClient's default strict mode would.
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.post(
            "/api/import/db", content=payload, headers=_csrf_headers(client)
        )
        assert resp.status_code == 500

        conn = get_db()._conn  # noqa: SLF001
        own_row = conn.execute(
            "SELECT 1 FROM sites WHERE id = ?", (own_site,)
        ).fetchone()
        assert own_row is not None, "live database's own site must survive"
        rebuild_state = conn.execute(
            "SELECT value FROM runtime_state WHERE key = 'import_rebuild_state'"
        ).fetchone()
        assert rebuild_state is None or str(rebuild_state["value"]) != "pending", (
            "no swap occurred -- the staged pending marker must not appear live"
        )

        staged_leftovers = list(Path(config.db_path).parent.glob("*.wxverify-import-*"))
        assert staged_leftovers == [], "the staged temp file must be cleaned up"

        # The guard was released: a second import is admitted, not 409.
        second_resp = client.post(
            "/api/import/db", content=payload, headers=_csrf_headers(client)
        )
        assert second_resp.status_code != 409
    close_db()


def _build_o17d_legacy_donor(
    tmp_path: Path, filename: str, *, drop_runtime_state: bool
) -> Path:
    path = tmp_path / filename
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    for table in (
        "verification_runs",
        "verification_evidence",
        "verification_day_context",
        "verification_results",
        "verification_verdicts",
        "verification_trigger_decisions",
    ):
        conn.execute(f"DROP TABLE {table}")
    if drop_runtime_state:
        conn.execute("DROP TABLE runtime_state")
    conn.execute("PRAGMA user_version = 3")
    site_id = _insert_site(conn, enabled=1)
    conn.execute(
        """
        INSERT INTO stations
            (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
        VALUES (?, 'SYN-STATION-A1', 39.7, -104.9, 1600.0, 1)
        """,
        (site_id,),
    )
    station_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.execute(
        """
        INSERT INTO station_observations
            (station_id, variable, valid_at, value, qc_flag, source_raw)
        VALUES (?, 'temperature', '2026-06-01T00:00:00Z', 10.0, 'ok', 'synthetic')
        """,
        (station_id,),
    )
    conn.execute(
        "INSERT INTO jobs (type, site_id, status) "
        "VALUES ('forecast_record', ?, 'pending')",
        (site_id,),
    )
    conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES "
        "('verification_run', ?, ?, 'pending')",
        (site_id, verification_job_key(site_id)),
    )
    conn.commit()
    conn.close()
    return path


def test_o17d_part1_legacy_donor_with_no_verification_tables_is_skipped_not_raised(
    tmp_path: Path,
) -> None:
    from wxverify.api.routes.db_transfer import _neutralize_imported_verification_chains

    donor_path = _build_o17d_legacy_donor(
        tmp_path, "donor-o17d-part1.db", drop_runtime_state=False
    )

    _neutralize_imported_verification_chains(donor_path)  # must not raise

    conn = sqlite3.connect(str(donor_path))
    conn.row_factory = sqlite3.Row
    try:
        names = {
            str(r[0])
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert not any(name.startswith("verification_") for name in names), (
            "the neutralizer skips a table-absent donor; it must not create tables"
        )
        job = conn.execute(
            "SELECT status, last_error FROM jobs WHERE type = 'verification_run'"
        ).fetchone()
        assert str(job["status"]) == "failed"
        assert str(job["last_error"]) == IMPORT_SUPPRESSED_REASON
        record_job = conn.execute(
            "SELECT status FROM jobs WHERE type = 'forecast_record'"
        ).fetchone()
        assert str(record_job["status"]) == "pending"
    finally:
        conn.close()


def test_o17d_part2_legacy_donor_imports_end_to_end_and_is_armed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    donor_path = _build_o17d_legacy_donor(
        tmp_path, "donor-o17d-part2.db", drop_runtime_state=False
    )
    payload = donor_path.read_bytes()

    close_db()
    config.db_path = str(tmp_path / "live-o17d.db")
    config.options_path = str(tmp_path / "missing-options.json")
    init_db(config.db_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.post(
            "/api/import/db", content=payload, headers=_csrf_headers(client)
        )
        assert resp.status_code == 200, resp.text

        conn = get_db()._conn  # noqa: SLF001
        assert get_setting(conn, VERIFICATION_PUBLISH_HOLD_KEY) == "1"
        assert get_runtime_state(conn, PUBLISH_HOLD_BOOTSTRAP_KEY) == "armed"
        names = {
            str(r[0])
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for table in (
            "verification_runs",
            "verification_evidence",
            "verification_day_context",
            "verification_results",
            "verification_verdicts",
            "verification_trigger_decisions",
        ):
            assert table in names
            count = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
            assert int(count["n"]) == 0
        record_job = conn.execute(
            "SELECT status FROM jobs WHERE type = 'forecast_record'"
        ).fetchone()
        assert str(record_job["status"]) == "pending"
    close_db()


def test_o17d_companion_runtime_state_table_is_created_not_skipped(
    tmp_path: Path,
) -> None:
    from wxverify.api.routes.db_transfer import _neutralize_imported_verification_chains

    donor_path = _build_o17d_legacy_donor(
        tmp_path, "donor-o17d-companion.db", drop_runtime_state=True
    )

    _neutralize_imported_verification_chains(donor_path)  # must not raise

    conn = sqlite3.connect(str(donor_path))
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_state'"
        ).fetchone()
        assert row is not None, (
            "ensure_runtime_state_table must CREATE the table when it is absent "
            "(this is the one guard that is create-if-absent, not skip)"
        )
    finally:
        conn.close()
