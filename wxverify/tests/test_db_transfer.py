"""Tests for DB export + import (db_transfer routes + Database.replace_from).

Harness idioms match test_web_ui.py / test_static_ingress.py: per-test tmp DB
via ``_init_tmp_db``, idle-worker app, ``TestClient(app, client=(ip, port))``
for Supervisor-vs-standalone discrimination. ``TestClient`` runs
``BackgroundTask`` inline before ``client.post()``/``client.get()`` return, so
export/import cleanup and the post-import derived rebuild are directly
observable without polling.

All fixture data is synthetic (fake site/station names and IDs, RFC-5737
``192.0.2.x`` for the non-Supervisor client) -- this is a PUBLIC repo.
"""

from __future__ import annotations

import asyncio
import os
import re
import secrets
import sqlite3
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from starlette.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db import connection as db_connection
from wxverify.db.connection import Database, close_db, get_db, init_db
from wxverify.db.migrations import TARGET_USER_VERSION

_SUPERVISOR_IP = "172.30.32.2"
_NON_SUPERVISOR_IP = "192.0.2.10"  # RFC-5737 documentation range
_INGRESS_TOKEN = "synthetic-db-transfer-token"  # noqa: S105
_INGRESS_PREFIX = f"/api/hassio_ingress/{_INGRESS_TOKEN}"


# ---------------------------------------------------------------------------
# Harness (verbatim idiom from test_web_ui.py / test_static_ingress.py).
# ---------------------------------------------------------------------------


async def _idle_worker(_db: object) -> None:
    """Drop-in run_worker shim that idles without touching the scheduler."""
    await asyncio.Event().wait()


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _make_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return create_app(root_path="")


def _make_site(conn: sqlite3.Connection, name: str, *, enabled: int = 1) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites
                (name, forecast_lat, forecast_lon, elevation_m, timezone, enabled)
            VALUES (?, 47.0, 25.0, 900.0, 'UTC', ?)
            """,
            (name, enabled),
        ).lastrowid
    )


def _feed_id(conn: sqlite3.Connection, model: str, source: str = "open-meteo") -> int:
    return int(
        conn.execute(
            "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
        ).fetchone()["id"]
    )


def _csrf_headers(
    client: TestClient, *, origin: str = "http://testserver"
) -> dict[str, str]:
    token = client.get("/api/csrf").json()["csrf_token"]
    return {
        "Origin": origin,
        "X-CSRF-Token": token,
        "Content-Type": "application/octet-stream",
    }


def _build_replacement_db(tmp_path: Path, filename: str, site_name: str) -> Path:
    """Build a standalone, fully-migrated DB file seeded with one site.

    Uses a direct ``Database()`` construction (NOT ``init_db()``) so it never
    clobbers the module-global ``_db_instance`` the live app occupies.
    """
    path = tmp_path / filename
    db = Database(str(path))
    try:
        _make_site(db._conn, site_name)  # noqa: SLF001
        db._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        db._conn.commit()  # noqa: SLF001
    finally:
        db.close()
    return path


# ---------------------------------------------------------------------------
# Part A -- Export (T1-T6).
# ---------------------------------------------------------------------------


def test_export_snapshot_integrity_matches_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T1: exported snapshot passes integrity_check and matches source rows."""
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "Export Site One")
    _make_site(conn, "Export Site Two")
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/api/export/db")
        assert resp.status_code == 200
        out = tmp_path / "exported.db"
        out.write_bytes(resp.content)
        # A fresh handle -- the pre-boot `conn` is closed by the lifespan's
        # own init_db() re-init and can no longer be queried.
        source = sqlite3.connect(config.db_path)
        try:
            source_names = {r[0] for r in source.execute("SELECT name FROM sites")}
        finally:
            source.close()
    exported = sqlite3.connect(str(out))
    try:
        assert exported.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        exported_names = {r[0] for r in exported.execute("SELECT name FROM sites")}
    finally:
        exported.close()
    assert source_names == {"Export Site One", "Export Site Two"}
    assert exported_names == source_names


def test_export_includes_uncheckpointed_wal_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T2: WAL-consistency discriminator -- uncheckpointed writes must export."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        site_id = _make_site(get_db()._conn, "WAL Site")  # noqa: SLF001
        wal_path = Path(f"{config.db_path}-wal")
        assert wal_path.exists() and wal_path.stat().st_size > 0, (
            "expected a non-empty -wal sidecar before export "
            "(uncheckpointed write) -- fixture setup failed"
        )
        resp = client.get("/api/export/db")
    assert resp.status_code == 200
    out = tmp_path / "exported.db"
    out.write_bytes(resp.content)
    exported = sqlite3.connect(str(out))
    try:
        row = exported.execute(
            "SELECT name FROM sites WHERE id=?", (site_id,)
        ).fetchone()
    finally:
        exported.close()
    assert row is not None and row[0] == "WAL Site", (
        "a naive stream of the main file (bypassing the WAL) would miss this row"
    )


def test_export_content_disposition_header_format(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T3: Content-Disposition matches the timestamped attachment filename."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/api/export/db")
    assert resp.status_code == 200
    disposition = resp.headers.get("content-disposition", "")
    assert re.fullmatch(
        r'attachment; filename="wxverify-\d{8}-\d{6}Z\.db"', disposition
    ), f"unexpected Content-Disposition: {disposition!r}"


def test_export_cleans_up_temp_on_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T4: no export temp remains after a successful download."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/api/export/db")
    assert resp.status_code == 200
    db_dir = Path(config.db_path).parent
    leftovers = list(db_dir.glob(".wxverify-export-*.db.tmp"))
    assert leftovers == [], f"export temp(s) not cleaned up: {leftovers}"


def test_export_error_path_cleans_up_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T5 (rewritten per F1): temp cleanup on export failure, correctly oracled.

    Pins the temp name via a fixed uuid4, lets the REAL snapshot run (so the
    temp genuinely exists on disk) before injecting the failure, and uses
    ``raise_server_exceptions=False`` so the 500 is observable rather than
    propagating out of ``client.get()``.
    """
    _init_tmp_db(tmp_path)
    fixed_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.uuid.uuid4", lambda: fixed_uuid
    )
    db_dir = Path(config.db_path).parent
    tmp = db_dir / f".wxverify-export-{fixed_uuid.hex}.db.tmp"

    real_read = Database.read

    async def _read_then_fail(self: Database, fn: Any) -> None:
        await real_read(self, fn)  # runs the real VACUUM INTO -- tmp now exists
        assert tmp.exists(), "temp must exist on disk before the synthetic failure"
        raise sqlite3.OperationalError("synthetic export failure")

    monkeypatch.setattr(Database, "read", _read_then_fail)
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.get("/api/export/db")
    assert resp.status_code == 500, (
        f"expected Starlette's default plain-text 500; got {resp.status_code}"
    )
    assert not tmp.exists(), "temp must be removed on export failure"


def test_export_sweeps_stale_temps_but_keeps_fresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T6a: a 2h-backdated temp is swept; a fresh one survives."""
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    stale = db_dir / ".wxverify-export-stale0000000000000000000000000000.db.tmp"
    fresh = db_dir / ".wxverify-export-fresh0000000000000000000000000000.db.tmp"
    stale.write_bytes(b"stale")
    fresh.write_bytes(b"fresh")
    old_time = time.time() - 7200
    os.utime(stale, (old_time, old_time))
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/api/export/db")
    assert resp.status_code == 200
    assert not stale.exists(), "backdated temp must be swept"
    assert fresh.exists(), "fresh temp must survive the sweep (may be in-flight)"


def test_export_sweep_tolerates_concurrent_removal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T6b: a temp that disappears mid-sweep must not 500 the export."""
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    racer = db_dir / ".wxverify-export-racer00000000000000000000000000.db.tmp"
    racer.write_bytes(b"racer")
    old_time = time.time() - 7200
    os.utime(racer, (old_time, old_time))

    real_stat = Path.stat

    def _stat_raises_for_racer(self: Path, *args: object, **kwargs: object) -> object:
        if self == racer:
            raise FileNotFoundError(racer)
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", _stat_raises_for_racer)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.get("/api/export/db")
    assert resp.status_code == 200, (
        "a concurrently-removed temp must not 500 the export "
        "(try/except (FileNotFoundError, OSError): continue in _sweep_stale)"
    )


# ---------------------------------------------------------------------------
# Part B -- Import (I1-I9 + the post-plan backup.exists() guard).
# ---------------------------------------------------------------------------


def test_import_round_trip_rebuilds_derived_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I1: valid import round-trip + full derived-table rebuild."""
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "Pre Import Site A")
    conn.commit()

    b_path = tmp_path / "source-b.db"
    b_db = Database(str(b_path))
    try:
        conn_b = b_db._conn  # noqa: SLF001
        site_b = _make_site(conn_b, "Post Import Site B")
        station_b = int(
            conn_b.execute(
                """
                INSERT INTO stations
                    (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)
                VALUES (?, 'SYN-STATION-B1', 47.0, 25.0, 900.0, 1)
                """,
                (site_b,),
            ).lastrowid
        )
        conn_b.execute(
            """
            INSERT INTO station_observations
                (station_id, variable, valid_at, value, qc_flag, source_raw)
            VALUES (?, 'temperature', '2035-06-01T00:00:00Z', 10.0, 'ok',
                    'synthetic-test')
            """,
            (station_b,),
        )
        # Stale/wrong observations row, inserted directly (bypassing
        # materialize_consensus) -- proves the rebuild recomputes it from the
        # imported station data rather than trusting the shipped value.
        conn_b.execute(
            """
            INSERT INTO observations
                (site_id, variable, valid_at, value, n_stations,
                 rejected_stations, computed_at)
            VALUES (?, 'temperature', '2035-06-01T00:00:00Z', 999.0, 1, 0,
                    '2020-01-01T00:00:00Z')
            """,
            (site_b,),
        )
        feed_id = _feed_id(conn_b, "ecmwf_ifs")
        # Poisoned score_cache marker: a window_key no real rebuild produces
        # (real windows are only "w:<rolling_days>" and "w:all").
        conn_b.execute(
            """
            INSERT INTO score_cache
                (site_id, feed_id, variable, day_ahead, window_key, n,
                 skill_score, computed_at)
            VALUES (?, ?, 'temperature', 1, 'w:poison-marker', 999, 42.0,
                    '2020-01-01T00:00:00Z')
            """,
            (site_b, feed_id),
        )
        # Orphaned forecast_pairs row on a CONCRETE, non-virtual feed: no
        # station_observations/observations row backs this (site, variable,
        # valid_at) cell. Must be non-virtual: materialize_multimodel_mean
        # unconditionally clears ALL virtual-feed forecast_pairs, which would
        # mask a regressed rebuild-step-1 DELETE if the orphan were virtual.
        conn_b.execute(
            """
            INSERT INTO forecast_pairs
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 day_ahead, forecast, observed, error, abs_error, sq_error,
                 cat_hit, cat_false, cat_miss, cat_correct_neg)
            VALUES (?, ?, 'wind', '2035-06-02T00:00:00Z', '2035-06-02T00:00:00Z',
                    24, 1, 5.0, 4.0, 1.0, 1.0, 1.0, 1, 0, 0, 0)
            """,
            (site_b, feed_id),
        )
        conn_b.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn_b.commit()
    finally:
        b_db.close()
    payload = b_path.read_bytes()

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "imported"
        sites = client.get("/api/sites").json()
    names = {s["name"] for s in sites}
    assert names == {"Post Import Site B"}, "app queries must now serve B"

    direct = sqlite3.connect(config.db_path)
    try:
        direct_names = {r[0] for r in direct.execute("SELECT name FROM sites")}
        assert direct_names == {"Post Import Site B"}, (
            "a fresh direct connection must also see only B"
        )
        marker = direct.execute(
            "SELECT 1 FROM score_cache WHERE window_key='w:poison-marker'"
        ).fetchone()
        assert marker is None, (
            "poisoned score_cache marker must be cleared by the post-import "
            "rebuild (mutation check: commenting out the BackgroundTask "
            "must turn this red)"
        )
        orphan = direct.execute(
            """
            SELECT 1 FROM forecast_pairs
            WHERE variable='wind' AND valid_at='2035-06-02T00:00:00Z'
            """
        ).fetchone()
        assert orphan is None, (
            "orphaned forecast_pairs row must be cleared by the rebuild's "
            "from-scratch DELETE (F-I2; mutation check: dropping that DELETE "
            "must turn this specific assertion red)"
        )
        recomputed = direct.execute(
            """
            SELECT value FROM observations
            WHERE site_id=? AND variable='temperature'
              AND valid_at='2035-06-01T00:00:00Z'
            """,
            (site_b,),
        ).fetchone()
        assert recomputed is not None and recomputed[0] == 10.0, (
            "observations must be recomputed from imported station data "
            "(elevation-matched -> exactly 10.0), not the shipped stale "
            "value (999.0)"
        )
        stamped = direct.execute(
            "SELECT value FROM runtime_state WHERE key='import_rebuild_done_at'"
        ).fetchone()
        assert stamped is not None, "import_rebuild_done_at must be stamped"
    finally:
        direct.close()


def _sqlite_bytes_with_user_version(path: Path, version: int) -> bytes:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE dummy (id INTEGER PRIMARY KEY)")
        conn.execute(f"PRAGMA user_version = {version}")
        conn.commit()
    finally:
        conn.close()
    return path.read_bytes()


def _sqlite_bytes_missing_required_table(path: Path) -> bytes:
    db = Database(str(path))
    db.close()
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("DROP TABLE stations")
        conn.commit()
    finally:
        conn.close()
    return path.read_bytes()


def _build_invalid_upload(tmp_path: Path, case: str) -> bytes:
    target = tmp_path / "invalid.db"
    if case == "random_bytes":
        return secrets.token_bytes(64)
    if case == "version_zero":
        return _sqlite_bytes_with_user_version(target, 0)
    if case == "version_too_new":
        return _sqlite_bytes_with_user_version(target, TARGET_USER_VERSION + 1)
    if case == "missing_table":
        return _sqlite_bytes_missing_required_table(target)
    raise ValueError(case)


@pytest.mark.parametrize(
    "case", ["random_bytes", "version_zero", "version_too_new", "missing_table"]
)
def test_import_rejects_invalid_upload_live_db_untouched(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I2: rejection matrix -- 422, live DB untouched, no temp, no backup."""
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "Guarded Site")
    conn.commit()
    payload = _build_invalid_upload(tmp_path, case)
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 422, f"{case}: expected 422, got {resp.status_code}"
        body = resp.json()
        assert "error" in body
        sites = client.get("/api/sites").json()
    names = {s["name"] for s in sites}
    assert names == {"Guarded Site"}, f"{case}: live DB must be untouched"
    db_dir = Path(config.db_path).parent
    assert list(db_dir.glob(".wxverify-import-*.db.tmp")) == [], (
        f"{case}: import temp must not remain"
    )
    assert list(db_dir.glob("*.db.bak")) == [], (
        f"{case}: no backup must be created before validation passes"
    )


def test_import_creates_correct_backup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I3: exactly one correct backup, containing the PRE-import rows."""
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "Pre Import Site A")
    conn.commit()
    b_path = _build_replacement_db(tmp_path, "source-b.db", "Post Import Site B")
    payload = b_path.read_bytes()
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
    assert resp.status_code == 200
    db_dir = Path(config.db_path).parent
    backups = list(db_dir.glob("wxverify-*.db.bak"))
    assert len(backups) == 1, f"expected exactly one backup; got {backups}"
    backup = backups[0]
    assert re.fullmatch(r"wxverify-\d{8}-\d{6}Z\.db\.bak", backup.name)
    bconn = sqlite3.connect(str(backup))
    try:
        assert bconn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        names = {r[0] for r in bconn.execute("SELECT name FROM sites")}
    finally:
        bconn.close()
    assert names == {"Pre Import Site A"}, "backup must contain the PRE-import rows"


def test_import_wal_sidecar_correctness_http_level(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I4 (HTTP-level): an uncheckpointed live WAL must not resurrect A."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        _make_site(get_db()._conn, "Uncheckpointed Site A")  # noqa: SLF001
        wal_path = Path(f"{config.db_path}-wal")
        assert wal_path.exists() and wal_path.stat().st_size > 0, (
            "expected uncommitted WAL activity before import (T2 idiom)"
        )
        b_path = _build_replacement_db(tmp_path, "source-b.db", "Fresh Site B")
        payload = b_path.read_bytes()
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 200
        sites = client.get("/api/sites").json()
    names = {s["name"] for s in sites}
    assert names == {"Fresh Site B"}, (
        "a surviving stale WAL would resurrect A or corrupt the import"
    )
    direct = sqlite3.connect(config.db_path)
    try:
        assert direct.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        direct_names = {r[0] for r in direct.execute("SELECT name FROM sites")}
    finally:
        direct.close()
    assert direct_names == {"Fresh Site B"}


def test_replace_from_cleans_sidecars_and_preserves_lock_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I4 (unit-level): planted sidecars don't survive; locks are never recreated.

    The sidecars are injected via a wrapped ``os.replace`` that plants them
    immediately AFTER the real rename -- the exact window ``_unlink_sidecars``
    (step 5) exists to cover ("a leftover from a previously crashed
    process"). Writing garbage bytes directly into the LIVE connection's own
    active -wal file (while that connection still holds real, uncheckpointed
    data) would corrupt genuine in-flight content instead of simulating an
    inert leftover -- this construction avoids that trap.
    """
    live_path = tmp_path / "unit-live.db"
    db = Database(str(live_path))
    try:
        _make_site(db._conn, "Unit Old Content")  # noqa: SLF001
        db._conn.commit()  # noqa: SLF001

        new_path = tmp_path / "unit-new.db"
        new_db = Database(str(new_path))
        try:
            _make_site(new_db._conn, "Unit New Content")  # noqa: SLF001
            new_db._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
            new_db._conn.commit()  # noqa: SLF001
        finally:
            new_db.close()

        wal_leftover = Path(f"{live_path}-wal")
        shm_leftover = Path(f"{live_path}-shm")
        real_replace = db_connection.os.replace

        def _replace_then_plant_sidecars(src: object, dst: object) -> None:
            real_replace(src, dst)
            wal_leftover.write_bytes(b"garbage-wal")
            shm_leftover.write_bytes(b"garbage-shm")

        monkeypatch.setattr(
            "wxverify.db.connection.os.replace", _replace_then_plant_sidecars
        )

        before_w = db._write_lock  # noqa: SLF001
        before_r = db._read_lock  # noqa: SLF001
        backup_path = tmp_path / "unit-backup.db.bak"
        asyncio.run(db.replace_from(new_path, backup_path))

        assert db._write_lock is before_w, (  # noqa: SLF001
            "write lock must never be recreated across a swap"
        )
        assert db._read_lock is before_r, (  # noqa: SLF001
            "read lock must never be recreated across a swap"
        )
        # The reopened connection is itself an open WAL-mode session, so a
        # FRESH, legitimate -wal file is expected to exist again by now (that
        # is the normal steady state, not a corruption artifact) -- assert on
        # CONTENT instead: our injected garbage bytes must not survive.
        assert wal_leftover.read_bytes() != b"garbage-wal", (
            "the garbage sidecar's bytes must not survive into the reopened session"
        )
        assert shm_leftover.read_bytes() != b"garbage-shm", (
            "the garbage sidecar's bytes must not survive into the reopened session"
        )
        row = db._conn.execute("SELECT name FROM sites").fetchone()  # noqa: SLF001
        assert row is not None and row[0] == "Unit New Content", (
            "reopened connection must serve the swapped-in content, proving the "
            "garbage sidecars did not corrupt the new session"
        )
    finally:
        db.close()


def test_import_swap_failure_os_replace_live_db_intact_and_served(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I5a: os.replace failure -> 500; live DB intact AND served through app."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        _make_site(get_db()._conn, "Surviving Site A")  # noqa: SLF001
        b_path = _build_replacement_db(tmp_path, "source-b.db", "Never Served B")
        payload = b_path.read_bytes()

        def _raise_os_replace(*_args: object, **_kwargs: object) -> None:
            raise OSError("synthetic os.replace failure")

        monkeypatch.setattr("wxverify.db.connection.os.replace", _raise_os_replace)
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 500, f"expected 500; got {resp.status_code}"
        sites = client.get("/api/sites").json()
    names = {s["name"] for s in sites}
    assert names == {"Surviving Site A"}, (
        "live DB must still be served through the app after a failed swap"
    )
    db_dir = Path(config.db_path).parent
    assert list(db_dir.glob(".wxverify-import-*.db.tmp")) == [], (
        "upload temp must be cleaned up even on swap failure"
    )


def test_import_swap_failure_migrations_restore_rollback_no_leak(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I5b: post-swap migration failure restores the backup; no leaked conn."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        _make_site(get_db()._conn, "Restored Site A")  # noqa: SLF001
        b_path = _build_replacement_db(tmp_path, "source-b.db", "Never Served B")
        payload = b_path.read_bytes()

        real_run_migrations = db_connection.run_migrations
        armed = {"on": False}

        def _flaky_run_migrations(conn: sqlite3.Connection) -> None:
            if armed["on"]:
                armed["on"] = False
                raise RuntimeError("synthetic post-swap migration failure")
            real_run_migrations(conn)

        monkeypatch.setattr(
            "wxverify.db.connection.run_migrations", _flaky_run_migrations
        )
        stale_conn = get_db()._conn  # noqa: SLF001
        headers = _csrf_headers(client)
        armed["on"] = True
        resp = client.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 500, f"expected 500; got {resp.status_code}"
        sites = client.get("/api/sites").json()
        # No-leaked-connection: the half-open connection from the failed
        # swap must have been closed, not left dangling.
        with pytest.raises(sqlite3.ProgrammingError):
            stale_conn.execute("SELECT 1")
        fresh_conn = get_db()._conn  # noqa: SLF001
        assert fresh_conn is not stale_conn, "the DB must reopen a NEW connection"
        assert (
            fresh_conn.execute("PRAGMA user_version").fetchone()[0]
            == TARGET_USER_VERSION
        ), "restored connection must be healthy"
    names = {s["name"] for s in sites}
    assert names == {"Restored Site A"}, "app must serve rows A again after rollback"
    db_dir = Path(config.db_path).parent
    assert list(db_dir.glob("*.db.bak")), "backup must still be on disk"


def test_import_mutation_guard_rejects_and_leaves_db_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I6: 4-way mutation-guard enforcement, live DB untouched throughout."""
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "Guard Site")
    conn.commit()
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        csrf_token = client.get("/api/csrf").json()["csrf_token"]

        # (a) missing X-CSRF-Token -> 403.
        resp_a = client.post(
            "/api/import/db",
            content=b"x",
            headers={
                "Origin": "http://testserver",
                "Content-Type": "application/octet-stream",
            },
        )
        assert resp_a.status_code == 403
        assert resp_a.json() == {"error": "bad csrf token"}

        # (b) multipart/form-data -> 415 (stays rejected everywhere).
        resp_b = client.post(
            "/api/import/db",
            content=b"--x\r\n--x--\r\n",
            headers={
                "Origin": "http://testserver",
                "X-CSRF-Token": csrf_token,
                "Content-Type": "multipart/form-data; boundary=x",
            },
        )
        assert resp_b.status_code == 415

        # (c) octet-stream to a NON-allowlisted mutating path -> 415.
        resp_c = client.post(
            "/api/catchup",
            content=b"x",
            headers={
                "Origin": "http://testserver",
                "X-CSRF-Token": csrf_token,
                "Content-Type": "application/octet-stream",
            },
        )
        assert resp_c.status_code == 415

        # (d) cross-origin -> 403.
        resp_d = client.post(
            "/api/import/db",
            content=b"x",
            headers={
                "Origin": "https://evil.example",
                "X-CSRF-Token": csrf_token,
                "Content-Type": "application/octet-stream",
            },
        )
        assert resp_d.status_code == 403
        assert resp_d.json() == {"error": "cross-origin mutation rejected"}

        sites = client.get("/api/sites").json()
    names = {s["name"] for s in sites}
    assert names == {"Guard Site"}, "live DB must be untouched by all 4 rejections"


def test_import_under_ingress_bare_path_resolves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I7: Supervisor client + X-Ingress-Path -> import resolves and succeeds.

    The request TARGET is the bare path: IngressPathMiddleware prepends the
    prefix onto scope["path"] internally (ingress.py:34), and Starlette's
    routing strips root_path back off for matching (get_route_path) -- the
    same bare-path idiom as test_static_ingress.py's regression oracle.

    The csrf cookie set_csrf_cookie() issues under ingress is Path-scoped to
    the ingress prefix (matching a real browser's ingress-prefixed URL), so
    httpx's cookie jar -- correctly applying RFC 6265 path-matching -- will
    NOT auto-attach it to this test's bare-path POST. A real ingress browser
    session sends its request to the prefixed URL and so has the cookie; this
    test simulates that by re-attaching the jar's stored value explicitly.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    b_path = _build_replacement_db(tmp_path, "source-ingress.db", "Ingress Site")
    payload = b_path.read_bytes()
    with TestClient(
        app, client=(_SUPERVISOR_IP, 4321), follow_redirects=False
    ) as client:
        csrf_token = client.get(
            "/api/csrf", headers={"X-Ingress-Path": _INGRESS_PREFIX}
        ).json()["csrf_token"]
        csrf_cookie = client.cookies.get("csrf")
        assert csrf_cookie is not None, "csrf cookie must have been set"
        resp = client.post(
            "/api/import/db",
            content=payload,
            headers={
                "X-Ingress-Path": _INGRESS_PREFIX,
                "Origin": "http://testserver",
                "X-CSRF-Token": csrf_token,
                "Content-Type": "application/octet-stream",
                "Cookie": f"csrf={csrf_cookie}",
            },
        )
    assert resp.status_code == 200, (
        f"expected 200 under ingress; got {resp.status_code}: {resp.text}"
    )


def test_import_standalone_bare_path_parity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I7 (paired positive): standalone, non-Supervisor client -- same parity."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    b_path = _build_replacement_db(tmp_path, "source-standalone.db", "Standalone Site")
    payload = b_path.read_bytes()
    with TestClient(
        app, client=(_NON_SUPERVISOR_IP, 9000), follow_redirects=False
    ) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
    assert resp.status_code == 200, (
        f"standalone import broken; got {resp.status_code}: {resp.text}"
    )


def test_import_content_length_over_cap_rejected_no_temp_created(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I8a: Content-Length header over cap -> 413, no temp ever created."""
    _init_tmp_db(tmp_path)
    monkeypatch.setattr("wxverify.api.routes.db_transfer._MAX_IMPORT_BYTES", 64)
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=b"x" * 200, headers=headers)
    assert resp.status_code == 413, f"expected 413; got {resp.status_code}"
    db_dir = Path(config.db_path).parent
    assert list(db_dir.glob(".wxverify-import-*.db.tmp")) == [], (
        "no import temp must ever be created when the header alone exceeds the cap"
    )


def test_import_oversized_streamed_body_rejected_despite_no_declared_length(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I8b: the byte counter (not the header) enforces the cap.

    A generator ``content=`` gives httpx no way to compute Content-Length, so
    no header is sent at all (empirically verified: declared length defaults
    to 0 in the route) while the real streamed body is counted and capped
    mid-stream -- this exercises "the header can lie; the counter cannot".
    """
    _init_tmp_db(tmp_path)
    monkeypatch.setattr("wxverify.api.routes.db_transfer._MAX_IMPORT_BYTES", 64)
    app = _make_app(monkeypatch)

    def _oversized_body() -> Iterator[bytes]:
        for _ in range(5):
            yield b"x" * 1000

    with TestClient(app, raise_server_exceptions=False) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=_oversized_body(), headers=headers)
    assert resp.status_code == 413, f"expected 413; got {resp.status_code}"
    db_dir = Path(config.db_path).parent
    assert list(db_dir.glob(".wxverify-import-*.db.tmp")) == [], (
        "temp must be removed even when the cap is only caught mid-stream"
    )


def test_import_reclaim_failure_does_not_abort_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I9: a post-swap reclaim failure must not abort the derived rebuild."""
    _init_tmp_db(tmp_path)

    def _raise_reclaim(_conn: sqlite3.Connection) -> int:
        raise RuntimeError("synthetic reclaim failure")

    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.reclaim_all_stale", _raise_reclaim
    )
    app = _make_app(monkeypatch)
    b_path = _build_replacement_db(tmp_path, "source-b.db", "Reclaim Site B")
    payload = b_path.read_bytes()
    with TestClient(app) as client:
        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
    assert resp.status_code == 200, f"expected 200; got {resp.status_code}"
    assert resp.json()["status"] == "imported"
    direct = sqlite3.connect(config.db_path)
    try:
        stamped = direct.execute(
            "SELECT value FROM runtime_state WHERE key='import_rebuild_done_at'"
        ).fetchone()
    finally:
        direct.close()
    assert stamped is not None, (
        "reclaim failure must not abort the derived rebuild that follows "
        "(mutation check: removing the reclaim-only try/except must turn "
        "this red)"
    )


def test_import_fails_cleanly_when_backup_path_already_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pins peters' post-plan backup.exists() guard in _replace_sync.

    Not in the original plan: a same-second import colliding on the
    timestamped backup name must fail loudly rather than let VACUUM INTO's
    own failure unlink a COMPLETE prior backup out from under the operator.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        _make_site(get_db()._conn, "Guarded Site A")  # noqa: SLF001
        b_path = _build_replacement_db(tmp_path, "source-b.db", "Never Served B")
        payload = b_path.read_bytes()

        fixed_now = datetime(2035, 1, 1, 12, 0, 0, tzinfo=UTC)
        monkeypatch.setattr(
            "wxverify.api.routes.db_transfer.utc_now", lambda: fixed_now
        )
        db_dir = Path(config.db_path).parent
        backup_path = db_dir / f"wxverify-{fixed_now:%Y%m%d-%H%M%S}Z.db.bak"
        backup_path.write_bytes(b"pre-existing backup contents")

        headers = _csrf_headers(client)
        resp = client.post("/api/import/db", content=payload, headers=headers)
        assert resp.status_code == 500, f"expected 500; got {resp.status_code}"
        sites = client.get("/api/sites").json()
    names = {s["name"] for s in sites}
    assert names == {"Guarded Site A"}, "live DB must be untouched"
    assert backup_path.exists(), (
        "the pre-existing backup must be preserved, not deleted"
    )
    assert backup_path.read_bytes() == b"pre-existing backup contents", (
        "the pre-existing backup's content must survive untouched"
    )
    db_dir = Path(config.db_path).parent
    assert list(db_dir.glob(".wxverify-import-*.db.tmp")) == [], (
        "upload temp must still be cleaned up on this failure path"
    )
