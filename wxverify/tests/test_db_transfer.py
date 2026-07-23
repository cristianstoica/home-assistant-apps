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
import contextlib
import gzip
import json
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
from starlette.requests import Request
from starlette.responses import Response
from starlette.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.api.csrf import issue_csrf_pair
from wxverify.api.errors import ApiError
from wxverify.api.guard import MutationGuard
from wxverify.api.routes import db_transfer
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
# Part A -- Export (X1-X18: prepare-then-stream begin/status/download trio).
# ---------------------------------------------------------------------------
#
# The old blocking `GET /api/export/db` route was removed; these tests target
# the new registry-backed `POST /api/export/begin` -> `GET /export/status/{id}`
# -> `GET /export/download/{id}` flow. `TestClient` runs the `begin` task's
# VACUUM on its own event loop and runs the download `BackgroundTask` inline, so
# state transitions and post-send cleanup are directly observable via polling.

_EXPORT_BASE = "/api/export"


@pytest.fixture(autouse=True)
def _reset_exports() -> Iterator[None]:
    """Isolate the module-global `_EXPORTS` registry across every test.

    The registry is process-global; without this, a `preparing`/`ready` entry
    (or its temp) from one test bleeds into the next and makes order matter.
    Clears before AND after, unlinking any lingering temp, so the export suite
    is order-independent (runs clean twice in one session).
    """

    def _clear() -> None:
        for job in list(db_transfer._EXPORTS.values()):  # noqa: SLF001
            db_transfer._unlink(job.path)  # noqa: SLF001
        db_transfer._EXPORTS.clear()  # noqa: SLF001

    _clear()
    yield
    _clear()


def _begin_headers(
    client: TestClient, *, origin: str = "http://testserver"
) -> dict[str, str]:
    """CSRF headers for the bodyless `begin` POST (no Content-Type/body)."""
    token = client.get("/api/csrf").json()["csrf_token"]
    return {"Origin": origin, "X-CSRF-Token": token}


def _await_ready(
    client: TestClient, export_id: str, *, tries: int = 50
) -> dict[str, Any]:
    """Poll `status` until it leaves `preparing`; return the terminal JSON.

    A tiny fixture DB's VACUUM completes well within `tries` short sleeps; the
    poll re-enters the TestClient event loop each iteration, which is what
    lets the fire-and-forget `begin` task make progress.
    """
    for _ in range(tries):
        resp = client.get(f"{_EXPORT_BASE}/status/{export_id}")
        assert resp.status_code == 200, f"status: {resp.status_code} {resp.text}"
        data: dict[str, Any] = resp.json()
        if data["state"] != "preparing":
            return data
        time.sleep(0.05)
    raise AssertionError(f"export {export_id} never left 'preparing'")


def _slow_read(delay: float = 0.3) -> Any:
    """A `Database.read` wrapper that sleeps on the loop before the real read.

    Keeps the snapshot in `preparing` long enough that an immediate `status`
    poll deterministically observes it, without a wall-clock assertion.
    """
    real_read = Database.read

    async def _read(self: Database, fn: Any) -> Any:
        await asyncio.sleep(delay)
        return await real_read(self, fn)

    return _read


def test_export_begin_returns_id_then_completes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X1: begin -> 202 {export_id: 32-hex}; snapshot then reaches ready>0."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        _make_site(get_db()._conn, "X1 Site")  # noqa: SLF001
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        assert resp.status_code == 202, f"{resp.status_code}: {resp.text}"
        export_id = resp.json()["export_id"]
        assert re.fullmatch(r"[0-9a-f]{32}", export_id), export_id
        final = _await_ready(client, export_id)
    assert final["state"] == "ready"
    assert final["size"] > 0


def test_export_begin_does_not_block_on_vacuum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X2: begin returns 202 while the VACUUM is still running (preparing).

    Discriminator vs. the removed blocking route: with the snapshot's read
    slowed, begin must still return promptly and an immediate status poll must
    read `preparing` -- the old design could not respond before VACUUM ended.
    """
    _init_tmp_db(tmp_path)
    monkeypatch.setattr(Database, "read", _slow_read())
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        assert resp.status_code == 202
        export_id = resp.json()["export_id"]
        status = client.get(f"{_EXPORT_BASE}/status/{export_id}")
        assert status.status_code == 200
        assert status.json() == {"state": "preparing"}, (
            "begin must return before VACUUM completes (fire-and-forget)"
        )
        _await_ready(client, export_id)  # drain so no task lingers at teardown


def test_export_status_transitions_preparing_to_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X3: preparing -> ready, with `size` equal to the on-disk temp size."""
    _init_tmp_db(tmp_path)
    monkeypatch.setattr(Database, "read", _slow_read())
    app = _make_app(monkeypatch)
    db_dir = Path(config.db_path).parent
    with TestClient(app) as client:
        _make_site(get_db()._conn, "X3 Site")  # noqa: SLF001
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = resp.json()["export_id"]
        assert client.get(f"{_EXPORT_BASE}/status/{export_id}").json() == {
            "state": "preparing"
        }
        final = _await_ready(client, export_id)
        assert final["state"] == "ready"
        # 0.8.2: the served artifact is the compressed `.db.gz`; the raw
        # `.db.tmp` is unlinked once compress succeeds, and `size` is the gz.
        raw = db_dir / f".wxverify-export-{export_id}.db.tmp"
        gz = db_dir / f".wxverify-export-{export_id}.db.gz"
        assert not raw.exists(), "raw snapshot temp must be dropped after compress"
        assert final["size"] == gz.stat().st_size, (
            "reported size must match the compressed snapshot on disk"
        )


def test_export_download_matches_source_and_disposition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X4: download streams an integrity-ok snapshot; rows + filename match.

    Mirrors the old T1 (integrity + row parity) and T3 (Content-Disposition)
    oracles against the new download route.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        _make_site(get_db()._conn, "Export Site One")  # noqa: SLF001
        _make_site(get_db()._conn, "Export Site Two")  # noqa: SLF001
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = resp.json()["export_id"]
        _await_ready(client, export_id)
        # Read the prepared compressed snapshot BEFORE downloading -- the
        # download's post-send background task unlinks it (0.8.2: the served
        # artifact is `.db.gz`; the raw `.db.tmp` is already gone).
        gz = Path(config.db_path).parent / f".wxverify-export-{export_id}.db.gz"
        prepared_bytes = gz.read_bytes()
        dl = client.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert dl.status_code == 200, f"{dl.status_code}: {dl.text}"
        assert dl.content == prepared_bytes, (
            "download must stream the prepared compressed snapshot byte-for-byte"
        )
        assert dl.content[:2] == b"\x1f\x8b", "download must carry gzip magic"
        disposition = dl.headers.get("content-disposition", "")
        assert re.fullmatch(
            r'attachment; filename="wxverify-\d{8}-\d{6}Z\.db\.gz"', disposition
        ), f"unexpected Content-Disposition: {disposition!r}"
        out = tmp_path / "downloaded.db"
        out.write_bytes(gzip.decompress(dl.content))
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


def test_export_download_includes_uncheckpointed_wal_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X5: WAL-consistency -- an uncheckpointed committed row must export.

    Mirrors old T2: the snapshot runs VACUUM INTO on the read connection,
    which sees WAL-committed data; a naive copy of the main file alone would
    miss the row while the -wal sidecar is still non-empty.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        site_id = _make_site(get_db()._conn, "WAL Site")  # noqa: SLF001
        wal_path = Path(f"{config.db_path}-wal")
        assert wal_path.exists() and wal_path.stat().st_size > 0, (
            "expected a non-empty -wal sidecar before export -- fixture setup failed"
        )
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = resp.json()["export_id"]
        _await_ready(client, export_id)
        dl = client.get(f"{_EXPORT_BASE}/download/{export_id}")
    assert dl.status_code == 200
    out = tmp_path / "downloaded.db"
    # 0.8.2: the download is gzip-compressed -- decompress before opening it.
    out.write_bytes(gzip.decompress(dl.content))
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


def test_export_download_cleans_up_temp_and_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X6: a successful download unlinks the temp AND pops the registry entry.

    `TestClient` runs `BackgroundTask(_finish_download)` inline before the GET
    returns, so both effects are observable immediately after download.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    db_dir = Path(config.db_path).parent
    with TestClient(app) as client:
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = resp.json()["export_id"]
        _await_ready(client, export_id)
        dl = client.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert dl.status_code == 200
        assert list(db_dir.glob(".wxverify-export-*.db.tmp")) == [], (
            "temp must be unlinked after a successful download"
        )
        assert export_id not in db_transfer._EXPORTS, (  # noqa: SLF001
            "registry entry must be popped after a successful download"
        )


def test_export_download_before_ready_returns_409(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X7: download while still preparing -> 409; temp + entry survive.

    Paired with X4/X6 (ready -> 200 -> cleaned): the 409 here must NOT reap
    the in-flight snapshot's temp or drop its entry.
    """
    _init_tmp_db(tmp_path)
    monkeypatch.setattr(Database, "read", _slow_read())
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = resp.json()["export_id"]
        dl = client.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert dl.status_code == 409
        assert dl.json() == {"error": "export still preparing"}
        assert export_id in db_transfer._EXPORTS, (  # noqa: SLF001
            "a 409-on-preparing must not drop the live entry"
        )
        # The 409 must not damage the in-flight snapshot: it still completes to
        # ready and downloads successfully afterward. (The download handler
        # raises 409 before any _unlink, so the preparing entry/temp are
        # untouched; asserting temp presence directly is timing-fragile because
        # VACUUM INTO only creates the temp once the slowed read actually runs.)
        assert _await_ready(client, export_id)["state"] == "ready"
        assert client.get(f"{_EXPORT_BASE}/download/{export_id}").status_code == 200


def test_export_status_and_download_unknown_id_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X8: an unknown (well-formed) id -> 404 on both status and download."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    unknown = uuid.uuid4().hex  # well-formed but never registered
    with TestClient(app, raise_server_exceptions=False) as client:
        status = client.get(f"{_EXPORT_BASE}/status/{unknown}")
        assert status.status_code == 404
        assert status.json() == {"error": "unknown export id"}
        dl = client.get(f"{_EXPORT_BASE}/download/{unknown}")
        assert dl.status_code == 404
        assert dl.json() == {"error": "unknown export id"}


def test_export_snapshot_failure_surfaces_error_not_hung_preparing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X9: a snapshot failure -> state:error, download 409, no leaked temp.

    The prepare task's `except Exception` branch must mark the entry terminal
    (never leave it hung in `preparing`) and unlink the temp.
    """
    _init_tmp_db(tmp_path)

    async def _read_raises(self: Database, fn: Any) -> None:
        raise sqlite3.OperationalError("synthetic snapshot failure")

    monkeypatch.setattr(Database, "read", _read_raises)
    app = _make_app(monkeypatch)
    db_dir = Path(config.db_path).parent
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        assert resp.status_code == 202
        export_id = resp.json()["export_id"]
        final = _await_ready(client, export_id)
        assert final == {"state": "error"}, "a failed snapshot must be terminal:error"
        dl = client.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert dl.status_code == 409
        assert dl.json() == {"error": "snapshot failed"}
    assert list(db_dir.glob(".wxverify-export-*.db.tmp")) == [], (
        "the prepare task must unlink the temp on failure"
    )


def test_export_begin_sweeps_stale_glob_temp_keeps_fresh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X10: begin's glob sweep unlinks a 2h-backdated temp, keeps a fresh one."""
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
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        assert resp.status_code == 202
        _await_ready(client, resp.json()["export_id"])
    assert not stale.exists(), "backdated temp must be swept on begin"
    assert fresh.exists(), "fresh temp must survive the sweep"


def test_export_begin_sweep_tolerates_concurrent_removal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X11: a temp whose stat() races to FileNotFoundError must not fail begin."""
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
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        assert resp.status_code == 202, (
            "a concurrently-removed temp must not fail begin "
            "(_sweep_stale swallows FileNotFoundError/OSError)"
        )
        _await_ready(client, resp.json()["export_id"])


def test_export_begin_registry_sweep_reaps_abandoned_terminal_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X12: _sweep_registry drops an old terminal entry, keeps an old preparing.

    Paired: the `ready` (terminal) backdated entry + its temp are reaped; the
    `preparing` backdated entry is skipped (a live VACUUM owns its temp).
    """
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    old = time.time() - 7200

    ready_temp = db_dir / ".wxverify-export-oldready000000000000000000000000.db.tmp"
    ready_temp.write_bytes(b"ready")
    db_transfer._EXPORTS["oldready"] = db_transfer._ExportJob(  # noqa: SLF001
        state="ready", path=ready_temp, created_at=old, size=5
    )
    prep_temp = db_dir / ".wxverify-export-oldprep0000000000000000000000000.db.tmp"
    prep_temp.write_bytes(b"prep")
    db_transfer._EXPORTS["oldprep"] = db_transfer._ExportJob(  # noqa: SLF001
        state="preparing", path=prep_temp, created_at=old
    )

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        assert resp.status_code == 202
        assert "oldready" not in db_transfer._EXPORTS, (  # noqa: SLF001
            "an old terminal entry must be reaped by _sweep_registry"
        )
        assert not ready_temp.exists(), "the reaped entry's temp must be unlinked"
        assert "oldprep" in db_transfer._EXPORTS, (  # noqa: SLF001
            "an old `preparing` entry must be skipped (its VACUUM owns the temp)"
        )
        assert prep_temp.exists(), "a skipped preparing entry's temp must survive"
        _await_ready(client, resp.json()["export_id"])


def test_export_expired_ready_entry_returns_409(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X13: a ready entry whose temp vanished -> 409 expired, entry popped.

    Deterministic (no Starlette FileResponse RuntimeError on a missing file):
    download checks existence and returns 409 before constructing the response.
    """
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    missing = db_dir / ".wxverify-export-gone00000000000000000000000000000.db.tmp"
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        db_transfer._EXPORTS["expired"] = db_transfer._ExportJob(  # noqa: SLF001
            state="ready", path=missing, created_at=time.time(), size=123
        )
        dl = client.get(f"{_EXPORT_BASE}/download/expired")
        assert dl.status_code == 409
        assert dl.json() == {"error": "export expired"}
        assert "expired" not in db_transfer._EXPORTS, (  # noqa: SLF001
            "an expired ready entry must be popped on the 409"
        )


def test_export_flow_under_ingress_and_standalone_parity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X14: full begin->status->download works under ingress AND standalone.

    Confirms the trio resolves at the bare path both behind the Supervisor
    ingress prefix (I7 cookie idiom) and for a standalone client.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)

    # Standalone (non-Supervisor) client.
    with TestClient(
        app, client=(_NON_SUPERVISOR_IP, 9000), follow_redirects=False
    ) as client:
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        assert resp.status_code == 202, f"standalone begin: {resp.text}"
        export_id = resp.json()["export_id"]
        _await_ready(client, export_id)
        dl = client.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert dl.status_code == 200, "standalone download must succeed"

    # Supervisor client under the ingress prefix (bare-path request target).
    with TestClient(
        app, client=(_SUPERVISOR_IP, 4321), follow_redirects=False
    ) as client:
        token = client.get(
            "/api/csrf", headers={"X-Ingress-Path": _INGRESS_PREFIX}
        ).json()["csrf_token"]
        csrf_cookie = client.cookies.get("csrf")
        assert csrf_cookie is not None, "csrf cookie must have been set under ingress"
        begin = client.post(
            f"{_EXPORT_BASE}/begin",
            headers={
                "X-Ingress-Path": _INGRESS_PREFIX,
                "Origin": "http://testserver",
                "X-CSRF-Token": token,
                "Cookie": f"csrf={csrf_cookie}",
            },
        )
        assert begin.status_code == 202, f"ingress begin: {begin.text}"
        export_id = begin.json()["export_id"]
        for _ in range(50):
            status = client.get(
                f"{_EXPORT_BASE}/status/{export_id}",
                headers={"X-Ingress-Path": _INGRESS_PREFIX},
            )
            assert status.status_code == 200
            if status.json()["state"] != "preparing":
                break
            time.sleep(0.05)
        else:
            raise AssertionError("ingress export never left preparing")
        assert status.json()["state"] == "ready"
        dl = client.get(
            f"{_EXPORT_BASE}/download/{export_id}",
            headers={"X-Ingress-Path": _INGRESS_PREFIX},
        )
        assert dl.status_code == 200, f"ingress download: {dl.status_code}"


def test_export_begin_enforces_csrf_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X15: begin is a mutating route -> MutationGuard enforces CSRF + origin."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        token = client.get("/api/csrf").json()["csrf_token"]

        # (a) missing X-CSRF-Token -> 403 bad csrf token.
        missing = client.post(
            f"{_EXPORT_BASE}/begin", headers={"Origin": "http://testserver"}
        )
        assert missing.status_code == 403
        assert missing.json() == {"error": "bad csrf token"}

        # (b) cross-origin -> 403 cross-origin mutation rejected.
        cross = client.post(
            f"{_EXPORT_BASE}/begin",
            headers={"Origin": "https://evil.example", "X-CSRF-Token": token},
        )
        assert cross.status_code == 403
        assert cross.json() == {"error": "cross-origin mutation rejected"}

        # Paired positive: a valid same-origin CSRF request is accepted.
        ok = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        assert ok.status_code == 202
        _await_ready(client, ok.json()["export_id"])


def test_export_glob_sweep_never_unlinks_live_preparing_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X16: the glob sweep skips a `preparing` entry's temp even when backdated.

    Paired: a backdated temp OWNED by a `preparing` entry survives (skipped via
    the `active` set); an equally-backdated UNREGISTERED temp is reaped -- so
    the survival is attributable to registration, not to mtime.
    """
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    old_time = time.time() - 7200

    live = db_dir / ".wxverify-export-liveprep000000000000000000000000.db.tmp"
    live.write_bytes(b"live-prep")
    os.utime(live, (old_time, old_time))
    db_transfer._EXPORTS["liveprep"] = db_transfer._ExportJob(  # noqa: SLF001
        state="preparing", path=live, created_at=time.time()
    )
    orphan = db_dir / ".wxverify-export-orphan00000000000000000000000000.db.tmp"
    orphan.write_bytes(b"orphan")
    os.utime(orphan, (old_time, old_time))

    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        assert resp.status_code == 202
        assert live.exists(), (
            "a backdated temp held by a `preparing` entry must NOT be swept"
        )
        assert not orphan.exists(), (
            "an equally-backdated UNREGISTERED temp must be swept"
        )
        _await_ready(client, resp.json()["export_id"])


def test_export_temp_vanished_after_vacuum_surfaces_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X17: temp reaped right after VACUUM -> state:error (never hung preparing).

    Wraps the read so the snapshot temp is unlinked immediately after the real
    VACUUM returns, simulating a concurrent reap. 0.8.2: the reap now lands
    during `_compress` (which reopens the raw temp), so the failure surfaces as
    a terminal `compress failed` error rather than the old post-VACUUM stat()
    path -- either way the entry must go terminal, never hang in `preparing`.
    """
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    real_read = Database.read

    async def _read_then_reap(self: Database, fn: Any) -> Any:
        result = await real_read(self, fn)  # runs VACUUM INTO -> temp exists
        for leftover in db_dir.glob(".wxverify-export-*.db.tmp"):
            leftover.unlink(missing_ok=True)
        return result

    monkeypatch.setattr(Database, "read", _read_then_reap)
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        assert resp.status_code == 202
        export_id = resp.json()["export_id"]
        final = _await_ready(client, export_id)
        assert final == {"state": "error"}, (
            "a temp missing after VACUUM must surface error, not hang in preparing"
        )
        dl = client.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert dl.status_code == 409
        # 0.8.2: the reap lands during compress (which opens the raw temp),
        # so the terminal error is "compress failed", not the old "snapshot
        # failed" post-VACUUM stat() path -- still terminal:error, 409 download.
        assert dl.json() == {"error": "compress failed"}


# --- X18: content-type allowlist gate under chunk framing (direct-app) ------
# ingress_stream forwards the POST body chunk-framed (Transfer-Encoding set, no
# Content-Length). These drive MutationGuard.dispatch directly with a fabricated
# request because httpx auto-computes Content-Length and cannot emit the
# header-less chunked shape HA ingress produces; the chunk framing is incidental
# here -- the guard gates the content-type allowlist on Content-Type presence.


async def _dummy_asgi(scope: object, receive: object, send: object) -> None:
    """Inert downstream app for the guard (dispatch never invokes it here)."""


def _run_guard(headers: dict[str, str], *, path: str) -> Response:
    """Run MutationGuard.dispatch over a fabricated POST; 200 == passed guard."""
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    scope: dict[str, Any] = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": raw,
        "server": ("testserver", 80),
        "client": (_NON_SUPERVISOR_IP, 12345),
    }
    request = Request(scope)
    guard = MutationGuard(_dummy_asgi, standalone_origin=None)

    async def _call_next(_req: Request) -> Response:
        return Response(status_code=200)  # sentinel: request passed the guard

    return asyncio.run(guard.dispatch(request, _call_next))


def test_guard_rejects_declared_disallowed_content_type() -> None:
    """X18: the content-type allowlist gate rejects a DECLARED disallowed
    Content-Type (chunk-framed or not); the gate keys on Content-Type presence.

    - multipart on a mutating route -> 415 (declared, not in the allowlist)
    - octet-stream on a NON-import mutating route -> 415
    - json is in the allowlist and reaches CSRF (valid -> through)
    - begin with NO declared Content-Type stays a pass (nothing to reject)
    """
    pair = issue_csrf_pair()
    csrf = {"X-CSRF-Token": pair.token, "Cookie": f"csrf={pair.nonce}"}

    # (a) multipart declared, chunk-framed -> 415 (disallowed content-type).
    multipart = _run_guard(
        {
            "Transfer-Encoding": "chunked",
            "Content-Type": "multipart/form-data; boundary=x",
            **csrf,
        },
        path="/api/catchup",
    )
    assert multipart.status_code == 415, "chunk-framed multipart must be rejected"
    assert json.loads(multipart.body) == {"error": "disallowed content-type"}, (
        "415 body must carry the disallowed-content-type error contract"
    )

    # (b) octet-stream to a non-import mutating route, chunk-framed -> 415.
    octet = _run_guard(
        {
            "Transfer-Encoding": "chunked",
            "Content-Type": "application/octet-stream",
            **csrf,
        },
        path="/api/catchup",
    )
    assert octet.status_code == 415, "octet-stream stays import-only under chunking"

    # (c) json, chunk-framed, valid CSRF -> passes the allowlist and CSRF (200).
    js = _run_guard(
        {
            "Transfer-Encoding": "chunked",
            "Content-Type": "application/json",
            "Origin": "http://testserver",
            **csrf,
        },
        path="/api/catchup",
    )
    assert js.status_code == 200, (
        "a json mutation must still pass the allowlist under chunk framing"
    )

    # (d) begin with no declared Content-Type -> pass (nothing to reject).
    bodyless = _run_guard(
        {"Origin": "http://testserver", **csrf},
        path="/api/export/begin",
    )
    assert bodyless.status_code == 200, (
        "a begin with no declared Content-Type must pass the allowlist gate"
    )


def _dispatch_begin(headers: dict[str, str]) -> tuple[int, bytes]:
    """Drive `MutationGuard.dispatch` over a fabricated bodyless `begin` POST,
    invoking the REAL `export_begin` handler as ``call_next``.

    Uses a hand-built scope (not httpx) so ``Transfer-Encoding: chunked`` is
    genuinely present with no Content-Length -- the exact shape HA ingress
    (``ingress_stream: true``) forwards for a bodyless POST, and the shape
    httpx cannot emit (it auto-computes Content-Length). Returns
    ``(status, body)`` of the guard's response: the real 202 + ``export_id``
    when the request passes the guard and reaches the handler.
    """
    raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    scope: dict[str, Any] = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/export/begin",
        "raw_path": b"/api/export/begin",
        "query_string": b"",
        "root_path": "",
        "headers": raw,
        "server": ("testserver", 80),
        "client": (_NON_SUPERVISOR_IP, 12345),
    }
    guard = MutationGuard(_dummy_asgi, standalone_origin=None)

    async def _call_next(_req: Request) -> Response:
        return await db_transfer.export_begin()

    async def _drive() -> tuple[int, bytes]:
        response = await guard.dispatch(Request(scope), _call_next)
        status, body = response.status_code, response.body
        # Cancel the fire-and-forget snapshot task so asyncio.run's loop closes
        # clean; `_reset_exports` unlinks the registered temp at teardown.
        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        for task in pending:
            with contextlib.suppress(BaseException):
                await task
        return status, body

    return asyncio.run(_drive())


def test_export_begin_survives_bodyless_chunked_ingress_framing(
    tmp_path: Path,
) -> None:
    """Regression (live 0.8.0 `415` on begin): HA ingress forwards a BODYLESS
    `POST /api/export/begin` as ``Transfer-Encoding: chunked`` with
    Content-Length stripped and NO Content-Type. The 0.8.0 guard derived
    ``has_body`` from the presence of Transfer-Encoding and 415'd such a
    request (``content_type == "" not in allowed``) BEFORE the handler ran.

    The 0.8.1 fix gates the allowlist on Content-Type PRESENCE, so a request
    with no declared Content-Type passes the guard and reaches the handler.
    The exact live signature must therefore now return 202 + an ``export_id``.

    Red/green: under the removed ``has_body``-includes-Transfer-Encoding
    predicate this request is 415'd before the handler (see the scratchpad
    red-proof driving HEAD's guard); under the fix it is 202. Same-origin +
    CSRF still run -- this test carries a valid Origin and CSRF pair, so the
    only thing the fix changes is the content-type gate.
    """
    _init_tmp_db(tmp_path)
    pair = issue_csrf_pair()
    headers = {
        # ingress_stream frames even a bodyless POST as chunked, no Content-Type.
        "Transfer-Encoding": "chunked",
        "Origin": "http://testserver",
        "Host": "testserver",
        "X-CSRF-Token": pair.token,
        "Cookie": f"csrf={pair.nonce}",
    }

    status, body = _dispatch_begin(headers)

    assert status == 202, (
        f"bodyless chunked begin must reach handler: {status} {body!r}"
    )
    export_id = json.loads(body)["export_id"]
    assert re.fullmatch(r"[0-9a-f]{32}", export_id), export_id


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


# ---------------------------------------------------------------------------
# Part C -- 0.8.2 gzip transfer (G1-G13).
# ---------------------------------------------------------------------------
#
# 0.8.2 gzip-compresses the export snapshot (`_compress` -> `.db.gz`) and
# auto-detects gzip on import in `_stream_to` (2-byte magic sniff -> bounded
# streaming inflate, 413-capped on the DECOMPRESSED size; raw passthrough for
# non-gzip). These tests target that new surface. Byte-boundary behavior is
# driven directly through `_stream_to` with a hand-built ASGI receive so the
# exact chunk framing (a single-byte first chunk) is deterministic -- httpx
# does not let a test control stream chunk boundaries.


def _stream_request(chunks: list[bytes]) -> Request:
    """A POST `Request` whose body streams exactly ``chunks`` in order.

    The hand-built ASGI ``receive`` delivers one ``http.request`` message per
    chunk (``more_body`` True on all but the last), giving a test byte-exact
    control over ``request.stream()`` chunk boundaries -- the seam the gzip
    sniff's <2-byte-prefix buffering turns on. An empty ``chunks`` yields a
    single empty terminal message (an empty upload).
    """
    scope: dict[str, Any] = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/import/db",
        "raw_path": b"/api/import/db",
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "server": ("testserver", 80),
        "client": (_NON_SUPERVISOR_IP, 12345),
    }
    messages: list[dict[str, Any]] = [
        {"type": "http.request", "body": chunk, "more_body": i < len(chunks) - 1}
        for i, chunk in enumerate(chunks)
    ] or [{"type": "http.request", "body": b"", "more_body": False}]
    pending = iter(messages)

    async def receive() -> dict[str, Any]:
        return next(pending, {"type": "http.request", "body": b"", "more_body": False})

    return Request(scope, receive)


def _run_stream_to(chunks: list[bytes], tmp: Path) -> int:
    """Drive the real `_stream_to` over ``chunks``; return the written count."""
    return asyncio.run(db_transfer._stream_to(_stream_request(chunks), tmp))  # noqa: SLF001


# --- G1-G3: export produces gzip, downloads it, round-trips through import --


def test_export_produces_ready_gzip_snapshot_round_trips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G1: begin -> ready gz; artifact is single-member gzip, size/path/raw
    are the gz contract, and its decompressed bytes import byte-identically.

    Pins the whole 0.8.2 export contract at once: `job.state=="ready"`,
    `job.path` ends `.db.gz` with gzip magic, `job.size` == the gz on disk,
    the raw `.db.tmp` was unlinked, and feeding the gz through the real import
    inflate yields exactly `gzip.decompress(gz)` -- which is a valid SQLite DB
    (`_validate_upload` passes). Byte-identity of the inflate is the paired
    positive that makes the corrupt/truncated 422 tests non-vacuous.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    db_dir = Path(config.db_path).parent
    with TestClient(app) as client:
        _make_site(get_db()._conn, "Gzip Round Trip Site")  # noqa: SLF001
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = resp.json()["export_id"]
        final = _await_ready(client, export_id)
        job = db_transfer._EXPORTS[export_id]  # noqa: SLF001
        gz_path = db_dir / f".wxverify-export-{export_id}.db.gz"
        raw_path = db_dir / f".wxverify-export-{export_id}.db.tmp"

        assert job.state == "ready"
        assert job.path == gz_path and job.path.name.endswith(".db.gz")
        assert not raw_path.exists(), "raw .db.tmp must be unlinked after compress"
        gz_bytes = gz_path.read_bytes()
        assert gz_bytes[:2] == b"\x1f\x8b", "artifact must carry gzip magic"
        assert final["size"] == len(gz_bytes) == gz_path.stat().st_size

    # The gz decompresses to a valid SQLite DB, and the real import inflate
    # reproduces it byte-for-byte on disk.
    decompressed = gzip.decompress(gz_bytes)
    assert decompressed[:16] == b"SQLite format 3\x00"
    import_tmp = tmp_path / "inflated.db"
    written = _run_stream_to([gz_bytes], import_tmp)
    assert written == len(decompressed)
    assert import_tmp.read_bytes() == decompressed, (
        "the streaming inflate must reproduce the compressed snapshot exactly"
    )
    db_transfer._validate_upload(import_tmp)  # must not raise  # noqa: SLF001


def test_export_download_gzip_media_type_and_filename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G2 (surface 7): download of a ready gz job carries `application/gzip`,
    a `.db.gz` filename, and gzip magic in its body."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        _make_site(get_db()._conn, "Gzip Download Site")  # noqa: SLF001
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = resp.json()["export_id"]
        _await_ready(client, export_id)
        dl = client.get(f"{_EXPORT_BASE}/download/{export_id}")
    assert dl.status_code == 200
    assert dl.headers.get("content-type") == "application/gzip", (
        "media_type must be application/gzip"
    )
    disposition = dl.headers.get("content-disposition", "")
    assert re.fullmatch(
        r'attachment; filename="wxverify-\d{8}-\d{6}Z\.db\.gz"', disposition
    ), f"unexpected Content-Disposition: {disposition!r}"
    assert dl.content[:2] == b"\x1f\x8b", "download body must be gzip"


def test_gzip_export_round_trips_end_to_end_through_http_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G3 (surface 1, end-to-end): the exact gz an export emits imports cleanly
    over HTTP and the imported DB serves the exported site."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        _make_site(get_db()._conn, "End To End Site")  # noqa: SLF001
        begin = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = begin.json()["export_id"]
        _await_ready(client, export_id)
        gz_payload = client.get(f"{_EXPORT_BASE}/download/{export_id}").content
        assert gz_payload[:2] == b"\x1f\x8b"

        resp = client.post(
            "/api/import/db", content=gz_payload, headers=_csrf_headers(client)
        )
        assert resp.status_code == 200, f"{resp.status_code}: {resp.text}"
        assert resp.json()["status"] == "imported"
        names = {s["name"] for s in client.get("/api/sites").json()}
    assert names == {"End To End Site"}, "imported gz must serve the exported site"


# --- G4-G5: the 2-byte magic sniff, both branches + the <2-byte boundary ----


def test_stream_to_sniffs_gzip_and_raw_branches(tmp_path: Path) -> None:
    """G4 (surfaces 2 + 3): a gzip body inflates to its source; a raw SQLite
    body passes through byte-for-byte with the inflate branch NOT taken.

    The raw fixture is a real SQLite DB (magic bytes ``SQLite format 3\\x00``,
    never ``1f 8b``), so if the sniff wrongly inflated it the write would error
    or corrupt -- an identity match proves the raw branch was taken.
    """
    raw = _build_replacement_db(tmp_path, "sniff-source.db", "Sniff Site").read_bytes()
    assert raw[:2] != b"\x1f\x8b", "a SQLite file must not look like gzip"

    # gzip branch: inflates back to the exact source bytes.
    gz_tmp = tmp_path / "from-gzip.db"
    assert _run_stream_to([gzip.compress(raw)], gz_tmp) == len(raw)
    assert gz_tmp.read_bytes() == raw, "gzip branch must inflate to the source"

    # raw branch: byte-for-byte passthrough (paired positive).
    raw_tmp = tmp_path / "from-raw.db"
    assert _run_stream_to([raw], raw_tmp) == len(raw)
    assert raw_tmp.read_bytes() == raw, "raw branch must pass through untouched"


def test_stream_to_buffers_sub_two_byte_first_chunk(tmp_path: Path) -> None:
    """G5 (surface 3, R3 boundary): a gzip upload whose FIRST chunk is the
    single magic byte ``\\x1f`` must still be detected and round-trip.

    Proves the <2-byte prefix is BUFFERED, not written raw: if the lone
    ``\\x1f`` were written to the raw branch, the inflate would never run and
    the tmp would differ. Paired with a raw upload whose first chunk is a
    single byte -- that must pass through intact (the buffered prefix flushes
    correctly to the raw branch too).
    """
    payload = b"single-member gzip payload " * 40
    gz = gzip.compress(payload)
    assert gz[:1] == b"\x1f"

    # gzip, first chunk == lone magic byte, remainder in the next chunk.
    gz_tmp = tmp_path / "split-gzip.db"
    written = _run_stream_to([gz[:1], gz[1:]], gz_tmp)
    assert written == len(payload)
    assert gz_tmp.read_bytes() == payload, (
        "a single-byte first chunk must be buffered, not written raw"
    )

    # raw, first chunk == single byte (paired positive): passes through intact.
    raw = b"RAW-not-gzip payload bytes " * 40
    assert raw[:2] != b"\x1f\x8b"
    raw_tmp = tmp_path / "split-raw.db"
    assert _run_stream_to([raw[:1], raw[1:]], raw_tmp) == len(raw)
    assert raw_tmp.read_bytes() == raw, "buffered single-byte prefix must flush raw"


# --- G6: zip-bomb bound -> 413, disk stays bounded --------------------------


def test_gzip_zip_bomb_rejected_413_and_disk_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G6 (surface 4): a gzip whose DECOMPRESSED size crosses the cap -> 413,
    and the on-disk tmp never exceeds the cap (the 413 fires before full
    expansion). Paired with a gzip UNDER the same cap that inflates fully."""
    cap = 4096
    monkeypatch.setattr("wxverify.api.routes.db_transfer._MAX_IMPORT_BYTES", cap)

    # Paired positive: comfortably under the cap -> inflates fully.
    under = b"under the cap"
    ok_tmp = tmp_path / "under.db"
    assert _run_stream_to([gzip.compress(under)], ok_tmp) == len(under)
    assert ok_tmp.read_bytes() == under

    # Bomb: 2 MiB of zeros compresses tiny but expands far past the cap.
    bomb = gzip.compress(b"\x00" * (2 * 1024 * 1024))
    bomb_tmp = tmp_path / "bomb.db"
    with pytest.raises(ApiError) as exc:
        _run_stream_to([bomb], bomb_tmp)
    assert exc.value.status_code == 413
    assert bomb_tmp.stat().st_size <= cap, (
        "413 must fire before the decompressed payload is fully written to disk"
    )


# --- G7-G8: corrupt / truncated gzip -> 422 (never 500) ---------------------


def test_import_corrupt_gzip_returns_422_not_500(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G7 (surface 5, R2): magic present but a corrupt payload -> 422 "not a
    valid gzip" (NOT a 500), live DB untouched, no leaked import temp.

    Uses a deterministic corrupt payload (magic + an invalid compression-method
    byte) rather than `os.urandom`, so the zlib.error branch -- and thus the
    exact 422 message -- is pinned rather than probabilistic.
    """
    conn = _init_tmp_db(tmp_path)
    _make_site(conn, "Corrupt Guard Site")
    conn.commit()
    # `1f 8b` magic, then method byte 0x00 (valid gzip requires 0x08=deflate):
    # zlib raises immediately on the header -> the decompress-loop 422.
    corrupt = b"\x1f\x8b" + b"\x00" * 4096
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.post(
            "/api/import/db", content=corrupt, headers=_csrf_headers(client)
        )
        assert resp.status_code == 422, f"expected 422; got {resp.status_code}"
        assert resp.json() == {"error": "not a valid gzip"}
        names = {s["name"] for s in client.get("/api/sites").json()}
    assert names == {"Corrupt Guard Site"}, "live DB must be untouched"
    db_dir = Path(config.db_path).parent
    assert list(db_dir.glob(".wxverify-import-*.db.tmp")) == [], (
        "import temp must be cleaned up on a corrupt-gzip 422"
    )


def test_import_truncated_gzip_returns_422(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G8 (surface 6, A2): a valid gzip cut off before its 8-byte trailer ->
    422 "truncated gzip stream". Paired positive: the full gzip imports (200).

    Truncation drops only the trailer, so the deflate body decodes without a
    zlib.error; the miss is caught by the post-stream ``decomp.eof`` check --
    distinct from the corrupt-payload path (G7).
    """
    _init_tmp_db(tmp_path)
    b_path = _build_replacement_db(tmp_path, "trunc-source.db", "Trunc Site")
    valid_gz = gzip.compress(b_path.read_bytes())
    truncated = valid_gz[:-8]  # strip exactly the CRC32 + ISIZE trailer
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        headers = _csrf_headers(client)
        bad = client.post("/api/import/db", content=truncated, headers=headers)
        assert bad.status_code == 422, f"expected 422; got {bad.status_code}"
        assert bad.json() == {"error": "truncated gzip stream"}

        # Paired positive: the intact gzip imports cleanly.
        ok = client.post(
            "/api/import/db", content=valid_gz, headers=_csrf_headers(client)
        )
    assert ok.status_code == 200, f"intact gzip must import: {ok.text}"
    db_dir = Path(config.db_path).parent
    assert list(db_dir.glob(".wxverify-import-*.db.tmp")) == [], (
        "import temp must be cleaned up on a truncated-gzip 422"
    )


# --- G9: empty upload -> 422 on both branches -------------------------------


def test_import_empty_upload_returns_422_both_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G9 (surface 10): a raw empty body AND a gzip-of-empty (decompresses to
    zero bytes) both -> 422 "empty upload". Paired positive: a non-empty valid
    import returns 200, so the 422 is not a blanket rejection."""
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    empty_gz = gzip.compress(b"")
    assert empty_gz[:2] == b"\x1f\x8b" and gzip.decompress(empty_gz) == b""
    with TestClient(app, raise_server_exceptions=False) as client:
        raw_empty = client.post(
            "/api/import/db", content=b"", headers=_csrf_headers(client)
        )
        assert raw_empty.status_code == 422, f"raw empty: {raw_empty.status_code}"
        assert raw_empty.json() == {"error": "empty upload"}

        gz_empty = client.post(
            "/api/import/db", content=empty_gz, headers=_csrf_headers(client)
        )
        assert gz_empty.status_code == 422, f"gzip-of-empty: {gz_empty.status_code}"
        assert gz_empty.json() == {"error": "empty upload"}

        # Paired positive: a real payload still imports.
        b_path = _build_replacement_db(tmp_path, "nonempty.db", "Non Empty Site")
        ok = client.post(
            "/api/import/db",
            content=b_path.read_bytes(),
            headers=_csrf_headers(client),
        )
    assert ok.status_code == 200, f"non-empty import must succeed: {ok.text}"


# --- G10: multi-member gzip decodes first member, then rejected -------------


def test_import_multi_member_gzip_is_not_a_validation_bypass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G10 (design note): only single-member gzip is supported -- the inflate
    honors just the FIRST member and routes trailing members to zlib's
    `unused_data`. The safety property under test: extra members cannot smuggle
    content past validation. A two-member gzip whose first member is only HALF
    a DB decodes to that truncated (corrupt) DB and is rejected by the existing
    integrity_check (422), NOT silently accepted.

    Paired positive: the same DB as a single, complete member imports (200).

    (Finding reported alongside this suite: a multi-member gzip whose first
    member IS a complete valid DB imports that member and ignores the rest --
    benign, since the imported content is still fully integrity-checked, but it
    is the reason this test splits the DB rather than appending garbage.)
    """
    _init_tmp_db(tmp_path)
    db_bytes = _build_replacement_db(tmp_path, "member.db", "Member Site").read_bytes()
    single = gzip.compress(db_bytes)
    half = len(db_bytes) // 2
    # Two members, each a valid gzip, but member 1 is only the first half of the
    # DB -> decoding member 1 alone yields a truncated, corrupt SQLite file.
    multi = gzip.compress(db_bytes[:half]) + gzip.compress(db_bytes[half:])
    app = _make_app(monkeypatch)
    with TestClient(app, raise_server_exceptions=False) as client:
        bad = client.post(
            "/api/import/db", content=multi, headers=_csrf_headers(client)
        )
        assert bad.status_code == 422, (
            f"multi-member (partial first member) must be rejected; "
            f"got {bad.status_code}: {bad.text}"
        )
        # Paired positive: the single-member form of the same DB imports.
        ok = client.post(
            "/api/import/db", content=single, headers=_csrf_headers(client)
        )
    assert ok.status_code == 200, f"single-member gzip must import: {ok.text}"


# --- G11-G12: export compress-failure paths mark terminal error, clean temps -


def test_export_compress_failure_terminal_error_cleans_temps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G11 (surface 8a): `_compress` raising -> job terminal `error`
    ("compress failed"), NOT hung `preparing`; both the raw temp and any gz are
    cleaned; download -> 409 with the error."""
    _init_tmp_db(tmp_path)

    def _boom(_src: Path, _dst: Path) -> None:
        raise RuntimeError("synthetic compress failure")

    monkeypatch.setattr(db_transfer, "_compress", _boom)
    app = _make_app(monkeypatch)
    db_dir = Path(config.db_path).parent
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = resp.json()["export_id"]
        final = _await_ready(client, export_id)
        assert final == {"state": "error"}, "compress failure must be terminal:error"
        dl = client.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert dl.status_code == 409
        assert dl.json() == {"error": "compress failed"}
    assert list(db_dir.glob(".wxverify-export-*.db.tmp")) == [], (
        "raw temp must be unlinked on compress failure"
    )
    assert list(db_dir.glob(".wxverify-export-*.db.gz")) == [], (
        "any partial gz must be unlinked on compress failure"
    )


def test_export_compress_cancelled_marks_error_reraises_cleans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G12 (surface 8b): a CancelledError during compress must mark the entry
    terminal `error`/"cancelled", re-raise the CancelledError, and unlink both
    temps -- never leave the entry hung in `preparing` (the sweep skips
    `preparing` forever).

    Drives `_prepare_export` directly and injects the cancel at the compress
    step (deterministic: the exact `except asyncio.CancelledError` cleanup
    branch is exercised without a thread-timing race).
    """
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    export_id = "cancelprobe0000000000000000000000"
    tmp = db_dir / f".wxverify-export-{export_id}.db.tmp"
    gz = db_dir / f".wxverify-export-{export_id}.db.gz"
    db_transfer._EXPORTS[export_id] = db_transfer._ExportJob(  # noqa: SLF001
        state="preparing", path=tmp, created_at=time.time()
    )

    def _cancel(_src: Path, _dst: Path) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(db_transfer, "_compress", _cancel)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(db_transfer._prepare_export(export_id, tmp))  # noqa: SLF001

    job = db_transfer._EXPORTS[export_id]  # noqa: SLF001
    assert job.state == "error" and job.error == "cancelled", (
        "cancellation must mark terminal:error, never leave it preparing"
    )
    assert not tmp.exists(), "raw temp must be unlinked on cancellation"
    assert not gz.exists(), "any partial gz must be unlinked on cancellation"


# --- G13: the sweep reaps orphaned `.db.gz` --------------------------------


def test_export_begin_sweeps_stale_gz_keeps_fresh_gz(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """G13 (surface 9): `_TMP_GLOBS` now covers `.wxverify-export-*.db.gz`, so
    begin's sweep unlinks an aged orphaned gz and keeps a fresh one.

    Paired on mtime alone (both unregistered): the stale gz (mtime past
    `_STALE_AFTER_S`) is reaped; the fresh gz survives -- so the survival is
    attributable to age, and the reaping to the new glob covering `.gz`.
    """
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    stale = db_dir / ".wxverify-export-stalegz00000000000000000000000.db.gz"
    fresh = db_dir / ".wxverify-export-freshgz00000000000000000000000.db.gz"
    stale.write_bytes(gzip.compress(b"stale export"))
    fresh.write_bytes(gzip.compress(b"fresh export"))
    old_time = time.time() - 7200
    os.utime(stale, (old_time, old_time))
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        assert resp.status_code == 202
        assert not stale.exists(), "an aged orphaned .db.gz must be swept on begin"
        assert fresh.exists(), "a fresh .db.gz must survive the sweep"
        _await_ready(client, resp.json()["export_id"])
