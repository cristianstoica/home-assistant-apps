"""Tests for DB export (db_transfer routes).

The database import half (upload, validation, rebuild, swap) was removed in
the storage-release plan (5.5); only export (``/export/begin``,
``/export/status``, ``/export/download``) remains, and so do its tests.

Harness idioms match test_web_ui.py / test_static_ingress.py: per-test tmp DB
via ``_init_tmp_db``, idle-worker app, ``TestClient(app, client=(ip, port))``
for Supervisor-vs-standalone discrimination. ``TestClient`` runs
``BackgroundTask`` inline before ``client.post()``/``client.get()`` return, so
export cleanup is directly observable without polling.

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
import sqlite3
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from starlette.requests import Request
from starlette.responses import Response
from starlette.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.api.csrf import issue_csrf_pair
from wxverify.api.guard import MutationGuard
from wxverify.api.routes import db_transfer
from wxverify.db.connection import Database, close_db, get_db, init_db

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
            VALUES (?, 40.0, -105.0, 900.0, 'UTC', ?)
            """,
            (name, enabled),
        ).lastrowid
    )


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
        # Read the prepared compressed snapshot to compare byte-for-byte. The
        # served artifact is the `.db.gz` (the raw `.db.tmp` was dropped at
        # compress). Retain-and-resume: the gz is NOT unlinked post-download --
        # it persists (see test_export_download_retains_artifact_and_registry_entry
        # and the Range-resume regression), so reading it before or after the
        # GET is equivalent.
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


def test_export_download_retains_artifact_and_registry_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X6: a successful download RETAINS the .db.gz AND its registry entry.

    Retain-and-resume: the post-send ``BackgroundTask(_finish_download)`` was
    removed, so a downloaded export is deliberately NOT deleted -- the identical
    file is served on every subsequent GET (repeat download or ``Range:``
    resume). Cleanup is entirely the TTL sweep's job (``run_export_sweeper`` /
    on-``/begin`` ``_sweep_registry``), covered separately.

    The raw ``.db.tmp`` glob-empty assertion still holds: the raw snapshot is
    unlinked at compress time, well before download; only the served ``.db.gz``
    persists.
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
            "raw snapshot temp must already be unlinked (dropped at compress)"
        )
        gz = db_dir / f".wxverify-export-{export_id}.db.gz"
        assert gz.exists(), (
            "the served .db.gz must SURVIVE the download (retain-and-resume): "
            "restoring BackgroundTask(_finish_download) would turn this red"
        )
        job = db_transfer._EXPORTS.get(export_id)  # noqa: SLF001
        assert job is not None and job.state == "ready", (
            "registry entry must be RETAINED (state=ready) after a download"
        )


def test_export_download_range_resume_and_repeat_get(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X6b (retain-and-resume regression): a retained export serves a `Range:`
    resume (206) and a plain repeat GET (200, full bytes) against the same id.

    This is the exact bug's oracle. The old one-shot
    ``BackgroundTask(_finish_download)`` deleted the gz and popped the entry
    after the FIRST send, so a browser's auto-resume `Range:` GET to the same
    URL 404'd ("unknown export id"). With the file retained, Starlette's
    ``FileResponse`` serves the identical bytes every time, so:

    - `Range: bytes=<half>-` -> 206 + correct `Content-Range` + `Accept-Ranges:
      bytes`, body == the corresponding tail slice of the full gz;
    - a plain repeat GET -> 200 with the full bytes, entry + gz still present.

    Under the removed BackgroundTask both follow-up GETs would 404 -- so this
    goes red on the unfixed code for the right reason.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    db_dir = Path(config.db_path).parent
    with TestClient(app) as client:
        _make_site(get_db()._conn, "Resume Site")  # noqa: SLF001
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = resp.json()["export_id"]
        _await_ready(client, export_id)

        # First full download establishes the served byte string.
        first = client.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert first.status_code == 200
        full = first.content
        assert len(full) > 1, "gz must be non-trivial to slice a Range"
        assert first.headers.get("accept-ranges") == "bytes", (
            "a retained file response must advertise byte-range support"
        )

        # Resume from the midpoint: 206 + exact Content-Range + tail bytes.
        start = len(full) // 2
        ranged = client.get(
            f"{_EXPORT_BASE}/download/{export_id}",
            headers={"Range": f"bytes={start}-"},
        )
        assert ranged.status_code == 206, (
            f"Range resume must return 206, got {ranged.status_code} "
            "(the removed BackgroundTask would 404 the resumed id)"
        )
        assert ranged.headers.get("accept-ranges") == "bytes"
        assert ranged.headers.get("content-range") == (
            f"bytes {start}-{len(full) - 1}/{len(full)}"
        ), f"unexpected Content-Range: {ranged.headers.get('content-range')!r}"
        assert ranged.content == full[start:], (
            "resumed bytes must equal the corresponding slice of the full gz"
        )

        # Plain repeat GET: the artifact is reusable -- full bytes again, 200.
        repeat = client.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert repeat.status_code == 200, "a retained export must serve a repeat GET"
        assert repeat.content == full, "repeat GET must return the identical bytes"

        # Bounded FIRST chunk (0.8.5 chunked-download contract): the browser
        # assembles the download from ~17 sequential ~4 MB bounded `Range`
        # requests (`bytes=0-N`, `bytes=N+1-2N+1`, ...) into a local Blob to
        # dodge Supervisor's ~30 s ingress-stream cut. Pin the first BOUNDED
        # chunk -- the case X6b's open-ended midpoint range does not exercise:
        # exact `Content-Range` string, `Content-Length == N+1`, and
        # byte-identity against the served `.db.gz` ON DISK (not just the
        # earlier `full` response body).
        served = (db_dir / f".wxverify-export-{export_id}.db.gz").read_bytes()
        assert served == full, "the served gz on disk must equal the full body"
        n = min(8, len(full) - 1)  # small bounded chunk; gz is non-trivial
        head = client.get(
            f"{_EXPORT_BASE}/download/{export_id}",
            headers={"Range": f"bytes=0-{n}"},
        )
        assert head.status_code == 206, (
            f"a bounded first-chunk Range must return 206, got {head.status_code}"
        )
        assert head.headers.get("content-range") == f"bytes 0-{n}/{len(full)}", (
            f"unexpected Content-Range: {head.headers.get('content-range')!r}"
        )
        assert head.headers.get("content-length") == str(n + 1), (
            "bounded-range Content-Length must be exactly the requested span (N+1)"
        )
        assert head.content == served[: n + 1], (
            "bounded-range body must be byte-identical to the served gz's "
            "first N+1 bytes on disk"
        )

        assert (db_dir / f".wxverify-export-{export_id}.db.gz").exists(), (
            "the gz must still exist after multiple downloads"
        )
        assert export_id in db_transfer._EXPORTS, (  # noqa: SLF001
            "the registry entry must persist across repeat/Range downloads"
        )


def test_export_status_size_matches_full_download_length(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X6c (0.8.5 chunked-download contract): `status.size` for a `ready`
    export equals the length the no-Range download actually serves.

    The 0.8.5 browser download reads `status.size` as the total, splits it into
    sequential bounded `Range` chunks, assembles them into a Blob, and gates on
    `blob.size === total`. So the number `status` reports for a `ready` export
    MUST equal the byte length the download serves -- otherwise the client's
    final size gate is comparing against a wrong total. X3 pins
    `size == gz.stat().st_size` and X4 pins `body == gz bytes`; this pins the
    end-to-end equality the frontend actually leans on -- `status.size` against
    the served no-Range download length -- in a single oracle.

    It also documents the exact NO-Range response shape (`200`, full body,
    `Content-Length == size`) that the client's strict `206`-required assembler
    is designed to REJECT: were an ingress proxy to strip the `Range` header and
    the server to fall back to a full `200`, the client must not accept it as a
    chunk. Pinning the unranged shape keeps that contract explicit even though
    the client refuses it.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    with TestClient(app) as client:
        _make_site(get_db()._conn, "Chunk Contract Site")  # noqa: SLF001
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = resp.json()["export_id"]
        final = _await_ready(client, export_id)
        assert final["state"] == "ready"
        size = final["size"]
        assert isinstance(size, int) and size > 0, f"unexpected size: {size!r}"

        # No-Range GET: a plain 200 whose body length equals the reported size.
        dl = client.get(f"{_EXPORT_BASE}/download/{export_id}")
        assert dl.status_code == 200, (
            "an unranged download must be a plain 200 -- the full-body fallback "
            "the 0.8.5 strict-206 assembler is built to reject"
        )
        assert len(dl.content) == size, (
            "status.size must equal the served full-download length: the 0.8.5 "
            "`blob.size === total` gate depends on this exact equality"
        )
        assert dl.headers.get("content-length") == str(size), (
            "no-Range Content-Length must equal the reported status size"
        )


def test_export_chunked_range_fetch_size_and_bytes_stay_consistent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X6d: a FULL chunked walk (mirroring the streaming browser assembler)
    stays internally consistent end-to-end, across every chunk, not just one.

    The browser's download assembler trusts exactly one arithmetic identity it
    has no other way to verify: it reads `status.size` as the declared total,
    issues successive bounded `Range` requests, accumulates `received +=
    chunk.length`, and -- before closing the output file -- asserts `received
    === total`, aborting a partial write on mismatch. X6b/X6c each pin a
    single request in isolation (a midpoint resume, a bounded first chunk, a
    no-Range full download); neither walks the WHOLE artifact or exercises a
    short final chunk. This test does both: the chunk size is derived from the
    actual artifact size so the split is deliberately NOT an exact multiple --
    the final chunk is short, which is exactly where a tail off-by-one hides.
    """
    _init_tmp_db(tmp_path)
    app = _make_app(monkeypatch)
    db_dir = Path(config.db_path).parent
    with TestClient(app) as client:
        _make_site(get_db()._conn, "Chunk Walk Site")  # noqa: SLF001
        resp = client.post(f"{_EXPORT_BASE}/begin", headers=_begin_headers(client))
        export_id = resp.json()["export_id"]
        final = _await_ready(client, export_id)
        total = final["size"]
        assert isinstance(total, int) and total > 0, f"unexpected size: {total!r}"

        gz_path = db_dir / f".wxverify-export-{export_id}.db.gz"
        on_disk = gz_path.read_bytes()

        chunk_size = max(1, total // 6)
        if total % chunk_size == 0:
            chunk_size += 1  # force a short final chunk (non-exact multiple)
        assert total % chunk_size != 0, "test must exercise a short tail chunk"

        received = bytearray()
        pos = 0
        chunks_seen = 0
        while pos < total:
            end = min(pos + chunk_size - 1, total - 1)
            chunk_resp = client.get(
                f"{_EXPORT_BASE}/download/{export_id}",
                headers={"Range": f"bytes={pos}-{end}"},
            )
            assert chunk_resp.status_code == 206, (
                f"chunk [{pos}-{end}] must be 206, got {chunk_resp.status_code}"
            )
            content_range = chunk_resp.headers.get("content-range", "")
            match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
            assert match, f"malformed Content-Range: {content_range!r}"
            resp_start, resp_end, resp_total = (int(g) for g in match.groups())
            assert resp_total == total, (
                f"Content-Range total {resp_total} must equal the declared "
                f"status size {total} (chunk [{pos}-{end}])"
            )
            assert (resp_start, resp_end) == (pos, end), (
                f"Content-Range bounds {resp_start}-{resp_end} must equal the "
                f"requested bounds {pos}-{end}"
            )
            received += chunk_resp.content
            pos = end + 1
            chunks_seen += 1

    assert chunks_seen >= 2, "the walk must exercise more than one chunk"
    assert len(received) == total, (
        f"sum of delivered chunk lengths ({len(received)}) must equal the "
        f"declared total ({total}) -- this is the client's own pre-close gate"
    )
    assert bytes(received) == on_disk, (
        "concatenating the chunks must reproduce the served artifact exactly"
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

    async def _read_raises(fn: Any) -> None:
        raise sqlite3.OperationalError("synthetic snapshot failure")

    app = _make_app(monkeypatch)
    db_dir = Path(config.db_path).parent
    with TestClient(app, raise_server_exceptions=False) as client:
        # Patched on the live instance, AFTER lifespan startup, so only the
        # export path's own snapshot read fails. `lifespan` makes its own
        # startup reads during migration/compaction; a class-level patch
        # applied before the app starts would break those too and fail
        # startup itself rather than the snapshot.
        monkeypatch.setattr(get_db(), "read", _read_raises)
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


def test_sweep_registry_reaps_backdated_retained_ready_export(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X12b: a downloaded-but-retained `ready` export is TTL-reaped by the sweep.

    Retain-and-resume leaves a downloaded export's ``.db.gz`` and its registry
    entry in place, so ``_sweep_registry`` is now the ONLY thing that reaps a
    downloaded-then-never-exported-again snapshot. Directly invoke it (no clock
    sleep): a back-dated (created_at older than ``_STALE_AFTER_S``) ``ready``
    entry whose path is the served ``.db.gz`` must have the gz unlinked and the
    entry popped. Paired positive: a FRESH ``ready`` entry + its gz survive the
    same sweep -- so the reap is attributable to the age cutoff, not to a blanket
    clear.
    """
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    stale_old = time.time() - db_transfer._STALE_AFTER_S - 1  # noqa: SLF001

    stale_gz = db_dir / ".wxverify-export-staleready0000000000000000000000.db.gz"
    stale_gz.write_bytes(b"\x1f\x8b\x08stale-retained-gz")
    db_transfer._EXPORTS["staleready"] = db_transfer._ExportJob(  # noqa: SLF001
        state="ready", path=stale_gz, created_at=stale_old, size=stale_gz.stat().st_size
    )
    fresh_gz = db_dir / ".wxverify-export-freshready0000000000000000000000.db.gz"
    fresh_gz.write_bytes(b"\x1f\x8b\x08fresh-retained-gz")
    db_transfer._EXPORTS["freshready"] = db_transfer._ExportJob(  # noqa: SLF001
        state="ready",
        path=fresh_gz,
        created_at=time.time(),
        size=fresh_gz.stat().st_size,
    )

    db_transfer._sweep_registry()  # noqa: SLF001

    assert "staleready" not in db_transfer._EXPORTS, (  # noqa: SLF001
        "a back-dated retained `ready` export must be reaped by _sweep_registry"
    )
    assert not stale_gz.exists(), "the reaped export's .db.gz must be unlinked"
    assert "freshready" in db_transfer._EXPORTS, (  # noqa: SLF001
        "a fresh `ready` export must survive the sweep (age-cutoff, not blanket)"
    )
    assert fresh_gz.exists(), "a fresh retained export's .db.gz must survive"


def test_run_export_sweeper_loop_reaps_on_tick(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """X12c: the lifespan ``run_export_sweeper`` loop reaps a stale retained
    entry on a tick -- driven deterministically, never a 300 s real sleep.

    The tick period is patched to 0 and the loop's ``_sweep_registry`` is wrapped
    to signal an ``asyncio.Event`` the moment it fires; the task is cancelled as
    soon as the event is set (via ``wait_for``, not a wall-clock poll). This
    pins that the loop actually calls the sweep on the API event loop, not just
    that the sweep function works in isolation.
    """
    _init_tmp_db(tmp_path)
    db_dir = Path(config.db_path).parent
    stale_old = time.time() - db_transfer._STALE_AFTER_S - 1  # noqa: SLF001
    stale_gz = db_dir / ".wxverify-export-loopstale00000000000000000000000.db.gz"
    stale_gz.write_bytes(b"\x1f\x8b\x08loop-stale-gz")
    db_transfer._EXPORTS["loopstale"] = db_transfer._ExportJob(  # noqa: SLF001
        state="ready", path=stale_gz, created_at=stale_old, size=stale_gz.stat().st_size
    )

    real_sweep = db_transfer._sweep_registry  # noqa: SLF001

    async def _drive() -> None:
        fired = asyncio.Event()

        def _wrapped_sweep() -> None:
            real_sweep()
            fired.set()

        monkeypatch.setattr(db_transfer, "_SWEEP_INTERVAL_S", 0)
        monkeypatch.setattr(db_transfer, "_sweep_registry", _wrapped_sweep)
        task = asyncio.create_task(db_transfer.run_export_sweeper())
        try:
            await asyncio.wait_for(fired.wait(), timeout=5)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    asyncio.run(_drive())

    assert "loopstale" not in db_transfer._EXPORTS, (  # noqa: SLF001
        "the sweeper loop must reap a back-dated retained entry on a tick"
    )
    assert not stale_gz.exists(), "the loop-reaped export's .db.gz must be unlinked"


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
    - octet-stream on a mutating route -> 415 (the import route it was for is gone)
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

    # (b) octet-stream, chunk-framed -> 415 (no route allows it anymore).
    octet = _run_guard(
        {
            "Transfer-Encoding": "chunked",
            "Content-Type": "application/octet-stream",
            **csrf,
        },
        path="/api/catchup",
    )
    assert octet.status_code == 415, "octet-stream must be rejected under chunking"

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
