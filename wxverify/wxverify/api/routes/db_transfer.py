"""DB transfer routes: export (VACUUM INTO snapshot) and import (overwrite)."""

from __future__ import annotations

import asyncio
import gzip
import logging
import shutil
import sqlite3
import time
import uuid
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask
from starlette.responses import JSONResponse

from wxverify import config
from wxverify.api.errors import ApiError
from wxverify.core.timeutil import utc_now
from wxverify.db.connection import get_db
from wxverify.db.migrations import TARGET_USER_VERSION
from wxverify.db.queue import reclaim_all_stale
from wxverify.db.runtime_state import set_runtime_state_now
from wxverify.scoring.consensus import materialize_consensus
from wxverify.scoring.engine import pair_and_score

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["export"])

_TMP_GLOBS = (
    ".wxverify-export-*.db.tmp",
    ".wxverify-import-*.db.tmp",
    # The `*.db.tmp` globs do NOT match the `.gz` sibling, so an export that
    # dies after compress but before download would otherwise leak its gz.
    ".wxverify-export-*.db.gz",
)
_STALE_AFTER_S = 3600.0
# 256 MiB: the live DB is single-digit MBs today, and /data must hold upload
# temp + backup + live DB simultaneously, so the cap bounds worst-case disk.
_MAX_IMPORT_BYTES = 256 * 1024 * 1024
# 1 MiB copy/inflate chunk: bounds the per-call decompress output (zip-bomb
# guard) and the compress copy buffer.
_DECOMP_CHUNK = 1 * 1024 * 1024
_REQUIRED_TABLES = ("sites", "stations", "station_observations")


# --- Export registry (prepare-then-stream) --------------------------------
# A tiny in-process store tracks each snapshot's lifecycle so `begin` can return
# immediately (headers emit at once) while `VACUUM INTO` runs off the event
# loop. Matches the repo's module-global singleton idiom (`_db_instance`,
# `_CSRF_KEY`). The map is mutated ONLY from coroutines on the event loop, so no
# lock is needed (single-threaded loop invariant); the sync `_snapshot` worker
# touches only the temp file, never the registry.
@dataclass
class _ExportJob:
    state: Literal["preparing", "ready", "error"]
    path: Path
    created_at: float
    size: int | None = None
    error: str | None = None
    task: asyncio.Task[None] | None = None


_EXPORTS: dict[str, _ExportJob] = {}


def _sweep_stale(db_dir: Path) -> None:
    """Reclaim transfer temps orphaned by a crash or client disconnect.

    Skips temps owned by a live ``preparing`` export: an in-flight VACUUM
    holds its temp open, and an mtime past the cutoff (a very slow VACUUM)
    must not let the sweep unlink the file out from under it.
    """
    cutoff = time.time() - _STALE_AFTER_S
    active = {job.path for job in _EXPORTS.values() if job.state == "preparing"}
    for pattern in _TMP_GLOBS:
        for leftover in db_dir.glob(pattern):
            if leftover in active:
                continue
            try:
                if leftover.stat().st_mtime < cutoff:
                    leftover.unlink(missing_ok=True)
            except (FileNotFoundError, OSError):
                # A concurrent export/import may remove its own temp between
                # the glob and the stat/unlink; skip it rather than abort the
                # sweep (and the enclosing request).
                continue


def _sweep_registry() -> None:
    """Drop terminal registry entries older than the temp-file cutoff.

    Skips ``preparing`` entries: a VACUUM in flight owns its temp, so
    reaping it would race the snapshot. Terminal (``ready``/``error``)
    entries past the cutoff are abandoned exports — unlink any surviving
    temp and forget them so the in-memory map cannot grow unbounded.
    """
    cutoff = time.time() - _STALE_AFTER_S
    for export_id in list(_EXPORTS):
        job = _EXPORTS[export_id]
        if job.state == "preparing" or job.created_at >= cutoff:
            continue
        _unlink(job.path)
        del _EXPORTS[export_id]


def _unlink(path: Path) -> None:
    path.unlink(missing_ok=True)


def _compress(src: Path, dst: Path) -> None:
    """Gzip ``src`` into ``dst`` (single-member, level 6). Sync; run off-loop."""
    with src.open("rb") as fin, gzip.open(dst, "wb", compresslevel=6) as fout:
        shutil.copyfileobj(fin, fout, length=_DECOMP_CHUNK)


async def _prepare_export(export_id: str, tmp: Path) -> None:
    """Fire-and-forget snapshot: VACUUM INTO ``tmp``, then gzip to ``.db.gz``.

    Runs the sync VACUUM via the existing serialized read executor
    (``get_db().read`` -> ``asyncio.to_thread``), so the event loop is
    never blocked and the snapshot stays mutually exclusive with an import
    swap. The raw ``.db.tmp`` is then compressed off-loop and dropped, so
    the served artifact is the single-member ``.db.gz`` and ``/data`` never
    holds both for longer than one compress. Terminal state is written back
    into the registry; a failure becomes ``error`` (never a hung
    ``preparing``). Never re-raises a plain Exception (an unretrieved task
    exception would only warn); re-raises CancelledError after cleanup.

    Sweep invariant (do not "fix" this into a race): during compress,
    ``job.path`` is still the raw ``.db.tmp``, so ``_sweep_stale``'s
    ``active`` set protects the raw from unlink. The in-flight ``.db.gz`` is
    NOT in ``active``; it rides the 3600 s mtime cutoff instead, exactly as
    a slow VACUUM's temp does. Compress of a single-digit-MB DB is ``<<`` 1 h,
    so the cutoff cannot reap a live gz.
    """

    def _snapshot(conn: sqlite3.Connection) -> None:
        conn.execute("VACUUM INTO ?", (str(tmp),))

    try:
        await get_db().read(_snapshot)
    except asyncio.CancelledError:
        _unlink(tmp)
        # Mark terminal before the mandatory re-raise so no path leaves the
        # entry hung in `preparing` (the sweep skips `preparing` forever).
        job = _EXPORTS.get(export_id)
        if job is not None:
            job.state = "error"
            job.error = "cancelled"
        raise
    except Exception:
        logger.exception("export: snapshot failed")
        _unlink(tmp)
        job = _EXPORTS.get(export_id)
        if job is not None:
            job.state = "error"
            job.error = "snapshot failed"
        return
    gz = tmp.parent / f".wxverify-export-{export_id}.db.gz"
    # Compress off-loop. `job.*` mutations stay AFTER the to_thread returns
    # (on the loop), so the loop-only `_EXPORTS` invariant holds. Both temps
    # are cleaned on every failure path -- nothing is ever left `preparing`.
    try:
        await asyncio.to_thread(_compress, tmp, gz)
    except asyncio.CancelledError:
        _unlink(tmp)
        _unlink(gz)
        job = _EXPORTS.get(export_id)
        if job is not None:
            job.state = "error"
            job.error = "cancelled"
        raise
    except Exception:
        logger.exception("export: compress failed")
        _unlink(tmp)
        _unlink(gz)
        job = _EXPORTS.get(export_id)
        if job is not None:
            job.state = "error"
            job.error = "compress failed"
        return
    # Raw snapshot is now redundant -- drop it immediately to bound /data.
    _unlink(tmp)
    job = _EXPORTS.get(export_id)
    if job is None:
        # Entry dropped (swept) mid-prepare -- don't leak the gz (the raw
        # tmp is already unlinked at this point).
        _unlink(gz)
        return
    # The terminal size read is inside failure handling: if the gz vanished
    # between compress and stat (e.g. an over-long window let a sweep reap
    # it), surface `error` -- never a hung `preparing`.
    try:
        size = gz.stat().st_size
    except OSError:
        logger.exception("export: snapshot gz missing after compress")
        _unlink(gz)
        job.state = "error"
        job.error = "snapshot failed"
        return
    job.path = gz
    job.state = "ready"
    job.size = size


async def _finish_download(export_id: str) -> None:
    """Post-send cleanup: forget the entry and unlink its temp.

    Async so the registry mutation runs on the event loop, not a
    threadpool thread — preserving the loop-only ``_EXPORTS`` invariant.
    """
    job = _EXPORTS.pop(export_id, None)
    if job is not None:
        _unlink(job.path)


@router.post("/export/begin")
async def export_begin() -> JSONResponse:
    """Start a fire-and-forget snapshot; return its id immediately.

    Sweeps terminal registry entries first, then orphaned temps (the glob
    sweep skips any temp a `preparing` entry still owns), then kicks off the
    VACUUM off the event loop. CSRF/same-origin are enforced upstream by
    MutationGuard (POST); this route carries no body.
    """
    db_dir = Path(config.db_path).parent
    _sweep_registry()
    _sweep_stale(db_dir)
    export_id = uuid.uuid4().hex
    tmp = db_dir / f".wxverify-export-{export_id}.db.tmp"
    job = _ExportJob(state="preparing", path=tmp, created_at=time.time())
    _EXPORTS[export_id] = job
    job.task = asyncio.create_task(_prepare_export(export_id, tmp))
    return JSONResponse({"export_id": export_id}, status_code=202)


@router.get("/export/status/{export_id}")
async def export_status(export_id: str) -> dict[str, str | int]:
    """Report a snapshot's state; include byte size once ready."""
    job = _EXPORTS.get(export_id)
    if job is None:
        raise ApiError(404, "unknown export id")
    if job.state == "ready" and job.size is not None:
        return {"state": "ready", "size": job.size}
    return {"state": job.state}


@router.get("/export/download/{export_id}")
async def export_download(export_id: str) -> FileResponse:
    """Stream the prebuilt snapshot; headers emit at once.

    The file path comes from the registry entry, never from the URL id
    (no path traversal). A background task forgets the entry and unlinks
    the temp after send.
    """
    job = _EXPORTS.get(export_id)
    if job is None:
        raise ApiError(404, "unknown export id")
    if job.state == "preparing":
        raise ApiError(409, "export still preparing")
    if job.state == "error":
        raise ApiError(409, job.error or "export failed")
    if not job.path.exists():
        _EXPORTS.pop(export_id, None)
        raise ApiError(409, "export expired")
    return FileResponse(
        job.path,
        media_type="application/gzip",
        filename=f"wxverify-{utc_now():%Y%m%d-%H%M%S}Z.db.gz",
        background=BackgroundTask(_finish_download, export_id),
    )


@router.post("/import/db")
async def import_db(request: Request) -> JSONResponse:
    """Replace the live database with an uploaded export (full overwrite)."""
    declared = int(request.headers.get("content-length", "0") or "0")
    if declared > _MAX_IMPORT_BYTES:
        raise ApiError(413, "file too large")
    db_dir = Path(config.db_path).parent
    tmp = db_dir / f".wxverify-import-{uuid.uuid4().hex}.db.tmp"
    try:
        received = await _stream_to(request, tmp)
        if received == 0:
            raise ApiError(422, "empty upload")
        _validate_upload(tmp)
        backup = db_dir / f"wxverify-{utc_now():%Y%m%d-%H%M%S}Z.db.bak"
        # COMMIT POINT: past a successful replace_from the live DB has been
        # overwritten, so the success response must go out regardless of any
        # downstream outcome — reclaim and rebuild run post-response.
        await get_db().replace_from(tmp, backup)
    finally:
        _unlink(tmp)
    return JSONResponse(
        {"status": "imported", "backup": backup.name, "rebuild": "started"},
        background=BackgroundTask(_rebuild_derived),
    )


async def _stream_to(request: Request, tmp: Path) -> int:
    """Stream the request body into ``tmp``, auto-inflating a gzip upload.

    The first two bytes are sniffed for the gzip magic (``1f 8b``). A gzip
    body is inflated with a bounded per-call output cap (the zip-bomb guard);
    a raw body is a byte-for-byte passthrough. Either way the returned count
    is the number of bytes WRITTEN to ``tmp`` (the decompressed size for a
    gzip upload), and the cap is enforced on that count -- the content-length
    header bounds only the compressed upload; the counter cannot lie.

    ``_compress`` emits a single-member gzip. A concatenated multi-member
    ``.gz`` inflates only its FIRST member (``zlib.decompressobj`` leaves the
    rest in ``unused_data``, never written), so such an upload imports its
    first member only -- trailing members are silently discarded, not
    rejected. The security property that holds: only the first, fully
    integrity-checked member ever becomes the DB (no validation bypass).
    """
    written = 0
    handle = await asyncio.to_thread(tmp.open, "wb")
    # `decomp is not None` is the gzip indicator once sniffing has decided.
    decomp = None
    decided = False
    prefix = b""
    try:
        async for chunk in request.stream():
            if not chunk:
                continue
            if not decided:
                prefix += chunk
                if len(prefix) < 2:
                    # Cannot branch on <2 bytes: buffer and wait. Writing the
                    # prefix raw now would corrupt a gzip upload whose first
                    # chunk is a single byte.
                    continue
                decided = True
                chunk = prefix
                prefix = b""
                if chunk[:2] == b"\x1f\x8b":
                    # wbits=31 selects the gzip (not zlib/raw) container.
                    decomp = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
            if decomp is not None:
                try:
                    data = chunk
                    while data:
                        # max_length caps output <=1 MiB/call, so the 413
                        # fires the instant cumulative output would cross.
                        out = decomp.decompress(data, _DECOMP_CHUNK)
                        written += len(out)
                        if written > _MAX_IMPORT_BYTES:
                            raise ApiError(413, "file too large")
                        if out:
                            await asyncio.to_thread(handle.write, out)
                        data = decomp.unconsumed_tail
                except zlib.error as exc:
                    raise ApiError(422, "not a valid gzip") from exc
            else:
                written += len(chunk)
                if written > _MAX_IMPORT_BYTES:
                    raise ApiError(413, "file too large")
                await asyncio.to_thread(handle.write, chunk)
        if decomp is not None:
            try:
                tail = decomp.flush()
            except zlib.error as exc:
                raise ApiError(422, "not a valid gzip") from exc
            written += len(tail)
            if written > _MAX_IMPORT_BYTES:
                raise ApiError(413, "file too large")
            if tail:
                await asyncio.to_thread(handle.write, tail)
            if not decomp.eof:
                # Stream ended without a complete gzip trailer: crisp
                # truncated-stream error rather than a later integrity fail.
                raise ApiError(422, "truncated gzip stream")
    finally:
        await asyncio.to_thread(handle.close)
    return written


def _validate_upload(tmp: Path) -> None:
    """Validate the upload via a read-only open, without touching the live DB."""
    try:
        conn = sqlite3.connect(f"file:{tmp}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        raise ApiError(422, "not a valid SQLite database") from exc
    try:
        try:
            row = conn.execute("PRAGMA integrity_check").fetchone()
        except sqlite3.DatabaseError as exc:
            raise ApiError(422, "not a valid SQLite database") from exc
        if row is None or str(row[0]) != "ok":
            raise ApiError(422, "database failed integrity check")
        version_row = conn.execute("PRAGMA user_version").fetchone()
        version = 0 if version_row is None else int(version_row[0])
        if version == 0:
            raise ApiError(422, "not a wxverify database")
        if version > TARGET_USER_VERSION:
            raise ApiError(422, "exported by a newer wxverify")
        names = {
            str(name_row[0])
            for name_row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        for table in _REQUIRED_TABLES:
            if table not in names:
                raise ApiError(422, f"missing required table: {table}")
    finally:
        conn.close()


async def _rebuild_derived() -> None:
    """Post-import background task: reclaim imported jobs, rebuild derived.

    Runs entirely post-response (nothing here can affect the already-sent
    200). The reclaim is error-isolated in its own try/except so a reclaim
    failure cannot abort the rebuild that follows.
    """
    db = get_db()
    # The jobs table arrives WITH the imported DB; running/pending rows in it
    # belong to the exporting process's past, and the boot-time reclaim will
    # not run again until the next restart.
    try:
        await db.write(reclaim_all_stale)
    except Exception:
        logger.exception("import: job reclaim failed")
    try:
        await db.write(_rebuild_all)
    except Exception:
        logger.exception("import: derived rebuild failed")


def _rebuild_all(conn: sqlite3.Connection) -> None:
    """Rebuild consensus, pairs, and scores from the imported station data.

    One write transaction, honoring the convergence invariant
    (worker/processor.py): observation-changing work runs the MONOLITHIC
    ``pair_and_score`` inline — never enqueued.
    """
    # From-scratch clear: per-cell dependent invalidation only reaches cells
    # present in station_observations or observations, so a forecast_pairs
    # row whose anchor observation is absent from BOTH tables would survive
    # stale. The unconditional delete guarantees none does — by construction.
    # score_cache needs no separate delete: the monolithic pair_and_score
    # clears the whole table itself.
    conn.execute("DELETE FROM forecast_pairs")
    # UNION of both tables: an observations row whose cell has no surviving
    # station rows is an orphan, and materializing its cell deletes it.
    # Per-cell calls are mandatory — they honor the load-bearing invalidation
    # contract in materialize_consensus.
    cells = conn.execute(
        """
        SELECT DISTINCT st.site_id AS site_id, so.variable AS variable,
               so.valid_at AS valid_at
        FROM station_observations so
        JOIN stations st ON st.id = so.station_id
        UNION
        SELECT site_id, variable, valid_at FROM observations
        """
    ).fetchall()
    for cell in cells:
        materialize_consensus(
            conn,
            site_id=int(cell["site_id"]),
            variable=str(cell["variable"]),
            valid_at=str(cell["valid_at"]),
        )
    pair_and_score(conn, site_id=None)
    set_runtime_state_now(conn, "import_rebuild_done_at")
