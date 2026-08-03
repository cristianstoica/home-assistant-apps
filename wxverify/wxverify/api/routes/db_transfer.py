"""DB transfer routes: export (VACUUM INTO snapshot) and import (overwrite)."""

from __future__ import annotations

import asyncio
import gzip
import logging
import re
import shutil
import sqlite3
import time
import uuid
import zlib
from dataclasses import dataclass
from datetime import datetime
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
# The lifespan sweeper's tick period. A retained export is reaped within one
# cutoff + one tick, so 300 s bounds a downloaded export to <= ~65 min on disk
# even if `/begin` is never called again.
_SWEEP_INTERVAL_S = 300.0
# 256 MiB: the live DB is single-digit MBs today, and /data must hold upload
# temp + backup + live DB simultaneously, so the cap bounds worst-case disk.
_MAX_IMPORT_BYTES = 256 * 1024 * 1024
# 1 MiB copy/inflate chunk: bounds the per-call decompress output (zip-bomb
# guard) and the compress copy buffer.
_DECOMP_CHUNK = 1 * 1024 * 1024
_REQUIRED_TABLES = ("sites", "stations", "station_observations")
# Every TEXT NOT NULL `variable` column in the schema. SQLite's type affinity
# lets a BLOB survive an insert into a TEXT column unconverted, so
# `PRAGMA integrity_check` (which validates storage-format consistency, not
# per-column type) accepts such a row silently; typeof()='blob' is the only
# check that catches it. Each table already carries an index covering
# `variable` (the UNIQUE/PRIMARY KEY constraint or an explicit CREATE INDEX),
# so this is an index scan rather than a table scan.
_BLOB_GUARDED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("station_observations", "variable"),
    ("observations", "variable"),
    ("forecast_samples", "variable"),
    ("forecast_pairs", "variable"),
    ("score_cache", "variable"),
)
# Anchored: only this producer's own output shapes are ever a sweep
# candidate. The token group is optional on purpose -- it spans BOTH
# `_new_backup_path`'s suffixed name and the pre-0.9 bare-timestamp name,
# so `.bak` files already sitting in /data from an earlier version stay
# sweepable across the upgrade.
_BAK_RE = re.compile(r"^wxverify-(\d{8}-\d{6})(?:-[0-9a-f]{8})?Z\.db\.bak$")
# The only await in import_db that can block on client behavior is the body
# read. 900 s bounds a full 256 MiB upload (_MAX_IMPORT_BYTES) at a ~291 KiB/s
# floor -- far below any LAN or ingress path -- so this can only fire on a
# genuinely stalled client, never on a slow-but-live one.
_IMPORT_STREAM_TIMEOUT_S = 900.0


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


def _looks_like_valid_sqlite(path: Path) -> bool:
    """Allowlist gate mirroring ``_validate_upload``'s shape (plan §3.2).

    ``quick_check`` alone is not enough: SQLite treats a zero-length file
    (the shape a SIGKILL mid-``VACUUM INTO`` leaves behind, since that call
    writes straight to the final filename with no temp-then-rename) as a
    valid, empty database. ``user_version != 0`` is the discriminator --
    ``VACUUM INTO`` preserves it, a truncated file has 0. Never raises into
    the sweep.
    """
    try:
        if path.stat().st_size <= 0:
            return False
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except (sqlite3.Error, OSError):
        return False
    try:
        try:
            row = conn.execute("PRAGMA quick_check").fetchone()
        except sqlite3.Error:
            return False
        if row is None or str(row[0]) != "ok":
            return False
        version_row = conn.execute("PRAGMA user_version").fetchone()
        if version_row is None or int(version_row[0]) == 0:
            return False
        names = {
            str(name_row[0])
            for name_row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        return all(table in names for table in _REQUIRED_TABLES)
    except (sqlite3.Error, OSError):
        return False
    finally:
        conn.close()


def _new_backup_path(db_dir: Path) -> Path:
    """Build a collision-proof `.bak` path: timestamp for humans/sort order,
    an 8-hex-char uuid4 suffix for uniqueness. The admission-control guard
    serializes imports but does not space them out in wall-clock time -- two
    sequential imports can complete and begin inside the same second, and
    the prior import's own backup is never deleted by its own sweep
    (`keep` excludes it), so a bare-timestamp name can collide with a
    still-live backup from the immediately preceding import.
    """
    token = uuid.uuid4().hex[:8]
    return db_dir / f"wxverify-{utc_now():%Y%m%d-%H%M%S}-{token}Z.db.bak"


def _unlink_bak(path: Path) -> None:
    """Delete one stale ``.bak`` candidate; log and continue on failure."""
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        logger.warning("bak sweep: failed to remove %s", path.name, exc_info=True)


def sweep_bak_files(db_dir: Path, *, keep: Path | None = None) -> None:
    """Keep exactly the newest ``.bak`` file, delete the rest (plan §3.2).

    ``keep``, when given, is the file just created by this import cycle and
    is unconditionally excluded from the delete candidate set and treated as
    the retained file regardless of what timestamp ordering would conclude.
    When ``keep`` is ``None`` (the startup sweep), the newest-by-embedded-
    timestamp candidate is retained instead. Either way the retained file is
    validated before anything is deleted: a corrupt/truncated "newest" file
    aborts the whole sweep rather than risk pruning every good backup down
    to a bad one.
    """
    candidates: list[tuple[str, Path]] = []
    for path in db_dir.glob("wxverify-*.db.bak"):
        if keep is not None and path == keep:
            continue
        match = _BAK_RE.fullmatch(path.name)
        if match is None:
            continue
        try:
            datetime.strptime(match.group(1), "%Y%m%d-%H%M%S")
        except ValueError:
            continue
        candidates.append((match.group(1), path))
    if not candidates:
        return
    candidates.sort(key=lambda item: (item[0], item[1].name), reverse=True)
    if keep is None:
        newest, stale = candidates[0][1], [path for _, path in candidates[1:]]
        if not stale:
            return
    else:
        newest, stale = keep, [path for _, path in candidates]
    if not _looks_like_valid_sqlite(newest):
        logger.warning(
            "bak sweep: newest backup %s failed validity check; skipping sweep",
            newest.name,
        )
        return
    for path in stale:
        _unlink_bak(path)


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


async def run_export_sweeper() -> None:
    """Periodically reap stale export registry entries and their temps.

    Retain-and-resume moves ALL export cleanup here. A downloaded export is
    deliberately NOT deleted post-send (so a repeat GET or a Firefox ``Range:``
    resume can complete against the same stable file), and the on-``/begin``
    sweep alone would leak a single downloaded-then-never-exported-again entry
    forever. This loop guarantees a retained ``ready`` export is TTL-reaped
    even when no further ``/begin`` ever arrives.

    Runs as a lifespan task on the API event loop, so the registry mutation in
    ``_sweep_registry`` stays on the single loop that owns ``_EXPORTS`` — the
    loop-only invariant is preserved (no threadpool registry mutation).
    """
    db_dir = Path(config.db_path).parent
    while True:
        await asyncio.sleep(_SWEEP_INTERVAL_S)
        try:
            _sweep_registry()
            _sweep_stale(db_dir)
        except Exception:
            logger.exception("export: periodic sweep failed")


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
    (no path traversal). The prepared artifact is RETAINED after send —
    it is NOT deleted post-download. This is the retain-and-resume fix:
    uvicorn silently swallows body sends after a client disconnect, so an
    aborted download still runs Starlette to EOF; deleting on that path
    (the old one-shot ``BackgroundTask``) removed the gz before Firefox's
    auto-retry could issue its ``Range:`` resume to the same URL, 404-ing
    the resume. Serving the identical file every time keeps the file's
    ETag/Last-Modified (``st_mtime``/``st_size`` derived) stable, so an
    ``If-Range`` resume validates and returns 206 instead of restarting.
    Cleanup is entirely the TTL sweep's job (``run_export_sweeper`` /
    on-``/begin`` ``_sweep_registry``) plus supersession by a later
    ``/begin``. Starlette's ``FileResponse`` implements Range/206/
    Accept-Ranges natively (0.46.2), so no Range code is added here.
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
    )


_import_in_progress = False


def _acquire_import_guard() -> None:
    """Reject a second import while one is in flight (plan: rare, destructive,
    operator-driven action -> reject, don't queue)."""
    global _import_in_progress
    if _import_in_progress:
        raise ApiError(409, "an import is already in progress")
    _import_in_progress = True


def _release_import_guard() -> None:
    global _import_in_progress
    _import_in_progress = False


@router.post("/import/db")
async def import_db(request: Request) -> JSONResponse:
    """Replace the live database with an uploaded export (full overwrite).

    Admission-controlled: only one import may be in flight, where "in
    flight" spans upload/validation/replace AND the post-response
    background rebuild + backup sweep (`_rebuild_derived`). A second
    request arriving anywhere in that window gets 409, not queued -- an
    import is a rare, destructive, operator-driven action.
    """
    _acquire_import_guard()
    try:
        declared = int(request.headers.get("content-length", "0") or "0")
        if declared > _MAX_IMPORT_BYTES:
            raise ApiError(413, "file too large")
        db_dir = Path(config.db_path).parent
        tmp = db_dir / f".wxverify-import-{uuid.uuid4().hex}.db.tmp"
        try:
            try:
                # A half-open client never delivers http.disconnect, so an
                # unbounded body read would hold the guard forever and 409
                # every later import until restart.
                async with asyncio.timeout(_IMPORT_STREAM_TIMEOUT_S):
                    received = await _stream_to(request, tmp)
            except TimeoutError as exc:
                raise ApiError(408, "upload stalled") from exc
            if received == 0:
                raise ApiError(422, "empty upload")
            _validate_upload(tmp)
            backup = _new_backup_path(db_dir)
            # COMMIT POINT: past a successful replace_from the live DB has
            # been overwritten, so the success response must go out
            # regardless of any downstream outcome -- reclaim and rebuild
            # run post-response. The guard, unlike the response, is held
            # across that post-response work (see _rebuild_derived_and_release).
            await get_db().replace_from(tmp, backup)
        finally:
            _unlink(tmp)
    except BaseException:
        # Every failure/cancellation exit releases the guard here, in the
        # same coroutine, before propagating -- 413/422/408 stall/replace
        # failure/CancelledError all funnel through this one clause. Note
        # the acquire is OUTSIDE this try: a 409 for a REJECTED request must
        # never reach here and clear the holder's guard.
        _release_import_guard()
        raise
    return JSONResponse(
        {"status": "imported", "backup": backup.name, "rebuild": "started"},
        background=BackgroundTask(_rebuild_derived_and_release, backup),
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
        try:
            fk_violation = conn.execute("PRAGMA foreign_key_check").fetchone()
        except sqlite3.DatabaseError as exc:
            raise ApiError(422, "not a valid SQLite database") from exc
        if fk_violation is not None:
            raise ApiError(422, "database failed foreign key check")
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
        for table, column in _BLOB_GUARDED_COLUMNS:
            if table not in names:
                continue
            try:
                blob_row = conn.execute(
                    f"SELECT 1 FROM {table} WHERE typeof({column}) = 'blob' LIMIT 1"
                ).fetchone()
            except sqlite3.DatabaseError as exc:
                raise ApiError(422, "not a valid SQLite database") from exc
            if blob_row is not None:
                raise ApiError(422, f"invalid data in {table}.{column}")
    finally:
        conn.close()


async def _rebuild_derived(backup: Path) -> None:
    """Post-import background task: reclaim imported jobs, rebuild derived.

    Runs entirely post-response (nothing here can affect the already-sent
    200). The reclaim is error-isolated in its own try/except so a reclaim
    failure cannot abort the rebuild that follows. ``backup`` is the ``.bak``
    file this import cycle just created; it is passed through to the
    retention sweep as ``keep`` so it survives regardless of ordering.
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
    try:
        await asyncio.to_thread(sweep_bak_files, backup.parent, keep=backup)
    except Exception:
        logger.exception("import: bak retention sweep failed")


async def _rebuild_derived_and_release(backup: Path) -> None:
    """Run the post-import background work, then release the import guard.

    `_rebuild_derived` itself never raises (every phase inside it is already
    error-isolated -- see its docstring), but the release is still in a
    `finally` so a future change to that function, or an exception raised
    by Starlette's own background-task machinery, cannot leak the guard.
    """
    try:
        await _rebuild_derived(backup)
    finally:
        _release_import_guard()


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
