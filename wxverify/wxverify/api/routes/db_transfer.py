"""DB transfer routes: export (VACUUM INTO snapshot)."""

from __future__ import annotations

import asyncio
import gzip
import logging
import shutil
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from fastapi import APIRouter
from fastapi.responses import FileResponse
from starlette.responses import JSONResponse

from wxverify import config
from wxverify.api.errors import ApiError
from wxverify.core.error_sanitize import safe_detail
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["export"])

_TMP_GLOBS = (
    ".wxverify-export-*.db.tmp",
    # An upload temp that an earlier version's interrupted database import
    # left behind (storage-release plan 5.5).
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
# 1 MiB copy chunk: the compress copy buffer.
_DECOMP_CHUNK = 1 * 1024 * 1024


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
                # A concurrent export may remove its own temp between
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
    never blocked. The raw ``.db.tmp`` is then compressed off-loop and dropped, so
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


@dataclass(frozen=True)
class SweeperDeath:
    """The latched fact that ``run_export_sweeper`` stopped running.

    ``detail`` is a rendered, redacted line for the ``crashed`` case and the
    empty string for ``returned``; the caller composes the operator-facing
    sentence from ``reason``, ``at`` and this field.
    """

    at: str
    reason: Literal["crashed", "returned"]
    detail: str


# Lower case marks this as `global`-rebound module state, the idiom `_EXPORTS`
# above is process-global for the same reason: the sweeper's death is a fact
# about THIS process, and only a restart -- which is also the only thing that
# fixes it -- clears it.
_sweeper_death: SweeperDeath | None = None


def on_export_sweeper_done(task: asyncio.Task[None]) -> None:
    """Latch and report the sweeper's death; a shutdown cancellation is not one.

    Registered at the creation site in the lifespan, mirroring the worker's
    create -> append -> ``add_done_callback`` order, so task ownership stays
    readable in the one place it is reasoned about.

    The ``cancelled()`` check FIRST is the whole false-positive defence: every
    clean shutdown cancels this task, so a callback without it would latch a
    death and alert the operator on every restart. It is also what keeps the
    callback total -- ``Task.exception()`` RAISES ``CancelledError`` on a
    cancelled task, and a raise here reaches only the loop's exception handler.

    Never raises: the only rendering it does is ``safe_detail``, which cannot.
    Loud but not fatal -- the process keeps serving and the sweeper is never
    restarted; see the plan's D3 for why a disk-hygiene timer does not earn
    the worker's ``os._exit(1)``.
    """
    global _sweeper_death
    if task.cancelled():
        return
    exc = task.exception()
    if exc is None:
        logger.critical("export sweeper returned unexpectedly (no exception)")
        _sweeper_death = SweeperDeath(
            at=isoformat_utc(utc_now()), reason="returned", detail=""
        )
        return
    logger.critical(
        "export sweeper crashed",
        exc_info=(type(exc), exc, exc.__traceback__),
    )
    _sweeper_death = SweeperDeath(
        at=isoformat_utc(utc_now()), reason="crashed", detail=safe_detail(exc)
    )


def export_sweeper_death() -> SweeperDeath | None:
    """The latched sweeper death, or ``None`` while it is still running."""
    return _sweeper_death


def reset_export_sweeper_death() -> None:
    """Clear the latch. Test seam only -- production never un-latches.

    Wired into an autouse fixture because two graceful-shutdown tests crash a
    REAL lifespan's sweeper; without this the latch leaks into unrelated
    modules as an unattributable ``overall == "critical"``.
    """
    global _sweeper_death
    _sweeper_death = None


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
