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
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask
from starlette.responses import JSONResponse

from wxverify import config
from wxverify.api.errors import ApiError
from wxverify.core.error_sanitize import safe_detail, sanitized_exception
from wxverify.core.timeutil import (
    is_canonical_utc_stamp,
    isoformat_utc,
    isoformat_utc_micro,
    parse_utc,
    utc_now,
)
from wxverify.db.connection import get_db
from wxverify.db.migrations import TARGET_USER_VERSION, schema_table_names
from wxverify.db.queue import reclaim_all_stale
from wxverify.db.runtime_state import (
    delete_runtime_state,
    ensure_runtime_state_table,
    set_runtime_state,
    set_runtime_state_now,
)
from wxverify.db.sanitize import sanitize_wedge_prone_timestamps, table_exists
from wxverify.db.site_days import (
    WriteKind,
    daily_close_on,
    filter_day_writes,
    writable_pairs_clause,
)
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
# 256 MiB, bounding worst-case /data use (upload temp + backup + live DB at
# once). Governs four refusals: the content-length check in import_db, and the
# three written-bytes checks in _stream_to -- after each gzip inflate call, on
# the raw passthrough branch, and after the final inflate flush.
MAX_IMPORT_BYTES = 256 * 1024 * 1024
MAX_IMPORT_MIB = MAX_IMPORT_BYTES // (1024 * 1024)
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
    # The daily-close scores and publications (P4); each `variable` is in
    # its table's primary key.
    ("forecast_value_changes", "variable"),
    ("close_pairs", "variable"),
    ("day_scores", "variable"),
    ("starting_scores", "variable"),
    ("publication_rankings", "variable"),
    ("publication_coverage", "variable"),
)
# The forecast identity stamps, checked by VALUE at import. Downstream they
# are string keys (the UNIQUE constraint, the pairing join, the latest-run
# partition), so a second spelling of one instant splits a forecast hour in
# two, and a stamp that does not parse makes pairing raise. The read-side
# `FORECAST_TIMESTAMP_LIKE` filter is only a shape filter (LIKE is
# case-insensitive and `_` matches any character), so it catches neither.
# The observation stamps are keys in the same way, and the post-import
# rebuild parses every one with no catch, so one bad stamp fails the whole
# rebuild.
_CANONICAL_STAMP_COLUMNS: tuple[tuple[str, str], ...] = (
    ("forecast_samples", "issued_at"),
    ("forecast_samples", "valid_at"),
    ("station_observations", "valid_at"),
    ("observations", "valid_at"),
    ("station_wind_records", "obs_at"),
    # A day record's bounds: the guard parses them and `open_day_clause`
    # compares them with julianday, so one bad stamp breaks or hides a day.
    ("site_days", "day_start_utc"),
    ("site_days", "day_end_utc"),
    # The daily-close scores and publications (P4): a download's order, a
    # pair's hour, and the instants a close and a publication were made,
    # which readers compare as text. The four `fetched_at` columns are not
    # listed: they copy `persist_fetch_result`'s stamp, which keeps its
    # microseconds, as `forecast_samples.fetched_at` does.
    ("forecast_downloads", "started_at"),
    ("forecast_downloads", "ended_at"),
    ("close_pairs", "valid_at"),
    ("close_records", "scored_at"),
    ("publications", "build_started_at"),
    ("publications", "published_at"),
)
# Nullable stamps of the same tables, checked apart because the check above
# refuses NULL.
_NULLABLE_STAMP_COLUMNS: tuple[tuple[str, str], ...] = (
    ("forecast_value_changes", "first_changed_valid_at"),
    ("close_pairs", "issued_at"),
)
# The daily-close tables' dates, checked by VALUE at import: each is a key
# (a day record, a station-list or zone change's first day) compared as text,
# so only the canonical `YYYY-MM-DD` spelling may come in.
_CANONICAL_DATE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("site_days", "local_date"),
    ("station_changes", "effective_date"),
    ("site_zone_changes", "effective_date"),
    # The close's two logs. Its three records are not listed: each has a
    # foreign key to `site_days`, so the foreign key check refuses their dates.
    ("close_stage_log", "local_date"),
    ("close_try_log", "local_date"),
    # The daily-close scores and publications (P4). None has a foreign key
    # to `site_days`, so every date of theirs is listed here or, when it may
    # be NULL, in `_NULLABLE_DATE_COLUMNS`.
    ("forecast_downloads", "local_date"),
    ("close_pairs", "local_date"),
    ("day_scores", "local_date"),
    ("close_records", "local_date"),
    ("close_record_downloads", "local_date"),
    ("starting_scores", "local_date"),
    ("starting_wind_days", "local_date"),
    ("publications", "cutoff"),
    ("publications", "first_record_date"),
    ("publications", "window_start_date"),
    ("publications", "wind_start_date"),
)
# Nullable dates of the same tables, checked apart because the check above
# refuses NULL.
_NULLABLE_DATE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("forecast_downloads", "round_date"),
    ("close_pairs", "download_date"),
    ("day_scores", "download_date"),
    ("close_record_downloads", "download_date"),
)
# The daily-close tables' change stamps, which the station-list and zone
# rules order as text, so only the fixed-width microsecond form may come in.
_MICRO_STAMP_COLUMNS: tuple[tuple[str, str], ...] = (
    ("station_changes", "changed_at"),
    ("site_zone_changes", "entered_at"),
)
# What `_flag_invalid_utf8` returns in place of text that is not valid UTF-8.
# A bare object equals no value a row can hold, so a row test finds only it.
_INVALID_UTF8 = object()
# A column name read from the upload is echoed in a refusal only when it
# fully matches this; any other name is shown by its position instead.
_SAFE_COLUMN_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}")
# Anchored: only this producer's own output shapes are ever a sweep
# candidate. The token group is optional on purpose -- it spans BOTH
# `_new_backup_path`'s suffixed name and the pre-0.9 bare-timestamp name,
# so `.bak` files already sitting in /data from an earlier version stay
# sweepable across the upgrade.
_BAK_RE = re.compile(r"^wxverify-(\d{8}-\d{6})(?:-[0-9a-f]{8})?Z\.db\.bak$")
# The only await in import_db that can block on client behavior is the body
# read. 900 s bounds a full 256 MiB upload (MAX_IMPORT_BYTES) at a ~291 KiB/s
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
    """Allowlist gate mirroring ``_validate_upload``'s shape.

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
    """Keep exactly the newest ``.bak`` file, delete the rest.

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


_import_in_progress = False


def _acquire_import_guard() -> None:
    """Reject a second import while one is in flight (rare, destructive,
    operator-driven action -> reject, don't queue)."""
    global _import_in_progress
    if _import_in_progress:
        raise ApiError(409, "an import is already in progress")
    _import_in_progress = True


def _release_import_guard() -> None:
    global _import_in_progress
    _import_in_progress = False


# The PRAGMA table_list, which the table guard relies on, arrived in 3.37.0.
_IMPORT_MIN_SQLITE: tuple[int, int, int] = (3, 37, 0)


def _require_import_sqlite() -> None:
    """Refuse the import when this SQLite cannot classify tables.

    The table guard asks SQLite's own PRAGMA ``table_list`` whether a name is
    an ordinary table, and that PRAGMA arrived in SQLite 3.37.0, while the
    add-on itself starts on 3.35. The version is read from the ``sqlite3``
    module on every call, never copied at import, so the refusal follows the
    library actually loaded.

    ``import_db`` calls this first, before the app reads, validates, stages
    or swaps in the upload, so a refusal leaves the database untouched.
    """
    running = sqlite3.sqlite_version_info
    if running < _IMPORT_MIN_SQLITE:
        found = ".".join(str(part) for part in running)
        raise ApiError(
            501,
            "database import needs SQLite 3.37.0 or newer; "
            f"this add-on is running SQLite {found}",
        )


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
        _require_import_sqlite()
        declared = int(request.headers.get("content-length", "0") or "0")
        if declared > MAX_IMPORT_BYTES:
            raise ApiError(
                413,
                "file too large: the upload exceeds the "
                f"{MAX_IMPORT_MIB} MiB import limit",
            )
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
            _stage_pending_rebuild_state(tmp)
            _settle_imported_closes(tmp)
            backup = _new_backup_path(db_dir)
            # COMMIT POINT: past a successful replace_from the live DB has
            # been overwritten, so the success response must go out
            # regardless of any downstream outcome -- reclaim and rebuild
            # run post-response. The guard, unlike the response, is held
            # across that post-response work (see _rebuild_derived_and_release).
            # Nothing may write runtime_state after this point: a write here
            # would be caught by `except BaseException` below and reported as
            # a 500 for an import that already committed. That is why the
            # `pending` stamp above happens on the STAGED file, before the
            # swap, not here -- any later rebuild outcome is only ever
            # logged and recorded through run_rebuild_all, never turned into
            # a failed response for a request that has already succeeded.
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
                        if written > MAX_IMPORT_BYTES:
                            raise ApiError(
                                413,
                                "file too large: the database exceeds the "
                                f"{MAX_IMPORT_MIB} MiB import limit, measured "
                                "after any decompression",
                            )
                        if out:
                            await asyncio.to_thread(handle.write, out)
                        data = decomp.unconsumed_tail
                except zlib.error as exc:
                    raise ApiError(422, "not a valid gzip") from exc
            else:
                written += len(chunk)
                if written > MAX_IMPORT_BYTES:
                    raise ApiError(
                        413,
                        "file too large: the database exceeds the "
                        f"{MAX_IMPORT_MIB} MiB import limit, measured "
                        "after any decompression",
                    )
                await asyncio.to_thread(handle.write, chunk)
        if decomp is not None:
            try:
                tail = decomp.flush()
            except zlib.error as exc:
                raise ApiError(422, "not a valid gzip") from exc
            written += len(tail)
            if written > MAX_IMPORT_BYTES:
                raise ApiError(
                    413,
                    "file too large: the database exceeds the "
                    f"{MAX_IMPORT_MIB} MiB import limit, measured "
                    "after any decompression",
                )
            if tail:
                await asyncio.to_thread(handle.write, tail)
            if not decomp.eof:
                # Stream ended without a complete gzip trailer: crisp
                # truncated-stream error rather than a later integrity fail.
                raise ApiError(422, "truncated gzip stream")
    finally:
        await asyncio.to_thread(handle.close)
    return written


def _guarded_table_present(conn: sqlite3.Connection, table: str) -> bool:
    """Whether guarded ``table`` exists, matched the way SQLite resolves names.

    SQLite matches identifiers ASCII-case-insensitively, so the code's
    lowercase name reaches a table stored as `Forecast_Samples`, which an
    exact Python test against `sqlite_master` names would skip.
    `COLLATE NOCASE` folds ASCII only, the same rule. Tables, views and
    indexes share one namespace, so the lookup covers all three: no row
    means genuinely absent (skip it; `create_tables` recreates it after the
    swap), exactly one `table` row means validate it, and anything else is
    refused. A same-named view or index read as absent would pass admission
    and then break the schema step at boot. ``table`` is always a code-owned
    name, bound as a parameter.

    A `table` row alone does not prove an ordinary table: SQLite records a
    virtual table, and each of its shadow tables, as `table` in
    `sqlite_master`. So the kind is confirmed with SQLite's own
    classification, the PRAGMA `table_list`, and only an object both
    catalogues call a plain `table` passes. It runs as a statement,
    `PRAGMA main.table_list(...)`, on purpose. The table-valued form,
    `pragma_table_list`, is looked up among the file's own tables first, so
    an object in the upload with that name, in any letter case, can take its
    place and answer with rows of its choosing. SQLite never looks a PRAGMA
    statement's name up among the file's objects, so nothing in the file can
    stand in for it. A PRAGMA argument cannot be bound, so, unlike the
    lookup above, the code-owned name goes in as a string literal with any
    `'` doubled, and the kind is read from the `type` column by name. The
    `sqlite_master` lookup stays because the PRAGMA does not list indexes.
    `table_list` needs SQLite 3.37: the import route refuses an older
    runtime before this runs, and on an older SQLite, which ignores the
    unknown PRAGMA and returns no `type` column, the file is still refused
    as an invalid database.
    """
    try:
        types = [
            str(type_row[0])
            for type_row in conn.execute(
                "SELECT type FROM sqlite_master WHERE name = ? COLLATE NOCASE"
                " AND type IN ('table', 'view', 'index')",
                (table,),
            )
        ]
        if not types:
            return False
        literal = "'" + table.replace("'", "''") + "'"
        cursor = conn.execute(f"PRAGMA main.table_list({literal})")
        columns = [column[0] for column in cursor.description or ()]
        if "type" not in columns:
            # Routed to the same invalid-database refusal as any other error.
            raise sqlite3.DatabaseError("PRAGMA table_list returned no type")
        type_at = columns.index("type")
        kinds = [str(kind_row[type_at]) for kind_row in cursor]
    except sqlite3.DatabaseError as exc:
        raise ApiError(422, "not a valid SQLite database") from exc
    if types == ["table"] and kinds == ["table"]:
        return True
    raise ApiError(422, f"not a table: {table}")


def _guarded_column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    """The lowercased column names of guarded ``table``; empty when absent.

    For a check of a column a later release added to an existing table: a
    file from before that release has the table without the column, and the
    open migrates it, so there is no value to check. The table is confirmed
    by ``_guarded_table_present`` first. The names come from the PRAGMA
    statement ``main.table_xinfo``, never the table-valued form, for the
    reason ``_guarded_table_present`` gives for ``table_list``; ``table_xinfo``
    also lists generated columns, which a ``SELECT`` reads like any other.
    SQLite matches column names ASCII-case-insensitively, so the names are
    lowercased. ``table`` is always a code-owned name.
    """
    if not _guarded_table_present(conn, table):
        return set()
    literal = "'" + table.replace("'", "''") + "'"
    try:
        cursor = conn.execute(f"PRAGMA main.table_xinfo({literal})")
        columns = [column[0] for column in cursor.description or ()]
        if "name" not in columns:
            # Routed to the same invalid-database refusal as any other error.
            raise sqlite3.DatabaseError("PRAGMA table_xinfo returned no name")
        name_at = columns.index("name")
        return {str(name_row[name_at]).lower() for name_row in cursor}
    except sqlite3.DatabaseError as exc:
        raise ApiError(422, "not a valid SQLite database") from exc


def _flag_invalid_utf8(raw: bytes) -> object:
    """A text factory that marks undecodable text instead of raising.

    It applies the same strict ``bytes.decode("utf-8")`` that the default
    ``str`` factory applies to the same bytes, so it flags exactly the
    values the app's own reads would fail on, in a UTF-8 or a UTF-16 file
    alike. It is called for TEXT values only, so a BLOB never reaches it.
    """
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return _INVALID_UTF8


def _refuse_invalid_text(conn: sqlite3.Connection, table: str, sql: str) -> None:
    """Refuse the upload if ``sql`` yields any text that is not valid UTF-8.

    The refusal names ``table``, always a code-owned name, and the column,
    taken from the cursor and shown only when it fully matches
    ``_SAFE_COLUMN_NAME``; any other name is shown as its 1-based position.
    The value itself is never shown. Decoding happens row by row as the
    cursor advances, so the ``try`` encloses the whole iteration: a read
    error at any row becomes the house invalid-database refusal, and only
    ``sqlite3.DatabaseError`` is caught, so the ``ApiError`` passes
    through. Rows are streamed, one at a time. The factory is looked up by
    name on every call, and ``str`` is restored for every later read on
    the connection.
    """
    conn.text_factory = _flag_invalid_utf8
    try:
        cursor = conn.execute(sql)
        for row in cursor:
            if _INVALID_UTF8 in row:
                index: int = row.index(_INVALID_UTF8)
                name = str(cursor.description[index][0])
                column = (
                    name
                    if _SAFE_COLUMN_NAME.fullmatch(name)
                    else f"<column {index + 1}>"
                )
                raise ApiError(
                    422, f"invalid text in {table}.{column}: expected valid UTF-8"
                )
    except sqlite3.DatabaseError as exc:
        raise ApiError(422, "not a valid SQLite database") from exc
    finally:
        conn.text_factory = str


def _is_null_or_canonical_stamp(value: object) -> bool:
    """Whether ``value`` is NULL or ``isoformat_utc``'s own UTC stamp."""
    return value is None or is_canonical_utc_stamp(value)


def _is_canonical_date(value: object) -> bool:
    """Whether ``value`` is a ``str`` in the canonical ``YYYY-MM-DD`` form."""
    if not isinstance(value, str):
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _is_null_or_canonical_date(value: object) -> bool:
    """Whether ``value`` is NULL or a canonical ``YYYY-MM-DD`` date."""
    return value is None or _is_canonical_date(value)


def _is_canonical_micro_stamp(value: object) -> bool:
    """Whether ``value`` is ``isoformat_utc_micro``'s own UTC stamp."""
    if not isinstance(value, str):
        return False
    try:
        return isoformat_utc_micro(parse_utc(value)) == value
    except ValueError:
        return False


def _refuse_bad_values(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    is_valid: Callable[[object], bool],
    message: str,
) -> None:
    """Refuse the upload with ``message`` if any ``table.column`` value fails.

    Skipped when ``table`` is absent (``_guarded_table_present``). Both names
    are code-owned. Distinct values are compared byte for byte, so every
    spelling is checked.
    """
    if not _guarded_table_present(conn, table):
        return
    try:
        bad = any(
            not is_valid(value_row[0])
            for value_row in conn.execute(
                f"SELECT DISTINCT {column} COLLATE BINARY FROM {table}"
            )
        )
    except sqlite3.DatabaseError as exc:
        raise ApiError(422, "not a valid SQLite database") from exc
    if bad:
        raise ApiError(422, message)


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
        # The catalogue comes before any read that decodes a name from it.
        # First its shape, as an allowlist of what SQLite itself writes: a
        # `type` spelled or stored any other way, or a name that is not TEXT,
        # lets a table SQLite still loads read as absent to
        # `_guarded_table_present`, and a `sql` that is neither TEXT nor NULL
        # carries column names the text scan never sees. Then its text: an
        # undecodable name or `sql` breaks the reads that follow, and an
        # undecodable column name breaks `SELECT *` with an error that is not
        # a `sqlite3` error.
        try:
            odd_entry = conn.execute(
                "SELECT 1 FROM sqlite_master"
                " WHERE typeof(type) != 'text'"
                " OR type NOT IN ('table', 'index', 'view', 'trigger')"
                " OR typeof(name) != 'text' OR typeof(tbl_name) != 'text'"
                " OR typeof(sql) NOT IN ('text', 'null')"
                " LIMIT 1"
            ).fetchone()
        except sqlite3.DatabaseError as exc:
            raise ApiError(422, "not a valid SQLite database") from exc
        if odd_entry is not None:
            raise ApiError(422, "invalid schema entry in sqlite_master")
        _refuse_invalid_text(
            conn, "sqlite_master", "SELECT type, name, tbl_name, sql FROM sqlite_master"
        )
        names = {
            str(name_row[0])
            for name_row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        for table in _REQUIRED_TABLES:
            if table not in names:
                raise ApiError(422, f"missing required table: {table}")
        # Absence is fine here: the result is dropped on purpose, and the call
        # is kept only for the refusals it raises.
        for table in schema_table_names():
            _guarded_table_present(conn, table)
        for table, column in _BLOB_GUARDED_COLUMNS:
            if not _guarded_table_present(conn, table):
                continue
            try:
                blob_row = conn.execute(
                    f"SELECT 1 FROM {table} WHERE typeof({column}) = 'blob' LIMIT 1"
                ).fetchone()
            except sqlite3.DatabaseError as exc:
                raise ApiError(422, "not a valid SQLite database") from exc
            if blob_row is not None:
                raise ApiError(422, f"invalid data in {table}.{column}")
        # Every column of every row of each app table present, after the BLOB
        # guard so its refusals keep their messages, and before the timestamp
        # check so an undecodable stamp gets this specific one.
        for table in schema_table_names():
            if _guarded_table_present(conn, table):
                _refuse_invalid_text(conn, table, f"SELECT * FROM {table}")
        for table, column in _CANONICAL_STAMP_COLUMNS:
            if not _guarded_table_present(conn, table):
                continue
            try:
                bad = any(
                    not is_canonical_utc_stamp(stamp_row[0])
                    for stamp_row in conn.execute(
                        f"SELECT DISTINCT {column} COLLATE BINARY FROM {table}"
                    )
                )
            except sqlite3.DatabaseError as exc:
                raise ApiError(422, "not a valid SQLite database") from exc
            if bad:
                raise ApiError(
                    422,
                    f"invalid timestamp in {table}.{column}: "
                    "expected UTC form YYYY-MM-DDTHH:MM:SSZ",
                )
        # The daily-close tables (10.6). `retry_at` is checked on its own
        # because the stamp check above refuses NULL, which it may hold.
        _refuse_bad_values(
            conn,
            "site_days",
            "retry_at",
            _is_null_or_canonical_stamp,
            "invalid timestamp in site_days.retry_at: "
            "expected UTC form YYYY-MM-DDTHH:MM:SSZ",
        )
        for table, column in _NULLABLE_STAMP_COLUMNS:
            _refuse_bad_values(
                conn,
                table,
                column,
                _is_null_or_canonical_stamp,
                f"invalid timestamp in {table}.{column}: "
                "expected UTC form YYYY-MM-DDTHH:MM:SSZ",
            )
        for table, column in _CANONICAL_DATE_COLUMNS:
            _refuse_bad_values(
                conn,
                table,
                column,
                _is_canonical_date,
                f"invalid date in {table}.{column}: expected YYYY-MM-DD",
            )
        for table, column in _NULLABLE_DATE_COLUMNS:
            _refuse_bad_values(
                conn,
                table,
                column,
                _is_null_or_canonical_date,
                f"invalid date in {table}.{column}: expected YYYY-MM-DD",
            )
        for table, column in _MICRO_STAMP_COLUMNS:
            _refuse_bad_values(
                conn,
                table,
                column,
                _is_canonical_micro_stamp,
                f"invalid timestamp in {table}.{column}: "
                "expected UTC form YYYY-MM-DDTHH:MM:SS.ffffffZ",
            )
        # The raw readings' two nullable columns, checked on their own because
        # the stamp check above refuses NULL. A file from before 0.16.14 has
        # the table without them, and the open rebuilds it, so each check is
        # skipped when its column is absent as well as when the table is.
        raw_columns = _guarded_column_names(conn, "station_wind_records")
        if "fetched_at" in raw_columns:
            _refuse_bad_values(
                conn,
                "station_wind_records",
                "fetched_at",
                _is_null_or_canonical_stamp,
                "invalid timestamp in station_wind_records.fetched_at: "
                "expected UTC form YYYY-MM-DDTHH:MM:SSZ",
            )
        if "local_date" in raw_columns:
            _refuse_bad_values(
                conn,
                "station_wind_records",
                "local_date",
                _is_null_or_canonical_date,
                "invalid date in station_wind_records.local_date: expected YYYY-MM-DD",
            )
    finally:
        conn.close()


def _stage_pending_rebuild_state(tmp: Path) -> None:
    """Mark the staged upload's rebuild state ``pending`` before promotion.

    ``replace_from``'s rename is atomic (db/connection.py), so writing this
    into the staged file -- never into the live DB after the swap -- makes
    the promoted database carry `pending` by construction: there is no
    window where the rename has committed and the state has not. Also drops
    any `import_rebuild_done_at`/`import_rebuild_error` rows the upload
    carries -- they describe the EXPORTING machine's own past rebuild, not
    this one, and nothing else clears them.

    `_validate_upload` only requires `_REQUIRED_TABLES` (sites, stations,
    station_observations) plus an in-range `user_version`; it never requires
    `runtime_state` (it only refuses one that is present but is not an
    ordinary table). A genuine prior export always has it -- this app's own
    `create_schema` creates it unconditionally, ahead of any version gate,
    on every boot of the exporting instance -- but `_validate_upload` cannot
    distinguish a genuine export from a file that was merely built to
    satisfy those same checks without ever passing through this app's
    `create_schema` (e.g. hand-assembled from the three required tables plus
    a plausible `user_version`). `ensure_runtime_state_table` guards that
    reachable gap. The checkpoint after commit is defensive: every export
    this app produces is VACUUM INTO output, already in rollback-journal
    mode, but a hand-copied WAL-mode upload may still carry a `-wal`
    sidecar; SQLite's own close-time checkpoint would flush it into the
    main file regardless, so this call just makes that flush explicit ahead
    of the rename.

    Also runs ``sanitize_wedge_prone_timestamps`` here for the same reason:
    on the staged file, before promotion, so the promoted database carries
    the fix-up by construction rather than racing a live write against it.
    """
    conn = sqlite3.connect(str(tmp))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            ensure_runtime_state_table(conn)
            set_runtime_state(conn, "import_rebuild_state", "pending")
            delete_runtime_state(conn, "import_rebuild_done_at", "import_rebuild_error")
            sanitize_wedge_prone_timestamps(conn)
        except BaseException:
            conn.rollback()
            raise
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()


_CLOSE_TABLES = (
    "close_attempts",
    "close_station_days",
    "close_publications",
    "close_stage_log",
    "close_try_log",
)


def _settle_imported_closes(tmp: Path) -> None:
    """Settle the staged upload's interrupted closes before promotion (12.6).

    Acts only when the staged file has the daily close's five tables and its
    close is on (``daily_close_on``); then ``settle_interrupted_closes`` runs
    with the import's time, so a ``closing`` day is failed as of the
    exporting add-on's last known run, its ``day_close`` jobs are completed
    without running and a ``building`` publication record is settled (plan
    5.5, 9.4). One ``BEGIN IMMEDIATE`` transaction on its own connection,
    rolled back and re-raised on any exception, so the import aborts before
    the swap and the live database is untouched; committed and checkpointed
    as ``_stage_pending_rebuild_state`` does. Runs after
    ``_stage_pending_rebuild_state``, which ensures ``runtime_state``.
    """
    from wxverify.worker.close_chain import settle_interrupted_closes

    conn = sqlite3.connect(str(tmp))
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            has_tables = all(table_exists(conn, table) for table in _CLOSE_TABLES)
            if has_tables and daily_close_on(conn):
                settle_interrupted_closes(conn, now_utc=utc_now())
        except BaseException:
            conn.rollback()
            raise
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
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
    await run_rebuild_all()
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


async def run_rebuild_all() -> None:
    """Run ``_rebuild_or_keep``, committing ``done`` or -- on failure --
    separately committing ``failed`` plus a sanitized reason.

    Shared by the post-import background task and the boot-time resume: a
    rebuild that raises must never leave ``import_rebuild_state`` hung at
    ``pending``, whichever caller ran it. ``_rebuild_all``'s own transaction
    rolls back on failure (so it never reaches its own ``done`` write); the
    failed transition is committed here, in a separate write, so it survives
    even though the attempt that triggered it did not.
    """
    db = get_db()
    try:
        await db.write(_rebuild_or_keep)
    except Exception as exc:
        logger.exception("import: derived rebuild failed")

        try:
            reason = sanitized_exception(exc)

            def _mark_failed(conn: sqlite3.Connection) -> None:
                set_runtime_state(conn, "import_rebuild_state", "failed")
                set_runtime_state(conn, "import_rebuild_error", reason)

            await db.write(_mark_failed)
        except Exception:
            # The state stays `pending`, so a later boot retries -- correct,
            # since the failure that broke the rebuild also broke recording
            # it -- whether that's `_mark_failed`'s write, or sanitizing
            # `exc` into `reason` beforehand. What must not happen is taking
            # the process down with it: this runs inside `lifespan` on the
            # boot-resume path.
            logger.exception("import: recording rebuild failure failed")


def _rebuild_or_keep(conn: sqlite3.Connection) -> None:
    """Rebuild a file whose daily close is on; keep one without the key.

    ``daily_close_on`` is read first (switch-over plan 11.2, R18). A file
    without the key, from before the daily close, is not rebuilt: its pairs,
    truth and scores are its starting history as they stand, so only
    ``import_rebuild_state`` = ``done`` is written, with one INFO line. A
    file with the key goes through ``_rebuild_all`` (P1 7.5).
    """
    if not daily_close_on(conn):
        set_runtime_state(conn, "import_rebuild_state", "done")
        logger.info(
            "import: file from before the daily close; its history is kept as it stands"
        )
        return
    _rebuild_all(conn)


def _rebuild_all(conn: sqlite3.Connection) -> None:
    """Rebuild consensus, pairs, and scores from the imported station data.

    One write transaction, honoring the convergence invariant
    (worker/processor.py): observation-changing work runs the MONOLITHIC
    ``pair_and_score`` inline — never enqueued.

    Called only while the imported file's daily close is on
    (``_rebuild_or_keep``): only days that still take writes are rebuilt
    (7.5); done days keep their pairs and consensus, and their cells are
    skipped, not refused.
    """
    now = utc_now()
    # From-scratch clear: per-cell dependent invalidation only reaches cells
    # present in station_observations or observations, so a forecast_pairs
    # row whose anchor observation is absent from BOTH tables would survive
    # stale. The delete guarantees none does — by construction — on every
    # day that takes pairs (writable_pairs_clause). score_cache needs no
    # separate delete: the monolithic pair_and_score clears the whole table
    # itself.
    conn.execute(
        "DELETE FROM forecast_pairs"
        f" WHERE {writable_pairs_clause(conn, None, now_utc=now)}"
    )
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
    instants_by_site: dict[int, set[str]] = {}
    for cell in cells:
        instants_by_site.setdefault(int(cell["site_id"]), set()).add(
            str(cell["valid_at"])
        )
    writable = {
        site_id: filter_day_writes(
            conn, site_id, sorted(instants), WriteKind.HOURLY, now_utc=now
        )
        for site_id, instants in instants_by_site.items()
    }
    kept = [
        cell
        for cell in cells
        if str(cell["valid_at"]) in writable[int(cell["site_id"])]
    ]
    logger.info(
        "import: consensus rebuild kept %d cells, skipped %d on days that"
        " take no writes",
        len(kept),
        len(cells) - len(kept),
    )
    for cell in kept:
        materialize_consensus(
            conn,
            site_id=int(cell["site_id"]),
            variable=str(cell["variable"]),
            valid_at=str(cell["valid_at"]),
            now_utc=now,
        )
    pair_and_score(conn, site_id=None)
    set_runtime_state_now(conn, "import_rebuild_done_at")
    set_runtime_state(conn, "import_rebuild_state", "done")
