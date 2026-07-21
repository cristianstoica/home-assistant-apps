"""DB transfer routes: export (VACUUM INTO snapshot) and import (overwrite)."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
import uuid
from pathlib import Path

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

_TMP_GLOBS = (".wxverify-export-*.db.tmp", ".wxverify-import-*.db.tmp")
_STALE_AFTER_S = 3600.0
# 256 MiB: the live DB is single-digit MBs today, and /data must hold upload
# temp + backup + live DB simultaneously, so the cap bounds worst-case disk.
_MAX_IMPORT_BYTES = 256 * 1024 * 1024
_REQUIRED_TABLES = ("sites", "stations", "station_observations")


def _sweep_stale(db_dir: Path) -> None:
    """Reclaim transfer temps orphaned by a crash or client disconnect."""
    cutoff = time.time() - _STALE_AFTER_S
    for pattern in _TMP_GLOBS:
        for leftover in db_dir.glob(pattern):
            try:
                if leftover.stat().st_mtime < cutoff:
                    leftover.unlink(missing_ok=True)
            except (FileNotFoundError, OSError):
                # A concurrent export/import may remove its own temp between
                # the glob and the stat/unlink; skip it rather than abort the
                # sweep (and the enclosing request).
                continue


def _unlink(path: Path) -> None:
    path.unlink(missing_ok=True)


@router.get("/export/db")
async def export_db() -> FileResponse:
    """Stream a consistent snapshot of the configured SQLite database."""
    db_dir = Path(config.db_path).parent
    _sweep_stale(db_dir)
    tmp = db_dir / f".wxverify-export-{uuid.uuid4().hex}.db.tmp"

    def _snapshot(conn: sqlite3.Connection) -> None:
        conn.execute("VACUUM INTO ?", (str(tmp),))

    try:
        await get_db().read(_snapshot)
    except BaseException:
        _unlink(tmp)
        raise
    return FileResponse(
        tmp,
        media_type="application/octet-stream",
        filename=f"wxverify-{utc_now():%Y%m%d-%H%M%S}Z.db",
        background=BackgroundTask(_unlink, tmp),
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
    """Stream the raw request body into ``tmp``; return the byte count.

    The cap is enforced on the counted bytes (the header can lie; the
    counter cannot).
    """
    received = 0
    handle = await asyncio.to_thread(tmp.open, "wb")
    try:
        async for chunk in request.stream():
            if not chunk:
                continue
            received += len(chunk)
            if received > _MAX_IMPORT_BYTES:
                raise ApiError(413, "file too large")
            await asyncio.to_thread(handle.write, chunk)
    finally:
        await asyncio.to_thread(handle.close)
    return received


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
