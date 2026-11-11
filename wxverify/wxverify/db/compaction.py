"""The one-time compaction of the database file at startup.

``Database._open`` calls ``compact_storage`` on the writer after the
migrations and the sanitize, before any reader opens. It runs only while
``runtime_state`` lacks ``STORAGE_COMPACTED_KEY``: a fresh file is built with
the key, and an upgraded file gets it once a compaction has run.
"""

from __future__ import annotations

import contextlib
import logging
import os
import shutil
import sqlite3
import time
from collections.abc import Mapping
from typing import Final, Literal

from wxverify.core import stop
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.migrations import STORAGE_COMPACTED_KEY
from wxverify.db.runtime_state import set_runtime_state

logger = logging.getLogger(__name__)

# The temp folders SQLite's unix VFS tries after SQLITE_TMPDIR and TMPDIR, in
# its order (`azTempDirs` in os_unix.c).
_FIXED_TEMP_DIRS: Final[tuple[str, ...]] = ("/var/tmp", "/usr/tmp", "/tmp", ".")

# need = 2.2 x live, in tenths: VACUUM's temp copy of the live pages, the
# rebuilt file passing through the WAL, and 10% on top.
_NEED_TENTHS: Final = 22

_Pragma = Literal["page_size", "page_count", "freelist_count"]


def sqlite_temp_dir(environ: Mapping[str, str] | None = None) -> str:
    """The folder SQLite's unix VFS picks for temp files, VACUUM's copy included.

    ``SQLITE_TMPDIR``, then ``TMPDIR`` (read from ``environ``, by default
    ``os.environ``; unset or empty values are skipped), then each of
    ``_FIXED_TEMP_DIRS``: the first that is a directory with write and
    execute access, as SQLite's ``unixTempFileDir`` checks it. ``"."`` when
    none qualifies.

    SQLite reads the two variables once, when the library starts, so this
    matches its choice while they are unchanged since then. The run script
    sets ``SQLITE_TMPDIR`` before the app starts.
    """
    env: Mapping[str, str] = os.environ if environ is None else environ
    candidates = (
        env.get("SQLITE_TMPDIR", ""),
        env.get("TMPDIR", ""),
        *_FIXED_TEMP_DIRS,
    )
    for path in candidates:
        if path and os.path.isdir(path) and os.access(path, os.W_OK | os.X_OK):
            return path
    return "."


def _free_bytes(path: str) -> int:
    """Free bytes on the file system holding ``path``."""
    return shutil.disk_usage(path).free


def _vacuum(conn: sqlite3.Connection) -> None:
    """Run ``VACUUM`` on ``conn``."""
    conn.execute("VACUUM")


def _checkpoint(conn: sqlite3.Connection) -> tuple[int, int, int]:
    """Run ``PRAGMA wal_checkpoint(TRUNCATE)``; its row ``(busy, log,
    checkpointed)``."""
    row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if row is None:
        raise sqlite3.OperationalError("wal_checkpoint returned no row")
    return int(row[0]), int(row[1]), int(row[2])


def _pragma_int(conn: sqlite3.Connection, name: _Pragma) -> int:
    row = conn.execute(f"PRAGMA {name}").fetchone()
    if row is None:
        raise sqlite3.OperationalError(f"PRAGMA {name} returned no row")
    return int(row[0])


def _marker_present(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM runtime_state WHERE key = ?", (STORAGE_COMPACTED_KEY,)
    ).fetchone()
    return row is not None


def _write_marker(conn: sqlite3.Connection) -> None:
    """Write ``STORAGE_COMPACTED_KEY`` in its own committed transaction.

    The value is the current time in the fresh build's format. On any
    failure the transaction is rolled back and the original error re-raised.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        set_runtime_state(conn, STORAGE_COMPACTED_KEY, isoformat_utc(utc_now()))
        conn.commit()
    except BaseException:
        with contextlib.suppress(sqlite3.Error):
            conn.rollback()
        raise


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000


def compact_storage(conn: sqlite3.Connection, db_path: str) -> None:
    """Compact the database file at ``db_path`` once, on the writer ``conn``.

    Returns at once, logging nothing above DEBUG, when ``runtime_state``
    holds ``STORAGE_COMPACTED_KEY``. Otherwise, each step announced to
    ``core.stop.begin_step``:

    - ``compaction_measure``: live = (page_count - freelist_count) x
      page_size, need = 2.2 x live rounded up. Free bytes are read on the
      database's folder and on ``sqlite_temp_dir()``; one INFO line gives
      them all. When either folder has less than need: WARNING, no marker,
      return.
    - ``compaction_checkpoint_before``, ``compaction_vacuum``,
      ``compaction_checkpoint_after``: ``wal_checkpoint(TRUNCATE)``,
      ``VACUUM``, ``wal_checkpoint(TRUNCATE)``, one timed INFO line each.
    - ``compaction_marker``: with ``freelist_count`` 0, the marker is written
      and committed. INFO with the file's size before and after when the last
      checkpoint was not busy and the file's size is page_count x page_size;
      otherwise WARNING that the file shrinks at the next checkpoint. A
      non-zero ``freelist_count`` is an ERROR naming ``compaction_vacuum``,
      with no marker.

    A stop (``core.stop.stopped_by``) is re-raised, with no marker written.
    Any other ``sqlite3.Error`` or ``OSError`` is logged as an ERROR naming
    the step, with no marker, and the function returns: the next start tries
    again. If the connection is left inside a transaction by then, it is
    rolled back, and an error from that rollback propagates.

    Raises ``RuntimeError`` when ``conn`` is inside a transaction on entry:
    VACUUM cannot run there.
    """
    if conn.in_transaction:
        raise RuntimeError("compact_storage needs a connection outside a transaction")
    step = "compaction_measure"
    try:
        if _marker_present(conn):
            logger.debug("compaction skipped: %s is recorded", STORAGE_COMPACTED_KEY)
            return
        stop.begin_step(step)
        page_size = _pragma_int(conn, "page_size")
        live = (
            _pragma_int(conn, "page_count") - _pragma_int(conn, "freelist_count")
        ) * page_size
        need = (live * _NEED_TENTHS + 9) // 10
        db_dir = os.path.dirname(os.path.abspath(db_path))
        temp_dir = sqlite_temp_dir()
        db_free = _free_bytes(db_dir)
        temp_free = _free_bytes(temp_dir)
        logger.info(
            "compaction measure: live %d bytes, need %d bytes; database folder %s"
            " free %d bytes; temp folder %s free %d bytes",
            live,
            need,
            db_dir,
            db_free,
            temp_dir,
            temp_free,
        )
        for folder, free in ((db_dir, db_free), (temp_dir, temp_free)):
            if free < need:
                logger.warning(
                    "compaction skipped: need %d, free %d on %s;"
                    " tried again at the next start",
                    need,
                    free,
                    folder,
                )
                return

        step = "compaction_checkpoint_before"
        stop.begin_step(step)
        started = time.perf_counter()
        busy, log, done = _checkpoint(conn)
        size_before = os.path.getsize(db_path)
        logger.info(
            "compaction checkpoint before VACUUM: %.0f ms, busy %d, log %d,"
            " checkpointed %d; file %d bytes",
            _elapsed_ms(started),
            busy,
            log,
            done,
            size_before,
        )

        step = "compaction_vacuum"
        stop.begin_step(step)
        started = time.perf_counter()
        _vacuum(conn)
        logger.info("compaction VACUUM: %.0f ms", _elapsed_ms(started))

        step = "compaction_checkpoint_after"
        stop.begin_step(step)
        started = time.perf_counter()
        busy, log, done = _checkpoint(conn)
        logger.info(
            "compaction checkpoint after VACUUM: %.0f ms, busy %d, log %d,"
            " checkpointed %d",
            _elapsed_ms(started),
            busy,
            log,
            done,
        )

        step = "compaction_marker"
        stop.begin_step(step)
        free_pages = _pragma_int(conn, "freelist_count")
        if free_pages != 0:
            logger.error(
                "compaction failed at step compaction_vacuum: freelist_count %d"
                " after VACUUM; no marker, tried again at the next start",
                free_pages,
            )
            return
        expected = _pragma_int(conn, "page_count") * _pragma_int(conn, "page_size")
        size_after = os.path.getsize(db_path)
        _write_marker(conn)
        if busy == 0 and size_after == expected:
            logger.info(
                "compaction done: file %d bytes before, %d bytes after",
                size_before,
                size_after,
            )
        else:
            logger.warning(
                "compaction done; file shrinks at the next checkpoint: file %d"
                " bytes before, %d bytes now, %d bytes expected; last checkpoint"
                " busy %d",
                size_before,
                size_after,
                expected,
                busy,
            )
    except (sqlite3.Error, OSError) as exc:
        if stop.stopped_by(exc):
            raise
        logger.error(
            "compaction failed at step %s: %s; no marker, tried again at the next"
            " start",
            step,
            exc,
        )
        if conn.in_transaction:
            conn.rollback()
