"""SQLite WAL connection facade with a single serialized writer."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil
import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

from wxverify import config
from wxverify.db.migrations import run_migrations

logger = logging.getLogger(__name__)

T = TypeVar("T")
_db_instance: Database | None = None


class Database:
    def __init__(self, path: str) -> None:
        config.ensure_parent_dir(path)
        if sqlite3.sqlite_version_info < (3, 35, 0):
            raise RuntimeError("sqlite 3.35.0 or newer is required")
        self.path = path
        # The locks are created exactly once, here, and are deliberately NOT
        # part of _open(): replace_from() holds both locks across _open(), so
        # recreating them there would let a coroutine that starts waiting
        # during the swap window capture a different lock object than the one
        # being held — two writers could then interleave on the shared write
        # connection.
        self._write_lock = asyncio.Lock()
        self._read_lock = asyncio.Lock()
        self._open()

    def _open(self) -> None:
        """(Re)open both connections on ``self.path``; locks are untouched."""
        self._conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._assert_pragmas(self._conn)
        self._run_immediate(run_migrations)
        self._read_conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self._read_conn.row_factory = sqlite3.Row
        self._assert_reader_pragmas(self._read_conn)

    @staticmethod
    def _assert_pragmas(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        row = conn.execute("PRAGMA foreign_keys").fetchone()
        if row is None or int(row[0]) != 1:
            raise RuntimeError("foreign_keys not enabled")

    @staticmethod
    def _assert_reader_pragmas(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")

    def _run_immediate(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        logger.debug("db txn begin")
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            result = fn(self._conn)
        except BaseException:
            logger.debug("db txn rollback")
            self._conn.rollback()
            raise
        self._conn.commit()
        logger.debug("db txn commit")
        return result

    async def write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        async with self._write_lock:
            return await asyncio.to_thread(self._run_immediate, fn)

    async def read(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        async with self._read_lock:
            return await asyncio.to_thread(fn, self._read_conn)

    def write_sync(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return self._run_immediate(fn)

    def read_sync(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return fn(self._read_conn)

    def close(self) -> None:
        self._read_conn.close()
        self._conn.close()

    async def replace_from(self, new_db: Path, backup: Path) -> None:
        """Replace the live DB file with ``new_db``, backing up the current DB.

        Lock order is fixed: write lock, then read lock. No other code path
        acquires both, so no deadlock ordering exists to violate. Holding
        both locks quiesces every DB access for the swap window; the locks
        themselves are never recreated (see ``__init__``), so mutual
        exclusion across the swap holds by construction.
        """
        async with self._write_lock, self._read_lock:
            await asyncio.to_thread(self._replace_sync, new_db, backup)

    def _replace_sync(self, new_db: Path, backup: Path) -> None:
        # 1. Flush the WAL into the main file. Fail -> raise; nothing changed.
        self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        # 2. Consistent, self-contained backup of the CURRENT DB. Direct
        # execute on the autocommit write conn, NOT via _run_immediate:
        # VACUUM INTO cannot run inside a transaction. The exists pre-check
        # keeps the unlink-on-failure below from deleting a COMPLETE prior
        # backup when a same-second import collides on the timestamped name
        # (VACUUM INTO refuses an existing output file).
        if backup.exists():
            raise FileExistsError(f"backup target already exists: {backup}")
        try:
            self._conn.execute("VACUUM INTO ?", (str(backup),))
        except BaseException:
            backup.unlink(missing_ok=True)
            raise
        # 3. On the last close after a checkpoint, SQLite itself removes the
        # -wal/-shm sidecars.
        self._read_conn.close()
        self._conn.close()
        # 4. Atomic rename, same filesystem. Fail -> reopen the untouched
        # live file and re-raise.
        try:
            os.replace(new_db, self.path)
        except BaseException:
            try:
                self._open()
            except BaseException:
                self._close_quietly()
                raise
            raise
        # 5. WAL-sidecar rule: a stale sidecar beside a new main file
        # corrupts it. Step 3 normally removes them already; this covers a
        # leftover from a previously crashed process.
        self._unlink_sidecars()
        # 6. Reopen on the new file; run_migrations upgrades an
        # older-user_version import here.
        try:
            self._open()
        except BaseException:
            # 7. Rollback: close any half-open connection (after a failed
            # _open(), _read_conn may already be closed — the suppressed
            # double-close is expected), restore the backup by COPY (the
            # backup must survive as the reversibility artifact), reopen,
            # and re-raise the original error.
            self._close_quietly()
            try:
                shutil.copy2(backup, self.path)
                self._unlink_sidecars()
                self._open()
            except BaseException as restore_exc:
                logger.critical(
                    "database unrecoverable after failed import; "
                    "restore the .bak in /data manually"
                )
                raise RuntimeError(
                    "database unrecoverable after failed import; "
                    "restore the .bak in /data manually"
                ) from restore_exc
            raise

    def _close_quietly(self) -> None:
        for conn in (self._conn, self._read_conn):
            with contextlib.suppress(Exception):
                conn.close()

    def _unlink_sidecars(self) -> None:
        for suffix in ("-wal", "-shm"):
            Path(f"{self.path}{suffix}").unlink(missing_ok=True)


def init_db(path: str | None = None) -> Database:
    global _db_instance
    if _db_instance is not None:
        _db_instance.close()
    _db_instance = Database(path or config.db_path)
    return _db_instance


def get_db() -> Database:
    if _db_instance is None:
        return init_db(config.db_path)
    return _db_instance


def close_db() -> None:
    global _db_instance
    if _db_instance is not None:
        _db_instance.close()
        _db_instance = None
