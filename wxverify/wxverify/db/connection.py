"""SQLite WAL connection facade with a single serialized writer."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Callable
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
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._write_lock = asyncio.Lock()
        self._assert_pragmas(self._conn)
        self._run_immediate(run_migrations)
        self._read_conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._read_conn.row_factory = sqlite3.Row
        self._assert_reader_pragmas(self._read_conn)
        self._read_lock = asyncio.Lock()

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
