"""Pinning tests for ``Database._run_immediate``'s commit-failure handling.

All fixture data is synthetic -- this is a PUBLIC repo.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pytest

from wxverify.db.connection import Database


def _make_db(tmp_path: Path) -> Database:
    db = Database(str(tmp_path / "commitfail.db"))
    db._conn.execute("CREATE TABLE cf_parent (id INTEGER PRIMARY KEY)")  # noqa: SLF001
    db._conn.execute(  # noqa: SLF001
        "CREATE TABLE cf_child (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL"
        " REFERENCES cf_parent (id) DEFERRABLE INITIALLY DEFERRED)"
    )
    return db


def _orphan(conn: sqlite3.Connection) -> None:
    """COMMIT fails: a deferred FK violation leaves the transaction open."""
    conn.execute("INSERT INTO cf_parent (id) VALUES (1)")
    conn.execute("INSERT INTO cf_child (id, parent_id) VALUES (1, 999)")


def _ids(db: Database) -> list[int]:
    """An independent reader: the separate ``_read_sync_conn``."""
    return db.read_sync(
        lambda c: [r[0] for r in c.execute("SELECT id FROM cf_parent ORDER BY id")]
    )


class _SyntheticOriginal(Exception):
    """A local exception standing in for whatever error the caller raised."""


class _SyntheticLogFailure(Exception):
    """A local exception standing in for a raising logger filter."""


def test_failed_commit_propagates_and_leaves_nothing_behind(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    try:
        mode = db._conn.execute("PRAGMA journal_mode").fetchone()[0]  # noqa: SLF001
        assert mode == "wal"

        with pytest.raises(
            sqlite3.IntegrityError, match="FOREIGN KEY constraint failed"
        ):
            db.write_sync(_orphan)

        assert db._conn.in_transaction is False  # noqa: SLF001
        assert (
            db._conn.execute("SELECT count(*) FROM cf_parent").fetchone()[0] == 0  # noqa: SLF001
        )
        # Not the oracle: a separate reader can't see an uncommitted
        # transaction even on the unfixed code. The writer-view count and
        # in_transaction above carry the kill.
        assert _ids(db) == []
    finally:
        db.close()


def test_next_write_succeeds_after_a_failed_commit(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    try:
        with pytest.raises(
            sqlite3.IntegrityError, match="FOREIGN KEY constraint failed"
        ):
            db.write_sync(_orphan)

        result = db.write_sync(
            lambda c: c.execute("INSERT INTO cf_parent (id) VALUES (2)").rowcount
        )
        assert result == 1
        assert _ids(db) == [2]
    finally:
        db.close()


def test_successful_write_still_persists(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    try:
        result = db.write_sync(
            lambda c: (
                c.execute("INSERT INTO cf_parent (id) VALUES (7)").rowcount and "done"
            )
        )
        assert result == "done"
        assert db._conn.in_transaction is False  # noqa: SLF001
        assert _ids(db) == [7]
    finally:
        db.close()


def test_raising_rollback_does_not_mask_the_original(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db = _make_db(tmp_path)
    original = _SyntheticOriginal("synthetic original failure")

    def _fn(conn: sqlite3.Connection) -> None:
        conn.close()
        raise original

    try:
        with (
            caplog.at_level(logging.ERROR, logger="wxverify.db.connection"),
            pytest.raises(_SyntheticOriginal) as info,
        ):
            db._run_immediate(_fn)  # noqa: SLF001
        assert info.value is original

        error_records = [
            r
            for r in caplog.records
            if r.name == "wxverify.db.connection" and r.levelno == logging.ERROR
        ]
        assert len(error_records) == 1
        record = error_records[0]
        assert (
            record.getMessage()
            == "db txn rollback failed; re-raising the original error"
        )
        assert record.exc_info is not None
        assert isinstance(record.exc_info[1], sqlite3.ProgrammingError)
        assert record.exc_info[2] is not None
    finally:
        db.close()


def test_raising_debug_line_after_rollback_does_not_mask_original(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db = _make_db(tmp_path)
    original = _SyntheticOriginal("synthetic original failure")
    seen: list[bool] = []
    logger = logging.getLogger("wxverify.db.connection")

    def _rollback_filter(record: logging.LogRecord) -> bool:
        if record.getMessage() == "db txn rollback":
            seen.append(db._conn.in_transaction)  # noqa: SLF001
            raise _SyntheticLogFailure("synthetic log failure")
        return True

    def _fn(conn: sqlite3.Connection) -> None:
        conn.execute("INSERT INTO cf_parent (id) VALUES (1)")
        raise original

    try:
        with caplog.at_level(logging.DEBUG, logger="wxverify.db.connection"):
            logger.addFilter(_rollback_filter)
            try:
                with pytest.raises(_SyntheticOriginal) as info:
                    db.write_sync(_fn)
            finally:
                logger.removeFilter(_rollback_filter)

        assert info.value is original
        assert seen == [False]
        assert db._conn.in_transaction is False  # noqa: SLF001
        assert (
            db._conn.execute("SELECT count(*) FROM cf_parent").fetchone()[0] == 0  # noqa: SLF001
        )
        error_or_above = [
            r
            for r in caplog.records
            if r.name == "wxverify.db.connection" and r.levelno >= logging.ERROR
        ]
        assert error_or_above == []
    finally:
        db.close()


def test_raising_failure_log_after_a_failed_rollback(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db = _make_db(tmp_path)
    original = _SyntheticOriginal("synthetic original failure")
    hits: list[str] = []
    logger = logging.getLogger("wxverify.db.connection")
    failure_message = "db txn rollback failed; re-raising the original error"

    def _failure_filter(record: logging.LogRecord) -> bool:
        if record.getMessage() == failure_message:
            hits.append(record.getMessage())
            raise _SyntheticLogFailure("synthetic log failure")
        return True

    def _fn(conn: sqlite3.Connection) -> None:
        conn.close()
        raise original

    try:
        with caplog.at_level(logging.ERROR, logger="wxverify.db.connection"):
            logger.addFilter(_failure_filter)
            try:
                with pytest.raises(_SyntheticOriginal) as info:
                    db._run_immediate(_fn)  # noqa: SLF001
            finally:
                logger.removeFilter(_failure_filter)

        assert info.value is original
        assert hits == [failure_message]
    finally:
        db.close()
