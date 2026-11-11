"""Pinning tests for the journal_mode refusal.

All fixture data is synthetic -- this is a PUBLIC repo.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from pathlib import Path

import pytest

from wxverify.db.connection import Database

_LOGGER_NAME = "wxverify.db.connection"


class _ForcedMode(sqlite3.Connection):
    """A writer connection whose `execute` rewrites the exact WAL-mode
    pragma text so SQLite really switches into a different journal mode
    (or raises a synthetic I/O error for C4), so the production code's
    real ``PRAGMA journal_mode`` read gets a real, non-WAL row back.

    `forced_mode` and `forced_error` are class attributes, not instance
    ones: `sqlite3.connect`'s `factory` constructs the connection itself,
    so a test sets these just before opening rather than passing them
    through `__init__`.
    """

    forced_mode: str = "wal"
    forced_error: bool = False

    def execute(self, sql, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003, ANN201
        if sql == "PRAGMA journal_mode=WAL":
            if self.forced_error:
                raise sqlite3.OperationalError("disk I/O error")
            sql = f"PRAGMA journal_mode={self.forced_mode}"
        return super().execute(sql, *args, **kwargs)


def _force_journal_mode(
    monkeypatch: pytest.MonkeyPatch, mode: str, *, error: bool = False
) -> list[sqlite3.Connection]:
    """Force every connection `Database` opens through `_ForcedMode`, and
    record each one opened (in open order: the writer first, then the
    pooled readers and the sync reader -- the readers never send the WAL
    pragma text, so they pass through unchanged)."""
    _ForcedMode.forced_mode = mode
    _ForcedMode.forced_error = error
    recorded: list[sqlite3.Connection] = []
    real_connect = sqlite3.connect

    def _wrapped(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        kwargs["factory"] = _ForcedMode
        conn = real_connect(*args, **kwargs)
        recorded.append(conn)
        return conn

    monkeypatch.setattr(sqlite3, "connect", _wrapped)
    return recorded


def _journal_mode_error_records(
    caplog: pytest.LogCaptureFixture,
) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == _LOGGER_NAME
        and record.levelno == logging.ERROR
        and "journal_mode" in record.getMessage()
    ]


# ---------------------------------------------------------------------------
# C1: the mode table.
# ---------------------------------------------------------------------------


def test_wal_mode_logs_nothing_and_the_open_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """C1 (wal). Catches: a check run for every mode, including the one
    that needs no warning."""
    _force_journal_mode(monkeypatch, "wal")
    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        db = Database(str(tmp_path / "wal.db"))
    try:
        assert _journal_mode_error_records(caplog) == []
    finally:
        db.close()


@pytest.mark.parametrize("mode", ["delete", "truncate", "persist"])
def test_rollback_journal_modes_log_once_and_the_open_still_succeeds(
    mode: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """C1 (delete/truncate/persist). Catches: the row still ignored; no
    refusal written for the rollback modes; one line per connection (the
    check also run from `_connect_reader`: more than one line)."""
    _force_journal_mode(monkeypatch, mode)
    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        db = Database(str(tmp_path / f"{mode}.db"))
    try:
        records = _journal_mode_error_records(caplog)
        assert len(records) == 1
        assert mode in records[0].getMessage()
        readback = db._conn.execute("PRAGMA journal_mode").fetchone()[0]  # noqa: SLF001
        assert readback == mode
    finally:
        db.close()


@pytest.mark.parametrize("mode", ["off", "memory"])
def test_unsafe_journal_modes_log_once_then_refuse_and_close_the_writer(
    mode: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """C1 (off/memory). Catches: off/memory let through; the line written
    after the raise (so never); a refused open that keeps its writer
    open."""
    recorded = _force_journal_mode(monkeypatch, mode)
    with (
        caplog.at_level(logging.ERROR, logger=_LOGGER_NAME),
        pytest.raises(RuntimeError),
    ):
        Database(str(tmp_path / f"{mode}.db"))
    records = _journal_mode_error_records(caplog)
    assert len(records) == 1
    assert mode in records[0].getMessage()
    assert len(recorded) == 1
    with pytest.raises(sqlite3.ProgrammingError):
        recorded[0].execute("SELECT 1")


# ---------------------------------------------------------------------------
# C2: a real quiet refusal, no forcing tool.
# ---------------------------------------------------------------------------


def test_a_real_nolock_connection_is_caught_without_forcing(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """C2. Catches: a check that trusts the requested mode instead of the
    returned row. Proves SQLite's own quiet refusal (no shared memory
    under `nolock`) reaches the rule, not only a forced pragma."""
    db_path = tmp_path / "nolock.db"
    conn = sqlite3.connect(f"file:{db_path}?nolock=1", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
            Database._assert_pragmas(conn)  # noqa: SLF001
        records = _journal_mode_error_records(caplog)
        assert len(records) == 1
        assert "delete" in records[0].getMessage()
        readback = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert readback == "delete"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# C3: a rollback mode still works -- the writer, the pooled reader and the
# sync reader all go on serving.
# ---------------------------------------------------------------------------


class _SyntheticSecondRowFailure(Exception):
    """Stands in for whatever a caller's write function can raise."""


def test_rollback_mode_write_and_read_paths_still_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C3. Catches: a degraded branch that returns from `_assert_pragmas`
    before the later pragmas; a writer or reader that cannot work outside
    WAL."""
    _force_journal_mode(monkeypatch, "delete")
    db = Database(str(tmp_path / "rollback.db"))
    try:
        foreign_keys = db._conn.execute("PRAGMA foreign_keys").fetchone()[0]  # noqa: SLF001
        assert int(foreign_keys) == 1
        busy_timeout = db._conn.execute("PRAGMA busy_timeout").fetchone()[0]  # noqa: SLF001
        assert int(busy_timeout) == 30000

        db._conn.execute("CREATE TABLE c3_rows (id INTEGER PRIMARY KEY)")  # noqa: SLF001

        def _ids(conn: sqlite3.Connection) -> list[int]:
            return [r[0] for r in conn.execute("SELECT id FROM c3_rows ORDER BY id")]

        def _insert_then_raise(conn: sqlite3.Connection) -> None:
            conn.execute("INSERT INTO c3_rows (id) VALUES (2)")
            raise _SyntheticSecondRowFailure("synthetic")

        async def _run() -> None:
            await db.write(lambda c: c.execute("INSERT INTO c3_rows (id) VALUES (1)"))
            assert await db.read(_ids) == [1]
            assert db.read_sync(_ids) == [1]

            with pytest.raises(_SyntheticSecondRowFailure):
                await db.write(_insert_then_raise)
            assert await db.read(_ids) == [1]
            assert db.read_sync(_ids) == [1]

        asyncio.run(_run())
    finally:
        db.close()


# ---------------------------------------------------------------------------
# C4: a real SQLite error at the pragma still fails, unmasked.
# ---------------------------------------------------------------------------


def test_a_real_pragma_error_propagates_and_writes_no_journal_mode_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """C4. Catches: a broad `except sqlite3.Error` around the pragma that
    logs and carries on."""
    _force_journal_mode(monkeypatch, "wal", error=True)
    with (
        caplog.at_level(logging.ERROR, logger=_LOGGER_NAME),
        pytest.raises(sqlite3.OperationalError, match="disk I/O error"),
    ):
        Database(str(tmp_path / "ioerror.db"))
    assert _journal_mode_error_records(caplog) == []


# ---------------------------------------------------------------------------
# C5: in-memory is refused whatever the path -- no forcing needed, SQLite
# itself reports journal_mode "memory" for a `:memory:` database.
# ---------------------------------------------------------------------------


def test_in_memory_database_is_refused_after_one_line(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """C5. Catches: a special case that lets an in-memory database
    through."""
    with (
        caplog.at_level(logging.ERROR, logger=_LOGGER_NAME),
        pytest.raises(RuntimeError),
    ):
        Database(":memory:")
    records = _journal_mode_error_records(caplog)
    assert len(records) == 1
    assert "memory" in records[0].getMessage()
