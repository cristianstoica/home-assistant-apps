"""Tests for ``wxverify.db.compaction`` and its
wiring into ``Database._open`` (``A/wxverify/db/connection.py``).

Fixture precondition: ``_build_fixture_db`` opens a fresh v17 database
through ``Database`` (writing ``runtime_state.storage_compacted_at`` as part
of a fresh build, per ``migrations._record_fresh_build_keys``), closes it,
then on a plain connection deletes that marker row, inserts and drops a
filler table to leave freed pages on the freelist (SQLite's default
``auto_vacuum=NONE`` never reclaims them on its own), and asserts both
``PRAGMA freelist_count() > 0`` and the marker's absence before returning the
path -- so every test starts from a database compaction has not yet touched,
never from an incidental one.

Relies on these fixed interface names (not yet implemented; import is
expected to fail until the production module lands):
``wxverify.db.compaction.sqlite_temp_dir``, ``._FIXED_TEMP_DIRS``,
``._free_bytes``, ``._vacuum``, ``._checkpoint``, ``.compact_storage``; the
marker key ``wxverify.db.migrations.STORAGE_COMPACTED_KEY``; and
``wxverify.core.stop`` (``request_stop``, ``reset_stop``, ``stopped_by``).
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

import pytest

from wxverify.core import stop
from wxverify.db import compaction
from wxverify.db.connection import Database
from wxverify.db.migrations import STORAGE_COMPACTED_KEY
from wxverify.db.runtime_state import delete_runtime_state, get_runtime_state

_FILLER_ROWS = 4000
_FILLER_BLOB = "x" * 2000

# Rows for a kept "live" table, used only by the interrupted-VACUUM test.
# VACUUM copies a kept table's rows with one INSERT ... SELECT statement, and
# stop's progress handler checks STOP every stop._PROGRESS_OPS (10,000) VM
# instructions within a statement. Budgeted conservatively at a few
# instructions per row, tens of thousands of rows puts that one statement's
# instruction count several times past 10,000, so a stop lands mid-copy
# instead of being dropped (no statement running) and only caught at the
# next begin_step once VACUUM has already finished.
_LIVE_TABLE_ROWS = 50_000
_LIVE_BLOB = "y" * 200


def _raw_connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _pragmas(db_path: Path) -> tuple[int, int, int]:
    """(freelist_count, page_count, page_size), read on a throwaway connection."""
    conn = _raw_connect(db_path)
    try:
        freelist = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        pages = int(conn.execute("PRAGMA page_count").fetchone()[0])
        size = int(conn.execute("PRAGMA page_size").fetchone()[0])
        return freelist, pages, size
    finally:
        conn.close()


def _build_fixture_db(tmp_path: Path, *, live_rows: int = 0) -> Path:
    """A v17 file with freed pages and no compaction marker.

    ``live_rows``, when positive, also creates a kept "live" table with
    that many rows (see ``_LIVE_TABLE_ROWS`` above) -- only the
    interrupted-VACUUM test passes it; every other test keeps the small,
    fast file.
    """
    db_path = tmp_path / "compaction.db"
    db = Database(str(db_path))
    db.close()

    conn = _raw_connect(db_path)
    try:
        delete_runtime_state(conn, STORAGE_COMPACTED_KEY)
        if live_rows:
            conn.execute("CREATE TABLE live (blob TEXT)")
            conn.executemany(
                "INSERT INTO live (blob) VALUES (?)",
                [(_LIVE_BLOB,)] * live_rows,
            )
        conn.execute("CREATE TABLE filler (blob TEXT)")
        conn.executemany(
            "INSERT INTO filler (blob) VALUES (?)",
            [(_FILLER_BLOB,)] * _FILLER_ROWS,
        )
        conn.execute("DROP TABLE filler")
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        freelist = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        assert freelist > 0, (
            "fixture must leave freed pages on the freelist for compaction "
            "to have something to reclaim"
        )
        marker = get_runtime_state(conn, STORAGE_COMPACTED_KEY)
        assert marker is None, "fixture must start with no compaction marker"
    finally:
        conn.close()
    return db_path


def test_compaction_writes_marker_and_empties_freelist(tmp_path: Path) -> None:
    """Catches: VACUUM running (or its effect check) being skipped/broken, so
    a database with freed pages opens without ever compacting -- the marker
    would stay absent and freelist_count would stay above zero.
    """
    db_path = _build_fixture_db(tmp_path)
    db = Database(str(db_path))
    try:
        marker = get_runtime_state(db._conn, STORAGE_COMPACTED_KEY)  # noqa: SLF001
        assert marker is not None, "compaction must write the marker on success"
        freelist = int(
            db._conn.execute("PRAGMA freelist_count").fetchone()[0]  # noqa: SLF001
        )
        assert freelist == 0, "a successful VACUUM must empty the freelist"
    finally:
        db.close()


def test_second_open_skips_compaction_when_marker_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: compaction re-running (or re-attempting VACUUM) on every open
    once the marker is present, instead of being gated on its absence.
    """
    db_path = _build_fixture_db(tmp_path)
    first = Database(str(db_path))
    first.close()

    def _unexpected_vacuum(conn: sqlite3.Connection) -> None:
        raise AssertionError("VACUUM must not run once the marker is present")

    monkeypatch.setattr(compaction, "_vacuum", _unexpected_vacuum)
    second = Database(str(db_path))
    second.close()


def test_short_free_space_warns_skips_and_start_carries_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Catches: the free-space guard not short-circuiting VACUUM when the
    database folder's disk is reported too small -- it would either VACUUM
    anyway or crash the start instead of logging a WARNING and continuing.

    ``_free_bytes`` is patched per path, shorting ONLY the database folder
    (plenty of room on the temp folder): a guard that dropped the database
    folder out of its ``(db_dir, db_free), (temp_dir, temp_free)`` loop,
    checking only the temp folder, would still pass a version of this test
    that shorted both folders at once -- see the paired
    ``test_short_temp_free_space_warns_skips_and_start_carries_on`` below,
    which shorts only the temp folder.
    """
    db_path = _build_fixture_db(tmp_path)
    db_dir = os.path.dirname(os.path.abspath(str(db_path)))
    temp_dir = compaction.sqlite_temp_dir()
    assert db_dir != temp_dir, (
        "the database and temp folders must differ for this test to tell"
        " the two checks apart"
    )

    def _free_bytes(path: str) -> int:
        return 1 if path == db_dir else 10**15

    monkeypatch.setattr(compaction, "_free_bytes", _free_bytes)

    def _unexpected_vacuum(conn: sqlite3.Connection) -> None:
        raise AssertionError("VACUUM must not run when free space is short")

    monkeypatch.setattr(compaction, "_vacuum", _unexpected_vacuum)
    caplog.set_level(logging.INFO, logger="wxverify.db.compaction")

    db = Database(str(db_path))
    try:
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            r.message.startswith("compaction skipped: need ")
            and "; tried again at the next start" in r.message
            for r in warnings
        ), "short free space must log the exact skip WARNING"
        marker = get_runtime_state(db._conn, STORAGE_COMPACTED_KEY)  # noqa: SLF001
        assert marker is None, "a skipped compaction must not write the marker"
        # "the start carries on": the Database constructed and a read works.
        row = db._conn.execute("SELECT 1").fetchone()  # noqa: SLF001
        assert row[0] == 1
    finally:
        db.close()


def test_short_temp_free_space_warns_skips_and_start_carries_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The paired case: only the temp folder (``sqlite_temp_dir()``) is
    reported too small, plenty of room on the database folder. Catches: a
    guard that dropped the temp folder out of the loop, checking only the
    database folder -- it would VACUUM anyway (into a temp folder it never
    checked).
    """
    db_path = _build_fixture_db(tmp_path)
    db_dir = os.path.dirname(os.path.abspath(str(db_path)))
    temp_dir = compaction.sqlite_temp_dir()
    assert db_dir != temp_dir, (
        "the database and temp folders must differ for this test to tell"
        " the two checks apart"
    )

    def _free_bytes(path: str) -> int:
        return 1 if path == temp_dir else 10**15

    monkeypatch.setattr(compaction, "_free_bytes", _free_bytes)

    def _unexpected_vacuum(conn: sqlite3.Connection) -> None:
        raise AssertionError("VACUUM must not run when free space is short")

    monkeypatch.setattr(compaction, "_vacuum", _unexpected_vacuum)
    caplog.set_level(logging.INFO, logger="wxverify.db.compaction")

    db = Database(str(db_path))
    try:
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            r.message.startswith("compaction skipped: need ")
            and "; tried again at the next start" in r.message
            for r in warnings
        ), "short temp-folder free space must log the exact skip WARNING too"
        marker = get_runtime_state(db._conn, STORAGE_COMPACTED_KEY)  # noqa: SLF001
        assert marker is None, "a skipped compaction must not write the marker"
        row = db._conn.execute("SELECT 1").fetchone()  # noqa: SLF001
        assert row[0] == 1
    finally:
        db.close()


def test_vacuum_error_logs_and_start_carries_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Catches: a VACUUM failure (sqlite3.Error/OSError) propagating out of
    ``Database()`` instead of being logged as an ERROR naming the failing
    step, with no marker written and the start continuing.
    """
    db_path = _build_fixture_db(tmp_path)

    def _raising_vacuum(conn: sqlite3.Connection) -> None:
        raise sqlite3.OperationalError("synthetic vacuum failure")

    monkeypatch.setattr(compaction, "_vacuum", _raising_vacuum)
    caplog.set_level(logging.INFO, logger="wxverify.db.compaction")

    db = Database(str(db_path))
    try:
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert any("compaction_vacuum" in r.message for r in errors), (
            "a VACUUM failure must log an ERROR naming the compaction_vacuum step"
        )
        marker = get_runtime_state(db._conn, STORAGE_COMPACTED_KEY)  # noqa: SLF001
        assert marker is None, "a failed compaction must not write the marker"
        row = db._conn.execute("SELECT 1").fetchone()  # noqa: SLF001
        assert row[0] == 1
    finally:
        db.close()


def test_checkpoint_busy_after_vacuum_warns_and_writes_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The middle case of the effect check: freelist_count is 0 (VACUUM genuinely ran)
    but the LAST checkpoint is reported busy. Catches: the effect check
    collapsing this three-valued result to only "success" or "error" -- it
    would either skip the WARNING and still claim success silently, or
    (worse) treat a busy checkpoint as a hard failure and withhold the
    marker that VACUUM itself already earned.
    """
    db_path = _build_fixture_db(tmp_path)
    real_checkpoint = compaction._checkpoint
    calls = {"n": 0}

    def _fake_checkpoint(conn: sqlite3.Connection) -> tuple[int, int, int]:
        calls["n"] += 1
        busy, log, checkpointed = real_checkpoint(conn)
        if calls["n"] == 2:
            return (1, log, checkpointed)
        return (busy, log, checkpointed)

    monkeypatch.setattr(compaction, "_checkpoint", _fake_checkpoint)
    caplog.set_level(logging.INFO, logger="wxverify.db.compaction")

    db = Database(str(db_path))
    try:
        assert calls["n"] == 2, (
            "the fixture must have driven compact_storage through both "
            "checkpoint calls for this to be the case under test"
        )
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            "file shrinks at the next checkpoint" in r.message for r in warnings
        ), "a busy last checkpoint must log the exact 'file shrinks' WARNING"
        marker = get_runtime_state(db._conn, STORAGE_COMPACTED_KEY)  # noqa: SLF001
        assert marker is not None, (
            "VACUUM itself succeeded, so the marker must still be written "
            "even though the last checkpoint was busy"
        )
    finally:
        db.close()


def test_stop_during_vacuum_leaves_no_marker_and_file_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: a stop mid-VACUUM either being swallowed (so the start
    reports success with a half-done compaction) or leaving the file
    genuinely altered -- an interrupted VACUUM must leave the file exactly
    as it was, and the next start must still rerun and finish compaction.

    The fixture's live table (``_LIVE_TABLE_ROWS`` rows) is needed because
    ``request_stop`` only interrupts a statement already running on the
    writer; with no statement running, SQLite drops that interrupt once the
    next statement starts, and only the progress handler (fired every
    ``stop._PROGRESS_OPS`` instructions within a statement) can still end
    VACUUM. A live table large enough keeps VACUUM's copy of it running
    past that threshold, so the stop lands mid-VACUUM instead of being
    caught only at the next ``begin_step`` after VACUUM has already
    finished.
    """
    db_path = _build_fixture_db(tmp_path, live_rows=_LIVE_TABLE_ROWS)
    conn = _raw_connect(db_path)
    try:
        conn.execute("CREATE TABLE sentinel (n INTEGER)")
        conn.execute("INSERT INTO sentinel (n) VALUES (42)")
        conn.commit()
    finally:
        conn.close()
    pre_freelist, pre_pages, pre_size = _pragmas(db_path)

    real_vacuum = compaction._vacuum
    vacuum_raised = False

    def _interrupting_vacuum(conn: sqlite3.Connection) -> None:
        nonlocal vacuum_raised
        stop.request_stop()
        try:
            real_vacuum(conn)
        except Exception:
            vacuum_raised = True
            raise

    monkeypatch.setattr(compaction, "_vacuum", _interrupting_vacuum)

    with pytest.raises(Exception) as exc_info:  # noqa: PT011
        Database(str(db_path))
    assert stop.stopped_by(exc_info.value), (
        "the exception raised out of Database() must be the one the stop "
        "caused, not some other failure masquerading as one"
    )
    assert vacuum_raised, (
        "the real VACUUM must itself have been interrupted mid-copy; a "
        "VACUUM that ran to completion and was only caught at the next "
        "begin_step must not pass this test"
    )

    post_freelist, post_pages, post_size = _pragmas(db_path)
    assert (post_freelist, post_pages, post_size) == (
        pre_freelist,
        pre_pages,
        pre_size,
    ), "an interrupted VACUUM must leave freelist/page count/page size unchanged"

    conn = _raw_connect(db_path)
    try:
        marker = get_runtime_state(conn, STORAGE_COMPACTED_KEY)
        assert marker is None, "an interrupted compaction must not write the marker"
        row = conn.execute("SELECT n FROM sentinel").fetchone()
        assert row["n"] == 42, "a seeded row must survive an interrupted VACUUM"
    finally:
        conn.close()

    # The next start reruns and finishes compaction, with nothing patched.
    monkeypatch.setattr(compaction, "_vacuum", real_vacuum)
    stop.reset_stop()
    reopened = Database(str(db_path))
    try:
        marker = get_runtime_state(reopened._conn, STORAGE_COMPACTED_KEY)  # noqa: SLF001
        assert marker is not None, "the next start must complete compaction"
        freelist = int(
            reopened._conn.execute("PRAGMA freelist_count").fetchone()[0]  # noqa: SLF001
        )
        assert freelist == 0
    finally:
        reopened.close()


def test_resolver_prefers_sqlite_tmpdir_over_tmpdir(tmp_path: Path) -> None:
    """Catches: the resolver checking TMPDIR (or a fixed dir) before
    SQLITE_TMPDIR, reversing SQLite's own unix-VFS precedence.
    """
    sqlite_dir = tmp_path / "sqlite_tmp"
    other_dir = tmp_path / "other_tmp"
    sqlite_dir.mkdir()
    other_dir.mkdir()
    result = compaction.sqlite_temp_dir(
        {"SQLITE_TMPDIR": str(sqlite_dir), "TMPDIR": str(other_dir)}
    )
    assert result == str(sqlite_dir)


def test_resolver_falls_back_to_tmpdir_when_sqlite_tmpdir_unusable(
    tmp_path: Path,
) -> None:
    """Catches: an unusable SQLITE_TMPDIR (missing, or present but not
    writable/executable) not being skipped in favour of TMPDIR.
    """
    missing = tmp_path / "does_not_exist"
    usable = tmp_path / "usable_tmp"
    usable.mkdir()
    result = compaction.sqlite_temp_dir(
        {"SQLITE_TMPDIR": str(missing), "TMPDIR": str(usable)}
    )
    assert result == str(usable)


def test_resolver_skips_empty_sqlite_tmpdir(tmp_path: Path) -> None:
    """Catches: an empty (set-but-blank) SQLITE_TMPDIR value being treated as
    present and used, instead of being skipped like an unset variable.
    """
    usable = tmp_path / "usable_tmp"
    usable.mkdir()
    result = compaction.sqlite_temp_dir({"SQLITE_TMPDIR": "", "TMPDIR": str(usable)})
    assert result == str(usable)


def test_resolver_falls_to_first_usable_fixed_dir_in_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: the fixed-dir fallback not walking ``_FIXED_TEMP_DIRS`` in
    the declared order, so a later usable dir wins over an earlier one.
    """
    unusable = tmp_path / "unusable_fixed"
    usable = tmp_path / "usable_fixed"
    unusable.mkdir()
    usable.mkdir()
    os.chmod(unusable, 0o000)
    try:
        monkeypatch.setattr(
            compaction, "_FIXED_TEMP_DIRS", (str(unusable), str(usable), ".")
        )
        result = compaction.sqlite_temp_dir({})
        assert result == str(usable)
    finally:
        os.chmod(unusable, 0o755)


def test_resolver_returns_dot_when_nothing_is_usable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches: the resolver raising, or returning something other than
    ``"."``, when SQLITE_TMPDIR/TMPDIR are unset and every fixed dir is
    unusable -- SQLite's own VFS falls to the current directory as its last
    resort, and this must match.
    """
    missing_a = tmp_path / "missing_a"
    missing_b = tmp_path / "missing_b"
    monkeypatch.setattr(
        compaction, "_FIXED_TEMP_DIRS", (str(missing_a), str(missing_b))
    )
    result = compaction.sqlite_temp_dir({})
    assert result == "."
