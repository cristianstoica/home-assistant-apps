"""Pinning tests for wxverify.db.sanitize.

All fixture data is synthetic -- this is a PUBLIC repo.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from wxverify import config
from wxverify.api.routes import db_transfer
from wxverify.core.timeutil import parse_utc
from wxverify.db.connection import Database, close_db
from wxverify.db.sanitize import sanitize_wedge_prone_timestamps


@pytest.fixture
def conn() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript(
        """
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY,
            next_attempt_at TEXT
        );
        CREATE TABLE station_poll_state (
            station_id INTEGER PRIMARY KEY,
            next_poll_at TEXT
        );
        """
    )
    return c


def test_unparseable_jobs_next_attempt_at_is_cleared_to_null(
    conn: sqlite3.Connection,
) -> None:
    conn.execute(
        "INSERT INTO jobs (id, next_attempt_at) VALUES (1, ?)", ("not-a-timestamp",)
    )
    sanitize_wedge_prone_timestamps(conn)
    row = conn.execute("SELECT next_attempt_at FROM jobs WHERE id = 1").fetchone()
    assert row["next_attempt_at"] is None


def test_valid_jobs_next_attempt_at_is_left_untouched(
    conn: sqlite3.Connection,
) -> None:
    valid = "2026-08-05T00:00:00.000000Z"
    conn.execute("INSERT INTO jobs (id, next_attempt_at) VALUES (1, ?)", (valid,))
    sanitize_wedge_prone_timestamps(conn)
    row = conn.execute("SELECT next_attempt_at FROM jobs WHERE id = 1").fetchone()
    assert row["next_attempt_at"] == valid


def test_unparseable_station_poll_next_poll_at_is_rewritten_to_near_future(
    conn: sqlite3.Connection,
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state (station_id, next_poll_at) VALUES (1, ?)",
        ("garbage",),
    )
    before = datetime.now(UTC)
    sanitize_wedge_prone_timestamps(conn)
    row = conn.execute(
        "SELECT next_poll_at FROM station_poll_state WHERE station_id = 1"
    ).fetchone()
    assert row["next_poll_at"] is not None
    fixed = parse_utc(row["next_poll_at"])
    # Rewritten to the fallback delay window (no settings table -> default
    # 300s), not left NULL -- NULL would make the station instantly due.
    assert before < fixed <= before + timedelta(seconds=300 + 5)


def test_missing_optional_tables_do_not_raise() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    sanitize_wedge_prone_timestamps(conn)


# ---------------------------------------------------------------------------
# OverflowError, not ValueError: a well-formed near-datetime.max/.min stamp
# carrying a UTC offset is accepted by datetime.fromisoformat, and it is
# parse_utc's own .astimezone(UTC) call that overflows. A narrow
# `except ValueError` misses this shape entirely.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "overflowing",
    ["9999-12-31T23:59:59-14:00", "0001-01-01T00:00:00+10:00"],
    ids=["near_max", "near_min"],
)
def test_overflowing_jobs_next_attempt_at_is_cleared_to_null(
    conn: sqlite3.Connection, overflowing: str
) -> None:
    conn.execute("INSERT INTO jobs (id, next_attempt_at) VALUES (1, ?)", (overflowing,))
    sanitize_wedge_prone_timestamps(conn)
    row = conn.execute("SELECT next_attempt_at FROM jobs WHERE id = 1").fetchone()
    assert row["next_attempt_at"] is None


@pytest.mark.parametrize(
    "overflowing",
    ["9999-12-31T23:59:59-14:00", "0001-01-01T00:00:00+10:00"],
    ids=["near_max", "near_min"],
)
def test_overflowing_station_poll_next_poll_at_is_rewritten(
    conn: sqlite3.Connection, overflowing: str
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state (station_id, next_poll_at) VALUES (1, ?)",
        (overflowing,),
    )
    before = datetime.now(UTC)
    sanitize_wedge_prone_timestamps(conn)
    row = conn.execute(
        "SELECT next_poll_at FROM station_poll_state WHERE station_id = 1"
    ).fetchone()
    assert row["next_poll_at"] is not None
    fixed = parse_utc(row["next_poll_at"])
    assert before < fixed <= before + timedelta(seconds=300 + 5)


def test_text_station_id_is_sanitized_without_int_coercion(
    conn: sqlite3.Connection,
) -> None:
    """``station_poll_state.station_id`` in a foreign database is not
    guaranteed to be an integer the way ``jobs.id`` (a rowid alias) is --
    ``int(row["station_id"])`` on a non-numeric TEXT value raises from the
    disposition path itself, not the parse path, so widening the parse
    ``except`` alone would not cover this. The station_id column here is
    declared INTEGER PRIMARY KEY, so route the TEXT value through a
    freeform table shaped like a foreign import instead.
    """
    conn.execute("DROP TABLE station_poll_state")
    conn.execute(
        "CREATE TABLE station_poll_state"
        " (station_id TEXT PRIMARY KEY, next_poll_at TEXT)"
    )
    conn.execute(
        "INSERT INTO station_poll_state (station_id, next_poll_at) VALUES (?, ?)",
        ("KXYZ0001", "not-a-timestamp"),
    )
    before = datetime.now(UTC)
    sanitize_wedge_prone_timestamps(conn)
    row = conn.execute(
        "SELECT next_poll_at FROM station_poll_state WHERE station_id = 'KXYZ0001'"
    ).fetchone()
    assert row["next_poll_at"] is not None
    fixed = parse_utc(row["next_poll_at"])
    assert before < fixed <= before + timedelta(seconds=300 + 5)


def test_malformed_settings_table_falls_back_to_default_interval(
    conn: sqlite3.Connection,
) -> None:
    """A ``settings`` table of the wrong shape (missing ``value``) raises
    ``sqlite3.OperationalError`` from ``get_number_setting``'s SELECT --
    guarded only by ``_table_exists``, which only checks the table exists,
    not its columns. This must fall back to the default interval rather than
    aborting the whole station-poll sanitize pass.
    """
    conn.execute("CREATE TABLE settings (key TEXT PRIMARY KEY)")
    conn.execute(
        "INSERT INTO station_poll_state (station_id, next_poll_at) VALUES (1, ?)",
        ("garbage",),
    )
    before = datetime.now(UTC)
    sanitize_wedge_prone_timestamps(conn)
    row = conn.execute(
        "SELECT next_poll_at FROM station_poll_state WHERE station_id = 1"
    ).fetchone()
    assert row["next_poll_at"] is not None
    fixed = parse_utc(row["next_poll_at"])
    assert before < fixed <= before + timedelta(seconds=300 + 5)


# ---------------------------------------------------------------------------
# Entry-point wiring: Database._open() (every open) and db_transfer's staged
# upload pass. These pin that the wedge fix actually runs from each real
# caller, not just that the underlying function works in isolation.
# ---------------------------------------------------------------------------


def test_database_open_sanitizes_a_wedged_jobs_row_on_every_open(
    tmp_path: Path,
) -> None:
    """A wedged job (its next_attempt_at sorts after every real ISO cutoff)
    inserted directly into the file -- as if hand-edited or swapped in
    outside the app -- must be cleared the moment ``Database`` opens that
    file, not only on the very first boot of a fresh database.
    """
    db_path = tmp_path / "wedged.db"
    db = Database(str(db_path))
    try:
        db._conn.execute(  # noqa: SLF001
            "INSERT INTO jobs (type, job_key, status, next_attempt_at)"
            " VALUES ('catchup', 'k', 'pending', 'zzzz-not-a-timestamp')"
        )
        db._conn.commit()  # noqa: SLF001
    finally:
        db.close()

    # Reopen the SAME file -- this is the "every open, not just first boot"
    # path: the wedged row was written after the first Database() call above
    # already ran migrations, so only a sanitize pass on THIS reopen can
    # catch it.
    reopened = Database(str(db_path))
    try:
        row = reopened._conn.execute(  # noqa: SLF001
            "SELECT next_attempt_at FROM jobs WHERE job_key = 'k'"
        ).fetchone()
        assert row["next_attempt_at"] is None, (
            "a wedged job row must be sanitized on every Database open, "
            "not only when the file is first created"
        )
    finally:
        reopened.close()


def test_boot_guard_clears_open_transaction_after_swallowed_commit_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_run_immediate``'s ``commit()`` sits outside its own try/except, so
    a commit-time failure during the boot-time sanitize pass leaves the
    writer connection inside an open ``BEGIN IMMEDIATE`` even though the
    guard around it swallows the exception. Left uncleared, that holds the
    write lock for the process lifetime and makes every later
    ``_run_immediate`` fail with "cannot start a transaction within a
    transaction". This pins that the guard rolls the open transaction back,
    proven by a real write succeeding after boot -- not merely by boot not
    raising, which the broken code also satisfies.
    """
    db_path = tmp_path / "guard.db"
    real_connect = sqlite3.connect
    state = {"commits": 0, "writer_opened": False}

    class FlakyConnection(sqlite3.Connection):
        def commit(self) -> None:
            state["commits"] += 1
            if state["commits"] == 2:
                raise sqlite3.OperationalError("disk I/O error")
            super().commit()

    def fake_connect(
        path: object, *args: object, **kwargs: object
    ) -> sqlite3.Connection:
        if path == str(db_path) and not state["writer_opened"]:
            state["writer_opened"] = True
            kwargs["factory"] = FlakyConnection
        return real_connect(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(sqlite3, "connect", fake_connect)

    db = Database(str(db_path))
    try:
        # The first commit (run_migrations) must have succeeded and the
        # second (sanitize) must have raised for this test to exercise the
        # intended path at all.
        assert state["commits"] == 2
        assert not db._conn.in_transaction, (  # noqa: SLF001
            "a swallowed commit failure must leave no open transaction"
        )
        result = db.write_sync(lambda conn: conn.execute("SELECT 1").fetchone()[0])
        assert result == 1, (
            "a later write must succeed, not raise 'cannot start a "
            "transaction within a transaction'"
        )
    finally:
        db.close()


def test_stage_pending_rebuild_state_sanitizes_the_staged_upload(
    tmp_path: Path,
) -> None:
    """``_stage_pending_rebuild_state`` runs on the staged upload file before
    promotion, so a wedged row in an imported database is fixed by
    construction -- the promoted file never carries it, regardless of what
    runs (or crashes) after the rename.
    """
    close_db()
    config.db_path = str(tmp_path / "unused.db")
    config.options_path = str(tmp_path / "missing-options.json")

    staged_path = tmp_path / "staged.db"
    staged = Database(str(staged_path))
    try:
        staged._conn.execute(  # noqa: SLF001
            "INSERT INTO jobs (type, job_key, status, next_attempt_at)"
            " VALUES ('catchup', 'k', 'pending', 'zzzz-not-a-timestamp')"
        )
        staged._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        staged._conn.commit()  # noqa: SLF001
    finally:
        staged.close()

    db_transfer._stage_pending_rebuild_state(staged_path)  # noqa: SLF001

    direct = sqlite3.connect(str(staged_path))
    try:
        row = direct.execute(
            "SELECT next_attempt_at FROM jobs WHERE job_key = 'k'"
        ).fetchone()
    finally:
        direct.close()
    assert row[0] is None, (
        "the staged file must be sanitized before promotion, so the wedge "
        "never survives into the live database"
    )
