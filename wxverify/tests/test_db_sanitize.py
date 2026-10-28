"""Pinning tests for wxverify.db.sanitize.

All fixture data is synthetic -- this is a PUBLIC repo.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from wxverify import config
from wxverify.api.routes import db_transfer
from wxverify.core.timeutil import isoformat_utc, parse_utc
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
# Boundary carrier: a well-formed near-datetime.max/.min stamp carrying a
# non-UTC offset is accepted by datetime.fromisoformat and overflows on the
# UTC conversion. parse_utc normalizes that to ValueError, so the sanitizer's
# broad catch handles it -- the catch stays broad because the carrier set
# here is not enumerable, not because this shape needs it.
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
    guarded only by ``table_exists``, which only checks the table exists,
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


def test_boot_guard_clears_open_transaction_after_failed_rollback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_run_immediate`` now rolls back a failed COMMIT itself, so the boot
    guard around the sanitize pass is only reached when THAT rollback also
    raises. This pins the guard for that case: it rolls the still-open
    transaction back a second time, proven by a real write succeeding after
    boot -- not merely by boot not raising, which the broken code also
    satisfies.
    """
    db_path = tmp_path / "guard.db"
    real_connect = sqlite3.connect
    state = {"commits": 0, "rollbacks": 0, "writer_opened": False}

    class FlakyConnection(sqlite3.Connection):
        def commit(self) -> None:
            state["commits"] += 1
            if state["commits"] == 2:
                raise sqlite3.OperationalError("synthetic commit failure")
            super().commit()

        def rollback(self) -> None:
            state["rollbacks"] += 1
            if state["rollbacks"] == 1:
                raise sqlite3.OperationalError("synthetic rollback failure")
            super().rollback()

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
        # intended path at all. The fake raises without issuing COMMIT,
        # which models a COMMIT failure that leaves the transaction open.
        assert state["commits"] == 2
        # rollbacks[0] is _run_immediate's own, which fails; rollbacks[1]
        # is the guard's, which succeeds.
        assert state["rollbacks"] == 2
        assert not db._conn.in_transaction, (  # noqa: SLF001
            "a failed rollback inside _run_immediate must still leave no "
            "open transaction, once the guard's own rollback runs"
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


# ---------------------------------------------------------------------------
# S5 (0.16.2 plan): table_exists resolves case-insensitively.
# ---------------------------------------------------------------------------


def test_sanitize_finds_mixed_case_jobs_and_station_poll_state() -> None:
    """S5: ``table_exists`` matches the way SQLite itself resolves names
    (``COLLATE NOCASE``), so a mixed-case ``Jobs``/``Station_Poll_State``
    pair still gets repaired rather than silently skipped -- the exact-name
    Python lookup this replaces would leave both wedged rows untouched.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE Jobs (
            id INTEGER PRIMARY KEY,
            next_attempt_at TEXT
        );
        CREATE TABLE Station_Poll_State (
            station_id INTEGER PRIMARY KEY,
            next_poll_at TEXT
        );
        """
    )
    conn.execute("INSERT INTO Jobs (next_attempt_at) VALUES ('zzzz')")
    conn.execute(
        "INSERT INTO Station_Poll_State (station_id, next_poll_at) VALUES (1, 'zzzz')"
    )
    # Precondition: only the mixed-case names exist, so a lowercase `jobs`/
    # `station_poll_state` cannot be what the lookup finds.
    names = {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert names == {"Jobs", "Station_Poll_State"}, (
        "fixture: only the mixed-case table names must exist"
    )

    before = datetime.now(UTC)
    sanitize_wedge_prone_timestamps(conn)

    job_row = conn.execute("SELECT next_attempt_at FROM Jobs").fetchone()
    assert job_row["next_attempt_at"] is None

    poll_row = conn.execute(
        "SELECT next_poll_at FROM Station_Poll_State WHERE station_id = 1"
    ).fetchone()
    assert poll_row["next_poll_at"] is not None
    fixed = parse_utc(poll_row["next_poll_at"])
    assert before < fixed <= before + timedelta(seconds=300 + 5)


# ---------------------------------------------------------------------------
# UP1-UP4 (plan §13.10, 0.16.13): release_key_refused_polls.
#
# The minimal `conn` fixture above (only station_id/next_poll_at) has none
# of the three columns `release_key_refused_polls` requires, so its own
# column guard skips the pass there -- those tests above keep passing
# unmodified, now with the release simply skipped. UP1-UP4 need the real
# columns (health_state, last_error, error_count), so they build a
# real-schema database via `Database(...)`, exactly like
# `test_database_open_sanitizes_a_wedged_jobs_row_on_every_open` and
# `test_stage_pending_rebuild_state_sanitizes_the_staged_upload` above.
#
# Synthetic data only (public repo): station ids "KTEST001".."KTEST005".
# ---------------------------------------------------------------------------


def _up_site(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES ('S', 40.0, -105.0, 900.0, 'UTC')"
        ).lastrowid
    )


def _up_station(conn: sqlite3.Connection, site_id: int, pws_id: str) -> int:
    return int(
        conn.execute(
            "INSERT INTO stations (site_id, pws_station_id, lat, lon,"
            " dem_elevation_m) VALUES (?, ?, 40.0, -105.0, 900.0)",
            (site_id, pws_id),
        ).lastrowid
    )


def _up_poll_state(
    conn: sqlite3.Connection,
    station_id: int,
    *,
    health_state: str,
    last_error: str | None,
    next_poll_at: str | None,
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state"
        " (station_id, health_state, last_error, next_poll_at)"
        " VALUES (?, ?, ?, ?)",
        (station_id, health_state, last_error, next_poll_at),
    )


_UP_NOW = datetime(2026, 10, 28, 12, 0, 0, tzinfo=UTC)


def test_up1_releases_only_terminal_401_403_future_polls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Five rows, one release-eligible each of the disqualifying ways:
    a 404 status (not 401/403), and a `next_poll_at` already in the past
    (`next_poll_at > :now` dropped). Only the first two (401 future, 403
    future) are released; `health_state`, `last_error` and `error_count`
    stay untouched on every row -- only `next_poll_at` is rewritten.

    Kills: matching every `terminal` row regardless of `last_error` (would
    also release the 404 row); matching `LIKE 'http 4%'` (would also match
    'http 404'); dropping `next_poll_at > :now` (would also release the
    already-past 401 row).
    """
    caplog.set_level(logging.INFO, logger="wxverify.db.sanitize")
    monkeypatch.setattr("wxverify.db.sanitize.utc_now", lambda: _UP_NOW)
    db = Database(str(tmp_path / "up1.db"))
    try:
        site_id = _up_site(db._conn)  # noqa: SLF001
        s1 = _up_station(db._conn, site_id, "KTEST001")  # noqa: SLF001
        s2 = _up_station(db._conn, site_id, "KTEST002")  # noqa: SLF001
        s3 = _up_station(db._conn, site_id, "KTEST003")  # noqa: SLF001
        s4 = _up_station(db._conn, site_id, "KTEST004")  # noqa: SLF001
        s5 = _up_station(db._conn, site_id, "KTEST005")  # noqa: SLF001

        t_plus20h = isoformat_utc(_UP_NOW + timedelta(hours=20))
        t_plus5h = isoformat_utc(_UP_NOW + timedelta(hours=5))
        t_minus1h = isoformat_utc(_UP_NOW - timedelta(hours=1))
        t_plus5min = isoformat_utc(_UP_NOW + timedelta(minutes=5))

        _up_poll_state(  # noqa: SLF001
            db._conn,
            s1,
            health_state="terminal",
            last_error="http 401",
            next_poll_at=t_plus20h,
        )
        _up_poll_state(  # noqa: SLF001
            db._conn,
            s2,
            health_state="terminal",
            last_error="http 403",
            next_poll_at=t_plus5h,
        )
        _up_poll_state(  # noqa: SLF001
            db._conn,
            s3,
            health_state="terminal",
            last_error="http 404",
            next_poll_at=t_plus20h,
        )
        _up_poll_state(  # noqa: SLF001
            db._conn,
            s4,
            health_state="terminal",
            last_error="http 401",
            next_poll_at=t_minus1h,
        )
        _up_poll_state(  # noqa: SLF001
            db._conn,
            s5,
            health_state="online",
            last_error=None,
            next_poll_at=t_plus5min,
        )
        db._conn.commit()  # noqa: SLF001

        before = {
            int(row["station_id"]): dict(row)
            for row in db._conn.execute(  # noqa: SLF001
                "SELECT station_id, health_state, last_error, error_count"
                " FROM station_poll_state"
            )
        }

        sanitize_wedge_prone_timestamps(db._conn)  # noqa: SLF001
        db._conn.commit()  # noqa: SLF001

        rows = {
            int(row["station_id"]): dict(row)
            for row in db._conn.execute(  # noqa: SLF001
                "SELECT station_id, health_state, last_error, error_count,"
                " next_poll_at FROM station_poll_state"
            )
        }

        now_stamp = isoformat_utc(_UP_NOW)
        assert rows[s1]["next_poll_at"] == now_stamp
        assert rows[s2]["next_poll_at"] == now_stamp
        assert rows[s3]["next_poll_at"] == t_plus20h  # 404: not released
        assert rows[s4]["next_poll_at"] == t_minus1h  # already past: not pulled forward
        assert rows[s5]["next_poll_at"] == t_plus5min  # online: untouched

        for station_id in (s1, s2, s3, s4, s5):
            before_row = before[station_id]
            after_row = rows[station_id]
            assert after_row["health_state"] == before_row["health_state"]
            assert after_row["last_error"] == before_row["last_error"]
            assert after_row["error_count"] == before_row["error_count"]

        assert any(
            r.levelno == logging.INFO
            and r.getMessage()
            == "sanitize: released 2 current-obs polls parked by a refused key"
            for r in caplog.records
        )
    finally:
        db.close()


def test_up2_removes_only_the_retired_all_1day_wind_hold(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A second, still-current wind hold key (`:history_all`) must survive;
    only the retired `:all_1day` key is deleted.

    Kills: a broader match (e.g. a prefix delete) that would remove every
    `wind_history_auth_hold:*` row.
    """
    caplog.set_level(logging.INFO, logger="wxverify.db.sanitize")
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE runtime_state"
        " (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES"
        " ('wind_history_auth_hold:all_1day', 'retired'),"
        " ('wind_history_auth_hold:history_all', 'current')"
    )
    sanitize_wedge_prone_timestamps(conn)
    remaining = {r["key"] for r in conn.execute("SELECT key FROM runtime_state")}
    assert remaining == {"wind_history_auth_hold:history_all"}
    assert any(
        r.levelno == logging.INFO
        and r.getMessage()
        == "sanitize: removed the retired wind lane hold"
        " wind_history_auth_hold:all_1day"
        for r in caplog.records
    )


def test_up3_second_call_changes_nothing_and_logs_no_info(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Nothing from 0.16.13 on writes a `terminal`/`http 401` poll still in
    the future or the retired wind-hold key, so after the first run neither
    matches: a second call must change nothing and log nothing at INFO.

    Kills: a release that is not idempotent (e.g. re-stamping `next_poll_at`
    to `now` again on the second call, which a frozen clock would not even
    reveal by itself -- the INFO-silence check is what catches it).
    """
    caplog.set_level(logging.INFO, logger="wxverify.db.sanitize")
    monkeypatch.setattr("wxverify.db.sanitize.utc_now", lambda: _UP_NOW)
    db = Database(str(tmp_path / "up3.db"))
    try:
        site_id = _up_site(db._conn)  # noqa: SLF001
        station_id = _up_station(db._conn, site_id, "KTEST001")  # noqa: SLF001
        future = isoformat_utc(_UP_NOW + timedelta(hours=20))
        _up_poll_state(  # noqa: SLF001
            db._conn,
            station_id,
            health_state="terminal",
            last_error="http 401",
            next_poll_at=future,
        )
        db._conn.execute(  # noqa: SLF001
            "INSERT INTO runtime_state (key, value) VALUES"
            " ('wind_history_auth_hold:all_1day', 'retired')"
        )
        db._conn.commit()  # noqa: SLF001

        sanitize_wedge_prone_timestamps(db._conn)  # noqa: SLF001
        db._conn.commit()  # noqa: SLF001
        now_stamp = isoformat_utc(_UP_NOW)
        after_first = db._conn.execute(  # noqa: SLF001
            "SELECT next_poll_at FROM station_poll_state WHERE station_id = ?",
            (station_id,),
        ).fetchone()
        assert after_first["next_poll_at"] == now_stamp
        wind_rows = {
            r["key"]
            for r in db._conn.execute(  # noqa: SLF001
                "SELECT key FROM runtime_state"
                " WHERE key = 'wind_history_auth_hold:all_1day'"
            )
        }
        assert wind_rows == set()

        caplog.clear()
        sanitize_wedge_prone_timestamps(db._conn)  # noqa: SLF001
        db._conn.commit()  # noqa: SLF001

        after_second = db._conn.execute(  # noqa: SLF001
            "SELECT next_poll_at FROM station_poll_state WHERE station_id = ?",
            (station_id,),
        ).fetchone()
        assert after_second["next_poll_at"] == now_stamp
        assert [r for r in caplog.records if r.levelno == logging.INFO] == []
    finally:
        db.close()


def test_up4_staged_upload_is_released_before_promotion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mirrors `test_stage_pending_rebuild_state_sanitizes_the_staged_upload`
    above: `_stage_pending_rebuild_state` runs `sanitize_wedge_prone_timestamps`
    (which calls `release_key_refused_polls`) on the staged file BEFORE the
    atomic rename promotes it, so a station parked by a now-expired key
    refusal is released by construction -- never only by the open-path boot
    guard running later against the live database.

    Kills: a release wired only into the open path (`Database.__init__`'s
    own sanitize call), never into the staging path.
    """
    close_db()
    config.db_path = str(tmp_path / "unused.db")
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setattr("wxverify.db.sanitize.utc_now", lambda: _UP_NOW)

    staged_path = tmp_path / "staged.db"
    staged = Database(str(staged_path))
    station_id: int
    try:
        site_id = _up_site(staged._conn)  # noqa: SLF001
        station_id = _up_station(staged._conn, site_id, "KTEST001")  # noqa: SLF001
        future = isoformat_utc(_UP_NOW + timedelta(hours=20))
        _up_poll_state(  # noqa: SLF001
            staged._conn,
            station_id,
            health_state="terminal",
            last_error="http 401",
            next_poll_at=future,
        )
        staged._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # noqa: SLF001
        staged._conn.commit()  # noqa: SLF001
    finally:
        staged.close()

    db_transfer._stage_pending_rebuild_state(staged_path)  # noqa: SLF001

    direct = sqlite3.connect(str(staged_path))
    direct.row_factory = sqlite3.Row
    try:
        row = direct.execute(
            "SELECT next_poll_at FROM station_poll_state WHERE station_id = ?",
            (station_id,),
        ).fetchone()
    finally:
        direct.close()
    assert row["next_poll_at"] == isoformat_utc(_UP_NOW), (
        "the staged file's refused-key poll must already be released at the "
        "point of promotion, not only once the live database next opens"
    )
