"""§9 oracles for the read-snapshot rebase of ``published_basis_report``.

Every oracle here proves a property of ``wxverify.db.snapshot.read_snapshot``
and/or the facade that wraps the published-run freshness derivation in it,
``wxverify.verification.freshness.published_basis_report``. See the plan's
own harness-rule bullets (reproduced in each oracle's docstring where they
apply) for why each construction is shaped the way it is.

All fixtures are synthetic: site name ``"snapshot-site"``, timezone
``"UTC"``, coordinates ``0.0``, provider/model names ``provider<N>`` /
``model<N>`` for anything this module seeds itself; the published run under
test, when one is needed with ``freshness.state == "fresh"``, comes from the
production chain via ``_chain_published_run`` (never ``_seed_published_run``,
which writes no manifest rows and can only ever report ``"unknown"``).
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

import pytest

from tests.helpers import assert_read_pool_at_rest
from tests.test_phase7_surface import _make_app, _make_site, _seed_published_run
from tests.test_phase8_section18_oracles import _open_app_db
from tests.test_run_input_manifest import (
    _INSIDE_HORIZON,
    _chain_published_run,
    _insert_sample_at,
)
from wxverify import config
from wxverify.db.connection import _READ_POOL_SIZE, Database, get_db
from wxverify.db.snapshot import SnapshotNestingError, read_snapshot
from wxverify.verification.freshness import published_basis_report
from wxverify.verification.manifest import (
    MANIFEST_COMPONENT_CONFIG_TRUTH,
    MANIFEST_COMPONENT_FORECAST_ARRIVALS,
)

# ---------------------------------------------------------------------------
# Shared harness.
# ---------------------------------------------------------------------------


def _side_conn(path: str) -> sqlite3.Connection:
    """A dedicated writer-side connection to the same file DB.

    Never ``db._conn`` -- per the house rule at
    ``tests/test_graceful_shutdown.py:291-300``: the shared writer runs real
    transactions from executor threads under ``Database._write_lock``, and a
    second, uncoordinated user of that same connection races it.
    """
    conn = sqlite3.connect(path, check_same_thread=False, timeout=5.0)
    conn.row_factory = sqlite3.Row
    return conn


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


class FailingRollback(sqlite3.Connection):
    """A reader whose ``rollback()`` always raises (F4/F5's trigger)."""

    def rollback(self) -> None:
        raise sqlite3.OperationalError("rollback boom")


class FailingRollbackAndClose(FailingRollback):
    """Additionally, ``close()`` raises -- the only way to reach F6."""

    def close(self) -> None:
        raise sqlite3.OperationalError("close boom")


def _rebuild_pool_with_factory(db: Database, factory: type[sqlite3.Connection]) -> None:
    """Fault injection per the harness rule: rebuild ``_read_conns`` through
    ``factory=``, mirroring ``_connect_reader``, then republish the queue.
    Never a patch of the method under test, never a patch of
    ``wxverify.db.connection.sqlite3.connect``."""
    for conn in db._read_conns:  # noqa: SLF001
        conn.close()
    fresh = [
        sqlite3.connect(
            db.path, check_same_thread=False, isolation_level=None, factory=factory
        )
        for _ in range(_READ_POOL_SIZE)
    ]
    for conn in fresh:
        conn.row_factory = sqlite3.Row
        db._assert_reader_pragmas(conn)  # noqa: SLF001
    db._read_conns = fresh  # noqa: SLF001
    db._stock_pool()  # noqa: SLF001


def _no_warning(records: list[logging.LogRecord], message: str) -> bool:
    return not any(r.getMessage() == message for r in records)


def _only_warning(records: list[logging.LogRecord], message: str) -> int:
    return sum(1 for r in records if r.getMessage() == message)


def _sql_trace(
    conn: sqlite3.Connection, fn: Callable[[sqlite3.Connection], object]
) -> list[str]:
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        fn(conn)
    finally:
        conn.set_trace_callback(None)
    return statements


# ---------------------------------------------------------------------------
# O1 -- barrier: a commit landing mid-derivation is not observed, and the
# next derivation observes it.
# ---------------------------------------------------------------------------


def test_o1_mid_derivation_commit_not_observed_then_observed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O1 -> at (b)/(c): correct = 'fresh' then 'changed', mutant (M1, no
    transaction at all) = 'changed' already on the first call. (a) proves
    the writer was never blocked -- the M-IMMEDIATE success criterion."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, _feeds, run_id = _chain_published_run(conn)
    row = conn.execute(
        "SELECT period_start, period_end FROM verification_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    period_start, period_end = row["period_start"], row["period_end"]

    db = get_db()
    side = _side_conn(config.db_path)
    entered = threading.Event()
    writer_done = threading.Event()
    writer_errors: list[BaseException] = []
    real_read_snapshot = read_snapshot

    @contextmanager
    def _recording(
        conn: sqlite3.Connection, *, label: str
    ) -> Iterator[sqlite3.Connection]:
        with real_read_snapshot(conn, label=label) as c:
            entered.set()
            # Bounded well past the writer side connection's own
            # busy_timeout (5.0s, set by _side_conn's timeout=5.0) so a
            # writer that blocks for the full busy_timeout under
            # M-IMMEDIATE (the mutant reader takes the write lock, so the
            # writer's own BEGIN IMMEDIATE waits out busy_timeout and
            # raises OperationalError) reliably sets writer_done via its
            # own `finally` before this wait gives up -- two equal-length
            # timers racing would otherwise sometimes raise "writer never
            # signalled completion" here instead of letting writer_errors
            # carry the real OperationalError through to assertion (a).
            if not writer_done.wait(timeout=10.0):
                raise AssertionError("writer never signalled completion")
            yield c

    monkeypatch.setattr("wxverify.verification.freshness.read_snapshot", _recording)

    def _writer() -> None:
        try:
            assert entered.wait(timeout=5.0)
            side.execute("BEGIN IMMEDIATE")
            cur = side.execute(
                "UPDATE daily_truth SET value = value + 1 "
                "WHERE site_id = ? AND local_date BETWEEN ? AND ?",
                (site_id, period_start, period_end),
            )
            assert cur.rowcount >= 1
            side.commit()
        except BaseException as exc:  # noqa: BLE001
            writer_errors.append(exc)
        finally:
            writer_done.set()

    thread = threading.Thread(target=_writer)
    try:
        thread.start()
        try:
            report = _run(db.read(lambda c: published_basis_report(c, site_id)))
        finally:
            # Joined on EVERY path out of the read -- including the
            # AssertionError/timeout path under M-IMMEDIATE -- so the
            # writer thread can never still be inside a blocking sqlite3
            # C call on `side` when `side.close()` runs in the outer
            # `finally` below. Closing a connection out from under a
            # thread still parked inside a busy-wait on it is a genuine
            # concurrent-access hazard and is what produced the earlier
            # SIGSEGV.
            thread.join(timeout=6.0)
            assert not thread.is_alive(), "writer thread did not stop in time"

        assert not writer_errors, writer_errors  # (a) never blocked
        assert writer_done.is_set()
        assert report.freshness.state == "fresh"  # (b)

        report2 = _run(db.read(lambda c: published_basis_report(c, site_id)))
        assert report2.freshness.state == "changed"  # (c)
        truth_component = next(
            c
            for c in report2.freshness.components
            if c.component == MANIFEST_COMPONENT_CONFIG_TRUTH
        )
        assert truth_component.state == "changed"
    finally:
        if thread.is_alive():
            thread.join()
        side.close()


# ---------------------------------------------------------------------------
# O1b -- __enter__ alone pins the snapshot.
# ---------------------------------------------------------------------------


def test_o1b_enter_alone_pins_the_snapshot(tmp_path: Path) -> None:
    """O1b -> at the first table read inside the block: correct = the
    pre-commit value, mutant M2 (no priming statement) drops the pin."""
    db_path = str(tmp_path / "o1b.db")
    file_conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
    file_conn.row_factory = sqlite3.Row
    file_conn.execute("PRAGMA journal_mode=WAL")
    file_conn.execute("PRAGMA foreign_keys=ON")
    file_conn.executescript(
        "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);"
        "INSERT INTO settings (key, value) VALUES ('min_n', '10');"
    )
    side = _side_conn(db_path)
    try:
        with read_snapshot(file_conn, label="o1b") as c:
            side.execute("UPDATE settings SET value = '99' WHERE key = 'min_n'")
            side.commit()
            row = c.execute("SELECT value FROM settings WHERE key = 'min_n'").fetchone()
            assert row["value"] == "10"
    finally:
        side.close()
        file_conn.close()


# ---------------------------------------------------------------------------
# O2 -- the boundary is the whole report, not just the inner derivation.
# ---------------------------------------------------------------------------


def test_o2_boundary_covers_the_whole_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O2 -> at the single returned report: correct = fresh/all-components-
    fresh/failed_newer_attempt False, then changed on the next; mutant M3
    (bracket around the inner derivation only) reads fresh + True."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id, feeds, run_id = _chain_published_run(conn)

    db = get_db()
    side = _side_conn(config.db_path)
    entered = threading.Event()
    writer_done = threading.Event()
    writer_errors: list[BaseException] = []
    real_read_snapshot = read_snapshot

    @contextmanager
    def _recording(
        conn: sqlite3.Connection, *, label: str
    ) -> Iterator[sqlite3.Connection]:
        with real_read_snapshot(conn, label=label) as c:
            entered.set()
            if not writer_done.wait(timeout=5.0):
                raise AssertionError("writer never signalled completion")
            yield c

    monkeypatch.setattr("wxverify.verification.freshness.read_snapshot", _recording)

    def _writer() -> None:
        try:
            assert entered.wait(timeout=5.0)
            side.execute("BEGIN IMMEDIATE")
            side.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('min_n', '31')"
            )
            side.execute(
                """
                INSERT INTO verification_runs
                    (site_id, tz_generation_id, methodology_version, app_version,
                     state, attempt, config_snapshot, period_start, period_end,
                     settled_through, bootstrap_seed, bootstrap_resamples,
                     input_fingerprint)
                SELECT site_id, tz_generation_id, methodology_version, app_version,
                       'failed', attempt + 1, config_snapshot, period_start,
                       period_end, settled_through, bootstrap_seed,
                       bootstrap_resamples, input_fingerprint
                FROM verification_runs WHERE id = ?
                """,
                (run_id,),
            )
            _insert_sample_at(side, site_id, feeds[0], _INSIDE_HORIZON)
            side.commit()
        except BaseException as exc:  # noqa: BLE001
            writer_errors.append(exc)
        finally:
            writer_done.set()

    thread = threading.Thread(target=_writer)
    try:
        thread.start()
        try:
            report = _run(db.read(lambda c: published_basis_report(c, site_id)))
        finally:
            thread.join(timeout=5.0)
            assert not thread.is_alive(), "writer thread did not stop in time"

        assert not writer_errors, writer_errors
        assert report.freshness.state == "fresh"
        assert all(c.state == "fresh" for c in report.freshness.components)
        assert report.failed_newer_attempt is False

        report2 = _run(db.read(lambda c: published_basis_report(c, site_id)))
        assert report2.freshness.state == "changed"
        by_component = {c.component: c.state for c in report2.freshness.components}
        assert by_component[MANIFEST_COMPONENT_CONFIG_TRUTH] == "changed"
        assert by_component[MANIFEST_COMPONENT_FORECAST_ARRIVALS] == "changed"
        assert report2.failed_newer_attempt is True
    finally:
        if thread.is_alive():
            thread.join()
        side.close()


# ---------------------------------------------------------------------------
# O3 -- an exception ends the transaction inside read_snapshot, not in the
# pool's safety net.
# ---------------------------------------------------------------------------


def test_o3_exception_ends_transaction_inside_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """O3 -> at (b), the absence of the mid-transaction-rollback WARNING:
    correct = absent (read_snapshot's own finally ended it), mutant M4 (no
    try/finally) leaves it present -- the ONLY assertion that distinguishes
    the two, because D4's safety net makes (a)/(c)/(d)/(e) pass either way."""
    db = Database(str(tmp_path / "o3.db"))
    conn = db._conn  # noqa: SLF001 -- to seed a site+run only, never used for the read
    site_id = _make_site(conn, "o3-site")
    _seed_published_run(conn, site_id, fresh_fingerprint=True)

    def _boom(*_a: object, **_k: object) -> str:
        raise sqlite3.OperationalError("boom")

    monkeypatch.setattr("wxverify.verification.runs.result_basis_fingerprint", _boom)

    try:
        caplog.set_level(logging.WARNING, logger="wxverify.db.connection")
        with pytest.raises(sqlite3.OperationalError):
            _run(db.read(lambda c: published_basis_report(c, site_id)))
        records = [r for r in caplog.records if r.name == "wxverify.db.connection"]
        assert _no_warning(
            records, "reader returned mid-transaction; rolling back"
        )  # (b)
        assert_read_pool_at_rest(db)  # (c)

        def _in_transaction(conn: sqlite3.Connection) -> bool:
            try:
                return bool(conn.in_transaction)
            except sqlite3.ProgrammingError:
                return False

        assert not any(  # (d)  # noqa: SLF001
            _in_transaction(c) for c in db._read_conns
        )

        side = _side_conn(db.path)
        try:
            side.execute("BEGIN IMMEDIATE")
            side.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('o3_probe', '1')"
            )
            side.commit()
        finally:
            side.close()
        for _ in range(_READ_POOL_SIZE):  # (e)
            row = _run(
                db.read(
                    lambda c: c.execute(
                        "SELECT value FROM settings WHERE key = 'o3_probe'"
                    ).fetchone()
                )
            )
            assert row["value"] == "1"
    finally:
        db.close()


# ---------------------------------------------------------------------------
# O3b -- a priming failure ends the transaction inside read_snapshot, and
# the body never runs.
# ---------------------------------------------------------------------------


def test_o3b_priming_failure_never_runs_the_body(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """O3b -> at (a)/(b): correct = OperationalError('prime boom') with no
    chained context and the body never entered; mutant M14 (priming hoisted
    above the try) is caught ONLY by (d), the absent mid-transaction WARNING
    -- (a),(b),(c),(e) all pass under M14 too."""

    class RaisesOnPrime(sqlite3.Connection):
        armed = True

        def execute(self, sql: str, *args: object, **kwargs: object) -> Any:  # type: ignore[override]
            if sql == "PRAGMA user_version" and RaisesOnPrime.armed:
                RaisesOnPrime.armed = False
                raise sqlite3.OperationalError("prime boom")
            return super().execute(sql, *args, **kwargs)

    db = Database(str(tmp_path / "o3b.db"))
    try:
        _rebuild_pool_with_factory(db, RaisesOnPrime)
        entered = {"value": False}

        def _cb(conn: sqlite3.Connection) -> None:
            with read_snapshot(conn, label="o3b"):
                entered["value"] = True

        caplog.set_level(logging.WARNING, logger="wxverify.db.connection")
        with pytest.raises(sqlite3.OperationalError) as excinfo:
            _run(db.read(_cb))
        assert str(excinfo.value) == "prime boom"  # (a)
        assert excinfo.value.__context__ is None  # (a)
        assert entered["value"] is False  # (b)

        def _in_transaction(conn: sqlite3.Connection) -> bool:
            try:
                return bool(conn.in_transaction)
            except sqlite3.ProgrammingError:
                return False

        assert not any(  # (c)  # noqa: SLF001
            _in_transaction(c) for c in db._read_conns
        )
        records = [r for r in caplog.records if r.name == "wxverify.db.connection"]
        assert _no_warning(
            records, "reader returned mid-transaction; rolling back"
        )  # (d)

        assert_read_pool_at_rest(db)  # (e)
        side = _side_conn(db.path)
        try:
            side.execute("BEGIN IMMEDIATE")
            side.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('o3b_probe', '1')"
            )
            side.commit()
        finally:
            side.close()
        for _ in range(_READ_POOL_SIZE):
            row = _run(
                db.read(
                    lambda c: c.execute(
                        "SELECT value FROM settings WHERE key = 'o3b_probe'"
                    ).fetchone()
                )
            )
            assert row["value"] == "1"
    finally:
        db.close()


# ---------------------------------------------------------------------------
# O4 -- the connection is never returned while a thread is still inside the
# snapshot (the F3 / run_to_completion regression guard).
# ---------------------------------------------------------------------------


def test_o4_connection_never_returned_mid_snapshot(tmp_path: Path) -> None:
    """O4 -> at (d), the absent mid-transaction WARNING: correct = absent
    (the cancellation shield keeps the coroutine parked until the thread
    actually leaves the block), mutant M11 (naive asyncio.to_thread, no
    shield) makes it present. The 50ms deferred release gives the
    cancellation a deterministic window to be observed while the callback
    is still parked inside the snapshot before the thread is allowed to
    proceed; simplifying it back to a synchronous release turns the kill
    into a race."""
    db = Database(str(tmp_path / "o4.db"))
    try:
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def _cb(conn: sqlite3.Connection) -> None:
            with read_snapshot(conn, label="o4"):
                entered.set()
                release.wait(timeout=2.0)
            finished.set()

        async def _drive() -> None:
            task = asyncio.create_task(db.read(_cb))
            assert await asyncio.to_thread(entered.wait, 2.0)
            task.cancel()
            task.cancel()
            asyncio.get_running_loop().call_later(0.05, release.set)
            with pytest.raises(asyncio.CancelledError):
                await task

        _logger_records: list[logging.LogRecord] = []
        handler = logging.Handler()
        handler.setLevel(logging.WARNING)
        handler.emit = lambda record: _logger_records.append(record)  # type: ignore[method-assign]
        target_logger = logging.getLogger("wxverify.db.connection")
        target_logger.addHandler(handler)
        try:
            _run(_drive())
        finally:
            target_logger.removeHandler(handler)

        release.set()
        assert finished.wait(2.0)

        assert_read_pool_at_rest(db)  # (a)

        def _in_transaction(conn: sqlite3.Connection) -> bool:
            try:
                return bool(conn.in_transaction)
            except sqlite3.ProgrammingError:
                return False

        assert not any(  # (b)  # noqa: SLF001
            _in_transaction(c) for c in db._read_conns
        )

        side = _side_conn(db.path)
        try:
            side.execute("BEGIN IMMEDIATE")
            side.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('o4_probe', '1')"
            )
            side.commit()
        finally:
            side.close()
        for _ in range(_READ_POOL_SIZE):  # (c)
            row = _run(
                db.read(
                    lambda c: c.execute(
                        "SELECT value FROM settings WHERE key = 'o4_probe'"
                    ).fetchone()
                )
            )
            assert row["value"] == "1"

        assert _no_warning(
            _logger_records, "reader returned mid-transaction; rolling back"
        )  # (d)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# O5 -- a database error never becomes "unknown".
# ---------------------------------------------------------------------------


def test_o5_database_error_never_becomes_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O5 -> at the response bodies: correct = 500 with no 'state':
    'unknown' anywhere, mutant M5 (except sqlite3.Error: return an unknown
    RunInputFreshness) is the silent-degradation shape this catches."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id = _make_site(conn, "o5-site")
    _seed_published_run(conn, site_id, fresh_fingerprint=True)

    def _boom(*_a: object, **_k: object) -> str:
        raise sqlite3.OperationalError("boom")

    monkeypatch.setattr("wxverify.verification.runs.result_basis_fingerprint", _boom)
    app = _make_app(monkeypatch)
    from fastapi.testclient import TestClient

    with TestClient(app, raise_server_exceptions=False) as client:
        api_resp = client.get(f"/api/verification/status?site={site_id}")
        page_resp = client.get(f"/verification?site={site_id}")

    assert api_resp.status_code == 500
    assert page_resp.status_code == 500
    assert '"state": "unknown"' not in api_resp.text
    assert '"state":"unknown"' not in api_resp.text
    assert '"state": "unknown"' not in page_resp.text
    assert "result_basis" not in api_resp.text


# ---------------------------------------------------------------------------
# O6 -- repeated derivations leak no readers (a leak/regression guard).
# ---------------------------------------------------------------------------


def test_o6_repeated_derivations_leak_no_readers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O6 -> at qsize() after every request: correct = always
    _READ_POOL_SIZE, mutant M6 (put_nowait omitted) drops it by one on the
    very first iteration. The startup read-cache warm is stubbed so the
    only pooled readers in flight are the request's own derivations --
    otherwise the warm can legitimately hold a reader across an iteration
    on a slow runner."""
    conn = _open_app_db(tmp_path, monkeypatch)
    site_id = _make_site(conn, "o6-site")
    _seed_published_run(conn, site_id, fresh_fingerprint=True)

    async def _no_warm(db: object) -> None:
        return None

    monkeypatch.setattr("wxverify.api.app.warm_read_cache", _no_warm)
    app = _make_app(monkeypatch)
    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        db = get_db()
        for i in range(50):
            if i % 2 == 0:
                resp = client.get(f"/api/verification/status?site={site_id}")
            else:
                resp = client.get(f"/verification?site={site_id}")
            assert resp.status_code == 200
            assert db._read_pool.qsize() == _READ_POOL_SIZE  # noqa: SLF001

    snapshot = db.read_timing_snapshot()
    assert snapshot  # something was recorded
    assert all(timing["errors"] == 0 for timing in snapshot.values())
    assert any(k.startswith("verification_status.<locals>._read:") for k in snapshot)
    assert any(k.startswith("verification_page.<locals>.<lambda>:") for k in snapshot)


# ---------------------------------------------------------------------------
# O7 -- a callback that leaves a transaction open is settled, and the stale
# snapshot is released.
# ---------------------------------------------------------------------------


def _pin_and_leave_open(conn: sqlite3.Connection) -> Any:
    conn.execute("BEGIN DEFERRED")
    return conn.execute("PRAGMA user_version").fetchone()


def test_o7_poisoned_connection_is_settled(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """O7 -> at (1)/(2): correct = the settle step rolls the leaked
    transaction back with exactly one WARNING, mutant M7 (no settle step)
    leaves it in_transaction and silent."""
    db = Database(str(tmp_path / "o7.db"))
    try:
        caplog.set_level(logging.WARNING, logger="wxverify.db.connection")
        _run(db.read(_pin_and_leave_open))

        assert not any(c.in_transaction for c in db._read_conns)  # (1)  # noqa: SLF001
        records = [r for r in caplog.records if r.name == "wxverify.db.connection"]
        assert (
            _only_warning(records, "reader returned mid-transaction; rolling back") == 1
        )  # (2)
        assert_read_pool_at_rest(db)  # (3)

        side = _side_conn(db.path)
        try:
            side.execute("BEGIN IMMEDIATE")
            side.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('o7_probe', '1')"
            )
            side.commit()
        finally:
            side.close()
        for _ in range(_READ_POOL_SIZE):  # (4)
            row = _run(
                db.read(
                    lambda c: c.execute(
                        "SELECT value FROM settings WHERE key = 'o7_probe'"
                    ).fetchone()
                )
            )
            assert row["value"] == "1"
    finally:
        db.close()


# ---------------------------------------------------------------------------
# O8 -- nesting is refused before any SQL, and the outer snapshot survives.
# ---------------------------------------------------------------------------


def test_o8_nesting_refused_outer_survives(tmp_path: Path) -> None:
    """O8 -> at the post-nesting read inside the outer block: correct =
    pre-commit value, mutant M8 (rely on SQLite's own nesting refusal)
    lets the inner manager's finally roll the outer snapshot back."""
    db_path = str(tmp_path / "o8.db")
    conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);"
        "INSERT INTO settings (key, value) VALUES ('min_n', '10');"
    )
    side = _side_conn(db_path)
    try:
        with read_snapshot(conn, label="outer") as outer:
            row = outer.execute(
                "SELECT value FROM settings WHERE key = 'min_n'"
            ).fetchone()
            assert row["value"] == "10"
            with (
                pytest.raises(SnapshotNestingError),
                read_snapshot(outer, label="inner"),
            ):
                pass
            side.execute("UPDATE settings SET value = '99' WHERE key = 'min_n'")
            side.commit()
            row2 = outer.execute(
                "SELECT value FROM settings WHERE key = 'min_n'"
            ).fetchone()
            assert row2["value"] == "10"
        assert conn.in_transaction is False
    finally:
        side.close()
        conn.close()


# ---------------------------------------------------------------------------
# O10 -- the bracket adds exactly two statements and re-reads nothing.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["chain", "legacy"])
def test_o10_bracket_adds_exactly_two_statements(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    """O10 -> at the bracketed statement list: correct =
    [BEGIN DEFERRED, PRAGMA user_version, *unbracketed, ROLLBACK], mutant
    M10 (priming twice) or M1 (no bracket, collapsing the two lists to
    equality) both diverge from that shape."""
    from tests.helpers import asof_conn, asof_make_site

    if kind == "chain":
        conn = asof_conn()
        site_id, _feeds, _run_id = _chain_published_run(conn)
    else:
        conn = asof_conn()
        site_id = asof_make_site(conn, "snapshot-site")
        # Minimal published-but-legacy run, mirroring _seed_published_run's
        # shape without needing the full app/HTTP harness.
        from wxverify.db.tz_generations import ensure_published_generation
        from wxverify.verification.runs import publish_run

        gen_id = ensure_published_generation(conn, site_id)
        cur = conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint)
            VALUES (?, ?, 1, 'test', 'running', 1, '{}', '2026-06-01',
                    '2026-06-01', NULL, 1, 40, 'fp')
            """,
            (site_id, gen_id),
        )
        run_id = int(cur.lastrowid)
        publish_run(conn, site_id, run_id)
        conn.commit()

    def _no_bracket(conn: sqlite3.Connection, *, label: str) -> Any:
        @contextmanager
        def _cm() -> Iterator[sqlite3.Connection]:
            yield conn

        return _cm()

    with monkeypatch.context() as m:
        m.setattr("wxverify.verification.freshness.read_snapshot", _no_bracket)
        unbracketed = _sql_trace(conn, lambda c: published_basis_report(c, site_id))

    bracketed = _sql_trace(conn, lambda c: published_basis_report(c, site_id))

    expected = ["BEGIN DEFERRED", "PRAGMA user_version", *unbracketed, "ROLLBACK"]
    assert bracketed == expected


# ---------------------------------------------------------------------------
# O11 -- a reader that cannot roll back is closed, replaced at its own
# slot, and never re-pooled.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("prior_reads", [0, 2], ids=["head", "non-head"])
def test_o11_failing_rollback_replaced_at_own_slot(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, prior_reads: int
) -> None:
    """O11 (a) -> at the identity of the closed/replaced slot: correct =
    exactly the drawn handle's slot, mutant M20 (hardcoded to slot 0) is
    caught only in the non-head case; M13 (rollback failure logged and the
    un-rolled-back connection returned) is caught at (1)/(2) in both."""
    db = Database(str(tmp_path / "o11.db"))
    try:
        _rebuild_pool_with_factory(db, FailingRollback)

        for _ in range(prior_reads):
            _run(db.read(lambda c: c.execute("SELECT 1").fetchone()))

        old = db._read_pool._queue[0]  # noqa: SLF001
        slot = prior_reads
        before = list(db._read_conns)  # noqa: SLF001
        assert old is db._read_conns[slot]  # noqa: SLF001
        if slot != 0:
            assert old is not db._read_conns[0]  # noqa: SLF001

        caplog.set_level(logging.WARNING, logger="wxverify.db.connection")
        _run(db.read(_pin_and_leave_open))

        with pytest.raises(sqlite3.ProgrammingError):
            old.execute("SELECT 1")  # (1)

        assert old not in db._read_conns  # noqa: SLF001
        assert old not in list(db._read_pool._queue)  # noqa: SLF001  # (2)
        assert type(db._read_conns[slot]) is sqlite3.Connection  # noqa: SLF001
        assert (
            sum(
                1
                for c in db._read_conns
                if type(c) is sqlite3.Connection  # noqa: SLF001
            )
            == 1
        )
        for i, c in enumerate(before):
            if i != slot:
                assert db._read_conns[i] is c  # noqa: SLF001

        assert_read_pool_at_rest(db)  # (3)
        assert {id(c) for c in db._read_pool._queue} == {  # noqa: SLF001
            id(c)
            for c in db._read_conns  # noqa: SLF001
        }

        records = [r for r in caplog.records if r.name == "wxverify.db.connection"]
        assert (
            _only_warning(records, "reader returned mid-transaction; rolling back") == 1
        )
        assert _only_warning(records, "reader rollback failed") == 1
        assert _only_warning(records, "reader closed after a failed rollback") == 1
        assert _only_warning(records, "reader replaced after a failed rollback") == 1
        assert _no_warning(records, "reader close failed after a failed rollback")
        assert _no_warning(
            records,
            "reader reconnect failed; the unusable handle stays pooled "
            "and every read that draws it fails until restart",
        )  # (4)

        _run(db.write(lambda c: c.execute("SELECT 1")))  # (5)

        for _ in range(_READ_POOL_SIZE):  # (6)
            row = _run(db.read(lambda c: c.execute("SELECT 1").fetchone()))
            assert row[0] == 1

        closed = db.close_if_idle()
        assert closed is True
        assert not any(
            r.getMessage().startswith("close skipped:") for r in caplog.records
        )  # (7)
        with pytest.raises(sqlite3.ProgrammingError):
            db._read_conns[slot].execute("SELECT 1")  # noqa: SLF001
        for i, c in enumerate(before):
            if i != slot:
                with pytest.raises(sqlite3.ProgrammingError):
                    c.execute("SELECT 1")
    finally:
        with suppress(Exception):
            db.close()


@pytest.mark.parametrize("prior_reads", [0, 2], ids=["head", "non-head"])
def test_o11b_failing_rollback_and_close(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, prior_reads: int
) -> None:
    """O11 (b) -> the only way to reach F6: rollback AND close both raise.
    Mutant M19 (the close-failure except reduced to `return conn`) leaves
    the open, still-read-marked handle re-pooled."""
    db = Database(str(tmp_path / "o11b.db"))
    try:
        _rebuild_pool_with_factory(db, FailingRollbackAndClose)

        for _ in range(prior_reads):
            _run(db.read(lambda c: c.execute("SELECT 1").fetchone()))

        old = db._read_pool._queue[0]  # noqa: SLF001
        slot = prior_reads
        assert old is db._read_conns[slot]  # noqa: SLF001

        caplog.set_level(logging.WARNING, logger="wxverify.db.connection")
        _run(db.read(_pin_and_leave_open))

        old.execute("SELECT 1")  # (1) still open -- un-closable

        assert old not in db._read_conns  # noqa: SLF001  # (2)
        assert old not in list(db._read_pool._queue)  # noqa: SLF001
        assert type(db._read_conns[slot]) is sqlite3.Connection  # noqa: SLF001

        assert_read_pool_at_rest(db)  # (3)
        assert {id(c) for c in db._read_pool._queue} == {  # noqa: SLF001
            id(c)
            for c in db._read_conns  # noqa: SLF001
        }

        records = [r for r in caplog.records if r.name == "wxverify.db.connection"]
        assert (
            _only_warning(records, "reader returned mid-transaction; rolling back") == 1
        )
        assert _only_warning(records, "reader rollback failed") == 1
        assert (
            _only_warning(records, "reader close failed after a failed rollback") == 1
        )
        assert any(
            r.getMessage() == "reader close failed after a failed rollback"
            and r.exc_info is not None
            for r in records
        )
        assert _only_warning(records, "reader replaced after a failed rollback") == 1
        assert _no_warning(records, "reader closed after a failed rollback")  # (4)

        # O11's own scope note: arm (b) restates only (1) and (4). The other
        # three pooled connections are STILL FailingRollbackAndClose (only
        # `old`'s slot was ever drawn), so a graceful whole-database close
        # here would hit their own always-raising close() -- a property of
        # this test's fixture, not of the settle step under test -- so
        # close_if_idle() is deliberately not exercised in this arm.
    finally:
        for c in db._read_conns:  # noqa: SLF001
            with suppress(Exception):
                sqlite3.Connection.close(c)
        with suppress(Exception):
            db._conn.close()  # noqa: SLF001
        with suppress(Exception):
            db._read_sync_conn.close()  # noqa: SLF001


# ---------------------------------------------------------------------------
# O12 -- a callback that poisons the connection AND raises is still
# settled.
# ---------------------------------------------------------------------------


def _poison_and_raise(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN DEFERRED")
    conn.execute("PRAGMA user_version").fetchone()
    raise RuntimeError("boom")


def test_o12_poison_and_raise_still_settled(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """O12 -> at (1)/(2): correct = the settle step runs even though the
    callback both poisoned the connection and raised; O12 is the ONLY
    oracle that distinguishes M12 (settle moved inside the try) from
    the unmodified implementation."""
    db = Database(str(tmp_path / "o12.db"))
    try:
        caplog.set_level(logging.WARNING, logger="wxverify.db.connection")
        with pytest.raises(RuntimeError):
            _run(db.read(_poison_and_raise))

        assert not any(c.in_transaction for c in db._read_conns)  # (1)  # noqa: SLF001
        records = [r for r in caplog.records if r.name == "wxverify.db.connection"]
        assert (
            _only_warning(records, "reader returned mid-transaction; rolling back") == 1
        )  # (2)
        assert_read_pool_at_rest(db)  # (3)

        side = _side_conn(db.path)
        try:
            side.execute("BEGIN IMMEDIATE")
            side.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES ('o12_probe', '1')"
            )
            side.commit()
        finally:
            side.close()
        for _ in range(_READ_POOL_SIZE):  # (4)
            row = _run(
                db.read(
                    lambda c: c.execute(
                        "SELECT value FROM settings WHERE key = 'o12_probe'"
                    ).fetchone()
                )
            )
            assert row["value"] == "1"
    finally:
        db.close()


# ---------------------------------------------------------------------------
# O13 -- after a replacement, later readers draw a live handle configured
# like the pool's own.
# ---------------------------------------------------------------------------


def test_o13_replacement_reader_is_configured_like_the_pool(tmp_path: Path) -> None:
    """O13 -> at the fresh handle's row_factory/pragmas: correct = matches
    _connect_reader's own setup, mutant M15 (bare sqlite3.connect, no
    factory) leaves them unset; M16 (no reconnect) surfaces as a
    ProgrammingError instead; M17 (list swap omitted) fails the identity
    sweep before this is even reached."""
    db = Database(str(tmp_path / "o13.db"))
    try:
        _rebuild_pool_with_factory(db, FailingRollback)
        _run(db.read(_pin_and_leave_open))

        for _ in range(_READ_POOL_SIZE):
            row = _run(db.read(lambda c: c.execute("SELECT 1").fetchone()))
            assert row[0] == 1
            assert_read_pool_at_rest(db)
            assert {id(c) for c in db._read_pool._queue} == {  # noqa: SLF001
                id(c)
                for c in db._read_conns  # noqa: SLF001
            }

        fresh = next(c for c in db._read_conns if type(c) is sqlite3.Connection)  # noqa: SLF001
        assert fresh.row_factory is sqlite3.Row
        assert fresh.execute("PRAGMA busy_timeout").fetchone()[0] == 30000
        assert fresh.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    finally:
        db.close()


# ---------------------------------------------------------------------------
# O14 -- after a replacement, the graceful close still completes and
# closes everything, the fresh handle included.
# ---------------------------------------------------------------------------


def test_o14_graceful_close_closes_the_fresh_handle_too(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """O14 -> at every handle raising ProgrammingError afterward: correct =
    the union of the queue and _read_conns at call time, mutant M17 (list
    swap omitted) leaves the fresh handle unreachable to close(); mutant
    M18 (pool shrunk) makes close_if_idle refuse."""
    db = Database(str(tmp_path / "o14.db"))
    try:
        _rebuild_pool_with_factory(db, FailingRollback)
        _run(db.read(_pin_and_leave_open))
        assert_read_pool_at_rest(db)
        assert {id(c) for c in db._read_pool._queue} == {  # noqa: SLF001
            id(c)
            for c in db._read_conns  # noqa: SLF001
        }

        handles = set(db._read_pool._queue) | set(db._read_conns)  # noqa: SLF001
        caplog.set_level(logging.WARNING, logger="wxverify.db.connection")
        closed = db.close_if_idle()
        assert closed is True
        assert not any(
            r.getMessage().startswith("close skipped:")
            for r in caplog.records
            if r.name == "wxverify.db.connection"
        )
        for c in handles:
            with pytest.raises(sqlite3.ProgrammingError):
                c.execute("SELECT 1")
    finally:
        with suppress(Exception):
            db.close()


# ---------------------------------------------------------------------------
# O15 -- when the reconnect itself fails, the closed handle is re-pooled,
# fails loudly once per draw, and shutdown still works.
# ---------------------------------------------------------------------------


def test_o15_reconnect_failure_repools_closed_handle(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """O15 -> at (4)/(5): correct = the reconnect-failed ERROR fires once
    and exactly one of a full sweep raises ProgrammingError, never a
    retry-storm; mutant M18 (the pool shrunk) fails (3)/(7); mutant M16 (no
    reconnect attempted) misses the reconnect-failed ERROR."""
    db = Database(str(tmp_path / "o15.db"))
    try:
        _rebuild_pool_with_factory(db, FailingRollback)

        def _raising() -> sqlite3.Connection:
            raise sqlite3.OperationalError("unable to open database file")

        monkeypatch.setattr(db, "_connect_reader", _raising)

        old = db._read_conns[0]  # noqa: SLF001
        caplog.set_level(logging.WARNING, logger="wxverify.db.connection")
        _run(db.read(_pin_and_leave_open))

        assert old is db._read_conns[0]  # noqa: SLF001  # (1)
        assert old in list(db._read_pool._queue)  # noqa: SLF001

        with pytest.raises(sqlite3.ProgrammingError):
            old.execute("SELECT 1")  # (2)

        assert_read_pool_at_rest(db)  # (3)
        assert {id(c) for c in db._read_pool._queue} == {  # noqa: SLF001
            id(c)
            for c in db._read_conns  # noqa: SLF001
        }

        records = [r for r in caplog.records if r.name == "wxverify.db.connection"]
        reconnect_msg = (
            "reader reconnect failed; the unusable handle stays pooled "
            "and every read that draws it fails until restart"
        )
        assert _only_warning(records, reconnect_msg) == 1
        assert any(
            r.getMessage() == reconnect_msg and r.exc_info is not None for r in records
        )
        assert _no_warning(records, "reader replaced after a failed rollback")  # (4)

        failures = 0
        for _ in range(_READ_POOL_SIZE):  # (5)
            try:
                row = _run(db.read(lambda c: c.execute("SELECT 1").fetchone()))
                assert row[0] == 1
            except sqlite3.ProgrammingError:
                failures += 1
        assert failures == 1

        assert_read_pool_at_rest(db)  # (6)
        records2 = [r for r in caplog.records if r.name == "wxverify.db.connection"]
        assert _only_warning(records2, reconnect_msg) == 1

        closed = db.close_if_idle()  # (7)
        assert closed is True
        assert not any(
            r.getMessage().startswith("close skipped:") for r in caplog.records
        )
    finally:
        with suppress(Exception):
            db.close()
