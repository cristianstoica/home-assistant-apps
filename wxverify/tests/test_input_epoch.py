"""§5.5 Step 1 oracles for the scoring-input epoch (``Database.input_epoch``).

Every test builds a real ``Database`` with a local ``_init_tmp_db`` and
creates its own scratch table on ``db._conn`` -- DDL on the autocommit writer
connection does not itself move the epoch. ``e0`` is always
``db.input_epoch`` read immediately before the step under test.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path

import pytest

from tests.helpers import assert_read_pool_at_rest
from wxverify import config
from wxverify.db.connection import (
    EPOCH_MOVED,
    Database,
    FencedWriter,
    StaleGenerationError,
    close_db,
    init_db,
)
from wxverify.db.snapshot import read_only_snapshot


def _init_tmp_db(tmp_path: Path) -> Database:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text("{}", encoding="utf-8")
    config.options_path = str(options_path)
    return init_db(str(db_path))


def _make_probe(db: Database) -> None:
    db._conn.execute("CREATE TABLE _probe(a INTEGER)")  # noqa: SLF001


def _named_probe_write(conn: sqlite3.Connection) -> None:
    conn.execute("INSERT INTO _probe(a) VALUES (1)")


# ---------------------------------------------------------------------------
# T-dv (M14)
# ---------------------------------------------------------------------------


def test_dv_foreign_connection_commit_moves_epoch_once(tmp_path: Path) -> None:
    """T-dv -> at `epoch_after_first`: correct = e0 + 1 (a foreign
    connection's commit is detected via PRAGMA data_version on the very next
    write transaction), mutant (M14, the data_version check removed from
    `_body`) = e0, because the mutant never observes the foreign commit at
    all, so the first no-op write does not bump either.
    T-dv -> at `epoch_after_second`: correct = epoch_after_first (no further
    foreign commit happened, so a second no-op write moves it by 0), mutant
    (M14) = epoch_after_first (unchanged) as well -- this second assertion
    alone would not distinguish M14, which is why it is paired with the
    first.
    """
    db = _init_tmp_db(tmp_path)
    _make_probe(db)

    side = sqlite3.connect(db.path, isolation_level=None, check_same_thread=False)

    def _noop(conn: sqlite3.Connection) -> None:
        conn.execute("SELECT 1").fetchone()

    async def _run() -> tuple[int, int, int]:
        e0 = db.input_epoch
        try:
            side.execute("INSERT INTO _probe(a) VALUES (1)")
        finally:
            side.close()

        await db.write(_noop)
        epoch_after_first = db.input_epoch
        await db.write(_noop)
        epoch_after_second = db.input_epoch
        return e0, epoch_after_first, epoch_after_second

    e0, epoch_after_first, epoch_after_second = asyncio.run(_run())

    assert epoch_after_first == e0 + 1
    assert epoch_after_second == epoch_after_first


# ---------------------------------------------------------------------------
# T-gate (M1, MX-changes, MX-gate)
# ---------------------------------------------------------------------------


def test_gate_bumps_only_on_an_actual_change(tmp_path: Path) -> None:
    """T-gate -> at `epoch_after_readonly`: correct = e0 (a write whose `fn`
    changes nothing never bumps), mutant (MX-gate, the bump runs
    unconditionally on every write regardless of whether anything changed)
    = e0 + 1: `_run_epoch_txn`'s `finally` bumps even on `_readonly`'s no-op
    `SELECT 1`. The SAME step also kills a second, distinct mutant:
    MX-changes in its last-statement form (gating on `SELECT changes() !=
    0` -- the SQL function reporting only the most recently completed
    INSERT/UPDATE/DELETE's own row count, not the transaction's cumulative
    `total_changes` delta) is observed to die HERE too, also at
    `epoch_after_readonly` = e0 + 1, because this fixture's own setup DML
    (`_init_tmp_db`/migrations) leaves `changes()` already nonzero before
    `_readonly` ever runs, so the mutant's `!= 0` gate is spuriously
    satisfied on the very first, otherwise-no-op write. (On a connection
    with no prior DML, `changes()` would read 0 at step 1, so the mutant
    would not bump there; at step 2 the trailing no-op UPDATE resets
    `changes()` to 0 after the INSERT, so the mutant's `!= 0` gate is not
    satisfied and it skips the bump there, dying at step 2 instead -- but
    that is not what this fixture, with its real setup DML, produces.)
    T-gate -> at `epoch_after_insert_then_noop_update`: correct =
    epoch_after_readonly + 1 (the INSERT changed a row, so the transaction's
    cumulative `total_changes` moved even though the trailing no-op UPDATE
    changed nothing), mutant (M1, plain `write`/`write_fenced` call
    `_run_immediate` directly and never reach `_run_epoch_txn` at all, so
    they never bump under any condition) = epoch_after_readonly + 0: `assert
    1 == 2` (the epoch is still 1, one step behind the required 2). M1 first
    diverges HERE, not at step 1, because at step 1 the correct outcome (no
    bump on a no-op write) and M1's outcome (never bumps at all) coincide.
    T-gate -> at `epoch_after_plain_insert`: correct =
    epoch_after_insert_then_noop_update + 1. This step is a paired positive,
    not a mutant-killing step in this table: it verifies the correct
    implementation's normal bump, and no listed mutant first fails here.
    T-gate -> at `epoch_after_fenced_insert`: correct =
    epoch_after_plain_insert + 1. Also a paired positive here, not a
    mutant-killing step: `write_fenced` bumping normally on a real change
    confirms the fenced path isn't itself broken, independent of the
    mutants above (all already dead by step 1 or 2).
    """
    db = _init_tmp_db(tmp_path)
    _make_probe(db)

    def _readonly(conn: sqlite3.Connection) -> None:
        conn.execute("SELECT 1").fetchone()

    def _insert_then_noop_update(conn: sqlite3.Connection) -> None:
        conn.execute("INSERT INTO _probe(a) VALUES (1)")
        conn.execute("UPDATE _probe SET a = a WHERE 0")

    def _plain_insert(conn: sqlite3.Connection) -> None:
        conn.execute("INSERT INTO _probe(a) VALUES (2)")

    async def _run() -> tuple[int, int, int, int, int]:
        e0 = db.input_epoch

        await db.write(_readonly)
        epoch_after_readonly = db.input_epoch

        await db.write(_insert_then_noop_update)
        epoch_after_insert_then_noop_update = db.input_epoch

        await db.write(_plain_insert)
        epoch_after_plain_insert = db.input_epoch

        await db.write_fenced(_plain_insert, generation=db.generation)
        epoch_after_fenced_insert = db.input_epoch

        return (
            e0,
            epoch_after_readonly,
            epoch_after_insert_then_noop_update,
            epoch_after_plain_insert,
            epoch_after_fenced_insert,
        )

    (
        e0,
        epoch_after_readonly,
        epoch_after_insert_then_noop_update,
        epoch_after_plain_insert,
        epoch_after_fenced_insert,
    ) = asyncio.run(_run())

    assert epoch_after_readonly == e0
    assert epoch_after_insert_then_noop_update == epoch_after_readonly + 1
    assert epoch_after_plain_insert == epoch_after_insert_then_noop_update + 1
    assert epoch_after_fenced_insert == epoch_after_plain_insert + 1


# ---------------------------------------------------------------------------
# T-ws (MX-write_sync)
# ---------------------------------------------------------------------------


def test_ws_write_sync_moves_epoch_on_an_actual_change(tmp_path: Path) -> None:
    """T-ws -> at `db.input_epoch` after the call: correct = e0 + 1
    (`write_sync` goes through `_run_epoch_txn` just like `write`), mutant
    (MX-write_sync, `write_sync` calls `_run_immediate` directly) = e0.
    """
    db = _init_tmp_db(tmp_path)
    _make_probe(db)
    e0 = db.input_epoch

    db.write_sync(lambda c: c.execute("INSERT INTO _probe(a) VALUES (1)"))

    assert db.input_epoch == e0 + 1


# ---------------------------------------------------------------------------
# T-EX (the exemption itself)
# ---------------------------------------------------------------------------


def test_ex_epoch_exempt_suppresses_the_bump_on_both_paths(tmp_path: Path) -> None:
    """T-EX -> at `epoch_after_exempt_write`: correct = e0
    (`epoch_exempt=True` on `Database.write` suppresses the bump even though
    the row-changed condition is met), mutant (a write path that ignores
    `epoch_exempt`) = e0 + 1.
    T-EX -> at `epoch_after_exempt_fenced_write`: correct =
    epoch_after_exempt_write (`FencedWriter(..., epoch_exempt=True)` also
    suppresses the bump), mutant = epoch_after_exempt_write + 1.
    T-EX -> at `epoch_after_plain_write`: correct =
    epoch_after_exempt_fenced_write + 1 (the identical insert WITHOUT the
    keyword moves the epoch normally -- the paired positive proving the
    suppression above was the keyword's doing, not some property of the
    insert itself), mutant (a `write` that is unconditionally exempt) =
    epoch_after_exempt_fenced_write + 0.
    T-EX -> at `epoch_after_plain_fenced_write`: correct =
    epoch_after_plain_write + 1, same pairing through `FencedWriter` with no
    `epoch_exempt`.
    T-EX -> at `row_count`: correct = 4 (every insert landed regardless of
    whether it moved the epoch -- the exemption suppresses the EPOCH bump
    only, never the write itself), mutant (a broken exemption that also
    skips the write) = fewer than 4.
    """
    db = _init_tmp_db(tmp_path)
    _make_probe(db)

    def _insert(value: int) -> Callable[[sqlite3.Connection], None]:
        def _fn(conn: sqlite3.Connection) -> None:
            conn.execute("INSERT INTO _probe(a) VALUES (?)", (value,))

        return _fn

    async def _run() -> tuple[int, int, int, int, int, int]:
        e0 = db.input_epoch

        await db.write(_insert(1), epoch_exempt=True)
        epoch_after_exempt_write = db.input_epoch

        await FencedWriter(db, db.generation, epoch_exempt=True).write(_insert(2))
        epoch_after_exempt_fenced_write = db.input_epoch

        await db.write(_insert(3))
        epoch_after_plain_write = db.input_epoch

        await FencedWriter(db, db.generation).write(_insert(4))
        epoch_after_plain_fenced_write = db.input_epoch

        row_count = db.read_sync(
            lambda c: c.execute("SELECT count(*) FROM _probe").fetchone()[0]
        )

        return (
            e0,
            epoch_after_exempt_write,
            epoch_after_exempt_fenced_write,
            epoch_after_plain_write,
            epoch_after_plain_fenced_write,
            row_count,
        )

    (
        e0,
        epoch_after_exempt_write,
        epoch_after_exempt_fenced_write,
        epoch_after_plain_write,
        epoch_after_plain_fenced_write,
        row_count,
    ) = asyncio.run(_run())

    assert epoch_after_exempt_write == e0
    assert epoch_after_exempt_fenced_write == epoch_after_exempt_write
    assert epoch_after_plain_write == epoch_after_exempt_fenced_write + 1
    assert epoch_after_plain_fenced_write == epoch_after_plain_write + 1
    assert row_count == 4


# ---------------------------------------------------------------------------
# T-M2 (M2)
# ---------------------------------------------------------------------------


def test_m2_bump_recorded_after_commit_not_inside_the_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T-M2 -> at `spy_readings`: correct = [e0] (the change-gated bump
    happens in `_run_epoch_txn`'s own `finally`, strictly AFTER
    `_run_immediate` has already committed and returned), mutant (M2, the
    bump moved inside `_body`, before `fn` returns and before the commit)
    = [e0 + 1], because by the time the spy reads `db.input_epoch` -- right
    after `real_run_immediate(fn)` returns -- the mutant has already applied
    the bump inside that same call.
    """
    db = _init_tmp_db(tmp_path)
    _make_probe(db)
    e0 = db.input_epoch
    real_run_immediate = db._run_immediate  # noqa: SLF001
    spy_readings: list[int] = []

    def _spy(fn: Callable[[sqlite3.Connection], object]) -> object:
        result = real_run_immediate(fn)
        spy_readings.append(db.input_epoch)
        return result

    monkeypatch.setattr(db, "_run_immediate", _spy)  # noqa: SLF001

    async def _run() -> None:
        await db.write(lambda c: c.execute("INSERT INTO _probe(a) VALUES (1)"))

    asyncio.run(_run())

    assert spy_readings == [e0]
    assert db.input_epoch == e0 + 1


# ---------------------------------------------------------------------------
# T-M15 (M15)
# ---------------------------------------------------------------------------


def test_m15_data_version_is_read_inside_begin_immediate_not_before(
    tmp_path: Path,
) -> None:
    """T-M15 -> at the `write_if_current` outcome: correct = `EPOCH_MOVED`
    (the data_version read happens inside the writer's own `BEGIN IMMEDIATE`
    transaction, so once connection B's competing commit finally lets that
    `BEGIN IMMEDIATE` through, the read observes B's change), mutant (M15,
    the data_version read moved to before `BEGIN IMMEDIATE`) = a tuple,
    because the mutant's read would have already run -- and seen nothing --
    before B ever commits.
    Precedent for a side connection holding the write lock while the writer
    waits on it: tests/test_freshness_read_snapshot.py:157-175.
    """
    db = _init_tmp_db(tmp_path)
    _make_probe(db)

    conn_b = sqlite3.connect(db.path, isolation_level=None, check_same_thread=False)
    conn_b.execute("BEGIN IMMEDIATE")
    conn_b.execute("INSERT INTO _probe(a) VALUES (1)")

    begun = threading.Event()
    errors: list[BaseException] = []

    def _trace(statement: str) -> None:
        if statement.strip().upper().startswith("BEGIN IMMEDIATE"):
            begun.set()

    def _commit_b_once_writer_has_begun() -> None:
        try:
            if not begun.wait(timeout=5.0):
                raise AssertionError("writer never entered BEGIN IMMEDIATE")  # noqa: TRY301
            conn_b.commit()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=_commit_b_once_writer_has_begun)
    thread.start()

    fn_ran = {"value": False}

    def _fn(conn: sqlite3.Connection) -> None:
        fn_ran["value"] = True

    async def _run() -> tuple[int, object]:
        e0 = db.input_epoch
        db._conn.set_trace_callback(_trace)  # noqa: SLF001
        try:
            result = await db.write_if_current(_fn, generation=db.generation, epoch=e0)
        finally:
            db._conn.set_trace_callback(None)  # noqa: SLF001
        return e0, result

    try:
        e0, result = asyncio.run(_run())
    finally:
        thread.join(timeout=5.0)
        conn_b.close()

    assert not errors, errors
    assert result is EPOCH_MOVED
    assert fn_ran["value"] is False
    assert db.input_epoch == e0 + 1


# ---------------------------------------------------------------------------
# T-M16 (M16)
# ---------------------------------------------------------------------------


def test_m16_open_seeds_dv_seen_so_the_first_write_does_not_spuriously_bump(
    tmp_path: Path,
) -> None:
    """Precondition (controlled, injected -- not ambient): PRAGMA
    data_version on the just-opened database must not be 0, or M16 is
    equivalent under this fixture and the test below would pass for the
    wrong reason. Stop and report rather than silently passing if it is 0.

    T-M16 -> at `epoch_after_noop`: correct = e0 (a no-op write right after
    `init_db` does not observe the database's own just-completed open as a
    foreign change, because `_open` seeds `_dv_seen` from that same
    connection's data_version), mutant (M16, `_open` does not set
    `_dv_seen`, leaving it at `__init__`'s default of 0) = e0 + 1, because
    the mutant's `_dv_seen` (still 0) essentially never matches a freshly
    opened connection's real data_version, so the very first write always
    sees a spurious "change".
    """
    db = _init_tmp_db(tmp_path)
    dv = db._conn.execute("PRAGMA data_version").fetchone()[0]  # noqa: SLF001
    if dv == 0:
        pytest.fail(
            "precondition failed: PRAGMA data_version is 0 on a freshly "
            "opened database, so M16 is equivalent under this fixture -- "
            "the fixture needs redesigning, not a pass through it"
        )

    e0 = db.input_epoch

    async def _run() -> int:
        await db.write(lambda c: c.execute("SELECT 1").fetchone())
        return db.input_epoch

    epoch_after_noop = asyncio.run(_run())

    assert epoch_after_noop == e0


# ---------------------------------------------------------------------------
# T-M16b (M16 through the CAS)
# ---------------------------------------------------------------------------


def test_m16b_a_fresh_database_cas_hits_on_its_own_read_at_epoch(
    tmp_path: Path,
) -> None:
    """T-M16b -> at the `write_if_current` result: correct = a `tuple` (the
    CAS hits, because `_dv_seen` was seeded at open, so `read_at_epoch`'s
    epoch and the CAS's own internal data_version check agree with no
    intervening foreign commit), mutant (M16, `_open` does not set
    `_dv_seen`) = `EPOCH_MOVED`, because the mutant's un-seeded `_dv_seen`
    makes the CAS's own transaction see a spurious data_version mismatch on
    its very first write.
    """
    db = _init_tmp_db(tmp_path)
    writer = FencedWriter(db, db.generation)

    async def _run() -> object:
        epoch, _result = await writer.read_at_epoch(lambda c: None, label="t")
        return await writer.write_if_current(lambda c: None, epoch=epoch)

    result = asyncio.run(_run())

    assert isinstance(result, tuple)
    assert result is not EPOCH_MOVED


# ---------------------------------------------------------------------------
# T-gen (MX-gen-order)
# ---------------------------------------------------------------------------


def test_gen_stale_generation_is_checked_before_the_epoch(tmp_path: Path) -> None:
    """Both the generation check and the epoch check fail in this
    construction, so only their ORDER decides the outcome.

    T-gen -> at the raised exception: correct = `StaleGenerationError` (the
    generation check runs first, so a caller whose database was replaced
    fails loudly instead of quietly recomputing against a database that is
    not the one it read from), mutant (MX-gen-order, the epoch check placed
    first) = `EPOCH_MOVED`, a value the caller would treat as "recompute",
    not "this is a different database now".
    T-gen -> at `ran`: correct = [] (`fn` never runs on either check
    failing), mutant = [] as well here -- this assertion alone does not
    distinguish the mutant; it is paired with the exception-type assertion
    above, which does.
    """
    db = _init_tmp_db(tmp_path)
    ran: list[int] = []

    async def _run() -> None:
        with pytest.raises(StaleGenerationError):
            await db.write_if_current(
                lambda c: ran.append(1),
                generation=db.generation + 1,
                epoch=db.input_epoch + 1,
            )

    asyncio.run(_run())

    assert ran == []


# ---------------------------------------------------------------------------
# T-M5 (M5)
# ---------------------------------------------------------------------------


def test_m5_read_at_epoch_reads_the_epoch_before_the_snapshot(tmp_path: Path) -> None:
    """T-M5 -> at the `write_if_current` result: correct = `EPOCH_MOVED`
    (`read_at_epoch` reads the epoch BEFORE submitting the snapshot read, so
    a write that lands while the snapshot is in flight is not covered by the
    epoch the later CAS checks against), mutant (M5, the epoch read moved to
    AFTER the snapshot returns) = a `tuple`, because the mutant's epoch read
    would already include the write that happened during the snapshot.
    T-M5 -> at `wait_ok`: correct = `True` (the snapshot must actually have
    started -- `snap_taken` must be set -- before the write is issued, or
    the whole rendezvous this test relies on never happened and the result
    below would be vacuously true). Not a mutant-killing assertion; a
    controlled-precondition check that stops the test with a clear reason
    if the interleaving it depends on failed to occur.
    T-M5 -> at `count`: correct = 0 (the snapshot's own row count was taken
    strictly before the INSERT landed, so it must see zero rows), mutant
    (a `read_at_epoch`/snapshot ordering bug that lets the write's row
    become visible to the gated read) = 1. Also not itself one of M5's
    listed kills, but a direct proof that the snapshot really executed
    before the write, independent of the epoch value.
    """
    db = _init_tmp_db(tmp_path)
    _make_probe(db)
    snap_taken = threading.Event()
    release = threading.Event()

    def _gated(conn: sqlite3.Connection) -> int:
        count = conn.execute("SELECT count(*) FROM _probe").fetchone()[0]
        snap_taken.set()
        if not release.wait(timeout=5.0):
            raise TimeoutError("release was never set")
        return int(count)

    writer = FencedWriter(db, db.generation)

    async def _run() -> object:
        task = asyncio.ensure_future(writer.read_at_epoch(_gated, label="t"))
        try:
            wait_ok = await asyncio.to_thread(snap_taken.wait, 5)
            assert wait_ok, "snap_taken was never set before the write"
            await db.write(lambda c: c.execute("INSERT INTO _probe(a) VALUES (1)"))
        finally:
            # Every exit path -- success, the wait_ok assertion, or a
            # db.write failure -- must release the gated read and consume
            # the task so no pending task is ever left behind.
            # Suppressing the task's own exception here is safe either way:
            # on the success path, `task.result()` right below re-raises it;
            # on a try-block failure, the original exception still
            # propagates once this cleanup finishes, regardless of the
            # task's outcome.
            release.set()
            with contextlib.suppress(BaseException):
                await task
        epoch, count = task.result()
        assert count == 0
        return await writer.write_if_current(lambda c: None, epoch=epoch)

    result = asyncio.run(_run())

    assert result is EPOCH_MOVED


# ---------------------------------------------------------------------------
# T-label (MX-label)
# ---------------------------------------------------------------------------


def test_label_slow_write_log_names_the_callback_not_the_wrapper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T-label -> at each "slow db write" record's message: correct =
    contains "_named_probe_write:" and neither "_body" nor "_call" (the
    label comes from the caller's own callback, via `_read_label`, not from
    `_run_epoch_txn`'s internal wrapper closures), mutant (MX-label, the
    label taken from the wrapper instead) = contains "_body:" or "_call:"
    and never "_named_probe_write:".
    """
    caplog.set_level(logging.INFO, logger="wxverify.db.connection")
    monkeypatch.setattr("wxverify.db.connection.SLOW_WRITE_MS", 0.0)
    db = _init_tmp_db(tmp_path)
    _make_probe(db)

    async def _run() -> None:
        await db.write(_named_probe_write)
        await db.write_if_current(
            _named_probe_write, generation=db.generation, epoch=db.input_epoch
        )

    asyncio.run(_run())

    slow_records = [
        r for r in caplog.records if r.getMessage().startswith("slow db write")
    ]
    assert len(slow_records) == 2
    for record in slow_records:
        message = record.getMessage()
        assert "_named_probe_write:" in message
        assert "_body" not in message
        assert "_call" not in message


# ---------------------------------------------------------------------------
# T-ros (the query_only reset)
# ---------------------------------------------------------------------------


def test_ros_query_only_reset_on_normal_exit_and_after_an_exception(
    tmp_path: Path,
) -> None:
    """T-ros -> at the first `query_only` read: correct = 1 (the pragma is
    ON for the duration of the `read_only_snapshot` block), mutant (a
    `read_only_snapshot` that never sets it) = 0.
    T-ros -> at the INSERT under `query_only`: correct = raises
    `sqlite3.OperationalError` (a read-only connection refuses a write),
    mutant (the pragma never actually set) = the INSERT succeeds silently.
    T-ros -> at `query_only_after` and `query_only_after_exception`: correct
    = 0 both times (the reset in `read_only_snapshot`'s own `finally` runs
    whether the block exited normally or raised), mutant (a reset that only
    runs on the happy path) = 1 after the exception case.
    """
    db = _init_tmp_db(tmp_path)
    _make_probe(db)

    def _fn(conn: sqlite3.Connection) -> None:
        with read_only_snapshot(conn, label="t"):
            query_only = conn.execute("PRAGMA query_only").fetchone()[0]
            assert query_only == 1
            with pytest.raises(sqlite3.OperationalError):
                conn.execute("INSERT INTO _probe(a) VALUES (1)")
        query_only_after = conn.execute("PRAGMA query_only").fetchone()[0]
        assert query_only_after == 0

        try:
            with read_only_snapshot(conn, label="t2"):
                raise ValueError("boom")
        except ValueError:
            pass
        query_only_after_exception = conn.execute("PRAGMA query_only").fetchone()[0]
        assert query_only_after_exception == 0

    async def _run() -> None:
        await db.read(_fn)

    asyncio.run(_run())

    assert_read_pool_at_rest(db)
