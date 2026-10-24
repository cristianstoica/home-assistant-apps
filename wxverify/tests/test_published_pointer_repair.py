"""Pinning tests for the published-pointer repair (#112, plan
§5.2-§5.3): a site re-added on an earlier version, on a reused id, can be
left pointing at a time zone generation that no longer exists (a
"dangling" pointer). ``repair_dangling_published_pointers`` re-seeds those
sites on every open; ``invalid_published_pointers`` reports every other bad
pointer without touching it.

All fixture data is synthetic -- this is a PUBLIC repo.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Callable
from pathlib import Path

import pytest

from wxverify.db import migrations, tz_generations
from wxverify.db.connection import Database
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import get_runtime_state, set_runtime_state
from wxverify.db.tz_generations import (
    apply_prospective_change,
    ensure_published_generation,
    invalid_published_pointers,
    published_pointer_key,
    repair_dangling_published_pointers,
    start_retrospective_correction,
)

_EFFECTIVE = "2026-01-01T00:00:00Z"
_OTHER_TZ = "Etc/GMT-3"
_PREFIX = "published-pointer repair"


def _conn(*, isolation_level: str | None = "") -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", isolation_level=isolation_level)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES (?, 0.0, 0.0, 0.0, 'UTC', 1)",
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _pointer(conn: sqlite3.Connection, site_id: int) -> str | None:
    return get_runtime_state(conn, published_pointer_key(site_id))


def _gen_row(conn: sqlite3.Connection, generation_id: int) -> tuple[object, ...] | None:
    row = conn.execute(
        "SELECT site_id, mode, state, timezone, effective_from, effective_to"
        " FROM timezone_generations WHERE id = ?",
        (generation_id,),
    ).fetchone()
    return None if row is None else tuple(row)


def _gen_count(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM timezone_generations").fetchone()[0])


def _replay_pre_0167_delete(conn: sqlite3.Connection, site_id: int) -> None:
    """Replays what a 0.16.6 delete did to the pointer: removed the site
    (cascading away its generations) but left the ``runtime_state`` pointer
    row behind untouched."""
    pointer_before = _pointer(conn, site_id)
    assert pointer_before is not None
    cur = conn.execute("DELETE FROM sites WHERE id = ?", (site_id,))
    assert cur.rowcount == 1
    assert _pointer(conn, site_id) == pointer_before, (
        "precondition: a 0.16.6 delete leaves the pointer -- rebuild, don't weaken"
    )


def _replay_pre_0167_create(
    conn: sqlite3.Connection, name: str, expected_id: int
) -> int:
    """Replays what a 0.16.6 create did on a reused id: inserted the new
    site and seeded through ``ensure_published_generation``, which trusted
    whatever pointer it inherited instead of checking it."""
    gen_count_before = _gen_count(conn)
    site_id = _make_site(conn, name)
    assert site_id == expected_id, (
        "precondition: the deleted id is reused -- rebuild, don't weaken"
    )
    inherited_pointer = _pointer(conn, site_id)
    assert inherited_pointer is not None
    ensure_published_generation(conn, site_id)
    assert _pointer(conn, site_id) == inherited_pointer, (
        "precondition: ensure trusts the inherited pointer -- rebuild, don't weaken"
    )
    assert _gen_count(conn) == gen_count_before, (
        "precondition: ensure trusts the inherited pointer -- rebuild, don't weaken"
    )
    return site_id


def _plant(path: Path, fn: Callable[[sqlite3.Connection], None]) -> None:
    db = Database(str(path))
    try:
        conn = db._conn  # noqa: SLF001 - building fixture content directly
        fn(conn)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        db.close()


def _repair_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        r
        for r in caplog.records
        if r.levelno >= logging.WARNING and r.getMessage().startswith(_PREFIX)
    ]


# ---------------------------------------------------------------------------
# R1-R6: the two pure functions, called directly.
# ---------------------------------------------------------------------------


def test_r1_dangling_pointer_below_the_highest_generation_id_is_repaired() -> None:
    conn = _conn()
    y = _make_site(conn, "Testsite Y")
    x = _make_site(conn, "Testsite X")
    assert ensure_published_generation(conn, y) == 1
    assert ensure_published_generation(conn, x) == 2
    assert apply_prospective_change(conn, y, _OTHER_TZ, _EFFECTIVE) == 3

    _replay_pre_0167_delete(conn, x)
    _replay_pre_0167_create(conn, "Testsite X2", x)

    assert _gen_row(conn, x) is None
    with pytest.raises(RuntimeError, match="published generation 2 missing"):
        apply_prospective_change(conn, x, _OTHER_TZ, _EFFECTIVE)

    assert repair_dangling_published_pointers(conn) == [2]
    assert _pointer(conn, 2) == "4"
    assert _gen_row(conn, 4) == (2, "initial", "published", "UTC", None, None)
    assert _pointer(conn, 1) == "3"
    assert invalid_published_pointers(conn) == []

    count_before_idempotence = _gen_count(conn)
    assert repair_dangling_published_pointers(conn) == []
    assert _gen_count(conn) == count_before_idempotence

    assert apply_prospective_change(conn, 2, _OTHER_TZ, _EFFECTIVE) == 5


def test_r2_delete_the_highest_id_recreate_repair() -> None:
    conn = _conn()
    y = _make_site(conn, "Testsite Y")
    x = _make_site(conn, "Testsite X")
    assert ensure_published_generation(conn, y) == 1
    assert ensure_published_generation(conn, x) == 2

    _replay_pre_0167_delete(conn, x)
    _replay_pre_0167_create(conn, "Testsite X2", x)
    assert _gen_row(conn, 2) is None

    assert repair_dangling_published_pointers(conn) == [2]
    assert _pointer(conn, 2) == "2"
    assert _gen_row(conn, 2) == (2, "initial", "published", "UTC", None, None)
    assert invalid_published_pointers(conn) == []


def _r3_valid(conn: sqlite3.Connection) -> None:
    y = _make_site(conn, "Testsite Y")
    x = _make_site(conn, "Testsite X")
    ensure_published_generation(conn, y)
    ensure_published_generation(conn, x)


def _r3_aliased(conn: sqlite3.Connection) -> None:
    y = _make_site(conn, "Testsite Y")
    x = _make_site(conn, "Testsite X")
    ensure_published_generation(conn, y)
    ensure_published_generation(conn, x)
    _replay_pre_0167_delete(conn, x)
    assert apply_prospective_change(conn, y, _OTHER_TZ, _EFFECTIVE) == 2
    _replay_pre_0167_create(conn, "Testsite X2", x)
    assert _gen_row(conn, 2) is not None
    assert _gen_row(conn, 2)[0] == 1  # type: ignore[index]


def _r3_own_building(conn: sqlite3.Connection) -> None:
    y = _make_site(conn, "Testsite Y")
    x = _make_site(conn, "Testsite X")
    ensure_published_generation(conn, y)
    ensure_published_generation(conn, x)
    _replay_pre_0167_delete(conn, x)
    _replay_pre_0167_create(conn, "Testsite X2", x)
    assert start_retrospective_correction(conn, x, _OTHER_TZ) == 2
    row = _gen_row(conn, 2)
    assert row is not None
    assert row[:3] == (2, "retrospective_correction", "building")


def _r3_non_numeric(conn: sqlite3.Connection) -> None:
    y = _make_site(conn, "Testsite Y")
    ensure_published_generation(conn, y)
    x = _make_site(conn, "Testsite X")
    set_runtime_state(conn, published_pointer_key(x), "x")


def _r3_half_parsable(conn: sqlite3.Connection) -> None:
    y = _make_site(conn, "Testsite Y")
    x = _make_site(conn, "Testsite X")
    ensure_published_generation(conn, y)
    ensure_published_generation(conn, x)
    set_runtime_state(conn, published_pointer_key(x), "2.0")


@pytest.mark.parametrize(
    ("build", "expected_invalid"),
    [
        (_r3_valid, []),
        (_r3_aliased, [2]),
        (_r3_own_building, [2]),
        (_r3_non_numeric, [2]),
        (_r3_half_parsable, [2]),
    ],
    ids=["valid", "aliased", "own_building", "non_numeric", "half_parsable"],
)
def test_r3_nothing_but_a_dangling_pointer_is_repaired(
    build: Callable[[sqlite3.Connection], None], expected_invalid: list[int]
) -> None:
    conn = _conn()
    build(conn)

    site_ids = [int(row[0]) for row in conn.execute("SELECT id FROM sites ORDER BY id")]
    pointers_before = {site_id: _pointer(conn, site_id) for site_id in site_ids}
    count_before = _gen_count(conn)

    assert repair_dangling_published_pointers(conn) == []
    assert {site_id: _pointer(conn, site_id) for site_id in site_ids} == pointers_before
    assert _gen_count(conn) == count_before
    assert invalid_published_pointers(conn) == expected_invalid


def test_r4_an_orphan_pointer_is_left_alone() -> None:
    conn = _conn()
    set_runtime_state(conn, published_pointer_key(9), "99")

    y = _make_site(conn, "Testsite Y")
    x = _make_site(conn, "Testsite X")
    ensure_published_generation(conn, y)
    ensure_published_generation(conn, x)
    _replay_pre_0167_delete(conn, x)
    _replay_pre_0167_create(conn, "Testsite X2", x)

    assert repair_dangling_published_pointers(conn) == [2]
    assert get_runtime_state(conn, published_pointer_key(9)) == "99"
    assert invalid_published_pointers(conn) == []


def test_r5_dangling_pointer_with_own_published_generation_is_reported() -> None:
    conn = _conn()
    y = _make_site(conn, "Testsite Y")
    x = _make_site(conn, "Testsite X")
    assert ensure_published_generation(conn, y) == 1
    assert ensure_published_generation(conn, x) == 2
    assert apply_prospective_change(conn, y, _OTHER_TZ, _EFFECTIVE) == 3

    _replay_pre_0167_delete(conn, x)
    _replay_pre_0167_create(conn, "Testsite X2", x)

    cur = conn.execute(
        "INSERT INTO timezone_generations (site_id, timezone, mode, state)"
        " VALUES (2, 'UTC', 'initial', 'published')"
    )
    assert cur.lastrowid == 4
    count_before = _gen_count(conn)

    assert repair_dangling_published_pointers(conn) == []
    assert invalid_published_pointers(conn) == [2]
    assert _pointer(conn, 2) == "2"
    assert _gen_count(conn) == count_before


def test_r6_candidates_are_read_before_any_write() -> None:
    conn = _conn()
    y = _make_site(conn, "Testsite Y")
    a = _make_site(conn, "Testsite A")
    assert ensure_published_generation(conn, y) == 1
    assert ensure_published_generation(conn, a) == 2
    assert apply_prospective_change(conn, y, _OTHER_TZ, _EFFECTIVE) == 3

    b = _make_site(conn, "Testsite B")
    assert ensure_published_generation(conn, b) == 4

    _replay_pre_0167_delete(conn, b)
    _replay_pre_0167_delete(conn, a)
    _replay_pre_0167_create(conn, "Testsite A2", a)
    _replay_pre_0167_create(conn, "Testsite B2", b)

    assert _gen_row(conn, 2) is None
    assert _gen_row(conn, 4) is None

    assert repair_dangling_published_pointers(conn) == [2, 3]
    assert _pointer(conn, 2) == "4"
    assert _pointer(conn, 3) == "5"
    assert _gen_row(conn, 4) == (2, "initial", "published", "UTC", None, None)
    assert _gen_row(conn, 5) == (3, "initial", "published", "UTC", None, None)
    assert invalid_published_pointers(conn) == []


# ---------------------------------------------------------------------------
# W1-W3: through a real open (``Database`` / ``replace_from``).
# ---------------------------------------------------------------------------


def test_w1_a_reopen_repairs_the_dangling_pointer_and_logs_both_warnings(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / "w1.db"

    def fn(conn: sqlite3.Connection) -> None:
        y = _make_site(conn, "Testsite Y")
        x = _make_site(conn, "Testsite X")
        z = _make_site(conn, "Testsite Z")
        assert ensure_published_generation(conn, y) == 1
        assert ensure_published_generation(conn, x) == 2
        assert ensure_published_generation(conn, z) == 3
        _replay_pre_0167_delete(conn, z)
        assert apply_prospective_change(conn, y, _OTHER_TZ, _EFFECTIVE) == 3
        _replay_pre_0167_delete(conn, x)
        _replay_pre_0167_create(conn, "Testsite X2", x)
        _replay_pre_0167_create(conn, "Testsite Z2", z)

    _plant(path, fn)

    caplog.set_level(logging.WARNING, logger="wxverify.db.migrations")
    db = Database(str(path))
    try:
        x2_pointer = db.read_sync(lambda conn: _pointer(conn, 2))
        x2_row = db.read_sync(lambda conn: _gen_row(conn, 4))
        y_pointer = db.read_sync(lambda conn: _pointer(conn, 1))
        z2_pointer = db.read_sync(lambda conn: _pointer(conn, 3))
    finally:
        db.close()

    assert x2_pointer == "4"
    assert x2_row is not None
    assert x2_row[:3] == (2, "initial", "published")
    assert y_pointer == "3"
    assert z2_pointer == "3"

    records = _repair_records(caplog)
    assert len(records) == 2
    first, second = records
    assert first.levelno == logging.WARNING
    assert "site(s) [2]" in first.getMessage()
    assert "re-created" in first.getMessage()
    assert second.levelno == logging.WARNING
    assert "site(s) [3]" in second.getMessage()
    assert "left unchanged" in second.getMessage()


def test_w2_an_imports_reopen_repairs(tmp_path: Path) -> None:
    async def _run() -> tuple[str | None, tuple[object, ...] | None]:
        db = Database(str(tmp_path / "live.db"))
        try:
            import_path = tmp_path / "import.db"

            def fn(conn: sqlite3.Connection) -> None:
                y = _make_site(conn, "Testsite Y")
                x = _make_site(conn, "Testsite X")
                assert ensure_published_generation(conn, y) == 1
                assert ensure_published_generation(conn, x) == 2
                assert apply_prospective_change(conn, y, _OTHER_TZ, _EFFECTIVE) == 3
                _replay_pre_0167_delete(conn, x)
                _replay_pre_0167_create(conn, "Testsite X2", x)

            _plant(import_path, fn)
            await db.replace_from(import_path, tmp_path / "backup.db.bak")
            pointer = await db.read(lambda conn: _pointer(conn, 2))
            row = await db.read(lambda conn: _gen_row(conn, 4))
            return pointer, row
        finally:
            db.close()

    pointer, row = asyncio.run(_run())
    assert pointer == "4"
    assert row is not None
    assert row[:3] == (2, "initial", "published")


def test_w3_a_healthy_open_writes_nothing_and_warns_nothing(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="wxverify.db.migrations")
    path = tmp_path / "w3.db"

    def fn(conn: sqlite3.Connection) -> None:
        y = _make_site(conn, "Testsite Y")
        x = _make_site(conn, "Testsite X")
        assert ensure_published_generation(conn, y) == 1
        assert ensure_published_generation(conn, x) == 2

    _plant(path, fn)

    db = Database(str(path))
    try:
        y_pointer = db.read_sync(lambda conn: _pointer(conn, 1))
        x_pointer = db.read_sync(lambda conn: _pointer(conn, 2))
        gen_count = db.read_sync(_gen_count)
    finally:
        db.close()

    assert _repair_records(caplog) == []
    assert y_pointer == "1"
    assert x_pointer == "2"
    assert gen_count == 2


# ---------------------------------------------------------------------------
# F1-F4: failure paths.
# ---------------------------------------------------------------------------


def test_f1_a_failing_repair_rolls_back_as_a_whole_and_the_open_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="wxverify.db.migrations")
    path = tmp_path / "f1.db"

    def fn(conn: sqlite3.Connection) -> None:
        y = _make_site(conn, "Testsite Y")
        a = _make_site(conn, "Testsite A")
        b = _make_site(conn, "Testsite B")
        assert ensure_published_generation(conn, y) == 1
        assert ensure_published_generation(conn, a) == 2
        assert ensure_published_generation(conn, b) == 3
        assert apply_prospective_change(conn, y, _OTHER_TZ, _EFFECTIVE) == 4
        _replay_pre_0167_delete(conn, b)
        _replay_pre_0167_delete(conn, a)
        _replay_pre_0167_create(conn, "Testsite A2", a)
        _replay_pre_0167_create(conn, "Testsite B2", b)

    _plant(path, fn)

    calls: list[int] = []
    real_ensure = tz_generations.ensure_published_generation

    def _stub(conn: sqlite3.Connection, site_id: int) -> int:
        calls.append(site_id)
        if len(calls) == 1:
            return real_ensure(conn, site_id)
        raise RuntimeError("synthetic seed failure")

    monkeypatch.setattr(tz_generations, "ensure_published_generation", _stub)

    db = Database(str(path))
    try:
        assert calls == [2, 3]
        a_pointer = db.read_sync(lambda conn: _pointer(conn, 2))
        b_pointer = db.read_sync(lambda conn: _pointer(conn, 3))
        gen_count = db.read_sync(_gen_count)
        assert a_pointer == "2"
        assert b_pointer == "3"
        assert gen_count == 2

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(error_records) == 1
        assert (
            error_records[0].getMessage().startswith("published-pointer repair failed")
        )

        assert not db._conn.in_transaction  # noqa: SLF001
        assert db.write_sync(lambda conn: conn.execute("SELECT 1").fetchone()[0]) == 1
    finally:
        db.close()

    monkeypatch.undo()

    db2 = Database(str(path))
    try:
        a_pointer2 = db2.read_sync(lambda conn: _pointer(conn, 2))
        b_pointer2 = db2.read_sync(lambda conn: _pointer(conn, 3))
    finally:
        db2.close()

    assert a_pointer2 == "5"
    assert b_pointer2 == "6"


def test_f2_sqlites_own_rollback_does_not_break_the_unwind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="wxverify.db.migrations")
    path = tmp_path / "f2.db"

    def fn(conn: sqlite3.Connection) -> None:
        y = _make_site(conn, "Testsite Y")
        x = _make_site(conn, "Testsite X")
        assert ensure_published_generation(conn, y) == 1
        assert ensure_published_generation(conn, x) == 2
        assert apply_prospective_change(conn, y, _OTHER_TZ, _EFFECTIVE) == 3
        _replay_pre_0167_delete(conn, x)
        _replay_pre_0167_create(conn, "Testsite X2", x)

    _plant(path, fn)

    def _stub(conn: sqlite3.Connection, site_id: int) -> int:
        conn.execute("ROLLBACK")
        raise sqlite3.OperationalError("database or disk is full")

    monkeypatch.setattr(tz_generations, "ensure_published_generation", _stub)

    db = Database(str(path))
    try:
        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(error_records) == 1
        assert (
            error_records[0].getMessage().startswith("published-pointer repair failed")
        )

        x2_pointer = db.read_sync(lambda conn: _pointer(conn, 2))
        gen_count = db.read_sync(_gen_count)
        assert x2_pointer == "2"
        assert gen_count == 2
        assert not db._conn.in_transaction  # noqa: SLF001
        assert db.write_sync(lambda conn: conn.execute("SELECT 1").fetchone()[0]) == 1
    finally:
        db.close()


def test_f3_a_base_exception_unwinds_and_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _conn(isolation_level=None)
    y = _make_site(conn, "Testsite Y")
    x = _make_site(conn, "Testsite X")
    assert ensure_published_generation(conn, y) == 1
    assert ensure_published_generation(conn, x) == 2
    assert apply_prospective_change(conn, y, _OTHER_TZ, _EFFECTIVE) == 3
    _replay_pre_0167_delete(conn, x)
    _replay_pre_0167_create(conn, "Testsite X2", x)

    assert not conn.in_transaction
    gen_count_before = _gen_count(conn)
    real_ensure = tz_generations.ensure_published_generation

    def _stub(conn: sqlite3.Connection, site_id: int) -> int:
        result = real_ensure(conn, site_id)
        raise SystemExit(3)
        return result  # pragma: no cover - unreachable, for type-checking only

    monkeypatch.setattr(tz_generations, "ensure_published_generation", _stub)

    with pytest.raises(SystemExit):
        migrations.repair_published_pointers(conn)

    assert conn.in_transaction is False
    assert _pointer(conn, 2) == "2"
    assert _gen_count(conn) == gen_count_before


def test_f4_a_failed_commit_at_release_is_unwound_and_logged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="wxverify.db.migrations")
    path = tmp_path / "f4.db"

    def fn(conn: sqlite3.Connection) -> None:
        y = _make_site(conn, "Testsite Y")
        x = _make_site(conn, "Testsite X")
        assert ensure_published_generation(conn, y) == 1
        assert ensure_published_generation(conn, x) == 2
        assert apply_prospective_change(conn, y, _OTHER_TZ, _EFFECTIVE) == 3
        _replay_pre_0167_delete(conn, x)
        _replay_pre_0167_create(conn, "Testsite X2", x)

    _plant(path, fn)

    real_connect = sqlite3.connect
    state = {"released": False, "writer_opened": False}

    class FlakyConnection(sqlite3.Connection):
        def execute(self, sql: str, *args: object, **kwargs: object) -> sqlite3.Cursor:
            if sql == "RELEASE repair_published_pointers" and not state["released"]:
                state["released"] = True
                raise sqlite3.OperationalError("database is locked")
            return super().execute(sql, *args, **kwargs)  # type: ignore[arg-type]

    def fake_connect(
        db_path: object, *args: object, **kwargs: object
    ) -> sqlite3.Connection:
        if db_path == str(path) and not state["writer_opened"]:
            state["writer_opened"] = True
            kwargs["factory"] = FlakyConnection
        return real_connect(db_path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(sqlite3, "connect", fake_connect)

    db = Database(str(path))
    try:
        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(error_records) == 1
        assert (
            error_records[0].getMessage().startswith("published-pointer repair failed")
        )

        x2_pointer = db.read_sync(lambda conn: _pointer(conn, 2))
        gen_count = db.read_sync(_gen_count)
        assert x2_pointer == "2"
        assert gen_count == 2
        assert not db._conn.in_transaction  # noqa: SLF001
        assert db.write_sync(lambda conn: conn.execute("SELECT 1").fetchone()[0]) == 1
    finally:
        db.close()
