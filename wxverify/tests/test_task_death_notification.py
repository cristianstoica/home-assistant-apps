"""O1-O9 for task-death notification (0.14.0): the export sweeper's
death (external ``add_done_callback``, latched in ``db_transfer.py``,
surfaced as a ``CRITICAL`` log plus one monitor condition, group
``process``).

Every mutant a claim below names is verified against a real scratch rebind
of the production symbol (never a repo edit): the oracle FAILS with the
mutant applied and PASSES once it is reverted.

Mutant table:

| Mutant | Rebind | Oracle |
| --- | --- | --- |
| A   ``on_export_sweeper_done``'s ``cancelled()`` guard deleted            | O2 |
| B   swallows ``CancelledError`` from ``task.exception()`` into ``None``   | O2 |
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app, lifespan
from wxverify.api.routes.db_transfer import export_sweeper_death
from wxverify.db.connection import close_db, init_db
from wxverify.db.migrations import run_migrations
from wxverify.monitor import build_verdict

# ---------------------------------------------------------------------------
# Shared stubs
# ---------------------------------------------------------------------------


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


# ---------------------------------------------------------------------------
# Group A - sweeper detection
# ---------------------------------------------------------------------------


def test_o1_crash_latches_with_redacted_diagnosable_detail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crashing sweeper latches ``reason="crashed"`` with a URL-redacted,
    type-name-prefixed detail.

    O1 -> at ``death.detail``: correct =
    ``"RuntimeError: boom at https://api.example.com/v1?key=%2A%2A%2A"``,
    mutant (``str(exc)`` rendered directly, no type prefix, no redaction) =
    ``"boom at https://api.example.com/v1?key=abc"`` — fails both the
    equality and the ``"abc" not in death.detail`` negative.
    """
    close_db()
    config.db_path = str(tmp_path / "o1-crash.db")
    config.options_path = str(tmp_path / "o1-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)

    reached = asyncio.Event()

    async def _crashing_sweeper() -> None:
        reached.set()
        raise RuntimeError("boom at https://api.example.com/v1?key=abc")

    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper", _crashing_sweeper
    )
    app = create_app(root_path="")

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        await reached.wait()
        await asyncio.sleep(0)
        await cm.__aexit__(None, None, None)

    asyncio.run(_run())

    death = export_sweeper_death()
    assert death is not None
    assert death.reason == "crashed"
    assert (
        death.detail == "RuntimeError: boom at https://api.example.com/v1?key=%2A%2A%2A"
    )
    assert "abc" not in death.detail


def test_o1_zero_argument_crash_detail_never_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The zero-argument case: the class name alone is the detail -- non-empty,
    named exactly once, and with no URL to redact.

    O1 -> at ``death.detail``: correct = ``"RuntimeError"``, mutant
    (``str(exc)`` rendered directly with no type prefix) = ``""``; mutant M5
    (the class name prefixed unconditionally over ``sanitized_exception``'s
    own class-name fallback) = ``"RuntimeError: RuntimeError"``.
    """
    close_db()
    config.db_path = str(tmp_path / "o1-zeroarg.db")
    config.options_path = str(tmp_path / "o1-zeroarg-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)

    reached = asyncio.Event()

    async def _crashing_sweeper() -> None:
        reached.set()
        raise RuntimeError

    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper", _crashing_sweeper
    )
    app = create_app(root_path="")

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        await reached.wait()
        await asyncio.sleep(0)
        await cm.__aexit__(None, None, None)

    asyncio.run(_run())

    death = export_sweeper_death()
    assert death is not None
    assert death.detail == "RuntimeError"


def test_o2_clean_shutdown_does_not_latch_and_callback_never_raises_into_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A real sweeper task, cancelled at an ordinary shutdown, must not latch
    a death, must not log CRITICAL, and must not raise into the event loop's
    exception handler.

    The single most important oracle here: mutant A (the
    ``task.cancelled()`` guard deleted) latches nothing and logs nothing —
    the first two assertions below stay green under it — and is caught only
    by the third.

    O2 -> at ``handled``: correct = ``[]``, mutant A (guard deleted; the
    resulting ``Task.exception()`` call raises ``CancelledError`` from
    inside the done-callback, which the loop's exception handler records) =
    a one-element list. Mutant B (swallows the ``CancelledError`` from
    ``task.exception()`` into a treated-as-``None`` result instead of
    skipping cancellation) is caught separately, at
    ``export_sweeper_death() is None``: correct = ``True``, mutant =
    ``False`` (reason ``"returned"`` latched on every clean shutdown).
    """
    close_db()
    config.db_path = str(tmp_path / "o2-clean.db")
    config.options_path = str(tmp_path / "o2-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")

    handled: list[dict[str, object]] = []

    async def _run() -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, ctx: handled.append(ctx))
        cm = lifespan(app)
        await cm.__aenter__()
        await cm.__aexit__(None, None, None)

    with caplog.at_level(logging.CRITICAL, logger="wxverify.api.routes.db_transfer"):
        asyncio.run(_run())

    assert export_sweeper_death() is None
    assert [r for r in caplog.records if r.levelno == logging.CRITICAL] == []
    assert handled == []


def test_o3_normal_return_latches_reason_returned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A sweeper coroutine that returns instead of looping forever latches
    ``reason="returned"`` with an empty detail.

    O3 -> at ``death.reason``: correct = ``"returned"``, mutant (a callback
    that treats a ``None`` from ``task.exception()`` as healthy and never
    latches) leaves ``export_sweeper_death()`` at ``None``.
    """
    close_db()
    config.db_path = str(tmp_path / "o3-returned.db")
    config.options_path = str(tmp_path / "o3-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)

    reached = asyncio.Event()

    async def _returning_sweeper() -> None:
        reached.set()

    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper", _returning_sweeper
    )
    app = create_app(root_path="")

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        await reached.wait()
        await asyncio.sleep(0)
        await cm.__aexit__(None, None, None)

    asyncio.run(_run())

    death = export_sweeper_death()
    assert death is not None
    assert death.reason == "returned"
    assert death.detail == ""


def test_o4_crash_log_is_critical_with_exc_info(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The crash log fires at CRITICAL, on the ``db_transfer`` logger, with a
    real ``exc_info`` triple attached.

    O4 -> at ``rec.levelno``: correct = ``logging.CRITICAL``, mutant
    (downgraded to ``warning``/``error``) = ``logging.WARNING`` /
    ``logging.ERROR``. Paired: ``rec.exc_info is not None`` and
    ``isinstance(rec.exc_info[1], RuntimeError)`` kill a mutant that drops
    the ``exc_info=`` triple, which would leave ``rec.exc_info`` as
    ``(None, None, None)``.
    """
    close_db()
    config.db_path = str(tmp_path / "o4-critical.db")
    config.options_path = str(tmp_path / "o4-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)

    reached = asyncio.Event()

    async def _crashing_sweeper() -> None:
        reached.set()
        raise RuntimeError("o4 boom")

    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper", _crashing_sweeper
    )
    app = create_app(root_path="")

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        await reached.wait()
        await asyncio.sleep(0)
        await cm.__aexit__(None, None, None)

    with caplog.at_level(logging.CRITICAL, logger="wxverify.api.routes.db_transfer"):
        asyncio.run(_run())

    recs = [
        r
        for r in caplog.records
        if r.name == "wxverify.api.routes.db_transfer"
        and r.getMessage() == "export sweeper crashed"
    ]
    assert len(recs) == 1, [r.getMessage() for r in caplog.records]
    rec = recs[0]
    assert rec.levelno == logging.CRITICAL
    assert rec.exc_info is not None
    assert isinstance(rec.exc_info[1], RuntimeError)


def test_o5_callback_never_raises_when_rendering_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pathological ``__str__`` degrades to the class name alone and the
    latch still lands; nothing propagates out of the done-callback.

    O5 -> at ``death``: correct = a latched ``SweeperDeath`` with
    ``detail == "BoomError"`` (class-name-only degradation via
    ``safe_detail``'s guard), mutant (an unwrapped ``sanitized_exception``
    call, i.e. not routing through ``safe_detail``'s try/except) = ``None``
    — the render raises before the assignment line runs, so the exception
    reaches the loop's default (not this test's) handler and the latch
    never happens; ``asyncio.run`` itself still returns normally either
    way, since a done-callback's exception never propagates through it.
    """
    close_db()
    config.db_path = str(tmp_path / "o5-badstr.db")
    config.options_path = str(tmp_path / "o5-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)

    class BoomError(RuntimeError):
        def __str__(self) -> str:
            raise ValueError("str exploded")

    reached = asyncio.Event()

    async def _crashing_sweeper() -> None:
        reached.set()
        raise BoomError("unrendered")

    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper", _crashing_sweeper
    )
    app = create_app(root_path="")

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        await reached.wait()
        await asyncio.sleep(0)
        await cm.__aexit__(None, None, None)

    asyncio.run(_run())  # must not raise

    death = export_sweeper_death()
    assert death is not None
    assert death.reason == "crashed"
    assert death.detail == "BoomError"


# ---------------------------------------------------------------------------
# Group B - sweeper surface
# ---------------------------------------------------------------------------


def test_o6_verdict_clean_when_export_sweeper_alive() -> None:
    """``export_sweeper_dead=None`` composes a green, detail-omitted
    condition that does not push ``overall`` off ``ok``.

    O6 -> at ``cond["ok"]``: correct = ``True``, mutant (an inverted
    condition, ``ok=export_sweeper_dead is not None``) = ``False``. Paired:
    ``"detail" not in cond`` kills a mutant that ships ``detail: null``
    instead of omitting the key (the ``as_dict`` contract at
    ``monitor.py:69-72``).
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    verdict = build_verdict(
        conn,
        pipeline_enabled=False,
        budget_enabled=False,
        db_enabled=False,
        now=datetime.now(UTC),
        export_sweeper_dead=None,
    )
    conditions = verdict["conditions"]
    assert isinstance(conditions, list)
    cond = next(c for c in conditions if c["id"] == "export_sweeper_dead")
    assert cond["ok"] is True
    assert cond["skipped"] is False
    assert "detail" not in cond
    assert verdict["overall"] == "ok"


def test_o7_verdict_trips_when_latched() -> None:
    """A rendered, non-``None`` death line trips the condition and folds
    into ``overall``.

    O7 -> at ``cond["severity"]``: correct = ``"critical"``, mutant (a
    ``"warning"`` severity typo) = ``"warning"`` — which also fails
    ``verdict["overall"] == "critical"`` since a warning-severity condition
    cannot raise ``overall`` past a pre-existing ``ok``.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    verdict = build_verdict(
        conn,
        pipeline_enabled=False,
        budget_enabled=False,
        db_enabled=False,
        now=datetime.now(UTC),
        export_sweeper_dead=(
            "export sweeper crashed at 2026-09-01T00:00:00Z: RuntimeError: boom"
        ),
    )
    conditions = verdict["conditions"]
    assert isinstance(conditions, list)
    cond = next(c for c in conditions if c["id"] == "export_sweeper_dead")
    assert cond["ok"] is False
    assert cond["severity"] == "critical"
    assert cond["group"] == "process"
    assert verdict["overall"] == "critical"


def test_o8_route_plumbs_the_latch_and_composes_the_crashed_detail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``/api/health/monitor`` reads the real latch and composes the
    operator-facing line itself — the one seam a ``build_verdict``-only unit
    test cannot see.

    O8 -> at ``cond["detail"]``: correct contains
    ``"RuntimeError: "`` and ``"key=%2A%2A%2A"`` and starts with
    ``"export sweeper crashed at "``, mutant (the route composes the detail
    from the timestamp alone, dropping ``death.detail``) = a detail string
    with no ``"RuntimeError: "`` segment at all — a regression that passes
    O1, O6, O7 and O9 unchanged, which is why this assertion has to live
    here.
    """
    close_db()
    config.db_path = str(tmp_path / "o8-crashed.db")
    config.options_path = str(tmp_path / "o8-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)

    async def _crashing_sweeper() -> None:
        raise RuntimeError("boom at https://api.example.com/v1?key=abc")

    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper", _crashing_sweeper
    )
    app = create_app(root_path="")
    with TestClient(app) as client:
        body = client.get("/api/health/monitor").json()

    conditions = body["conditions"]
    assert isinstance(conditions, list)
    cond = next(c for c in conditions if c["id"] == "export_sweeper_dead")
    assert cond["ok"] is False
    detail = cond["detail"]
    assert isinstance(detail, str)
    assert detail.startswith("export sweeper crashed at ")
    assert "RuntimeError: " in detail
    assert "key=%2A%2A%2A" in detail
    assert "abc" not in detail


def test_o8_returned_detail_is_exact_with_no_trailing_colon(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ``returned`` path composes an exact line, with no trailing
    ``": "`` — pinning the empty-detail branch.

    O8 -> at ``cond["detail"]``: correct =
    ``f"export sweeper returned unexpectedly at {death.at}"``, mutant
    (unconditionally appending ``": "`` after the timestamp regardless of
    ``reason``) = the same string with a trailing ``": "``.
    """
    close_db()
    config.db_path = str(tmp_path / "o8-returned.db")
    config.options_path = str(tmp_path / "o8-returned-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)

    async def _returning_sweeper() -> None:
        return None

    monkeypatch.setattr(
        "wxverify.api.routes.db_transfer.run_export_sweeper", _returning_sweeper
    )
    app = create_app(root_path="")
    with TestClient(app) as client:
        body = client.get("/api/health/monitor").json()

    death = export_sweeper_death()
    assert death is not None
    assert death.reason == "returned"
    conditions = body["conditions"]
    assert isinstance(conditions, list)
    cond = next(c for c in conditions if c["id"] == "export_sweeper_dead")
    assert cond["detail"] == f"export sweeper returned unexpectedly at {death.at}"


def test_o9_condition_survives_every_group_toggled_off() -> None:
    """The condition is evaluated outside every ``try`` and every toggle: it
    survives all three monitor groups disabled.

    O9 -> at ``verdict["overall"]``: correct = ``"critical"``, mutant (the
    condition placed inside one of the toggled ``else`` branches, e.g.
    ``if pipeline_enabled: ... condition appended``) = ``"ok"``, since with
    every group disabled the condition would never be appended at all.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    verdict = build_verdict(
        conn,
        pipeline_enabled=False,
        budget_enabled=False,
        db_enabled=False,
        now=datetime.now(UTC),
        export_sweeper_dead=(
            "export sweeper returned unexpectedly at 2026-09-01T00:00:00Z"
        ),
    )
    cond = next(c for c in verdict["conditions"] if c["id"] == "export_sweeper_dead")
    assert cond["ok"] is False
    assert verdict["overall"] == "critical"


def test_o9_condition_survives_a_db_readable_sqlite_error(tmp_path: Path) -> None:
    """The condition also survives a genuine ``sqlite3.Error`` short-circuit
    on the db group.

    O9 -> at ``verdict["overall"]``: correct = ``"critical"`` even though
    the db-group read raised, mutant (the condition appended only inside a
    ``try`` that a ``sqlite3.Error`` unwinds through, e.g. nested under the
    db-group ``try``) = the condition dropped from ``conditions`` entirely,
    with ``overall`` still ``"critical"`` from ``db_readable`` alone but no
    ``export_sweeper_dead`` entry to find — the ``next(...)`` call below
    raises ``StopIteration``.
    """
    close_db()
    db = init_db(str(tmp_path / "o9-db-error.db"))
    conn = db._conn  # noqa: SLF001
    conn.close()  # any further query now raises sqlite3.ProgrammingError

    verdict = build_verdict(
        conn,
        pipeline_enabled=False,
        budget_enabled=False,
        db_enabled=True,
        now=datetime.now(UTC),
        export_sweeper_dead=(
            "export sweeper returned unexpectedly at 2026-09-01T00:00:00Z"
        ),
    )
    cond = next(c for c in verdict["conditions"] if c["id"] == "export_sweeper_dead")
    assert cond["ok"] is False
    db_cond = next(c for c in verdict["conditions"] if c["id"] == "db_readable")
    assert db_cond["ok"] is False
    assert verdict["overall"] == "critical"
