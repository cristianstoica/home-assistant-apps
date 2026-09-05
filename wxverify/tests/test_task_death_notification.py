"""O1-O15 (incl. O12b) and O17 for task-death notification (0.14.0 plan).

Two independently-detected facts, two independent surfaces:

* the export sweeper's death (external ``add_done_callback``, latched in
  ``db_transfer.py``, surfaced as a ``CRITICAL`` log plus one monitor
  condition, group ``process``);
* the read-cache warm's outcome (internal self-report inside
  ``warm_read_cache``, surfaced only at ``/api/worker/status``).

Every mutant a claim below names is verified against a real scratch rebind
of the production symbol (never a repo edit, except the two placement
mutants noted below, each run under its own ``PYTHONPYCACHEPREFIX`` and
restored with an md5-verified diff): the oracle FAILS with the mutant
applied and PASSES once it is reverted. That run is captured in the
dispatching QA report, not asserted here as prose.

Mutant table:

| Mutant | Rebind | Oracle |
| --- | --- | --- |
| A   ``on_export_sweeper_done``'s ``cancelled()`` guard deleted            | O2 |
| B   swallows ``CancelledError`` from ``task.exception()`` into ``None``   | O2 |
| M1  ``detail=sanitized_exception(exc)`` unguarded (case 3 driver)          | O11 |
| M2  same bare rendering (case 2 driver)                                   | O11 |
| M3  the ``running`` write moved outside the ``try`` (source edit)         | O11 |
| M4  the ``failed``-write guard deleted, ``safe_detail`` kept (source edit) | O11 |

O16 (the four-file ``_BASE_WORKER_STATUS_KEYS`` / autouse-reset migration)
and O18/O19 (unmodified re-runs / cross-test isolation) are out of scope for
this file; they are already satisfied elsewhere.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest
from fastapi.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app, lifespan
from wxverify.api.routes.db_transfer import export_sweeper_death
from wxverify.db.connection import FencedWriter, close_db, init_db
from wxverify.monitor import build_verdict
from wxverify.verification import read_cache as rc

# ---------------------------------------------------------------------------
# Shared stubs
# ---------------------------------------------------------------------------


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


async def _no_warm(db: object) -> None:
    return None


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
    monkeypatch.setattr("wxverify.api.app.warm_read_cache", _no_warm)

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
    """The zero-argument case: the type-name prefix keeps the detail
    non-empty and carries no URL to redact.

    O1 -> at ``death.detail``: correct = ``"RuntimeError: "``, mutant
    (``str(exc)`` rendered directly with no type prefix) = ``""``.
    """
    close_db()
    config.db_path = str(tmp_path / "o1-zeroarg.db")
    config.options_path = str(tmp_path / "o1-zeroarg-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    monkeypatch.setattr("wxverify.api.app.warm_read_cache", _no_warm)

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
    assert death.detail == "RuntimeError: "


def test_o2_clean_shutdown_does_not_latch_and_callback_never_raises_into_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A real sweeper task, cancelled at an ordinary shutdown, must not latch
    a death, must not log CRITICAL, and must not raise into the event loop's
    exception handler.

    The single most important oracle in this plan: mutant A (the
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
    ``safe_detail``'s guard), mutant (F4's unwrapped ``sanitized_exception``
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
    conn = cast(sqlite3.Connection, None)
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
    conn = cast(sqlite3.Connection, None)
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
    ``": "`` — pinning the empty-detail branch of D4.

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
    conn = cast(sqlite3.Connection, None)
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


# ---------------------------------------------------------------------------
# Group C - warm self-report
# ---------------------------------------------------------------------------


def test_o10_successful_warm_records_ok(tmp_path: Path) -> None:
    """A clean warm (no published targets at all) records ``ok`` with zero
    failed derivations.

    O10 -> at ``outcome.state``: correct = ``"ok"``, mutant (state never
    advanced past ``running``, e.g. the terminal ``_note_warm`` call
    deleted) = ``"running"``.
    """
    close_db()
    db = init_db(str(tmp_path / "o10-ok.db"))
    assert rc.warm_outcome() is None
    asyncio.run(rc.warm_read_cache(db))
    outcome = rc.warm_outcome()
    assert outcome is not None
    assert outcome.state == "ok"
    assert outcome.derivations_failed == 0


def test_o11_case1_published_targets_crash_records_failed_with_redacted_detail(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Case 1: a crash inside the snapshot step records ``failed`` with a
    declared, redacted detail, and the existing failure log still fires
    verbatim.

    O11 -> at ``outcome.detail``: correct =
    ``"RuntimeError: boom at https://api.example.com/v1?key=%2A%2A%2A"``,
    mutant M1 (``detail=sanitized_exception(exc)`` unguarded — irrelevant
    here since ``sanitized_exception`` alone still renders fine for a
    normal ``__str__``, so this case does not discriminate M1 by itself;
    see case 3) would instead drop the type-name prefix, giving
    ``"boom at https://api.example.com/v1?key=%2A%2A%2A"``.
    """
    close_db()
    db = init_db(str(tmp_path / "o11-case1.db"))

    def _boom(conn: sqlite3.Connection) -> list[tuple[int, int]]:
        raise RuntimeError("boom at https://api.example.com/v1?key=abc")

    monkeypatch.setattr(rc, "_published_targets", _boom)
    with caplog.at_level(logging.ERROR, logger="wxverify.verification.read_cache"):
        result = asyncio.run(rc.warm_read_cache(db))
    assert result is None

    outcome = rc.warm_outcome()
    assert outcome is not None
    assert outcome.state == "failed"
    assert (
        outcome.detail
        == "RuntimeError: boom at https://api.example.com/v1?key=%2A%2A%2A"
    )
    assert "abc" not in outcome.detail
    assert any(r.getMessage() == "read-cache warm failed" for r in caplog.records)


def test_o11_case2_zero_argument_exception_detail_never_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 2: a zero-argument exception still yields a non-empty,
    type-name-prefixed detail.

    O11 -> at ``outcome.detail``: correct = ``"RuntimeError: "``, mutant M2
    (``detail=sanitized_exception(exc)`` unguarded, i.e. no type-name
    prefix) = ``""``.
    """
    close_db()
    db = init_db(str(tmp_path / "o11-case2.db"))

    def _boom(conn: sqlite3.Connection) -> list[tuple[int, int]]:
        raise RuntimeError

    monkeypatch.setattr(rc, "_published_targets", _boom)
    asyncio.run(rc.warm_read_cache(db))
    outcome = rc.warm_outcome()
    assert outcome is not None
    assert outcome.state == "failed"
    assert outcome.detail == "RuntimeError: "


def test_o11_case3_pathological_str_never_escapes_and_never_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 3: an exception whose ``__str__`` itself raises still degrades
    to the class-name-only detail, and ``warm_read_cache`` never raises.

    O11 -> at ``outcome.state``: correct = ``"failed"`` (``safe_detail``'s
    own guard degrades the unrenderable exception to ``"BoomError"`` before
    ``_note_warm("failed", ...)`` is called), mutant M1
    (``detail=sanitized_exception(exc)`` unguarded, swapped into the SAME
    inner ``try`` that already surrounds this ``_note_warm`` call) =
    ``"running"`` — the render itself raises while the argument is being
    evaluated, so the inner ``except`` catches it before ``_note_warm``'s
    body ever runs, and the ``"running"`` write from entry is never
    overwritten. ``result is None`` still holds under both, since the outer
    handler swallows the render failure either way — it is not the
    discriminator.
    """
    close_db()
    db = init_db(str(tmp_path / "o11-case3.db"))

    class BoomError(RuntimeError):
        def __str__(self) -> str:
            raise ValueError("str exploded")

    def _boom(conn: sqlite3.Connection) -> list[tuple[int, int]]:
        raise BoomError("unrendered")

    monkeypatch.setattr(rc, "_published_targets", _boom)
    result = asyncio.run(rc.warm_read_cache(db))
    assert result is None

    outcome = rc.warm_outcome()
    assert outcome is not None
    assert outcome.state == "failed"
    assert outcome.detail == "BoomError"


def test_o11_case4_running_write_placement_survives_a_first_call_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Case 4: the entry-time ``running`` write raising does not escape,
    because it sits inside the total ``try``; the guarded ``failed`` write
    that follows (the second ``_note_warm`` call, unaffected by the stub by
    then) still lands.

    O11 -> at ``outcome.state``: correct = ``"failed"`` with ``calls["n"]
    == 2``, mutant M3 (the ``running`` write moved outside the ``try``) lets
    the first-call raise escape ``warm_read_cache`` entirely, so
    ``asyncio.run(...)`` raises instead of returning ``None``. (M3 is
    verified separately via a source edit under its own
    ``PYTHONPYCACHEPREFIX``, restored and md5-checked — see the release's
    mutation report.)
    """
    close_db()
    db = init_db(str(tmp_path / "o11-case4.db"))

    calls = {"n": 0}
    real_note_warm = rc._note_warm

    def _counted_note_warm(*args: object, **kwargs: object) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("first _note_warm call fails")
        real_note_warm(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(rc, "_note_warm", _counted_note_warm)
    result = asyncio.run(rc.warm_read_cache(db))
    assert result is None

    outcome = rc.warm_outcome()
    assert outcome is not None
    assert outcome.state == "failed"
    assert calls["n"] == 2


def test_o11_case5_always_raising_writer_never_escapes_and_logs_the_guard(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Case 5: an outcome writer that raises on every call still never
    escapes ``warm_read_cache``, and the inner guard's own failure log
    fires. Deliberately no assertion on ``warm_outcome().state``: with an
    always-raising writer the entry-time ``running`` write is call 1 and it
    raises too, so nothing is ever recorded.

    O11 -> at ``result``: correct = ``None`` (this is the control: the
    guard, present, still lets ``warm_read_cache`` return normally), mutant
    M4 (the inner ``try``/``except`` around the ``failed`` write deleted)
    lets the second ``_note_warm`` raise escape the outer handler, so
    ``asyncio.run(...)`` raises instead of returning ``None``. (M4 is
    verified separately via a source edit under its own
    ``PYTHONPYCACHEPREFIX``, restored and md5-checked — see the release's
    mutation report.)
    """
    close_db()
    db = init_db(str(tmp_path / "o11-case5.db"))

    def _always_raising(*args: object, **kwargs: object) -> None:
        raise RuntimeError("note_warm always fails")

    monkeypatch.setattr(rc, "_note_warm", _always_raising)
    with caplog.at_level(logging.ERROR, logger="wxverify.verification.read_cache"):
        result = asyncio.run(rc.warm_read_cache(db))
    assert result is None
    assert any(
        r.getMessage() == "read-cache warm: recording the outcome failed"
        for r in caplog.records
    )


def test_o12_never_finished_warm_stays_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``BaseException`` leaving the frame (the class cancellation
    belongs to) writes no terminal state at all: the outcome stays
    ``running``.

    O12 -> at ``outcome.state``: correct = ``"running"``, mutant (an
    unconditional ``finally: _note_warm("ok")``) = ``"ok"`` — erasing
    exactly the signal this plan exists to create.
    """
    close_db()
    db = init_db(str(tmp_path / "o12-neverfinished.db"))

    class _Fatal(BaseException):
        pass

    def _boom(*args: object, **kwargs: object) -> None:
        raise _Fatal("simulated fatal exit")

    monkeypatch.setattr(rc, "_reconcile_pins", _boom)

    async def _drive() -> None:
        task = asyncio.ensure_future(rc.warm_read_cache(db))
        with pytest.raises(_Fatal):
            await task
        assert task.done()

    asyncio.run(_drive())

    outcome = rc.warm_outcome()
    assert outcome is not None
    assert outcome.state == "running"


def test_o12b_later_warm_overwrites_a_dead_warms_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A later warm overwrites a dead warm's ``running`` with its own
    terminal state — the accepted cost of the single-slot design (F10).
    Warm 1 is TERMINATED (a ``BaseException`` out of it, reusing O12's own
    construction), never suspended-and-released: a suspend/release
    construction would let warm 1 resume, find its ticket stale, and write
    ``superseded`` — failing this oracle's own ``== "ok"`` assertion
    against correct code. That construction belongs to O13, not here.

    O12b -> at the final ``outcome.state``: correct = ``"ok"`` (warm 2's
    own terminal write), mutant (an implementation that refuses to
    overwrite a ``running`` state from a later warm) would leave the slot
    at ``"running"`` — which would look like a fix and would instead make
    the live warm invisible.
    """
    close_db()
    db = init_db(str(tmp_path / "o12b-overwrite.db"))

    class _Fatal(BaseException):
        pass

    real_reconcile_pins = rc._reconcile_pins

    def _boom(*args: object, **kwargs: object) -> None:
        raise _Fatal("simulated fatal exit")

    monkeypatch.setattr(rc, "_reconcile_pins", _boom)

    async def _kill_warm_1() -> None:
        task = asyncio.ensure_future(rc.warm_read_cache(db))
        with pytest.raises(_Fatal):
            await task

    asyncio.run(_kill_warm_1())

    # Liveness control: the state this oracle claims to overwrite really was
    # established by warm 1 finishing dead.
    outcome = rc.warm_outcome()
    assert outcome is not None
    assert outcome.state == "running"

    monkeypatch.setattr(rc, "_reconcile_pins", real_reconcile_pins)
    asyncio.run(rc.warm_read_cache(db))  # warm 2, real, runs to completion

    outcome = rc.warm_outcome()
    assert outcome is not None
    assert outcome.state == "ok"


def test_o13_superseded_is_neither_failed_nor_ok(tmp_path: Path) -> None:
    """A warm that loses the epoch race INSIDE ``_fill`` (the realistic
    shape) records ``superseded``, and the loser's keys never reach the
    pinned tier.

    Single-site construction, named explicitly per the plan: warm A blocks
    inside ``_fill`` (via a blocking ``daily_rank_conclusions``), warm B
    bumps the epoch and runs to completion, then A is released.

    O13 -> at ``outcome.state``: correct = ``"superseded"``, mutant (an
    unconditional ``_note_warm("ok")`` after ``_reconcile_pins``, the naive
    "completed normally" reading) = ``"ok"`` for a warm that pinned nothing.
    Paired: ``not any(k[2] == run_a for k in rc._PINNED)`` grounds the label
    in the observable effect (A's keys never pinned) rather than the label
    asserting about itself.
    """
    from tests.test_verification_read_cache import _full_run, _site

    close_db()
    db = init_db(str(tmp_path / "o13-superseded.db"))
    conn = db._conn  # noqa: SLF001
    site_id = _site(conn)
    run_a = _full_run(conn, site_id, state="published")
    conn.commit()

    entered_a = threading.Event()
    gate_a = threading.Event()
    real_w7 = rc.daily_rank_conclusions

    def _blocking_w7(
        conn: sqlite3.Connection,
        run_id: int,
        *,
        leads: tuple[int, ...] | None = None,
    ) -> dict[str, dict[str, object]]:
        if run_id == run_a:
            entered_a.set()
            assert gate_a.wait(timeout=5.0)
        return real_w7(conn, run_id, leads=leads)

    rc.daily_rank_conclusions = _blocking_w7  # type: ignore[assignment]
    try:

        async def _scenario() -> None:
            task_a = asyncio.create_task(rc.warm_read_cache(db))
            await asyncio.wait_for(asyncio.to_thread(entered_a.wait, 5.0), timeout=5.0)

            _full_run(conn, site_id, state="published")
            conn.commit()
            await rc.warm_read_cache(db)  # warm B, real, runs to completion

            gate_a.set()
            await asyncio.wait_for(task_a, timeout=5.0)

        asyncio.run(_scenario())
    finally:
        rc.daily_rank_conclusions = real_w7  # type: ignore[assignment]

    outcome = rc.warm_outcome()
    assert outcome is not None
    assert outcome.state == "superseded"
    assert not any(k[2] == run_a for k in rc._PINNED)


def test_o14_partial_derivation_failure_surfaces_the_fixture_derived_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Making exactly ``daily_rank_conclusions`` raise for one site yields
    ``derivations_failed == 1`` with ``state == "ok"`` — a warm that warmed
    partially is visible, not indistinguishable from a clean one.

    O14 -> at ``outcome.derivations_failed``: correct = ``1`` (one of the
    site's two per-site derivations, W7, raised and was swallowed), mutant
    (the counter dropped, i.e. ``_fill``'s inner catch never appends to
    ``failures``) = ``0``, reporting a clean ``ok`` for a warm that warmed
    nothing.
    """
    from tests.test_verification_read_cache import _full_run, _site

    close_db()
    db = init_db(str(tmp_path / "o14-partial.db"))
    conn = db._conn  # noqa: SLF001
    site_id = _site(conn)
    _full_run(conn, site_id, state="published")
    conn.commit()

    def _boom_w7(
        conn: sqlite3.Connection,
        run_id: int,
        *,
        leads: tuple[int, ...] | None = None,
    ) -> dict[str, dict[str, object]]:
        raise RuntimeError("W7 exploded for this site")

    monkeypatch.setattr(rc, "daily_rank_conclusions", _boom_w7)
    asyncio.run(rc.warm_read_cache(db))

    outcome = rc.warm_outcome()
    assert outcome is not None
    assert outcome.state == "ok"
    assert outcome.derivations_failed == 1


def test_o15_publish_time_call_site_records_the_outcome(tmp_path: Path) -> None:
    """The publish-time call site (``worker/verification_run.py:197``) also
    records — the same self-report covers both call sites for free.

    O15 -> at ``rc.warm_outcome()``: correct = not ``None`` after driving the
    real chain to its terminal chunk, mutant (instrumenting only the
    lifespan boot warm, e.g. recording only from ``api/app.py``'s call
    site) = ``None``, since this path never touches the lifespan at all.
    """
    from tests.test_verification_read_cache import _o19_drive_all_chunks, _o19_make_site

    close_db()
    db = init_db(str(tmp_path / "o15-publish-time.db"))
    conn = db._conn  # noqa: SLF001
    site_id = _o19_make_site(conn)
    payload: dict[str, object] = {"trigger_date": "2026-06-06"}
    writer = FencedWriter(db, db.generation)

    assert rc.warm_outcome() is None
    asyncio.run(_o19_drive_all_chunks(db, writer, site_id, payload))

    outcome = rc.warm_outcome()
    assert outcome is not None


# ---------------------------------------------------------------------------
# Group D - surfaces and regressions
# ---------------------------------------------------------------------------


def test_o17_read_cache_warm_key_present_and_none_before_any_warm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With the boot warm stubbed out, the key is present and ``None``
    before any warm has run — never absent.

    O17 -> at ``body["read_cache_warm"]``: correct = ``None``, mutant (a
    set-only-after-a-warm implementation, i.e. an absent key, F12) fails
    ``"read_cache_warm" in body`` outright.
    """
    close_db()
    config.db_path = str(tmp_path / "o17-stubbed.db")
    config.options_path = str(tmp_path / "o17-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    monkeypatch.setattr("wxverify.api.app.warm_read_cache", _no_warm)
    app = create_app(root_path="")
    with TestClient(app) as client:
        body = client.get("/api/worker/status").json()

    assert "read_cache_warm" in body
    assert body["read_cache_warm"] is None


def test_o17_un_stubbed_control_returns_a_dict_once_a_warm_has_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Un-stubbed control: without the boot-warm stub, the real warm task
    has already written ``running`` (or further) by the time the first
    request is served, proving the stub above — not vacuous test setup — is
    what makes ``None`` reachable.

    O17 -> at ``body["read_cache_warm"]``: correct = a ``dict``, mutant
    (none named; this is the positive half of the paired absence test) —
    without this control, O17's ``is None`` assertion would be
    indistinguishable from a test that never gave the warm a chance to run
    at all.
    """
    close_db()
    config.db_path = str(tmp_path / "o17-control.db")
    config.options_path = str(tmp_path / "o17-control-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")
    with TestClient(app) as client:
        body = client.get("/api/worker/status").json()

    assert isinstance(body["read_cache_warm"], dict)
    assert set(body["read_cache_warm"]) == {
        "state",
        "at",
        "detail",
        "derivations_failed",
    }
