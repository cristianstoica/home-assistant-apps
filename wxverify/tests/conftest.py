"""Shared pytest fixtures.

The export sweeper's death latch
(``wxverify.api.routes.db_transfer._sweeper_death``) is process-global, so it
is reset before each test: ``test_graceful_shutdown.py`` crashes
a REAL lifespan's sweeper twice, which latches a death that is deliberately
terminal for the process. Left standing, it would surface in ``test_monitor.py``
as an ``overall == "critical"`` with no test to attribute it to.

The process-wide stop flag (``wxverify.core.stop.STOP``) is process-global for
the same reason and needs the same treatment: ``lifespan``'s ``finally`` now
calls ``request_stop()`` on every shutdown, so a test that drives a real
lifespan leaves ``STOP`` set for every test that runs after it.

Real network access is denied for every test by ``_deny_network``; the
mechanism, its ledger and its allowlist live in ``tests/network_guard.py`` so
that ``tests/test_network_guard.py`` can import the same objects this fixture
uses (this file is imported as a top-level ``conftest`` module and must not
be imported by tests).

``idle_current_obs_poller`` (plan §6.3.6) patches the real current-obs lane
out of ``run_worker`` for tests that drive it directly against a fake
``db`` (a ``_FakeDb`` with no real ``jobs``/``station_poll_state`` schema
behind it). Without it, the second lane created inside ``run_worker`` would
issue its own ``db.write`` calls against that fake and crash on data the
fake was never built to hold. It is NOT autouse: tests that exercise the
real poller (the supervisor contract tests, the app-level stop test, DL5)
must not have it applied.

``restore_logging_state`` (autouse) snapshots the root logger's level and
handlers, and the ``httpx``/``httpcore`` loggers' levels and filters, and
restores them after every test. Without it, a CLI test that drives
``main()`` under ``capsys`` leaves a root handler bound to a closed capture
stream, and the next unrelated ``logging.info(...)`` (commonly from
``db/migrations.py``) prints "--- Logging error ---" instead of failing the
test that caused it. It is autouse, not opt-in, so that no future
``main()``-driving test can reintroduce the leak by omission; the mechanism
and its known limits are in the fixture's own docstring.

``pytester`` (declared via ``pytest_plugins``) lets
``tests/test_cli_logging_isolation.py`` run its regression check in a
separate pytest subprocess, so the CLI's ``basicConfig(force=True)`` fires
in a fresh session instead of against this session's own root handlers.

``_check_day_close_records`` (autouse; plan 2026-10-20-daily-close-day-
state-and-guard.md §13.1) calls ``tests.helpers.check_day_close_records``
after every test. With no connection registered by
``tests.helpers.day_close_site`` it does nothing, so the existing suite is
untouched; a daily-close test that forgets ``note_day_clock`` for a later
clock fails here instead of silently passing on a wrong one.
"""

import asyncio
import logging
import platform
import sqlite3
import sys
from collections.abc import Awaitable, Callable, Iterator

import pytest

from tests.network_guard import deny_network_scope
from wxverify.api.routes.db_transfer import reset_export_sweeper_death
from wxverify.core.stop import reset_stop

pytest_plugins = ["pytester"]


def pytest_report_header(config: pytest.Config) -> str:
    return (
        f"sqlite {sqlite3.sqlite_version}; python {platform.python_version()}"
        f" at {sys.executable}"
    )


@pytest.fixture(autouse=True)
def _reset_export_sweeper_death() -> None:
    """Un-latch the export sweeper's death before each test."""
    reset_export_sweeper_death()


@pytest.fixture(autouse=True)
def _reset_core_stop() -> None:
    """Clear the process-wide stop flag before each test."""
    reset_stop()


@pytest.fixture(autouse=True)
def _deny_network() -> Iterator[None]:
    """Fail any test that tries to leave the process over a socket."""
    yield from deny_network_scope()


@pytest.fixture(autouse=True)
def _check_day_close_records() -> Iterator[None]:
    """Check daily-close ``site_days`` records stop at the clock a test used.

    Imported inside the fixture body rather than at module top, since the
    module it comes from (``tests.helpers``) imports ``wxverify.db.site_days``
    lazily, from inside ``day_close_site``, before that module exists.
    """
    yield
    from tests.helpers import check_day_close_records

    check_day_close_records()


@pytest.fixture(autouse=True)
def restore_logging_state() -> Iterator[None]:
    """Snapshot and restore root + httpx/httpcore logger state.

    ``_configure_logging()``'s ``basicConfig(force=True)`` installs a fresh
    ``StreamHandler`` bound to *this test's* ``sys.stdout``. Under
    ``capsys`` that stream is the call phase's ``CaptureIO``, which pytest
    closes when the call phase ends; without ``capsys``, ``sys.stdout`` is the
    session-long fd-capture ``EncodedFile``, never closed per test -- which
    is why only ``capsys``-based tests leaked. Blindly re-adding whatever
    handlers were on the root logger before the test ran, and blindly
    leaving whatever ``_configure_logging`` installed during the test still
    attached afterward, both leak a handler bound to an already-expired
    capture stream into later tests: the next unrelated
    ``logging.info(...)`` that reaches the root logger then tries to write
    to a closed file and prints "--- Logging error ---" to the terminal
    instead of failing the test that actually caused it.

    So on teardown: (1) any handler installed during the test -- i.e. not
    part of the original snapshot -- is explicitly closed, not merely
    detached, so it can't be resurrected by identity elsewhere; (2) a
    snapshotted handler whose stream was closed during the test is dropped
    rather than reattached in a broken state. ``force=True`` itself never
    trips guard (2): ``Handler.close()`` leaves a ``StreamHandler``'s
    stream open, and ``FileHandler.close()`` sets its stream to ``None``
    (see below), so with pytest's own handlers guard (2) is defensive only.

    Handlers are restored by identity and order, but re-attaching cannot
    undo ``basicConfig(force=True)`` having *closed* a file handler: pytest
    installs its own ``logging``-plugin file handler
    (``_FileHandler(os.devnull)`` by default, or a real path with
    ``--log-file``), and once ``force=True`` closes it, re-adding the same
    object to ``root.handlers`` does not make it functional again -- it
    stays inert. So with ``--log-file`` set, this fixture does not fully
    restore that handler's function; it only guarantees no *closed-stream*
    write ever reaches it after the test that closed it. This is a known
    limitation, not a claim of complete restoration of every handler.

    There is also a known unprotected window: it runs from the moment
    pytest's ``item_capture`` phase closes the ``CaptureIO`` (at the end of
    the call phase) through every function-scoped finalizer that runs
    *before* this fixture's own teardown. Nothing in this suite logs during
    that window today; if something later does, the "--- Logging error
    ---" would surface in the *causing* test's own teardown, not leak into
    a later, unrelated test.

    Deliberately NOT declared with a ``capsys`` parameter: it could not
    close the window above, because ``item_capture`` closes the call
    phase's ``CaptureIO`` before any fixture finalizer runs; and pulling
    ``capsys`` into every test would switch the whole suite from fd capture
    to per-test sys capture and make any future ``capfd`` test error with
    "cannot use capfd and capsys at the same time".
    """
    root = logging.getLogger()
    saved_root_level = root.level
    saved_root_handlers = list(root.handlers)

    wire_saved: dict[str, tuple[int, list[logging.Filter]]] = {}
    for name in ("httpx", "httpcore"):
        lg = logging.getLogger(name)
        wire_saved[name] = (lg.level, list(lg.filters))

    yield

    # Restore root: close/discard anything the test installed, and only
    # reattach original handlers whose stream is still writable.
    root.setLevel(saved_root_level)
    for h in list(root.handlers):
        root.removeHandler(h)
        if h not in saved_root_handlers:
            h.close()
    for h in saved_root_handlers:
        stream = getattr(h, "stream", None)
        if stream is not None and getattr(stream, "closed", False):
            continue
        root.addHandler(h)

    # Restore wire loggers
    for name, (lvl, filters) in wire_saved.items():
        lg = logging.getLogger(name)
        lg.setLevel(lvl)
        lg.filters = list(filters)


async def _idle_poller(db: object, *, run_job: Callable[..., Awaitable[None]]) -> None:
    """A current-obs lane that never claims anything (plan §6.3.6)."""
    await asyncio.Event().wait()


@pytest.fixture
def idle_current_obs_poller(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the current-obs lane inside ``run_worker`` with an idle stub.

    The patch target is ``wxverify.worker.processor.run_current_obs_poller``
    -- the name ``run_worker`` resolves at call time -- never the defining
    module, ``wxverify.worker.current_obs_poller``.
    """
    monkeypatch.setattr(
        "wxverify.worker.processor.run_current_obs_poller", _idle_poller
    )
