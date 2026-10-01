"""Shared pytest fixtures.

The verification read cache (``wxverify.verification.read_cache``) is
process-global in-memory state, and the test suite re-initialises the process
database many times over -- every fresh database starts at generation 0 and
run ids repeat across tests. Resetting in an autouse fixture, rather than
opting in per test, is what keeps that state from leaking between unrelated
test modules as an intermittent failure nobody can attribute.

The wind weights cache (``wxverify.forecast.wind_blend``) is process-global
for the same reason and is reset the same way, so that its counters start at
zero in every test and no entry outlives the test that stored it.

The export sweeper's death latch
(``wxverify.api.routes.db_transfer._sweeper_death``) is process-global for the
same reason and needs the same treatment: ``test_graceful_shutdown.py`` crashes
a REAL lifespan's sweeper twice, which latches a death that is deliberately
terminal for the process. Left standing, it would surface in ``test_monitor.py``
as an ``overall == "critical"`` with no test to attribute it to.

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
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Iterator

import pytest

from tests.network_guard import deny_network_scope
from wxverify.api.routes.db_transfer import reset_export_sweeper_death
from wxverify.forecast.wind_blend import reset_wind_weights_cache
from wxverify.verification.read_cache import reset_read_cache

pytest_plugins = ["pytester"]


@pytest.fixture(autouse=True)
def _reset_verification_read_cache() -> None:
    """Empty the verification read cache before each test."""
    reset_read_cache()


@pytest.fixture(autouse=True)
def _reset_wind_weights_cache() -> None:
    """Empty the wind weights cache and zero its counters before each test."""
    reset_wind_weights_cache()


@pytest.fixture(autouse=True)
def _reset_export_sweeper_death() -> None:
    """Un-latch the export sweeper's death before each test."""
    reset_export_sweeper_death()


@pytest.fixture(autouse=True)
def _deny_network() -> Iterator[None]:
    """Fail any test that tries to leave the process over a socket."""
    yield from deny_network_scope()


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
