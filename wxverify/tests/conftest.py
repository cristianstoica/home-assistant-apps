"""Shared pytest fixtures.

The verification read cache (``wxverify.verification.read_cache``) is
process-global in-memory state, and the test suite re-initialises the process
database many times over -- every fresh database starts at generation 0 and
run ids repeat across tests. Resetting in an autouse fixture, rather than
opting in per test, is what keeps that state from leaking between unrelated
test modules as an intermittent failure nobody can attribute.

The export sweeper's death latch
(``wxverify.api.routes.db_transfer._sweeper_death``) is process-global for the
same reason and needs the same treatment: ``test_graceful_shutdown.py`` crashes
a REAL lifespan's sweeper twice, which latches a death that is deliberately
terminal for the process. Left standing, it would surface in ``test_monitor.py``
as an ``overall == "critical"`` with no test to attribute it to.
"""

import pytest

from wxverify.api.routes.db_transfer import reset_export_sweeper_death
from wxverify.verification.read_cache import reset_read_cache


@pytest.fixture(autouse=True)
def _reset_verification_read_cache() -> None:
    """Empty the verification read cache before each test."""
    reset_read_cache()


@pytest.fixture(autouse=True)
def _reset_export_sweeper_death() -> None:
    """Un-latch the export sweeper's death before each test."""
    reset_export_sweeper_death()
