"""Shared pytest fixtures.

The verification read cache (``wxverify.verification.read_cache``) is
process-global in-memory state, and the test suite re-initialises the process
database many times over -- every fresh database starts at generation 0 and
run ids repeat across tests. Resetting in an autouse fixture, rather than
opting in per test, is what keeps that state from leaking between unrelated
test modules as an intermittent failure nobody can attribute.
"""

import pytest

from wxverify.verification.read_cache import reset_read_cache


@pytest.fixture(autouse=True)
def _reset_verification_read_cache() -> None:
    """Empty the verification read cache before each test."""
    reset_read_cache()
