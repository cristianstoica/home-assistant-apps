"""Shared fixtures for the publish-guard test suite."""

from __future__ import annotations

import socket
from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def _block_network(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Make any real network attempt raise, for every test in this suite.

    The guard's own tests inject a fake transport and never need a socket.
    This fixture is the backstop that proves it: if a test somehow bypassed
    the fake and reached real networking code, it fails loudly here instead
    of quietly calling ghcr.io.
    """

    def _blocked_connect(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("network access is blocked in this test suite")

    def _blocked_create_connection(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("network access is blocked in this test suite")

    monkeypatch.setattr(socket.socket, "connect", _blocked_connect)
    monkeypatch.setattr(socket, "create_connection", _blocked_create_connection)
    yield
