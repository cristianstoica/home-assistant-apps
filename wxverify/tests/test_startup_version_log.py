"""The startup version log.

``lifespan``'s first log line reports this run's ``__version__``, SQLite
and Python versions, so an after-install check can compare the deployed
image's log against the CI test-session header (``conftest.py``'s
``pytest_report_header``). Pattern follows test_task_death_notification.py's
O2: a real ``lifespan`` driven directly, worker patched idle so the
async-generator reaches ``yield`` and returns promptly on ``__aexit__``.
"""

from __future__ import annotations

import asyncio
import logging
import platform
import sqlite3
from pathlib import Path

import pytest

from wxverify import __version__, config
from wxverify.api.app import create_app, lifespan
from wxverify.db.connection import close_db


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


def test_lifespan_logs_wxverify_sqlite_python_versions_at_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """``lifespan`` logs, at INFO, exactly the startup version line.

    There is no "absent" counterpart for a startup line (it always runs),
    so this pins the one literal rather than pairing a
    negative/positive: ``__version__`` ties the line to this release, and
    the SQLite/Python values are read live, not hardcoded, from the same
    ``sqlite3``/``platform`` calls the app line uses.
    """
    close_db()
    config.db_path = str(tmp_path / "startup-log.db")
    config.options_path = str(tmp_path / "startup-log-missing-options.json")
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    app = create_app(root_path="")

    async def _run() -> None:
        cm = lifespan(app)
        await cm.__aenter__()
        await cm.__aexit__(None, None, None)

    with caplog.at_level(logging.INFO, logger="wxverify.api.app"):
        asyncio.run(_run())

    expected = (
        f"wxverify {__version__}; sqlite {sqlite3.sqlite_version}; "
        f"python {platform.python_version()}"
    )
    matches = [r for r in caplog.records if r.getMessage() == expected]
    assert [r.levelno for r in matches] == [logging.INFO], [
        r.getMessage() for r in caplog.records
    ]
