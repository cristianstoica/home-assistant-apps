"""Regression: the CLI's `basicConfig(force=True)` must not leak a logging
handler bound to a closed capsys stream into a later, unrelated test.

`_configure_logging()` (wxverify/__main__.py) calls
`logging.basicConfig(stream=sys.stdout, ..., force=True)` for every
non-`serve` command. When `main()` is driven in-process under `capsys`,
that installs a `StreamHandler` bound to *that test's* `CaptureIO`, which
pytest closes when the call phase ends. Without the autouse
`restore_logging_state` fixture in `tests/conftest.py`, that closed-stream
handler stays on the root logger, and the next unrelated INFO+ record
(commonly emitted from `wxverify/db/migrations.py`) makes
`StreamHandler.emit` hit `ValueError: I/O operation on closed file`, which
`logging.Handler.handleError` reports by printing "--- Logging error ---"
instead of failing the test that actually caused it.

This drives a real *child* pytest session via `pytester.runpytest_subprocess`
rather than in-process: an in-process child would run inside this session's
own root logger, closing ITS handlers via `force=True`, inheriting this
session's `--log-level`, and sharing the process-wide `wxverify.config` /
DB singleton with the outer session -- none of which reflects the real
leak (a *user's* pytest session run once, not nested inside another).
"""

from __future__ import annotations

from pathlib import Path

import pytest

ADDON_ROOT = Path(__file__).resolve().parents[1]

#: The child test module. Its tests run in source order: the child loads no
#: order-randomizing plugin (none is pinned in uv.lock). If one is ever added,
#: test_b fails with a KeyError on BEFORE -- a loud failure, never a false pass.
_INNER = '''
"""Child session: reproduces the leak in a fresh, real pytest process."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

from wxverify import config
from wxverify.__main__ import main

BEFORE: dict[str, object] = {}
WIRE = ("httpx", "httpcore")


def test_a_cli_under_capsys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Drive a one-shot CLI command in-process under capsys, as a real CLI
    smoke test would, and snapshot the resulting (leaking, pre-fix) state.
    """
    db_path = tmp_path / "wxverify.db"
    monkeypatch.setattr(config, "db_path", str(db_path))
    monkeypatch.setattr(config, "options_path", str(tmp_path / "missing-options.json"))

    root = logging.getLogger()
    BEFORE["level"] = root.level
    BEFORE["handlers"] = list(root.handlers)
    for name in WIRE:
        lg = logging.getLogger(name)
        BEFORE[f"{name}_snapshot"] = (lg.level, list(lg.filters))

    rc = main(["--db", str(db_path), "timezone", "status"])
    out = capsys.readouterr().out

    assert rc == 0, f"CLI must exit 0; got {rc}"
    assert "no timezone generations" in out, (
        f"expected the empty-generations message; got: {out!r}"
    )

    # Premise checks: basicConfig(force=True) actually ran and actually
    # changed global logging state -- otherwise this test would prove
    # nothing about the isolation fixture under test.
    assert BEFORE["level"] == logging.WARNING, (
        f"premise: root logger must start at the Python default WARNING "
        f"before _configure_logging runs; got {BEFORE['level']!r}"
    )
    assert root.level == logging.INFO, (
        f"premise: _configure_logging must have set root level to INFO "
        f"(no WXV_LOG_LEVEL set); got {root.level!r}"
    )
    httpx_before_filters = BEFORE["httpx_snapshot"][1]  # type: ignore[index]
    httpx_logger = logging.getLogger("httpx")
    assert list(httpx_logger.filters) != httpx_before_filters, (
        "premise: _configure_logging must attach a fresh redaction filter "
        "to the httpx logger, changing its filter list"
    )

    leaked = [
        h
        for h in root.handlers
        if isinstance(h, logging.StreamHandler) and h.stream is sys.stdout
    ]
    assert len(leaked) == 1, (
        f"premise: exactly one StreamHandler bound to this test's capsys "
        f"sys.stdout must be installed; got {len(leaked)}: {root.handlers!r}"
    )
    BEFORE["leaked"] = leaked[0]


def test_b_later_log_call(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A later, unrelated test's log call must not hit the prior test's
    now-closed capsys handler, and the prior test's global logging mutation
    must not still be in effect.
    """
    errors: list[BaseException] = []
    original_handle_error = logging.Handler.handleError

    def _record_handle_error(self: logging.Handler, record: logging.LogRecord) -> None:
        errors.append(RuntimeError(f"handleError called for {record.getMessage()!r}"))

    monkeypatch.setattr(logging.Handler, "handleError", _record_handle_error)

    logging.getLogger("wxverify.probe").error("probe after CLI test")

    assert errors == [], (
        f"a later test's log call must not raise inside a leaked, "
        f"closed-stream handler; got: {errors}"
    )

    root = logging.getLogger()
    assert BEFORE["leaked"] not in root.handlers, (
        "the prior test's capsys-bound handler must have been removed and "
        "closed by teardown, not left attached to the root logger"
    )
    assert root.handlers == BEFORE["handlers"], (
        f"root handlers must be restored to the exact pre-test objects, "
        f"in order; got {root.handlers!r}, expected {BEFORE['handlers']!r}"
    )
    assert root.level == BEFORE["level"], (
        f"root level must be restored to the pre-test level; "
        f"got {root.level!r}, expected {BEFORE['level']!r}"
    )
    for name in WIRE:
        lg = logging.getLogger(name)
        saved_level, saved_filters = BEFORE[f"{name}_snapshot"]  # type: ignore[misc]
        assert lg.level == saved_level, (
            f"{name} logger level must be restored; got {lg.level!r}, "
            f"expected {saved_level!r}"
        )
        assert list(lg.filters) == saved_filters, (
            f"{name} logger filters must be restored; got {lg.filters!r}, "
            f"expected {saved_filters!r}"
        )

    assert "probe after CLI test" in caplog.messages, (
        "the probe log call must have actually reached caplog -- otherwise "
        "the absence of a handleError call proves nothing"
    )
'''


def test_cli_logging_is_restored_before_the_next_test(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run the two child tests in a real, isolated pytest subprocess and
    confirm the leak is gone and no "Logging error" banner is printed.
    """
    pytester.makeini("[pytest]\n")
    conftest_src = (Path(__file__).resolve().parent / "conftest.py").read_text(
        encoding="utf-8"
    )
    pytester.makeconftest(conftest_src)
    pytester.makepyfile(test_inner=_INNER)

    monkeypatch.setenv("PYTHONPATH", str(ADDON_ROOT))
    monkeypatch.delenv("WXV_LOG_LEVEL", raising=False)
    # Make the child deterministic: strip anything from the outer session's
    # environment that could change the child's test order or logging
    # premises out from under this test's fixed-order assumptions.
    monkeypatch.delenv("PYTEST_ADDOPTS", raising=False)
    # No order-randomizing or xdist plugin is declared in uv.lock (checked:
    # neither pytest-randomly nor pytest-xdist appears among the pinned
    # packages), so there is nothing further to disable with -p no:<plugin>.

    result = pytester.runpytest_subprocess("-p", "no:cacheprovider", "-rA", timeout=120)

    result.assert_outcomes(passed=2)
    result.stdout.no_fnmatch_line("*Logging error*")
    result.stderr.no_fnmatch_line("*Logging error*")
