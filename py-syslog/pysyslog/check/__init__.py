# pyright: strict
"""The ``--check`` self-validation oracle, factored out of ``__main__``.

``run_check`` is the single entry point ``main`` dispatches to. The CLI surface,
flag names, stderr strings, and exit codes are identical to the pre-refactor
inline oracle — the built-in fixture corpus is the regression net that proves it.
"""

from __future__ import annotations

import sys

from .bind import check_bind
from .config_checks import (
    check_invalid_options,
    check_listen_host,
    check_reject_unknown_sources,
    check_size_guard_config,
)
from .datagrams import check_datagrams, check_trace
from .options import resolved_config
from .storage import check_storage
from .survival import check_internal_error, check_warn_once, check_write_error


def run_check(options_path: str, storage: bool, write_error: bool, bind: bool) -> int:
    """Dispatch the --check variants; exit 0 only when every assertion holds."""
    from ..__main__ import (
        configure_logging,
    )  # lazy: avoids check->__main__ import cycle

    configure_logging("info")
    if write_error:
        return 0 if check_write_error() else 1
    if storage:
        return 0 if check_storage() else 1
    if bind:
        return 0 if check_bind() else 1

    if resolved_config(options_path) is None:
        return 1
    ok = check_datagrams()
    ok = check_trace() and ok
    ok = check_warn_once() and ok
    ok = check_internal_error() and ok
    ok = check_listen_host() and ok
    ok = check_size_guard_config() and ok
    ok = check_invalid_options() and ok
    ok = check_reject_unknown_sources() and ok
    if ok:
        print("CHECK PASSED", file=sys.stderr)
        return 0
    print("CHECK FAILED", file=sys.stderr)
    return 1
