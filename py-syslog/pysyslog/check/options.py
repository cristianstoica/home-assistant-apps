# pyright: strict
"""Shared ``--check`` option/config helpers and the pinned receive clock.

``default_check_options`` is the built-in options payload the oracle validates
off-HAOS; ``resolved_config`` resolves + prints the Config for ``--check``
visibility; ``pinned_clock`` is the deterministic receive clock the fixtures
were rendered against.
"""

from __future__ import annotations

import sys

from .. import config, fixtures
from ..config import ConfigError
from ..models import Config


def pinned_clock() -> str:
    """Deterministic receive clock for ``--check`` (matches the fixtures)."""
    return fixtures.PINNED_RECV_TS


def default_check_options() -> dict[str, object]:
    """The built-in options payload ``--check`` validates off-HAOS.

    Mirrors the ``config.yaml`` default seed (the `CHECK_SOURCES` mapping), so
    bare ``--check`` self-validates with no ``/data/options.json`` present.

    `listen_host` uses an RFC 5737 documentation address rather than the
    schema's ``0.0.0.0`` default: ``--check`` never binds a real socket, so the
    value is exercise-only, and keeping the bind-all literal out of Python
    preserves the py/bind-socket-all-network-interfaces invariant (no bind-all
    string literal anywhere on a path that could reach ``socket.bind``).
    """
    return {
        "listen_port": 5514,
        "listen_host": "192.0.2.10",
        "retention_days": 30,
        "log_level": "info",
        "sources": [dict(entry) for entry in fixtures.CHECK_SOURCES],
    }


def resolved_config(options_path: str) -> Config | None:
    """Resolve + print the Config for ``--check`` visibility; None on error.

    Reads ``options_path`` if it exists; otherwise (the default path, absent
    off-HAOS) validates the built-in default payload so ``--check`` runs without
    a file. An explicit ``--options`` pointing at a missing/invalid file still
    errors, naming the cause.
    """
    from pathlib import Path

    try:
        if Path(options_path).exists():
            cfg = config.load(options_path)
        elif options_path == config.DEFAULT_OPTIONS_PATH:
            cfg = config.validate(default_check_options())
        else:
            cfg = config.load(options_path)  # explicit path -> name the error
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return None
    print(
        f"resolved config: port={cfg.listen_port} host={cfg.listen_host} "
        f"retention={cfg.retention_days}d level={cfg.log_level} "
        f"sources={list(cfg.sources)} "
        f"log_dir={cfg.log_dir} log_file={cfg.log_file}",
        file=sys.stderr,
    )
    return cfg
