# pyright: strict
"""CLI entrypoint: the live adaptive scheduler + the self-validating ``--check``.

Modes:
  * (default)   — load options, wire the production seams + signals, and run the
                  adaptive per-station polling loop until SIGTERM/SIGINT.
  * ``--check`` — run the offline self-validation oracle (config rejection,
                  entity-id-shape + station-key contract, HA request shaping,
                  health/freshness evaluation, the terminal/transient
                  classification incl. 429-on-update_entity precedence, the
                  reward/backoff sequences, and stop-during-sleep
                  interruptibility). All seams are faked — no network, no real
                  sockets/threads. Exit 0 only on all-pass.

Diagnostics (startup, station registration, per-poll outcomes, warnings) go to
**stderr** for the HA Log tab; ``--check`` writes its PASS/FAIL report to stderr.
The oracle itself lives in the `check` package; this module keeps only the
live/CLI surface.

The ``SUPERVISOR_TOKEN`` bearer is read from the environment (injected by the
Supervisor when ``homeassistant_api: true``); a missing/blank token fails fast
with a clear message rather than spinning doomed unauthenticated polls.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import sys
from types import FrameType

from . import __version__, config
from .config import ConfigError


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyweather",
        description=(
            "Adaptive Weather.com PWS poller (HA add-on py-weather). Default mode "
            "runs the per-station polling loop; --check self-validates offline."
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"py-weather {__version__}"
    )
    parser.add_argument(
        "--options",
        metavar="PATH",
        default=config.DEFAULT_OPTIONS_PATH,
        help=(
            "path to options.json (default /data/options.json). Use a local file "
            "to run --check off-HAOS."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="run the offline self-validation oracle; exit non-zero on mismatch",
    )
    return parser


def configure_logging(level: str) -> None:
    """Send all diagnostics to **stderr** (the HA Log tab stream)."""
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


def _run_loop(options_path: str) -> int:
    """Load options, wire the production seams + signals, and run until stop."""
    from .haapi import HaApiClient
    from .httpclient import UrllibHttpClient
    from .runtime import EventSleeper, SystemWallClock, monotonic
    from .scheduler import Scheduler

    try:
        cfg = config.load(options_path)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1
    configure_logging(cfg.log_level)

    token = os.environ.get("SUPERVISOR_TOKEN", "").strip()
    if token == "":
        print(
            "config error: SUPERVISOR_TOKEN is not set "
            "(the add-on needs homeassistant_api: true)",
            file=sys.stderr,
        )
        return 1

    http = UrllibHttpClient(secrets=(token,))
    api = HaApiClient(http, token, float(cfg.request_timeout_seconds))
    sleeper = EventSleeper()
    scheduler = Scheduler(
        cfg,
        api=api,
        clock=monotonic,
        wall_clock=SystemWallClock(),
        sleeper=sleeper,
        rng=random.Random(),
    )

    def _handle(_signum: int, _frame: FrameType | None) -> None:
        sleeper.request_stop()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    scheduler.run_loop()
    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse args and dispatch to the requested mode."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.check:
        from .check import run_check

        configure_logging("info")
        return run_check()
    return _run_loop(args.options)


if __name__ == "__main__":
    sys.exit(main())
