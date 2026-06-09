# pyright: strict
"""CLI entrypoint: the live updater loop + the self-validating ``--check`` oracle.

Modes:
  * (default)            — load options, run the reconcile loop until SIGTERM/
                           SIGINT.
  * ``--check``          — run the offline self-validation oracle (config
                           rejection, Azure URL/body/token shaping, URL endpoint
                           shaping, IP parse/guard, resolver three-way outcome,
                           per-request status handling, bounded interruptible
                           backoff via a synchronous fake clock/stop, callback
                           confirmation gated on the state seam, startup
                           self-heal, and a no-secret-leakage assertion). All
                           seams are faked — no network, no real sockets/threads.
                           Exit 0 only on all-pass.
  * ``--check --dry-run [--options PATH]`` — load + validate the options for the
                           configured provider and print the **redacted** planned
                           action (method/host/record label/redacted body only),
                           then exit without touching the network.

Diagnostics (status, warnings) go to **stderr** for the HA Log tab; ``--check``
writes its PASS/FAIL report to stderr. The oracle itself lives in the `check`
package; this module keeps only the live/CLI surface.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from types import FrameType

from . import __version__, config
from .config import ConfigError


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pyddns",
        description=(
            "Generic multi-provider dynamic-DNS updater (HA add-on py-ddns). "
            "Default mode runs the reconcile loop; --check self-validates; "
            "--check --dry-run prints the redacted planned action."
        ),
    )
    parser.add_argument("--version", action="version", version=f"py-ddns {__version__}")
    parser.add_argument(
        "--options",
        metavar="PATH",
        default=config.DEFAULT_OPTIONS_PATH,
        help=(
            "path to options.json (default /data/options.json). Use a local file "
            "to run --check --dry-run off-HAOS."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="run the offline self-validation oracle; exit non-zero on mismatch",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="(with --check) print the redacted planned action and exit; no network",
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
    from .httpclient import UrllibHttpClient
    from .ipsource import IpSourceClient
    from .providers import build_provider
    from .resolver import DnsResolver
    from .runtime import EventSleeper, monotonic
    from .state import FileState
    from .updater import Updater

    try:
        selection = config.load(options_path)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1
    cfg = selection.config
    configure_logging(cfg.log_level)
    if selection.azure_options_ignored:
        # Both sections were filled; URL won. Warn now that logging is configured
        # so the line reaches the HA Log tab (load() runs before configure_logging).
        config.warn_azure_ignored(logging.getLogger("pyddns"))

    http = UrllibHttpClient()
    sleeper = EventSleeper()
    provider = build_provider(cfg, http, monotonic)
    updater = Updater(
        cfg,
        ip_source=IpSourceClient(cfg.ip_source_urls, http),
        provider=provider,
        resolver=DnsResolver(cfg.test_ns),
        state=FileState(cfg.state_path),
        clock=monotonic,
        sleeper=sleeper,
    )

    def _handle(_signum: int, _frame: FrameType | None) -> None:
        sleeper.request_stop()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    logging.getLogger("pyddns").info(
        "py-ddns starting: provider=%s name=%s interval=%ds",
        cfg.provider.value,
        cfg.name,
        cfg.interval_seconds,
    )
    updater.run_loop()
    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse args and dispatch to the requested mode."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.check:
        from .check import run_check, run_dry_run

        configure_logging("info")
        if args.dry_run:
            return run_dry_run(args.options)
        return run_check()
    if args.dry_run:
        parser.error("--dry-run requires --check")
    return _run_loop(args.options)


if __name__ == "__main__":
    sys.exit(main())
