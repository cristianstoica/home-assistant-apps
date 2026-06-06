# pyright: strict
"""CLI entrypoint: live collector + the self-validating ``--check`` oracle.

Modes:
  * (default)            — load options, bind, serve until SIGTERM/SIGINT.
  * ``--check``          — load + validate options, then drive the fixture
                           corpus through the real `process_datagram` seam with a
                           pinned clock and assert produced == expected for every
                           fixture line, tag, ``protocol``, ``sender_ts``,
                           site/host, the aggregate counters, and corpus integrity
                           (per-protocol fixture tally sums to the counters). Also
                           asserts the resolver warns exactly once across repeats,
                           drives the loop-level ``internal_errors`` survival path
                           through the real `Server.handle_one`, and rejects bad
                           options via both `config.validate` (field naming) and
                           `config.load` (malformed JSON / non-object / missing
                           file, cause naming). Exit 0 only on all-match.
  * ``--check --storage`` — exercise the real `Writer` state machine
                           (rollover / gzip atomicity+contents / prune-by-
                           filename-date / reconciliation / ENOTDIR) in a
                           tempdir with an injected fake clock.
  * ``--check --write-error`` — drive one datagram through the seam with a
                           `WriterProtocol` fake whose ``write()`` raises
                           `WriteError`; assert ``write_errors++``, ``written``
                           unchanged, a throttled warning, no echo, loop continues.

Diagnostics (stats, warnings) go to **stderr**; stored lines are echoed to
**stdout** for the HA Log tab. ``--check`` writes its PASS/FAIL report to stderr
so it never pollutes a captured stdout stream. The ``--check`` oracle itself
lives in the `check` package; this module keeps only the live/CLI surface.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from types import FrameType

from . import __version__, config
from .config import ConfigError
from .models import WriterProtocol
from .server import Server


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pysyslog",
        description=(
            "Durable UDP syslog collector (HA add-on py-syslog). Receives RFC "
            "3164/5424 datagrams, resolves each sender to a site/host, and "
            "writes one daily-rotated, gzip-compressed, retained file under "
            "/data/log. Default mode binds and serves; --check self-validates."
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"py-syslog {__version__}"
    )
    parser.add_argument(
        "--options",
        metavar="PATH",
        default=config.DEFAULT_OPTIONS_PATH,
        help=(
            "path to options.json (default /data/options.json). Use a local "
            "file to run --check off-HAOS."
        ),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="self-validate against the built-in fixture corpus; exit non-zero on mismatch",
    )
    parser.add_argument(
        "--storage",
        action="store_true",
        help="(with --check) exercise the real Writer state machine in a tempdir",
    )
    parser.add_argument(
        "--write-error",
        action="store_true",
        help="(with --check) assert the WriteError contract via a raising writer fake",
    )
    parser.add_argument(
        "--bind",
        action="store_true",
        help="(with --check) bind a real loopback UDP socket to prove the bind path",
    )
    return parser


def configure_logging(level: str) -> None:
    """Send diagnostics to **stderr**; stored lines go to stdout separately."""
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


# --- live mode ---------------------------------------------------------------


def _run_server(options_path: str) -> int:
    """Load options, wire signals, and serve until stop."""
    from .server import BindError, Throttle, make_throttled_warn
    from .writer import Writer, WriteError

    try:
        cfg = config.load(options_path)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1
    configure_logging(cfg.log_level)
    # One throttle shared between the server's own warnings and the Writer's
    # size-guard warnings, so a segment-roll flood cannot warn at roll rate.
    throttle = Throttle()
    try:
        writer: WriterProtocol = Writer(
            cfg.log_dir,
            cfg.log_file,
            cfg.retention_days,
            min_free_percent=cfg.min_free_percent,
            max_log_percent=cfg.max_log_percent,
            max_segment_mb=cfg.max_segment_mb,
            warn=make_throttled_warn(throttle),
        )
    except WriteError as exc:
        print(f"fatal: {exc}", file=sys.stderr)
        return 1
    server = Server(cfg, writer, throttle)

    def _handle(_signum: int, _frame: FrameType | None) -> None:
        server.request_stop()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    try:
        return server.run()
    except BindError as exc:
        print(f"fatal: {exc}", file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> int:
    """Parse args and dispatch to the requested mode."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.check:
        from .check import run_check

        return run_check(args.options, args.storage, args.write_error, args.bind)
    if args.storage or args.write_error or args.bind:
        parser.error("--storage, --write-error, and --bind require --check")
    return _run_server(args.options)


if __name__ == "__main__":
    sys.exit(main())
