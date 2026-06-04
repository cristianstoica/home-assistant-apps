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
so it never pollutes a captured stdout stream.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from types import FrameType

from . import __version__, config, fixtures
from .config import ConfigError
from .models import Config, SourceMapping, SyslogRecord, WriterProtocol
from .parser import parse
from .server import Counters, Server, process_datagram, trace_datagram
from .resolver import Resolver

# The protocol tags the parser may emit; the datagram corpus must stay inside
# this set, and the per-protocol fixture tally must sum to the expected
# protocol counters (so the line corpus and the counter corpus cannot drift
# apart independently).
_VALID_PROTOCOLS = ("rfc3164", "rfc5424", "unknown")


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
    return parser


def _configure_logging(level: str) -> None:
    """Send diagnostics to **stderr**; stored lines go to stdout separately."""
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )


def _pinned_clock() -> str:
    """Deterministic receive clock for ``--check`` (matches the fixtures)."""
    return fixtures.PINNED_RECV_TS


# --- live mode ---------------------------------------------------------------


def _run_server(options_path: str) -> int:
    """Load options, wire signals, and serve until stop."""
    from .writer import Writer, WriteError

    try:
        cfg = config.load(options_path)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1
    _configure_logging(cfg.log_level)
    try:
        writer: WriterProtocol = Writer(cfg.log_dir, cfg.log_file, cfg.retention_days)
    except WriteError as exc:
        print(f"fatal: {exc}", file=sys.stderr)
        return 1
    server = Server(cfg, writer)

    def _handle(_signum: int, _frame: FrameType | None) -> None:
        server.request_stop()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    return server.run()


# --- --check: fixture corpus -------------------------------------------------


def _check_datagrams() -> bool:
    """Drive the datagram corpus through the seam; assert lines, fields, counters.

    Three layers, each its own PASS/FAIL line(s):

    1. **Line** — produced == ``expected_line`` through the real
       `process_datagram` seam (with the pinned clock).
    2. **Fields** — `parse(raw, PINNED_RECV_TS)` yields ``protocol`` and
       ``sender_ts`` matching the fixture, and `resolver.resolve(client_ip)`
       yields ``(site, host)`` matching the fixture. The seam composes these but
       hides them; asserting them directly pins the parse/resolve contract a
       broken renderer could otherwise mask.
    3. **Counters + corpus-integrity** — the aggregate counters match
       `EXPECTED_COUNTERS`, every fixture ``protocol`` is in `_VALID_PROTOCOLS`,
       and the per-protocol fixture tally sums to the expected protocol counters
       (the line corpus and the counter corpus cannot drift independently).

    Returns ``True`` only if every layer holds.
    """
    sources = {
        entry["ip"]: SourceMapping(
            ip=entry["ip"], site=entry["site"], host=entry["host"]
        )
        for entry in fixtures.CHECK_SOURCES
    }
    resolver = Resolver(sources)
    counters = Counters()
    capture = _CaptureWriter()

    ok = True
    protocol_tally: dict[str, int] = {}
    for fixture in fixtures.DATAGRAMS:
        protocol_tally[fixture.protocol] = protocol_tally.get(fixture.protocol, 0) + 1

        # Layer 1: the rendered line through the real seam.
        capture.lines.clear()
        process_datagram(
            fixture.raw,
            fixture.client_ip,
            resolver=resolver,
            writer=capture,
            counters=counters,
            clock=_pinned_clock,
        )
        produced = capture.lines[0] if capture.lines else ""
        if produced == fixture.expected_line:
            print(f"PASS  [{fixture.tag}] {fixture.name}", file=sys.stderr)
        else:
            ok = False
            print(f"FAIL  [{fixture.tag}] {fixture.name}", file=sys.stderr)
            print(f"  expected: {fixture.expected_line!r}", file=sys.stderr)
            print(f"  produced: {produced!r}", file=sys.stderr)

        # Pin the headline contract directly: one datagram -> exactly one
        # physical line (a single trailing newline, none embedded). This catches
        # a future expected_line that itself wrongly embedded a newline, which
        # blob equality above would silently accept.
        if produced.count("\n") == 1 and produced.endswith("\n"):
            print(f"PASS  [{fixture.tag}] one physical line", file=sys.stderr)
        else:
            ok = False
            print(
                f"FAIL  [{fixture.tag}] one physical line: "
                f"newlines={produced.count(chr(10))} "
                f"trailing={produced.endswith(chr(10))}",
                file=sys.stderr,
            )

        # Layer 2: the parse + resolve fields the line is composed from.
        record = parse(fixture.raw.decode("utf-8", "replace"), fixtures.PINNED_RECV_TS)
        if record.protocol == fixture.protocol:
            print(f"PASS  [{fixture.tag}] protocol={record.protocol}", file=sys.stderr)
        else:
            ok = False
            print(
                f"FAIL  [{fixture.tag}] protocol: expected "
                f"{fixture.protocol!r}, got {record.protocol!r}",
                file=sys.stderr,
            )
        if record.sender_ts == fixture.sender_ts:
            print(
                f"PASS  [{fixture.tag}] sender_ts={record.sender_ts!r}",
                file=sys.stderr,
            )
        else:
            ok = False
            print(
                f"FAIL  [{fixture.tag}] sender_ts: expected "
                f"{fixture.sender_ts!r}, got {record.sender_ts!r}",
                file=sys.stderr,
            )
        resolved_site, resolved_host = resolver.resolve(fixture.client_ip)
        if (resolved_site, resolved_host) == (fixture.site, fixture.host):
            print(
                f"PASS  [{fixture.tag}] resolved=({resolved_site}, {resolved_host})",
                file=sys.stderr,
            )
        else:
            ok = False
            print(
                f"FAIL  [{fixture.tag}] resolved: expected "
                f"({fixture.site}, {fixture.host}), got "
                f"({resolved_site}, {resolved_host})",
                file=sys.stderr,
            )

    # Layer 3a: aggregate counters.
    produced_counters = counters.as_dict()
    for key, expected in fixtures.EXPECTED_COUNTERS.items():
        actual = produced_counters.get(key)
        if actual == expected:
            print(f"PASS  counter {key}={actual}", file=sys.stderr)
        else:
            ok = False
            print(
                f"FAIL  counter {key}: expected {expected}, got {actual}",
                file=sys.stderr,
            )

    # Layer 3b: corpus integrity — every protocol is known, and the per-protocol
    # fixture tally matches the expected protocol counters (line vs counter
    # corpora cannot drift apart).
    unknown_protocols = sorted(set(protocol_tally) - set(_VALID_PROTOCOLS))
    if unknown_protocols:
        ok = False
        print(
            f"FAIL  corpus: unknown protocol tag(s) {unknown_protocols}",
            file=sys.stderr,
        )
    else:
        print(
            f"PASS  corpus: all protocols in {list(_VALID_PROTOCOLS)}",
            file=sys.stderr,
        )
    for proto in _VALID_PROTOCOLS:
        tallied = protocol_tally.get(proto, 0)
        expected = fixtures.EXPECTED_COUNTERS[proto]
        if tallied == expected:
            print(f"PASS  corpus: {proto} fixtures sum to {tallied}", file=sys.stderr)
        else:
            ok = False
            print(
                f"FAIL  corpus: {proto} fixtures sum to {tallied}, "
                f"counter expects {expected}",
                file=sys.stderr,
            )
    return ok


def _check_listen_host() -> bool:
    """Assert a configured ``listen_host`` round-trips into `Config.listen_host`.

    The rejection side (missing / empty / non-string) is asserted by the
    `INVALID_OPTIONS` corpus via `_check_invalid_options`; this pins the positive
    contract: a valid bind address supplied in the options payload reaches
    ``Config.listen_host`` unchanged, so the live ``_bind`` binds the configured
    interface rather than a hardcoded address. (``--check`` is offline and never
    binds a real socket, so this asserts the value plumbing, not the bind call.)
    """
    bind_host = "192.0.2.20"
    options = {**_default_check_options(), "listen_host": bind_host}
    try:
        cfg = config.validate(options)
    except ConfigError as exc:
        print(
            f"FAIL  listen-host: valid host rejected -> {exc}",
            file=sys.stderr,
        )
        return False
    ok = cfg.listen_host == bind_host
    if ok:
        print(
            f"PASS  listen-host: configured host round-trips ({bind_host})",
            file=sys.stderr,
        )
    else:
        print(
            f"FAIL  listen-host: expected {bind_host!r}, got {cfg.listen_host!r}",
            file=sys.stderr,
        )
    print(f"LISTEN-HOST CHECK {'PASSED' if ok else 'FAILED'}", file=sys.stderr)
    return ok


def _check_invalid_options() -> bool:
    """Assert invalid options are rejected with a cause-naming `ConfigError`.

    Two layers:

    1. **Field validation** — each `INVALID_OPTIONS` payload through
       `config.validate` raises a `ConfigError` naming the offending field.
    2. **File-level loading** — `config.load` against a tempfile rejects
       malformed JSON, a non-object top-level value, and a non-existent path,
       each with a `ConfigError` whose message names the cause.
    """
    ok = True
    for fixture in fixtures.INVALID_OPTIONS:
        try:
            config.validate(fixture.options)
        except ConfigError as exc:
            if fixture.field in str(exc):
                print(
                    f"PASS  invalid-options [{fixture.name}] -> {exc}",
                    file=sys.stderr,
                )
            else:
                ok = False
                print(
                    f"FAIL  invalid-options [{fixture.name}]: message "
                    f"{str(exc)!r} does not name field {fixture.field!r}",
                    file=sys.stderr,
                )
        else:
            ok = False
            print(
                f"FAIL  invalid-options [{fixture.name}]: expected ConfigError, "
                "none raised",
                file=sys.stderr,
            )
    return _check_load_negatives() and ok


def _check_load_negatives() -> bool:
    """Assert `config.load` rejects bad files with a cause-naming `ConfigError`.

    Covers the file-level branches `config.validate` (payload-only) can never
    reach: unreadable JSON, a top-level JSON array (not an object), and a
    missing file. Each must raise a `ConfigError` whose message contains the
    expected cause substring.
    """
    import tempfile
    from pathlib import Path

    ok = True

    def _assert_load_error(name: str, content: str, cause: str) -> bool:
        with tempfile.TemporaryDirectory() as tmp:
            opts = Path(tmp) / "options.json"
            opts.write_text(content, encoding="utf-8")
            try:
                config.load(str(opts))
            except ConfigError as exc:
                if cause in str(exc):
                    print(f"PASS  load-negative [{name}] -> {exc}", file=sys.stderr)
                    return True
                print(
                    f"FAIL  load-negative [{name}]: message {str(exc)!r} does "
                    f"not name cause {cause!r}",
                    file=sys.stderr,
                )
                return False
            else:
                print(
                    f"FAIL  load-negative [{name}]: expected ConfigError, none raised",
                    file=sys.stderr,
                )
                return False

    ok = _assert_load_error("malformed JSON", "{ not json", "invalid JSON") and ok
    ok = (
        _assert_load_error(
            "top-level array", '["a", "b"]', "top-level value must be an object"
        )
        and ok
    )

    # Non-existent path: load must name the unreadable cause.
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "does-not-exist.json"
        try:
            config.load(str(missing))
        except ConfigError as exc:
            if "cannot read" in str(exc):
                print(f"PASS  load-negative [missing path] -> {exc}", file=sys.stderr)
            else:
                ok = False
                print(
                    f"FAIL  load-negative [missing path]: message {str(exc)!r} "
                    "does not name cause 'cannot read'",
                    file=sys.stderr,
                )
        else:
            ok = False
            print(
                "FAIL  load-negative [missing path]: expected ConfigError, none raised",
                file=sys.stderr,
            )
    return ok


def _default_check_options() -> dict[str, object]:
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


def _resolved_config(options_path: str) -> Config | None:
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
            cfg = config.validate(_default_check_options())
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


class _CaptureWriter:
    """A `WriterProtocol` fake that records written lines (no I/O)."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def write(self, line: str) -> None:
        self.lines.append(line)

    def close(self) -> None:
        pass


def _run_check(options_path: str, storage: bool, write_error: bool) -> int:
    """Dispatch the --check variants; exit 0 only when every assertion holds."""
    _configure_logging("info")
    if write_error:
        return 0 if _check_write_error() else 1
    if storage:
        return 0 if _check_storage() else 1

    # Default --check: requires loadable options (the production default seed
    # validates), then the corpus + field + counter assertions, the warn-once
    # and internal-error survival paths, and the invalid-options corpus.
    if _resolved_config(options_path) is None:
        return 1
    ok = _check_datagrams()
    ok = _check_trace() and ok
    ok = _check_warn_once() and ok
    ok = _check_internal_error() and ok
    ok = _check_listen_host() and ok
    ok = _check_invalid_options() and ok
    if ok:
        print("CHECK PASSED", file=sys.stderr)
        return 0
    print("CHECK FAILED", file=sys.stderr)
    return 1


# --- --check --write-error ---------------------------------------------------


class _RaisingWriter:
    """A `WriterProtocol` fake whose ``write()`` always raises `WriteError`."""

    def __init__(self) -> None:
        self.write_calls = 0

    def write(self, line: str) -> None:
        from .writer import WriteError

        self.write_calls += 1
        raise WriteError("injected write failure")

    def close(self) -> None:
        pass


class _RecordingWarn:
    """A recording stub for the seam's injectable ``warn(key, message)``.

    The live loop passes its **throttled** warner here; this stub records each
    ``(key, message)`` so the oracle can assert a WARNING fired and was keyed on
    the ``client_ip`` (the audited "throttled WARNING" contract).
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def __call__(self, key: str, message: str) -> None:
        self.calls.append((key, message))


def _check_write_error() -> bool:
    """Assert the WriteError contract through the real seam."""
    import io
    import contextlib

    sources = {
        entry["ip"]: SourceMapping(
            ip=entry["ip"], site=entry["site"], host=entry["host"]
        )
        for entry in fixtures.CHECK_SOURCES
    }
    resolver = Resolver(sources)
    counters = Counters()
    writer = _RaisingWriter()
    warn = _RecordingWarn()
    fixture = fixtures.DATAGRAMS[0]

    # Capture the DEBUG trace around the FIRST call only: this is the only mode
    # that reaches the WriteError branch, where the trace renders write=error.
    logger = logging.getLogger("pysyslog")
    trace_handler = _DebugRecordingHandler()
    logger.addHandler(trace_handler)
    prev_level = logger.level
    echo = io.StringIO()
    try:
        logger.setLevel(logging.DEBUG)
        with contextlib.redirect_stdout(echo):
            process_datagram(
                fixture.raw,
                fixture.client_ip,
                resolver=resolver,
                writer=writer,
                counters=counters,
                clock=_pinned_clock,
                warn=warn,
            )
    finally:
        logger.removeHandler(trace_handler)
        logger.setLevel(prev_level)

    trace_reports_error = (
        len(trace_handler.messages) == 1 and "write=error" in trace_handler.messages[0]
    )
    warned_keyed_on_ip = len(warn.calls) == 1 and fixture.client_ip in warn.calls[0][0]
    ok = True
    checks: list[tuple[str, bool]] = [
        ("write() was called", writer.write_calls == 1),
        ("write_errors incremented to 1", counters.write_errors == 1),
        ("written did NOT increment", counters.written == 0),
        ("received incremented to 1", counters.received == 1),
        ("no stdout echo for failed datagram", echo.getvalue() == ""),
        (
            f"throttled WARNING fired, keyed on client_ip ({fixture.client_ip})",
            warned_keyed_on_ip,
        ),
        (
            "DEBUG trace fired exactly once and reported write=error",
            trace_reports_error,
        ),
    ]
    for label, passed in checks:
        if passed:
            print(f"PASS  {label}", file=sys.stderr)
        else:
            ok = False
            print(f"FAIL  {label}", file=sys.stderr)

    # The loop continues: a second datagram still processes (seam returns
    # normally, does not raise).
    try:
        process_datagram(
            fixture.raw,
            fixture.client_ip,
            resolver=resolver,
            writer=writer,
            counters=counters,
            clock=_pinned_clock,
        )
        print("PASS  seam continues after WriteError", file=sys.stderr)
    except Exception:  # pragma: no cover - contract is "does not raise"
        ok = False
        print("FAIL  seam raised on second datagram", file=sys.stderr)

    if ok:
        print("WRITE-ERROR CHECK PASSED", file=sys.stderr)
    else:
        print("WRITE-ERROR CHECK FAILED", file=sys.stderr)
    return ok


# --- --check: warn-once + internal-error survival ----------------------------


class _RecordingHandler(logging.Handler):
    """A logging handler that records WARNING-level messages from `pysyslog`.

    Used to assert the `Resolver` warn-once contract at its real emission site
    (the resolver warns through ``logging.getLogger("pysyslog")`` directly; it
    has no injectable ``warn`` callback, so a handler is the only seam).
    """

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.WARNING:
            self.messages.append(record.getMessage())


class _DebugRecordingHandler(logging.Handler):
    """A logging handler that records DEBUG-and-up messages from `pysyslog`.

    Sibling of `_RecordingHandler`, used to assert the consolidated DEBUG trace
    `server.trace_datagram` emits. Stores ``record.getMessage()`` — the lazily
    ``%``-formatted message body — so the oracle can pin the trace's
    one-physical-line and ``write=`` outcome invariants. The body carries no
    terminator (the live `StreamHandler` adds the trailing newline), so its
    embedded-newline count must be zero.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.DEBUG:
            self.messages.append(record.getMessage())


class _RaisingResolver(Resolver):
    """A `Resolver` whose ``resolve()`` raises a non-`WriteError` exception.

    Subclasses the real `Resolver` (so it satisfies the concrete type
    `process_datagram` expects) and overrides only `resolve` to drive the
    loop-level ``except Exception`` survival path: the seam is raising-
    transparent, so only an *unexpected* dependency exception (here, resolution)
    reaches `Server.handle_one`'s catch-all.
    """

    def __init__(self) -> None:
        super().__init__({})
        self.calls = 0

    def resolve(self, ip: str) -> tuple[str, str]:
        self.calls += 1
        raise RuntimeError(f"injected resolver failure for {ip}")


def _check_warn_once() -> bool:
    """Assert the resolver warns exactly once across repeats of one unknown IP.

    A fresh `Resolver` resolves the same unknown IP twice; exactly one WARNING
    must fire and the IP must land in ``seen_unknown`` (so subsequent datagrams
    from a noisy unknown sender are silent).
    """
    unknown_ip = "203.0.113.9"
    resolver = Resolver({})
    handler = _RecordingHandler()
    logger = logging.getLogger("pysyslog")
    logger.addHandler(handler)
    prev_level = logger.level
    logger.setLevel(logging.WARNING)
    try:
        first = resolver.resolve(unknown_ip)
        second = resolver.resolve(unknown_ip)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev_level)

    ok = True
    checks: list[tuple[str, bool]] = [
        (
            "both resolves stamped unknown/<ip>",
            first == second == ("unknown", unknown_ip),
        ),
        ("exactly one WARNING across two resolves", len(handler.messages) == 1),
        (f"{unknown_ip} recorded in seen_unknown", unknown_ip in resolver.seen_unknown),
    ]
    for label, passed in checks:
        if passed:
            print(f"PASS  warn-once: {label}", file=sys.stderr)
        else:
            ok = False
            print(f"FAIL  warn-once: {label}", file=sys.stderr)
    if not ok and handler.messages:
        print(f"  warnings seen: {handler.messages!r}", file=sys.stderr)
    print(
        f"WARN-ONCE CHECK {'PASSED' if ok else 'FAILED'}",
        file=sys.stderr,
    )
    return ok


def _check_internal_error() -> bool:
    """Assert the loop-level ``internal_errors`` survival path through `handle_one`.

    Builds a real `Server` and drives two datagrams through the **same**
    `Server.handle_one` the live loop uses:

    * **First** — the resolver seam is swapped for a fake whose ``resolve()``
      raises a non-`WriteError`. The exception lands in `handle_one`'s
      ``except Exception``: ``internal_errors == 1``, ``written == 0``, no stdout
      echo, and one throttled WARNING keyed on the ``client_ip``.
    * **Second** — the real resolver is restored and a normal in-corpus datagram
      is driven through. It must process to completion (``written == 1``,
      ``internal_errors`` unchanged) — proving the loop *survives* the poison
      datagram rather than merely re-attempting one.

    A `WriteError` would be the seam's own counted failure, handled inside
    `process_datagram`; only the *unexpected* resolver exception reaches this
    catch-all — the path with zero coverage until this check.
    """
    import io
    import contextlib

    cfg = config.validate(_default_check_options())
    capture = _CaptureWriter()
    server = Server(cfg, capture)
    raising = _RaisingResolver()
    # Inject the raising resolver through the public diagnostic seam so
    # handle_one's catch-all is exercised through the real Server, not a
    # re-implemented loop body; replace_resolver returns the original to restore.
    real_resolver = server.replace_resolver(raising)

    handler = _RecordingHandler()
    logger = logging.getLogger("pysyslog")
    logger.addHandler(handler)
    prev_level = logger.level
    logger.setLevel(logging.WARNING)

    client_ip = fixtures.SOURCE_IP
    raw = fixtures.DATAGRAMS[0].raw
    poison_echo = io.StringIO()
    survivor_echo = io.StringIO()
    try:
        with contextlib.redirect_stdout(poison_echo):
            server.handle_one(raw, client_ip)  # raises inside -> caught
        poison_internal = server.counters.internal_errors  # snapshot after poison
        poison_written = server.counters.written

        # Restore the real resolver; the next datagram must process cleanly.
        server.replace_resolver(real_resolver)
        with contextlib.redirect_stdout(survivor_echo):
            server.handle_one(raw, client_ip)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev_level)

    warned_keyed_on_ip = len(handler.messages) == 1 and client_ip in handler.messages[0]
    ok = True
    checks: list[tuple[str, bool]] = [
        ("poison datagram reached the resolver seam", raising.calls == 1),
        ("internal_errors incremented to 1", poison_internal == 1),
        ("written stayed 0 (poison never reached the writer)", poison_written == 0),
        ("no stdout echo for the poison datagram", poison_echo.getvalue() == ""),
        (
            f"throttled WARNING fired, keyed on client_ip ({client_ip})",
            warned_keyed_on_ip,
        ),
        (
            "loop survives: next datagram wrote one line",
            server.counters.written == 1,
        ),
        (
            "internal_errors unchanged after the survivor processed",
            server.counters.internal_errors == 1,
        ),
        ("survivor datagram echoed one line to stdout", survivor_echo.getvalue() != ""),
    ]
    for label, passed in checks:
        if passed:
            print(f"PASS  internal-error: {label}", file=sys.stderr)
        else:
            ok = False
            print(f"FAIL  internal-error: {label}", file=sys.stderr)
    if not ok and handler.messages:
        print(f"  warnings seen: {handler.messages!r}", file=sys.stderr)
    print(
        f"INTERNAL-ERROR CHECK {'PASSED' if ok else 'FAILED'}",
        file=sys.stderr,
    )
    return ok


# --- --check: DEBUG trace -----------------------------------------------------


def _check_trace() -> bool:
    """Assert the consolidated DEBUG trace contract `server.trace_datagram` owns.

    Four layers, each restoring the module logger's level/handler in ``finally``
    so a leaked DEBUG level or handler cannot corrupt later checks in the same
    process:

    * **info no-op (run first)** — at the default ``info`` level, a DEBUG handler
      attached and one in-corpus datagram driven through `process_datagram` must
      capture **zero** records (the logger's INFO level suppresses DEBUG
      emission before any handler is reached).
    * **DEBUG success path** — at ``logging.DEBUG`` the full corpus drives one
      trace per datagram; every message is one physical line (no embedded
      newline — the `StreamHandler`, not the message, owns the terminator) and
      carries ``write=written``.
    * **repr() neutralization (load-bearing)** — `trace_datagram` is called
      directly with a hand-built `SyslogRecord` whose ``program`` and
      ``sender_ts`` carry line breaks and C1 controls. No reachable datagram can
      route a line break into those parser-cleaned fields, so this direct call —
      bypassing the parser — is the only thing that pins the ``repr()`` guard:
      the captured message must stay one physical line.

    Returns ``True`` only if every layer holds.
    """
    logger = logging.getLogger("pysyslog")
    sources = {
        entry["ip"]: SourceMapping(
            ip=entry["ip"], site=entry["site"], host=entry["host"]
        )
        for entry in fixtures.CHECK_SOURCES
    }
    resolver = Resolver(sources)
    ok = True

    # Layer 0 (run FIRST, at the default info level): the DEBUG trace is a true
    # no-op below DEBUG. Drive one datagram with a DEBUG handler attached and
    # assert zero records — the logger's INFO level suppresses DEBUG emission
    # before any handler is reached.
    info_handler = _DebugRecordingHandler()
    logger.addHandler(info_handler)
    prev_level = logger.level
    try:
        logger.setLevel(logging.INFO)
        process_datagram(
            fixtures.DATAGRAMS[0].raw,
            fixtures.DATAGRAMS[0].client_ip,
            resolver=resolver,
            writer=_CaptureWriter(),
            counters=Counters(),
            clock=_pinned_clock,
        )
    finally:
        logger.removeHandler(info_handler)
        logger.setLevel(prev_level)
    if not info_handler.messages:
        print("PASS  trace: info-level emits no DEBUG trace", file=sys.stderr)
    else:
        ok = False
        print(
            f"FAIL  trace: info-level emitted {len(info_handler.messages)} "
            "DEBUG record(s); expected 0",
            file=sys.stderr,
        )

    # Layers 1+2 (success path): at DEBUG, the full corpus emits one trace per
    # datagram; each is one physical line and reports write=written. The DEBUG
    # handler also sees the resolver's own warn-once WARNING for the unknown-src
    # fixture (WARNING >= DEBUG), so the trace records are isolated by their
    # stable "datagram from " prefix — the trace's load-bearing identifier — to
    # keep "one trace per datagram" counting traces, not incidental log noise.
    handler = _DebugRecordingHandler()
    logger.addHandler(handler)
    prev_level = logger.level
    try:
        logger.setLevel(logging.DEBUG)
        for fixture in fixtures.DATAGRAMS:
            process_datagram(
                fixture.raw,
                fixture.client_ip,
                resolver=resolver,
                writer=_CaptureWriter(),
                counters=Counters(),
                clock=_pinned_clock,
            )
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev_level)

    traces = [msg for msg in handler.messages if msg.startswith("datagram from ")]
    expected_count = len(fixtures.DATAGRAMS)
    if len(traces) == expected_count:
        print(
            f"PASS  trace: one DEBUG trace per datagram ({expected_count})",
            file=sys.stderr,
        )
    else:
        ok = False
        print(
            f"FAIL  trace: captured {len(traces)} DEBUG trace(s), "
            f"expected {expected_count}",
            file=sys.stderr,
        )
    embedded = [msg for msg in traces if msg.count("\n") != 0]
    if not embedded:
        print("PASS  trace: every DEBUG trace is one physical line", file=sys.stderr)
    else:
        ok = False
        print(
            f"FAIL  trace: {len(embedded)} DEBUG trace(s) carry embedded "
            f"newline(s); first: {embedded[0]!r}",
            file=sys.stderr,
        )
    missing_outcome = [msg for msg in traces if "write=written" not in msg]
    if not missing_outcome:
        print("PASS  trace: every DEBUG trace reports write=written", file=sys.stderr)
    else:
        ok = False
        print(
            f"FAIL  trace: {len(missing_outcome)} DEBUG trace(s) lack "
            f"'write=written'; first: {missing_outcome[0]!r}",
            file=sys.stderr,
        )

    # Layer 3 (the load-bearing guard): pin repr() neutralization with a DIRECT
    # trace_datagram call carrying a hostile program/sender_ts the parser would
    # never produce. This bypasses the parser's upstream field-cleaning, so it is
    # the only assertion that proves repr() — not the grammar — keeps the trace
    # to one physical line.
    hostile = SyslogRecord(
        recv_ts=fixtures.PINNED_RECV_TS,
        protocol="rfc3164",
        priority_text="user.notice",
        program="app\nINJECT\rcr ls ps\x85nel",
        sender_ts="ts\nfake\x80c1",
        message="body",
        malformed=False,
        raw="raw",
    )
    repr_handler = _DebugRecordingHandler()
    logger.addHandler(repr_handler)
    prev_level = logger.level
    try:
        logger.setLevel(logging.DEBUG)
        trace_datagram("192.0.2.1", hostile, "home", "router1", "written")
    finally:
        logger.removeHandler(repr_handler)
        logger.setLevel(prev_level)
    if len(repr_handler.messages) == 1 and repr_handler.messages[0].count("\n") == 0:
        print(
            "PASS  trace: repr() neutralizes hostile program/sender_ts to one line",
            file=sys.stderr,
        )
    else:
        ok = False
        first_repr = repr_handler.messages[0] if repr_handler.messages else None
        print(
            "FAIL  trace: hostile program/sender_ts split the DEBUG line: "
            f"records={len(repr_handler.messages)} first={first_repr!r}",
            file=sys.stderr,
        )

    print(f"TRACE CHECK {'PASSED' if ok else 'FAILED'}", file=sys.stderr)
    return ok


# --- --check --storage -------------------------------------------------------


def _check_storage() -> bool:
    """Exercise the real `Writer` state machine deterministically.

    Each sub-check runs in its own tempdir with an injected fake clock (no real
    midnight wait): rollover, gzip atomicity + contents, prune-by-filename-date
    (incl. the re-gzipped-fresh-mtime escape case), four-state reconciliation,
    and the ENOTDIR startup-fatal path. Returns ``True`` only when all pass.
    """
    import gzip
    import tempfile
    from datetime import datetime, timedelta, timezone
    from pathlib import Path

    from .writer import WriteError, Writer

    results: list[tuple[str, bool]] = []

    def _record(label: str, passed: bool) -> None:
        results.append((label, passed))
        print(f"{'PASS' if passed else 'FAIL'}  {label}", file=sys.stderr)

    class _FakeClock:
        def __init__(self, start: datetime) -> None:
            self.value = start

        def __call__(self) -> datetime:
            return self.value

    day0 = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    # (1) rollover + (2) gzip atomicity + contents.
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=30, now=clock)
        w.write("day0 line A\n")
        w.write("day0 line B\n")
        clock.value = day0 + timedelta(days=1)
        w.write("day1 line C\n")
        w.close()
        d = Path(tmp)
        archive = d / "syslog.log.2026-06-01.gz"
        base = d / "syslog.log"
        tmp_partials = list(d.glob("*.gz.tmp"))
        _record("rollover: prior-day archive exists", archive.is_file())
        _record(
            "rollover: active base holds only the new day",
            base.read_text(encoding="utf-8") == "day1 line C\n",
        )
        if archive.is_file():
            with gzip.open(archive, "rt", encoding="utf-8") as fh:
                content = fh.read()
            _record(
                "gzip contents == day0 lines",
                content == "day0 line A\nday0 line B\n",
            )
        else:
            _record("gzip contents == day0 lines", False)
        _record("no *.gz.tmp partial remains", tmp_partials == [])

    # (3) prune-by-filename-date, incl. a re-gzipped orphan with a fresh mtime.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # An old archive far beyond retention (embedded date wins over mtime).
        old_archive = d / "syslog.log.2026-01-01.gz"
        with gzip.open(old_archive, "wt", encoding="utf-8") as fh:
            fh.write("ancient\n")
        # An orphaned *uncompressed* old file: reconciliation re-gzips it with a
        # FRESH mtime; prune must still drop it by its embedded date.
        orphan = d / "syslog.log.2026-01-02"
        orphan.write_text("orphan\n", encoding="utf-8")
        clock = _FakeClock(day0)
        Writer(tmp, "syslog.log", retention_days=30, now=clock).close()
        gone_archive = not old_archive.exists()
        regzipped = d / "syslog.log.2026-01-02.gz"
        gone_orphan_archive = not regzipped.exists() and not orphan.exists()
        _record("prune drops far-past archive by embedded date", gone_archive)
        _record(
            "prune drops re-gzipped orphan by embedded date (mtime-escape case)",
            gone_orphan_archive,
        )

    # (4) four-state reconciliation seeded at once.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        prior_day = (day0 - timedelta(days=1)).date()
        # (a) prior-day active base
        base = d / "syslog.log"
        base.write_text("stale active\n", encoding="utf-8")
        import os

        stale_ts = (day0 - timedelta(days=1)).timestamp()
        os.utime(base, (stale_ts, stale_ts))
        # (b) orphaned uncompressed file (recent enough to survive prune)
        orphan_day = (day0 - timedelta(days=2)).date()
        orphan = d / f"syslog.log.{orphan_day.isoformat()}"
        orphan.write_text("orphan body\n", encoding="utf-8")
        # (c) stale *.gz.tmp partial
        partial = d / "syslog.log.2026-05-20.gz.tmp"
        partial.write_bytes(b"partial garbage")
        # (d) source + final .gz pair (crash after replace, before unlink)
        pair_day = (day0 - timedelta(days=3)).date()
        pair_source = d / f"syslog.log.{pair_day.isoformat()}"
        pair_source.write_text("dup source\n", encoding="utf-8")
        pair_final = d / f"syslog.log.{pair_day.isoformat()}.gz"
        with gzip.open(pair_final, "wt", encoding="utf-8") as fh:
            fh.write("final already\n")

        clock = _FakeClock(day0)
        Writer(tmp, "syslog.log", retention_days=30, now=clock).close()

        rotated = d / f"syslog.log.{prior_day.isoformat()}.gz"
        orphan_gz = d / f"syslog.log.{orphan_day.isoformat()}.gz"
        _record(
            "reconcile: stale base rotated to embedded-mtime-day", rotated.is_file()
        )
        _record("reconcile: orphan gzipped", orphan_gz.is_file())
        _record("reconcile: orphan source removed", not orphan.exists())
        _record("reconcile: *.gz.tmp partial deleted", not partial.exists())
        _record(
            "reconcile: dup source deleted, final kept",
            (not pair_source.exists()) and pair_final.is_file(),
        )
        _record(
            "reconcile: no *.gz.tmp partials remain", list(d.glob("*.gz.tmp")) == []
        )

    # (5) startup-fatal: log_dir pointing at a regular file -> WriteError (ENOTDIR).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        regular_file = d / "not_a_dir"
        regular_file.write_text("i am a file\n", encoding="utf-8")
        bad_dir = regular_file / "log"
        raised = False
        try:
            Writer(str(bad_dir), "syslog.log", retention_days=30)
        except WriteError:
            raised = True
        _record("startup-fatal: regular-file log_dir raises WriteError", raised)

    ok = all(passed for _, passed in results)
    print(f"STORAGE CHECK {'PASSED' if ok else 'FAILED'}", file=sys.stderr)
    return ok


def main(argv: list[str] | None = None) -> int:
    """Parse args and dispatch to the requested mode."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.check:
        return _run_check(args.options, args.storage, args.write_error)
    if args.storage or args.write_error:
        parser.error("--storage and --write-error require --check")
    return _run_server(args.options)


if __name__ == "__main__":
    sys.exit(main())
