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
    from .server import Throttle, make_throttled_warn
    from .writer import Writer, WriteError

    try:
        cfg = config.load(options_path)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1
    _configure_logging(cfg.log_level)
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

    # The shipped production default (config.yaml: ``listen_host: 0.0.0.0``) must
    # stay accepted by `_require_ipv4`. The no-bind-all-in-seed invariant keeps
    # ``0.0.0.0`` out of every fixture seed, so no corpus assertion drives it
    # through `validate`; this is the one place the invariant permits the literal.
    # Without this guard a future IPv4 tightening could silently reject the
    # default while the oracle stayed green.
    bind_all = "0.0.0.0"
    bind_all_options = {**_default_check_options(), "listen_host": bind_all}
    try:
        bind_all_cfg = config.validate(bind_all_options)
    except ConfigError as exc:
        ok = False
        print(
            f"FAIL  listen-host: shipped default {bind_all!r} rejected -> {exc}",
            file=sys.stderr,
        )
    else:
        if bind_all_cfg.listen_host == bind_all:
            print(
                f"PASS  listen-host: shipped default round-trips ({bind_all})",
                file=sys.stderr,
            )
        else:
            ok = False
            print(
                f"FAIL  listen-host: expected {bind_all!r}, "
                f"got {bind_all_cfg.listen_host!r}",
                file=sys.stderr,
            )

    print(f"LISTEN-HOST CHECK {'PASSED' if ok else 'FAILED'}", file=sys.stderr)
    return ok


def _check_size_guard_config() -> bool:
    """Assert valid size-guard knobs round-trip into the `Config` unchanged.

    The rejection side (out-of-range percents/MB, and the coherence gate) is
    asserted by the `INVALID_OPTIONS` corpus via `_check_invalid_options`; this
    pins the positive contract, mirroring `_check_listen_host`: a coherent guard
    config (both percents set, segment rotation enabled) reaches
    ``Config.min_free_percent`` / ``max_log_percent`` / ``max_segment_mb``
    unchanged, so the live Writer is wired with the operator's values.
    """
    options = {
        **_default_check_options(),
        "min_free_percent": 10,
        "max_log_percent": 25,
        "max_segment_mb": 64,
    }
    try:
        cfg = config.validate(options)
    except ConfigError as exc:
        print(
            f"FAIL  size-guard-config: valid guard rejected -> {exc}",
            file=sys.stderr,
        )
        print("SIZE-GUARD-CONFIG CHECK FAILED", file=sys.stderr)
        return False
    ok = (cfg.min_free_percent, cfg.max_log_percent, cfg.max_segment_mb) == (
        10,
        25,
        64,
    )
    if ok:
        print(
            "PASS  size-guard-config: coherent guard round-trips "
            "(min_free=10 max_log=25 segment=64MB)",
            file=sys.stderr,
        )
    else:
        print(
            "FAIL  size-guard-config: expected (10, 25, 64), got "
            f"({cfg.min_free_percent}, {cfg.max_log_percent}, {cfg.max_segment_mb})",
            file=sys.stderr,
        )
    print(
        f"SIZE-GUARD-CONFIG CHECK {'PASSED' if ok else 'FAILED'}",
        file=sys.stderr,
    )
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


class _FakeStats:
    """A zero-valued `WriterStats` stand-in for the `WriterProtocol` fakes.

    The fakes exercise the datagram path, not the size guard, so the guard
    counters stay 0 — which is also what `EXPECTED_COUNTERS` asserts.
    """

    size_rotations = 0
    space_prunes = 0
    bytes_reclaimed = 0


class _CaptureWriter:
    """A `WriterProtocol` fake that records written lines (no I/O)."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.stats = _FakeStats()

    def write(self, line: str) -> None:
        self.lines.append(line)

    def close(self) -> None:
        pass

    def disk_free_pct(self) -> int | None:
        return None

    def log_dir_mb(self) -> int | None:
        return None

    def enforce_space_tick(self) -> None:
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
    ok = _check_size_guard_config() and ok
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
        self.stats = _FakeStats()

    def write(self, line: str) -> None:
        from .writer import WriteError

        self.write_calls += 1
        raise WriteError("injected write failure")

    def close(self) -> None:
        pass

    def disk_free_pct(self) -> int | None:
        return None

    def log_dir_mb(self) -> int | None:
        return None

    def enforce_space_tick(self) -> None:
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
    and the ENOTDIR startup-fatal path.

    Cases A–I add the size guard: size-rotation into numbered segments (A),
    ``(date, seq)`` prune ordering / keep-newest (B), both-constraint pruning
    (C), restart-safe next-seq (D), ring-buffer keep-newest end-to-end (E), the
    only-active-file terminal (F), statvfs-failure degrade-safe (G), the
    seq-overflow terminal (H), and guard-disabled == 1.2.0 (I). Cases J–M close
    the audited branch gaps: numbered-orphan reconciliation (J), next-seq scans
    AFTER reconcile (K), the in-loop ``space:scandir`` measurement-failure branch
    distinct from the pre-loop ``space:statvfs`` (M1), the ``space:unlink``
    prune-failure branch (M2), and the live ``disk_free_pct`` / ``log_dir_mb``
    gauges incl. their None paths and regular-files-only byte sum (M3/M4). L2/L3
    pin a single-oversized-line roll and byte-exact segment contents. Size-rolls
    are driven deterministically by writing fixed large lines past a low
    ``max_segment_mb``; the free-space dimension is driven via the injectable
    ``volume_stats`` seam (no real disk pressure). Returns ``True`` only when all
    pass.
    """
    import gzip
    import os
    import tempfile
    from datetime import datetime, timedelta, timezone
    from pathlib import Path

    from .writer import WriteError, Writer, segment_sort_key

    results: list[tuple[str, bool]] = []

    def _record(label: str, passed: bool) -> None:
        results.append((label, passed))
        print(f"{'PASS' if passed else 'FAIL'}  {label}", file=sys.stderr)

    class _FakeClock:
        def __init__(self, start: datetime) -> None:
            self.value = start

        def __call__(self) -> datetime:
            return self.value

    class _FakeVolume:
        """Mutable ``(total, free)`` stand-in for the injectable volume_stats seam.

        ``free`` can be re-pinned mid-test so a prune (which the guard re-measures
        as freeing space) can be modeled, and ``total`` fixed so the percentage
        budgets are deterministic without real disk pressure.
        """

        def __init__(self, total: int, free: int) -> None:
            self.total = total
            self.free = free

        def __call__(self, _path: Path) -> tuple[int, int]:
            return (self.total, self.free)

    class _RaisingVolume:
        """An injectable volume_stats seam that always raises ``OSError``."""

        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, _path: Path) -> tuple[int, int]:
            self.calls += 1
            raise OSError("injected statvfs failure")

    # A 128 KiB line: 8 writes == exactly 1 MiB, so a Writer with
    # ``max_segment_mb=1`` rolls on every 8th write — deterministic crossings
    # with no real disk pressure.
    _LINE_128K = "x" * (128 * 1024 - 1) + "\n"

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

    # === Size guard (Cases A–I) ===

    def _prune_victims(warn: _RecordingWarn) -> list[str]:
        """The segment names pruned, in prune order, parsed from space:prune warns."""
        names: list[str] = []
        for key, message in warn.calls:
            if key == "space:prune":
                # "space guard pruned <name> (disk_free=..%...); kept newest"
                names.append(message.split("pruned ", 1)[1].split(" ", 1)[0])
        return names

    # (A) size-rotation produces numbered segments.
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        warn = _RecordingWarn()
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=30,
            now=clock,
            max_segment_mb=1,
            warn=warn,
        )
        for _ in range(16):  # 16 * 128 KiB == 2 MiB -> exactly two rolls
            w.write(_LINE_128K)
        w.write("post-002 tail\n")
        w.close()
        d = Path(tmp)
        seg1 = d / "syslog.log.2026-06-01.001.gz"
        seg2 = d / "syslog.log.2026-06-01.002.gz"
        base = d / "syslog.log"
        _record("A: size-roll segment 001 exists", seg1.is_file())
        _record("A: size-roll segment 002 exists", seg2.is_file())
        _record("A: size_rotations == 2", w.stats.size_rotations == 2)
        _record(
            "A: active base holds only the post-002 tail",
            base.read_text(encoding="utf-8") == "post-002 tail\n",
        )
        if seg1.is_file():
            with gzip.open(seg1, "rt", encoding="utf-8") as fh:
                seg1_lines = fh.read().count("\n")
            _record("A: segment 001 gzip preserved 8 lines", seg1_lines == 8)
        else:
            _record("A: segment 001 gzip preserved 8 lines", False)
        _record("A: no *.gz.tmp partial remains", list(d.glob("*.gz.tmp")) == [])

    # (B) (date, seq) prune ordering / keep-newest, plus direct sort-key pin.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # Seed daily + numbered segments OUT of lexical order on disk, each an
        # exact 1000-byte ".gz" (raw bytes; the prune path never decompresses).
        seeded = [
            "syslog.log.2026-06-02.001.gz",  # newest (survivor)
            "syslog.log.2026-06-01.002.gz",
            "syslog.log.2026-06-01.gz",  # bare daily: (date, -1), oldest in its day
            "syslog.log.2026-06-01.001.gz",
        ]
        for name in seeded:
            (d / name).write_bytes(b"\x00" * 1000)
        clock = _FakeClock(day0)
        warn = _RecordingWarn()
        # total fixed, free huge (floor never trips); cap = 10% of 10_000 = 1000
        # bytes admits exactly one segment, so the loop prunes oldest-first until
        # only the highest-(date, seq) segment survives.
        vol = _FakeVolume(total=10_000, free=10_000)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,  # retention must not prune the seeded segments
            now=clock,
            max_log_percent=10,  # cap = 1000 bytes -> exactly one segment fits
            max_segment_mb=1,
            warn=warn,
            volume_stats=vol,
        )
        w.enforce_space_tick()
        w.close()
        victims = _prune_victims(warn)
        survivors = sorted(p.name for p in d.glob("syslog.log.*.gz"))
        keys_sorted = victims == sorted(victims, key=segment_sort_key)
        _record("B: victims pruned in ascending (date, seq) order", keys_sorted)
        _record(
            "B: bare daily (date,-1) pruned before same-day 001",
            "syslog.log.2026-06-01.gz" in victims
            and (
                victims.index("syslog.log.2026-06-01.gz")
                < victims.index("syslog.log.2026-06-01.001.gz")
            ),
        )
        _record(
            "B: highest (date, seq) segment survives",
            survivors == ["syslog.log.2026-06-02.001.gz"],
        )
        # Direct sort-key pin on a hand-built name list (independent of pruning).
        hand = [
            "syslog.log.2026-06-01.002.gz",
            "syslog.log.2026-06-01.gz",
            "syslog.log.2026-06-02.001.gz",
            "syslog.log.2026-06-01.001.gz",
        ]
        expect = [
            "syslog.log.2026-06-01.gz",  # (2026-06-01, -1)
            "syslog.log.2026-06-01.001.gz",
            "syslog.log.2026-06-01.002.gz",
            "syslog.log.2026-06-02.001.gz",
        ]
        _record(
            "B: segment_sort_key orders daily<001<002<next-day",
            sorted(hand, key=segment_sort_key) == expect,
        )

    # (C) both-constraint prune: the floor dimension is driven via the injected
    # volume_stats seam (free climbs as segments are pruned), and the loop exits
    # the moment BOTH predicates hold — not before, not after.
    class _SteppingVolume:
        """A volume whose free space climbs one step per *consumed* read.

        Models the reality that each prune frees space: every call returns the
        current free, then advances it by ``step`` for the next iteration, so the
        guard's re-measure-each-iteration loop observes free rising toward the
        floor. ``total`` is fixed for deterministic percentage budgets.
        """

        def __init__(self, total: int, free: int, step: int) -> None:
            self.total = total
            self.free = free
            self.step = step

        def __call__(self, _path: Path) -> tuple[int, int]:
            current = self.free
            self.free += self.step
            return (self.total, current)

    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # Five exact-100-byte ".gz" files (raw bytes; the prune path never
        # decompresses, it only stat()s + unlink()s). Both floor and cap active.
        # Basis total=1000: floor=30% -> free must reach 300; cap=40% -> log_dir
        # must fall to <=400 bytes (i.e. <=4 segments). Floor binds harder here.
        for i in range(1, 6):
            (d / f"syslog.log.2026-06-01.{i:03d}.gz").write_bytes(b"\x00" * 100)
        clock = _FakeClock(day0)
        warn = _RecordingWarn()
        # free starts 240 (<300 floor) and climbs +20 per read; the pre-loop read
        # consumes 240->260, then each loop-top read advances. Floor (free>=300)
        # is reached after enough reads that two prunes have occurred.
        stepping = _SteppingVolume(total=1000, free=240, step=20)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            min_free_percent=30,  # floor: free >= 300
            max_log_percent=40,  # cap: log_dir <= 400 bytes (<= 4 segments)
            max_segment_mb=1,
            warn=warn,
            volume_stats=stepping,
        )
        before_c = 5
        w.enforce_space_tick()
        w.close()
        after_c = len(list(d.glob("syslog.log.*.gz")))
        pruned_c = before_c - after_c
        # It cannot exit before the floor is met (free starts below floor and only
        # the prune loop's re-reads raise it), so at least one prune must occur.
        _record("C: both-constraint loop pruned at least one segment", pruned_c >= 1)
        # And it must NOT over-prune to zero: once both predicates hold it stops,
        # so survivors remain (both limits are satisfiable here).
        _record(
            "C: both-constraint loop stops once both hold (survivors remain)",
            after_c >= 1,
        )

    # (C-cap) cap-only: pruning stops as soon as log_dir <= cap (no over-prune).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # Four exact-100-byte segments (400 bytes of logs). Floor disabled (free
        # 100%); cap basis total=500 -> cap=60% = 300 bytes -> holds 3 segments,
        # so exactly one is pruned and three survive (no over-prune).
        for i in range(1, 5):
            (d / f"syslog.log.2026-06-01.{i:03d}.gz").write_bytes(b"\x00" * 100)
        clock = _FakeClock(day0)
        warn = _RecordingWarn()
        vol = _FakeVolume(total=500, free=500)  # free 100% -> floor never trips
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            max_log_percent=60,  # cap = 300 bytes -> 3 segments fit
            max_segment_mb=1,
            warn=warn,
            volume_stats=vol,
        )
        w.enforce_space_tick()
        w.close()
        survivors_cap = len(list(d.glob("syslog.log.*.gz")))
        _record(
            "C-cap: cap-only stops as soon as log_dir <= cap (3 survive)",
            survivors_cap == 3,
        )

    # (D) restart-safe next-seq: a second Writer in the same dir continues seq.
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=30, now=clock, max_segment_mb=1)
        for _ in range(16):  # two rolls -> 001, 002
            w.write(_LINE_128K)
        w.close()
        d = Path(tmp)
        had_002 = (d / "syslog.log.2026-06-01.002.gz").is_file()
        # Second Writer, same dir + same UTC day: next roll must be 003, no clobber.
        clock2 = _FakeClock(day0)
        w2 = Writer(tmp, "syslog.log", retention_days=30, now=clock2, max_segment_mb=1)
        for _ in range(8):  # one more roll
            w2.write(_LINE_128K)
        w2.close()
        had_003 = (d / "syslog.log.2026-06-01.003.gz").is_file()
        still_002 = (d / "syslog.log.2026-06-01.002.gz").is_file()
        _record("D: first Writer produced 002", had_002)
        _record("D: restart continues at next free seq (003)", had_003)
        _record("D: prior 002 segment not clobbered", still_002)

    # (E) ring-buffer keep-newest end-to-end: tight cap, many segments, newest K
    # survive; active base intact; counters reflect deletions.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        seeded_e = [f"syslog.log.2026-06-01.{i:03d}.gz" for i in range(1, 7)]
        for name in seeded_e:  # exact 2000-byte raw ".gz" files
            (d / name).write_bytes(b"\x00" * 2000)
        clock = _FakeClock(day0)
        warn = _RecordingWarn()
        vol = _FakeVolume(total=1_000_000, free=999_999)  # floor never trips
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            max_log_percent=1,  # cap = 10_000 bytes -> only newest few survive
            max_segment_mb=1,
            warn=warn,
            volume_stats=vol,
        )
        w.write("active stays\n")
        before_e = len(seeded_e)
        w.enforce_space_tick()
        base_text = (d / "syslog.log").read_text(encoding="utf-8")
        w.close()
        survivors_e = sorted(p.name for p in d.glob("syslog.log.*.gz"))
        # Survivors must be a newest-suffix of the seeded ascending list.
        survived_count = len(survivors_e)
        keep_newest = (
            survived_count < before_e
            and survivors_e == seeded_e[before_e - survived_count :]
        )
        _record("E: ring buffer kept the newest K segments", keep_newest)
        _record("E: active base intact after pruning", base_text == "active stays\n")
        _record(
            "E: space_prunes counts the deletions",
            w.stats.space_prunes == before_e - survived_count,
        )
        _record("E: bytes_reclaimed > 0", w.stats.bytes_reclaimed > 0)

    # (F) terminal: only active file left, still over budget.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # One prunable segment; cap so tight it can never be satisfied by pruning.
        with gzip.open(
            d / "syslog.log.2026-06-01.001.gz", "wt", encoding="utf-8"
        ) as fh:
            fh.write("w" * 4096 + "\n")
        clock = _FakeClock(day0)
        warn = _RecordingWarn()
        vol = _FakeVolume(total=1_000_000, free=1)  # free ~0% << any floor
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            min_free_percent=50,  # floor unsatisfiable by pruning logs
            max_segment_mb=1,
            warn=warn,
            volume_stats=vol,
        )
        w.write("active line\n")
        w.enforce_space_tick()  # must terminate, not loop forever
        base_text = (d / "syslog.log").read_text(encoding="utf-8")
        w.close()
        exhausted = [k for k, _ in warn.calls if k == "space:exhausted"]
        _record("F: guard terminates (no infinite loop)", True)
        _record("F: active base not deleted/truncated", base_text == "active line\n")
        _record("F: exactly one space:exhausted WARNING", len(exhausted) == 1)
        _record(
            "F: space_prunes counts only the removable segment",
            w.stats.space_prunes == 1,
        )

    # (G) statvfs failure degrades safe.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        with gzip.open(
            d / "syslog.log.2026-06-01.001.gz", "wt", encoding="utf-8"
        ) as fh:
            fh.write("g" * 1024 + "\n")
        clock = _FakeClock(day0)
        warn = _RecordingWarn()
        raising_vol = _RaisingVolume()
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            max_log_percent=1,
            max_segment_mb=1,
            warn=warn,
            volume_stats=raising_vol,
        )
        w.enforce_space_tick()  # statvfs raises -> warn, no prune
        statvfs_warns = [k for k, _ in warn.calls if k == "space:statvfs"]
        seg_present = (d / "syslog.log.2026-06-01.001.gz").is_file()
        # Subsequent write() still works (write path does not touch volume_stats
        # when no size-roll occurs; here the active file is well under 1 MiB).
        wrote_ok = True
        try:
            w.write("after statvfs failure\n")
        except WriteError:
            wrote_ok = False
        w.close()
        _record("G: one space:statvfs WARNING", len(statvfs_warns) == 1)
        _record("G: no prune occurred (segment intact)", seg_present)
        _record("G: no space_prunes counted", w.stats.space_prunes == 0)
        _record("G: subsequent write() works normally", wrote_ok)

    # (H) seq overflow terminal: pre-seed 999, drive next-seq > 999.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        with gzip.open(
            d / "syslog.log.2026-06-01.999.gz", "wt", encoding="utf-8"
        ) as fh:
            fh.write("seeded 999\n")
        clock = _FakeClock(day0)
        warn = _RecordingWarn()
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            max_segment_mb=1,
            warn=warn,
        )
        for _ in range(8):  # cross 1 MiB -> next seq would be 1000
            w.write(_LINE_128K)
        # The base is now over-threshold; one more write proves it keeps
        # accepting past the seq-overflow terminal (the un-throttled recording
        # stub records each over-threshold write's warn; the live server throttles
        # them to one per window).
        w.write("still accepting\n")
        w.close()
        overflow_keys = {k for k, _ in warn.calls if k == "space:seq-overflow"}
        overflow_count = len([k for k, _ in warn.calls if k == "space:seq-overflow"])
        no_1000 = not (d / "syslog.log.2026-06-01.1000.gz").exists()
        base_text = (d / "syslog.log").read_text(encoding="utf-8")
        _record(
            "H: seq-overflow WARNING fired (single throttle key)",
            overflow_count >= 1 and overflow_keys == {"space:seq-overflow"},
        )
        _record("H: no segment beyond 999 produced", no_1000)
        _record(
            "H: size_rotations stayed 0 (roll skipped)", w.stats.size_rotations == 0
        )
        _record(
            "H: active file keeps accepting writes",
            base_text.endswith("still accepting\n"),
        )

    # (I) guard disabled == 1.2.0: all three knobs 0 -> only daily archives.
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=30, now=clock)  # all knobs 0
        for _ in range(16):  # would roll twice IF size-rotation were enabled
            w.write(_LINE_128K)
        clock.value = day0 + timedelta(days=1)
        w.write("day1\n")
        w.close()
        d = Path(tmp)
        numbered = list(d.glob("syslog.log.*.[0-9][0-9][0-9].gz"))
        daily = d / "syslog.log.2026-06-01.gz"
        _record("I: no numbered segments produced when disabled", numbered == [])
        _record("I: daily archive still produced", daily.is_file())
        _record("I: size_rotations == 0 when disabled", w.stats.size_rotations == 0)
        _record("I: space_prunes == 0 when disabled", w.stats.space_prunes == 0)

    # (J) reconcile of NUMBERED orphans (the size-segment trio): three states
    # seeded at once for a prior UTC day, mirroring the daily-form Case (4) but
    # for the <date>.<NNN> forms the size guard introduced.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        prior = (day0 - timedelta(days=1)).date().isoformat()
        # (a) uncompressed numbered orphan, NO final -> re-gzipped, source gone.
        orphan_a = d / f"syslog.log.{prior}.001"
        orphan_a.write_text("orphan 001 body\n", encoding="utf-8")
        # (b) uncompressed numbered orphan WITH an existing final .gz (crash after
        #     replace, before unlink) -> redundant source removed, final kept.
        orphan_b_src = d / f"syslog.log.{prior}.002"
        orphan_b_src.write_text("dup 002 source\n", encoding="utf-8")
        orphan_b_final = d / f"syslog.log.{prior}.002.gz"
        with gzip.open(orphan_b_final, "wt", encoding="utf-8") as fh:
            fh.write("002 final already\n")
        # (c) stale numbered *.gz.tmp partial -> deleted, none remain.
        partial_c = d / f"syslog.log.{prior}.003.gz.tmp"
        partial_c.write_bytes(b"partial 003 garbage")

        clock = _FakeClock(day0)
        Writer(tmp, "syslog.log", retention_days=30, now=clock).close()

        regz_a = d / f"syslog.log.{prior}.001.gz"
        _record(
            "J: numbered uncompressed orphan re-gzipped, source removed",
            regz_a.is_file() and not orphan_a.exists(),
        )
        if regz_a.is_file():
            with gzip.open(regz_a, "rt", encoding="utf-8") as fh:
                regz_a_body = fh.read()
            _record(
                "J: re-gzipped numbered orphan preserves its body",
                regz_a_body == "orphan 001 body\n",
            )
        else:
            _record("J: re-gzipped numbered orphan preserves its body", False)
        if orphan_b_final.is_file():
            with gzip.open(orphan_b_final, "rt", encoding="utf-8") as fh:
                orphan_b_body = fh.read()
        else:
            orphan_b_body = ""
        _record(
            "J: numbered dup source removed, existing final kept untouched",
            orphan_b_final.is_file()
            and not orphan_b_src.exists()
            and orphan_b_body == "002 final already\n",
        )
        _record(
            "J: numbered *.gz.tmp partial deleted, none remain",
            not partial_c.exists() and list(d.glob("*.gz.tmp")) == [],
        )

    # (K) next-seq scans AFTER reconcile: a same-UTC-day uncompressed orphan
    # (005) is re-gzipped at construction, so the FIRST live size-roll must land
    # on 006 (proving _next_seq reads the post-reconcile segment set, not a
    # pre-reconcile snapshot that would collide at 001).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        today = day0.date().isoformat()
        orphan_005 = d / f"syslog.log.{today}.005"
        orphan_005.write_text("pre-existing 005\n", encoding="utf-8")
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=30, now=clock, max_segment_mb=1)
        regz_005 = d / f"syslog.log.{today}.005.gz"
        for _ in range(8):  # cross 1 MiB exactly once -> one live roll
            w.write(_LINE_128K)
        w.close()
        seg_006 = d / f"syslog.log.{today}.006.gz"
        clobber_001 = d / f"syslog.log.{today}.001.gz"
        _record(
            "K: same-day orphan re-gzipped to 005 at construction", regz_005.is_file()
        )
        _record(
            "K: first live roll lands on 006 (next-seq is post-reconcile)",
            seg_006.is_file(),
        )
        _record("K: roll did not collide back at 001", not clobber_001.exists())

    # (M1) in-loop measurement failure -> space:scandir, degrade-safe. The volume
    # seam succeeds on the pre-loop read (so _enforce_space enters the prune loop)
    # then raises on the next read (the loop-top re-measure), exercising the
    # in-loop except branch distinct from the pre-loop space:statvfs branch.
    class _FailAfterVolume:
        """Succeeds for the first ``ok_calls`` reads, then raises ``OSError``."""

        def __init__(self, total: int, free: int, ok_calls: int) -> None:
            self.total = total
            self.free = free
            self.ok_calls = ok_calls
            self.calls = 0

        def __call__(self, _path: Path) -> tuple[int, int]:
            self.calls += 1
            if self.calls > self.ok_calls:
                raise OSError("injected in-loop measurement failure")
            return (self.total, self.free)

    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        with gzip.open(
            d / "syslog.log.2026-06-01.001.gz", "wt", encoding="utf-8"
        ) as fh:
            fh.write("m1" * 1024 + "\n")
        clock = _FakeClock(day0)
        warn = _RecordingWarn()
        # Pre-loop read (call 1) returns free 0% -> floor unmet -> enter loop;
        # the loop-top re-measure (call 2) raises -> space:scandir, no prune.
        fail_after = _FailAfterVolume(total=1_000_000, free=1, ok_calls=1)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            min_free_percent=50,
            max_segment_mb=1,
            warn=warn,
            volume_stats=fail_after,
        )
        w.enforce_space_tick()
        scandir_warns = [k for k, _ in warn.calls if k == "space:scandir"]
        statvfs_warns_m1 = [k for k, _ in warn.calls if k == "space:statvfs"]
        seg_intact = (d / "syslog.log.2026-06-01.001.gz").is_file()
        wrote_ok_m1 = True
        try:
            w.write("after scandir failure\n")
        except WriteError:
            wrote_ok_m1 = False
        w.close()
        _record("M1: exactly one space:scandir WARNING", len(scandir_warns) == 1)
        _record(
            "M1: pre-loop space:statvfs did NOT fire (read 1 succeeded)",
            statvfs_warns_m1 == [],
        )
        _record("M1: no prune occurred (segment intact)", seg_intact)
        _record("M1: no space_prunes counted", w.stats.space_prunes == 0)
        _record("M1: subsequent write() works normally", wrote_ok_m1)

    # (M2) unlink failure mid-prune -> space:unlink, degrade-safe. With no inject
    # seam for unlink, the parent dir is made non-writable (0o500) so victim
    # .unlink() raises PermissionError (an OSError); permissions are restored in
    # finally so the TemporaryDirectory can clean up.
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        (d / "syslog.log.2026-06-01.001.gz").write_bytes(b"\x00" * 1000)
        base_m2 = d / "syslog.log"
        base_m2.write_text("active m2\n", encoding="utf-8")
        clock = _FakeClock(day0)
        warn = _RecordingWarn()
        # cap so tight the lone segment is over budget -> guard tries to prune it.
        vol = _FakeVolume(total=10_000, free=10_000)  # floor disabled (free 100%)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            max_log_percent=1,  # cap = 100 bytes; the 1000-byte segment is over
            max_segment_mb=1,
            warn=warn,
            volume_stats=vol,
        )
        os.chmod(d, 0o500)  # read+execute, no write -> unlink raises
        try:
            w.enforce_space_tick()  # must warn, not crash
        finally:
            os.chmod(d, 0o700)  # restore for assertions + cleanup
        unlink_warns = [k for k, _ in warn.calls if k == "space:unlink"]
        seg_still = (d / "syslog.log.2026-06-01.001.gz").is_file()
        base_still = base_m2.read_text(encoding="utf-8")
        w.close()
        _record("M2: exactly one space:unlink WARNING", len(unlink_warns) == 1)
        _record("M2: no crash; loop returned after the failed unlink", True)
        _record(
            "M2: space_prunes NOT counted on a failed unlink", w.stats.space_prunes == 0
        )
        _record("M2: un-unlinkable victim segment still present", seg_still)
        _record("M2: active base untouched", base_still == "active m2\n")

    # (M3 / M4) the live gauges Writer.disk_free_pct / log_dir_mb feed the stats
    # line. M3 pins the happy figures + the two None paths; M4 pins that the
    # log-dir byte sum counts ONLY regular files (skips a subdir + a symlink).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        # Two known-size regular .gz files: 3 MiB + 1 MiB = 4 MiB of logs.
        (d / "syslog.log.2026-06-01.001.gz").write_bytes(b"\x00" * (3 * 1024 * 1024))
        (d / "syslog.log.2026-06-01.002.gz").write_bytes(b"\x00" * (1 * 1024 * 1024))
        clock = _FakeClock(day0)
        vol = _FakeVolume(total=1000, free=250)  # 25% free
        w = Writer(tmp, "syslog.log", retention_days=3650, now=clock, volume_stats=vol)
        # Writer.__init__ opens an (empty) active base; it is a regular file but
        # contributes 0 bytes, so the whole-MB figure stays the segments' 4 MiB.
        _record("M3: disk_free_pct == free*100//total (25)", w.disk_free_pct() == 25)
        _record(
            "M3: log_dir_mb sums regular files to whole MB (4)", w.log_dir_mb() == 4
        )

        # M4: a subdirectory and a symlink-to-large-file must NOT count toward
        # the log-dir byte sum (is_file(follow_symlinks=False) skips both).
        (d / "subdir").mkdir()
        (d / "subdir" / "big.gz").write_bytes(b"\x00" * (5 * 1024 * 1024))
        large_target = d / "target_large.bin"
        large_target.write_bytes(b"\x00" * (9 * 1024 * 1024))
        link = d / "syslog.log.2026-06-01.003.gz"
        link.symlink_to(large_target)
        _record(
            "M4: log_dir_mb ignores subdir + symlink (still 4 + target_large)",
            # target_large.bin is itself a regular file at the top level (9 MiB),
            # so the regular-file sum is now 4 + 9 == 13 MiB; the 5 MiB subdir
            # file and the symlink (which points at the same 9 MiB) add nothing.
            w.log_dir_mb() == 13,
        )
        w.close()

    # (M3-None) the two None gauge paths: a raising volume seam, and a degenerate
    # total<=0 volume. Both must render the stats line as '?' (return None) rather
    # than crash. Separate Writers so each sees only its own volume seam.
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            volume_stats=_RaisingVolume(),
        )
        _record(
            "M3: disk_free_pct() is None on measurement OSError",
            w.disk_free_pct() is None,
        )
        w.close()
    with tempfile.TemporaryDirectory() as tmp:
        clock = _FakeClock(day0)
        w = Writer(
            tmp,
            "syslog.log",
            retention_days=3650,
            now=clock,
            volume_stats=_FakeVolume(total=0, free=0),
        )
        _record(
            "M3: disk_free_pct() is None when total <= 0", w.disk_free_pct() is None
        )
        w.close()

    # (L2) a single oversized line (~1.5 MiB) lands in the active base, and the
    # NEXT write triggers exactly one roll whose segment holds the full oversized
    # line intact (the active file may exceed the threshold by one datagram).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=3650, now=clock, max_segment_mb=1)
        big_line = "z" * (3 * 512 * 1024 - 1) + "\n"  # ~1.5 MiB, single line
        w.write(big_line)  # over 1 MiB after this write -> rolls on this call
        seg_l2 = d / "syslog.log.2026-06-01.001.gz"
        _record("L2: oversized single line triggers one roll", seg_l2.is_file())
        _record("L2: exactly one size_rotation", w.stats.size_rotations == 1)
        if seg_l2.is_file():
            with gzip.open(seg_l2, "rt", encoding="utf-8") as fh:
                seg_l2_body = fh.read()
            _record(
                "L2: rolled segment holds the full oversized line",
                seg_l2_body == big_line,
            )
        else:
            _record("L2: rolled segment holds the full oversized line", False)
        w.close()

    # (L3) segment gzip content is byte-exact: the rolled .gz decompresses to the
    # exact concatenation of the lines written into it (Case A checks line COUNT;
    # this pins the bytes, catching a future truncation/interleave regression).
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        clock = _FakeClock(day0)
        w = Writer(tmp, "syslog.log", retention_days=3650, now=clock, max_segment_mb=1)
        written_lines = [f"L3 line {i:04d}\n" for i in range(8)]
        body = "".join(written_lines)
        # Pad to just over 1 MiB so exactly one roll captures these 8 lines plus
        # a trailing filler line; assert the segment holds the 8 lines verbatim.
        w.write(body)
        w.write(_LINE_128K * 8)  # push the active file past 1 MiB -> one roll
        seg_l3 = d / "syslog.log.2026-06-01.001.gz"
        if seg_l3.is_file():
            with gzip.open(seg_l3, "rt", encoding="utf-8") as fh:
                seg_l3_body = fh.read()
            _record(
                "L3: rolled segment begins with the exact written lines",
                seg_l3_body.startswith(body),
            )
        else:
            _record("L3: rolled segment begins with the exact written lines", False)
        w.close()

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
