# pyright: strict
"""Survival-path checks: warn-once, internal-error, and write-error contracts."""

from __future__ import annotations

import logging
import sys

from .. import config, fixtures
from ..models import SourceMapping
from ..resolver import Resolver
from ..server import Counters, Server, process_datagram
from .fakes import (
    CaptureWriter,
    DebugRecordingHandler,
    RaisingResolver,
    RaisingWriter,
    RecordingHandler,
    RecordingWarn,
)
from .options import default_check_options, pinned_clock


def check_write_error() -> bool:
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
    writer = RaisingWriter()
    warn = RecordingWarn()
    fixture = fixtures.DATAGRAMS[0]

    # Capture the DEBUG trace around the FIRST call only: this is the only mode
    # that reaches the WriteError branch, where the trace renders write=error.
    logger = logging.getLogger("pysyslog")
    trace_handler = DebugRecordingHandler()
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
                reject_unknown_sources=False,
                clock=pinned_clock,
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
            reject_unknown_sources=False,
            clock=pinned_clock,
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


def check_warn_once() -> bool:
    """Assert the resolver warns exactly once across repeats of one unknown IP.

    A fresh `Resolver` resolves the same unknown IP twice; exactly one WARNING
    must fire and the IP must land in ``seen_unknown`` (so subsequent datagrams
    from a noisy unknown sender are silent).
    """
    unknown_ip = "203.0.113.9"
    resolver = Resolver({})
    handler = RecordingHandler()
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


def check_internal_error() -> bool:
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

    cfg = config.validate(default_check_options())
    capture = CaptureWriter()
    server = Server(cfg, capture)
    raising = RaisingResolver()
    # Inject the raising resolver through the public diagnostic seam so
    # handle_one's catch-all is exercised through the real Server, not a
    # re-implemented loop body; replace_resolver returns the original to restore.
    real_resolver = server.replace_resolver(raising)

    handler = RecordingHandler()
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
