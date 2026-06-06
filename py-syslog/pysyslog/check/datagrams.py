# pyright: strict
"""Datagram-corpus and DEBUG-trace checks for the ``--check`` oracle."""

from __future__ import annotations

import logging
import sys

from .. import fixtures
from ..models import SourceMapping, SyslogRecord
from ..parser import parse
from ..resolver import Resolver
from ..server import Counters, process_datagram, trace_datagram
from .fakes import CaptureWriter, DebugRecordingHandler
from .options import pinned_clock

# The protocol tags the parser may emit; the datagram corpus must stay inside
# this set, and the per-protocol fixture tally must sum to the expected
# protocol counters (so the line corpus and the counter corpus cannot drift
# apart independently).
_VALID_PROTOCOLS = ("rfc3164", "rfc5424", "unknown")


def check_datagrams() -> bool:
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
    capture = CaptureWriter()

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
            reject_unknown_sources=False,
            clock=pinned_clock,
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


def check_trace() -> bool:
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
    info_handler = DebugRecordingHandler()
    logger.addHandler(info_handler)
    prev_level = logger.level
    try:
        logger.setLevel(logging.INFO)
        process_datagram(
            fixtures.DATAGRAMS[0].raw,
            fixtures.DATAGRAMS[0].client_ip,
            resolver=resolver,
            writer=CaptureWriter(),
            counters=Counters(),
            reject_unknown_sources=False,
            clock=pinned_clock,
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
    handler = DebugRecordingHandler()
    logger.addHandler(handler)
    prev_level = logger.level
    try:
        logger.setLevel(logging.DEBUG)
        for fixture in fixtures.DATAGRAMS:
            process_datagram(
                fixture.raw,
                fixture.client_ip,
                resolver=resolver,
                writer=CaptureWriter(),
                counters=Counters(),
                reject_unknown_sources=False,
                clock=pinned_clock,
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
    repr_handler = DebugRecordingHandler()
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
