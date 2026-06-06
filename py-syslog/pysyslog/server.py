# pyright: strict
"""UDP receive loop + the shared ``process_datagram`` seam.

`process_datagram(data, client_ip, *, resolver, writer, counters, clock)` is the
single per-datagram pipeline. It is used by **both** the live ``recvfrom`` loop
and the ``--check`` / ``--write-error`` oracles, so the failure paths are tested
through the exact production code. Every dependency is an explicit parameter
(no module globals): `resolver` owns the warn-once set, `writer` is typed
`WriterProtocol`, `clock` is the injectable receive-time source, and `counters`
is mutated in a **fixed order**.

The seam is **raising-transparent**: it does not catch unexpected exceptions
(the loop level does, so the oracle can share the seam unchanged). It captures
``recv_ts = clock()`` once at entry and passes it into ``parse(raw, recv_ts)`` —
the parser reads no clock.
"""

from __future__ import annotations

import errno
import logging
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Callable

from .models import Config, SyslogRecord, WriterProtocol, format_line
from .parser import parse
from .resolver import Resolver

_log = logging.getLogger("pysyslog")

# Throttle window (seconds) for repeated WARNING bursts so one misbehaving
# sender cannot flood stderr.
_WARN_THROTTLE_S = 10.0
# Periodic stats cadence (seconds), driven off the 1 s recvfrom timeout tick.
_STATS_INTERVAL_S = 600.0
# recvfrom socket timeout (seconds): the normal per-tick shutdown re-check.
_RECV_TIMEOUT_S = 1.0
_MAX_DATAGRAM = 65535


class Counters:
    """Mutable per-run counters, mutated by `process_datagram` in fixed order.

    `unknown` is the **protocol**-unknown count (a malformed parse yields
    ``protocol="unknown"``); `unknown_source` is the **resolver**-miss count.
    They are distinct so the stats line reports both.
    """

    def __init__(self) -> None:
        self.received = 0
        self.rfc3164 = 0
        self.rfc5424 = 0
        self.unknown = 0  # protocol unknown (malformed parse)
        self.malformed = 0
        self.unknown_source = 0
        self.rejected_sources = 0
        self.written = 0
        self.write_errors = 0
        self.internal_errors = 0
        # Size-guard counters: the live `Writer` owns the authoritative values
        # (its `stats` object); the server copies them in at stats-emit time.
        # The datagram corpus uses a capture writer (no guard), so they stay 0
        # there — which `EXPECTED_COUNTERS` asserts.
        self.size_rotations = 0
        self.space_prunes = 0
        self.bytes_reclaimed = 0

    def as_dict(self) -> dict[str, int]:
        """Snapshot the counters as a plain dict (for stats + ``--check``).

        The three size-guard counters are appended in fixed order after the
        existing datagram counters.
        """
        return {
            "received": self.received,
            "rfc3164": self.rfc3164,
            "rfc5424": self.rfc5424,
            "unknown": self.unknown,
            "malformed": self.malformed,
            "unknown_source": self.unknown_source,
            "rejected_sources": self.rejected_sources,
            "written": self.written,
            "write_errors": self.write_errors,
            "internal_errors": self.internal_errors,
            "size_rotations": self.size_rotations,
            "space_prunes": self.space_prunes,
            "bytes_reclaimed": self.bytes_reclaimed,
        }


def _utc_now_iso() -> str:
    """Default injectable receive clock: current UTC time as ISO-8601."""
    return datetime.now(timezone.utc).isoformat()


def _default_warn(_key: str, message: str) -> None:
    """Default seam warner: an un-throttled WARNING (the live loop passes a
    throttled one). The ``key`` is ignored here; it only matters to a throttle.
    """
    _log.warning("%s", message)


class _Throttle:
    """Per-key WARNING throttle: emit at most once per window per key."""

    def __init__(self, window: float = _WARN_THROTTLE_S) -> None:
        self._window = window
        self._last: dict[str, float] = {}

    def should_emit(self, key: str, monotonic: float) -> bool:
        last = self._last.get(key)
        if last is None or (monotonic - last) >= self._window:
            self._last[key] = monotonic
            return True
        return False


# Public alias: ``__main__`` builds one throttle to share between the server's
# own warnings and the Writer's size-guard warnings. Exposed under a public name
# so the cross-module construction is not a private-usage access.
Throttle = _Throttle


def make_throttled_warn(throttle: _Throttle) -> Callable[[str, str], None]:
    """Build a ``warn(key, message)`` callback backed by `throttle`.

    The live `Writer`'s size-guard warnings share the server's throttle through
    this (one warn per key per window), so a segment-roll flood cannot warn at
    roll rate. Exposed so ``__main__`` can wire the Writer's warner from the same
    throttle it hands the `Server`.
    """

    def _warn(key: str, message: str) -> None:
        if throttle.should_emit(key, time.monotonic()):
            _log.warning("%s", message)

    return _warn


def _trace_datagram(
    client_ip: str,
    record: SyslogRecord,
    site: str,
    host: str,
    outcome: str,
) -> None:
    """Emit one consolidated DEBUG line surfacing the parse + resolve decision.

    Opt-in only: a true no-op unless DEBUG is enabled (guarded by the caller's
    ``isEnabledFor`` check and the ``_log.debug`` level gate). Goes to **stderr**
    (the diagnostics stream), never to the stdout collected-data echo.

    `program` and `sender_ts` are sender-controlled and may carry embedded line
    breaks, so both are rendered through ``repr()`` before they enter the trace.
    ``repr()`` escapes every line-break and control code point (``\\n`` / ``\\r``
    and ``\\xNN`` / ``\\uNNNN``), so the result is guaranteed a single physical
    line — the same one-physical-line contract the stored-line path enforces, so
    a crafted datagram cannot split this diagnostics line into extra physical
    lines. `site`/`host` are config-derived (an unknown source resolves
    ``host == client_ip``), and the remaining fields are bounded enums/flags.

    Each ``%s`` arg is passed lazily so no formatting happens below DEBUG.
    """
    _log.debug(
        "datagram from %s: protocol=%s priority=%s program=%s sender_ts=%s "
        "resolved=%s/%s malformed=%s write=%s",
        client_ip,
        record.protocol,
        record.priority_text,
        repr(record.program),
        repr(record.sender_ts),
        site,
        host,
        record.malformed,
        outcome,
    )


# Public seam alias: the ``--check`` trace oracle calls the trace renderer
# directly (bypassing the parser) to pin the ``repr()`` line-break
# neutralization on hostile ``program`` / ``sender_ts`` fields. Exposed under a
# public name so the cross-module call is not a private-usage access.
trace_datagram = _trace_datagram


def process_datagram(
    data: bytes,
    client_ip: str,
    *,
    resolver: Resolver,
    writer: WriterProtocol,
    counters: Counters,
    reject_unknown_sources: bool,
    clock: Callable[[], str] = _utc_now_iso,
    warn: Callable[[str, str], None] = _default_warn,
) -> None:
    """Run one datagram through the full pipeline. Raising-transparent seam.

    Counter order is fixed: ``received`` first → protocol/``malformed`` after
    parse → ``unknown_source`` after resolve → ``written`` only after a
    successful ``write()`` + ``flush()`` → ``write_errors`` only on `WriteError`.
    The stdout echo is best-effort, fires only after ``written``, and its failure
    is swallowed (never counted, never retried). A `WriteError` is caught here
    (it is the expected, counted failure) and reported via the injectable `warn`
    ``(key, message)`` callback — the live loop passes its **throttled** warner so
    a disk-fill flood cannot warn at line rate; the default is un-throttled so
    the oracles stay simple. Any other exception propagates to the loop-level
    catch-all.
    """
    from .writer import WriteError

    counters.received += 1
    recv_ts = clock()
    raw = data.decode("utf-8", errors="replace")
    record = parse(raw, recv_ts)

    if record.protocol == "rfc3164":
        counters.rfc3164 += 1
    elif record.protocol == "rfc5424":
        counters.rfc5424 += 1
    else:
        counters.unknown += 1
    if record.malformed:
        counters.malformed += 1

    if not resolver.is_known(client_ip):
        counters.unknown_source += 1
        if reject_unknown_sources:
            site, host = "unknown", client_ip  # trace labels; resolve() bypassed
            counters.rejected_sources += 1
            resolver.note_unknown_rejected(
                client_ip
            )  # warn-once per IP via seen_unknown
            if _log.isEnabledFor(logging.DEBUG):
                _trace_datagram(client_ip, record, site, host, "rejected")
            return
    site, host = resolver.resolve(client_ip)
    line = format_line(record, site, host)
    try:
        writer.write(line)
    except WriteError:
        counters.write_errors += 1
        if _log.isEnabledFor(logging.DEBUG):
            _trace_datagram(client_ip, record, site, host, "error")
        warn(f"write:{client_ip}", f"write failed for datagram from {client_ip}")
        return
    counters.written += 1
    if _log.isEnabledFor(logging.DEBUG):
        _trace_datagram(client_ip, record, site, host, "written")
    try:
        sys.stdout.write(line)
        sys.stdout.flush()
    except OSError:
        # Best-effort echo: a failed stdout write is never counted or retried.
        pass


class Server:
    """The live UDP collector: bind, recvfrom loop, shutdown, stats."""

    def __init__(
        self,
        config: Config,
        writer: WriterProtocol,
        throttle: _Throttle | None = None,
    ) -> None:
        self._config = config
        self._writer = writer
        self._resolver = Resolver(config.sources)
        self._counters = Counters()
        self._stop = threading.Event()
        # The live collector shares one throttle between its own warnings and the
        # Writer's size-guard warnings (``__main__`` builds the Writer's warner
        # from this same throttle); the oracle call sites omit it and get a fresh
        # one.
        self._throttle = throttle if throttle is not None else _Throttle()
        self._sock: socket.socket | None = None

    @property
    def counters(self) -> Counters:
        return self._counters

    def replace_resolver(self, resolver: Resolver) -> Resolver:
        """Swap the resolver seam, returning the previous one.

        A diagnostic/self-check seam: the ``--check`` oracle injects a resolver
        whose ``resolve()`` raises so it can drive `handle_one`'s loop-level
        ``except Exception`` survival path through the *real* `Server`, then
        restores the original to prove the next datagram still processes. The
        live collector never calls this.
        """
        previous = self._resolver
        self._resolver = resolver
        return previous

    def request_stop(self) -> None:
        """Signal the loop to stop (from a signal handler)."""
        self._stop.set()

    def _bind(self) -> socket.socket:
        """Bind ``<listen_host>:<listen_port>``; a bind failure is fatal (exit 1)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.bind((self._config.listen_host, self._config.listen_port))
        except OSError as exc:
            sock.close()
            _log.error("cannot bind UDP :%d — %s", self._config.listen_port, exc)
            sys.exit(1)
        sock.settimeout(_RECV_TIMEOUT_S)
        return sock

    def _warn_throttled(self, key: str, message: str) -> None:
        if self._throttle.should_emit(key, time.monotonic()):
            _log.warning("%s", message)

    def handle_one(self, data: bytes, client_ip: str) -> None:
        """Run one datagram through the seam under the loop-level catch-all.

        This is the exact per-datagram body of `run`'s loop, factored out so the
        ``--check`` oracle can drive the ``internal_errors`` survival path through
        the *same* ``except Exception`` the live loop uses (the seam is
        raising-transparent, so a seam-direct call would never reach it). A
        `WriteError` is the seam's own counted failure and is handled inside
        `process_datagram`; only an *unexpected* exception lands here, bumping
        ``internal_errors``, emitting one throttled WARNING keyed on `client_ip`,
        and being swallowed so the next datagram still processes.
        """
        try:
            process_datagram(
                data,
                client_ip,
                resolver=self._resolver,
                writer=self._writer,
                counters=self._counters,
                reject_unknown_sources=self._config.reject_unknown_sources,
                warn=self._warn_throttled,
            )
        except Exception:
            self._counters.internal_errors += 1
            self._warn_throttled(
                f"internal:{client_ip}",
                f"internal error processing datagram from {client_ip}",
            )

    def run(self) -> int:
        """Bind and serve until stop is signalled; flush + final stats; exit 0.

        The receive call's timeout-vs-OSError split lives here; the seam call is
        wrapped in a loop-level catch-all so one poison datagram can never kill
        the collector.
        """
        self._sock = self._bind()
        _log.info(
            "py-syslog listening on UDP :%d (%d source mapping(s))",
            self._config.listen_port,
            len(self._config.sources),
        )
        last_stats = time.monotonic()
        try:
            while not self._stop.is_set():
                pair = self._receive()
                now = time.monotonic()
                if now - last_stats >= _STATS_INTERVAL_S:
                    # Backstop the write-driven guard: re-check the budget at the
                    # stats tick in case a non-log file grew the volume without a
                    # size-roll occurring. A no-op when the guard is disabled.
                    self._writer.enforce_space_tick()
                    self._emit_stats("periodic")
                    last_stats = now
                if pair is None:
                    continue
                data, client_ip = pair
                self.handle_one(data, client_ip)
        finally:
            self._sock.close()
            self._writer.close()
            self._emit_stats("shutdown")
        return 0

    def _receive(self) -> tuple[bytes, str] | None:
        """One ``recvfrom`` under a 1 s timeout. Never propagates an `OSError`.

        ``TimeoutError`` (the normal per-second tick) → re-check stop, ``None``,
        no warning. ``EINTR`` → throttled warning, ``None`` (retry next loop).
        Any other ``OSError`` → throttled warning, ``None``.
        """
        assert self._sock is not None
        try:
            data, addr = self._sock.recvfrom(_MAX_DATAGRAM)
        except TimeoutError:
            return None
        except OSError as exc:
            if exc.errno == errno.EINTR:
                self._warn_throttled("recv:eintr", "recvfrom interrupted (EINTR)")
            else:
                self._warn_throttled("recv:oserror", f"recvfrom error: {exc}")
            return None
        return (data, addr[0])

    def _emit_stats(self, label: str) -> None:
        # Copy the Writer's authoritative guard counters into the Counters snapshot
        # (the Writer owns them; the server merely surfaces them here).
        writer_stats = self._writer.stats
        self._counters.size_rotations = writer_stats.size_rotations
        self._counters.space_prunes = writer_stats.space_prunes
        self._counters.bytes_reclaimed = writer_stats.bytes_reclaimed

        counts = self._counters.as_dict()
        # Storage segment: the size-guard counters plus two always-rendered live
        # gauges read at tick time (a measurement failure renders ``?`` rather
        # than crashing the stats line).
        free_pct = self._writer.disk_free_pct()
        log_mb = self._writer.log_dir_mb()
        free_text = str(free_pct) if free_pct is not None else "?"
        log_text = str(log_mb) if log_mb is not None else "?"

        counter_keys = (
            "received",
            "rfc3164",
            "rfc5424",
            "unknown",
            "malformed",
            "unknown_source",
            "rejected_sources",
            "written",
            "write_errors",
            "internal_errors",
        )
        datagram_part = " ".join(f"{key}={counts[key]}" for key in counter_keys)
        storage_part = (
            f"disk_free_pct={free_text} log_dir_mb={log_text} "
            f"size_rotations={counts['size_rotations']} "
            f"space_prunes={counts['space_prunes']} "
            f"bytes_reclaimed={counts['bytes_reclaimed']}"
        )
        _log.info("stats (%s): %s %s", label, datagram_part, storage_part)
