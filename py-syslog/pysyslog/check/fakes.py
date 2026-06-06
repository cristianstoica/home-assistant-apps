# pyright: strict
"""`WriterProtocol` / handler / resolver fakes shared across the check modules.

Each public fake is imported by at least one sibling `check/*.py` module, so it
is package-API and carries a public name. ``_FakeStats`` stays private: it is
read only by the fake classes in this same module.
"""

from __future__ import annotations

import logging

from ..resolver import Resolver


class _FakeStats:
    """A zero-valued `WriterStats` stand-in for the `WriterProtocol` fakes.

    The fakes exercise the datagram path, not the size guard, so the guard
    counters stay 0 — which is also what `EXPECTED_COUNTERS` asserts.
    """

    size_rotations = 0
    space_prunes = 0
    bytes_reclaimed = 0


class CaptureWriter:
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


class RaisingWriter:
    """A `WriterProtocol` fake whose ``write()`` always raises `WriteError`."""

    def __init__(self) -> None:
        self.write_calls = 0
        self.stats = _FakeStats()

    def write(self, line: str) -> None:
        from ..writer import WriteError

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


class RecordingWarn:
    """A recording stub for the seam's injectable ``warn(key, message)``.

    The live loop passes its **throttled** warner here; this stub records each
    ``(key, message)`` so the oracle can assert a WARNING fired and was keyed on
    the ``client_ip`` (the audited "throttled WARNING" contract).
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def __call__(self, key: str, message: str) -> None:
        self.calls.append((key, message))


class RecordingHandler(logging.Handler):
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


class DebugRecordingHandler(logging.Handler):
    """A logging handler that records DEBUG-and-up messages from `pysyslog`.

    Sibling of `RecordingHandler`, used to assert the consolidated DEBUG trace
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


class RaisingResolver(Resolver):
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
