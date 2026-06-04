# pyright: strict
"""Data structures and the canonical output-line renderer.

`NamedTuple`s (not `@dataclass`) match the repo idiom (`scripts/apc_manager.py`).
`format_line` is the single place the on-disk line shape and the one-datagram =
one-physical-line escaping contract live; everything else (parser, writer,
server) feeds it but does not format.
"""

from __future__ import annotations

from typing import NamedTuple, Protocol


class SourceMapping(NamedTuple):
    """One configured sender: its IP and the site/host it resolves to."""

    ip: str
    site: str
    host: str


class Config(NamedTuple):
    """Validated, fully-resolved runtime configuration.

    `log_dir` / `log_file` are dev-override keys (defaults ``/data/log`` /
    ``syslog.log``); they are absent from the HA schema, so a deployed add-on
    never sets them and the production storage path cannot be misconfigured.
    `listen_host` is the local bind address (``0.0.0.0`` = all interfaces);
    its default lives only in the HA schema (``config.yaml``), never as a Python
    literal. `sources` is keyed by sender IP for O(1) resolution.

    `min_free_percent` / `max_log_percent` / `max_segment_mb` are the size-guard
    knobs (all default ``0`` = disabled, so a 1.2.0 upgrade changes nothing).
    `min_free_percent` is the free-space floor (prune to keep ≥ this % of the
    volume free); `max_log_percent` is the log-dir cap (prune so the log dir
    occupies ≤ this % of the volume); `max_segment_mb` is the size-rotation
    trigger (roll the active file to a ``.gz`` segment at this many MB).
    Validation requires `max_segment_mb` > 0 whenever either percent > 0 —
    without intra-day segments there is nothing to prune.
    """

    listen_port: int
    listen_host: str
    retention_days: int
    min_free_percent: int
    max_log_percent: int
    max_segment_mb: int
    log_level: str
    sources: dict[str, SourceMapping]
    log_dir: str
    log_file: str


class SyslogRecord(NamedTuple):
    """A fully-formed parse result. Never half-built; the parser never raises.

    `recv_ts` — collector receive time (UTC ISO-8601), the authoritative
    ordering key, supplied by the caller (the parser reads no clock).
    `protocol` — ``"rfc3164"`` | ``"rfc5424"`` | ``"unknown"``.
    `sender_ts` — the sender's own timestamp token as sent (``""`` when absent
    or unparseable, rendered ``-``).
    `raw` — the full decoded datagram (``errors="replace"``), kept for
    malformed/unknown output.
    """

    recv_ts: str
    protocol: str
    priority_text: str
    program: str
    sender_ts: str
    message: str
    malformed: bool
    raw: str


class WriterProtocol(Protocol):
    """Structural type for the storage sink (real ``Writer`` + test fakes).

    Beyond the durability surface (`write` / `close`), the storage sink exposes
    a read-only size-guard surface the server samples at stats-emit time: the
    two live gauges (`disk_free_pct` / `log_dir_mb`, ``None`` on a measurement
    failure so the stats line degrades rather than crashes), the cumulative
    guard counters (`stats`), and the periodic-tick backstop (`enforce_space_tick`).
    Test fakes implement these as no-ops / zeros (they exercise the datagram
    path, not the guard).
    """

    def write(self, line: str) -> None: ...

    def close(self) -> None: ...

    @property
    def stats(self) -> WriterStats: ...

    def disk_free_pct(self) -> int | None: ...

    def log_dir_mb(self) -> int | None: ...

    def enforce_space_tick(self) -> None: ...


class WriterStats(Protocol):
    """Read-only view of the storage sink's cumulative size-guard counters."""

    @property
    def size_rotations(self) -> int: ...

    @property
    def space_prunes(self) -> int: ...

    @property
    def bytes_reclaimed(self) -> int: ...


def _escape(text: str) -> str:
    """Backslash-escape every code point that a downstream renderer, terminal, or
    log viewer could treat as a line break, so one datagram can never split into
    extra, unstamped physical lines.

    ``\\`` itself is escaped first (so the escape is unambiguous and
    reversible for the common control chars), then the named controls, then:

    * any remaining C0 control character (U+0000-U+001F, and U+007F DEL) and any
      C1 control character (U+0080-U+009F, which includes U+0085 NEL — a hard
      line break to several renderers) as ``\\xNN`` (two hex digits);
    * U+2028 LINE SEPARATOR and U+2029 PARAGRAPH SEPARATOR — Unicode line breaks
      that exceed two hex digits — as ``\\uNNNN`` (four hex digits).

    Everything else (printable text) passes through. The decode step already
    mapped invalid bytes to U+FFFD, so the input here is text; the output is
    single-line text, not byte-exact.
    """
    out: list[str] = []
    for ch in text:
        code = ord(ch)
        if ch == "\\":
            out.append("\\\\")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif code < 0x20 or code == 0x7F or 0x80 <= code <= 0x9F:
            out.append(f"\\x{code:02x}")
        elif code in (0x2028, 0x2029):
            out.append(f"\\u{code:04x}")
        else:
            out.append(ch)
    return "".join(out)


def format_line(record: SyslogRecord, site: str, host: str) -> str:
    """Render one stored line, terminated by exactly one ``\\n``.

    Canonical shape::

        <recv_ts> <site> <host> <priority_text> <program>: [<sender_ts>] <message>\\n

    `message`, `sender_ts`, and `raw` are escaped per the contract so the
    result is exactly one physical line. A malformed record carries the
    escaped raw datagram as its message; an empty `sender_ts` renders ``-``.
    """
    sender = _escape(record.sender_ts) if record.sender_ts else "-"
    body = record.message if not record.malformed else record.raw
    message = _escape(body)
    return (
        f"{record.recv_ts} {site} {host} {record.priority_text} "
        f"{record.program}: [{sender}] {message}\n"
    )
