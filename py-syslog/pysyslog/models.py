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
    `sources` is keyed by sender IP for O(1) resolution.
    """

    listen_port: int
    retention_days: int
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
    """Structural type for the storage sink (real ``Writer`` + test fakes)."""

    def write(self, line: str) -> None: ...

    def close(self) -> None: ...


def _escape(text: str) -> str:
    """Backslash-escape CR, LF, TAB, and C0 control chars so one datagram can
    never split into extra, unstamped physical lines.

    ``\\`` itself is escaped first (so the escape is unambiguous and
    reversible for the common control chars), then the named controls, then any
    remaining C0 control character (U+0000-U+001F, and U+007F DEL) as
    ``\\xNN``. The decode step already mapped invalid bytes to U+FFFD, so the
    input here is text; the output is single-line text, not byte-exact.
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
        elif code < 0x20 or code == 0x7F:
            out.append(f"\\x{code:02x}")
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
