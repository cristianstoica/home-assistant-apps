# pyright: strict
"""Pure syslog parsing: ``parse(raw, recv_ts) -> SyslogRecord``.

This is the riskiest, most change-prone surface (every device phrases syslog
slightly differently), so it is isolated and kept **pure**: deterministic in its
two arguments, no clock, no I/O, and it **never raises**. A failure path always
returns a fully-formed record with ``malformed=True`` and ``program="MALFORMED"``
rather than throwing — the caller (`server.process_datagram`) owns ordering and
counters and must never see an exception from here.

RFC 3164 is primary; RFC 5424 is handled defensively (the ``<PRI>VERSION`` form
with VERSION a digit). The sender's own timestamp is captured into `sender_ts`
verbatim (rendered later); the authoritative ordering key is the
caller-supplied `recv_ts`.
"""

from __future__ import annotations

import re

from .models import SyslogRecord

# Facility/severity name tables (RFC 3164 / 5424). Index = numeric code.
_FACILITIES: tuple[str, ...] = (
    "kern",
    "user",
    "mail",
    "daemon",
    "auth",
    "syslog",
    "lpr",
    "news",
    "uucp",
    "cron",
    "authpriv",
    "ftp",
    "ntp",
    "audit",
    "alert",
    "clock",
    "local0",
    "local1",
    "local2",
    "local3",
    "local4",
    "local5",
    "local6",
    "local7",
)
_SEVERITIES: tuple[str, ...] = (
    "emerg",
    "alert",
    "crit",
    "err",
    "warning",
    "notice",
    "info",
    "debug",
)

# <PRI> prefix: 1-3 digits, value 0-191.
_PRI_RE = re.compile(r"^<(\d{1,3})>")
# RFC 3164 timestamp: "Mmm dd hh:mm:ss" (day may be space-padded).
_RFC3164_TS_RE = re.compile(
    r"^([A-Z][a-z]{2} [ 0-9]\d \d{2}:\d{2}:\d{2}) (.*)$",
    re.DOTALL,
)
# RFC 5424: VERSION SP TIMESTAMP SP HOSTNAME SP APP-NAME SP PROCID SP MSGID SP rest
_RFC5424_RE = re.compile(
    r"^(\d{1,2}) (\S+) (\S+) (\S+) (\S+) (\S+) (.*)$",
    re.DOTALL,
)
# tag[pid]: or tag: at the head of a 3164 message body.
_TAG_RE = re.compile(r"^([^\s:\[]+)(?:\[(\d+)\])?: ?(.*)$", re.DOTALL)


def _priority_text(pri: int) -> str:
    """Map a numeric PRI to ``<facility>.<severity>`` text, or ``"unknown"``.

    PRI is ``facility * 8 + severity``; valid range is 0-191. Out-of-range or
    out-of-table codes yield ``"unknown"`` (the record is still well-formed).
    """
    if pri < 0 or pri > 191:
        return "unknown"
    facility = pri // 8
    severity = pri % 8
    if facility >= len(_FACILITIES) or severity >= len(_SEVERITIES):
        return "unknown"
    return f"{_FACILITIES[facility]}.{_SEVERITIES[severity]}"


def _malformed(raw: str, recv_ts: str) -> SyslogRecord:
    """A fully-formed record flagging an unparseable datagram (never raises)."""
    return SyslogRecord(
        recv_ts=recv_ts,
        protocol="unknown",
        priority_text="unknown",
        program="MALFORMED",
        sender_ts="",
        message="",
        malformed=True,
        raw=raw,
        structured_data="",
    )


def _parse_rfc5424(
    pri_text: str, rest: str, raw: str, recv_ts: str
) -> SyslogRecord | None:
    """Try the RFC 5424 shape (``VERSION SP TIMESTAMP SP HOSTNAME SP ...``).

    Returns ``None`` if `rest` does not look like 5424 so the caller can fall
    back to 3164. The leading token must be a version number (1-2 digits).
    """
    match = _RFC5424_RE.match(rest)
    if match is None:
        return None
    _version, timestamp, _hostname, app_name, procid, _msgid, tail = match.groups()
    # tail = STRUCTURED-DATA SP MSG. SD is either "-" (nil) or one-or-more
    # "[...]" elements; split it off so MSG is the human-readable remainder and
    # the captured SD region can be preserved (rendered only when opted in).
    structured_data, message = _split_structured_data(tail)
    sender_ts = "" if timestamp == "-" else timestamp
    program = app_name
    if procid not in ("-", ""):
        program = f"{program}[{procid}]"
    return SyslogRecord(
        recv_ts=recv_ts,
        protocol="rfc5424",
        priority_text=pri_text,
        program=program,
        sender_ts=sender_ts,
        message=message,
        malformed=False,
        raw=raw,
        structured_data=structured_data,
    )


def _split_structured_data(tail: str) -> tuple[str, str]:
    """Split the RFC 5424 STRUCTURED-DATA prefix from the trailing MSG.

    Returns ``(structured_data, message)``. `tail` is ``STRUCTURED-DATA [SP MSG]``.
    STRUCTURED-DATA is ``-`` (nil) or a run of ``[...]`` elements (no SP between
    elements). Bracket matching ignores ``]`` escaped as ``\\]`` inside param
    values, per RFC 5424.

    The SD slice is ``tail[:i]`` using the same boundary index ``i`` the
    bracket-matcher computes; MSG is preserved byte-for-byte. Edge cases yield an
    empty SD: nil ``-`` → ``("", msg)``, no leading ``[`` → ``("", tail)``,
    unterminated SD → ``("", "")``. Pure string slicing only — never raises.
    """
    if tail.startswith("-"):
        # nil SD; MSG is whatever follows the single space after "-".
        return ("", tail[2:] if tail.startswith("- ") else "")
    if not tail.startswith("["):
        # No recognizable SD; treat the whole tail as the message.
        return ("", tail)
    i = 0
    n = len(tail)
    while i < n and tail[i] == "[":
        depth = 0
        while i < n:
            ch = tail[i]
            if ch == "\\" and i + 1 < n:
                i += 2
                continue
            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    i += 1
                    break
            i += 1
        else:
            # Unterminated SD element; bail out, no message recoverable.
            return ("", "")
    structured_data = tail[:i]
    # A single SP separates SD from MSG.
    if i < n and tail[i] == " ":
        return (structured_data, tail[i + 1 :])
    return (structured_data, tail[i:])


def _parse_rfc3164(pri_text: str, rest: str, raw: str, recv_ts: str) -> SyslogRecord:
    """Parse the RFC 3164 body (``TIMESTAMP SP HOSTNAME SP TAG: MSG``).

    Defensive: if the leading timestamp is absent, treat the whole remainder as
    the message with no `sender_ts`. The record is always well-formed.
    """
    sender_ts = ""
    body = rest
    ts_match = _RFC3164_TS_RE.match(rest)
    if ts_match is not None:
        sender_ts = ts_match.group(1)
        after_ts = ts_match.group(2)
        # after_ts = HOSTNAME SP TAG... ; drop the hostname token.
        parts = after_ts.split(" ", 1)
        body = parts[1] if len(parts) == 2 else parts[0]
    program, message = _extract_tag(body)
    return SyslogRecord(
        recv_ts=recv_ts,
        protocol="rfc3164",
        priority_text=pri_text,
        program=program,
        sender_ts=sender_ts,
        message=message,
        malformed=False,
        raw=raw,
        structured_data="",
    )


def _extract_tag(body: str) -> tuple[str, str]:
    """Split a 3164 message body into (program, message).

    Recognizes ``tag:`` and ``tag[pid]:``. If no tag is present the program is
    ``"-"`` and the whole body is the message.
    """
    match = _TAG_RE.match(body)
    if match is None:
        return ("-", body)
    tag, pid, message = match.groups()
    program = f"{tag}[{pid}]" if pid is not None else tag
    return (program, message)


def parse(raw: str, recv_ts: str) -> SyslogRecord:
    """Parse one decoded datagram into a `SyslogRecord`. Pure; never raises.

    `raw` is the already-decoded datagram text (``errors="replace"``); `recv_ts`
    is the collector receive time supplied by the caller. Any internal failure
    falls through to a ``malformed=True`` record so the seam's counter order and
    the "never silently drop" guarantee hold.
    """
    try:
        text = raw.rstrip("\r\n")
        pri_match = _PRI_RE.match(text)
        if pri_match is None:
            return _malformed(raw, recv_ts)
        pri = int(pri_match.group(1))
        pri_text = _priority_text(pri)
        rest = text[pri_match.end() :]
        # RFC 5424 is "<PRI>VERSION SP ..." — a digit immediately after PRI.
        rfc5424 = _parse_rfc5424(pri_text, rest, raw, recv_ts)
        if rfc5424 is not None:
            return rfc5424
        return _parse_rfc3164(pri_text, rest, raw, recv_ts)
    except Exception:
        # Purity guarantee: a parser bug must never propagate to the seam.
        return _malformed(raw, recv_ts)
