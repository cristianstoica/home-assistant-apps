# pyright: strict
"""Built-in self-validation corpus for ``--check``.

Two corpora live here, both consumed by `__main__`:

1. `DATAGRAMS` — a list of `DatagramFixture`, each pairing a ``(client_ip, raw)``
   input with the **expected** rendered line, protocol tag, ``sender_ts``, and
   resolved ``(site, host)``. ``--check`` drives each through the real
   `server.process_datagram` seam (with a pinned clock) and asserts produced ==
   expected, then asserts the aggregate counters. A broken parser, resolver,
   renderer, counter order, or escaping rule fails the check.

2. `INVALID_OPTIONS` — a list of `InvalidOptionsFixture`, each an options
   payload that must be rejected by `config.load` with a `ConfigError` whose
   message **names** the offending field. This makes the duplicate-ip /
   empty-field / out-of-range rejection an automated assertion.

The corpus is the regression oracle a pytest suite would otherwise be; it
declares expected values rather than recomputing them, so it catches drift.

All addresses are RFC 5737 documentation IPs and the site/host labels are
generic examples; real deployments configure their own mapping via the HA
options UI.
"""

from __future__ import annotations

from typing import Any, NamedTuple

# Pinned receive timestamp injected into every fixture parse so output is
# deterministic (the live path captures this from the real clock).
PINNED_RECV_TS: str = "2026-06-03T12:00:00+00:00"

# An example configured sender (RFC 5737 documentation address). The --check
# corpus resolves this IP to the example site/host mapping below.
SOURCE_IP: str = "192.0.2.1"

# The source-mapping list the --check Config is built from (an example mapping;
# real deployments configure their own via the HA options UI).
CHECK_SOURCES: list[dict[str, str]] = [
    {"ip": SOURCE_IP, "site": "home", "host": "router1"},
]


class DatagramFixture(NamedTuple):
    """One datagram input plus its fully-expected processing result.

    `tag` is the human-facing ``--check`` category
    (``3164`` / ``5424`` / ``malformed`` / ``unknown-src``); `protocol` is the
    `SyslogRecord.protocol` the parser must produce.
    """

    name: str
    client_ip: str
    raw: bytes
    tag: str
    protocol: str
    sender_ts: str
    site: str
    host: str
    expected_line: str


class InvalidOptionsFixture(NamedTuple):
    """An options payload that `config.load` must reject by naming `field`."""

    name: str
    options: dict[str, Any]
    field: str


DATAGRAMS: list[DatagramFixture] = [
    DatagramFixture(
        name="rfc3164 with PRI (configured mapping)",
        client_ip=SOURCE_IP,
        raw=b"<13>Jun  3 11:59:58 myhost kernel: link down",
        tag="3164",
        protocol="rfc3164",
        sender_ts="Jun  3 11:59:58",
        site="home",
        host="router1",
        expected_line=(
            "2026-06-03T12:00:00+00:00 home router1 user.notice "
            "kernel: [Jun  3 11:59:58] link down\n"
        ),
    ),
    DatagramFixture(
        name="rfc3164 without PRI",
        client_ip=SOURCE_IP,
        raw=b"Jun  3 11:59:58 myhost kernel: link down",
        tag="malformed",
        protocol="unknown",
        sender_ts="",
        site="home",
        host="router1",
        expected_line=(
            "2026-06-03T12:00:00+00:00 home router1 unknown "
            "MALFORMED: [-] Jun  3 11:59:58 myhost kernel: link down\n"
        ),
    ),
    DatagramFixture(
        name="rfc3164 tag[pid]",
        client_ip=SOURCE_IP,
        raw=b"<38>Jun  3 11:59:58 myhost sshd[1234]: accepted",
        tag="3164",
        protocol="rfc3164",
        sender_ts="Jun  3 11:59:58",
        site="home",
        host="router1",
        expected_line=(
            "2026-06-03T12:00:00+00:00 home router1 auth.info "
            "sshd[1234]: [Jun  3 11:59:58] accepted\n"
        ),
    ),
    DatagramFixture(
        name="rfc5424 nil structured-data",
        client_ip=SOURCE_IP,
        raw=b"<165>1 2026-06-03T11:59:58.000Z myhost app 4711 ID47 - msg here",
        tag="5424",
        protocol="rfc5424",
        sender_ts="2026-06-03T11:59:58.000Z",
        site="home",
        host="router1",
        expected_line=(
            "2026-06-03T12:00:00+00:00 home router1 local4.notice "
            "app[4711]: [2026-06-03T11:59:58.000Z] msg here\n"
        ),
    ),
    DatagramFixture(
        name="rfc5424 with structured-data",
        client_ip=SOURCE_IP,
        raw=(
            b"<165>1 2026-06-03T11:59:58.000Z myhost evntslog - ID47 "
            b'[exampleSDID@32473 iut="3" eventID="1011"] An application event'
        ),
        tag="5424",
        protocol="rfc5424",
        sender_ts="2026-06-03T11:59:58.000Z",
        site="home",
        host="router1",
        expected_line=(
            "2026-06-03T12:00:00+00:00 home router1 local4.notice "
            "evntslog: [2026-06-03T11:59:58.000Z] An application event\n"
        ),
    ),
    DatagramFixture(
        name="unknown source IP",
        client_ip="203.0.113.9",
        raw=b"<14>Jun  3 12:00:01 otherhost prog: hello from elsewhere",
        tag="unknown-src",
        protocol="rfc3164",
        sender_ts="Jun  3 12:00:01",
        site="unknown",
        host="203.0.113.9",
        expected_line=(
            "2026-06-03T12:00:00+00:00 unknown 203.0.113.9 user.info "
            "prog: [Jun  3 12:00:01] hello from elsewhere\n"
        ),
    ),
    DatagramFixture(
        name="malformed binary datagram",
        client_ip=SOURCE_IP,
        raw=b"\x00\x01\x02 not syslog at all \xff\xfe",
        tag="malformed",
        protocol="unknown",
        sender_ts="",
        site="home",
        host="router1",
        # \xff\xfe decode to U+FFFD (replacement char) before escaping; the NUL
        # and control bytes escape to \x00 etc. Exactly one physical line.
        expected_line=(
            "2026-06-03T12:00:00+00:00 home router1 unknown "
            "MALFORMED: [-] \\x00\\x01\\x02 not syslog at all ��\n"
        ),
    ),
    DatagramFixture(
        name="CR/LF + NUL escaping contract",
        client_ip=SOURCE_IP,
        raw=b"<13>Jun  3 11:59:58 myhost app: line1\nline2\twith tab\x00end",
        tag="3164",
        protocol="rfc3164",
        sender_ts="Jun  3 11:59:58",
        site="home",
        host="router1",
        # The embedded LF, TAB, and NUL are all escaped -> one stamped line.
        expected_line=(
            "2026-06-03T12:00:00+00:00 home router1 user.notice "
            "app: [Jun  3 11:59:58] line1\\nline2\\twith tab\\x00end\n"
        ),
    ),
    DatagramFixture(
        name="C1 control + Unicode line-separator escaping contract",
        client_ip=SOURCE_IP,
        # UTF-8 for U+0080 (\xc2\x80), U+0085 NEL (\xc2\x85), U+009F (\xc2\x9f),
        # U+2028 LINE SEPARATOR (\xe2\x80\xa8), U+2029 PARAGRAPH SEPARATOR
        # (\xe2\x80\xa9) interleaved with printable chars so each escape is
        # unambiguous. These are validly-decoded code points (not invalid bytes),
        # so decode passes them through; the escaper, not decode, must neutralize
        # them or the datagram splits into extra unstamped lines.
        raw=(
            b"<13>Jun  3 11:59:58 myhost app: "
            b"a\xc2\x80b\xc2\x85c\xc2\x9fd\xe2\x80\xa8e\xe2\x80\xa9f"
        ),
        tag="3164",
        protocol="rfc3164",
        sender_ts="Jun  3 11:59:58",
        site="home",
        host="router1",
        # C1 controls render as \xNN (two hex), U+2028/U+2029 as \uNNNN (four
        # hex) -> one stamped physical line, no line-injection.
        expected_line=(
            "2026-06-03T12:00:00+00:00 home router1 user.notice "
            "app: [Jun  3 11:59:58] a\\x80b\\x85c\\x9fd\\u2028e\\u2029f\n"
        ),
    ),
    DatagramFixture(
        name="all escape classes + legit UTF-8 in one line",
        client_ip=SOURCE_IP,
        # One body mixing every escape class against legitimate multi-byte
        # UTF-8, to prove the escaper neutralizes line-splitters without
        # mangling real content. Body after "app: ":
        #   x \ y \t z \r A \r\n B \x01 C \x7f D \xc2\x85(NEL) E ' ' F
        #   then café-日本-🚀 (verbatim multi-byte UTF-8).
        # \ exercises the \\ self-escape branch and \x7f exercises the DEL arm
        # -- neither is hit by any other datagram fixture.
        raw=(
            b"<13>Jun  3 11:59:58 myhost app: "
            b"x\\y\tz\rA\r\nB\x01C\x7fD\xc2\x85E F caf\xc3\xa9-"
            b"\xe6\x97\xa5\xe6\x9c\xac-\xf0\x9f\x9a\x80"
        ),
        tag="3164",
        protocol="rfc3164",
        sender_ts="Jun  3 11:59:58",
        site="home",
        host="router1",
        # Backslash -> \\, TAB -> \t, bare CR -> \r, CRLF -> \r\n, C0 0x01 ->
        # \x01, DEL 0x7f -> \x7f, C1 NEL 0x85 -> \x85; the literal space and the
        # café-日本-🚀 run pass through verbatim (space is not a splitter). One
        # stamped physical line, no line-injection.
        expected_line=(
            "2026-06-03T12:00:00+00:00 home router1 user.notice "
            "app: [Jun  3 11:59:58] "
            "x\\\\y\\tz\\rA\\r\\nB\\x01C\\x7fD\\x85E F café-日本-🚀\n"
        ),
    ),
]


def _valid_base() -> dict[str, Any]:
    """A minimal valid options payload to mutate per invalid-options fixture.

    `listen_host` carries an RFC 5737 documentation address (not the schema's
    ``0.0.0.0`` default): these payloads never bind a socket, and keeping the
    bind-all literal out of Python preserves the no-bind-all-literal invariant.
    """
    return {
        "listen_port": 5514,
        "listen_host": "192.0.2.10",
        "retention_days": 30,
        "log_level": "info",
        "sources": [
            {"ip": "192.0.2.1", "site": "home", "host": "router1"},
        ],
    }


def _with_sources(sources: list[dict[str, str]]) -> dict[str, Any]:
    base = _valid_base()
    base["sources"] = sources
    return base


INVALID_OPTIONS: list[InvalidOptionsFixture] = [
    InvalidOptionsFixture(
        name="duplicate source ip",
        options=_with_sources(
            [
                {"ip": "192.0.2.1", "site": "home", "host": "router1"},
                {"ip": "192.0.2.1", "site": "home", "host": "other"},
            ]
        ),
        field="ip",
    ),
    InvalidOptionsFixture(
        name="empty source ip",
        options=_with_sources([{"ip": "", "site": "home", "host": "router1"}]),
        field="ip",
    ),
    InvalidOptionsFixture(
        name="empty source site",
        options=_with_sources([{"ip": "192.0.2.1", "site": "", "host": "router1"}]),
        field="site",
    ),
    InvalidOptionsFixture(
        name="empty source host",
        options=_with_sources([{"ip": "192.0.2.1", "site": "home", "host": ""}]),
        field="host",
    ),
    InvalidOptionsFixture(
        name="out-of-range retention_days",
        options={**_valid_base(), "retention_days": 99999},
        field="retention_days",
    ),
    InvalidOptionsFixture(
        name="missing listen_host",
        options={k: v for k, v in _valid_base().items() if k != "listen_host"},
        field="listen_host",
    ),
    InvalidOptionsFixture(
        name="empty listen_host",
        options={**_valid_base(), "listen_host": ""},
        field="listen_host",
    ),
    InvalidOptionsFixture(
        name="non-string listen_host",
        options={**_valid_base(), "listen_host": 1234},
        field="listen_host",
    ),
    InvalidOptionsFixture(
        name="whitespace-only listen_host",
        options={**_valid_base(), "listen_host": "   "},
        field="listen_host",
    ),
]


# The expected aggregate counters after driving DATAGRAMS through the seam.
# received = all 10; rfc3164 = 6 (incl. the unknown-src one); rfc5424 = 2;
# unknown protocol = 2 malformed; malformed = 2; unknown source = 1; written = 10.
EXPECTED_COUNTERS: dict[str, int] = {
    "received": 10,
    "rfc3164": 6,
    "rfc5424": 2,
    "unknown": 2,
    "malformed": 2,
    "unknown_source": 1,
    "written": 10,
    "write_errors": 0,
    "internal_errors": 0,
}
