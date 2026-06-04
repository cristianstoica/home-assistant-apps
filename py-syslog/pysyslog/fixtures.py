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
]


def _valid_base() -> dict[str, Any]:
    """A minimal valid options payload to mutate per invalid-options fixture."""
    return {
        "listen_port": 5514,
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
]


# The expected aggregate counters after driving DATAGRAMS through the seam.
# received = all 8; rfc3164 = 4 (incl. the unknown-src one); rfc5424 = 2;
# unknown protocol = 2 malformed; malformed = 2; unknown source = 1; written = 8.
EXPECTED_COUNTERS: dict[str, int] = {
    "received": 8,
    "rfc3164": 4,
    "rfc5424": 2,
    "unknown": 2,
    "malformed": 2,
    "unknown_source": 1,
    "written": 8,
    "write_errors": 0,
    "internal_errors": 0,
}
