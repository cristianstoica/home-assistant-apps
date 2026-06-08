# pyright: strict
"""Built-in self-validation corpus for ``--check`` (the regression oracle).

This declares **expected** values rather than recomputing them, so it catches
drift in the parser/shaper/classifier logic the way a pytest suite would. The
check modules drive the production seams against these fixtures and assert the
produced value equals the declared one.

Corpora here:

* `INVALID_OPTIONS` — options payloads `config.validate` must reject with a
  `ConfigError` whose message names the offending field (per-provider rejection,
  HTTPS-only contract, name↔zone contract, range checks).
* `NAME_ZONE_CASES` — name/zone pairs and the expected derived label, or the
  expected rejection substring (apex / wrong-zone / empty-label).
* `IP_SOURCE_BODIES` — raw echo-endpoint bodies and the expected parsed global
  IPv4 (or ``None`` for malformed / non-global).
* `DNS_REPLIES` — wire-format reply builders and the expected `ResolveOutcome`.
* `RETRY_AFTER_HEADERS` — header strings and the expected parsed seconds.

Addresses are RFC 5737 documentation IPs (203.0.113.x / 198.51.100.x /
192.0.2.x), except the IP-source *accept* cases, which need a globally-routable
value (`EXAMPLE_GLOBAL_IPV4`) because the production guard rejects the non-global
doc ranges. All hostnames are example.* / documentation zones. A real deployment
configures its own values via the HA options UI.
"""

from __future__ import annotations

from typing import Any, NamedTuple

# --- a non-secret example azure credential group (RFC-doc / placeholder values) -
# The client_secret here is a fixed placeholder string the no-secret-leakage check
# asserts never appears in any logged/printed output. It is not a real secret.
EXAMPLE_CLIENT_SECRET = "EXAMPLE~secret~value~do~not~use~0000"

# The six credential fields of the nested ``azure`` group (snake_case option
# keys). `example_azure_group` adds the optional `ip_sources`/`ttl` on top.
EXAMPLE_AZURE_TOKEN: dict[str, str] = {
    "tenant_id": "00000000-0000-0000-0000-000000000001",
    "subscription_id": "00000000-0000-0000-0000-000000000002",
    "resource_group": "rg-example",
    "zone": "example.com",
    "client_id": "00000000-0000-0000-0000-000000000003",
    "client_secret": EXAMPLE_CLIENT_SECRET,
}

# A secret callback URL whose path encodes the secret token; the no-secret-leakage
# check asserts this path segment never appears in any logged/printed output.
EXAMPLE_URL_SECRET = "s3cr3t-callback-token-abcdef"
EXAMPLE_URL_ENDPOINT = f"https://dynamicdns.example.com/update/{EXAMPLE_URL_SECRET}"


def example_azure_group(**overrides: Any) -> dict[str, Any]:
    """The nested ``azure`` credential group (the six fields + ip_sources)."""
    group: dict[str, Any] = dict(EXAMPLE_AZURE_TOKEN)
    group["ip_sources"] = "https://api.ipify.org"
    group.update(overrides)
    return group


def example_azure_options(**overrides: Any) -> dict[str, Any]:
    """A valid Azure-mode options payload (nested ``azure`` group; no ``url``).

    Stays a `config.validate`-acceptable payload — it is the no-``--options``
    ``--check --dry-run`` default render at ``dryrun.py:37`` — so the loader infers
    Azure mode and resolves to a plan. Override individual top-level keys via
    ``**overrides`` (e.g. ``ttl=`` is overridden inside the group below, not here).
    """
    base: dict[str, Any] = {
        "name": "home.example.com",
        "azure": example_azure_group(),
        "interval_seconds": 120,
        "drift_reconcile_seconds": 3600,
        "log_level": "info",
    }
    base.update(overrides)
    return base


def example_url_options(**overrides: Any) -> dict[str, Any]:
    """A valid URL-mode options payload (nested ``url`` group; no ``azure``).

    Override individual top-level keys via ``**overrides``; the ``url`` group can
    be replaced wholesale (e.g. ``url={"endpoint": ..., "send_myip": True}``).
    """
    base: dict[str, Any] = {
        "name": "home.example.com",
        "url": {"endpoint": EXAMPLE_URL_ENDPOINT, "send_myip": False},
        "interval_seconds": 120,
        "drift_reconcile_seconds": 3600,
        "log_level": "info",
    }
    base.update(overrides)
    return base


class InvalidOptionsFixture(NamedTuple):
    """An options payload `config.validate` must reject by naming `field`."""

    name: str
    options: dict[str, Any]
    field: str


INVALID_OPTIONS: list[InvalidOptionsFixture] = [
    # --- inferred-provider gate rejection ---
    InvalidOptionsFixture(
        name="neither section filled (nothing to do)",
        options={
            "name": "home.example.com",
            "interval_seconds": 120,
            "drift_reconcile_seconds": 3600,
            "log_level": "info",
        },
        field="url.endpoint / azure",
    ),
    InvalidOptionsFixture(
        name="azure: missing name",
        options=example_azure_options(name=""),
        field="name",
    ),
    InvalidOptionsFixture(
        name="azure: group missing client_secret field",
        options=example_azure_options(
            azure=example_azure_group(
                **{
                    k: v for k, v in EXAMPLE_AZURE_TOKEN.items() if k != "client_secret"
                },
                client_secret="",
            )
        ),
        field="azure.client_secret",
    ),
    InvalidOptionsFixture(
        name="azure: non-dict group (stale flat string)",
        options={
            "name": "home.example.com",
            "azure": "not-a-dict",
            "url": {"endpoint": ""},
        },
        # url.endpoint blank -> azure_selected False on a non-dict azure: the
        # group-type guard fires first, naming `azure`.
        field="azure: must be an object",
    ),
    # --- non-dict url group ---
    InvalidOptionsFixture(
        name="url: non-dict group",
        options={
            "name": "home.example.com",
            "url": "https://dynamicdns.example.com/u/x",
        },
        field="url: must be an object",
    ),
    # --- name<->zone contract (azure) ---
    InvalidOptionsFixture(
        name="azure: name is the zone apex",
        options=example_azure_options(name="example.com"),
        field="name",
    ),
    InvalidOptionsFixture(
        name="azure: name under a different zone",
        options=example_azure_options(name="home.other.net"),
        field="name",
    ),
    # --- DNS hostname syntax contract (both archetypes, integration path) ---
    InvalidOptionsFixture(
        name="azure: name with an over-long label (>63 octets)",
        options=example_azure_options(name="x" * 64 + ".example.com"),
        field="name",
    ),
    InvalidOptionsFixture(
        name="url: name with an over-long label (>63 octets)",
        options=example_url_options(name="x" * 64 + ".example.com"),
        field="name",
    ),
    InvalidOptionsFixture(
        name="azure: name with an illegal char (underscore)",
        options=example_azure_options(name="bad_label.example.com"),
        field="name",
    ),
    InvalidOptionsFixture(
        name="url: empty name (required for DNS verification)",
        options=example_url_options(name=""),
        field="name",
    ),
    # --- HTTPS-only contract (url.endpoint) ---
    InvalidOptionsFixture(
        name="url: http endpoint",
        options=example_url_options(
            url={"endpoint": "http://dynamicdns.example.com/u/x"}
        ),
        field="url.endpoint",
    ),
    InvalidOptionsFixture(
        name="url: file endpoint",
        options=example_url_options(url={"endpoint": "file:///etc/passwd"}),
        field="url.endpoint",
    ),
    InvalidOptionsFixture(
        name="url: schemeless endpoint",
        options=example_url_options(url={"endpoint": "dynamicdns.example.com/u/x"}),
        field="url.endpoint",
    ),
    InvalidOptionsFixture(
        name="url: endpoint with userinfo",
        options=example_url_options(
            url={"endpoint": "https://user:pass@dynamicdns.example.com/u/x"}
        ),
        field="url.endpoint",
    ),
    InvalidOptionsFixture(
        name="url: endpoint with fragment",
        options=example_url_options(
            url={"endpoint": "https://dynamicdns.example.com/u/x#frag"}
        ),
        field="url.endpoint",
    ),
    # --- HTTPS-only contract (azure.ip_sources, split-string shape) ---
    InvalidOptionsFixture(
        name="azure: http ip source",
        options=example_azure_options(
            azure=example_azure_group(ip_sources="http://api.ipify.org")
        ),
        field="azure.ip_sources",
    ),
    InvalidOptionsFixture(
        name="azure: file ip source (second in list)",
        options=example_azure_options(
            azure=example_azure_group(
                ip_sources="https://api.ipify.org, file:///tmp/ip"
            )
        ),
        field="azure.ip_sources",
    ),
    InvalidOptionsFixture(
        name="azure: ip source with userinfo",
        options=example_azure_options(
            azure=example_azure_group(ip_sources="https://u:p@api.ipify.org")
        ),
        field="azure.ip_sources",
    ),
    InvalidOptionsFixture(
        name="azure: ip source with fragment",
        options=example_azure_options(
            azure=example_azure_group(ip_sources="https://api.ipify.org/#frag")
        ),
        field="azure.ip_sources",
    ),
    # --- range / enum checks ---
    InvalidOptionsFixture(
        name="ttl below range",
        options=example_azure_options(azure=example_azure_group(ttl=10)),
        field="azure.ttl",
    ),
    InvalidOptionsFixture(
        name="interval below range",
        options=example_azure_options(interval_seconds=30),
        field="interval_seconds",
    ),
    InvalidOptionsFixture(
        name="drift above range",
        options=example_azure_options(drift_reconcile_seconds=999999),
        field="drift_reconcile_seconds",
    ),
    InvalidOptionsFixture(
        name="bad log_level",
        options=example_azure_options(log_level="trace"),
        field="log_level",
    ),
    InvalidOptionsFixture(
        name="url_send_myip not a bool",
        options=example_url_options(
            url={"endpoint": EXAMPLE_URL_ENDPOINT, "send_myip": 1}
        ),
        field="url.send_myip",
    ),
]


class NameZoneCase(NamedTuple):
    """A name/zone derivation case.

    Exactly one of `expected_label` (accept) or `expected_reject` (reject,
    substring of the `ConfigError`) is set.
    """

    name: str
    zone: str
    expected_label: str | None
    expected_reject: str | None


NAME_ZONE_CASES: list[NameZoneCase] = [
    NameZoneCase("home.example.com", "example.com", "home", None),
    NameZoneCase("a.b.c.example.com", "example.com", "a.b.c", None),
    NameZoneCase("HOME.Example.COM", "example.com", "home", None),  # case-insensitive
    NameZoneCase("home.example.com.", "example.com", "home", None),  # trailing dot
    NameZoneCase("home.example.com", "example.com.", "home", None),  # zone trailing dot
    NameZoneCase("example.com", "example.com", None, "apex"),
    NameZoneCase("home.other.net", "example.com", None, "not under"),
    NameZoneCase("", "example.com", None, "required"),
]


class IpBodyCase(NamedTuple):
    """A raw IP-source echo body and the expected parsed value (``None`` = reject)."""

    name: str
    body: str
    expected: str | None


# A genuinely global-unicast IPv4 for the IP-source *accept* cases. RFC 5737
# documentation addresses (203.0.113.x / 198.51.100.x / 192.0.2.x) are reported
# is_global=False by ``ipaddress`` — they are IANA special-purpose, not globally
# reachable — so the production ``parse_global_ipv4`` guard correctly *rejects*
# them. The doc IPs stay as record-value/reject fixtures elsewhere; the IP-source
# accept path needs a global value to exercise the success branch.
EXAMPLE_GLOBAL_IPV4 = "11.22.33.44"

IP_SOURCE_BODIES: list[IpBodyCase] = [
    IpBodyCase("clean", EXAMPLE_GLOBAL_IPV4, EXAMPLE_GLOBAL_IPV4),
    IpBodyCase(
        "trailing newline (icanhazip)", f"{EXAMPLE_GLOBAL_IPV4}\n", EXAMPLE_GLOBAL_IPV4
    ),
    IpBodyCase(
        "surrounding whitespace", f"  {EXAMPLE_GLOBAL_IPV4}  \n", EXAMPLE_GLOBAL_IPV4
    ),
    IpBodyCase("empty body", "", None),
    IpBodyCase("two tokens", "203.0.113.7 evil.com", None),
    IpBodyCase("not an ip", "not-an-ip", None),
    IpBodyCase("rfc1918 private", "192.168.1.5", None),
    IpBodyCase("cgnat 100.64/10", "100.64.1.1", None),
    IpBodyCase("loopback", "127.0.0.1", None),
    IpBodyCase("link-local", "169.254.1.1", None),
    IpBodyCase("unspecified 0.0.0.0", "0.0.0.0", None),
]


class RetryAfterCase(NamedTuple):
    """A ``Retry-After`` header value and the expected parsed seconds (or ``None``)."""

    header: str | None
    expected: float | None


RETRY_AFTER_HEADERS: list[RetryAfterCase] = [
    RetryAfterCase("30", 30.0),
    RetryAfterCase("  5 ", 5.0),
    RetryAfterCase("0", 0.0),
    RetryAfterCase("-1", None),
    RetryAfterCase("Wed, 21 Oct 2026 07:28:00 GMT", None),  # HTTP-date form unsupported
    RetryAfterCase("nonsense", None),
    RetryAfterCase(None, None),
]
