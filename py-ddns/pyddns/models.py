# pyright: strict
"""Data structures and the seams (Protocols) the updater reconciles against.

`NamedTuple`s (not `@dataclass`) match the repo idiom (py-syslog `models.py`,
`scripts/apc_manager.py`). The provider seam covers **both archetypes**
explicitly: the API archetype (`azure`) reads + writes the record itself; the
callback archetype (`url`) lets the server determine the IP and judges drift by
DNS-resolving `name`.

The resolver seam returns a three-way `ResolveOutcome`, never a bare ``None``:
"query failed" (transient) and "no record exists" (authoritative empty) are
distinct states the updater's confirmation gate reasons on separately, and
collapsing them to one ``None`` would let a transient resolver hiccup be
misread as "stale".
"""

from __future__ import annotations

from enum import Enum
from ipaddress import IPv4Address
from typing import NamedTuple, Protocol


class Provider(str, Enum):
    """The selectable provider archetypes (the ``provider:`` option)."""

    AZURE = "azure"
    URL = "url"


class AzureToken(NamedTuple):
    """The parsed ``azure_token`` SP credential blob.

    `zone` is authoritative for the name↔zone contract (see `config`); the two
    distinct identifiers from the bootstrap are kept apart elsewhere — `client_id`
    here is the **application (client) ID** used for runtime auth (never the SP
    object ID used in the role assignment). `client_secret` is a secret and is
    never logged.
    """

    tenant_id: str
    subscription_id: str
    resource_group: str
    zone: str
    client_id: str
    client_secret: str


class Config(NamedTuple):
    """Validated, fully-resolved runtime configuration.

    `provider` selects the archetype. `name` is the FQDN: for `azure` it drives
    the zone+record (validated against the token's `zone`); for `url` it is kept
    only for the DNS-resolve status/verification readout (it does not drive the
    update). `record_label` is the relative label derived by stripping the zone
    suffix off `name` (azure only; ``""`` for url). `azure` carries the parsed
    token; `url` carries `url_endpoint` (a secret) + `url_send_myip`.

    `state_path` is a dev-override key (default ``/data/last_known_ip``); it is
    absent from the HA schema, so a deployed add-on never sets it and the
    production state path cannot be misconfigured.

    For `url`, `url_insecure_skip_verify` opts the callback path out of TLS
    certificate verification (default off; never affects azure / ip-source TLS).
    """

    provider: Provider
    name: str
    test_ns: str
    azure: AzureToken | None
    record_label: str
    url_endpoint: str
    url_send_myip: bool
    url_insecure_skip_verify: bool
    ttl: int
    interval_seconds: int
    drift_reconcile_seconds: int
    ip_source_urls: tuple[str, ...]
    log_level: str
    state_path: str


class ApplyAction(str, Enum):
    """How `DnsProvider.apply` resolved this cycle's assertion.

    Distinguished so the updater and logs never claim a "match" they cannot
    substantiate: a known-IP write, a server-detected fire (no client IP), a
    skip because no valid IP was available, or a failure.
    """

    WROTE_KNOWN_IP = "wrote-known-ip"
    FIRED_SERVER_DETECTED = "fired-server-detected"
    SKIPPED_NO_IP = "skipped-no-ip"
    FAILED = "failed"


class ApplyResult(NamedTuple):
    """The outcome of one `DnsProvider.apply` call.

    `action` is the archetype-aware classification; `detail` is a redacted,
    secret-free human string for the log line. `written_ip` is the IP an API
    write committed (``None`` for a callback fire or a skip), so the updater can
    persist the detected IP only on a confirmed API write.
    """

    action: ApplyAction
    detail: str
    written_ip: IPv4Address | None


class ResolveStatus(str, Enum):
    """The three-way resolver outcome (never collapsed to one ``None``).

    `RESOLVED` carries a concrete value; `NO_RECORD` is an authoritative empty /
    NXDOMAIN answer (the record genuinely does not exist); `FAILED` is a
    transient query failure (timeout, NS unreachable, UDP loss). The updater's
    callback-confirmation gate treats these three completely differently.
    """

    RESOLVED = "resolved"
    NO_RECORD = "no-record"
    FAILED = "failed"


class ResolveOutcome(NamedTuple):
    """A resolver result: a status plus the value when (and only when) RESOLVED.

    `value` is the resolved `IPv4Address` iff `status is RESOLVED`, else
    ``None``. Callers must branch on `status`, never infer "stale" from a
    ``None`` value (a `FAILED` query also carries ``None``).
    """

    status: ResolveStatus
    value: IPv4Address | None


class IpSource(Protocol):
    """Egress-IPv4 discovery seam.

    Returns the box's detected global-unicast egress IPv4, or ``None`` when every
    configured source failed or returned a non-global address (the updater then
    holds last-good for `azure`, and still fires for `url`).
    """

    def detect(self) -> IPv4Address | None: ...


class Resolver(Protocol):
    """DNS-resolve seam for `name`, returning the three-way `ResolveOutcome`.

    Honors `test_ns` (a cache-free authoritative/recursive UDP query) when set,
    else falls back to the system resolver. A transient failure is reported as
    `ResolveStatus.FAILED`, distinct from `NO_RECORD`.
    """

    def resolve(self, name: str) -> ResolveOutcome: ...


class DnsProvider(Protocol):
    """The provider seam covering both archetypes.

    `read_current` returns the provider's authoritative current value (API
    archetype: a management GET) or ``None`` (callback archetype: the server owns
    the value, so drift is judged by DNS-resolving `name` instead).

    `apply` asserts the record. The API archetype **requires** a known
    `detected_ip` (a ``None`` is a `SKIPPED_NO_IP` no-op — nothing valid to
    write); the callback archetype **fires regardless** (server-side detection is
    the whole point), appending `myip` only when a detected IP is known.
    """

    def read_current(self) -> IPv4Address | None: ...

    def apply(self, detected_ip: IPv4Address | None) -> ApplyResult: ...


class Clock(Protocol):
    """Monotonic seconds source, injected so backoff is deterministic in tests."""

    def __call__(self) -> float: ...


class Sleeper(Protocol):
    """Interruptible sleep seam.

    Sleeps `seconds`, returning early if the stop signal fires; returns ``True``
    iff stop was signalled (so the caller aborts before the next attempt). The
    real impl is a ``threading.Event.wait``; the `--check` oracle injects a
    fully synchronous fake — **no real ``Timer`` / thread**.
    """

    def __call__(self, seconds: float) -> bool: ...
