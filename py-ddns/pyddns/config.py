# pyright: strict
"""Load and validate ``/data/options.json`` into a `Config`.

Validation is strict and **names the offending field** on every rejection, so a
misconfigured add-on fails fast with an actionable message (the py-syslog
`config.py:load` pattern). Validation is **per-provider**: the `azure` token is
required only for `provider=azure`, `url_endpoint` only for `provider=url`.

Two security-load-bearing contracts live here:

* **Azure name↔zone contract.** The token blob's `zone` is authoritative.
  `name` must be a strict sub-record of `zone` (``name.endswith("." + zone)``
  **and** ``name != zone``); the relative record label is derived by stripping
  the zone suffix. The **zone apex is rejected** (``name == zone``) — a host
  DDNS updater must never repoint a zone apex, which on a shared zone is the
  live site's record. Wrong-zone / empty / malformed labels are rejected.

* **HTTPS-only URL contract.** `url_endpoint` and every `ip_source_urls` entry
  must be an absolute ``https://`` URL with a host and no userinfo or fragment.
  A plaintext callback would leak the record-repointing secret in transit; a
  plaintext/spoofable ip-source could make the add-on publish an attacker-chosen
  A record. No insecure opt-in in v1.

`state_path` is a recognized **optional dev-override** key (default
``/data/last_known_ip``), absent from the HA schema, so a deployed add-on never
sets it; passing it via ``--options`` is a documented testing override.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit

from .models import AzureToken, Config, Provider

DEFAULT_OPTIONS_PATH = "/data/options.json"
DEFAULT_STATE_PATH = "/data/last_known_ip"

_VALID_LOG_LEVELS = ("debug", "info", "warning", "error")
_VALID_PROVIDERS = tuple(p.value for p in Provider)
_MIN_TTL = 30
_MAX_TTL = 86400
_MIN_INTERVAL = 60
_MAX_INTERVAL = 86400
_MIN_DRIFT = 0
_MAX_DRIFT = 86400

_AZURE_TOKEN_FIELDS = (
    "tenantId",
    "subscriptionId",
    "resourceGroup",
    "zone",
    "clientId",
    "clientSecret",
)

_DEFAULT_IP_SOURCES = (
    "https://api.ipify.org",
    "https://icanhazip.com",
)

# A single DNS label: 1-63 LDH chars (letters/digits/hyphen), no leading or
# trailing hyphen. ASCII-only — IDN is not supported in v1 (the resolver's idna
# path remains as defense-in-depth, not a config-accepted input).
_DNS_LABEL_RE = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")
_MAX_HOSTNAME_OCTETS = 253


class ConfigError(Exception):
    """Raised when options are invalid; the message names the offending field."""


def _require_int(options: dict[str, object], field: str, default: int) -> int:
    """Read an int field, rejecting the wrong type (``bool`` is not an int here)."""
    if field not in options:
        return default
    value = options[field]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{field}: must be an integer")
    return value


def _require_str(options: dict[str, object], field: str, default: str) -> str:
    """Read a str field of the expected type (no emptiness check at this layer)."""
    if field not in options:
        return default
    value = options[field]
    if not isinstance(value, str):
        raise ConfigError(f"{field}: must be a string")
    return value


def _require_bool(options: dict[str, object], field: str, default: bool) -> bool:
    """Read a real bool field. JSON/HA ``bool`` schema yields a Python ``bool``;
    an int (incl. 0/1) or string is rejected so the option is unambiguous."""
    if field not in options:
        return default
    value = options[field]
    if not isinstance(value, bool):
        raise ConfigError(f"{field}: must be a boolean")
    return value


def _normalize_dns_name(value: str) -> str:
    """Lowercase + strip a single trailing dot for case/trailing-dot-insensitive
    comparison of DNS names. Empty stays empty (caught by the caller)."""
    lowered = value.strip().lower()
    return lowered[:-1] if lowered.endswith(".") else lowered


def _validate_https_url(value: str, field: str) -> None:
    """Reject anything that is not an absolute ``https://`` URL with a bare host.

    Rejects: a non-``https`` scheme (``http:`` / ``file:`` / schemeless), a
    missing host, embedded userinfo (``user:pass@``), and a fragment. A plaintext
    or spoofable URL on either the callback or an ip-source is a credential-leak
    / record-poisoning vector, so the contract is strict and names `field`.
    """
    try:
        parts = urlsplit(value)
    except ValueError:
        raise ConfigError(f"{field}: must be a valid https:// URL") from None
    if parts.scheme != "https":
        raise ConfigError(
            f"{field}: must be an https:// URL (got scheme {parts.scheme!r})"
        )
    if not parts.hostname:
        raise ConfigError(f"{field}: must include a host")
    if parts.username is not None or parts.password is not None:
        raise ConfigError(f"{field}: must not contain userinfo (user:pass@)")
    if parts.fragment != "":
        raise ConfigError(f"{field}: must not contain a fragment (#...)")


def validate_dns_hostname(name: str, field: str) -> None:
    """Reject a `name` that is not a syntactically valid ASCII DNS hostname.

    Validates the **normalized** wire form (`_normalize_dns_name`: lowercased,
    single trailing dot stripped) so the form actually sent on the wire is what
    is checked. The contract is an allowlist (proceed only if nothing but
    known-safe LDH remains), the security-honest direction for input fed to the
    resolver:

    * non-empty after normalization (else ``{field}: required``);
    * total length ≤ 253 octets;
    * every dot-separated label is 1-63 chars and matches `_DNS_LABEL_RE`
      (letters/digits/hyphen, no leading/trailing hyphen).

    Fail-fast at config load: an invalid `name` never reaches
    `resolver.resolve()` (the resolver's idna guard is then defense-in-depth, not
    the first line). Naming `field` mirrors the rest of the validator.
    """
    norm = _normalize_dns_name(name)
    if norm == "":
        raise ConfigError(f"{field}: required")
    if len(norm.encode("ascii", "replace")) > _MAX_HOSTNAME_OCTETS:
        raise ConfigError(f"{field}: hostname exceeds 253 octets")
    for label in norm.split("."):
        if not 1 <= len(label) <= 63 or _DNS_LABEL_RE.match(label) is None:
            raise ConfigError(
                f"{field}: label {label!r} is not a valid DNS label "
                "(1-63 chars, letters/digits/hyphen)"
            )


def _parse_azure_token(raw: object) -> AzureToken:
    """Parse + validate the ``azure_token`` SP credential blob.

    Accepts either a JSON-string blob (the pasted SP credential) or an already-
    decoded object. Every field in `_AZURE_TOKEN_FIELDS` is required and must be a
    non-empty string. The error names the offending field; the secret value is
    never echoed.
    """
    if isinstance(raw, str):
        text = raw.strip()
        if text == "":
            raise ConfigError("azure_token: required when provider=azure")
        try:
            decoded: object = json.loads(text)
        except json.JSONDecodeError:
            raise ConfigError(
                "azure_token: must be a valid JSON credential blob"
            ) from None
    else:
        decoded = raw
    if not isinstance(decoded, dict):
        raise ConfigError("azure_token: must be a JSON object credential blob")
    blob = cast(dict[str, object], decoded)
    values: dict[str, str] = {}
    for field in _AZURE_TOKEN_FIELDS:
        value = blob.get(field)
        if not isinstance(value, str) or value.strip() == "":
            raise ConfigError(f"azure_token.{field}: required non-empty string")
        values[field] = value.strip()
    return AzureToken(
        tenant_id=values["tenantId"],
        subscription_id=values["subscriptionId"],
        resource_group=values["resourceGroup"],
        zone=values["zone"],
        client_id=values["clientId"],
        client_secret=values["clientSecret"],
    )


def derive_record_label(name: str, zone: str) -> str:
    """Derive the relative record label by enforcing the name↔zone contract.

    Returns the relative label (e.g. ``home`` for ``home.example.com`` under zone
    ``example.com``). Rejects an empty `name`, a `name` whose normalized form does
    not strictly fall under `zone`, and the **zone apex** (``name == zone``,
    label ``@``). Comparison is case- and trailing-dot-insensitive.
    """
    norm_name = _normalize_dns_name(name)
    norm_zone = _normalize_dns_name(zone)
    if norm_name == "":
        raise ConfigError("name: required when provider=azure")
    if norm_zone == "":
        raise ConfigError("azure_token.zone: must not be empty")
    if norm_name == norm_zone:
        raise ConfigError(
            f"name: must be a sub-record of zone {norm_zone!r}, not the zone apex "
            "(a host DDNS updater must never repoint a zone apex)"
        )
    suffix = "." + norm_zone
    if not norm_name.endswith(suffix):
        raise ConfigError(
            f"name: {norm_name!r} is not under the token's zone {norm_zone!r}"
        )
    label = norm_name[: -len(suffix)]
    if label == "":
        raise ConfigError(f"name: empty record label under zone {norm_zone!r}")
    return label


def _build_ip_sources(raw: object) -> tuple[str, ...]:
    """Validate the ip_source_urls list (HTTPS-only) into a tuple.

    A missing key uses the built-in default pair. Rejects a non-list, a
    non-string entry, and any non-``https`` / hostless / userinfo / fragment
    entry — each naming ``ip_source_urls[i]``. An empty list is rejected for
    `azure` by the caller (the API archetype needs an egress-IP source).
    """
    if raw is None:
        return _DEFAULT_IP_SOURCES
    if not isinstance(raw, list):
        raise ConfigError("ip_source_urls: must be a list")
    entries = cast(list[object], raw)
    urls: list[str] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, str):
            raise ConfigError(f"ip_source_urls[{index}]: must be a string")
        _validate_https_url(entry, f"ip_source_urls[{index}]")
        urls.append(entry.strip())
    return tuple(urls)


def validate(options: dict[str, object]) -> Config:
    """Validate an already-parsed options dict into a `Config`.

    Pure with respect to its argument (no I/O). Raises `ConfigError` naming the
    field on any bad type, out-of-range value, or per-provider contract breach.
    """
    provider_raw = _require_str(options, "provider", Provider.AZURE.value)
    if provider_raw not in _VALID_PROVIDERS:
        raise ConfigError(f"provider: must be one of {', '.join(_VALID_PROVIDERS)}")
    provider = Provider(provider_raw)

    name = _require_str(options, "name", "")
    test_ns = _require_str(options, "test_ns", "")

    ttl = _require_int(options, "ttl", 60)
    if ttl < _MIN_TTL or ttl > _MAX_TTL:
        raise ConfigError(f"ttl: must be {_MIN_TTL}-{_MAX_TTL}")

    interval_seconds = _require_int(options, "interval_seconds", 120)
    if interval_seconds < _MIN_INTERVAL or interval_seconds > _MAX_INTERVAL:
        raise ConfigError(f"interval_seconds: must be {_MIN_INTERVAL}-{_MAX_INTERVAL}")

    drift_reconcile_seconds = _require_int(options, "drift_reconcile_seconds", 3600)
    if drift_reconcile_seconds < _MIN_DRIFT or drift_reconcile_seconds > _MAX_DRIFT:
        raise ConfigError(f"drift_reconcile_seconds: must be {_MIN_DRIFT}-{_MAX_DRIFT}")

    log_level = _require_str(options, "log_level", "info")
    if log_level not in _VALID_LOG_LEVELS:
        raise ConfigError(f"log_level: must be one of {', '.join(_VALID_LOG_LEVELS)}")

    ip_source_urls = _build_ip_sources(options.get("ip_source_urls"))

    url_send_myip = _require_bool(options, "url_send_myip", False)

    azure: AzureToken | None = None
    record_label = ""
    url_endpoint = ""

    if provider is Provider.AZURE:
        if name.strip() == "":
            raise ConfigError("name: required when provider=azure")
        # Label-syntax validation is complementary to derive_record_label's
        # name<->zone relationship check: this rejects a malformed label before
        # the zone-suffix derivation looks at the structure.
        validate_dns_hostname(name, "name")
        if not ip_source_urls:
            raise ConfigError(
                "ip_source_urls: at least one source required when provider=azure"
            )
        azure = _parse_azure_token(options.get("azure_token", ""))
        record_label = derive_record_label(name, azure.zone)
    else:  # Provider.URL
        # `name` is REQUIRED for the url archetype: it is the DNS
        # verification/drift signal (resolved post-fire to confirm the callback
        # took, and used to suppress a steady-state refire). An unvalidated
        # `name` would reach resolver.resolve() unguarded.
        if name.strip() == "":
            raise ConfigError(
                "name: required for the DNS verification readout when provider=url"
            )
        validate_dns_hostname(name, "name")
        url_endpoint = _require_str(options, "url_endpoint", "")
        if url_endpoint.strip() == "":
            raise ConfigError("url_endpoint: required when provider=url")
        _validate_https_url(url_endpoint.strip(), "url_endpoint")
        url_endpoint = url_endpoint.strip()

    state_path = _require_str(options, "state_path", DEFAULT_STATE_PATH)

    return Config(
        provider=provider,
        name=name.strip(),
        test_ns=test_ns.strip(),
        azure=azure,
        record_label=record_label,
        url_endpoint=url_endpoint,
        url_send_myip=url_send_myip,
        ttl=ttl,
        interval_seconds=interval_seconds,
        drift_reconcile_seconds=drift_reconcile_seconds,
        ip_source_urls=ip_source_urls,
        log_level=log_level,
        state_path=state_path,
    )


def load(path: str = DEFAULT_OPTIONS_PATH) -> Config:
    """Read + validate an options.json file into a `Config`.

    Raises `ConfigError` (naming the cause) if the file is missing or not valid
    JSON, and otherwise delegates field validation to `validate`.
    """
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"options file: cannot read {path}: {exc}") from exc
    try:
        parsed: object = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"options file: invalid JSON in {path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ConfigError("options file: top-level value must be an object")
    options = cast(dict[str, object], parsed)
    return validate(options)
