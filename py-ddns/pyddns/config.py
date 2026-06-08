# pyright: strict
"""Load and validate ``/data/options.json`` into a `Config`.

Validation is strict and **names the offending field** on every rejection, so a
misconfigured add-on fails fast with an actionable message (the py-syslog
`config.py:load` pattern). There is **no HA `provider` option** — the runtime
provider is *inferred* from whichever nested section the operator filled: a
present `url.endpoint` selects the callback archetype, otherwise any of the six
Azure credential fields selects the API archetype.

Two security-load-bearing contracts live here:

* **Azure name↔zone contract.** The `azure.zone` field is authoritative.
  `name` must be a strict sub-record of `zone` (``name.endswith("." + zone)``
  **and** ``name != zone``); the relative record label is derived by stripping
  the zone suffix. The **zone apex is rejected** (``name == zone``) — a host
  DDNS updater must never repoint a zone apex, which on a shared zone is the
  live site's record. Wrong-zone / empty / malformed labels are rejected.

* **HTTPS-only URL contract.** `url.endpoint` and every `azure.ip_sources` entry
  must be an absolute ``https://`` URL with a host and no userinfo or fragment.
  A plaintext callback would leak the record-repointing secret in transit; a
  plaintext/spoofable ip-source could make the add-on publish an attacker-chosen
  A record. No insecure opt-in in v1.

When **both** sections are filled, the callback URL wins (the Azure group is
ignored, not parsed) and the loader surfaces that on `ConfigSelection` so the
imperative shell can warn — `validate()` itself stays pure (no logging).

`state_path` is a recognized **optional dev-override** key (default
``/data/last_known_ip``), absent from the HA schema, so a deployed add-on never
sets it; passing it via ``--options`` is a documented testing override.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import NamedTuple, cast
from urllib.parse import urlsplit

from .models import AzureToken, Config, Provider

DEFAULT_OPTIONS_PATH = "/data/options.json"
DEFAULT_STATE_PATH = "/data/last_known_ip"

_VALID_LOG_LEVELS = ("debug", "info", "warning", "error")
_MIN_TTL = 30
_MAX_TTL = 86400
_MIN_INTERVAL = 60
_MAX_INTERVAL = 86400
_MIN_DRIFT = 0
_MAX_DRIFT = 86400

# The six Azure credential fields, as the snake_case option keys under `azure:`.
# These both (a) drive the `azure_selected` allowlist gate and (b) map onto the
# unchanged `AzureToken` NamedTuple fields. `zone` is snake_case-identical to its
# old camelCase form, so it stays the sixth entry (it must NOT be dropped — a
# zone-only Azure group still contributes to `azure_selected`). `ttl`,
# `ip_sources`, and `send_myip` carry defaults and are deliberately excluded from
# this set so a value-blind ``any(...)`` cannot mis-fire selection.
_AZURE_TOKEN_FIELDS = (
    "tenant_id",
    "subscription_id",
    "resource_group",
    "zone",
    "client_id",
    "client_secret",
)

_DEFAULT_IP_SOURCES = (
    "https://api.ipify.org",
    "https://icanhazip.com",
)

# The shared production warning text, in one place (see `warn_azure_ignored`).
_AZURE_IGNORED_MESSAGE = "Azure options ignored due to callback URL present"

# A single DNS label: 1-63 LDH chars (letters/digits/hyphen), no leading or
# trailing hyphen. ASCII-only — IDN is not supported in v1 (the resolver's idna
# path remains as defense-in-depth, not a config-accepted input).
_DNS_LABEL_RE = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")
_MAX_HOSTNAME_OCTETS = 253


class ConfigError(Exception):
    """Raised when options are invalid; the message names the offending field."""


class ConfigSelection(NamedTuple):
    """The validated `Config` plus the both-filled provenance flag.

    `azure_options_ignored` is ``True`` iff a callback URL was selected **while**
    the Azure group was also populated — the URL-wins path sets ``config.azure``
    to ``None`` and does **not** parse the ignored group, so the both-filled
    condition cannot be re-derived from `config` alone and must travel here.

    `validate()` stays pure (it never logs); the imperative shell (``_run_loop`` /
    ``run_dry_run``) reads this flag and calls `warn_azure_ignored` once logging
    is configured, so the warning reaches the HA Log tab and the dry-run preview.
    """

    config: Config
    azure_options_ignored: bool


def warn_azure_ignored(logger: logging.Logger) -> None:
    """Emit the single 'Azure options ignored' warning (the one shared source).

    Logged at **warning** level deliberately: the real dry-run/production runtime
    configures the logger at ``INFO`` (``basicConfig(level=INFO)``), so a
    ``debug`` emit would silently vanish there while still passing the ``--check``
    recorder (which runs at DEBUG). The text has a single source so ``--check``,
    the dry-run preview, and production all emit the identical string.
    """
    logger.warning(_AZURE_IGNORED_MESSAGE)


def _require_int(
    options: dict[str, object], field: str, default: int, label: str | None = None
) -> int:
    """Read an int field, rejecting the wrong type (``bool`` is not an int here).

    `label` overrides the field name in the error (for a nested group key whose
    lookup key is bare, e.g. ``ttl`` under ``azure`` → ``azure.ttl``).
    """
    if field not in options:
        return default
    value = options[field]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{label or field}: must be an integer")
    return value


def _require_str(
    options: dict[str, object], field: str, default: str, label: str | None = None
) -> str:
    """Read a str field of the expected type (no emptiness check at this layer).

    `label` overrides the field name in the error (see `_require_int`).
    """
    if field not in options:
        return default
    value = options[field]
    if not isinstance(value, str):
        raise ConfigError(f"{label or field}: must be a string")
    return value


def _require_bool(
    options: dict[str, object], field: str, default: bool, label: str | None = None
) -> bool:
    """Read a real bool field. JSON/HA ``bool`` schema yields a Python ``bool``;
    an int (incl. 0/1) or string is rejected so the option is unambiguous.

    `label` overrides the field name in the error (see `_require_int`).
    """
    if field not in options:
        return default
    value = options[field]
    if not isinstance(value, bool):
        raise ConfigError(f"{label or field}: must be a boolean")
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


def _require_group(options: dict[str, object], field: str) -> dict[str, object]:
    """Read a nested option group, requiring it to be absent or a dict.

    A non-dict ``url``/``azure`` (e.g. a stale flat string lingering in
    ``/data/options.json`` from the migration off the single ``azure_token`` blob)
    is rejected naming the field — never crashed, never silently treated as "not
    configured". An absent group yields an empty dict so every read sees a "not
    configured" view.
    """
    if field not in options:
        return {}
    value = options[field]
    if not isinstance(value, dict):
        raise ConfigError(f"{field}: must be an object")
    return cast(dict[str, object], value)


def _parse_azure_token(group: dict[str, object]) -> AzureToken:
    """Parse + validate the nested ``azure`` credential group into an `AzureToken`.

    Every field in `_AZURE_TOKEN_FIELDS` (the six credential keys, including
    `zone`) is required and must be a non-empty string. The error names the
    offending field (``azure.<field>``); the secret value is never echoed.
    """
    values: dict[str, str] = {}
    for field in _AZURE_TOKEN_FIELDS:
        value = group.get(field)
        if not isinstance(value, str) or value.strip() == "":
            raise ConfigError(f"azure.{field}: required non-empty string")
        values[field] = value.strip()
    return AzureToken(
        tenant_id=values["tenant_id"],
        subscription_id=values["subscription_id"],
        resource_group=values["resource_group"],
        zone=values["zone"],
        client_id=values["client_id"],
        client_secret=values["client_secret"],
    )


def _is_filled(value: object) -> bool:
    """True iff `value` is a non-blank string (the allowlist selection predicate)."""
    return isinstance(value, str) and value.strip() != ""


def _azure_selected(group: dict[str, object]) -> bool:
    """True iff any of the six Azure credential fields is a non-blank string.

    `ttl`, `ip_sources`, `send_myip`, and an empty/absent group are excluded — they
    carry defaults, so a value-blind ``any(group.values())`` would mis-fire. A
    partial group (even `zone` alone) still selects Azure mode.
    """
    return any(_is_filled(group.get(field)) for field in _AZURE_TOKEN_FIELDS)


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
        raise ConfigError("name: required")
    if norm_zone == "":
        raise ConfigError("azure.zone: must not be empty")
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
    """Validate the comma/space-separated ``azure.ip_sources`` string (HTTPS-only).

    An absent key, an empty string, or a string of only separators resolves to the
    built-in default pair (``_DEFAULT_IP_SOURCES``) — the default Azure box loads
    with the built-in sources and **no** error. There is no "empty source set
    rejected" path. A **non-empty** value is split on commas and whitespace; each
    resulting entry must pass the HTTPS-only check, each rejection naming
    ``azure.ip_sources``. A non-string group value is rejected the same way.
    """
    if raw is None:
        return _DEFAULT_IP_SOURCES
    if not isinstance(raw, str):
        raise ConfigError("azure.ip_sources: must be a string")
    entries = [token for token in re.split(r"[,\s]+", raw.strip()) if token != ""]
    if not entries:
        return _DEFAULT_IP_SOURCES
    for entry in entries:
        _validate_https_url(entry, "azure.ip_sources")
    return tuple(entries)


def validate(options: dict[str, object]) -> ConfigSelection:
    """Validate an already-parsed options dict into a `ConfigSelection`.

    Pure with respect to its argument (no I/O, **no logging**). Raises
    `ConfigError` naming the field on any bad type, out-of-range value, or
    contract breach. The provider is **inferred** from the filled section:

    * a present, non-blank `url.endpoint` selects the callback archetype (URL
      wins — if the Azure group is also populated it is **ignored**, not parsed,
      and ``azure_options_ignored`` is set on the returned `ConfigSelection`);
    * else any of the six `azure` credential fields selects the API archetype;
    * else neither is configured and validation errors.
    """
    url_group = _require_group(options, "url")
    azure_group = _require_group(options, "azure")

    name = _require_str(options, "name", "")
    test_ns = _require_str(options, "test_ns", "")

    interval_seconds = _require_int(options, "interval_seconds", 120)
    if interval_seconds < _MIN_INTERVAL or interval_seconds > _MAX_INTERVAL:
        raise ConfigError(f"interval_seconds: must be {_MIN_INTERVAL}-{_MAX_INTERVAL}")

    drift_reconcile_seconds = _require_int(options, "drift_reconcile_seconds", 3600)
    if drift_reconcile_seconds < _MIN_DRIFT or drift_reconcile_seconds > _MAX_DRIFT:
        raise ConfigError(f"drift_reconcile_seconds: must be {_MIN_DRIFT}-{_MAX_DRIFT}")

    log_level = _require_str(options, "log_level", "info")
    if log_level not in _VALID_LOG_LEVELS:
        raise ConfigError(f"log_level: must be one of {', '.join(_VALID_LOG_LEVELS)}")

    # URL mode keeps the built-in default IP sources; the `ip_sources` and `ttl`
    # fields live under the Azure group for UI placement only, so both are read
    # regardless of the selected provider (an absent/empty `ip_sources` resolves
    # to the defaults; `ttl` is range-checked here so a bad value is named even on
    # the URL path).
    ip_source_urls = _build_ip_sources(azure_group.get("ip_sources"))
    ttl = _require_int(azure_group, "ttl", 60, label="azure.ttl")
    if ttl < _MIN_TTL or ttl > _MAX_TTL:
        raise ConfigError(f"azure.ttl: must be {_MIN_TTL}-{_MAX_TTL}")

    url_send_myip = _require_bool(url_group, "send_myip", False, label="url.send_myip")

    url_selected = _is_filled(url_group.get("endpoint"))
    azure_selected = _azure_selected(azure_group)

    azure: AzureToken | None = None
    record_label = ""
    url_endpoint = ""
    azure_options_ignored = False

    if url_selected:
        provider = Provider.URL
        # URL wins: the Azure group (if any) is ignored and NOT parsed. The flag
        # travels on ConfigSelection so the shell can warn after logging is up —
        # validate() must not log.
        azure_options_ignored = azure_selected
        # `name` is REQUIRED for the url archetype: it is the DNS
        # verification/drift signal (resolved post-fire to confirm the callback
        # took, and used to suppress a steady-state refire). An unvalidated
        # `name` would reach resolver.resolve() unguarded.
        if name.strip() == "":
            raise ConfigError("name: required for the DNS verification readout")
        validate_dns_hostname(name, "name")
        url_endpoint = _require_str(url_group, "endpoint", "", label="url.endpoint")
        if url_endpoint.strip() == "":
            raise ConfigError("url.endpoint: required")
        _validate_https_url(url_endpoint.strip(), "url.endpoint")
        url_endpoint = url_endpoint.strip()
    elif azure_selected:
        provider = Provider.AZURE
        if name.strip() == "":
            raise ConfigError("name: required")
        # Label-syntax validation is complementary to derive_record_label's
        # name<->zone relationship check: this rejects a malformed label before
        # the zone-suffix derivation looks at the structure.
        validate_dns_hostname(name, "name")
        azure = _parse_azure_token(azure_group)
        record_label = derive_record_label(name, azure.zone)
    else:
        raise ConfigError(
            "url.endpoint / azure: neither a callback URL nor Azure credentials "
            "are configured — nothing to do (fill the Callback URL or Azure DNS "
            "section)"
        )

    state_path = _require_str(options, "state_path", DEFAULT_STATE_PATH)

    config = Config(
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
    return ConfigSelection(config=config, azure_options_ignored=azure_options_ignored)


def load(path: str = DEFAULT_OPTIONS_PATH) -> ConfigSelection:
    """Read + validate an options.json file into a `ConfigSelection`.

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
