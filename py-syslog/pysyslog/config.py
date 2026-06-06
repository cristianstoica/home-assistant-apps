# pyright: strict
"""Load and validate ``/data/options.json`` into a `Config`.

Validation is strict and **names the offending field** on every rejection, so a
misconfigured add-on fails fast with an actionable message rather than binding
and silently mis-resolving. The list-of-sources is converted to a
``dict[ip] -> SourceMapping``; a duplicate ``ip`` would silently overwrite an
identity, so it is rejected here. Empty ``ip`` / ``site`` / ``host`` are rejected
for the same reason.

`log_dir` / `log_file` are recognized **optional dev-override** keys (defaults
``/data/log`` / ``syslog.log``). They are absent from the HA schema, so a
deployed add-on never sets them; passing them via ``--options`` is a documented
testing override, not an unknown field.
"""

from __future__ import annotations

import ipaddress
import json
from pathlib import Path
from typing import cast

from .models import Config, SourceMapping

DEFAULT_OPTIONS_PATH = "/data/options.json"
DEFAULT_LOG_DIR = "/data/log"
DEFAULT_LOG_FILE = "syslog.log"

_VALID_LOG_LEVELS = ("debug", "info", "warning", "error")
_MIN_RETENTION = 1
_MAX_RETENTION = 3650
_MIN_PORT = 1
_MAX_PORT = 65535
_MIN_PCT = 0
_MAX_PCT = 99
_MIN_SEG_MB = 0
_MAX_SEG_MB = 4096


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


def _build_sources(raw_sources: object) -> dict[str, SourceMapping]:
    """Validate the sources list into an IP-keyed mapping.

    Rejects: a non-list ``sources``; a non-dict entry; an empty/whitespace
    ``ip`` / ``site`` / ``host``; or a duplicate ``ip`` (which would otherwise
    silently overwrite an identity). The error names the offending field.
    """
    if not isinstance(raw_sources, list):
        raise ConfigError("sources: must be a list")
    entries = cast(list[object], raw_sources)
    sources: dict[str, SourceMapping] = {}
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ConfigError(f"sources[{index}]: must be an object")
        entry_dict = cast(dict[str, object], entry)
        ip = _source_field(entry_dict, "ip", index)
        _require_ipv4(ip, "ip", f" in sources[{index}]")
        site = _source_field(entry_dict, "site", index)
        _reject_control_chars(site, "site", f" in sources[{index}]")
        host = _source_field(entry_dict, "host", index)
        _reject_control_chars(host, "host", f" in sources[{index}]")
        if ip in sources:
            raise ConfigError(f"ip: duplicate source ip {ip!r}")
        sources[ip] = SourceMapping(ip=ip, site=site, host=host)
    return sources


def _source_field(entry: dict[str, object], field: str, index: int) -> str:
    """Read and non-empty-validate one source field, naming it on failure."""
    value = entry.get(field)
    if not isinstance(value, str):
        raise ConfigError(f"{field}: must be a string in sources[{index}]")
    if value.strip() == "":
        raise ConfigError(f"{field}: must not be empty in sources[{index}]")
    return value


def _reject_control_chars(value: str, field: str, context: str) -> None:
    """Reject config-derived labels carrying any char ``_escape`` would transform
    into a line break / control escape (C0, DEL, C1, U+2028/U+2029).

    `site`/`host` are config-derived and emitted RAW into the stored line (the
    render path does not escape them), so a control char here is the only way one
    could split or corrupt a stored line. Rejecting at load keeps the render path
    a no-op for these fields. Backslash is intentionally allowed (not a line
    break; it would render harmlessly).
    """
    for ch in value:
        code = ord(ch)
        if (
            code < 0x20
            or code == 0x7F
            or 0x80 <= code <= 0x9F
            or code in (0x2028, 0x2029)
        ):
            raise ConfigError(f"{field}: must not contain control characters{context}")


def _require_ipv4(value: str, field: str, context: str = "") -> None:
    """Reject anything that is not a bare IPv4 literal. AF_INET only (the socket
    binds ``socket.AF_INET``), so IPv6 is intentionally rejected here too.

    ``ipaddress.IPv4Address`` rejects empty/whitespace, embedded control chars,
    IPv6, CIDR, leading zeros, and trailing spaces — exactly the garbage that
    would otherwise flow into ``socket.bind`` or a resolver key.
    """
    try:
        ipaddress.IPv4Address(value)
    except (ipaddress.AddressValueError, ValueError):
        raise ConfigError(f"{field}: must be an IPv4 address{context}") from None


def validate(options: dict[str, object]) -> Config:
    """Validate an already-parsed options dict into a `Config`.

    Pure with respect to its argument (no I/O). Raises `ConfigError` naming the
    field on any bad type, out-of-range value, empty/duplicate source field.
    """
    listen_port = _require_int(options, "listen_port", 5514)
    if listen_port < _MIN_PORT or listen_port > _MAX_PORT:
        raise ConfigError(f"listen_port: must be {_MIN_PORT}-{_MAX_PORT}")

    # `listen_host` is required (no Python default): the HA schema in config.yaml
    # supplies the bind-all (``0.0.0.0``) default so a deployed add-on always
    # provides it at runtime. Keeping the default *out* of Python means no
    # bind-all string literal can flow into the socket.bind sink — the
    # py/bind-socket-all-network-interfaces invariant. The dev/--options path
    # must supply it explicitly.
    listen_host = options.get("listen_host")
    if not isinstance(listen_host, str):
        raise ConfigError("listen_host: must be a string")
    if listen_host.strip() == "":
        raise ConfigError("listen_host: must not be empty")
    _require_ipv4(listen_host, "listen_host")

    retention_days = _require_int(options, "retention_days", 30)
    if retention_days < _MIN_RETENTION or retention_days > _MAX_RETENTION:
        raise ConfigError(f"retention_days: must be {_MIN_RETENTION}-{_MAX_RETENTION}")

    min_free_percent = _require_int(options, "min_free_percent", 0)
    if min_free_percent < _MIN_PCT or min_free_percent > _MAX_PCT:
        raise ConfigError(f"min_free_percent: must be {_MIN_PCT}-{_MAX_PCT}")

    max_log_percent = _require_int(options, "max_log_percent", 0)
    if max_log_percent < _MIN_PCT or max_log_percent > _MAX_PCT:
        raise ConfigError(f"max_log_percent: must be {_MIN_PCT}-{_MAX_PCT}")

    max_segment_mb = _require_int(options, "max_segment_mb", 0)
    if max_segment_mb < _MIN_SEG_MB or max_segment_mb > _MAX_SEG_MB:
        raise ConfigError(f"max_segment_mb: must be {_MIN_SEG_MB}-{_MAX_SEG_MB}")

    # Coherence gate (allowlist-style): proceed only if size-rotation is enabled
    # whenever either percentage guard is. Without intra-day segments there is
    # nothing to prune, so a flood in the single active file silently defeats the
    # cap; rejecting here turns that invisible failure into a fail-fast
    # ConfigError. The all-zero 1.2.0 default passes this check untouched.
    if (min_free_percent > 0 or max_log_percent > 0) and max_segment_mb <= 0:
        raise ConfigError(
            "max_segment_mb: size guard (min_free_percent/max_log_percent) "
            "requires max_segment_mb > 0"
        )

    reject_unknown_sources = _require_bool(options, "reject_unknown_sources", False)

    log_level = _require_str(options, "log_level", "info")
    if log_level not in _VALID_LOG_LEVELS:
        raise ConfigError(f"log_level: must be one of {', '.join(_VALID_LOG_LEVELS)}")

    sources = _build_sources(options.get("sources", []))
    log_dir = _require_str(options, "log_dir", DEFAULT_LOG_DIR)
    log_file = _require_str(options, "log_file", DEFAULT_LOG_FILE)

    return Config(
        listen_port=listen_port,
        listen_host=listen_host,
        retention_days=retention_days,
        min_free_percent=min_free_percent,
        max_log_percent=max_log_percent,
        max_segment_mb=max_segment_mb,
        reject_unknown_sources=reject_unknown_sources,
        log_level=log_level,
        sources=sources,
        log_dir=log_dir,
        log_file=log_file,
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
