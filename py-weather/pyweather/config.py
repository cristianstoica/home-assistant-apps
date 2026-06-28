# pyright: strict
"""Load and validate ``/data/options.json`` into a `Config`.

Validation is strict and **names the offending field** on every rejection, so a
misconfigured add-on fails fast with an actionable message (the py-ddns /
py-syslog `config.py:load` pattern). Validation is **pure**: shape/range/
station-key checks only — it never reads ``/states`` and never resolves or
confirms live sensor presence. Live representative-sensor presence is resolved
only at runtime by the discovery glob and the binary obstime-presence health
predicate (`health` / `scheduler`).

Two contract-bearing checks live here:

* **Station-key allowlist.** Each station ``key`` must match ``^[a-z0-9]+$`` —
  the key is interpolated into **both** the ``update_entity`` matcher and the
  runtime discovery glob ``sensor.wu_*_<key>``, so it must be a regex-literal,
  lowercase-entity-id charset (matching HA's convention). It rejects the
  upper-case ``stationId`` form and any regex metacharacter. The charset check
  runs **before** any pattern is built from the key.

* **Representative entity-id shape.** ``update_entity`` must match the anchored
  ``^sensor\\.wu_[a-z0-9]+_<key>$``, where ``<key>`` is that station's own
  (already-allowlisted) ``key``. The metric segment (``[a-z0-9]+``) must be
  non-empty and the trailing key must match the station's ``key`` — tying the
  entity to its station so a wrong-key copy-paste is caught at validation time.
  This rejects the registry form ``sensor.rest_wu_*`` (the ``rest_`` prefix fails
  the required ``wu_`` immediately after ``sensor.``) and a bare ``sensor.``. The
  looser "starts with ``sensor.``" rule is **not** sufficient. Because ``<key>``
  is interpolated into the matcher, the key allowlist above (canonical guard)
  guarantees ``key`` is regex-literal before the pattern is built.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import cast

from .models import Config, Station

DEFAULT_OPTIONS_PATH = "/data/options.json"

_VALID_LOG_LEVELS = ("debug", "info", "warning", "error")

# A uniform allowlist posture: every duration field is bounded. The cadence
# fields share the py-ddns 60-86400 floor/ceiling; the sleeper-spent and timeout
# fields are bounded 1-300 rather than left open-ended.
_MIN_CADENCE = 60
_MAX_CADENCE = 86400
_MIN_SHORT = 1
_MAX_SHORT = 300

# The lowercase-alphanumeric station-key allowlist. Regex-literal by
# construction, so the key can be interpolated into the entity matcher / glob.
_STATION_KEY_RE = re.compile(r"^[a-z0-9]+$")


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


def _range_int(
    options: dict[str, object], field: str, default: int, low: int, high: int
) -> int:
    """Read an int field and reject it outside the inclusive ``[low, high]`` band."""
    value = _require_int(options, field, default)
    if value < low or value > high:
        raise ConfigError(f"{field}: must be {low}-{high}")
    return value


def _validate_station(raw: object, index: int) -> Station:
    """Validate one raw station object into a `Station`, naming the offending field.

    Enforces the key allowlist **before** building the entity matcher (so the key
    is always regex-literal), then the anchored ``update_entity`` shape tying the
    entity to this station's key.
    """
    if not isinstance(raw, dict):
        raise ConfigError(f"stations[{index}]: must be an object")
    station = cast(dict[str, object], raw)

    key = station.get("key")
    if not isinstance(key, str) or key == "":
        raise ConfigError(f"stations[{index}].key: required non-empty string")
    if _STATION_KEY_RE.match(key) is None:
        raise ConfigError(
            f"stations[{index}].key: {key!r} must be lowercase alphanumeric "
            "(^[a-z0-9]+$)"
        )

    update_entity = station.get("update_entity")
    if not isinstance(update_entity, str) or update_entity == "":
        raise ConfigError(f"stations[{index}].update_entity: required non-empty string")
    # `key` is allowlisted above, so it is regex-literal here; the anchored shape
    # ties the metric segment + trailing key to this station.
    matcher = re.compile(r"^sensor\.wu_[a-z0-9]+_" + key + r"$")
    if matcher.match(update_entity) is None:
        raise ConfigError(
            f"stations[{index}].update_entity: {update_entity!r} must match "
            f"sensor.wu_<metric>_{key} (not the registry sensor.rest_wu_* form, "
            "and the trailing key must match this station's key)"
        )

    return Station(key=key, update_entity=update_entity)


def validate(options: dict[str, object]) -> Config:
    """Validate an already-parsed options dict into a `Config`.

    Pure with respect to its argument (no I/O, no logging). Raises `ConfigError`
    naming the field on any bad type, out-of-range value, or contract breach.
    """
    max_backoff_seconds = _range_int(
        options, "max_backoff_seconds", 86400, _MIN_CADENCE, _MAX_CADENCE
    )
    # The high bound is the fixed 1800s healthy-slow-uploader ceiling
    # (== cadence.MAX, the clamp ceiling), NOT _MAX_CADENCE: the learned interval
    # is clamp(period * FACTOR, min_interval_seconds, cadence.MAX), so capping the
    # floor at 1800 guarantees low <= high at the clamp call site (a floor above
    # the ceiling would invert the clamp and void the ceiling). Hard-coded rather
    # than importing cadence.MAX to keep config.py importing only from .models.
    min_interval_seconds = _range_int(
        options, "min_interval_seconds", 300, _MIN_CADENCE, 1800
    )
    settle_seconds = _range_int(options, "settle_seconds", 15, _MIN_SHORT, _MAX_SHORT)
    startup_stagger_seconds = _range_int(
        options, "startup_stagger_seconds", 10, _MIN_SHORT, _MAX_SHORT
    )
    request_timeout_seconds = _range_int(
        options, "request_timeout_seconds", 30, _MIN_SHORT, _MAX_SHORT
    )

    log_level = _require_str(options, "log_level", "info")
    if log_level not in _VALID_LOG_LEVELS:
        raise ConfigError(f"log_level: must be one of {', '.join(_VALID_LOG_LEVELS)}")

    raw_stations = options.get("stations")
    if raw_stations is None:
        raise ConfigError("stations: required (a list, possibly empty)")
    if not isinstance(raw_stations, list):
        raise ConfigError("stations: must be a list")
    stations_list = cast(list[object], raw_stations)

    stations: list[Station] = []
    seen_keys: set[str] = set()
    for index, raw in enumerate(stations_list):
        station = _validate_station(raw, index)
        if station.key in seen_keys:
            raise ConfigError(f"stations[{index}].key: duplicate key {station.key!r}")
        seen_keys.add(station.key)
        stations.append(station)

    return Config(
        max_backoff_seconds=max_backoff_seconds,
        min_interval_seconds=min_interval_seconds,
        settle_seconds=settle_seconds,
        startup_stagger_seconds=startup_stagger_seconds,
        request_timeout_seconds=request_timeout_seconds,
        log_level=log_level,
        stations=tuple(stations),
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
