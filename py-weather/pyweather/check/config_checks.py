# pyright: strict
"""Config-surface checks: invalid-options rejection, entity-id + station-key contracts.

All cases here are **pure** — driven entirely from static options dicts with **no
fixture ``/states`` payload** (using ``/states`` here would re-introduce
I/O-in-validation coupling). The runtime presence/floor enforcement lives in the
health/scheduler checks instead.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from .. import config, fixtures
from ..config import ConfigError
from .report import report


def check_valid_defaults() -> bool:
    """Assert the default options payload validates to the expected `Config`.

    Pins the eight default stations (key + representative entity-id), the default
    cadence/timeout values, and that ``temp`` is the required-core representative
    target for every station.
    """
    cfg = config.validate(fixtures.default_options())
    checks: list[tuple[str, bool]] = [
        ("default options validate", True),
        ("eight default stations", len(cfg.stations) == 8),
        (
            "every station targets sensor.wu_temp_<key>",
            all(s.update_entity == f"sensor.wu_temp_{s.key}" for s in cfg.stations),
        ),
        (
            "default station keys match the synthetic placeholder ids",
            tuple(s.key for s in cfg.stations) == fixtures.DEFAULT_STATION_KEYS,
        ),
        (
            "default cadence/timeout values",
            cfg.healthy_interval_min == 300
            and cfg.healthy_interval_max == 400
            and cfg.initial_backoff_seconds == 300
            and cfg.max_backoff_seconds == 86400
            and cfg.settle_seconds == 15
            and cfg.startup_stagger_seconds == 10
            and cfg.request_timeout_seconds == 30,
        ),
        (
            "every default station expects 10 sensors",
            all(s.expected_sensors == 10 for s in cfg.stations),
        ),
    ]
    return report("VALID-DEFAULTS", "valid-defaults", checks)


def check_invalid_options() -> bool:
    """Assert every `INVALID_OPTIONS` payload is rejected, naming the field.

    Two layers (mirroring py-ddns):

    1. **Field validation** — each payload through `config.validate` raises a
       `ConfigError` whose message contains the expected field token (range
       floors/ceilings, cross-field relations, the log-level enum, the stations
       list shape, the station-key allowlist, and the anchored ``update_entity``
       shape).
    2. **File loading** — `config.load` rejects malformed JSON, a non-object
       top-level value, and a missing path, each naming the cause.
    """
    checks: list[tuple[str, bool]] = []
    for fixture in fixtures.INVALID_OPTIONS:
        try:
            config.validate(fixture.options)
        except ConfigError as exc:
            passed = fixture.field in str(exc)
            checks.append(
                (f"[{fixture.name}] rejected naming {fixture.field!r}", passed)
            )
            if not passed:
                print(
                    f"  (got {str(exc)!r}, expected to name {fixture.field!r})",
                    file=sys.stderr,
                )
        else:
            checks.append((f"[{fixture.name}] raised ConfigError", False))
    ok = report("INVALID-OPTIONS", "invalid-options", checks)
    return _check_load_negatives() and ok


def _check_load_negatives() -> bool:
    """Assert `config.load` rejects bad files with a cause-naming `ConfigError`."""
    checks: list[tuple[str, bool]] = []

    def _assert_load_error(name: str, content: str, cause: str) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            opts = Path(tmp) / "options.json"
            opts.write_text(content, encoding="utf-8")
            try:
                config.load(str(opts))
            except ConfigError as exc:
                checks.append((f"load [{name}] names {cause!r}", cause in str(exc)))
            else:
                checks.append((f"load [{name}] raised ConfigError", False))

    _assert_load_error("malformed JSON", "{ not json", "invalid JSON")
    _assert_load_error(
        "top-level array", '["a", "b"]', "top-level value must be an object"
    )
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "does-not-exist.json"
        try:
            config.load(str(missing))
        except ConfigError as exc:
            checks.append(
                ("load [missing path] names 'cannot read'", "cannot read" in str(exc))
            )
        else:
            checks.append(("load [missing path] raised ConfigError", False))
    return report("LOAD-NEGATIVES", "load-negative", checks)


def _rejects(options: dict[str, object]) -> bool:
    """True iff `config.validate` rejects `options` with a `ConfigError`."""
    try:
        config.validate(options)
    except ConfigError:
        return True
    return False


def _accepts(options: dict[str, object]) -> bool:
    """True iff `config.validate` accepts `options`."""
    try:
        config.validate(options)
    except ConfigError:
        return False
    return True


def _one_station(key: str, update_entity: str) -> dict[str, object]:
    """A default-options payload with a single station (for shape/key oracles)."""
    return fixtures.default_options(
        stations=[{"key": key, "update_entity": update_entity, "expected_sensors": 10}]
    )


def check_entity_shape() -> bool:
    """Pure representative entity-ID shape validation (no ``/states``).

    For station ``istation01``: rejects the registry form
    ``sensor.rest_wu_temp_istation01``, rejects the mismatched-key
    ``sensor.wu_temp_istation06``, and accepts the correct ``sensor.wu_temp_istation01``.
    """
    checks: list[tuple[str, bool]] = [
        (
            "registry sensor.rest_wu_* form rejected",
            _rejects(_one_station("istation01", "sensor.rest_wu_temp_istation01")),
        ),
        (
            "mismatched-key sensor.wu_temp_istation06 rejected for key istation01",
            _rejects(_one_station("istation01", "sensor.wu_temp_istation06")),
        ),
        (
            "correct sensor.wu_temp_istation01 accepted",
            _accepts(_one_station("istation01", "sensor.wu_temp_istation01")),
        ),
    ]
    return report("ENTITY-SHAPE", "entity-shape", checks)


def check_station_key_contract() -> bool:
    """Pure station-key contract validation (no ``/states``).

    Rejects an empty ``stations`` list; rejects the invalid keys ``istation_01``
    (underscore), ``ISTATION01`` (uppercase), and ``.*`` (regex metacharacters);
    accepts the valid keys ``istation01`` and ``istation08``. For the ``.*`` case,
    additionally proves the key is regex-literal: a wrong-suffix ``update_entity``
    under key ``.*`` is still rejected (the key is not treated as a wildcard).
    """
    checks: list[tuple[str, bool]] = [
        (
            "empty stations list rejected",
            _rejects(fixtures.default_options(stations=[])),
        ),
        (
            "key 'istation_01' (underscore) rejected",
            _rejects(_one_station("istation_01", "sensor.wu_temp_istation_01")),
        ),
        (
            "key 'ISTATION01' (uppercase) rejected",
            _rejects(_one_station("ISTATION01", "sensor.wu_temp_ISTATION01")),
        ),
        (
            "key '.*' (regex metacharacters) rejected",
            _rejects(_one_station(".*", "sensor.wu_temp_istation01")),
        ),
        (
            "valid key 'istation01' accepted",
            _accepts(_one_station("istation01", "sensor.wu_temp_istation01")),
        ),
        (
            "valid key 'istation08' accepted",
            _accepts(_one_station("istation08", "sensor.wu_temp_istation08")),
        ),
        # If `.*` were treated as a wildcard rather than regex-literal, a
        # wrong-suffix update_entity would slip through the matcher. The key
        # allowlist rejects `.*` outright, so this stays rejected — proving the
        # key cannot act as a wildcard against the entity matcher.
        (
            "key '.*' does not act as a wildcard (wrong-suffix entity still rejected)",
            _rejects(_one_station(".*", "sensor.wu_temp_anything")),
        ),
    ]
    return report("STATION-KEY", "station-key", checks)
