# pyright: strict
"""Built-in self-validation corpus for ``--check`` (the regression oracle).

This declares **expected** values and fixture payloads rather than recomputing
them, so it catches drift in the validator / freshness / health / classification
logic the way a pytest suite would. The check modules drive the production seams
against these fixtures and assert the produced value equals the declared one.

Corpora here:

* `default_options` — the valid default options payload (mirrors ``config.yaml``).
* `INVALID_OPTIONS` — payloads `config.validate` must reject by naming a field.
* `make_states` / the ``/states`` builders — synthetic ``/states`` arrays for the
  runtime health/freshness oracles (entity-id shape ``sensor.wu_<metric>_<key>``).

All values are synthetic. The eight default station keys are synthetic
placeholders (``istation01`` … ``istation08``) carrying no real PWS id, so the
default-options oracle exercises the real key/entity-id shape without leaking any
real station; no secrets are involved (py-weather holds none — the bearer is the
Supervisor's, injected at runtime).
"""

from __future__ import annotations

from typing import Any, NamedTuple

# A non-secret placeholder bearer the HA-request-shaping oracle asserts is sent
# verbatim in the Authorization header. Not a real token.
EXAMPLE_TOKEN = "EXAMPLE-supervisor-token-0000"

# The eight default (synthetic, placeholder) stations: key -> representative
# entity-id, all with the `temp` required-core representative target and
# expected_sensors 10.
DEFAULT_STATION_KEYS = (
    "istation01",
    "istation02",
    "istation03",
    "istation04",
    "istation05",
    "istation06",
    "istation07",
    "istation08",
)


def default_stations() -> list[dict[str, Any]]:
    """The eight default station option objects (key/update_entity/expected_sensors)."""
    return [
        {
            "key": key,
            "update_entity": f"sensor.wu_temp_{key}",
            "expected_sensors": 10,
        }
        for key in DEFAULT_STATION_KEYS
    ]


def default_options(**overrides: Any) -> dict[str, Any]:
    """A valid default options payload (mirrors ``config.yaml`` defaults).

    Override individual top-level keys via ``**overrides`` (e.g. a bad range or a
    replaced ``stations`` list).
    """
    base: dict[str, Any] = {
        "healthy_interval_min": 300,
        "healthy_interval_max": 400,
        "initial_backoff_seconds": 300,
        "max_backoff_seconds": 86400,
        "settle_seconds": 15,
        "startup_stagger_seconds": 10,
        "request_timeout_seconds": 30,
        "log_level": "info",
        "stations": default_stations(),
    }
    base.update(overrides)
    return base


class InvalidOptionsFixture(NamedTuple):
    """An options payload `config.validate` must reject by naming `field`."""

    name: str
    options: dict[str, Any]
    field: str


INVALID_OPTIONS: list[InvalidOptionsFixture] = [
    # --- cadence range floors -------------------------------------------------
    InvalidOptionsFixture(
        name="healthy_interval_min below 60",
        options=default_options(healthy_interval_min=30),
        field="healthy_interval_min",
    ),
    InvalidOptionsFixture(
        name="initial_backoff_seconds below 60",
        options=default_options(initial_backoff_seconds=30),
        field="initial_backoff_seconds",
    ),
    InvalidOptionsFixture(
        name="max_backoff_seconds below 60",
        options=default_options(max_backoff_seconds=30, initial_backoff_seconds=30),
        # initial<60 is checked first; pin the field the validator names.
        field="initial_backoff_seconds",
    ),
    InvalidOptionsFixture(
        name="max_backoff_seconds alone below 60",
        options=default_options(max_backoff_seconds=59, initial_backoff_seconds=59),
        field="initial_backoff_seconds",
    ),
    InvalidOptionsFixture(
        # initial_backoff_seconds is at its default (300, in range) so the
        # initial range check (lines 151-153 of config.py) passes; the
        # max_backoff_seconds range check (lines 154-156) fires independently.
        name="max_backoff_seconds below 60 (initial in range)",
        options=default_options(max_backoff_seconds=59),
        field="max_backoff_seconds",
    ),
    # --- cross-field range relations -----------------------------------------
    InvalidOptionsFixture(
        name="healthy_interval_min > healthy_interval_max",
        options=default_options(healthy_interval_min=400, healthy_interval_max=300),
        field="healthy_interval_min",
    ),
    InvalidOptionsFixture(
        name="max_backoff_seconds < initial_backoff_seconds",
        options=default_options(initial_backoff_seconds=600, max_backoff_seconds=300),
        field="max_backoff_seconds",
    ),
    # --- timeout upper bound --------------------------------------------------
    InvalidOptionsFixture(
        name="request_timeout_seconds above 300",
        options=default_options(request_timeout_seconds=301),
        field="request_timeout_seconds",
    ),
    # --- log level enum -------------------------------------------------------
    InvalidOptionsFixture(
        name="bad log_level",
        options=default_options(log_level="trace"),
        field="log_level",
    ),
    # --- stations list shape --------------------------------------------------
    InvalidOptionsFixture(
        name="empty stations list",
        options=default_options(stations=[]),
        field="stations",
    ),
    InvalidOptionsFixture(
        name="duplicate station keys",
        options=default_options(
            stations=[
                {
                    "key": "istation01",
                    "update_entity": "sensor.wu_temp_istation01",
                    "expected_sensors": 10,
                },
                {
                    "key": "istation01",
                    "update_entity": "sensor.wu_temp_istation01",
                    "expected_sensors": 10,
                },
            ]
        ),
        field="duplicate",
    ),
    # --- station key allowlist ------------------------------------------------
    InvalidOptionsFixture(
        name="station key with underscore",
        options=default_options(
            stations=[
                {
                    "key": "istation_01",
                    "update_entity": "sensor.wu_temp_istation_01",
                    "expected_sensors": 10,
                },
            ]
        ),
        field="key",
    ),
    InvalidOptionsFixture(
        name="station key uppercase",
        options=default_options(
            stations=[
                {
                    "key": "ISTATION01",
                    "update_entity": "sensor.wu_temp_ISTATION01",
                    "expected_sensors": 10,
                },
            ]
        ),
        field="key",
    ),
    InvalidOptionsFixture(
        name="station key regex metacharacters",
        options=default_options(
            stations=[
                {
                    "key": ".*",
                    "update_entity": "sensor.wu_temp_istation01",
                    "expected_sensors": 10,
                },
            ]
        ),
        field="key",
    ),
    # --- representative entity-id shape --------------------------------------
    InvalidOptionsFixture(
        name="registry rest_ form rejected",
        options=default_options(
            stations=[
                {
                    "key": "istation01",
                    "update_entity": "sensor.rest_wu_temp_istation01",
                    "expected_sensors": 10,
                },
            ]
        ),
        field="update_entity",
    ),
    InvalidOptionsFixture(
        name="mismatched key in update_entity",
        options=default_options(
            stations=[
                {
                    "key": "istation01",
                    "update_entity": "sensor.wu_temp_istation06",
                    "expected_sensors": 10,
                },
            ]
        ),
        field="update_entity",
    ),
    InvalidOptionsFixture(
        name="bare sensor. update_entity (empty metric)",
        options=default_options(
            stations=[
                {
                    "key": "istation01",
                    "update_entity": "sensor.",
                    "expected_sensors": 10,
                },
            ]
        ),
        field="update_entity",
    ),
    # --- expected_sensors -----------------------------------------------------
    InvalidOptionsFixture(
        name="expected_sensors not positive",
        options=default_options(
            stations=[
                {
                    "key": "istation01",
                    "update_entity": "sensor.wu_temp_istation01",
                    "expected_sensors": 0,
                },
            ]
        ),
        field="expected_sensors",
    ),
]


# A fixed pre-POST t0 the freshness oracle compares against. Timestamps "before"
# t0 are stale (not advanced); timestamps "after" t0 are fresh (advanced).
T0_ISO = "2026-06-21T12:00:00+00:00"
STALE_ISO = "2026-06-21T11:59:00+00:00"  # one minute before t0
FRESH_ISO = "2026-06-21T12:00:30+00:00"  # 30s after t0
FRESH_ISO_Z = "2026-06-21T12:00:30Z"  # Z-form of an after-t0 instant
NAIVE_ISO = "2026-06-21T12:00:30"  # offset-less (naive) — unparseable for freshness


class StateFixture(NamedTuple):
    """A raw ``/states`` entity object (the dict the GET /states array carries)."""

    entity_id: str
    state: str
    last_reported: Any
    last_updated: Any
    last_changed: Any


def state_obj(
    entity_id: str,
    state: str,
    *,
    last_reported: Any = None,
    last_updated: Any = None,
    last_changed: Any = None,
) -> dict[str, Any]:
    """Build one ``/states`` entity dict (omitting unset timestamp keys).

    A key set to the sentinel ``None`` is **omitted** entirely (absent from the
    payload), distinct from being present-but-JSON-``null``. To assert the
    present-but-null path, pass ``last_reported=NULL`` (the explicit JSON-null
    marker below).
    """
    obj: dict[str, Any] = {"entity_id": entity_id, "state": state}
    if last_reported is not _OMIT:
        obj["last_reported"] = last_reported
    if last_updated is not _OMIT:
        obj["last_updated"] = last_updated
    if last_changed is not _OMIT:
        obj["last_changed"] = last_changed
    return obj


class _Omit:
    """Sentinel: a timestamp key omitted entirely from the payload."""


_OMIT = _Omit()
OMIT = _OMIT
# JSON null marker: a key present with a null value (distinct from omitted).
NULL = None


def station_states(
    key: str,
    *,
    temp_state: str = "12.3",
    temp_last_reported: Any = _OMIT,
    temp_last_updated: Any = _OMIT,
    temp_last_changed: Any = _OMIT,
    humidity_state: str = "60",
    pressure_state: str = "1013",
    uv_state: str | None = "2",
    include_uv: bool = True,
    extra: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build a station's ``/states`` array (temp/humidity/pressure + optional uv).

    The representative ``temp`` sensor's timestamp fields are controllable so the
    freshness paths can be exercised; the other required-core sensors carry no
    timestamps (they are only state-usability checked). `include_uv=False` drops
    the optional ``uv`` sensor entirely (absence); `uv_state` sets its value (e.g.
    ``"unavailable"`` for the present-but-unavailable optional case).
    """
    states: list[dict[str, Any]] = [
        state_obj(
            f"sensor.wu_temp_{key}",
            temp_state,
            last_reported=temp_last_reported,
            last_updated=temp_last_updated,
            last_changed=temp_last_changed,
        ),
        state_obj(f"sensor.wu_humidity_{key}", humidity_state),
        state_obj(f"sensor.wu_pressure_{key}", pressure_state),
    ]
    if include_uv and uv_state is not None:
        states.append(state_obj(f"sensor.wu_uv_{key}", uv_state))
    if extra:
        states.extend(extra)
    return states
