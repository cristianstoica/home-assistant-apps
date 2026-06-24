# pyright: strict
"""Built-in self-validation corpus for ``--check`` (the regression oracle).

This declares **expected** values and fixture payloads rather than recomputing
them, so it catches drift in the validator / health / cadence / classification
logic the way a pytest suite would. The check modules drive the production seams
against these fixtures and assert the produced value equals the declared one.

Corpora here:

* `default_options` — the valid default options payload (mirrors ``config.yaml``).
* `INVALID_OPTIONS` — payloads `config.validate` must reject by naming a field.
* the ``/states`` builders (`station_states` / `obstime_states`) — synthetic
  ``/states`` arrays for the runtime health oracle (entity-id shape
  ``sensor.wu_<metric>_<key>``).

All values are synthetic. The eight default station keys are synthetic
placeholders (``istation01`` … ``istation08``) carrying no real PWS id, so the
default-options oracle exercises the real key/entity-id shape without leaking any
real station; no secrets are involved (py-weather holds none — the bearer is the
Supervisor's, injected at runtime).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple

# A non-secret placeholder bearer the HA-request-shaping oracle asserts is sent
# verbatim in the Authorization header. Not a real token.
EXAMPLE_TOKEN = "EXAMPLE-supervisor-token-0000"

# The eight default (synthetic, placeholder) stations: key -> representative
# entity-id, all with the `sensor.wu_temp_<key>` refresh POST target and
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
    """A valid default options payload.

    Cadence/timeout/log-level values mirror the ``config.yaml`` defaults; ``stations``
    is a synthetic eight-station fleet (NOT the manifest's ``stations: []`` first-run
    placeholder) so the validator/health oracles exercise the real key/entity-id shape.
    Override any top-level key via ``**overrides`` (e.g. ``stations=[]`` for the
    auto-populate-trigger case).
    """
    base: dict[str, Any] = {
        "max_backoff_seconds": 86400,
        "min_interval_seconds": 300,
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
        name="max_backoff_seconds below 60 (initial in range)",
        options=default_options(max_backoff_seconds=59),
        field="max_backoff_seconds",
    ),
    # --- min_interval_seconds floor/ceiling ----------------------------------
    InvalidOptionsFixture(
        name="min_interval_seconds below 60",
        options=default_options(min_interval_seconds=30),
        field="min_interval_seconds",
    ),
    InvalidOptionsFixture(
        name="min_interval_seconds above the 1800 ceiling",
        options=default_options(min_interval_seconds=2000),
        field="min_interval_seconds",
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


# A fixed wall-clock instant the scheduler oracle injects via its FakeWallClock.
T0_ISO = "2026-06-21T12:00:00+00:00"


# --- cadence obstime series ---------------------------------------------------
# A fixed cadence-window epoch the obstime builders count forward from. Z-form
# (UTC) ISO-8601, the shape WU serves in `obsTimeUtc` and `cadence.parse_obstime`
# accepts. `OBSTIME_T0` alone is the single-event (no-measurable-gap) fixture.
_OBSTIME_EPOCH = datetime(2026, 6, 21, 0, 0, 0, tzinfo=timezone.utc)
OBSTIME_T0 = _OBSTIME_EPOCH.strftime("%Y-%m-%dT%H:%M:%SZ")

# A representative obstime string in the two health-relevant shapes: a parseable
# Z-form (online) and an offset-less naive form (unparseable ⇒ offline).
OBSTIME_NAIVE = "2026-06-23T19:27:26"  # offset-less (naive) — unparseable


def obstime_series(gap_seconds: int, count: int) -> tuple[str, ...]:
    """`count` evenly-spaced obsTimeUtc strings, each `gap_seconds` after the last.

    Newest last (the `StationCadence.events` ordering), so the consecutive deltas
    `cadence.gaps` derives are all exactly `gap_seconds`.
    """
    return tuple(
        (_OBSTIME_EPOCH + timedelta(seconds=gap_seconds * i)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        for i in range(count)
    )


def obstime_irregular(inter_gaps: list[int]) -> tuple[str, ...]:
    """obsTimeUtc strings whose consecutive deltas are exactly `inter_gaps`.

    Produces ``len(inter_gaps) + 1`` events (newest last): the cumulative sum of
    `inter_gaps` from `_OBSTIME_EPOCH`, so `cadence.gaps` recovers `inter_gaps`
    verbatim (used to assert the median ignores a single bursty gap).
    """
    offset = 0
    out = [_OBSTIME_EPOCH.strftime("%Y-%m-%dT%H:%M:%SZ")]
    for gap in inter_gaps:
        offset += gap
        out.append(
            (_OBSTIME_EPOCH + timedelta(seconds=offset)).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
    return tuple(out)


def state_obj(entity_id: str, state: str) -> dict[str, Any]:
    """Build one ``/states`` entity dict (entity_id + state)."""
    return {"entity_id": entity_id, "state": state}


class _Omit:
    """Sentinel: an obstime sensor omitted entirely from the payload."""


_OMIT = _Omit()
OMIT = _OMIT


def station_states(
    key: str,
    *,
    temp_state: str = "12.3",
    humidity_state: str = "60",
    pressure_state: str = "1013",
    uv_state: str | None = "2",
    include_uv: bool = True,
    extra: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build a station's ``/states`` array (temp/humidity/pressure + optional uv).

    `include_uv=False` drops the optional ``uv`` sensor entirely (absence);
    `uv_state` sets its value (e.g. ``"unavailable"`` for the
    present-but-unavailable optional case).
    """
    states: list[dict[str, Any]] = [
        state_obj(f"sensor.wu_obstimeutc_{key}", "2026-06-23T12:00:00+00:00"),
        state_obj(f"sensor.wu_temp_{key}", temp_state),
        state_obj(f"sensor.wu_humidity_{key}", humidity_state),
        state_obj(f"sensor.wu_pressure_{key}", pressure_state),
    ]
    if include_uv and uv_state is not None:
        states.append(state_obj(f"sensor.wu_uv_{key}", uv_state))
    if extra:
        states.extend(extra)
    return states


def obstime_states(
    key: str,
    *,
    obstime: Any = _OMIT,  # _OMIT = sensor absent; a string = its state
    obstime_state_override: str | None = None,  # for "unavailable" etc.
) -> list[dict[str, Any]]:
    """A station /states array whose representative is sensor.wu_obstimeutc_<key>.

    `obstime` sets the obstime sensor's state to an ISO-8601 string; _OMIT
    drops the sensor entirely (offline); `obstime_state_override` forces a
    non-timestamp state like 'unavailable'.
    """
    states: list[dict[str, Any]] = [
        state_obj(f"sensor.wu_temp_{key}", "12.3"),
        state_obj(f"sensor.wu_humidity_{key}", "60"),
    ]
    if obstime_state_override is not None:
        states.append(state_obj(f"sensor.wu_obstimeutc_{key}", obstime_state_override))
    elif obstime is not _OMIT:
        states.append(state_obj(f"sensor.wu_obstimeutc_{key}", str(obstime)))
    return states
