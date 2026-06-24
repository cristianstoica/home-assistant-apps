# pyright: strict
"""Binary obstime-presence health checks, driven from fixture ``/states`` payloads.

These exercise the pure `health.evaluate` over fixture ``/states`` arrays: a
present + parseable ``obstimeutc`` reads ONLINE (including a frozen-but-present
value — the false-unhealthy trap is gone); absent / ``unavailable`` / naive /
garbage reads OFFLINE; and a missing optional metric on an otherwise-online
station stays ONLINE (no required-core gate).
"""

from __future__ import annotations

from typing import Any

from .. import fixtures
from ..config import validate
from ..health import evaluate
from ..models import EntityState, HealthStatus, Station
from .report import report


def _station(key: str = "istation01", expected: int = 10) -> Station:
    """A single validated `Station` for `key` with `expected` sensors."""
    cfg = validate(
        fixtures.default_options(
            stations=[
                {
                    "key": key,
                    "update_entity": f"sensor.wu_temp_{key}",
                    "expected_sensors": expected,
                }
            ]
        )
    )
    return cfg.stations[0]


def _to_entity_states(raw: list[dict[str, Any]]) -> list[EntityState]:
    """Project raw ``/states`` dicts to `EntityState`s (the GET /states parse).

    Driving this locally keeps the health oracle pure (no HTTP seam) while
    matching the production projection (entity_id + state only).
    """
    out: list[EntityState] = []
    for obj in raw:
        out.append(
            EntityState(
                entity_id=str(obj["entity_id"]),
                state=str(obj["state"]),
            )
        )
    return out


def _evaluate(
    raw: list[dict[str, Any]], station: Station | None = None
) -> HealthStatus:
    return evaluate(station or _station(), _to_entity_states(raw)).status


def check_health() -> bool:
    """Binary obstime-presence health: present+parseable ⇒ ONLINE; absent /
    unavailable / naive / garbage ⇒ OFFLINE; a frozen-but-present obstime is
    still ONLINE (the false-unhealthy trap is gone)."""
    checks: list[tuple[str, bool]] = []
    # present + parseable ⇒ ONLINE.
    checks.append(
        (
            "obstimeutc present+parseable (Z form) ⇒ ONLINE",
            _evaluate(
                fixtures.obstime_states("istation01", obstime=fixtures.OBSTIME_T0)
            )
            is HealthStatus.ONLINE,
        )
    )
    # absent ⇒ OFFLINE.
    checks.append(
        (
            "obstimeutc sensor absent ⇒ OFFLINE",
            _evaluate(fixtures.obstime_states("istation01"))  # _OMIT
            is HealthStatus.OFFLINE,
        )
    )
    # unavailable ⇒ OFFLINE.
    checks.append(
        (
            "obstimeutc state 'unavailable' ⇒ OFFLINE",
            _evaluate(
                fixtures.obstime_states(
                    "istation01", obstime_state_override="unavailable"
                )
            )
            is HealthStatus.OFFLINE,
        )
    )
    # naive/unparseable ⇒ OFFLINE.
    checks.append(
        (
            "obstimeutc naive (offset-less) ⇒ OFFLINE",
            _evaluate(
                fixtures.obstime_states("istation01", obstime=fixtures.OBSTIME_NAIVE)
            )
            is HealthStatus.OFFLINE,
        )
    )
    checks.append(
        (
            "obstimeutc garbage string ⇒ OFFLINE",
            _evaluate(fixtures.obstime_states("istation01", obstime="not-a-timestamp"))
            is HealthStatus.OFFLINE,
        )
    )
    # frozen-but-present (same value, no advance) ⇒ STILL ONLINE (the bug fix).
    checks.append(
        (
            "frozen-but-present obstimeutc ⇒ ONLINE (freshness decoupled from health)",
            _evaluate(
                fixtures.obstime_states("istation01", obstime=fixtures.OBSTIME_T0)
            )
            is HealthStatus.ONLINE,
        )
    )
    # missing optional metric on an online station ⇒ still ONLINE.
    online_no_humidity = [
        fixtures.state_obj("sensor.wu_temp_istation01", "12.3"),
        fixtures.state_obj("sensor.wu_obstimeutc_istation01", fixtures.OBSTIME_T0),
    ]
    checks.append(
        (
            "missing metric on an online station ⇒ still ONLINE (no required-core gate)",
            _evaluate(online_no_humidity) is HealthStatus.ONLINE,
        )
    )
    return report("HEALTH", "health", checks)
