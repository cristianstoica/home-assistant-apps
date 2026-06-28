# pyright: strict
"""Pure station-health evaluation over a ``/states`` projection.

Given a station and the list of `EntityState`s discovered for it, this decides
a **binary** data-presence `HealthStatus` (online / offline — the API-client
``TERMINAL`` fault is raised there, never produced here). Pure: no I/O, no
clock. The scheduler owns the poll loop; this module judges a single snapshot.

The data-presence contract (the single canonical definition; the scheduler
references it, never restates it):

* **Discovery** is by entity-id shape ``sensor.wu_<metric>_<key>``. The
  representative sensor is ``sensor.wu_obstimeutc_<key>`` — its ``state`` carries
  the WU ``obsTimeUtc`` ISO-8601 string.
* **Online iff** the representative is present, usable, and a parseable
  timestamp. A WU 204 collapses the whole REST resource, so obstime presence
  alone captures online vs offline.
* **No freshness, no required-core gate** (both removed in v0.3.0). A
  present-but-frozen obstime is still online; individual sibling metrics are not
  inspected at all — there is no per-metric count or shortfall signal, only the
  binary obstime-presence verdict.
"""

from __future__ import annotations

from .cadence import parse_obstime
from .models import EntityState, HealthResult, HealthStatus, Station

_UNUSABLE_STATES = ("unavailable", "unknown", "none", "")


def _is_usable(state: str) -> bool:
    """True iff `state` is not one of the unusable sentinels (case-insensitive)."""
    return state.strip().lower() not in _UNUSABLE_STATES


def discover(states: list[EntityState], key: str) -> dict[str, EntityState]:
    """Return ``{metric: EntityState}`` for entities matching ``sensor.wu_*_<key>``.

    The metric is the segment between ``sensor.wu_`` and the trailing
    ``_<key>``. A non-matching or malformed entity-id is skipped. The `key` is
    config-allowlisted (``^[a-z0-9]+$``) so the suffix match is unambiguous.
    """
    prefix = "sensor.wu_"
    suffix = "_" + key
    found: dict[str, EntityState] = {}
    for entity in states:
        eid = entity.entity_id
        if not eid.startswith(prefix) or not eid.endswith(suffix):
            continue
        metric = eid[len(prefix) : len(eid) - len(suffix)]
        if metric == "":
            continue
        found[metric] = entity
    return found


def evaluate(station: Station, states: list[EntityState]) -> HealthResult:
    """Binary data-presence health for one /states snapshot.

    ONLINE  — sensor.wu_obstimeutc_<key> present and a parseable timestamp.
    OFFLINE — absent / unavailable / unparseable (a WU 204 collapses the whole
              REST resource, so obstime presence alone captures online/offline).

    No t0, no freshness, no required-core gate (all removed in v0.3.0).
    Individual sibling metrics are not inspected at all — there is no
    per-metric count or shortfall signal, only the binary obstime-presence
    verdict. The TERMINAL classification is the API client's, never produced
    here.
    """
    discovered = discover(states, station.key)
    representative = discovered.get("obstimeutc")
    if representative is None or not _is_usable(representative.state):
        return HealthResult(
            HealthStatus.OFFLINE,
            f"{station.key}: obstimeutc sensor absent/unavailable (offline)",
        )
    if parse_obstime(representative.state) is None:
        return HealthResult(
            HealthStatus.OFFLINE,
            f"{station.key}: obstimeutc unparseable (offline)",
        )
    return HealthResult(
        HealthStatus.ONLINE,
        f"{station.key}: obstimeutc present (online)",
    )
