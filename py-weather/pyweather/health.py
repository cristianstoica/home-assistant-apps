# pyright: strict
"""Pure station-health and freshness evaluation over a ``/states`` projection.

Given a station, the list of `EntityState`s discovered for it, and the pre-POST
``t0``, this decides the four-way `HealthStatus` (confirmed / inconclusive /
unhealthy / terminal — though terminal is raised by the API client, never
produced here). Pure: no I/O, no clock — the wall-clock ``t0`` is passed in. The
scheduler owns the re-read loop; this module judges a single snapshot.

The freshness contract (the single canonical definition; the scheduler and the
health predicate reference it, never restate it):

* **Discovery** is by entity-id shape ``sensor.wu_<metric>_<key>``. The
  representative sensor is ``sensor.wu_temp_<key>``.
* **Required-core floor** — ``temp``/``humidity``/``pressure`` must each be
  present-and-usable. An individually-unavailable or absent **optional** metric
  (e.g. ``uv``) is non-fatal; falling short of ``expected_sensors`` on optional
  metrics is the scheduler's soft-logged signal, never a health gate here.
* **Freshness — primary path** (representative ``last_reported`` present and
  non-null/non-empty, HA 2024.8+): refreshed iff ``last_reported`` is strictly
  later than ``t0``. ``last_reported`` advances on every state write (including an
  identical-value write), so an unchanged temperature still counts fresh. A
  non-advancing ``last_reported`` is a genuine failed-freshness signal
  (unhealthy). A **naive/unparseable** non-null ``last_reported`` is treated as a
  failed freshness check (unhealthy) — a malformed non-null timestamp is still a
  real signal, never silently healthy.
* **Freshness — degrade-safe fallback** (representative ``last_reported`` absent
  or present-but-``null``/empty, older Core): fall back to ``last_updated`` (or
  ``last_changed`` if ``last_updated`` is absent). An **advanced** fallback
  timestamp is a positive confirmation (CONFIRMED). An **unchanged** fallback
  timestamp on an otherwise-successful poll (representative present, required-core
  usable) is **inconclusive-but-not-unhealthy** (INCONCLUSIVE): accepted, not
  backed off, but not rewarded with the fast cadence.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .config import REQUIRED_CORE_METRICS
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


def _parse_ha_timestamp(value: str) -> datetime | None:
    """Parse an HA ISO-8601 timestamp to a tz-aware UTC datetime, else ``None``.

    Handles HA's ``+00:00`` and ``Z`` offset forms (``datetime.fromisoformat`` on
    Python 3.11+ accepts both). A **naive** (offset-less) or unparseable value
    returns ``None`` — on the primary path the caller treats that as a failed
    freshness check (a malformed non-null timestamp is still a real signal).
    """
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _advanced_past(value: str | None, t0: datetime) -> bool | None:
    """Three-way: ``True`` advanced, ``False`` not advanced, ``None`` unparseable.

    ``None`` input (absent/null) returns ``None`` (no signal). A present string
    that is naive/unparseable also returns ``None`` (the caller decides whether
    that is a primary-path failure or a fallback miss). A parseable timestamp
    returns whether it is strictly later than `t0`.
    """
    if value is None:
        return None
    parsed = _parse_ha_timestamp(value)
    if parsed is None:
        return None
    return parsed > t0


def evaluate(station: Station, states: list[EntityState], t0: datetime) -> HealthResult:
    """Evaluate one ``/states`` snapshot for `station` into a `HealthResult`.

    Order of judgment:

    1. Discover ``sensor.wu_*_<key>`` sensors. The representative is
       ``sensor.wu_temp_<key>``; absent ⇒ UNHEALTHY (transient: re-poll).
    2. Required-core floor: any of ``temp``/``humidity``/``pressure`` absent or
       unusable ⇒ UNHEALTHY.
    3. Freshness: primary ``last_reported`` path vs degrade-safe fallback, per the
       module contract. The result drives CONFIRMED vs INCONCLUSIVE vs UNHEALTHY.

    The terminal classification is the API client's, never produced here.
    """
    discovered = discover(states, station.key)
    discovered_count = len(discovered)

    representative = discovered.get("temp")
    if representative is None:
        return HealthResult(
            HealthStatus.UNHEALTHY,
            f"{station.key}: representative sensor.wu_temp_{station.key} absent",
            discovered_count,
        )

    for metric in REQUIRED_CORE_METRICS:
        sensor = discovered.get(metric)
        if sensor is None:
            return HealthResult(
                HealthStatus.UNHEALTHY,
                f"{station.key}: required-core sensor.wu_{metric}_{station.key} absent",
                discovered_count,
            )
        if not _is_usable(sensor.state):
            return HealthResult(
                HealthStatus.UNHEALTHY,
                f"{station.key}: required-core {metric} unusable ({sensor.state!r})",
                discovered_count,
            )

    # --- freshness ---------------------------------------------------------
    # Primary path fires only when last_reported is present and non-null/empty.
    if representative.last_reported is not None:
        advanced = _advanced_past(representative.last_reported, t0)
        if advanced is True:
            return HealthResult(
                HealthStatus.CONFIRMED,
                f"{station.key}: confirmed via last_reported advance",
                discovered_count,
            )
        # advanced is False (parseable, not later than t0) OR None (present but
        # naive/unparseable): both are a genuine primary-path failed-freshness
        # signal — never silently healthy.
        reason = "not advanced" if advanced is False else "naive/unparseable"
        return HealthResult(
            HealthStatus.UNHEALTHY,
            f"{station.key}: primary last_reported {reason} (transient)",
            discovered_count,
        )

    # Degrade-safe fallback: last_updated, then last_changed.
    fallback = representative.last_updated
    if fallback is None:
        fallback = representative.last_changed
    fallback_advanced = _advanced_past(fallback, t0)
    if fallback_advanced is True:
        return HealthResult(
            HealthStatus.CONFIRMED,
            f"{station.key}: confirmed via fallback timestamp advance",
            discovered_count,
        )
    # Fallback did not advance (or is absent/naive): the POST succeeded, the
    # representative is present, and required-core is usable, so this is
    # inconclusive-but-accepted, NOT unhealthy (an identical-value write cannot
    # advance last_updated/last_changed and must not be mistaken for an outage).
    return HealthResult(
        HealthStatus.INCONCLUSIVE,
        f"{station.key}: inconclusive (fallback timestamp not advanced; accepted)",
        discovered_count,
    )
