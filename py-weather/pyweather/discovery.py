# pyright: strict
"""Pure discovery transform: project a `/states` snapshot into candidate stations.

Given the full `/states` projection (the same `list[EntityState]` the runtime
health path consumes), discover every Weather.com PWS station from its pinned
representative entity `sensor.wu_obstimeutc_<key>`. The transform is **pure** — no
I/O, no logging — so it is fully oracle-testable against a `/states` fixture. It
RETURNS the non-conforming entity ids it skipped (rather than logging them) so
the impure `__main__._discover_and_persist` caller emits the operator-facing
WARNING; this keeps the pure/impure split the `--check` oracle relies on.

Discovery rule:

* The representative shape is the anchored `^sensor\\.wu_obstimeutc_([a-z0-9]+)$`
  — the capture group is the station `key`. The match anchors on the `obstimeutc`
  metric specifically because `sensor.wu_obstimeutc_<key>` is the entity health
  and cadence read, and the entity the poller drives.
* `update_entity` = `sensor.wu_obstimeutc_<key>`. Health keys off the obstime
  sensor's presence alone — there is no sibling-count signal.
* Any `sensor.wu_obstimeutc_*` whose suffix is NOT bare lowercase-alphanumeric (an
  underscore or uppercase char, e.g. `sensor.wu_obstimeutc_back_yard`) is SKIPPED
  into `skipped_entity_ids` — preserving the strict `^[a-z0-9]+$` key contract
  rather than weakening it.
* Keys are de-duplicated (a key can only match `sensor.wu_obstimeutc_<key>` once).
"""

from __future__ import annotations

import re
from typing import NamedTuple

from .models import EntityState, Station

# The representative-entity shape; the capture group is the (allowlisted) key.
# `sensor.wu_obstimeutc_` then a bare lowercase-alphanumeric suffix, anchored both ends.
_REPRESENTATIVE_RE = re.compile(r"^sensor\.wu_obstimeutc_([a-z0-9]+)$")
# The looser prefix that flags a non-conforming representative for the skip list:
# it looks like an obstimeutc representative but its suffix fails the strict key shape.
_OBSTIME_PREFIX = "sensor.wu_obstimeutc_"


class DiscoveryResult(NamedTuple):
    """The pure transform's per-scan output: discovered stations + skipped ids.

    `stations` is the de-duplicated, key-sorted list of discovered `Station`s.
    `skipped_entity_ids` is every `sensor.wu_obstimeutc_*` entity whose suffix failed
    the strict `^[a-z0-9]+$` key contract — RETURNED for the impure caller to log
    at WARNING, never logged here (the pure/impure split). This is per-scan
    TELEMETRY and is distinct from the `__main__` startup CONTROL-FLOW result
    (`list[Station] | None` + `SystemExit`); the two never merge.
    """

    stations: list[Station]
    skipped_entity_ids: list[str]


def discover_stations(states: list[EntityState]) -> DiscoveryResult:
    """Project a `/states` snapshot into discovered stations + skipped ids (pure).

    Matches the anchored representative shape, builds one `Station` per unique
    key, and collects any `sensor.wu_obstimeutc_*` with a non-conforming suffix
    into `skipped_entity_ids`.
    Returns both; logs nothing.
    """
    stations: list[Station] = []
    skipped: list[str] = []
    seen: set[str] = set()
    for entity in states:
        eid = entity.entity_id
        match = _REPRESENTATIVE_RE.match(eid)
        if match is None:
            if eid.startswith(_OBSTIME_PREFIX):
                # Looks like an obstimeutc representative but the suffix is not a
                # bare lowercase-alphanumeric key — skip (preserve the key contract).
                skipped.append(eid)
            continue
        key = match.group(1)
        if key in seen:
            continue
        seen.add(key)
        stations.append(
            Station(
                key=key,
                update_entity=f"sensor.wu_obstimeutc_{key}",
            )
        )
    stations.sort(key=lambda s: s.key)
    return DiscoveryResult(stations=stations, skipped_entity_ids=skipped)


def merge_station_counts(first: list[Station], confirm: list[Station]) -> list[Station]:
    """Union two reads' keys into one deduplicated, key-sorted station list.

    The resolved set is the UNION of both reads' keys: a key in either read
    appears once in the output. The confirmation re-read exists so a station
    that surfaces late (its representative arrived only on the second /states
    read) is still picked up — never to drop a first-read station on a
    confirmation blip. With no per-station count, two reads of the same key are
    identical `Station`s, so the union is a plain key dedup.
    """
    best: dict[str, Station] = {}
    for station in [*first, *confirm]:
        best.setdefault(station.key, station)
    return sorted(best.values(), key=lambda s: s.key)


def render_stations_block(stations: list[Station]) -> str:
    """Render a paste-ready `stations:` YAML block for the persist-failure log.

    Emitted by the impure caller when `set_options` fails, so the operator can
    copy the discovered list straight into the Configuration tab. Stdlib string
    formatting only — no PyYAML (the shape is fixed and trivially renderable).
    """
    lines = ["stations:"]
    for station in stations:
        lines.append(f"  - key: {station.key}")
        lines.append(f"    update_entity: {station.update_entity}")
    return "\n".join(lines) + "\n"
