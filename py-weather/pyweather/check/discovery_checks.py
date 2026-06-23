# pyright: strict
"""Pure discovery-transform checks: shape matching, skips, counting, dedup, merge, render.

Drives the pure `discovery` module against synthetic `/states` `EntityState`
fixtures (no I/O). Asserts the anchored `^sensor\\.wu_temp_([a-z0-9]+)$` match,
that non-conforming suffixes are RETURNED in `skipped_entity_ids` (the transform
logs nothing), `expected_sensors` counting via `health.discover`, dedup, the
per-key `max` union of two reads, and the paste-ready YAML block.
"""

from __future__ import annotations

from .. import config, fixtures
from ..discovery import (
    discover_stations,
    merge_station_counts,
    render_stations_block,
)
from ..models import EntityState, Station
from .report import report


def _entities(raw: list[dict[str, object]]) -> list[EntityState]:
    """Project synthetic `/states` dicts into `EntityState` (mirrors haapi parsing)."""
    out: list[EntityState] = []
    for obj in raw:
        eid = obj["entity_id"]
        state = obj["state"]
        assert isinstance(eid, str) and isinstance(state, str)
        out.append(
            EntityState(
                entity_id=eid,
                state=state,
                last_reported=None,
                last_updated=None,
                last_changed=None,
            )
        )
    return out


def check_discovery_transform() -> bool:
    """Assert the pure discovery transform's matching/skip/count/dedup contract."""
    checks: list[tuple[str, bool]] = []

    # --- single conforming station: temp + 3 siblings ⇒ expected_sensors == 4 -
    one = _entities(fixtures.station_states("istation01"))  # temp+humidity+pressure+uv
    result = discover_stations(one)
    st = {s.key: s for s in result.stations}
    checks += [
        (
            "single conforming key discovered",
            [s.key for s in result.stations] == ["istation01"],
        ),
        (
            "update_entity is sensor.wu_temp_<key>",
            st["istation01"].update_entity == "sensor.wu_temp_istation01",
        ),
        (
            "expected_sensors counts all 4 sibling metrics via health.discover",
            st["istation01"].expected_sensors == 4,
        ),
        ("no skipped ids for a clean fleet", result.skipped_entity_ids == []),
    ]

    # --- two stations, distinct keys ----------------------------------------
    two = _entities(
        fixtures.station_states("istation01") + fixtures.station_states("istation02")
    )
    r2 = discover_stations(two)
    checks.append(
        (
            "two distinct keys discovered (sorted)",
            sorted(s.key for s in r2.stations) == ["istation01", "istation02"],
        )
    )

    # --- non-conforming suffix ⇒ skipped, not a station ---------------------
    # `sensor.wu_temp_back_yard` has an underscore suffix (fails ^[a-z0-9]+$):
    # it must NOT become a station and MUST land in skipped_entity_ids.
    mixed = _entities(
        fixtures.station_states("istation01")
        + [
            {"entity_id": "sensor.wu_temp_back_yard", "state": "10.0"},
            {"entity_id": "sensor.wu_temp_UPPER", "state": "11.0"},
        ]
    )
    rm = discover_stations(mixed)
    checks += [
        (
            "non-conforming suffixes excluded from stations",
            [s.key for s in rm.stations] == ["istation01"],
        ),
        (
            "non-conforming suffixes returned in skipped_entity_ids",
            sorted(rm.skipped_entity_ids)
            == ["sensor.wu_temp_UPPER", "sensor.wu_temp_back_yard"],
        ),
    ]

    # --- empty input ⇒ empty stations, empty skips --------------------------
    empty = discover_stations([])
    checks += [
        ("empty input ⇒ empty stations", empty.stations == []),
        ("empty input ⇒ empty skipped_entity_ids", empty.skipped_entity_ids == []),
    ]

    # --- non-temp metric does not anchor a station --------------------------
    # Only `sensor.wu_temp_<key>` is the representative; a humidity-only entity
    # for a key with no temp must yield no station.
    humidity_only = _entities(
        [{"entity_id": "sensor.wu_humidity_istation09", "state": "55"}]
    )
    checks.append(
        (
            "humidity-only key (no temp representative) ⇒ no station",
            discover_stations(humidity_only).stations == [],
        )
    )
    return report("DISCOVERY-TRANSFORM", "discovery", checks)


def check_discovery_merge_and_render() -> bool:
    """Assert `merge_station_counts` (UNION + per-key max) and `render_stations_block`."""
    a = Station(
        key="istation01", update_entity="sensor.wu_temp_istation01", expected_sensors=1
    )
    b_same = Station(
        key="istation01", update_entity="sensor.wu_temp_istation01", expected_sensors=4
    )
    confirm_only = Station(
        key="istation02", update_entity="sensor.wu_temp_istation02", expected_sensors=3
    )

    # same key in both reads ⇒ per-key max (4, not 1, never "last wins")
    merged_same = {s.key: s for s in merge_station_counts([a], [b_same])}
    # confirmation-only key ⇒ added to the union
    merged_union = {s.key: s for s in merge_station_counts([a], [b_same, confirm_only])}
    # confirmation LOWER than first ⇒ first-read (higher) value kept
    merged_lower = {s.key: s for s in merge_station_counts([b_same], [a])}
    # first-read-only key absent from confirm ⇒ first-read station survives
    merged_first_only = {s.key: s for s in merge_station_counts([a], [])}

    checks: list[tuple[str, bool]] = [
        (
            "same-key union takes the higher count (max, not last-wins)",
            merged_same["istation01"].expected_sensors == 4,
        ),
        (
            "confirm-only key is unioned in",
            set(merged_union) == {"istation01", "istation02"},
        ),
        (
            "confirm-only key keeps its confirmation count",
            merged_union["istation02"].expected_sensors == 3,
        ),
        (
            "confirmation lower than first keeps the higher first-read count",
            merged_lower["istation01"].expected_sensors == 4,
        ),
        (
            "first-read-only key survives an empty confirmation",
            set(merged_first_only) == {"istation01"},
        ),
        (
            "render emits a paste-ready stations: block with key/update_entity/expected_sensors",
            render_stations_block([a])
            == (
                "stations:\n"
                "  - key: istation01\n"
                "    update_entity: sensor.wu_temp_istation01\n"
                "    expected_sensors: 1\n"
            ),
        ),
    ]
    return report("DISCOVERY-MERGE", "discovery-merge", checks)


def check_discovery_construction_passes_validator() -> bool:
    """Assert every discovered `Station` round-trips through `config.validate`.

    The discovered stations are NOT routed back through `config.validate` in
    production; they are `Station`s built directly in `discover_stations`. This pins
    that the construction is equivalent to a validator round-trip — for each
    discovered `Station`, a one-station options payload built from its
    `{key, update_entity, expected_sensors}` is fed through the public
    `config.validate` and must yield the SAME `Station` back. So a future change to
    the representative regex or the `update_entity` template that broke the
    per-station key / entity-shape / count contract is caught here at `--check` time,
    not silently shipped on the discovered path.
    """
    states = _entities(
        fixtures.station_states("istation01") + fixtures.station_states("istation02")
    )
    discovered = discover_stations(states).stations
    checks: list[tuple[str, bool]] = [
        (
            "discovered fleet is non-empty (guard against a vacuous round-trip)",
            len(discovered) == 2,
        ),
    ]
    for st in discovered:
        # Round-trip through the PUBLIC validator (not the private
        # _validate_station): a one-station options payload built from the
        # discovered fields must validate AND yield the SAME Station back, so a
        # future change to the representative regex or update_entity template
        # that broke the per-station key/entity-shape/count contract is caught
        # here at --check time. config.validate routes the single station
        # through _validate_station internally (config.py:186) and preserves
        # expected_sensors verbatim, so this is the equivalent round-trip
        # without a reportPrivateUsage strict error.
        cfg = config.validate(
            fixtures.default_options(
                stations=[
                    {
                        "key": st.key,
                        "update_entity": st.update_entity,
                        "expected_sensors": st.expected_sensors,
                    }
                ]
            )
        )
        checks.append(
            (
                f"discovered {st.key} round-trips through config.validate unchanged",
                cfg.stations == (st,),
            )
        )
    return report("DISCOVERY-VALIDATOR", "discovery-validator", checks)
