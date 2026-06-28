# pyright: strict
"""Pure discovery-transform checks: shape matching, skips, dedup, merge, render.

Drives the pure `discovery` module against synthetic `/states` `EntityState`
fixtures (no I/O). Asserts the anchored `^sensor\\.wu_obstimeutc_([a-z0-9]+)$` match,
that non-conforming suffixes are RETURNED in `skipped_entity_ids` (the transform
logs nothing), key dedup, the key-union of two reads, and the paste-ready YAML block.
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
        out.append(EntityState(entity_id=eid, state=state))
    return out


def check_discovery_transform() -> bool:
    """Assert the pure discovery transform's matching/skip/count/dedup contract."""
    checks: list[tuple[str, bool]] = []

    # --- single conforming station: obstimeutc + temp + humidity + pressure + uv
    one = _entities(
        fixtures.station_states("istation01")
    )  # obstimeutc+temp+humidity+pressure+uv
    result = discover_stations(one)
    st = {s.key: s for s in result.stations}
    checks += [
        (
            "single conforming key discovered",
            [s.key for s in result.stations] == ["istation01"],
        ),
        (
            "update_entity is sensor.wu_obstimeutc_<key>",
            st["istation01"].update_entity == "sensor.wu_obstimeutc_istation01",
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
    # `sensor.wu_obstimeutc_back_yard` has an underscore suffix (fails ^[a-z0-9]+$):
    # it must NOT become a station and MUST land in skipped_entity_ids.
    mixed = _entities(
        fixtures.station_states("istation01")
        + [
            {"entity_id": "sensor.wu_obstimeutc_back_yard", "state": "10.0"},
            {"entity_id": "sensor.wu_obstimeutc_UPPER", "state": "11.0"},
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
            == ["sensor.wu_obstimeutc_UPPER", "sensor.wu_obstimeutc_back_yard"],
        ),
    ]

    # --- empty input ⇒ empty stations, empty skips --------------------------
    empty = discover_stations([])
    checks += [
        ("empty input ⇒ empty stations", empty.stations == []),
        ("empty input ⇒ empty skipped_entity_ids", empty.skipped_entity_ids == []),
    ]

    # --- non-obstimeutc metric does not anchor a station --------------------
    # Only `sensor.wu_obstimeutc_<key>` is the representative; a humidity-only
    # entity for a key with no obstimeutc must yield no station.
    humidity_only = _entities(
        [{"entity_id": "sensor.wu_humidity_istation09", "state": "55"}]
    )
    checks.append(
        (
            "humidity-only key (no obstimeutc representative) ⇒ no station",
            discover_stations(humidity_only).stations == [],
        )
    )
    return report("DISCOVERY-TRANSFORM", "discovery", checks)


def check_discovery_merge_and_render() -> bool:
    """Assert `merge_station_counts` (key UNION + dedup) and `render_stations_block`."""
    a = Station(
        key="istation01",
        update_entity="sensor.wu_obstimeutc_istation01",
    )
    b_same = Station(
        key="istation01",
        update_entity="sensor.wu_obstimeutc_istation01",
    )
    confirm_only = Station(
        key="istation02",
        update_entity="sensor.wu_obstimeutc_istation02",
    )

    # same key in both reads ⇒ one station (dedup). Capture the raw list first so
    # the assertion can prove the UNDERLYING list deduped, not just the dict-comp.
    merged_same_list = merge_station_counts([a], [b_same])
    merged_same = {s.key: s for s in merged_same_list}
    # confirmation-only key ⇒ added to the union
    merged_union = {s.key: s for s in merge_station_counts([a], [b_same, confirm_only])}
    # first-read-only key absent from confirm ⇒ first-read station survives
    merged_first_only = {s.key: s for s in merge_station_counts([a], [])}
    # confirm-only key with first read empty ⇒ confirm station surfaces
    merged_confirm_only = {s.key: s for s in merge_station_counts([], [confirm_only])}

    checks: list[tuple[str, bool]] = [
        (
            "same key in both reads ⇒ one deduplicated station",
            len(merged_same_list) == 1 and set(merged_same) == {"istation01"},
        ),
        (
            "confirm-only key is unioned in",
            set(merged_union) == {"istation01", "istation02"},
        ),
        (
            "first-read-only key survives an empty confirmation",
            set(merged_first_only) == {"istation01"},
        ),
        (
            "a station surfacing only on the confirmation read is picked up",
            set(merged_confirm_only) == {"istation02"},
        ),
        (
            "render emits a paste-ready stations: block with key/update_entity",
            render_stations_block([a])
            == (
                "stations:\n"
                "  - key: istation01\n"
                "    update_entity: sensor.wu_obstimeutc_istation01\n"
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
    `{key, update_entity}` is fed through the public
    `config.validate` and must yield the SAME `Station` back. So a future change to
    the representative regex or the `update_entity` template that broke the
    per-station key / entity-shape contract is caught here at `--check` time,
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
        # that broke the per-station key/entity-shape contract is caught
        # here at --check time. config.validate routes the single station
        # through _validate_station internally (config.py:186) and preserves
        # both fields verbatim, so this is the equivalent round-trip
        # without a reportPrivateUsage strict error.
        cfg = config.validate(
            fixtures.default_options(
                stations=[
                    {
                        "key": st.key,
                        "update_entity": st.update_entity,
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
