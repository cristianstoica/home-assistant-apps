# pyright: strict
"""``--check`` oracle for the pure ``/data`` cadence (de)serialization (`pyweather.state`).

Pins the two pure halves of the persistence seam: `check_state_roundtrip` proves
``serialize → deserialize`` preserves events (including an empty-events station)
and the key set, and `check_state_tolerant_load` proves every corruption
(garbage JSON / non-object top-level / unknown version / missing stations /
non-string event / malformed per-station entry) degrades to the valid subset or
``{}`` and never raises. The FS shell (`load_state`/`save_state`) is review-only
and not driven here.
"""

from __future__ import annotations

from ..models import StationCadence
from ..state import deserialize, serialize
from .report import report


def check_state_roundtrip() -> bool:
    """serialize → deserialize round-trips events for multiple stations."""
    original = {
        "istation01": StationCadence(
            events=("2026-06-23T19:00:00Z", "2026-06-23T19:15:00Z")
        ),
        "istation02": StationCadence(events=()),
    }
    restored = deserialize(serialize(original))
    checks = [
        (
            "round-trip preserves station01 events",
            restored.get("istation01") == original["istation01"],
        ),
        (
            "round-trip preserves an empty-events station",
            restored.get("istation02") == original["istation02"],
        ),
        ("round-trip key set matches", set(restored) == set(original)),
    ]
    return report("STATE-ROUNDTRIP", "state", checks)


def check_state_tolerant_load() -> bool:
    """Every corruption degrades to {} (or drops the bad key), never raises."""
    checks = [
        ("garbage JSON ⇒ {}", deserialize("{ not json") == {}),
        ("non-object top-level ⇒ {}", deserialize("[1,2,3]") == {}),
        (
            "unknown version (with station data) ⇒ {}",
            deserialize(
                '{"version":99,"stations":{"k":{"events":["2026-06-23T19:00:00Z"]}}}'
            )
            == {},
        ),
        ("missing stations key ⇒ {}", deserialize('{"version":1}') == {}),
        (
            "non-string event dropped, key kept",
            deserialize(
                '{"version":1,"stations":{"k":{"events":["2026-06-23T19:00:00Z",5]}}}'
            )
            == {"k": StationCadence(events=("2026-06-23T19:00:00Z",))},
        ),
        (
            "malformed per-station entry skipped",
            deserialize('{"version":1,"stations":{"bad":42,"ok":{"events":[]}}}')
            == {"ok": StationCadence(events=())},
        ),
    ]
    return report("STATE-TOLERANT", "state-tolerant", checks)
