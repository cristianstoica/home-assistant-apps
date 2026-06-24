# pyright: strict
"""Persisted ``/data`` cadence state: pure (de)serialization + a best-effort shell.

The scheduler learns a per-station poll interval from a rolling window of raw
``obsTimeUtc`` strings (`StationCadence.events`). This module persists those
windows to ``/data`` across add-on restarts so a fresh boot resumes the learned
cadence instead of cold-starting.

`serialize` / `deserialize` are the pure, ``--check``-covered halves: `serialize`
emits a versioned JSON envelope, `deserialize` is *tolerant* — every corruption
(missing/garbage/unknown-version/malformed-station) degrades to the valid subset
(or ``{}``), never raising, so a clobbered ``/data`` file can never crash boot.

`load_state` / `save_state` are the imperative-shell wrappers (review-only, no
``--check``): they touch the filesystem and are best-effort — a missing file or
an `OSError` on save is logged and swallowed, so a transient FS fault never
crashes the poll loop (in-memory scheduling continues). This mirrors the
discovered-stations best-effort persist in ``__main__``.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import cast

from .models import StationCadence

STATE_VERSION = 1
DEFAULT_STATE_PATH = "/data/pyweather-cadence.json"

_log = logging.getLogger("pyweather")


def serialize(stations: dict[str, StationCadence]) -> str:
    """Emit the per-station cadence windows as a versioned JSON envelope."""
    return json.dumps(
        {
            "version": STATE_VERSION,
            "stations": {
                key: {"events": list(c.events)} for key, c in stations.items()
            },
        }
    )


def deserialize(text: str) -> dict[str, StationCadence]:
    """Parse the /data JSON into per-station cadence windows; tolerant of every
    corruption (missing/garbage/unknown-version/unknown-key) → degrade to the
    valid subset (or {}), never raise."""
    try:
        parsed: object = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    blob = cast(dict[str, object], parsed)
    if blob.get("version") != STATE_VERSION:
        return {}
    raw_stations = blob.get("stations")
    if not isinstance(raw_stations, dict):
        return {}
    stations_map = cast(dict[str, object], raw_stations)
    out: dict[str, StationCadence] = {}
    for key, entry in stations_map.items():
        if not isinstance(entry, dict):
            continue
        events_raw = cast(dict[str, object], entry).get("events")
        if not isinstance(events_raw, list):
            continue
        events = tuple(e for e in cast(list[object], events_raw) if isinstance(e, str))
        out[key] = StationCadence(events=events)
    return out


def load_state(path: str) -> dict[str, StationCadence]:
    """Read + deserialize the cadence state file; a missing file / OSError → {}."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return {}
    return deserialize(text)


def save_state(path: str, stations: dict[str, StationCadence]) -> None:
    """Atomically persist the cadence windows to `path` (best-effort).

    Writes to `path + ".tmp"` then `os.replace`s it into place. An OSError
    (disk-full, permission, /data unavailable) is logged and swallowed —
    persistence is best-effort, so a transient FS failure must never crash the
    poll loop; in-memory scheduling continues and the next cycle retries the
    save. Mirrors the discovered-stations best-effort persist in __main__."""
    tmp = path + ".tmp"
    try:
        Path(tmp).write_text(serialize(stations), encoding="utf-8")
        os.replace(tmp, path)
    except OSError as exc:
        _log.warning(
            "could not persist cadence state to %s (%s); continuing with "
            "in-memory scheduling",
            path,
            exc,
        )
        # Best-effort cleanup of the abandoned temp file (ignore if it too fails).
        try:
            Path(tmp).unlink(missing_ok=True)
        except OSError:
            pass
