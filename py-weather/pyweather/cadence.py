# pyright: strict
"""Pure cadence estimator: learn a per-station poll interval from raw obstimes.

Given the persisted window of raw ``obsTimeUtc`` strings (`StationCadence.events`,
newest last), this derives the next poll interval with no I/O and no clock:

    gaps → median → ``* FACTOR`` → clamp(``min_interval``, ``MAX``) → jitter.

The estimator stores raw events (not a derived period) so a future estimator
change re-derives from history. `base_interval` is the deterministic,
jitter-free value the oracle pins to exact numbers; `jittered_interval` wraps it
in the injected `JitterSource` (the value the scheduler actually schedules).

`parse_obstime` is the single canonical ``obsTimeUtc`` parser — both this module
and `health.py` parse the same string, so it lives here and `health.py` imports
it. This module imports only `models` + stdlib, so no circular import arises.
"""

from __future__ import annotations

from datetime import datetime, timezone
from statistics import median

from .models import JitterSource

# The fixed healthy-slow-uploader ceiling: the learned interval never relaxes
# past this, regardless of how slowly a station uploads.
MAX = 1800
# A confirmed-offline (204 / dead) station is re-probed once a day.
OFFLINE_REPROBE = 86400
# The learned interval is the median gap scaled down by this factor, so the
# poller leads the uploader slightly rather than chasing it.
FACTOR = 0.8
# The persisted cadence window length (last N raw obstimes, newest last).
N = 6


def parse_obstime(value: str | None) -> datetime | None:
    """Parse an obsTimeUtc ISO-8601 string to a tz-aware UTC datetime, else None.

    Accepts the ``Z`` and ``+00:00`` offset forms (3.11+ fromisoformat). A None,
    naive (offset-less), or unparseable value returns None — the offline /
    no-event signal. Pure: no clock.
    """
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def gaps(events: tuple[str, ...]) -> list[float]:
    """Consecutive inter-event deltas in seconds, skipping unparseable entries.

    Returns ``[]`` for fewer than two parseable events (no measurable gap).
    """
    parsed = [p for p in (parse_obstime(e) for e in events) if p is not None]
    return [(parsed[i + 1] - parsed[i]).total_seconds() for i in range(len(parsed) - 1)]


def clamp(value: float, low: int, high: int) -> int:
    """Round `value` and clamp it into ``[low, high]``."""
    return int(max(low, min(high, round(value))))


def base_interval(events: tuple[str, ...], min_interval: int) -> int:
    """Deterministic, jitter-free learned interval (cold start ⇒ `min_interval`).

    Fewer than two parseable events ⇒ no measurable gap ⇒ hold at `min_interval`
    (cold start). Otherwise ``clamp(round(median(gaps) * FACTOR), min_interval,
    MAX)``.
    """
    g = gaps(events)
    if len(g) < 1:  # < 2 parseable events ⇒ cold start
        return min_interval
    return clamp(median(g) * FACTOR, min_interval, MAX)


def jittered_interval(
    events: tuple[str, ...], min_interval: int, jitter: JitterSource
) -> int:
    """The value the scheduler schedules: `base_interval` passed through `jitter`."""
    return round(jitter(float(base_interval(events, min_interval))))


def is_stale(events: tuple[str, ...], now: datetime, learned_interval: int) -> bool:
    """True iff there is ≥1 parseable event AND the last is older than 3×learned.

    Pure boolean: `now` is passed in (no clock). No parseable event ⇒ False (no
    signal). The threshold is strict ``>`` 3×`learned_interval`.
    """
    parsed = [p for p in (parse_obstime(e) for e in events) if p is not None]
    if not parsed:
        return False
    return (now - parsed[-1]).total_seconds() > 3 * learned_interval
