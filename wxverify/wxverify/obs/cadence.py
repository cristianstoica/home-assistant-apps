"""Pure cadence estimator: learn a per-station obs poll interval from obstimes.

Ported from py-weather's ``pyweather/cadence.py`` (the sibling add-on) with one
deliberate adaptation: py-weather jitters the learned interval with an injected
``random.uniform``-backed ``JitterSource``; wxverify replaces that RNG seam with
a deterministic blake2b hash-jitter (mirroring ``core.hashing.obs_jitter_minutes``)
so scheduling is reproducible in tests with no RNG injection.

The derivation is: parsed obstime gaps → median → ``* FACTOR`` → clamp to
``[min_interval, MAX_INTERVAL_SECONDS]`` → deterministic ±``JITTER_FRACTION``
jitter. No I/O, no clock reads inside the math: ``events`` and ``min_interval``
are parameters, and ``is_stale`` takes ``now`` as an argument. The caller (S-M3)
owns the settings-backed ``min_interval`` and the wall-clock ``now``.

``parse_obstime`` is the STRICT parser: it operates only on the already-normalized
stored cadence events. The tolerant normalization of raw upstream payloads lives
in S-M3's ``pws_adapter`` normalizer, NOT here (plan §5.7).
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from statistics import median

# The learned interval is the median gap scaled down by this factor, so the
# poller leads the uploader slightly rather than chasing it (py-weather FACTOR).
FACTOR = 0.8
# The fixed healthy-slow-uploader ceiling: the learned interval never relaxes
# past this, regardless of how slowly a station uploads (py-weather MAX).
MAX_INTERVAL_SECONDS = 1800
# The persisted cadence window length (last N raw obstimes, newest last;
# py-weather N).
WINDOW_N = 6
# Stale-advisory multiplier: the last obs is "stale" past this many learned
# intervals. Advisory only — it does NOT gate scheduling (plan §5.7).
STALE_FACTOR = 3
# The deterministic jitter half-width as a fraction of the base interval:
# the scheduled value lands in ``base * [1 - f, 1 + f]`` (py-weather's ±15%).
JITTER_FRACTION = 0.15
# blake2b keying seed, matching core.hashing.obs_jitter_minutes' construction.
_JITTER_SEED = 1729


def parse_obstime(value: str | None) -> datetime | None:
    """Parse an obsTimeUtc ISO-8601 string to a tz-aware UTC datetime, else None.

    STRICT parser (py-weather ``cadence.parse_obstime``, lines 38-53): accepts the
    ``Z`` and ``+00:00`` offset forms (3.11+ ``fromisoformat``). A None, naive
    (offset-less), or unparseable value returns ``None`` — the offline / no-event
    signal. Pure: no clock. Operates only on already-normalized stored cadence
    events; tolerance of raw payloads lives in S-M3's normalizer, not here.
    """
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _gaps(events: tuple[str, ...]) -> list[float]:
    """Consecutive inter-event deltas in seconds, skipping unparseable entries.

    Returns ``[]`` for fewer than two parseable events (no measurable gap).
    Ported from py-weather ``cadence.gaps`` (lines 56-62).
    """
    parsed = [p for p in (parse_obstime(e) for e in events) if p is not None]
    return [(parsed[i + 1] - parsed[i]).total_seconds() for i in range(len(parsed) - 1)]


def _clamp(value: float, low: int, high: int) -> int:
    """Round ``value`` and clamp it into ``[low, high]`` (py-weather ``clamp``)."""
    return int(max(low, min(high, round(value))))


def base_interval(events: tuple[str, ...], min_interval: int) -> int:
    """Deterministic, jitter-free learned interval (cold start ⇒ ``min_interval``).

    Ported from py-weather ``cadence.base_interval`` (lines 70-80). Fewer than two
    parseable events ⇒ no measurable gap ⇒ hold at ``min_interval`` (cold start).
    Otherwise ``clamp(round(median(gaps) * FACTOR), min_interval,
    MAX_INTERVAL_SECONDS)``. Only the newest ``WINDOW_N`` events carry a cadence;
    the caller persists at most that many.
    """
    g = _gaps(events)
    if len(g) < 1:  # < 2 parseable events ⇒ cold start
        return min_interval
    return _clamp(median(g) * FACTOR, min_interval, MAX_INTERVAL_SECONDS)


def obs_cadence_jitter(station_id: int, cycle_bucket: int, base_interval: int) -> int:
    """Deterministic ±``JITTER_FRACTION`` jitter offset in seconds for a poll.

    The wxverify adaptation of py-weather's ``JitterSource`` (``random.uniform(
    base*0.85, base*1.15)``): instead of live RNG, derive a signed offset in
    ``[-span, +span]`` where ``span = round(JITTER_FRACTION * base_interval)``,
    keyed deterministically on ``(station_id, cycle_bucket)`` via blake2b — the
    same construction as ``core.hashing.obs_jitter_minutes``. Returns the OFFSET
    (add it to ``base_interval`` to get the scheduled value); a zero ``base_interval``
    yields ``0``.

    ``cycle_bucket`` is a monotonic poll-cycle counter the caller (S-M3) supplies
    (e.g. ``int(last_obs_epoch // base_interval)``, as ``scheduler._enqueue_due_obs``
    already derives it): successive polls of the SAME station fall in different
    buckets and so draw different offsets, spreading load without an RNG seam.
    """
    span = round(JITTER_FRACTION * base_interval)
    if span <= 0:
        return 0
    digest = hashlib.blake2b(
        f"{_JITTER_SEED}:{station_id}:{cycle_bucket}".encode(), digest_size=8
    ).digest()
    return int.from_bytes(digest, "big") % (2 * span + 1) - span


def is_stale(events: tuple[str, ...], now: datetime, learned_interval: int) -> bool:
    """True iff ≥1 parseable event AND the last is older than STALE_FACTOR×learned.

    Ported from py-weather ``cadence.is_stale`` (lines 90-99). Pure boolean:
    ``now`` is passed in (no clock). No parseable event ⇒ ``False`` (no signal).
    The threshold is strict ``>`` ``STALE_FACTOR`` × ``learned_interval``. Advisory
    only — it does NOT gate scheduling (plan §5.7); the caller emits a WARNING.
    """
    parsed = [p for p in (parse_obstime(e) for e in events) if p is not None]
    if not parsed:
        return False
    return (now - parsed[-1]).total_seconds() > STALE_FACTOR * learned_interval
