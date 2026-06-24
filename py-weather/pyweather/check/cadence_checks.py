# pyright: strict
"""``--check`` oracle for the pure cadence estimator (`pyweather.cadence`).

Drives `base_interval` / `clamp` / `jittered_interval` / `is_stale` against the
fixed obstime fixtures and pins each result to a SPECIFIC computed number, so a
hollow assertion that passes regardless of the math cannot survive: the estimator
band (cold start ⇒ MIN, fast ⇒ floor, slow ⇒ in-band, very-slow ⇒ MAX), the
median's burst-resistance, the clamp invariant, the ±15% jitter band, and the
strict ``>`` stale boundary are each caught by a fixture one step off the edge.

The jitter seam is injected as a deterministic fixed-factor fake (`_FixedJitter`),
never a live RNG, so the band assertions are exact.
"""

from __future__ import annotations

from datetime import datetime

from .. import fixtures
from ..cadence import base_interval, clamp, is_stale, jittered_interval
from .report import report


class _FixedJitter:
    """Deterministic `JitterSource` fake: multiply `base` by a fixed factor.

    Injected so the band assertions pin exact numbers (``720*0.85 = 612``,
    ``720*1.15 = 828``) with no live RNG.
    """

    def __init__(self, factor: float) -> None:
        self._factor = factor

    def __call__(self, base: float) -> float:
        return base * self._factor


def check_cadence_estimator() -> bool:
    """Assert base_interval over fixed event lists: cold start holds MIN; a
    fast (60s-gap) station floors at 300; a slow (900s-gap) station relaxes
    toward but never past 1800; median ignores one bursty gap."""
    checks: list[tuple[str, bool]] = []
    # < 2 events ⇒ cold start at MIN.
    checks.append(
        (
            "0 events ⇒ MIN (cold start)",
            base_interval((), 300) == 300,
        )
    )
    checks.append(
        (
            "1 event ⇒ MIN (no measurable gap)",
            base_interval((fixtures.OBSTIME_T0,), 300) == 300,
        )
    )
    # Exactly 2 events (1 gap) ⇒ past cold start ⇒ computes (900*0.8=720), not MIN.
    checks.append(
        (
            "2 events (1 gap) ⇒ 720 (past cold start, not MIN)",
            base_interval(fixtures.obstime_series(900, 2), 300) == 720,
        )
    )
    # Fast: 60s gaps ⇒ 60*0.8=48, clamped up to MIN 300.
    checks.append(
        (
            "60s-gap fast station floors at MIN 300 (48 clamped up)",
            base_interval(fixtures.obstime_series(60, 6), 300) == 300,
        )
    )
    # Slow: 900s gaps ⇒ 900*0.8=720, in band ⇒ 720.
    checks.append(
        (
            "900s-gap slow station ⇒ 720 (in band)",
            base_interval(fixtures.obstime_series(900, 6), 300) == 720,
        )
    )
    # Very slow: 3000s gaps ⇒ 2400 clamped down to MAX 1800.
    checks.append(
        (
            "3000s-gap station clamps down to MAX 1800",
            base_interval(fixtures.obstime_series(3000, 6), 300) == 1800,
        )
    )
    # Median ignores a single bursty gap: gaps [900,900,900,1] ⇒ median 900 ⇒ 720.
    checks.append(
        (
            "one bursty gap does not skew (median) ⇒ 720",
            base_interval(fixtures.obstime_irregular([900, 900, 900, 1]), 300) == 720,
        )
    )
    return report("CADENCE-ESTIMATOR", "cadence", checks)


def check_clamp() -> bool:
    """Clamp below MIN ⇒ MIN; above MAX ⇒ MAX; in-band ⇒ unchanged (rounded)."""
    checks = [
        ("below MIN clamps up to 300", clamp(48.0, 300, 1800) == 300),
        ("above MAX clamps down to 1800", clamp(2400.0, 300, 1800) == 1800),
        ("in-band passes through (rounded)", clamp(719.6, 300, 1800) == 720),
    ]
    return report("CLAMP", "clamp", checks)


def check_jitter_band() -> bool:
    """An injected jitter draw keeps the output within ±15% of base; the seam
    is deterministic (no live RNG)."""
    events = fixtures.obstime_series(900, 6)  # base_interval ⇒ 720
    lo = _FixedJitter(0.85)
    hi = _FixedJitter(1.15)
    checks = [
        (
            "jitter at -15% ⇒ 612 (720*0.85), in band",
            jittered_interval(events, 300, lo) == 612,
        ),
        (
            "jitter at +15% ⇒ 828 (720*1.15), in band",
            jittered_interval(events, 300, hi) == 828,
        ),
        (
            "output never below base*0.85",
            jittered_interval(events, 300, lo) >= round(720 * 0.85),
        ),
        (
            "output never above base*1.15",
            jittered_interval(events, 300, hi) <= round(720 * 1.15),
        ),
    ]
    return report("JITTER-BAND", "jitter", checks)


def check_stale_predicate() -> bool:
    """obstime age > 3*learned ⇒ stale flag; at/under ⇒ no flag; no events ⇒ no flag."""
    now = datetime.fromisoformat("2026-06-23T12:00:00+00:00")
    learned = 300  # 3*learned = 900s
    # last event 901s before now ⇒ stale.
    stale = ("2026-06-23T11:44:59+00:00",)
    # last event 899s before now ⇒ not stale.
    fresh = ("2026-06-23T11:45:01+00:00",)
    # last event exactly 900s before now (age == 3*learned) ⇒ not stale.
    boundary = ("2026-06-23T11:45:00+00:00",)
    checks = [
        ("age 901s > 3*300 ⇒ stale", is_stale(stale, now, learned) is True),
        ("age 899s <= 3*300 ⇒ not stale", is_stale(fresh, now, learned) is False),
        (
            "age == 900s (exact boundary) ⇒ not stale",
            is_stale(boundary, now, learned) is False,
        ),
        ("no parseable events ⇒ not stale", is_stale((), now, learned) is False),
    ]
    return report("STALE-PREDICATE", "stale", checks)
