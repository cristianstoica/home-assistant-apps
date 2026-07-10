"""Bucket-1-A cadence oracle: pin the S-M2 pure cadence estimator behaviour.

Ported from py-weather's ``pyweather/check/cadence_checks.py`` oracle and
``pyweather/fixtures.py`` fixture builders.  The numeric expected values and
the fixture shapes are identical to py-weather's; only the names and import
paths are translated to wxverify's module layout.

Translation key (py-weather → wxverify):
  ``pyweather.cadence.base_interval``    → ``wxverify.obs.cadence.base_interval``
  ``pyweather.cadence.is_stale``         → ``wxverify.obs.cadence.is_stale``
  ``pyweather.cadence.parse_obstime``    → ``wxverify.obs.cadence.parse_obstime``
  ``pyweather.cadence.clamp``            → ``wxverify.obs.cadence._clamp`` (internal)
  ``pyweather.cadence.jittered_interval`` → replaced by ``obs_cadence_jitter``
    (wxverify uses deterministic blake2b instead of injected RNG; tests assert
    bounds + exact values instead of injecting a fixed-factor fake)
  ``fixtures.obstime_series``            → inline ``_obstime_series`` (same logic)
  ``fixtures.obstime_irregular``         → inline ``_obstime_irregular`` (same logic)
  ``fixtures.OBSTIME_T0``                → inline ``_OBSTIME_T0`` (same constant)
  ``fixtures.OBSTIME_NAIVE``             → inline ``_OBSTIME_NAIVE`` (same value)
  Constants: py-weather ``MAX``/``N``/``FACTOR`` →
             wxverify ``MAX_INTERVAL_SECONDS``/``WINDOW_N``/``FACTOR``/``STALE_FACTOR``
             /``JITTER_FRACTION``

All inputs are explicit (no wall-clock, no RNG).  The jitter function is
deterministic by construction (blake2b), so its exact values are pinned
directly without injection.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import pytest

from wxverify.obs.cadence import (
    FACTOR,
    JITTER_FRACTION,
    MAX_INTERVAL_SECONDS,
    STALE_FACTOR,
    WINDOW_N,
    base_interval,
    is_stale,
    obs_cadence_jitter,
    parse_obstime,
)

# ---------------------------------------------------------------------------
# Fixture builders — ported verbatim from py-weather's fixtures.py
# (same epoch, same strftime format, same accumulation logic)
# ---------------------------------------------------------------------------

_OBSTIME_EPOCH = datetime(2026, 6, 21, 0, 0, 0, tzinfo=UTC)
# Single-event (no measurable gap) fixture; matches fixtures.OBSTIME_T0
_OBSTIME_T0 = _OBSTIME_EPOCH.strftime("%Y-%m-%dT%H:%M:%SZ")
# Naive (offset-less) form — unparseable; matches fixtures.OBSTIME_NAIVE
_OBSTIME_NAIVE = "2026-06-23T19:27:26"


def _obstime_series(gap_seconds: int, count: int) -> tuple[str, ...]:
    """``count`` evenly-spaced obsTimeUtc strings, each ``gap_seconds`` after the last.

    Newest last.  Ported from ``fixtures.obstime_series``.
    """
    return tuple(
        (_OBSTIME_EPOCH + timedelta(seconds=gap_seconds * i)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        for i in range(count)
    )


def _obstime_irregular(inter_gaps: list[int]) -> tuple[str, ...]:
    """obsTimeUtc strings whose consecutive deltas are exactly ``inter_gaps``.

    Produces ``len(inter_gaps) + 1`` events (newest last).
    Ported from ``fixtures.obstime_irregular``.
    """
    offset = 0
    out = [_OBSTIME_EPOCH.strftime("%Y-%m-%dT%H:%M:%SZ")]
    for gap in inter_gaps:
        offset += gap
        out.append(
            (_OBSTIME_EPOCH + timedelta(seconds=offset)).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
    return tuple(out)


# ---------------------------------------------------------------------------
# Constants: exported contract for the S-M3 caller
# ---------------------------------------------------------------------------


def test_window_n_exported() -> None:
    """WINDOW_N==6 is exported so the S-M3 caller can truncate events to it."""
    assert WINDOW_N == 6


# ---------------------------------------------------------------------------
# parse_obstime — ported from py-weather cadence_checks implicitly (same fn)
# ---------------------------------------------------------------------------


def test_parse_obstime_z_form() -> None:
    """Z-suffix → tz-aware UTC datetime at the exact instant."""
    result = parse_obstime("2026-06-21T00:00:00Z")
    assert result is not None
    assert result.tzinfo is not None
    # Verify the exact instant
    assert result == datetime(2026, 6, 21, 0, 0, 0, tzinfo=UTC)


def test_parse_obstime_plus00_form() -> None:
    """+00:00 offset form → tz-aware UTC datetime at the exact instant."""
    result = parse_obstime("2026-06-21T12:30:00+00:00")
    assert result is not None
    assert result.tzinfo is not None
    assert result == datetime(2026, 6, 21, 12, 30, 0, tzinfo=UTC)


def test_parse_obstime_none_returns_none() -> None:
    """None input → None (offline / no-event signal)."""
    assert parse_obstime(None) is None


def test_parse_obstime_naive_returns_none() -> None:
    """Naive (offset-less) string → None; matches _OBSTIME_NAIVE fixture shape."""
    assert parse_obstime(_OBSTIME_NAIVE) is None


def test_parse_obstime_garbage_returns_none() -> None:
    """Unparseable garbage → None."""
    assert parse_obstime("not-a-timestamp") is None


def test_parse_obstime_paired_positive_negative() -> None:
    """Positive (Z-form parseable) paired with negative (naive unparseable).

    The positive goes red if parse_obstime rejects a valid timestamp;
    the negative goes red if parse_obstime accepts an offset-less one.
    Neither can pass vacuously.
    """
    good = "2026-06-23T12:00:00Z"
    bad = "2026-06-23T12:00:00"  # offset-less / naive
    assert parse_obstime(good) is not None, "Z-form must be parseable"
    assert parse_obstime(bad) is None, "naive form must be rejected"


# ---------------------------------------------------------------------------
# base_interval — ported from cadence_checks.check_cadence_estimator
# Numbers below are the py-weather oracle values, verified live against the
# wxverify port by hoare this pass.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "events,min_interval,expected",
    [
        # < 2 events ⇒ cold start at min_interval.
        # py-weather: "0 events ⇒ MIN (cold start)"
        ((), 300, 300),
        # py-weather: "1 event ⇒ MIN (no measurable gap)"
        ((_OBSTIME_T0,), 300, 300),
        # Exactly 2 events (1 gap of 900s): 900*0.8=720, past cold start.
        # py-weather: "2 events (1 gap) ⇒ 720 (past cold start, not MIN)"
        (_obstime_series(900, 2), 300, 720),
        # Fast: 60s gaps → 60*0.8=48, clamps UP to MIN 300.
        # py-weather: "60s-gap fast station floors at MIN 300 (48 clamped up)"
        (_obstime_series(60, 6), 300, 300),
        # Slow: 900s gaps → 900*0.8=720, in band.
        # py-weather: "900s-gap slow station ⇒ 720 (in band)"
        (_obstime_series(900, 6), 300, 720),
        # Very slow: 3000s gaps → 2400, clamps DOWN to MAX 1800.
        # py-weather: "3000s-gap station clamps down to MAX 1800"
        (_obstime_series(3000, 6), 300, 1800),
        # Bursty: gaps [900,900,900,1] → median=900 → 720.
        # py-weather: "one bursty gap does not skew (median) ⇒ 720"
        # This is the point of using median not mean: burst-resistance.
        (_obstime_irregular([900, 900, 900, 1]), 300, 720),
    ],
    ids=[
        "cold-start-0-events",
        "cold-start-1-event",
        "two-events-900s-gap-720",
        "fast-60s-clamps-up-to-300",
        "slow-900s-in-band-720",
        "very-slow-3000s-clamps-down-to-1800",
        "bursty-median-ignores-outlier-720",
    ],
)
def test_base_interval(
    events: tuple[str, ...], min_interval: int, expected: int
) -> None:
    """base_interval: cold start holds MIN; fast floors; slow in-band; very-slow caps.

    All expected values ported from py-weather's cadence oracle and verified
    live against the wxverify port.
    """
    assert base_interval(events, min_interval) == expected


def test_base_interval_clamp_invariant_low() -> None:
    """Computed value below min_interval always clamps UP to min_interval.

    Paired positive: a 60s-gap series produces 720 (in-band) when min=50.
    Paired negative: the same series is held at 300 when min=300 (floor).
    Both sides must be true — a floor bug flips one.
    """
    fast_events = _obstime_series(60, 6)  # raw = 48; floors at MIN
    in_band_events = _obstime_series(900, 6)  # raw = 720; in band at both MINs

    # Fast series clamps to the min floor in both cases
    assert base_interval(fast_events, 300) == 300, "48 must clamp up to 300"
    assert base_interval(fast_events, 50) == 50, "48 must clamp up to 50"

    # In-band series is unaffected by a low min
    assert base_interval(in_band_events, 50) == 720, "720 must pass through when min=50"
    assert base_interval(in_band_events, 300) == 720, (
        "720 must pass through when min=300"
    )


def test_base_interval_clamp_invariant_high() -> None:
    """Computed value above MAX_INTERVAL_SECONDS clamps DOWN to it.

    Paired positive: 3000s gaps clamp to 1800.
    Paired negative: 900s gaps (720) do NOT clamp to 1800 (they're under it).
    """
    assert base_interval(_obstime_series(3000, 6), 300) == MAX_INTERVAL_SECONDS
    assert base_interval(_obstime_series(900, 6), 300) < MAX_INTERVAL_SECONDS


# ---------------------------------------------------------------------------
# §13-A cadence-math half: sub-hour-resolution gap (plan note)
# Two obstimes exactly 5 minutes apart → 300s gap → base_interval == 300.
# (Not 1800 — the cadence math sees the real 300s gap, not a floored hour.)
# ---------------------------------------------------------------------------


def test_base_interval_sub_hour_resolution() -> None:
    """Two obstimes 300s apart (within the same hour) → base_interval == 300.

    §13-A cadence-math half: 0.8×300=240 clamps up to the 300 floor.
    This pins that the cadence math sees the real 300s gap — not 1800.
    (The other §13-A half — that S-M3's _obs_instant emits two distinct
    non-floored events — is an S-M3 concern and is not tested here.)
    """
    events = _obstime_series(300, 2)
    result = base_interval(events, 300)
    assert result == 300, (
        f"0.8×300=240 must clamp up to min_interval=300, not 1800; got {result}"
    )


# ---------------------------------------------------------------------------
# is_stale — ported from cadence_checks.check_stale_predicate
# Exact obstime strings and expected bool from py-weather oracle.
# ---------------------------------------------------------------------------

_STALE_NOW = datetime.fromisoformat("2026-06-23T12:00:00+00:00")
_STALE_LEARNED = 300  # 3 × 300 = 900s threshold


def test_is_stale_true_one_second_over() -> None:
    """age 901s > 3×300 → stale; strict > boundary caught."""
    # py-weather: "age 901s > 3*300 ⇒ stale"
    events = ("2026-06-23T11:44:59+00:00",)
    assert is_stale(events, _STALE_NOW, _STALE_LEARNED) is True


def test_is_stale_false_one_second_under() -> None:
    """age 899s < 3×300 → not stale."""
    # py-weather: "age 899s <= 3*300 ⇒ not stale"
    events = ("2026-06-23T11:45:01+00:00",)
    assert is_stale(events, _STALE_NOW, _STALE_LEARNED) is False


def test_is_stale_false_at_exact_boundary() -> None:
    """age == 900s (exactly 3×300) → not stale; operator is strict >."""
    # py-weather: "age == 900s (exact boundary) ⇒ not stale"
    events = ("2026-06-23T11:45:00+00:00",)
    assert is_stale(events, _STALE_NOW, _STALE_LEARNED) is False


def test_is_stale_false_empty_events() -> None:
    """Empty events → not stale (no parseable event → no signal)."""
    # py-weather: "no parseable events ⇒ not stale"
    assert is_stale((), _STALE_NOW, _STALE_LEARNED) is False


def test_is_stale_false_unparseable_events() -> None:
    """All-unparseable events tuple → treated as empty → not stale.

    Injected precondition: a tuple containing only a naive (offset-less) string.
    Paired with test_is_stale_true_one_second_over (which confirms stale fires
    when the precondition is a valid parseable event), so the false result
    cannot be a vacuous green.
    """
    events = (_OBSTIME_NAIVE,)  # offset-less → parse_obstime returns None
    assert is_stale(events, _STALE_NOW, _STALE_LEARNED) is False


def test_is_stale_stale_factor_constant() -> None:
    """STALE_FACTOR==3 is exported; is_stale uses it as the multiplier."""
    assert STALE_FACTOR == 3
    # Verify: the stale threshold is exactly STALE_FACTOR × learned_interval.
    # One second under the threshold → not stale.
    threshold_seconds = STALE_FACTOR * _STALE_LEARNED  # 900
    under_age = threshold_seconds - 1  # 899
    last_event = _STALE_NOW - timedelta(seconds=under_age)
    events = (last_event.strftime("%Y-%m-%dT%H:%M:%S+00:00"),)
    assert is_stale(events, _STALE_NOW, _STALE_LEARNED) is False


# ---------------------------------------------------------------------------
# obs_cadence_jitter — deterministic blake2b; no RNG injection needed.
# Replaces py-weather's injected-fake jitter tests with bound + exact-value
# assertions (the deterministic seam makes injection unnecessary).
# ---------------------------------------------------------------------------

_JITTER_BASE = 1000  # span = round(0.15 × 1000) = 150
_JITTER_SPAN = round(JITTER_FRACTION * _JITTER_BASE)  # 150


def test_obs_cadence_jitter_zero_base() -> None:
    """base_interval=0 → offset 0 (no span, nothing to jitter)."""
    assert obs_cadence_jitter(1, 0, 0) == 0
    assert obs_cadence_jitter(99, 7, 0) == 0


def test_obs_cadence_jitter_bounds_sweep() -> None:
    """Every (station_id, cycle_bucket) pair at base=1000 stays in [-150, +150].

    Sweeps 50 stations × 20 cycle_buckets = 1000 keys.  A modulo bug or
    off-by-one in the span calculation would produce an out-of-range value.
    """
    assert _JITTER_SPAN == 150, f"expected span 150; got {_JITTER_SPAN}"
    out_of_range: list[tuple[int, int, int]] = []
    for sid in range(50):
        for cb in range(20):
            offset = obs_cadence_jitter(sid, cb, _JITTER_BASE)
            if not (-_JITTER_SPAN <= offset <= _JITTER_SPAN):
                out_of_range.append((sid, cb, offset))
    assert not out_of_range, (
        f"offsets out of [-{_JITTER_SPAN}, +{_JITTER_SPAN}]: {out_of_range}"
    )


def test_obs_cadence_jitter_exact_values_station1_cb0() -> None:
    """station_id=1, cycle_bucket=0, base=1000 → exact offset 1 (blake2b-pinned).

    If the hash construction or seed changes, this turns red — that is the point.
    """
    assert obs_cadence_jitter(1, 0, _JITTER_BASE) == 1


def test_obs_cadence_jitter_exact_values_station2_cb5() -> None:
    """station_id=2, cycle_bucket=5, base=1000 → exact offset -110 (blake2b-pinned)."""
    assert obs_cadence_jitter(2, 5, _JITTER_BASE) == -110


def test_obs_cadence_jitter_exact_values_station42_cb100() -> None:
    """station_id=42, cycle_bucket=100, base=1000 → -78 (blake2b-pinned)."""
    assert obs_cadence_jitter(42, 100, _JITTER_BASE) == -78


def test_obs_cadence_jitter_deterministic_same_inputs() -> None:
    """Same (station_id, cycle_bucket, base_interval) → identical offset."""
    first = obs_cadence_jitter(7, 3, _JITTER_BASE)
    second = obs_cadence_jitter(7, 3, _JITTER_BASE)
    third = obs_cadence_jitter(7, 3, _JITTER_BASE)
    assert first == second == third, (
        "obs_cadence_jitter must return the same offset for repeated identical inputs"
    )


def test_obs_cadence_jitter_different_cycle_buckets_generally_differ() -> None:
    """Different cycle_buckets for the same station generally produce different offsets.

    This pins that successive polls (incrementing cycle_bucket) spread load —
    the whole point of the cycle_bucket parameter.  We check 5 consecutive
    buckets; all identical would mean the bucket is silently ignored.
    """
    station_id = 7
    offsets = [obs_cadence_jitter(station_id, cb, _JITTER_BASE) for cb in range(5)]
    # At least two must differ — if all five are the same the bucket is inert.
    unique = set(offsets)
    assert len(unique) > 1, (
        f"All 5 cycle_bucket offsets for station {station_id} are "
        f"identical ({offsets}); cycle_bucket appears to have no effect"
    )


def test_obs_cadence_jitter_jitter_fraction_constant() -> None:
    """JITTER_FRACTION==0.15 is exported; span = round(0.15 × base)."""
    assert JITTER_FRACTION == 0.15
    # The span for base=1000 must be exactly 150.
    assert round(JITTER_FRACTION * 1000) == 150


def test_obs_cadence_jitter_factor_constant() -> None:
    """FACTOR==0.8 is exported; the cadence derivation uses it."""
    assert FACTOR == 0.8


def _recompute_jitter(station_id: int, cycle_bucket: int, base: int) -> int:
    """Replicate the blake2b construction from cadence.py for cross-checking."""
    _JITTER_SEED = 1729
    span = round(JITTER_FRACTION * base)
    if span <= 0:
        return 0
    digest = hashlib.blake2b(
        f"{_JITTER_SEED}:{station_id}:{cycle_bucket}".encode(), digest_size=8
    ).digest()
    return int.from_bytes(digest, "big") % (2 * span + 1) - span


def test_obs_cadence_jitter_matches_recomputed_construction() -> None:
    """obs_cadence_jitter matches an independent reimplementation of blake2b.

    A refactor that changes the hash key, seed, or modulo arithmetic will
    diverge here even if the exact-value pins above somehow stay coincidentally
    correct.
    """
    for sid, cb, base in [(1, 0, 1000), (2, 5, 1000), (42, 100, 1000), (0, 0, 720)]:
        expected = _recompute_jitter(sid, cb, base)
        got = obs_cadence_jitter(sid, cb, base)
        assert got == expected, (
            f"station_id={sid}, cycle_bucket={cb}, base={base}: "
            f"module returned {got}, recomputed {expected}"
        )
