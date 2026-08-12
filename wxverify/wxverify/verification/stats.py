"""Pure verification statistics: error metrics, ETS, block bootstrap (§11/§12).

Everything here is arithmetic over already-selected numbers — no SQLite, no
clock reads, stdlib only (`random`, `math`). The clustered moving-block
bootstrap resamples TARGET DATES (the §12 clustering unit); callers map the
resampled date indices back onto per-lead paired series themselves.
"""

from __future__ import annotations

import math
import random
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass


def mean(values: Sequence[float]) -> float:
    """Arithmetic mean; raises on empty input (callers gate on n first)."""
    if not values:
        raise ValueError("mean of empty sequence")
    return sum(values) / len(values)


def mae(errors: Sequence[float]) -> float:
    """Mean absolute error over signed errors (predicted - truth)."""
    return mean([abs(e) for e in errors])


def bias(errors: Sequence[float]) -> float:
    """Mean signed error (predicted - truth)."""
    return mean(list(errors))


def rmse(errors: Sequence[float]) -> float:
    """Root-mean-square error over signed errors."""
    return math.sqrt(mean([e * e for e in errors]))


@dataclass(frozen=True)
class Contingency:
    """2x2 occurrence contingency counts."""

    hits: int = 0
    misses: int = 0
    false_alarms: int = 0
    correct_negatives: int = 0

    @property
    def n(self) -> int:
        return self.hits + self.misses + self.false_alarms + self.correct_negatives

    def add(self, outcome: str) -> Contingency:
        """A new table with one classified day added."""
        return Contingency(
            hits=self.hits + (outcome == "hit"),
            misses=self.misses + (outcome == "miss"),
            false_alarms=self.false_alarms + (outcome == "false_alarm"),
            correct_negatives=self.correct_negatives + (outcome == "correct_negative"),
        )


def classify_occurrence(predicted_wet: bool, observed_wet: bool) -> str:
    """One day's contingency class from predicted/observed wet flags."""
    if predicted_wet:
        return "hit" if observed_wet else "false_alarm"
    return "miss" if observed_wet else "correct_negative"


def ets(table: Contingency) -> float | None:
    """Equitable Threat Score; None when undefined (§11).

    Undefined when the table is empty or when hits equal the chance-hit
    expectation exactly at a zero denominator (all-one-class samples).
    """
    n = table.n
    if n == 0:
        return None
    hits_random = (table.hits + table.misses) * (table.hits + table.false_alarms) / n
    denominator = table.hits + table.misses + table.false_alarms - hits_random
    if denominator == 0:
        return None
    return (table.hits - hits_random) / denominator


def moving_block_indices(rng: random.Random, n: int, block_length: int) -> list[int]:
    """One moving-block resample of ``range(n)``: overlapping blocks of
    ``block_length`` consecutive indices, drawn with replacement, truncated
    to exactly ``n`` indices (§12). ``n`` short of one block falls back to
    a single truncated block."""
    if n <= 0:
        return []
    length = min(block_length, n)
    starts_hi = n - length  # inclusive
    out: list[int] = []
    while len(out) < n:
        start = rng.randint(0, starts_hi)
        out.extend(range(start, start + length))
    return out[:n]


def resample_counts(indices: Sequence[int]) -> Counter[int]:
    """Multiplicity of each original index in one resample."""
    return Counter(indices)


def percentile_ci(samples: Sequence[float], level: float) -> tuple[float, float]:
    """Two-sided percentile confidence interval at ``level`` (e.g. 0.95)."""
    if not samples:
        raise ValueError("percentile CI of empty sample set")
    ordered = sorted(samples)
    alpha = 1.0 - level
    return (
        _quantile(ordered, alpha / 2.0),
        _quantile(ordered, 1.0 - alpha / 2.0),
    )


def _quantile(ordered: Sequence[float], q: float) -> float:
    """Linear-interpolation quantile over an ALREADY SORTED sequence."""
    if len(ordered) == 1:
        return ordered[0]
    position = q * (len(ordered) - 1)
    lo = math.floor(position)
    hi = math.ceil(position)
    if lo == hi:
        return ordered[lo]
    weight = position - lo
    return ordered[lo] * (1.0 - weight) + ordered[hi] * weight
