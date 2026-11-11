"""Metric arithmetic over stored sums."""

from __future__ import annotations

import math
from collections.abc import Mapping

from pydantic import BaseModel, ConfigDict


class MetricResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    n: int
    bias: float | None = None
    mae: float | None = None
    rmse: float | None = None
    skill_score: float | None = None
    confident: bool
    pod: float | None = None
    far: float | None = None
    csi: float | None = None
    ets: float | None = None
    hss: float | None = None


#: One cell's sums, keyed by the ``day_scores`` sum columns:
#: ``n``, ``err_n``, ``err_sum``, ``abs_sum``, ``sq_sum``,
#: ``hits``, ``false_alarms``, ``misses``, ``correct_negs``, ``skill_n``,
#: ``skill_sq_sum`` and ``skill_ref_sq_sum``. A missing key reads as NULL.
MetricSums = Mapping[str, float | int | None]


def metrics_from_sums(sums: MetricSums, *, variable: str, min_n: int) -> MetricResult:
    """The metrics of one cell from its sums.

    The one place the metric arithmetic lives: the daily close's stored
    sums go through it. Bias is ``err_sum / err_n``, MAE ``abs_sum / err_n``,
    RMSE the square root of ``sq_sum / err_n``. Continuous skill is one
    minus the ratio of the mean squared errors over the ``skill_n`` pairs
    that have a persistence reference (no reference pair, or a reference
    error of 0, gives none).
    Rain (``variable == "precip"``) takes POD, FAR, CSI, ETS and HSS from
    the four contingency sums, with ETS as its skill and 0.0 for a mean
    that has no value. ``n == 0`` gives ``MetricResult(n=0,
    confident=False)``. Pure.
    """
    n = _count(sums.get("n"))
    if n == 0:
        return MetricResult(n=0, confident=False)
    err_n = _count(sums.get("err_n"))
    if variable == "precip":
        h = _total(sums.get("hits"))
        f = _total(sums.get("false_alarms"))
        m = _total(sums.get("misses"))
        cn = _total(sums.get("correct_negs"))
        pod = _safe_div(h, h + m)
        far = _safe_div(f, h + f)
        csi = _safe_div(h, h + f + m)
        total = h + f + m + cn
        random_hits = ((h + f) * (h + m) / total) if total else 0.0
        ets = _safe_div(h - random_hits, h + f + m - random_hits)
        hss = _safe_div(2 * (h * cn - f * m), (h + m) * (m + cn) + (h + f) * (f + cn))
        mse = _mean(sums.get("sq_sum"), err_n) or 0.0
        return MetricResult(
            n=n,
            bias=_mean(sums.get("err_sum"), err_n) or 0.0,
            mae=_mean(sums.get("abs_sum"), err_n) or 0.0,
            rmse=math.sqrt(mse),
            pod=pod,
            far=far,
            csi=csi,
            ets=ets,
            hss=hss,
            skill_score=ets,
            confident=n >= min_n and ets is not None,
        )
    mse = _mean(sums.get("sq_sum"), err_n)
    skill = _skill_from_sums(sums)
    return MetricResult(
        n=n,
        bias=_mean(sums.get("err_sum"), err_n),
        mae=_mean(sums.get("abs_sum"), err_n),
        rmse=None if mse is None else math.sqrt(mse),
        skill_score=skill,
        confident=n >= min_n and skill is not None,
    )


def _skill_from_sums(sums: MetricSums) -> float | None:
    """One minus the feed's mean squared error over the persistence one's,
    each over the ``skill_n`` referenced pairs, as ``AVG`` makes them."""
    skill_n = _count(sums.get("skill_n"))
    feed_mse = _mean(sums.get("skill_sq_sum"), skill_n)
    reference_mse = _mean(sums.get("skill_ref_sq_sum"), skill_n)
    if feed_mse is None or reference_mse is None or reference_mse == 0:
        return None
    return 1.0 - feed_mse / reference_mse


def _count(value: float | int | None) -> int:
    return 0 if value is None else int(value)


def _total(value: float | int | None) -> float:
    return float(value or 0)


def _mean(total: float | int | None, count: int) -> float | None:
    """``total / count`` as SQL's ``AVG`` gives it; ``None`` for no value."""
    if count == 0 or total is None:
        return None
    return float(total) / count


def _safe_div(num: float, den: float) -> float | None:
    if den == 0:
        return None
    return num / den
