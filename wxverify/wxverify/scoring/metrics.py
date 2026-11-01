"""Metric aggregation strategies."""

from __future__ import annotations

import math
import sqlite3
from collections.abc import Mapping
from typing import Protocol

from pydantic import BaseModel, ConfigDict

from wxverify.verification.asof import knowable_pair_predicate


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


#: One cell's sums, keyed by the ``day_scores`` sum columns (daily-close
#: plan 6.4): ``n``, ``err_n``, ``err_sum``, ``abs_sum``, ``sq_sum``,
#: ``hits``, ``false_alarms``, ``misses``, ``correct_negs``, ``skill_n``,
#: ``skill_sq_sum`` and ``skill_ref_sq_sum``. A missing key reads as NULL.
MetricSums = Mapping[str, float | int | None]


def metrics_from_sums(sums: MetricSums, *, variable: str, min_n: int) -> MetricResult:
    """The metrics of one cell from its sums (daily-close plan 6.4).

    The one place the metric arithmetic lives: both strategies call it after
    their SELECT, and the daily close's stored sums go through it too. Bias
    is ``err_sum / err_n``, MAE ``abs_sum / err_n``, RMSE the square root of
    ``sq_sum / err_n``. Continuous skill is one minus the ratio of the mean
    squared errors over the ``skill_n`` pairs that have a persistence
    reference (no reference pair, or a reference error of 0, gives none).
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


class MetricStrategy(Protocol):
    def aggregate(
        self,
        conn: sqlite3.Connection,
        *,
        site_id: int,
        feed_id: int,
        variable: str,
        day_ahead: int,
        window_cutoff: str | None,
        min_n: int,
        as_of: str | None = None,
    ) -> MetricResult: ...


def _window_clause(window_cutoff: str | None) -> tuple[str, tuple[object, ...]]:
    if window_cutoff is None:
        return "", ()
    return "AND valid_at >= ?", (window_cutoff,)


def _asof_clause(alias: str, as_of: str | None) -> tuple[str, tuple[object, ...]]:
    """Knowability clause for the as-of path; empty on the live path.

    With ``as_of=None`` the composed SQL is byte-identical to the production
    statement — the as-of path is pure parameterization, never a fork
    (plan §6, §18.6 live-equivalence).
    """
    if as_of is None:
        return "", ()
    sql, params = knowable_pair_predicate(alias, as_of=as_of)
    return f"AND {sql}", params


class ContinuousStrategy:
    def aggregate(
        self,
        conn: sqlite3.Connection,
        *,
        site_id: int,
        feed_id: int,
        variable: str,
        day_ahead: int,
        window_cutoff: str | None,
        min_n: int,
        as_of: str | None = None,
    ) -> MetricResult:
        clause, extra = _window_clause(window_cutoff)
        asof_clause, asof_params = _asof_clause("forecast_pairs", as_of)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS n, COUNT(error) AS err_n, SUM(error) AS err_sum,
                   SUM(abs_error) AS abs_sum, SUM(sq_error) AS sq_sum
            FROM forecast_pairs
            WHERE site_id=? AND feed_id=? AND variable=? AND day_ahead=?
              {clause}
              {asof_clause}
            """,
            (site_id, feed_id, variable, day_ahead, *extra, *asof_params),
        ).fetchone()
        if row is None:
            return MetricResult(n=0, confident=False)
        sums: dict[str, float | int | None] = {
            key: row[key] for key in ("n", "err_n", "err_sum", "abs_sum", "sq_sum")
        }
        if _count(sums["n"]) > 0:
            sums.update(
                _paired_skill(
                    conn,
                    site_id,
                    feed_id,
                    variable,
                    day_ahead,
                    window_cutoff,
                    as_of=as_of,
                )
            )
        return metrics_from_sums(sums, variable=variable, min_n=min_n)


class PrecipStrategy:
    def aggregate(
        self,
        conn: sqlite3.Connection,
        *,
        site_id: int,
        feed_id: int,
        variable: str,
        day_ahead: int,
        window_cutoff: str | None,
        min_n: int,
        as_of: str | None = None,
    ) -> MetricResult:
        clause, extra = _window_clause(window_cutoff)
        asof_clause, asof_params = _asof_clause("forecast_pairs", as_of)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS n,
                   SUM(cat_hit) AS hits, SUM(cat_false) AS false_alarms,
                   SUM(cat_miss) AS misses, SUM(cat_correct_neg) AS correct_negs,
                   COUNT(error) AS err_n, SUM(error) AS err_sum,
                   SUM(abs_error) AS abs_sum, SUM(sq_error) AS sq_sum
            FROM forecast_pairs
            WHERE site_id=? AND feed_id=? AND variable=? AND day_ahead=?
              {clause}
              {asof_clause}
            """,
            (site_id, feed_id, variable, day_ahead, *extra, *asof_params),
        ).fetchone()
        if row is None:
            return MetricResult(n=0, confident=False)
        sums: dict[str, float | int | None] = {
            key: row[key]
            for key in (
                "n",
                "hits",
                "false_alarms",
                "misses",
                "correct_negs",
                "err_n",
                "err_sum",
                "abs_sum",
                "sq_sum",
            )
        }
        return metrics_from_sums(sums, variable=variable, min_n=min_n)


def strategy_for(variable: str) -> MetricStrategy:
    if variable == "precip":
        return PrecipStrategy()
    return ContinuousStrategy()


def _paired_skill(
    conn: sqlite3.Connection,
    site_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    window_cutoff: str | None,
    *,
    as_of: str | None = None,
) -> dict[str, float | int | None]:
    """The skill sums of one cell: the count of the feed's pairs that join a
    persistence pair at the same ``valid_at``, ``lead_hours`` and
    ``day_ahead``, and the sums of the two sides' ``sq_error``; ``{}`` with
    no persistence feed. ``metrics_from_sums`` makes the skill from them."""
    persistence = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
    ).fetchone()
    if persistence is None:
        return {}
    clause = "" if window_cutoff is None else "AND fp.valid_at >= ?"
    # Knowability applies to BOTH sides of the paired-skill join — the
    # candidate (fp) AND the persistence reference (pp) — never fp alone
    # (plan §6). An unknowable reference drops the whole paired row.
    asof_fp, asof_fp_params = _asof_clause("fp", as_of)
    asof_pp, asof_pp_params = _asof_clause("pp", as_of)
    params: tuple[object, ...] = (
        site_id,
        feed_id,
        int(persistence["id"]),
        variable,
        day_ahead,
    )
    if window_cutoff is not None:
        params = (*params, window_cutoff)
    params = (*params, *asof_fp_params, *asof_pp_params)
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS skill_n,
               SUM(fp.sq_error) AS skill_sq_sum,
               SUM(pp.sq_error) AS skill_ref_sq_sum
        FROM forecast_pairs fp
        JOIN forecast_pairs pp
          ON pp.site_id = fp.site_id
         AND pp.variable = fp.variable
         AND pp.valid_at = fp.valid_at
         AND pp.lead_hours = fp.lead_hours
         AND pp.day_ahead = fp.day_ahead
        WHERE fp.site_id=? AND fp.feed_id=?
          AND pp.feed_id=?
          AND fp.variable=? AND fp.day_ahead=?
          {clause}
          {asof_fp}
          {asof_pp}
        """,
        params,
    ).fetchone()
    if row is None:
        return {}
    return {key: row[key] for key in ("skill_n", "skill_sq_sum", "skill_ref_sq_sum")}


def _safe_div(num: float, den: float) -> float | None:
    if den == 0:
        return None
    return num / den
