"""Metric aggregation strategies."""

from __future__ import annotations

import math
import sqlite3
from typing import Protocol

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
    ) -> MetricResult: ...


def _window_clause(window_cutoff: str | None) -> tuple[str, tuple[object, ...]]:
    if window_cutoff is None:
        return "", ()
    return "AND valid_at >= ?", (window_cutoff,)


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
    ) -> MetricResult:
        clause, extra = _window_clause(window_cutoff)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS n, AVG(error) AS bias, AVG(abs_error) AS mae,
                   AVG(sq_error) AS mse
            FROM forecast_pairs
            WHERE site_id=? AND feed_id=? AND variable=? AND day_ahead=?
              {clause}
            """,
            (site_id, feed_id, variable, day_ahead, *extra),
        ).fetchone()
        if row is None:
            return MetricResult(n=0, confident=False)
        n = int(row["n"])
        if n == 0:
            return MetricResult(n=0, confident=False)
        mse = float(row["mse"])
        skill = _paired_skill(
            conn, site_id, feed_id, variable, day_ahead, window_cutoff
        )
        return MetricResult(
            n=n,
            bias=float(row["bias"]),
            mae=float(row["mae"]),
            rmse=math.sqrt(mse),
            skill_score=skill,
            confident=n >= min_n and skill is not None,
        )


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
    ) -> MetricResult:
        clause, extra = _window_clause(window_cutoff)
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS n,
                   SUM(cat_hit) AS h, SUM(cat_false) AS f,
                   SUM(cat_miss) AS m, SUM(cat_correct_neg) AS cn,
                   AVG(error) AS bias, AVG(abs_error) AS mae, AVG(sq_error) AS mse
            FROM forecast_pairs
            WHERE site_id=? AND feed_id=? AND variable=? AND day_ahead=?
              {clause}
            """,
            (site_id, feed_id, variable, day_ahead, *extra),
        ).fetchone()
        if row is None:
            return MetricResult(n=0, confident=False)
        n = int(row["n"])
        if n == 0:
            return MetricResult(n=0, confident=False)
        h = float(row["h"] or 0)
        f = float(row["f"] or 0)
        m = float(row["m"] or 0)
        cn = float(row["cn"] or 0)
        pod = _safe_div(h, h + m)
        far = _safe_div(f, h + f)
        csi = _safe_div(h, h + f + m)
        total = h + f + m + cn
        random_hits = ((h + f) * (h + m) / total) if total else 0.0
        ets = _safe_div(h - random_hits, h + f + m - random_hits)
        hss = _safe_div(2 * (h * cn - f * m), (h + m) * (m + cn) + (h + f) * (f + cn))
        mse = float(row["mse"] or 0.0)
        return MetricResult(
            n=n,
            bias=float(row["bias"] or 0.0),
            mae=float(row["mae"] or 0.0),
            rmse=math.sqrt(mse),
            pod=pod,
            far=far,
            csi=csi,
            ets=ets,
            hss=hss,
            skill_score=ets,
            confident=n >= min_n and ets is not None,
        )


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
) -> float | None:
    persistence = conn.execute(
        "SELECT id FROM feeds WHERE source='virtual' AND model='_persistence'"
    ).fetchone()
    if persistence is None:
        return None
    clause = "" if window_cutoff is None else "AND fp.valid_at >= ?"
    params: tuple[object, ...] = (
        site_id,
        feed_id,
        int(persistence["id"]),
        variable,
        day_ahead,
    )
    if window_cutoff is not None:
        params = (*params, window_cutoff)
    row = conn.execute(
        f"""
        SELECT AVG(fp.sq_error) AS feed_mse,
               AVG(pp.sq_error) AS persistence_mse
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
        """,
        params,
    ).fetchone()
    if row is None or row["feed_mse"] is None or row["persistence_mse"] is None:
        return None
    feed_mse = float(row["feed_mse"])
    persistence_mse = float(row["persistence_mse"])
    if persistence_mse == 0:
        return None
    return 1.0 - feed_mse / persistence_mse


def _safe_div(num: float, den: float) -> float | None:
    if den == 0:
        return None
    return num / den
