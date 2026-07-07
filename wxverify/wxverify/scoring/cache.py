"""Score-cache freshness and upsert helpers."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from wxverify.core.timeutil import parse_utc, utc_day_bucket
from wxverify.scoring.metrics import MetricResult


@dataclass(frozen=True, kw_only=True)
class ScoreCacheRow:
    site_id: int = 0
    feed_id: int = 0
    variable: str = ""
    day_ahead: int = 0
    window_key: str = ""
    n: int = 0
    bias: float | None = None
    mae: float | None = None
    rmse: float | None = None
    pod: float | None = None
    far: float | None = None
    csi: float | None = None
    ets: float | None = None
    hss: float | None = None
    skill_score: float | None = None
    computed_at: str | None = None


def is_cache_fresh(
    row: ScoreCacheRow, window_key: str, today_utc_bucket: str | None = None
) -> bool:
    bucket = _parse_utc_day_bucket(row.computed_at)
    if bucket is None:
        return False
    if window_key == "w:all":
        return True
    return bucket >= (today_utc_bucket or utc_day_bucket())


def upsert_score_cache(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    window_key: str,
    result: MetricResult,
    computed_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO score_cache
            (site_id, feed_id, variable, day_ahead, window_key, n, bias, mae,
             rmse, pod, far, csi, ets, hss, skill_score, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_id, feed_id, variable, day_ahead, window_key) DO UPDATE SET
            n=excluded.n, bias=excluded.bias, mae=excluded.mae,
            rmse=excluded.rmse, pod=excluded.pod, far=excluded.far,
            csi=excluded.csi, ets=excluded.ets, hss=excluded.hss,
            skill_score=excluded.skill_score, computed_at=excluded.computed_at
        """,
        (
            site_id,
            feed_id,
            variable,
            day_ahead,
            window_key,
            result.n,
            result.bias,
            result.mae,
            result.rmse,
            result.pod,
            result.far,
            result.csi,
            result.ets,
            result.hss,
            result.skill_score,
            computed_at,
        ),
    )


def _parse_utc_day_bucket(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        return parse_utc(value).date().isoformat()
    except ValueError:
        return None
