"""Read-side leaderboard and curve queries."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from wxverify.core.timeutil import window_cutoff
from wxverify.scoring.cache import ScoreCacheRow, is_cache_fresh
from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.metrics import strategy_for
from wxverify.settings.keys import get_number_setting


@dataclass(frozen=True)
class LeaderboardRow:
    feed_id: int
    source: str
    model: str
    n: int
    skill_score: float | None
    badge: int | None
    below_baseline: bool
    confident: bool
    bias: float | None
    mae: float | None
    rmse: float | None
    window_key: str
    window_days: int | None


@dataclass(frozen=True)
class WindowResolution:
    window_key: str
    window_days: int | None
    cutoff: str | None
    cache_backed: bool


@dataclass(frozen=True)
class LeaderboardResult:
    rows: list[LeaderboardRow]
    cache_miss: bool
    window_key: str
    window_days: int | None


def leaderboard(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str,
) -> list[LeaderboardRow]:
    return leaderboard_with_status(
        conn,
        site_id=site_id,
        variable=variable,
        day_ahead=day_ahead,
        window=window,
    ).rows


def leaderboard_with_status(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str,
) -> LeaderboardResult:
    resolved = resolve_window(conn, window)
    if resolved.cache_backed:
        rows = _cached_leaderboard(
            conn,
            site_id=site_id,
            variable=variable,
            day_ahead=day_ahead,
            resolved=resolved,
        )
        if rows is not None:
            return LeaderboardResult(
                rows=rows,
                cache_miss=False,
                window_key=resolved.window_key,
                window_days=resolved.window_days,
            )
    rows = _live_leaderboard(
        conn,
        site_id=site_id,
        variable=variable,
        day_ahead=day_ahead,
        resolved=resolved,
    )
    return LeaderboardResult(
        rows=rows,
        cache_miss=resolved.cache_backed,
        window_key=resolved.window_key,
        window_days=resolved.window_days,
    )


def _live_leaderboard(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    resolved: WindowResolution,
) -> list[LeaderboardRow]:
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    feeds = conn.execute(
        f"""
        SELECT DISTINCT fp.feed_id, f.source, f.model
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE fp.site_id = ?
          AND fp.variable = ?
          AND fp.day_ahead = ?
          AND {active_competitor_clause(site_expr="fp.site_id")}
        ORDER BY f.source, f.model
        """,
        (site_id, variable, day_ahead),
    ).fetchall()
    rows: list[LeaderboardRow] = []
    for feed in feeds:
        result = strategy_for(variable).aggregate(
            conn,
            site_id=site_id,
            feed_id=int(feed["feed_id"]),
            variable=variable,
            day_ahead=day_ahead,
            window_cutoff=resolved.cutoff,
            min_n=min_n,
        )
        rows.append(
            LeaderboardRow(
                feed_id=int(feed["feed_id"]),
                source=str(feed["source"]),
                model=str(feed["model"]),
                n=result.n,
                skill_score=result.skill_score,
                badge=score_badge(result.skill_score),
                below_baseline=below_baseline(result.skill_score),
                confident=result.confident,
                bias=result.bias,
                mae=result.mae,
                rmse=result.rmse,
                window_key=resolved.window_key,
                window_days=resolved.window_days,
            )
        )
    return rows


def _cached_leaderboard(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    resolved: WindowResolution,
) -> list[LeaderboardRow] | None:
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    rows = conn.execute(
        f"""
        SELECT sc.site_id, sc.feed_id, sc.variable, sc.day_ahead, sc.window_key,
               sc.n, sc.bias, sc.mae, sc.rmse, sc.skill_score, sc.computed_at,
               f.source, f.model
        FROM score_cache sc
        JOIN feeds f ON f.id = sc.feed_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = sc.site_id AND sfs.feed_id = sc.feed_id
        WHERE sc.site_id = ?
          AND sc.variable = ?
          AND sc.day_ahead = ?
          AND sc.window_key = ?
          AND {active_competitor_clause(site_expr="sc.site_id")}
        ORDER BY f.source, f.model
        """,
        (site_id, variable, day_ahead, resolved.window_key),
    ).fetchall()
    if not rows:
        return None
    cached_feed_ids = {int(row["feed_id"]) for row in rows}
    expected_feed_ids = _expected_active_feed_ids(
        conn,
        site_id=site_id,
        variable=variable,
        day_ahead=day_ahead,
        resolved=resolved,
    )
    if cached_feed_ids != expected_feed_ids:
        return None
    out: list[LeaderboardRow] = []
    for row in rows:
        cache_row = ScoreCacheRow(
            site_id=int(row["site_id"]),
            feed_id=int(row["feed_id"]),
            variable=str(row["variable"]),
            day_ahead=int(row["day_ahead"]),
            window_key=str(row["window_key"]),
            n=int(row["n"]),
            bias=_optional_float(row["bias"]),
            mae=_optional_float(row["mae"]),
            rmse=_optional_float(row["rmse"]),
            skill_score=_optional_float(row["skill_score"]),
            computed_at=None if row["computed_at"] is None else str(row["computed_at"]),
        )
        if not is_cache_fresh(cache_row, resolved.window_key):
            return None
        raw = cache_row.skill_score
        out.append(
            LeaderboardRow(
                feed_id=cache_row.feed_id,
                source=str(row["source"]),
                model=str(row["model"]),
                n=cache_row.n,
                skill_score=raw,
                badge=score_badge(raw),
                below_baseline=below_baseline(raw),
                confident=cache_row.n >= min_n and raw is not None,
                bias=cache_row.bias,
                mae=cache_row.mae,
                rmse=cache_row.rmse,
                window_key=resolved.window_key,
                window_days=resolved.window_days,
            )
        )
    return out


def _expected_active_feed_ids(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    resolved: WindowResolution,
) -> set[int]:
    window_clause = "" if resolved.cutoff is None else "AND fp.valid_at >= ?"
    params: tuple[object, ...] = (site_id, variable, day_ahead)
    if resolved.cutoff is not None:
        params = (*params, resolved.cutoff)
    rows = conn.execute(
        f"""
        SELECT DISTINCT fp.feed_id
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE fp.site_id = ?
          AND fp.variable = ?
          AND fp.day_ahead = ?
          {window_clause}
          AND {active_competitor_clause(site_expr="fp.site_id")}
        """,
        params,
    ).fetchall()
    return {int(row["feed_id"]) for row in rows}


def score_badge(raw: float | None) -> int | None:
    if raw is None:
        return None
    return round(max(0.0, raw) * 100)


def below_baseline(raw: float | None) -> bool:
    return raw is not None and raw < 0


def cutoff_for_window(conn: sqlite3.Connection, window: str) -> str | None:
    return resolve_window(conn, window).cutoff


def resolve_window(conn: sqlite3.Connection, window: str) -> WindowResolution:
    if window == "all":
        return WindowResolution(
            window_key="w:all", window_days=None, cutoff=None, cache_backed=True
        )
    if window == "rolling":
        days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
        return WindowResolution(
            window_key=f"w:{days}",
            window_days=days,
            cutoff=window_cutoff(days),
            cache_backed=True,
        )
    if window.endswith("d") and window[:-1].isdigit():
        days = int(window[:-1])
        return WindowResolution(
            window_key=f"live:{days}d",
            window_days=days,
            cutoff=window_cutoff(days),
            cache_backed=False,
        )
    days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
    return WindowResolution(
        window_key=f"w:{days}",
        window_days=days,
        cutoff=window_cutoff(days),
        cache_backed=True,
    )


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, int | float | str):
        raise TypeError("invalid numeric cache value")
    return float(value)
