"""Read-side composite score query: cache-backed windows plus a live path."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Literal

from wxverify.core.timeutil import parse_utc, utc_now
from wxverify.db.queue import enqueue_if_absent
from wxverify.scoring.cache import ScoreCacheRow, is_cache_fresh
from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.leaderboard import WindowResolution, resolve_window, score_badge
from wxverify.scoring.metrics import strategy_for
from wxverify.settings.keys import get_number_setting

CompositeStatus = Literal["hit", "stale", "rebuilding", "empty", "live"]

_RESCORE_FAILURE_COOLDOWN = timedelta(minutes=15)


@dataclass
class CompositeParts:
    source: str
    model: str
    raw_components: dict[str, float] = field(default_factory=lambda: {})
    components: dict[str, float] = field(default_factory=lambda: {})


@dataclass(frozen=True)
class CompositeResult:
    rows: list[dict[str, object]]
    status: CompositeStatus


def composite(
    conn: sqlite3.Connection, *, site_id: int, window: str = "rolling"
) -> list[dict[str, object]]:
    """Return composite rows for a window; cache-backed windows read the cache."""
    return composite_with_status(conn, site_id=site_id, window=window).rows


def composite_with_status(
    conn: sqlite3.Connection, *, site_id: int, window: str = "rolling"
) -> CompositeResult:
    """Return composite rows plus a cache status for the resolved window.

    Cache-backed windows (``rolling`` / ``all``) are served from ``score_cache``:
    a fresh full snapshot is a ``hit``, a same-window full-but-stale snapshot is
    served as ``stale`` (never recomputed live), and an absent/partial snapshot
    is ``rebuilding`` (empty rows). The no-input gate runs before any cache
    branch: a missing/disabled site or no active-competitor pairs within the
    window cutoff is genuinely ``empty`` regardless of cache contents. Custom
    ``Nd`` windows always compute live and return ``live`` — the engine never
    writes ``w:{N}d`` cache keys, so they must not look like cache misses.
    Callers enqueue a rescore (after the read closes) only for ``stale`` and
    ``rebuilding``.
    """
    resolved = resolve_window(conn, window)
    if not resolved.cache_backed:
        rows = _live_composite(
            conn,
            site_id=site_id,
            window_key=resolved.window_key,
            cutoff=resolved.cutoff,
        )
        return CompositeResult(rows=rows, status="live")
    expected_cells = _expected_active_cells(conn, site_id=site_id, resolved=resolved)
    if not expected_cells or not _site_enabled(conn, site_id):
        return CompositeResult(rows=[], status="empty")
    cached = _cached_composite(
        conn, site_id=site_id, resolved=resolved, expected_cells=expected_cells
    )
    if cached is None:
        return CompositeResult(rows=[], status="rebuilding")
    rows, fresh = cached
    return CompositeResult(rows=rows, status="hit" if fresh else "stale")


def enqueue_composite_rescore(conn: sqlite3.Connection, site_id: int) -> None:
    """Enqueue a ``pair_and_score`` rescore with terminal-failure cooldown.

    Composite-only guard: if the latest ``pair_and_score`` job for this site is
    terminally ``failed`` and was updated within the last 15 minutes, suppress
    the enqueue so per-request cache misses cannot re-enqueue a persistently
    failing scoring job on every poll. The latest outcome (not "any recent
    failed row") decides, so a later ``completed`` job supersedes an older
    failure. Leaderboard/curve enqueues (``_enqueue_score``) are not gated.
    """
    row = conn.execute(
        """
        SELECT status, updated_at
        FROM jobs
        WHERE type = 'pair_and_score'
          AND site_id = ?
          AND job_key = 'score'
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (site_id,),
    ).fetchone()
    if row is not None and str(row["status"]) == "failed":
        updated_at = parse_utc(str(row["updated_at"]))
        if utc_now() - updated_at < _RESCORE_FAILURE_COOLDOWN:
            return
    enqueue_if_absent(conn, "pair_and_score", site_id, "score", {"site_id": site_id})


def _site_enabled(conn: sqlite3.Connection, site_id: int) -> bool:
    row = conn.execute("SELECT enabled FROM sites WHERE id=?", (site_id,)).fetchone()
    return row is not None and bool(row["enabled"])


def _expected_active_cells(
    conn: sqlite3.Connection, *, site_id: int, resolved: WindowResolution
) -> set[tuple[int, str, int]]:
    """Whole-window active cell universe: (feed_id, variable, day_ahead)."""
    window_clause = "" if resolved.cutoff is None else "AND fp.valid_at >= ?"
    params: tuple[object, ...] = (
        (site_id,) if resolved.cutoff is None else (site_id, resolved.cutoff)
    )
    rows = conn.execute(
        f"""
        SELECT DISTINCT fp.feed_id, fp.variable, fp.day_ahead
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE fp.site_id = ?
          {window_clause}
          AND {active_competitor_clause(site_expr="fp.site_id")}
        """,
        params,
    ).fetchall()
    return {
        (int(row["feed_id"]), str(row["variable"]), int(row["day_ahead"]))
        for row in rows
    }


def _cached_composite(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    resolved: WindowResolution,
    expected_cells: set[tuple[int, str, int]],
) -> tuple[list[dict[str, object]], bool] | None:
    """Serve composite from ``score_cache``; None only on absent/mismatch.

    Returns ``(rows, fresh)``: a full same-window snapshot is served even when
    stale (``fresh=False``) — the stale path must never fall through to the
    live recompute. The cell-set match is whole-window and cross-variable: any
    expected cell missing from the cache makes the entire window a miss.
    """
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    rows = conn.execute(
        f"""
        SELECT sc.feed_id, sc.variable, sc.day_ahead, sc.n, sc.skill_score,
               sc.computed_at, f.source, f.model
        FROM score_cache sc
        JOIN feeds f ON f.id = sc.feed_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = sc.site_id AND sfs.feed_id = sc.feed_id
        WHERE sc.site_id = ?
          AND sc.window_key = ?
          AND {active_competitor_clause(site_expr="sc.site_id")}
        ORDER BY f.source, f.model, sc.variable, sc.day_ahead
        """,
        (site_id, resolved.window_key),
    ).fetchall()
    if not rows:
        return None
    cached_cells = {
        (int(row["feed_id"]), str(row["variable"]), int(row["day_ahead"]))
        for row in rows
    }
    if cached_cells != expected_cells:
        return None
    fresh = True
    grouped: dict[int, CompositeParts] = {}
    by_variable: dict[int, dict[str, list[float]]] = {}
    for row in rows:
        cache_row = ScoreCacheRow(
            n=int(row["n"]),
            skill_score=_optional_float(row["skill_score"]),
            computed_at=None if row["computed_at"] is None else str(row["computed_at"]),
        )
        if not is_cache_fresh(cache_row, resolved.window_key):
            fresh = False
        skill = cache_row.skill_score
        if skill is None or cache_row.n < min_n:
            continue
        feed_id = int(row["feed_id"])
        grouped.setdefault(
            feed_id,
            CompositeParts(source=str(row["source"]), model=str(row["model"])),
        )
        by_variable.setdefault(feed_id, {}).setdefault(str(row["variable"]), []).append(
            skill
        )
    for feed_id, variables in by_variable.items():
        parts = grouped[feed_id]
        for variable, scores in variables.items():
            raw = sum(scores) / len(scores)
            parts.raw_components[variable] = raw
            parts.components[variable] = max(0.0, raw)
    return _format_composite(grouped, resolved.window_key), fresh


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, int | float | str):
        raise TypeError("invalid numeric cache value")
    return float(value)


def _live_composite(
    conn: sqlite3.Connection, *, site_id: int, window_key: str, cutoff: str | None
) -> list[dict[str, object]]:
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    window_clause = "" if cutoff is None else "AND fp.valid_at >= ?"
    params: tuple[object, ...] = (site_id,) if cutoff is None else (site_id, cutoff)
    rows = conn.execute(
        f"""
        SELECT DISTINCT fp.feed_id, f.source, f.model, fp.variable, fp.day_ahead
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE fp.site_id = ?
          {window_clause}
          AND {active_competitor_clause(site_expr="fp.site_id")}
        ORDER BY f.source, f.model, fp.variable, fp.day_ahead
        """,
        params,
    ).fetchall()
    grouped: dict[int, CompositeParts] = {}
    by_variable: dict[int, dict[str, list[float]]] = {}
    for row in rows:
        variable = str(row["variable"])
        result = strategy_for(variable).aggregate(
            conn,
            site_id=site_id,
            feed_id=int(row["feed_id"]),
            variable=variable,
            day_ahead=int(row["day_ahead"]),
            window_cutoff=cutoff,
            min_n=min_n,
        )
        if result.skill_score is None or result.n < min_n:
            continue
        feed_id = int(row["feed_id"])
        grouped.setdefault(
            feed_id,
            CompositeParts(source=str(row["source"]), model=str(row["model"])),
        )
        by_variable.setdefault(feed_id, {}).setdefault(variable, []).append(
            result.skill_score
        )

    for feed_id, variables in by_variable.items():
        parts = grouped[feed_id]
        for variable, scores in variables.items():
            raw = sum(scores) / len(scores)
            parts.raw_components[variable] = raw
            parts.components[variable] = max(0.0, raw)

    return _format_composite(grouped, window_key)


def _format_composite(
    grouped: dict[int, CompositeParts], window_key: str
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for feed_id, parts in grouped.items():
        if not parts.components:
            continue
        score = sum(parts.components.values()) / len(parts.components)
        raw_score = sum(parts.raw_components.values()) / len(parts.raw_components)
        out.append(
            {
                "feed_id": feed_id,
                "source": parts.source,
                "model": parts.model,
                "window_key": window_key,
                "component_count": len(parts.components),
                "components": dict(sorted(parts.components.items())),
                "raw_components": dict(sorted(parts.raw_components.items())),
                "score": score,
                "raw_score": raw_score,
                "badge": score_badge(score),
                "below_baseline": raw_score < 0,
            }
        )
    out.sort(key=_sort_key)
    return out


def _sort_key(row: dict[str, object]) -> tuple[float, str, str, int]:
    score = row["score"]
    feed_id = row["feed_id"]
    if not isinstance(score, float) or not isinstance(feed_id, int):
        raise TypeError("invalid composite row")
    return (-score, str(row["source"]), str(row["model"]), feed_id)
