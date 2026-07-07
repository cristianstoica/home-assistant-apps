"""Live read-side composite score query."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.leaderboard import resolve_window, score_badge
from wxverify.scoring.metrics import strategy_for
from wxverify.settings.keys import get_number_setting


@dataclass
class CompositeParts:
    source: str
    model: str
    raw_components: dict[str, float] = field(default_factory=lambda: {})
    components: dict[str, float] = field(default_factory=lambda: {})


def composite(
    conn: sqlite3.Connection, *, site_id: int, window: str = "rolling"
) -> list[dict[str, object]]:
    resolved = resolve_window(conn, window)
    return _live_composite(
        conn,
        site_id=site_id,
        window_key=resolved.window_key,
        cutoff=resolved.cutoff,
    )


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
