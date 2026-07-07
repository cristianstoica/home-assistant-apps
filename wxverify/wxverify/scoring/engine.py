"""Scoring engine orchestration."""

from __future__ import annotations

import sqlite3

from wxverify.core.timeutil import isoformat_utc, window_cutoff
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.metrics import strategy_for
from wxverify.scoring.multimodel import materialize_multimodel_mean
from wxverify.scoring.pairing import pair_real_models
from wxverify.scoring.persistence import materialize_persistence
from wxverify.settings.keys import get_number_setting


def pair_and_score(conn: sqlite3.Connection, site_id: int | None = None) -> None:
    pair_real_models(conn, site_id)
    materialize_persistence(conn, site_id)
    materialize_multimodel_mean(conn, site_id)
    _clear_score_cache(conn, site_id)
    rolling_days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
    min_n = get_number_setting(conn, "min_n", 30, minimum=0)
    _score_window(
        conn, site_id, f"w:{rolling_days}", window_cutoff(rolling_days), min_n
    )
    _score_window(conn, site_id, "w:all", None, min_n)


def _clear_score_cache(conn: sqlite3.Connection, site_id: int | None) -> None:
    if site_id is None:
        conn.execute("DELETE FROM score_cache")
        return
    conn.execute("DELETE FROM score_cache WHERE site_id=?", (site_id,))


def _score_window(
    conn: sqlite3.Connection,
    site_id: int | None,
    window_key: str,
    cutoff: str | None,
    min_n: int,
) -> None:
    params: tuple[object, ...]
    where = ""
    if site_id is None:
        params = ()
    else:
        where = "WHERE site_id = ?"
        params = (site_id,)
    cells = conn.execute(
        f"""
        SELECT DISTINCT site_id, feed_id, variable, day_ahead
        FROM forecast_pairs
        {where}
        """,
        params,
    ).fetchall()
    now = isoformat_utc()
    for cell in cells:
        result = strategy_for(str(cell["variable"])).aggregate(
            conn,
            site_id=int(cell["site_id"]),
            feed_id=int(cell["feed_id"]),
            variable=str(cell["variable"]),
            day_ahead=int(cell["day_ahead"]),
            window_cutoff=cutoff,
            min_n=min_n,
        )
        if result.n == 0:
            continue
        upsert_score_cache(
            conn,
            site_id=int(cell["site_id"]),
            feed_id=int(cell["feed_id"]),
            variable=str(cell["variable"]),
            day_ahead=int(cell["day_ahead"]),
            window_key=window_key,
            result=result,
            computed_at=now,
        )
