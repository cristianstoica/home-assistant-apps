"""Read-side leaderboard and curve queries."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from wxverify.core.timeutil import window_cutoff
from wxverify.db.snapshot import read_snapshot
from wxverify.settings.keys import get_number_setting

LeaderboardStatus = Literal["hit", "empty", "live"]


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
    status: LeaderboardStatus
    window_key: str
    window_days: int | None
    #: The cutoff and id of the publication read (9.6); None with no
    #: publication.
    evidence_through: date | None = None
    publication_id: int | None = None


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
    """Return leaderboard rows plus a status for the resolved window.

    The verdict of ``leaderboard_with_status_in_transaction``. Every read it
    depends on runs inside one WAL read snapshot (``read_snapshot``), so the
    rows describe one state of the database. The block is read-only. A
    caller that already holds a transaction on ``conn`` must call
    ``leaderboard_with_status_in_transaction`` instead, because the snapshot
    refuses to nest: ``forecast_ranking_with_status`` when a forecast route's
    read snapshot is open.
    """
    with read_snapshot(conn, label="leaderboard"):
        return leaderboard_with_status_in_transaction(
            conn,
            site_id=site_id,
            variable=variable,
            day_ahead=day_ahead,
            window=window,
        )


def leaderboard_with_status_in_transaction(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str,
) -> LeaderboardResult:
    """``leaderboard_with_status`` for a caller that already holds the snapshot.

    Identical verdict logic; issues no transaction of its own and requires
    none. ``forecast_ranking_with_status`` calls it when a forecast route's
    read snapshot is open on ``conn``, where the facade's ``read_snapshot``
    would refuse to nest, and ``leaderboard_in_snapshot`` wraps it. Other
    read paths call the facade.

    The verdict comes from the active publication (9.2): no publication, or
    the site disabled, is ``empty``; ``rolling`` and ``all`` are the
    publication's ``w:{N}`` and ``w:all`` rows, ``hit``; an ``Nd`` window is
    summed live over the N closed dates ending at the publication's cutoff,
    ``live``.
    """
    return _published_leaderboard(
        conn,
        site_id=site_id,
        variable=variable,
        day_ahead=day_ahead,
        window=window,
    )


def leaderboard_in_snapshot(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str,
) -> LeaderboardResult:
    """``leaderboard_with_status`` for a response that reads several
    rankings inside its own snapshot (9.3: ``/curve`` and the dashboard).

    Raises ``RuntimeError`` when no transaction is open on ``conn``, so it
    can never read outside a caller-held snapshot; the facade serves every
    other read.
    """
    if not conn.in_transaction:
        raise RuntimeError("leaderboard_in_snapshot needs the caller's snapshot")
    return leaderboard_with_status_in_transaction(
        conn,
        site_id=site_id,
        variable=variable,
        day_ahead=day_ahead,
        window=window,
    )


def _published_leaderboard(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str,
) -> LeaderboardResult:
    """The verdict of ``leaderboard_with_status_in_transaction``."""
    # Deferred: the publication module imports this one (its rows are
    # ``LeaderboardRow``s) through ``forecast/data.py``.
    from wxverify.scoring.publication import (
        active_publication,
        live_ranking,
        published_ranking,
    )

    pub = (
        active_publication(conn, site_id=site_id, at_utc=None)
        if _site_enabled(conn, site_id)
        else None
    )
    if pub is None:
        resolved = resolve_window(conn, window)
        return LeaderboardResult(
            rows=[],
            status="empty",
            window_key=resolved.window_key,
            window_days=resolved.window_days,
        )
    live_days = _live_window_days(window)
    if live_days is not None:
        live = live_ranking(
            conn,
            pub=pub,
            variable=variable,
            day_ahead=day_ahead,
            window_days=live_days,
        )
        return LeaderboardResult(
            rows=list(live.values()),
            status="live",
            window_key=f"live:{live_days}d",
            window_days=live_days,
            evidence_through=pub.cutoff,
            publication_id=pub.id,
        )
    window_key, window_days = (
        ("w:all", None)
        if window == "all"
        else (f"w:{pub.window_days}", pub.window_days)
    )
    published = published_ranking(
        conn,
        pub=pub,
        variable=variable,
        day_ahead=day_ahead,
        window_key=window_key,
    )
    return LeaderboardResult(
        rows=list(published.values()),
        status="hit",
        window_key=window_key,
        window_days=window_days,
        evidence_through=pub.cutoff,
        publication_id=pub.id,
    )


def _live_window_days(window: str) -> int | None:
    """N for an ``Nd`` window token, else None (``resolve_window``'s test)."""
    if window.endswith("d") and window[:-1].isdigit():
        return int(window[:-1])
    return None


def _site_enabled(conn: sqlite3.Connection, site_id: int) -> bool:
    row = conn.execute("SELECT enabled FROM sites WHERE id=?", (site_id,)).fetchone()
    return row is not None and bool(row["enabled"])


def score_badge(raw: float | None) -> int | None:
    if raw is None:
        return None
    return round(max(0.0, raw) * 100)


def below_baseline(raw: float | None) -> bool:
    return raw is not None and raw < 0


def resolve_window(
    conn: sqlite3.Connection, window: str, *, now: datetime | None = None
) -> WindowResolution:
    """Resolve a window token; ``now`` anchors relative cutoffs (default: now)."""
    if window == "all":
        return WindowResolution(
            window_key="w:all", window_days=None, cutoff=None, cache_backed=True
        )
    if window == "rolling":
        days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
        return WindowResolution(
            window_key=f"w:{days}",
            window_days=days,
            cutoff=window_cutoff(days, now),
            cache_backed=True,
        )
    if window.endswith("d") and window[:-1].isdigit():
        days = int(window[:-1])
        return WindowResolution(
            window_key=f"live:{days}d",
            window_days=days,
            cutoff=window_cutoff(days, now),
            cache_backed=False,
        )
    days = get_number_setting(conn, "rolling_window_days", 30, minimum=1)
    return WindowResolution(
        window_key=f"w:{days}",
        window_days=days,
        cutoff=window_cutoff(days, now),
        cache_backed=True,
    )
