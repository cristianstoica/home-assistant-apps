"""As-of sample reconstruction — implementer tests.

The daily close's day_scores rows are the only scoring path, so there is
no as-of pair-knowability query to cover here.

What this file covers:

* ``resolve_window(now=T)`` anchors relative cutoffs at T, not wall clock
  (``scoring.leaderboard``, still live under the publication-window path).

All fixture values are synthetic.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta

from wxverify.core.timeutil import isoformat_utc, parse_utc, utc_now
from wxverify.db.migrations import run_migrations
from wxverify.scoring.leaderboard import resolve_window

# Canonical decision time for these fixtures.
_T = "2035-01-02T12:00:00Z"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


# ---------------------------------------------------------------------------
# resolve_window(now=T)
# ---------------------------------------------------------------------------


def test_resolve_window_now_anchors_relative_cutoffs_at_t() -> None:
    conn = _conn()
    t = parse_utc(_T)
    assert resolve_window(conn, "7d", now=t).cutoff == isoformat_utc(
        t - timedelta(days=7)
    )
    # Default rolling window (30 d) anchored at T.
    assert resolve_window(conn, "rolling", now=t).cutoff == isoformat_utc(
        t - timedelta(days=30)
    )
    # "all" has no cutoff regardless of the anchor.
    assert resolve_window(conn, "all", now=t).cutoff is None


def test_resolve_window_default_now_is_wall_clock() -> None:
    conn = _conn()
    before = utc_now() - timedelta(days=7, minutes=1)
    cutoff = resolve_window(conn, "7d").cutoff
    after = utc_now() - timedelta(days=7) + timedelta(minutes=1)
    assert cutoff is not None
    assert before < parse_utc(cutoff) < after
