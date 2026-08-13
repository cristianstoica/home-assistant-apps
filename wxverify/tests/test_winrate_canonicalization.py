"""Equivalence tests for the win-rate canonical-cell rewrite.

``_winrate_reference`` is an independently vendored reduction: it runs the
same eligibility predicate production still uses, then canonicalizes and
scores in Python with a plain max-issued_at dict reduction, rather than the
ROW_NUMBER()-based SQL canonicalization ``winrate`` now uses. Matching output
between the two across reissues, ties, meteoblue-member eligibility,
cross-site scoping, and window filtering pins the rewrite against the
scenarios most likely to make old and new diverge.

``_winrate_reference`` is deliberately frozen: it must never be updated to
track later changes in ``wxverify.scoring.winrate``. Its entire value as an
oracle comes from encoding the pre-rewrite behavior independently of
production; "fixing" it to match a subsequent production change would
silently destroy that independence and let a real regression pass
unnoticed. The one accepted exception is that it imports
``active_competitor_clause`` from production, so a change there moves both
sides together and is not caught by this file.

Synthetic data only (public repo): fake site names, the repo's existing
47/25 lat-lon convention, no real station or device identifiers.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.scoring.effective import active_competitor_clause
from wxverify.scoring.leaderboard import cutoff_for_window
from wxverify.scoring.winrate import winrate


@dataclass(frozen=True)
class _LegacyCanonicalCell:
    feed_id: int
    source: str
    model: str
    valid_at: str
    issued_at: str
    abs_error: float


@dataclass
class _LegacyFeedStats:
    source: str
    model: str
    covered: int = 0
    comparable: int = 0
    wins: float = 0.0


def _winrate_reference(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    variable: str,
    day_ahead: int,
    window: str = "rolling",
) -> list[dict[str, object]]:
    cutoff = cutoff_for_window(conn, window)
    window_clause = "" if cutoff is None else "AND fp.valid_at >= ?"
    params: list[object] = [site_id, variable, day_ahead]
    if cutoff is not None:
        params.append(cutoff)
    rows = conn.execute(
        f"""
        SELECT fp.feed_id, f.source, f.model, fp.valid_at, fp.issued_at,
               fp.abs_error
        FROM forecast_pairs fp
        JOIN feeds f ON f.id = fp.feed_id
        LEFT JOIN site_feed_state sfs
          ON sfs.site_id = fp.site_id AND sfs.feed_id = fp.feed_id
        WHERE fp.site_id = ?
          AND fp.variable = ?
          AND fp.day_ahead = ?
          AND fp.abs_error IS NOT NULL
          {window_clause}
          AND {active_competitor_clause(site_expr="fp.site_id")}
        ORDER BY fp.valid_at, fp.feed_id, fp.issued_at
        """,
        tuple(params),
    ).fetchall()
    canonical: dict[tuple[int, str], _LegacyCanonicalCell] = {}
    stats: dict[int, _LegacyFeedStats] = {}
    for row in rows:
        feed_id = int(row["feed_id"])
        cell = _LegacyCanonicalCell(
            feed_id=feed_id,
            source=str(row["source"]),
            model=str(row["model"]),
            valid_at=str(row["valid_at"]),
            issued_at=str(row["issued_at"]),
            abs_error=float(row["abs_error"]),
        )
        stats.setdefault(
            feed_id, _LegacyFeedStats(source=cell.source, model=cell.model)
        )
        key = (feed_id, cell.valid_at)
        previous = canonical.get(key)
        if previous is None or cell.issued_at > previous.issued_at:
            canonical[key] = cell

    cells_by_valid_at: dict[str, list[_LegacyCanonicalCell]] = {}
    for cell in canonical.values():
        stats[cell.feed_id].covered += 1
        cells_by_valid_at.setdefault(cell.valid_at, []).append(cell)

    for cells in cells_by_valid_at.values():
        if len(cells) < 2:
            continue
        best = min(cell.abs_error for cell in cells)
        winners = [cell for cell in cells if abs(cell.abs_error - best) <= 1e-9]
        credit = 1.0 / len(winners)
        winner_ids = {cell.feed_id for cell in winners}
        for cell in cells:
            feed_stats = stats[cell.feed_id]
            feed_stats.comparable += 1
            if cell.feed_id in winner_ids:
                feed_stats.wins += credit

    return [
        {
            "feed_id": feed_id,
            "source": feed_stats.source,
            "model": feed_stats.model,
            "covered": feed_stats.covered,
            "comparable": feed_stats.comparable,
            "wins": feed_stats.wins,
            "win_rate": None
            if feed_stats.comparable == 0
            else feed_stats.wins / feed_stats.comparable,
        }
        for feed_id, feed_stats in sorted(
            stats.items(),
            key=lambda item: (
                1.0
                if item[1].comparable == 0
                else -(item[1].wins / item[1].comparable),
                item[1].source,
                item[1].model,
                item[0],
            ),
        )
    ]


def _fresh_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _insert_site(conn: sqlite3.Connection, name: str) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES (?, 47, 25, 900, 'UTC')
            """,
            (name,),
        ).lastrowid
    )


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source=? AND model=?", (source, model)
    ).fetchone()
    assert row is not None
    return int(row["id"])


def _eligible_feed_ids(conn: sqlite3.Connection, n: int) -> list[int]:
    rows = conn.execute(
        """
        SELECT id FROM feeds
        WHERE is_virtual = 0
          AND NOT (source='meteoblue' AND model != 'multimodel')
        ORDER BY id
        LIMIT ?
        """,
        (n,),
    ).fetchall()
    assert len(rows) >= n
    return [int(row["id"]) for row in rows]


def _add_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    issued_at: str,
    valid_at: str,
    abs_error: float,
    variable: str = "temperature",
    day_ahead: int = 1,
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, error, abs_error, sq_error,
             tz_generation_id)
        VALUES (?, ?, ?, ?, ?, 24, ?, ?, 0.0, ?, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            day_ahead,
            abs_error,
            abs_error,
            abs_error,
            abs_error**2,
            ensure_published_generation(conn, site_id),
        ),
    )


def test_winrate_matches_reference_for_reissues_ties_and_sparse_cells() -> None:
    conn = _fresh_conn()
    site_id = _insert_site(conn, "WinrateEqReissue")
    fa, fb, fc = _eligible_feed_ids(conn, 3)

    # fa has TWO issued_at rows for the same valid_at (a reissue). The
    # correct canonical pick is the LATER issued_at (abs_error=2.0) even
    # though the EARLIER issued_at (abs_error=0.5) is numerically better.
    # fb's abs_error (1.0) sits strictly between fa's two candidates, so
    # whichever of fa's rows is picked as canonical decides the winner at
    # this valid_at: fb wins if the later (correct) row is picked, fa wins
    # if the earlier row is picked instead -- this is what actually
    # exercises the max-issued_at tie-break rather than a min-abs_error
    # shortcut or a reversed issued_at ordering.
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=fa,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        abs_error=0.5,
    )
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=fa,
        issued_at="2026-01-01T06:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        abs_error=2.0,
    )
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=fb,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        abs_error=1.0,
    )

    # An exact tie at a second valid_at between fa and fb.
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=fa,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-03T00:00:00Z",
        abs_error=1.0,
    )
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=fb,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-03T00:00:00Z",
        abs_error=1.0,
    )

    # fc is never compared against anything at any valid_at: covered=1,
    # comparable=0, win_rate=None.
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=fc,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-04T00:00:00Z",
        abs_error=9.0,
    )

    actual = winrate(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
    )
    expected = _winrate_reference(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
    )
    assert actual == expected

    by_feed = {row["feed_id"]: row for row in actual}
    assert by_feed[fa]["wins"] == 0.5  # loss at 01-02, half-credit tie at 01-03
    assert by_feed[fc]["covered"] == 1
    assert by_feed[fc]["comparable"] == 0
    assert by_feed[fc]["win_rate"] is None


def test_winrate_matches_reference_for_near_tie_and_differing_denominators() -> None:
    """Two corrections to the naive "just add a 3-way tie" reading:

    (1) A bare 3-way near-tie whose three credits (1/3 each) are summed
    within ONE group proves nothing about ORDER BY: three equal fractional
    credits sum to exactly the same float in every summation order, so it is
    just as order-insensitive as the pre-existing 2-way exact tie. It DOES
    still pin something else worth pinning: the 1e-9 tolerance predicate
    itself (winrate.py:108). f0/f1/f2 below are constructed to be close but
    NOT bit-identical -- an implementation using exact `==` instead of
    `abs(diff) <= 1e-9` would credit only f0 (the strict min) instead of
    splitting 1/3 to each.

    (2) What actually detects the `ORDER BY c.valid_at, c.feed_id` this
    module documents as "Load-bearing, not cosmetic": one feed (f0)
    accumulating credit from three DIFFERENT-sized tie groups (3-way, 7-way,
    2-way, in that chronological order) across three valid_at slots. 1/3,
    1/7 and 1/2 have no common representation short of the full sum, so
    float addition of these three specific values is order-dependent --
    verified separately that summing them in (1/3, 1/7, 1/2) order and in
    the fully-reversed (1/2, 1/7, 1/3) order differ in the last bit (the
    naive "reverse the tie sizes" pairing of 1/3, 1/2, 1/7 does NOT
    discriminate under reversal -- this specific assignment of tie-width to
    valid_at slot was chosen because it does). Groups are summed into `wins`
    in the order winrate() visits them, which is the row order the SQL
    returns -- so a build that reorders or drops the ORDER BY can make f0's
    total sum in a different order and come out bit-different from
    `_winrate_reference`, which always visits groups in (valid_at, feed_id)
    order by construction.
    """
    conn = _fresh_conn()
    site_id = _insert_site(conn, "WinrateEqNearTieDenoms")
    f0, f1, f2, f3, f4, f5, f6 = _eligible_feed_ids(conn, 7)

    # V1 (2026-02-01): a THREE-WAY near-tie, credit 1/3 each. Not
    # bit-identical (spaced 3e-10 apart), but within the 1e-9 tolerance.
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=f0,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-02-01T00:00:00Z",
        abs_error=10.0,
    )
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=f1,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-02-01T00:00:00Z",
        abs_error=10.0 + 3e-10,
    )
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=f2,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-02-01T00:00:00Z",
        abs_error=10.0 + 6e-10,
    )

    # V2 (2026-03-01): an exact SEVEN-WAY tie, credit 1/7 each.
    for feed_id in (f0, f1, f2, f3, f4, f5, f6):
        _add_pair(
            conn,
            site_id=site_id,
            feed_id=feed_id,
            issued_at="2026-01-01T00:00:00Z",
            valid_at="2026-03-01T00:00:00Z",
            abs_error=2.0,
        )

    # V3 (2026-04-01): an exact TWO-WAY tie, credit 1/2 each.
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=f0,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-04-01T00:00:00Z",
        abs_error=1.0,
    )
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=f3,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-04-01T00:00:00Z",
        abs_error=1.0,
    )

    actual = winrate(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
    )
    expected = _winrate_reference(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
    )
    assert actual == expected

    by_feed = {row["feed_id"]: row for row in actual}
    # f0 collects all three credits, in valid_at order: 1/3 + 1/7 + 1/2.
    assert by_feed[f0]["wins"] == (1.0 / 3.0 + 1.0 / 7.0 + 1.0 / 2.0)
    # f1 and f2 share f0's V1 near-tie credit despite not being
    # bit-identical to it, plus their V2 share.
    assert by_feed[f1]["wins"] == 1.0 / 3.0 + 1.0 / 7.0
    assert by_feed[f2]["wins"] == 1.0 / 3.0 + 1.0 / 7.0
    # f3 shares f0's V3 credit plus its V2 share.
    assert by_feed[f3]["wins"] == 1.0 / 7.0 + 1.0 / 2.0


def test_winrate_matches_reference_for_meteoblue_member_eligibility() -> None:
    conn = _fresh_conn()
    site_id = _insert_site(conn, "WinrateEqMeteoblue")
    # meteoblue member models are not among the default seeded feeds -- only
    # the multimodel package feed is -- so the member this test exercises is
    # hand-inserted here.
    member_id = int(
        conn.execute(
            "INSERT INTO feeds (source, model, fetch_interval_minutes)"
            " VALUES ('meteoblue', 'ecmwf', 360)"
        ).lastrowid
    )
    package_id = _feed_id(conn, "meteoblue", "multimodel")
    other_row = conn.execute(
        "SELECT id FROM feeds WHERE source != 'meteoblue' AND is_virtual = 0"
        " ORDER BY id LIMIT 1"
    ).fetchone()
    assert other_row is not None
    other_id = int(other_row["id"])

    _add_pair(
        conn,
        site_id=site_id,
        feed_id=member_id,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        abs_error=1.0,
    )
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=other_id,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        abs_error=2.0,
    )

    # Package subscribed for this site -> the member is eligible (a
    # meteoblue member's own subscription state never enters into this: only
    # the package's does) and its pair counts, matching the reference.
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, package_id),
    )
    subscribed = winrate(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
    )
    assert subscribed == _winrate_reference(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
    )
    assert any(row["feed_id"] == member_id for row in subscribed)

    # Package unsubscribed for this site -> the member drops out entirely,
    # in both implementations.
    conn.execute(
        "UPDATE site_feed_state SET enabled=0 WHERE site_id=? AND feed_id=?",
        (site_id, package_id),
    )
    unsubscribed = winrate(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
    )
    assert unsubscribed == _winrate_reference(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
    )
    assert not any(row["feed_id"] == member_id for row in unsubscribed)


def test_winrate_bind_order_is_not_confused_across_sites() -> None:
    conn = _fresh_conn()
    site_a = _insert_site(conn, "WinrateEqSiteA")
    site_b = _insert_site(conn, "WinrateEqSiteB")
    fa, fb = _eligible_feed_ids(conn, 2)

    _add_pair(
        conn,
        site_id=site_a,
        feed_id=fa,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        abs_error=1.0,
    )
    _add_pair(
        conn,
        site_id=site_a,
        feed_id=fb,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        abs_error=5.0,
    )
    # Same feed_ids, same valid_at, OPPOSITE winner at site B: if site_id
    # binding ever leaked across the two sites this would show up as a
    # mismatched winner or a merged/doubled row count.
    _add_pair(
        conn,
        site_id=site_b,
        feed_id=fa,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        abs_error=5.0,
    )
    _add_pair(
        conn,
        site_id=site_b,
        feed_id=fb,
        issued_at="2026-01-01T00:00:00Z",
        valid_at="2026-01-02T00:00:00Z",
        abs_error=1.0,
    )

    for site_id in (site_a, site_b):
        actual = winrate(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
        )
        expected = _winrate_reference(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
        )
        assert actual == expected

    a_result = {
        row["feed_id"]: row
        for row in winrate(
            conn, site_id=site_a, variable="temperature", day_ahead=1, window="all"
        )
    }
    b_result = {
        row["feed_id"]: row
        for row in winrate(
            conn, site_id=site_b, variable="temperature", day_ahead=1, window="all"
        )
    }
    assert a_result[fa]["wins"] == 1.0
    assert a_result[fb]["wins"] == 0.0
    assert b_result[fa]["wins"] == 0.0
    assert b_result[fb]["wins"] == 1.0


def test_winrate_window_filtering_matches_reference() -> None:
    conn = _fresh_conn()
    site_id = _insert_site(conn, "WinrateEqWindow")
    fa, fb = _eligible_feed_ids(conn, 2)

    # Future-dated rows always satisfy "valid_at >= cutoff" against the real
    # wall clock; past-dated rows never do -- so this needs no utc_now()
    # patching, mirroring test_winrate_applies_window_to_canonical_cells in
    # tests/test_m1_m5.py.
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=fa,
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-02T00:00:00Z",
        abs_error=1.0,
    )
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=fb,
        issued_at="2035-01-01T00:00:00Z",
        valid_at="2035-01-02T00:00:00Z",
        abs_error=2.0,
    )
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=fa,
        issued_at="2020-01-01T00:00:00Z",
        valid_at="2020-01-02T00:00:00Z",
        abs_error=9.0,
    )
    _add_pair(
        conn,
        site_id=site_id,
        feed_id=fb,
        issued_at="2020-01-01T00:00:00Z",
        valid_at="2020-01-02T00:00:00Z",
        abs_error=9.0,
    )

    for window in ("1d", "all"):
        actual = winrate(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window=window
        )
        expected = _winrate_reference(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window=window
        )
        assert actual == expected

    windowed = {
        row["feed_id"]: row
        for row in winrate(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window="1d"
        )
    }
    assert windowed[fa]["covered"] == 1
    assert windowed[fb]["covered"] == 1

    unwindowed = {
        row["feed_id"]: row
        for row in winrate(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window="all"
        )
    }
    assert unwindowed[fa]["covered"] == 2
    assert unwindowed[fb]["covered"] == 2
