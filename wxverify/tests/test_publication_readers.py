"""Readers (daily-close plan 2026-10-22-daily-close-pairs-and-publication.md,
section 9; test IDs RD1-RD17, plan 18.8).

Written before P4's reader surface exists. Most tests below fail at a
``from wxverify.scoring.publication import ...`` with ``ModuleNotFoundError``
(``Publication``, ``active_publication``, ``published_ranking``,
``published_coverage``, ``pending_ranking_settings`` and ``closed_day_pairs``
are guessed to live in ``wxverify.scoring.publication`` -- the plan names their
signatures (16.1/16.2/9.1) but not their module for every one of them; this is
the plainest reading, named in the test-authoring report). The ones that
import only existing names (``leaderboard_with_status_in_transaction``,
``composite_with_status``, ``winrate``, ``load_wind_serving``,
``load_future_samples``, ``forecast_fingerprint``) instead fail at a
``sqlite3.OperationalError: no such table`` when they seed P4's own tables
by hand (not yet migrated), or at an assertion once the table exists but the
close-on reader branch does not -- both failures Step 3 accepts.

Real SQLite throughout (``tests.helpers.day_close_site``/``asof_conn``, real
migrations); nothing in the code under test is mocked. Synthetic data only:
station ids via ``day_close_site`` (``ISTATION01``/``ISTATION02``, and
``ISTATION03``/``ISTATION04`` for a second site, RD17), feed sources
``synthetic-rd-x``/``synthetic-rd-y``/``synthetic-rd-z``, zone ``UTC``, site
name ``Testsite``.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import D, asof_conn, day_close_site, note_day_clock
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.settings.keys import set_setting

# ---------------------------------------------------------------------------
# Seed helpers (local to this file -- tests/helpers.py is never edited).
# ---------------------------------------------------------------------------

_D1 = D - timedelta(days=1)
_D2 = D - timedelta(days=2)
_D3 = D - timedelta(days=3)
_D4 = D - timedelta(days=4)
_D5 = D - timedelta(days=5)


def _seed_feed(conn, name: str) -> int:
    source = f"synthetic-rd-{name.lower()}"
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES (?, ?, 1, 0, 360, 168, 0)",
        (source, source),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _subscribe(conn, site_id: int, feed_id: int, *, enabled: int = 1) -> None:
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, ?, 0)",
        (site_id, feed_id, enabled),
    )


def _set_enabled(conn, site_id: int, feed_id: int, enabled: int) -> None:
    conn.execute(
        "UPDATE site_feed_state SET enabled = ? WHERE site_id = ? AND feed_id = ?",
        (enabled, site_id, feed_id),
    )


def _seed_publication(
    conn,
    *,
    site_id: int,
    cutoff,
    window_days: int = 5,
    min_n: int = 3,
    first_record_date=None,
    published_at: str | None = None,
) -> int:
    first_record_date = first_record_date or cutoff
    window_start = cutoff - timedelta(days=window_days - 1)
    now = published_at or isoformat_utc()
    cur = conn.execute(
        """
        INSERT INTO publications
            (site_id, cutoff, first_record_date, window_days,
             window_start_date, wind_start_date, settings,
             methodology_version, scoring_rule, wind_method, wind_constants,
             build_started_at, published_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 4,
                'first-stored-download-of-local-day/1', 'persistence_blend',
                '{}', ?, ?)
        """,
        (
            site_id,
            cutoff.isoformat(),
            first_record_date.isoformat(),
            window_days,
            window_start.isoformat(),
            window_start.isoformat(),
            json.dumps({"rolling_window_days": window_days, "min_n": min_n}),
            now,
            now,
        ),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_ranking(
    conn,
    *,
    publication_id: int,
    feed_id: int,
    variable: str,
    day_ahead: int,
    window_key: str,
    n: int,
    skill_score: float | None,
    bias: float | None = None,
    mae: float | None = None,
    rmse: float | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO publication_rankings
            (publication_id, feed_id, variable, day_ahead, window_key, n,
             bias, mae, rmse, pod, far, csi, ets, hss, skill_score)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, ?)
        """,
        (
            publication_id,
            feed_id,
            variable,
            day_ahead,
            window_key,
            n,
            bias,
            mae,
            rmse,
            skill_score,
        ),
    )


def _seed_wind_weight(
    conn,
    *,
    publication_id: int,
    feed_id: int,
    day_ahead: int,
    mae_ms: float,
    days: int,
    weight: float,
) -> None:
    conn.execute(
        """
        INSERT INTO publication_wind_weights
            (publication_id, feed_id, day_ahead, mae_ms, days, weight)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (publication_id, feed_id, day_ahead, mae_ms, days, weight),
    )


def _seed_day_score(
    conn,
    *,
    site_id: int,
    local_date,
    feed_id: int,
    variable: str = "temperature",
    day_ahead: int = 1,
    status: str = "scored",
    n: int = 1,
    err_n: int = 1,
    err_sum: float = 2.0,
    abs_sum: float = 2.0,
    sq_sum: float = 4.0,
) -> None:
    conn.execute(
        """
        INSERT INTO day_scores
            (site_id, local_date, feed_id, variable, day_ahead, status,
             download_id, download_date, fetched_at, n, err_n, err_sum,
             abs_sum, sq_sum, hits, false_alarms, misses, correct_negs,
             skill_n, skill_sq_sum, skill_ref_sq_sum, wind_fc_high,
             wind_fc_hours, wind_obs_high)
        VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, ?, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """,
        (
            site_id,
            local_date.isoformat(),
            feed_id,
            variable,
            day_ahead,
            status,
            n,
            err_n,
            err_sum,
            abs_sum,
            sq_sum,
        ),
    )


def _seed_forecast_history(
    conn,
    *,
    site_id: int,
    local_date,
    feed_id: int,
    variable: str,
    day_ahead: int,
    valid_at: str,
    lead_hours: int,
    forecast: float,
    observed: float,
    issued_at: str | None = None,
) -> None:
    """A ``forecast_history`` row plus the ``observations`` row it joins to
    (storage-release plan 3.1, 3.6). ``observations`` is keyed on
    ``(site_id, variable, valid_at)`` alone -- one shared truth value, never
    per feed -- so a second call at the same ``valid_at`` (a second feed's
    row) must pass the same ``observed`` and is a silent no-op on that
    insert (``INSERT OR IGNORE``)."""
    conn.execute(
        """
        INSERT INTO forecast_history
            (site_id, feed_id, variable, day_ahead, valid_at, local_date,
             value, lead_hours, issued_at, contributors)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            site_id,
            feed_id,
            variable,
            day_ahead,
            valid_at,
            local_date.isoformat(),
            forecast,
            lead_hours,
            issued_at or valid_at,
        ),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO observations
            (site_id, variable, valid_at, value, n_stations, computed_at)
        VALUES (?, ?, ?, ?, 3, ?)
        """,
        (site_id, variable, valid_at, observed, valid_at),
    )


def _seed_download(
    conn,
    *,
    site_id: int,
    feed_id: int,
    local_date,
    slot: str,
    started_at: str,
    ended_at: str,
    fetched_at: str | None,
    labels: dict[str, list[str]],
    round_date=None,
    round_try: int | None = None,
    outcome: str = "stored",
    usable: int = 1,
    inserted: int = 1,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO forecast_downloads
            (site_id, feed_id, local_date, slot, round_date, round_try,
             started_at, ended_at, fetched_at, outcome, reason, usable,
             inserted, labels)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
        """,
        (
            site_id,
            feed_id,
            local_date.isoformat(),
            slot,
            round_date.isoformat() if round_date else None,
            round_try,
            started_at,
            ended_at,
            fetched_at,
            outcome,
            usable,
            inserted,
            json.dumps(labels),
        ),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _close_through_with_missed(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    first_date,
    last_closed_date,
    missed_date=None,
) -> None:
    """Closes every date from ``first_date`` to ``last_closed_date``
    inclusive, except ``missed_date``, which becomes ``missed`` via P1's
    own retry-exhaustion path (``begin_close`` then ``fail_close``, twice)
    in place of ``closed``. Dates are processed in order, as P1 requires
    (``_has_earlier_undone``, ``db/site_days.py:757``): a ``missed`` date
    counts as done, so the date after it still closes. ``missed_date``
    defaults to None: every date closes.

    Copied from ``tests/test_publication_build.py:1476``
    (``_close_through_with_missed``) rather than imported -- a cross-test-
    module import of a private, underscore-named helper between two
    independent reader/build test files is awkward and this file's own
    seed helpers are already declared local (module docstring); kept
    behaviorally identical to the original (RD4 (ii)/RD5 (ii), plan 18.8).
    """
    from wxverify.db.site_days import (
        begin_close,
        ensure_site_days,
        fail_close,
        finish_close,
        start_site_days,
    )

    start_dt = datetime(first_date.year, first_date.month, first_date.day, tzinfo=UTC)
    start_site_days(conn, site_id, first_date, now_utc=start_dt)
    far_future = note_day_clock(
        datetime(
            last_closed_date.year,
            last_closed_date.month,
            last_closed_date.day,
            tzinfo=UTC,
        )
        + timedelta(days=3)
    )
    ensure_site_days(conn, site_id, now_utc=far_future)
    d = first_date
    while d <= last_closed_date:
        row = conn.execute(
            "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
            (site_id, d.isoformat()),
        ).fetchone()
        assert row is not None
        t = parse_utc(row["day_end_utc"]) + timedelta(minutes=5)
        if d == missed_date:
            begin_close(conn, site_id, d, now_utc=t)
            fail_close(conn, site_id, d, now_utc=t, reason="rd_ii synthetic retry 1")
            t2 = t + timedelta(hours=1, minutes=1)
            begin_close(conn, site_id, d, now_utc=t2)
            fail_close(conn, site_id, d, now_utc=t2, reason="rd_ii synthetic retry 2")
        else:
            begin_close(conn, site_id, d, now_utc=t)
            finish_close(conn, site_id, d, now_utc=t)
        d += timedelta(days=1)


def _rolling_fixture(conn):
    """The 18.8 shared default (plan row, :897): close on; D-3 to D-1
    closed through P1's ``begin_close``/``finish_close`` (``day_close_site``
    ``with_records=False``, then this file's own
    ``_close_through_with_missed`` with no missed date) and D open --
    ``day_close_site``'s own ``with_records=True`` records would leave D-1
    open and D-2 missed, which no publication for D-1 can follow; a
    publication for D-1 with ``first_record_date`` D-3 (the site's first
    record), ``window_days=5``/``min_n=3`` while the saved settings are
    7/10; its rows put X above Y.
    """
    site_id = day_close_site(conn, with_records=False)
    _close_through_with_missed(conn, site_id, first_date=_D3, last_closed_date=_D1)
    x = _seed_feed(conn, "X")
    y = _seed_feed(conn, "Y")
    _subscribe(conn, site_id, x)
    _subscribe(conn, site_id, y)
    set_setting(conn, "rolling_window_days", "7")
    set_setting(conn, "min_n", "10")
    pub_id = _seed_publication(
        conn,
        site_id=site_id,
        cutoff=_D1,
        window_days=5,
        min_n=3,
        first_record_date=_D3,
    )
    _seed_ranking(
        conn,
        publication_id=pub_id,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        window_key="w:5",
        n=3,
        skill_score=0.8,
    )
    _seed_ranking(
        conn,
        publication_id=pub_id,
        feed_id=y,
        variable="temperature",
        day_ahead=1,
        window_key="w:5",
        n=5,
        skill_score=0.3,
    )
    conn.commit()
    return site_id, x, y, pub_id


# ---------------------------------------------------------------------------
# RD1
# ---------------------------------------------------------------------------


def test_rd1_leaderboard_rolling_reads_publication_min_n_not_saved() -> None:
    """RD1: with the close on, ``rolling`` reads the active publication's
    rows and takes ``confident`` from the PUBLICATION's ``min_n`` (3), not
    the saved setting (10); status ``hit``; ``evidence_through`` the
    publication's cutoff (9.2)."""
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    conn = asof_conn()
    site_id, x, y, pub_id = _rolling_fixture(conn)

    result = leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )

    assert result.status == "hit"
    feed_ids = [row.feed_id for row in result.rows]
    assert feed_ids[0] == x, (
        f"the publication ranks X above Y (skill 0.8 vs 0.3) -- got {feed_ids!r}"
    )
    x_row = next(row for row in result.rows if row.feed_id == x)
    assert x_row.confident is True, (
        "M79 -> at this assertion: correct = True (X's n=3 meets the"
        " PUBLICATION's min_n=3), mutant M79 (confident from the saved"
        " min_n=10) = False (3 < 10)"
    )
    assert result.evidence_through == _D1
    assert result.publication_id == pub_id


def test_rd1_composite_rolling_reads_publication_not_score_cache() -> None:
    """RD1 (second reader): ``composite_with_status``'s ``rolling`` window
    takes the same branch (9.2)."""
    from wxverify.scoring.composite import composite_with_status

    conn = asof_conn()
    site_id, x, y, _pub_id = _rolling_fixture(conn)

    result = composite_with_status(conn, site_id=site_id, window="rolling")

    assert result.status == "hit"
    feed_ids = {row.get("feed_id") for row in result.rows}
    assert x in feed_ids and y in feed_ids, (
        "M79 -> at this assertion: correct = both X and Y present (read"
        f" from the publication's rows), mutant = {feed_ids!r}"
    )


# ---------------------------------------------------------------------------
# RD2
# ---------------------------------------------------------------------------


def test_rd2_feed_toggle_changes_the_read_with_no_new_publication() -> None:
    """RD2: switching Y off removes its row from the next read; switching
    it back on restores it -- both acting at once, through the SAME
    publication (no new row written, 9.1)."""
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    conn = asof_conn()
    site_id, x, y, pub_id = _rolling_fixture(conn)

    before = conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0]

    _set_enabled(conn, site_id, y, 0)
    conn.commit()
    result_off = leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )
    feed_ids_off = {row.feed_id for row in result_off.rows}
    assert y not in feed_ids_off, (
        "M80 -> at this assertion: correct = Y absent once switched off"
        " (the subscription is applied at READ time, 9.1), mutant M80 (the"
        " subscription applied at build time) = Y still present -- got"
        f" {feed_ids_off!r}"
    )
    assert x in feed_ids_off

    _set_enabled(conn, site_id, y, 1)
    conn.commit()
    result_on = leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )
    feed_ids_on = {row.feed_id for row in result_on.rows}
    assert y in feed_ids_on, (
        "M80 -> at this assertion: correct = Y present again once switched"
        " back on (read-time subscription), mutant M80 (subscription fixed"
        f" at build time) = Y stays absent -- got {feed_ids_on!r}"
    )

    after = conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
    assert after == before == 1, "no new publication should have been written"
    assert result_on.publication_id == pub_id == result_off.publication_id


# ---------------------------------------------------------------------------
# RD3
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("window", ["rolling", "all", "3d"])
def test_rd3_no_publication_is_empty(window: str) -> None:
    """RD3 (i): with the close on and no publication row at all, every
    window answers ``empty`` (9.2).

    Live data a wrong fallback could surface is seeded first --
    ``day_scores`` rows across D-2 to D (so an ``Nd`` view that forgot the
    no-publication gate would still find rows to sum for ``3d``). Without
    this seed the row would pass "empty" merely because there was nothing
    at all to read -- a vacuous pass that could not distinguish P4's
    no-publication rule from an ordinary empty database."""
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    conn = asof_conn()
    site_id = day_close_site(conn, with_records=True)
    x = _seed_feed(conn, "X")
    _subscribe(conn, site_id, x)
    for day in (D - timedelta(days=2), D - timedelta(days=1), D):
        _seed_day_score(conn, site_id=site_id, local_date=day, feed_id=x, n=5)
    conn.commit()

    result = leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window=window
    )
    assert result.status == "empty", (
        "M81 -> at this assertion: correct = 'empty' (no active publication:"
        " the no-input gate runs before any window-specific read, so the"
        " seeded day_scores rows must never surface, 9.2), mutant M81 (the"
        " leaderboard with no publication falls back to a live read) = the"
        f" seeded live row would be read instead -- got {result.status!r}"
    )
    assert result.rows == []


@pytest.mark.parametrize("window", ["rolling", "all", "3d"])
def test_rd3_site_disabled_is_empty_even_with_a_publication(window: str) -> None:
    """RD3 (ii): a publication exists, but the site is disabled -- still
    ``empty`` for every window (9.2's no-input gate runs first)."""
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    conn = asof_conn()
    site_id, x, y, _pub_id = _rolling_fixture(conn)
    conn.execute("UPDATE sites SET enabled = 0 WHERE id = ?", (site_id,))
    conn.commit()

    result = leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window=window
    )
    assert result.status == "empty"
    assert result.rows == []


# ---------------------------------------------------------------------------
# RD4
# ---------------------------------------------------------------------------


def test_rd4_nd_window_sums_only_the_closed_dates_ending_at_cutoff_not_today() -> None:
    """RD4 (i): ``window="3d"`` sums only the 3 dates ending at the
    publication's cutoff D-1 -- D-3, D-2, D-1 -- never D, even once D
    itself closes AFTER the publication is built (its stage 7 not run
    yet, plan row RD4, :904). ``day_scores`` seeded at n=1 on D-3/D-2/D-1
    and n=100 on D (9.2)."""
    from wxverify.db.site_days import begin_close, finish_close
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    conn = asof_conn()
    site_id, x, _y, _pub_id = _rolling_fixture(conn)

    for day in (_D3, _D2, _D1):
        _seed_day_score(conn, site_id=site_id, local_date=day, feed_id=x, n=1)
    _seed_day_score(conn, site_id=site_id, local_date=D, feed_id=x, n=100)
    conn.commit()

    # D closes after the publication is built; the window must still end at
    # the publication's cutoff D-1, never at today.
    d_row = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, D.isoformat()),
    ).fetchone()
    assert d_row is not None
    t = parse_utc(d_row["day_end_utc"]) + timedelta(minutes=5)
    begin_close(conn, site_id, D, now_utc=t)
    finish_close(conn, site_id, D, now_utc=t)
    conn.commit()

    result = leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="3d"
    )

    assert result.status == "live"
    row = next(row for row in result.rows if row.feed_id == x)
    assert row.n == 3, (
        "M82 -> at this assertion: correct = 3 (D-3, D-2 and D-1 -- the 3"
        " dates ending at the publication's cutoff D-1, with D now closed"
        " too), mutant M82 (an Nd view ends at today instead of"
        " pub.cutoff) = 102 (the last 3 days ending today: D-2's n=1 +"
        " D-1's n=1 + D's n=100)"
    )


def test_rd4_ii_nd_window_excludes_a_missed_dates_leftover_day_scores() -> None:
    """RD4 (ii): D-2's record is ``missed`` (P1's ``begin_close`` then
    ``fail_close``, twice -- retry exhaustion, the same mechanism PB17 uses
    in ``test_publication_build.py``), its ``day_scores`` row left in
    place; the "3d" window (D-3 to D-1) must sum only D-3 and D-1, never
    D-2's leftover row (7.2, 9.2).

    A dedicated site/close-chain, not ``_rolling_fixture`` -- a cross-test
    ``missed`` state from a shared fixture would leave which mechanism
    produced it ambiguous, so this row builds its own site via P1's
    retry-exhaustion path directly.
    """
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    conn = asof_conn()
    site_id = day_close_site(conn, with_records=False)
    x = _seed_feed(conn, "X")
    _subscribe(conn, site_id, x)

    _close_through_with_missed(
        conn, site_id, first_date=_D3, last_closed_date=_D1, missed_date=_D2
    )
    pub_id = _seed_publication(
        conn,
        site_id=site_id,
        cutoff=_D1,
        window_days=5,
        min_n=1,
        first_record_date=_D3,
    )
    _seed_ranking(
        conn,
        publication_id=pub_id,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        window_key="w:5",
        n=3,
        skill_score=0.5,
    )

    for day in (_D3, _D2, _D1):
        _seed_day_score(conn, site_id=site_id, local_date=day, feed_id=x, n=1)
    conn.commit()

    result = leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="3d"
    )

    assert result.status == "live"
    row = next(row for row in result.rows if row.feed_id == x)
    assert row.n == 2, (
        "M152 -> at this assertion: correct = 2 (D-3's and D-1's n=1 rows"
        " only -- D-2 is missed, and 7.2/9.2 say every read of day_scores"
        " keeps only dates whose record is closed), mutant M152 (an Nd"
        " view sums day_scores without the record's closed state) = 3"
        " (D-2's leftover n=1 row wrongly included)"
    )


# ---------------------------------------------------------------------------
# RD5
# ---------------------------------------------------------------------------


def test_rd5_winrate_reads_forecast_history_closed_and_starting() -> None:
    """RD5: the win rate credits a win from a closed date's
    ``forecast_history`` row AND a win from a starting-history date's
    ``forecast_history`` row, inside the same (rolling, 5-date) window
    (9.2; storage-release plan A2/3.6 -- both rows now live in the one
    table, told apart only by the date's ``site_days`` state)."""
    from wxverify.scoring.winrate import winrate

    conn = asof_conn()
    site_id, x, y, _pub_id = _rolling_fixture(conn)

    # A closed date (D-1, inside the 5-date window) where X wins the hour.
    valid_closed = f"{_D1.isoformat()}T00:00:00Z"
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=_D1,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_closed,
        lead_hours=6,
        forecast=10.0,
        observed=10.0,
    )
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=_D1,
        feed_id=y,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_closed,
        lead_hours=6,
        forecast=12.0,
        observed=10.0,
    )

    # A starting-history date (D-5, before the site's first record D-3, but
    # still inside the 5-date rolling window) where Y wins the hour.
    valid_starting = f"{_D5.isoformat()}T00:00:00Z"
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=_D5,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_starting,
        lead_hours=6,
        forecast=10.0,
        observed=5.0,
        issued_at=f"{(_D5 - timedelta(hours=6)).isoformat()}T00:00:00Z",
    )
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=_D5,
        feed_id=y,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_starting,
        lead_hours=6,
        forecast=5.0,
        observed=5.0,
        issued_at=f"{(_D5 - timedelta(hours=6)).isoformat()}T00:00:00Z",
    )
    conn.commit()

    rows = winrate(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )
    wins_by_feed = {row["feed_id"]: row.get("wins", 0.0) for row in rows}

    assert wins_by_feed.get(x, 0.0) >= 1.0, (
        "the win rate must credit X's win from the CLOSED date's"
        f" forecast_history row (9.2) -- got {wins_by_feed!r}"
    )
    assert wins_by_feed.get(y, 0.0) >= 1.0, (
        "M83 -> at this assertion: correct = Y credited a win from the"
        " STARTING-HISTORY date's forecast_history row, mutant M83 (the"
        " win rate's starting part dropped) = Y gets no win from that date"
        f" -- got {wins_by_feed!r}"
    )


def test_rd5_ii_winrate_excludes_a_missed_dates_leftover_forecast_history() -> None:
    """RD5 (ii): as (i) -- X wins a closed date's hour, Y wins a
    starting-history date's hour, inside the same (rolling, 5-date)
    window -- plus a ``missed`` date inside the window (D-2, via P1's
    ``begin_close`` then ``fail_close``, twice) whose leftover
    ``forecast_history`` row gives Y a perfect, winning hour. Read, it
    would raise Y's win count to 2; excluded (7.2, 9.2), Y's count must
    stay 1, the same as (i).

    A dedicated site/close-chain, not ``_rolling_fixture``, for the same
    reason as RD4 (ii): a cross-test ``missed`` state from a shared
    fixture would leave which mechanism produced it ambiguous, so this
    row builds its own site via P1's retry-exhaustion path directly.
    """
    from wxverify.scoring.winrate import winrate

    conn = asof_conn()
    site_id = day_close_site(conn, with_records=False)
    x = _seed_feed(conn, "X")
    y = _seed_feed(conn, "Y")
    _subscribe(conn, site_id, x)
    _subscribe(conn, site_id, y)

    _close_through_with_missed(
        conn, site_id, first_date=_D3, last_closed_date=_D1, missed_date=_D2
    )
    _seed_publication(
        conn,
        site_id=site_id,
        cutoff=_D1,
        window_days=5,
        min_n=1,
        first_record_date=_D3,
    )

    # A closed date (D-1, inside the 5-date window) where X wins the hour.
    valid_closed = f"{_D1.isoformat()}T00:00:00Z"
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=_D1,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_closed,
        lead_hours=6,
        forecast=10.0,
        observed=10.0,
    )
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=_D1,
        feed_id=y,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_closed,
        lead_hours=6,
        forecast=12.0,
        observed=10.0,
    )

    # A starting-history date (D-5, before the site's first record D-3, but
    # still inside the 5-date rolling window) where Y wins the hour.
    valid_starting = f"{_D5.isoformat()}T00:00:00Z"
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=_D5,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_starting,
        lead_hours=6,
        forecast=10.0,
        observed=5.0,
        issued_at=f"{(_D5 - timedelta(hours=6)).isoformat()}T00:00:00Z",
    )
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=_D5,
        feed_id=y,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_starting,
        lead_hours=6,
        forecast=5.0,
        observed=5.0,
        issued_at=f"{(_D5 - timedelta(hours=6)).isoformat()}T00:00:00Z",
    )

    # The missed D-2's leftover forecast_history: Y perfect (would win the
    # hour if read), X well off. Left in place, as 7.2 describes a missed
    # date's rows ("left in place") -- the record's own `closed` state is
    # the only thing that must exclude them.
    valid_missed = f"{_D2.isoformat()}T00:00:00Z"
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=_D2,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_missed,
        lead_hours=6,
        forecast=20.0,
        observed=10.0,
    )
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=_D2,
        feed_id=y,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_missed,
        lead_hours=6,
        forecast=10.0,
        observed=10.0,
    )
    conn.commit()

    rows = winrate(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )
    wins_by_feed = {row["feed_id"]: row.get("wins", 0.0) for row in rows}

    assert wins_by_feed.get(x, 0.0) == 1.0, (
        "X's only win is D-1's closed-date hour; D-2's leftover loss for X"
        f" must not change this -- got {wins_by_feed!r}"
    )
    assert wins_by_feed.get(y, 0.0) == 1.0, (
        "M153 -> at this assertion: correct = 1.0 (Y's win from the"
        " STARTING-HISTORY D-5 hour only; D-2's leftover forecast_history"
        " row is excluded because its record is missed, 7.2/9.2), mutant"
        " M153 (the win rate reads forecast_history without the record's"
        " closed state) = 2.0 (D-2's leftover perfect Y row wrongly also"
        f" counted) -- got {wins_by_feed!r}"
    )


# ---------------------------------------------------------------------------
# RD6
# ---------------------------------------------------------------------------


def test_rd6_wind_serving_filters_by_subscription_then_empty() -> None:
    """RD6: ``load_wind_serving`` reads the active publication's weights,
    dropping a switched-off feed's row; with no publication it returns
    ``{}`` (9.4)."""
    from wxverify.forecast.wind_blend import load_wind_serving

    conn = asof_conn()
    site_id, x, y, pub_id = _rolling_fixture(conn)
    _seed_wind_weight(
        conn,
        publication_id=pub_id,
        feed_id=x,
        day_ahead=1,
        mae_ms=1.0,
        days=25,
        weight=0.6,
    )
    _seed_wind_weight(
        conn,
        publication_id=pub_id,
        feed_id=y,
        day_ahead=1,
        mae_ms=2.0,
        days=20,
        weight=0.4,
    )
    conn.commit()

    serving = load_wind_serving(conn, site_id=site_id, timezone="UTC", today=D)
    keys = set(serving.weights.keys())
    assert (x, 1) in keys and (y, 1) in keys, (
        f"both feeds expected before disabling Y -- got {keys!r}"
    )

    _set_enabled(conn, site_id, y, 0)
    conn.commit()
    serving_off = load_wind_serving(conn, site_id=site_id, timezone="UTC", today=D)
    keys_off = set(serving_off.weights.keys())
    assert (y, 1) not in keys_off, (
        "M84 -> at this assertion: correct = Y's key absent once switched"
        " off, mutant M84 (wind weights not filtered by the subscription)"
        f" = Y's key still present -- got {keys_off!r}"
    )
    assert (x, 1) in keys_off

    # Paired positive / second half: no publication at all -> {}.
    conn2 = asof_conn()
    empty_site = day_close_site(conn2, with_records=True)
    conn2.commit()
    serving_empty = load_wind_serving(
        conn2, site_id=empty_site, timezone="UTC", today=D
    )
    assert serving_empty.weights == {}, (
        "with no active publication, load_wind_serving must answer with no"
        f" weights (9.4) -- got {serving_empty.weights!r}"
    )


# ---------------------------------------------------------------------------
# RD7
# ---------------------------------------------------------------------------


def _rd7_make_file_db(tmp_path: Path) -> tuple[sqlite3.Connection, str]:
    """A real, file-backed, WAL-mode SQLite database (own-helper, local to
    this file): RD7 needs a SECOND, independent connection whose commit is
    actually visible to the first -- ``asof_conn``'s ``:memory:`` database
    is private to its own connection, so seeding a publication through a
    second ``asof_conn()`` would land in an unrelated, empty database and
    never be at risk of leaking into the first connection's reads at all
    (a vacuous rendezvous). Mirrors ``tests/test_publication_build.py``'s
    own ``_make_db`` (own-helpers rule: not imported from it)."""
    from wxverify import config
    from wxverify.db.connection import close_db, init_db

    close_db()
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    return db._conn, str(db_path)  # noqa: SLF001 - test builds the fixture directly


def test_rd7_curve_response_reads_one_publication_version_throughout(
    tmp_path: Path,
) -> None:
    """RD7: one response built inside ONE ``read_snapshot`` -- the
    mechanism 9.3 names for ``/curve`` -- sees the SAME publication (D-1)
    at every leaderboard read inside it, even when a SECOND connection
    commits a newer (D) publication while that snapshot is held open (9.3).

    Deterministic rendezvous in place of a timing race: the spied function
    is ``leaderboard_with_status_in_transaction`` (``wxverify.scoring.
    leaderboard``) -- the per-day_ahead reader ``/curve``
    (``api/routes/dashboard.py:77-97``) must call once per lead, since its
    OWN snapshotting facade, ``leaderboard_with_status``, "refuses to nest"
    inside a caller-held snapshot such as ``/curve``'s one
    ``read_snapshot`` (the facade's own docstring). On the spy's
    FIRST call, a second, real connection to the SAME on-disk file commits
    a newer publication before the spy returns; the second call, still
    inside the first connection's one open snapshot, is the one put at
    risk. Every value in the response (each call's ``evidence_through``)
    must still come from the ORIGINAL (D-1) publication.
    """
    from wxverify.db.snapshot import read_snapshot
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    conn, db_path = _rd7_make_file_db(tmp_path)
    site_id, x, y, _pub_d1 = _rolling_fixture(conn)

    seen_cutoffs: list[object] = []
    committed = {"done": False}

    def _spy(*, day_ahead: int):
        result = leaderboard_with_status_in_transaction(
            conn,
            site_id=site_id,
            variable="temperature",
            day_ahead=day_ahead,
            window="rolling",
        )
        seen_cutoffs.append(result.evidence_through)
        if not committed["done"]:
            committed["done"] = True
            blocker = sqlite3.connect(db_path, timeout=0)
            try:
                _seed_publication(
                    blocker, site_id=site_id, cutoff=D, window_days=5, min_n=3
                )
                blocker.commit()
            finally:
                blocker.close()
        return result

    with read_snapshot(conn, label="rd7-test"):
        _spy(day_ahead=1)
        _spy(day_ahead=1)

    assert seen_cutoffs, "fixture bug: the spy must be called"
    assert len(set(seen_cutoffs)) == 1 and seen_cutoffs[0] == _D1, (
        "M85 -> at this assertion: correct = every call inside the one"
        " snapshot reports evidence_through D-1, the publication"
        " active when the snapshot opened (9.3: one read_snapshot"
        " per response), mutant M85 (/curve reads each leaderboard in its"
        " own snapshot) = the second call, no longer sharing the first"
        f" call's snapshot, would see the newly-committed D publication"
        f" instead -- got {seen_cutoffs!r}"
    )


def test_rd7_curve_route_itself_reads_one_publication_version_throughout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RD7, routed through the real production code: the test above proves
    the MECHANISM (a read_snapshot holds one publication version
    across calls inside it); it calls
    ``leaderboard_with_status_in_transaction`` directly inside a
    manually-opened ``read_snapshot`` of its own, so a mutant that
    changes what ``/curve`` itself does (M85: drops
    ``read_snapshot`` and reads each of its eight leads through
    ``leaderboard_with_status``, which opens its own snapshot per call)
    never runs any of the route's own code and cannot be seen by it.

    This test instead calls ``wxverify.api.routes.dashboard.curve`` itself
    (the actual ``/curve`` handler, as ``tests/test_write_lock_serialization
    .py`` calls ``leaderboard`` directly) against a real two-connection
    file database, with a second connection committing a newer (D)
    publication right after the FIRST of the eight leads is read -- the
    same rendezvous as above, but spliced into the route's own call to
    ``leaderboard_in_snapshot`` (``wxverify.api.routes.dashboard``'s own
    import, patched by name there), so the divergence is in the route's
    own code path, not a copy of it.
    """
    import wxverify.api.routes.dashboard as dashboard_module
    from wxverify.api.routes.dashboard import curve

    conn, db_path = _rd7_make_file_db(tmp_path)
    site_id, _x, _y, _pub_d1 = _rolling_fixture(conn)

    real_in_snapshot = dashboard_module.leaderboard_in_snapshot
    real_with_status = dashboard_module.leaderboard_with_status
    seen_cutoffs: list[object] = []
    committed = {"done": False}

    def _record(result) -> None:
        seen_cutoffs.append(result.evidence_through)
        if not committed["done"]:
            committed["done"] = True
            blocker = sqlite3.connect(db_path, timeout=0)
            try:
                _seed_publication(
                    blocker, site_id=site_id, cutoff=D, window_days=5, min_n=3
                )
                blocker.commit()
            finally:
                blocker.close()

    def _spy_in_snapshot(conn_arg, *, site_id, variable, day_ahead, window):
        result = real_in_snapshot(
            conn_arg,
            site_id=site_id,
            variable=variable,
            day_ahead=day_ahead,
            window=window,
        )
        _record(result)
        return result

    def _spy_with_status(conn_arg, *, site_id, variable, day_ahead, window):
        # Patched too: a mutant that calls this instead of
        # leaderboard_in_snapshot (M85's own change) must still be seen by
        # this oracle, not silently miss the spy altogether.
        result = real_with_status(
            conn_arg,
            site_id=site_id,
            variable=variable,
            day_ahead=day_ahead,
            window=window,
        )
        _record(result)
        return result

    monkeypatch.setattr(dashboard_module, "leaderboard_in_snapshot", _spy_in_snapshot)
    monkeypatch.setattr(dashboard_module, "leaderboard_with_status", _spy_with_status)

    asyncio.run(
        curve(site=site_id, variable="temperature", window="rolling", lead="D+1", top=5)
    )

    assert seen_cutoffs, (
        "fixture bug: the route must call leaderboard_in_snapshot or"
        " leaderboard_with_status for each of its 8 leads"
    )
    assert len(seen_cutoffs) == 8, (
        f"fixture bug: /curve reads 8 leads, got {len(seen_cutoffs)} spy calls"
    )
    assert len(set(seen_cutoffs)) == 1 and seen_cutoffs[0] == _D1, (
        "M85 -> at this assertion (the route's OWN eight calls, not a"
        " manual simulation of the mechanism): correct = every one of"
        " /curve's 8 leaderboard reads reports evidence_through D-1, the"
        " publication active when /curve's one read_snapshot"
        " opened (9.3), mutant M85 (/curve drops read_snapshot and"
        " reads each lead through leaderboard_with_status, its own"
        " snapshot per call) = the leads read after the first would see"
        f" the newly-committed D publication instead -- got {seen_cutoffs!r}"
    )


# ---------------------------------------------------------------------------
# RD8
# ---------------------------------------------------------------------------


def test_rd8_served_forecast_follows_the_publication_order_at_blend_depth_1() -> None:
    """RD8: read at D 08:00Z, with ``forecast_blend_depth=1``, the served
    temperature at D+1 is X's -- the publication's order, never a live
    recompute. X and Y each have a sample at every hour 00-23Z of D+1 (so
    both cover the whole local day and ``meta.feeds``, the extrema set,
    names the serving feed); ``_rolling_fixture``'s published ranking
    alone decides X over Y (skill 0.8 vs 0.3, both confident under the
    publication's own ``min_n=3``) -- no pair table or live recompute is
    read for serving any more (storage release, table 1.B)."""
    from wxverify.forecast.service import build_forecast

    conn = asof_conn()
    site_id, x, y, _pub_id = _rolling_fixture(conn)
    set_setting(conn, "forecast_blend_depth", "1")

    target_date = D + timedelta(days=1)
    issued_at = f"{D.isoformat()}T00:00:00Z"
    for feed_id, value in ((x, 20.0), (y, 10.0)):
        for hour in range(24):
            valid_at = f"{target_date.isoformat()}T{hour:02d}:00:00Z"
            conn.execute(
                """
                INSERT INTO forecast_samples
                    (site_id, feed_id, variable, issued_at, valid_at,
                     lead_hours, value, source_raw, model_run_id, fetched_at)
                VALUES (?, ?, 'temperature', ?, ?, 36, ?, '{}', 'run-rd8', ?)
                """,
                (site_id, feed_id, issued_at, valid_at, value, issued_at),
            )

    conn.commit()

    view = build_forecast(
        conn,
        site_id=site_id,
        timezone="UTC",
        rain_threshold_mm=0.2,
        now=datetime(D.year, D.month, D.day, 8, tzinfo=UTC),
    )
    tile = next(t for t in view.tiles if t.date_iso == target_date.isoformat())
    served_ids = [ref.feed_id for ref in tile.temp.meta.feeds]
    assert served_ids and served_ids[0] == x, (
        "M86 -> at this assertion: correct = X serves (the PUBLICATION's"
        " rankings rank X above Y, skill 0.8 vs 0.3, both confident under"
        " the publication's min_n=3), mutant M86 (serving reads"
        " score_cache with the close on) = Y serves (the w:7 score_cache"
        " has X n=3/Y n=5, neither confident under the SAVED min_n=10, so"
        " ranking falls back to highest n -- Y's 5 over X's 3) -- got"
        f" {served_ids!r}"
    )


# ---------------------------------------------------------------------------
# RD9
# ---------------------------------------------------------------------------


def test_rd9_download_date_counts_from_the_round_not_the_label() -> None:
    """RD9: a sample fetched by the round at D 00:10Z, whose run label is
    dated D-1, valid on D+1: ``load_future_samples`` reports
    ``download_date`` D (the round's receipt day), so day ahead counts as
    1, never 2 as the label's own date would give (9.5, :148)."""
    from wxverify.forecast.data import load_future_samples

    conn = asof_conn()
    site_id = day_close_site(conn, with_records=True)
    x = _seed_feed(conn, "X")
    _subscribe(conn, site_id, x)

    label = f"{_D1.isoformat()}T18:00:00Z"
    fetched_at = f"{D.isoformat()}T00:10:04Z"
    valid_at = f"{(D + timedelta(days=1)).isoformat()}T12:00:00Z"
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, 42, 15.0, '{}', 'run-rd9', ?)
        """,
        (site_id, x, label, valid_at, fetched_at),
    )
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=x,
        local_date=D,
        slot="round",
        round_date=D,
        round_try=1,
        started_at=f"{D.isoformat()}T00:10:00Z",
        ended_at=fetched_at,
        fetched_at=fetched_at,
        labels={str(x): [label]},
    )
    conn.commit()

    rows = load_future_samples(
        conn,
        site_id=site_id,
        since_valid_at=f"{D.isoformat()}T00:00:00Z",
        served_only=True,
    )
    row = next(r for r in rows if r.feed_id == x)
    got = getattr(row, "download_date", None)
    assert got == D, (
        "M87 -> at this assertion: correct = D (the round's own local_date,"
        " 9.5), mutant M87 (_candidates/load_future_samples keeps the"
        f" issue-time day ahead, from the label's D-1) -- got {got!r}"
    )


# ---------------------------------------------------------------------------
# RD10
# ---------------------------------------------------------------------------


def test_rd10_fingerprint_changes_when_a_new_publication_lands() -> None:
    """RD10: ``forecast_fingerprint`` differs before and after a new
    publication is committed (9.5)."""
    from wxverify.forecast.data import forecast_fingerprint

    conn = asof_conn()
    site_id, _x, _y, _pub_id = _rolling_fixture(conn)

    before = forecast_fingerprint(conn, site_id=site_id)
    _seed_publication(conn, site_id=site_id, cutoff=D, window_days=5, min_n=3)
    conn.commit()
    after = forecast_fingerprint(conn, site_id=site_id)

    assert before != after, (
        "M88 -> at this assertion: correct = different tokens (the"
        " fingerprint includes the active publication id, 9.5), mutant M88"
        " (the fingerprint built without the publication id) = identical"
        f" tokens {before!r}"
    )


# ---------------------------------------------------------------------------
# RD11
# ---------------------------------------------------------------------------


def test_rd11_pending_ranking_settings_compares_publication_to_saved_values() -> None:
    """RD11: ``pending_ranking_settings`` reports the (published, saved)
    pair for each of the two settings that differ, and ``{}`` for no
    publication (13.1). Paired positive/negative in one test."""
    from wxverify.scoring.publication import pending_ranking_settings

    conn = asof_conn()
    site_id, _x, _y, pub_id = _rolling_fixture(conn)

    from wxverify.scoring.publication import active_publication

    pub = active_publication(conn, site_id=site_id, at_utc=None)
    assert pub is not None and pub.id == pub_id

    pending = pending_ranking_settings(conn, pub=pub)
    assert pending == {"rolling_window_days": (5, 7), "min_n": (3, 10)}, (
        "M89 -> at this assertion: correct = {'rolling_window_days': (5, 7),"
        " 'min_n': (3, 10)} (published vs saved), mutant M89 (pending"
        " settings compared with the saved value twice) would show no"
        f" difference ({{}}) -- got {pending!r}"
    )

    assert pending_ranking_settings(conn, pub=None) == {}, (
        "paired negative: with pub=None there is nothing published to"
        " compare against, so the result must be empty"
    )


# ---------------------------------------------------------------------------
# RD12
# ---------------------------------------------------------------------------


def test_rd12_active_publication_resolves_by_published_at_not_by_id() -> None:
    """RD12: ``active_publication`` at a series of instants around two
    publications' ``published_at`` times resolves by TIME, not by the
    newest ``id`` -- a later-dated row with a HIGHER id but published
    AFTER both probe windows must never leak into them (7.5)."""
    from wxverify.scoring.publication import active_publication

    conn = asof_conn()
    site_id, _x, _y, pub_d1_id = _rolling_fixture(conn)
    # _rolling_fixture already wrote one publication (D-1); reuse its id as
    # t1 by giving it a known published_at, then add t2 (D) and a later-id,
    # later-cutoff (D-3) row published strictly after both.
    t1 = "2030-01-01T00:00:00.000Z"
    t2 = "2030-01-02T00:00:00.000Z"
    t3 = "2030-01-03T00:00:00.000Z"
    conn.execute(
        "UPDATE publications SET published_at = ? WHERE id = ?", (t1, pub_d1_id)
    )
    _seed_publication(
        conn, site_id=site_id, cutoff=D, window_days=5, min_n=3, published_at=t2
    )
    _seed_publication(
        conn,
        site_id=site_id,
        cutoff=_D3,
        window_days=5,
        min_n=3,
        published_at=t3,
    )
    conn.commit()

    def _one_sec_before(stamp: str) -> str:
        from wxverify.core.timeutil import isoformat_utc as _fmt
        from wxverify.core.timeutil import parse_utc as _parse

        return _fmt(_parse(stamp) - timedelta(seconds=1))

    def _one_sec_after(stamp: str) -> str:
        from wxverify.core.timeutil import isoformat_utc as _fmt
        from wxverify.core.timeutil import parse_utc as _parse

        return _fmt(_parse(stamp) + timedelta(seconds=1))

    cases = [
        (_one_sec_before(t1), None),
        (t1, _D1),
        (_one_sec_before(t2), _D1),
        (t2, D),
        (_one_sec_after(t2), D),
        (None, D),
    ]
    for at_utc, expected_cutoff in cases:
        pub = active_publication(conn, site_id=site_id, at_utc=at_utc)
        got_cutoff = None if pub is None else pub.cutoff
        assert got_cutoff == expected_cutoff, (
            "M90 -> at this assertion: correct = resolve by published_at"
            f" (7.5); at {at_utc!r} expected cutoff {expected_cutoff!r},"
            " mutant M90 (the active publication picked by id instead of"
            f" cutoff/published_at) would pick the highest-id row -- got {got_cutoff!r}"
        )


# ---------------------------------------------------------------------------
# RD14
# ---------------------------------------------------------------------------


def test_rd14_chart_pairs_returns_only_a_closed_dates_rows() -> None:
    """RD14: the history chart reads D-1's (closed) ``forecast_history``
    row, and omits D's -- open, but holding a leftover ``forecast_history``
    row, as a failed close attempt's stage 5 leaves it (plan row RD14,
    :913; 16.2). ``closed_day_pairs`` itself is gone (storage-release plan
    A3): its one caller, the history chart, now reads this through
    ``chart_pairs`` (A4/3.6), so this is where the fact it protected is
    checked."""
    from wxverify.scoring.history import chart_pairs

    conn = asof_conn()
    site_id, x, _y, _pub_id = _rolling_fixture(conn)
    valid_at = f"{_D1.isoformat()}T00:00:00Z"
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=_D1,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_at,
        lead_hours=6,
        forecast=10.0,
        observed=9.0,
    )
    # D is open, but a failed close attempt's stage 5 can leave a
    # forecast_history row for it in place anyway -- chart_pairs must
    # still omit an open date's rows, leftover row or not.
    valid_at_open = f"{D.isoformat()}T00:00:00Z"
    _seed_forecast_history(
        conn,
        site_id=site_id,
        local_date=D,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        valid_at=valid_at_open,
        lead_hours=6,
        forecast=12.0,
        observed=9.0,
    )
    conn.commit()

    closed_rows = chart_pairs(
        conn,
        site_id=site_id,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        low=parse_utc(valid_at),
        high=parse_utc(valid_at),
    )
    assert len(closed_rows) == 1 and closed_rows[0][0] == valid_at

    open_rows = chart_pairs(
        conn,
        site_id=site_id,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        low=parse_utc(valid_at_open),
        high=parse_utc(valid_at_open),
    )
    assert open_rows == [], (
        "M93 -> at this assertion: correct = [] for the open date D (its"
        " record is not closed, 16.2), mutant M93 (chart_pairs returns an"
        " open day's rows too) = a 1-row list holding the leftover"
        f" forecast_history row just seeded for D -- got {open_rows!r}"
    )


# ---------------------------------------------------------------------------
# RD15
# ---------------------------------------------------------------------------


def test_rd15_substitute_flag_set_only_when_a_stale_chosen_feed_is_left_out() -> None:
    """RD15: read at D 08:00Z. X is stale (``last_usable_fetch_at`` D-1
    08:00Z, more than twice its 360-minute interval before the read); Y
    is fresh (D 06:00Z). X's only future sample is at D+1 (day ahead 1,
    the publication's existing ranking already puts X above Y there); Y's
    samples reach D+1 and D+2. At day ahead 2 the publication ranks X
    (skill 0.8, n 3) above Y (0.3, n 5), but X has no D+2 sample, so
    fresh Y serves there instead -- the chosen feed (X) is stale and left
    out, so ``substitute`` is True. Feed Z (subscribed, fresh) supplies
    the only D+3 sample; Y, ranked first at day ahead 3 (skill 0.3, n 5,
    no row for Z there) but FRESH, is chosen-but-absent there too, yet
    ``substitute`` must stay False -- only a STALE chosen-but-absent feed
    sets it (plan row RD15, :914; 11.3)."""
    from wxverify.forecast.service import build_forecast

    conn = asof_conn()
    site_id, x, y, pub_id = _rolling_fixture(conn)
    z = _seed_feed(conn, "Z")
    _subscribe(conn, site_id, z)
    set_setting(conn, "forecast_blend_depth", "1")

    conn.execute(
        "UPDATE site_feed_state SET last_usable_fetch_at = ?"
        " WHERE site_id = ? AND feed_id = ?",
        (f"{_D1.isoformat()}T08:00:00Z", site_id, x),
    )
    conn.execute(
        "UPDATE site_feed_state SET last_usable_fetch_at = ?"
        " WHERE site_id = ? AND feed_id = ?",
        (f"{D.isoformat()}T06:00:00Z", site_id, y),
    )
    conn.execute(
        "UPDATE site_feed_state SET last_usable_fetch_at = ?"
        " WHERE site_id = ? AND feed_id = ?",
        (f"{D.isoformat()}T07:00:00Z", site_id, z),
    )

    d_plus_1 = D + timedelta(days=1)
    d_plus_2 = D + timedelta(days=2)
    d_plus_3 = D + timedelta(days=3)
    issued_x = f"{D.isoformat()}T00:00:00Z"
    issued_y = f"{D.isoformat()}T06:00:00Z"
    issued_z = f"{D.isoformat()}T07:00:00Z"
    # X: valid only through D+1 (stale: no sample reaches D+2).
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, 36, 20.0, '{}', 'run-rd15x', ?)
        """,
        (site_id, x, issued_x, f"{d_plus_1.isoformat()}T12:00:00Z", issued_x),
    )
    # Y: fresh, valid through D+2 only (no D+3 sample).
    for day, lead in ((d_plus_1, 30), (d_plus_2, 54)):
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'temperature', ?, ?, ?, 10.0, '{}', 'run-rd15y', ?)
            """,
            (site_id, y, issued_y, f"{day.isoformat()}T12:00:00Z", lead, issued_y),
        )
    # Z: fresh, the only feed with a D+3 sample.
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, 78, 15.0, '{}', 'run-rd15z', ?)
        """,
        (site_id, z, issued_z, f"{d_plus_3.isoformat()}T12:00:00Z", issued_z),
    )

    _seed_ranking(
        conn,
        publication_id=pub_id,
        feed_id=x,
        variable="temperature",
        day_ahead=2,
        window_key="w:5",
        n=3,
        skill_score=0.8,
    )
    _seed_ranking(
        conn,
        publication_id=pub_id,
        feed_id=y,
        variable="temperature",
        day_ahead=2,
        window_key="w:5",
        n=5,
        skill_score=0.3,
    )
    _seed_ranking(
        conn,
        publication_id=pub_id,
        feed_id=y,
        variable="temperature",
        day_ahead=3,
        window_key="w:5",
        n=5,
        skill_score=0.3,
    )
    conn.commit()

    now = datetime(D.year, D.month, D.day, 8, tzinfo=UTC)
    view = build_forecast(
        conn, site_id=site_id, timezone="UTC", rain_threshold_mm=0.2, now=now
    )
    tile_d1 = next(t for t in view.tiles if t.date_iso == d_plus_1.isoformat())
    tile_d2 = next(t for t in view.tiles if t.date_iso == d_plus_2.isoformat())
    tile_d3 = next(t for t in view.tiles if t.date_iso == d_plus_3.isoformat())

    assert getattr(tile_d1.temp.meta, "substitute", None) is False, (
        "D+1: X itself serves (it still has a sample there, and ranks"
        " above Y) -- substitute must be False"
    )
    assert getattr(tile_d2.temp.meta, "substitute", None) is True, (
        "correct = True (X ranks above Y at day ahead 2 but has no D+2"
        " sample, and X IS stale, so fresh Y serving in its place sets"
        " substitute). A mutant that drops the stale test entirely (M94)"
        " would ALSO read True here -- this assertion alone cannot"
        " distinguish the two; the D+3 assertion below does."
    )
    assert getattr(tile_d3.temp.meta, "substitute", None) is False, (
        "M94 -> at this assertion: correct = False (Y ranks first at day"
        " ahead 3 but has no D+3 sample at all, and Y is FRESH, not"
        " stale, so Z serving in its place must not set substitute),"
        " mutant M94 (substitute set for a fresh feed left out: the stale"
        " test dropped, so any chosen feed out of the blend set sets it)"
        " = True (Y is chosen -- ranked first, confident -- and absent"
        " from the served set {Z}, which is all the mutant checks)"
    )


# ---------------------------------------------------------------------------
# RD16
# ---------------------------------------------------------------------------


def test_rd16_download_date_from_the_later_round_not_the_earlier_sample_fetch() -> None:
    """RD16: X's label L is first stored by a download at D-1 18:10Z; the
    round at D 00:10Z downloads L again (``inserted=0``, no new sample
    row); a sample of L valid on D+1 is looked up at day ahead 1, from the
    ROUND's ``forecast_downloads`` row (``local_date`` D) -- not 2, which
    the sample's own ``fetched_at`` on D-1 would give (9.5)."""
    from wxverify.forecast.data import load_future_samples

    conn = asof_conn()
    site_id = day_close_site(conn, with_records=True)
    x = _seed_feed(conn, "X")
    _subscribe(conn, site_id, x)

    label = f"{_D1.isoformat()}T18:00:00Z"
    first_fetched_at = f"{_D1.isoformat()}T18:10:04Z"
    second_fetched_at = f"{D.isoformat()}T00:10:04Z"
    valid_at = f"{(D + timedelta(days=1)).isoformat()}T12:00:00Z"
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, 18, 15.0, '{}', 'run-rd16', ?)
        """,
        (site_id, x, label, valid_at, first_fetched_at),
    )
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=x,
        local_date=_D1,
        slot="unscheduled",
        started_at=f"{_D1.isoformat()}T18:10:00Z",
        ended_at=first_fetched_at,
        fetched_at=first_fetched_at,
        labels={str(x): [label]},
    )
    _seed_download(
        conn,
        site_id=site_id,
        feed_id=x,
        local_date=D,
        slot="round",
        round_date=D,
        round_try=1,
        started_at=f"{D.isoformat()}T00:10:00Z",
        ended_at=second_fetched_at,
        fetched_at=None,  # inserted=0: nothing new persisted by this try
        labels={str(x): [label]},
        inserted=0,
    )
    conn.commit()

    rows = load_future_samples(
        conn,
        site_id=site_id,
        since_valid_at=f"{D.isoformat()}T00:00:00Z",
        served_only=True,
    )
    row = next(r for r in rows if r.feed_id == x)
    assert getattr(row, "download_date", None) == D, (
        "M132 -> at this assertion: correct = D (the round's own"
        " local_date -- the LATEST stored download carrying label L at or"
        " before the read time), mutant M132 (the served day ahead counted"
        " from the sample's own fetched_at on D-1) -- got"
        f" {getattr(row, 'download_date', None)!r}"
    )


# ---------------------------------------------------------------------------
# RD17
# ---------------------------------------------------------------------------


def test_rd17_latest_published_at_is_the_newest_over_active_publications() -> None:
    """RD17: with no publication at all, None (paired negative); then with
    two sites -- site A with a D-1 publication published at t1 and an
    OLDER-cutoff D-3 publication published LATER, at t3, and site B with a
    D-1 publication published at t2 (t1 < t2 < t3) -- the newest
    ``published_at`` over the sites' ACTIVE publications (plan row RD17,
    :916). Site A's active publication is its D-1 one (the newer cutoff),
    published at t1, not its D-3 row published at t3, so t3 must never
    surface."""
    from wxverify.scoring.publication import latest_published_at

    conn = asof_conn()
    assert latest_published_at(conn) is None, (
        "paired negative: no publication at all -> None"
    )

    site_a = day_close_site(conn, with_records=True)
    site_b = day_close_site(
        conn, with_records=True, station_ids=("ISTATION03", "ISTATION04")
    )

    t1 = "2030-01-01T00:00:00.000Z"
    t2 = "2030-01-02T00:00:00.000Z"
    t3 = "2030-01-03T00:00:00.000Z"
    _seed_publication(
        conn, site_id=site_a, cutoff=_D1, window_days=5, min_n=3, published_at=t1
    )
    _seed_publication(
        conn, site_id=site_a, cutoff=_D3, window_days=5, min_n=3, published_at=t3
    )
    _seed_publication(
        conn, site_id=site_b, cutoff=_D1, window_days=5, min_n=3, published_at=t2
    )
    conn.commit()

    got = latest_published_at(conn)
    assert got == t2, (
        "M154 -> at this assertion: correct = t2 (site A's ACTIVE"
        " publication is its D-1 row -- the newer cutoff -- published at"
        " t1; site B's active publication is its only row, published at"
        " t2; the newest published_at over just those two active rows is"
        " t2, 16.2), mutant M154 (latest_published_at takes the newest"
        " published_at over EVERY publication, not over the sites'"
        " active ones) = t3 (site A's older-cutoff D-3 row, never active,"
        f" published latest of all three rows) -- got {got!r}"
    )
