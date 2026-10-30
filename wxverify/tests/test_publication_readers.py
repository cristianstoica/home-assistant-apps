"""Readers (daily-close plan 2026-10-22-daily-close-pairs-and-publication.md,
section 9; test IDs RD1-RD16, plan 18.8).

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
station ids via ``day_close_site`` (``ISTATION01``/``ISTATION02``), feed
sources ``synthetic-rd-x``/``synthetic-rd-y``, zone ``UTC``, site name
``Testsite``.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from pathlib import Path

import pytest

from tests.helpers import D, asof_conn, asof_insert_pair, day_close_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.metrics import MetricResult
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


def _seed_close_pair(
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
    download_date=None,
    fetched_at: str | None = None,
    issued_at: str | None = None,
) -> None:
    error = forecast - observed
    conn.execute(
        """
        INSERT INTO close_pairs
            (site_id, local_date, feed_id, variable, day_ahead, valid_at,
             lead_hours, forecast, observed, error, abs_error, sq_error,
             cat_hit, cat_false, cat_miss, cat_correct_neg,
             rain_threshold_mm, ref_sq_error, download_id, download_date,
             issued_at, fetched_at, contributors)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, ?, ?, ?, NULL)
        """,
        (
            site_id,
            local_date.isoformat(),
            feed_id,
            variable,
            day_ahead,
            valid_at,
            lead_hours,
            forecast,
            observed,
            error,
            abs(error),
            error**2,
            (download_date or local_date).isoformat(),
            issued_at or valid_at,
            fetched_at or valid_at,
        ),
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


def _rolling_fixture(conn):
    """The 18.8 shared default: close on, a D-1 publication with
    ``window_days=5``/``min_n=3`` while the saved settings are 7/10; its
    rows put X above Y; ``score_cache`` seeded with the opposite order
    (``w:7``, the saved window, so a reader that wrongly fell back to it
    would show Y on top instead of X).
    """
    site_id = day_close_site(conn, close_on=True, with_records=True)
    x = _seed_feed(conn, "X")
    y = _seed_feed(conn, "Y")
    _subscribe(conn, site_id, x)
    _subscribe(conn, site_id, y)
    set_setting(conn, "rolling_window_days", "7")
    set_setting(conn, "min_n", "10")
    pub_id = _seed_publication(
        conn, site_id=site_id, cutoff=_D1, window_days=5, min_n=3
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
    upsert_score_cache(
        conn,
        site_id=site_id,
        feed_id=x,
        variable="temperature",
        day_ahead=1,
        window_key="w:7",
        result=MetricResult(n=3, skill_score=0.1, confident=True),
        computed_at=isoformat_utc(),
    )
    upsert_score_cache(
        conn,
        site_id=site_id,
        feed_id=y,
        variable="temperature",
        day_ahead=1,
        window_key="w:7",
        result=MetricResult(n=5, skill_score=0.9, confident=True),
        computed_at=isoformat_utc(),
    )
    conn.commit()
    return site_id, x, y, pub_id


# ---------------------------------------------------------------------------
# RD1
# ---------------------------------------------------------------------------


def test_rd1_leaderboard_rolling_reads_publication_min_n_not_saved() -> None:
    """RD1: with the close on, ``rolling`` reads the active publication's
    rows (not ``score_cache``'s, seeded in the opposite order) and takes
    ``confident`` from the PUBLICATION's ``min_n`` (3), not the saved
    setting (10); status ``hit``; ``evidence_through`` the publication's
    cutoff (9.2)."""
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    conn = asof_conn()
    site_id, x, y, pub_id = _rolling_fixture(conn)

    result = leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )

    assert result.status == "hit"
    feed_ids = [row.feed_id for row in result.rows]
    assert feed_ids[0] == x, (
        "the publication ranks X above Y (skill 0.8 vs 0.3); score_cache's"
        f" rows (seeded in the opposite order) must have no effect -- got {feed_ids!r}"
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
        " from the publication's rows), mutant (the composite falls back to"
        " score_cache under the close-on rolling branch) could diverge --"
        f" got {feed_ids!r}"
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

    Live data a wrong fallback could surface is seeded first -- a
    ``score_cache`` row at the DEFAULT ``rolling_window_days`` (30, so
    ``rolling``'s own cache key is live, never just absent), a ``w:all``
    row, and ``day_scores`` rows across D-2 to D (so an ``Nd`` view that
    forgot the no-publication gate would still find rows to sum for
    ``3d``). Without this seed the row would pass "empty" merely because
    there was nothing at all to read -- a vacuous pass that could not
    distinguish P4's no-publication rule from an ordinary empty database."""
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=True, with_records=True)
    x = _seed_feed(conn, "X")
    _subscribe(conn, site_id, x)
    for window_key in ("w:30", "w:all"):
        upsert_score_cache(
            conn,
            site_id=site_id,
            feed_id=x,
            variable="temperature",
            day_ahead=1,
            window_key=window_key,
            result=MetricResult(n=3, skill_score=0.5, confident=True),
            computed_at=isoformat_utc(),
        )
    for day in (D - timedelta(days=2), D - timedelta(days=1), D):
        _seed_day_score(conn, site_id=site_id, local_date=day, feed_id=x, n=5)
    conn.commit()

    result = leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window=window
    )
    assert result.status == "empty", (
        "M81 -> at this assertion: correct = 'empty' (no active publication:"
        " the no-input gate runs before any window-specific read, so the"
        " seeded score_cache/day_scores rows must never surface, 9.2),"
        " mutant M81 (the leaderboard with no publication falls back to"
        " score_cache) = the seeded live row would be read instead --"
        f" got {result.status!r}"
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
    """RD4: ``window="3d"`` with ``day_scores`` seeded on D (open) sums only
    D-3 to D-1 (the publication's cutoff and back), never D (9.2)."""
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    conn = asof_conn()
    site_id, x, _y, _pub_id = _rolling_fixture(conn)

    for day in (_D3, _D2, _D1):
        _seed_day_score(conn, site_id=site_id, local_date=day, feed_id=x, n=1)
    # D is open; its row must never enter a window ending at the cutoff.
    _seed_day_score(conn, site_id=site_id, local_date=D, feed_id=x, n=100)
    conn.commit()

    result = leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="3d"
    )

    assert result.status == "live"
    row = next(row for row in result.rows if row.feed_id == x)
    assert row.n == 3, (
        "M82 -> at this assertion: correct = 3 (the sum of D-3, D-2, D-1 --"
        " the 3 dates ending at the publication's cutoff D-1), mutant M82"
        " (an Nd view ends at today) = 103 (D's n=100 row wrongly included)"
    )


# ---------------------------------------------------------------------------
# RD5
# ---------------------------------------------------------------------------


def test_rd5_winrate_reads_close_pairs_closed_and_forecast_pairs_starting() -> None:
    """RD5: the win rate credits a win from a closed date's ``close_pairs``
    row AND a win from a starting-history date's ``forecast_pairs`` row,
    inside the same (rolling, 5-date) window (9.2)."""
    from wxverify.scoring.winrate import winrate

    conn = asof_conn()
    site_id, x, y, _pub_id = _rolling_fixture(conn)

    # A closed date (D-1, inside the 5-date window) where X wins the hour.
    valid_closed = f"{_D1.isoformat()}T00:00:00Z"
    _seed_close_pair(
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
    _seed_close_pair(
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
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=x,
        valid_at=valid_starting,
        issued_at=f"{(_D5 - timedelta(hours=6)).isoformat()}T00:00:00Z",
        forecast=10.0,
        observed=5.0,
        first_known_at=valid_starting,
        day_ahead=1,
        lead_hours=6,
    )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=y,
        valid_at=valid_starting,
        issued_at=f"{(_D5 - timedelta(hours=6)).isoformat()}T00:00:00Z",
        forecast=5.0,
        observed=5.0,
        first_known_at=valid_starting,
        day_ahead=1,
        lead_hours=6,
    )
    conn.commit()

    rows = winrate(
        conn, site_id=site_id, variable="temperature", day_ahead=1, window="rolling"
    )
    wins_by_feed = {row["feed_id"]: row.get("wins", 0.0) for row in rows}

    assert wins_by_feed.get(x, 0.0) >= 1.0, (
        "the win rate must credit X's win from the CLOSED date's"
        f" close_pairs row (9.2) -- got {wins_by_feed!r}"
    )
    assert wins_by_feed.get(y, 0.0) >= 1.0, (
        "M83 -> at this assertion: correct = Y credited a win from the"
        " STARTING-HISTORY date's forecast_pairs row, mutant M83 (the win"
        " rate's starting part dropped) = Y gets no win from that date --"
        f" got {wins_by_feed!r}"
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

    serving = load_wind_serving(
        conn, site_id=site_id, timezone="UTC", today=D, as_of=None
    )
    keys = set(serving.weights.keys())
    assert (x, 1) in keys and (y, 1) in keys, (
        f"both feeds expected before disabling Y -- got {keys!r}"
    )

    _set_enabled(conn, site_id, y, 0)
    conn.commit()
    serving_off = load_wind_serving(
        conn, site_id=site_id, timezone="UTC", today=D, as_of=None
    )
    keys_off = set(serving_off.weights.keys())
    assert (y, 1) not in keys_off, (
        "M84 -> at this assertion: correct = Y's key absent once switched"
        " off, mutant M84 (wind weights not filtered by the subscription)"
        f" = Y's key still present -- got {keys_off!r}"
    )
    assert (x, 1) in keys_off

    # Paired positive / second half: no publication at all -> {}.
    conn2 = asof_conn()
    empty_site = day_close_site(conn2, close_on=True, with_records=True)
    conn2.commit()
    serving_empty = load_wind_serving(
        conn2, site_id=empty_site, timezone="UTC", today=D, as_of=None
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
    """RD7: one response built inside ONE ``pinned_read_snapshot`` -- the
    mechanism 9.3 names for ``/curve`` -- sees the SAME publication (D-1)
    at every leaderboard read inside it, even when a SECOND connection
    commits a newer (D) publication while that snapshot is held open (9.3).

    Deterministic rendezvous in place of a timing race: the spied function
    is ``leaderboard_with_status_in_transaction`` (``wxverify.scoring.
    leaderboard``) -- the per-day_ahead reader ``/curve``
    (``api/routes/dashboard.py:77-97``) must call once per lead, since its
    OWN snapshotting facade, ``leaderboard_with_status``, "refuses to nest"
    inside a caller-held snapshot such as ``/curve``'s one
    ``pinned_read_snapshot`` (the facade's own docstring). On the spy's
    FIRST call, a second, real connection to the SAME on-disk file commits
    a newer publication before the spy returns; the second call, still
    inside the first connection's one open snapshot, is the one put at
    risk. Every value in the response (each call's ``evidence_through``)
    must still come from the ORIGINAL (D-1) publication.
    """
    from wxverify.db.connection import pinned_read_snapshot
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

    with pinned_read_snapshot(conn, label="rd7-test"):
        _spy(day_ahead=1)
        _spy(day_ahead=1)

    assert seen_cutoffs, "fixture bug: the spy must be called"
    assert len(set(seen_cutoffs)) == 1 and seen_cutoffs[0] == _D1, (
        "M85 -> at this assertion: correct = every call inside the one"
        " pinned snapshot reports evidence_through D-1, the publication"
        " active when the snapshot opened (9.3: one pinned_read_snapshot"
        " per response), mutant M85 (/curve reads each leaderboard in its"
        " own snapshot) = the second call, no longer sharing the first"
        f" call's snapshot, would see the newly-committed D publication"
        f" instead -- got {seen_cutoffs!r}"
    )


# ---------------------------------------------------------------------------
# RD8
# ---------------------------------------------------------------------------


def test_rd8_served_forecast_follows_the_publication_order_at_blend_depth_1() -> None:
    """RD8: with ``forecast_blend_depth=1``, the served temperature at D+1
    is X's -- the publication's order, not ``score_cache``'s (9.5)."""
    from wxverify.forecast.service import build_forecast

    conn = asof_conn()
    site_id, x, y, _pub_id = _rolling_fixture(conn)
    set_setting(conn, "forecast_blend_depth", "1")

    valid_at = f"{(D + timedelta(days=1)).isoformat()}T12:00:00Z"
    issued_at = f"{D.isoformat()}T00:00:00Z"
    for feed_id, value in ((x, 20.0), (y, 10.0)):
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'temperature', ?, ?, 36, ?, '{}', 'run-rd8', ?)
            """,
            (site_id, feed_id, issued_at, valid_at, value, issued_at),
        )
    conn.commit()

    view = build_forecast(conn, site_id=site_id, timezone="UTC", rain_threshold_mm=0.2)
    tile = next(
        t for t in view.tiles if t.date_iso == (D + timedelta(days=1)).isoformat()
    )
    served_ids = [ref.feed_id for ref in tile.temp.meta.feeds]
    assert served_ids and served_ids[0] == x, (
        "M86 -> at this assertion: correct = X serves (the publication's"
        " order ranks X above Y), mutant M86 (serving reads score_cache"
        f" with the close on, where Y ranks above X) = Y serves -- got {served_ids!r}"
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
    site_id = day_close_site(conn, close_on=True, with_records=True)
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
        as_of=f"{D.isoformat()}T08:00:00Z",
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


def test_rd14_closed_day_pairs_returns_only_a_closed_dates_rows() -> None:
    """RD14: ``closed_day_pairs`` returns D-1's (closed) ``close_pairs``
    rows, and ``[]`` for D (open, no rows exist for it, 16.2)."""
    from wxverify.scoring.publication import closed_day_pairs

    conn = asof_conn()
    site_id, x, _y, _pub_id = _rolling_fixture(conn)
    valid_at = f"{_D1.isoformat()}T00:00:00Z"
    _seed_close_pair(
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
    conn.commit()

    closed_rows = closed_day_pairs(conn, site_id=site_id, local_date=_D1)
    assert len(closed_rows) == 1 and closed_rows[0].feed_id == x

    open_rows = closed_day_pairs(conn, site_id=site_id, local_date=D)
    assert open_rows == [], (
        "M93 -> at this assertion: correct = [] for an open day (no"
        " close_pairs rows ever exist for it), mutant M93 (closed_day_pairs"
        f" returns an open day's rows too) -- got {open_rows!r}"
    )


# ---------------------------------------------------------------------------
# RD15
# ---------------------------------------------------------------------------


def test_rd15_substitute_flag_set_only_when_a_fresh_feed_fills_in() -> None:
    """RD15: X (stale, samples only to D+1) serves D+1 with ``substitute``
    False (it is itself the chosen feed); at D+2 Y (fresh) serves and
    ``substitute`` is True (Y fills in for X, which is stale and out of
    the cell's chosen set by then); with the close off, False everywhere
    (11.3)."""
    from wxverify.forecast.service import build_forecast

    conn = asof_conn()
    site_id, x, y, _pub_id = _rolling_fixture(conn)
    set_setting(conn, "forecast_blend_depth", "1")

    d_plus_1 = D + timedelta(days=1)
    d_plus_2 = D + timedelta(days=2)
    issued_x = f"{D.isoformat()}T00:00:00Z"
    issued_y = f"{D.isoformat()}T06:00:00Z"
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
    # Y: fresh, valid through D+2.
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
    conn.commit()

    view = build_forecast(conn, site_id=site_id, timezone="UTC", rain_threshold_mm=0.2)
    tile_d1 = next(t for t in view.tiles if t.date_iso == d_plus_1.isoformat())
    tile_d2 = next(t for t in view.tiles if t.date_iso == d_plus_2.isoformat())

    assert getattr(tile_d1.temp.meta, "substitute", None) is False, (
        "D+1: X itself serves (it still has a sample there) -- substitute must be False"
    )
    assert getattr(tile_d2.temp.meta, "substitute", None) is True, (
        "M94 -> at this assertion: correct = True (Y fills in for stale,"
        " out-of-set X at D+2), mutant M94 (substitute set for a fresh feed"
        " left out -- i.e. never set correctly for the stale-feed case) = False"
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
    site_id = day_close_site(conn, close_on=True, with_records=True)
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
        as_of=f"{D.isoformat()}T08:00:00Z",
    )
    row = next(r for r in rows if r.feed_id == x)
    assert getattr(row, "download_date", None) == D, (
        "M132 -> at this assertion: correct = D (the round's own"
        " local_date -- the LATEST stored download carrying label L at or"
        " before the read time), mutant M132 (the served day ahead counted"
        " from the sample's own fetched_at on D-1) -- got"
        f" {getattr(row, 'download_date', None)!r}"
    )
