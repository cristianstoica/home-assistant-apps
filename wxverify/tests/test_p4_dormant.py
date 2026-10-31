"""Dormant-while-close-off paths for daily-close P4 (plan
2026-10-22-daily-close-pairs-and-publication.md, section 17, test IDs
PD1, PD2, PD3, PD4, PD5, 18.12).

Real SQLite throughout; nothing in the code under test is mocked except
the provider adapter stand-in PD3 drives a real fetch through (never a
real network call). PD5 drives its imports in a fresh interpreter via
``subprocess.run`` (16.1), so it fails with a non-zero exit from that
subprocess before P4's four body modules exist, rather than at this
file's own collection. Synthetic data only: station ids
``ISTATION01``/``ISTATION02`` (via ``tests.helpers.day_close_site``), site
names ``Testsite``/``site-a``/``site-b``, zone ``UTC``, model names
``model-a``/``model-b``, feed sources ``synthetic-pd-x``.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import (
    NOW,
    D,
    asof_conn,
    asof_insert_pair,
    asof_make_real_feed,
    asof_make_site,
    asof_persistence_feed_id,
    day_close_site,
    install_provider_standin,
    seed_round_schedule_feeds,
)
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.settings.keys import set_setting

# ---------------------------------------------------------------------------
# PD1
# ---------------------------------------------------------------------------


def test_pd1_leaderboard_nd_window_close_off_reads_live_pairs_not_day_scores() -> None:
    """PD1: with the close off, an ``Nd`` leaderboard window is computed
    live from ``forecast_pairs`` exactly as today, never from the new
    ``day_scores``/``starting_scores`` tables -- ``day_scores`` is left
    empty on purpose, so a reader that wrongly took the close-on branch
    (M120) would find nothing there and drop the cell, diverging from the
    live count below. ``evidence_through`` and ``publication_id`` are also
    None, as 17 says for a build without the close on.

    A ``publications`` row for D-1 is inserted by hand (18.12's literal
    PD1 fixture) to show it too has no effect on this close-off read, even
    though this particular (``Nd``) branch does not consult it either way
    (9.2: only ``rolling``/``all`` do).
    """
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction

    conn = asof_conn()
    site_id = asof_make_site(conn, "Testsite")
    feed_id = asof_make_real_feed(conn, "model-a")

    now = datetime.now(UTC)
    valid_at = isoformat_utc(now - timedelta(hours=2))
    issued_at = isoformat_utc(now - timedelta(hours=8))
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=valid_at,
        issued_at=issued_at,
        forecast=10.0,
        observed=8.0,
        first_known_at=issued_at,
        day_ahead=0,
        lead_hours=2,
    )

    # The plan's literal fixture: a publications row for D-1, inserted by
    # hand, present but (for this window) inert while the close is off.
    conn.execute(
        """
        INSERT INTO publications
            (site_id, cutoff, first_record_date, window_days,
             window_start_date, wind_start_date, settings,
             methodology_version, scoring_rule, wind_method, wind_constants,
             build_started_at, published_at)
        VALUES (?, ?, ?, 30, ?, ?, '{}', 4,
                'first-stored-download-of-local-day/1', 'persistence_blend',
                '{}', ?, ?)
        """,
        (
            site_id,
            (D - timedelta(days=1)).isoformat(),
            (D - timedelta(days=1)).isoformat(),
            (D - timedelta(days=1)).isoformat(),
            (D - timedelta(days=1)).isoformat(),
            isoformat_utc(now),
            isoformat_utc(now),
        ),
    )
    conn.commit()

    day_scores_count = conn.execute("SELECT COUNT(*) FROM day_scores").fetchone()[0]
    assert day_scores_count == 0, (
        "fixture bug: day_scores must stay empty so the divergence below"
        " actually discriminates the close-on branch from the close-off one"
    )

    result = leaderboard_with_status_in_transaction(
        conn, site_id=site_id, variable="temperature", day_ahead=0, window="7d"
    )

    assert result.status == "live"
    assert len(result.rows) == 1 and result.rows[0].feed_id == feed_id
    assert result.rows[0].n == 1, (
        "M120 -> at this assertion: correct = 1 (close off: today's live"
        " computation from forecast_pairs, 17), mutant M120 (a reader's"
        " close-on branch taken with the close off) = the Nd computation"
        " reads day_scores/starting_scores instead (9.2), which are empty,"
        " so the cell is dropped or n is 0"
    )
    assert result.evidence_through is None, (
        "mutant -> at this assertion: correct = None (17: with the close"
        " off, evidence_through is never set), mutant (the publication's"
        " cutoff read regardless of the close switch) = the D-1 cutoff"
        " inserted by hand above"
    )
    assert result.publication_id is None, (
        "mutant -> at this assertion: correct = None (17: with the close"
        " off, publication_id is never set), mutant (the publication's id"
        " read regardless of the close switch) = the id of the row"
        " inserted by hand above"
    )


def test_pd1_composite_nd_window_close_off_reads_live_pairs_not_day_scores() -> None:
    """PD1 (second reader): the same divergence as above for
    ``composite_with_status``'s ``Nd`` window (9.2: it moves to the day
    sums under the close too)."""
    from wxverify.scoring.composite import composite_with_status

    conn = asof_conn()
    set_setting(conn, "min_n", "1")
    site_id = asof_make_site(conn, "Testsite")
    feed_id = asof_make_real_feed(conn, "model-a")

    now = datetime.now(UTC)
    valid_at = isoformat_utc(now - timedelta(hours=2))
    issued_at = isoformat_utc(now - timedelta(hours=8))
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=valid_at,
        issued_at=issued_at,
        forecast=10.0,
        observed=8.0,
        first_known_at=issued_at,
        day_ahead=0,
        lead_hours=2,
    )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=asof_persistence_feed_id(conn),
        valid_at=valid_at,
        issued_at=issued_at,
        forecast=11.0,
        observed=8.0,
        first_known_at=issued_at,
        day_ahead=0,
        lead_hours=2,
    )
    _pd1_publication(conn, site_id=site_id, cutoff=D - timedelta(days=1))
    conn.commit()

    day_scores_count = conn.execute("SELECT COUNT(*) FROM day_scores").fetchone()[0]
    assert day_scores_count == 0, (
        "fixture bug: day_scores must stay empty so the divergence below"
        " actually discriminates the close-on branch from the close-off one"
    )

    result = composite_with_status(conn, site_id=site_id, window="7d")

    assert result.status == "live"
    feed_rows = [row for row in result.rows if row.get("feed_id") == feed_id]
    assert len(feed_rows) == 1, (
        "M120 -> at this assertion: correct = exactly one row for the"
        " seeded feed (close off: today's live computation from"
        " forecast_pairs, 17), mutant M120 (a reader's close-on branch"
        " taken with the close off) = no row (the live composite would"
        " read day_scores/starting_scores instead, 9.2, which are empty)"
    )


def _pd1_feed(conn) -> int:
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES ('synthetic-pd-x', 'synthetic-pd-x', 1, 0, 360, 168, 0)"
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _pd1_publication(conn, *, site_id: int, cutoff) -> None:
    """The literal PD1 fixture (18.12): a publications row inserted by
    hand, present but inert for every close-off reader below."""
    now = isoformat_utc()
    conn.execute(
        """
        INSERT INTO publications
            (site_id, cutoff, first_record_date, window_days,
             window_start_date, wind_start_date, settings,
             methodology_version, scoring_rule, wind_method, wind_constants,
             build_started_at, published_at)
        VALUES (?, ?, ?, 30, ?, ?, '{}', 4,
                'first-stored-download-of-local-day/1', 'persistence_blend',
                '{}', ?, ?)
        """,
        (
            site_id,
            cutoff.isoformat(),
            cutoff.isoformat(),
            cutoff.isoformat(),
            cutoff.isoformat(),
            now,
            now,
        ),
    )


def test_pd1_leaderboard_rolling_and_all_close_off_read_forecast_pairs() -> None:
    """PD1 (added reader): with the close off, ``rolling`` and ``all`` are
    served from ``score_cache`` exactly as today -- never from the
    publication's rows, inserted by hand and otherwise left untouched
    (9.2, 17)."""
    from wxverify.scoring.cache import upsert_score_cache
    from wxverify.scoring.leaderboard import leaderboard_with_status_in_transaction
    from wxverify.scoring.metrics import MetricResult

    conn = asof_conn()
    site_id = asof_make_site(conn, "Testsite")
    feed_id = _pd1_feed(conn)
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )
    now = datetime.now(UTC)
    valid_at = isoformat_utc(now - timedelta(hours=2))
    issued_at = isoformat_utc(now - timedelta(hours=8))
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at=valid_at,
        issued_at=issued_at,
        forecast=10.0,
        observed=8.0,
        first_known_at=issued_at,
        day_ahead=1,
        lead_hours=2,
    )
    upsert_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_key="w:30",
        result=MetricResult(n=4, skill_score=0.4, confident=True),
        computed_at=isoformat_utc(),
    )
    upsert_score_cache(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_key="w:all",
        result=MetricResult(n=4, skill_score=0.4, confident=True),
        computed_at=isoformat_utc(),
    )
    _pd1_publication(conn, site_id=site_id, cutoff=D - timedelta(days=1))
    conn.commit()

    for window in ("rolling", "all"):
        result = leaderboard_with_status_in_transaction(
            conn, site_id=site_id, variable="temperature", day_ahead=1, window=window
        )
        assert result.status == "hit", (
            f"window={window}: close off must take today's cache-backed"
            f" 'hit' branch, not the close-on 'empty'/'live' statuses --"
            f" got {result.status!r}"
        )
        assert [r.feed_id for r in result.rows] == [feed_id], (
            f"window={window}: close off must serve the cached row for the"
            f" seeded feed, got {[r.feed_id for r in result.rows]!r}"
        )
        assert result.evidence_through is None, (
            "M120 -> at this assertion: correct = None (close off never"
            f" sets evidence_through, 17), window={window}, mutant M120 (a"
            " reader's close-on branch taken with the close off) = the"
            " hand-inserted publication's cutoff"
        )
        assert result.publication_id is None


def test_pd1_winrate_close_off_ignores_close_pairs() -> None:
    """PD1 (added reader): with the close off, the win rate credits a win
    from ``forecast_pairs`` only -- a ``close_pairs`` row for the SAME
    hour, crediting the OTHER feed, must have no effect (9.2, 17)."""
    from wxverify.scoring.winrate import winrate

    conn = asof_conn()
    site_id = asof_make_site(conn, "Testsite")
    feed_a = asof_make_real_feed(conn, "model-a")
    feed_b = asof_make_real_feed(conn, "model-b")
    valid_at = isoformat_utc(datetime.now(UTC).replace(microsecond=0))
    issued_at = isoformat_utc(datetime.now(UTC) - timedelta(hours=6))
    # forecast_pairs: feed_a wins this hour (abs_error 0).
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_a,
        valid_at=valid_at,
        issued_at=issued_at,
        forecast=10.0,
        observed=10.0,
        first_known_at=issued_at,
        day_ahead=0,
        lead_hours=6,
    )
    asof_insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_b,
        valid_at=valid_at,
        issued_at=issued_at,
        forecast=14.0,
        observed=10.0,
        first_known_at=issued_at,
        day_ahead=0,
        lead_hours=6,
    )
    conn.execute(
        """
        INSERT INTO close_pairs
            (site_id, local_date, feed_id, variable, day_ahead, valid_at,
             lead_hours, forecast, observed, error, abs_error, sq_error,
             cat_hit, cat_false, cat_miss, cat_correct_neg,
             rain_threshold_mm, ref_sq_error, download_id, download_date,
             issued_at, fetched_at, contributors)
        VALUES (?, ?, ?, 'temperature', 0, ?, 6, 10.0, 10.0, 0.0, 0.0, 0.0,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, NULL)
        """,
        (
            site_id,
            D.isoformat(),
            feed_b,  # close_pairs: feed_b would win instead, if wrongly read
            valid_at,
            D.isoformat(),
            issued_at,
            valid_at,
        ),
    )
    conn.commit()

    rows = winrate(conn, site_id=site_id, variable="temperature", day_ahead=0)
    wins_by_feed = {row["feed_id"]: row.get("wins", 0.0) for row in rows}
    x_win = wins_by_feed.get(feed_a, 0.0) >= 1.0
    y_win = wins_by_feed.get(feed_b, 0.0) == 0.0
    assert x_win and y_win, (
        "M120 -> at this assertion: correct = feed_a (forecast_pairs' own"
        " winner) credited and feed_b not (close off: forecast_pairs only,"
        " 17), mutant M120 (a reader's close-on branch taken with the"
        f" close off, crediting close_pairs' feed_b instead) -- got {wins_by_feed!r}"
    )


def test_pd1_wind_serving_close_off_ignores_publication_wind_weights() -> None:
    """PD1 (added reader): with the close off, ``load_wind_serving`` (no
    ``as_of``) answers from the live training cache, never from a
    publication's ``publication_wind_weights`` rows -- with no training
    data at all it answers ``{}``, even though a bogus weight for this
    feed sits in a hand-inserted publication (9.4, 17)."""
    from wxverify.forecast.wind_blend import load_wind_serving

    conn = asof_conn()
    site_id = asof_make_site(conn, "Testsite")
    feed_id = _pd1_feed(conn)
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )
    cutoff = D - timedelta(days=1)
    _pd1_publication(conn, site_id=site_id, cutoff=cutoff)
    pub_id = conn.execute(
        "SELECT id FROM publications WHERE site_id = ? AND cutoff = ?",
        (site_id, cutoff.isoformat()),
    ).fetchone()[0]
    conn.execute(
        """
        INSERT INTO publication_wind_weights
            (publication_id, feed_id, day_ahead, mae_ms, days, weight)
        VALUES (?, ?, 1, 1.0, 25, 1.0)
        """,
        (pub_id, feed_id),
    )
    conn.commit()

    serving = load_wind_serving(
        conn, site_id=site_id, timezone="UTC", today=D, as_of=None
    )
    assert serving.weights == {}, (
        "M120 -> at this assertion: correct = {} (close off: no training"
        " data exists, so the live path finds nothing to serve, 17),"
        " mutant M120 (a reader's close-on branch taken with the close"
        " off) = the publication's bogus weight for this feed leaks"
        f" through -- got {serving.weights!r}"
    )


def test_pd1_served_forecast_close_off_keeps_issue_time_day_ahead() -> None:
    """PD1 (added reader): with the close off, ``load_future_samples``
    never consults ``forecast_downloads`` -- ``download_date`` stays None
    regardless of any download row on file (9.5, 17)."""
    from wxverify.forecast.data import load_future_samples

    conn = asof_conn()
    site_id = asof_make_site(conn, "Testsite")
    feed_id = _pd1_feed(conn)
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )
    now = datetime.now(UTC).replace(microsecond=0)
    issued_at = isoformat_utc(now - timedelta(days=2))
    valid_at = isoformat_utc(now + timedelta(hours=6))
    fetched_at = isoformat_utc(now - timedelta(hours=1))
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'temperature', ?, ?, 54, 12.0, '{}', 'run-pd1', ?)
        """,
        (site_id, feed_id, issued_at, valid_at, fetched_at),
    )
    conn.execute(
        """
        INSERT INTO forecast_downloads
            (site_id, feed_id, local_date, slot, round_date, round_try,
             started_at, ended_at, fetched_at, outcome, reason, usable,
             inserted, labels)
        VALUES (?, ?, ?, 'round', ?, 1, ?, ?, ?, 'stored', NULL, 1, 1, ?)
        """,
        (
            site_id,
            feed_id,
            D.isoformat(),
            D.isoformat(),
            fetched_at,
            fetched_at,
            fetched_at,
            json.dumps({str(feed_id): [issued_at]}),
        ),
    )
    conn.commit()

    rows = load_future_samples(
        conn,
        site_id=site_id,
        since_valid_at=isoformat_utc(now),
        served_only=False,
        as_of=isoformat_utc(now),
    )
    row = next(r for r in rows if r.feed_id == feed_id)
    assert getattr(row, "download_date", None) is None, (
        "M120 -> at this assertion: correct = None (close off: the"
        " download-date lookup never runs, 9.5, 17), mutant M120 (a"
        " reader's close-on branch taken with the close off) = D (the"
        " forecast_downloads row's local_date leaks through)"
    )


def test_pd1_record_schema_has_null_publication_columns_available() -> None:
    """PD1 (added reader, schema half): ``forecast_of_record`` already
    carries the new, nullable ``publication_id``/``publication_cutoff``/
    ``publication_age_days`` columns (section 15.3), so a close-off record
    row can leave them NULL.

    This is a narrower check than driving a full ``build_forecast_record``
    call (which needs a complete station/truth/sample fixture, the same
    heavy one ``tests/test_record_publication.py``'s RC rows build) --
    named as a gap in the test-authoring report rather than built here for
    time. What IS exercised: inserting a ``missed``-shaped record row by
    hand (as ``_write_missed_rows`` would, 12) with every publication
    column NULL succeeds against the real schema, and reading it back
    gives NULL for all three -- the shape a close-off (or close-on,
    missed) record actually writes.
    """
    from wxverify.db.tz_generations import ensure_published_generation

    conn = asof_conn()
    site_id = asof_make_site(conn, "Testsite")
    conn.commit()
    generation_id = ensure_published_generation(conn, site_id)

    cur = conn.execute(
        """
        INSERT INTO forecast_of_record
            (site_id, tz_generation_id, timezone, snapshot_local_date,
             snapshot_local_time, snapshot_utc, target_local_date, variable,
             display_lead, status, missed_reason, methodology_version,
             app_version, leaderboard_status, publication_id,
             publication_cutoff, publication_age_days)
        VALUES (?, ?, 'UTC', ?, '07:00', ?, ?, 'temperature', 0, 'missed',
                'no_input', 4, 'test', NULL, NULL, NULL, NULL)
        """,
        (
            site_id,
            generation_id,
            D.isoformat(),
            isoformat_utc(),
            D.isoformat(),
        ),
    )
    row_id = cur.lastrowid
    conn.commit()

    row = conn.execute(
        "SELECT publication_id, publication_cutoff, publication_age_days"
        " FROM forecast_of_record WHERE id = ?",
        (row_id,),
    ).fetchone()
    assert tuple(row) == (None, None, None), (
        "the three new publication columns must accept and return NULL --"
        f" got {tuple(row)!r}"
    )


def test_pd1_record_build_close_off_leaves_publication_columns_null() -> None:
    """PD1 (record half): with the close off, a 07:00 record built the
    normal way (``build_forecast_record``, section 12's entry point)
    leaves the three publication columns NULL and sets
    ``leaderboard_status`` as today -- a JSON object from the live score
    cache, never None (None is close-on-only, RC1's own M101 pin in
    ``tests/test_record_publication.py``).

    Builds the fixture via ``tests.helpers.build_record_site_with_sample``,
    copied there from ``test_record_publication.py``'s own
    ``_build_site_with_sample`` (another agent's file, left untouched).
    """
    from tests.helpers import build_record_site_with_sample
    from wxverify.verification.record import build_forecast_record

    t = datetime(D.year, D.month, D.day, 7, 0, tzinfo=UTC)

    conn = asof_conn()
    site_id, _feed_id = build_record_site_with_sample(conn, close_on=False)

    build_forecast_record(conn, site_id, D.isoformat(), now=t)

    row = conn.execute(
        "SELECT publication_id, publication_cutoff, publication_age_days,"
        "       leaderboard_status"
        " FROM forecast_of_record"
        " WHERE site_id = ? AND snapshot_local_date = ? AND variable = 'temperature'"
        " AND target_local_date = ?",
        (site_id, D.isoformat(), D.isoformat()),
    ).fetchone()
    assert row is not None, "build_forecast_record must write at least one row"
    assert (
        row["publication_id"],
        row["publication_cutoff"],
        row["publication_age_days"],
    ) == (None, None, None), (
        "M120 -> at this assertion: correct = (None, None, None) (close"
        " off: the record never touches the publication columns, section"
        " 17), mutant M120 (a reader's close-on branch taken with the"
        " close off) would instead stamp a publication identity onto a"
        f" record built with the close off -- got {tuple(row)[:3]!r}"
    )
    assert row["leaderboard_status"] is not None, (
        "paired positive: close off still computes and saves the live"
        " as-of leaderboard_status (today's behavior, unchanged by P4,"
        " section 12) -- RC1's own close-ON pin (M101,"
        " test_record_publication.py) is that this column goes NULL only"
        " with the close ON; a close-off build that also left it NULL"
        " would make that divergence meaningless"
    )


def test_pd1_header_absent_close_off_present_close_on(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PD1 (header, paired negative/positive): close off, ``GET
    /api/leaderboard`` carries no ``X-Evidence-Through`` header (today's
    shape); close on, with an active publication, it DOES (9.6). The
    close-off half alone would be a vacuous pass today (no header exists
    at all yet) -- the close-on half is the controlled positive that
    makes the pair meaningful.
    """
    from fastapi.testclient import TestClient

    from tests.test_leaderboard_cache_backed import _start_app

    app = _start_app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        from wxverify.db.connection import get_db

        db = get_db()

        def _seed_off(conn) -> int:
            site_id = asof_make_site(conn, "HeaderSite")
            feed_id = _pd1_feed(conn)
            conn.execute(
                "INSERT INTO site_feed_state"
                " (site_id, feed_id, enabled, error_count)"
                " VALUES (?, ?, 1, 0)",
                (site_id, feed_id),
            )
            return site_id

        site_id = db.write_sync(_seed_off)

        resp_off = client.get(
            "/api/leaderboard",
            params={"site": site_id, "variable": "temperature", "lead": "D+1"},
        )
        assert resp_off.status_code == 200
        assert "X-Evidence-Through" not in resp_off.headers, (
            "close off: no X-Evidence-Through header should be sent (today's"
            f" shape, 17) -- got headers {dict(resp_off.headers)!r}"
        )

        def _turn_on_and_publish(conn) -> None:
            from wxverify.db.runtime_state import set_runtime_state
            from wxverify.db.site_days import DAILY_CLOSE_ON_KEY

            set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc())
            _pd1_publication(conn, site_id=site_id, cutoff=D - timedelta(days=1))

        db.write_sync(_turn_on_and_publish)

        resp_on = client.get(
            "/api/leaderboard",
            params={"site": site_id, "variable": "temperature", "lead": "D+1"},
        )
        assert resp_on.status_code == 200
        assert "X-Evidence-Through" in resp_on.headers, (
            "paired positive: close on, with an active publication, the"
            " X-Evidence-Through header must be sent (9.6) -- got headers"
            f" {dict(resp_on.headers)!r}"
        )


# ---------------------------------------------------------------------------
# PD2
# ---------------------------------------------------------------------------


def test_pd2_score_day_called_directly_with_close_off_is_refused() -> None:
    """PD2: with the close off, calling ``score_day`` directly on a
    ``closed`` day's record is refused by P1's guard before any other
    work: ``ScoreDayRefused``, and nothing written to ``day_scores`` (17;
    P1 §4.8).

    Paired positive: the identical call shape, close ON, on a day in the
    ``closing`` state (the one state the guard accepts DAY_SCORES/
    CLOSE_RECORD writes for, plan §6.1) raises a *different* exception,
    ``ScoreDayInputMissing`` (for want of truth rows) -- proving the
    refusal above comes from the close switch specifically, not from
    ``score_day`` never reaching the guard either way.
    """
    from wxverify.db.site_days import day_record
    from wxverify.scoring.close_scores import ScoreDayRefused, score_day

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False, with_records=True)
    closed_date = D - timedelta(days=3)
    record = day_record(conn, site_id, closed_date)
    assert record is not None and record.state == "closed", (
        "fixture bug: day_close_site's D-3 record must be closed"
    )

    with pytest.raises(ScoreDayRefused):
        score_day(conn, record=record, attempt=1, truth_version="fp-1", now_utc=NOW)
    conn.rollback()

    written = conn.execute(
        "SELECT COUNT(*) FROM day_scores WHERE site_id = ? AND local_date = ?",
        (site_id, closed_date.isoformat()),
    ).fetchone()[0]
    assert written == 0, (
        "M121 -> at this assertion: correct = 0 (the guard refused before"
        " any write, P1 §4.8), mutant M121 (score_day skips the guard) ="
        " a row was written for this closed day with the close off"
    )

    # --- Paired positive -----------------------------------------------
    from wxverify.db.runtime_state import set_runtime_state
    from wxverify.db.site_days import (
        DAILY_CLOSE_ON_KEY,
        begin_close,
        ensure_site_days,
        start_site_days,
    )
    from wxverify.scoring.close_scores import ScoreDayInputMissing

    conn2 = asof_conn()
    site_b = day_close_site(conn2, close_on=True, with_records=False)
    closing_date = D - timedelta(days=5)
    start_site_days(
        conn2,
        site_b,
        closing_date,
        now_utc=datetime(
            closing_date.year, closing_date.month, closing_date.day, tzinfo=UTC
        ),
    )
    ensure_site_days(conn2, site_b, now_utc=NOW)
    set_runtime_state(conn2, DAILY_CLOSE_ON_KEY, isoformat_utc(NOW))
    d_row = conn2.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_b, closing_date.isoformat()),
    ).fetchone()
    assert d_row is not None
    t = parse_utc(d_row[0]) + timedelta(minutes=30)
    begin_close(conn2, site_b, closing_date, now_utc=t)  # leaves state 'closing'

    closing_record = day_record(conn2, site_b, closing_date)
    assert closing_record is not None and closing_record.state == "closing", (
        "fixture bug: begin_close without finish_close must leave the"
        " record in the 'closing' state"
    )

    with pytest.raises(ScoreDayInputMissing) as excinfo:
        score_day(
            conn2,
            record=closing_record,
            attempt=1,
            truth_version="fp-1",
            now_utc=t,
        )
    assert not isinstance(excinfo.value, ScoreDayRefused), (
        "M121 -> at this assertion: correct = ScoreDayInputMissing, not"
        " ScoreDayRefused (a 'closing' day with the close on passes the"
        " guard and fails later for want of truth rows, plan §6.1), mutant"
        " (the guard applied unconditionally regardless of state/switch) ="
        " ScoreDayRefused here too, which would make this case"
        " indistinguishable from the close-off case above"
    )


# ---------------------------------------------------------------------------
# PD3
#
# PD3 pins TODAY'S close-off behavior and, by design, may already pass
# before P4 is built (Step 3, section 20): the fetch score gate's enqueue
# is unconditional in today's code, so it fires regardless of the close
# switch until piece 8 routes it through enqueue_scoring_run.
# ---------------------------------------------------------------------------


def test_pd3_fetch_that_would_pair_enqueues_one_pair_and_score_job_close_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PD3: with the close off, a forward fetch whose sample would pair
    enqueues exactly one ``pair_and_score`` job, as today (17). Drives the
    real production call point, ``_persist_fetch_success``
    (``worker/feed_fetch.py``), the same way ``test_close_triggers.py``'s
    TR3 fetch-gate case does -- with the close OFF here instead of on.
    """
    from wxverify.feeds.seam import FetchResult, NormalizedSample
    from wxverify.worker.domain_backoff import SOURCE_DOMAINS
    from wxverify.worker.feed_fetch import FeedFetchTarget, _persist_fetch_success

    conn = asof_conn()
    site_id = day_close_site(conn, close_on=False, with_records=False)
    feed_id = _pd1_feed(conn)
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )
    conn.commit()
    monkeypatch.setitem(SOURCE_DOMAINS, "synthetic-pd-x", "synthetic-pd-x")

    issued_at = f"{D.isoformat()}T00:00:00Z"
    valid_at = f"{D.isoformat()}T06:00:00Z"
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
        " VALUES (?, 'temperature', ?, 10.0, 2)",
        (site_id, valid_at),
    )
    conn.commit()

    target = FeedFetchTarget(
        site_id=site_id,
        feed_id=feed_id,
        lat=0.0,
        lon=0.0,
        source="synthetic-pd-x",
        model="synthetic-pd-x",
        max_lead_hours=168,
    )
    sample = NormalizedSample(
        model="synthetic-pd-x",
        variable="temperature",
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=6,
        value=11.0,
        source_raw="synthetic-pd-x:raw",
        model_run_id=issued_at,
    )
    _persist_fetch_success(conn, target, FetchResult(samples=[sample], grid=None))
    conn.commit()

    count = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type = 'pair_and_score' AND site_id = ?",
        (site_id,),
    ).fetchone()[0]
    assert count == 1, (
        "M122 -> at this assertion: correct = 1 (close off: today's"
        " unconditional enqueue, 17), mutant M122 (enqueue_scoring_run"
        f" returns with the close off) = 0 -- got {count}"
    )


# ---------------------------------------------------------------------------
# PD4
#
# PD4 pins TODAY'S close-off schedule and, by design, may already pass
# before P4 is built.
# ---------------------------------------------------------------------------


def test_pd4_round_and_daytime_schedule_run_unconditionally_of_the_close_switch(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PD4 (schedule half): with the close OFF, a round try still fires at
    the scheduled local time for a subscribed feed -- the round/daytime
    schedule (section 4, 5) replaces the old interval-based
    ``_enqueue_due_feeds`` unconditionally, not gated on the close switch
    (17's round-schedule row: "live" from install, close off included).
    """
    from tests.helpers import MovingClock, patch_close_clock, tick
    from wxverify import config
    from wxverify.db.connection import close_db, init_db

    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001 - test inspects the real writer connection
    site_id = day_close_site(conn, close_on=False, with_records=False)
    ids = seed_round_schedule_feeds(conn, site_id, subscribe=("X",))
    conn.commit()

    standin = install_provider_standin(monkeypatch)
    from tests.helpers import (
        ROUND_SCHEDULE_SOURCES,
        ProviderReply,
        round_schedule_sample,
    )

    source = ROUND_SCHEDULE_SOURCES["X"]
    standin.program(
        source,
        ProviderReply(
            samples=(
                round_schedule_sample(
                    source,
                    "temperature",
                    f"{D.isoformat()}T00:00:00Z",
                    f"{D.isoformat()}T06:00:00Z",
                    6,
                    10.0,
                ),
            )
        ),
    )

    clock = MovingClock(datetime(D.year, D.month, D.day, 0, 10, 0, tzinfo=UTC))
    patch_close_clock(monkeypatch, clock)
    asyncio.run(tick(db, clock))

    row = conn.execute(
        "SELECT COUNT(*) FROM jobs WHERE type = 'fetch_feed' AND job_key = ?",
        (f"round:{ids['X']}",),
    ).fetchone()
    assert row[0] >= 1, (
        "M123 -> at this assertion: correct = at least one round-keyed job"
        " created at the scheduled local time, even with the close off"
        " (17), mutant M123 (the schedule waits for the switch) = no round"
        f" job created -- got {row[0]}"
    )


def test_pd4_cleanup_after_a_replay_publish_runs_with_the_close_off() -> None:
    """PD4 (cleanup half): the replay chain's ``publish`` -> ``cleanup``
    transition (14.4, the same mechanism WC7 pins in
    ``tests/test_worksheet_cleanup.py``) runs with the close OFF -- the
    state this fixture builds in by never touching ``DAILY_CLOSE_ON_KEY``
    (section 17: live from install, close off included; the replay chain
    itself consults no close-switch state at all).

    Shares ``tests.helpers.build_verification_replay_site`` and its
    drivers with WC7; this test is the close-off half of the identical
    mechanism, asserted independently so M123's "the schedule waits for
    the switch" claim is checked against the cleanup path too, not only
    the round/daytime schedule above.
    """
    from tests.helpers import (
        build_verification_replay_site,
        drive_verification_chain,
        drive_verification_chain_until_phase,
    )
    from wxverify.db.site_days import daily_close_on
    from wxverify.verification.runs import published_run_id
    from wxverify.verification.truth import mark_daily_truth_stale
    from wxverify.worker.verification_run import (
        _load_state,  # noqa: SLF001
        advance_verification,
    )

    conn = asof_conn()
    assert daily_close_on(conn) is False, (
        "fixture bug: this test's whole point is the close-off state"
    )

    site_id, _feeds = build_verification_replay_site(conn)
    drive_verification_chain(conn, site_id, {"trigger_date": "2026-07-06"})
    run1 = published_run_id(conn, site_id)
    assert run1 is not None, "fixture bug: the first drive must publish a run"

    for hour in range(24):
        conn.execute(
            "INSERT INTO observations"
            " (site_id, variable, valid_at, value, n_stations, computed_at)"
            " VALUES (?, 'temperature', ?, ?, 3, '2026-07-07T02:00:00Z')",
            (site_id, f"2026-07-03T{hour:02d}:00:00Z", 10.0 + 0.25 * hour),
        )
    marked = mark_daily_truth_stale(
        conn, site_id=site_id, variable="temperature", valid_at="2026-07-03T12:00:00Z"
    )
    assert marked == 2, "fixture bug: exactly the two temperature quantities"
    conn.commit()

    drive_verification_chain_until_phase(
        conn, site_id, {"trigger_date": "2026-07-08"}, "publish"
    )
    assert daily_close_on(conn) is False, (
        "fixture bug: nothing in this drive may turn the close on"
    )
    blob = _load_state(conn, site_id)
    assert blob is not None and blob.get("phase") == "publish"
    run2 = blob["run_id"]
    assert run2 != run1

    advance_verification(conn, site_id, {"trigger_date": "2026-07-08"})
    conn.commit()
    assert published_run_id(conn, site_id) == run2

    for _ in range(200):
        blob = _load_state(conn, site_id)
        if blob is None:
            break
        advance_verification(conn, site_id, {"trigger_date": "2026-07-08"})
        conn.commit()
    else:
        raise AssertionError("the cleanup sub-chain never cleared its state")

    assert daily_close_on(conn) is False, (
        "fixture bug: the close must still be off at the end of the drive"
    )
    run1_evidence = conn.execute(
        "SELECT COUNT(*) FROM verification_evidence WHERE run_id = ?", (run1,)
    ).fetchone()[0]
    assert run1_evidence == 0, (
        "M123 -> at this assertion: correct = 0 (the cleanup after a"
        " replay publish runs unconditionally of the close switch, 17),"
        " mutant M123 (the cleanup waits for the switch too) = run1's"
        f" evidence still present with the close off -- got {run1_evidence}"
    )
    run2_evidence = conn.execute(
        "SELECT COUNT(*) FROM verification_evidence WHERE run_id = ?", (run2,)
    ).fetchone()[0]
    assert run2_evidence > 0, (
        "paired positive: the now-published run's own evidence is never"
        " touched (14.1), close off included"
    )


# ---------------------------------------------------------------------------
# PD5
# ---------------------------------------------------------------------------

_P4_BODY_MODULES = (
    "wxverify.worker.round",
    "wxverify.scoring.close_scores",
    "wxverify.scoring.publication",
    "wxverify.verification.cleanup",
)
_CHAIN_MODULES = (
    "wxverify.worker.close_chain",
    "wxverify.worker.scheduler",
    "wxverify.worker.processor",
)


def _loaded_modules_after_import(module: str, probe: tuple[str, ...]) -> str:
    """Run ``import <module>`` alone in a fresh interpreter and report
    which of ``probe`` landed in ``sys.modules`` (16.1).
    """
    code = (
        "import sys\n"
        f"import {module}\n"
        f"print(','.join(m for m in {probe!r} if m in sys.modules))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
    )
    stderr_tail = "\n".join(result.stderr.strip().splitlines()[-5:])
    assert result.returncode == 0, (
        f"import {module!r} failed in the fresh interpreter (exit"
        f" {result.returncode}); stderr tail:\n{stderr_tail}"
    )
    return result.stdout.strip()


def test_pd5_close_p4_import_alone_pulls_in_no_p4_body_module() -> None:
    """PD5 (i): importing ``wxverify.worker.close_p4`` alone, in a fresh
    interpreter, pulls none of P4's four body modules into
    ``sys.modules`` -- each call point imports its body inside the
    function, at call time (16.1).

    Passes before P4: it guards that piece 6 keeps ``close_p4``'s imports
    deferred, and today's ``close_p4.py`` already meets that rule, so this
    case is green now and stays green as a regression guard through P4.
    """
    loaded = _loaded_modules_after_import("wxverify.worker.close_p4", _P4_BODY_MODULES)
    assert loaded == "", (
        "M141 -> at this assertion: correct = '' (close_p4 keeps its"
        " module imports to db.*, deferring each P4 body import inside its"
        " function, 16.1), mutant M141 (worker/round.py imports"
        " worker/close_chain.py at the top) = loading close_p4 alone pulls"
        f" that import chain in transitively through round.py -- got"
        f" {loaded!r}"
    )


@pytest.mark.parametrize("module", _P4_BODY_MODULES)
def test_pd5_each_p4_body_module_alone_pulls_in_none_of_the_chain(
    module: str,
) -> None:
    """PD5 (ii): each of P4's four body modules, imported alone in a fresh
    interpreter, pulls none of ``close_chain``, ``scheduler`` or
    ``processor`` into ``sys.modules`` (16.1: no P4 module imports any of
    the three, directly or through another module).
    """
    loaded = _loaded_modules_after_import(module, _CHAIN_MODULES)
    if module == "wxverify.worker.round":
        reason = (
            "M141 -> at this assertion: correct = '' (worker/round.py's own"
            " top-level imports reach none of the three, 16.1), mutant M141"
            " (worker/round.py imports worker/close_chain.py at the top) ="
            f" loading {module} alone pulls it into sys.modules -- got"
            f" {loaded!r}"
        )
    else:
        # M141 only names worker/round.py; for the other three body modules
        # this is a general regression guard for 16.1's import-direction
        # rule, not a claim of killing M141 specifically.
        reason = (
            f"at this assertion: correct = '' (none of {module}'s own"
            " top-level imports reach worker/close_chain.py, worker/"
            "scheduler.py or worker/processor.py, 16.1), a similar"
            f" top-level import added to {module} = loading it alone would"
            f" pull at least one of them into sys.modules -- got {loaded!r}"
        )
    assert loaded == "", reason
