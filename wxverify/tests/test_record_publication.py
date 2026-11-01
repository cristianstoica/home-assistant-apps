"""The 07:00 record (daily-close plan 2026-10-22-daily-close-pairs-and-
publication.md, section 12; test IDs RC1-RC5, plan 18.10).

``compute_forecast_record``/``build_forecast_record`` (``wxverify.verification
.record``) already exist at HEAD, built for today's (close-off) behavior;
section 12 is piece 7's rewrite of their close-on branch. Every test here
seeds a real ``publications`` row by plain INSERT (piece 1's schema, not
built through ``publish``, per plan 18.1/20's piece rule -- RC rows go
green in piece 7, after ``publish`` exists, but seeding one directly keeps
this file independent of piece 6's own correctness) and calls
``build_forecast_record`` directly, so before piece 7 these tests fail on
a missing column (``forecast_of_record.publication_id`` etc, added in
piece 1) or, before piece 1, on the ``publications`` table itself being
absent -- the failure Step 3 accepts.

Real SQLite throughout, through ``init_db`` on a ``tmp_path`` file. Zone
UTC; T (the snapshot instant) is D 07:00Z under the methodology default
(``SNAPSHOT_LOCAL_TIME``, unset here) -- plan 18.10's own fixture.
Synthetic data only: feed source ``synthetic-rc-x``, no real id or zone.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import D, day_close_site
from wxverify import config
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.connection import close_db, init_db
from wxverify.verification.methodology import METHODOLOGY_VERSION

CUTOFF_D1 = D - timedelta(days=1)
CUTOFF_D2 = D - timedelta(days=2)
T = datetime(D.year, D.month, D.day, 7, 0, tzinfo=UTC)

# ---------------------------------------------------------------------------
# Shared fixtures and seed helpers (own-helpers rule: nothing here touches
# tests/helpers.py or another test file -- a leaner, independent copy of
# test_publication_build.py's own ``_seed_publication_direct``).
# ---------------------------------------------------------------------------


def _make_conn(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    tmp_path.mkdir(parents=True, exist_ok=True)
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 -- tests build the fixture on the real writer conn


def _make_feed(conn: sqlite3.Connection) -> int:
    cur = conn.execute(
        "INSERT INTO feeds"
        " (source, model, enabled, default_subscribed,"
        "  fetch_interval_minutes, max_lead_hours, is_virtual)"
        " VALUES ('synthetic-rc-x', 'synthetic-rc-x', 1, 0, 360, 168, 0)"
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _subscribe(conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )


def _seed_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    value: float,
    fetched_at: str,
) -> None:
    conn.execute(
        "INSERT INTO forecast_samples"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value,"
        "  source_raw, model_run_id, fetched_at)"
        " VALUES (?, ?, 'temperature', ?, ?, ?, ?, 'synthetic', ?, ?)",
        (
            site_id,
            feed_id,
            issued_at,
            valid_at,
            lead_hours,
            value,
            issued_at,
            fetched_at,
        ),
    )


def _seed_publication(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    cutoff,
    published_at: datetime,
    window_days: int = 5,
    min_n: int = 1,
) -> int:
    cur = conn.execute(
        "INSERT INTO publications"
        " (site_id, cutoff, first_record_date, window_days, window_start_date,"
        "  wind_start_date, settings, methodology_version, scoring_rule,"
        "  wind_method, wind_constants, build_started_at, published_at)"
        " VALUES (?,?,?,?,?,?,?,?,'first-stored-download-of-local-day/1',"
        "         'inverse_mae_daily_high_v1','{}',?,?)",
        (
            site_id,
            cutoff.isoformat(),
            (cutoff - timedelta(days=10)).isoformat(),
            window_days,
            (cutoff - timedelta(days=window_days - 1)).isoformat(),
            (cutoff - timedelta(days=29)).isoformat(),
            f'{{"rolling_window_days": {window_days}, "min_n": {min_n}}}',
            METHODOLOGY_VERSION,
            isoformat_utc(published_at),
            isoformat_utc(published_at),
        ),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _seed_ranking(
    conn: sqlite3.Connection,
    *,
    publication_id: int,
    feed_id: int,
    window_days: int,
) -> None:
    conn.execute(
        "INSERT INTO publication_rankings"
        " (publication_id, variable, day_ahead, window_key, feed_id, n, bias,"
        "  mae, rmse, pod, far, csi, ets, hss, skill_score)"
        " VALUES (?, 'temperature', 0, ?, ?, 10, 0.1, 0.5, 0.6,"
        "         NULL, NULL, NULL, NULL, NULL, 0.9)",
        (publication_id, f"w:{window_days}", feed_id),
    )


def _fetch_record_row(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    snapshot_local_date,
    target_date,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM forecast_of_record"
        " WHERE site_id = ? AND snapshot_local_date = ? AND variable = 'temperature'"
        " AND target_local_date = ?",
        (site_id, snapshot_local_date.isoformat(), target_date.isoformat()),
    ).fetchone()


def _assert_record_publication_columns(conn: sqlite3.Connection) -> None:
    """P4 piece 1 (15.3) adds ``publication_id``/``publication_cutoff``/
    ``publication_age_days`` to ``forecast_of_record``; assert each exists
    FIRST, with a message naming it as piece 1's column, so a Step-3
    failure here reads as a clear missing-column reason rather than a bare
    ``IndexError`` from the row access further down."""
    columns = {row[1] for row in conn.execute("PRAGMA table_info(forecast_of_record)")}
    for column in ("publication_id", "publication_cutoff", "publication_age_days"):
        assert column in columns, (
            f"forecast_of_record.{column} is P4 piece 1's column (15.3) --"
            " not yet migrated"
        )


def _build_site_with_sample(tmp_path: Path) -> tuple[sqlite3.Connection, int, int]:
    """Zone UTC, close on, one subscribed feed with one usable temperature
    sample valid at D 06:00Z (day ahead 0 from D)."""
    conn = _make_conn(tmp_path)
    site_id = day_close_site(conn, close_on=True, with_records=False)
    feed_x = _make_feed(conn)
    _subscribe(conn, site_id, feed_x)
    issued_at = f"{D.isoformat()}T00:00:00Z"
    valid_at = f"{D.isoformat()}T06:00:00Z"
    _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_x,
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=6,
        value=12.0,
        fetched_at=issued_at,
    )
    conn.commit()
    return conn, site_id, feed_x


# ---------------------------------------------------------------------------
# RC1
# ---------------------------------------------------------------------------


def test_rc1_record_uses_the_active_publication_at_t(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RC1: a D-1 publication published at D 01:30Z (before T, D 07:00Z) is
    active at T -> the record's saved publication_id/cutoff/age (1) match
    it, and leaderboard_status is NULL (the live as-of ranking is not
    read with the close on, 12).

    The ``leaderboard_status`` column is written by its OWN
    ``if close_on: None else: ...`` check (``verification/record.py``,
    the line building ``leaderboard_status``), independent of the earlier
    ``if not close_on and key not in status_cache:`` guard that
    ``_leaderboard_status_cell`` sits behind (M101's target): with the
    close on, that first guard merely controls whether the live
    leaderboard status cell is COMPUTED AT ALL, a wasted read whose result
    the second, unaffected check then always discards. So a bare
    ``leaderboard_status is None`` assertion cannot discriminate the
    mutant -- it is None either way. The spy below observes the guard
    directly: it must prevent the live read happening in the first place.
    """
    from wxverify.verification.record import build_forecast_record

    conn, site_id, feed_x = _build_site_with_sample(tmp_path)
    publication_id = _seed_publication(
        conn,
        site_id=site_id,
        cutoff=CUTOFF_D1,
        published_at=datetime(D.year, D.month, D.day, 1, 30, tzinfo=UTC),
    )
    _seed_ranking(conn, publication_id=publication_id, feed_id=feed_x, window_days=5)
    conn.commit()

    import wxverify.verification.record as record_module

    real_status_in_transaction = record_module.leaderboard_with_status_in_transaction
    calls = {"n": 0}

    def _counting_status_in_transaction(*args, **kwargs):
        calls["n"] += 1
        return real_status_in_transaction(*args, **kwargs)

    monkeypatch.setattr(
        record_module,
        "leaderboard_with_status_in_transaction",
        _counting_status_in_transaction,
    )

    build_forecast_record(conn, site_id, D.isoformat(), now=T)

    assert calls["n"] == 0, (
        "M101 -> at this assertion: correct = 0 (close on: the live"
        " leaderboard-status cell is never computed at all, section 12 --"
        " _leaderboard_status_cell, and so"
        " leaderboard_with_status_in_transaction, must not even be"
        " called), mutant M101 (the per-cell guard"
        " 'if not close_on and key not in status_cache:' loses its 'not"
        " close_on' term, so it fires as soon as a cell's key is first"
        " seen, close on or not) = at least 1 call -- the write still"
        " ends up None (a separate, unaffected check), so this count is"
        " the only place the mutant is visible"
    )

    row = _fetch_record_row(conn, site_id=site_id, snapshot_local_date=D, target_date=D)
    assert row is not None, "build_forecast_record must write at least one row"
    assert row["publication_id"] == publication_id
    assert row["publication_cutoff"] == CUTOFF_D1.isoformat()
    assert row["publication_age_days"] == 1
    assert row["leaderboard_status"] is None, (
        "the close-on record never calls _leaderboard_status_cell for its"
        " OWN write of this column (section 12) -- kept as a paired"
        " positive; the spy above is M101's actual kill, since both"
        " branches write None here regardless"
    )


# ---------------------------------------------------------------------------
# RC2
# ---------------------------------------------------------------------------


def test_rc2_the_record_uses_the_publication_active_at_t_not_the_latest(
    tmp_path: Path,
) -> None:
    """RC2: the D-1 publication published at D 08:00Z (after T); the D-2
    publication (published D-1 01:30Z) is the one active at T -> its id,
    age 2 (12)."""
    from wxverify.verification.record import build_forecast_record

    conn, site_id, feed_x = _build_site_with_sample(tmp_path)
    publication_d2 = _seed_publication(
        conn,
        site_id=site_id,
        cutoff=CUTOFF_D2,
        published_at=datetime(D.year, D.month, D.day, 1, 30, tzinfo=UTC)
        - timedelta(days=1),
    )
    _seed_publication(
        conn,
        site_id=site_id,
        cutoff=CUTOFF_D1,
        published_at=datetime(D.year, D.month, D.day, 8, 0, tzinfo=UTC),
    )
    _seed_ranking(conn, publication_id=publication_d2, feed_id=feed_x, window_days=5)
    conn.commit()

    build_forecast_record(conn, site_id, D.isoformat(), now=T)

    row = _fetch_record_row(conn, site_id=site_id, snapshot_local_date=D, target_date=D)
    assert row is not None
    assert row["publication_id"] == publication_d2, (
        "M102 -> at this assertion: correct = D-2's publication id (the"
        " one active AT T; D-1's own publication is not published until"
        " 08:00Z, after T), mutant M102 (the record reads the latest"
        " publication, not the one at T) would pick D-1's instead"
    )
    assert row["publication_age_days"] == 2


# ---------------------------------------------------------------------------
# RC3
# ---------------------------------------------------------------------------


def test_rc3_a_late_computation_still_resolves_the_publication_at_t(
    tmp_path: Path,
) -> None:
    """RC3: RC2's same seed, but the record is computed late, at D 09:00Z
    -- still resolves the publication active at T (D 07:00Z), not at the
    actual (later) computation time (12)."""
    from wxverify.verification.record import build_forecast_record

    conn, site_id, feed_x = _build_site_with_sample(tmp_path)
    publication_d2 = _seed_publication(
        conn,
        site_id=site_id,
        cutoff=CUTOFF_D2,
        published_at=datetime(D.year, D.month, D.day, 1, 30, tzinfo=UTC)
        - timedelta(days=1),
    )
    _seed_publication(
        conn,
        site_id=site_id,
        cutoff=CUTOFF_D1,
        published_at=datetime(D.year, D.month, D.day, 8, 0, tzinfo=UTC),
    )
    _seed_ranking(conn, publication_id=publication_d2, feed_id=feed_x, window_days=5)
    conn.commit()

    late_now = datetime(D.year, D.month, D.day, 9, 0, tzinfo=UTC)
    build_forecast_record(conn, site_id, D.isoformat(), now=late_now)

    row = _fetch_record_row(conn, site_id=site_id, snapshot_local_date=D, target_date=D)
    assert row is not None
    assert row["publication_id"] == publication_d2, (
        "M103 -> at this assertion: correct = D-2's publication id (the"
        " active-publication lookup uses T, D 07:00Z, not the build's own"
        " (later) clock), mutant M103 (at_utc taken from the build time)"
        " would resolve at 09:00Z instead, past D-1's 08:00Z publish, and"
        " pick D-1's publication"
    )
    assert row["publication_age_days"] == 2


# ---------------------------------------------------------------------------
# RC4
# ---------------------------------------------------------------------------


def test_rc4_no_publication_at_t_leaves_the_three_columns_null(
    tmp_path: Path,
) -> None:
    """RC4: no publication exists at all -> the three publication columns
    stay NULL; the feed is still recorded (unranked, selected by sample
    count, 12)."""
    from wxverify.verification.record import build_forecast_record

    conn, site_id, _feed_x = _build_site_with_sample(tmp_path)
    _assert_record_publication_columns(conn)

    build_forecast_record(conn, site_id, D.isoformat(), now=T)

    row = _fetch_record_row(conn, site_id=site_id, snapshot_local_date=D, target_date=D)
    assert row is not None, "a sample is knowable at T even with no publication"
    assert (
        row["publication_id"],
        row["publication_cutoff"],
        row["publication_age_days"],
    ) == (None, None, None), (
        "M104 -> at this assertion: correct = (None, None, None) (no"
        " publication at T), mutant M104 (no publication at T raises)"
        " would instead cancel the whole build, leaving no row at all"
    )


# ---------------------------------------------------------------------------
# RC5
# ---------------------------------------------------------------------------


def test_rc5_a_missed_row_leaves_the_three_columns_null(tmp_path: Path) -> None:
    """RC5: a ``missed`` row (the gap scan's own writer) leaves the three
    publication columns NULL too (12)."""
    from wxverify.db.switch import ensure_site_generation
    from wxverify.verification.record import _write_missed_rows

    conn, site_id, _feed_x = _build_site_with_sample(tmp_path)
    _assert_record_publication_columns(conn)
    generation_id = ensure_site_generation(conn, site_id)
    reasons = {
        (variable, day): "window_closed"
        for variable in ("temperature", "wind", "precip")
        for day in range(8)
    }

    _write_missed_rows(
        conn,
        site_id=site_id,
        generation_id=generation_id,
        timezone="UTC",
        wall_clock="07:00",
        local_date=D,
        snapshot_utc=T,
        reasons=reasons,
    )
    conn.commit()

    row = _fetch_record_row(conn, site_id=site_id, snapshot_local_date=D, target_date=D)
    assert row is not None
    assert row["status"] == "missed"
    assert (
        row["publication_id"],
        row["publication_cutoff"],
        row["publication_age_days"],
    ) == (None, None, None), (
        "M105 -> at this assertion: correct = (None, None, None) (a"
        " missed row carries no publication identity), mutant M105"
        " (missed rows given publication values) would stamp a"
        " publication onto a row that was never built from one"
    )
