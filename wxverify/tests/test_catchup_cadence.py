"""``catchup._due_open_meteo_targets`` must fail closed on a corrupt/foreign
``fetch_interval_minutes`` (via ``worker.cadence.parse_fetch_interval_minutes``)
or an unparseable ``last_run_at``: skip the affected feed for this tick,
never invent a schedule for a metered provider call, and never crash the
whole catchup pass -- a healthy sibling feed in the same site must still be
picked up.

All fixture data is synthetic -- this is a PUBLIC repo.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from wxverify import config
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import close_db, init_db
from wxverify.worker.catchup import CatchupSite, _due_open_meteo_targets


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


def _insert_site(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('Q', 47, 25, 900, 'UTC')
            """
        ).lastrowid
    )


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = ? AND model = ?", (source, model)
    ).fetchone()
    assert row is not None, f"seed feed missing: {source}/{model}"
    return int(row["id"])


def _subscribe(
    conn: sqlite3.Connection, site_id: int, feed_id: int, *, last_run_at: str
) -> None:
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled, last_run_at)
        VALUES (?, ?, 1, ?)
        """,
        (site_id, feed_id, last_run_at),
    )


@pytest.mark.parametrize(
    "literal",
    ["9e999", "x'0001'", "0", "-60", "43201"],
    ids=["real_infinity", "blob", "zero", "negative", "over_max"],
)
def test_hostile_fetch_interval_minutes_skips_only_that_feed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, literal: str
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    control_feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    bad_feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    far_past = isoformat_utc(utc_now() - timedelta(days=30))
    _subscribe(conn, site_id, control_feed_id, last_run_at=far_past)
    _subscribe(conn, site_id, bad_feed_id, last_run_at=far_past)
    conn.execute(
        f"UPDATE feeds SET fetch_interval_minutes = {literal} WHERE id = ?",
        (bad_feed_id,),
    )

    site = CatchupSite(site_id=site_id, lat=47.0, lon=25.0, timezone="UTC")
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.cadence"):
        targets = _due_open_meteo_targets(conn, site=site, window_end=utc_now())

    target_feed_ids = {t.feed_id for t in targets}
    assert control_feed_id in target_feed_ids, (
        "the healthy sibling feed must still be scheduled this tick"
    )
    assert bad_feed_id not in target_feed_ids, (
        "a feed with a rejected fetch_interval_minutes must not be scheduled "
        "-- inventing a cadence would charge a metered provider call"
    )

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, (
        f"expected exactly one warning; got: {[r.getMessage() for r in warnings]}"
    )
    assert "fetch_interval_minutes" in warnings[0].getMessage()
    assert f"feed={bad_feed_id}" in warnings[0].getMessage()


def test_hostile_fetch_interval_minutes_with_null_last_run_at_skips_that_feed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A never-run feed (NULL last_run_at) with a hostile cadence must not
    be treated as due unconditionally -- the cadence guard has to run BEFORE
    the due decision, not only inside the elapsed-time branch that a NULL
    last_run_at never reaches.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    control_feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    bad_feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 9e999 WHERE id = ?",
        (bad_feed_id,),
    )
    # No site_feed_state row for either feed -> both have NULL last_run_at.

    site = CatchupSite(site_id=site_id, lat=47.0, lon=25.0, timezone="UTC")
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.cadence"):
        targets = _due_open_meteo_targets(conn, site=site, window_end=utc_now())

    target_feed_ids = {t.feed_id for t in targets}
    assert control_feed_id in target_feed_ids, (
        "the healthy never-run sibling feed must still be scheduled this tick"
    )
    assert bad_feed_id not in target_feed_ids, (
        "a never-run feed with a rejected fetch_interval_minutes must not be "
        "scheduled unconditionally -- inventing a cadence would charge a "
        "metered provider call"
    )


def test_unparseable_last_run_at_skips_only_that_feed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    control_feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    bad_feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    far_past = isoformat_utc(utc_now() - timedelta(days=30))
    _subscribe(conn, site_id, control_feed_id, last_run_at=far_past)
    _subscribe(conn, site_id, bad_feed_id, last_run_at="not-a-timestamp")

    site = CatchupSite(site_id=site_id, lat=47.0, lon=25.0, timezone="UTC")
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.catchup"):
        targets = _due_open_meteo_targets(conn, site=site, window_end=utc_now())

    target_feed_ids = {t.feed_id for t in targets}
    assert control_feed_id in target_feed_ids
    assert bad_feed_id not in target_feed_ids

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "last_run_at" in warnings[0].getMessage()
    assert f"feed={bad_feed_id}" in warnings[0].getMessage()


def test_overflowing_last_run_at_skips_only_that_feed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A well-formed near-``datetime.max`` stamp carrying a UTC offset is not
    a ``ValueError`` -- ``datetime.fromisoformat`` accepts it, and it is
    ``parse_utc``'s own ``.astimezone(UTC)`` that raises ``OverflowError``.
    The catch here must be broad enough to still fail closed on this shape,
    not just on ``'not-a-timestamp'``-style garbage.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    control_feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    bad_feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    far_past = isoformat_utc(utc_now() - timedelta(days=30))
    _subscribe(conn, site_id, control_feed_id, last_run_at=far_past)
    _subscribe(conn, site_id, bad_feed_id, last_run_at="9999-12-31T23:59:59-14:00")

    site = CatchupSite(site_id=site_id, lat=47.0, lon=25.0, timezone="UTC")
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.catchup"):
        targets = _due_open_meteo_targets(conn, site=site, window_end=utc_now())

    target_feed_ids = {t.feed_id for t in targets}
    assert control_feed_id in target_feed_ids
    assert bad_feed_id not in target_feed_ids

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "last_run_at" in warnings[0].getMessage()
    assert f"feed={bad_feed_id}" in warnings[0].getMessage()


def test_valid_interval_and_null_last_run_at_are_due(tmp_path: Path) -> None:
    """Paired positive: a never-run feed (NULL last_run_at) is due
    unconditionally, and a valid cadence that has genuinely elapsed is also
    due -- proving the skip above is about the corrupt values specifically,
    not a bug that always skips.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    never_run_feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    elapsed_feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    far_past = isoformat_utc(utc_now() - timedelta(days=30))
    _subscribe(conn, site_id, elapsed_feed_id, last_run_at=far_past)
    # never_run_feed_id: no site_feed_state row at all -> last_run_at is NULL
    # via the LEFT JOIN, so it must be due regardless of elapsed time, given
    # a parseable cadence.

    site = CatchupSite(site_id=site_id, lat=47.0, lon=25.0, timezone="UTC")
    targets = _due_open_meteo_targets(conn, site=site, window_end=utc_now())

    target_feed_ids = {t.feed_id for t in targets}
    assert never_run_feed_id in target_feed_ids
    assert elapsed_feed_id in target_feed_ids


def test_not_yet_elapsed_valid_interval_is_not_due(tmp_path: Path) -> None:
    """Negative complement: a feed with a VALID cadence that has NOT yet
    elapsed since last_run_at must not be scheduled -- proves the elapsed
    check itself still runs correctly once cadence parsing succeeds.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    now = datetime(2026, 1, 1, tzinfo=UTC)
    recent = isoformat_utc(now - timedelta(minutes=1))
    _subscribe(conn, site_id, feed_id, last_run_at=recent)

    site = CatchupSite(site_id=site_id, lat=47.0, lon=25.0, timezone="UTC")
    targets = _due_open_meteo_targets(conn, site=site, window_end=now)

    assert feed_id not in {t.feed_id for t in targets}
