"""``catchup._due_open_meteo_targets`` must fail closed on a corrupt/foreign
``fetch_interval_minutes`` (via ``worker.cadence.parse_fetch_interval_minutes``)
or an unparseable ``last_run_at``: skip the affected feed for this tick,
never invent a schedule for a metered provider call, and never crash the
whole catchup pass -- a healthy sibling feed in the same site must still be
picked up.

Also covers ``worker.cadence.effective_due_minutes`` itself (the 690-720
lead band), its pin against ``monitor.FEED_STALE_HOURS``, and the shared
cadence rule's parity between the two sites that call it --
``catchup._due_open_meteo_targets`` and ``scheduler._enqueue_due_feeds`` --
for the cases where each site keeps its own (sometimes opposite) handling.

All fixture data is synthetic -- this is a PUBLIC repo.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from wxverify import config, monitor
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import close_db, init_db
from wxverify.worker.cadence import FEED_STALE_LIMIT_MINUTES, effective_due_minutes
from wxverify.worker.catchup import CatchupSite, _due_open_meteo_targets
from wxverify.worker.scheduler import _enqueue_due_feeds  # noqa: SLF001


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
            VALUES ('Q', 40, -105, 900, 'UTC')
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
    ["9e999", "x'0001'", "0", "-60", "43201", "1.9", "43199.5"],
    ids=[
        "real_infinity",
        "blob",
        "zero",
        "negative",
        "over_max",
        "non_integral_fraction",
        "non_integral_large_in_range",
    ],
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

    site = CatchupSite(site_id=site_id, lat=40.0, lon=-105.0, timezone="UTC")
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

    site = CatchupSite(site_id=site_id, lat=40.0, lon=-105.0, timezone="UTC")
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


@pytest.mark.parametrize(
    "literal",
    ["1.9", "43199.5"],
    ids=["non_integral_fraction", "non_integral_large_in_range"],
)
def test_hostile_non_integral_fetch_interval_minutes_with_null_last_run_at_skips(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, literal: str
) -> None:
    """Same NULL-``last_run_at`` guard-ordering requirement as the blob/
    infinity cases, exercised for a non-integral REAL specifically."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    control_feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    bad_feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    conn.execute(
        f"UPDATE feeds SET fetch_interval_minutes = {literal} WHERE id = ?",
        (bad_feed_id,),
    )
    # No site_feed_state row for either feed -> both have NULL last_run_at.

    site = CatchupSite(site_id=site_id, lat=40.0, lon=-105.0, timezone="UTC")
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


def test_exact_integral_real_fetch_interval_minutes_is_accepted(
    tmp_path: Path,
) -> None:
    """Regression pin: a whole-number cadence written via a REAL literal
    (e.g. 360.0) -- which this INTEGER-affinity column stores losslessly as
    the integer 360 -- is a legitimate cadence and must still schedule the
    feed, not be swept up by the non-integral rejection. The genuine
    REAL-carrier acceptance is pinned at the unit layer in
    test_cadence_parse.py::test_accepts_exact_integral_real."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    far_past = isoformat_utc(utc_now() - timedelta(days=30))
    _subscribe(conn, site_id, feed_id, last_run_at=far_past)
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 360.0 WHERE id = ?", (feed_id,)
    )

    site = CatchupSite(site_id=site_id, lat=40.0, lon=-105.0, timezone="UTC")
    targets = _due_open_meteo_targets(conn, site=site, window_end=utc_now())

    assert feed_id in {t.feed_id for t in targets}


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

    site = CatchupSite(site_id=site_id, lat=40.0, lon=-105.0, timezone="UTC")
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
    """A well-formed near-``datetime.max`` stamp carrying a non-UTC offset:
    ``datetime.fromisoformat`` accepts it, and it is the ``.astimezone(UTC)``
    conversion that overflows -- a carrier ``parse_utc`` now normalizes to
    ``ValueError``. The broad catch here is retained deliberately anyway:
    the carrier set is not enumerable, and this reader must fail closed on
    any unreadable shape, not just ``'not-a-timestamp'``-style garbage.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    control_feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    bad_feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    far_past = isoformat_utc(utc_now() - timedelta(days=30))
    _subscribe(conn, site_id, control_feed_id, last_run_at=far_past)
    _subscribe(conn, site_id, bad_feed_id, last_run_at="9999-12-31T23:59:59-14:00")

    site = CatchupSite(site_id=site_id, lat=40.0, lon=-105.0, timezone="UTC")
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

    site = CatchupSite(site_id=site_id, lat=40.0, lon=-105.0, timezone="UTC")
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

    site = CatchupSite(site_id=site_id, lat=40.0, lon=-105.0, timezone="UTC")
    targets = _due_open_meteo_targets(conn, site=site, window_end=now)

    assert feed_id not in {t.feed_id for t in targets}


# ---------------------------------------------------------------------------
# T17 -- effective_due_minutes: the 690-720 lead band, table-driven over all
# eight plan values (both edges of the band and a point on either side).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "interval_minutes,expected",
    [
        (720, 690),
        (700, 690),
        (691, 690),
        (690, 690),
        (689, 689),
        (360, 360),
        (721, 721),
        (1440, 1440),
    ],
)
def test_t17_effective_due_minutes_band(interval_minutes: int, expected: int) -> None:
    """Each pair pins a distinct way the band could be built wrong.

    720 -> 690 catches 720 left out of the band (an unfixed `<` instead of
    `<=` at the limit would return 720 unchanged here).
    360 -> 360 catches the lead taken off every interval (that bug would
    return 330).
    1440 -> 1440 catches every interval above 690 capped (that bug would
    return 690).
    """
    assert effective_due_minutes(interval_minutes) == expected


# ---------------------------------------------------------------------------
# T19 -- `_due_open_meteo_targets` with a real 720-minute-interval feed
# (`gem_global`, seeded at 720 by config.OPEN_METEO_FETCH_INTERVAL_MINUTES),
# last run 689 or 690 minutes before window_end. Catches the helper wired
# into the scheduler only, never into catchup (T18, in
# test_open_meteo_run_attribution.py, pins the scheduler side).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "minutes_before,expect_due",
    [(689, False), (690, True)],
)
def test_t19_catchup_720_minute_feed_due_at_690_not_689(
    tmp_path: Path, minutes_before: int, expect_due: bool
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "gem_global")
    seeded = conn.execute(
        "SELECT fetch_interval_minutes FROM feeds WHERE id = ?", (feed_id,)
    ).fetchone()
    # At a seeded 690 the raw rule would pass both rows too, and this test
    # would stop catching the helper missing from catchup.
    assert seeded["fetch_interval_minutes"] == 720
    window_end = datetime(2026, 1, 1, tzinfo=UTC)
    last_run_at = isoformat_utc(window_end - timedelta(minutes=minutes_before))
    _subscribe(conn, site_id, feed_id, last_run_at=last_run_at)

    site = CatchupSite(site_id=site_id, lat=40.0, lon=-105.0, timezone="UTC")
    targets = _due_open_meteo_targets(conn, site=site, window_end=window_end)

    is_due = feed_id in {t.feed_id for t in targets}
    assert is_due is expect_due


# ---------------------------------------------------------------------------
# T20 -- NULL last_run_at, an unreadable interval, and an unreadable
# last_run_at, each exercised at BOTH due sites, so a fix that merges the
# scheduler's and catchup's due rules into one shared helper body and drops
# either site's own handling of these three cases cannot pass.
#
# The catchup side of all three cases is already covered by existing tests
# above (reused here rather than duplicated):
#   - NULL last_run_at -> due:
#     test_valid_interval_and_null_last_run_at_are_due
#   - unreadable interval -> skipped (continue, fails CLOSED):
#     test_hostile_fetch_interval_minutes_skips_only_that_feed
#   - unreadable last_run_at -> skipped (fails CLOSED):
#     test_unparseable_last_run_at_skips_only_that_feed and
#     test_overflowing_last_run_at_skips_only_that_feed
#
# The three tests below are the scheduler-side half of each pair.
# ---------------------------------------------------------------------------


def _fetch_feed_jobs(conn: sqlite3.Connection, feed_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM jobs WHERE type = 'fetch_feed' AND job_key = ?",
        (f"fetch:{feed_id}",),
    ).fetchall()


def test_t20_scheduler_null_last_run_at_is_due(tmp_path: Path) -> None:
    """Scheduler-side half of T20's NULL last_run_at case: a never-run feed
    is due unconditionally, same direction as the catchup side pinned by
    test_valid_interval_and_null_last_run_at_are_due.
    """
    conn = _init_tmp_db(tmp_path)
    _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    # No site_feed_state row -> last_run_at is NULL via the LEFT JOIN.

    _enqueue_due_feeds(conn)

    assert len(_fetch_feed_jobs(conn, feed_id)) == 1


def test_t20_scheduler_hostile_fetch_interval_skips_only_that_feed(
    tmp_path: Path,
) -> None:
    """Scheduler-side half of T20's unreadable-interval case: fails CLOSED
    (skip, via `continue`) before any due decision, same direction as the
    catchup side pinned by
    test_hostile_fetch_interval_minutes_skips_only_that_feed.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    control_feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    bad_feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    far_past = isoformat_utc(utc_now() - timedelta(days=30))
    _subscribe(conn, site_id, control_feed_id, last_run_at=far_past)
    _subscribe(conn, site_id, bad_feed_id, last_run_at=far_past)
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 9e999 WHERE id = ?",
        (bad_feed_id,),
    )

    _enqueue_due_feeds(conn)

    assert len(_fetch_feed_jobs(conn, control_feed_id)) == 1
    assert _fetch_feed_jobs(conn, bad_feed_id) == []


def test_t20_scheduler_unreadable_last_run_at_fails_open_and_is_due(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Scheduler-side half of T20's unreadable-last_run_at case: fails OPEN
    (due) -- the OPPOSITE direction from catchup's fail-CLOSED handling
    pinned by test_unparseable_last_run_at_skips_only_that_feed. Each site
    keeps its own direction for this case; a merged rule could only pick
    one direction and would fail either this test or that one.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _subscribe(conn, site_id, feed_id, last_run_at="not-a-timestamp")

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.scheduler"):
        _enqueue_due_feeds(conn)

    assert len(_fetch_feed_jobs(conn, feed_id)) == 1
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "unreadable last_run_at" in warnings[0].getMessage()
    assert f"feed={feed_id}" in warnings[0].getMessage()


# ---------------------------------------------------------------------------
# T21 -- pin: cadence.py's own copy of the stale limit must track the
# monitor's, without importing it (the `#:` comment on
# FEED_STALE_LIMIT_MINUTES in cadence.py: "cadence must not depend on the
# monitor").
# ---------------------------------------------------------------------------


def test_t21_feed_stale_limit_pinned_to_monitor_feed_stale_hours() -> None:
    """If the monitor's stale-hours limit changes without this constant
    following it, the 30-minute lead silently stops leading the actual
    stale limit (or starts leading a number that no longer means anything).
    """
    assert FEED_STALE_LIMIT_MINUTES == monitor.FEED_STALE_HOURS * 60
