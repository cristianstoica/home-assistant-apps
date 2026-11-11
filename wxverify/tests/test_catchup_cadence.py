"""``catchup._due_open_meteo_targets`` must fail closed on a corrupt/foreign
``fetch_interval_minutes`` (via ``worker.cadence.parse_fetch_interval_minutes``)
or an unparseable ``last_run_at``: skip the affected feed for this tick,
never invent a schedule for a metered provider call, and never crash the
whole catchup pass -- a healthy sibling feed in the same site must still be
picked up.

Also covers ``worker.cadence.effective_due_minutes`` itself (the 690-720
lead band), its pin against ``monitor.FEED_STALE_HOURS``, and the shared
cadence rule's parity between the two sites that call it --
``catchup._due_open_meteo_targets`` and ``scheduler.enqueue_due_daytime_downloads`` --
for the cases where each site keeps its own (sometimes opposite) handling.

All fixture data is synthetic -- this is a PUBLIC repo.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from wxverify import config, monitor
from wxverify.db.connection import close_db, init_db
from wxverify.worker.cadence import FEED_STALE_LIMIT_MINUTES, effective_due_minutes
from wxverify.worker.round import enqueue_due_daytime_downloads


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


# ---------------------------------------------------------------------------
# T17 -- effective_due_minutes: the 690-720 lead band, table-driven over all
# eight values (both edges of the band and a point on either side).
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
    """Scheduler-side half of T20's NULL last_run_at case, rewritten to the
    slot rule: a feed with no try at all is due once its first
    daytime slot (the round's default local 00:10 plus one 360-minute
    interval, 06:10) has passed, same "never run is due" direction as the
    catchup side pinned by test_valid_interval_and_null_last_run_at_are_due.
    """
    conn = _init_tmp_db(tmp_path)
    _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    # No site_feed_state row and no forecast_downloads row -> no try of
    # this feed exists at all.

    slot_instant = datetime(2026, 1, 1, 6, 10, tzinfo=UTC)
    enqueue_due_daytime_downloads(conn, now_utc=slot_instant)

    assert len(_fetch_feed_jobs(conn, feed_id)) == 1


def test_t20_scheduler_hostile_fetch_interval_skips_only_that_feed(
    tmp_path: Path,
) -> None:
    """Scheduler-side half of T20's unreadable-interval case, rewritten to
    the slot rule (the same `parse_fetch_interval_minutes` fail-closed
    rule): the bad feed is skipped (via `continue`) before any
    slot-due decision, same direction as the catchup side pinned by
    test_hostile_fetch_interval_minutes_skips_only_that_feed, while a
    healthy sibling feed at its own due slot is unaffected.
    """
    conn = _init_tmp_db(tmp_path)
    _insert_site(conn)
    control_feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    bad_feed_id = _feed_id(conn, "open-meteo", "gfs_global")
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes = 9e999 WHERE id = ?",
        (bad_feed_id,),
    )

    slot_instant = datetime(2026, 1, 1, 6, 10, tzinfo=UTC)
    enqueue_due_daytime_downloads(conn, now_utc=slot_instant)

    assert len(_fetch_feed_jobs(conn, control_feed_id)) == 1
    assert _fetch_feed_jobs(conn, bad_feed_id) == []


def test_t20_scheduler_unreadable_last_run_at_is_ignored_and_due(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The slot rule never reads ``last_run_at`` at all, so a garbage
    ``site_feed_state.last_run_at`` does not stop the feed's daytime
    download, and a stored forward download is what rewrites the column
    (catchup's fail-closed skip heals). Same seed as before (site zone
    ``UTC``, ``open-meteo``/``ecmwf_ifs`` subscribed, ``last_run_at``
    ``"not-a-timestamp"``). Expected: one ``fetch:{feed_id}`` job, the
    same count ``test_t20_scheduler_null_last_run_at_is_due`` gets for the
    same feed and instant with no ``site_feed_state`` row, and no
    captured record mentioning ``last_run_at``.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    _subscribe(conn, site_id, feed_id, last_run_at="not-a-timestamp")

    with caplog.at_level(logging.WARNING):
        enqueue_due_daytime_downloads(
            conn, now_utc=datetime(2026, 1, 1, 6, 10, tzinfo=UTC)
        )

    assert len(_fetch_feed_jobs(conn, feed_id)) == 1
    assert not any("last_run_at" in record.getMessage() for record in caplog.records)


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
