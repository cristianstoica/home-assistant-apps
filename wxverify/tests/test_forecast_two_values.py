"""Tests for Part B of the rain release (plans/2026-11-01-rain-release.md):
storing two values per forecast hour -- the newest (``forecast_samples``,
now upserted) and the scored one (the new ``forecast_scored`` table, one row
per feed/variable/hour/day-ahead, written for a local day's FIRST stored
download only).

B1-B5, one test per row of the plan's test list. ``forecast_scored`` and
``idx_samples_slot`` do not exist before Part B is built, and
``forecast_samples``' ``INSERT OR IGNORE`` has not yet become the
``ON CONFLICT ... DO UPDATE`` upsert the plan describes -- every test here
fails now because of that: B1-B3 fail on either the still-``INSERT OR
IGNORE`` newest value (never upserted to a later download's revision) or a
missing/absent ``forecast_scored`` row; B4 fails at the ``CREATE TRIGGER``
itself (no such table); B5 fails at ``run_migrations``' second call
finding no ``migrate_v16`` to run (``PRAGMA user_version`` stays at 15).

Two constructions, by row:
- B1-B3 drive the real write path end to end through the job pipeline
  (``enqueue_if_absent``/``claim_next_job``/``run_claimed_job``, the
  ``ProviderStandIn`` pattern of ``tests/test_download_record.py``), since
  these rows test what a live download sequence persists.
- B4 forces the scored write to fail with a SQLite trigger on
  ``forecast_scored`` (``RAISE(ABORT, ...)``), rather than guessing the
  as-yet-unbuilt call site's name -- the trigger fires regardless of how
  the write is implemented, so the test is not coupled to that choice.
- B5 builds a made-up v15 file (``run_migrations`` stopped at 15, minus
  ``forecast_scored`` and ``idx_samples_slot``) with two labels at one
  hour, computes the 0.16.14 oracle with the CURRENT, unmodified
  ``scored_download``/
  ``scored_version`` before touching anything, then migrates and checks
  the carry-over against that oracle -- sharing no code with
  ``migrate_v16``'s own carry-over logic.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import (
    NOW,
    D,
    MovingClock,
    ProviderReply,
    asof_make_real_feed,
    day_close_site,
    disable_other_enqueuers,
    install_provider_standin,
    patch_close_clock,
    round_schedule_sample,
    seed_round_schedule_feeds,
    standard_night,
)
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import claim_next_job, enqueue_if_absent
from wxverify.forecast.data import load_future_samples
from wxverify.scoring.downloads import scored_download, scored_version
from wxverify.worker.processor import run_claimed_job

_SOURCE = "synthetic-round-x"


def _stamp(dt: datetime) -> str:
    return isoformat_utc(dt)


def _setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    clock_start: datetime | None = None,
) -> tuple[object, sqlite3.Connection, int, int, object, MovingClock]:
    """P3's standard night (daily close on), feed X only, the provider
    stand-in and a :class:`MovingClock` over every close-chain module, as
    ``tests/test_download_record.py``'s own ``_setup`` builds it. Returns
    ``(db, conn, site_id, feed_id, standin, clock)``.
    """
    db, _writer, site_id, _stations = standard_night(tmp_path)
    conn = db._conn  # noqa: SLF001 -- tests read the real writer conn directly
    feed_ids = seed_round_schedule_feeds(conn, site_id, subscribe=("X",))
    conn.execute("UPDATE feeds SET default_subscribed=0 WHERE source='open-meteo'")
    conn.commit()
    standin = install_provider_standin(monkeypatch)
    disable_other_enqueuers(monkeypatch)
    start = clock_start or datetime(D.year, D.month, D.day, 0, 0, tzinfo=UTC)
    clock = MovingClock(start)
    patch_close_clock(monkeypatch, clock)
    return db, conn, site_id, feed_ids["X"], standin, clock


def _run_fetch(
    db: object,
    conn: sqlite3.Connection,
    standin: object,
    feed_id: int,
    site_id: int,
    samples: tuple[object, ...],
    clock: MovingClock,
    at: datetime,
) -> object:
    """Move ``clock`` to ``at``, program ``standin`` with ``samples`` for
    :data:`_SOURCE`, then run one ``fetch_feed`` job for ``feed_id`` through
    the real job pipeline, exactly as ``tests/test_download_record.py``'s
    ``_run_one`` does for one feed. Returns the claimed job.
    """
    clock.set(at)
    standin.program(_SOURCE, ProviderReply(samples=samples))
    enqueue_if_absent(
        conn, "fetch_feed", site_id, f"fetch:{feed_id}", {"feed_id": feed_id}
    )
    conn.commit()
    job = claim_next_job(conn)
    assert job is not None, "no fetch_feed job was claimable"
    asyncio.run(run_claimed_job(db, job, lane="main"))
    conn.commit()
    return job


def _downloads(conn: sqlite3.Connection, feed_id: int) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT * FROM forecast_downloads WHERE feed_id=? ORDER BY id",
            (feed_id,),
        )
    )


def _scored(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    valid_at: str,
    day_ahead: int,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT value, download_date FROM forecast_scored"
        " WHERE site_id=? AND feed_id=? AND variable='temperature'"
        " AND valid_at=? AND day_ahead=?",
        (site_id, feed_id, valid_at, day_ahead),
    ).fetchone()


def _count(conn: sqlite3.Connection, table: str, feed_id: int) -> int:
    row = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE feed_id=?",  # noqa: S608
        (feed_id,),
    ).fetchone()
    return int(row[0])


def test_b1_same_label_two_values_newest_served_first_scored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B1: two downloads with the same label and different values, for each
    day ahead D+0 to D+7: the Forecast page reads the second; the close of
    each target day scores the first download. Both downloads share the
    SAME label and the SAME eight valid_at hours, so the second download's
    upsert also collides on the old ``UNIQUE(..., issued_at, valid_at)``,
    pinning that the new ``ON CONFLICT`` target resolves it rather than
    raising on the untargeted constraint.
    """
    s = D
    db, conn, site_id, feed_id, standin, clock = _setup(tmp_path, monkeypatch)
    label = _stamp(datetime(s.year, s.month, s.day, 0, 10, tzinfo=UTC))
    valid_ats = [
        _stamp(datetime(s.year, s.month, s.day, 12, 0, tzinfo=UTC) + timedelta(days=k))
        for k in range(8)
    ]

    first_samples = tuple(
        round_schedule_sample(_SOURCE, "temperature", label, valid_at, 12 + 24 * k, 1.0)
        for k, valid_at in enumerate(valid_ats)
    )
    _run_fetch(
        db,
        conn,
        standin,
        feed_id,
        site_id,
        first_samples,
        clock,
        datetime(s.year, s.month, s.day, 0, 10, tzinfo=UTC),
    )

    second_samples = tuple(
        round_schedule_sample(_SOURCE, "temperature", label, valid_at, 12 + 24 * k, 2.0)
        for k, valid_at in enumerate(valid_ats)
    )
    _run_fetch(
        db,
        conn,
        standin,
        feed_id,
        site_id,
        second_samples,
        clock,
        datetime(s.year, s.month, s.day, 6, 10, tzinfo=UTC),
    )

    downloads = _downloads(conn, feed_id)
    assert len(downloads) == 2, (
        f"expected 2 download rows (one per try), got {len(downloads)}"
    )
    assert downloads[1]["outcome"] == "stored", (
        "the second download, which collides with the first on BOTH"
        " forecast_samples unique keys, must still record 'stored' -- got"
        f" {downloads[1]['outcome']!r}"
    )

    sample_rows = conn.execute(
        "SELECT valid_at, value FROM forecast_samples"
        " WHERE site_id=? AND feed_id=? AND variable='temperature'"
        " ORDER BY valid_at",
        (site_id, feed_id),
    ).fetchall()
    assert len(sample_rows) == 8, (
        "forecast_samples must keep exactly one row per hour (upserted),"
        f" found {len(sample_rows)}"
    )
    for row in sample_rows:
        assert row["value"] == 2.0, (
            "forecast_samples must hold the second (newest) download's"
            f" value at {row['valid_at']!r}: correct = 2.0, got {row['value']!r}"
        )

    future_rows = load_future_samples(
        conn,
        site_id=site_id,
        since_valid_at=_stamp(datetime(s.year, s.month, s.day, 0, 0, tzinfo=UTC)),
        served_only=False,
    )
    page_values = {
        row.valid_at: row.value
        for row in future_rows
        if row.feed_id == feed_id and row.variable == "temperature"
    }
    for valid_at in valid_ats:
        assert page_values.get(valid_at) == 2.0, (
            "the Forecast page must read the second (newest) download's"
            f" value at {valid_at!r}: correct = 2.0, got"
            f" {page_values.get(valid_at)!r}"
        )

    for k, valid_at in enumerate(valid_ats):
        scored = _scored(
            conn, site_id=site_id, feed_id=feed_id, valid_at=valid_at, day_ahead=k
        )
        assert scored is not None, (
            f"day_ahead {k}: no forecast_scored row -- the close must score"
            " S's first download"
        )
        assert scored["value"] == 1.0, (
            f"day_ahead {k}: the close must score the FIRST download's"
            f" value: correct = 1.0, got {scored['value']!r}"
        )


def test_b2_evening_to_midnight_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B2: S-1 at 22:00 brings label L with 1.0, S at 00:30 brings L with
    2.0, S at 03:00 brings L with 3.0. The page shows 3.0; the close scores
    2.0 for S's day ahead (whose first download was at 00:30); 1.0 is
    scored only for S-1, never for S.
    """
    s = D
    s_minus_1 = s - timedelta(days=1)
    db, conn, site_id, feed_id, standin, clock = _setup(
        tmp_path,
        monkeypatch,
        clock_start=datetime(
            s_minus_1.year, s_minus_1.month, s_minus_1.day, 20, 0, tzinfo=UTC
        ),
    )
    label = _stamp(
        datetime(s_minus_1.year, s_minus_1.month, s_minus_1.day, 18, 0, tzinfo=UTC)
    )
    valid_at = _stamp(
        datetime(s.year, s.month, s.day, 12, 0, tzinfo=UTC) + timedelta(days=2)
    )

    _run_fetch(
        db,
        conn,
        standin,
        feed_id,
        site_id,
        (round_schedule_sample(_SOURCE, "temperature", label, valid_at, 66, 1.0),),
        clock,
        datetime(s_minus_1.year, s_minus_1.month, s_minus_1.day, 22, 0, tzinfo=UTC),
    )
    _run_fetch(
        db,
        conn,
        standin,
        feed_id,
        site_id,
        (round_schedule_sample(_SOURCE, "temperature", label, valid_at, 66, 2.0),),
        clock,
        datetime(s.year, s.month, s.day, 0, 30, tzinfo=UTC),
    )
    _run_fetch(
        db,
        conn,
        standin,
        feed_id,
        site_id,
        (round_schedule_sample(_SOURCE, "temperature", label, valid_at, 66, 3.0),),
        clock,
        datetime(s.year, s.month, s.day, 3, 0, tzinfo=UTC),
    )

    sample_row = conn.execute(
        "SELECT value FROM forecast_samples"
        " WHERE site_id=? AND feed_id=? AND variable='temperature' AND valid_at=?",
        (site_id, feed_id, valid_at),
    ).fetchone()
    assert sample_row is not None
    assert sample_row["value"] == 3.0, (
        "the Forecast page must show the 03:00 download's value: correct ="
        f" 3.0, got {sample_row['value']!r}"
    )

    # valid_at's local date is S + 2 days: S's day_ahead for this hour.
    scored_for_s = _scored(
        conn, site_id=site_id, feed_id=feed_id, valid_at=valid_at, day_ahead=2
    )
    assert scored_for_s is not None, (
        "no forecast_scored row for S -- the close must score the first"
        " download after S's local midnight (00:30)"
    )
    assert scored_for_s["value"] == 2.0, (
        "1.0 is never scored for S -- the close must score the 00:30"
        f" download: correct = 2.0, got {scored_for_s['value']!r}"
    )
    assert scored_for_s["download_date"] == s.isoformat(), (
        f"expected download_date {s.isoformat()!r}, got"
        f" {scored_for_s['download_date']!r}"
    )

    # valid_at's local date is (S-1) + 3 days: S-1's own day_ahead for this
    # hour -- the paired positive showing 1.0 IS scored, just under S-1,
    # not under S.
    scored_for_s_minus_1 = _scored(
        conn, site_id=site_id, feed_id=feed_id, valid_at=valid_at, day_ahead=3
    )
    assert scored_for_s_minus_1 is not None, (
        "fixture sanity: S-1's 22:00 download should have scored its own day ahead"
    )
    assert scored_for_s_minus_1["value"] == 1.0, (
        "S-1's own first (and only) download for S-1 must be scored as"
        f" 1.0, got {scored_for_s_minus_1['value']!r}"
    )


def test_b3_interim_downloads_not_kept_value_changes_are_pairwise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B3: after 4 downloads in one day, each hour has one forecast_samples
    row and one forecast_scored row per day ahead. The value-change rows
    compare each download with the value it replaced, not the first
    download's value.
    """
    s = D
    db, conn, site_id, feed_id, standin, clock = _setup(tmp_path, monkeypatch)
    valid_ats = [
        _stamp(datetime(s.year, s.month, s.day, 12, 0, tzinfo=UTC) + timedelta(days=k))
        for k in range(3)
    ]
    run_times = [
        datetime(s.year, s.month, s.day, hour, 10, tzinfo=UTC)
        for hour in (0, 6, 12, 18)
    ]
    values = (1.0, 2.0, 3.0, 4.0)
    for value, at in zip(values, run_times, strict=True):
        label = _stamp(at)
        samples = tuple(
            round_schedule_sample(
                _SOURCE, "temperature", label, valid_at, 12 + 24 * k, value
            )
            for k, valid_at in enumerate(valid_ats)
        )
        _run_fetch(db, conn, standin, feed_id, site_id, samples, clock, at)

    downloads = _downloads(conn, feed_id)
    assert len(downloads) == 4, f"expected 4 download rows, got {len(downloads)}"

    sample_rows = conn.execute(
        "SELECT valid_at, value FROM forecast_samples"
        " WHERE site_id=? AND feed_id=? AND variable='temperature'"
        " ORDER BY valid_at",
        (site_id, feed_id),
    ).fetchall()
    assert len(sample_rows) == 3, (
        f"forecast_samples must keep one row per hour, found {len(sample_rows)}"
    )
    for row in sample_rows:
        assert row["value"] == 4.0, (
            "interim downloads are not kept -- forecast_samples must hold"
            f" the LAST download's value, got {row['value']!r}"
        )

    for k, valid_at in enumerate(valid_ats):
        scored = _scored(
            conn, site_id=site_id, feed_id=feed_id, valid_at=valid_at, day_ahead=k
        )
        assert scored is not None, f"day_ahead {k}: no forecast_scored row"
        assert scored["value"] == 1.0, (
            "interim downloads are not kept -- forecast_scored must hold"
            f" the FIRST download's value: correct = 1.0, got {scored['value']!r}"
        )

    download_ids = [int(row["id"]) for row in downloads]
    changes = conn.execute(
        "SELECT download_id, previous_download_id, hours_compared,"
        " hours_changed, max_abs_change"
        " FROM forecast_value_changes WHERE feed_id=? ORDER BY download_id",
        (feed_id,),
    ).fetchall()
    # The first download has no previous stored download to compare
    # against, so only downloads 2-4 log a value-change row (one each,
    # since there is one variable, temperature).
    assert len(changes) == 3, (
        f"expected 3 value-change rows (downloads 2-4 only), got {len(changes)}"
    )
    for change, prev_id, this_id in zip(
        changes, download_ids[:-1], download_ids[1:], strict=True
    ):
        assert change["download_id"] == this_id, (
            f"expected download_id {this_id}, got {change['download_id']!r}"
        )
        assert change["previous_download_id"] == prev_id, (
            "each value-change row must compare against the download it"
            f" REPLACED ({prev_id}), not the first download, got"
            f" {change['previous_download_id']!r}"
        )
        assert change["hours_compared"] == 3
        assert change["hours_changed"] == 3
        assert change["max_abs_change"] == pytest.approx(1.0), (
            "each step changed every hour by exactly 1.0 from the value it"
            f" replaced, got {change['max_abs_change']!r}"
        )


def test_b4_failed_scored_write_rolls_back_the_whole_save(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B4: a failed save stores nothing. Force an error in the scored
    write with a SQLite trigger: no change to forecast_samples,
    forecast_scored or forecast_value_changes, forecast_downloads holds only
    the failure log's one 'failed' row, and the job is retried.
    """
    s = D
    db, conn, site_id, feed_id, standin, clock = _setup(
        tmp_path,
        monkeypatch,
        clock_start=datetime(s.year, s.month, s.day, 0, 10, tzinfo=UTC),
    )
    # Fires regardless of how/where the scored write is implemented, so
    # this test is not coupled to that call site's name. Raises
    # sqlite3.OperationalError right here before Part B is built, since
    # forecast_scored does not exist yet -- the right reason for this test
    # to fail now.
    conn.execute(
        """
        CREATE TRIGGER b4_force_scored_write_failure
        BEFORE INSERT ON forecast_scored
        BEGIN
            SELECT RAISE(ABORT, 'b4 forced scored-write failure');
        END
        """
    )
    conn.commit()

    label = _stamp(datetime(s.year, s.month, s.day, 0, 10, tzinfo=UTC))
    valid_at = _stamp(datetime(s.year, s.month, s.day, 12, 0, tzinfo=UTC))
    job = _run_fetch(
        db,
        conn,
        standin,
        feed_id,
        site_id,
        (round_schedule_sample(_SOURCE, "temperature", label, valid_at, 12, 5.0),),
        clock,
        datetime(s.year, s.month, s.day, 0, 10, tzinfo=UTC),
    )

    assert _count(conn, "forecast_samples", feed_id) == 0, (
        "a failed save must leave forecast_samples unchanged (0 rows)"
    )
    # The rolled-back save leaves exactly the failure log's own row
    # (processor._fail_job -> _record_failed_fetch, pinned by
    # tests/test_download_record.py DL2): one 'failed' download, nothing else.
    download_outcomes = [str(row["outcome"]) for row in _downloads(conn, feed_id)]
    assert download_outcomes == ["failed"], (
        "a failed save must leave only the failure log's one 'failed'"
        f" forecast_downloads row, got {download_outcomes!r}"
    )
    assert _count(conn, "forecast_scored", feed_id) == 0, (
        "a failed save must leave forecast_scored unchanged (0 rows)"
    )
    assert _count(conn, "forecast_value_changes", feed_id) == 0, (
        "a failed save must leave forecast_value_changes unchanged (0 rows)"
    )

    job_row = conn.execute(
        "SELECT status, retry_count, next_attempt_at FROM jobs WHERE id=?",
        (job.id,),
    ).fetchone()
    assert job_row is not None
    assert job_row["status"] == "pending", (
        "the job must be scheduled for retry (status 'pending'), got"
        f" {job_row['status']!r}"
    )
    assert job_row["retry_count"] == 1
    assert job_row["next_attempt_at"] is not None, (
        "a retried job must carry a next_attempt_at"
    )


def test_b5_migration_keeps_newest_value_and_backfills_scored_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """B5: migration on a made-up v15 file with several labels per hour:
    each hour keeps the row the 0.16.14 page showed; the open days'
    forecast_scored rows equal 0.16.14's scored_download/scored_version; a
    second run changes nothing.
    """
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    # The made-up v15 file: the first run stops at 15 (the pattern of
    # tests/test_rain_history_fill.py), then v16's table and index go, since
    # run_migrations makes both whatever its target.
    with monkeypatch.context() as v15:
        v15.setattr("wxverify.db.migrations.TARGET_USER_VERSION", 15)
        run_migrations(conn)
    conn.execute("DROP TABLE forecast_scored")
    conn.execute("DROP INDEX idx_samples_slot")
    conn.execute("PRAGMA user_version = 15")
    conn.commit()
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 15, (
        "fixture sanity: this file must start at v15"
    )

    site_id = day_close_site(conn, timezone="UTC", close_on=True, with_records=True)
    feed_id = asof_make_real_feed(conn, "b5-migration-model")
    conn.commit()

    oldest_open_row = conn.execute(
        "SELECT MIN(local_date) FROM site_days WHERE site_id=? AND state='open'",
        (site_id,),
    ).fetchone()
    assert oldest_open_row[0] is not None, "fixture sanity: no open day seeded"
    s = date.fromisoformat(oldest_open_row[0])

    l1 = _stamp(datetime(s.year, s.month, s.day, 0, 10, tzinfo=UTC))
    l2 = _stamp(datetime(s.year, s.month, s.day, 6, 10, tzinfo=UTC))
    valid_at = _stamp(datetime(s.year, s.month, s.day, 12, 0, tzinfo=UTC))

    for label in (l1, l2):
        conn.execute(
            "INSERT INTO forecast_downloads"
            " (site_id, feed_id, local_date, slot, started_at, ended_at,"
            "  fetched_at, outcome, usable, inserted, labels)"
            " VALUES (?, ?, ?, 'round', ?, ?, ?, 'stored', 1, 1, ?)",
            (
                site_id,
                feed_id,
                s.isoformat(),
                label,
                label,
                label,
                f'{{"{feed_id}": ["{label}"]}}',
            ),
        )
    conn.execute(
        "INSERT INTO forecast_samples"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value,"
        "  source_raw, model_run_id, fetched_at)"
        " VALUES (?, ?, 'temperature', ?, ?, 12, 10.0, 'b5:raw', ?, ?)",
        (site_id, feed_id, l1, valid_at, l1, l1),
    )
    conn.execute(
        "INSERT INTO forecast_samples"
        " (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value,"
        "  source_raw, model_run_id, fetched_at)"
        " VALUES (?, ?, 'temperature', ?, ?, 6, 20.0, 'b5:raw', ?, ?)",
        (site_id, feed_id, l2, valid_at, l2, l2),
    )
    conn.commit()

    # The oracle: what 0.16.14's OWN, unmodified scored_download/
    # scored_version compute for S, read before the migration touches
    # anything -- this shares no code with migrate_v16's carry-over, which
    # must reproduce it.
    download = scored_download(conn, site_id=site_id, feed_id=feed_id, local_date=s)
    assert download is not None, "fixture sanity: no scored download for S"
    version = scored_version(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        download=download,
        start_utc=datetime(s.year, s.month, s.day, 0, 0, tzinfo=UTC),
        end_utc=datetime(s.year, s.month, s.day, 0, 0, tzinfo=UTC) + timedelta(days=1),
    )
    oracle_scored_value = version[("temperature", valid_at)].value
    oracle_samples_value = conn.execute(
        "SELECT value FROM forecast_samples"
        " WHERE site_id=? AND feed_id=? AND variable='temperature' AND valid_at=?"
        " ORDER BY issued_at DESC LIMIT 1",
        (site_id, feed_id, valid_at),
    ).fetchone()[0]
    assert oracle_scored_value == 10.0, "fixture sanity: expected L1's value"
    assert oracle_samples_value == 20.0, "fixture sanity: expected L2's value"

    monkeypatch.setattr("wxverify.db.migrations.utc_now", lambda: NOW)
    run_migrations(conn)  # applies migrate_v16 once it exists

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 16, (
        "run_migrations must bring a v15 file to v16"
    )

    kept_samples = conn.execute(
        "SELECT value FROM forecast_samples"
        " WHERE site_id=? AND feed_id=? AND variable='temperature' AND valid_at=?",
        (site_id, feed_id, valid_at),
    ).fetchall()
    assert len(kept_samples) == 1, (
        f"the hour must keep exactly one row, found {len(kept_samples)}"
    )
    assert kept_samples[0]["value"] == oracle_samples_value, (
        "the kept row must be the one the 0.16.14 page showed: correct ="
        f" {oracle_samples_value!r}, got {kept_samples[0]['value']!r}"
    )

    scored_row = conn.execute(
        "SELECT value, download_date FROM forecast_scored"
        " WHERE site_id=? AND feed_id=? AND variable='temperature'"
        " AND valid_at=? AND day_ahead=0",
        (site_id, feed_id, valid_at),
    ).fetchone()
    assert scored_row is not None, (
        "the open day's forecast_scored row is missing after migration"
    )
    assert scored_row["value"] == oracle_scored_value, (
        "the carry-over must equal 0.16.14's own scored_download/"
        f"scored_version: correct = {oracle_scored_value!r}, got"
        f" {scored_row['value']!r}"
    )
    assert scored_row["download_date"] == s.isoformat()

    index_row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_samples_slot'"
    ).fetchone()
    assert index_row is not None, "idx_samples_slot must exist after migration"

    before_second_run = (
        conn.execute("SELECT COUNT(*) FROM forecast_samples").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM forecast_scored").fetchone()[0],
        kept_samples[0]["value"],
        scored_row["value"],
    )
    run_migrations(conn)  # a second run must change nothing
    after_second_run = (
        conn.execute("SELECT COUNT(*) FROM forecast_samples").fetchone()[0],
        conn.execute("SELECT COUNT(*) FROM forecast_scored").fetchone()[0],
        conn.execute(
            "SELECT value FROM forecast_samples"
            " WHERE site_id=? AND feed_id=? AND variable='temperature'"
            " AND valid_at=?",
            (site_id, feed_id, valid_at),
        ).fetchone()[0],
        conn.execute(
            "SELECT value FROM forecast_scored"
            " WHERE site_id=? AND feed_id=? AND variable='temperature'"
            " AND valid_at=? AND day_ahead=0",
            (site_id, feed_id, valid_at),
        ).fetchone()[0],
    )
    assert after_second_run == before_second_run, (
        f"a second migration run must change nothing: before ="
        f" {before_second_run!r}, after = {after_second_run!r}"
    )
