"""`migrations.drop_forecast_record` -- start-up cleanup of the
removed daily forecast record (`wxverify/db/migrations.py`, called from
`run_migrations` on every start).

Real SQLite throughout; no mocked database. `forecast_of_record`'s DDL is
the exact text dropped from `create_tables` (`wxverify/db/migrations.py`
before this release) -- kept here as a literal so the fixture needs no
production code that no longer exists. Synthetic site/job data only.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pytest

from wxverify.db.migrations import run_migrations
from wxverify.db.switch import ensure_site_generation

# The historical `forecast_of_record` DDL, verbatim, from before this
# release removed it from `create_tables`. Kept here only to give the drop
# step something real to drop.
_FORECAST_OF_RECORD_DDL = """
    CREATE TABLE IF NOT EXISTS forecast_of_record (
        id INTEGER PRIMARY KEY,
        site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
        tz_generation_id INTEGER NOT NULL
            REFERENCES timezone_generations(id),
        timezone TEXT NOT NULL,
        tz_rebuild_in_progress INTEGER NOT NULL DEFAULT 0
            CHECK(tz_rebuild_in_progress IN (0,1)),
        snapshot_local_date TEXT NOT NULL,
        snapshot_local_time TEXT NOT NULL,
        snapshot_utc TEXT NOT NULL,
        target_local_date TEXT NOT NULL,
        variable TEXT NOT NULL
            CHECK(variable IN ('temperature','wind','precip')),
        display_lead INTEGER NOT NULL
            CHECK(display_lead BETWEEN 0 AND 7),
        status TEXT NOT NULL CHECK(status IN ('recorded','missed')),
        missed_reason TEXT,
        write_path TEXT
            CHECK(write_path IS NULL
                  OR write_path IN ('on_time','late_reconstruction')),
        write_latency_seconds INTEGER,
        policy TEXT,
        methodology_version INTEGER NOT NULL,
        app_version TEXT NOT NULL,
        candidates TEXT,
        selected_feed_ids TEXT,
        feed_weights TEXT,
        effective_cells TEXT,
        source_runs TEXT,
        hourly_values TEXT,
        daily_quantities TEXT,
        leaderboard_status TEXT,
        created_at TEXT NOT NULL
            DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
        -- the publication the record used: plain values with no foreign key
        publication_id INTEGER,
        publication_cutoff TEXT,
        publication_age_days INTEGER,
        CHECK(
            (status = 'missed'
             AND missed_reason IS NOT NULL AND write_path IS NULL)
            OR (status = 'recorded'
                AND missed_reason IS NULL AND write_path IS NOT NULL)
        ),
        UNIQUE(site_id, tz_generation_id, snapshot_local_date,
               variable, target_local_date)
    );
"""

# Every job type the `jobs` table's CHECK still allows, less the two this
# release removed (`forecast_record`, `record_gap_scan`); `catchup` is
# site-scoped NULL and seeded apart from the rest.
_KEPT_SITE_SCOPED_JOB_TYPES = (
    "fetch_feed",
    "fetch_obs",
    "fetch_current_obs",
    "pair_and_score",
    "backfill_site",
    "verification_run",
    "timezone_correction",
    "day_close",
    "day_publish",
)
_REMOVED_JOB_TYPES = ("forecast_record", "record_gap_scan")

_MIGRATIONS_LOGGER = "wxverify.db.migrations"


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _run(conn: sqlite3.Connection) -> None:
    """Mirror `Database._run_immediate` on a bare connection."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        run_migrations(conn)
    except BaseException:
        conn.rollback()
        raise
    conn.commit()


def _seed_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 10.0, 10.0, 5.0, 'UTC')",
        (name,),
    )
    site_id = cur.lastrowid
    assert site_id is not None
    return int(site_id)


def _insert_forecast_of_record_row(
    conn: sqlite3.Connection, *, site_id: int, tz_generation_id: int
) -> None:
    conn.execute(
        "INSERT INTO forecast_of_record"
        " (site_id, tz_generation_id, timezone, snapshot_local_date,"
        "  snapshot_local_time, snapshot_utc, target_local_date, variable,"
        "  display_lead, status, write_path, methodology_version, app_version)"
        " VALUES (?, ?, 'UTC', '2026-07-06', '06:00:00', '2026-07-06T06:00:00Z',"
        "  '2026-07-07', 'temperature', 1, 'recorded', 'on_time', 1,"
        "  'synthetic-test-0.0.0')",
        (site_id, tz_generation_id),
    )


def _insert_job(
    conn: sqlite3.Connection,
    *,
    job_type: str,
    site_id: int | None,
    job_key: str,
    status: str,
) -> int:
    cur = conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES (?, ?, ?, ?)",
        (job_type, site_id, job_key, status),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone()
    return row is not None


def _snapshot_jobs(
    conn: sqlite3.Connection, ids: list[int]
) -> dict[int, tuple[object, ...]]:
    return {
        jid: tuple(
            conn.execute(
                "SELECT type, site_id, job_key, status, payload, next_attempt_at,"
                " retry_count, max_retries, last_error, result, created_at,"
                " updated_at FROM jobs WHERE id = ?",
                (jid,),
            ).fetchone()
        )
        for jid in ids
    }


def _snapshot_runtime_state(conn: sqlite3.Connection) -> dict[str, tuple[str, str]]:
    return {
        row["key"]: (row["value"], row["updated_at"])
        for row in conn.execute("SELECT key, value, updated_at FROM runtime_state")
    }


def _snapshot_settings(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        row["key"]: row["value"]
        for row in conn.execute("SELECT key, value FROM settings")
    }


def _drop_forecast_record_info_records(
    caplog: pytest.LogCaptureFixture,
) -> list[logging.LogRecord]:
    return [
        r
        for r in caplog.records
        if r.name == _MIGRATIONS_LOGGER
        and r.getMessage().startswith("drop_forecast_record:")
    ]


def test_drop_forecast_record_drops_only_the_removed_leftovers(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The table, the two removed job types (any status) and the exact
    gap-scan-failure runtime_state prefix go; every other job, runtime_state
    key and setting -- including a key that a LIKE match on the same prefix
    would wrongly reach -- compares equal, row for row. A third run changes
    nothing and logs zero counts.

    Catches: `LIKE` in place of `substr` equality (the `X`-for-`_` key would
    also be deleted), a status filter added to the job delete (the pending
    row of a removed type would survive, since the real delete has none), a
    wrong job type in the delete's type list (a kept-type row would vanish
    too), and a delete that reaches `settings` (the snapshot settings would
    be gone).
    """
    caplog.set_level(logging.INFO, logger=_MIGRATIONS_LOGGER)
    conn = _connect(tmp_path / "drop.db")
    _run(conn)  # fresh file -> v17; nothing to drop yet

    site_id = _seed_site(conn, "drop-site")
    gen_id = ensure_site_generation(conn, site_id)
    conn.execute(_FORECAST_OF_RECORD_DDL)
    _insert_forecast_of_record_row(conn, site_id=site_id, tz_generation_id=gen_id)

    removed_ids = [
        _insert_job(
            conn,
            job_type=job_type,
            site_id=site_id,
            job_key=f"{job_type}:{status}",
            status=status,
        )
        for job_type in _REMOVED_JOB_TYPES
        for status in ("pending", "failed")
    ]
    kept_ids = [
        _insert_job(
            conn,
            job_type=job_type,
            site_id=site_id,
            job_key=f"{job_type}:{status}",
            status=status,
        )
        for job_type in _KEPT_SITE_SCOPED_JOB_TYPES
        for status in ("pending", "failed")
    ]
    kept_ids += [
        _insert_job(
            conn,
            job_type="catchup",
            site_id=None,
            job_key=f"catchup:{status}",
            status=status,
        )
        for status in ("pending", "failed")
    ]

    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES (?, ?)",
        ("record_gap_scan_failures:1", "3"),
    )
    # A `LIKE` match on 'record_gap_scan_failures:%' would also reach this
    # key (every underscore in the pattern is a single-char wildcard under
    # `LIKE`); the real `substr` equality must not.
    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES (?, ?)",
        ("record_gap_scanXfailures:1", "9"),
    )
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?)",
        ("record_snapshot_local_time", "06:30"),
    )
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?)",
        (f"record_snapshot_local_time:{site_id}", "06:45"),
    )
    conn.commit()

    kept_jobs_before = _snapshot_jobs(conn, kept_ids)
    runtime_before = _snapshot_runtime_state(conn)
    settings_before = _snapshot_settings(conn)

    caplog.clear()
    _run(conn)

    info = _drop_forecast_record_info_records(caplog)
    assert len(info) == 1
    assert info[0].args == (1, len(removed_ids), 1), (
        f"drop_forecast_record's own INFO counts -> at this assertion: correct ="
        f" (1, {len(removed_ids)}, 1), mutant = {info[0].args!r}"
    )

    assert not _table_exists(conn, "forecast_of_record"), (
        "forecast_of_record must be dropped"
    )
    for jid in removed_ids:
        row = conn.execute("SELECT 1 FROM jobs WHERE id = ?", (jid,)).fetchone()
        assert row is None, f"removed-type job {jid} must be deleted"
    assert (
        conn.execute(
            "SELECT 1 FROM runtime_state WHERE key = 'record_gap_scan_failures:1'"
        ).fetchone()
        is None
    )

    assert _snapshot_jobs(conn, kept_ids) == kept_jobs_before, (
        "every kept job row must survive the drop step unchanged"
    )
    runtime_after = _snapshot_runtime_state(conn)
    assert runtime_after == {
        k: v for k, v in runtime_before.items() if k != "record_gap_scan_failures:1"
    }, "every other runtime_state key -- including the LIKE-bait key -- must survive"
    assert _snapshot_settings(conn) == settings_before, (
        "the drop step must never touch settings"
    )

    caplog.clear()
    _run(conn)
    info_again = _drop_forecast_record_info_records(caplog)
    assert len(info_again) == 1
    assert info_again[0].args == (0, 0, 0), (
        "a repeat run on an already-dropped file -> at this assertion: correct ="
        f" (0, 0, 0), mutant = {info_again[0].args!r}"
    )


class _RuntimeStateDeleteFails(sqlite3.Connection):
    """A connection whose `execute` raises on exactly the drop step's third
    statement -- the `runtime_state` prefix delete -- and passes every
    other statement through unchanged."""

    def execute(self, sql, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003, ANN201
        if sql.lstrip().startswith("DELETE FROM runtime_state WHERE substr("):
            raise sqlite3.OperationalError("synthetic: runtime_state delete fails")
        return super().execute(sql, *args, **kwargs)


def _connect_raising(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path), isolation_level=None, factory=_RuntimeStateDeleteFails
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def test_a_failure_on_the_last_statement_rolls_back_every_statement_in_the_step(
    tmp_path: Path,
) -> None:
    """A step without its own savepoint, or one that covers only the last
    statement, would leave the table dropped and the jobs deleted even
    though the step as a whole failed; this must not happen.

    Catches: a missing savepoint around the step, or a savepoint that
    covers only the `runtime_state` delete.
    """
    db_path = tmp_path / "rollback.db"
    conn = _connect(db_path)
    _run(conn)  # fresh file -> v17

    site_id = _seed_site(conn, "rollback-site")
    gen_id = ensure_site_generation(conn, site_id)
    conn.execute(_FORECAST_OF_RECORD_DDL)
    _insert_forecast_of_record_row(conn, site_id=site_id, tz_generation_id=gen_id)
    removed_ids = [
        _insert_job(
            conn,
            job_type=job_type,
            site_id=site_id,
            job_key=f"{job_type}:1",
            status="failed",
        )
        for job_type in _REMOVED_JOB_TYPES
    ]
    conn.commit()
    conn.close()

    raising_conn = _connect_raising(db_path)
    with pytest.raises(sqlite3.OperationalError, match="synthetic"):
        _run(raising_conn)
    raising_conn.close()

    fresh = _connect(db_path)
    assert _table_exists(fresh, "forecast_of_record"), (
        "the table must still be there after the rolled-back run"
    )
    row_count = fresh.execute("SELECT COUNT(*) FROM forecast_of_record").fetchone()[0]
    assert row_count == 1
    for jid in removed_ids:
        row = fresh.execute("SELECT 1 FROM jobs WHERE id = ?", (jid,)).fetchone()
        assert row is not None, f"job {jid} must still be there after the rollback"
    fresh.close()
