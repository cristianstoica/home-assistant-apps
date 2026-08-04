"""Total-reader guards on the worker tick path.

scheduler_tick's due-row readers and claim_next_job's row disposition must
never raise on foreign/corrupt data (an imported or restored database is a
legal carrier for it): an unreadable row is either a self-healing WARNING
that lets the tick continue, or a terminal 'failed' row -- never an unhandled
exception that takes the whole worker loop down with it.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pytest

from wxverify import config
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import close_db, init_db
from wxverify.db.queue import (
    EnqueueResult,
    claim_next_job,
    enqueue_if_absent_with_cooldown,
)
from wxverify.worker.scheduler import scheduler_tick


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 - tests inspect the real writer connection


def _insert_site(conn: sqlite3.Connection, name: str = "Q") -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES (?, 47, 25, 900, 'UTC')
            """,
            (name,),
        ).lastrowid
    )


def _insert_station(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)
            VALUES (?, ?, 47, 25, 900)
            """,
            (site_id, pws_station_id),
        ).lastrowid
    )


def _feed_id(conn: sqlite3.Connection, source: str, model: str) -> int:
    row = conn.execute(
        "SELECT id FROM feeds WHERE source = ? AND model = ?", (source, model)
    ).fetchone()
    assert row is not None, f"seed feed missing: {source}/{model}"
    return int(row["id"])


def _pending_job_exists(
    conn: sqlite3.Connection, job_type: str, site_id: int, job_key: str
) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM jobs
        WHERE type = ? AND site_id = ? AND job_key = ? AND status = 'pending'
        """,
        (job_type, site_id, job_key),
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# scheduler_tick: every due-row reader must fail open or closed, never raise
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ExpectedJobs:
    must_exist: tuple[tuple[str, int, str], ...]
    must_not_exist: tuple[tuple[str, int, str], ...] = ()


@dataclass(frozen=True)
class _TickCarrierCase:
    case_id: str
    logger_name: str
    warning_substring: str
    setup: Callable[[sqlite3.Connection], _ExpectedJobs]


def _setup_jobs_updated_at_blob(conn: sqlite3.Connection) -> _ExpectedJobs:
    """A prior failed job with a BLOB updated_at, reached via the due-feed
    loop's cooldown wrapper (open-meteo/ecmwf_ifs is default_subscribed, so
    no site_feed_state row is needed for it to be due)."""
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    job_key = f"fetch:{feed_id}"
    conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, payload, status, updated_at)
        VALUES ('fetch_feed', ?, ?, '{}', 'failed', x'0001')
        """,
        (site_id, job_key),
    )
    return _ExpectedJobs(must_exist=(("fetch_feed", site_id, job_key),))


def _setup_site_feed_state_last_run_at_blob(conn: sqlite3.Connection) -> _ExpectedJobs:
    """google/blend is default_subscribed=0, so this row would silently vanish
    from the tick (COALESCE falls back to default_subscribed) if `enabled`
    were left NULL instead of set explicitly -- the column has no DEFAULT."""
    site_id = _insert_site(conn)
    feed_id = _feed_id(conn, "google", "blend")
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled, last_run_at)
        VALUES (?, ?, 1, x'0001')
        """,
        (site_id, feed_id),
    )
    return _ExpectedJobs(must_exist=(("fetch_feed", site_id, f"fetch:{feed_id}"),))


def _setup_fetch_interval_minutes_hostile(
    conn: sqlite3.Connection, literal: str
) -> _ExpectedJobs:
    """meteoblue/multimodel (default_subscribed=0, enabled=1 explicit here)
    gets an unreadable cadence; open-meteo/ecmwf_ifs (default_subscribed=1,
    no site_feed_state row) is the unaffected control that must still run."""
    site_id = _insert_site(conn)
    control_feed_id = _feed_id(conn, "open-meteo", "ecmwf_ifs")
    bad_feed_id = _feed_id(conn, "meteoblue", "multimodel")
    past = isoformat_utc(utc_now() - timedelta(days=30))
    conn.execute(
        """
        INSERT INTO site_feed_state (site_id, feed_id, enabled, last_run_at)
        VALUES (?, ?, 1, ?)
        """,
        (site_id, bad_feed_id, past),
    )
    conn.execute(
        f"UPDATE feeds SET fetch_interval_minutes = {literal} WHERE id = ?",
        (bad_feed_id,),
    )
    return _ExpectedJobs(
        must_exist=(("fetch_feed", site_id, f"fetch:{control_feed_id}"),),
        must_not_exist=(("fetch_feed", site_id, f"fetch:{bad_feed_id}"),),
    )


def _setup_fetch_interval_minutes_real_infinity(
    conn: sqlite3.Connection,
) -> _ExpectedJobs:
    return _setup_fetch_interval_minutes_hostile(conn, "9e999")


def _setup_fetch_interval_minutes_blob(conn: sqlite3.Connection) -> _ExpectedJobs:
    return _setup_fetch_interval_minutes_hostile(conn, "x'0001'")


def _setup_sites_last_obs_at_blob(conn: sqlite3.Connection) -> _ExpectedJobs:
    site_id = _insert_site(conn)
    _insert_station(conn, site_id, "PWS-CASE-OBS")
    conn.execute("UPDATE sites SET last_obs_at = x'0001' WHERE id = ?", (site_id,))
    return _ExpectedJobs(must_exist=(("fetch_obs", site_id, "obs"),))


def _setup_stations_site_id_hostile(
    conn: sqlite3.Connection, literal: str
) -> _ExpectedJobs:
    """A corrupt station must be skipped while a healthy sibling still runs.
    Planted with foreign_keys OFF: the row arrives via replace_from's
    os.replace, so it never passes through an FK-enforcing connection."""
    site_id = _insert_site(conn)
    bad = _insert_station(conn, site_id, "PWS-CASE-BAD")
    good = _insert_station(conn, site_id, "PWS-CASE-GOOD")
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute(f"UPDATE stations SET site_id = {literal} WHERE id = ?", (bad,))
    conn.execute("PRAGMA foreign_keys=ON")
    return _ExpectedJobs(
        must_exist=(("fetch_current_obs", site_id, f"curobs:{good}"),),
        must_not_exist=(("fetch_current_obs", site_id, f"curobs:{bad}"),),
    )


def _setup_stations_site_id_real_infinity(conn: sqlite3.Connection) -> _ExpectedJobs:
    return _setup_stations_site_id_hostile(conn, "9e999")


def _setup_stations_site_id_blob(conn: sqlite3.Connection) -> _ExpectedJobs:
    return _setup_stations_site_id_hostile(conn, "x'0001'")


_TICK_CARRIER_CASES: tuple[_TickCarrierCase, ...] = (
    _TickCarrierCase(
        case_id="jobs_updated_at_blob_via_cooldown_wrapper",
        logger_name="wxverify.db.queue",
        warning_substring="updated_at",
        setup=_setup_jobs_updated_at_blob,
    ),
    _TickCarrierCase(
        case_id="site_feed_state_last_run_at_blob",
        logger_name="wxverify.worker.scheduler",
        warning_substring="last_run_at",
        setup=_setup_site_feed_state_last_run_at_blob,
    ),
    _TickCarrierCase(
        case_id="feeds_fetch_interval_minutes_real_infinity",
        logger_name="wxverify.worker.scheduler",
        warning_substring="fetch_interval_minutes",
        setup=_setup_fetch_interval_minutes_real_infinity,
    ),
    _TickCarrierCase(
        case_id="feeds_fetch_interval_minutes_blob",
        logger_name="wxverify.worker.scheduler",
        warning_substring="fetch_interval_minutes",
        setup=_setup_fetch_interval_minutes_blob,
    ),
    _TickCarrierCase(
        case_id="sites_last_obs_at_blob",
        logger_name="wxverify.worker.scheduler",
        warning_substring="last_obs_at",
        setup=_setup_sites_last_obs_at_blob,
    ),
    _TickCarrierCase(
        case_id="stations_site_id_real_infinity",
        logger_name="wxverify.worker.scheduler",
        warning_substring="site_id",
        setup=_setup_stations_site_id_real_infinity,
    ),
    _TickCarrierCase(
        case_id="stations_site_id_blob",
        logger_name="wxverify.worker.scheduler",
        warning_substring="site_id",
        setup=_setup_stations_site_id_blob,
    ),
)


@pytest.mark.parametrize(
    "case", _TICK_CARRIER_CASES, ids=[c.case_id for c in _TICK_CARRIER_CASES]
)
def test_scheduler_tick_survives_hostile_reader_values(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, case: _TickCarrierCase
) -> None:
    conn = _init_tmp_db(tmp_path)
    expected = case.setup(conn)

    with caplog.at_level(logging.WARNING, logger=case.logger_name):
        scheduler_tick(conn)  # must not raise

    for job_type, site_id, job_key in expected.must_exist:
        assert _pending_job_exists(conn, job_type, site_id, job_key), (
            f"expected a pending {job_type} job for key={job_key!r}"
        )
    for job_type, site_id, job_key in expected.must_not_exist:
        assert not _pending_job_exists(conn, job_type, site_id, job_key), (
            f"the corrupted row's own job must be skipped this tick, key={job_key!r}"
        )

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, (
        f"expected exactly one WARNING, got {[r.getMessage() for r in warnings]}"
    )
    assert case.warning_substring in warnings[0].getMessage()


def test_enqueue_if_absent_with_cooldown_direct_call_fails_open_on_blob_updated_at(
    tmp_path: Path,
) -> None:
    """The cooldown wrapper's non-worker caller (a post-read rescore trigger)
    must see the same fail-open behaviour as the scheduler's own call sites."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    job_type, job_key = "fetch_feed", "fetch:1"
    conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, payload, status, updated_at)
        VALUES (?, ?, ?, '{}', 'failed', x'0001')
        """,
        (job_type, site_id, job_key),
    )

    result = enqueue_if_absent_with_cooldown(
        conn, job_type, site_id, job_key, {}, cooldown=timedelta(hours=1)
    )

    assert isinstance(result, EnqueueResult)
    assert result.created is True
    assert result.job_id is not None


# ---------------------------------------------------------------------------
# claim_next_job: the disposition must be a total catch, not an allowlist
# ---------------------------------------------------------------------------


def _corrupt_retry_count_real_infinity(conn: sqlite3.Connection, job_id: int) -> None:
    conn.execute("UPDATE jobs SET retry_count = 9e999 WHERE id = ?", (job_id,))


def _corrupt_payload_deeply_nested_array(conn: sqlite3.Connection, job_id: int) -> None:
    nested = "[" * 200_000 + "]" * 200_000
    conn.execute(f"UPDATE jobs SET payload = '{nested}' WHERE id = ?", (job_id,))


@pytest.mark.parametrize(
    "corrupt",
    [_corrupt_retry_count_real_infinity, _corrupt_payload_deeply_nested_array],
    ids=["retry_count_real_infinity", "payload_deeply_nested_array"],
)
def test_claim_next_job_dispositions_carriers_an_allowlist_would_miss(
    tmp_path: Path, corrupt: Callable[[sqlite3.Connection, int], None]
) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    job_id = int(
        conn.execute(
            """
            INSERT INTO jobs (type, site_id, job_key, payload, status)
            VALUES ('fetch_feed', ?, 'fetch:unreadable', '{}', 'pending')
            """,
            (site_id,),
        ).lastrowid
    )
    corrupt(conn, job_id)

    assert claim_next_job(conn) is None, (
        "an unreadable row must not be handed to the worker"
    )

    row = conn.execute(
        "SELECT status, last_error, updated_at FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    assert row["status"] == "failed"
    assert row["last_error"] == "unreadable job row"

    assert claim_next_job(conn) is None, (
        "the disposed row is terminal, not 'pending' -- it must never be re-claimed"
    )
    row_after = conn.execute(
        "SELECT status, last_error, updated_at FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()
    assert (row_after["status"], row_after["last_error"], row_after["updated_at"]) == (
        row["status"],
        row["last_error"],
        row["updated_at"],
    ), "a second claim call must not touch the already-dispositioned row"
