"""Part A of the switch-over migration (plan ``2026-10-22-daily-close-switch-
over.md`` §16, §17.5, MG7-MG8): the gate that keeps ``migrate_v15_switch``
from rerunning on a file already at its target version (MG7), and the
0.16.12 boundary step's current, retired-rescore-free behaviour (MG8) --
both exercised on every normal boot, not just a one-time upgrade.

Synthetic data only: stations/sites are bare synthetic rows (no real
station ids, coordinates or site names); dates are far-future (2030) to
avoid any resemblance to real collection history.
"""

from __future__ import annotations

import sqlite3

from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
from wxverify.db.migrations import TARGET_USER_VERSION, run_migrations
from wxverify.db.runtime_state import delete_runtime_state, get_runtime_state
from wxverify.db.switch import ensure_site_generation

# ---------------------------------------------------------------------------
# Small fixture helpers (local to this file; tests/helpers.py untouched).
# ---------------------------------------------------------------------------


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _insert_site(conn: sqlite3.Connection, name: str = "mg-site") -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES (?, 0.0, 0.0, 0.0, 'UTC')",
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    generation_id: int,
    valid_at: str,
    variable: str = "temperature",
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_pairs
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             day_ahead, forecast, observed, tz_generation_id)
        VALUES (?, ?, ?, ?, ?, 1, 0, 10.0, 10.0, ?)
        """,
        (site_id, feed_id, variable, valid_at, valid_at, generation_id),
    )


def _insert_job(
    conn: sqlite3.Connection,
    *,
    type_: str,
    site_id: int | None,
    status: str,
    job_key: str | None,
) -> int:
    cur = conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, status) VALUES (?, ?, ?, ?)",
        (type_, site_id, job_key, status),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _jobs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()


# ---------------------------------------------------------------------------
# MG7 -- Part A is gated on current < 15: a file already at 15 is left
# alone on a later open. M33.
#
# Calls run_migrations directly; no wxverify.db.switch import is needed
# here, because the gate lives in run_migrations itself (plan 16.1, 16.2),
# which today fails any v15 file outright ("newer than this app") since
# TARGET_USER_VERSION is still 14 -- itself a legitimate red for "P6's
# behaviour is missing".
# ---------------------------------------------------------------------------


def test_mg7_part_a_is_gated_and_does_not_rerun_on_an_already_switched_file() -> None:
    conn = _conn()
    _insert_site(conn)
    job_id = _insert_job(
        conn, type_="catchup", site_id=None, status="pending", job_key="catchup"
    )
    conn.execute("PRAGMA user_version = 15")

    run_migrations(conn)

    remaining_ids = {r["id"] for r in _jobs(conn)}
    assert job_id in remaining_ids, (
        "M33 -> at this assertion: correct = the pending catchup row still "
        f"present (Part A gated on current < 15, {TARGET_USER_VERSION=} "
        "target 15 == current 15 skips it), mutant = the row was deleted "
        "by a rerun of A2"
    )


# ---------------------------------------------------------------------------
# MG8 -- the 0.16.12 boundary step keeps only its boundary-key write; its
# rain-pair delete, correction restart and rescore are retired (§16.3).
# M34.
#
# Calls run_migrations directly; no wxverify.db.switch import needed. Today
# (before §16.3's retirement lands) record_precip_hour_start_boundary still
# deletes the rain pairs and score_cache row and enqueues pair_and_score,
# so this is a genuine red against current, unfixed behaviour.
# ---------------------------------------------------------------------------


def test_mg8_boundary_step_keeps_only_its_key_write() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    # One migrations pass to build the schema and seed default feeds, but
    # WITHOUT writing the boundary key yet: delete it straight after.
    run_migrations(conn)
    delete_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)

    site_id = _insert_site(conn)
    generation_id = ensure_site_generation(conn, site_id)
    om_feed = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' AND is_virtual=0 LIMIT 1"
    ).fetchone()
    assert om_feed is not None
    feed_id = int(om_feed["id"])

    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'precip', '2030-01-01T00:00:00Z', '2030-01-01T01:00:00Z',
                1, 0.5, '{}', 'run1', '2030-01-01T00:05:00Z')
        """,
        (site_id, feed_id),
    )
    _insert_pair(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        generation_id=generation_id,
        valid_at="2030-01-01T01:00:00Z",
        variable="precip",
    )
    conn.execute(
        "INSERT INTO score_cache (site_id, feed_id, variable, day_ahead,"
        " window_key, n, computed_at) VALUES (?, ?, 'precip', 0, '7d', 1,"
        " '2030-01-01T00:00:00Z')",
        (site_id, feed_id),
    )
    jobs_before = {r["id"] for r in _jobs(conn)}

    run_migrations(conn)

    boundary = get_runtime_state(conn, PRECIP_HOUR_START_BOUNDARY_KEY)
    assert boundary is not None, (
        "M34 -> at this assertion: correct = the boundary key written, "
        "mutant = it is still absent"
    )

    rain_pairs = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_pairs WHERE variable='precip'"
    ).fetchone()["n"]
    assert rain_pairs == 1, (
        "M34 -> at this assertion: correct = 1 (the rain pair kept, the "
        f"rain-pair delete retired), mutant = {rain_pairs!r} (deleted)"
    )
    score_rows = conn.execute(
        "SELECT COUNT(*) AS n FROM score_cache WHERE variable='precip'"
    ).fetchone()["n"]
    assert score_rows == 1, (
        "M34 -> at this assertion: correct = 1 (the rain score_cache row "
        f"kept), mutant = {score_rows!r} (deleted)"
    )
    jobs_after = {r["id"] for r in _jobs(conn)}
    assert jobs_after == jobs_before, (
        "M34 -> at this assertion: correct = no new job row (no "
        "timezone_correction or pair_and_score enqueue), mutant = "
        f"{jobs_after - jobs_before!r} newly enqueued"
    )
