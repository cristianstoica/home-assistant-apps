"""Part A of the switch-over migration (plan ``2026-10-22-daily-close-switch-
over.md`` §16, §17.5, MG8): the 0.16.12 boundary step's current,
retired-rescore-free behaviour, exercised on every normal boot, not just a
one-time upgrade. MG7 (the gate that kept ``migrate_v15_switch`` from
rerunning on a file already at its target version) is retired:
``migrate_v15_switch`` and every migration below the current target are
deleted (storage release 4.1), and a file at user_version 1-15 is now
refused outright rather than silently left alone.

Synthetic data only: stations/sites are bare synthetic rows (no real
station ids, coordinates or site names); dates are far-future (2030) to
avoid any resemblance to real collection history.
"""

from __future__ import annotations

import sqlite3

from wxverify.collection.precip_labels import PRECIP_HOUR_START_BOUNDARY_KEY
from wxverify.db.migrations import run_migrations
from wxverify.db.runtime_state import delete_runtime_state, get_runtime_state

# ---------------------------------------------------------------------------
# Small fixture helpers (local to this file; tests/helpers.py untouched).
# ---------------------------------------------------------------------------


def _insert_site(conn: sqlite3.Connection, name: str = "mg-site") -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
        " timezone) VALUES (?, 0.0, 0.0, 0.0, 'UTC')",
        (name,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_history_row(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    valid_at: str,
    local_date: str,
    variable: str = "temperature",
) -> None:
    conn.execute(
        """
        INSERT INTO forecast_history
            (site_id, feed_id, variable, day_ahead, valid_at, local_date,
             value, lead_hours, issued_at, contributors)
        VALUES (?, ?, ?, 0, ?, ?, 10.0, 1, ?, NULL)
        """,
        (site_id, feed_id, variable, valid_at, local_date, valid_at),
    )


def _jobs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()


# ---------------------------------------------------------------------------
# MG7 -- retired: ``migrate_v15_switch`` and every migration below the
# current ``TARGET_USER_VERSION`` are deleted (storage release 4.1); a
# file at user_version 1-15, including 15 itself, is now refused outright
# ("too old for this release"), not silently left alone on a later open.
# Nothing about Part A's old rerun gate survives to test.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# MG8 -- the 0.16.12 boundary step keeps only its boundary-key write; its
# rain-pair delete, correction restart and rescore are retired (§16.3).
# M34. Calls run_migrations directly; no wxverify.db.switch import needed.
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
    _insert_history_row(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        valid_at="2030-01-01T01:00:00Z",
        local_date="2030-01-01",
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

    rain_history_rows = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_history WHERE variable='precip'"
    ).fetchone()["n"]
    assert rain_history_rows == 1, (
        "M34 -> at this assertion: correct = 1 (the rain history row kept, "
        f"the rain-pair delete retired), mutant = {rain_history_rows!r} (deleted)"
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
