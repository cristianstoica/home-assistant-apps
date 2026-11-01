"""QA oracles for §18.9 (no-change jobs) plus §14 publish preconditions and
input-fingerprint sensitivity.

These drive the REAL sync chain (regen → decide → start → simulate →
aggregate → bootstrap → publish) against a migrated in-memory database and
then interrogate the durable §14 artifacts: trigger-decision rows, run
rows, the published pointer, and the evidence tables. The bootstrap phase
is emulated synchronously the same way the implementer suite does (the
async orchestrator only shuttles the same three calls).

All fixture data is synthetic: fake site name, fake models, UTC, invented
values. Expected values are hand-derived from the spec.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from tests.helpers import asof_conn, asof_make_real_feed
from wxverify.db.switch import ensure_site_generation
from wxverify.verification.runs import (
    capture_config_snapshot,
    input_fingerprint,
    publish_run,
    published_run_id,
    seed_from_fingerprint,
)


def test_seed_from_fingerprint_fits_sqlite_integer() -> None:
    # sha256("a"*64)[:8] = 0xffe054fe7ae0cb6d — top bit set before masking;
    # the seed must fit SQLite's signed 64-bit INTEGER range.
    for fingerprint in ["a" * 64, "b" * 64, "0" * 64, "f" * 64]:
        seed = seed_from_fingerprint(fingerprint)
        assert 0 <= seed < 2**63


_PERIOD_DAYS = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05"]
_QUANTITY_VALUES = {
    "temperature_high": 21.0,
    "temperature_low": 9.0,
    "wind_max": 6.0,
    "precip_total": 0.0,
    "precip_occurrence": 0.0,
}


def _make_site(conn: sqlite3.Connection) -> tuple[int, list[int]]:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,
                           timezone)
        VALUES ('oracle-town', 40.0, -105.0, 900.0, 'UTC')
        """
    )
    assert cur.lastrowid is not None
    site_id = int(cur.lastrowid)
    feeds = [
        asof_make_real_feed(conn, "model-gamma"),
        asof_make_real_feed(conn, "model-delta"),
    ]
    generation_id = ensure_site_generation(conn, site_id)
    for day in _PERIOD_DAYS:
        for quantity, value in _QUANTITY_VALUES.items():
            conn.execute(
                """
                INSERT INTO daily_truth
                    (site_id, local_date, quantity, value, eligible,
                     covered_hours, expected_slots, wet_hours, dry_hours,
                     rain_threshold_mm, day_start_utc, day_end_utc, timezone,
                     tz_generation_id)
                VALUES (?, ?, ?, ?, 1, 24, 24, ?, ?, 0.2, ?, ?, 'UTC', ?)
                """,
                (
                    site_id,
                    day,
                    quantity,
                    value,
                    0 if quantity.startswith("precip") else None,
                    24 if quantity.startswith("precip") else None,
                    f"{day}T00:00:00Z",
                    f"{day}T23:59:59Z",
                    generation_id,
                ),
            )
    values = {"temperature": 15.0, "wind": 5.0, "precip": 0.0}
    issued = "2026-05-31T05:00:00Z"
    for feed_index, feed_id in enumerate(feeds):
        for variable, base in values.items():
            for day_offset in range(len(_PERIOD_DAYS) + 8):
                for hour in range(24):
                    total_hours = day_offset * 24 + hour
                    valid = (
                        datetime(2026, 6, 1, tzinfo=UTC) + timedelta(hours=total_hours)
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
                    conn.execute(
                        """
                        INSERT INTO forecast_samples
                            (site_id, feed_id, variable, issued_at, valid_at,
                             lead_hours, value, source_raw, model_run_id,
                             fetched_at)
                        VALUES (?, ?, ?, ?, ?, 6, ?, '{}', 'run-a', ?)
                        """,
                        (
                            site_id,
                            feed_id,
                            variable,
                            issued,
                            valid,
                            base + 0.5 * feed_index,
                            issued,
                        ),
                    )
    conn.commit()
    return site_id, feeds


def _run_row(conn: sqlite3.Connection, run_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert row is not None
    return cast(sqlite3.Row, row)


# ---------------------------------------------------------------------------
# N5 — fingerprint sensitivity: every §14 truth-row component and the
# sample high-water mark each flip the fingerprint
# ---------------------------------------------------------------------------


def test_fingerprint_reacts_to_every_truth_component_and_high_water() -> None:
    conn = asof_conn()
    site_id, feeds = _make_site(conn)
    snapshot = capture_config_snapshot(conn, site_id)

    def fp() -> str:
        return input_fingerprint(conn, site_id, snapshot)

    prints = [fp()]

    def mutate(sql: str) -> None:
        cur = conn.execute(
            sql + " WHERE site_id = ? AND local_date = '2026-06-02'"
            " AND quantity = 'temperature_high'",
            (site_id,),
        )
        assert cur.rowcount == 1
        prints.append(fp())

    mutate("UPDATE daily_truth SET value = value + 0.5")
    mutate("UPDATE daily_truth SET eligible = 0")
    mutate("UPDATE daily_truth SET covered_hours = 23")
    mutate("UPDATE daily_truth SET stale = 1")
    conn.execute(
        """
        INSERT INTO forecast_samples
            (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
             value, source_raw, model_run_id, fetched_at)
        VALUES (?, ?, 'wind', '2026-06-05T05:00:00Z', '2026-06-06T12:00:00Z',
                6, 4.0, '{}', 'run-b', '2026-06-05T05:00:00Z')
        """,
        (site_id, feeds[0]),
    )
    prints.append(fp())
    assert len(prints) == 6
    assert len(set(prints)) == 6


def test_publish_run_requires_a_running_row() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    generation = ensure_site_generation(conn, site_id)
    cur = conn.execute(
        """
        INSERT INTO verification_runs
            (site_id, tz_generation_id, methodology_version, app_version,
             state, attempt, config_snapshot, period_start, period_end,
             settled_through, bootstrap_seed, bootstrap_resamples,
             input_fingerprint, created_at)
        VALUES (?, ?, 1, 'test', 'failed', 1, '{}', '2026-06-01',
                '2026-06-05', '2026-06-05', 1, 10, 'a' || ?,
                '2026-06-05T12:00:00Z')
        """,
        (site_id, generation, site_id),
    )
    assert cur.lastrowid is not None
    run_id = int(cur.lastrowid)
    with pytest.raises(RuntimeError, match="not publishable"):
        publish_run(conn, site_id, run_id)
    assert published_run_id(conn, site_id) is None
    assert str(_run_row(conn, run_id)["state"]) == "failed"
