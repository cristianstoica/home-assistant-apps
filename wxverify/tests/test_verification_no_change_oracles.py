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
from wxverify.db.queue import Job
from wxverify.db.runtime_state import get_runtime_state
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.verification.engine import prepare_bootstrap_inputs
from wxverify.verification.runs import (
    capture_config_snapshot,
    input_fingerprint,
    publish_run,
    published_run_id,
    run_config_from_row,
    seed_from_fingerprint,
)
from wxverify.verification.truth import mark_daily_truth_stale
from wxverify.worker.verification_run import (
    _compute_verdicts,  # noqa: SLF001
    _load_state,  # noqa: SLF001
    _persist_verdicts,  # noqa: SLF001
    advance_verification,
    mark_verification_failed,
    verification_job_key,
    verification_state_key,
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
    generation_id = ensure_published_generation(conn, site_id)
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


def _drive_chain(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    *,
    resamples: int = 40,
    max_steps: int = 300,
) -> None:
    for _ in range(max_steps):
        blob = _load_state(conn, site_id)
        if blob is not None and blob.get("phase") == "bootstrap":
            run_id = blob["run_id"]
            assert isinstance(run_id, int)
            cfg = run_config_from_row(conn, run_id)
            inputs = prepare_bootstrap_inputs(conn, cfg)
            verdicts = _compute_verdicts(inputs, cfg.bootstrap_seed, resamples)
            _persist_verdicts(conn, site_id, cfg, verdicts)
            continue
        if not advance_verification(conn, site_id, payload):
            return
    raise AssertionError("verification chain did not terminate")


def _drive_until_phase(
    conn: sqlite3.Connection,
    site_id: int,
    payload: dict[str, object],
    phase: str,
    *,
    resamples: int = 40,
    max_steps: int = 300,
) -> None:
    for _ in range(max_steps):
        blob = _load_state(conn, site_id)
        if blob is not None and blob.get("phase") == phase:
            return
        if blob is not None and blob.get("phase") == "bootstrap":
            run_id = blob["run_id"]
            assert isinstance(run_id, int)
            cfg = run_config_from_row(conn, run_id)
            inputs = prepare_bootstrap_inputs(conn, cfg)
            verdicts = _compute_verdicts(inputs, cfg.bootstrap_seed, resamples)
            _persist_verdicts(conn, site_id, cfg, verdicts)
            continue
        assert advance_verification(conn, site_id, payload)
    raise AssertionError(f"chain never reached phase {phase!r}")


def _decisions(conn: sqlite3.Connection, site_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM verification_trigger_decisions
        WHERE site_id = ? ORDER BY id
        """,
        (site_id,),
    ).fetchall()


def _evidence_dump(conn: sqlite3.Connection, run_id: int) -> list[tuple[object, ...]]:
    rows = conn.execute(
        "SELECT * FROM verification_evidence WHERE run_id = ? ORDER BY id",
        (run_id,),
    ).fetchall()
    return [tuple(row) for row in rows]


def _run_row(conn: sqlite3.Connection, run_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM verification_runs WHERE id = ?", (run_id,)
    ).fetchone()
    assert row is not None
    return cast(sqlite3.Row, row)


# ---------------------------------------------------------------------------
# N1 — §18.9: identical inputs the next night write ONLY a decision row;
# no run, no evidence churn, pointer untouched
# ---------------------------------------------------------------------------


def test_no_change_night_writes_only_a_decision_row() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    _drive_chain(conn, site_id, {"trigger_date": "2026-06-06"})
    run1 = published_run_id(conn, site_id)
    assert run1 is not None
    dump_before = _evidence_dump(conn, run1)
    assert dump_before  # non-vacuous: the published run HAS evidence

    _drive_chain(conn, site_id, {"trigger_date": "2026-06-07"})

    decisions = _decisions(conn, site_id)
    assert [str(d["decision"]) for d in decisions] == [
        "run_started",
        "no_change_skip",
    ]
    skip = decisions[1]
    assert skip["run_id"] is None
    assert "fingerprint" in str(skip["reason"])
    assert str(skip["input_fingerprint"]) == str(
        _run_row(conn, run1)["input_fingerprint"]
    )
    n_runs = conn.execute(
        "SELECT COUNT(*) AS n FROM verification_runs WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    assert int(n_runs["n"]) == 1
    assert published_run_id(conn, site_id) == run1
    assert _evidence_dump(conn, run1) == dump_before
    assert get_runtime_state(conn, verification_state_key(site_id)) is None


# ---------------------------------------------------------------------------
# N2 — §18.9: late station data flips the fingerprint through stale-truth
# regeneration and drives a genuinely new run
# ---------------------------------------------------------------------------


def test_late_station_data_regenerates_truth_and_reruns() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    _drive_chain(conn, site_id, {"trigger_date": "2026-06-06"})
    run1 = published_run_id(conn, site_id)
    assert run1 is not None
    fp1 = str(_run_row(conn, run1)["input_fingerprint"])
    run1_evidence = len(_evidence_dump(conn, run1))
    assert run1_evidence > 0

    # Late consensus for 2026-06-03: 24 hourly temperatures 10.0 + 0.25*h.
    for hour in range(24):
        conn.execute(
            """
            INSERT INTO observations
                (site_id, variable, valid_at, value, n_stations, computed_at)
            VALUES (?, 'temperature', ?, ?, 3, '2026-06-07T02:00:00Z')
            """,
            (site_id, f"2026-06-03T{hour:02d}:00:00Z", 10.0 + 0.25 * hour),
        )
    marked = mark_daily_truth_stale(
        conn,
        site_id=site_id,
        variable="temperature",
        valid_at="2026-06-03T12:00:00Z",
    )
    # Exactly the two temperature quantities of that day — never wind/precip.
    assert marked == 2
    conn.commit()

    _drive_chain(conn, site_id, {"trigger_date": "2026-06-08"})

    run2 = published_run_id(conn, site_id)
    assert run2 is not None and run2 != run1
    fp2 = str(_run_row(conn, run2)["input_fingerprint"])
    assert fp2 != fp1
    decisions = [str(d["decision"]) for d in _decisions(conn, site_id)]
    assert decisions == ["run_started", "run_started"]

    # Regenerated truth: high = max slot = 10 + 0.25*23 = 15.75, low = 10.0,
    # 24 covered hours -> eligible, stale cleared.
    high = conn.execute(
        """
        SELECT value, eligible, stale FROM daily_truth
        WHERE site_id = ? AND local_date = '2026-06-03'
          AND quantity = 'temperature_high'
        """,
        (site_id,),
    ).fetchone()
    assert high is not None
    assert float(high["value"]) == pytest.approx(15.75)
    assert int(high["eligible"]) == 1
    assert int(high["stale"]) == 0
    low = conn.execute(
        """
        SELECT value, stale FROM daily_truth
        WHERE site_id = ? AND local_date = '2026-06-03'
          AND quantity = 'temperature_low'
        """,
        (site_id,),
    ).fetchone()
    assert low is not None
    assert float(low["value"]) == pytest.approx(10.0)
    assert int(low["stale"]) == 0

    # The OLD published run's evidence survives the new run's start wipe.
    assert len(_evidence_dump(conn, run1)) == run1_evidence


# ---------------------------------------------------------------------------
# N3 — a failed attempt does NOT block the same fingerprint below the cap:
# the next night re-runs, wipes the failed attempt's partial evidence, and
# counts attempt 2
# ---------------------------------------------------------------------------


def test_failed_attempt_reruns_same_fingerprint_and_wipes_evidence() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    payload: dict[str, object] = {
        "trigger_date": "2026-06-06",
        "snapshot_days_per_chunk": 2,
    }
    # Advance until the running attempt has written some partial evidence.
    run1: int | None = None
    for _ in range(50):
        assert advance_verification(conn, site_id, payload)
        row = conn.execute(
            """
            SELECT id FROM verification_runs
            WHERE site_id = ? AND state = 'running'
            """,
            (site_id,),
        ).fetchone()
        if row is None:
            continue
        run1 = int(row["id"])
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM verification_evidence WHERE run_id = ?",
            (run1,),
        ).fetchone()
        if int(n["n"]) > 0:
            break
    else:
        raise AssertionError("chain never wrote partial evidence")
    assert run1 is not None
    fp1 = str(_run_row(conn, run1)["input_fingerprint"])

    job = Job(
        id=1,
        type="verification_run",
        site_id=site_id,
        job_key=verification_job_key(site_id),
        payload=payload,
        status="running",
        retry_count=3,
        max_retries=3,
    )
    mark_verification_failed(conn, job)
    assert str(_run_row(conn, run1)["state"]) == "failed"

    _drive_chain(conn, site_id, {"trigger_date": "2026-06-07"})

    run2 = published_run_id(conn, site_id)
    assert run2 is not None and run2 != run1
    row2 = _run_row(conn, run2)
    # Same inputs -> same fingerprint; one prior failure -> attempt 2.
    assert str(row2["input_fingerprint"]) == fp1
    assert int(row2["attempt"]) == 2
    decisions = [str(d["decision"]) for d in _decisions(conn, site_id)]
    assert decisions == ["run_started", "run_started"]
    # The failed attempt keeps its metadata row but ZERO evidence.
    assert _evidence_dump(conn, run1) == []
    foreign = conn.execute(
        "SELECT COUNT(*) AS n FROM verification_evidence WHERE run_id != ?",
        (run2,),
    ).fetchone()
    assert int(foreign["n"]) == 0


# ---------------------------------------------------------------------------
# N4 — the attempt cap is scoped to THE fingerprint: two failures under a
# DIFFERENT fingerprint never block tonight's run
# ---------------------------------------------------------------------------


def test_attempt_cap_is_fingerprint_scoped() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    snapshot = capture_config_snapshot(conn, site_id)
    for _ in range(2):
        conn.execute(
            """
            INSERT INTO verification_runs
                (site_id, tz_generation_id, methodology_version, app_version,
                 state, attempt, config_snapshot, period_start, period_end,
                 settled_through, bootstrap_seed, bootstrap_resamples,
                 input_fingerprint, created_at)
            VALUES (?, ?, 1, 'test', 'failed', 1, '{}', '2026-06-01',
                    '2026-06-05', '2026-06-05', 1, 10, ?,
                    '2026-06-05T12:00:00Z')
            """,
            (site_id, int(str(snapshot["tz_generation_id"])), "0" * 64),
        )
    conn.commit()
    _drive_chain(conn, site_id, {"trigger_date": "2026-06-06"})
    assert published_run_id(conn, site_id) is not None
    decisions = [str(d["decision"]) for d in _decisions(conn, site_id)]
    assert decisions == ["run_started"]


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


# ---------------------------------------------------------------------------
# N6 — §14 publish preconditions: any integrity violation aborts the
# publish transaction, keeps the run 'running', and never flips the pointer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "tamper",
    ["missing_verdict", "no_results", "extra_verdict"],
)
def test_publish_integrity_gate_blocks_pointer_flip(tamper: str) -> None:
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    payload: dict[str, object] = {"trigger_date": "2026-06-06"}
    _drive_until_phase(conn, site_id, payload, "publish")
    blob = _load_state(conn, site_id)
    assert blob is not None
    run_id = blob["run_id"]
    assert isinstance(run_id, int)

    if tamper == "missing_verdict":
        conn.execute(
            "DELETE FROM verification_verdicts WHERE run_id = ? AND variable = 'wind'",
            (run_id,),
        )
    elif tamper == "no_results":
        conn.execute("DELETE FROM verification_results WHERE run_id = ?", (run_id,))
    else:
        conn.execute(
            """
            INSERT INTO verification_verdicts
                (run_id, variable, outcome, recommended_depth,
                 incumbent_depth, tested_family)
            VALUES (?, 'bogus_var', 'recommend', NULL, 2, '{}')
            """,
            (run_id,),
        )
    conn.commit()

    with pytest.raises(RuntimeError, match="integrity"):
        advance_verification(conn, site_id, payload)
    assert str(_run_row(conn, run_id)["state"]) == "running"
    assert published_run_id(conn, site_id) is None


def test_publish_run_requires_a_running_row() -> None:
    conn = asof_conn()
    site_id, _feeds = _make_site(conn)
    generation = ensure_published_generation(conn, site_id)
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
