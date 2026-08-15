"""§18.14 record-scheduling oracle suite (phase 5 QA).

Complements ``tests/test_forecast_record.py`` with the adversarial
scheduling oracles: exactly one snapshot per local day across the doubled
autumn hour (job-key dedupe while pending AND the record-rows gate after
completion), the spring-forward first-instant-after-the-gap due check, the
gap-scan enqueue gate on ANY jobs row for the day's key, and the
``forecast_record_gap`` monitor condition with its slack window (paired
positive/negative). The claim-priority tier and v3->v4 job-type acceptance
are already pinned in ``tests/test_tz_correction_oracles.py`` and are not
duplicated here. All fixture values are synthetic.

Every test names the production mutation it kills; the mutation loop ran
each mutation, observed the named oracle red, and restored the file
byte-identical (sha256-verified, stale-``__pycache__`` purged per run).
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta

import pytest

from tests.helpers import asof_conn, asof_make_site
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.monitor import build_verdict
from wxverify.settings.keys import set_setting
from wxverify.verification.record import build_forecast_record, resolve_snapshot_utc
from wxverify.worker.scheduler import _enqueue_due_forecast_records

_DAY = date(2035, 6, 15)


def _make_site_tz(conn: sqlite3.Connection, name: str, timezone: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 40.0, -105.0, 900.0, ?)
        """,
        (name, timezone),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _freeze_scheduler_now(
    monkeypatch: pytest.MonkeyPatch, holder: dict[str, datetime]
) -> None:
    monkeypatch.setattr("wxverify.worker.scheduler.utc_now", lambda: holder["now"])


def _seed_day_samples(conn: sqlite3.Connection, site_id: int, day: date) -> None:
    """One variable's worth of knowable-at-T samples for snapshot ``day``."""
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('example-src', ?, 1, 360, 192)
        """,
        (f"grid-{day.isoformat()}",),
    )
    assert cur.lastrowid is not None
    issued = datetime.combine(day, datetime.min.time(), UTC)
    for hour in range(24):
        valid = datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, 'temperature', ?, ?, ?, 10.0, '{}', 'run-x', ?)
            """,
            (
                site_id,
                int(cur.lastrowid),
                isoformat_utc(issued),
                isoformat_utc(valid),
                max(1, hour),
                isoformat_utc(issued + timedelta(minutes=5)),
            ),
        )


def _record_jobs(conn: sqlite3.Connection, site_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT job_key, payload, status FROM jobs
        WHERE site_id = ? AND type = 'forecast_record' ORDER BY id
        """,
        (site_id,),
    ).fetchall()


def _gapscan_jobs(conn: sqlite3.Connection, site_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT job_key, status FROM jobs
        WHERE site_id = ? AND type = 'record_gap_scan' ORDER BY id
        """,
        (site_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Oracle 1 — one snapshot per local day across the fall-back doubled hour:
# active-job dedupe before completion, the record-rows gate after it, and a
# fresh enqueue on the NEXT local day.
# ---------------------------------------------------------------------------


def test_due_check_single_snapshot_across_fall_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """§14 due check: 2035-11-04 America/New_York repeats the 01:00-02:00
    wall-clock hour; a 01:30 snapshot time must still yield exactly one
    ``forecast_record`` job for the day -- at both UTC instants of the
    doubled hour, before AND after the job completes.

    Kills, one at a time:
    - dropping the ``record_day_has_any_row`` gate from
      ``_enqueue_due_forecast_records`` (the second occurrence re-enqueues a
      completed day);
    - widening that gate to any-rows-for-the-site (dropping the
      ``snapshot_local_date`` binding from the probe): the NEXT local day
      then never enqueues.
    """
    conn = asof_conn()
    site_id = _make_site_tz(conn, "sched-fallback-site", "America/New_York")
    ensure_published_generation(conn, site_id)
    set_setting(conn, "record_snapshot_local_time", "01:30")

    holder = {"now": datetime(2035, 11, 4, 5, 45, tzinfo=UTC)}  # 01:45 EDT
    _freeze_scheduler_now(monkeypatch, holder)
    _enqueue_due_forecast_records(conn)
    jobs = _record_jobs(conn, site_id)
    assert [str(j["job_key"]) for j in jobs] == ["record:2035-11-04"]
    assert '"snapshot_local_date":"2035-11-04"' in str(jobs[0]["payload"]).replace(
        " ", ""
    )

    # Second UTC instant of the doubled hour, job still pending: the active
    # dedupe (job_key + idx_jobs_active_dedupe) holds it to one row.
    holder["now"] = datetime(2035, 11, 4, 6, 45, tzinfo=UTC)  # 01:45 EST
    _enqueue_due_forecast_records(conn)
    assert len(_record_jobs(conn, site_id)) == 1

    # Complete the day: the job finishes and its record rows land.
    build_forecast_record(
        conn,
        site_id,
        "2035-11-04",
        now=datetime(2035, 11, 4, 5, 45, tzinfo=UTC),
    )
    conn.execute(
        "UPDATE jobs SET status = 'completed'"
        " WHERE site_id = ? AND type = 'forecast_record'",
        (site_id,),
    )
    # Doubled hour again, active dedupe now inert (no pending row): only the
    # record-rows gate prevents a duplicate snapshot for the same local day.
    holder["now"] = datetime(2035, 11, 4, 6, 50, tzinfo=UTC)
    _enqueue_due_forecast_records(conn)
    assert len(_record_jobs(conn, site_id)) == 1

    # Paired positive: the next local day (T = 01:30 EST = 06:30Z) enqueues.
    holder["now"] = datetime(2035, 11, 5, 6, 35, tzinfo=UTC)
    _enqueue_due_forecast_records(conn)
    keys = [str(j["job_key"]) for j in _record_jobs(conn, site_id)]
    assert keys == ["record:2035-11-04", "record:2035-11-05"]


# ---------------------------------------------------------------------------
# Oracle 2 — spring-forward: T is the first instant at/after the wall-clock
# time; before it, nothing enqueues.
# ---------------------------------------------------------------------------


def test_due_check_spring_forward_first_instant_after_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """§3/§14: 2035-03-11 America/New_York skips 02:00-03:00; a 02:30
    snapshot time resolves to 03:30 EDT (07:30Z), and the due check must not
    enqueue before that instant.

    Kills, one at a time:
    - dropping the ``if now < snapshot_utc: continue`` gate in
      ``_enqueue_due_forecast_records`` (a job enqueues at 06:45Z);
    - ``fold=1`` (or naive-local arithmetic) in ``resolve_snapshot_utc``:
      the gap time then maps to 06:30Z, BEFORE the gap, and the 06:45Z tick
      enqueues a snapshot the 02:30 product could not have known.
    """
    conn = asof_conn()
    site_id = _make_site_tz(conn, "sched-springfwd-site", "America/New_York")
    ensure_published_generation(conn, site_id)
    set_setting(conn, "record_snapshot_local_time", "02:30")
    assert resolve_snapshot_utc(
        "America/New_York", date(2035, 3, 11), "02:30"
    ) == datetime(2035, 3, 11, 7, 30, tzinfo=UTC)

    holder = {"now": datetime(2035, 3, 11, 6, 45, tzinfo=UTC)}
    _freeze_scheduler_now(monkeypatch, holder)
    _enqueue_due_forecast_records(conn)
    assert _record_jobs(conn, site_id) == []

    holder["now"] = datetime(2035, 3, 11, 7, 35, tzinfo=UTC)
    _enqueue_due_forecast_records(conn)
    jobs = _record_jobs(conn, site_id)
    assert [str(j["job_key"]) for j in jobs] == ["record:2035-03-11"]


# ---------------------------------------------------------------------------
# Oracle 3 — gap-scan enqueue is gated on ANY jobs row for the day's key: a
# completed scan does not re-enqueue that day, and the next day gets its own.
# ---------------------------------------------------------------------------


def test_gapscan_enqueue_gated_on_any_jobs_row_for_the_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """§14: the daily ``gapscan:<date>`` enqueue is gated on ANY jobs row
    for that key -- completed included -- so the scan runs once per local
    day, not once per scheduler tick.

    Kills, one at a time:
    - narrowing the gate query with ``AND status IN ('pending','running')``
      (a completed scan re-enqueues on the very next tick);
    - de-scoping the key to a constant ``"gapscan"`` (the next local day
      then never gets its scan).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "sched-gapscan-site")
    ensure_published_generation(conn, site_id)

    day1_t = resolve_snapshot_utc("UTC", _DAY, "07:00")
    holder = {"now": day1_t + timedelta(hours=1)}
    _freeze_scheduler_now(monkeypatch, holder)
    _enqueue_due_forecast_records(conn)
    scans = _gapscan_jobs(conn, site_id)
    assert [str(j["job_key"]) for j in scans] == [f"gapscan:{_DAY.isoformat()}"]

    conn.execute(
        "UPDATE jobs SET status = 'completed'"
        " WHERE site_id = ? AND type = 'record_gap_scan'",
        (site_id,),
    )
    _enqueue_due_forecast_records(conn)
    assert len(_gapscan_jobs(conn, site_id)) == 1  # ANY-row gate holds

    # Close out day 1 so only the gap-scan behavior varies on day 2.
    build_forecast_record(conn, site_id, _DAY.isoformat(), now=holder["now"])
    conn.execute(
        "UPDATE jobs SET status = 'completed'"
        " WHERE site_id = ? AND type = 'forecast_record'",
        (site_id,),
    )
    day2 = _DAY + timedelta(days=1)
    holder["now"] = resolve_snapshot_utc("UTC", day2, "07:00") + timedelta(hours=1)
    _enqueue_due_forecast_records(conn)
    keys = [str(j["job_key"]) for j in _gapscan_jobs(conn, site_id)]
    assert keys == [f"gapscan:{_DAY.isoformat()}", f"gapscan:{day2.isoformat()}"]


# ---------------------------------------------------------------------------
# Oracle 4 — forecast_record_gap monitor condition: paired positive/negative
# around the slack window, and verdict degradation when tripped.
# ---------------------------------------------------------------------------


def test_monitor_record_gap_pairs_and_slack_window() -> None:
    """§16 via ``build_verdict`` (the production read path): a site whose
    latest expected snapshot day has no rows trips ``forecast_record_gap``
    and degrades the overall verdict; inside the post-T slack the expected
    day is still yesterday, so a queued-but-not-yet-run job does not flap
    the condition. The slack value (PENDING_OVERDUE_MINUTES) pins
    implementation latitude (architect to ratify).

    Kills, one at a time:
    - in ``sites_with_record_gap``, dropping ``+ slack`` from the
      expected-day comparison (the 10-minutes-past-T probe flips to today
      and trips);
    - collapsing the expected-day choice to ``local_today`` in both arms
      (same probe trips: yesterday IS recorded);
    - in ``_pipeline_conditions``, ``record_gap_n > 0`` -> ``> 1`` (the
      single-gap positive arm reads ok).
    """
    conn = asof_conn()
    site_id = asof_make_site(conn, "monitor-gap-site")
    ensure_published_generation(conn, site_id)
    # The record day needs real samples: a day with nothing knowable at T now
    # writes no rows at all, so without this the log never begins and the
    # condition is vacuously ok in BOTH arms.
    _seed_day_samples(conn, site_id, _DAY)
    t_day = resolve_snapshot_utc("UTC", _DAY, "07:00")
    build_forecast_record(
        conn, site_id, _DAY.isoformat(), now=t_day + timedelta(minutes=5)
    )
    next_t = resolve_snapshot_utc("UTC", _DAY + timedelta(days=1), "07:00")

    def gap_condition(now: datetime) -> tuple[dict[str, object], str]:
        verdict = build_verdict(
            conn,
            pipeline_enabled=True,
            budget_enabled=False,
            db_enabled=False,
            now=now,
        )
        conditions = verdict["conditions"]
        assert isinstance(conditions, list)
        matches = [
            c
            for c in conditions
            if isinstance(c, dict) and c.get("id") == "forecast_record_gap"
        ]
        assert len(matches) == 1, "forecast_record_gap condition must be present"
        return matches[0], str(verdict["overall"])

    # Negative arm, inside the slack: expected day is yesterday (recorded).
    inside_slack, _ = gap_condition(next_t + timedelta(minutes=10))
    assert inside_slack["ok"] is True

    # Positive arm, past the slack: today is expected and missing.
    tripped, overall = gap_condition(next_t + timedelta(minutes=20))
    assert tripped["ok"] is False
    assert tripped["count"] == 1
    assert overall != "ok"
