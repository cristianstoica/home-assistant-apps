"""Forecast-of-record builder, gap scan, scheduling, and monitor (plan §7/§14/§16).

Implementer tests for the phase-5 machinery; the §18.5/§18.14 oracle
families build on these seams separately. All fixture values are synthetic.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta

import pytest

import wxverify.verification.record as record_mod
from wxverify.core.timeutil import isoformat_utc
from wxverify.db.migrations import run_migrations
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.settings.keys import set_setting
from wxverify.verification.record import (
    MISSED_WINDOW_CLOSED,
    RECORD_DAY_COUNT,
    build_forecast_record,
    record_day_complete,
    record_day_has_any_row,
    resolve_snapshot_utc,
    run_record_gap_scan,
    sites_with_record_gap,
    snapshot_wall_clock,
)
from wxverify.worker.control import JobCancelled, JobDeferred
from wxverify.worker.scheduler import _enqueue_due_forecast_records

_DAY = date(2035, 6, 15)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _make_site(conn: sqlite3.Connection, name: str, timezone: str = "UTC") -> int:
    cur = conn.execute(
        """
        INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
        VALUES (?, 47.0, 25.0, 900.0, ?)
        """,
        (name, timezone),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _make_feed(conn: sqlite3.Connection, model: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO feeds (source, model, default_subscribed,
                           fetch_interval_minutes, max_lead_hours)
        VALUES ('example-src', ?, 1, 360, 192)
        """,
        (model,),
    )
    assert cur.lastrowid is not None
    return int(cur.lastrowid)


def _insert_temp_day(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    local_date: date,
    issued_at: str,
    fetched_at: str | None,
    value: float,
    variable: str = "temperature",
) -> None:
    issued = datetime.fromisoformat(issued_at.replace("Z", "+00:00"))
    for hour in range(24):
        valid = datetime(
            local_date.year, local_date.month, local_date.day, hour, tzinfo=UTC
        )
        lead = max(1, int((valid - issued).total_seconds() // 3600))
        conn.execute(
            """
            INSERT INTO forecast_samples
                (site_id, feed_id, variable, issued_at, valid_at, lead_hours,
                 value, source_raw, model_run_id, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-x', ?)
            """,
            (
                site_id,
                feed_id,
                variable,
                issued_at,
                isoformat_utc(valid),
                lead,
                value,
                fetched_at,
            ),
        )


#: Sample value per variable for the full-grid fixture below.
_GRID_VALUES = {"temperature": 10.0, "wind": 5.0, "precip": 0.0}


def _insert_full_grid(
    conn: sqlite3.Connection,
    *,
    site_id: int,
    feed_id: int,
    start_date: date,
    issued_at: str,
    fetched_at: str | None,
) -> None:
    """Seed every (variable x display day) identity the record grid spans.

    A record day is only written for identities that had samples at T, so a
    fixture seeding one variable for one day produces one row, not a grid.
    Anything asserting on the full 3 x 8 identity set needs the full grid.
    """
    for day in range(RECORD_DAY_COUNT):
        for variable, value in _GRID_VALUES.items():
            _insert_temp_day(
                conn,
                site_id=site_id,
                feed_id=feed_id,
                local_date=start_date + timedelta(days=day),
                issued_at=issued_at,
                fetched_at=fetched_at,
                value=value,
                variable=variable,
            )


def _seed_grid_for(
    conn: sqlite3.Connection, *, site_id: int, feed_id: int, day: date
) -> None:
    """Full grid for snapshot ``day``, issued that morning (before its T)."""
    issued = datetime.combine(day, datetime.min.time(), UTC) + timedelta(hours=6)
    _insert_full_grid(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        start_date=day,
        issued_at=isoformat_utc(issued),
        fetched_at=isoformat_utc(issued + timedelta(minutes=5)),
    )


def _snapshot_t(local_date: date = _DAY) -> datetime:
    return resolve_snapshot_utc("UTC", local_date, "07:00")


def _row_count(conn: sqlite3.Connection, site_id: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM forecast_of_record WHERE site_id = ?",
        (site_id,),
    ).fetchone()
    return int(row["n"])


# ---------------------------------------------------------------- snapshot T


def test_resolve_snapshot_utc_fixed_offset() -> None:
    # Etc/GMT-3 is UTC+3 (POSIX sign inversion): 07:00 local == 04:00Z.
    resolved = resolve_snapshot_utc("Etc/GMT-3", _DAY, "07:00")
    assert isoformat_utc(resolved) == "2035-06-15T04:00:00Z"


def test_resolve_snapshot_utc_fallback_first_occurrence() -> None:
    # 2035-11-04 America/New_York repeats 01:00-02:00; fold=0 picks the
    # FIRST occurrence (EDT, UTC-4) per the §3 first-instant rule.
    resolved = resolve_snapshot_utc("America/New_York", date(2035, 11, 4), "01:30")
    assert isoformat_utc(resolved) == "2035-11-04T05:30:00Z"


def test_resolve_snapshot_utc_rejects_bad_wall_clock() -> None:
    with pytest.raises(ValueError):
        resolve_snapshot_utc("UTC", _DAY, "25:99")


def test_snapshot_wall_clock_resolution_order() -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    assert snapshot_wall_clock(conn, site_id) == "07:00"
    set_setting(conn, "record_snapshot_local_time", "06:30")
    assert snapshot_wall_clock(conn, site_id) == "06:30"
    set_setting(conn, f"record_snapshot_local_time:{site_id}", "05:15")
    assert snapshot_wall_clock(conn, site_id) == "05:15"
    # Unparseable per-site value falls through to the global one.
    set_setting(conn, f"record_snapshot_local_time:{site_id}", "not-a-time")
    assert snapshot_wall_clock(conn, site_id) == "06:30"


# ------------------------------------------------------------- record builder


def test_record_job_writes_full_grid() -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    _insert_full_grid(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        start_date=_DAY,
        issued_at="2035-06-15T06:00:00Z",
        fetched_at="2035-06-15T06:05:00Z",
    )
    t = _snapshot_t()
    build_forecast_record(conn, site_id, _DAY.isoformat(), now=t + timedelta(minutes=5))
    rows = conn.execute(
        "SELECT * FROM forecast_of_record WHERE site_id = ? ORDER BY id",
        (site_id,),
    ).fetchall()
    assert len(rows) == 24  # 3 variables x 8 target days
    assert {str(r["variable"]) for r in rows} == {"temperature", "wind", "precip"}
    assert {int(r["display_lead"]) for r in rows} == set(range(8))
    assert all(str(r["status"]) == "recorded" for r in rows)
    assert all(str(r["write_path"]) == "on_time" for r in rows)
    assert all(int(r["write_latency_seconds"]) == 300 for r in rows)
    assert all(str(r["snapshot_utc"]) == isoformat_utc(t) for r in rows)

    day0_temp = next(
        r
        for r in rows
        if str(r["variable"]) == "temperature" and int(r["display_lead"]) == 0
    )
    assert json.loads(str(day0_temp["selected_feed_ids"])) == [feed_id]
    hourly = json.loads(str(day0_temp["hourly_values"]))
    assert len(hourly) == 24
    assert all(v == 10.0 for _, v in hourly)
    # Obligation (a): daily_quantities carries BOTH artifacts — the DISPLAYED
    # dailies (aggregate-per-feed-then-blend, §6/§7) and the §5 coverage
    # outcomes from coverage.evaluate_variable.
    quantities = json.loads(str(day0_temp["daily_quantities"]))
    displayed = quantities["displayed"]
    assert displayed["high_c"] == 10.0
    assert displayed["low_c"] == 10.0
    assert displayed["partial"] is False
    assert displayed["low_confidence"] is True  # single unscored feed
    outcomes = quantities["outcomes"]
    assert {q["quantity"] for q in outcomes} == {
        "temperature_high",
        "temperature_low",
    }
    assert all(q["eligible"] for q in outcomes)
    assert all(q["covered_hours"] == 24 for q in outcomes)
    policy = json.loads(str(day0_temp["policy"]))
    assert set(policy) >= {"blend_depth", "min_n", "window_days", "rain_threshold_mm"}

    day5_wind = next(
        r for r in rows if str(r["variable"]) == "wind" and int(r["display_lead"]) == 5
    )
    assert json.loads(str(day5_wind["selected_feed_ids"])) == [feed_id]


def test_record_skips_cells_with_no_samples() -> None:
    # Inverted from the pre-0.11.1 behaviour: a cell with nothing knowable at T
    # used to be written as an empty ``recorded`` row, which is indistinguishable
    # from a real all-feeds-agree-on-nothing day. Only sampled identities land.
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    _insert_temp_day(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=_DAY,
        issued_at="2035-06-15T06:00:00Z",
        fetched_at="2035-06-15T06:05:00Z",
        value=10.0,
    )
    t = _snapshot_t()
    build_forecast_record(conn, site_id, _DAY.isoformat(), now=t + timedelta(minutes=5))
    rows = conn.execute(
        "SELECT variable, display_lead FROM forecast_of_record WHERE site_id = ?",
        (site_id,),
    ).fetchall()
    assert [(str(r["variable"]), int(r["display_lead"])) for r in rows] == [
        ("temperature", 0)
    ]


def test_record_retry_is_idempotent() -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    _insert_temp_day(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=_DAY,
        issued_at="2035-06-14T06:00:00Z",
        fetched_at="2035-06-14T06:05:00Z",
        value=10.0,
    )
    t = _snapshot_t()
    build_forecast_record(conn, site_id, _DAY.isoformat(), now=t + timedelta(minutes=5))
    ids_before = [
        int(r["id"])
        for r in conn.execute(
            "SELECT id FROM forecast_of_record WHERE site_id=? ORDER BY id", (site_id,)
        )
    ]
    # Retry hours later (still in window) after a post-T sample landed.
    _insert_temp_day(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=_DAY,
        issued_at="2035-06-15T08:00:00Z",
        fetched_at="2035-06-15T08:05:00Z",
        value=99.0,
    )
    build_forecast_record(conn, site_id, _DAY.isoformat(), now=t + timedelta(hours=6))
    ids_after = [
        int(r["id"])
        for r in conn.execute(
            "SELECT id FROM forecast_of_record WHERE site_id=? ORDER BY id", (site_id,)
        )
    ]
    assert ids_after == ids_before  # confirmed, never replaced


def test_record_excludes_samples_fetched_after_t() -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    _insert_temp_day(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=_DAY,
        issued_at="2035-06-14T06:00:00Z",
        fetched_at="2035-06-14T06:05:00Z",
        value=10.0,
    )
    # Newer run issued before T but fetched AFTER T: not available at T.
    _insert_temp_day(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=_DAY,
        issued_at="2035-06-15T06:00:00Z",
        fetched_at="2035-06-15T09:00:00Z",
        value=99.0,
    )
    t = _snapshot_t()
    build_forecast_record(conn, site_id, _DAY.isoformat(), now=t + timedelta(hours=3))
    day0_temp = conn.execute(
        """
        SELECT hourly_values FROM forecast_of_record
        WHERE site_id=? AND variable='temperature' AND display_lead=0
        """,
        (site_id,),
    ).fetchone()
    hourly = json.loads(str(day0_temp["hourly_values"]))
    assert all(v == 10.0 for _, v in hourly)


def test_record_before_t_defers_and_beyond_window_cancels() -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    ensure_published_generation(conn, site_id)
    t = _snapshot_t()
    with pytest.raises(JobDeferred) as deferred:
        build_forecast_record(
            conn, site_id, _DAY.isoformat(), now=t - timedelta(minutes=5)
        )
    assert deferred.value.next_attempt_at == isoformat_utc(t)
    with pytest.raises(JobCancelled):
        build_forecast_record(
            conn, site_id, _DAY.isoformat(), now=t + timedelta(hours=25)
        )
    assert _row_count(conn, site_id) == 0  # a failing attempt writes NOTHING


def test_record_disabled_site_cancels() -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    conn.execute("UPDATE sites SET enabled=0 WHERE id=?", (site_id,))
    with pytest.raises(JobCancelled):
        build_forecast_record(conn, site_id, _DAY.isoformat())


# ----------------------------------------------------------------- gap scan


def test_gap_scan_fresh_site_is_noop() -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    ensure_published_generation(conn, site_id)
    assert run_record_gap_scan(conn, site_id, {}) is None
    assert _row_count(conn, site_id) == 0


def test_gap_scan_missed_beyond_window_reconstructs_inside() -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    generation_id = ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    # One grid per snapshot day, issued that morning before T, so every day the
    # scan visits has a full 3 x 8 identity set knowable as of its own T.
    for offset in range(4):
        _seed_grid_for(
            conn, site_id=site_id, feed_id=feed_id, day=_DAY + timedelta(days=offset)
        )
    t0 = _snapshot_t(_DAY)
    build_forecast_record(
        conn, site_id, _DAY.isoformat(), now=t0 + timedelta(minutes=5)
    )
    # Now = D+3's T + 1h: D+1 and D+2 windows are closed, D+3 is in-window.
    now = _snapshot_t(_DAY + timedelta(days=3)) + timedelta(hours=1)
    assert run_record_gap_scan(conn, site_id, {}, now=now) is None

    for offset, expected_status in ((1, "missed"), (2, "missed"), (3, "recorded")):
        day_iso = (_DAY + timedelta(days=offset)).isoformat()
        assert record_day_has_any_row(conn, site_id, generation_id, day_iso)
        rows = conn.execute(
            """
            SELECT status, missed_reason, write_path FROM forecast_of_record
            WHERE site_id=? AND snapshot_local_date=?
            """,
            (site_id, day_iso),
        ).fetchall()
        assert len(rows) == 24
        assert all(str(r["status"]) == expected_status for r in rows)
        if expected_status == "missed":
            assert all(str(r["missed_reason"]) == MISSED_WINDOW_CLOSED for r in rows)
            assert all(r["write_path"] is None for r in rows)
        else:
            assert all(str(r["write_path"]) == "late_reconstruction" for r in rows)


def test_gap_scan_chunks_with_continuation(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    for offset in range(7):
        _seed_grid_for(
            conn, site_id=site_id, feed_id=feed_id, day=_DAY + timedelta(days=offset)
        )
    t0 = _snapshot_t(_DAY)
    build_forecast_record(
        conn, site_id, _DAY.isoformat(), now=t0 + timedelta(minutes=5)
    )
    monkeypatch.setattr(record_mod, "GAP_SCAN_MAX_DATES", 2)
    now = _snapshot_t(_DAY + timedelta(days=5)) + timedelta(hours=26)
    # The traversal starts at the log's origin (D+0) rather than its tail, so
    # the chunk budget is spent on D+0..D+6 -- the already-complete D+0 costs a
    # slot and is simply cleared.
    seen: list[str] = []
    payload: dict[str, object] = {}
    for _ in range(4):
        nxt = run_record_gap_scan(conn, site_id, payload, now=now)
        if nxt is None:
            break
        seen.append(str(nxt["after_date"]))
        payload = nxt
    else:  # pragma: no cover - guards an unterminated scan
        raise AssertionError("gap scan did not terminate")
    assert seen == [(_DAY + timedelta(days=off)).isoformat() for off in (1, 3, 5)]
    # now is T(D+5)+26h == D+6 09:00: D+1..D+5 are closed (missed) and D+6
    # (today, in-window) is late-reconstructed -- 7 days of 24 rows total.
    assert _row_count(conn, site_id) == 24 * 7


# ---------------------------------------------------------------- scheduler


def test_scheduler_enqueues_record_and_gap_scan_once() -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    # Midnight snapshot time: today's T has always passed in this test.
    set_setting(conn, "record_snapshot_local_time", "00:00")
    _enqueue_due_forecast_records(conn)
    _enqueue_due_forecast_records(conn)  # dedupe via job_key
    jobs = conn.execute(
        "SELECT type, job_key FROM jobs WHERE site_id=? ORDER BY id", (site_id,)
    ).fetchall()
    types = [str(r["type"]) for r in jobs]
    assert types.count("forecast_record") == 1
    assert types.count("record_gap_scan") == 1
    assert all(str(r["job_key"]).startswith(("record:", "gapscan:")) for r in jobs)


def test_scheduler_skips_record_when_rows_exist() -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    set_setting(conn, "record_snapshot_local_time", "00:00")
    generation_id = ensure_published_generation(conn, site_id)
    feed_id = _make_feed(conn, "model-a")
    today = datetime.now(UTC).date()
    # The scheduler now gates on COMPLETENESS, not on presence, so the day has
    # to carry every identity before it counts as done.
    issued = datetime.combine(today - timedelta(days=1), datetime.min.time(), UTC)
    issued += timedelta(hours=23)
    _insert_full_grid(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        start_date=today,
        issued_at=isoformat_utc(issued),
        fetched_at=isoformat_utc(issued + timedelta(minutes=5)),
    )
    t = resolve_snapshot_utc("UTC", today, "00:00")
    build_forecast_record(
        conn, site_id, today.isoformat(), now=t + timedelta(minutes=1)
    )
    assert record_day_complete(conn, site_id, generation_id, today.isoformat())
    _enqueue_due_forecast_records(conn)
    n = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE site_id=? AND type='forecast_record'",
        (site_id,),
    ).fetchone()
    assert int(n["n"]) == 0


# ------------------------------------------------------------------ monitor


def test_sites_with_record_gap() -> None:
    conn = _conn()
    site_id = _make_site(conn, "site-a")
    ensure_published_generation(conn, site_id)
    now = _snapshot_t(_DAY) + timedelta(hours=2)
    # Log not begun: never counted as a gap.
    assert sites_with_record_gap(conn, now) == 0
    feed_id = _make_feed(conn, "model-a")
    _insert_temp_day(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        local_date=_DAY,
        issued_at="2035-06-14T06:00:00Z",
        fetched_at="2035-06-14T06:05:00Z",
        value=10.0,
    )
    build_forecast_record(
        conn, site_id, _DAY.isoformat(), now=_snapshot_t(_DAY) + timedelta(minutes=5)
    )
    # Expected day (today, past T + slack) has rows: no gap.
    assert sites_with_record_gap(conn, now) == 0
    # Two days later at T+2h: the expected day has no rows -> gap.
    later = _snapshot_t(_DAY + timedelta(days=2)) + timedelta(hours=2)
    assert sites_with_record_gap(conn, later) == 1
    # Just before that day's T: expected day is yesterday (also missing).
    before_t = _snapshot_t(_DAY + timedelta(days=2)) - timedelta(hours=2)
    assert sites_with_record_gap(conn, before_t) == 1
