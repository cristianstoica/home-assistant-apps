"""Tests for the scored round's schedule and daytime downloads (P4 plan
§18.2, §18.3) --
``plans/2026-10-22-daily-close-pairs-and-publication.md``.

RS1-RS13 and DT1-DT4, one test per row (some rows have two or three
parts, kept in one test). Every assertion that kills a mutant named in
the row's last column (plan §19.2) carries a message of the form
``"M<n> -> at this assertion: correct = <X>, mutant = <Y>"``.

``wxverify.worker.round`` (``round_state``, ``schedule_day``,
``enqueue_due_round_tries``, ``enqueue_due_daytime_downloads``) and the
``scoring_round_local_time`` setter do not exist before piece 3 (plan
§20 Step 4), so this whole file fails at collection with
``ModuleNotFoundError`` until then -- the plan's Step 3 accepts that.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from wxverify.worker.round import (  # P4 piece 3 (plan §4.4, §4.5, §5.1, §16.1)
    round_state,
    schedule_day,
)

from tests.helpers import (
    STANDARD_NIGHT_R,
    D,
    MovingClock,
    ProviderConnectError,
    ProviderEmpty,
    ProviderHttpError,
    ProviderReply,
    asof_make_site,
    disable_other_enqueuers,
    failed_scope_count,
    install_provider_standin,
    meteoblue_member_sample,
    patch_close_clock,
    round_schedule_sample,
    seed_round_schedule_feeds,
    standard_night,
    subscribe_meteoblue_package,
    tick,
)
from wxverify.core.timeutil import isoformat_utc, parse_utc
from wxverify.db.queue import claim_next_job, reclaim_all_stale
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.tz_generations import ensure_published_generation
from wxverify.monitor import FAILED_JOB_AGE_HOURS, FAILED_SCOPES_SQL
from wxverify.worker.processor import run_claimed_job

R = STANDARD_NIGHT_R  # 2024-06-11 (plan §15.1's standard night: R = D + 1)


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------


def _night(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    subscribe: tuple[str, ...] = ("X", "Y", "Z"),
    clock_start: datetime | None = None,
):
    """P3's standard night (``standard_night``) plus P4's round-schedule
    feeds (``seed_round_schedule_feeds``), the provider stand-in
    installed, and a :class:`MovingClock` patched over every close-chain
    module. Also disables the daily close's other job-kind enqueuers
    (verification runs, score refresh, wind days, forecast records,
    obs), which are irrelevant to the round/daytime schedule and would
    otherwise add job-queue noise. Returns ``(db, conn, site_id,
    feed_ids, standin, clock)``.
    """
    db, _writer, site_id, _stations = standard_night(tmp_path)
    conn = db._conn  # noqa: SLF001 -- tests read the real writer conn directly
    feed_ids = seed_round_schedule_feeds(conn, site_id, subscribe=subscribe)
    conn.commit()
    standin = install_provider_standin(monkeypatch)
    disable_other_enqueuers(monkeypatch)
    start = clock_start or datetime(D.year, D.month, D.day, 23, 0, tzinfo=UTC)
    clock = MovingClock(start)
    patch_close_clock(monkeypatch, clock)
    return db, conn, site_id, feed_ids, standin, clock


def _round_jobs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Every ``round:{feed_id}``-keyed ``fetch_feed`` job, in id order."""
    return list(
        conn.execute(
            "SELECT * FROM jobs WHERE type='fetch_feed'"
            " AND job_key LIKE 'round:%' ORDER BY id"
        )
    )


def _job_for_key(conn: sqlite3.Connection, job_key: str) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT * FROM jobs WHERE type='fetch_feed' AND job_key=? ORDER BY id",
            (job_key,),
        )
    )


def _downloads(conn: sqlite3.Connection, feed_id: int) -> list[sqlite3.Row]:
    """Every ``forecast_downloads`` row of ``feed_id``, in id order (4.2)."""
    return list(
        conn.execute(
            "SELECT * FROM forecast_downloads WHERE feed_id=? ORDER BY id",
            (feed_id,),
        )
    )


def _label(model: str, hour: int) -> str:
    """A synthetic ``issued_at``/run-label stamp, distinct per hour so two
    different calls never collide by accident."""
    return f"{R.isoformat()}T{hour:02d}:00:00Z[{model}]"


def _reply(feed_id_name: str, model: str, hour: int) -> ProviderReply:
    """One usable sample at lead 6h for ``model`` (plan §18.1's "a reply
    with given run labels and values")."""
    issued_at = _label(model, hour)
    valid_at = isoformat_utc(parse_utc(issued_at.split("[")[0]) + timedelta(hours=6))
    return ProviderReply(
        samples=(
            round_schedule_sample(model, "temperature", issued_at, valid_at, 6, 10.0),
        )
    )


def _is_cancelled(row: sqlite3.Row) -> bool:
    """A completed job with no success marker (``complete``, no
    ``result``) -- the ``JobCancelled`` branch of ``run_claimed_job``
    (``worker/processor.py:409-417``)."""
    return row["status"] == "completed" and row["result"] is None


def _insert_pending_job(
    conn: sqlite3.Connection,
    job_type: str,
    site_id: int,
    job_key: str,
    payload: dict[str, object],
    time_of_day: str,
) -> None:
    """A ``pending`` job row by plain INSERT (plan §18.1's "seed helpers
    by plain INSERT"), stamped on D's date at ``time_of_day`` so claim
    order among several such rows follows their insertion order (RS9)."""
    stamp = f"{D.isoformat()}T{time_of_day}.000Z"
    conn.execute(
        "INSERT INTO jobs"
        " (type, site_id, job_key, payload, status, created_at, updated_at)"
        " VALUES (?, ?, ?, ?, 'pending', ?, ?)",
        (job_type, site_id, job_key, json.dumps(payload), stamp, stamp),
    )


# ---------------------------------------------------------------------------
# §18.2 The round's schedule
# ---------------------------------------------------------------------------


def test_rs1_round_starts_at_the_setting_with_try_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS1: standard night; ticks at R 00:09:59Z, 00:10Z; X answers."""
    db, conn, _site_id, feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=("X", "Y", "Z")
    )
    for name in ("X", "Y", "Z"):
        standin.program(f"synthetic-round-{name.lower()}", _reply(name, name, 0))

    asyncio.run(
        tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 9, 59, tzinfo=UTC))
    )
    before = _round_jobs(conn)
    assert len(before) == 0, (
        f"M1 -> at this assertion: correct = 0, mutant = {len(before)}"
    )

    asyncio.run(
        tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 10, 0, tzinfo=UTC))
    )
    after = _round_jobs(conn)
    keys = {row["job_key"] for row in after}
    expected_keys = {f"round:{feed_ids[name]}" for name in ("X", "Y", "Z")}
    assert keys == expected_keys, (
        f"M140 -> at this assertion: correct = {sorted(expected_keys)!r},"
        f" mutant = {sorted(keys)!r}"
    )
    for row in after:
        assert row["max_retries"] == 0, (
            f"M2 -> at this assertion: correct = 0, mutant = {row['max_retries']}"
        )
        payload = json.loads(row["payload"])
        assert payload["slot"] == "round"
        assert payload["round_date"] == R.isoformat()
        assert payload["round_try"] == 1


def test_rs2_four_tries_at_five_twelve_and_twenty_minutes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS2: X fails tries 1-3 (404, ConnectError, empty), succeeds at try 4."""
    db, conn, _site_id, feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=("X",)
    )
    standin.program(
        "synthetic-round-x",
        ProviderHttpError(404),
        ProviderConnectError(),
        ProviderEmpty(),
        _reply("X", "synthetic-round-x", 0),
    )

    for minute in (10, 15, 22, 30):
        asyncio.run(
            tick(db, clock, at=datetime(R.year, R.month, R.day, 0, minute, tzinfo=UTC))
        )
        job = claim_next_job(conn)
        assert job is not None, f"no round try pending at minute {minute}"
        asyncio.run(run_claimed_job(db, job, lane="main"))
        conn.commit()

    rows = _downloads(conn, feed_ids["X"])
    assert len(rows) == 4, (
        f"M3/M4 -> at this assertion: correct = 4, mutant = {len(rows)}"
    )
    outcome = rows[-1]["outcome"]
    assert outcome == "stored", (
        f"M3/M4 -> at this assertion: correct = 'stored', mutant = {outcome!r}"
    )

    fifth = _job_for_key(conn, f"round:{feed_ids['X']}")
    pending_fifth = [r for r in fifth if r["status"] == "pending"]
    assert pending_fifth == [], (
        f"M4 -> at this assertion: correct = 0 pending, mutant = {len(pending_fifth)}"
    )


def test_rs3_no_fifth_try_and_bound_cancels_the_last_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS3 (i): X fails every try, then the clock moves to 00:41Z.
    (ii): only Y subscribed; Y fails tries 1-3, and its try 4 (enqueued at
    the 00:30Z tick) is first claimed at exactly 00:40Z.
    """
    db, conn, site_id, feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=("X",)
    )
    standin.program(
        "synthetic-round-x",
        ProviderHttpError(404),
        ProviderHttpError(404),
        ProviderHttpError(404),
        ProviderHttpError(404),
    )
    for minute in (10, 15, 22, 30):
        asyncio.run(
            tick(db, clock, at=datetime(R.year, R.month, R.day, 0, minute, tzinfo=UTC))
        )
        job = claim_next_job(conn)
        assert job is not None
        asyncio.run(run_claimed_job(db, job, lane="main"))
        conn.commit()
    state_before = round_state(conn, site_id=site_id, round_date=R)
    assert state_before.settled is False, (
        "M6 -> at this assertion: correct = False (try 4 just ran, not settled"
        f" until read again), mutant = {state_before.settled}"
    )

    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 41, tzinfo=UTC)))
    no_fifth = _round_jobs(conn)
    pending = [r for r in no_fifth if r["status"] == "pending"]
    assert pending == [], (
        f"M4 -> at this assertion: correct = 0, mutant = {len(pending)}"
    )
    state_after = round_state(conn, site_id=site_id, round_date=R)
    assert state_after.settled is True, (
        f"M4 -> at this assertion: correct = True, mutant = {state_after.settled}"
    )

    # (ii): only Y subscribed.
    db2, conn2, site2_id, feed2_ids, standin2, clock2 = _night(
        tmp_path / "rs3ii", monkeypatch, subscribe=("Y",)
    )
    standin2.program(
        "synthetic-round-y",
        ProviderHttpError(404),
        ProviderHttpError(404),
        ProviderHttpError(404),
    )
    for minute in (10, 15, 22):
        asyncio.run(
            tick(
                db2, clock2, at=datetime(R.year, R.month, R.day, 0, minute, tzinfo=UTC)
            )
        )
        job = claim_next_job(conn2)
        assert job is not None
        asyncio.run(run_claimed_job(db2, job, lane="main"))
        conn2.commit()
    asyncio.run(
        tick(db2, clock2, at=datetime(R.year, R.month, R.day, 0, 30, tzinfo=UTC))
    )
    try4 = _job_for_key(conn2, f"round:{feed2_ids['Y']}")
    pending_try4 = [r for r in try4 if r["status"] == "pending"]
    assert len(pending_try4) == 1, "try 4 must be enqueued at the 00:30Z tick"

    clock2.set(datetime(R.year, R.month, R.day, 0, 40, tzinfo=UTC))
    job4 = claim_next_job(conn2)
    assert job4 is not None
    asyncio.run(run_claimed_job(db2, job4, lane="main"))
    conn2.commit()
    row4 = conn2.execute("SELECT * FROM jobs WHERE id=?", (job4.id,)).fetchone()
    assert _is_cancelled(row4), (
        f"M5 -> at this assertion: correct = cancelled (no call), mutant ="
        f" status={row4['status']!r} result={row4['result']!r}"
    )
    assert standin2.calls.get("synthetic-round-y", 0) == 3, (
        f"M5 -> at this assertion: correct = 3 (try 4 makes no call),"
        f" mutant = {standin2.calls.get('synthetic-round-y', 0)}"
    )
    downloads = _downloads(conn2, feed2_ids["Y"])
    rows_n = len(downloads)
    assert rows_n == 3, (
        f"M5 -> at this assertion: correct = 3 (no fourth row), mutant = {rows_n}"
    )


def test_rs4_budget_and_ineligible_settle_without_a_second_try(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS4: X stored at try 1; Y's try 1 returns BudgetExhausted; Z
    ineligible (switched off at the site after enqueue)."""
    from wxverify.collection.budget import set_source_cap

    db, conn, site_id, feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=("X", "Y", "Z")
    )
    standin.program("synthetic-round-x", _reply("X", "synthetic-round-x", 0))
    set_source_cap(conn, "synthetic-round-y", daily_call_limit=0)
    conn.commit()

    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 10, tzinfo=UTC)))
    # Switch Z off at the site AFTER its round job is enqueued.
    conn.execute(
        "UPDATE site_feed_state SET enabled=0 WHERE site_id=? AND feed_id=?",
        (site_id, feed_ids["Z"]),
    )
    conn.commit()

    for name in ("X", "Y", "Z"):
        job = claim_next_job(conn)
        assert job is not None, name
        asyncio.run(run_claimed_job(db, job, lane="main"))
        conn.commit()

    x_rows = _downloads(conn, feed_ids["X"])
    y_rows = _downloads(conn, feed_ids["Y"])
    z_rows = _downloads(conn, feed_ids["Z"])
    assert x_rows[-1]["outcome"] == "stored"
    assert y_rows[-1]["outcome"] == "budget"
    assert z_rows[-1]["outcome"] == "ineligible"

    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 15, tzinfo=UTC)))
    second_tries = _round_jobs(conn)
    pending_second = [r for r in second_tries if r["status"] == "pending"]
    assert pending_second == [], (
        f"M6 -> at this assertion: correct = 0, mutant = {len(pending_second)}"
    )
    state = round_state(conn, site_id=site_id, round_date=R)
    assert state.settled is True, (
        f"M6 -> at this assertion: correct = True, mutant = {state.settled}"
    )


def test_rs5_round_state_with_no_try_and_a_future_date(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS5: round_state for a round date with no try at all, and for one
    3 days ahead."""
    _db, conn, site_id, _feed_ids, _standin, _clock = _night(tmp_path, monkeypatch)

    state_r = round_state(conn, site_id=site_id, round_date=R)
    assert state_r.settled is False, (
        f"M7 -> at this assertion: correct = False, mutant = {state_r.settled}"
    )
    record_r = schedule_day(conn, site_id=site_id, local_date=R)
    # The bound is always the setting's time (default 00:10) + 30 minutes,
    # on R's own schedule_day -- computed independently of any try.
    expected_r = record_r.day_start_utc + timedelta(minutes=40)
    assert state_r.bound_utc == expected_r, (
        f"M7 -> at this assertion: correct = {expected_r.isoformat()},"
        f" mutant = {state_r.bound_utc.isoformat()}"
    )

    future = R + timedelta(days=3)
    state_future = round_state(conn, site_id=site_id, round_date=future)
    record_future = schedule_day(conn, site_id=site_id, local_date=future)
    expected_future = record_future.day_start_utc + timedelta(minutes=40)
    assert state_future.settled is False
    assert state_future.bound_utc == expected_future, (
        f"M7 -> at this assertion: correct = {expected_future.isoformat()},"
        f" mutant = {state_future.bound_utc.isoformat()}"
    )


def test_rs6_round_time_setting_range(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS6: scoring_round_local_time set to 01:30; 04:59; 05:01; 00:04."""
    from wxverify.worker.round import set_scoring_round_local_time_sync

    from wxverify.settings.keys import get_setting

    _db, conn, site_id, _feed_ids, _standin, _clock = _night(tmp_path, monkeypatch)

    set_scoring_round_local_time_sync(conn, "01:30")
    conn.commit()
    record = schedule_day(conn, site_id=site_id, local_date=R)
    state = round_state(conn, site_id=site_id, round_date=R)
    expected_bound = record.day_start_utc + timedelta(hours=2)
    assert state.bound_utc == expected_bound, (
        f"M8 -> at this assertion: correct = {expected_bound.isoformat()},"
        f" mutant = {state.bound_utc.isoformat()}"
    )

    set_scoring_round_local_time_sync(conn, "04:59")
    conn.commit()
    assert get_setting(conn, "scoring_round_local_time") == "04:59"

    for bad in ("05:01", "00:04"):
        with pytest.raises(ValueError):
            set_scoring_round_local_time_sync(conn, bad)
        unchanged = get_setting(conn, "scoring_round_local_time")
        assert unchanged == "04:59", (
            f"M8 -> at this assertion: correct = '04:59' (refused, unchanged),"
            f" mutant = {unchanged!r}"
        )


def test_rs7_round_start_from_the_records_zone_not_sites_timezone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS7: zone Etc/GMT-14, close on, R's record from P1's helper; after
    the records exist, UPDATE sites SET timezone='UTC'."""
    from tests.helpers import day_close_site
    from wxverify import config
    from wxverify.db.connection import close_db, init_db

    close_db()
    db_path = tmp_path / "rs7.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001

    site_id = day_close_site(
        conn, timezone="Etc/GMT-14", close_on=True, with_records=True
    )
    seed_round_schedule_feeds(conn, site_id, subscribe=("X",))
    conn.commit()
    install_provider_standin(monkeypatch)
    disable_other_enqueuers(monkeypatch)
    clock = MovingClock(datetime(D.year, D.month, D.day, 23, 0, tzinfo=UTC))
    patch_close_clock(monkeypatch, clock)

    record_before = schedule_day(conn, site_id=site_id, local_date=R)
    conn.execute("UPDATE sites SET timezone='UTC' WHERE id=?", (site_id,))
    conn.commit()

    state = round_state(conn, site_id=site_id, round_date=R)
    expected_start = record_before.day_start_utc + timedelta(hours=0, minutes=10)
    expected_bound = expected_start + timedelta(minutes=30)
    assert state.bound_utc == expected_bound, (
        f"M9 -> at this assertion: correct = {expected_bound.isoformat()},"
        f" mutant = {state.bound_utc.isoformat()}"
    )
    # The round's local 00:10 on R in Etc/GMT-14 (UTC+14) falls on R-1 10:10Z.
    assert expected_start.date() == (R - timedelta(days=1)), (
        "fixture sanity: Etc/GMT-14's local midnight falls a day earlier in UTC"
    )


def test_rs8_meteoblue_slots_dropped_latest_first_and_budget_stops_a_try(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS8: round tries 2 and 3 for P (meteoblue-style), then the day's
    slots."""
    db, conn, site_id, _feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=()
    )
    package_id = subscribe_meteoblue_package(conn, site_id)
    conn.commit()
    p1_issued_at = _label("P1", 0)
    p1_valid_at = isoformat_utc(
        parse_utc(p1_issued_at.split("[")[0]) + timedelta(hours=6)
    )
    standin.program(
        "meteoblue",
        ProviderHttpError(500),
        ProviderReply(
            samples=(
                meteoblue_member_sample(
                    "synthetic-p1", "temperature", p1_issued_at, p1_valid_at, 6, 10.0
                ),
            )
        ),
    )

    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 10, tzinfo=UTC)))
    job1 = claim_next_job(conn)
    assert job1 is not None
    asyncio.run(run_claimed_job(db, job1, lane="main"))
    conn.commit()

    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 15, tzinfo=UTC)))
    job2 = claim_next_job(conn)
    assert job2 is not None
    asyncio.run(run_claimed_job(db, job2, lane="main"))
    conn.commit()

    slots = sorted(
        json.loads(r["payload"])["slot"]
        for r in _job_for_key(conn, f"fetch:{package_id}")
    )
    assert slots == ["06:10"], (
        f"M10/M11 -> at this assertion: correct = ['06:10'], mutant = {slots!r}"
    )

    from wxverify.collection.budget import set_source_cap

    set_source_cap(conn, "meteoblue", daily_call_limit=0)
    conn.commit()
    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 22, tzinfo=UTC)))
    job3 = claim_next_job(conn)
    if job3 is not None:
        asyncio.run(run_claimed_job(db, job3, lane="main"))
        conn.commit()
        rows = _downloads(conn, package_id)
        outcome = rows[-1]["outcome"]
        assert outcome == "budget", (
            f"M10/M11 -> at this assertion: correct = 'budget', mutant = {outcome!r}"
        )


def test_rs9_round_tries_claim_before_an_ordinary_fetch_and_verification_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS9: claim_next_job with a pending ordinary fetch_feed, a pending
    verification_run and a pending round try, all due, round created
    last."""
    _db, conn, site_id, feed_ids, _standin, _clock = _night(
        tmp_path, monkeypatch, subscribe=("X", "Y")
    )
    _insert_pending_job(
        conn, "fetch_feed", site_id, f"fetch:{feed_ids['X']}", {}, "00:00:00"
    )
    _insert_pending_job(conn, "verification_run", site_id, "verify:1", {}, "00:00:01")
    _insert_pending_job(
        conn,
        "fetch_feed",
        site_id,
        f"round:{feed_ids['Y']}",
        {"slot": "round", "round_date": R.isoformat(), "round_try": 1},
        "00:00:02",
    )
    conn.commit()

    job = claim_next_job(conn)
    assert job is not None
    assert job.job_key == f"round:{feed_ids['Y']}", (
        f"M12 -> at this assertion: correct = 'round:{feed_ids['Y']}',"
        f" mutant = {job.job_key!r}"
    )


def test_rs10_backoff_then_success_while_the_package_is_capped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS10: X answers a 500 at try 1 and a reply at try 2; P's cap is
    already used up, so P's try 1 is refused; then the clock moves past
    X's backoff end and past the next UTC midnight."""
    from wxverify.collection.budget import set_source_cap

    db, conn, site_id, feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=("X",)
    )
    package_id = subscribe_meteoblue_package(conn, site_id)
    set_source_cap(conn, "meteoblue", daily_call_limit=0)
    conn.commit()
    standin.program(
        "synthetic-round-x",
        ProviderHttpError(500),
        _reply("X", "synthetic-round-x", 0),
    )

    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 10, tzinfo=UTC)))
    for job in (claim_next_job(conn), claim_next_job(conn)):
        if job is None:
            continue
        asyncio.run(run_claimed_job(db, job, lane="main"))
        conn.commit()

    x_rows = _downloads(conn, feed_ids["X"])
    p_rows = _downloads(conn, package_id)
    assert x_rows[-1]["outcome"] == "backoff"
    assert p_rows[-1]["outcome"] == "budget"

    clock.set(datetime(R.year, R.month, R.day, 2, 0, tzinfo=UTC))
    asyncio.run(tick(db, clock))
    job = claim_next_job(conn)
    if job is not None:
        asyncio.run(run_claimed_job(db, job, lane="main"))
        conn.commit()

    clock.set(datetime(R.year, R.month, R.day + 1, 0, 0, tzinfo=UTC))
    asyncio.run(tick(db, clock))
    pending_round = [r for r in _round_jobs(conn) if r["status"] == "pending"]
    assert pending_round == [], (
        f"M126 -> at this assertion: correct = 0, mutant = {len(pending_round)}"
    )
    calls_after = standin.calls.get("synthetic-round-x", 0)
    assert calls_after == 2, (
        f"M126 -> at this assertion: correct = 2, mutant = {calls_after}"
    )
    rows_per_try = len(_downloads(conn, feed_ids["X"]))
    assert rows_per_try == 2, (
        f"M126 -> at this assertion: correct = 2 (one row per round_try),"
        f" mutant = {rows_per_try}"
    )


def test_rs11_a_reclaimed_round_try_is_cancelled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS11 (i): X's try 1 running when a wrapper raises Stop; the add-on
    restarts after 00:40Z, reclaim_all_stale puts the job back to
    pending. (ii): X's try 1 writes stored, then Stop before the job is
    marked done; the add-on restarts before 00:40Z and the job is
    reclaimed."""
    db, conn, _site_id, feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=("X",)
    )
    standin.program("synthetic-round-x", _reply("X", "synthetic-round-x", 0))

    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 10, tzinfo=UTC)))
    job = claim_next_job(conn)
    assert job is not None
    conn.execute("UPDATE jobs SET status='running' WHERE id=?", (job.id,))
    conn.commit()

    clock.set(datetime(R.year, R.month, R.day, 0, 41, tzinfo=UTC))
    reclaim_all_stale(conn)
    conn.commit()
    reclaimed = conn.execute("SELECT * FROM jobs WHERE id=?", (job.id,)).fetchone()
    assert reclaimed["status"] == "pending"
    asyncio.run(run_claimed_job(db, job, lane="main"))
    conn.commit()
    row_after = conn.execute("SELECT * FROM jobs WHERE id=?", (job.id,)).fetchone()
    assert _is_cancelled(row_after), (
        f"M127 -> at this assertion: correct = cancelled, mutant ="
        f" status={row_after['status']!r} result={row_after['result']!r}"
    )
    call_count = standin.calls.get("synthetic-round-x", 0)
    assert call_count == 0, (
        f"M127 -> at this assertion: correct = 0, mutant = {call_count}"
    )
    assert _downloads(conn, feed_ids["X"]) == [], (
        "M127 -> at this assertion: correct = 0 rows, mutant = a row was written"
    )

    # (ii): X's try 1 writes `stored`, then a restart reclaims it before
    # the bound -- also cancelled, with no second row for try 1.
    db2, conn2, _site2, feed2_ids, standin2, clock2 = _night(
        tmp_path / "rs11ii", monkeypatch, subscribe=("X",)
    )
    standin2.program("synthetic-round-x", _reply("X", "synthetic-round-x", 0))
    asyncio.run(
        tick(db2, clock2, at=datetime(R.year, R.month, R.day, 0, 10, tzinfo=UTC))
    )
    job2 = claim_next_job(conn2)
    assert job2 is not None
    asyncio.run(run_claimed_job(db2, job2, lane="main"))
    conn2.commit()
    conn2.execute("UPDATE jobs SET status='running' WHERE id=?", (job2.id,))
    conn2.commit()

    clock2.set(datetime(R.year, R.month, R.day, 0, 20, tzinfo=UTC))
    reclaim_all_stale(conn2)
    conn2.commit()
    asyncio.run(run_claimed_job(db2, job2, lane="main"))
    conn2.commit()
    rows2 = _downloads(conn2, feed2_ids["X"])
    try_one_rows = [r for r in rows2 if r["round_try"] == 1]
    n_try_one = len(try_one_rows)
    assert n_try_one == 1, (
        f"M127 -> at this assertion: correct = 1, mutant = {n_try_one}"
    )


def test_rs12_round_without_a_record_uses_the_latest_records_zone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS12: close on, zone Etc/GMT-14, only X subscribed; D the site's
    first record (made during D-1, so R has none); worker_started_at
    before D's end; no tick during D."""
    from wxverify import config
    from wxverify.db.connection import close_db, init_db
    from wxverify.db.site_days import DAILY_CLOSE_ON_KEY, start_site_days

    close_db()
    db_path = tmp_path / "rs12.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001

    site_id = asof_make_site(conn, "rs12-site")
    conn.execute("UPDATE sites SET timezone='Etc/GMT-14' WHERE id=?", (site_id,))
    ensure_published_generation(conn, site_id)
    seed_round_schedule_feeds(conn, site_id, subscribe=("X",))

    d_minus_1 = D - timedelta(days=1)
    seed_now = datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 12, tzinfo=UTC)
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(seed_now))
    start_site_days(conn, site_id, D, now_utc=seed_now)
    set_runtime_state(conn, "worker_started_at", isoformat_utc(seed_now))
    conn.commit()

    d_record_before = conn.execute(
        "SELECT day_end_utc FROM site_days WHERE site_id=? AND local_date=?",
        (site_id, D.isoformat()),
    ).fetchone()
    assert d_record_before is not None
    no_r_yet = conn.execute(
        "SELECT 1 FROM site_days WHERE site_id=? AND local_date=?",
        (site_id, R.isoformat()),
    ).fetchone()
    assert no_r_yet is None, "R must have no record before (i)"

    # (i): UPDATE sites timezone to UTC, then round_state(R) called directly.
    conn.execute("UPDATE sites SET timezone='UTC' WHERE id=?", (site_id,))
    conn.commit()
    try:
        state = round_state(conn, site_id=site_id, round_date=R)
    except Exception as exc:  # pragma: no cover -- only a mutant raises here
        pytest.fail(
            "M135 -> at this assertion: correct = RoundState without raising,"
            f" mutant = raises {exc!r}"
        )
    expected_bound = parse_utc(d_record_before["day_end_utc"]) + timedelta(minutes=40)
    assert state.settled is False
    assert state.bound_utc == expected_bound, (
        f"M136 -> at this assertion: correct = {expected_bound.isoformat()},"
        f" mutant = {state.bound_utc.isoformat()}"
    )
    no_r_row = conn.execute(
        "SELECT 1 FROM site_days WHERE site_id=? AND local_date=?",
        (site_id, R.isoformat()),
    ).fetchone()
    assert no_r_row is None, (
        "M135 -> at this assertion: correct = no row (read-only stand-in),"
        " mutant = a site_days row was added"
    )


def test_rs13_failed_scopes_resolve_across_round_and_fetch_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS13 (i): close off, zone UTC: X's round tries raise ConnectError,
    then its 06:10 download stores; Y (1440-minute interval) has its four
    round tries raise ConnectError. (ii): a fresh database with seeded
    job rows. (iii): EXPLAIN QUERY PLAN of FAILED_SCOPES_SQL."""
    from wxverify import config
    from wxverify.db.connection import close_db, init_db
    from wxverify.db.migrations import run_migrations

    # (i)
    close_db()
    db_path = tmp_path / "rs13i.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001
    site_id = asof_make_site(conn, "rs13-site")
    ensure_published_generation(conn, site_id)
    feed_ids = seed_round_schedule_feeds(conn, site_id, subscribe=("X", "Y"))
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes=1440 WHERE id=?", (feed_ids["Y"],)
    )
    conn.commit()
    set_runtime_state(
        conn,
        "worker_started_at",
        isoformat_utc(datetime(D.year, D.month, D.day, 12, tzinfo=UTC)),
    )
    conn.commit()
    standin = install_provider_standin(monkeypatch)
    disable_other_enqueuers(monkeypatch)
    clock = MovingClock(datetime(D.year, D.month, D.day, 23, 0, tzinfo=UTC))
    patch_close_clock(monkeypatch, clock)
    standin.program(
        "synthetic-round-x",
        ProviderConnectError(),
        ProviderConnectError(),
        ProviderConnectError(),
        ProviderConnectError(),
        _reply("X", "synthetic-round-x", 6),
    )
    standin.program(
        "synthetic-round-y",
        ProviderConnectError(),
        ProviderConnectError(),
        ProviderConnectError(),
        ProviderConnectError(),
    )
    for minute in (10, 15, 22, 30):
        asyncio.run(
            tick(db, clock, at=datetime(R.year, R.month, R.day, 0, minute, tzinfo=UTC))
        )
        for _ in range(2):
            job = claim_next_job(conn)
            if job is None:
                break
            with pytest.raises(Exception):  # noqa: B017, PT011
                asyncio.run(run_claimed_job(db, job, lane="main"))
            conn.commit()
    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 6, 10, tzinfo=UTC)))
    job = claim_next_job(conn)
    if job is not None:
        asyncio.run(run_claimed_job(db, job, lane="main"))
        conn.commit()

    clock.set(datetime(R.year, R.month, R.day + 2, 6, 0, tzinfo=UTC))
    count = failed_scope_count(conn, now_utc=clock.now())
    assert count == 1, (
        f"M137/M138/M139 -> at this assertion: correct = 1, mutant = {count}"
    )

    # (ii)
    close_db()
    db_path2 = tmp_path / "rs13ii.db"
    config.db_path = str(db_path2)
    db2 = init_db(str(db_path2))
    conn2 = db2._conn  # noqa: SLF001
    site2 = asof_make_site(conn2, "rs13-site-2")
    ensure_published_generation(conn2, site2)
    feed2_ids = seed_round_schedule_feeds(conn2, site2, subscribe=("Z", "G"))
    now2 = datetime(2026, 1, 10, 12, tzinfo=UTC)
    stamp = isoformat_utc(now2 - timedelta(hours=49))
    rows = [
        ("fetch_feed", f"round:{feed2_ids['G']}", "failed", None),
        ("fetch_feed", f"fetch:{feed2_ids['G']}", "failed", None),
        ("fetch_feed", f"fetch:{feed2_ids['Z']}", "failed", None),
        ("fetch_feed", f"round:{feed2_ids['Z']}", "completed", "ok"),
    ]
    for job_type, job_key, status, result in rows:
        conn2.execute(
            "INSERT INTO jobs (type, site_id, job_key, payload, status, result,"
            " created_at, updated_at) VALUES (?, ?, ?, '{}', ?, ?, ?, ?)",
            (job_type, site2, job_key, status, result, stamp, stamp),
        )
    conn2.commit()
    count2 = failed_scope_count(conn2, now_utc=now2)
    assert count2 == 1, (
        f"M137/M138/M139 -> at this assertion: correct = 1, mutant = {count2}"
    )

    # (iii)
    plan_conn = sqlite3.connect(":memory:", cached_statements=0)
    plan_conn.row_factory = sqlite3.Row
    run_migrations(plan_conn)
    cutoff = isoformat_utc(
        datetime(2026, 7, 7, 12, tzinfo=UTC) - timedelta(hours=FAILED_JOB_AGE_HOURS)
    )
    plan = [
        str(row["detail"])
        for row in plan_conn.execute(
            "EXPLAIN QUERY PLAN " + FAILED_SCOPES_SQL, (cutoff,)
        )
    ]
    has_r_search = any(
        "SEARCH r" in line and "idx_jobs_type_key_site" in line and "site_id=?" in line
        for line in plan
    )
    assert has_r_search, (
        "M137 -> at this assertion: correct = True (a 'SEARCH r ..."
        f" idx_jobs_type_key_site ... site_id=?' line exists), mutant = {plan!r}"
    )
    has_k_search = any(
        "SEARCH k" in line and "idx_jobs_type_key_site" in line and "site_id=?" in line
        for line in plan
    )
    assert has_k_search, (
        "O11 (unchanged) -> at this assertion: correct = True (a 'SEARCH k"
        f" ...' line too), mutant = {plan!r}"
    )


# ---------------------------------------------------------------------------
# §18.3 Daytime downloads
# ---------------------------------------------------------------------------


def test_dt1_fixed_daytime_slots_with_no_creep(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DT1: X (360) and G (720) through one day, every download
    succeeding, each taking 4 minutes; no creep: the next day's are the
    same."""
    db, conn, _site_id, feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=("X", "G")
    )
    hours_x = (0, 6, 12, 18)
    hours_g = (0, 12)
    for hour in hours_x:
        standin.program("synthetic-round-x", _reply("X", "synthetic-round-x", hour))
    for hour in hours_g:
        standin.program("synthetic-round-g", _reply("G", "synthetic-round-g", hour))
    for day_offset in (1, 2):  # R and R+1: "the next day's are the same"
        day = R + timedelta(days=day_offset - 1)
        for hour, minute in ((0, 10), (6, 10), (12, 10), (18, 10)):
            asyncio.run(
                tick(
                    db,
                    clock,
                    at=datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC),
                )
            )
            for job in iter(lambda: claim_next_job(conn), None):
                asyncio.run(run_claimed_job(db, job, lane="main"))
                conn.commit()

    calls_x = standin.calls.get("synthetic-round-x", 0)
    calls_g = standin.calls.get("synthetic-round-g", 0)
    assert calls_x == 8, (
        f"M13/M14 -> at this assertion: correct = 8, mutant = {calls_x}"
    )
    assert calls_g == 4, (
        f"M13/M14 -> at this assertion: correct = 4, mutant = {calls_g}"
    )


def test_dt2_an_outage_fetches_the_latest_missed_slot_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DT2: the add-on stopped from 05:00Z to 13:00Z."""
    db, conn, _site_id, feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=("X",)
    )
    standin.program("synthetic-round-x", _reply("X", "synthetic-round-x", 0))
    standin.program("synthetic-round-x", _reply("X", "synthetic-round-x", 12))

    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 10, tzinfo=UTC)))
    job = claim_next_job(conn)
    assert job is not None
    asyncio.run(run_claimed_job(db, job, lane="main"))
    conn.commit()

    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 13, 0, tzinfo=UTC)))
    jobs_after = _job_for_key(conn, f"fetch:{feed_ids['X']}")
    slots = sorted(json.loads(r["payload"])["slot"] for r in jobs_after)
    assert slots == ["12:10"], (
        f"M15 -> at this assertion: correct = ['12:10'], mutant = {slots!r}"
    )


def test_dt3_a_feed_with_a_1440_minute_interval_has_the_round_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DT3: a feed with interval 1440."""
    db, conn, _site_id, feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=("Y",)
    )
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes=1440 WHERE id=?", (feed_ids["Y"],)
    )
    conn.commit()
    standin.program("synthetic-round-y", _reply("Y", "synthetic-round-y", 0))

    for hour, minute in ((0, 10), (6, 10), (12, 10), (18, 10)):
        asyncio.run(
            tick(
                db, clock, at=datetime(R.year, R.month, R.day, hour, minute, tzinfo=UTC)
            )
        )
        for job in iter(lambda: claim_next_job(conn), None):
            asyncio.run(run_claimed_job(db, job, lane="main"))
            conn.commit()

    daytime_jobs = _job_for_key(conn, f"fetch:{feed_ids['Y']}")
    assert daytime_jobs == [], (
        f"M14 -> at this assertion: correct = 0, mutant = {len(daytime_jobs)}"
    )


def test_dt4_an_unreadable_interval_skips_the_feed_not_the_others(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """DT4: a feed whose fetch_interval_minutes is 0, then 'abc'."""
    db, conn, _site_id, feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=("X", "Y")
    )
    standin.program("synthetic-round-x", _reply("X", "synthetic-round-x", 0))
    standin.program("synthetic-round-x", _reply("X", "synthetic-round-x", 6))

    for bad in (0, "abc"):
        conn.execute(
            "UPDATE feeds SET fetch_interval_minutes=? WHERE id=?",
            (bad, feed_ids["Y"]),
        )
        conn.commit()
        with caplog.at_level("WARNING"):
            asyncio.run(
                tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 10, tzinfo=UTC))
            )
        job_x = claim_next_job(conn)
        assert job_x is not None, (
            "M16 -> at this assertion: correct = X's round try still runs,"
            " mutant = no job (the bad interval on Y broke the whole tick)"
        )
        asyncio.run(run_claimed_job(db, job_x, lane="main"))
        conn.commit()
        bad_jobs = _job_for_key(conn, f"fetch:{feed_ids['Y']}") + _job_for_key(
            conn, f"round:{feed_ids['Y']}"
        )
        assert bad_jobs == [], (
            f"M16 -> at this assertion: correct = 0 (Y never gets a job),"
            f" mutant = {len(bad_jobs)}"
        )
        clock.advance(timedelta(hours=1))
