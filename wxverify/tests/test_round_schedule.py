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
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from tests.helpers import (
    STANDARD_NIGHT_R,
    D,
    MovingClock,
    ProviderConnectError,
    ProviderEmpty,
    ProviderHttpError,
    ProviderOutcome,
    ProviderReply,
    asof_make_site,
    disable_other_enqueuers,
    failed_scope_count,
    install_provider_standin,
    patch_close_clock,
    round_schedule_sample,
    seed_round_schedule_feeds,
    standard_night,
    subscribe_meteoblue_package,
    tick,
)
from wxverify.core.timeutil import isoformat_utc, local_day_slots, parse_utc
from wxverify.db.queue import claim_next_job, reclaim_all_stale
from wxverify.db.runtime_state import set_runtime_state
from wxverify.db.switch import ensure_site_generation
from wxverify.monitor import FAILED_JOB_AGE_HOURS, FAILED_SCOPES_SQL
from wxverify.worker.processor import run_claimed_job
from wxverify.worker.round import (  # P4 piece 3 (plan §4.4, §4.5, §5.1, §16.1)
    round_state,
    schedule_day,
)

R = STANDARD_NIGHT_R  # 2024-06-11 (plan §15.1's standard night: R = D + 1)


# ---------------------------------------------------------------------------
# Local helpers
# ---------------------------------------------------------------------------


def _exclude_seeded_feeds_from_the_round(conn: sqlite3.Connection) -> None:
    """Take every seeded real feed besides X, Y, Z, G and the meteoblue
    package out of the round/daytime schedule. The open-meteo feeds
    ``config.FEED_SEEDS`` installs at boot are ``default_subscribed=1``
    (plan §18.1's test feeds are X, Y, Z, G and P only), and
    ``_schedule_feeds`` (``worker/round.py``) treats a real feed as
    subscribed via ``COALESCE(sfs.enabled, f.default_subscribed) = 1``
    -- with no ``site_feed_state`` row, every seeded open-meteo feed
    would join every round and every daytime tick alongside the test's
    own feeds. A plain UPDATE on the global ``feeds`` row (plan §18.1's
    "seed helpers by plain INSERT [or UPDATE]"), not a per-site
    ``site_feed_state`` row, since ``default_subscribed`` is the column
    ``_schedule_feeds`` falls back to. meteoblue is already
    ``default_subscribed=0`` (``subscribe_meteoblue_package`` opts it in
    explicitly) and the seeded virtual yardsticks are excluded by
    ``_schedule_feeds``'s own ``is_virtual = 0`` clause, so neither needs
    touching here.
    """
    conn.execute("UPDATE feeds SET default_subscribed=0 WHERE source='open-meteo'")


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
    obs) while keeping the round and daytime enqueuers live, since this
    file's tests exercise those two directly; the other seeded feeds are
    excluded from the round/daytime schedule too (see
    ``_exclude_seeded_feeds_from_the_round``), so only the row's own
    feeds (plus any package a test subscribes with
    ``subscribe_meteoblue_package``) ever appear in it. Returns ``(db,
    conn, site_id, feed_ids, standin, clock)``.
    """
    db, _writer, site_id, _stations = standard_night(tmp_path)
    conn = db._conn  # noqa: SLF001 -- tests read the real writer conn directly
    feed_ids = seed_round_schedule_feeds(conn, site_id, subscribe=subscribe)
    _exclude_seeded_feeds_from_the_round(conn)
    conn.commit()
    standin = install_provider_standin(monkeypatch)
    disable_other_enqueuers(
        monkeypatch,
        keep=("enqueue_due_round_tries", "enqueue_due_daytime_downloads"),
    )
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


def _label(model: str, hour: int, *, day: date = R) -> str:
    """A synthetic ``issued_at``/run-label stamp, distinct per hour so two
    different calls never collide by accident. ``day`` lets a caller
    issue the reply on a day other than ``R`` (DT1's second day), so a
    6-hour lead from it is not already past."""
    return f"{day.isoformat()}T{hour:02d}:00:00Z[{model}]"


def _reply(feed_id_name: str, model: str, hour: int, *, day: date = R) -> ProviderReply:
    """One usable sample at lead 6h for ``model`` (plan §18.1's "a reply
    with given run labels and values")."""
    issued_at = _label(model, hour, day=day)
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
    # RS3 (i): after try 4 has ended (whether it stored or failed), the
    # round is settled (4.5) -- try 4 is this feed's last try regardless
    # of its outcome.
    state_after_try4 = round_state(conn, site_id=site_id, round_date=R)
    assert state_after_try4.settled is True, (
        f"RS3(i): correct = True (after try 4), got {state_after_try4.settled}"
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
    from wxverify.settings.keys import get_setting
    from wxverify.worker.round import set_scoring_round_local_time_sync

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

    site_id = day_close_site(conn, timezone="Etc/GMT-14", with_records=True)
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
    """RS8 (amended) (a): round tries 1 to 3 for P (meteoblue-style) all
    end without a stored row, so a fourth try is still due. Ticking for
    real through the day's slot times shows P's daytime slots that day
    are 06:10 only (12:10 and 18:10 dropped, latest first).
    (b): the same tries 1 to 3 on a fresh night, then, with the cap's
    calls used up, try 4 ends ``budget`` with no call, and 06:10 is
    dropped too (every recorded retry counts, 4.4)."""
    from wxverify.collection.budget import set_source_cap

    # (a)
    db, conn, site_id, _feed_ids, standin, clock = _night(
        tmp_path / "rs8a", monkeypatch, subscribe=()
    )
    package_id = subscribe_meteoblue_package(conn, site_id)
    conn.commit()
    standin.program(
        "meteoblue",
        ProviderHttpError(404),  # try 1: failed
        ProviderConnectError(),  # try 2: failed (a retry; 1 slot dropped)
        ProviderConnectError(),  # try 3: failed (a retry; 2 slots dropped)
    )

    for minute in (10, 15, 22):
        asyncio.run(
            tick(db, clock, at=datetime(R.year, R.month, R.day, 0, minute, tzinfo=UTC))
        )
        job = claim_next_job(conn)
        assert job is not None, f"no round try pending at minute {minute}"
        asyncio.run(run_claimed_job(db, job, lane="main"))
        conn.commit()

    round_rows = _downloads(conn, package_id)
    assert round_rows[-1]["outcome"] != "stored", (
        "fixture sanity: try 3 must not store, so a fourth try is still due"
    )

    # Tick for real through the day's three slot times, claiming and
    # running any job each tick creates.
    for hour, minute in ((6, 10), (12, 10), (18, 10)):
        asyncio.run(
            tick(
                db, clock, at=datetime(R.year, R.month, R.day, hour, minute, tzinfo=UTC)
            )
        )
        while (job := claim_next_job(conn)) is not None:
            asyncio.run(run_claimed_job(db, job, lane="main"))
            conn.commit()

    slots = sorted(
        json.loads(r["payload"])["slot"]
        for r in _job_for_key(conn, f"fetch:{package_id}")
    )
    assert slots == ["06:10"], (
        "M10/M11 -> at this assertion: correct = ['06:10'] (12:10/18:10"
        f" dropped), mutant = {slots!r}"
    )

    # (b): the same tries 1-3, on a fresh night, then the cap's calls
    # used up.
    db2, conn2, site2_id, _feed2_ids, standin2, clock2 = _night(
        tmp_path / "rs8b", monkeypatch, subscribe=()
    )
    package2_id = subscribe_meteoblue_package(conn2, site2_id)
    conn2.commit()
    standin2.program(
        "meteoblue",
        ProviderHttpError(404),  # try 1: failed
        ProviderConnectError(),  # try 2: failed (a retry; 1 slot dropped)
        ProviderConnectError(),  # try 3: failed (a retry; 2 slots dropped)
    )

    for minute in (10, 15, 22):
        asyncio.run(
            tick(
                db2, clock2, at=datetime(R.year, R.month, R.day, 0, minute, tzinfo=UTC)
            )
        )
        job = claim_next_job(conn2)
        assert job is not None, f"no round try pending at minute {minute}"
        asyncio.run(run_claimed_job(db2, job, lane="main"))
        conn2.commit()

    round_rows2 = _downloads(conn2, package2_id)
    assert round_rows2[-1]["outcome"] != "stored", (
        "fixture sanity: try 3 must not store, so a fourth try is still due"
    )

    set_source_cap(conn2, "meteoblue", daily_call_limit=0)
    conn2.commit()
    calls_before_try4 = standin2.calls.get("meteoblue", 0)

    # Try 4 is due at the round's own offset, 00:30Z, still inside the
    # round's 00:40Z bound -- the only instant it can ever run.
    asyncio.run(
        tick(db2, clock2, at=datetime(R.year, R.month, R.day, 0, 30, tzinfo=UTC))
    )
    job = claim_next_job(conn2)
    assert job is not None, "try 4 was not enqueued at 00:30Z"
    asyncio.run(run_claimed_job(db2, job, lane="main"))
    conn2.commit()

    round_rows2 = _downloads(conn2, package2_id)
    outcome = round_rows2[-1]["outcome"]
    assert outcome == "budget", (
        f"M10/M11 -> at this assertion: correct = 'budget', mutant = {outcome!r}"
    )
    calls_after_try4 = standin2.calls.get("meteoblue", 0)
    assert calls_after_try4 == calls_before_try4, (
        "M10/M11 -> at this assertion: correct = no call for try 4 (budget),"
        f" mutant = {calls_after_try4 - calls_before_try4} more call(s)"
    )

    # With every try (1 to 4) now recorded, the day's real slot ticks must
    # drop 06:10 too -- no job is ever created for it.
    for hour, minute in ((6, 10), (12, 10), (18, 10)):
        asyncio.run(
            tick(
                db2,
                clock2,
                at=datetime(R.year, R.month, R.day, hour, minute, tzinfo=UTC),
            )
        )
        job = claim_next_job(conn2)
        if job is not None:
            asyncio.run(run_claimed_job(db2, job, lane="main"))
            conn2.commit()

    slots_after_try4 = sorted(
        json.loads(r["payload"])["slot"]
        for r in _job_for_key(conn2, f"fetch:{package2_id}")
    )
    assert slots_after_try4 == [], (
        "M10/M11 -> at this assertion: correct = [] (06:10 dropped too),"
        f" mutant = {slots_after_try4!r}"
    )
    calls_after_daytime = standin2.calls.get("meteoblue", 0)
    assert calls_after_daytime == calls_after_try4, (
        "M10/M11 -> at this assertion: correct = no further adapter call"
        f" (06:10 dropped), mutant ="
        f" {calls_after_daytime - calls_after_try4} more call(s)"
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

    # X's try 2 is due at the start plus 5 minutes (00:15Z), still inside
    # the round's 00:40Z bound -- run it there, not after jumping straight
    # to 02:00Z, which would land past the bound and get it cancelled
    # instead of stored (cause 5).
    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 15, tzinfo=UTC)))
    job = claim_next_job(conn)
    assert job is not None, "X's try 2 was not enqueued at 00:15Z"
    asyncio.run(run_claimed_job(db, job, lane="main"))
    conn.commit()
    x_rows = _downloads(conn, feed_ids["X"])
    assert x_rows[-1]["outcome"] == "stored", (
        f"M126 -> at this assertion: correct = 'stored', mutant ="
        f" {x_rows[-1]['outcome']!r}"
    )

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
    # Run the job the way a restart's own claim loop would -- claim the
    # now-pending row again rather than reusing the stale `job` object
    # from before the reclaim, whose claim run_claimed_job's
    # `_still_claimed` check no longer finds 'running' (cause 6): running
    # it unclaimed would silently drop the outcome and leave the row
    # 'pending' instead of cancelling it.
    reclaimed_job = claim_next_job(conn)
    assert reclaimed_job is not None
    assert reclaimed_job.id == job.id
    asyncio.run(run_claimed_job(db, reclaimed_job, lane="main"))
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
    # Re-claim before re-running, as (i) does (cause 6): job2's claim is
    # stale once reclaim_all_stale re-pends the row.
    reclaimed_job2 = claim_next_job(conn2)
    assert reclaimed_job2 is not None
    assert reclaimed_job2.id == job2.id
    asyncio.run(run_claimed_job(db2, reclaimed_job2, lane="main"))
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
    ensure_site_generation(conn, site_id)
    seed_round_schedule_feeds(conn, site_id, subscribe=("X",))

    d_minus_1 = D - timedelta(days=1)
    # 00:00Z on D-1 is D-1 14:00 local in Etc/GMT-14 (UTC+14) -- still
    # D-1's local date. 12:00Z would be D 02:00 local (cause 7): local
    # day D-1 runs from D-2 10:00Z to D-1 10:00Z, so anything at or past
    # D-1 10:00Z is already D's local day.
    seed_now = datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 0, tzinfo=UTC)
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

    # M136 targeted: round_state's bound_utc alone does not discriminate
    # the zone the stand-in is built in here, because _round_start_utc
    # reads day.timezone (never touched by the swap to sites.timezone),
    # and the round's configured local time still falls inside the wider,
    # UTC-zone stand-in bounds the mutant produces. Read schedule_day's own
    # bounds directly: its day_end_utc must come from L's stored zone
    # (Etc/GMT-14), not from sites.timezone (now UTC).
    stand_in = schedule_day(conn, site_id=site_id, local_date=R)
    expected_day_end = local_day_slots(R, "Etc/GMT-14")[1]
    assert stand_in.day_end_utc == expected_day_end, (
        f"M136 -> at this assertion: correct = {expected_day_end.isoformat()},"
        f" mutant = {stand_in.day_end_utc.isoformat()}"
    )


def test_rs13_failed_scopes_resolve_across_round_and_fetch_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RS13 (i): close on, zone UTC: X's round tries raise ConnectError,
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
    ensure_site_generation(conn, site_id)
    feed_ids = seed_round_schedule_feeds(conn, site_id, subscribe=("X", "Y"))
    conn.execute(
        "UPDATE feeds SET fetch_interval_minutes=1440 WHERE id=?", (feed_ids["Y"],)
    )
    _exclude_seeded_feeds_from_the_round(conn)
    conn.commit()
    set_runtime_state(
        conn,
        "worker_started_at",
        isoformat_utc(datetime(D.year, D.month, D.day, 12, tzinfo=UTC)),
    )
    from wxverify.db.site_days import DAILY_CLOSE_ON_KEY  # noqa: PLC0415

    set_runtime_state(
        conn,
        DAILY_CLOSE_ON_KEY,
        isoformat_utc(datetime(D.year, D.month, D.day, 12, tzinfo=UTC)),
    )
    conn.commit()
    standin = install_provider_standin(monkeypatch)
    disable_other_enqueuers(
        monkeypatch,
        keep=("enqueue_due_round_tries", "enqueue_due_daytime_downloads"),
    )
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
            # A round try's ConnectError is caught inside run_claimed_job
            # and recorded as a failed job (4.4's "no_usable, failed and
            # backoff count as failed tries") -- it does not propagate
            # (cause 8).
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
    ensure_site_generation(conn2, site2)
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


def test_rs14_a_changeover_day_with_no_round_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """RS14 (i): close on, zone UTC, only X subscribed; D the site's first
    record, made by P1's helper during D-1; worker_started_at before D's
    end; at D 12:00Z ``enter_zone_change`` to Etc/GMT-14, so R is a
    changeover day with no round slot (D's local 00:10, lead to R's
    10:00Z bound, falls outside R's own short record)."""
    from wxverify import config
    from wxverify.db.connection import close_db, init_db
    from wxverify.db.site_days import (
        DAILY_CLOSE_ON_KEY,
        enter_zone_change,
        start_site_days,
    )
    from wxverify.worker.round import enqueue_due_round_tries, round_try_stale

    close_db()
    db_path = tmp_path / "rs14.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001

    site_id = asof_make_site(conn, "rs14-site")
    ensure_site_generation(conn, site_id)
    feed_ids = seed_round_schedule_feeds(conn, site_id, subscribe=("X",))
    _exclude_seeded_feeds_from_the_round(conn)

    d_minus_1 = D - timedelta(days=1)
    seed_now = datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 0, tzinfo=UTC)
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(seed_now))
    start_site_days(conn, site_id, D, now_utc=seed_now)
    set_runtime_state(
        conn,
        "worker_started_at",
        isoformat_utc(datetime(D.year, D.month, D.day, 0, tzinfo=UTC)),
    )
    conn.commit()

    change_at = datetime(D.year, D.month, D.day, 12, tzinfo=UTC)
    changeover_date = enter_zone_change(conn, site_id, "Etc/GMT-14", now_utc=change_at)
    conn.commit()
    assert changeover_date == R, "R must be the changeover day"

    try:
        state = round_state(conn, site_id=site_id, round_date=R)
    except Exception as exc:  # pragma: no cover -- only a mutant raises here
        pytest.fail(
            "M145 -> at this assertion: correct = RoundState without raising,"
            f" mutant = raises {exc!r}"
        )
    expected_bound = datetime(R.year, R.month, R.day, 0, tzinfo=UTC)
    assert state.settled is True, (
        f"M145 -> at this assertion: correct = True, mutant = {state.settled}"
    )
    assert state.bound_utc == expected_bound, (
        f"M146 -> at this assertion: correct = {expected_bound.isoformat()},"
        f" mutant = {state.bound_utc.isoformat()}"
    )

    stale = round_try_stale(
        conn,
        site_id=site_id,
        feed_id=feed_ids["X"],
        round_date=R,
        now_utc=expected_bound,
    )
    assert stale is True, f"M145 -> at this assertion: correct = True, mutant = {stale}"

    with caplog.at_level("WARNING"):
        enqueue_due_round_tries(conn, now_utc=expected_bound)
        conn.commit()
        enqueue_due_round_tries(conn, now_utc=expected_bound + timedelta(minutes=10))
        conn.commit()
    round_jobs = _job_for_key(conn, f"round:{feed_ids['X']}")
    assert round_jobs == [], f"correct = 0 round: jobs, got = {len(round_jobs)}"
    assert caplog.records == [], (
        "M145 -> at this assertion: correct = no WARNING record, mutant ="
        f" {[r.getMessage() for r in caplog.records]!r}"
    )


def test_rs14_ii_d_attempt_starts_on_the_r_0000z_tick(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """RS14 (ii): a fresh night with the same setup as (i) -- close on,
    zone UTC, only X subscribed; D the site's first record, made during
    D-1; ``worker_started_at`` before D's end; at D 12:00Z
    ``enter_zone_change`` to ``Etc/GMT-14``, making R a changeover day
    with no round slot. With ``disable_other_enqueuers``, real ticks at D
    23:59Z and R 00:00Z: neither tick logs an ERROR line for the site,
    and at the R 00:00Z tick D's attempt 1 starts and D moves to
    ``closing`` (``_close_due_days``, D's own round being R's, 4.4, 4.5;
    ``worker/close_chain.py:1946-1983``)."""
    from wxverify import config
    from wxverify.db.connection import close_db, init_db
    from wxverify.db.site_days import (
        DAILY_CLOSE_ON_KEY,
        enter_zone_change,
        start_site_days,
    )

    close_db()
    db_path = tmp_path / "rs14ii.db"
    config.db_path = str(db_path)
    db = init_db(str(db_path))
    conn = db._conn  # noqa: SLF001

    site_id = asof_make_site(conn, "rs14ii-site")
    ensure_site_generation(conn, site_id)
    seed_round_schedule_feeds(conn, site_id, subscribe=("X",))
    _exclude_seeded_feeds_from_the_round(conn)

    d_minus_1 = D - timedelta(days=1)
    seed_now = datetime(d_minus_1.year, d_minus_1.month, d_minus_1.day, 0, tzinfo=UTC)
    set_runtime_state(conn, DAILY_CLOSE_ON_KEY, isoformat_utc(seed_now))
    start_site_days(conn, site_id, D, now_utc=seed_now)
    set_runtime_state(
        conn,
        "worker_started_at",
        isoformat_utc(datetime(D.year, D.month, D.day, 0, tzinfo=UTC)),
    )
    conn.commit()

    change_at = datetime(D.year, D.month, D.day, 12, tzinfo=UTC)
    changeover_date = enter_zone_change(conn, site_id, "Etc/GMT-14", now_utc=change_at)
    conn.commit()
    assert changeover_date == R, "R must be the changeover day"

    disable_other_enqueuers(monkeypatch)
    clock = MovingClock(datetime(D.year, D.month, D.day, 23, 59, tzinfo=UTC))
    patch_close_clock(monkeypatch, clock)

    caplog.set_level("ERROR", logger="wxverify.worker.close_chain")
    asyncio.run(
        tick(db, clock, at=datetime(D.year, D.month, D.day, 23, 59, tzinfo=UTC))
    )
    errors_at_d_tick = [r for r in caplog.records if r.levelname == "ERROR"]
    assert errors_at_d_tick == [], (
        "M145/M146 -> at this assertion: correct = no ERROR line (D has"
        " not ended yet, so nothing is due), mutant ="
        f" {[r.getMessage() for r in errors_at_d_tick]!r}"
    )

    row_before = conn.execute(
        "SELECT state FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, D.isoformat()),
    ).fetchone()
    assert row_before is not None
    assert row_before["state"] == "open", "D must still be open before R 00:00Z"

    caplog.clear()
    asyncio.run(tick(db, clock, at=datetime(R.year, R.month, R.day, 0, 0, tzinfo=UTC)))
    errors_at_r_tick = [r for r in caplog.records if r.levelname == "ERROR"]
    assert errors_at_r_tick == [], (
        "M145/M146 -> at this assertion: correct = no ERROR line at the R"
        " 00:00Z tick, mutant (round_state(R) raises, or answers the"
        " wrong settled/bound_utc, so D's own round -- R's, 4.5's '_round'"
        f" -- fails the site's steps) = {[r.getMessage() for r in errors_at_r_tick]!r}"
    )

    row_after = conn.execute(
        "SELECT state FROM site_days WHERE site_id = ? AND local_date = ?",
        (site_id, D.isoformat()),
    ).fetchone()
    assert row_after is not None
    assert row_after["state"] == "closing", (
        "M145/M146 -> at this assertion: correct = 'closing' (D's attempt 1"
        " starts once round_state(R) answers settled True with bound_utc"
        " R's day_start_utc, so D's due instant is R 00:00Z), mutant ="
        f" {row_after['state']!r}"
    )

    attempt_row = conn.execute(
        "SELECT 1 FROM close_attempts WHERE site_id = ? AND local_date = ?"
        " AND attempt = 1",
        (site_id, D.isoformat()),
    ).fetchone()
    assert attempt_row is not None, (
        "M145/M146 -> at this assertion: correct = a close_attempts row for"
        " D's attempt 1 (_start_attempt ran), mutant = no such row (the"
        " site's steps raised instead and began nothing this tick)"
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

    # Each download takes 4 minutes of the test clock (the row's own
    # words), modelled by advancing ``clock`` inside the adapter call, so
    # ``started_at`` and ``ended_at`` differ and a schedule counted from
    # the last download's end (M13) would creep away from the fixed
    # slots. A tick's job loop resets the clock back to the tick's own
    # instant before each job it claims runs, so a second feed processed
    # in the same tick (X and G both round at 00:10, both at 12:10)
    # still starts at that instant rather than inheriting the first
    # feed's +4-minute drift.
    real_consume = standin._consume  # noqa: SLF001

    def _consume_with_delay(source: str) -> ProviderOutcome:
        outcome = real_consume(source)
        clock.advance(timedelta(minutes=4))
        return outcome

    monkeypatch.setattr(standin, "_consume", _consume_with_delay)

    for day_offset in (1, 2):  # R and R+1: "the next day's are the same"
        day = R + timedelta(days=day_offset - 1)
        # Each day's replies are issued ON that day, never on R, so a
        # reply programmed for R+1 is not already past when a 6-hour
        # lead is added to it (the found cause of M13/M14's spurious
        # failure).
        for hour in hours_x:
            standin.program(
                "synthetic-round-x",
                _reply("X", "synthetic-round-x", hour, day=day),
            )
        for hour in hours_g:
            standin.program(
                "synthetic-round-g",
                _reply("G", "synthetic-round-g", hour, day=day),
            )
        for hour, minute in ((0, 10), (6, 10), (12, 10), (18, 10)):
            at = datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)
            asyncio.run(tick(db, clock, at=at))
            for job in iter(lambda: claim_next_job(conn), None):
                clock.set(at)
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

    def _expected(
        day: date, hour: int, minute: int, slot: str
    ) -> tuple[str, str, str, str]:
        started = datetime(day.year, day.month, day.day, hour, minute, tzinfo=UTC)
        ended = started + timedelta(minutes=4)
        return (slot, isoformat_utc(started), isoformat_utc(ended), "stored")

    day2 = R + timedelta(days=1)
    expected_x = [
        _expected(R, 0, 10, "round"),
        _expected(R, 6, 10, "06:10"),
        _expected(R, 12, 10, "12:10"),
        _expected(R, 18, 10, "18:10"),
        _expected(day2, 0, 10, "round"),
        _expected(day2, 6, 10, "06:10"),
        _expected(day2, 12, 10, "12:10"),
        _expected(day2, 18, 10, "18:10"),
    ]
    actual_x = [
        (row["slot"], row["started_at"], row["ended_at"], row["outcome"])
        for row in _downloads(conn, feed_ids["X"])
    ]
    assert actual_x == expected_x, (
        f"M13/M14 -> at this assertion: correct = {expected_x!r}, mutant = {actual_x!r}"
    )

    expected_g = [
        _expected(R, 0, 10, "round"),
        _expected(R, 12, 10, "12:10"),
        _expected(day2, 0, 10, "round"),
        _expected(day2, 12, 10, "12:10"),
    ]
    actual_g = [
        (row["slot"], row["started_at"], row["ended_at"], row["outcome"])
        for row in _downloads(conn, feed_ids["G"])
    ]
    assert actual_g == expected_g, (
        f"M13/M14 -> at this assertion: correct = {expected_g!r}, mutant = {actual_g!r}"
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
    """DT4: a feed whose fetch_interval_minutes is 0, then 'abc' (cause 9:
    the second pass must not re-tick R 00:10 -- X's round already stored
    there in the first pass, so no second round try is ever due; it
    needs X's own next slot, 06:10, instead)."""
    db, conn, _site_id, feed_ids, standin, clock = _night(
        tmp_path, monkeypatch, subscribe=("X", "Y")
    )
    standin.program("synthetic-round-x", _reply("X", "synthetic-round-x", 0))
    standin.program("synthetic-round-x", _reply("X", "synthetic-round-x", 6))

    cases = (
        (0, datetime(R.year, R.month, R.day, 0, 10, tzinfo=UTC), "out-of-range"),
        (
            "abc",
            datetime(R.year, R.month, R.day, 6, 10, tzinfo=UTC),
            "unreadable",
        ),
    )
    for bad, at, warning_kind in cases:
        conn.execute(
            "UPDATE feeds SET fetch_interval_minutes=? WHERE id=?",
            (bad, feed_ids["Y"]),
        )
        conn.commit()
        with caplog.at_level("WARNING"):
            asyncio.run(tick(db, clock, at=at))
        assert any(
            "cadence:" in record.getMessage() and warning_kind in record.getMessage()
            for record in caplog.records
        ), (
            f"DT4: expected a 'cadence: {warning_kind} ...' warning for"
            f" fetch_interval_minutes={bad!r}"
        )
        caplog.clear()
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
