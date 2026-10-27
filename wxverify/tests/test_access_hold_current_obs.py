"""The current-obs lane's refused-access behaviour (plan 2026-10-27
refused-access-hold §13.3, CO1-CO5).

CO1 — a 401/403 writes the ``current_obs`` hold, defers the job to the
      hold's end, consumes (and does not refund) one budget call, and
      touches nothing else: ``station_poll_state``, ``domain_backoffs``,
      the other two kinds' hold rows, ``stations``.
CO2 — a hold already in force blocks the reservation itself: no call, no
      budget row at all (tested directly against ``_reserve_current_obs_call``
      on a bare autocommit connection, so an order-swap mutant's write is
      not hidden by transaction rollback — see the mutant note on the test).
CO3 — six stations, one shared ``current_obs`` key: only the first claim of
      a throttled round makes the call; the other five meet the hold the
      first one just wrote. Advancing past the hold's end repeats exactly
      once more, with ``since`` preserved from the first refusal.
CO4 — a 2xx reply clears ONLY the ``current_obs`` hold (a separately-held
      ``hourly`` kind is untouched) and resumes normal poll-state recording.
CO5 — the monitor's current-obs lateness view (``_obs_collection_delays``)
      must not count held stations as ``overdue``: their jobs are pending
      and working as designed, not stuck.

Isolation: CO2 uses a bare in-memory ``sqlite3.Connection`` (autocommit,
no ``Database``/no transaction wrapper) because its mutant only shows up
without one. Every other test uses a real tmp-path ``Database``
(``init_db``), run through ``run_claimed_job``/``_enqueue_and_claim`` exactly
as the worker does, with ``wxverify.worker.processor.fetch_current_observation``
monkeypatched to a canned response (never a mock of the system under test).

Time control: CO3/CO5's drain loop needs a controllable, advanceable clock
shared by three independently-imported names. A single mutable
``box = {"now": ...}`` is patched into ``wxverify.core.timeutil.utc_now``
(covers ``queue.py``'s claim ordering AND every bare ``isoformat_utc()`` call
process-wide, since that function's ``__globals__`` always resolves against
``core.timeutil``'s own module dict -- including ``scheduler.enqueue_due_current_obs``)
plus ``wxverify.worker.access_hold.utc_now`` (that module's own bare
``utc_now()`` import, used by both ``record_access_refused`` and
``check_access_hold``). CO1/CO2/CO4 need no clock control: CO1/CO2 compare
only self-consistent before/after state, and CO4 patches
``access_hold.utc_now`` directly (no loop, no shared box needed).
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from wxverify import config
from wxverify.db.connection import Database, close_db, init_db
from wxverify.db.migrations import create_schema, seed_default_sources
from wxverify.db.queue import claim_next_current_obs_job, enqueue_if_absent
from wxverify.monitor import _obs_collection_delays  # noqa: PLC2701
from wxverify.worker.access_hold import (
    AccessHeld,
    clear_access_refused,
    read_access_record,
    record_access_refused,
)
from wxverify.worker.current_obs_poller import _enqueue_and_claim  # noqa: PLC2701
from wxverify.worker.processor import (
    _reserve_current_obs_call,  # noqa: PLC2701
    run_claimed_job,
)

_WEATHER_COM_URL = httpx.URL("https://api.weather.com/v2/pws/observations/current")

# ---------------------------------------------------------------------------
# DB fixture helpers
# ---------------------------------------------------------------------------


def _make_conn():  # type: ignore[no-untyped-def]
    """Bare in-memory, autocommit (``isolation_level=None``) schema-current
    connection -- used only by CO2, which needs each statement to commit on
    its own rather than inside one rolled-back-on-exception transaction."""
    import sqlite3

    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    seed_default_sources(conn)
    return conn


def _init_tmp_db(tmp_path: Path) -> Database:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps({"weathercom_key": "ci-placeholder"}), encoding="utf-8"
    )
    config.options_path = str(options_path)
    return init_db(str(db_path))


def _seed_site(conn, *, name: str = "SITE-A") -> int:  # type: ignore[no-untyped-def]
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 40.0, -105.0, 900.0, 'UTC')",
        (name,),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_station(conn, site_id: int, *, pws_id: str, enabled: int = 1) -> int:  # type: ignore[no-untyped-def]
    conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
        " VALUES (?, ?, 40.0, -105.0, 900.0, ?)",
        (site_id, pws_id, enabled),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_poll_state(
    conn,  # type: ignore[no-untyped-def]
    station_id: int,
    *,
    health_state: str = "online",
    next_poll_at: str,
    last_error: str | None = None,
    error_count: int = 0,
) -> None:
    conn.execute(
        "INSERT INTO station_poll_state"
        " (station_id, health_state, next_poll_at, cadence_events,"
        "  last_error, error_count, updated_at)"
        " VALUES (?, ?, ?, '[]', ?, ?, ?)",
        (station_id, health_state, next_poll_at, last_error, error_count, next_poll_at),
    )


def _seed_current_obs_job(
    conn,
    site_id: int,
    station_id: int,
    *,
    key: str | None = None,  # type: ignore[no-untyped-def]
) -> None:
    enqueue_if_absent(
        conn,
        "fetch_current_obs",
        site_id,
        key or f"curobs:{station_id}",
        {"station_id": station_id},
    )


def _poll_state_row(conn, station_id: int):  # type: ignore[no-untyped-def]
    return conn.execute(
        "SELECT * FROM station_poll_state WHERE station_id = ?", (station_id,)
    ).fetchone()


def _current_obs_row(conn, station_id: int):  # type: ignore[no-untyped-def]
    return conn.execute(
        "SELECT * FROM station_current_obs WHERE station_id = ?", (station_id,)
    ).fetchone()


def _station_row(conn, station_id: int):  # type: ignore[no-untyped-def]
    return conn.execute("SELECT * FROM stations WHERE id = ?", (station_id,)).fetchone()


def _job_row(conn, job_id: int):  # type: ignore[no-untyped-def]
    return conn.execute(
        "SELECT status, next_attempt_at, retry_count FROM jobs WHERE id = ?", (job_id,)
    ).fetchone()


_PAST_ISO = "2026-07-10T11:59:00Z"  # due (before any "now" used below)


def _refusal_response(status: int) -> httpx.Response:
    return httpx.Response(
        status,
        content=b"",
        headers={},
        request=httpx.Request("GET", str(_WEATHER_COM_URL)),
    )


def _online_body(obs_time_utc: str = "2026-07-10T11:55:00Z") -> bytes:
    return json.dumps(
        {
            "observations": [
                {
                    "obsTimeUtc": obs_time_utc,
                    "humidity": 50.0,
                    "winddir": 180.0,
                    "uv": 1.0,
                    "neighborhood": "Test Quarter",
                    "metric": {
                        "temp": 20.0,
                        "dewpt": 10.0,
                        "windSpeed": 5.0,
                        "windGust": 8.0,
                        "pressure": 1012.0,
                        "precipRate": 0.0,
                        "precipTotal": 0.0,
                    },
                }
            ]
        }
    ).encode()


def _online_response() -> httpx.Response:
    return httpx.Response(
        200,
        content=_online_body(),
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", str(_WEATHER_COM_URL)),
    )


async def _drain_current_obs_lane_advancing(
    db: Database,
    box: dict[str, datetime],
    step: timedelta = timedelta(seconds=2),
) -> None:
    """Claim and run current-obs jobs until none is claimable, advancing
    ``box["now"]`` by ``step`` after every claimed-and-run job.

    Without this, a held try's own clock reading is identical to the
    refusal's -- CO3's "hold extended by held tries" mutant needs the
    held tries to land strictly after the refusal that wrote ``until``,
    so a mutant that re-extends the hold on a held try has somewhere to
    show it.

    Bounded at 100 claimed-and-run jobs (the real tests need at most about
    12): if a held job were ever completed instead of deferred, its due
    station would be re-enqueued on every pass and this loop would never
    run dry, hanging the test instead of failing it. Hitting the cap fails
    loudly instead.
    """
    max_jobs = 100
    for _ in range(max_jobs):
        claimed = await db.write(
            lambda conn: _enqueue_and_claim(conn, stamp_heartbeat=False)
        )
        if claimed is None:
            return
        await run_claimed_job(db, claimed.job, lane="current_obs")
        box["now"] = box["now"] + step
    pytest.fail(
        f"current_obs lane never ran dry after {max_jobs} claimed-and-run "
        "jobs -- held jobs are probably being completed instead of "
        "deferred, so the due station keeps getting re-enqueued"
    )


# ---------------------------------------------------------------------------
# CO1 — 401/403 writes the hold, touches nothing else
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("refusal_status", [401, 403])
def test_co1_refusal_writes_hold_and_touches_nothing_else(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, refusal_status: int
) -> None:
    """A 401/403 current-obs call: one call; the hold row as AH1; the job
    deferred to the hold's end with ``retry_count`` 0; one budget call
    reserved and NOT refunded; ``station_poll_state`` and ``stations``
    byte-identical; no ``domain_backoffs`` row; no ``hourly``/``five_minute``
    hold row.

    Mutant (today's path): ``_fetch_current_obs``'s 401/403 branch replaced
    by falling through to the ordinary TERMINAL persist path -- at the "hold
    row written" assertion below: correct = a non-None record, mutant =
    None (``persist_poll_result`` would have run instead, writing
    ``station_poll_state`` with ``health_state='terminal'`` and leaving no
    ``runtime_state`` row at all).

    Mutant (domain backoff): a spurious ``domain_backoffs`` write added to
    the 401/403 branch -- at the "no domain_backoffs row" assertion below:
    correct = 0, mutant = 1.

    Mutant (refund): the reservation refunded on 401/403 -- at the "one call
    reserved, not refunded" assertion below: correct = 1, mutant = 0.

    Mutant (every-kind refusal): ``record_access_refused`` called for every
    kind in ``ACCESS_KINDS`` instead of just ``current_obs`` -- at the
    "no hourly/five_minute hold row" assertion below: correct = None,
    mutant = a non-None record for that kind.
    """

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id, pws_id="ISTATION01")
        _seed_poll_state(
            conn,
            station_id,
            health_state="online",
            next_poll_at=_PAST_ISO,
            last_error=None,
            error_count=0,
        )
        before_poll_state = dict(_poll_state_row(conn, station_id))
        before_station = dict(_station_row(conn, station_id))

        _seed_current_obs_job(conn, site_id, station_id)

        calls = {"n": 0}

        async def _fake_fetch(
            pws_station_id: str,
            api_key: str,
            *,
            client: httpx.AsyncClient | None = None,
            timeout_seconds: float = 10.0,
        ) -> httpx.Response:
            calls["n"] += 1
            return _refusal_response(refusal_status)

        monkeypatch.setattr(
            "wxverify.worker.processor.fetch_current_observation", _fake_fetch
        )

        job = await db.write(claim_next_current_obs_job)
        assert job is not None
        await run_claimed_job(db, job, lane="current_obs")

        assert calls["n"] == 1, "exactly one provider call must be made"

        record = read_access_record(conn, "current_obs")
        assert record is not None, "the current_obs hold row must be written"
        assert record.status == refusal_status
        assert record.until is not None

        job_row = _job_row(conn, job.id)
        assert job_row is not None
        assert job_row["status"] == "pending"
        assert job_row["next_attempt_at"] == record.until
        assert job_row["retry_count"] == 0

        budget_row = conn.execute(
            "SELECT calls FROM api_budget WHERE source = 'weathercom'"
        ).fetchone()
        assert budget_row is not None and budget_row["calls"] == 1, (
            "one call must be reserved and not refunded"
        )

        assert conn.execute("SELECT COUNT(*) FROM domain_backoffs").fetchone()[0] == 0

        assert read_access_record(conn, "hourly") is None
        assert read_access_record(conn, "five_minute") is None

        assert dict(_poll_state_row(conn, station_id)) == before_poll_state
        assert dict(_station_row(conn, station_id)) == before_station

    asyncio.run(run())


# ---------------------------------------------------------------------------
# CO2 — a hold in force blocks reservation entirely; no budget consumed
# ---------------------------------------------------------------------------


def test_co2_hold_in_force_blocks_reservation_before_any_budget_write() -> None:
    """``_reserve_current_obs_call`` raises ``AccessHeld`` with no budget row
    created at all, while the hold is in force.

    Uses a bare autocommit connection (no transaction wrapper) deliberately:
    in the real job pipeline, a reorder mutant's budget write would be
    rolled back along with everything else in the same ``BEGIN IMMEDIATE``
    transaction when ``check_access_hold`` still raises afterward, making
    the two mutants below equivalent under that oracle. On this connection
    each statement commits as it runs, so the order genuinely matters.

    Mutant (order swap): ``check_domain_backoff``/``check_access_hold``
    moved to AFTER ``reserve_budget`` -- at the "no budget row" assertion
    below: correct = 0, mutant = 1 (``reserve_budget``'s ``UPDATE`` commits
    immediately, before the hold check raises).

    Mutant (no check): the ``check_access_hold`` call removed from
    ``_reserve_current_obs_call`` -- at the ``pytest.raises(AccessHeld)``
    block below: correct = raises, mutant = returns a ``Reservation``
    (no raise; the call proceeds).
    """
    conn = _make_conn()
    site_id = _seed_site(conn)
    station_id = _seed_station(conn, site_id, pws_id="ISTATION01")
    record_access_refused(conn, "current_obs", 401)

    with pytest.raises(AccessHeld):
        _reserve_current_obs_call(conn, site_id, station_id)

    assert conn.execute("SELECT COUNT(*) FROM api_budget").fetchone()[0] == 0, (
        "no budget row may be created while the hold blocks the reservation"
    )

    # Paired positive, same test: once the hold is cleared, the identical
    # call DOES reserve a budget row -- proving the zero above is the
    # hold's effect, not a broken reservation path.
    clear_access_refused(conn, "current_obs")
    reservation = _reserve_current_obs_call(conn, site_id, station_id)
    assert reservation.calls == 1
    row = conn.execute(
        "SELECT calls FROM api_budget WHERE source = 'weathercom'"
    ).fetchone()
    assert row is not None and row["calls"] == 1


def test_co2_job_level_hold_in_force_defers_the_claimed_job_without_a_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The same hold-in-force precondition as the test above, but read
    through the real job pipeline (``run_claimed_job``) on a claimed
    ``fetch_current_obs`` job rather than by calling
    ``_reserve_current_obs_call`` directly: no provider call, no
    ``api_budget`` row with a nonzero ``calls``, ``station_poll_state``
    byte-identical, and the job left ``pending`` with ``next_attempt_at``
    equal to the hold's own ``until`` and ``retry_count`` 0.

    Mutant: ``_fetch_current_obs``'s ``except AccessHeld as held: raise
    JobDeferred(held.until) from None`` handling removed, so the inner
    reservation write's ``AccessHeld`` is never caught at the job level --
    at the "job row pending" assertion below: correct = ``"pending"`` with
    ``next_attempt_at`` == the hold's ``until``, mutant = the exception
    propagates out of ``run_claimed_job`` uncaught (this test would error
    rather than assert, which still distinguishes the mutant from the
    fix).
    """
    t0 = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: t0)

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id, pws_id="ISTATION01")
        _seed_poll_state(
            conn,
            station_id,
            health_state="online",
            next_poll_at=_PAST_ISO,
            last_error=None,
            error_count=0,
        )
        before_poll_state = dict(_poll_state_row(conn, station_id))

        until = record_access_refused(conn, "current_obs", 401)

        _seed_current_obs_job(conn, site_id, station_id)

        calls = {"n": 0}

        async def _fake_fetch(
            pws_station_id: str,
            api_key: str,
            *,
            client: httpx.AsyncClient | None = None,
            timeout_seconds: float = 10.0,
        ) -> httpx.Response:
            calls["n"] += 1
            return _refusal_response(401)

        monkeypatch.setattr(
            "wxverify.worker.processor.fetch_current_observation", _fake_fetch
        )

        job = await db.write(claim_next_current_obs_job)
        assert job is not None
        await run_claimed_job(db, job, lane="current_obs")

        assert calls["n"] == 0, "a held try must never reach the provider"

        budget_row = conn.execute(
            "SELECT calls FROM api_budget WHERE source = 'weathercom'"
        ).fetchone()
        assert budget_row is None or budget_row["calls"] == 0, (
            "no budget call may be reserved while the hold blocks the try"
        )

        assert dict(_poll_state_row(conn, station_id)) == before_poll_state

        job_row = _job_row(conn, job.id)
        assert job_row is not None
        assert job_row["status"] == "pending"
        assert job_row["next_attempt_at"] == until
        assert job_row["retry_count"] == 0

    asyncio.run(run())


# ---------------------------------------------------------------------------
# CO3 — a throttled shared key across six stations
# ---------------------------------------------------------------------------

_SIX_PWS_IDS = (
    "ISTATION01",
    "ISTATION02",
    "ISTATION03",
    "ISTATION04",
    "ISTATION05",
    "ISTATION06",
)


def test_co3_six_stations_share_one_throttled_hold_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Six due stations, every call answered 401: exactly one real provider
    call per throttle window, not six -- the other five meet the hold the
    first one just wrote, and the next window repeats exactly once more
    with ``since`` preserved.

    Mutant (per-station hold): ``access_hold_key`` keyed by station instead
    of by kind -- at the "exactly one call" assertion below: correct = 1,
    mutant = 6 (each station's own hold would never block the others).

    Mutant (hold extended by held tries): a held try's ``AccessHeld`` path
    itself re-extends ``until`` (instead of only a successful refusal doing
    so) -- at the "exactly one more call" assertion in the second round
    below: correct = 2 (total), mutant = 1 (the hold would still be in
    force at ``until + 1s`` because the five held tries during round one
    kept pushing ``until`` forward past that point). The round-1 drain
    advances the clock by 2 s after every claimed-and-run job (never
    rewinds it), so the five held tries land strictly AFTER T, giving a
    re-extending mutant room to push ``until`` past ``record1.until + 1s``.
    """
    box = {"now": datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)}
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: box["now"])
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: box["now"])

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        station_ids = []
        due = (box["now"] - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        for pws_id in _SIX_PWS_IDS:
            sid = _seed_station(conn, site_id, pws_id=pws_id)
            _seed_poll_state(conn, sid, health_state="online", next_poll_at=due)
            station_ids.append(sid)
        before_poll_states = {
            sid: dict(_poll_state_row(conn, sid)) for sid in station_ids
        }

        calls = {"n": 0}

        async def _fake_fetch(
            pws_station_id: str,
            api_key: str,
            *,
            client: httpx.AsyncClient | None = None,
            timeout_seconds: float = 10.0,
        ) -> httpx.Response:
            calls["n"] += 1
            return _refusal_response(401)

        monkeypatch.setattr(
            "wxverify.worker.processor.fetch_current_observation", _fake_fetch
        )

        # -- Round 1: all six due at T; drain to quiescence, advancing the
        # clock after every claimed-and-run job so the five held tries land
        # strictly after T. ------------------------------------------------
        await _drain_current_obs_lane_advancing(db, box)

        assert calls["n"] == 1, "only the first claim should ever reach the provider"
        record1 = read_access_record(conn, "current_obs")
        assert record1 is not None and record1.until is not None
        assert record1.since == "2026-07-10T12:00:00Z"
        assert record1.until == "2026-07-10T13:00:00Z", (
            "until must be exactly T + 3600s after round 1's one call"
        )

        for sid in station_ids:
            job_row = conn.execute(
                "SELECT status, next_attempt_at FROM jobs"
                " WHERE type = 'fetch_current_obs' AND job_key = ?",
                (f"curobs:{sid}",),
            ).fetchone()
            assert job_row is not None
            assert job_row["status"] == "pending"
            assert job_row["next_attempt_at"] == record1.until
            assert dict(_poll_state_row(conn, sid)) == before_poll_states[sid]

        # -- Still inside the hold, one minute before it ends: no claim at
        # all (proves the held tries truly made no call, rather than just
        # not reaching the provider for some other reason). ----------------
        box["now"] = datetime(2026, 7, 10, 12, 59, 0, tzinfo=UTC)
        still_held = await db.write(
            lambda conn: _enqueue_and_claim(conn, stamp_heartbeat=False)
        )
        assert still_held is None, (
            "no job must be claimable one minute before the hold ends"
        )
        assert calls["n"] == 1, "the call count must not move while still held"

        # -- Round 2: advance to just past the hold's end; repeat once. -------
        until1 = datetime.fromisoformat(record1.until.replace("Z", "+00:00"))
        box["now"] = until1 + timedelta(seconds=1)

        await _drain_current_obs_lane_advancing(db, box)

        assert calls["n"] == 2, "exactly one more call once the hold has ended"
        record2 = read_access_record(conn, "current_obs")
        assert record2 is not None and record2.until is not None
        assert record2.since == record1.since, "since must stay T across windows"
        expected_until2 = until1 + timedelta(seconds=1) + timedelta(seconds=3600)
        assert record2.until == expected_until2.isoformat().replace("+00:00", "Z"), (
            "until must be exactly round 2's one call's time + 3600s -- if a"
            " held try had re-extended the hold during round 1, round 2 would"
            " have made no call at all and this comparison would be moot"
        )

    asyncio.run(run())


# ---------------------------------------------------------------------------
# CO4 — a 2xx reply clears ONLY the current_obs hold
# ---------------------------------------------------------------------------


def test_co4_2xx_reply_clears_only_current_obs_hold(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Once the ``current_obs`` hold has ended, a 200 reply: one call; the
    station's poll state ``online`` with the new reading; the
    ``current_obs`` hold row deleted; a separately-held ``hourly`` row
    left untouched (paired positive -- the clear is kind-scoped, not a
    blanket clear).

    Mutant (no clear): ``_persist_poll_result_clearing``'s
    ``clear_access_refused`` call removed -- at the "current_obs hold row
    deleted" assertion below: correct = None, mutant = the still-present
    JSON record (non-None).

    Mutant (clears every kind): the clear applied to every kind in
    ``ACCESS_KINDS`` instead of just ``current_obs`` -- at the "hourly
    untouched" assertion below: correct = a non-None record, mutant = None
    (hourly would be cleared too).
    """
    t0 = datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: t0)

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id, pws_id="ISTATION01")
        _seed_poll_state(
            conn,
            station_id,
            health_state="offline",
            next_poll_at=_PAST_ISO,
            last_error="prior-poll-error",
            error_count=5,
        )

        until = record_access_refused(conn, "current_obs", 401)
        record_access_refused(conn, "hourly", 403)

        # Advance access_hold's own clock past the hold's end -- the job's
        # own due time is seeded in the past independently, so no other
        # clock needs patching.
        after = datetime.fromisoformat(until.replace("Z", "+00:00")) + timedelta(
            seconds=1
        )
        monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: after)

        _seed_current_obs_job(conn, site_id, station_id)

        calls = {"n": 0}

        async def _fake_fetch(
            pws_station_id: str,
            api_key: str,
            *,
            client: httpx.AsyncClient | None = None,
            timeout_seconds: float = 10.0,
        ) -> httpx.Response:
            calls["n"] += 1
            return _online_response()

        monkeypatch.setattr(
            "wxverify.worker.processor.fetch_current_observation", _fake_fetch
        )

        job = await db.write(claim_next_current_obs_job)
        assert job is not None
        await run_claimed_job(db, job, lane="current_obs")

        assert calls["n"] == 1

        poll_row = _poll_state_row(conn, station_id)
        assert poll_row is not None
        assert poll_row["health_state"] == "online"
        assert poll_row["last_error"] is None
        assert poll_row["error_count"] == 0

        obs_row = _current_obs_row(conn, station_id)
        assert obs_row is not None
        assert obs_row["temp"] == 20.0

        assert read_access_record(conn, "current_obs") is None, (
            "the current_obs hold row must be deleted by the 2xx reply"
        )
        assert read_access_record(conn, "hourly") is not None, (
            "a separately-held hourly row must be left untouched"
        )

    asyncio.run(run())


# ---------------------------------------------------------------------------
# CO5 — the monitor must not count held stations as overdue
# ---------------------------------------------------------------------------


def test_co5_held_stations_count_as_in_backoff_not_overdue(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """After CO3's round-1 state (six stations deferred to the hold's end),
    ``_obs_collection_delays`` at T + 30 min gives ``overdue=0,
    in_backoff=6``: the stations are working as designed, not stuck.

    Mutant (treat held as overdue, e.g. completing the job instead of
    deferring it): at the assertion below, correct = (0, 6), mutant =
    (6, 0) -- a completed job leaves no pending ``next_attempt_at`` to
    push ``effective`` past the monitor's 15-minute cutoff, so every
    station reads as overdue on its stale ``due_basis`` alone.
    """
    box = {"now": datetime(2026, 7, 10, 12, 0, 0, tzinfo=UTC)}
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: box["now"])
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: box["now"])

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        due = (box["now"] - timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        for pws_id in _SIX_PWS_IDS:
            sid = _seed_station(conn, site_id, pws_id=pws_id)
            _seed_poll_state(conn, sid, health_state="online", next_poll_at=due)

        async def _fake_fetch(
            pws_station_id: str,
            api_key: str,
            *,
            client: httpx.AsyncClient | None = None,
            timeout_seconds: float = 10.0,
        ) -> httpx.Response:
            return _refusal_response(401)

        monkeypatch.setattr(
            "wxverify.worker.processor.fetch_current_observation", _fake_fetch
        )

        await _drain_current_obs_lane_advancing(db, box)

        box["now"] = box["now"] + timedelta(minutes=30)
        overdue, in_backoff, _oldest = _obs_collection_delays(conn, box["now"])
        assert (overdue, in_backoff) == (0, 6)

    asyncio.run(run())
