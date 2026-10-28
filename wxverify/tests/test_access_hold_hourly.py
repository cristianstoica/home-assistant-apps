"""The hourly download lane's refused-access behaviour (wxverify 0.16.13
round 2, commit 3; plan HD1-HD5).

HD1 — a 401/403 on the FIRST station: one call, the ``hourly`` hold row
      written, the cycle's job completes with ``result`` NULL (not failed
      or retried), ``last_obs_cycle_at`` stamped, the second station never
      called, one budget call reserved and NOT refunded, no
      ``domain_backoffs`` row, the other two kinds' hold rows absent, both
      stations' rows byte-identical, and the held-cycle INFO line logged
      exactly once.
HD2 — a fake that answers every call with 401: over a clock-driven hour (60
      one-minute scheduler ticks), exactly one provider call is made, not
      six -- the held cycle completes instead of entering the queue's
      ordinary fail/retry ladder, so the site is not due again inside the
      hour.
HD3 — a hold already in force when the cycle is due: no call at all; the
      job completes with ``result`` NULL; the cycle is still stamped; and
      immediately after, the scheduler finds nothing due to enqueue.
HD4 — station1 (200, with a reading) then station2 (401): station1's
      reading is persisted and counted (site ``last_obs_at`` raised, one
      ``pair_and_score`` job queued) even though the cycle then holds on
      station2.
HD5 — a granted call (200 with a reading, and separately a 2xx reply with
      no observations that parks the station) clears the ``hourly`` hold
      row.

Isolation: every test uses a real tmp-path ``Database`` (``init_db``), run
through the real job pipeline (``enqueue_if_absent`` + ``claim_next_job`` +
``run_claimed_job(db, job, lane="main")``), never a synthetic in-memory
``Job`` -- HD1/HD3/HD4 need the real ``jobs`` row's ``status``/
``retry_count``/``result`` columns, which a hand-built ``Job`` bypasses.
Weather.com itself is faked at the transport layer (``httpx.MockTransport``
patched in for ``wxverify.worker.processor.httpx.AsyncClient``, exactly as
``tests/test_fetch_obs_partial_cycle.py`` does), never by mocking
``_fetch_obs`` or any of its helpers.

Time control: ``processor.py`` and ``obs/pws_adapter.py`` each import
``utc_now`` by bare name, so each needs its own monkeypatch target; HD2's
drive loop additionally needs ``wxverify.worker.scheduler.utc_now`` (the
due-check) and ``wxverify.db.queue.utc_now`` plus
``wxverify.core.timeutil.utc_now`` (the claim ordering and the retry-delay
math a "re-raise the 401" mutant would exercise). Every loop that drives
claimed jobs is bounded and ``pytest.fail``s loudly on the bound instead of
hanging, per last round's incident.

Every station id below is synthetic: ``ISTATION01``/``ISTATION02``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from wxverify import config
from wxverify.db.connection import Database, close_db, init_db
from wxverify.db.queue import claim_next_job, enqueue_if_absent
from wxverify.worker.access_hold import read_access_record
from wxverify.worker.processor import run_claimed_job
from wxverify.worker.scheduler import _enqueue_due_obs  # noqa: PLC2701

_API_KEY = "0123456789abcdef0123456789abcdef"
_TZ = "UTC"

# ---------------------------------------------------------------------------
# Shared harness
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _weathercom_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _API_KEY)


@pytest.fixture(autouse=True)
def _fast_pace(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_pace(site_id: int, station_id: int, ordinal: int) -> None:
        return None

    monkeypatch.setattr("wxverify.worker.processor.pace_station_call", _fake_pace)


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
        " VALUES (?, 0.0, 0.0, 0.0, ?)",
        (name, _TZ),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_station(conn, site_id: int, *, pws_id: str) -> int:  # type: ignore[no-untyped-def]
    conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m)"
        " VALUES (?, ?, 0.0, 0.0, 0.0)",
        (site_id, pws_id),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _station_row(conn, station_id: int):  # type: ignore[no-untyped-def]
    row = conn.execute("SELECT * FROM stations WHERE id=?", (station_id,)).fetchone()
    assert row is not None
    return row


def _site_row(conn, site_id: int):  # type: ignore[no-untyped-def]
    row = conn.execute(
        "SELECT last_obs_at, last_obs_cycle_at FROM sites WHERE id=?", (site_id,)
    ).fetchone()
    assert row is not None
    return row


def _job_row(conn, job_id: int):  # type: ignore[no-untyped-def]
    row = conn.execute(
        "SELECT status, next_attempt_at, retry_count, result FROM jobs WHERE id=?",
        (job_id,),
    ).fetchone()
    assert row is not None
    return row


def _budget_calls(conn) -> int:  # type: ignore[no-untyped-def]
    row = conn.execute(
        "SELECT COALESCE(SUM(calls), 0) FROM api_budget WHERE source='weathercom'"
    ).fetchone()
    return int(row[0])


def _refusal_response(status: int) -> httpx.Response:
    return httpx.Response(status, content=b"")


def _obs_response(readings: list[tuple[str, float]]) -> httpx.Response:
    body = {
        "observations": [
            {"obsTimeUtc": stamp, "metric": {"temp": value}}
            for stamp, value in readings
        ]
    }
    return httpx.Response(
        200,
        content=json.dumps(body).encode(),
        headers={"content-type": "application/json"},
    )


def _no_content_response() -> httpx.Response:
    return httpx.Response(204)


def _freeze(monkeypatch: pytest.MonkeyPatch, when: datetime) -> None:
    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: when)
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: when)


async def _run_main_job(db: Database, handler) -> int:  # type: ignore[no-untyped-def]
    """Claim and run exactly one main-lane job through the real transport fake.

    Returns the claimed job's id so the caller can re-read its row.
    """
    real_client = httpx.AsyncClient
    with patch(
        "wxverify.worker.processor.httpx.AsyncClient",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),  # type: ignore[arg-type]
    ):
        job = await db.write(claim_next_job)
        assert job is not None
        await run_claimed_job(db, job, lane="main")
        return job.id


# ---------------------------------------------------------------------------
# HD1 — 401/403 on the first station holds hourly, completes the cycle
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("refusal_status", [401, 403])
def test_hd1_refusal_holds_hourly_and_completes_the_cycle_without_a_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    refusal_status: int,
) -> None:
    """A 401/403 from the first of two stations: one call; the second
    station never called; the ``hourly`` hold row written; the job
    completes with ``result`` NULL (not failed/retried); ``last_obs_cycle_at``
    stamped; both stations' rows untouched; one budget call reserved and
    NOT refunded; no ``domain_backoffs`` row; the other two kinds' holds
    absent; the held-cycle INFO line logged exactly once with
    ``kind=hourly`` and ``fetched=0 of 2``.

    Mutant (today's path disabled) -> at "job completes, not retried" below:
    correct = job_row["status"] == "completed", mutant = "pending" (the
    401/403 branch's ``if exc.response.status_code in (401, 403):`` guard
    disabled, so the error falls through to the ordinary backoff/retry
    path and ``fail()`` leaves the row ``pending`` with ``retry_count`` 1).

    Mutant (writing stations.last_error) -> at "station rows untouched"
    below: correct = both stations' rows equal their seeded state, mutant =
    station1's ``last_error`` is no longer NULL (a station-error write
    added to the hold branch).

    Mutant (every-kind recording) -> at "no current_obs/five_minute hold"
    below: correct = None for both, mutant = a non-None record for each
    (the hold written for every kind in ``ACCESS_KINDS`` instead of just
    ``"hourly"``).
    """

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
        _freeze(monkeypatch, now)
        site_id = _seed_site(conn)
        s1 = _seed_station(conn, site_id, pws_id="ISTATION01")
        s2 = _seed_station(conn, site_id, pws_id="ISTATION02")
        before1 = dict(_station_row(conn, s1))
        before2 = dict(_station_row(conn, s2))

        enqueue_if_absent(conn, "fetch_obs", site_id, "obs", {})

        calls = {"n": 0}
        called: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            called.append(str(request.url.params["stationId"]))
            return _refusal_response(refusal_status)

        caplog.set_level(logging.INFO, logger="wxverify.worker.processor")
        job_id = await _run_main_job(db, handler)

        assert calls["n"] == 1, "exactly one provider call: the hold stops the cycle"
        assert called == ["ISTATION01"], "the second station must never be called"

        record = read_access_record(conn, "hourly")
        assert record is not None, "the hourly hold row must be written"
        assert record.status == refusal_status

        assert read_access_record(conn, "current_obs") is None
        assert read_access_record(conn, "five_minute") is None

        job_row = _job_row(conn, job_id)
        assert job_row["status"] == "completed", (
            "job completes, not retried -- a cancelled/held cycle resolves "
            "normally, not as a failure"
        )
        assert job_row["retry_count"] == 0
        assert job_row["result"] is None

        site_row = _site_row(conn, site_id)
        assert site_row["last_obs_cycle_at"] is not None

        assert dict(_station_row(conn, s1)) == before1, "station rows untouched"
        assert dict(_station_row(conn, s2)) == before2, "station rows untouched"

        assert _budget_calls(conn) == 1, "one call reserved and NOT refunded"
        assert conn.execute("SELECT COUNT(*) FROM domain_backoffs").fetchone()[0] == 0

        held_lines = [
            r
            for r in caplog.records
            if r.name == "wxverify.worker.processor"
            and r.levelno == logging.INFO
            and r.getMessage().startswith("fetch_obs held")
        ]
        assert len(held_lines) == 1, "the held-cycle INFO line logs exactly once"
        message = held_lines[0].getMessage()
        assert "kind=hourly" in message
        assert "fetched=0 of 2" in message

    asyncio.run(run())


# ---------------------------------------------------------------------------
# HD2 — an hour of 401s produces one call, not six
# ---------------------------------------------------------------------------


def test_hd2_an_hour_of_refusals_makes_exactly_one_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Over 60 one-minute scheduler ticks, each enqueuing due obs cycles and
    draining the main lane dry, a provider that answers every call with 401
    is called exactly once.

    Mutant (today's path disabled) -> at the call-count assertion below:
    correct = 1, mutant = 6 (the 401/403 branch disabled, so the job falls
    into the ordinary fail/retry ladder: one initial try plus five retries,
    each re-claimable well inside the hour, before the row finally goes
    terminal -- the held cycle never completes, so the scheduler also
    re-enqueues it every time the failure cooldown is not what suppresses
    it).
    """
    t0 = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    box = {"now": t0}

    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: box["now"])
    monkeypatch.setattr("wxverify.obs.pws_adapter.utc_now", lambda: box["now"])
    monkeypatch.setattr("wxverify.worker.scheduler.utc_now", lambda: box["now"])
    monkeypatch.setattr("wxverify.db.queue.utc_now", lambda: box["now"])
    monkeypatch.setattr("wxverify.core.timeutil.utc_now", lambda: box["now"])

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        _seed_station(conn, site_id, pws_id="ISTATION01")

        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return _refusal_response(401)

        real_client = httpx.AsyncClient
        with patch(
            "wxverify.worker.processor.httpx.AsyncClient",
            lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),  # type: ignore[arg-type]
        ):
            for _ in range(60):
                await db.write(lambda conn: _enqueue_due_obs(conn))
                for _ in range(20):
                    job = await db.write(claim_next_job)
                    if job is None:
                        break
                    await run_claimed_job(db, job, lane="main")
                else:
                    pytest.fail(
                        "main lane never ran dry within 20 claimed-and-run jobs "
                        "at one minute tick -- a held job is probably being "
                        "retried instead of completed"
                    )
                box["now"] = box["now"] + timedelta(minutes=1)

        assert calls["n"] == 1, "one call across the whole hour, not six"

    asyncio.run(run())


# ---------------------------------------------------------------------------
# HD3 — a hold already in force blocks the whole cycle, but still stamps it
# ---------------------------------------------------------------------------


def test_hd3_hold_in_force_blocks_the_cycle_but_still_stamps_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A hold already covering ``hourly`` when the cycle is due: no call at
    all; the job completes with ``result`` NULL; ``last_obs_cycle_at`` is
    stamped anyway; and right after, the scheduler finds nothing due to
    enqueue.

    Mutant (deferring to until) -> at "job completes, not deferred" below:
    correct = job_row["status"] == "completed", mutant = "pending" with
    ``next_attempt_at`` equal to the hold's ``until`` (``raise
    JobCancelled()`` replaced by ``raise JobDeferred(held_until)``).

    Mutant (not stamping) -> at "cycle is stamped anyway" below: correct =
    a non-None ``last_obs_cycle_at``, mutant = still None (the
    ``_complete_obs_cycle`` write skipped while held).

    Mutant (held-completes-ok) -> at "result is NULL" below: correct =
    None, mutant = ``"ok"`` (``raise JobCancelled()`` replaced by a plain
    ``return``, so the normal success path writes the ``"ok"`` marker).
    """
    from wxverify.worker.access_hold import record_access_refused

    t0 = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: t0)
    _freeze(monkeypatch, t0)

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        _seed_station(conn, site_id, pws_id="ISTATION01")

        record_access_refused(conn, "hourly", 401)

        enqueue_if_absent(conn, "fetch_obs", site_id, "obs", {})

        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return _refusal_response(401)

        job_id = await _run_main_job(db, handler)

        assert calls["n"] == 0, "the hold blocks the reservation before any call"

        job_row = _job_row(conn, job_id)
        assert job_row["status"] == "completed", "job completes, not deferred"
        assert job_row["result"] is None, "result is NULL"

        site_row = _site_row(conn, site_id)
        assert site_row["last_obs_cycle_at"] is not None, "cycle is stamped anyway"

        await db.write(lambda conn: _enqueue_due_obs(conn))
        pending = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE type='fetch_obs' AND status='pending'"
        ).fetchone()[0]
        assert pending == 0, "nothing due right after the stamped cycle"

    asyncio.run(run())


# ---------------------------------------------------------------------------
# HD4 — a station fetched before the refusal is not dropped
# ---------------------------------------------------------------------------


def test_hd4_a_station_fetched_before_the_refusal_is_not_dropped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """station1 answers 200 with a reading, station2 answers 401: station1's
    reading is persisted and counted even though the cycle then holds.

    Mutant (dropping fetched stations) -> at "last_obs_at raised" and "one
    pair_and_score job queued" below: correct = the new, later stamp /
    exactly 1, mutant = the prior (seeded) stamp / 0 (``changed``/
    ``newest_obs_at`` reset to their initial values on the station2 break,
    discarding station1's already-persisted result from the cycle's own
    bookkeeping).

    Mutant (held-completes-ok) -> same assertion as HD3/HD1's: at "result
    is NULL" below: correct = None, mutant = ``"ok"``.
    """
    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        _seed_station(conn, site_id, pws_id="ISTATION01")
        _seed_station(conn, site_id, pws_id="ISTATION02")

        enqueue_if_absent(conn, "fetch_obs", site_id, "obs", {})

        def handler(request: httpx.Request) -> httpx.Response:
            station = str(request.url.params["stationId"])
            if station == "ISTATION01":
                return _obs_response([("2026-09-22T11:00:00Z", 10.0)])
            return _refusal_response(401)

        job_id = await _run_main_job(db, handler)

        job_row = _job_row(conn, job_id)
        assert job_row["status"] == "completed"
        assert job_row["result"] is None, "result is NULL"

        site_row = _site_row(conn, site_id)
        assert site_row["last_obs_at"] == "2026-09-22T11:00:00Z", "last_obs_at raised"

        queued = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE type='pair_and_score' AND site_id=?",
            (site_id,),
        ).fetchone()[0]
        assert queued == 1, "one pair_and_score job queued"

        record = read_access_record(conn, "hourly")
        assert record is not None and record.status == 401

    asyncio.run(run())


# ---------------------------------------------------------------------------
# HD5 — a granted call clears the hourly hold
# ---------------------------------------------------------------------------


def _preexisting_hold(monkeypatch: pytest.MonkeyPatch, conn) -> str:  # type: ignore[no-untyped-def]
    """Seed an hourly hold whose ``until`` has already passed by the time the
    next call is made, so ``check_access_hold`` does not itself block the
    call -- only the eventual granted reply's own clear removes the row.
    """
    from wxverify.worker.access_hold import record_access_refused

    t0 = datetime(2026, 9, 22, 11, 0, 0, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: t0)
    until = record_access_refused(conn, "hourly", 401)
    after = datetime.fromisoformat(until.replace("Z", "+00:00")) + timedelta(seconds=1)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: after)
    return until


def test_hd5a_a_200_reply_clears_the_hourly_hold(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A granted 200 reply with a reading clears the hourly hold row.

    Mutant (clearing every kind) -> at "only hourly's row is gone" below:
    correct = ``current_obs``'s hold untouched (non-None), mutant = also
    cleared (the single ``clear_access_refused(conn, "hourly")`` in
    ``_persist_station_observations`` replaced by a loop over every kind).
    """
    from wxverify.worker.access_hold import read_access_record, record_access_refused

    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: now)

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        _seed_station(conn, site_id, pws_id="ISTATION01")

        _preexisting_hold(monkeypatch, conn)
        record_access_refused(conn, "current_obs", 401)

        enqueue_if_absent(conn, "fetch_obs", site_id, "obs", {})

        def handler(request: httpx.Request) -> httpx.Response:
            return _obs_response([("2026-09-22T11:00:00Z", 10.0)])

        await _run_main_job(db, handler)

        assert read_access_record(conn, "hourly") is None, "hourly's hold row is gone"
        assert read_access_record(conn, "current_obs") is not None, (
            "only hourly's row is gone -- current_obs stays held"
        )

    asyncio.run(run())


def test_hd5b_a_no_content_reply_parks_the_station_and_clears_the_hold(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A granted 2xx reply with no observations parks the station's history
    AND clears the hourly hold row -- a parked station is still a granted
    call.

    Mutant (no clear on no-content) -> at "hourly's hold row is gone" below:
    correct = None, mutant = a non-None record (the
    ``clear_access_refused(conn, "hourly")`` call removed from
    ``_park_station_history_clearing``).
    """
    from wxverify.worker.access_hold import read_access_record

    now = datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)
    _freeze(monkeypatch, now)
    monkeypatch.setattr("wxverify.worker.access_hold.utc_now", lambda: now)

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id, pws_id="ISTATION01")

        _preexisting_hold(monkeypatch, conn)

        enqueue_if_absent(conn, "fetch_obs", site_id, "obs", {})

        def handler(request: httpx.Request) -> httpx.Response:
            return _no_content_response()

        await _run_main_job(db, handler)

        assert read_access_record(conn, "hourly") is None, "hourly's hold row is gone"

        parked = dict(_station_row(conn, station_id))
        assert parked["history_next_attempt_at"] is not None, "station is parked"

    asyncio.run(run())
