"""The current-obs lane: claim partition, poller loop and DL3.

Covers plan §14.2 tests P, P2, X, Q, the idle-poller-seam test, H, F, W and
DL3 -- the 2026-09-23 station-health-and-retries plan, commit 5 (D1.2).

EQP (the query-plan relationship for the two claim clauses) lives in
``tests/test_query_plan_regressions.py``, so the shipping-SQLite CI job runs
it too. K1 (the shared call-lock limit across lanes) lives in
``tests/test_weathercom_call_lock.py`` alongside K2-K5d. C1-C5, the two
further supervisor cases, the app-level stop test, DL4, DL5 and the C3
repeated-cancel reclaim test live in ``tests/test_graceful_shutdown.py``
alongside the rest of the real two-lane supervisor harness.

Convention: ``asyncio.run(run())`` (no pytest-asyncio), matching the rest of
this suite. Real tmp ``Database`` via ``init_db`` for every test that drives
``db.write``/``run_claimed_job``/the poller loop; a bare in-memory
``sqlite3.Connection`` for the pure claim-partition tests (P, P2) that never
touch the async machinery.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from wxverify import config
from wxverify.collection.budget import (
    is_refundable_transport_error,
)
from wxverify.db.connection import Database, FencedWriter, close_db, init_db
from wxverify.db.migrations import (
    create_schema,
    seed_default_feeds,
    seed_default_settings,
    seed_default_sources,
)
from wxverify.db.queue import (
    Job,
    claim_next_current_obs_job,
    claim_next_job,
    complete,
    defer_job,
    enqueue_if_absent,
    fail,
)
from wxverify.db.runtime_state import RUNTIME_STATE_KEYS
from wxverify.worker import current_obs_poller as current_obs_poller_module
from wxverify.worker.current_obs import Health, PollOutcome, persist_poll_result
from wxverify.worker.current_obs_poller import (
    HEARTBEAT_KEY,
    _enqueue_and_claim,  # noqa: PLC2701
    run_current_obs_poller,
)
from wxverify.worker.processor import (
    _complete_and_continue,  # noqa: PLC2701
    _persist_poll_result_and_refund,  # noqa: PLC2701
    _record_current_obs_backoff,  # noqa: PLC2701
    _reserve_current_obs_call,  # noqa: PLC2701
    run_claimed_job,
    run_worker,
)

_STATION_PWS_ID = "ISTATION01"

# ---------------------------------------------------------------------------
# DB fixture helpers
# ---------------------------------------------------------------------------


def _make_conn() -> sqlite3.Connection:
    """Open an in-memory schema-current DB, no Database wrapper."""
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    create_schema(conn)
    seed_default_sources(conn)
    seed_default_feeds(conn)
    seed_default_settings(conn)
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


async def _await_task_done(task: asyncio.Task[Any], *, timeout: float = 5.0) -> None:
    """Bound a completion wait for a cancelled task instead of an unbounded
    ``await task``.

    Mirrors ``tests/test_graceful_shutdown.py``'s helper of the same name:
    observe completion with a bounded ``asyncio.wait`` first, only then
    ``await`` the (now-done) task to raise/return; on a genuine hang, cancel
    and drain before failing so nothing outlives the test.
    """
    done, pending = await asyncio.wait({task}, timeout=timeout)
    if pending:
        task.cancel()
        with contextlib.suppress(BaseException):
            await asyncio.wait({task}, timeout=1.0)
        raise AssertionError(f"task did not complete within {timeout}s")
    assert task in done


def _seed_site(conn: sqlite3.Connection, *, name: str = "SITE-A") -> int:
    conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)"
        " VALUES (?, 40.0, -105.0, 900.0, 'UTC')",
        (name,),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_station(
    conn: sqlite3.Connection,
    site_id: int,
    *,
    pws_id: str = _STATION_PWS_ID,
    enabled: int = 1,
) -> int:
    conn.execute(
        "INSERT INTO stations"
        " (site_id, pws_station_id, lat, lon, dem_elevation_m, enabled)"
        " VALUES (?, ?, 40.0, -105.0, 900.0, ?)",
        (site_id, pws_id, enabled),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _seed_current_obs_job(
    conn: sqlite3.Connection, site_id: int, station_id: int, *, key: str = "curobs:1"
) -> None:
    enqueue_if_absent(
        conn, "fetch_current_obs", site_id, key, {"station_id": station_id}
    )


def _seed_obs_job(
    conn: sqlite3.Connection, site_id: int, *, key: str = "obs:1"
) -> None:
    enqueue_if_absent(conn, "fetch_obs", site_id, key, {})


# ---------------------------------------------------------------------------
# P: claim partition
# ---------------------------------------------------------------------------


def test_p_main_lane_ignores_current_obs_but_claims_fetch_obs() -> None:
    conn = _make_conn()
    site_id = _seed_site(conn)
    station_id = _seed_station(conn, site_id)
    _seed_current_obs_job(conn, site_id, station_id)

    # Only a pending fetch_current_obs: the main lane must not claim it.
    assert claim_next_job(conn) is None

    _seed_obs_job(conn, site_id)
    job = claim_next_job(conn)
    assert job is not None
    assert job.type == "fetch_obs"


def test_p_poller_lane_ignores_fetch_obs_but_claims_current_obs() -> None:
    conn = _make_conn()
    site_id = _seed_site(conn)
    _seed_obs_job(conn, site_id)

    # Only a pending fetch_obs: the current-obs lane must not claim it.
    assert claim_next_current_obs_job(conn) is None

    station_id = _seed_station(conn, site_id)
    _seed_current_obs_job(conn, site_id, station_id)
    job = claim_next_current_obs_job(conn)
    assert job is not None
    assert job.type == "fetch_current_obs"


# ---------------------------------------------------------------------------
# P2: off-schema BLOB type
# ---------------------------------------------------------------------------


def test_p2_off_schema_blob_type_is_claimed_by_main_lane_not_stranded() -> None:
    conn = _make_conn()
    site_id = _seed_site(conn)

    conn.execute("PRAGMA ignore_check_constraints=ON")
    try:
        conn.execute(
            "INSERT INTO jobs"
            " (type, site_id, job_key, payload, status, next_attempt_at, max_retries)"
            " VALUES (?, ?, 'curobs:blob', '{}', 'pending', '2020-01-01T00:00:00Z', 0)",
            (b"fetch_current_obs", site_id),
        )
    finally:
        conn.execute("PRAGMA ignore_check_constraints=OFF")

    # The poller lane must never claim it.
    assert claim_next_current_obs_job(conn) is None

    job = claim_next_job(conn)
    assert job is not None
    assert job.type == "b'fetch_current_obs'"

    disposition = fail(conn, job.id, f"unknown job type {job.type}")
    assert disposition is not None
    assert disposition.terminal is True
    row = conn.execute("SELECT status FROM jobs WHERE id = ?", (job.id,)).fetchone()
    assert row["status"] == "failed"


# ---------------------------------------------------------------------------
# X / Q: claim exclusivity
# ---------------------------------------------------------------------------


def test_x_claim_exclusivity_each_lane_gets_its_own_type(tmp_path: Path) -> None:
    """C5: one real Database, one pending job of each partition.

    ``asyncio.gather`` drives both claims concurrently; each lane must get
    its own type, and a second round must return None on both.
    """

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001 - seeding on the real writer connection
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_current_obs_job(conn, site_id, station_id)
        _seed_obs_job(conn, site_id)

        main_job, poller_job = await asyncio.gather(
            db.write(claim_next_job), db.write(claim_next_current_obs_job)
        )
        assert main_job is not None
        assert poller_job is not None
        assert main_job.type == "fetch_obs"
        assert poller_job.type == "fetch_current_obs"

        second_main, second_poller = await asyncio.gather(
            db.write(claim_next_job), db.write(claim_next_current_obs_job)
        )
        assert second_main is None
        assert second_poller is None

    asyncio.run(run())


def test_q_partition_claim_does_not_double_claim_at_scale(tmp_path: Path) -> None:
    """The §4.1 partition differential at reduced test size: every claim
    lands in exactly one lane, and no job is claimed twice.
    """

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        current_obs_keys = [f"curobs:{i}" for i in range(30)]
        obs_keys = [f"obs:{i}" for i in range(30)]
        for key in current_obs_keys:
            _seed_current_obs_job(conn, site_id, station_id, key=key)
        for key in obs_keys:
            _seed_obs_job(conn, site_id, key=key)

        claimed_main: list[Job] = []
        claimed_poller: list[Job] = []
        seen_ids: set[int] = set()
        for _ in range(len(current_obs_keys) + len(obs_keys) + 2):
            main_job, poller_job = await asyncio.gather(
                db.write(claim_next_job), db.write(claim_next_current_obs_job)
            )
            if main_job is not None:
                assert main_job.id not in seen_ids
                seen_ids.add(main_job.id)
                assert main_job.type != "fetch_current_obs"
                claimed_main.append(main_job)
            if poller_job is not None:
                assert poller_job.id not in seen_ids
                seen_ids.add(poller_job.id)
                assert poller_job.type == "fetch_current_obs"
                claimed_poller.append(poller_job)

        assert len(claimed_main) == len(obs_keys)
        assert len(claimed_poller) == len(current_obs_keys)

    asyncio.run(run())


# ---------------------------------------------------------------------------
# Idle-poller seam
# ---------------------------------------------------------------------------


def test_run_worker_resolves_poller_via_the_processor_module_attribute(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``run_worker`` must call the ``run_current_obs_poller`` NAME bound on
    ``wxverify.worker.processor``, not a fresh import from the poller
    module -- otherwise patching the processor attribute (the whole test
    seam in §6.3.6) would silently stop working.
    """
    recorder: dict[str, bool] = {"called": False}

    async def _recording_poller(db: object, *, run_job: object) -> None:
        recorder["called"] = True
        await asyncio.Event().wait()

    monkeypatch.setattr(
        "wxverify.worker.processor.run_current_obs_poller", _recording_poller
    )
    monkeypatch.setattr(
        "wxverify.worker.processor._run_main_lane",
        lambda db: asyncio.Event().wait(),
    )

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        task = asyncio.create_task(run_worker(db))
        for _ in range(20):
            if recorder["called"]:
                break
            await asyncio.sleep(0)
        assert recorder["called"] is True
        task.cancel()
        await _await_task_done(task)
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())


# ---------------------------------------------------------------------------
# H: heartbeat
# ---------------------------------------------------------------------------


class _SleepGate:
    """Blocking-fake rendezvous: turns the poller's idle-sleep into a
    test-controlled step instead of a real 5s wait, without letting the loop
    race ahead of the assertions between iterations.
    """

    def __init__(self) -> None:
        self.calls = 0
        self._reached = asyncio.Event()
        self._release = asyncio.Event()

    async def sleep(self, seconds: float) -> None:
        self.calls += 1
        self._reached.set()
        await self._release.wait()
        self._release.clear()

    async def wait_reached(self) -> None:
        await self._reached.wait()
        self._reached.clear()

    def release(self) -> None:
        self._release.set()


def test_h_heartbeat_stamps_on_first_iteration_then_holds_for_60s(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """H: the first iteration always stamps; a second iteration inside the
    60s window must not; a later iteration once 60s have elapsed since the
    LAST stamp must resume stamping (RUNTIME_HEARTBEAT_INTERVAL_SECONDS).

    ``time`` and ``asyncio`` are replaced as NAMES bound in the poller
    module's own namespace (``monkeypatch.setattr(current_obs_poller_module,
    ...)``), never as attributes reached through into the real global
    modules (``...current_obs_poller.time.monotonic`` etc). The latter would
    patch the process-wide ``time``/``asyncio`` modules themselves --
    freezing the event loop's own clock (so ``asyncio.wait_for``'s deadlines
    stop working) and replacing ``asyncio.sleep`` everywhere, not just
    inside this module.
    """
    assert HEARTBEAT_KEY in RUNTIME_STATE_KEYS

    stamped: list[str] = []

    def _fake_set_runtime_state_now(conn: sqlite3.Connection, key: str) -> None:
        stamped.append(key)

    # iter1: t=100.0 (no prior heartbeat) -> stamps, last_heartbeat=100.0.
    # iter2: t=130.0 (30s since 100.0, < 60s) -> must NOT stamp.
    # iter3: t=160.0 (60s since 100.0, >= 60s) -> must RESUME stamping.
    monotonic_values = iter([100.0, 130.0, 160.0])

    def _fake_monotonic() -> float:
        try:
            return next(monotonic_values)
        except StopIteration:
            return 160.0

    gate = _SleepGate()
    monkeypatch.setattr(
        "wxverify.worker.current_obs_poller.set_runtime_state_now",
        _fake_set_runtime_state_now,
    )
    monkeypatch.setattr(
        current_obs_poller_module, "time", SimpleNamespace(monotonic=_fake_monotonic)
    )
    monkeypatch.setattr(
        current_obs_poller_module, "asyncio", SimpleNamespace(sleep=gate.sleep)
    )

    async def run() -> None:
        db = _init_tmp_db(tmp_path)

        async def _unused_run_job(db: object, job: object, *, lane: str) -> None:
            raise AssertionError("no job should ever be claimed in this test")

        task = asyncio.create_task(run_current_obs_poller(db, run_job=_unused_run_job))
        try:
            await asyncio.wait_for(gate.wait_reached(), timeout=5)
            assert stamped == [HEARTBEAT_KEY]  # first iteration: t=100.0, stamps

            gate.release()
            await asyncio.wait_for(gate.wait_reached(), timeout=5)
            # second iteration: t=130.0, only 30s after the first -- must NOT
            # stamp again (RUNTIME_HEARTBEAT_INTERVAL_SECONDS is 60.0).
            assert stamped == [HEARTBEAT_KEY]

            gate.release()
            await asyncio.wait_for(gate.wait_reached(), timeout=5)
            # third iteration: t=160.0, 60s after the first stamp -- must
            # RESUME stamping.
            assert stamped == [HEARTBEAT_KEY, HEARTBEAT_KEY]
        finally:
            task.cancel()
            await _await_task_done(task)
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(run())


# ---------------------------------------------------------------------------
# F: fence capture
# ---------------------------------------------------------------------------


class _BumpingFakeDb:
    """A fake ``Database`` whose single claim ``write`` schedules a
    generation bump via ``call_soon`` before returning the claimed job.

    ``call_soon`` fires only once the loop regains control, so the fence
    capture inside ``run_claimed_job`` -- which must run with NO ``await``
    between the claim returning and ``FencedWriter(db, db.generation)`` --
    observes the PRE-bump generation. Any inserted ``await`` before that
    capture lets the callback run first and would observe the bumped value
    instead (the named mutant).

    NOTE: the test below only replays this invariant against a bare
    ``FencedWriter`` construction, never against the poller loop's own
    claim-to-run_job hand-off -- so it is empirically EQUIVALENT to a real
    mutant that inserts an ``await`` between the claim and ``run_job`` in
    ``run_current_obs_poller`` (verified: that mutant leaves the entire
    suite green, this test included). See
    ``test_f2_poller_loop_hands_off_to_run_job_before_any_bump_can_land``
    below for the discriminator that samples the real call site.
    """

    def __init__(self, job: Job) -> None:
        self._job = job
        self.generation = 5

    async def write(self, fn: object) -> Job:
        loop = asyncio.get_running_loop()
        loop.call_soon(self._bump)
        return self._job

    def _bump(self) -> None:
        self.generation = 6


def test_f_fence_capture_observes_the_pre_bump_generation() -> None:
    job = Job(
        id=1,
        type="fetch_current_obs",
        site_id=1,
        job_key="curobs:1",
        payload={"station_id": 1},
        status="running",
        retry_count=0,
        max_retries=5,
    )
    fake_db = _BumpingFakeDb(job)

    captured: dict[str, int] = {}
    real_fenced_writer = FencedWriter

    class _RecordingWriter(real_fenced_writer):  # type: ignore[misc]
        def __init__(self, db: object, generation: int) -> None:
            captured["generation"] = generation
            super().__init__(db, generation)

    async def run() -> None:
        claimed = await fake_db.write(claim_next_job)
        _RecordingWriter(fake_db, fake_db.generation)
        assert claimed is job

    asyncio.run(run())
    assert captured["generation"] == 5  # pre-bump, not the post-bump 6


class _StopPoller(Exception):
    """Sentinel to unwind the poller's ``while True`` after one claim."""


class _PollerFakeDb:
    """A fake ``Database`` whose single claim ``write`` schedules a
    generation bump via ``call_soon`` -- same rendezvous shape as
    ``_BumpingFakeDb`` above -- then drives ``run_current_obs_poller``
    itself, so ``run_job`` observes whatever generation is live at its own
    call site instead of one hand-copied by the test.
    """

    def __init__(self, job: Job) -> None:
        self._job = job
        self._claimed = False
        self.generation = 5

    async def write(self, fn: object) -> Job | None:
        if self._claimed:
            return None
        self._claimed = True
        loop = asyncio.get_running_loop()
        loop.call_soon(self._bump)
        return self._job

    def _bump(self) -> None:
        self.generation = 6


def test_f2_poller_loop_hands_off_to_run_job_before_any_bump_can_land() -> None:
    """F discriminator: drives the real ``run_current_obs_poller`` loop,
    unlike ``test_f_fence_capture_observes_the_pre_bump_generation`` above,
    which only replays the invariant against a bare ``FencedWriter`` and
    never calls the poller loop at all -- so it cannot see a regression at
    the loop's own claim-to-run_job hand-off, only at the harness's stand-in
    for it.

    Mutant: an ``await`` inserted between the claim returning and
    ``await run_job(...)`` in ``run_current_obs_poller`` lets the scheduled
    bump land first; ``run_job`` would then observe generation 6, not 5.
    """
    job = Job(
        id=1,
        type="fetch_current_obs",
        site_id=1,
        job_key="curobs:1",
        payload={"station_id": 1},
        status="running",
        retry_count=0,
        max_retries=5,
    )
    fake_db = _PollerFakeDb(job)
    observed: dict[str, int] = {}

    async def _recording_run_job(db: object, claimed_job: object, *, lane: str) -> None:
        observed["generation"] = fake_db.generation
        raise _StopPoller

    async def run() -> None:
        with pytest.raises(_StopPoller):
            await run_current_obs_poller(fake_db, run_job=_recording_run_job)

    asyncio.run(run())
    assert observed["generation"] == 5  # pre-bump, not the post-bump 6


def test_f3_real_run_claimed_jobs_fence_capture_observes_the_pre_bump_generation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """F real-runner discriminator: neither ``test_f`` (a bare ``FencedWriter``
    construction) nor ``test_f2`` (a fake ``Database``) ever calls the REAL
    ``wxverify.worker.processor.run_claimed_job`` -- so an ``await`` inserted
    at the very top of the real function, before its own fence capture,
    would pass every other test in this module. This test drives a real
    ``Database``, a real claimed job, and the real ``run_claimed_job``.

    Construction: schedule a generation bump via ``loop.call_soon`` right
    after the claim's ``db.write`` returns and before ``run_claimed_job`` is
    awaited -- the callback only runs once the surrounding task actually
    suspends, which the correct code does not do until AFTER
    ``FencedWriter(db, db.generation)`` has already captured the
    pre-bump value (matching the real function's own claim -- see its
    docstring -- that no await separates the claim from that capture).

    Mutant: insert ``await asyncio.sleep(0)`` before the fence capture in
    ``run_claimed_job`` -- the scheduled bump would land first, so the
    ``FencedWriter`` the recording subclass observes would be constructed
    with the POST-bump generation, and ``captured["generation"] ==
    pre_bump_generation`` would fail.
    """

    def _online_body() -> bytes:
        return json.dumps(
            {
                "observations": [
                    {
                        "obsTimeUtc": "2026-07-10T11:55:00Z",
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

    async def _fake_fetch_current_observation(
        pws_station_id: str,
        api_key: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 10.0,
    ) -> httpx.Response:
        # A genuine suspension point downstream of the claim: guarantees the
        # scheduled bump below has a chance to land before run_claimed_job
        # returns, so the test cannot pass merely because nothing ever
        # yielded.
        await asyncio.sleep(0)
        return httpx.Response(
            200,
            content=_online_body(),
            headers={"content-type": "application/json"},
            request=httpx.Request(
                "GET", "https://api.weather.com/v2/pws/observations/current"
            ),
        )

    monkeypatch.setattr(
        "wxverify.worker.processor.fetch_current_observation",
        _fake_fetch_current_observation,
    )

    captured: dict[str, int] = {}
    real_fenced_writer = FencedWriter

    class _RecordingWriter(real_fenced_writer):  # type: ignore[misc]
        def __init__(self, db: object, generation: int) -> None:
            captured["generation"] = generation
            super().__init__(db, generation)

    monkeypatch.setattr("wxverify.worker.processor.FencedWriter", _RecordingWriter)

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        _seed_current_obs_job(conn, site_id, station_id)

        job = await db.write(claim_next_current_obs_job)
        assert job is not None

        loop = asyncio.get_running_loop()
        pre_bump_generation = db.generation

        def _bump() -> None:
            db._generation += 1  # noqa: SLF001 -- simulates an import replacing the db

        loop.call_soon(_bump)
        await run_claimed_job(db, job, lane="current_obs")

        assert db.generation == pre_bump_generation + 1, (
            "the bump must actually have landed during this run, or the "
            "test proves nothing"
        )
        assert captured["generation"] == pre_bump_generation

    asyncio.run(run())


# ---------------------------------------------------------------------------
# W: single-executor write-set invariant
# ---------------------------------------------------------------------------

_ALLOWED_CURRENT_OBS_TABLES = frozenset(
    {
        "station_current_obs",
        "station_poll_state",
        "api_budget",
        "domain_backoffs",
        "jobs",
        "runtime_state",
    }
)


def test_w_current_obs_lane_write_set_stays_within_its_allowlist(
    tmp_path: Path,
) -> None:
    db = _init_tmp_db(tmp_path)
    conn = db._conn  # noqa: SLF001
    site_id = _seed_site(conn)
    station_id = _seed_station(conn, site_id)

    written_tables: set[str] = set()

    def _authorizer(
        action_code: int,
        arg1: str | None,
        arg2: str | None,
        dbname: str | None,
        source: str | None,
    ) -> int:
        if (
            action_code
            in (
                sqlite3.SQLITE_INSERT,
                sqlite3.SQLITE_UPDATE,
                sqlite3.SQLITE_DELETE,
            )
            and arg1 is not None
        ):
            written_tables.add(arg1)
        return sqlite3.SQLITE_OK

    conn.set_authorizer(_authorizer)
    try:
        reservation = _reserve_current_obs_call(conn, site_id, station_id)

        persist_poll_result(
            conn,
            site_id,
            station_id,
            PollOutcome(
                Health.ONLINE,
                obs=_online_obs(),
                obs_instant="2026-07-10T11:55:00Z",
            ),
        )
        persist_poll_result(conn, site_id, station_id, PollOutcome(Health.OFFLINE))
        persist_poll_result(conn, site_id, station_id, PollOutcome(Health.TERMINAL))
        persist_poll_result(conn, site_id, station_id, PollOutcome(Health.TRANSIENT))

        _persist_poll_result_and_refund(
            conn, site_id, station_id, PollOutcome(Health.TRANSIENT), reservation
        )

        response = httpx.Response(
            429,
            request=httpx.Request(
                "GET", "https://api.weather.com/v2/pws/observations/current"
            ),
        )
        _record_current_obs_backoff(
            conn, site_id, station_id, response, PollOutcome(Health.TRANSIENT)
        )

        _enqueue_and_claim(conn, stamp_heartbeat=True)

        _seed_current_obs_job(conn, site_id, station_id, key="curobs:disposition")
        job = claim_next_current_obs_job(conn)
        assert job is not None
        _complete_and_continue(conn, job.id, None)

        _seed_current_obs_job(conn, site_id, station_id, key="curobs:defer")
        deferred = claim_next_current_obs_job(conn)
        assert deferred is not None
        defer_job(conn, deferred.id, "2099-01-01T00:00:00Z")

        _seed_current_obs_job(conn, site_id, station_id, key="curobs:cancel")
        cancelled = claim_next_current_obs_job(conn)
        assert cancelled is not None
        complete(conn, cancelled.id)
    finally:
        conn.set_authorizer(None)

    assert written_tables <= _ALLOWED_CURRENT_OBS_TABLES


def _online_obs() -> Any:
    from wxverify.obs.pws_adapter import CurrentObservation

    return CurrentObservation(
        obs_time_utc="2026-07-10T11:55:00Z",
        temp=20.0,
        humidity=None,
        dewpt=None,
        wind_speed=None,
        wind_gust=None,
        wind_dir=None,
        pressure=None,
        precip_rate=None,
        precip_total=None,
        uv=None,
        neighborhood=None,
    )


# ---------------------------------------------------------------------------
# DL3: current-obs provider deadline
# ---------------------------------------------------------------------------


def test_dl3_current_obs_deadline_is_not_refunded_and_fails_transiently(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from wxverify.obs.pws_adapter import ProviderDeadlineExceeded

    async def _raise_deadline(*args: object, **kwargs: object) -> None:
        raise ProviderDeadlineExceeded("provider call exceeded its deadline")

    monkeypatch.setattr(
        "wxverify.worker.processor.fetch_current_observation", _raise_deadline
    )

    async def run() -> None:
        db = _init_tmp_db(tmp_path)
        conn = db._conn  # noqa: SLF001
        site_id = _seed_site(conn)
        station_id = _seed_station(conn, site_id)
        before = conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(calls), 0) AS c"
            " FROM api_budget WHERE source = 'weathercom'"
        ).fetchone()

        _seed_current_obs_job(conn, site_id, station_id)
        job = await db.write(claim_next_current_obs_job)
        assert job is not None

        await run_claimed_job(db, job, lane="current_obs")

        after = conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(calls), 0) AS c"
            " FROM api_budget WHERE source = 'weathercom'"
        ).fetchone()
        # DL3: ProviderDeadlineExceeded is a TimeoutError, not one of the
        # refundable transport errors -- the reserved call is NOT refunded.
        assert not is_refundable_transport_error(ProviderDeadlineExceeded("x"))
        # The reserve creates the api_budget row on first use; the point of
        # this assertion is the CALL COUNT, not the row count.
        assert after["c"] == before["c"] + 1

        poll_state = conn.execute(
            "SELECT health_state FROM station_poll_state WHERE station_id = ?",
            (station_id,),
        ).fetchone()
        assert poll_state is not None
        assert poll_state["health_state"] == "transient"

        row = conn.execute(
            "SELECT status, retry_count FROM jobs WHERE id = ?", (job.id,)
        ).fetchone()
        assert row["status"] in ("pending", "failed")
        assert row["retry_count"] == 1

        from wxverify.worker.station_pacing import weathercom_call_lock

        assert weathercom_call_lock().locked() is False

    asyncio.run(run())
