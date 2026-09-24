"""Shared weather.com call lock (§6.3.10) and the add-station route's
bounded wait (§17 item 11) -- commit 4 of the 2026-09-23
station-health-and-retries plan. Covers K2-K5d of plan §14.2.

Out of scope here (plan §14.2, commit 4 row of §16.1):

- K1 (the limit across lanes) and the shared-lock assertion inside the
  supervisor's C4 stale-generation case need the separate current-obs lane
  introduced in commit 5 (``run_claimed_job``, ``run_current_obs_poller``),
  which does not exist at this commit.
- DL1 and DL2 (the provider-call deadline) live in ``test_provider_deadline.py``;
  DL3-DL5 need commit 5's claimed-job lane and land with it.

Adaptation from the plan's literal wording: every K2/K5 case that the plan
describes via ``run_claimed_job`` (a commit-5 name) is instead driven
through today's entry points -- ``processor._fetch_obs``,
``catchup._fetch_missing_station_history``,
``backfill.fetch_station_history_window``, ``processor._fetch_current_obs``
and the add-station route -- because ``run_claimed_job`` is not introduced
until commit 5 (plan §16.1 row 5, and confirmed absent from
``worker/processor.py`` at this commit). The plan's own §14.2 test-sm3
precedent (``test_disabled_site_station_does_not_starve_main_claim``)
documents the same substitution for D0. The plan's §6.3.10 line-number
table (e.g. ``processor.py:526``, ``:545``) predates commits 1-3 renumbering
the file; this file locates every call site by content (diffed against the
worktree) rather than by those line numbers, and no anchor mismatch beyond
the run_claimed_job substitution was found.

Convention: every provider call is faked at its
``wxverify.worker.<module>.fetch_*`` (or ``wxverify.api.routes.stations.
validate_station``) import site -- matching ``test_sm4_backoff.py`` and
``test_generation_fence.py`` -- rather than routed through
``httpx.MockTransport``, because the lock's scope is the point in question,
not the HTTP layer. ``asyncio.run(run())`` drives every non-route case; no
pytest-asyncio. Route cases use ``TestClient`` + ``client.portal``,
following ``test_generation_fence.py:243-300``, with the plan's
hang-guard/teardown discipline (§14.2, "Add-station route bounded wait")
so a failed guard reports instead of hanging.

Every ``db.write``/``FencedWriter.write`` call in this file runs its
callback in a worker thread (``connection.py`` -> ``run_to_completion`` ->
``asyncio.to_thread``), where ``asyncio.get_running_loop()`` raises. Every
test below therefore captures ``lock = weathercom_call_lock()`` on the
event loop before driving any code, and every wrapper that can run inside a
write reads ``lock.locked()`` on that captured object, never
``weathercom_call_lock()`` itself.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from wxverify import config
from wxverify.api.app import create_app
from wxverify.db.connection import Database, FencedWriter, close_db
from wxverify.obs.pws_adapter import PwsObservation, PwsStation
from wxverify.worker import processor as processor_module
from wxverify.worker.backfill import fetch_station_history_window
from wxverify.worker.catchup import (
    CatchupSite,
    _fetch_missing_station_history,  # noqa: PLC2701 - exercising directly, no claimed job at this commit
)
from wxverify.worker.control import JobDeferred
from wxverify.worker.processor import (
    _fetch_current_obs,  # noqa: PLC2701
    _fetch_obs,  # noqa: PLC2701
)
from wxverify.worker.station_pacing import acquire_within, weathercom_call_lock

# ---------------------------------------------------------------------------
# Shared helpers -- synthetic placeholder data only
# ---------------------------------------------------------------------------

_HANG_GUARD_SECONDS = 5.0
_WEATHER_COM_CURRENT_URL = httpx.URL(
    "https://api.weather.com/v2/pws/observations/current"
)


async def _idle_worker(db: object) -> None:
    await asyncio.Event().wait()


def _seed_site(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute(
        "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone, "
        "enabled) VALUES (?, 40.0, -105.0, 900.0, 'UTC', 1)",
        (name,),
    )
    site_id = cur.lastrowid
    assert site_id is not None
    return int(site_id)


def _seed_station(conn: sqlite3.Connection, site_id: int, pws_station_id: str) -> int:
    cur = conn.execute(
        "INSERT INTO stations (site_id, pws_station_id, lat, lon, dem_elevation_m) "
        "VALUES (?, ?, 40.0, -105.0, 900.0)",
        (site_id, pws_station_id),
    )
    station_id = cur.lastrowid
    assert station_id is not None
    return int(station_id)


def _online_response() -> httpx.Response:
    body = json.dumps(
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
    return httpx.Response(
        200,
        content=body,
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", _WEATHER_COM_CURRENT_URL),
    )


def _rate_limited_response() -> httpx.Response:
    return httpx.Response(
        429, content=b"", request=httpx.Request("GET", _WEATHER_COM_CURRENT_URL)
    )


async def _release_lock(lock: asyncio.Lock) -> None:
    """Async wrapper: ``Lock.release`` is sync, and ``BlockingPortal.call``
    only awaits a coroutine function (plan §14.2, add-station fixture note).
    """
    lock.release()


async def _get_app_lock() -> asyncio.Lock:
    return weathercom_call_lock()


def _open_db_path(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _init_route_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[list[str], threading.Event]:
    """Common wiring for every add-station route test in this file: a fresh
    tmp DB, the weathercom key, a fake ``validate_station`` that records its
    calls and signals ``entered``, a no-network fake elevation lookup, and
    an idle worker (network guard).
    """
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    config.standalone_origin = None
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")

    validate_calls: list[str] = []
    entered = threading.Event()

    async def _fake_validate_station(station_id: str, api_key: str) -> PwsStation:
        validate_calls.append(station_id)
        entered.set()
        return PwsStation(station_id=station_id, lat=40.1, lon=-104.9)

    async def _fake_lookup_elevation_m(lat: float, lon: float) -> float:
        return 900.0

    monkeypatch.setattr(
        "wxverify.api.routes.stations.validate_station", _fake_validate_station
    )
    monkeypatch.setattr(
        "wxverify.api.routes.stations.lookup_elevation_m", _fake_lookup_elevation_m
    )
    monkeypatch.setattr("wxverify.api.app.run_worker", _idle_worker)
    return validate_calls, entered


def _create_site(client: TestClient, headers: dict[str, str], name: str) -> int:
    site = client.post(
        "/api/sites",
        json={
            "name": name,
            "forecast_lat": 40.0,
            "forecast_lon": -105.0,
            "elevation_m": 900.0,
            "timezone": "UTC",
        },
        headers=headers,
    )
    assert site.status_code == 200
    return int(site.json()["id"])


# ---------------------------------------------------------------------------
# K2 -- scope: every call site holds the lock while calling the provider
# ---------------------------------------------------------------------------


def test_k2_lock_held_at_history_catchup_and_backfill_call_sites(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """K2 (scope): history (``_fetch_obs``), catchup
    (``_fetch_missing_station_history``) and backfill
    (``fetch_station_history_window``), each driven through its existing
    entry point once.

    Mutant (backfill): drop the lock from the backfill call site --
    ``async with weathercom_call_lock():`` replaced by no lock at all.
    Killed by ``locked_at["backfill"]`` being False instead of True.
    """
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")
    db = Database(str(db_path))

    async def _run() -> None:
        try:
            lock = weathercom_call_lock()
            locked_at: dict[str, bool] = {}

            history_site = _seed_site(db._conn, "History Site")  # noqa: SLF001
            _seed_station(db._conn, history_site, "ISTATION-HIST")
            catchup_site = _seed_site(db._conn, "Catchup Site")  # noqa: SLF001
            _seed_station(db._conn, catchup_site, "ISTATION-CATCH")
            backfill_site = _seed_site(db._conn, "Backfill Site")  # noqa: SLF001
            _seed_station(db._conn, backfill_site, "ISTATION-BACK")

            async def fake_history(
                station_id_arg: str,
                api_key: str,
                *,
                hours: int = 0,
                timezone: str | None = None,
                client: httpx.AsyncClient | None = None,
            ) -> list[PwsObservation]:
                locked_at["history"] = lock.locked()
                return []

            async def fake_history_range_catchup(
                station_id_arg: str,
                api_key: str,
                *,
                window_start: str,
                window_end: str,
                timezone: str | None = None,
                client: httpx.AsyncClient | None = None,
            ) -> list[PwsObservation]:
                locked_at["catchup"] = lock.locked()
                return []

            async def fake_history_range_backfill(
                station_id_arg: str,
                api_key: str,
                *,
                window_start: str,
                window_end: str,
                timezone: str | None = None,
                client: httpx.AsyncClient | None = None,
            ) -> list[PwsObservation]:
                locked_at["backfill"] = lock.locked()
                return []

            monkeypatch.setattr(
                "wxverify.worker.processor.fetch_hourly_history", fake_history
            )
            monkeypatch.setattr(
                "wxverify.worker.catchup.fetch_hourly_history_range",
                fake_history_range_catchup,
            )
            monkeypatch.setattr(
                "wxverify.worker.backfill.fetch_hourly_history_range",
                fake_history_range_backfill,
            )

            await _fetch_obs(db, FencedWriter(db, db.generation), history_site)

            await _fetch_missing_station_history(
                db,
                FencedWriter(db, db.generation),
                CatchupSite(site_id=catchup_site, lat=40.0, lon=-105.0, timezone="UTC"),
                window_start="2026-06-01T00:00:00Z",
                window_end="2026-06-01T06:00:00Z",
            )

            await fetch_station_history_window(
                db,
                FencedWriter(db, db.generation),
                backfill_site,
                window_start="2026-06-01T00:00:00Z",
                window_end="2026-06-01T06:00:00Z",
                timezone="UTC",
            )

            assert locked_at == {
                "history": True,
                "catchup": True,
                "backfill": True,
            }
        finally:
            db.close()

    asyncio.run(_run())


def test_k2_lock_held_at_current_obs_call_site_and_outcome_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """K2 (scope): the current-obs call site (``_fetch_current_obs``) and
    its two outcome-write wrappers -- ``persist_poll_result`` for a 200
    reply, ``processor._record_current_obs_backoff`` for a 429 -- each
    recorded at entry.

    Mutant: move the current-obs outcome ``write_after_reservation`` out of
    the ``async with``. Killed by ``locked_at["persist_200"]`` (or
    ``["backoff_429"]``) being False instead of True.
    """
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", "secret-weather")
    db = Database(str(db_path))

    async def _run() -> None:
        try:
            lock = weathercom_call_lock()
            locked_at: dict[str, bool] = {}

            site_a = _seed_site(db._conn, "Current A")  # noqa: SLF001
            station_a = _seed_station(db._conn, site_a, "ISTATION-CUR-A")
            site_b = _seed_site(db._conn, "Current B")  # noqa: SLF001
            station_b = _seed_station(db._conn, site_b, "ISTATION-CUR-B")

            async def fake_current_obs_200(
                pws_station_id: str,
                api_key: str,
                *,
                client: httpx.AsyncClient | None = None,
                timeout_seconds: float = 10.0,
            ) -> httpx.Response:
                locked_at["fetch_200"] = lock.locked()
                return _online_response()

            async def fake_current_obs_429(
                pws_station_id: str,
                api_key: str,
                *,
                client: httpx.AsyncClient | None = None,
                timeout_seconds: float = 10.0,
            ) -> httpx.Response:
                locked_at["fetch_429"] = lock.locked()
                return _rate_limited_response()

            real_persist = processor_module.persist_poll_result

            def spy_persist(
                conn: sqlite3.Connection,
                site_id_arg: int,
                station_id_arg: int,
                outcome: object,
            ) -> None:
                locked_at["persist_200"] = lock.locked()
                real_persist(conn, site_id_arg, station_id_arg, outcome)

            real_backoff = processor_module._record_current_obs_backoff

            def spy_backoff(
                conn: sqlite3.Connection,
                site_id_arg: int,
                station_id_arg: int,
                response: httpx.Response,
                outcome: object,
            ) -> str | None:
                locked_at["backoff_429"] = lock.locked()
                return real_backoff(
                    conn, site_id_arg, station_id_arg, response, outcome
                )

            monkeypatch.setattr(
                "wxverify.worker.processor.fetch_current_observation",
                fake_current_obs_200,
            )
            monkeypatch.setattr(
                "wxverify.worker.processor.persist_poll_result", spy_persist
            )
            await _fetch_current_obs(
                db, FencedWriter(db, db.generation), site_a, station_a
            )

            monkeypatch.setattr(
                "wxverify.worker.processor.fetch_current_observation",
                fake_current_obs_429,
            )
            monkeypatch.setattr(
                "wxverify.worker.processor._record_current_obs_backoff", spy_backoff
            )
            with pytest.raises(JobDeferred):
                await _fetch_current_obs(
                    db, FencedWriter(db, db.generation), site_b, station_b
                )

            assert locked_at == {
                "fetch_200": True,
                "persist_200": True,
                "fetch_429": True,
                "backoff_429": True,
            }
        finally:
            db.close()

    asyncio.run(_run())


def test_k2_lock_held_at_add_station_route(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """K2 (scope): the add-station route's call site. The route's lock is
    captured on the app loop through ``client.portal.call`` (as K5a's
    fixture does), and read from inside the fake ``validate_station`` --
    which itself runs on the app loop, so no cross-thread read is needed
    here.
    """
    validate_calls, entered = _init_route_fixture(tmp_path, monkeypatch)
    locked_at: dict[str, bool] = {}
    lock_box: dict[str, asyncio.Lock] = {}

    async def _fake_validate_station_locked(
        station_id: str, api_key: str
    ) -> PwsStation:
        validate_calls.append(station_id)
        entered.set()
        locked_at["validate_station"] = lock_box["lock"].locked()
        return PwsStation(station_id=station_id, lat=40.1, lon=-104.9)

    monkeypatch.setattr(
        "wxverify.api.routes.stations.validate_station",
        _fake_validate_station_locked,
    )

    app = create_app(root_path="")
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        site_id = _create_site(client, headers, "K2 Route Site")

        lock_box["lock"] = client.portal.call(_get_app_lock)

        response = client.post(
            f"/api/sites/{site_id}/stations",
            json={"pws_station_id": "ISTATION-K2-ROUTE"},
            headers=headers,
        )
        assert response.status_code == 200

    assert locked_at == {"validate_station": True}


# ---------------------------------------------------------------------------
# K3 -- FIFO canary
# ---------------------------------------------------------------------------


def test_k3_fifo_canary_pins_stdlib_wakeup_order() -> None:
    """K3: hold the lock, queue three waiters in order, release, and create
    a fourth acquirer immediately after the release. Acquisition order is
    ``[0, 1, 2, late]`` -- probed on Python 3.13.14 as ``[0, 1, 2, 99]``
    (plan §14.2). Pins the stdlib property §6.3.10's fairness note relies
    on: a Python upgrade that changes ``asyncio.Lock``'s FIFO wakeup order
    fails here rather than only surfacing as a silent fairness regression.
    """

    async def run() -> list[int]:
        async with asyncio.timeout(_HANG_GUARD_SECONDS):
            lock = asyncio.Lock()
            order: list[int] = []
            await lock.acquire()

            async def waiter(n: int) -> None:
                async with lock:
                    order.append(n)

            tasks = [asyncio.ensure_future(waiter(i)) for i in range(3)]
            for _ in range(3):
                await asyncio.sleep(0)  # let each waiter queue on the lock

            lock.release()
            late = asyncio.ensure_future(waiter(99))

            await asyncio.gather(*tasks, late)
            return order

    assert asyncio.run(run()) == [0, 1, 2, 99]


# ---------------------------------------------------------------------------
# K4 -- loop binding
# ---------------------------------------------------------------------------


def test_k4_loop_binding_two_sequential_runs_never_raise_and_differ() -> None:
    """K4 (loop binding): two sequential ``asyncio.run`` calls each contend
    ``weathercom_call_lock()`` (a holder task and a waiter). Neither raises,
    and the two lock objects differ.

    Mutant: a plain module-level ``asyncio.Lock()``, bound to the first
    loop that contends it. Killed by the second ``asyncio.run`` call
    raising ``RuntimeError(... is bound to a different event loop)`` instead
    of returning.
    """

    async def _run() -> asyncio.Lock:
        async with asyncio.timeout(_HANG_GUARD_SECONDS):
            lock = weathercom_call_lock()

            async def holder() -> None:
                async with lock:
                    await asyncio.sleep(0.02)

            async def waiter() -> None:
                await asyncio.sleep(0.005)
                async with lock:
                    pass

            await asyncio.gather(holder(), waiter())
            return lock

    lock1 = asyncio.run(_run())
    lock2 = asyncio.run(_run())
    assert lock1 is not lock2


# ---------------------------------------------------------------------------
# K5a -- timeout (unit case + the route)
# ---------------------------------------------------------------------------


def test_k5a_acquire_within_times_out_without_taking_the_lock() -> None:
    """K5a unit case: ``acquire_within`` returns False while queued, without
    acquiring, and True once the holder releases. The outer
    ``asyncio.timeout`` is the hang guard: if the helper loses its own
    bound, this fails fast with ``TimeoutError`` instead of hanging the
    run (no pytest timeout is configured).

    Mutants killed by the outer guard or the assertions below, all on this
    one case: an unbounded ``await lock.acquire()`` in the helper (the
    guard fires); ``lock.release()`` on the timeout path
    (``lock.locked()`` is False after the first call).
    """

    async def run() -> None:
        async with asyncio.timeout(1):
            lock = asyncio.Lock()
            await lock.acquire()

            got = await acquire_within(lock, 0.05)
            assert got is False
            assert lock.locked() is True, (
                "a False result must leave the lock with its actual holder"
            )

            lock.release()
            got2 = await acquire_within(lock, 0.1)
            assert got2 is True

    asyncio.run(run())


def test_k5a_add_station_route_returns_503_after_the_bounded_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """K5a route case: with the wait patched to 0.2 s, a request that finds
    the lock already held gets a 503 after the bound, makes no provider
    call, and leaves the budget and the stations table untouched.

    Mutants, each killed by the named assertion below:
    - an unbounded ``await lock.acquire()`` (the hang guard: the thread is
      still alive after ``join(5)``);
    - the reserve moved above the acquisition (the budget-row assertion);
    - calling ``validate_station`` after a False result
      (``validate_calls == []``, ``not entered.is_set()``);
    - ``TimeoutError`` not caught, giving a 500 (the status-code assertion);
    - ``lock.release()`` on the timeout path (``lock.locked() is True``
      after the response);
    - a different message text (the body-equality assertion).
    """
    validate_calls, entered = _init_route_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "wxverify.api.routes.stations.ADD_STATION_CALL_WAIT_SECONDS", 0.2
    )

    app = create_app(root_path="")
    held = False
    outcome: dict[str, Any] = {}
    threads: list[threading.Thread] = []
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        site_id = _create_site(client, headers, "K5a Site")

        lock = client.portal.call(_get_app_lock)
        client.portal.call(lock.acquire)
        held = True
        try:

            def _drive() -> None:
                try:
                    outcome["response"] = client.post(
                        f"/api/sites/{site_id}/stations",
                        json={"pws_station_id": "ISTATION-K5A"},
                        headers=headers,
                    )
                except Exception as exc:  # noqa: BLE001 - captured, not swallowed
                    outcome["exception"] = exc

            thread = threading.Thread(target=_drive)
            threads.append(thread)
            thread.start()
            thread.join(_HANG_GUARD_SECONDS)
            assert not thread.is_alive(), (
                "the request thread never finished within the hang guard"
            )

            response = outcome.get("response")
            assert response is not None, f"the request raised instead: {outcome!r}"
            assert response.status_code == 503
            assert response.json() == {
                "error": (
                    "Weather provider busy; station was not added. Try again shortly."
                )
            }
            assert validate_calls == []
            assert not entered.is_set()
            assert lock.locked() is True, (
                "the route's failure path must release nothing it never held"
            )
        finally:
            if held:
                client.portal.call(_release_lock, lock)
                held = False
            for t in threads:
                t.join(_HANG_GUARD_SECONDS)

    live_conn = _open_db_path(config.db_path)
    try:
        budget = live_conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(calls), 0) AS calls "
            "FROM api_budget WHERE source='weathercom'"
        ).fetchone()
        station_count = live_conn.execute(
            "SELECT COUNT(*) AS n FROM stations WHERE site_id=?", (site_id,)
        ).fetchone()["n"]
    finally:
        live_conn.close()
    assert budget["n"] == 0
    assert budget["calls"] == 0
    assert station_count == 0


# ---------------------------------------------------------------------------
# K5b -- cancel while queued
# ---------------------------------------------------------------------------


def test_k5b_cancel_while_sole_waiter_releases_nothing() -> None:
    """K5b step 1: a holder holds the lock; a queued
    ``acquire_within(lock, 10)`` is cancelled. ``CancelledError`` propagates,
    and the lock -- never acquired by the cancelled waiter -- stays held by
    the original holder.

    Mutants: catch ``CancelledError`` in the helper and return False
    (``pytest.raises`` fails to observe it); ``lock.release()`` in a
    ``finally`` in the helper (``lock.locked()`` is False even though the
    real holder above never released).
    """

    async def run() -> None:
        async with asyncio.timeout(_HANG_GUARD_SECONDS):
            lock = asyncio.Lock()
            await lock.acquire()

            waiter = asyncio.create_task(acquire_within(lock, 10))
            await asyncio.sleep(0)
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            assert lock.locked() is True

    asyncio.run(run())


def test_k5b_cancel_at_hand_off_still_wakes_the_next_waiter() -> None:
    """K5b step 2 (hand-off): a holder releases and cancels waiter W with
    no ``await`` between, so the lock is handed to W and W is cancelled
    before it runs. W raises ``CancelledError``, and waiter B still returns
    True -- the ``locks.py:113-121`` re-wake §6.3.10 relies on.

    Mutant: ``asyncio.wait_for(asyncio.shield(lock.acquire()), timeout_s)``.
    The shielded inner acquire keeps the hand-off alive under the
    cancellation, so B times out instead of returning True within the
    outer guard.
    """

    async def run() -> None:
        async with asyncio.timeout(_HANG_GUARD_SECONDS):
            lock = asyncio.Lock()
            await lock.acquire()

            w = asyncio.create_task(acquire_within(lock, 10))
            b = asyncio.create_task(acquire_within(lock, 10))
            await asyncio.sleep(0)
            await asyncio.sleep(0)

            lock.release()
            w.cancel()

            with pytest.raises(asyncio.CancelledError):
                await w
            assert await b is True

    asyncio.run(run())


# ---------------------------------------------------------------------------
# K5c -- recovery
# ---------------------------------------------------------------------------


def test_k5c_recovery_after_a_timeout_the_next_request_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """K5c: with the wait patched to 0.2 s, hold the lock and POST (503);
    release; POST again (200). ``validate_station`` was called exactly
    once, and one station row exists with ``pws_station_id =
    'ISTATION-K5C'``.

    Mutant: an orphaned acquisition (the shielded form from K5b, or
    ``asyncio.wait``'s non-cancelling timeout form) takes the lock when the
    holder releases and never gives it back, so the second POST also
    returns 503 -- killed by the second status-code assertion.
    """
    validate_calls, entered = _init_route_fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(
        "wxverify.api.routes.stations.ADD_STATION_CALL_WAIT_SECONDS", 0.2
    )

    app = create_app(root_path="")
    held = False
    outcome: dict[str, Any] = {}
    threads: list[threading.Thread] = []
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        site_id = _create_site(client, headers, "K5c Site")

        lock = client.portal.call(_get_app_lock)
        client.portal.call(lock.acquire)
        held = True
        try:

            def _drive_first() -> None:
                try:
                    outcome["first"] = client.post(
                        f"/api/sites/{site_id}/stations",
                        json={"pws_station_id": "ISTATION-K5C"},
                        headers=headers,
                    )
                except Exception as exc:  # noqa: BLE001 - captured, not swallowed
                    outcome["first_exception"] = exc

            first_thread = threading.Thread(target=_drive_first)
            threads.append(first_thread)
            first_thread.start()
            first_thread.join(_HANG_GUARD_SECONDS)
            assert not first_thread.is_alive(), (
                "the first request thread never finished within the hang guard"
            )
            first = outcome.get("first")
            assert first is not None, f"the first request raised: {outcome!r}"
            assert first.status_code == 503

            client.portal.call(_release_lock, lock)
            held = False

            second = client.post(
                f"/api/sites/{site_id}/stations",
                json={"pws_station_id": "ISTATION-K5C"},
                headers=headers,
            )
            assert second.status_code == 200
        finally:
            if held:
                client.portal.call(_release_lock, lock)
                held = False
            for t in threads:
                t.join(_HANG_GUARD_SECONDS)

    assert validate_calls == ["ISTATION-K5C"]
    live_conn = _open_db_path(config.db_path)
    try:
        row = live_conn.execute(
            "SELECT COUNT(*) AS n FROM stations "
            "WHERE site_id=? AND pws_station_id='ISTATION-K5C'",
            (site_id,),
        ).fetchone()
    finally:
        live_conn.close()
    assert row["n"] == 1


# ---------------------------------------------------------------------------
# K5d -- waits, then proceeds
# ---------------------------------------------------------------------------


def test_k5d_add_station_route_waits_then_proceeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """K5d: with the default 30 s wait, hold the lock and send the POST.
    ``entered.wait(0.5)`` is False and the budget is unchanged while the
    request is still queued. Release: ``entered`` is set and the response
    is 200.

    Mutant: drop the lock from the route entirely. Killed by
    ``entered.wait(0.5)`` becoming True (``validate_station`` runs at once,
    before the external hold is ever released).
    """
    validate_calls, entered = _init_route_fixture(tmp_path, monkeypatch)

    app = create_app(root_path="")
    held = False
    outcome: dict[str, Any] = {}
    threads: list[threading.Thread] = []
    with TestClient(app) as client:
        csrf = client.get("/api/csrf").json()["csrf_token"]
        headers = {"Origin": "http://testserver", "X-CSRF-Token": csrf}
        site_id = _create_site(client, headers, "K5d Site")

        lock = client.portal.call(_get_app_lock)
        client.portal.call(lock.acquire)
        held = True
        try:

            def _drive() -> None:
                try:
                    outcome["response"] = client.post(
                        f"/api/sites/{site_id}/stations",
                        json={"pws_station_id": "ISTATION-K5D"},
                        headers=headers,
                    )
                except Exception as exc:  # noqa: BLE001 - captured, not swallowed
                    outcome["exception"] = exc

            thread = threading.Thread(target=_drive)
            threads.append(thread)
            thread.start()

            assert not entered.wait(0.5), (
                "validate_station must not run before the lock is released"
            )

            live_conn = _open_db_path(config.db_path)
            try:
                budget = live_conn.execute(
                    "SELECT COUNT(*) AS n, COALESCE(SUM(calls), 0) AS calls "
                    "FROM api_budget WHERE source='weathercom'"
                ).fetchone()
            finally:
                live_conn.close()
            assert budget["n"] == 0
            assert budget["calls"] == 0

            client.portal.call(_release_lock, lock)
            held = False

            thread.join(_HANG_GUARD_SECONDS)
            assert not thread.is_alive(), (
                "the request thread never finished within the hang guard"
            )
            assert entered.is_set()
            response = outcome.get("response")
            assert response is not None, f"the request raised instead: {outcome!r}"
            assert response.status_code == 200
        finally:
            if held:
                client.portal.call(_release_lock, lock)
                held = False
            for t in threads:
                t.join(_HANG_GUARD_SECONDS)

    assert validate_calls == ["ISTATION-K5D"]
