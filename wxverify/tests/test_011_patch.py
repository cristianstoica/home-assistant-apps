"""Tests for wxverify 0.1.1 patch — bugs 1–4."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from starlette.responses import Response

from wxverify import config
from wxverify.api.csrf import issue_csrf_pair, set_csrf_cookie
from wxverify.api.ingress import IngressPathMiddleware
from wxverify.collection.budget import Reservation, refund_budget, reserve_budget
from wxverify.core.timeutil import isoformat_utc, utc_now
from wxverify.db.connection import (
    FencedWriter,
    StaleGenerationError,
    close_db,
    get_db,
    init_db,
)
from wxverify.db.queue import (
    FailDisposition,
    Job,
    claim_next_job,
    enqueue_if_absent,
    fail,
)
from wxverify.feeds.seam import CostEstimate, FetchResult
from wxverify.provider_ops import enqueue_fetch_for_feed
from wxverify.worker.control import JobCancelled, JobDeferred
from wxverify.worker.domain_backoff import record_http_backoff
from wxverify.worker.feed_fetch import (
    BackoffActive,
    fetch_feed_once,
    fetch_feed_retry_floor_seconds,
)
from wxverify.worker.processor import dispatch, run_worker
from wxverify.worker.scheduler import scheduler_tick

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _init_tmp_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001


def _insert_site(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m, timezone)
            VALUES ('PatchTest', 47.0, 25.0, 900.0, 'UTC')
            """
        ).lastrowid
    )


def _open_meteo_feed_id(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "SELECT id FROM feeds"
            " WHERE source='open-meteo' AND is_virtual=0"
            " ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )


def _google_feed_id(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "SELECT id FROM feeds WHERE source='google' ORDER BY id LIMIT 1"
        ).fetchone()["id"]
    )


def _subscribe(conn: sqlite3.Connection, site_id: int, feed_id: int) -> None:
    conn.execute(
        "INSERT INTO site_feed_state (site_id, feed_id, enabled, error_count)"
        " VALUES (?, ?, 1, 0)",
        (site_id, feed_id),
    )


def _index_exists(conn: sqlite3.Connection, name: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?", (name,)
        ).fetchone()
        is not None
    )


def _make_job(
    job_type: str = "fetch_feed",
    site_id: int = 1,
    job_id: int = 1,
    retry_count: int = 0,
    max_retries: int = 5,
) -> Job:
    return Job(
        id=job_id,
        type=job_type,
        site_id=site_id,
        job_key="test-key",
        payload={},
        status="running",
        retry_count=retry_count,
        max_retries=max_retries,
    )


class _FakeDb:
    """Minimal shim for worker loop logging tests (passes None as conn)."""

    generation = 0

    async def write(self, fn):  # type: ignore[no-untyped-def]
        return fn(None)

    async def read(self, fn):  # type: ignore[no-untyped-def]
        return fn(None)

    async def write_fenced(self, fn, *, generation):  # type: ignore[no-untyped-def]
        return fn(None)


class _WriteCountDb:
    """Spy that wraps a real Database and counts write calls."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.count = 0

    @property
    def generation(self) -> int:
        return self._inner.generation  # type: ignore[no-any-return]

    async def write(self, fn):  # type: ignore[no-untyped-def]
        self.count += 1
        return await self._inner.write(fn)

    async def read(self, fn):  # type: ignore[no-untyped-def]
        return await self._inner.read(fn)

    async def write_fenced(self, fn, *, generation):  # type: ignore[no-untyped-def]
        self.count += 1
        return await self._inner.write_fenced(fn, generation=generation)


class _GenerationFenceDb:
    """Fake Database whose write_fenced enforces the same generation check
    as the real one, with a `.generation` a test can bump mid-loop to
    simulate a replace_from landing between a job's claim and its outcome
    write -- without any real concurrency or a second sqlite file."""

    def __init__(self) -> None:
        self.generation = 0

    async def write(self, fn):  # type: ignore[no-untyped-def]
        return fn(None)

    async def read(self, fn):  # type: ignore[no-untyped-def]
        return fn(None)

    async def write_fenced(self, fn, *, generation):  # type: ignore[no-untyped-def]
        if generation != self.generation:
            raise StaleGenerationError(generation, self.generation)
        return fn(None)


class _StopLoop(Exception):
    pass


def _claim_once(job: Job):  # type: ignore[no-untyped-def]
    """Return a claim_next_job stub: yields *job* once then raises _StopLoop."""
    calls: list[int] = []

    def claim(conn: sqlite3.Connection) -> Job | None:
        calls.append(1)
        if len(calls) == 1:
            return job
        raise _StopLoop()

    return claim


class _FakeClock:
    """Mutable clock for wxverify.db.queue's utc_now/isoformat_utc.

    isoformat_utc's real no-arg branch resolves utc_now() against
    core.timeutil's own namespace, not queue.py's -- so patching queue.py's
    utc_now alone would leave claim_next_job's bare isoformat_utc() call
    (the value compared against next_attempt_at) reading the real wall
    clock. Both names are patched together, mirroring the pattern already
    used for current_obs/domain_backoff clock freezing.
    """

    def __init__(self, start: datetime) -> None:
        self.now = start

    def utc_now(self) -> datetime:
        return self.now

    def isoformat_utc(self, value: datetime | None = None) -> str:
        dt = value if value is not None else self.now
        return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def patch(self, monkeypatch: pytest.MonkeyPatch, module: str) -> None:
        monkeypatch.setattr(f"{module}.utc_now", self.utc_now)
        monkeypatch.setattr(f"{module}.isoformat_utc", self.isoformat_utc)


def _patch_worker_infra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Silence heartbeats, scheduler, and purge in worker loop tests."""
    monkeypatch.setattr(
        "wxverify.worker.processor.set_runtime_state_now", lambda c, k: None
    )
    monkeypatch.setattr("wxverify.worker.processor.scheduler_tick", lambda c: None)
    monkeypatch.setattr(
        "wxverify.worker.processor.purge_failed_jobs_older_than", lambda c, h: None
    )


# ---------------------------------------------------------------------------
# IngressPathMiddleware
# ---------------------------------------------------------------------------


async def _echo_root_path(scope: Any, receive: Any, send: Any) -> None:
    body = (scope.get("root_path") or "").encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain")],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def _get_via_middleware(
    *,
    client: tuple[str, int],
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    app = IngressPathMiddleware(_echo_root_path)
    transport = httpx.ASGITransport(app=app, client=client)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        return await c.get("/", headers=headers or {})


def test_ingress_trusted_client_sets_root_path() -> None:
    resp = asyncio.run(
        _get_via_middleware(
            client=("172.30.32.2", 4321),
            headers={"X-Ingress-Path": "/api/hassio_ingress/synthetic-token"},
        )
    )
    assert resp.status_code == 200
    assert resp.text == "/api/hassio_ingress/synthetic-token"


def test_ingress_trusted_client_trailing_slash_stripped() -> None:
    resp = asyncio.run(
        _get_via_middleware(
            client=("172.30.32.2", 4321),
            headers={"X-Ingress-Path": "/api/hassio_ingress/synthetic-token/"},
        )
    )
    assert resp.text == "/api/hassio_ingress/synthetic-token"


def test_ingress_untrusted_client_ignores_header() -> None:
    resp = asyncio.run(
        _get_via_middleware(
            client=("10.0.0.1", 4321),
            headers={"X-Ingress-Path": "/api/hassio_ingress/synthetic-token"},
        )
    )
    assert resp.text == ""  # root_path unchanged


def test_ingress_no_header_passes_through() -> None:
    resp = asyncio.run(_get_via_middleware(client=("172.30.32.2", 4321)))
    assert resp.status_code == 200
    assert resp.text == ""


def test_ingress_none_client_no_crash() -> None:
    """scope['client'] is None must not crash and must leave root_path unchanged."""

    async def run() -> None:
        app = IngressPathMiddleware(_echo_root_path)
        scope: dict[str, Any] = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [(b"x-ingress-path", b"/api/hassio_ingress/synthetic-token")],
            "client": None,
            "root_path": "initial",
        }
        sent: list[dict[str, Any]] = []

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": b""}

        async def send_fn(event: dict[str, Any]) -> None:
            sent.append(event)

        await app(scope, receive, send_fn)
        assert scope.get("root_path") == "initial"  # client=None must not mutate
        assert sent, "app must have sent a response"

    asyncio.run(run())


def test_csrf_cookie_sets_prefixed_path_and_deletes_stale_root() -> None:
    response = Response()
    pair = issue_csrf_pair()
    set_csrf_cookie(response, pair, "/api/hassio_ingress/synthetic-token")
    cookie_headers = [v.decode() for k, v in response.raw_headers if k == b"set-cookie"]
    assert any(
        "Path=/api/hassio_ingress/synthetic-token" in h for h in cookie_headers
    ), "prefixed Path not found in Set-Cookie headers"
    assert any(
        "Path=/" in h and ("Max-Age=0" in h or "expires=" in h) for h in cookie_headers
    ), "stale-cookie deletion header at Path=/ not found"


def test_csrf_cookie_root_path_emits_single_cookie_at_slash() -> None:
    response = Response()
    pair = issue_csrf_pair()
    set_csrf_cookie(response, pair, "")
    cookie_headers = [v.decode() for k, v in response.raw_headers if k == b"set-cookie"]
    assert len(cookie_headers) == 1, "no stale delete should be issued for path='/'"
    assert "Path=/" in cookie_headers[0]


# ---------------------------------------------------------------------------
# Budget refund: unit tests
# ---------------------------------------------------------------------------


def test_budget_reserve_refund_restores_calls(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    reservation = reserve_budget(conn, "open-meteo", calls=3)
    before = conn.execute(
        "SELECT calls FROM api_budget WHERE source='open-meteo' AND billing_day=?",
        (reservation.billing_day,),
    ).fetchone()
    assert before["calls"] == 3
    refund_budget(conn, reservation)
    after = conn.execute(
        "SELECT calls FROM api_budget WHERE source='open-meteo' AND billing_day=?",
        (reservation.billing_day,),
    ).fetchone()
    assert after["calls"] == 0


def test_budget_refund_floors_at_zero(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    conn.execute(
        "INSERT OR IGNORE INTO api_budget (source, billing_day, calls, credits)"
        " VALUES ('open-meteo', '2026-01-01', 2, 0)"
    )
    # Refund more than is in the bucket
    res = Reservation(
        source="open-meteo", billing_day="2026-01-01", calls=10, credits=0
    )
    refund_budget(conn, res)
    row = conn.execute(
        "SELECT calls FROM api_budget"
        " WHERE source='open-meteo' AND billing_day='2026-01-01'"
    ).fetchone()
    assert row["calls"] == 0


def test_budget_refund_credits_none_decrements_calls_only(tmp_path: Path) -> None:
    """credits=None in reserve_budget stores 0; refund leaves credits col unchanged."""
    conn = _init_tmp_db(tmp_path)
    conn.execute(
        "INSERT OR IGNORE INTO api_budget (source, billing_day, calls, credits)"
        " VALUES ('open-meteo', '2026-01-01', 3, 7)"
    )
    # credits=0 in Reservation mirrors reserve_budget(..., credits=None)
    res = Reservation(source="open-meteo", billing_day="2026-01-01", calls=3, credits=0)
    refund_budget(conn, res)
    row = conn.execute(
        "SELECT calls, credits FROM api_budget"
        " WHERE source='open-meteo' AND billing_day='2026-01-01'"
    ).fetchone()
    assert row["calls"] == 0  # 3 − 3 = 0
    assert row["credits"] == 7  # unchanged


def test_budget_refund_targets_own_billing_day_not_today(tmp_path: Path) -> None:
    """Cross-midnight: refund decrements the reservation's row, not another day's."""
    conn = _init_tmp_db(tmp_path)
    conn.execute(
        "INSERT OR IGNORE INTO api_budget (source, billing_day, calls, credits)"
        " VALUES ('open-meteo', '2026-01-01', 5, 0)"
    )
    conn.execute(
        "INSERT OR IGNORE INTO api_budget (source, billing_day, calls, credits)"
        " VALUES ('open-meteo', '2099-12-31', 3, 0)"
    )
    res = Reservation(source="open-meteo", billing_day="2026-01-01", calls=2, credits=0)
    refund_budget(conn, res)
    past = conn.execute(
        "SELECT calls FROM api_budget"
        " WHERE source='open-meteo' AND billing_day='2026-01-01'"
    ).fetchone()
    other = conn.execute(
        "SELECT calls FROM api_budget"
        " WHERE source='open-meteo' AND billing_day='2099-12-31'"
    ).fetchone()
    assert past["calls"] == 3  # 5 − 2
    assert other["calls"] == 3  # untouched


# ---------------------------------------------------------------------------
# Budget refund: flow tests via fetch_feed_once
# ---------------------------------------------------------------------------


def test_fetch_feed_connect_error_refunds_budget(tmp_path: Path) -> None:
    """ConnectError → budget net 0, last_error set, exception propagates."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _open_meteo_feed_id(conn)
    db = get_db()

    class _ConnectErrorAdapter:
        supports_historical = False

        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: Any) -> FetchResult:
            raise httpx.ConnectError("synthetic connect error")

    def _build(source: str, client: httpx.AsyncClient) -> _ConnectErrorAdapter:
        return _ConnectErrorAdapter()

    with pytest.raises(httpx.ConnectError):
        asyncio.run(fetch_feed_once(db, site_id, feed_id, adapter_builder=_build))

    budget = conn.execute(
        "SELECT calls FROM api_budget WHERE source='open-meteo'"
    ).fetchone()
    assert budget is None or budget["calls"] == 0, "budget must be net 0 after refund"

    state = conn.execute(
        "SELECT last_error FROM site_feed_state WHERE site_id=? AND feed_id=?",
        (site_id, feed_id),
    ).fetchone()
    assert state is not None and state["last_error"] is not None


def test_fetch_feed_429_consumes_budget_and_writes_backoff(tmp_path: Path) -> None:
    """429 → budget stays consumed, backoff row written, returns BackoffActive."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _open_meteo_feed_id(conn)
    db = get_db()

    _req = httpx.Request(
        "GET", "https://api.synthetic-provider.example.com/v1?apikey=synthetic"
    )
    _resp = httpx.Response(429, headers={"Retry-After": "120"}, request=_req)

    class _Http429Adapter:
        supports_historical = False

        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: Any) -> FetchResult:
            raise httpx.HTTPStatusError("429", request=_req, response=_resp)

    def _build(source: str, client: httpx.AsyncClient) -> _Http429Adapter:
        return _Http429Adapter()

    result = asyncio.run(fetch_feed_once(db, site_id, feed_id, adapter_builder=_build))
    assert isinstance(result, BackoffActive)

    budget = conn.execute(
        "SELECT calls FROM api_budget WHERE source='open-meteo'"
    ).fetchone()
    assert budget is not None and budget["calls"] == 1, "budget must remain consumed"

    backoff = conn.execute("SELECT domain FROM domain_backoffs").fetchone()
    assert backoff is not None, "backoff row must be written"


def test_fetch_feed_read_timeout_keeps_budget(tmp_path: Path) -> None:
    """ReadTimeout keeps budget: request may have reached the provider."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _open_meteo_feed_id(conn)
    db = get_db()

    class _ReadTimeoutAdapter:
        supports_historical = False

        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: Any) -> FetchResult:
            raise httpx.ReadTimeout("synthetic read timeout")

    def _build(source: str, client: httpx.AsyncClient) -> _ReadTimeoutAdapter:
        return _ReadTimeoutAdapter()

    with pytest.raises(httpx.ReadTimeout):
        asyncio.run(fetch_feed_once(db, site_id, feed_id, adapter_builder=_build))

    budget = conn.execute(
        "SELECT calls FROM api_budget WHERE source='open-meteo'"
    ).fetchone()
    assert budget is not None and budget["calls"] == 1, "ReadTimeout must NOT refund"


def test_fetch_feed_success_consumes_budget_exactly_once(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _open_meteo_feed_id(conn)
    db = get_db()

    class _SuccessAdapter:
        supports_historical = False

        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: Any) -> FetchResult:
            return FetchResult(samples=[], grid=None)

    def _build(source: str, client: httpx.AsyncClient) -> _SuccessAdapter:
        return _SuccessAdapter()

    asyncio.run(fetch_feed_once(db, site_id, feed_id, adapter_builder=_build))

    budget = conn.execute(
        "SELECT calls FROM api_budget WHERE source='open-meteo'"
    ).fetchone()
    assert budget is not None and budget["calls"] == 1


def test_fetch_feed_readtimeout_total_calls_capped_at_max_retries_plus_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A metered feed whose endpoint always times out must still go terminal
    after max_retries automatic retries: total budget burn across the whole
    retry sequence stays capped at (1 + max_retries) calls, not an uncapped
    short generic ladder that can blow through a small daily call budget.

    The fake clock here advances past the cadence floor on EVERY cycle, so
    the floor never binds and this test cannot observe whether it is wired
    in at all -- it only exercises the retry CAP. The floor's own spacing is
    pinned separately by
    test_fetch_feed_readtimeout_retry_spaced_by_cadence_floor, below.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _google_feed_id(conn)
    _subscribe(conn, site_id, feed_id)
    interval_row = conn.execute(
        "SELECT fetch_interval_minutes FROM feeds WHERE id=?", (feed_id,)
    ).fetchone()
    floor_seconds = int(interval_row["fetch_interval_minutes"]) * 60
    db = get_db()

    class _GoogleReadTimeoutAdapter:
        supports_historical = False

        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: Any) -> FetchResult:
            raise httpx.ReadTimeout("synthetic read timeout")

    def _build(source: str, client: httpx.AsyncClient) -> _GoogleReadTimeoutAdapter:
        return _GoogleReadTimeoutAdapter()

    monkeypatch.setattr("wxverify.worker.processor.build_adapter", _build)
    _patch_worker_infra(monkeypatch)

    clock = _FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    clock.patch(monkeypatch, "wxverify.db.queue")

    enqueue_if_absent(
        conn,
        "fetch_feed",
        site_id,
        f"fetch:{feed_id}",
        {"feed_id": feed_id},
        max_retries=3,
    )

    cycles = {"n": 0}
    max_cycles = 7

    def _advancing_claim(claim_conn: sqlite3.Connection) -> Job | None:
        cycles["n"] += 1
        if cycles["n"] > max_cycles:
            raise _StopLoop()
        if cycles["n"] > 1:
            clock.now = clock.now + timedelta(seconds=floor_seconds + 1)
        return claim_next_job(claim_conn)

    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _advancing_claim)

    with pytest.raises(_StopLoop):
        asyncio.run(run_worker(db))

    budget = conn.execute(
        "SELECT calls FROM api_budget WHERE source='google'"
    ).fetchone()
    assert budget is not None and budget["calls"] == 4, (
        "budget burn across automatic retries must stay capped at"
        f" 1 + max_retries calls; got {budget['calls'] if budget else None}"
    )

    job_row = conn.execute(
        "SELECT status, retry_count FROM jobs WHERE site_id=? AND type='fetch_feed'",
        (site_id,),
    ).fetchone()
    assert job_row is not None and job_row["status"] == "failed"


def test_fetch_feed_readtimeout_retry_spaced_by_cadence_floor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pins the composition at run_worker's failure-handling call site --
    fail(..., min_delay_seconds=fetch_feed_retry_floor_seconds(conn, job))
    (worker/processor.py) -- by running the real worker loop and asserting
    the exact next_attempt_at spacing it produces, not just a call count. A
    call-count assertion alone cannot tell a spaced retry from an unspaced
    one that merely stops for an unrelated reason (e.g. max_retries): the
    cap test above (test_fetch_feed_readtimeout_total_calls_capped_at_
    max_retries_plus_one) passes unchanged even if min_delay_seconds is
    silently dropped from that call site, because 1 + max_retries calls
    still happen -- just sooner than the feed's cadence allows.

    Two claim cycles, matching fetch_feed_retry_floor_seconds's own
    contract: the floor is None (skipped) on the first automatic retry
    (job.retry_count == 0 at claim time) and only starts binding from the
    second retry onward, so a single-cycle test cannot observe it either.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _google_feed_id(conn)
    _subscribe(conn, site_id, feed_id)
    interval_row = conn.execute(
        "SELECT fetch_interval_minutes FROM feeds WHERE id=?", (feed_id,)
    ).fetchone()
    floor_seconds = int(interval_row["fetch_interval_minutes"]) * 60
    db = get_db()

    class _GoogleReadTimeoutAdapter:
        supports_historical = False

        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: Any) -> FetchResult:
            raise httpx.ReadTimeout("synthetic read timeout")

    def _build(source: str, client: httpx.AsyncClient) -> _GoogleReadTimeoutAdapter:
        return _GoogleReadTimeoutAdapter()

    monkeypatch.setattr("wxverify.worker.processor.build_adapter", _build)
    _patch_worker_infra(monkeypatch)

    clock = _FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    clock.patch(monkeypatch, "wxverify.db.queue")

    enqueue_if_absent(
        conn,
        "fetch_feed",
        site_id,
        f"fetch:{feed_id}",
        {"feed_id": feed_id},
        max_retries=3,
    )

    cycles = {"n": 0}

    def _advancing_claim(claim_conn: sqlite3.Connection) -> Job | None:
        cycles["n"] += 1
        if cycles["n"] > 2:
            raise _StopLoop()
        if cycles["n"] > 1:
            # Jump past the FIRST retry's short exponential delay (2s) so the
            # SECOND claim can happen at all -- this advance is unrelated to
            # the floor under test, which only starts binding once this
            # second failure is itself recorded, below.
            clock.now = clock.now + timedelta(seconds=floor_seconds + 1)
        return claim_next_job(claim_conn)

    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _advancing_claim)

    with pytest.raises(_StopLoop):
        asyncio.run(run_worker(db))

    row = conn.execute(
        "SELECT retry_count, next_attempt_at FROM jobs"
        " WHERE type='fetch_feed' AND site_id=?",
        (site_id,),
    ).fetchone()
    assert row is not None and row["retry_count"] == 2
    assert row["next_attempt_at"] == clock.isoformat_utc(
        clock.now + timedelta(seconds=floor_seconds)
    ), (
        "the second automatic retry must be spaced at the feed's own fetch"
        " cadence by the min_delay_seconds=fetch_feed_retry_floor_seconds(...)"
        " composition in run_worker -- a dropped/broken composition would"
        " retry far sooner than the feed's cadence allows"
    )


# ---------------------------------------------------------------------------
# Fetch-feed retry ladder: cadence floor, retry cap, cooldown wiring
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "case",
    [
        "missing_feed_id",
        "invalid_feed_id_type",
        "feed_row_missing",
        "unparseable_interval",
        "interval_overflow",
        "interval_out_of_range_high",
        "interval_zero",
        "interval_negative",
    ],
)
def test_fetch_feed_retry_floor_seconds_fails_closed_to_fallback_ceiling(
    case: str, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Any lookup/parse miss in the cadence-derived floor must fail closed to
    the fallback ceiling, never fall through to the short generic ladder --
    and each fallback branch must log a warning, not fail silently.
    """
    conn = _init_tmp_db(tmp_path)
    feed_id = _google_feed_id(conn)

    if case == "missing_feed_id":
        payload: dict[str, object] = {}
        expected_fragment = "missing/invalid feed_id"
    elif case == "invalid_feed_id_type":
        payload = {"feed_id": "not-a-number"}
        expected_fragment = "missing/invalid feed_id"
    elif case == "feed_row_missing":
        payload = {"feed_id": 999999}
        expected_fragment = "feed row missing"
    elif case == "unparseable_interval":
        conn.execute(
            "UPDATE feeds SET fetch_interval_minutes='not-a-number' WHERE id=?",
            (feed_id,),
        )
        payload = {"feed_id": feed_id}
        expected_fragment = "unreadable fetch_interval_minutes"
    elif case == "interval_overflow":
        # OverflowError sub-case, distinct from the ValueError above: a REAL
        # affinity column can hold an IEEE754 infinity, and int() on that
        # raises OverflowError rather than ValueError -- this codebase has
        # hit exactly that carrier before (see _job_from_row in db/queue.py),
        # so the except clause deliberately catches both.
        conn.execute(
            "UPDATE feeds SET fetch_interval_minutes=? WHERE id=?",
            (float("inf"), feed_id),
        )
        payload = {"feed_id": feed_id}
        expected_fragment = "unreadable fetch_interval_minutes"
    elif case == "interval_out_of_range_high":
        # int() succeeds here (unlike interval_overflow above): a large but
        # finite value, e.g. a millisecond epoch stamp pasted into a minutes
        # field. It clears the API schema's ge=1 bound cleanly -- the
        # overflow this guards against happens later, in queue.fail()'s
        # utc_now() + timedelta(seconds=...) -- the datetime addition, not
        # the timedelta itself -- rather than in this function's own int()
        # parse.
        conn.execute(
            "UPDATE feeds SET fetch_interval_minutes=? WHERE id=?",
            (4_300_000_000, feed_id),
        )
        payload = {"feed_id": feed_id}
        expected_fragment = "out-of-range fetch_interval_minutes"
    elif case == "interval_zero":
        conn.execute("UPDATE feeds SET fetch_interval_minutes=0 WHERE id=?", (feed_id,))
        payload = {"feed_id": feed_id}
        expected_fragment = "out-of-range fetch_interval_minutes"
    else:
        assert case == "interval_negative"
        conn.execute(
            "UPDATE feeds SET fetch_interval_minutes=-60 WHERE id=?", (feed_id,)
        )
        payload = {"feed_id": feed_id}
        expected_fragment = "out-of-range fetch_interval_minutes"

    job = Job(
        id=1,
        type="fetch_feed",
        site_id=1,
        job_key="test-key",
        payload=payload,
        status="running",
        retry_count=0,
        max_retries=5,
    )
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.feed_fetch"):
        result = fetch_feed_retry_floor_seconds(conn, job)
    assert result == 3600  # fallback ceiling

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, (
        f"expected exactly one warning; got: {[r.getMessage() for r in warnings]}"
    )
    assert expected_fragment in warnings[0].getMessage()


def test_fetch_feed_astronomically_large_feed_id_does_not_crash_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fetch_feed job whose payload feed_id is an unbounded Python int (a
    foreign/imported row, the same threat model the payload `feed_id` guard
    already accepts) must not crash-loop the worker. _payload_feed_id has no
    range check, so the feeds lookup SELECT inside the retry-floor helper
    raises OverflowError binding it -- a raise that sits OUTSIDE the
    helper's own try/except, inside the failure-recording write itself.
    Pre-fix this escapes run_worker entirely (nothing narrower than
    asyncio.CancelledError catches it there), so retry_count never advances
    and the job would crash-loop forever across process restarts instead of
    going through the ordinary fail()/retry ladder. Also pins that the
    broad-catch fallback fails CLOSED to the fixed 3600s ceiling rather than
    open (None): a bare crash-avoidance/call-count check cannot tell those
    apart.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    huge_feed_id = int("9" * 400)

    # Patch the clock BEFORE enqueue_if_absent: patching after would leave
    # this row's next_attempt_at stamped on the real wall clock, and
    # claim_next_job would then never claim it -- the test would pass
    # vacuously (job never dispatched, no assertion below ever exercised).
    clock = _FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    clock.patch(monkeypatch, "wxverify.db.queue")

    enqueue_result = enqueue_if_absent(
        conn,
        "fetch_feed",
        site_id,
        f"fetch:{huge_feed_id}",
        {"feed_id": huge_feed_id},
    )
    job_id = enqueue_result.job_id
    assert job_id is not None

    db = get_db()
    _patch_worker_infra(monkeypatch)

    cycles = {"n": 0}

    def _claim_real_once(claim_conn: sqlite3.Connection) -> Job | None:
        cycles["n"] += 1
        if cycles["n"] > 1:
            raise _StopLoop()
        return claim_next_job(claim_conn)

    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_real_once)

    with pytest.raises(_StopLoop):
        asyncio.run(run_worker(db))

    row = conn.execute(
        "SELECT status, retry_count, last_error, next_attempt_at FROM jobs WHERE id=?",
        (job_id,),
    ).fetchone()
    assert row is not None
    assert row["retry_count"] == 1, (
        "the failure must be recorded through the normal fail() path, not"
        " lost to a crash escaping run_worker"
    )
    assert row["status"] == "pending"
    assert row["last_error"] is not None
    assert row["next_attempt_at"] == clock.isoformat_utc(
        clock.now + timedelta(seconds=3600)
    ), (
        "the broad-catch wrapper in fetch_feed_retry_floor_seconds must fail"
        " CLOSED to the fixed fallback ceiling, not open (None) -- failing"
        " open here would let a foreign/corrupt feed_id retry on the short"
        " generic exponential ladder instead of the fallback ceiling"
    )


def test_fetch_feed_retry_floor_seconds_happy_path_and_non_fetch_feed_type(
    tmp_path: Path,
) -> None:
    """Valid feed_id with a parseable cadence returns it unclamped from the
    second automatic retry onward (job.retry_count >= 1 at claim time); the
    first automatic retry (retry_count == 0) always skips the floor, and
    every other job type is untouched (returns None) regardless of
    retry_count.
    """
    conn = _init_tmp_db(tmp_path)
    feed_id = _google_feed_id(conn)
    interval_minutes = int(
        conn.execute(
            "SELECT fetch_interval_minutes FROM feeds WHERE id=?", (feed_id,)
        ).fetchone()["fetch_interval_minutes"]
    )

    first_retry_job = Job(
        id=1,
        type="fetch_feed",
        site_id=1,
        job_key="test-key",
        payload={"feed_id": feed_id},
        status="running",
        retry_count=0,
        max_retries=5,
    )
    assert fetch_feed_retry_floor_seconds(conn, first_retry_job) is None

    second_retry_job = Job(
        id=1,
        type="fetch_feed",
        site_id=1,
        job_key="test-key",
        payload={"feed_id": feed_id},
        status="running",
        retry_count=1,
        max_retries=5,
    )
    assert (
        fetch_feed_retry_floor_seconds(conn, second_retry_job) == interval_minutes * 60
    )

    other_job = Job(
        id=2,
        type="backfill_site",
        site_id=1,
        job_key="test-key",
        payload={"feed_id": feed_id},
        status="running",
        retry_count=1,
        max_retries=5,
    )
    assert fetch_feed_retry_floor_seconds(conn, other_job) is None


def test_scheduler_tick_does_not_reenqueue_fetch_feed_within_failure_cooldown(
    tmp_path: Path,
) -> None:
    """A fetch_feed job that failed minutes ago must not be re-enqueued by
    the next scheduler tick -- the same cooldown that already protects
    fetch_obs/fetch_current_obs must also protect fetch_feed.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _google_feed_id(conn)
    _subscribe(conn, site_id, feed_id)
    job_key = f"fetch:{feed_id}"
    recent_failure = isoformat_utc(utc_now() - timedelta(minutes=5))
    conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, payload, status, updated_at)
        VALUES ('fetch_feed', ?, ?, '{}', 'failed', ?)
        """,
        (site_id, job_key, recent_failure),
    )

    scheduler_tick(conn)

    count = conn.execute(
        "SELECT COUNT(*) FROM jobs"
        " WHERE type='fetch_feed' AND site_id=? AND job_key=? AND status='pending'",
        (site_id, job_key),
    ).fetchone()[0]
    assert count == 0, "a terminal failure inside the cooldown must suppress re-enqueue"


def test_scheduler_tick_enqueues_fetch_feed_with_reduced_retry_cap(
    tmp_path: Path,
) -> None:
    """The scheduler's automatic due-feed path must cap fetch_feed retries at
    3, not the schema default of 5 -- a metered provider job that keeps
    failing must not fall back to the longer generic retry cap.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _google_feed_id(conn)
    _subscribe(conn, site_id, feed_id)

    scheduler_tick(conn)

    row = conn.execute(
        "SELECT max_retries FROM jobs"
        " WHERE type='fetch_feed' AND site_id=? AND job_key=?",
        (site_id, f"fetch:{feed_id}"),
    ).fetchone()
    assert row is not None and row["max_retries"] == 3


@pytest.mark.parametrize(
    "job_type", ["backfill_site", "pair_and_score", "fetch_obs", "fetch_current_obs"]
)
def test_fail_generic_ladder_unchanged_for_non_fetch_feed_types(
    job_type: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """fail() with no min_delay_seconds keeps the unmodified schema-default
    max_retries and the plain 2**retry_count backoff -- the retry ladder
    changes only for fetch_feed, composed by its own caller.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)

    clock = _FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    clock.patch(monkeypatch, "wxverify.db.queue")

    result = enqueue_if_absent(conn, job_type, site_id, "test-key", {})
    assert result.job_id is not None

    disposition = fail(conn, result.job_id, "synthetic failure")
    assert disposition is not None
    assert disposition.max_retries == 5
    assert disposition.terminal is False

    row = conn.execute(
        "SELECT next_attempt_at FROM jobs WHERE id=?", (result.job_id,)
    ).fetchone()
    expected = clock.isoformat_utc(clock.now + timedelta(seconds=2))
    assert row["next_attempt_at"] == expected


def test_enqueue_fetch_for_feed_ignores_cooldown_and_caps_retries(
    tmp_path: Path,
) -> None:
    """The operator-initiated manual-retry route must never be silently
    dropped by the scheduler's terminal-failure cooldown, and must still get
    the same reduced retry cap as the automatic path.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _google_feed_id(conn)
    _subscribe(conn, site_id, feed_id)
    job_key = f"fetch:{feed_id}"
    recent_failure = isoformat_utc(utc_now() - timedelta(minutes=5))
    conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, payload, status, updated_at)
        VALUES ('fetch_feed', ?, ?, '{}', 'failed', ?)
        """,
        (site_id, job_key, recent_failure),
    )

    result = enqueue_fetch_for_feed(conn, site_id, feed_id)

    assert result.created is True, (
        "a manual retry must never be silently dropped by the cooldown"
    )
    row = conn.execute(
        "SELECT max_retries FROM jobs WHERE id=?", (result.job_id,)
    ).fetchone()
    assert row is not None and row["max_retries"] == 3


def test_claim_next_job_skips_pending_row_with_future_next_attempt_at(
    tmp_path: Path,
) -> None:
    """A pending row scheduled for the future must not be claimed early --
    the retry-floor/backoff delay this whole ladder relies on is enforced by
    this WHERE gate, not by caller discipline.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    future = isoformat_utc(utc_now() + timedelta(hours=1))
    conn.execute(
        """
        INSERT INTO jobs (type, site_id, job_key, payload, status, next_attempt_at)
        VALUES ('fetch_feed', ?, 'future-key', '{}', 'pending', ?)
        """,
        (site_id, future),
    )

    assert claim_next_job(conn) is None


def test_fail_min_delay_seconds_floor_dominates_small_exponential_term(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When the caller-supplied floor exceeds the exponential term, the floor
    must win -- a max()-vs-min() composition bug would silently let the much
    shorter exponential delay through instead.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    conn.execute(
        """
        INSERT INTO jobs
            (type, site_id, job_key, payload, status, retry_count, max_retries)
        VALUES ('fetch_feed', ?, 'floor-key', '{}', 'running', 0, 5)
        """,
        (site_id,),
    )
    job_id = conn.execute("SELECT id FROM jobs WHERE job_key='floor-key'").fetchone()[
        "id"
    ]

    clock = _FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    clock.patch(monkeypatch, "wxverify.db.queue")

    # retry_count 0 -> 1: exponential term is min(3600, 2**1) == 2s, far below
    # a realistic multi-hour cadence floor -- the floor must dominate.
    disposition = fail(conn, job_id, "boom", min_delay_seconds=21600)

    assert disposition is not None and disposition.terminal is False
    expected = isoformat_utc(
        datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=21600)
    )
    assert disposition.next_attempt_at == expected, (
        "min_delay_seconds must act as a floor (max), not be overridden by"
        " the much shorter exponential term"
    )


def test_fail_exponential_term_dominates_small_min_delay_seconds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When the exponential term exceeds a small caller-supplied floor, the
    floor must not pull the delay down -- guards both a min()/max() swap and
    an accidental override/replace of the exponential term entirely.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    conn.execute(
        """
        INSERT INTO jobs
            (type, site_id, job_key, payload, status, retry_count, max_retries)
        VALUES ('fetch_feed', ?, 'exp-key', '{}', 'running', 9, 15)
        """,
        (site_id,),
    )
    job_id = conn.execute("SELECT id FROM jobs WHERE job_key='exp-key'").fetchone()[
        "id"
    ]

    clock = _FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    clock.patch(monkeypatch, "wxverify.db.queue")

    # retry_count 9 -> 10: exponential term is min(3600, 2**10) == 1024s,
    # comfortably above a small 10s floor.
    disposition = fail(conn, job_id, "boom", min_delay_seconds=10)

    assert disposition is not None and disposition.terminal is False
    expected = isoformat_utc(datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=1024))
    assert disposition.next_attempt_at == expected, (
        "a small min_delay_seconds must not shorten the exponential term"
    )


# ---------------------------------------------------------------------------
# Worker logging (caplog oracles)
# ---------------------------------------------------------------------------


def test_worker_generic_exception_logs_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Dispatch raises → exactly one WARNING with job type and sanitized message."""
    job = _make_job(job_type="fetch_feed", site_id=42)

    async def _raise_runtime(db: Any, writer: Any, j: Job) -> None:
        raise RuntimeError("synthetic provider failure")

    def _retry_disposition(
        conn: Any, job_id: int, error: str, *, min_delay_seconds: int | None = None
    ) -> FailDisposition:
        return FailDisposition(
            terminal=False,
            retry_count=1,
            max_retries=5,
            next_attempt_at="2099-01-01T00:00:00.000Z",
        )

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _raise_runtime)
    monkeypatch.setattr("wxverify.worker.processor.fail", _retry_disposition)
    monkeypatch.setattr(
        "wxverify.worker.processor.fetch_feed_retry_floor_seconds", lambda c, j: None
    )

    with (
        caplog.at_level(logging.WARNING, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "fetch_feed" in msg
    assert "synthetic provider failure" in msg


def test_worker_terminal_failure_logs_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Retries exhausted → ERROR record (not WARNING)."""
    job = _make_job(job_type="fetch_feed", site_id=42, retry_count=5, max_retries=5)

    async def _raise_runtime(db: Any, writer: Any, j: Job) -> None:
        raise RuntimeError("terminal failure")

    def _terminal_disposition(
        conn: Any, job_id: int, error: str, *, min_delay_seconds: int | None = None
    ) -> FailDisposition:
        return FailDisposition(
            terminal=True, retry_count=6, max_retries=5, next_attempt_at=None
        )

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _raise_runtime)
    monkeypatch.setattr("wxverify.worker.processor.fail", _terminal_disposition)
    monkeypatch.setattr(
        "wxverify.worker.processor.fetch_feed_retry_floor_seconds", lambda c, j: None
    )

    with (
        caplog.at_level(logging.ERROR, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "failed" in errors[0].getMessage()


def test_worker_deferred_job_cycle_line_info_deferred_line_debug(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """BC2: 'job deferred' moved INFO→DEBUG; cycle: outcome=deferred stays INFO.

    The old test matched 'deferred' against the new cycle: INFO line — a false
    oracle that would pass even if BC2 were reverted or the wrong line fired.
    This retargeted version pins the real post-BC2 contract:
      - 'job deferred …' is a DEBUG record (present at DEBUG, absent at INFO)
      - 'cycle: job=… outcome=deferred' is an INFO record

    Will go red if BC2 is reverted (job deferred re-promoted to INFO), or if the
    cycle: line stops carrying outcome=deferred.
    """
    job = _make_job(job_type="fetch_feed", site_id=42)

    async def _defer(db: Any, writer: Any, j: Job) -> None:
        raise JobDeferred("2099-01-01T00:00:00.000Z")

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _defer)
    monkeypatch.setattr("wxverify.worker.processor.defer_job", lambda c, jid, at: None)

    # At DEBUG: both the cycle INFO line and the 'job deferred' DEBUG line appear
    with (
        caplog.at_level(logging.DEBUG, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    # 'job deferred' must be a DEBUG record, not INFO
    job_deferred_info = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO and "job deferred" in r.getMessage()
    ]
    assert len(job_deferred_info) == 0, (
        "BC2: 'job deferred' must NOT appear at INFO level"
    )

    job_deferred_debug = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and "job deferred" in r.getMessage()
    ]
    assert len(job_deferred_debug) == 1, (
        f"'job deferred' must appear at DEBUG exactly once; "
        f"messages: {[r.getMessage() for r in caplog.records]}"
    )

    # cycle: INFO line carries outcome=deferred — the sanctioned INFO oracle
    cycle_info = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO and "cycle: job=" in r.getMessage()
    ]
    assert len(cycle_info) == 1
    assert "outcome=deferred" in cycle_info[0].getMessage(), (
        f"cycle: line must carry outcome=deferred; got: {cycle_info[0].getMessage()!r}"
    )


_STALE_GENERATION_OUTCOMES = ("success", "deferred", "cancelled", "failure")


@pytest.mark.parametrize("outcome_kind", _STALE_GENERATION_OUTCOMES)
def test_worker_loop_survives_a_stale_generation_on_every_outcome(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    outcome_kind: str,
) -> None:
    """A database replace landing between a job's claim and its outcome
    write must abandon only that one job, never the worker loop -- for
    every one of the four ways dispatch can finish (plain return,
    JobDeferred, JobCancelled, or an ordinary failure).

    'failure' is the case that actually kills the loop if the fence's catch
    is a sibling `except` arm instead of one wrapping the whole per-job
    try/except: the write that trips the fence happens inside `except
    Exception`'s own body, so only a handler around the entire statement --
    never a sibling of it -- can ever see the exception it raises. 'success'
    would misleadingly still pass under that same bug, since its triggering
    write sits directly in the try body a sibling handler does reach.
    """
    job1 = _make_job(job_type="fetch_feed", site_id=42, job_id=1)
    job2 = _make_job(job_type="fetch_feed", site_id=43, job_id=2)
    pending_jobs = [job1, job2]

    def _claim(conn: Any) -> Job | None:
        if pending_jobs:
            return pending_jobs.pop(0)
        raise _StopLoop()

    dispatched_ids: list[int] = []

    async def _fake_dispatch(db_arg: Any, writer_arg: Any, job_arg: Job) -> None:
        dispatched_ids.append(job_arg.id)
        if job_arg.id == 1:
            db_arg.generation += 1
            if outcome_kind == "success":
                return None
            if outcome_kind == "deferred":
                raise JobDeferred("2099-01-01T00:00:00.000Z")
            if outcome_kind == "cancelled":
                raise JobCancelled()
            raise RuntimeError("synthetic failure for the generation-fence oracle")
        return None

    completed: list[int] = []
    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim)
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _fake_dispatch)
    monkeypatch.setattr(
        "wxverify.worker.processor.complete", lambda c, jid: completed.append(jid)
    )

    with (
        caplog.at_level(logging.INFO, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_GenerationFenceDb()))  # type: ignore[arg-type]

    # The loop reached job 2 and completed it normally -- proof the stale
    # write on job 1 abandoned only job 1, not the loop itself.
    assert dispatched_ids == [1, 2]
    assert completed == [2], (
        "job 1's outcome write must never run once its generation is stale"
    )

    abandoned = [r for r in caplog.records if "job abandoned" in r.getMessage()]
    assert len(abandoned) == 1
    assert abandoned[0].levelno == logging.INFO
    assert "id=1" in abandoned[0].getMessage()


def test_domain_backoff_429_logs_warning_with_domain_and_retry(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """record_http_backoff with 429 → WARNING with domain and next-attempt."""
    conn = _init_tmp_db(tmp_path)
    req = httpx.Request("GET", "https://api.synthetic-provider.example.com/v1")
    resp = httpx.Response(429, headers={"Retry-After": "60"}, request=req)

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.domain_backoff"):
        result = record_http_backoff(conn, resp)

    assert result is not None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "api.synthetic-provider.example.com" in msg
    assert "429" in msg


def test_worker_url_secrets_redacted_in_logs(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """key= and appid= params must be redacted from all log records."""
    job = _make_job(job_type="fetch_feed", site_id=42)

    async def _raise_with_secret_url(db: Any, writer: Any, j: Job) -> None:
        raise RuntimeError(
            "Request to https://api.example.com/forecast"
            "?key=SYNTHETIC-SECRET&appid=SYNTHETIC-SECRET failed"
        )

    def _retry(
        conn: Any, job_id: int, error: str, *, min_delay_seconds: int | None = None
    ) -> FailDisposition:
        return FailDisposition(
            terminal=False,
            retry_count=1,
            max_retries=5,
            next_attempt_at="2099-01-01T00:00:00.000Z",
        )

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _raise_with_secret_url)
    monkeypatch.setattr("wxverify.worker.processor.fail", _retry)
    monkeypatch.setattr(
        "wxverify.worker.processor.fetch_feed_retry_floor_seconds", lambda c, j: None
    )

    with (
        caplog.at_level(logging.WARNING, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    assert "SYNTHETIC-SECRET" not in caplog.text, (
        "sanitized_exception must redact key= and appid= query params"
    )


# ---------------------------------------------------------------------------
# idx_pairs_cell index + phase-split write discipline
# ---------------------------------------------------------------------------


def test_idx_pairs_cell_created_on_fresh_db(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    assert _index_exists(conn, "idx_pairs_cell")
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 3


def test_idx_pairs_cell_created_on_pre_existing_v2_db(tmp_path: Path) -> None:
    """idx_pairs_cell is added to a v2 DB that lacks it; the migration bumps to 3."""
    # Build a full-migration DB, then drop the index to simulate the old 0.1.0 schema.
    conn = _init_tmp_db(tmp_path)
    assert _index_exists(conn, "idx_pairs_cell")
    conn.execute("DROP INDEX idx_pairs_cell")
    assert not _index_exists(conn, "idx_pairs_cell")

    # Re-initialize — simulates upgrade install booting with 0.1.1.
    close_db()
    db2 = init_db(str(tmp_path / "wxverify.db"))
    conn2 = db2._conn  # noqa: SLF001

    assert _index_exists(conn2, "idx_pairs_cell"), "idx_pairs_cell must be re-created"
    version = conn2.execute("PRAGMA user_version").fetchone()[0]
    assert version == 3, "user_version must reach 3 after the migration"


def test_pair_and_score_dispatch_issues_at_least_four_write_transactions(
    tmp_path: Path,
) -> None:
    """pair_and_score dispatches each phase in its own db.write (≥4 transactions)."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    db = get_db()
    spy = _WriteCountDb(db)
    writer = FencedWriter(spy, spy.generation)  # type: ignore[arg-type]

    job = _make_job(job_type="pair_and_score", site_id=site_id)
    asyncio.run(dispatch(spy, writer, job))  # type: ignore[arg-type]

    assert spy.count >= 4, f"Expected ≥4 write transactions, got {spy.count}"


def test_pair_and_score_stops_when_site_disabled_between_phases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Enabled gate re-checked per phase: disabling mid-run aborts remaining phases."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    db = get_db()

    phases_called: list[int] = []

    def _phase0(c: sqlite3.Connection, sid: int | None) -> None:
        phases_called.append(0)
        c.execute("UPDATE sites SET enabled=0 WHERE id=?", (sid,))

    def _phase1(c: sqlite3.Connection, sid: int | None) -> None:
        phases_called.append(1)  # must never run

    monkeypatch.setattr(
        "wxverify.worker.processor.PAIR_PHASES",
        (_phase0, _phase1),
    )

    job = _make_job(job_type="pair_and_score", site_id=site_id)
    writer = FencedWriter(db, db.generation)
    with pytest.raises(JobCancelled):
        asyncio.run(dispatch(db, writer, job))

    assert phases_called == [0], (
        "phase 0 must run; phase 1 must be blocked by the enabled gate"
    )
