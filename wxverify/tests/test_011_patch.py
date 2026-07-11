"""Tests for wxverify 0.1.1 patch — bugs 1–4."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from pathlib import Path
from typing import Any

import httpx
import pytest
from starlette.responses import Response

from wxverify import config
from wxverify.api.csrf import issue_csrf_pair, set_csrf_cookie
from wxverify.api.ingress import IngressPathMiddleware
from wxverify.collection.budget import Reservation, refund_budget, reserve_budget
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.queue import FailDisposition, Job
from wxverify.feeds.seam import CostEstimate, FetchResult
from wxverify.worker.control import JobCancelled, JobDeferred
from wxverify.worker.domain_backoff import record_http_backoff
from wxverify.worker.feed_fetch import BackoffActive, fetch_feed_once
from wxverify.worker.processor import dispatch, run_worker

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

    async def write(self, fn):  # type: ignore[no-untyped-def]
        return fn(None)

    async def read(self, fn):  # type: ignore[no-untyped-def]
        return fn(None)


class _WriteCountDb:
    """Spy that wraps a real Database and counts write calls."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.count = 0

    async def write(self, fn):  # type: ignore[no-untyped-def]
        self.count += 1
        return await self._inner.write(fn)

    async def read(self, fn):  # type: ignore[no-untyped-def]
        return await self._inner.read(fn)


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
# Bug 1 — IngressPathMiddleware
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
# Bug 4 — Budget refund: unit tests
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
# Bug 4 — Budget refund: flow tests via fetch_feed_once
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


# ---------------------------------------------------------------------------
# Bug 3 — Worker logging (caplog oracles)
# ---------------------------------------------------------------------------


def test_worker_generic_exception_logs_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Dispatch raises → exactly one WARNING with job type and sanitized message."""
    job = _make_job(job_type="fetch_feed", site_id=42)

    async def _raise_runtime(db: Any, j: Job) -> None:
        raise RuntimeError("synthetic provider failure")

    def _retry_disposition(conn: Any, job_id: int, error: str) -> FailDisposition:
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

    async def _raise_runtime(db: Any, j: Job) -> None:
        raise RuntimeError("terminal failure")

    def _terminal_disposition(conn: Any, job_id: int, error: str) -> FailDisposition:
        return FailDisposition(
            terminal=True, retry_count=6, max_retries=5, next_attempt_at=None
        )

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _raise_runtime)
    monkeypatch.setattr("wxverify.worker.processor.fail", _terminal_disposition)

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

    async def _defer(db: Any, j: Job) -> None:
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

    async def _raise_with_secret_url(db: Any, j: Job) -> None:
        raise RuntimeError(
            "Request to https://api.example.com/forecast"
            "?key=SYNTHETIC-SECRET&appid=SYNTHETIC-SECRET failed"
        )

    def _retry(conn: Any, job_id: int, error: str) -> FailDisposition:
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

    with (
        caplog.at_level(logging.WARNING, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    assert "SYNTHETIC-SECRET" not in caplog.text, (
        "sanitized_exception must redact key= and appid= query params"
    )


# ---------------------------------------------------------------------------
# Bug 2 residuals — idx_pairs_cell index + phase-split write discipline
# ---------------------------------------------------------------------------


def test_idx_pairs_cell_created_on_fresh_db(tmp_path: Path) -> None:
    conn = _init_tmp_db(tmp_path)
    assert _index_exists(conn, "idx_pairs_cell")
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 3


def test_idx_pairs_cell_created_on_pre_existing_v2_db(tmp_path: Path) -> None:
    """idx_pairs_cell is added to a user_version=2 DB that lacks it; S-M1 bumps to 3."""
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
    assert version == 3, "user_version must reach 3 after S-M1 migration"


def test_pair_and_score_dispatch_issues_at_least_four_write_transactions(
    tmp_path: Path,
) -> None:
    """pair_and_score dispatches each phase in its own db.write (≥4 transactions)."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    db = get_db()
    spy = _WriteCountDb(db)

    job = _make_job(job_type="pair_and_score", site_id=site_id)
    asyncio.run(dispatch(spy, job))  # type: ignore[arg-type]

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
        "wxverify.worker.processor.PAIR_AND_SCORE_PHASES",
        (_phase0, _phase1),
    )

    job = _make_job(job_type="pair_and_score", site_id=site_id)
    with pytest.raises(JobCancelled):
        asyncio.run(dispatch(db, job))

    assert phases_called == [0], (
        "phase 0 must run; phase 1 must be blocked by the enabled gate"
    )
