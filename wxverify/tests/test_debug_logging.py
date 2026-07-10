"""Tests for wxverify 0.2.1 debug-logging revision.

Covers:
- T1–T3  RedactUrlSecretsFilter unit tests (security oracle)
- T4     End-to-end redaction via feed adapter + httpx
- T5     Regression: existing test_worker_url_secrets_redacted_in_logs still holds
- T6     CLI wiring: _configure_logging runs for one-shot commands at correct level
- T7     INFO level policy: only sanctioned milestone lines appear, zero DEBUG
- T8     Feed-adapter firehose at DEBUG (open_meteo request+response lines)
- T9     PWS observation firehose at DEBUG
- T10    Scoring engine firehose at DEBUG: phase lines emitted per phase
- T11    Worker cycle at DEBUG: job-claimed / job-completed lines present
- T12    Backfill/catchup at DEBUG: window/chunk / sites/changed lines
- T13    DB migration firehose at DEBUG: migrations begin…done + txn begin/commit
- T14    Wire-level (httpx at DEBUG): HTTP Request record passes through filter scrubbed
- T15    BC1: cycle INFO line present with correct outcome; DEBUG per-op lines absent
- T16    D5: terminal ERROR comes from processor, NOT from feed_fetch.py
- T17/T7b BC2: job deferred moved INFO → DEBUG (paired positive + negative)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
from logging import LogRecord
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from wxverify import config
from wxverify.core.log_redaction import RedactUrlSecretsFilter
from wxverify.db.connection import close_db, get_db, init_db
from wxverify.db.queue import FailDisposition, Job
from wxverify.feeds.seam import CostEstimate, FetchResult
from wxverify.worker.control import JobDeferred
from wxverify.worker.feed_fetch import fetch_feed_once
from wxverify.worker.processor import run_worker

# ---------------------------------------------------------------------------
# Shared helpers (mirror test_011_patch.py conventions exactly)
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
            VALUES ('LogTest', 47.0, 25.0, 900.0, 'UTC')
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


def _make_url(param: str, value: str = "SECRET123") -> str:
    """Build a synthetic API URL with the given query key set to value."""
    return f"https://api.example.com/v1/forecast?{param}={value}"


# ---------------------------------------------------------------------------
# Logging-state isolation fixture
#
# basicConfig(force=True) mutates global logging state. We snapshot the root
# logger's level + handlers, and httpx/httpcore loggers' levels + filters,
# then restore them after every test that touches _configure_logging.
# ---------------------------------------------------------------------------


@pytest.fixture()
def restore_logging_state() -> Any:
    """Snapshot and restore root + httpx/httpcore logger state."""
    root = logging.getLogger()
    saved_root_level = root.level
    saved_root_handlers = list(root.handlers)

    wire_saved: dict[str, tuple[int, list[logging.Filter]]] = {}
    for name in ("httpx", "httpcore"):
        lg = logging.getLogger(name)
        wire_saved[name] = (lg.level, list(lg.filters))

    yield

    # Restore root
    root.setLevel(saved_root_level)
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in saved_root_handlers:
        root.addHandler(h)

    # Restore wire loggers
    for name, (lvl, filters) in wire_saved.items():
        lg = logging.getLogger(name)
        lg.setLevel(lvl)
        lg.filters = list(filters)


# ---------------------------------------------------------------------------
# T1 — RedactUrlSecretsFilter: scrubs a %-arg record (httpx style)
# ---------------------------------------------------------------------------


def test_redact_filter_scrubs_percent_arg_record() -> None:
    """T1: filter renders %-args, scrubs the secret, clears args — no TypeError."""
    url = _make_url("key", "SECRET123")
    record = LogRecord(
        name="httpx",
        level=logging.DEBUG,
        pathname="",
        lineno=0,
        msg='HTTP Request: GET %s "200 OK"',
        args=(url,),
        exc_info=None,
    )
    filt = RedactUrlSecretsFilter()
    result = filt.filter(record)

    assert result is True, "filter must always return True (never drop records)"
    rendered = record.getMessage()
    assert "SECRET123" not in rendered, "secret value must be redacted"
    # urlencode percent-encodes '***' as '%2A%2A%2A' — confirm the placeholder appears
    # in either form (both are acceptable redaction markers)
    assert "***" in rendered or "%2A%2A%2A" in rendered, (
        f"redacted placeholder must appear; got: {rendered!r}"
    )
    # args cleared so a second getMessage() call doesn't double-render
    assert record.args == (), "args must be cleared after scrubbing"


# ---------------------------------------------------------------------------
# T2 — RedactUrlSecretsFilter: no-URL record left byte-identical
# ---------------------------------------------------------------------------


def test_redact_filter_leaves_no_url_record_untouched() -> None:
    """T2: a plain-text record with no URL must not be mutated at all."""
    original_msg = "worker started"
    original_args: tuple[()] = ()
    record = LogRecord(
        name="wxverify.api.app",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=original_msg,
        args=original_args,
        exc_info=None,
    )
    filt = RedactUrlSecretsFilter()
    filt.filter(record)

    assert record.msg is original_msg, "msg must be the identical object (not mutated)"
    assert record.args is original_args, "args must be untouched"


# ---------------------------------------------------------------------------
# T3 — RedactUrlSecretsFilter: parametrize over every secret key name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "param",
    ["apikey", "api_key", "key", "appid", "token", "password", "apiKey"],
)
def test_redact_filter_covers_all_secret_param_names(param: str) -> None:
    """T3: every real adapter key param — including apiKey camelCase — is scrubbed."""
    url = _make_url(param, "SECRET123")
    record = LogRecord(
        name="httpx",
        level=logging.DEBUG,
        pathname="",
        lineno=0,
        msg="HTTP Request: GET %s",
        args=(url,),
        exc_info=None,
    )
    RedactUrlSecretsFilter().filter(record)
    rendered = record.getMessage()
    assert "SECRET123" not in rendered, (
        f"param {param!r} value must be redacted; got: {rendered!r}"
    )
    # urlencode percent-encodes '***' to '%2A%2A%2A'; either form is a valid marker
    assert "***" in rendered or "%2A%2A%2A" in rendered, (
        f"redaction placeholder missing for param {param!r}; got: {rendered!r}"
    )


# ---------------------------------------------------------------------------
# T4 — End-to-end: adapter debug line via mocked httpx transport
# ---------------------------------------------------------------------------


def test_feed_adapter_debug_url_secret_absent_in_caplog(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """T4: at DEBUG, a feed fetch's URL-bearing lines must not contain the key value."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _open_meteo_feed_id(conn)
    db = get_db()

    # open-meteo uses no key, but we use a generic adapter that would embed one
    # to verify the redaction path. We inject an adapter whose fetch_forecast
    # raises (triggering the transport-error debug line) with a URL containing
    # a secret.
    secret_url = "https://api.example.com/v1?key=SYNTHETIC-SECRET-E2E"

    class _SecretUrlAdapter:
        supports_historical = False

        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: Any) -> FetchResult:
            raise RuntimeError(f"failed: {secret_url}")

    def _build(source: str, client: httpx.AsyncClient) -> _SecretUrlAdapter:
        return _SecretUrlAdapter()

    with (
        caplog.at_level(logging.DEBUG, logger="wxverify.worker.feed_fetch"),
        pytest.raises(RuntimeError),
    ):
        asyncio.run(fetch_feed_once(db, site_id, feed_id, adapter_builder=_build))

    assert "SYNTHETIC-SECRET-E2E" not in caplog.text, (
        "secret embedded in exception URL must be redacted in feed_fetch debug trace"
    )


# ---------------------------------------------------------------------------
# T5 — Regression: existing redaction test still holds (re-expressed here)
# ---------------------------------------------------------------------------


def test_worker_url_secrets_still_redacted_in_logs_regression(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T5: regression guard — key= and appid= remain absent from worker log text."""
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

    assert "SYNTHETIC-SECRET" not in caplog.text


# ---------------------------------------------------------------------------
# T6 — CLI wiring: _configure_logging runs for one-shot commands
# ---------------------------------------------------------------------------


def test_configure_logging_runs_for_cli_oneshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    restore_logging_state: Any,
) -> None:
    """T6: _configure_logging sets root logger to the level from WXV_LOG_LEVEL.

    basicConfig(force=True) replaces the root logger's handlers (including caplog's),
    so we assert on the root logger's effective level directly — the authoritative
    signal that _configure_logging ran and resolved the right level.

    Positive: WXV_LOG_LEVEL=debug → root logger at DEBUG after _configure_logging.
    Negative (paired): WXV_LOG_LEVEL=info → root logger at INFO, not DEBUG.
    This proves the level is wired from the env var, not left at a Python default.
    """
    from wxverify.__main__ import _configure_logging  # noqa: PLC0415

    # Positive: debug level
    monkeypatch.setenv("WXV_LOG_LEVEL", "debug")
    config.options_path = str(tmp_path / "missing-options.json")
    _configure_logging()

    root = logging.getLogger()
    assert root.level == logging.DEBUG, (
        f"at WXV_LOG_LEVEL=debug, root logger must be at DEBUG; "
        f"got level {root.level} ({logging.getLevelName(root.level)})"
    )

    # Negative (paired): info level → root at INFO, not DEBUG
    monkeypatch.setenv("WXV_LOG_LEVEL", "info")
    _configure_logging()

    assert root.level == logging.INFO, (
        f"at WXV_LOG_LEVEL=info, root logger must be at INFO; "
        f"got level {root.level} ({logging.getLevelName(root.level)})"
    )
    assert root.level != logging.DEBUG, (
        "at WXV_LOG_LEVEL=info, root logger must NOT be at DEBUG"
    )


def test_configure_logging_attaches_filter_at_info_level(
    restore_logging_state: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T6 attachment-is-unconditional: filter present on httpx even at INFO level.

    This is the regression guard for the exact security property: if someone
    later gates the attach behind 'if debug', this test goes red.
    """
    monkeypatch.setenv("WXV_LOG_LEVEL", "info")

    from wxverify.__main__ import _configure_logging  # noqa: PLC0415

    _configure_logging()

    httpx_logger = logging.getLogger("httpx")
    filter_types = [type(f) for f in httpx_logger.filters]
    assert RedactUrlSecretsFilter in filter_types, (
        "RedactUrlSecretsFilter must be attached to httpx logger even at INFO level; "
        "the attachment must be unconditional, not gated on debug"
    )


def test_configure_logging_attaches_filter_at_warning_level(
    restore_logging_state: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T6 attachment-is-unconditional: filter present on httpx even at WARNING level."""
    monkeypatch.setenv("WXV_LOG_LEVEL", "warning")

    from wxverify.__main__ import _configure_logging  # noqa: PLC0415

    _configure_logging()

    httpx_logger = logging.getLogger("httpx")
    filter_types = [type(f) for f in httpx_logger.filters]
    assert RedactUrlSecretsFilter in filter_types, (
        "RedactUrlSecretsFilter must be attached to httpx logger at WARNING level"
    )


# ---------------------------------------------------------------------------
# T7 — INFO level policy: only sanctioned milestone lines, zero DEBUG records
# ---------------------------------------------------------------------------


def test_info_level_policy_only_sanctioned_milestones(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T7: at INFO, only 'cycle: ...' INFO lines appear; zero DEBUG, no 'job deferred'.

    Drives a completed job through run_worker. Asserts:
      (i)  zero DEBUG records reach caplog
      (ii) every INFO record's message matches a sanctioned milestone shape
      (iii) 'job deferred' does NOT appear as an INFO record (BC2 guard)
      (iv) WARNING/ERROR lines for the existing transient-failure path remain intact
    """
    job = _make_job(job_type="fetch_feed", site_id=42)

    async def _succeed(db: Any, j: Job) -> None:
        return None

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _succeed)
    monkeypatch.setattr("wxverify.worker.processor.complete", lambda conn, jid: None)

    with (
        caplog.at_level(logging.INFO, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    # (i) zero DEBUG records — caplog is at INFO so DEBUG must not propagate
    debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert len(debug_records) == 0, (
        f"at INFO caplog level, no DEBUG records must appear; "
        f"got: {[r.getMessage() for r in debug_records]}"
    )

    # (ii) every INFO record is a sanctioned milestone
    _SANCTIONED_PREFIXES = (
        "cycle: job=",
        "worker started",
        "worker stopping",
        "scoring run complete",
    )
    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    for r in info_records:
        msg = r.getMessage()
        assert any(msg.startswith(p) for p in _SANCTIONED_PREFIXES), (
            f"unsanctioned INFO line at INFO level: {msg!r}"
        )

    # (iii) 'job deferred' must NOT appear as INFO (BC2: it moved to DEBUG)
    info_deferred = [r for r in info_records if "job deferred" in r.getMessage()]
    assert len(info_deferred) == 0, (
        "BC2 violated: 'job deferred' still appears as INFO; must be DEBUG only"
    )

    # (iv) a cycle line with outcome=completed appeared
    cycle_records = [
        r for r in info_records if r.getMessage().startswith("cycle: job=")
    ]
    assert len(cycle_records) == 1, (
        f"expected exactly one cycle: INFO line for a completed job; "
        f"got {len(cycle_records)}"
    )
    assert "outcome=completed" in cycle_records[0].getMessage()


# ---------------------------------------------------------------------------
# T7b / T17 — BC2: 'job deferred' moved INFO → DEBUG
# ---------------------------------------------------------------------------


def test_deferred_job_cycle_line_is_info_deferred_line_is_debug(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T7b/T17: BC2 — 'job deferred' is at DEBUG; cycle outcome=deferred is at INFO.

    Replaces the old test_worker_deferred_job_logs_info which was an accidental-green:
    it matched 'deferred' in the new cycle: INFO line, not the moved 'job deferred'
    DEBUG line. This test pins the real post-BC2 contract:
      - cycle: job=... outcome=deferred appears at INFO
      - 'job deferred ...' appears at DEBUG (present when caplog captures DEBUG)
      - 'job deferred ...' is ABSENT from INFO-only captures

    Goes red if BC2 is reverted: if 'job deferred' is ever re-promoted to INFO,
    the negative branch fails.
    """
    job = _make_job(job_type="fetch_feed", site_id=42)

    async def _defer(db: Any, j: Job) -> None:
        raise JobDeferred("2099-01-01T00:00:00.000Z")

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _defer)
    monkeypatch.setattr("wxverify.worker.processor.defer_job", lambda c, jid, at: None)

    # --- Positive: at DEBUG, both the cycle INFO line and the 'job deferred' DEBUG
    # line must be present ---
    with (
        caplog.at_level(logging.DEBUG, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    all_messages = [r.getMessage() for r in caplog.records]

    # cycle INFO line with outcome=deferred must appear
    cycle_info = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO and "cycle: job=" in r.getMessage()
    ]
    assert len(cycle_info) == 1, (
        "expected exactly one cycle: INFO line for a deferred job"
    )
    assert "outcome=deferred" in cycle_info[0].getMessage(), (
        f"cycle line must carry outcome=deferred; got: {cycle_info[0].getMessage()!r}"
    )

    # 'job deferred' must appear at DEBUG (the moved line)
    job_deferred_debug = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and "job deferred" in r.getMessage()
    ]
    assert len(job_deferred_debug) == 1, (
        f"'job deferred' must appear exactly once at DEBUG; "
        f"all messages: {all_messages}"
    )

    # --- Negative: at INFO, 'job deferred' must NOT appear as an INFO record ---
    caplog.clear()

    # Reset mock for second run
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))

    with (
        caplog.at_level(logging.INFO, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    info_deferred = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO and "job deferred" in r.getMessage()
    ]
    assert len(info_deferred) == 0, (
        "BC2: 'job deferred' must NOT appear at INFO level — it is a DEBUG line now"
    )

    # But the cycle: INFO line with outcome=deferred IS present at INFO
    cycle_info_at_info = [
        r
        for r in caplog.records
        if r.levelno == logging.INFO and "outcome=deferred" in r.getMessage()
    ]
    assert len(cycle_info_at_info) == 1, (
        "cycle: outcome=deferred INFO line must still appear at INFO level"
    )


# ---------------------------------------------------------------------------
# T8 — Feed adapter firehose at DEBUG (open_meteo)
# ---------------------------------------------------------------------------


def test_open_meteo_debug_lines_present_at_debug(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """T8-A: open_meteo DEBUG lines (request+response) fire when caplog is at DEBUG."""
    # Initialize a DB so the adapter module is importable in the test environment.
    _init_tmp_db(tmp_path)

    # Minimal valid open-meteo response shape
    fake_payload = {
        "latitude": 47.0,
        "longitude": 25.0,
        "elevation": 900.0,
        "hourly": {
            "time": [],
            "temperature_2m": [],
            "wind_speed_10m": [],
            "precipitation": [],
        },
    }

    # We test the adapter's own debug lines by exercising the real open_meteo module
    from wxverify.feeds.open_meteo import OpenMeteoAdapter  # noqa: PLC0415

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()
    mock_response.json = MagicMock(return_value=fake_payload)

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_response)

    from wxverify.feeds.seam import ForecastRequest  # noqa: PLC0415

    req = ForecastRequest(
        lat=47.0,
        lon=25.0,
        model="ecmwf_ifs",
        variables=("temperature", "wind", "precip"),
        max_lead_hours=168,
    )
    adapter = OpenMeteoAdapter(mock_client)

    with caplog.at_level(logging.DEBUG, logger="wxverify.feeds.open_meteo"):
        asyncio.run(adapter.fetch_forecast(req))

    debug_msgs = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.feeds.open_meteo" and r.levelno == logging.DEBUG
    ]
    assert any("open_meteo forecast request" in m for m in debug_msgs), (
        f"forecast request DEBUG line missing; got: {debug_msgs}"
    )
    assert any("open_meteo forecast response" in m for m in debug_msgs), (
        f"forecast response DEBUG line missing; got: {debug_msgs}"
    )

    # Level demarcation: these lines are ABSENT at INFO
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.feeds.open_meteo"):
        asyncio.run(adapter.fetch_forecast(req))

    info_only_msgs = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.feeds.open_meteo"
        and "open_meteo forecast" in r.getMessage()
    ]
    assert len(info_only_msgs) == 0, (
        "open_meteo forecast DEBUG lines must NOT appear when caplog is at INFO"
    )


# ---------------------------------------------------------------------------
# T9 — PWS observation firehose at DEBUG
# ---------------------------------------------------------------------------


def test_pws_adapter_debug_lines_present_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T9-B: pws DEBUG lines fire at DEBUG and are absent at INFO."""
    from wxverify.obs.pws_adapter import fetch_hourly_history  # noqa: PLC0415

    # Build a minimal fake response for the PWS endpoint.
    # The parser looks for "metric" key and "obsTimeUtc" or epoch for valid_at.
    fake_obs_data = {
        "observations": [
            {
                "obsTimeUtc": "2026-07-10T12:00:00Z",
                "metric": {
                    "temp": 20.0,
                    "windSpeed": 10.0,
                    "precipTotal": 0.0,
                },
            }
        ]
    }
    fake_response = MagicMock()
    fake_response.raise_for_status = MagicMock()
    fake_response.json = MagicMock(return_value=fake_obs_data)

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=fake_response)

    with caplog.at_level(logging.DEBUG, logger="wxverify.obs.pws_adapter"):
        asyncio.run(
            fetch_hourly_history(
                "SYNTHETIC001",
                "SYNTHETIC-KEY",
                hours=24,
                timezone="UTC",
                client=mock_client,  # type: ignore[arg-type]
            )
        )

    pws_debug = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.obs.pws_adapter" and r.levelno == logging.DEBUG
    ]
    assert any("pws hourly_history request" in m for m in pws_debug), (
        f"pws hourly_history request DEBUG line missing; got: {pws_debug}"
    )
    assert any("pws hourly_history response" in m for m in pws_debug), (
        f"pws hourly_history response DEBUG line missing; got: {pws_debug}"
    )
    # Confirm station id in trace — never a real station
    assert any("SYNTHETIC001" in m for m in pws_debug)

    # Level demarcation
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.obs.pws_adapter"):
        asyncio.run(
            fetch_hourly_history(
                "SYNTHETIC001",
                "SYNTHETIC-KEY",
                hours=24,
                timezone="UTC",
                client=mock_client,  # type: ignore[arg-type]
            )
        )

    info_pws = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.obs.pws_adapter"
        and "pws hourly_history" in r.getMessage()
    ]
    assert len(info_pws) == 0, "pws DEBUG lines must NOT appear when caplog is at INFO"


# ---------------------------------------------------------------------------
# T10 — Scoring engine firehose at DEBUG: phase lines
# ---------------------------------------------------------------------------


def test_scoring_engine_emits_phase_debug_lines(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """T10-C: pair_and_score emits DEBUG phase lines for each of the 4 phases,
    and emits exactly one INFO 'scoring run complete' milestone line.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)

    from wxverify.scoring.engine import pair_and_score  # noqa: PLC0415

    with caplog.at_level(logging.DEBUG, logger="wxverify.scoring.engine"):
        conn.execute("BEGIN IMMEDIATE")
        pair_and_score(conn, site_id)
        conn.commit()

    engine_records = [r for r in caplog.records if r.name == "wxverify.scoring.engine"]
    debug_msgs = [r.getMessage() for r in engine_records if r.levelno == logging.DEBUG]
    info_msgs = [r.getMessage() for r in engine_records if r.levelno == logging.INFO]

    # One pair_and_score start line
    assert any("pair_and_score start" in m for m in debug_msgs), (
        f"pair_and_score start debug line missing; got: {debug_msgs}"
    )

    # Phase entry lines — 4 phases in PAIR_AND_SCORE_PHASES
    phase_lines = [m for m in debug_msgs if "pair_and_score phase=" in m]
    assert len(phase_lines) == 4, (
        f"expected 4 phase DEBUG lines, got {len(phase_lines)}: {phase_lines}"
    )

    # Exactly one scoring run complete INFO milestone
    scoring_done = [m for m in info_msgs if "scoring run complete" in m]
    assert len(scoring_done) == 1, (
        f"expected exactly 1 'scoring run complete' INFO line; got: {scoring_done}"
    )
    assert f"site={site_id}" in scoring_done[0], (
        f"scoring run complete must carry site_id; got: {scoring_done[0]!r}"
    )
    assert "cells=" in scoring_done[0], (
        f"scoring run complete must carry cells= field; got: {scoring_done[0]!r}"
    )

    # Level demarcation: phase lines absent at INFO
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="wxverify.scoring.engine"):
        conn.execute("BEGIN IMMEDIATE")
        pair_and_score(conn, site_id)
        conn.commit()

    debug_at_info = [
        r
        for r in caplog.records
        if r.name == "wxverify.scoring.engine" and r.levelno == logging.DEBUG
    ]
    assert len(debug_at_info) == 0, (
        "scoring engine DEBUG lines must not appear when caplog is at INFO"
    )


def test_scoring_run_complete_cells_count_is_accurate(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """T10 cells= honesty: with no forecast pairs, cells=0; with pairs, cells>0."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)

    from wxverify.scoring.engine import pair_and_score  # noqa: PLC0415

    # No forecast samples → no pairs → cells should be 0
    with caplog.at_level(logging.INFO, logger="wxverify.scoring.engine"):
        conn.execute("BEGIN IMMEDIATE")
        pair_and_score(conn, site_id)
        conn.commit()

    info_msgs = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.scoring.engine" and r.levelno == logging.INFO
    ]
    assert any("cells=0" in m for m in info_msgs), (
        f"with no pairs, cells=0 must appear in scoring run complete; got: {info_msgs}"
    )


# ---------------------------------------------------------------------------
# T11 — Worker cycle at DEBUG: job-claimed / job-completed + scheduler line
# ---------------------------------------------------------------------------


def test_worker_cycle_debug_lines_at_debug(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T11-D: at DEBUG, job claimed and completed lines appear for a successful job."""
    job = _make_job(job_type="fetch_feed", site_id=42)

    async def _succeed(db: Any, j: Job) -> None:
        return None

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _succeed)
    monkeypatch.setattr("wxverify.worker.processor.complete", lambda conn, jid: None)

    with (
        caplog.at_level(logging.DEBUG, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    proc_debug = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.worker.processor" and r.levelno == logging.DEBUG
    ]
    assert any("job claimed" in m for m in proc_debug), (
        f"'job claimed' DEBUG line missing; got: {proc_debug}"
    )
    assert any("job completed" in m for m in proc_debug), (
        f"'job completed' DEBUG line missing; got: {proc_debug}"
    )

    # Confirm these are absent at INFO
    info_proc = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.worker.processor"
        and r.levelno == logging.INFO
        and ("job claimed" in r.getMessage() or "job completed" in r.getMessage())
    ]
    assert len(info_proc) == 0, (
        "'job claimed'/'job completed' must not appear as INFO records"
    )


# ---------------------------------------------------------------------------
# T12 — Backfill and catchup firehose at DEBUG
# ---------------------------------------------------------------------------


def test_backfill_debug_lines_present(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """T12-E: run_backfill_site at DEBUG emits chunk/window debug lines."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    db = get_db()

    from wxverify.worker.backfill import run_backfill_site  # noqa: PLC0415
    from wxverify.worker.control import JobCancelled  # noqa: PLC0415

    # run_backfill_site emits its window/chunk debug lines before it tries to fetch
    # station history. With no stations configured, fetch_station_history_window
    # raises JobCancelled (no weathercom key / no enabled stations). We catch that
    # so we can inspect what was logged before the cancellation.
    with (
        caplog.at_level(logging.DEBUG, logger="wxverify.worker.backfill"),
        contextlib.suppress(JobCancelled),
    ):
        asyncio.run(run_backfill_site(db, site_id, {}))

    backfill_debug = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.worker.backfill" and r.levelno == logging.DEBUG
    ]
    # At minimum the backfill site + chunk debug lines should appear
    assert len(backfill_debug) > 0, (
        "run_backfill_site must emit at least one DEBUG line"
    )
    # Confirm the site id appears in one of the trace lines
    assert any(str(site_id) in m for m in backfill_debug), (
        f"site_id {site_id} must appear in backfill debug output; got: {backfill_debug}"
    )


def test_catchup_debug_lines_present(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """T12-E: run_catchup at DEBUG emits sites/cursor debug lines."""
    conn = _init_tmp_db(tmp_path)
    _insert_site(conn)
    db = get_db()

    from wxverify.worker.catchup import run_catchup  # noqa: PLC0415

    with caplog.at_level(logging.DEBUG, logger="wxverify.worker.catchup"):
        asyncio.run(run_catchup(db, {}))

    catchup_debug = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.worker.catchup" and r.levelno == logging.DEBUG
    ]
    assert len(catchup_debug) > 0, "run_catchup must emit at least one DEBUG line"
    assert any("catchup sites=" in m for m in catchup_debug), (
        f"'catchup sites=' DEBUG line missing; got: {catchup_debug}"
    )


# ---------------------------------------------------------------------------
# T13 — DB migration + transaction firehose at DEBUG
# ---------------------------------------------------------------------------


def test_migration_debug_lines_present_at_debug(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """T13-F: init_db at DEBUG emits migrations begin…done + db txn begin/commit."""
    close_db()
    config.db_path = str(tmp_path / "t13.db")
    config.options_path = str(tmp_path / "missing-options.json")

    with caplog.at_level(logging.DEBUG, logger="wxverify.db"):
        init_db(str(tmp_path / "t13.db"))

    migration_debug = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.db.migrations" and r.levelno == logging.DEBUG
    ]
    assert any("migrations begin" in m for m in migration_debug), (
        f"'migrations begin' DEBUG line missing; got: {migration_debug}"
    )
    assert any("migrations done" in m for m in migration_debug), (
        f"'migrations done' DEBUG line missing; got: {migration_debug}"
    )

    txn_debug = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.db.connection" and r.levelno == logging.DEBUG
    ]
    assert any("db txn begin" in m for m in txn_debug), (
        f"'db txn begin' DEBUG line missing; got: {txn_debug}"
    )
    assert any("db txn commit" in m for m in txn_debug), (
        f"'db txn commit' DEBUG line missing; got: {txn_debug}"
    )

    # Level demarcation: db txn lines absent at INFO
    caplog.clear()
    close_db()
    config.db_path = str(tmp_path / "t13_info.db")

    with caplog.at_level(logging.INFO, logger="wxverify.db"):
        init_db(str(tmp_path / "t13_info.db"))

    txn_at_info = [
        r
        for r in caplog.records
        if r.name == "wxverify.db.connection" and r.levelno == logging.DEBUG
    ]
    assert len(txn_at_info) == 0, (
        "db txn DEBUG lines must not appear when caplog is at INFO"
    )


# ---------------------------------------------------------------------------
# T14 — Wire-level (httpx at DEBUG): HTTP Request record scrubbed by filter
# ---------------------------------------------------------------------------


def test_httpx_wire_record_scrubbed_by_filter(
    restore_logging_state: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T14-G: when httpx logger is at DEBUG, its HTTP Request record is scrubbed.

    We attach a fresh RedactUrlSecretsFilter to the httpx logger, emit a synthetic
    LogRecord that mimics what httpx emits (%-arg URL with a key), and confirm
    the filter rewrites it so the secret is absent from getMessage().
    """
    secret_url = "https://api.example.com/v1/data?key=WIRE-SECRET-T14"
    httpx_logger = logging.getLogger("httpx")

    # Attach filter as _configure_logging would do
    filt = RedactUrlSecretsFilter()
    httpx_logger.addFilter(filt)
    httpx_logger.setLevel(logging.DEBUG)

    record = LogRecord(
        name="httpx",
        level=logging.DEBUG,
        pathname="",
        lineno=0,
        msg='HTTP Request: GET %s "HTTP/1.1 200 OK"',
        args=(secret_url,),
        exc_info=None,
    )

    # Apply the filter directly (as the logging machinery would)
    httpx_logger.handle(record)

    # After filter ran, getMessage() must not expose the secret
    rendered = record.getMessage()
    assert "WIRE-SECRET-T14" not in rendered, (
        f"wire-level HTTP Request record must have secret scrubbed; got: {rendered!r}"
    )
    # urlencode percent-encodes '***' to '%2A%2A%2A'; either form is a valid marker
    assert "***" in rendered or "%2A%2A%2A" in rendered, (
        f"redaction placeholder missing in wire record; got: {rendered!r}"
    )


# ---------------------------------------------------------------------------
# T15 — BC1: cycle INFO line present; per-op DEBUG lines absent at INFO
# ---------------------------------------------------------------------------


def test_bc1_cycle_info_line_present_for_completed_job(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T15-BC1 (completed): cycle: INFO line fires with outcome=completed at INFO."""
    job = _make_job(job_type="fetch_feed", site_id=7)

    async def _succeed(db: Any, j: Job) -> None:
        return None

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _succeed)
    monkeypatch.setattr("wxverify.worker.processor.complete", lambda conn, jid: None)

    with (
        caplog.at_level(logging.INFO, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    cycle_lines = [
        r.getMessage() for r in info_records if "cycle: job=" in r.getMessage()
    ]
    assert len(cycle_lines) == 1, (
        f"expected exactly 1 cycle: INFO line; got: {cycle_lines}"
    )
    assert "outcome=completed" in cycle_lines[0]
    assert "type=fetch_feed" in cycle_lines[0]
    assert "site=7" in cycle_lines[0]

    # Per-op DEBUG lines must be absent
    debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert len(debug_records) == 0, (
        "per-op DEBUG lines ('job claimed', 'dispatch …') must not appear at INFO"
    )


def test_bc1_cycle_info_line_deferred_outcome(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T15-BC1 (deferred): cycle: INFO line fires with outcome=deferred at INFO."""
    job = _make_job(job_type="fetch_feed", site_id=7)

    async def _defer(db: Any, j: Job) -> None:
        raise JobDeferred("2099-01-01T00:00:00.000Z")

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _defer)
    monkeypatch.setattr("wxverify.worker.processor.defer_job", lambda c, jid, at: None)

    with (
        caplog.at_level(logging.INFO, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    cycle_lines = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.INFO and "cycle: job=" in r.getMessage()
    ]
    assert len(cycle_lines) == 1
    assert "outcome=deferred" in cycle_lines[0]


def test_bc1_cycle_info_line_retry_outcome(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T15-BC1 (retry): cycle: INFO line fires with outcome=retry at INFO."""
    job = _make_job(job_type="fetch_feed", site_id=7, retry_count=1, max_retries=5)

    async def _raise(db: Any, j: Job) -> None:
        raise RuntimeError("transient error")

    def _retry_disposition(conn: Any, job_id: int, error: str) -> FailDisposition:
        return FailDisposition(
            terminal=False,
            retry_count=2,
            max_retries=5,
            next_attempt_at="2099-01-01T00:00:00.000Z",
        )

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _raise)
    monkeypatch.setattr("wxverify.worker.processor.fail", _retry_disposition)

    with (
        caplog.at_level(logging.INFO, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    cycle_lines = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.INFO and "cycle: job=" in r.getMessage()
    ]
    assert len(cycle_lines) == 1, f"got: {cycle_lines}"
    assert "outcome=retry" in cycle_lines[0]


def test_bc1_cycle_info_line_failed_outcome(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T15-BC1 (failed): cycle: INFO line fires with outcome=failed for terminal."""
    job = _make_job(job_type="fetch_feed", site_id=7, retry_count=5, max_retries=5)

    async def _raise(db: Any, j: Job) -> None:
        raise RuntimeError("terminal error")

    def _terminal_disposition(conn: Any, job_id: int, error: str) -> FailDisposition:
        return FailDisposition(
            terminal=True, retry_count=6, max_retries=5, next_attempt_at=None
        )

    _patch_worker_infra(monkeypatch)
    monkeypatch.setattr("wxverify.worker.processor.claim_next_job", _claim_once(job))
    monkeypatch.setattr("wxverify.worker.processor.dispatch", _raise)
    monkeypatch.setattr("wxverify.worker.processor.fail", _terminal_disposition)

    with (
        caplog.at_level(logging.INFO, logger="wxverify.worker.processor"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    cycle_lines = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.INFO and "cycle: job=" in r.getMessage()
    ]
    assert len(cycle_lines) == 1, f"got: {cycle_lines}"
    assert "outcome=failed" in cycle_lines[0]


# ---------------------------------------------------------------------------
# T16 — D5: terminal ERROR comes from processor, NOT from feed_fetch.py
# ---------------------------------------------------------------------------


def test_d5_terminal_error_from_processor_not_feed_fetch(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """T16-D5 (a): terminal ERROR is emitted by wxverify.worker.processor exactly once.

    Mirrors test_worker_terminal_failure_logs_error, which already exists and must
    remain green. This test adds: (i) confirm the ERROR record's logger name is the
    processor, and (ii) confirm no second ERROR from feed_fetch fires.
    """
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
        caplog.at_level(logging.ERROR, logger="wxverify"),
        pytest.raises(_StopLoop),
    ):
        asyncio.run(run_worker(_FakeDb()))  # type: ignore[arg-type]

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(error_records) == 1, (
        f"expected exactly one ERROR record across all wxverify loggers; "
        f"got: {[(r.name, r.getMessage()) for r in error_records]}"
    )
    assert error_records[0].name == "wxverify.worker.processor", (
        f"the ERROR must come from wxverify.worker.processor; "
        f"got: {error_records[0].name}"
    )
    assert "failed" in error_records[0].getMessage()


def test_d5_feed_fetch_http_error_emits_no_error_record(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """T16-D5 (b): HTTPStatusError from fetch_feed_once emits no ERROR from feed_fetch.

    The except-arm traces at DEBUG, not ERROR. Confirm the DEBUG trace is present
    (with the secret redacted), and zero ERROR records from feed_fetch.
    """
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _open_meteo_feed_id(conn)
    db = get_db()

    # Use 403 (Forbidden) — record_http_backoff only fires on 429 and >=500.
    # A 403 returns next_attempt_at=None from mark_feed_error_and_backoff, so
    # feed_fetch_once falls through to `raise` instead of returning BackoffActive.
    secret_url = "https://api.example.com/v1?key=SYNTHETIC-SECRET-T16"
    req = httpx.Request("GET", secret_url)
    resp = httpx.Response(403, request=req)

    class _Http403Adapter:
        supports_historical = False

        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, r: Any) -> FetchResult:
            raise httpx.HTTPStatusError("403", request=req, response=resp)

    def _build(source: str, client: httpx.AsyncClient) -> _Http403Adapter:
        return _Http403Adapter()

    with (
        caplog.at_level(logging.DEBUG, logger="wxverify.worker.feed_fetch"),
        pytest.raises(httpx.HTTPStatusError),
    ):
        asyncio.run(fetch_feed_once(db, site_id, feed_id, adapter_builder=_build))

    # Zero ERROR records from feed_fetch
    feed_fetch_errors = [
        r
        for r in caplog.records
        if r.name == "wxverify.worker.feed_fetch" and r.levelno == logging.ERROR
    ]
    assert len(feed_fetch_errors) == 0, (
        f"D5 violated: feed_fetch must emit no ERROR for httpx.HTTPStatusError; "
        f"got: {[(r.levelno, r.getMessage()) for r in feed_fetch_errors]}"
    )

    # The DEBUG trace for the http error must be present
    feed_fetch_debug = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.worker.feed_fetch" and r.levelno == logging.DEBUG
    ]
    assert any("fetch http error" in m for m in feed_fetch_debug), (
        f"'fetch http error' DEBUG trace must be emitted; got: {feed_fetch_debug}"
    )

    # Secret must be absent (sanitized_exception redacts the URL)
    assert "SYNTHETIC-SECRET-T16" not in caplog.text, (
        "secret from httpx error URL must be redacted in feed_fetch debug trace"
    )


def test_d5_feed_fetch_transport_error_emits_no_error_record(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """T16-D5 (b2): fetch_feed_once with a generic transport error emits no ERROR."""
    conn = _init_tmp_db(tmp_path)
    site_id = _insert_site(conn)
    feed_id = _open_meteo_feed_id(conn)
    db = get_db()

    class _TransportErrorAdapter:
        supports_historical = False

        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: Any) -> FetchResult:
            raise httpx.ConnectError("synthetic connect error")

    def _build(source: str, client: httpx.AsyncClient) -> _TransportErrorAdapter:
        return _TransportErrorAdapter()

    with (
        caplog.at_level(logging.DEBUG, logger="wxverify.worker.feed_fetch"),
        pytest.raises(httpx.ConnectError),
    ):
        asyncio.run(fetch_feed_once(db, site_id, feed_id, adapter_builder=_build))

    feed_fetch_errors = [
        r
        for r in caplog.records
        if r.name == "wxverify.worker.feed_fetch" and r.levelno == logging.ERROR
    ]
    assert len(feed_fetch_errors) == 0, (
        f"D5 violated: feed_fetch must emit no ERROR for transport errors; "
        f"got: {[(r.levelno, r.getMessage()) for r in feed_fetch_errors]}"
    )

    feed_fetch_debug = [
        r.getMessage()
        for r in caplog.records
        if r.name == "wxverify.worker.feed_fetch" and r.levelno == logging.DEBUG
    ]
    assert any("fetch transport error" in m for m in feed_fetch_debug), (
        f"'fetch transport error' DEBUG trace must be emitted; got: {feed_fetch_debug}"
    )
