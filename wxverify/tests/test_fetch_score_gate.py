"""Tests for the fetch-triggered scoring gate (plan: fetch-scoring-gate §11).

Covers §11.2 (the gate on the fetch path). §11.1's probe tests
(`real_model_pair_pending`) were removed: the probe's answer only decides
whether to call `enqueue_scoring_run`, which enqueues nothing with the daily
close on (scoring/engine.py:122-124) -- the add-on's only install runs with
the close on. The probe still runs on every download regardless
(feed_fetch.py:556), which is why the live-path probe-error test
(`test_gate_live_path_probe_error_rolls_back_fetch`) and the probe-timing
log tests below stay. Synthetic fixtures: 'Testsite' at 12.3456/65.4321,
dates in 2035 (plan §11).
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import types
from pathlib import Path
from typing import Any

import pytest

from wxverify import config
from wxverify.core.timeutil import utc_now
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.migrations import run_migrations
from wxverify.feeds.seam import CostEstimate, FetchResult, NormalizedSample
from wxverify.worker import feed_fetch as feed_fetch_module
from wxverify.worker.feed_fetch import (
    DownloadSlot,
    FeedFetchTarget,
    _persist_fetch_success,
    fetch_feed_once,
)

_SITE_NAME = "Testsite"
_LAT = 12.3456
_LON = 65.4321


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", cached_statements=0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    run_migrations(conn)
    return conn


def _seed_site(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    timezone: str = "UTC",
    enabled: int = 1,
) -> int:
    conn.execute(
        "INSERT INTO sites (id, name, forecast_lat, forecast_lon, elevation_m,"
        " timezone, enabled) VALUES (?, ?, ?, ?, 100.0, ?, ?)",
        (site_id, _SITE_NAME, _LAT, _LON, timezone, enabled),
    )
    return site_id


def _seed_feed(
    conn: sqlite3.Connection, *, max_lead_hours: int = 168, is_virtual: int = 0
) -> int:
    # Derived from the current row count, so uniqueness never depends on
    # cross-test ordering.
    n = conn.execute("SELECT COUNT(*) AS n FROM feeds").fetchone()["n"]
    cur = conn.execute(
        "INSERT INTO feeds (source, model, fetch_interval_minutes,"
        " max_lead_hours, is_virtual) VALUES ('open-meteo', ?, 60, ?, ?)",
        (f"probe-model-{n}", max_lead_hours, is_virtual),
    )
    feed_id = cur.lastrowid
    assert feed_id is not None
    return int(feed_id)


def _feed_model(conn: sqlite3.Connection, feed_id: int) -> str:
    row = conn.execute("SELECT model FROM feeds WHERE id = ?", (feed_id,)).fetchone()
    assert row is not None
    return str(row["model"])


def _seed_sample(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    feed_id: int,
    variable: str = "temperature",
    issued_at: str,
    valid_at: str,
    lead_hours: int = 1,
    value: float = 10.0,
) -> int:
    cur = conn.execute(
        "INSERT INTO forecast_samples (site_id, feed_id, variable, issued_at,"
        " valid_at, lead_hours, value, source_raw, model_run_id, fetched_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, '{}', 'run-1', ?)",
        (site_id, feed_id, variable, issued_at, valid_at, lead_hours, value, issued_at),
    )
    sample_id = cur.lastrowid
    assert sample_id is not None
    return int(sample_id)


def _seed_obs(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    variable: str = "temperature",
    valid_at: str,
    value: float = 9.0,
) -> None:
    conn.execute(
        "INSERT INTO observations (site_id, variable, valid_at, value, n_stations)"
        " VALUES (?, ?, ?, ?, 1)",
        (site_id, variable, valid_at, value),
    )


# ---------------------------------------------------------------------------
# 11.1 Probe (real_model_pair_pending) -- removed. The probe's answer only
# decides whether to call enqueue_scoring_run, which enqueues nothing with
# the daily close on (scoring/engine.py:122-124) -- the add-on's only
# install runs with the close on.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 11.2 Gate on the fetch path
# ---------------------------------------------------------------------------


def _pairable_result(
    model: str, *, issued_at: str, valid_at: str, lead_hours: int = 1
) -> FetchResult:
    sample = NormalizedSample(
        model=model,
        variable="temperature",
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=lead_hours,
        value=10.0,
        source_raw="{}",
        model_run_id="run-1",
    )
    return FetchResult(samples=[sample], grid=None)


def _target(conn: sqlite3.Connection, feed_id: int) -> FeedFetchTarget:
    return FeedFetchTarget(
        site_id=1,
        feed_id=feed_id,
        lat=_LAT,
        lon=_LON,
        source="open-meteo",
        model=_feed_model(conn, feed_id),
        max_lead_hours=168,
    )


def _score_job_count(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM jobs"
            " WHERE type='pair_and_score' AND job_key='score'"
        ).fetchone()["n"]
    )


@pytest.mark.parametrize("fenced", [False, True], ids=["unfenced", "fenced"])
def test_gate_live_path_probe_error_rolls_back_fetch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fenced: bool
) -> None:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db_obj = init_db(str(db_path))
    conn = db_obj._conn  # noqa: SLF001
    site_id = _seed_site(conn)
    feed_id = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' AND is_virtual=0"
        " ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    model = _feed_model(conn, feed_id)
    _seed_obs(conn, site_id=site_id, valid_at="2035-03-02T00:30:00Z")
    db = get_db()
    # Same rationale as the enqueue-failure test above: both writer paths
    # share db_obj._conn, so one assertion target covers both parametrized
    # cases.
    writer = FencedWriter(db, db.generation) if fenced else None

    class _PairableAdapter:
        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: Any) -> FetchResult:
            return _pairable_result(
                model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-03-02T00:30:00Z"
            )

    def _build(source: str, client: Any) -> _PairableAdapter:
        return _PairableAdapter()

    exc = sqlite3.DatabaseError("database disk image is malformed")
    exc.sqlite_errorcode = sqlite3.SQLITE_CORRUPT  # type: ignore[attr-defined]

    def _raise_probe(*args: Any, **kwargs: Any) -> bool:
        raise exc

    monkeypatch.setattr(feed_fetch_module, "real_model_pair_pending", _raise_probe)

    with pytest.raises(sqlite3.DatabaseError) as excinfo:
        asyncio.run(
            fetch_feed_once(
                db,
                site_id,
                feed_id,
                writer=writer,
                adapter_builder=_build,
                slot=DownloadSlot(
                    slot="unscheduled",
                    round_date=None,
                    round_try=None,
                    started_at=utc_now(),
                ),
            )
        )

    assert excinfo.value is exc
    assert (
        int(conn.execute("SELECT COUNT(*) AS n FROM forecast_samples").fetchone()["n"])
        == 0
    )
    assert _score_job_count(conn) == 0
    assert conn.in_transaction is False


# ---------------------------------------------------------------------------
# 11.2 The gate's log lines (T-gate-log-*) and slow-probe INFO line
# (T-gate-slow*, T-gate-fast)
# ---------------------------------------------------------------------------


def _info_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        r
        for r in caplog.records
        if r.name == "wxverify.worker.feed_fetch"
        and r.levelno == logging.INFO
        and r.getMessage().startswith("fetch score gate slow")
    ]


def _run_gate(
    conn: sqlite3.Connection, target: FeedFetchTarget, result: FetchResult
) -> None:
    conn.commit()
    conn.execute("BEGIN")
    _persist_fetch_success(conn, target, result)
    conn.commit()


def _patch_monotonic(
    monkeypatch: pytest.MonkeyPatch,
    conn: sqlite3.Connection,
    values: list[float],
) -> dict[str, Any]:
    """Stub `feed_fetch.time.monotonic` with the listed values in order.

    Records the `pair_and_score` jobs row count observed at the SECOND call
    (before the gate's own enqueue runs), and the total call count.
    """
    state: dict[str, Any] = {"calls": 0, "count_at_second_call": None}

    def fake() -> float:
        state["calls"] += 1
        if state["calls"] == 2:
            state["count_at_second_call"] = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM jobs WHERE type = 'pair_and_score'"
                ).fetchone()["n"]
            )
        return values[state["calls"] - 1]

    monkeypatch.setattr(
        feed_fetch_module, "time", types.SimpleNamespace(monotonic=fake)
    )
    return state


def test_gate_slow_probe_logs_info_over_the_threshold(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _conn()
    _seed_site(conn)
    feed_id = _seed_feed(conn)
    target = _target(conn, feed_id)
    _seed_obs(conn, valid_at="2035-03-02T00:30:00Z")
    result = _pairable_result(
        target.model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-03-02T00:30:00Z"
    )
    state = _patch_monotonic(monkeypatch, conn, [100.0, 100.3])

    with caplog.at_level(logging.INFO, logger="wxverify.worker.feed_fetch"):
        _run_gate(conn, target, result)

    records = _info_records(caplog)
    assert len(records) == 1
    msg = records[0].getMessage()
    assert f"site=1 feed={feed_id} ids=1" in msg
    assert "elapsed=0.300s" in msg
    assert state["calls"] == 2
    assert state["count_at_second_call"] == 0


def test_gate_fast_probe_logs_no_info(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _conn()
    _seed_site(conn)
    feed_id = _seed_feed(conn)
    target = _target(conn, feed_id)
    _seed_obs(conn, valid_at="2035-03-02T00:30:00Z")
    result = _pairable_result(
        target.model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-03-02T00:30:00Z"
    )
    state = _patch_monotonic(monkeypatch, conn, [100.0, 100.1])

    with caplog.at_level(logging.INFO, logger="wxverify.worker.feed_fetch"):
        _run_gate(conn, target, result)

    assert _info_records(caplog) == []
    assert state["calls"] == 2
    assert state["count_at_second_call"] == 0


def test_gate_slow_probe_boundary_is_strictly_greater_than(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    conn = _conn()
    _seed_site(conn)
    feed_id = _seed_feed(conn)
    target = _target(conn, feed_id)
    _seed_obs(conn, valid_at="2035-03-02T00:30:00Z")
    result = _pairable_result(
        target.model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-03-02T00:30:00Z"
    )
    # Binary-exact: 100.25 - 100.0 == 0.25 exactly.
    state = _patch_monotonic(monkeypatch, conn, [100.0, 100.25])

    with caplog.at_level(logging.INFO, logger="wxverify.worker.feed_fetch"):
        _run_gate(conn, target, result)

    assert _info_records(caplog) == []
    assert state["calls"] == 2
    assert state["count_at_second_call"] == 0


# ---------------------------------------------------------------------------
# 11.3 Obs, 11.4 Window, and 11.5 Ranking/sweep -- removed. All three drove
# the close-off scoring/leaderboard-cache path (`leaderboard_with_status`,
# `set_rolling_window_days_sync`'s job-enqueue side effects), which no
# install reaches with the daily close on.
# ---------------------------------------------------------------------------
