"""Tests for the fetch-triggered scoring gate (plan: fetch-scoring-gate §11).

Covers §11.1 (the `real_model_pair_pending` probe) and §11.2 (the gate on
the fetch path). Synthetic fixtures: 'Testsite' at 12.3456/65.4321, dates
in 2035 (plan §11).
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
import pytest

import wxverify.core.timeutil as timeutil_module
from wxverify import config
from wxverify.core.timeutil import day_ahead, isoformat_utc
from wxverify.db import queue as queue_module
from wxverify.db.connection import FencedWriter, close_db, get_db, init_db
from wxverify.db.migrations import run_migrations
from wxverify.db.queue import Job, claim_next_job, complete, enqueue_if_absent
from wxverify.feeds.seam import CostEstimate, FetchResult, NormalizedSample
from wxverify.scoring.cache import upsert_score_cache
from wxverify.scoring.leaderboard import leaderboard_with_status
from wxverify.scoring.metrics import MetricResult
from wxverify.scoring.pairing import (
    compute_real_model_pairs,
    real_model_pair_pending,
)
from wxverify.settings.keys import get_setting, set_setting
from wxverify.settings.service import set_rolling_window_days_sync
from wxverify.worker import feed_fetch as feed_fetch_module
from wxverify.worker import scheduler as scheduler_module
from wxverify.worker.feed_fetch import (
    FeedFetchTarget,
    _persist_fetch_success,
    fetch_feed_once,
)
from wxverify.worker.processor import dispatch, run_claimed_job
from wxverify.worker.scheduler import scheduler_tick

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


def _published_generation_id(conn: sqlite3.Connection, site_id: int) -> int:
    from wxverify.db.tz_generations import ensure_published_generation

    return ensure_published_generation(conn, site_id)


def _seed_pair(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    feed_id: int,
    variable: str,
    issued_at: str,
    valid_at: str,
    lead_hours: int,
    tz_generation_id: int,
) -> None:
    bucket = day_ahead(issued_at, valid_at, "UTC")
    conn.execute(
        "INSERT INTO forecast_pairs (site_id, feed_id, variable, issued_at,"
        " valid_at, lead_hours, day_ahead, forecast, observed,"
        " tz_generation_id) VALUES (?, ?, ?, ?, ?, ?, ?, 10.0, 9.0, ?)",
        (
            site_id,
            feed_id,
            variable,
            issued_at,
            valid_at,
            lead_hours,
            bucket,
            tz_generation_id,
        ),
    )


def _candidate(
    conn: sqlite3.Connection,
    *,
    site_id: int = 1,
    variable: str = "temperature",
    issued_at: str = "2035-03-01T00:00:00Z",
    valid_at: str = "2035-03-02T00:30:00Z",
    lead_hours: int = 1,
    max_lead_hours: int = 168,
    is_virtual: int = 0,
    obs_value: float = 9.0,
    with_obs: bool = True,
) -> tuple[int, int]:
    """Seed one pairable candidate sample + its feed (+ obs unless with_obs=False).

    Returns (sample_id, feed_id).
    """
    feed_id = _seed_feed(conn, max_lead_hours=max_lead_hours, is_virtual=is_virtual)
    sample_id = _seed_sample(
        conn,
        site_id=site_id,
        feed_id=feed_id,
        variable=variable,
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=lead_hours,
    )
    if with_obs:
        _seed_obs(
            conn, site_id=site_id, variable=variable, valid_at=valid_at, value=obs_value
        )
    return sample_id, feed_id


# ---------------------------------------------------------------------------
# 11.1 Probe (real_model_pair_pending)
# ---------------------------------------------------------------------------


def test_probe_base_pairable_sample_returns_true() -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, _ = _candidate(conn)
    assert real_model_pair_pending(conn, (sample_id,)) is True


def test_probe_virtual_feed_returns_false() -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, _ = _candidate(conn, is_virtual=1)
    assert real_model_pair_pending(conn, (sample_id,)) is False


def test_probe_lead_at_feed_max_returns_true() -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, _ = _candidate(conn, max_lead_hours=3, lead_hours=3)
    assert real_model_pair_pending(conn, (sample_id,)) is True


def test_probe_lead_over_feed_max_returns_false() -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, _ = _candidate(conn, max_lead_hours=3, lead_hours=4)
    assert real_model_pair_pending(conn, (sample_id,)) is False


def test_probe_no_matching_observation_returns_false() -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, _ = _candidate(conn, with_obs=False)
    assert real_model_pair_pending(conn, (sample_id,)) is False


def test_probe_already_published_pair_returns_false() -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, feed_id = _candidate(conn)
    tz_gen = _published_generation_id(conn, 1)
    _seed_pair(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2035-03-01T00:00:00Z",
        valid_at="2035-03-02T00:30:00Z",
        lead_hours=1,
        tz_generation_id=tz_gen,
    )
    assert real_model_pair_pending(conn, (sample_id,)) is False


def test_probe_pair_under_unpublished_generation_still_pends() -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, feed_id = _candidate(conn)
    # A second, NOT-published generation for the same site.
    cur = conn.execute(
        "INSERT INTO timezone_generations (site_id, timezone, mode, state)"
        " VALUES (1, 'UTC', 'initial', 'building')"
    )
    other_gen = cur.lastrowid
    assert other_gen is not None
    _seed_pair(
        conn,
        feed_id=feed_id,
        variable="temperature",
        issued_at="2035-03-01T00:00:00Z",
        valid_at="2035-03-02T00:30:00Z",
        lead_hours=1,
        tz_generation_id=int(other_gen),
    )
    assert real_model_pair_pending(conn, (sample_id,)) is True


def test_probe_wind_switching_site_returns_false() -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, _ = _candidate(conn, variable="wind")
    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES ('wind_basis:1', 'switching')"
    )
    assert real_model_pair_pending(conn, (sample_id,)) is False


def test_probe_wind_unknown_state_returns_false() -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, _ = _candidate(conn, variable="wind")
    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES ('wind_basis:1', 'bogus')"
    )
    assert real_model_pair_pending(conn, (sample_id,)) is False


@pytest.mark.parametrize("state", ["staging", "rescoring", None])
def test_probe_wind_open_state_returns_true(state: str | None) -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, _ = _candidate(conn, variable="wind")
    if state is None:
        row = conn.execute(
            "SELECT 1 FROM runtime_state WHERE key = 'wind_basis:1'"
        ).fetchone()
        assert row is None, "precondition: no wind_basis row for this site"
    else:
        conn.execute(
            "INSERT INTO runtime_state (key, value) VALUES ('wind_basis:1', ?)",
            (state,),
        )
    assert real_model_pair_pending(conn, (sample_id,)) is True


def test_probe_temperature_unaffected_by_switching_wind_basis() -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, _ = _candidate(conn, variable="temperature")
    conn.execute(
        "INSERT INTO runtime_state (key, value) VALUES ('wind_basis:1', 'switching')"
    )
    assert real_model_pair_pending(conn, (sample_id,)) is True


def test_probe_negative_bucket_returns_false() -> None:
    conn = _conn()
    _seed_site(conn)
    sample_id, _ = _candidate(
        conn,
        issued_at="2035-03-10T00:30:00Z",
        valid_at="2035-03-09T23:30:00Z",
        lead_hours=1,
    )
    assert real_model_pair_pending(conn, (sample_id,)) is False


@pytest.mark.parametrize(
    ("days_later", "expected"),
    [(0, True), (7, True), (8, False)],
)
def test_probe_bucket_boundary(days_later: int, expected: bool) -> None:
    conn = _conn()
    _seed_site(conn)
    issued_at = "2035-03-01T00:00:00Z"
    valid_at = f"2035-03-{1 + days_later:02d}T00:30:00Z"
    sample_id, _ = _candidate(
        conn,
        issued_at=issued_at,
        valid_at=valid_at,
        lead_hours=1,
        max_lead_hours=400,
    )
    assert real_model_pair_pending(conn, (sample_id,)) is expected


def test_probe_bad_timezone_fails_open_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _conn()
    _seed_site(conn, timezone="Not/AZone")
    sample_id, _ = _candidate(conn)
    with caplog.at_level(logging.WARNING, logger="wxverify.scoring.pairing"):
        result = real_model_pair_pending(conn, (sample_id,))
    assert result is True
    assert any("day-ahead bucket failed" in r.getMessage() for r in caplog.records)


def test_probe_order_independent_any_match() -> None:
    conn = _conn()
    _seed_site(conn)
    pairable_id, _ = _candidate(conn)
    unpairable_id, _ = _candidate(conn, valid_at="2035-04-01T00:30:00Z", with_obs=False)
    assert real_model_pair_pending(conn, (unpairable_id, pairable_id)) is True
    assert real_model_pair_pending(conn, (pairable_id, unpairable_id)) is True
    assert real_model_pair_pending(conn, (unpairable_id,)) is False


def test_probe_empty_ids_returns_false() -> None:
    conn = _conn()
    _seed_site(conn)
    assert real_model_pair_pending(conn, ()) is False


def test_probe_matches_compute_real_model_pairs_on_a_mixed_fixture() -> None:
    conn = _conn()
    _seed_site(conn)

    pairable_id, pairable_feed = _candidate(conn, valid_at="2035-03-02T00:30:00Z")
    virtual_id, _ = _candidate(conn, valid_at="2035-03-03T00:30:00Z", is_virtual=1)
    no_obs_id, _ = _candidate(conn, valid_at="2035-03-04T00:30:00Z", with_obs=False)
    tz_gen = _published_generation_id(conn, 1)
    published_id, published_feed = _candidate(conn, valid_at="2035-03-05T00:30:00Z")
    _seed_pair(
        conn,
        feed_id=published_feed,
        variable="temperature",
        issued_at="2035-03-01T00:00:00Z",
        valid_at="2035-03-05T00:30:00Z",
        lead_hours=1,
        tz_generation_id=tz_gen,
    )

    delta = compute_real_model_pairs(conn, 1)

    for sample_id, feed_id, variable, issued_at, valid_at in [
        (
            pairable_id,
            pairable_feed,
            "temperature",
            "2035-03-01T00:00:00Z",
            "2035-03-02T00:30:00Z",
        ),
        (virtual_id, 0, "temperature", "2035-03-01T00:00:00Z", "2035-03-03T00:30:00Z"),
        (no_obs_id, 0, "temperature", "2035-03-01T00:00:00Z", "2035-03-04T00:30:00Z"),
        (
            published_id,
            published_feed,
            "temperature",
            "2035-03-01T00:00:00Z",
            "2035-03-05T00:30:00Z",
        ),
    ]:
        expected = any(
            op.values[1] == feed_id
            and op.values[2] == variable
            and op.values[3] == issued_at
            and op.values[4] == valid_at
            for op in delta.ops
        )
        assert real_model_pair_pending(conn, (sample_id,)) is expected


def test_registry_map_probes_the_stored_feed_not_the_target_feed() -> None:
    conn = _conn()
    _seed_site(conn)
    target_feed = _seed_feed(conn)
    other_feed = _seed_feed(conn)
    other_model = _feed_model(conn, other_feed)
    _seed_obs(conn, valid_at="2035-03-02T00:30:00Z", value=9.0)

    target = FeedFetchTarget(
        site_id=1,
        feed_id=target_feed,
        lat=_LAT,
        lon=_LON,
        source="open-meteo",
        model=_feed_model(conn, target_feed),
        max_lead_hours=168,
    )
    sample = NormalizedSample(
        model=other_model,
        variable="temperature",
        issued_at="2035-03-01T00:00:00Z",
        valid_at="2035-03-02T00:30:00Z",
        lead_hours=1,
        value=10.0,
        source_raw="{}",
        model_run_id="run-1",
    )
    result = FetchResult(samples=[sample], grid=None)

    conn.commit()
    conn.execute("BEGIN")
    outcome = _persist_fetch_success(conn, target, result)
    conn.commit()

    assert outcome.inserted_count == 1
    row = conn.execute(
        "SELECT feed_id FROM forecast_samples WHERE id = ?",
        (outcome.inserted_sample_ids[0],),
    ).fetchone()
    assert row["feed_id"] == other_feed, (
        "sample must register under the stored (second) feed"
    )
    job = conn.execute(
        "SELECT 1 FROM jobs WHERE type='pair_and_score' AND job_key='score'"
    ).fetchone()
    assert job is not None


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


def _pair_count(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) AS n FROM forecast_pairs").fetchone()["n"])


def _score_job_count(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM jobs"
            " WHERE type='pair_and_score' AND job_key='score'"
        ).fetchone()["n"]
    )


def test_gate_pairable_sample_queues_a_job_that_increases_pairs() -> None:
    from wxverify.scoring.engine import pair_and_score

    conn = _conn()
    _seed_site(conn)
    feed_id = _seed_feed(conn)
    target = _target(conn, feed_id)
    _seed_obs(conn, valid_at="2035-03-02T00:30:00Z")
    pair_and_score(conn, 1)
    before = _pair_count(conn)

    result = _pairable_result(
        target.model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-03-02T00:30:00Z"
    )
    conn.commit()
    conn.execute("BEGIN")
    _persist_fetch_success(conn, target, result)
    conn.commit()

    jobs = conn.execute(
        "SELECT type, job_key, status, payload FROM jobs"
        " WHERE type='pair_and_score' AND job_key='score'"
    ).fetchall()
    assert len(jobs) == 1
    job = jobs[0]
    assert job["status"] == "pending"
    import json

    assert json.loads(job["payload"]) == {"site_id": 1}

    pair_and_score(conn, 1)
    assert _pair_count(conn) > before


def test_gate_future_only_sample_queues_nothing_and_stays_sound() -> None:
    from wxverify.scoring.engine import pair_and_score

    conn = _conn()
    _seed_site(conn)
    feed_id = _seed_feed(conn)
    target = _target(conn, feed_id)
    pair_and_score(conn, 1)
    before = _pair_count(conn)

    # No observation exists at this valid_at: future-only / unpairable.
    result = _pairable_result(
        target.model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-06-01T00:30:00Z"
    )
    conn.commit()
    conn.execute("BEGIN")
    _persist_fetch_success(conn, target, result)
    conn.commit()

    assert _score_job_count(conn) == 0
    pair_and_score(conn, 1)
    assert _pair_count(conn) == before


def test_gate_duplicate_refetch_inserts_nothing_and_queues_no_new_job() -> None:
    conn = _conn()
    _seed_site(conn)
    feed_id = _seed_feed(conn)
    target = _target(conn, feed_id)
    _seed_obs(conn, valid_at="2035-03-02T00:30:00Z")

    result = _pairable_result(
        target.model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-03-02T00:30:00Z"
    )
    conn.commit()
    conn.execute("BEGIN")
    _persist_fetch_success(conn, target, result)
    conn.commit()
    assert _score_job_count(conn) == 1

    conn.commit()
    conn.execute("BEGIN")
    outcome = _persist_fetch_success(conn, target, result)
    conn.commit()

    assert outcome.inserted_count == 0
    assert outcome.inserted_sample_ids == ()
    assert _score_job_count(conn) == 1


def test_ids_mixed_payload_selects_only_the_newly_inserted_rows() -> None:
    """T-ids: a payload of two already-stored samples plus one new one must
    report exactly the new one, both by count and by id."""
    conn = _conn()
    _seed_site(conn)
    feed_id = _seed_feed(conn)
    target = _target(conn, feed_id)
    stored_valid_ats = ("2035-03-02T00:30:00Z", "2035-03-02T01:30:00Z")
    new_valid_at = "2035-03-02T02:30:00Z"
    for valid_at in stored_valid_ats:
        _seed_obs(conn, valid_at=valid_at)
    conn.commit()
    conn.execute("BEGIN")
    _persist_fetch_success(
        conn,
        target,
        FetchResult(
            samples=[
                NormalizedSample(
                    model=target.model,
                    variable="temperature",
                    issued_at="2035-03-01T00:00:00Z",
                    valid_at=valid_at,
                    lead_hours=1,
                    value=10.0,
                    source_raw="{}",
                    model_run_id="run-1",
                )
                for valid_at in stored_valid_ats
            ],
            grid=None,
        ),
    )
    conn.commit()

    conn.commit()
    conn.execute("BEGIN")
    mixed_result = FetchResult(
        samples=[
            NormalizedSample(
                model=target.model,
                variable="temperature",
                issued_at="2035-03-01T00:00:00Z",
                valid_at=valid_at,
                lead_hours=1,
                value=10.0,
                source_raw="{}",
                model_run_id="run-1",
            )
            for valid_at in (*stored_valid_ats, new_valid_at)
        ],
        grid=None,
    )
    outcome = _persist_fetch_success(conn, target, mixed_result)
    conn.commit()

    assert outcome.inserted_count == 1
    assert outcome.inserted_count == len(outcome.inserted_sample_ids)
    for sample_id in outcome.inserted_sample_ids:
        row = conn.execute(
            "SELECT feed_id, variable, issued_at, valid_at, lead_hours"
            " FROM forecast_samples WHERE id = ?",
            (sample_id,),
        ).fetchone()
        assert (
            row["feed_id"],
            row["variable"],
            row["issued_at"],
            row["valid_at"],
            row["lead_hours"],
        ) == (feed_id, "temperature", "2035-03-01T00:00:00Z", new_valid_at, 1)


def test_gate_rollback_discards_both_samples_and_the_job() -> None:
    conn = _conn()
    _seed_site(conn)
    feed_id = _seed_feed(conn)
    target = _target(conn, feed_id)
    _seed_obs(conn, valid_at="2035-03-02T00:30:00Z")

    result = _pairable_result(
        target.model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-03-02T00:30:00Z"
    )
    conn.commit()
    conn.execute("BEGIN")
    _persist_fetch_success(conn, target, result)
    conn.rollback()

    assert (
        int(conn.execute("SELECT COUNT(*) AS n FROM forecast_samples").fetchone()["n"])
        == 0
    )
    assert _score_job_count(conn) == 0


@pytest.mark.parametrize("fenced", [False, True], ids=["unfenced", "fenced"])
def test_gate_live_path_injected_enqueue_failure_rolls_back_fetch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fenced: bool
) -> None:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    conn = init_db(str(db_path))._conn  # noqa: SLF001
    site_id = _seed_site(conn)
    feed_id = conn.execute(
        "SELECT id FROM feeds WHERE source='open-meteo' AND is_virtual=0"
        " ORDER BY id LIMIT 1"
    ).fetchone()["id"]
    model = _feed_model(conn, feed_id)
    _seed_obs(conn, site_id=site_id, valid_at="2035-03-02T00:30:00Z")
    db = get_db()
    # A FencedWriter and the unfenced CLI path both write through the same
    # underlying Database._conn (see FencedWriter.write -> write_fenced),
    # so `conn` above is the right handle to assert on either way.
    writer = FencedWriter(db, db.generation) if fenced else None

    class _PairableAdapter:
        supports_historical = False

        def estimate_cost(self, req: Any) -> CostEstimate:
            return CostEstimate(calls=1)

        async def fetch_forecast(self, req: Any) -> FetchResult:
            return _pairable_result(
                model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-03-02T00:30:00Z"
            )

    def _build(source: str, client: Any) -> _PairableAdapter:
        return _PairableAdapter()

    def _raise_enqueue(*args: Any, **kwargs: Any) -> None:
        raise sqlite3.OperationalError("synthetic enqueue failure")

    monkeypatch.setattr(feed_fetch_module, "enqueue_if_absent", _raise_enqueue)

    with pytest.raises(sqlite3.OperationalError):
        asyncio.run(
            fetch_feed_once(db, site_id, feed_id, writer=writer, adapter_builder=_build)
        )

    assert (
        int(conn.execute("SELECT COUNT(*) AS n FROM forecast_samples").fetchone()["n"])
        == 0
    )
    assert _score_job_count(conn) == 0


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
        supports_historical = False

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
            fetch_feed_once(db, site_id, feed_id, writer=writer, adapter_builder=_build)
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


def _debug_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        r
        for r in caplog.records
        if r.name == "wxverify.worker.feed_fetch"
        and r.levelno == logging.DEBUG
        and r.getMessage().startswith("fetch score gate site=")
    ]


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


def test_gate_log_created_on_a_fresh_pairable_fetch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _conn()
    _seed_site(conn)
    feed_id = _seed_feed(conn)
    target = _target(conn, feed_id)
    _seed_obs(conn, valid_at="2035-03-02T00:30:00Z")
    result = _pairable_result(
        target.model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-03-02T00:30:00Z"
    )

    with caplog.at_level(logging.DEBUG, logger="wxverify.worker.feed_fetch"):
        _run_gate(conn, target, result)

    records = _debug_records(caplog)
    assert len(records) == 1
    assert "pairable=True created=True" in records[0].getMessage()
    assert _score_job_count(conn) == 1


def test_gate_log_dedupe_against_an_existing_active_score_row(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _conn()
    _seed_site(conn)
    feed_id = _seed_feed(conn)
    target = _target(conn, feed_id)
    _seed_obs(conn, valid_at="2035-03-02T00:30:00Z")
    seeded = enqueue_if_absent(conn, "pair_and_score", 1, "score", {"site_id": 1})
    assert seeded.job_id is not None
    result = _pairable_result(
        target.model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-03-02T00:30:00Z"
    )

    with caplog.at_level(logging.DEBUG, logger="wxverify.worker.feed_fetch"):
        _run_gate(conn, target, result)

    records = _debug_records(caplog)
    assert len(records) == 1
    assert "pairable=True created=False" in records[0].getMessage()
    rows = conn.execute(
        "SELECT id FROM jobs WHERE type='pair_and_score' AND job_key='score'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["id"] == seeded.job_id


def test_gate_log_skip_on_a_future_only_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    conn = _conn()
    _seed_site(conn)
    feed_id = _seed_feed(conn)
    target = _target(conn, feed_id)
    # No observation: future-only / unpairable.
    result = _pairable_result(
        target.model, issued_at="2035-03-01T00:00:00Z", valid_at="2035-06-01T00:30:00Z"
    )

    with caplog.at_level(logging.DEBUG, logger="wxverify.worker.feed_fetch"):
        _run_gate(conn, target, result)

    records = _debug_records(caplog)
    assert len(records) == 1
    assert "pairable=False created=None" in records[0].getMessage()
    assert _score_job_count(conn) == 0


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


def test_gate_slow_probe_const_is_the_plan_figure() -> None:
    assert feed_fetch_module.SCORE_GATE_SLOW_SECONDS == 0.25


# ---------------------------------------------------------------------------
# 11.4 Window (T-window-1..3, T-window-4, T-window-boot-1..4)
# ---------------------------------------------------------------------------


def _score_jobs_for_site(conn: sqlite3.Connection, site_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT id, status FROM jobs"
        " WHERE type='pair_and_score' AND job_key='score' AND site_id=?"
        " ORDER BY id",
        (site_id,),
    ).fetchall()


def _three_site_fixture(conn: sqlite3.Connection) -> None:
    """Two enabled sites (1, 2) and one disabled site (3)."""
    _seed_site(conn, site_id=1, enabled=1)
    _seed_site(conn, site_id=2, enabled=1)
    _seed_site(conn, site_id=3, enabled=0)


def _delete_rolling_window_setting(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM settings WHERE key = 'rolling_window_days'")
    assert (
        conn.execute(
            "SELECT value FROM settings WHERE key = 'rolling_window_days'"
        ).fetchone()
        is None
    ), "precondition: migration seeds rolling_window_days, it must be deleted first"


def _cache_window_keys(conn: sqlite3.Connection, site_id: int) -> set[str]:
    return {
        row["window_key"]
        for row in conn.execute(
            "SELECT DISTINCT window_key FROM score_cache WHERE site_id = ?",
            (site_id,),
        )
    }


def test_window_change_queues_each_enabled_site_and_drops_the_old_slice() -> None:
    conn = _conn()
    _three_site_fixture(conn)
    feed_id = _seed_feed(conn)
    fresh = "2035-01-01T00:00:00Z"
    for window_key in ("w:30", "w:all"):
        upsert_score_cache(
            conn,
            site_id=1,
            feed_id=feed_id,
            variable="temperature",
            day_ahead=1,
            window_key=window_key,
            result=MetricResult(n=1, skill_score=0.5, confident=True),
            computed_at=fresh,
        )

    set_rolling_window_days_sync(conn, 14)  # 30 -> 14

    assert len(_score_jobs_for_site(conn, 1)) == 1
    assert _score_jobs_for_site(conn, 1)[0]["status"] == "pending"
    assert len(_score_jobs_for_site(conn, 2)) == 1
    assert _score_jobs_for_site(conn, 2)[0]["status"] == "pending"
    assert _score_jobs_for_site(conn, 3) == []
    # Old w:30 slice removed, no stale w:14 leftover, w:all kept.
    assert _cache_window_keys(conn, 1) == {"w:all"}


def test_window_unchanged_reapply_queues_nothing_but_still_deletes_stale_slice() -> (
    None
):
    conn = _conn()
    _three_site_fixture(conn)
    feed_id = _seed_feed(conn)
    # A w:14 leftover from a previous, now-stale window value.
    upsert_score_cache(
        conn,
        site_id=1,
        feed_id=feed_id,
        variable="temperature",
        day_ahead=1,
        window_key="w:14",
        result=MetricResult(n=1, skill_score=0.5, confident=True),
        computed_at="2035-01-01T00:00:00Z",
    )

    set_rolling_window_days_sync(conn, 30)  # already 30: unchanged

    assert _score_jobs_for_site(conn, 1) == []
    assert _score_jobs_for_site(conn, 2) == []
    # The DELETE still runs even when prior == days.
    assert "w:14" not in _cache_window_keys(conn, 1)


def test_window_change_dedupes_against_an_existing_pending_row() -> None:
    conn = _conn()
    _three_site_fixture(conn)
    seeded_1 = enqueue_if_absent(conn, "pair_and_score", 1, "score", {"site_id": 1})
    assert seeded_1.job_id is not None

    set_rolling_window_days_sync(conn, 14)  # 30 -> 14

    rows_1 = _score_jobs_for_site(conn, 1)
    assert len(rows_1) == 1
    assert rows_1[0]["id"] == seeded_1.job_id
    rows_2 = _score_jobs_for_site(conn, 2)
    assert len(rows_2) == 1


def test_window_running_row_dedupes_and_does_not_queue_behind_it() -> None:
    conn = _conn()
    _three_site_fixture(conn)

    enqueue_if_absent(conn, "pair_and_score", 1, "score", {"site_id": 1})
    claimed = claim_next_job(conn)
    assert claimed is not None
    rows = _score_jobs_for_site(conn, 1)
    assert len(rows) == 1
    running_id = rows[0]["id"]
    assert claimed.id == running_id
    assert rows[0]["status"] == "running"

    set_rolling_window_days_sync(conn, 14)  # 30 -> 14

    rows_1 = _score_jobs_for_site(conn, 1)
    assert len(rows_1) == 1, (
        "setter must dedupe against the running row, not queue behind it"
    )
    assert rows_1[0]["id"] == running_id
    assert rows_1[0]["status"] == "running"

    rows_2 = _score_jobs_for_site(conn, 2)
    assert len(rows_2) == 1
    assert rows_2[0]["status"] == "pending"

    rows_3 = _score_jobs_for_site(conn, 3)
    assert rows_3 == []


def test_window_boot_absent_to_30_queues_nothing() -> None:
    conn = _conn()
    _three_site_fixture(conn)
    _delete_rolling_window_setting(conn)

    set_rolling_window_days_sync(conn, 30)

    assert _score_jobs_for_site(conn, 1) == []
    assert _score_jobs_for_site(conn, 2) == []
    assert get_setting(conn, "rolling_window_days") == "30"


def test_window_boot_absent_to_14_queues_enabled_sites_only() -> None:
    conn = _conn()
    _three_site_fixture(conn)
    _delete_rolling_window_setting(conn)

    set_rolling_window_days_sync(conn, 14)

    rows_1 = _score_jobs_for_site(conn, 1)
    rows_2 = _score_jobs_for_site(conn, 2)
    assert len(rows_1) == 1
    assert rows_1[0]["status"] == "pending"
    assert len(rows_2) == 1
    assert rows_2[0]["status"] == "pending"
    assert _score_jobs_for_site(conn, 3) == []
    assert get_setting(conn, "rolling_window_days") == "14"


def test_window_boot_same_value_applied_twice_queues_once() -> None:
    conn = _conn()
    _three_site_fixture(conn)

    set_rolling_window_days_sync(conn, 14)  # 30 -> 14
    rows_1 = _score_jobs_for_site(conn, 1)
    rows_2 = _score_jobs_for_site(conn, 2)
    assert len(rows_1) == 1
    assert len(rows_2) == 1

    # Complete both before the second apply: left pending, enqueue_if_absent's
    # own dedupe would hide an erroneous second enqueue.
    complete(conn, rows_1[0]["id"], result="ok")
    complete(conn, rows_2[0]["id"], result="ok")
    assert all(
        row["status"] != "pending" and row["status"] != "running"
        for row in [*_score_jobs_for_site(conn, 1), *_score_jobs_for_site(conn, 2)]
    )

    set_rolling_window_days_sync(conn, 14)  # 14 -> 14, unchanged

    rows_1_after = _score_jobs_for_site(conn, 1)
    rows_2_after = _score_jobs_for_site(conn, 2)
    assert len(rows_1_after) == 1, "no new row for site 1"
    assert len(rows_2_after) == 1, "no new row for site 2"
    assert all(row["status"] == "completed" for row in rows_1_after)
    assert all(row["status"] == "completed" for row in rows_2_after)

    total = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type='pair_and_score' AND job_key='score'"
    ).fetchone()["n"]
    assert total == 2
    pending_or_running = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs"
        " WHERE type='pair_and_score' AND job_key='score'"
        " AND status IN ('pending', 'running')"
    ).fetchone()["n"]
    assert pending_or_running == 0


def test_window_boot_rollback_restores_setting_jobs_and_cache() -> None:
    conn = _conn()
    _three_site_fixture(conn)
    feed_id = _seed_feed(conn)
    fresh = "2035-01-01T00:00:00Z"
    for window_key in ("w:30", "w:all"):
        upsert_score_cache(
            conn,
            site_id=1,
            feed_id=feed_id,
            variable="temperature",
            day_ahead=1,
            window_key=window_key,
            result=MetricResult(n=1, skill_score=0.5, confident=True),
            computed_at=fresh,
        )

    def _cache_count(window_key: str) -> int:
        return int(
            conn.execute(
                "SELECT COUNT(*) AS n FROM score_cache WHERE window_key = ?",
                (window_key,),
            ).fetchone()["n"]
        )

    before_w30 = _cache_count("w:30")
    before_wall = _cache_count("w:all")
    assert before_w30 == 1
    assert before_wall == 1

    conn.commit()
    assert conn.in_transaction is False
    conn.execute("BEGIN")

    set_rolling_window_days_sync(conn, 14)

    # Inside the transaction, the setter took effect.
    assert get_setting(conn, "rolling_window_days") == "14"
    assert _cache_count("w:30") == 0
    assert len(_score_jobs_for_site(conn, 1)) == 1
    assert _score_jobs_for_site(conn, 1)[0]["status"] == "pending"
    assert len(_score_jobs_for_site(conn, 2)) == 1
    assert _score_jobs_for_site(conn, 2)[0]["status"] == "pending"

    conn.rollback()

    assert get_setting(conn, "rolling_window_days") == "30"
    assert _score_jobs_for_site(conn, 1) == []
    assert _score_jobs_for_site(conn, 2) == []
    assert _cache_count("w:30") == before_w30
    assert _cache_count("w:all") == before_wall


# ---------------------------------------------------------------------------
# 11.4 Backstop (T-backstop-*)
# ---------------------------------------------------------------------------


def _freeze(monkeypatch: pytest.MonkeyPatch, moment: datetime) -> None:
    """Patch both name-imported ``utc_now`` call sites to the same instant."""
    monkeypatch.setattr(queue_module, "utc_now", lambda: moment)
    monkeypatch.setattr(scheduler_module, "utc_now", lambda: moment)


def _seed_resolved_score_job(
    conn: sqlite3.Connection,
    site_id: int,
    at: datetime,
    status: str,
    *,
    result: str | None = None,
) -> int:
    """Hand-seed a pair_and_score job already resolved to ``status`` at
    instant ``at``, in the exact row shape ``complete()``/``fail()`` write
    (status + updated_at; ``result`` NULL unless given). Hand-seeding, not a
    call through ``complete()``/``fail()``, because those write ``updated_at``
    via ``isoformat_utc()``'s own default, which reads ``utc_now`` from
    ``wxverify.core.timeutil``'s namespace -- a third name-import this
    helper would otherwise have to patch on top of the two the backstop
    due-check itself reads."""
    stamp = at.isoformat().replace("+00:00", "Z")
    cur = conn.execute(
        "INSERT INTO jobs (type, site_id, job_key, payload, status, result,"
        " updated_at) VALUES ('pair_and_score', ?, 'score', '{}', ?, ?, ?)",
        (site_id, status, result, stamp),
    )
    conn.commit()
    job_id = cur.lastrowid
    assert job_id is not None
    return job_id


_T = datetime(2035, 3, 10, 0, 0, 0, tzinfo=UTC)


def test_backstop_no_prior_score_job_queues_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _conn()
    _seed_site(conn)

    _freeze(monkeypatch, _T)
    scheduler_tick(conn)

    assert _score_jobs_for_site(conn, 1) == []


def test_backstop_fresh_completion_queues_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _conn()
    _seed_site(conn)
    _seed_resolved_score_job(conn, 1, _T, "completed")

    _freeze(monkeypatch, _T + timedelta(hours=6) - timedelta(seconds=1))
    scheduler_tick(conn)

    # No NEW pair_and_score row beyond the one already resolved.
    rows = _score_jobs_for_site(conn, 1)
    assert len(rows) == 1
    assert rows[0]["status"] == "completed"


def test_backstop_due_refresh_queues_one_job_via_the_full_tick(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _conn()
    _seed_site(conn)
    _seed_resolved_score_job(conn, 1, _T, "completed")

    _freeze(monkeypatch, _T + timedelta(hours=6))
    scheduler_tick(conn)

    rows = _score_jobs_for_site(conn, 1)
    assert len(rows) == 2
    assert rows[0]["status"] == "completed"
    assert rows[1]["status"] == "pending"


def test_backstop_recent_failure_stays_in_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _conn()
    _seed_site(conn)
    _seed_resolved_score_job(conn, 1, _T, "failed")

    _freeze(monkeypatch, _T + timedelta(minutes=59))
    scheduler_tick(conn)

    rows = _score_jobs_for_site(conn, 1)
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"


def test_backstop_old_failure_is_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _conn()
    _seed_site(conn)
    _seed_resolved_score_job(conn, 1, _T, "failed")

    _freeze(monkeypatch, _T + timedelta(hours=1))
    scheduler_tick(conn)

    rows = _score_jobs_for_site(conn, 1)
    assert len(rows) == 2
    assert rows[0]["status"] == "failed"
    assert rows[1]["status"] == "pending"


def test_backstop_unreadable_stamp_queues_once_and_warns_once(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _conn()
    _seed_site(conn)
    _freeze(monkeypatch, _T)
    seeded = enqueue_if_absent(conn, "pair_and_score", 1, "score", {"site_id": 1})
    assert seeded.job_id is not None
    conn.execute(
        "UPDATE jobs SET status='completed', updated_at='not-a-stamp' WHERE id=?",
        (seeded.job_id,),
    )
    conn.commit()

    with caplog.at_level(logging.WARNING, logger="wxverify.worker.scheduler"):
        scheduler_tick(conn)

    rows = _score_jobs_for_site(conn, 1)
    assert len(rows) == 2
    assert rows[1]["status"] == "pending"
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING
        and "score refresh: unreadable updated_at" in r.getMessage()
    ]
    assert len(warnings) == 1

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="wxverify.worker.scheduler"):
        scheduler_tick(conn)

    # The bad-stamp row is untouched (still status='completed' with the same
    # bad stamp) and the new pending row dedupes the second tick, so no new
    # row and no new warning.
    assert len(_score_jobs_for_site(conn, 1)) == 2
    assert [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING
        and "score refresh: unreadable updated_at" in r.getMessage()
    ] == []


def test_backstop_active_row_dedupes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _conn()
    _seed_site(conn)
    _freeze(monkeypatch, _T)
    seeded = enqueue_if_absent(conn, "pair_and_score", 1, "score", {"site_id": 1})
    assert seeded.job_id is not None

    _freeze(monkeypatch, _T + timedelta(hours=7))
    scheduler_tick(conn)

    rows = _score_jobs_for_site(conn, 1)
    assert len(rows) == 1
    assert rows[0]["id"] == seeded.job_id
    assert rows[0]["status"] == "pending"


def test_backstop_disabled_site_queues_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _conn()
    _seed_site(conn, enabled=0)
    _seed_resolved_score_job(conn, 1, _T, "completed")

    _freeze(monkeypatch, _T + timedelta(hours=7))
    scheduler_tick(conn)

    rows = _score_jobs_for_site(conn, 1)
    assert len(rows) == 1
    assert rows[0]["status"] == "completed"


def test_backstop_cancelled_completion_counts_as_fresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _conn()
    _seed_site(conn)
    _seed_resolved_score_job(conn, 1, _T, "completed")
    # The helper's complete() call leaves result=NULL already (a "cancelled
    # completion" per the backstop's own docstring); assert that precondition
    # before relying on it.
    row = _score_jobs_for_site(conn, 1)[0]
    assert (
        conn.execute("SELECT result FROM jobs WHERE id = ?", (row["id"],)).fetchone()[
            "result"
        ]
        is None
    )

    _freeze(monkeypatch, _T + timedelta(hours=5))
    scheduler_tick(conn)

    rows = _score_jobs_for_site(conn, 1)
    assert len(rows) == 1
    assert rows[0]["status"] == "completed"


# ---------------------------------------------------------------------------
# 11.3 Obs (T-obs-1, T-obs-2, T-obs-3)
# ---------------------------------------------------------------------------

_OBS_API_KEY = "0123456789abcdef0123456789abcdef"


def _init_tmp_obs_db(tmp_path: Path) -> sqlite3.Connection:
    close_db()
    db_path = tmp_path / "wxverify.db"
    config.db_path = str(db_path)
    config.options_path = str(tmp_path / "missing-options.json")
    db = init_db(str(db_path))
    return db._conn  # noqa: SLF001 -- tests inspect the real writer connection


def _seed_obs_site_and_stations(
    conn: sqlite3.Connection, station_ids: list[str]
) -> tuple[int, dict[str, int]]:
    site_id = int(
        conn.execute(
            "INSERT INTO sites (name, forecast_lat, forecast_lon, elevation_m,"
            " timezone) VALUES (?, ?, ?, 100.0, 'UTC')",
            (_SITE_NAME, _LAT, _LON),
        ).lastrowid
    )
    ids: dict[str, int] = {}
    for pws_id in station_ids:
        ids[pws_id] = int(
            conn.execute(
                "INSERT INTO stations"
                " (site_id, pws_station_id, lat, lon, dem_elevation_m)"
                " VALUES (?, ?, ?, ?, 100.0)",
                (site_id, pws_id, _LAT, _LON),
            ).lastrowid
        )
    return site_id, ids


def _obs_payload_response(readings: list[tuple[str, float]]) -> httpx.Response:
    """A 200 whose body decodes to one temperature observation per reading
    (only ``metric.temp`` is populated)."""
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


def _empty_obs_response() -> httpx.Response:
    return httpx.Response(200, content=b'{"observations": []}')


def _run_fetch_obs(
    db: object, writer: FencedWriter, site_id: int, handler: object
) -> None:
    real_client = httpx.AsyncClient
    with patch(
        "wxverify.worker.processor.httpx.AsyncClient",
        lambda *a, **kw: real_client(transport=httpx.MockTransport(handler)),  # type: ignore[arg-type]
    ):
        asyncio.run(
            dispatch(
                db,  # type: ignore[arg-type]
                writer,
                Job(
                    id=1,
                    type="fetch_obs",
                    site_id=site_id,
                    job_key="obs",
                    payload={},
                    status="running",
                    retry_count=0,
                    max_retries=5,
                ),
            )
        )


def test_obs_abort_after_a_changed_station_still_queues_that_stations_pair_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression pin for §3's lost trigger: a later station's transport
    error must not drop the score-run trigger a changed earlier station
    already queued (``_persist_station_observations``'s own per-station
    enqueue, see M12)."""
    conn = _init_tmp_obs_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _OBS_API_KEY)
    monkeypatch.setattr(
        "wxverify.worker.processor.pace_station_call",
        lambda site_id, station_id, ordinal: asyncio.sleep(0),
    )
    site_id, ids = _seed_obs_site_and_stations(
        conn, ["ISTATION01", "ISTATION02", "ISTATION03"]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        if station == "ISTATION01":
            return _obs_payload_response([("2035-03-01T11:00:00Z", 10.0)])
        if station == "ISTATION02":
            return _empty_obs_response()
        raise httpx.ConnectError("synthetic", request=request)

    db = get_db()
    writer = FencedWriter(db, db.generation)
    with pytest.raises(httpx.ConnectError):
        _run_fetch_obs(db, writer, site_id, handler)

    rows = conn.execute(
        "SELECT status FROM jobs WHERE type='pair_and_score' AND site_id=?",
        (site_id,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["status"] == "pending"


def test_obs_no_change_across_every_station_queues_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every station reports only a reading already stored byte-for-byte --
    not an empty payload, which `_persist_station_observations` would never
    even reach the `changed` computation for. Seed via a real fetch cycle
    first (so the stored row has the exact value/qc_flag/source_raw the
    repeat response will be compared against), retire that cycle's job, then
    replay the identical payload and confirm nothing new is queued."""
    conn = _init_tmp_obs_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _OBS_API_KEY)
    monkeypatch.setattr(
        "wxverify.worker.processor.pace_station_call",
        lambda site_id, station_id, ordinal: asyncio.sleep(0),
    )
    # Freeze "now" inside every reading's RECENT_REFRESH_HOURS window so the
    # replay cycle below doesn't park a station for "no recent history" --
    # that park check runs off the real wall clock otherwise, and would
    # skip every station on the second cycle regardless of the gate under
    # test, hiding the mutant this test must kill.
    moment = datetime(2035, 3, 1, 13, 30, tzinfo=UTC)
    monkeypatch.setattr("wxverify.worker.processor.utc_now", lambda: moment)
    site_id, ids = _seed_obs_site_and_stations(
        conn, ["ISTATION01", "ISTATION02", "ISTATION03"]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        return _obs_payload_response([(f"2035-03-01T1{station[-1]}:00:00Z", 10.0)])

    db = get_db()
    writer = FencedWriter(db, db.generation)

    # Seed: a first cycle stores each station's reading (and does queue a
    # score run -- that is the paired positive this test leans on: the gate
    # fires on real change). Retire that job so the dedupe check below can't
    # mask the second cycle's own enqueue decision.
    _run_fetch_obs(db, writer, site_id, handler)
    stored_before = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations"
    ).fetchone()["n"]
    assert stored_before > 0
    seeded_job = conn.execute(
        "SELECT id FROM jobs WHERE type='pair_and_score' AND site_id=?", (site_id,)
    ).fetchone()
    assert seeded_job is not None
    conn.execute("UPDATE jobs SET status='completed' WHERE id=?", (seeded_job["id"],))
    conn.commit()

    # Replay: every station now reports exactly what's already on file.
    _run_fetch_obs(db, writer, site_id, handler)

    rows = conn.execute(
        "SELECT 1 FROM jobs WHERE type='pair_and_score' AND site_id=?"
        " AND status IN ('pending', 'running')",
        (site_id,),
    ).fetchall()
    assert rows == []
    stored_after = conn.execute(
        "SELECT COUNT(*) AS n FROM station_observations"
    ).fetchone()["n"]
    assert stored_after == stored_before


def test_obs_full_success_queues_exactly_one_deduped_pair_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every station reports a new observation: the per-station enqueue
    (M12) and ``_complete_obs_cycle``'s cycle-end enqueue must dedupe to one
    row, not stack."""
    conn = _init_tmp_obs_db(tmp_path)
    monkeypatch.setenv("WXV_WEATHERCOM_KEY", _OBS_API_KEY)
    monkeypatch.setattr(
        "wxverify.worker.processor.pace_station_call",
        lambda site_id, station_id, ordinal: asyncio.sleep(0),
    )
    site_id, ids = _seed_obs_site_and_stations(
        conn, ["ISTATION01", "ISTATION02", "ISTATION03"]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        station = str(request.url.params["stationId"])
        return _obs_payload_response([(f"2035-03-01T1{station[-1]}:00:00Z", 10.0)])

    db = get_db()
    writer = FencedWriter(db, db.generation)
    _run_fetch_obs(db, writer, site_id, handler)

    rows = conn.execute(
        "SELECT status FROM jobs WHERE type='pair_and_score' AND site_id=?",
        (site_id,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["status"] == "pending"


# ---------------------------------------------------------------------------
# 11.5 Ranking effect under a controlled clock (w:30) and sweep
# ---------------------------------------------------------------------------

_RANK_T0 = datetime(2035, 3, 10, 2, 0, 0, tzinfo=UTC)


def _z(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def _freeze_rank(monkeypatch: pytest.MonkeyPatch, moment: datetime) -> None:
    """Patch every ``utc_now`` name-import the ranking path reads: the
    queue/scheduler pair the backstop's own ``_freeze`` already covers,
    plus ``wxverify.core.timeutil``'s own module-global. ``engine.py``,
    ``leaderboard.py`` and ``cache.py`` never name-import ``utc_now``
    directly -- their clock reads (``window_cutoff``, ``isoformat_utc``,
    ``isoformat_utc_micro``, ``utc_day_bucket``) all resolve through that
    global, so patching it covers the scoring engine, the cache-freshness
    check, and the job's own ``complete()`` stamp in one call."""
    _freeze(monkeypatch, moment)
    monkeypatch.setattr(timeutil_module, "utc_now", lambda: moment)


def _mark_default_subscribed(conn: sqlite3.Connection, *feed_ids: int) -> None:
    """``_seed_feed`` leaves ``default_subscribed = 0`` (the migration's own
    default for a newly inserted feed); with no ``site_feed_state`` row to
    override it, ``active_competitor_clause`` -- which every leaderboard
    read filters through -- would then treat the feed as not a competitor
    and omit it from both the live query and the cache read, leaving only
    the virtual persistence feed visible. The pairing/gate probes in
    §11.1/11.2 never read the leaderboard, so they never needed this."""
    conn.executemany(
        "UPDATE feeds SET default_subscribed = 1 WHERE id = ?",
        [(feed_id,) for feed_id in feed_ids],
    )


def _seed_rank_fixture(conn: sqlite3.Connection, t0: datetime) -> tuple[int, int, int]:
    """Two real feeds, A and B. A's advantage lies entirely in pairs just
    inside ``[t0 - 30d, t0 - 30d + 4h)`` -- in the window at t0, out of it
    4h later -- while A also keeps worse pairs well inside the window (so
    A never drops out of the leaderboard's expected feed set); B is steady
    across both groups. Observed values vary day to day: a fixture whose
    "steady" observations are all equal makes the auto-materialized
    persistence forecast perfect every time, so ``persistence_mse == 0``
    and ``_paired_skill`` returns ``None`` for every feed (a divide-by-zero
    guard, not a bug) -- varying the values keeps persistence imperfect.
    Each edge point also seeds a predecessor observation 24h earlier so
    persistence can pair there too, making the edge group's skill sensitive
    to A/B's own values rather than silently absent from the computation.
    """
    site_id = _seed_site(conn)
    set_setting(conn, "min_n", "1")
    feed_a = _seed_feed(conn)
    feed_b = _seed_feed(conn)
    _mark_default_subscribed(conn, feed_a, feed_b)

    edge_start = t0 - timedelta(days=30)
    edge_obs = [10.0, 12.0, 9.0]
    edge_predecessor_obs = [14.0, 7.0, 16.0]
    for hour, observed in enumerate(edge_obs):
        valid_at = edge_start + timedelta(hours=hour)
        _seed_obs(conn, site_id=site_id, valid_at=_z(valid_at), value=observed)
        _seed_obs(
            conn,
            site_id=site_id,
            valid_at=_z(valid_at - timedelta(hours=24)),
            value=edge_predecessor_obs[hour],
        )
        _seed_sample(
            conn,
            site_id=site_id,
            feed_id=feed_a,
            issued_at=_z(valid_at - timedelta(hours=24)),
            valid_at=_z(valid_at),
            lead_hours=24,
            value=observed,  # A: perfect in this group
        )
        _seed_sample(
            conn,
            site_id=site_id,
            feed_id=feed_b,
            issued_at=_z(valid_at - timedelta(hours=24)),
            valid_at=_z(valid_at),
            lead_hours=24,
            value=observed + 6.0,  # B: bad in this group
        )

    steady_obs = [10.0, 12.0, 9.0, 13.0, 8.0]  # t0 - 5d .. t0 - 1d
    for day_offset, observed in zip(range(5, 0, -1), steady_obs, strict=True):
        valid_at = t0 - timedelta(days=day_offset)
        _seed_obs(conn, site_id=site_id, valid_at=_z(valid_at), value=observed)
        _seed_sample(
            conn,
            site_id=site_id,
            feed_id=feed_a,
            issued_at=_z(valid_at - timedelta(hours=24)),
            valid_at=_z(valid_at),
            lead_hours=24,
            value=observed + 3.0,  # A: mediocre in this group
        )
        _seed_sample(
            conn,
            site_id=site_id,
            feed_id=feed_b,
            issued_at=_z(valid_at - timedelta(hours=24)),
            valid_at=_z(valid_at),
            lead_hours=24,
            value=observed + 1.0,  # B: good in this group
        )
    conn.commit()
    return site_id, feed_a, feed_b


async def _run_queued_pair_and_score(db: object) -> None:
    """Claim and run the one pending job via the real worker path, as
    ``tests/test_graceful_shutdown.py`` does."""
    job = await db.write(claim_next_job)  # type: ignore[attr-defined]
    assert job is not None and job.type == "pair_and_score", job
    await run_claimed_job(db, job, lane="main")  # type: ignore[arg-type]


def _claim_pair_and_score_only(conn: sqlite3.Connection, site_id: int) -> Job | None:
    """Claim the pending ``pair_and_score`` row for ``site_id`` directly,
    the same status transition ``claim_next_job``'s ``_claim`` performs,
    but filtered to this type and site. ``scheduler_tick`` also queues an
    unrelated daily ``forecast_record`` job once the site's local snapshot
    time has passed (§14), and that job type claims ahead of everything
    else by design -- ``claim_next_job`` would hand back that job instead
    of the one this test means to run, and (in this fixture, with no
    station/network stub) that job fails and is re-claimed every tick."""
    row = conn.execute(
        "UPDATE jobs SET status = 'running', updated_at = ?"
        " WHERE id = (SELECT id FROM jobs WHERE type = 'pair_and_score'"
        " AND site_id = ? AND status = 'pending' ORDER BY created_at, id"
        " LIMIT 1)"
        " RETURNING id, type, site_id, job_key, payload, status,"
        " retry_count, max_retries",
        (isoformat_utc(), site_id),
    ).fetchone()
    if row is None:
        return None
    return Job(
        id=row["id"],
        type=row["type"],
        site_id=row["site_id"],
        job_key=row["job_key"],
        payload=json.loads(row["payload"]),
        status=row["status"],
        retry_count=row["retry_count"],
        max_retries=row["max_retries"],
    )


def _rank_skill_scores(
    conn: sqlite3.Connection, site_id: int, feed_a: int, feed_b: int
) -> tuple[str, float, float]:
    result = leaderboard_with_status(
        conn,
        site_id=site_id,
        variable="temperature",
        day_ahead=1,
        window="rolling",
    )
    rows = {r.feed_id: r for r in result.rows if r.feed_id in (feed_a, feed_b)}
    assert feed_a in rows and feed_b in rows, (
        "both feeds must stay in the expected feed set"
    )
    skill_a = rows[feed_a].skill_score
    skill_b = rows[feed_b].skill_score
    assert skill_a is not None and skill_b is not None
    return result.status, skill_a, skill_b


def test_rank_order_follows_real_pairing_then_backstop_recomputes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T-rank steps 1, 2 and 4: the real job path's leaderboard order at
    T0, a stale-cache read 4h later that leaves it unchanged, and the time
    backstop's recompute 6h later that flips it (matching the twin
    fixture's order in
    ``test_rank_twin_fixture_confirms_the_order_is_not_vacuous``)."""
    conn = _init_tmp_obs_db(tmp_path)
    db = get_db()
    site_id, feed_a, feed_b = _seed_rank_fixture(conn, _RANK_T0)
    enqueue_if_absent(conn, "pair_and_score", site_id, "score", {"site_id": site_id})
    conn.commit()

    # Step 1: run the queued job at T0, then read.
    _freeze_rank(monkeypatch, _RANK_T0)
    asyncio.run(_run_queued_pair_and_score(db))
    with patch.object(timeutil_module, "utc_now", lambda: _RANK_T0):
        status0, skill_a0, skill_b0 = _rank_skill_scores(conn, site_id, feed_a, feed_b)
    assert status0 == "hit"
    assert skill_a0 > skill_b0, "O0: A must rank above B at T0"

    # Step 2: a future-only, unpairable sample at T1 queues no job, and the
    # stale cache still serves O0 unchanged.
    t1 = _RANK_T0 + timedelta(hours=4)
    target = FeedFetchTarget(
        site_id=site_id,
        feed_id=feed_a,
        lat=_LAT,
        lon=_LON,
        source="open-meteo",
        model=_feed_model(conn, feed_a),
        max_lead_hours=168,
    )
    future_sample = NormalizedSample(
        model=_feed_model(conn, feed_a),
        variable="temperature",
        issued_at=_z(t1),
        valid_at=_z(t1 + timedelta(hours=48)),
        lead_hours=48,
        value=1.0,
        source_raw="{}",
        model_run_id="future",
    )
    conn.commit()
    conn.execute("BEGIN")
    _persist_fetch_success(
        conn, target, FetchResult(samples=[future_sample], grid=None)
    )
    conn.commit()
    assert (
        conn.execute(
            "SELECT 1 FROM jobs WHERE type = 'pair_and_score' AND site_id = ?"
            " AND status = 'pending'",
            (site_id,),
        ).fetchone()
        is None
    ), "a future-only, unpairable sample must not queue a score run"

    with patch.object(timeutil_module, "utc_now", lambda: t1):
        status1, skill_a1, skill_b1 = _rank_skill_scores(conn, site_id, feed_a, feed_b)
    assert status1 == "hit"
    assert (skill_a1, skill_b1) == (skill_a0, skill_b0), (
        "the stale cache must still serve O0 unchanged"
    )

    # Step 4: the backstop queues nothing at T0+6h-1s, one job at T0+6h;
    # running it recomputes and must flip the order (matching the twin's
    # O1 below).
    before_count = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE type = 'pair_and_score' AND site_id = ?",
        (site_id,),
    ).fetchone()["n"]

    almost_due = _RANK_T0 + timedelta(hours=6) - timedelta(seconds=1)
    _freeze_rank(monkeypatch, almost_due)
    scheduler_tick(conn)
    assert (
        conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE type = 'pair_and_score'"
            " AND site_id = ?",
            (site_id,),
        ).fetchone()["n"]
        == before_count
    )

    due = _RANK_T0 + timedelta(hours=6)
    _freeze_rank(monkeypatch, due)
    scheduler_tick(conn)
    assert (
        conn.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE type = 'pair_and_score'"
            " AND site_id = ?",
            (site_id,),
        ).fetchone()["n"]
        == before_count + 1
    )

    async def _run_backstop() -> None:
        job = await db.write(  # type: ignore[attr-defined]
            lambda c: _claim_pair_and_score_only(c, site_id)
        )
        assert job is not None and job.type == "pair_and_score", job
        await run_claimed_job(db, job, lane="main")  # type: ignore[arg-type]

    asyncio.run(_run_backstop())

    with patch.object(timeutil_module, "utc_now", lambda: due):
        status2, skill_a2, skill_b2 = _rank_skill_scores(conn, site_id, feed_a, feed_b)
    assert status2 == "hit"
    assert skill_b2 > skill_a2, (
        "the backstop's recompute must flip the order: B above A, as in"
        " the twin fixture's O1"
    )


def test_rank_twin_fixture_confirms_the_order_is_not_vacuous(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T-rank step 3, the non-vacuity guard: the identical fixture, scored
    at T1 instead of T0, must give the opposite order. If it did not, O0's
    assertions in
    ``test_rank_order_follows_real_pairing_then_backstop_recomputes`` would
    hold for a fixture-independent reason and the test would be vacuous."""
    conn = _init_tmp_obs_db(tmp_path)
    db = get_db()
    site_id, feed_a, feed_b = _seed_rank_fixture(conn, _RANK_T0)
    enqueue_if_absent(conn, "pair_and_score", site_id, "score", {"site_id": site_id})
    conn.commit()

    t1 = _RANK_T0 + timedelta(hours=4)
    _freeze_rank(monkeypatch, t1)
    asyncio.run(_run_queued_pair_and_score(db))

    with patch.object(timeutil_module, "utc_now", lambda: t1):
        status1, skill_a1, skill_b1 = _rank_skill_scores(conn, site_id, feed_a, feed_b)
    assert status1 == "hit"
    assert skill_b1 > skill_a1, (
        "O1: scored at T1, B must rank above A -- the opposite of O0"
    )


def _seed_sweep_fixture(conn: sqlite3.Connection, t0: datetime) -> tuple[int, int, int]:
    """Feed A has pairs well inside the rolling window at both T0 and
    T0 + 4h. Feed C's only pairs lie in ``[t0 - 30d, t0 - 30d + 4h)`` -- in
    the window at T0, out of it by T1 = T0 + 4h."""
    site_id = _seed_site(conn)
    set_setting(conn, "min_n", "1")
    feed_a = _seed_feed(conn)
    feed_c = _seed_feed(conn)
    _mark_default_subscribed(conn, feed_a, feed_c)

    for day_offset, observed in zip(
        range(5, 0, -1), [10.0, 12.0, 9.0, 13.0, 8.0], strict=True
    ):
        valid_at = t0 - timedelta(days=day_offset)
        _seed_obs(conn, site_id=site_id, valid_at=_z(valid_at), value=observed)
        _seed_sample(
            conn,
            site_id=site_id,
            feed_id=feed_a,
            issued_at=_z(valid_at - timedelta(hours=24)),
            valid_at=_z(valid_at),
            lead_hours=24,
            value=observed + 2.0,
        )

    edge_start = t0 - timedelta(days=30)
    for hour, observed in enumerate([10.0, 12.0, 9.0]):
        valid_at = edge_start + timedelta(hours=hour)
        _seed_obs(conn, site_id=site_id, valid_at=_z(valid_at), value=observed)
        _seed_sample(
            conn,
            site_id=site_id,
            feed_id=feed_c,
            issued_at=_z(valid_at - timedelta(hours=24)),
            valid_at=_z(valid_at),
            lead_hours=24,
            value=observed + 1.0,
        )
    conn.commit()
    return site_id, feed_a, feed_c


def test_sweep_stale_feed_set_rebuilds_then_the_backstop_clears_its_cache_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T-sweep: a feed whose only pairs fall out of the rolling window
    between T0 and T1 makes the cached feed set stale (``rebuilding``), and
    the backstop's recompute at T0+6h drops its rolling-window
    ``score_cache`` rows, restoring ``hit``."""
    conn = _init_tmp_obs_db(tmp_path)
    db = get_db()
    site_id, feed_a, feed_c = _seed_sweep_fixture(conn, _RANK_T0)
    enqueue_if_absent(conn, "pair_and_score", site_id, "score", {"site_id": site_id})
    conn.commit()

    _freeze_rank(monkeypatch, _RANK_T0)
    asyncio.run(_run_queued_pair_and_score(db))

    with patch.object(timeutil_module, "utc_now", lambda: _RANK_T0):
        result0 = leaderboard_with_status(
            conn,
            site_id=site_id,
            variable="temperature",
            day_ahead=1,
            window="rolling",
        )
    assert result0.status == "hit"
    assert {r.feed_id for r in result0.rows} >= {feed_a, feed_c}
    assert (
        conn.execute(
            "SELECT COUNT(*) AS n FROM score_cache WHERE feed_id = ?"
            " AND window_key = 'w:30'",
            (feed_c,),
        ).fetchone()["n"]
        == 1
    )

    t1 = _RANK_T0 + timedelta(hours=4)
    with patch.object(timeutil_module, "utc_now", lambda: t1):
        result1 = leaderboard_with_status(
            conn,
            site_id=site_id,
            variable="temperature",
            day_ahead=1,
            window="rolling",
        )
    assert result1.status == "rebuilding"
    assert result1.rows == []

    due = _RANK_T0 + timedelta(hours=6)
    _freeze_rank(monkeypatch, due)
    scheduler_tick(conn)
    assert (
        conn.execute(
            "SELECT 1 FROM jobs WHERE type = 'pair_and_score' AND site_id = ?"
            " AND status = 'pending'",
            (site_id,),
        ).fetchone()
        is not None
    )

    async def _run_backstop() -> None:
        job = await db.write(  # type: ignore[attr-defined]
            lambda c: _claim_pair_and_score_only(c, site_id)
        )
        assert job is not None and job.type == "pair_and_score", job
        await run_claimed_job(db, job, lane="main")  # type: ignore[arg-type]

    asyncio.run(_run_backstop())

    assert (
        conn.execute(
            "SELECT COUNT(*) AS n FROM score_cache WHERE feed_id = ?"
            " AND window_key = 'w:30'",
            (feed_c,),
        ).fetchone()["n"]
        == 0
    ), "the backstop's recompute must drop C's stale rolling-window row"

    with patch.object(timeutil_module, "utc_now", lambda: due):
        result2 = leaderboard_with_status(
            conn,
            site_id=site_id,
            variable="temperature",
            day_ahead=1,
            window="rolling",
        )
    assert result2.status == "hit"
    assert feed_c not in {r.feed_id for r in result2.rows}
